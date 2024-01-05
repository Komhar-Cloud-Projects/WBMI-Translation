WITH
SQ_WBPOLBatchRenewalPolicyStage AS (
	--SM#791680 In the existing Query data from both stage and archival tables are used , inorder to reduce the --dependency on archival tables 
	--and increase the parallel processing(currently archival and integration jobs runs in sequence) we have developed the below query.
	
	--in the below Query Data from both stage table and archival tabel are used as input , incase archival and datawarehouse jobs runs parallelely then the beloq query will process all the --records without having any dependency on archival jobs.
	--Stage Tables - Containes Todays's Data
	--Stage Archival Tables - Containes Data Till Yesterday
	--booth will be union which is equal to data till today , incase of any duplicates will be handled in --the mapping.
	
	select 
	c.PolicyNumber AS PolicyNumber
	       ,ISNULL(RIGHT('00' + CONVERT(VARCHAR(3), d.PolicyVersion), 2), '00') AS PolicyVersion
	       ,a.AutoRenewedIndicator AS AutoRenewedIndicator
	       ,a.CustomerCarePolicyQualifiedAutomaticRenewalIndicator AS CustomerCarePolicyQualifiedAutomaticRenewalIndicator
	FROM WBPOLBatchRenewalPolicyStage a
	JOIN 
	(select HistoryId, SessionId, state from archDCTransactionStaging 
	--where ExtractDate < (select min(ExtractDate) from DCTransactionStaging)
	union all 
	select HistoryId, SessionId, state from DCTransactionStaging 
	
	) b
	
	ON a.HistoryId = b.HistoryID -- and 
	
	JOIN 
	(SELECT PolicyId, PolicyNumber, SessionId, Status from archDCPolicyStaging 
	--where ExtractDate < (select min(ExtractDate) from DCPolicyStaging)
	union all 
	SELECT PolicyId, PolicyNumber, SessionId, Status from DCPolicyStaging 
	) c
	
	ON b.SessionId = c.SessionId  
	
	JOIN 
	(select PolicyId, PolicyVersion from archWBPolicyStaging 
	--where ExtractDate < (select min(ExtractDate) from WBPolicyStaging)
	
	union all
	select PolicyId, PolicyVersion from WBPolicyStaging
	) d
	
	ON c.PolicyId = d.PolicyId  --and d.AuditId<>106190
	WHERE b.state = 'committed'
	AND c.Status <> 'Quote'
	order by c.PolicyNumber,ISNULL(RIGHT('00'+CONVERT(VARCHAR(3),d.PolicyVersion),2),'00'),b.SessionId
),
AGG_Remove_Duplicates AS (
	SELECT
	PolicyNumber, 
	PolicyVersion, 
	PolicyNumber || PolicyVersion AS PolicyKey, 
	AutoRenewedIndicator AS FutureAutomaticRenewalIndicator, 
	CustomerCarePolicyQualifiedAutomaticRenewalIndicator
	FROM SQ_WBPOLBatchRenewalPolicyStage
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY NULL) = 1
),
LKP_policy AS (
	SELECT
	pol_ak_id,
	pol_key
	FROM (
		SELECT 
			pol_ak_id,
			pol_key
		FROM V2.policy
		WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_ak_id) = 1
),
FILTRANS AS (
	SELECT
	LKP_policy.pol_ak_id, 
	AGG_Remove_Duplicates.FutureAutomaticRenewalIndicator, 
	AGG_Remove_Duplicates.CustomerCarePolicyQualifiedAutomaticRenewalIndicator
	FROM AGG_Remove_Duplicates
	LEFT JOIN LKP_policy
	ON LKP_policy.pol_key = AGG_Remove_Duplicates.PolicyKey
	WHERE pol_ak_id<>-1
),
EXP_values AS (
	SELECT
	pol_ak_id,
	FutureAutomaticRenewalIndicator AS i_FutureAutomaticRenewalIndicator,
	-- *INF*: DECODE(TRUE,
	-- i_FutureAutomaticRenewalIndicator='T','1',
	-- i_FutureAutomaticRenewalIndicator='1','1',
	-- '0')
	DECODE(TRUE,
	i_FutureAutomaticRenewalIndicator = 'T', '1',
	i_FutureAutomaticRenewalIndicator = '1', '1',
	'0') AS o_FutureAutomaticRenewalIndicator,
	CustomerCarePolicyQualifiedAutomaticRenewalIndicator,
	-- *INF*: DECODE(TRUE,
	-- CustomerCarePolicyQualifiedAutomaticRenewalIndicator='T','1',
	-- CustomerCarePolicyQualifiedAutomaticRenewalIndicator='1','1',
	-- '0')
	DECODE(TRUE,
	CustomerCarePolicyQualifiedAutomaticRenewalIndicator = 'T', '1',
	CustomerCarePolicyQualifiedAutomaticRenewalIndicator = '1', '1',
	'0') AS o_CustomerCarePolicyQualifiedAutomaticRenewalIndicator
	FROM FILTRANS
),
LKP_PolicyExtension AS (
	SELECT
	PolicyExtensionId,
	FutureAutomaticRenewalFlag,
	CustomerCarePolicyFutureAutomaticRenewalFlag,
	i_PolicyAKId,
	PolicyAKId
	FROM (
		SELECT 
			PolicyExtensionId,
			FutureAutomaticRenewalFlag,
			CustomerCarePolicyFutureAutomaticRenewalFlag,
			i_PolicyAKId,
			PolicyAKId
		FROM PolicyExtension
		WHERE SourceSystemId= '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKId ORDER BY PolicyExtensionId) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_PolicyExtension.PolicyExtensionId AS lkp_PolicyExtensionId,
	LKP_PolicyExtension.FutureAutomaticRenewalFlag AS lkp_FutureAutomaticRenewalFlag,
	LKP_PolicyExtension.CustomerCarePolicyFutureAutomaticRenewalFlag AS lkp_CustomerCarePolicyFutureAutomaticRenewalFlag,
	-- *INF*: IIF(lkp_FutureAutomaticRenewalFlag='T','1','0')
	IFF(lkp_FutureAutomaticRenewalFlag = 'T', '1', '0') AS v_lkp_FutureAutomaticRenewalFlag,
	-- *INF*: iif(lkp_CustomerCarePolicyFutureAutomaticRenewalFlag='T','1','0')
	IFF(lkp_CustomerCarePolicyFutureAutomaticRenewalFlag = 'T', '1', '0') AS v_lkp_CustomerCarePolicyFutureAutomaticRenewalFlag,
	-- *INF*: --DECODE(TRUE, lkp_PolicyExtensionId=-1,'NEW', FutureAutomaticRenewalIndicator<>v_lkp_FutureAutomaticRenewalFlag,'UPDATE', 'NOCHANGE' )
	-- DECODE(TRUE, lkp_PolicyExtensionId=-1,'NEW', FutureAutomaticRenewalIndicator<>v_lkp_FutureAutomaticRenewalFlag,'UPDATE', 
	-- CustomerCarePolicyQualifiedAutomaticRenewalIndicator<>v_lkp_CustomerCarePolicyFutureAutomaticRenewalFlag,'UPDATE',
	-- 'NOCHANGE' )
	DECODE(TRUE,
	lkp_PolicyExtensionId = - 1, 'NEW',
	FutureAutomaticRenewalIndicator <> v_lkp_FutureAutomaticRenewalFlag, 'UPDATE',
	CustomerCarePolicyQualifiedAutomaticRenewalIndicator <> v_lkp_CustomerCarePolicyFutureAutomaticRenewalFlag, 'UPDATE',
	'NOCHANGE') AS o_changed_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_audit_id,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_source_sys_id,
	SYSDATE AS o_created_date,
	SYSDATE AS o_modified_date,
	EXP_values.pol_ak_id,
	EXP_values.o_FutureAutomaticRenewalIndicator AS FutureAutomaticRenewalIndicator,
	EXP_values.o_CustomerCarePolicyQualifiedAutomaticRenewalIndicator AS CustomerCarePolicyQualifiedAutomaticRenewalIndicator
	FROM EXP_values
	LEFT JOIN LKP_PolicyExtension
	ON LKP_PolicyExtension.PolicyAKId = EXP_values.pol_ak_id
),
RTRTRANS AS (
	SELECT
	lkp_PolicyExtensionId,
	o_changed_flag AS changed_flag,
	o_audit_id AS audit_id,
	o_source_sys_id AS source_sys_id,
	o_created_date AS created_date,
	o_modified_date AS modified_date,
	pol_ak_id,
	FutureAutomaticRenewalIndicator,
	CustomerCarePolicyQualifiedAutomaticRenewalIndicator
	FROM EXP_Detect_Changes
),
RTRTRANS_Insert AS (SELECT * FROM RTRTRANS WHERE changed_flag='NEW'),
RTRTRANS_Update AS (SELECT * FROM RTRTRANS WHERE changed_flag='UPDATE'),
UPD_CodeChange AS (
	SELECT
	lkp_PolicyExtensionId, 
	modified_date, 
	FutureAutomaticRenewalIndicator, 
	CustomerCarePolicyQualifiedAutomaticRenewalIndicator AS CustomerCarePolicyQualifiedAutomaticRenewalIndicator3
	FROM RTRTRANS_Update
),
TGT_PolicyExtension_UPDATE AS (
	MERGE INTO PolicyExtension AS T
	USING UPD_CodeChange AS S
	ON T.PolicyExtensionId = S.lkp_PolicyExtensionId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.modified_date, T.FutureAutomaticRenewalFlag = S.FutureAutomaticRenewalIndicator, T.CustomerCarePolicyFutureAutomaticRenewalFlag = S.CustomerCarePolicyQualifiedAutomaticRenewalIndicator3
),
TGT_PolicyExtension_INSERT AS (
	INSERT INTO PolicyExtension
	(AuditId, SourceSystemId, CreatedDate, ModifiedDate, PolicyAKId, FutureAutomaticRenewalFlag, CustomerCarePolicyFutureAutomaticRenewalFlag)
	SELECT 
	audit_id AS AUDITID, 
	source_sys_id AS SOURCESYSTEMID, 
	created_date AS CREATEDDATE, 
	modified_date AS MODIFIEDDATE, 
	pol_ak_id AS POLICYAKID, 
	FutureAutomaticRenewalIndicator AS FUTUREAUTOMATICRENEWALFLAG, 
	CustomerCarePolicyQualifiedAutomaticRenewalIndicator AS CUSTOMERCAREPOLICYFUTUREAUTOMATICRENEWALFLAG
	FROM RTRTRANS_Insert
),
WITH
SQ_PolicyExtension AS (
	SELECT
		PolicyExtensionId,
		AuditId,
		SourceSystemId,
		CreatedDate,
		ModifiedDate,
		PolicyAKId,
		FutureAutomaticRenewalFlag,
		CustomerCarePolicyFutureAutomaticRenewalFlag
	FROM PolicyExtension
),
EXP_DefaultValues AS (
	SELECT
	SourceSystemId,
	PolicyAKId,
	FutureAutomaticRenewalFlag AS i_FutureAutomaticRenewalFlag,
	-- *INF*: IIF(i_FutureAutomaticRenewalFlag='T','1','0')
	IFF(i_FutureAutomaticRenewalFlag = 'T', '1', '0') AS o_FutureAutomaticRenewalFlag,
	CustomerCarePolicyFutureAutomaticRenewalFlag,
	-- *INF*: IIF(CustomerCarePolicyFutureAutomaticRenewalFlag='T','1','0')
	IFF(CustomerCarePolicyFutureAutomaticRenewalFlag = 'T', '1', '0') AS o_CustomerCarePolicyFutureAutomaticRenewalFlag
	FROM SQ_PolicyExtension
),
LKP_PolicyExtensionDim AS (
	SELECT
	PolicyExtensionDimId,
	FutureAutomaticRenewalFlag,
	CustomerCarePolicyFutureAutomaticRenewalFlag,
	EDWPolicyAKId
	FROM (
		SELECT 
			PolicyExtensionDimId,
			FutureAutomaticRenewalFlag,
			CustomerCarePolicyFutureAutomaticRenewalFlag,
			EDWPolicyAKId
		FROM PolicyExtensionDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWPolicyAKId ORDER BY PolicyExtensionDimId) = 1
),
EXP_InsertOrUpdate AS (
	SELECT
	LKP_PolicyExtensionDim.PolicyExtensionDimId AS lkp_PolicyExtensionDimId,
	LKP_PolicyExtensionDim.FutureAutomaticRenewalFlag AS lkp_FutureAutomaticRenewalFlag,
	LKP_PolicyExtensionDim.CustomerCarePolicyFutureAutomaticRenewalFlag AS lkp_CustomerCarePolicyFutureAutomaticRenewalFlag,
	-- *INF*: IIF(lkp_FutureAutomaticRenewalFlag='T','1','0')
	IFF(lkp_FutureAutomaticRenewalFlag = 'T', '1', '0') AS v_lkp_FutureAutomaticRenewalFlag,
	-- *INF*: IIF(lkp_CustomerCarePolicyFutureAutomaticRenewalFlag='T','1','0')
	IFF(lkp_CustomerCarePolicyFutureAutomaticRenewalFlag = 'T', '1', '0') AS v_lkp_CustomerCarePolicyFutureAutomaticRenewalFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	EXP_DefaultValues.SourceSystemId,
	SYSDATE AS o_CreateDate,
	SYSDATE AS o_ModifiedDate,
	EXP_DefaultValues.PolicyAKId,
	EXP_DefaultValues.o_FutureAutomaticRenewalFlag AS FutureAutomaticRenewalFlag,
	EXP_DefaultValues.o_CustomerCarePolicyFutureAutomaticRenewalFlag AS CustomerCarePolicyFutureAutomaticRenewalFlag,
	-- *INF*: DECODE(TRUE,
	-- lkp_PolicyExtensionDimId=-1,'NEW',
	-- v_lkp_FutureAutomaticRenewalFlag != FutureAutomaticRenewalFlag,'UPDATE',
	-- v_lkp_CustomerCarePolicyFutureAutomaticRenewalFlag != CustomerCarePolicyFutureAutomaticRenewalFlag,'UPDATE',
	-- 'NOCHANGE'
	-- )
	DECODE(TRUE,
		lkp_PolicyExtensionDimId = - 1, 'NEW',
		v_lkp_FutureAutomaticRenewalFlag != FutureAutomaticRenewalFlag, 'UPDATE',
		v_lkp_CustomerCarePolicyFutureAutomaticRenewalFlag != CustomerCarePolicyFutureAutomaticRenewalFlag, 'UPDATE',
		'NOCHANGE') AS o_ChangeFlag
	FROM EXP_DefaultValues
	LEFT JOIN LKP_PolicyExtensionDim
	ON LKP_PolicyExtensionDim.EDWPolicyAKId = EXP_DefaultValues.PolicyAKId
),
RTRTRANS AS (
	SELECT
	lkp_PolicyExtensionDimId AS PolicyExtensionDimId,
	o_AuditId AS AuditId,
	SourceSystemId,
	o_CreateDate AS CreateDate,
	o_ModifiedDate AS ModifiedDate,
	PolicyAKId,
	FutureAutomaticRenewalFlag,
	CustomerCarePolicyFutureAutomaticRenewalFlag,
	o_ChangeFlag AS ChangeFlag
	FROM EXP_InsertOrUpdate
),
RTRTRANS_INSERT AS (SELECT * FROM RTRTRANS WHERE ChangeFlag='NEW'),
RTRTRANS_UPDATE AS (SELECT * FROM RTRTRANS WHERE ChangeFlag='UPDATE'),
UPDTRANS AS (
	SELECT
	PolicyExtensionDimId, 
	ModifiedDate, 
	FutureAutomaticRenewalFlag, 
	CustomerCarePolicyFutureAutomaticRenewalFlag AS CustomerCarePolicyFutureAutomaticRenewalFlag3
	FROM RTRTRANS_UPDATE
),
TGT_PolicyExtensionDim_UPDATE AS (
	MERGE INTO PolicyExtensionDim AS T
	USING UPDTRANS AS S
	ON T.PolicyExtensionDimId = S.PolicyExtensionDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.FutureAutomaticRenewalFlag = S.FutureAutomaticRenewalFlag, T.CustomerCarePolicyFutureAutomaticRenewalFlag = S.CustomerCarePolicyFutureAutomaticRenewalFlag3
),
TGT_PolicyExtensionDim_INSERT AS (
	INSERT INTO PolicyExtensionDim
	(AuditId, SourceSystemId, CreatedDate, ModifiedDate, EDWPolicyAKId, FutureAutomaticRenewalFlag, CustomerCarePolicyFutureAutomaticRenewalFlag)
	SELECT 
	AUDITID, 
	SOURCESYSTEMID, 
	CreateDate AS CREATEDDATE, 
	MODIFIEDDATE, 
	PolicyAKId AS EDWPOLICYAKID, 
	FUTUREAUTOMATICRENEWALFLAG, 
	CUSTOMERCAREPOLICYFUTUREAUTOMATICRENEWALFLAG
	FROM RTRTRANS_INSERT
),
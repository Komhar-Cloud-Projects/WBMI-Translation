WITH
SQ_DCPolicyStaging AS (
	SELECT DISTINCT DCP.PolicyNumber
		,WBP.PolicyVersionFormatted
		,ISNULL(DCT.TransactionDate, DCT.CreatedDate) TransactionDate
		,DCT.EffectiveDate
		,DCT.Type
		,Doc.Caption
		,REPLACE(Doc.FormName,'_','') as FormName
		,Doc.Selected
		,Doc.OnPolicy
		,Doc.[Add]
		,Doc.[Remove]
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCPolicyStaging DCP
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBPolicyStaging WBP ON WBP.policyid = DCP.PolicyId
		AND WBP.SessionId = DCP.SessionId	
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCTransactionStaging DCT ON DCT.SessionId = WBP.SessionId	
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCSessionStaging DSS ON DSS.SessionId = DCP.SessionId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCLPrintDocStage Doc ON Doc.SessionId = DCP.SessionId
	WHERE DSS.Purpose = 'Onset'
	AND DCT.state = 'committed' AND DCP.Status<>'Quote'
	AND Doc.FormName @{pipeline().parameters.EXCLUDE_FORM}
	AND DCT.Type @{pipeline().parameters.EXCLUDE_TTYPE}
),
EXP_Default AS (
	SELECT
	PolicyNumber,
	PolicyVersionFormatted,
	TransactionDate,
	EffectiveDate,
	Type AS TransactionType,
	Caption,
	FormName,
	Selected,
	OnPolicy,
	Add,
	Remove,
	-- *INF*: LTRIM(RTRIM(FormName))
	LTRIM(RTRIM(FormName)) AS v_FormNumber,
	-- *INF*: REG_REPLACE(v_FormNumber,'[^0-9]','')
	REG_REPLACE(v_FormNumber, '[^0-9]', '') AS v_FormNumberRemoveChar,
	-- *INF*: SUBSTR(LTRIM(RTRIM(v_FormNumberRemoveChar)),
	-- LENGTH(LTRIM(RTRIM(v_FormNumberRemoveChar)))-3,
	-- 4)
	SUBSTR(LTRIM(RTRIM(v_FormNumberRemoveChar)), LENGTH(LTRIM(RTRIM(v_FormNumberRemoveChar))) - 3, 4) AS v_FormEditionDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	sysdate AS o_CreatedDate,
	sysdate AS o_ModifiedDate,
	-- *INF*: LTRIM(RTRIM(PolicyNumber)) || LTRIM(RTRIM(PolicyVersionFormatted))
	LTRIM(RTRIM(PolicyNumber)) || LTRIM(RTRIM(PolicyVersionFormatted)) AS o_pol_key,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(Caption)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(Caption) AS o_FormName,
	-- *INF*: LTRIM(RTRIM(FormName))
	LTRIM(RTRIM(FormName)) AS o_FormNumber,
	-- *INF*: TO_DATE(LPAD(v_FormEditionDate,4,'0'),'MMRR')
	TO_DATE(LPAD(v_FormEditionDate, 4, '0'), 'MMRR') AS o_FormEditionDate,
	-- *INF*: DECODE(TRUE,
	-- Add='1',
	-- 			DECODE(TRUE,			
	-- 			Remove='1' ,'0',
	-- 			'1'
	-- 			),
	-- ISNULL(Add), 
	-- 					DECODE(TRUE,
	-- 					ISNULL(OnPolicy) AND ISNULL(Remove), 						NULL,	
	-- 					OnPolicy ='1' AND ISNULL(Remove),'1',
	-- 					'0'
	-- 					 ),
	-- Add='0',
	-- 			DECODE(TRUE,	
	-- 			ISNULL(OnPolicy) AND ISNULL(Remove), 						NULL,	
	-- 			OnPolicy ='1' AND Remove ='1', '0',	
	-- 			OnPolicy ='1' AND ISNULL(Remove), '1',
	-- 			OnPolicy ='1' AND Remove ='0', '1',			
	-- 			 '0'
	-- 			),
	-- '0'
	-- )
	DECODE(TRUE,
		Add = '1', DECODE(TRUE,
		Remove = '1', '0',
		'1'),
		Add IS NULL, DECODE(TRUE,
		OnPolicy IS NULL AND Remove IS NULL, NULL,
		OnPolicy = '1' AND Remove IS NULL, '1',
		'0'),
		Add = '0', DECODE(TRUE,
		OnPolicy IS NULL AND Remove IS NULL, NULL,
		OnPolicy = '1' AND Remove = '1', '0',
		OnPolicy = '1' AND Remove IS NULL, '1',
		OnPolicy = '1' AND Remove = '0', '1',
		'0'),
		'0') AS o_AddRemoveFlag
	FROM SQ_DCPolicyStaging
),
LKP_Form AS (
	SELECT
	FormId,
	FormName,
	FormNumber,
	FormEditionDate
	FROM (
		SELECT 
			FormId,
			FormName,
			FormNumber,
			FormEditionDate
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Form
		WHERE SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY FormName,FormNumber,FormEditionDate ORDER BY FormId) = 1
),
LKP_Policy AS (
	SELECT
	pol_ak_id,
	pol_key,
	pol_num,
	eff_from_date,
	eff_to_date
	FROM (
		SELECT 
			pol_ak_id,
			pol_key,
			pol_num,
			eff_from_date,
			eff_to_date
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy
		WHERE source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key,pol_num,eff_from_date,eff_to_date ORDER BY pol_ak_id DESC) = 1
),
LKP_PolicyForm AS (
	SELECT
	PolicyFormId,
	PolicyAKID,
	FormID,
	FormTransactionCreatedDate
	FROM (
		SELECT @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyForm.PolicyFormId as PolicyFormId, @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyForm.PolicyAKID as PolicyAKID, @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyForm.FormID as FormID, @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyForm.FormTransactionCreatedDate as FormTransactionCreatedDate FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyForm 
		INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy ON pol_ak_id = PolicyAKID
		WHERE SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		AND exists (select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WBPolicyStaging
		where (PolicyNumber + PolicyVersionFormatted) = pol_key)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID,FormID,FormTransactionCreatedDate ORDER BY PolicyFormId) = 1
),
EXP_No_NULL AS (
	SELECT
	LKP_PolicyForm.PolicyFormId,
	EXP_Default.o_AuditID AS AuditID,
	EXP_Default.o_SourceSystemID AS SourceSystemID,
	EXP_Default.o_CreatedDate AS CreatedDate,
	EXP_Default.o_ModifiedDate AS ModifiedDate,
	LKP_Policy.pol_ak_id AS in_PolicyAKID,
	-- *INF*: DECODE(TRUE,ISNULL(in_PolicyAKID),-1,in_PolicyAKID)
	DECODE(TRUE,
		in_PolicyAKID IS NULL, - 1,
		in_PolicyAKID) AS o_PolicyAKID,
	LKP_Form.FormId AS in_FormID,
	-- *INF*: DECODE(TRUE,ISNULL(in_FormID),-1,in_FormID)
	DECODE(TRUE,
		in_FormID IS NULL, - 1,
		in_FormID) AS o_FormID,
	EXP_Default.TransactionDate,
	EXP_Default.EffectiveDate,
	EXP_Default.TransactionType,
	EXP_Default.o_AddRemoveFlag AS AddRemoveFlag,
	-- *INF*: MAKE_DATE_TIME(1800,01,01)
	MAKE_DATE_TIME(1800, 01, 01) AS FormAddedDate
	FROM EXP_Default
	LEFT JOIN LKP_Form
	ON LKP_Form.FormName = EXP_Default.o_FormName AND LKP_Form.FormNumber = EXP_Default.o_FormNumber AND LKP_Form.FormEditionDate = EXP_Default.o_FormEditionDate
	LEFT JOIN LKP_Policy
	ON LKP_Policy.pol_key = EXP_Default.o_pol_key AND LKP_Policy.pol_num = EXP_Default.PolicyNumber AND LKP_Policy.eff_from_date <= EXP_Default.TransactionDate AND LKP_Policy.eff_to_date >= EXP_Default.TransactionDate
	LEFT JOIN LKP_PolicyForm
	ON LKP_PolicyForm.PolicyAKID = LKP_Policy.pol_ak_id AND LKP_PolicyForm.FormID = LKP_Form.FormId AND LKP_PolicyForm.FormTransactionCreatedDate = EXP_Default.TransactionDate
),
FIL_Existing AS (
	SELECT
	PolicyFormId, 
	AuditID, 
	SourceSystemID, 
	CreatedDate, 
	ModifiedDate, 
	o_PolicyAKID AS PolicyAKID, 
	o_FormID AS FormID, 
	TransactionDate, 
	EffectiveDate, 
	TransactionType, 
	AddRemoveFlag, 
	FormAddedDate
	FROM EXP_No_NULL
	WHERE ISNULL(PolicyFormId) AND NOT ISNULL(AddRemoveFlag)
),
PolicyForm AS (
	INSERT INTO PolicyForm
	(AuditId, SourceSystemID, CreatedDate, ModifiedDate, PolicyAKID, FormID, FormTransactionCreatedDate, FormTransactionEffectiveDate, FormTransactionType, FormAddRemoveFlag, FormAddedDate)
	SELECT 
	AuditID AS AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	POLICYAKID, 
	FORMID, 
	TransactionDate AS FORMTRANSACTIONCREATEDDATE, 
	EffectiveDate AS FORMTRANSACTIONEFFECTIVEDATE, 
	TransactionType AS FORMTRANSACTIONTYPE, 
	AddRemoveFlag AS FORMADDREMOVEFLAG, 
	FORMADDEDDATE
	FROM FIL_Existing
),
SQ_PolicyForm_Update_FormAddedDate AS (
	select
	temp1.PolicyFormId,
	case
	when temp2.rnk2 is null then temp1.FormTransactionCreatedDate
	when temp2.FormAddRemoveFlag = 0 then temp1.FormTransactionCreatedDate
	else NULL
	end 'FormAddedDate'
	from
	(select PolicyAKID , FormId, PolicyFormId, FormTransactionCreatedDate, FormAddRemoveFlag,FormAddedDate,
	ROW_NUMBER() over (partition by policyakid, formid order by FormTransactionCreatedDate) as rnk1 
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyForm
	where policyAKID in (select distinct policyAKID from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyForm where AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and PolicyAKID <> -1)
	) temp1
	left join 
	(select PolicyAKID, formid, PolicyFormId, FormTransactionCreatedDate, FormAddRemoveFlag,
	ROW_NUMBER() over (partition by policyakid, formid order by FormTransactionCreatedDate) as rnk2
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyForm
	where policyAKID in (select distinct policyAKID from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyForm where AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} 
	and PolicyAKID <> -1)
	) temp2
	on temp1.rnk1 = temp2.rnk2 + 1
	and temp1.PolicyAKID = temp2.PolicyAKID
	and temp1.FormID = temp2.FormID
),
EXP_Default_Place_Holder AS (
	SELECT
	PolicyFormId,
	FormAddedDate,
	sysdate AS ModifiedDate
	FROM SQ_PolicyForm_Update_FormAddedDate
),
FIL_Remove_NULL_Rows AS (
	SELECT
	PolicyFormId, 
	FormAddedDate, 
	ModifiedDate
	FROM EXP_Default_Place_Holder
	WHERE NOT ISNULL(FormAddedDate)
),
UPD_Update_Table AS (
	SELECT
	PolicyFormId, 
	FormAddedDate, 
	ModifiedDate
	FROM FIL_Remove_NULL_Rows
),
PolicyForm_Update_FormAddedDate AS (
	MERGE INTO PolicyForm AS T
	USING UPD_Update_Table AS S
	ON T.PolicyFormId = S.PolicyFormId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.FormAddedDate = S.FormAddedDate
),
SQ_PolicyForm_Update_FormAddedDate1 AS (
	SELECT policyformid , FormAddedDate
	FROM
	(SELECT  policyformid ,
	case
	when FormAddedDate = '01-Jan-1800' then (select top 1 FormAddedDate from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyForm where FormTransactionCreatedDate <= t.FormTransactionCreatedDate and FormAddedDate <> '01-Jan-1800'
	and policyakid = t.policyakid and formid = t.formid
	order by FormTransactionCreatedDate desc)
	else NULL
	end as  'FormAddedDate'
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyForm t
	WHERE PolicyAKID <> -1 AND policyakid IN (SELECT DISTINCT policyakid FROM DBO.PolicyForm WHERE modifieddate > '@{pipeline().parameters.SELECTION_START_TS}' ) ) A
	WHERE FormAddedDate is not null
),
EXP_Default_Place_Holder1 AS (
	SELECT
	PolicyFormId,
	FormAddedDate,
	sysdate AS ModifiedDate
	FROM SQ_PolicyForm_Update_FormAddedDate1
),
FIL_Remove_NULL_Rows1 AS (
	SELECT
	PolicyFormId, 
	FormAddedDate, 
	ModifiedDate
	FROM EXP_Default_Place_Holder1
	WHERE NOT ISNULL(FormAddedDate)
),
UPD_Update_Table1 AS (
	SELECT
	PolicyFormId, 
	FormAddedDate, 
	ModifiedDate
	FROM FIL_Remove_NULL_Rows1
),
PolicyForm_Update_FormAddedDate1 AS (
	MERGE INTO PolicyForm AS T
	USING UPD_Update_Table1 AS S
	ON T.PolicyFormId = S.PolicyFormId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.FormAddedDate = S.FormAddedDate
),
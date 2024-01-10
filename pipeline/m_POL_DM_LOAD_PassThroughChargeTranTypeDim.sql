WITH
SQ_PassThroughChargeTransaction AS (
	SELECT DISTINCT
	PTCT.SourceSystemID as SourceSystemID,
	LTRIM(RTRIM(PTCT.ReasonAmendedCode)) as ReasonAmendedCode, 
	PTCT.PassThroughChargeTransactionCodeId as PassThroughChargeTransactionCodeId,
	PTCT.SupPassThroughChargeTypeID as SupPassThroughChargeTypeID 
	FROM PassThroughChargeTransaction PTCT
	WHERE PTCT.CREATEDDATE>='@{pipeline().parameters.SELECTION_START_TS}'
),
exp_get_data AS (
	SELECT
	SourceSystemID,
	ReasonAmendedCode,
	PassThroughChargeTransactionCodeId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: TO_DATE('1/1/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('1/1/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	1 AS CurrentSnapshotFlag,
	SupPassThroughChargeTypeID
	FROM SQ_PassThroughChargeTransaction
),
lkp_SupPassThroughChargeType AS (
	SELECT
	PassThroughChargeType,
	SupPassThroughChargeTypeID
	FROM (
		SELECT 
			PassThroughChargeType,
			SupPassThroughChargeTypeID
		FROM SupPassThroughChargeType
		WHERE CurrentSnapShotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SupPassThroughChargeTypeID ORDER BY PassThroughChargeType DESC) = 1
),
lkp_sup_premim_transaction_code AS (
	SELECT
	StandardPremiumTransactionCode,
	StandardPremiumTransactionCodeDescription,
	prem_trans_type_descript,
	sup_prem_trans_code_id
	FROM (
		SELECT
		sup_premium_transaction_code.sup_prem_trans_code_id as sup_prem_trans_code_id,
		sup_premium_transaction_code.StandardPremiumTransactionCode as StandardPremiumTransactionCode, 
		sup_premium_transaction_code.prem_trans_type_descript as prem_trans_type_descript, 
		sup_premium_transaction_code.StandardPremiumTransactionCodeDescription as StandardPremiumTransactionCodeDescription 
		FROM sup_premium_transaction_code
		where sup_premium_transaction_code.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_prem_trans_code_id ORDER BY StandardPremiumTransactionCode DESC) = 1
),
lkp_sup_reason_amended_code AS (
	SELECT
	StandardReasonAmendedDescription,
	StandardReasonAmendedCode,
	rsn_amended_code,
	source_sys_id
	FROM (
		SELECT 
			StandardReasonAmendedDescription,
			StandardReasonAmendedCode,
			rsn_amended_code,
			source_sys_id
		FROM sup_reason_amended_code
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY rsn_amended_code,source_sys_id ORDER BY StandardReasonAmendedDescription) = 1
),
Exp_PassThroughTransTypeDim AS (
	SELECT
	lkp_sup_premim_transaction_code.StandardPremiumTransactionCode AS lkp_StandardPremiumTransactionCode,
	-- *INF*: IIF(ISNULL(lkp_StandardPremiumTransactionCode) or IS_SPACES(lkp_StandardPremiumTransactionCode) or LENGTH(lkp_StandardPremiumTransactionCode)=0,'N/A',LTRIM(RTRIM(lkp_StandardPremiumTransactionCode)))
	IFF(lkp_StandardPremiumTransactionCode IS NULL OR IS_SPACES(lkp_StandardPremiumTransactionCode) OR LENGTH(lkp_StandardPremiumTransactionCode) = 0, 'N/A', LTRIM(RTRIM(lkp_StandardPremiumTransactionCode))) AS StandardPremiumTransactionCode,
	lkp_sup_premim_transaction_code.StandardPremiumTransactionCodeDescription AS lkp_StandardPremiumTransactionCodeDescription,
	-- *INF*: iif(isnull(lkp_StandardPremiumTransactionCodeDescription) or IS_SPACES(lkp_StandardPremiumTransactionCodeDescription) or LENGTH(lkp_StandardPremiumTransactionCodeDescription)=0,'N/A',LTRIM(RTRIM(lkp_StandardPremiumTransactionCodeDescription)))
	IFF(lkp_StandardPremiumTransactionCodeDescription IS NULL OR IS_SPACES(lkp_StandardPremiumTransactionCodeDescription) OR LENGTH(lkp_StandardPremiumTransactionCodeDescription) = 0, 'N/A', LTRIM(RTRIM(lkp_StandardPremiumTransactionCodeDescription))) AS StandardPremiumTransactionCodeDescription,
	lkp_sup_premim_transaction_code.prem_trans_type_descript AS lkp_prem_trans_type_descript,
	-- *INF*: iif(isnull(lkp_prem_trans_type_descript) or IS_SPACES(lkp_prem_trans_type_descript) or LENGTH(lkp_prem_trans_type_descript)=0,'N/A',LTRIM(RTRIM(lkp_prem_trans_type_descript)))
	IFF(lkp_prem_trans_type_descript IS NULL OR IS_SPACES(lkp_prem_trans_type_descript) OR LENGTH(lkp_prem_trans_type_descript) = 0, 'N/A', LTRIM(RTRIM(lkp_prem_trans_type_descript))) AS prem_trans_type_descript,
	lkp_sup_reason_amended_code.StandardReasonAmendedDescription AS lkp_rsn_amended_code_descript,
	-- *INF*: iif(isnull(lkp_rsn_amended_code_descript) or IS_SPACES(lkp_rsn_amended_code_descript) or LENGTH(lkp_rsn_amended_code_descript)=0,'N/A',LTRIM(RTRIM(lkp_rsn_amended_code_descript)))
	IFF(lkp_rsn_amended_code_descript IS NULL OR IS_SPACES(lkp_rsn_amended_code_descript) OR LENGTH(lkp_rsn_amended_code_descript) = 0, 'N/A', LTRIM(RTRIM(lkp_rsn_amended_code_descript))) AS rsn_amended_code_descript,
	lkp_sup_reason_amended_code.StandardReasonAmendedCode AS lkp_StandardReasonAmendedCode,
	-- *INF*: iif(isnull(lkp_StandardReasonAmendedCode) or IS_SPACES(lkp_StandardReasonAmendedCode) or LENGTH(lkp_StandardReasonAmendedCode)=0,'N/A',LTRIM(RTRIM(lkp_StandardReasonAmendedCode)))
	IFF(lkp_StandardReasonAmendedCode IS NULL OR IS_SPACES(lkp_StandardReasonAmendedCode) OR LENGTH(lkp_StandardReasonAmendedCode) = 0, 'N/A', LTRIM(RTRIM(lkp_StandardReasonAmendedCode))) AS ReasonAmendedCode,
	exp_get_data.AuditID,
	exp_get_data.EffectiveDate,
	exp_get_data.ExpirationDate,
	exp_get_data.CurrentSnapshotFlag,
	lkp_SupPassThroughChargeType.PassThroughChargeType AS lkp_PassThroughChargeType,
	-- *INF*: iif(isnull(lkp_PassThroughChargeType) or IS_SPACES(lkp_PassThroughChargeType) or LENGTH(lkp_PassThroughChargeType)=0,'N/A',LTRIM(RTRIM(lkp_PassThroughChargeType)))
	IFF(lkp_PassThroughChargeType IS NULL OR IS_SPACES(lkp_PassThroughChargeType) OR LENGTH(lkp_PassThroughChargeType) = 0, 'N/A', LTRIM(RTRIM(lkp_PassThroughChargeType))) AS PassThroughChargeType
	FROM exp_get_data
	LEFT JOIN lkp_SupPassThroughChargeType
	ON lkp_SupPassThroughChargeType.SupPassThroughChargeTypeID = exp_get_data.SupPassThroughChargeTypeID
	LEFT JOIN lkp_sup_premim_transaction_code
	ON lkp_sup_premim_transaction_code.sup_prem_trans_code_id = exp_get_data.PassThroughChargeTransactionCodeId
	LEFT JOIN lkp_sup_reason_amended_code
	ON lkp_sup_reason_amended_code.rsn_amended_code = exp_get_data.ReasonAmendedCode AND lkp_sup_reason_amended_code.source_sys_id = exp_get_data.SourceSystemID
),
lkp_PassThroughChargeTransTypeDim AS (
	SELECT
	PassThroughChargeTransactionTypeDimID,
	PassThroughChargeTransactionCode,
	ReasonAmendedCode,
	PassThroughChargeType
	FROM (
		SELECT 
			PassThroughChargeTransactionTypeDimID,
			PassThroughChargeTransactionCode,
			ReasonAmendedCode,
			PassThroughChargeType
		FROM PassThroughChargeTransactionTypeDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PassThroughChargeTransactionCode,ReasonAmendedCode,PassThroughChargeType ORDER BY PassThroughChargeTransactionTypeDimID DESC) = 1
),
fil_new_records AS (
	SELECT
	lkp_PassThroughChargeTransTypeDim.PassThroughChargeTransactionTypeDimID AS lkp_PassThroughChargeTransactionTypeDimID, 
	Exp_PassThroughTransTypeDim.CurrentSnapshotFlag, 
	Exp_PassThroughTransTypeDim.AuditID, 
	Exp_PassThroughTransTypeDim.EffectiveDate, 
	Exp_PassThroughTransTypeDim.ExpirationDate, 
	Exp_PassThroughTransTypeDim.StandardPremiumTransactionCode, 
	Exp_PassThroughTransTypeDim.StandardPremiumTransactionCodeDescription, 
	Exp_PassThroughTransTypeDim.prem_trans_type_descript, 
	Exp_PassThroughTransTypeDim.ReasonAmendedCode, 
	Exp_PassThroughTransTypeDim.rsn_amended_code_descript, 
	Exp_PassThroughTransTypeDim.PassThroughChargeType
	FROM Exp_PassThroughTransTypeDim
	LEFT JOIN lkp_PassThroughChargeTransTypeDim
	ON lkp_PassThroughChargeTransTypeDim.PassThroughChargeTransactionCode = Exp_PassThroughTransTypeDim.StandardPremiumTransactionCode AND lkp_PassThroughChargeTransTypeDim.ReasonAmendedCode = Exp_PassThroughTransTypeDim.ReasonAmendedCode AND lkp_PassThroughChargeTransTypeDim.PassThroughChargeType = Exp_PassThroughTransTypeDim.PassThroughChargeType
	WHERE ISNULL(lkp_PassThroughChargeTransactionTypeDimID)
),
agg_PassThroughTypeDim AS (
	SELECT
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	StandardPremiumTransactionCode,
	StandardPremiumTransactionCodeDescription,
	prem_trans_type_descript,
	ReasonAmendedCode,
	rsn_amended_code_descript,
	PassThroughChargeType
	FROM fil_new_records
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StandardPremiumTransactionCode, ReasonAmendedCode, PassThroughChargeType ORDER BY NULL) = 1
),
exp_Pass_through AS (
	SELECT
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	StandardPremiumTransactionCode,
	StandardPremiumTransactionCodeDescription,
	prem_trans_type_descript,
	ReasonAmendedCode,
	rsn_amended_code_descript,
	PassThroughChargeType
	FROM agg_PassThroughTypeDim
),
PassThroughChargeTransactionTypeDim AS (
	INSERT INTO Shortcut_to_PassThroughChargeTransactionTypeDim
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, PassThroughChargeTransactionCode, PassThroughChargeTransactionCodeDescription, PassThroughChargeTransactionTypeDescription, ReasonAmendedCode, ReasonAmendedCodeDescription, PassThroughChargeType)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	StandardPremiumTransactionCode AS PASSTHROUGHCHARGETRANSACTIONCODE, 
	StandardPremiumTransactionCodeDescription AS PASSTHROUGHCHARGETRANSACTIONCODEDESCRIPTION, 
	prem_trans_type_descript AS PASSTHROUGHCHARGETRANSACTIONTYPEDESCRIPTION, 
	REASONAMENDEDCODE, 
	rsn_amended_code_descript AS REASONAMENDEDCODEDESCRIPTION, 
	PASSTHROUGHCHARGETYPE
	FROM exp_Pass_through
),
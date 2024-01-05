WITH
SQ_PremiumTransaction AS (
	(SELECT DISTINCT
	PT.SourceSystemID,
	LTRIM(RTRIM(PT.PremiumType)), 
	LTRIM(RTRIM(PT.ReasonAmendedCode)), 
	PT.SupPremiumTransactionCodeId,
	0 as CustomerCareCommissionRate
	FROM
	PremiumTransaction PT
	WHERE PT.CREATEDDATE>='@{pipeline().parameters.SELECTION_START_TS}' and PT.SourceSystemID='PMS'
	@{pipeline().parameters.WHERE_CLAUSE_PMS}
	
	UNION ALL
	
	SELECT DISTINCT
	PT.SourceSystemID,
	LTRIM(RTRIM(PT.PremiumType)), 
	LTRIM(RTRIM(PT.ReasonAmendedCode)), 
	PT.SupPremiumTransactionCodeId,
	POLCOV.CustomerCareCommissionRate
	FROM
	PremiumTransaction PT
	INNER JOIN RatingCoverage RC
	ON PT.RatingCoverageAKID=RC.RatingCoverageAKID
	AND PT.EffectiveDate=RC.EffectiveDate
	INNER JOIN PolicyCoverage POLCOV
	ON RC.PolicyCoverageAKID=POLCOV.PolicyCoverageAKID AND POLCOV.CurrentSnapshotFlag=1
	WHERE PT.CREATEDDATE>='@{pipeline().parameters.SELECTION_START_TS}' and PT.SourceSystemID='DCT'
	@{pipeline().parameters.WHERE_CLAUSE_DCT}
	)
	
	UNION
	
	SELECT DISTINCT
	PT.SourceSystemID,
	'C', 
	LTRIM(RTRIM(PT.ReasonAmendedCode)),
	PT.SupPremiumTransactionCodeId,
	0 as CustomerCareCommissionRate
	FROM
	PremiumTransaction PT INNER JOIN StatisticalCoverage SC
	ON PT.StatisticalCoverageAKID=SC.StatisticalCoverageAKID
	WHERE
	PT.CREATEDDATE>='@{pipeline().parameters.SELECTION_START_TS}' and 
	SC.MajorPerilCode='050'
	@{pipeline().parameters.WHERE_CLAUSE_PMS}
),
exp_Collect_Data_columns AS (
	SELECT
	SourceSystemID,
	PremiumType,
	ReasonAmendedCode,
	SupPremiumTransactionCodeId,
	CustomerCareCommissionRate,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	SYSDATE AS createddate,
	SYSDATE AS modifieddate,
	-- *INF*: TO_DATE('1/1/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('1/1/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate
	FROM SQ_PremiumTransaction
),
lkp_sup_premium_transaction_code AS (
	SELECT
	prem_trans_code_descript,
	prem_trans_type_descript,
	StandardPremiumTransactionCode,
	sup_prem_trans_code_id
	FROM (
		SELECT
		SPTC. sup_prem_trans_code_id as sup_prem_trans_code_id,
		SPTC.prem_trans_code_descript as prem_trans_code_descript, 
		SPTC.prem_trans_type_descript as prem_trans_type_descript, 
		SPTC.StandardPremiumTransactionCode as StandardPremiumTransactionCode
		FROM sup_premium_transaction_code SPTC
		where SPTC.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_prem_trans_code_id ORDER BY prem_trans_code_descript DESC) = 1
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
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_reason_amended_code
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY rsn_amended_code,source_sys_id ORDER BY StandardReasonAmendedDescription) = 1
),
Exp_Collect_LookupData AS (
	SELECT
	lkp_sup_premium_transaction_code.prem_trans_code_descript AS lkp_prem_trans_code_descript,
	-- *INF*: iif(isnull(lkp_prem_trans_code_descript) or IS_SPACES(lkp_prem_trans_code_descript) or LENGTH(lkp_prem_trans_code_descript)=0,'N/A',LTRIM(RTRIM(lkp_prem_trans_code_descript)))
	IFF(lkp_prem_trans_code_descript IS NULL OR IS_SPACES(lkp_prem_trans_code_descript) OR LENGTH(lkp_prem_trans_code_descript) = 0, 'N/A', LTRIM(RTRIM(lkp_prem_trans_code_descript))) AS o_lkp_prem_trans_code_descript,
	lkp_sup_premium_transaction_code.prem_trans_type_descript AS lkp_prem_trans_type_descript,
	-- *INF*: iif(isnull(lkp_prem_trans_type_descript) or IS_SPACES(lkp_prem_trans_type_descript) or LENGTH(lkp_prem_trans_type_descript)=0,'N/A',LTRIM(RTRIM(lkp_prem_trans_type_descript)))
	IFF(lkp_prem_trans_type_descript IS NULL OR IS_SPACES(lkp_prem_trans_type_descript) OR LENGTH(lkp_prem_trans_type_descript) = 0, 'N/A', LTRIM(RTRIM(lkp_prem_trans_type_descript))) AS o_lkp_prem_trans_type_descript,
	lkp_sup_reason_amended_code.StandardReasonAmendedDescription AS lkp_rsn_amended_code_descript,
	-- *INF*: iif(isnull(lkp_rsn_amended_code_descript) or IS_SPACES(lkp_rsn_amended_code_descript) or LENGTH(lkp_rsn_amended_code_descript)=0,'N/A',LTRIM(RTRIM(lkp_rsn_amended_code_descript)))
	IFF(lkp_rsn_amended_code_descript IS NULL OR IS_SPACES(lkp_rsn_amended_code_descript) OR LENGTH(lkp_rsn_amended_code_descript) = 0, 'N/A', LTRIM(RTRIM(lkp_rsn_amended_code_descript))) AS o_lkp_rsn_amended_code_descript,
	lkp_sup_premium_transaction_code.StandardPremiumTransactionCode AS lkp_StandardPremiumTransactionCode,
	-- *INF*: iif(isnull(lkp_StandardPremiumTransactionCode) or IS_SPACES(lkp_StandardPremiumTransactionCode) or LENGTH(lkp_StandardPremiumTransactionCode)=0,'N/A',LTRIM(RTRIM(lkp_StandardPremiumTransactionCode)))
	IFF(lkp_StandardPremiumTransactionCode IS NULL OR IS_SPACES(lkp_StandardPremiumTransactionCode) OR LENGTH(lkp_StandardPremiumTransactionCode) = 0, 'N/A', LTRIM(RTRIM(lkp_StandardPremiumTransactionCode))) AS o_PremiumTransactionCode,
	lkp_sup_reason_amended_code.StandardReasonAmendedCode AS lkp_StandardReasonAmendedCode,
	-- *INF*: IIF(ISNULL(lkp_StandardReasonAmendedCode) or IS_SPACES(lkp_StandardReasonAmendedCode) or LENGTH(lkp_StandardReasonAmendedCode)=0,'N/A',LTRIM(RTRIM(lkp_StandardReasonAmendedCode)))
	IFF(lkp_StandardReasonAmendedCode IS NULL OR IS_SPACES(lkp_StandardReasonAmendedCode) OR LENGTH(lkp_StandardReasonAmendedCode) = 0, 'N/A', LTRIM(RTRIM(lkp_StandardReasonAmendedCode))) AS o_ReasonAmendedCode,
	exp_Collect_Data_columns.PremiumType,
	exp_Collect_Data_columns.CurrentSnapshotFlag,
	exp_Collect_Data_columns.audit_id,
	exp_Collect_Data_columns.createddate,
	exp_Collect_Data_columns.modifieddate,
	exp_Collect_Data_columns.EffectiveDate,
	exp_Collect_Data_columns.ExpirationDate,
	exp_Collect_Data_columns.CustomerCareCommissionRate AS i_CustomerCareCommissionRate,
	i_CustomerCareCommissionRate AS o_CustomerCareCommissionRate
	FROM exp_Collect_Data_columns
	LEFT JOIN lkp_sup_premium_transaction_code
	ON lkp_sup_premium_transaction_code.sup_prem_trans_code_id = exp_Collect_Data_columns.SupPremiumTransactionCodeId
	LEFT JOIN lkp_sup_reason_amended_code
	ON lkp_sup_reason_amended_code.rsn_amended_code = exp_Collect_Data_columns.ReasonAmendedCode AND lkp_sup_reason_amended_code.source_sys_id = exp_Collect_Data_columns.SourceSystemID
),
lkp_PremiumTransactionTypeDim AS (
	SELECT
	PremiumTransactionTypeDimID,
	PremiumTransactionCode,
	ReasonAmendedCode,
	PremiumTypeCode,
	CustomerCareCommissionRate
	FROM (
		SELECT 
		PTTD.PremiumTransactionTypeDimID as PremiumTransactionTypeDimID,
		LTRIM(RTRIM(PTTD.PremiumTransactionCode)) as PremiumTransactionCode, 
		LTRIM(RTRIM(PTTD.ReasonAmendedCode)) as ReasonAmendedCode, 
		LTRIM(RTRIM(PTTD.PremiumTypeCode)) as PremiumTypeCode ,
		CustomerCareCommissionRate as CustomerCareCommissionRate
		FROM PremiumTransactionTypeDim PTTD
		where PTTD.CurrentSnapShotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionCode,ReasonAmendedCode,PremiumTypeCode,CustomerCareCommissionRate ORDER BY PremiumTransactionTypeDimID DESC) = 1
),
fil_PremiumTransactionTypeDim AS (
	SELECT
	lkp_PremiumTransactionTypeDim.PremiumTransactionTypeDimID AS lkp_PremiumTransactionTypeDimID, 
	lkp_PremiumTransactionTypeDim.PremiumTransactionCode AS lkp_PremiumTransactionCode, 
	lkp_PremiumTransactionTypeDim.ReasonAmendedCode AS lkp_ReasonAmendedCode, 
	lkp_PremiumTransactionTypeDim.PremiumTypeCode AS lkp_PremiumTypeCode, 
	Exp_Collect_LookupData.o_lkp_prem_trans_code_descript, 
	Exp_Collect_LookupData.o_lkp_prem_trans_type_descript, 
	Exp_Collect_LookupData.o_lkp_rsn_amended_code_descript, 
	Exp_Collect_LookupData.o_PremiumTransactionCode AS PremiumTransactionCode, 
	Exp_Collect_LookupData.o_ReasonAmendedCode AS ReasonAmendedCode, 
	Exp_Collect_LookupData.PremiumType, 
	Exp_Collect_LookupData.CurrentSnapshotFlag, 
	Exp_Collect_LookupData.audit_id, 
	Exp_Collect_LookupData.createddate, 
	Exp_Collect_LookupData.modifieddate, 
	Exp_Collect_LookupData.EffectiveDate, 
	Exp_Collect_LookupData.ExpirationDate, 
	Exp_Collect_LookupData.o_CustomerCareCommissionRate AS CustomerCareCommissionRate
	FROM Exp_Collect_LookupData
	LEFT JOIN lkp_PremiumTransactionTypeDim
	ON lkp_PremiumTransactionTypeDim.PremiumTransactionCode = Exp_Collect_LookupData.o_PremiumTransactionCode AND lkp_PremiumTransactionTypeDim.ReasonAmendedCode = Exp_Collect_LookupData.o_ReasonAmendedCode AND lkp_PremiumTransactionTypeDim.PremiumTypeCode = Exp_Collect_LookupData.PremiumType AND lkp_PremiumTransactionTypeDim.CustomerCareCommissionRate = Exp_Collect_LookupData.o_CustomerCareCommissionRate
	WHERE ISNULL(lkp_PremiumTransactionTypeDimID)
OR
(
(ISNULL(PremiumTransactionCode) AND ISNULL(lkp_ReasonAmendedCode) AND ISNULL(lkp_PremiumTypeCode))
AND
(PremiumTransactionCode='N/A' or ReasonAmendedCode<>'N/A' or lkp_PremiumTypeCode<>'N/A')
)
),
AGG_REM_Duplicates AS (
	SELECT
	lkp_PremiumTransactionCode, 
	lkp_ReasonAmendedCode, 
	lkp_PremiumTypeCode, 
	o_lkp_prem_trans_code_descript, 
	o_lkp_prem_trans_type_descript, 
	o_lkp_rsn_amended_code_descript, 
	PremiumTransactionCode, 
	ReasonAmendedCode, 
	PremiumType, 
	CurrentSnapshotFlag, 
	audit_id, 
	createddate, 
	modifieddate, 
	EffectiveDate, 
	ExpirationDate, 
	CustomerCareCommissionRate
	FROM fil_PremiumTransactionTypeDim
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionCode, ReasonAmendedCode, PremiumType, CustomerCareCommissionRate ORDER BY NULL) = 1
),
PremiumTransactionTypeDimINS AS (
	INSERT INTO Shortcut_to_PremiumTransactionTypeDim
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, PremiumTransactionCode, PremiumTransactionCodeDescription, PremiumTransactionTypeDescription, ReasonAmendedCode, ReasonAmendedCodeDescription, PremiumTypeCode, CustomerCareCommissionRate)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	audit_id AS AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	createddate AS CREATEDDATE, 
	modifieddate AS MODIFIEDDATE, 
	PREMIUMTRANSACTIONCODE, 
	o_lkp_prem_trans_code_descript AS PREMIUMTRANSACTIONCODEDESCRIPTION, 
	o_lkp_prem_trans_type_descript AS PREMIUMTRANSACTIONTYPEDESCRIPTION, 
	REASONAMENDEDCODE, 
	o_lkp_rsn_amended_code_descript AS REASONAMENDEDCODEDESCRIPTION, 
	PremiumType AS PREMIUMTYPECODE, 
	CUSTOMERCARECOMMISSIONRATE
	FROM AGG_REM_Duplicates
),
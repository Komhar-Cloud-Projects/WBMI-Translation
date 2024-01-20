WITH
SQ_PremiumMasterCalculation_StatisticalCoverage AS (
	SELECT DISTINCT RTRIM(PC.PremiumMasterTransactionCode) PremiumMasterTransactionCode,
	       RTRIM(PC.PremiumMasterPremiumType) PremiumMasterPremiumType,
	       RTRIM(PC.PremiumMasterReasonAmendedCode) PremiumMasterReasonAmendedCode,
	       PC.PremiumMasterCustomerCareCommissionRate
	FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation PC with (nolock)
	WHERE PC.PremiumMasterRunDate > '@{pipeline().parameters.SELECTION_START_TS}'
	
	UNION
	
	SELECT DISTINCT RTRIM(PC.PremiumMasterTransactionCode) PremiumMasterTransactionCode,
	'C' PremiumMasterPremiumType,
	 RTRIM(PC.PremiumMasterReasonAmendedCode) PremiumMasterReasonAmendedCode,
	1 as PremiumMasterCustomerCareCommissionRate
	FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation PC with (nolock),
	    @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC  with (nolock)
	WHERE
	      PC.StatisticalCoverageAKID = SC.StatisticalCoverageAKID
	       AND SC.CurrentSnapshotFlag =1 AND sc.MajorPerilCode='050' 
	       and PC.PremiumMasterRunDate > '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_Collect_Data_columns AS (
	SELECT
	PremiumMasterTransactionCode,
	-- *INF*: rtrim(PremiumMasterTransactionCode)
	rtrim(PremiumMasterTransactionCode) AS PremiumMasterTransactionCode_out,
	PremiumMasterPremiumType,
	-- *INF*: rtrim(PremiumMasterPremiumType)
	rtrim(PremiumMasterPremiumType) AS PremiumMasterPremiumType_out,
	PremiumMasterReasonAmendedCode AS ReasonAmendedCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(ReasonAmendedCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(ReasonAmendedCode) AS ReasonAmendedCode_out,
	PremiumMasterCustomerCareCommissionRate,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	SYSDATE AS createddate,
	SYSDATE AS modifieddate,
	-- *INF*: TO_DATE('1/1/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('1/1/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate
	FROM SQ_PremiumMasterCalculation_StatisticalCoverage
),
lkp_sup_premium_transaction_code AS (
	SELECT
	prem_trans_code_descript,
	prem_trans_type_descript,
	prem_trans_code
	FROM (
		SELECT 
		SPTC.prem_trans_code_descript as prem_trans_code_descript, 
		SPTC.prem_trans_type_descript as prem_trans_type_descript, 
		LTRIM(RTRIM(SPTC.prem_trans_code)) as prem_trans_code
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_premium_transaction_code SPTC
		where SPTC.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY prem_trans_code ORDER BY prem_trans_code_descript DESC) = 1
),
lkp_sup_reason_amended_code AS (
	SELECT
	rsn_amended_code_descript,
	rsn_amended_code
	FROM (
		SELECT 
		sup_reason_amended_code.rsn_amended_code_descript as rsn_amended_code_descript, 
		LTRIM(RTRIM(sup_reason_amended_code.rsn_amended_code)) as rsn_amended_code FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_reason_amended_code
		where sup_reason_amended_code.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY rsn_amended_code ORDER BY rsn_amended_code_descript DESC) = 1
),
Exp_Collect_LookupData AS (
	SELECT
	lkp_sup_premium_transaction_code.prem_trans_code_descript AS lkp_prem_trans_code_descript,
	-- *INF*: iif(isnull(lkp_prem_trans_code_descript) or IS_SPACES(lkp_prem_trans_code_descript) or LENGTH(lkp_prem_trans_code_descript)=0,'N/A',LTRIM(RTRIM(lkp_prem_trans_code_descript)))
	IFF(
	    lkp_prem_trans_code_descript IS NULL
	    or LENGTH(lkp_prem_trans_code_descript)>0
	    and TRIM(lkp_prem_trans_code_descript)=''
	    or LENGTH(lkp_prem_trans_code_descript) = 0,
	    'N/A',
	    LTRIM(RTRIM(lkp_prem_trans_code_descript))
	) AS o_lkp_prem_trans_code_descript,
	lkp_sup_premium_transaction_code.prem_trans_type_descript AS lkp_prem_trans_type_descript,
	-- *INF*: iif(isnull(lkp_prem_trans_type_descript) or IS_SPACES(lkp_prem_trans_type_descript) or LENGTH(lkp_prem_trans_type_descript)=0,'N/A',LTRIM(RTRIM(lkp_prem_trans_type_descript)))
	IFF(
	    lkp_prem_trans_type_descript IS NULL
	    or LENGTH(lkp_prem_trans_type_descript)>0
	    and TRIM(lkp_prem_trans_type_descript)=''
	    or LENGTH(lkp_prem_trans_type_descript) = 0,
	    'N/A',
	    LTRIM(RTRIM(lkp_prem_trans_type_descript))
	) AS o_lkp_prem_trans_type_descript,
	lkp_sup_reason_amended_code.rsn_amended_code_descript AS lkp_rsn_amended_code_descript,
	-- *INF*: iif(isnull(lkp_rsn_amended_code_descript) or IS_SPACES(lkp_rsn_amended_code_descript) or LENGTH(lkp_rsn_amended_code_descript)=0,'N/A',LTRIM(RTRIM(lkp_rsn_amended_code_descript)))
	IFF(
	    lkp_rsn_amended_code_descript IS NULL
	    or LENGTH(lkp_rsn_amended_code_descript)>0
	    and TRIM(lkp_rsn_amended_code_descript)=''
	    or LENGTH(lkp_rsn_amended_code_descript) = 0,
	    'N/A',
	    LTRIM(RTRIM(lkp_rsn_amended_code_descript))
	) AS o_lkp_rsn_amended_code_descript,
	EXP_Collect_Data_columns.PremiumMasterTransactionCode_out AS PremiumTransactionCode,
	EXP_Collect_Data_columns.PremiumMasterPremiumType_out AS PremiumType,
	EXP_Collect_Data_columns.ReasonAmendedCode_out AS ReasonAmendedCode,
	EXP_Collect_Data_columns.CurrentSnapshotFlag,
	EXP_Collect_Data_columns.audit_id,
	EXP_Collect_Data_columns.createddate,
	EXP_Collect_Data_columns.modifieddate,
	EXP_Collect_Data_columns.EffectiveDate,
	EXP_Collect_Data_columns.ExpirationDate,
	EXP_Collect_Data_columns.PremiumMasterCustomerCareCommissionRate,
	PremiumMasterCustomerCareCommissionRate AS o_PremiumMasterCustomerCareCommissionRate
	FROM EXP_Collect_Data_columns
	LEFT JOIN lkp_sup_premium_transaction_code
	ON lkp_sup_premium_transaction_code.prem_trans_code = EXP_Collect_Data_columns.PremiumMasterTransactionCode_out
	LEFT JOIN lkp_sup_reason_amended_code
	ON lkp_sup_reason_amended_code.rsn_amended_code = EXP_Collect_Data_columns.PremiumMasterTransactionCode_out
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
RTR_PremiumTransactionTypeDim AS (
	SELECT
	lkp_PremiumTransactionTypeDim.PremiumTransactionTypeDimID AS lkp_PremiumTransactionTypeDimID,
	Exp_Collect_LookupData.o_lkp_prem_trans_code_descript,
	Exp_Collect_LookupData.o_lkp_prem_trans_type_descript,
	Exp_Collect_LookupData.o_lkp_rsn_amended_code_descript,
	Exp_Collect_LookupData.PremiumTransactionCode,
	Exp_Collect_LookupData.PremiumType,
	Exp_Collect_LookupData.ReasonAmendedCode,
	Exp_Collect_LookupData.CurrentSnapshotFlag,
	Exp_Collect_LookupData.audit_id,
	Exp_Collect_LookupData.createddate,
	Exp_Collect_LookupData.modifieddate,
	Exp_Collect_LookupData.EffectiveDate,
	Exp_Collect_LookupData.ExpirationDate,
	Exp_Collect_LookupData.o_PremiumMasterCustomerCareCommissionRate AS PremiumMasterCustomerCareCommissionRate
	FROM Exp_Collect_LookupData
	LEFT JOIN lkp_PremiumTransactionTypeDim
	ON lkp_PremiumTransactionTypeDim.PremiumTransactionCode = Exp_Collect_LookupData.PremiumTransactionCode AND lkp_PremiumTransactionTypeDim.ReasonAmendedCode = Exp_Collect_LookupData.ReasonAmendedCode AND lkp_PremiumTransactionTypeDim.PremiumTypeCode = Exp_Collect_LookupData.PremiumType AND lkp_PremiumTransactionTypeDim.CustomerCareCommissionRate = Exp_Collect_LookupData.o_PremiumMasterCustomerCareCommissionRate
),
RTR_PremiumTransactionTypeDim_INSERT AS (SELECT * FROM RTR_PremiumTransactionTypeDim WHERE isnull(lkp_PremiumTransactionTypeDimID)),
RTR_PremiumTransactionTypeDim_DEFAULT1 AS (SELECT * FROM RTR_PremiumTransactionTypeDim WHERE NOT ( (isnull(lkp_PremiumTransactionTypeDimID)) )),
UPD_PremiumTransactionTypeDim AS (
	SELECT
	lkp_PremiumTransactionTypeDimID AS PremiumTransactionTypeDimID, 
	CurrentSnapshotFlag, 
	audit_id AS AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	createddate AS CreatedDate, 
	modifieddate AS ModifiedDate, 
	PremiumTransactionCode, 
	o_lkp_prem_trans_code_descript AS PremiumTransactionCodeDescription, 
	o_lkp_prem_trans_type_descript AS PremiumTransactionTypeDescription, 
	ReasonAmendedCode, 
	o_lkp_rsn_amended_code_descript AS ReasonAmendedCodeDescription, 
	PremiumType AS PremiumTypeCode, 
	PremiumMasterCustomerCareCommissionRate AS PremiumMasterCustomerCareCommissionRate2
	FROM RTR_PremiumTransactionTypeDim_DEFAULT1
),
PremiumTransactionTypeDimUPD AS (
	MERGE INTO Shortcut_to_PremiumTransactionTypeDim AS T
	USING UPD_PremiumTransactionTypeDim AS S
	ON T.PremiumTransactionTypeDimID = S.PremiumTransactionTypeDimID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.PremiumTransactionCode = S.PremiumTransactionCode, T.PremiumTransactionCodeDescription = S.PremiumTransactionCodeDescription, T.PremiumTransactionTypeDescription = S.PremiumTransactionTypeDescription, T.ReasonAmendedCode = S.ReasonAmendedCode, T.ReasonAmendedCodeDescription = S.ReasonAmendedCodeDescription, T.PremiumTypeCode = S.PremiumTypeCode, T.CustomerCareCommissionRate = S.PremiumMasterCustomerCareCommissionRate2
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
	PremiumMasterCustomerCareCommissionRate AS CUSTOMERCARECOMMISSIONRATE
	FROM RTR_PremiumTransactionTypeDim_INSERT
),
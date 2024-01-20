WITH
LKP_PIF_02_FOR_FIELDS AS (
	SELECT
	pif_number_installments_a,
	pol_sym,
	pol_num,
	pol_mod,
	pif_symbol,
	pif_policy_number,
	pif_module
	FROM (
		SELECT DISTINCT A.pif_number_installments_a as pif_number_installments_a, A.pif_symbol as pif_symbol, A.pif_policy_number as pif_policy_number, A.pif_module as pif_module 
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME2}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.arch_pif_02_stage A
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module ORDER BY pif_number_installments_a) = 1
),
LKP_sup_premium_transaction_code AS (
	SELECT
	StandardPremiumTransactionCode,
	sup_prem_trans_code_id
	FROM (
		SELECT 
			StandardPremiumTransactionCode,
			sup_prem_trans_code_id
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_premium_transaction_code
		WHERE crrnt_snpsht_flag='1'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_prem_trans_code_id ORDER BY StandardPremiumTransactionCode) = 1
),
lkp_sup_reason_amended_code AS (
	SELECT
	StandardReasonAmendedCode,
	rsn_amended_code,
	source_sys_id
	FROM (
		SELECT StandardReasonAmendedCode as StandardReasonAmendedCode,
		source_sys_id as source_sys_id,
		CASE WHEN source_sys_id='DCT' THEN LOWER(rsn_amended_code) ELSE rsn_amended_code END as rsn_amended_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_reason_amended_code
		WHERE crrnt_snpsht_flag=1
		ORDER BY CASE WHEN source_sys_id='DCT' THEN LOWER(rsn_amended_code) ELSE rsn_amended_code END, source_sys_id
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY rsn_amended_code,source_sys_id ORDER BY StandardReasonAmendedCode) = 1
),
SQ_PremiumCalculationDirectTransactions_DCT AS (
	Declare @YearMonth varchar(6) =  (select substring(convert(varchar(6),(DATEADD(MM,@{pipeline().parameters.NO_OF_MONTHS},GETDATE())),112),1,6) )
	
	SELECT 	pol.source_sys_id,
	POL.pol_ak_id, 
	POL.agency_ak_id, 
	POL.pol_sym, POL.pol_num, POL.pol_mod, 
	POL.pol_key, POL.pol_eff_date, POL.pol_exp_date, POL.pms_pol_lob_code, 
	POL.pol_term, POL.pol_issue_code, POL.pol_audit_frqncy, 
	POL.contract_cust_ak_id, CUSADDR.addr_line_1, CUSADDR.city_name, CUSADDR.state_prov_code, 
	POLCOV.RiskLocationAKID, LOC.LocationUnitNumber, LOC.LocationIndicator, POLCOV.PolicyCoverageAKID, 
	POLCOV.InsuranceLine, POLCOV.TypeBureauCode, POLCOV.PolicyCoverageEffectiveDate, 
	POLCOV.PolicyCoverageExpirationDate, 
	-1 AS StatisticalCoverageAKID, 
	RATCOV.SubLocationUnitNumber, 
	'N/A' AS RiskUnitGroup, 
	'N/A' AS RiskUnitGroupSequenceNumber, 
	'N/A' AS RiskUnit, 
	'N/A' AS RiskUnitSequenceNumber, 
	'N/A' AS MajorPerilCode, 
	'N/A' AS MajorPerilSequenceNumber, 
	RATCOV.SublineCode, 
	'N/A' AS PMSTypeExposure, 
	'1800-1-1' AS StatisticalCoverageEffectiveDate, 
	'2100-12-31 23:59:59' AS StatisticalCoverageExpirationDate, 
	PT.AgencyActualCommissionRate, 
	
	PT.PremiumLoadSequence, PT.PremiumTransactionAKID, PT.ReinsuranceCoverageAKID, PT.PMSFunctionCode, 
	PT.SupPremiumTransactionCodeId, PT.PremiumTransactionEnteredDate, PT.PremiumTransactionEffectiveDate, PT.PremiumTransactionExpirationDate, PT.PremiumTransactionBookedDate, PT.PremiumTransactionAmount, PT.FullTermPremium, PT.PremiumType, PT.ReasonAmendedCode, PT.OffsetOnsetCode, 
	
	RATCOV.RatingCoverageAKID, RATCOV.ClassCode, PT.Exposure, 
	RATCOV.EffectiveDate, RATCOV.ExpirationDate,
	POLCOV.CustomerCareCommissionRate , PT.WrittenExposure, PT.DeclaredEventFlag
	FROM
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Policy POL with (nolock),
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_address CUSADDR with (nolock),
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation LOC  with (nolock), 
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage POLCOV  with (nolock),
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RATCOV with (nolock),
	    @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT with (nolock)
	WHERE 
	
	substring(convert(varchar(6),PT.PremiumTransactionBookedDate,112),1,6) = @YearMonth
	AND substring(convert(varchar(6),PT.PremiumTransactionEnteredDate,112),1,6) <= @YearMonth
	
		AND CUSADDR.contract_cust_ak_id = POL.contract_cust_ak_id
		AND LOC.PolicyAKID = POL.pol_ak_id
		AND LOC.RiskLocationAKID = POLCOV.RiskLocationAKID
		AND POLCOV.PolicyCoverageAKID = RATCOV.PolicyCoverageAKID
	    AND PT.RatingCoverageAKId = RATCOV.RatingCoverageAKID
	    AND PT.PremiumType = 'D'
		AND CUSADDR.crrnt_snpsht_flag = 1  
		AND POL.crrnt_snpsht_flag = 1 
		AND LOC.CurrentSnapshotFlag =1
		AND POLCOV.CurrentSnapshotFlag =1 
		AND PT.CurrentSnapshotFlag =1 
		AND RATCOV.EffectiveDate=PT.EffectiveDate
		AND CUSADDR.source_sys_id = 'DCT'
		AND POL.source_sys_id = 'DCT' 
		AND LOC.SourceSystemID ='DCT'
		AND POLCOV.SourceSystemID ='DCT' 
		AND PT.SourceSystemID ='DCT' 
	@{pipeline().parameters.WHERE_CLAUSE_DCT}
		ORDER BY 
		 PT.PremiumTransactionId
),
EXP_PremiumMaster_DCT AS (
	SELECT
	source_sys_id AS i_source_sys_id,
	pol_ak_id AS i_PolicyAKID,
	agency_ak_id AS i_AgencyAKID,
	pol_sym AS i_pol_sym,
	pol_num AS i_pol_num,
	pol_mod AS i_pol_mod,
	pol_key AS i_pol_key,
	pol_eff_date AS i_pol_eff_date,
	pol_exp_date AS i_pol_exp_date,
	pms_pol_lob_code AS i_pms_pol_lob_code,
	pol_term AS i_pol_term,
	pol_issue_code AS i_pol_issue_code,
	pol_audit_frqncy AS i_pol_audit_frqncy,
	contract_cust_ak_id AS i_ContractCustomerAKID,
	addr_line_1 AS i_PremiumMasterRiskAddress,
	city_name AS i_city_name,
	state_prov_code AS i_state_prov_code,
	RiskLocationAKID AS i_RiskLocationAKID,
	LocationUnitNumber AS i_LocationUnitNumber,
	LocationIndicator AS i_LocationIndicator,
	PolicyCoverageAKID AS i_PolicyCoverageAKID,
	InsuranceLine AS i_InsuranceLine,
	TypeBureauCode AS i_TypeBureauCode,
	PolicyCoverageEffectiveDate AS i_PolicyCoverageEffectiveDate,
	PolicyCoverageExpirationDate AS i_PolicyCoverageExpirationDate,
	StatisticalCoverageAKID AS i_StatisticalCoverageAKID,
	SubLocationUnitNumber AS i_SubLocationUnitNumber,
	RiskUnitGroup AS i_RiskUnitGroup,
	RiskUnitGroupSequenceNumber AS i_RiskUnitGroupSequenceNumber,
	RiskUnit AS i_RiskUnit,
	RiskUnitSequenceNumber AS i_RiskUnitSequenceNumber,
	MajorPerilCode AS i_MajorPerilCode,
	MajorPerilSequenceNumber AS i_MajorPerilSequenceNumber,
	SublineCode AS i_SublineCode,
	PMSTypeExposure AS i_PMSTypeExposure,
	StatisticalCoverageEffectiveDate AS i_StatisticalCoverageEffectiveDate,
	StatisticalCoverageExpirationDate AS i_StatisticalCoverageExpirationDate,
	AgencyActualCommissionRate AS i_AgencyActualCommissionRate,
	PremiumLoadSequence AS i_PremiumLoadSequence,
	PremiumTransactionAKID AS i_PremiumTransactionAKID,
	ReinsuranceCoverageAKID AS i_ReinsuranceCoverageAKID,
	PMSFunctionCode AS i_PMSFunctionCode,
	SupPremiumTransactionCodeId AS i_SupPremiumTransactionCodeId,
	PremiumTransactionEnteredDate AS i_PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate AS i_PremiumTransactionEffectiveDate,
	PremiumTransactionExpirationDate AS i_PremiumTransactionExpirationDate,
	PremiumTransactionBookedDate AS i_PremiumTransactionBookedDate,
	PremiumTransactionAmount AS i_PremiumTransactionAmount,
	FullTermPremium AS i_FullTermPremium,
	PremiumType AS i_PremiumMasterPremiumType,
	ReasonAmendedCode AS i_ReasonAmendedCode,
	OffsetOnsetCode AS i_OffsetOnsetIndicator,
	RatingCoverageAKID AS i_RatingCoverageAKID,
	ClassCode AS i_ClassCode,
	Exposure AS i_Exposure,
	RatingCoverageEffectiveDate AS i_RatingCoverageEffectiveDate,
	RatingCoverageExpirationDate AS i_RatingCoverageExpirationDate,
	WrittenExposure AS i_WrittenExposure,
	-- *INF*: :LKP.LKP_sup_premium_transaction_code(i_SupPremiumTransactionCodeId)
	LKP_SUP_PREMIUM_TRANSACTION_CODE_i_SupPremiumTransactionCodeId.StandardPremiumTransactionCode AS v_PremiumTransactionCode,
	-- *INF*: :LKP.LKP_SUP_REASON_AMENDED_CODE(LOWER(i_ReasonAmendedCode),i_source_sys_id)
	LKP_SUP_REASON_AMENDED_CODE_LOWER_i_ReasonAmendedCode_i_source_sys_id.StandardReasonAmendedCode AS v_ReasonAmendedCode,
	-- *INF*: DECODE(TRUE,
	-- i_InsuranceLine='WorkersCompensation' and i_PremiumTransactionAmount<0 AND i_WrittenExposure>0,
	-- -1 * i_WrittenExposure,
	-- i_WrittenExposure)
	DECODE(
	    TRUE,
	    i_InsuranceLine = 'WorkersCompensation' and i_PremiumTransactionAmount < 0 AND i_WrittenExposure > 0, - 1 * i_WrittenExposure,
	    i_WrittenExposure
	) AS v_WrittenExposure,
	'DCT' AS o_SourceSystemID,
	i_AgencyAKID AS o_AgencyAKID,
	i_PolicyAKID AS o_PolicyAKID,
	i_ContractCustomerAKID AS o_ContractCustomerAKID,
	i_RiskLocationAKID AS o_RiskLocationAKID,
	i_PolicyCoverageAKID AS o_PolicyCoverageAKID,
	i_StatisticalCoverageAKID AS o_StatisticalCoverageAKID,
	i_ReinsuranceCoverageAKID AS o_ReinsuranceCoverageAKID,
	i_PremiumTransactionAKID AS o_PremiumTransactionAKID,
	-1 AS o_BureauStatisticalCodeAKID,
	i_pol_sym AS o_pol_sym,
	i_pol_num AS o_pol_num,
	i_pol_mod AS o_pol_mod,
	i_pol_key AS o_pol_key,
	i_pol_eff_date AS o_pol_eff_date,
	i_pol_exp_date AS o_pol_exp_date,
	i_pms_pol_lob_code AS o_pms_pol_lob_code,
	i_pol_issue_code AS o_pol_issue_code,
	i_LocationUnitNumber AS o_LocationUnitNumber,
	i_LocationIndicator AS o_LocationIndicator,
	i_InsuranceLine AS o_InsuranceLine,
	i_TypeBureauCode AS o_TypeBureauCode,
	i_PolicyCoverageEffectiveDate AS o_PolicyCoverageEffectiveDate,
	i_PolicyCoverageExpirationDate AS o_PolicyCoverageExpirationDate,
	i_SubLocationUnitNumber AS o_SubLocationUnitNumber,
	i_RiskUnitGroup AS o_RiskUnitGroup,
	i_RiskUnitGroupSequenceNumber AS o_RiskUnitGroupSequenceNumber,
	i_RiskUnit AS o_RiskUnit,
	i_RiskUnitSequenceNumber AS o_RiskUnitSequenceNumber,
	i_MajorPerilCode AS o_MajorPerilCode,
	i_MajorPerilSequenceNumber AS o_MajorPerilSequenceNumber,
	i_SublineCode AS o_SublineCode,
	i_PMSTypeExposure AS o_PMSTypeExposure,
	i_StatisticalCoverageEffectiveDate AS o_StatisticalCoverageEffectiveDate,
	i_StatisticalCoverageExpirationDate AS o_StatisticalCoverageExpirationDate,
	i_PremiumLoadSequence AS o_PremiumLoadSequence,
	i_PMSFunctionCode AS o_PMSFunctionCode,
	-- *INF*: IIF(ISNULL(v_PremiumTransactionCode), 'N/A', v_PremiumTransactionCode)
	IFF(v_PremiumTransactionCode IS NULL, 'N/A', v_PremiumTransactionCode) AS o_PremiumTransactionCode,
	i_PremiumTransactionEnteredDate AS o_PremiumTransactionEnteredDate,
	i_PremiumTransactionEffectiveDate AS o_PremiumTransactionEffectiveDate,
	i_ClassCode AS o_ClassCode,
	i_PremiumTransactionExpirationDate AS o_PremiumTransactionExpirationDate,
	i_PremiumTransactionBookedDate AS o_PremiumTransactionBookedDate,
	-- *INF*: IIF(NOT ISNULL(v_ReasonAmendedCode), v_ReasonAmendedCode, 'N/A')
	IFF(v_ReasonAmendedCode IS NOT NULL, v_ReasonAmendedCode, 'N/A') AS o_ReasonAmendedCode,
	'N/A' AS o_BureauSpecialUseCode,
	'N/A' AS o_PMSAnnualStatementLine,
	i_AgencyActualCommissionRate AS o_AgencyActualCommissionRate,
	-- *INF*: DECODE(TRUE,
	-- i_InsuranceLine='WorkersCompensation' and i_PremiumTransactionAmount<0 AND i_Exposure>0,
	-- -1 * i_Exposure,
	-- i_Exposure)
	DECODE(
	    TRUE,
	    i_InsuranceLine = 'WorkersCompensation' and i_PremiumTransactionAmount < 0 AND i_Exposure > 0, - 1 * i_Exposure,
	    i_Exposure
	) AS o_Exposure,
	'N/A' AS o_RatingDateIndicator,
	'N/A' AS o_BureauStatisticalUserLine,
	'N/A' AS o_StatisticalCodes,
	i_PremiumTransactionAmount AS o_PremiumTransactionAmount,
	i_FullTermPremium AS o_FullTermPremium,
	-- *INF*: TO_DATE('21001231235959','YYYYMMDDHH24MISS')
	TO_TIMESTAMP('21001231235959', 'YYYYMMDDHH24MISS') AS o_PremiumMasterBureauInceptionDate,
	i_PremiumMasterRiskAddress AS o_PremiumMasterRiskAddress,
	i_city_name || ', ' || i_state_prov_code AS o_PremiumMasterRiskCityState,
	'N/A' AS o_PremiumMasterRenewalIndicator,
	i_PremiumMasterPremiumType AS o_PremiumMasterPremiumType,
	'N/A' AS o_Status,
	-- *INF*: DECODE(RTRIM(i_OffsetOnsetIndicator), 'N/A', 'DIRECT', 'Offset', 'OFFSET', 'Onset', 'ONSET', 'N/A')
	DECODE(
	    RTRIM(i_OffsetOnsetIndicator),
	    'N/A', 'DIRECT',
	    'Offset', 'OFFSET',
	    'Onset', 'ONSET',
	    'N/A'
	) AS o_PremiumMasterStatus,
	i_pol_audit_frqncy AS o_pol_audit_frqncy,
	i_pol_term AS o_pol_term,
	i_RatingCoverageAKID AS o_RatingCoverageAKID,
	i_RatingCoverageEffectiveDate AS o_RatingCoverageEffectiveDate,
	i_RatingCoverageExpirationDate AS o_RatingCoverageExpirationDate,
	CustomerCareCommissionRate,
	v_WrittenExposure AS o_WrittenExposure,
	DeclaredEventsFlag
	FROM SQ_PremiumCalculationDirectTransactions_DCT
	LEFT JOIN LKP_SUP_PREMIUM_TRANSACTION_CODE LKP_SUP_PREMIUM_TRANSACTION_CODE_i_SupPremiumTransactionCodeId
	ON LKP_SUP_PREMIUM_TRANSACTION_CODE_i_SupPremiumTransactionCodeId.sup_prem_trans_code_id = i_SupPremiumTransactionCodeId

	LEFT JOIN LKP_SUP_REASON_AMENDED_CODE LKP_SUP_REASON_AMENDED_CODE_LOWER_i_ReasonAmendedCode_i_source_sys_id
	ON LKP_SUP_REASON_AMENDED_CODE_LOWER_i_ReasonAmendedCode_i_source_sys_id.rsn_amended_code = LOWER(i_ReasonAmendedCode)
	AND LKP_SUP_REASON_AMENDED_CODE_LOWER_i_ReasonAmendedCode_i_source_sys_id.source_sys_id = i_source_sys_id

),
SQ_PremiumCalculationDirectTransactions_PMS AS (
	SELECT 	POL.source_sys_id,
		POL.pol_ak_id, 
	      POL.agency_ak_id, 
		POL.pol_sym, 
		POL.pol_num, 
		POL.pol_mod, 
		POL.pol_key, 
		POL.pol_eff_date, 
		POL.pol_exp_date, 
		POL.pms_pol_lob_code, 
	      POL.pol_term,
		POL.pol_issue_code, 
	      POL.pol_audit_frqncy,
		POL.contract_cust_ak_id, 
		CUSADDR.addr_line_1, 
		CUSADDR.city_name, 
		CUSADDR.state_prov_code, 
		CUSADDR.zip_postal_code, 
		LOC.RiskLocationAKID, 
		LOC.LocationUnitNumber, 
		LOC.LocationIndicator, 
		POLCOV.PolicyCoverageAKID, 
		POLCOV.InsuranceLine, 
		POLCOV.TypeBureauCode, 
		POLCOV.PolicyCoverageEffectiveDate, 
		POLCOV.PolicyCoverageExpirationDate, 
		STATCOV.StatisticalCoverageAKID, 
		STATCOV.SubLocationUnitNumber, 
		STATCOV.RiskUnitGroup, 
		STATCOV.RiskUnitGroupSequenceNumber, 
		STATCOV.RiskUnit, 
		STATCOV.RiskUnitSequenceNumber, 
		STATCOV.MajorPerilCode, 
		STATCOV.MajorPerilSequenceNumber, 
		STATCOV.SublineCode, 
		STATCOV.PMSTypeExposure, 
		STATCOV.ClassCode, 
		PT.Exposure, 
		STATCOV.StatisticalCoverageEffectiveDate,
		STATCOV.StatisticalCoverageExpirationDate, 
		PT.AgencyActualCommissionRate, 
		STATCOV.ReinsuranceSectionCode, 
		PT.PremiumLoadSequence, 
		PT.PremiumTransactionAKID, 
	      PT.ReinsuranceCoverageAKID, 
		PT.PMSFunctionCode, 
		PT.SupPremiumTransactionCodeId, 
		PT.PremiumTransactionEnteredDate, 
		PT.PremiumTransactionEffectiveDate, 		
		PT.PremiumTransactionExpirationDate, 
		PT.PremiumTransactionBookedDate, 
		PT.PremiumTransactionAmount, 
		PT.FullTermPremium, 
		PT.PremiumType, 
		PT.ReasonAmendedCode, 
		STATCOD.BureauStatisticalCodeAKID, 
		STATCOD.BureauCode1, 
		STATCOD.BureauCode2, 
		STATCOD.BureauCode3, 
		STATCOD.BureauCode4, 
		STATCOD.BureauCode5, 
		STATCOD.BureauCode6, 
		STATCOD.BureauCode7, 
		STATCOD.BureauCode8, 
		STATCOD.BureauCode9, 
		STATCOD.BureauCode10, 
		STATCOD.BureauCode11, 
		STATCOD.BureauCode12, 
		STATCOD.BureauCode13, 
		STATCOD.BureauCode14, 
		STATCOD.BureauCode15, 
		STATCOD.BureauSpecialUseCode, 
		STATCOD.PMSAnnualStatementLine, 
		STATCOD.RatingDateIndicator, 
		STATCOD.BureauStatisticalUserLine, 
		STATCOD.AuditReinstatementIndicator,
		PT.WrittenExposure, PT.DeclaredEventFlag
	FROM
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Policy POL,
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_address CUSADDR,
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation LOC  with (nolock), 
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage POLCOV  with (nolock),
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage STATCOV  with (nolock),
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.BureauStatisticalCode STATCOD with (nolock),
	    	@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT with (nolock)
	WHERE exists
		(select distinct POL1.pol_key	
		FROM
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation LOC1  with (nolock), 
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Policy POL1  with (nolock),
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage POLCOV1  with (nolock),
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage STATCOV1  with (nolock),
	    	@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT1 with (nolock)
		WHERE	
		    LOC1.PolicyAKID = POL1.pol_ak_id
		AND LOC1.RiskLocationAKID = POLCOV1.RiskLocationAKID
		AND POLCOV1.PolicyCoverageAKID = STATCOV1.PolicyCoverageAKID
	    	AND STATCOV1.StatisticalCoverageAKID = PT1.StatisticalCoverageAKID
		AND POL1.crrnt_snpsht_flag = 1 
		AND LOC1.CurrentSnapshotFlag =1
		AND STATCOV1.CurrentSnapshotFlag =1 
		AND POLCOV1.CurrentSnapshotFlag =1 
		AND PT1.CurrentSnapshotFlag =1 
		AND POL1.source_sys_id = 'PMS' 
		AND LOC1.SourceSystemID='PMS' 
		AND STATCOV1.SourceSystemID='PMS'  
		AND POLCOV1.SourceSystemID='PMS'  
		AND PT1.SourceSystemID='PMS' 
		AND datepart(MM,PremiumTransactionBookedDate)=datepart(MM,(DATEADD(MM,@{pipeline().parameters.NO_OF_MONTHS},GETDATE()))) 
		AND datepart(YYYY,PremiumTransactionBookedDate)=datepart(YYYY,(DATEADD(MM,@{pipeline().parameters.NO_OF_MONTHS},GETDATE())))
	    	AND PT1.PremiumType = 'D'
	    	AND POL.pol_key = POL1.pol_key)
	    	AND CUSADDR.contract_cust_ak_id = POL.contract_cust_ak_id
	    	AND LOC.PolicyAKID = POL.pol_ak_id
		AND LOC.RiskLocationAKID = POLCOV.RiskLocationAKID
		AND POLCOV.PolicyCoverageAKID = STATCOV.PolicyCoverageAKID
	    	AND STATCOV.StatisticalCoverageAKID = PT.StatisticalCoverageAKID
	    	AND PT.PremiumTransactionAKID = STATCOD.PremiumTransactionAKID
	      AND PT.PremiumType = 'D'
		AND CUSADDR.crrnt_snpsht_flag = 1  
		AND POL.crrnt_snpsht_flag = 1 
		AND LOC.CurrentSnapshotFlag =1
		AND STATCOV.CurrentSnapshotFlag =1 
		AND POLCOV.CurrentSnapshotFlag =1 
		AND PT.CurrentSnapshotFlag =1 
		AND STATCOD.CurrentSnapshotFlag =1
		AND CUSADDR.source_sys_id = 'PMS'  
		AND POL.source_sys_id = 'PMS'  
		AND LOC.SourceSystemID='PMS' 
		AND STATCOV.SourceSystemID='PMS'  
		AND POLCOV.SourceSystemID='PMS' 
		AND PT.SourceSystemID='PMS' 
		AND STATCOD.SourceSystemID='PMS' 
		@{pipeline().parameters.WHERE_CLAUSE_PMS}
		ORDER BY 
		 POL.pol_sym
	    	,POL.pol_num
	    	,POL.pol_mod 
	      ,PT.PMSFunctionCode
		,POLCOV.InsuranceLine
		,LOC.LocationUnitNumber
		,STATCOV.SubLocationUnitNumber
		,STATCOV.RiskUnitGroup
		,STATCOV.RiskUnitGroupSequenceNumber
		,STATCOV.RiskUnit 
		,STATCOV.RiskUnitSequenceNumber 
		,STATCOV.PMSTypeExposure
		,STATCOV.MajorPerilCode
		,STATCOV.MajorPerilSequenceNumber
		,POLCOV.PolicyCoverageEffectiveDate
	    	,PT.PremiumTransactionEffectiveDate
		,PT.PremiumTransactionEnteredDate
		,PT.PremiumLoadSequence  desc
),
EXP_DirectTransactions AS (
	SELECT
	source_sys_id AS i_source_sys_id,
	agency_ak_id,
	pol_ak_id,
	pol_sym,
	pol_num,
	pol_mod,
	pol_key,
	pol_eff_date,
	pol_exp_date,
	pms_pol_lob_code,
	pol_issue_code,
	contract_cust_ak_id,
	addr_line_1,
	city_name,
	state_prov_code,
	zip_postal_code,
	RiskLocationAKID,
	LocationUnitNumber,
	LocationIndicator,
	PolicyCoverageAKID,
	InsuranceLine,
	TypeBureauCode,
	PolicyCoverageEffectiveDate,
	PolicyCoverageExpirationDate,
	StatisticalCoverageAKID,
	SubLocationUnitNumber,
	RiskUnitGroup,
	RiskUnitGroupSequenceNumber,
	RiskUnit,
	RiskUnitSequenceNumber,
	MajorPerilCode,
	MajorPerilSequenceNumber,
	SublineCode,
	PMSTypeExposure,
	ClassCode,
	Exposure AS i_Exposure,
	StatisticalCoverageEffectiveDate,
	StatisticalCoverageExpirationDate,
	AgencyActualCommissionRate,
	ReinsuranceSectionCode,
	PremiumLoadSequence,
	PremiumTransactionAKID,
	ReinsuranceCoverageAKID,
	PMSFunctionCode,
	SupPremiumTransactionCodeId,
	-- *INF*: :LKP.LKP_sup_premium_transaction_code(SupPremiumTransactionCodeId)
	LKP_SUP_PREMIUM_TRANSACTION_CODE_SupPremiumTransactionCodeId.StandardPremiumTransactionCode AS v_PremiumTransactionCode,
	-- *INF*: IIF(ISNULL(v_PremiumTransactionCode), 'N/A', v_PremiumTransactionCode)
	IFF(v_PremiumTransactionCode IS NULL, 'N/A', v_PremiumTransactionCode) AS PremiumTransactionCode,
	PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	PremiumTransactionExpirationDate,
	PremiumTransactionBookedDate,
	PremiumTransactionAmount,
	FullTermPremium,
	PremiumType,
	ReasonAmendedCode AS i_ReasonAmendedCode,
	-- *INF*: :LKP.LKP_SUP_REASON_AMENDED_CODE(i_ReasonAmendedCode,i_source_sys_id)
	LKP_SUP_REASON_AMENDED_CODE_i_ReasonAmendedCode_i_source_sys_id.StandardReasonAmendedCode AS v_ReasonAmendedCode,
	-- *INF*: IIF(NOT ISNULL(v_ReasonAmendedCode), v_ReasonAmendedCode, 'N/A')
	IFF(v_ReasonAmendedCode IS NOT NULL, v_ReasonAmendedCode, 'N/A') AS o_ReasonAmendedCode,
	BureauStatisticalCodeAKID,
	BureauCode1,
	BureauCode2,
	BureauCode3,
	BureauCode4,
	BureauCode5,
	BureauCode6,
	BureauCode7,
	BureauCode8,
	BureauCode9,
	BureauCode10,
	BureauCode11,
	BureauCode12,
	BureauCode13,
	BureauCode14,
	BureauCode15,
	BureauSpecialUseCode,
	PMSAnnualStatementLine,
	RatingDateIndicator,
	BureauStatisticalUserLine,
	AuditReinstatementIndicator,
	pol_audit_frqncy,
	pol_term,
	WrittenExposure AS i_WrittenExposure,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(InsuranceLine))='WC' AND PremiumTransactionAmount<0 AND i_WrittenExposure>0,
	-- -1 * i_WrittenExposure,
	-- i_WrittenExposure)
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(InsuranceLine)) = 'WC' AND PremiumTransactionAmount < 0 AND i_WrittenExposure > 0, - 1 * i_WrittenExposure,
	    i_WrittenExposure
	) AS v_WrittenExposure,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(InsuranceLine))='WC' AND PremiumTransactionAmount<0 AND i_Exposure>0,
	-- -1 * i_Exposure,
	-- i_Exposure)
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(InsuranceLine)) = 'WC' AND PremiumTransactionAmount < 0 AND i_Exposure > 0, - 1 * i_Exposure,
	    i_Exposure
	) AS o_Exposure,
	v_WrittenExposure AS o_WrittenExposure,
	DeclaredEventsFlag
	FROM SQ_PremiumCalculationDirectTransactions_PMS
	LEFT JOIN LKP_SUP_PREMIUM_TRANSACTION_CODE LKP_SUP_PREMIUM_TRANSACTION_CODE_SupPremiumTransactionCodeId
	ON LKP_SUP_PREMIUM_TRANSACTION_CODE_SupPremiumTransactionCodeId.sup_prem_trans_code_id = SupPremiumTransactionCodeId

	LEFT JOIN LKP_SUP_REASON_AMENDED_CODE LKP_SUP_REASON_AMENDED_CODE_i_ReasonAmendedCode_i_source_sys_id
	ON LKP_SUP_REASON_AMENDED_CODE_i_ReasonAmendedCode_i_source_sys_id.rsn_amended_code = i_ReasonAmendedCode
	AND LKP_SUP_REASON_AMENDED_CODE_i_ReasonAmendedCode_i_source_sys_id.source_sys_id = i_source_sys_id

),
EXP_Evaluate_fields_for_OnsetOffset AS (
	SELECT
	agency_ak_id,
	pol_ak_id,
	pol_sym,
	pol_num,
	pol_mod,
	pol_key,
	pol_eff_date,
	pol_exp_date,
	pms_pol_lob_code,
	pol_issue_code,
	contract_cust_ak_id,
	addr_line_1,
	city_name,
	state_prov_code,
	zip_postal_code,
	RiskLocationAKID,
	LocationUnitNumber,
	LocationIndicator,
	PolicyCoverageAKID,
	InsuranceLine,
	TypeBureauCode,
	PolicyCoverageEffectiveDate,
	PolicyCoverageExpirationDate,
	StatisticalCoverageAKID,
	SubLocationUnitNumber,
	RiskUnitGroup,
	RiskUnitGroupSequenceNumber,
	RiskUnit,
	RiskUnitSequenceNumber,
	MajorPerilCode,
	MajorPerilSequenceNumber,
	SublineCode,
	PMSTypeExposure,
	ClassCode,
	o_Exposure AS Exposure,
	o_WrittenExposure AS WrittenExposure,
	StatisticalCoverageEffectiveDate,
	StatisticalCoverageExpirationDate,
	AgencyActualCommissionRate,
	ReinsuranceSectionCode,
	PremiumLoadSequence,
	PremiumTransactionAKID,
	ReinsuranceCoverageAKID,
	PMSFunctionCode,
	PremiumTransactionCode,
	PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	PremiumTransactionExpirationDate,
	PremiumTransactionBookedDate,
	PremiumTransactionAmount,
	FullTermPremium,
	PremiumType,
	o_ReasonAmendedCode AS ReasonAmendedCode,
	BureauStatisticalCodeAKID,
	BureauCode1,
	BureauCode2,
	BureauCode3,
	BureauCode4,
	BureauCode5,
	BureauCode6,
	BureauCode7,
	BureauCode8,
	BureauCode9,
	BureauCode10,
	BureauCode11,
	BureauCode12,
	BureauCode13,
	BureauCode14,
	BureauCode15,
	BureauSpecialUseCode,
	PMSAnnualStatementLine,
	RatingDateIndicator,
	BureauStatisticalUserLine,
	-- *INF*: BureauCode1 || BureauCode2 || BureauCode3 || BureauCode4 || BureauCode5 || BureauCode6 || BureauCode7 || BureauCode8 || BureauCode9 || BureauCode10 || BureauCode11 || BureauCode12 || BureauCode13 || BureauCode14 || BureauCode15
	-- 
	-- --- concatenates all elemental fields to get the full unchanged 38 byte statistical code field for future operations
	BureauCode1 || BureauCode2 || BureauCode3 || BureauCode4 || BureauCode5 || BureauCode6 || BureauCode7 || BureauCode8 || BureauCode9 || BureauCode10 || BureauCode11 || BureauCode12 || BureauCode13 || BureauCode14 || BureauCode15 AS v_StatisticalCodes,
	-- *INF*: IIF(pol_sym =v_prev_row_pol_sym AND
	--        pol_num = v_prev_row_pol_num AND
	--        pol_mod = v_prev_row_pol_mod AND
	--        PMSFunctionCode = v_prev_row_PMSFunctionCode AND
	--        InsuranceLine = v_prev_row_InsuranceLine AND
	--        LocationUnitNumber = v_prev_row_LocationUnitNumber  AND
	--        SubLocationUnitNumber= v_prev_row_SubLocationUnitNumber  AND
	--        RiskUnitGroup = v_prev_row_RiskUnitGroup AND
	--        RiskUnitGroupSequenceNumber  = v_prev_row_RiskUnitGroupSequenceNumber AND	 
	--        RiskUnit = v_prev_row_RiskUnit  AND
	--        RiskUnitSequenceNumber = v_prev_row_RiskUnitSequenceNumber  AND
	--        PMSTypeExposure = v_prev_row_PMSTypeExposure AND
	--        MajorPerilCode = v_prev_row_MajorPerilCode  AND
	--        MajorPerilSequenceNumber = v_prev_row_MajorPerilSequenceNumber        
	--        ,'PRIOR_TRANS_FOUND','NOPRIOR_TRANS')
	-- 
	-- -- SEE COMMENTS
	IFF(
	    pol_sym = v_prev_row_pol_sym
	    and pol_num = v_prev_row_pol_num
	    and pol_mod = v_prev_row_pol_mod
	    and PMSFunctionCode = v_prev_row_PMSFunctionCode
	    and InsuranceLine = v_prev_row_InsuranceLine
	    and LocationUnitNumber = v_prev_row_LocationUnitNumber
	    and SubLocationUnitNumber = v_prev_row_SubLocationUnitNumber
	    and RiskUnitGroup = v_prev_row_RiskUnitGroup
	    and RiskUnitGroupSequenceNumber = v_prev_row_RiskUnitGroupSequenceNumber
	    and RiskUnit = v_prev_row_RiskUnit
	    and RiskUnitSequenceNumber = v_prev_row_RiskUnitSequenceNumber
	    and PMSTypeExposure = v_prev_row_PMSTypeExposure
	    and MajorPerilCode = v_prev_row_MajorPerilCode
	    and MajorPerilSequenceNumber = v_prev_row_MajorPerilSequenceNumber,
	    'PRIOR_TRANS_FOUND',
	    'NOPRIOR_TRANS'
	) AS v_Prior_Transaction_Found,
	v_Prior_Transaction_Found AS Prior_Transaction_Found,
	-- *INF*: SUBSTR(TO_CHAR((ADD_TO_DATE(SYSTIMESTAMP(),'MM',@{pipeline().parameters.NO_OF_MONTHS})),'YYYYMMDD'),1,6)
	-- 
	-- -- determines account date per closeout month from parametrized number of months subtracted from system date
	SUBSTR(TO_CHAR((DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTHS},CURRENT_TIMESTAMP())), 'YYYYMMDD'), 1, 6) AS v_Account_Date,
	-- *INF*: IIF(substr(TO_CHAR(PremiumTransactionBookedDate, 'YYYYMMDD'),1,6) = v_Account_Date,'VALID','INVALID')
	-- 
	-- -- flags direct premium transactions as ineligible for being written out if not booked in closeout month
	IFF(
	    substr(TO_CHAR(PremiumTransactionBookedDate, 'YYYYMMDD'), 1, 6) = v_Account_Date, 'VALID',
	    'INVALID'
	) AS v_Valid_Record_Generation,
	-- *INF*: ROUND(
	-- DATE_DIFF(PremiumTransactionEffectiveDate,PremiumTransactionExpirationDate,'D')/DATE_DIFF(pol_eff_date,pol_exp_date,'D'),4)
	ROUND(DATEDIFF(DAY,PremiumTransactionEffectiveDate,PremiumTransactionExpirationDate) / DATEDIFF(DAY,pol_eff_date,pol_exp_date), 4) AS v_Pro_Rata_Factor,
	-- *INF*: IIF(v_Prior_Transaction_Found = 'NOPRIOR_TRANS',0.0,
	--                (v_Accum_Total_Original_Premium + v_prev_row_FullTermPremium))
	-- -------
	-- --IIF(v_Prior_Transaction_Found = 'NOPRIOR_TRANS',0.0,
	--    ---         IIF(sar_exposure <> v_prev_row_sar_exposure or sar_premium < 0.0, v_prev_accum_original_premium,
	-- ---(v_accum_Total_Original_Premium + v_prev_row_sar_original_prem)))
	-- ------
	-- --IIF(v_Prior_Transaction_Found = 'NOPRIOR_TRANS',0.0,
	--    ---            IIF(sar_premium < 0.0, v_prev_accum_original_premium,(v_accum_Total_Original_Premium + v_prev_row_sar_original_prem)))
	IFF(
	    v_Prior_Transaction_Found = 'NOPRIOR_TRANS', 0.0,
	    (v_Accum_Total_Original_Premium + v_prev_row_FullTermPremium)
	) AS v_Total_Original_Premium,
	-- *INF*: IIF(v_Prior_Transaction_Found = 'PRIOR_TRANS_FOUND',v_Total_Original_Premium,0.0)
	-- 
	-- 
	-- --'PRIOR_TRANS_FOUND','NOPRIOR_TRANS'
	IFF(v_Prior_Transaction_Found = 'PRIOR_TRANS_FOUND', v_Total_Original_Premium, 0.0) AS v_Accum_Total_Original_Premium,
	-- *INF*: ROUND(((-1) *  v_Total_Original_Premium * v_Pro_Rata_Factor),4)
	ROUND(((- 1) * v_Total_Original_Premium * v_Pro_Rata_Factor), 4) AS v_Offset_Premium,
	v_Offset_Premium AS Offset_Premium,
	-- *INF*: ROUND(((-1) * v_Total_Original_Premium),4)
	ROUND(((- 1) * v_Total_Original_Premium), 4) AS v_Offset_Original_Premium,
	v_Offset_Original_Premium AS Offset_Original_Premium,
	-- *INF*: ROUND((PremiumTransactionAmount - v_Offset_Premium),4)
	ROUND((PremiumTransactionAmount - v_Offset_Premium), 4) AS v_Onset_Premium,
	v_Onset_Premium AS Onset_Premium,
	-- *INF*: ROUND((FullTermPremium - v_Offset_Original_Premium) ,4) 
	ROUND((FullTermPremium - v_Offset_Original_Premium), 4) AS v_Onset_Original_Premium,
	v_Onset_Original_Premium AS Onset_Original_Premium,
	-- *INF*: (-1) * v_prev_row_Exposure
	(- 1) * v_prev_row_Exposure AS Offset_Exposure,
	Exposure AS Onset_Exposure,
	-- *INF*: (-1) * v_prev_row_WrittenExposure
	(- 1) * v_prev_row_WrittenExposure AS Offset_WrittenExposure,
	WrittenExposure AS Onset_WrittenExposure,
	-- *INF*: IIF(RTRIM(PremiumTransactionCode)='12','12','22')
	IFF(RTRIM(PremiumTransactionCode) = '12', '12', '22') AS Onset_Transaction_Code,
	-- *INF*: IIF(RTRIM(PremiumTransactionCode)='12','22','12')
	IFF(RTRIM(PremiumTransactionCode) = '12', '22', '12') AS Offset_Transaction_Code,
	v_offset_RiskLocationAKID AS Offset_RiskLocationAKID,
	v_offset_StatisticalCodes AS Offset_StatisticalCodes,
	v_offset_BureauStatisticalUserLine AS Offset_BureauStatisticalUserLine,
	v_offset_ReasonAmendedCode AS Offset_ReasonAmendedCode,
	v_offset_PremiumTransactionExpirationDate AS Offset_PremiumTransactionExpirationDate,
	v_offset_AgencyActualCommissionRate AS Offset_AgencyActualCommissionRate,
	v_offset_SublineCode AS Offset_SublineCode,
	v_offset_ClassCode AS Offset_ClassCode,
	v_offset_StatisticalCoverageAKID AS Offset_StatisticalCoverageAKID,
	v_offset_PremiumTransactionAKID AS Offset_PremiumTransactionAKID,
	-- *INF*: IIF(IN(RTRIM(PremiumTransactionCode),'12','13','22','28'),'VALID_TRANS_CODES','INVALID_TRANS_CODES')
	IFF(
	    RTRIM(PremiumTransactionCode) IN ('12','13','22','28'), 'VALID_TRANS_CODES',
	    'INVALID_TRANS_CODES'
	) AS v_valid_transaction_codes,
	-- *INF*: IIF(v_Valid_Record_Generation ='VALID',
	--             IIF(v_valid_transaction_codes ='VALID_TRANS_CODES' AND v_Prior_Transaction_Found ='PRIOR_TRANS_FOUND' AND NOT IN(pms_pol_lob_code,'WC' ,'WCP'),
	--                                     IIF(v_Onset_Original_Premium = 0.00,'GENERATE_PREMIUM','ONSET_OFFSET'),'GENERATE_PREMIUM'),'NOGENERATION')
	-- 
	IFF(
	    v_Valid_Record_Generation = 'VALID',
	    IFF(
	        v_valid_transaction_codes = 'VALID_TRANS_CODES'
	        and v_Prior_Transaction_Found = 'PRIOR_TRANS_FOUND'
	        and NOT pms_pol_lob_code IN ('WC','WCP'),
	        IFF(
	            v_Onset_Original_Premium = 0.00, 'GENERATE_PREMIUM', 'ONSET_OFFSET'
	        ),
	        'GENERATE_PREMIUM'
	    ),
	    'NOGENERATION'
	) AS v_status,
	v_status AS Status,
	pol_sym AS v_prev_row_pol_sym,
	pol_num AS v_prev_row_pol_num,
	pol_mod AS v_prev_row_pol_mod,
	PMSFunctionCode AS v_prev_row_PMSFunctionCode,
	InsuranceLine AS v_prev_row_InsuranceLine,
	LocationUnitNumber AS v_prev_row_LocationUnitNumber,
	SubLocationUnitNumber AS v_prev_row_SubLocationUnitNumber,
	RiskUnitGroup AS v_prev_row_RiskUnitGroup,
	RiskUnitGroupSequenceNumber AS v_prev_row_RiskUnitGroupSequenceNumber,
	RiskUnit AS v_prev_row_RiskUnit,
	RiskUnitSequenceNumber AS v_prev_row_RiskUnitSequenceNumber,
	PMSTypeExposure AS v_prev_row_PMSTypeExposure,
	MajorPerilCode AS v_prev_row_MajorPerilCode,
	MajorPerilSequenceNumber AS v_prev_row_MajorPerilSequenceNumber,
	PremiumTransactionAmount AS v_prev_row_PremiumTransactionAmount,
	FullTermPremium AS v_prev_row_FullTermPremium,
	v_prev_row_Exposure AS v_offset_Exposure,
	Exposure AS v_prev_row_Exposure,
	WrittenExposure AS v_prev_row_WrittenExposure,
	v_prev_row_BureauStatisticalUserLine AS v_offset_BureauStatisticalUserLine,
	BureauStatisticalUserLine AS v_prev_row_BureauStatisticalUserLine,
	v_prev_row_RiskLocationAKID AS v_offset_RiskLocationAKID,
	RiskLocationAKID AS v_prev_row_RiskLocationAKID,
	v_prev_row_ReasonAmendedCode AS v_offset_ReasonAmendedCode,
	ReasonAmendedCode AS v_prev_row_ReasonAmendedCode,
	v_prev_row_PremiumTransactionExpirationDate AS v_offset_PremiumTransactionExpirationDate,
	PremiumTransactionExpirationDate AS v_prev_row_PremiumTransactionExpirationDate,
	v_prev_row_AgencyActualCommissionRate AS v_offset_AgencyActualCommissionRate,
	AgencyActualCommissionRate AS v_prev_row_AgencyActualCommissionRate,
	v_prev_row_SublineCode AS v_offset_SublineCode,
	SublineCode AS v_prev_row_SublineCode,
	v_prev_row_ClassCode AS v_offset_ClassCode,
	ClassCode AS v_prev_row_ClassCode,
	v_prev_row_StatisticalCodes AS v_offset_StatisticalCodes,
	v_StatisticalCodes AS v_prev_row_StatisticalCodes,
	v_prev_row_StatisticalCoverageAKID AS v_offset_StatisticalCoverageAKID,
	StatisticalCoverageAKID AS v_prev_row_StatisticalCoverageAKID,
	v_StatisticalCodes AS StatisticalCodes,
	-- *INF*: IIF(RatingDateIndicator='C',StatisticalCoverageEffectiveDate,pol_eff_date)
	IFF(RatingDateIndicator = 'C', StatisticalCoverageEffectiveDate, pol_eff_date) AS v_inc_date,
	-- *INF*: DECODE(TRUE,IN( RTRIM(PremiumTransactionCode),'83','84'),PremiumTransactionEffectiveDate,v_inc_date)
	DECODE(
	    TRUE,
	    RTRIM(PremiumTransactionCode) IN ('83','84'), PremiumTransactionEffectiveDate,
	    v_inc_date
	) AS PremiumMasterBureauInceptionDate,
	-- *INF*: rtrim(addr_line_1)
	rtrim(addr_line_1) AS PremiumMasterRiskAddress,
	-- *INF*: rtrim(city_name) || ', ' || rtrim(state_prov_code)
	rtrim(city_name) || ', ' || rtrim(state_prov_code) AS PremiumMasterRiskCityState,
	AuditReinstatementIndicator,
	-- *INF*: IIF(RTRIM(PremiumTransactionCode) = '15' AND RTRIM(AuditReinstatementIndicator) = 'R','R',' ')
	IFF(
	    RTRIM(PremiumTransactionCode) = '15' AND RTRIM(AuditReinstatementIndicator) = 'R', 'R', ' '
	) AS PremiumMasterRenewalIndicator,
	pol_audit_frqncy,
	pol_term,
	v_prev_row_PremiumTransactionAKID AS v_offset_PremiumTransactionAKID,
	PremiumTransactionAKID AS v_prev_row_PremiumTransactionAKID,
	1 AS o_PremiumMasterCustomerCareCommissionRate,
	DeclaredEventsFlag
	FROM EXP_DirectTransactions
),
FIL_No_Generate AS (
	SELECT
	agency_ak_id AS AgencyAKID, 
	pol_ak_id AS PolicyAKID, 
	contract_cust_ak_id AS ContractCustomerAKID, 
	RiskLocationAKID, 
	Offset_RiskLocationAKID, 
	PolicyCoverageAKID, 
	StatisticalCoverageAKID, 
	PremiumTransactionAKID, 
	BureauStatisticalCodeAKID, 
	ReinsuranceCoverageAKID, 
	pol_sym, 
	pol_num, 
	pol_mod, 
	pol_key, 
	pol_eff_date, 
	pol_exp_date, 
	pms_pol_lob_code, 
	pol_issue_code, 
	LocationUnitNumber, 
	LocationIndicator, 
	InsuranceLine, 
	TypeBureauCode, 
	PolicyCoverageEffectiveDate, 
	PolicyCoverageExpirationDate, 
	SubLocationUnitNumber, 
	RiskUnitGroup, 
	RiskUnitGroupSequenceNumber, 
	RiskUnit, 
	RiskUnitSequenceNumber, 
	MajorPerilCode, 
	MajorPerilSequenceNumber, 
	SublineCode, 
	PMSTypeExposure, 
	ClassCode, 
	Exposure, 
	WrittenExposure, 
	StatisticalCoverageEffectiveDate, 
	StatisticalCoverageExpirationDate, 
	AgencyActualCommissionRate, 
	ReinsuranceSectionCode, 
	PremiumLoadSequence, 
	PMSFunctionCode, 
	PremiumTransactionCode, 
	PremiumTransactionEnteredDate, 
	PremiumTransactionEffectiveDate, 
	PremiumTransactionExpirationDate, 
	PremiumTransactionBookedDate, 
	PremiumTransactionAmount, 
	FullTermPremium, 
	PremiumType, 
	ReasonAmendedCode, 
	BureauSpecialUseCode, 
	PMSAnnualStatementLine, 
	RatingDateIndicator, 
	BureauStatisticalUserLine, 
	Prior_Transaction_Found, 
	Offset_Premium, 
	Offset_Original_Premium, 
	Onset_Premium, 
	Onset_Original_Premium, 
	Offset_Exposure, 
	Onset_Exposure, 
	Offset_WrittenExposure, 
	Onset_WrittenExposure, 
	Onset_Transaction_Code, 
	Offset_Transaction_Code, 
	Offset_StatisticalCodes, 
	Offset_BureauStatisticalUserLine, 
	Offset_ReasonAmendedCode, 
	Offset_PremiumTransactionExpirationDate, 
	Offset_AgencyActualCommissionRate, 
	Offset_SublineCode, 
	Offset_ClassCode, 
	Offset_StatisticalCoverageAKID, 
	Offset_PremiumTransactionAKID, 
	Status, 
	StatisticalCodes, 
	PremiumMasterBureauInceptionDate, 
	PremiumMasterRiskAddress, 
	PremiumMasterRiskCityState, 
	PremiumMasterRenewalIndicator, 
	pol_audit_frqncy, 
	pol_term, 
	o_PremiumMasterCustomerCareCommissionRate AS PremiumMasterCustomerCareCommissionRate, 
	DeclaredEventsFlag
	FROM EXP_Evaluate_fields_for_OnsetOffset
	WHERE Status <> 'NOGENERATION'
),
RTR_Onset_Offset_Direct_Part1Ceded AS (
	SELECT
	AgencyAKID,
	PolicyAKID,
	ContractCustomerAKID,
	RiskLocationAKID,
	Offset_RiskLocationAKID,
	PolicyCoverageAKID,
	StatisticalCoverageAKID,
	ReinsuranceCoverageAKID,
	PremiumTransactionAKID,
	BureauStatisticalCodeAKID,
	pol_sym,
	pol_num,
	pol_mod,
	pol_key,
	pol_eff_date,
	pol_exp_date,
	pms_pol_lob_code,
	pol_issue_code,
	LocationUnitNumber,
	LocationIndicator,
	InsuranceLine,
	TypeBureauCode,
	PolicyCoverageEffectiveDate,
	PolicyCoverageExpirationDate,
	SubLocationUnitNumber,
	RiskUnitGroup,
	RiskUnitGroupSequenceNumber,
	RiskUnit,
	RiskUnitSequenceNumber,
	MajorPerilCode,
	MajorPerilSequenceNumber,
	SublineCode,
	PMSTypeExposure,
	ClassCode,
	Exposure,
	WrittenExposure,
	StatisticalCoverageEffectiveDate,
	StatisticalCoverageExpirationDate,
	AgencyActualCommissionRate,
	ReinsuranceSectionCode,
	PremiumLoadSequence,
	PMSFunctionCode,
	PremiumTransactionCode,
	PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	PremiumTransactionExpirationDate,
	PremiumTransactionBookedDate,
	PremiumTransactionAmount,
	FullTermPremium,
	PremiumType,
	ReasonAmendedCode,
	BureauSpecialUseCode,
	PMSAnnualStatementLine,
	RatingDateIndicator,
	Prior_Transaction_Found,
	BureauStatisticalUserLine,
	Offset_Premium,
	Offset_Original_Premium,
	Onset_Premium,
	Onset_Original_Premium,
	Offset_Exposure,
	Onset_Exposure,
	Offset_WrittenExposure,
	Onset_WrittenExposure,
	Onset_Transaction_Code,
	Offset_Transaction_Code,
	Offset_StatisticalCodes,
	Offset_BureauStatisticalUserLine,
	Offset_ReasonAmendedCode,
	Offset_PremiumTransactionExpirationDate,
	Offset_AgencyActualCommissionRate,
	Offset_SublineCode,
	Offset_ClassCode,
	Offset_StatisticalCoverageAKID,
	Offset_PremiumTransactionAKID,
	Status,
	StatisticalCodes,
	PremiumMasterBureauInceptionDate,
	PremiumMasterRiskAddress,
	PremiumMasterRiskCityState,
	PremiumMasterRenewalIndicator,
	pol_audit_frqncy,
	pol_term,
	PremiumMasterCustomerCareCommissionRate,
	DeclaredEventsFlag
	FROM FIL_No_Generate
),
RTR_Onset_Offset_Direct_Part1Ceded_ONSET AS (SELECT * FROM RTR_Onset_Offset_Direct_Part1Ceded WHERE Status ='ONSET_OFFSET' and Prior_Transaction_Found = 'PRIOR_TRANS_FOUND'),
RTR_Onset_Offset_Direct_Part1Ceded_REINSURANCE AS (SELECT * FROM RTR_Onset_Offset_Direct_Part1Ceded WHERE IIF(IN(Status,'ONSET_OFFSET','GENERATE_PREMIUM')  ,TRUE,FALSE)),
RTR_Onset_Offset_Direct_Part1Ceded_OFFSET AS (SELECT * FROM RTR_Onset_Offset_Direct_Part1Ceded WHERE Status ='ONSET_OFFSET' and Prior_Transaction_Found = 'PRIOR_TRANS_FOUND'),
RTR_Onset_Offset_Direct_Part1Ceded_PREMIUMMASTER AS (SELECT * FROM RTR_Onset_Offset_Direct_Part1Ceded WHERE Status = 'GENERATE_PREMIUM'
--(Status = 'GENERATE_PREMIUM' and Prior_Transaction_Found = 'NOPRIOR_TRANS')
--OR 
--(Prior_Transaction_Found = 'NOPRIOR_TRANS')),
EXP_generate_PremiumMasterRecord AS (
	SELECT
	'PMS' AS o_SourceSystemID,
	AgencyAKID AS AgencyAKID5,
	PolicyAKID AS PolicyAKID5,
	ContractCustomerAKID AS ContractCustomerAKID5,
	RiskLocationAKID AS RiskLocationAKID5,
	PolicyCoverageAKID AS PolicyCoverageAKID5,
	StatisticalCoverageAKID AS StatisticalCoverageAKID5,
	ReinsuranceCoverageAKID AS ReinsuranceCoverageAKID5,
	PremiumTransactionAKID AS PremiumTransactionAKID5,
	BureauStatisticalCodeAKID AS BureauStatisticalCodeAKID5,
	pol_sym AS pol_sym5,
	pol_num AS pol_num5,
	pol_mod AS pol_mod5,
	pol_key AS pol_key5,
	pol_eff_date AS pol_eff_date5,
	pol_exp_date AS pol_exp_date5,
	pms_pol_lob_code AS pms_pol_lob_code5,
	pol_issue_code AS pol_issue_code5,
	LocationUnitNumber AS LocationUnitNumber5,
	LocationIndicator AS LocationIndicator5,
	InsuranceLine AS InsuranceLine5,
	TypeBureauCode AS TypeBureauCode5,
	PolicyCoverageEffectiveDate AS PolicyCoverageEffectiveDate5,
	PolicyCoverageExpirationDate AS PolicyCoverageExpirationDate5,
	SubLocationUnitNumber AS SubLocationUnitNumber5,
	RiskUnitGroup AS RiskUnitGroup5,
	RiskUnitGroupSequenceNumber AS RiskUnitGroupSequenceNumber5,
	RiskUnit AS RiskUnit5,
	RiskUnitSequenceNumber AS RiskUnitSequenceNumber5,
	MajorPerilCode AS MajorPerilCode5,
	MajorPerilSequenceNumber AS MajorPerilSequenceNumber5,
	SublineCode AS SublineCode5,
	PMSTypeExposure AS PMSTypeExposure5,
	StatisticalCoverageEffectiveDate AS StatisticalCoverageEffectiveDate5,
	StatisticalCoverageExpirationDate AS StatisticalCoverageExpirationDate5,
	PremiumLoadSequence AS PremiumLoadSequence5,
	PMSFunctionCode AS PMSFunctionCode5,
	PremiumTransactionCode AS PremiumTransactionCode5,
	PremiumTransactionEnteredDate AS PremiumTransactionEnteredDate5,
	PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate5,
	ClassCode AS ClassCode5,
	PremiumTransactionExpirationDate AS PremiumTransactionExpirationDate5,
	PremiumTransactionBookedDate AS PremiumTransactionBookedDate5,
	ReasonAmendedCode AS ReasonAmendedCode5,
	BureauSpecialUseCode AS BureauSpecialUseCode5,
	PremiumType AS PremiumType5,
	PMSAnnualStatementLine AS PMSAnnualStatementLine5,
	AgencyActualCommissionRate AS AgencyActualCommissionRate5,
	Exposure AS Exposure5,
	RatingDateIndicator AS RatingDateIndicator5,
	BureauStatisticalUserLine AS BureauStatisticalUserLine5,
	StatisticalCodes AS StatisticalCodes5,
	PremiumTransactionAmount AS PremiumTransactionAmount5,
	FullTermPremium AS FullTermPremium5,
	PremiumMasterBureauInceptionDate AS PremiumMasterBureauInceptionDate5,
	PremiumMasterRiskAddress AS PremiumMasterRiskAddress5,
	PremiumMasterRiskCityState AS PremiumMasterRiskCityState5,
	PremiumMasterRenewalIndicator AS PremiumMasterRenewalIndicator5,
	Status AS Status5,
	'DIRECT' AS PremiumMasterStatus5,
	pol_audit_frqncy AS pol_audit_frqncy5,
	pol_term AS pol_term5,
	-1 AS o_RatingCoverageAKId,
	-- *INF*: TO_DATE('21001231235959','YYYYMMDDHH24MISS')
	TO_TIMESTAMP('21001231235959', 'YYYYMMDDHH24MISS') AS o_RatingCoverageEffectiveDate,
	-- *INF*: TO_DATE('21001231235959','YYYYMMDDHH24MISS')
	TO_TIMESTAMP('21001231235959', 'YYYYMMDDHH24MISS') AS o_RatingCoverageExpirationDate,
	PremiumMasterCustomerCareCommissionRate AS PremiumMasterCustomerCareCommissionRate5,
	WrittenExposure AS WrittenExposure5,
	DeclaredEventsFlag AS DeclaredEventsFlag5
	FROM RTR_Onset_Offset_Direct_Part1Ceded_PREMIUMMASTER
),
EXP_generate_offset AS (
	SELECT
	'PMS' AS o_SourceSystemID,
	AgencyAKID AS AgencyAKID4,
	PolicyAKID AS PolicyAKID4,
	ContractCustomerAKID AS ContractCustomerAKID4,
	Offset_RiskLocationAKID AS RiskLocationAKID4,
	PolicyCoverageAKID AS PolicyCoverageAKID4,
	Offset_StatisticalCoverageAKID AS StatisticalCoverageAKID4,
	ReinsuranceCoverageAKID AS ReinsuranceCoverageAKID4,
	PremiumTransactionAKID AS PremiumTransactionAKID4,
	BureauStatisticalCodeAKID AS BureauStatisticalCodeAKID4,
	pol_sym AS pol_sym4,
	pol_num AS pol_num4,
	pol_mod AS pol_mod4,
	pol_key AS pol_key4,
	pol_eff_date AS pol_eff_date4,
	pol_exp_date AS pol_exp_date4,
	pms_pol_lob_code AS pms_pol_lob_code4,
	pol_issue_code AS pol_issue_code4,
	LocationUnitNumber AS LocationUnitNumber4,
	LocationIndicator AS LocationIndicator4,
	InsuranceLine AS InsuranceLine4,
	TypeBureauCode AS TypeBureauCode4,
	PolicyCoverageEffectiveDate AS PolicyCoverageEffectiveDate4,
	PolicyCoverageExpirationDate AS PolicyCoverageExpirationDate4,
	SubLocationUnitNumber AS SubLocationUnitNumber4,
	RiskUnitGroup AS RiskUnitGroup4,
	RiskUnitGroupSequenceNumber AS RiskUnitGroupSequenceNumber4,
	RiskUnit AS RiskUnit4,
	RiskUnitSequenceNumber AS RiskUnitSequenceNumber4,
	MajorPerilCode AS MajorPerilCode4,
	MajorPerilSequenceNumber AS MajorPerilSequenceNumber4,
	Exposure AS Exposure4,
	StatisticalCoverageEffectiveDate AS StatisticalCoverageEffectiveDate4,
	StatisticalCoverageExpirationDate AS StatisticalCoverageExpirationDate4,
	Offset_Premium AS Offset_Premium4,
	Offset_Original_Premium AS Offset_Original_Premium4,
	Offset_Exposure AS Offset_Exposure4,
	Offset_Transaction_Code AS Offset_Transaction_Code4,
	Offset_StatisticalCodes AS Offset_StatisticalCodes4,
	Offset_BureauStatisticalUserLine AS Offset_BureauStatisticalUserLine4,
	Offset_StateProvinceCode AS Offset_StateProvinceCode4,
	Offset_ReasonAmendedCode AS Offset_ReasonAmendedCode4,
	Offset_PremiumTransactionExpirationDate AS Offset_PremiumTransactionExpirationDate4,
	Offset_AgencyActualCommissionRate AS Offset_AgencyActualCommissionRate4,
	Offset_SublineCode AS Offset_SublineCode4,
	Offset_ClassCode AS Offset_ClassCode4,
	Offset_PremiumTransactionAKID,
	Status AS Status4,
	'OFFSET' AS PremiumMasterStatus4,
	PremiumMasterBureauInceptionDate AS PremiumMasterBureauInceptionDate4,
	PremiumMasterRiskAddress AS PremiumMasterRiskAddress4,
	PremiumMasterRiskCityState AS PremiumMasterRiskCityState4,
	PremiumMasterRenewalIndicator AS PremiumMasterRenewalIndicator4,
	SublineCode AS SublineCode4,
	ReasonAmendedCode AS ReasonAmendedCode4,
	BureauSpecialUseCode AS BureauSpecialUseCode4,
	PMSAnnualStatementLine AS PMSAnnualStatementLine4,
	PremiumType AS PremiumType4,
	RatingDateIndicator AS RatingDateIndicator4,
	PremiumTransactionBookedDate AS PremiumTransactionBookedDate4,
	PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate4,
	PremiumTransactionEnteredDate AS PremiumTransactionEnteredDate4,
	PMSFunctionCode AS PMSFunctionCode4,
	PremiumLoadSequence AS PremiumLoadSequence4,
	PMSTypeExposure AS PMSTypeExposure4,
	pol_audit_frqncy AS pol_audit_frqncy4,
	pol_term AS pol_term4,
	-1 AS o_RatingCoverageAKId,
	-- *INF*: TO_DATE('21001231235959','YYYYMMDDHH24MISS')
	TO_TIMESTAMP('21001231235959', 'YYYYMMDDHH24MISS') AS o_RatingCoverageEffectiveDate,
	-- *INF*: TO_DATE('21001231235959','YYYYMMDDHH24MISS')
	TO_TIMESTAMP('21001231235959', 'YYYYMMDDHH24MISS') AS o_RatingCoverageExpirationDate,
	PremiumMasterCustomerCareCommissionRate AS PremiumMasterCustomerCareCommissionRate4,
	Offset_WrittenExposure AS Offset_WrittenExposure4,
	DeclaredEventsFlag AS DeclaredEventsFlag4
	FROM RTR_Onset_Offset_Direct_Part1Ceded_OFFSET
),
EXP_generate_onset AS (
	SELECT
	'PMS' AS o_SourceSystemID,
	AgencyAKID AS AgencyAKID1,
	PolicyAKID AS PolicyAKID1,
	ContractCustomerAKID AS ContractCustomerAKID1,
	RiskLocationAKID AS RiskLocationAKID1,
	PolicyCoverageAKID AS PolicyCoverageAKID1,
	StatisticalCoverageAKID AS StatisticalCoverageAKID1,
	ReinsuranceCoverageAKID AS ReinsuranceCoverageAKID1,
	PremiumTransactionAKID AS PremiumTransactionAKID1,
	BureauStatisticalCodeAKID AS BureauStatisticalCodeAKID1,
	pol_sym AS pol_sym1,
	pol_num AS pol_num1,
	pol_mod AS pol_mod1,
	pol_key AS pol_key1,
	pol_eff_date AS pol_eff_date1,
	pol_exp_date AS pol_exp_date1,
	pms_pol_lob_code AS pms_pol_lob_code1,
	pol_issue_code AS pol_issue_code1,
	LocationUnitNumber AS LocationUnitNumber1,
	LocationIndicator AS LocationIndicator1,
	InsuranceLine AS InsuranceLine1,
	TypeBureauCode AS TypeBureauCode1,
	PolicyCoverageEffectiveDate AS PolicyCoverageEffectiveDate1,
	PolicyCoverageExpirationDate AS PolicyCoverageExpirationDate1,
	SubLocationUnitNumber AS SubLocationUnitNumber1,
	RiskUnitGroup AS RiskUnitGroup1,
	RiskUnitGroupSequenceNumber AS RiskUnitGroupSequenceNumber1,
	RiskUnit AS RiskUnit1,
	RiskUnitSequenceNumber AS RiskUnitSequenceNumber1,
	MajorPerilCode AS MajorPerilCode1,
	MajorPerilSequenceNumber AS MajorPerilSequenceNumber1,
	SublineCode AS SublineCode1,
	PMSTypeExposure AS PMSTypeExposure1,
	ClassCode AS ClassCode1,
	StatisticalCoverageEffectiveDate AS StatisticalCoverageEffectiveDate1,
	StatisticalCoverageExpirationDate AS StatisticalCoverageExpirationDate1,
	AgencyActualCommissionRate AS AgencyActualCommissionRate1,
	PremiumLoadSequence AS PremiumLoadSequence1,
	PMSFunctionCode AS PMSFunctionCode1,
	PremiumTransactionEnteredDate AS PremiumTransactionEnteredDate1,
	PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate1,
	PremiumTransactionExpirationDate AS PremiumTransactionExpirationDate1,
	PremiumTransactionBookedDate AS PremiumTransactionBookedDate1,
	PremiumType AS PremiumType1,
	ReasonAmendedCode AS ReasonAmendedCode1,
	BureauSpecialUseCode AS BureauSpecialUseCode1,
	PMSAnnualStatementLine AS PMSAnnualStatementLine1,
	RatingDateIndicator AS RatingDateIndicator1,
	BureauStatisticalUserLine AS BureauStatisticalUserLine1,
	Onset_Premium AS Onset_Premium1,
	Onset_Original_Premium AS Onset_Original_Premium1,
	Onset_Exposure AS Onset_Exposure1,
	Onset_Transaction_Code AS Onset_Transaction_Code1,
	Status AS Status1,
	'ONSET' AS PremiumMasterStatus1,
	StatisticalCodes AS StatisticalCodes1,
	PremiumMasterBureauInceptionDate AS PremiumMasterBureauInceptionDate1,
	PremiumMasterRiskAddress AS PremiumMasterRiskAddress1,
	PremiumMasterRiskCityState AS PremiumMasterRiskCityState1,
	PremiumMasterRenewalIndicator AS PremiumMasterRenewalIndicator1,
	pol_audit_frqncy AS pol_audit_frqncy1,
	pol_term AS pol_term1,
	-1 AS o_RatingCoverageAKId,
	-- *INF*: TO_DATE('21001231235959','YYYYMMDDHH24MISS')
	TO_TIMESTAMP('21001231235959', 'YYYYMMDDHH24MISS') AS o_RatingCoverageEffectiveDate,
	-- *INF*: TO_DATE('21001231235959','YYYYMMDDHH24MISS')
	TO_TIMESTAMP('21001231235959', 'YYYYMMDDHH24MISS') AS o_RatingCoverageExpirationDate,
	PremiumMasterCustomerCareCommissionRate AS PremiumMasterCustomerCareCommissionRate1,
	Onset_WrittenExposure AS Onset_WrittenExposure1,
	DeclaredEventsFlag AS DeclaredEventsFlag1
	FROM RTR_Onset_Offset_Direct_Part1Ceded_ONSET
),
SQ_reinsurance_coverage AS (
	SELECT REINCOV.reins_cov_ak_id, REINCOV.pol_ak_id, REINCOV.reins_ins_line, REINCOV.reins_loc_unit_num, REINCOV.reins_sub_loc_unit_num, REINCOV.reins_risk_unit_grp, REINCOV.reins_risk_unit_grp_seq_num, REINCOV.reins_risk_unit, REINCOV.reins_risk_unit_seq_num, REINCOV.reins_section_code, REINCOV.reins_eff_date, REINCOV.reins_exp_date, REINCOV.reins_prcnt_prem_ceded, REINCOV.reins_prcnt_facultative_commssn 
	FROM
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Policy POL,
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}. reinsurance_coverage REINCOV
	WHERE EXISTS
		(SELECT DISTINCT POL1.pol_key	
		FROM
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation LOC1  with (nolock), 
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Policy POL1  with (nolock),
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage POLCOV1  with (nolock),
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage STATCOV1  with (nolock),
	    	@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT1 with (nolock)
		WHERE	
		LOC1.PolicyAKID = POL1.pol_ak_id
		AND LOC1.RiskLocationAKID = POLCOV1.RiskLocationAKID
		AND POLCOV1.PolicyCoverageAKID = STATCOV1.PolicyCoverageAKID
	    	AND STATCOV1.StatisticalCoverageAKID = PT1.StatisticalCoverageAKID
		AND POL1.crrnt_snpsht_flag = 1 
		AND LOC1.CurrentSnapshotFlag =1
		AND STATCOV1.CurrentSnapshotFlag =1 
		AND POLCOV1.CurrentSnapshotFlag =1 
		AND PT1.CurrentSnapshotFlag =1 
		AND POL1.source_sys_id = 'PMS' 
		AND LOC1.SourceSystemID='PMS'
		AND STATCOV1.SourceSystemID='PMS' 
		AND POLCOV1.SourceSystemID='PMS' 
		AND PT1.SourceSystemID='PMS'
		AND datepart(MM,PremiumTransactionBookedDate)=datepart(MM,(DATEADD(MM,@{pipeline().parameters.NO_OF_MONTHS},GETDATE()))) 
		AND datepart(YYYY,PremiumTransactionBookedDate)=datepart(YYYY,(DATEADD(MM,@{pipeline().parameters.NO_OF_MONTHS},GETDATE())))
	    	AND PT1.PremiumType = 'D'
	    	AND POL.pol_key = POL1.pol_key)
	    	AND REINCOV.pol_ak_id = POL.pol_ak_id
		AND POL.crrnt_snpsht_flag = 1 
		AND REINCOV.crrnt_snpsht_flag = 1
		AND POL.source_sys_id = 'PMS' 
		AND REINCOV.source_sys_id = 'PMS'
),
EXP_ReinsCov_Prejoin_ops AS (
	SELECT
	reins_cov_ak_id,
	pol_ak_id,
	reins_ins_line,
	reins_loc_unit_num,
	reins_sub_loc_unit_num,
	reins_risk_unit_grp,
	reins_risk_unit_grp_seq_num,
	reins_risk_unit,
	reins_risk_unit_seq_num,
	reins_section_code,
	-- *INF*: ltrim(rtrim(reins_section_code))
	ltrim(rtrim(reins_section_code)) AS reins_section_code_out,
	reins_eff_date,
	reins_exp_date,
	reins_prcnt_prem_ceded,
	reins_prcnt_facultative_commssn
	FROM SQ_reinsurance_coverage
),
EXP_Reinsurance_PreJoin_ops AS (
	SELECT
	AgencyAKID AS AgencyAKID3,
	PolicyAKID AS PolicyAKID3,
	ContractCustomerAKID AS ContractCustomerAKID3,
	RiskLocationAKID AS RiskLocationAKID3,
	PolicyCoverageAKID AS PolicyCoverageAKID3,
	StatisticalCoverageAKID AS StatisticalCoverageAKID3,
	ReinsuranceCoverageAKID AS ReinsuranceCoverageAKID3,
	PremiumTransactionAKID AS PremiumTransactionAKID3,
	BureauStatisticalCodeAKID AS BureauStatisticalCodeAKID3,
	pol_sym AS pol_sym3,
	pol_num AS pol_num3,
	pol_mod AS pol_mod3,
	pol_key AS pol_key3,
	pol_eff_date AS pol_eff_date3,
	pol_exp_date AS pol_exp_date3,
	pms_pol_lob_code AS pms_pol_lob_code3,
	pol_issue_code AS pol_issue_code3,
	LocationUnitNumber AS LocationUnitNumber3,
	LocationIndicator AS LocationIndicator3,
	InsuranceLine AS InsuranceLine3,
	TypeBureauCode AS TypeBureauCode3,
	PolicyCoverageEffectiveDate AS PolicyCoverageEffectiveDate3,
	PolicyCoverageExpirationDate AS PolicyCoverageExpirationDate3,
	SubLocationUnitNumber AS SubLocationUnitNumber3,
	RiskUnitGroup AS RiskUnitGroup3,
	RiskUnitGroupSequenceNumber AS RiskUnitGroupSequenceNumber3,
	RiskUnit AS RiskUnit3,
	RiskUnitSequenceNumber AS RiskUnitSequenceNumber3,
	MajorPerilCode AS MajorPerilCode3,
	MajorPerilSequenceNumber AS MajorPerilSequenceNumber3,
	SublineCode AS SublineCode3,
	PMSTypeExposure AS PMSTypeExposure3,
	ClassCode AS ClassCode3,
	Exposure AS Exposure3,
	StatisticalCoverageEffectiveDate AS StatisticalCoverageEffectiveDate3,
	StatisticalCoverageExpirationDate AS StatisticalCoverageExpirationDate3,
	AgencyActualCommissionRate AS AgencyActualCommissionRate3,
	ReinsuranceSectionCode AS ReinsuranceSectionCode3,
	-- *INF*: LTRIM(RTRIM(ReinsuranceSectionCode3))
	LTRIM(RTRIM(ReinsuranceSectionCode3)) AS ReinsuranceSectionCode_Out,
	PremiumLoadSequence AS PremiumLoadSequence3,
	PMSFunctionCode AS PMSFunctionCode3,
	PremiumTransactionCode AS PremiumTransactionCode3,
	PremiumTransactionEnteredDate AS PremiumTransactionEnteredDate3,
	PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate3,
	PremiumTransactionExpirationDate AS PremiumTransactionExpirationDate3,
	PremiumTransactionBookedDate AS PremiumTransactionBookedDate3,
	PremiumTransactionAmount AS PremiumTransactionAmount3,
	FullTermPremium AS FullTermPremium3,
	PremiumType AS PremiumType3,
	ReasonAmendedCode AS ReasonAmendedCode3,
	BureauSpecialUseCode AS BureauSpecialUseCode3,
	PMSAnnualStatementLine AS PMSAnnualStatementLine3,
	RatingDateIndicator AS RatingDateIndicator3,
	Prior_Transaction_Found AS Prior_Transaction_Found3,
	BureauStatisticalUserLine AS BureauStatisticalUserLine3,
	Offset_Premium AS Offset_Premium3,
	Offset_Original_Premium AS Offset_Original_Premium3,
	Onset_Premium AS Onset_Premium3,
	Onset_Original_Premium AS Onset_Original_Premium3,
	Offset_Exposure AS Offset_Exposure3,
	Onset_Exposure AS Onset_Exposure3,
	Onset_Transaction_Code AS Onset_Transaction_Code3,
	Offset_Transaction_Code AS Offset_Transaction_Code3,
	Offset_StatisticalCodes AS Offset_StatisticalCodes3,
	Offset_BureauStatisticalUserLine AS Offset_BureauStatisticalUserLine3,
	Offset_ReasonAmendedCode AS Offset_ReasonAmendedCode3,
	Offset_PremiumTransactionExpirationDate AS Offset_PremiumTransactionExpirationDate3,
	Offset_AgencyActualCommissionRate AS Offset_AgencyActualCommissionRate3,
	Offset_SublineCode AS Offset_SublineCode3,
	Offset_ClassCode AS Offset_ClassCode3,
	Status AS Status3,
	StatisticalCodes AS StatisticalCodes3,
	PremiumMasterBureauInceptionDate AS PremiumMasterBureauInceptionDate3,
	PremiumMasterRiskAddress AS PremiumMasterRiskAddress3,
	PremiumMasterRiskCityState AS PremiumMasterRiskCityState3,
	PremiumMasterRenewalIndicator AS PremiumMasterRenewalIndicator3,
	pol_audit_frqncy AS pol_audit_frqncy3,
	pol_term AS pol_term3,
	PremiumMasterCustomerCareCommissionRate AS PremiumMasterCustomerCareCommissionRate3,
	DeclaredEventsFlag AS DeclaredEventsFlag3
	FROM RTR_Onset_Offset_Direct_Part1Ceded_REINSURANCE
),
JNR_DirectPremiumTransaction_ReinsuranceCoverage AS (SELECT
	EXP_Reinsurance_PreJoin_ops.AgencyAKID3, 
	EXP_Reinsurance_PreJoin_ops.PolicyAKID3, 
	EXP_Reinsurance_PreJoin_ops.ContractCustomerAKID3, 
	EXP_Reinsurance_PreJoin_ops.RiskLocationAKID3, 
	EXP_Reinsurance_PreJoin_ops.PolicyCoverageAKID3, 
	EXP_Reinsurance_PreJoin_ops.StatisticalCoverageAKID3, 
	EXP_Reinsurance_PreJoin_ops.ReinsuranceCoverageAKID3, 
	EXP_Reinsurance_PreJoin_ops.PremiumTransactionAKID3, 
	EXP_Reinsurance_PreJoin_ops.BureauStatisticalCodeAKID3, 
	EXP_Reinsurance_PreJoin_ops.pol_sym3, 
	EXP_Reinsurance_PreJoin_ops.pol_num3, 
	EXP_Reinsurance_PreJoin_ops.pol_mod3, 
	EXP_Reinsurance_PreJoin_ops.pol_key3, 
	EXP_Reinsurance_PreJoin_ops.pol_eff_date3, 
	EXP_Reinsurance_PreJoin_ops.pol_exp_date3, 
	EXP_Reinsurance_PreJoin_ops.pms_pol_lob_code3, 
	EXP_Reinsurance_PreJoin_ops.pol_term3, 
	EXP_Reinsurance_PreJoin_ops.pol_issue_code3, 
	EXP_Reinsurance_PreJoin_ops.LocationUnitNumber3, 
	EXP_Reinsurance_PreJoin_ops.LocationIndicator3, 
	EXP_Reinsurance_PreJoin_ops.InsuranceLine3, 
	EXP_Reinsurance_PreJoin_ops.TypeBureauCode3, 
	EXP_Reinsurance_PreJoin_ops.PolicyCoverageEffectiveDate3, 
	EXP_Reinsurance_PreJoin_ops.PolicyCoverageExpirationDate3, 
	EXP_Reinsurance_PreJoin_ops.SubLocationUnitNumber3, 
	EXP_Reinsurance_PreJoin_ops.RiskUnitGroup3, 
	EXP_Reinsurance_PreJoin_ops.RiskUnitGroupSequenceNumber3, 
	EXP_Reinsurance_PreJoin_ops.RiskUnit3, 
	EXP_Reinsurance_PreJoin_ops.RiskUnitSequenceNumber3, 
	EXP_Reinsurance_PreJoin_ops.MajorPerilCode3, 
	EXP_Reinsurance_PreJoin_ops.MajorPerilSequenceNumber3, 
	EXP_Reinsurance_PreJoin_ops.SublineCode3, 
	EXP_Reinsurance_PreJoin_ops.PMSTypeExposure3, 
	EXP_Reinsurance_PreJoin_ops.ClassCode3, 
	EXP_Reinsurance_PreJoin_ops.Exposure3, 
	EXP_Reinsurance_PreJoin_ops.StatisticalCoverageEffectiveDate3, 
	EXP_Reinsurance_PreJoin_ops.StatisticalCoverageExpirationDate3, 
	EXP_Reinsurance_PreJoin_ops.AgencyActualCommissionRate3, 
	EXP_Reinsurance_PreJoin_ops.ReinsuranceSectionCode_Out, 
	EXP_Reinsurance_PreJoin_ops.PremiumLoadSequence3, 
	EXP_Reinsurance_PreJoin_ops.PMSFunctionCode3, 
	EXP_Reinsurance_PreJoin_ops.PremiumTransactionCode3, 
	EXP_Reinsurance_PreJoin_ops.PremiumTransactionEnteredDate3, 
	EXP_Reinsurance_PreJoin_ops.PremiumTransactionEffectiveDate3, 
	EXP_Reinsurance_PreJoin_ops.PremiumTransactionExpirationDate3, 
	EXP_Reinsurance_PreJoin_ops.PremiumTransactionBookedDate3, 
	EXP_Reinsurance_PreJoin_ops.PremiumTransactionAmount3, 
	EXP_Reinsurance_PreJoin_ops.FullTermPremium3, 
	EXP_Reinsurance_PreJoin_ops.PremiumType3, 
	EXP_Reinsurance_PreJoin_ops.ReasonAmendedCode3, 
	EXP_Reinsurance_PreJoin_ops.BureauSpecialUseCode3, 
	EXP_Reinsurance_PreJoin_ops.PMSAnnualStatementLine3, 
	EXP_Reinsurance_PreJoin_ops.RatingDateIndicator3, 
	EXP_Reinsurance_PreJoin_ops.Prior_Transaction_Found3, 
	EXP_Reinsurance_PreJoin_ops.BureauStatisticalUserLine3, 
	EXP_Reinsurance_PreJoin_ops.Offset_Premium3, 
	EXP_Reinsurance_PreJoin_ops.Offset_Original_Premium3, 
	EXP_Reinsurance_PreJoin_ops.Onset_Premium3, 
	EXP_Reinsurance_PreJoin_ops.Onset_Original_Premium3, 
	EXP_Reinsurance_PreJoin_ops.Offset_Exposure3, 
	EXP_Reinsurance_PreJoin_ops.Onset_Exposure3, 
	EXP_Reinsurance_PreJoin_ops.Onset_Transaction_Code3, 
	EXP_Reinsurance_PreJoin_ops.Offset_Transaction_Code3, 
	EXP_Reinsurance_PreJoin_ops.Offset_StatisticalCodes3, 
	EXP_Reinsurance_PreJoin_ops.Offset_BureauStatisticalUserLine3, 
	EXP_Reinsurance_PreJoin_ops.Offset_ReasonAmendedCode3, 
	EXP_Reinsurance_PreJoin_ops.Offset_PremiumTransactionExpirationDate3, 
	EXP_Reinsurance_PreJoin_ops.Offset_AgencyActualCommissionRate3, 
	EXP_Reinsurance_PreJoin_ops.Offset_SublineCode3, 
	EXP_Reinsurance_PreJoin_ops.Offset_ClassCode3, 
	EXP_Reinsurance_PreJoin_ops.Status3, 
	EXP_Reinsurance_PreJoin_ops.StatisticalCodes3, 
	EXP_Reinsurance_PreJoin_ops.PremiumMasterBureauInceptionDate3, 
	EXP_Reinsurance_PreJoin_ops.PremiumMasterRiskAddress3, 
	EXP_Reinsurance_PreJoin_ops.PremiumMasterRiskCityState3, 
	EXP_Reinsurance_PreJoin_ops.PremiumMasterRenewalIndicator3, 
	EXP_Reinsurance_PreJoin_ops.pol_audit_frqncy3, 
	EXP_Reinsurance_PreJoin_ops.PremiumMasterCustomerCareCommissionRate3, 
	EXP_ReinsCov_Prejoin_ops.reins_cov_ak_id, 
	EXP_ReinsCov_Prejoin_ops.pol_ak_id, 
	EXP_ReinsCov_Prejoin_ops.reins_ins_line, 
	EXP_ReinsCov_Prejoin_ops.reins_loc_unit_num, 
	EXP_ReinsCov_Prejoin_ops.reins_sub_loc_unit_num, 
	EXP_ReinsCov_Prejoin_ops.reins_risk_unit_grp, 
	EXP_ReinsCov_Prejoin_ops.reins_risk_unit_grp_seq_num, 
	EXP_ReinsCov_Prejoin_ops.reins_risk_unit, 
	EXP_ReinsCov_Prejoin_ops.reins_risk_unit_seq_num, 
	EXP_ReinsCov_Prejoin_ops.reins_section_code_out, 
	EXP_ReinsCov_Prejoin_ops.reins_eff_date, 
	EXP_ReinsCov_Prejoin_ops.reins_exp_date, 
	EXP_ReinsCov_Prejoin_ops.reins_prcnt_prem_ceded, 
	EXP_ReinsCov_Prejoin_ops.reins_prcnt_facultative_commssn, 
	EXP_Reinsurance_PreJoin_ops.DeclaredEventsFlag3
	FROM EXP_Reinsurance_PreJoin_ops
	INNER JOIN EXP_ReinsCov_Prejoin_ops
	ON EXP_ReinsCov_Prejoin_ops.pol_ak_id = EXP_Reinsurance_PreJoin_ops.PolicyAKID3 AND EXP_ReinsCov_Prejoin_ops.reins_section_code_out = EXP_Reinsurance_PreJoin_ops.ReinsuranceSectionCode_Out
),
EXP_Generate_DirectPremium_Reinsurance AS (
	SELECT
	pol_sym3 AS pol_sym,
	pol_num3 AS pol_num,
	pol_mod3 AS pol_mod,
	PMSFunctionCode3,
	reins_section_code_out AS reins_section_code,
	reins_ins_line AS reins_insurance_line,
	reins_loc_unit_num AS reins_location_number,
	reins_sub_loc_unit_num AS reins_sub_location_number,
	reins_risk_unit_grp AS reins_risk_unit_group,
	-- *INF*: TO_CHAR(reins_risk_unit_group)
	TO_CHAR(reins_risk_unit_group) AS v_reins_risk_unit_group,
	reins_risk_unit_grp_seq_num AS reins_seq_rsk_unt_grp,
	reins_risk_unit_seq_num AS reins_risk_sequence,
	reins_risk_unit AS reins_location,
	reins_eff_date AS reins_effective_date,
	reins_prcnt_prem_ceded AS reins_percent_prem_ceded,
	reins_prcnt_facultative_commssn AS reins_percent_fac_comm,
	reins_exp_date AS reins_expiration_date,
	RiskUnit3,
	PolicyCoverageExpirationDate3,
	LocationUnitNumber3,
	-- *INF*: IIF(LocationIndicator3 = 'N','N/A',LocationUnitNumber3)
	IFF(LocationIndicator3 = 'N', 'N/A', LocationUnitNumber3) AS v_LocationUnitNumber,
	LocationIndicator3,
	InsuranceLine3,
	SubLocationUnitNumber3,
	RiskUnitGroup3,
	RiskUnitGroupSequenceNumber3,
	RiskUnitSequenceNumber3,
	PMSTypeExposure3,
	-- *INF*: TO_DECIMAL(reins_location_number)
	CAST(reins_location_number AS FLOAT) AS v_reins_location_number,
	-- *INF*: TO_DECIMAL(LocationUnitNumber3)
	CAST(LocationUnitNumber3 AS FLOAT) AS v_location_number,
	MajorPerilCode3,
	pms_pol_lob_code3,
	PremiumTransactionCode3,
	PremiumTransactionAmount3,
	FullTermPremium3,
	PremiumTransactionEffectiveDate3,
	PremiumTransactionExpirationDate3,
	-- *INF*: TO_INTEGER(:LKP.LKP_PIF_02_FOR_FIELDS(pol_sym,pol_num,pol_mod))
	-- 
	-- -- Lookup to arch_pif_02 to determine pif_number_installments
	CAST(LKP_PIF_02_FOR_FIELDS_pol_sym_pol_num_pol_mod.pif_number_installments_a AS INTEGER) AS v_pif_number_installments,
	-- *INF*:  IIF((v_pif_number_installments>1) ,
	-- IIF((reins_expiration_date>Hold_sar_exp_date)
	--          ,Hold_sar_exp_date,reins_expiration_date),reins_expiration_date)
	-- 
	-- -- see comments
	IFF(
	    (v_pif_number_installments > 1),
	    IFF(
	        (reins_expiration_date > Hold_sar_exp_date), Hold_sar_exp_date,
	        reins_expiration_date
	    ),
	    reins_expiration_date
	) AS ReinsuranceCoverageExpirationDate,
	ReasonAmendedCode3,
	PMSAnnualStatementLine3 AS PMSAnnualStatementLine,
	ReinsuranceSectionCode_Out AS ReinsuranceSectionCode3,
	-- *INF*:  IIF(NOT IN(LTRIM(RTRIM(pms_pol_lob_code3)),'HP') ,
	-- IIF((reins_expiration_date>=reins_effective_date)
	--         AND
	--        ((PremiumTransactionEffectiveDate3 >= reins_effective_date) AND (PremiumTransactionEffectiveDate3 < reins_expiration_date)
	--          OR
	--          (PremiumTransactionExpirationDate3 > reins_effective_date) AND (PremiumTransactionExpirationDate3 <= reins_expiration_date )
	--          OR
	--          (PremiumTransactionEffectiveDate3 <= reins_effective_date) AND (PremiumTransactionExpirationDate3 >= reins_expiration_date))
	--          AND NOT IN (MajorPerilCode3,'042','044','183')
	--          ,'VALID','INVALID'),'INVALID')
	-- 
	-- -- see comments
	-- 
	IFF(
	    NOT LTRIM(RTRIM(pms_pol_lob_code3)) IN ('HP'),
	    IFF(
	        (reins_expiration_date >= reins_effective_date)
	        and ((PremiumTransactionEffectiveDate3 >= reins_effective_date)
	        and (PremiumTransactionEffectiveDate3 < reins_expiration_date)
	        or (PremiumTransactionExpirationDate3 > reins_effective_date)
	        and (PremiumTransactionExpirationDate3 <= reins_expiration_date)
	        or (PremiumTransactionEffectiveDate3 <= reins_effective_date)
	        and (PremiumTransactionExpirationDate3 >= reins_expiration_date))
	        and NOT MajorPerilCode3 IN ('042','044','183'),
	        'VALID',
	        'INVALID'
	    ),
	    'INVALID'
	) AS v_reins_validity,
	-- *INF*: DECODE(v_reins_validity,'INVALID','NOREINSURANCE',
	-- 'VALID',IIF(LTRIM(RTRIM(reins_insurance_line)) = 'GL' and v_reins_location_number <> v_location_number,'NOREINSURANCE',
	-- IIF(RTRIM(reins_location)=RiskUnit3 OR RTRIM(reins_location) = 'N/A','REINSURANCE','NOREINSURANCE')))
	-- 
	-- 
	-- --DECODE(v_reins_validity,'INVALID','NOREINSURANCE',
	-- --'VALID',IIF(sar_id5 ='51' AND v_reins_risk_unit_group = '286',IIF(reins_insurance_line = sar_insurance_line AND reins_location_number = --v_sar_location  AND v_reins_risk_unit_group = sar_risk_unit_group, 'REINSURANCE','NOREINSURANCE'),
	-- --IIF(ISNULL(reins_location_number),
	-- --IIF(reins_location=sar_unit5 OR IS_SPACES(reins_location) OR reins_location = '0','REINSURANCE','NOREINSURANCE'),
	-- --IIF(reins_location_number = v_sar_location,
	-- --'REINSURANCE','NOREINSURANCE')))
	-- --)
	-- 
	-- 
	-- 
	-- 
	-- 
	DECODE(
	    v_reins_validity,
	    'INVALID', 'NOREINSURANCE',
	    'VALID', IFF(
	        LTRIM(RTRIM(reins_insurance_line)) = 'GL' and v_reins_location_number <> v_location_number,
	        'NOREINSURANCE',
	        IFF(
	            RTRIM(reins_location) = RiskUnit3
	        or RTRIM(reins_location) = 'N/A',
	            'REINSURANCE',
	            'NOREINSURANCE'
	        )
	    )
	) AS v_reinsurance_flag,
	v_reinsurance_flag AS reinsurance_flag,
	-- *INF*: DECODE(v_reinsurance_flag,'NOREINSURANCE','NOGENERATE',
	--                                                              'REINSURANCE',
	-- IIF(
	--        (IN(MajorPerilCode3,'088','089','127','184','128','188') OR  
	--                ( IN(ReasonAmendedCode3,'COL','CWO'))  OR 
	--                      IN(PremiumTransactionCode3,'59','69') ),
	--                                                        'NOGENERATE','GENERATE'))
	-- 
	-- 
	-- 
	-- --DECODE(v_reinsurance_flag,'NOREINSURANCE','NOGENERATE',
	-- --                                                             'REINSURANCE',
	-- --IIF(
	-- --       (IN(MajorPerilCode3,'088','089','127','184','128','188') OR  
	-- --               (ReinsuranceSectionCode3 = '2' AND IN(ReasonAmendedCode3,'COL','CWO'))  OR 
	-- --                     IN(PremiumTransactionCode3,'59','69') ),
	--    --                                                    'NOGENERATE','GENERATE'))
	-- 
	-- ---OR (sar_insurance_line = 'GL' and sar_risk_unit_group = '286')
	-- 
	-- 
	-- 
	DECODE(
	    v_reinsurance_flag,
	    'NOREINSURANCE', 'NOGENERATE',
	    'REINSURANCE', IFF(
	        (MajorPerilCode3 IN ('088','089','127','184','128','188')
	        or (ReasonAmendedCode3 IN ('COL','CWO'))
	        or PremiumTransactionCode3 IN ('59','69')),
	        'NOGENERATE',
	        'GENERATE'
	    )
	) AS v_Reins_generate_flag,
	v_Reins_generate_flag AS Generate_flag,
	StatisticalCoverageEffectiveDate3,
	-- *INF*: IIF(IN(PremiumTransactionCode3,'14','24'),StatisticalCoverageEffectiveDate3,PremiumTransactionEffectiveDate3)
	IFF(
	    PremiumTransactionCode3 IN ('14','24'), StatisticalCoverageEffectiveDate3,
	    PremiumTransactionEffectiveDate3
	) AS Hold_sar_eff_date,
	-- *INF*: IIF(IN(PremiumTransactionCode3,'14','24'),PremiumTransactionEffectiveDate3,PremiumTransactionExpirationDate3)
	IFF(
	    PremiumTransactionCode3 IN ('14','24'), PremiumTransactionEffectiveDate3,
	    PremiumTransactionExpirationDate3
	) AS Hold_sar_exp_date,
	-- *INF*: DATE_DIFF(PremiumTransactionEffectiveDate3,PremiumTransactionExpirationDate3,'D')
	DATEDIFF(DAY,PremiumTransactionEffectiveDate3,PremiumTransactionExpirationDate3) AS Length_of_Stat,
	-- *INF*: IIF(PremiumTransactionEffectiveDate3 >= reins_effective_date 
	--                            AND PremiumTransactionEffectiveDate3 < reins_expiration_date,
	--                            DATE_DIFF(PremiumTransactionEffectiveDate3,reins_expiration_date,'D'),
	--            IIF(PremiumTransactionExpirationDate3 > reins_effective_date  
	--                          AND PremiumTransactionExpirationDate3 <= reins_expiration_date,
	--                            DATE_DIFF(reins_effective_date,PremiumTransactionExpirationDate3,'D'),DATE_DIFF(reins_effective_date,reins_expiration_date,'D')))
	IFF(
	    PremiumTransactionEffectiveDate3 >= reins_effective_date
	    and PremiumTransactionEffectiveDate3 < reins_expiration_date,
	    DATEDIFF(DAY,PremiumTransactionEffectiveDate3,reins_expiration_date),
	    IFF(
	        PremiumTransactionExpirationDate3 > reins_effective_date
	        and PremiumTransactionExpirationDate3 <= reins_expiration_date,
	        DATEDIFF(DAY,reins_effective_date,PremiumTransactionExpirationDate3),
	        DATEDIFF(DAY,reins_effective_date,reins_expiration_date)
	    )
	) AS Length_of_Stat_Overlap,
	-- *INF*: IIF(PremiumTransactionExpirationDate3 < reins_effective_date OR PremiumTransactionEffectiveDate3 > reins_expiration_date, 0,
	-- 	IIF (PremiumTransactionEffectiveDate3 >= reins_effective_date AND PremiumTransactionExpirationDate3 <= reins_expiration_date,
	-- 		IIF((Hold_sar_eff_date >=reins_effective_date AND reins_expiration_date >= Hold_sar_exp_date) OR (reins_effective_date > Hold_sar_exp_date),1,ROUND(Length_of_Stat_Overlap / Length_of_Stat, 2) )))
	-- 
	-- --IIF(PremiumTransactionEffectiveDate3 >= reins_effective_date AND PremiumTransactionExpirationDate3 <=reins_expiration_date,1,
	--    --     IIF((Hold_sar_eff_date >= reins_effective_date AND reins_expiration_date >= Hold_sar_exp_date) OR 
	--      --                  ( reins_effective_date > Hold_sar_exp_date),1,ROUND(Length_of_Stat_Overlap/Length_of_Stat,2)))
	-- 
	-- 
	-- 
	-- 
	-- ---IIF((Hold_sar_eff_date >= v_reins_effective_date AND v_reins_expiration_date >= Hold_sar_exp_date) OR 
	--     --   (v_reins_effective_date > Hold_sar_exp_date),1,ROUND(Length_of_Stat_Overlap/Length_of_Stat,2))
	-- 
	-- 
	-- ---IIF(sar_trans_eff_date >=v_reins_effective_date AND sar_expiration_date <= v_reins_expiration_date,1,ROUND(Length_of_Stat_Overlap/Length_of_Stat,2))
	-- 
	-- 
	-- 
	-- 
	IFF(
	    PremiumTransactionExpirationDate3 < reins_effective_date
	    or PremiumTransactionEffectiveDate3 > reins_expiration_date,
	    0,
	    IFF(
	        PremiumTransactionEffectiveDate3 >= reins_effective_date
	        and PremiumTransactionExpirationDate3 <= reins_expiration_date,
	        IFF(
	            (Hold_sar_eff_date >= reins_effective_date
	            and reins_expiration_date >= Hold_sar_exp_date)
	            or (reins_effective_date > Hold_sar_exp_date),
	            1,
	            ROUND(Length_of_Stat_Overlap / Length_of_Stat, 2)
	        )
	    )
	) AS I_Pro_Rata_Factor,
	-- *INF*: ROUND(PremiumTransactionAmount3 * reins_percent_prem_ceded * I_Pro_Rata_Factor,3)
	ROUND(PremiumTransactionAmount3 * reins_percent_prem_ceded * I_Pro_Rata_Factor, 3) AS V_REINS_CEDED_PREMIUM,
	V_REINS_CEDED_PREMIUM AS OUT_REINS_CEDED_PREMIUM,
	-- *INF*: ROUND(FullTermPremium3 * reins_percent_prem_ceded  * I_Pro_Rata_Factor ,3)
	ROUND(FullTermPremium3 * reins_percent_prem_ceded * I_Pro_Rata_Factor, 3) AS V_REINS_CEDED_ORIG_PREMIUM,
	V_REINS_CEDED_ORIG_PREMIUM AS OUT_REINS_CEDED_ORIG_PREMIUM,
	MajorPerilSequenceNumber3,
	PremiumTransactionEnteredDate3,
	SublineCode3 AS SubLineCode3,
	BureauStatisticalUserLine3,
	TypeBureauCode3,
	ClassCode3,
	-- *INF*: 0
	-- 
	-- -- Always zero for derived ceded reinsurance records
	0 AS Exposure3,
	-- *INF*: 0
	-- 
	-- -- Always zero for derived ceded reinsurance records
	0 AS WrittenExposure,
	StatisticalCodes3,
	reins_percent_fac_comm AS AgencyActualCommissionRate3,
	pol_audit_frqncy3,
	PremiumTransactionBookedDate3,
	RatingDateIndicator3,
	pol_eff_date3,
	pol_exp_date3,
	BureauSpecialUseCode3,
	'PMS' AS o_SourceSystemID,
	AgencyAKID3,
	PolicyAKID3,
	ContractCustomerAKID3,
	RiskLocationAKID3,
	PolicyCoverageAKID3,
	StatisticalCoverageAKID3,
	reins_cov_ak_id AS ReinsuranceCoverageAKID3,
	PremiumTransactionAKID3,
	BureauStatisticalCodeAKID3,
	Offset_Premium3,
	Offset_Original_Premium3,
	Onset_Premium3,
	Onset_Original_Premium3,
	Prior_Transaction_Found3,
	Status3,
	Offset_Exposure3,
	Onset_Exposure3,
	pol_key3,
	StatisticalCoverageExpirationDate3,
	PolicyCoverageEffectiveDate3,
	PremiumLoadSequence3,
	PremiumType3,
	Onset_Transaction_Code3,
	Offset_Transaction_Code3,
	Offset_StatisticalCodes3,
	Offset_BureauStatisticalUserLine3,
	Offset_StateProvinceCode3,
	Offset_ReasonAmendedCode3,
	Offset_PremiumTransactionExpirationDate3,
	Offset_AgencyActualCommissionRate3,
	Offset_SublineCode3,
	Offset_ClassCode3,
	PremiumMasterBureauInceptionDate3,
	PremiumMasterRiskAddress3,
	PremiumMasterRiskCityState3,
	PremiumMasterRenewalIndicator3,
	pol_issue_code3,
	'C' AS PremiumMasterPremiumType3,
	'REINSURANCE' AS PremiumMasterStatus3,
	pol_term3,
	-1 AS o_RatingCoverageAKId,
	-- *INF*: TO_DATE('21001231235959','YYYYMMDDHH24MISS')
	TO_TIMESTAMP('21001231235959', 'YYYYMMDDHH24MISS') AS o_RatingCoverageEffectiveDate,
	-- *INF*: TO_DATE('21001231235959','YYYYMMDDHH24MISS')
	TO_TIMESTAMP('21001231235959', 'YYYYMMDDHH24MISS') AS o_RatingCoverageExpirationDate,
	PremiumMasterCustomerCareCommissionRate3,
	DeclaredEventsFlag3
	FROM JNR_DirectPremiumTransaction_ReinsuranceCoverage
	LEFT JOIN LKP_PIF_02_FOR_FIELDS LKP_PIF_02_FOR_FIELDS_pol_sym_pol_num_pol_mod
	ON LKP_PIF_02_FOR_FIELDS_pol_sym_pol_num_pol_mod.pif_symbol = pol_sym
	AND LKP_PIF_02_FOR_FIELDS_pol_sym_pol_num_pol_mod.pif_policy_number = pol_num
	AND LKP_PIF_02_FOR_FIELDS_pol_sym_pol_num_pol_mod.pif_module = pol_mod

),
FIL_Non_Reinsurance AS (
	SELECT
	o_SourceSystemID AS SourceSystemID, 
	AgencyAKID3, 
	PolicyAKID3, 
	ContractCustomerAKID3, 
	RiskLocationAKID3, 
	PolicyCoverageAKID3, 
	StatisticalCoverageAKID3, 
	ReinsuranceCoverageAKID3, 
	PremiumTransactionAKID3, 
	BureauStatisticalCodeAKID3, 
	pol_sym AS pol_sym3, 
	pol_num AS pol_num3, 
	pol_mod AS pol_mod3, 
	pol_key3, 
	pol_eff_date3, 
	pol_exp_date3, 
	pms_pol_lob_code3, 
	pol_issue_code3, 
	LocationUnitNumber3, 
	LocationIndicator3, 
	InsuranceLine3, 
	TypeBureauCode3, 
	PolicyCoverageEffectiveDate3, 
	PolicyCoverageExpirationDate3, 
	SubLocationUnitNumber3, 
	RiskUnitGroup3, 
	RiskUnitGroupSequenceNumber3, 
	RiskUnit3, 
	RiskUnitSequenceNumber3, 
	MajorPerilCode3, 
	MajorPerilSequenceNumber3, 
	SubLineCode3 AS SublineCode3, 
	PMSTypeExposure3, 
	ClassCode3, 
	Exposure3, 
	StatisticalCoverageEffectiveDate3, 
	StatisticalCoverageExpirationDate3, 
	AgencyActualCommissionRate3, 
	ReinsuranceSectionCode3, 
	PremiumLoadSequence3, 
	PMSFunctionCode3, 
	PremiumTransactionCode3, 
	PremiumTransactionEnteredDate3, 
	PremiumTransactionEffectiveDate3, 
	ReinsuranceCoverageExpirationDate AS PremiumTransactionExpirationDate3, 
	PremiumTransactionBookedDate3, 
	OUT_REINS_CEDED_PREMIUM AS PremiumTransactionAmount3, 
	OUT_REINS_CEDED_ORIG_PREMIUM AS FullTermPremium3, 
	ReasonAmendedCode3, 
	BureauSpecialUseCode3, 
	PMSAnnualStatementLine AS PMSAnnualStatementLine3, 
	RatingDateIndicator3, 
	BureauStatisticalUserLine3, 
	PremiumMasterPremiumType3, 
	Status3, 
	PremiumMasterStatus3, 
	StatisticalCodes3, 
	PremiumMasterBureauInceptionDate3, 
	PremiumMasterRiskAddress3, 
	PremiumMasterRiskCityState3, 
	PremiumMasterRenewalIndicator3, 
	Generate_flag AS generate_flag, 
	pol_audit_frqncy3, 
	pol_term3, 
	o_RatingCoverageAKId AS RatingCoverageAKId, 
	o_RatingCoverageEffectiveDate AS RatingCoverageEffectiveDate, 
	o_RatingCoverageExpirationDate AS RatingCoverageExpirationDate, 
	PremiumMasterCustomerCareCommissionRate3, 
	WrittenExposure, 
	DeclaredEventsFlag3
	FROM EXP_Generate_DirectPremium_Reinsurance
	WHERE generate_flag ='GENERATE'
),
Union_All_Part1_Pipelines AS (
	SELECT o_SourceSystemID AS SourceSystemID, AgencyAKID5 AS AgencyAKID, PolicyAKID5 AS PolicyAKID, ContractCustomerAKID5 AS ContractCustomerAKID, RiskLocationAKID5 AS RiskLocationAKID, PolicyCoverageAKID5 AS PolicyCoverageAKID, StatisticalCoverageAKID5 AS StatisticalCoverageAKID, ReinsuranceCoverageAKID5 AS ReinsuranceCoverageAKID, PremiumTransactionAKID5 AS PremiumTransactionAKID, BureauStatisticalCodeAKID5 AS BureauStatisticalCodeAKID, pol_sym5 AS pol_sym, pol_num5 AS pol_num, pol_mod5 AS pol_mod, pol_key5 AS pol_key, pol_eff_date5 AS pol_eff_date, pol_exp_date5 AS pol_exp_date, pms_pol_lob_code5 AS pms_pol_lob_code, pol_issue_code5 AS pol_issue_code, LocationUnitNumber5 AS LocationUnitNumber, LocationIndicator5 AS LocationIndicator, InsuranceLine5 AS InsuranceLine, TypeBureauCode5 AS TypeBureauCode, PolicyCoverageEffectiveDate5 AS PolicyCoverageEffectiveDate, PolicyCoverageExpirationDate5 AS PolicyCoverageExpirationDate, SubLocationUnitNumber5 AS SubLocationUnitNumber, RiskUnitGroup5 AS RiskUnitGroup, RiskUnitGroupSequenceNumber5 AS RiskUnitGroupSequenceNumber, RiskUnit5 AS RiskUnit, RiskUnitSequenceNumber5 AS RiskUnitSequenceNumber, MajorPerilCode5 AS MajorPerilCode, MajorPerilSequenceNumber5 AS MajorPerilSequenceNumber, SublineCode5 AS SublineCode, PMSTypeExposure5 AS PMSTypeExposure, StatisticalCoverageEffectiveDate5 AS StatisticalCoverageEffectiveDate, StatisticalCoverageExpirationDate5 AS StatisticalCoverageExpirationDate, PremiumLoadSequence5 AS PremiumLoadSequence, PMSFunctionCode5 AS PMSFunctionCode, PremiumTransactionCode5 AS PremiumTransactionCode, PremiumTransactionEnteredDate5 AS PremiumTransactionEnteredDate, PremiumTransactionEffectiveDate5 AS PremiumTransactionEffectiveDate, ClassCode5 AS ClassCode, PremiumTransactionExpirationDate5 AS PremiumTransactionExpirationDate, PremiumTransactionBookedDate5 AS PremiumTransactionBookedDate, ReasonAmendedCode5 AS ReasonAmendedCode, BureauSpecialUseCode5 AS BureauSpecialUseCode, PMSAnnualStatementLine5 AS PMSAnnualStatementLine, AgencyActualCommissionRate5 AS AgencyActualCommissionRate, Exposure5 AS Exposure, RatingDateIndicator5 AS RatingDateIndicator, BureauStatisticalUserLine5 AS BureauStatisticalUserLine, StatisticalCodes5 AS StatisticalCodes, PremiumTransactionAmount5 AS PremiumTransactionAmount, FullTermPremium5 AS FullTermPremium, PremiumMasterBureauInceptionDate5 AS PremiumMasterBureauInceptionDate, PremiumMasterRiskAddress5 AS PremiumMasterRiskAddress, PremiumMasterRiskCityState5 AS PremiumMasterRiskCityState, PremiumMasterRenewalIndicator5 AS PremiumMasterRenewalIndicator, PremiumType5 AS PremiumMasterPremiumType, Status5 AS Status, PremiumMasterStatus5 AS PremiumMasterStatus, pol_audit_frqncy5 AS pol_audit_frqncy, pol_term5 AS pol_term, o_RatingCoverageAKId AS RatingCoverageAKID, o_RatingCoverageEffectiveDate AS RatingCoverageEffectiveDate, o_RatingCoverageExpirationDate AS RatingCoverageExpirationDate, PremiumMasterCustomerCareCommissionRate5 AS PremiumMasterCustomerCareCommissionRate, WrittenExposure5 AS PemiumMasterWrittenExposure, DeclaredEventsFlag5 AS DeclaredEventFlag
	FROM EXP_generate_PremiumMasterRecord
	UNION
	SELECT SourceSystemID, AgencyAKID3 AS AgencyAKID, PolicyAKID3 AS PolicyAKID, ContractCustomerAKID3 AS ContractCustomerAKID, RiskLocationAKID3 AS RiskLocationAKID, PolicyCoverageAKID3 AS PolicyCoverageAKID, StatisticalCoverageAKID3 AS StatisticalCoverageAKID, ReinsuranceCoverageAKID3 AS ReinsuranceCoverageAKID, PremiumTransactionAKID3 AS PremiumTransactionAKID, BureauStatisticalCodeAKID3 AS BureauStatisticalCodeAKID, pol_sym3 AS pol_sym, pol_num3 AS pol_num, pol_mod3 AS pol_mod, pol_key3 AS pol_key, pol_eff_date3 AS pol_eff_date, pol_exp_date3 AS pol_exp_date, pms_pol_lob_code3 AS pms_pol_lob_code, pol_issue_code3 AS pol_issue_code, LocationUnitNumber3 AS LocationUnitNumber, LocationIndicator3 AS LocationIndicator, InsuranceLine3 AS InsuranceLine, TypeBureauCode3 AS TypeBureauCode, PolicyCoverageEffectiveDate3 AS PolicyCoverageEffectiveDate, PolicyCoverageExpirationDate3 AS PolicyCoverageExpirationDate, SubLocationUnitNumber3 AS SubLocationUnitNumber, RiskUnitGroup3 AS RiskUnitGroup, RiskUnitGroupSequenceNumber3 AS RiskUnitGroupSequenceNumber, RiskUnit3 AS RiskUnit, RiskUnitSequenceNumber3 AS RiskUnitSequenceNumber, MajorPerilCode3 AS MajorPerilCode, MajorPerilSequenceNumber3 AS MajorPerilSequenceNumber, SublineCode3 AS SublineCode, PMSTypeExposure3 AS PMSTypeExposure, StatisticalCoverageEffectiveDate3 AS StatisticalCoverageEffectiveDate, StatisticalCoverageExpirationDate3 AS StatisticalCoverageExpirationDate, PremiumLoadSequence3 AS PremiumLoadSequence, PMSFunctionCode3 AS PMSFunctionCode, PremiumTransactionCode3 AS PremiumTransactionCode, PremiumTransactionEnteredDate3 AS PremiumTransactionEnteredDate, PremiumTransactionEffectiveDate3 AS PremiumTransactionEffectiveDate, ClassCode3 AS ClassCode, PremiumTransactionExpirationDate3 AS PremiumTransactionExpirationDate, PremiumTransactionBookedDate3 AS PremiumTransactionBookedDate, ReasonAmendedCode3 AS ReasonAmendedCode, BureauSpecialUseCode3 AS BureauSpecialUseCode, PMSAnnualStatementLine3 AS PMSAnnualStatementLine, AgencyActualCommissionRate3 AS AgencyActualCommissionRate, Exposure3 AS Exposure, RatingDateIndicator3 AS RatingDateIndicator, BureauStatisticalUserLine3 AS BureauStatisticalUserLine, StatisticalCodes3 AS StatisticalCodes, PremiumTransactionAmount3 AS PremiumTransactionAmount, FullTermPremium3 AS FullTermPremium, PremiumMasterBureauInceptionDate3 AS PremiumMasterBureauInceptionDate, PremiumMasterRiskAddress3 AS PremiumMasterRiskAddress, PremiumMasterRiskCityState3 AS PremiumMasterRiskCityState, PremiumMasterRenewalIndicator3 AS PremiumMasterRenewalIndicator, PremiumMasterPremiumType3 AS PremiumMasterPremiumType, Status3 AS Status, PremiumMasterStatus3 AS PremiumMasterStatus, pol_audit_frqncy3 AS pol_audit_frqncy, pol_term3 AS pol_term, RatingCoverageAKId AS RatingCoverageAKID, RatingCoverageEffectiveDate, RatingCoverageExpirationDate, PremiumMasterCustomerCareCommissionRate3 AS PremiumMasterCustomerCareCommissionRate, WrittenExposure AS PemiumMasterWrittenExposure, DeclaredEventsFlag3 AS DeclaredEventFlag
	FROM FIL_Non_Reinsurance
	UNION
	SELECT o_SourceSystemID AS SourceSystemID, AgencyAKID1 AS AgencyAKID, PolicyAKID1 AS PolicyAKID, ContractCustomerAKID1 AS ContractCustomerAKID, RiskLocationAKID1 AS RiskLocationAKID, PolicyCoverageAKID1 AS PolicyCoverageAKID, StatisticalCoverageAKID1 AS StatisticalCoverageAKID, ReinsuranceCoverageAKID1 AS ReinsuranceCoverageAKID, PremiumTransactionAKID1 AS PremiumTransactionAKID, BureauStatisticalCodeAKID1 AS BureauStatisticalCodeAKID, pol_sym1 AS pol_sym, pol_num1 AS pol_num, pol_mod1 AS pol_mod, pol_key1 AS pol_key, pol_eff_date1 AS pol_eff_date, pol_exp_date1 AS pol_exp_date, pms_pol_lob_code1 AS pms_pol_lob_code, pol_issue_code1 AS pol_issue_code, LocationUnitNumber1 AS LocationUnitNumber, LocationIndicator1 AS LocationIndicator, InsuranceLine1 AS InsuranceLine, TypeBureauCode1 AS TypeBureauCode, PolicyCoverageEffectiveDate1 AS PolicyCoverageEffectiveDate, PolicyCoverageExpirationDate1 AS PolicyCoverageExpirationDate, SubLocationUnitNumber1 AS SubLocationUnitNumber, RiskUnitGroup1 AS RiskUnitGroup, RiskUnitGroupSequenceNumber1 AS RiskUnitGroupSequenceNumber, RiskUnit1 AS RiskUnit, RiskUnitSequenceNumber1 AS RiskUnitSequenceNumber, MajorPerilCode1 AS MajorPerilCode, MajorPerilSequenceNumber1 AS MajorPerilSequenceNumber, SublineCode1 AS SublineCode, PMSTypeExposure1 AS PMSTypeExposure, StatisticalCoverageEffectiveDate1 AS StatisticalCoverageEffectiveDate, StatisticalCoverageExpirationDate1 AS StatisticalCoverageExpirationDate, PremiumLoadSequence1 AS PremiumLoadSequence, PMSFunctionCode1 AS PMSFunctionCode, Onset_Transaction_Code1 AS PremiumTransactionCode, PremiumTransactionEnteredDate1 AS PremiumTransactionEnteredDate, PremiumTransactionEffectiveDate1 AS PremiumTransactionEffectiveDate, ClassCode1 AS ClassCode, PremiumTransactionExpirationDate1 AS PremiumTransactionExpirationDate, PremiumTransactionBookedDate1 AS PremiumTransactionBookedDate, ReasonAmendedCode1 AS ReasonAmendedCode, BureauSpecialUseCode1 AS BureauSpecialUseCode, PMSAnnualStatementLine1 AS PMSAnnualStatementLine, AgencyActualCommissionRate1 AS AgencyActualCommissionRate, Onset_Exposure1 AS Exposure, RatingDateIndicator1 AS RatingDateIndicator, BureauStatisticalUserLine1 AS BureauStatisticalUserLine, StatisticalCodes1 AS StatisticalCodes, Onset_Premium1 AS PremiumTransactionAmount, Onset_Original_Premium1 AS FullTermPremium, PremiumMasterBureauInceptionDate1 AS PremiumMasterBureauInceptionDate, PremiumMasterRiskAddress1 AS PremiumMasterRiskAddress, PremiumMasterRiskCityState1 AS PremiumMasterRiskCityState, PremiumMasterRenewalIndicator1 AS PremiumMasterRenewalIndicator, PremiumType1 AS PremiumMasterPremiumType, Status1 AS Status, PremiumMasterStatus1 AS PremiumMasterStatus, pol_audit_frqncy1 AS pol_audit_frqncy, pol_term1 AS pol_term, o_RatingCoverageAKId AS RatingCoverageAKID, o_RatingCoverageEffectiveDate AS RatingCoverageEffectiveDate, o_RatingCoverageExpirationDate AS RatingCoverageExpirationDate, PremiumMasterCustomerCareCommissionRate1 AS PremiumMasterCustomerCareCommissionRate, Onset_WrittenExposure1 AS PemiumMasterWrittenExposure, DeclaredEventsFlag1 AS DeclaredEventFlag
	FROM EXP_generate_onset
	UNION
	SELECT o_SourceSystemID AS SourceSystemID, AgencyAKID4 AS AgencyAKID, PolicyAKID4 AS PolicyAKID, ContractCustomerAKID4 AS ContractCustomerAKID, RiskLocationAKID4 AS RiskLocationAKID, PolicyCoverageAKID4 AS PolicyCoverageAKID, StatisticalCoverageAKID4 AS StatisticalCoverageAKID, ReinsuranceCoverageAKID4 AS ReinsuranceCoverageAKID, Offset_PremiumTransactionAKID AS PremiumTransactionAKID, BureauStatisticalCodeAKID4 AS BureauStatisticalCodeAKID, pol_sym4 AS pol_sym, pol_num4 AS pol_num, pol_mod4 AS pol_mod, pol_key4 AS pol_key, pol_eff_date4 AS pol_eff_date, pol_exp_date4 AS pol_exp_date, pms_pol_lob_code4 AS pms_pol_lob_code, pol_issue_code4 AS pol_issue_code, LocationUnitNumber4 AS LocationUnitNumber, LocationIndicator4 AS LocationIndicator, InsuranceLine4 AS InsuranceLine, TypeBureauCode4 AS TypeBureauCode, PolicyCoverageEffectiveDate4 AS PolicyCoverageEffectiveDate, PolicyCoverageExpirationDate4 AS PolicyCoverageExpirationDate, SubLocationUnitNumber4 AS SubLocationUnitNumber, RiskUnitGroup4 AS RiskUnitGroup, RiskUnitGroupSequenceNumber4 AS RiskUnitGroupSequenceNumber, RiskUnit4 AS RiskUnit, RiskUnitSequenceNumber4 AS RiskUnitSequenceNumber, MajorPerilCode4 AS MajorPerilCode, MajorPerilSequenceNumber4 AS MajorPerilSequenceNumber, Offset_SublineCode4 AS SublineCode, PMSTypeExposure4 AS PMSTypeExposure, StatisticalCoverageEffectiveDate4 AS StatisticalCoverageEffectiveDate, StatisticalCoverageExpirationDate4 AS StatisticalCoverageExpirationDate, PremiumLoadSequence4 AS PremiumLoadSequence, PMSFunctionCode4 AS PMSFunctionCode, Offset_Transaction_Code4 AS PremiumTransactionCode, PremiumTransactionEnteredDate4 AS PremiumTransactionEnteredDate, PremiumTransactionEffectiveDate4 AS PremiumTransactionEffectiveDate, Offset_ClassCode4 AS ClassCode, Offset_PremiumTransactionExpirationDate4 AS PremiumTransactionExpirationDate, PremiumTransactionBookedDate4 AS PremiumTransactionBookedDate, ReasonAmendedCode4 AS ReasonAmendedCode, BureauSpecialUseCode4 AS BureauSpecialUseCode, PMSAnnualStatementLine4 AS PMSAnnualStatementLine, Offset_AgencyActualCommissionRate4 AS AgencyActualCommissionRate, Offset_Exposure4 AS Exposure, RatingDateIndicator4 AS RatingDateIndicator, Offset_BureauStatisticalUserLine4 AS BureauStatisticalUserLine, Offset_StatisticalCodes4 AS StatisticalCodes, Offset_Premium4 AS PremiumTransactionAmount, Offset_Original_Premium4 AS FullTermPremium, PremiumMasterBureauInceptionDate4 AS PremiumMasterBureauInceptionDate, PremiumMasterRiskAddress4 AS PremiumMasterRiskAddress, PremiumMasterRiskCityState4 AS PremiumMasterRiskCityState, PremiumMasterRenewalIndicator4 AS PremiumMasterRenewalIndicator, PremiumType4 AS PremiumMasterPremiumType, Status4 AS Status, PremiumMasterStatus4 AS PremiumMasterStatus, pol_audit_frqncy4 AS pol_audit_frqncy, pol_term4 AS pol_term, o_RatingCoverageAKId AS RatingCoverageAKID, o_RatingCoverageEffectiveDate AS RatingCoverageEffectiveDate, o_RatingCoverageExpirationDate AS RatingCoverageExpirationDate, PremiumMasterCustomerCareCommissionRate4 AS PremiumMasterCustomerCareCommissionRate, Offset_WrittenExposure4 AS PemiumMasterWrittenExposure, DeclaredEventsFlag4 AS DeclaredEventFlag
	FROM EXP_generate_offset
	UNION
	SELECT o_SourceSystemID AS SourceSystemID, o_AgencyAKID AS AgencyAKID, o_PolicyAKID AS PolicyAKID, o_ContractCustomerAKID AS ContractCustomerAKID, o_RiskLocationAKID AS RiskLocationAKID, o_PolicyCoverageAKID AS PolicyCoverageAKID, o_StatisticalCoverageAKID AS StatisticalCoverageAKID, o_ReinsuranceCoverageAKID AS ReinsuranceCoverageAKID, o_PremiumTransactionAKID AS PremiumTransactionAKID, o_BureauStatisticalCodeAKID AS BureauStatisticalCodeAKID, o_pol_sym AS pol_sym, o_pol_num AS pol_num, o_pol_mod AS pol_mod, o_pol_key AS pol_key, o_pol_eff_date AS pol_eff_date, o_pol_exp_date AS pol_exp_date, o_pms_pol_lob_code AS pms_pol_lob_code, o_pol_issue_code AS pol_issue_code, o_LocationUnitNumber AS LocationUnitNumber, o_LocationIndicator AS LocationIndicator, o_InsuranceLine AS InsuranceLine, o_TypeBureauCode AS TypeBureauCode, o_PolicyCoverageEffectiveDate AS PolicyCoverageEffectiveDate, o_PolicyCoverageExpirationDate AS PolicyCoverageExpirationDate, o_SubLocationUnitNumber AS SubLocationUnitNumber, o_RiskUnitGroup AS RiskUnitGroup, o_RiskUnitGroupSequenceNumber AS RiskUnitGroupSequenceNumber, o_RiskUnit AS RiskUnit, o_RiskUnitSequenceNumber AS RiskUnitSequenceNumber, o_MajorPerilCode AS MajorPerilCode, o_MajorPerilSequenceNumber AS MajorPerilSequenceNumber, o_SublineCode AS SublineCode, o_PMSTypeExposure AS PMSTypeExposure, o_StatisticalCoverageEffectiveDate AS StatisticalCoverageEffectiveDate, o_StatisticalCoverageExpirationDate AS StatisticalCoverageExpirationDate, o_PremiumLoadSequence AS PremiumLoadSequence, o_PMSFunctionCode AS PMSFunctionCode, o_PremiumTransactionCode AS PremiumTransactionCode, o_PremiumTransactionEnteredDate AS PremiumTransactionEnteredDate, o_PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate, o_ClassCode AS ClassCode, o_PremiumTransactionExpirationDate AS PremiumTransactionExpirationDate, o_PremiumTransactionBookedDate AS PremiumTransactionBookedDate, o_ReasonAmendedCode AS ReasonAmendedCode, o_BureauSpecialUseCode AS BureauSpecialUseCode, o_PMSAnnualStatementLine AS PMSAnnualStatementLine, o_AgencyActualCommissionRate AS AgencyActualCommissionRate, o_Exposure AS Exposure, o_RatingDateIndicator AS RatingDateIndicator, o_BureauStatisticalUserLine AS BureauStatisticalUserLine, o_StatisticalCodes AS StatisticalCodes, o_PremiumTransactionAmount AS PremiumTransactionAmount, o_FullTermPremium AS FullTermPremium, o_PremiumMasterBureauInceptionDate AS PremiumMasterBureauInceptionDate, o_PremiumMasterRiskAddress AS PremiumMasterRiskAddress, o_PremiumMasterRiskCityState AS PremiumMasterRiskCityState, o_PremiumMasterRenewalIndicator AS PremiumMasterRenewalIndicator, o_PremiumMasterPremiumType AS PremiumMasterPremiumType, o_Status AS Status, o_PremiumMasterStatus AS PremiumMasterStatus, o_pol_audit_frqncy AS pol_audit_frqncy, o_pol_term AS pol_term, o_RatingCoverageAKID AS RatingCoverageAKID, o_RatingCoverageEffectiveDate AS RatingCoverageEffectiveDate, o_RatingCoverageExpirationDate AS RatingCoverageExpirationDate, CustomerCareCommissionRate AS PremiumMasterCustomerCareCommissionRate, o_WrittenExposure AS PemiumMasterWrittenExposure, DeclaredEventsFlag AS DeclaredEventFlag
	FROM EXP_PremiumMaster_DCT
),
FIL_Invalid_records AS (
	SELECT
	SourceSystemID, 
	AgencyAKID AS AgencyAKID5, 
	PolicyAKID AS PolicyAKID5, 
	ContractCustomerAKID AS ContractCustomerAKID5, 
	RiskLocationAKID AS RiskLocationAKID5, 
	PolicyCoverageAKID AS PolicyCoverageAKID5, 
	StatisticalCoverageAKID AS StatisticalCoverageAKID5, 
	ReinsuranceCoverageAKID AS ReinsuranceCoverageAKID5, 
	PremiumTransactionAKID AS PremiumTransactionAKID5, 
	BureauStatisticalCodeAKID AS BureauStatisticalCodeAKID5, 
	pol_sym AS pol_sym5, 
	pol_num AS pol_num5, 
	pol_mod AS pol_mod5, 
	pol_key AS pol_key5, 
	pol_eff_date AS pol_eff_date5, 
	pol_exp_date AS pol_exp_date5, 
	InsuranceLine AS InsuranceLine5, 
	TypeBureauCode AS TypeBureauCode5, 
	PolicyCoverageEffectiveDate AS PolicyCoverageEffectiveDate5, 
	PolicyCoverageExpirationDate AS PolicyCoverageExpirationDate5, 
	MajorPerilCode AS MajorPerilCode5, 
	SublineCode AS SublineCode5, 
	PremiumLoadSequence AS PremiumLoadSequence5, 
	PremiumTransactionCode AS PremiumTransactionCode5, 
	PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate5, 
	ClassCode AS ClassCode5, 
	PremiumTransactionExpirationDate AS PremiumTransactionExpirationDate5, 
	PremiumTransactionBookedDate AS PremiumTransactionBookedDate5, 
	ReasonAmendedCode AS ReasonAmendedCode5, 
	AgencyActualCommissionRate AS AgencyActualCommissionRate5, 
	Exposure AS Exposure5, 
	BureauStatisticalUserLine AS BureauStatisticalUserLine5, 
	StatisticalCodes AS StatisticalCodes5, 
	PremiumTransactionAmount AS PremiumTransactionAmount5, 
	FullTermPremium AS FullTermPremium5, 
	PremiumMasterBureauInceptionDate AS PremiumMasterBureauInceptionDate5, 
	PremiumMasterRiskAddress AS PremiumMasterRiskAddress5, 
	PremiumMasterRiskCityState AS PremiumMasterRiskCityState5, 
	PremiumMasterRenewalIndicator AS PremiumMasterRenewalIndicator5, 
	PremiumMasterPremiumType AS PremiumMasterPremiumType5, 
	PremiumMasterStatus AS PremiumMasterStatus5, 
	pol_audit_frqncy AS pol_audit_frqncy5, 
	RatingCoverageAKID, 
	RatingCoverageEffectiveDate, 
	RatingCoverageExpirationDate, 
	PremiumTransactionEnteredDate, 
	PremiumMasterCustomerCareCommissionRate, 
	PemiumMasterWrittenExposure, 
	DeclaredEventFlag
	FROM Union_All_Part1_Pipelines
	WHERE TRUE

--Below filter check has been removed as part of Limits, as we need to load the transactions with 0.0 premium
--IIF(PremiumTransactionAmount5 = 0.0 AND FullTermPremium5 =0.0,FALSE,TRUE)
),
mplt_PremiumMasterStatisticalCodeDerivation AS (WITH
	LKP_Pif_43LD_for_AuditCode AS (
		SELECT
		pmd4d_frequency,
		pif_symbol,
		pif_policy_number,
		pif_module,
		pmd4d_insurance_line
		FROM (
			SELECT DISTINCT A.pmd4d_frequency as pmd4d_frequency, A.pif_symbol as pif_symbol, A.pif_policy_number as pif_policy_number, A.pif_module as pif_module, A.pmd4d_insurance_line as pmd4d_insurance_line FROM 
			@{pipeline().parameters.SOURCE_DATABASE_NAME2}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.arch_pif_43ld_stage A
			WHERE A.pmd4d_segment_id = '43'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module,pmd4d_insurance_line ORDER BY pmd4d_frequency DESC) = 1
	),
	LKP_PIF_03_for_CounterSign AS (
		SELECT
		comments_area_Return,
		pif_symbol,
		pif_policy_number,
		pif_module
		FROM (
			SELECT DISTINCT A.comments_reason_suspended + A.comments_area as comments_area_Return, A.pif_symbol as pif_symbol, A.pif_policy_number as pif_policy_number, A.pif_module as pif_module
			FROM @{pipeline().parameters.SOURCE_DATABASE_NAME2}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.arch_pif_03_stage A
			where A.comments_reason_suspended = 'CS'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module ORDER BY comments_area_Return DESC) = 1
	),
	LKP_PIF_02_FOR_FIELDS AS (
		SELECT
		pif_number_installments_a,
		pol_sym,
		pol_num,
		pol_mod,
		pif_symbol,
		pif_policy_number,
		pif_module
		FROM (
			SELECT DISTINCT A.pif_number_installments_a as pif_number_installments_a, A.pif_symbol as pif_symbol, A.pif_policy_number as pif_policy_number, A.pif_module as pif_module 
			FROM @{pipeline().parameters.SOURCE_DATABASE_NAME2}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.arch_pif_02_stage A
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module ORDER BY pif_number_installments_a DESC) = 1
	),
	IN_PremiumMasterRecord AS (
		
	),
	EXP_Pre_StatCode_ops AS (
		SELECT
		SourceSystemID,
		AgencyAKID,
		PolicyAKID,
		ContractCustomerAKID,
		RiskLocationAKID,
		PolicyCoverageAKID,
		StatisticalCoverageAKID,
		ReinsuranceCoverageAKID,
		PremiumTransactionAKID,
		BureauStatisticalCodeAKID,
		PremiumTransactionCode,
		PremiumTransactionBookedDate,
		PolicyCoverageEffectiveDate,
		PolicyCoverageExpirationDate,
		PremiumTransactionEffectiveDate,
		PremiumTransactionExpirationDate,
		PremiumType AS PremiumMasterPremiumType,
		AgencyActualCommissionRate,
		Exposure,
		StatisticalCodes,
		BureauInceptionDate AS PremiumMasterBureauInceptionDate,
		PremiumTransactionAmount,
		FullTermPremium,
		RiskAddress AS PremiumMasterRiskAddress,
		RiskCityState AS PremiumMasterRiskCityState,
		RenewalIndicator AS PremiumMasterRenewalIndicator,
		ClassCode,
		TypeBureauCode,
		-- *INF*: LTRIM(RTRIM(TypeBureauCode))
		-- 
		-- -- Type Bureau Code is transformed in the statistical code expression however it passes through unchanged to the target field
		LTRIM(RTRIM(TypeBureauCode)) AS v_type_bureau,
		MajorPerilCode,
		PremiumLoadSequence5,
		-- *INF*: DECODE(TRUE,IN( v_type_bureau,'AL','LP','AI','LI','RL'), '100',
		-- IN( v_type_bureau,'GS','GM','RG'),'400',
		-- IN( v_type_bureau,'WC','WP'),'500',
		-- IN( v_type_bureau,'GL','GI','GN','RQ'),'600',
		-- IN( v_type_bureau,'FF','FM','BF','BP','FT','FP'),'711',
		-- IN( v_type_bureau,'BD'),'722',
		-- IN( v_type_bureau,'BI','BT','RB','CR','C1','C2'),'800','N/A')
		DECODE(
		    TRUE,
		    v_type_bureau IN ('AL','LP','AI','LI','RL'), '100',
		    v_type_bureau IN ('GS','GM','RG'), '400',
		    v_type_bureau IN ('WC','WP'), '500',
		    v_type_bureau IN ('GL','GI','GN','RQ'), '600',
		    v_type_bureau IN ('FF','FM','BF','BP','FT','FP'), '711',
		    v_type_bureau IN ('BD'), '722',
		    v_type_bureau IN ('BI','BT','RB','CR','C1','C2'), '800',
		    'N/A'
		) AS v_BureauStatisticalLine,
		v_BureauStatisticalLine AS PremiumMasterBureauStatisticalLine,
		pol_key5,
		pol_sym,
		pol_num,
		pol_mod,
		InsuranceLine5,
		-- *INF*: IIF(InsuranceLine5='N/A','  ',LTRIM(RTRIM(InsuranceLine5)))
		IFF(InsuranceLine5 = 'N/A', '  ', LTRIM(RTRIM(InsuranceLine5))) AS ins_line,
		pif02_audit_code,
		-- *INF*: IIF(SourceSystemID='PMS', :LKP.LKP_PIF_43LD_FOR_AUDITCODE(pol_sym, pol_num, pol_mod, ins_line), 'N/A')
		IFF(
		    SourceSystemID = 'PMS',
		    LKP_PIF_43LD_FOR_AUDITCODE_pol_sym_pol_num_pol_mod_ins_line.pmd4d_frequency,
		    'N/A'
		) AS v_pif43ld_audit_code,
		-- *INF*: IIF(SourceSystemID='PMS', IIF(ISNULL(v_pif43ld_audit_code),pif02_audit_code,v_pif43ld_audit_code),'N/A')
		IFF(
		    SourceSystemID = 'PMS',
		    IFF(
		        v_pif43ld_audit_code IS NULL, pif02_audit_code, v_pif43ld_audit_code
		    ),
		    'N/A'
		) AS AuditCode,
		-- *INF*: IIF(SourceSystemID='PMS', :LKP.LKP_PIF_03_FOR_COUNTERSIGN(pol_sym,pol_num,pol_mod), 'N/A')
		IFF(
		    SourceSystemID = 'PMS',
		    LKP_PIF_03_FOR_COUNTERSIGN_pol_sym_pol_num_pol_mod.comments_area_Return,
		    'N/A'
		) AS v_comments_area,
		-- *INF*: IIF(SUBSTR(v_comments_area,1,2)='CS',SUBSTR(v_comments_area,3,12),'')
		IFF(SUBSTR(v_comments_area, 1, 2) = 'CS', SUBSTR(v_comments_area, 3, 12), '') AS comments_cs,
		pol_eff_date5,
		pol_exp_date5,
		-- *INF*: to_integer(TO_CHAR(pol_exp_date5, 'YYYY'))
		CAST(TO_CHAR(pol_exp_date5, 'YYYY') AS INTEGER) AS v_pif_exp_yr,
		-- *INF*: to_integer(TO_CHAR(pol_exp_date5, 'MM'))
		CAST(TO_CHAR(pol_exp_date5, 'MM') AS INTEGER) AS v_pif_exp_mm,
		-- *INF*: to_integer(TO_CHAR(pol_eff_date5, 'YYYY'))
		CAST(TO_CHAR(pol_eff_date5, 'YYYY') AS INTEGER) AS v_pif_eff_yr,
		-- *INF*: to_integer(TO_CHAR(pol_eff_date5, 'MM'))
		CAST(TO_CHAR(pol_eff_date5, 'MM') AS INTEGER) AS v_pif_eff_mm,
		-- *INF*: IIF(SourceSystemID='PMS', :LKP.LKP_PIF_02_FOR_FIELDS(pol_sym,pol_num,pol_mod),'1')
		IFF(
		    SourceSystemID = 'PMS',
		    LKP_PIF_02_FOR_FIELDS_pol_sym_pol_num_pol_mod.pif_number_installments_a,
		    '1'
		) AS v_pif_installment_term,
		-- *INF*: TO_INTEGER(v_pif_installment_term)
		CAST(v_pif_installment_term AS INTEGER) AS v_pif_installment_term_n,
		-- *INF*:  ( v_pif_exp_yr * 12 + v_pif_exp_mm) - (v_pif_eff_yr * 12 + v_pif_eff_mm)
		-- 
		-- --to_integer(DATE_DIFF(pol_exp_date5,pol_eff_date5,'MM'))
		(v_pif_exp_yr * 12 + v_pif_exp_mm) - (v_pif_eff_yr * 12 + v_pif_eff_mm) AS v_calc_pol_term,
		-- *INF*: IIF(Pol_Term='999' or ISNULL(Pol_Term),IIF(v_calc_pol_term<1,1,v_calc_pol_term),to_integer(Pol_Term))
		IFF(
		    Pol_Term = '999' or Pol_Term IS NULL,
		    IFF(
		        v_calc_pol_term < 1, 1, v_calc_pol_term
		    ),
		    CAST(Pol_Term AS INTEGER)
		) AS v_pol_term,
		-- *INF*: (to_integer(DATE_DIFF(PremiumTransactionEffectiveDate,pol_eff_date5,'MM')) / v_pif_installment_term_n) + 1
		(CAST(DATEDIFF(MONTH,PremiumTransactionEffectiveDate,pol_eff_date5) AS INTEGER) / v_pif_installment_term_n) + 1 AS v_calc_installments,
		-- *INF*: v_pif_eff_yr+TO_INTEGER((v_pif_installment_term_n * v_calc_installments)/12,TRUE)+1
		v_pif_eff_yr + CAST((v_pif_installment_term_n * v_calc_installments) / 12 AS INTEGER) + 1 AS v_calc_yr,
		-- *INF*: IIF(v_pif_installment_term_n <> 0,IIF(Pol_Term='0' OR Pol_Term='999',v_pif_exp_yr,
		-- 	IIF(v_calc_yr<v_pif_exp_yr,v_calc_yr,v_pif_exp_yr)),9999)
		IFF(
		    v_pif_installment_term_n <> 0,
		    IFF(
		        Pol_Term = '0'
		    or Pol_Term = '999', v_pif_exp_yr,
		        IFF(
		            v_calc_yr < v_pif_exp_yr, v_calc_yr, v_pif_exp_yr
		        )
		    ),
		    9999
		) AS v_PremiumMasterPolicyExpirationYear,
		-- *INF*: substr(to_char(v_PremiumMasterPolicyExpirationYear),3,2)
		substr(to_char(v_PremiumMasterPolicyExpirationYear), 3, 2) AS PremiumMasterPolicyExpirationYear,
		-- *INF*: to_char(v_pol_term)
		to_char(v_pol_term) AS PremiumMasterPolicyTerm,
		-- *INF*: IIF(IN(v_pif_installment_term,'0','999'),v_pif_installment_term,
		-- to_char(v_calc_installments))
		IFF(
		    v_pif_installment_term IN ('0','999'), v_pif_installment_term, to_char(v_calc_installments)
		) AS PremiumMasterInstallmentNumber,
		RecordType AS PremiumMasterRecordType,
		BureauStatisticalUserLine5,
		-- *INF*: ltrim(rtrim(BureauStatisticalUserLine5))
		-- 
		-- -- Product line is the first two bytes of the stat sarbreakdownline and saruserline fields
		ltrim(rtrim(BureauStatisticalUserLine5)) AS ProductLine,
		SubLineCode,
		ReasonAmendedCode,
		Pol_Term,
		RatingCoverageAKId,
		RatingCoverageEffectiveDate,
		RatingCoverageExpirationDate,
		PremiumTransactionEnteredDate,
		PremiumMasterCustomerCareCommissionRate,
		WrittenExposure
		FROM IN_PremiumMasterRecord
		LEFT JOIN LKP_PIF_43LD_FOR_AUDITCODE LKP_PIF_43LD_FOR_AUDITCODE_pol_sym_pol_num_pol_mod_ins_line
		ON LKP_PIF_43LD_FOR_AUDITCODE_pol_sym_pol_num_pol_mod_ins_line.pif_symbol = pol_sym
		AND LKP_PIF_43LD_FOR_AUDITCODE_pol_sym_pol_num_pol_mod_ins_line.pif_policy_number = pol_num
		AND LKP_PIF_43LD_FOR_AUDITCODE_pol_sym_pol_num_pol_mod_ins_line.pif_module = pol_mod
		AND LKP_PIF_43LD_FOR_AUDITCODE_pol_sym_pol_num_pol_mod_ins_line.pmd4d_insurance_line = ins_line
	
		LEFT JOIN LKP_PIF_03_FOR_COUNTERSIGN LKP_PIF_03_FOR_COUNTERSIGN_pol_sym_pol_num_pol_mod
		ON LKP_PIF_03_FOR_COUNTERSIGN_pol_sym_pol_num_pol_mod.pif_symbol = pol_sym
		AND LKP_PIF_03_FOR_COUNTERSIGN_pol_sym_pol_num_pol_mod.pif_policy_number = pol_num
		AND LKP_PIF_03_FOR_COUNTERSIGN_pol_sym_pol_num_pol_mod.pif_module = pol_mod
	
		LEFT JOIN LKP_PIF_02_FOR_FIELDS LKP_PIF_02_FOR_FIELDS_pol_sym_pol_num_pol_mod
		ON LKP_PIF_02_FOR_FIELDS_pol_sym_pol_num_pol_mod.pif_symbol = pol_sym
		AND LKP_PIF_02_FOR_FIELDS_pol_sym_pol_num_pol_mod.pif_policy_number = pol_num
		AND LKP_PIF_02_FOR_FIELDS_pol_sym_pol_num_pol_mod.pif_module = pol_mod
	
	),
	EXP_Transform_Statistical_Codes AS (
		SELECT
		StatisticalCodes AS statistical_code,
		MajorPerilCode AS major_peril,
		TypeBureauCode AS Type_Bureau,
		ClassCode AS sar_class_code,
		-- *INF*: statistical_code
		-- 
		-- --DECODE(TRUE, Type_Bureau = 'BE', ' '  || statistical_code,
		-- --Type_Bureau = 'BF', ' '  || statistical_code,
		-- --Type_Bureau = 'RP' AND major_peril = '145', ' '  || statistical_code,
		-- --Type_Bureau = 'RL' AND major_peril = '114', '  '  || statistical_code,
		-- --Type_Bureau = 'RL' AND major_peril = '119', '     '  || statistical_code,
		-- --statistical_code)
		-- 
		-- ---- Had to introduce space at the begining of the field because of LTRIM(RTRIM)) to statistical codes in Temp_Policy_transaction Table.
		statistical_code AS v_statistical_code,
		'D' AS v_stat_plan_id,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,1,1))=0,' ',SUBSTR(v_statistical_code,1,1))
		-- 
		-- ----8/18/2011 Uma Bollu - Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
		IFF(LENGTH(SUBSTR(v_statistical_code, 1, 1)) = 0, ' ', SUBSTR(v_statistical_code, 1, 1)) AS v_pos_1,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,2,1))=0,' ',SUBSTR(v_statistical_code,2,1))
		-- 
		-- ----Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
		IFF(LENGTH(SUBSTR(v_statistical_code, 2, 1)) = 0, ' ', SUBSTR(v_statistical_code, 2, 1)) AS v_pos_2,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,3,1))=0,' ',SUBSTR(v_statistical_code,3,1))
		-- 
		-- ----Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
		IFF(LENGTH(SUBSTR(v_statistical_code, 3, 1)) = 0, ' ', SUBSTR(v_statistical_code, 3, 1)) AS v_pos_3,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,4,1))=0,' ',SUBSTR(v_statistical_code,4,1))
		-- 
		-- ----Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
		IFF(LENGTH(SUBSTR(v_statistical_code, 4, 1)) = 0, ' ', SUBSTR(v_statistical_code, 4, 1)) AS v_pos_4,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,5,1))=0,' ',SUBSTR(v_statistical_code,5,1))
		-- 
		-- ----8/18/2011 Uma Bollu - Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
		IFF(LENGTH(SUBSTR(v_statistical_code, 5, 1)) = 0, ' ', SUBSTR(v_statistical_code, 5, 1)) AS v_pos_5,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,6,1))=0,' ',SUBSTR(v_statistical_code,6,1))
		-- 
		-- 
		-- ----8/18/2011 Uma Bollu - Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
		IFF(LENGTH(SUBSTR(v_statistical_code, 6, 1)) = 0, ' ', SUBSTR(v_statistical_code, 6, 1)) AS v_pos_6,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,7,1))=0,' ',SUBSTR(v_statistical_code,7,1))
		IFF(LENGTH(SUBSTR(v_statistical_code, 7, 1)) = 0, ' ', SUBSTR(v_statistical_code, 7, 1)) AS v_pos_7,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,8,1))=0,' ',SUBSTR(v_statistical_code,8,1))
		IFF(LENGTH(SUBSTR(v_statistical_code, 8, 1)) = 0, ' ', SUBSTR(v_statistical_code, 8, 1)) AS v_pos_8,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,9,1))=0,' ',SUBSTR(v_statistical_code,9,1))
		IFF(LENGTH(SUBSTR(v_statistical_code, 9, 1)) = 0, ' ', SUBSTR(v_statistical_code, 9, 1)) AS v_pos_9,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,10,1))=0,' ',SUBSTR(v_statistical_code,10,1))
		IFF(LENGTH(SUBSTR(v_statistical_code, 10, 1)) = 0, ' ', SUBSTR(v_statistical_code, 10, 1)) AS v_pos_10,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,11,1))=0,' ',SUBSTR(v_statistical_code,11,1))
		IFF(LENGTH(SUBSTR(v_statistical_code, 11, 1)) = 0, ' ', SUBSTR(v_statistical_code, 11, 1)) AS v_pos_11,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,12,1))=0,' ',SUBSTR(v_statistical_code,12,1))
		IFF(LENGTH(SUBSTR(v_statistical_code, 12, 1)) = 0, ' ', SUBSTR(v_statistical_code, 12, 1)) AS v_pos_12,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,13,1))=0,' ',SUBSTR(v_statistical_code,13,1))
		IFF(LENGTH(SUBSTR(v_statistical_code, 13, 1)) = 0, ' ', SUBSTR(v_statistical_code, 13, 1)) AS v_pos_13,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,14,1))=0,' ',SUBSTR(v_statistical_code,14,1))
		IFF(LENGTH(SUBSTR(v_statistical_code, 14, 1)) = 0, ' ', SUBSTR(v_statistical_code, 14, 1)) AS v_pos_14,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,15,1))=0,' ',SUBSTR(v_statistical_code,15,1))
		IFF(LENGTH(SUBSTR(v_statistical_code, 15, 1)) = 0, ' ', SUBSTR(v_statistical_code, 15, 1)) AS v_pos_15,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,16,1))=0,' ',SUBSTR(v_statistical_code,16,1))
		IFF(LENGTH(SUBSTR(v_statistical_code, 16, 1)) = 0, ' ', SUBSTR(v_statistical_code, 16, 1)) AS v_pos_16,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,17,1))=0,' ',SUBSTR(v_statistical_code,17,1))
		IFF(LENGTH(SUBSTR(v_statistical_code, 17, 1)) = 0, ' ', SUBSTR(v_statistical_code, 17, 1)) AS v_pos_17,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,18,1))=0,' ',SUBSTR(v_statistical_code,18,1))
		IFF(LENGTH(SUBSTR(v_statistical_code, 18, 1)) = 0, ' ', SUBSTR(v_statistical_code, 18, 1)) AS v_pos_18,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,19,1))=0,' ',SUBSTR(v_statistical_code,19,1))
		IFF(LENGTH(SUBSTR(v_statistical_code, 19, 1)) = 0, ' ', SUBSTR(v_statistical_code, 19, 1)) AS v_pos_19,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,20,1))=0,' ',SUBSTR(v_statistical_code,20,1))
		IFF(LENGTH(SUBSTR(v_statistical_code, 20, 1)) = 0, ' ', SUBSTR(v_statistical_code, 20, 1)) AS v_pos_20,
		-- *INF*: DECODE(TRUE,Type_Bureau='RP','0',
		-- LENGTH(SUBSTR(v_statistical_code,21,1))=0,' ',SUBSTR(v_statistical_code,21,1))
		-- --IIF(LENGTH(SUBSTR(v_statistical_code,21,1))=0,' ',SUBSTR(v_statistical_code,21,1))
		-- 
		-- 
		-- --- Statistical Code field is initialised at the begining of the WMM01A0 module to all spaces but since it is a sign field for Type Bureau of RP, these are defaulted to '0'
		DECODE(
		    TRUE,
		    Type_Bureau = 'RP', '0',
		    LENGTH(SUBSTR(v_statistical_code, 21, 1)) = 0, ' ',
		    SUBSTR(v_statistical_code, 21, 1)
		) AS v_pos_21,
		-- *INF*: DECODE(TRUE,Type_Bureau='RP','0',
		-- LENGTH(SUBSTR(v_statistical_code,22,1))=0,' ',SUBSTR(v_statistical_code,22,1))
		-- 
		-- --IIF(LENGTH(SUBSTR(v_statistical_code,22,1))=0,' ',SUBSTR(v_statistical_code,22,1))
		-- 
		-- --- Statistical Code field is initialised at the begining of the WMM01A0 module to all spaces but since it is a sign field for Type Bureau of RP, these are defaulted to '0'
		DECODE(
		    TRUE,
		    Type_Bureau = 'RP', '0',
		    LENGTH(SUBSTR(v_statistical_code, 22, 1)) = 0, ' ',
		    SUBSTR(v_statistical_code, 22, 1)
		) AS v_pos_22,
		-- *INF*: DECODE(TRUE,Type_Bureau='RP','0',
		-- LENGTH(SUBSTR(v_statistical_code,23,1))=0,' ',SUBSTR(v_statistical_code,23,1))
		-- 
		-- --IIF(LENGTH(SUBSTR(v_statistical_code,23,1))=0,' ',SUBSTR(v_statistical_code,23,1))
		-- 
		-- --- Statistical Code field is initialised at the begining of the WMM01A0 module to all spaces but since it is a sign field for Type Bureau of RP, these are defaulted to '0'
		DECODE(
		    TRUE,
		    Type_Bureau = 'RP', '0',
		    LENGTH(SUBSTR(v_statistical_code, 23, 1)) = 0, ' ',
		    SUBSTR(v_statistical_code, 23, 1)
		) AS v_pos_23,
		-- *INF*: DECODE(TRUE,Type_Bureau='RP','0',
		-- LENGTH(SUBSTR(v_statistical_code,24,1))=0,' ',SUBSTR(v_statistical_code,24,1))
		-- 
		-- --IIF(LENGTH(SUBSTR(v_statistical_code,24,1))=0,' ',SUBSTR(v_statistical_code,24,1))
		-- 
		-- --- Statistical Code field is initialised at the begining of the WMM01A0 module to all spaces but since it is a sign field for Type Bureau of RP, these are defaulted to '0'
		DECODE(
		    TRUE,
		    Type_Bureau = 'RP', '0',
		    LENGTH(SUBSTR(v_statistical_code, 24, 1)) = 0, ' ',
		    SUBSTR(v_statistical_code, 24, 1)
		) AS v_pos_24,
		-- *INF*: DECODE(TRUE,Type_Bureau='RP','{',
		-- LENGTH(SUBSTR(v_statistical_code,25,1))=0,' ',SUBSTR(v_statistical_code,25,1))
		-- 
		-- --- IN COBOL "{" represents a  +ve sign and "}" is -ve sign, since this is base rate for Type_Bureau RP is a sign field so COBOL creates "{". Replicating the COBOL logic.
		-- 
		-- --- Statistical Code field is initialised at the begining of the WMM01A0 module to all spaces but since it is a sign field for Type Bureau of RP, these are defaulted to '{'
		-- 
		-- --IIF(LENGTH(SUBSTR(v_statistical_code,25,1))=0,' ',SUBSTR(v_statistical_code,25,1))
		DECODE(
		    TRUE,
		    Type_Bureau = 'RP', '{',
		    LENGTH(SUBSTR(v_statistical_code, 25, 1)) = 0, ' ',
		    SUBSTR(v_statistical_code, 25, 1)
		) AS v_pos_25,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,26,1))=0,' ',SUBSTR(v_statistical_code,26,1))
		IFF(LENGTH(SUBSTR(v_statistical_code, 26, 1)) = 0, ' ', SUBSTR(v_statistical_code, 26, 1)) AS v_pos_26,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,27,1))=0,' ',SUBSTR(v_statistical_code,27,1))
		IFF(LENGTH(SUBSTR(v_statistical_code, 27, 1)) = 0, ' ', SUBSTR(v_statistical_code, 27, 1)) AS v_pos_27,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,28,1))=0,' ',SUBSTR(v_statistical_code,28,1))
		IFF(LENGTH(SUBSTR(v_statistical_code, 28, 1)) = 0, ' ', SUBSTR(v_statistical_code, 28, 1)) AS v_pos_28,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,29,1))=0,' ',SUBSTR(v_statistical_code,29,1))
		IFF(LENGTH(SUBSTR(v_statistical_code, 29, 1)) = 0, ' ', SUBSTR(v_statistical_code, 29, 1)) AS v_pos_29,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,30,1))=0,' ',SUBSTR(v_statistical_code,30,1))
		IFF(LENGTH(SUBSTR(v_statistical_code, 30, 1)) = 0, ' ', SUBSTR(v_statistical_code, 30, 1)) AS v_pos_30,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,31,1))=0,' ',SUBSTR(v_statistical_code,31,1))
		-- 
		-- ----8/18/2011 Uma Bollu - Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
		IFF(LENGTH(SUBSTR(v_statistical_code, 31, 1)) = 0, ' ', SUBSTR(v_statistical_code, 31, 1)) AS v_pos_31,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,32,1))=0,' ',SUBSTR(v_statistical_code,32,1))
		-- 
		IFF(LENGTH(SUBSTR(v_statistical_code, 32, 1)) = 0, ' ', SUBSTR(v_statistical_code, 32, 1)) AS v_pos_32,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,33,1))=0,' ',SUBSTR(v_statistical_code,33,1))
		-- 
		-- 
		IFF(LENGTH(SUBSTR(v_statistical_code, 33, 1)) = 0, ' ', SUBSTR(v_statistical_code, 33, 1)) AS v_pos_33,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,34,1))=0,' ',SUBSTR(v_statistical_code,34,1))
		-- 
		-- 
		IFF(LENGTH(SUBSTR(v_statistical_code, 34, 1)) = 0, ' ', SUBSTR(v_statistical_code, 34, 1)) AS v_pos_34,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,35,1))=0,' ',SUBSTR(v_statistical_code,35,1))
		-- 
		-- 
		IFF(LENGTH(SUBSTR(v_statistical_code, 35, 1)) = 0, ' ', SUBSTR(v_statistical_code, 35, 1)) AS v_pos_35,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,36,1))=0,' ',SUBSTR(v_statistical_code,36,1))
		-- 
		-- 
		IFF(LENGTH(SUBSTR(v_statistical_code, 36, 1)) = 0, ' ', SUBSTR(v_statistical_code, 36, 1)) AS v_pos_36,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,37,1))=0,' ',SUBSTR(v_statistical_code,37,1))
		-- 
		-- 
		IFF(LENGTH(SUBSTR(v_statistical_code, 37, 1)) = 0, ' ', SUBSTR(v_statistical_code, 37, 1)) AS v_pos_37,
		-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,38,1))=0,' ',SUBSTR(v_statistical_code,38,1))
		-- 
		-- 
		IFF(LENGTH(SUBSTR(v_statistical_code, 38, 1)) = 0, ' ', SUBSTR(v_statistical_code, 38, 1)) AS v_pos_38,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38) AS Generic,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 )
		-- 
		-- ---- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16) AS v_Stat_Code_AC,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 )
		-- 
		-- ---- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16) AS v_Stat_Codes_AI,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_26 || '       ' || v_pos_25 || v_pos_23  || v_pos_24  || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22)
		-- 
		-- 
		-- ---- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_26 || '       ' || v_pos_25 || v_pos_23 || v_pos_24 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22) AS v_Stat_Codes_AL,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_8 || v_pos_9 || v_pos_10  || v_pos_11|| v_pos_20 || v_pos_21  || 
		-- '             ' ||  v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19  )
		-- 
		--  -----It has a Filler of 13 spaces
		-- --- I have checked this code this is fine
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_20 || v_pos_21 || '             ' || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19) AS v_Stat_Codes_AN,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 ||
		-- '      ' || v_pos_14 || v_pos_23  || v_pos_24  || '  '  ||  v_pos_26  || v_pos_27  || v_pos_28  || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22)
		-- 
		-- --- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || '      ' || v_pos_14 || v_pos_23 || v_pos_24 || '  ' || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22) AS v_Stat_Codes_AP,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_11 || v_pos_10 || 
		--   v_pos_12 || v_pos_13 )
		-- 
		-- --- Verified the logic
		-- 
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_11 || v_pos_10 || v_pos_12 || v_pos_13) AS v_Stat_Codes_A2,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_11 || v_pos_12 )
		-- 
		-- --- Verified logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_11 || v_pos_12) AS v_Stat_Codes_A3,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 ||
		-- '           '  ||  v_pos_22 || v_pos_29 || '  ' || v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28)
		-- 
		-- --- Verified logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || '           ' || v_pos_22 || v_pos_29 || '  ' || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28) AS v_Stat_Codes_BB,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17  || v_pos_20  || v_pos_27  || v_pos_28  || v_pos_29 || '    ' ||v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26 )
		-- 
		-- 
		-- -- Verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_20 || v_pos_27 || v_pos_28 || v_pos_29 || '    ' || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26) AS v_Stat_Codes_BC,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_5  || v_pos_6 || v_pos_7)
		-- 
		-- --- Verified logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_5 || v_pos_6 || v_pos_7) AS v_Stat_Codes_BD,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 ||  v_pos_6 || v_pos_7 || '                    ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13)
		-- 
		-- 
		--  ---  Verified Logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_6 || v_pos_7 || '                    ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13) AS v_Stat_Codes_BE,
		-- *INF*: ('  '  || v_pos_4  || v_pos_5 || ' ' || v_pos_14 || '  ' || v_pos_15 || v_pos_16 || '   ' ||  v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || '     ' || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22 || '   ')
		-- 
		-- 
		-- --8/22/2011 - Added 2 spaces in the beginning. In COBOL, statitistical code field is initialised to spaces at the start of reformatting. If there is no code to move certain fields then the spaces stay as it is except other fileds are layed out over spaces.
		-- --- Verified the logic
		-- 
		('  ' || v_pos_4 || v_pos_5 || ' ' || v_pos_14 || '  ' || v_pos_15 || v_pos_16 || '   ' || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || '     ' || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || '   ') AS v_Stat_Codes_BF,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_4  || v_pos_5)
		-- 
		-- --- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_4 || v_pos_5) AS v_Stat_Codes_BP,
		-- *INF*: (v_pos_1 || v_pos_2 )
		-- 
		-- --- Verified the logic
		(v_pos_1 || v_pos_2) AS v_Stat_Codes_BI,
		-- *INF*: v_pos_1
		-- 
		-- -- verified the logic
		v_pos_1 AS v_Stat_Codes_BL,
		-- *INF*: (SUBSTR(sar_class_code,1,3) || '  ' || v_pos_18  ||  v_pos_19 || v_pos_1 ||  ' ' ||  v_pos_2 || v_pos_3 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 
		-- || '    ' ||  v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34 || v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28 || '   ' )
		-- 
		-- --- Verfied the logic
		(SUBSTR(sar_class_code, 1, 3) || '  ' || v_pos_18 || v_pos_19 || v_pos_1 || ' ' || v_pos_2 || v_pos_3 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || '    ' || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || '   ') AS v_Stat_Codes_BM,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || '      '  ||  v_pos_8 || v_pos_9 || '           ' || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19)
		-- 
		--  ---- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || '      ' || v_pos_8 || v_pos_9 || '           ' || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19) AS v_Stat_Codes_BT,
		-- *INF*: (v_pos_1 || v_pos_2 || '      '  || v_pos_9 || v_pos_10 || v_pos_17 || v_pos_18 ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31)
		-- 
		-- ---- verified the logic
		(v_pos_1 || v_pos_2 || '      ' || v_pos_9 || v_pos_10 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31) AS v_Stat_Codes_B2,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17)
		-- 
		-- ----- verified the logic
		-- 
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17) AS v_Stat_Codes_CC,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || 
		--  v_pos_17 || v_pos_18  || ' ' ||  v_pos_20 || '              ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 )
		-- 
		-- ---- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_17 || v_pos_18 || ' ' || v_pos_20 || '              ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16) AS v_Stat_Codes_CF,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
		-- 
		-- ---- Generic 
		-- -- No Change from Input copybook to Output
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38) AS v_Stat_Code_CR,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_6 || v_pos_7 || ' '  || v_pos_9 || '  ' || v_pos_12 || ' ' || v_pos_14 || v_pos_15 )
		-- 
		-- ---- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_6 || v_pos_7 || ' ' || v_pos_9 || '  ' || v_pos_12 || ' ' || v_pos_14 || v_pos_15) AS v_Stat_Codes_CI,
		-- *INF*: (v_pos_1 || v_pos_4  || v_pos_6 || v_pos_7 )
		-- 
		-- ---- verified the logic
		(v_pos_1 || v_pos_4 || v_pos_6 || v_pos_7) AS v_Stat_Codes_CL,
		-- *INF*: ('  ' || v_pos_1 || v_pos_2 || v_pos_5  || v_pos_6 || v_pos_7)
		-- 
		-- ---- verified the logic
		('  ' || v_pos_1 || v_pos_2 || v_pos_5 || v_pos_6 || v_pos_7) AS v_Stat_Codes_CP,
		-- *INF*: (v_pos_3 || v_pos_4  || v_pos_5 )
		-- 
		-- ---- verified the logic
		(v_pos_3 || v_pos_4 || v_pos_5) AS v_Stat_Codes_CN,
		-- *INF*: v_pos_1
		-- 
		-- -----
		v_pos_1 AS v_Stat_Codes_EI,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_6 || v_pos_7 || '                   ' ||v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16)
		-- 
		-- ---- verified the logic
		-- --- 19 spaces
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_7 || '                   ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16) AS v_Stat_Codes_EQ,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 )
		-- 
		-- ---- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4) AS v_Stat_Codes_FC,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 
		-- || '                  ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 )
		-- 
		-- ---- verified the logic
		-- ---- 18 Spaces
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || '                  ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16) AS v_Stat_Codes_FF,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5)
		-- 
		-- ---- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5) AS v_Stat_Codes_FM,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || '                   ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16)
		-- 
		-- ---- verified the logic
		-- --- 19 spaces
		-- 
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || '                   ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16) AS v_Stat_Codes_FO,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3)
		-- 
		-- ---- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3) AS v_Stat_Codes_FP,
		-- *INF*: (v_pos_1 || v_pos_2 || ' ' || v_pos_3 || '  ' || v_pos_6 || v_pos_7 || '   ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 ||
		-- '       ' || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22 || '   ')
		-- 
		-- ---- verified the logic
		(v_pos_1 || v_pos_2 || ' ' || v_pos_3 || '  ' || v_pos_6 || v_pos_7 || '   ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || '       ' || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || '   ') AS v_Stat_Codes_FT,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_12 || v_pos_13 || '                '  || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9)
		-- 
		-- ---- verified the logic
		-- -- 17 Spaces
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_12 || v_pos_13 || '                ' || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9) AS v_Stat_Codes_GI,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_6 || v_pos_7 || v_pos_4  || v_pos_5  || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || '      ' || v_pos_13 || v_pos_29  || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19 ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28)
		-- 
		-- ---- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_6 || v_pos_7 || v_pos_4 || v_pos_5 || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || '      ' || v_pos_13 || v_pos_29 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28) AS v_Stat_Codes_GL,
		-- *INF*: (v_pos_1 || '           '  ||   v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7)
		-- 
		-- ---- verified the logic
		-- 
		(v_pos_1 || '           ' || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7) AS v_Stat_Codes_GP,
		-- *INF*: (v_pos_1 || '                       ' || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_11 || v_pos_12 || v_pos_13)
		-- 
		-- ---- verified the logic
		-- --- 23 spaces
		-- 
		-- 
		-- 
		(v_pos_1 || '                       ' || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_11 || v_pos_12 || v_pos_13) AS v_Stat_Codes_GS,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_10 || ' ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_18  ||  v_pos_19  
		-- || ' ' || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || '                ')
		-- 
		-- 
		-- ---- verified the logic
		-- --- 16 Spaces at the end
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_10 || ' ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_18 || v_pos_19 || ' ' || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || '                ') AS v_Stat_Codes_HO,
		-- *INF*: ('        ' || v_pos_11 || v_pos_12 || '               '  || v_pos_4  || v_pos_5  || v_pos_6  || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_14 || v_pos_15 || v_pos_17)
		-- 
		-- ---- verified the logic
		('        ' || v_pos_11 || v_pos_12 || '               ' || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_14 || v_pos_15 || v_pos_17) AS v_Stat_Codes_IM,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  || v_pos_24  || v_pos_25  || v_pos_26 || v_pos_28  || v_pos_29  || v_pos_30 || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35)
		-- 
		-- ---- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35) AS v_Stat_Codes_JR,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  )
		-- 
		-- ---- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5) AS v_Stat_Codes_ME,
		-- *INF*: (v_pos_1 || ' '  || v_pos_3 || v_pos_4  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_10 || ' ' ||  v_pos_11 || v_pos_12 || v_pos_13 || '  ' || v_pos_18  || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || ' ' || '                ' ) 
		-- 
		-- --- need logic for stat-plan -id
		-- ---- 16 Spaces at the end
		(v_pos_1 || ' ' || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_10 || ' ' || v_pos_11 || v_pos_12 || v_pos_13 || '  ' || v_pos_18 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || ' ' || '                ') AS v_Stat_Codes_MH,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || '                  '  || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7)
		-- 
		--  --- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3 || '                  ' || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7) AS v_Stat_Codes_MI,
		-- *INF*: (v_pos_6 || v_pos_7 || v_pos_3 || v_pos_4  || v_pos_2 || '      ' || v_pos_1 || '        ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || ' ' ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24 )
		-- 
		--  --- verified the logic
		(v_pos_6 || v_pos_7 || v_pos_3 || v_pos_4 || v_pos_2 || '      ' || v_pos_1 || '        ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || ' ' || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24) AS v_Stat_Codes_ML,
		-- *INF*: -- No Stats code in the Output Copybook just the policy_type logic
		'' AS v_Stat_Codes_MP,
		-- *INF*: (SUBSTR(sar_class_code,1,3) || v_pos_17 || v_pos_18 ||  v_pos_19  || v_pos_1 || v_pos_2 || v_pos_3 || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || '   ' || '       ' || '      ' )
		-- 
		-- --- Need to look at complete logic
		-- 
		(SUBSTR(sar_class_code, 1, 3) || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_1 || v_pos_2 || v_pos_3 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || '   ' || '       ' || '      ') AS v_Stat_Codes_M2,
		-- *INF*: ( '                 ' || v_stat_plan_id)
		-- 
		-- ----verified the logic
		('                 ' || v_stat_plan_id) AS v_Stat_Codes_NE,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  ||  v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_19)
		-- 
		-- --- Verified the Logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_19) AS v_Stat_Codes_PC,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || ' ' || v_pos_19  || v_pos_20  ||  v_pos_21)
		-- 
		-- --- verified the logic
		--  
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || ' ' || v_pos_19 || v_pos_20 || v_pos_21) AS v_Stat_Codes_PH,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
		-- 
		-- --- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38) AS v_Stat_Code_PF,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
		-- 
		-- --- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38) AS v_Stat_Code_PI,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
		-- 
		-- --- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38) AS v_Stat_Code_PL,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 ||  v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18)
		-- 
		-- --- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18) AS v_Stat_Codes_PM,
		-- *INF*: (v_pos_1 || v_pos_2)
		-- 
		-- --- verified the logic
		-- 
		(v_pos_1 || v_pos_2) AS v_Stat_Codes_RB,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3)
		-- 
		-- --- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3) AS v_Stat_Codes_RG,
		-- *INF*: (v_pos_1 || v_pos_2)
		-- 
		-- --- verified the logic
		(v_pos_1 || v_pos_2) AS v_Stat_Codes_RI,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24)
		-- 
		-- --- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24) AS v_Stat_Codes_RL,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_8 || v_pos_9 || v_pos_10)
		-- 
		-- --- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_8 || v_pos_9 || v_pos_10) AS v_Stat_Codes_RM,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || ' ' || 
		-- v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21 || v_pos_22 ||  v_pos_23  || v_pos_24)
		-- 
		-- --- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || ' ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24) AS v_Stat_Codes_RN,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29 || v_pos_30 || v_pos_31|| v_pos_33 || v_pos_34  ||  v_pos_35  || v_pos_32)
		-- 
		-- ----
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_32) AS v_Stat_Codes_RP,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5 )
		-- 
		-- --- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5) AS v_Stat_Codes_RQ,
		-- *INF*: (v_pos_1 || ' ' || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 )
		-- 
		-- --- verified the logic
		(v_pos_1 || ' ' || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8) AS v_Stat_Codes_SM,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_6 || v_pos_8 || v_pos_11 || v_pos_9)
		-- 
		-- --- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_8 || v_pos_11 || v_pos_9) AS v_Stat_Codes_TH,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 
		-- || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19
		-- ||  v_pos_22  ||  v_pos_23  || v_pos_24 || '       ' || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36)
		-- 
		-- --- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_22 || v_pos_23 || v_pos_24 || '       ' || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36) AS v_Stat_Codes_VL,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19 
		--  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30 || ' ' || v_pos_32  ||  v_pos_33
		-- || v_pos_34  ||  v_pos_35  || v_pos_36 )
		-- 
		-- --- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || ' ' || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36) AS v_Stat_Codes_VP,
		-- *INF*: ('   ' || v_pos_4  || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12  || ' ' || v_pos_14 || v_pos_15 || '              ' 
		-- || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34 || v_pos_35)
		-- 
		-- --- verified the logic
		('   ' || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || ' ' || v_pos_14 || v_pos_15 || '              ' || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35) AS v_Stat_Codes_VN,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  
		-- || ' ' || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || '    ' || v_pos_36 || v_pos_37  || v_pos_38)
		-- 
		-- ---- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || ' ' || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || '    ' || v_pos_36 || v_pos_37 || v_pos_38) AS v_Stat_Codes_VC,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31)
		-- 
		--  --- verified the logic
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31) AS v_Stat_Codes_WC,
		-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
		(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38) AS v_Stat_Code_WP,
		-- *INF*: ('   ' || v_pos_1 || v_pos_2 || '            ' || v_stat_plan_id)
		-- 
		-- --8/19/2011 Added v_stat_plan_id
		-- --- need to bring stat plan_id
		--  --- verified the logic but need stat plan id
		-- 
		('   ' || v_pos_1 || v_pos_2 || '            ' || v_stat_plan_id) AS v_Stat_Codes_WL,
		-- *INF*: DECODE(Type_Bureau, 'AC', v_Stat_Code_AC, 'AI', v_Stat_Codes_AI, 'AL', v_Stat_Codes_AL, 'AN', v_Stat_Codes_AN, 'AP', v_Stat_Codes_AP, 'A2', v_Stat_Codes_A2, 'A3', v_Stat_Codes_A3, 'BB', v_Stat_Codes_BB, 'BC', v_Stat_Codes_BC, 'BD', v_Stat_Codes_BD, 'BE', v_Stat_Codes_BE, 'BF', v_Stat_Codes_BF, 'BP', v_Stat_Codes_BP, 'BI', v_Stat_Codes_BI, 'BL', v_Stat_Codes_BL, 'BM', v_Stat_Codes_BM, 'BT', v_Stat_Codes_BT, 'B2', v_Stat_Codes_B2, 'CC', v_Stat_Codes_CC, 'CF', v_Stat_Codes_CF, 'CI', v_Stat_Codes_CI, 'CL', v_Stat_Codes_CL, 'CN', v_Stat_Codes_CN, 'CP', v_Stat_Codes_CP, 'EI', v_Stat_Codes_EI, 'EQ', v_Stat_Codes_EQ, 'FC', v_Stat_Codes_FC, 'FF', v_Stat_Codes_FF, 'FM', v_Stat_Codes_FM, 'FO', v_Stat_Codes_FO, 'FP', v_Stat_Codes_FP, 'FT', v_Stat_Codes_FT, 'GI', v_Stat_Codes_GI, 'GL', v_Stat_Codes_GL, 'GP', v_Stat_Codes_GP, 'GS', v_Stat_Codes_GS, 'HO', v_Stat_Codes_HO, 'IM', v_Stat_Codes_IM, 'JR', v_Stat_Codes_JR, 'ME', v_Stat_Codes_ME, 'MH', v_Stat_Codes_MH, 'MI', v_Stat_Codes_MI, 'ML',
		-- v_Stat_Codes_ML, 'MP', v_Stat_Codes_MP, 'M2', v_Stat_Codes_M2, 'NE', v_Stat_Codes_NE, 'PC', v_Stat_Codes_PC, 'PH', v_Stat_Codes_PH, 'PM', v_Stat_Codes_PM, 'RB', v_Stat_Codes_RB, 'RG', v_Stat_Codes_RG, 'RI', v_Stat_Codes_RI, 'RL', v_Stat_Codes_RL, 'RM', v_Stat_Codes_RM, 'RN', v_Stat_Codes_RN, 'RP', v_Stat_Codes_RP, 'RQ', v_Stat_Codes_RQ, 'SM', v_Stat_Codes_SM, 'TH', v_Stat_Codes_TH, 'VL', v_Stat_Codes_VL, 'VP', v_Stat_Codes_VP, 'VN', v_Stat_Codes_VN, 'VC', v_Stat_Codes_VC, 'WC', v_Stat_Codes_WC, 'WL', v_Stat_Codes_WL,
		-- 'CR', v_Stat_Code_CR, 'PF', v_Stat_Code_PF,'PI', v_Stat_Code_PI, 'PL', v_Stat_Code_PL,
		-- 'WP', v_Stat_Code_WP,v_statistical_code) 
		DECODE(
		    Type_Bureau,
		    'AC', v_Stat_Code_AC,
		    'AI', v_Stat_Codes_AI,
		    'AL', v_Stat_Codes_AL,
		    'AN', v_Stat_Codes_AN,
		    'AP', v_Stat_Codes_AP,
		    'A2', v_Stat_Codes_A2,
		    'A3', v_Stat_Codes_A3,
		    'BB', v_Stat_Codes_BB,
		    'BC', v_Stat_Codes_BC,
		    'BD', v_Stat_Codes_BD,
		    'BE', v_Stat_Codes_BE,
		    'BF', v_Stat_Codes_BF,
		    'BP', v_Stat_Codes_BP,
		    'BI', v_Stat_Codes_BI,
		    'BL', v_Stat_Codes_BL,
		    'BM', v_Stat_Codes_BM,
		    'BT', v_Stat_Codes_BT,
		    'B2', v_Stat_Codes_B2,
		    'CC', v_Stat_Codes_CC,
		    'CF', v_Stat_Codes_CF,
		    'CI', v_Stat_Codes_CI,
		    'CL', v_Stat_Codes_CL,
		    'CN', v_Stat_Codes_CN,
		    'CP', v_Stat_Codes_CP,
		    'EI', v_Stat_Codes_EI,
		    'EQ', v_Stat_Codes_EQ,
		    'FC', v_Stat_Codes_FC,
		    'FF', v_Stat_Codes_FF,
		    'FM', v_Stat_Codes_FM,
		    'FO', v_Stat_Codes_FO,
		    'FP', v_Stat_Codes_FP,
		    'FT', v_Stat_Codes_FT,
		    'GI', v_Stat_Codes_GI,
		    'GL', v_Stat_Codes_GL,
		    'GP', v_Stat_Codes_GP,
		    'GS', v_Stat_Codes_GS,
		    'HO', v_Stat_Codes_HO,
		    'IM', v_Stat_Codes_IM,
		    'JR', v_Stat_Codes_JR,
		    'ME', v_Stat_Codes_ME,
		    'MH', v_Stat_Codes_MH,
		    'MI', v_Stat_Codes_MI,
		    'ML', v_Stat_Codes_ML,
		    'MP', v_Stat_Codes_MP,
		    'M2', v_Stat_Codes_M2,
		    'NE', v_Stat_Codes_NE,
		    'PC', v_Stat_Codes_PC,
		    'PH', v_Stat_Codes_PH,
		    'PM', v_Stat_Codes_PM,
		    'RB', v_Stat_Codes_RB,
		    'RG', v_Stat_Codes_RG,
		    'RI', v_Stat_Codes_RI,
		    'RL', v_Stat_Codes_RL,
		    'RM', v_Stat_Codes_RM,
		    'RN', v_Stat_Codes_RN,
		    'RP', v_Stat_Codes_RP,
		    'RQ', v_Stat_Codes_RQ,
		    'SM', v_Stat_Codes_SM,
		    'TH', v_Stat_Codes_TH,
		    'VL', v_Stat_Codes_VL,
		    'VP', v_Stat_Codes_VP,
		    'VN', v_Stat_Codes_VN,
		    'VC', v_Stat_Codes_VC,
		    'WC', v_Stat_Codes_WC,
		    'WL', v_Stat_Codes_WL,
		    'CR', v_Stat_Code_CR,
		    'PF', v_Stat_Code_PF,
		    'PI', v_Stat_Code_PI,
		    'PL', v_Stat_Code_PL,
		    'WP', v_Stat_Code_WP,
		    v_statistical_code
		) AS V_Formatted_Stat_Codes,
		-- *INF*: SUBSTR(V_Formatted_Stat_Codes,1,25)
		SUBSTR(V_Formatted_Stat_Codes, 1, 25) AS Formatted_Stat_Codes,
		-- *INF*: SUBSTR(V_Formatted_Stat_Codes,26,9)
		SUBSTR(V_Formatted_Stat_Codes, 26, 9) AS Formatted_Stat_Codes_26_34,
		-- *INF*: SUBSTR(V_Formatted_Stat_Codes,35,4)
		SUBSTR(V_Formatted_Stat_Codes, 35, 4) AS Formatted_Stat_Codes_34_38,
		-- *INF*: DECODE(Type_Bureau,'AI', (v_pos_11 || v_pos_12),
		-- 'AL', (v_pos_15  ||  v_pos_16),
		-- 'AN',(v_pos_12 || v_pos_13),
		-- 'AP',(v_pos_12 || v_pos_13),
		-- 'A2',(v_pos_8 || v_pos_9),
		-- 'A3',(v_pos_8 || v_pos_9),
		-- 'BB',(v_pos_20 || v_pos_21),
		-- 'BC',(v_pos_18 || v_pos_19),
		-- 'BE', ( v_pos_4  || v_pos_5),
		-- 'BF', (v_pos_1  ||  v_pos_2),
		-- 'BP', (' '  ||  v_pos_2),
		-- 'BI', (v_pos_3 ||  v_pos_4),
		-- 'BL', (v_pos_3  ||  v_pos_4),
		-- 'BM',(v_pos_20 || v_pos_21),
		-- 'BT', (v_pos_11  ||  v_pos_12),
		-- 'B2',(v_pos_14  ||  v_pos_15),
		-- 'CF', (v_pos_8  || v_pos_9),
		-- 'CI',(v_pos_3  ||  v_pos_4),
		-- 'CN', (v_pos_1  ||  v_pos_2),
		-- 'CP', (v_pos_3  ||  v_pos_4),
		-- 'EI', (v_pos_2  ||  v_pos_3),
		-- 'EQ', (v_pos_8  || v_pos_9),
		-- 'FF', (v_pos_8  || v_pos_9),
		-- 'FI', (v_pos_1  ||  v_pos_2),
		-- 'FM', (v_pos_6  ||  v_pos_7),
		-- 'FO', (v_pos_8  || v_pos_9),
		-- 'FP', (v_pos_2  ||  v_pos_3),
		-- 'FT', (v_pos_4  ||  v_pos_5),
		-- 'GI', (v_pos_10  ||  v_pos_11),
		-- 'GL',(v_pos_20 || v_pos_21),
		-- 'GM', (v_pos_1  ||  v_pos_2),
		-- 'GP', (v_pos_8  || v_pos_9),
		-- 'GS',(v_pos_3  ||  v_pos_4),
		-- 'II', (v_pos_1  ||  v_pos_2),
		-- 'IM', (v_pos_1  ||  v_pos_2),
		-- 'MI',(v_pos_10  ||  v_pos_11),
		-- 'ML', (v_pos_16  ||  v_pos_17),
		-- 'MP', (v_pos_1  ||  v_pos_2),
		-- 'M2', (v_pos_15  ||  v_pos_16),'  ')
		-- 
		-- 
		-- 
		-- 
		DECODE(
		    Type_Bureau,
		    'AI', (v_pos_11 || v_pos_12),
		    'AL', (v_pos_15 || v_pos_16),
		    'AN', (v_pos_12 || v_pos_13),
		    'AP', (v_pos_12 || v_pos_13),
		    'A2', (v_pos_8 || v_pos_9),
		    'A3', (v_pos_8 || v_pos_9),
		    'BB', (v_pos_20 || v_pos_21),
		    'BC', (v_pos_18 || v_pos_19),
		    'BE', (v_pos_4 || v_pos_5),
		    'BF', (v_pos_1 || v_pos_2),
		    'BP', (' ' || v_pos_2),
		    'BI', (v_pos_3 || v_pos_4),
		    'BL', (v_pos_3 || v_pos_4),
		    'BM', (v_pos_20 || v_pos_21),
		    'BT', (v_pos_11 || v_pos_12),
		    'B2', (v_pos_14 || v_pos_15),
		    'CF', (v_pos_8 || v_pos_9),
		    'CI', (v_pos_3 || v_pos_4),
		    'CN', (v_pos_1 || v_pos_2),
		    'CP', (v_pos_3 || v_pos_4),
		    'EI', (v_pos_2 || v_pos_3),
		    'EQ', (v_pos_8 || v_pos_9),
		    'FF', (v_pos_8 || v_pos_9),
		    'FI', (v_pos_1 || v_pos_2),
		    'FM', (v_pos_6 || v_pos_7),
		    'FO', (v_pos_8 || v_pos_9),
		    'FP', (v_pos_2 || v_pos_3),
		    'FT', (v_pos_4 || v_pos_5),
		    'GI', (v_pos_10 || v_pos_11),
		    'GL', (v_pos_20 || v_pos_21),
		    'GM', (v_pos_1 || v_pos_2),
		    'GP', (v_pos_8 || v_pos_9),
		    'GS', (v_pos_3 || v_pos_4),
		    'II', (v_pos_1 || v_pos_2),
		    'IM', (v_pos_1 || v_pos_2),
		    'MI', (v_pos_10 || v_pos_11),
		    'ML', (v_pos_16 || v_pos_17),
		    'MP', (v_pos_1 || v_pos_2),
		    'M2', (v_pos_15 || v_pos_16),
		    '  '
		) AS V_Policy_Type,
		V_Policy_Type AS Policy_Type,
		-- *INF*: SUBSTR(sar_class_code,1,3)
		SUBSTR(sar_class_code, 1, 3) AS v_sar_class_3,
		-- *INF*: DECODE(TRUE,
		-- IN (Type_Bureau,'BP','FP','BF','FT'),V_Policy_Type)
		DECODE(
		    TRUE,
		    Type_Bureau IN ('BP','FP','BF','FT'), V_Policy_Type
		) AS v_type_policy_45,
		-- *INF*: DECODE(TRUE,
		-- Type_Bureau='BP',v_pos_2,
		-- Type_Bureau='BF',v_pos_2,
		-- Type_Bureau='FP',' ',
		-- Type_Bureau='FT',' '  )
		DECODE(
		    TRUE,
		    Type_Bureau = 'BP', v_pos_2,
		    Type_Bureau = 'BF', v_pos_2,
		    Type_Bureau = 'FP', ' ',
		    Type_Bureau = 'FT', ' '
		) AS v_type_of_bond_6,
		-- *INF*: DECODE(TRUE,
		--  IN(Type_Bureau,'BP','BF','FP','FT'),v_sar_class_3  || v_type_policy_45 || v_type_of_bond_6,
		-- sar_class_code)
		DECODE(
		    TRUE,
		    Type_Bureau IN ('BP','BF','FP','FT'), v_sar_class_3 || v_type_policy_45 || v_type_of_bond_6,
		    sar_class_code
		) AS v_hold_sar_class_code,
		v_hold_sar_class_code AS sar_class_code_out
		FROM EXP_Pre_StatCode_ops
	),
	EXP_Derive_CSP_Fields AS (
		SELECT
		Formatted_Stat_Codes_26_34,
		-- *INF*: SUBSTR(Formatted_Stat_Codes_26_34,1,3)
		SUBSTR(Formatted_Stat_Codes_26_34, 1, 3) AS csp_rate_mod,
		-- *INF*: SUBSTR(Formatted_Stat_Codes_26_34,4,3)
		SUBSTR(Formatted_Stat_Codes_26_34, 4, 3) AS csp_rate_dep
		FROM EXP_Transform_Statistical_Codes
	),
	EXP_PassThrough_Preoutput AS (
		SELECT
		EXP_Pre_StatCode_ops.SourceSystemID,
		EXP_Pre_StatCode_ops.AgencyAKID,
		EXP_Pre_StatCode_ops.PolicyAKID,
		EXP_Pre_StatCode_ops.ContractCustomerAKID,
		EXP_Pre_StatCode_ops.RiskLocationAKID,
		EXP_Pre_StatCode_ops.PolicyCoverageAKID,
		EXP_Pre_StatCode_ops.StatisticalCoverageAKID,
		EXP_Pre_StatCode_ops.ReinsuranceCoverageAKID,
		EXP_Pre_StatCode_ops.PremiumTransactionAKID,
		EXP_Pre_StatCode_ops.BureauStatisticalCodeAKID,
		EXP_Pre_StatCode_ops.PremiumTransactionCode,
		EXP_Pre_StatCode_ops.PremiumTransactionBookedDate,
		EXP_Pre_StatCode_ops.PremiumMasterPolicyExpirationYear,
		EXP_Pre_StatCode_ops.PolicyCoverageEffectiveDate,
		EXP_Pre_StatCode_ops.PolicyCoverageExpirationDate,
		EXP_Pre_StatCode_ops.PremiumTransactionEffectiveDate,
		EXP_Pre_StatCode_ops.PremiumTransactionExpirationDate,
		EXP_Pre_StatCode_ops.PremiumMasterPolicyTerm,
		EXP_Transform_Statistical_Codes.Policy_Type AS PremiumMasterBureauPolicyType,
		EXP_Pre_StatCode_ops.AuditCode AS PremiumMasterAuditCode,
		EXP_Pre_StatCode_ops.PremiumMasterPremiumType,
		EXP_Pre_StatCode_ops.PremiumMasterBureauStatisticalLine,
		EXP_Pre_StatCode_ops.AgencyActualCommissionRate AS PremiumMasterAgencyCommissionRate,
		EXP_Pre_StatCode_ops.Exposure AS PremiumMasterExposure,
		EXP_Transform_Statistical_Codes.Formatted_Stat_Codes AS PremiumMasterStatisticalCode1,
		EXP_Transform_Statistical_Codes.Formatted_Stat_Codes_26_34 AS PremiumMasterStatisticalCode2,
		EXP_Transform_Statistical_Codes.Formatted_Stat_Codes_34_38 AS PremiumMasterStatisticalCode3,
		EXP_Derive_CSP_Fields.csp_rate_mod AS PremiumMasterRateModifier,
		EXP_Derive_CSP_Fields.csp_rate_dep AS PremiumMasterRateDeparture,
		EXP_Pre_StatCode_ops.PremiumMasterBureauInceptionDate,
		EXP_Pre_StatCode_ops.comments_cs,
		EXP_Pre_StatCode_ops.PremiumTransactionAmount AS PremiumMasterPremium,
		EXP_Pre_StatCode_ops.FullTermPremium AS PremiumMasterFullTermPremium,
		EXP_Pre_StatCode_ops.PremiumMasterRiskAddress,
		EXP_Pre_StatCode_ops.PremiumMasterRiskCityState,
		EXP_Pre_StatCode_ops.PremiumMasterRenewalIndicator,
		EXP_Pre_StatCode_ops.ClassCode AS PremiumMasterClassCode,
		EXP_Pre_StatCode_ops.TypeBureauCode AS TypeBureauCodeIn,
		-- *INF*: DECODE(TRUE,IN( substr(ProductLine,1,2),'87','88','89','90','91','92','93','94','95','96','97'), 'NB',TypeBureauCodeIn)
		-- 
		-- -- Type Bureau Code is overriden to 'NB' for SAR-NON-REPORTING-SB-LINE (see comment)
		DECODE(
		    TRUE,
		    substr(ProductLine, 1, 2) IN ('87','88','89','90','91','92','93','94','95','96','97'), 'NB',
		    TypeBureauCodeIn
		) AS PremiumMasterTypeBureauCode,
		EXP_Pre_StatCode_ops.PremiumMasterRecordType,
		EXP_Pre_StatCode_ops.PremiumMasterInstallmentNumber,
		EXP_Pre_StatCode_ops.ProductLine,
		EXP_Pre_StatCode_ops.SubLineCode,
		EXP_Pre_StatCode_ops.pol_key5 AS PolicyKey,
		EXP_Pre_StatCode_ops.ReasonAmendedCode AS PremiumMasterReasonAmendedCode,
		EXP_Pre_StatCode_ops.RatingCoverageAKId,
		EXP_Pre_StatCode_ops.RatingCoverageEffectiveDate,
		EXP_Pre_StatCode_ops.RatingCoverageExpirationDate,
		EXP_Pre_StatCode_ops.PremiumTransactionEnteredDate,
		EXP_Pre_StatCode_ops.PremiumMasterCustomerCareCommissionRate,
		EXP_Pre_StatCode_ops.WrittenExposure AS PremiumMasterWrittenExposure
		FROM EXP_Derive_CSP_Fields
		 -- Manually join with EXP_Pre_StatCode_ops
		 -- Manually join with EXP_Transform_Statistical_Codes
	),
	OUT_PremiumMasterRecord AS (
		SELECT
		SourceSystemID, 
		AgencyAKID, 
		PolicyAKID, 
		ContractCustomerAKID, 
		RiskLocationAKID, 
		PolicyCoverageAKID, 
		StatisticalCoverageAKID, 
		ReinsuranceCoverageAKID, 
		PremiumTransactionAKID, 
		BureauStatisticalCodeAKID, 
		PremiumTransactionCode, 
		PremiumTransactionBookedDate, 
		PremiumMasterPolicyExpirationYear, 
		PolicyCoverageEffectiveDate, 
		PolicyCoverageExpirationDate, 
		PremiumTransactionEffectiveDate, 
		PremiumTransactionExpirationDate, 
		PremiumMasterPolicyTerm, 
		PremiumMasterBureauPolicyType, 
		PremiumMasterAuditCode, 
		PremiumMasterPremiumType, 
		PremiumMasterBureauStatisticalLine, 
		PremiumMasterAgencyCommissionRate, 
		PremiumMasterExposure, 
		PremiumMasterStatisticalCode1, 
		PremiumMasterStatisticalCode2, 
		PremiumMasterStatisticalCode3, 
		PremiumMasterRateModifier, 
		PremiumMasterRateDeparture, 
		PremiumMasterBureauInceptionDate, 
		comments_cs, 
		PremiumMasterPremium, 
		PremiumMasterFullTermPremium, 
		PremiumMasterRiskAddress, 
		PremiumMasterRiskCityState, 
		PremiumMasterRenewalIndicator, 
		PremiumMasterClassCode, 
		PremiumMasterTypeBureauCode, 
		PremiumMasterRecordType, 
		PremiumMasterInstallmentNumber, 
		ProductLine AS PremiumMasterProductLine, 
		SubLineCode AS PremiumMasterSubLine, 
		PolicyKey, 
		PremiumMasterReasonAmendedCode, 
		RatingCoverageAKId, 
		RatingCoverageEffectiveDate, 
		RatingCoverageExpirationDate, 
		PremiumTransactionEnteredDate, 
		PremiumMasterCustomerCareCommissionRate, 
		PremiumMasterWrittenExposure
		FROM EXP_PassThrough_Preoutput
	),
),
EXP_Insert_DirectTransactions AS (
	SELECT
	mplt_PremiumMasterStatisticalCodeDerivation.SourceSystemID1 AS i_SourceSystemID,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterPolicyExpirationYear AS i_PremiumMasterPolicyExpirationYear,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumTransactionBookedDate1 AS i_PremiumMasterBookedDate,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumTransactionEffectiveDate1 AS i_PremiumMasterCoverageEffectiveDate,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumTransactionExpirationDate1 AS i_PremiumMasterCoverageExpirationDate,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterPolicyTerm AS i_PremiumMasterPolicyTerm,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterBureauStatisticalLine AS i_PremiumMasterBureauStatisticalLine,
	mplt_PremiumMasterStatisticalCodeDerivation.comments_cs AS i_comments_cs,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterCustomerCareCommissionRate1 AS i_PremiumMasterCustomerCareCommissionRate,
	mplt_PremiumMasterStatisticalCodeDerivation.AgencyAKID1 AS AgencyAKID,
	mplt_PremiumMasterStatisticalCodeDerivation.PolicyAKID1 AS PolicyAKID,
	mplt_PremiumMasterStatisticalCodeDerivation.ContractCustomerAKID1 AS ContractCustomerAKID,
	mplt_PremiumMasterStatisticalCodeDerivation.RiskLocationAKID1 AS RiskLocationAKID,
	mplt_PremiumMasterStatisticalCodeDerivation.PolicyCoverageAKID1 AS PolicyCoverageAKID,
	mplt_PremiumMasterStatisticalCodeDerivation.StatisticalCoverageAKID1 AS StatisticalCoverageAKID,
	mplt_PremiumMasterStatisticalCodeDerivation.ReinsuranceCoverageAKID1 AS ReinsuranceCoverageAKID,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumTransactionAKID1 AS PremiumTransactionAKID,
	mplt_PremiumMasterStatisticalCodeDerivation.BureauStatisticalCodeAKID1 AS BureauStatisticalCodeAKID,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumTransactionCode1 AS PremiumMasterTransactionCode,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterBureauPolicyType,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterAuditCode,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterPremiumType,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterAgencyCommissionRate,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterExposure,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterStatisticalCode1,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterStatisticalCode2,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterStatisticalCode3,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterRateModifier,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterRateDeparture,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterBureauInceptionDate,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterPremium,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterFullTermPremium,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterRiskAddress,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterRiskCityState,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterRenewalIndicator,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterRecordType,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterClassCode,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterTypeBureauCode,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterProductLine,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterSubLine,
	mplt_PremiumMasterStatisticalCodeDerivation.PolicyKey,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterReasonAmendedCode,
	mplt_PremiumMasterStatisticalCodeDerivation.RatingCoverageAKId1 AS RatingCoverageAKId,
	mplt_PremiumMasterStatisticalCodeDerivation.RatingCoverageEffectiveDate1 AS RatingCoverageEffectiveDate,
	mplt_PremiumMasterStatisticalCodeDerivation.RatingCoverageExpirationDate1 AS RatingCoverageExpirationDate,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumTransactionEnteredDate1,
	-- *INF*: SET_DATE_PART(
	--          SET_DATE_PART(
	--                      SET_DATE_PART(last_day(add_to_date(sysdate,'MM',@{pipeline().parameters.NO_OF_MONTHS})), 'HH', 23) 
	--                                           ,'MI',59)
	--                                ,'SS',59)
	-- 
	-- -- Determine last day of month and change timestamp to 23:59:59
	DATEADD(SECOND,59-DATE_PART(SECOND,DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,last_day(DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTHS},CURRENT_TIMESTAMP))),last_day(DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTHS},CURRENT_TIMESTAMP)))),DATEADD(HOUR,23-DATE_PART(HOUR,last_day(DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTHS},CURRENT_TIMESTAMP))),last_day(DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTHS},CURRENT_TIMESTAMP))))),DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,last_day(DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTHS},CURRENT_TIMESTAMP))),last_day(DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTHS},CURRENT_TIMESTAMP)))),DATEADD(HOUR,23-DATE_PART(HOUR,last_day(DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTHS},CURRENT_TIMESTAMP))),last_day(DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTHS},CURRENT_TIMESTAMP))))) AS o_PremiumMasterBookedDateOut,
	1 AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS o_ExpirationDate,
	i_SourceSystemID AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	-1 AS o_DefaultID,
	-- *INF*: IIF(ISNULL(i_PremiumMasterPolicyExpirationYear),'N/A', i_PremiumMasterPolicyExpirationYear)
	IFF(i_PremiumMasterPolicyExpirationYear IS NULL, 'N/A', i_PremiumMasterPolicyExpirationYear) AS o_PremiumMasterPolicyExpirationYear,
	-- *INF*: IIF((IN(ltrim(rtrim(PremiumMasterTransactionCode)),'55','65')),TO_DATE('12/31/2099 00:00:00','MM/DD/YYYY HH24:MI:SS'),i_PremiumMasterCoverageEffectiveDate)
	IFF(
	    (ltrim(rtrim(PremiumMasterTransactionCode)) IN ('55','65')),
	    TO_TIMESTAMP('12/31/2099 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    i_PremiumMasterCoverageEffectiveDate
	) AS o_PremiumMasterCoverageEffectiveDate,
	-- *INF*: IIF((IN(ltrim(rtrim(PremiumMasterTransactionCode)),'55','65')),TO_DATE('12/31/2099 00:00:00','MM/DD/YYYY HH24:MI:SS'),i_PremiumMasterCoverageExpirationDate)
	IFF(
	    (ltrim(rtrim(PremiumMasterTransactionCode)) IN ('55','65')),
	    TO_TIMESTAMP('12/31/2099 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    i_PremiumMasterCoverageExpirationDate
	) AS o_PremiumMasterCoverageExpirationDate,
	-- *INF*: IIF(ISNULL(i_PremiumMasterPolicyTerm), 'N/A', i_PremiumMasterPolicyTerm)
	IFF(i_PremiumMasterPolicyTerm IS NULL, 'N/A', i_PremiumMasterPolicyTerm) AS o_PremiumMasterPolicyTerm,
	-- *INF*: IIF(ISNULL(ltrim(rtrim(i_PremiumMasterBureauStatisticalLine))),'N/A',i_PremiumMasterBureauStatisticalLine)
	IFF(
	    ltrim(rtrim(i_PremiumMasterBureauStatisticalLine)) IS NULL, 'N/A',
	    i_PremiumMasterBureauStatisticalLine
	) AS o_PremiumMasterBureauStatisticalLine,
	-- *INF*: IIF(ISNULL(SUBSTR(i_comments_cs,1,1)),'   ',SUBSTR(i_comments_cs,1,1))
	IFF(SUBSTR(i_comments_cs, 1, 1) IS NULL, '   ', SUBSTR(i_comments_cs, 1, 1)) AS o_PremiumMasterCountersignAgencyType,
	-- *INF*: IIF(ISNULL(SUBSTR(i_comments_cs,2,7)),'   ',SUBSTR(i_comments_cs,2,7))
	IFF(SUBSTR(i_comments_cs, 2, 7) IS NULL, '   ', SUBSTR(i_comments_cs, 2, 7)) AS o_PremiumMasterCountersignAgencyCode,
	-- *INF*: IIF(ISNULL(SUBSTR(i_comments_cs,9,2)),'   ',SUBSTR(i_comments_cs,9,2))
	IFF(SUBSTR(i_comments_cs, 9, 2) IS NULL, '   ', SUBSTR(i_comments_cs, 9, 2)) AS o_PremiumMasterCountersignAgencyState,
	-- *INF*: IIF(ISNULL(SUBSTR(i_comments_cs,11,2)),'   ',REPLACECHR(0, SUBSTR(i_comments_cs,11,2), ',', '0'))
	IFF(
	    SUBSTR(i_comments_cs, 11, 2) IS NULL, '   ',
	    REGEXP_REPLACE(SUBSTR(i_comments_cs, 11, 2),',','0','i')
	) AS o_PremiumMasterCountersignAgencyRate,
	-- *INF*: IIF(LTRIM(RTRIM(PremiumMasterPremiumType)) = 'D','0','50')
	IFF(LTRIM(RTRIM(PremiumMasterPremiumType)) = 'D', '0', '50') AS o_TaxBoardPercentage,
	-- *INF*: IIF(ISNULL(i_PremiumMasterCustomerCareCommissionRate),1,i_PremiumMasterCustomerCareCommissionRate)
	IFF(
	    i_PremiumMasterCustomerCareCommissionRate IS NULL, 1,
	    i_PremiumMasterCustomerCareCommissionRate
	) AS o_PremiumMasterCustomerCareCommissionRate,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterWrittenExposure,
	FIL_Invalid_records.DeclaredEventFlag,
	-- *INF*: DECODE(DeclaredEventFlag, 'T', 1, 'F', 0,Null)
	DECODE(
	    DeclaredEventFlag,
	    'T', 1,
	    'F', 0,
	    Null
	) AS o_DeclaredEventFlag
	FROM FIL_Invalid_records
	 -- Manually join with mplt_PremiumMasterStatisticalCodeDerivation
),
LKP_Sup_Type_Bureau_Code AS (
	SELECT
	StandardTypeBureauCode,
	type_bureau_code
	FROM (
		SELECT 
			StandardTypeBureauCode,
			type_bureau_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_type_bureau_code
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY type_bureau_code ORDER BY StandardTypeBureauCode) = 1
),
PremiumMasterCalculation AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumMasterCalculation
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, AgencyAKID, PolicyAKID, ContractCustomerAKID, RiskLocationAKID, PolicyCoverageAKID, StatisticalCoverageAKID, ReinsuranceCoverageAKID, PremiumTransactionAKID, BureauStatisticalCodeAKID, PremiumMasterTransactionCode, PremiumMasterPolicyExpirationYear, PremiumMasterCoverageEffectiveDate, PremiumMasterCoverageExpirationDate, PremiumMasterPolicyTerm, PremiumMasterBureauPolicyType, PremiumMasterAuditCode, PremiumMasterPremiumType, PremiumMasterTypeBureauCode, PremiumMasterBureauStatisticalLine, PremiumMasterProductLine, PremiumMasterAgencyCommissionRate, PremiumMasterClassCode, PremiumMasterExposure, PremiumMasterSubLine, PremiumMasterStatisticalCode1, PremiumMasterStatisticalCode2, PremiumMasterStatisticalCode3, PremiumMasterRateModifier, PremiumMasterRateDeparture, PremiumMasterBureauInceptionDate, PremiumMasterCountersignAgencyType, PremiumMasterCountersignAgencyCode, PremiumMasterCountersignAgencyState, PremiumMasterCountersignAgencyRate, PremiumMasterPremium, PremiumMasterFullTermPremium, TaxBoardPercentage, PremiumMasterRiskAddress, PremiumMasterRiskCityState, PremiumMasterRenewalIndicator, PremiumMasterRecordType, RatingCoverageAKId, RatingCoverageEffectiveDate, RatingCoverageExpirationDate, StagePremiumMasterPKID, PolicyKey, PremiumMasterRunDate, PremiumMasterReasonAmendedCode, PremiumTransactionEnteredDate, PremiumMasterCustomerCareCommissionRate, PremiumMasterWrittenExposure, DeclaredEventFlag)
	SELECT 
	EXP_Insert_DirectTransactions.o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	EXP_Insert_DirectTransactions.o_AuditID AS AUDITID, 
	EXP_Insert_DirectTransactions.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_Insert_DirectTransactions.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_Insert_DirectTransactions.o_SourceSystemID AS SOURCESYSTEMID, 
	EXP_Insert_DirectTransactions.o_CreatedDate AS CREATEDDATE, 
	EXP_Insert_DirectTransactions.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_Insert_DirectTransactions.AGENCYAKID, 
	EXP_Insert_DirectTransactions.POLICYAKID, 
	EXP_Insert_DirectTransactions.CONTRACTCUSTOMERAKID, 
	EXP_Insert_DirectTransactions.RISKLOCATIONAKID, 
	EXP_Insert_DirectTransactions.POLICYCOVERAGEAKID, 
	EXP_Insert_DirectTransactions.STATISTICALCOVERAGEAKID, 
	EXP_Insert_DirectTransactions.REINSURANCECOVERAGEAKID, 
	EXP_Insert_DirectTransactions.PREMIUMTRANSACTIONAKID, 
	EXP_Insert_DirectTransactions.BUREAUSTATISTICALCODEAKID, 
	EXP_Insert_DirectTransactions.PREMIUMMASTERTRANSACTIONCODE, 
	EXP_Insert_DirectTransactions.o_PremiumMasterPolicyExpirationYear AS PREMIUMMASTERPOLICYEXPIRATIONYEAR, 
	EXP_Insert_DirectTransactions.o_PremiumMasterCoverageEffectiveDate AS PREMIUMMASTERCOVERAGEEFFECTIVEDATE, 
	EXP_Insert_DirectTransactions.o_PremiumMasterCoverageExpirationDate AS PREMIUMMASTERCOVERAGEEXPIRATIONDATE, 
	EXP_Insert_DirectTransactions.o_PremiumMasterPolicyTerm AS PREMIUMMASTERPOLICYTERM, 
	EXP_Insert_DirectTransactions.PREMIUMMASTERBUREAUPOLICYTYPE, 
	EXP_Insert_DirectTransactions.PREMIUMMASTERAUDITCODE, 
	EXP_Insert_DirectTransactions.PREMIUMMASTERPREMIUMTYPE, 
	LKP_Sup_Type_Bureau_Code.StandardTypeBureauCode AS PREMIUMMASTERTYPEBUREAUCODE, 
	EXP_Insert_DirectTransactions.o_PremiumMasterBureauStatisticalLine AS PREMIUMMASTERBUREAUSTATISTICALLINE, 
	EXP_Insert_DirectTransactions.PREMIUMMASTERPRODUCTLINE, 
	EXP_Insert_DirectTransactions.PREMIUMMASTERAGENCYCOMMISSIONRATE, 
	EXP_Insert_DirectTransactions.PREMIUMMASTERCLASSCODE, 
	EXP_Insert_DirectTransactions.PREMIUMMASTEREXPOSURE, 
	EXP_Insert_DirectTransactions.PREMIUMMASTERSUBLINE, 
	EXP_Insert_DirectTransactions.PREMIUMMASTERSTATISTICALCODE1, 
	EXP_Insert_DirectTransactions.PREMIUMMASTERSTATISTICALCODE2, 
	EXP_Insert_DirectTransactions.PREMIUMMASTERSTATISTICALCODE3, 
	EXP_Insert_DirectTransactions.PREMIUMMASTERRATEMODIFIER, 
	EXP_Insert_DirectTransactions.PREMIUMMASTERRATEDEPARTURE, 
	EXP_Insert_DirectTransactions.PREMIUMMASTERBUREAUINCEPTIONDATE, 
	EXP_Insert_DirectTransactions.o_PremiumMasterCountersignAgencyType AS PREMIUMMASTERCOUNTERSIGNAGENCYTYPE, 
	EXP_Insert_DirectTransactions.o_PremiumMasterCountersignAgencyCode AS PREMIUMMASTERCOUNTERSIGNAGENCYCODE, 
	EXP_Insert_DirectTransactions.o_PremiumMasterCountersignAgencyState AS PREMIUMMASTERCOUNTERSIGNAGENCYSTATE, 
	EXP_Insert_DirectTransactions.o_PremiumMasterCountersignAgencyRate AS PREMIUMMASTERCOUNTERSIGNAGENCYRATE, 
	EXP_Insert_DirectTransactions.PREMIUMMASTERPREMIUM, 
	EXP_Insert_DirectTransactions.PREMIUMMASTERFULLTERMPREMIUM, 
	EXP_Insert_DirectTransactions.o_TaxBoardPercentage AS TAXBOARDPERCENTAGE, 
	EXP_Insert_DirectTransactions.PREMIUMMASTERRISKADDRESS, 
	EXP_Insert_DirectTransactions.PREMIUMMASTERRISKCITYSTATE, 
	EXP_Insert_DirectTransactions.PREMIUMMASTERRENEWALINDICATOR, 
	EXP_Insert_DirectTransactions.PREMIUMMASTERRECORDTYPE, 
	EXP_Insert_DirectTransactions.RATINGCOVERAGEAKID, 
	EXP_Insert_DirectTransactions.RATINGCOVERAGEEFFECTIVEDATE, 
	EXP_Insert_DirectTransactions.RATINGCOVERAGEEXPIRATIONDATE, 
	EXP_Insert_DirectTransactions.o_DefaultID AS STAGEPREMIUMMASTERPKID, 
	EXP_Insert_DirectTransactions.POLICYKEY, 
	EXP_Insert_DirectTransactions.o_PremiumMasterBookedDateOut AS PREMIUMMASTERRUNDATE, 
	EXP_Insert_DirectTransactions.PREMIUMMASTERREASONAMENDEDCODE, 
	EXP_Insert_DirectTransactions.PremiumTransactionEnteredDate1 AS PREMIUMTRANSACTIONENTEREDDATE, 
	EXP_Insert_DirectTransactions.o_PremiumMasterCustomerCareCommissionRate AS PREMIUMMASTERCUSTOMERCARECOMMISSIONRATE, 
	EXP_Insert_DirectTransactions.PREMIUMMASTERWRITTENEXPOSURE, 
	EXP_Insert_DirectTransactions.o_DeclaredEventFlag AS DECLAREDEVENTFLAG
	FROM EXP_Insert_DirectTransactions
),
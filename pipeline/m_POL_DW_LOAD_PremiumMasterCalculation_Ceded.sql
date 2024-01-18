WITH
SQ_PremiumCalculationCededTransactions AS (
	SELECT	POL.source_sys_id,
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
		PT.PremiumTransactionCode, 
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
	WHERE 
	    	          CUSADDR.contract_cust_ak_id = POL.contract_cust_ak_id
	    	AND LOC.PolicyAKID = POL.pol_ak_id
		AND LOC.RiskLocationAKID = POLCOV.RiskLocationAKID
		AND POLCOV.PolicyCoverageAKID = STATCOV.PolicyCoverageAKID
	    	AND STATCOV.StatisticalCoverageAKID = PT.StatisticalCoverageAKID
	    	AND PT.PremiumTransactionAKID = STATCOD.PremiumTransactionAKID
	      AND PT.PremiumType = 'C'
		AND CUSADDR.crrnt_snpsht_flag = 1  
		AND POL.crrnt_snpsht_flag = 1 
		AND LOC.CurrentSnapshotFlag =1
		AND STATCOV.CurrentSnapshotFlag =1 
		AND POLCOV.CurrentSnapshotFlag =1 
		AND PT.CurrentSnapshotFlag =1 
		AND STATCOD.CurrentSnapshotFlag =1
	      	AND datepart(MM,PremiumTransactionBookedDate)=datepart(MM,(DATEADD(MM,@{pipeline().parameters.NO_OF_MONTHS},GETDATE()))) 
		AND datepart(YYYY,PremiumTransactionBookedDate)=datepart(YYYY,(DATEADD(MM,@{pipeline().parameters.NO_OF_MONTHS},GETDATE())))
		@{pipeline().parameters.WHERE_CLAUSE}
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
EXP_CededTransactions AS (
	SELECT
	source_sys_id,
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
	PremiumTransactionCode,
	PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	PremiumTransactionExpirationDate,
	PremiumTransactionBookedDate,
	PremiumTransactionAmount,
	FullTermPremium,
	PremiumType,
	ReasonAmendedCode,
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
	FROM SQ_PremiumCalculationCededTransactions
),
LKP_Reinsurance_Coverage AS (
	SELECT
	reins_prcnt_facultative_commssn,
	reins_cov_ak_id
	FROM (
		SELECT 
			reins_prcnt_facultative_commssn,
			reins_cov_ak_id
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.reinsurance_coverage
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY reins_cov_ak_id ORDER BY reins_prcnt_facultative_commssn DESC) = 1
),
lkp_sup_reason_amended_code AS (
	SELECT
	StandardReasonAmendedCode,
	rsn_amended_code,
	source_sys_id
	FROM (
		SELECT 
			StandardReasonAmendedCode,
			rsn_amended_code,
			source_sys_id
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_reason_amended_code
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY rsn_amended_code,source_sys_id ORDER BY StandardReasonAmendedCode) = 1
),
EXP_Evaluate_fields AS (
	SELECT
	EXP_CededTransactions.agency_ak_id,
	EXP_CededTransactions.pol_ak_id,
	EXP_CededTransactions.pol_sym,
	EXP_CededTransactions.pol_num,
	EXP_CededTransactions.pol_mod,
	EXP_CededTransactions.pol_key,
	EXP_CededTransactions.pol_eff_date,
	EXP_CededTransactions.pol_exp_date,
	EXP_CededTransactions.pms_pol_lob_code,
	EXP_CededTransactions.pol_issue_code,
	EXP_CededTransactions.contract_cust_ak_id,
	EXP_CededTransactions.addr_line_1,
	EXP_CededTransactions.city_name,
	EXP_CededTransactions.state_prov_code,
	EXP_CededTransactions.zip_postal_code,
	EXP_CededTransactions.RiskLocationAKID,
	EXP_CededTransactions.LocationUnitNumber,
	EXP_CededTransactions.LocationIndicator,
	EXP_CededTransactions.PolicyCoverageAKID,
	EXP_CededTransactions.InsuranceLine,
	EXP_CededTransactions.TypeBureauCode,
	EXP_CededTransactions.PolicyCoverageEffectiveDate,
	EXP_CededTransactions.PolicyCoverageExpirationDate,
	EXP_CededTransactions.StatisticalCoverageAKID,
	EXP_CededTransactions.SubLocationUnitNumber,
	EXP_CededTransactions.RiskUnitGroup,
	EXP_CededTransactions.RiskUnitGroupSequenceNumber,
	EXP_CededTransactions.RiskUnit,
	EXP_CededTransactions.RiskUnitSequenceNumber,
	EXP_CededTransactions.MajorPerilCode,
	EXP_CededTransactions.MajorPerilSequenceNumber,
	EXP_CededTransactions.SublineCode,
	EXP_CededTransactions.PMSTypeExposure,
	EXP_CededTransactions.ClassCode,
	EXP_CededTransactions.o_Exposure AS Exposure,
	EXP_CededTransactions.StatisticalCoverageEffectiveDate,
	EXP_CededTransactions.StatisticalCoverageExpirationDate,
	LKP_Reinsurance_Coverage.reins_prcnt_facultative_commssn,
	EXP_CededTransactions.AgencyActualCommissionRate,
	EXP_CededTransactions.ReinsuranceSectionCode,
	EXP_CededTransactions.PremiumLoadSequence,
	EXP_CededTransactions.PremiumTransactionAKID,
	EXP_CededTransactions.ReinsuranceCoverageAKID,
	EXP_CededTransactions.PMSFunctionCode,
	EXP_CededTransactions.PremiumTransactionCode,
	EXP_CededTransactions.PremiumTransactionEnteredDate,
	EXP_CededTransactions.PremiumTransactionEffectiveDate,
	EXP_CededTransactions.PremiumTransactionExpirationDate,
	EXP_CededTransactions.PremiumTransactionBookedDate,
	EXP_CededTransactions.PremiumTransactionAmount,
	EXP_CededTransactions.FullTermPremium,
	EXP_CededTransactions.PremiumType,
	lkp_sup_reason_amended_code.StandardReasonAmendedCode AS ReasonAmendedCode,
	EXP_CededTransactions.BureauStatisticalCodeAKID,
	EXP_CededTransactions.BureauCode1,
	EXP_CededTransactions.BureauCode2,
	EXP_CededTransactions.BureauCode3,
	EXP_CededTransactions.BureauCode4,
	EXP_CededTransactions.BureauCode5,
	EXP_CededTransactions.BureauCode6,
	EXP_CededTransactions.BureauCode7,
	EXP_CededTransactions.BureauCode8,
	EXP_CededTransactions.BureauCode9,
	EXP_CededTransactions.BureauCode10,
	EXP_CededTransactions.BureauCode11,
	EXP_CededTransactions.BureauCode12,
	EXP_CededTransactions.BureauCode13,
	EXP_CededTransactions.BureauCode14,
	EXP_CededTransactions.BureauCode15,
	EXP_CededTransactions.BureauSpecialUseCode,
	EXP_CededTransactions.PMSAnnualStatementLine,
	EXP_CededTransactions.RatingDateIndicator,
	EXP_CededTransactions.BureauStatisticalUserLine,
	-- *INF*: BureauCode1 || BureauCode2 || BureauCode3 || BureauCode4 || BureauCode5 || BureauCode6 || BureauCode7 || BureauCode8 || BureauCode9 || BureauCode10 || BureauCode11 || BureauCode12 || BureauCode13 || BureauCode14 || BureauCode15
	-- 
	-- ---- we concatenate the individual BureauCode components to get a 38 byte string for future manipulations
	BureauCode1 || BureauCode2 || BureauCode3 || BureauCode4 || BureauCode5 || BureauCode6 || BureauCode7 || BureauCode8 || BureauCode9 || BureauCode10 || BureauCode11 || BureauCode12 || BureauCode13 || BureauCode14 || BureauCode15 AS v_StatisticalCodes,
	-- *INF*: SUBSTR(TO_CHAR((ADD_TO_DATE(SYSTIMESTAMP(),'MM',@{pipeline().parameters.NO_OF_MONTHS})),'YYYYMMDD'),1,6)
	-- 
	-- -- This calculation derives the monthly close out date subtracting the parametrized number of months from current system date
	SUBSTR(TO_CHAR((DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTHS},CURRENT_TIMESTAMP())), 'YYYYMMDD'), 1, 6) AS v_Account_Date,
	-- *INF*: IIF(substr(TO_CHAR(PremiumTransactionBookedDate, 'YYYYMMDD'),1,6) = v_Account_Date,'VALID','INVALID')
	-- 
	-- --- We flag transactions that were booked outside of the closeout month as invalid
	IFF(
	    substr(TO_CHAR(PremiumTransactionBookedDate, 'YYYYMMDD'), 1, 6) = v_Account_Date, 'VALID',
	    'INVALID'
	) AS v_Valid_Record_Generation,
	-- *INF*: 'REINSURANCE'
	-- 
	-- 
	-- --Below check was comented out as part of Limits, as we need the Transactions which have a premium of 0.0
	-- --IIF(PremiumTransactionAmount = 0.0 AND FullTermPremium =0.0,'NOGENERATION','REINSURANCE')
	-- 
	-- -- Flagging transactions with zero monies for dropping since they are not passed through to the Premium Master
	-- 
	'REINSURANCE' AS v_status,
	-- *INF*: IIF(v_Valid_Record_Generation = 'VALID',v_status,v_Valid_Record_Generation)
	-- 
	-- -- setting a flag for transactions in the closeout month and marking the remaining and/or those with zero monies as invalid
	IFF(v_Valid_Record_Generation = 'VALID', v_status, v_Valid_Record_Generation) AS Status,
	v_StatisticalCodes AS StatisticalCodes,
	-- *INF*: IIF(RatingDateIndicator='C',StatisticalCoverageEffectiveDate,pol_eff_date)
	-- 
	-- -- Inception date defaults to policy effective date except if the Rating Date Indicator is 'C' in which case it is the transaction Coverage effective date
	IFF(RatingDateIndicator = 'C', StatisticalCoverageEffectiveDate, pol_eff_date) AS v_inc_date,
	-- *INF*: DECODE(TRUE,IN( RTRIM(PremiumTransactionCode),'83','84'),PremiumTransactionEffectiveDate,v_inc_date)
	-- 
	-- -- for SERP or SERP reversed premium transactions, the inception date is overriden by transaction effective date
	DECODE(
	    TRUE,
	    RTRIM(PremiumTransactionCode) IN ('83','84'), PremiumTransactionEffectiveDate,
	    v_inc_date
	) AS PremiumMasterBureauInceptionDate,
	-- *INF*: rtrim(addr_line_1)
	rtrim(addr_line_1) AS PremiumMasterRiskAddress,
	-- *INF*: rtrim(city_name) || ', ' || rtrim(state_prov_code)
	rtrim(city_name) || ', ' || rtrim(state_prov_code) AS PremiumMasterRiskCityState,
	EXP_CededTransactions.AuditReinstatementIndicator,
	-- *INF*: IIF(RTRIM(PremiumTransactionCode) = '15' AND RTRIM(AuditReinstatementIndicator) = 'R','R',' ')
	-- 
	-- -- Renewal indicator is defaulted to spaces except if it is a reinstated policy in which case it is 'R'
	IFF(
	    RTRIM(PremiumTransactionCode) = '15' AND RTRIM(AuditReinstatementIndicator) = 'R', 'R', ' '
	) AS PremiumMasterRenewalIndicator,
	EXP_CededTransactions.pol_audit_frqncy,
	EXP_CededTransactions.pol_term,
	-- *INF*: 'CEDED'
	-- 
	-- -- flagging pure ceded transactions that pass through and create corresponding ceded transaction on a one to one basis
	'CEDED' AS PremiumMasterRecordType,
	EXP_CededTransactions.o_WrittenExposure AS WrittenExposure,
	EXP_CededTransactions.DeclaredEventsFlag
	FROM EXP_CededTransactions
	LEFT JOIN LKP_Reinsurance_Coverage
	ON LKP_Reinsurance_Coverage.reins_cov_ak_id = EXP_CededTransactions.ReinsuranceCoverageAKID
	LEFT JOIN lkp_sup_reason_amended_code
	ON lkp_sup_reason_amended_code.rsn_amended_code = EXP_CededTransactions.ReasonAmendedCode AND lkp_sup_reason_amended_code.source_sys_id = EXP_CededTransactions.source_sys_id
),
LKP_Sup_Type_Bureau_Code AS (
	SELECT
	type_bureau_code,
	StandardTypeBureauCode,
	TypeBureauCode
	FROM (
		SELECT 
			type_bureau_code,
			StandardTypeBureauCode,
			TypeBureauCode
		FROM sup_type_bureau_code
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY type_bureau_code ORDER BY type_bureau_code) = 1
),
FIL_Invalid_Records AS (
	SELECT
	EXP_Evaluate_fields.agency_ak_id AS AgencyAKID, 
	EXP_Evaluate_fields.pol_ak_id AS PolicyAKID, 
	EXP_Evaluate_fields.contract_cust_ak_id AS ContractCustomerAKID, 
	EXP_Evaluate_fields.RiskLocationAKID, 
	EXP_Evaluate_fields.PolicyCoverageAKID, 
	EXP_Evaluate_fields.StatisticalCoverageAKID, 
	EXP_Evaluate_fields.PremiumTransactionAKID, 
	EXP_Evaluate_fields.BureauStatisticalCodeAKID, 
	EXP_Evaluate_fields.ReinsuranceCoverageAKID, 
	EXP_Evaluate_fields.pol_sym, 
	EXP_Evaluate_fields.pol_num, 
	EXP_Evaluate_fields.pol_mod, 
	EXP_Evaluate_fields.pol_key, 
	EXP_Evaluate_fields.pol_eff_date, 
	EXP_Evaluate_fields.pol_exp_date, 
	EXP_Evaluate_fields.pms_pol_lob_code, 
	EXP_Evaluate_fields.pol_issue_code, 
	EXP_Evaluate_fields.LocationUnitNumber, 
	EXP_Evaluate_fields.LocationIndicator, 
	EXP_Evaluate_fields.InsuranceLine, 
	LKP_Sup_Type_Bureau_Code.StandardTypeBureauCode AS TypeBureauCode, 
	EXP_Evaluate_fields.PolicyCoverageEffectiveDate, 
	EXP_Evaluate_fields.PolicyCoverageExpirationDate, 
	EXP_Evaluate_fields.SubLocationUnitNumber, 
	EXP_Evaluate_fields.RiskUnitGroup, 
	EXP_Evaluate_fields.RiskUnitGroupSequenceNumber, 
	EXP_Evaluate_fields.RiskUnit, 
	EXP_Evaluate_fields.RiskUnitSequenceNumber, 
	EXP_Evaluate_fields.MajorPerilCode, 
	EXP_Evaluate_fields.MajorPerilSequenceNumber, 
	EXP_Evaluate_fields.SublineCode, 
	EXP_Evaluate_fields.PMSTypeExposure, 
	EXP_Evaluate_fields.ClassCode, 
	EXP_Evaluate_fields.Exposure, 
	EXP_Evaluate_fields.StatisticalCoverageEffectiveDate, 
	EXP_Evaluate_fields.StatisticalCoverageExpirationDate, 
	EXP_Evaluate_fields.reins_prcnt_facultative_commssn AS AgencyActualCommissionRate, 
	EXP_Evaluate_fields.ReinsuranceSectionCode, 
	EXP_Evaluate_fields.PremiumLoadSequence, 
	EXP_Evaluate_fields.PMSFunctionCode, 
	EXP_Evaluate_fields.PremiumTransactionCode, 
	EXP_Evaluate_fields.PremiumTransactionEnteredDate, 
	EXP_Evaluate_fields.PremiumTransactionEffectiveDate, 
	EXP_Evaluate_fields.PremiumTransactionExpirationDate, 
	EXP_Evaluate_fields.PremiumTransactionBookedDate, 
	EXP_Evaluate_fields.PremiumTransactionAmount, 
	EXP_Evaluate_fields.FullTermPremium, 
	EXP_Evaluate_fields.PremiumType, 
	EXP_Evaluate_fields.ReasonAmendedCode, 
	EXP_Evaluate_fields.BureauSpecialUseCode, 
	EXP_Evaluate_fields.PMSAnnualStatementLine, 
	EXP_Evaluate_fields.RatingDateIndicator, 
	EXP_Evaluate_fields.BureauStatisticalUserLine, 
	EXP_Evaluate_fields.Status, 
	EXP_Evaluate_fields.StatisticalCodes, 
	EXP_Evaluate_fields.PremiumMasterBureauInceptionDate, 
	EXP_Evaluate_fields.PremiumMasterRiskAddress, 
	EXP_Evaluate_fields.PremiumMasterRiskCityState, 
	EXP_Evaluate_fields.PremiumMasterRenewalIndicator, 
	EXP_Evaluate_fields.pol_audit_frqncy, 
	EXP_Evaluate_fields.pol_term, 
	EXP_Evaluate_fields.PremiumMasterRecordType, 
	EXP_Evaluate_fields.WrittenExposure, 
	EXP_Evaluate_fields.DeclaredEventsFlag
	FROM EXP_Evaluate_fields
	LEFT JOIN LKP_Sup_Type_Bureau_Code
	ON LKP_Sup_Type_Bureau_Code.type_bureau_code = EXP_Evaluate_fields.TypeBureauCode
	WHERE Status = 'REINSURANCE'

--- Filters all invalid records that are ineligible because of date or premium monies being zero
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
EXP_Insert_CededTransactions AS (
	SELECT
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	'PMS' AS SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
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
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumTransactionBookedDate1 AS PremiumMasterBookedDate,
	-- *INF*: SET_DATE_PART(
	--          SET_DATE_PART(
	--                      SET_DATE_PART(last_day(add_to_date(sysdate,'MM',@{pipeline().parameters.NO_OF_MONTHS})), 'HH', 23) 
	--                                           ,'MI',59)
	--                                ,'SS',59)
	-- 
	-- -- Determine last day of month and change timestamp to 23:59:59
	-- 
	DATEADD(SECOND,59-DATE_PART(SECOND,DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,last_day(DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTHS},CURRENT_TIMESTAMP))),last_day(DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTHS},CURRENT_TIMESTAMP)))),DATEADD(HOUR,23-DATE_PART(HOUR,last_day(DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTHS},CURRENT_TIMESTAMP))),last_day(DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTHS},CURRENT_TIMESTAMP))))),DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,last_day(DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTHS},CURRENT_TIMESTAMP))),last_day(DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTHS},CURRENT_TIMESTAMP)))),DATEADD(HOUR,23-DATE_PART(HOUR,last_day(DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTHS},CURRENT_TIMESTAMP))),last_day(DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTHS},CURRENT_TIMESTAMP))))) AS PremiumMasterBookedDateOut,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterPolicyExpirationYear,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumTransactionEffectiveDate1 AS PremiumMasterCoverageEffectiveDate,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumTransactionExpirationDate1 AS PremiumMasterCoverageExpirationDate,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterPolicyTerm,
	-- *INF*: IIF(ISNULL(PremiumMasterPolicyTerm), '0', PremiumMasterPolicyTerm)
	IFF(PremiumMasterPolicyTerm IS NULL, '0', PremiumMasterPolicyTerm) AS o_PremiumMasterPolicyTerm,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterBureauPolicyType,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterAuditCode,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterPremiumType,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterBureauStatisticalLine,
	-- *INF*: IIF(ISNULL(ltrim(rtrim(PremiumMasterBureauStatisticalLine))),'N/A',PremiumMasterBureauStatisticalLine)
	IFF(
	    ltrim(rtrim(PremiumMasterBureauStatisticalLine)) IS NULL, 'N/A',
	    PremiumMasterBureauStatisticalLine
	) AS PremiumMasterBureauStatisticalLineout,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterAgencyCommissionRate,
	-- *INF*: IIF(ISNULL(PremiumMasterAgencyCommissionRate), 0.00000, PremiumMasterAgencyCommissionRate)
	IFF(PremiumMasterAgencyCommissionRate IS NULL, 0.00000, PremiumMasterAgencyCommissionRate) AS o_PremiumMasterAgencyCommissionRate,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterExposure,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterStatisticalCode1,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterStatisticalCode2,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterStatisticalCode3,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterRateModifier,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterRateDeparture,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterBureauInceptionDate,
	mplt_PremiumMasterStatisticalCodeDerivation.comments_cs,
	-- *INF*: IIF(ISNULL(SUBSTR(comments_cs,1,1)),'   ',SUBSTR(comments_cs,1,1))
	IFF(SUBSTR(comments_cs, 1, 1) IS NULL, '   ', SUBSTR(comments_cs, 1, 1)) AS PremiumMasterCountersignAgencyType,
	-- *INF*: IIF(ISNULL(SUBSTR(comments_cs,2,7)),'   ',SUBSTR(comments_cs,2,7))
	IFF(SUBSTR(comments_cs, 2, 7) IS NULL, '   ', SUBSTR(comments_cs, 2, 7)) AS PremiumMasterCountersignAgencyCode,
	-- *INF*: IIF(ISNULL(SUBSTR(comments_cs,9,2)),'   ',SUBSTR(comments_cs,9,2))
	IFF(SUBSTR(comments_cs, 9, 2) IS NULL, '   ', SUBSTR(comments_cs, 9, 2)) AS PremiumMasterCountersignAgencyState,
	-- *INF*: IIF(ISNULL(SUBSTR(comments_cs,11,2)),'   ',REPLACECHR(0, SUBSTR(comments_cs,11,2), ',', '0'))
	IFF(
	    SUBSTR(comments_cs, 11, 2) IS NULL, '   ',
	    REGEXP_REPLACE(SUBSTR(comments_cs, 11, 2),',','0','i')
	) AS PremiumMasterCountersignAgencyRate,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterPremium,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterFullTermPremium,
	'50' AS TaxBoardPercentage,
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
	-1 AS DefaultID,
	-1 AS RatingCoverageAKId,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS RatingCoverageEffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS RatingCoverageExpirationDate,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumTransactionEnteredDate1,
	1 AS o_PremiumMasterCustomerCareCommissionRate,
	mplt_PremiumMasterStatisticalCodeDerivation.PremiumMasterWrittenExposure,
	FIL_Invalid_Records.DeclaredEventsFlag,
	-- *INF*: DECODE(DeclaredEventsFlag, 'T', 1, 'F', 0,Null)
	DECODE(
	    DeclaredEventsFlag,
	    'T', 1,
	    'F', 0,
	    Null
	) AS o_DeclaredEventsFlag
	FROM FIL_Invalid_Records
	 -- Manually join with mplt_PremiumMasterStatisticalCodeDerivation
),
PremiumMasterCalculation AS (
	INSERT INTO PremiumMasterCalculation
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, AgencyAKID, PolicyAKID, ContractCustomerAKID, RiskLocationAKID, PolicyCoverageAKID, StatisticalCoverageAKID, ReinsuranceCoverageAKID, PremiumTransactionAKID, BureauStatisticalCodeAKID, PremiumMasterTransactionCode, PremiumMasterPolicyExpirationYear, PremiumMasterCoverageEffectiveDate, PremiumMasterCoverageExpirationDate, PremiumMasterPolicyTerm, PremiumMasterBureauPolicyType, PremiumMasterAuditCode, PremiumMasterPremiumType, PremiumMasterTypeBureauCode, PremiumMasterBureauStatisticalLine, PremiumMasterProductLine, PremiumMasterAgencyCommissionRate, PremiumMasterClassCode, PremiumMasterExposure, PremiumMasterSubLine, PremiumMasterStatisticalCode1, PremiumMasterStatisticalCode2, PremiumMasterStatisticalCode3, PremiumMasterRateModifier, PremiumMasterRateDeparture, PremiumMasterBureauInceptionDate, PremiumMasterCountersignAgencyType, PremiumMasterCountersignAgencyCode, PremiumMasterCountersignAgencyState, PremiumMasterCountersignAgencyRate, PremiumMasterPremium, PremiumMasterFullTermPremium, TaxBoardPercentage, PremiumMasterRiskAddress, PremiumMasterRiskCityState, PremiumMasterRenewalIndicator, PremiumMasterRecordType, RatingCoverageAKId, RatingCoverageEffectiveDate, RatingCoverageExpirationDate, StagePremiumMasterPKID, PolicyKey, PremiumMasterRunDate, PremiumMasterReasonAmendedCode, PremiumTransactionEnteredDate, PremiumMasterCustomerCareCommissionRate, PremiumMasterWrittenExposure, DeclaredEventFlag)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	AGENCYAKID, 
	POLICYAKID, 
	CONTRACTCUSTOMERAKID, 
	RISKLOCATIONAKID, 
	POLICYCOVERAGEAKID, 
	STATISTICALCOVERAGEAKID, 
	REINSURANCECOVERAGEAKID, 
	PREMIUMTRANSACTIONAKID, 
	BUREAUSTATISTICALCODEAKID, 
	PREMIUMMASTERTRANSACTIONCODE, 
	PREMIUMMASTERPOLICYEXPIRATIONYEAR, 
	PREMIUMMASTERCOVERAGEEFFECTIVEDATE, 
	PREMIUMMASTERCOVERAGEEXPIRATIONDATE, 
	o_PremiumMasterPolicyTerm AS PREMIUMMASTERPOLICYTERM, 
	PREMIUMMASTERBUREAUPOLICYTYPE, 
	PREMIUMMASTERAUDITCODE, 
	PREMIUMMASTERPREMIUMTYPE, 
	PREMIUMMASTERTYPEBUREAUCODE, 
	PremiumMasterBureauStatisticalLineout AS PREMIUMMASTERBUREAUSTATISTICALLINE, 
	PREMIUMMASTERPRODUCTLINE, 
	o_PremiumMasterAgencyCommissionRate AS PREMIUMMASTERAGENCYCOMMISSIONRATE, 
	PREMIUMMASTERCLASSCODE, 
	PREMIUMMASTEREXPOSURE, 
	PREMIUMMASTERSUBLINE, 
	PREMIUMMASTERSTATISTICALCODE1, 
	PREMIUMMASTERSTATISTICALCODE2, 
	PREMIUMMASTERSTATISTICALCODE3, 
	PREMIUMMASTERRATEMODIFIER, 
	PREMIUMMASTERRATEDEPARTURE, 
	PREMIUMMASTERBUREAUINCEPTIONDATE, 
	PREMIUMMASTERCOUNTERSIGNAGENCYTYPE, 
	PREMIUMMASTERCOUNTERSIGNAGENCYCODE, 
	PREMIUMMASTERCOUNTERSIGNAGENCYSTATE, 
	PREMIUMMASTERCOUNTERSIGNAGENCYRATE, 
	PREMIUMMASTERPREMIUM, 
	PREMIUMMASTERFULLTERMPREMIUM, 
	TAXBOARDPERCENTAGE, 
	PREMIUMMASTERRISKADDRESS, 
	PREMIUMMASTERRISKCITYSTATE, 
	PREMIUMMASTERRENEWALINDICATOR, 
	PREMIUMMASTERRECORDTYPE, 
	RATINGCOVERAGEAKID, 
	RATINGCOVERAGEEFFECTIVEDATE, 
	RATINGCOVERAGEEXPIRATIONDATE, 
	DefaultID AS STAGEPREMIUMMASTERPKID, 
	POLICYKEY, 
	PremiumMasterBookedDateOut AS PREMIUMMASTERRUNDATE, 
	PREMIUMMASTERREASONAMENDEDCODE, 
	PremiumTransactionEnteredDate1 AS PREMIUMTRANSACTIONENTEREDDATE, 
	o_PremiumMasterCustomerCareCommissionRate AS PREMIUMMASTERCUSTOMERCARECOMMISSIONRATE, 
	PREMIUMMASTERWRITTENEXPOSURE, 
	o_DeclaredEventsFlag AS DECLAREDEVENTFLAG
	FROM EXP_Insert_CededTransactions
),
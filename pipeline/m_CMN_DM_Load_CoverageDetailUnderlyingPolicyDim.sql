WITH
SQ_CoverageDetailUnderlyingPolicy AS (
	select CDD.EffectiveDate,
	CDD.ExpirationDate,
	CDD.CoverageDetailDimId, 
	CDD.CoverageGuid, 
	CDUP.UnderlyingInsuranceCompanyName, 
	CDUP.UnderlyingPolicyKey, 
	CDUP.UnderlyingPolicyType,
	UnderlyingPolicyLimit,
	UnderlyingPolicyLimitType
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailUnderlyingPolicy CDUP with (nolock)
	join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD 
	on CDUP.PremiumTransactionId=CDD.EDWPremiumTransactionPKId
	where (CDUP.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}' OR CDD.ModifedDate>='@{pipeline().parameters.SELECTION_START_TS}')
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_GetValues AS (
	SELECT
	EffectiveDate,
	ExpirationDate,
	CoverageDetailDimId,
	CoverageGuid,
	UnderlyingInsuranceCompanyName,
	UnderlyingPolicyKey,
	UnderlyingPolicyType,
	UnderlyingPolicyLimit,
	UnderlyingPolicyLimitType
	FROM SQ_CoverageDetailUnderlyingPolicy
),
AGG_UnderlyingPolicy AS (
	SELECT
	EffectiveDate,
	ExpirationDate,
	CoverageDetailDimId,
	CoverageGuid,
	UnderlyingInsuranceCompanyName,
	UnderlyingPolicyKey,
	UnderlyingPolicyType,
	UnderlyingPolicyLimit AS i_UnderlyingPolicyLimit,
	UnderlyingPolicyLimitType AS i_UnderlyingPolicyLimitType,
	-- *INF*: DECODE(UnderlyingPolicyType,
	-- 'GeneralLiability',
	-- 'Underlying-GeneralLiability EACH OCCURRENCE LIMIT',
	-- 'EmployersLiability',
	-- 'Underlying-EmployersLiability BODILY INJURY BY ACCIDENT:  EACH ACCIDENT',
	-- 'CommercialAutoLiability',
	-- 'Underlying-CommercialAutoLiability PROPERTY DAMAGE - EACH ACCIDENT',
	-- 'GarageLiability',
	-- 'Underlying-GarageLiability EACH ACCIDENT - GARAGE OPERATIONS AUTO ONLY',
	-- 'BusinessownersLiability',
	-- 'Underlying-BusinessownersLiability EACH OCCURRENCE LIMIT',
	-- 'LiquorLiability',
	-- 'Underlying-LiquorLiability EACH OCCURRENCE LIMIT',
	-- 'SMARTbusinessLiability',
	-- 'Underlying-SMARTbusinessLiability EACH OCCURRENCE LIMIT',
	-- 'SBOPGeneralLiability',
	-- 'Underlying-SBOPGeneralLiability EACH OCCURRENCE LIMIT',
	-- 'CBOPGeneralLiability',
	-- 'Underlying-CBOPGeneralLiability EACH OCCURRENCE LIMIT',
	-- 'CPPProfessionalLiability',
	-- 'Underlying-CPPProfessionalLiability EACH OCCURRENCE LIMIT',
	-- 'SBOPProfessionalLiability',
	-- 'Underlying-SBOPProfessionalLiability EACH OCCURRENCE LIMIT',
	-- 'SMARTProfessionalLiability',
	-- 'Underlying-SMARTProfessionalLiability EACH OCCURRENCE LIMIT',
	-- 'GLOhioStopGapEmployersLiability',
	-- 'Underlying-GLOhioStopGapEmployersLiability-BodilyInjuryByAccident-EachAccident',
	-- 'AutoOhioStopGapEmployersLiability',
	-- 'Underlying-AutoOhioStopGapEmployersLiability-BodilyInjuryByAccident-EachAccident',
	-- 'SMARTOhioStopGapEmployersLiability',
	-- 'Underlying-SMARTOhioStopGapEmployersLiability-BodilyInjuryByAccident-EachAccident',
	-- 'SBOPOhioStopGapEmployersLiability',
	-- 'Underlying-SBOPOhioStopGapEmployersLiability BODILY INJURY BY ACCIDENT:  EACH ACCIDENT',
	-- 'SMARTEmployeeBenefitsLiability', 'Underlying-SMARTEmployeeBenefitsLiabilityEachEmployee',
	-- 'CommercialAutoEmployeeBenefitsLiability','Underlying-CommAutoEmployeeBenefitsLiabilityEachEmployee',
	-- 'BOPEmployeeBenefitsLiability','Underlying-BOPEmployeeBenefitsLiabilityEachEmployee',
	-- 'SBOPEmployeeBenefitsLiability','Underlying-SBOPEmployeeBenefitsLiabilityEachEmployee',
	-- 'CPPEmployeeBenefitsLiability','Underlying-CPPEmployeeBenefitsLiabilityEachEmployee',
	-- 'N/A'
	-- )
	DECODE(
	    UnderlyingPolicyType,
	    'GeneralLiability', 'Underlying-GeneralLiability EACH OCCURRENCE LIMIT',
	    'EmployersLiability', 'Underlying-EmployersLiability BODILY INJURY BY ACCIDENT:  EACH ACCIDENT',
	    'CommercialAutoLiability', 'Underlying-CommercialAutoLiability PROPERTY DAMAGE - EACH ACCIDENT',
	    'GarageLiability', 'Underlying-GarageLiability EACH ACCIDENT - GARAGE OPERATIONS AUTO ONLY',
	    'BusinessownersLiability', 'Underlying-BusinessownersLiability EACH OCCURRENCE LIMIT',
	    'LiquorLiability', 'Underlying-LiquorLiability EACH OCCURRENCE LIMIT',
	    'SMARTbusinessLiability', 'Underlying-SMARTbusinessLiability EACH OCCURRENCE LIMIT',
	    'SBOPGeneralLiability', 'Underlying-SBOPGeneralLiability EACH OCCURRENCE LIMIT',
	    'CBOPGeneralLiability', 'Underlying-CBOPGeneralLiability EACH OCCURRENCE LIMIT',
	    'CPPProfessionalLiability', 'Underlying-CPPProfessionalLiability EACH OCCURRENCE LIMIT',
	    'SBOPProfessionalLiability', 'Underlying-SBOPProfessionalLiability EACH OCCURRENCE LIMIT',
	    'SMARTProfessionalLiability', 'Underlying-SMARTProfessionalLiability EACH OCCURRENCE LIMIT',
	    'GLOhioStopGapEmployersLiability', 'Underlying-GLOhioStopGapEmployersLiability-BodilyInjuryByAccident-EachAccident',
	    'AutoOhioStopGapEmployersLiability', 'Underlying-AutoOhioStopGapEmployersLiability-BodilyInjuryByAccident-EachAccident',
	    'SMARTOhioStopGapEmployersLiability', 'Underlying-SMARTOhioStopGapEmployersLiability-BodilyInjuryByAccident-EachAccident',
	    'SBOPOhioStopGapEmployersLiability', 'Underlying-SBOPOhioStopGapEmployersLiability BODILY INJURY BY ACCIDENT:  EACH ACCIDENT',
	    'SMARTEmployeeBenefitsLiability', 'Underlying-SMARTEmployeeBenefitsLiabilityEachEmployee',
	    'CommercialAutoEmployeeBenefitsLiability', 'Underlying-CommAutoEmployeeBenefitsLiabilityEachEmployee',
	    'BOPEmployeeBenefitsLiability', 'Underlying-BOPEmployeeBenefitsLiabilityEachEmployee',
	    'SBOPEmployeeBenefitsLiability', 'Underlying-SBOPEmployeeBenefitsLiabilityEachEmployee',
	    'CPPEmployeeBenefitsLiability', 'Underlying-CPPEmployeeBenefitsLiabilityEachEmployee',
	    'N/A'
	) AS v_UnderlyingPolicyLimitType1,
	-- *INF*: DECODE(UnderlyingPolicyType,
	-- 'GeneralLiability',
	-- 'Underlying-GeneralLiability AGGREGATE LIMIT',
	-- 'EmployersLiability',
	-- 'Underlying-EmployersLiability BODILY INJURY BY DISEASE:   EACH EMPLOYEE',
	-- 'GarageLiability',
	-- 'Underlying-GarageLiability EACH ACCIDENT - GARAGE OPERATIONS OTHER THAN AUTO ONLY',
	-- 'BusinessownersLiability',
	-- 'Underlying-BusinessownersLiability AGGREGATE LIMIT',
	-- 'LiquorLiability',
	-- 'Underlying-LiquorLiability AGGREGATE LIMIT',
	-- 'SMARTbusinessLiability',
	-- 'Underlying-SMARTbusinessLiability AGGREGATE LIMIT',
	-- 'SBOPGeneralLiability',
	-- 'Underlying-SBOPGeneralLiability AGGREGATE LIMIT',
	-- 'CBOPGeneralLiability',
	-- 'Underlying-CBOPGeneralLiability AGGREGATE LIMIT',
	-- 'CPPProfessionalLiability',
	-- 'Underlying-CPPProfessionalLiability AGGREGATE LIMIT',
	-- 'SBOPProfessionalLiability',
	-- 'Underlying-SBOPProfessionalLiability AGGREGATE LIMIT',
	-- 'SMARTProfessionalLiability',
	-- 'Underlying-SMARTProfessionalLiability AGGREGATE LIMIT',
	-- 'GLOhioStopGapEmployersLiability',
	-- 'Underlying-GLOhioStopGapEmployersLiability-BodilyInjuryByDisease-EachEmployee',
	-- 'AutoOhioStopGapEmployersLiability',
	-- 'Underlying-AutoOhioStopGapEmployersLiability-BodilyInjuryByDisease-EachEmployee',
	-- 'SMARTOhioStopGapEmployersLiability',
	-- 'Underlying-SMARTOhioStopGapEmployersLiability-BodilyInjuryByDisease-EachEmployee',
	-- 'SBOPOhioStopGapEmployersLiability',
	-- 'Underlying-SBOPOhioStopGapEmployersLiability BODILY INJURY BY DISEASE:   EACH EMPLOYEE',
	-- 'SMARTEmployeeBenefitsLiability', 'Underlying-SMARTEmployeeBenefitsLiabilityAggregate',
	-- 'CommercialAutoEmployeeBenefitsLiability','Underlying-CommAutoEmployeeBenefitsLiabilityAggregate',
	-- 'BOPEmployeeBenefitsLiability','Underlying-BOPEmployeeBenefitsLiabilityAggregrate',
	-- 'SBOPEmployeeBenefitsLiability','Underlying-SBOPEmployeeBenefitsLiabilityAggregrate',
	-- 'CPPEmployeeBenefitsLiability','Underlying-CPPEmployeeBenefitsLiabilityAggregrate',
	-- 'N/A'
	-- )
	DECODE(
	    UnderlyingPolicyType,
	    'GeneralLiability', 'Underlying-GeneralLiability AGGREGATE LIMIT',
	    'EmployersLiability', 'Underlying-EmployersLiability BODILY INJURY BY DISEASE:   EACH EMPLOYEE',
	    'GarageLiability', 'Underlying-GarageLiability EACH ACCIDENT - GARAGE OPERATIONS OTHER THAN AUTO ONLY',
	    'BusinessownersLiability', 'Underlying-BusinessownersLiability AGGREGATE LIMIT',
	    'LiquorLiability', 'Underlying-LiquorLiability AGGREGATE LIMIT',
	    'SMARTbusinessLiability', 'Underlying-SMARTbusinessLiability AGGREGATE LIMIT',
	    'SBOPGeneralLiability', 'Underlying-SBOPGeneralLiability AGGREGATE LIMIT',
	    'CBOPGeneralLiability', 'Underlying-CBOPGeneralLiability AGGREGATE LIMIT',
	    'CPPProfessionalLiability', 'Underlying-CPPProfessionalLiability AGGREGATE LIMIT',
	    'SBOPProfessionalLiability', 'Underlying-SBOPProfessionalLiability AGGREGATE LIMIT',
	    'SMARTProfessionalLiability', 'Underlying-SMARTProfessionalLiability AGGREGATE LIMIT',
	    'GLOhioStopGapEmployersLiability', 'Underlying-GLOhioStopGapEmployersLiability-BodilyInjuryByDisease-EachEmployee',
	    'AutoOhioStopGapEmployersLiability', 'Underlying-AutoOhioStopGapEmployersLiability-BodilyInjuryByDisease-EachEmployee',
	    'SMARTOhioStopGapEmployersLiability', 'Underlying-SMARTOhioStopGapEmployersLiability-BodilyInjuryByDisease-EachEmployee',
	    'SBOPOhioStopGapEmployersLiability', 'Underlying-SBOPOhioStopGapEmployersLiability BODILY INJURY BY DISEASE:   EACH EMPLOYEE',
	    'SMARTEmployeeBenefitsLiability', 'Underlying-SMARTEmployeeBenefitsLiabilityAggregate',
	    'CommercialAutoEmployeeBenefitsLiability', 'Underlying-CommAutoEmployeeBenefitsLiabilityAggregate',
	    'BOPEmployeeBenefitsLiability', 'Underlying-BOPEmployeeBenefitsLiabilityAggregrate',
	    'SBOPEmployeeBenefitsLiability', 'Underlying-SBOPEmployeeBenefitsLiabilityAggregrate',
	    'CPPEmployeeBenefitsLiability', 'Underlying-CPPEmployeeBenefitsLiabilityAggregrate',
	    'N/A'
	) AS v_UnderlyingPolicyLimitType2,
	-- *INF*: DECODE(UnderlyingPolicyType,
	-- 'GeneralLiability',
	-- 'Underlying-GeneralLiability PRODUCTS-COMPLETED OPERATIONS AGGREGATE LIMIT',
	-- 'EmployersLiability',
	-- 'Underlying-EmployersLiability BODILY INJURY BY DISEASE:   POLICY LIMIT',
	-- 'GarageLiability',
	-- 'Underlying-GarageLiability AGGREGATE - GARAGE OPERATIONS OTHER THAN AUTO ONLY',
	-- 'BusinessownersLiability',
	-- 'Underlying-BusinessownersLiability PRODUCTS-COMPLETED OPERATIONS AGGREGATE LIMIT',
	-- 'LiquorLiability',
	-- 'Underlying-LiquorLiability PRODUCTS-COMPLETED OPERATIONS AGGREGATE LIMIT',
	-- 'SMARTbusinessLiability',
	-- 'Underlying-SMARTbusinessLiability PRODUCTS-COMPLETED OPERATIONS AGGREGATE LIMIT',
	-- 'SBOPGeneralLiability',
	-- 'Underlying-SBOPGeneralLiability PRODUCTS-COMPLETED OPERATIONS AGGREGATE LIMIT',
	-- 'CBOPGeneralLiability',
	-- 'Underlying-CBOPGeneralLiability PRODUCTS-COMPLETED OPERATIONS AGGREGATE LIMIT',
	-- 'GLOhioStopGapEmployersLiability',
	-- 'Underlying-GLOhioStopGapEmployersLiability-BodilyInjuryByDisease-Aggregate',
	-- 'AutoOhioStopGapEmployersLiability',
	-- 'Underlying-AutoOhioStopGapEmployersLiability-BodilyInjuryByDisease-Aggregate',
	-- 'SMARTOhioStopGapEmployersLiability',
	-- 'Underlying-SMARTOhioStopGapEmployersLiability-BodilyInjuryByDisease-Aggregate',
	-- 'SBOPOhioStopGapEmployersLiability',
	-- 'Underlying-SBOPOhioStopGapEmployersLiability BODILY INJURY BY DISEASE:   POLICY LIMIT',
	-- 'N/A'
	-- )
	DECODE(
	    UnderlyingPolicyType,
	    'GeneralLiability', 'Underlying-GeneralLiability PRODUCTS-COMPLETED OPERATIONS AGGREGATE LIMIT',
	    'EmployersLiability', 'Underlying-EmployersLiability BODILY INJURY BY DISEASE:   POLICY LIMIT',
	    'GarageLiability', 'Underlying-GarageLiability AGGREGATE - GARAGE OPERATIONS OTHER THAN AUTO ONLY',
	    'BusinessownersLiability', 'Underlying-BusinessownersLiability PRODUCTS-COMPLETED OPERATIONS AGGREGATE LIMIT',
	    'LiquorLiability', 'Underlying-LiquorLiability PRODUCTS-COMPLETED OPERATIONS AGGREGATE LIMIT',
	    'SMARTbusinessLiability', 'Underlying-SMARTbusinessLiability PRODUCTS-COMPLETED OPERATIONS AGGREGATE LIMIT',
	    'SBOPGeneralLiability', 'Underlying-SBOPGeneralLiability PRODUCTS-COMPLETED OPERATIONS AGGREGATE LIMIT',
	    'CBOPGeneralLiability', 'Underlying-CBOPGeneralLiability PRODUCTS-COMPLETED OPERATIONS AGGREGATE LIMIT',
	    'GLOhioStopGapEmployersLiability', 'Underlying-GLOhioStopGapEmployersLiability-BodilyInjuryByDisease-Aggregate',
	    'AutoOhioStopGapEmployersLiability', 'Underlying-AutoOhioStopGapEmployersLiability-BodilyInjuryByDisease-Aggregate',
	    'SMARTOhioStopGapEmployersLiability', 'Underlying-SMARTOhioStopGapEmployersLiability-BodilyInjuryByDisease-Aggregate',
	    'SBOPOhioStopGapEmployersLiability', 'Underlying-SBOPOhioStopGapEmployersLiability BODILY INJURY BY DISEASE:   POLICY LIMIT',
	    'N/A'
	) AS v_UnderlyingPolicyLimitType3,
	-- *INF*: DECODE(UnderlyingPolicyType,
	-- 'GeneralLiability',
	-- 'Underlying-GeneralLiability PERSONAL INJURY AND ADVERTISING INJURY LIMIT',
	-- 'BusinessownersLiability',
	-- 'Underlying-BusinessownersLiability PERSONAL INJURY AND ADVERTISING INJURY LIMIT',
	-- 'LiquorLiability',
	-- 'Underlying-LiquorLiability EACH COMMON CAUSE LIMIT',
	-- 'SMARTbusinessLiability',
	-- 'Underlying-SMARTbusinessLiability PERSONAL INJURY AND ADVERTISING INJURY LIMIT',
	-- 'SBOPGeneralLiability',
	-- 'Underlying-SBOPGeneralLiability PERSONAL INJURY AND ADVERTISING INJURY LIMIT',
	-- 'CBOPGeneralLiability',
	-- 'Underlying-CBOPGeneralLiability PERSONAL INJURY AND ADVERTISING INJURY LIMIT',
	-- 'N/A'
	-- )
	DECODE(
	    UnderlyingPolicyType,
	    'GeneralLiability', 'Underlying-GeneralLiability PERSONAL INJURY AND ADVERTISING INJURY LIMIT',
	    'BusinessownersLiability', 'Underlying-BusinessownersLiability PERSONAL INJURY AND ADVERTISING INJURY LIMIT',
	    'LiquorLiability', 'Underlying-LiquorLiability EACH COMMON CAUSE LIMIT',
	    'SMARTbusinessLiability', 'Underlying-SMARTbusinessLiability PERSONAL INJURY AND ADVERTISING INJURY LIMIT',
	    'SBOPGeneralLiability', 'Underlying-SBOPGeneralLiability PERSONAL INJURY AND ADVERTISING INJURY LIMIT',
	    'CBOPGeneralLiability', 'Underlying-CBOPGeneralLiability PERSONAL INJURY AND ADVERTISING INJURY LIMIT',
	    'N/A'
	) AS v_UnderlyingPolicyLimitType4,
	v_UnderlyingPolicyLimitType1 AS o_UnderlyingPolicyLimitType1,
	v_UnderlyingPolicyLimitType2 AS o_UnderlyingPolicyLimitType2,
	v_UnderlyingPolicyLimitType3 AS o_UnderlyingPolicyLimitType3,
	v_UnderlyingPolicyLimitType4 AS o_UnderlyingPolicyLimitType4,
	-- *INF*: FIRST(i_UnderlyingPolicyLimit, i_UnderlyingPolicyLimitType = v_UnderlyingPolicyLimitType1)
	FIRST(i_UnderlyingPolicyLimit, i_UnderlyingPolicyLimitType = v_UnderlyingPolicyLimitType1) AS o_UnderlyingPolicyLimit1,
	-- *INF*: FIRST(i_UnderlyingPolicyLimit, i_UnderlyingPolicyLimitType = v_UnderlyingPolicyLimitType2)
	FIRST(i_UnderlyingPolicyLimit, i_UnderlyingPolicyLimitType = v_UnderlyingPolicyLimitType2) AS o_UnderlyingPolicyLimit2,
	-- *INF*: FIRST(i_UnderlyingPolicyLimit, i_UnderlyingPolicyLimitType = v_UnderlyingPolicyLimitType3)
	FIRST(i_UnderlyingPolicyLimit, i_UnderlyingPolicyLimitType = v_UnderlyingPolicyLimitType3) AS o_UnderlyingPolicyLimit3,
	-- *INF*: FIRST(i_UnderlyingPolicyLimit, i_UnderlyingPolicyLimitType = v_UnderlyingPolicyLimitType4)
	FIRST(i_UnderlyingPolicyLimit, i_UnderlyingPolicyLimitType = v_UnderlyingPolicyLimitType4) AS o_UnderlyingPolicyLimit4
	FROM EXP_GetValues
	GROUP BY CoverageDetailDimId, UnderlyingInsuranceCompanyName, UnderlyingPolicyKey, UnderlyingPolicyType
),
EXP_Metadata AS (
	SELECT
	o_UnderlyingPolicyLimit1 AS i_UnderlyingPolicyLimit1,
	o_UnderlyingPolicyLimit2 AS i_UnderlyingPolicyLimit2,
	o_UnderlyingPolicyLimit3 AS i_UnderlyingPolicyLimit3,
	o_UnderlyingPolicyLimit4 AS i_UnderlyingPolicyLimit4,
	EffectiveDate,
	ExpirationDate,
	CoverageDetailDimId,
	CoverageGuid,
	UnderlyingInsuranceCompanyName,
	UnderlyingPolicyKey,
	UnderlyingPolicyType,
	o_UnderlyingPolicyLimitType1 AS UnderlyingPolicyLimitType1,
	o_UnderlyingPolicyLimitType2 AS UnderlyingPolicyLimitType2,
	o_UnderlyingPolicyLimitType3 AS UnderlyingPolicyLimitType3,
	o_UnderlyingPolicyLimitType4 AS UnderlyingPolicyLimitType4,
	-- *INF*: IIF(ISNULL(i_UnderlyingPolicyLimit1), 'N/A', i_UnderlyingPolicyLimit1)
	IFF(i_UnderlyingPolicyLimit1 IS NULL, 'N/A', i_UnderlyingPolicyLimit1) AS o_UnderlyingPolicyLimit1,
	-- *INF*: IIF(ISNULL(i_UnderlyingPolicyLimit2), 'N/A', i_UnderlyingPolicyLimit2)
	IFF(i_UnderlyingPolicyLimit2 IS NULL, 'N/A', i_UnderlyingPolicyLimit2) AS o_UnderlyingPolicyLimit2,
	-- *INF*: IIF(ISNULL(i_UnderlyingPolicyLimit3), 'N/A', i_UnderlyingPolicyLimit3)
	IFF(i_UnderlyingPolicyLimit3 IS NULL, 'N/A', i_UnderlyingPolicyLimit3) AS o_UnderlyingPolicyLimit3,
	-- *INF*: IIF(ISNULL(i_UnderlyingPolicyLimit4), 'N/A', i_UnderlyingPolicyLimit4)
	IFF(i_UnderlyingPolicyLimit4 IS NULL, 'N/A', i_UnderlyingPolicyLimit4) AS o_UnderlyingPolicyLimit4,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	CURRENT_TIMESTAMP AS o_CreateDate,
	CURRENT_TIMESTAMP AS o_ModifiedDate
	FROM AGG_UnderlyingPolicy
),
LKP_CoverageDetailUnderlyingPolicyDim AS (
	SELECT
	CoverageDetailUnderlyingPolicyDimId,
	EffectiveDate,
	ExpirationDate,
	UnderlyingPolicyLimit1,
	UnderlyingPolicyLimitType1,
	UnderlyingPolicyLimit2,
	UnderlyingPolicyLimitType2,
	UnderlyingPolicyLimit3,
	UnderlyingPolicyLimitType3,
	UnderlyingPolicyLimit4,
	UnderlyingPolicyLimitType4,
	CoverageDetailDimId,
	UnderlyingInsuranceCompanyName,
	UnderlyingPolicyKey,
	UnderlyingPolicyType
	FROM (
		SELECT CDUPD.CoverageDetailUnderlyingPolicyDimId as CoverageDetailUnderlyingPolicyDimId, 
		CDUPD.EffectiveDate as EffectiveDate,
		CDUPD.ExpirationDate as ExpirationDate,
		CDUPD.UnderlyingPolicyLimit1 as UnderlyingPolicyLimit1, 
		CDUPD.UnderlyingPolicyLimitType1 as UnderlyingPolicyLimitType1, 
		CDUPD.UnderlyingPolicyLimit2 as UnderlyingPolicyLimit2, 
		CDUPD.UnderlyingPolicyLimitType2 as UnderlyingPolicyLimitType2, 
		CDUPD.UnderlyingPolicyLimit3 as UnderlyingPolicyLimit3, 
		CDUPD.UnderlyingPolicyLimitType3 as UnderlyingPolicyLimitType3, 
		CDUPD.UnderlyingPolicyLimit4 as UnderlyingPolicyLimit4, 
		CDUPD.UnderlyingPolicyLimitType4 as UnderlyingPolicyLimitType4, 
		CDUPD.CoverageDetailDimId as CoverageDetailDimId, 
		CDUPD.UnderlyingInsuranceCompanyName as UnderlyingInsuranceCompanyName, 
		CDUPD.UnderlyingPolicyKey as UnderlyingPolicyKey, 
		CDUPD.UnderlyingPolicyType as UnderlyingPolicyType 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailUnderlyingPolicyDim CDUPD
		INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD on CDD.coveragedetaildimid = CDUPD.CoverageDetailDimId
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailUnderlyingPolicy CDUP
		on CDUP.PremiumTransactionId=CDD.EDWPremiumTransactionPKId
		WHERE (CDUP.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}' OR CDD.ModifedDate>='@{pipeline().parameters.SELECTION_START_TS}')
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageDetailDimId,UnderlyingInsuranceCompanyName,UnderlyingPolicyKey,UnderlyingPolicyType ORDER BY CoverageDetailUnderlyingPolicyDimId) = 1
),
EXP_Update AS (
	SELECT
	LKP_CoverageDetailUnderlyingPolicyDim.CoverageDetailUnderlyingPolicyDimId AS lkp_CoverageDetailUnderlyingPolicyDimId,
	LKP_CoverageDetailUnderlyingPolicyDim.EffectiveDate AS lkp_EffectiveDate,
	LKP_CoverageDetailUnderlyingPolicyDim.ExpirationDate AS lkp_ExpirationDate,
	LKP_CoverageDetailUnderlyingPolicyDim.UnderlyingPolicyLimit1 AS lkp_UnderlyingPolicyLimit1,
	LKP_CoverageDetailUnderlyingPolicyDim.UnderlyingPolicyLimitType1 AS lkp_UnderlyingPolicyLimitType1,
	LKP_CoverageDetailUnderlyingPolicyDim.UnderlyingPolicyLimit2 AS lkp_UnderlyingPolicyLimit2,
	LKP_CoverageDetailUnderlyingPolicyDim.UnderlyingPolicyLimitType2 AS lkp_UnderlyingPolicyLimitType2,
	LKP_CoverageDetailUnderlyingPolicyDim.UnderlyingPolicyLimit3 AS lkp_UnderlyingPolicyLimit3,
	LKP_CoverageDetailUnderlyingPolicyDim.UnderlyingPolicyLimitType3 AS lkp_UnderlyingPolicyLimitType3,
	LKP_CoverageDetailUnderlyingPolicyDim.UnderlyingPolicyLimit4 AS lkp_UnderlyingPolicyLimit4,
	LKP_CoverageDetailUnderlyingPolicyDim.UnderlyingPolicyLimitType4 AS lkp_UnderlyingPolicyLimitType4,
	1 AS o_CurrentSnapshotFlag,
	EXP_Metadata.o_AuditId AS AuditId,
	EXP_Metadata.EffectiveDate,
	EXP_Metadata.ExpirationDate,
	EXP_Metadata.o_CreateDate AS CreateDate,
	EXP_Metadata.o_ModifiedDate AS ModifiedDate,
	EXP_Metadata.CoverageDetailDimId,
	EXP_Metadata.CoverageGuid,
	EXP_Metadata.UnderlyingInsuranceCompanyName,
	EXP_Metadata.UnderlyingPolicyKey,
	EXP_Metadata.UnderlyingPolicyType,
	EXP_Metadata.o_UnderlyingPolicyLimit1 AS UnderlyingPolicyLimit1,
	EXP_Metadata.UnderlyingPolicyLimitType1,
	EXP_Metadata.o_UnderlyingPolicyLimit2 AS UnderlyingPolicyLimit2,
	EXP_Metadata.UnderlyingPolicyLimitType2,
	EXP_Metadata.o_UnderlyingPolicyLimit3 AS UnderlyingPolicyLimit3,
	EXP_Metadata.UnderlyingPolicyLimitType3,
	EXP_Metadata.o_UnderlyingPolicyLimit4 AS UnderlyingPolicyLimit4,
	EXP_Metadata.UnderlyingPolicyLimitType4,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_CoverageDetailUnderlyingPolicyDimId), 'NEW',
	-- lkp_EffectiveDate != EffectiveDate OR lkp_ExpirationDate != ExpirationDate OR 
	-- lkp_UnderlyingPolicyLimitType1 != UnderlyingPolicyLimitType1 OR 
	-- lkp_UnderlyingPolicyLimit1 != UnderlyingPolicyLimit1 OR lkp_UnderlyingPolicyLimitType2 != UnderlyingPolicyLimitType2 OR 
	-- lkp_UnderlyingPolicyLimit2 != UnderlyingPolicyLimit2 OR 
	-- lkp_UnderlyingPolicyLimitType3 != UnderlyingPolicyLimitType3 OR 
	-- lkp_UnderlyingPolicyLimit3 != UnderlyingPolicyLimit3 OR 
	-- lkp_UnderlyingPolicyLimitType4 != UnderlyingPolicyLimitType4 OR 
	-- lkp_UnderlyingPolicyLimit4 != UnderlyingPolicyLimit4,
	-- 'UPDATE',
	-- 'NOCHANGE')
	DECODE(
	    TRUE,
	    lkp_CoverageDetailUnderlyingPolicyDimId IS NULL, 'NEW',
	    lkp_EffectiveDate != EffectiveDate OR lkp_ExpirationDate != ExpirationDate OR lkp_UnderlyingPolicyLimitType1 != UnderlyingPolicyLimitType1 OR lkp_UnderlyingPolicyLimit1 != UnderlyingPolicyLimit1 OR lkp_UnderlyingPolicyLimitType2 != UnderlyingPolicyLimitType2 OR lkp_UnderlyingPolicyLimit2 != UnderlyingPolicyLimit2 OR lkp_UnderlyingPolicyLimitType3 != UnderlyingPolicyLimitType3 OR lkp_UnderlyingPolicyLimit3 != UnderlyingPolicyLimit3 OR lkp_UnderlyingPolicyLimitType4 != UnderlyingPolicyLimitType4 OR lkp_UnderlyingPolicyLimit4 != UnderlyingPolicyLimit4, 'UPDATE',
	    'NOCHANGE'
	) AS o_ChangeFlag
	FROM EXP_Metadata
	LEFT JOIN LKP_CoverageDetailUnderlyingPolicyDim
	ON LKP_CoverageDetailUnderlyingPolicyDim.CoverageDetailDimId = EXP_Metadata.CoverageDetailDimId AND LKP_CoverageDetailUnderlyingPolicyDim.UnderlyingInsuranceCompanyName = EXP_Metadata.UnderlyingInsuranceCompanyName AND LKP_CoverageDetailUnderlyingPolicyDim.UnderlyingPolicyKey = EXP_Metadata.UnderlyingPolicyKey AND LKP_CoverageDetailUnderlyingPolicyDim.UnderlyingPolicyType = EXP_Metadata.UnderlyingPolicyType
),
RTR_InsertUpdate AS (
	SELECT
	lkp_CoverageDetailUnderlyingPolicyDimId,
	o_CurrentSnapshotFlag,
	AuditId,
	EffectiveDate,
	ExpirationDate,
	CreateDate,
	ModifiedDate,
	CoverageDetailDimId,
	CoverageGuid,
	UnderlyingInsuranceCompanyName,
	UnderlyingPolicyKey,
	UnderlyingPolicyType,
	UnderlyingPolicyLimit1,
	UnderlyingPolicyLimitType1,
	UnderlyingPolicyLimit2,
	UnderlyingPolicyLimitType2,
	UnderlyingPolicyLimit3,
	UnderlyingPolicyLimitType3,
	UnderlyingPolicyLimit4,
	UnderlyingPolicyLimitType4,
	o_ChangeFlag AS ChangeFlag
	FROM EXP_Update
),
RTR_InsertUpdate_Insert AS (SELECT * FROM RTR_InsertUpdate WHERE ChangeFlag='NEW'),
RTR_InsertUpdate_Update AS (SELECT * FROM RTR_InsertUpdate WHERE ChangeFlag='UPDATE'),
UPD_Update AS (
	SELECT
	lkp_CoverageDetailUnderlyingPolicyDimId, 
	AuditId, 
	EffectiveDate, 
	ExpirationDate, 
	ModifiedDate, 
	UnderlyingPolicyLimit1, 
	UnderlyingPolicyLimitType1, 
	UnderlyingPolicyLimit2, 
	UnderlyingPolicyLimitType2, 
	UnderlyingPolicyLimit AS UnderlyingPolicyLimit3, 
	UnderlyingPolicyLimitType AS UnderlyingPolicyLimitType3, 
	UnderlyingPolicyLimit4, 
	UnderlyingPolicyLimitType4
	FROM RTR_InsertUpdate_Update
),
TGT_CoverageDetailUnderlyingPolicyDim_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailUnderlyingPolicyDim AS T
	USING UPD_Update AS S
	ON T.CoverageDetailUnderlyingPolicyDimId = S.lkp_CoverageDetailUnderlyingPolicyDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditID = S.AuditId, T.EffectiveDate = S.EffectiveDate, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate, T.UnderlyingPolicyLimit1 = S.UnderlyingPolicyLimit1, T.UnderlyingPolicyLimitType1 = S.UnderlyingPolicyLimitType1, T.UnderlyingPolicyLimit2 = S.UnderlyingPolicyLimit2, T.UnderlyingPolicyLimitType2 = S.UnderlyingPolicyLimitType2, T.UnderlyingPolicyLimit3 = S.UnderlyingPolicyLimit3, T.UnderlyingPolicyLimitType3 = S.UnderlyingPolicyLimitType3, T.UnderlyingPolicyLimit4 = S.UnderlyingPolicyLimit4, T.UnderlyingPolicyLimitType4 = S.UnderlyingPolicyLimitType4
),
TGT_CoverageDetailUnderlyingPolicyDim_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailUnderlyingPolicyDim
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, CoverageDetailDimId, UnderlyingInsuranceCompanyName, UnderlyingPolicyKey, UnderlyingPolicyType, UnderlyingPolicyLimit1, UnderlyingPolicyLimitType1, UnderlyingPolicyLimit2, UnderlyingPolicyLimitType2, UnderlyingPolicyLimit3, UnderlyingPolicyLimitType3, UnderlyingPolicyLimit4, UnderlyingPolicyLimitType4)
	SELECT 
	o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	AuditId AS AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CreateDate AS CREATEDDATE, 
	MODIFIEDDATE, 
	COVERAGEDETAILDIMID, 
	UNDERLYINGINSURANCECOMPANYNAME, 
	UNDERLYINGPOLICYKEY, 
	UNDERLYINGPOLICYTYPE, 
	UnderlyingPolicyLimit AS UNDERLYINGPOLICYLIMIT1, 
	UnderlyingPolicyLimitType AS UNDERLYINGPOLICYLIMITTYPE1, 
	UNDERLYINGPOLICYLIMIT2, 
	UNDERLYINGPOLICYLIMITTYPE2, 
	UNDERLYINGPOLICYLIMIT3, 
	UNDERLYINGPOLICYLIMITTYPE3, 
	UNDERLYINGPOLICYLIMIT4, 
	UNDERLYINGPOLICYLIMITTYPE4
	FROM RTR_InsertUpdate_Insert
),
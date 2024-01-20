WITH
SQ_DCWCStateDefaultStaging AS (
	SELECT
		WC_StateDefaultId,
		SessionId,
		WC_StateId,
		AdmiraltyIncreasedLiabilityCombinedLimitDefault,
		AdmiraltyIncreasedLiabilityLowerLimitDefault,
		AdmiraltyIncreasedLiabilityUpperLimitDefault,
		AggregateLimitsDefault,
		AlcoholOrDrugFreeWorkplaceCoalMineDefault,
		AlcoholOrDrugFreeWorkplaceDefault,
		AlternatePreferredPlanClaimsInfoDefault,
		AlternatePreferredPlanEmployerTypeDefault,
		ARAPFactorDefault,
		AssignedRiskSurchargeFactorDefault,
		AtomicRadiationFactorDefault,
		AtomicRadiationUnitsOfExposureDefault,
		BenefitsDeductibleDefault,
		BenefitsDeductibleIndicatorDefault,
		BlanketWaiverFlatDefault,
		BlanketWaiverSelectionDefault,
		CertifiedRiskManagementProgramOrServiceDefault,
		CertifiedSafetyCommitteeCreditDefault,
		CoinsuranceSelectionDefault,
		CompulsoryWorkplaceSafetyNumberOfYearsNonCompliantDefault,
		ContractorsCreditFactorDefault,
		ContractorsCreditIndicatorDefault,
		DCAdditionalBenefitsIndicatorDefault,
		DeductibleAmountDefault,
		DeductibleSelectionDefault,
		DeductibleTypeDefault,
		DesignatedMedicalProviderProgramIndicatorDefault,
		DrugAndAlcoholPreventionProgramCreditIndicatorDefault,
		DrugFreeWorkplaceFactorDefault,
		EachAccidentLimitDefault,
		EachEmployeeDiseaseLimitDefault,
		EmployersLiabilityCoverageEndorsementIndicatorDefault,
		EmployingPreviouslyInjuredEmployeesFactorDefault,
		ExperienceModEffectiveDateDefault,
		ExperienceModificationFactorDefault,
		ExperienceModModTypeDefault,
		ExperienceModRiskIDDefault,
		ExperienceModTypeDefault,
		ExperienceRatingOptionsDefault,
		FELAIncreasedLiabilityCombinedLimitDefault,
		FELAIncreasedLiabilityLowerLimitDefault,
		FELAIncreasedLiabilityUpperLimitDefault,
		FlatRateAdjustmentFactorDefault,
		ForeignVoluntaryCompensationFlatFeeDefault,
		HealthcareNetworkFactorDefault,
		HealthcareNetworkIndicatorDefault,
		LargeDeductibleCreditDeductibleDefault,
		LargeDeductibleFactorDefault,
		LossCostTypeDefault,
		ManagedCareFactorDefault,
		MeritRatingALSelectionsDefault,
		MeritRatingARSelectionsDefault,
		MeritRatingDESelectionsDefault,
		MeritRatingFLSelectionsDefault,
		MeritRatingGASelectionsDefault,
		MeritRatingMASelectionsDefault,
		MeritRatingMESelectionsDefault,
		MeritRatingMISelectionsDefault,
		MeritRatingModIndicatorDefault,
		MeritRatingNYSelectionsDefault,
		MeritRatingOKSelectionsDefault,
		MeritRatingORSelectionsDefault,
		MeritRatingPASelectionsDefault,
		MeritRatingSDSelectionsDefault,
		NonRatableIncreasedLimitsFactorDefault,
		NumberOfStatesDefault,
		NumberOfStrikeDutyDaysDefault,
		NumberOfStrikeDutyEmployeesDefault,
		NYPolicyTypeDefault,
		PackageCreditFactorDefault,
		PerAccidentAndAggregateCombinedDeductiblesDefault,
		PerAccidentDeductiblesDefault,
		PolicyLimitDefault,
		RepatriationIndicatorDefault,
		SafetyCertificationProgramDefault,
		SafetyDeviceRateReductionIndicatorDefault,
		SafetyIncentiveProgramDefault,
		SafetyInvestmentCreditFactorDefault,
		ScheduleModificationFactorDefault,
		ScheduleRatingChoiceDefault,
		ScheduleRatingIndicatorDefault,
		SmallDeductibleCreditDeductibleDefault,
		SmallDeductibleSelectionDefault,
		StrikeDutySurchargeDefault,
		StrikeDutySurchargeIndicatorDefault,
		TabularAdjustmentProgramDefault,
		TypeOfEmployerDefault,
		VoluntaryCompensationFlatFeeDefault,
		WaiverOfSubrogationDefault,
		WaiverOfSubrogationFactorDefault,
		WCPRIndicatorDefault,
		WorkplaceSafetyCreditIndicatorDefault,
		WorkplaceSafetyPercentageDefault,
		DefaultFlexibleRatingAdjustmentFactor,
		ExtractDate,
		SourceSystemId
	FROM DCWCStateDefaultStaging
),
EXP_Metadata AS (
	SELECT
	WC_StateDefaultId AS i_WC_StateDefaultId,
	SessionId AS i_SessionId,
	WC_StateId AS i_WC_StateId,
	AdmiraltyIncreasedLiabilityCombinedLimitDefault AS i_AdmiraltyIncreasedLiabilityCombinedLimitDefault,
	AdmiraltyIncreasedLiabilityLowerLimitDefault AS i_AdmiraltyIncreasedLiabilityLowerLimitDefault,
	AdmiraltyIncreasedLiabilityUpperLimitDefault AS i_AdmiraltyIncreasedLiabilityUpperLimitDefault,
	AggregateLimitsDefault AS i_AggregateLimitsDefault,
	AlcoholOrDrugFreeWorkplaceCoalMineDefault AS i_AlcoholOrDrugFreeWorkplaceCoalMineDefault,
	AlcoholOrDrugFreeWorkplaceDefault AS i_AlcoholOrDrugFreeWorkplaceDefault,
	AlternatePreferredPlanClaimsInfoDefault AS i_AlternatePreferredPlanClaimsInfoDefault,
	AlternatePreferredPlanEmployerTypeDefault AS i_AlternatePreferredPlanEmployerTypeDefault,
	ARAPFactorDefault AS i_ARAPFactorDefault,
	AssignedRiskSurchargeFactorDefault AS i_AssignedRiskSurchargeFactorDefault,
	AtomicRadiationFactorDefault AS i_AtomicRadiationFactorDefault,
	AtomicRadiationUnitsOfExposureDefault AS i_AtomicRadiationUnitsOfExposureDefault,
	BenefitsDeductibleDefault AS i_BenefitsDeductibleDefault,
	BenefitsDeductibleIndicatorDefault AS i_BenefitsDeductibleIndicatorDefault,
	BlanketWaiverFlatDefault AS i_BlanketWaiverFlatDefault,
	BlanketWaiverSelectionDefault AS i_BlanketWaiverSelectionDefault,
	CertifiedRiskManagementProgramOrServiceDefault AS i_CertifiedRiskManagementProgramOrServiceDefault,
	CertifiedSafetyCommitteeCreditDefault AS i_CertifiedSafetyCommitteeCreditDefault,
	CoinsuranceSelectionDefault AS i_CoinsuranceSelectionDefault,
	CompulsoryWorkplaceSafetyNumberOfYearsNonCompliantDefault AS i_CompulsoryWorkplaceSafetyNumberOfYearsNonCompliantDefault,
	ContractorsCreditFactorDefault AS i_ContractorsCreditFactorDefault,
	ContractorsCreditIndicatorDefault AS i_ContractorsCreditIndicatorDefault,
	DCAdditionalBenefitsIndicatorDefault AS i_DCAdditionalBenefitsIndicatorDefault,
	DeductibleAmountDefault AS i_DeductibleAmountDefault,
	DeductibleSelectionDefault AS i_DeductibleSelectionDefault,
	DeductibleTypeDefault AS i_DeductibleTypeDefault,
	DesignatedMedicalProviderProgramIndicatorDefault AS i_DesignatedMedicalProviderProgramIndicatorDefault,
	DrugAndAlcoholPreventionProgramCreditIndicatorDefault AS i_DrugAndAlcoholPreventionProgramCreditIndicatorDefault,
	DrugFreeWorkplaceFactorDefault AS i_DrugFreeWorkplaceFactorDefault,
	EachAccidentLimitDefault AS i_EachAccidentLimitDefault,
	EachEmployeeDiseaseLimitDefault AS i_EachEmployeeDiseaseLimitDefault,
	EmployersLiabilityCoverageEndorsementIndicatorDefault AS i_EmployersLiabilityCoverageEndorsementIndicatorDefault,
	EmployingPreviouslyInjuredEmployeesFactorDefault AS i_EmployingPreviouslyInjuredEmployeesFactorDefault,
	ExperienceModEffectiveDateDefault AS i_ExperienceModEffectiveDateDefault,
	ExperienceModificationFactorDefault AS i_ExperienceModificationFactorDefault,
	ExperienceModModTypeDefault AS i_ExperienceModModTypeDefault,
	ExperienceModRiskIDDefault AS i_ExperienceModRiskIDDefault,
	ExperienceModTypeDefault AS i_ExperienceModTypeDefault,
	ExperienceRatingOptionsDefault AS i_ExperienceRatingOptionsDefault,
	FELAIncreasedLiabilityCombinedLimitDefault AS i_FELAIncreasedLiabilityCombinedLimitDefault,
	FELAIncreasedLiabilityLowerLimitDefault AS i_FELAIncreasedLiabilityLowerLimitDefault,
	FELAIncreasedLiabilityUpperLimitDefault AS i_FELAIncreasedLiabilityUpperLimitDefault,
	FlatRateAdjustmentFactorDefault AS i_FlatRateAdjustmentFactorDefault,
	ForeignVoluntaryCompensationFlatFeeDefault AS i_ForeignVoluntaryCompensationFlatFeeDefault,
	HealthcareNetworkFactorDefault AS i_HealthcareNetworkFactorDefault,
	HealthcareNetworkIndicatorDefault AS i_HealthcareNetworkIndicatorDefault,
	LargeDeductibleCreditDeductibleDefault AS i_LargeDeductibleCreditDeductibleDefault,
	LargeDeductibleFactorDefault AS i_LargeDeductibleFactorDefault,
	LossCostTypeDefault AS i_LossCostTypeDefault,
	ManagedCareFactorDefault AS i_ManagedCareFactorDefault,
	MeritRatingALSelectionsDefault AS i_MeritRatingALSelectionsDefault,
	MeritRatingARSelectionsDefault AS i_MeritRatingARSelectionsDefault,
	MeritRatingDESelectionsDefault AS i_MeritRatingDESelectionsDefault,
	MeritRatingFLSelectionsDefault AS i_MeritRatingFLSelectionsDefault,
	MeritRatingGASelectionsDefault AS i_MeritRatingGASelectionsDefault,
	MeritRatingMASelectionsDefault AS i_MeritRatingMASelectionsDefault,
	MeritRatingMESelectionsDefault AS i_MeritRatingMESelectionsDefault,
	MeritRatingMISelectionsDefault AS i_MeritRatingMISelectionsDefault,
	MeritRatingModIndicatorDefault AS i_MeritRatingModIndicatorDefault,
	MeritRatingNYSelectionsDefault AS i_MeritRatingNYSelectionsDefault,
	MeritRatingOKSelectionsDefault AS i_MeritRatingOKSelectionsDefault,
	MeritRatingORSelectionsDefault AS i_MeritRatingORSelectionsDefault,
	MeritRatingPASelectionsDefault AS i_MeritRatingPASelectionsDefault,
	MeritRatingSDSelectionsDefault AS i_MeritRatingSDSelectionsDefault,
	NonRatableIncreasedLimitsFactorDefault AS i_NonRatableIncreasedLimitsFactorDefault,
	NumberOfStatesDefault AS i_NumberOfStatesDefault,
	NumberOfStrikeDutyDaysDefault AS i_NumberOfStrikeDutyDaysDefault,
	NumberOfStrikeDutyEmployeesDefault AS i_NumberOfStrikeDutyEmployeesDefault,
	NYPolicyTypeDefault AS i_NYPolicyTypeDefault,
	PackageCreditFactorDefault AS i_PackageCreditFactorDefault,
	PerAccidentAndAggregateCombinedDeductiblesDefault AS i_PerAccidentAndAggregateCombinedDeductiblesDefault,
	PerAccidentDeductiblesDefault AS i_PerAccidentDeductiblesDefault,
	PolicyLimitDefault AS i_PolicyLimitDefault,
	RepatriationIndicatorDefault AS i_RepatriationIndicatorDefault,
	SafetyCertificationProgramDefault AS i_SafetyCertificationProgramDefault,
	SafetyDeviceRateReductionIndicatorDefault AS i_SafetyDeviceRateReductionIndicatorDefault,
	SafetyIncentiveProgramDefault AS i_SafetyIncentiveProgramDefault,
	SafetyInvestmentCreditFactorDefault AS i_SafetyInvestmentCreditFactorDefault,
	ScheduleModificationFactorDefault AS i_ScheduleModificationFactorDefault,
	ScheduleRatingChoiceDefault AS i_ScheduleRatingChoiceDefault,
	ScheduleRatingIndicatorDefault AS i_ScheduleRatingIndicatorDefault,
	SmallDeductibleCreditDeductibleDefault AS i_SmallDeductibleCreditDeductibleDefault,
	SmallDeductibleSelectionDefault AS i_SmallDeductibleSelectionDefault,
	StrikeDutySurchargeDefault AS i_StrikeDutySurchargeDefault,
	StrikeDutySurchargeIndicatorDefault AS i_StrikeDutySurchargeIndicatorDefault,
	TabularAdjustmentProgramDefault AS i_TabularAdjustmentProgramDefault,
	TypeOfEmployerDefault AS i_TypeOfEmployerDefault,
	VoluntaryCompensationFlatFeeDefault AS i_VoluntaryCompensationFlatFeeDefault,
	WaiverOfSubrogationDefault AS i_WaiverOfSubrogationDefault,
	WaiverOfSubrogationFactorDefault AS i_WaiverOfSubrogationFactorDefault,
	WCPRIndicatorDefault AS i_WCPRIndicatorDefault,
	WorkplaceSafetyCreditIndicatorDefault AS i_WorkplaceSafetyCreditIndicatorDefault,
	WorkplaceSafetyPercentageDefault AS i_WorkplaceSafetyPercentageDefault,
	DefaultFlexibleRatingAdjustmentFactor AS i_DefaultFlexibleRatingAdjustmentFactor,
	ExtractDate AS i_ExtractDate,
	SourceSystemId AS i_SourceSystemId,
	i_WC_StateId AS o_WC_StateId,
	i_WC_StateDefaultId AS o_WC_StateDefaultId,
	i_SessionId AS o_SessionId,
	i_AdmiraltyIncreasedLiabilityCombinedLimitDefault AS o_AdmiraltyIncreasedLiabilityCombinedLimitDefault,
	i_AdmiraltyIncreasedLiabilityLowerLimitDefault AS o_AdmiraltyIncreasedLiabilityLowerLimitDefault,
	i_AdmiraltyIncreasedLiabilityUpperLimitDefault AS o_AdmiraltyIncreasedLiabilityUpperLimitDefault,
	i_AggregateLimitsDefault AS o_AggregateLimitsDefault,
	-- *INF*: DECODE(i_AlcoholOrDrugFreeWorkplaceCoalMineDefault  ,'T',1,'F',0,NULL)
	DECODE(
	    i_AlcoholOrDrugFreeWorkplaceCoalMineDefault,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AlcoholOrDrugFreeWorkplaceCoalMineDefault,
	-- *INF*: DECODE(i_AlcoholOrDrugFreeWorkplaceDefault  ,'T',1,'F',0,NULL)
	DECODE(
	    i_AlcoholOrDrugFreeWorkplaceDefault,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AlcoholOrDrugFreeWorkplaceDefault,
	i_AlternatePreferredPlanClaimsInfoDefault AS o_AlternatePreferredPlanClaimsInfoDefault,
	i_AlternatePreferredPlanEmployerTypeDefault AS o_AlternatePreferredPlanEmployerTypeDefault,
	i_ARAPFactorDefault AS o_ARAPFactorDefault,
	i_AssignedRiskSurchargeFactorDefault AS o_AssignedRiskSurchargeFactorDefault,
	i_AtomicRadiationFactorDefault AS o_AtomicRadiationFactorDefault,
	i_AtomicRadiationUnitsOfExposureDefault AS o_AtomicRadiationUnitsOfExposureDefault,
	i_BenefitsDeductibleDefault AS o_BenefitsDeductibleDefault,
	-- *INF*: DECODE(i_BenefitsDeductibleIndicatorDefault  ,'T',1,'F',0,NULL)
	DECODE(
	    i_BenefitsDeductibleIndicatorDefault,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_BenefitsDeductibleIndicatorDefault,
	i_BlanketWaiverFlatDefault AS o_BlanketWaiverFlatDefault,
	i_BlanketWaiverSelectionDefault AS o_BlanketWaiverSelectionDefault,
	i_CertifiedRiskManagementProgramOrServiceDefault AS o_CertifiedRiskManagementProgramOrServiceDefault,
	-- *INF*: DECODE(i_CertifiedSafetyCommitteeCreditDefault  ,'T',1,'F',0,NULL)
	DECODE(
	    i_CertifiedSafetyCommitteeCreditDefault,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_CertifiedSafetyCommitteeCreditDefault,
	i_CoinsuranceSelectionDefault AS o_CoinsuranceSelectionDefault,
	i_CompulsoryWorkplaceSafetyNumberOfYearsNonCompliantDefault AS o_CompulsoryWorkplaceSafetyNumberOfYearsNonCompliantDefault,
	i_ContractorsCreditFactorDefault AS o_ContractorsCreditFactorDefault,
	-- *INF*: DECODE(i_ContractorsCreditIndicatorDefault  ,'T',1,'F',0,NULL)
	DECODE(
	    i_ContractorsCreditIndicatorDefault,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ContractorsCreditIndicatorDefault,
	-- *INF*: DECODE(i_DCAdditionalBenefitsIndicatorDefault  ,'T',1,'F',0,NULL)
	DECODE(
	    i_DCAdditionalBenefitsIndicatorDefault,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_DCAdditionalBenefitsIndicatorDefault,
	i_DeductibleAmountDefault AS o_DeductibleAmountDefault,
	i_DeductibleSelectionDefault AS o_DeductibleSelectionDefault,
	i_DeductibleTypeDefault AS o_DeductibleTypeDefault,
	-- *INF*: DECODE(i_DesignatedMedicalProviderProgramIndicatorDefault  ,'T',1,'F',0,NULL)
	DECODE(
	    i_DesignatedMedicalProviderProgramIndicatorDefault,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_DesignatedMedicalProviderProgramIndicatorDefault,
	-- *INF*: DECODE(i_DrugAndAlcoholPreventionProgramCreditIndicatorDefault  ,'T',1,'F',0,NULL)
	DECODE(
	    i_DrugAndAlcoholPreventionProgramCreditIndicatorDefault,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_DrugAndAlcoholPreventionProgramCreditIndicatorDefault,
	i_DrugFreeWorkplaceFactorDefault AS o_DrugFreeWorkplaceFactorDefault,
	i_EachAccidentLimitDefault AS o_EachAccidentLimitDefault,
	i_EachEmployeeDiseaseLimitDefault AS o_EachEmployeeDiseaseLimitDefault,
	-- *INF*: DECODE(i_EmployersLiabilityCoverageEndorsementIndicatorDefault   ,'T',1,'F',0,NULL)
	DECODE(
	    i_EmployersLiabilityCoverageEndorsementIndicatorDefault,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_EmployersLiabilityCoverageEndorsementIndicatorDefault,
	i_EmployingPreviouslyInjuredEmployeesFactorDefault AS o_EmployingPreviouslyInjuredEmployeesFactorDefault,
	i_ExperienceModEffectiveDateDefault AS o_ExperienceModEffectiveDateDefault,
	i_ExperienceModificationFactorDefault AS o_ExperienceModificationFactorDefault,
	i_ExperienceModModTypeDefault AS o_ExperienceModModTypeDefault,
	i_ExperienceModRiskIDDefault AS o_ExperienceModRiskIDDefault,
	i_ExperienceModTypeDefault AS o_ExperienceModTypeDefault,
	i_ExperienceRatingOptionsDefault AS o_ExperienceRatingOptionsDefault,
	i_FELAIncreasedLiabilityCombinedLimitDefault AS o_FELAIncreasedLiabilityCombinedLimitDefault,
	i_FELAIncreasedLiabilityLowerLimitDefault AS o_FELAIncreasedLiabilityLowerLimitDefault,
	i_FELAIncreasedLiabilityUpperLimitDefault AS o_FELAIncreasedLiabilityUpperLimitDefault,
	i_FlatRateAdjustmentFactorDefault AS o_FlatRateAdjustmentFactorDefault,
	i_ForeignVoluntaryCompensationFlatFeeDefault AS o_ForeignVoluntaryCompensationFlatFeeDefault,
	i_HealthcareNetworkFactorDefault AS o_HealthcareNetworkFactorDefault,
	-- *INF*: DECODE(i_HealthcareNetworkIndicatorDefault  ,'T',1,'F',0,NULL)
	DECODE(
	    i_HealthcareNetworkIndicatorDefault,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_HealthcareNetworkIndicatorDefault,
	i_LargeDeductibleCreditDeductibleDefault AS o_LargeDeductibleCreditDeductibleDefault,
	i_LargeDeductibleFactorDefault AS o_LargeDeductibleFactorDefault,
	i_LossCostTypeDefault AS o_LossCostTypeDefault,
	i_ManagedCareFactorDefault AS o_ManagedCareFactorDefault,
	i_MeritRatingALSelectionsDefault AS o_MeritRatingALSelectionsDefault,
	i_MeritRatingARSelectionsDefault AS o_MeritRatingARSelectionsDefault,
	i_MeritRatingDESelectionsDefault AS o_MeritRatingDESelectionsDefault,
	i_MeritRatingFLSelectionsDefault AS o_MeritRatingFLSelectionsDefault,
	i_MeritRatingGASelectionsDefault AS o_MeritRatingGASelectionsDefault,
	i_MeritRatingMASelectionsDefault AS o_MeritRatingMASelectionsDefault,
	i_MeritRatingMESelectionsDefault AS o_MeritRatingMESelectionsDefault,
	i_MeritRatingMISelectionsDefault AS o_MeritRatingMISelectionsDefault,
	-- *INF*: DECODE(i_MeritRatingModIndicatorDefault  ,'T',1,'F',0,NULL)
	DECODE(
	    i_MeritRatingModIndicatorDefault,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_MeritRatingModIndicatorDefault,
	i_MeritRatingNYSelectionsDefault AS o_MeritRatingNYSelectionsDefault,
	i_MeritRatingOKSelectionsDefault AS o_MeritRatingOKSelectionsDefault,
	i_MeritRatingORSelectionsDefault AS o_MeritRatingORSelectionsDefault,
	i_MeritRatingPASelectionsDefault AS o_MeritRatingPASelectionsDefault,
	i_MeritRatingSDSelectionsDefault AS o_MeritRatingSDSelectionsDefault,
	i_NonRatableIncreasedLimitsFactorDefault AS o_NonRatableIncreasedLimitsFactorDefault,
	i_NumberOfStatesDefault AS o_NumberOfStatesDefault,
	i_NumberOfStrikeDutyDaysDefault AS o_NumberOfStrikeDutyDaysDefault,
	i_NumberOfStrikeDutyEmployeesDefault AS o_NumberOfStrikeDutyEmployeesDefault,
	i_NYPolicyTypeDefault AS o_NYPolicyTypeDefault,
	i_PackageCreditFactorDefault AS o_PackageCreditFactorDefault,
	i_PerAccidentAndAggregateCombinedDeductiblesDefault AS o_PerAccidentAndAggregateCombinedDeductiblesDefault,
	i_PerAccidentDeductiblesDefault AS o_PerAccidentDeductiblesDefault,
	i_PolicyLimitDefault AS o_PolicyLimitDefault,
	-- *INF*: DECODE(i_RepatriationIndicatorDefault  ,'T',1,'F',0,NULL)
	DECODE(
	    i_RepatriationIndicatorDefault,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_RepatriationIndicatorDefault,
	i_SafetyCertificationProgramDefault AS o_SafetyCertificationProgramDefault,
	-- *INF*: DECODE(i_SafetyDeviceRateReductionIndicatorDefault  ,'T',1,'F',0,NULL)
	DECODE(
	    i_SafetyDeviceRateReductionIndicatorDefault,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_SafetyDeviceRateReductionIndicatorDefault,
	i_SafetyIncentiveProgramDefault AS o_SafetyIncentiveProgramDefault,
	i_SafetyInvestmentCreditFactorDefault AS o_SafetyInvestmentCreditFactorDefault,
	i_ScheduleModificationFactorDefault AS o_ScheduleModificationFactorDefault,
	i_ScheduleRatingChoiceDefault AS o_ScheduleRatingChoiceDefault,
	-- *INF*: DECODE(i_ScheduleRatingIndicatorDefault ,'T',1,'F',0,NULL)
	DECODE(
	    i_ScheduleRatingIndicatorDefault,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ScheduleRatingIndicatorDefault,
	i_SmallDeductibleCreditDeductibleDefault AS o_SmallDeductibleCreditDeductibleDefault,
	i_SmallDeductibleSelectionDefault AS o_SmallDeductibleSelectionDefault,
	i_StrikeDutySurchargeDefault AS o_StrikeDutySurchargeDefault,
	-- *INF*: DECODE(i_StrikeDutySurchargeIndicatorDefault  ,'T',1,'F',0,NULL)
	DECODE(
	    i_StrikeDutySurchargeIndicatorDefault,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_StrikeDutySurchargeIndicatorDefault,
	-- *INF*: DECODE(i_TabularAdjustmentProgramDefault  ,'T',1,'F',0,NULL)
	DECODE(
	    i_TabularAdjustmentProgramDefault,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TabularAdjustmentProgramDefault,
	i_TypeOfEmployerDefault AS o_TypeOfEmployerDefault,
	i_VoluntaryCompensationFlatFeeDefault AS o_VoluntaryCompensationFlatFeeDefault,
	i_WaiverOfSubrogationDefault AS o_WaiverOfSubrogationDefault,
	i_WaiverOfSubrogationFactorDefault AS o_WaiverOfSubrogationFactorDefault,
	-- *INF*: DECODE(i_WCPRIndicatorDefault  ,'T',1,'F',0,NULL)
	DECODE(
	    i_WCPRIndicatorDefault,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_WCPRIndicatorDefault,
	-- *INF*: DECODE(i_WorkplaceSafetyCreditIndicatorDefault ,'T',1,'F',0,NULL)
	DECODE(
	    i_WorkplaceSafetyCreditIndicatorDefault,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_WorkplaceSafetyCreditIndicatorDefault,
	i_WorkplaceSafetyPercentageDefault AS o_WorkplaceSafetyPercentageDefault,
	i_DefaultFlexibleRatingAdjustmentFactor AS o_DefaultFlexibleRatingAdjustmentFactor,
	i_ExtractDate AS o_ExtractDate,
	i_SourceSystemId AS o_SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCWCStateDefaultStaging
),
archDCWCStateDefaultStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCWCStateDefaultStaging
	(WC_StateId, WC_StateDefaultId, SessionId, AdmiraltyIncreasedLiabilityCombinedLimitDefault, AdmiraltyIncreasedLiabilityLowerLimitDefault, AdmiraltyIncreasedLiabilityUpperLimitDefault, AggregateLimitsDefault, AlcoholOrDrugFreeWorkplaceCoalMineDefault, AlcoholOrDrugFreeWorkplaceDefault, AlternatePreferredPlanClaimsInfoDefault, AlternatePreferredPlanEmployerTypeDefault, ARAPFactorDefault, AssignedRiskSurchargeFactorDefault, AtomicRadiationFactorDefault, AtomicRadiationUnitsOfExposureDefault, BenefitsDeductibleDefault, BenefitsDeductibleIndicatorDefault, BlanketWaiverFlatDefault, BlanketWaiverSelectionDefault, CertifiedRiskManagementProgramOrServiceDefault, CertifiedSafetyCommitteeCreditDefault, CoinsuranceSelectionDefault, CompulsoryWorkplaceSafetyNumberOfYearsNonCompliantDefault, ContractorsCreditFactorDefault, ContractorsCreditIndicatorDefault, DCAdditionalBenefitsIndicatorDefault, DeductibleAmountDefault, DeductibleSelectionDefault, DeductibleTypeDefault, DesignatedMedicalProviderProgramIndicatorDefault, DrugAndAlcoholPreventionProgramCreditIndicatorDefault, DrugFreeWorkplaceFactorDefault, EachAccidentLimitDefault, EachEmployeeDiseaseLimitDefault, EmployersLiabilityCoverageEndorsementIndicatorDefault, EmployingPreviouslyInjuredEmployeesFactorDefault, ExperienceModEffectiveDateDefault, ExperienceModificationFactorDefault, ExperienceModModTypeDefault, ExperienceModRiskIDDefault, ExperienceModTypeDefault, ExperienceRatingOptionsDefault, FELAIncreasedLiabilityCombinedLimitDefault, FELAIncreasedLiabilityLowerLimitDefault, FELAIncreasedLiabilityUpperLimitDefault, FlatRateAdjustmentFactorDefault, ForeignVoluntaryCompensationFlatFeeDefault, HealthcareNetworkFactorDefault, HealthcareNetworkIndicatorDefault, LargeDeductibleCreditDeductibleDefault, LargeDeductibleFactorDefault, LossCostTypeDefault, ManagedCareFactorDefault, MeritRatingALSelectionsDefault, MeritRatingARSelectionsDefault, MeritRatingDESelectionsDefault, MeritRatingFLSelectionsDefault, MeritRatingGASelectionsDefault, MeritRatingMASelectionsDefault, MeritRatingMESelectionsDefault, MeritRatingMISelectionsDefault, MeritRatingModIndicatorDefault, MeritRatingNYSelectionsDefault, MeritRatingOKSelectionsDefault, MeritRatingORSelectionsDefault, MeritRatingPASelectionsDefault, MeritRatingSDSelectionsDefault, NonRatableIncreasedLimitsFactorDefault, NumberOfStatesDefault, NumberOfStrikeDutyDaysDefault, NumberOfStrikeDutyEmployeesDefault, NYPolicyTypeDefault, PackageCreditFactorDefault, PerAccidentAndAggregateCombinedDeductiblesDefault, PerAccidentDeductiblesDefault, PolicyLimitDefault, RepatriationIndicatorDefault, SafetyCertificationProgramDefault, SafetyDeviceRateReductionIndicatorDefault, SafetyIncentiveProgramDefault, SafetyInvestmentCreditFactorDefault, ScheduleModificationFactorDefault, ScheduleRatingChoiceDefault, ScheduleRatingIndicatorDefault, SmallDeductibleCreditDeductibleDefault, SmallDeductibleSelectionDefault, StrikeDutySurchargeDefault, StrikeDutySurchargeIndicatorDefault, TabularAdjustmentProgramDefault, TypeOfEmployerDefault, VoluntaryCompensationFlatFeeDefault, WaiverOfSubrogationDefault, WaiverOfSubrogationFactorDefault, WCPRIndicatorDefault, WorkplaceSafetyCreditIndicatorDefault, WorkplaceSafetyPercentageDefault, DefaultFlexibleRatingAdjustmentFactor, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	o_WC_StateId AS WC_STATEID, 
	o_WC_StateDefaultId AS WC_STATEDEFAULTID, 
	o_SessionId AS SESSIONID, 
	o_AdmiraltyIncreasedLiabilityCombinedLimitDefault AS ADMIRALTYINCREASEDLIABILITYCOMBINEDLIMITDEFAULT, 
	o_AdmiraltyIncreasedLiabilityLowerLimitDefault AS ADMIRALTYINCREASEDLIABILITYLOWERLIMITDEFAULT, 
	o_AdmiraltyIncreasedLiabilityUpperLimitDefault AS ADMIRALTYINCREASEDLIABILITYUPPERLIMITDEFAULT, 
	o_AggregateLimitsDefault AS AGGREGATELIMITSDEFAULT, 
	o_AlcoholOrDrugFreeWorkplaceCoalMineDefault AS ALCOHOLORDRUGFREEWORKPLACECOALMINEDEFAULT, 
	o_AlcoholOrDrugFreeWorkplaceDefault AS ALCOHOLORDRUGFREEWORKPLACEDEFAULT, 
	o_AlternatePreferredPlanClaimsInfoDefault AS ALTERNATEPREFERREDPLANCLAIMSINFODEFAULT, 
	o_AlternatePreferredPlanEmployerTypeDefault AS ALTERNATEPREFERREDPLANEMPLOYERTYPEDEFAULT, 
	o_ARAPFactorDefault AS ARAPFACTORDEFAULT, 
	o_AssignedRiskSurchargeFactorDefault AS ASSIGNEDRISKSURCHARGEFACTORDEFAULT, 
	o_AtomicRadiationFactorDefault AS ATOMICRADIATIONFACTORDEFAULT, 
	o_AtomicRadiationUnitsOfExposureDefault AS ATOMICRADIATIONUNITSOFEXPOSUREDEFAULT, 
	o_BenefitsDeductibleDefault AS BENEFITSDEDUCTIBLEDEFAULT, 
	o_BenefitsDeductibleIndicatorDefault AS BENEFITSDEDUCTIBLEINDICATORDEFAULT, 
	o_BlanketWaiverFlatDefault AS BLANKETWAIVERFLATDEFAULT, 
	o_BlanketWaiverSelectionDefault AS BLANKETWAIVERSELECTIONDEFAULT, 
	o_CertifiedRiskManagementProgramOrServiceDefault AS CERTIFIEDRISKMANAGEMENTPROGRAMORSERVICEDEFAULT, 
	o_CertifiedSafetyCommitteeCreditDefault AS CERTIFIEDSAFETYCOMMITTEECREDITDEFAULT, 
	o_CoinsuranceSelectionDefault AS COINSURANCESELECTIONDEFAULT, 
	o_CompulsoryWorkplaceSafetyNumberOfYearsNonCompliantDefault AS COMPULSORYWORKPLACESAFETYNUMBEROFYEARSNONCOMPLIANTDEFAULT, 
	o_ContractorsCreditFactorDefault AS CONTRACTORSCREDITFACTORDEFAULT, 
	o_ContractorsCreditIndicatorDefault AS CONTRACTORSCREDITINDICATORDEFAULT, 
	o_DCAdditionalBenefitsIndicatorDefault AS DCADDITIONALBENEFITSINDICATORDEFAULT, 
	o_DeductibleAmountDefault AS DEDUCTIBLEAMOUNTDEFAULT, 
	o_DeductibleSelectionDefault AS DEDUCTIBLESELECTIONDEFAULT, 
	o_DeductibleTypeDefault AS DEDUCTIBLETYPEDEFAULT, 
	o_DesignatedMedicalProviderProgramIndicatorDefault AS DESIGNATEDMEDICALPROVIDERPROGRAMINDICATORDEFAULT, 
	o_DrugAndAlcoholPreventionProgramCreditIndicatorDefault AS DRUGANDALCOHOLPREVENTIONPROGRAMCREDITINDICATORDEFAULT, 
	o_DrugFreeWorkplaceFactorDefault AS DRUGFREEWORKPLACEFACTORDEFAULT, 
	o_EachAccidentLimitDefault AS EACHACCIDENTLIMITDEFAULT, 
	o_EachEmployeeDiseaseLimitDefault AS EACHEMPLOYEEDISEASELIMITDEFAULT, 
	o_EmployersLiabilityCoverageEndorsementIndicatorDefault AS EMPLOYERSLIABILITYCOVERAGEENDORSEMENTINDICATORDEFAULT, 
	o_EmployingPreviouslyInjuredEmployeesFactorDefault AS EMPLOYINGPREVIOUSLYINJUREDEMPLOYEESFACTORDEFAULT, 
	o_ExperienceModEffectiveDateDefault AS EXPERIENCEMODEFFECTIVEDATEDEFAULT, 
	o_ExperienceModificationFactorDefault AS EXPERIENCEMODIFICATIONFACTORDEFAULT, 
	o_ExperienceModModTypeDefault AS EXPERIENCEMODMODTYPEDEFAULT, 
	o_ExperienceModRiskIDDefault AS EXPERIENCEMODRISKIDDEFAULT, 
	o_ExperienceModTypeDefault AS EXPERIENCEMODTYPEDEFAULT, 
	o_ExperienceRatingOptionsDefault AS EXPERIENCERATINGOPTIONSDEFAULT, 
	o_FELAIncreasedLiabilityCombinedLimitDefault AS FELAINCREASEDLIABILITYCOMBINEDLIMITDEFAULT, 
	o_FELAIncreasedLiabilityLowerLimitDefault AS FELAINCREASEDLIABILITYLOWERLIMITDEFAULT, 
	o_FELAIncreasedLiabilityUpperLimitDefault AS FELAINCREASEDLIABILITYUPPERLIMITDEFAULT, 
	o_FlatRateAdjustmentFactorDefault AS FLATRATEADJUSTMENTFACTORDEFAULT, 
	o_ForeignVoluntaryCompensationFlatFeeDefault AS FOREIGNVOLUNTARYCOMPENSATIONFLATFEEDEFAULT, 
	o_HealthcareNetworkFactorDefault AS HEALTHCARENETWORKFACTORDEFAULT, 
	o_HealthcareNetworkIndicatorDefault AS HEALTHCARENETWORKINDICATORDEFAULT, 
	o_LargeDeductibleCreditDeductibleDefault AS LARGEDEDUCTIBLECREDITDEDUCTIBLEDEFAULT, 
	o_LargeDeductibleFactorDefault AS LARGEDEDUCTIBLEFACTORDEFAULT, 
	o_LossCostTypeDefault AS LOSSCOSTTYPEDEFAULT, 
	o_ManagedCareFactorDefault AS MANAGEDCAREFACTORDEFAULT, 
	o_MeritRatingALSelectionsDefault AS MERITRATINGALSELECTIONSDEFAULT, 
	o_MeritRatingARSelectionsDefault AS MERITRATINGARSELECTIONSDEFAULT, 
	o_MeritRatingDESelectionsDefault AS MERITRATINGDESELECTIONSDEFAULT, 
	o_MeritRatingFLSelectionsDefault AS MERITRATINGFLSELECTIONSDEFAULT, 
	o_MeritRatingGASelectionsDefault AS MERITRATINGGASELECTIONSDEFAULT, 
	o_MeritRatingMASelectionsDefault AS MERITRATINGMASELECTIONSDEFAULT, 
	o_MeritRatingMESelectionsDefault AS MERITRATINGMESELECTIONSDEFAULT, 
	o_MeritRatingMISelectionsDefault AS MERITRATINGMISELECTIONSDEFAULT, 
	o_MeritRatingModIndicatorDefault AS MERITRATINGMODINDICATORDEFAULT, 
	o_MeritRatingNYSelectionsDefault AS MERITRATINGNYSELECTIONSDEFAULT, 
	o_MeritRatingOKSelectionsDefault AS MERITRATINGOKSELECTIONSDEFAULT, 
	o_MeritRatingORSelectionsDefault AS MERITRATINGORSELECTIONSDEFAULT, 
	o_MeritRatingPASelectionsDefault AS MERITRATINGPASELECTIONSDEFAULT, 
	o_MeritRatingSDSelectionsDefault AS MERITRATINGSDSELECTIONSDEFAULT, 
	o_NonRatableIncreasedLimitsFactorDefault AS NONRATABLEINCREASEDLIMITSFACTORDEFAULT, 
	o_NumberOfStatesDefault AS NUMBEROFSTATESDEFAULT, 
	o_NumberOfStrikeDutyDaysDefault AS NUMBEROFSTRIKEDUTYDAYSDEFAULT, 
	o_NumberOfStrikeDutyEmployeesDefault AS NUMBEROFSTRIKEDUTYEMPLOYEESDEFAULT, 
	o_NYPolicyTypeDefault AS NYPOLICYTYPEDEFAULT, 
	o_PackageCreditFactorDefault AS PACKAGECREDITFACTORDEFAULT, 
	o_PerAccidentAndAggregateCombinedDeductiblesDefault AS PERACCIDENTANDAGGREGATECOMBINEDDEDUCTIBLESDEFAULT, 
	o_PerAccidentDeductiblesDefault AS PERACCIDENTDEDUCTIBLESDEFAULT, 
	o_PolicyLimitDefault AS POLICYLIMITDEFAULT, 
	o_RepatriationIndicatorDefault AS REPATRIATIONINDICATORDEFAULT, 
	o_SafetyCertificationProgramDefault AS SAFETYCERTIFICATIONPROGRAMDEFAULT, 
	o_SafetyDeviceRateReductionIndicatorDefault AS SAFETYDEVICERATEREDUCTIONINDICATORDEFAULT, 
	o_SafetyIncentiveProgramDefault AS SAFETYINCENTIVEPROGRAMDEFAULT, 
	o_SafetyInvestmentCreditFactorDefault AS SAFETYINVESTMENTCREDITFACTORDEFAULT, 
	o_ScheduleModificationFactorDefault AS SCHEDULEMODIFICATIONFACTORDEFAULT, 
	o_ScheduleRatingChoiceDefault AS SCHEDULERATINGCHOICEDEFAULT, 
	o_ScheduleRatingIndicatorDefault AS SCHEDULERATINGINDICATORDEFAULT, 
	o_SmallDeductibleCreditDeductibleDefault AS SMALLDEDUCTIBLECREDITDEDUCTIBLEDEFAULT, 
	o_SmallDeductibleSelectionDefault AS SMALLDEDUCTIBLESELECTIONDEFAULT, 
	o_StrikeDutySurchargeDefault AS STRIKEDUTYSURCHARGEDEFAULT, 
	o_StrikeDutySurchargeIndicatorDefault AS STRIKEDUTYSURCHARGEINDICATORDEFAULT, 
	o_TabularAdjustmentProgramDefault AS TABULARADJUSTMENTPROGRAMDEFAULT, 
	o_TypeOfEmployerDefault AS TYPEOFEMPLOYERDEFAULT, 
	o_VoluntaryCompensationFlatFeeDefault AS VOLUNTARYCOMPENSATIONFLATFEEDEFAULT, 
	o_WaiverOfSubrogationDefault AS WAIVEROFSUBROGATIONDEFAULT, 
	o_WaiverOfSubrogationFactorDefault AS WAIVEROFSUBROGATIONFACTORDEFAULT, 
	o_WCPRIndicatorDefault AS WCPRINDICATORDEFAULT, 
	o_WorkplaceSafetyCreditIndicatorDefault AS WORKPLACESAFETYCREDITINDICATORDEFAULT, 
	o_WorkplaceSafetyPercentageDefault AS WORKPLACESAFETYPERCENTAGEDEFAULT, 
	o_DefaultFlexibleRatingAdjustmentFactor AS DEFAULTFLEXIBLERATINGADJUSTMENTFACTOR, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),
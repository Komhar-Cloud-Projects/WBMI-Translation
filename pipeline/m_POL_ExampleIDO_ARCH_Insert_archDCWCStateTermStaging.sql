WITH
SQ_DCWCStateTermStaging AS (
	SELECT
		WC_StateTermId,
		SessionId,
		WC_StateId,
		Id,
		AlternatePreferredPlanClaimsInfo,
		AlternatePreferredPlanEmployerType,
		AtomicRadiationUnitsOfExposure,
		BuildGroup,
		CoinsuranceSelection,
		DeductibleSelection,
		DeductibleType,
		EmployingPreviouslyInjuredEmployeesFactorDisplay,
		ExperienceModEffectiveDate,
		ExperienceModType,
		ExperienceRatingOptions,
		ManagedCareFactorDisplay,
		NumberOfStrikeDutyDays,
		NumberOfStrikeDutyEmployeeDays,
		NumberOfStrikeDutyEmployees,
		PeriodEndDate,
		PeriodStartDate,
		PeriodTerm,
		PolicyType,
		RateEffectiveDate,
		SafetyCertificationFactorDisplay,
		ScheduleRatingChoice,
		SmallDeductibleSelection,
		TermType,
		Type,
		TypeOfEmployer,
		WaiverOfSubrogationFactorDisplay,
		WorkplaceSafetyProgramFactorDisplay,
		ExtractDate,
		SourceSystemId,
		CombinedPolicyPremium,
		ManualPremium,
		MinimumPremium,
		ModifiedPremium,
		SubjectPremium,
		TotalStandardPremium
	FROM DCWCStateTermStaging
),
EXP_Metadata AS (
	SELECT
	WC_StateTermId AS i_WC_StateTermId,
	SessionId AS i_SessionId,
	WC_StateId AS i_WC_StateId,
	Id AS i_Id,
	AlternatePreferredPlanClaimsInfo AS i_AlternatePreferredPlanClaimsInfo,
	AlternatePreferredPlanEmployerType AS i_AlternatePreferredPlanEmployerType,
	AtomicRadiationUnitsOfExposure AS i_AtomicRadiationUnitsOfExposure,
	BuildGroup AS i_BuildGroup,
	CoinsuranceSelection AS i_CoinsuranceSelection,
	DeductibleSelection AS i_DeductibleSelection,
	DeductibleType AS i_DeductibleType,
	EmployingPreviouslyInjuredEmployeesFactorDisplay AS i_EmployingPreviouslyInjuredEmployeesFactorDisplay,
	ExperienceModEffectiveDate AS i_ExperienceModEffectiveDate,
	ExperienceModType AS i_ExperienceModType,
	ExperienceRatingOptions AS i_ExperienceRatingOptions,
	ManagedCareFactorDisplay AS i_ManagedCareFactorDisplay,
	NumberOfStrikeDutyDays AS i_NumberOfStrikeDutyDays,
	NumberOfStrikeDutyEmployeeDays AS i_NumberOfStrikeDutyEmployeeDays,
	NumberOfStrikeDutyEmployees AS i_NumberOfStrikeDutyEmployees,
	PeriodEndDate AS i_PeriodEndDate,
	PeriodStartDate AS i_PeriodStartDate,
	PeriodTerm AS i_PeriodTerm,
	PolicyType AS i_PolicyType,
	RateEffectiveDate AS i_RateEffectiveDate,
	SafetyCertificationFactorDisplay AS i_SafetyCertificationFactorDisplay,
	ScheduleRatingChoice AS i_ScheduleRatingChoice,
	SmallDeductibleSelection AS i_SmallDeductibleSelection,
	TermType AS i_TermType,
	Type AS i_Type,
	TypeOfEmployer AS i_TypeOfEmployer,
	WaiverOfSubrogationFactorDisplay AS i_WaiverOfSubrogationFactorDisplay,
	WorkplaceSafetyProgramFactorDisplay AS i_WorkplaceSafetyProgramFactorDisplay,
	ExtractDate AS i_ExtractDate,
	SourceSystemId AS i_SourceSystemId,
	i_WC_StateId AS o_WC_StateId,
	i_WC_StateTermId AS o_WC_StateTermId,
	i_SessionId AS o_SessionId,
	i_Id AS o_Id,
	i_AlternatePreferredPlanClaimsInfo AS o_AlternatePreferredPlanClaimsInfo,
	i_AlternatePreferredPlanEmployerType AS o_AlternatePreferredPlanEmployerType,
	i_AtomicRadiationUnitsOfExposure AS o_AtomicRadiationUnitsOfExposure,
	i_BuildGroup AS o_BuildGroup,
	i_CoinsuranceSelection AS o_CoinsuranceSelection,
	i_DeductibleSelection AS o_DeductibleSelection,
	i_DeductibleType AS o_DeductibleType,
	i_EmployingPreviouslyInjuredEmployeesFactorDisplay AS o_EmployingPreviouslyInjuredEmployeesFactorDisplay,
	i_ExperienceModEffectiveDate AS o_ExperienceModEffectiveDate,
	i_ExperienceModType AS o_ExperienceModType,
	i_ExperienceRatingOptions AS o_ExperienceRatingOptions,
	i_ManagedCareFactorDisplay AS o_ManagedCareFactorDisplay,
	i_NumberOfStrikeDutyDays AS o_NumberOfStrikeDutyDays,
	i_NumberOfStrikeDutyEmployeeDays AS o_NumberOfStrikeDutyEmployeeDays,
	i_NumberOfStrikeDutyEmployees AS o_NumberOfStrikeDutyEmployees,
	i_PeriodEndDate AS o_PeriodEndDate,
	i_PeriodStartDate AS o_PeriodStartDate,
	i_PeriodTerm AS o_PeriodTerm,
	i_PolicyType AS o_PolicyType,
	i_RateEffectiveDate AS o_RateEffectiveDate,
	i_SafetyCertificationFactorDisplay AS o_SafetyCertificationFactorDisplay,
	i_ScheduleRatingChoice AS o_ScheduleRatingChoice,
	i_SmallDeductibleSelection AS o_SmallDeductibleSelection,
	i_TermType AS o_TermType,
	i_Type AS o_Type,
	i_TypeOfEmployer AS o_TypeOfEmployer,
	i_WaiverOfSubrogationFactorDisplay AS o_WaiverOfSubrogationFactorDisplay,
	i_WorkplaceSafetyProgramFactorDisplay AS o_WorkplaceSafetyProgramFactorDisplay,
	i_ExtractDate AS o_ExtractDate,
	i_SourceSystemId AS o_SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	CombinedPolicyPremium,
	ManualPremium,
	MinimumPremium,
	ModifiedPremium,
	SubjectPremium,
	TotalStandardPremium
	FROM SQ_DCWCStateTermStaging
),
archDCWCStateTermStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCWCStateTermStaging
	(WC_StateId, WC_StateTermId, SessionId, Id, AlternatePreferredPlanClaimsInfo, AlternatePreferredPlanEmployerType, AtomicRadiationUnitsOfExposure, BuildGroup, CoinsuranceSelection, DeductibleSelection, DeductibleType, EmployingPreviouslyInjuredEmployeesFactorDisplay, ExperienceModEffectiveDate, ExperienceModType, ExperienceRatingOptions, ManagedCareFactorDisplay, NumberOfStrikeDutyDays, NumberOfStrikeDutyEmployeeDays, NumberOfStrikeDutyEmployees, PeriodEndDate, PeriodStartDate, PeriodTerm, PolicyType, RateEffectiveDate, SafetyCertificationFactorDisplay, ScheduleRatingChoice, SmallDeductibleSelection, TermType, Type, TypeOfEmployer, WaiverOfSubrogationFactorDisplay, WorkplaceSafetyProgramFactorDisplay, ExtractDate, SourceSystemId, AuditId, CombinedPolicyPremium, ManualPremium, MinimumPremium, ModifiedPremium, SubjectPremium, TotalStandardPremium)
	SELECT 
	o_WC_StateId AS WC_STATEID, 
	o_WC_StateTermId AS WC_STATETERMID, 
	o_SessionId AS SESSIONID, 
	o_Id AS ID, 
	o_AlternatePreferredPlanClaimsInfo AS ALTERNATEPREFERREDPLANCLAIMSINFO, 
	o_AlternatePreferredPlanEmployerType AS ALTERNATEPREFERREDPLANEMPLOYERTYPE, 
	o_AtomicRadiationUnitsOfExposure AS ATOMICRADIATIONUNITSOFEXPOSURE, 
	o_BuildGroup AS BUILDGROUP, 
	o_CoinsuranceSelection AS COINSURANCESELECTION, 
	o_DeductibleSelection AS DEDUCTIBLESELECTION, 
	o_DeductibleType AS DEDUCTIBLETYPE, 
	o_EmployingPreviouslyInjuredEmployeesFactorDisplay AS EMPLOYINGPREVIOUSLYINJUREDEMPLOYEESFACTORDISPLAY, 
	o_ExperienceModEffectiveDate AS EXPERIENCEMODEFFECTIVEDATE, 
	o_ExperienceModType AS EXPERIENCEMODTYPE, 
	o_ExperienceRatingOptions AS EXPERIENCERATINGOPTIONS, 
	o_ManagedCareFactorDisplay AS MANAGEDCAREFACTORDISPLAY, 
	o_NumberOfStrikeDutyDays AS NUMBEROFSTRIKEDUTYDAYS, 
	o_NumberOfStrikeDutyEmployeeDays AS NUMBEROFSTRIKEDUTYEMPLOYEEDAYS, 
	o_NumberOfStrikeDutyEmployees AS NUMBEROFSTRIKEDUTYEMPLOYEES, 
	o_PeriodEndDate AS PERIODENDDATE, 
	o_PeriodStartDate AS PERIODSTARTDATE, 
	o_PeriodTerm AS PERIODTERM, 
	o_PolicyType AS POLICYTYPE, 
	o_RateEffectiveDate AS RATEEFFECTIVEDATE, 
	o_SafetyCertificationFactorDisplay AS SAFETYCERTIFICATIONFACTORDISPLAY, 
	o_ScheduleRatingChoice AS SCHEDULERATINGCHOICE, 
	o_SmallDeductibleSelection AS SMALLDEDUCTIBLESELECTION, 
	o_TermType AS TERMTYPE, 
	o_Type AS TYPE, 
	o_TypeOfEmployer AS TYPEOFEMPLOYER, 
	o_WaiverOfSubrogationFactorDisplay AS WAIVEROFSUBROGATIONFACTORDISPLAY, 
	o_WorkplaceSafetyProgramFactorDisplay AS WORKPLACESAFETYPROGRAMFACTORDISPLAY, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	COMBINEDPOLICYPREMIUM, 
	MANUALPREMIUM, 
	MINIMUMPREMIUM, 
	MODIFIEDPREMIUM, 
	SUBJECTPREMIUM, 
	TOTALSTANDARDPREMIUM
	FROM EXP_Metadata
),
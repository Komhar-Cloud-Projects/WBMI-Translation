WITH
SQ_DCCACoveragePIPStage AS (
	SELECT
		DCCACoveragePIPStageId,
		ExtractDate,
		SourceSystemId,
		CoverageId,
		CA_CoveragePIPId,
		SessionId,
		AccDeathBenDescription,
		AccDeathBenOtherDescription,
		AdditionalChiropractic,
		AdditionalCombinationLimitIndicator,
		AdditionalLimit,
		AdditionalLimitPA,
		AlternativeCare,
		ApplyPIPDeductible,
		AutoDealersOptions,
		Broadened,
		BroadenedPIP,
		CoordinationOfBenefits,
		DeductibleOnly,
		EmployeeOperated,
		EmployeeOperatedOrWorkersComp,
		ExcessMedical,
		ExcessWorkLoss,
		ExclusionOfWorkLossIndicator,
		ExclustionOfWorkLoss,
		ExtendedPIP,
		FamilyMembers,
		Furnished,
		GovernmentSponsored,
		GuestPIP,
		IncreasedLimits,
		InterstateBasis,
		ManagedCare,
		MedicalExpenseBenefit,
		MedicalExpenseElimination,
		MedicalExpensesDescription,
		MedicalExpensesOtherDescription,
		NamedFamily,
		NamedFamilyMembers,
		NamedInsuredOnly,
		NumberOfFamilyMembers,
		NumberOfNamedIndividuals,
		NumberOfResidentRelatives,
		OptionalBasicEconomicLoss,
		OwnerOperated,
		PedestrianFirstParty,
		PedestrianOnly,
		PIPWaiver,
		ResidentChildren,
		StackedPIP,
		TortLimitationElimination,
		TortMedicalExpenseSecondaryOption,
		TXPIPAutoDescription,
		TXPIPAutoOtherDescription,
		UnderwriterOverride,
		WorkComp,
		WorkCompAndEmployeeOperated,
		WorkLossAndADB,
		WorkLossCoordination,
		WorkLossDescription,
		WorkLossOtherDescription
	FROM DCCACoveragePIPStage
),
EXP_Set_MetaData AS (
	SELECT
	DCCACoveragePIPStageId,
	ExtractDate,
	SourceSystemId,
	CoverageId,
	CA_CoveragePIPId,
	SessionId,
	AccDeathBenDescription,
	AccDeathBenOtherDescription,
	AdditionalChiropractic AS i_AdditionalChiropractic,
	-- *INF*: DECODE(i_AdditionalChiropractic,'T','1','F','0',NULL)
	DECODE(
	    i_AdditionalChiropractic,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_AdditionalChiropractic,
	AdditionalCombinationLimitIndicator AS i_AdditionalCombinationLimitIndicator,
	-- *INF*: DECODE(i_AdditionalCombinationLimitIndicator,'T','1','F','0')
	DECODE(
	    i_AdditionalCombinationLimitIndicator,
	    'T', '1',
	    'F', '0'
	) AS o_AdditionalCombinationLimitIndicator,
	AdditionalLimit AS i_AdditionalLimit,
	-- *INF*: DECODE(i_AdditionalLimit,'T','1','F','0',NULL)
	DECODE(
	    i_AdditionalLimit,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_AdditionalLimit,
	AdditionalLimitPA AS i_AdditionalLimitPA,
	-- *INF*: DECODE(i_AdditionalLimitPA,'T','1','F','0',NULL)
	DECODE(
	    i_AdditionalLimitPA,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_AdditionalLimitPA,
	AlternativeCare AS i_AlternativeCare,
	-- *INF*: DECODE(i_AlternativeCare,'T','1','F','0',NULL)
	DECODE(
	    i_AlternativeCare,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_AlternativeCare,
	ApplyPIPDeductible,
	AutoDealersOptions,
	Broadened AS i_Broadened,
	-- *INF*: DECODE(i_Broadened,'T','1','F','0',NULL)
	DECODE(
	    i_Broadened,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_Broadened,
	BroadenedPIP AS i_BroadenedPIP,
	-- *INF*: DECODE(i_BroadenedPIP,'T','1','F','0',NULL)
	DECODE(
	    i_BroadenedPIP,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_BroadenedPIP,
	CoordinationOfBenefits,
	DeductibleOnly AS i_DeductibleOnly,
	-- *INF*: DECODE(i_DeductibleOnly,'T','1','F','0',NULL)
	DECODE(
	    i_DeductibleOnly,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_DeductibleOnly,
	EmployeeOperated AS i_EmployeeOperated,
	-- *INF*: DECODE(i_EmployeeOperated,'T','1','F','0',NULL)
	DECODE(
	    i_EmployeeOperated,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EmployeeOperated,
	EmployeeOperatedOrWorkersComp AS i_EmployeeOperatedOrWorkersComp,
	-- *INF*: DECODE(i_EmployeeOperatedOrWorkersComp,'T','1','F','0',NULL)
	DECODE(
	    i_EmployeeOperatedOrWorkersComp,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EmployeeOperatedOrWorkersComp,
	ExcessMedical AS i_ExcessMedical,
	-- *INF*: DECODE(i_ExcessMedical,'T','1','F','0',NULL)
	DECODE(
	    i_ExcessMedical,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ExcessMedical,
	ExcessWorkLoss AS i_ExcessWorkLoss,
	-- *INF*: DECODE(i_ExcessWorkLoss,'T','1','F','0',NULL)
	DECODE(
	    i_ExcessWorkLoss,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ExcessWorkLoss,
	ExclusionOfWorkLossIndicator AS i_ExclusionOfWorkLossIndicator,
	-- *INF*: DECODE(i_ExclusionOfWorkLossIndicator,'T','1','F','0',NULL)
	DECODE(
	    i_ExclusionOfWorkLossIndicator,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ExclusionOfWorkLossIndicator,
	ExclustionOfWorkLoss,
	ExtendedPIP,
	FamilyMembers AS i_FamilyMembers,
	-- *INF*: DECODE(i_FamilyMembers,'T','1','F','0',NULL)
	DECODE(
	    i_FamilyMembers,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_FamilyMembers,
	Furnished AS i_Furnished,
	-- *INF*: DECODE(i_Furnished,'T','1','F','0',NULL)
	DECODE(
	    i_Furnished,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_Furnished,
	GovernmentSponsored AS i_GovernmentSponsored,
	-- *INF*: DECODE(i_GovernmentSponsored,'T','1','F','0',NULL)
	DECODE(
	    i_GovernmentSponsored,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_GovernmentSponsored,
	GuestPIP AS i_GuestPIP,
	-- *INF*: DECODE(i_GuestPIP,'T','1','F','0',NULL)
	DECODE(
	    i_GuestPIP,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_GuestPIP,
	IncreasedLimits AS i_IncreasedLimits,
	-- *INF*: DECODE(i_IncreasedLimits,'T','1','F','0',NULL)
	DECODE(
	    i_IncreasedLimits,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_IncreasedLimits,
	InterstateBasis AS i_InterstateBasis,
	-- *INF*: DECODE(i_InterstateBasis,'T','1','F','0',NULL)
	DECODE(
	    i_InterstateBasis,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_InterstateBasis,
	ManagedCare AS i_ManagedCare,
	-- *INF*: DECODE(i_ManagedCare,'T','1','F','0',NULL)
	DECODE(
	    i_ManagedCare,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ManagedCare,
	MedicalExpenseBenefit AS i_MedicalExpenseBenefit,
	-- *INF*: DECODE(i_MedicalExpenseBenefit,'T','1','F','0',NULL)
	DECODE(
	    i_MedicalExpenseBenefit,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_MedicalExpenseBenefit,
	MedicalExpenseElimination AS i_MedicalExpenseElimination,
	-- *INF*: DECODE(i_MedicalExpenseElimination,'T','1','F','0',NULL)
	DECODE(
	    i_MedicalExpenseElimination,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_MedicalExpenseElimination,
	MedicalExpensesDescription,
	MedicalExpensesOtherDescription,
	NamedFamily,
	NamedFamilyMembers,
	NamedInsuredOnly AS i_NamedInsuredOnly,
	-- *INF*: DECODE(i_NamedInsuredOnly,'T','1','F','0',NULL)
	DECODE(
	    i_NamedInsuredOnly,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_NamedInsuredOnly,
	NumberOfFamilyMembers,
	NumberOfNamedIndividuals,
	NumberOfResidentRelatives,
	OptionalBasicEconomicLoss AS i_OptionalBasicEconomicLoss,
	-- *INF*: DECODE(i_OptionalBasicEconomicLoss,'T','1','F','0',NULL)
	DECODE(
	    i_OptionalBasicEconomicLoss,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_OptionalBasicEconomicLoss,
	OwnerOperated AS i_OwnerOperated,
	-- *INF*: DECODE(i_OwnerOperated,'T','1','F','0',NULL)
	DECODE(
	    i_OwnerOperated,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_OwnerOperated,
	PedestrianFirstParty AS i_PedestrianFirstParty,
	-- *INF*: DECODE(i_PedestrianFirstParty,'T','1','F','0',NULL)
	DECODE(
	    i_PedestrianFirstParty,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_PedestrianFirstParty,
	PedestrianOnly AS i_PedestrianOnly,
	-- *INF*: DECODE(i_PedestrianOnly,'T','1','F','0',NULL)
	DECODE(
	    i_PedestrianOnly,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_PedestrianOnly,
	PIPWaiver AS i_PIPWaiver,
	-- *INF*: DECODE(i_PIPWaiver,'T','1','F','0',NULL)
	DECODE(
	    i_PIPWaiver,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_PIPWaiver,
	ResidentChildren AS i_ResidentChildren,
	-- *INF*: DECODE(i_ResidentChildren,'T','1','F','0',NULL)
	DECODE(
	    i_ResidentChildren,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ResidentChildren,
	StackedPIP AS i_StackedPIP,
	-- *INF*: DECODE(i_StackedPIP,'T','1','F','0',NULL)
	DECODE(
	    i_StackedPIP,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_StackedPIP,
	TortLimitationElimination AS i_TortLimitationElimination,
	-- *INF*: DECODE(i_TortLimitationElimination,'T','1','F','0',NULL)
	DECODE(
	    i_TortLimitationElimination,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TortLimitationElimination,
	TortMedicalExpenseSecondaryOption AS i_TortMedicalExpenseSecondaryOption,
	-- *INF*: DECODE(i_TortMedicalExpenseSecondaryOption,'T','1','F','0',NULL)
	DECODE(
	    i_TortMedicalExpenseSecondaryOption,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TortMedicalExpenseSecondaryOption,
	TXPIPAutoDescription,
	TXPIPAutoOtherDescription,
	UnderwriterOverride AS i_UnderwriterOverride,
	-- *INF*: DECODE(i_UnderwriterOverride,'T','1','F','0',NULL)
	DECODE(
	    i_UnderwriterOverride,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_UnderwriterOverride,
	WorkComp AS i_WorkComp,
	-- *INF*: DECODE(i_WorkComp,'T','1','F','0',NULL)
	DECODE(
	    i_WorkComp,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_WorkComp,
	WorkCompAndEmployeeOperated AS i_WorkCompAndEmployeeOperated,
	-- *INF*: DECODE(i_WorkCompAndEmployeeOperated,'T','1','F','0',NULL)
	DECODE(
	    i_WorkCompAndEmployeeOperated,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_WorkCompAndEmployeeOperated,
	WorkLossAndADB,
	WorkLossCoordination AS i_WorkLossCoordination,
	-- *INF*: DECODE(i_WorkLossCoordination,'T','1','F','0',NULL)
	DECODE(
	    i_WorkLossCoordination,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_WorkLossCoordination,
	WorkLossDescription,
	WorkLossOtherDescription,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCACoveragePIPStage
),
ArchDCCACoveragePIPStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCACoveragePIPStage
	(ExtractDate, SourceSystemId, AuditId, DCCACoveragePIPStageId, CoverageId, CA_CoveragePIPId, SessionId, AccDeathBenDescription, AccDeathBenOtherDescription, AdditionalChiropractic, AdditionalCombinationLimitIndicator, AdditionalLimit, AdditionalLimitPA, AlternativeCare, ApplyPIPDeductible, AutoDealersOptions, Broadened, BroadenedPIP, CoordinationOfBenefits, DeductibleOnly, EmployeeOperated, EmployeeOperatedOrWorkersComp, ExcessMedical, ExcessWorkLoss, ExclusionOfWorkLossIndicator, ExclustionOfWorkLoss, ExtendedPIP, FamilyMembers, Furnished, GovernmentSponsored, GuestPIP, IncreasedLimits, InterstateBasis, ManagedCare, MedicalExpenseBenefit, MedicalExpenseElimination, MedicalExpensesDescription, MedicalExpensesOtherDescription, NamedFamily, NamedFamilyMembers, NamedInsuredOnly, NumberOfFamilyMembers, NumberOfNamedIndividuals, NumberOfResidentRelatives, OptionalBasicEconomicLoss, OwnerOperated, PedestrianFirstParty, PedestrianOnly, PIPWaiver, ResidentChildren, StackedPIP, TortLimitationElimination, TortMedicalExpenseSecondaryOption, TXPIPAutoDescription, TXPIPAutoOtherDescription, UnderwriterOverride, WorkComp, WorkCompAndEmployeeOperated, WorkLossAndADB, WorkLossCoordination, WorkLossDescription, WorkLossOtherDescription)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCCACOVERAGEPIPSTAGEID, 
	COVERAGEID, 
	CA_COVERAGEPIPID, 
	SESSIONID, 
	ACCDEATHBENDESCRIPTION, 
	ACCDEATHBENOTHERDESCRIPTION, 
	o_AdditionalChiropractic AS ADDITIONALCHIROPRACTIC, 
	o_AdditionalCombinationLimitIndicator AS ADDITIONALCOMBINATIONLIMITINDICATOR, 
	o_AdditionalLimit AS ADDITIONALLIMIT, 
	o_AdditionalLimitPA AS ADDITIONALLIMITPA, 
	o_AlternativeCare AS ALTERNATIVECARE, 
	APPLYPIPDEDUCTIBLE, 
	AUTODEALERSOPTIONS, 
	o_Broadened AS BROADENED, 
	o_BroadenedPIP AS BROADENEDPIP, 
	COORDINATIONOFBENEFITS, 
	o_DeductibleOnly AS DEDUCTIBLEONLY, 
	o_EmployeeOperated AS EMPLOYEEOPERATED, 
	o_EmployeeOperatedOrWorkersComp AS EMPLOYEEOPERATEDORWORKERSCOMP, 
	o_ExcessMedical AS EXCESSMEDICAL, 
	o_ExcessWorkLoss AS EXCESSWORKLOSS, 
	o_ExclusionOfWorkLossIndicator AS EXCLUSIONOFWORKLOSSINDICATOR, 
	EXCLUSTIONOFWORKLOSS, 
	EXTENDEDPIP, 
	o_FamilyMembers AS FAMILYMEMBERS, 
	o_Furnished AS FURNISHED, 
	o_GovernmentSponsored AS GOVERNMENTSPONSORED, 
	o_GuestPIP AS GUESTPIP, 
	o_IncreasedLimits AS INCREASEDLIMITS, 
	o_InterstateBasis AS INTERSTATEBASIS, 
	o_ManagedCare AS MANAGEDCARE, 
	o_MedicalExpenseBenefit AS MEDICALEXPENSEBENEFIT, 
	o_MedicalExpenseElimination AS MEDICALEXPENSEELIMINATION, 
	MEDICALEXPENSESDESCRIPTION, 
	MEDICALEXPENSESOTHERDESCRIPTION, 
	NAMEDFAMILY, 
	NAMEDFAMILYMEMBERS, 
	o_NamedInsuredOnly AS NAMEDINSUREDONLY, 
	NUMBEROFFAMILYMEMBERS, 
	NUMBEROFNAMEDINDIVIDUALS, 
	NUMBEROFRESIDENTRELATIVES, 
	o_OptionalBasicEconomicLoss AS OPTIONALBASICECONOMICLOSS, 
	o_OwnerOperated AS OWNEROPERATED, 
	o_PedestrianFirstParty AS PEDESTRIANFIRSTPARTY, 
	o_PedestrianOnly AS PEDESTRIANONLY, 
	o_PIPWaiver AS PIPWAIVER, 
	o_ResidentChildren AS RESIDENTCHILDREN, 
	o_StackedPIP AS STACKEDPIP, 
	o_TortLimitationElimination AS TORTLIMITATIONELIMINATION, 
	o_TortMedicalExpenseSecondaryOption AS TORTMEDICALEXPENSESECONDARYOPTION, 
	TXPIPAUTODESCRIPTION, 
	TXPIPAUTOOTHERDESCRIPTION, 
	o_UnderwriterOverride AS UNDERWRITEROVERRIDE, 
	o_WorkComp AS WORKCOMP, 
	o_WorkCompAndEmployeeOperated AS WORKCOMPANDEMPLOYEEOPERATED, 
	WORKLOSSANDADB, 
	o_WorkLossCoordination AS WORKLOSSCOORDINATION, 
	WORKLOSSDESCRIPTION, 
	WORKLOSSOTHERDESCRIPTION
	FROM EXP_Set_MetaData
),
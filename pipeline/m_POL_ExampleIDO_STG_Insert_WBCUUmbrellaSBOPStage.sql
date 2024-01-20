WITH
SQ_WB_CU_UmbrellaSBOP AS (
	WITH cte_WBCUUmbrellaSBOP(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.WB_CU_UmbrellaSBOPId, 
	X.SessionId, 
	X.Deleted, 
	X.CoverageForm, 
	X.Description, 
	X.EffectiveDate, 
	X.ExpirationDate, 
	X.PersonalLiability,
	X.PolicyNumber, 
	X.ProfessionalLiabilityEachOccurrenceClaimLimit, 
	X.ProfessionalLiabilityAggregate, 
	X.BodilyInjuryByAccidentEachAccident, 
	X.BodilyInjuryByDiseaseEachEmployee, 
	X.BodilyInjuryByDiseaseAggregate, 
	X.FirstMillionBasePremium, 
	X.FirstMillionModifiedPremium, 
	X.PremOpsTotalPremiumTable1, 
	X.PremOpsTotalPremiumTable2, 
	X.PremOpsTotalPremiumTable3, 
	X.AllOtherPremium, 
	X.ProductsTotalPremiumTableA, 
	X.ProductsTotalPremiumTableB, 
	X.ProductsTotalPremiumTableC, 
	X.IncludeProfessionalLiability, 
	X.AllOtherPremiumOverride, 
	X.PremOpsTotalPremiumTable1PremiumOverride, 
	X.PremOpsTotalPremiumTable2PremiumOverride, 
	X.PremOpsTotalPremiumTable3PremiumOverride, 
	X.ProductsTotalPremiumTableAPremiumOverride, 
	X.ProductsTotalPremiumTableBPremiumOverride, 
	X.ProductsTotalPremiumTableCPremiumOverride, 
	X.AllOtherRevisedPremium, 
	X.PremOpsTotalPremiumTable1RevisedPremium, 
	X.PremOpsTotalPremiumTable2RevisedPremium, 
	X.PremOpsTotalPremiumTable3RevisedPremium, 
	X.ProductsTotalPremiumTableARevisedPremium, 
	X.ProductsTotalPremiumTableBRevisedPremium, 
	X.ProductsTotalPremiumTableCRevisedPremium, 
	X.ScheduledModification, 
	X.IncludeCGL, 
	X.IncludeOhioStopGapLiability, 
	X.Premium, 
	X.PremiumChange, 
	X.PremiumWritten, 
	X.IDField, 
	X.UmbrellaSBOPScheduledModificationLocationInComment, 
	X.UmbrellaSBOPScheduledModificationLocationInModification, 
	X.UmbrellaSBOPScheduledModificationLocationOutComment, 
	X.UmbrellaSBOPScheduledModificationLocationOutModification, 
	X.UmbrellaSBOPScheduledModificationPremisesComment, 
	X.UmbrellaSBOPScheduledModificationPremisesModification, 
	X.UmbrellaSBOPScheduledModificationEquipmentComment, 
	X.UmbrellaSBOPScheduledModificationEquipmentModification, 
	X.UmbrellaSBOPScheduledModificationManagementComment, 
	X.UmbrellaSBOPScheduledModificationManagementModification, 
	X.UmbrellaSBOPScheduledModificationEmployeesComment, 
	X.UmbrellaSBOPScheduledModificationEmployeesModification, 
	X.UmbrellaSBOPScheduledModificationCooperationMedicalComment, 
	X.UmbrellaSBOPScheduledModificationCooperationMedicalModification, 
	X.UmbrellaSBOPScheduledModificationCooperationSafetyComment, 
	X.UmbrellaSBOPScheduledModificationCooperationSafetyModification, 
	X.ModificationTotal, 
	X.ModificationTotalForSBOPBusinessDetailPage 
	FROM
	WB_CU_UmbrellaSBOP X
	inner join
	cte_WBCUUmbrellaSBOP Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	LineId,
	WB_CU_UmbrellaSBOPId,
	SessionId,
	Deleted AS i_Deleted,
	-- *INF*: DECODE(i_Deleted,'T',1,'F',0,NULL)
	DECODE(
	    i_Deleted,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Deleted,
	CoverageForm,
	Description,
	EffectiveDate,
	ExpirationDate,
	PersonalLiability AS i_PersonalLiability,
	-- *INF*: DECODE(i_PersonalLiability,'T',1,'F',0,NULL)
	DECODE(
	    i_PersonalLiability,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PersonalLiability,
	PolicyNumber,
	ProfessionalLiabilityEachOccurrenceClaimLimit,
	ProfessionalLiabilityAggregate,
	BodilyInjuryByAccidentEachAccident,
	BodilyInjuryByDiseaseEachEmployee,
	BodilyInjuryByDiseaseAggregate,
	FirstMillionBasePremium,
	FirstMillionModifiedPremium,
	PremOpsTotalPremiumTable1,
	PremOpsTotalPremiumTable2,
	PremOpsTotalPremiumTable3,
	AllOtherPremium,
	ProductsTotalPremiumTableA,
	ProductsTotalPremiumTableB,
	ProductsTotalPremiumTableC,
	IncludeProfessionalLiability AS i_IncludeProfessionalLiability,
	AllOtherPremiumOverride AS i_AllOtherPremiumOverride,
	PremOpsTotalPremiumTable1PremiumOverride AS i_PremOpsTotalPremiumTable1PremiumOverride,
	PremOpsTotalPremiumTable2PremiumOverride AS i_PremOpsTotalPremiumTable2PremiumOverride,
	PremOpsTotalPremiumTable3PremiumOverride AS i_PremOpsTotalPremiumTable3PremiumOverride,
	ProductsTotalPremiumTableAPremiumOverride AS i_ProductsTotalPremiumTableAPremiumOverride,
	ProductsTotalPremiumTableBPremiumOverride AS i_ProductsTotalPremiumTableBPremiumOverride,
	ProductsTotalPremiumTableCPremiumOverride AS i_ProductsTotalPremiumTableCPremiumOverride,
	-- *INF*: DECODE(i_IncludeProfessionalLiability,'T',1,'F',0,NULL)
	DECODE(
	    i_IncludeProfessionalLiability,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IncludeProfessionalLiability,
	-- *INF*: DECODE(i_AllOtherPremiumOverride,'T',1,'F',0,NULL)
	DECODE(
	    i_AllOtherPremiumOverride,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AllOtherPremiumOverride,
	-- *INF*: DECODE(i_PremOpsTotalPremiumTable1PremiumOverride,'T',1,'F',0,NULL)
	DECODE(
	    i_PremOpsTotalPremiumTable1PremiumOverride,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PremOpsTotalPremiumTable1PremiumOverride,
	-- *INF*: DECODE(i_PremOpsTotalPremiumTable2PremiumOverride,'T',1,'F',0,NULL)
	DECODE(
	    i_PremOpsTotalPremiumTable2PremiumOverride,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PremOpsTotalPremiumTable2PremiumOverride,
	-- *INF*: DECODE(i_PremOpsTotalPremiumTable3PremiumOverride,'T',1,'F',0,NULL)
	DECODE(
	    i_PremOpsTotalPremiumTable3PremiumOverride,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PremOpsTotalPremiumTable3PremiumOverride,
	-- *INF*: DECODE(i_ProductsTotalPremiumTableAPremiumOverride,'T',1,'F',0,NULL)
	DECODE(
	    i_ProductsTotalPremiumTableAPremiumOverride,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ProductsTotalPremiumTableAPremiumOverride,
	-- *INF*: DECODE(i_ProductsTotalPremiumTableBPremiumOverride,'T',1,'F',0,NULL)
	DECODE(
	    i_ProductsTotalPremiumTableBPremiumOverride,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ProductsTotalPremiumTableBPremiumOverride,
	-- *INF*: DECODE(i_ProductsTotalPremiumTableCPremiumOverride,'T',1,'F',0,NULL)
	DECODE(
	    i_ProductsTotalPremiumTableCPremiumOverride,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ProductsTotalPremiumTableCPremiumOverride,
	AllOtherRevisedPremium,
	PremOpsTotalPremiumTable1RevisedPremium,
	PremOpsTotalPremiumTable2RevisedPremium,
	PremOpsTotalPremiumTable3RevisedPremium,
	ProductsTotalPremiumTableARevisedPremium,
	ProductsTotalPremiumTableBRevisedPremium,
	ProductsTotalPremiumTableCRevisedPremium,
	ScheduledModification,
	IncludeCGL AS i_IncludeCGL,
	IncludeOhioStopGapLiability AS i_IncludeOhioStopGapLiability,
	-- *INF*: DECODE(i_IncludeCGL,'T',1,'F',0,NULL)
	DECODE(
	    i_IncludeCGL,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IncludeCGL,
	-- *INF*: DECODE(i_IncludeOhioStopGapLiability,'T',1,'F',0,NULL)
	DECODE(
	    i_IncludeOhioStopGapLiability,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IncludeOhioStopGapLiability,
	Premium,
	PremiumChange,
	PremiumWritten,
	IDField,
	UmbrellaSBOPScheduledModificationLocationInComment,
	UmbrellaSBOPScheduledModificationLocationInModification,
	UmbrellaSBOPScheduledModificationLocationOutComment,
	UmbrellaSBOPScheduledModificationLocationOutModification,
	UmbrellaSBOPScheduledModificationPremisesComment,
	UmbrellaSBOPScheduledModificationPremisesModification,
	UmbrellaSBOPScheduledModificationEquipmentComment,
	UmbrellaSBOPScheduledModificationEquipmentModification,
	UmbrellaSBOPScheduledModificationManagementComment,
	UmbrellaSBOPScheduledModificationManagementModification,
	UmbrellaSBOPScheduledModificationEmployeesComment,
	UmbrellaSBOPScheduledModificationEmployeesModification,
	UmbrellaSBOPScheduledModificationCooperationMedicalComment,
	UmbrellaSBOPScheduledModificationCooperationMedicalModification,
	UmbrellaSBOPScheduledModificationCooperationSafetyComment,
	UmbrellaSBOPScheduledModificationCooperationSafetyModification,
	ModificationTotal,
	ModificationTotalForSBOPBusinessDetailPage,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_CU_UmbrellaSBOP
),
WBCUUmbrellaSBOPStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCUUmbrellaSBOPStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCUUmbrellaSBOPStage
	(LineId, WBCUUmbrellaSBOPId, SessionId, Deleted, CoverageForm, Description, EffectiveDate, ExpirationDate, PersonalLiability, PolicyNumber, ProfessionalLiabilityEachOccurrenceClaimLimit, ProfessionalLiabilityAggregate, BodilyInjuryByAccidentEachAccident, BodilyInjuryByDiseaseEachEmployee, BodilyInjuryByDiseaseAggregate, FirstMillionBasePremium, FirstMillionModifiedPremium, PremOpsTotalPremiumTable1, PremOpsTotalPremiumTable2, PremOpsTotalPremiumTable3, AllOtherPremium, ProductsTotalPremiumTableA, ProductsTotalPremiumTableB, ProductsTotalPremiumTableC, IncludeProfessionalLiability, AllOtherPremiumOverride, PremOpsTotalPremiumTable1PremiumOverride, PremOpsTotalPremiumTable2PremiumOverride, PremOpsTotalPremiumTable3PremiumOverride, ProductsTotalPremiumTableAPremiumOverride, ProductsTotalPremiumTableBPremiumOverride, ProductsTotalPremiumTableCPremiumOverride, AllOtherRevisedPremium, PremOpsTotalPremiumTable1RevisedPremium, PremOpsTotalPremiumTable2RevisedPremium, PremOpsTotalPremiumTable3RevisedPremium, ProductsTotalPremiumTableARevisedPremium, ProductsTotalPremiumTableBRevisedPremium, ProductsTotalPremiumTableCRevisedPremium, ScheduledModification, IncludeCGL, IncludeOhioStopGapLiability, Premium, PremiumChange, PremiumWritten, IDField, UmbrellaSBOPScheduledModificationLocationInComment, UmbrellaSBOPScheduledModificationLocationInModification, UmbrellaSBOPScheduledModificationLocationOutComment, UmbrellaSBOPScheduledModificationLocationOutModification, UmbrellaSBOPScheduledModificationPremisesComment, UmbrellaSBOPScheduledModificationPremisesModification, UmbrellaSBOPScheduledModificationEquipmentComment, UmbrellaSBOPScheduledModificationEquipmentModification, UmbrellaSBOPScheduledModificationManagementComment, UmbrellaSBOPScheduledModificationManagementModification, UmbrellaSBOPScheduledModificationEmployeesComment, UmbrellaSBOPScheduledModificationEmployeesModification, UmbrellaSBOPScheduledModificationCooperationMedicalComment, UmbrellaSBOPScheduledModificationCooperationMedicalModification, UmbrellaSBOPScheduledModificationCooperationSafetyComment, UmbrellaSBOPScheduledModificationCooperationSafetyModification, ModificationTotal, ModificationTotalForSBOPBusinessDetailPage, ExtractDate, SourceSystemId)
	SELECT 
	LINEID, 
	WB_CU_UmbrellaSBOPId AS WBCUUMBRELLASBOPID, 
	SESSIONID, 
	o_Deleted AS DELETED, 
	COVERAGEFORM, 
	DESCRIPTION, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	o_PersonalLiability AS PERSONALLIABILITY, 
	POLICYNUMBER, 
	PROFESSIONALLIABILITYEACHOCCURRENCECLAIMLIMIT, 
	PROFESSIONALLIABILITYAGGREGATE, 
	BODILYINJURYBYACCIDENTEACHACCIDENT, 
	BODILYINJURYBYDISEASEEACHEMPLOYEE, 
	BODILYINJURYBYDISEASEAGGREGATE, 
	FIRSTMILLIONBASEPREMIUM, 
	FIRSTMILLIONMODIFIEDPREMIUM, 
	PREMOPSTOTALPREMIUMTABLE1, 
	PREMOPSTOTALPREMIUMTABLE2, 
	PREMOPSTOTALPREMIUMTABLE3, 
	ALLOTHERPREMIUM, 
	PRODUCTSTOTALPREMIUMTABLEA, 
	PRODUCTSTOTALPREMIUMTABLEB, 
	PRODUCTSTOTALPREMIUMTABLEC, 
	o_IncludeProfessionalLiability AS INCLUDEPROFESSIONALLIABILITY, 
	o_AllOtherPremiumOverride AS ALLOTHERPREMIUMOVERRIDE, 
	o_PremOpsTotalPremiumTable1PremiumOverride AS PREMOPSTOTALPREMIUMTABLE1PREMIUMOVERRIDE, 
	o_PremOpsTotalPremiumTable2PremiumOverride AS PREMOPSTOTALPREMIUMTABLE2PREMIUMOVERRIDE, 
	o_PremOpsTotalPremiumTable3PremiumOverride AS PREMOPSTOTALPREMIUMTABLE3PREMIUMOVERRIDE, 
	o_ProductsTotalPremiumTableAPremiumOverride AS PRODUCTSTOTALPREMIUMTABLEAPREMIUMOVERRIDE, 
	o_ProductsTotalPremiumTableBPremiumOverride AS PRODUCTSTOTALPREMIUMTABLEBPREMIUMOVERRIDE, 
	o_ProductsTotalPremiumTableCPremiumOverride AS PRODUCTSTOTALPREMIUMTABLECPREMIUMOVERRIDE, 
	ALLOTHERREVISEDPREMIUM, 
	PREMOPSTOTALPREMIUMTABLE1REVISEDPREMIUM, 
	PREMOPSTOTALPREMIUMTABLE2REVISEDPREMIUM, 
	PREMOPSTOTALPREMIUMTABLE3REVISEDPREMIUM, 
	PRODUCTSTOTALPREMIUMTABLEAREVISEDPREMIUM, 
	PRODUCTSTOTALPREMIUMTABLEBREVISEDPREMIUM, 
	PRODUCTSTOTALPREMIUMTABLECREVISEDPREMIUM, 
	SCHEDULEDMODIFICATION, 
	o_IncludeCGL AS INCLUDECGL, 
	o_IncludeOhioStopGapLiability AS INCLUDEOHIOSTOPGAPLIABILITY, 
	PREMIUM, 
	PREMIUMCHANGE, 
	PREMIUMWRITTEN, 
	IDFIELD, 
	UMBRELLASBOPSCHEDULEDMODIFICATIONLOCATIONINCOMMENT, 
	UMBRELLASBOPSCHEDULEDMODIFICATIONLOCATIONINMODIFICATION, 
	UMBRELLASBOPSCHEDULEDMODIFICATIONLOCATIONOUTCOMMENT, 
	UMBRELLASBOPSCHEDULEDMODIFICATIONLOCATIONOUTMODIFICATION, 
	UMBRELLASBOPSCHEDULEDMODIFICATIONPREMISESCOMMENT, 
	UMBRELLASBOPSCHEDULEDMODIFICATIONPREMISESMODIFICATION, 
	UMBRELLASBOPSCHEDULEDMODIFICATIONEQUIPMENTCOMMENT, 
	UMBRELLASBOPSCHEDULEDMODIFICATIONEQUIPMENTMODIFICATION, 
	UMBRELLASBOPSCHEDULEDMODIFICATIONMANAGEMENTCOMMENT, 
	UMBRELLASBOPSCHEDULEDMODIFICATIONMANAGEMENTMODIFICATION, 
	UMBRELLASBOPSCHEDULEDMODIFICATIONEMPLOYEESCOMMENT, 
	UMBRELLASBOPSCHEDULEDMODIFICATIONEMPLOYEESMODIFICATION, 
	UMBRELLASBOPSCHEDULEDMODIFICATIONCOOPERATIONMEDICALCOMMENT, 
	UMBRELLASBOPSCHEDULEDMODIFICATIONCOOPERATIONMEDICALMODIFICATION, 
	UMBRELLASBOPSCHEDULEDMODIFICATIONCOOPERATIONSAFETYCOMMENT, 
	UMBRELLASBOPSCHEDULEDMODIFICATIONCOOPERATIONSAFETYMODIFICATION, 
	MODIFICATIONTOTAL, 
	MODIFICATIONTOTALFORSBOPBUSINESSDETAILPAGE, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),
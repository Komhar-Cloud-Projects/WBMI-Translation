WITH
SQ_WB_CU_UmbrellaSMARTbusiness AS (
	WITH cte_WBCUUmbrellaSMARTbusiness(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.WB_CU_UmbrellaSMARTbusinessId, 
	X.SessionId, 
	X.Deleted, 
	X.Description, 
	X.EffectiveDate, 
	X.ExpirationDate, 
	X.PersonalLiability, 
	X.PolicyNumber, 
	X.ProfessionalLiabilityAggregateLimit, 
	X.SMARTbusinessPremium, 
	X.FirstMillionBasePremium, 
	X.FirstMillionModifiedPremium, 
	X.RevisedPremium, 
	X.CoverageForm, 
	X.IncludeProfessionalLiability, 
	X.ScheduledModification, 
	X.Override, 
	X.Premium, 
	X.PremiumChange, 
	X.PremiumWritten, 
	X.IDField, 
	X.UmbrellaSMARTBusinessScheduledModificationLocationInComment, 
	X.UmbrellaSMARTBusinessScheduledModificationLocationInModification, 
	X.UmbrellaSMARTBusinessScheduledModificationLocationOutComment, 
	X.UmbrellaSMARTBusinessScheduledModificationLocationOutModification, 
	X.UmbrellaSMARTBusinessScheduledModificationPremisesComment, 
	X.UmbrellaSMARTBusinessScheduledModificationPremisesModification, 
	X.UmbrellaSMARTBusinessScheduledModificationEquipmentComment, 
	X.UmbrellaSMARTBusinessScheduledModificationEquipmentModification, 
	X.UmbrellaSMARTBusinessScheduledModificationManagementComment, 
	X.UmbrellaSMARTBusinessScheduledModificationManagementModification, 
	X.UmbrellaSMARTBusinessScheduledModificationEmployeesComment, 
	X.UmbrellaSMARTBusinessScheduledModificationEmployeesModification, 
	X.UmbrellaSMARTBusinessScheduledModificationCooperationMedicalComment, 
	X.UmbrellaSMARTBusinessScheduledModificationCooperationMedicalModification, 
	X.UmbrellaSMARTBusinessScheduledModificationCooperationSafetyComment, 
	X.UmbrellaSMARTBusinessScheduledModificationCooperationSafetyModification, 
	X.ModificationTotal, 
	X.ModificationTotalForSMARTBusinessDetailPage 
	FROM
	WB_CU_UmbrellaSMARTbusiness X
	inner join
	cte_WBCUUmbrellaSMARTbusiness Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	LineId,
	WB_CU_UmbrellaSMARTbusinessId,
	SessionId,
	Deleted AS i_Deleted,
	-- *INF*: DECODE(i_Deleted,'T',1,'F',0,NULL)
	DECODE(
	    i_Deleted,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Deleted,
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
	ProfessionalLiabilityAggregateLimit,
	SMARTbusinessPremium,
	FirstMillionBasePremium,
	FirstMillionModifiedPremium,
	RevisedPremium,
	CoverageForm,
	IncludeProfessionalLiability AS i_IncludeProfessionalLiability,
	-- *INF*: DECODE(i_IncludeProfessionalLiability,'T',1,'F',0,NULL)
	DECODE(
	    i_IncludeProfessionalLiability,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IncludeProfessionalLiability,
	ScheduledModification,
	Override AS i_Override,
	-- *INF*: DECODE(i_Override,'T',1,'F',0,NULL)
	DECODE(
	    i_Override,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Override,
	Premium,
	PremiumChange,
	PremiumWritten,
	IDField,
	UmbrellaSMARTBusinessScheduledModificationLocationInComment,
	UmbrellaSMARTBusinessScheduledModificationLocationInModification,
	UmbrellaSMARTBusinessScheduledModificationLocationOutComment,
	UmbrellaSMARTBusinessScheduledModificationLocationOutModification,
	UmbrellaSMARTBusinessScheduledModificationPremisesComment,
	UmbrellaSMARTBusinessScheduledModificationPremisesModification,
	UmbrellaSMARTBusinessScheduledModificationEquipmentComment,
	UmbrellaSMARTBusinessScheduledModificationEquipmentModification,
	UmbrellaSMARTBusinessScheduledModificationManagementComment,
	UmbrellaSMARTBusinessScheduledModificationManagementModification,
	UmbrellaSMARTBusinessScheduledModificationEmployeesComment,
	UmbrellaSMARTBusinessScheduledModificationEmployeesModification,
	UmbrellaSMARTBusinessScheduledModificationCooperationMedicalComment,
	UmbrellaSMARTBusinessScheduledModificationCooperationMedicalModification,
	UmbrellaSMARTBusinessScheduledModificationCooperationSafetyComment,
	UmbrellaSMARTBusinessScheduledModificationCooperationSafetyModification,
	ModificationTotal,
	ModificationTotalForSMARTBusinessDetailPage,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_CU_UmbrellaSMARTbusiness
),
WBCUUmbrellaSMARTBusinessStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCUUmbrellaSMARTBusinessStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCUUmbrellaSMARTBusinessStage
	(LineId, WBCUUmbrellaSMARTbusinessId, SessionId, Deleted, Description, EffectiveDate, ExpirationDate, PersonalLiability, PolicyNumber, ProfessionalLiabilityAggregateLimit, SMARTbusinessPremium, FirstMillionBasePremium, FirstMillionModifiedPremium, RevisedPremium, CoverageForm, IncludeProfessionalLiability, ScheduledModification, Override, Premium, PremiumChange, PremiumWritten, IDField, UmbrellaSMARTBusinessScheduledModificationLocationInComment, UmbrellaSMARTBusinessScheduledModificationLocationInModification, UmbrellaSMARTBusinessScheduledModificationLocationOutComment, UmbrellaSMARTBusinessScheduledModificationLocationOutModification, UmbrellaSMARTBusinessScheduledModificationPremisesComment, UmbrellaSMARTBusinessScheduledModificationPremisesModification, UmbrellaSMARTBusinessScheduledModificationEquipmentComment, UmbrellaSMARTBusinessScheduledModificationEquipmentModification, UmbrellaSMARTBusinessScheduledModificationManagementComment, UmbrellaSMARTBusinessScheduledModificationManagementModification, UmbrellaSMARTBusinessScheduledModificationEmployeesComment, UmbrellaSMARTBusinessScheduledModificationEmployeesModification, UmbrellaSMARTBusinessScheduledModificationCooperationMedicalComment, UmbrellaSMARTBusinessScheduledModificationCooperationMedicalModification, UmbrellaSMARTBusinessScheduledModificationCooperationSafetyComment, UmbrellaSMARTBusinessScheduledModificationCooperationSafetyModification, ModificationTotal, ModificationTotalForSMARTBusinessDetailPage, ExtractDate, SourceSystemId)
	SELECT 
	LINEID, 
	WB_CU_UmbrellaSMARTbusinessId AS WBCUUMBRELLASMARTBUSINESSID, 
	SESSIONID, 
	o_Deleted AS DELETED, 
	DESCRIPTION, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	o_PersonalLiability AS PERSONALLIABILITY, 
	POLICYNUMBER, 
	PROFESSIONALLIABILITYAGGREGATELIMIT, 
	SMARTBUSINESSPREMIUM, 
	FIRSTMILLIONBASEPREMIUM, 
	FIRSTMILLIONMODIFIEDPREMIUM, 
	REVISEDPREMIUM, 
	COVERAGEFORM, 
	o_IncludeProfessionalLiability AS INCLUDEPROFESSIONALLIABILITY, 
	SCHEDULEDMODIFICATION, 
	o_Override AS OVERRIDE, 
	PREMIUM, 
	PREMIUMCHANGE, 
	PREMIUMWRITTEN, 
	IDFIELD, 
	UMBRELLASMARTBUSINESSSCHEDULEDMODIFICATIONLOCATIONINCOMMENT, 
	UMBRELLASMARTBUSINESSSCHEDULEDMODIFICATIONLOCATIONINMODIFICATION, 
	UMBRELLASMARTBUSINESSSCHEDULEDMODIFICATIONLOCATIONOUTCOMMENT, 
	UMBRELLASMARTBUSINESSSCHEDULEDMODIFICATIONLOCATIONOUTMODIFICATION, 
	UMBRELLASMARTBUSINESSSCHEDULEDMODIFICATIONPREMISESCOMMENT, 
	UMBRELLASMARTBUSINESSSCHEDULEDMODIFICATIONPREMISESMODIFICATION, 
	UMBRELLASMARTBUSINESSSCHEDULEDMODIFICATIONEQUIPMENTCOMMENT, 
	UMBRELLASMARTBUSINESSSCHEDULEDMODIFICATIONEQUIPMENTMODIFICATION, 
	UMBRELLASMARTBUSINESSSCHEDULEDMODIFICATIONMANAGEMENTCOMMENT, 
	UMBRELLASMARTBUSINESSSCHEDULEDMODIFICATIONMANAGEMENTMODIFICATION, 
	UMBRELLASMARTBUSINESSSCHEDULEDMODIFICATIONEMPLOYEESCOMMENT, 
	UMBRELLASMARTBUSINESSSCHEDULEDMODIFICATIONEMPLOYEESMODIFICATION, 
	UMBRELLASMARTBUSINESSSCHEDULEDMODIFICATIONCOOPERATIONMEDICALCOMMENT, 
	UMBRELLASMARTBUSINESSSCHEDULEDMODIFICATIONCOOPERATIONMEDICALMODIFICATION, 
	UMBRELLASMARTBUSINESSSCHEDULEDMODIFICATIONCOOPERATIONSAFETYCOMMENT, 
	UMBRELLASMARTBUSINESSSCHEDULEDMODIFICATIONCOOPERATIONSAFETYMODIFICATION, 
	MODIFICATIONTOTAL, 
	MODIFICATIONTOTALFORSMARTBUSINESSDETAILPAGE, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),
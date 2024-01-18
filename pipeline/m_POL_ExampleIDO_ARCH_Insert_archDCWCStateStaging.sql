WITH
SQ_DCWCStateStaging AS (
	SELECT
		LineId,
		WC_StateId,
		SessionId,
		Id,
		AnniversaryRating,
		CarrierType,
		EffectiveDate,
		NonRatableIncreasedLimits,
		NormalAnniversaryRatingDate,
		State,
		WorkplaceSafetyProgramNonEstablishedSurchargeIndicator,
		ExtractDate,
		SourceSystemId,
		ManualPremium,
		ModifiedPremium,
		SubjectPremium,
		TotalEstimatedPremium,
		TotalStatePremium,
		USLandHManualPremium
	FROM DCWCStateStaging
),
EXP_Metadata AS (
	SELECT
	LineId AS i_LineId,
	WC_StateId AS i_WC_StateId,
	SessionId AS i_SessionId,
	Id AS i_Id,
	AnniversaryRating AS i_AnniversaryRating,
	CarrierType AS i_CarrierType,
	EffectiveDate AS i_EffectiveDate,
	NonRatableIncreasedLimits AS i_NonRatableIncreasedLimits,
	NormalAnniversaryRatingDate AS i_NormalAnniversaryRatingDate,
	State AS i_State,
	WorkplaceSafetyProgramNonEstablishedSurchargeIndicator AS i_WorkplaceSafetyProgramNonEstablishedSurchargeIndicator,
	ExtractDate AS i_ExtractDate,
	SourceSystemId AS i_SourceSystemId,
	i_LineId AS o_LineId,
	i_WC_StateId AS o_WC_StateId,
	i_SessionId AS o_SessionId,
	i_Id AS o_Id,
	-- *INF*: DECODE(i_AnniversaryRating  ,'T',1,'F',0,NULL)
	DECODE(
	    i_AnniversaryRating,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AnniversaryRating,
	i_CarrierType AS o_CarrierType,
	i_EffectiveDate AS o_EffectiveDate,
	i_NonRatableIncreasedLimits AS o_NonRatableIncreasedLimits,
	i_NormalAnniversaryRatingDate AS o_NormalAnniversaryRatingDate,
	i_State AS o_State,
	-- *INF*: DECODE(i_WorkplaceSafetyProgramNonEstablishedSurchargeIndicator  ,'T',1,'F',0,NULL)
	DECODE(
	    i_WorkplaceSafetyProgramNonEstablishedSurchargeIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_WorkplaceSafetyProgramNonEstablishedSurchargeIndicator,
	i_ExtractDate AS o_ExtractDate,
	i_SourceSystemId AS o_SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	ManualPremium,
	ModifiedPremium,
	SubjectPremium,
	TotalEstimatedPremium,
	TotalStatePremium,
	USLandHManualPremium
	FROM SQ_DCWCStateStaging
),
archDCWCStateStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCWCStateStaging
	(LineId, WC_StateId, SessionId, Id, AnniversaryRating, CarrierType, EffectiveDate, NonRatableIncreasedLimits, NormalAnniversaryRatingDate, State, WorkplaceSafetyProgramNonEstablishedSurchargeIndicator, ExtractDate, SourceSystemId, AuditId, ManualPremium, ModifiedPremium, SubjectPremium, TotalEstimatedPremium, TotalStatePremium, USLandHManualPremium)
	SELECT 
	o_LineId AS LINEID, 
	o_WC_StateId AS WC_STATEID, 
	o_SessionId AS SESSIONID, 
	o_Id AS ID, 
	o_AnniversaryRating AS ANNIVERSARYRATING, 
	o_CarrierType AS CARRIERTYPE, 
	o_EffectiveDate AS EFFECTIVEDATE, 
	o_NonRatableIncreasedLimits AS NONRATABLEINCREASEDLIMITS, 
	o_NormalAnniversaryRatingDate AS NORMALANNIVERSARYRATINGDATE, 
	o_State AS STATE, 
	o_WorkplaceSafetyProgramNonEstablishedSurchargeIndicator AS WORKPLACESAFETYPROGRAMNONESTABLISHEDSURCHARGEINDICATOR, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	MANUALPREMIUM, 
	MODIFIEDPREMIUM, 
	SUBJECTPREMIUM, 
	TOTALESTIMATEDPREMIUM, 
	TOTALSTATEPREMIUM, 
	USLANDHMANUALPREMIUM
	FROM EXP_Metadata
),
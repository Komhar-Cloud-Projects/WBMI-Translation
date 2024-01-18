WITH
SQ_DC_WC_State AS (
	WITH cte_DCWCState(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.WC_StateId, 
	X.SessionId, 
	X.Id, 
	X.AnniversaryRating, 
	X.CarrierType, 
	X.EffectiveDate, 
	X.NonRatableIncreasedLimits, 
	X.NormalAnniversaryRatingDate, 
	X.State, 
	X.WorkplaceSafetyProgramNonEstablishedSurchargeIndicator,
	X.ManualPremium,
	X.ModifiedPremium,
	X.SubjectPremium,
	X.TotalEstimatedPremium,
	X.TotalStatePremium,
	X.USLandHManualPremium	  
	FROM
	DC_WC_State X
	inner join
	cte_DCWCState Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
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
	i_LineId AS o_LineId,
	i_WC_StateId AS o_WC_StateId,
	i_SessionId AS o_SessionId,
	i_Id AS o_Id,
	-- *INF*: DECODE(i_AnniversaryRating ,'T',1,'F',0,NULL)
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
	-- *INF*: DECODE(i_WorkplaceSafetyProgramNonEstablishedSurchargeIndicator ,'T',1,'F',0,NULL)
	DECODE(
	    i_WorkplaceSafetyProgramNonEstablishedSurchargeIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_WorkplaceSafetyProgramNonEstablishedSurchargeIndicator,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	ManualPremium,
	ModifiedPremium,
	SubjectPremium,
	TotalEstimatedPremium,
	TotalStatePremium,
	USLandHManualPremium
	FROM SQ_DC_WC_State
),
DCWCStateStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCWCStateStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCWCStateStaging
	(LineId, WC_StateId, SessionId, Id, AnniversaryRating, CarrierType, EffectiveDate, NonRatableIncreasedLimits, NormalAnniversaryRatingDate, State, WorkplaceSafetyProgramNonEstablishedSurchargeIndicator, ExtractDate, SourceSystemId, ManualPremium, ModifiedPremium, SubjectPremium, TotalEstimatedPremium, TotalStatePremium, USLandHManualPremium)
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
	MANUALPREMIUM, 
	MODIFIEDPREMIUM, 
	SUBJECTPREMIUM, 
	TOTALESTIMATEDPREMIUM, 
	TOTALSTATEPREMIUM, 
	USLANDHMANUALPREMIUM
	FROM EXP_Metadata
),
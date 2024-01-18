WITH
SQ_DC_CF_RatingGroup AS (
	WITH cte_DCCFRatingGroup(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CF_RiskId, 
	X.CF_RatingGroupId, 
	X.SessionId, 
	X.Id, 
	X.CauseOfLoss, 
	X.DeductibleIncreasedTheft, 
	X.DeductibleWindHail, 
	X.Earthquake, 
	X.EarthquakeLimit, 
	X.EarthquakeLimitOverrideSelect, 
	X.Flood, 
	X.FloodLimit, 
	X.FloodLimitCalc, 
	X.FloodLimitOverrideSelect, 
	X.HurricaneCalculationChoice, 
	X.HurricaneDeductible, 
	X.Number, 
	X.RatingType 
	FROM
	DC_CF_RatingGroup X
	inner join
	cte_DCCFRatingGroup Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CF_RiskId,
	CF_RatingGroupId,
	SessionId,
	Id,
	CauseOfLoss,
	DeductibleIncreasedTheft,
	DeductibleWindHail,
	Earthquake,
	-- *INF*: DECODE(Earthquake,'T',1,'F',0,NULL)
	DECODE(
	    Earthquake,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Earthquake,
	EarthquakeLimit,
	EarthquakeLimitOverrideSelect,
	-- *INF*: DECODE(EarthquakeLimitOverrideSelect,'T',1,'F',0,NULL)
	DECODE(
	    EarthquakeLimitOverrideSelect,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_EarthquakeLimitOverrideSelect,
	Flood,
	-- *INF*: DECODE(Flood,'T',1,'F',0,NULL)
	DECODE(
	    Flood,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Flood,
	FloodLimit,
	FloodLimitCalc,
	FloodLimitOverrideSelect,
	-- *INF*: DECODE(FloodLimitOverrideSelect,'T',1,'F',0,NULL)
	DECODE(
	    FloodLimitOverrideSelect,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_FloodLimitOverrideSelect,
	HurricaneCalculationChoice,
	HurricaneDeductible,
	Number,
	RatingType,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYS_ID,
	sysdate AS ExtractDate
	FROM SQ_DC_CF_RatingGroup
),
DCCFRatingGroupStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFRatingGroupStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFRatingGroupStaging
	(CF_RatingGroupId, SessionId, Id, CauseOfLoss, DeductibleIncreasedTheft, DeductibleWindHail, Earthquake, EarthquakeLimit, EarthquakeLimitOverrideSelect, Flood, FloodLimit, FloodLimitCalc, FloodLimitOverrideSelect, HurricaneCalculationChoice, HurricaneDeductible, Number, RatingType, ExtractDate, SourceSystemId, CF_RiskId)
	SELECT 
	CF_RATINGGROUPID, 
	SESSIONID, 
	ID, 
	CAUSEOFLOSS, 
	DEDUCTIBLEINCREASEDTHEFT, 
	DEDUCTIBLEWINDHAIL, 
	o_Earthquake AS EARTHQUAKE, 
	EARTHQUAKELIMIT, 
	o_EarthquakeLimitOverrideSelect AS EARTHQUAKELIMITOVERRIDESELECT, 
	o_Flood AS FLOOD, 
	FLOODLIMIT, 
	FLOODLIMITCALC, 
	o_FloodLimitOverrideSelect AS FLOODLIMITOVERRIDESELECT, 
	HURRICANECALCULATIONCHOICE, 
	HURRICANEDEDUCTIBLE, 
	NUMBER, 
	RATINGTYPE, 
	EXTRACTDATE, 
	SOURCE_SYS_ID AS SOURCESYSTEMID, 
	CF_RISKID
	FROM EXP_Metadata
),
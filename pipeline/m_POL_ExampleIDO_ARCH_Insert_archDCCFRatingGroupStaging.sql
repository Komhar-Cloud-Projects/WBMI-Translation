WITH
SQ_DCCFRatingGroupStaging AS (
	SELECT
		DCCFRatingGroupStagingId,
		CF_RatingGroupId,
		SessionId,
		Id,
		CauseOfLoss,
		DeductibleIncreasedTheft,
		DeductibleWindHail,
		Earthquake,
		EarthquakeLimit,
		EarthquakeLimitOverrideSelect,
		Flood,
		FloodLimit,
		FloodLimitCalc,
		FloodLimitOverrideSelect,
		HurricaneCalculationChoice,
		HurricaneDeductible,
		Number,
		RatingType,
		ExtractDate,
		SourceSystemId,
		CF_RiskId
	FROM DCCFRatingGroupStaging
),
EXP_Metadata AS (
	SELECT
	DCCFRatingGroupStagingId,
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
	ExtractDate,
	SourceSystemId,
	CF_RiskId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCFRatingGroupStaging
),
archDCCFRatingGroupStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCCFRatingGroupStaging
	(CF_RiskId, CF_RatingGroupId, SessionId, Id, CauseOfLoss, DeductibleIncreasedTheft, DeductibleWindHail, Earthquake, EarthquakeLimit, EarthquakeLimitOverrideSelect, Flood, FloodLimit, FloodLimitCalc, FloodLimitOverrideSelect, HurricaneCalculationChoice, HurricaneDeductible, Number, RatingType, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	CF_RISKID, 
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
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),
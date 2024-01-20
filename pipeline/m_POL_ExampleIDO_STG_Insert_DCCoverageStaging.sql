WITH
SQ_DC_Coverage AS (
	SELECT	X.ObjectId, 
			X.ObjectName, 
			X.CoverageId, 
			X.SessionId, 
			X.Id, 
			X.Type, 
			X.BaseRate, 
			X.BasePremium, 
			X.Premium, 
			X.Change, 
			X.Written, 
			X.Prior, 
			X.PriorTerm, 
			X.CancelPremium, 
			X.ExposureBasis, 
			X.FullEarnedIndicator, 
			X.LossCostModifier, 
			X.PremiumFE, 
			X.Deleted, 
			X.Indicator 
	FROM
			DC_Coverage X WITH(nolock)
	        INNER JOIN wbexampledata.dbo.wb_edwdataloadincrementalsessions Y WITH(
	                  nolock)
	               ON X.SessionId = Y.SessionId 
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	ObjectId,
	ObjectName,
	CoverageId,
	SessionId,
	Id,
	Type,
	BaseRate,
	BasePremium,
	Premium,
	Change,
	Written,
	Prior,
	PriorTerm,
	CancelPremium,
	ExposureBasis,
	FullEarnedIndicator,
	LossCostModifier,
	PremiumFE,
	Deleted AS i_Deleted,
	Indicator AS i_Indicator,
	-- *INF*: DECODE(i_Deleted, 'T', 1, 'F', 0, NULL)
	DECODE(
	    i_Deleted,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Deleted,
	-- *INF*: DECODE(i_Indicator, 'T', 1, 'F', 0, NULL)
	DECODE(
	    i_Indicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Indicator,
	-- *INF*: DECODE(ExposureBasis, 'T', 1, 'F', 0, NULL)
	DECODE(
	    ExposureBasis,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ExposureBasis,
	-- *INF*: DECODE(FullEarnedIndicator, 'T', 1, 'F', 0, NULL)
	DECODE(
	    FullEarnedIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_FullEarnedIndicator,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_Coverage
),
DCCoverageStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCoverageStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCoverageStaging
	(ObjectId, ObjectName, CoverageId, SessionId, Id, Type, BaseRate, BasePremium, Premium, Change, Written, Prior, PriorTerm, CancelPremium, ExposureBasis, FullEarnedIndicator, LossCostModifier, PremiumFE, ExtractDate, SourceSystemId, Deleted, Indicator)
	SELECT 
	OBJECTID, 
	OBJECTNAME, 
	COVERAGEID, 
	SESSIONID, 
	ID, 
	TYPE, 
	BASERATE, 
	BASEPREMIUM, 
	PREMIUM, 
	CHANGE, 
	WRITTEN, 
	PRIOR, 
	PRIORTERM, 
	CANCELPREMIUM, 
	o_ExposureBasis AS EXPOSUREBASIS, 
	o_FullEarnedIndicator AS FULLEARNEDINDICATOR, 
	LOSSCOSTMODIFIER, 
	PREMIUMFE, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_Deleted AS DELETED, 
	o_Indicator AS INDICATOR
	FROM EXP_Metadata
),
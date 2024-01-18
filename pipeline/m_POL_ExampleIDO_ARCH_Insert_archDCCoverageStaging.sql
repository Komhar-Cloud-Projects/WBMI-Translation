WITH
SQ_DCCoverageStaging AS (
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
		Deleted,
		Indicator,
		ExtractDate,
		SourceSystemId
	FROM DCCoverageStaging
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
	-- *INF*: DECODE(i_Deleted,'T', 1, 'F', 0, NULL)
	DECODE(
	    i_Deleted,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Deleted,
	-- *INF*: DECODE(i_Indicator,'T', 1, 'F', 0, NULL)
	DECODE(
	    i_Indicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Indicator,
	ExtractDate,
	SourceSystemId,
	-- *INF*: DECODE(ExposureBasis,'T', 1, 'F', 0, NULL)
	DECODE(
	    ExposureBasis,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ExposureBasis,
	-- *INF*: DECODE(FullEarnedIndicator,'T', 1, 'F', 0, NULL)
	DECODE(
	    FullEarnedIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_FullEarnedIndicator,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCoverageStaging
),
archDCCoverageStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCCoverageStaging
	(ObjectId, ObjectName, CoverageId, SessionId, Id, Type, BaseRate, BasePremium, Premium, Change, Written, Prior, PriorTerm, CancelPremium, ExposureBasis, FullEarnedIndicator, LossCostModifier, PremiumFE, ExtractDate, SourceSystemId, AuditId, Deleted, Indicator)
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
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	o_Deleted AS DELETED, 
	o_Indicator AS INDICATOR
	FROM EXP_Metadata
),
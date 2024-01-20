WITH
SQ_WBCALineStaging AS (
	SELECT
		WBCALineStagingId,
		ExtractDate,
		SourceSystemId,
		CA_LineId,
		WB_CA_LineId,
		SessionId,
		CheckWB1327,
		Commission,
		ConsentToRate,
		ContributionIndicator,
		OverrideIndicator,
		PolicyType,
		Override,
		RatingInfo1,
		RatingInfo2,
		Instructions1,
		Instructions2,
		GarageDealerClassCode,
		CoverageForm
	FROM WBCALineStaging
),
EXP_handle AS (
	SELECT
	WBCALineStagingId,
	ExtractDate,
	SourceSystemId,
	CA_LineId,
	WB_CA_LineId,
	SessionId,
	CheckWB1327,
	Commission,
	ConsentToRate,
	ContributionIndicator,
	OverrideIndicator,
	PolicyType,
	Override,
	RatingInfo1,
	RatingInfo2,
	Instructions1,
	Instructions2,
	GarageDealerClassCode,
	CoverageForm,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_auditid,
	-- *INF*: DECODE(ConsentToRate,'T',1,'F',0,NULL)
	DECODE(
	    ConsentToRate,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ConsentToRate,
	-- *INF*: DECODE(ContributionIndicator,'T',1,'F',0,NULL)
	DECODE(
	    ContributionIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ContributionIndicator,
	-- *INF*: DECODE(OverrideIndicator,'T',1,'F',0,NULL)
	DECODE(
	    OverrideIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_OverrideIndicator,
	-- *INF*: DECODE(Override,'T',1,'F',0,NULL)
	DECODE(
	    Override,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Override
	FROM SQ_WBCALineStaging
),
archWBCALineStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archWBCALineStaging
	(ExtractDate, SourceSystemId, AuditId, WBCALineStagingId, CA_LineId, WB_CA_LineId, SessionId, CheckWB1327, Commission, ConsentToRate, ContributionIndicator, OverrideIndicator, PolicyType, Override, RatingInfo1, RatingInfo2, Instructions1, Instructions2, GarageDealerClassCode, CoverageForm)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_auditid AS AUDITID, 
	WBCALINESTAGINGID, 
	CA_LINEID, 
	WB_CA_LINEID, 
	SESSIONID, 
	CHECKWB1327, 
	COMMISSION, 
	o_ConsentToRate AS CONSENTTORATE, 
	o_ContributionIndicator AS CONTRIBUTIONINDICATOR, 
	o_OverrideIndicator AS OVERRIDEINDICATOR, 
	POLICYTYPE, 
	o_Override AS OVERRIDE, 
	RATINGINFO1, 
	RATINGINFO2, 
	INSTRUCTIONS1, 
	INSTRUCTIONS2, 
	GARAGEDEALERCLASSCODE, 
	COVERAGEFORM
	FROM EXP_handle
),
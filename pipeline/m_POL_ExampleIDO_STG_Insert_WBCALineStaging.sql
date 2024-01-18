WITH
SQ_WB_CA_Line AS (
	WITH cte_WBCALine(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CA_LineId, 
	X.WB_CA_LineId, 
	X.SessionId, 
	X.CheckWB1327, 
	X.Commission, 
	X.ConsentToRate, 
	X.ContributionIndicator, 
	X.OverrideIndicator, 
	X.PolicyType, 
	X.Override, 
	X.RatingInfo1, 
	X.RatingInfo2, 
	X.Instructions1, 
	X.Instructions2,
	X.GarageDealerClassCode,
	X.CoverageForm 
	FROM
	WB_CA_Line X
	inner join
	cte_WBCALine Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_HANDLE AS (
	SELECT
	CA_LineId AS i_CA_LineId,
	WB_CA_LineId AS i_WB_CA_LineId,
	SessionId AS i_SessionId,
	CheckWB1327 AS i_CheckWB1327,
	Commission AS i_Commission,
	ConsentToRate AS i_ConsentToRate,
	ContributionIndicator AS i_ContributionIndicator,
	OverrideIndicator AS i_OverrideIndicator,
	PolicyType AS i_PolicyType,
	Override AS i_Override,
	RatingInfo1 AS i_RatingInfo1,
	RatingInfo2 AS i_RatingInfo2,
	Instructions1 AS i_Instructions1,
	Instructions2 AS i_Instructions2,
	GarageDealerClassCode AS i_GarageDealerClassCode,
	CoverageForm AS i_CoverageForm,
	sysdate AS o_Extracdate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_sourceSystemid,
	i_CA_LineId AS o_CA_LineId,
	i_WB_CA_LineId AS o_WB_CA_LineId,
	i_SessionId AS o_SessionId,
	i_CheckWB1327 AS o_CheckWB1327,
	i_Commission AS o_Commission,
	-- *INF*: decode(i_ConsentToRate,'T',1,'F',0,NULL)
	decode(
	    i_ConsentToRate,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ConsentToRate,
	-- *INF*: decode(i_ContributionIndicator,'T',1,'F',0,NULL)
	decode(
	    i_ContributionIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ContributionIndicator,
	-- *INF*: DECODE(i_OverrideIndicator,'T',1,'F',0,NULL)
	DECODE(
	    i_OverrideIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_OverrideIndicator,
	i_PolicyType AS o_PolicyType,
	-- *INF*: decode(i_Override,'T',1,'F',0,NULL)
	decode(
	    i_Override,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Override,
	i_RatingInfo1 AS o_RatingInfo1,
	i_RatingInfo2 AS o_RatingInfo2,
	i_Instructions1 AS o_Instructions1,
	i_Instructions2 AS o_Instructions2,
	i_GarageDealerClassCode AS o_GarageDealerClassCode,
	i_CoverageForm AS o_CoverageForm
	FROM SQ_WB_CA_Line
),
WBCALineStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCALineStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCALineStaging
	(ExtractDate, SourceSystemId, CA_LineId, WB_CA_LineId, SessionId, CheckWB1327, Commission, ConsentToRate, ContributionIndicator, OverrideIndicator, PolicyType, Override, RatingInfo1, RatingInfo2, Instructions1, Instructions2, GarageDealerClassCode, CoverageForm)
	SELECT 
	o_Extracdate AS EXTRACTDATE, 
	o_sourceSystemid AS SOURCESYSTEMID, 
	o_CA_LineId AS CA_LINEID, 
	o_WB_CA_LineId AS WB_CA_LINEID, 
	o_SessionId AS SESSIONID, 
	o_CheckWB1327 AS CHECKWB1327, 
	o_Commission AS COMMISSION, 
	o_ConsentToRate AS CONSENTTORATE, 
	o_ContributionIndicator AS CONTRIBUTIONINDICATOR, 
	o_OverrideIndicator AS OVERRIDEINDICATOR, 
	o_PolicyType AS POLICYTYPE, 
	o_Override AS OVERRIDE, 
	o_RatingInfo1 AS RATINGINFO1, 
	o_RatingInfo2 AS RATINGINFO2, 
	o_Instructions1 AS INSTRUCTIONS1, 
	o_Instructions2 AS INSTRUCTIONS2, 
	o_GarageDealerClassCode AS GARAGEDEALERCLASSCODE, 
	o_CoverageForm AS COVERAGEFORM
	FROM EXP_HANDLE
),
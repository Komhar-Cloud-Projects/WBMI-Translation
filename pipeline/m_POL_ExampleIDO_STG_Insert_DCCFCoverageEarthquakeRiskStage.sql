WITH
SQ_DC_CF_CoverageEarthquakeRisk AS (
	WITH cte_DCCFCoverageEarthquakeRisk(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.CF_CoverageEarthquakeRiskId, 
	X.SessionId, 
	X.AdditionalEarthquake, 
	X.ARate, 
	X.BaseRate, 
	X.LimitedEarthquake, 
	X.NetRate, 
	X.NetRateEE, 
	X.Prem, 
	X.PremiumRatingGroup, 
	X.SteelFrame 
	FROM
	DC_CF_CoverageEarthquakeRisk X
	inner join
	cte_DCCFCoverageEarthquakeRisk Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CoverageId,
	CF_CoverageEarthquakeRiskId,
	SessionId,
	AdditionalEarthquake AS i_AdditionalEarthquake,
	-- *INF*: DECODE(i_AdditionalEarthquake,'T','1','F','0')
	DECODE(
	    i_AdditionalEarthquake,
	    'T', '1',
	    'F', '0'
	) AS o_AdditionalEarthquake,
	ARate,
	BaseRate,
	LimitedEarthquake AS i_LimitedEarthquake,
	-- *INF*: DECODE(i_LimitedEarthquake,'T','1','F','0')
	DECODE(
	    i_LimitedEarthquake,
	    'T', '1',
	    'F', '0'
	) AS o_LimitedEarthquake,
	NetRate,
	NetRateEE,
	Prem,
	PremiumRatingGroup,
	SteelFrame AS i_SteelFrame,
	-- *INF*: DECODE(i_SteelFrame,'T','1','F','0')
	DECODE(
	    i_SteelFrame,
	    'T', '1',
	    'F', '0'
	) AS o_SteelFrame,
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CF_CoverageEarthquakeRisk
),
DCCFCoverageEarthquakeRiskStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFCoverageEarthquakeRiskStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFCoverageEarthquakeRiskStage
	(ExtractDate, SourceSystemId, CoverageId, CF_CoverageEarthquakeRiskId, SessionId, AdditionalEarthquake, ARate, BaseRate, LimitedEarthquake, NetRate, NetRateEE, Prem, PremiumRatingGroup, SteelFrame)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	COVERAGEID, 
	CF_COVERAGEEARTHQUAKERISKID, 
	SESSIONID, 
	o_AdditionalEarthquake AS ADDITIONALEARTHQUAKE, 
	ARATE, 
	BASERATE, 
	o_LimitedEarthquake AS LIMITEDEARTHQUAKE, 
	NETRATE, 
	NETRATEEE, 
	PREM, 
	PREMIUMRATINGGROUP, 
	o_SteelFrame AS STEELFRAME
	FROM EXP_Metadata
),
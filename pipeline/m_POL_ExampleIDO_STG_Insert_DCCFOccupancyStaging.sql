WITH
SQ_DC_CF_Occupancy AS (
	WITH cte_DCCFOccupancy(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CF_RiskId, 
	X.CF_OccupancyId, 
	X.SessionId, 
	X.Id, 
	X.OccupancyType, 
	X.ClassLimit, 
	X.ClassLimitOverride, 
	X.ClassLimitOverrideInput, 
	X.CSP, 
	X.CSPOverride, 
	X.Description, 
	X.DescriptionLA, 
	X.DescriptionMS, 
	X.DescriptionWA, 
	X.OccupancyTypeMonoline, 
	X.OccupancyTypeOverride, 
	X.ProtectionClassMultiplier, 
	X.RateGroup, 
	X.RateGroupOverride 
	FROM
	DC_CF_Occupancy X
	inner join
	cte_DCCFOccupancy Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CF_RiskId AS i_CF_RiskId,
	CF_OccupancyId AS i_CF_OccupancyId,
	SessionId AS i_SessionId,
	Id AS i_Id,
	OccupancyType AS i_OccupancyType,
	ClassLimit AS i_ClassLimit,
	ClassLimitOverride AS i_ClassLimitOverride,
	ClassLimitOverrideInput AS i_ClassLimitOverrideInput,
	CSP AS i_CSP,
	CSPOverride AS i_CSPOverride,
	Description AS i_Description,
	DescriptionLA AS i_DescriptionLA,
	DescriptionMS AS i_DescriptionMS,
	DescriptionWA AS i_DescriptionWA,
	OccupancyTypeMonoline AS i_OccupancyTypeMonoline,
	OccupancyTypeOverride AS i_OccupancyTypeOverride,
	ProtectionClassMultiplier AS i_ProtectionClassMultiplier,
	RateGroup AS i_RateGroup,
	RateGroupOverride AS i_RateGroupOverride,
	i_CF_OccupancyId AS o_CF_OccupancyId,
	i_SessionId AS o_SessionId,
	i_Id AS o_Id,
	i_OccupancyType AS o_OccupancyType,
	i_ClassLimit AS o_ClassLimit,
	i_ClassLimitOverride AS o_ClassLimitOverride,
	i_ClassLimitOverrideInput AS o_ClassLimitOverrideInput,
	i_CSP AS o_CSP,
	i_CSPOverride AS o_CSPOverride,
	i_Description AS o_Description,
	i_DescriptionLA AS o_DescriptionLA,
	i_DescriptionMS AS o_DescriptionMS,
	i_DescriptionWA AS o_DescriptionWA,
	i_OccupancyTypeMonoline AS o_OccupancyTypeMonoline,
	i_OccupancyTypeOverride AS o_OccupancyTypeOverride,
	-- *INF*: DECODE(i_ProtectionClassMultiplier,'T',1,'F',0,NULL)
	DECODE(
	    i_ProtectionClassMultiplier,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ProtectionClassMultiplier,
	i_RateGroup AS o_RateGroup,
	i_RateGroupOverride AS o_RateGroupOverride,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	i_CF_RiskId AS o_CF_RiskId
	FROM SQ_DC_CF_Occupancy
),
DCCFOccupancyStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFOccupancyStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFOccupancyStaging
	(CF_OccupancyId, SessionId, Id, OccupancyType, ClassLimit, ClassLimitOverride, ClassLimitOverrideInput, CSP, CSPOverride, Description, DescriptionLA, DescriptionMS, DescriptionWA, OccupancyTypeMonoline, OccupancyTypeOverride, ProtectionClassMultiplier, RateGroup, RateGroupOverride, ExtractDate, SourceSystemId, CF_RiskId)
	SELECT 
	o_CF_OccupancyId AS CF_OCCUPANCYID, 
	o_SessionId AS SESSIONID, 
	o_Id AS ID, 
	o_OccupancyType AS OCCUPANCYTYPE, 
	o_ClassLimit AS CLASSLIMIT, 
	o_ClassLimitOverride AS CLASSLIMITOVERRIDE, 
	o_ClassLimitOverrideInput AS CLASSLIMITOVERRIDEINPUT, 
	o_CSP AS CSP, 
	o_CSPOverride AS CSPOVERRIDE, 
	o_Description AS DESCRIPTION, 
	o_DescriptionLA AS DESCRIPTIONLA, 
	o_DescriptionMS AS DESCRIPTIONMS, 
	o_DescriptionWA AS DESCRIPTIONWA, 
	o_OccupancyTypeMonoline AS OCCUPANCYTYPEMONOLINE, 
	o_OccupancyTypeOverride AS OCCUPANCYTYPEOVERRIDE, 
	o_ProtectionClassMultiplier AS PROTECTIONCLASSMULTIPLIER, 
	o_RateGroup AS RATEGROUP, 
	o_RateGroupOverride AS RATEGROUPOVERRIDE, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_CF_RiskId AS CF_RISKID
	FROM EXP_Metadata
),
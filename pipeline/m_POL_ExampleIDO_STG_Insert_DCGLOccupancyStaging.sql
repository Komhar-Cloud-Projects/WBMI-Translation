WITH
SQ_DC_GL_Occupancy AS (
	WITH cte_DCGLOccupancy(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.GL_RiskId, 
	X.GL_OccupancyId, 
	X.SessionId, 
	X.Id, 
	X.Type, 
	X.ShortDescription, 
	X.GLClassCodeOverride, 
	X.GLPremiumBasisOverride, 
	X.OccupancyTypeMonoline, 
	X.GLClassCode, 
	X.GLPremiumBasis 
	FROM
	DC_GL_Occupancy X
	inner join
	cte_DCGLOccupancy Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	GL_RiskId,
	GL_OccupancyId,
	SessionId,
	Id,
	Type,
	ShortDescription,
	GLClassCodeOverride,
	GLPremiumBasisOverride,
	OccupancyTypeMonoline,
	GLClassCode,
	GLPremiumBasis,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_GL_Occupancy
),
DCGLOccupancyStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCGLOccupancyStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCGLOccupancyStaging
	(GL_RiskId, GL_OccupancyId, SessionId, Id, Type, ShortDescription, GLClassCodeOverride, GLPremiumBasisOverride, OccupancyTypeMonoline, GLClassCode, GLPremiumBasis, ExtractDate, SourceSystemId)
	SELECT 
	GL_RISKID, 
	GL_OCCUPANCYID, 
	SESSIONID, 
	ID, 
	TYPE, 
	SHORTDESCRIPTION, 
	GLCLASSCODEOVERRIDE, 
	GLPREMIUMBASISOVERRIDE, 
	OCCUPANCYTYPEMONOLINE, 
	GLCLASSCODE, 
	GLPREMIUMBASIS, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),
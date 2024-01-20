WITH
SQ_WB_GL_Risk AS (
	WITH cte_WBGLRisk(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.GL_RiskId, 
	X.WB_GL_RiskId, 
	X.SessionId, 
	X.XCUCoverage, 
	X.ExcludeExplosionCollapse1UndergroundAll, 
	X.ExcludeExplosionCollapse2AndOrUnderground, 
	X.IncludeExplosion, 
	X.IncludeCollapse, 
	X.IncludeUnderground, 
	X.ExcludeExplosionCollapse3AndOrUndergroundSpecOps, 
	X.ExcludeCollapse, 
	X.ExcludeExplosion, 
	X.ExcludeUnderground, 
	X.WestBendAuditable, 
	X.ILFTableAssignmentCode, 
	X.ProductsCompletedOpsTableAssignmentCode 
	FROM
	WB_GL_Risk X
	inner join
	cte_WBGLRisk Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	GL_RiskId AS i_GL_RiskId,
	WB_GL_RiskId AS i_WB_GL_RiskId,
	SessionId AS i_SessionId,
	XCUCoverage AS i_XCUCoverage,
	ExcludeExplosionCollapse1UndergroundAll AS i_ExcludeExplosionCollapse1UndergroundAll,
	ExcludeExplosionCollapse2AndOrUnderground AS i_ExcludeExplosionCollapse2AndOrUnderground,
	IncludeExplosion AS i_IncludeExplosion,
	IncludeCollapse AS i_IncludeCollapse,
	IncludeUnderground AS i_IncludeUnderground,
	ExcludeExplosionCollapse3AndOrUndergroundSpecOps AS i_ExcludeExplosionCollapse3AndOrUndergroundSpecOps,
	ExcludeCollapse AS i_ExcludeCollapse,
	ExcludeExplosion AS i_ExcludeExplosion,
	ExcludeUnderground AS i_ExcludeUnderground,
	WestBendAuditable AS i_WestBendAuditable,
	ILFTableAssignmentCode AS i_ILFTableAssignmentCode,
	ProductsCompletedOpsTableAssignmentCode AS i_ProductsCompletedOpsTableAssignmentCode,
	i_GL_RiskId AS o_GL_RiskId,
	i_WB_GL_RiskId AS o_WB_GL_RiskId,
	i_SessionId AS o_SessionId,
	-- *INF*: DECODE(i_XCUCoverage,'T',1,'F',0,NULL)
	DECODE(
	    i_XCUCoverage,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_XCUCoverage,
	-- *INF*: DECODE(i_ExcludeExplosionCollapse1UndergroundAll,'T',1,'F',0,NULL)
	DECODE(
	    i_ExcludeExplosionCollapse1UndergroundAll,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ExcludeExplosionCollapse1UndergroundAll,
	-- *INF*: DECODE(i_ExcludeExplosionCollapse2AndOrUnderground,'T',1,'F',0,NULL)
	DECODE(
	    i_ExcludeExplosionCollapse2AndOrUnderground,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ExcludeExplosionCollapse2AndOrUnderground,
	-- *INF*: DECODE(i_IncludeExplosion,'T',1,'F',0,NULL)
	DECODE(
	    i_IncludeExplosion,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IncludeExplosion,
	-- *INF*: DECODE(i_IncludeCollapse,'T',1,'F',0,NULL)
	DECODE(
	    i_IncludeCollapse,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IncludeCollapse,
	-- *INF*: DECODE(i_IncludeUnderground,'T',1,'F',0,NULL)
	DECODE(
	    i_IncludeUnderground,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IncludeUnderground,
	-- *INF*: DECODE(i_ExcludeExplosionCollapse3AndOrUndergroundSpecOps,'T',1,'F',0,NULL)
	DECODE(
	    i_ExcludeExplosionCollapse3AndOrUndergroundSpecOps,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ExcludeExplosionCollapse3AndOrUndergroundSpecOps,
	-- *INF*: DECODE(i_ExcludeCollapse,'T',1,'F',0,NULL)
	DECODE(
	    i_ExcludeCollapse,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ExcludeCollapse,
	-- *INF*: DECODE(i_ExcludeExplosion,'T',1,'F',0,NULL)
	DECODE(
	    i_ExcludeExplosion,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ExcludeExplosion,
	-- *INF*: DECODE(i_ExcludeUnderground,'T',1,'F',0,NULL)
	DECODE(
	    i_ExcludeUnderground,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ExcludeUnderground,
	-- *INF*: DECODE(i_WestBendAuditable,'T',1,'F',0,NULL)
	DECODE(
	    i_WestBendAuditable,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_WestBendAuditable,
	i_ILFTableAssignmentCode AS o_ILFTableAssignmentCode,
	i_ProductsCompletedOpsTableAssignmentCode AS o_ProductsCompletedOpsTableAssignmentCode,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_GL_Risk
),
WbGlRiskStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WbGlRiskStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WbGlRiskStage
	(GlRiskId, WbGlRiskId, SessionId, XCUCoverage, ExcludeExplosionCollapse1UndergroundAll, ExcludeExplosionCollapse2AndOrUnderground, IncludeExplosion, IncludeCollapse, IncludeUnderground, ExcludeExplosionCollapse3AndOrUndergroundSpecOps, ExcludeCollapse, ExcludeExplosion, ExcludeUnderground, WestBendAuditable, ILFTableAssignmentCode, ProductsCompletedOpsTableAssignmentCode, ExtractDate, SourceSystemId)
	SELECT 
	o_GL_RiskId AS GLRISKID, 
	o_WB_GL_RiskId AS WBGLRISKID, 
	o_SessionId AS SESSIONID, 
	o_XCUCoverage AS XCUCOVERAGE, 
	o_ExcludeExplosionCollapse1UndergroundAll AS EXCLUDEEXPLOSIONCOLLAPSE1UNDERGROUNDALL, 
	o_ExcludeExplosionCollapse2AndOrUnderground AS EXCLUDEEXPLOSIONCOLLAPSE2ANDORUNDERGROUND, 
	o_IncludeExplosion AS INCLUDEEXPLOSION, 
	o_IncludeCollapse AS INCLUDECOLLAPSE, 
	o_IncludeUnderground AS INCLUDEUNDERGROUND, 
	o_ExcludeExplosionCollapse3AndOrUndergroundSpecOps AS EXCLUDEEXPLOSIONCOLLAPSE3ANDORUNDERGROUNDSPECOPS, 
	o_ExcludeCollapse AS EXCLUDECOLLAPSE, 
	o_ExcludeExplosion AS EXCLUDEEXPLOSION, 
	o_ExcludeUnderground AS EXCLUDEUNDERGROUND, 
	o_WestBendAuditable AS WESTBENDAUDITABLE, 
	o_ILFTableAssignmentCode AS ILFTABLEASSIGNMENTCODE, 
	o_ProductsCompletedOpsTableAssignmentCode AS PRODUCTSCOMPLETEDOPSTABLEASSIGNMENTCODE, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),
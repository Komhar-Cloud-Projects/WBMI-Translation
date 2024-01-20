WITH
SQ_WbGlRiskStage AS (
	SELECT
		WbGlRiskStageId,
		GlRiskId,
		WbGlRiskId,
		SessionId,
		XCUCoverage,
		ExcludeExplosionCollapse1UndergroundAll,
		ExcludeExplosionCollapse2AndOrUnderground,
		IncludeExplosion,
		IncludeCollapse,
		IncludeUnderground,
		ExcludeExplosionCollapse3AndOrUndergroundSpecOps,
		ExcludeCollapse,
		ExcludeExplosion,
		ExcludeUnderground,
		WestBendAuditable,
		ILFTableAssignmentCode,
		ProductsCompletedOpsTableAssignmentCode,
		ExtractDate,
		SourceSystemId
	FROM WbGlRiskStage
),
EXP_Metadata AS (
	SELECT
	WbGlRiskStageId AS i_WbGlRiskStagingId,
	GlRiskId AS i_GlRiskId,
	WbGlRiskId AS i_WbGlRiskId,
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
	ExtractDate AS i_ExtractDate,
	SourceSystemId AS i_SourceSystemId,
	i_WbGlRiskStagingId AS o_WbGlRiskStagingId,
	i_GlRiskId AS o_GlRiskId,
	i_WbGlRiskId AS o_WbGlRiskId,
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
	i_ExtractDate AS o_ExtractDate,
	i_SourceSystemId AS o_SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WbGlRiskStage
),
ArchWBGlRiskStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBGlRiskStage
	(WbGlRiskStageId, GlRiskId, WbGlRiskId, SessionId, XCUCoverage, ExcludeExplosionCollapse1UndergroundAll, ExcludeExplosionCollapse2AndOrUnderground, IncludeExplosion, IncludeCollapse, IncludeUnderground, ExcludeExplosionCollapse3AndOrUndergroundSpecOps, ExcludeCollapse, ExcludeExplosion, ExcludeUnderground, WestBendAuditable, ILFTableAssignmentCode, ProductsCompletedOpsTableAssignmentCode, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	o_WbGlRiskStagingId AS WBGLRISKSTAGEID, 
	o_GlRiskId AS GLRISKID, 
	o_WbGlRiskId AS WBGLRISKID, 
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
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),
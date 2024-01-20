WITH
SQ_WBCoverageStage AS (
	SELECT
		WBCoverageStageId,
		ExtractDate,
		SourceSystemId,
		CoverageId,
		WBCoverageId,
		SessionId,
		Indicator,
		IndicatorbValue
	FROM WBCoverageStage
),
EXP_Metadata AS (
	SELECT
	WBCoverageStageId,
	ExtractDate,
	SourceSystemId,
	CoverageId,
	WBCoverageId,
	SessionId,
	Indicator AS i_Indicator,
	IndicatorbValue AS i_IndicatorbValue,
	-- *INF*: DECODE(i_Indicator,'T','1','F','0',NULL)
	DECODE(
	    i_Indicator,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_Indicator,
	-- *INF*: DECODE(i_IndicatorbValue,'T','1','F','0',NULL)
	DECODE(
	    i_IndicatorbValue,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_IndicatorbValue,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBCoverageStage
),
ArchWBCoverageStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBCoverageStage
	(ExtractDate, SourceSystemId, AuditId, WBCoverageStageId, CoverageId, WBCoverageId, SessionId, Indicator, IndicatorbValue)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBCOVERAGESTAGEID, 
	COVERAGEID, 
	WBCOVERAGEID, 
	SESSIONID, 
	o_Indicator AS INDICATOR, 
	o_IndicatorbValue AS INDICATORBVALUE
	FROM EXP_Metadata
),
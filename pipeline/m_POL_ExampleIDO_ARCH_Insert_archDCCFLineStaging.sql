WITH
SQ_DCCFLineStaging AS (
	SELECT
		DCCFLineStagingId,
		ExtractDate,
		SourceSystemId,
		LineId,
		CF_LineId,
		SessionId,
		ElectricalApparatus,
		ExpenseModFactor,
		FloodInceptionDate,
		FormsTentativeRates,
		StandardPolicy
	FROM DCCFLineStaging
),
EXP_Metadata AS (
	SELECT
	DCCFLineStagingId,
	ExtractDate,
	SourceSystemId,
	LineId,
	CF_LineId,
	SessionId,
	ElectricalApparatus AS i_ElectricalApparatus,
	-- *INF*: DECODE(i_ElectricalApparatus,'T',1,'F',0,NULL)
	DECODE(
	    i_ElectricalApparatus,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ElectricalApparatus,
	ExpenseModFactor,
	FloodInceptionDate,
	FormsTentativeRates AS i_FormsTentativeRates,
	-- *INF*: DECODE(i_FormsTentativeRates,'T',1,'F',0,NULL)
	DECODE(
	    i_FormsTentativeRates,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_FormsTentativeRates,
	StandardPolicy AS i_StandardPolicy,
	-- *INF*: DECODE(i_StandardPolicy,'T',1,'F',0,NULL)
	DECODE(
	    i_StandardPolicy,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_StandardPolicy,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCFLineStaging
),
archDCCFLineStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCCFLineStaging
	(ExtractDate, SourceSystemId, AuditId, DCCFLineStagingId, LineId, CF_LineId, SessionId, ElectricalApparatus, ExpenseModFactor, FloodInceptionDate, FormsTentativeRates, StandardPolicy)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCCFLINESTAGINGID, 
	LINEID, 
	CF_LINEID, 
	SESSIONID, 
	o_ElectricalApparatus AS ELECTRICALAPPARATUS, 
	EXPENSEMODFACTOR, 
	FLOODINCEPTIONDATE, 
	o_FormsTentativeRates AS FORMSTENTATIVERATES, 
	o_StandardPolicy AS STANDARDPOLICY
	FROM EXP_Metadata
),
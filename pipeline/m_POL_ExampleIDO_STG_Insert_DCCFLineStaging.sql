WITH
SQ_DC_CF_Line AS (
	WITH cte_DCCFLine(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.CF_LineId, 
	X.SessionId, 
	X.ElectricalApparatus, 
	X.ExpenseModFactor, 
	X.FloodInceptionDate, 
	X.FormsTentativeRates, 
	X.StandardPolicy 
	FROM
	DC_CF_Line X
	inner join
	cte_DCCFLine Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	LineId,
	CF_LineId,
	SessionId,
	ElectricalApparatus,
	ExpenseModFactor,
	FloodInceptionDate,
	FormsTentativeRates,
	StandardPolicy,
	-- *INF*: DECODE(ElectricalApparatus, 'T', 1, 'F', 0, NULL)
	DECODE(
	    ElectricalApparatus,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ElectricalApparatus,
	-- *INF*: DECODE(FormsTentativeRates, 'T', 1, 'F', 0, NULL)
	DECODE(
	    FormsTentativeRates,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_FormsTentativeRates,
	-- *INF*: DECODE(StandardPolicy, 'T', 1, 'F', 0, NULL)
	DECODE(
	    StandardPolicy,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_StandardPolicy,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CF_Line
),
DCCFLineStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFLineStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFLineStaging
	(ExtractDate, SourceSystemId, LineId, CF_LineId, SessionId, ElectricalApparatus, ExpenseModFactor, FloodInceptionDate, FormsTentativeRates, StandardPolicy)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
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
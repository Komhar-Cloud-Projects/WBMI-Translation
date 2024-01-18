WITH
SQ_DC_IM_Line AS (
	WITH cte_DCIMLine(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.IM_LineId, 
	X.SessionId, 
	X.Description, 
	X.PolicyPayment 
	FROM
	DC_IM_Line X
	inner join
	cte_DCIMLine Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_DCIMLineStage AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	LineId,
	IM_LineId,
	SessionId,
	Description,
	PolicyPayment
	FROM SQ_DC_IM_Line
),
DCIMLineStage AS (
	TRUNCATE TABLE DCIMLineStage;
	INSERT INTO DCIMLineStage
	(ExtractDate, SourceSystemId, LineId, IM_LineId, SessionId, Description, PolicyPayment)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	LINEID, 
	IM_LINEID, 
	SESSIONID, 
	DESCRIPTION, 
	POLICYPAYMENT
	FROM EXP_DCIMLineStage
),
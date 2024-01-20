WITH
SQ_WB_Reason AS (
	WITH cte_WBReason(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.TransactionId, 
	X.WB_ReasonId, 
	X.SessionId, 
	X.Code, 
	X.CodeCaption, 
	X.Description 
	FROM
	WB_Reason X
	inner join
	cte_WBReason Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	TransactionId,
	WB_ReasonId,
	SessionId,
	Code,
	CodeCaption,
	Description,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_Reason
),
WBReasonStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBReasonStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBReasonStaging
	(TransactionId, WB_ReasonId, SessionId, Code, CodeCaption, Description, ExtractDate, SourceSystemId)
	SELECT 
	TRANSACTIONID, 
	WB_REASONID, 
	SESSIONID, 
	CODE, 
	CODECAPTION, 
	DESCRIPTION, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),
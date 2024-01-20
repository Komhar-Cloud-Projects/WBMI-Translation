WITH
SQ_WB_CA_EndorsementWB516 AS (
	WITH cte_WBCAEndorsementWB516(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.WB_CoverageId, 
	X.WB_CA_EndorsementWB516Id, 
	X.SessionId, 
	X.RetroactiveDate, 
	X.NumberEmployees 
	FROM
	WB_CA_EndorsementWB516 X
	inner join
	cte_WBCAEndorsementWB516 Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemid,
	WB_CoverageId,
	WB_CA_EndorsementWB516Id,
	SessionId,
	RetroactiveDate,
	NumberEmployees
	FROM SQ_WB_CA_EndorsementWB516
),
WBCAEndorsementWB516Stage AS (
	TRUNCATE TABLE WBCAEndorsementWB516Stage;
	INSERT INTO WBCAEndorsementWB516Stage
	(ExtractDate, SourceSystemid, WB_CoverageId, WB_CA_EndorsementWB516Id, SessionId, RetroactiveDate, NumberEmployees)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemid AS SOURCESYSTEMID, 
	WB_COVERAGEID, 
	WB_CA_ENDORSEMENTWB516ID, 
	SESSIONID, 
	RETROACTIVEDATE, 
	NUMBEREMPLOYEES
	FROM EXP_Metadata
),
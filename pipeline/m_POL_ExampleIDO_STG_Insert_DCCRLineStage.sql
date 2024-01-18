WITH
SQ_DC_CR_Line AS (
	WITH cte_DCCRLine(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.CR_LineId, 
	X.SessionId, 
	X.Id, 
	X.AdditionalPremises, 
	X.CoverageType, 
	X.Description, 
	X.ERISARatableEmployees, 
	X.PolicyType, 
	X.TotalRatableEmployees 
	FROM
	DC_CR_Line X
	inner join
	cte_DCCRLine Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	LineId,
	CR_LineId,
	SessionId,
	Id,
	AdditionalPremises,
	CoverageType,
	Description,
	ERISARatableEmployees,
	PolicyType,
	TotalRatableEmployees,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CR_Line
),
DCCRLineStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCRLineStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCRLineStage
	(LineId, CrLineId, SessionId, Id, AdditionalPremises, CoverageType, Description, ERISARatableEmployees, PolicyType, TotalRatableEmployees, ExtractDate, SourceSystemId)
	SELECT 
	LINEID, 
	CR_LineId AS CRLINEID, 
	SESSIONID, 
	ID, 
	ADDITIONALPREMISES, 
	COVERAGETYPE, 
	DESCRIPTION, 
	ERISARATABLEEMPLOYEES, 
	POLICYTYPE, 
	TOTALRATABLEEMPLOYEES, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),
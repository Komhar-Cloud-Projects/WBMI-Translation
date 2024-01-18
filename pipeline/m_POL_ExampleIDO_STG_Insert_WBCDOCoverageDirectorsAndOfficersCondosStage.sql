WITH
SQ_WB_CDO_CoverageDirectorsAndOfficersCondos AS (
	WITH cte_WBCDOCoverageDirectorsAndOfficersCondos(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.WB_CDO_CoverageDirectorsAndOfficersCondosId, 
	X.SessionId, 
	X.RiskType, 
	X.NumberOfUnits, 
	X.RetroactiveDate 
	FROM
	WB_CDO_CoverageDirectorsAndOfficersCondos X
	inner join
	cte_WBCDOCoverageDirectorsAndOfficersCondos Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CoverageId,
	WB_CDO_CoverageDirectorsAndOfficersCondosId,
	SessionId,
	RiskType,
	NumberOfUnits,
	RetroactiveDate,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_CDO_CoverageDirectorsAndOfficersCondos
),
WBCDOCoverageDirectorsAndOfficersCondosStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCDOCoverageDirectorsAndOfficersCondosStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCDOCoverageDirectorsAndOfficersCondosStage
	(ExtractDate, SourceSystemId, CoverageId, WB_CDO_CoverageDirectorsAndOfficersCondosId, SessionId, RiskType, NumberOfUnits, RetroactiveDate)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	COVERAGEID, 
	WB_CDO_COVERAGEDIRECTORSANDOFFICERSCONDOSID, 
	SESSIONID, 
	RISKTYPE, 
	NUMBEROFUNITS, 
	RETROACTIVEDATE
	FROM EXP_Metadata
),
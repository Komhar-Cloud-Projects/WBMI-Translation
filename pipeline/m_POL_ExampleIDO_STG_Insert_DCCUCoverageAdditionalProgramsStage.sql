WITH
SQ_DC_CU_CoverageAdditionalPrograms AS (
	WITH cte_DCCUCoverageAdditionalPrograms(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.CU_CoverageAdditionalProgramsId, 
	X.SessionId, 
	X.AdditionalCoveredPrograms, 
	X.RetroActiveDate 
	FROM
	DC_CU_CoverageAdditionalPrograms X
	inner join
	cte_DCCUCoverageAdditionalPrograms Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CoverageId,
	CU_CoverageAdditionalProgramsId,
	SessionId,
	AdditionalCoveredPrograms,
	RetroActiveDate,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CU_CoverageAdditionalPrograms
),
DCCUCoverageAdditionalProgramsStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCUCoverageAdditionalProgramsStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCUCoverageAdditionalProgramsStage
	(CoverageId, CUCoverageAdditionalProgramsId, SessionId, AdditionalCoveredPrograms, RetroActiveDate, ExtractDate, SourceSystemId)
	SELECT 
	COVERAGEID, 
	CU_CoverageAdditionalProgramsId AS CUCOVERAGEADDITIONALPROGRAMSID, 
	SESSIONID, 
	ADDITIONALCOVEREDPROGRAMS, 
	RETROACTIVEDATE, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),
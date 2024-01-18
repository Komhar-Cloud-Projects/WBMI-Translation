WITH
SQ_DC_BP_CoverageEmployeeDishonesty AS (
	WITH cte_dcbpcovemp(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.BP_CoverageEmployeeDishonestyId, 
	X.SessionId,
	X.Arate, 
	X.Employees
	FROM
	DC_BP_CoverageEmployeeDishonesty X
	inner join
	cte_dcbpcovemp Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_MetaData AS (
	SELECT
	CoverageId,
	BP_CoverageEmployeeDishonestyId,
	SessionId,
	Arate,
	Employees,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_BP_CoverageEmployeeDishonesty
),
DCBPCoverageEmployeeDishonestyStage AS (
	TRUNCATE TABLE DCBPCoverageEmployeeDishonestyStage;
	INSERT INTO DCBPCoverageEmployeeDishonestyStage
	(ExtractDate, SourceSystemid, CoverageId, BP_CoverageEmployeeDishonestyId, SessionId, Arate, Employees)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	COVERAGEID, 
	BP_COVERAGEEMPLOYEEDISHONESTYID, 
	SESSIONID, 
	ARATE, 
	EMPLOYEES
	FROM EXP_MetaData
),
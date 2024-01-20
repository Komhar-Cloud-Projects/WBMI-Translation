WITH
SQ_DC_GL_CoverageLiquorLiability AS (
	WITH cte_DCGLCoverageLiquorLiability(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.GL_LocationId, 
	X.GL_CoverageLiquorLiabilityId, 
	X.SessionId, 
	X.Id, 
	X.GL_LocationXmlId, 
	X.Auditable, 
	X.ExtendedReportingPeriod, 
	X.ExtendedReportingPeriodPremium, 
	X.LiquorExcludeDeductible, 
	X.Exposure, 
	X.ExposureAudited, 
	X.ExposureEstimated 
	FROM
	DC_GL_CoverageLiquorLiability X
	inner join
	cte_DCGLCoverageLiquorLiability Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CoverageId,
	GL_LocationId,
	GL_CoverageLiquorLiabilityId,
	SessionId,
	Id,
	GL_LocationXmlId,
	Auditable AS i_Auditable,
	-- *INF*: IIF(i_Auditable='T','1','0')
	IFF(i_Auditable = 'T', '1', '0') AS o_Auditable,
	ExtendedReportingPeriod AS i_ExtendedReportingPeriod,
	-- *INF*: IIF(i_ExtendedReportingPeriod='T','1','0')
	IFF(i_ExtendedReportingPeriod = 'T', '1', '0') AS o_ExtendedReportingPeriod,
	ExtendedReportingPeriodPremium,
	LiquorExcludeDeductible AS i_LiquorExcludeDeductible,
	-- *INF*: IIF(i_LiquorExcludeDeductible='T','1','0')
	IFF(i_LiquorExcludeDeductible = 'T', '1', '0') AS o_LiquorExcludeDeductible,
	Exposure,
	ExposureAudited,
	ExposureEstimated,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_GL_CoverageLiquorLiability
),
DCGLCoverageLiquorLiabilityStaging AS (
	TRUNCATE TABLE DCGLCoverageLiquorLiabilityStaging;
	INSERT INTO DCGLCoverageLiquorLiabilityStaging
	(CoverageId, GL_LocationId, GL_CoverageLiquorLiabilityId, SessionId, Id, GL_LocationXmlId, Auditable, ExtendedReportingPeriod, ExtendedReportingPeriodPremium, LiquorExcludeDeductible, Exposure, ExposureAudited, ExposureEstimated, ExtractDate, SourceSystemId)
	SELECT 
	COVERAGEID, 
	GL_LOCATIONID, 
	GL_COVERAGELIQUORLIABILITYID, 
	SESSIONID, 
	ID, 
	GL_LOCATIONXMLID, 
	o_Auditable AS AUDITABLE, 
	o_ExtendedReportingPeriod AS EXTENDEDREPORTINGPERIOD, 
	EXTENDEDREPORTINGPERIODPREMIUM, 
	o_LiquorExcludeDeductible AS LIQUOREXCLUDEDEDUCTIBLE, 
	EXPOSURE, 
	EXPOSUREAUDITED, 
	EXPOSUREESTIMATED, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),
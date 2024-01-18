WITH
SQ_DCGLCoverageLiquorLiabilityStaging AS (
	SELECT
		CoverageId,
		GL_LocationId,
		GL_CoverageLiquorLiabilityId,
		SessionId,
		Id,
		GL_LocationXmlId,
		Auditable,
		ExtendedReportingPeriod,
		ExtendedReportingPeriodPremium,
		LiquorExcludeDeductible,
		Exposure,
		ExposureAudited,
		ExposureEstimated,
		ExtractDate,
		SourceSystemId
	FROM DCGLCoverageLiquorLiabilityStaging
),
EXPTRANS AS (
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
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_DCGLCoverageLiquorLiabilityStaging
),
archDCGLCoverageLiquorLiabilityStaging AS (
	INSERT INTO archDCGLCoverageLiquorLiabilityStaging
	(CoverageId, GL_LocationId, GL_CoverageLiquorLiabilityId, SessionId, Id, GL_LocationXmlId, Auditable, ExtendedReportingPeriod, ExtendedReportingPeriodPremium, LiquorExcludeDeductible, Exposure, ExposureAudited, ExposureEstimated, ExtractDate, SourceSystemId, AuditId)
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
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID
	FROM EXPTRANS
),
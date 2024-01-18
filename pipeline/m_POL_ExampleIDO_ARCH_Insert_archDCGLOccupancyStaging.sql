WITH
SQ_DCGLOccupancyStaging AS (
	SELECT
		GL_RiskId,
		GL_OccupancyId,
		SessionId,
		Id,
		Type,
		ShortDescription,
		GLClassCodeOverride,
		GLPremiumBasisOverride,
		OccupancyTypeMonoline,
		GLClassCode,
		GLPremiumBasis,
		ExtractDate,
		SourceSystemId
	FROM DCGLOccupancyStaging
),
EXP_Metadata AS (
	SELECT
	GL_RiskId,
	GL_OccupancyId,
	SessionId,
	Id,
	Type,
	ShortDescription,
	GLClassCodeOverride,
	GLPremiumBasisOverride,
	OccupancyTypeMonoline,
	GLClassCode,
	GLPremiumBasis,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCGLOccupancyStaging
),
archDCGLOccupancyStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCGLOccupancyStaging
	(GL_RiskId, GL_OccupancyId, SessionId, Id, Type, ShortDescription, GLClassCodeOverride, GLPremiumBasisOverride, OccupancyTypeMonoline, GLClassCode, GLPremiumBasis, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	GL_RISKID, 
	GL_OCCUPANCYID, 
	SESSIONID, 
	ID, 
	TYPE, 
	SHORTDESCRIPTION, 
	GLCLASSCODEOVERRIDE, 
	GLPREMIUMBASISOVERRIDE, 
	OCCUPANCYTYPEMONOLINE, 
	GLCLASSCODE, 
	GLPREMIUMBASIS, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),
WITH
SQ_WBGLReinsuranceStage AS (
	SELECT
		WBGLReinsuranceStageId,
		ExtractDate,
		SourceSystemId,
		WBCLReinsuranceId,
		WBGLReinsuranceId,
		SessionId,
		SpecialConditionsIncluded,
		SpecialConditionsExcluded,
		SpecialConditionsAnnotation,
		GrossReinsurancePremiumOCPGLSBOP,
		GrossReinsurancePremiumRR
	FROM WBGLReinsuranceStage1
),
EXP_Metadata AS (
	SELECT
	WBGLReinsuranceStageId,
	ExtractDate,
	SourceSystemId,
	WBCLReinsuranceId,
	WBGLReinsuranceId,
	SessionId,
	SpecialConditionsIncluded,
	SpecialConditionsExcluded,
	SpecialConditionsAnnotation,
	GrossReinsurancePremiumOCPGLSBOP,
	GrossReinsurancePremiumRR,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_WBGLReinsuranceStage
),
ArchWBGLReinsuranceStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBGLReinsuranceStage
	(ExtractDate, SourceSystemId, AuditId, WBCLReinsuranceId, WBGLReinsuranceId, SessionId, SpecialConditionsIncluded, SpecialConditionsExcluded, SpecialConditionsAnnotation, GrossReinsurancePremiumOCPGLSBOP, GrossReinsurancePremiumRR)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID, 
	WBCLREINSURANCEID, 
	WBGLREINSURANCEID, 
	SESSIONID, 
	SPECIALCONDITIONSINCLUDED, 
	SPECIALCONDITIONSEXCLUDED, 
	SPECIALCONDITIONSANNOTATION, 
	GROSSREINSURANCEPREMIUMOCPGLSBOP, 
	GROSSREINSURANCEPREMIUMRR
	FROM EXP_Metadata
),
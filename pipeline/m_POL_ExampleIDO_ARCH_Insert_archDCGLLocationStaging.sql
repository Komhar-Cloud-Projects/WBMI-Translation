WITH
SQ_DCGLLocationStaging AS (
	SELECT
		GL_LocationId,
		SessionId,
		Id,
		ExcludeCoverageCMedicalPayments,
		TerrorismTerritory,
		Territory,
		ExtractDate,
		SourceSystemId,
		Description,
		Number
	FROM DCGLLocationStaging
),
EXP_Metadata AS (
	SELECT
	GL_LocationId,
	SessionId,
	Id,
	ExcludeCoverageCMedicalPayments,
	TerrorismTerritory,
	Territory,
	ExtractDate,
	SourceSystemId,
	-- *INF*: DECODE(ExcludeCoverageCMedicalPayments,'T',1,'F',0,NULL)
	DECODE(
	    ExcludeCoverageCMedicalPayments,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ExcludeCoverageCMedicalPayments,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	Description,
	Number
	FROM SQ_DCGLLocationStaging
),
archDCGLLocationStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCGLLocationStaging
	(GL_LocationId, SessionId, Id, ExcludeCoverageCMedicalPayments, TerrorismTerritory, Territory, ExtractDate, SourceSystemId, AuditId, Description, Number)
	SELECT 
	GL_LOCATIONID, 
	SESSIONID, 
	ID, 
	o_ExcludeCoverageCMedicalPayments AS EXCLUDECOVERAGECMEDICALPAYMENTS, 
	TERRORISMTERRITORY, 
	TERRITORY, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DESCRIPTION, 
	NUMBER
	FROM EXP_Metadata
),
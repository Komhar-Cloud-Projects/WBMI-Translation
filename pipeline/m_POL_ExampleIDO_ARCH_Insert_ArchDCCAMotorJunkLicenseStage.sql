WITH
SQ_DCCAMotorJunkLicenseStage AS (
	SELECT
		DCCAMotorJunkLicenseStageId,
		ExtractDate,
		SourceSystemId,
		CA_StateId,
		CA_MotorJunkLicenseId,
		SessionId,
		Id,
		CertificateOfInsurance,
		Territory
	FROM DCCAMotorJunkLicenseStage
),
EXP_Metadata AS (
	SELECT
	DCCAMotorJunkLicenseStageId,
	ExtractDate,
	SourceSystemId,
	CA_StateId,
	CA_MotorJunkLicenseId,
	SessionId,
	Id,
	CertificateOfInsurance,
	Territory,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCAMotorJunkLicenseStage
),
ArchDCCAMotorJunkLicenseStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCAMotorJunkLicenseStage
	(ExtractDate, SourceSystemId, AuditId, DCCAMotorJunkLicenseStageId, CA_StateId, CA_MotorJunkLicenseId, SessionId, Id, CertificateOfInsurance, Territory)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCCAMOTORJUNKLICENSESTAGEID, 
	CA_STATEID, 
	CA_MOTORJUNKLICENSEID, 
	SESSIONID, 
	ID, 
	CERTIFICATEOFINSURANCE, 
	TERRITORY
	FROM EXP_Metadata
),
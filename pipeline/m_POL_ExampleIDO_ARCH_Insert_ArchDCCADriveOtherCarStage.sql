WITH
SQ_DCCADriveOtherCarStage AS (
	SELECT
		DCCADriveOtherCarStageId,
		ExtractDate,
		SourceSystemId,
		CA_StateId,
		CA_DriveOtherCarId,
		SessionId,
		Id,
		CertificateOfInsurance,
		NumberOfEmployeesEstimate,
		RiskDOCStacked,
		RiskDOCUIMStacked,
		Territory
	FROM DCCADriveOtherCarStage
),
EXP_Metadata AS (
	SELECT
	DCCADriveOtherCarStageId,
	ExtractDate,
	SourceSystemId,
	CA_StateId,
	CA_DriveOtherCarId,
	SessionId,
	Id,
	CertificateOfInsurance,
	NumberOfEmployeesEstimate,
	RiskDOCStacked AS i_RiskDOCStacked,
	RiskDOCUIMStacked AS i_RiskDOCUIMStacked,
	-- *INF*: DECODE(i_RiskDOCStacked, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_RiskDOCStacked,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_RiskDOCStacked,
	-- *INF*: DECODE(i_RiskDOCUIMStacked, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_RiskDOCUIMStacked,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_RiskDOCUIMStacked,
	Territory,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCADriveOtherCarStage
),
ArchDCCADriveOtherCarStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCADriveOtherCarStage
	(ExtractDate, SourceSystemId, AuditId, DCCADriveOtherCarStageId, CA_StateId, CA_DriveOtherCarId, SessionId, Id, CertificateOfInsurance, NumberOfEmployeesEstimate, RiskDOCStacked, RiskDOCUIMStacked, Territory)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCCADRIVEOTHERCARSTAGEID, 
	CA_STATEID, 
	CA_DRIVEOTHERCARID, 
	SESSIONID, 
	ID, 
	CERTIFICATEOFINSURANCE, 
	NUMBEROFEMPLOYEESESTIMATE, 
	o_RiskDOCStacked AS RISKDOCSTACKED, 
	o_RiskDOCUIMStacked AS RISKDOCUIMSTACKED, 
	TERRITORY
	FROM EXP_Metadata
),
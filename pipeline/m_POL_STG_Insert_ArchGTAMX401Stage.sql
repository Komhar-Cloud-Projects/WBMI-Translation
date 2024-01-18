WITH
SQ_GTAMX401Stage AS (
	SELECT
		GTAMX401StageId,
		ExtractDate,
		SourceSystemId,
		TableField,
		KeyLength,
		LocationCode,
		MasterCompanyNumber,
		TypeBureauCode,
		MajorPerilCode,
		CoverageCode,
		OutputDataLength,
		BureauCoverageCode,
		DecutibleType,
		DecutibleAmount,
		SublineCode
	FROM GTAMX401Stage
),
EXP_Set_AuditID AS (
	SELECT
	GTAMX401StageId,
	ExtractDate,
	SourceSystemId,
	TableField,
	KeyLength,
	LocationCode,
	MasterCompanyNumber,
	TypeBureauCode,
	MajorPerilCode,
	CoverageCode,
	OutputDataLength,
	BureauCoverageCode,
	DecutibleType,
	DecutibleAmount,
	SublineCode,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_GTAMX401Stage
),
ArchGTAMX401Stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchGTAMX401Stage
	(ExtractDate, SourceSystemId, AuditId, GTAMX401StageId, TableField, KeyLength, LocationCode, MasterCompanyNumber, TypeBureauCode, MajorPerilCode, CoverageCode, OutputDataLength, BureauCoverageCode, DecutibleType, DecutibleAmount, SublineCode)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	GTAMX401STAGEID, 
	TABLEFIELD, 
	KEYLENGTH, 
	LOCATIONCODE, 
	MASTERCOMPANYNUMBER, 
	TYPEBUREAUCODE, 
	MAJORPERILCODE, 
	COVERAGECODE, 
	OUTPUTDATALENGTH, 
	BUREAUCOVERAGECODE, 
	DECUTIBLETYPE, 
	DECUTIBLEAMOUNT, 
	SUBLINECODE
	FROM EXP_Set_AuditID
),
WITH
SQ_DCCADriverStaging AS (
	SELECT
		DCCADriverStagingId,
		ExtractDate,
		SourceSystemId,
		CA_DriverId,
		SessionId,
		Id,
		BroadenNoFault,
		DateOfHire,
		DriversLicenseNumber,
		JobTitle,
		PercentageOfUse,
		StateLicensed,
		UseVehicleNumber,
		YearsExperience,
		YearLicensed,
		LineId
	FROM DCCADriverStaging
),
EXP_Metadata AS (
	SELECT
	DCCADriverStagingId,
	ExtractDate,
	SourceSystemId,
	CA_DriverId,
	SessionId,
	Id,
	BroadenNoFault,
	DateOfHire,
	DriversLicenseNumber,
	JobTitle,
	PercentageOfUse,
	StateLicensed,
	UseVehicleNumber,
	YearsExperience,
	YearLicensed,
	LineId,
	-- *INF*: DECODE(BroadenNoFault, 'T', 1, 'F', 0, NULL)
	DECODE(
	    BroadenNoFault,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_BroadenNoFault,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCADriverStaging
),
ArchDCCADriverStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCADriverStaging
	(ExtractDate, SourceSystemId, AuditId, LineId, CA_DriverId, SessionId, Id, BroadenNoFault, DateOfHire, DriversLicenseNumber, JobTitle, PercentageOfUse, StateLicensed, UseVehicleNumber, YearsExperience, YearLicensed)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	LINEID, 
	CA_DRIVERID, 
	SESSIONID, 
	ID, 
	o_BroadenNoFault AS BROADENNOFAULT, 
	DATEOFHIRE, 
	DRIVERSLICENSENUMBER, 
	JOBTITLE, 
	PERCENTAGEOFUSE, 
	STATELICENSED, 
	USEVEHICLENUMBER, 
	YEARSEXPERIENCE, 
	YEARLICENSED
	FROM EXP_Metadata
),
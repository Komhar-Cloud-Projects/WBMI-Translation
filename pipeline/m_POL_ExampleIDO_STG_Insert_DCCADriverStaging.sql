WITH
SQ_DC_CA_Driver AS (
	WITH cte_DCCADriver(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.CA_DriverId, 
	X.SessionId, 
	X.Id, 
	X.BroadenNoFault, 
	X.DateOfHire, 
	X.DriversLicenseNumber, 
	X.JobTitle, 
	X.PercentageOfUse, 
	X.StateLicensed, 
	X.UseVehicleNumber, 
	X.YearsExperience, 
	X.YearLicensed 
	FROM
	DC_CA_Driver X
	inner join
	cte_DCCADriver Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	LineId,
	CA_DriverId,
	SessionId,
	Id,
	BroadenNoFault,
	DateOfHire,
	DriversLicenseNumber,
	-- *INF*: IIF(SUBSTR(DriversLicenseNumber, -1, 1) = '?', SUBSTR(DriversLicenseNumber, 1, LENGTH(DriversLicenseNumber)-1) , DriversLicenseNumber)
	IFF(
	    SUBSTR(DriversLicenseNumber, - 1, 1) = '?',
	    SUBSTR(DriversLicenseNumber, 1, LENGTH(DriversLicenseNumber) - 1),
	    DriversLicenseNumber
	) AS o_DriversLicenseNumber,
	JobTitle,
	PercentageOfUse,
	StateLicensed,
	UseVehicleNumber,
	YearsExperience,
	YearLicensed,
	-- *INF*: DECODE(BroadenNoFault, 'T', 1, 'F', 0, NULL)
	DECODE(
	    BroadenNoFault,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_BroadenNoFault,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CA_Driver
),
DCCADriverStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCADriverStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCADriverStaging
	(ExtractDate, SourceSystemId, LineId, CA_DriverId, SessionId, Id, BroadenNoFault, DateOfHire, DriversLicenseNumber, JobTitle, PercentageOfUse, StateLicensed, UseVehicleNumber, YearsExperience, YearLicensed)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	LINEID, 
	CA_DRIVERID, 
	SESSIONID, 
	ID, 
	o_BroadenNoFault AS BROADENNOFAULT, 
	DATEOFHIRE, 
	o_DriversLicenseNumber AS DRIVERSLICENSENUMBER, 
	JOBTITLE, 
	PERCENTAGEOFUSE, 
	STATELICENSED, 
	USEVEHICLENUMBER, 
	YEARSEXPERIENCE, 
	YEARLICENSED
	FROM EXP_Metadata
),
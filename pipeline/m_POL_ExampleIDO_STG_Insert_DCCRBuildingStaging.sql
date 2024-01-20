WITH
SQ_DC_CR_Building AS (
	WITH cte_DCCRBuilding(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.CR_LocationId, 
	X.CR_BuildingId, 
	X.SessionId, 
	X.Id, 
	X.Deleted, 
	X.AlarmCompanyHasKeys, 
	X.AlarmType, 
	X.BulletResistingEnclosure, 
	X.BulletResistingEnclosureAndAlarm, 
	X.BurglarAlarmSystem, 
	X.CentralStation, 
	X.CentralStationAtleastHourly, 
	X.CertificateNumber, 
	X.ConnectedWith, 
	X.ConstructionCode, 
	X.CoveredProperty, 
	X.Description, 
	X.DoorType, 
	X.EffectiveDate, 
	X.ExpirationDate, 
	X.GradeExtentOfProtection, 
	X.GuardsOnDuty, 
	X.HoldupAlarm, 
	X.Insured, 
	X.LocalGong, 
	X.LockedSafe, 
	X.NameOfCompany, 
	X.NumberOfGuards, 
	X.NumberOfProtectiveDevices, 
	X.NumberOfStories, 
	X.NumberOfWatchpersons, 
	X.OutsideCentralStation, 
	X.OutsideGong, 
	X.PoliceStation, 
	X.PrivateWatchPerson, 
	X.RegisterHourlyOnAClock, 
	X.RoofCovering, 
	X.RoofDeckAttachment, 
	X.RoofGeometry, 
	X.RoofWallConstruction, 
	X.SignalACentralStation, 
	X.Sprinkler, 
	X.SquareFt, 
	X.ULClassification, 
	X.ULIProtectiveBag, 
	X.UnexpiredCertificate, 
	X.WindowProtection, 
	X.WindstormLossMitigation, 
	X.YearBuilt, 
	X.CR_LocationXmlId 
	FROM
	DC_CR_Building X
	inner join
	cte_DCCRBuilding Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	SYSDATE AS ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemId,
	LineId,
	CR_LocationId,
	CR_BuildingId,
	SessionId,
	Id,
	Deleted,
	-- *INF*: DECODE(Deleted, 'T', 1, 'F', 0, NULL)
	DECODE(
	    Deleted,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Deleted,
	AlarmCompanyHasKeys,
	-- *INF*: DECODE(AlarmCompanyHasKeys, 'T', 1, 'F', 0, NULL)
	DECODE(
	    AlarmCompanyHasKeys,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AlarmCompanyHasKeys,
	AlarmType,
	BulletResistingEnclosure,
	-- *INF*: DECODE(BulletResistingEnclosure, 'T', 1, 'F', 0, NULL)
	DECODE(
	    BulletResistingEnclosure,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_BulletResistingEnclosure,
	BulletResistingEnclosureAndAlarm,
	-- *INF*: DECODE(BulletResistingEnclosureAndAlarm, 'T', 1, 'F', 0, NULL)
	DECODE(
	    BulletResistingEnclosureAndAlarm,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_BulletResistingEnclosureAndAlarm,
	BurglarAlarmSystem,
	-- *INF*: DECODE(BurglarAlarmSystem, 'T', 1, 'F', 0, NULL)
	DECODE(
	    BurglarAlarmSystem,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_BurglarAlarmSystem,
	CentralStation,
	-- *INF*: DECODE(CentralStation, 'T', 1, 'F', 0, NULL)
	DECODE(
	    CentralStation,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_CentralStation,
	CentralStationAtleastHourly,
	-- *INF*: DECODE(CentralStationAtleastHourly, 'T', 1, 'F', 0, NULL)
	DECODE(
	    CentralStationAtleastHourly,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_CentralStationAtleastHourly,
	CertificateNumber,
	ConnectedWith,
	ConstructionCode,
	CoveredProperty,
	-- *INF*: DECODE(CoveredProperty, 'T', 1, 'F', 0, NULL)
	DECODE(
	    CoveredProperty,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_CoveredProperty,
	Description,
	DoorType,
	EffectiveDate,
	ExpirationDate,
	GradeExtentOfProtection,
	-- *INF*: DECODE(GradeExtentOfProtection, 'T', 1, 'F', 0, NULL)
	DECODE(
	    GradeExtentOfProtection,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_GradeExtentOfProtection,
	GuardsOnDuty,
	-- *INF*: DECODE(GuardsOnDuty, 'T', 1, 'F', 0, NULL)
	DECODE(
	    GuardsOnDuty,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_GuardsOnDuty,
	HoldupAlarm,
	-- *INF*: DECODE(HoldupAlarm, 'T', 1, 'F', 0, NULL)
	DECODE(
	    HoldupAlarm,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_HoldupAlarm,
	Insured,
	-- *INF*: DECODE(Insured, 'T', 1, 'F', 0, NULL)
	DECODE(
	    Insured,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Insured,
	LocalGong,
	-- *INF*: DECODE(LocalGong, 'T', 1, 'F', 0, NULL)
	DECODE(
	    LocalGong,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_LocalGong,
	LockedSafe,
	-- *INF*: DECODE(LockedSafe, 'T', 1, 'F', 0, NULL)
	DECODE(
	    LockedSafe,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_LockedSafe,
	NameOfCompany,
	NumberOfGuards,
	NumberOfProtectiveDevices,
	NumberOfStories,
	NumberOfWatchpersons,
	OutsideCentralStation,
	-- *INF*: DECODE(OutsideCentralStation, 'T', 1, 'F', 0, NULL)
	DECODE(
	    OutsideCentralStation,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_OutsideCentralStation,
	OutsideGong,
	-- *INF*: DECODE(OutsideGong, 'T', 1, 'F', 0, NULL)
	DECODE(
	    OutsideGong,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_OutsideGong,
	PoliceStation,
	-- *INF*: DECODE(PoliceStation, 'T', 1, 'F', 0, NULL)
	DECODE(
	    PoliceStation,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PoliceStation,
	PrivateWatchPerson,
	-- *INF*: DECODE(PrivateWatchPerson, 'T', 1, 'F', 0, NULL)
	DECODE(
	    PrivateWatchPerson,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PrivateWatchPerson,
	RegisterHourlyOnAClock,
	-- *INF*: DECODE(RegisterHourlyOnAClock, 'T', 1, 'F', 0, NULL)
	DECODE(
	    RegisterHourlyOnAClock,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_RegisterHourlyOnAClock,
	RoofCovering,
	RoofDeckAttachment,
	RoofGeometry,
	RoofWallConstruction,
	SignalACentralStation,
	-- *INF*: DECODE(SignalACentralStation, 'T', 1, 'F', 0, NULL)
	DECODE(
	    SignalACentralStation,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_SignalACentralStation,
	Sprinkler,
	-- *INF*: DECODE(Sprinkler, 'T', 1, 'F', 0, NULL)
	DECODE(
	    Sprinkler,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Sprinkler,
	SquareFt,
	ULClassification,
	ULIProtectiveBag,
	-- *INF*: DECODE(ULIProtectiveBag, 'T', 1, 'F', 0, NULL)
	DECODE(
	    ULIProtectiveBag,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ULIProtectiveBag,
	UnexpiredCertificate,
	-- *INF*: DECODE(UnexpiredCertificate, 'T', 1, 'F', 0, NULL)
	DECODE(
	    UnexpiredCertificate,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_UnexpiredCertificate,
	WindowProtection,
	WindstormLossMitigation,
	-- *INF*: DECODE(WindstormLossMitigation, 'T', 1, 'F', 0, NULL)
	DECODE(
	    WindstormLossMitigation,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_WindstormLossMitigation,
	YearBuilt,
	CR_LocationXmlId
	FROM SQ_DC_CR_Building
),
DCCRBuildingStaging AS (
	TRUNCATE TABLE DCCRBuildingStaging;
	INSERT INTO DCCRBuildingStaging
	(ExtractDate, SourceSystemId, LineId, CR_LocationId, CR_BuildingId, SessionId, Id, Deleted, AlarmCompanyHasKeys, AlarmType, BulletResistingEnclosure, BulletResistingEnclosureAndAlarm, BurglarAlarmSystem, CentralStation, CentralStationAtleastHourly, CertificateNumber, ConnectedWith, ConstructionCode, CoveredProperty, Description, DoorType, EffectiveDate, ExpirationDate, GradeExtentOfProtection, GuardsOnDuty, HoldupAlarm, Insured, LocalGong, LockedSafe, NameOfCompany, NumberOfGuards, NumberOfProtectiveDevices, NumberOfStories, NumberOfWatchpersons, OutsideCentralStation, OutsideGong, PoliceStation, PrivateWatchPerson, RegisterHourlyOnAClock, RoofCovering, RoofDeckAttachment, RoofGeometry, RoofWallConstruction, SignalACentralStation, Sprinkler, SquareFt, ULClassification, ULIProtectiveBag, UnexpiredCertificate, WindowProtection, WindstormLossMitigation, YearBuilt, CR_LocationXmlId)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	LINEID, 
	CR_LOCATIONID, 
	CR_BUILDINGID, 
	SESSIONID, 
	ID, 
	o_Deleted AS DELETED, 
	o_AlarmCompanyHasKeys AS ALARMCOMPANYHASKEYS, 
	ALARMTYPE, 
	o_BulletResistingEnclosure AS BULLETRESISTINGENCLOSURE, 
	o_BulletResistingEnclosureAndAlarm AS BULLETRESISTINGENCLOSUREANDALARM, 
	o_BurglarAlarmSystem AS BURGLARALARMSYSTEM, 
	o_CentralStation AS CENTRALSTATION, 
	o_CentralStationAtleastHourly AS CENTRALSTATIONATLEASTHOURLY, 
	CERTIFICATENUMBER, 
	CONNECTEDWITH, 
	CONSTRUCTIONCODE, 
	o_CoveredProperty AS COVEREDPROPERTY, 
	DESCRIPTION, 
	DOORTYPE, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	o_GradeExtentOfProtection AS GRADEEXTENTOFPROTECTION, 
	o_GuardsOnDuty AS GUARDSONDUTY, 
	o_HoldupAlarm AS HOLDUPALARM, 
	o_Insured AS INSURED, 
	o_LocalGong AS LOCALGONG, 
	o_LockedSafe AS LOCKEDSAFE, 
	NAMEOFCOMPANY, 
	NUMBEROFGUARDS, 
	NUMBEROFPROTECTIVEDEVICES, 
	NUMBEROFSTORIES, 
	NUMBEROFWATCHPERSONS, 
	o_OutsideCentralStation AS OUTSIDECENTRALSTATION, 
	o_OutsideGong AS OUTSIDEGONG, 
	o_PoliceStation AS POLICESTATION, 
	o_PrivateWatchPerson AS PRIVATEWATCHPERSON, 
	o_RegisterHourlyOnAClock AS REGISTERHOURLYONACLOCK, 
	ROOFCOVERING, 
	ROOFDECKATTACHMENT, 
	ROOFGEOMETRY, 
	ROOFWALLCONSTRUCTION, 
	o_SignalACentralStation AS SIGNALACENTRALSTATION, 
	o_Sprinkler AS SPRINKLER, 
	SQUAREFT, 
	ULCLASSIFICATION, 
	o_ULIProtectiveBag AS ULIPROTECTIVEBAG, 
	o_UnexpiredCertificate AS UNEXPIREDCERTIFICATE, 
	WINDOWPROTECTION, 
	o_WindstormLossMitigation AS WINDSTORMLOSSMITIGATION, 
	YEARBUILT, 
	CR_LOCATIONXMLID
	FROM EXP_Metadata
),
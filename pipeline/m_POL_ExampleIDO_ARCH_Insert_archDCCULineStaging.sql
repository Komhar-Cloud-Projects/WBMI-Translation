WITH
SQ_DCCULineStaging AS (
	SELECT
		DCCULineStagingId,
		ExtractDate,
		SourceSystemId,
		LineId,
		CU_LineId,
		SessionId,
		Id,
		AutoOperatedOver300Miles,
		AutoSubjectToTimeConstraints,
		ClaimsMade,
		Description,
		ForeignSales,
		IncludeBusinessowners,
		IncludeCommercialAuto,
		IncludeEmployersLiability,
		IncludeGeneralLiability,
		InternetSalesPercent,
		LiquorSalesPercent,
		MountedMechanicalEquipment,
		NumberOfSwimmingPools,
		NumberOfYearsInBusiness,
		SpecifiedAdditionalCountries,
		SpecifiedExceptedCountries
	FROM DCCULineStaging
),
EXP_Metadata AS (
	SELECT
	DCCULineStagingId,
	ExtractDate,
	SourceSystemId,
	LineId,
	CU_LineId,
	SessionId,
	Id,
	AutoOperatedOver300Miles,
	AutoSubjectToTimeConstraints,
	ClaimsMade,
	Description,
	ForeignSales,
	IncludeBusinessowners,
	IncludeCommercialAuto,
	IncludeEmployersLiability,
	IncludeGeneralLiability,
	InternetSalesPercent,
	LiquorSalesPercent,
	MountedMechanicalEquipment,
	NumberOfSwimmingPools,
	NumberOfYearsInBusiness,
	SpecifiedAdditionalCountries,
	SpecifiedExceptedCountries,
	-- *INF*: DECODE(AutoOperatedOver300Miles, 'T', 1, 'F', 0, NULL)
	DECODE(
	    AutoOperatedOver300Miles,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AutoOperatedOver300Miles,
	-- *INF*: DECODE(AutoSubjectToTimeConstraints, 'T', 1, 'F', 0, NULL)
	DECODE(
	    AutoSubjectToTimeConstraints,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AutoSubjectToTimeConstraints,
	-- *INF*: DECODE(ClaimsMade, 'T', 1, 'F', 0, NULL)
	DECODE(
	    ClaimsMade,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ClaimsMade,
	-- *INF*: DECODE(ForeignSales, 'T', 1, 'F', 0, NULL)
	DECODE(
	    ForeignSales,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ForeignSales,
	-- *INF*: DECODE(IncludeBusinessowners, 'T', 1, 'F', 0, NULL)
	DECODE(
	    IncludeBusinessowners,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IncludeBusinessowners,
	-- *INF*: DECODE(IncludeCommercialAuto, 'T', 1, 'F', 0, NULL)
	DECODE(
	    IncludeCommercialAuto,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IncludeCommercialAuto,
	-- *INF*: DECODE(IncludeEmployersLiability, 'T', 1, 'F', 0, NULL)
	DECODE(
	    IncludeEmployersLiability,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IncludeEmployersLiability,
	-- *INF*: DECODE(IncludeGeneralLiability, 'T', 1, 'F', 0, NULL)
	DECODE(
	    IncludeGeneralLiability,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IncludeGeneralLiability,
	-- *INF*: DECODE(MountedMechanicalEquipment, 'T', 1, 'F', 0, NULL)
	DECODE(
	    MountedMechanicalEquipment,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_MountedMechanicalEquipment,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCULineStaging
),
ArchDCCULineStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCULineStaging
	(ExtractDate, SourceSystemId, AuditId, LineId, CU_LineId, SessionId, Id, AutoOperatedOver300Miles, AutoSubjectToTimeConstraints, ClaimsMade, Description, ForeignSales, IncludeBusinessowners, IncludeCommercialAuto, IncludeEmployersLiability, IncludeGeneralLiability, InternetSalesPercent, LiquorSalesPercent, MountedMechanicalEquipment, NumberOfSwimmingPools, NumberOfYearsInBusiness, SpecifiedAdditionalCountries, SpecifiedExceptedCountries)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	LINEID, 
	CU_LINEID, 
	SESSIONID, 
	ID, 
	o_AutoOperatedOver300Miles AS AUTOOPERATEDOVER300MILES, 
	o_AutoSubjectToTimeConstraints AS AUTOSUBJECTTOTIMECONSTRAINTS, 
	o_ClaimsMade AS CLAIMSMADE, 
	DESCRIPTION, 
	o_ForeignSales AS FOREIGNSALES, 
	o_IncludeBusinessowners AS INCLUDEBUSINESSOWNERS, 
	o_IncludeCommercialAuto AS INCLUDECOMMERCIALAUTO, 
	o_IncludeEmployersLiability AS INCLUDEEMPLOYERSLIABILITY, 
	o_IncludeGeneralLiability AS INCLUDEGENERALLIABILITY, 
	INTERNETSALESPERCENT, 
	LIQUORSALESPERCENT, 
	o_MountedMechanicalEquipment AS MOUNTEDMECHANICALEQUIPMENT, 
	NUMBEROFSWIMMINGPOOLS, 
	NUMBEROFYEARSINBUSINESS, 
	SPECIFIEDADDITIONALCOUNTRIES, 
	SPECIFIEDEXCEPTEDCOUNTRIES
	FROM EXP_Metadata
),
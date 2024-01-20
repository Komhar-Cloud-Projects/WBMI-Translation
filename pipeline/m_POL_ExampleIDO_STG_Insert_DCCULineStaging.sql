WITH
SQ_DC_CU_Line AS (
	WITH cte_DCCULine(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.CU_LineId, 
	X.SessionId, 
	X.Id, 
	X.AutoOperatedOver300Miles, 
	X.AutoSubjectToTimeConstraints, 
	X.ClaimsMade, 
	X.Description, 
	X.ForeignSales, 
	X.IncludeBusinessowners, 
	X.IncludeCommercialAuto, 
	X.IncludeEmployersLiability, 
	X.IncludeGeneralLiability, 
	X.InternetSalesPercent, 
	X.LiquorSalesPercent, 
	X.MountedMechanicalEquipment, 
	X.NumberOfSwimmingPools, 
	X.NumberOfYearsInBusiness, 
	X.SpecifiedAdditionalCountries, 
	X.SpecifiedExceptedCountries 
	FROM
	DC_CU_Line X
	inner join
	cte_DCCULine Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
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
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CU_Line
),
DCCULineStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCULineStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCULineStaging
	(ExtractDate, SourceSystemId, LineId, CU_LineId, SessionId, Id, AutoOperatedOver300Miles, AutoSubjectToTimeConstraints, ClaimsMade, Description, ForeignSales, IncludeBusinessowners, IncludeCommercialAuto, IncludeEmployersLiability, IncludeGeneralLiability, InternetSalesPercent, LiquorSalesPercent, MountedMechanicalEquipment, NumberOfSwimmingPools, NumberOfYearsInBusiness, SpecifiedAdditionalCountries, SpecifiedExceptedCountries)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
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
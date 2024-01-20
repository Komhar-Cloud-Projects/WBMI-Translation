WITH
SQ_DC_WC_CoverageManualPremium AS (
	WITH cte_DCWCCoverageManualPremium(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT
	X.CoverageId,
	X.WC_CoverageManualPremiumId,
	X.SessionId,
	X.AdmiraltyProgramType,
	X.CommercialConstructionPayrollTerritory1,
	X.CommercialConstructionPayrollTerritory2,
	X.CommercialConstructionPayrollTerritory3,
	X.EmployeeType,
	X.ExposureBasis,
	X.FELAProgramType,
	X.FireHomeAreasPopulation,
	X.FireOutsideAreasPopulation,
	X.MinimumPremium,
	X.NonRatableElementRate,
	X.NumberOfAdditionalFireProtectionContracts,
	X.NumberOfApparatus,
	X.NumberOfEmployees,
	X.NumberOfFullTimeEmployees,
	X.NumberOfGinningLocations,
	X.NumberOfPartTimeEmployees,
	X.NumberOfWeeks,
	X.PeriodDate,
	X.PrivateResidencePremium,
	X.StateOnlyIndicator,
	X.UpsetBasis,
	X.USLandHAct,
	X.VolunteerAmbulanceEmployersLiabilityIndicator,
	X.VolunteerFirefightersEmployersLiabilityIndicator,
	X.WaiverOfSubrogationType
	FROM
	DC_WC_CoverageManualPremium X
	inner join
	cte_DCWCCoverageManualPremium Y on X.SessionId = Y.SessionId
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CoverageId,
	WC_CoverageManualPremiumId,
	SessionId,
	AdmiraltyProgramType,
	CommercialConstructionPayrollTerritory1,
	CommercialConstructionPayrollTerritory2,
	CommercialConstructionPayrollTerritory3,
	EmployeeType,
	ExposureBasis,
	FELAProgramType,
	FireHomeAreasPopulation,
	FireOutsideAreasPopulation,
	MinimumPremium,
	NonRatableElementRate,
	NumberOfAdditionalFireProtectionContracts,
	NumberOfApparatus,
	NumberOfEmployees,
	NumberOfFullTimeEmployees,
	NumberOfGinningLocations,
	NumberOfPartTimeEmployees,
	NumberOfWeeks,
	PeriodDate,
	PrivateResidencePremium,
	StateOnlyIndicator,
	UpsetBasis,
	USLandHAct,
	VolunteerAmbulanceEmployersLiabilityIndicator,
	VolunteerFirefightersEmployersLiabilityIndicator,
	WaiverOfSubrogationType,
	SYSDATE AS ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemId
	FROM SQ_DC_WC_CoverageManualPremium
),
DCWCCoverageManualPremiumStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCWCCoverageManualPremiumStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCWCCoverageManualPremiumStaging
	(CoverageId, WC_CoverageManualPremiumId, SessionId, AdmiraltyProgramType, CommercialConstructionPayrollTerritory1, CommercialConstructionPayrollTerritory2, CommercialConstructionPayrollTerritory3, EmployeeType, ExposureBasis, FELAProgramType, FireHomeAreasPopulation, FireOutsideAreasPopulation, MinimumPremium, NonRatableElementRate, NumberOfAdditionalFireProtectionContracts, NumberOfApparatus, NumberOfEmployees, NumberOfFullTimeEmployees, NumberOfGinningLocations, NumberOfPartTimeEmployees, NumberOfWeeks, PeriodDate, PrivateResidencePremium, StateOnlyIndicator, UpsetBasis, USLandHAct, VolunteerAmbulanceEmployersLiabilityIndicator, VolunteerFirefightersEmployersLiabilityIndicator, WaiverOfSubrogationType, ExtractDate, SourceSystemId)
	SELECT 
	COVERAGEID, 
	WC_COVERAGEMANUALPREMIUMID, 
	SESSIONID, 
	ADMIRALTYPROGRAMTYPE, 
	COMMERCIALCONSTRUCTIONPAYROLLTERRITORY1, 
	COMMERCIALCONSTRUCTIONPAYROLLTERRITORY2, 
	COMMERCIALCONSTRUCTIONPAYROLLTERRITORY3, 
	EMPLOYEETYPE, 
	EXPOSUREBASIS, 
	FELAPROGRAMTYPE, 
	FIREHOMEAREASPOPULATION, 
	FIREOUTSIDEAREASPOPULATION, 
	MINIMUMPREMIUM, 
	NONRATABLEELEMENTRATE, 
	NUMBEROFADDITIONALFIREPROTECTIONCONTRACTS, 
	NUMBEROFAPPARATUS, 
	NUMBEROFEMPLOYEES, 
	NUMBEROFFULLTIMEEMPLOYEES, 
	NUMBEROFGINNINGLOCATIONS, 
	NUMBEROFPARTTIMEEMPLOYEES, 
	NUMBEROFWEEKS, 
	PERIODDATE, 
	PRIVATERESIDENCEPREMIUM, 
	STATEONLYINDICATOR, 
	UPSETBASIS, 
	USLANDHACT, 
	VOLUNTEERAMBULANCEEMPLOYERSLIABILITYINDICATOR, 
	VOLUNTEERFIREFIGHTERSEMPLOYERSLIABILITYINDICATOR, 
	WAIVEROFSUBROGATIONTYPE, 
	EXTRACTDATE, 
	SOURCESYSTEMID
	FROM EXP_Metadata
),
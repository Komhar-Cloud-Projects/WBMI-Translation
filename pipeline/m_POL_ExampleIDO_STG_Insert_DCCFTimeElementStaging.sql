WITH
SQ_DC_CF_TimeElement AS (
	WITH cte_DCCFTimeElement(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CF_RiskId, 
	X.CF_TimeElementId, 
	X.SessionId, 
	X.Id, 
	X.CoverageForm, 
	X.CoinsurancePercentage, 
	X.RiskType, 
	X.BuildingStatus, 
	X.CivilAuthority, 
	X.TimePeriodSelect, 
	X.CombinedOperations, 
	X.Combination, 
	X.CoinsurancePercentageSuspended, 
	X.OrdinanceOfLaw, 
	X.LossAdjustment, 
	X.RadioTelevisionAntennas, 
	X.FieldActExclusion, 
	X.CommunicationSupply, 
	X.OverheadCommunicationLines, 
	X.OverheadPowerTransmissionLines, 
	X.PowerSupply, 
	X.WaterSupplySelect, 
	X.UtilServicesInfoPublicUtilitySelect, 
	X.UtilServicesIndicator, 
	X.ARate, 
	X.LossPaymentLimitType, 
	X.DependentPropertiesSelect, 
	X.SquareFtForNonManufacturing, 
	X.SquareFtForManufacturingOrMining, 
	X.SquareFtForRental 
	FROM
	DC_CF_TimeElement X
	inner join
	cte_DCCFTimeElement Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CF_RiskId,
	CF_TimeElementId,
	SessionId,
	Id,
	CoverageForm,
	CoinsurancePercentage,
	RiskType,
	BuildingStatus,
	CivilAuthority,
	TimePeriodSelect,
	CombinedOperations,
	Combination,
	CoinsurancePercentageSuspended,
	OrdinanceOfLaw,
	LossAdjustment,
	RadioTelevisionAntennas,
	FieldActExclusion,
	CommunicationSupply,
	OverheadCommunicationLines,
	OverheadPowerTransmissionLines,
	PowerSupply,
	WaterSupplySelect,
	UtilServicesInfoPublicUtilitySelect,
	UtilServicesIndicator,
	-- *INF*: DECODE(OrdinanceOfLaw, 'T', 1, 'F', 0, NULL)
	DECODE(
	    OrdinanceOfLaw,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_OrdinanceOfLaw,
	-- *INF*: DECODE(LossAdjustment, 'T', 1, 'F', 0, NULL)
	DECODE(
	    LossAdjustment,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_LossAdjustment,
	-- *INF*: DECODE(RadioTelevisionAntennas, 'T', 1, 'F', 0, NULL)
	DECODE(
	    RadioTelevisionAntennas,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_RadioTelevisionAntennas,
	-- *INF*: DECODE(FieldActExclusion, 'T', 1, 'F', 0, NULL)
	DECODE(
	    FieldActExclusion,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_FieldActExclusion,
	-- *INF*: DECODE(CommunicationSupply, 'T', 1, 'F', 0, NULL)
	DECODE(
	    CommunicationSupply,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_CommunicationSupply,
	-- *INF*: DECODE(OverheadCommunicationLines, 'T', 1, 'F', 0, NULL)
	DECODE(
	    OverheadCommunicationLines,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_OverheadCommunicationLines,
	-- *INF*: DECODE(OverheadPowerTransmissionLines, 'T', 1, 'F', 0, NULL)
	DECODE(
	    OverheadPowerTransmissionLines,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_OverheadPowerTransmissionLines,
	-- *INF*: DECODE(PowerSupply, 'T', 1, 'F', 0, NULL)
	DECODE(
	    PowerSupply,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PowerSupply,
	-- *INF*: DECODE(WaterSupplySelect, 'T', 1, 'F', 0, NULL)
	DECODE(
	    WaterSupplySelect,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_WaterSupplySelect,
	-- *INF*: DECODE(UtilServicesInfoPublicUtilitySelect, 'T', 1, 'F', 0, NULL)
	DECODE(
	    UtilServicesInfoPublicUtilitySelect,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_UtilServicesInfoPublicUtilitySelect,
	-- *INF*: DECODE(UtilServicesIndicator, 'T', 1, 'F', 0, NULL)
	DECODE(
	    UtilServicesIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_UtilServicesIndicator,
	ARate,
	LossPaymentLimitType,
	DependentPropertiesSelect,
	-- *INF*: DECODE(DependentPropertiesSelect, 'T', 1, 'F', 0, NULL)
	DECODE(
	    DependentPropertiesSelect,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_DependentPropertiesSelect,
	SquareFtForNonManufacturing,
	SquareFtForManufacturingOrMining,
	SquareFtForRental,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CF_TimeElement
),
DCCFTimeElementStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFTimeElementStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFTimeElementStaging
	(CF_TimeElementId, SessionId, Id, CoverageForm, CoinsurancePercentage, RiskType, BuildingStatus, CivilAuthority, TimePeriodSelect, CombinedOperations, Combination, CoinsurancePercentageSuspended, OrdinanceOfLaw, LossAdjustment, RadioTelevisionAntennas, FieldActExclusion, CommunicationSupply, OverheadCommunicationLines, OverheadPowerTransmissionLines, PowerSupply, WaterSupplySelect, UtilServicesInfoPublicUtilitySelect, UtilServicesIndicator, ARate, LossPaymentLimitType, DependentPropertiesSelect, SquareFtForNonManufacturing, SquareFtForManufacturingOrMining, SquareFtForRental, ExtractDate, SourceSystemId, CF_RiskId)
	SELECT 
	CF_TIMEELEMENTID, 
	SESSIONID, 
	ID, 
	COVERAGEFORM, 
	COINSURANCEPERCENTAGE, 
	RISKTYPE, 
	BUILDINGSTATUS, 
	CIVILAUTHORITY, 
	TIMEPERIODSELECT, 
	COMBINEDOPERATIONS, 
	COMBINATION, 
	COINSURANCEPERCENTAGESUSPENDED, 
	o_OrdinanceOfLaw AS ORDINANCEOFLAW, 
	o_LossAdjustment AS LOSSADJUSTMENT, 
	o_RadioTelevisionAntennas AS RADIOTELEVISIONANTENNAS, 
	o_FieldActExclusion AS FIELDACTEXCLUSION, 
	o_CommunicationSupply AS COMMUNICATIONSUPPLY, 
	o_OverheadCommunicationLines AS OVERHEADCOMMUNICATIONLINES, 
	o_OverheadPowerTransmissionLines AS OVERHEADPOWERTRANSMISSIONLINES, 
	o_PowerSupply AS POWERSUPPLY, 
	o_WaterSupplySelect AS WATERSUPPLYSELECT, 
	o_UtilServicesInfoPublicUtilitySelect AS UTILSERVICESINFOPUBLICUTILITYSELECT, 
	o_UtilServicesIndicator AS UTILSERVICESINDICATOR, 
	ARATE, 
	LOSSPAYMENTLIMITTYPE, 
	o_DependentPropertiesSelect AS DEPENDENTPROPERTIESSELECT, 
	SQUAREFTFORNONMANUFACTURING, 
	SQUAREFTFORMANUFACTURINGORMINING, 
	SQUAREFTFORRENTAL, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CF_RISKID
	FROM EXP_Metadata
),
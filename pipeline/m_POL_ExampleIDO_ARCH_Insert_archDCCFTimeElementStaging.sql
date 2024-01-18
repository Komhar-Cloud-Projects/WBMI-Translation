WITH
SQ_DCCFTimeElementStaging AS (
	SELECT
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
		ARate,
		LossPaymentLimitType,
		DependentPropertiesSelect,
		SquareFtForNonManufacturing,
		SquareFtForManufacturingOrMining,
		SquareFtForRental,
		ExtractDate,
		SourceSystemId,
		CF_RiskId
	FROM DCCFTimeElementStaging
),
EXP_Metadata AS (
	SELECT
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
	ExtractDate,
	SourceSystemId,
	CF_RiskId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCFTimeElementStaging
),
archDCCFTimeElementStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCCFTimeElementStaging
	(CF_RiskId, CF_TimeElementId, SessionId, Id, CoverageForm, CoinsurancePercentage, RiskType, BuildingStatus, CivilAuthority, TimePeriodSelect, CombinedOperations, Combination, CoinsurancePercentageSuspended, OrdinanceOfLaw, LossAdjustment, RadioTelevisionAntennas, FieldActExclusion, CommunicationSupply, OverheadCommunicationLines, OverheadPowerTransmissionLines, PowerSupply, WaterSupplySelect, UtilServicesInfoPublicUtilitySelect, UtilServicesIndicator, ARate, LossPaymentLimitType, DependentPropertiesSelect, SquareFtForNonManufacturing, SquareFtForManufacturingOrMining, SquareFtForRental, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	CF_RISKID, 
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
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),
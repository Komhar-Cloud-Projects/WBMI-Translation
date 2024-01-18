WITH
SQ_WBCFPropertyStage AS (
	SELECT
		WBCFPropertyStageId,
		ExtractDate,
		SourceSystemId,
		CF_PropertyId,
		WB_CF_PropertyId,
		SessionId,
		KansasPropertyCredit,
		AgreedValue,
		RateStatus,
		RiskID,
		RateEffectiveDate,
		RCP,
		SprinklerProtectionDevice,
		OtherSprinklerProtectionDevice,
		HydrostaticWaterPressure,
		BG1SpecificRateSelectString,
		CoinsurancePercentageEmployeeTools,
		RCB,
		AttachedSignSelect,
		AttachedSignConstruction,
		AttachedSignDescription,
		PricePerSquareFoot,
		ControllingIteratorIndicator,
		SprinklerCreditFactor,
		ControllingIteratorIndicatorForLocation,
		OccupancyCategory,
		AgreedValueIndicator
	FROM WBCFPropertyStage
),
EXP_Metadata AS (
	SELECT
	WBCFPropertyStageId,
	ExtractDate,
	SourceSystemId,
	CF_PropertyId,
	WB_CF_PropertyId,
	SessionId,
	KansasPropertyCredit AS i_KansasPropertyCredit,
	-- *INF*: DECODE(i_KansasPropertyCredit, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_KansasPropertyCredit,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_KansasPropertyCredit,
	AgreedValue AS i_AgreedValue,
	-- *INF*: DECODE(i_AgreedValue, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_AgreedValue,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_AgreedValue,
	RateStatus,
	RiskID,
	RateEffectiveDate,
	RCP,
	SprinklerProtectionDevice,
	OtherSprinklerProtectionDevice,
	HydrostaticWaterPressure AS i_HydrostaticWaterPressure,
	-- *INF*: DECODE(i_HydrostaticWaterPressure, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_HydrostaticWaterPressure,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_HydrostaticWaterPressure,
	BG1SpecificRateSelectString,
	CoinsurancePercentageEmployeeTools,
	RCB,
	AttachedSignSelect AS i_AttachedSignSelect,
	-- *INF*: DECODE(i_AttachedSignSelect,'T',1,'F',0,NULL)
	DECODE(
	    i_AttachedSignSelect,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AttachedSignSelect,
	AttachedSignConstruction,
	AttachedSignDescription,
	PricePerSquareFoot,
	ControllingIteratorIndicator AS i_ControllingIteratorIndicator,
	-- *INF*: DECODE(i_ControllingIteratorIndicator, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_ControllingIteratorIndicator,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ControllingIteratorIndicator,
	SprinklerCreditFactor,
	ControllingIteratorIndicatorForLocation AS i_ControllingIteratorIndicatorForLocation,
	-- *INF*: DECODE(i_ControllingIteratorIndicatorForLocation, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_ControllingIteratorIndicatorForLocation,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ControllingIteratorIndicatorForLocation,
	OccupancyCategory,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	AgreedValueIndicator AS i_AgreedValueIndicator,
	-- *INF*: DECODE(i_AgreedValueIndicator, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_AgreedValueIndicator,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_AgreedValueIndicator
	FROM SQ_WBCFPropertyStage
),
ArchWBCFPropertyStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBCFPropertyStage
	(ExtractDate, SourceSystemId, AuditId, WBCFPropertyStageId, CF_PropertyId, WB_CF_PropertyId, SessionId, KansasPropertyCredit, AgreedValue, RateStatus, RiskID, RateEffectiveDate, RCP, SprinklerProtectionDevice, OtherSprinklerProtectionDevice, HydrostaticWaterPressure, BG1SpecificRateSelectString, CoinsurancePercentageEmployeeTools, RCB, AttachedSignSelect, AttachedSignConstruction, AttachedSignDescription, PricePerSquareFoot, ControllingIteratorIndicator, SprinklerCreditFactor, ControllingIteratorIndicatorForLocation, OccupancyCategory, AgreedValueIndicator)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBCFPROPERTYSTAGEID, 
	CF_PROPERTYID, 
	WB_CF_PROPERTYID, 
	SESSIONID, 
	o_KansasPropertyCredit AS KANSASPROPERTYCREDIT, 
	o_AgreedValue AS AGREEDVALUE, 
	RATESTATUS, 
	RISKID, 
	RATEEFFECTIVEDATE, 
	RCP, 
	SPRINKLERPROTECTIONDEVICE, 
	OTHERSPRINKLERPROTECTIONDEVICE, 
	o_HydrostaticWaterPressure AS HYDROSTATICWATERPRESSURE, 
	BG1SPECIFICRATESELECTSTRING, 
	COINSURANCEPERCENTAGEEMPLOYEETOOLS, 
	RCB, 
	o_AttachedSignSelect AS ATTACHEDSIGNSELECT, 
	ATTACHEDSIGNCONSTRUCTION, 
	ATTACHEDSIGNDESCRIPTION, 
	PRICEPERSQUAREFOOT, 
	o_ControllingIteratorIndicator AS CONTROLLINGITERATORINDICATOR, 
	SPRINKLERCREDITFACTOR, 
	o_ControllingIteratorIndicatorForLocation AS CONTROLLINGITERATORINDICATORFORLOCATION, 
	OCCUPANCYCATEGORY, 
	o_AgreedValueIndicator AS AGREEDVALUEINDICATOR
	FROM EXP_Metadata
),
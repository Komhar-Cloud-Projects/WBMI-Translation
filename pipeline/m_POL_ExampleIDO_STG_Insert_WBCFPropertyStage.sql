WITH
SQ_WB_CF_Property AS (
	WITH cte_WBTransaction(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CF_PropertyId, 
	X.WB_CF_PropertyId,
	X.SessionId,
	X.KansasPropertyCredit,
	X.AgreedValue,
	X.RateStatus,
	X.RiskID,
	X.RateEffectiveDate,
	X.RCP,
	X.SprinklerProtectionDevice,
	X.OtherSprinklerProtectionDevice,
	X.HydrostaticWaterPressure,
	X.BG1SpecificRateSelectString,
	X.CoinsurancePercentageEmployeeTools,
	X.RCB, X.AttachedSignSelect,
	X.AttachedSignConstruction,
	X.AttachedSignDescription,
	X.PricePerSquareFoot,
	X.ControllingIteratorIndicator,
	X.SprinklerCreditFactor,
	X.ControllingIteratorIndicatorForLocation,
	X.OccupancyCategory,
	X.AgreedValueIndicator
	FROM
	 WB_CF_Property X
	inner join
	cte_WBTransaction Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CF_PropertyId,
	WB_CF_PropertyId,
	SessionId,
	KansasPropertyCredit AS i_KansasPropertyCredit,
	-- *INF*: DECODE(i_KansasPropertyCredit,'T',1,'F',0,NULL)
	DECODE(
	    i_KansasPropertyCredit,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_KansasPropertyCredit,
	AgreedValue AS i_AgreedValue,
	-- *INF*: DECODE(i_AgreedValue,'T',1,'F',0,NULL)
	DECODE(
	    i_AgreedValue,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AgreedValue,
	RateStatus,
	RiskID,
	RateEffectiveDate,
	RCP,
	SprinklerProtectionDevice,
	OtherSprinklerProtectionDevice,
	HydrostaticWaterPressure AS i_HydrostaticWaterPressure,
	-- *INF*: DECODE(i_HydrostaticWaterPressure,'T',1,'F',0,NULL)
	DECODE(
	    i_HydrostaticWaterPressure,
	    'T', 1,
	    'F', 0,
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
	-- *INF*: DECODE(i_ControllingIteratorIndicator,'T',1,'F',0,NULL)
	DECODE(
	    i_ControllingIteratorIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ControllingIteratorIndicator,
	SprinklerCreditFactor,
	ControllingIteratorIndicatorForLocation AS i_ControllingIteratorIndicatorForLocation,
	-- *INF*: DECODE(i_ControllingIteratorIndicatorForLocation,'T',1,'F',0,NULL)
	DECODE(
	    i_ControllingIteratorIndicatorForLocation,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ControllingIteratorIndicatorForLocation,
	OccupancyCategory,
	AgreedValueIndicator,
	-- *INF*: DECODE(AgreedValueIndicator,'T',1,'F',0,NULL)
	DECODE(
	    AgreedValueIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AgreedValueIndicator,
	CURRENT_TIMESTAMP AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_CF_Property
),
WBCFPropertyStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCFPropertyStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCFPropertyStage
	(ExtractDate, SourceSystemId, CF_PropertyId, WB_CF_PropertyId, SessionId, KansasPropertyCredit, AgreedValue, RateStatus, RiskID, RateEffectiveDate, RCP, SprinklerProtectionDevice, OtherSprinklerProtectionDevice, HydrostaticWaterPressure, BG1SpecificRateSelectString, CoinsurancePercentageEmployeeTools, RCB, AttachedSignSelect, AttachedSignConstruction, AttachedSignDescription, PricePerSquareFoot, ControllingIteratorIndicator, SprinklerCreditFactor, ControllingIteratorIndicatorForLocation, OccupancyCategory, AgreedValueIndicator)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
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
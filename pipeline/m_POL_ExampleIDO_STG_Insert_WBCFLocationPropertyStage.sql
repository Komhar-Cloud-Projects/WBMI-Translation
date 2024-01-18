WITH
SQ_WB_CF_LocationProperty AS (
	WITH cte_WBCFLocationProperty(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CF_LocationPropertyId, 
	X.WB_CF_LocationPropertyId, 
	X.SessionId, 
	X.ProtectionClassOverride, 
	X.IncreasePersonalComputersCoverageEDP, 
	X.ExcessPersonalComputersEDPLimit, 
	X.IncreaseMoneyAndSecurities, 
	X.ExcessMoneyAndSecuritiesOutsideLimit, 
	X.IncreaseOutdoorDetachedSigns, 
	X.ExcessOutdoorDetachedSignsLimit, 
	X.ExcessMoneyAndSecuritiesInsideLimit, 
	X.SetCommentValue, 
	X.SetValueValue, 
	X.MultipleLocationCreditFactor, 
	X.IncreasePersonalComputersCoverageEDPText, 
	X.IncreaseMoneyAndSecuritiesText, 
	X.IncreaseOutdoorDetachedSignsText, 
	X.LocationIRPMManagementSetCommentValue, 
	X.LocationIRPMManagementSetValueValue, 
	X.LocationIRPMEmployeesSetCommentValue, 
	X.LocationIRPMEmployeesSetValueValue, 
	X.LocationIRPMLocationSetCommentValue, 
	X.LocationIRPMLocationSetValueValue, 
	X.LocationIRPMPremisesSetCommentValue, 
	X.LocationIRPMPremisesSetValueValue, 
	X.LocationIRPMProtectionSetCommentValue, 
	X.LocationIRPMProtectionSetValueValue, 
	X.ExcessPersonalComputersEDPPremium, 
	X.OutdoorDetachedSignsPremium, 
	X.MoneyAndSecuritiesInsidePremium, 
	X.MoneyAndSecuritiesOutsidePremium 
	FROM
	WB_CF_LocationProperty X
	inner join
	cte_WBCFLocationProperty Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CF_LocationPropertyId,
	WB_CF_LocationPropertyId,
	SessionId,
	ProtectionClassOverride,
	IncreasePersonalComputersCoverageEDP AS i_IncreasePersonalComputersCoverageEDP,
	-- *INF*: DECODE(i_IncreasePersonalComputersCoverageEDP,
	-- 'T',
	-- '1',
	-- 'F',
	-- '0',
	-- NULL
	-- )
	DECODE(
	    i_IncreasePersonalComputersCoverageEDP,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_IncreasePersonalComputersCoverageEDP,
	ExcessPersonalComputersEDPLimit,
	IncreaseMoneyAndSecurities AS i_IncreaseMoneyAndSecurities,
	-- *INF*: DECODE(i_IncreaseMoneyAndSecurities,
	-- 'T',
	-- '1',
	-- 'F',
	-- '0',
	-- NULL
	-- )
	DECODE(
	    i_IncreaseMoneyAndSecurities,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_IncreaseMoneyAndSecurities,
	ExcessMoneyAndSecuritiesOutsideLimit,
	IncreaseOutdoorDetachedSigns AS i_IncreaseOutdoorDetachedSigns,
	-- *INF*: DECODE(i_IncreaseOutdoorDetachedSigns,
	-- 'T',
	-- '1',
	-- 'F',
	-- '0',
	-- NULL
	-- )
	DECODE(
	    i_IncreaseOutdoorDetachedSigns,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_IncreaseOutdoorDetachedSigns,
	ExcessOutdoorDetachedSignsLimit,
	ExcessMoneyAndSecuritiesInsideLimit,
	SetCommentValue,
	SetValueValue,
	MultipleLocationCreditFactor,
	IncreasePersonalComputersCoverageEDPText,
	IncreaseMoneyAndSecuritiesText,
	IncreaseOutdoorDetachedSignsText,
	LocationIRPMManagementSetCommentValue,
	LocationIRPMManagementSetValueValue,
	LocationIRPMEmployeesSetCommentValue,
	LocationIRPMEmployeesSetValueValue,
	LocationIRPMLocationSetCommentValue,
	LocationIRPMLocationSetValueValue,
	LocationIRPMPremisesSetCommentValue,
	LocationIRPMPremisesSetValueValue,
	LocationIRPMProtectionSetCommentValue,
	LocationIRPMProtectionSetValueValue,
	ExcessPersonalComputersEDPPremium,
	OutdoorDetachedSignsPremium,
	MoneyAndSecuritiesInsidePremium,
	MoneyAndSecuritiesOutsidePremium,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_CF_LocationProperty
),
WBCFLocationPropertyStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCFLocationPropertyStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCFLocationPropertyStage
	(ExtractDate, SourceSystemId, CFLocationPropertyId, WBCFLocationPropertyId, SessionId, ProtectionClassOverride, IncreasePersonalComputersCoverageEDP, ExcessPersonalComputersEDPLimit, IncreaseMoneyAndSecurities, ExcessMoneyAndSecuritiesOutsideLimit, IncreaseOutdoorDetachedSigns, ExcessOutdoorDetachedSignsLimit, ExcessMoneyAndSecuritiesInsideLimit, LocationIRPMManagementSetCommentValue, LocationIRPMManagementSetValueValue, SetCommentValue, SetValueValue, LocationIRPMEmployeesSetCommentValue, LocationIRPMEmployeesSetValueValue, LocationIRPMLocationSetCommentValue, LocationIRPMLocationSetValueValue, LocationIRPMPremisesSetCommentValue, LocationIRPMPremisesSetValueValue, LocationIRPMProtectionSetCommentValue, LocationIRPMProtectionSetValueValue, MultipleLocationCreditFactor, IncreasePersonalComputersCoverageEDPText, IncreaseMoneyAndSecuritiesText, IncreaseOutdoorDetachedSignsText, ExcessPersonalComputersEDPPremium, OutdoorDetachedSignsPremium, MoneyAndSecuritiesInsidePremium, MoneyAndSecuritiesOutsidePremium)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CF_LocationPropertyId AS CFLOCATIONPROPERTYID, 
	WB_CF_LocationPropertyId AS WBCFLOCATIONPROPERTYID, 
	SESSIONID, 
	PROTECTIONCLASSOVERRIDE, 
	o_IncreasePersonalComputersCoverageEDP AS INCREASEPERSONALCOMPUTERSCOVERAGEEDP, 
	EXCESSPERSONALCOMPUTERSEDPLIMIT, 
	o_IncreaseMoneyAndSecurities AS INCREASEMONEYANDSECURITIES, 
	EXCESSMONEYANDSECURITIESOUTSIDELIMIT, 
	o_IncreaseOutdoorDetachedSigns AS INCREASEOUTDOORDETACHEDSIGNS, 
	EXCESSOUTDOORDETACHEDSIGNSLIMIT, 
	EXCESSMONEYANDSECURITIESINSIDELIMIT, 
	LOCATIONIRPMMANAGEMENTSETCOMMENTVALUE, 
	LOCATIONIRPMMANAGEMENTSETVALUEVALUE, 
	SETCOMMENTVALUE, 
	SETVALUEVALUE, 
	LOCATIONIRPMEMPLOYEESSETCOMMENTVALUE, 
	LOCATIONIRPMEMPLOYEESSETVALUEVALUE, 
	LOCATIONIRPMLOCATIONSETCOMMENTVALUE, 
	LOCATIONIRPMLOCATIONSETVALUEVALUE, 
	LOCATIONIRPMPREMISESSETCOMMENTVALUE, 
	LOCATIONIRPMPREMISESSETVALUEVALUE, 
	LOCATIONIRPMPROTECTIONSETCOMMENTVALUE, 
	LOCATIONIRPMPROTECTIONSETVALUEVALUE, 
	MULTIPLELOCATIONCREDITFACTOR, 
	INCREASEPERSONALCOMPUTERSCOVERAGEEDPTEXT, 
	INCREASEMONEYANDSECURITIESTEXT, 
	INCREASEOUTDOORDETACHEDSIGNSTEXT, 
	EXCESSPERSONALCOMPUTERSEDPPREMIUM, 
	OUTDOORDETACHEDSIGNSPREMIUM, 
	MONEYANDSECURITIESINSIDEPREMIUM, 
	MONEYANDSECURITIESOUTSIDEPREMIUM
	FROM EXP_Metadata
),
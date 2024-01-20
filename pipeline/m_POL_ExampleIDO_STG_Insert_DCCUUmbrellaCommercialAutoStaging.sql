WITH
SQ_DC_CU_UmbrellaCommercialAuto AS (
	WITH cte_DCCUUmbrellaCommercialAuto(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.CU_UmbrellaCommercialAutoId, 
	X.SessionId, 
	X.Id, 
	X.Description, 
	X.EffectiveDate, 
	X.ExpirationDate, 
	X.ExtraHeavyVehicleCount, 
	X.ExtraHeavyVehicleCountAudited, 
	X.ExtraHeavyVehicleCountEstimated, 
	X.HeavyVehicleCount, 
	X.HeavyVehicleCountAudited, 
	X.HeavyVehicleCountEstimated, 
	X.LightVehicleCount, 
	X.LightVehicleCountAudited, 
	X.LightVehicleCountEstimated, 
	X.MediumVehicleCount, 
	X.MediumVehicleCountAudited, 
	X.MediumVehicleCountEstimated, 
	X.PolicyNumber, 
	X.PolicyType, 
	X.PrivatePassengerVehicleCount, 
	X.PrivatePassengerVehicleCountAudited, 
	X.PrivatePassengerVehicleCountEstimated, 
	X.TerrorismCA, 
	X.TerrorismSelectCA2386, 
	X.TerrorismSelectCA2387 
	FROM
	DC_CU_UmbrellaCommercialAuto X
	inner join
	cte_DCCUUmbrellaCommercialAuto Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	LineId,
	CU_UmbrellaCommercialAutoId,
	SessionId,
	Id,
	Description,
	EffectiveDate,
	ExpirationDate,
	ExtraHeavyVehicleCount,
	ExtraHeavyVehicleCountAudited,
	ExtraHeavyVehicleCountEstimated,
	HeavyVehicleCount,
	HeavyVehicleCountAudited,
	HeavyVehicleCountEstimated,
	LightVehicleCount,
	LightVehicleCountAudited,
	LightVehicleCountEstimated,
	MediumVehicleCount,
	MediumVehicleCountAudited,
	MediumVehicleCountEstimated,
	PolicyNumber,
	PolicyType,
	PrivatePassengerVehicleCount,
	PrivatePassengerVehicleCountAudited,
	PrivatePassengerVehicleCountEstimated,
	TerrorismCA,
	TerrorismSelectCA2386,
	TerrorismSelectCA2387,
	-- *INF*: DECODE(TerrorismCA, 'T', 1, 'F', 0, NULL)
	DECODE(
	    TerrorismCA,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TerrorismCA,
	-- *INF*: DECODE(TerrorismSelectCA2386, 'T', 1, 'F', 0, NULL)
	DECODE(
	    TerrorismSelectCA2386,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TerrorismSelectCA2386,
	-- *INF*: DECODE(TerrorismSelectCA2387, 'T', 1, 'F', 0, NULL)
	DECODE(
	    TerrorismSelectCA2387,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TerrorismSelectCA2387,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CU_UmbrellaCommercialAuto
),
DCCUUmbrellaCommercialAutoStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCUUmbrellaCommercialAutoStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCUUmbrellaCommercialAutoStaging
	(ExtractDate, SourceSystemId, LineId, CU_UmbrellaCommercialAutoId, SessionId, Id, Description, EffectiveDate, ExpirationDate, ExtraHeavyVehicleCount, ExtraHeavyVehicleCountAudited, ExtraHeavyVehicleCountEstimated, HeavyVehicleCount, HeavyVehicleCountAudited, HeavyVehicleCountEstimated, LightVehicleCount, LightVehicleCountAudited, LightVehicleCountEstimated, MediumVehicleCount, MediumVehicleCountAudited, MediumVehicleCountEstimated, PolicyNumber, PolicyType, PrivatePassengerVehicleCount, PrivatePassengerVehicleCountAudited, PrivatePassengerVehicleCountEstimated, TerrorismCA, TerrorismSelectCA2386, TerrorismSelectCA2387)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	LINEID, 
	CU_UMBRELLACOMMERCIALAUTOID, 
	SESSIONID, 
	ID, 
	DESCRIPTION, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	EXTRAHEAVYVEHICLECOUNT, 
	EXTRAHEAVYVEHICLECOUNTAUDITED, 
	EXTRAHEAVYVEHICLECOUNTESTIMATED, 
	HEAVYVEHICLECOUNT, 
	HEAVYVEHICLECOUNTAUDITED, 
	HEAVYVEHICLECOUNTESTIMATED, 
	LIGHTVEHICLECOUNT, 
	LIGHTVEHICLECOUNTAUDITED, 
	LIGHTVEHICLECOUNTESTIMATED, 
	MEDIUMVEHICLECOUNT, 
	MEDIUMVEHICLECOUNTAUDITED, 
	MEDIUMVEHICLECOUNTESTIMATED, 
	POLICYNUMBER, 
	POLICYTYPE, 
	PRIVATEPASSENGERVEHICLECOUNT, 
	PRIVATEPASSENGERVEHICLECOUNTAUDITED, 
	PRIVATEPASSENGERVEHICLECOUNTESTIMATED, 
	o_TerrorismCA AS TERRORISMCA, 
	o_TerrorismSelectCA2386 AS TERRORISMSELECTCA2386, 
	o_TerrorismSelectCA2387 AS TERRORISMSELECTCA2387
	FROM EXP_Metadata
),
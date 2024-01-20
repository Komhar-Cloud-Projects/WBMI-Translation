WITH
SQ_WB_BP_LocationAccount AS (
	WITH cte_WBBPLocationAccount(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.WB_CL_LocationAccountId, 
	X.WB_BP_LocationAccountId, 
	X.SessionId, 
	X.BuildingLessThan1000FtFromFireHydrant, 
	X.BuildingLessThan5MilesFromFireDepartment, 
	X.ProtectionClassOverride, 
	X.QuotedScheduleModification, 
	X.LiabilityOnly, 
	X.TerritoryProtectionClassWithLeadingZero, 
	X.AnyAlcoholSold, 
	X.AnyOutdoorServiceArea, 
	X.LicenseOrPermitNumber, 
	X.TotalSalesOffPremises, 
	X.TotalSalesOnPremises, 
	X.CompleteInformation, 
	X.Q10041HasCookingEquipProtectedServicedPerNFPA, 
	X.Q10045ComplianceWithNFPALifeSafetyCodesRestaurant, 
	X.Q10051RestaurantExposuresCatering, 
	X.Q10058RestaurantExposuresFastFood, 
	X.Q10040AAnyCommercialCooking, 
	X.Q10216HasOnSiteConsumptionOfFoodBeverages, 
	X.Q10150DoesVehicleRepairInstallation 
	FROM
	WB_BP_LocationAccount X
	inner join
	cte_WBBPLocationAccount Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXPTRANS AS (
	SELECT
	WB_CL_LocationAccountId,
	WB_BP_LocationAccountId,
	SessionId,
	BuildingLessThan1000FtFromFireHydrant AS i_BuildingLessThan1000FtFromFireHydrant,
	-- *INF*: DECODE(i_BuildingLessThan1000FtFromFireHydrant,'T','1','F','0',NULL)
	DECODE(
	    i_BuildingLessThan1000FtFromFireHydrant,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_BuildingLessThan1000FtFromFireHydrant,
	BuildingLessThan5MilesFromFireDepartment AS i_BuildingLessThan5MilesFromFireDepartment,
	-- *INF*: DECODE(i_BuildingLessThan5MilesFromFireDepartment,'T','1','F','0',NULL)
	DECODE(
	    i_BuildingLessThan5MilesFromFireDepartment,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_BuildingLessThan5MilesFromFireDepartment,
	ProtectionClassOverride,
	QuotedScheduleModification,
	LiabilityOnly,
	TerritoryProtectionClassWithLeadingZero,
	AnyAlcoholSold,
	AnyOutdoorServiceArea,
	LicenseOrPermitNumber,
	TotalSalesOffPremises,
	TotalSalesOnPremises,
	CompleteInformation AS i_CompleteInformation,
	-- *INF*: DECODE(i_CompleteInformation,'T','1','F','0',NULL)
	DECODE(
	    i_CompleteInformation,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_CompleteInformation,
	Q10041HasCookingEquipProtectedServicedPerNFPA,
	Q10045ComplianceWithNFPALifeSafetyCodesRestaurant,
	Q10051RestaurantExposuresCatering,
	Q10058RestaurantExposuresFastFood,
	Q10040AAnyCommercialCooking,
	Q10216HasOnSiteConsumptionOfFoodBeverages,
	Q10150DoesVehicleRepairInstallation,
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_BP_LocationAccount
),
WBBPLocationAccountStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBBPLocationAccountStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBBPLocationAccountStage
	(ExtractDate, SourceSystemId, WB_CL_LocationAccountId, WB_BP_LocationAccountId, SessionId, BuildingLessThan1000FtFromFireHydrant, BuildingLessThan5MilesFromFireDepartment, ProtectionClassOverride, QuotedScheduleModification, LiabilityOnly, TerritoryProtectionClassWithLeadingZero, AnyAlcoholSold, AnyOutdoorServiceArea, LicenseOrPermitNumber, TotalSalesOffPremises, TotalSalesOnPremises, CompleteInformation, Q10041HasCookingEquipProtectedServicedPerNFPA, Q10045ComplianceWithNFPALifeSafetyCodesRestaurant, Q10051RestaurantExposuresCatering, Q10058RestaurantExposuresFastFood, Q10040AAnyCommercialCooking, Q10216HasOnSiteConsumptionOfFoodBeverages, Q10150DoesVehicleRepairInstallation)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	WB_CL_LOCATIONACCOUNTID, 
	WB_BP_LOCATIONACCOUNTID, 
	SESSIONID, 
	o_BuildingLessThan1000FtFromFireHydrant AS BUILDINGLESSTHAN1000FTFROMFIREHYDRANT, 
	o_BuildingLessThan5MilesFromFireDepartment AS BUILDINGLESSTHAN5MILESFROMFIREDEPARTMENT, 
	PROTECTIONCLASSOVERRIDE, 
	QUOTEDSCHEDULEMODIFICATION, 
	LIABILITYONLY, 
	TERRITORYPROTECTIONCLASSWITHLEADINGZERO, 
	ANYALCOHOLSOLD, 
	ANYOUTDOORSERVICEAREA, 
	LICENSEORPERMITNUMBER, 
	TOTALSALESOFFPREMISES, 
	TOTALSALESONPREMISES, 
	o_CompleteInformation AS COMPLETEINFORMATION, 
	Q10041HASCOOKINGEQUIPPROTECTEDSERVICEDPERNFPA, 
	Q10045COMPLIANCEWITHNFPALIFESAFETYCODESRESTAURANT, 
	Q10051RESTAURANTEXPOSURESCATERING, 
	Q10058RESTAURANTEXPOSURESFASTFOOD, 
	Q10040AANYCOMMERCIALCOOKING, 
	Q10216HASONSITECONSUMPTIONOFFOODBEVERAGES, 
	Q10150DOESVEHICLEREPAIRINSTALLATION
	FROM EXPTRANS
),
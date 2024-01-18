WITH
SQ_WBBPLocationAccountStage AS (
	SELECT
		WBBPLocationAccountStageId,
		ExtractDate,
		SourceSystemId,
		WB_CL_LocationAccountId,
		WB_BP_LocationAccountId,
		SessionId,
		BuildingLessThan1000FtFromFireHydrant,
		BuildingLessThan5MilesFromFireDepartment,
		ProtectionClassOverride,
		QuotedScheduleModification,
		LiabilityOnly,
		TerritoryProtectionClassWithLeadingZero,
		AnyAlcoholSold,
		AnyOutdoorServiceArea,
		LicenseOrPermitNumber,
		TotalSalesOffPremises,
		TotalSalesOnPremises,
		CompleteInformation,
		Q10041HasCookingEquipProtectedServicedPerNFPA,
		Q10045ComplianceWithNFPALifeSafetyCodesRestaurant,
		Q10051RestaurantExposuresCatering,
		Q10058RestaurantExposuresFastFood,
		Q10040AAnyCommercialCooking,
		Q10216HasOnSiteConsumptionOfFoodBeverages,
		Q10150DoesVehicleRepairInstallation
	FROM WBBPLocationAccountStage
),
EXPTRANS AS (
	SELECT
	WBBPLocationAccountStageId,
	ExtractDate,
	SourceSystemId,
	WB_CL_LocationAccountId,
	WB_BP_LocationAccountId,
	SessionId,
	BuildingLessThan1000FtFromFireHydrant AS I_BuildingLessThan1000FtFromFireHydrant,
	-- *INF*: DECODE(I_BuildingLessThan1000FtFromFireHydrant,'T','1','F','0',NULL)
	DECODE(
	    I_BuildingLessThan1000FtFromFireHydrant,
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
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBBPLocationAccountStage
),
ArchWBBPLocationAccountStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBBPLocationAccountStage
	(ExtractDate, SourceSystemId, AuditId, WBBPLocationAccountStageId, WB_CL_LocationAccountId, WB_BP_LocationAccountId, SessionId, BuildingLessThan1000FtFromFireHydrant, BuildingLessThan5MilesFromFireDepartment, ProtectionClassOverride, QuotedScheduleModification, LiabilityOnly, TerritoryProtectionClassWithLeadingZero, AnyAlcoholSold, AnyOutdoorServiceArea, LicenseOrPermitNumber, TotalSalesOffPremises, TotalSalesOnPremises, CompleteInformation, Q10041HasCookingEquipProtectedServicedPerNFPA, Q10045ComplianceWithNFPALifeSafetyCodesRestaurant, Q10051RestaurantExposuresCatering, Q10058RestaurantExposuresFastFood, Q10040AAnyCommercialCooking, Q10216HasOnSiteConsumptionOfFoodBeverages, Q10150DoesVehicleRepairInstallation)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBBPLOCATIONACCOUNTSTAGEID, 
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
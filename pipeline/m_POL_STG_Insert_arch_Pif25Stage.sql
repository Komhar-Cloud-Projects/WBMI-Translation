WITH
SQ_Pif25Stage AS (
	SELECT
		Pif25StageId,
		PifSymbol,
		PifPolicyNumber,
		PifModule,
		DriverRecLength,
		DriverActionCode,
		DriverFileId,
		DriverKey,
		DriverId,
		DriverName,
		DriverLicenseNumber,
		DOBYear,
		DOBMonth,
		DOBDay,
		DriverLicenseState,
		DriverSex,
		DriverMaritalStatus,
		DriverRelationToInds,
		PoVehicleNo1,
		PoVehicleNo2,
		PoVehicleNo3,
		PtVehicleNo1,
		PtVehicleNo2,
		PtVehicleNo3,
		DriverAgeAlph,
		DriverLastMvrDateMM,
		DriverLastMvrDateDD,
		DriverLastMvrDateYY,
		DriverCfAttribute,
		DrivAttrValue1,
		AttributeYear1,
		AttributeMonth1,
		AttributeDay1,
		DrivAttrValue2,
		AttributeYear2,
		AttributeMonth2,
		AttributeDay2,
		DrivAttrValue3,
		AttributeYear3,
		AttributeMonth3,
		AttributeDay3,
		DrivAttrValue4,
		AttributeYear4,
		AttributeMonth4,
		AttributeDay4,
		DrivAttrValue5,
		AttributeYear5,
		AttributeMonth5,
		AttributeDay5,
		DrivAttrValue6,
		AttributeYear6,
		AttributeMonth6,
		AttributeDay6,
		DrivAttrValue7,
		AttributeYear7,
		AttributeMonth7,
		AttributeDay7,
		DrivAttrValue8,
		AttributeYear8,
		AttributeMonth8,
		AttributeDay8,
		DrivAttrValue9,
		AttributeYear9,
		AttributeMonth9,
		AttributeDay9,
		DrivAttrValue10,
		AttributeYear10,
		AttributeMonth10,
		AttributeDay10,
		DrivAttrValue11,
		AttributeYear11,
		AttributeMonth11,
		AttributeDay11,
		DrivAttrValue12,
		AttributeYear12,
		AttributeMonth12,
		AttributeDay12,
		DrivAttrValue13,
		AttributeYear13,
		AttributeMonth13,
		AttributeDay13,
		DrivAttrValue14,
		AttributeYear14,
		AttributeMonth14,
		AttributeDay14,
		DrivAttrValue15,
		AttributeYear15,
		AttributeMonth15,
		AttributeDay15,
		DriverAaaInd,
		DldYear,
		DldMonth,
		DldDay,
		DriverNumYearsLicensed,
		DriverMajorconvictions,
		DriverMinorconvictions,
		DriverBiAccidents,
		DriverPdAccidents,
		DriverPmsfutureUse,
		DriverMinAttrbInd,
		DriverAdvantagetier,
		DriverWbcMvrOrderInd,
		DriverLicensedInd,
		DriverCustFutureUse,
		DriverYr2000CustUse,
		DriverDupKeySeqNum,
		ExtractDate,
		SourceSystemId
	FROM Pif25Stage
),
EXP_arch_pif_25_stage AS (
	SELECT
	Pif25StageId AS pif_25_stage_id,
	PifSymbol AS PIF_SYMBOL,
	PifPolicyNumber AS PIF_POLICY_NUMBER,
	PifModule AS PIF_MODULE,
	DriverRecLength AS DIRVER_REC_LENGTH,
	DriverActionCode AS DRIVER_ACTION_CODE,
	DriverFileId AS DRIVER_FILE_ID,
	DriverKey AS DRIVER_KEY,
	DriverId AS DRIVER_ID,
	DriverName AS DRIVER_NAME,
	DriverLicenseNumber AS i_DRIVER_LICENSE_NUMBER,
	NULL AS o_DRIVER_LICENSE_NUMBER,
	DOBYear AS DOB_YEAR,
	DOBMonth AS DOB_MONTH,
	DOBDay AS DOB_DAY,
	DriverLicenseState AS DRIVER_LICENSE_STATE,
	DriverSex AS DRIVER_SEX,
	DriverMaritalStatus AS DRIVER_MARITAL_STATUS,
	DriverRelationToInds AS DRIVER_RELATION_TO_INDS,
	PoVehicleNo1 AS PO_VEHICLE_NO_1,
	PoVehicleNo2 AS PO_VEHICLE_NO_2,
	PoVehicleNo3 AS PO_VEHICLE_NO_3,
	PtVehicleNo1 AS PT_VEHICLE_NO_1,
	PtVehicleNo2 AS PT_VEHICLE_NO_2,
	PtVehicleNo3 AS PT_VEHICLE_NO_3,
	DriverAgeAlph AS DRIVER_AGE_ALPH,
	DriverLastMvrDateMM AS DRIVER_LAST_MVR_DATE_MM,
	DriverLastMvrDateDD AS DRIVER_LAST_MVR_DATE_DD,
	DriverLastMvrDateYY AS DRIVER_LAST_MVR_DATE_YY,
	DriverCfAttribute AS DRIVER_CF_ATTRIBUTE,
	DrivAttrValue1 AS DRIV_ATTR_VALUE_1,
	AttributeYear1 AS ATTRIBUTE_YEAR_1,
	AttributeMonth1 AS ATTRIBUTE_MONTH_1,
	AttributeDay1 AS ATTRIBUTE_DAY_1,
	DrivAttrValue2 AS DRIV_ATTR_VALUE_2,
	AttributeYear2 AS ATTRIBUTE_YEAR_2,
	AttributeMonth2 AS ATTRIBUTE_MONTH_2,
	AttributeDay2 AS ATTRIBUTE_DAY_2,
	DrivAttrValue3 AS DRIV_ATTR_VALUE_3,
	AttributeYear3 AS ATTRIBUTE_YEAR_3,
	AttributeMonth3 AS ATTRIBUTE_MONTH_3,
	AttributeDay3 AS ATTRIBUTE_DAY_3,
	DrivAttrValue4 AS DRIV_ATTR_VALUE_4,
	AttributeYear4 AS ATTRIBUTE_YEAR_4,
	AttributeMonth4 AS ATTRIBUTE_MONTH_4,
	AttributeDay4 AS ATTRIBUTE_DAY_4,
	DrivAttrValue5 AS DRIV_ATTR_VALUE_5,
	AttributeYear5 AS ATTRIBUTE_YEAR_5,
	AttributeMonth5 AS ATTRIBUTE_MONTH_5,
	AttributeDay5 AS ATTRIBUTE_DAY_5,
	DrivAttrValue6 AS DRIV_ATTR_VALUE_6,
	AttributeYear6 AS ATTRIBUTE_YEAR_6,
	AttributeMonth6 AS ATTRIBUTE_MONTH_6,
	AttributeDay6 AS ATTRIBUTE_DAY_6,
	DrivAttrValue7 AS DRIV_ATTR_VALUE_7,
	AttributeYear7 AS ATTRIBUTE_YEAR_7,
	AttributeMonth7 AS ATTRIBUTE_MONTH_7,
	AttributeDay7 AS ATTRIBUTE_DAY_7,
	DrivAttrValue8 AS DRIV_ATTR_VALUE_8,
	AttributeYear8 AS ATTRIBUTE_YEAR_8,
	AttributeMonth8 AS ATTRIBUTE_MONTH_8,
	AttributeDay8 AS ATTRIBUTE_DAY_8,
	DrivAttrValue9 AS DRIV_ATTR_VALUE_9,
	AttributeYear9 AS ATTRIBUTE_YEAR_9,
	AttributeMonth9 AS ATTRIBUTE_MONTH_9,
	AttributeDay9 AS ATTRIBUTE_DAY_9,
	DrivAttrValue10 AS DRIV_ATTR_VALUE_10,
	AttributeYear10 AS ATTRIBUTE_YEAR_10,
	AttributeMonth10 AS ATTRIBUTE_MONTH_10,
	AttributeDay10 AS ATTRIBUTE_DAY_10,
	DrivAttrValue11 AS DRIV_ATTR_VALUE_11,
	AttributeYear11 AS ATTRIBUTE_YEAR_11,
	AttributeMonth11 AS ATTRIBUTE_MONTH_11,
	AttributeDay11 AS ATTRIBUTE_DAY_11,
	DrivAttrValue12 AS DRIV_ATTR_VALUE_12,
	AttributeYear12 AS ATTRIBUTE_YEAR_12,
	AttributeMonth12 AS ATTRIBUTE_MONTH_12,
	AttributeDay12 AS ATTRIBUTE_DAY_12,
	DrivAttrValue13 AS DRIV_ATTR_VALUE_13,
	AttributeYear13 AS ATTRIBUTE_YEAR_13,
	AttributeMonth13 AS ATTRIBUTE_MONTH_13,
	AttributeDay13 AS ATTRIBUTE_DAY_13,
	DrivAttrValue14 AS DRIV_ATTR_VALUE_14,
	AttributeYear14 AS ATTRIBUTE_YEAR_14,
	AttributeMonth14 AS ATTRIBUTE_MONTH_14,
	AttributeDay14 AS ATTRIBUTE_DAY_14,
	DrivAttrValue15 AS DRIV_ATTR_VALUE_15,
	AttributeYear15 AS ATTRIBUTE_YEAR_15,
	AttributeMonth15 AS ATTRIBUTE_MONTH_15,
	AttributeDay15 AS ATTRIBUTE_DAY_15,
	DriverAaaInd AS DRIVER_AAA_IND,
	DldYear AS DLD_YEAR,
	DldMonth AS DLD_MONTH,
	DldDay AS DLD_DAY,
	DriverNumYearsLicensed AS DRIVER_NUM_YEARS_LICENSED,
	DriverMajorconvictions AS DRIVER_MAJOR_CONVICTIONS,
	DriverMinorconvictions AS DRIVER_MINOR_CONVICTIONS,
	DriverBiAccidents AS DRIVER_BI_ACCIDENTS,
	DriverPdAccidents AS DRIVER_PD_ACCIDENTS,
	DriverPmsfutureUse AS DRIVER_PMS_FUTURE_USE,
	DriverMinAttrbInd AS DRIVER_MIN_ATTRB_IND,
	DriverAdvantagetier AS DRIVER_ADVANTAGE_TIER,
	DriverWbcMvrOrderInd AS DRIVER_WBC_MVR_ORDER_IND,
	DriverLicensedInd AS DRIVER_LICENSED_IND,
	DriverCustFutureUse AS DRIVER_CUST_FUTURE_USE,
	DriverYr2000CustUse AS DRIVER_YR2000_CUST_USE,
	DriverDupKeySeqNum AS DRIVER_DUP_KEY_SEQ_NUM,
	ExtractDate AS EXTRACT_DATE,
	SourceSystemId AS SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID
	FROM SQ_Pif25Stage
),
ArchPif25Stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchPif25Stage
	(Pif25StageId, PifSymbol, PifPolicyNumber, PifModule, DriverRecLength, DriverActionCode, DriverFileId, DriverKey, DriverId, DriverName, DriverLicenseNumber, DOBYear, DOBMonth, DOBDay, DriverLicenseState, DriverSex, DriverMaritalStatus, DriverRelationToInds, PoVehicleNo1, PoVehicleNo2, PoVehicleNo3, PtVehicleNo1, PtVehicleNo2, PtVehicleNo3, DriverAgeAlph, DriverLastMvrDateMM, DriverLastMvrDateDD, DriverLastMvrDateYY, DriverCfAttribute, DrivAttrValue1, AttributeYear1, AttributeMonth1, AttributeDay1, DrivAttrValue2, AttributeYear2, AttributeMonth2, AttributeDay2, DrivAttrValue3, AttributeYear3, AttributeMonth3, AttributeDay3, DrivAttrValue4, AttributeYear4, AttributeMonth4, AttributeDay4, DrivAttrValue5, AttributeYear5, AttributeMonth5, AttributeDay5, DrivAttrValue6, AttributeYear6, AttributeMonth6, AttributeDay6, DrivAttrValue7, AttributeYear7, AttributeMonth7, AttributeDay7, DrivAttrValue8, AttributeYear8, AttributeMonth8, AttributeDay8, DrivAttrValue9, AttributeYear9, AttributeMonth9, AttributeDay9, DrivAttrValue10, AttributeYear10, AttributeMonth10, AttributeDay10, DrivAttrValue11, AttributeYear11, AttributeMonth11, AttributeDay11, DrivAttrValue12, AttributeYear12, AttributeMonth12, AttributeDay12, DrivAttrValue13, AttributeYear13, AttributeMonth13, AttributeDay13, DrivAttrValue14, AttributeYear14, AttributeMonth14, AttributeDay14, DrivAttrValue15, AttributeYear15, AttributeMonth15, AttributeDay15, DriverAaaInd, DldYear, DldMonth, DldDay, DriverNumYearsLicensed, DriverMajorconvictions, DriverMinorconvictions, DriverBiAccidents, DriverPdAccidents, DriverPmsfutureUse, DriverMinAttrbInd, DriverAdvantagetier, DriverWbcMvrOrderInd, DriverLicensedInd, DriverCustFutureUse, DriverYr2000CustUse, DriverDupKeySeqNum, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	pif_25_stage_id AS PIF25STAGEID, 
	PIF_SYMBOL AS PIFSYMBOL, 
	PIF_POLICY_NUMBER AS PIFPOLICYNUMBER, 
	PIF_MODULE AS PIFMODULE, 
	DIRVER_REC_LENGTH AS DRIVERRECLENGTH, 
	DRIVER_ACTION_CODE AS DRIVERACTIONCODE, 
	DRIVER_FILE_ID AS DRIVERFILEID, 
	DRIVER_KEY AS DRIVERKEY, 
	DRIVER_ID AS DRIVERID, 
	DRIVER_NAME AS DRIVERNAME, 
	o_DRIVER_LICENSE_NUMBER AS DRIVERLICENSENUMBER, 
	DOB_YEAR AS DOBYEAR, 
	DOB_MONTH AS DOBMONTH, 
	DOB_DAY AS DOBDAY, 
	DRIVER_LICENSE_STATE AS DRIVERLICENSESTATE, 
	DRIVER_SEX AS DRIVERSEX, 
	DRIVER_MARITAL_STATUS AS DRIVERMARITALSTATUS, 
	DRIVER_RELATION_TO_INDS AS DRIVERRELATIONTOINDS, 
	PO_VEHICLE_NO_1 AS POVEHICLENO1, 
	PO_VEHICLE_NO_2 AS POVEHICLENO2, 
	PO_VEHICLE_NO_3 AS POVEHICLENO3, 
	PT_VEHICLE_NO_1 AS PTVEHICLENO1, 
	PT_VEHICLE_NO_2 AS PTVEHICLENO2, 
	PT_VEHICLE_NO_3 AS PTVEHICLENO3, 
	DRIVER_AGE_ALPH AS DRIVERAGEALPH, 
	DRIVER_LAST_MVR_DATE_MM AS DRIVERLASTMVRDATEMM, 
	DRIVER_LAST_MVR_DATE_DD AS DRIVERLASTMVRDATEDD, 
	DRIVER_LAST_MVR_DATE_YY AS DRIVERLASTMVRDATEYY, 
	DRIVER_CF_ATTRIBUTE AS DRIVERCFATTRIBUTE, 
	DRIV_ATTR_VALUE_1 AS DRIVATTRVALUE1, 
	ATTRIBUTE_YEAR_1 AS ATTRIBUTEYEAR1, 
	ATTRIBUTE_MONTH_1 AS ATTRIBUTEMONTH1, 
	ATTRIBUTE_DAY_1 AS ATTRIBUTEDAY1, 
	DRIV_ATTR_VALUE_2 AS DRIVATTRVALUE2, 
	ATTRIBUTE_YEAR_2 AS ATTRIBUTEYEAR2, 
	ATTRIBUTE_MONTH_2 AS ATTRIBUTEMONTH2, 
	ATTRIBUTE_DAY_2 AS ATTRIBUTEDAY2, 
	DRIV_ATTR_VALUE_3 AS DRIVATTRVALUE3, 
	ATTRIBUTE_YEAR_3 AS ATTRIBUTEYEAR3, 
	ATTRIBUTE_MONTH_3 AS ATTRIBUTEMONTH3, 
	ATTRIBUTE_DAY_3 AS ATTRIBUTEDAY3, 
	DRIV_ATTR_VALUE_4 AS DRIVATTRVALUE4, 
	ATTRIBUTE_YEAR_4 AS ATTRIBUTEYEAR4, 
	ATTRIBUTE_MONTH_4 AS ATTRIBUTEMONTH4, 
	ATTRIBUTE_DAY_4 AS ATTRIBUTEDAY4, 
	DRIV_ATTR_VALUE_5 AS DRIVATTRVALUE5, 
	ATTRIBUTE_YEAR_5 AS ATTRIBUTEYEAR5, 
	ATTRIBUTE_MONTH_5 AS ATTRIBUTEMONTH5, 
	ATTRIBUTE_DAY_5 AS ATTRIBUTEDAY5, 
	DRIV_ATTR_VALUE_6 AS DRIVATTRVALUE6, 
	ATTRIBUTE_YEAR_6 AS ATTRIBUTEYEAR6, 
	ATTRIBUTE_MONTH_6 AS ATTRIBUTEMONTH6, 
	ATTRIBUTE_DAY_6 AS ATTRIBUTEDAY6, 
	DRIV_ATTR_VALUE_7 AS DRIVATTRVALUE7, 
	ATTRIBUTE_YEAR_7 AS ATTRIBUTEYEAR7, 
	ATTRIBUTE_MONTH_7 AS ATTRIBUTEMONTH7, 
	ATTRIBUTE_DAY_7 AS ATTRIBUTEDAY7, 
	DRIV_ATTR_VALUE_8 AS DRIVATTRVALUE8, 
	ATTRIBUTE_YEAR_8 AS ATTRIBUTEYEAR8, 
	ATTRIBUTE_MONTH_8 AS ATTRIBUTEMONTH8, 
	ATTRIBUTE_DAY_8 AS ATTRIBUTEDAY8, 
	DRIV_ATTR_VALUE_9 AS DRIVATTRVALUE9, 
	ATTRIBUTE_YEAR_9 AS ATTRIBUTEYEAR9, 
	ATTRIBUTE_MONTH_9 AS ATTRIBUTEMONTH9, 
	ATTRIBUTE_DAY_9 AS ATTRIBUTEDAY9, 
	DRIV_ATTR_VALUE_10 AS DRIVATTRVALUE10, 
	ATTRIBUTE_YEAR_10 AS ATTRIBUTEYEAR10, 
	ATTRIBUTE_MONTH_10 AS ATTRIBUTEMONTH10, 
	ATTRIBUTE_DAY_10 AS ATTRIBUTEDAY10, 
	DRIV_ATTR_VALUE_11 AS DRIVATTRVALUE11, 
	ATTRIBUTE_YEAR_11 AS ATTRIBUTEYEAR11, 
	ATTRIBUTE_MONTH_11 AS ATTRIBUTEMONTH11, 
	ATTRIBUTE_DAY_11 AS ATTRIBUTEDAY11, 
	DRIV_ATTR_VALUE_12 AS DRIVATTRVALUE12, 
	ATTRIBUTE_YEAR_12 AS ATTRIBUTEYEAR12, 
	ATTRIBUTE_MONTH_12 AS ATTRIBUTEMONTH12, 
	ATTRIBUTE_DAY_12 AS ATTRIBUTEDAY12, 
	DRIV_ATTR_VALUE_13 AS DRIVATTRVALUE13, 
	ATTRIBUTE_YEAR_13 AS ATTRIBUTEYEAR13, 
	ATTRIBUTE_MONTH_13 AS ATTRIBUTEMONTH13, 
	ATTRIBUTE_DAY_13 AS ATTRIBUTEDAY13, 
	DRIV_ATTR_VALUE_14 AS DRIVATTRVALUE14, 
	ATTRIBUTE_YEAR_14 AS ATTRIBUTEYEAR14, 
	ATTRIBUTE_MONTH_14 AS ATTRIBUTEMONTH14, 
	ATTRIBUTE_DAY_14 AS ATTRIBUTEDAY14, 
	DRIV_ATTR_VALUE_15 AS DRIVATTRVALUE15, 
	ATTRIBUTE_YEAR_15 AS ATTRIBUTEYEAR15, 
	ATTRIBUTE_MONTH_15 AS ATTRIBUTEMONTH15, 
	ATTRIBUTE_DAY_15 AS ATTRIBUTEDAY15, 
	DRIVER_AAA_IND AS DRIVERAAAIND, 
	DLD_YEAR AS DLDYEAR, 
	DLD_MONTH AS DLDMONTH, 
	DLD_DAY AS DLDDAY, 
	DRIVER_NUM_YEARS_LICENSED AS DRIVERNUMYEARSLICENSED, 
	DRIVER_MAJOR_CONVICTIONS AS DRIVERMAJORCONVICTIONS, 
	DRIVER_MINOR_CONVICTIONS AS DRIVERMINORCONVICTIONS, 
	DRIVER_BI_ACCIDENTS AS DRIVERBIACCIDENTS, 
	DRIVER_PD_ACCIDENTS AS DRIVERPDACCIDENTS, 
	DRIVER_PMS_FUTURE_USE AS DRIVERPMSFUTUREUSE, 
	DRIVER_MIN_ATTRB_IND AS DRIVERMINATTRBIND, 
	DRIVER_ADVANTAGE_TIER AS DRIVERADVANTAGETIER, 
	DRIVER_WBC_MVR_ORDER_IND AS DRIVERWBCMVRORDERIND, 
	DRIVER_LICENSED_IND AS DRIVERLICENSEDIND, 
	DRIVER_CUST_FUTURE_USE AS DRIVERCUSTFUTUREUSE, 
	DRIVER_YR2000_CUST_USE AS DRIVERYR2000CUSTUSE, 
	DRIVER_DUP_KEY_SEQ_NUM AS DRIVERDUPKEYSEQNUM, 
	EXTRACT_DATE AS EXTRACTDATE, 
	SOURCE_SYSTEM_ID AS SOURCESYSTEMID, 
	AUDIT_ID AS AUDITID
	FROM EXP_arch_pif_25_stage
),
WITH
SQ_CommAutoClass AS (

-- TODO Manual --

),
EXP_Detect_Changes AS (
	SELECT
	LineOfBusinessAbbreviation AS i_LineOfBusinessAbbreviation,
	RatingStateCode AS i_RatingStateCode,
	EffectiveDate AS i_ClassEffectiveDate,
	ExpirationDate AS i_ClassExpirationDate,
	ClassCode AS i_ClassCode,
	ClassDescription AS i_ClassDescription,
	OriginatingOrganizationCode AS i_ClassCodeOriginatingOrganization,
	VehicleTypeSize AS i_CommercialAutoVehicleTypeSize,
	BusinessUseClass AS i_CommercialAutoBusinessUseClass,
	SecondaryClass AS i_SecondaryClass,
	RadiusofOperation AS i_RadiusofOperation,
	FleetType AS i_FleetType,
	SecondaryClassGroup AS i_SecondaryClassGroup,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	sysdate AS o_CreatedDate,
	sysdate AS o_ModifiedDate,
	-- *INF*: LTRIM(RTRIM(i_LineOfBusinessAbbreviation))
	LTRIM(RTRIM(i_LineOfBusinessAbbreviation)) AS o_LineOfBusinessAbbreviation,
	i_RatingStateCode AS o_RatingStateCode,
	-- *INF*: TO_DATE(  SUBSTR( i_ClassEffectiveDate ,1,19 )  ,'YYYY-MM-DD HH24:MI:SS')
	TO_DATE(SUBSTR(i_ClassEffectiveDate, 1, 19), 'YYYY-MM-DD HH24:MI:SS') AS o_ClassEffectiveDate,
	-- *INF*: TO_DATE(SUBSTR(i_ClassExpirationDate , 1,19  ),'YYYY-MM-DD HH24:MI:SS')
	TO_DATE(SUBSTR(i_ClassExpirationDate, 1, 19), 'YYYY-MM-DD HH24:MI:SS') AS o_ClassExpirationDate,
	i_ClassCode AS o_ClassCode,
	-- *INF*: LTRIM(RTRIM(i_ClassDescription))
	LTRIM(RTRIM(i_ClassDescription)) AS o_ClassDescription,
	-- *INF*: LTRIM(RTRIM(i_ClassCodeOriginatingOrganization))
	LTRIM(RTRIM(i_ClassCodeOriginatingOrganization)) AS o_ClassCodeOriginatingOrganization,
	-- *INF*: LTRIM(RTRIM(i_CommercialAutoVehicleTypeSize))
	LTRIM(RTRIM(i_CommercialAutoVehicleTypeSize)) AS o_CommercialAutoVehicleTypeSize,
	-- *INF*: LTRIM(RTRIM(i_CommercialAutoBusinessUseClass))
	LTRIM(RTRIM(i_CommercialAutoBusinessUseClass)) AS o_CommercialAutoBusinessUseClass,
	-- *INF*: LTRIM(RTRIM(i_SecondaryClass))
	LTRIM(RTRIM(i_SecondaryClass)) AS o_SecondaryClass,
	-- *INF*: LTRIM(RTRIM(i_RadiusofOperation))
	LTRIM(RTRIM(i_RadiusofOperation)) AS o_RadiusofOperation,
	-- *INF*: LTRIM(RTRIM(i_FleetType))
	LTRIM(RTRIM(i_FleetType)) AS o_FleetType,
	-- *INF*: LTRIM(RTRIM(i_SecondaryClassGroup))
	LTRIM(RTRIM(i_SecondaryClassGroup)) AS o_SecondaryClassGroup
	FROM SQ_CommAutoClass
),
SupClassificationCommercialAuto_IR AS (
	TRUNCATE TABLE SupClassificationCommercialAuto;
	INSERT INTO SupClassificationCommercialAuto
	(AuditId, CreatedDate, ModifiedDate, LineOfBusinessAbbreviation, RatingStateCode, EffectiveDate, ExpirationDate, ClassCode, ClassDescription, OriginatingOrganizationCode, VehicleTypeSize, BusinessUseClass, SecondaryClass, RadiusofOperation, FleetType, SecondaryClassGroup)
	SELECT 
	o_AuditId AS AUDITID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_LineOfBusinessAbbreviation AS LINEOFBUSINESSABBREVIATION, 
	o_RatingStateCode AS RATINGSTATECODE, 
	o_ClassEffectiveDate AS EFFECTIVEDATE, 
	o_ClassExpirationDate AS EXPIRATIONDATE, 
	o_ClassCode AS CLASSCODE, 
	o_ClassDescription AS CLASSDESCRIPTION, 
	o_ClassCodeOriginatingOrganization AS ORIGINATINGORGANIZATIONCODE, 
	o_CommercialAutoVehicleTypeSize AS VEHICLETYPESIZE, 
	o_CommercialAutoBusinessUseClass AS BUSINESSUSECLASS, 
	o_SecondaryClass AS SECONDARYCLASS, 
	o_RadiusofOperation AS RADIUSOFOPERATION, 
	o_FleetType AS FLEETTYPE, 
	o_SecondaryClassGroup AS SECONDARYCLASSGROUP
	FROM EXP_Detect_Changes
),
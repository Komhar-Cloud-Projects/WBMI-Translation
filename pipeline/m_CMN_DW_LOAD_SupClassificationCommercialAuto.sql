WITH
LKP_SupClassificationCommercialAuto_CurrentChangeFlag AS (
	SELECT
	SupClassificationCommercialAutoID,
	RatingStateCode,
	ClassCode,
	EffectiveDate,
	ClassDescription,
	OriginatingOrganizationCode,
	VehicleTypeSize,
	BusinessUseClass,
	SecondaryClass,
	RadiusofOperation,
	FleetType,
	SecondaryClassGroup
	FROM (
		SELECT 
			SupClassificationCommercialAutoID,
			RatingStateCode,
			ClassCode,
			EffectiveDate,
			ClassDescription,
			OriginatingOrganizationCode,
			VehicleTypeSize,
			BusinessUseClass,
			SecondaryClass,
			RadiusofOperation,
			FleetType,
			SecondaryClassGroup
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationCommercialAuto
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingStateCode,ClassCode,EffectiveDate,ClassDescription,OriginatingOrganizationCode,VehicleTypeSize,BusinessUseClass,SecondaryClass,RadiusofOperation,FleetType,SecondaryClassGroup ORDER BY SupClassificationCommercialAutoID) = 1
),
SQ_SupClassificationCommercialAuto AS (
	SELECT
		SupClassificationCommercialAutoId AS SupClassificationCommercialAutoID,
		AuditId,
		CreatedDate,
		ModifiedDate,
		LineOfBusinessAbbreviation,
		RatingStateCode,
		EffectiveDate,
		ExpirationDate,
		ClassCode,
		ClassDescription,
		OriginatingOrganizationCode AS ClassCodeOriginatingOrganization,
		VehicleTypeSize,
		BusinessUseClass,
		SecondaryClass,
		RadiusofOperation,
		FleetType,
		SecondaryClassGroup
	FROM SupClassificationCommercialAuto
	INNER JOIN SupClassificationCommercialAuto
),
LKP_SupClassificationCommercialAuto AS (
	SELECT
	SupClassificationCommercialAutoID,
	RatingStateCode,
	ClassCode,
	EffectiveDate,
	ExpirationDate,
	ClassDescription,
	OriginatingOrganizationCode,
	VehicleTypeSize,
	BusinessUseClass,
	SecondaryClass,
	RadiusofOperation,
	FleetType,
	SecondaryClassGroup
	FROM (
		SELECT 
			SupClassificationCommercialAutoID,
			RatingStateCode,
			ClassCode,
			EffectiveDate,
			ExpirationDate,
			ClassDescription,
			OriginatingOrganizationCode,
			VehicleTypeSize,
			BusinessUseClass,
			SecondaryClass,
			RadiusofOperation,
			FleetType,
			SecondaryClassGroup
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationCommercialAuto
		WHERE CurrentSnapshotFlag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingStateCode,ClassCode,OriginatingOrganizationCode ORDER BY SupClassificationCommercialAutoID) = 1
),
EXP_Detect_Changes AS (
	SELECT
	SQ_SupClassificationCommercialAuto.LineOfBusinessAbbreviation AS i_LineOfBusinessAbbreviation,
	SQ_SupClassificationCommercialAuto.RatingStateCode AS i_RatingStateCode,
	SQ_SupClassificationCommercialAuto.EffectiveDate AS i_EffectiveDate,
	SQ_SupClassificationCommercialAuto.ExpirationDate AS i_ExpirationDate,
	SQ_SupClassificationCommercialAuto.ClassCode AS i_ClassCode,
	SQ_SupClassificationCommercialAuto.ClassDescription AS i_ClassDescription,
	SQ_SupClassificationCommercialAuto.ClassCodeOriginatingOrganization AS i_ClassCodeOriginatingOrganization,
	SQ_SupClassificationCommercialAuto.VehicleTypeSize AS i_CommercialAutoVehicleTypeSize,
	SQ_SupClassificationCommercialAuto.BusinessUseClass AS i_CommercialAutoBusinessUseClass,
	SQ_SupClassificationCommercialAuto.SecondaryClass AS i_SecondaryClass,
	SQ_SupClassificationCommercialAuto.RadiusofOperation AS i_RadiusofOperation,
	SQ_SupClassificationCommercialAuto.FleetType AS i_FleetType,
	SQ_SupClassificationCommercialAuto.SecondaryClassGroup AS i_SecondaryClassGroup,
	LKP_SupClassificationCommercialAuto.SupClassificationCommercialAutoID AS lkp_SupClassificationCommercialAutoID,
	LKP_SupClassificationCommercialAuto.RatingStateCode AS lkp_RatingStateCode,
	LKP_SupClassificationCommercialAuto.ClassCode AS lkp_ClassCode,
	LKP_SupClassificationCommercialAuto.EffectiveDate AS lkp_EffectiveDate,
	LKP_SupClassificationCommercialAuto.ExpirationDate AS lkp_ExpirationDate,
	LKP_SupClassificationCommercialAuto.ClassDescription AS lkp_ClassDescription,
	LKP_SupClassificationCommercialAuto.OriginatingOrganizationCode AS lkp_ClassCodeOriginatingOrganization,
	LKP_SupClassificationCommercialAuto.VehicleTypeSize AS lkp_CommercialAutoVehicleTypeSize,
	LKP_SupClassificationCommercialAuto.BusinessUseClass AS lkp_CommercialAutoBusinessUseClass,
	LKP_SupClassificationCommercialAuto.SecondaryClass AS lkp_SecondaryClass,
	LKP_SupClassificationCommercialAuto.RadiusofOperation AS lkp_RadiusofOperation,
	LKP_SupClassificationCommercialAuto.FleetType AS lkp_FleetType,
	LKP_SupClassificationCommercialAuto.SecondaryClassGroup AS lkp_SecondaryClassGroup,
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL(:LKP.LKP_SupClassificationCommercialAuto_CurrentChangeFlag(i_RatingStateCode,i_ClassCode,i_EffectiveDate,i_ClassDescription,i_ClassCodeOriginatingOrganization,i_CommercialAutoVehicleTypeSize,i_CommercialAutoBusinessUseClass,i_SecondaryClass,i_RadiusofOperation,i_FleetType,i_SecondaryClassGroup)),
	-- 'NOCHANGE',
	-- 'INSERT')						
	-- 
	-- 
	-- 
	-- 
	-- 
	--  
	DECODE(TRUE,
		LKP_SUPCLASSIFICATIONCOMMERCIALAUTO_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_ClassCodeOriginatingOrganization_i_CommercialAutoVehicleTypeSize_i_CommercialAutoBusinessUseClass_i_SecondaryClass_i_RadiusofOperation_i_FleetType_i_SecondaryClassGroup.SupClassificationCommercialAutoID IS NOT NULL, 'NOCHANGE',
		'INSERT'
	) AS v_RecordPopulated,
	-- *INF*: DECODE(TRUE,
	-- --i_ExpirationDate   <=  lkp_EffectiveDate OR 1=0 , 'NOCHANGE',
	-- i_ExpirationDate   <=  lkp_EffectiveDate OR v_RecordPopulated = 'NOCHANGE', 'NOCHANGE',
	-- ISNULL(lkp_SupClassificationCommercialAutoID) OR ( i_RatingStateCode = lkp_RatingStateCode
	-- 								AND   i_ClassCode = lkp_ClassCode
	--                                                    AND   i_ClassCodeOriginatingOrganization = lkp_ClassCodeOriginatingOrganization  
	-- 			 					AND   (i_ClassDescription <>lkp_ClassDescription 
	-- 								  OR i_EffectiveDate <> lkp_EffectiveDate
	-- 								  OR i_ExpirationDate <> lkp_ExpirationDate
	-- 								  OR  i_CommercialAutoVehicleTypeSize <> lkp_CommercialAutoVehicleTypeSize  
	-- 								  OR  i_CommercialAutoBusinessUseClass <> lkp_CommercialAutoBusinessUseClass  
	-- 								  OR  i_SecondaryClass <> lkp_SecondaryClass   
	-- 								  OR  i_RadiusofOperation <> lkp_RadiusofOperation   
	-- 								  OR  i_FleetType <> lkp_FleetType   
	-- 								  OR  i_SecondaryClassGroup <> lkp_SecondaryClassGroup 
	-- 								  )
	-- 								),'INSERT',
	-- i_RatingStateCode<>lkp_RatingStateCode OR
	-- i_ClassCode<>lkp_ClassCode  OR
	-- i_ClassCodeOriginatingOrganization <> lkp_ClassCodeOriginatingOrganization  ,
	-- 'UPDATE',
	-- 'NOCHANGE'
	-- )
	DECODE(TRUE,
		i_ExpirationDate <= lkp_EffectiveDate 
		OR v_RecordPopulated = 'NOCHANGE', 'NOCHANGE',
		lkp_SupClassificationCommercialAutoID IS NULL 
		OR ( i_RatingStateCode = lkp_RatingStateCode 
			AND i_ClassCode = lkp_ClassCode 
			AND i_ClassCodeOriginatingOrganization = lkp_ClassCodeOriginatingOrganization 
			AND ( i_ClassDescription <> lkp_ClassDescription 
				OR i_EffectiveDate <> lkp_EffectiveDate 
				OR i_ExpirationDate <> lkp_ExpirationDate 
				OR i_CommercialAutoVehicleTypeSize <> lkp_CommercialAutoVehicleTypeSize 
				OR i_CommercialAutoBusinessUseClass <> lkp_CommercialAutoBusinessUseClass 
				OR i_SecondaryClass <> lkp_SecondaryClass 
				OR i_RadiusofOperation <> lkp_RadiusofOperation 
				OR i_FleetType <> lkp_FleetType 
				OR i_SecondaryClassGroup <> lkp_SecondaryClassGroup 
			) 
		), 'INSERT',
		i_RatingStateCode <> lkp_RatingStateCode 
		OR i_ClassCode <> lkp_ClassCode 
		OR i_ClassCodeOriginatingOrganization <> lkp_ClassCodeOriginatingOrganization, 'UPDATE',
		'NOCHANGE'
	) AS v_ChangeFlag,
	'Please correct the EffectiveDate in CSV file for ClassCode = '||i_ClassCode||' and RatingStateCode = '|| i_RatingStateCode ||', because EffectiveDate should reflect the real effective date for any change on this ClassCode.' AS v_ErrorMessage,
	-- *INF*: 'PASS'
	-- --DECODE(TRUE, 
	-- --i_RatingStateCode = lkp_RatingStateCode
	-- --AND i_ClassCode = lkp_ClassCode
	-- --AND i_EffectiveDate  = lkp_EffectiveDate
	-- --AND 
	-- --(i_ClassDescription <>lkp_ClassDescription
	-- --OR i_ClassCodeOriginatingOrganization <> lkp_ClassCodeOriginatingOrganization
	-- --OR i_CommercialAutoVehicleTypeSize <> lkp_CommercialAutoVehicleTypeSize
	-- --OR i_CommercialAutoBusinessUseClass <> lkp_CommercialAutoBusinessUseClass
	-- --OR i_SecondaryClass <> lkp_SecondaryClass
	-- --OR i_RadiusofOperation <> lkp_RadiusofOperation
	-- --OR i_FleetType <> lkp_FleetType
	-- --OR i_SecondaryClassGroup <> lkp_SecondaryClassGroup
	-- --), 
	-- --ERROR(v_ErrorMessage)
	-- --,'PASS
	'PASS' AS v_RaiseError,
	v_ChangeFlag AS o_ChangeFlag,
	1 AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	-- *INF*: @{pipeline().parameters.SOURCE_SYSTEM_ID}
	-- --'N/A'
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	sysdate AS o_CreatedDate,
	sysdate AS o_ModifiedDate,
	i_LineOfBusinessAbbreviation AS o_LineOfBusinessAbbreviation,
	i_RatingStateCode AS o_RatingStateCode,
	-- *INF*: i_EffectiveDate
	-- --IIF(v_ChangeFlag='INSERT',
	-- 	--TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	--TO_DATE(TO_CHAR(SYSDATE,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS'))
	i_EffectiveDate AS o_ClassEffectiveDate,
	-- *INF*: i_ExpirationDate
	-- --TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	i_ExpirationDate AS o_ClassExpirationDate,
	i_ClassCode AS o_ClassCode,
	i_ClassDescription AS o_ClassDescription,
	i_ClassCodeOriginatingOrganization AS o_ClassCodeOriginatingOrganization,
	i_CommercialAutoVehicleTypeSize AS o_CommercialAutoVehicleTypeSize,
	i_CommercialAutoBusinessUseClass AS o_CommercialAutoBusinessUseClass,
	i_SecondaryClass AS o_SecondaryClass,
	i_RadiusofOperation AS o_RadiusofOperation,
	i_FleetType AS o_FleetType,
	i_SecondaryClassGroup AS o_SecondaryClassGroup
	FROM SQ_SupClassificationCommercialAuto
	LEFT JOIN LKP_SupClassificationCommercialAuto
	ON LKP_SupClassificationCommercialAuto.RatingStateCode = SQ_SupClassificationCommercialAuto.RatingStateCode AND LKP_SupClassificationCommercialAuto.ClassCode = SQ_SupClassificationCommercialAuto.ClassCode AND LKP_SupClassificationCommercialAuto.OriginatingOrganizationCode = SQ_SupClassificationCommercialAuto.ClassCodeOriginatingOrganization
	LEFT JOIN LKP_SUPCLASSIFICATIONCOMMERCIALAUTO_CURRENTCHANGEFLAG LKP_SUPCLASSIFICATIONCOMMERCIALAUTO_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_ClassCodeOriginatingOrganization_i_CommercialAutoVehicleTypeSize_i_CommercialAutoBusinessUseClass_i_SecondaryClass_i_RadiusofOperation_i_FleetType_i_SecondaryClassGroup
	ON LKP_SUPCLASSIFICATIONCOMMERCIALAUTO_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_ClassCodeOriginatingOrganization_i_CommercialAutoVehicleTypeSize_i_CommercialAutoBusinessUseClass_i_SecondaryClass_i_RadiusofOperation_i_FleetType_i_SecondaryClassGroup.RatingStateCode = i_RatingStateCode
	AND LKP_SUPCLASSIFICATIONCOMMERCIALAUTO_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_ClassCodeOriginatingOrganization_i_CommercialAutoVehicleTypeSize_i_CommercialAutoBusinessUseClass_i_SecondaryClass_i_RadiusofOperation_i_FleetType_i_SecondaryClassGroup.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONCOMMERCIALAUTO_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_ClassCodeOriginatingOrganization_i_CommercialAutoVehicleTypeSize_i_CommercialAutoBusinessUseClass_i_SecondaryClass_i_RadiusofOperation_i_FleetType_i_SecondaryClassGroup.EffectiveDate = i_EffectiveDate
	AND LKP_SUPCLASSIFICATIONCOMMERCIALAUTO_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_ClassCodeOriginatingOrganization_i_CommercialAutoVehicleTypeSize_i_CommercialAutoBusinessUseClass_i_SecondaryClass_i_RadiusofOperation_i_FleetType_i_SecondaryClassGroup.ClassDescription = i_ClassDescription
	AND LKP_SUPCLASSIFICATIONCOMMERCIALAUTO_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_ClassCodeOriginatingOrganization_i_CommercialAutoVehicleTypeSize_i_CommercialAutoBusinessUseClass_i_SecondaryClass_i_RadiusofOperation_i_FleetType_i_SecondaryClassGroup.OriginatingOrganizationCode = i_ClassCodeOriginatingOrganization
	AND LKP_SUPCLASSIFICATIONCOMMERCIALAUTO_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_ClassCodeOriginatingOrganization_i_CommercialAutoVehicleTypeSize_i_CommercialAutoBusinessUseClass_i_SecondaryClass_i_RadiusofOperation_i_FleetType_i_SecondaryClassGroup.VehicleTypeSize = i_CommercialAutoVehicleTypeSize
	AND LKP_SUPCLASSIFICATIONCOMMERCIALAUTO_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_ClassCodeOriginatingOrganization_i_CommercialAutoVehicleTypeSize_i_CommercialAutoBusinessUseClass_i_SecondaryClass_i_RadiusofOperation_i_FleetType_i_SecondaryClassGroup.BusinessUseClass = i_CommercialAutoBusinessUseClass
	AND LKP_SUPCLASSIFICATIONCOMMERCIALAUTO_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_ClassCodeOriginatingOrganization_i_CommercialAutoVehicleTypeSize_i_CommercialAutoBusinessUseClass_i_SecondaryClass_i_RadiusofOperation_i_FleetType_i_SecondaryClassGroup.SecondaryClass = i_SecondaryClass
	AND LKP_SUPCLASSIFICATIONCOMMERCIALAUTO_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_ClassCodeOriginatingOrganization_i_CommercialAutoVehicleTypeSize_i_CommercialAutoBusinessUseClass_i_SecondaryClass_i_RadiusofOperation_i_FleetType_i_SecondaryClassGroup.RadiusofOperation = i_RadiusofOperation
	AND LKP_SUPCLASSIFICATIONCOMMERCIALAUTO_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_ClassCodeOriginatingOrganization_i_CommercialAutoVehicleTypeSize_i_CommercialAutoBusinessUseClass_i_SecondaryClass_i_RadiusofOperation_i_FleetType_i_SecondaryClassGroup.FleetType = i_FleetType
	AND LKP_SUPCLASSIFICATIONCOMMERCIALAUTO_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_ClassCodeOriginatingOrganization_i_CommercialAutoVehicleTypeSize_i_CommercialAutoBusinessUseClass_i_SecondaryClass_i_RadiusofOperation_i_FleetType_i_SecondaryClassGroup.SecondaryClassGroup = i_SecondaryClassGroup

),
RTR_Insert_Update AS (
	SELECT
	o_ChangeFlag AS ChangeFlag,
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	o_AuditId AS AuditId,
	o_SourceSystemID AS SourceSystemID,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_LineOfBusinessAbbreviation AS LineOfBusinessAbbreviation,
	o_RatingStateCode AS RatingStateCode,
	o_ClassEffectiveDate AS ClassEffectiveDate,
	o_ClassExpirationDate AS ClassExpirationDate,
	o_ClassCode AS ClassCode,
	o_ClassDescription AS ClassDescription,
	o_ClassCodeOriginatingOrganization AS ClassCodeOriginatingOrganization,
	o_CommercialAutoVehicleTypeSize AS CommercialAutoVehicleTypeSize,
	o_CommercialAutoBusinessUseClass AS CommercialAutoBusinessUseClass,
	o_SecondaryClass AS SecondaryClass,
	o_RadiusofOperation AS RadiusofOperation,
	o_FleetType AS FleetType,
	o_SecondaryClassGroup AS SecondaryClassGroup
	FROM EXP_Detect_Changes
),
RTR_Insert_Update_INSERT_OR_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE ChangeFlag='INSERT'
OR ChangeFlag='UPDATE'),
SupClassificationCommercialAuto AS (
	INSERT INTO SupClassificationCommercialAuto
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, LineOfBusinessAbbreviation, RatingStateCode, ClassCode, ClassDescription, OriginatingOrganizationCode, VehicleTypeSize, BusinessUseClass, SecondaryClass, RadiusofOperation, FleetType, SecondaryClassGroup)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	ClassEffectiveDate AS EFFECTIVEDATE, 
	ClassExpirationDate AS EXPIRATIONDATE, 
	SourceSystemID AS SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	LINEOFBUSINESSABBREVIATION, 
	RATINGSTATECODE, 
	CLASSCODE, 
	CLASSDESCRIPTION, 
	ClassCodeOriginatingOrganization AS ORIGINATINGORGANIZATIONCODE, 
	CommercialAutoVehicleTypeSize AS VEHICLETYPESIZE, 
	CommercialAutoBusinessUseClass AS BUSINESSUSECLASS, 
	SECONDARYCLASS, 
	RADIUSOFOPERATION, 
	FLEETTYPE, 
	SECONDARYCLASSGROUP
	FROM RTR_Insert_Update_INSERT_OR_UPDATE
),
SQ_SupClassificationCommercialAuto_CheckExpDate AS (
	SELECT SupClassificationCommercialAuto.SupClassificationCommercialAutoId
	     , SupClassificationCommercialAuto.LineOfBusinessAbbreviation
	     , SupClassificationCommercialAuto.RatingStateCode
		 , SupClassificationCommercialAuto.EffectiveDate
		 , SupClassificationCommercialAuto.ExpirationDate
		 , SupClassificationCommercialAuto.ClassCode 
		 , SupClassificationCommercialAuto.ClassDescription
		 , SupClassificationCommercialAuto.OriginatingOrganizationCode
		 , SupClassificationCommercialAuto.VehicleTypeSize	
		 , SupClassificationCommercialAuto.BusinessUseClass	
		 , SupClassificationCommercialAuto.SecondaryClass	
		 , SupClassificationCommercialAuto.RadiusofOperation	
		 , SupClassificationCommercialAuto.FleetType	
		 , SupClassificationCommercialAuto.SecondaryClassGroup	
	FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationCommercialAuto
	where  CurrentSnapshotFlag =1
	ORDER BY SupClassificationCommercialAuto.ClassCode  ,
	SupClassificationCommercialAuto.RatingStateCode, 
	SupClassificationCommercialAuto.EffectiveDate DESC,
	SupClassificationCommercialAuto.CreatedDate DESC
),
EXP_Lag_Eff_dates AS (
	SELECT
	SupClassificationCommercialAutoID,
	LineOfBusinessAbbreviation,
	RatingStateCode,
	EffectiveDate,
	ExpirationDate,
	ClassCode,
	ClassDescription,
	ClassCodeOriginatingOrganization,
	VehicleTypeSize AS CommercialAutoVehicleTypeSize,
	BusinessUseClass AS CommercialAutoBusinessUseClass,
	SecondaryClass,
	RadiusofOperation,
	FleetType,
	SecondaryClassGroup,
	-- *INF*: DECODE(TRUE,RatingStateCode = v_PREV_ROW_RatingStateCode
	-- 								AND   ClassCode = v_PREV_ROW_ClassCode
	--                                                    AND   ClassCodeOriginatingOrganization = v_PREV_ROW_ClassCodeOriginatingOrganization
	-- 			 					and (ClassDescription <>v_PREV_ROW_ClassDescription
	-- 								  --OR EffectiveDate <> v_PREV_ROW_EffectiveDate
	-- 								 -- OR ExpirationDate <> v_PREV_ROW_ExpirationDate
	--                                                      OR ADD_TO_DATE(ExpirationDate,'SS',+1) <>v_PREV_ROW_EffectiveDate   					
	-- 								  OR CommercialAutoVehicleTypeSize <> v_PREV_ROW_CommercialAutoVehicleTypeSize
	-- 								  OR CommercialAutoBusinessUseClass <> v_PREV_ROW_CommercialAutoBusinessUseClass
	-- 								  OR SecondaryClass <> v_PREV_ROW_SecondaryClass
	-- 								  OR RadiusofOperation <> v_PREV_ROW_RadiusofOperation
	-- 								  OR FleetType <> v_PREV_ROW_FleetType
	-- 								  OR SecondaryClassGroup <> v_PREV_ROW_SecondaryClassGroup
	-- 								  )
	-- 		,'0','1')
	DECODE(TRUE,
		RatingStateCode = v_PREV_ROW_RatingStateCode 
		AND ClassCode = v_PREV_ROW_ClassCode 
		AND ClassCodeOriginatingOrganization = v_PREV_ROW_ClassCodeOriginatingOrganization 
		AND ( ClassDescription <> v_PREV_ROW_ClassDescription 
			OR DATEADD(SECOND,+ 1,ExpirationDate) <> v_PREV_ROW_EffectiveDate 
			OR CommercialAutoVehicleTypeSize <> v_PREV_ROW_CommercialAutoVehicleTypeSize 
			OR CommercialAutoBusinessUseClass <> v_PREV_ROW_CommercialAutoBusinessUseClass 
			OR SecondaryClass <> v_PREV_ROW_SecondaryClass 
			OR RadiusofOperation <> v_PREV_ROW_RadiusofOperation 
			OR FleetType <> v_PREV_ROW_FleetType 
			OR SecondaryClassGroup <> v_PREV_ROW_SecondaryClassGroup 
		), '0',
		'1'
	) AS v_CurrentSnapshotFlag,
	-- *INF*: ADD_TO_DATE(   --v_PREV_ROW_EffectiveDate
	-- 
	-- 	IIF(v_PREV_ROW_EffectiveDate =  TO_DATE('1800-01-01 00:00:00' , 'YYYY-MM-DD HH24:MI:SS' ) , sysdate ,v_PREV_ROW_EffectiveDate )
	-- 
	-- ,'SS',-1)
	-- 
	-- 
	-- --ADD_TO_DATE(v_PREV_ROW_EffectiveDate,'SS',-1)
	DATEADD(SECOND,- 1,IFF(v_PREV_ROW_EffectiveDate = TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'
		),
		sysdate,
		v_PREV_ROW_EffectiveDate
	)) AS v_ClassExpirationDate,
	v_CurrentSnapshotFlag AS o_CurrentSnapshotFlag,
	v_ClassExpirationDate AS ClassExpirationDate,
	LineOfBusinessAbbreviation AS v_PREV_ROW_LineOfBusinessAbbreviation,
	RatingStateCode AS v_PREV_ROW_RatingStateCode,
	ClassCode AS v_PREV_ROW_ClassCode,
	EffectiveDate AS v_PREV_ROW_EffectiveDate,
	ExpirationDate AS v_PREV_ROW_ExpirationDate,
	ClassDescription AS v_PREV_ROW_ClassDescription,
	ClassCodeOriginatingOrganization AS v_PREV_ROW_ClassCodeOriginatingOrganization,
	CommercialAutoVehicleTypeSize AS v_PREV_ROW_CommercialAutoVehicleTypeSize,
	CommercialAutoBusinessUseClass AS v_PREV_ROW_CommercialAutoBusinessUseClass,
	SecondaryClass AS v_PREV_ROW_SecondaryClass,
	RadiusofOperation AS v_PREV_ROW_RadiusofOperation,
	FleetType AS v_PREV_ROW_FleetType,
	SecondaryClassGroup AS v_PREV_ROW_SecondaryClassGroup,
	sysdate AS ModifiedDate
	FROM SQ_SupClassificationCommercialAuto_CheckExpDate
),
FIL_FirstRowInAKGroup AS (
	SELECT
	SupClassificationCommercialAutoID, 
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag, 
	ClassExpirationDate AS ExpirationDate, 
	ModifiedDate
	FROM EXP_Lag_Eff_dates
	WHERE CurrentSnapshotFlag ='0'
),
UPD_SupISOGLClassGroup AS (
	SELECT
	SupClassificationCommercialAutoID, 
	CurrentSnapshotFlag, 
	ExpirationDate, 
	ModifiedDate
	FROM FIL_FirstRowInAKGroup
),
SupClassificationCommercialAuto_CheckExpDate AS (
	MERGE INTO SupClassificationCommercialAuto AS T
	USING UPD_SupISOGLClassGroup AS S
	ON T.SupClassificationCommercialAutoId = S.SupClassificationCommercialAutoID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),
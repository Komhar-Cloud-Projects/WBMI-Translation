WITH
LKP_SupClassificationCommercialProperty_CurrentChangeFlag AS (
	SELECT
	SupClassificationCommercialPropertyId,
	RatingStateCode,
	ClassCode,
	OriginatingOrganizationCode,
	ClassDescription,
	ISOCPRatingGroup,
	CommercialPropertySpecialClass,
	EffectiveDate
	FROM (
		SELECT 
			SupClassificationCommercialPropertyId,
			RatingStateCode,
			ClassCode,
			OriginatingOrganizationCode,
			ClassDescription,
			ISOCPRatingGroup,
			CommercialPropertySpecialClass,
			EffectiveDate
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationCommercialProperty
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingStateCode,ClassCode,OriginatingOrganizationCode,ClassDescription,ISOCPRatingGroup,CommercialPropertySpecialClass,EffectiveDate ORDER BY SupClassificationCommercialPropertyId) = 1
),
SQ_SupClassificationCommercialProperty_IR AS (
	SELECT
		SupClassificationCommercialPropertyId,
		AuditId,
		CreatedDate,
		ModifiedDate,
		LineOfBusinessAbbreviation AS LineofBusinessAbbreviation,
		RatingStateCode,
		ClassCode,
		ClassDescription,
		OriginatingOrganizationCode,
		ISOCPRatingGroup,
		CommercialPropertySpecialClass,
		EffectiveDate,
		ExpirationDate
	FROM SupClassificationCommercialProperty_IR
),
EXP_Necessary AS (
	SELECT
	LineofBusinessAbbreviation AS i_LineofBusinessAbbreviation,
	RatingStateCode AS i_RatingStateCode,
	ClassCode AS i_ClassCode,
	ClassDescription AS i_ClassDescription,
	OriginatingOrganizationCode AS i_OriginatingOrganizationCode,
	ISOCPRatingGroup AS i_ISOCPRatingGroup,
	CommercialPropertySpecialClass AS i_PropertySpecialClass,
	EffectiveDate,
	ExpirationDate,
	i_LineofBusinessAbbreviation AS o_LineofBusinessAbbreviation,
	i_RatingStateCode AS o_RatingStateCode,
	i_ClassCode AS o_ClassCode,
	i_ClassDescription AS o_ClassDescription,
	i_OriginatingOrganizationCode AS o_OriginatingOrganizationCode,
	i_ISOCPRatingGroup AS o_ISOCPRatingGroup,
	i_PropertySpecialClass AS o_PropertySpecialClass
	FROM SQ_SupClassificationCommercialProperty_IR
),
LKP_SupClassificationCommercialProperty AS (
	SELECT
	SupClassificationCommercialPropertyId,
	EffectiveDate,
	ExpirationDate,
	RatingStateCode,
	ClassCode,
	ClassDescription,
	OriginatingOrganizationCode,
	ISOCPRatingGroup,
	CommercialPropertySpecialClass
	FROM (
		SELECT 
			SupClassificationCommercialPropertyId,
			EffectiveDate,
			ExpirationDate,
			RatingStateCode,
			ClassCode,
			ClassDescription,
			OriginatingOrganizationCode,
			ISOCPRatingGroup,
			CommercialPropertySpecialClass
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationCommercialProperty
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingStateCode,ClassCode,OriginatingOrganizationCode ORDER BY SupClassificationCommercialPropertyId) = 1
),
EXP_UpdateOrInsert AS (
	SELECT
	LKP_SupClassificationCommercialProperty.SupClassificationCommercialPropertyId AS lkp_SupClassificationCommercialPropertyId,
	LKP_SupClassificationCommercialProperty.EffectiveDate AS lkp_EffectiveDate,
	LKP_SupClassificationCommercialProperty.ExpirationDate AS lkp_ExpirationDate,
	LKP_SupClassificationCommercialProperty.RatingStateCode AS lkp_RatingStateCode,
	LKP_SupClassificationCommercialProperty.ClassCode AS lkp_ClassCode,
	LKP_SupClassificationCommercialProperty.ClassDescription AS lkp_ClassDescription,
	LKP_SupClassificationCommercialProperty.OriginatingOrganizationCode AS lkp_ClassCodeOriginatingOrganization,
	LKP_SupClassificationCommercialProperty.ISOCPRatingGroup AS lkp_ISOCPRatingGroup,
	LKP_SupClassificationCommercialProperty.CommercialPropertySpecialClass AS lkp_PropertySpecialClass,
	EXP_Necessary.EffectiveDate AS i_EffectiveDate,
	EXP_Necessary.ExpirationDate AS i_ExpirationDate,
	EXP_Necessary.o_LineofBusinessAbbreviation AS i_LineofBusinessAbbreviation,
	EXP_Necessary.o_RatingStateCode AS i_RatingStateCode,
	EXP_Necessary.o_ClassCode AS i_ClassCode,
	EXP_Necessary.o_ClassDescription AS i_ClassDescription,
	EXP_Necessary.o_OriginatingOrganizationCode AS i_ClassCodeOriginatingOrganization,
	EXP_Necessary.o_ISOCPRatingGroup AS i_ISOCPRatingGroup,
	EXP_Necessary.o_PropertySpecialClass AS i_PropertySpecialClass,
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL(:LKP.LKP_SUPCLASSIFICATIONCOMMERCIALPROPERTY_CURRENTCHANGEFLAG(i_RatingStateCode,i_ClassCode,i_ClassDescription,lkp_ClassCodeOriginatingOrganization,i_ISOCPRatingGroup,i_PropertySpecialClass,i_EffectiveDate)),
	-- 'NOCHANGE',
	-- 'INSERT')						
	DECODE(
	    TRUE,
	    LKP_SUPCLASSIFICATIONCOMMERCIALPROPERTY_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_lkp_ClassCodeOriginatingOrganization_i_ISOCPRatingGroup_i_PropertySpecialClass_i_EffectiveDate.SupClassificationCommercialPropertyId IS NOT NULL, 'NOCHANGE',
	    'INSERT'
	) AS v_RecordPopulated,
	-- *INF*: DECODE(true,
	-- i_ExpirationDate   <=  lkp_EffectiveDate OR v_RecordPopulated = 'NOCHANGE', 'NOCHANGE',
	--  ISNULL(lkp_SupClassificationCommercialPropertyId) 
	-- OR (  i_RatingStateCode = lkp_RatingStateCode 	
	-- AND  i_ClassCode = lkp_ClassCode 
	-- AND i_ClassCodeOriginatingOrganization = lkp_ClassCodeOriginatingOrganization
	-- 			 				   AND  (i_ClassDescription <>lkp_ClassDescription 
	-- 								  OR i_EffectiveDate <> lkp_EffectiveDate
	--                                                      OR i_ExpirationDate <> lkp_ExpirationDate
	-- 								  OR i_ISOCPRatingGroup <> lkp_ISOCPRatingGroup
	--                                                       OR lkp_PropertySpecialClass <> i_PropertySpecialClass)
	-- 								),'INSERT',
	-- i_RatingStateCode <>lkp_RatingStateCode OR
	-- i_ClassCode <>lkp_ClassCode  OR
	-- i_ClassCodeOriginatingOrganization<>lkp_ClassCodeOriginatingOrganization,
	-- 'UPDATE',
	-- 'NOCHANGE'
	-- )
	DECODE(
	    true,
	    i_ExpirationDate <= lkp_EffectiveDate OR v_RecordPopulated = 'NOCHANGE', 'NOCHANGE',
	    lkp_SupClassificationCommercialPropertyId IS NULL OR (i_RatingStateCode = lkp_RatingStateCode AND i_ClassCode = lkp_ClassCode AND i_ClassCodeOriginatingOrganization = lkp_ClassCodeOriginatingOrganization AND (i_ClassDescription <> lkp_ClassDescription OR i_EffectiveDate <> lkp_EffectiveDate OR i_ExpirationDate <> lkp_ExpirationDate OR i_ISOCPRatingGroup <> lkp_ISOCPRatingGroup OR lkp_PropertySpecialClass <> i_PropertySpecialClass)), 'INSERT',
	    i_RatingStateCode <> lkp_RatingStateCode OR i_ClassCode <> lkp_ClassCode OR i_ClassCodeOriginatingOrganization <> lkp_ClassCodeOriginatingOrganization, 'UPDATE',
	    'NOCHANGE'
	) AS v_ChangeFlag,
	1 AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	-- *INF*: i_EffectiveDate
	-- 
	-- --IIF(v_ChangeFlag='INSERT',
	-- 	--TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	--TO_DATE(TO_CHAR(SYSDATE,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS'))
	i_EffectiveDate AS o_EffectiveDate,
	-- *INF*: i_ExpirationDate
	-- 
	-- --TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	i_ExpirationDate AS o_ExpirationDate,
	-- *INF*: @{pipeline().parameters.SOURCE_SYSTEM_ID}
	-- --'InsRef'
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	v_ChangeFlag AS o_ChangeFlag
	FROM EXP_Necessary
	LEFT JOIN LKP_SupClassificationCommercialProperty
	ON LKP_SupClassificationCommercialProperty.RatingStateCode = EXP_Necessary.o_RatingStateCode AND LKP_SupClassificationCommercialProperty.ClassCode = EXP_Necessary.o_ClassCode AND LKP_SupClassificationCommercialProperty.OriginatingOrganizationCode = EXP_Necessary.o_OriginatingOrganizationCode
	LEFT JOIN LKP_SUPCLASSIFICATIONCOMMERCIALPROPERTY_CURRENTCHANGEFLAG LKP_SUPCLASSIFICATIONCOMMERCIALPROPERTY_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_lkp_ClassCodeOriginatingOrganization_i_ISOCPRatingGroup_i_PropertySpecialClass_i_EffectiveDate
	ON LKP_SUPCLASSIFICATIONCOMMERCIALPROPERTY_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_lkp_ClassCodeOriginatingOrganization_i_ISOCPRatingGroup_i_PropertySpecialClass_i_EffectiveDate.RatingStateCode = i_RatingStateCode
	AND LKP_SUPCLASSIFICATIONCOMMERCIALPROPERTY_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_lkp_ClassCodeOriginatingOrganization_i_ISOCPRatingGroup_i_PropertySpecialClass_i_EffectiveDate.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONCOMMERCIALPROPERTY_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_lkp_ClassCodeOriginatingOrganization_i_ISOCPRatingGroup_i_PropertySpecialClass_i_EffectiveDate.OriginatingOrganizationCode = i_ClassDescription
	AND LKP_SUPCLASSIFICATIONCOMMERCIALPROPERTY_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_lkp_ClassCodeOriginatingOrganization_i_ISOCPRatingGroup_i_PropertySpecialClass_i_EffectiveDate.ClassDescription = lkp_ClassCodeOriginatingOrganization
	AND LKP_SUPCLASSIFICATIONCOMMERCIALPROPERTY_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_lkp_ClassCodeOriginatingOrganization_i_ISOCPRatingGroup_i_PropertySpecialClass_i_EffectiveDate.ISOCPRatingGroup = i_ISOCPRatingGroup
	AND LKP_SUPCLASSIFICATIONCOMMERCIALPROPERTY_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_lkp_ClassCodeOriginatingOrganization_i_ISOCPRatingGroup_i_PropertySpecialClass_i_EffectiveDate.CommercialPropertySpecialClass = i_PropertySpecialClass
	AND LKP_SUPCLASSIFICATIONCOMMERCIALPROPERTY_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_lkp_ClassCodeOriginatingOrganization_i_ISOCPRatingGroup_i_PropertySpecialClass_i_EffectiveDate.EffectiveDate = i_EffectiveDate

),
RTR_InsertOrUpdate AS (
	SELECT
	lkp_SupClassificationCommercialPropertyId,
	i_LineofBusinessAbbreviation AS LineofBusinessAbbreviation,
	i_RatingStateCode AS RatingStateCode,
	i_ClassCode AS ClassCode,
	i_ClassDescription AS ClassDescription,
	i_ClassCodeOriginatingOrganization AS ClassCodeOriginatingOrganization,
	i_ISOCPRatingGroup AS ISOCPRatingGroup,
	i_PropertySpecialClass AS PropertySpecialClass,
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	o_AuditId AS AuditId,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_SourceSystemId AS SourceSystemId,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_ChangeFlag AS ChangeFlag
	FROM EXP_UpdateOrInsert
),
RTR_InsertOrUpdate_INSERT_or_UPDATE AS (SELECT * FROM RTR_InsertOrUpdate WHERE ChangeFlag='INSERT' or ChangeFlag='UPDATE'),
SupClassificationCommercialProperty_IL AS (
	INSERT INTO SupClassificationCommercialProperty
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, LineOfBusinessAbbreviation, RatingStateCode, ClassCode, ClassDescription, OriginatingOrganizationCode, ISOCPRatingGroup, CommercialPropertySpecialClass)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	LineofBusinessAbbreviation AS LINEOFBUSINESSABBREVIATION, 
	RATINGSTATECODE, 
	CLASSCODE, 
	CLASSDESCRIPTION, 
	ClassCodeOriginatingOrganization AS ORIGINATINGORGANIZATIONCODE, 
	ISOCPRATINGGROUP, 
	PropertySpecialClass AS COMMERCIALPROPERTYSPECIALCLASS
	FROM RTR_InsertOrUpdate_INSERT_or_UPDATE
),
SQ_SupClassificationCommercialProperty_CheckExpDate AS (
	SELECT SupClassificationCommercialProperty.SupClassificationCommercialPropertyId
	     , SupClassificationCommercialProperty.CurrentSnapshotFlag
	
		 , SupClassificationCommercialProperty.EffectiveDate
		 , SupClassificationCommercialProperty.ExpirationDate
		 , SupClassificationCommercialProperty.LineOfBusinessAbbreviation
	     , SupClassificationCommercialProperty.RatingStateCode
		 , SupClassificationCommercialProperty.ClassCode 
		 , SupClassificationCommercialProperty.ClassDescription
		 , SupClassificationCommercialProperty.OriginatingOrganizationCode
		 , SupClassificationCommercialProperty.ISOCPRatingGroup
		 , SupClassificationCommercialProperty.CommercialPropertySpecialClass
	FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationCommercialProperty
	where CurrentSnapshotFlag = 1
	ORDER BY SupClassificationCommercialProperty.ClassCode  ,
	SupClassificationCommercialProperty.RatingStateCode, 
	SupClassificationCommercialProperty.EffectiveDate DESC,
	SupClassificationCommercialProperty.CreatedDate DESC
),
EXP_Lag_Eff_dates AS (
	SELECT
	SupClassificationCommercialPropertyId,
	EffectiveDate,
	ExpirationDate,
	LineOfBusinessAbbreviation,
	RatingStateCode,
	ClassCode,
	ClassDescription,
	OriginatingOrganizationCode,
	ISOCPRatingGroup,
	CommercialPropertySpecialClass,
	CurrentSnapshotFlag,
	-- *INF*: DECODE(TRUE,RatingStateCode = v_PREV_ROW_RatingStateCode
	-- 								AND  ClassCode = v_PREV_ROW_ClassCode
	--                                                    AND  OriginatingOrganizationCode = v_PREV_ROW_OriginatingOrganizationCode
	-- 			 					and (ClassDescription <>v_PREV_ROW_ClassDescription
	-- 								  --OR EffectiveDate <> v_PREV_ROW_EffectiveDate
	-- 								  OR  ADD_TO_DATE(ExpirationDate,'SS',+1) <>v_PREV_ROW_EffectiveDate  
	-- 								  OR ISOCPRatingGroup <> v_PREV_ROW_ISOCPRatingGroup
	-- 								  OR CommercialPropertySpecialClass <> v_PREV_ROW_CommercialPropertySpecialClass
	-- 								  )
	-- 		,'0','1')
	DECODE(
	    TRUE,
	    RatingStateCode = v_PREV_ROW_RatingStateCode AND ClassCode = v_PREV_ROW_ClassCode AND OriginatingOrganizationCode = v_PREV_ROW_OriginatingOrganizationCode and (ClassDescription <> v_PREV_ROW_ClassDescription OR DATEADD(SECOND,+ 1,ExpirationDate) <> v_PREV_ROW_EffectiveDate OR ISOCPRatingGroup <> v_PREV_ROW_ISOCPRatingGroup OR CommercialPropertySpecialClass <> v_PREV_ROW_CommercialPropertySpecialClass), '0',
	    '1'
	) AS v_CurrentSnapshotFlag,
	-- *INF*: ADD_TO_DATE(   --v_PREV_ROW_EffectiveDate
	-- 
	-- 	IIF(v_PREV_ROW_EffectiveDate =  TO_DATE('1800-01-01 00:00:00' , 'YYYY-MM-DD HH24:MI:SS' ) , sysdate ,v_PREV_ROW_EffectiveDate )
	-- 
	-- ,'SS',-1)
	DATEADD(SECOND,- 1,
	    IFF(
	        v_PREV_ROW_EffectiveDate = TO_TIMESTAMP('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),
	        CURRENT_TIMESTAMP,
	        v_PREV_ROW_EffectiveDate
	    )) AS v_ClassExpirationDate,
	v_CurrentSnapshotFlag AS o_CurrentSnapshotFlag,
	v_ClassExpirationDate AS o_ClassExpirationDate,
	EffectiveDate AS v_PREV_ROW_EffectiveDate,
	ExpirationDate AS v_PREV_ROW_ExpirationDate,
	LineOfBusinessAbbreviation AS v_PREV_ROW_LineOfBusinessAbbreviation,
	RatingStateCode AS v_PREV_ROW_RatingStateCode,
	ClassCode AS v_PREV_ROW_ClassCode,
	ClassDescription AS v_PREV_ROW_ClassDescription,
	OriginatingOrganizationCode AS v_PREV_ROW_OriginatingOrganizationCode,
	ISOCPRatingGroup AS v_PREV_ROW_ISOCPRatingGroup,
	CommercialPropertySpecialClass AS v_PREV_ROW_CommercialPropertySpecialClass,
	SYSDATE AS ModifiedDate
	FROM SQ_SupClassificationCommercialProperty_CheckExpDate
),
FIL_FirstRowInAKGroup AS (
	SELECT
	SupClassificationCommercialPropertyId, 
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag, 
	ModifiedDate, 
	o_ClassExpirationDate AS ExpirationDate
	FROM EXP_Lag_Eff_dates
	WHERE CurrentSnapshotFlag = '0'
),
UPD_SupClassificationCommercialProperty AS (
	SELECT
	SupClassificationCommercialPropertyId, 
	CurrentSnapshotFlag, 
	ModifiedDate, 
	ExpirationDate
	FROM FIL_FirstRowInAKGroup
),
SupClassificationCommercialProperty_CheckExpDate AS (
	MERGE INTO SupClassificationCommercialProperty AS T
	USING UPD_SupClassificationCommercialProperty AS S
	ON T.SupClassificationCommercialPropertyId = S.SupClassificationCommercialPropertyId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),
WITH
LKP_SupClassificationGarage_CurrentChangeFlag AS (
	SELECT
	SupClassificationGarageId,
	EffectiveDate,
	ClassCode,
	ClassDescription,
	RatingStateCode,
	OriginatingOrganizationCode
	FROM (
		SELECT 
			SupClassificationGarageId,
			EffectiveDate,
			ClassCode,
			ClassDescription,
			RatingStateCode,
			OriginatingOrganizationCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationGarage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EffectiveDate,ClassCode,ClassDescription,RatingStateCode,OriginatingOrganizationCode ORDER BY SupClassificationGarageId) = 1
),
SQ_SupClassificationGarage AS (
	SELECT
		SupClassificationGarageId,
		AuditId,
		CreatedDate,
		ModifiedDate,
		LineOfBusinessAbbreviation,
		RatingStateCode,
		EffectiveDate,
		ExpirationDate,
		ClassCode,
		ClassDescription,
		OriginatingOrganizationCode
	FROM SupClassificationGarage
),
LKP_SupClassificationGarage AS (
	SELECT
	SupClassificationGarageId,
	RatingStateCode,
	ClassCode,
	ClassDescription,
	OriginatingOrganizationCode,
	EffectiveDate,
	ExpirationDate
	FROM (
		SELECT 
			SupClassificationGarageId,
			RatingStateCode,
			ClassCode,
			ClassDescription,
			OriginatingOrganizationCode,
			EffectiveDate,
			ExpirationDate
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationGarage
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingStateCode,ClassCode,OriginatingOrganizationCode ORDER BY SupClassificationGarageId) = 1
),
EXP_Detect_Changes AS (
	SELECT
	SQ_SupClassificationGarage.LineOfBusinessAbbreviation AS i_LineOfBusinessAbbreviation,
	SQ_SupClassificationGarage.RatingStateCode AS i_RatingStateCode,
	SQ_SupClassificationGarage.ClassCode AS i_ClassCode,
	SQ_SupClassificationGarage.ClassDescription AS i_ClassDescription,
	SQ_SupClassificationGarage.OriginatingOrganizationCode AS i_ClassCodeOriginatingOrganization,
	SQ_SupClassificationGarage.EffectiveDate AS i_EffectiveDate,
	SQ_SupClassificationGarage.ExpirationDate AS i_ExpirationDate,
	LKP_SupClassificationGarage.SupClassificationGarageId AS lkp_SupClassificationGarageId,
	LKP_SupClassificationGarage.RatingStateCode AS lkp_RatingStateCode,
	LKP_SupClassificationGarage.ClassCode AS lkp_ClassCode,
	LKP_SupClassificationGarage.ClassDescription AS lkp_ClassDescription,
	LKP_SupClassificationGarage.OriginatingOrganizationCode AS lkp_ClassCodeOriginatingOrganization,
	LKP_SupClassificationGarage.EffectiveDate AS lkp_EffectiveDate,
	LKP_SupClassificationGarage.ExpirationDate AS lkp_ExpirationDate,
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL(:LKP.LKP_SUPCLASSIFICATIONGARAGE_CURRENTCHANGEFLAG(i_RatingStateCode,i_ClassCode,i_ClassDescription,i_ClassCodeOriginatingOrganization,i_EffectiveDate)),
	-- 'NOCHANGE',
	-- 'INSERT')						
	DECODE(TRUE,
		NOT LKP_SUPCLASSIFICATIONGARAGE_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_i_ClassCodeOriginatingOrganization_i_EffectiveDate.SupClassificationGarageId IS NULL, 'NOCHANGE',
		'INSERT') AS v_RecordPopulated,
	-- *INF*: DECODE(TRUE,
	-- i_ExpirationDate   <=  lkp_EffectiveDate OR v_RecordPopulated = 'NOCHANGE', 'NOCHANGE',
	-- ISNULL(lkp_SupClassificationGarageId) OR ( i_RatingStateCode = lkp_RatingStateCode
	-- 								 AND i_ClassCode = lkp_ClassCode
	--                                                     AND i_ClassCodeOriginatingOrganization = lkp_ClassCodeOriginatingOrganization  
	-- 			 					 AND  (i_ClassDescription <>lkp_ClassDescription 
	-- 								 OR i_EffectiveDate <> lkp_EffectiveDate 
	-- 								 OR i_ExpirationDate <> lkp_ExpirationDate
	-- 								     )
	-- 								),'INSERT',
	-- i_RatingStateCode<>lkp_RatingStateCode OR
	-- i_ClassCode<>lkp_ClassCode OR
	-- i_ClassCodeOriginatingOrganization <> lkp_ClassCodeOriginatingOrganization  ,
	-- 'UPDATE',
	-- 'NOCHANGE'
	-- )
	DECODE(TRUE,
		i_ExpirationDate <= lkp_EffectiveDate OR v_RecordPopulated = 'NOCHANGE', 'NOCHANGE',
		lkp_SupClassificationGarageId IS NULL OR ( i_RatingStateCode = lkp_RatingStateCode AND i_ClassCode = lkp_ClassCode AND i_ClassCodeOriginatingOrganization = lkp_ClassCodeOriginatingOrganization AND ( i_ClassDescription <> lkp_ClassDescription OR i_EffectiveDate <> lkp_EffectiveDate OR i_ExpirationDate <> lkp_ExpirationDate ) ), 'INSERT',
		i_RatingStateCode <> lkp_RatingStateCode OR i_ClassCode <> lkp_ClassCode OR i_ClassCodeOriginatingOrganization <> lkp_ClassCodeOriginatingOrganization, 'UPDATE',
		'NOCHANGE') AS v_ChangeFlag,
	'Please correct the EffectiveDate in CSV file for ClassCode = '||i_ClassCode||' and RatingStateCode = '|| i_RatingStateCode ||', because EffectiveDate should reflect the real effective date for any change on this ClassCode.' AS v_ErrorMessage,
	-- *INF*: DECODE(TRUE, 
	-- i_RatingStateCode = lkp_RatingStateCode
	-- AND i_ClassCode = lkp_ClassCode
	-- AND i_EffectiveDate  = lkp_EffectiveDate
	-- AND 
	-- (i_ClassDescription <>lkp_ClassDescription
	-- OR i_ClassCodeOriginatingOrganization <> lkp_ClassCodeOriginatingOrganization
	-- ), 
	-- ERROR(v_ErrorMessage)
	-- ,'PASS')
	DECODE(TRUE,
		i_RatingStateCode = lkp_RatingStateCode AND i_ClassCode = lkp_ClassCode AND i_EffectiveDate = lkp_EffectiveDate AND ( i_ClassDescription <> lkp_ClassDescription OR i_ClassCodeOriginatingOrganization <> lkp_ClassCodeOriginatingOrganization ), ERROR(v_ErrorMessage),
		'PASS') AS v_RaiseError,
	v_ChangeFlag AS o_ChangeFlag,
	1 AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
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
	i_ClassCodeOriginatingOrganization AS o_ClassCodeOriginatingOrganization
	FROM SQ_SupClassificationGarage
	LEFT JOIN LKP_SupClassificationGarage
	ON LKP_SupClassificationGarage.RatingStateCode = SQ_SupClassificationGarage.RatingStateCode AND LKP_SupClassificationGarage.ClassCode = SQ_SupClassificationGarage.ClassCode AND LKP_SupClassificationGarage.OriginatingOrganizationCode = SQ_SupClassificationGarage.OriginatingOrganizationCode
	LEFT JOIN LKP_SUPCLASSIFICATIONGARAGE_CURRENTCHANGEFLAG LKP_SUPCLASSIFICATIONGARAGE_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_i_ClassCodeOriginatingOrganization_i_EffectiveDate
	ON LKP_SUPCLASSIFICATIONGARAGE_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_i_ClassCodeOriginatingOrganization_i_EffectiveDate.EffectiveDate = i_RatingStateCode
	AND LKP_SUPCLASSIFICATIONGARAGE_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_i_ClassCodeOriginatingOrganization_i_EffectiveDate.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONGARAGE_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_i_ClassCodeOriginatingOrganization_i_EffectiveDate.ClassDescription = i_ClassDescription
	AND LKP_SUPCLASSIFICATIONGARAGE_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_i_ClassCodeOriginatingOrganization_i_EffectiveDate.RatingStateCode = i_ClassCodeOriginatingOrganization
	AND LKP_SUPCLASSIFICATIONGARAGE_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_i_ClassCodeOriginatingOrganization_i_EffectiveDate.OriginatingOrganizationCode = i_EffectiveDate

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
	o_ClassCodeOriginatingOrganization AS ClassCodeOriginatingOrganization
	FROM EXP_Detect_Changes
),
RTR_Insert_Update_INSERT_OR_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE ChangeFlag='INSERT'
OR ChangeFlag='UPDATE'),
SupClassificationGarage_Insert AS (
	INSERT INTO SupClassificationGarage
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, LineOfBusinessAbbreviation, RatingStateCode, ClassCode, ClassDescription, OriginatingOrganizationCode)
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
	ClassCodeOriginatingOrganization AS ORIGINATINGORGANIZATIONCODE
	FROM RTR_Insert_Update_INSERT_OR_UPDATE
),
SQ_SupClassificationGarage_CheckExpDate AS (
	SELECT SupClassificationGarage.SupClassificationGarageId
	     , SupClassificationGarage.CurrentSnapshotFlag
	
		 , SupClassificationGarage.EffectiveDate
		 , SupClassificationGarage.ExpirationDate
		 , SupClassificationGarage.LineOfBusinessAbbreviation
	     , SupClassificationGarage.RatingStateCode
		 , SupClassificationGarage.ClassCode 
		 , SupClassificationGarage.ClassDescription
		 , SupClassificationGarage.OriginatingOrganizationCode
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationGarage
	where CurrentSnapshotFlag = 1
	ORDER BY SupClassificationGarage.ClassCode  ,
	SupClassificationGarage.RatingStateCode, 
	SupClassificationGarage.EffectiveDate DESC,
	SupClassificationGarage.CreatedDate DESC
),
EXP_Lag_Eff_dates AS (
	SELECT
	SupClassificationGarageId,
	EffectiveDate,
	ExpirationDate,
	LineOfBusinessAbbreviation,
	RatingStateCode,
	ClassCode,
	ClassDescription,
	OriginatingOrganizationCode,
	CurrentSnapshotFlag AS i_CurrentSnapshotFlag,
	-- *INF*: DECODE(TRUE,	 RatingStateCode = v_PREV_ROW_RatingStateCode
	-- 				  AND ClassCode = v_PREV_ROW_ClassCode
	--                            AND  OriginatingOrganizationCode = v_PREV_ROW_OriginatingOrganizationCode
	-- 			 	  AND (ClassDescription <>v_PREV_ROW_ClassDescription
	-- 					--OR EffectiveDate <> v_PREV_ROW_EffectiveDate
	-- 					--OR ExpirationDate <> v_PREV_ROW_ExpirationDate
	-- 	                         OR  ADD_TO_DATE(ExpirationDate,'SS',+1) <>v_PREV_ROW_EffectiveDate   
	-- 								  )
	-- 		,'0','1')
	DECODE(TRUE,
		RatingStateCode = v_PREV_ROW_RatingStateCode AND ClassCode = v_PREV_ROW_ClassCode AND OriginatingOrganizationCode = v_PREV_ROW_OriginatingOrganizationCode AND ( ClassDescription <> v_PREV_ROW_ClassDescription OR ADD_TO_DATE(ExpirationDate, 'SS', + 1) <> v_PREV_ROW_EffectiveDate ), '0',
		'1') AS v_CurrentSnapshotFlag,
	-- *INF*: ADD_TO_DATE(   --v_PREV_ROW_EffectiveDate
	-- 
	-- 	IIF(v_PREV_ROW_EffectiveDate =  TO_DATE('1800-01-01 00:00:00' , 'YYYY-MM-DD HH24:MI:SS' ) , sysdate ,v_PREV_ROW_EffectiveDate )
	-- 
	-- ,'SS',-1)
	-- 
	-- --ADD_TO_DATE(v_PREV_ROW_EffectiveDate,'SS',-1)
	ADD_TO_DATE(IFF(v_PREV_ROW_EffectiveDate = TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), sysdate, v_PREV_ROW_EffectiveDate), 'SS', - 1) AS v_ClassExpirationDate,
	v_CurrentSnapshotFlag AS o_ClassExpirationDate,
	v_ClassExpirationDate AS ClassExpirationDate,
	EffectiveDate AS v_PREV_ROW_EffectiveDate,
	ExpirationDate AS v_PREV_ROW_ExpirationDate,
	LineOfBusinessAbbreviation AS v_PREV_ROW_LineOfBusinessAbbreviation,
	RatingStateCode AS v_PREV_ROW_RatingStateCode,
	ClassCode AS v_PREV_ROW_ClassCode,
	ClassDescription AS v_PREV_ROW_ClassDescription,
	OriginatingOrganizationCode AS v_PREV_ROW_OriginatingOrganizationCode,
	sysdate AS ModifiedDate
	FROM SQ_SupClassificationGarage_CheckExpDate
),
FIL_FirstRowInAKGroup AS (
	SELECT
	SupClassificationGarageId, 
	o_ClassExpirationDate AS CurrentSnapshotFlag, 
	ModifiedDate, 
	ClassExpirationDate
	FROM EXP_Lag_Eff_dates
	WHERE CurrentSnapshotFlag= '0'
),
UPD_SupISOGLClassGroup AS (
	SELECT
	SupClassificationGarageId, 
	CurrentSnapshotFlag, 
	ModifiedDate, 
	ClassExpirationDate
	FROM FIL_FirstRowInAKGroup
),
SupClassificationGarage_Update AS (
	MERGE INTO SupClassificationGarage AS T
	USING UPD_SupISOGLClassGroup AS S
	ON T.SupClassificationGarageId = S.SupClassificationGarageId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ClassExpirationDate, T.ModifiedDate = S.ModifiedDate
),
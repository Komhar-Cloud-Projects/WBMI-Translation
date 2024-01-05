WITH
LKP_SupClassificationCrime_CurrentChangeFlag AS (
	SELECT
	SupClassificationCrimeId,
	RatingStateCode,
	ClassCode,
	OriginatingOrganizationCode,
	EffectiveDate,
	ClassDescription,
	IndustryGroup
	FROM (
		SELECT 
			SupClassificationCrimeId,
			RatingStateCode,
			ClassCode,
			OriginatingOrganizationCode,
			EffectiveDate,
			ClassDescription,
			IndustryGroup
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationCrime
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingStateCode,ClassCode,OriginatingOrganizationCode,EffectiveDate,ClassDescription,IndustryGroup ORDER BY SupClassificationCrimeId) = 1
),
SQ_SupClassificationCrime AS (
	SELECT
		SupClassificationCrimeId,
		AuditId,
		CreatedDate,
		ModifiedDate,
		LineOfBusinessAbbreviation,
		RatingStateCode,
		EffectiveDate,
		ExpirationDate,
		ClassCode,
		ClassDescription,
		OriginatingOrganizationCode,
		IndustryGroup
	FROM SupClassificationCrime
	INNER JOIN SupClassificationCrime
),
LKP_SupClassificationCrime AS (
	SELECT
	SupClassificationCrimeId,
	RatingStateCode,
	ClassCode,
	OriginatingOrganizationCode,
	ClassDescription,
	IndustryGroup,
	EffectiveDate,
	ExpirationDate
	FROM (
		SELECT 
			SupClassificationCrimeId,
			RatingStateCode,
			ClassCode,
			OriginatingOrganizationCode,
			ClassDescription,
			IndustryGroup,
			EffectiveDate,
			ExpirationDate
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationCrime
		WHERE CurrentSnapshotFlag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingStateCode,ClassCode,OriginatingOrganizationCode ORDER BY SupClassificationCrimeId) = 1
),
EXP_Detect_Changes AS (
	SELECT
	SQ_SupClassificationCrime.LineOfBusinessAbbreviation AS i_LineOfBusinessAbbreviation,
	SQ_SupClassificationCrime.RatingStateCode AS i_RatingStateCode,
	SQ_SupClassificationCrime.ClassCode AS i_ClassCode,
	SQ_SupClassificationCrime.ClassDescription AS i_ClassDescription,
	SQ_SupClassificationCrime.OriginatingOrganizationCode AS i_OriginatingOrganizationCode,
	SQ_SupClassificationCrime.IndustryGroup AS i_CrimeIndustryGroup,
	SQ_SupClassificationCrime.EffectiveDate AS i_EffectiveDate,
	SQ_SupClassificationCrime.ExpirationDate AS i_ExpirationDate,
	LKP_SupClassificationCrime.SupClassificationCrimeId AS lkp_SupClassificationId,
	LKP_SupClassificationCrime.RatingStateCode AS lkp_RatingStateCode,
	LKP_SupClassificationCrime.ClassCode AS lkp_ClassCode,
	LKP_SupClassificationCrime.OriginatingOrganizationCode AS lkp_OriginatingOrganizationCode,
	LKP_SupClassificationCrime.ClassDescription AS lkp_ClassDescription,
	LKP_SupClassificationCrime.IndustryGroup AS lkp_CrimeIndustryGroup,
	LKP_SupClassificationCrime.EffectiveDate AS lkp_EffectiveDate,
	LKP_SupClassificationCrime.ExpirationDate AS lkp_ExpirationDate,
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL(:LKP.LKP_SupClassificationCrime_CurrentChangeFlag(i_RatingStateCode,i_ClassCode,i_OriginatingOrganizationCode,i_EffectiveDate,i_ClassDescription,i_CrimeIndustryGroup)),
	-- 'NOCHANGE',
	-- 'INSERT')						
	DECODE(TRUE,
	NOT LKP_SUPCLASSIFICATIONCRIME_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_OriginatingOrganizationCode_i_EffectiveDate_i_ClassDescription_i_CrimeIndustryGroup.SupClassificationCrimeId IS NULL, 'NOCHANGE',
	'INSERT') AS v_RecordPopulated,
	-- *INF*: DECODE(TRUE,
	-- i_ExpirationDate   <=  lkp_EffectiveDate OR v_RecordPopulated = 'NOCHANGE', 'NOCHANGE',
	--  ISNULL(lkp_SupClassificationId) 
	-- OR (  i_RatingStateCode = lkp_RatingStateCode 	
	-- AND  i_ClassCode = lkp_ClassCode 
	-- AND  i_OriginatingOrganizationCode = lkp_OriginatingOrganizationCode 
	-- 			 				   AND  (i_ClassDescription <>lkp_ClassDescription 
	-- 								  OR i_EffectiveDate <> lkp_EffectiveDate
	--                                                      OR i_ExpirationDate <> lkp_ExpirationDate
	-- 								  OR i_CrimeIndustryGroup <> lkp_CrimeIndustryGroup 
	-- 								  )
	-- 								),'INSERT',
	-- i_RatingStateCode <>lkp_RatingStateCode OR
	-- i_ClassCode <>lkp_ClassCode  OR
	-- i_OriginatingOrganizationCode <> lkp_OriginatingOrganizationCode,
	-- 'UPDATE',
	-- 'NOCHANGE'
	-- )
	DECODE(TRUE,
	i_ExpirationDate <= lkp_EffectiveDate OR v_RecordPopulated = 'NOCHANGE', 'NOCHANGE',
	lkp_SupClassificationId IS NULL OR ( i_RatingStateCode = lkp_RatingStateCode AND i_ClassCode = lkp_ClassCode AND i_OriginatingOrganizationCode = lkp_OriginatingOrganizationCode AND ( i_ClassDescription <> lkp_ClassDescription OR i_EffectiveDate <> lkp_EffectiveDate OR i_ExpirationDate <> lkp_ExpirationDate OR i_CrimeIndustryGroup <> lkp_CrimeIndustryGroup ) ), 'INSERT',
	i_RatingStateCode <> lkp_RatingStateCode OR i_ClassCode <> lkp_ClassCode OR i_OriginatingOrganizationCode <> lkp_OriginatingOrganizationCode, 'UPDATE',
	'NOCHANGE') AS v_ChangeFlag_Insert,
	'Please correct the EffectiveDate in CSV file for ClassCode = '||i_ClassCode||' and RatingStateCode = '|| i_RatingStateCode ||', because EffectiveDate should reflect the real effective date for any change on this ClassCode.' AS v_ErrorMessage,
	-- *INF*: 'PASS'
	-- --DECODE(TRUE, 
	-- --i_RatingStateCode = lkp_RatingStateCode
	-- --AND i_ClassCode = lkp_ClassCode
	-- --AND i_EffectiveDate  = lkp_EffectiveDate
	-- --AND 
	-- --(i_ClassDescription <>lkp_ClassDescription
	-- --OR i_OriginatingOrganizationCode <> lkp_OriginatingOrganizationCode
	-- --OR i_CrimeIndustryGroup <> lkp_CrimeIndustryGroup), 
	-- --ERROR(v_ErrorMessage)
	-- --,'PASS')
	'PASS' AS v_RaiseError,
	v_ChangeFlag_Insert AS o_ChangeFlag_Insert,
	1 AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	-- *INF*: i_EffectiveDate
	-- --IIF(v_ChangeFlag='INSERT',
	-- --	TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- --	TO_DATE(TO_CHAR(SYSDATE,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS'))
	i_EffectiveDate AS o_EffectiveDate,
	-- *INF*: i_ExpirationDate
	-- --TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	i_ExpirationDate AS o_ExpirationDate,
	-- *INF*: @{pipeline().parameters.SOURCE_SYSTEM_ID}
	-- --'N/A'
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	sysdate AS o_CreatedDate,
	sysdate AS o_ModifiedDate,
	i_LineOfBusinessAbbreviation AS o_LineOfBusinessAbbreviation,
	i_RatingStateCode AS o_RatingStateCode,
	i_ClassCode AS o_ClassCode,
	i_ClassDescription AS o_ClassDescription,
	i_OriginatingOrganizationCode AS o_OriginatingOrganizationCode,
	i_CrimeIndustryGroup AS o_CrimeIndustryGroup
	FROM SQ_SupClassificationCrime
	LEFT JOIN LKP_SupClassificationCrime
	ON LKP_SupClassificationCrime.RatingStateCode = SQ_SupClassificationCrime.RatingStateCode AND LKP_SupClassificationCrime.ClassCode = SQ_SupClassificationCrime.ClassCode AND LKP_SupClassificationCrime.OriginatingOrganizationCode = SQ_SupClassificationCrime.OriginatingOrganizationCode
	LEFT JOIN LKP_SUPCLASSIFICATIONCRIME_CURRENTCHANGEFLAG LKP_SUPCLASSIFICATIONCRIME_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_OriginatingOrganizationCode_i_EffectiveDate_i_ClassDescription_i_CrimeIndustryGroup
	ON LKP_SUPCLASSIFICATIONCRIME_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_OriginatingOrganizationCode_i_EffectiveDate_i_ClassDescription_i_CrimeIndustryGroup.RatingStateCode = i_RatingStateCode
	AND LKP_SUPCLASSIFICATIONCRIME_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_OriginatingOrganizationCode_i_EffectiveDate_i_ClassDescription_i_CrimeIndustryGroup.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONCRIME_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_OriginatingOrganizationCode_i_EffectiveDate_i_ClassDescription_i_CrimeIndustryGroup.OriginatingOrganizationCode = i_OriginatingOrganizationCode
	AND LKP_SUPCLASSIFICATIONCRIME_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_OriginatingOrganizationCode_i_EffectiveDate_i_ClassDescription_i_CrimeIndustryGroup.EffectiveDate = i_EffectiveDate
	AND LKP_SUPCLASSIFICATIONCRIME_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_OriginatingOrganizationCode_i_EffectiveDate_i_ClassDescription_i_CrimeIndustryGroup.ClassDescription = i_ClassDescription
	AND LKP_SUPCLASSIFICATIONCRIME_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_OriginatingOrganizationCode_i_EffectiveDate_i_ClassDescription_i_CrimeIndustryGroup.IndustryGroup = i_CrimeIndustryGroup

),
RTR_Insert_Update AS (
	SELECT
	lkp_SupClassificationId AS SupClassificationId,
	o_ChangeFlag_Insert AS ChangeFlag,
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	o_AuditId AS AuditId,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_SourceSystemId AS SourceSystemId,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_LineOfBusinessAbbreviation AS LineOfBusinessAbbreviation,
	o_RatingStateCode AS RatingStateCode,
	o_ClassCode AS ClassCode,
	o_ClassDescription AS ClassDescription,
	o_OriginatingOrganizationCode AS OriginatingOrganizationCode,
	o_CrimeIndustryGroup AS IndustryGroup
	FROM EXP_Detect_Changes
),
RTR_Insert_Update_INSERT_OR_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE ChangeFlag='INSERT' OR ChangeFlag = 'UPDATE'),
SupClassificationCrime AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationCrime
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, LineOfBusinessAbbreviation, RatingStateCode, ClassCode, ClassDescription, OriginatingOrganizationCode, IndustryGroup)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	LINEOFBUSINESSABBREVIATION, 
	RATINGSTATECODE, 
	CLASSCODE, 
	CLASSDESCRIPTION, 
	ORIGINATINGORGANIZATIONCODE, 
	INDUSTRYGROUP
	FROM RTR_Insert_Update_INSERT_OR_UPDATE
),
SQ_SupClassificationCrime_Target AS (
	SELECT SupClassificationCrime.SupClassificationCrimeId
	     , SupClassificationCrime.CurrentSnapshotFlag
	
		 , SupClassificationCrime.EffectiveDate
		 , SupClassificationCrime.ExpirationDate
		 , SupClassificationCrime.LineOfBusinessAbbreviation
	     , SupClassificationCrime.RatingStateCode
		 , SupClassificationCrime.ClassCode 
		 , SupClassificationCrime.ClassDescription
		 , SupClassificationCrime.OriginatingOrganizationCode
		 , SupClassificationCrime.IndustryGroup	
	FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationCrime
	where CurrentSnapshotFlag = 1
	ORDER BY SupClassificationCrime.ClassCode  ,
	SupClassificationCrime.RatingStateCode, 
	SupClassificationCrime.EffectiveDate DESC,
	SupClassificationCrime.CreatedDate DESC
),
EXP_Lag_Eff_dates AS (
	SELECT
	SupClassificationCrimeId AS SupClassificationId,
	CurrentSnapshotFlag,
	EffectiveDate,
	ExpirationDate,
	LineOfBusinessAbbreviation,
	RatingStateCode,
	ClassCode,
	ClassDescription,
	OriginatingOrganizationCode,
	IndustryGroup,
	-- *INF*: DECODE(TRUE,RatingStateCode = v_PREV_ROW_RatingStateCode
	-- 		 AND ClassCode = v_PREV_ROW_ClassCode
	--              AND OriginatingOrganizationCode = v_PREV_ROW_OriginatingOrganizationCode
	-- 		AND (	ClassDescription <> v_PREV_ROW_ClassDescription
	-- 			--OR 	EffectiveDate <>  v_PREV_ROW_EffectiveDate   
	-- 			OR  ADD_TO_DATE(ExpirationDate,'SS',+1) <>v_PREV_ROW_EffectiveDate   
	-- 	             OR IndustryGroup <>  v_PREV_ROW_IndustryGroup
	-- 	             )
	-- 		,'0','1')
	DECODE(TRUE,
	RatingStateCode = v_PREV_ROW_RatingStateCode AND ClassCode = v_PREV_ROW_ClassCode AND OriginatingOrganizationCode = v_PREV_ROW_OriginatingOrganizationCode AND ( ClassDescription <> v_PREV_ROW_ClassDescription OR ADD_TO_DATE(ExpirationDate, 'SS', + 1) <> v_PREV_ROW_EffectiveDate OR IndustryGroup <> v_PREV_ROW_IndustryGroup ), '0',
	'1') AS v_CurrentSnapshotFlag,
	-- *INF*: ADD_TO_DATE(   --v_PREV_ROW_EffectiveDate
	-- 
	-- 	IIF(v_PREV_ROW_EffectiveDate =  TO_DATE('1800-01-01 00:00:00' , 'YYYY-MM-DD HH24:MI:SS' ) , sysdate ,v_PREV_ROW_EffectiveDate )
	-- 
	-- ,'SS',-1)
	ADD_TO_DATE(IFF(v_PREV_ROW_EffectiveDate = TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), sysdate, v_PREV_ROW_EffectiveDate), 'SS', - 1) AS v_ClassExpirationDate,
	v_CurrentSnapshotFlag AS o_CurrentSnapshotFlag,
	v_ClassExpirationDate AS ClassExpirationDate,
	EffectiveDate AS v_PREV_ROW_EffectiveDate,
	ExpirationDate AS v_PREV_ROW_ExpirationDate,
	LineOfBusinessAbbreviation AS v_PREV_ROW_LineOfBusinessAbbreviation,
	RatingStateCode AS v_PREV_ROW_RatingStateCode,
	ClassCode AS v_PREV_ROW_ClassCode,
	ClassDescription AS v_PREV_ROW_ClassDescription,
	OriginatingOrganizationCode AS v_PREV_ROW_OriginatingOrganizationCode,
	IndustryGroup AS v_PREV_ROW_IndustryGroup,
	sysdate AS ModifiedDate
	FROM SQ_SupClassificationCrime_Target
),
FIL_FirstRowInAKGroup AS (
	SELECT
	SupClassificationId, 
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag, 
	ModifiedDate, 
	ClassExpirationDate, 
	EffectiveDate, 
	ExpirationDate, 
	LineOfBusinessAbbreviation, 
	RatingStateCode, 
	ClassCode, 
	ClassDescription, 
	OriginatingOrganizationCode, 
	IndustryGroup
	FROM EXP_Lag_Eff_dates
	WHERE CurrentSnapshotFlag ='0'
),
UPD_SupClassificationCrime AS (
	SELECT
	SupClassificationId, 
	CurrentSnapshotFlag, 
	ModifiedDate, 
	ClassExpirationDate, 
	EffectiveDate, 
	ExpirationDate, 
	LineOfBusinessAbbreviation, 
	RatingStateCode, 
	ClassCode, 
	ClassDescription, 
	OriginatingOrganizationCode, 
	IndustryGroup
	FROM FIL_FirstRowInAKGroup
),
SupClassificationCrime_CheckExpDate AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationCrime AS T
	USING UPD_SupClassificationCrime AS S
	ON T.SupClassificationCrimeId = S.SupClassificationId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ClassExpirationDate, T.ModifiedDate = S.ModifiedDate
),
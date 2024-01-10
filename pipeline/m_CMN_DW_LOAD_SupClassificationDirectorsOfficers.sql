WITH
LKP_SupClassificationDirectorsOfficers_CurrentChangeFlag AS (
	SELECT
	SupClassificationDirectorsOfficersId,
	RatingStateCode,
	ClassCode,
	EffectiveDate,
	ClassDescription,
	OriginatingOrganizationCode
	FROM (
		SELECT 
			SupClassificationDirectorsOfficersId,
			RatingStateCode,
			ClassCode,
			EffectiveDate,
			ClassDescription,
			OriginatingOrganizationCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationDirectorsOfficers
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingStateCode,ClassCode,EffectiveDate,ClassDescription,OriginatingOrganizationCode ORDER BY SupClassificationDirectorsOfficersId) = 1
),
SQ_SupClassificationDirectorsOfficers AS (
	SELECT
		SupClassificationDirectorsOfficersId,
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
	FROM SupClassificationDirectorsOfficers
),
LKP_SupClassificationDirectorsOfficers AS (
	SELECT
	SupClassificationDirectorsOfficersId,
	EffectiveDate,
	ExpirationDate,
	RatingStateCode,
	ClassCode,
	ClassDescription,
	OriginatingOrganizationCode,
	Note
	FROM (
		SELECT 
			SupClassificationDirectorsOfficersId,
			EffectiveDate,
			ExpirationDate,
			RatingStateCode,
			ClassCode,
			ClassDescription,
			OriginatingOrganizationCode,
			Note
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationDirectorsOfficers
		WHERE CurrentSnapshotFlag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingStateCode,OriginatingOrganizationCode,ClassCode ORDER BY SupClassificationDirectorsOfficersId) = 1
),
EXP_Detect_Changes AS (
	SELECT
	SQ_SupClassificationDirectorsOfficers.EffectiveDate AS i_EffectiveDate,
	SQ_SupClassificationDirectorsOfficers.ExpirationDate AS i_ExpirationDate,
	SQ_SupClassificationDirectorsOfficers.LineOfBusinessAbbreviation AS i_LineOfBusinessAbbreviation,
	SQ_SupClassificationDirectorsOfficers.RatingStateCode AS i_RatingStateCode,
	SQ_SupClassificationDirectorsOfficers.ClassCode AS i_ClassCode,
	SQ_SupClassificationDirectorsOfficers.ClassDescription AS i_ClassDescription,
	SQ_SupClassificationDirectorsOfficers.OriginatingOrganizationCode AS i_ClassCodeOriginatingOrganization,
	LKP_SupClassificationDirectorsOfficers.SupClassificationDirectorsOfficersId AS lkp_SupClassificationDNOId,
	LKP_SupClassificationDirectorsOfficers.EffectiveDate AS lkp_EffectiveDate,
	LKP_SupClassificationDirectorsOfficers.ExpirationDate AS lkp_ExpirationDate,
	LKP_SupClassificationDirectorsOfficers.RatingStateCode AS lkp_RatingStateCode,
	LKP_SupClassificationDirectorsOfficers.ClassCode AS lkp_ClassCode,
	LKP_SupClassificationDirectorsOfficers.ClassDescription AS lkp_ClassDescription,
	LKP_SupClassificationDirectorsOfficers.OriginatingOrganizationCode AS lkp_ClassCodeOriginatingOrganization,
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL(:LKP.LKP_SUPCLASSIFICATIONDIRECTORSOFFICERS_CURRENTCHANGEFLAG(i_RatingStateCode,i_ClassCode,i_EffectiveDate,i_ClassDescription,i_ClassCodeOriginatingOrganization)),
	-- 'NOCHANGE',
	-- 'INSERT')						
	DECODE(TRUE,
		NOT LKP_SUPCLASSIFICATIONDIRECTORSOFFICERS_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_ClassCodeOriginatingOrganization.SupClassificationDirectorsOfficersId IS NULL, 'NOCHANGE',
		'INSERT') AS v_RecordPopulated,
	-- *INF*: DECODE(TRUE,
	-- i_ExpirationDate   <=  lkp_EffectiveDate OR v_RecordPopulated = 'NOCHANGE', 'NOCHANGE',
	-- ISNULL(lkp_SupClassificationDNOId) OR (i_RatingStateCode = lkp_RatingStateCode
	-- 								AND i_ClassCode = lkp_ClassCode
	--                                                    AND i_ClassCodeOriginatingOrganization = lkp_ClassCodeOriginatingOrganization   
	-- 			 					AND (i_ClassDescription <>lkp_ClassDescription    
	-- 								  OR i_ExpirationDate <> lkp_ExpirationDate
	--                                                      OR  i_EffectiveDate <> lkp_EffectiveDate
	-- 								   )
	-- 								),'INSERT',
	-- i_RatingStateCode<>lkp_RatingStateCode OR
	-- i_ClassCode<>lkp_ClassCode OR 
	--  i_ClassCodeOriginatingOrganization <> lkp_ClassCodeOriginatingOrganization ,
	-- 'UPDATE',
	-- 'NOCHANGE'
	-- )
	DECODE(TRUE,
		i_ExpirationDate <= lkp_EffectiveDate OR v_RecordPopulated = 'NOCHANGE', 'NOCHANGE',
		lkp_SupClassificationDNOId IS NULL OR ( i_RatingStateCode = lkp_RatingStateCode AND i_ClassCode = lkp_ClassCode AND i_ClassCodeOriginatingOrganization = lkp_ClassCodeOriginatingOrganization AND ( i_ClassDescription <> lkp_ClassDescription OR i_ExpirationDate <> lkp_ExpirationDate OR i_EffectiveDate <> lkp_EffectiveDate ) ), 'INSERT',
		i_RatingStateCode <> lkp_RatingStateCode OR i_ClassCode <> lkp_ClassCode OR i_ClassCodeOriginatingOrganization <> lkp_ClassCodeOriginatingOrganization, 'UPDATE',
		'NOCHANGE') AS v_ChangeFlag,
	v_ChangeFlag AS o_ChangeFlag,
	1 AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	sysdate AS o_CreatedDate,
	sysdate AS o_ModifiedDate,
	i_LineOfBusinessAbbreviation AS o_LineOfBusinessAbbreviation,
	i_RatingStateCode AS o_RatingStateCode,
	-- *INF*: i_EffectiveDate
	-- 
	-- --IIF(v_ChangeFlag='INSERT',
	-- 	--TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	--TO_DATE(TO_CHAR(SYSDATE,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS'))
	i_EffectiveDate AS o_ClassEffectiveDate,
	-- *INF*: i_ExpirationDate
	-- 
	-- --TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	i_ExpirationDate AS o_ClassExpirationDate,
	i_ClassCode AS o_ClassCode,
	i_ClassDescription AS o_ClassDescription,
	i_ClassCodeOriginatingOrganization AS o_ClassCodeOriginatingOrganization
	FROM SQ_SupClassificationDirectorsOfficers
	LEFT JOIN LKP_SupClassificationDirectorsOfficers
	ON LKP_SupClassificationDirectorsOfficers.RatingStateCode = SQ_SupClassificationDirectorsOfficers.RatingStateCode AND LKP_SupClassificationDirectorsOfficers.OriginatingOrganizationCode = SQ_SupClassificationDirectorsOfficers.OriginatingOrganizationCode AND LKP_SupClassificationDirectorsOfficers.ClassCode = SQ_SupClassificationDirectorsOfficers.ClassCode
	LEFT JOIN LKP_SUPCLASSIFICATIONDIRECTORSOFFICERS_CURRENTCHANGEFLAG LKP_SUPCLASSIFICATIONDIRECTORSOFFICERS_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_ClassCodeOriginatingOrganization
	ON LKP_SUPCLASSIFICATIONDIRECTORSOFFICERS_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_ClassCodeOriginatingOrganization.RatingStateCode = i_RatingStateCode
	AND LKP_SUPCLASSIFICATIONDIRECTORSOFFICERS_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_ClassCodeOriginatingOrganization.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONDIRECTORSOFFICERS_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_ClassCodeOriginatingOrganization.EffectiveDate = i_EffectiveDate
	AND LKP_SUPCLASSIFICATIONDIRECTORSOFFICERS_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_ClassCodeOriginatingOrganization.ClassDescription = i_ClassDescription
	AND LKP_SUPCLASSIFICATIONDIRECTORSOFFICERS_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_ClassCodeOriginatingOrganization.OriginatingOrganizationCode = i_ClassCodeOriginatingOrganization

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
SupClassificationDirectorsOfficers_IL AS (
	INSERT INTO SupClassificationDirectorsOfficers
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
SQ_SupClassificationDirectorsOfficers_CheckExpDate AS (
	SELECT SupClassificationDirectorsOfficers.SupClassificationDirectorsOfficersId, SupClassificationDirectorsOfficers.CurrentSnapshotFlag, SupClassificationDirectorsOfficers.EffectiveDate, SupClassificationDirectorsOfficers.ExpirationDate, SupClassificationDirectorsOfficers.LineOfBusinessAbbreviation, SupClassificationDirectorsOfficers.RatingStateCode, SupClassificationDirectorsOfficers.ClassCode, SupClassificationDirectorsOfficers.ClassDescription, SupClassificationDirectorsOfficers.OriginatingOrganizationCode 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationDirectorsOfficers
	where CurrentSnapshotFlag = 1
	ORDER BY SupClassificationDirectorsOfficers.ClassCode  ,
	SupClassificationDirectorsOfficers.RatingStateCode, 
	SupClassificationDirectorsOfficers.EffectiveDate DESC,
	SupClassificationDirectorsOfficers.CreatedDate DESC
),
EXP_Lag_Eff_dates AS (
	SELECT
	SupClassificationDirectorsOfficersId,
	EffectiveDate,
	ExpirationDate,
	LineOfBusinessAbbreviation,
	RatingStateCode,
	ClassCode,
	ClassDescription,
	OriginatingOrganizationCode,
	CurrentSnapshotFlag AS i_CurrentSnapshotFlag,
	-- *INF*: DECODE(TRUE,RatingStateCode = v_PREV_ROW_RatingStateCode
	-- 								AND ClassCode = v_PREV_ROW_ClassCode
	--                                                    AND OriginatingOrganizationCode = v_PREV_ROW_OriginatingOrganizationCode
	-- 			 					AND (ClassDescription <>v_PREV_ROW_ClassDescription
	-- 								 -- OR EffectiveDate <> v_PREV_ROW_EffectiveDate
	-- 								  OR   ADD_TO_DATE(ExpirationDate,'SS',+1) <>v_PREV_ROW_EffectiveDate   
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
	v_CurrentSnapshotFlag AS o_CurrentSnapshotFlag,
	v_ClassExpirationDate AS ClassExpirationDate,
	EffectiveDate AS v_PREV_ROW_EffectiveDate,
	ExpirationDate AS v_PREV_ROW_ExpirationDate,
	LineOfBusinessAbbreviation AS v_PREV_ROW_LineOfBusinessAbbreviation,
	RatingStateCode AS v_PREV_ROW_RatingStateCode,
	ClassCode AS v_PREV_ROW_ClassCode,
	ClassDescription AS v_PREV_ROW_ClassDescription,
	OriginatingOrganizationCode AS v_PREV_ROW_OriginatingOrganizationCode,
	sysdate AS ModifiedDate
	FROM SQ_SupClassificationDirectorsOfficers_CheckExpDate
),
FIL_FirstRowInAKGroup AS (
	SELECT
	SupClassificationDirectorsOfficersId, 
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag, 
	ModifiedDate, 
	ClassExpirationDate
	FROM EXP_Lag_Eff_dates
	WHERE CurrentSnapshotFlag = '0'
),
UPD_SupISOGLClassGroup AS (
	SELECT
	SupClassificationDirectorsOfficersId, 
	CurrentSnapshotFlag, 
	ModifiedDate, 
	ClassExpirationDate
	FROM FIL_FirstRowInAKGroup
),
SupClassificationDirectorsOfficers_CheckExpDate AS (
	MERGE INTO SupClassificationDirectorsOfficers AS T
	USING UPD_SupISOGLClassGroup AS S
	ON T.SupClassificationDirectorsOfficersId = S.SupClassificationDirectorsOfficersId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ClassExpirationDate, T.ModifiedDate = S.ModifiedDate
),
WITH
LKP_SupClassificationErrorsOmmisions_CurrentChangeFlag AS (
	SELECT
	SupClassificationErrorsOmissionsId,
	RatingStateCode,
	ClassCode,
	EffectiveDate,
	OriginatingOrganizationCode,
	ClassDescription
	FROM (
		SELECT 
			SupClassificationErrorsOmissionsId,
			RatingStateCode,
			ClassCode,
			EffectiveDate,
			OriginatingOrganizationCode,
			ClassDescription
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationErrorsOmissions
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingStateCode,ClassCode,EffectiveDate,OriginatingOrganizationCode,ClassDescription ORDER BY SupClassificationErrorsOmissionsId) = 1
),
SQ_SupClassificationErrorsOmissions AS (
	SELECT
		SupClassificationErrorsOmissionsId,
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
	FROM SupClassificationErrorsOmissions
),
LKP_SupClassificationErrorsOmmisions AS (
	SELECT
	SupClassificationErrorsOmissionsId,
	EffectiveDate,
	ExpirationDate,
	RatingStateCode,
	ClassCode,
	ClassDescription,
	OriginatingOrganizationCode
	FROM (
		SELECT 
			SupClassificationErrorsOmissionsId,
			EffectiveDate,
			ExpirationDate,
			RatingStateCode,
			ClassCode,
			ClassDescription,
			OriginatingOrganizationCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationErrorsOmissions
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingStateCode,ClassCode,OriginatingOrganizationCode ORDER BY SupClassificationErrorsOmissionsId) = 1
),
EXP_Detect_Changes AS (
	SELECT
	SQ_SupClassificationErrorsOmissions.EffectiveDate AS i_EffectiveDate,
	SQ_SupClassificationErrorsOmissions.ExpirationDate AS i_ExpirationDate,
	SQ_SupClassificationErrorsOmissions.LineOfBusinessAbbreviation AS i_LineOfBusinessAbbreviation,
	SQ_SupClassificationErrorsOmissions.RatingStateCode AS i_RatingStateCode,
	SQ_SupClassificationErrorsOmissions.ClassCode AS i_ClassCode,
	SQ_SupClassificationErrorsOmissions.ClassDescription AS i_ClassDescription,
	SQ_SupClassificationErrorsOmissions.OriginatingOrganizationCode AS i_ClassCodeOriginatingOrganization,
	LKP_SupClassificationErrorsOmmisions.SupClassificationErrorsOmissionsId AS lkp_SupClassificationErrorsOmissionsId,
	LKP_SupClassificationErrorsOmmisions.EffectiveDate AS lkp_EffectiveDate,
	LKP_SupClassificationErrorsOmmisions.ExpirationDate AS lkp_ExpirationDate,
	LKP_SupClassificationErrorsOmmisions.RatingStateCode AS lkp_RatingStateCode,
	LKP_SupClassificationErrorsOmmisions.ClassCode AS lkp_ClassCode,
	LKP_SupClassificationErrorsOmmisions.ClassDescription AS lkp_ClassDescription,
	LKP_SupClassificationErrorsOmmisions.OriginatingOrganizationCode AS lkp_ClassCodeOriginatingOrganization,
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL(:LKP.LKP_SUPCLASSIFICATIONERRORSOMMISIONS_CURRENTCHANGEFLAG(i_RatingStateCode,i_ClassCode,i_ClassCodeOriginatingOrganization,i_EffectiveDate,i_ClassDescription)),
	-- 'NOCHANGE',
	-- 'INSERT')						
	DECODE(TRUE,
	NOT LKP_SUPCLASSIFICATIONERRORSOMMISIONS_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassCodeOriginatingOrganization_i_EffectiveDate_i_ClassDescription.SupClassificationErrorsOmissionsId IS NULL, 'NOCHANGE',
	'INSERT') AS v_RecordPopulated,
	-- *INF*: DECODE(TRUE,
	-- i_ExpirationDate   <=  lkp_EffectiveDate OR v_RecordPopulated = 'NOCHANGE', 'NOCHANGE',
	-- ISNULL(lkp_SupClassificationErrorsOmissionsId) OR ( i_RatingStateCode = lkp_RatingStateCode
	-- 								AND i_ClassCode = lkp_ClassCode
	--                                                    AND i_ClassCodeOriginatingOrganization = lkp_ClassCodeOriginatingOrganization 
	-- 			 					AND (i_ClassDescription <>lkp_ClassDescription  
	-- 								  OR i_EffectiveDate <> lkp_EffectiveDate
	--                                                      OR i_ExpirationDate <> lkp_ExpirationDate								  
	-- 								    )
	-- 								),'INSERT',
	-- i_RatingStateCode<>lkp_RatingStateCode OR
	-- i_ClassCode<>lkp_ClassCode OR 
	--  i_ClassCodeOriginatingOrganization <> lkp_ClassCodeOriginatingOrganization  ,
	-- 'UPDATE',
	-- 'NOCHANGE'
	-- )
	DECODE(TRUE,
	i_ExpirationDate <= lkp_EffectiveDate OR v_RecordPopulated = 'NOCHANGE', 'NOCHANGE',
	lkp_SupClassificationErrorsOmissionsId IS NULL OR ( i_RatingStateCode = lkp_RatingStateCode AND i_ClassCode = lkp_ClassCode AND i_ClassCodeOriginatingOrganization = lkp_ClassCodeOriginatingOrganization AND ( i_ClassDescription <> lkp_ClassDescription OR i_EffectiveDate <> lkp_EffectiveDate OR i_ExpirationDate <> lkp_ExpirationDate ) ), 'INSERT',
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
	FROM SQ_SupClassificationErrorsOmissions
	LEFT JOIN LKP_SupClassificationErrorsOmmisions
	ON LKP_SupClassificationErrorsOmmisions.RatingStateCode = SQ_SupClassificationErrorsOmissions.RatingStateCode AND LKP_SupClassificationErrorsOmmisions.ClassCode = SQ_SupClassificationErrorsOmissions.ClassCode AND LKP_SupClassificationErrorsOmmisions.OriginatingOrganizationCode = SQ_SupClassificationErrorsOmissions.OriginatingOrganizationCode
	LEFT JOIN LKP_SUPCLASSIFICATIONERRORSOMMISIONS_CURRENTCHANGEFLAG LKP_SUPCLASSIFICATIONERRORSOMMISIONS_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassCodeOriginatingOrganization_i_EffectiveDate_i_ClassDescription
	ON LKP_SUPCLASSIFICATIONERRORSOMMISIONS_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassCodeOriginatingOrganization_i_EffectiveDate_i_ClassDescription.RatingStateCode = i_RatingStateCode
	AND LKP_SUPCLASSIFICATIONERRORSOMMISIONS_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassCodeOriginatingOrganization_i_EffectiveDate_i_ClassDescription.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONERRORSOMMISIONS_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassCodeOriginatingOrganization_i_EffectiveDate_i_ClassDescription.EffectiveDate = i_ClassCodeOriginatingOrganization
	AND LKP_SUPCLASSIFICATIONERRORSOMMISIONS_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassCodeOriginatingOrganization_i_EffectiveDate_i_ClassDescription.OriginatingOrganizationCode = i_EffectiveDate
	AND LKP_SUPCLASSIFICATIONERRORSOMMISIONS_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassCodeOriginatingOrganization_i_EffectiveDate_i_ClassDescription.ClassDescription = i_ClassDescription

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
SupClassificationErrorsOmissions_IL AS (
	INSERT INTO SupClassificationErrorsOmissions
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
SQ_SupClassificationErrorsOmissions_CheckExpDate AS (
	SELECT SupClassificationErrorsOmissions.SupClassificationErrorsOmissionsId, SupClassificationErrorsOmissions.CurrentSnapshotFlag, SupClassificationErrorsOmissions.EffectiveDate, SupClassificationErrorsOmissions.ExpirationDate, SupClassificationErrorsOmissions.LineOfBusinessAbbreviation, SupClassificationErrorsOmissions.RatingStateCode, SupClassificationErrorsOmissions.ClassCode, SupClassificationErrorsOmissions.ClassDescription, SupClassificationErrorsOmissions.OriginatingOrganizationCode 
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationErrorsOmissions
	where CurrentSnapshotFlag = 1
	ORDER BY SupClassificationErrorsOmissions.ClassCode,
	SupClassificationErrorsOmissions.RatingStateCode,
	SupClassificationErrorsOmissions.EffectiveDate DESC,
	SupClassificationErrorsOmissions.CreatedDate DESC
),
EXP_Lag_Eff_dates AS (
	SELECT
	SupClassificationErrorsOmissionsId,
	EffectiveDate,
	ExpirationDate,
	LineOfBusinessAbbreviation,
	RatingStateCode,
	ClassCode,
	ClassDescription,
	OriginatingOrganizationCode,
	CurrentSnapshotFlag AS i_CurrentSnapshotFlag,
	-- *INF*: DECODE(TRUE, RatingStateCode = v_PREV_ROW_RatingStateCode
	-- 								AND ClassCode = v_PREV_ROW_ClassCode
	--                                                    AND OriginatingOrganizationCode = v_PREV_ROW_OriginatingOrganizationCode
	-- 			 					AND (ClassDescription <>v_PREV_ROW_ClassDescription
	-- 								 -- OR EffectiveDate <> v_PREV_ROW_EffectiveDate
	-- 								OR  ADD_TO_DATE(ExpirationDate,'SS',+1) <>v_PREV_ROW_EffectiveDate   
	-- 								  
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
	FROM SQ_SupClassificationErrorsOmissions_CheckExpDate
),
FIL_FirstRowInAKGroup AS (
	SELECT
	SupClassificationErrorsOmissionsId, 
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag, 
	ModifiedDate, 
	ClassExpirationDate
	FROM EXP_Lag_Eff_dates
	WHERE CurrentSnapshotFlag = '0'
),
UPD_SupISOGLClassGroup AS (
	SELECT
	SupClassificationErrorsOmissionsId, 
	CurrentSnapshotFlag, 
	ModifiedDate, 
	ClassExpirationDate
	FROM FIL_FirstRowInAKGroup
),
SupClassificationErrorsOmissions_IL_CheckExpDate AS (
	MERGE INTO SupClassificationErrorsOmissions AS T
	USING UPD_SupISOGLClassGroup AS S
	ON T.SupClassificationErrorsOmissionsId = S.SupClassificationErrorsOmissionsId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ClassExpirationDate, T.ModifiedDate = S.ModifiedDate
),
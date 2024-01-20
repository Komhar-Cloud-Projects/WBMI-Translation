WITH
SQ_SupNurseImpact AS (
	SELECT
	S.NurseImpactId, 
	S.EffectiveDate, 
	S.ImpactType, 
	S.ImpactCategory, 
	S.Description, 
	S.ImpactCategoryExpirationDate 
	
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupNurseImpact S
	
	where
		S.CreatedDate >= '@{pipeline().parameters.SELECTION_START_TS}'
	AND
	      S.CurrentSnapshotFlag = 1
),
EXP_Src_Values_Default AS (
	SELECT
	NurseImpactId,
	ImpactType AS IN_ImpactType,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(IN_ImpactType),'N/A',
	-- IS_SPACES(IN_ImpactType),'N/A',
	-- LENGTH(IN_ImpactType)=0,'N/A',
	-- LTRIM(RTRIM(IN_ImpactType)))
	-- 
	-- 
	DECODE(
	    TRUE,
	    IN_ImpactType IS NULL, 'N/A',
	    LENGTH(IN_ImpactType)>0 AND TRIM(IN_ImpactType)='', 'N/A',
	    LENGTH(IN_ImpactType) = 0, 'N/A',
	    LTRIM(RTRIM(IN_ImpactType))
	) AS ImpactType,
	ImpactCategory AS IN_ImpactCategory,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(IN_ImpactCategory),'N/A',
	-- IS_SPACES(IN_ImpactCategory),'N/A',
	-- LENGTH(IN_ImpactCategory)=0,'N/A',
	-- LTRIM(RTRIM(IN_ImpactCategory)))
	DECODE(
	    TRUE,
	    IN_ImpactCategory IS NULL, 'N/A',
	    LENGTH(IN_ImpactCategory)>0 AND TRIM(IN_ImpactCategory)='', 'N/A',
	    LENGTH(IN_ImpactCategory) = 0, 'N/A',
	    LTRIM(RTRIM(IN_ImpactCategory))
	) AS ImpactCategory,
	Description AS IN_Description,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(IN_Description),'N/A',
	-- IS_SPACES(IN_Description),'N/A',
	-- LENGTH(IN_Description)=0,'N/A',
	-- LTRIM(RTRIM(IN_Description)))
	DECODE(
	    TRUE,
	    IN_Description IS NULL, 'N/A',
	    LENGTH(IN_Description)>0 AND TRIM(IN_Description)='', 'N/A',
	    LENGTH(IN_Description) = 0, 'N/A',
	    LTRIM(RTRIM(IN_Description))
	) AS Description,
	ImpactCategoryExpirationDate AS IN_ImpactCategoryExpirationDate,
	-- *INF*: iif(isnull(ltrim(rtrim(IN_ImpactCategoryExpirationDate))),TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS'),IN_ImpactCategoryExpirationDate)
	IFF(
	    ltrim(rtrim(IN_ImpactCategoryExpirationDate)) IS NULL,
	    TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'),
	    IN_ImpactCategoryExpirationDate
	) AS v_ImpactCategoryExpirationDate,
	v_ImpactCategoryExpirationDate AS ImpactCategoryExpirationDate,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate
	FROM SQ_SupNurseImpact
),
LKP_NurseAssignmentImpactDim AS (
	SELECT
	NurseAssignmentImpactDimId,
	EdwSupNurseImpactPkId
	FROM (
		SELECT
		N.NurseAssignmentImpactDimId as NurseAssignmentImpactDimId,
		N.EdwSupNurseImpactPkId as EdwSupNurseImpactPkId
		
		 FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.NurseAssignmentImpactDim N
		
		where
		N.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EdwSupNurseImpactPkId ORDER BY NurseAssignmentImpactDimId) = 1
),
RTR_Insert_Update AS (
	SELECT
	LKP_NurseAssignmentImpactDim.NurseAssignmentImpactDimId AS Lkp_NurseAssignmentImpactDimId,
	EXP_Src_Values_Default.CurrentSnapshotFlag,
	EXP_Src_Values_Default.AuditID,
	EXP_Src_Values_Default.EffectiveDate,
	EXP_Src_Values_Default.ExpirationDate,
	EXP_Src_Values_Default.CreatedDate,
	EXP_Src_Values_Default.ModifiedDate,
	EXP_Src_Values_Default.NurseImpactId AS EdwSupNurseImpactPkId,
	EXP_Src_Values_Default.ImpactType,
	EXP_Src_Values_Default.ImpactCategory,
	EXP_Src_Values_Default.Description
	FROM EXP_Src_Values_Default
	LEFT JOIN LKP_NurseAssignmentImpactDim
	ON LKP_NurseAssignmentImpactDim.EdwSupNurseImpactPkId = EXP_Src_Values_Default.NurseImpactId
),
RTR_Insert_Update_Insert AS (SELECT * FROM RTR_Insert_Update WHERE ISNULL(Lkp_NurseAssignmentImpactDimId)),
RTR_Insert_Update_DEFAULT1 AS (SELECT * FROM RTR_Insert_Update WHERE NOT ( (ISNULL(Lkp_NurseAssignmentImpactDimId)) )),
UPD_Insert AS (
	SELECT
	CurrentSnapshotFlag AS CurrentSnapshotFlag1, 
	AuditID AS AuditID1, 
	EffectiveDate AS EffectiveDate1, 
	ExpirationDate AS ExpirationDate1, 
	CreatedDate AS CreatedDate1, 
	ModifiedDate AS ModifiedDate1, 
	EdwSupNurseImpactPkId AS EdwSupNurseImpactPkId1, 
	ImpactType AS ImpactType1, 
	ImpactCategory AS ImpactCategory1, 
	Description AS Description1
	FROM RTR_Insert_Update_Insert
),
NurseAssignmentImpactDim_Insert AS (
	INSERT INTO NurseAssignmentImpactDim
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, EdwSupNurseImpactPkId, ImpactType, ImpactCategory, Description)
	SELECT 
	CurrentSnapshotFlag1 AS CURRENTSNAPSHOTFLAG, 
	AuditID1 AS AUDITID, 
	EffectiveDate1 AS EFFECTIVEDATE, 
	ExpirationDate1 AS EXPIRATIONDATE, 
	CreatedDate1 AS CREATEDDATE, 
	ModifiedDate1 AS MODIFIEDDATE, 
	EdwSupNurseImpactPkId1 AS EDWSUPNURSEIMPACTPKID, 
	ImpactType1 AS IMPACTTYPE, 
	ImpactCategory1 AS IMPACTCATEGORY, 
	Description1 AS DESCRIPTION
	FROM UPD_Insert
),
UPD_Update AS (
	SELECT
	Lkp_NurseAssignmentImpactDimId AS Lkp_NurseAssignmentImpactDimId2, 
	CurrentSnapshotFlag AS CurrentSnapshotFlag2, 
	AuditID AS AuditID2, 
	EffectiveDate AS EffectiveDate2, 
	ExpirationDate AS ExpirationDate2, 
	CreatedDate AS CreatedDate2, 
	ModifiedDate AS ModifiedDate2, 
	EdwSupNurseImpactPkId AS EdwSupNurseImpactPkId2, 
	ImpactType AS ImpactType2, 
	ImpactCategory AS ImpactCategory2, 
	Description AS Description2
	FROM RTR_Insert_Update_DEFAULT1
),
NurseAssignmentImpactDim_Update AS (
	MERGE INTO NurseAssignmentImpactDim AS T
	USING UPD_Update AS S
	ON T.NurseAssignmentImpactDimId = S.Lkp_NurseAssignmentImpactDimId2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag2, T.AuditID = S.AuditID2, T.EffectiveDate = S.EffectiveDate2, T.ExpirationDate = S.ExpirationDate2, T.CreatedDate = S.CreatedDate2, T.ModifiedDate = S.ModifiedDate2, T.EdwSupNurseImpactPkId = S.EdwSupNurseImpactPkId2, T.ImpactType = S.ImpactType2, T.ImpactCategory = S.ImpactCategory2, T.Description = S.Description2
),
SQ_NurseAssignmentImpactDim AS (
	SELECT
	A.NurseAssignmentImpactDimId,
	A.EffectiveDate,
	A.ExpirationDate,
	A.EdwSupNurseImpactPkId 
	
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.NurseAssignmentImpactDim A
	
	where Exists
	 (
	SELECT 1
	FROM 
	@{pipeline().parameters.TARGET_TABLE_OWNER}.NurseAssignmentImpactDim B
	
	WHERE
	A.EdwSupNurseImpactPkId = B.EdwSupNurseImpactPkId
	AND
	B.CurrentSnapshotFlag = 1
	
	GROUP BY
	B.EdwSupNurseImpactPkId
	HAVING 
	count(*) > 1
	 )
	
	ORDER BY 
	A.EdwSupNurseImpactPkId, 
	A.EffectiveDate DESC
),
EXP_Lag_ExpirationDate AS (
	SELECT
	NurseAssignmentImpactDimId,
	0 AS CurrentSnapshotFlag,
	EffectiveDate,
	ExpirationDate AS orig_ExpirationDate,
	-- *INF*: decode(true,
	-- EdwSupNurseImpactPkId = v_PREV_ROW_EdwSupNurseImpactPkId,
	-- add_to_date(v_PREV_ROW_EffectiveDate,'SS',-1),
	-- orig_ExpirationDate)
	decode(
	    true,
	    EdwSupNurseImpactPkId = v_PREV_ROW_EdwSupNurseImpactPkId, DATEADD(SECOND,- 1,v_PREV_ROW_EffectiveDate),
	    orig_ExpirationDate
	) AS v_ExpirationDate,
	v_ExpirationDate AS ExpirationDate,
	SYSDATE AS ModifiedDate,
	EdwSupNurseImpactPkId,
	EffectiveDate AS v_PREV_ROW_EffectiveDate,
	EdwSupNurseImpactPkId AS v_PREV_ROW_EdwSupNurseImpactPkId
	FROM SQ_NurseAssignmentImpactDim
),
FIL_FirstRowAkId AS (
	SELECT
	NurseAssignmentImpactDimId, 
	CurrentSnapshotFlag, 
	orig_ExpirationDate, 
	ExpirationDate, 
	ModifiedDate
	FROM EXP_Lag_ExpirationDate
	WHERE orig_ExpirationDate  !=  ExpirationDate
),
UPD_NurseAssignmentImpactDim AS (
	SELECT
	NurseAssignmentImpactDimId, 
	CurrentSnapshotFlag, 
	ExpirationDate, 
	ModifiedDate
	FROM FIL_FirstRowAkId
),
NurseAssignmentImpactDim_Expire AS (
	MERGE INTO NurseAssignmentImpactDim AS T
	USING UPD_NurseAssignmentImpactDim AS S
	ON T.NurseAssignmentImpactDimId = S.NurseAssignmentImpactDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),
WITH
SQ_nurse_assignment_impact_stage AS (
	SELECT
		NurseAssignmentImpactStageId,
		nurse_assignment_id,
		impact_type,
		impact_category,
		savings_amount,
		impact_comment,
		created_ts,
		created_user_id,
		modified_ts,
		modified_user_id,
		ExtractDate,
		SourceSystemId
	FROM nurse_assignment_impact_stage
),
EXP_Src_Values AS (
	SELECT
	nurse_assignment_id,
	-- *INF*: iif(isnull(ltrim(rtrim(nurse_assignment_id))), -1, nurse_assignment_id)
	IFF(ltrim(rtrim(nurse_assignment_id)) IS NULL, - 1, nurse_assignment_id) AS o_nurse_assignment_id,
	impact_type,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(impact_type),'N/A',
	-- IS_SPACES(impact_type),'N/A',
	-- LENGTH(impact_type)=0,'N/A',
	-- LTRIM(RTRIM(impact_type)))
	DECODE(TRUE,
	impact_type IS NULL, 'N/A',
	IS_SPACES(impact_type), 'N/A',
	LENGTH(impact_type) = 0, 'N/A',
	LTRIM(RTRIM(impact_type))) AS o_impact_type,
	impact_category,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(impact_category),'N/A',
	-- IS_SPACES(impact_category),'N/A',
	-- LENGTH(impact_category)=0,'N/A',
	-- LTRIM(RTRIM(impact_category)))
	DECODE(TRUE,
	impact_category IS NULL, 'N/A',
	IS_SPACES(impact_category), 'N/A',
	LENGTH(impact_category) = 0, 'N/A',
	LTRIM(RTRIM(impact_category))) AS o_impact_category,
	savings_amount,
	-- *INF*: iif(isnull(ltrim(rtrim(savings_amount))),0,savings_amount)
	IFF(ltrim(rtrim(savings_amount)) IS NULL, 0, savings_amount) AS o_saving_amount,
	impact_comment,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(impact_comment),'N/A',
	-- IS_SPACES(impact_comment),'N/A',
	-- LENGTH(impact_comment)=0,'N/A',
	-- LTRIM(RTRIM(impact_comment)))
	DECODE(TRUE,
	impact_comment IS NULL, 'N/A',
	IS_SPACES(impact_comment), 'N/A',
	LENGTH(impact_comment) = 0, 'N/A',
	LTRIM(RTRIM(impact_comment))) AS o_impact_comment
	FROM SQ_nurse_assignment_impact_stage
),
LKP_NurseAssignment AS (
	SELECT
	NurseAssignmentAkId,
	nurse_assignment_id
	FROM (
		SELECT
		N.NurseAssignmentAkId as NurseAssignmentAkId, 
		N.nurse_assignment_id as nurse_assignment_id
		
		FROM
		 @{pipeline().parameters.TARGET_TABLE_OWNER}.NurseAssignment N
		
		where
		N.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY nurse_assignment_id ORDER BY NurseAssignmentAkId) = 1
),
LKP_SupNurseImpact AS (
	SELECT
	NurseImpactId,
	ImpactType,
	ImpactCategory
	FROM (
		SELECT
		S.NurseImpactId as NurseImpactId, 
		S.ImpactType as ImpactType, 
		S.ImpactCategory as ImpactCategory 
		
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.SupNurseImpact S
		
		where
		S.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ImpactType,ImpactCategory ORDER BY NurseImpactId) = 1
),
EXP_Lkp_Default AS (
	SELECT
	LKP_NurseAssignment.NurseAssignmentAkId,
	-- *INF*: iif(isnull(NurseAssignmentAkId), -1, NurseAssignmentAkId)
	IFF(NurseAssignmentAkId IS NULL, - 1, NurseAssignmentAkId) AS o_NurseAssignmentAkId,
	LKP_SupNurseImpact.NurseImpactId,
	-- *INF*: iif(isnull(NurseImpactId), -1,NurseImpactId)
	IFF(NurseImpactId IS NULL, - 1, NurseImpactId) AS o_NurseImpactId
	FROM 
	LEFT JOIN LKP_NurseAssignment
	ON LKP_NurseAssignment.nurse_assignment_id = EXP_Src_Values.o_nurse_assignment_id
	LEFT JOIN LKP_SupNurseImpact
	ON LKP_SupNurseImpact.ImpactType = EXP_Src_Values.o_impact_type AND LKP_SupNurseImpact.ImpactCategory = EXP_Src_Values.o_impact_category
),
LKP_NurseAssignmentImpact AS (
	SELECT
	NurseAssignmentImpactId,
	NurseAssignmentImpactAkId,
	NurseAssignmentAkId,
	NurseImpactId,
	SavingsAmount,
	Comment
	FROM (
		SELECT
		N.NurseAssignmentImpactId as NurseAssignmentImpactId, N.NurseAssignmentImpactAkId as NurseAssignmentImpactAkId, N.SavingsAmount as SavingsAmount, N.Comment as Comment, N.NurseAssignmentAkId as NurseAssignmentAkId, N.NurseImpactId as NurseImpactId 
		
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.NurseAssignmentImpact N
		
		WHERE
		CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY NurseAssignmentAkId,NurseImpactId ORDER BY NurseAssignmentImpactId) = 1
),
EXP_TargetLkp_Detect_Changes AS (
	SELECT
	LKP_NurseAssignmentImpact.NurseAssignmentImpactId AS Lkp_NurseAssignmentImpactId,
	LKP_NurseAssignmentImpact.NurseAssignmentImpactAkId AS Lkp_NurseAssignmentImpactAkId,
	LKP_NurseAssignmentImpact.NurseAssignmentAkId AS Lkp_NurseAssignmentAkId,
	LKP_NurseAssignmentImpact.NurseImpactId AS Lkp_NurseImpactId,
	LKP_NurseAssignmentImpact.SavingsAmount AS Lkp_SavingsAmount,
	LKP_NurseAssignmentImpact.Comment AS Lkp_Comment,
	-- *INF*: iif(isnull(Lkp_NurseAssignmentImpactId), 'NEW', 
	-- 
	--   iif( 
	-- 
	--     ltrim(rtrim(Lkp_NurseAssignmentAkId)) != ltrim(rtrim(NurseAssignmentAkId))
	-- 
	-- or
	-- 
	--     ltrim(rtrim(Lkp_NurseImpactId)) != ltrim(rtrim(NurseImpactId))
	-- 
	-- or
	-- 
	--     ltrim(rtrim(Lkp_SavingsAmount)) != ltrim(rtrim(SavingsAmount))
	-- 
	-- or
	-- 
	-- ltrim(rtrim(Lkp_Comment)) != ltrim(rtrim(Comment)),
	-- 
	--    'UPDATE', 'NOCHANGE' )
	-- 
	--    )
	-- 
	IFF(Lkp_NurseAssignmentImpactId IS NULL, 'NEW', IFF(ltrim(rtrim(Lkp_NurseAssignmentAkId)) != ltrim(rtrim(NurseAssignmentAkId)) OR ltrim(rtrim(Lkp_NurseImpactId)) != ltrim(rtrim(NurseImpactId)) OR ltrim(rtrim(Lkp_SavingsAmount)) != ltrim(rtrim(SavingsAmount)) OR ltrim(rtrim(Lkp_Comment)) != ltrim(rtrim(Comment)), 'UPDATE', 'NOCHANGE')) AS v_ChangedFlag,
	v_ChangedFlag AS ChangedFlag,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	-- *INF*: iif(v_ChangedFlag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_ChangedFlag = 'NEW', to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), sysdate) AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemId,
	sysdate AS CreatedDate,
	sysdate AS ModifiedDate,
	EXP_Lkp_Default.o_NurseAssignmentAkId AS NurseAssignmentAkId,
	EXP_Lkp_Default.o_NurseImpactId AS NurseImpactId,
	EXP_Src_Values.o_saving_amount AS SavingsAmount,
	EXP_Src_Values.o_impact_comment AS Comment
	FROM EXP_Lkp_Default
	 -- Manually join with EXP_Src_Values
	LEFT JOIN LKP_NurseAssignmentImpact
	ON LKP_NurseAssignmentImpact.NurseAssignmentAkId = EXP_Lkp_Default.o_NurseAssignmentAkId AND LKP_NurseAssignmentImpact.NurseImpactId = EXP_Lkp_Default.o_NurseImpactId
),
FIL_Lkp_Records AS (
	SELECT
	ChangedFlag, 
	CurrentSnapshotFlag, 
	AuditId, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemId, 
	CreatedDate, 
	ModifiedDate, 
	NurseAssignmentAkId, 
	NurseImpactId, 
	SavingsAmount, 
	Comment, 
	Lkp_NurseAssignmentImpactAkId
	FROM EXP_TargetLkp_Detect_Changes
	WHERE ChangedFlag = 'NEW' or ChangedFlag = 'UPDATE'
),
SEQ_NurseAssignmentImpact AS (
	CREATE SEQUENCE SEQ_NurseAssignmentImpact
	START = 0
	INCREMENT = 1;
),
EXP_AKid_Insert_Target AS (
	SELECT
	ChangedFlag,
	CurrentSnapshotFlag,
	AuditId,
	EffectiveDate,
	ExpirationDate,
	SourceSystemId,
	CreatedDate,
	ModifiedDate,
	NurseAssignmentAkId,
	NurseImpactId,
	SavingsAmount,
	Comment,
	-- *INF*: iif(ChangedFlag = 'NEW', NEXTVAL, Lkp_NurseAssignmentImpactAkId)
	IFF(ChangedFlag = 'NEW', NEXTVAL, Lkp_NurseAssignmentImpactAkId) AS NurseAssignmentImpactAkId,
	Lkp_NurseAssignmentImpactAkId,
	SEQ_NurseAssignmentImpact.NEXTVAL
	FROM FIL_Lkp_Records
),
NurseAssignmentImpact_Insert AS (
	INSERT INTO NurseAssignmentImpact
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, NurseAssignmentImpactAkId, NurseAssignmentAkId, NurseImpactId, SavingsAmount, Comment)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	NURSEASSIGNMENTIMPACTAKID, 
	NURSEASSIGNMENTAKID, 
	NURSEIMPACTID, 
	SAVINGSAMOUNT, 
	COMMENT
	FROM EXP_AKid_Insert_Target
),
SQ_NurseAssignmentImpact AS (
	SELECT
	A.NurseAssignmentImpactId,
	A.EffectiveDate,
	A.ExpirationDate,
	A.NurseAssignmentImpactAkId 
	
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.NurseAssignmentImpact A
	
	where Exists
	   (
	SELECT 1 
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.NurseAssignmentImpact B
	where
	B.CurrentSnapshotFlag = 1
	AND
	A.NurseAssignmentImpactAkId = B.NurseAssignmentImpactAkId
	
	group by 
	B.NurseAssignmentImpactAkId
	
	having 
	count(*) > 1
	    )
	
	AND
	A.CurrentSnapshotFlag = 1
	
	order by 
	A.NurseAssignmentImpactAkId, 
	A.EffectiveDate DESC
),
EXP_Lag_ExpirationDate AS (
	SELECT
	NurseAssignmentImpactId,
	0 AS CurrentSnapshotFlag,
	EffectiveDate,
	ExpirationDate AS orig_ExpirationDate,
	-- *INF*: decode(true,
	-- NurseAssignmentImpactAkId = v_PREV_ROW_NurseAssignmentImpactAkId,
	-- add_to_date(v_PREV_ROW_EffectiveDate, 'SS', -1),
	-- orig_ExpirationDate)
	decode(true,
	NurseAssignmentImpactAkId = v_PREV_ROW_NurseAssignmentImpactAkId, add_to_date(v_PREV_ROW_EffectiveDate, 'SS', - 1),
	orig_ExpirationDate) AS v_ExpirationDate,
	v_ExpirationDate AS ExpirationDate,
	EffectiveDate AS v_PREV_ROW_EffectiveDate,
	NurseAssignmentImpactAkId AS v_PREV_ROW_NurseAssignmentImpactAkId,
	SYSDATE AS ModifiedDate,
	NurseAssignmentImpactAkId
	FROM SQ_NurseAssignmentImpact
),
FIL_FirsrRowAkId AS (
	SELECT
	NurseAssignmentImpactId, 
	CurrentSnapshotFlag, 
	orig_ExpirationDate, 
	ExpirationDate, 
	ModifiedDate
	FROM EXP_Lag_ExpirationDate
	WHERE orig_ExpirationDate != ExpirationDate
),
UPD_NurseAssignmentImpact AS (
	SELECT
	NurseAssignmentImpactId, 
	CurrentSnapshotFlag, 
	ExpirationDate, 
	ModifiedDate
	FROM FIL_FirsrRowAkId
),
NurseAssignmentImpact_Update AS (
	MERGE INTO NurseAssignmentImpact AS T
	USING UPD_NurseAssignmentImpact AS S
	ON T.NurseAssignmentImpactId = S.NurseAssignmentImpactId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),
SQ_NurseAssignmentImpact_Expire AS (
	select  
	NAI.NurseAssignmentImpactId,
	NAI.EffectiveDate,
	NAI.ExpirationDate,
	NAI.NurseAssignmentAkId
	
	from
	@{pipeline().parameters.TARGET_TABLE_OWNER}.NurseAssignmentImpact NAI,
	@{pipeline().parameters.TARGET_TABLE_OWNER}.SupNurseImpact SI,
	@{pipeline().parameters.TARGET_TABLE_OWNER}.NurseAssignment NA,
	WC_Stage.dbo.nurse_assignment_impact_deleted_stage NAID
	
	where
	NAI.NurseAssignmentAkId = NA.NurseAssignmentAkId
	AND
	SI.NurseImpactId = NAI.NurseImpactId
	AND
	NAID.nurse_assignment_id = NA.nurse_assignment_id
	AND
	SI.ImpactCategory = NAID.impact_category 
	AND
	SI.ImpactType = NAID.impact_type
	AND
	NAI.CurrentSnapshotFlag = 1
	AND
	SI.CurrentSnapshotFlag = 1
	AND
	NA.CurrentSnapshotFlag =1
),
EXP_Expire AS (
	SELECT
	NurseAssignmentImpactId,
	EffectiveDate,
	ExpirationDate,
	NurseAssignmentAkId,
	0 AS CurrentSnapshotFlag,
	SYSDATE AS ModifiedDate
	FROM SQ_NurseAssignmentImpact_Expire
),
FIL_Expire AS (
	SELECT
	NurseAssignmentImpactId, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM EXP_Expire
	WHERE NOT isnull(NurseAssignmentImpactId)
),
UPD_Expire AS (
	SELECT
	NurseAssignmentImpactId, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM FIL_Expire
),
NurseAssignmentImpact_Expire1 AS (
	MERGE INTO NurseAssignmentImpact AS T
	USING UPD_Expire AS S
	ON T.NurseAssignmentImpactId = S.NurseAssignmentImpactId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ModifiedDate = S.ModifiedDate
),
WITH
SQ_clmt_nurse_assignment_stage AS (
	SELECT
		ClmtNurseAssignmentStageId,
		nurse_assignment_id,
		clmt_nurse_manage_id,
		assigned_nurse_id,
		open_date,
		closed_date,
		assignment_comment,
		work_time_saved_weeks,
		work_time_saved_days,
		created_ts,
		created_user_id,
		modified_ts,
		modified_user_id,
		ExtractDate,
		SourceSystemId
	FROM clmt_nurse_assignment_stage
),
EXP_Src_Values AS (
	SELECT
	nurse_assignment_id,
	-- *INF*: iif(isnull(ltrim(rtrim(nurse_assignment_id))),-1,nurse_assignment_id)
	IFF(ltrim(rtrim(nurse_assignment_id
			)
		) IS NULL,
		- 1,
		nurse_assignment_id
	) AS o_nurse_assignment_id,
	clmt_nurse_manage_id,
	-- *INF*: iif(isnull(ltrim(rtrim(clmt_nurse_manage_id))),-1,clmt_nurse_manage_id)
	IFF(ltrim(rtrim(clmt_nurse_manage_id
			)
		) IS NULL,
		- 1,
		clmt_nurse_manage_id
	) AS o_clmt_nurse_manage_id,
	assigned_nurse_id,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(assigned_nurse_id),'N/A',
	-- IS_SPACES(assigned_nurse_id),'N/A',
	-- LENGTH(assigned_nurse_id)=0,'N/A',
	-- LTRIM(RTRIM(assigned_nurse_id)))
	DECODE(TRUE,
		assigned_nurse_id IS NULL, 'N/A',
		LENGTH(assigned_nurse_id)>0 AND TRIM(assigned_nurse_id)='', 'N/A',
		LENGTH(assigned_nurse_id
		) = 0, 'N/A',
		LTRIM(RTRIM(assigned_nurse_id
			)
		)
	) AS o_assigned_nurse_id,
	open_date,
	-- *INF*: iif(isnull(ltrim(rtrim(open_date))),to_date('01/01/1800','MM/DD/YYYY'),open_date)
	IFF(ltrim(rtrim(open_date
			)
		) IS NULL,
		to_date('01/01/1800', 'MM/DD/YYYY'
		),
		open_date
	) AS o_open_date,
	closed_date,
	-- *INF*: iif(isnull(ltrim(rtrim(closed_date))),to_date('12/31/2100','MM/DD/YYYY'),closed_date)
	IFF(ltrim(rtrim(closed_date
			)
		) IS NULL,
		to_date('12/31/2100', 'MM/DD/YYYY'
		),
		closed_date
	) AS o_closed_date,
	assignment_comment,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(assignment_comment),'N/A',
	-- IS_SPACES(assignment_comment),'N/A',
	-- LENGTH(assignment_comment)=0,'N/A',
	-- LTRIM(RTRIM(assignment_comment)))
	DECODE(TRUE,
		assignment_comment IS NULL, 'N/A',
		LENGTH(assignment_comment)>0 AND TRIM(assignment_comment)='', 'N/A',
		LENGTH(assignment_comment
		) = 0, 'N/A',
		LTRIM(RTRIM(assignment_comment
			)
		)
	) AS o_assignment_comment,
	work_time_saved_weeks,
	-- *INF*: iif(isnull(ltrim(rtrim(work_time_saved_weeks))),0,work_time_saved_weeks)
	IFF(ltrim(rtrim(work_time_saved_weeks
			)
		) IS NULL,
		0,
		work_time_saved_weeks
	) AS o_work_time_saved_weeks,
	work_time_saved_days,
	-- *INF*: iif(isnull(ltrim(rtrim(work_time_saved_days))),0,work_time_saved_days)
	IFF(ltrim(rtrim(work_time_saved_days
			)
		) IS NULL,
		0,
		work_time_saved_days
	) AS o_work_time_saved_days
	FROM SQ_clmt_nurse_assignment_stage
),
LKP_NurseCase AS (
	SELECT
	NurseCaseAkId,
	clmt_nurse_manage_id
	FROM (
		SELECT
		N.NurseCaseAkId as NurseCaseAkId, 
		N.clmt_nurse_manage_id as clmt_nurse_manage_id
		
		 FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.NurseCase N
		
		where
		N.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY clmt_nurse_manage_id ORDER BY NurseCaseAkId) = 1
),
LKP_claim_party AS (
	SELECT
	claim_party_ak_id,
	claim_party_key
	FROM (
		SELECT
		CP.claim_party_ak_id as claim_party_ak_id, 
		CP.claim_party_key as claim_party_key
		
		 FROM
		   @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party CP
		
		where
		CP.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_key ORDER BY claim_party_ak_id) = 1
),
EXP_Lkp_Default AS (
	SELECT
	LKP_claim_party.claim_party_ak_id,
	-- *INF*: iif(isnull(claim_party_ak_id),-1,claim_party_ak_id)
	IFF(claim_party_ak_id IS NULL,
		- 1,
		claim_party_ak_id
	) AS o_claim_party_ak_id,
	LKP_NurseCase.NurseCaseAkId,
	-- *INF*: iif(isnull(NurseCaseAkId),-1,NurseCaseAkId)
	IFF(NurseCaseAkId IS NULL,
		- 1,
		NurseCaseAkId
	) AS o_NurseCaseAkId
	FROM 
	LEFT JOIN LKP_NurseCase
	ON LKP_NurseCase.clmt_nurse_manage_id = EXP_Src_Values.o_clmt_nurse_manage_id
	LEFT JOIN LKP_claim_party
	ON LKP_claim_party.claim_party_key = EXP_Src_Values.o_assigned_nurse_id
),
LKP_NurseAssignment AS (
	SELECT
	NurseAssignmentId,
	NurseAssignmentAkId,
	NurseCaseAkId,
	claim_party_ak_id,
	nurse_assignment_id,
	OpenDate,
	ClosedDate,
	Comment,
	TimeSavedWeeks,
	TimeSavedDays
	FROM (
		SELECT NurseAssignment.NurseAssignmentId as NurseAssignmentId, NurseAssignment.NurseAssignmentAkId as NurseAssignmentAkId, NurseAssignment.nurse_assignment_id as nurse_assignment_id, NurseAssignment.OpenDate as OpenDate, NurseAssignment.ClosedDate as ClosedDate, NurseAssignment.Comment as Comment, NurseAssignment.TimeSavedWeeks as TimeSavedWeeks, NurseAssignment.TimeSavedDays as TimeSavedDays, NurseAssignment.NurseCaseAkId as NurseCaseAkId, NurseAssignment.claim_party_ak_id as claim_party_ak_id 
		
		FROM NurseAssignment
		
		where 
		CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY NurseCaseAkId,nurse_assignment_id,claim_party_ak_id ORDER BY NurseAssignmentId) = 1
),
EXP_TargetLkp_Detect_Changes AS (
	SELECT
	LKP_NurseAssignment.NurseAssignmentId AS Lkp_NurseAssignmentId,
	LKP_NurseAssignment.NurseAssignmentAkId AS Lkp_NurseAssignmentAkId,
	LKP_NurseAssignment.NurseCaseAkId AS Lkp_NurseCaseAkId,
	LKP_NurseAssignment.claim_party_ak_id AS Lkp_claim_party_ak_id,
	LKP_NurseAssignment.nurse_assignment_id AS Lkp_nurse_assignment_id,
	LKP_NurseAssignment.OpenDate AS Lkp_OpenDate,
	LKP_NurseAssignment.ClosedDate AS Lkp_ClosedDate,
	LKP_NurseAssignment.Comment AS Lkp_Comment,
	LKP_NurseAssignment.TimeSavedWeeks AS Lkp_TimeSavedWeeks,
	LKP_NurseAssignment.TimeSavedDays AS Lkp_TimeSavedDays,
	-- *INF*: iif(isnull(Lkp_NurseAssignmentId), 'NEW',
	-- 
	--   iif(
	-- 
	--  --      ltrim(rtrim(Lkp_NurseAssignmentAkId)) != ltrim(rtrim(NurseAssignmentAkId))
	-- 
	-- --or
	-- 
	--        ltrim(rtrim(Lkp_NurseCaseAkId)) != ltrim(rtrim(NurseCaseAkId))
	-- 
	-- or
	-- 
	--        ltrim(rtrim(Lkp_claim_party_ak_id)) != ltrim(rtrim(claim_party_ak_id))
	-- 
	-- or
	-- 
	--        ltrim(rtrim(Lkp_nurse_assignment_id)) != ltrim(rtrim(nurse_assignment_id))
	-- 
	-- or
	-- 
	--        ltrim(rtrim(Lkp_OpenDate)) != ltrim(rtrim(OpenDate))
	-- 
	-- or
	-- 
	--        ltrim(rtrim(Lkp_ClosedDate)) != ltrim(rtrim(ClosedDate))
	-- 
	-- or
	-- 
	--      ltrim(rtrim(Lkp_Comment)) != ltrim(rtrim(Comment))
	-- 
	-- or
	-- 
	--        ltrim(rtrim(Lkp_TimeSavedWeeks)) != ltrim(rtrim(TimeSavedWeeks))
	-- 
	-- or
	-- 
	--        ltrim(rtrim(Lkp_TimeSavedDays)) != ltrim(rtrim(TimeSavedDays)),
	-- 
	--   'UPDATE', 'NOCHANGE')
	-- 
	--    )
	IFF(Lkp_NurseAssignmentId IS NULL,
		'NEW',
		IFF(ltrim(rtrim(Lkp_NurseCaseAkId
				)
			) != ltrim(rtrim(NurseCaseAkId
				)
			) 
			OR ltrim(rtrim(Lkp_claim_party_ak_id
				)
			) != ltrim(rtrim(claim_party_ak_id
				)
			) 
			OR ltrim(rtrim(Lkp_nurse_assignment_id
				)
			) != ltrim(rtrim(nurse_assignment_id
				)
			) 
			OR ltrim(rtrim(Lkp_OpenDate
				)
			) != ltrim(rtrim(OpenDate
				)
			) 
			OR ltrim(rtrim(Lkp_ClosedDate
				)
			) != ltrim(rtrim(ClosedDate
				)
			) 
			OR ltrim(rtrim(Lkp_Comment
				)
			) != ltrim(rtrim(Comment
				)
			) 
			OR ltrim(rtrim(Lkp_TimeSavedWeeks
				)
			) != ltrim(rtrim(TimeSavedWeeks
				)
			) 
			OR ltrim(rtrim(Lkp_TimeSavedDays
				)
			) != ltrim(rtrim(TimeSavedDays
				)
			),
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_ChangedFlag,
	v_ChangedFlag AS ChangedFlag,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	-- *INF*: iif(v_ChangedFlag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_ChangedFlag = 'NEW',
		to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		sysdate
	) AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemId,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	EXP_Lkp_Default.o_NurseCaseAkId AS NurseCaseAkId,
	EXP_Lkp_Default.o_claim_party_ak_id AS claim_party_ak_id,
	EXP_Src_Values.o_nurse_assignment_id AS nurse_assignment_id,
	EXP_Src_Values.o_open_date AS OpenDate,
	EXP_Src_Values.o_closed_date AS ClosedDate,
	EXP_Src_Values.o_assignment_comment AS Comment,
	EXP_Src_Values.o_work_time_saved_weeks AS TimeSavedWeeks,
	EXP_Src_Values.o_work_time_saved_days AS TimeSavedDays
	FROM EXP_Lkp_Default
	 -- Manually join with EXP_Src_Values
	LEFT JOIN LKP_NurseAssignment
	ON LKP_NurseAssignment.NurseCaseAkId = EXP_Lkp_Default.o_NurseCaseAkId AND LKP_NurseAssignment.nurse_assignment_id = EXP_Src_Values.o_nurse_assignment_id AND LKP_NurseAssignment.claim_party_ak_id = EXP_Lkp_Default.o_claim_party_ak_id
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
	NurseCaseAkId, 
	claim_party_ak_id, 
	nurse_assignment_id, 
	OpenDate, 
	ClosedDate, 
	Comment AS o_Comment, 
	TimeSavedWeeks AS o_TimeSavedWeeks, 
	TimeSavedDays AS o_TimeSavedDays, 
	Lkp_NurseAssignmentAkId
	FROM EXP_TargetLkp_Detect_Changes
	WHERE ChangedFlag = 'NEW' or ChangedFlag = 'UPDATE'
),
SEQ_NurseAssignment AS (
	CREATE SEQUENCE SEQ_NurseAssignment
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
	NurseCaseAkId AS o_NurseCaseAkId,
	claim_party_ak_id AS o_claim_party_ak_id,
	nurse_assignment_id AS o_nurse_assignment_id,
	OpenDate,
	ClosedDate,
	o_Comment,
	o_TimeSavedWeeks,
	o_TimeSavedDays,
	-- *INF*: iif(ChangedFlag = 'NEW', NEXTVAL, Lkp_NurseAssignmentAkId)
	IFF(ChangedFlag = 'NEW',
		NEXTVAL,
		Lkp_NurseAssignmentAkId
	) AS NurseAssignmentAkId,
	Lkp_NurseAssignmentAkId,
	SEQ_NurseAssignment.NEXTVAL
	FROM FIL_Lkp_Records
),
NurseAssignment_Insert AS (
	INSERT INTO NurseAssignment
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, NurseAssignmentAkId, NurseCaseAkId, claim_party_ak_id, nurse_assignment_id, OpenDate, ClosedDate, Comment, TimeSavedWeeks, TimeSavedDays)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	NURSEASSIGNMENTAKID, 
	o_NurseCaseAkId AS NURSECASEAKID, 
	o_claim_party_ak_id AS CLAIM_PARTY_AK_ID, 
	o_nurse_assignment_id AS NURSE_ASSIGNMENT_ID, 
	OPENDATE, 
	CLOSEDDATE, 
	o_Comment AS COMMENT, 
	o_TimeSavedWeeks AS TIMESAVEDWEEKS, 
	o_TimeSavedDays AS TIMESAVEDDAYS
	FROM EXP_AKid_Insert_Target
),
SQ_NurseAssignment AS (
	SELECT
	A.NurseAssignmentId,
	A.EffectiveDate,
	A.ExpirationDate,
	A.NurseAssignmentAkId 
	
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.NurseAssignment A
	
	Where Exists 
	    ( 
	SELECT 1
	FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.NurseAssignment B
	
	where
	B.CurrentSnapshotFlag =1
	AND
	A.NurseAssignmentAkId = B.NurseAssignmentAkId 
	
	group by 
	B.NurseAssignmentAkId 
	
	having 
	count(*) > 1 
	  )
	
	AND
	A.CurrentSnapshotFlag = 1
	
	order by 
	A.NurseAssignmentAkId , 
	A.EffectiveDate DESC
),
EXP_Lag_ExpirationDate AS (
	SELECT
	NurseAssignmentId,
	EffectiveDate,
	ExpirationDate AS orig_ExpirationDate,
	-- *INF*: DECODE(TRUE,
	-- 	NurseAssignmentAkId= v_PREV_ROW_NurseAssignmentAkId, ADD_TO_DATE(v_PREV_ROW_EffectiveDate,'SS',-1),
	-- 	orig_ExpirationDate)
	DECODE(TRUE,
		NurseAssignmentAkId = v_PREV_ROW_NurseAssignmentAkId, DATEADD(SECOND,- 1,v_PREV_ROW_EffectiveDate),
		orig_ExpirationDate
	) AS v_ExpirationDate,
	v_ExpirationDate AS ExpirationDate,
	SYSDATE AS ModifiedDate,
	EffectiveDate AS v_PREV_ROW_EffectiveDate,
	NurseAssignmentAkId AS v_PREV_ROW_NurseAssignmentAkId,
	NurseAssignmentAkId,
	0 AS CurrentSnapshotFlag
	FROM SQ_NurseAssignment
),
FIL_FirstRowAkId AS (
	SELECT
	NurseAssignmentId, 
	orig_ExpirationDate, 
	ExpirationDate, 
	ModifiedDate, 
	CurrentSnapshotFlag
	FROM EXP_Lag_ExpirationDate
	WHERE orig_ExpirationDate != ExpirationDate
),
UPD_NurseAssignment AS (
	SELECT
	NurseAssignmentId, 
	ExpirationDate, 
	ModifiedDate, 
	CurrentSnapshotFlag
	FROM FIL_FirstRowAkId
),
NurseAssignment_Update AS (
	MERGE INTO NurseAssignment AS T
	USING UPD_NurseAssignment AS S
	ON T.NurseAssignmentId = S.NurseAssignmentId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),
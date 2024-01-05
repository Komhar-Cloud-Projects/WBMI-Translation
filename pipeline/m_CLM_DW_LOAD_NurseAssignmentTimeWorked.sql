WITH
SQ_nurse_assignment_time_worked_stage AS (
	SELECT
		NurseAssignmentTimeWorkedStageId,
		nurse_assignment_id,
		time_worked_seq,
		date_worked,
		time_worked_hours,
		created_ts,
		created_user_id,
		modified_ts,
		modified_user_id,
		ExtractDate,
		SourceSystemId
	FROM nurse_assignment_time_worked_stage
),
EXP_Src_Values AS (
	SELECT
	nurse_assignment_id,
	-- *INF*: iif(isnull(ltrim(rtrim(nurse_assignment_id))),
	--  -1,nurse_assignment_id)
	IFF(ltrim(rtrim(nurse_assignment_id)) IS NULL, - 1, nurse_assignment_id) AS o_nurse_assignment_id,
	time_worked_seq,
	-- *INF*: iif(isnull(ltrim(rtrim(time_worked_seq))), -1, time_worked_seq)
	IFF(ltrim(rtrim(time_worked_seq)) IS NULL, - 1, time_worked_seq) AS o_time_worked_seq,
	date_worked,
	-- *INF*: iif(isnull(ltrim(rtrim(date_worked))), to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'), date_worked)
	IFF(ltrim(rtrim(date_worked)) IS NULL, to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), date_worked) AS o_date_worked,
	time_worked_hours,
	-- *INF*: iif(isnull(ltrim(rtrim(time_worked_hours))), 0, time_worked_hours)
	IFF(ltrim(rtrim(time_worked_hours)) IS NULL, 0, time_worked_hours) AS o_time_worked_hours
	FROM SQ_nurse_assignment_time_worked_stage
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
EXP_Lkp_Defualt AS (
	SELECT
	NurseAssignmentAkId,
	-- *INF*: iif(isnull(NurseAssignmentAkId), -1,NurseAssignmentAkId)
	IFF(NurseAssignmentAkId IS NULL, - 1, NurseAssignmentAkId) AS o_NurseAssignmentAkId
	FROM LKP_NurseAssignment
),
LKP_NurseAssignmentTimeWorked AS (
	SELECT
	NurseAssignmentTimeWorkedId,
	NurseAssignmentTimeWorkedAkId,
	NurseAssignmentAkId,
	TimeWorkedSequence,
	WorkedDate,
	TimeWorkedHours,
	IN_TimeWorkedSequence
	FROM (
		SELECT 
		N.NurseAssignmentTimeWorkedId as NurseAssignmentTimeWorkedId, N.NurseAssignmentTimeWorkedAkId as NurseAssignmentTimeWorkedAkId, 
		N.TimeWorkedSequence as TimeWorkedSequence, 
		N.WorkedDate as WorkedDate, 
		N.TimeWorkedHours as TimeWorkedHours, 
		N.NurseAssignmentAkId as NurseAssignmentAkId 
		
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.NurseAssignmentTimeWorked N
		
		where
		N.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY NurseAssignmentAkId,TimeWorkedSequence ORDER BY NurseAssignmentTimeWorkedId) = 1
),
EXP_TargetLkp_Detect_Changes AS (
	SELECT
	LKP_NurseAssignmentTimeWorked.NurseAssignmentTimeWorkedId AS Lkp_NurseAssignmentTimeWorkedId,
	LKP_NurseAssignmentTimeWorked.NurseAssignmentTimeWorkedAkId AS Lkp_NurseAssignmentTimeWorkedAkId,
	LKP_NurseAssignmentTimeWorked.NurseAssignmentAkId AS Lkp_NurseAssignmentAkId,
	LKP_NurseAssignmentTimeWorked.TimeWorkedSequence AS Lkp_TimeWorkedSequence,
	LKP_NurseAssignmentTimeWorked.WorkedDate AS Lkp_WorkedDate,
	LKP_NurseAssignmentTimeWorked.TimeWorkedHours AS Lkp_TimeWorkedHours,
	-- *INF*: iif(isnull(Lkp_NurseAssignmentTimeWorkedId), 'NEW',
	-- 
	--   iif(  
	-- 
	--  ltrim(rtrim(Lkp_NurseAssignmentAkId)) != ltrim(rtrim(NurseAssignmentAkId))
	-- 
	-- or
	-- 
	--    ltrim(rtrim(Lkp_TimeWorkedSequence)) != ltrim(rtrim(TimeWorkedSequence))
	-- 
	-- or
	-- 
	--    ltrim(rtrim(Lkp_WorkedDate)) != ltrim(rtrim(WorkedDate))
	-- 
	-- or
	-- 
	--    ltrim(rtrim(Lkp_TimeWorkedHours)) != ltrim(rtrim(TimeWorkedHours)),
	-- 
	-- --or
	-- 
	-- --   ltrim(rtrim(Lkp_NurseAssignmentTimeWorkedAkId)) != ltrim(rtrim(NurseAssignmentTimeWorkedAkId))
	-- 
	--    'UPDATE', 'NOCHANGE')
	--   
	--    )
	IFF(Lkp_NurseAssignmentTimeWorkedId IS NULL, 'NEW', IFF(ltrim(rtrim(Lkp_NurseAssignmentAkId)) != ltrim(rtrim(NurseAssignmentAkId)) OR ltrim(rtrim(Lkp_TimeWorkedSequence)) != ltrim(rtrim(TimeWorkedSequence)) OR ltrim(rtrim(Lkp_WorkedDate)) != ltrim(rtrim(WorkedDate)) OR ltrim(rtrim(Lkp_TimeWorkedHours)) != ltrim(rtrim(TimeWorkedHours)), 'UPDATE', 'NOCHANGE')) AS v_ChangedFlag,
	v_ChangedFlag AS ChangedFlag,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	-- *INF*: iif(v_ChangedFlag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_ChangedFlag = 'NEW', to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), sysdate) AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemId,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	EXP_Lkp_Defualt.o_NurseAssignmentAkId AS NurseAssignmentAkId,
	EXP_Src_Values.o_time_worked_seq AS TimeWorkedSequence,
	EXP_Src_Values.o_date_worked AS WorkedDate,
	EXP_Src_Values.o_time_worked_hours AS TimeWorkedHours
	FROM EXP_Lkp_Defualt
	 -- Manually join with EXP_Src_Values
	LEFT JOIN LKP_NurseAssignmentTimeWorked
	ON LKP_NurseAssignmentTimeWorked.NurseAssignmentAkId = EXP_Lkp_Defualt.o_NurseAssignmentAkId AND LKP_NurseAssignmentTimeWorked.TimeWorkedSequence = EXP_Src_Values.o_time_worked_seq
),
FIL_Lkp_Target AS (
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
	TimeWorkedSequence, 
	WorkedDate, 
	TimeWorkedHours, 
	Lkp_NurseAssignmentTimeWorkedAkId
	FROM EXP_TargetLkp_Detect_Changes
	WHERE ChangedFlag = 'NEW' or ChangedFlag = 'UPDATE'
),
SEQ_NurseAssignmentTimeWorked AS (
	CREATE SEQUENCE SEQ_NurseAssignmentTimeWorked
	START = 0
	INCREMENT = 1;
),
EXP_Akid_Insert_Target AS (
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
	TimeWorkedSequence,
	WorkedDate,
	TimeWorkedHours,
	-- *INF*: iif(ChangedFlag = 'NEW', NEXTVAL, Lkp_NurseAssignmentTimeWorkedAkId)
	IFF(ChangedFlag = 'NEW', NEXTVAL, Lkp_NurseAssignmentTimeWorkedAkId) AS NurseAssignmentTimeWorkedAkId,
	Lkp_NurseAssignmentTimeWorkedAkId,
	SEQ_NurseAssignmentTimeWorked.NEXTVAL
	FROM FIL_Lkp_Target
),
NurseAssignmentTimeWorked_Insert AS (
	INSERT INTO NurseAssignmentTimeWorked
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, NurseAssignmentTimeWorkedAkId, NurseAssignmentAkId, TimeWorkedSequence, WorkedDate, TimeWorkedHours)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	NURSEASSIGNMENTTIMEWORKEDAKID, 
	NURSEASSIGNMENTAKID, 
	TIMEWORKEDSEQUENCE, 
	WORKEDDATE, 
	TIMEWORKEDHOURS
	FROM EXP_Akid_Insert_Target
),
SQ_NurseAssignmentTimeWorked AS (
	SELECT
	A.NurseAssignmentTimeWorkedId,
	A.EffectiveDate,
	A.ExpirationDate, 
	A.NurseAssignmentTimeWorkedAkId 
	
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.NurseAssignmentTimeWorked A
	
	Where Exists 
	  ( 
	SELECT 1
	FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.NurseAssignmentTimeWorked B
	where
	B.CurrentSnapshotFlag =1
	AND
	A.NurseAssignmentTimeWorkedAkId= B.NurseAssignmentTimeWorkedAkId
	
	group by 
	B.NurseAssignmentTimeWorkedAkId
	
	having 
	count(*) > 1 
	   )
	
	AND
	A.CurrentSnapshotFlag = 1
	
	order by 
	A.NurseAssignmentTimeWorkedAkId, 
	A.EffectiveDate DESC
),
EXP_Lag_ExpirationDate AS (
	SELECT
	NurseAssignmentTimeWorkedId,
	0 AS CurrentSnapshotFlag,
	EffectiveDate,
	ExpirationDate AS orig_ExpirationDate,
	-- *INF*: DECODE(TRUE,
	-- 	NurseAssignmentTimeWorkedAkId= v_PREV_ROW_NurseAssignmentTimeWorkedAkId, ADD_TO_DATE(v_PREV_ROW_EffectiveDate,'SS',-1),
	-- 	orig_ExpirationDate)
	-- 
	DECODE(TRUE,
	NurseAssignmentTimeWorkedAkId = v_PREV_ROW_NurseAssignmentTimeWorkedAkId, ADD_TO_DATE(v_PREV_ROW_EffectiveDate, 'SS', - 1),
	orig_ExpirationDate) AS v_ExpirationDate,
	v_ExpirationDate AS ExpirationDate,
	EffectiveDate AS v_PREV_ROW_EffectiveDate,
	NurseAssignmentTimeWorkedAkId AS v_PREV_ROW_NurseAssignmentTimeWorkedAkId,
	NurseAssignmentTimeWorkedAkId,
	sysdate AS ModifiedDate
	FROM SQ_NurseAssignmentTimeWorked
),
FIL_FirstRowAkId AS (
	SELECT
	NurseAssignmentTimeWorkedId, 
	CurrentSnapshotFlag, 
	orig_ExpirationDate, 
	ExpirationDate, 
	ModifiedDate
	FROM EXP_Lag_ExpirationDate
	WHERE TRUE
),
UPD_NurseAssignmentTimeWorked AS (
	SELECT
	NurseAssignmentTimeWorkedId, 
	CurrentSnapshotFlag, 
	ExpirationDate, 
	ModifiedDate
	FROM FIL_FirstRowAkId
),
NurseAssignmentTimeWorked_Update AS (
	MERGE INTO NurseAssignmentTimeWorked AS T
	USING UPD_NurseAssignmentTimeWorked AS S
	ON T.NurseAssignmentTimeWorkedId = S.NurseAssignmentTimeWorkedId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),
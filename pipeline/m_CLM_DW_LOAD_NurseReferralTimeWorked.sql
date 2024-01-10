WITH
SQ_nurse_referral_time_worked_stage AS (
	SELECT
		NurseReferralTimeWorkedStageId,
		nurse_referral_id,
		time_worked_seq,
		date_worked,
		time_worked_hours,
		created_ts,
		created_user_id,
		modified_ts,
		modified_user_id,
		ExtractDate,
		SourceSystemId
	FROM nurse_referral_time_worked_stage
),
EXP_Src_Values AS (
	SELECT
	nurse_referral_id,
	-- *INF*: iif(isnull(ltrim(rtrim(nurse_referral_id))), -1, 
	--   nurse_referral_id)
	IFF(ltrim(rtrim(nurse_referral_id)) IS NULL, - 1, nurse_referral_id) AS o_nurse_referral_id,
	time_worked_seq,
	-- *INF*: iif(isnull(ltrim(rtrim(time_worked_seq))), -1, time_worked_seq)
	IFF(ltrim(rtrim(time_worked_seq)) IS NULL, - 1, time_worked_seq) AS o_time_worked_seq,
	date_worked,
	-- *INF*: iif(isnull(ltrim(rtrim(date_worked))), to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'), date_worked)
	IFF(ltrim(rtrim(date_worked)) IS NULL, to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), date_worked) AS o_date_worked,
	time_worked_hours,
	-- *INF*: iif(isnull(ltrim(rtrim(time_worked_hours))), 0, time_worked_hours)
	IFF(ltrim(rtrim(time_worked_hours)) IS NULL, 0, time_worked_hours) AS o_time_worked_hours
	FROM SQ_nurse_referral_time_worked_stage
),
LKP_NurseReferral AS (
	SELECT
	NurseReferralAkId,
	nurse_referral_id
	FROM (
		SELECT
		N.NurseReferralAkId as NurseReferralAkId, 
		N.nurse_referral_id as nurse_referral_id
		
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.NurseReferral N
		
		where
		N.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY nurse_referral_id ORDER BY NurseReferralAkId) = 1
),
EXP_Lkp_Default AS (
	SELECT
	NurseReferralAkId,
	-- *INF*: iif(isnull(NurseReferralAkId), -1, NurseReferralAkId)
	IFF(NurseReferralAkId IS NULL, - 1, NurseReferralAkId) AS o_NurseReferralAkId
	FROM LKP_NurseReferral
),
LKP_NurseReferralTimeWorked AS (
	SELECT
	NurseReferralTimeWorkedId,
	NurseReferralTimeWorkedAkId,
	NurseReferralAkId,
	TimeWorkedSequence,
	WorkedDate,
	TimeWorkedHours
	FROM (
		SELECT 
		N.NurseReferralTimeWorkedId as NurseReferralTimeWorkedId, 
		N.NurseReferralTimeWorkedAkId as NurseReferralTimeWorkedAkId, 
		N.TimeWorkedSequence as TimeWorkedSequence, 
		N.WorkedDate as WorkedDate, 
		N.TimeWorkedHours as TimeWorkedHours, 
		N.NurseReferralAkId as NurseReferralAkId
		
		 FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.NurseReferralTimeWorked N
		
		where
		N.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY NurseReferralAkId,TimeWorkedSequence ORDER BY NurseReferralTimeWorkedId) = 1
),
EXP_TargetLkp_Detect_Changes AS (
	SELECT
	LKP_NurseReferralTimeWorked.NurseReferralTimeWorkedId AS Lkp_NurseReferralTimeWorkedId,
	LKP_NurseReferralTimeWorked.NurseReferralTimeWorkedAkId AS Lkp_NurseReferralTimeWorkedAkId,
	LKP_NurseReferralTimeWorked.NurseReferralAkId AS Lkp_NurseReferralAkId,
	LKP_NurseReferralTimeWorked.TimeWorkedSequence AS Lkp_TimeWorkedSequence,
	LKP_NurseReferralTimeWorked.WorkedDate AS Lkp_WorkedDate,
	LKP_NurseReferralTimeWorked.TimeWorkedHours AS Lkp_TimeWorkedHours,
	-- *INF*: iif(isnull(Lkp_NurseReferralTimeWorkedId), 'NEW',
	-- 
	--   iif(  
	-- 
	--  ltrim(rtrim(Lkp_NurseReferralAkId)) != ltrim(rtrim(NurseReferralAkId))
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
	-- --   ltrim(rtrim(Lkp_NurseReferralTimeWorkedAkId)) != ltrim(rtrim(NurseReferralTimeWorkedAkId))
	-- 
	--    'UPDATE', 'NOCHANGE')
	--   
	--    )
	IFF(Lkp_NurseReferralTimeWorkedId IS NULL, 'NEW', IFF(ltrim(rtrim(Lkp_NurseReferralAkId)) != ltrim(rtrim(NurseReferralAkId)) OR ltrim(rtrim(Lkp_TimeWorkedSequence)) != ltrim(rtrim(TimeWorkedSequence)) OR ltrim(rtrim(Lkp_WorkedDate)) != ltrim(rtrim(WorkedDate)) OR ltrim(rtrim(Lkp_TimeWorkedHours)) != ltrim(rtrim(TimeWorkedHours)), 'UPDATE', 'NOCHANGE')) AS v_ChangedFlag,
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
	EXP_Lkp_Default.o_NurseReferralAkId AS NurseReferralAkId,
	EXP_Src_Values.o_time_worked_seq AS TimeWorkedSequence,
	EXP_Src_Values.o_date_worked AS WorkedDate,
	EXP_Src_Values.o_time_worked_hours AS TimeWorkedHours
	FROM EXP_Lkp_Default
	 -- Manually join with EXP_Src_Values
	LEFT JOIN LKP_NurseReferralTimeWorked
	ON LKP_NurseReferralTimeWorked.NurseReferralAkId = EXP_Lkp_Default.o_NurseReferralAkId AND LKP_NurseReferralTimeWorked.TimeWorkedSequence = EXP_Src_Values.o_time_worked_seq
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
	NurseReferralAkId, 
	TimeWorkedSequence, 
	WorkedDate, 
	TimeWorkedHours, 
	Lkp_NurseReferralTimeWorkedAkId
	FROM EXP_TargetLkp_Detect_Changes
	WHERE ChangedFlag = 'NEW' or ChangedFlag = 'UPDATE'
),
SEQ_NurseReferralTimeWorked AS (
	CREATE SEQUENCE SEQ_NurseReferralTimeWorked
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
	NurseReferralAkId,
	TimeWorkedSequence,
	WorkedDate,
	TimeWorkedHours,
	-- *INF*: iif(ChangedFlag = 'NEW', NEXTVAL, Lkp_NurseReferralTimeWorkedAkId)
	IFF(ChangedFlag = 'NEW', NEXTVAL, Lkp_NurseReferralTimeWorkedAkId) AS NurseReferralTimeWorkedAkId,
	Lkp_NurseReferralTimeWorkedAkId,
	SEQ_NurseReferralTimeWorked.NEXTVAL
	FROM FIL_Lkp_Target
),
NurseReferralTimeWorked_Insert AS (
	INSERT INTO NurseReferralTimeWorked
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, NurseReferralTimeWorkedAkId, NurseReferralAkId, TimeWorkedSequence, WorkedDate, TimeWorkedHours)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	NURSEREFERRALTIMEWORKEDAKID, 
	NURSEREFERRALAKID, 
	TIMEWORKEDSEQUENCE, 
	WORKEDDATE, 
	TIMEWORKEDHOURS
	FROM EXP_Akid_Insert_Target
),
SQ_NurseReferralTimeWorked AS (
	SELECT
	A.NurseReferralTimeWorkedId,
	A.EffectiveDate,
	A.ExpirationDate, 
	A.NurseReferralTimeWorkedAkId 
	
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.NurseReferralTimeWorked A
	
	Where Exists
	   ( 
	SELECT 1
	FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.NurseReferralTimeWorked B
	where
	B.CurrentSnapshotFlag =1
	AND
	A.NurseReferralTimeWorkedAkId = B.NurseReferralTimeWorkedAkId 
	
	group by 
	B.NurseReferralTimeWorkedAkId 
	
	having
	count(*) > 1
	     )
	
	AND
	A.CurrentSnapshotFlag = 1
	
	order by 
	A.NurseReferralTimeWorkedAkId , 
	A.EffectiveDate desc
),
EXP_Lag_ExpirationDate AS (
	SELECT
	NurseReferralTimeWorkedId,
	0 AS CurrentSnapshotFlag,
	EffectiveDate,
	ExpirationDate AS orig_ExpirationDate,
	-- *INF*: DECODE(TRUE,
	-- 	NurseReferralTimeWorkedAkId= v_PREV_ROW_NurseReferralTimeWorkedAkId, ADD_TO_DATE(v_PREV_ROW_EffectiveDate,'SS',-1),
	-- 	orig_ExpirationDate)
	-- 
	DECODE(TRUE,
		NurseReferralTimeWorkedAkId = v_PREV_ROW_NurseReferralTimeWorkedAkId, ADD_TO_DATE(v_PREV_ROW_EffectiveDate, 'SS', - 1),
		orig_ExpirationDate) AS v_ExpirationDate,
	v_ExpirationDate AS ExpirationDate,
	EffectiveDate AS v_PREV_ROW_EffectiveDate,
	NurseReferralTimeWorkedAkId AS v_PREV_ROW_NurseReferralTimeWorkedAkId,
	NurseReferralTimeWorkedAkId,
	sysdate AS ModifiedDate
	FROM SQ_NurseReferralTimeWorked
),
FIL_FirstRowAkId AS (
	SELECT
	NurseReferralTimeWorkedId, 
	CurrentSnapshotFlag, 
	orig_ExpirationDate, 
	ExpirationDate, 
	ModifiedDate
	FROM EXP_Lag_ExpirationDate
	WHERE TRUE
),
UPD_NurseReferralTimeWorked AS (
	SELECT
	NurseReferralTimeWorkedId, 
	CurrentSnapshotFlag, 
	ExpirationDate, 
	ModifiedDate
	FROM FIL_FirstRowAkId
),
NurseReferralTimeWorked_Update AS (
	MERGE INTO NurseReferralTimeWorked AS T
	USING UPD_NurseReferralTimeWorked AS S
	ON T.NurseReferralTimeWorkedId = S.NurseReferralTimeWorkedId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),
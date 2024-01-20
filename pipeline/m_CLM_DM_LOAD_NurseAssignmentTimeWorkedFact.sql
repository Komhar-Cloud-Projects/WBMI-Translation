WITH
Lkp_CALANDER_DIM AS (
	SELECT
	clndr_id,
	clndr_date
	FROM (
		SELECT 
			clndr_id,
			clndr_date
		FROM calendar_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY clndr_date ORDER BY clndr_id) = 1
),
SQ_NurseAssignmentTimeWorked AS (
	SELECT 
	N.NurseAssignmentTimeWorkedId, 
	N.NurseAssignmentTimeWorkedAkId, 
	N.NurseAssignmentAkId,
	N.TimeWorkedSequence, 
	N.WorkedDate, 
	N.TimeWorkedHours 
	
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseAssignmentTimeWorked N
	
	where
	N.CurrentSnapshotFlag = 1
),
EXP_Scr_values_Default AS (
	SELECT
	NurseAssignmentTimeWorkedId,
	NurseAssignmentTimeWorkedAkId,
	NurseAssignmentAkId,
	TimeWorkedSequence AS IN_TimeWorkedSequence,
	-- *INF*: iif(isnull(IN_TimeWorkedSequence),-1,IN_TimeWorkedSequence)
	IFF(IN_TimeWorkedSequence IS NULL, - 1, IN_TimeWorkedSequence) AS v_TimeWorkedSequence,
	v_TimeWorkedSequence AS TimeWorkedSequence,
	WorkedDate AS IN_WorkedDate,
	-- *INF*: :LKP.LKP_CALANDER_DIM(to_date(to_char(IN_WorkedDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALANDER_DIM_to_date_to_char_IN_WorkedDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_WorkedDate,
	-- *INF*: iif(isnull(v_WorkedDate),-1,v_WorkedDate)
	IFF(v_WorkedDate IS NULL, - 1, v_WorkedDate) AS TimeWorkedEnteredDateId,
	TimeWorkedHours AS IN_TimeWorkedHours,
	-- *INF*: iif(isnull(IN_TimeWorkedHours), 0,IN_TimeWorkedHours) 
	IFF(IN_TimeWorkedHours IS NULL, 0, IN_TimeWorkedHours) AS v_TimeWorkedHours,
	v_TimeWorkedHours AS TimeWorkedHours
	FROM SQ_NurseAssignmentTimeWorked
	LEFT JOIN LKP_CALANDER_DIM LKP_CALANDER_DIM_to_date_to_char_IN_WorkedDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALANDER_DIM_to_date_to_char_IN_WorkedDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(IN_WorkedDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

),
LKP_NurseAssignment AS (
	SELECT
	NurseAssignmentId,
	NurseAssignmentAkId,
	NurseCaseAkId
	FROM (
		SELECT
		N.NurseAssignmentId as NurseAssignmentId, 
		N.NurseCaseAkId as NurseCaseAkId, 
		N.claim_party_ak_id as claim_party_ak_id, 
		N.NurseAssignmentAkId as NurseAssignmentAkId
		
		 FROM
		 @{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseAssignment N
		
		where
		N.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY NurseAssignmentAkId ORDER BY NurseAssignmentId) = 1
),
EXP_Lkp_Values AS (
	SELECT
	NurseAssignmentId,
	NurseAssignmentAkId,
	NurseCaseAkId
	FROM LKP_NurseAssignment
),
LKP_NurseAssignmentDim AS (
	SELECT
	NurseAssignmentDimId,
	EdwNurseAssignmentAkId
	FROM (
		SELECT 
		N.NurseAssignmentDimId as NurseAssignmentDimId,
		N.EdwNurseAssignmentPkId as EdwNurseAssignmentPkId, 
		N.EdwNurseAssignmentAkId as EdwNurseAssignmentAkId
		
		 FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.NurseAssignmentDim N
		
		where
		N.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EdwNurseAssignmentAkId ORDER BY NurseAssignmentDimId) = 1
),
mplt_Claimant_Occurrence_dim_ids AS (WITH
	INPUT AS (
		
	),
	LKP_NurseCase_EDW_Scr AS (
		SELECT
		claim_party_occurrence_ak_id,
		NurseCaseAkId
		FROM (
			SELECT 
			N.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, 
			N.NurseCaseAkId as NurseCaseAkId
			
			 FROM 
			@{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseCase N
			
			where
			CurrentSnapshotFlag = 1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY NurseCaseAkId ORDER BY claim_party_occurrence_ak_id) = 1
	),
	LKP_claimant_dim_DM_Tgt AS (
		SELECT
		claimant_dim_id,
		edw_claim_party_occurrence_ak_id
		FROM (
			SELECT 
			C.claimant_dim_id as claimant_dim_id, C.edw_claim_party_occurrence_ak_id as edw_claim_party_occurrence_ak_id
			
			 FROM
			 @{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_dim C
			
			where
			crrnt_snpsht_flag = 1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_party_occurrence_ak_id ORDER BY claimant_dim_id) = 1
	),
	LKP_claim_party_occurrence_EDW_Scr AS (
		SELECT
		claim_occurrence_ak_id,
		claim_party_occurrence_ak_id
		FROM (
			SELECT 
			C.claim_occurrence_ak_id as claim_occurrence_ak_id, C.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id
			
			 FROM 
			@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence C
			
			where
			crrnt_snpsht_flag = 1
			AND
			claim_party_role_code in ('CLMT','CMT')
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id ORDER BY claim_occurrence_ak_id) = 1
	),
	LKP_claim_occurrence_dim_DM_Tgt AS (
		SELECT
		claim_occurrence_dim_id,
		edw_claim_occurrence_ak_id
		FROM (
			SELECT
			C.claim_occurrence_dim_id as claim_occurrence_dim_id, C.edw_claim_occurrence_ak_id as edw_claim_occurrence_ak_id 
			
			FROM 
			@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence_dim C
			
			where
			crrnt_snpsht_flag = 1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_occurrence_ak_id ORDER BY claim_occurrence_dim_id) = 1
	),
	OUTPUT AS (
		SELECT
		LKP_claimant_dim_DM_Tgt.claimant_dim_id, 
		LKP_claim_occurrence_dim_DM_Tgt.claim_occurrence_dim_id
		FROM 
		LEFT JOIN LKP_claim_occurrence_dim_DM_Tgt
		ON LKP_claim_occurrence_dim_DM_Tgt.edw_claim_occurrence_ak_id = LKP_claim_party_occurrence_EDW_Scr.claim_occurrence_ak_id
		LEFT JOIN LKP_claimant_dim_DM_Tgt
		ON LKP_claimant_dim_DM_Tgt.edw_claim_party_occurrence_ak_id = LKP_NurseCase_EDW_Scr.claim_party_occurrence_ak_id
	),
),
EXP_Defualt_Values AS (
	SELECT
	LKP_NurseAssignmentDim.NurseAssignmentDimId AS IN_NurseAssignmentDimId,
	-- *INF*: iif(isnull(IN_NurseAssignmentDimId), -1,IN_NurseAssignmentDimId)
	IFF(IN_NurseAssignmentDimId IS NULL, - 1, IN_NurseAssignmentDimId) AS v_NurseAssignmentDimId,
	v_NurseAssignmentDimId AS NurseAssignmentDimId,
	mplt_Claimant_Occurrence_dim_ids.claim_occurrence_dim_id AS IN_claim_occurrence_dim_id,
	-- *INF*: iif(isnull(IN_claim_occurrence_dim_id), -1,IN_claim_occurrence_dim_id)
	IFF(IN_claim_occurrence_dim_id IS NULL, - 1, IN_claim_occurrence_dim_id) AS v_claim_occurrence_dim_id,
	v_claim_occurrence_dim_id AS claim_occurrence_dim_id,
	mplt_Claimant_Occurrence_dim_ids.claimant_dim_id AS IN_claimant_dim_id,
	-- *INF*: iif(isnull(IN_claimant_dim_id),-1,IN_claimant_dim_id)
	IFF(IN_claimant_dim_id IS NULL, - 1, IN_claimant_dim_id) AS v_claimant_dim_id,
	v_claimant_dim_id AS claimant_dim_id,
	EXP_Scr_values_Default.NurseAssignmentTimeWorkedId,
	EXP_Scr_values_Default.TimeWorkedSequence,
	EXP_Scr_values_Default.TimeWorkedEnteredDateId,
	EXP_Scr_values_Default.TimeWorkedHours
	FROM EXP_Scr_values_Default
	 -- Manually join with mplt_Claimant_Occurrence_dim_ids
	LEFT JOIN LKP_NurseAssignmentDim
	ON LKP_NurseAssignmentDim.EdwNurseAssignmentAkId = EXP_Lkp_Values.NurseAssignmentAkId
),
LKP_NurseAssignmentTimeWorkedFact AS (
	SELECT
	NurseAssignmentTimeWorkedFactId,
	EdwNurseAssignmentTimeWorkedPkId,
	claimant_dim_id,
	claim_occurrence_dim_id,
	NurseAssignmentDimId,
	TimeWorkedEnteredDateId,
	TimeWorkedSequence,
	TimeWorkedHours,
	IN_NurseAssignmentTimeWorkedPKId
	FROM (
		SELECT 
			NurseAssignmentTimeWorkedFactId,
			EdwNurseAssignmentTimeWorkedPkId,
			claimant_dim_id,
			claim_occurrence_dim_id,
			NurseAssignmentDimId,
			TimeWorkedEnteredDateId,
			TimeWorkedSequence,
			TimeWorkedHours,
			IN_NurseAssignmentTimeWorkedPKId
		FROM NurseAssignmentTimeWorkedFact
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EdwNurseAssignmentTimeWorkedPkId ORDER BY NurseAssignmentTimeWorkedFactId) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_NurseAssignmentTimeWorkedFact.NurseAssignmentTimeWorkedFactId AS Lkp_NurseAssignmentTimeWorkedFactId,
	LKP_NurseAssignmentTimeWorkedFact.EdwNurseAssignmentTimeWorkedPkId AS Lkp_EdwNurseAssignmentTimeWorkedPkId,
	LKP_NurseAssignmentTimeWorkedFact.claimant_dim_id AS Lkp_claimant_dim_id,
	LKP_NurseAssignmentTimeWorkedFact.claim_occurrence_dim_id AS Lkp_claim_occurrence_dim_id,
	LKP_NurseAssignmentTimeWorkedFact.NurseAssignmentDimId AS Lkp_NurseAssignmentDimId,
	LKP_NurseAssignmentTimeWorkedFact.TimeWorkedEnteredDateId AS Lkp_TimeWorkedEnteredDateId,
	LKP_NurseAssignmentTimeWorkedFact.TimeWorkedSequence AS Lkp_TimeWorkedSequence,
	LKP_NurseAssignmentTimeWorkedFact.TimeWorkedHours AS Lkp_TimeWorkedHours,
	-- *INF*: iif(isnull(Lkp_NurseAssignmentTimeWorkedFactId), 'NEW',
	-- 
	-- iif (
	-- 
	-- Lkp_EdwNurseAssignmentTimeWorkedPkId != NurseAssignmentTimeWorkedId
	--  OR 
	-- Lkp_claimant_dim_id != claimant_dim_id
	--  OR 
	-- Lkp_claim_occurrence_dim_id != claim_occurrence_dim_id
	--  OR 
	-- Lkp_NurseAssignmentDimId != NurseAssignmentDimId
	--  OR 
	-- Lkp_TimeWorkedEnteredDateId != TimeWorkedEnteredDateId
	--  OR 
	-- Lkp_TimeWorkedSequence != TimeWorkedSequence
	--  OR 
	-- Lkp_TimeWorkedHours != TimeWorkedHours,
	-- 
	--   'UPDATE','NOCHANGE' )
	-- 
	--   )       
	IFF(
	    Lkp_NurseAssignmentTimeWorkedFactId IS NULL, 'NEW',
	    IFF(
	        Lkp_EdwNurseAssignmentTimeWorkedPkId != NurseAssignmentTimeWorkedId
	        or Lkp_claimant_dim_id != claimant_dim_id
	        or Lkp_claim_occurrence_dim_id != claim_occurrence_dim_id
	        or Lkp_NurseAssignmentDimId != NurseAssignmentDimId
	        or Lkp_TimeWorkedEnteredDateId != TimeWorkedEnteredDateId
	        or Lkp_TimeWorkedSequence != TimeWorkedSequence
	        or Lkp_TimeWorkedHours != TimeWorkedHours,
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_ChangedFlag,
	v_ChangedFlag AS ChangedFlag,
	EXP_Defualt_Values.NurseAssignmentDimId,
	EXP_Defualt_Values.claim_occurrence_dim_id,
	EXP_Defualt_Values.claimant_dim_id,
	EXP_Defualt_Values.NurseAssignmentTimeWorkedId,
	EXP_Defualt_Values.TimeWorkedSequence,
	EXP_Defualt_Values.TimeWorkedEnteredDateId,
	EXP_Defualt_Values.TimeWorkedHours,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM EXP_Defualt_Values
	LEFT JOIN LKP_NurseAssignmentTimeWorkedFact
	ON LKP_NurseAssignmentTimeWorkedFact.EdwNurseAssignmentTimeWorkedPkId = EXP_Defualt_Values.NurseAssignmentTimeWorkedId
),
RTR_Insert_Update AS (
	SELECT
	Lkp_NurseAssignmentTimeWorkedFactId,
	ChangedFlag,
	NurseAssignmentDimId,
	claim_occurrence_dim_id,
	claimant_dim_id,
	NurseAssignmentTimeWorkedId,
	TimeWorkedSequence,
	TimeWorkedEnteredDateId,
	TimeWorkedHours,
	o_AuditId AS AuditId
	FROM EXP_Detect_Changes
),
RTR_Insert_Update_Insert AS (SELECT * FROM RTR_Insert_Update WHERE ChangedFlag = 'NEW'),
RTR_Insert_Update_DEFAULT1 AS (SELECT * FROM RTR_Insert_Update WHERE NOT ( (ChangedFlag = 'NEW') )),
UPD_Insert AS (
	SELECT
	NurseAssignmentDimId AS NurseAssignmentDimId1, 
	claim_occurrence_dim_id AS claim_occurrence_dim_id1, 
	claimant_dim_id AS claimant_dim_id1, 
	NurseAssignmentTimeWorkedId AS NurseAssignmentTimeWorkedId1, 
	TimeWorkedSequence AS TimeWorkedSequence1, 
	TimeWorkedEnteredDateId AS TimeWorkedEnteredDateId1, 
	TimeWorkedHours AS TimeWorkedHours1, 
	AuditId AS AuditId1
	FROM RTR_Insert_Update_Insert
),
NurseAssignmentTimeWorkedFact_Insert AS (
	TRUNCATE TABLE NurseAssignmentTimeWorkedFact;
	INSERT INTO NurseAssignmentTimeWorkedFact
	(AuditId, EdwNurseAssignmentTimeWorkedPkId, claimant_dim_id, claim_occurrence_dim_id, NurseAssignmentDimId, TimeWorkedEnteredDateId, TimeWorkedSequence, TimeWorkedHours)
	SELECT 
	AuditId1 AS AUDITID, 
	NurseAssignmentTimeWorkedId1 AS EDWNURSEASSIGNMENTTIMEWORKEDPKID, 
	claimant_dim_id1 AS CLAIMANT_DIM_ID, 
	claim_occurrence_dim_id1 AS CLAIM_OCCURRENCE_DIM_ID, 
	NurseAssignmentDimId1 AS NURSEASSIGNMENTDIMID, 
	TimeWorkedEnteredDateId1 AS TIMEWORKEDENTEREDDATEID, 
	TimeWorkedSequence1 AS TIMEWORKEDSEQUENCE, 
	TimeWorkedHours1 AS TIMEWORKEDHOURS
	FROM UPD_Insert
),
UPD_Update AS (
	SELECT
	Lkp_NurseAssignmentTimeWorkedFactId AS Lkp_NurseAssignmentTimeWorkedFactId2, 
	NurseAssignmentDimId AS NurseAssignmentDimId2, 
	claim_occurrence_dim_id AS claim_occurrence_dim_id2, 
	claimant_dim_id AS claimant_dim_id2, 
	NurseAssignmentTimeWorkedId AS NurseAssignmentTimeWorkedId2, 
	TimeWorkedSequence AS TimeWorkedSequence2, 
	TimeWorkedEnteredDateId AS TimeWorkedEnteredDateId2, 
	TimeWorkedHours AS TimeWorkedHours2, 
	AuditId AS AuditId2
	FROM RTR_Insert_Update_DEFAULT1
),
NurseAssignmentTimeWorkedFact_Update AS (
	MERGE INTO NurseAssignmentTimeWorkedFact AS T
	USING UPD_Update AS S
	ON T.NurseAssignmentTimeWorkedFactId = S.Lkp_NurseAssignmentTimeWorkedFactId2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AuditId2, T.EdwNurseAssignmentTimeWorkedPkId = S.NurseAssignmentTimeWorkedId2, T.claimant_dim_id = S.claimant_dim_id2, T.claim_occurrence_dim_id = S.claim_occurrence_dim_id2, T.NurseAssignmentDimId = S.NurseAssignmentDimId2, T.TimeWorkedEnteredDateId = S.TimeWorkedEnteredDateId2, T.TimeWorkedSequence = S.TimeWorkedSequence2, T.TimeWorkedHours = S.TimeWorkedHours2
),
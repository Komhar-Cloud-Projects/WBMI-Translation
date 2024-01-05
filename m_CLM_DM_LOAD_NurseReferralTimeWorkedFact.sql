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
SQ_NurseReferralTimeWorked AS (
	SELECT
	N.NurseReferralTimeWorkedId, 
	N.NurseReferralAkId,
	N.TimeWorkedSequence,
	N.WorkedDate,
	N.TimeWorkedHours 
	
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseReferralTimeWorked N
	
	where
	CurrentSnapshotFlag = 1
),
EXP_Scr_values AS (
	SELECT
	NurseReferralTimeWorkedId,
	NurseReferralAkId,
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
	FROM SQ_NurseReferralTimeWorked
	LEFT JOIN LKP_CALANDER_DIM LKP_CALANDER_DIM_to_date_to_char_IN_WorkedDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALANDER_DIM_to_date_to_char_IN_WorkedDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(IN_WorkedDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

),
LKP_NurseReferral_EDW_Scr AS (
	SELECT
	NurseReferralId,
	NurseReferralAkId,
	NurseCaseAkId
	FROM (
		SELECT
		N.NurseReferralId as NurseReferralId,
		N.NurseCaseAkId as NurseCaseAkId,
		N.claim_party_ak_id as claim_party_ak_id,
		N.ReferralDate as ReferralDate,
		N.NurseReferralAkId as NurseReferralAkId
		
		 FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseReferral N
		
		where
		CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY NurseReferralAkId ORDER BY NurseReferralId) = 1
),
EXP_NurseAssignment AS (
	SELECT
	NurseReferralId,
	NurseReferralAkId,
	NurseCaseAkId
	FROM LKP_NurseReferral_EDW_Scr
),
LKP_NurseReferralDim_DM_Tgt AS (
	SELECT
	NurseReferralDimId,
	EdwNurseReferralAkId
	FROM (
		SELECT
		N.NurseReferralDimId as NurseReferralDimId, 
		N.EdwNurseReferralPkId as EdwNurseReferralPkId, 
		N.EdwNurseReferralAkId as EdwNurseReferralAkId
		
		 FROM
		 @{pipeline().parameters.TARGET_TABLE_OWNER}.NurseReferralDim N
		
		where
		CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EdwNurseReferralAkId ORDER BY NurseReferralDimId) = 1
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
	LKP_NurseReferralDim_DM_Tgt.NurseReferralDimId AS IN_NurseReferralDimId,
	-- *INF*: iif(isnull(IN_NurseReferralDimId),-1,IN_NurseReferralDimId)
	IFF(IN_NurseReferralDimId IS NULL, - 1, IN_NurseReferralDimId) AS v_NurseReferralDimId,
	v_NurseReferralDimId AS NurseReferralDimId,
	mplt_Claimant_Occurrence_dim_ids.claim_occurrence_dim_id AS IN_claim_occurrence_dim_id,
	-- *INF*: iif(isnull(IN_claim_occurrence_dim_id), -1,IN_claim_occurrence_dim_id)
	IFF(IN_claim_occurrence_dim_id IS NULL, - 1, IN_claim_occurrence_dim_id) AS v_claim_occurrence_dim_id,
	v_claim_occurrence_dim_id AS claim_occurrence_dim_id,
	mplt_Claimant_Occurrence_dim_ids.claimant_dim_id AS IN_claimant_dim_id,
	-- *INF*: iif(isnull(IN_claimant_dim_id),-1,IN_claimant_dim_id)
	IFF(IN_claimant_dim_id IS NULL, - 1, IN_claimant_dim_id) AS v_claimant_dim_id,
	v_claimant_dim_id AS claimant_dim_id,
	EXP_Scr_values.NurseReferralTimeWorkedId,
	EXP_Scr_values.TimeWorkedSequence,
	EXP_Scr_values.TimeWorkedEnteredDateId,
	EXP_Scr_values.TimeWorkedHours
	FROM EXP_Scr_values
	 -- Manually join with mplt_Claimant_Occurrence_dim_ids
	LEFT JOIN LKP_NurseReferralDim_DM_Tgt
	ON LKP_NurseReferralDim_DM_Tgt.EdwNurseReferralAkId = EXP_NurseAssignment.NurseReferralAkId
),
LKP_NurseReferralTimeWorkedFact_Target AS (
	SELECT
	NurseReferralTimeWorkedFactId,
	EdwNurseReferralTimeWorkedPkId,
	claimant_dim_id,
	claim_occurrence_dim_id,
	NurseReferralDimId,
	TimeWorkedEnteredDateId,
	TimeWorkedSequence,
	TimeWorkedHours
	FROM (
		SELECT 
			NurseReferralTimeWorkedFactId,
			EdwNurseReferralTimeWorkedPkId,
			claimant_dim_id,
			claim_occurrence_dim_id,
			NurseReferralDimId,
			TimeWorkedEnteredDateId,
			TimeWorkedSequence,
			TimeWorkedHours
		FROM NurseReferralTimeWorkedFact
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EdwNurseReferralTimeWorkedPkId ORDER BY NurseReferralTimeWorkedFactId) = 1
),
EXP_Lkp_Detect_Changes AS (
	SELECT
	LKP_NurseReferralTimeWorkedFact_Target.NurseReferralTimeWorkedFactId AS Lkp_NurseReferralTimeWorkedFactId,
	LKP_NurseReferralTimeWorkedFact_Target.EdwNurseReferralTimeWorkedPkId AS Lkp_EdwNurseReferralTimeWorkedPkId,
	LKP_NurseReferralTimeWorkedFact_Target.claimant_dim_id AS Lkp_claimant_dim_id,
	LKP_NurseReferralTimeWorkedFact_Target.claim_occurrence_dim_id AS Lkp_claim_occurrence_dim_id,
	LKP_NurseReferralTimeWorkedFact_Target.NurseReferralDimId AS Lkp_NurseReferralDimId,
	LKP_NurseReferralTimeWorkedFact_Target.TimeWorkedEnteredDateId AS Lkp_TimeWorkedEnteredDateId,
	LKP_NurseReferralTimeWorkedFact_Target.TimeWorkedSequence AS Lkp_TimeWorkedSequence,
	LKP_NurseReferralTimeWorkedFact_Target.TimeWorkedHours AS Lkp_TimeWorkedHours,
	-- *INF*: iif(isnull(Lkp_NurseReferralTimeWorkedFactId), 'NEW',
	-- 
	-- iif (
	-- 
	-- Lkp_EdwNurseReferralTimeWorkedPkId != NurseReferralTimeWorkedId
	--  OR 
	-- Lkp_claimant_dim_id != claimant_dim_id
	--  OR 
	-- Lkp_claim_occurrence_dim_id != claim_occurrence_dim_id
	--  OR 
	-- Lkp_NurseReferralDimId != NurseReferralDimId
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
	IFF(Lkp_NurseReferralTimeWorkedFactId IS NULL, 'NEW', IFF(Lkp_EdwNurseReferralTimeWorkedPkId != NurseReferralTimeWorkedId OR Lkp_claimant_dim_id != claimant_dim_id OR Lkp_claim_occurrence_dim_id != claim_occurrence_dim_id OR Lkp_NurseReferralDimId != NurseReferralDimId OR Lkp_TimeWorkedEnteredDateId != TimeWorkedEnteredDateId OR Lkp_TimeWorkedSequence != TimeWorkedSequence OR Lkp_TimeWorkedHours != TimeWorkedHours, 'UPDATE', 'NOCHANGE')) AS v_ChangedFlag,
	v_ChangedFlag AS ChangedFlag,
	EXP_Defualt_Values.NurseReferralDimId,
	EXP_Defualt_Values.claim_occurrence_dim_id,
	EXP_Defualt_Values.claimant_dim_id,
	EXP_Defualt_Values.NurseReferralTimeWorkedId,
	EXP_Defualt_Values.TimeWorkedSequence,
	EXP_Defualt_Values.TimeWorkedEnteredDateId,
	EXP_Defualt_Values.TimeWorkedHours,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM EXP_Defualt_Values
	LEFT JOIN LKP_NurseReferralTimeWorkedFact_Target
	ON LKP_NurseReferralTimeWorkedFact_Target.EdwNurseReferralTimeWorkedPkId = EXP_Defualt_Values.NurseReferralTimeWorkedId
),
RTR_Target AS (
	SELECT
	Lkp_NurseReferralTimeWorkedFactId,
	ChangedFlag,
	NurseReferralDimId,
	claim_occurrence_dim_id,
	claimant_dim_id,
	NurseReferralTimeWorkedId,
	TimeWorkedSequence,
	TimeWorkedEnteredDateId,
	TimeWorkedHours,
	o_AuditId AS AuditId
	FROM EXP_Lkp_Detect_Changes
),
RTR_Target_Insert AS (SELECT * FROM RTR_Target WHERE ChangedFlag = 'NEW'),
RTR_Target_DEFAULT1 AS (SELECT * FROM RTR_Target WHERE NOT ( (ChangedFlag = 'NEW') )),
UPD_Insert AS (
	SELECT
	NurseReferralDimId AS NurseReferralDimId1, 
	claim_occurrence_dim_id AS claim_occurrence_dim_id1, 
	claimant_dim_id AS claimant_dim_id1, 
	NurseReferralTimeWorkedId AS NurseReferralTimeWorkedId1, 
	TimeWorkedSequence AS TimeWorkedSequence1, 
	TimeWorkedEnteredDateId AS TimeWorkedEnteredDateId1, 
	TimeWorkedHours AS TimeWorkedHours1, 
	AuditId AS AuditId1
	FROM RTR_Target_Insert
),
NurseReferralTimeWorkedFact_Insert AS (
	TRUNCATE TABLE NurseReferralTimeWorkedFact;
	INSERT INTO NurseReferralTimeWorkedFact
	(AuditId, EdwNurseReferralTimeWorkedPkId, claimant_dim_id, claim_occurrence_dim_id, NurseReferralDimId, TimeWorkedEnteredDateId, TimeWorkedSequence, TimeWorkedHours)
	SELECT 
	AuditId1 AS AUDITID, 
	NurseReferralTimeWorkedId1 AS EDWNURSEREFERRALTIMEWORKEDPKID, 
	claimant_dim_id1 AS CLAIMANT_DIM_ID, 
	claim_occurrence_dim_id1 AS CLAIM_OCCURRENCE_DIM_ID, 
	NurseReferralDimId1 AS NURSEREFERRALDIMID, 
	TimeWorkedEnteredDateId1 AS TIMEWORKEDENTEREDDATEID, 
	TimeWorkedSequence1 AS TIMEWORKEDSEQUENCE, 
	TimeWorkedHours1 AS TIMEWORKEDHOURS
	FROM UPD_Insert
),
UPD_Update AS (
	SELECT
	Lkp_NurseReferralTimeWorkedFactId AS Lkp_NurseReferralTimeWorkedFactId2, 
	NurseReferralDimId AS NurseReferralDimId2, 
	claim_occurrence_dim_id AS claim_occurrence_dim_id2, 
	claimant_dim_id AS claimant_dim_id2, 
	NurseReferralTimeWorkedId AS NurseReferralTimeWorkedId2, 
	TimeWorkedSequence AS TimeWorkedSequence2, 
	TimeWorkedEnteredDateId AS TimeWorkedEnteredDateId2, 
	TimeWorkedHours AS TimeWorkedHours2, 
	AuditId AS AuditId2
	FROM RTR_Target_DEFAULT1
),
NurseReferralTimeWorkedFact_Update AS (
	MERGE INTO NurseReferralTimeWorkedFact AS T
	USING UPD_Update AS S
	ON T.NurseReferralTimeWorkedFactId = S.Lkp_NurseReferralTimeWorkedFactId2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AuditId2, T.EdwNurseReferralTimeWorkedPkId = S.NurseReferralTimeWorkedId2, T.claimant_dim_id = S.claimant_dim_id2, T.claim_occurrence_dim_id = S.claim_occurrence_dim_id2, T.NurseReferralDimId = S.NurseReferralDimId2, T.TimeWorkedEnteredDateId = S.TimeWorkedEnteredDateId2, T.TimeWorkedSequence = S.TimeWorkedSequence2, T.TimeWorkedHours = S.TimeWorkedHours2
),
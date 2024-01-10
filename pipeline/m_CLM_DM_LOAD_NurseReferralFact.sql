WITH
SQ_NurseReferral AS (
	SELECT 
	N.NurseReferralId, 
	N.NurseReferralAkId, 
	N.NurseCaseAkId 
	
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseReferral N
	
	where
	N.CurrentSnapshotFlag = 1
),
EXP_Scr_Values AS (
	SELECT
	NurseReferralId,
	NurseReferralAkId,
	NurseCaseAkId
	FROM SQ_NurseReferral
),
LKP_NurseReferralDim AS (
	SELECT
	NurseReferralDimId,
	EdwNurseReferralAkId
	FROM (
		SELECT
		N.NurseReferralDimId as NurseReferralDimId, 
		N.EdwNurseReferralAkId as EdwNurseReferralAkId
		
		 FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.NurseReferralDim N
		
		where
		N.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EdwNurseReferralAkId ORDER BY NurseReferralDimId) = 1
),
LKP_NurseReferralTimeWorked AS (
	SELECT
	ReferralTotalTimeWorkedHours,
	NurseReferralAkId
	FROM (
		SELECT
		COUNT(*) as Total_Count,
		N.NurseReferralAkId as NurseReferralAkId,
		SUM(N.TimeWorkedHours) as ReferralTotalTimeWorkedHours  
		
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseReferralTimeWorked N
		
		where
		N.CurrentSnapshotFlag = 1
		
		Group by
		N.NurseReferralAkId
		
		having
		COUNT(*) >= 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY NurseReferralAkId ORDER BY ReferralTotalTimeWorkedHours) = 1
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
	EXP_Scr_Values.NurseReferralId,
	LKP_NurseReferralDim.NurseReferralDimId AS IN_NurseReferralDimId,
	-- *INF*: iif(isnull(IN_NurseReferralDimId),-1,IN_NurseReferralDimId)
	IFF(IN_NurseReferralDimId IS NULL,
		- 1,
		IN_NurseReferralDimId
	) AS v_NurseReferralDimId,
	v_NurseReferralDimId AS NurseReferralDimId,
	mplt_Claimant_Occurrence_dim_ids.claimant_dim_id AS IN_claimant_dim_id,
	-- *INF*: iif(isnull(IN_claimant_dim_id), -1,IN_claimant_dim_id)
	IFF(IN_claimant_dim_id IS NULL,
		- 1,
		IN_claimant_dim_id
	) AS v_claimant_dim_id,
	v_claimant_dim_id AS claimant_dim_id,
	mplt_Claimant_Occurrence_dim_ids.claim_occurrence_dim_id AS IN_claim_occurrence_dim_id,
	-- *INF*: iif(isnull(IN_claim_occurrence_dim_id) ,-1, IN_claim_occurrence_dim_id)
	IFF(IN_claim_occurrence_dim_id IS NULL,
		- 1,
		IN_claim_occurrence_dim_id
	) AS v_claim_occurrence_dim_id,
	v_claim_occurrence_dim_id AS claim_occurrence_dim_id,
	LKP_NurseReferralTimeWorked.ReferralTotalTimeWorkedHours AS IN_ReferralTotalTimeWorkedHours,
	-- *INF*: iif(isnull(IN_ReferralTotalTimeWorkedHours),0,IN_ReferralTotalTimeWorkedHours)
	IFF(IN_ReferralTotalTimeWorkedHours IS NULL,
		0,
		IN_ReferralTotalTimeWorkedHours
	) AS v_ReferralTotalTimeWorkedHours,
	v_ReferralTotalTimeWorkedHours AS ReferralTotalTimeWorkedHours
	FROM EXP_Scr_Values
	 -- Manually join with mplt_Claimant_Occurrence_dim_ids
	LEFT JOIN LKP_NurseReferralDim
	ON LKP_NurseReferralDim.EdwNurseReferralAkId = EXP_Scr_Values.NurseReferralAkId
	LEFT JOIN LKP_NurseReferralTimeWorked
	ON LKP_NurseReferralTimeWorked.NurseReferralAkId = EXP_Scr_Values.NurseReferralAkId
),
LKP_NurseReferralFact AS (
	SELECT
	NurseReferralFactId,
	EdwNurseReferralPkId,
	claimant_dim_id,
	claim_occurrence_dim_id,
	NurseReferralDimId,
	ReferralTotalTimeWorkedHours
	FROM (
		SELECT 
			NurseReferralFactId,
			EdwNurseReferralPkId,
			claimant_dim_id,
			claim_occurrence_dim_id,
			NurseReferralDimId,
			ReferralTotalTimeWorkedHours
		FROM NurseReferralFact
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EdwNurseReferralPkId ORDER BY NurseReferralFactId) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_NurseReferralFact.NurseReferralFactId AS Lkp_NurseReferralFactId,
	LKP_NurseReferralFact.EdwNurseReferralPkId AS Lkp_EdwNurseReferralPkId,
	LKP_NurseReferralFact.claimant_dim_id AS Lkp_claimant_dim_id,
	LKP_NurseReferralFact.claim_occurrence_dim_id AS Lkp_claim_occurrence_dim_id,
	LKP_NurseReferralFact.NurseReferralDimId AS Lkp_NurseReferralDimId,
	LKP_NurseReferralFact.ReferralTotalTimeWorkedHours AS Lkp_ReferralTotalTimeWorkedHours,
	-- *INF*: iif(isnull(Lkp_NurseReferralFactId),'NEW',
	-- 
	-- iif(
	-- 
	--   Lkp_EdwNurseReferralPkId != NurseReferralId
	--  OR 
	--   Lkp_claimant_dim_id != claimant_dim_id
	--  OR 
	--   Lkp_claim_occurrence_dim_id != claim_occurrence_dim_id
	--  OR 
	--   Lkp_NurseReferralDimId != NurseReferralDimId
	--   OR
	--   Lkp_ReferralTotalTimeWorkedHours != ReferralTotalTimeWorkedHours,
	-- 
	--   'UPDATE','NOCHANGE' )
	-- 
	--    )
	IFF(Lkp_NurseReferralFactId IS NULL,
		'NEW',
		IFF(Lkp_EdwNurseReferralPkId != NurseReferralId 
			OR Lkp_claimant_dim_id != claimant_dim_id 
			OR Lkp_claim_occurrence_dim_id != claim_occurrence_dim_id 
			OR Lkp_NurseReferralDimId != NurseReferralDimId 
			OR Lkp_ReferralTotalTimeWorkedHours != ReferralTotalTimeWorkedHours,
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_ChangedFlag,
	v_ChangedFlag AS ChangedFlag,
	EXP_Defualt_Values.NurseReferralId,
	EXP_Defualt_Values.NurseReferralDimId,
	EXP_Defualt_Values.claimant_dim_id,
	EXP_Defualt_Values.claim_occurrence_dim_id,
	EXP_Defualt_Values.ReferralTotalTimeWorkedHours,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM EXP_Defualt_Values
	LEFT JOIN LKP_NurseReferralFact
	ON LKP_NurseReferralFact.EdwNurseReferralPkId = EXP_Defualt_Values.NurseReferralId
),
RTR_Insert_Update AS (
	SELECT
	Lkp_NurseReferralFactId,
	ChangedFlag,
	NurseReferralId,
	NurseReferralDimId,
	claimant_dim_id,
	claim_occurrence_dim_id,
	ReferralTotalTimeWorkedHours,
	o_AuditId
	FROM EXP_Detect_Changes
),
RTR_Insert_Update_Insert AS (SELECT * FROM RTR_Insert_Update WHERE ChangedFlag = 'NEW'),
RTR_Insert_Update_DEFAULT1 AS (SELECT * FROM RTR_Insert_Update WHERE NOT ( (ChangedFlag = 'NEW') )),
UPD_Update AS (
	SELECT
	Lkp_NurseReferralFactId AS Lkp_NurseReferralFactId2, 
	NurseReferralId AS NurseReferralId2, 
	NurseReferralDimId AS NurseReferralDimId2, 
	claimant_dim_id AS claimant_dim_id2, 
	claim_occurrence_dim_id AS claim_occurrence_dim_id2, 
	ReferralTotalTimeWorkedHours, 
	o_AuditId AS o_AuditId2
	FROM RTR_Insert_Update_DEFAULT1
),
NurseReferralFact_Update AS (
	MERGE INTO NurseReferralFact AS T
	USING UPD_Update AS S
	ON T.NurseReferralFactId = S.Lkp_NurseReferralFactId2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.o_AuditId2, T.EdwNurseReferralPkId = S.NurseReferralId2, T.claimant_dim_id = S.claimant_dim_id2, T.claim_occurrence_dim_id = S.claim_occurrence_dim_id2, T.NurseReferralDimId = S.NurseReferralDimId2, T.ReferralTotalTimeWorkedHours = S.ReferralTotalTimeWorkedHours
),
UPD_Insert AS (
	SELECT
	NurseReferralId AS NurseReferralId1, 
	NurseReferralDimId AS NurseReferralDimId1, 
	claimant_dim_id AS claimant_dim_id1, 
	claim_occurrence_dim_id AS claim_occurrence_dim_id1, 
	ReferralTotalTimeWorkedHours, 
	o_AuditId AS o_AuditId1
	FROM RTR_Insert_Update_Insert
),
NurseReferralFact_Insert AS (
	TRUNCATE TABLE NurseReferralFact;
	INSERT INTO NurseReferralFact
	(AuditId, EdwNurseReferralPkId, claimant_dim_id, claim_occurrence_dim_id, NurseReferralDimId, ReferralTotalTimeWorkedHours)
	SELECT 
	o_AuditId1 AS AUDITID, 
	NurseReferralId1 AS EDWNURSEREFERRALPKID, 
	claimant_dim_id1 AS CLAIMANT_DIM_ID, 
	claim_occurrence_dim_id1 AS CLAIM_OCCURRENCE_DIM_ID, 
	NurseReferralDimId1 AS NURSEREFERRALDIMID, 
	REFERRALTOTALTIMEWORKEDHOURS
	FROM UPD_Insert
),
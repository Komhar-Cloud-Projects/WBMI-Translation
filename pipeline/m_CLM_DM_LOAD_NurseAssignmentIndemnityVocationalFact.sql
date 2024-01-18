WITH
SQ_NurseAssignmentImpact AS (
	SELECT
	A.NurseAssignmentImpactId, 
	A.NurseAssignmentAkId, 
	A.SavingsAmount, 
	B.NurseImpactId, 
	B.ImpactCategory 
	
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseAssignmentImpact A, @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupNurseImpact B
	
	WHERE
	 A.NurseImpactId = B.NurseImpactId
	AND
	A.CurrentSnapshotFlag = 1
	AND
	B.CurrentSnapshotFlag = 1
	AND
	B.ImpactType = 'I'
),
EXP_Src_Values AS (
	SELECT
	NurseAssignmentImpactId,
	NurseAssignmentAkId,
	SavingsAmount,
	NurseImpactId1,
	ImpactCategory
	FROM SQ_NurseAssignmentImpact
),
SRT_Src_Values AS (
	SELECT
	NurseAssignmentImpactId, 
	NurseAssignmentAkId, 
	SavingsAmount, 
	NurseImpactId1, 
	ImpactCategory
	FROM EXP_Src_Values
	ORDER BY NurseAssignmentAkId ASC, ImpactCategory ASC
),
AGG_Indemnity_SavingsAmount AS (
	SELECT
	NurseAssignmentImpactId,
	NurseAssignmentAkId,
	ImpactCategory,
	SavingsAmount,
	-- *INF*: sum(SavingsAmount)
	sum(SavingsAmount) AS IndemnityVocationalCategorySavings,
	NurseImpactId1
	FROM SRT_Src_Values
	GROUP BY NurseAssignmentAkId, ImpactCategory
),
LKP_NurseAssignment AS (
	SELECT
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
	QUALIFY ROW_NUMBER() OVER (PARTITION BY NurseAssignmentAkId ORDER BY NurseAssignmentAkId) = 1
),
LKP_NurseAssignmentDim AS (
	SELECT
	NurseAssignmentDimId,
	EdwNurseAssignmentAkId
	FROM (
		SELECT
		N.NurseAssignmentDimId as NurseAssignmentDimId, 
		N.EdwNurseAssignmentAkId as EdwNurseAssignmentAkId
		
		 FROM
		 @{pipeline().parameters.TARGET_TABLE_OWNER}.NurseAssignmentDim N
		
		where
		N.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EdwNurseAssignmentAkId ORDER BY NurseAssignmentDimId) = 1
),
LKP_NurseAssignmentImpactDim AS (
	SELECT
	NurseAssignmentImpactDimId,
	EdwSupNurseImpactPkId
	FROM (
		SELECT
		N.NurseAssignmentImpactDimId as NurseAssignmentImpactDimId,
		N.EdwSupNurseImpactPkId as EdwSupNurseImpactPkId, 
		N.ImpactType as ImpactType,
		N.ImpactCategory as ImpactCategory
		
		 FROM
		 @{pipeline().parameters.TARGET_TABLE_OWNER}.NurseAssignmentImpactDim N
		
		where
		N.ImpactType = 'I'
		AND
		N.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EdwSupNurseImpactPkId ORDER BY NurseAssignmentImpactDimId) = 1
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
EXP_Default_Values AS (
	SELECT
	mplt_Claimant_Occurrence_dim_ids.claimant_dim_id AS IN_claimant_dim_id,
	-- *INF*: iif(isnull(IN_claimant_dim_id),-1,IN_claimant_dim_id)
	IFF(IN_claimant_dim_id IS NULL, - 1, IN_claimant_dim_id) AS v_claimant_dim_id,
	v_claimant_dim_id AS claimant_dim_id,
	mplt_Claimant_Occurrence_dim_ids.claim_occurrence_dim_id AS IN_claim_occurrence_dim_id,
	-- *INF*: iif(isnull(IN_claim_occurrence_dim_id),-1,IN_claim_occurrence_dim_id)
	IFF(IN_claim_occurrence_dim_id IS NULL, - 1, IN_claim_occurrence_dim_id) AS v_claim_occurrence_dim_id,
	v_claim_occurrence_dim_id AS claim_occurrence_dim_id,
	AGG_Indemnity_SavingsAmount.NurseAssignmentImpactId,
	LKP_NurseAssignmentDim.NurseAssignmentDimId AS IN_NurseAssignmentDimId,
	-- *INF*: iif(isnull(IN_NurseAssignmentDimId),-1,IN_NurseAssignmentDimId)
	IFF(IN_NurseAssignmentDimId IS NULL, - 1, IN_NurseAssignmentDimId) AS v_NurseAssignmentDimId,
	v_NurseAssignmentDimId AS NurseAssignmentDimId,
	LKP_NurseAssignmentImpactDim.NurseAssignmentImpactDimId AS IN_NurseAssignmentImpactDimId,
	-- *INF*: iif(isnull(IN_NurseAssignmentImpactDimId),-1,IN_NurseAssignmentImpactDimId)
	IFF(IN_NurseAssignmentImpactDimId IS NULL, - 1, IN_NurseAssignmentImpactDimId) AS v_NurseAssignmentImpactDimId,
	v_NurseAssignmentImpactDimId AS NurseAssignmentImpactDimId,
	AGG_Indemnity_SavingsAmount.IndemnityVocationalCategorySavings AS IN_IndemnityVocationalCategorySavings,
	-- *INF*: iif(isnull(IN_IndemnityVocationalCategorySavings),0,IN_IndemnityVocationalCategorySavings)
	IFF(IN_IndemnityVocationalCategorySavings IS NULL, 0, IN_IndemnityVocationalCategorySavings) AS v_IndemnityVocationalCategorySavings,
	v_IndemnityVocationalCategorySavings AS IndemnityVocationalCategorySavings
	FROM AGG_Indemnity_SavingsAmount
	 -- Manually join with mplt_Claimant_Occurrence_dim_ids
	LEFT JOIN LKP_NurseAssignmentDim
	ON LKP_NurseAssignmentDim.EdwNurseAssignmentAkId = LKP_NurseAssignment.NurseAssignmentAkId
	LEFT JOIN LKP_NurseAssignmentImpactDim
	ON LKP_NurseAssignmentImpactDim.EdwSupNurseImpactPkId = AGG_Indemnity_SavingsAmount.NurseImpactId1
),
LKP_NurseAssignmentIndemnityVocationalFact AS (
	SELECT
	NurseAssignmentIndemnityVocationalFactId,
	EdwNurseAssignmentImpactPkId,
	claimant_dim_id,
	claim_occurrence_dim_id,
	NurseAssignmentDimId,
	NurseAssignmentImpactDimId,
	IndemnityVocationalCategorySavings
	FROM (
		SELECT 
			NurseAssignmentIndemnityVocationalFactId,
			EdwNurseAssignmentImpactPkId,
			claimant_dim_id,
			claim_occurrence_dim_id,
			NurseAssignmentDimId,
			NurseAssignmentImpactDimId,
			IndemnityVocationalCategorySavings
		FROM NurseAssignmentIndemnityVocationalFact
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EdwNurseAssignmentImpactPkId ORDER BY NurseAssignmentIndemnityVocationalFactId) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_NurseAssignmentIndemnityVocationalFact.NurseAssignmentIndemnityVocationalFactId AS Lkp_NurseAssignmentIndemnityVocationalFactId,
	LKP_NurseAssignmentIndemnityVocationalFact.EdwNurseAssignmentImpactPkId AS Lkp_EdwNurseAssignmentImpactPkId,
	LKP_NurseAssignmentIndemnityVocationalFact.claimant_dim_id AS Lkp_claimant_dim_id,
	LKP_NurseAssignmentIndemnityVocationalFact.claim_occurrence_dim_id AS Lkp_claim_occurrence_dim_id,
	LKP_NurseAssignmentIndemnityVocationalFact.NurseAssignmentDimId AS Lkp_NurseAssignmentDimId,
	LKP_NurseAssignmentIndemnityVocationalFact.NurseAssignmentImpactDimId AS Lkp_NurseAssignmentImpactDimId,
	LKP_NurseAssignmentIndemnityVocationalFact.IndemnityVocationalCategorySavings AS Lkp_IndemnityVocationalCategorySavings,
	-- *INF*: iif(isnull(Lkp_NurseAssignmentIndemnityVocationalFactId),'NEW',
	-- iif (
	-- 
	-- Lkp_claimant_dim_id <> claimant_dim_id
	-- or
	-- Lkp_claim_occurrence_dim_id <> claim_occurrence_dim_id
	-- or
	-- Lkp_EdwNurseAssignmentImpactPkId <> NurseAssignmentImpactId
	-- or
	-- Lkp_NurseAssignmentDimId <> NurseAssignmentDimId
	-- or
	-- Lkp_NurseAssignmentImpactDimId <> NurseAssignmentImpactDimId
	-- or 
	-- Lkp_IndemnityVocationalCategorySavings <> IndemnityVocationalCategorySavings,
	-- 
	--  'UPDATE','NOCHANGE' )
	-- 
	--   )
	IFF(
	    Lkp_NurseAssignmentIndemnityVocationalFactId IS NULL, 'NEW',
	    IFF(
	        Lkp_claimant_dim_id <> claimant_dim_id
	        or Lkp_claim_occurrence_dim_id <> claim_occurrence_dim_id
	        or Lkp_EdwNurseAssignmentImpactPkId <> NurseAssignmentImpactId
	        or Lkp_NurseAssignmentDimId <> NurseAssignmentDimId
	        or Lkp_NurseAssignmentImpactDimId <> NurseAssignmentImpactDimId
	        or Lkp_IndemnityVocationalCategorySavings <> IndemnityVocationalCategorySavings,
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_ChangedFlag,
	v_ChangedFlag AS ChangedFlag,
	EXP_Default_Values.claimant_dim_id,
	EXP_Default_Values.claim_occurrence_dim_id,
	EXP_Default_Values.NurseAssignmentImpactId,
	EXP_Default_Values.NurseAssignmentDimId,
	EXP_Default_Values.NurseAssignmentImpactDimId,
	EXP_Default_Values.IndemnityVocationalCategorySavings,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM EXP_Default_Values
	LEFT JOIN LKP_NurseAssignmentIndemnityVocationalFact
	ON LKP_NurseAssignmentIndemnityVocationalFact.EdwNurseAssignmentImpactPkId = EXP_Default_Values.NurseAssignmentImpactId
),
RTR_Insert_Update AS (
	SELECT
	ChangedFlag,
	Lkp_NurseAssignmentIndemnityVocationalFactId,
	claimant_dim_id,
	claim_occurrence_dim_id,
	NurseAssignmentImpactId,
	NurseAssignmentDimId,
	NurseAssignmentImpactDimId,
	IndemnityVocationalCategorySavings,
	o_AuditId
	FROM EXP_Detect_Changes
),
RTR_Insert_Update_Insert AS (SELECT * FROM RTR_Insert_Update WHERE ChangedFlag = 'NEW'),
RTR_Insert_Update_DEFAULT1 AS (SELECT * FROM RTR_Insert_Update WHERE NOT ( (ChangedFlag = 'NEW') )),
UPD_Update AS (
	SELECT
	Lkp_NurseAssignmentIndemnityVocationalFactId AS Lkp_NurseAssignmentIndemnityVocationalFactId2, 
	claimant_dim_id AS claimant_dim_id2, 
	claim_occurrence_dim_id AS claim_occurrence_dim_id2, 
	NurseAssignmentImpactId AS NurseAssignmentImpactId2, 
	NurseAssignmentDimId AS NurseAssignmentDimId2, 
	NurseAssignmentImpactDimId AS NurseAssignmentImpactDimId2, 
	IndemnityVocationalCategorySavings, 
	o_AuditId AS o_AuditId2
	FROM RTR_Insert_Update_DEFAULT1
),
NurseAssignmentIndemnityVocationalFact_Update AS (
	MERGE INTO NurseAssignmentIndemnityVocationalFact AS T
	USING UPD_Update AS S
	ON T.NurseAssignmentIndemnityVocationalFactId = S.Lkp_NurseAssignmentIndemnityVocationalFactId2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.o_AuditId2, T.EdwNurseAssignmentImpactPkId = S.NurseAssignmentImpactId2, T.claimant_dim_id = S.claimant_dim_id2, T.claim_occurrence_dim_id = S.claim_occurrence_dim_id2, T.NurseAssignmentDimId = S.NurseAssignmentDimId2, T.NurseAssignmentImpactDimId = S.NurseAssignmentImpactDimId2, T.IndemnityVocationalCategorySavings = S.IndemnityVocationalCategorySavings
),
UPD_Insert AS (
	SELECT
	claimant_dim_id AS claimant_dim_id1, 
	claim_occurrence_dim_id AS claim_occurrence_dim_id1, 
	NurseAssignmentImpactId AS NurseAssignmentImpactId1, 
	NurseAssignmentDimId AS NurseAssignmentDimId1, 
	NurseAssignmentImpactDimId AS NurseAssignmentImpactDimId1, 
	IndemnityVocationalCategorySavings, 
	o_AuditId AS o_AuditId1
	FROM RTR_Insert_Update_Insert
),
NurseAssignmentIndemnityVocationalFact_Insert AS (
	TRUNCATE TABLE NurseAssignmentIndemnityVocationalFact;
	INSERT INTO NurseAssignmentIndemnityVocationalFact
	(AuditId, EdwNurseAssignmentImpactPkId, claimant_dim_id, claim_occurrence_dim_id, NurseAssignmentDimId, NurseAssignmentImpactDimId, IndemnityVocationalCategorySavings)
	SELECT 
	o_AuditId1 AS AUDITID, 
	NurseAssignmentImpactId1 AS EDWNURSEASSIGNMENTIMPACTPKID, 
	claimant_dim_id1 AS CLAIMANT_DIM_ID, 
	claim_occurrence_dim_id1 AS CLAIM_OCCURRENCE_DIM_ID, 
	NurseAssignmentDimId1 AS NURSEASSIGNMENTDIMID, 
	NurseAssignmentImpactDimId1 AS NURSEASSIGNMENTIMPACTDIMID, 
	INDEMNITYVOCATIONALCATEGORYSAVINGS
	FROM UPD_Insert
),
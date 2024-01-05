WITH
SQ_NurseCase AS (
	SELECT 
	N.NurseCaseId, 
	N.NurseCaseAkId, 
	N.claim_party_occurrence_ak_id, 
	N.EstimatedSavingsAmount 
	
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseCase N
	
	where
	N.CurrentSnapshotFlag = 1
),
EXP_Scr_Values AS (
	SELECT
	NurseCaseId,
	NurseCaseAkId,
	claim_party_occurrence_ak_id,
	EstimatedSavingsAmount
	FROM SQ_NurseCase
),
LKP_NurseAssignment AS (
	SELECT
	TotalAssignmentTimeWorked,
	NurseCaseAkId
	FROM (
		SELECT 
		COUNT(*) as Total_Count,
		NC.NurseCaseAkId as NurseCaseAkId,
		SUM(NAT.TimeWorkedHours) as TotalAssignmentTimeWorked 
		
		
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseCase NC,
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseAssignment NA,
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseAssignmentTimeWorked NAT
		
		where
		NC.NurseCaseAkId = NA.NurseCaseAkId
		AND
		NA.NurseAssignmentAkId = NAT.NurseAssignmentAkId
		AND
		NC.CurrentSnapshotFlag = 1
		AND
		NA.CurrentSnapshotFlag = 1
		AND
		NAT.CurrentSnapshotFlag = 1
		
		Group by
		NC.NurseCaseAkId
		
		Having
		COUNT(*) >= 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY NurseCaseAkId ORDER BY TotalAssignmentTimeWorked) = 1
),
LKP_NurseReferral AS (
	SELECT
	TotalReferralTimeWorked,
	NurseCaseAkId
	FROM (
		SELECT 
		COUNT(*) as Total_Count,
		NC.NurseCaseAkId as NurseCaseAkId,
		SUM(NRT.TimeWorkedHours) as TotalReferralTimeWorked 
		
		
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseCase NC,
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseReferral NR,
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseReferralTimeWorked NRT
		
		where
		NC.NurseCaseAkId = NR.NurseCaseAkId
		AND
		NR.NurseReferralAkId = NRT.NurseReferralAkId
		AND
		NC.CurrentSnapshotFlag = 1
		AND
		NR.CurrentSnapshotFlag = 1
		AND
		NRT.CurrentSnapshotFlag = 1
		
		Group by
		NC.NurseCaseAkId
		
		Having
		COUNT(*) >= 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY NurseCaseAkId ORDER BY TotalReferralTimeWorked) = 1
),
LKP_claim_party_occurrence AS (
	SELECT
	claim_occurrence_ak_id,
	claim_party_occurrence_ak_id
	FROM (
		SELECT
		CPO.claim_occurrence_ak_id as claim_occurrence_ak_id, 
		CPO.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id
		
		 FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence CPO
		
		where
		CPO.crrnt_snpsht_flag = 1
		AND
		CPO.claim_party_role_code in ('CLMT','CMT')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id ORDER BY claim_occurrence_ak_id) = 1
),
LKP_claim_occurrence_dim AS (
	SELECT
	claim_occurrence_dim_id,
	edw_claim_occurrence_ak_id
	FROM (
		SELECT 
		C.claim_occurrence_dim_id as claim_occurrence_dim_id, 
		C.edw_claim_occurrence_ak_id as edw_claim_occurrence_ak_id
		
		 FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence_dim C
		
		where
		C.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_occurrence_ak_id ORDER BY claim_occurrence_dim_id) = 1
),
LKP_claimant_dim AS (
	SELECT
	claimant_dim_id,
	edw_claim_party_occurrence_ak_id
	FROM (
		SELECT 
		C.claimant_dim_id as claimant_dim_id, 
		C.edw_claim_party_occurrence_ak_id as edw_claim_party_occurrence_ak_id 
		
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_dim C
		
		where
		C.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_party_occurrence_ak_id ORDER BY claimant_dim_id) = 1
),
EXP_Default_Values AS (
	SELECT
	EXP_Scr_Values.NurseCaseId,
	LKP_claimant_dim.claimant_dim_id AS IN_claimant_dim_id,
	-- *INF*: iif(isnull(IN_claimant_dim_id),-1,IN_claimant_dim_id)
	IFF(IN_claimant_dim_id IS NULL, - 1, IN_claimant_dim_id) AS v_claimant_dim_id,
	v_claimant_dim_id AS claimant_dim_id,
	LKP_claim_occurrence_dim.claim_occurrence_dim_id AS IN_claim_occurrence_dim_id,
	-- *INF*: iif(isnull(IN_claim_occurrence_dim_id),-1,IN_claim_occurrence_dim_id)
	IFF(IN_claim_occurrence_dim_id IS NULL, - 1, IN_claim_occurrence_dim_id) AS v_claim_occurrence_dim_id,
	v_claim_occurrence_dim_id AS claim_occurrence_dim_id,
	EXP_Scr_Values.EstimatedSavingsAmount AS IN_EstimatedSavingsAmount,
	-- *INF*: iif( NOT ISNULL(IN_EstimatedSavingsAmount),IN_EstimatedSavingsAmount,0)
	IFF(NOT IN_EstimatedSavingsAmount IS NULL, IN_EstimatedSavingsAmount, 0) AS v_EstimatedSavingAmount,
	v_EstimatedSavingAmount AS EstimatedSavings,
	LKP_NurseAssignment.TotalAssignmentTimeWorked AS IN_TotalAssignmentTimeWorked,
	-- *INF*: iif(isnull(IN_TotalAssignmentTimeWorked),0,IN_TotalAssignmentTimeWorked)
	IFF(IN_TotalAssignmentTimeWorked IS NULL, 0, IN_TotalAssignmentTimeWorked) AS v_TotalAssignmentTimeWorked,
	v_TotalAssignmentTimeWorked AS TotalAssignmentTimeWorked,
	LKP_NurseReferral.TotalReferralTimeWorked AS IN_TotalReferralTimeWorked,
	-- *INF*: iif(isnull(IN_TotalReferralTimeWorked),0,IN_TotalReferralTimeWorked)
	IFF(IN_TotalReferralTimeWorked IS NULL, 0, IN_TotalReferralTimeWorked) AS v_TotalReferralTimeWorked,
	v_TotalReferralTimeWorked AS TotalReferralTimeWorked,
	v_TotalAssignmentTimeWorked + v_TotalReferralTimeWorked AS v_TotalTimeWorked,
	v_TotalTimeWorked AS TotalTimeWorked
	FROM EXP_Scr_Values
	LEFT JOIN LKP_NurseAssignment
	ON LKP_NurseAssignment.NurseCaseAkId = EXP_Scr_Values.NurseCaseAkId
	LEFT JOIN LKP_NurseReferral
	ON LKP_NurseReferral.NurseCaseAkId = EXP_Scr_Values.NurseCaseAkId
	LEFT JOIN LKP_claim_occurrence_dim
	ON LKP_claim_occurrence_dim.edw_claim_occurrence_ak_id = LKP_claim_party_occurrence.claim_occurrence_ak_id
	LEFT JOIN LKP_claimant_dim
	ON LKP_claimant_dim.edw_claim_party_occurrence_ak_id = EXP_Scr_Values.claim_party_occurrence_ak_id
),
LKP_NurseCaseFact AS (
	SELECT
	NurseCaseFactId,
	EdwNurseCasePkId,
	claimant_dim_id,
	claim_occurrence_dim_id,
	EstimatedSavings,
	TotalAssignmentTimeWorked,
	TotalReferralTimeWorked,
	TotalTimeWorked
	FROM (
		SELECT 
			NurseCaseFactId,
			EdwNurseCasePkId,
			claimant_dim_id,
			claim_occurrence_dim_id,
			EstimatedSavings,
			TotalAssignmentTimeWorked,
			TotalReferralTimeWorked,
			TotalTimeWorked
		FROM NurseCaseFact
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EdwNurseCasePkId ORDER BY NurseCaseFactId) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_NurseCaseFact.NurseCaseFactId AS Lkp_NurseCaseFactId,
	LKP_NurseCaseFact.EdwNurseCasePkId AS Lkp_EdwNurseCasePkId,
	LKP_NurseCaseFact.claimant_dim_id AS Lkp_claimant_dim_id,
	LKP_NurseCaseFact.claim_occurrence_dim_id AS Lkp_claim_occurrence_dim_id,
	LKP_NurseCaseFact.EstimatedSavings AS Lkp_EstimatedSavings,
	LKP_NurseCaseFact.TotalAssignmentTimeWorked AS Lkp_TotalAssignmentTimeWorked,
	LKP_NurseCaseFact.TotalReferralTimeWorked AS Lkp_TotalReferralTimeWorked,
	LKP_NurseCaseFact.TotalTimeWorked AS Lkp_TotalTimeWorked,
	-- *INF*: iif(isnull(Lkp_NurseCaseFactId),'NEW',
	-- 
	-- iif (
	-- 
	-- Lkp_EdwNurseCasePkId != NurseCaseId
	--  OR 
	-- Lkp_claimant_dim_id != claimant_dim_id
	--   OR 
	-- Lkp_claim_occurrence_dim_id != claim_occurrence_dim_id
	--  OR 
	-- Lkp_EstimatedSavings != EstimatedSavings
	--  OR 
	-- Lkp_TotalAssignmentTimeWorked != TotalAssignmentTimeWorked
	--  OR 
	-- Lkp_TotalReferralTimeWorked != TotalReferralTimeWorked
	--  OR 
	-- Lkp_TotalTimeWorked != TotalTimeWorked,
	-- 
	--   'UPDATE','NOCHANGE' )
	-- 
	--   )
	IFF(Lkp_NurseCaseFactId IS NULL, 'NEW', IFF(Lkp_EdwNurseCasePkId != NurseCaseId OR Lkp_claimant_dim_id != claimant_dim_id OR Lkp_claim_occurrence_dim_id != claim_occurrence_dim_id OR Lkp_EstimatedSavings != EstimatedSavings OR Lkp_TotalAssignmentTimeWorked != TotalAssignmentTimeWorked OR Lkp_TotalReferralTimeWorked != TotalReferralTimeWorked OR Lkp_TotalTimeWorked != TotalTimeWorked, 'UPDATE', 'NOCHANGE')) AS v_ChangedFlag,
	v_ChangedFlag AS ChangedFlag,
	EXP_Default_Values.NurseCaseId,
	EXP_Default_Values.claimant_dim_id,
	EXP_Default_Values.claim_occurrence_dim_id,
	EXP_Default_Values.EstimatedSavings,
	EXP_Default_Values.TotalAssignmentTimeWorked,
	EXP_Default_Values.TotalReferralTimeWorked,
	EXP_Default_Values.TotalTimeWorked,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM EXP_Default_Values
	LEFT JOIN LKP_NurseCaseFact
	ON LKP_NurseCaseFact.EdwNurseCasePkId = EXP_Default_Values.NurseCaseId
),
RTR_Insert_Update AS (
	SELECT
	Lkp_NurseCaseFactId,
	ChangedFlag,
	NurseCaseId,
	claimant_dim_id,
	claim_occurrence_dim_id,
	EstimatedSavings,
	TotalAssignmentTimeWorked,
	TotalReferralTimeWorked,
	TotalTimeWorked,
	o_AuditId AS AuditId
	FROM EXP_Detect_Changes
),
RTR_Insert_Update_Insert AS (SELECT * FROM RTR_Insert_Update WHERE ChangedFlag = 'NEW'),
RTR_Insert_Update_DEFAULT1 AS (SELECT * FROM RTR_Insert_Update WHERE NOT ( (ChangedFlag = 'NEW') )),
UPD_Update AS (
	SELECT
	Lkp_NurseCaseFactId AS Lkp_NurseCaseFactId2, 
	NurseCaseId AS NurseCaseId2, 
	claimant_dim_id AS claimant_dim_id2, 
	claim_occurrence_dim_id AS claim_occurrence_dim_id2, 
	EstimatedSavings, 
	TotalAssignmentTimeWorked, 
	TotalReferralTimeWorked, 
	TotalTimeWorked, 
	AuditId AS AuditId2
	FROM RTR_Insert_Update_DEFAULT1
),
NurseCaseFact_Update AS (
	MERGE INTO NurseCaseFact AS T
	USING UPD_Update AS S
	ON T.NurseCaseFactId = S.Lkp_NurseCaseFactId2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AuditId2, T.EdwNurseCasePkId = S.NurseCaseId2, T.claimant_dim_id = S.claimant_dim_id2, T.claim_occurrence_dim_id = S.claim_occurrence_dim_id2, T.EstimatedSavings = S.EstimatedSavings, T.TotalAssignmentTimeWorked = S.TotalAssignmentTimeWorked, T.TotalReferralTimeWorked = S.TotalReferralTimeWorked, T.TotalTimeWorked = S.TotalTimeWorked
),
UPD_Insert AS (
	SELECT
	NurseCaseId AS NurseCaseId1, 
	claimant_dim_id AS claimant_dim_id1, 
	claim_occurrence_dim_id AS claim_occurrence_dim_id1, 
	EstimatedSavings, 
	TotalAssignmentTimeWorked, 
	TotalReferralTimeWorked, 
	TotalTimeWorked, 
	AuditId AS AuditId1
	FROM RTR_Insert_Update_Insert
),
NurseCaseFact_Insert AS (
	TRUNCATE TABLE NurseCaseFact;
	INSERT INTO NurseCaseFact
	(AuditId, EdwNurseCasePkId, claimant_dim_id, claim_occurrence_dim_id, EstimatedSavings, TotalAssignmentTimeWorked, TotalReferralTimeWorked, TotalTimeWorked)
	SELECT 
	AuditId1 AS AUDITID, 
	NurseCaseId1 AS EDWNURSECASEPKID, 
	claimant_dim_id1 AS CLAIMANT_DIM_ID, 
	claim_occurrence_dim_id1 AS CLAIM_OCCURRENCE_DIM_ID, 
	ESTIMATEDSAVINGS, 
	TOTALASSIGNMENTTIMEWORKED, 
	TOTALREFERRALTIMEWORKED, 
	TOTALTIMEWORKED
	FROM UPD_Insert
),
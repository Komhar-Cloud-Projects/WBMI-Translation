WITH
SQ_NurseAssignment AS (
	SELECT 
	N.NurseAssignmentId, 
	N.NurseAssignmentAkId, 
	N.NurseCaseAkId, 
	N.claim_party_ak_id, 
	N.TimeSavedWeeks, 
	N.TimeSavedDays 
	
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseAssignment N
	
	where
	N.CurrentSnapshotFlag = 1
),
EXP_Scr_Values AS (
	SELECT
	NurseAssignmentId,
	NurseAssignmentAkId,
	NurseCaseAkId,
	claim_party_ak_id,
	TimeSavedWeeks,
	TimeSavedDays
	FROM SQ_NurseAssignment
),
LKP_NurseAssignmentDim AS (
	SELECT
	NurseAssignmentDimId,
	EdwNurseAssignmentAkId
	FROM (
		SELECT 
		N.NurseAssignmentDimId as NurseAssignmentDimId, N.EdwNurseAssignmentAkId as EdwNurseAssignmentAkId 
		
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.NurseAssignmentDim N
		
		where
		N.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EdwNurseAssignmentAkId ORDER BY NurseAssignmentDimId) = 1
),
LKP_NurseAssignmentImpact AS (
	SELECT
	TimeSavedWeeks,
	TimeSavedDays,
	NurseAssignmentAkId
	FROM (
		SELECT 
		distinct
		NAI.NurseAssignmentAkID as NurseAssignmentAkID,
		NA.TimeSavedWeeks as TimeSavedWeeks,
		NA.TimeSavedDays as TimeSavedDays
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.SupNurseImpact SI,
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseAssignmentImpact NAI,
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseAssignment NA
		
		where
		SI.NurseImpactId = NAI.NurseImpactId
		AND
		NAI.NurseAssignmentAkId = NA.NurseAssignmentAkID
		AND
		SI.ImpactType = 'I'
		AND
		SI.CurrentSnapshotFlag = 1
		AND
		NAI.CurrentSnapshotFlag = 1
		AND
		NA.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY NurseAssignmentAkId ORDER BY TimeSavedWeeks) = 1
),
LKP_NurseAssignmentImpact_Indemnity AS (
	SELECT
	TotalIndemnityVocationalCategorySavings,
	NurseAssignmentAkId
	FROM (
		SELECT
		COUNT(*) as Total_Count,
		NA.NurseAssignmentAkId as NurseAssignmentAkId,
		SUM(NI.SavingsAmount) as TotalIndemnityVocationalCategorySavings
		
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.SupNurseImpact SI,
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseAssignmentImpact NI,
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseAssignment NA
		
		where
		SI.NurseImpactId = NI.NurseImpactId
		AND
		NI.NurseAssignmentAkId = NA.NurseAssignmentAkId
		AND
		SI.ImpactType = 'I'
		AND
		SI.CurrentSnapshotFlag = 1
		AND
		NI.CurrentSnapshotFlag = 1
		AND
		NA.CurrentSnapshotFlag = 1
		
		Group by
		NA.NurseAssignmentAkId
		
		having
		COUNT(*) >= 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY NurseAssignmentAkId ORDER BY TotalIndemnityVocationalCategorySavings) = 1
),
LKP_NurseAssignmentImpact_Medical AS (
	SELECT
	TotalMedicalCostSavings,
	NurseAssignmentAkId
	FROM (
		SELECT
		COUNT(*) as Total_Count,
		NA.NurseAssignmentAkId as NurseAssignmentAkId,
		SUM(NI.SavingsAmount) as TotalMedicalCostSavings
		
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.SupNurseImpact SI,
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseAssignmentImpact NI,
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseAssignment NA
		
		where
		SI.NurseImpactId = NI.NurseImpactId
		AND
		NI.NurseAssignmentAkId = NA.NurseAssignmentAkId
		AND
		SI.ImpactType = 'M'
		AND
		SI.CurrentSnapshotFlag = 1
		AND
		NI.CurrentSnapshotFlag = 1
		AND
		NA.CurrentSnapshotFlag = 1
		
		Group by
		NA.NurseAssignmentAkId
		
		having
		COUNT(*) >= 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY NurseAssignmentAkId ORDER BY TotalMedicalCostSavings) = 1
),
LKP_NurseAssignmentTimeWorked AS (
	SELECT
	TotalAssignmentTimeWorked,
	NurseAssignmentAkId
	FROM (
		SELECT 
		COUNT(*) as Total_Count,
		N.NurseAssignmentAkId as NurseAssignmentAkId,
		SUM(N.TimeWorkedHours) as TotalAssignmentTimeWorked
		  
		FROM 
		 @{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseAssignmentTimeWorked N
		
		where
		N.CurrentSnapshotFlag = 1
		
		Group by
		NurseAssignmentAkId
		
		having
		COUNT(*) >= 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY NurseAssignmentAkId ORDER BY TotalAssignmentTimeWorked) = 1
),
LKP_NurseCase AS (
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
		N.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY NurseCaseAkId ORDER BY claim_party_occurrence_ak_id) = 1
),
LKP_workers_comp_claimant_detail AS (
	SELECT
	ttd_rate,
	dtd_rate,
	claim_party_occurrence_ak_id
	FROM (
		SELECT
		WC.ttd_rate as ttd_rate, 
		WC.dtd_rate as dtd_rate, 
		WC.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id
		
		 FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.workers_comp_claimant_detail WC
		
		where
		WC.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id ORDER BY ttd_rate) = 1
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
	EXP_Scr_Values.NurseAssignmentId,
	LKP_NurseAssignmentDim.NurseAssignmentDimId AS IN_NurseAssignmentDimId,
	-- *INF*: iif(isnull(IN_NurseAssignmentDimId),-1,IN_NurseAssignmentDimId)
	IFF(IN_NurseAssignmentDimId IS NULL, - 1, IN_NurseAssignmentDimId) AS v_NurseAssignmentDimId,
	v_NurseAssignmentDimId AS NurseAssignmentDimId,
	mplt_Claimant_Occurrence_dim_ids.claimant_dim_id AS IN_claimant_dim_id,
	-- *INF*: iif(isnull(IN_claimant_dim_id),-1,IN_claimant_dim_id)
	IFF(IN_claimant_dim_id IS NULL, - 1, IN_claimant_dim_id) AS v_claimant_dim_id,
	v_claimant_dim_id AS claimant_dim_id,
	mplt_Claimant_Occurrence_dim_ids.claim_occurrence_dim_id AS IN_claim_occurrence_dim_id,
	-- *INF*: iif(isnull(IN_claim_occurrence_dim_id),-1,IN_claim_occurrence_dim_id)
	IFF(IN_claim_occurrence_dim_id IS NULL, - 1, IN_claim_occurrence_dim_id) AS v_claim_occurrence_dim_id,
	v_claim_occurrence_dim_id AS claim_occurrence_dim_id,
	LKP_NurseAssignmentImpact.TimeSavedWeeks AS IN_TimeSavedWeeks,
	-- *INF*: iif(isnull(IN_TimeSavedWeeks),0,IN_TimeSavedWeeks)
	IFF(IN_TimeSavedWeeks IS NULL, 0, IN_TimeSavedWeeks) AS v_TimeSavedWeeks,
	v_TimeSavedWeeks AS TimeSavedWeeks,
	LKP_NurseAssignmentImpact.TimeSavedDays AS IN_TimeSavedDays,
	-- *INF*: iif(isnull(IN_TimeSavedDays),0,IN_TimeSavedDays)
	IFF(IN_TimeSavedDays IS NULL, 0, IN_TimeSavedDays) AS v_TimeSavedDays,
	v_TimeSavedDays AS TimeSavedDays,
	LKP_workers_comp_claimant_detail.ttd_rate AS IN_ttd_rate,
	-- *INF*: iif(isnull(IN_ttd_rate),0,IN_ttd_rate)
	IFF(IN_ttd_rate IS NULL, 0, IN_ttd_rate) AS v_ttd_rate,
	v_ttd_rate AS ttd_rate,
	LKP_workers_comp_claimant_detail.dtd_rate AS IN_dtd_rate,
	-- *INF*: iif(isnull(IN_dtd_rate),0,IN_dtd_rate)
	IFF(IN_dtd_rate IS NULL, 0, IN_dtd_rate) AS v_dtd_rate,
	v_dtd_rate AS dtd_rate,
	-- *INF*:  ( v_TimeSavedWeeks * v_ttd_rate )  +  ( v_TimeSavedDays * v_dtd_rate ) 
	(v_TimeSavedWeeks * v_ttd_rate) + (v_TimeSavedDays * v_dtd_rate) AS v_EarlyReturnToWorkSavings,
	v_EarlyReturnToWorkSavings AS EarlyReturnToWorkSavings,
	LKP_NurseAssignmentTimeWorked.TotalAssignmentTimeWorked AS IN_TotalAssignmentTimeWorked,
	-- *INF*: iif(isnull(IN_TotalAssignmentTimeWorked),0,IN_TotalAssignmentTimeWorked)
	IFF(IN_TotalAssignmentTimeWorked IS NULL, 0, IN_TotalAssignmentTimeWorked) AS v_TotalAssignmentTimeWorked,
	v_TotalAssignmentTimeWorked AS TotalAssignmentTimeWorked,
	LKP_NurseAssignmentImpact_Medical.TotalMedicalCostSavings AS IN_TotalMedicalCostSavings,
	-- *INF*: iif(isnull(IN_TotalMedicalCostSavings),0,IN_TotalMedicalCostSavings)
	IFF(IN_TotalMedicalCostSavings IS NULL, 0, IN_TotalMedicalCostSavings) AS v_TotalMedicalCostSavings,
	v_TotalMedicalCostSavings AS TotalMedicalCostSavings,
	LKP_NurseAssignmentImpact_Indemnity.TotalIndemnityVocationalCategorySavings AS IN_TotalIndemnityVocationalCategorySavings,
	-- *INF*: iif(isnull(IN_TotalIndemnityVocationalCategorySavings),0,IN_TotalIndemnityVocationalCategorySavings)
	IFF(
	    IN_TotalIndemnityVocationalCategorySavings IS NULL, 0,
	    IN_TotalIndemnityVocationalCategorySavings
	) AS v_TotalIndemnityVocationalCategorySavings,
	v_TotalIndemnityVocationalCategorySavings AS TotalIndemnityVocationalCategorySavings,
	v_EarlyReturnToWorkSavings + v_TotalIndemnityVocationalCategorySavings AS v_TotalIndemnityVocationalSavings,
	v_TotalIndemnityVocationalSavings AS TotalIndemnityVocationalSavings,
	v_TotalMedicalCostSavings + v_TotalIndemnityVocationalSavings AS v_TotalCostImpact,
	v_TotalCostImpact AS TotalCostImpact
	FROM EXP_Scr_Values
	 -- Manually join with mplt_Claimant_Occurrence_dim_ids
	LEFT JOIN LKP_NurseAssignmentDim
	ON LKP_NurseAssignmentDim.EdwNurseAssignmentAkId = EXP_Scr_Values.NurseAssignmentAkId
	LEFT JOIN LKP_NurseAssignmentImpact
	ON LKP_NurseAssignmentImpact.NurseAssignmentAkId = EXP_Scr_Values.NurseAssignmentAkId
	LEFT JOIN LKP_NurseAssignmentImpact_Indemnity
	ON LKP_NurseAssignmentImpact_Indemnity.NurseAssignmentAkId = EXP_Scr_Values.NurseAssignmentAkId
	LEFT JOIN LKP_NurseAssignmentImpact_Medical
	ON LKP_NurseAssignmentImpact_Medical.NurseAssignmentAkId = EXP_Scr_Values.NurseAssignmentAkId
	LEFT JOIN LKP_NurseAssignmentTimeWorked
	ON LKP_NurseAssignmentTimeWorked.NurseAssignmentAkId = EXP_Scr_Values.NurseAssignmentAkId
	LEFT JOIN LKP_workers_comp_claimant_detail
	ON LKP_workers_comp_claimant_detail.claim_party_occurrence_ak_id = LKP_NurseCase.claim_party_occurrence_ak_id
),
LKP_NurseAssignmentFact AS (
	SELECT
	NurseAssignmentFactId,
	EdwNurseAssignmentPkId,
	claimant_dim_id,
	claim_occurrence_dim_id,
	NurseAssignmentDimId,
	TimeSavedWeeks,
	TimeSavedDays,
	EarlyReturnToWorkSavings,
	TotalAssignmentTimeWorkedHours,
	TotalMedicalCostSavings,
	TotalIndemnityVocationalCategorySavings,
	TotalIndemnityVocationalSavings,
	TotalCostImpact
	FROM (
		SELECT 
			NurseAssignmentFactId,
			EdwNurseAssignmentPkId,
			claimant_dim_id,
			claim_occurrence_dim_id,
			NurseAssignmentDimId,
			TimeSavedWeeks,
			TimeSavedDays,
			EarlyReturnToWorkSavings,
			TotalAssignmentTimeWorkedHours,
			TotalMedicalCostSavings,
			TotalIndemnityVocationalCategorySavings,
			TotalIndemnityVocationalSavings,
			TotalCostImpact
		FROM NurseAssignmentFact
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EdwNurseAssignmentPkId ORDER BY NurseAssignmentFactId) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_NurseAssignmentFact.NurseAssignmentFactId AS Lkp_NurseAssignmentFactId,
	LKP_NurseAssignmentFact.EdwNurseAssignmentPkId AS Lkp_EdwNurseAssignmentPkId,
	LKP_NurseAssignmentFact.claimant_dim_id AS Lkp_claimant_dim_id,
	LKP_NurseAssignmentFact.claim_occurrence_dim_id AS Lkp_claim_occurrence_dim_id,
	LKP_NurseAssignmentFact.NurseAssignmentDimId AS Lkp_NurseAssignmentDimId,
	LKP_NurseAssignmentFact.TimeSavedWeeks AS Lkp_TimeSavedWeeks,
	LKP_NurseAssignmentFact.TimeSavedDays AS Lkp_TimeSavedDays,
	LKP_NurseAssignmentFact.EarlyReturnToWorkSavings AS Lkp_EarlyReturnToWorkSavings,
	LKP_NurseAssignmentFact.TotalAssignmentTimeWorkedHours AS Lkp_TotalAssignmentTimeWorkedHours,
	LKP_NurseAssignmentFact.TotalMedicalCostSavings AS Lkp_TotalMedicalCostSavings,
	LKP_NurseAssignmentFact.TotalIndemnityVocationalCategorySavings AS Lkp_TotalIndemnityVocationalCategorySavings,
	LKP_NurseAssignmentFact.TotalIndemnityVocationalSavings AS Lkp_TotalIndemnityVocationalSavings,
	LKP_NurseAssignmentFact.TotalCostImpact AS Lkp_TotalCostImpact,
	-- *INF*: iif(isnull(Lkp_NurseAssignmentFactId),'NEW',
	-- 
	-- iif (
	-- 
	-- Lkp_EdwNurseAssignmentPkId != NurseAssignmentId
	--  OR 
	-- Lkp_claimant_dim_id != claimant_dim_id
	--  OR 
	-- Lkp_claim_occurrence_dim_id != claim_occurrence_dim_id
	--  OR 
	-- Lkp_NurseAssignmentDimId != NurseAssignmentDimId
	--  OR 
	-- Lkp_TimeSavedWeeks != TimeSavedWeeks
	--  OR 
	-- Lkp_TimeSavedDays != TimeSavedDays
	--  OR 
	-- Lkp_EarlyReturnToWorkSavings != EarlyReturnToWorkSavings
	--  OR 
	-- Lkp_TotalAssignmentTimeWorkedHours != TotalAssignmentTimeWorked
	--  OR 
	-- Lkp_TotalMedicalCostSavings != TotalMedicalCostSavings
	--  OR 
	-- Lkp_TotalIndemnityVocationalCategorySavings != TotalIndemnityVocationalCategorySavings
	--  OR 
	-- Lkp_TotalIndemnityVocationalSavings != TotalIndemnityVocationalSavings
	--  OR 
	-- Lkp_TotalCostImpact != TotalCostImpact,
	-- 
	--  'UPDATE','NOCHANGE' )
	-- 
	--   )
	-- 
	-- 
	IFF(
	    Lkp_NurseAssignmentFactId IS NULL, 'NEW',
	    IFF(
	        Lkp_EdwNurseAssignmentPkId != NurseAssignmentId
	        or Lkp_claimant_dim_id != claimant_dim_id
	        or Lkp_claim_occurrence_dim_id != claim_occurrence_dim_id
	        or Lkp_NurseAssignmentDimId != NurseAssignmentDimId
	        or Lkp_TimeSavedWeeks != TimeSavedWeeks
	        or Lkp_TimeSavedDays != TimeSavedDays
	        or Lkp_EarlyReturnToWorkSavings != EarlyReturnToWorkSavings
	        or Lkp_TotalAssignmentTimeWorkedHours != TotalAssignmentTimeWorked
	        or Lkp_TotalMedicalCostSavings != TotalMedicalCostSavings
	        or Lkp_TotalIndemnityVocationalCategorySavings != TotalIndemnityVocationalCategorySavings
	        or Lkp_TotalIndemnityVocationalSavings != TotalIndemnityVocationalSavings
	        or Lkp_TotalCostImpact != TotalCostImpact,
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_ChangedFlag,
	v_ChangedFlag AS ChangedFlag,
	EXP_Default_Values.NurseAssignmentId,
	EXP_Default_Values.NurseAssignmentDimId,
	EXP_Default_Values.claimant_dim_id,
	EXP_Default_Values.claim_occurrence_dim_id,
	EXP_Default_Values.TimeSavedWeeks,
	EXP_Default_Values.TimeSavedDays,
	EXP_Default_Values.ttd_rate,
	EXP_Default_Values.dtd_rate,
	EXP_Default_Values.EarlyReturnToWorkSavings,
	EXP_Default_Values.TotalAssignmentTimeWorked,
	EXP_Default_Values.TotalMedicalCostSavings,
	EXP_Default_Values.TotalIndemnityVocationalCategorySavings,
	EXP_Default_Values.TotalIndemnityVocationalSavings,
	EXP_Default_Values.TotalCostImpact,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM EXP_Default_Values
	LEFT JOIN LKP_NurseAssignmentFact
	ON LKP_NurseAssignmentFact.EdwNurseAssignmentPkId = EXP_Default_Values.NurseAssignmentId
),
RTR_Insert_Update AS (
	SELECT
	Lkp_NurseAssignmentFactId,
	ChangedFlag,
	NurseAssignmentId,
	NurseAssignmentDimId,
	claimant_dim_id,
	claim_occurrence_dim_id,
	TimeSavedWeeks,
	TimeSavedDays,
	EarlyReturnToWorkSavings,
	TotalAssignmentTimeWorked,
	TotalMedicalCostSavings,
	TotalIndemnityVocationalCategorySavings,
	TotalIndemnityVocationalSavings,
	TotalCostImpact,
	o_AuditId
	FROM EXP_Detect_Changes
),
RTR_Insert_Update_Insert AS (SELECT * FROM RTR_Insert_Update WHERE ChangedFlag = 'NEW'),
RTR_Insert_Update_DEFAULT1 AS (SELECT * FROM RTR_Insert_Update WHERE NOT ( (ChangedFlag = 'NEW') )),
UPD_Insert AS (
	SELECT
	NurseAssignmentId AS NurseAssignmentId1, 
	NurseAssignmentDimId AS NurseAssignmentDimId1, 
	claimant_dim_id AS claimant_dim_id1, 
	claim_occurrence_dim_id AS claim_occurrence_dim_id1, 
	TimeSavedWeeks AS TimeSavedWeeks1, 
	TimeSavedDays AS TimeSavedDays1, 
	EarlyReturnToWorkSavings AS EarlyReturnToWorkSavings1, 
	TotalAssignmentTimeWorked AS TotalAssignmentTimeWorked1, 
	TotalMedicalCostSavings AS TotalMedicalCostSavings1, 
	TotalIndemnityVocationalCategorySavings AS TotalIndemnityVocationalCategorySavings1, 
	TotalIndemnityVocationalSavings AS TotalIndemnityVocationalSavings1, 
	TotalCostImpact AS TotalCostImpact1, 
	o_AuditId AS o_AuditId1
	FROM RTR_Insert_Update_Insert
),
NurseAssignmentFact_Insert AS (
	TRUNCATE TABLE NurseAssignmentFact;
	INSERT INTO NurseAssignmentFact
	(AuditId, EdwNurseAssignmentPkId, claimant_dim_id, claim_occurrence_dim_id, NurseAssignmentDimId, TimeSavedWeeks, TimeSavedDays, EarlyReturnToWorkSavings, TotalAssignmentTimeWorkedHours, TotalMedicalCostSavings, TotalIndemnityVocationalCategorySavings, TotalIndemnityVocationalSavings, TotalCostImpact)
	SELECT 
	o_AuditId1 AS AUDITID, 
	NurseAssignmentId1 AS EDWNURSEASSIGNMENTPKID, 
	claimant_dim_id1 AS CLAIMANT_DIM_ID, 
	claim_occurrence_dim_id1 AS CLAIM_OCCURRENCE_DIM_ID, 
	NurseAssignmentDimId1 AS NURSEASSIGNMENTDIMID, 
	TimeSavedWeeks1 AS TIMESAVEDWEEKS, 
	TimeSavedDays1 AS TIMESAVEDDAYS, 
	EarlyReturnToWorkSavings1 AS EARLYRETURNTOWORKSAVINGS, 
	TotalAssignmentTimeWorked1 AS TOTALASSIGNMENTTIMEWORKEDHOURS, 
	TotalMedicalCostSavings1 AS TOTALMEDICALCOSTSAVINGS, 
	TotalIndemnityVocationalCategorySavings1 AS TOTALINDEMNITYVOCATIONALCATEGORYSAVINGS, 
	TotalIndemnityVocationalSavings1 AS TOTALINDEMNITYVOCATIONALSAVINGS, 
	TotalCostImpact1 AS TOTALCOSTIMPACT
	FROM UPD_Insert
),
UPD_Update AS (
	SELECT
	Lkp_NurseAssignmentFactId AS Lkp_NurseAssignmentFactId2, 
	NurseAssignmentId AS NurseAssignmentId2, 
	NurseAssignmentDimId AS NurseAssignmentDimId2, 
	claimant_dim_id AS claimant_dim_id2, 
	claim_occurrence_dim_id AS claim_occurrence_dim_id2, 
	TimeSavedWeeks AS TimeSavedWeeks2, 
	TimeSavedDays AS TimeSavedDays2, 
	EarlyReturnToWorkSavings AS EarlyReturnToWorkSavings2, 
	TotalAssignmentTimeWorked AS TotalAssignmentTimeWorked2, 
	TotalMedicalCostSavings AS TotalMedicalCostSavings2, 
	TotalIndemnityVocationalCategorySavings AS TotalIndemnityVocationalCategorySavings2, 
	TotalIndemnityVocationalSavings AS TotalIndemnityVocationalSavings2, 
	TotalCostImpact AS TotalCostImpact2, 
	o_AuditId AS o_AuditId2
	FROM RTR_Insert_Update_DEFAULT1
),
NurseAssignmentFact_Update AS (
	MERGE INTO NurseAssignmentFact AS T
	USING UPD_Update AS S
	ON T.NurseAssignmentFactId = S.Lkp_NurseAssignmentFactId2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.o_AuditId2, T.EdwNurseAssignmentPkId = S.NurseAssignmentId2, T.claimant_dim_id = S.claimant_dim_id2, T.claim_occurrence_dim_id = S.claim_occurrence_dim_id2, T.NurseAssignmentDimId = S.NurseAssignmentDimId2, T.TimeSavedWeeks = S.TimeSavedWeeks2, T.TimeSavedDays = S.TimeSavedDays2, T.EarlyReturnToWorkSavings = S.EarlyReturnToWorkSavings2, T.TotalAssignmentTimeWorkedHours = S.TotalAssignmentTimeWorked2, T.TotalMedicalCostSavings = S.TotalMedicalCostSavings2, T.TotalIndemnityVocationalCategorySavings = S.TotalIndemnityVocationalCategorySavings2, T.TotalIndemnityVocationalSavings = S.TotalIndemnityVocationalSavings2, T.TotalCostImpact = S.TotalCostImpact2
),
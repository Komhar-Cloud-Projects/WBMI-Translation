WITH
SQ_clmt_nurse_manage_stage AS (
	SELECT
		ClmtNurseManageStageId,
		clmt_nurse_manage_id,
		tch_claim_nbr,
		tch_client_id,
		pms_policy_sym,
		pms_policy_num,
		pms_policy_mod,
		pms_date_of_loss,
		pms_loss_occurence,
		pms_loss_claimant,
		source_system_id,
		estimated_savings_amount,
		created_ts,
		created_user_id,
		modified_ts,
		modified_user_id,
		ExtractDate,
		SourceSystemId
	FROM clmt_nurse_manage_stage
),
EXP_Src_Values AS (
	SELECT
	ClmtNurseManageStageId,
	clmt_nurse_manage_id,
	-- *INF*: iif(isnull(ltrim(rtrim(clmt_nurse_manage_id))), -1, clmt_nurse_manage_id)
	IFF(ltrim(rtrim(clmt_nurse_manage_id)) IS NULL, - 1, clmt_nurse_manage_id) AS o_clmt_nurse_manage_id,
	tch_claim_nbr,
	tch_client_id,
	pms_policy_sym,
	pms_policy_num,
	pms_policy_mod,
	pms_date_of_loss,
	pms_loss_occurence,
	pms_loss_claimant,
	-- *INF*: to_char(pms_date_of_loss,'MMDDYYYY')
	to_char(pms_date_of_loss, 'MMDDYYYY') AS v_pms_date_of_loss,
	-- *INF*: IIF(length(tch_claim_nbr)>0 ,ltrim(rtrim(tch_claim_nbr)),
	-- 
	-- IIF(length(pms_policy_sym)>0,
	-- ltrim(rtrim(pms_policy_sym || pms_policy_num || pms_policy_mod || v_pms_date_of_loss ||  pms_loss_occurence)),
	-- 
	-- 'N/A'))
	IFF(
	    length(tch_claim_nbr) > 0, ltrim(rtrim(tch_claim_nbr)),
	    IFF(
	        length(pms_policy_sym) > 0,
	        ltrim(rtrim(pms_policy_sym || pms_policy_num || pms_policy_mod || v_pms_date_of_loss || pms_loss_occurence)),
	        'N/A'
	    )
	) AS o_claim_nbr,
	-- *INF*: IIF(length(tch_claim_nbr)>0,ltrim(rtrim(tch_client_id)),
	-- 
	-- IIF(length(pms_policy_sym)>0,
	-- ltrim(rtrim(pms_policy_sym || pms_policy_num || pms_policy_mod || v_pms_date_of_loss ||  pms_loss_occurence || pms_loss_claimant || 'CMT')),
	-- 
	-- 'N/A'))
	IFF(
	    length(tch_claim_nbr) > 0, ltrim(rtrim(tch_client_id)),
	    IFF(
	        length(pms_policy_sym) > 0,
	        ltrim(rtrim(pms_policy_sym || pms_policy_num || pms_policy_mod || v_pms_date_of_loss || pms_loss_occurence || pms_loss_claimant || 'CMT')),
	        'N/A'
	    )
	) AS o_client_id,
	source_system_id,
	estimated_savings_amount,
	-- *INF*: IIF(isnull(ltrim(rtrim(estimated_savings_amount))),0,estimated_savings_amount)
	IFF(ltrim(rtrim(estimated_savings_amount)) IS NULL, 0, estimated_savings_amount) AS o_estimated_savings_amount
	FROM SQ_clmt_nurse_manage_stage
),
LKP_claim_party_occurrence AS (
	SELECT
	claim_party_occurrence_ak_id,
	claimant_num,
	claim_party_role_code
	FROM (
		SELECT
		CPO.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id,
		LTRIM(RTRIM(CO.claim_occurrence_key)) as claimant_num,
		LTRIM(RTRIM(CP.claim_party_key)) as claim_party_role_code
		
		FROM
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence CPO,
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party CP,
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence CO
		
		WHERE
		CO.claim_occurrence_ak_id = CPO.claim_occurrence_ak_id
		AND
		CP.claim_party_ak_id = CPO.claim_party_ak_id
		AND
		CPO.claim_party_role_code in ('CLMT', 'CMT')
		AND
		CO.crrnt_snpsht_flag = 1
		AND
		CP.crrnt_snpsht_flag = 1
		AND
		CPO.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_num,claim_party_role_code ORDER BY claim_party_occurrence_ak_id) = 1
),
EXP_Lkp_Default AS (
	SELECT
	claim_party_occurrence_ak_id,
	-- *INF*: iif(isnull(claim_party_occurrence_ak_id), -1, claim_party_occurrence_ak_id)
	IFF(claim_party_occurrence_ak_id IS NULL, - 1, claim_party_occurrence_ak_id) AS o_claim_party_occurence_ak_id
	FROM LKP_claim_party_occurrence
),
LKP_NurseCase AS (
	SELECT
	NurseCaseId,
	claim_party_occurrence_ak_id,
	clmt_nurse_manage_id,
	EstimatedSavingsAmount,
	NurseCaseAkId
	FROM (
		SELECT
		N.NurseCaseId as NurseCaseId, 
		N.clmt_nurse_manage_id as clmt_nurse_manage_id, 
		N.EstimatedSavingsAmount as EstimatedSavingsAmount,
		N.NurseCaseAkId as NurseCaseAkId, 
		N.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id
		
		FROM
		 @{pipeline().parameters.TARGET_TABLE_OWNER}.NurseCase N
		
		where
		N.CurrentSnapshotFlag= 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id,clmt_nurse_manage_id ORDER BY NurseCaseId) = 1
),
EXP_TargetLkp_Detect_Changes AS (
	SELECT
	LKP_NurseCase.NurseCaseId AS Lkp_NurseCaseId,
	LKP_NurseCase.claim_party_occurrence_ak_id AS Lkp_claim_party_occurrence_ak_id,
	LKP_NurseCase.clmt_nurse_manage_id AS Lkp_clmt_nurse_manage_id,
	LKP_NurseCase.EstimatedSavingsAmount AS Lkp_EstimatedSavingsAmount,
	-- *INF*: iif(isnull(Lkp_NurseCaseId),'NEW',
	--   
	--   iif(
	--   
	--       LTRIM(RTRIM(Lkp_claim_party_occurrence_ak_id)) != LTRIM(RTRIM(claim_party_occurrence_ak_id))
	-- 
	--  or
	--   
	-- --        LTRIM(RTRIM(Lkp_NurseCaseAkId)) != LTRIM(RTRIM(NurseCaseAkId))
	-- 
	-- --or
	--   
	--         LTRIM(RTRIM(Lkp_clmt_nurse_manage_id)) != LTRIM(RTRIM(clmt_nurse_manage_id))
	-- 
	-- or
	--   
	--         LTRIM(RTRIM(Lkp_EstimatedSavingsAmount)) != LTRIM(RTRIM(EstimatedSavingsAmount)),
	--         
	--         'UPDATE', 'NOCHANGE')
	--    )
	IFF(
	    Lkp_NurseCaseId IS NULL, 'NEW',
	    IFF(
	        LTRIM(RTRIM(Lkp_claim_party_occurrence_ak_id)) != LTRIM(RTRIM(claim_party_occurrence_ak_id))
	        or LTRIM(RTRIM(Lkp_clmt_nurse_manage_id)) != LTRIM(RTRIM(clmt_nurse_manage_id))
	        or LTRIM(RTRIM(Lkp_EstimatedSavingsAmount)) != LTRIM(RTRIM(EstimatedSavingsAmount)),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_ChangedFlag,
	v_ChangedFlag AS ChangedFlag,
	-- *INF*: iif(v_ChangedFlag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(
	    v_ChangedFlag = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	EXP_Lkp_Default.o_claim_party_occurence_ak_id AS claim_party_occurrence_ak_id,
	EXP_Src_Values.o_estimated_savings_amount AS EstimatedSavingsAmount,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	EXP_Src_Values.o_clmt_nurse_manage_id AS clmt_nurse_manage_id,
	EXP_Src_Values.source_system_id AS SourceSystemId,
	LKP_NurseCase.NurseCaseAkId AS Lkp_NurseCaseAkId
	FROM EXP_Lkp_Default
	 -- Manually join with EXP_Src_Values
	LEFT JOIN LKP_NurseCase
	ON LKP_NurseCase.claim_party_occurrence_ak_id = EXP_Lkp_Default.o_claim_party_occurence_ak_id AND LKP_NurseCase.clmt_nurse_manage_id = EXP_Src_Values.o_clmt_nurse_manage_id
),
FIL_Lkp_Records AS (
	SELECT
	ChangedFlag, 
	EffectiveDate, 
	ExpirationDate, 
	CreatedDate, 
	ModifiedDate, 
	claim_party_occurrence_ak_id, 
	EstimatedSavingsAmount, 
	CurrentSnapshotFlag, 
	AuditId, 
	clmt_nurse_manage_id, 
	SourceSystemId, 
	Lkp_NurseCaseAkId
	FROM EXP_TargetLkp_Detect_Changes
	WHERE ChangedFlag = 'NEW' or ChangedFlag = 'UPDATE'
),
SEQ_NurseCase AS (
	CREATE SEQUENCE SEQ_NurseCase
	START = 0
	INCREMENT = 1;
),
EXP_AKid_Insert_Target AS (
	SELECT
	ChangedFlag,
	-- *INF*: IIF(ChangedFlag='NEW', NEXTVAL, Lkp_NurseCaseAkId)
	IFF(ChangedFlag = 'NEW', NEXTVAL, Lkp_NurseCaseAkId) AS NurseCaseAkId,
	EffectiveDate,
	ExpirationDate,
	CreatedDate,
	ModifiedDate,
	claim_party_occurrence_ak_id,
	EstimatedSavingsAmount,
	CurrentSnapshotFlag,
	AuditId,
	clmt_nurse_manage_id,
	SourceSystemId,
	Lkp_NurseCaseAkId,
	SEQ_NurseCase.NEXTVAL
	FROM FIL_Lkp_Records
),
NurseCase_Insert AS (
	INSERT INTO NurseCase
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, NurseCaseAkId, claim_party_occurrence_ak_id, clmt_nurse_manage_id, EstimatedSavingsAmount)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	NURSECASEAKID, 
	CLAIM_PARTY_OCCURRENCE_AK_ID, 
	CLMT_NURSE_MANAGE_ID, 
	ESTIMATEDSAVINGSAMOUNT
	FROM EXP_AKid_Insert_Target
),
SQ_NurseCase AS (
	SELECT
	A.NurseCaseId,
	A.EffectiveDate,
	A.ExpirationDate,
	A.NurseCaseAkId 
	
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.NurseCase A
	
	Where Exists 
	   ( 
	SELECT 1
	FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.NurseCase B
	
	where
	B.CurrentSnapshotFlag =1
	AND
	A.NurseCaseAkId = B.NurseCaseAkId
	
	group by 
	B.NurseCaseAkId
	
	having
	count(*) > 1 
	   )
	
	AND
	A.CurrentSnapshotFlag = 1
	
	order by 
	A.NurseCaseAkId, 
	A.EffectiveDate DESC
),
EXP_Lag_ExpirationDate AS (
	SELECT
	NurseCaseId,
	EffectiveDate,
	ExpirationDate AS orig_ExpirationDate,
	-- *INF*: DECODE(TRUE, 
	-- NurseCaseAkId= v_PREV_ROW_NurseCaseAkId, ADD_TO_DATE(v_PREV_ROW_EffectiveDate,'SS',-1),
	-- 	orig_ExpirationDate)
	DECODE(
	    TRUE,
	    NurseCaseAkId = v_PREV_ROW_NurseCaseAkId, DATEADD(SECOND,- 1,v_PREV_ROW_EffectiveDate),
	    orig_ExpirationDate
	) AS v_ExpirationDate,
	v_ExpirationDate AS ExpirationDate,
	EffectiveDate AS v_PREV_ROW_EffectiveDate,
	NurseCaseAkId AS v_PREV_ROW_NurseCaseAkId,
	SYSDATE AS ModifiedDate,
	0 AS CurrentSnapshotFlag,
	NurseCaseAkId
	FROM SQ_NurseCase
),
FIL_FirstRowAKid AS (
	SELECT
	NurseCaseId, 
	orig_ExpirationDate, 
	ExpirationDate, 
	ModifiedDate, 
	CurrentSnapshotFlag
	FROM EXP_Lag_ExpirationDate
	WHERE orig_ExpirationDate != ExpirationDate
),
UPD_NurseCase AS (
	SELECT
	NurseCaseId, 
	ExpirationDate, 
	ModifiedDate, 
	CurrentSnapshotFlag
	FROM FIL_FirstRowAKid
),
NurseCase_Update AS (
	MERGE INTO NurseCase AS T
	USING UPD_NurseCase AS S
	ON T.NurseCaseId = S.NurseCaseId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),
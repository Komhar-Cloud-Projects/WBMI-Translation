WITH
SQ_clmt_surgery_detail_stage AS (
	SELECT
		ClmtSurgeryDetailStageId,
		clmt_surgery_detail_id,
		surgery_type_cd,
		source_system_id,
		created_ts,
		created_user_id,
		modified_ts,
		modified_user_id,
		ExtractDate,
		SourceSystemId
	FROM clmt_surgery_detail_stage
),
EXP_Src_Value AS (
	SELECT
	clmt_surgery_detail_id,
	-- *INF*: iif(isnull(clmt_surgery_detail_id),-1, clmt_surgery_detail_id)
	IFF(clmt_surgery_detail_id IS NULL, - 1, clmt_surgery_detail_id) AS o_clmt_surgery_detail_id,
	surgery_type_cd,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(surgery_type_cd),'N/A',
	-- IS_SPACES(surgery_type_cd),'N/A',
	-- LENGTH(surgery_type_cd)=0,'N/A',
	-- LTRIM(RTRIM(surgery_type_cd)))
	DECODE(TRUE,
		surgery_type_cd IS NULL, 'N/A',
		IS_SPACES(surgery_type_cd), 'N/A',
		LENGTH(surgery_type_cd) = 0, 'N/A',
		LTRIM(RTRIM(surgery_type_cd))) AS o_surgery_type_cd,
	source_system_id
	FROM SQ_clmt_surgery_detail_stage
),
LKP_SupSurgeryType AS (
	SELECT
	SupSurgeryTypeId,
	SurgeryTypeCode
	FROM (
		SELECT
		SupSurgeryType.SupSurgeryTypeId as SupSurgeryTypeId, SupSurgeryType.SurgeryTypeCode as SurgeryTypeCode 
		
		FROM SupSurgeryType
		
		where 
		
		CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SurgeryTypeCode ORDER BY SupSurgeryTypeId) = 1
),
LKP_exceed_clmt_surgery_relation_stage AS (
	SELECT
	tch_claim_nbr,
	tch_client_id,
	clmt_surgery_detail_id
	FROM (
		SELECT
		E.tch_claim_nbr as tch_claim_nbr, 
		E.tch_client_id as tch_client_id, 
		E.clmt_surgery_detail_id as clmt_surgery_detail_id
		
		 FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.exceed_clmt_surgery_relation_stage E
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY clmt_surgery_detail_id ORDER BY tch_claim_nbr) = 1
),
LKP_pms_clmt_surgery_relation_stage AS (
	SELECT
	pms_policy_sym,
	pms_policy_num,
	pms_policy_mod,
	pms_date_of_loss,
	pms_loss_occurence,
	pms_loss_claimant,
	clmt_surgery_detail_id
	FROM (
		SELECT
		P.pms_policy_sym as pms_policy_sym, 
		P.pms_policy_num as pms_policy_num, 
		P.pms_policy_mod as pms_policy_mod, 
		P.pms_date_of_loss as pms_date_of_loss, P.pms_loss_occurence as pms_loss_occurence, P.pms_loss_claimant as pms_loss_claimant, P.clmt_surgery_detail_id as clmt_surgery_detail_id
		
		 FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.pms_clmt_surgery_relation_stage P
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY clmt_surgery_detail_id ORDER BY pms_policy_sym) = 1
),
EXP_pms_exceed AS (
	SELECT
	LKP_exceed_clmt_surgery_relation_stage.tch_claim_nbr,
	LKP_exceed_clmt_surgery_relation_stage.tch_client_id,
	LKP_pms_clmt_surgery_relation_stage.pms_policy_sym,
	LKP_pms_clmt_surgery_relation_stage.pms_policy_num,
	LKP_pms_clmt_surgery_relation_stage.pms_policy_mod,
	LKP_pms_clmt_surgery_relation_stage.pms_date_of_loss,
	LKP_pms_clmt_surgery_relation_stage.pms_loss_occurence,
	LKP_pms_clmt_surgery_relation_stage.pms_loss_claimant,
	-- *INF*: to_char(pms_date_of_loss,'MMDDYYYY')
	to_char(pms_date_of_loss, 'MMDDYYYY') AS v_pms_date_of_loss,
	-- *INF*: IIF(length(tch_claim_nbr)>0 ,ltrim(rtrim(tch_claim_nbr)),
	-- 
	-- IIF(length(pms_policy_sym)>0,
	-- ltrim(rtrim(pms_policy_sym || pms_policy_num || pms_policy_mod || v_pms_date_of_loss ||  pms_loss_occurence)),
	-- 
	-- 'N/A'))
	IFF(length(tch_claim_nbr) > 0, ltrim(rtrim(tch_claim_nbr)), IFF(length(pms_policy_sym) > 0, ltrim(rtrim(pms_policy_sym || pms_policy_num || pms_policy_mod || v_pms_date_of_loss || pms_loss_occurence)), 'N/A')) AS o_tch_claim_nbr,
	-- *INF*: IIF(length(tch_claim_nbr)>0,ltrim(rtrim(tch_client_id)),
	-- 
	-- IIF(length(pms_policy_sym)>0,
	-- ltrim(rtrim(pms_policy_sym || pms_policy_num || pms_policy_mod || v_pms_date_of_loss ||  pms_loss_occurence || pms_loss_claimant || 'CMT')),
	-- 
	-- 'N/A'))
	IFF(length(tch_claim_nbr) > 0, ltrim(rtrim(tch_client_id)), IFF(length(pms_policy_sym) > 0, ltrim(rtrim(pms_policy_sym || pms_policy_num || pms_policy_mod || v_pms_date_of_loss || pms_loss_occurence || pms_loss_claimant || 'CMT')), 'N/A')) AS o_tch_client_id
	FROM 
	LEFT JOIN LKP_exceed_clmt_surgery_relation_stage
	ON LKP_exceed_clmt_surgery_relation_stage.clmt_surgery_detail_id = EXP_Src_Value.o_clmt_surgery_detail_id
	LEFT JOIN LKP_pms_clmt_surgery_relation_stage
	ON LKP_pms_clmt_surgery_relation_stage.clmt_surgery_detail_id = EXP_Src_Value.o_clmt_surgery_detail_id
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
		CPO.claim_party_role_code in ('CMT', 'CLMT')
		AND
		CO.crrnt_snpsht_flag = 1
		AND
		CP.crrnt_snpsht_flag = 1
		AND
		CPO.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_num,claim_party_role_code ORDER BY claim_party_occurrence_ak_id) = 1
),
EXP_Lkp_Defualt AS (
	SELECT
	LKP_claim_party_occurrence.claim_party_occurrence_ak_id,
	-- *INF*: iif(isnull(claim_party_occurrence_ak_id), -1,claim_party_occurrence_ak_id)
	IFF(claim_party_occurrence_ak_id IS NULL, - 1, claim_party_occurrence_ak_id) AS o_claim_party_occurrence_ak_id,
	LKP_SupSurgeryType.SupSurgeryTypeId,
	-- *INF*: iif(isnull(SupSurgeryTypeId), -1, SupSurgeryTypeId)
	IFF(SupSurgeryTypeId IS NULL, - 1, SupSurgeryTypeId) AS o_SupSurgeryTypeId
	FROM 
	LEFT JOIN LKP_SupSurgeryType
	ON LKP_SupSurgeryType.SurgeryTypeCode = EXP_Src_Value.o_surgery_type_cd
	LEFT JOIN LKP_claim_party_occurrence
	ON LKP_claim_party_occurrence.claimant_num = EXP_pms_exceed.o_tch_claim_nbr AND LKP_claim_party_occurrence.claim_party_role_code = EXP_pms_exceed.o_tch_client_id
),
LKP_ClaimantSurgeryDetail_Target AS (
	SELECT
	ClaimantSurgeryDetailId,
	claim_party_occurrence_ak_Id,
	SupSurgeryTypeId,
	clmt_surgery_detail_id,
	ClaimantSurgeryDetailAkId
	FROM (
		SELECT 
		C.ClaimantSurgeryDetailId as ClaimantSurgeryDetailId, C.clmt_surgery_detail_id as clmt_surgery_detail_id, C.ClaimantSurgeryDetailAkId as ClaimantSurgeryDetailAkId, C.claim_party_occurrence_ak_Id as claim_party_occurrence_ak_Id, 
		C.SupSurgeryTypeId as SupSurgeryTypeId
		
		 FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.ClaimantSurgeryDetail C
		
		where
		    CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_Id,clmt_surgery_detail_id,SupSurgeryTypeId ORDER BY ClaimantSurgeryDetailId) = 1
),
EXP_TargetLkp_Detect_Changes AS (
	SELECT
	LKP_ClaimantSurgeryDetail_Target.ClaimantSurgeryDetailId AS Lkp_ClaimantSurgeryDetailId,
	LKP_ClaimantSurgeryDetail_Target.claim_party_occurrence_ak_Id AS Lkp_claim_party_occurrence_ak_Id,
	LKP_ClaimantSurgeryDetail_Target.SupSurgeryTypeId AS Lkp_SupSurgeryTypeId,
	LKP_ClaimantSurgeryDetail_Target.clmt_surgery_detail_id AS Lkp_clmt_surgery_detail_id,
	-- *INF*: iif(isnull(Lkp_ClaimantSurgeryDetailId),'NEW',
	--   
	--   iif(
	--   
	--       LTRIM(RTRIM(Lkp_claim_party_occurrence_ak_Id)) != LTRIM(RTRIM(claim_party_occurrence_ak_Id))
	-- 
	--     or
	--   
	--       LTRIM(RTRIM(Lkp_SupSurgeryTypeId)) != LTRIM(RTRIM(SupSurgeryTypeId))
	-- 
	-- --    or
	--   
	--  --     LTRIM(RTRIM(Lkp_ClaimantSurgeryDetailAkId)) != LTRIM(RTRIM(ClaimantSurgeryDetailAkId))
	-- 
	--    or
	-- 
	--       LTRIM(RTRIM(Lkp_clmt_surgery_detail_id)) != LTRIM(RTRIM(clmt_surgery_detail_id)),
	--         
	--         'UPDATE', 'NOCHANGE')
	-- 
	--    )
	IFF(Lkp_ClaimantSurgeryDetailId IS NULL, 'NEW', IFF(LTRIM(RTRIM(Lkp_claim_party_occurrence_ak_Id)) != LTRIM(RTRIM(claim_party_occurrence_ak_Id)) OR LTRIM(RTRIM(Lkp_SupSurgeryTypeId)) != LTRIM(RTRIM(SupSurgeryTypeId)) OR LTRIM(RTRIM(Lkp_clmt_surgery_detail_id)) != LTRIM(RTRIM(clmt_surgery_detail_id)), 'UPDATE', 'NOCHANGE')) AS v_ChangedFlag,
	v_ChangedFlag AS ChangedFlag,
	EXP_Lkp_Defualt.o_claim_party_occurrence_ak_id AS claim_party_occurrence_ak_Id,
	EXP_Lkp_Defualt.o_SupSurgeryTypeId AS SupSurgeryTypeId,
	-- *INF*: iif(v_ChangedFlag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_ChangedFlag = 'NEW', to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), sysdate) AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	EXP_Src_Value.source_system_id AS SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	1 AS CurrentSnapshotFlag,
	LKP_ClaimantSurgeryDetail_Target.ClaimantSurgeryDetailAkId AS Lkp_ClaimantSurgeryDetailAkId,
	EXP_Src_Value.o_clmt_surgery_detail_id AS clmt_surgery_detail_id
	FROM EXP_Lkp_Defualt
	 -- Manually join with EXP_Src_Value
	LEFT JOIN LKP_ClaimantSurgeryDetail_Target
	ON LKP_ClaimantSurgeryDetail_Target.claim_party_occurrence_ak_Id = EXP_Lkp_Defualt.o_claim_party_occurrence_ak_id AND LKP_ClaimantSurgeryDetail_Target.clmt_surgery_detail_id = EXP_Src_Value.o_clmt_surgery_detail_id AND LKP_ClaimantSurgeryDetail_Target.SupSurgeryTypeId = LKP_SupSurgeryType.SupSurgeryTypeId
),
FIL_Lkp_Records AS (
	SELECT
	ChangedFlag, 
	claim_party_occurrence_ak_Id, 
	SupSurgeryTypeId, 
	EffectiveDate, 
	ExpirationDate, 
	CreatedDate, 
	ModifiedDate, 
	SourceSystemId, 
	AuditId, 
	CurrentSnapshotFlag, 
	Lkp_ClaimantSurgeryDetailAkId, 
	clmt_surgery_detail_id
	FROM EXP_TargetLkp_Detect_Changes
	WHERE ChangedFlag = 'NEW'  OR ChangedFlag = 'UPDATE'
),
SEQ_ClaimantSugeryDetail AS (
	CREATE SEQUENCE SEQ_ClaimantSugeryDetail
	START = 0
	INCREMENT = 1;
),
EXP_AKid_Insert_Target AS (
	SELECT
	ChangedFlag,
	SEQ_ClaimantSugeryDetail.NEXTVAL,
	-- *INF*: IIF(ChangedFlag='NEW', NEXTVAL, Lkp_ClaimantSurgeryDetailAkId)
	IFF(ChangedFlag = 'NEW', NEXTVAL, Lkp_ClaimantSurgeryDetailAkId) AS o_ClaimantSurgeryDetailAkId,
	claim_party_occurrence_ak_Id,
	SupSurgeryTypeId,
	EffectiveDate,
	ExpirationDate,
	CreatedDate,
	ModifiedDate,
	SourceSystemId,
	AuditId,
	CurrentSnapshotFlag,
	Lkp_ClaimantSurgeryDetailAkId,
	clmt_surgery_detail_id
	FROM FIL_Lkp_Records
),
ClaimantSurgeryDetail_Insert AS (
	INSERT INTO ClaimantSurgeryDetail
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, ClaimantSurgeryDetailAkId, claim_party_occurrence_ak_Id, SupSurgeryTypeId, clmt_surgery_detail_id)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	o_ClaimantSurgeryDetailAkId AS CLAIMANTSURGERYDETAILAKID, 
	CLAIM_PARTY_OCCURRENCE_AK_ID, 
	SUPSURGERYTYPEID, 
	CLMT_SURGERY_DETAIL_ID
	FROM EXP_AKid_Insert_Target
),
SQ_ClaimantSurgeryDetail AS (
	SELECT
	A.ClaimantSurgeryDetailId,
	A.EffectiveDate,
	A.ExpirationDate,
	A.ClaimantSurgeryDetailAkId
	
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.ClaimantSurgeryDetail A
	
	where Exists
	   ( 
	SELECT 1 
	FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.ClaimantSurgeryDetail B
	
	where
	A.ClaimantSurgeryDetailAkId = B.ClaimantSurgeryDetailAkId
	AND
	B.CurrentSnapshotFlag = 1
	
	group by 
	B.ClaimantSurgeryDetailAkId
	
	having 
	count(*) > 1 
	   )
	
	AND
	A.CurrentSnapshotFlag = 1
	
	order by 
	A.ClaimantSurgeryDetailAkId, 
	A.EffectiveDate DESC
),
EXP_Lap_ExpirationDate AS (
	SELECT
	ClaimantSurgeryDetailId,
	EffectiveDate,
	ExpirationDate AS orig_ExpirationDate,
	-- *INF*: DECODE(TRUE, 
	-- ClaimantSurgeryDetailAkId= v_PREV_ROW_ClaimantSurgeryDetailAkId, ADD_TO_DATE(v_PREV_ROW_EffectiveDate,'SS',-1),
	-- 	orig_ExpirationDate)
	DECODE(TRUE,
		ClaimantSurgeryDetailAkId = v_PREV_ROW_ClaimantSurgeryDetailAkId, ADD_TO_DATE(v_PREV_ROW_EffectiveDate, 'SS', - 1),
		orig_ExpirationDate) AS v_ExpirationDate,
	v_ExpirationDate AS ExpirationDate,
	EffectiveDate AS v_PREV_ROW_EffectiveDate,
	ClaimantSurgeryDetailAkId AS v_PREV_ROW_ClaimantSurgeryDetailAkId,
	SYSDATE AS ModifiedDate,
	0 AS CurrentSnapshotFlag,
	ClaimantSurgeryDetailAkId
	FROM SQ_ClaimantSurgeryDetail
),
FIL_FirstRowAkId AS (
	SELECT
	ClaimantSurgeryDetailId, 
	orig_ExpirationDate, 
	ExpirationDate, 
	ModifiedDate, 
	CurrentSnapshotFlag
	FROM EXP_Lap_ExpirationDate
	WHERE orig_ExpirationDate  != ExpirationDate
),
UPD_ClaimantSurgeryDetail_Update AS (
	SELECT
	ClaimantSurgeryDetailId, 
	ExpirationDate, 
	ModifiedDate, 
	CurrentSnapshotFlag
	FROM FIL_FirstRowAkId
),
ClaimantSurgeryDetail_Update AS (
	MERGE INTO ClaimantSurgeryDetail AS T
	USING UPD_ClaimantSurgeryDetail_Update AS S
	ON T.ClaimantSurgeryDetailId = S.ClaimantSurgeryDetailId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),
SQ_ClaimantSurgeryDetail_Expire AS (
	SELECT 
	CS.ClaimantSurgeryDetailId, 
	CS.EffectiveDate, 
	CS.ExpirationDate, 
	CS.SupSurgeryTypeId, 
	CS.clmt_surgery_detail_id 
	
	FROM
	RPT_EDM.dbo.ClaimantSurgeryDetail CS,
	RPT_EDM.dbo.SupSurgeryType S,
	WC_Stage..clmt_surgery_detail_deleted_stage CSD
	 
	where
	CS.clmt_surgery_detail_id = CSD.clmt_surgery_detail_id
	AND
	CSD.surgery_type_cd = S.SurgeryTypeCode
	AND
	CS.SupSurgeryTypeId = S.SupSurgeryTypeId
	AND
	S.CurrentSnapshotFlag = 1
	AND
	CS.CurrentSnapshotFlag = 1
),
EXP_Expiring AS (
	SELECT
	ClaimantSurgeryDetailId,
	0 AS CurrentSnapshotFlag,
	clmt_surgery_detail_id,
	SupSurgeryTypeId,
	SYSDATE AS ModifiedDate,
	EffectiveDate,
	ExpirationDate
	FROM SQ_ClaimantSurgeryDetail_Expire
),
FIL_Expire AS (
	SELECT
	ClaimantSurgeryDetailId, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM EXP_Expiring
	WHERE NOT isnull(ClaimantSurgeryDetailId)
),
UPD_Expire AS (
	SELECT
	ClaimantSurgeryDetailId, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM FIL_Expire
),
ClaimantSurgeryDetail_Expire1 AS (
	MERGE INTO ClaimantSurgeryDetail AS T
	USING UPD_Expire AS S
	ON T.ClaimantSurgeryDetailId = S.ClaimantSurgeryDetailId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ModifiedDate = S.ModifiedDate
),
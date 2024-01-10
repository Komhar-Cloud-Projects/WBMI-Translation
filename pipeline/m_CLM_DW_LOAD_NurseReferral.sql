WITH
SQ_clmt_nurse_referral_stage AS (
	SELECT
		ClmntNurseReferralStageId,
		nurse_referral_id,
		clmt_nurse_manage_id,
		referred_to_nurse_id,
		referral_date,
		created_ts,
		created_user_id,
		modified_ts,
		modified_user_id,
		ExtractDate,
		SourceSystemId
	FROM clmt_nurse_referral_stage
),
EXP_Src_Values AS (
	SELECT
	nurse_referral_id,
	-- *INF*: iif(isnull(ltrim(rtrim(nurse_referral_id))),-1,nurse_referral_id)
	IFF(ltrim(rtrim(nurse_referral_id)) IS NULL, - 1, nurse_referral_id) AS o_nurse_referral_id,
	clmt_nurse_manage_id,
	-- *INF*: iif(isnull(ltrim(rtrim(clmt_nurse_manage_id))),0,clmt_nurse_manage_id)
	IFF(ltrim(rtrim(clmt_nurse_manage_id)) IS NULL, 0, clmt_nurse_manage_id) AS o_clmt_nurse_manage_id,
	referred_to_nurse_id,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(referred_to_nurse_id),'N/A',
	-- IS_SPACES(referred_to_nurse_id),'N/A',
	-- LENGTH(referred_to_nurse_id)=0,'N/A',
	-- LTRIM(RTRIM(referred_to_nurse_id)))
	DECODE(TRUE,
		referred_to_nurse_id IS NULL, 'N/A',
		IS_SPACES(referred_to_nurse_id), 'N/A',
		LENGTH(referred_to_nurse_id) = 0, 'N/A',
		LTRIM(RTRIM(referred_to_nurse_id))) AS o_referred_to_nurse_id,
	referral_date,
	-- *INF*: iif(isnull(ltrim(rtrim(referral_date))),to_date('1/1/1800','MM/DD/YYYY'),referral_date)
	IFF(ltrim(rtrim(referral_date)) IS NULL, to_date('1/1/1800', 'MM/DD/YYYY'), referral_date) AS o_referral_date
	FROM SQ_clmt_nurse_referral_stage
),
LKP_NurseCase AS (
	SELECT
	NurseCaseAkid,
	clmt_nurse_manage_id
	FROM (
		SELECT
		NurseCase.SourceSystemId as SourceSystemId, NurseCase.NurseCaseAkid as NurseCaseAkid, NurseCase.clmt_nurse_manage_id as clmt_nurse_manage_id
		
		 FROM NurseCase
		
		where
		CurrentSnapshotFlag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY clmt_nurse_manage_id ORDER BY NurseCaseAkid) = 1
),
LKP_claim_party AS (
	SELECT
	claim_party_ak_id,
	claim_party_key
	FROM (
		SELECT
		CP.claim_party_ak_id as claim_party_ak_id, 
		CP.claim_party_key as claim_party_key 
		
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party CP
		
		where
		CP.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_key ORDER BY claim_party_ak_id) = 1
),
EXP_Lkp_Defualt AS (
	SELECT
	LKP_NurseCase.NurseCaseAkid,
	-- *INF*: iif(isnull(NurseCaseAkid), -1, NurseCaseAkid)
	IFF(NurseCaseAkid IS NULL, - 1, NurseCaseAkid) AS o_NurseCaseAkid,
	LKP_claim_party.claim_party_ak_id,
	-- *INF*: iif(isnull(claim_party_ak_id), -1,claim_party_ak_id)
	IFF(claim_party_ak_id IS NULL, - 1, claim_party_ak_id) AS o_claim_party_ak_id
	FROM 
	LEFT JOIN LKP_NurseCase
	ON LKP_NurseCase.clmt_nurse_manage_id = EXP_Src_Values.o_clmt_nurse_manage_id
	LEFT JOIN LKP_claim_party
	ON LKP_claim_party.claim_party_key = EXP_Src_Values.o_referred_to_nurse_id
),
LKP_NurseReferral AS (
	SELECT
	NurseReferralId,
	NurseCaseAkId,
	claim_party_ak_id,
	nurse_referral_id,
	ReferralDate,
	NurseReferralAkId
	FROM (
		SELECT
		N.NurseReferralId as NurseReferralId, 
		N.nurse_referral_id as nurse_referral_id, 
		N.ReferralDate as ReferralDate, 
		N.NurseReferralAkId as NurseReferralAkId, 
		N.NurseCaseAkId as NurseCaseAkId, 
		N.claim_party_ak_id as claim_party_ak_id
		
		 FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.NurseReferral N
		
		where
		N.CurrentSnapshotFlag  = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY NurseCaseAkId,claim_party_ak_id,nurse_referral_id ORDER BY NurseReferralId) = 1
),
EXP_TargetLkp_Detect_Changes AS (
	SELECT
	LKP_NurseReferral.NurseReferralId AS Lkp_NurseReferralId,
	LKP_NurseReferral.NurseCaseAkId AS Lkp_NurseCaseAkId,
	LKP_NurseReferral.claim_party_ak_id AS Lkp_claim_party_ak_id,
	LKP_NurseReferral.nurse_referral_id AS LKp_nurse_referral_id,
	LKP_NurseReferral.ReferralDate AS Lkp_ReferralDate,
	-- *INF*: iif(isnull(Lkp_NurseReferralId),'NEW',
	-- 
	--   iif(
	-- 
	--    ltrim(rtrim(Lkp_NurseCaseAkId)) != ltrim(rtrim(NurseCaseAkId))
	-- 
	-- or
	-- 
	--   ltrim(rtrim(Lkp_claim_party_ak_id)) != ltrim(rtrim(claim_party_ak_id))
	-- 
	-- or
	-- 
	--   ltrim(rtrim(LKp_nurse_referral_id)) != ltrim(rtrim(nurse_referral_id))
	-- 
	-- or
	-- 
	--   ltrim(rtrim(Lkp_ReferralDate)) != ltrim(rtrim(ReferralDate)),
	-- 
	--     'UPDATE', 'NOCHANGE')
	-- 
	--    )
	IFF(Lkp_NurseReferralId IS NULL, 'NEW', IFF(ltrim(rtrim(Lkp_NurseCaseAkId)) != ltrim(rtrim(NurseCaseAkId)) OR ltrim(rtrim(Lkp_claim_party_ak_id)) != ltrim(rtrim(claim_party_ak_id)) OR ltrim(rtrim(LKp_nurse_referral_id)) != ltrim(rtrim(nurse_referral_id)) OR ltrim(rtrim(Lkp_ReferralDate)) != ltrim(rtrim(ReferralDate)), 'UPDATE', 'NOCHANGE')) AS v_ChangedFlag,
	v_ChangedFlag AS ChangedFlag,
	EXP_Lkp_Defualt.o_NurseCaseAkid AS NurseCaseAkId,
	EXP_Lkp_Defualt.o_claim_party_ak_id AS claim_party_ak_id,
	EXP_Src_Values.o_nurse_referral_id AS nurse_referral_id,
	EXP_Src_Values.o_referral_date AS ReferralDate,
	-- *INF*: IIF(v_ChangedFlag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'), SYSDATE)
	IFF(v_ChangedFlag = 'NEW', TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemId,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	LKP_NurseReferral.NurseReferralAkId AS Lkp_NurseReferralAkId
	FROM EXP_Lkp_Defualt
	 -- Manually join with EXP_Src_Values
	LEFT JOIN LKP_NurseReferral
	ON LKP_NurseReferral.NurseCaseAkId = EXP_Lkp_Defualt.o_NurseCaseAkid AND LKP_NurseReferral.claim_party_ak_id = EXP_Lkp_Defualt.o_claim_party_ak_id AND LKP_NurseReferral.nurse_referral_id = EXP_Src_Values.o_nurse_referral_id
),
FIL_Lkp_Records AS (
	SELECT
	ChangedFlag, 
	NurseCaseAkId, 
	claim_party_ak_id, 
	nurse_referral_id, 
	ReferralDate, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemId, 
	CreatedDate, 
	ModifiedDate, 
	CurrentSnapshotFlag, 
	AuditId, 
	Lkp_NurseReferralAkId
	FROM EXP_TargetLkp_Detect_Changes
	WHERE ChangedFlag = 'NEW' or ChangedFlag = 'UPDATE'
),
SEQ_NurseReferral AS (
	CREATE SEQUENCE SEQ_NurseReferral
	START = 0
	INCREMENT = 1;
),
EXP_Akid_Insert_Target AS (
	SELECT
	ChangedFlag,
	NurseCaseAkId,
	claim_party_ak_id,
	nurse_referral_id,
	ReferralDate,
	EffectiveDate,
	ExpirationDate,
	SourceSystemId,
	CreatedDate,
	ModifiedDate,
	CurrentSnapshotFlag,
	AuditId,
	Lkp_NurseReferralAkId,
	-- *INF*: iif(ChangedFlag = 'NEW', NEXTVAL, Lkp_NurseReferralAkId)
	IFF(ChangedFlag = 'NEW', NEXTVAL, Lkp_NurseReferralAkId) AS NurseReferralAkId,
	SEQ_NurseReferral.NEXTVAL
	FROM FIL_Lkp_Records
),
NurseReferral_Insert AS (
	INSERT INTO NurseReferral
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, NurseReferralAkId, NurseCaseAkId, claim_party_ak_id, nurse_referral_id, ReferralDate)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	NURSEREFERRALAKID, 
	NURSECASEAKID, 
	CLAIM_PARTY_AK_ID, 
	NURSE_REFERRAL_ID, 
	REFERRALDATE
	FROM EXP_Akid_Insert_Target
),
SQ_NurseReferral AS (
	SELECT
	A.NurseReferralId,
	A.EffectiveDate,
	A.ExpirationDate,
	A.NurseReferralAkId 
	
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.NurseReferral A
	
	Where Exists 
	    ( 
	SELECT 1
	FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.NurseReferral B
	
	where
	B.CurrentSnapshotFlag =1
	AND
	A.NurseReferralAkId = B.NurseReferralAkId 
	
	group by 
	B.NurseReferralAkId 
	
	having 
	count(*) > 1
	    )
	
	AND
	A.CurrentSnapshotFlag = 1
	
	order by 
	A.NurseReferralAkId , 
	A.EffectiveDate DESC
),
EXP_Lag_ExpirationDate AS (
	SELECT
	NurseReferralId,
	EffectiveDate,
	ExpirationDate AS orig_ExpirationDate,
	-- *INF*: DECODE(TRUE,
	-- 	NurseReferralAkId= v_PREV_ROW_NurseReferralAkId, ADD_TO_DATE(v_PREV_ROW_EffectiveDate,'SS',-1),
	-- 	orig_ExpirationDate)
	DECODE(TRUE,
		NurseReferralAkId = v_PREV_ROW_NurseReferralAkId, ADD_TO_DATE(v_PREV_ROW_EffectiveDate, 'SS', - 1),
		orig_ExpirationDate) AS v_ExpirationDate,
	v_ExpirationDate AS ExpirationDate,
	EffectiveDate AS v_PREV_ROW_EffectiveDate,
	NurseReferralAkId AS v_PREV_ROW_NurseReferralAkId,
	SYSDATE AS ModifiedDate,
	NurseReferralAkId,
	0 AS CurrentSnapshotFlag
	FROM SQ_NurseReferral
),
FIL_FirstRowAkId AS (
	SELECT
	NurseReferralId, 
	orig_ExpirationDate, 
	ExpirationDate, 
	ModifiedDate, 
	CurrentSnapshotFlag
	FROM EXP_Lag_ExpirationDate
	WHERE orig_ExpirationDate != ExpirationDate
),
UPD_NurseReferral AS (
	SELECT
	NurseReferralId, 
	ExpirationDate, 
	ModifiedDate, 
	CurrentSnapshotFlag
	FROM FIL_FirstRowAkId
),
NurseReferral_Update AS (
	MERGE INTO NurseReferral AS T
	USING UPD_NurseReferral AS S
	ON T.NurseReferralId = S.NurseReferralId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),
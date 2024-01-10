WITH
SQ_NurseReferral AS (
	SELECT
	N.NurseReferralId, 
	N.EffectiveDate, 
	N.SourceSystemId, 
	N.NurseReferralAkId, 
	N.claim_party_ak_id, 
	N.ReferralDate 
	
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseReferral N
	
	where
		N.CreatedDate >= '@{pipeline().parameters.SELECTION_START_TS}'
	AND
	      N.CurrentSnapshotFlag = 1
),
EXP_Src_Values_Default AS (
	SELECT
	EdwNurseReferralPkId,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	SourceSystemId,
	sysdate AS CreatedDate,
	sysdate AS ModifiedDate,
	NurseReferralAkId AS IN_NurseReferralAkId,
	-- *INF*: iif(isnull(IN_NurseReferralAkId), -1, IN_NurseReferralAkId)
	IFF(IN_NurseReferralAkId IS NULL, - 1, IN_NurseReferralAkId) AS v_NurseReferralAkId,
	v_NurseReferralAkId AS EdwNurseReferralAkId,
	claim_party_ak_id AS IN_claim_party_ak_id,
	-- *INF*: iif(isnull(ltrim(rtrim(IN_claim_party_ak_id))), -1, IN_claim_party_ak_id)
	IFF(ltrim(rtrim(IN_claim_party_ak_id)) IS NULL, - 1, IN_claim_party_ak_id) AS v_claim_party_ak_id,
	v_claim_party_ak_id AS claim_party_ak_id,
	ReferralDate AS IN_ReferralDate,
	-- *INF*: iif(isnull(IN_ReferralDate), to_date('1/1/1800','MM/DD/YYYY'), IN_ReferralDate)
	IFF(IN_ReferralDate IS NULL, to_date('1/1/1800', 'MM/DD/YYYY'), IN_ReferralDate) AS v_RefferalDate,
	v_RefferalDate AS ReferralDate
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
LKP_claim_party AS (
	SELECT
	claim_party_full_name,
	claim_party_first_name,
	claim_party_last_name,
	claim_party_mid_name,
	claim_party_name_prfx,
	claim_party_name_sfx,
	claim_party_ak_id
	FROM (
		SELECT
		CP.claim_party_full_name as claim_party_full_name, CP.claim_party_first_name as claim_party_first_name, CP.claim_party_last_name as claim_party_last_name, CP.claim_party_mid_name as claim_party_mid_name, CP.claim_party_name_prfx as claim_party_name_prfx, CP.claim_party_name_sfx as claim_party_name_sfx, 
		CP.claim_party_ak_id as claim_party_ak_id
		
		 FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party CP
		
		where
		CP.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_ak_id ORDER BY claim_party_full_name) = 1
),
EXP_Lkp_Records AS (
	SELECT
	LKP_NurseReferralDim.NurseReferralDimId,
	LKP_claim_party.claim_party_full_name AS NurseFullName,
	-- *INF*: decode(true,
	-- isnull(NurseFullName),'N/A',
	-- Length(NurseFullName)= 0, 'N/A',
	-- is_spaces(NurseFullName), 'N/A',
	-- ltrim(rtrim(NurseFullName)))
	decode(true,
		NurseFullName IS NULL, 'N/A',
		Length(NurseFullName) = 0, 'N/A',
		is_spaces(NurseFullName), 'N/A',
		ltrim(rtrim(NurseFullName))) AS NurseFullName1,
	LKP_claim_party.claim_party_first_name AS NurseFirstName,
	-- *INF*: decode(true,
	-- isnull(NurseFirstName),'N/A',
	-- Length(NurseFirstName)= 0, 'N/A',
	-- is_spaces(NurseFirstName), 'N/A',
	-- ltrim(rtrim(NurseFirstName)))
	decode(true,
		NurseFirstName IS NULL, 'N/A',
		Length(NurseFirstName) = 0, 'N/A',
		is_spaces(NurseFirstName), 'N/A',
		ltrim(rtrim(NurseFirstName))) AS NurseFirstName1,
	LKP_claim_party.claim_party_last_name AS NurseLastName,
	-- *INF*: decode(true,
	-- isnull(NurseLastName),'N/A',
	-- Length(NurseLastName)= 0, 'N/A',
	-- is_spaces(NurseLastName), 'N/A',
	-- ltrim(rtrim(NurseLastName)))
	decode(true,
		NurseLastName IS NULL, 'N/A',
		Length(NurseLastName) = 0, 'N/A',
		is_spaces(NurseLastName), 'N/A',
		ltrim(rtrim(NurseLastName))) AS NurseLastName1,
	LKP_claim_party.claim_party_mid_name AS NurseMiddleName,
	-- *INF*: decode(true,
	-- isnull(NurseMiddleName),'N/A',
	-- Length(NurseMiddleName)= 0, 'N/A',
	-- is_spaces(NurseMiddleName), 'N/A',
	-- ltrim(rtrim(NurseMiddleName)))
	decode(true,
		NurseMiddleName IS NULL, 'N/A',
		Length(NurseMiddleName) = 0, 'N/A',
		is_spaces(NurseMiddleName), 'N/A',
		ltrim(rtrim(NurseMiddleName))) AS NurseMiddleName1,
	LKP_claim_party.claim_party_name_prfx AS NurseNamePrefix,
	-- *INF*: decode(true,
	-- isnull(NurseNamePrefix),'N/A',
	-- Length(NurseNamePrefix)= 0, 'N/A',
	-- is_spaces(NurseNamePrefix), 'N/A',
	-- ltrim(rtrim(NurseNamePrefix)))
	decode(true,
		NurseNamePrefix IS NULL, 'N/A',
		Length(NurseNamePrefix) = 0, 'N/A',
		is_spaces(NurseNamePrefix), 'N/A',
		ltrim(rtrim(NurseNamePrefix))) AS NurseNamePrefix1,
	LKP_claim_party.claim_party_name_sfx AS NurseNameSuffix,
	-- *INF*: decode(true,
	-- isnull(NurseNameSuffix),'N/A',
	-- Length(NurseNameSuffix)= 0, 'N/A',
	-- is_spaces(NurseNameSuffix), 'N/A',
	-- ltrim(rtrim(NurseNameSuffix)))
	decode(true,
		NurseNameSuffix IS NULL, 'N/A',
		Length(NurseNameSuffix) = 0, 'N/A',
		is_spaces(NurseNameSuffix), 'N/A',
		ltrim(rtrim(NurseNameSuffix))) AS NurseNameSuffix1
	FROM 
	LEFT JOIN LKP_NurseReferralDim
	ON LKP_NurseReferralDim.EdwNurseReferralAkId = EXP_Src_Values_Default.EdwNurseReferralAkId
	LEFT JOIN LKP_claim_party
	ON LKP_claim_party.claim_party_ak_id = EXP_Src_Values_Default.claim_party_ak_id
),
RTR_Insert_Update AS (
	SELECT
	EXP_Lkp_Records.NurseReferralDimId,
	EXP_Src_Values_Default.CurrentSnapshotFlag,
	EXP_Src_Values_Default.AuditId AS AuditID,
	EXP_Src_Values_Default.EffectiveDate,
	EXP_Src_Values_Default.ExpirationDate,
	EXP_Src_Values_Default.CreatedDate,
	EXP_Src_Values_Default.ModifiedDate,
	EXP_Src_Values_Default.EdwNurseReferralPkId,
	EXP_Src_Values_Default.EdwNurseReferralAkId,
	EXP_Src_Values_Default.ReferralDate,
	EXP_Lkp_Records.NurseFullName1 AS NurseFullName,
	EXP_Lkp_Records.NurseFirstName1 AS NurseFirstName,
	EXP_Lkp_Records.NurseLastName1 AS NurseLastName,
	EXP_Lkp_Records.NurseMiddleName1 AS NurseMiddleName,
	EXP_Lkp_Records.NurseNamePrefix1 AS NurseNamePrefix,
	EXP_Lkp_Records.NurseNameSuffix1 AS NurseNameSuffix
	FROM EXP_Lkp_Records
	 -- Manually join with EXP_Src_Values_Default
),
RTR_Insert_Update_Insert AS (SELECT * FROM RTR_Insert_Update WHERE isnull(NurseReferralDimId)),
RTR_Insert_Update_DEFAULT1 AS (SELECT * FROM RTR_Insert_Update WHERE NOT ( (isnull(NurseReferralDimId)) )),
UPD_Insert AS (
	SELECT
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	CreatedDate, 
	ModifiedDate, 
	EdwNurseReferralPkId, 
	EdwNurseReferralAkId, 
	ReferralDate, 
	NurseFullName, 
	NurseFirstName, 
	NurseLastName, 
	NurseMiddleName, 
	NurseNamePrefix, 
	NurseNameSuffix
	FROM RTR_Insert_Update_Insert
),
NurseReferralDim_Insert AS (
	INSERT INTO NurseReferralDim
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, EdwNurseReferralPkId, EdwNurseReferralAkId, ReferralDate, NurseFullName, NurseFirstName, NurseLastName, NurseMiddleName, NurseNamePrefix, NurseNameSuffix)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	EDWNURSEREFERRALPKID, 
	EDWNURSEREFERRALAKID, 
	REFERRALDATE, 
	NURSEFULLNAME, 
	NURSEFIRSTNAME, 
	NURSELASTNAME, 
	NURSEMIDDLENAME, 
	NURSENAMEPREFIX, 
	NURSENAMESUFFIX
	FROM UPD_Insert
),
UPD_Update AS (
	SELECT
	NurseReferralDimId, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	CreatedDate, 
	ModifiedDate, 
	EdwNurseReferralPkId, 
	EdwNurseReferralAkId, 
	ReferralDate, 
	NurseFullName, 
	NurseFirstName, 
	NurseLastName, 
	NurseMiddleName, 
	NurseNamePrefix, 
	NurseNameSuffix
	FROM RTR_Insert_Update_DEFAULT1
),
NurseReferralDim_Update AS (
	MERGE INTO NurseReferralDim AS T
	USING UPD_Update AS S
	ON T.NurseReferralDimId = S.NurseReferralDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.AuditID = S.AuditID, T.EffectiveDate = S.EffectiveDate, T.ExpirationDate = S.ExpirationDate, T.CreatedDate = S.CreatedDate, T.ModifiedDate = S.ModifiedDate, T.EdwNurseReferralPkId = S.EdwNurseReferralPkId, T.EdwNurseReferralAkId = S.EdwNurseReferralAkId, T.ReferralDate = S.ReferralDate, T.NurseFullName = S.NurseFullName, T.NurseFirstName = S.NurseFirstName, T.NurseLastName = S.NurseLastName, T.NurseMiddleName = S.NurseMiddleName, T.NurseNamePrefix = S.NurseNamePrefix, T.NurseNameSuffix = S.NurseNameSuffix
),
SQ_NurseReferralDim AS (
	SELECT
	A.NurseReferralDimId,
	A.EffectiveDate,
	A.ExpirationDate,
	A.EdwNurseReferralAkId 
	
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseReferralDim A
	
	where Exists
	   (
	SELECT 1
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.NurseReferralDim B
	
	WHERE
	A.EdwNurseReferralAkId = B.EdwNurseReferralAkId
	AND
	B.CurrentSnapshotFlag = 1
	
	GROUP BY
	B.EdwNurseReferralAkId
	
	HAVING
	count(*) > 1
	    ) 
	
	ORDER BY
	A.EdwNurseReferralAkId,
	A.EffectiveDate DESC
),
EXP_Lag_ExpirationDate AS (
	SELECT
	NurseReferralDimId,
	0 AS CurrentSnapshotFlag,
	EffectiveDate,
	ExpirationDate AS orig_ExpirationDate,
	-- *INF*: decode(true,
	-- EdwNurseReferralAkId = v_PREV_ROW_EdwNurseReferralAkId,
	-- add_to_date(v_PREV_ROW_EffectiveDate, 'SS', -1),
	-- orig_ExpirationDate)
	decode(true,
		EdwNurseReferralAkId = v_PREV_ROW_EdwNurseReferralAkId, add_to_date(v_PREV_ROW_EffectiveDate, 'SS', - 1),
		orig_ExpirationDate) AS v_ExpirationDate,
	v_ExpirationDate AS ExpirationDate,
	sysdate AS ModifiedDate,
	EdwNurseReferralAkId,
	EffectiveDate AS v_PREV_ROW_EffectiveDate,
	EdwNurseReferralAkId AS v_PREV_ROW_EdwNurseReferralAkId
	FROM SQ_NurseReferralDim
),
FIL_FirstRowAkId AS (
	SELECT
	NurseReferralDimId, 
	CurrentSnapshotFlag, 
	orig_ExpirationDate, 
	ExpirationDate, 
	ModifiedDate
	FROM EXP_Lag_ExpirationDate
	WHERE orig_ExpirationDate != ExpirationDate
),
UPD_NurseReferralDim AS (
	SELECT
	NurseReferralDimId, 
	CurrentSnapshotFlag, 
	ExpirationDate, 
	ModifiedDate
	FROM FIL_FirstRowAkId
),
NurseReferralDim_Expire AS (
	MERGE INTO NurseReferralDim AS T
	USING UPD_NurseReferralDim AS S
	ON T.NurseReferralDimId = S.NurseReferralDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),
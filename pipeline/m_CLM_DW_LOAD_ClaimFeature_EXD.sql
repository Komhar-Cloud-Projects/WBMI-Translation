WITH
SQ_Clm_Feature_Staging AS (
	SELECT
		SourceSystemId,
		Tch_Claim_Nbr,
		Tch_Client_Id,
		Cov_Type_Cd,
		Bur_Cause_Loss,
		Adjuster_Client_Id,
		Created_TS,
		Modified_TS
	FROM Clm_Feature_Staging
),
EXP_Validate_Source_Values AS (
	SELECT
	Tch_Claim_Nbr,
	Tch_Client_Id,
	Cov_Type_Cd,
	Bur_Cause_Loss,
	Adjuster_Client_Id AS in_Adjuster_Client_Id,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_Adjuster_Client_Id)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_Adjuster_Client_Id) AS out_Adjuster_Client_Id,
	Created_TS,
	Modified_TS,
	SourceSystemId
	FROM SQ_Clm_Feature_Staging
),
LKP_FeatureRepAkId AS (
	SELECT
	claim_rep_ak_id,
	claim_rep_key,
	source_sys_id
	FROM (
		SELECT 
			claim_rep_ak_id,
			claim_rep_key,
			source_sys_id
		FROM claim_representative
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_key,source_sys_id ORDER BY claim_rep_ak_id) = 1
),
mplt_claim_party_occurrence_Claimant AS (WITH
	INPUT AS (
		
	),
	EXP_SetClaimantRoleCode AS (
		SELECT
		claim_occurrence_key,
		claim_party_key,
		source_sys_id,
		-- *INF*: DECODE(source_sys_id,
		-- 	'EXCEED', 'CLMT',
		-- 	'PMS', 'CMT',
		-- 	NULL)
		DECODE(source_sys_id,
			'EXCEED', 'CLMT',
			'PMS', 'CMT',
			NULL) AS ClaimantRoleCode
		FROM INPUT
	),
	LKP_Claim_Party_Occurrence_AK_ID AS (
		SELECT
		claim_party_occurrence_ak_id,
		claim_occurrence_key,
		claim_party_key,
		claim_party_role_code
		FROM (
			SELECT 
			CPO.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, 
			LTRIM(RTRIM(CO.claim_occurrence_key)) as claim_occurrence_key, 
			LTRIM(RTRIM(CP.claim_party_key)) as claim_party_key,
			LTRIM(RTRIM(CPO.claim_party_role_code)) as claim_party_role_code
			FROM 
			@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence CPO,
			@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party CP, 
			@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence CO 
			WHERE 
			CO.claim_occurrence_ak_id = CPO.claim_occurrence_ak_id  
			AND CP.claim_party_ak_id = CPO.claim_party_ak_id 
			AND CO.crrnt_snpsht_flag = 1
			AND CP.crrnt_snpsht_flag = 1
			AND CPO.crrnt_snpsht_flag = 1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_key,claim_party_key,claim_party_role_code ORDER BY claim_party_occurrence_ak_id) = 1
	),
	OUTPUT AS (
		SELECT
		claim_party_occurrence_ak_id
		FROM LKP_Claim_Party_Occurrence_AK_ID
	),
),
EXP_Lookup_Values AS (
	SELECT
	mplt_claim_party_occurrence_Claimant.claim_party_occurrence_ak_id,
	EXP_Validate_Source_Values.Cov_Type_Cd AS in_MajorPerilCode,
	-- *INF*: SUBSTR(in_MajorPerilCode,1,3)
	SUBSTR(in_MajorPerilCode, 1, 3) AS out_MajorPerilCode,
	EXP_Validate_Source_Values.Bur_Cause_Loss AS in_Bur_Cause_Loss,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(SUBSTR(in_Bur_Cause_Loss,1,2))
	:UDF.DEFAULT_VALUE_FOR_STRINGS(SUBSTR(in_Bur_Cause_Loss, 1, 2)) AS CauseOfLoss,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(SUBSTR(in_Bur_Cause_Loss,3,1))
	:UDF.DEFAULT_VALUE_FOR_STRINGS(SUBSTR(in_Bur_Cause_Loss, 3, 1)) AS ReserveCategory,
	EXP_Validate_Source_Values.Created_TS,
	EXP_Validate_Source_Values.Modified_TS,
	LKP_FeatureRepAkId.claim_rep_ak_id AS in_FeatureRepresentativeAkId,
	-- *INF*: IIF(ISNULL(in_FeatureRepresentativeAkId),
	-- 	-1,
	-- 	in_FeatureRepresentativeAkId)
	IFF(in_FeatureRepresentativeAkId IS NULL, - 1, in_FeatureRepresentativeAkId) AS FeatureRepresentativeAkId,
	-- *INF*: DECODE(TRUE,
	-- 	ISNULL(in_FeatureRepresentativeAkId), TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	-- 	NOT ISNULL(Modified_TS), Modified_TS,
	-- 	NOT ISNULL(Created_TS), Created_TS,
	-- 	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'))
	DECODE(TRUE,
		in_FeatureRepresentativeAkId IS NULL, TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
		NOT Modified_TS IS NULL, Modified_TS,
		NOT Created_TS IS NULL, Created_TS,
		TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')) AS FeatureRepresentativeAssignedDate,
	EXP_Validate_Source_Values.SourceSystemId
	FROM EXP_Validate_Source_Values
	 -- Manually join with mplt_claim_party_occurrence_Claimant
	LEFT JOIN LKP_FeatureRepAkId
	ON LKP_FeatureRepAkId.claim_rep_key = EXP_Validate_Source_Values.out_Adjuster_Client_Id AND LKP_FeatureRepAkId.source_sys_id = EXP_Validate_Source_Values.SourceSystemId
),
LKP_ClaimFeature AS (
	SELECT
	ClaimFeatureId,
	CurrentSnapshotFlag,
	EffectiveDate,
	ExpirationDate,
	SourceSystemId,
	CreatedDate,
	ModifiedDate,
	ClaimFeatureAKId,
	ClaimPartyOccurrenceAKId,
	MajorPerilCode,
	ReserveCategory,
	CauseOfLoss,
	ClaimRepresentativeAkId,
	FeatureRepresentativeAssignedDate
	FROM (
		SELECT 
			ClaimFeatureId,
			CurrentSnapshotFlag,
			EffectiveDate,
			ExpirationDate,
			SourceSystemId,
			CreatedDate,
			ModifiedDate,
			ClaimFeatureAKId,
			ClaimPartyOccurrenceAKId,
			MajorPerilCode,
			ReserveCategory,
			CauseOfLoss,
			ClaimRepresentativeAkId,
			FeatureRepresentativeAssignedDate
		FROM ClaimFeature
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClaimPartyOccurrenceAKId,MajorPerilCode,CauseOfLoss,ReserveCategory,SourceSystemId ORDER BY ClaimFeatureId) = 1
),
EXP_Detect_Changes AS (
	SELECT
	EXP_Lookup_Values.claim_party_occurrence_ak_id,
	EXP_Lookup_Values.out_MajorPerilCode AS MajorPerilCode,
	EXP_Lookup_Values.CauseOfLoss,
	EXP_Lookup_Values.ReserveCategory,
	EXP_Lookup_Values.FeatureRepresentativeAkId,
	EXP_Lookup_Values.FeatureRepresentativeAssignedDate,
	EXP_Lookup_Values.SourceSystemId,
	LKP_ClaimFeature.ClaimFeatureAKId AS lkp_ClaimFeatureAKId,
	LKP_ClaimFeature.ClaimRepresentativeAkId AS lkp_ClaimRepresentativeAkId,
	-- *INF*: IIF(ISNULL(lkp_ClaimFeatureAKId),
	-- 	'NEW',	
	-- 	IIF(lkp_ClaimRepresentativeAkId != FeatureRepresentativeAkId,
	-- 		'UPDATE',
	-- 		'NOCHANGE'))
	IFF(lkp_ClaimFeatureAKId IS NULL, 'NEW', IFF(lkp_ClaimRepresentativeAkId != FeatureRepresentativeAkId, 'UPDATE', 'NOCHANGE')) AS v_ChangedFlag,
	v_ChangedFlag AS ChangedFlag
	FROM EXP_Lookup_Values
	LEFT JOIN LKP_ClaimFeature
	ON LKP_ClaimFeature.ClaimPartyOccurrenceAKId = EXP_Lookup_Values.claim_party_occurrence_ak_id AND LKP_ClaimFeature.MajorPerilCode = EXP_Lookup_Values.out_MajorPerilCode AND LKP_ClaimFeature.CauseOfLoss = EXP_Lookup_Values.CauseOfLoss AND LKP_ClaimFeature.ReserveCategory = EXP_Lookup_Values.ReserveCategory AND LKP_ClaimFeature.SourceSystemId = EXP_Lookup_Values.SourceSystemId
),
FIL_Insert AS (
	SELECT
	claim_party_occurrence_ak_id, 
	MajorPerilCode, 
	CauseOfLoss, 
	ReserveCategory, 
	FeatureRepresentativeAkId, 
	FeatureRepresentativeAssignedDate, 
	SourceSystemId, 
	lkp_ClaimFeatureAKId, 
	ChangedFlag
	FROM EXP_Detect_Changes
	WHERE ChangedFlag = 'NEW' OR ChangedFlag = 'UPDATE'
),
SEQ_ClaimFeature AS (
	CREATE SEQUENCE SEQ_ClaimFeature
	START = 0
	INCREMENT = 1;
),
EXP_Insert_Target AS (
	SELECT
	lkp_ClaimFeatureAKId,
	SEQ_ClaimFeature.NEXTVAL,
	ChangedFlag,
	-- *INF*: IIF(ChangedFlag='NEW',
	-- 	NEXTVAL,
	-- 	lkp_ClaimFeatureAKId)
	IFF(ChangedFlag = 'NEW', NEXTVAL, lkp_ClaimFeatureAKId) AS ClaimFeatureAkId,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	-- *INF*: IIF(ChangedFlag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	SYSDATE)
	IFF(ChangedFlag = 'NEW', TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	SourceSystemId,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	claim_party_occurrence_ak_id,
	MajorPerilCode,
	CauseOfLoss,
	ReserveCategory,
	FeatureRepresentativeAkId,
	FeatureRepresentativeAssignedDate
	FROM FIL_Insert
),
ClaimFeature_Insert AS (
	INSERT INTO ClaimFeature
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, ClaimFeatureAKId, ClaimPartyOccurrenceAKId, MajorPerilCode, ReserveCategory, CauseOfLoss, ClaimRepresentativeAkId, FeatureRepresentativeAssignedDate)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	ClaimFeatureAkId AS CLAIMFEATUREAKID, 
	claim_party_occurrence_ak_id AS CLAIMPARTYOCCURRENCEAKID, 
	MAJORPERILCODE, 
	RESERVECATEGORY, 
	CAUSEOFLOSS, 
	FeatureRepresentativeAkId AS CLAIMREPRESENTATIVEAKID, 
	FEATUREREPRESENTATIVEASSIGNEDDATE
	FROM EXP_Insert_Target
),
SQ_ClaimFeature AS (
	SELECT a.ClaimFeatureId, a.EffectiveDate, a.ExpirationDate, a.ClaimFeatureAKId
	FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.ClaimFeature a
	WHERE a.SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
	and EXISTS(SELECT 1			
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.ClaimFeature b
			WHERE b.SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND b.CurrentSnapshotFlag = 1
			AND b.ClaimFeatureAKId = a.ClaimFeatureAKId
			GROUP BY b.ClaimFeatureAKId
			HAVING COUNT(*) > 1)
	ORDER BY a.ClaimFeatureAKId, a.EffectiveDate DESC--
),
EXP_Lag_Eff_From_Date AS (
	SELECT
	ClaimFeatureId,
	EffectiveDate,
	ExpirationDate AS orig_ExpirationDate,
	ClaimFeatureAKId,
	-- *INF*: DECODE(TRUE,
	-- 	ClaimFeatureAKId = v_PREV_ROW_ClaimFeatureAKId, ADD_TO_DATE(v_PREV_ROW_EffectiveDate,'SS',-1),
	-- 	orig_ExpirationDate)
	DECODE(TRUE,
		ClaimFeatureAKId = v_PREV_ROW_ClaimFeatureAKId, ADD_TO_DATE(v_PREV_ROW_EffectiveDate, 'SS', - 1),
		orig_ExpirationDate) AS v_ExpirationDate,
	v_ExpirationDate AS ExpirationDate,
	EffectiveDate AS v_PREV_ROW_EffectiveDate,
	ClaimFeatureAKId AS v_PREV_ROW_ClaimFeatureAKId,
	SYSDATE AS ModifiedDate,
	0 AS CurrentSnapshotFlag
	FROM SQ_ClaimFeature
),
FIL_RowsToExpire AS (
	SELECT
	ClaimFeatureId, 
	orig_ExpirationDate, 
	ExpirationDate, 
	ModifiedDate, 
	CurrentSnapshotFlag
	FROM EXP_Lag_Eff_From_Date
	WHERE orig_ExpirationDate <> ExpirationDate
),
UPD_ClaimFeature_Expire AS (
	SELECT
	ClaimFeatureId, 
	ExpirationDate, 
	ModifiedDate, 
	CurrentSnapshotFlag
	FROM FIL_RowsToExpire
),
ClaimFeature_Update AS (
	MERGE INTO ClaimFeature AS T
	USING UPD_ClaimFeature_Expire AS S
	ON T.ClaimFeatureId = S.ClaimFeatureId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),
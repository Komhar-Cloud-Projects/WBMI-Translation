WITH
SQ_CLAIM_TAB_STAGE AS (
	SELECT
		clm_claim_nbr,
		clm_survey_recipient,
		clm_survey_contact_method,
		clm_survey_primary_handler
	FROM CLAIM_TAB_STAGE
),
EXP_Source AS (
	SELECT
	clm_claim_nbr,
	clm_survey_recipient,
	clm_survey_contact_method,
	clm_survey_primary_handler,
	'Closed Claim Survey' AS o_SurveyType
	FROM SQ_CLAIM_TAB_STAGE
),
LKP_SupClaimSurveyContactMethod AS (
	SELECT
	SurveyContactMethodDescription,
	SurveyContactMethodCode
	FROM (
		SELECT 
			SurveyContactMethodDescription,
			SurveyContactMethodCode
		FROM SupClaimSurveyContactMethod
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SurveyContactMethodCode ORDER BY SurveyContactMethodDescription) = 1
),
LKP_SupClaimSurveyType AS (
	SELECT
	SupClaimSurveyTypeId,
	ClaimSurveyTypeDescription
	FROM (
		select SupClaimSurveyTypeId as SupClaimSurveyTypeId,
		ClaimSurveyTypeDescription as ClaimSurveyTypeDescription 
		from dbo.SupClaimSurveyType
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClaimSurveyTypeDescription ORDER BY SupClaimSurveyTypeId) = 1
),
LKP_claim_occurrence AS (
	SELECT
	claim_occurrence_ak_id,
	claim_occurrence_key
	FROM (
		select claim_occurrence_ak_id as claim_occurrence_ak_id,
		RTRIM(claim_occurrence_key) as claim_occurrence_key
		FROM dbo.claim_occurrence
		WHERE crrnt_snpsht_flag = 1
		and source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_key ORDER BY claim_occurrence_ak_id) = 1
),
LKP_claim_party AS (
	SELECT
	claim_party_ak_id,
	claim_party_key
	FROM (
		select cp.claim_party_ak_id as claim_party_ak_id, 
		RTRIM(cp.claim_party_key) as claim_party_key 
		from dbo.claim_party cp 
		INNER JOIN @{pipeline().parameters.STAGE_DB_NAME}.DBO.claim_tab_stage cts ON cp.claim_party_key = cts.clm_survey_recipient
		where cp.crrnt_snpsht_flag = 1
		and cp.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_key ORDER BY claim_party_ak_id) = 1
),
LKP_claim_representative AS (
	SELECT
	claim_rep_ak_id,
	claim_rep_key
	FROM (
		select claim_rep_ak_id as claim_rep_ak_id, 
		claim_rep_key as claim_rep_key
		from dbo.claim_representative
		where crrnt_snpsht_flag = 1
		and source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_key ORDER BY claim_rep_ak_id) = 1
),
EXP_LookupValues AS (
	SELECT
	LKP_claim_occurrence.claim_occurrence_ak_id AS lkp_claim_occurrence_ak_id,
	-- *INF*: IIF(ISNULL(lkp_claim_occurrence_ak_id),
	-- -1,
	-- lkp_claim_occurrence_ak_id)
	IFF(lkp_claim_occurrence_ak_id IS NULL, - 1, lkp_claim_occurrence_ak_id) AS claim_occurrence_ak_id,
	LKP_SupClaimSurveyType.SupClaimSurveyTypeId AS lkp_SupClaimSurveyTypeId,
	-- *INF*: IIF(ISNULL(lkp_SupClaimSurveyTypeId),
	-- 1,
	-- lkp_SupClaimSurveyTypeId)
	IFF(lkp_SupClaimSurveyTypeId IS NULL, 1, lkp_SupClaimSurveyTypeId) AS SupClaimSurveyTypeId,
	LKP_claim_party.claim_party_ak_id AS lkp_recipient_claim_party_ak_id,
	-- *INF*: IIF(ISNULL(lkp_recipient_claim_party_ak_id),
	-- -1,
	-- lkp_recipient_claim_party_ak_id)
	IFF(lkp_recipient_claim_party_ak_id IS NULL, - 1, lkp_recipient_claim_party_ak_id) AS recipient_claim_party_ak_id,
	LKP_SupClaimSurveyContactMethod.SurveyContactMethodDescription AS lkp_SurveyContactMethodDescription,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(lkp_SurveyContactMethodDescription)
	UDF_DEFAULT_VALUE_FOR_STRINGS(lkp_SurveyContactMethodDescription) AS SurveyContactMethodDescription,
	LKP_claim_representative.claim_rep_ak_id AS lkp_claim_rep_ak_id,
	-- *INF*: IIF(ISNULL(lkp_claim_rep_ak_id),
	-- -3,
	-- lkp_claim_rep_ak_id)
	IFF(lkp_claim_rep_ak_id IS NULL, - 3, lkp_claim_rep_ak_id) AS claim_rep_ak_id
	FROM 
	LEFT JOIN LKP_SupClaimSurveyContactMethod
	ON LKP_SupClaimSurveyContactMethod.SurveyContactMethodCode = EXP_Source.clm_survey_contact_method
	LEFT JOIN LKP_SupClaimSurveyType
	ON LKP_SupClaimSurveyType.ClaimSurveyTypeDescription = EXP_Source.o_SurveyType
	LEFT JOIN LKP_claim_occurrence
	ON LKP_claim_occurrence.claim_occurrence_key = EXP_Source.clm_claim_nbr
	LEFT JOIN LKP_claim_party
	ON LKP_claim_party.claim_party_key = EXP_Source.clm_survey_recipient
	LEFT JOIN LKP_claim_representative
	ON LKP_claim_representative.claim_rep_key = EXP_Source.clm_survey_primary_handler
),
LKP_ClaimOccurrenceSurvey AS (
	SELECT
	ClaimOccurrenceSurveyId,
	RecipientClaimPartyAKID,
	SurveyContactMethodDescription,
	AdjusterClaimRepresentativeAKID,
	ClaimOccurrenceAKID,
	SupClaimSurveyTypeId
	FROM (
		select ClaimOccurrenceSurveyId as ClaimOccurrenceSurveyId,
			ClaimOccurrenceAKID as ClaimOccurrenceAKID,
			SupClaimSurveyTypeId as SupClaimSurveyTypeId,
			RecipientClaimPartyAKID as RecipientClaimPartyAKID,
			SurveyContactMethodDescription as SurveyContactMethodDescription,
			AdjusterClaimRepresentativeAKID as AdjusterClaimRepresentativeAKID
		from dbo.ClaimOccurrenceSurvey
		where CurrentSnapshotFlag = 1
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClaimOccurrenceAKID,SupClaimSurveyTypeId ORDER BY ClaimOccurrenceSurveyId) = 1
),
EXP_DetectChanges AS (
	SELECT
	LKP_ClaimOccurrenceSurvey.ClaimOccurrenceSurveyId AS lkp_ClaimOccurrenceSurveyId,
	EXP_LookupValues.claim_occurrence_ak_id,
	EXP_LookupValues.SupClaimSurveyTypeId,
	LKP_ClaimOccurrenceSurvey.RecipientClaimPartyAKID AS lkp_RecipientClaimPartyAKID,
	LKP_ClaimOccurrenceSurvey.SurveyContactMethodDescription AS lkp_SurveyContactMethodDescription,
	LKP_ClaimOccurrenceSurvey.AdjusterClaimRepresentativeAKID AS lkp_AdjusterClaimRepresentativeAKID,
	EXP_LookupValues.recipient_claim_party_ak_id,
	EXP_LookupValues.SurveyContactMethodDescription,
	EXP_LookupValues.claim_rep_ak_id,
	-- *INF*: DECODE(TRUE,
	-- 	ISNULL(lkp_ClaimOccurrenceSurveyId) 
	-- 	AND recipient_claim_party_ak_id = -1 
	-- 	AND SurveyContactMethodDescription = 'N/A' 
	-- 	AND claim_rep_ak_id < 0,
	-- 		'IGNORE',
	-- 	ISNULL(lkp_ClaimOccurrenceSurveyId), 
	-- 		'NEW',
	-- 	lkp_RecipientClaimPartyAKID <> recipient_claim_party_ak_id
	-- 	OR lkp_SurveyContactMethodDescription <> SurveyContactMethodDescription
	-- 	OR lkp_AdjusterClaimRepresentativeAKID <> claim_rep_ak_id,
	-- 		'UPDATE',
	-- 	'NOCHANGE'
	-- 	)
	DECODE(
	    TRUE,
	    lkp_ClaimOccurrenceSurveyId IS NULL AND recipient_claim_party_ak_id = - 1 AND SurveyContactMethodDescription = 'N/A' AND claim_rep_ak_id < 0, 'IGNORE',
	    lkp_ClaimOccurrenceSurveyId IS NULL, 'NEW',
	    lkp_RecipientClaimPartyAKID <> recipient_claim_party_ak_id OR lkp_SurveyContactMethodDescription <> SurveyContactMethodDescription OR lkp_AdjusterClaimRepresentativeAKID <> claim_rep_ak_id, 'UPDATE',
	    'NOCHANGE'
	) AS v_ChangeFlag,
	v_ChangeFlag AS o_ChangeFlag,
	-- *INF*: IIF(v_ChangeFlag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	SYSDATE)
	IFF(
	    v_ChangeFlag = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS EffectiveFromDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS EffectiveToDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CurrentTime
	FROM EXP_LookupValues
	LEFT JOIN LKP_ClaimOccurrenceSurvey
	ON LKP_ClaimOccurrenceSurvey.ClaimOccurrenceAKID = EXP_LookupValues.claim_occurrence_ak_id AND LKP_ClaimOccurrenceSurvey.SupClaimSurveyTypeId = EXP_LookupValues.SupClaimSurveyTypeId
),
FIL_NewOrChanged AS (
	SELECT
	claim_occurrence_ak_id, 
	SupClaimSurveyTypeId, 
	o_ChangeFlag, 
	EffectiveFromDate, 
	EffectiveToDate, 
	SourceSystemID, 
	CurrentSnapshotFlag, 
	AuditId, 
	CurrentTime, 
	recipient_claim_party_ak_id, 
	SurveyContactMethodDescription, 
	claim_rep_ak_id
	FROM EXP_DetectChanges
	WHERE o_ChangeFlag='NEW' OR o_ChangeFlag='UPDATE'
),
EXP_Target AS (
	SELECT
	claim_occurrence_ak_id,
	SupClaimSurveyTypeId,
	EffectiveFromDate,
	EffectiveToDate,
	SourceSystemID,
	CurrentSnapshotFlag,
	AuditId,
	CurrentTime,
	recipient_claim_party_ak_id,
	SurveyContactMethodDescription,
	claim_rep_ak_id
	FROM FIL_NewOrChanged
),
ClaimOccurrenceSurvey_Insert AS (
	INSERT INTO ClaimOccurrenceSurvey
	(ClaimOccurrenceAKID, SupClaimSurveyTypeId, EffectiveFromDate, EffectiveToDate, SourceSystemID, CurrentSnapshotFlag, AuditID, CreatedDate, ModifiedDate, RecipientClaimPartyAKID, SurveyContactMethodDescription, AdjusterClaimRepresentativeAKID)
	SELECT 
	claim_occurrence_ak_id AS CLAIMOCCURRENCEAKID, 
	SUPCLAIMSURVEYTYPEID, 
	EFFECTIVEFROMDATE, 
	EFFECTIVETODATE, 
	SOURCESYSTEMID, 
	CURRENTSNAPSHOTFLAG, 
	AuditId AS AUDITID, 
	CurrentTime AS CREATEDDATE, 
	CurrentTime AS MODIFIEDDATE, 
	recipient_claim_party_ak_id AS RECIPIENTCLAIMPARTYAKID, 
	SURVEYCONTACTMETHODDESCRIPTION, 
	claim_rep_ak_id AS ADJUSTERCLAIMREPRESENTATIVEAKID
	FROM EXP_Target
),
SQ_ClaimOccurrenceSurvey AS (
	SELECT a.ClaimOccurrenceSurveyId, 
		a.ClaimOccurrenceAKID, 
		a.SupClaimSurveyTypeId,
		a.EffectiveFromDate, 
		a.EffectiveToDate 
	FROM dbo.ClaimOccurrenceSurvey a
	WHERE a.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND EXISTS(SELECT 1			
			FROM dbo.ClaimOccurrenceSurvey b
			WHERE b.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
				AND b.CurrentSnapshotFlag = 1
				AND a.ClaimOccurrenceAKID = b.ClaimOccurrenceAKID
				AND a.SupClaimSurveyTypeId = b.SupClaimSurveyTypeId
			GROUP BY b.ClaimOccurrenceAKID, b.SupClaimSurveyTypeId
			HAVING COUNT(*) > 1)
	ORDER BY a.ClaimOccurrenceAKID, a.SupClaimSurveyTypeId, a.EffectiveFromDate DESC
	--
),
EXP_EffectiveToDate AS (
	SELECT
	ClaimOccurrenceSurveyId,
	ClaimOccurrenceAKID,
	SupClaimSurveyTypeId,
	EffectiveFromDate,
	EffectiveToDate AS OriginalEffectiveToDate,
	-- *INF*: DECODE(TRUE,
	-- 	ClaimOccurrenceAKID = v_PREV_ROW_ClaimOccurrenceAKID AND SupClaimSurveyTypeId = v_PREV_ROW_SupClaimSurveyTypeId, 
	-- 		ADD_TO_DATE(v_PREV_ROW_EffectiveFromDate,'SS',-1),
	-- 	OriginalEffectiveToDate)
	DECODE(
	    TRUE,
	    ClaimOccurrenceAKID = v_PREV_ROW_ClaimOccurrenceAKID AND SupClaimSurveyTypeId = v_PREV_ROW_SupClaimSurveyTypeId, DATEADD(SECOND,- 1,v_PREV_ROW_EffectiveFromDate),
	    OriginalEffectiveToDate
	) AS v_EffectiveToDate,
	v_EffectiveToDate AS o_EffectiveToDate,
	ClaimOccurrenceAKID AS v_PREV_ROW_ClaimOccurrenceAKID,
	SupClaimSurveyTypeId AS v_PREV_ROW_SupClaimSurveyTypeId,
	EffectiveFromDate AS v_PREV_ROW_EffectiveFromDate,
	SYSDATE AS ModifiedDate,
	0 AS CurrentSnapshotFlag
	FROM SQ_ClaimOccurrenceSurvey
),
FIL_RowsToExpire AS (
	SELECT
	ClaimOccurrenceSurveyId, 
	OriginalEffectiveToDate, 
	o_EffectiveToDate AS NewEffectiveToDate, 
	ModifiedDate, 
	CurrentSnapshotFlag
	FROM EXP_EffectiveToDate
	WHERE OriginalEffectiveToDate <> NewEffectiveToDate
),
UPDTRANS AS (
	SELECT
	ClaimOccurrenceSurveyId, 
	NewEffectiveToDate AS EffectiveToDate, 
	ModifiedDate, 
	CurrentSnapshotFlag
	FROM FIL_RowsToExpire
),
ClaimOccurrenceSurvey_Update AS (
	MERGE INTO ClaimOccurrenceSurvey AS T
	USING UPDTRANS AS S
	ON T.ClaimOccurrenceSurveyId = S.ClaimOccurrenceSurveyId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.EffectiveToDate = S.EffectiveToDate, T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ModifiedDate = S.ModifiedDate
),
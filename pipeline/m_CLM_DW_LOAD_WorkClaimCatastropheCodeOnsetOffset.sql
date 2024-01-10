WITH
SQ_cat_code AS (
	select  distinct CPO.claim_party_occurrence_ak_id as ClaimPartyOccurrenceAKID,
	CPO.claim_occurrence_ak_id as ClaimOccurrenceAKID,
	CO.claim_occurrence_key as ClaimOccurrenceKey, 
	CP.claim_party_key as ClaimantPartyKey, 
	CO2.claim_cat_code as OldClaimCatastropheCode,
	CO.claim_cat_code as NewClaimCatastropheCode, 
	CO.s3p_claim_updated_date ClaimUpdateDate
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence CO
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence CO2 
	on CO.claim_occurrence_ak_id=CO2.claim_occurrence_ak_id and CO.claim_cat_code<>CO2.claim_cat_code
	and CO.crrnt_snpsht_flag=1 and dateadd(ss, 1, CO2.eff_to_date)=CO.eff_from_date 
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence CPO
	on CPO.claim_occurrence_ak_id=CO.claim_occurrence_ak_id and CPO.crrnt_snpsht_flag=1 and CPO.claim_party_role_code = 'CLMT'
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party CP
	on CPO.claim_party_ak_id=CP.claim_party_ak_id and CP.crrnt_snpsht_flag=1
	where CO.created_date>='@{pipeline().parameters.SELECTION_START_TS}'   and CO.source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	and exists (select 1from @{pipeline().parameters.STAGE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_tab_stage where CO.claim_occurrence_key=CLM_CLAIM_NBR )
	@{pipeline().parameters.WHERE_CLAUSE}
	UNION
	select  distinct CPO.claim_party_occurrence_ak_id as ClaimPartyOccurrenceAKID,
	CPO.claim_occurrence_ak_id as ClaimOccurrenceAKID,
	CO.claim_occurrence_key as ClaimOccurrenceKey,
	CP.claim_party_key as ClaimantPartyKey,
	CO2.wc_cat_code  as OldClaimCatastropheCode,
	CO.wc_cat_code  as NewClaimCatastropheCode,
	CO.s3p_claim_updated_date ClaimUpdateDate
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence CO
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence CO2
	on CO.claim_occurrence_ak_id=CO2.claim_occurrence_ak_id
	and co.claim_occurrence_type_code = 'WCC'
	and CO2.wc_cat_code <> CO.wc_cat_code
	and CO.crrnt_snpsht_flag=1 and dateadd(ss, 1, CO2.eff_to_date)=CO.eff_from_date
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence CPO
	on CPO.claim_occurrence_ak_id=CO.claim_occurrence_ak_id and CPO.crrnt_snpsht_flag=1 and CPO.claim_party_role_code = 'CLMT'
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party CP
	on CPO.claim_party_ak_id=CP.claim_party_ak_id and CP.crrnt_snpsht_flag=1
	where CO.created_date>='@{pipeline().parameters.SELECTION_END_TS}'   and CO.source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Record_Changes AS (
	SELECT
	ClaimPartyOccurrenceAKID,
	ClaimOccurrenceAKID,
	ClaimOccurrenceKey,
	ClaimantPartyKey,
	OldClaimCatastropheCode,
	NewClaimCatastropheCode,
	ClaimUpdateDate,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CURRENT_TIMESTAMP AS o_CreatedDate,
	CURRENT_TIMESTAMP AS o_ModifiedDate
	FROM SQ_cat_code
),
LKP_WorkClaimCatastropheCodeOnsetOffset AS (
	SELECT
	WorkClaimCatastropheCodeOnsetOffsetId,
	ClaimPartyOccurrenceAKID,
	OldClaimCatastropheCode,
	NewClaimCatastropheCode,
	ClaimUpdateDate,
	i_ClaimPartyOccurrenceAKID,
	i_OldClaimCatastropheCode,
	i_NewClaimCatastropheCode,
	i_ClaimUpdateDate
	FROM (
		SELECT 
			WorkClaimCatastropheCodeOnsetOffsetId,
			ClaimPartyOccurrenceAKID,
			OldClaimCatastropheCode,
			NewClaimCatastropheCode,
			ClaimUpdateDate,
			i_ClaimPartyOccurrenceAKID,
			i_OldClaimCatastropheCode,
			i_NewClaimCatastropheCode,
			i_ClaimUpdateDate
		FROM WorkClaimCatastropheCodeOnsetOffset
		WHERE exists (select 1 from  @{pipeline().parameters.STAGE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_tab_stage where WorkClaimCatastropheCodeOnsetOffset.ClaimOccurrenceKey=CLM_CLAIM_NBR )
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClaimPartyOccurrenceAKID,OldClaimCatastropheCode,NewClaimCatastropheCode,ClaimUpdateDate ORDER BY WorkClaimCatastropheCodeOnsetOffsetId) = 1
),
FIL_Existing AS (
	SELECT
	LKP_WorkClaimCatastropheCodeOnsetOffset.WorkClaimCatastropheCodeOnsetOffsetId AS LKP_work_claim_cat_code_onset_offset_id, 
	EXP_Record_Changes.o_CurrentSnapshotFlag AS CurrentSnapshotFlag, 
	EXP_Record_Changes.o_AuditId AS AuditId, 
	EXP_Record_Changes.o_SourceSystemId AS SourceSystemId, 
	EXP_Record_Changes.o_CreatedDate AS CreatedDate, 
	EXP_Record_Changes.o_ModifiedDate AS ModifiedDate, 
	EXP_Record_Changes.ClaimPartyOccurrenceAKID, 
	EXP_Record_Changes.ClaimOccurrenceAKID, 
	EXP_Record_Changes.ClaimOccurrenceKey, 
	EXP_Record_Changes.ClaimantPartyKey, 
	EXP_Record_Changes.OldClaimCatastropheCode, 
	EXP_Record_Changes.NewClaimCatastropheCode, 
	EXP_Record_Changes.ClaimUpdateDate
	FROM EXP_Record_Changes
	LEFT JOIN LKP_WorkClaimCatastropheCodeOnsetOffset
	ON LKP_WorkClaimCatastropheCodeOnsetOffset.ClaimPartyOccurrenceAKID = EXP_Record_Changes.ClaimPartyOccurrenceAKID AND LKP_WorkClaimCatastropheCodeOnsetOffset.OldClaimCatastropheCode = EXP_Record_Changes.OldClaimCatastropheCode AND LKP_WorkClaimCatastropheCodeOnsetOffset.NewClaimCatastropheCode = EXP_Record_Changes.NewClaimCatastropheCode AND LKP_WorkClaimCatastropheCodeOnsetOffset.ClaimUpdateDate = EXP_Record_Changes.ClaimUpdateDate
	WHERE ISNULL(LKP_work_claim_cat_code_onset_offset_id)
),
WorkClaimCatastropheCodeOnsetOffset AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkClaimCatastropheCodeOnsetOffset
	(CurrentSnapshotFlag, AuditId, SourceSystemId, CreatedDate, ModifiedDate, ClaimPartyOccurrenceAKID, ClaimOccurrenceAKID, ClaimOccurrenceKey, ClaimantPartyKey, OldClaimCatastropheCode, NewClaimCatastropheCode, ClaimUpdateDate)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	CLAIMPARTYOCCURRENCEAKID, 
	CLAIMOCCURRENCEAKID, 
	CLAIMOCCURRENCEKEY, 
	CLAIMANTPARTYKEY, 
	OLDCLAIMCATASTROPHECODE, 
	NEWCLAIMCATASTROPHECODE, 
	CLAIMUPDATEDATE
	FROM FIL_Existing
),
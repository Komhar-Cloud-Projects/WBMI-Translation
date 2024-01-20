WITH
LKP_Get_Max_Sapiens_SourceSequenceNumber AS (
	SELECT
	Source_Seq_Num,
	ID
	FROM (
		SELECT MAX(A.SourceSequenceNumber) AS Source_Seq_Num,
			1 AS ID
		FROM (
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SapiensReinsurancePolicy
			UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsurancePolicy
			UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SapiensReinsuranceClaim
			UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceClaim
			UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SapiensReinsuranceClaimRestate
			UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceClaimRestate
		       UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SapiensReinsurancePolicyRestate
		       UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsurancePolicyRestate	) A
			--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ID ORDER BY Source_Seq_Num DESC) = 1
),
SQ_claim_occurrence_dim AS (
	-- Retrieve all values since they might be useful for audit purposes
	select distinct FINAL.SapiensClaimNumber, 
		FINAL.prev_loss_date, FINAL.curr_loss_date, FINAL.prev_cat_code, FINAL.curr_cat_code, FINAL.prev_rel_claim_id, FINAL.curr_rel_claim_id, FINAL.prev_pol_key, FINAL.curr_pol_key,
		 FINAL.Rank_value
	from (select RelevantChanges.SapiensClaimNumber, 
			FIRST_VALUE(RelevantChanges.prev_loss_date) over (partition by SapiensClaimNumber order by RelevantChanges.NegateDate) as prev_loss_date,
			LAST_VALUE(RelevantChanges.curr_loss_date) over (partition by SapiensClaimNumber order by RelevantChanges.NegateDate) as curr_loss_date,
			FIRST_VALUE(RelevantChanges.prev_cat_code) over (partition by SapiensClaimNumber order by RelevantChanges.NegateDate) as prev_cat_code,
			LAST_VALUE(RelevantChanges.curr_cat_code) over (partition by SapiensClaimNumber order by RelevantChanges.NegateDate) as curr_cat_code,
			FIRST_VALUE(prev_rel_claim_id) over (partition by SapiensClaimNumber order by NegateDate) as prev_rel_claim_id,
			LAST_VALUE(curr_rel_claim_id) over (partition by SapiensClaimNumber order by NegateDate) as curr_rel_claim_id, 
			FIRST_VALUE(RelevantChanges.prev_pol_key) over (partition by SapiensClaimNumber order by RelevantChanges.NegateDate) as prev_pol_key,
			LAST_VALUE(RelevantChanges.curr_pol_key) over (partition by SapiensClaimNumber order by RelevantChanges.NegateDate) as curr_pol_key,
			LAST_VALUE(RelevantChanges.NegateDate) over (partition by SapiensClaimNumber order by NegateDate) as NegateDate, 
			ROW_NUMBER() over (partition by RelevantChanges.SapiensClaimNumber order by RelevantChanges.NegateDate desc) as Rank_value
		from (select AllType2Changes.SapiensClaimNumber, 
				AllType2Changes.prev_loss_date, AllType2Changes.curr_loss_date, AllType2Changes.prev_cat_code, AllType2Changes.curr_cat_code, case when ISNUMERIC(AllType2Changes.prev_rel_claim_id)=0 then 0 else AllType2Changes.prev_rel_claim_id end as prev_rel_claim_id,
				AllType2Changes.curr_rel_claim_id, AllType2Changes.prev_pol_key, AllType2Changes.curr_pol_key,AllType2Changes.NegateDate
			from
			(
				select ROW_NUMBER() over (partition by cod.edw_claim_occurrence_ak_id order by cod.eff_from_date desc) as RowNum, 
					cast(cod.eff_from_date as date) as NegateDate, 
					CASE cod.claim_num when 'N/A' then SUBSTRING(cod.claim_occurrence_key, 1, 20) else cod.claim_num END as SapiensClaimNumber, 
					lag(cast(cod.claim_loss_date as date)) over (partition by cod.edw_claim_occurrence_ak_id order by cod.eff_from_date) as prev_loss_date, 
					cast(cod.claim_loss_date as date) as curr_loss_date,
					lag(cod.claim_cat_code) over (partition by cod.edw_claim_occurrence_ak_id order by cod.eff_from_date) as prev_cat_code, 
					cod.claim_cat_code as curr_cat_code,
					lag (case when ISNUMERIC(cod.ClaimRelationshipKey)=0 then 0 else cod.ClaimRelationshipKey end) over (partition by cod.edw_claim_occurrence_ak_id order by cod.eff_from_date) as prev_rel_claim_id, 
					(case when ISNUMERIC(cod.ClaimRelationshipKey)=0 then 0 else cod.ClaimRelationshipKey end) as curr_rel_claim_id,
					lag(co.pol_key) over (partition by cod.edw_claim_occurrence_ak_id order by cod.eff_from_date) as prev_pol_key,
					co.pol_key as curr_pol_key,
					cod.edw_claim_occurrence_ak_id, cod.eff_from_date, cod.eff_to_date, cod.modified_date, cod.created_date
				from dbo.claim_occurrence_dim cod
				join @{pipeline().parameters.LKP_DATABASE_NAME}.@{pipeline().parameters.LKP_TABLE_OWNER}.claim_occurrence co on cod.edw_claim_occurrence_pk_id = co.claim_occurrence_id
			) AllType2Changes
			where ((AllType2Changes.prev_loss_date <> AllType2Changes.curr_loss_date AND AllType2Changes.prev_loss_date IS NOT NULL)
				OR (AllType2Changes.prev_cat_code <> AllType2Changes.curr_cat_code AND AllType2Changes.prev_cat_code IS NOT NULL)
				--OR (AllType2Changes.prev_rel_claim_id <> AllType2Changes.curr_rel_claim_id AND AllType2Changes.prev_rel_claim_id IS NOT NULL)
				OR (case when AllType2Changes.prev_rel_claim_id is NULL OR ISNUMERIC(AllType2Changes.prev_rel_claim_id)=0 then 0 else AllType2Changes.prev_rel_claim_id end  <> AllType2Changes.curr_rel_claim_id)
				OR (AllType2Changes.prev_pol_key <> AllType2Changes.curr_pol_key AND AllType2Changes.prev_pol_key IS NOT NULL)
				)
			AND AllType2Changes.created_date > '@{pipeline().parameters.SELECTION_START_TS}'
		) RelevantChanges 
	) FINAL
	where FINAL.Rank_value = 1
	 @{pipeline().parameters.WHERE} 
	order by FINAL.SapiensClaimNumber
),
EXP_Collect AS (
	SELECT
	SapiensClaimNumber AS i_SapiensClaimNumber,
	-- *INF*: LTRIM(RTRIM(i_SapiensClaimNumber))
	LTRIM(RTRIM(i_SapiensClaimNumber)) AS o_SapiensClaimNumber,
	prev_loss_date,
	curr_loss_date,
	prev_cat_code,
	curr_cat_code,
	prev_rel_claim_id,
	curr_rel_claim_id,
	prev_pol_key,
	curr_pol_key
	FROM SQ_claim_occurrence_dim
),
SQ_SapiensClaimRestate_CSV AS (

-- TODO Manual --

),
EXP_Collect_ManualRequests AS (
	SELECT
	SapiensClaimNumber AS i_SapiensClaimNumber,
	-- *INF*: LTRIM(RTRIM(i_SapiensClaimNumber))
	LTRIM(RTRIM(i_SapiensClaimNumber)) AS o_SapiensClaimNumber
	FROM SQ_SapiensClaimRestate_CSV
),
FIL_Ensure_Valid_SapiensClaimNumber_Format AS (
	SELECT
	o_SapiensClaimNumber AS SapiensClaimNumber
	FROM EXP_Collect_ManualRequests
	WHERE LENGTH(SapiensClaimNumber)=7 or LENGTH(SapiensClaimNumber)=20
),
AGG_UniqueClaims AS (
	SELECT
	SapiensClaimNumber
	FROM FIL_Ensure_Valid_SapiensClaimNumber_Format
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SapiensClaimNumber ORDER BY NULL) = 1
),
LKP_Most_Recent_Policy_For_Claim AS (
	SELECT
	POLICY_NO,
	CLAIM_ID
	FROM (
		SELECT a.MAX_SOURCE_SEQ_NUM AS SOURCE_SEQ_NUM,
			RTRIM(maxforclaim.POLICY_NO) AS POLICY_NO,
			RTRIM(a.CLAIM_ID) AS CLAIM_ID
		FROM (
			SELECT ac.CLAIM_ID,
				MAX(ac.SOURCE_SEQ_NUM) AS MAX_SOURCE_SEQ_NUM
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceHeaderExtract ac WITH (NOLOCK)
			WHERE ac.DATA_SOURCE = 'SRL'
			AND ac.DOCUMENT_TYPE <> 'G'
			GROUP BY ac.CLAIM_ID
			) a
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceHeaderExtract maxforclaim WITH (NOLOCK) ON a.MAX_SOURCE_SEQ_NUM = maxforclaim.SOURCE_SEQ_NUM
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CLAIM_ID ORDER BY POLICY_NO) = 1
),
LKP_claim_occurrence AS (
	SELECT
	pol_key,
	EDWSapiensClaimNumber
	FROM (
		select co.pol_key as pol_key,
			SUBSTRING(co.claim_occurrence_key, 1, 20) as EDWSapiensClaimNumber
		from @{pipeline().parameters.LKP_DATABASE_NAME}.@{pipeline().parameters.LKP_TABLE_OWNER}.claim_occurrence co with (nolock)
		where co.crrnt_snpsht_flag = 1
		and co.source_sys_id = 'PMS'
		union all 
		select co.pol_key as pol_key,
			RTRIM(co.s3p_claim_num) as EDWSapiensClaimNumber
		from @{pipeline().parameters.LKP_DATABASE_NAME}.@{pipeline().parameters.LKP_TABLE_OWNER}.claim_occurrence co with (nolock)
		where co.crrnt_snpsht_flag = 1
		and co.source_sys_id = 'EXCEED'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWSapiensClaimNumber ORDER BY pol_key) = 1
),
EXP_ManualRestateRequest AS (
	SELECT
	LKP_Most_Recent_Policy_For_Claim.POLICY_NO,
	LKP_claim_occurrence.pol_key,
	-- *INF*: IIF(ISNULL(POLICY_NO) OR LENGTH(RTRIM(POLICY_NO))=0,
	-- pol_key,
	-- POLICY_NO)
	IFF(POLICY_NO IS NULL OR LENGTH(RTRIM(POLICY_NO)) = 0, pol_key, POLICY_NO) AS o_prev_pol_key,
	LKP_claim_occurrence.EDWSapiensClaimNumber AS SapiensClaimNumber,
	'' AS DefaultChar,
	0 AS DefaultNum
	FROM 
	LEFT JOIN LKP_Most_Recent_Policy_For_Claim
	ON LKP_Most_Recent_Policy_For_Claim.CLAIM_ID = AGG_UniqueClaims.SapiensClaimNumber
	LEFT JOIN LKP_claim_occurrence
	ON LKP_claim_occurrence.EDWSapiensClaimNumber = AGG_UniqueClaims.SapiensClaimNumber
),
JNR_Automated_and_Manual_Restates AS (SELECT
	EXP_Collect.o_SapiensClaimNumber AS automated_SapiensClaimNumber, 
	EXP_Collect.prev_loss_date AS automated_prev_loss_date, 
	EXP_Collect.curr_loss_date AS automated_curr_loss_date, 
	EXP_Collect.prev_cat_code AS automated_prev_cat_code, 
	EXP_Collect.curr_cat_code AS automated_curr_cat_code, 
	EXP_Collect.prev_rel_claim_id AS automated_prev_rel_claim_id, 
	EXP_Collect.curr_rel_claim_id AS automated_curr_rel_claim_id, 
	EXP_Collect.prev_pol_key AS automated_prev_pol_key, 
	EXP_Collect.curr_pol_key AS automated_curr_pol_key, 
	EXP_ManualRestateRequest.SapiensClaimNumber AS manual_SapiensClaimNumber, 
	EXP_ManualRestateRequest.DefaultChar AS manual_prev_cat_code, 
	EXP_ManualRestateRequest.DefaultChar AS manual_curr_cat_code, 
	EXP_ManualRestateRequest.DefaultNum AS manual_prev_rel_claim_id, 
	EXP_ManualRestateRequest.DefaultNum AS manual_curr_rel_claim_id, 
	EXP_ManualRestateRequest.o_prev_pol_key AS manual_prev_pol_key
	FROM EXP_Collect
	FULL OUTER JOIN EXP_ManualRestateRequest
	ON EXP_ManualRestateRequest.SapiensClaimNumber = EXP_Collect.o_SapiensClaimNumber
),
EXP_Combine_Automatic_And_Manual AS (
	SELECT
	automated_SapiensClaimNumber,
	automated_prev_loss_date,
	automated_curr_loss_date,
	automated_prev_cat_code,
	automated_curr_cat_code,
	automated_prev_rel_claim_id,
	automated_curr_rel_claim_id,
	automated_prev_pol_key,
	automated_curr_pol_key,
	manual_SapiensClaimNumber,
	manual_prev_loss_date,
	manual_curr_loss_date,
	manual_prev_cat_code,
	manual_curr_cat_code,
	manual_prev_rel_claim_id,
	manual_curr_rel_claim_id,
	manual_prev_pol_key,
	manual_curr_pol_key,
	-- *INF*: IIF(ISNULL(automated_SapiensClaimNumber),
	-- manual_SapiensClaimNumber,
	-- automated_SapiensClaimNumber)
	IFF(
	    automated_SapiensClaimNumber IS NULL, manual_SapiensClaimNumber,
	    automated_SapiensClaimNumber
	) AS o_SapiensClaimNumber,
	-- *INF*: IIF(ISNULL(automated_prev_loss_date),
	-- manual_prev_loss_date,
	-- automated_prev_loss_date)
	IFF(automated_prev_loss_date IS NULL, manual_prev_loss_date, automated_prev_loss_date) AS o_prev_loss_date,
	-- *INF*: IIF(ISNULL(automated_curr_loss_date),
	-- manual_curr_loss_date,
	-- automated_curr_loss_date)
	IFF(automated_curr_loss_date IS NULL, manual_curr_loss_date, automated_curr_loss_date) AS o_curr_loss_date,
	-- *INF*: IIF(ISNULL(automated_prev_cat_code),
	-- manual_prev_cat_code,
	-- automated_prev_cat_code)
	IFF(automated_prev_cat_code IS NULL, manual_prev_cat_code, automated_prev_cat_code) AS o_prev_cat_code,
	-- *INF*: IIF(ISNULL(automated_curr_cat_code),
	-- manual_curr_cat_code,
	-- automated_curr_cat_code)
	IFF(automated_curr_cat_code IS NULL, manual_curr_cat_code, automated_curr_cat_code) AS o_curr_cat_code,
	-- *INF*: IIF(ISNULL(automated_prev_rel_claim_id),
	-- manual_prev_rel_claim_id,
	-- automated_prev_rel_claim_id)
	IFF(
	    automated_prev_rel_claim_id IS NULL, manual_prev_rel_claim_id, automated_prev_rel_claim_id
	) AS o_prev_rel_claim_id,
	-- *INF*: IIF(ISNULL(automated_curr_rel_claim_id),
	-- manual_curr_rel_claim_id,
	-- automated_curr_rel_claim_id)
	IFF(
	    automated_curr_rel_claim_id IS NULL, manual_curr_rel_claim_id, automated_curr_rel_claim_id
	) AS o_curr_rel_claim_id,
	-- *INF*: IIF(ISNULL(automated_prev_pol_key),
	-- manual_prev_pol_key,
	-- automated_prev_pol_key)
	IFF(automated_prev_pol_key IS NULL, manual_prev_pol_key, automated_prev_pol_key) AS o_prev_pol_key,
	-- *INF*: IIF(ISNULL(automated_curr_pol_key),
	-- manual_curr_pol_key,
	-- automated_curr_pol_key)
	IFF(automated_curr_pol_key IS NULL, manual_curr_pol_key, automated_curr_pol_key) AS o_curr_pol_key
	FROM JNR_Automated_and_Manual_Restates
),
LKP_SapiensReinsuranceClaimRestate_Exists AS (
	SELECT
	SapiensReinsuranceClaimRestateId,
	ClaimNumber
	FROM (
		SELECT 
			SapiensReinsuranceClaimRestateId,
			ClaimNumber
		FROM SapiensReinsuranceClaimRestate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClaimNumber ORDER BY SapiensReinsuranceClaimRestateId) = 1
),
FIL_Claim_Not_In_SapiensReinsuranceClaimRestate AS (
	SELECT
	EXP_Combine_Automatic_And_Manual.o_SapiensClaimNumber AS claim_num, 
	EXP_Combine_Automatic_And_Manual.o_prev_loss_date AS prev_loss_date, 
	EXP_Combine_Automatic_And_Manual.o_curr_loss_date AS curr_loss_date, 
	EXP_Combine_Automatic_And_Manual.o_prev_cat_code AS prev_cat_code, 
	EXP_Combine_Automatic_And_Manual.o_curr_cat_code AS curr_cat_code, 
	EXP_Combine_Automatic_And_Manual.o_prev_rel_claim_id AS prev_rel_claim_id, 
	EXP_Combine_Automatic_And_Manual.o_curr_rel_claim_id AS curr_rel_claim_id, 
	EXP_Combine_Automatic_And_Manual.o_prev_pol_key AS prev_pol_key, 
	EXP_Combine_Automatic_And_Manual.o_curr_pol_key AS curr_pol_key, 
	LKP_SapiensReinsuranceClaimRestate_Exists.SapiensReinsuranceClaimRestateId
	FROM EXP_Combine_Automatic_And_Manual
	LEFT JOIN LKP_SapiensReinsuranceClaimRestate_Exists
	ON LKP_SapiensReinsuranceClaimRestate_Exists.ClaimNumber = EXP_Combine_Automatic_And_Manual.o_SapiensClaimNumber
	WHERE ISNULL(SapiensReinsuranceClaimRestateId)
),
LKP_Most_Recent_DocType_For_Claim AS (
	SELECT
	DOCUMENT_TYPE,
	CLAIM_ID
	FROM (
		select a.MAX_SOURCE_SEQ_NUM as SOURCE_SEQ_NUM,
			maxforclaim.DOCUMENT_TYPE as DOCUMENT_TYPE,
			RTRIM(a.CLAIM_ID) as CLAIM_ID 
		from (select ac.CLAIM_ID, 
				MAX(ac.SOURCE_SEQ_NUM) as MAX_SOURCE_SEQ_NUM
			from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceHeaderExtract ac with (nolock)
			where ac.DATA_SOURCE = 'SRL'
			group by ac.CLAIM_ID) a
		join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceHeaderExtract maxforclaim with (nolock) 
			on a.MAX_SOURCE_SEQ_NUM = maxforclaim.SOURCE_SEQ_NUM
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CLAIM_ID ORDER BY DOCUMENT_TYPE) = 1
),
EXP_Set_SSN_NegateFlag AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	SYSDATE AS o_CurrentTimestamp,
	-- *INF*: IIF(ISNULL(:LKP.LKP_Get_Max_Sapiens_SourceSequenceNumber(1)),
	--  0,
	--  :LKP.LKP_Get_Max_Sapiens_SourceSequenceNumber(1))
	IFF(
	    LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1.Source_Seq_Num IS NULL, 0,
	    LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1.Source_Seq_Num
	) AS v_lkp_Source_Seq_Num,
	v_count + 1 AS v_count,
	v_lkp_Source_Seq_Num + v_count AS o_Source_Seq_Num,
	FIL_Claim_Not_In_SapiensReinsuranceClaimRestate.claim_num AS SapiensClaimNumber,
	FIL_Claim_Not_In_SapiensReinsuranceClaimRestate.prev_loss_date,
	FIL_Claim_Not_In_SapiensReinsuranceClaimRestate.curr_loss_date,
	FIL_Claim_Not_In_SapiensReinsuranceClaimRestate.prev_cat_code,
	FIL_Claim_Not_In_SapiensReinsuranceClaimRestate.curr_cat_code,
	FIL_Claim_Not_In_SapiensReinsuranceClaimRestate.prev_rel_claim_id,
	FIL_Claim_Not_In_SapiensReinsuranceClaimRestate.curr_rel_claim_id,
	FIL_Claim_Not_In_SapiensReinsuranceClaimRestate.prev_pol_key,
	FIL_Claim_Not_In_SapiensReinsuranceClaimRestate.curr_pol_key,
	@{pipeline().parameters.SELECTION_START_TS} AS NegateDate,
	LKP_Most_Recent_DocType_For_Claim.DOCUMENT_TYPE AS i_DOCUMENT_TYPE,
	-- *INF*: IIF(ISNULL(i_DOCUMENT_TYPE),'0',
	--     IIF(i_DOCUMENT_TYPE='G',
	-- '0',
	-- '1')
	-- )
	IFF(i_DOCUMENT_TYPE IS NULL, '0', IFF(
	        i_DOCUMENT_TYPE = 'G', '0', '1'
	    )) AS NegateFlag
	FROM FIL_Claim_Not_In_SapiensReinsuranceClaimRestate
	LEFT JOIN LKP_Most_Recent_DocType_For_Claim
	ON LKP_Most_Recent_DocType_For_Claim.CLAIM_ID = FIL_Claim_Not_In_SapiensReinsuranceClaimRestate.claim_num
	LEFT JOIN LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1
	ON LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1.ID = 1

),
SapiensReinsuranceClaimRestate AS (
	INSERT INTO SapiensReinsuranceClaimRestate
	(AuditId, CreatedDate, ModifiedDate, ClaimNumber, PreviousLossDate, CurrentLossDate, PreviousCatastropheCode, CurrentCatastropheCode, NegateDate, SourceSequenceNumber, TransactionNumber, PreviousClaimRelationshipId, CurrentClaimRelationshipId, PreviousPolicyKey, CurrentPolicyKey, NegateFlag)
	SELECT 
	o_AuditId AS AUDITID, 
	o_CurrentTimestamp AS CREATEDDATE, 
	o_CurrentTimestamp AS MODIFIEDDATE, 
	SapiensClaimNumber AS CLAIMNUMBER, 
	prev_loss_date AS PREVIOUSLOSSDATE, 
	curr_loss_date AS CURRENTLOSSDATE, 
	prev_cat_code AS PREVIOUSCATASTROPHECODE, 
	curr_cat_code AS CURRENTCATASTROPHECODE, 
	NEGATEDATE, 
	o_Source_Seq_Num AS SOURCESEQUENCENUMBER, 
	o_Source_Seq_Num AS TRANSACTIONNUMBER, 
	prev_rel_claim_id AS PREVIOUSCLAIMRELATIONSHIPID, 
	curr_rel_claim_id AS CURRENTCLAIMRELATIONSHIPID, 
	prev_pol_key AS PREVIOUSPOLICYKEY, 
	curr_pol_key AS CURRENTPOLICYKEY, 
	NEGATEFLAG
	FROM EXP_Set_SSN_NegateFlag
),
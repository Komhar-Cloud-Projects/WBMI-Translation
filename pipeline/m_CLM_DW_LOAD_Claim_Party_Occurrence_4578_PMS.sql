WITH
LKP_PMS_Adjustor AS (
	SELECT
	adnm_type_adjustor,
	in_Claim_Adjustor_Nbr,
	adnm_adjustor_nbr
	FROM (
		SELECT 
			adnm_type_adjustor,
			in_Claim_Adjustor_Nbr,
			adnm_adjustor_nbr
		FROM pms_adjuster_master_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY adnm_adjustor_nbr ORDER BY adnm_type_adjustor) = 1
),
LKP_CLAIM_OCCURRENCE AS (
	SELECT
	claim_occurrence_ak_id,
	claim_occurrence_key
	FROM (
		SELECT 
		   claim_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id, 
		   claim_occurrence.claim_occurrence_key as claim_occurrence_key 
		FROM 
		   claim_occurrence
		WHERE
		   source_sys_id = '@{pipeline().parameters.SOURCE_SYS_ID}' AND crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_key ORDER BY claim_occurrence_ak_id) = 1
),
LKP_CLAIM_PARTY AS (
	SELECT
	claim_party_ak_id,
	claim_party_key
	FROM (
		SELECT 
		   claim_party.claim_party_ak_id as claim_party_ak_id, 
		   claim_party.claim_party_key as claim_party_key 
		FROM 
		   claim_party
		WHERE 
		   source_sys_id = 'PMS' AND crrnt_snpsht_flag = '1'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_key ORDER BY claim_party_ak_id) = 1
),
LKP_CLAIM_CASE_AK_ID AS (
	SELECT
	claim_case_ak_id,
	claim_case_key
	FROM (
		SELECT 
		claim_case.claim_case_ak_id as claim_case_ak_id, 
		claim_case.claim_case_key as claim_case_key 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_case 
		WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_case_key ORDER BY claim_case_ak_id) = 1
),
SQ_pif_4578_stage AS (
	SELECT distinct
	ltrim(rtrim(a.pif_symbol)) as pif_symbol
	, ltrim(rtrim(a.pif_policy_number)) as pif_policy_number
	, ltrim(rtrim(a.pif_module)) as pif_module
	, ltrim(rtrim(a.loss_year)) as loss_year
	, ltrim(rtrim(a.loss_month)) as loss_month
	, ltrim(rtrim(a.loss_day)) as loss_day
	, ltrim(rtrim(a.loss_occurence)) as loss_occurence
	, ltrim(rtrim(a.loss_claimant)) as loss_claimant
	, ltrim(rtrim(a.loss_adjustor_no)) as loss_adjustor_no
	
	FROM
	 pif_4578_stage a
	WHERE
	 (a.loss_part = '7') AND 
	(a.logical_flag ='0')
	AND LEN(LTRIM(RTRIM(a.loss_draft_no))) > 0
),
EXP_Source AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module,
	pif_symbol  || pif_policy_number  || pif_module AS v_SYM_NUM_MODE,
	loss_year,
	loss_month,
	loss_day,
	-- *INF*: IIF ( LENGTH(to_char(loss_month)) = 1, '0' || TO_CHAR(loss_month),TO_CHAR(loss_month))
	-- ||  
	-- IIF ( LENGTH(to_char(loss_day)) = 1, '0' || TO_CHAR(loss_day), TO_CHAR(loss_day) )
	-- ||  
	-- TO_CHAR(loss_year)
	IFF(LENGTH(to_char(loss_month)) = 1, '0' || TO_CHAR(loss_month), TO_CHAR(loss_month)) || IFF(LENGTH(to_char(loss_day)) = 1, '0' || TO_CHAR(loss_day), TO_CHAR(loss_day)) || TO_CHAR(loss_year) AS v_CLM_LOSS_DT,
	loss_occurence,
	loss_claimant,
	v_SYM_NUM_MODE || v_CLM_LOSS_DT || loss_occurence AS claim_occurrence,
	loss_adjustor_no AS IN_loss_adjustor_no,
	-- *INF*: IIF(ISNULL(IN_loss_adjustor_no) OR IS_SPACES(LTRIM(RTRIM(IN_loss_adjustor_no))) OR LENGTH(LTRIM(RTRIM(IN_loss_adjustor_no))) = 0, 'N/A', LTRIM(RTRIM(IN_loss_adjustor_no)))
	IFF(IN_loss_adjustor_no IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_loss_adjustor_no))) OR LENGTH(LTRIM(RTRIM(IN_loss_adjustor_no))) = 0, 'N/A', LTRIM(RTRIM(IN_loss_adjustor_no))) AS o_loss_adjustor_no,
	-- *INF*: :LKP.LKP_PMS_ADJUSTOR(IN_loss_adjustor_no)
	LKP_PMS_ADJUSTOR_IN_loss_adjustor_no.adnm_type_adjustor AS adnm_type_adjustor
	FROM SQ_pif_4578_stage
	LEFT JOIN LKP_PMS_ADJUSTOR LKP_PMS_ADJUSTOR_IN_loss_adjustor_no
	ON LKP_PMS_ADJUSTOR_IN_loss_adjustor_no.adnm_adjustor_nbr = IN_loss_adjustor_no

),
EXP_LKP_Values AS (
	SELECT
	claim_occurrence AS CLAIM_OCCURRENCE_KEY,
	o_loss_adjustor_no AS adnm_adjustor_nbr,
	adnm_type_adjustor,
	adnm_adjustor_nbr || adnm_type_adjustor AS v_claim_party_key,
	loss_claimant,
	-- *INF*: :LKP.LKP_CLAIM_PARTY(v_claim_party_key)
	LKP_CLAIM_PARTY_v_claim_party_key.claim_party_ak_id AS claim_party_ak_id,
	-- *INF*: :LKP.LKP_CLAIM_OCCURRENCE(CLAIM_OCCURRENCE_KEY)
	LKP_CLAIM_OCCURRENCE_CLAIM_OCCURRENCE_KEY.claim_occurrence_ak_id AS claim_occurrence_ak_id,
	CLAIM_OCCURRENCE_KEY || loss_claimant AS v_claim_case_key,
	-- *INF*: iif(isnull(:LKP.LKP_CLAIM_CASE_AK_ID(v_claim_case_key))
	-- ,-1
	-- ,:LKP.LKP_CLAIM_CASE_AK_ID(v_claim_case_key)
	-- )
	IFF(LKP_CLAIM_CASE_AK_ID_v_claim_case_key.claim_case_ak_id IS NULL, - 1, LKP_CLAIM_CASE_AK_ID_v_claim_case_key.claim_case_ak_id) AS claim_case_ak_id,
	-- *INF*: TO_DATE('1/1/1800','MM/DD/YYYY')
	TO_DATE('1/1/1800', 'MM/DD/YYYY') AS denial_date
	FROM EXP_Source
	LEFT JOIN LKP_CLAIM_PARTY LKP_CLAIM_PARTY_v_claim_party_key
	ON LKP_CLAIM_PARTY_v_claim_party_key.claim_party_key = v_claim_party_key

	LEFT JOIN LKP_CLAIM_OCCURRENCE LKP_CLAIM_OCCURRENCE_CLAIM_OCCURRENCE_KEY
	ON LKP_CLAIM_OCCURRENCE_CLAIM_OCCURRENCE_KEY.claim_occurrence_key = CLAIM_OCCURRENCE_KEY

	LEFT JOIN LKP_CLAIM_CASE_AK_ID LKP_CLAIM_CASE_AK_ID_v_claim_case_key
	ON LKP_CLAIM_CASE_AK_ID_v_claim_case_key.claim_case_key = v_claim_case_key

),
Agg_Data AS (
	SELECT
	claim_party_ak_id,
	adnm_type_adjustor,
	claim_occurrence_ak_id,
	denial_date,
	claim_case_ak_id
	FROM EXP_LKP_Values
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_ak_id, adnm_type_adjustor, claim_occurrence_ak_id, denial_date, claim_case_ak_id ORDER BY NULL) = 1
),
LKP_CLAIM_PARTY_OCCURRENCE AS (
	SELECT
	claim_party_occurrence_id,
	claim_party_occurrence_ak_id,
	denial_date,
	claim_party_ak_id,
	claim_occurrence_ak_id,
	claim_case_ak_id,
	claim_party_role_code
	FROM (
		SELECT 
		  a.claim_party_occurrence_id as claim_party_occurrence_id
		, a.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id
		, a.denial_date as denial_date
		, a.claim_party_ak_id as claim_party_ak_id
		, a.claim_occurrence_ak_id as claim_occurrence_ak_id
		, a.claim_case_ak_id as claim_case_ak_id
		, ltrim(rtrim(a.claim_party_role_code)) as claim_party_role_code 
		FROM claim_party_occurrence a
		WHERE a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND a.crrnt_snpsht_flag = 1
		ORDER BY a.claim_occurrence_ak_id, a.claim_party_role_code, a.claim_party_ak_id --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_ak_id,claim_occurrence_ak_id,claim_case_ak_id,claim_party_role_code ORDER BY claim_party_occurrence_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	Agg_Data.claim_party_ak_id AS CLAIM_PARTY_AK_ID,
	Agg_Data.adnm_type_adjustor AS CLAIM_PARTY_ROLE_CODE,
	Agg_Data.claim_occurrence_ak_id AS CLAIM_OCCURRENCE_AK_ID,
	Agg_Data.denial_date AS DENIAL_DATE,
	Agg_Data.claim_case_ak_id AS CLAIM_CASE_AK_ID,
	LKP_CLAIM_PARTY_OCCURRENCE.claim_party_occurrence_id AS lkp_claim_party_occurrence_id,
	LKP_CLAIM_PARTY_OCCURRENCE.claim_party_occurrence_ak_id AS lkp_claim_party_occurrence_ak_id,
	LKP_CLAIM_PARTY_OCCURRENCE.denial_date AS lkp_denial_date,
	'0' AS logical_flag_op,
	1 AS Crrnt_Snpsht_Flag,
	-- *INF*: iif(isnull(lkp_claim_party_occurrence_id), 'NEW',
	-- 	iif ((lkp_denial_date <> DENIAL_DATE), 
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(lkp_claim_party_occurrence_id IS NULL, 'NEW', IFF(( lkp_denial_date <> DENIAL_DATE ), 'UPDATE', 'NOCHANGE')) AS v_Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_Id,
	-- *INF*: IIF(v_Changed_Flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	SYSDATE)
	IFF(v_Changed_Flag = 'NEW', TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS Eff_From_Date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS Eff_To_Date,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID,
	SYSDATE AS Created_Date,
	SYSDATE AS Modified_Date,
	'N/A' AS Dummy
	FROM Agg_Data
	LEFT JOIN LKP_CLAIM_PARTY_OCCURRENCE
	ON LKP_CLAIM_PARTY_OCCURRENCE.claim_party_ak_id = Agg_Data.claim_party_ak_id AND LKP_CLAIM_PARTY_OCCURRENCE.claim_occurrence_ak_id = Agg_Data.claim_occurrence_ak_id AND LKP_CLAIM_PARTY_OCCURRENCE.claim_case_ak_id = Agg_Data.claim_case_ak_id AND LKP_CLAIM_PARTY_OCCURRENCE.claim_party_role_code = Agg_Data.adnm_type_adjustor
),
FIL_Insert AS (
	SELECT
	lkp_claim_party_occurrence_ak_id, 
	CLAIM_PARTY_AK_ID, 
	CLAIM_OCCURRENCE_AK_ID, 
	CLAIM_CASE_AK_ID, 
	CLAIM_PARTY_ROLE_CODE, 
	DENIAL_DATE, 
	Dummy AS IPFCGQ_OFFSET_ONSET_IND, 
	logical_flag_op, 
	Crrnt_Snpsht_Flag, 
	Audit_Id, 
	Eff_From_Date, 
	Eff_To_Date, 
	SOURCE_SYSTEM_ID, 
	Created_Date, 
	Modified_Date, 
	Changed_Flag, 
	Dummy AS out_claimant_num
	FROM EXP_Detect_Changes
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
SEQ_claim_party_occurrence AS (
	CREATE SEQUENCE SEQ_claim_party_occurrence
	START = 0
	INCREMENT = 1;
),
EXP_Determine_Ak AS (
	SELECT
	lkp_claim_party_occurrence_ak_id,
	SEQ_claim_party_occurrence.NEXTVAL,
	-- *INF*: IIF(Changed_Flag='NEW', NEXTVAL, lkp_claim_party_occurrence_ak_id)
	IFF(Changed_Flag = 'NEW', NEXTVAL, lkp_claim_party_occurrence_ak_id) AS claim_party_occurrence_ak_id,
	CLAIM_PARTY_AK_ID,
	CLAIM_OCCURRENCE_AK_ID,
	CLAIM_CASE_AK_ID,
	CLAIM_PARTY_ROLE_CODE,
	DENIAL_DATE,
	IPFCGQ_OFFSET_ONSET_IND,
	logical_flag_op,
	Crrnt_Snpsht_Flag,
	Audit_Id,
	Eff_From_Date,
	Eff_To_Date,
	SOURCE_SYSTEM_ID,
	Created_Date,
	Modified_Date,
	Changed_Flag,
	out_claimant_num,
	'N/A' AS Out_Default_String
	FROM FIL_Insert
),
claim_party_occurrence_Insert AS (
	INSERT INTO claim_party_occurrence
	(claim_party_occurrence_ak_id, claim_occurrence_ak_id, claim_party_ak_id, claim_case_ak_id, claim_party_role_code, claimant_num, denial_date, offset_onset_ind, logical_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, preferred_contact_method)
	SELECT 
	CLAIM_PARTY_OCCURRENCE_AK_ID, 
	CLAIM_OCCURRENCE_AK_ID AS CLAIM_OCCURRENCE_AK_ID, 
	CLAIM_PARTY_AK_ID AS CLAIM_PARTY_AK_ID, 
	CLAIM_CASE_AK_ID AS CLAIM_CASE_AK_ID, 
	CLAIM_PARTY_ROLE_CODE AS CLAIM_PARTY_ROLE_CODE, 
	out_claimant_num AS CLAIMANT_NUM, 
	DENIAL_DATE AS DENIAL_DATE, 
	IPFCGQ_OFFSET_ONSET_IND AS OFFSET_ONSET_IND, 
	logical_flag_op AS LOGICAL_FLAG, 
	Crrnt_Snpsht_Flag AS CRRNT_SNPSHT_FLAG, 
	Audit_Id AS AUDIT_ID, 
	Eff_From_Date AS EFF_FROM_DATE, 
	Eff_To_Date AS EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	Created_Date AS CREATED_DATE, 
	Modified_Date AS MODIFIED_DATE, 
	Out_Default_String AS PREFERRED_CONTACT_METHOD
	FROM EXP_Determine_Ak
),
SQ_claim_party_occurrence AS (
	SELECT 
	a.claim_party_occurrence_id, 
	a.claim_occurrence_ak_id, 
	a.claim_party_role_code, 
	a.claim_party_ak_id, 
	a.claim_case_ak_id,
	a.eff_from_date, 
	a.eff_to_date 
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence a
	WHERE 
	 a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND 
	 EXISTS (SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence b
			WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.claim_occurrence_ak_id = b.claim_occurrence_ak_id
			AND a.claim_party_role_code = b.claim_party_role_code
	            AND a.claim_party_occurrence_ak_id = b.claim_party_occurrence_ak_id
			AND a.claim_party_ak_id = b.claim_party_ak_id
	            AND a.claim_case_ak_id = b.claim_case_ak_id
			GROUP BY claim_occurrence_ak_id, claim_party_role_code, claim_party_ak_id, claim_case_ak_id
			HAVING COUNT(*) > 1)
	ORDER BY claim_occurrence_ak_id, claim_party_role_code, claim_party_ak_id, a.claim_case_ak_id, eff_from_date  DESC
	
	--The extra condition ****** AND a.claim_party_occurrence_ak_id = b.claim_party_occurrence_ak_id *****  has been added on 11/18/2009 because 45GJ, 45GQ and 4578 sources loading claim_party_occurrence have different logical AK ids. 45GJ and 45GQ don't consider claim_case_ak_id as part of the overall AK id of target table whereas 4578 does. As a result, this pipeline was expiring some of the records loaded by 4578. In order to aviod that, this extra condition has been included.
),
EXP_Lag_eff_from_date AS (
	SELECT
	claim_party_occurrence_id,
	claim_party_role_code,
	claim_occurrence_ak_id,
	claim_party_ak_id,
	claim_case_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	claim_occurrence_ak_id = v_PREV_ROW_claim_occurrence_ak_id AND
	-- 	claim_party_ak_id = v_PREV_ROW_claim_party_ak_id AND
	-- 	claim_party_role_code = v_PREV_ROW_claim_party_role_code AND
	--       claim_case_ak_id=v_PREV_ROW_claim_case_ak_id,
	-- ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- orig_eff_to_date)
	DECODE(TRUE,
		claim_occurrence_ak_id = v_PREV_ROW_claim_occurrence_ak_id AND claim_party_ak_id = v_PREV_ROW_claim_party_ak_id AND claim_party_role_code = v_PREV_ROW_claim_party_role_code AND claim_case_ak_id = v_PREV_ROW_claim_case_ak_id, ADD_TO_DATE(v_PREV_ROW_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	claim_occurrence_ak_id AS v_PREV_ROW_claim_occurrence_ak_id,
	claim_party_ak_id AS v_PREV_ROW_claim_party_ak_id,
	claim_party_role_code AS v_PREV_ROW_claim_party_role_code,
	claim_case_ak_id AS v_PREV_ROW_claim_case_ak_id,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_claim_party_occurrence
),
FIL_FirstRowInAKGroup AS (
	SELECT
	claim_party_occurrence_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <> eff_to_date

--If these two dates equal each other we are dealing with the first row in an AK group.  This row
--does not need to be expired or updated for any reason thus it can be filtered out
-- but we must source it to capture the eff_from_date of this row 
--so that we can properly expire the subsequent row
),
UPD_Claim_Party_Occurrence AS (
	SELECT
	claim_party_occurrence_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FIL_FirstRowInAKGroup
),
claim_party_occurrence_Update AS (
	MERGE INTO claim_party_occurrence AS T
	USING UPD_Claim_Party_Occurrence AS S
	ON T.claim_party_occurrence_id = S.claim_party_occurrence_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),
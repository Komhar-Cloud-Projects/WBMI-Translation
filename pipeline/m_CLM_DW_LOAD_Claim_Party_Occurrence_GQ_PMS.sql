WITH
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
SQ_PIF_42GQ_ATY_stage AS (
	SELECT (PIF_SYMBOL + PIF_POLICY_NUMBER + PIF_MODULE + 
	CASE len(IPFCGQ_MONTH_OF_LOSS) when 1 THEN '0' + CAST(IPFCGQ_MONTH_OF_LOSS AS VARCHAR) ELSE CAST(IPFCGQ_MONTH_OF_LOSS AS VARCHAR) END + 
	CASE len(IPFCGQ_DAY_OF_LOSS) when 1 THEN '0' + CAST(IPFCGQ_DAY_OF_LOSS AS VARCHAR) ELSE CAST(IPFCGQ_DAY_OF_LOSS AS VARCHAR) END +
	CAST(IPFCGQ_YEAR_OF_LOSS AS VARCHAR) + 
	CAST(IPFCGQ_LOSS_OCCURENCE AS VARCHAR) + 
	CAST(IPFCGQ_LOSS_CLAIMANT AS VARCHAR) + 'ATTY') as CLAIM_PARTY_KEY, 
	(PIF_SYMBOL + PIF_POLICY_NUMBER + PIF_MODULE + 
	CASE len(IPFCGQ_MONTH_OF_LOSS) when 1 THEN '0' + CAST(IPFCGQ_MONTH_OF_LOSS AS VARCHAR) ELSE CAST(IPFCGQ_MONTH_OF_LOSS AS VARCHAR) END + 
	CASE len(IPFCGQ_DAY_OF_LOSS) when 1 THEN '0' + CAST(IPFCGQ_DAY_OF_LOSS AS VARCHAR) ELSE CAST(IPFCGQ_DAY_OF_LOSS AS VARCHAR) END +
	CAST(IPFCGQ_YEAR_OF_LOSS AS VARCHAR) + 
	CAST(IPFCGQ_LOSS_OCCURENCE AS VARCHAR)) as CLAIM_OCCURRENCE_KEY,
	'ATTY' as USE_CODE
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PIF_42GQ_ATY_STAGE 
	WHERE IPFCGQ_ATTORNEY_NAME_1 IS NOT NULL AND LEN(RTRIM(IPFCGQ_ATTORNEY_NAME_1)) <> 0
	and pif_42gq_aty_stage.logical_flag='0'
	UNION
	SELECT (PIF_SYMBOL + PIF_POLICY_NUMBER + PIF_MODULE + 
	CASE len(IPFCGQ_MONTH_OF_LOSS) when 1 THEN '0' + CAST(IPFCGQ_MONTH_OF_LOSS AS VARCHAR) ELSE CAST(IPFCGQ_MONTH_OF_LOSS AS VARCHAR) END + 
	CASE len(IPFCGQ_DAY_OF_LOSS) when 1 THEN '0' + CAST(IPFCGQ_DAY_OF_LOSS AS VARCHAR) ELSE CAST(IPFCGQ_DAY_OF_LOSS AS VARCHAR) END +
	CAST(IPFCGQ_YEAR_OF_LOSS AS VARCHAR) + 
	CAST(IPFCGQ_LOSS_OCCURENCE AS VARCHAR) + 
	CAST(IPFCGQ_LOSS_CLAIMANT AS VARCHAR) + 'PLAT') as CLAIM_PARTY_KEY, 
	(PIF_SYMBOL + PIF_POLICY_NUMBER + PIF_MODULE + 
	CASE len(IPFCGQ_MONTH_OF_LOSS) when 1 THEN '0' + CAST(IPFCGQ_MONTH_OF_LOSS AS VARCHAR) ELSE CAST(IPFCGQ_MONTH_OF_LOSS AS VARCHAR) END + 
	CASE len(IPFCGQ_DAY_OF_LOSS) when 1 THEN '0' + CAST(IPFCGQ_DAY_OF_LOSS AS VARCHAR) ELSE CAST(IPFCGQ_DAY_OF_LOSS AS VARCHAR) END +
	CAST(IPFCGQ_YEAR_OF_LOSS AS VARCHAR) + 
	CAST(IPFCGQ_LOSS_OCCURENCE AS VARCHAR)) as CLAIM_OCCURRENCE_KEY,
	'PLAT' as USE_CODE
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PIF_42GQ_ATY_STAGE 
	WHERE IPFCGQ_PLAINTIFF_1 IS NOT NULL AND LEN(RTRIM(IPFCGQ_PLAINTIFF_1)) <> 0 
	and pif_42gq_aty_stage.logical_flag='0'
	UNION
	SELECT (PIF_SYMBOL + PIF_POLICY_NUMBER + PIF_MODULE + 
	CASE len(IPFCGQ_MONTH_OF_LOSS) when 1 THEN '0' + CAST(IPFCGQ_MONTH_OF_LOSS AS VARCHAR) ELSE CAST(IPFCGQ_MONTH_OF_LOSS AS VARCHAR) END + 
	CASE len(IPFCGQ_DAY_OF_LOSS) when 1 THEN '0' + CAST(IPFCGQ_DAY_OF_LOSS AS VARCHAR) ELSE CAST(IPFCGQ_DAY_OF_LOSS AS VARCHAR) END +
	CAST(IPFCGQ_YEAR_OF_LOSS AS VARCHAR) + 
	CAST(IPFCGQ_LOSS_OCCURENCE AS VARCHAR) + 
	CAST(IPFCGQ_LOSS_CLAIMANT AS VARCHAR) + 'DEFD') as CLAIM_PARTY_KEY, 
	(PIF_SYMBOL + PIF_POLICY_NUMBER + PIF_MODULE + 
	CASE len(IPFCGQ_MONTH_OF_LOSS) when 1 THEN '0' + CAST(IPFCGQ_MONTH_OF_LOSS AS VARCHAR) ELSE CAST(IPFCGQ_MONTH_OF_LOSS AS VARCHAR) END + 
	CASE len(IPFCGQ_DAY_OF_LOSS) when 1 THEN '0' + CAST(IPFCGQ_DAY_OF_LOSS AS VARCHAR) ELSE CAST(IPFCGQ_DAY_OF_LOSS AS VARCHAR) END +
	CAST(IPFCGQ_YEAR_OF_LOSS AS VARCHAR) + 
	CAST(IPFCGQ_LOSS_OCCURENCE AS VARCHAR)) as CLAIM_OCCURRENCE_KEY,
	'DEFD' as USE_CODE
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PIF_42GQ_ATY_STAGE 
	WHERE IPFCGQ_DEFENDANT_1 IS NOT NULL AND LEN(RTRIM(IPFCGQ_DEFENDANT_1)) <> 0
	and pif_42gq_aty_stage.logical_flag='0'
),
EXP_Values AS (
	SELECT
	CLAIM_PARTY_KEY,
	CLAIM_OCCURRENCE_KEY,
	USE_CODE,
	IPFCGQ_OFFSET_ONSET_IND
	FROM SQ_PIF_42GQ_ATY_stage
),
EXP_Lkp_Values AS (
	SELECT
	CLAIM_PARTY_KEY,
	CLAIM_OCCURRENCE_KEY,
	-- *INF*: LTRIM(RTRIM(SUBSTR(CLAIM_PARTY_KEY,1,26)))
	LTRIM(RTRIM(SUBSTR(CLAIM_PARTY_KEY, 1, 26))) AS V_Claim_Case_Key,
	-- *INF*: :LKP.LKP_CLAIM_CASE_AK_ID(V_Claim_Case_Key)
	LKP_CLAIM_CASE_AK_ID_V_Claim_Case_Key.claim_case_ak_id AS v_CLAIM_CASE_AK_ID,
	-- *INF*: IIF(ISNULL(v_CLAIM_CASE_AK_ID),-1,v_CLAIM_CASE_AK_ID)
	IFF(v_CLAIM_CASE_AK_ID IS NULL, - 1, v_CLAIM_CASE_AK_ID) AS CLAIM_CASE_AK_ID,
	USE_CODE AS IPFCGQ_USE_CODE,
	-- *INF*: LTRIM(RTRIM(IPFCGQ_USE_CODE))
	LTRIM(RTRIM(IPFCGQ_USE_CODE)) AS USE_CODE,
	-- *INF*: :LKP.LKP_CLAIM_PARTY(CLAIM_PARTY_KEY)
	-- 
	-- -- IIF(ISNULL(:LKP.LKP_CLAIM_PARTY(CLAIM_PARTY_KEY)),0,:LKP.LKP_CLAIM_PARTY(CLAIM_PARTY_KEY))
	LKP_CLAIM_PARTY_CLAIM_PARTY_KEY.claim_party_ak_id AS CLAIM_PARTY_AK_ID,
	-- *INF*: IIF(
	-- ISNULL(:LKP.LKP_CLAIM_OCCURRENCE(CLAIM_OCCURRENCE_KEY)),
	-- 0,
	-- :LKP.LKP_CLAIM_OCCURRENCE(CLAIM_OCCURRENCE_KEY))
	IFF(LKP_CLAIM_OCCURRENCE_CLAIM_OCCURRENCE_KEY.claim_occurrence_ak_id IS NULL, 0, LKP_CLAIM_OCCURRENCE_CLAIM_OCCURRENCE_KEY.claim_occurrence_ak_id) AS CLAIM_OCCURRENCE_AK_ID,
	-- *INF*: TO_DATE('1/1/1800','MM/DD/YYYY')
	-- 
	TO_DATE('1/1/1800', 'MM/DD/YYYY') AS DENIAL_DATE,
	IPFCGQ_OFFSET_ONSET_IND AS in_IPFCGQ_OFFSET_ONSET_IND,
	-- *INF*: IIF((ISNULL(in_IPFCGQ_OFFSET_ONSET_IND) OR IS_SPACES(in_IPFCGQ_OFFSET_ONSET_IND) OR LENGTH(in_IPFCGQ_OFFSET_ONSET_IND) = 0),
	-- 'N/A',
	-- in_IPFCGQ_OFFSET_ONSET_IND)
	-- 
	IFF(( in_IPFCGQ_OFFSET_ONSET_IND IS NULL OR IS_SPACES(in_IPFCGQ_OFFSET_ONSET_IND) OR LENGTH(in_IPFCGQ_OFFSET_ONSET_IND) = 0 ), 'N/A', in_IPFCGQ_OFFSET_ONSET_IND) AS IPFCGQ_OFFSET_ONSET_IND
	FROM EXP_Values
	LEFT JOIN LKP_CLAIM_CASE_AK_ID LKP_CLAIM_CASE_AK_ID_V_Claim_Case_Key
	ON LKP_CLAIM_CASE_AK_ID_V_Claim_Case_Key.claim_case_key = V_Claim_Case_Key

	LEFT JOIN LKP_CLAIM_PARTY LKP_CLAIM_PARTY_CLAIM_PARTY_KEY
	ON LKP_CLAIM_PARTY_CLAIM_PARTY_KEY.claim_party_key = CLAIM_PARTY_KEY

	LEFT JOIN LKP_CLAIM_OCCURRENCE LKP_CLAIM_OCCURRENCE_CLAIM_OCCURRENCE_KEY
	ON LKP_CLAIM_OCCURRENCE_CLAIM_OCCURRENCE_KEY.claim_occurrence_key = CLAIM_OCCURRENCE_KEY

),
LKP_claim_party_occurrence AS (
	SELECT
	claim_party_occurrence_id,
	claim_party_occurrence_ak_id,
	denial_date,
	offset_onset_ind,
	claim_occurrence_ak_id,
	claim_party_role_code,
	claim_party_ak_id
	FROM (
		SELECT 
		a.claim_party_occurrence_id as claim_party_occurrence_id, 
		a.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id,
		a.denial_date as denial_date, 
		a.offset_onset_ind as offset_onset_ind,
		a.claim_occurrence_ak_id as claim_occurrence_ak_id, 
		ltrim(rtrim(a.claim_party_role_code)) as claim_party_role_code, 
		a.claim_party_ak_id as claim_party_ak_id 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence a
		WHERE a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND a.crrnt_snpsht_flag = 1
		ORDER BY claim_occurrence_ak_id, claim_party_role_code, claim_party_ak_id --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,claim_party_role_code,claim_party_ak_id ORDER BY claim_party_occurrence_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	EXP_Lkp_Values.CLAIM_PARTY_AK_ID,
	EXP_Lkp_Values.USE_CODE AS IPFCGQ_USE_CODE,
	EXP_Lkp_Values.CLAIM_OCCURRENCE_AK_ID,
	EXP_Lkp_Values.DENIAL_DATE,
	EXP_Lkp_Values.IPFCGQ_OFFSET_ONSET_IND,
	EXP_Lkp_Values.CLAIM_CASE_AK_ID,
	LKP_claim_party_occurrence.claim_party_occurrence_id AS lkp_claim_party_occurrence_id,
	LKP_claim_party_occurrence.claim_party_occurrence_ak_id AS lkp_claim_party_occurrence_ak_id,
	LKP_claim_party_occurrence.denial_date AS lkp_denial_date,
	LKP_claim_party_occurrence.offset_onset_ind AS lkp_offset_onset_ind,
	'0' AS logical_flag_op,
	1 AS Crrnt_Snpsht_Flag,
	-- *INF*: iif(isnull(lkp_claim_party_occurrence_id), 'NEW',
	-- 	iif (lkp_denial_date <> DENIAL_DATE OR 
	-- 	ltrim(rtrim(lkp_offset_onset_ind)) <> ltrim(rtrim(IPFCGQ_OFFSET_ONSET_IND)) , 
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(lkp_claim_party_occurrence_id IS NULL, 'NEW', IFF(lkp_denial_date <> DENIAL_DATE OR ltrim(rtrim(lkp_offset_onset_ind)) <> ltrim(rtrim(IPFCGQ_OFFSET_ONSET_IND)), 'UPDATE', 'NOCHANGE')) AS v_Changed_Flag,
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
	'N/A' AS out_claimant_num
	FROM EXP_Lkp_Values
	LEFT JOIN LKP_claim_party_occurrence
	ON LKP_claim_party_occurrence.claim_occurrence_ak_id = EXP_Lkp_Values.CLAIM_OCCURRENCE_AK_ID AND LKP_claim_party_occurrence.claim_party_role_code = EXP_Lkp_Values.USE_CODE AND LKP_claim_party_occurrence.claim_party_ak_id = EXP_Lkp_Values.CLAIM_PARTY_AK_ID
),
FIL_Insert AS (
	SELECT
	lkp_claim_party_occurrence_ak_id, 
	CLAIM_PARTY_AK_ID, 
	CLAIM_OCCURRENCE_AK_ID, 
	CLAIM_CASE_AK_ID, 
	IPFCGQ_USE_CODE, 
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
	out_claimant_num
	FROM EXP_Detect_Changes
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
SEQ_claim_party_occurrence AS (
	CREATE SEQUENCE SEQ_claim_party_occurrence
	START = 0
	INCREMENT = 1;
),
EXP_Determine_AK AS (
	SELECT
	SEQ_claim_party_occurrence.NEXTVAL,
	lkp_claim_party_occurrence_ak_id,
	-- *INF*: IIF(Changed_Flag='NEW', NEXTVAL, lkp_claim_party_occurrence_ak_id)
	IFF(Changed_Flag = 'NEW', NEXTVAL, lkp_claim_party_occurrence_ak_id) AS claim_party_occurrence_ak_id,
	CLAIM_PARTY_AK_ID,
	CLAIM_OCCURRENCE_AK_ID,
	CLAIM_CASE_AK_ID,
	IPFCGQ_USE_CODE,
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
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence
	(claim_party_occurrence_ak_id, claim_occurrence_ak_id, claim_party_ak_id, claim_case_ak_id, claim_party_role_code, claimant_num, denial_date, offset_onset_ind, logical_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, preferred_contact_method)
	SELECT 
	CLAIM_PARTY_OCCURRENCE_AK_ID, 
	CLAIM_OCCURRENCE_AK_ID AS CLAIM_OCCURRENCE_AK_ID, 
	CLAIM_PARTY_AK_ID AS CLAIM_PARTY_AK_ID, 
	CLAIM_CASE_AK_ID AS CLAIM_CASE_AK_ID, 
	IPFCGQ_USE_CODE AS CLAIM_PARTY_ROLE_CODE, 
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
	FROM EXP_Determine_AK
),
SQ_claim_party_occurrence AS (
	SELECT 
	a.claim_party_occurrence_id, 
	a.claim_occurrence_ak_id, 
	a.claim_party_role_code, 
	a.claim_party_ak_id, 
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
			GROUP BY claim_occurrence_ak_id, claim_party_role_code, claim_party_ak_id
			HAVING COUNT(*) > 1)
	ORDER BY claim_occurrence_ak_id, claim_party_role_code, claim_party_ak_id, eff_from_date  DESC
	
	--The extra condition ****** AND a.claim_party_occurrence_ak_id = b.claim_party_occurrence_ak_id *****  has been added on 11/18/2009 because 45GJ, 45GQ and 4578 sources loading claim_party_occurrence have different logical AK ids. 45GJ and 45GQ don't consider claim_case_ak_id as part of the overall AK id of target table whereas 4578 does. As a result, this pipeline was expiring some of the records loaded by 4578. In order to aviod that, this extra condition has been included.
),
EXP_Lag_eff_from_date AS (
	SELECT
	claim_party_occurrence_id,
	claim_party_role_code,
	claim_occurrence_ak_id,
	claim_party_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	claim_occurrence_ak_id = v_PREV_ROW_claim_occurrence_ak_id AND
	-- 	claim_party_ak_id = v_PREV_ROW_claim_party_ak_id AND
	-- 	claim_party_role_code = v_PREV_ROW_claim_party_role_code,
	-- ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- orig_eff_to_date)
	DECODE(TRUE,
		claim_occurrence_ak_id = v_PREV_ROW_claim_occurrence_ak_id AND claim_party_ak_id = v_PREV_ROW_claim_party_ak_id AND claim_party_role_code = v_PREV_ROW_claim_party_role_code, ADD_TO_DATE(v_PREV_ROW_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	claim_occurrence_ak_id AS v_PREV_ROW_claim_occurrence_ak_id,
	claim_party_ak_id AS v_PREV_ROW_claim_party_ak_id,
	claim_party_role_code AS v_PREV_ROW_claim_party_role_code,
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
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence AS T
	USING UPD_Claim_Party_Occurrence AS S
	ON T.claim_party_occurrence_id = S.claim_party_occurrence_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),
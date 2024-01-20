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
LKP_Claim_Party_Key AS (
	SELECT
	claim_party_key,
	claim_party_ak_id
	FROM (
		SELECT 
		claim_party.claim_party_key as claim_party_key, 
		claim_party.claim_party_ak_id as claim_party_ak_id 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party, @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence 
		WHERE 
		claim_party.claim_party_ak_id  = claim_party_occurrence.claim_party_ak_id
		AND claim_party.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND claim_party.crrnt_snpsht_flag = 1
		AND claim_party_occurrence.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND claim_party_occurrence.crrnt_snpsht_flag = 1
		AND claim_party_occurrence.claim_case_ak_id = -1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_ak_id ORDER BY claim_party_key) = 1
),
SQ_PIF_42GJ_stage AS (
	SELECT 
	LTRIM(RTRIM((PIF_SYMBOL + PIF_POLICY_NUMBER + PIF_MODULE + 
	CASE len(IPFC4J_LOSS_MONTH) when 1 THEN '0' + CAST(IPFC4J_LOSS_MONTH AS VARCHAR) ELSE CAST(IPFC4J_LOSS_MONTH AS VARCHAR) END + 
	CASE len(IPFC4J_LOSS_DAY) when 1 THEN '0' + CAST(IPFC4J_LOSS_DAY AS VARCHAR) ELSE CAST(IPFC4J_LOSS_DAY AS VARCHAR) END +
	CAST(IPFC4J_LOSS_YEAR AS VARCHAR) +
	CAST(IPFC4J_LOSS_OCCURENCE AS VARCHAR) + 
	CAST(IPFC4J_LOSS_CLAIMANT AS VARCHAR) + IPFC4J_USE_CODE))) as CLAIM_PARTY_KEY, 
	(PIF_SYMBOL + PIF_POLICY_NUMBER + PIF_MODULE + 
	CASE len(IPFC4J_LOSS_MONTH) when 1 THEN '0' + CAST(IPFC4J_LOSS_MONTH AS VARCHAR) ELSE CAST(IPFC4J_LOSS_MONTH AS VARCHAR) END + 
	CASE len(IPFC4J_LOSS_DAY) when 1 THEN '0' + CAST(IPFC4J_LOSS_DAY AS VARCHAR) ELSE CAST(IPFC4J_LOSS_DAY AS VARCHAR) END +
	CAST(IPFC4J_LOSS_YEAR AS VARCHAR) + CAST(IPFC4J_LOSS_OCCURENCE AS VARCHAR)) as CLAIM_OCCURRENCE_KEY,
	pif_42gj_stage.pif_symbol, pif_42gj_stage.pif_policy_number, pif_42gj_stage.pif_module, pif_42gj_stage.ipfc4j_loss_year, pif_42gj_stage.ipfc4j_loss_month, pif_42gj_stage.ipfc4j_loss_day, pif_42gj_stage.ipfc4j_loss_occurence, pif_42gj_stage.ipfc4j_loss_claimant, pif_42gj_stage.ipfc4j_use_code, pif_42gj_stage.ipfc4j_offset_onset_ind, pif_42gj_stage.logical_flag 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.PIF_42GJ_STAGE
	WHERE
	IPFC4J_USE_CODE IN ('CMT','DRV','PS','HS')
	and pif_42gj_stage.logical_flag in ('0','1','5')
),
EXP_Values AS (
	SELECT
	CLAIM_PARTY_KEY,
	CLAIM_OCCURRENCE_KEY,
	PIF_SYMBOL,
	PIF_POLICY_NUMBER,
	PIF_MODULE,
	IPFC4J_LOSS_YEAR,
	IPFC4J_LOSS_MONTH,
	IPFC4J_LOSS_DAY,
	IPFC4J_LOSS_OCCURENCE,
	IPFC4J_LOSS_CLAIMANT,
	IPFC4J_USE_CODE,
	IPFC4J_OFFSET_ONSET_IND,
	logical_flag
	FROM SQ_PIF_42GJ_stage
),
LKP_PIF_42GQ_CMT AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module,
	ipfcgq_year_of_loss,
	ipfcgq_month_of_loss,
	ipfcgq_day_of_loss,
	ipfcgq_loss_occurence,
	ipfcgq_loss_claimant,
	ipfcgq_claimant_use_code,
	ipfcgq_denial_year,
	ipfcgq_denial_month,
	ipfcgq_denial_day
	FROM (
		SELECT 
			pif_symbol,
			pif_policy_number,
			pif_module,
			ipfcgq_year_of_loss,
			ipfcgq_month_of_loss,
			ipfcgq_day_of_loss,
			ipfcgq_loss_occurence,
			ipfcgq_loss_claimant,
			ipfcgq_claimant_use_code,
			ipfcgq_denial_year,
			ipfcgq_denial_month,
			ipfcgq_denial_day
		FROM pif_42gq_cmt_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module,ipfcgq_year_of_loss,ipfcgq_month_of_loss,ipfcgq_day_of_loss,ipfcgq_loss_occurence,ipfcgq_loss_claimant,ipfcgq_claimant_use_code ORDER BY pif_symbol) = 1
),
EXP_Lkp_Values AS (
	SELECT
	EXP_Values.CLAIM_PARTY_KEY,
	EXP_Values.CLAIM_OCCURRENCE_KEY,
	EXP_Values.IPFC4J_USE_CODE,
	-- *INF*: LTRIM(RTRIM(IPFC4J_USE_CODE))
	LTRIM(RTRIM(IPFC4J_USE_CODE)) AS USE_CODE,
	-- *INF*: :LKP.LKP_CLAIM_PARTY(CLAIM_PARTY_KEY)
	-- 
	-- --IIF(ISNULL(:LKP.LKP_CLAIM_PARTY(CLAIM_PARTY_KEY)),0,:LKP.LKP_CLAIM_PARTY(CLAIM_PARTY_KEY))
	LKP_CLAIM_PARTY_CLAIM_PARTY_KEY.claim_party_ak_id AS CLAIM_PARTY_AK_ID,
	-- *INF*: :LKP.LKP_CLAIM_OCCURRENCE(CLAIM_OCCURRENCE_KEY)
	-- 
	-- --IIF (ISNULL(:LKP.LKP_CLAIM_OCCURRENCE(CLAIM_OCCURRENCE_KEY)),0,:LKP.LKP_CLAIM_OCCURRENCE(CLAIM_OCCURRENCE_KEY))
	LKP_CLAIM_OCCURRENCE_CLAIM_OCCURRENCE_KEY.claim_occurrence_ak_id AS CLAIM_OCCURRENCE_AK_ID,
	-- *INF*: LTRIM(RTRIM(SUBSTR(CLAIM_PARTY_KEY,1,26)))
	-- 
	LTRIM(RTRIM(SUBSTR(CLAIM_PARTY_KEY, 1, 26))) AS V_Claim_case_key,
	-- *INF*: :LKP.LKP_CLAIM_CASE_AK_ID(V_Claim_case_key)
	LKP_CLAIM_CASE_AK_ID_V_Claim_case_key.claim_case_ak_id AS v_Claim_Case_AK_ID,
	-- *INF*: IIF(ISNULL(v_Claim_Case_AK_ID),-1,v_Claim_Case_AK_ID)
	IFF(v_Claim_Case_AK_ID IS NULL, - 1, v_Claim_Case_AK_ID) AS Claim_Case_AK_ID,
	EXP_Values.IPFC4J_LOSS_CLAIMANT AS IPFCGQ_LOSS_CLAIMANT,
	-- *INF*: iif((ISNULL(IPFCGQ_LOSS_CLAIMANT) OR IS_SPACES(IPFCGQ_LOSS_CLAIMANT) OR LENGTH(IPFCGQ_LOSS_CLAIMANT)=0),
	-- 'N/A',
	-- rtrim(IPFCGQ_LOSS_CLAIMANT))
	IFF(
	    (IPFCGQ_LOSS_CLAIMANT IS NULL
	    or LENGTH(IPFCGQ_LOSS_CLAIMANT)>0
	    and TRIM(IPFCGQ_LOSS_CLAIMANT)=''
	    or LENGTH(IPFCGQ_LOSS_CLAIMANT) = 0),
	    'N/A',
	    rtrim(IPFCGQ_LOSS_CLAIMANT)
	) AS out_IPFCGQ_LOSS_CLAIMANT,
	EXP_Values.IPFC4J_OFFSET_ONSET_IND,
	-- *INF*: IIF((ISNULL(IPFC4J_OFFSET_ONSET_IND) OR IS_SPACES(IPFC4J_OFFSET_ONSET_IND) OR LENGTH(IPFC4J_OFFSET_ONSET_IND)=0),
	-- 'N/A',
	-- RTRIM(IPFC4J_OFFSET_ONSET_IND))
	IFF(
	    (IPFC4J_OFFSET_ONSET_IND IS NULL
	    or LENGTH(IPFC4J_OFFSET_ONSET_IND)>0
	    and TRIM(IPFC4J_OFFSET_ONSET_IND)=''
	    or LENGTH(IPFC4J_OFFSET_ONSET_IND) = 0),
	    'N/A',
	    RTRIM(IPFC4J_OFFSET_ONSET_IND)
	) AS out_IPFC4J_OFFSET_ONSET_IND,
	EXP_Values.logical_flag,
	LKP_PIF_42GQ_CMT.pif_symbol,
	LKP_PIF_42GQ_CMT.pif_policy_number,
	LKP_PIF_42GQ_CMT.pif_module,
	LKP_PIF_42GQ_CMT.ipfcgq_year_of_loss,
	LKP_PIF_42GQ_CMT.ipfcgq_month_of_loss,
	LKP_PIF_42GQ_CMT.ipfcgq_day_of_loss,
	LKP_PIF_42GQ_CMT.ipfcgq_loss_occurence,
	LKP_PIF_42GQ_CMT.ipfcgq_loss_claimant AS ipfcgq_loss_claimant1,
	LKP_PIF_42GQ_CMT.ipfcgq_claimant_use_code,
	LKP_PIF_42GQ_CMT.ipfcgq_denial_year,
	LKP_PIF_42GQ_CMT.ipfcgq_denial_month,
	LKP_PIF_42GQ_CMT.ipfcgq_denial_day,
	-- *INF*: IIF(ISNULL(ipfcgq_denial_year) OR LENGTH(to_char(ipfcgq_denial_year)) =0, '0000', TO_CHAR(ipfcgq_denial_year))
	IFF(
	    ipfcgq_denial_year IS NULL OR LENGTH(to_char(ipfcgq_denial_year)) = 0, '0000',
	    TO_CHAR(ipfcgq_denial_year)
	) AS var_DENIAL_year,
	-- *INF*: IIF(ISNULL(ipfcgq_denial_month) OR LENGTH(to_char(ipfcgq_denial_month)) =0, '00', 
	-- IIF(LENGTH(to_char(ipfcgq_denial_month)) =1, '0'  || TO_CHAR(ipfcgq_denial_month)))
	IFF(
	    ipfcgq_denial_month IS NULL OR LENGTH(to_char(ipfcgq_denial_month)) = 0, '00',
	    IFF(
	        LENGTH(to_char(ipfcgq_denial_month)) = 1, '0' || TO_CHAR(ipfcgq_denial_month)
	    )
	) AS var_DENIAL_month,
	-- *INF*: IIF(ISNULL(ipfcgq_denial_day) OR LENGTH(to_char(ipfcgq_denial_day)) =0, '00', 
	-- IIF(LENGTH(to_char(ipfcgq_denial_day)) =1, '0'  || TO_CHAR(ipfcgq_denial_day)))
	IFF(
	    ipfcgq_denial_day IS NULL OR LENGTH(to_char(ipfcgq_denial_day)) = 0, '00',
	    IFF(
	        LENGTH(to_char(ipfcgq_denial_day)) = 1, '0' || TO_CHAR(ipfcgq_denial_day)
	    )
	) AS var_DENIAL_day,
	var_DENIAL_month || '/' || var_DENIAL_day || '/' ||  var_DENIAL_year AS var_DENIAL_DATE,
	-- *INF*: IIF(LENGTH(var_DENIAL_DATE) < 10 OR var_DENIAL_year='0000' OR var_DENIAL_month = '00' OR var_DENIAL_day = '00',
	-- TO_DATE('1/1/1800','MM/DD/YYYY'),
	-- TO_DATE(var_DENIAL_DATE,'MM/DD/YYYY'))
	IFF(
	    LENGTH(var_DENIAL_DATE) < 10
	    or var_DENIAL_year = '0000'
	    or var_DENIAL_month = '00'
	    or var_DENIAL_day = '00',
	    TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'),
	    TO_TIMESTAMP(var_DENIAL_DATE, 'MM/DD/YYYY')
	) AS out_DENIAL_DATE
	FROM EXP_Values
	LEFT JOIN LKP_PIF_42GQ_CMT
	ON LKP_PIF_42GQ_CMT.pif_symbol = EXP_Values.PIF_SYMBOL AND LKP_PIF_42GQ_CMT.pif_policy_number = EXP_Values.PIF_POLICY_NUMBER AND LKP_PIF_42GQ_CMT.pif_module = EXP_Values.PIF_MODULE AND LKP_PIF_42GQ_CMT.ipfcgq_year_of_loss = EXP_Values.IPFC4J_LOSS_YEAR AND LKP_PIF_42GQ_CMT.ipfcgq_month_of_loss = EXP_Values.IPFC4J_LOSS_MONTH AND LKP_PIF_42GQ_CMT.ipfcgq_day_of_loss = EXP_Values.IPFC4J_LOSS_DAY AND LKP_PIF_42GQ_CMT.ipfcgq_loss_occurence = EXP_Values.IPFC4J_LOSS_OCCURENCE AND LKP_PIF_42GQ_CMT.ipfcgq_loss_claimant = EXP_Values.IPFC4J_LOSS_CLAIMANT AND LKP_PIF_42GQ_CMT.ipfcgq_claimant_use_code = EXP_Values.IPFC4J_USE_CODE
	LEFT JOIN LKP_CLAIM_PARTY LKP_CLAIM_PARTY_CLAIM_PARTY_KEY
	ON LKP_CLAIM_PARTY_CLAIM_PARTY_KEY.claim_party_key = CLAIM_PARTY_KEY

	LEFT JOIN LKP_CLAIM_OCCURRENCE LKP_CLAIM_OCCURRENCE_CLAIM_OCCURRENCE_KEY
	ON LKP_CLAIM_OCCURRENCE_CLAIM_OCCURRENCE_KEY.claim_occurrence_key = CLAIM_OCCURRENCE_KEY

	LEFT JOIN LKP_CLAIM_CASE_AK_ID LKP_CLAIM_CASE_AK_ID_V_Claim_case_key
	ON LKP_CLAIM_CASE_AK_ID_V_Claim_case_key.claim_case_key = V_Claim_case_key

),
LKP_claim_party_occurrence AS (
	SELECT
	claim_party_occurrence_id,
	claim_party_occurrence_ak_id,
	claimant_num,
	denial_date,
	offset_onset_ind,
	claim_occurrence_ak_id,
	claim_party_role_code,
	claim_party_ak_id
	FROM (
		SELECT 
		a.claim_party_occurrence_id as claim_party_occurrence_id, 
		a.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, 
		a.claimant_num as claimant_num,
		a.denial_date as denial_date, 
		a.offset_onset_ind as offset_onset_ind,
		a.claim_occurrence_ak_id as claim_occurrence_ak_id, 
		ltrim(rtrim(a.claim_party_role_code)) as claim_party_role_code, 
		a.claim_party_ak_id as claim_party_ak_id 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence a
		WHERE  a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND a.crrnt_snpsht_flag = 1
		ORDER BY claim_occurrence_ak_id, claim_party_role_code, claim_party_ak_id --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,claim_party_role_code,claim_party_ak_id ORDER BY claim_party_occurrence_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	EXP_Lkp_Values.CLAIM_PARTY_AK_ID,
	EXP_Lkp_Values.USE_CODE AS IPFC4J_USE_CODE,
	EXP_Lkp_Values.CLAIM_OCCURRENCE_AK_ID,
	EXP_Lkp_Values.Claim_Case_AK_ID,
	EXP_Lkp_Values.out_DENIAL_DATE AS DENIAL_DATE,
	EXP_Lkp_Values.out_IPFCGQ_LOSS_CLAIMANT AS IPFCGQ_LOSS_CLAIMANT,
	EXP_Lkp_Values.out_IPFC4J_OFFSET_ONSET_IND AS IPFC4J_OFFSET_ONSET_IND,
	LKP_claim_party_occurrence.claim_party_occurrence_id AS lkp_claim_party_occurrence_id,
	LKP_claim_party_occurrence.claim_party_occurrence_ak_id AS lkp_claim_party_occurrence_ak_id,
	LKP_claim_party_occurrence.denial_date AS lkp_denial_date,
	LKP_claim_party_occurrence.claimant_num AS lkp_claimant_num,
	LKP_claim_party_occurrence.offset_onset_ind AS lkp_offset_onset_ind,
	EXP_Lkp_Values.logical_flag,
	1 AS Crrnt_Snpsht_Flag,
	-- *INF*: iif(isnull(lkp_claim_party_occurrence_id),'NEW',
	-- 	iif (lkp_denial_date <> DENIAL_DATE OR 
	-- 	ltrim(rtrim(lkp_claimant_num)) <> ltrim(rtrim(IPFCGQ_LOSS_CLAIMANT)) OR 
	-- 	ltrim(rtrim(lkp_offset_onset_ind)) <> ltrim(rtrim(IPFC4J_OFFSET_ONSET_IND)) , 
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(
	    lkp_claim_party_occurrence_id IS NULL, 'NEW',
	    IFF(
	        lkp_denial_date <> DENIAL_DATE
	        or ltrim(rtrim(lkp_claimant_num)) <> ltrim(rtrim(IPFCGQ_LOSS_CLAIMANT))
	        or ltrim(rtrim(lkp_offset_onset_ind)) <> ltrim(rtrim(IPFC4J_OFFSET_ONSET_IND)),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_Id,
	-- *INF*: IIF(v_Changed_Flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	SYSDATE)
	IFF(
	    v_Changed_Flag = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS Eff_From_Date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS Eff_To_Date,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID,
	SYSDATE AS Created_Date,
	SYSDATE AS Modified_Date
	FROM EXP_Lkp_Values
	LEFT JOIN LKP_claim_party_occurrence
	ON LKP_claim_party_occurrence.claim_occurrence_ak_id = EXP_Lkp_Values.CLAIM_OCCURRENCE_AK_ID AND LKP_claim_party_occurrence.claim_party_role_code = EXP_Lkp_Values.USE_CODE AND LKP_claim_party_occurrence.claim_party_ak_id = EXP_Lkp_Values.CLAIM_PARTY_AK_ID
),
FIL_Insert AS (
	SELECT
	lkp_claim_party_occurrence_ak_id, 
	CLAIM_OCCURRENCE_AK_ID, 
	CLAIM_PARTY_AK_ID, 
	Claim_Case_AK_ID, 
	IPFC4J_USE_CODE, 
	DENIAL_DATE, 
	IPFCGQ_LOSS_CLAIMANT, 
	IPFC4J_OFFSET_ONSET_IND, 
	logical_flag, 
	Crrnt_Snpsht_Flag, 
	Audit_Id, 
	Eff_From_Date, 
	Eff_To_Date, 
	SOURCE_SYSTEM_ID, 
	Created_Date, 
	Modified_Date, 
	Changed_Flag
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
	CLAIM_OCCURRENCE_AK_ID,
	CLAIM_PARTY_AK_ID,
	Claim_Case_AK_ID,
	IPFC4J_USE_CODE,
	DENIAL_DATE,
	IPFCGQ_LOSS_CLAIMANT,
	IPFC4J_OFFSET_ONSET_IND,
	logical_flag,
	Crrnt_Snpsht_Flag,
	Audit_Id,
	Eff_From_Date,
	Eff_To_Date,
	SOURCE_SYSTEM_ID,
	Created_Date,
	Modified_Date,
	Changed_Flag,
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
	Claim_Case_AK_ID AS CLAIM_CASE_AK_ID, 
	IPFC4J_USE_CODE AS CLAIM_PARTY_ROLE_CODE, 
	IPFCGQ_LOSS_CLAIMANT AS CLAIMANT_NUM, 
	DENIAL_DATE AS DENIAL_DATE, 
	IPFC4J_OFFSET_ONSET_IND AS OFFSET_ONSET_IND, 
	LOGICAL_FLAG, 
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
	 EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence b
			WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.claim_occurrence_ak_id = b.claim_occurrence_ak_id
	            AND a.claim_party_occurrence_ak_id = b.claim_party_occurrence_ak_id
			AND a.claim_party_role_code = b.claim_party_role_code
			AND a.claim_party_ak_id = b.claim_party_ak_id
			GROUP BY claim_occurrence_ak_id, claim_party_role_code, claim_party_ak_id
			HAVING COUNT(*) > 1)
	ORDER BY claim_occurrence_ak_id, claim_party_role_code, claim_party_ak_id, eff_from_date  DESC
	
	--The extra condition ****** AND a.claim_party_occurrence_ak_id = b.claim_party_occurrence_ak_id *****  has been added on 11/18/2009 because 45GJ, 45GQ and 4578 sources loading claim_party_occurrence have different logical AK ids. 45GJ and 45GQ don't consider claim_case_ak_id as part of the overall AK id of target table whereas 4578 does. As a result, this pipeline was expiring some of the records loaded by 4578. In order to aviod that, this extra condition has been included.
),
EXP_Lag_eff_from_date AS (
	SELECT
	claim_party_occurrence_id,
	claim_occurrence_ak_id,
	claim_party_role_code,
	claim_party_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	claim_occurrence_ak_id = v_PREV_ROW_claim_occurrence_ak_id AND
	-- 	claim_party_ak_id = v_PREV_ROW_claim_party_ak_id AND
	-- 	claim_party_role_code = v_PREV_ROW_claim_party_role_code,
	-- ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- orig_eff_to_date)
	DECODE(
	    TRUE,
	    claim_occurrence_ak_id = v_PREV_ROW_claim_occurrence_ak_id AND claim_party_ak_id = v_PREV_ROW_claim_party_ak_id AND claim_party_role_code = v_PREV_ROW_claim_party_role_code, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
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
SQ_claim_party_occurrence_claim_case_ak_id_update AS (
	SELECT 
	claim_party_occurrence.claim_party_occurrence_id, 
	claim_party_occurrence.claim_party_occurrence_ak_id, 
	claim_party_occurrence.claim_occurrence_ak_id, 
	claim_party_occurrence.claim_party_ak_id, 
	claim_party_occurrence.claim_case_ak_id 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence 
	WHERE
	 claim_party_occurrence.claim_case_ak_id = -1 
	AND claim_party_occurrence.crrnt_snpsht_flag = 1
	AND claim_party_occurrence.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
),
EXP_Evaluate AS (
	SELECT
	claim_party_occurrence_id,
	claim_party_occurrence_ak_id,
	claim_occurrence_ak_id,
	claim_party_ak_id,
	claim_case_ak_id,
	-- *INF*: :LKP.LKP_CLAIM_PARTY_KEY(claim_party_ak_id)
	LKP_CLAIM_PARTY_KEY_claim_party_ak_id.claim_party_key AS v_claim_party_key,
	-- *INF*: SUBSTR(v_claim_party_key,1,26)
	SUBSTR(v_claim_party_key, 1, 26) AS v_Claim_Case_Key,
	-- *INF*: :LKP.LKP_CLAIM_CASE_AK_ID(v_Claim_Case_Key)
	LKP_CLAIM_CASE_AK_ID_v_Claim_Case_Key.claim_case_ak_id AS v_Claim_Case_Ak_id,
	-- *INF*: IIF(ISNULL(v_Claim_Case_Ak_id),-1,v_Claim_Case_Ak_id)
	IFF(v_Claim_Case_Ak_id IS NULL, - 1, v_Claim_Case_Ak_id) AS Out_Claim_Case_Ak_id
	FROM SQ_claim_party_occurrence_claim_case_ak_id_update
	LEFT JOIN LKP_CLAIM_PARTY_KEY LKP_CLAIM_PARTY_KEY_claim_party_ak_id
	ON LKP_CLAIM_PARTY_KEY_claim_party_ak_id.claim_party_ak_id = claim_party_ak_id

	LEFT JOIN LKP_CLAIM_CASE_AK_ID LKP_CLAIM_CASE_AK_ID_v_Claim_Case_Key
	ON LKP_CLAIM_CASE_AK_ID_v_Claim_Case_Key.claim_case_key = v_Claim_Case_Key

),
FIL_Claim_Case_Ak_id AS (
	SELECT
	claim_party_occurrence_id, 
	Out_Claim_Case_Ak_id
	FROM EXP_Evaluate
	WHERE Out_Claim_Case_Ak_id  != -1
),
UPD_Claim_Case_Ak_id_Update AS (
	SELECT
	claim_party_occurrence_id, 
	Out_Claim_Case_Ak_id
	FROM FIL_Claim_Case_Ak_id
),
claim_party_occurrence_Update_Claim_Case_Ak_id AS (
	MERGE INTO claim_party_occurrence AS T
	USING UPD_Claim_Case_Ak_id_Update AS S
	ON T.claim_party_occurrence_id = S.claim_party_occurrence_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.claim_case_ak_id = S.Out_Claim_Case_Ak_id
),
WITH
LKP_Sup_State AS (
	SELECT
	state_code,
	state_abbrev
	FROM (
		SELECT 
		ltrim(rtrim(a.state_code)) as state_code, 
		case when len(ltrim(rtrim(a.state_abbrev)))=1 then '0' + ltrim(rtrim(a.state_abbrev)) ELSE ltrim(rtrim(a.state_abbrev)) END  as state_abbrev 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_state a
		WHERE
		crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_abbrev ORDER BY state_code) = 1
),
SQ_pif_42gp_stage AS (
	SELECT A.pif_symbol, A.pif_policy_number, A.pif_module, A.ipfcgp_year_of_loss, A.ipfcgp_month_of_loss, A.ipfcgp_day_of_loss, A.ipfcgp_loss_occurence, A.ipfcgp_loss_adjustor_no, A.ipfcgp_loss_accident_state, A.ipfcgp_loss_catastrophe_no, A.ipfcgp_loss_time, A.ipfcgp_discovery_year, A.ipfcgp_discovery_month, A.ipfcgp_discovery_day, A.ipfcgp_claim_status, A.ipfcgp_accident_description, A.ipfcgp_offset_onset_ind, A.logical_flag 
	FROM
	 pif_42gp_stage A
	WHERE A.logical_flag IN ('0','1')
),
EXP_Value AS (
	SELECT
	ipfcgp_loss_accident_state AS CLM_LOSS_STATE_CD,
	ipfcgp_loss_catastrophe_no AS in_CLM_CATASTROPHE_CODE,
	-- *INF*: iif(isnull(in_CLM_CATASTROPHE_CODE),'N/A',
	--    iif(is_spaces(in_CLM_CATASTROPHE_CODE),'N/A',
	--     LPAD(rtrim(in_CLM_CATASTROPHE_CODE),3,'0')))
	IFF(
	    in_CLM_CATASTROPHE_CODE IS NULL, 'N/A',
	    IFF(
	        LENGTH(in_CLM_CATASTROPHE_CODE)>0
	    and TRIM(in_CLM_CATASTROPHE_CODE)='', 'N/A',
	        LPAD(rtrim(in_CLM_CATASTROPHE_CODE), 3, '0')
	    )
	) AS CLM_CATASTROPHE_CODE,
	pif_symbol AS PIF_SYMBOL,
	pif_policy_number AS PIF_POLICY_NUMBER,
	pif_module AS PIF_MODULE,
	ipfcgp_year_of_loss AS IPFCGP_YEAR_OF_LOSS,
	-- *INF*: TO_CHAR(IPFCGP_YEAR_OF_LOSS)
	TO_CHAR(IPFCGP_YEAR_OF_LOSS) AS v_IPFCGP_YEAR_OF_LOSS,
	ipfcgp_month_of_loss AS IPFCGP_MONTH_OF_LOSS,
	-- *INF*: TO_CHAR(IPFCGP_MONTH_OF_LOSS)
	TO_CHAR(IPFCGP_MONTH_OF_LOSS) AS v_IPFCGP_MONTH_OF_LOSS,
	ipfcgp_day_of_loss AS IPFCGP_DAY_OF_LOSS,
	-- *INF*: TO_CHAR(IPFCGP_DAY_OF_LOSS)
	TO_CHAR(IPFCGP_DAY_OF_LOSS) AS v_IPFCGP_DAY_OF_LOSS,
	ipfcgp_loss_occurence AS in_IPFCGP_LOSS_OCCURENCE,
	-- *INF*: TO_INTEGER(in_IPFCGP_LOSS_OCCURENCE)
	CAST(in_IPFCGP_LOSS_OCCURENCE AS INTEGER) AS v_IPFCGP_LOSS_OCCURENCE,
	v_IPFCGP_LOSS_OCCURENCE AS IPFCGP_LOSS_OCCURENCE,
	ipfcgp_loss_time AS IPFCGP_LOSS_TIME,
	-- *INF*: TO_INTEGER(IPFCGP_LOSS_TIME)
	CAST(IPFCGP_LOSS_TIME AS INTEGER) AS v_IPFCGP_LOSS_TIME,
	ipfcgp_discovery_year AS IPFCGP_DISCOVERY_YEAR,
	ipfcgp_discovery_month AS IPFCGP_DISCOVERY_MONTH,
	ipfcgp_discovery_day AS IPFCGP_DISCOVERY_DAY,
	PIF_SYMBOL  || PIF_POLICY_NUMBER  || PIF_MODULE AS v_SYM_NUM_MODE,
	-- *INF*: IIF ( LENGTH(v_IPFCGP_MONTH_OF_LOSS) = 1, '0' || v_IPFCGP_MONTH_OF_LOSS, v_IPFCGP_MONTH_OF_LOSS)
	-- ||  
	-- IIF ( LENGTH(v_IPFCGP_DAY_OF_LOSS ) = 1, '0' || v_IPFCGP_DAY_OF_LOSS, v_IPFCGP_DAY_OF_LOSS )
	-- ||  
	-- v_IPFCGP_YEAR_OF_LOSS
	-- 
	-- 
	IFF(
	    LENGTH(v_IPFCGP_MONTH_OF_LOSS) = 1, '0' || v_IPFCGP_MONTH_OF_LOSS, v_IPFCGP_MONTH_OF_LOSS
	) || IFF(LENGTH(v_IPFCGP_DAY_OF_LOSS) = 1, '0' || v_IPFCGP_DAY_OF_LOSS, v_IPFCGP_DAY_OF_LOSS) || v_IPFCGP_YEAR_OF_LOSS AS v_CLM_LOSS_DT,
	-- *INF*: IIF ((ISNULL(IPFCGP_MONTH_OF_LOSS ) OR ISNULL(IPFCGP_DAY_OF_LOSS) OR ISNULL(IPFCGP_YEAR_OF_LOSS) )
	-- , TO_DATE ('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	-- ,TO_DATE(v_IPFCGP_MONTH_OF_LOSS  || '/' ||  v_IPFCGP_DAY_OF_LOSS  || '/' ||  v_IPFCGP_YEAR_OF_LOSS , 'MM/DD/YYYY')
	-- )
	IFF(
	    (IPFCGP_MONTH_OF_LOSS IS NULL OR IPFCGP_DAY_OF_LOSS IS NULL OR IPFCGP_YEAR_OF_LOSS IS NULL),
	    TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    TO_TIMESTAMP(v_IPFCGP_MONTH_OF_LOSS || '/' || v_IPFCGP_DAY_OF_LOSS || '/' || v_IPFCGP_YEAR_OF_LOSS, 'MM/DD/YYYY')
	) AS CLM_LOSS_DT,
	-- *INF*: v_SYM_NUM_MODE  || v_CLM_LOSS_DT  || in_IPFCGP_LOSS_OCCURENCE
	-- 
	-- --v_SYM_NUM_MODE  || v_CLM_LOSS_DT  || v_IPFCGP_LOSS_OCCURENCE
	v_SYM_NUM_MODE || v_CLM_LOSS_DT || in_IPFCGP_LOSS_OCCURENCE AS CLM_CLAIM_NBR,
	-- *INF*: IIF ((ISNULL(IPFCGP_DISCOVERY_MONTH) OR ISNULL(IPFCGP_DISCOVERY_DAY ) OR ISNULL(IPFCGP_DISCOVERY_YEAR) OR IS_SPACES(IPFCGP_DISCOVERY_MONTH) OR IS_SPACES(IPFCGP_DISCOVERY_DAY ) OR IS_SPACES(IPFCGP_DISCOVERY_YEAR))
	-- , TO_DATE ('01/01/1800','MM/DD/YYYY')
	-- ,TO_DATE ((IPFCGP_DISCOVERY_MONTH || '/' || IPFCGP_DISCOVERY_DAY || '/' || IPFCGP_DISCOVERY_YEAR),'MM/DD/YYYY')
	-- )
	IFF(
	    (IPFCGP_DISCOVERY_MONTH IS NULL
	    or IPFCGP_DISCOVERY_DAY IS NULL
	    or IPFCGP_DISCOVERY_YEAR IS NULL
	    or LENGTH(IPFCGP_DISCOVERY_MONTH)>0
	    and TRIM(IPFCGP_DISCOVERY_MONTH)=''
	    or LENGTH(IPFCGP_DISCOVERY_DAY)>0
	    and TRIM(IPFCGP_DISCOVERY_DAY)=''
	    or LENGTH(IPFCGP_DISCOVERY_YEAR)>0
	    and TRIM(IPFCGP_DISCOVERY_YEAR)=''),
	    TO_TIMESTAMP('01/01/1800', 'MM/DD/YYYY'),
	    TO_TIMESTAMP((IPFCGP_DISCOVERY_MONTH || '/' || IPFCGP_DISCOVERY_DAY || '/' || IPFCGP_DISCOVERY_YEAR), 'MM/DD/YYYY')
	) AS CLM_DISCOVERY_DT,
	v_SYM_NUM_MODE AS CON_POLICY_KEY,
	ipfcgp_accident_description,
	ipfcgp_offset_onset_ind,
	logical_flag,
	ipfcgp_loss_adjustor_no,
	ipfcgp_claim_status
	FROM SQ_pif_42gp_stage
),
LKP_Claim_Stage AS (
	SELECT
	claim_id,
	policy_sym,
	policy_num,
	policy_mod,
	loss_month,
	loss_day,
	loss_year,
	loss_occurrence
	FROM (
		SELECT 
		a.claim_id as claim_id, 
		a.policy_sym as policy_sym, 
		a.policy_num as policy_num, 
		a.policy_mod as policy_mod, 
		MONTH(a.loss_date) as loss_month, 
		DAY(a.loss_date) as loss_day, 
		YEAR(a.loss_date) as loss_year, 
		a.loss_occurrence as loss_occurrence 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_stage a
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY policy_sym,policy_num,policy_mod,loss_month,loss_day,loss_year,loss_occurrence ORDER BY claim_id) = 1
),
LKP_Log_Note_Stage AS (
	SELECT
	create_date,
	claim_id
	FROM (
		SELECT 
		MAX(a.create_date) as create_date, 
		a.claim_id as claim_id 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.log_note_stage a
		GROUP BY a.claim_id
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY create_date) = 1
),
LKP_PIF_42X6_Stage AS (
	SELECT
	min_reported_date,
	pif_symbol,
	pif_policy_number,
	pif_module,
	ipfcx6_year_of_loss,
	ipfcx6_month_of_loss,
	ipfcx6_day_of_loss,
	ipfcx6_usr_loss_occurence
	FROM (
		SELECT  
		MIN(
		(CASE LEN(CONVERT(varchar(2), A.ipfcx6_loss_reported_month)) WHEN 1 THEN '0' + CONVERT(varchar(2), A.ipfcx6_loss_reported_month) 
		                     ELSE CONVERT(varchar(2), A.ipfcx6_loss_reported_month) END) + '/' + 
		                        (CASE LEN(CONVERT(varchar(2), A.ipfcx6_loss_reported_day)) WHEN 1 THEN '0' + CONVERT(varchar(2), A.ipfcx6_loss_reported_day) 
		                     ELSE CONVERT(varchar(2), A.ipfcx6_loss_reported_day) END) + '/' + CONVERT(varchar(4), A.ipfcx6_loss_reported_year) 
		) as min_reported_date,
		A.pif_symbol as pif_symbol, 
		A.pif_policy_number as pif_policy_number, 
		A.pif_module as pif_module, 
		A.ipfcx6_year_of_loss as ipfcx6_year_of_loss, 
		A.ipfcx6_month_of_loss as ipfcx6_month_of_loss, 
		A.ipfcx6_day_of_loss as ipfcx6_day_of_loss, 
		(A.ipfcx6_loss_occ_fdigit + A.ipfcx6_usr_loss_occurence) as ipfcx6_usr_loss_occurence 
		FROM pif_42x6_stage A
		WHERE  A.logical_flag IN ('0','1')
		GROUP BY 
		pif_symbol, 
		pif_policy_number, 
		pif_module, 
		ipfcx6_year_of_loss, 
		ipfcx6_month_of_loss, 
		ipfcx6_day_of_loss, 
		(ipfcx6_loss_occ_fdigit + ipfcx6_usr_loss_occurence)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module,ipfcx6_year_of_loss,ipfcx6_month_of_loss,ipfcx6_day_of_loss,ipfcx6_usr_loss_occurence ORDER BY min_reported_date) = 1
),
LKP_Pif_42gm_Stage AS (
	SELECT
	ipfc4m_rsk_rmk_verbg,
	pif_symbol,
	pif_policy_number,
	pif_module,
	ipfc4m_loss_year,
	ipfc4m_loss_month,
	ipfc4m_loss_day,
	ipfc4m_loss_occurence
	FROM (
		SELECT 
		a.ipfc4m_rsk_rmk_verbg as ipfc4m_rsk_rmk_verbg, 
		a.pif_symbol as pif_symbol, 
		a.pif_policy_number as pif_policy_number, 
		a.pif_module as pif_module, 
		a.ipfc4m_loss_year as ipfc4m_loss_year, 
		a.ipfc4m_loss_month as ipfc4m_loss_month, 
		a.ipfc4m_loss_day as ipfc4m_loss_day, 
		a.ipfc4m_loss_occurence as ipfc4m_loss_occurence
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_42gm_stage a
		WHERE ipfc4m_use_code='LOD'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module,ipfc4m_loss_year,ipfc4m_loss_month,ipfc4m_loss_day,ipfc4m_loss_occurence ORDER BY ipfc4m_rsk_rmk_verbg DESC) = 1
),
LKP_Pif_42gq_Aut_Stage AS (
	SELECT
	ipfcgq_driver_number,
	pif_symbol,
	pif_policy_number,
	pif_module,
	ipfcgq_year_of_loss,
	ipfcgq_month_of_loss,
	ipfcgq_day_of_loss,
	ipfcgq_loss_occurence
	FROM (
		SELECT 
			ipfcgq_driver_number,
			pif_symbol,
			pif_policy_number,
			pif_module,
			ipfcgq_year_of_loss,
			ipfcgq_month_of_loss,
			ipfcgq_day_of_loss,
			ipfcgq_loss_occurence
		FROM pif_42gq_aut_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module,ipfcgq_year_of_loss,ipfcgq_month_of_loss,ipfcgq_day_of_loss,ipfcgq_loss_occurence ORDER BY ipfcgq_driver_number) = 1
),
LKP_Pif_4578_Stage AS (
	SELECT
	loss_fault_code,
	pif_symbol,
	pif_policy_number,
	pif_module,
	loss_year,
	loss_month,
	loss_day,
	loss_occurence
	FROM (
		SELECT 
		a.loss_fault_code as loss_fault_code, 
		a.pif_symbol as pif_symbol, 
		a.pif_policy_number as pif_policy_number, 
		a.pif_module as pif_module, 
		a.loss_year as loss_year, 
		a.loss_month as loss_month, 
		a.loss_day as loss_day, 
		a.loss_occurence as loss_occurence 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_4578_stage a
		ORDER BY loss_transaction_date, loss_fault_code ASC --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module,loss_year,loss_month,loss_day,loss_occurence ORDER BY loss_fault_code DESC) = 1
),
LKP_Sup_Claim_Catastrophe_Code AS (
	SELECT
	cat_start_date,
	cat_end_date,
	cat_code
	FROM (
		SELECT 
		a.cat_start_date as cat_start_date, 
		a.cat_end_date as cat_end_date, 
		rtrim(ltrim(a.cat_code)) as cat_code 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_catastrophe_code a
		WHERE
		crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY cat_code ORDER BY cat_start_date) = 1
),
LKP_Sup_State_sup_state_id AS (
	SELECT
	sup_state_id,
	state_abbrev
	FROM (
		SELECT 
			sup_state_id,
			state_abbrev
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_state
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_abbrev ORDER BY sup_state_id) = 1
),
LKP_V2_Policy AS (
	SELECT
	pol_key_ak_id,
	pol_key
	FROM (
		SELECT 
		a.pol_ak_id as pol_key_ak_id, 
		a.pol_key as pol_key 
		FROM V2.policy a
		WHERE a.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_key_ak_id) = 1
),
mplt_claim_occurrence_next_diary AS (WITH
	INPUT AS (
		
	),
	LKP_Task_NextDueDiary_ByClaim AS (
		SELECT
		DueDate,
		ClaimId
		FROM (
			select MIN(T.DueDate) as DueDate, T.ClaimId as ClaimId 
			from @{pipeline().parameters.SOURCE_TABLE_OWNER}.TaskStage T 
			join @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupTaskStatusStage STS on T.SupTaskStatusId = STS.SupTaskStatusId and STS.Description = 'Open' 
			where T.SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
			group by T.ClaimId
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY ClaimId ORDER BY DueDate DESC) = 1
	),
	LKP_Task_DiaryByClaimAndDueDate AS (
		SELECT
		DueDate,
		AssignedUserName,
		ClaimId
		FROM (
			select T.DueDate as DueDate, T.AssignedUserName as AssignedUserName, T.ClaimId as ClaimId
			from @{pipeline().parameters.SOURCE_TABLE_OWNER}.TaskStage T
			join @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupTaskStatusStage STS on T.SupTaskStatusId = STS.SupTaskStatusId and STS.Description = 'Open' 
			where T.SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY ClaimId,DueDate ORDER BY DueDate DESC) = 1
	),
	OUTPUT AS (
		SELECT
		DueDate, 
		AssignedUserName
		FROM LKP_Task_DiaryByClaimAndDueDate
	),
),
EXP_Lkp_Values AS (
	SELECT
	EXP_Value.CLM_CLAIM_NBR,
	'N/A' AS CLM_CSR_CLAIM_NBR,
	'N/A' AS CLM_TYPE_CD,
	'N/A' AS CLM_POSTAL_CD,
	EXP_Value.CLM_DISCOVERY_DT AS in_CLM_DISCOVERY_DT,
	-- *INF*: iif(isnull(in_CLM_DISCOVERY_DT),
	-- TO_DATE('1/1/1800','MM/DD/YYYY'),
	-- in_CLM_DISCOVERY_DT)
	IFF(
	    in_CLM_DISCOVERY_DT IS NULL, TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'), in_CLM_DISCOVERY_DT
	) AS CLM_DISCOVERY_DT,
	EXP_Value.CLM_LOSS_DT AS in_CLM_LOSS_DT,
	-- *INF*: iif(isnull(in_CLM_LOSS_DT),
	-- TO_DATE('1/1/1800','MM/DD/YYYY'),
	-- in_CLM_LOSS_DT)
	IFF(in_CLM_LOSS_DT IS NULL, TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'), in_CLM_LOSS_DT) AS CLM_LOSS_DT,
	'N/A' AS CLM_LOSS_CITY,
	'N/A' AS CLM_LOSS_COUNTY,
	EXP_Value.CLM_LOSS_STATE_CD AS in_CLM_LOSS_STATE_CD,
	-- *INF*: IIF(ISNULL(:LKP.lkp_sup_state(in_CLM_LOSS_STATE_CD)),
	-- 'N/A',
	-- :LKP.lkp_sup_state(in_CLM_LOSS_STATE_CD))
	IFF(
	    LKP_SUP_STATE_in_CLM_LOSS_STATE_CD.state_code IS NULL, 'N/A',
	    LKP_SUP_STATE_in_CLM_LOSS_STATE_CD.state_code
	) AS CLM_LOSS_STATE_CD,
	-- *INF*: TO_DATE('1/1/1800','MM/DD/YYYY')
	-- 
	TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY') AS CLM_REI_NOTIFY_DT,
	'N/A' AS CLM_METHOD_RPTD,
	'N/A' AS CLM_HOW_CLM_RPTD,
	'N/A' AS CLM_VIOL_CIT_DESC,
	EXP_Value.in_IPFCGP_LOSS_OCCURENCE AS in_CON_OCCURRENCE_NBR,
	-- *INF*: IIF(ISNULL(in_CON_OCCURRENCE_NBR) OR IS_SPACES(in_CON_OCCURRENCE_NBR),'N/A',
	--    LPAD(rtrim( in_CON_OCCURRENCE_NBR),3,'0'))
	-- 
	-- -- Changed the logic to LPAD with '0' on 8/4/2010
	IFF(
	    in_CON_OCCURRENCE_NBR IS NULL
	    or LENGTH(in_CON_OCCURRENCE_NBR)>0
	    and TRIM(in_CON_OCCURRENCE_NBR)='',
	    'N/A',
	    LPAD(rtrim(in_CON_OCCURRENCE_NBR), 3, '0')
	) AS CON_OCCURRENCE_NBR,
	EXP_Value.CLM_CATASTROPHE_CODE AS in_CLM_CATASTROPHE_CODE,
	-- *INF*: iif(isnull(in_CLM_CATASTROPHE_CODE),'N/A',
	--    iif(is_spaces(in_CLM_CATASTROPHE_CODE),'N/A',
	--     rtrim(in_CLM_CATASTROPHE_CODE)))
	IFF(
	    in_CLM_CATASTROPHE_CODE IS NULL, 'N/A',
	    IFF(
	        LENGTH(in_CLM_CATASTROPHE_CODE)>0
	    and TRIM(in_CLM_CATASTROPHE_CODE)='', 'N/A',
	        rtrim(in_CLM_CATASTROPHE_CODE)
	    )
	) AS v_CLM_CATASTROPHE_CODE,
	v_CLM_CATASTROPHE_CODE AS CLM_CATASTROPHE_CODE,
	'N/A' AS TCC_COMMENT_TXT,
	LKP_Sup_Claim_Catastrophe_Code.cat_start_date AS in_COC_START_DT,
	-- *INF*: IIF(v_CLM_CATASTROPHE_CODE = 'N/A', TO_DATE('1/1/1800', 'MM/DD/YYYY'), 
	-- IIF(ISNULL(in_COC_START_DT),TO_DATE('1/1/1800','MM/DD/YYYY'),
	-- in_COC_START_DT))
	IFF(
	    v_CLM_CATASTROPHE_CODE = 'N/A', TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'),
	    IFF(
	        in_COC_START_DT IS NULL, TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'), in_COC_START_DT
	    )
	) AS COC_START_DT,
	LKP_Sup_Claim_Catastrophe_Code.cat_end_date AS in_COC_END_DT,
	-- *INF*: IIF(v_CLM_CATASTROPHE_CODE = 'N/A', TO_DATE('12/31/2100', 'MM/DD/YYYY'), 
	-- IIF(ISNULL(in_COC_END_DT),TO_DATE('12/31/2100','MM/DD/YYYY'),
	-- in_COC_END_DT))
	IFF(
	    v_CLM_CATASTROPHE_CODE = 'N/A', TO_TIMESTAMP('12/31/2100', 'MM/DD/YYYY'),
	    IFF(
	        in_COC_END_DT IS NULL, TO_TIMESTAMP('12/31/2100', 'MM/DD/YYYY'), in_COC_END_DT
	    )
	) AS COC_END_DT,
	LKP_V2_Policy.pol_key_ak_id AS CON_pol_key_ak_id,
	-- *INF*: IIF(ISNULL(CON_pol_key_ak_id) , 
	-- -1, 
	-- CON_pol_key_ak_id)
	IFF(CON_pol_key_ak_id IS NULL, - 1, CON_pol_key_ak_id) AS out_CON_pol_key_ak_id,
	'N/A' AS CLM_STATUS_CD,
	-- *INF*: TO_DATE('1/1/1800','MM/DD/YYYY')
	TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY') AS CLM_CREATE_TS,
	EXP_Value.ipfcgp_offset_onset_ind AS in_ipfcgp_offset_onset_ind,
	-- *INF*: IIF(
	-- (ISNULL(in_ipfcgp_offset_onset_ind) OR IS_SPACES(in_ipfcgp_offset_onset_ind)),
	-- 'N/A',
	-- in_ipfcgp_offset_onset_ind)
	IFF(
	    (in_ipfcgp_offset_onset_ind IS NULL
	    or LENGTH(in_ipfcgp_offset_onset_ind)>0
	    and TRIM(in_ipfcgp_offset_onset_ind)=''),
	    'N/A',
	    in_ipfcgp_offset_onset_ind
	) AS OFFSET_ONSET_INDICATOR,
	LKP_Pif_42gm_Stage.ipfc4m_rsk_rmk_verbg AS in_ipfc4m_rsk_rmk_verbg,
	EXP_Value.ipfcgp_accident_description AS in_ipfcgp_accident_description,
	-- *INF*: IIF ((ISNULL(in_ipfc4m_rsk_rmk_verbg) OR IS_SPACES (in_ipfc4m_rsk_rmk_verbg)),
	-- IIF ((ISNULL(in_ipfcgp_accident_description) OR IS_SPACES (in_ipfcgp_accident_description)),'N/A',in_ipfcgp_accident_description)
	-- ,in_ipfc4m_rsk_rmk_verbg)
	-- 
	IFF(
	    (in_ipfc4m_rsk_rmk_verbg IS NULL
	    or LENGTH(in_ipfc4m_rsk_rmk_verbg)>0
	    and TRIM(in_ipfc4m_rsk_rmk_verbg)=''),
	    IFF(
	        (in_ipfcgp_accident_description IS NULL
	        or LENGTH(in_ipfcgp_accident_description)>0
	        and TRIM(in_ipfcgp_accident_description)=''),
	        'N/A',
	        in_ipfcgp_accident_description
	    ),
	    in_ipfc4m_rsk_rmk_verbg
	) AS LOSS_DESCRIPTION,
	LKP_Pif_4578_Stage.loss_fault_code AS in_loss_fault_code,
	-- *INF*: IIF((ISNULL(in_loss_fault_code) OR IS_SPACES(in_loss_fault_code)),
	-- 'N/A',
	-- in_loss_fault_code)
	IFF(
	    (in_loss_fault_code IS NULL OR LENGTH(in_loss_fault_code)>0 AND TRIM(in_loss_fault_code)=''),
	    'N/A',
	    in_loss_fault_code
	) AS CLM_AT_FAULT_CD,
	-- *INF*: RTRIM(LTRIM(IIF((ISNULL(in_loss_fault_code) OR IS_SPACES(in_loss_fault_code)),
	-- 'N/A',
	-- in_loss_fault_code)))
	RTRIM(LTRIM(
	        IFF(
	            (in_loss_fault_code IS NULL
	            or LENGTH(in_loss_fault_code)>0
	            and TRIM(in_loss_fault_code)=''),
	            'N/A',
	            in_loss_fault_code
	        ))) AS o_CLM_AT_FAULT_CD,
	LKP_Pif_42gq_Aut_Stage.ipfcgq_driver_number AS in_CLM_DRIVER_NBR,
	-- *INF*: IIF(
	-- ISNULL(in_CLM_DRIVER_NBR)  ,
	-- -1,in_CLM_DRIVER_NBR)
	-- 
	IFF(in_CLM_DRIVER_NBR IS NULL, - 1, in_CLM_DRIVER_NBR) AS CLM_DRIVER_NBR,
	-- *INF*: IIF(
	-- ISNULL(in_CLM_DRIVER_NBR)  ,
	-- 0,1)
	IFF(in_CLM_DRIVER_NBR IS NULL, 0, 1) AS CLM_DRV_SAME_IND,
	LKP_Log_Note_Stage.create_date AS in_LOG_NOTE_LAST_ACTIVITY_DATE,
	-- *INF*: iif(isnull(in_LOG_NOTE_LAST_ACTIVITY_DATE), TO_DATE('1/1/1800','MM/DD/YYYY'),
	--    in_LOG_NOTE_LAST_ACTIVITY_DATE)
	IFF(
	    in_LOG_NOTE_LAST_ACTIVITY_DATE IS NULL, TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'),
	    in_LOG_NOTE_LAST_ACTIVITY_DATE
	) AS LOG_NOTE_LAST_ACTIVITY_DATE,
	mplt_claim_occurrence_next_diary.DueDate AS in_CLAIM_NEXT_DIARY_DATE,
	-- *INF*: iif(isnull(in_CLAIM_NEXT_DIARY_DATE), TO_DATE('1/1/1800','MM/DD/YYYY'),
	--   in_CLAIM_NEXT_DIARY_DATE)
	IFF(
	    in_CLAIM_NEXT_DIARY_DATE IS NULL, TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'),
	    in_CLAIM_NEXT_DIARY_DATE
	) AS CLAIM_NEXT_DIARY_DATE,
	'N/A' AS CLM_NOT_CLAIM_IND,
	EXP_Value.logical_flag,
	EXP_Value.CON_POLICY_KEY,
	EXP_Value.ipfcgp_claim_status AS in_ipfcgp_claim_status,
	-- *INF*: IIF(ISNULL(in_ipfcgp_claim_status),'N/A',in_ipfcgp_claim_status)
	IFF(in_ipfcgp_claim_status IS NULL, 'N/A', in_ipfcgp_claim_status) AS v_ipfcgp_claim_status,
	-- *INF*: DECODE(v_ipfcgp_claim_status,
	-- 'N/A' , 'N/A',
	-- 'O' , 'OPE',
	-- 'C' , 'CWP',
	-- 'Z' , 'OFF',
	-- 'N' , 'NOT',
	-- 'CNP')
	DECODE(
	    v_ipfcgp_claim_status,
	    'N/A', 'N/A',
	    'O', 'OPE',
	    'C', 'CWP',
	    'Z', 'OFF',
	    'N', 'NOT',
	    'CNP'
	) AS ipfcgp_claim_status,
	-- *INF*: DECODE(in_ipfcgp_claim_status,
	-- 'N' , 'Y',
	-- 'N/A')
	DECODE(
	    in_ipfcgp_claim_status,
	    'N', 'Y',
	    'N/A'
	) AS ipfcgp_notice_claim_ind,
	LKP_PIF_42X6_Stage.min_reported_date AS in_reported_date,
	-- *INF*: IIF(ISNULL(in_reported_date), TO_DATE ('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'), in_reported_date)
	IFF(
	    in_reported_date IS NULL, TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    in_reported_date
	) AS reported_date,
	mplt_claim_occurrence_next_diary.AssignedUserName AS claim_rep_full_name,
	-- *INF*: IIF(ISNULL(claim_rep_full_name),'N/A',claim_rep_full_name)
	IFF(claim_rep_full_name IS NULL, 'N/A', claim_rep_full_name) AS claim_rep_full_name_out,
	LKP_Sup_State_sup_state_id.sup_state_id AS in_sup_state_id,
	-- *INF*: IIF(ISNULL(in_sup_state_id), -1,in_sup_state_id)
	IFF(in_sup_state_id IS NULL, - 1, in_sup_state_id) AS SupStateID
	FROM EXP_Value
	 -- Manually join with mplt_claim_occurrence_next_diary
	LEFT JOIN LKP_Log_Note_Stage
	ON LKP_Log_Note_Stage.claim_id = LKP_Claim_Stage.claim_id
	LEFT JOIN LKP_PIF_42X6_Stage
	ON LKP_PIF_42X6_Stage.pif_symbol = EXP_Value.PIF_SYMBOL AND LKP_PIF_42X6_Stage.pif_policy_number = EXP_Value.PIF_POLICY_NUMBER AND LKP_PIF_42X6_Stage.pif_module = EXP_Value.PIF_MODULE AND LKP_PIF_42X6_Stage.ipfcx6_year_of_loss = EXP_Value.IPFCGP_YEAR_OF_LOSS AND LKP_PIF_42X6_Stage.ipfcx6_month_of_loss = EXP_Value.IPFCGP_MONTH_OF_LOSS AND LKP_PIF_42X6_Stage.ipfcx6_day_of_loss = EXP_Value.IPFCGP_DAY_OF_LOSS AND LKP_PIF_42X6_Stage.ipfcx6_usr_loss_occurence = EXP_Value.in_IPFCGP_LOSS_OCCURENCE
	LEFT JOIN LKP_Pif_42gm_Stage
	ON LKP_Pif_42gm_Stage.pif_symbol = EXP_Value.PIF_SYMBOL AND LKP_Pif_42gm_Stage.pif_policy_number = EXP_Value.PIF_POLICY_NUMBER AND LKP_Pif_42gm_Stage.pif_module = EXP_Value.PIF_MODULE AND LKP_Pif_42gm_Stage.ipfc4m_loss_year = EXP_Value.IPFCGP_YEAR_OF_LOSS AND LKP_Pif_42gm_Stage.ipfc4m_loss_month = EXP_Value.IPFCGP_MONTH_OF_LOSS AND LKP_Pif_42gm_Stage.ipfc4m_loss_day = EXP_Value.IPFCGP_DAY_OF_LOSS AND LKP_Pif_42gm_Stage.ipfc4m_loss_occurence = EXP_Value.in_IPFCGP_LOSS_OCCURENCE
	LEFT JOIN LKP_Pif_42gq_Aut_Stage
	ON LKP_Pif_42gq_Aut_Stage.pif_symbol = EXP_Value.PIF_SYMBOL AND LKP_Pif_42gq_Aut_Stage.pif_policy_number = EXP_Value.PIF_POLICY_NUMBER AND LKP_Pif_42gq_Aut_Stage.pif_module = EXP_Value.PIF_MODULE AND LKP_Pif_42gq_Aut_Stage.ipfcgq_year_of_loss = EXP_Value.IPFCGP_YEAR_OF_LOSS AND LKP_Pif_42gq_Aut_Stage.ipfcgq_month_of_loss = EXP_Value.IPFCGP_MONTH_OF_LOSS AND LKP_Pif_42gq_Aut_Stage.ipfcgq_day_of_loss = EXP_Value.IPFCGP_DAY_OF_LOSS AND LKP_Pif_42gq_Aut_Stage.ipfcgq_loss_occurence = EXP_Value.in_IPFCGP_LOSS_OCCURENCE
	LEFT JOIN LKP_Pif_4578_Stage
	ON LKP_Pif_4578_Stage.pif_symbol = EXP_Value.PIF_SYMBOL AND LKP_Pif_4578_Stage.pif_policy_number = EXP_Value.PIF_POLICY_NUMBER AND LKP_Pif_4578_Stage.pif_module = EXP_Value.PIF_MODULE AND LKP_Pif_4578_Stage.loss_year = EXP_Value.IPFCGP_YEAR_OF_LOSS AND LKP_Pif_4578_Stage.loss_month = EXP_Value.IPFCGP_MONTH_OF_LOSS AND LKP_Pif_4578_Stage.loss_day = EXP_Value.IPFCGP_DAY_OF_LOSS AND LKP_Pif_4578_Stage.loss_occurence = EXP_Value.in_IPFCGP_LOSS_OCCURENCE
	LEFT JOIN LKP_Sup_Claim_Catastrophe_Code
	ON LKP_Sup_Claim_Catastrophe_Code.cat_code = EXP_Value.CLM_CATASTROPHE_CODE
	LEFT JOIN LKP_Sup_State_sup_state_id
	ON LKP_Sup_State_sup_state_id.state_abbrev = EXP_Value.CLM_LOSS_STATE_CD
	LEFT JOIN LKP_V2_Policy
	ON LKP_V2_Policy.pol_key = EXP_Value.CON_POLICY_KEY
	LEFT JOIN LKP_SUP_STATE LKP_SUP_STATE_in_CLM_LOSS_STATE_CD
	ON LKP_SUP_STATE_in_CLM_LOSS_STATE_CD.state_abbrev = in_CLM_LOSS_STATE_CD

),
LKP_Claim_Occurrence AS (
	SELECT
	claim_occurrence_id,
	claim_occurrence_ak_id,
	pol_key_ak_id,
	claim_occurrence_type_code,
	source_claim_occurrence_status_code,
	notice_claim_ind,
	s3p_claim_created_date,
	source_claim_rpted_date,
	rpt_method,
	how_claim_rpted,
	loss_loc_addr,
	loss_loc_city,
	loss_loc_county,
	loss_loc_state,
	loss_loc_zip,
	claim_loss_date,
	claim_discovery_date,
	claim_cat_code,
	claim_cat_start_date,
	claim_cat_end_date,
	s3p_claim_num,
	reins_notified_date,
	claim_occurrence_num,
	claim_voilation_citation_descript,
	claim_loss_descript,
	claim_insd_at_fault_code,
	claim_insd_driver_num,
	claim_insd_driver_ind,
	claim_log_note_last_act_date,
	next_diary_date,
	next_diary_date_rep,
	offset_onset_ind,
	claim_occurrence_key
	FROM (
		SELECT 
		a.claim_occurrence_id as claim_occurrence_id, 
		a.claim_occurrence_ak_id as claim_occurrence_ak_id, 
		a.pol_key_ak_id as pol_key_ak_id, 
		a.claim_occurrence_type_code as claim_occurrence_type_code, 
		a.source_claim_occurrence_status_code as source_claim_occurrence_status_code, 
		a.notice_claim_ind as notice_claim_ind,
		a.s3p_claim_created_date as s3p_claim_created_date, 
		a.source_claim_rpted_date as source_claim_rpted_date,
		a.rpt_method as rpt_method, 
		a.how_claim_rpted as how_claim_rpted, 
		a.loss_loc_addr as loss_loc_addr, 
		a.loss_loc_city as loss_loc_city, 
		a.loss_loc_county as loss_loc_county, 
		a.loss_loc_state as loss_loc_state, 
		a.loss_loc_zip as loss_loc_zip, 
		a.claim_loss_date as claim_loss_date, 
		a.claim_discovery_date as claim_discovery_date, 
		a.claim_cat_code as claim_cat_code, 
		a.claim_cat_start_date as claim_cat_start_date,
		a.claim_cat_end_date as claim_cat_end_date, 
		a.s3p_claim_num as s3p_claim_num, 
		a.reins_notified_date as reins_notified_date, 
		a.claim_occurrence_num as claim_occurrence_num, 
		a.claim_voilation_citation_descript as claim_voilation_citation_descript, 
		a.claim_loss_descript as claim_loss_descript, 
		a.claim_insd_at_fault_code as claim_insd_at_fault_code, 
		a.claim_insd_driver_num as claim_insd_driver_num,
		a.claim_insd_driver_ind as claim_insd_driver_ind, 
		a.claim_log_note_last_act_date as claim_log_note_last_act_date, 
		a.next_diary_date as next_diary_date, 
		a.next_diary_date_rep as next_diary_date_rep, 
		a.offset_onset_ind as offset_onset_ind, 
		a.claim_occurrence_key as claim_occurrence_key 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence a
		WHERE a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND a.crrnt_snpsht_flag = 1
		ORDER BY claim_occurrence_key --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_key ORDER BY claim_occurrence_id) = 1
),
LKP_Sup_Claim_Insured_At_Fault_Code AS (
	SELECT
	sup_claim_insd_at_fault_code_id,
	claim_insd_at_fault_code
	FROM (
		SELECT 
			sup_claim_insd_at_fault_code_id,
			claim_insd_at_fault_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_insured_at_fault_code
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_insd_at_fault_code ORDER BY sup_claim_insd_at_fault_code_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	EXP_Lkp_Values.CLM_CLAIM_NBR,
	EXP_Lkp_Values.CLM_CSR_CLAIM_NBR,
	EXP_Lkp_Values.CLM_POSTAL_CD,
	EXP_Lkp_Values.CLM_DISCOVERY_DT,
	EXP_Lkp_Values.CLM_LOSS_DT,
	EXP_Lkp_Values.CLM_LOSS_CITY,
	EXP_Lkp_Values.CLM_LOSS_COUNTY,
	EXP_Lkp_Values.CLM_LOSS_STATE_CD,
	EXP_Lkp_Values.CLM_TYPE_CD,
	EXP_Lkp_Values.CLM_REI_NOTIFY_DT,
	EXP_Lkp_Values.CLM_METHOD_RPTD,
	EXP_Lkp_Values.CLM_HOW_CLM_RPTD,
	EXP_Lkp_Values.CLM_VIOL_CIT_DESC,
	EXP_Lkp_Values.CON_OCCURRENCE_NBR,
	EXP_Lkp_Values.CLM_CATASTROPHE_CODE,
	EXP_Lkp_Values.TCC_COMMENT_TXT,
	EXP_Lkp_Values.COC_START_DT,
	EXP_Lkp_Values.COC_END_DT,
	EXP_Lkp_Values.out_CON_pol_key_ak_id AS CON_pol_key_ak_id,
	EXP_Lkp_Values.CLM_STATUS_CD,
	EXP_Lkp_Values.CLM_CREATE_TS,
	EXP_Lkp_Values.LOSS_DESCRIPTION,
	EXP_Lkp_Values.CLM_AT_FAULT_CD,
	EXP_Lkp_Values.CLM_DRIVER_NBR,
	EXP_Lkp_Values.CLM_DRV_SAME_IND,
	EXP_Lkp_Values.OFFSET_ONSET_INDICATOR,
	EXP_Lkp_Values.LOG_NOTE_LAST_ACTIVITY_DATE,
	EXP_Lkp_Values.CLAIM_NEXT_DIARY_DATE,
	EXP_Lkp_Values.CLM_NOT_CLAIM_IND,
	EXP_Lkp_Values.reported_date,
	LKP_Claim_Occurrence.claim_occurrence_id,
	LKP_Claim_Occurrence.claim_occurrence_ak_id AS LKP_claim_occurrence_ak_id,
	LKP_Claim_Occurrence.pol_key_ak_id,
	LKP_Claim_Occurrence.claim_occurrence_type_code,
	LKP_Claim_Occurrence.source_claim_occurrence_status_code AS claim_occurrence_status_code,
	LKP_Claim_Occurrence.s3p_claim_created_date,
	LKP_Claim_Occurrence.rpt_method,
	LKP_Claim_Occurrence.how_claim_rpted,
	LKP_Claim_Occurrence.loss_loc_addr,
	LKP_Claim_Occurrence.loss_loc_city,
	LKP_Claim_Occurrence.loss_loc_county,
	LKP_Claim_Occurrence.loss_loc_state,
	LKP_Claim_Occurrence.loss_loc_zip,
	LKP_Claim_Occurrence.claim_loss_date,
	LKP_Claim_Occurrence.claim_discovery_date,
	LKP_Claim_Occurrence.claim_cat_code,
	LKP_Claim_Occurrence.claim_cat_start_date,
	LKP_Claim_Occurrence.claim_cat_end_date,
	LKP_Claim_Occurrence.s3p_claim_num,
	LKP_Claim_Occurrence.reins_notified_date,
	LKP_Claim_Occurrence.claim_occurrence_num,
	LKP_Claim_Occurrence.claim_voilation_citation_descript,
	LKP_Claim_Occurrence.claim_loss_descript,
	LKP_Claim_Occurrence.claim_insd_at_fault_code,
	LKP_Claim_Occurrence.claim_insd_driver_ind AS claim_insd_drvr_ind,
	LKP_Claim_Occurrence.claim_log_note_last_act_date,
	LKP_Claim_Occurrence.next_diary_date,
	LKP_Claim_Occurrence.next_diary_date_rep,
	LKP_Claim_Occurrence.offset_onset_ind,
	LKP_Claim_Occurrence.claim_insd_driver_num AS claim_insd_drvr_num,
	LKP_Claim_Occurrence.notice_claim_ind,
	1 AS Crrnt_Snpsht_Flag,
	-- *INF*: iif(isnull(claim_occurrence_id),'NEW',
	-- 	iif (
	-- 	(ltrim(rtrim(CLM_CSR_CLAIM_NBR)) <> ltrim(rtrim(s3p_claim_num))) or
	-- 	(ltrim(rtrim(CLM_POSTAL_CD)) <> ltrim(rtrim(loss_loc_zip))) or
	-- 	(CLM_DISCOVERY_DT <> claim_discovery_date ) or
	-- 	(CLM_LOSS_DT  <> claim_loss_date ) or
	-- 	(ltrim(rtrim(CLM_LOSS_CITY)) <> ltrim(rtrim(loss_loc_city))) or
	-- 	(ltrim(rtrim(CLM_LOSS_COUNTY)) <> ltrim(rtrim(loss_loc_county) )) or
	-- 	(ltrim(rtrim(CLM_LOSS_STATE_CD)) <>  ltrim(rtrim(loss_loc_state))) or
	-- 	(ltrim(rtrim(CLM_TYPE_CD)) <> ltrim(rtrim(claim_occurrence_type_code))) or
	-- 	(CLM_REI_NOTIFY_DT <> reins_notified_date) or
	-- 	(ltrim(rtrim(CLM_METHOD_RPTD)) <> ltrim(rtrim(rpt_method))) or
	-- 	(ltrim(rtrim(CLM_HOW_CLM_RPTD)) <> ltrim(rtrim(how_claim_rpted))) or
	-- 	(ltrim(rtrim(CLM_VIOL_CIT_DESC)) <> ltrim(rtrim(claim_voilation_citation_descript))) or
	-- 	(ltrim(rtrim(CON_OCCURRENCE_NBR)) <> ltrim(rtrim(claim_occurrence_num))) or
	-- 	(ltrim(rtrim(CLM_CATASTROPHE_CODE)) <> ltrim(rtrim(claim_cat_code))) or
	-- 	(ltrim(rtrim(TCC_COMMENT_TXT)) <>  ltrim(rtrim(loss_loc_addr))) or
	-- 	(COC_START_DT <> claim_cat_start_date) or
	-- 	(COC_END_DT <> claim_cat_end_date) or
	-- 	( CLM_CREATE_TS <> s3p_claim_created_date) or
	-- 	(ltrim(rtrim( LOSS_DESCRIPTION)) <> ltrim(rtrim(claim_loss_descript))) or
	-- 	(ltrim(rtrim(CLM_AT_FAULT_CD))  <> ltrim(rtrim(claim_insd_at_fault_code))) or
	-- 	( CLM_DRIVER_NBR <> claim_insd_drvr_num) or
	-- 	(ltrim(rtrim( CLM_DRV_SAME_IND)) <> ltrim(rtrim(claim_insd_drvr_ind))) or
	-- 	(LOG_NOTE_LAST_ACTIVITY_DATE  <> claim_log_note_last_act_date) or
	-- 	(claim_rep_full_name_out  <> next_diary_date_rep) or
	-- 	( CLAIM_NEXT_DIARY_DATE <> next_diary_date) or
	-- 	(ltrim(rtrim(OFFSET_ONSET_INDICATOR))  <> ltrim(rtrim(offset_onset_ind))) or
	-- 	(CON_pol_key_ak_id <>  pol_key_ak_id)  or 
	--      (ltrim(rtrim(ipfcgp_claim_status))  <> ltrim(rtrim(claim_occurrence_status_code))) or
	-- 	(ltrim(rtrim(ipfcgp_notice_claim_ind))  <> ltrim(rtrim(notice_claim_ind))) or   
	--        reported_date <> source_claim_rpted_date  ,
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(
	    claim_occurrence_id IS NULL, 'NEW',
	    IFF(
	        (ltrim(rtrim(CLM_CSR_CLAIM_NBR)) <> ltrim(rtrim(s3p_claim_num)))
	        or (ltrim(rtrim(CLM_POSTAL_CD)) <> ltrim(rtrim(loss_loc_zip)))
	        or (CLM_DISCOVERY_DT <> claim_discovery_date)
	        or (CLM_LOSS_DT <> claim_loss_date)
	        or (ltrim(rtrim(CLM_LOSS_CITY)) <> ltrim(rtrim(loss_loc_city)))
	        or (ltrim(rtrim(CLM_LOSS_COUNTY)) <> ltrim(rtrim(loss_loc_county)))
	        or (ltrim(rtrim(CLM_LOSS_STATE_CD)) <> ltrim(rtrim(loss_loc_state)))
	        or (ltrim(rtrim(CLM_TYPE_CD)) <> ltrim(rtrim(claim_occurrence_type_code)))
	        or (CLM_REI_NOTIFY_DT <> reins_notified_date)
	        or (ltrim(rtrim(CLM_METHOD_RPTD)) <> ltrim(rtrim(rpt_method)))
	        or (ltrim(rtrim(CLM_HOW_CLM_RPTD)) <> ltrim(rtrim(how_claim_rpted)))
	        or (ltrim(rtrim(CLM_VIOL_CIT_DESC)) <> ltrim(rtrim(claim_voilation_citation_descript)))
	        or (ltrim(rtrim(CON_OCCURRENCE_NBR)) <> ltrim(rtrim(claim_occurrence_num)))
	        or (ltrim(rtrim(CLM_CATASTROPHE_CODE)) <> ltrim(rtrim(claim_cat_code)))
	        or (ltrim(rtrim(TCC_COMMENT_TXT)) <> ltrim(rtrim(loss_loc_addr)))
	        or (COC_START_DT <> claim_cat_start_date)
	        or (COC_END_DT <> claim_cat_end_date)
	        or (CLM_CREATE_TS <> s3p_claim_created_date)
	        or (ltrim(rtrim(LOSS_DESCRIPTION)) <> ltrim(rtrim(claim_loss_descript)))
	        or (ltrim(rtrim(CLM_AT_FAULT_CD)) <> ltrim(rtrim(claim_insd_at_fault_code)))
	        or (CLM_DRIVER_NBR <> claim_insd_drvr_num)
	        or (ltrim(rtrim(CLM_DRV_SAME_IND)) <> ltrim(rtrim(claim_insd_drvr_ind)))
	        or (LOG_NOTE_LAST_ACTIVITY_DATE <> claim_log_note_last_act_date)
	        or (claim_rep_full_name_out <> next_diary_date_rep)
	        or (CLAIM_NEXT_DIARY_DATE <> next_diary_date)
	        or (ltrim(rtrim(OFFSET_ONSET_INDICATOR)) <> ltrim(rtrim(offset_onset_ind)))
	        or (CON_pol_key_ak_id <> pol_key_ak_id)
	        or (ltrim(rtrim(ipfcgp_claim_status)) <> ltrim(rtrim(claim_occurrence_status_code)))
	        or (ltrim(rtrim(ipfcgp_notice_claim_ind)) <> ltrim(rtrim(notice_claim_ind)))
	        or reported_date <> source_claim_rpted_date,
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
	SYSDATE AS Modified_Date,
	EXP_Lkp_Values.logical_flag,
	EXP_Lkp_Values.CON_POLICY_KEY,
	0 AS err_flag,
	EXP_Lkp_Values.ipfcgp_claim_status,
	EXP_Lkp_Values.ipfcgp_notice_claim_ind,
	LKP_Claim_Occurrence.source_claim_rpted_date,
	'N/A' AS Default_NA,
	EXP_Lkp_Values.claim_rep_full_name_out,
	EXP_Lkp_Values.SupStateID,
	LKP_Sup_Claim_Insured_At_Fault_Code.sup_claim_insd_at_fault_code_id AS in_sup_claim_insd_at_fault_code_id,
	-- *INF*: IIF(ISNULL(in_sup_claim_insd_at_fault_code_id),-1, in_sup_claim_insd_at_fault_code_id)
	IFF(in_sup_claim_insd_at_fault_code_id IS NULL, - 1, in_sup_claim_insd_at_fault_code_id) AS sup_claim_insd_at_fault_code_id
	FROM EXP_Lkp_Values
	LEFT JOIN LKP_Claim_Occurrence
	ON LKP_Claim_Occurrence.claim_occurrence_key = EXP_Lkp_Values.CLM_CLAIM_NBR
	LEFT JOIN LKP_Sup_Claim_Insured_At_Fault_Code
	ON LKP_Sup_Claim_Insured_At_Fault_Code.claim_insd_at_fault_code = EXP_Lkp_Values.o_CLM_AT_FAULT_CD
),
FIL_Insert AS (
	SELECT
	LKP_claim_occurrence_ak_id, 
	CLM_CLAIM_NBR, 
	CLM_CSR_CLAIM_NBR, 
	CLM_POSTAL_CD, 
	CLM_DISCOVERY_DT, 
	CLM_LOSS_DT, 
	CLM_LOSS_CITY, 
	CLM_LOSS_COUNTY, 
	CLM_LOSS_STATE_CD, 
	CLM_TYPE_CD, 
	CLM_REI_NOTIFY_DT, 
	CLM_METHOD_RPTD, 
	CLM_HOW_CLM_RPTD, 
	CLM_VIOL_CIT_DESC, 
	CON_OCCURRENCE_NBR, 
	CLM_CATASTROPHE_CODE, 
	TCC_COMMENT_TXT, 
	COC_START_DT, 
	COC_END_DT, 
	CON_pol_key_ak_id, 
	Crrnt_Snpsht_Flag, 
	SOURCE_SYSTEM_ID, 
	Audit_Id, 
	Eff_From_Date, 
	Eff_To_Date, 
	Created_Date, 
	Modified_Date, 
	Changed_Flag, 
	CLM_STATUS_CD, 
	CLM_CREATE_TS, 
	LOSS_DESCRIPTION, 
	CLM_AT_FAULT_CD, 
	CLM_DRIVER_NBR, 
	CLM_DRV_SAME_IND, 
	OFFSET_ONSET_INDICATOR, 
	LOG_NOTE_LAST_ACTIVITY_DATE, 
	CLAIM_NEXT_DIARY_DATE, 
	claim_rep_full_name_out, 
	CLM_NOT_CLAIM_IND, 
	logical_flag, 
	CON_POLICY_KEY, 
	err_flag, 
	ipfcgp_claim_status, 
	ipfcgp_notice_claim_ind, 
	reported_date, 
	Default_NA, 
	SupStateID, 
	sup_claim_insd_at_fault_code_id
	FROM EXP_Detect_Changes
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
SEQ_claim_occurrence AS (
	CREATE SEQUENCE SEQ_claim_occurrence
	START = 0
	INCREMENT = 1;
),
EXP_Determine_AK AS (
	SELECT
	LKP_claim_occurrence_ak_id,
	-- *INF*: IIF(Changed_Flag='NEW',
	-- NEXTVAL,
	-- LKP_claim_occurrence_ak_id)
	IFF(Changed_Flag = 'NEW', NEXTVAL, LKP_claim_occurrence_ak_id) AS Out_claim_occurrence_ak_id,
	CLM_CLAIM_NBR,
	CLM_CSR_CLAIM_NBR,
	CLM_POSTAL_CD,
	CLM_DISCOVERY_DT,
	CLM_LOSS_DT,
	CLM_LOSS_CITY,
	CLM_LOSS_COUNTY,
	CLM_LOSS_STATE_CD,
	CLM_TYPE_CD,
	CLM_REI_NOTIFY_DT,
	CLM_METHOD_RPTD,
	CLM_HOW_CLM_RPTD,
	CLM_VIOL_CIT_DESC,
	SOURCE_SYSTEM_ID,
	CON_OCCURRENCE_NBR,
	CLM_CATASTROPHE_CODE,
	TCC_COMMENT_TXT,
	COC_START_DT,
	COC_END_DT,
	CON_pol_key_ak_id,
	Crrnt_Snpsht_Flag,
	Audit_Id,
	Eff_From_Date,
	Eff_To_Date,
	Created_Date,
	Modified_Date,
	Changed_Flag,
	CLM_STATUS_CD,
	CLM_CREATE_TS,
	LOSS_DESCRIPTION,
	CLM_AT_FAULT_CD,
	CLM_DRIVER_NBR,
	CLM_DRV_SAME_IND,
	OFFSET_ONSET_INDICATOR,
	LOG_NOTE_LAST_ACTIVITY_DATE,
	CLAIM_NEXT_DIARY_DATE,
	claim_rep_full_name_out,
	CLM_NOT_CLAIM_IND,
	logical_flag,
	CON_POLICY_KEY,
	err_flag,
	ipfcgp_claim_status,
	ipfcgp_notice_claim_ind,
	reported_date,
	Default_NA,
	SEQ_claim_occurrence.NEXTVAL,
	SupStateID,
	sup_claim_insd_at_fault_code_id,
	'N/A' AS ClaimRelationshipKey
	FROM FIL_Insert
),
Claim_Occurrence_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence
	(claim_occurrence_ak_id, pol_key_ak_id, pol_key, claim_occurrence_key, claim_occurrence_type_code, source_claim_occurrence_status_code, notice_claim_ind, s3p_claim_created_date, source_claim_rpted_date, s3p_claim_updated_date, rpt_method, how_claim_rpted, loss_loc_addr, loss_loc_city, loss_loc_county, loss_loc_state, loss_loc_zip, claim_loss_date, claim_discovery_date, claim_cat_code, claim_cat_start_date, claim_cat_end_date, s3p_claim_num, reins_notified_date, claim_occurrence_num, claim_voilation_citation_descript, claim_loss_descript, claim_insd_at_fault_code, claim_insd_driver_num, claim_insd_driver_ind, claim_log_note_last_act_date, err_flag_bal_txn, next_diary_date, next_diary_date_rep, offset_onset_ind, err_flag_bal_reins, logical_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, claim_created_by_key, wc_cat_code, SupStateId, SupClaimInsuredAtFaultCodeId, PrimaryWorkGroup, SecondaryWorkGroup, ClaimRelationshipKey)
	SELECT 
	EXP_Determine_AK.Out_claim_occurrence_ak_id AS CLAIM_OCCURRENCE_AK_ID, 
	EXP_Determine_AK.CON_pol_key_ak_id AS POL_KEY_AK_ID, 
	EXP_Determine_AK.CON_POLICY_KEY AS POL_KEY, 
	EXP_Determine_AK.CLM_CLAIM_NBR AS CLAIM_OCCURRENCE_KEY, 
	EXP_Determine_AK.CLM_TYPE_CD AS CLAIM_OCCURRENCE_TYPE_CODE, 
	EXP_Determine_AK.ipfcgp_claim_status AS SOURCE_CLAIM_OCCURRENCE_STATUS_CODE, 
	EXP_Determine_AK.ipfcgp_notice_claim_ind AS NOTICE_CLAIM_IND, 
	EXP_Determine_AK.CLM_CREATE_TS AS S3P_CLAIM_CREATED_DATE, 
	EXP_Determine_AK.reported_date AS SOURCE_CLAIM_RPTED_DATE, 
	EXP_Determine_AK.CLM_CREATE_TS AS S3P_CLAIM_UPDATED_DATE, 
	EXP_Determine_AK.CLM_METHOD_RPTD AS RPT_METHOD, 
	EXP_Determine_AK.CLM_HOW_CLM_RPTD AS HOW_CLAIM_RPTED, 
	EXP_Determine_AK.TCC_COMMENT_TXT AS LOSS_LOC_ADDR, 
	EXP_Determine_AK.CLM_LOSS_CITY AS LOSS_LOC_CITY, 
	EXP_Determine_AK.CLM_LOSS_COUNTY AS LOSS_LOC_COUNTY, 
	EXP_Determine_AK.CLM_LOSS_STATE_CD AS LOSS_LOC_STATE, 
	EXP_Determine_AK.CLM_POSTAL_CD AS LOSS_LOC_ZIP, 
	EXP_Determine_AK.CLM_LOSS_DT AS CLAIM_LOSS_DATE, 
	EXP_Determine_AK.CLM_DISCOVERY_DT AS CLAIM_DISCOVERY_DATE, 
	EXP_Determine_AK.CLM_CATASTROPHE_CODE AS CLAIM_CAT_CODE, 
	EXP_Determine_AK.COC_START_DT AS CLAIM_CAT_START_DATE, 
	EXP_Determine_AK.COC_END_DT AS CLAIM_CAT_END_DATE, 
	EXP_Determine_AK.CLM_CSR_CLAIM_NBR AS S3P_CLAIM_NUM, 
	EXP_Determine_AK.CLM_REI_NOTIFY_DT AS REINS_NOTIFIED_DATE, 
	FIL_Insert.CON_OCCURRENCE_NBR AS CLAIM_OCCURRENCE_NUM, 
	EXP_Determine_AK.CLM_VIOL_CIT_DESC AS CLAIM_VOILATION_CITATION_DESCRIPT, 
	EXP_Determine_AK.LOSS_DESCRIPTION AS CLAIM_LOSS_DESCRIPT, 
	EXP_Determine_AK.CLM_AT_FAULT_CD AS CLAIM_INSD_AT_FAULT_CODE, 
	EXP_Determine_AK.CLM_DRIVER_NBR AS CLAIM_INSD_DRIVER_NUM, 
	EXP_Determine_AK.CLM_DRV_SAME_IND AS CLAIM_INSD_DRIVER_IND, 
	EXP_Determine_AK.LOG_NOTE_LAST_ACTIVITY_DATE AS CLAIM_LOG_NOTE_LAST_ACT_DATE, 
	EXP_Determine_AK.err_flag AS ERR_FLAG_BAL_TXN, 
	EXP_Determine_AK.CLAIM_NEXT_DIARY_DATE AS NEXT_DIARY_DATE, 
	EXP_Determine_AK.claim_rep_full_name_out AS NEXT_DIARY_DATE_REP, 
	EXP_Determine_AK.OFFSET_ONSET_INDICATOR AS OFFSET_ONSET_IND, 
	EXP_Determine_AK.err_flag AS ERR_FLAG_BAL_REINS, 
	EXP_Determine_AK.LOGICAL_FLAG, 
	EXP_Determine_AK.Crrnt_Snpsht_Flag AS CRRNT_SNPSHT_FLAG, 
	EXP_Determine_AK.Audit_Id AS AUDIT_ID, 
	EXP_Determine_AK.Eff_From_Date AS EFF_FROM_DATE, 
	EXP_Determine_AK.Eff_To_Date AS EFF_TO_DATE, 
	EXP_Determine_AK.SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	EXP_Determine_AK.Created_Date AS CREATED_DATE, 
	EXP_Determine_AK.Modified_Date AS MODIFIED_DATE, 
	EXP_Determine_AK.Default_NA AS CLAIM_CREATED_BY_KEY, 
	EXP_Determine_AK.Default_NA AS WC_CAT_CODE, 
	EXP_Determine_AK.SupStateID AS SUPSTATEID, 
	EXP_Determine_AK.sup_claim_insd_at_fault_code_id AS SUPCLAIMINSUREDATFAULTCODEID, 
	EXP_Determine_AK.Default_NA AS PRIMARYWORKGROUP, 
	EXP_Determine_AK.Default_NA AS SECONDARYWORKGROUP, 
	EXP_Determine_AK.CLAIMRELATIONSHIPKEY
	FROM EXP_Determine_AK
),
SQ_Claim_Occurrence AS (
	SELECT 
		a.claim_occurrence_id, 
		a.claim_occurrence_key, 
		a.eff_from_date, 
		a.eff_to_date 
	FROM
	 	@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence a
	WHERE 
	 a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND 
	 EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence b
			WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.claim_occurrence_key = b.claim_occurrence_key
			GROUP BY claim_occurrence_key
			HAVING COUNT(*) > 1)
	ORDER BY claim_occurrence_key, eff_from_date  DESC
),
EXP_Lag_Eff_From_Date AS (
	SELECT
	claim_occurrence_id,
	claim_occurrence_key,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	claim_occurrence_key = v_PREV_ROW_occurrence_key, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(
	    TRUE,
	    claim_occurrence_key = v_PREV_ROW_occurrence_key, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	claim_occurrence_key AS v_PREV_ROW_occurrence_key,
	sysdate AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_Claim_Occurrence
),
FIL_FirstRowInAKGroup AS (
	SELECT
	claim_occurrence_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Lag_Eff_From_Date
	WHERE orig_eff_to_date <> eff_to_date

--If these two dates equal each other we are dealing with the first row in an AK group.  This row
--does not need to be expired or updated for any reason thus it can be filtered out
-- but we must source it to capture the eff_from_date of this row 
--so that we can properly expire the subsequent row
),
UPD_Claim_Occurrence AS (
	SELECT
	claim_occurrence_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FIL_FirstRowInAKGroup
),
Claim_Occurrence_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence AS T
	USING UPD_Claim_Occurrence AS S
	ON T.claim_occurrence_id = S.claim_occurrence_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),
SQ_TaskStage AS (
	select distinct ClaimId
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.TaskStage
	where SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
	and (CreatedDate > '@{pipeline().parameters.SELECTION_START_TS}' or ModifiedDate > '@{pipeline().parameters.SELECTION_START_TS}')
	and not exists (select 1 
					from @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_42gp_stage 
					where pif_symbol + pif_policy_number + pif_module + right('00' + LTRIM(STR(ipfcgp_month_of_loss)), 2) + right('00' + LTRIM(STR(ipfcgp_day_of_loss)), 2) + ltrim(str(ipfcgp_year_of_loss)) + ipfcgp_loss_occurence = ClaimId
					and logical_flag in ('0','1'))
),
EXP_Collect AS (
	SELECT
	ClaimId,
	SYSDATE AS OUT_modified_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS OUT_source_sys_id,
	'T' AS OUT_crrnt_snpsht_flag
	FROM SQ_TaskStage
),
mplt_claim_occurrence_next_diary1 AS (WITH
	INPUT AS (
		
	),
	LKP_Task_NextDueDiary_ByClaim AS (
		SELECT
		DueDate,
		ClaimId
		FROM (
			select MIN(T.DueDate) as DueDate, T.ClaimId as ClaimId 
			from @{pipeline().parameters.SOURCE_TABLE_OWNER}.TaskStage T 
			join @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupTaskStatusStage STS on T.SupTaskStatusId = STS.SupTaskStatusId and STS.Description = 'Open' 
			where T.SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
			group by T.ClaimId
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY ClaimId ORDER BY DueDate DESC) = 1
	),
	LKP_Task_DiaryByClaimAndDueDate AS (
		SELECT
		DueDate,
		AssignedUserName,
		ClaimId
		FROM (
			select T.DueDate as DueDate, T.AssignedUserName as AssignedUserName, T.ClaimId as ClaimId
			from @{pipeline().parameters.SOURCE_TABLE_OWNER}.TaskStage T
			join @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupTaskStatusStage STS on T.SupTaskStatusId = STS.SupTaskStatusId and STS.Description = 'Open' 
			where T.SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY ClaimId,DueDate ORDER BY DueDate DESC) = 1
	),
	OUTPUT AS (
		SELECT
		DueDate, 
		AssignedUserName
		FROM LKP_Task_DiaryByClaimAndDueDate
	),
),
EXP_Default_Values AS (
	SELECT
	DueDate AS IN_DueDate,
	-- *INF*: IIF(ISNULL(IN_DueDate),
	-- TO_DATE('1/1/1800','MM/DD/YYYY'),
	-- IN_DueDate
	-- )
	IFF(IN_DueDate IS NULL, TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'), IN_DueDate) AS OUT_DueDate,
	AssignedUserName AS IN_AssignedUserName,
	-- *INF*: IIF(ISNULL(IN_AssignedUserName),
	-- 'N/A',
	-- IN_AssignedUserName)
	IFF(IN_AssignedUserName IS NULL, 'N/A', IN_AssignedUserName) AS OUT_AssignedUserName
	FROM mplt_claim_occurrence_next_diary1
),
LKP_claim_occurrence_active_record AS (
	SELECT
	claim_occurrence_id,
	claim_occurrence_ak_id,
	pol_key_ak_id,
	pol_key,
	claim_occurrence_key,
	claim_occurrence_type_code,
	source_claim_occurrence_status_code,
	notice_claim_ind,
	s3p_claim_created_date,
	source_claim_rpted_date,
	s3p_claim_updated_date,
	rpt_method,
	how_claim_rpted,
	loss_loc_addr,
	loss_loc_city,
	loss_loc_county,
	loss_loc_state,
	loss_loc_zip,
	claim_loss_date,
	claim_discovery_date,
	claim_cat_code,
	claim_cat_start_date,
	claim_cat_end_date,
	s3p_claim_num,
	reins_notified_date,
	claim_occurrence_num,
	claim_voilation_citation_descript,
	claim_loss_descript,
	claim_insd_at_fault_code,
	claim_insd_driver_num,
	claim_insd_driver_ind,
	claim_log_note_last_act_date,
	err_flag_bal_txn,
	next_diary_date,
	next_diary_date_rep,
	offset_onset_ind,
	err_flag_bal_reins,
	logical_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	claim_created_by_key,
	wc_cat_code,
	SupStateId,
	SupClaimInsuredAtFaultCodeId,
	PrimaryWorkGroup,
	SecondaryWorkGroup
	FROM (
		SELECT 
			claim_occurrence_id,
			claim_occurrence_ak_id,
			pol_key_ak_id,
			pol_key,
			claim_occurrence_key,
			claim_occurrence_type_code,
			source_claim_occurrence_status_code,
			notice_claim_ind,
			s3p_claim_created_date,
			source_claim_rpted_date,
			s3p_claim_updated_date,
			rpt_method,
			how_claim_rpted,
			loss_loc_addr,
			loss_loc_city,
			loss_loc_county,
			loss_loc_state,
			loss_loc_zip,
			claim_loss_date,
			claim_discovery_date,
			claim_cat_code,
			claim_cat_start_date,
			claim_cat_end_date,
			s3p_claim_num,
			reins_notified_date,
			claim_occurrence_num,
			claim_voilation_citation_descript,
			claim_loss_descript,
			claim_insd_at_fault_code,
			claim_insd_driver_num,
			claim_insd_driver_ind,
			claim_log_note_last_act_date,
			err_flag_bal_txn,
			next_diary_date,
			next_diary_date_rep,
			offset_onset_ind,
			err_flag_bal_reins,
			logical_flag,
			crrnt_snpsht_flag,
			audit_id,
			eff_from_date,
			eff_to_date,
			source_sys_id,
			created_date,
			modified_date,
			claim_created_by_key,
			wc_cat_code,
			SupStateId,
			SupClaimInsuredAtFaultCodeId,
			PrimaryWorkGroup,
			SecondaryWorkGroup
		FROM claim_occurrence
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_key,source_sys_id,crrnt_snpsht_flag ORDER BY claim_occurrence_id) = 1
),
UPD_claim_occurrence_diary AS (
	SELECT
	LKP_claim_occurrence_active_record.claim_occurrence_id, 
	EXP_Default_Values.OUT_DueDate AS DueDate, 
	EXP_Default_Values.OUT_AssignedUserName AS AssignedUserName, 
	EXP_Collect.OUT_modified_date AS modified_date
	FROM EXP_Collect
	 -- Manually join with EXP_Default_Values
	LEFT JOIN LKP_claim_occurrence_active_record
	ON LKP_claim_occurrence_active_record.claim_occurrence_key = EXP_Collect.ClaimId AND LKP_claim_occurrence_active_record.source_sys_id = EXP_Collect.OUT_source_sys_id AND LKP_claim_occurrence_active_record.crrnt_snpsht_flag = EXP_Collect.OUT_crrnt_snpsht_flag
),
claim_occurrence_update_diary AS (
	MERGE INTO claim_occurrence AS T
	USING UPD_claim_occurrence_diary AS S
	ON T.claim_occurrence_id = S.claim_occurrence_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.next_diary_date = S.DueDate, T.next_diary_date_rep = S.AssignedUserName, T.modified_date = S.modified_date
),
WITH
LKP_claimat_calc_rptd_date AS (
	SELECT
	claimant_date,
	claim_occurrence_ak_id
	FROM (
		SELECT CALC.claimant_date as claimant_date, CPO.claim_occurrence_ak_id as claim_occurrence_ak_id FROM claimant_calculation CALC, claim_party_occurrence CPO
		WHERE
		CPO.claim_party_occurrence_ak_id = CALC.claim_party_occurrence_ak_id
		ORDER BY CPO.claim_occurrence_ak_id , CALC.claimant_date --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id ORDER BY claimant_date) = 1
),
SQ_claim_occurrence_reserve_calculation AS (
	SELECT A.claim_occurrence_ak_id, A.financial_type_code, A.reserve_date, A.reserve_date_type, A.source_sys_id 
	FROM
	 claim_occurrence_reserve_calculation A
	WHERE A.claim_occurrence_ak_id 
	IN(
	SELECT B.claim_occurrence_ak_id FROM claim_occurrence_reserve_calculation B
	WHERE B.created_date >='@{pipeline().parameters.SELECTION_START_TS}'
	)
	ORDER BY A.claim_occurrence_ak_id, A.reserve_date, A.reserve_date_type
),
EXP_get_values AS (
	SELECT
	claim_occurrence_ak_id,
	financial_type_code,
	reserve_date,
	reserve_date_type,
	-- *INF*: IIF(claim_occurrence_ak_id = v_prev_row_claim_occurrence_ak_id, 'OLD', 'NEW')
	IFF(claim_occurrence_ak_id = v_prev_row_claim_occurrence_ak_id, 'OLD', 'NEW') AS v_claim_occurrence,
	-- *INF*: IIF(v_claim_occurrence = 'NEW', IIF(financial_type_code = 'D', reserve_date_type,'NA'), IIF(financial_type_code = 'D', reserve_date_type, v_claim_occurrence_date_type_D_old))
	IFF(
	    v_claim_occurrence = 'NEW',
	    IFF(
	        financial_type_code = 'D', reserve_date_type, 'NA'
	    ),
	    IFF(
	        financial_type_code = 'D', reserve_date_type, v_claim_occurrence_date_type_D_old
	    )
	) AS v_claim_occurrence_date_type_D,
	v_claim_occurrence_date_type_D AS v_claim_occurrence_date_type_D_old,
	-- *INF*: IIF(v_claim_occurrence = 'NEW', IIF(financial_type_code = 'E', reserve_date_type,'NA'), IIF(financial_type_code = 'E', reserve_date_type, v_claim_occurrence_date_type_E_old))
	IFF(
	    v_claim_occurrence = 'NEW',
	    IFF(
	        financial_type_code = 'E', reserve_date_type, 'NA'
	    ),
	    IFF(
	        financial_type_code = 'E', reserve_date_type, v_claim_occurrence_date_type_E_old
	    )
	) AS v_claim_occurrence_date_type_E,
	v_claim_occurrence_date_type_E AS v_claim_occurrence_date_type_E_old,
	-- *INF*: IIF(v_claim_occurrence = 'NEW', IIF(financial_type_code = 'S', reserve_date_type,'NA'), IIF(financial_type_code = 'S', reserve_date_type, v_claim_occurrence_date_type_S_old))
	IFF(
	    v_claim_occurrence = 'NEW',
	    IFF(
	        financial_type_code = 'S', reserve_date_type, 'NA'
	    ),
	    IFF(
	        financial_type_code = 'S', reserve_date_type, v_claim_occurrence_date_type_S_old
	    )
	) AS v_claim_occurrence_date_type_S,
	v_claim_occurrence_date_type_S AS v_claim_occurrence_date_type_S_old,
	-- *INF*: IIF(v_claim_occurrence = 'NEW', IIF(financial_type_code = 'B', reserve_date_type,'NA'), IIF(financial_type_code = 'B', reserve_date_type, v_claim_occurrence_date_type_B_old))
	IFF(
	    v_claim_occurrence = 'NEW',
	    IFF(
	        financial_type_code = 'B', reserve_date_type, 'NA'
	    ),
	    IFF(
	        financial_type_code = 'B', reserve_date_type, v_claim_occurrence_date_type_B_old
	    )
	) AS v_claim_occurrence_date_type_B,
	v_claim_occurrence_date_type_B AS v_claim_occurrence_date_type_B_old,
	-- *INF*: IIF(v_claim_occurrence = 'NEW', IIF(financial_type_code = 'R', reserve_date_type,'NA'), IIF(financial_type_code = 'R', reserve_date_type, v_claim_occurrence_date_type_R_old))
	IFF(
	    v_claim_occurrence = 'NEW',
	    IFF(
	        financial_type_code = 'R', reserve_date_type, 'NA'
	    ),
	    IFF(
	        financial_type_code = 'R', reserve_date_type, v_claim_occurrence_date_type_R_old
	    )
	) AS v_claim_occurrence_date_type_R,
	v_claim_occurrence_date_type_R AS v_claim_occurrence_date_type_R_old,
	-- *INF*: IIF(
	-- (ISNULL(v_claim_occurrence_date_type_D) OR v_claim_occurrence_date_type_D = 'NA' OR v_claim_occurrence_date_type_D = '1NOTICEONLY' )
	-- AND(ISNULL(v_claim_occurrence_date_type_E) OR v_claim_occurrence_date_type_E = 'NA' OR v_claim_occurrence_date_type_E = '1NOTICEONLY') 
	-- AND (ISNULL(v_claim_occurrence_date_type_S) OR v_claim_occurrence_date_type_S = 'NA' OR v_claim_occurrence_date_type_S = '1NOTICEONLY') 
	-- AND (ISNULL(v_claim_occurrence_date_type_B) OR v_claim_occurrence_date_type_B = 'NA' OR v_claim_occurrence_date_type_B = '1NOTICEONLY')
	-- AND (ISNULL(v_claim_occurrence_date_type_R) OR v_claim_occurrence_date_type_R = 'NA' OR v_claim_occurrence_date_type_R = '1NOTICEONLY'), '1NOTICEONLY', 
	-- 
	-- IIF(v_claim_occurrence_date_type_D = '2OPEN' OR v_claim_occurrence_date_type_D = '4REOPEN' OR v_claim_occurrence_date_type_E = '2OPEN' OR v_claim_occurrence_date_type_E = '4REOPEN' OR v_claim_occurrence_date_type_S = '2OPEN' OR v_claim_occurrence_date_type_S = '4REOPEN' OR v_claim_occurrence_date_type_B = '2OPEN' OR v_claim_occurrence_date_type_B = '4REOPEN' OR v_claim_occurrence_date_type_R = '2OPEN' OR v_claim_occurrence_date_type_R = '4REOPEN', '2OPEN',
	-- 
	-- '3CLOSED'))
	IFF(
	    (v_claim_occurrence_date_type_D IS NULL
	    or v_claim_occurrence_date_type_D = 'NA'
	    or v_claim_occurrence_date_type_D = '1NOTICEONLY')
	    and (v_claim_occurrence_date_type_E IS NULL
	    or v_claim_occurrence_date_type_E = 'NA'
	    or v_claim_occurrence_date_type_E = '1NOTICEONLY')
	    and (v_claim_occurrence_date_type_S IS NULL
	    or v_claim_occurrence_date_type_S = 'NA'
	    or v_claim_occurrence_date_type_S = '1NOTICEONLY')
	    and (v_claim_occurrence_date_type_B IS NULL
	    or v_claim_occurrence_date_type_B = 'NA'
	    or v_claim_occurrence_date_type_B = '1NOTICEONLY')
	    and (v_claim_occurrence_date_type_R IS NULL
	    or v_claim_occurrence_date_type_R = 'NA'
	    or v_claim_occurrence_date_type_R = '1NOTICEONLY'),
	    '1NOTICEONLY',
	    IFF(
	        v_claim_occurrence_date_type_D = '2OPEN'
	        or v_claim_occurrence_date_type_D = '4REOPEN'
	        or v_claim_occurrence_date_type_E = '2OPEN'
	        or v_claim_occurrence_date_type_E = '4REOPEN'
	        or v_claim_occurrence_date_type_S = '2OPEN'
	        or v_claim_occurrence_date_type_S = '4REOPEN'
	        or v_claim_occurrence_date_type_B = '2OPEN'
	        or v_claim_occurrence_date_type_B = '4REOPEN'
	        or v_claim_occurrence_date_type_R = '2OPEN'
	        or v_claim_occurrence_date_type_R = '4REOPEN',
	        '2OPEN',
	        '3CLOSED'
	    )
	) AS v_overall_claim_occurrence_date_type_crrnt,
	-- *INF*: IIF(v_overall_claim_occurrence_date_type_crrnt = '1NOTICEONLY', '1NOTICEONLY', IIF(v_overall_claim_occurrence_date_type_crrnt = '2OPEN', IIF(IN(v_claim_occurrence_date_type_out_old , '3CLOSED', '5CLOSEDAFTERREOPEN', '4REOPEN') AND v_claim_occurrence = 'OLD', '4REOPEN', '2OPEN'), IIF(v_overall_claim_occurrence_date_type_crrnt = '3CLOSED', IIF(IN(v_claim_occurrence_date_type_out_old, '4REOPEN', '5CLOSEDAFTERREOPEN'), '5CLOSEDAFTERREOPEN', '3CLOSED'))))
	IFF(
	    v_overall_claim_occurrence_date_type_crrnt = '1NOTICEONLY', '1NOTICEONLY',
	    IFF(
	        v_overall_claim_occurrence_date_type_crrnt = '2OPEN',
	        IFF(
	            v_claim_occurrence_date_type_out_old IN ('3CLOSED','5CLOSEDAFTERREOPEN','4REOPEN')
	            and v_claim_occurrence = 'OLD',
	            '4REOPEN',
	            '2OPEN'
	        ),
	        IFF(
	            v_overall_claim_occurrence_date_type_crrnt = '3CLOSED',
	            IFF(
	                v_claim_occurrence_date_type_out_old IN ('4REOPEN','5CLOSEDAFTERREOPEN'),
	                '5CLOSEDAFTERREOPEN',
	                '3CLOSED'
	            )
	        )
	    )
	) AS v_claim_occurrence_date_type_out,
	-- *INF*: IIF(v_claim_occurrence = 'NEW', 'INSERT', IIF(v_claim_occurrence_date_type_out = v_claim_occurrence_date_type_out_old, 'NOCHANGE', 'INSERT'))
	IFF(
	    v_claim_occurrence = 'NEW', 'INSERT',
	    IFF(
	        v_claim_occurrence_date_type_out = v_claim_occurrence_date_type_out_old, 'NOCHANGE',
	        'INSERT'
	    )
	) AS v_insert_flag,
	v_insert_flag AS insert_flag_out,
	v_claim_occurrence_date_type_out AS v_claim_occurrence_date_type_out_old,
	v_claim_occurrence_date_type_out AS claim_occurrence_date_type_out,
	claim_occurrence_ak_id AS v_prev_row_claim_occurrence_ak_id,
	reserve_date_type AS v_prev_row_reserve_date_type,
	financial_type_code AS v_prev_row_financial_type_code,
	source_sys_id
	FROM SQ_claim_occurrence_reserve_calculation
),
FIL_non_inserts AS (
	SELECT
	claim_occurrence_ak_id, 
	financial_type_code, 
	reserve_date, 
	reserve_date_type, 
	insert_flag_out, 
	claim_occurrence_date_type_out, 
	source_sys_id
	FROM EXP_get_values
	WHERE insert_flag_out = 'INSERT'
),
LKP_Claim_occurrence_Calc1 AS (
	SELECT
	claim_occurrence_calculation_id,
	claim_occurrence_status_code,
	claim_occurrence_reported_date,
	claim_supplemental_ind,
	claim_financial_ind,
	claim_recovery_ind,
	claim_notice_only_ind,
	claim_occurrence_ak_id,
	claim_occurrence_date,
	claim_occurrence_date_type
	FROM (
		SELECT 
			claim_occurrence_calculation_id,
			claim_occurrence_status_code,
			claim_occurrence_reported_date,
			claim_supplemental_ind,
			claim_financial_ind,
			claim_recovery_ind,
			claim_notice_only_ind,
			claim_occurrence_ak_id,
			claim_occurrence_date,
			claim_occurrence_date_type
		FROM claim_occurrence_calculation
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,claim_occurrence_date,claim_occurrence_date_type ORDER BY claim_occurrence_calculation_id) = 1
),
EXP_get_lookup_values AS (
	SELECT
	FIL_non_inserts.claim_occurrence_ak_id,
	FIL_non_inserts.financial_type_code,
	FIL_non_inserts.reserve_date,
	FIL_non_inserts.reserve_date_type,
	FIL_non_inserts.claim_occurrence_date_type_out,
	FIL_non_inserts.source_sys_id,
	LKP_Claim_occurrence_Calc1.claim_occurrence_calculation_id AS lkp_caim_occurrence_clac_id
	FROM FIL_non_inserts
	LEFT JOIN LKP_Claim_occurrence_Calc1
	ON LKP_Claim_occurrence_Calc1.claim_occurrence_ak_id = FIL_non_inserts.claim_occurrence_ak_id AND LKP_Claim_occurrence_Calc1.claim_occurrence_date = FIL_non_inserts.reserve_date AND LKP_Claim_occurrence_Calc1.claim_occurrence_date_type = FIL_non_inserts.claim_occurrence_date_type_out
),
FIL_existing_records AS (
	SELECT
	EXP_get_lookup_values.lkp_caim_occurrence_clac_id AS lkp_claim_occurrence_calculation_id, 
	FIL_non_inserts.claim_occurrence_ak_id, 
	FIL_non_inserts.financial_type_code, 
	FIL_non_inserts.reserve_date, 
	FIL_non_inserts.reserve_date_type, 
	FIL_non_inserts.claim_occurrence_date_type_out, 
	FIL_non_inserts.source_sys_id
	FROM EXP_get_lookup_values
	 -- Manually join with FIL_non_inserts
	WHERE IIF(ISNULL(lkp_claim_occurrence_calculation_id), TRUE, FALSE)
),
LKP_Claimant_calc_financial_ind AS (
	SELECT
	IN_claim_occurrence_ak_id,
	IN_reserve_date,
	claim_occurrence_ak_id,
	claimant_date,
	claimant_financial_ind
	FROM (
		SELECT CALC.claimant_financial_ind as claimant_financial_ind, CPO.claim_occurrence_ak_id as claim_occurrence_ak_id, CALC.claimant_date as claimant_date FROM claimant_calculation CALC, claim_party_occurrence CPO
		WHERE
		CPO.claim_party_occurrence_ak_id = CALC.claim_party_occurrence_ak_id
		AND CALC.claimant_financial_ind = 'Y'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,claimant_date ORDER BY IN_claim_occurrence_ak_id) = 1
),
LKP_Claimant_calc_supplemental_ind AS (
	SELECT
	IN_claim_occurrence_ak_id,
	IN_reserve_date,
	claim_occurrence_ak_id,
	claimant_date,
	claimant_supplemental_ind
	FROM (
		SELECT CALC.claimant_supplemental_ind as claimant_supplemental_ind, CPO.claim_occurrence_ak_id as claim_occurrence_ak_id, CALC.claimant_date as claimant_date FROM claimant_calculation CALC, claim_party_occurrence CPO
		WHERE
		CPO.claim_party_occurrence_ak_id = CALC.claim_party_occurrence_ak_id
		AND CALC.claimant_supplemental_ind = 'Y'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,claimant_date ORDER BY IN_claim_occurrence_ak_id) = 1
),
LKP_claim_occurrence AS (
	SELECT
	IN_claim_occurrence_ak_id,
	claim_occurrence_ak_id,
	s3p_claim_created_date
	FROM (
		SELECT 
			IN_claim_occurrence_ak_id,
			claim_occurrence_ak_id,
			s3p_claim_created_date
		FROM claim_occurrence
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id ORDER BY IN_claim_occurrence_ak_id) = 1
),
LKP_claimant_calc_notice_only AS (
	SELECT
	IN_claim_occurrence_ak_id,
	IN_reserve_date,
	claim_occurrence_ak_id,
	claimant_date,
	claimant_notice_only_ind
	FROM (
		SELECT CALC.claimant_notice_only_ind as claimant_notice_only_ind, CPO.claim_occurrence_ak_id as claim_occurrence_ak_id, CALC.claimant_date as claimant_date FROM claimant_calculation CALC, claim_party_occurrence CPO
		WHERE
		CPO.claim_party_occurrence_ak_id = CALC.claim_party_occurrence_ak_id
		AND CALC.claimant_notice_only_ind = 'N'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,claimant_date ORDER BY IN_claim_occurrence_ak_id) = 1
),
LKP_claimant_calc_recovery_ind AS (
	SELECT
	IN_claim_occurrence_ak_id,
	IN_reserve_date,
	claim_occurrence_ak_id,
	claimant_date,
	claimant_recovery_ind
	FROM (
		SELECT CALC.claimant_recovery_ind as claimant_recovery_ind, CPO.claim_occurrence_ak_id as claim_occurrence_ak_id, CALC.claimant_date as claimant_date FROM claimant_calculation CALC, claim_party_occurrence CPO
		WHERE
		CPO.claim_party_occurrence_ak_id = CALC.claim_party_occurrence_ak_id
		AND CALC.claimant_recovery_ind = 'Y'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,claimant_date ORDER BY IN_claim_occurrence_ak_id) = 1
),
EXP_calculate_values AS (
	SELECT
	FIL_existing_records.claim_occurrence_ak_id,
	FIL_existing_records.reserve_date,
	FIL_existing_records.reserve_date_type,
	FIL_existing_records.claim_occurrence_date_type_out,
	-- *INF*: DECODE(claim_occurrence_date_type_out, '1NOTICEONLY', 'N', '2OPEN', 'O', '3CLOSED', 'C', '4REOPEN', 'O', '5CLOSEDAFTERREOPEN', 'C')
	DECODE(
	    claim_occurrence_date_type_out,
	    '1NOTICEONLY', 'N',
	    '2OPEN', 'O',
	    '3CLOSED', 'C',
	    '4REOPEN', 'O',
	    '5CLOSEDAFTERREOPEN', 'C'
	) AS claim_occurrence_status_code_out,
	-- *INF*: :LKP.LKP_CLAIMAT_CALC_RPTD_DATE(claim_occurrence_ak_id)
	LKP_CLAIMAT_CALC_RPTD_DATE_claim_occurrence_ak_id.claimant_date AS v_claim_occurrence_rpted_date_pms,
	v_claim_occurrence_rpted_date_pms AS claim_occurrence_rpted_date_pms,
	LKP_claim_occurrence.s3p_claim_created_date AS claim_occurrence_rpted_date_exceed,
	-- *INF*: IIF(source_sys_id = 'EXCEED', claim_occurrence_rpted_date_exceed, v_claim_occurrence_rpted_date_pms)
	IFF(
	    source_sys_id = 'EXCEED', claim_occurrence_rpted_date_exceed,
	    v_claim_occurrence_rpted_date_pms
	) AS claim_occurrence_rpted_date_out,
	LKP_claimant_calc_notice_only.claimant_notice_only_ind AS lkp_claimant_notice_only_ind,
	-- *INF*: IIF(ISNULL(lkp_claimant_notice_only_ind), 'Y', 'N')
	IFF(lkp_claimant_notice_only_ind IS NULL, 'Y', 'N') AS claim_occurrence_notice_only_indicator,
	LKP_claimant_calc_recovery_ind.claimant_recovery_ind AS lkp_claimant_recovery_ind,
	-- *INF*: IIF(ISNULL(lkp_claimant_recovery_ind), 'N', 'Y')
	IFF(lkp_claimant_recovery_ind IS NULL, 'N', 'Y') AS claim_occurrence_recovery_ind_out,
	LKP_Claimant_calc_supplemental_ind.claimant_supplemental_ind AS lkp_claimant_supplemental_ind,
	-- *INF*: IIF(ISNULL(lkp_claimant_supplemental_ind), 'N', 'Y')
	IFF(lkp_claimant_supplemental_ind IS NULL, 'N', 'Y') AS claim_occurrence_supplemental_ind_out,
	LKP_Claimant_calc_financial_ind.claimant_financial_ind AS lkp_claimant_financial_ind,
	-- *INF*: IIF(ISNULL(lkp_claimant_financial_ind), 'N', 'Y')
	IFF(lkp_claimant_financial_ind IS NULL, 'N', 'Y') AS claim_occurrence_financial_indicator,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	reserve_date AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS') 
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	FIL_existing_records.source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM FIL_existing_records
	LEFT JOIN LKP_Claimant_calc_financial_ind
	ON LKP_Claimant_calc_financial_ind.claim_occurrence_ak_id = FIL_existing_records.claim_occurrence_ak_id AND LKP_Claimant_calc_financial_ind.claimant_date <= FIL_existing_records.reserve_date
	LEFT JOIN LKP_Claimant_calc_supplemental_ind
	ON LKP_Claimant_calc_supplemental_ind.claim_occurrence_ak_id = FIL_existing_records.claim_occurrence_ak_id AND LKP_Claimant_calc_supplemental_ind.claimant_date <= FIL_existing_records.reserve_date
	LEFT JOIN LKP_claim_occurrence
	ON LKP_claim_occurrence.claim_occurrence_ak_id = FIL_existing_records.claim_occurrence_ak_id
	LEFT JOIN LKP_claimant_calc_notice_only
	ON LKP_claimant_calc_notice_only.claim_occurrence_ak_id = FIL_existing_records.claim_occurrence_ak_id AND LKP_claimant_calc_notice_only.claimant_date <= FIL_existing_records.reserve_date
	LEFT JOIN LKP_claimant_calc_recovery_ind
	ON LKP_claimant_calc_recovery_ind.claim_occurrence_ak_id = FIL_existing_records.claim_occurrence_ak_id AND LKP_claimant_calc_recovery_ind.claimant_date <= FIL_existing_records.reserve_date
	LEFT JOIN LKP_CLAIMAT_CALC_RPTD_DATE LKP_CLAIMAT_CALC_RPTD_DATE_claim_occurrence_ak_id
	ON LKP_CLAIMAT_CALC_RPTD_DATE_claim_occurrence_ak_id.claim_occurrence_ak_id = claim_occurrence_ak_id

),
SEQ_claim_occ_calc_ak_id AS (
	CREATE SEQUENCE SEQ_claim_occ_calc_ak_id
	START = 0
	INCREMENT = 1;
),
claim_occurrence_calculation AS (
	INSERT INTO claim_occurrence_calculation
	(claim_occurrence_calculation_ak_id, claim_occurrence_ak_id, claim_occurrence_status_code, claim_occurrence_date, claim_occurrence_date_type, claim_occurrence_reported_date, claim_supplemental_ind, claim_financial_ind, claim_recovery_ind, claim_notice_only_ind, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	SEQ_claim_occ_calc_ak_id.NEXTVAL AS CLAIM_OCCURRENCE_CALCULATION_AK_ID, 
	CLAIM_OCCURRENCE_AK_ID, 
	claim_occurrence_status_code_out AS CLAIM_OCCURRENCE_STATUS_CODE, 
	reserve_date AS CLAIM_OCCURRENCE_DATE, 
	claim_occurrence_date_type_out AS CLAIM_OCCURRENCE_DATE_TYPE, 
	claim_occurrence_rpted_date_out AS CLAIM_OCCURRENCE_REPORTED_DATE, 
	claim_occurrence_supplemental_ind_out AS CLAIM_SUPPLEMENTAL_IND, 
	claim_occurrence_financial_indicator AS CLAIM_FINANCIAL_IND, 
	claim_occurrence_recovery_ind_out AS CLAIM_RECOVERY_IND, 
	claim_occurrence_notice_only_indicator AS CLAIM_NOTICE_ONLY_IND, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM EXP_calculate_values
),
SQ_claim_occurrence_insert_missing_claims AS (
	SELECT claim_occurrence.claim_occurrence_ak_id, claim_occurrence.source_claim_occurrence_status_code, claim_occurrence.notice_claim_ind, claim_occurrence.s3p_claim_created_date, claim_occurrence.s3p_claim_updated_date, claim_occurrence.source_sys_id 
	FROM
	 claim_occurrence
	WHERE  claim_occurrence.claim_occurrence_ak_id NOT IN
	(SELECT DISTINCT claim_occurrence_ak_id FROM claim_occurrence_calculation)
	AND claim_occurrence.source_sys_id = 'EXCEED' and claim_occurrence.crrnt_snpsht_flag = 1
),
EXP_set_values_insert_missing_claims AS (
	SELECT
	claim_occurrence_ak_id,
	notice_claim_ind AS S3P_not_claim_ind,
	source_claim_occurrence_status_code AS s3p_claim_occurrence_status_code,
	-- *INF*: IIF(S3P_not_claim_ind = 'N', S3P_not_claim_ind, IIF(s3p_claim_occurrence_status_code = 'C', 'O',s3p_claim_occurrence_status_code) )
	IFF(
	    S3P_not_claim_ind = 'N', S3P_not_claim_ind,
	    IFF(
	        s3p_claim_occurrence_status_code = 'C', 'O', s3p_claim_occurrence_status_code
	    )
	) AS claim_occurrence_status_code_out,
	-- *INF*: IIF(S3P_not_claim_ind = 'N', S3P_not_claim_ind, s3p_claim_occurrence_status_code)
	IFF(S3P_not_claim_ind = 'N', S3P_not_claim_ind, s3p_claim_occurrence_status_code) AS claim_occurrence_status_code_out_closed,
	-- *INF*: IIF(S3P_not_claim_ind = 'N', '1NOTICEONLY', IIF(s3p_claim_occurrence_status_code = 'E', '6OPENEDINERROR', '7OPEN\NOFINANCIAL'))
	IFF(
	    S3P_not_claim_ind = 'N', '1NOTICEONLY',
	    IFF(
	        s3p_claim_occurrence_status_code = 'E', '6OPENEDINERROR', '7OPEN\NOFINANCIAL'
	    )
	) AS claim_occurrence_date_type_out,
	-- *INF*: IIF(S3P_not_claim_ind = 'C', '8CLOSED\NOFINANCIAL')
	IFF(S3P_not_claim_ind = 'C', '8CLOSED\NOFINANCIAL') AS claim_occurrence_date_type_out_closed,
	s3p_claim_created_date,
	s3p_claim_updated_date,
	'N' AS claim_supplemental_ind,
	'N' AS claim_financial_ind,
	'N' AS claim_recovery_ind,
	-- *INF*: IIF(S3P_not_claim_ind = 'N', 'Y', 'N')
	IFF(S3P_not_claim_ind = 'N', 'Y', 'N') AS claim_notice_only_ind,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	s3p_claim_created_date AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS') 
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM SQ_claim_occurrence_insert_missing_claims
),
RTR_OPEN_CLOSED_insert_missing_claims AS (
	SELECT
	claim_occurrence_ak_id,
	s3p_claim_occurrence_status_code,
	claim_occurrence_status_code_out,
	claim_occurrence_status_code_out_closed,
	claim_occurrence_date_type_out,
	claim_occurrence_date_type_out_closed,
	s3p_claim_created_date,
	s3p_claim_updated_date,
	claim_supplemental_ind,
	claim_financial_ind,
	claim_recovery_ind,
	claim_notice_only_ind,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date
	FROM EXP_set_values_insert_missing_claims
),
RTR_OPEN_CLOSED_insert_missing_claims_INSERT_OPEN AS (SELECT * FROM RTR_OPEN_CLOSED_insert_missing_claims WHERE TRUE),
RTR_OPEN_CLOSED_insert_missing_claims_INSERT_CLOSED AS (SELECT * FROM RTR_OPEN_CLOSED_insert_missing_claims WHERE s3p_claim_occurrence_status_code = 'C'),
SEQ_claim_occ_calc_ak_id1 AS (
	CREATE SEQUENCE SEQ_claim_occ_calc_ak_id1
	START = 0
	INCREMENT = 1;
),
claim_occurrence_calculation_insert_closed_no_fin3 AS (
	INSERT INTO claim_occurrence_calculation
	(claim_occurrence_calculation_ak_id, claim_occurrence_ak_id, claim_occurrence_status_code, claim_occurrence_date, claim_occurrence_date_type, claim_occurrence_reported_date, claim_supplemental_ind, claim_financial_ind, claim_recovery_ind, claim_notice_only_ind, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	SEQ_claim_occ_calc_ak_id1.NEXTVAL AS CLAIM_OCCURRENCE_CALCULATION_AK_ID, 
	CLAIM_OCCURRENCE_AK_ID, 
	claim_occurrence_status_code_out_closed AS CLAIM_OCCURRENCE_STATUS_CODE, 
	s3p_claim_created_date AS CLAIM_OCCURRENCE_DATE, 
	claim_occurrence_date_type_out_closed AS CLAIM_OCCURRENCE_DATE_TYPE, 
	s3p_claim_created_date AS CLAIM_OCCURRENCE_REPORTED_DATE, 
	CLAIM_SUPPLEMENTAL_IND, 
	CLAIM_FINANCIAL_IND, 
	CLAIM_RECOVERY_IND, 
	CLAIM_NOTICE_ONLY_IND, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	s3p_claim_created_date AS EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM RTR_OPEN_CLOSED_insert_missing_claims_INSERT_CLOSED
),
claim_occurrence_calculation_insert_missing_claims1 AS (
	INSERT INTO claim_occurrence_calculation
	(claim_occurrence_calculation_ak_id, claim_occurrence_ak_id, claim_occurrence_status_code, claim_occurrence_date, claim_occurrence_date_type, claim_occurrence_reported_date, claim_supplemental_ind, claim_financial_ind, claim_recovery_ind, claim_notice_only_ind, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	SEQ_claim_occ_calc_ak_id1.NEXTVAL AS CLAIM_OCCURRENCE_CALCULATION_AK_ID, 
	CLAIM_OCCURRENCE_AK_ID, 
	claim_occurrence_status_code_out AS CLAIM_OCCURRENCE_STATUS_CODE, 
	s3p_claim_created_date AS CLAIM_OCCURRENCE_DATE, 
	claim_occurrence_date_type_out AS CLAIM_OCCURRENCE_DATE_TYPE, 
	s3p_claim_created_date AS CLAIM_OCCURRENCE_REPORTED_DATE, 
	CLAIM_SUPPLEMENTAL_IND, 
	CLAIM_FINANCIAL_IND, 
	CLAIM_RECOVERY_IND, 
	CLAIM_NOTICE_ONLY_IND, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM RTR_OPEN_CLOSED_insert_missing_claims_INSERT_OPEN
),
SQ_claim_occurrence_check_changed_statuses AS (
	SELECT claim_occurrence.claim_occurrence_ak_id, claim_occurrence.source_claim_occurrence_status_code, claim_occurrence.notice_claim_ind, claim_occurrence.s3p_claim_created_date, claim_occurrence.s3p_claim_updated_date, claim_occurrence.source_sys_id 
	FROM
	 claim_occurrence
	WHERE  
	claim_occurrence.claim_occurrence_ak_id IN
	(SELECT DISTINCT claim_occurrence_ak_id FROM claim_occurrence_calculation where claim_occurrence_date_type 
	in ('1NOTICEONLY', '6OPENEDINERROR', '7OPEN\NOFINANCIAL', '8CLOSED\NOFINANCIAL' )) 
	AND claim_occurrence.source_sys_id = 'EXCEED' and claim_occurrence.crrnt_snpsht_flag = 1
),
EXP_set_values AS (
	SELECT
	claim_occurrence_ak_id,
	notice_claim_ind AS S3P_not_claim_ind,
	source_claim_occurrence_status_code AS s3p_claim_occurrence_status_code,
	-- *INF*: IIF(S3P_not_claim_ind = 'N', S3P_not_claim_ind, IIF(s3p_claim_occurrence_status_code = 'C', 'O',s3p_claim_occurrence_status_code) )
	IFF(
	    S3P_not_claim_ind = 'N', S3P_not_claim_ind,
	    IFF(
	        s3p_claim_occurrence_status_code = 'C', 'O', s3p_claim_occurrence_status_code
	    )
	) AS claim_occurrence_status_code_out,
	-- *INF*: IIF(S3P_not_claim_ind = 'N', S3P_not_claim_ind, s3p_claim_occurrence_status_code)
	IFF(S3P_not_claim_ind = 'N', S3P_not_claim_ind, s3p_claim_occurrence_status_code) AS claim_occurrence_status_code_out_closed,
	-- *INF*: IIF(S3P_not_claim_ind = 'N', '1NOTICEONLY', IIF(s3p_claim_occurrence_status_code = 'E', '6OPENEDINERROR', '7OPEN\NOFINANCIAL'))
	IFF(
	    S3P_not_claim_ind = 'N', '1NOTICEONLY',
	    IFF(
	        s3p_claim_occurrence_status_code = 'E', '6OPENEDINERROR', '7OPEN\NOFINANCIAL'
	    )
	) AS claim_occurrence_date_type_out,
	-- *INF*: IIF(S3P_not_claim_ind = 'C', '8CLOSED\NOFINANCIAL')
	IFF(S3P_not_claim_ind = 'C', '8CLOSED\NOFINANCIAL') AS claim_occurrence_date_type_out_closed,
	s3p_claim_created_date,
	s3p_claim_updated_date,
	'N' AS claim_supplemental_ind,
	'N' AS claim_financial_ind,
	'N' AS claim_recovery_ind,
	-- *INF*: IIF(S3P_not_claim_ind = 'N', 'Y', 'N')
	IFF(S3P_not_claim_ind = 'N', 'Y', 'N') AS claim_notice_only_ind,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	s3p_claim_created_date AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS') 
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM SQ_claim_occurrence_check_changed_statuses
),
RTR_OPEN_CLOSED AS (
	SELECT
	claim_occurrence_ak_id,
	s3p_claim_occurrence_status_code,
	claim_occurrence_status_code_out,
	claim_occurrence_status_code_out_closed,
	claim_occurrence_date_type_out,
	claim_occurrence_date_type_out_closed,
	s3p_claim_created_date,
	s3p_claim_updated_date,
	claim_supplemental_ind,
	claim_financial_ind,
	claim_recovery_ind,
	claim_notice_only_ind,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date
	FROM EXP_set_values
),
RTR_OPEN_CLOSED_INSERT_OPEN AS (SELECT * FROM RTR_OPEN_CLOSED WHERE TRUE),
RTR_OPEN_CLOSED_INSERT_CLOSED AS (SELECT * FROM RTR_OPEN_CLOSED WHERE s3p_claim_occurrence_status_code = 'C'),
LKP_Claim_occurrence_Calc_Open_No_fin AS (
	SELECT
	claim_occurrence_calculation_id,
	claim_occurrence_status_code,
	claim_occurrence_reported_date,
	claim_supplemental_ind,
	claim_financial_ind,
	claim_recovery_ind,
	claim_notice_only_ind,
	claim_occurrence_ak_id,
	claim_occurrence_date,
	claim_occurrence_date_type
	FROM (
		SELECT 
			claim_occurrence_calculation_id,
			claim_occurrence_status_code,
			claim_occurrence_reported_date,
			claim_supplemental_ind,
			claim_financial_ind,
			claim_recovery_ind,
			claim_notice_only_ind,
			claim_occurrence_ak_id,
			claim_occurrence_date,
			claim_occurrence_date_type
		FROM claim_occurrence_calculation
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,claim_occurrence_date,claim_occurrence_date_type ORDER BY claim_occurrence_calculation_id) = 1
),
EXP_determine_insert_for_open AS (
	SELECT
	RTR_OPEN_CLOSED_INSERT_OPEN.claim_occurrence_ak_id AS claim_occurrence_ak_id1,
	RTR_OPEN_CLOSED_INSERT_OPEN.claim_occurrence_status_code_out AS claim_occurrence_status_code_out1,
	RTR_OPEN_CLOSED_INSERT_OPEN.claim_occurrence_date_type_out AS claim_occurrence_date_type_out1,
	RTR_OPEN_CLOSED_INSERT_OPEN.s3p_claim_created_date AS s3p_claim_created_date1,
	RTR_OPEN_CLOSED_INSERT_OPEN.claim_supplemental_ind AS claim_supplemental_ind1,
	RTR_OPEN_CLOSED_INSERT_OPEN.claim_financial_ind AS claim_financial_ind1,
	RTR_OPEN_CLOSED_INSERT_OPEN.claim_recovery_ind AS claim_recovery_ind1,
	RTR_OPEN_CLOSED_INSERT_OPEN.claim_notice_only_ind AS claim_notice_only_ind1,
	RTR_OPEN_CLOSED_INSERT_OPEN.crrnt_snpsht_flag AS crrnt_snpsht_flag1,
	RTR_OPEN_CLOSED_INSERT_OPEN.audit_id AS audit_id1,
	RTR_OPEN_CLOSED_INSERT_OPEN.eff_from_date AS eff_from_date1,
	RTR_OPEN_CLOSED_INSERT_OPEN.eff_to_date AS eff_to_date1,
	RTR_OPEN_CLOSED_INSERT_OPEN.source_sys_id AS source_sys_id1,
	RTR_OPEN_CLOSED_INSERT_OPEN.created_date AS created_date1,
	RTR_OPEN_CLOSED_INSERT_OPEN.modified_date AS modified_date1,
	LKP_Claim_occurrence_Calc_Open_No_fin.claim_occurrence_calculation_id AS lkp_claim_occurrence_Calc_id,
	LKP_Claim_occurrence_Calc_Open_No_fin.claim_occurrence_status_code AS lkp_claim_occurrence_status_code,
	LKP_Claim_occurrence_Calc_Open_No_fin.claim_occurrence_reported_date AS lkp_claim_occurrence_reported_date,
	LKP_Claim_occurrence_Calc_Open_No_fin.claim_supplemental_ind AS lkp_claim_supplemental_ind,
	LKP_Claim_occurrence_Calc_Open_No_fin.claim_financial_ind AS lkp_claim_financial_ind,
	LKP_Claim_occurrence_Calc_Open_No_fin.claim_recovery_ind AS lkp_claim_recovery_ind,
	LKP_Claim_occurrence_Calc_Open_No_fin.claim_notice_only_ind AS lkp_claim_notice_only_ind,
	-- *INF*: IIF(ISNULL(lkp_claim_occurrence_Calc_id), 'I',
	-- IIF(lkp_claim_occurrence_status_code != claim_occurrence_status_code_out1 
	-- OR lkp_claim_occurrence_reported_date != s3p_claim_created_date1
	-- OR lkp_claim_supplemental_ind != claim_supplemental_ind1
	-- OR lkp_claim_financial_ind != claim_financial_ind1
	-- OR lkp_claim_recovery_ind != claim_recovery_ind1
	-- OR lkp_claim_notice_only_ind != claim_notice_only_ind1, 'U', 'N'))
	IFF(
	    lkp_claim_occurrence_Calc_id IS NULL, 'I',
	    IFF(
	        lkp_claim_occurrence_status_code != claim_occurrence_status_code_out1
	        or lkp_claim_occurrence_reported_date != s3p_claim_created_date1
	        or lkp_claim_supplemental_ind != claim_supplemental_ind1
	        or lkp_claim_financial_ind != claim_financial_ind1
	        or lkp_claim_recovery_ind != claim_recovery_ind1
	        or lkp_claim_notice_only_ind != claim_notice_only_ind1,
	        'U',
	        'N'
	    )
	) AS insert_update_flag
	FROM RTR_OPEN_CLOSED_INSERT_OPEN
	LEFT JOIN LKP_Claim_occurrence_Calc_Open_No_fin
	ON LKP_Claim_occurrence_Calc_Open_No_fin.claim_occurrence_ak_id = RTR_OPEN_CLOSED.claim_occurrence_ak_id1 AND LKP_Claim_occurrence_Calc_Open_No_fin.claim_occurrence_date = RTR_OPEN_CLOSED.s3p_claim_created_date1 AND LKP_Claim_occurrence_Calc_Open_No_fin.claim_occurrence_date_type = RTR_OPEN_CLOSED.claim_occurrence_date_type_out1
),
RTR_insert_update_opens AS (
	SELECT
	claim_occurrence_ak_id1 AS claim_occurrence_ak_id,
	claim_occurrence_status_code_out1 AS claim_occurrence_status_code_out,
	claim_occurrence_date_type_out1 AS claim_occurrence_date_type_out,
	s3p_claim_created_date1 AS s3p_claim_created_date,
	claim_supplemental_ind1 AS claim_supplemental_ind,
	claim_financial_ind1 AS claim_financial_ind,
	claim_recovery_ind1 AS claim_recovery_ind,
	claim_notice_only_ind1 AS claim_notice_only_ind,
	crrnt_snpsht_flag1 AS crrnt_snpsht_flag,
	audit_id1 AS audit_id,
	eff_from_date1 AS eff_from_date,
	eff_to_date1 AS eff_to_date,
	source_sys_id1 AS source_sys_id,
	created_date1 AS created_date,
	modified_date1 AS modified_date,
	insert_update_flag,
	lkp_claim_occurrence_Calc_id
	FROM EXP_determine_insert_for_open
),
RTR_insert_update_opens_Insert AS (SELECT * FROM RTR_insert_update_opens WHERE insert_update_flag = 'I'),
RTR_insert_update_opens_Update AS (SELECT * FROM RTR_insert_update_opens WHERE insert_update_flag = 'U'),
UPD_insert_open AS (
	SELECT
	claim_occurrence_ak_id AS claim_occurrence_ak_id1, 
	claim_occurrence_status_code_out AS claim_occurrence_status_code_out1, 
	claim_occurrence_date_type_out AS claim_occurrence_date_type_out1, 
	s3p_claim_created_date AS s3p_claim_created_date1, 
	claim_supplemental_ind AS claim_supplemental_ind1, 
	claim_financial_ind AS claim_financial_ind1, 
	claim_recovery_ind AS claim_recovery_ind1, 
	claim_notice_only_ind AS claim_notice_only_ind1, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag1, 
	audit_id AS audit_id1, 
	eff_from_date AS eff_from_date1, 
	eff_to_date AS eff_to_date1, 
	source_sys_id AS source_sys_id1, 
	created_date AS created_date1, 
	modified_date AS modified_date1, 
	insert_update_flag AS insert_update_flag1
	FROM RTR_insert_update_opens_Insert
),
claim_occurrence_calculation_Insert_Open_No_fin AS (
	INSERT INTO claim_occurrence_calculation
	(claim_occurrence_calculation_ak_id, claim_occurrence_ak_id, claim_occurrence_status_code, claim_occurrence_date, claim_occurrence_date_type, claim_occurrence_reported_date, claim_supplemental_ind, claim_financial_ind, claim_recovery_ind, claim_notice_only_ind, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	SEQ_claim_occ_calc_ak_id1.NEXTVAL AS CLAIM_OCCURRENCE_CALCULATION_AK_ID, 
	claim_occurrence_ak_id1 AS CLAIM_OCCURRENCE_AK_ID, 
	claim_occurrence_status_code_out1 AS CLAIM_OCCURRENCE_STATUS_CODE, 
	s3p_claim_created_date1 AS CLAIM_OCCURRENCE_DATE, 
	claim_occurrence_date_type_out1 AS CLAIM_OCCURRENCE_DATE_TYPE, 
	s3p_claim_created_date1 AS CLAIM_OCCURRENCE_REPORTED_DATE, 
	claim_supplemental_ind1 AS CLAIM_SUPPLEMENTAL_IND, 
	claim_financial_ind1 AS CLAIM_FINANCIAL_IND, 
	claim_recovery_ind1 AS CLAIM_RECOVERY_IND, 
	claim_notice_only_ind1 AS CLAIM_NOTICE_ONLY_IND, 
	crrnt_snpsht_flag1 AS CRRNT_SNPSHT_FLAG, 
	audit_id1 AS AUDIT_ID, 
	eff_from_date1 AS EFF_FROM_DATE, 
	eff_to_date1 AS EFF_TO_DATE, 
	source_sys_id1 AS SOURCE_SYS_ID, 
	created_date1 AS CREATED_DATE, 
	modified_date1 AS MODIFIED_DATE
	FROM UPD_insert_open
),
UPD_update_open AS (
	SELECT
	claim_occurrence_ak_id AS claim_occurrence_ak_id3, 
	claim_occurrence_status_code_out AS claim_occurrence_status_code_out3, 
	claim_occurrence_date_type_out AS claim_occurrence_date_type_out3, 
	s3p_claim_created_date AS s3p_claim_created_date3, 
	claim_supplemental_ind AS claim_supplemental_ind3, 
	claim_financial_ind AS claim_financial_ind3, 
	claim_recovery_ind AS claim_recovery_ind3, 
	claim_notice_only_ind AS claim_notice_only_ind3, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag3, 
	audit_id AS audit_id3, 
	eff_from_date AS eff_from_date3, 
	eff_to_date AS eff_to_date3, 
	source_sys_id AS source_sys_id3, 
	created_date AS created_date3, 
	modified_date AS modified_date3, 
	insert_update_flag AS insert_update_flag3, 
	lkp_claim_occurrence_Calc_id AS lkp_claim_occurrence_Calc_id3
	FROM RTR_insert_update_opens_Update
),
claim_occurrence_calculation_Update_Open_No_fin AS (
	MERGE INTO claim_occurrence_calculation AS T
	USING UPD_update_open AS S
	ON T.claim_occurrence_calculation_id = S.lkp_claim_occurrence_Calc_id3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.claim_occurrence_status_code = S.claim_occurrence_status_code_out3, T.claim_occurrence_reported_date = S.s3p_claim_created_date3, T.claim_supplemental_ind = S.claim_supplemental_ind3, T.claim_financial_ind = S.claim_financial_ind3, T.claim_recovery_ind = S.claim_recovery_ind3, T.claim_notice_only_ind = S.claim_notice_only_ind3, T.modified_date = S.modified_date3
),
LKP_Claim_occurrence_Calc_Closed_No_fin AS (
	SELECT
	claim_occurrence_calculation_id,
	claim_occurrence_status_code,
	claim_occurrence_reported_date,
	claim_supplemental_ind,
	claim_financial_ind,
	claim_recovery_ind,
	claim_notice_only_ind,
	claim_occurrence_ak_id,
	claim_occurrence_date,
	claim_occurrence_date_type
	FROM (
		SELECT 
			claim_occurrence_calculation_id,
			claim_occurrence_status_code,
			claim_occurrence_reported_date,
			claim_supplemental_ind,
			claim_financial_ind,
			claim_recovery_ind,
			claim_notice_only_ind,
			claim_occurrence_ak_id,
			claim_occurrence_date,
			claim_occurrence_date_type
		FROM claim_occurrence_calculation
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,claim_occurrence_date,claim_occurrence_date_type ORDER BY claim_occurrence_calculation_id) = 1
),
EXP_determine_insert_for_closed AS (
	SELECT
	RTR_OPEN_CLOSED_INSERT_CLOSED.claim_occurrence_ak_id AS claim_occurrence_ak_id3,
	RTR_OPEN_CLOSED_INSERT_CLOSED.claim_occurrence_status_code_out_closed AS claim_occurrence_status_code_out_closed3,
	RTR_OPEN_CLOSED_INSERT_CLOSED.claim_occurrence_date_type_out_closed AS claim_occurrence_date_type_out_closed3,
	RTR_OPEN_CLOSED_INSERT_CLOSED.s3p_claim_created_date AS s3p_claim_created_date3,
	RTR_OPEN_CLOSED_INSERT_CLOSED.s3p_claim_updated_date AS s3p_claim_updated_date3,
	RTR_OPEN_CLOSED_INSERT_CLOSED.claim_supplemental_ind AS claim_supplemental_ind3,
	RTR_OPEN_CLOSED_INSERT_CLOSED.claim_financial_ind AS claim_financial_ind3,
	RTR_OPEN_CLOSED_INSERT_CLOSED.claim_recovery_ind AS claim_recovery_ind3,
	RTR_OPEN_CLOSED_INSERT_CLOSED.claim_notice_only_ind AS claim_notice_only_ind3,
	RTR_OPEN_CLOSED_INSERT_CLOSED.crrnt_snpsht_flag AS crrnt_snpsht_flag3,
	RTR_OPEN_CLOSED_INSERT_CLOSED.audit_id AS audit_id3,
	RTR_OPEN_CLOSED_INSERT_CLOSED.eff_to_date AS eff_to_date3,
	RTR_OPEN_CLOSED_INSERT_CLOSED.source_sys_id AS source_sys_id3,
	RTR_OPEN_CLOSED_INSERT_CLOSED.created_date AS created_date3,
	RTR_OPEN_CLOSED_INSERT_CLOSED.modified_date AS modified_date3,
	LKP_Claim_occurrence_Calc_Closed_No_fin.claim_occurrence_calculation_id AS lkp_claim_occurrence_Calc_id,
	LKP_Claim_occurrence_Calc_Closed_No_fin.claim_occurrence_status_code AS lkp_claim_occurrence_status_code,
	LKP_Claim_occurrence_Calc_Closed_No_fin.claim_occurrence_reported_date AS lkp_claim_occurrence_reported_date,
	LKP_Claim_occurrence_Calc_Closed_No_fin.claim_supplemental_ind AS lkp_claim_supplemental_ind,
	LKP_Claim_occurrence_Calc_Closed_No_fin.claim_financial_ind AS lkp_claim_financial_ind,
	LKP_Claim_occurrence_Calc_Closed_No_fin.claim_recovery_ind AS lkp_claim_recovery_ind,
	LKP_Claim_occurrence_Calc_Closed_No_fin.claim_notice_only_ind AS lkp_claim_notice_only_ind,
	-- *INF*: IIF(ISNULL(lkp_claim_occurrence_Calc_id), 'I',
	-- IIF(lkp_claim_occurrence_status_code != claim_occurrence_status_code_out_closed3
	-- OR lkp_claim_occurrence_reported_date != s3p_claim_created_date3
	-- OR lkp_claim_supplemental_ind != claim_supplemental_ind3
	-- OR lkp_claim_financial_ind != claim_financial_ind3
	-- OR lkp_claim_recovery_ind != claim_recovery_ind3
	-- OR lkp_claim_notice_only_ind != claim_notice_only_ind3, 'U', 'N'))
	IFF(
	    lkp_claim_occurrence_Calc_id IS NULL, 'I',
	    IFF(
	        lkp_claim_occurrence_status_code != claim_occurrence_status_code_out_closed3
	        or lkp_claim_occurrence_reported_date != s3p_claim_created_date3
	        or lkp_claim_supplemental_ind != claim_supplemental_ind3
	        or lkp_claim_financial_ind != claim_financial_ind3
	        or lkp_claim_recovery_ind != claim_recovery_ind3
	        or lkp_claim_notice_only_ind != claim_notice_only_ind3,
	        'U',
	        'N'
	    )
	) AS insert_update_flag
	FROM RTR_OPEN_CLOSED_INSERT_CLOSED
	LEFT JOIN LKP_Claim_occurrence_Calc_Closed_No_fin
	ON LKP_Claim_occurrence_Calc_Closed_No_fin.claim_occurrence_ak_id = RTR_OPEN_CLOSED.claim_occurrence_ak_id3 AND LKP_Claim_occurrence_Calc_Closed_No_fin.claim_occurrence_date = RTR_OPEN_CLOSED.s3p_claim_created_date3 AND LKP_Claim_occurrence_Calc_Closed_No_fin.claim_occurrence_date_type = RTR_OPEN_CLOSED.claim_occurrence_date_type_out_closed3
),
RTR_insert_update_closed AS (
	SELECT
	claim_occurrence_ak_id3 AS claim_occurrence_ak_id,
	claim_occurrence_status_code_out_closed3 AS claim_occurrence_status_code_out,
	claim_occurrence_date_type_out_closed3 AS claim_occurrence_date_type_out,
	s3p_claim_created_date3 AS s3p_claim_created_date,
	s3p_claim_updated_date3 AS s3p_claim_updated_date,
	claim_supplemental_ind3 AS claim_supplemental_ind,
	claim_financial_ind3 AS claim_financial_ind,
	claim_recovery_ind3 AS claim_recovery_ind,
	claim_notice_only_ind3 AS claim_notice_only_ind,
	crrnt_snpsht_flag3 AS crrnt_snpsht_flag,
	audit_id3 AS audit_id,
	s3p_claim_created_date3 AS eff_from_date,
	eff_to_date3 AS eff_to_date,
	source_sys_id3 AS source_sys_id,
	created_date3 AS created_date,
	modified_date3 AS modified_date,
	insert_update_flag,
	lkp_claim_occurrence_Calc_id
	FROM EXP_determine_insert_for_closed
),
RTR_insert_update_closed_Insert AS (SELECT * FROM RTR_insert_update_closed WHERE insert_update_flag = 'I'),
RTR_insert_update_closed_Update AS (SELECT * FROM RTR_insert_update_closed WHERE insert_update_flag = 'U'),
UPD_update_closed_no_fin AS (
	SELECT
	claim_occurrence_ak_id AS claim_occurrence_ak_id3, 
	claim_occurrence_status_code_out AS claim_occurrence_status_code_out3, 
	claim_occurrence_date_type_out AS claim_occurrence_date_type_out3, 
	s3p_claim_created_date AS s3p_claim_created_date3, 
	s3p_claim_updated_date AS s3p_claim_updated_date3, 
	claim_supplemental_ind AS claim_supplemental_ind3, 
	claim_financial_ind AS claim_financial_ind3, 
	claim_recovery_ind AS claim_recovery_ind3, 
	claim_notice_only_ind AS claim_notice_only_ind3, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag3, 
	audit_id AS audit_id3, 
	eff_from_date AS eff_from_date3, 
	eff_to_date AS eff_to_date3, 
	source_sys_id AS source_sys_id3, 
	created_date AS created_date3, 
	modified_date AS modified_date3, 
	insert_update_flag AS insert_update_flag3, 
	lkp_claim_occurrence_Calc_id AS lkp_claim_occurrence_Calc_id3
	FROM RTR_insert_update_closed_Update
),
claim_occurrence_calculation_Update_Closed_no_fin AS (
	MERGE INTO claim_occurrence_calculation AS T
	USING UPD_update_closed_no_fin AS S
	ON T.claim_occurrence_calculation_id = S.lkp_claim_occurrence_Calc_id3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.claim_occurrence_status_code = S.claim_occurrence_status_code_out3, T.claim_occurrence_reported_date = S.s3p_claim_created_date3, T.claim_supplemental_ind = S.claim_supplemental_ind3, T.claim_financial_ind = S.claim_financial_ind3, T.claim_recovery_ind = S.claim_recovery_ind3, T.claim_notice_only_ind = S.claim_notice_only_ind3, T.modified_date = S.modified_date3
),
UPD_insert_closed_no_fin AS (
	SELECT
	claim_occurrence_ak_id AS claim_occurrence_ak_id1, 
	claim_occurrence_status_code_out AS claim_occurrence_status_code_out1, 
	claim_occurrence_date_type_out AS claim_occurrence_date_type_out1, 
	s3p_claim_created_date AS s3p_claim_created_date1, 
	s3p_claim_updated_date AS s3p_claim_updated_date1, 
	claim_supplemental_ind AS claim_supplemental_ind1, 
	claim_financial_ind AS claim_financial_ind1, 
	claim_recovery_ind AS claim_recovery_ind1, 
	claim_notice_only_ind AS claim_notice_only_ind1, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag1, 
	audit_id AS audit_id1, 
	eff_from_date AS eff_from_date1, 
	eff_to_date AS eff_to_date1, 
	source_sys_id AS source_sys_id1, 
	created_date AS created_date1, 
	modified_date AS modified_date1, 
	insert_update_flag AS insert_update_flag1, 
	lkp_claim_occurrence_Calc_id AS lkp_claim_occurrence_Calc_id1
	FROM RTR_insert_update_closed_Insert
),
claim_occurrence_calculation_insert_closed_no_fin AS (
	INSERT INTO claim_occurrence_calculation
	(claim_occurrence_calculation_ak_id, claim_occurrence_ak_id, claim_occurrence_status_code, claim_occurrence_date, claim_occurrence_date_type, claim_occurrence_reported_date, claim_supplemental_ind, claim_financial_ind, claim_recovery_ind, claim_notice_only_ind, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	SEQ_claim_occ_calc_ak_id1.NEXTVAL AS CLAIM_OCCURRENCE_CALCULATION_AK_ID, 
	claim_occurrence_ak_id1 AS CLAIM_OCCURRENCE_AK_ID, 
	claim_occurrence_status_code_out1 AS CLAIM_OCCURRENCE_STATUS_CODE, 
	s3p_claim_created_date1 AS CLAIM_OCCURRENCE_DATE, 
	claim_occurrence_date_type_out1 AS CLAIM_OCCURRENCE_DATE_TYPE, 
	s3p_claim_created_date1 AS CLAIM_OCCURRENCE_REPORTED_DATE, 
	claim_supplemental_ind1 AS CLAIM_SUPPLEMENTAL_IND, 
	claim_financial_ind1 AS CLAIM_FINANCIAL_IND, 
	claim_recovery_ind1 AS CLAIM_RECOVERY_IND, 
	claim_notice_only_ind1 AS CLAIM_NOTICE_ONLY_IND, 
	crrnt_snpsht_flag1 AS CRRNT_SNPSHT_FLAG, 
	audit_id1 AS AUDIT_ID, 
	s3p_claim_created_date1 AS EFF_FROM_DATE, 
	eff_to_date1 AS EFF_TO_DATE, 
	source_sys_id1 AS SOURCE_SYS_ID, 
	created_date1 AS CREATED_DATE, 
	modified_date1 AS MODIFIED_DATE
	FROM UPD_insert_closed_no_fin
),
SQ_claim_occurrence_openedinerror AS (
	SELECT claim_occurrence.claim_occurrence_ak_id, claim_occurrence.source_claim_occurrence_status_code, claim_occurrence.notice_claim_ind, claim_occurrence.s3p_claim_created_date, claim_occurrence.s3p_claim_updated_date, claim_occurrence.source_sys_id 
	FROM
	 claim_occurrence
	WHERE claim_occurrence_ak_id IN
	(select claim_occurrence_ak_id from claim_occurrence_calculation group by claim_occurrence_ak_id having count(*) = 1 and MAX(claim_occurrence_date_type) = '7OPEN\NOFINANCIAL')
	AND notice_claim_ind != 'N' AND source_claim_occurrence_status_code = 'C'
	AND claim_occurrence.source_sys_id = 'EXCEED'
),
EXP_Values AS (
	SELECT
	claim_occurrence_ak_id,
	source_claim_occurrence_status_code AS s3p_claim_occurrence_status_code,
	notice_claim_ind AS s3p_not_claim_ind,
	s3p_claim_created_date,
	s3p_claim_updated_date,
	'N' AS indicators,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS') 
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	'8CLOSED\NOFINANCIAL' AS claim_occurrence_date_type_out_closed,
	source_sys_id
	FROM SQ_claim_occurrence_openedinerror
),
claim_occurrence_calculation_Insert_Only_Closed_no_fin AS (
	INSERT INTO claim_occurrence_calculation
	(claim_occurrence_calculation_ak_id, claim_occurrence_ak_id, claim_occurrence_status_code, claim_occurrence_date, claim_occurrence_date_type, claim_occurrence_reported_date, claim_supplemental_ind, claim_financial_ind, claim_recovery_ind, claim_notice_only_ind, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	SEQ_claim_occ_calc_ak_id1.NEXTVAL AS CLAIM_OCCURRENCE_CALCULATION_AK_ID, 
	CLAIM_OCCURRENCE_AK_ID, 
	s3p_claim_occurrence_status_code AS CLAIM_OCCURRENCE_STATUS_CODE, 
	s3p_claim_created_date AS CLAIM_OCCURRENCE_DATE, 
	claim_occurrence_date_type_out_closed AS CLAIM_OCCURRENCE_DATE_TYPE, 
	s3p_claim_created_date AS CLAIM_OCCURRENCE_REPORTED_DATE, 
	indicators AS CLAIM_SUPPLEMENTAL_IND, 
	indicators AS CLAIM_FINANCIAL_IND, 
	indicators AS CLAIM_RECOVERY_IND, 
	indicators AS CLAIM_NOTICE_ONLY_IND, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	s3p_claim_created_date AS EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM EXP_Values
),
SQ_claim_occurrence_calculation_update_crrnt_snpsht_flag AS (
	SELECT a.claim_occurrence_calculation_id, a.claim_occurrence_ak_id, a.eff_from_date, a.eff_to_date, a.source_sys_id 
	FROM
	 claim_occurrence_calculation a WHERE EXISTS
	(
	SELECT 1 from @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence_calculation b
	WHERE b.crrnt_snpsht_flag = 1
	and a.claim_occurrence_ak_id = b.claim_occurrence_ak_id
	and a.source_sys_id  = b.source_sys_id 
	GROUP BY b.claim_occurrence_ak_id, b.source_sys_id 
	HAVING COUNT(*) > 1
	)
	ORDER BY a.claim_occurrence_ak_id, a.source_sys_id,  a.eff_from_date  DESC, a.claim_occurrence_calculation_ak_id DESC
	
	-- In the order by clause we added claim_occurrence_calculation_ak_id  DESC ,because say a claim has staus order of 
	-- '4REOPEN',
	-- '5CLOSEDAFTERREOPEN',
	-- '4REOPEN' on same day for PMS data , then the latest row with '4REOPEN' status should have a crrnt_snpsht_flag value of  1.
	--PL (7/16/2009): the order by logic was intially based on "date type" but, the logic was changed to use claim_occurrence_calculation_ak_id later becasue we started allowing for multiple reopens & closed after reopens on the same day. There used to be a physical AK id constraint that was removed and the order by condition was changed.
),
EXP_Expire_Rows AS (
	SELECT
	claim_occurrence_calculation_id,
	claim_occurrence_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	source_sys_id,
	-- *INF*: DECODE (TRUE, claim_occurrence_ak_id = v_PREV_ROW_claim_party_occurrence_ak_id and source_sys_id = v_PREV_ROW_source_sys_id , ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(
	    TRUE,
	    claim_occurrence_ak_id = v_PREV_ROW_claim_party_occurrence_ak_id and source_sys_id = v_PREV_ROW_source_sys_id, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	claim_occurrence_ak_id AS v_PREV_ROW_claim_party_occurrence_ak_id,
	source_sys_id AS v_PREV_ROW_source_sys_id,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	0 AS crrnt_Snpsht_flag,
	sysdate AS modified_date
	FROM SQ_claim_occurrence_calculation_update_crrnt_snpsht_flag
),
FLT_Claimant_cov_dtl_calc_Upd AS (
	SELECT
	claim_occurrence_calculation_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_Snpsht_flag, 
	modified_date
	FROM EXP_Expire_Rows
	WHERE orig_eff_to_date != eff_to_date
),
UPD_Claim_Occurrence_Calc AS (
	SELECT
	claim_occurrence_calculation_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_Snpsht_flag, 
	modified_date
	FROM FLT_Claimant_cov_dtl_calc_Upd
),
claim_occurrence_calculation_update_crrnt_snpsht_flag AS (
	MERGE INTO claim_occurrence_calculation AS T
	USING UPD_Claim_Occurrence_Calc AS S
	ON T.claim_occurrence_calculation_id = S.claim_occurrence_calculation_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_Snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),
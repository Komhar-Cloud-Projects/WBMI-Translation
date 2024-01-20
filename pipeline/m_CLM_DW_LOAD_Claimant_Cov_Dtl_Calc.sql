WITH
LKP_claim_transaction AS (
	SELECT
	claimant_cov_det_ak_id,
	IN_claimant_cov_det_ak_id,
	IN_trans_date,
	trans_code,
	trans_date
	FROM (
		SELECT claim_transaction.trans_code as trans_code, claim_transaction.claimant_cov_det_ak_id as claimant_cov_det_ak_id,  claim_transaction.trans_date as trans_date FROM claim_transaction
		where claim_transaction.trans_code in('24', '34')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,trans_date ORDER BY claimant_cov_det_ak_id) = 1
),
SQ_claimant_coverage_detail_reserve_calculation AS (
	SELECT a.claimant_cov_det_ak_id, a.financial_type_code, a.reserve_date, a.reserve_date_type, a.source_sys_id 
	FROM
	 claimant_coverage_detail_reserve_calculation a WHERE a.claimant_cov_det_ak_id IN
	(SELECT b.claimant_cov_det_ak_id 
	FROM claimant_coverage_detail_reserve_calculation b WHERE b.created_date >= '@{pipeline().parameters.SELECTION_START_TS}' ) 
	ORDER BY a.claimant_cov_det_ak_id, a.reserve_date,a.reserve_date_type
),
EXP_get_values AS (
	SELECT
	claimant_cov_det_ak_id,
	financial_type_code,
	reserve_date,
	reserve_date_type,
	-- *INF*: IIF(claimant_cov_det_ak_id = v_prev_row_claimant_cov_det_ak_id , 'OLD', 'NEW')
	IFF(claimant_cov_det_ak_id = v_prev_row_claimant_cov_det_ak_id, 'OLD', 'NEW') AS v_coverage,
	-- *INF*: IIF(v_coverage = 'NEW', IIF(financial_type_code = 'D', reserve_date_type,'NA'), IIF(financial_type_code = 'D', reserve_date_type, v_claimant_cov_date_type_D_old))
	IFF(
	    v_coverage = 'NEW',
	    IFF(
	        financial_type_code = 'D', reserve_date_type, 'NA'
	    ),
	    IFF(
	        financial_type_code = 'D', reserve_date_type, v_claimant_cov_date_type_D_old
	    )
	) AS v_claimant_cov_date_type_D,
	v_claimant_cov_date_type_D AS v_claimant_cov_date_type_D_old,
	-- *INF*: IIF(v_coverage = 'NEW', IIF(financial_type_code = 'E', reserve_date_type,'NA'), IIF(financial_type_code = 'E', reserve_date_type, v_claimant_cov_date_type_E_old))
	IFF(
	    v_coverage = 'NEW',
	    IFF(
	        financial_type_code = 'E', reserve_date_type, 'NA'
	    ),
	    IFF(
	        financial_type_code = 'E', reserve_date_type, v_claimant_cov_date_type_E_old
	    )
	) AS v_claimant_cov_date_type_E,
	v_claimant_cov_date_type_E AS v_claimant_cov_date_type_E_old,
	-- *INF*: IIF(v_coverage = 'NEW', IIF(financial_type_code = 'S', reserve_date_type,'NA'), IIF(financial_type_code = 'S', reserve_date_type, v_claimant_cov_date_type_S_old))
	IFF(
	    v_coverage = 'NEW',
	    IFF(
	        financial_type_code = 'S', reserve_date_type, 'NA'
	    ),
	    IFF(
	        financial_type_code = 'S', reserve_date_type, v_claimant_cov_date_type_S_old
	    )
	) AS v_claimant_cov_date_type_S,
	v_claimant_cov_date_type_S AS v_claimant_cov_date_type_S_old,
	-- *INF*: IIF(v_coverage = 'NEW', IIF(financial_type_code = 'B', reserve_date_type,'NA'), IIF(financial_type_code = 'B', reserve_date_type, v_claimant_cov_date_type_B_old))
	IFF(
	    v_coverage = 'NEW',
	    IFF(
	        financial_type_code = 'B', reserve_date_type, 'NA'
	    ),
	    IFF(
	        financial_type_code = 'B', reserve_date_type, v_claimant_cov_date_type_B_old
	    )
	) AS v_claimant_cov_date_type_B,
	v_claimant_cov_date_type_B AS v_claimant_cov_date_type_B_old,
	-- *INF*: IIF(v_coverage = 'NEW', IIF(financial_type_code = 'R', reserve_date_type,'NA'), IIF(financial_type_code = 'R', reserve_date_type, v_claimant_cov_date_type_R_old))
	IFF(
	    v_coverage = 'NEW',
	    IFF(
	        financial_type_code = 'R', reserve_date_type, 'NA'
	    ),
	    IFF(
	        financial_type_code = 'R', reserve_date_type, v_claimant_cov_date_type_R_old
	    )
	) AS v_claimant_cov_date_type_R,
	v_claimant_cov_date_type_R AS v_claimant_cov_date_type_R_old,
	-- *INF*: IIF(v_coverage = 'NEW', IIF(financial_type_code = 'R', 'Y','N'), IIF(financial_type_code = 'R', 'Y', v_claimant_cov_recovery_ind_old))
	IFF(
	    v_coverage = 'NEW', IFF(
	        financial_type_code = 'R', 'Y', 'N'
	    ),
	    IFF(
	        financial_type_code = 'R', 'Y', v_claimant_cov_recovery_ind_old
	    )
	) AS v_claimant_cov_recovery_ind,
	v_claimant_cov_recovery_ind AS v_claimant_cov_recovery_ind_old,
	v_claimant_cov_recovery_ind AS claimant_cov_recovery_ind_out,
	-- *INF*: IIF(
	-- (ISNULL(v_claimant_cov_date_type_D) OR v_claimant_cov_date_type_D = 'NA' OR v_claimant_cov_date_type_D = '1NOTICEONLY' )
	-- AND(ISNULL(v_claimant_cov_date_type_E) OR v_claimant_cov_date_type_E = 'NA' OR v_claimant_cov_date_type_E = '1NOTICEONLY') 
	-- AND (ISNULL(v_claimant_cov_date_type_S) OR v_claimant_cov_date_type_S = 'NA' OR v_claimant_cov_date_type_S = '1NOTICEONLY') 
	-- AND (ISNULL(v_claimant_cov_date_type_B) 
	-- OR v_claimant_cov_date_type_B = 'NA' OR v_claimant_cov_date_type_B = '1NOTICEONLY') 
	-- AND (ISNULL(v_claimant_cov_date_type_R) 
	-- OR v_claimant_cov_date_type_R = 'NA' OR v_claimant_cov_date_type_R = '1NOTICEONLY'), '1NOTICEONLY', 
	-- 
	-- IIF(v_claimant_cov_date_type_D = '2OPEN' OR v_claimant_cov_date_type_D = '4REOPEN' OR v_claimant_cov_date_type_E = '2OPEN' OR v_claimant_cov_date_type_E = '4REOPEN' OR v_claimant_cov_date_type_S = '2OPEN' OR v_claimant_cov_date_type_S = '4REOPEN' OR v_claimant_cov_date_type_B = '2OPEN' OR v_claimant_cov_date_type_B = '4REOPEN' OR v_claimant_cov_date_type_R = '2OPEN' OR v_claimant_cov_date_type_R = '4REOPEN', '2OPEN',
	-- 
	-- '3CLOSED'))
	IFF(
	    (v_claimant_cov_date_type_D IS NULL
	    or v_claimant_cov_date_type_D = 'NA'
	    or v_claimant_cov_date_type_D = '1NOTICEONLY')
	    and (v_claimant_cov_date_type_E IS NULL
	    or v_claimant_cov_date_type_E = 'NA'
	    or v_claimant_cov_date_type_E = '1NOTICEONLY')
	    and (v_claimant_cov_date_type_S IS NULL
	    or v_claimant_cov_date_type_S = 'NA'
	    or v_claimant_cov_date_type_S = '1NOTICEONLY')
	    and (v_claimant_cov_date_type_B IS NULL
	    or v_claimant_cov_date_type_B = 'NA'
	    or v_claimant_cov_date_type_B = '1NOTICEONLY')
	    and (v_claimant_cov_date_type_R IS NULL
	    or v_claimant_cov_date_type_R = 'NA'
	    or v_claimant_cov_date_type_R = '1NOTICEONLY'),
	    '1NOTICEONLY',
	    IFF(
	        v_claimant_cov_date_type_D = '2OPEN'
	        or v_claimant_cov_date_type_D = '4REOPEN'
	        or v_claimant_cov_date_type_E = '2OPEN'
	        or v_claimant_cov_date_type_E = '4REOPEN'
	        or v_claimant_cov_date_type_S = '2OPEN'
	        or v_claimant_cov_date_type_S = '4REOPEN'
	        or v_claimant_cov_date_type_B = '2OPEN'
	        or v_claimant_cov_date_type_B = '4REOPEN'
	        or v_claimant_cov_date_type_R = '2OPEN'
	        or v_claimant_cov_date_type_R = '4REOPEN',
	        '2OPEN',
	        '3CLOSED'
	    )
	) AS v_overall_claimant_cov_date_type_crrnt,
	-- *INF*: IIF(v_overall_claimant_cov_date_type_crrnt = '1NOTICEONLY', '1NOTICEONLY', IIF(v_overall_claimant_cov_date_type_crrnt = '2OPEN', IIF(IN(v_claimant_cov_date_type_out_old,  '3CLOSED', '5CLOSEDAFTERREOPEN', '4REOPEN')  AND v_coverage = 'OLD', '4REOPEN', '2OPEN'), IIF(v_overall_claimant_cov_date_type_crrnt = '3CLOSED', IIF(IN(v_claimant_cov_date_type_out_old , '4REOPEN', '5CLOSEDAFTERREOPEN') AND  v_coverage = 'OLD', '5CLOSEDAFTERREOPEN', '3CLOSED'))))
	-- 
	-- --IIF(v_overall_claimant_cov_date_type_crrnt = 'NOTICEONLY', 'NOTICEONLY', IIF(v_overall_claimant_cov_date_type_crrnt = 'OPEN', IIF(v_overall_claimant_cov_date_type_old = 'CLOSED' AND v_coverage = 'OLD', 'REOPEN', 'OPEN'), IIF(v_overall_claimant_cov_date_type_crrnt = 'CLOSED', 'CLOSED')))
	IFF(
	    v_overall_claimant_cov_date_type_crrnt = '1NOTICEONLY', '1NOTICEONLY',
	    IFF(
	        v_overall_claimant_cov_date_type_crrnt = '2OPEN',
	        IFF(
	            v_claimant_cov_date_type_out_old IN ('3CLOSED','5CLOSEDAFTERREOPEN','4REOPEN')
	            and v_coverage = 'OLD',
	            '4REOPEN',
	            '2OPEN'
	        ),
	        IFF(
	            v_overall_claimant_cov_date_type_crrnt = '3CLOSED',
	            IFF(
	                v_claimant_cov_date_type_out_old IN ('4REOPEN','5CLOSEDAFTERREOPEN')
	                and v_coverage = 'OLD',
	                '5CLOSEDAFTERREOPEN',
	                '3CLOSED'
	            )
	        )
	    )
	) AS v_claimant_cov_date_type_out,
	-- *INF*: IIF(v_coverage = 'NEW', 'INSERT', IIF(v_claimant_cov_date_type_out = v_claimant_cov_date_type_out_old, 'NOCHANGE', 'INSERT'))
	IFF(
	    v_coverage = 'NEW', 'INSERT',
	    IFF(
	        v_claimant_cov_date_type_out = v_claimant_cov_date_type_out_old, 'NOCHANGE',
	        'INSERT'
	    )
	) AS v_insert_flag,
	v_insert_flag AS insert_flag_out,
	v_claimant_cov_date_type_out AS v_claimant_cov_date_type_out_old,
	v_claimant_cov_date_type_out AS claimant_cov_date_type_out,
	claimant_cov_det_ak_id AS v_prev_row_claimant_cov_det_ak_id,
	reserve_date_type AS v_prev_row_reserve_date_type,
	financial_type_code AS v_prev_row_financial_type_code,
	source_sys_id
	FROM SQ_claimant_coverage_detail_reserve_calculation
),
FIL_remove_non_inserts AS (
	SELECT
	insert_flag_out, 
	claimant_cov_det_ak_id, 
	reserve_date, 
	reserve_date_type, 
	claimant_cov_cause_of_loss, 
	claimant_cov_reserve_ctgry, 
	claimant_cov_date_type_out, 
	source_sys_id, 
	claimant_cov_recovery_ind_out
	FROM EXP_get_values
	WHERE insert_flag_out = 'INSERT'
),
LKP_claimant_cov_dtl_calc AS (
	SELECT
	IN_reserve_date,
	IN_claimant_cov_date_type,
	claimant_cov_det_calculation_id,
	claimant_cov_det_ak_id,
	claimant_cov_date,
	claimant_cov_date_type
	FROM (
		SELECT 
			IN_reserve_date,
			IN_claimant_cov_date_type,
			claimant_cov_det_calculation_id,
			claimant_cov_det_ak_id,
			claimant_cov_date,
			claimant_cov_date_type
		FROM claimant_coverage_detail_calculation
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,claimant_cov_date,claimant_cov_date_type ORDER BY IN_reserve_date) = 1
),
FIL_existing_records AS (
	SELECT
	LKP_claimant_cov_dtl_calc.claimant_cov_det_calculation_id AS lkp_claimant_cov_det_calculation_id, 
	FIL_remove_non_inserts.claimant_cov_det_ak_id, 
	FIL_remove_non_inserts.reserve_date, 
	FIL_remove_non_inserts.reserve_date_type, 
	FIL_remove_non_inserts.claimant_cov_cause_of_loss, 
	FIL_remove_non_inserts.claimant_cov_reserve_ctgry, 
	FIL_remove_non_inserts.claimant_cov_date_type_out, 
	FIL_remove_non_inserts.source_sys_id, 
	FIL_remove_non_inserts.claimant_cov_recovery_ind_out
	FROM FIL_remove_non_inserts
	LEFT JOIN LKP_claimant_cov_dtl_calc
	ON LKP_claimant_cov_dtl_calc.claimant_cov_det_ak_id = FIL_remove_non_inserts.claimant_cov_det_ak_id AND LKP_claimant_cov_dtl_calc.claimant_cov_date = FIL_remove_non_inserts.reserve_date AND LKP_claimant_cov_dtl_calc.claimant_cov_date_type = FIL_remove_non_inserts.claimant_cov_date_type_out
	WHERE IIF(ISNULL(lkp_claimant_cov_det_calculation_id), TRUE, FALSE)
),
EXP_calculate_values AS (
	SELECT
	claimant_cov_det_ak_id,
	reserve_date,
	reserve_date_type,
	claimant_cov_cause_of_loss,
	claimant_cov_reserve_ctgry,
	claimant_cov_date_type_out,
	-- *INF*: DECODE(claimant_cov_date_type_out, '1NOTICEONLY', 'N', '2OPEN', 'O', '3CLOSED', 'C', '4REOPEN', 'O', '5CLOSEDAFTERREOPEN', 'C')
	DECODE(
	    claimant_cov_date_type_out,
	    '1NOTICEONLY', 'N',
	    '2OPEN', 'O',
	    '3CLOSED', 'C',
	    '4REOPEN', 'O',
	    '5CLOSEDAFTERREOPEN', 'C'
	) AS claimant_cov_status_code_out,
	-- *INF*: IIF(claimant_cov_date_type_out =  '1NOTICEONLY', 'Y', 'N')
	IFF(claimant_cov_date_type_out = '1NOTICEONLY', 'Y', 'N') AS claimant_cov_ntice_only_indicator,
	claimant_cov_recovery_ind_out,
	-- *INF*: IIF(ISNULL(:LKP.LKP_CLAIM_TRANSACTION(claimant_cov_det_ak_id, reserve_date)), 'N', 'Y')
	IFF(
	    LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_reserve_date.claimant_cov_det_ak_id IS NULL,
	    'N',
	    'Y'
	) AS claimant_cov_supplemental_ind_out,
	-- *INF*: IIF(claimant_cov_date_type_out =  '1NOTICEONLY', 'N', 'Y')
	IFF(claimant_cov_date_type_out = '1NOTICEONLY', 'N', 'Y') AS claimant_cov_financial_indicator,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	reserve_date AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS') 
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM FIL_existing_records
	LEFT JOIN LKP_CLAIM_TRANSACTION LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_reserve_date
	ON LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_reserve_date.claimant_cov_det_ak_id = claimant_cov_det_ak_id
	AND LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_reserve_date.trans_date = reserve_date

),
SEQ_Claimant_Cov_Detail_Calc_AK_ID AS (
	CREATE SEQUENCE SEQ_Claimant_Cov_Detail_Calc_AK_ID
	START = 0
	INCREMENT = 1;
),
claimant_coverage_detail_calculation_insert AS (
	INSERT INTO claimant_coverage_detail_calculation
	(claimant_cov_det_calculation_ak_id, claimant_cov_det_ak_id, claimant_cov_status_code, claimant_cov_date, claimant_cov_date_type, claimant_cov_supplemental_ind, claimant_cov_financial_ind, claimant_cov_recovery_ind, claimant_cov_notice_only_ind, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	SEQ_Claimant_Cov_Detail_Calc_AK_ID.NEXTVAL AS CLAIMANT_COV_DET_CALCULATION_AK_ID, 
	CLAIMANT_COV_DET_AK_ID, 
	claimant_cov_status_code_out AS CLAIMANT_COV_STATUS_CODE, 
	reserve_date AS CLAIMANT_COV_DATE, 
	claimant_cov_date_type_out AS CLAIMANT_COV_DATE_TYPE, 
	claimant_cov_supplemental_ind_out AS CLAIMANT_COV_SUPPLEMENTAL_IND, 
	claimant_cov_financial_indicator AS CLAIMANT_COV_FINANCIAL_IND, 
	claimant_cov_recovery_ind_out AS CLAIMANT_COV_RECOVERY_IND, 
	claimant_cov_ntice_only_indicator AS CLAIMANT_COV_NOTICE_ONLY_IND, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	reserve_date AS EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM EXP_calculate_values
),
SQ_claimant_coverage_detail_calculation_update AS (
	SELECT a.claimant_cov_det_calculation_id, a.claimant_cov_det_ak_id,  a.eff_from_date, a.eff_to_date, a.source_sys_id FROM
	 claimant_coverage_detail_calculation a
	WHERE EXISTS
	(SELECT 1			
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_detail_calculation b
	WHERE b.crrnt_snpsht_flag = 1
	AND a.claimant_cov_det_ak_id = b.claimant_cov_det_ak_id
	and a.source_sys_id = b.source_sys_id
	GROUP BY b.claimant_cov_det_ak_id,  b.source_sys_id
		HAVING COUNT(*) > 1)
	ORDER BY a.claimant_cov_det_ak_id, a.source_sys_id,  a.eff_from_date  DESC, a.claimant_cov_det_calculation_ak_id DESC
	
	
	-- In the order by clause we added claimant_cov_det_calculation_ak_id DESC ,because say a coverage has staus order of 
	-- '4REOPEN',
	-- '5CLOSEDAFTERREOPEN',
	-- '4REOPEN' on same day for PMS data , then the latest row with '4REOPEN' status should have a crrnt_snpsht_flag value of  1.
),
EXP_Expire_Rows AS (
	SELECT
	claimant_cov_det_calculation_id,
	claimant_cov_det_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	source_sys_id,
	-- *INF*: DECODE (TRUE, claimant_cov_det_ak_id = v_PREV_ROW_claimant_cov_det_ak_id and source_sys_id = v_PREV_ROW_source_sys_id, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(
	    TRUE,
	    claimant_cov_det_ak_id = v_PREV_ROW_claimant_cov_det_ak_id and source_sys_id = v_PREV_ROW_source_sys_id, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	claimant_cov_det_ak_id AS v_PREV_ROW_claimant_cov_det_ak_id,
	source_sys_id AS v_PREV_ROW_source_sys_id,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	0 AS crrnt_Snpsht_flag,
	sysdate AS modified_date
	FROM SQ_claimant_coverage_detail_calculation_update
),
FLT_Claimant_cov_dtl_calc_Upd AS (
	SELECT
	claimant_cov_det_calculation_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_Snpsht_flag, 
	modified_date
	FROM EXP_Expire_Rows
	WHERE orig_eff_to_date != eff_to_date
),
UPD_Claimant_cov_dtl_calc AS (
	SELECT
	claimant_cov_det_calculation_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_Snpsht_flag, 
	modified_date
	FROM FLT_Claimant_cov_dtl_calc_Upd
),
claimant_coverage_detail_calculation_update AS (
	MERGE INTO claimant_coverage_detail_calculation AS T
	USING UPD_Claimant_cov_dtl_calc AS S
	ON T.claimant_cov_det_calculation_id = S.claimant_cov_det_calculation_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_Snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),
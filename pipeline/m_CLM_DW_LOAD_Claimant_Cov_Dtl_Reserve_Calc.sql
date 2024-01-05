WITH
LKP_Claimant_cov_dtl_rsrv_calc_open_closed AS (
	SELECT
	claimant_cov_det_reserve_calculation_id,
	IN_claimant_cov_det_ak_id,
	IN_financial_type_code,
	IN_reserve_date_type,
	claimant_cov_det_ak_id,
	financial_type_code,
	reserve_date_type
	FROM (
		SELECT 
			claimant_cov_det_reserve_calculation_id,
			IN_claimant_cov_det_ak_id,
			IN_financial_type_code,
			IN_reserve_date_type,
			claimant_cov_det_ak_id,
			financial_type_code,
			reserve_date_type
		FROM claimant_coverage_detail_reserve_calculation
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,financial_type_code,reserve_date_type ORDER BY claimant_cov_det_reserve_calculation_id) = 1
),
LKP_claimant_cov_dtl_rsrv_calc AS (
	SELECT
	claimant_cov_det_reserve_calculation_id,
	IN_claimant_cov_det_ak_id,
	IN_financial_type_code,
	IN_reserve_date,
	IN_reserve_date_type,
	claimant_cov_det_ak_id,
	financial_type_code,
	reserve_date,
	reserve_date_type
	FROM (
		SELECT 
			claimant_cov_det_reserve_calculation_id,
			IN_claimant_cov_det_ak_id,
			IN_financial_type_code,
			IN_reserve_date,
			IN_reserve_date_type,
			claimant_cov_det_ak_id,
			financial_type_code,
			reserve_date,
			reserve_date_type
		FROM claimant_coverage_detail_reserve_calculation
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,financial_type_code,reserve_date,reserve_date_type ORDER BY claimant_cov_det_reserve_calculation_id) = 1
),
SQ_claim_transaction AS (
	SELECT claim_transaction.claimant_cov_det_ak_id, claim_transaction.cause_of_loss, claim_transaction.reserve_ctgry, claim_transaction.financial_type_code, claim_transaction.trans_date, claim_transaction.trans_code, claim_transaction.source_sys_id 
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction claim_transaction 
	WHERE claim_transaction.modified_date > = '@{pipeline().parameters.SELECTION_START_TS}'
	AND claim_transaction.trans_offset_onset_ind  in ('N','N/A')
	ORDER BY
	 claim_transaction.claimant_cov_det_ak_id, claim_transaction.cause_of_loss, claim_transaction.reserve_ctgry, claim_transaction.financial_type_code, claim_transaction.trans_date
),
RTR_Grp_Fin_Typ_Cds AS (
	SELECT
	claimant_cov_det_ak_id,
	financial_type_code,
	cause_of_loss,
	reserve_ctgry,
	trans_code,
	trans_date,
	source_sys_id
	FROM SQ_claim_transaction
),
RTR_Grp_Fin_Typ_Cds_Grp_Open AS (SELECT * FROM RTR_Grp_Fin_Typ_Cds WHERE IN(trans_code, '90','91','92','23','20','33','30','22','40', '66')),
RTR_Grp_Fin_Typ_Cds_Grp_Reopen AS (SELECT * FROM RTR_Grp_Fin_Typ_Cds WHERE (
(source_sys_id = 'PMS' AND financial_type_code = 'D' AND IN(trans_code, '66', '23') ) OR 
(source_sys_id = 'PMS' AND IN(financial_type_code, 'E', 'S', 'B', 'R') AND 
trans_code = '92') OR 
(source_sys_id = 'EXCEED' AND IN(financial_type_code, 'D', 'E', 'S', 'B', 'R') 
AND trans_code = '66')
)),
RTR_Grp_Fin_Typ_Cds_Grp_NoticeOnly AS (SELECT * FROM RTR_Grp_Fin_Typ_Cds WHERE trans_code = '43'),
RTR_Grp_Fin_Typ_Cds_Grp_Closed AS (SELECT * FROM RTR_Grp_Fin_Typ_Cds WHERE IN(trans_code, '22','23','40','41','42','32','33','30', '20')),
RTR_Grp_Fin_Typ_Cds_Grp_Reclosed AS (SELECT * FROM RTR_Grp_Fin_Typ_Cds WHERE (
(source_sys_id = 'PMS' AND financial_type_code = 'D' AND IN(trans_code, '22', '40','41', '42', '23') ) OR 
(source_sys_id = 'PMS' AND financial_type_code = 'E' AND IN(trans_code, '22', '40','41', '42')) OR 
(source_sys_id = 'PMS' AND IN(financial_type_code, 'S', 'B', 'R') AND IN(trans_code, '32', '40', '41', '42', '30'))
OR(source_sys_id = 'EXCEED' AND IN(financial_type_code, 'D', 'E') AND IN(trans_code, '22', '40','41', '42'))
OR (source_sys_id = 'EXCEED' AND IN(financial_type_code, 'S', 'B', 'R') AND IN(trans_code, '32', '40', '41', '42', '30'))
)



--IN(trans_code, '22', '40', '41', '42', '32', '30')),
EXP_Set_values_reopen AS (
	SELECT
	trans_date AS reserve_date,
	claimant_cov_det_ak_id,
	financial_type_code,
	cause_of_loss,
	'4REOPEN' AS reserve_date_type,
	reserve_ctgry,
	source_sys_id,
	'O' AS financial_type_status_code,
	trans_code,
	-- *INF*: :LKP.LKP_CLAIMANT_COV_DTL_RSRV_CALC(claimant_cov_det_ak_id,  financial_type_code, reserve_date, '4REOPEN')
	LKP_CLAIMANT_COV_DTL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_4REOPEN.claimant_cov_det_reserve_calculation_id AS lkp_claimant_cov_det_reserve_calculation_id,
	1 AS logical_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	reserve_date AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	to_date('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM RTR_Grp_Fin_Typ_Cds_Grp_Reopen
	LEFT JOIN LKP_CLAIMANT_COV_DTL_RSRV_CALC LKP_CLAIMANT_COV_DTL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_4REOPEN
	ON LKP_CLAIMANT_COV_DTL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_4REOPEN.claimant_cov_det_ak_id = claimant_cov_det_ak_id
	AND LKP_CLAIMANT_COV_DTL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_4REOPEN.financial_type_code = financial_type_code
	AND LKP_CLAIMANT_COV_DTL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_4REOPEN.reserve_date = reserve_date
	AND LKP_CLAIMANT_COV_DTL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_4REOPEN.reserve_date_type = '4REOPEN'

),
FIL_Remove_existing_Reopens AS (
	SELECT
	reserve_date, 
	claimant_cov_det_ak_id, 
	financial_type_code, 
	cause_of_loss, 
	reserve_date_type, 
	reserve_ctgry, 
	source_sys_id, 
	trans_code3_reopen, 
	financial_type_status_code, 
	trans_code, 
	lkp_claimant_cov_det_reserve_calculation_id, 
	logical_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	created_date, 
	modified_date
	FROM EXP_Set_values_reopen
	WHERE IIF(ISNULL(lkp_claimant_cov_det_reserve_calculation_id), TRUE, FALSE)
),
AGG_remove_duplicates_reopen AS (
	SELECT
	claimant_cov_det_ak_id, 
	cause_of_loss, 
	financial_type_code, 
	reserve_date, 
	reserve_ctgry, 
	reserve_date_type, 
	source_sys_id, 
	trans_code3_reopen, 
	financial_type_status_code, 
	logical_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	created_date, 
	modified_date
	FROM FIL_Remove_existing_Reopens
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id, financial_type_code, reserve_date ORDER BY NULL) = 1
),
SEQ_Clmt_Cov_Dtl_Rsrv_Calc_Open_Close_AK_ID AS (
	CREATE SEQUENCE SEQ_Clmt_Cov_Dtl_Rsrv_Calc_Open_Close_AK_ID
	START = 0
	INCREMENT = 1;
),
clmt_cov_dtl_rsrv_calc_reopen AS (
	INSERT INTO claimant_coverage_detail_reserve_calculation
	(claimant_cov_det_reserve_calculation_ak_id, claimant_cov_det_ak_id, financial_type_code, reserve_date, reserve_date_type, financial_type_status_code, logical_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	SEQ_Clmt_Cov_Dtl_Rsrv_Calc_Open_Close_AK_ID.NEXTVAL AS CLAIMANT_COV_DET_RESERVE_CALCULATION_AK_ID, 
	CLAIMANT_COV_DET_AK_ID, 
	FINANCIAL_TYPE_CODE, 
	RESERVE_DATE, 
	RESERVE_DATE_TYPE, 
	FINANCIAL_TYPE_STATUS_CODE, 
	LOGICAL_FLAG, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM AGG_remove_duplicates_reopen
),
EXP_Set_values_Reclosed AS (
	SELECT
	claimant_cov_det_ak_id,
	financial_type_code,
	cause_of_loss,
	trans_code,
	trans_date,
	reserve_ctgry,
	source_sys_id,
	'5CLOSEDAFTERREOPEN' AS reserve_date_type,
	'C' AS financial_type_status_code,
	-- *INF*: IIF(IN(trans_code, '40', '30'), 100, 1)
	IFF(IN(trans_code, '40', '30'), 100, 1) AS logical_flag,
	-- *INF*: :LKP.LKP_CLAIMANT_COV_DTL_RSRV_CALC(claimant_cov_det_ak_id,  financial_type_code, trans_date, '5CLOSEDAFTERREOPEN')
	LKP_CLAIMANT_COV_DTL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_trans_date_5CLOSEDAFTERREOPEN.claimant_cov_det_reserve_calculation_id AS lkp_claimant_cov_det_reserve_calculation_id,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	trans_date AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	to_date('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM RTR_Grp_Fin_Typ_Cds_Grp_Reclosed
	LEFT JOIN LKP_CLAIMANT_COV_DTL_RSRV_CALC LKP_CLAIMANT_COV_DTL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_trans_date_5CLOSEDAFTERREOPEN
	ON LKP_CLAIMANT_COV_DTL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_trans_date_5CLOSEDAFTERREOPEN.claimant_cov_det_ak_id = claimant_cov_det_ak_id
	AND LKP_CLAIMANT_COV_DTL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_trans_date_5CLOSEDAFTERREOPEN.financial_type_code = financial_type_code
	AND LKP_CLAIMANT_COV_DTL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_trans_date_5CLOSEDAFTERREOPEN.reserve_date = trans_date
	AND LKP_CLAIMANT_COV_DTL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_trans_date_5CLOSEDAFTERREOPEN.reserve_date_type = '5CLOSEDAFTERREOPEN'

),
FIL_Remove_existing_reclosed AS (
	SELECT
	claimant_cov_det_ak_id, 
	financial_type_code, 
	cause_of_loss, 
	trans_code, 
	trans_date, 
	reserve_ctgry, 
	source_sys_id, 
	reserve_date_type, 
	financial_type_status_code, 
	logical_flag, 
	lkp_claimant_cov_det_reserve_calculation_id, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	created_date, 
	modified_date
	FROM EXP_Set_values_Reclosed
	WHERE IIF(ISNULL(lkp_claimant_cov_det_reserve_calculation_id), TRUE, FALSE)
),
AGG_remove_duplicates_reclosed AS (
	SELECT
	claimant_cov_det_ak_id, 
	cause_of_loss, 
	financial_type_code, 
	trans_date, 
	reserve_ctgry, 
	trans_code, 
	source_sys_id, 
	reserve_date_type, 
	financial_type_status_code, 
	logical_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	created_date, 
	modified_date
	FROM FIL_Remove_existing_reclosed
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id, financial_type_code, trans_date ORDER BY NULL) = 1
),
clmt_cov_dtl_rsrv_calc_reclosed AS (
	INSERT INTO claimant_coverage_detail_reserve_calculation
	(claimant_cov_det_reserve_calculation_ak_id, claimant_cov_det_ak_id, financial_type_code, reserve_date, reserve_date_type, financial_type_status_code, logical_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	SEQ_Clmt_Cov_Dtl_Rsrv_Calc_Open_Close_AK_ID.NEXTVAL AS CLAIMANT_COV_DET_RESERVE_CALCULATION_AK_ID, 
	CLAIMANT_COV_DET_AK_ID, 
	FINANCIAL_TYPE_CODE, 
	trans_date AS RESERVE_DATE, 
	RESERVE_DATE_TYPE, 
	FINANCIAL_TYPE_STATUS_CODE, 
	LOGICAL_FLAG, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM AGG_remove_duplicates_reclosed
),
EXP_set_values_notice_only AS (
	SELECT
	claimant_cov_det_ak_id,
	financial_type_code,
	cause_of_loss,
	reserve_ctgry,
	trans_code,
	trans_date,
	source_sys_id,
	'1NOTICEONLY' AS reserve_date_type,
	'N' AS financial_type_status_code,
	1 AS logical_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	trans_date AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	to_date('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM RTR_Grp_Fin_Typ_Cds_Grp_NoticeOnly
),
LKP_claim_transaction AS (
	SELECT
	IN_claimant_cov_det_ak_id,
	IN_financial_type_code,
	IN_trans_date,
	claim_trans_id,
	claim_trans_ak_id,
	claimant_cov_det_ak_id,
	financial_type_code,
	trans_code,
	trans_date
	FROM (
		SELECT 
			IN_claimant_cov_det_ak_id,
			IN_financial_type_code,
			IN_trans_date,
			claim_trans_id,
			claim_trans_ak_id,
			claimant_cov_det_ak_id,
			financial_type_code,
			trans_code,
			trans_date
		FROM claim_transaction
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,financial_type_code,trans_date ORDER BY IN_claimant_cov_det_ak_id) = 1
),
FIL_Non_notice_only AS (
	SELECT
	LKP_claim_transaction.claim_trans_id, 
	EXP_set_values_notice_only.claimant_cov_det_ak_id, 
	EXP_set_values_notice_only.financial_type_code, 
	EXP_set_values_notice_only.cause_of_loss, 
	EXP_set_values_notice_only.reserve_ctgry, 
	EXP_set_values_notice_only.trans_code, 
	EXP_set_values_notice_only.trans_date, 
	EXP_set_values_notice_only.source_sys_id, 
	EXP_set_values_notice_only.reserve_date_type, 
	EXP_set_values_notice_only.financial_type_status_code, 
	EXP_set_values_notice_only.logical_flag, 
	EXP_set_values_notice_only.crrnt_snpsht_flag, 
	EXP_set_values_notice_only.audit_id, 
	EXP_set_values_notice_only.eff_from_date, 
	EXP_set_values_notice_only.eff_to_date, 
	EXP_set_values_notice_only.created_date, 
	EXP_set_values_notice_only.modified_date
	FROM EXP_set_values_notice_only
	LEFT JOIN LKP_claim_transaction
	ON LKP_claim_transaction.claimant_cov_det_ak_id = EXP_set_values_notice_only.claimant_cov_det_ak_id AND LKP_claim_transaction.financial_type_code = EXP_set_values_notice_only.financial_type_code AND LKP_claim_transaction.trans_date < EXP_set_values_notice_only.trans_date
	WHERE ISNULL(claim_trans_id)
),
EXP_find_existing_notice_only AS (
	SELECT
	claim_trans_id,
	claimant_cov_det_ak_id,
	financial_type_code,
	cause_of_loss,
	reserve_ctgry,
	trans_code,
	trans_date,
	source_sys_id,
	reserve_date_type,
	financial_type_status_code,
	logical_flag,
	-- *INF*: :LKP.LKP_CLAIMANT_COV_DTL_RSRV_CALC(claimant_cov_det_ak_id,  financial_type_code, trans_date, '1NOTICEONLY')
	LKP_CLAIMANT_COV_DTL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_trans_date_1NOTICEONLY.claimant_cov_det_reserve_calculation_id AS lkp_claimant_cov_det_reserve_calculation_id,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	created_date,
	modified_date
	FROM FIL_Non_notice_only
	LEFT JOIN LKP_CLAIMANT_COV_DTL_RSRV_CALC LKP_CLAIMANT_COV_DTL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_trans_date_1NOTICEONLY
	ON LKP_CLAIMANT_COV_DTL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_trans_date_1NOTICEONLY.claimant_cov_det_ak_id = claimant_cov_det_ak_id
	AND LKP_CLAIMANT_COV_DTL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_trans_date_1NOTICEONLY.financial_type_code = financial_type_code
	AND LKP_CLAIMANT_COV_DTL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_trans_date_1NOTICEONLY.reserve_date = trans_date
	AND LKP_CLAIMANT_COV_DTL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_trans_date_1NOTICEONLY.reserve_date_type = '1NOTICEONLY'

),
FIL_remove_existing_notice_only AS (
	SELECT
	claim_trans_id, 
	claimant_cov_det_ak_id, 
	financial_type_code, 
	cause_of_loss, 
	reserve_ctgry, 
	trans_code, 
	trans_date, 
	source_sys_id, 
	reserve_date_type, 
	financial_type_status_code, 
	logical_flag, 
	lkp_claimant_cov_det_reserve_calculation_id, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	created_date, 
	modified_date
	FROM EXP_find_existing_notice_only
	WHERE IIF(ISNULL(lkp_claimant_cov_det_reserve_calculation_id), TRUE, FALSE)
),
clmt_cov_dtl_rsrv_calc_notice_only AS (
	INSERT INTO claimant_coverage_detail_reserve_calculation
	(claimant_cov_det_reserve_calculation_ak_id, claimant_cov_det_ak_id, financial_type_code, reserve_date, reserve_date_type, financial_type_status_code, logical_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	SEQ_Clmt_Cov_Dtl_Rsrv_Calc_Open_Close_AK_ID.NEXTVAL AS CLAIMANT_COV_DET_RESERVE_CALCULATION_AK_ID, 
	CLAIMANT_COV_DET_AK_ID, 
	FINANCIAL_TYPE_CODE, 
	trans_date AS RESERVE_DATE, 
	RESERVE_DATE_TYPE, 
	FINANCIAL_TYPE_STATUS_CODE, 
	LOGICAL_FLAG, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_remove_existing_notice_only
),
AGG_Cov_Fin_Txn_Cd_closed AS (
	SELECT
	claimant_cov_det_ak_id, 
	cause_of_loss, 
	reserve_ctgry, 
	financial_type_code, 
	trans_code, 
	trans_date, 
	MIN(trans_date, trans_code = '22') AS trans_date_op_22, 
	MIN(trans_date, trans_code = '23') AS trans_date_op_23, 
	MIN(trans_date, trans_code = '41') AS trans_date_op_41, 
	MIN(trans_date, trans_code = '42') AS trans_date_op_42, 
	MIN(trans_date, trans_code = '32') AS trans_date_op_32, 
	MIN(trans_date, trans_code = '33') AS trans_date_op_33, 
	MIN(trans_date, trans_code = '30') AS trans_date_op_30, 
	MIN(trans_date, NOT IN(trans_code, '20', '30', '40')) AS trans_date_op, 
	MAX(trans_date, IN(trans_code, '20', '30', '40')) AS trans_date_op_20_40, 
	source_sys_id
	FROM RTR_Grp_Fin_Typ_Cds_Grp_Closed
	GROUP BY claimant_cov_det_ak_id, financial_type_code
),
EXP_Concat_Dates_Closed AS (
	SELECT
	claimant_cov_det_ak_id,
	financial_type_code,
	cause_of_loss,
	reserve_ctgry,
	trans_code,
	trans_date_op_22 AS trans_date_22_op,
	trans_date_op_23 AS trans_date_23_op,
	trans_date_op_41 AS trans_date_41_op,
	trans_date_op_42 AS trans_date_42_op,
	trans_date_op_32 AS trans_date_32_op,
	trans_date_op_33 AS trans_date_33_op,
	trans_date_op_30 AS trans_date_30_op,
	-- *INF*: IIF(NOT ISNULL(trans_date_23_op), trans_date_23_op, IIF(NOT ISNULL(trans_date_22_op), trans_date_22_op, IIF(NOT ISNULL(trans_date_41_op), trans_date_41_op, IIF(NOT ISNULL(trans_date_42_op), trans_date_42_op, IIF(NOT ISNULL(trans_date_20_40_op), trans_date_20_40_op)))))
	-- 
	-- 
	IFF(NOT trans_date_23_op IS NULL, trans_date_23_op, IFF(NOT trans_date_22_op IS NULL, trans_date_22_op, IFF(NOT trans_date_41_op IS NULL, trans_date_41_op, IFF(NOT trans_date_42_op IS NULL, trans_date_42_op, IFF(NOT trans_date_20_40_op IS NULL, trans_date_20_40_op))))) AS close_dt_d_e,
	-- *INF*: IIF(NOT ISNULL(trans_date_23_op), trans_date_23_op, IIF(NOT ISNULL(trans_date_32_op), trans_date_32_op, IIF(NOT ISNULL(trans_date_33_op), trans_date_33_op, IIF(NOT ISNULL(trans_date_20_40_op), trans_date_20_40_op, IIF(NOT ISNULL(trans_date_41_op), trans_date_41_op, IIF(NOT ISNULL(trans_date_42_op), trans_date_42_op, IIF(NOT ISNULL(trans_date_30_op), trans_date_30_op, IIF(NOT ISNULL(trans_date_22_op), trans_date_22_op))))))))
	-- 
	-- --IIF(NOT ISNULL(trans_date_32_op),trans_date_32_op,IIF(NOT ISNULL(trans_date_33_op),trans_date_33_op,IIF(NOT ISNULL(trans_date_20_40_op),trans_date_20_40_op,IIF(NOT ISNULL(trans_date_41_op),trans_date_41_op,IIF(NOT ISNULL(trans_date_42_op),trans_date_42_op,IIF(NOT ISNULL(trans_date_30_op),trans_date_30_op, IIF(NOT ISNULL(trans_date_23_op), trans_date_23_op, IIF(NOT ISNULL(trans_date_22_op), trans_date_22_op))))))))
	IFF(NOT trans_date_23_op IS NULL, trans_date_23_op, IFF(NOT trans_date_32_op IS NULL, trans_date_32_op, IFF(NOT trans_date_33_op IS NULL, trans_date_33_op, IFF(NOT trans_date_20_40_op IS NULL, trans_date_20_40_op, IFF(NOT trans_date_41_op IS NULL, trans_date_41_op, IFF(NOT trans_date_42_op IS NULL, trans_date_42_op, IFF(NOT trans_date_30_op IS NULL, trans_date_30_op, IFF(NOT trans_date_22_op IS NULL, trans_date_22_op)))))))) AS close_dt_s_b_r,
	trans_date_op,
	trans_date_op_20_40 AS trans_date_20_40_op,
	-- *INF*: IIF(NOT ISNULL(trans_date_op), trans_date_op, trans_date_20_40_op)
	IFF(NOT trans_date_op IS NULL, trans_date_op, trans_date_20_40_op) AS close_dt,
	source_sys_id
	FROM AGG_Cov_Fin_Txn_Cd_closed
),
EXP_set_default_values_closed AS (
	SELECT
	'3CLOSED' AS reserve_date_type,
	close_dt AS reserve_date,
	claimant_cov_det_ak_id,
	financial_type_code,
	reserve_ctgry,
	trans_code,
	cause_of_loss,
	source_sys_id,
	1 AS Crrnt_SnapSht_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID,
	reserve_date AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	to_date('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	sysdate AS created_date,
	sysdate AS modified_date,
	'C' AS financial_type_status_code,
	1 AS logical_flag,
	-- *INF*: :LKP.LKP_CLAIMANT_COV_DTL_RSRV_CALC_OPEN_CLOSED(claimant_cov_det_ak_id,  financial_type_code, '3CLOSED')
	-- 
	-- --:LKP.LKP_CLAM_COV_DTL_RSRV_CALC_OPEN_CLOSED(claimant_cov_det_ak_id, cause_of_loss, financial_type_code, 'CLOSED')
	LKP_CLAIMANT_COV_DTL_RSRV_CALC_OPEN_CLOSED_claimant_cov_det_ak_id_financial_type_code_3CLOSED.claimant_cov_det_reserve_calculation_id AS lkp_claimant_cov_det_reserve_calculation_id
	FROM EXP_Concat_Dates_Closed
	LEFT JOIN LKP_CLAIMANT_COV_DTL_RSRV_CALC_OPEN_CLOSED LKP_CLAIMANT_COV_DTL_RSRV_CALC_OPEN_CLOSED_claimant_cov_det_ak_id_financial_type_code_3CLOSED
	ON LKP_CLAIMANT_COV_DTL_RSRV_CALC_OPEN_CLOSED_claimant_cov_det_ak_id_financial_type_code_3CLOSED.claimant_cov_det_ak_id = claimant_cov_det_ak_id
	AND LKP_CLAIMANT_COV_DTL_RSRV_CALC_OPEN_CLOSED_claimant_cov_det_ak_id_financial_type_code_3CLOSED.financial_type_code = financial_type_code
	AND LKP_CLAIMANT_COV_DTL_RSRV_CALC_OPEN_CLOSED_claimant_cov_det_ak_id_financial_type_code_3CLOSED.reserve_date_type = '3CLOSED'

),
RTR_existing_closed AS (
	SELECT
	reserve_date_type,
	reserve_date,
	claimant_cov_det_ak_id,
	financial_type_code,
	trans_code,
	cause_of_loss,
	reserve_ctgry,
	source_sys_id,
	Crrnt_SnapSht_Flag,
	AUDIT_ID,
	eff_from_date,
	eff_to_date,
	created_date,
	modified_date,
	financial_type_status_code,
	logical_flag,
	lkp_claimant_cov_det_reserve_calculation_id
	FROM EXP_set_default_values_closed
),
RTR_existing_closed_INSERT AS (SELECT * FROM RTR_existing_closed WHERE ISNULL(lkp_claimant_cov_det_reserve_calculation_id)),
RTR_existing_closed_UPDATE AS (SELECT * FROM RTR_existing_closed WHERE FALSE

--NOT ISNULL(lkp_claimant_cov_det_reserve_calculation_id) AND IN(trans_code, '20','30', '40')),
UPD_clmnt_cov_detl_rsrv_clac_closed_insert AS (
	SELECT
	reserve_date_type, 
	reserve_date, 
	claimant_cov_det_ak_id, 
	financial_type_code, 
	trans_code, 
	cause_of_loss, 
	reserve_ctgry, 
	source_sys_id, 
	Crrnt_SnapSht_Flag, 
	AUDIT_ID, 
	eff_from_date, 
	eff_to_date, 
	created_date, 
	modified_date, 
	financial_type_status_code, 
	logical_flag, 
	lkp_claimant_cov_det_reserve_calculation_id
	FROM RTR_existing_closed_INSERT
),
clmt_cov_dtl_rsrv_calc_closed_insert AS (
	INSERT INTO claimant_coverage_detail_reserve_calculation
	(claimant_cov_det_reserve_calculation_ak_id, claimant_cov_det_ak_id, financial_type_code, reserve_date, reserve_date_type, financial_type_status_code, logical_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	SEQ_Clmt_Cov_Dtl_Rsrv_Calc_Open_Close_AK_ID.NEXTVAL AS CLAIMANT_COV_DET_RESERVE_CALCULATION_AK_ID, 
	CLAIMANT_COV_DET_AK_ID, 
	FINANCIAL_TYPE_CODE, 
	RESERVE_DATE, 
	RESERVE_DATE_TYPE, 
	FINANCIAL_TYPE_STATUS_CODE, 
	LOGICAL_FLAG, 
	Crrnt_SnapSht_Flag AS CRRNT_SNPSHT_FLAG, 
	AUDIT_ID AS AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM UPD_clmnt_cov_detl_rsrv_clac_closed_insert
),
UPD_clmnt_cov_detl_rsrv_clac_closed_update AS (
	SELECT
	reserve_date_type AS reserve_date_type3, 
	reserve_date AS reserve_date3, 
	claimant_cov_det_ak_id AS claimant_cov_det_ak_id3, 
	financial_type_code AS financial_type_code3, 
	trans_code AS trans_code3, 
	cause_of_loss AS cause_of_loss3, 
	source_sys_id AS source_sys_id3, 
	Crrnt_SnapSht_Flag AS Crrnt_SnapSht_Flag3, 
	AUDIT_ID AS AUDIT_ID3, 
	eff_from_date AS eff_from_date3, 
	eff_to_date AS eff_to_date3, 
	created_date AS created_date3, 
	modified_date AS modified_date3, 
	financial_type_status_code AS stat_fin_typ_cd_op3, 
	logical_flag AS logical_flag3, 
	lkp_claimant_cov_det_reserve_calculation_id AS lkp_claimant_cov_det_reserve_calculation_id3
	FROM RTR_existing_closed_UPDATE
),
clmt_cov_dtl_rsrv_calc_closed_update AS (
	MERGE INTO claimant_coverage_detail_reserve_calculation AS T
	USING UPD_clmnt_cov_detl_rsrv_clac_closed_update AS S
	ON T.claimant_cov_det_reserve_calculation_id = S.lkp_claimant_cov_det_reserve_calculation_id3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.reserve_date = S.reserve_date3, T.modified_date = S.modified_date3
),
AGG_Cov_Fin_Txn_Cd_open AS (
	SELECT
	claimant_cov_det_ak_id, 
	cause_of_loss, 
	reserve_ctgry, 
	financial_type_code, 
	trans_code, 
	trans_date, 
	MIN(trans_date, trans_code = '90') AS trans_date_op_90, 
	MIN(trans_date, trans_code = '91') AS trans_date_op_91, 
	MIN(trans_date, trans_code = '92') AS trans_date_op_92, 
	MIN(trans_date, trans_code = '23') AS trans_date_op_23, 
	MIN(trans_date, in(trans_code, '20', '40')) AS trans_date_op_20_40, 
	MIN(trans_date, trans_code = '33') AS trans_date_op_33, 
	MIN(trans_date, trans_code = '30') AS trans_date_op_30, 
	MIN(trans_date, trans_code = '22') AS trans_date_op_22, 
	MIN(trans_date, trans_code = '66') AS trans_date_op_66, 
	MIN(trans_date) AS trans_date_op, 
	source_sys_id
	FROM RTR_Grp_Fin_Typ_Cds_Grp_Open
	GROUP BY claimant_cov_det_ak_id, financial_type_code
),
EXP_Concat_Dates_Open AS (
	SELECT
	claimant_cov_det_ak_id,
	financial_type_code,
	cause_of_loss,
	reserve_ctgry,
	trans_date_op_90 AS trans_date_90_op,
	trans_date_op_91 AS trans_date_91_op,
	trans_date_op_92 AS trans_date_92_op,
	trans_date_op_23 AS trans_date_23_op,
	trans_date_op_20_40 AS trans_date_20_40_op,
	trans_date_op_66,
	trans_date_op_33 AS trans_date_33_op,
	trans_date_op_30 AS trans_date_30_op,
	trans_date_op_22 AS trans_date_22_op,
	trans_date_op,
	-- *INF*: IIF(NOT ISNULL(trans_date_23_op),trans_date_23_op,IIF(NOT ISNULL(trans_date_90_op),trans_date_90_op,IIF(NOT ISNULL(trans_date_91_op),trans_date_91_op,IIF(NOT ISNULL(trans_date_92_op),trans_date_92_op,IIF(NOT ISNULL(trans_date_20_40_op),trans_date_20_40_op, IIF(NOT ISNULL(trans_date_op_66), trans_date_op_66, IIF(NOT ISNULL(trans_date_22_op), trans_date_22_op)))))))
	IFF(NOT trans_date_23_op IS NULL, trans_date_23_op, IFF(NOT trans_date_90_op IS NULL, trans_date_90_op, IFF(NOT trans_date_91_op IS NULL, trans_date_91_op, IFF(NOT trans_date_92_op IS NULL, trans_date_92_op, IFF(NOT trans_date_20_40_op IS NULL, trans_date_20_40_op, IFF(NOT trans_date_op_66 IS NULL, trans_date_op_66, IFF(NOT trans_date_22_op IS NULL, trans_date_22_op))))))) AS open_dt_d_e,
	-- *INF*: IIF(NOT ISNULL(trans_date_90_op),trans_date_90_op,IIF(NOT ISNULL(trans_date_91_op),trans_date_91_op,IIF(NOT ISNULL(trans_date_92_op),trans_date_92_op,IIF(NOT ISNULL(trans_date_33_op),trans_date_33_op,IIF(NOT ISNULL(trans_date_30_op),trans_date_30_op, IIF(NOT ISNULL(trans_date_20_40_op), trans_date_20_40_op, IIF(NOT ISNULL(trans_date_op_66), trans_date_op_66, IIF(NOT ISNULL(trans_date_22_op), trans_date_22_op, IIF(NOT ISNULL(trans_date_23_op), trans_date_23_op)))))))))
	IFF(NOT trans_date_90_op IS NULL, trans_date_90_op, IFF(NOT trans_date_91_op IS NULL, trans_date_91_op, IFF(NOT trans_date_92_op IS NULL, trans_date_92_op, IFF(NOT trans_date_33_op IS NULL, trans_date_33_op, IFF(NOT trans_date_30_op IS NULL, trans_date_30_op, IFF(NOT trans_date_20_40_op IS NULL, trans_date_20_40_op, IFF(NOT trans_date_op_66 IS NULL, trans_date_op_66, IFF(NOT trans_date_22_op IS NULL, trans_date_22_op, IFF(NOT trans_date_23_op IS NULL, trans_date_23_op))))))))) AS open_dt_s_b_r,
	-- *INF*: IIF( IN(financial_type_code, 'S','B','R'), open_dt_s_b_r, open_dt_d_e)
	IFF(IN(financial_type_code, 'S', 'B', 'R'), open_dt_s_b_r, open_dt_d_e) AS open_dt,
	source_sys_id
	FROM AGG_Cov_Fin_Txn_Cd_open
),
EXP_set_default_values_open AS (
	SELECT
	'2OPEN' AS reserve_date_type,
	trans_date_op AS reserve_date,
	claimant_cov_det_ak_id,
	financial_type_code,
	cause_of_loss,
	reserve_ctgry,
	source_sys_id,
	1 AS Crrnt_SnapSht_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID,
	reserve_date AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	to_date('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	sysdate AS created_date,
	sysdate AS modified_date,
	'O' AS financial_type_status_code,
	1 AS logical_flag,
	-- *INF*: :LKP.LKP_CLAIMANT_COV_DTL_RSRV_CALC_OPEN_CLOSED(claimant_cov_det_ak_id,  financial_type_code, '2OPEN')
	-- 
	-- --:LKP.LKP_CLAM_COV_DTL_RSRV_CALC_OPEN_CLOSED(claimant_cov_det_ak_id,cause_of_loss,financial_type_code,'OPEN')
	LKP_CLAIMANT_COV_DTL_RSRV_CALC_OPEN_CLOSED_claimant_cov_det_ak_id_financial_type_code_2OPEN.claimant_cov_det_reserve_calculation_id AS lkp_claimant_cov_det_reserve_calculation_id
	FROM EXP_Concat_Dates_Open
	LEFT JOIN LKP_CLAIMANT_COV_DTL_RSRV_CALC_OPEN_CLOSED LKP_CLAIMANT_COV_DTL_RSRV_CALC_OPEN_CLOSED_claimant_cov_det_ak_id_financial_type_code_2OPEN
	ON LKP_CLAIMANT_COV_DTL_RSRV_CALC_OPEN_CLOSED_claimant_cov_det_ak_id_financial_type_code_2OPEN.claimant_cov_det_ak_id = claimant_cov_det_ak_id
	AND LKP_CLAIMANT_COV_DTL_RSRV_CALC_OPEN_CLOSED_claimant_cov_det_ak_id_financial_type_code_2OPEN.financial_type_code = financial_type_code
	AND LKP_CLAIMANT_COV_DTL_RSRV_CALC_OPEN_CLOSED_claimant_cov_det_ak_id_financial_type_code_2OPEN.reserve_date_type = '2OPEN'

),
FIL_existing_opens AS (
	SELECT
	reserve_date_type, 
	reserve_date, 
	claimant_cov_det_ak_id, 
	financial_type_code, 
	cause_of_loss, 
	reserve_ctgry, 
	source_sys_id, 
	Crrnt_SnapSht_Flag, 
	AUDIT_ID, 
	eff_from_date, 
	eff_to_date, 
	created_date, 
	modified_date, 
	financial_type_status_code, 
	logical_flag, 
	lkp_claimant_cov_det_reserve_calculation_id
	FROM EXP_set_default_values_open
	WHERE IIF(ISNULL(lkp_claimant_cov_det_reserve_calculation_id), TRUE, FALSE)
),
clmt_cov_dtl_rsrv_calc_open AS (
	INSERT INTO claimant_coverage_detail_reserve_calculation
	(claimant_cov_det_reserve_calculation_ak_id, claimant_cov_det_ak_id, financial_type_code, reserve_date, reserve_date_type, financial_type_status_code, logical_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	SEQ_Clmt_Cov_Dtl_Rsrv_Calc_Open_Close_AK_ID.NEXTVAL AS CLAIMANT_COV_DET_RESERVE_CALCULATION_AK_ID, 
	CLAIMANT_COV_DET_AK_ID, 
	FINANCIAL_TYPE_CODE, 
	RESERVE_DATE, 
	RESERVE_DATE_TYPE, 
	FINANCIAL_TYPE_STATUS_CODE, 
	LOGICAL_FLAG, 
	Crrnt_SnapSht_Flag AS CRRNT_SNPSHT_FLAG, 
	AUDIT_ID AS AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_existing_opens
),
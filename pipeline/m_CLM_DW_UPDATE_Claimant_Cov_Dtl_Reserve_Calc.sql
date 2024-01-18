WITH
LKP_claimant_cov_dl_rsrv_calc AS (
	SELECT
	reserve_date,
	IN_claimant_cov_det_ak_id,
	IN_financial_type_code,
	IN_reserve_date,
	IN_reserve_date_type,
	claimant_cov_det_ak_id,
	financial_type_code,
	reserve_date_type
	FROM (
		SELECT 
			reserve_date,
			IN_claimant_cov_det_ak_id,
			IN_financial_type_code,
			IN_reserve_date,
			IN_reserve_date_type,
			claimant_cov_det_ak_id,
			financial_type_code,
			reserve_date_type
		FROM claimant_coverage_detail_reserve_calculation
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,financial_type_code,reserve_date,reserve_date_type ORDER BY reserve_date) = 1
),
SQ_claimant_coverage_detail_reserve_calculation_delete_reopens_recloses AS (
	SELECT claimant_coverage_detail_reserve_calculation.claimant_cov_det_reserve_calculation_id, claimant_coverage_detail_reserve_calculation.claimant_cov_det_reserve_calculation_ak_id, claimant_coverage_detail_reserve_calculation.claimant_cov_det_ak_id, claimant_coverage_detail_reserve_calculation.financial_type_code, claimant_coverage_detail_reserve_calculation.reserve_date, claimant_coverage_detail_reserve_calculation.reserve_date_type, claimant_coverage_detail_reserve_calculation.financial_type_status_code, claimant_coverage_detail_reserve_calculation.logical_flag, claimant_coverage_detail_reserve_calculation.crrnt_snpsht_flag, claimant_coverage_detail_reserve_calculation.audit_id, claimant_coverage_detail_reserve_calculation.eff_from_date, claimant_coverage_detail_reserve_calculation.eff_to_date, claimant_coverage_detail_reserve_calculation.source_sys_id, claimant_coverage_detail_reserve_calculation.created_date, claimant_coverage_detail_reserve_calculation.modified_date 
	FROM
	 claimant_coverage_detail_reserve_calculation
	where claimant_coverage_detail_reserve_calculation.reserve_date_type != '1NOTICEONLY' AND
	claimant_coverage_detail_reserve_calculation.created_date >= '@{pipeline().parameters.SELECTION_START_TS}'
	ORDER BY claimant_coverage_detail_reserve_calculation.claimant_cov_det_ak_id,
	claimant_coverage_detail_reserve_calculation.financial_type_code, 
	claimant_coverage_detail_reserve_calculation.reserve_date
),
RTR_delete_reopens_recloses AS (
	SELECT
	claimant_cov_det_reserve_calculation_id,
	claimant_cov_det_reserve_calculation_ak_id,
	claimant_cov_det_ak_id,
	financial_type_code,
	reserve_date,
	reserve_date_type,
	financial_type_status_code,
	logical_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date
	FROM SQ_claimant_coverage_detail_reserve_calculation_delete_reopens_recloses
),
RTR_delete_reopens_recloses_DELETE_REOPEN AS (SELECT * FROM RTR_delete_reopens_recloses WHERE reserve_date_type = '4REOPEN'),
RTR_delete_reopens_recloses_DELETE_RECLOSED AS (SELECT * FROM RTR_delete_reopens_recloses WHERE reserve_date_type = 
'5CLOSEDAFTERREOPEN'),
RTR_delete_reopens_recloses_UPDATE_REOPEN AS (SELECT * FROM RTR_delete_reopens_recloses WHERE TRUE),
EXP_get_values AS (
	SELECT
	claimant_cov_det_reserve_calculation_id,
	claimant_cov_det_reserve_calculation_ak_id,
	claimant_cov_det_ak_id,
	financial_type_code,
	reserve_date,
	reserve_date_type,
	financial_type_status_code,
	claimant_cov_cause_of_loss,
	claimant_cov_reserve_ctgry,
	logical_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date
	FROM RTR_delete_reopens_recloses_UPDATE_REOPEN
),
AGG_get_min_reserve_date AS (
	SELECT
	claimant_cov_det_reserve_calculation_id,
	claimant_cov_det_reserve_calculation_ak_id,
	claimant_cov_det_ak_id,
	claimant_cov_cause_of_loss,
	claimant_cov_reserve_ctgry,
	financial_type_code,
	reserve_date,
	reserve_date_type,
	financial_type_status_code,
	logical_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	-- *INF*: MIN(reserve_date)
	MIN(reserve_date) AS reserve_date_out
	FROM EXP_get_values
	GROUP BY claimant_cov_det_ak_id, financial_type_code
),
EXP_get_lookup_values AS (
	SELECT
	claimant_cov_det_reserve_calculation_id,
	claimant_cov_det_reserve_calculation_ak_id,
	claimant_cov_det_ak_id,
	claimant_cov_cause_of_loss,
	claimant_cov_reserve_ctgry,
	financial_type_code,
	reserve_date_out,
	reserve_date_type,
	financial_type_status_code,
	-- *INF*: :LKP.LKP_CLAIMANT_COV_DL_RSRV_CALC(claimant_cov_det_ak_id, financial_type_code, reserve_date_out, '2OPEN')
	-- 
	-- --:LKP.LKP_CLMNT_COV_DTL_RSRV_CALC(claimant_cov_det_ak_id, claimant_cov_cause_of_loss, financial_type_code, reserve_date_out, 'OPEN')
	LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_out_2OPEN.reserve_date AS V_reserve_date_open,
	-- *INF*: :LKP.LKP_CLAIMANT_COV_DL_RSRV_CALC(claimant_cov_det_ak_id,  financial_type_code, reserve_date_out, '3CLOSED')
	-- 
	-- --:LKP.LKP_CLMNT_COV_DTL_RSRV_CALC(claimant_cov_det_ak_id, claimant_cov_cause_of_loss, financial_type_code, reserve_date_out, 'CLOSED')
	LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_out_3CLOSED.reserve_date AS V_reserve_date_closed,
	-- *INF*: :LKP.LKP_CLAIMANT_COV_DL_RSRV_CALC(claimant_cov_det_ak_id, financial_type_code, reserve_date_out, '4REOPEN')
	-- 
	-- --:LKP.LKP_CLMNT_COV_DTL_RSRV_CALC(claimant_cov_det_ak_id, claimant_cov_cause_of_loss, financial_type_code, reserve_date_out, 'REOPEN')
	LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_out_4REOPEN.reserve_date AS V_reserve_date_reopen,
	-- *INF*: :LKP.LKP_CLAIMANT_COV_DL_RSRV_CALC(claimant_cov_det_ak_id, financial_type_code, reserve_date_out, '5CLOSEDAFTERREOPEN')
	-- 
	-- --:LKP.LKP_CLMNT_COV_DTL_RSRV_CALC(claimant_cov_det_ak_id, claimant_cov_cause_of_loss, financial_type_code, reserve_date_out, 'CLOSEDAFTERREOPEN')
	LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_out_5CLOSEDAFTERREOPEN.reserve_date AS V_reserve_Date_reclosed,
	-- *INF*: IIF(ISNULL(V_reserve_date_open), IIF(NOT ISNULL(V_reserve_date_closed), 'INSERTOPEN'), 'NOCHANGE')
	-- 
	-- --IIF(ISNULL(V_reserve_date_open), IIF(NOT ISNULL(V_reserve_date_closed),  'INSERTOPEN',  'NOCHANGE')
	-- 
	-- 
	-- 
	-- 
	IFF(
	    V_reserve_date_open IS NULL,
	    IFF(
	        V_reserve_date_closed IS NOT NULL, 'INSERTOPEN'
	    ),
	    'NOCHANGE'
	) AS V_flag,
	V_flag AS flag,
	-- *INF*: IIF(ISNULL(V_reserve_date_open), 
	-- IIF(ISNULL(V_reserve_date_closed), reserve_date_out, V_reserve_date_closed), V_reserve_date_open)
	IFF(
	    V_reserve_date_open IS NULL,
	    IFF(
	        V_reserve_date_closed IS NULL, reserve_date_out, V_reserve_date_closed
	    ),
	    V_reserve_date_open
	) AS reserve_date_new,
	logical_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date
	FROM AGG_get_min_reserve_date
	LEFT JOIN LKP_CLAIMANT_COV_DL_RSRV_CALC LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_out_2OPEN
	ON LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_out_2OPEN.claimant_cov_det_ak_id = claimant_cov_det_ak_id
	AND LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_out_2OPEN.financial_type_code = financial_type_code
	AND LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_out_2OPEN.reserve_date = reserve_date_out
	AND LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_out_2OPEN.reserve_date_type = '2OPEN'

	LEFT JOIN LKP_CLAIMANT_COV_DL_RSRV_CALC LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_out_3CLOSED
	ON LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_out_3CLOSED.claimant_cov_det_ak_id = claimant_cov_det_ak_id
	AND LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_out_3CLOSED.financial_type_code = financial_type_code
	AND LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_out_3CLOSED.reserve_date = reserve_date_out
	AND LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_out_3CLOSED.reserve_date_type = '3CLOSED'

	LEFT JOIN LKP_CLAIMANT_COV_DL_RSRV_CALC LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_out_4REOPEN
	ON LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_out_4REOPEN.claimant_cov_det_ak_id = claimant_cov_det_ak_id
	AND LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_out_4REOPEN.financial_type_code = financial_type_code
	AND LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_out_4REOPEN.reserve_date = reserve_date_out
	AND LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_out_4REOPEN.reserve_date_type = '4REOPEN'

	LEFT JOIN LKP_CLAIMANT_COV_DL_RSRV_CALC LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_out_5CLOSEDAFTERREOPEN
	ON LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_out_5CLOSEDAFTERREOPEN.claimant_cov_det_ak_id = claimant_cov_det_ak_id
	AND LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_out_5CLOSEDAFTERREOPEN.financial_type_code = financial_type_code
	AND LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_out_5CLOSEDAFTERREOPEN.reserve_date = reserve_date_out
	AND LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_out_5CLOSEDAFTERREOPEN.reserve_date_type = '5CLOSEDAFTERREOPEN'

),
FIL_insert_open AS (
	SELECT
	claimant_cov_det_reserve_calculation_id, 
	claimant_cov_det_reserve_calculation_ak_id, 
	claimant_cov_det_ak_id, 
	claimant_cov_cause_of_loss, 
	claimant_cov_reserve_ctgry, 
	financial_type_code, 
	reserve_date_out, 
	reserve_date_type, 
	financial_type_status_code, 
	flag, 
	reserve_date_new, 
	logical_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date
	FROM EXP_get_lookup_values
	WHERE flag= 'INSERTOPEN'
),
EXP_insert_missing_opens AS (
	SELECT
	claimant_cov_det_reserve_calculation_id,
	claimant_cov_det_reserve_calculation_ak_id,
	claimant_cov_det_ak_id,
	claimant_cov_cause_of_loss,
	claimant_cov_reserve_ctgry,
	financial_type_code,
	reserve_date_out AS reserve_date,
	'2OPEN' AS reserve_date_type,
	'O' AS financial_type_status_code,
	flag,
	-1 AS logical_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	reserve_date AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS') 
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM FIL_insert_open
),
SEQ_Clmt_Cov_Dtl_Rsrv_Calc_AK_ID AS (
	CREATE SEQUENCE SEQ_Clmt_Cov_Dtl_Rsrv_Calc_AK_ID
	START = 0
	INCREMENT = 1;
),
claimant_coverage_detail_reserve_calculation_insert_open AS (
	INSERT INTO claimant_coverage_detail_reserve_calculation
	(claimant_cov_det_reserve_calculation_ak_id, claimant_cov_det_ak_id, financial_type_code, reserve_date, reserve_date_type, financial_type_status_code, logical_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	SEQ_Clmt_Cov_Dtl_Rsrv_Calc_AK_ID.NEXTVAL AS CLAIMANT_COV_DET_RESERVE_CALCULATION_AK_ID, 
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
	FROM EXP_insert_missing_opens
),
EXP_find_extra_reopens AS (
	SELECT
	claimant_cov_det_reserve_calculation_id,
	claimant_cov_det_reserve_calculation_ak_id,
	claimant_cov_det_ak_id,
	financial_type_code,
	reserve_date,
	reserve_date_type,
	financial_type_status_code,
	claimant_cov_cause_of_loss,
	claimant_cov_reserve_ctgry,
	logical_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	-- *INF*: :LKP.LKP_CLAIMANT_COV_DL_RSRV_CALC(claimant_cov_det_ak_id, financial_type_code, reserve_date, '2OPEN')
	-- 
	-- --:LKP.LKP_CLMNT_COV_DTL_RSRV_CALC(claimant_cov_det_ak_id, claimant_cov_cause_of_loss, financial_type_code, reserve_date, 'OPEN')
	LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_2OPEN.reserve_date AS v_Open_date,
	-- *INF*: :LKP.LKP_CLAIMANT_COV_DL_RSRV_CALC(claimant_cov_det_ak_id, financial_type_code, reserve_date, '3CLOSED')
	-- 
	-- --:LKP.LKP_CLMNT_COV_DTL_RSRV_CALC(claimant_cov_det_ak_id, claimant_cov_cause_of_loss, financial_type_code, reserve_date, 'OPEN')
	LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_3CLOSED.reserve_date AS v_closed_date,
	-- *INF*: IIF(v_Open_date = reserve_date, 'DELETE',  IIF(ISNULL(v_Open_date) AND ISNULL(v_closed_date), 'UPDATE', 'NOCHANGE'))
	-- 
	-- 
	IFF(
	    v_Open_date = reserve_date, 'DELETE',
	    IFF(
	        v_Open_date IS NULL AND v_closed_date IS NULL, 'UPDATE', 'NOCHANGE'
	    )
	) AS delete_flag
	FROM RTR_delete_reopens_recloses_DELETE_REOPEN
	LEFT JOIN LKP_CLAIMANT_COV_DL_RSRV_CALC LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_2OPEN
	ON LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_2OPEN.claimant_cov_det_ak_id = claimant_cov_det_ak_id
	AND LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_2OPEN.financial_type_code = financial_type_code
	AND LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_2OPEN.reserve_date = reserve_date
	AND LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_2OPEN.reserve_date_type = '2OPEN'

	LEFT JOIN LKP_CLAIMANT_COV_DL_RSRV_CALC LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_3CLOSED
	ON LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_3CLOSED.claimant_cov_det_ak_id = claimant_cov_det_ak_id
	AND LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_3CLOSED.financial_type_code = financial_type_code
	AND LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_3CLOSED.reserve_date = reserve_date
	AND LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_3CLOSED.reserve_date_type = '3CLOSED'

),
RTRTRANS AS (
	SELECT
	claimant_cov_det_reserve_calculation_id,
	claimant_cov_det_reserve_calculation_ak_id,
	claimant_cov_det_ak_id,
	financial_type_code,
	reserve_date,
	reserve_date_type,
	financial_type_status_code,
	claimant_cov_cause_of_loss,
	claimant_cov_reserve_ctgry,
	logical_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	delete_flag
	FROM EXP_find_extra_reopens
),
RTRTRANS_DELETE AS (SELECT * FROM RTRTRANS WHERE delete_flag = 'DELETE'),
RTRTRANS_UPDATE AS (SELECT * FROM RTRTRANS WHERE delete_flag = 'UPDATE'),
UPD_Delete_reopen AS (
	SELECT
	claimant_cov_det_reserve_calculation_id, 
	claimant_cov_det_reserve_calculation_ak_id, 
	claimant_cov_det_ak_id, 
	financial_type_code, 
	reserve_date, 
	reserve_date_type, 
	financial_type_status_code, 
	claimant_cov_cause_of_loss, 
	claimant_cov_reserve_ctgry, 
	logical_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date, 
	delete_flag
	FROM RTRTRANS_DELETE
),
claimant_coverage_detail_reserve_calculation_delete_reopen AS (
	DELETE FROM claimant_coverage_detail_reserve_calculation
	WHERE (claimant_cov_det_reserve_calculation_id) IN (SELECT  CLAIMANT_COV_DET_RESERVE_CALCULATION_ID FROM UPD_Delete_reopen)
),
EXP_update_reopens AS (
	SELECT
	claimant_cov_det_reserve_calculation_id,
	claimant_cov_det_reserve_calculation_ak_id,
	claimant_cov_det_ak_id,
	claimant_cov_cause_of_loss,
	claimant_cov_reserve_ctgry,
	financial_type_code,
	reserve_date AS reserve_date_out,
	'2OPEN' AS reserve_date_type,
	financial_type_status_code,
	-2 AS logical_flag,
	1 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM RTRTRANS_UPDATE
),
UPD_reopens AS (
	SELECT
	claimant_cov_det_reserve_calculation_id, 
	claimant_cov_det_reserve_calculation_ak_id, 
	claimant_cov_det_ak_id, 
	claimant_cov_cause_of_loss, 
	claimant_cov_reserve_ctgry, 
	financial_type_code, 
	reserve_date_out, 
	reserve_date_type, 
	financial_type_status_code, 
	flag, 
	logical_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date
	FROM EXP_update_reopens
),
claimant_coverage_detail_reserve_calculation_update_reopen AS (
	MERGE INTO claimant_coverage_detail_reserve_calculation AS T
	USING UPD_reopens AS S
	ON T.claimant_cov_det_reserve_calculation_id = S.claimant_cov_det_reserve_calculation_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.reserve_date_type = S.reserve_date_type, T.logical_flag = S.logical_flag, T.modified_date = S.modified_date
),
EXP_find_extra_recloses AS (
	SELECT
	claimant_cov_det_reserve_calculation_id,
	claimant_cov_det_reserve_calculation_ak_id,
	claimant_cov_det_ak_id,
	financial_type_code,
	reserve_date,
	reserve_date_type,
	financial_type_status_code,
	claimant_cov_cause_of_loss,
	claimant_cov_reserve_ctgry,
	logical_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	-- *INF*: :LKP.LKP_CLAIMANT_COV_DL_RSRV_CALC(claimant_cov_det_ak_id,  financial_type_code, reserve_date, '3CLOSED')
	-- 
	-- --:LKP.LKP_CLMNT_COV_DTL_RSRV_CALC(claimant_cov_det_ak_id, claimant_cov_cause_of_loss, financial_type_code, reserve_date, 'CLOSED')
	LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_3CLOSED.reserve_date AS v_Closed_date,
	-- *INF*: IIF(v_Closed_date = reserve_date OR(logical_flag = '100' AND v_Closed_date < reserve_date), 'DELETE',  'NOCHANGE')
	-- 
	-- 
	IFF(
	    v_Closed_date = reserve_date OR (logical_flag = '100' AND v_Closed_date < reserve_date),
	    'DELETE',
	    'NOCHANGE'
	) AS delete_flag
	FROM RTR_delete_reopens_recloses_DELETE_RECLOSED
	LEFT JOIN LKP_CLAIMANT_COV_DL_RSRV_CALC LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_3CLOSED
	ON LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_3CLOSED.claimant_cov_det_ak_id = claimant_cov_det_ak_id
	AND LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_3CLOSED.financial_type_code = financial_type_code
	AND LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_3CLOSED.reserve_date = reserve_date
	AND LKP_CLAIMANT_COV_DL_RSRV_CALC_claimant_cov_det_ak_id_financial_type_code_reserve_date_3CLOSED.reserve_date_type = '3CLOSED'

),
FIL_recloses AS (
	SELECT
	claimant_cov_det_reserve_calculation_id, 
	claimant_cov_det_reserve_calculation_ak_id, 
	claimant_cov_det_ak_id, 
	financial_type_code, 
	reserve_date, 
	reserve_date_type, 
	financial_type_status_code, 
	claimant_cov_cause_of_loss, 
	claimant_cov_reserve_ctgry, 
	logical_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date, 
	delete_flag
	FROM EXP_find_extra_recloses
	WHERE delete_flag = 'DELETE'
),
UPD_delete_reclose AS (
	SELECT
	claimant_cov_det_reserve_calculation_id, 
	claimant_cov_det_reserve_calculation_ak_id, 
	claimant_cov_det_ak_id, 
	financial_type_code, 
	reserve_date, 
	reserve_date_type, 
	financial_type_status_code, 
	claimant_cov_cause_of_loss, 
	claimant_cov_reserve_ctgry, 
	logical_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date, 
	delete_flag
	FROM FIL_recloses
),
claimant_coverage_detail_reserve_calculation_delete_reclosed AS (
	DELETE FROM claimant_coverage_detail_reserve_calculation
	WHERE (claimant_cov_det_reserve_calculation_id) IN (SELECT  CLAIMANT_COV_DET_RESERVE_CALCULATION_ID FROM UPD_delete_reclose)
),
SQ_claimant_coverage_detail_reserve_calculation_insert_closed_reclosed AS (
	SELECT claimant_coverage_detail_reserve_calculation.claimant_cov_det_reserve_calculation_id, claimant_coverage_detail_reserve_calculation.claimant_cov_det_reserve_calculation_ak_id, claimant_coverage_detail_reserve_calculation.claimant_cov_det_ak_id, claimant_coverage_detail_reserve_calculation.financial_type_code, claimant_coverage_detail_reserve_calculation.reserve_date, claimant_coverage_detail_reserve_calculation.reserve_date_type, claimant_coverage_detail_reserve_calculation.financial_type_status_code, claimant_coverage_detail_reserve_calculation.logical_flag, claimant_coverage_detail_reserve_calculation.crrnt_snpsht_flag, claimant_coverage_detail_reserve_calculation.audit_id, claimant_coverage_detail_reserve_calculation.eff_from_date, claimant_coverage_detail_reserve_calculation.eff_to_date, claimant_coverage_detail_reserve_calculation.source_sys_id, claimant_coverage_detail_reserve_calculation.created_date, claimant_coverage_detail_reserve_calculation.modified_date 
	FROM
	 claimant_coverage_detail_reserve_calculation
	WHERE claimant_coverage_detail_reserve_calculation.reserve_date_type != '1NOTICEONLY' 
	AND claimant_coverage_detail_reserve_calculation.created_date >= '@{pipeline().parameters.SELECTION_START_TS}'
	ORDER BY claimant_coverage_detail_reserve_calculation.claimant_cov_det_ak_id,
	claimant_coverage_detail_reserve_calculation.financial_type_code, 
	claimant_coverage_detail_reserve_calculation.reserve_date, 
	claimant_coverage_detail_reserve_calculation.reserve_date_type
),
EXP_determine_insert AS (
	SELECT
	claimant_cov_det_reserve_calculation_id,
	claimant_cov_det_reserve_calculation_ak_id,
	claimant_cov_det_ak_id,
	financial_type_code,
	reserve_date,
	reserve_date_type,
	financial_type_status_code,
	logical_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	-- *INF*: IIF(reserve_date_type = '4REOPEN', IIF(claimant_cov_det_ak_id = v_ak_id AND claimant_cov_cause_of_loss = V_prev_row_claimant_cov_cause_of_loss  AND claimant_cov_reserve_ctgry = v_prev_row_claimant_cov_reserve_ctgry AND financial_type_code = v_prev_row_financial_type_code, IIF(v_prev_row_reserve_date_type = '2OPEN', 'INSERT_CLOSED', IIF(v_prev_row_reserve_date_type = '4REOPEN', 'INSERT_RECLOSED', 'NOCHANGE')), 'NOCHANGE'), 'NOCHANGE')
	IFF(
	    reserve_date_type = '4REOPEN',
	    IFF(
	        claimant_cov_det_ak_id = v_ak_id
	        and claimant_cov_cause_of_loss = V_prev_row_claimant_cov_cause_of_loss
	        and claimant_cov_reserve_ctgry = v_prev_row_claimant_cov_reserve_ctgry
	        and financial_type_code = v_prev_row_financial_type_code,
	        IFF(
	            v_prev_row_reserve_date_type = '2OPEN', 'INSERT_CLOSED',
	            IFF(
	                v_prev_row_reserve_date_type = '4REOPEN', 'INSERT_RECLOSED',
	                'NOCHANGE'
	            )
	        ),
	        'NOCHANGE'
	    ),
	    'NOCHANGE'
	) AS v_insert_flag,
	v_insert_flag AS insert_flag,
	v_prev_row_reserve_date AS v_prev_row_reserve_date_out,
	v_prev_row_reserve_date_out AS prev_row_reserve_date_out,
	claimant_cov_det_ak_id AS v_ak_id,
	claimant_cov_cause_of_loss AS V_prev_row_claimant_cov_cause_of_loss,
	claimant_cov_reserve_ctgry AS v_prev_row_claimant_cov_reserve_ctgry,
	financial_type_code AS v_prev_row_financial_type_code,
	reserve_date AS v_prev_row_reserve_date,
	reserve_date_type AS v_prev_row_reserve_date_type
	FROM SQ_claimant_coverage_detail_reserve_calculation_insert_closed_reclosed
),
FIL_no_change AS (
	SELECT
	claimant_cov_det_reserve_calculation_id, 
	claimant_cov_det_reserve_calculation_ak_id, 
	claimant_cov_det_ak_id, 
	financial_type_code, 
	reserve_date, 
	prev_row_reserve_date_out, 
	reserve_date_type, 
	financial_type_status_code, 
	claimant_cov_cause_of_loss, 
	claimant_cov_reserve_ctgry, 
	insert_flag, 
	logical_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date
	FROM EXP_determine_insert
	WHERE IIF(insert_flag = 'NOCHANGE', FALSE, TRUE)
),
EXP_set_default_value AS (
	SELECT
	claimant_cov_det_reserve_calculation_id,
	claimant_cov_det_reserve_calculation_ak_id,
	claimant_cov_det_ak_id,
	financial_type_code,
	reserve_date,
	prev_row_reserve_date_out,
	insert_flag,
	-- *INF*: IIF(insert_flag = 'INSERT_CLOSED', '3CLOSED', IIF(insert_flag = 'INSERT_RECLOSED', '5CLOSEDAFTERREOPEN'))
	-- 
	-- 
	IFF(
	    insert_flag = 'INSERT_CLOSED', '3CLOSED',
	    IFF(
	        insert_flag = 'INSERT_RECLOSED', '5CLOSEDAFTERREOPEN'
	    )
	) AS reserve_date_type,
	'C' AS financial_type_status_code,
	claimant_cov_cause_of_loss,
	claimant_cov_reserve_ctgry,
	-3 AS logical_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	prev_row_reserve_date_out AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS') 
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM FIL_no_change
),
claimant_coverage_detail_reserve_calculation_insert_closed AS (
	INSERT INTO claimant_coverage_detail_reserve_calculation
	(claimant_cov_det_reserve_calculation_ak_id, claimant_cov_det_ak_id, financial_type_code, reserve_date, reserve_date_type, financial_type_status_code, logical_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	SEQ_Clmt_Cov_Dtl_Rsrv_Calc_AK_ID.NEXTVAL AS CLAIMANT_COV_DET_RESERVE_CALCULATION_AK_ID, 
	CLAIMANT_COV_DET_AK_ID, 
	FINANCIAL_TYPE_CODE, 
	prev_row_reserve_date_out AS RESERVE_DATE, 
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
	FROM EXP_set_default_value
),
SQ_claimant_coverage_detail_reserve_calculation_update AS (
	SELECT a.claimant_cov_det_reserve_calculation_id, a.claimant_cov_det_ak_id, a.financial_type_code,  a.eff_from_date, a.eff_to_date, a.source_sys_id 
	FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_detail_reserve_calculation a
	where EXISTS (SELECT 1			
		FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_detail_reserve_calculation b
	WHERE b.crrnt_snpsht_flag = 1
	AND a.claimant_cov_det_ak_id = b.claimant_cov_det_ak_id
	and a.source_sys_id = b.source_sys_id
	and a.financial_type_code = b.financial_type_code
	GROUP BY b.claimant_cov_det_ak_id, b.financial_type_code, b.source_sys_id
		HAVING COUNT(*) > 1)
	ORDER BY a.claimant_cov_det_ak_id, a.financial_type_code, a.source_sys_id,  a.eff_from_date  DESC, a.reserve_date_type DESC
	
	--EXISTS Subquery exists to pick AK Groups that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of eff_to_date='12/31/2100 23:59:59' and all columns of the AK
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
	
	--ORDER BY of main query orders all rows first by the AK and then by the eff_from_date in a DESC format
	--the descending order is important because this allows us to avoid another lookup and properly apply the eff_to_date by utilizing a local variable to keep track
	
	--- In the order by clause we add reserve_date_type DESC because status of '2OPEN' and '3CLOSED' happens on same day for PMS data, the row with '3CLOSED' 
	--- status should have a crrnt_snpsht_flag =1
),
EXP_Expire_Rows AS (
	SELECT
	claimant_cov_det_reserve_calculation_id,
	claimant_cov_det_ak_id,
	financial_type_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	source_sys_id,
	-- *INF*: DECODE (TRUE, claimant_cov_det_ak_id = v_PREV_ROW_claimant_cov_det_ak_id and source_sys_id = v_PREV_ROW_source_sys_id and financial_type_code = v_PREV_ROW_financial_type_code ,ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(
	    TRUE,
	    claimant_cov_det_ak_id = v_PREV_ROW_claimant_cov_det_ak_id and source_sys_id = v_PREV_ROW_source_sys_id and financial_type_code = v_PREV_ROW_financial_type_code, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	claimant_cov_det_ak_id AS v_PREV_ROW_claimant_cov_det_ak_id,
	financial_type_code AS v_PREV_ROW_financial_type_code,
	source_sys_id AS v_PREV_ROW_source_sys_id,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	0 AS crrnt_Snpsht_flag,
	sysdate AS modified_date
	FROM SQ_claimant_coverage_detail_reserve_calculation_update
),
FLT_Claimant_cov_dtl_rsrv_calc_Upd AS (
	SELECT
	claimant_cov_det_reserve_calculation_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_Snpsht_flag, 
	modified_date
	FROM EXP_Expire_Rows
	WHERE orig_eff_to_date != eff_to_date
),
UPD_Claimant_cov_dtl_rsrv_calc AS (
	SELECT
	claimant_cov_det_reserve_calculation_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_Snpsht_flag, 
	modified_date
	FROM FLT_Claimant_cov_dtl_rsrv_calc_Upd
),
clmt_cov_dtl_rsrv_calc_update AS (
	MERGE INTO claimant_coverage_detail_reserve_calculation AS T
	USING UPD_Claimant_cov_dtl_rsrv_calc AS S
	ON T.claimant_cov_det_reserve_calculation_id = S.claimant_cov_det_reserve_calculation_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_Snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),
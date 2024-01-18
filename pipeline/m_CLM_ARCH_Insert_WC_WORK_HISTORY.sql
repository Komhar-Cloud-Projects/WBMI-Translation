WITH
SQ_wc_work_history_stage AS (
	SELECT
		wc_work_history_id,
		wch_claim_nbr,
		wch_client_id,
		wch_object_seq_nbr,
		wch_create_ts,
		wch_return_dt,
		wch_last_wrk_dt,
		wch_ret_type_cd,
		wch_same_emp_ind,
		wch_update_ts,
		wch_entry_opr_id,
		wch_update_opr_id,
		extract_date,
		as_of_date,
		record_count,
		source_system_id,
		wrh_restrictions,
		wrh_empr_pd_lit,
		wrh_empr_pd_amt
	FROM wc_work_history_stage
),
EXPTRANS AS (
	SELECT
	wc_work_history_id AS WC_WORK_HISTORY_ID,
	wch_claim_nbr AS WCH_CLAIM_NBR,
	wch_client_id AS WCH_CLIENT_ID,
	wch_object_seq_nbr AS WCH_OBJECT_SEQ_NBR,
	wch_create_ts AS WCH_CREATE_TS,
	wch_return_dt AS WCH_RETURN_DT,
	wch_last_wrk_dt AS WCH_LAST_WRK_DT,
	wch_ret_type_cd AS WCH_RET_TYPE_CD,
	wch_same_emp_ind AS WCH_SAME_EMP_IND,
	wch_update_ts AS WCH_UPDATE_TS,
	wch_entry_opr_id AS WCH_ENTRY_OPR_ID,
	wch_update_opr_id AS WCH_UPDATE_OPR_ID,
	extract_date AS EXTRACT_DATE,
	as_of_date AS AS_OF_DATE,
	record_count AS RECORD_COUNT,
	source_system_id AS SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP,
	wrh_restrictions,
	wrh_empr_pd_lit,
	wrh_empr_pd_amt
	FROM SQ_wc_work_history_stage
),
arch_wc_work_history_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ARCH_WC_WORK_HISTORY_STAGE
	(wc_work_history_id, wch_claim_nbr, wch_client_id, wch_object_seq_nbr, wch_create_ts, wch_return_dt, wch_last_wrk_dt, wch_ret_type_cd, wch_same_emp_ind, wch_update_ts, wch_entry_opr_id, wch_update_opr_id, extract_date, as_of_date, record_count, source_system_id, wrh_restrictions, wrh_empr_pd_lit, wrh_empr_pd_amt)
	SELECT 
	WC_WORK_HISTORY_ID AS WC_WORK_HISTORY_ID, 
	WCH_CLAIM_NBR AS WCH_CLAIM_NBR, 
	WCH_CLIENT_ID AS WCH_CLIENT_ID, 
	WCH_OBJECT_SEQ_NBR AS WCH_OBJECT_SEQ_NBR, 
	WCH_CREATE_TS AS WCH_CREATE_TS, 
	WCH_RETURN_DT AS WCH_RETURN_DT, 
	WCH_LAST_WRK_DT AS WCH_LAST_WRK_DT, 
	WCH_RET_TYPE_CD AS WCH_RET_TYPE_CD, 
	WCH_SAME_EMP_IND AS WCH_SAME_EMP_IND, 
	WCH_UPDATE_TS AS WCH_UPDATE_TS, 
	WCH_ENTRY_OPR_ID AS WCH_ENTRY_OPR_ID, 
	WCH_UPDATE_OPR_ID AS WCH_UPDATE_OPR_ID, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	WRH_RESTRICTIONS, 
	WRH_EMPR_PD_LIT, 
	WRH_EMPR_PD_AMT
	FROM EXPTRANS
),
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
AG_Deduplicate AS (
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
	source_system_id, 
	wrh_restrictions, 
	wrh_empr_pd_lit, 
	wrh_empr_pd_amt
	FROM SQ_wc_work_history_stage
	QUALIFY ROW_NUMBER() OVER (PARTITION BY wch_claim_nbr, wch_client_id ORDER BY NULL) = 1
),
EXP_Workers_Comp_Claimant_Work_History AS (
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
	source_system_id,
	wrh_restrictions,
	wrh_empr_pd_lit,
	wrh_empr_pd_amt
	FROM AG_Deduplicate
),
EXP_Lkp_Values_workers_comp_claimant_detail AS (
	SELECT
	wch_claim_nbr AS IN_CWC_CLAIM_NBR,
	wch_client_id AS IN_CWC_CLIENT_ID,
	wch_object_seq_nbr AS IN_CWC_OBJECT_SEQ_NBR,
	source_system_id AS IN_SOURCE_SYSTEM_ID,
	wrh_restrictions AS IN_wrh_restrictions,
	wrh_empr_pd_lit AS IN_wrh_empr_pd_lit,
	wrh_empr_pd_amt AS IN_wrh_empr_pd_amt,
	wch_last_wrk_dt AS IN_wch_last_wrk_dt,
	wch_return_dt AS IN_wch_return_dt,
	wch_ret_type_cd AS IN_wch_ret_type_cd,
	wch_same_emp_ind AS IN_wch_same_emp_ind,
	wch_create_ts AS IN_wch_create_ts,
	-- *INF*: IIF(ISNULL(IN_wch_last_wrk_dt),TO_DATE('1/1/1800','MM/DD/YYYY'),IN_wch_last_wrk_dt)
	-- 
	-- 
	-- 
	IFF(IN_wch_last_wrk_dt IS NULL, TO_DATE('1/1/1800', 'MM/DD/YYYY'), IN_wch_last_wrk_dt) AS EMP_LAST_DAY_WORKED,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_CWC_CLAIM_NBR))),'N/A',IIF(IS_SPACES(IN_CWC_CLAIM_NBR),'N/A',LTRIM(RTRIM(IN_CWC_CLAIM_NBR))))
	IFF(LTRIM(RTRIM(IN_CWC_CLAIM_NBR)) IS NULL, 'N/A', IFF(IS_SPACES(IN_CWC_CLAIM_NBR), 'N/A', LTRIM(RTRIM(IN_CWC_CLAIM_NBR)))) AS CWC_CLAIM_NBR,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_CWC_CLIENT_ID))),'N/A',IIF(IS_SPACES(IN_CWC_CLIENT_ID),'N/A',LTRIM(RTRIM(IN_CWC_CLIENT_ID))))
	IFF(LTRIM(RTRIM(IN_CWC_CLIENT_ID)) IS NULL, 'N/A', IFF(IS_SPACES(IN_CWC_CLIENT_ID), 'N/A', LTRIM(RTRIM(IN_CWC_CLIENT_ID)))) AS CWC_CLIENT_ID,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_SOURCE_SYSTEM_ID))),'N/A',IIF(IS_SPACES(IN_SOURCE_SYSTEM_ID),'N/A',LTRIM(RTRIM(IN_SOURCE_SYSTEM_ID))))
	IFF(LTRIM(RTRIM(IN_SOURCE_SYSTEM_ID)) IS NULL, 'N/A', IFF(IS_SPACES(IN_SOURCE_SYSTEM_ID), 'N/A', LTRIM(RTRIM(IN_SOURCE_SYSTEM_ID)))) AS SOURCE_SYSTEM_ID,
	-- *INF*: IIF(ISNULL(IN_CWC_OBJECT_SEQ_NBR),0,IN_CWC_OBJECT_SEQ_NBR)
	IFF(IN_CWC_OBJECT_SEQ_NBR IS NULL, 0, IN_CWC_OBJECT_SEQ_NBR) AS CWC_OBJECT_SEQ_NBR,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_wrh_empr_pd_lit))),'N/A',IIF(IS_SPACES(IN_wrh_empr_pd_lit),'N/A',LTRIM(RTRIM(IN_wrh_empr_pd_lit))))
	IFF(LTRIM(RTRIM(IN_wrh_empr_pd_lit)) IS NULL, 'N/A', IFF(IS_SPACES(IN_wrh_empr_pd_lit), 'N/A', LTRIM(RTRIM(IN_wrh_empr_pd_lit)))) AS EMPLYR_PAID_LITIGATED_IND,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_wrh_restrictions))),'N/A',IIF(IS_SPACES(IN_wrh_restrictions),'N/A',LTRIM(RTRIM(IN_wrh_restrictions))))
	-- 
	-- 
	IFF(LTRIM(RTRIM(IN_wrh_restrictions)) IS NULL, 'N/A', IFF(IS_SPACES(IN_wrh_restrictions), 'N/A', LTRIM(RTRIM(IN_wrh_restrictions)))) AS RETURN_TO_WORK_WITH_RESTRICTION_IND,
	-- *INF*: IIF(ISNULL(IN_wrh_empr_pd_amt),0,IN_wrh_empr_pd_amt)
	IFF(IN_wrh_empr_pd_amt IS NULL, 0, IN_wrh_empr_pd_amt) AS EMPLYR_PAID_AMT,
	-- *INF*: IIF(ISNULL(IN_wch_return_dt),TO_DATE('1/1/1800','MM/DD/YYYY'),IN_wch_return_dt)
	IFF(IN_wch_return_dt IS NULL, TO_DATE('1/1/1800', 'MM/DD/YYYY'), IN_wch_return_dt) AS RETURN_TO_WORK_DATE,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_wch_same_emp_ind))),'N/A',IIF(IS_SPACES(IN_wch_same_emp_ind),'N/A',LTRIM(RTRIM(IN_wch_same_emp_ind))))
	IFF(LTRIM(RTRIM(IN_wch_same_emp_ind)) IS NULL, 'N/A', IFF(IS_SPACES(IN_wch_same_emp_ind), 'N/A', LTRIM(RTRIM(IN_wch_same_emp_ind)))) AS RETURN_TO_WORK_WITH_SAME_EMPLYR,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_wch_ret_type_cd))),'N/A',IIF(IS_SPACES(IN_wch_ret_type_cd),'N/A',LTRIM(RTRIM(IN_wch_ret_type_cd))))
	IFF(LTRIM(RTRIM(IN_wch_ret_type_cd)) IS NULL, 'N/A', IFF(IS_SPACES(IN_wch_ret_type_cd), 'N/A', LTRIM(RTRIM(IN_wch_ret_type_cd)))) AS RETURN_TO_WORK_TYPE,
	-- *INF*: IIF(ISNULL(IN_wch_create_ts),TO_DATE('1/1/1800','MM/DD/YYYY'),IN_wch_create_ts)
	-- 
	IFF(IN_wch_create_ts IS NULL, TO_DATE('1/1/1800', 'MM/DD/YYYY'), IN_wch_create_ts) AS wch_create_ts
	FROM EXP_Workers_Comp_Claimant_Work_History
),
LKP_Claim_Party_Occurrence_AK_ID AS (
	SELECT
	claim_party_occurrence_ak_id,
	claimant_num,
	claim_party_role_code
	FROM (
		SELECT CPO.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, 
		LTRIM(RTRIM(CO.claim_occurrence_key)) as claimant_num, 
		LTRIM(RTRIM(CP.claim_party_key)) as claim_party_role_code
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence CPO,
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party CP, 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence CO 
		WHERE CO.claim_occurrence_ak_id = CPO.claim_occurrence_ak_id  AND CP.claim_party_ak_id = CPO.claim_party_ak_id 
		AND CO.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND CP.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND CPO.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND CPO.claim_party_role_code = 'CLMT'
		AND CO.crrnt_snpsht_flag = 1
		AND CP.crrnt_snpsht_flag = 1
		AND CPO.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_num,claim_party_role_code ORDER BY claim_party_occurrence_ak_id DESC) = 1
),
LKP_Workers_Comp_Claimant_Detail AS (
	SELECT
	wc_claimant_det_ak_id,
	claim_party_occurrence_ak_id
	FROM (
		SELECT 
		A.wc_claimant_det_ak_id as wc_claimant_det_ak_id,  
		A.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.workers_comp_claimant_detail A
		WHERE (source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}') AND (CRRNT_SNPSHT_FLAG='1')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id ORDER BY wc_claimant_det_ak_id DESC) = 1
),
LKP_Workers_COMP_CLAIMENT_WORK_HISTORY AS (
	SELECT
	wc_claimant_det_ak_id,
	emp_last_day_worked,
	return_to_work_date,
	return_to_work_with_same_emplyr_ind,
	emplyr_paid_litigated_ind,
	emplyr_paid_amt,
	return_to_work_type,
	wc_claimant_work_hist_ak_id,
	return_to_work_with_restriction_ind,
	work_hist_created_date
	FROM (
		SELECT CPO.wc_claimant_det_ak_id as wc_claimant_det_ak_id,
		CPO.emp_last_day_worked as emp_last_day_worked ,
		CPO.return_to_work_date as return_to_work_date ,
		CPO.return_to_work_with_same_emplyr_ind as return_to_work_with_same_emplyr_ind , 
		CPO.emplyr_paid_litigated_ind as emplyr_paid_litigated_ind ,
		CPO.emplyr_paid_amt as emplyr_paid_amt,
		CPO.return_to_work_type as return_to_work_type,
		CPO.return_to_work_with_restriction_ind as return_to_work_with_restriction_ind ,
		CPO.wc_claimant_work_hist_ak_id as wc_claimant_work_hist_ak_id,
		CPO.work_hist_created_date as work_hist_created_date 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.workers_comp_claimant_work_history CPO  
		WHERE    CPO.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY wc_claimant_det_ak_id,work_hist_created_date ORDER BY wc_claimant_det_ak_id DESC) = 1
),
EXP_DETECT_CHANGES_workers_comp_claimant_detail AS (
	SELECT
	EXP_Lkp_Values_workers_comp_claimant_detail.CWC_CLAIM_NBR,
	EXP_Lkp_Values_workers_comp_claimant_detail.CWC_CLIENT_ID,
	EXP_Lkp_Values_workers_comp_claimant_detail.CWC_OBJECT_SEQ_NBR,
	LKP_Claim_Party_Occurrence_AK_ID.claim_party_occurrence_ak_id AS CLAIM_PARTY_OCCURRENC_AK_ID,
	-- *INF*: IIF(ISNULL(wc_claimant_work_hist_ak_id),'NEW',
	-- IIF(LKP_return_to_work_date <> RETURN_TO_WORK_DATE OR LKP_emplyr_paid_amt <> EMPLYR_PAID_AMT OR ltrim(rtrim(LKP_emplyr_paid_litigated_ind)) <> ltrim(rtrim(EMPLYR_PAID_LITIGATED_IND)) OR  ltrim(rtrim(LKP_return_to_work_with_same_emplyr_ind)) <>  ltrim(rtrim(RETURN_TO_WORK_WITH_SAME_EMPLYR)) OR LKP_emp_last_day_worked <> EMP_LAST_DAY_WORKED  OR ltrim(rtrim(RETURN_TO_WORK_TYPE ))<> ltrim(rtrim(LKP_return_to_work_type))  OR ltrim(rtrim(LKP_return_to_work_with_restriction_ind)) <> ltrim(rtrim( RETURN_TO_WORK_WITH_RESTRICTION_IND))  ,'UPDATE',
	-- 'NOCHANGE') )
	IFF(wc_claimant_work_hist_ak_id IS NULL, 'NEW', IFF(LKP_return_to_work_date <> RETURN_TO_WORK_DATE OR LKP_emplyr_paid_amt <> EMPLYR_PAID_AMT OR ltrim(rtrim(LKP_emplyr_paid_litigated_ind)) <> ltrim(rtrim(EMPLYR_PAID_LITIGATED_IND)) OR ltrim(rtrim(LKP_return_to_work_with_same_emplyr_ind)) <> ltrim(rtrim(RETURN_TO_WORK_WITH_SAME_EMPLYR)) OR LKP_emp_last_day_worked <> EMP_LAST_DAY_WORKED OR ltrim(rtrim(RETURN_TO_WORK_TYPE)) <> ltrim(rtrim(LKP_return_to_work_type)) OR ltrim(rtrim(LKP_return_to_work_with_restriction_ind)) <> ltrim(rtrim(RETURN_TO_WORK_WITH_RESTRICTION_IND)), 'UPDATE', 'NOCHANGE')) AS V_CHANGE_FLAG,
	V_CHANGE_FLAG AS CHANGE_FLAG_OP,
	1 AS CRRNT_SNPSHT_FLAG,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID,
	-- *INF*: IIF(V_CHANGE_FLAG='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	TO_DATE(TO_CHAR(SYSDATE,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS'))
	IFF(V_CHANGE_FLAG = 'NEW', TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE(TO_CHAR(SYSDATE, 'MM/DD/YYYY HH24:MI:SS'), 'MM/DD/YYYY HH24:MI:SS')) AS EFF_FROM_DATE,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS EFF_TO_DATE,
	EXP_Lkp_Values_workers_comp_claimant_detail.SOURCE_SYSTEM_ID,
	SYSDATE AS CREATED_DATE,
	SYSDATE AS MODIFIED_DATE,
	LKP_Workers_Comp_Claimant_Detail.wc_claimant_det_ak_id AS LKP_wc_claimant_det_ak_id,
	LKP_wc_claimant_det_ak_id AS wc_claimant_det_ak_id,
	EXP_Lkp_Values_workers_comp_claimant_detail.EMP_LAST_DAY_WORKED,
	EXP_Lkp_Values_workers_comp_claimant_detail.RETURN_TO_WORK_WITH_RESTRICTION_IND,
	EXP_Lkp_Values_workers_comp_claimant_detail.EMPLYR_PAID_LITIGATED_IND,
	EXP_Lkp_Values_workers_comp_claimant_detail.EMPLYR_PAID_AMT,
	EXP_Lkp_Values_workers_comp_claimant_detail.RETURN_TO_WORK_DATE,
	EXP_Lkp_Values_workers_comp_claimant_detail.RETURN_TO_WORK_WITH_SAME_EMPLYR,
	EXP_Lkp_Values_workers_comp_claimant_detail.RETURN_TO_WORK_TYPE,
	LKP_Workers_COMP_CLAIMENT_WORK_HISTORY.emp_last_day_worked AS LKP_emp_last_day_worked,
	LKP_Workers_COMP_CLAIMENT_WORK_HISTORY.return_to_work_date AS LKP_return_to_work_date,
	LKP_Workers_COMP_CLAIMENT_WORK_HISTORY.return_to_work_with_same_emplyr_ind AS LKP_return_to_work_with_same_emplyr_ind,
	LKP_Workers_COMP_CLAIMENT_WORK_HISTORY.emplyr_paid_litigated_ind AS LKP_emplyr_paid_litigated_ind,
	LKP_Workers_COMP_CLAIMENT_WORK_HISTORY.emplyr_paid_amt AS LKP_emplyr_paid_amt,
	LKP_Workers_COMP_CLAIMENT_WORK_HISTORY.return_to_work_type AS LKP_return_to_work_type,
	LKP_Workers_COMP_CLAIMENT_WORK_HISTORY.return_to_work_with_restriction_ind AS LKP_return_to_work_with_restriction_ind,
	LKP_Workers_COMP_CLAIMENT_WORK_HISTORY.wc_claimant_work_hist_ak_id,
	LKP_Workers_COMP_CLAIMENT_WORK_HISTORY.work_hist_created_date AS LKP_work_hist_created_date,
	EXP_Lkp_Values_workers_comp_claimant_detail.wch_create_ts
	FROM EXP_Lkp_Values_workers_comp_claimant_detail
	LEFT JOIN LKP_Claim_Party_Occurrence_AK_ID
	ON LKP_Claim_Party_Occurrence_AK_ID.claimant_num = EXP_Lkp_Values_workers_comp_claimant_detail.IN_CWC_CLAIM_NBR AND LKP_Claim_Party_Occurrence_AK_ID.claim_party_role_code = EXP_Lkp_Values_workers_comp_claimant_detail.IN_CWC_CLIENT_ID
	LEFT JOIN LKP_Workers_COMP_CLAIMENT_WORK_HISTORY
	ON LKP_Workers_COMP_CLAIMENT_WORK_HISTORY.wc_claimant_det_ak_id = LKP_Workers_Comp_Claimant_Detail.wc_claimant_det_ak_id AND LKP_Workers_COMP_CLAIMENT_WORK_HISTORY.work_hist_created_date = EXP_Lkp_Values_workers_comp_claimant_detail.wch_create_ts
	LEFT JOIN LKP_Workers_Comp_Claimant_Detail
	ON LKP_Workers_Comp_Claimant_Detail.claim_party_occurrence_ak_id = LKP_Claim_Party_Occurrence_AK_ID.claim_party_occurrence_ak_id
),
FIL_INSERT_workers_comp_claimant_detail AS (
	SELECT
	CHANGE_FLAG_OP, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYSTEM_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	wc_claimant_det_ak_id AS LKP_wc_claimant_det_ak_id, 
	RETURN_TO_WORK_WITH_RESTRICTION_IND, 
	EMPLYR_PAID_LITIGATED_IND, 
	EMPLYR_PAID_AMT, 
	EMP_LAST_DAY_WORKED, 
	RETURN_TO_WORK_DATE, 
	RETURN_TO_WORK_WITH_SAME_EMPLYR, 
	RETURN_TO_WORK_TYPE, 
	wc_claimant_work_hist_ak_id, 
	wch_create_ts
	FROM EXP_DETECT_CHANGES_workers_comp_claimant_detail
	WHERE CHANGE_FLAG_OP<>'NOCHANGE'
),
SEQ_Workers_Comp_Claiment_Work_History AS (
	CREATE SEQUENCE SEQ_Workers_Comp_Claiment_Work_History
	START = 0
	INCREMENT = 1;
),
EXP_INSERT AS (
	SELECT
	SEQ_Workers_Comp_Claiment_Work_History.NEXTVAL,
	CHANGE_FLAG_OP,
	CRRNT_SNPSHT_FLAG,
	AUDIT_ID,
	EFF_FROM_DATE,
	EFF_TO_DATE,
	SOURCE_SYSTEM_ID,
	CREATED_DATE,
	MODIFIED_DATE,
	LKP_wc_claimant_det_ak_id,
	-- *INF*: IIF(CHANGE_FLAG_OP='NEW', NEXTVAL, IN_wc_claimant_work_hist_ak_id)
	IFF(CHANGE_FLAG_OP = 'NEW', NEXTVAL, IN_wc_claimant_work_hist_ak_id) AS WC_CLAIMANT_WORK_HIST_AK_ID,
	RETURN_TO_WORK_WITH_RESTRICTION_IND,
	EMPLYR_PAID_LITIGATED_IND,
	EMPLYR_PAID_AMT,
	EMP_LAST_DAY_WORKED,
	RETURN_TO_WORK_DATE,
	RETURN_TO_WORK_WITH_SAME_EMPLYR,
	RETURN_TO_WORK_TYPE,
	wc_claimant_work_hist_ak_id AS IN_wc_claimant_work_hist_ak_id,
	wch_create_ts
	FROM FIL_INSERT_workers_comp_claimant_detail
),
workers_comp_claimant_work_history AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.workers_comp_claimant_work_history
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, wc_claimant_work_hist_ak_id, wc_claimant_det_ak_id, work_hist_created_date, emp_last_day_worked, return_to_work_date, return_to_work_type, return_to_work_with_same_emplyr_ind, return_to_work_with_restriction_ind, emplyr_paid_litigated_ind, emplyr_paid_amt)
	SELECT 
	CRRNT_SNPSHT_FLAG AS CRRNT_SNPSHT_FLAG, 
	AUDIT_ID AS AUDIT_ID, 
	EFF_FROM_DATE AS EFF_FROM_DATE, 
	EFF_TO_DATE AS EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	CREATED_DATE AS CREATED_DATE, 
	MODIFIED_DATE AS MODIFIED_DATE, 
	WC_CLAIMANT_WORK_HIST_AK_ID AS WC_CLAIMANT_WORK_HIST_AK_ID, 
	LKP_wc_claimant_det_ak_id AS WC_CLAIMANT_DET_AK_ID, 
	wch_create_ts AS WORK_HIST_CREATED_DATE, 
	EMP_LAST_DAY_WORKED AS EMP_LAST_DAY_WORKED, 
	RETURN_TO_WORK_DATE AS RETURN_TO_WORK_DATE, 
	RETURN_TO_WORK_TYPE AS RETURN_TO_WORK_TYPE, 
	RETURN_TO_WORK_WITH_SAME_EMPLYR AS RETURN_TO_WORK_WITH_SAME_EMPLYR_IND, 
	RETURN_TO_WORK_WITH_RESTRICTION_IND AS RETURN_TO_WORK_WITH_RESTRICTION_IND, 
	EMPLYR_PAID_LITIGATED_IND AS EMPLYR_PAID_LITIGATED_IND, 
	EMPLYR_PAID_AMT AS EMPLYR_PAID_AMT
	FROM EXP_INSERT
),
SQ_workers_comp_claimant_work_history AS (
	SELECT a.wc_claimant_work_hist_id as wc_claimant_work_hist_id , 
	a.wc_claimant_work_hist_ak_id   as wc_claimant_work_hist_ak_id , 
	a.eff_from_date as eff_from_date,
	 a.eff_to_date as  eff_to_date 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.workers_comp_claimant_work_history a
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.workers_comp_claimant_work_history b
			WHERE b.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND b.crrnt_snpsht_flag = 1
			AND a.wc_claimant_work_hist_ak_id = b.wc_claimant_work_hist_ak_id
			GROUP BY wc_claimant_work_hist_ak_id
			HAVING COUNT(*) > 1)
	ORDER BY wc_claimant_work_hist_ak_id, eff_from_date  DESC
),
EXP_Lag_eff_from_date1 AS (
	SELECT
	wc_claimant_work_hist_id,
	wc_claimant_work_hist_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: IIF(wc_claimant_work_hist_ak_id = v_PREV_ROW_wc_claimant_det_ak_id, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),orig_eff_to_date)
	IFF(wc_claimant_work_hist_ak_id = v_PREV_ROW_wc_claimant_det_ak_id, ADD_TO_DATE(v_PREV_ROW_eff_from_date, 'SS', - 1), orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	wc_claimant_work_hist_ak_id AS v_PREV_ROW_wc_claimant_det_ak_id,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_workers_comp_claimant_work_history
),
FIL_FirstRowInAKGroup AS (
	SELECT
	wc_claimant_work_hist_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Lag_eff_from_date1
	WHERE orig_eff_to_date <> eff_to_date

--If these two dates equal each other we are dealing with the first row in an AK group.  This row
--does not need to be expired or updated for any reason thus it can be filtered out
-- but we must source it to capture the eff_from_date of this row 
--so that we can properly expire the subsequent row
),
UPD_workers_comp_claimant_work_history AS (
	SELECT
	wc_claimant_work_hist_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FIL_FirstRowInAKGroup
),
workers_comp_claimant_work_history2 AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.workers_comp_claimant_work_history AS T
	USING UPD_workers_comp_claimant_work_history AS S
	ON T.wc_claimant_work_hist_id = S.wc_claimant_work_hist_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),
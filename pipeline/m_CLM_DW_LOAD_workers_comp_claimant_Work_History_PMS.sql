WITH
SQ_PIF_42GQ_CMT_stage AS (
	SELECT   
	A.pif_symbol,
	 A.pif_policy_number, 
	A.pif_module,  
	A.ipfcgq_year_of_loss, 
	A.ipfcgq_month_of_loss,
	 A.ipfcgq_day_of_loss, 
	A.ipfcgq_loss_occurence, 
	A.ipfcgq_loss_claimant
	 from  pif_42gq_cmt_stage A        
	where    A.logical_flag  in ('0','1')
	AND  EXISTS(SELECT 'X' 
	FROM PIF_42X6_stage B   
	WHERE  A.pif_policy_number =  B.pif_policy_number
	 AND A.pif_symbol = B.pif_symbol
	AND  A.pif_module =  B.pif_module
	AND B.ipfcx6_insurance_line  = 'WC' )
),
EXP_VALIDATE_workers_comp_claimant_work_history_PMS AS (
	SELECT
	PIF_SYMBOL,
	PIF_POLICY_NUMBER,
	PIF_MODULE,
	IPFCGQ_YEAR_OF_LOSS,
	IPFCGQ_MONTH_OF_LOSS,
	IPFCGQ_DAY_OF_LOSS,
	IPFCGQ_LOSS_OCCURENCE,
	IPFCGQ_LOSS_CLAIMANT
	FROM SQ_PIF_42GQ_CMT_stage
),
EXP_Lkp_Values_workers_comp_claimant_work_history_PMS AS (
	SELECT
	PIF_SYMBOL AS IN_PIF_SYMBOL,
	PIF_POLICY_NUMBER AS IN_PIF_POLICY_NUMBER,
	PIF_MODULE AS IN_PIF_MODULE,
	IPFCGQ_YEAR_OF_LOSS AS IN_IPFCGQ_YEAR_OF_LOSS,
	IPFCGQ_MONTH_OF_LOSS AS IN_IPFCGQ_MONTH_OF_LOSS,
	IPFCGQ_DAY_OF_LOSS AS IN_IPFCGQ_DAY_OF_LOSS,
	IPFCGQ_LOSS_OCCURENCE AS IN_IPFCGQ_LOSS_OCCURENCE,
	IPFCGQ_LOSS_CLAIMANT AS IN_IPFCGQ_LOSS_CLAIMANT,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_PIF_SYMBOL))),'N/A',IIF(IS_SPACES(IN_PIF_SYMBOL),'N/A',LTRIM(RTRIM(IN_PIF_SYMBOL))))
	IFF(LTRIM(RTRIM(IN_PIF_SYMBOL
			)
		) IS NULL,
		'N/A',
		IFF(LENGTH(IN_PIF_SYMBOL)>0 AND TRIM(IN_PIF_SYMBOL)='',
			'N/A',
			LTRIM(RTRIM(IN_PIF_SYMBOL
				)
			)
		)
	) AS V_PIF_SYMBOL,
	V_PIF_SYMBOL AS PIF_SYMBOL,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_PIF_POLICY_NUMBER))),'N/A',IIF(IS_SPACES(IN_PIF_POLICY_NUMBER),'N/A',LTRIM(RTRIM(IN_PIF_POLICY_NUMBER))))
	IFF(LTRIM(RTRIM(IN_PIF_POLICY_NUMBER
			)
		) IS NULL,
		'N/A',
		IFF(LENGTH(IN_PIF_POLICY_NUMBER)>0 AND TRIM(IN_PIF_POLICY_NUMBER)='',
			'N/A',
			LTRIM(RTRIM(IN_PIF_POLICY_NUMBER
				)
			)
		)
	) AS V_PIF_POLICY_NUMBER,
	V_PIF_POLICY_NUMBER AS PIF_POLICY_NUMBER,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_PIF_MODULE))),'N/A',IIF(IS_SPACES(IN_PIF_MODULE),'N/A',LTRIM(RTRIM(IN_PIF_MODULE))))
	IFF(LTRIM(RTRIM(IN_PIF_MODULE
			)
		) IS NULL,
		'N/A',
		IFF(LENGTH(IN_PIF_MODULE)>0 AND TRIM(IN_PIF_MODULE)='',
			'N/A',
			LTRIM(RTRIM(IN_PIF_MODULE
				)
			)
		)
	) AS V_PIF_MODULE,
	V_PIF_MODULE AS PIF_MODULE,
	-- *INF*: IIF(ISNULL(IN_IPFCGQ_YEAR_OF_LOSS),1800,IN_IPFCGQ_YEAR_OF_LOSS)
	IFF(IN_IPFCGQ_YEAR_OF_LOSS IS NULL,
		1800,
		IN_IPFCGQ_YEAR_OF_LOSS
	) AS V_IPFCGQ_YEAR_OF_LOSS,
	V_IPFCGQ_YEAR_OF_LOSS AS IPFCGQ_YEAR_OF_LOSS,
	-- *INF*: IIF(ISNULL(IN_IPFCGQ_MONTH_OF_LOSS),01,IN_IPFCGQ_MONTH_OF_LOSS)
	IFF(IN_IPFCGQ_MONTH_OF_LOSS IS NULL,
		01,
		IN_IPFCGQ_MONTH_OF_LOSS
	) AS V_IPFCGQ_MONTH_OF_LOSS,
	V_IPFCGQ_MONTH_OF_LOSS AS IPFCGQ_MONTH_OF_LOSS,
	-- *INF*: IIF(ISNULL(IN_IPFCGQ_DAY_OF_LOSS),01,IN_IPFCGQ_DAY_OF_LOSS)
	IFF(IN_IPFCGQ_DAY_OF_LOSS IS NULL,
		01,
		IN_IPFCGQ_DAY_OF_LOSS
	) AS V_IPFCGQ_DAY_OF_LOSS,
	V_IPFCGQ_DAY_OF_LOSS AS IPFCGQ_DAY_OF_LOSS,
	-- *INF*: IIF(ISNULL(IN_IPFCGQ_LOSS_OCCURENCE),'000',IN_IPFCGQ_LOSS_OCCURENCE)
	IFF(IN_IPFCGQ_LOSS_OCCURENCE IS NULL,
		'000',
		IN_IPFCGQ_LOSS_OCCURENCE
	) AS V_IPFCGQ_LOSS_OCCURENCE,
	V_IPFCGQ_LOSS_OCCURENCE AS IPFCGQ_LOSS_OCCURENCE,
	-- *INF*: IIF(ISNULL(IN_IPFCGQ_LOSS_CLAIMANT),'000',IN_IPFCGQ_LOSS_CLAIMANT)
	IFF(IN_IPFCGQ_LOSS_CLAIMANT IS NULL,
		'000',
		IN_IPFCGQ_LOSS_CLAIMANT
	) AS V_IPFCGQ_LOSS_CLAIMANT,
	V_IPFCGQ_LOSS_CLAIMANT AS IPFCGQ_LOSS_CLAIMANT,
	-- *INF*: TO_CHAR(V_IPFCGQ_YEAR_OF_LOSS)
	TO_CHAR(V_IPFCGQ_YEAR_OF_LOSS
	) AS V_LOSS_YEAR,
	-- *INF*: TO_CHAR(V_IPFCGQ_MONTH_OF_LOSS)
	TO_CHAR(V_IPFCGQ_MONTH_OF_LOSS
	) AS V_LOSS_MONTH,
	-- *INF*: TO_CHAR(V_IPFCGQ_DAY_OF_LOSS)
	TO_CHAR(V_IPFCGQ_DAY_OF_LOSS
	) AS V_LOSS_DAY,
	-- *INF*: IIF ( LENGTH(V_LOSS_MONTH) = 1, '0' || V_LOSS_MONTH, V_LOSS_MONTH)
	-- ||  
	-- IIF ( LENGTH(V_LOSS_DAY ) = 1, '0' || V_LOSS_DAY, V_LOSS_DAY )
	-- ||  
	-- V_LOSS_YEAR
	IFF(LENGTH(V_LOSS_MONTH
		) = 1,
		'0' || V_LOSS_MONTH,
		V_LOSS_MONTH
	) || IFF(LENGTH(V_LOSS_DAY
		) = 1,
		'0' || V_LOSS_DAY,
		V_LOSS_DAY
	) || V_LOSS_YEAR AS V_LOSS_DATE,
	-- *INF*: V_PIF_SYMBOL || V_PIF_POLICY_NUMBER || V_PIF_MODULE || V_LOSS_DATE || TO_CHAR(V_IPFCGQ_LOSS_OCCURENCE)
	V_PIF_SYMBOL || V_PIF_POLICY_NUMBER || V_PIF_MODULE || V_LOSS_DATE || TO_CHAR(V_IPFCGQ_LOSS_OCCURENCE
	) AS V_OCCURRENCE_KEY,
	V_OCCURRENCE_KEY AS CLAIM_OCCURRENCE_KEY,
	'CMT' AS V_PARTY_ROLE_CODE,
	-- *INF*: V_OCCURRENCE_KEY||TO_CHAR(V_IPFCGQ_LOSS_CLAIMANT)||V_PARTY_ROLE_CODE
	V_OCCURRENCE_KEY || TO_CHAR(V_IPFCGQ_LOSS_CLAIMANT
	) || V_PARTY_ROLE_CODE AS V_LOSS_PARTY_KEY,
	V_LOSS_PARTY_KEY AS CLAIM_PARTY_KEY,
	-- *INF*: TO_DATE('1/1/1800','MM/DD/YYYY')
	TO_DATE('1/1/1800', 'MM/DD/YYYY'
	) AS o_work_hist_created_date
	FROM EXP_VALIDATE_workers_comp_claimant_work_history_PMS
),
LKP_42GQ_WC1 AS (
	SELECT
	ipfcgq_last_day_worked,
	ipfcgq_return_to_work_date,
	pif_symbol,
	pif_policy_number,
	pif_module,
	ipfcgq_year_of_loss,
	ipfcgq_month_of_loss,
	ipfcgq_day_of_loss,
	ipfcgq_loss_occurence,
	ipfcgq_loss_claimant
	FROM (
		select         r.ipfcgq_return_to_work_date   as ipfcgq_return_to_work_date  ,
		       r.ipfcgq_last_day_worked      as  ipfcgq_last_day_worked,
		      r.pif_symbol       as pif_symbol,
		      r.pif_policy_number as pif_policy_number ,
		      r.pif_module  as pif_module,
		       r.ipfcgq_loss_claimant   as    ipfcgq_loss_claimant,
		       r.ipfcgq_loss_occurence as  ipfcgq_loss_occurence, 
		       r.ipfcgq_year_of_loss as ipfcgq_year_of_loss  ,
		       r.ipfcgq_month_of_loss as ipfcgq_month_of_loss,
		       r.ipfcgq_day_of_loss as   ipfcgq_day_of_loss 
		FROM (
		
		select CAST(C.ipfcgq_return_to_work_date  AS datetime) as ipfcgq_return_to_work_date ,
		      CAST(C.ipfcgq_last_day_worked AS datetime) as ipfcgq_last_day_worked,
		      C.pif_symbol as pif_symbol,
		      C.pif_policy_number pif_policy_number,
		      C.pif_module as pif_module,
		      C.IPFCGQ_LOSS_CLAIMANT as ipfcgq_loss_claimant,
		      C.IPFCGQ_LOSS_OCCURENCE as ipfcgq_loss_occurence,
		      C.IPFCGQ_YEAR_OF_LOSS as ipfcgq_year_of_loss,
		        C.IPFCGQ_MONTH_OF_LOSS as ipfcgq_month_of_loss,
		          C.IPFCGQ_DAY_OF_LOSS as ipfcgq_day_of_loss  
		FROM  PIF_42GQ_WC1_stage  C  
		) r
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module,ipfcgq_year_of_loss,ipfcgq_month_of_loss,ipfcgq_day_of_loss,ipfcgq_loss_occurence,ipfcgq_loss_claimant ORDER BY ipfcgq_last_day_worked DESC) = 1
),
LKP_Claim_Party_Occurrence_AK_ID AS (
	SELECT
	claim_party_occurrence_ak_id,
	claimant_num,
	claim_party_role_code
	FROM (
		SELECT CPO.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, 
		---CO.claim_occurrence_type_code as offset_onset_ind,
		LTRIM(RTRIM(CO.claim_occurrence_key)) as claimant_num, 
		LTRIM(RTRIM(CP.claim_party_key)) as claim_party_role_code
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence CPO,
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party CP, 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence CO 
		WHERE CO.claim_occurrence_ak_id = CPO.claim_occurrence_ak_id  
		AND CP.claim_party_ak_id = CPO.claim_party_ak_id 
		AND CO.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND CP.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND CPO.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND CPO.claim_party_role_code = 'CMT'
		AND CO.crrnt_snpsht_flag = 1
		AND CP.crrnt_snpsht_flag = 1
		AND CPO.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_num,claim_party_role_code ORDER BY claim_party_occurrence_ak_id DESC) = 1
),
LKP_workers_comp_claimant_detail AS (
	SELECT
	wc_claimant_det_ak_id,
	claim_party_occurrence_ak_id
	FROM (
		SELECT workers_comp_claimant_detail.wc_claimant_det_ak_id as wc_claimant_det_ak_id,           
		               workers_comp_claimant_detail.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id 
		FROM workers_comp_claimant_detail
		WHERE (source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}') AND (CRRNT_SNPSHT_FLAG='1')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id ORDER BY wc_claimant_det_ak_id DESC) = 1
),
LKP_WORK_HISTORY AS (
	SELECT
	wc_claimant_work_hist_ak_id,
	wc_claimant_det_ak_id,
	emp_last_day_worked,
	return_to_work_date,
	return_to_work_type,
	return_to_work_with_same_emplyr_ind,
	return_to_work_with_restriction_ind,
	emplyr_paid_litigated_ind,
	emplyr_paid_amt,
	work_hist_created_date,
	source_sys_id,
	IN_work_hist_created_date
	FROM (
		SELECT wh.wc_claimant_work_hist_ak_id as wc_claimant_work_hist_ak_id, 
		       wh.wc_claimant_det_ak_id    as wc_claimant_det_ak_id,
		       wh.emp_last_day_worked      as emp_last_day_worked,   
		       wh.return_to_work_date      as return_to_work_date,
		       wh.return_to_work_type      as return_to_work_type,
		       wh.return_to_work_with_same_emplyr_ind      as return_to_work_with_same_emplyr_ind,
		       wh.return_to_work_with_restriction_ind      as return_to_work_with_restriction_ind,
		       wh.emplyr_paid_litigated_ind      as emplyr_paid_litigated_ind,
		       wh.emplyr_paid_amt      as emplyr_paid_amt,
		       wh.work_hist_created_date  as work_hist_created_date,
		       wh.source_sys_id  as source_sys_id       
		FROM workers_comp_claimant_work_history wh
		WHERE (wh.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}') AND (wh.CRRNT_SNPSHT_FLAG='1')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY wc_claimant_det_ak_id,work_hist_created_date ORDER BY wc_claimant_work_hist_ak_id DESC) = 1
),
EXP_DETECT_CHANGES_workers_comp_claimant_work_history AS (
	SELECT
	EXP_Lkp_Values_workers_comp_claimant_work_history_PMS.PIF_SYMBOL,
	LKP_WORK_HISTORY.work_hist_created_date AS LKP_work_hist_created_date,
	LKP_WORK_HISTORY.wc_claimant_work_hist_ak_id AS LKP_WC_CLAIMANT_WORK_HIST_AK_ID,
	LKP_WORK_HISTORY.wc_claimant_det_ak_id AS LKP_WC_CLAIMANT_DET_AK_ID,
	LKP_WORK_HISTORY.emp_last_day_worked AS LKP_EMP_LAST_DAY_WORKED,
	LKP_WORK_HISTORY.return_to_work_date AS LKP_RETURN_TO_WORK_DATE,
	LKP_42GQ_WC1.ipfcgq_last_day_worked AS IN_EMP_LAST_DAY_WORKED,
	-- *INF*:   IIF( ISNULL(IN_EMP_LAST_DAY_WORKED) OR  IN_EMP_LAST_DAY_WORKED =TO_DATE('1/1/1900','MM/DD/YYYY')  ,TO_DATE('1/1/1800','MM/DD/YYYY'), IN_EMP_LAST_DAY_WORKED)
	IFF(IN_EMP_LAST_DAY_WORKED IS NULL 
		OR IN_EMP_LAST_DAY_WORKED = TO_DATE('1/1/1900', 'MM/DD/YYYY'
		),
		TO_DATE('1/1/1800', 'MM/DD/YYYY'
		),
		IN_EMP_LAST_DAY_WORKED
	) AS V_EMP_LAST_DAY_WORKED_STR,
	LKP_WORK_HISTORY.return_to_work_type AS LKP_return_to_work_type,
	LKP_WORK_HISTORY.emplyr_paid_amt AS LKP_emplyr_paid_amt,
	LKP_WORK_HISTORY.return_to_work_with_same_emplyr_ind AS LKP_return_to_work_with_same_emplyr_ind,
	LKP_WORK_HISTORY.source_sys_id AS LKP_source_sys_id,
	LKP_WORK_HISTORY.return_to_work_with_restriction_ind AS LKP_return_to_work_with_restriction_ind,
	LKP_WORK_HISTORY.emplyr_paid_litigated_ind AS LKP_emplyr_paid_litigated_ind,
	V_EMP_LAST_DAY_WORKED_STR AS o_emp_last_day_worked,
	LKP_42GQ_WC1.ipfcgq_return_to_work_date AS IN_RETURN_TO_WORK_DATE,
	-- *INF*:  IIF( ISNULL(IN_RETURN_TO_WORK_DATE)  OR  IN_RETURN_TO_WORK_DATE =TO_DATE('1/1/1900','MM/DD/YYYY')    ,TO_DATE('1/1/1800','MM/DD/YYYY'), IN_RETURN_TO_WORK_DATE)
	--  
	-- 
	IFF(IN_RETURN_TO_WORK_DATE IS NULL 
		OR IN_RETURN_TO_WORK_DATE = TO_DATE('1/1/1900', 'MM/DD/YYYY'
		),
		TO_DATE('1/1/1800', 'MM/DD/YYYY'
		),
		IN_RETURN_TO_WORK_DATE
	) AS V_RETURN_TO_WORK_DATE_STR,
	V_RETURN_TO_WORK_DATE_STR AS o_return_to_work_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS v_source_sys_id,
	'N/A' AS v_return_to_work_type,
	v_return_to_work_type AS o_return_to_work_type,
	'N/A' AS v_return_to_work_with_same_emplyr_ind,
	v_return_to_work_with_same_emplyr_ind AS o_return_to_work_with_same_emplyr_ind,
	'N/A' AS v_return_to_work_with_restriction_ind,
	v_return_to_work_with_restriction_ind AS o_return_to_work_with_restriction_ind,
	'N/A' AS v_emplyr_paid_litigated_ind,
	v_emplyr_paid_litigated_ind AS o_emplyr_paid_litigated_ind,
	0 AS v_emplyr_paid_amt,
	v_emplyr_paid_amt AS o_emplyr_paid_amt,
	SYSDATE AS v_sysdate,
	-- *INF*: IIF(ISNULL(LKP_WC_CLAIMANT_WORK_HIST_AK_ID),'NEW',
	-- IIF(  (
	-- LKP_EMP_LAST_DAY_WORKED <> V_EMP_LAST_DAY_WORKED_STR OR 
	-- LKP_RETURN_TO_WORK_DATE <> V_RETURN_TO_WORK_DATE_STR  or LKP_return_to_work_type <> v_return_to_work_type or LKP_return_to_work_with_same_emplyr_ind <> v_return_to_work_with_same_emplyr_ind or LKP_return_to_work_with_restriction_ind <> v_return_to_work_with_restriction_ind or v_emplyr_paid_litigated_ind <> LKP_emplyr_paid_litigated_ind or LKP_emplyr_paid_amt <> v_emplyr_paid_amt  or LKP_source_sys_id <>  v_source_sys_id),
	-- 'UPDATE','NOCHANGE'))
	IFF(LKP_WC_CLAIMANT_WORK_HIST_AK_ID IS NULL,
		'NEW',
		IFF(( LKP_EMP_LAST_DAY_WORKED <> V_EMP_LAST_DAY_WORKED_STR 
				OR LKP_RETURN_TO_WORK_DATE <> V_RETURN_TO_WORK_DATE_STR 
				OR LKP_return_to_work_type <> v_return_to_work_type 
				OR LKP_return_to_work_with_same_emplyr_ind <> v_return_to_work_with_same_emplyr_ind 
				OR LKP_return_to_work_with_restriction_ind <> v_return_to_work_with_restriction_ind 
				OR v_emplyr_paid_litigated_ind <> LKP_emplyr_paid_litigated_ind 
				OR LKP_emplyr_paid_amt <> v_emplyr_paid_amt 
				OR LKP_source_sys_id <> v_source_sys_id 
			),
			'UPDATE',
			'NOCHANGE'
		)
	) AS V_CHANGE_FLAG,
	V_CHANGE_FLAG AS CHANGE_FLAG_OP,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: IIF(V_CHANGE_FLAG='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	TO_DATE(TO_CHAR(SYSDATE,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS'))
	IFF(V_CHANGE_FLAG = 'NEW',
		TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		TO_DATE(TO_CHAR(SYSDATE, 'MM/DD/YYYY HH24:MI:SS'
			), 'MM/DD/YYYY HH24:MI:SS'
		)
	) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	v_sysdate AS created_date,
	v_sysdate AS modified_date,
	LKP_workers_comp_claimant_detail.wc_claimant_det_ak_id
	FROM EXP_Lkp_Values_workers_comp_claimant_work_history_PMS
	LEFT JOIN LKP_42GQ_WC1
	ON LKP_42GQ_WC1.pif_symbol = EXP_Lkp_Values_workers_comp_claimant_work_history_PMS.PIF_SYMBOL AND LKP_42GQ_WC1.pif_policy_number = EXP_Lkp_Values_workers_comp_claimant_work_history_PMS.PIF_POLICY_NUMBER AND LKP_42GQ_WC1.pif_module = EXP_Lkp_Values_workers_comp_claimant_work_history_PMS.PIF_MODULE AND LKP_42GQ_WC1.ipfcgq_year_of_loss = EXP_Lkp_Values_workers_comp_claimant_work_history_PMS.IPFCGQ_YEAR_OF_LOSS AND LKP_42GQ_WC1.ipfcgq_month_of_loss = EXP_Lkp_Values_workers_comp_claimant_work_history_PMS.IPFCGQ_MONTH_OF_LOSS AND LKP_42GQ_WC1.ipfcgq_day_of_loss = EXP_Lkp_Values_workers_comp_claimant_work_history_PMS.IPFCGQ_DAY_OF_LOSS AND LKP_42GQ_WC1.ipfcgq_loss_occurence = EXP_Lkp_Values_workers_comp_claimant_work_history_PMS.IPFCGQ_LOSS_OCCURENCE AND LKP_42GQ_WC1.ipfcgq_loss_claimant = EXP_Lkp_Values_workers_comp_claimant_work_history_PMS.IPFCGQ_LOSS_CLAIMANT
	LEFT JOIN LKP_WORK_HISTORY
	ON LKP_WORK_HISTORY.wc_claimant_det_ak_id = LKP_workers_comp_claimant_detail.wc_claimant_det_ak_id AND LKP_WORK_HISTORY.work_hist_created_date = EXP_Lkp_Values_workers_comp_claimant_work_history_PMS.o_work_hist_created_date
	LEFT JOIN LKP_workers_comp_claimant_detail
	ON LKP_workers_comp_claimant_detail.claim_party_occurrence_ak_id = LKP_Claim_Party_Occurrence_AK_ID.claim_party_occurrence_ak_id
),
FIL_INSERT_workers_comp_claimant_work_history_PMS AS (
	SELECT
	wc_claimant_det_ak_id, 
	CHANGE_FLAG_OP, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date, 
	o_return_to_work_type, 
	o_return_to_work_with_same_emplyr_ind, 
	o_return_to_work_with_restriction_ind, 
	o_emplyr_paid_litigated_ind, 
	o_emplyr_paid_amt, 
	o_return_to_work_date, 
	o_emp_last_day_worked, 
	LKP_WC_CLAIMANT_WORK_HIST_AK_ID AS WC_CLAIMANT_WORK_HIST_AK_ID
	FROM EXP_DETECT_CHANGES_workers_comp_claimant_work_history
	WHERE CHANGE_FLAG_OP<>'NOCHANGE'
),
SEQ_Workers_Comp_Claiment_Work_History AS (
	CREATE SEQUENCE SEQ_Workers_Comp_Claiment_Work_History
	START = 0
	INCREMENT = 1;
),
EXP_INSERT AS (
	SELECT
	wc_claimant_det_ak_id AS wc_claimant_det_ak_id_IN,
	1+ v_cntr AS v_cntr,
	wc_claimant_det_ak_id_IN AS o_wc_claimant_det_ak_id,
	CHANGE_FLAG_OP,
	SEQ_Workers_Comp_Claiment_Work_History.NEXTVAL,
	-- *INF*: IIF(CHANGE_FLAG_OP='NEW', NEXTVAL, WC_CLAIMANT_WORK_HIST_AK_ID)
	IFF(CHANGE_FLAG_OP = 'NEW',
		NEXTVAL,
		WC_CLAIMANT_WORK_HIST_AK_ID
	) AS o_wc_claimant_work_hist_ak_id,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	WC_CLAIMANT_WORK_HIST_AK_ID,
	-- *INF*: TO_DATE('1/1/1800','MM/DD/YYYY')
	TO_DATE('1/1/1800', 'MM/DD/YYYY'
	) AS o_work_hist_created_date,
	o_return_to_work_date,
	o_emp_last_day_worked,
	o_return_to_work_type,
	o_return_to_work_with_same_emplyr_ind,
	o_return_to_work_with_restriction_ind,
	o_emplyr_paid_litigated_ind,
	o_emplyr_paid_amt
	FROM FIL_INSERT_workers_comp_claimant_work_history_PMS
),
workers_comp_claimant_work_history_insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.workers_comp_claimant_work_history
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, wc_claimant_work_hist_ak_id, wc_claimant_det_ak_id, work_hist_created_date, emp_last_day_worked, return_to_work_date, return_to_work_type, return_to_work_with_same_emplyr_ind, return_to_work_with_restriction_ind, emplyr_paid_litigated_ind, emplyr_paid_amt)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	o_wc_claimant_work_hist_ak_id AS WC_CLAIMANT_WORK_HIST_AK_ID, 
	o_wc_claimant_det_ak_id AS WC_CLAIMANT_DET_AK_ID, 
	o_work_hist_created_date AS WORK_HIST_CREATED_DATE, 
	o_emp_last_day_worked AS EMP_LAST_DAY_WORKED, 
	o_return_to_work_date AS RETURN_TO_WORK_DATE, 
	o_return_to_work_type AS RETURN_TO_WORK_TYPE, 
	o_return_to_work_with_same_emplyr_ind AS RETURN_TO_WORK_WITH_SAME_EMPLYR_IND, 
	o_return_to_work_with_restriction_ind AS RETURN_TO_WORK_WITH_RESTRICTION_IND, 
	o_emplyr_paid_litigated_ind AS EMPLYR_PAID_LITIGATED_IND, 
	o_emplyr_paid_amt AS EMPLYR_PAID_AMT
	FROM EXP_INSERT
),
SQ_workers_comp_claimant_work_history AS (
	SELECT a.wc_claimant_work_hist_id, a.wc_claimant_work_hist_ak_id, a.eff_from_date, a.eff_to_date 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.workers_comp_claimant_work_history a
	WHERE EXISTS(SELECT 1			
			FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.workers_comp_claimant_work_history b
			WHERE b.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND b.crrnt_snpsht_flag = 1
			AND a.wc_claimant_work_hist_ak_id = b.wc_claimant_work_hist_ak_id
			GROUP BY wc_claimant_work_hist_ak_id
			HAVING COUNT(*) > 1)
	ORDER BY wc_claimant_work_hist_ak_id, eff_from_date  DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	wc_claimant_work_hist_id,
	audit_id AS wc_claimant_work_hist_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: IIF(wc_claimant_work_hist_ak_id = v_PREV_ROW_wc_claimant_det_ak_id, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),orig_eff_to_date)
	IFF(wc_claimant_work_hist_ak_id = v_PREV_ROW_wc_claimant_det_ak_id,
		DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
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
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <> eff_to_date

--If these two dates equal each other we are dealing with the first row in an AK group.  This row
--does not need to be expired or updated for any reason thus it can be filtered out
-- but we must source it to capture the eff_from_date of this row 
--so that we can properly expire the subsequent row
),
UPD_workers_comp_claimant_work_historyl_PMS AS (
	SELECT
	wc_claimant_work_hist_id AS wc_claimant_work_history_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FIL_FirstRowInAKGroup
),
workers_comp_claimant_work_history_update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.workers_comp_claimant_work_history AS T
	USING UPD_workers_comp_claimant_work_historyl_PMS AS S
	ON T.wc_claimant_work_hist_id = S.wc_claimant_work_history_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),
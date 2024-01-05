WITH
LKP_claim_occurrence AS (
	SELECT
	claim_occurrence_ak_id,
	claim_occurrence_key,
	in_claim_occurrence_key
	FROM (
		SELECT 
		claim_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id, 
		rtrim(claim_occurrence.claim_occurrence_key) as claim_occurrence_key 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence claim_occurrence
		WHERE
		crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_key ORDER BY claim_occurrence_ak_id) = 1
),
SQ_med_bill_stage AS (
	SELECT 
	rtrim(med_bill_stage.med_bill_id), 
	CASE rtrim(med_bill_stage.vendor_bill_num) when '' then 'N/A' ELSE rtrim(med_bill_stage.vendor_bill_num) END, 
	CASE rtrim(med_bill_stage.pt_acct_num) WHEN '' THEN 'N/A' ELSE rtrim(med_bill_stage.pt_acct_num) END,
	CASE rtrim(med_bill_stage.pt_last_name) WHEN '' THEN 'N/A' ELSE rtrim(med_bill_stage.pt_last_name) END ,
	CASE rtrim(med_bill_stage.pt_first_name) WHEN '' THEN 'N/A' ELSE rtrim(med_bill_stage.pt_first_name) END,
	CASE rtrim(med_bill_stage.pt_mid_name) WHEN '' THEN 'N/A' ELSE rtrim(med_bill_stage.pt_mid_name) END,
	CASE rtrim(med_bill_stage.pt_addr) WHEN '' THEN 'N/A' ELSE rtrim(med_bill_stage.pt_addr) END,
	CASE rtrim(med_bill_stage.pt_city) WHEN '' THEN 'N/A' ELSE rtrim(med_bill_stage.pt_city) END ,
	CASE rtrim(med_bill_stage.pt_state) WHEN '' THEN 'N/A' ELSE rtrim(med_bill_stage.pt_state) END,
	CASE  rtrim(med_bill_stage.pt_zip_code) WHEN '' THEN 'N/A' ELSE rtrim(med_bill_stage.pt_zip_code) END,
	med_bill_stage.pt_dob  ,
	CASE rtrim(med_bill_stage.pt_gndr) WHEN '' THEN 'N/A' ELSE rtrim(med_bill_stage.pt_gndr) END,
	CASE rtrim(med_bill_stage.pt_ssn) WHEN '' THEN 'N/A' ELSE rtrim(med_bill_stage.pt_ssn) END,
	med_bill_stage.pt_inj_dt,
	CASE rtrim(med_bill_stage.refer_physician) WHEN '' THEN 'N/A' ELSE rtrim(med_bill_stage.refer_physician) END,
	med_bill_stage.serv_from_date,
	med_bill_stage.serv_to_date ,
	CASE rtrim(med_bill_stage.inpt_outpt_ind) WHEN '' THEN 'N/A' ELSE rtrim(med_bill_stage.inpt_outpt_ind) END,
	med_bill_stage.bill_issued_date,
	med_bill_stage.bill_rcvd_date,
	med_bill_stage.bus_rcvd_date,
	med_bill_stage.bill_process_date,
	med_bill_stage.pt_admit_date,
	med_bill_stage.pt_discharge_date ,
	CASE med_bill_stage.daily_hospital_rt WHEN NULL THEN 0 ELSE med_bill_stage.daily_hospital_rt END ,
	CASE med_bill_stage.bill_review_cost WHEN NULL THEN 0 ELSE med_bill_stage.bill_review_cost  END,
	CASE med_bill_stage.total_bill_charge WHEN NULL THEN 0 ELSE med_bill_stage.total_bill_charge  END,
	CASE med_bill_stage.total_bill_red WHEN NULL THEN 0 ELSE med_bill_stage.total_bill_red  END,
	CASE med_bill_stage.total_network_red WHEN NULL THEN 0 ELSE med_bill_stage.total_network_red  END,
	CASE med_bill_stage.total_recom_pay WHEN NULL THEN 0 ELSE med_bill_stage.total_recom_pay  END,
	CASE med_bill_stage.total_addtl_charge WHEN NULL THEN 0 ELSE med_bill_stage.total_addtl_charge  END,
	CASE rtrim(med_bill_stage.bill_type) WHEN '' THEN 'N/A' ELSE rtrim(med_bill_stage.bill_type) END ,
	CASE rtrim(med_bill_stage.bill_status_code) WHEN '' THEN 'N/A' ELSE rtrim(med_bill_stage.bill_status_code) END ,
	CASE rtrim(med_bill_stage.fee_sched_code) WHEN '' THEN 'N/A' ELSE rtrim(med_bill_stage.fee_sched_code) END,
	CASE rtrim(med_bill_stage.network_name) WHEN '' THEN 'N/A' ELSE rtrim(med_bill_stage.network_name) END,
	CASE rtrim(med_bill_stage.network_num) WHEN '' THEN 'N/A' ELSE rtrim(med_bill_stage.network_num) END,
	CASE med_bill_stage.serv_line_num WHEN NULL THEN 0 ELSE med_bill_stage.serv_line_num END,
	CASE rtrim(med_bill_stage.deleted_ind) WHEN '' THEN 'N/A' ELSE rtrim(med_bill_stage.deleted_ind) END,
	CASE rtrim(med_bill_stage.emplyr) WHEN '' THEN 'N/A' ELSE rtrim(med_bill_stage.emplyr) END,
	CASE rtrim(med_bill_stage.acct_id) WHEN '' THEN 'N/A' ELSE rtrim(med_bill_stage.acct_id) END,
	CASE rtrim(med_bill_stage.vendor_code) WHEN '' THEN 'N/A' ELSE rtrim(med_bill_stage.vendor_code) END,
	CASE rtrim(med_bill_stage.ebill_ind) WHEN '' THEN 'N/A' ELSE rtrim(med_bill_stage.ebill_ind) END,
	CASE rtrim(med_bill_stage.autopay_ind) WHEN '' THEN 'N/A' ELSE rtrim(med_bill_stage.autopay_ind) END,
	med_bill_stage.eor_rcvd_date, 
	med_bill_stage.original_vendor_bill_num,
	CASE RTRIM(auto_adjudicated) WHEN '' THEN 'N/A' ELSE RTRIM(auto_adjudicated) END 
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.med_bill_stage 
	order by med_bill_stage.med_bill_id
),
LKP_clm_clt_eor_stage AS (
	SELECT
	in_cce_tch_bill_nbr,
	cce_claim_nbr,
	cce_client_id,
	cce_eor_status,
	denial_reason_cd,
	cce_draft_nbr,
	cce_tch_bill_nbr
	FROM (
		SELECT	RTRIM(cce_claim_nbr) as cce_claim_nbr
		,		RTRIM(cce_client_id) as cce_client_id 
		,		RTRIM(cce_eor_status) as cce_eor_status
		,		RTRIM(denial_reason_cd) as denial_reason_cd
		,		RTRIM(cce_draft_nbr) as cce_draft_nbr
		,		RTRIM(cce_tch_bill_nbr) as cce_tch_bill_nbr 
		FROM clm_clt_eor_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY cce_tch_bill_nbr ORDER BY in_cce_tch_bill_nbr) = 1
),
LKP_claim_party_occurrence AS (
	SELECT
	claim_party_occurrence_ak_id,
	claim_party_key,
	claim_occurrence_key
	FROM (
		SELECT	CPO.claim_party_occurrence_ak_id	AS claim_party_occurrence_ak_id
		,		RTRIM(CP.claim_party_key)			AS claim_party_key
		,		RTRIM(CO.claim_occurrence_key)		AS claim_occurrence_key
		FROM	claim_party_occurrence				CPO
		INNER	JOIN vw_claim_party1				CP
			ON	CP.claim_party_ak_id				= CPO.claim_party_ak_id
		INNER	JOIN claim_occurrence				CO
			ON	CO.claim_occurrence_ak_id			= CPO.claim_occurrence_ak_id
		WHERE	CPO.crrnt_snpsht_flag = 1
		AND		CP.crrnt_snpsht_flag = 1
		AND		CO.crrnt_snpsht_flag = 1
		AND		CPO.claim_party_role_code = 'CLMT'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_key,claim_occurrence_key ORDER BY claim_party_occurrence_ak_id) = 1
),
LKP_claim_payment AS (
	SELECT
	micro_ecd_draft_num,
	total_pay_amt,
	pay_disbursement_date,
	claim_pay_num
	FROM (
		SELECT 
		claim_payment.micro_ecd_draft_num as micro_ecd_draft_num, 
		claim_payment.total_pay_amt as total_pay_amt, 
		claim_payment.pay_disbursement_date as pay_disbursement_date, 
		rtrim(claim_payment.claim_pay_num) as claim_pay_num 
		FROM claim_payment
		WHERE source_sys_Id = 'EXCEED'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_pay_num ORDER BY micro_ecd_draft_num DESC) = 1
),
LKP_medical_bill_vendor AS (
	SELECT
	med_bill_vendor_ak_id,
	vendor_code
	FROM (
		SELECT	med_bill_vendor_ak_id AS med_bill_vendor_ak_id
		,		vendor_code AS vendor_code
		FROM	medical_bill_vendor
		WHERE	crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY vendor_code ORDER BY med_bill_vendor_ak_id) = 1
),
LKP_pms_clt_eor_stage AS (
	SELECT
	pce_policy_sym,
	pce_policy_num,
	pce_policy_mod,
	pce_date_of_loss,
	pce_occurrence,
	pce_paid_ts,
	check_number,
	amount_paid_by_chk,
	denial_reason_cd,
	pce_tch_bill_nbr
	FROM (
		SELECT pms_clt_eor_stage.pce_policy_sym as pce_policy_sym, pms_clt_eor_stage.pce_policy_num as pce_policy_num, pms_clt_eor_stage.pce_policy_mod as pce_policy_mod, pms_clt_eor_stage.pce_date_of_loss as pce_date_of_loss, pms_clt_eor_stage.pce_occurrence as pce_occurrence, pms_clt_eor_stage.pce_paid_ts as pce_paid_ts, pms_clt_eor_stage.check_number as check_number, pms_clt_eor_stage.amount_paid_by_chk as amount_paid_by_chk, pms_clt_eor_stage.denial_reason_cd as denial_reason_cd, 
		rtrim(pms_clt_eor_stage.pce_tch_bill_nbr) as pce_tch_bill_nbr 
		FROM pms_clt_eor_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pce_tch_bill_nbr ORDER BY pce_policy_sym) = 1
),
EXP_ASSIGN_DEFAULTS AS (
	SELECT
	SQ_med_bill_stage.med_bill_id,
	vendor_bill_num AS vendor_bill_num_out,
	pt_acct_num AS pt_acct_num_out,
	pt_last_name AS pt_last_name_out,
	pt_first_name AS pt_first_name_out,
	pt_mid_name AS pt_mid_name_out,
	-- *INF*: IIF(pt_first_name='N/A','', ' '  || pt_first_name) || 
	-- IIF(pt_mid_name='N/A','', ' '  ||  pt_mid_name) || 
	-- IIF(pt_last_name='N/A','', ' '  || pt_last_name) 
	IFF(pt_first_name = 'N/A', '', ' ' || pt_first_name) || IFF(pt_mid_name = 'N/A', '', ' ' || pt_mid_name) || IFF(pt_last_name = 'N/A', '', ' ' || pt_last_name) AS out_pt_full_name,
	pt_addr AS pt_addr_out,
	pt_city AS pt_city_out,
	pt_state AS pt_state_out,
	pt_zip_code AS pt_zip_code_out,
	pt_dob AS pt_dob_out,
	pt_gndr AS pt_gndr_out,
	pt_ssn AS pt_ssn_out,
	pt_inj_dt AS pt_inj_dt_out,
	refer_physician AS refer_physician_out,
	serv_from_date AS serv_from_date_out,
	serv_to_date AS serv_to_date_out,
	inpt_outpt_ind AS inpt_outpt_ind_out,
	bill_issued_date AS bill_issued_date_out,
	bill_rcvd_date AS bill_rcvd_date_out,
	bus_rcvd_date AS bus_rcvd_date_out,
	bill_process_date AS bill_process_date_out,
	pt_admit_date AS pt_admit_date_out,
	pt_discharge_date AS pt_discharge_date_out,
	daily_hospital_rt AS daily_hospital_rt_out,
	bill_review_cost AS bill_review_cost_out,
	total_bill_charge AS total_bill_charge_out,
	total_bill_red AS total_bill_red_out,
	total_network_red AS total_network_red_out,
	total_recom_pay AS total_recom_pay_out,
	total_addtl_charge AS total_addtl_charge_out,
	bill_type AS bill_type_out,
	bill_status_code AS bill_status_code_out,
	fee_sched_code AS fee_sched_code_out,
	network_name AS network_name_out,
	network_num AS network_num_out,
	serv_line_num AS serv_line_num_out,
	deleted_ind AS deleted_ind_out,
	emplyr AS emplyr_out,
	acct_id AS acct_id_out,
	vendor_code AS vendor_code_out,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	SQ_med_bill_stage.vendor_bill_num,
	SQ_med_bill_stage.pt_acct_num,
	SQ_med_bill_stage.pt_last_name,
	SQ_med_bill_stage.pt_first_name,
	SQ_med_bill_stage.pt_mid_name,
	SQ_med_bill_stage.pt_addr,
	SQ_med_bill_stage.pt_city,
	SQ_med_bill_stage.pt_state,
	SQ_med_bill_stage.pt_zip_code,
	SQ_med_bill_stage.pt_dob,
	SQ_med_bill_stage.pt_gndr,
	SQ_med_bill_stage.pt_ssn,
	SQ_med_bill_stage.pt_inj_dt,
	SQ_med_bill_stage.refer_physician,
	SQ_med_bill_stage.serv_from_date,
	SQ_med_bill_stage.serv_to_date,
	SQ_med_bill_stage.inpt_outpt_ind,
	SQ_med_bill_stage.bill_issued_date,
	SQ_med_bill_stage.bill_rcvd_date,
	SQ_med_bill_stage.bus_rcvd_date,
	SQ_med_bill_stage.bill_process_date,
	SQ_med_bill_stage.pt_admit_date,
	SQ_med_bill_stage.pt_discharge_date,
	SQ_med_bill_stage.daily_hospital_rt,
	SQ_med_bill_stage.bill_review_cost,
	SQ_med_bill_stage.total_bill_charge,
	SQ_med_bill_stage.total_bill_red,
	SQ_med_bill_stage.total_network_red,
	SQ_med_bill_stage.total_recom_pay,
	SQ_med_bill_stage.total_addtl_charge,
	SQ_med_bill_stage.bill_type,
	SQ_med_bill_stage.bill_status_code,
	SQ_med_bill_stage.fee_sched_code,
	SQ_med_bill_stage.network_name,
	SQ_med_bill_stage.network_num,
	SQ_med_bill_stage.serv_line_num,
	SQ_med_bill_stage.deleted_ind,
	SQ_med_bill_stage.emplyr,
	SQ_med_bill_stage.acct_id,
	SQ_med_bill_stage.vendor_code,
	SQ_med_bill_stage.ebill_ind,
	SQ_med_bill_stage.autopay_ind AS in_autopay_ind,
	LKP_clm_clt_eor_stage.cce_claim_nbr AS EXD_claim_num,
	LKP_clm_clt_eor_stage.denial_reason_cd AS EXD_denial_reason_cd,
	LKP_claim_payment.micro_ecd_draft_num AS EXD_draft_num,
	LKP_claim_payment.total_pay_amt AS EXD_draft_amt,
	LKP_claim_payment.pay_disbursement_date AS EXD_paid_date,
	LKP_pms_clt_eor_stage.pce_policy_sym,
	LKP_pms_clt_eor_stage.pce_policy_num,
	LKP_pms_clt_eor_stage.pce_policy_mod,
	LKP_pms_clt_eor_stage.pce_date_of_loss,
	LKP_pms_clt_eor_stage.pce_occurrence,
	-- *INF*: replaceChr(0,to_char(pce_date_of_loss),'/','')
	replaceChr(0, to_char(pce_date_of_loss), '/', '') AS v_PMS_date_of_loss,
	-- *INF*: pce_policy_sym || pce_policy_num || pce_policy_mod ||  
	-- v_PMS_date_of_loss 
	-- || lpad(to_char(pce_occurrence),3,'0')
	-- 
	pce_policy_sym || pce_policy_num || pce_policy_mod || v_PMS_date_of_loss || lpad(to_char(pce_occurrence), 3, '0') AS v_PMS_claim_num,
	LKP_pms_clt_eor_stage.pce_paid_ts AS PMS_paid_date,
	LKP_pms_clt_eor_stage.check_number AS PMS_draft_num,
	LKP_pms_clt_eor_stage.amount_paid_by_chk AS PMS_draft_amt,
	LKP_pms_clt_eor_stage.denial_reason_cd AS PMS_denial_reason_cd,
	-- *INF*: IIF(NOT ISNULL(EXD_claim_num), EXD_claim_num,
	-- IIF(NOT ISNULL(v_PMS_claim_num), v_PMS_claim_num,
	-- 'N/A'))
	IFF(NOT EXD_claim_num IS NULL, EXD_claim_num, IFF(NOT v_PMS_claim_num IS NULL, v_PMS_claim_num, 'N/A')) AS v_claim_num,
	-- *INF*: :LKP.LKP_claim_occurrence(v_claim_num)
	LKP_CLAIM_OCCURRENCE_v_claim_num.claim_occurrence_ak_id AS v_claim_occ_ak_id,
	-- *INF*: IIF(isnull(v_claim_occ_ak_id),-1,v_claim_occ_ak_id)
	IFF(v_claim_occ_ak_id IS NULL, - 1, v_claim_occ_ak_id) AS claim_occ_ak_id_out,
	LKP_claim_party_occurrence.claim_party_occurrence_ak_id AS in_claim_party_occurrence_ak_id,
	-- *INF*: iif(isnull(in_claim_party_occurrence_ak_id),-1,in_claim_party_occurrence_ak_id)
	IFF(in_claim_party_occurrence_ak_id IS NULL, - 1, in_claim_party_occurrence_ak_id) AS claim_party_occurrence_ak_id,
	LKP_medical_bill_vendor.med_bill_vendor_ak_id AS in_med_bill_vendor_ak_id,
	-- *INF*: iif(isnull(in_med_bill_vendor_ak_id),-1,in_med_bill_vendor_ak_id)
	IFF(in_med_bill_vendor_ak_id IS NULL, - 1, in_med_bill_vendor_ak_id) AS med_bill_vendor_ak_id,
	-- *INF*: IIF(length(ebill_ind)=0,'N/A',ebill_ind)
	IFF(length(ebill_ind) = 0, 'N/A', ebill_ind) AS ebill_ind_out,
	-- *INF*: IIF(NOT ISNULL(EXD_denial_reason_cd), EXD_denial_reason_cd,
	-- IIF(NOT ISNULL(PMS_denial_reason_cd), PMS_denial_reason_cd,
	-- 'N/A'))
	IFF(NOT EXD_denial_reason_cd IS NULL, EXD_denial_reason_cd, IFF(NOT PMS_denial_reason_cd IS NULL, PMS_denial_reason_cd, 'N/A')) AS denial_rsn_cd_out,
	-- *INF*: IIF(NOT isnull(EXD_draft_num) , EXD_draft_num,
	-- IIF(NOT isnull(PMS_draft_num) , PMS_draft_num,
	-- 'N/A'))
	IFF(NOT EXD_draft_num IS NULL, EXD_draft_num, IFF(NOT PMS_draft_num IS NULL, PMS_draft_num, 'N/A')) AS draft_num_out,
	-- *INF*: IIF(NOT isnull(EXD_draft_amt) ,EXD_draft_amt,
	-- IIF(NOT isnull(PMS_draft_amt) , PMS_draft_amt,
	-- 0))
	-- 
	-- 
	IFF(NOT EXD_draft_amt IS NULL, EXD_draft_amt, IFF(NOT PMS_draft_amt IS NULL, PMS_draft_amt, 0)) AS draft_amt_out,
	-- *INF*: IIF(NOT ISNULL(EXD_paid_date) ,EXD_paid_date,
	-- IIF(NOT ISNULL(PMS_paid_date) ,PMS_paid_date,
	-- TO_DATE('1/1/1800','MM/DD/YYYY')))
	-- 
	IFF(NOT EXD_paid_date IS NULL, EXD_paid_date, IFF(NOT PMS_paid_date IS NULL, PMS_paid_date, TO_DATE('1/1/1800', 'MM/DD/YYYY'))) AS draft_paid_date_out,
	LKP_clm_clt_eor_stage.cce_eor_status AS in_med_bill_review_code,
	-- *INF*: iif(isnull(in_med_bill_review_code),'N/A',in_med_bill_review_code)
	IFF(in_med_bill_review_code IS NULL, 'N/A', in_med_bill_review_code) AS med_bill_review_code,
	-- *INF*: iif(isnull(in_autopay_ind),'N/A',in_autopay_ind)
	-- 
	--  
	IFF(in_autopay_ind IS NULL, 'N/A', in_autopay_ind) AS autopay_ind_out,
	SQ_med_bill_stage.eor_rcvd_date AS in_eor_rcvd_date,
	-- *INF*: IIF(ISNULL( in_eor_rcvd_date) ,
	-- TO_DATE('1/1/1800','MM/DD/YYYY'),in_eor_rcvd_date)
	IFF(in_eor_rcvd_date IS NULL, TO_DATE('1/1/1800', 'MM/DD/YYYY'), in_eor_rcvd_date) AS o_eor_rcvd_date,
	SQ_med_bill_stage.original_vendor_bill_num AS in_original_vendor_bill_num,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_original_vendor_bill_num)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_original_vendor_bill_num) AS o_original_vendor_bill_num,
	SQ_med_bill_stage.auto_adjudicated,
	-- *INF*: IIF(ISNULL(auto_adjudicated) OR RTRIM(auto_adjudicated)='','N/A',auto_adjudicated)
	IFF(auto_adjudicated IS NULL OR RTRIM(auto_adjudicated) = '', 'N/A', auto_adjudicated) AS o_auto_adjudicated
	FROM SQ_med_bill_stage
	LEFT JOIN LKP_claim_party_occurrence
	ON LKP_claim_party_occurrence.claim_party_key = LKP_clm_clt_eor_stage.cce_client_id AND LKP_claim_party_occurrence.claim_occurrence_key = LKP_clm_clt_eor_stage.cce_claim_nbr
	LEFT JOIN LKP_claim_payment
	ON LKP_claim_payment.claim_pay_num = LKP_clm_clt_eor_stage.cce_draft_nbr
	LEFT JOIN LKP_clm_clt_eor_stage
	ON LKP_clm_clt_eor_stage.cce_tch_bill_nbr = SQ_med_bill_stage.med_bill_id
	LEFT JOIN LKP_medical_bill_vendor
	ON LKP_medical_bill_vendor.vendor_code = SQ_med_bill_stage.vendor_code
	LEFT JOIN LKP_pms_clt_eor_stage
	ON LKP_pms_clt_eor_stage.pce_tch_bill_nbr = SQ_med_bill_stage.med_bill_id
	LEFT JOIN LKP_CLAIM_OCCURRENCE LKP_CLAIM_OCCURRENCE_v_claim_num
	ON LKP_CLAIM_OCCURRENCE_v_claim_num.claim_occurrence_key = v_claim_num

),
LKP_medical_bill AS (
	SELECT
	med_bill_ak_id,
	claim_occurrence_ak_id,
	med_bill_key,
	in_med_bill_id,
	vendor_bill_num,
	patient_acct_num,
	patient_last_name,
	patient_first_name,
	patient_mid_name,
	patient_full_name,
	patient_addr,
	patient_city,
	patient_state,
	patient_zip_code,
	patient_birthdate,
	patient_gndr,
	patient_ssn,
	patient_inj_date,
	refer_physician,
	serv_from_date,
	serv_to_date,
	inpatient_outpatient_ind,
	bill_issued_date,
	bill_rcvd_date,
	bus_rcvd_date,
	bill_process_date,
	patient_admit_date,
	patient_discharge_date,
	daily_hospital_rate,
	bill_review_cost,
	total_bill_charge,
	total_bill_review_red,
	total_network_red,
	total_recommend_pay,
	total_addtional_charge,
	bill_type,
	bill_status_code,
	fee_sched_code,
	network_name,
	network_num,
	serv_line_num,
	deleted_ind,
	emplyr,
	acct_id,
	bill_review_vendor_code,
	ebill_ind,
	denial_rsn_code,
	draft_num,
	draft_amt,
	draft_paid_date,
	claim_party_occurrence_ak_id,
	med_bill_vendor_ak_id,
	med_bill_review_code,
	autopay_ind,
	eor_rcvd_date,
	OriginalVendorBillNumber,
	AutoAdjudicatedIndicator
	FROM (
		SELECT 	med_bill_ak_id					AS med_bill_ak_id 
		,		claim_occurrence_ak_id			AS claim_occurrence_ak_id 
		, med_bill_key as med_bill_key
		,		rtrim(vendor_bill_num)			AS vendor_bill_num 
		,		rtrim(patient_acct_num)			AS patient_acct_num 
		,		rtrim(patient_last_name)		AS patient_last_name 
		,		rtrim(patient_first_name)		AS patient_first_name 
		,		rtrim(patient_mid_name)			AS patient_mid_name 
		,		rtrim(patient_full_name)			AS patient_full_name 
		,		rtrim(patient_addr)				AS patient_addr 
		,		rtrim(patient_city)				AS patient_city 
		,		rtrim(patient_state)			AS patient_state 
		,		rtrim(patient_zip_code)			AS patient_zip_code 
		,		patient_birthdate				AS patient_birthdate 
		,		rtrim(patient_gndr)				AS patient_gndr 
		,		rtrim(patient_ssn)				AS patient_ssn 
		,		patient_inj_date				AS patient_inj_date 
		,		rtrim(refer_physician)			AS refer_physician 
		,		serv_from_date					AS serv_from_date 
		,		serv_to_date					AS serv_to_date 
		,		rtrim(inpatient_outpatient_ind) AS inpatient_outpatient_ind 
		,		bill_issued_date				AS bill_issued_date 
		,		bill_rcvd_date					AS bill_rcvd_date 
		,		bus_rcvd_date					AS bus_rcvd_date 
		,		bill_process_date				AS bill_process_date 
		,		patient_admit_date				AS patient_admit_date 
		,		patient_discharge_date			AS patient_discharge_date 
		,		daily_hospital_rate				AS daily_hospital_rate 
		,		bill_review_cost				AS bill_review_cost 
		,		total_bill_charge				AS total_bill_charge 
		,		total_bill_review_red			AS total_bill_review_red 
		,		total_network_red				AS total_network_red 
		,		total_recommend_pay				AS total_recommend_pay 
		,		total_addtional_charge			AS total_addtional_charge 
		,		rtrim(bill_type)				AS bill_type 
		,		rtrim(bill_status_code)			AS bill_status_code 
		,		rtrim(fee_sched_code)			AS fee_sched_code 
		,		rtrim(network_name)				AS network_name 
		,		network_num						AS network_num 
		,		serv_line_num					AS serv_line_num 
		,		rtrim(deleted_ind)				AS deleted_ind 
		,		rtrim(emplyr)					AS emplyr 
		,		rtrim(acct_id)					AS acct_id 
		,		rtrim(bill_review_vendor_code)	AS bill_review_vendor_code 
		,		rtrim(ebill_ind)				AS ebill_ind 
		,		rtrim(denial_rsn_code)			AS denial_rsn_code 
		,		rtrim(draft_num)				AS draft_num 
		,		draft_amt						AS draft_amt 
		,		draft_paid_date					AS draft_paid_date
		,		claim_party_occurrence_ak_id	AS claim_party_occurrence_ak_id
		,		med_bill_key					AS med_bill_key 
		,		med_bill_vendor_ak_id			AS med_bill_vendor_ak_id
		,           med_bill_review_code       as med_bill_review_code
		,           autopay_ind                            as autopay_ind
		,           eor_rcvd_date                       as eor_rcvd_date
		,           OriginalVendorBillNumber    AS OriginalVendorBillNumber
		,           AutoAdjudicatedIndicator    AS AutoAdjudicatedIndicator
		FROM	@{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill
		WHERE	crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY med_bill_key ORDER BY med_bill_ak_id DESC) = 1
),
EXPTRANS AS (
	SELECT
	LKP_medical_bill.med_bill_ak_id,
	LKP_medical_bill.claim_occurrence_ak_id,
	EXP_ASSIGN_DEFAULTS.med_bill_id,
	LKP_medical_bill.vendor_bill_num,
	LKP_medical_bill.patient_acct_num,
	LKP_medical_bill.patient_last_name,
	LKP_medical_bill.patient_first_name,
	LKP_medical_bill.patient_mid_name,
	LKP_medical_bill.patient_full_name,
	LKP_medical_bill.patient_addr,
	LKP_medical_bill.patient_city,
	LKP_medical_bill.patient_state,
	LKP_medical_bill.patient_zip_code,
	LKP_medical_bill.patient_birthdate,
	LKP_medical_bill.patient_gndr,
	LKP_medical_bill.patient_ssn,
	LKP_medical_bill.patient_inj_date,
	LKP_medical_bill.refer_physician,
	LKP_medical_bill.serv_from_date,
	LKP_medical_bill.serv_to_date,
	LKP_medical_bill.inpatient_outpatient_ind,
	LKP_medical_bill.bill_issued_date,
	LKP_medical_bill.bill_rcvd_date,
	LKP_medical_bill.bus_rcvd_date,
	LKP_medical_bill.bill_process_date,
	LKP_medical_bill.patient_admit_date,
	LKP_medical_bill.patient_discharge_date,
	LKP_medical_bill.daily_hospital_rate,
	LKP_medical_bill.bill_review_cost,
	LKP_medical_bill.total_bill_charge,
	LKP_medical_bill.total_bill_review_red,
	LKP_medical_bill.total_network_red,
	LKP_medical_bill.total_recommend_pay,
	LKP_medical_bill.total_addtional_charge,
	LKP_medical_bill.bill_type,
	LKP_medical_bill.bill_status_code,
	LKP_medical_bill.fee_sched_code,
	LKP_medical_bill.network_name,
	LKP_medical_bill.network_num,
	LKP_medical_bill.serv_line_num,
	LKP_medical_bill.deleted_ind,
	LKP_medical_bill.emplyr,
	LKP_medical_bill.acct_id,
	LKP_medical_bill.bill_review_vendor_code,
	LKP_medical_bill.ebill_ind,
	LKP_medical_bill.denial_rsn_code,
	LKP_medical_bill.draft_num,
	LKP_medical_bill.draft_amt,
	LKP_medical_bill.draft_paid_date,
	LKP_medical_bill.claim_party_occurrence_ak_id,
	LKP_medical_bill.med_bill_vendor_ak_id,
	LKP_medical_bill.med_bill_review_code,
	LKP_medical_bill.autopay_ind,
	LKP_medical_bill.eor_rcvd_date,
	LKP_medical_bill.OriginalVendorBillNumber,
	LKP_medical_bill.AutoAdjudicatedIndicator,
	EXP_ASSIGN_DEFAULTS.crrnt_snpsht_flag,
	EXP_ASSIGN_DEFAULTS.audit_id,
	-- *INF*: IIF(ISnull(med_bill_ak_id),'NEW',
	-- iif (
	-- in_vendor_bill_num<>vendor_bill_num or
	-- in_pt_acct_num <> patient_acct_num or
	-- in_pt_last_name <>patient_last_name or
	-- in_pt_first_name<>patient_first_name or
	-- in_pt_mid_name <>patient_mid_name or
	-- in_pt_addr <> patient_addr or
	-- in_pt_city<>patient_city or
	-- in_pt_state<>patient_state or
	-- in_pt_zip_code <> patient_zip_code or
	-- in_pt_dob <> patient_birthdate or
	-- in_pt_gndr<> patient_gndr or
	-- in_pt_ssn <> patient_ssn or
	-- in_pt_inj_dt <> patient_inj_date or
	-- in_refer_physician <> refer_physician or
	-- in_serv_from_date<>serv_from_date or
	-- in_serv_to_date<>serv_to_date or
	-- in_inpt_outpt_ind<>inpatient_outpatient_ind or
	-- in_bill_issued_date<>bill_issued_date or
	-- in_bill_rcvd_date<>bill_rcvd_date or
	-- in_bus_rcvd_date<>bus_rcvd_date or
	-- in_bill_process_date<>bill_process_date or
	-- in_pt_admit_date<>patient_admit_date or
	-- in_pt_discharge_date<>patient_discharge_date or 
	-- in_daily_hospital_rt<>daily_hospital_rate or 
	-- in_bill_review_cost<>bill_review_cost or 
	-- in_total_bill_charge<>total_bill_charge or 
	-- in_total_bill_red<>total_bill_review_red or 
	-- in_total_network_red<>total_network_red or 
	-- in_total_recom_pay<>total_recommend_pay or 
	-- in_total_addtl_charge<>total_addtional_charge or 
	-- in_bill_type<>bill_type or 
	-- in_bill_status_code<>bill_status_code or 
	-- in_fee_sched_code<>fee_sched_code or 
	-- in_network_name<>network_name or 
	-- in_network_num<>network_num or 
	-- in_serv_line_num<>serv_line_num or 
	-- in_deleted_ind<>deleted_ind or 
	-- in_emplyr<>emplyr or 
	-- in_acct_id<>acct_id or 
	-- in_vendor_code<>bill_review_vendor_code or 
	-- in_autopay_ind<>autopay_ind or 
	-- in_ebill_ind<> ebill_ind or
	-- in_med_bill_review_code<>med_bill_review_code or 
	-- in_original_vendor_bill_num<>OriginalVendorBillNumber or 
	-- in_auto_adjudicated<>AutoAdjudicatedIndicator or 
	-- in_denial_rsn_cd<>denial_rsn_code or 
	-- in_draft_num<>draft_num or 
	-- in_draft_paid_date <> draft_paid_date
	-- , 'UPDATE','NOCHANGE')) 
	IFF(med_bill_ak_id IS NULL, 'NEW', IFF(in_vendor_bill_num <> vendor_bill_num OR in_pt_acct_num <> patient_acct_num OR in_pt_last_name <> patient_last_name OR in_pt_first_name <> patient_first_name OR in_pt_mid_name <> patient_mid_name OR in_pt_addr <> patient_addr OR in_pt_city <> patient_city OR in_pt_state <> patient_state OR in_pt_zip_code <> patient_zip_code OR in_pt_dob <> patient_birthdate OR in_pt_gndr <> patient_gndr OR in_pt_ssn <> patient_ssn OR in_pt_inj_dt <> patient_inj_date OR in_refer_physician <> refer_physician OR in_serv_from_date <> serv_from_date OR in_serv_to_date <> serv_to_date OR in_inpt_outpt_ind <> inpatient_outpatient_ind OR in_bill_issued_date <> bill_issued_date OR in_bill_rcvd_date <> bill_rcvd_date OR in_bus_rcvd_date <> bus_rcvd_date OR in_bill_process_date <> bill_process_date OR in_pt_admit_date <> patient_admit_date OR in_pt_discharge_date <> patient_discharge_date OR in_daily_hospital_rt <> daily_hospital_rate OR in_bill_review_cost <> bill_review_cost OR in_total_bill_charge <> total_bill_charge OR in_total_bill_red <> total_bill_review_red OR in_total_network_red <> total_network_red OR in_total_recom_pay <> total_recommend_pay OR in_total_addtl_charge <> total_addtional_charge OR in_bill_type <> bill_type OR in_bill_status_code <> bill_status_code OR in_fee_sched_code <> fee_sched_code OR in_network_name <> network_name OR in_network_num <> network_num OR in_serv_line_num <> serv_line_num OR in_deleted_ind <> deleted_ind OR in_emplyr <> emplyr OR in_acct_id <> acct_id OR in_vendor_code <> bill_review_vendor_code OR in_autopay_ind <> autopay_ind OR in_ebill_ind <> ebill_ind OR in_med_bill_review_code <> med_bill_review_code OR in_original_vendor_bill_num <> OriginalVendorBillNumber OR in_auto_adjudicated <> AutoAdjudicatedIndicator OR in_denial_rsn_cd <> denial_rsn_code OR in_draft_num <> draft_num OR in_draft_paid_date <> draft_paid_date, 'UPDATE', 'NOCHANGE')) AS VChange_flag,
	VChange_flag AS Change_flag,
	EXP_ASSIGN_DEFAULTS.claim_occ_ak_id_out AS in_claim_occurrence_ak_id,
	EXP_ASSIGN_DEFAULTS.claim_party_occurrence_ak_id AS in_claim_party_occurrence_ak_id,
	EXP_ASSIGN_DEFAULTS.med_bill_vendor_ak_id AS in_med_bill_vendor_ak_id,
	EXP_ASSIGN_DEFAULTS.vendor_bill_num_out AS in_vendor_bill_num,
	EXP_ASSIGN_DEFAULTS.pt_acct_num_out AS in_pt_acct_num,
	EXP_ASSIGN_DEFAULTS.pt_last_name_out AS in_pt_last_name,
	EXP_ASSIGN_DEFAULTS.pt_first_name_out AS in_pt_first_name,
	EXP_ASSIGN_DEFAULTS.pt_mid_name_out AS in_pt_mid_name,
	EXP_ASSIGN_DEFAULTS.out_pt_full_name AS in_pt_full_name,
	EXP_ASSIGN_DEFAULTS.pt_addr_out AS in_pt_addr,
	EXP_ASSIGN_DEFAULTS.pt_city_out AS in_pt_city,
	EXP_ASSIGN_DEFAULTS.pt_state_out AS in_pt_state,
	EXP_ASSIGN_DEFAULTS.pt_zip_code_out AS in_pt_zip_code,
	EXP_ASSIGN_DEFAULTS.pt_dob_out AS in_pt_dob,
	EXP_ASSIGN_DEFAULTS.pt_gndr_out AS in_pt_gndr,
	EXP_ASSIGN_DEFAULTS.pt_ssn_out AS in_pt_ssn,
	EXP_ASSIGN_DEFAULTS.pt_inj_dt_out AS in_pt_inj_dt,
	EXP_ASSIGN_DEFAULTS.refer_physician_out AS in_refer_physician,
	EXP_ASSIGN_DEFAULTS.serv_from_date_out AS in_serv_from_date,
	EXP_ASSIGN_DEFAULTS.serv_to_date_out AS in_serv_to_date,
	EXP_ASSIGN_DEFAULTS.inpt_outpt_ind_out AS in_inpt_outpt_ind,
	EXP_ASSIGN_DEFAULTS.bill_issued_date_out AS in_bill_issued_date,
	EXP_ASSIGN_DEFAULTS.bill_rcvd_date_out AS in_bill_rcvd_date,
	EXP_ASSIGN_DEFAULTS.bus_rcvd_date_out AS in_bus_rcvd_date,
	EXP_ASSIGN_DEFAULTS.bill_process_date_out AS in_bill_process_date,
	EXP_ASSIGN_DEFAULTS.pt_admit_date_out AS in_pt_admit_date,
	EXP_ASSIGN_DEFAULTS.pt_discharge_date_out AS in_pt_discharge_date,
	EXP_ASSIGN_DEFAULTS.daily_hospital_rt_out AS in_daily_hospital_rt,
	EXP_ASSIGN_DEFAULTS.bill_review_cost_out AS in_bill_review_cost,
	EXP_ASSIGN_DEFAULTS.total_bill_charge_out AS in_total_bill_charge,
	EXP_ASSIGN_DEFAULTS.total_bill_red_out AS in_total_bill_red,
	EXP_ASSIGN_DEFAULTS.total_network_red_out AS in_total_network_red,
	EXP_ASSIGN_DEFAULTS.total_recom_pay_out AS in_total_recom_pay,
	EXP_ASSIGN_DEFAULTS.total_addtl_charge_out AS in_total_addtl_charge,
	EXP_ASSIGN_DEFAULTS.bill_type_out AS in_bill_type,
	EXP_ASSIGN_DEFAULTS.bill_status_code_out AS in_bill_status_code,
	EXP_ASSIGN_DEFAULTS.fee_sched_code_out AS in_fee_sched_code,
	EXP_ASSIGN_DEFAULTS.network_name_out AS in_network_name,
	EXP_ASSIGN_DEFAULTS.network_num_out AS in_network_num,
	EXP_ASSIGN_DEFAULTS.serv_line_num_out AS in_serv_line_num,
	EXP_ASSIGN_DEFAULTS.deleted_ind_out AS in_deleted_ind,
	EXP_ASSIGN_DEFAULTS.emplyr_out AS in_emplyr,
	EXP_ASSIGN_DEFAULTS.acct_id_out AS in_acct_id,
	EXP_ASSIGN_DEFAULTS.vendor_code_out AS in_vendor_code,
	EXP_ASSIGN_DEFAULTS.ebill_ind_out AS in_ebill_ind,
	EXP_ASSIGN_DEFAULTS.denial_rsn_cd_out AS in_denial_rsn_cd,
	EXP_ASSIGN_DEFAULTS.draft_num_out AS in_draft_num,
	EXP_ASSIGN_DEFAULTS.draft_amt_out AS in_draft_amt,
	EXP_ASSIGN_DEFAULTS.draft_paid_date_out AS in_draft_paid_date,
	EXP_ASSIGN_DEFAULTS.med_bill_review_code AS in_med_bill_review_code,
	EXP_ASSIGN_DEFAULTS.autopay_ind_out AS in_autopay_ind,
	EXP_ASSIGN_DEFAULTS.o_eor_rcvd_date AS in_eor_rcvd_date,
	EXP_ASSIGN_DEFAULTS.o_original_vendor_bill_num AS in_original_vendor_bill_num,
	EXP_ASSIGN_DEFAULTS.o_auto_adjudicated AS in_auto_adjudicated
	FROM EXP_ASSIGN_DEFAULTS
	LEFT JOIN LKP_medical_bill
	ON LKP_medical_bill.med_bill_key = EXP_ASSIGN_DEFAULTS.med_bill_id
),
FIL_NEW_CHANGED_ROWS AS (
	SELECT
	in_claim_occurrence_ak_id AS claim_occurrence_ak_id, 
	in_claim_party_occurrence_ak_id AS claim_party_occurrence_ak_id, 
	in_med_bill_vendor_ak_id AS med_bill_vendor_ak_id, 
	med_bill_ak_id, 
	med_bill_id, 
	in_vendor_bill_num AS vendor_bill_num, 
	in_pt_acct_num AS pt_acct_num, 
	in_pt_last_name AS pt_last_name, 
	in_pt_first_name AS pt_first_name, 
	in_pt_mid_name AS pt_mid_name, 
	in_pt_full_name AS pt_full_name, 
	in_pt_addr AS pt_addr, 
	in_pt_city AS pt_city, 
	in_pt_state AS pt_state, 
	in_pt_zip_code AS pt_zip_code, 
	in_pt_dob AS pt_dob, 
	in_pt_gndr AS pt_gndr, 
	in_pt_ssn AS pt_ssn, 
	in_pt_inj_dt AS pt_inj_dt, 
	in_refer_physician AS refer_physician, 
	in_serv_from_date AS serv_from_date, 
	in_serv_to_date AS serv_to_date, 
	in_inpt_outpt_ind AS inpt_outpt_ind, 
	in_bill_issued_date AS bill_issued_date, 
	in_bill_rcvd_date AS bill_rcvd_date, 
	in_bus_rcvd_date AS bus_rcvd_date, 
	in_bill_process_date AS bill_process_date, 
	in_pt_admit_date AS pt_admit_date, 
	in_pt_discharge_date AS pt_discharge_date, 
	in_daily_hospital_rt AS daily_hospital_rt, 
	in_bill_review_cost AS bill_review_cost, 
	in_total_bill_charge AS total_bill_charge, 
	in_total_bill_red AS total_bill_red, 
	in_total_network_red AS total_network_red, 
	in_total_recom_pay AS total_recom_pay, 
	in_total_addtl_charge AS total_addtl_charge, 
	in_bill_type AS bill_type, 
	in_bill_status_code AS bill_status_code, 
	in_fee_sched_code AS fee_sched_code, 
	in_network_name AS network_name, 
	in_network_num AS network_num, 
	in_serv_line_num AS serv_line_num, 
	in_deleted_ind AS deleted_ind, 
	in_emplyr AS emplyr, 
	in_acct_id AS acct_id, 
	in_vendor_code AS vendor_code, 
	crrnt_snpsht_flag, 
	audit_id, 
	in_ebill_ind AS ebill_ind, 
	in_denial_rsn_cd AS denial_rsn_code, 
	in_draft_num AS draft_num, 
	in_draft_amt AS draft_amt, 
	in_draft_paid_date AS draft_paid_date, 
	in_med_bill_review_code AS med_bill_review_code, 
	in_autopay_ind AS autopay_ind, 
	in_eor_rcvd_date AS eor_rcvd_date, 
	in_original_vendor_bill_num AS OriginalVendorBillNumber, 
	in_auto_adjudicated AS AutoAdjudicatedIndicator, 
	Change_flag
	FROM EXPTRANS
	WHERE Change_flag='NEW' or Change_flag='UPDATE'
),
SEQ_med_bill_ak_id AS (
	CREATE SEQUENCE SEQ_med_bill_ak_id
	START = 2000000
	INCREMENT = 1;
),
EXP_AUDIT_FIELDS AS (
	SELECT
	med_bill_ak_id,
	claim_occurrence_ak_id,
	med_bill_id,
	vendor_bill_num,
	pt_acct_num,
	pt_last_name,
	pt_first_name AS patient_first_name,
	pt_mid_name,
	pt_addr,
	pt_city,
	pt_state,
	pt_zip_code,
	pt_dob,
	pt_gndr,
	pt_ssn,
	pt_inj_dt,
	refer_physician,
	serv_from_date,
	serv_to_date,
	inpt_outpt_ind,
	bill_issued_date,
	bill_rcvd_date,
	bus_rcvd_date,
	bill_process_date,
	pt_admit_date,
	pt_discharge_date,
	-- *INF*: TO_DATE('1/1/1800','mm/dd/yyyy')
	TO_DATE('1/1/1800', 'mm/dd/yyyy') AS bill_cov_from_to_dt_default,
	daily_hospital_rt,
	bill_review_cost,
	total_bill_charge,
	total_bill_red,
	total_network_red,
	total_recom_pay,
	total_addtl_charge,
	bill_type,
	bill_status_code,
	fee_sched_code,
	network_name,
	network_num,
	serv_line_num,
	deleted_ind,
	emplyr,
	acct_id,
	vendor_code,
	crrnt_snpsht_flag,
	audit_id,
	-- *INF*: IIF(Change_flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),SYSDATE)
	IFF(Change_flag = 'NEW', TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	sysdate AS created_date,
	pt_full_name,
	ebill_ind,
	denial_rsn_code,
	draft_num,
	draft_amt,
	draft_paid_date,
	claim_party_occurrence_ak_id,
	med_bill_vendor_ak_id,
	med_bill_review_code,
	autopay_ind AS in_autopay_ind,
	eor_rcvd_date,
	OriginalVendorBillNumber,
	AutoAdjudicatedIndicator,
	Change_flag,
	SEQ_med_bill_ak_id.NEXTVAL,
	-- *INF*: IIF(Change_flag='NEW', NEXTVAL, med_bill_ak_id)
	IFF(Change_flag = 'NEW', NEXTVAL, med_bill_ak_id) AS med_bill_ak_id_out
	FROM FIL_NEW_CHANGED_ROWS
),
medical_bill_insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill
	(med_bill_ak_id, claim_occurrence_ak_id, med_bill_key, vendor_bill_num, patient_acct_num, patient_full_name, patient_last_name, patient_first_name, patient_mid_name, patient_addr, patient_city, patient_state, patient_zip_code, patient_birthdate, patient_gndr, patient_ssn, patient_inj_date, refer_physician, serv_from_date, serv_to_date, inpatient_outpatient_ind, bill_issued_date, bill_rcvd_date, bus_rcvd_date, bill_process_date, patient_admit_date, patient_discharge_date, bill_cover_from_date, bill_cover_to_date, daily_hospital_rate, bill_review_cost, total_bill_charge, total_bill_review_red, total_network_red, total_recommend_pay, total_addtional_charge, bill_type, bill_status_code, fee_sched_code, network_name, network_num, serv_line_num, deleted_ind, emplyr, acct_id, bill_review_vendor_code, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, ebill_ind, draft_num, draft_amt, draft_paid_date, denial_rsn_code, med_bill_vendor_ak_id, claim_party_occurrence_ak_id, med_bill_review_code, autopay_ind, eor_rcvd_date, OriginalVendorBillNumber, AutoAdjudicatedIndicator)
	SELECT 
	med_bill_ak_id_out AS MED_BILL_AK_ID, 
	CLAIM_OCCURRENCE_AK_ID, 
	med_bill_id AS MED_BILL_KEY, 
	VENDOR_BILL_NUM, 
	pt_acct_num AS PATIENT_ACCT_NUM, 
	pt_full_name AS PATIENT_FULL_NAME, 
	pt_last_name AS PATIENT_LAST_NAME, 
	PATIENT_FIRST_NAME, 
	pt_mid_name AS PATIENT_MID_NAME, 
	pt_addr AS PATIENT_ADDR, 
	pt_city AS PATIENT_CITY, 
	pt_state AS PATIENT_STATE, 
	pt_zip_code AS PATIENT_ZIP_CODE, 
	pt_dob AS PATIENT_BIRTHDATE, 
	pt_gndr AS PATIENT_GNDR, 
	pt_ssn AS PATIENT_SSN, 
	pt_inj_dt AS PATIENT_INJ_DATE, 
	REFER_PHYSICIAN, 
	SERV_FROM_DATE, 
	SERV_TO_DATE, 
	inpt_outpt_ind AS INPATIENT_OUTPATIENT_IND, 
	BILL_ISSUED_DATE, 
	BILL_RCVD_DATE, 
	BUS_RCVD_DATE, 
	BILL_PROCESS_DATE, 
	pt_admit_date AS PATIENT_ADMIT_DATE, 
	pt_discharge_date AS PATIENT_DISCHARGE_DATE, 
	bill_cov_from_to_dt_default AS BILL_COVER_FROM_DATE, 
	bill_cov_from_to_dt_default AS BILL_COVER_TO_DATE, 
	daily_hospital_rt AS DAILY_HOSPITAL_RATE, 
	BILL_REVIEW_COST, 
	TOTAL_BILL_CHARGE, 
	total_bill_red AS TOTAL_BILL_REVIEW_RED, 
	TOTAL_NETWORK_RED, 
	total_recom_pay AS TOTAL_RECOMMEND_PAY, 
	total_addtl_charge AS TOTAL_ADDTIONAL_CHARGE, 
	BILL_TYPE, 
	BILL_STATUS_CODE, 
	FEE_SCHED_CODE, 
	NETWORK_NAME, 
	NETWORK_NUM, 
	SERV_LINE_NUM, 
	DELETED_IND, 
	EMPLYR, 
	ACCT_ID, 
	vendor_code AS BILL_REVIEW_VENDOR_CODE, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	created_date AS MODIFIED_DATE, 
	EBILL_IND, 
	DRAFT_NUM, 
	DRAFT_AMT, 
	DRAFT_PAID_DATE, 
	DENIAL_RSN_CODE, 
	MED_BILL_VENDOR_AK_ID, 
	CLAIM_PARTY_OCCURRENCE_AK_ID, 
	MED_BILL_REVIEW_CODE, 
	in_autopay_ind AS AUTOPAY_IND, 
	EOR_RCVD_DATE, 
	ORIGINALVENDORBILLNUMBER, 
	AUTOADJUDICATEDINDICATOR
	FROM EXP_AUDIT_FIELDS
),
SQ_medical_bill AS (
	SELECT 
	medical_bill.med_bill_id, 
	medical_bill.med_bill_key, 
	medical_bill.eff_from_date, 
	medical_bill.eff_to_date 
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill medical_bill
	WHERE
	medical_bill.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and exists
	(
	select 1 from @{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill medical_bill2
	where source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and crrnt_snpsht_flag = 1 and
	medical_bill.med_bill_key = medical_bill2.med_bill_key 
	group by medical_bill2.med_bill_key having count(*) > 1
	)
	
	order by medical_bill.med_bill_key, medical_bill.eff_from_date desc
),
EXP_Lag_eff_from_date1 AS (
	SELECT
	med_bill_id,
	med_bill_key,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	med_bill_key = v_PREV_ROW_occurrence_key, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(TRUE,
	med_bill_key = v_PREV_ROW_occurrence_key, ADD_TO_DATE(v_PREV_ROW_eff_from_date, 'SS', - 1),
	orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	med_bill_key AS v_PREV_ROW_occurrence_key,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_medical_bill
),
FILTRANS AS (
	SELECT
	med_bill_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Lag_eff_from_date1
	WHERE orig_eff_to_date <> eff_to_date
),
UPD_EFF_TO_DATE AS (
	SELECT
	med_bill_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FILTRANS
),
medical_bill_update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill AS T
	USING UPD_EFF_TO_DATE AS S
	ON T.med_bill_id = S.med_bill_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),
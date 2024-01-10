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
LKP_CLAIM_PARTY_OCCURRENCE AS (
	SELECT
	claim_party_occurrence_ak_id,
	claim_occurrence_ak_id,
	claim_party_ak_id,
	claim_party_role_code
	FROM (
		SELECT claim_party_occurrence.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, claim_party_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id, claim_party_occurrence.claim_party_ak_id as claim_party_ak_id, LTRIM(RTRIM(claim_party_occurrence.claim_party_role_code)) as claim_party_role_code FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence  
		WHERE     (source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}')  AND (CRRNT_SNPSHT_FLAG='1')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,claim_party_ak_id,claim_party_role_code ORDER BY claim_party_occurrence_ak_id) = 1
),
LKP_Claimant_Coverage_Detail_Exceed AS (
	SELECT
	claimant_cov_det_ak_id,
	claim_party_occurrence_ak_id,
	ins_line,
	loc_unit_num,
	sub_loc_unit_num,
	risk_unit_grp,
	risk_unit_grp_seq_num,
	risk_unit,
	risk_unit_seq_num,
	major_peril_code,
	reserve_ctgry,
	cause_of_loss,
	major_peril_seq
	FROM (
		SELECT claimant_coverage_detail.claimant_cov_det_ak_id as claimant_cov_det_ak_id, 
		claimant_coverage_detail.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, 
		LTRIM(RTRIM(claimant_coverage_detail.ins_line)) as ins_line, 
		LTRIM(RTRIM(claimant_coverage_detail.loc_unit_num)) as loc_unit_num,
		LTRIM(RTRIM(claimant_coverage_detail.sub_loc_unit_num)) as sub_loc_unit_num,
		LTRIM(RTRIM(claimant_coverage_detail.risk_unit_grp)) as  risk_unit_grp,
		LTRIM(RTRIM(claimant_coverage_detail.risk_unit_grp_seq_num)) as risk_unit_grp_seq_num,
		ltrim(rtrim(claimant_coverage_detail.risk_unit)) as risk_unit,
		ltrim(rtrim(claimant_coverage_detail.risk_unit_seq_num)) as risk_unit_seq_num,
		LTRIM(RTRIM(claimant_coverage_detail.major_peril_code)) as major_peril_code, 
		LTRIM(RTRIM(claimant_coverage_detail.reserve_ctgry)) as reserve_ctgry, 
		LTRIM(RTRIM(claimant_coverage_detail.cause_of_loss)) as cause_of_loss,
		ltrim(rtrim(claimant_coverage_detail.major_peril_seq)) as major_peril_seq 
		FROM dbo.claimant_coverage_detail  
		WHERE claimant_coverage_detail.SOURCE_SYS_ID = 'EXCEED'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id,ins_line,loc_unit_num,sub_loc_unit_num,risk_unit_grp,risk_unit_grp_seq_num,risk_unit,risk_unit_seq_num,major_peril_code,reserve_ctgry,cause_of_loss,major_peril_seq ORDER BY claimant_cov_det_ak_id DESC) = 1
),
LKP_Claim_Party_Occurrence_Exceed AS (
	SELECT
	claim_party_occurrence_ak_id,
	claim_occurrence_ak_id,
	claimant_num
	FROM (
		SELECT claim_party_occurrence.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, claim_party_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id, claim_party_occurrence.claimant_num as claimant_num FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence
		WHERE crrnt_snpsht_flag = 1 and source_sys_id = 'EXCEED' and claim_party_role_code = 'CLMT'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,claimant_num ORDER BY claim_party_occurrence_ak_id) = 1
),
LKP_Claim_Transaction AS (
	SELECT
	claim_trans_id,
	claimant_cov_det_ak_id
	FROM (
		SELECT 
			claim_trans_id,
			claimant_cov_det_ak_id
		FROM claim_transaction
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id ORDER BY claim_trans_id) = 1
),
LKP_CLAIMANT_DETAIL_COVERAGE_PMS AS (
	SELECT
	claimant_cov_det_ak_id,
	claim_party_occurrence_ak_id,
	loc_unit_num,
	sub_loc_unit_num,
	ins_line,
	risk_unit_grp,
	risk_unit_grp_seq_num,
	risk_unit,
	risk_unit_seq_num,
	major_peril_code,
	major_peril_seq,
	pms_loss_disability,
	reserve_ctgry,
	cause_of_loss,
	pms_mbr
	FROM (
		SELECT 
		A.claimant_cov_det_ak_id as claimant_cov_det_ak_id, 
		A.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, 
		LTRIM(RTRIM(A.loc_unit_num)) as loc_unit_num, 
		LTRIM(RTRIM(A.sub_loc_unit_num)) as sub_loc_unit_num, A.ins_line as ins_line, 
		LTRIM(RTRIM(A.risk_unit_grp)) as risk_unit_grp, 
		LTRIM(RTRIM(A.risk_unit_grp_seq_num)) as risk_unit_grp_seq_num, 
		LTRIM(RTRIM(A.risk_unit)) as risk_unit, 
		A.risk_unit_seq_num as risk_unit_seq_num, 
		LTRIM(RTRIM(A.major_peril_code)) as major_peril_code, 
		A.major_peril_seq as major_peril_seq, A.pms_loss_disability as pms_loss_disability, A.reserve_ctgry as reserve_ctgry, 
		A.cause_of_loss as cause_of_loss, A.pms_mbr as pms_mbr
		
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_detail A
		
		WHERE     
		(A.source_sys_id = 'PMS')  
		AND 
		(A.CRRNT_SNPSHT_FLAG='1')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id,loc_unit_num,sub_loc_unit_num,ins_line,risk_unit_grp,risk_unit_grp_seq_num,risk_unit,risk_unit_seq_num,major_peril_code,major_peril_seq,pms_loss_disability,reserve_ctgry,cause_of_loss,pms_mbr ORDER BY claimant_cov_det_ak_id) = 1
),
LKP_Reinsurance_Coverage AS (
	SELECT
	reins_cov_ak_id,
	pol_ak_id,
	reins_co_num
	FROM (
		SELECT RC.reins_cov_ak_id as reins_cov_ak_id, 
		RC.pol_ak_id as pol_ak_id, 
		LTRIM(RTRIM(RC.reins_co_num)) as reins_co_num
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.reinsurance_coverage RC
		WHERE crrnt_snpsht_flag =1 AND reins_section_code = 'N/A'
		
		--- We use reins_section_code to differentiate between records from PIF_40 and PIF_4578 stage tables.
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id,reins_co_num ORDER BY reins_cov_ak_id DESC) = 1
),
LKP_V2_Policy AS (
	SELECT
	pol_ak_id,
	pol_key
	FROM (
		SELECT policy.pol_ak_id as pol_ak_id, 
		policy.pol_key as pol_key 
		FROM 
		V2.policy
		Where crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_ak_id DESC) = 1
),
SQ_pifmstr_PIF_4578_stage_90_91 AS (
	SELECT A.pif_4578_stage_id, A.pif_symbol, A.pif_policy_number, A.pif_module, A.loss_rec_length, A.loss_action_code, A.loss_file_id, A.loss_id, A.loss_insurance_line, A.loss_location_number, A.loss_sub_location_number, A.loss_risk_unit_group, A.loss_class_code_group, A.loss_class_code_member, A.loss_unit, A.loss_sequence_risk_unit, A.loss_type_exposure, A.loss_major_peril, A.loss_major_peril_seq, A.loss_year_item_effective, A.loss_month_item_effective, A.loss_day_item_effective, A.loss_part, A.loss_year, A.loss_month, A.loss_day, A.loss_occurence, A.loss_claimant, A.loss_member, A.loss_disability, A.loss_reserve_category, A.loss_layer, A.loss_reins_key_id, A.loss_reins_co_no, A.loss_reins_broker, A.loss_base_transaction, A.loss_transaction, A.loss_draft_control_seq, A.loss_sub_part_code, A.loss_segment_status, A.loss_entry_operator, A.loss_transaction_category, A.loss_year_reported, A.loss_month_reported, A.loss_day_reported, A.loss_cause, A.loss_adjustor_no, A.loss_examiner, A.loss_cost_containment, A.loss_paid_or_resv_amt, A.loss_bank_number, A.loss_draft_amount, A.loss_draft_no, A.loss_draft_check_ind, A.loss_transaction_date, A.loss_draft_pay_to_1, A.loss_draft_pay_to_2, A.loss_draft_pay_to_3, A.loss_draft_mail_to, A.loss_net_change_dollars, A.loss_account_entered_date, A.loss_average_reserve_code, A.loss_handling_office, A.loss_start_yr, A.loss_start_mo, A.loss_start_da, A.loss_fault_code, A.tc, A.ia, A.loss_payment_rate, A.loss_frequency, A.loss_period_pay, A.loss_sub_line, A.loss_payee_phrase, A.loss_memo_phrase, A.iws_origin_indicator, A.loss_aia_codes_1_2, A.loss_aia_codes_3_4, A.loss_aia_codes_5_6, A.loss_aia_sub_code, A.loss_accident_state, A.loss_handling_branch, A.loss_1099_number, A.loss_claim_payee, A.loss_claim_payee_name, A.loss_notes_draft_payee, A.loss_claim_number, A.loss_type_claim_payee, A.loss_zpcd_inj_loc, A.loss_special_use_1, A.loss_special_use_2, A.loss_time, A.loss_type_disability, A.loss_claims_made_ind, A.loss_misc_adjustor_ind, A.loss_pms_future_use, A.loss_offset_onset_ind, A.loss_sub_cont_id, A.loss_rpt_year, A.loss_rpt_mon, A.loss_rpt_day, A.loss_s3_transaction_date, A.loss_rr_reported_date, A.loss_yr2000_cust_use, A.loss_duplicate_key_sequence, A.inf_action, A.logical_flag, A.extract_date, A.as_of_date, A.record_count, A.source_system_id 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_4578_stage A
	where loss_part= 8 
	and LTRIM(RTRIM(logical_flag)) in ('0','-1')
	AND A.loss_transaction IN ('90', '91') 
	order by pif_symbol,pif_policy_number,pif_module,loss_year,loss_month,loss_day,loss_occurence,loss_claimant,loss_location_number,
	loss_sub_location_number, loss_insurance_line,loss_risk_unit_group,loss_unit,loss_major_peril,loss_major_peril_seq,
	loss_disability,loss_reserve_category,loss_cause,loss_member,loss_type_exposure,loss_offset_onset_ind,loss_transaction_date,
	loss_transaction_category,loss_draft_no,loss_sequence_risk_unit,loss_transaction, loss_reins_co_no, loss_base_transaction, loss_adjustor_no,loss_account_entered_date,loss_notes_draft_payee --//Commenting ORDER BY
),
AGG_amts_on_90_91 AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module,
	loss_year,
	loss_month,
	loss_day,
	loss_occurence,
	loss_claimant,
	loss_location_number,
	loss_sub_location_number,
	loss_insurance_line,
	loss_risk_unit_group,
	loss_unit,
	loss_major_peril,
	loss_major_peril_seq,
	loss_disability,
	loss_reserve_category,
	loss_cause,
	loss_member,
	loss_type_exposure,
	loss_offset_onset_ind,
	loss_transaction_date,
	loss_transaction_category,
	loss_draft_no,
	loss_sequence_risk_unit,
	loss_transaction,
	loss_reins_co_no,
	pif_4578_stage_id,
	loss_rec_length,
	loss_action_code,
	loss_file_id,
	loss_id,
	loss_class_code_group,
	loss_class_code_member,
	loss_year_item_effective,
	loss_month_item_effective,
	loss_day_item_effective,
	loss_part,
	loss_layer,
	loss_reins_key_id,
	loss_reins_broker,
	loss_base_transaction,
	loss_draft_control_seq,
	loss_sub_part_code,
	loss_segment_status,
	loss_entry_operator,
	loss_year_reported,
	loss_month_reported,
	loss_day_reported,
	loss_adjustor_no,
	loss_examiner,
	loss_cost_containment,
	loss_paid_or_resv_amt AS in_loss_paid_or_resv_amt,
	-- *INF*: SUM(in_loss_paid_or_resv_amt)
	SUM(in_loss_paid_or_resv_amt) AS loss_paid_or_resv_amt,
	loss_bank_number,
	loss_draft_amount,
	loss_draft_check_ind,
	loss_draft_pay_to_1,
	loss_draft_pay_to_2,
	loss_draft_pay_to_3,
	loss_draft_mail_to,
	loss_net_change_dollars AS in_loss_net_change_dollars,
	-- *INF*: SUM(in_loss_net_change_dollars)
	SUM(in_loss_net_change_dollars) AS loss_net_change_dollars,
	loss_account_entered_date,
	loss_average_reserve_code,
	loss_handling_office,
	loss_start_yr,
	loss_start_mo,
	loss_start_da,
	loss_fault_code,
	tc,
	ia,
	loss_payment_rate,
	loss_frequency,
	loss_period_pay,
	loss_sub_line,
	loss_payee_phrase,
	loss_memo_phrase,
	iws_origin_indicator,
	loss_aia_codes_1_2,
	loss_aia_codes_3_4,
	loss_aia_codes_5_6,
	loss_aia_sub_code,
	loss_accident_state,
	loss_handling_branch,
	loss_1099_number,
	loss_claim_payee,
	loss_claim_payee_name,
	loss_notes_draft_payee,
	loss_claim_number,
	loss_type_claim_payee,
	loss_zpcd_inj_loc,
	loss_special_use_1,
	loss_special_use_2,
	loss_time,
	loss_type_disability,
	loss_claims_made_ind,
	loss_misc_adjustor_ind,
	loss_pms_future_use,
	loss_sub_cont_id,
	loss_rpt_year,
	loss_rpt_mon,
	loss_rpt_day,
	loss_s3_transaction_date,
	loss_rr_reported_date,
	loss_yr2000_cust_use,
	loss_duplicate_key_sequence,
	inf_action,
	logical_flag,
	extract_date,
	as_of_date,
	record_count,
	source_system_id
	FROM SQ_pifmstr_PIF_4578_stage_90_91
	GROUP BY pif_symbol, pif_policy_number, pif_module, loss_year, loss_month, loss_day, loss_occurence, loss_claimant, loss_location_number, loss_sub_location_number, loss_insurance_line, loss_risk_unit_group, loss_unit, loss_major_peril, loss_major_peril_seq, loss_disability, loss_reserve_category, loss_cause, loss_member, loss_type_exposure, loss_offset_onset_ind, loss_transaction_date, loss_transaction_category, loss_draft_no, loss_sequence_risk_unit, loss_transaction, loss_reins_co_no
),
SQ_pifmstr_PIF_4578_stage AS (
	SELECT A.pif_4578_stage_id, A.pif_symbol, A.pif_policy_number, A.pif_module, A.loss_rec_length, A.loss_action_code, A.loss_file_id, A.loss_id, A.loss_insurance_line, A.loss_location_number, A.loss_sub_location_number, A.loss_risk_unit_group, A.loss_class_code_group, A.loss_class_code_member, A.loss_unit, A.loss_sequence_risk_unit, A.loss_type_exposure, A.loss_major_peril, A.loss_major_peril_seq, A.loss_year_item_effective, A.loss_month_item_effective, A.loss_day_item_effective, A.loss_part, A.loss_year, A.loss_month, A.loss_day, A.loss_occurence, A.loss_claimant, A.loss_member, A.loss_disability, A.loss_reserve_category, A.loss_layer, A.loss_reins_key_id, A.loss_reins_co_no, A.loss_reins_broker, A.loss_base_transaction, A.loss_transaction, A.loss_draft_control_seq, A.loss_sub_part_code, A.loss_segment_status, A.loss_entry_operator, A.loss_transaction_category, A.loss_year_reported, A.loss_month_reported, A.loss_day_reported, A.loss_cause, A.loss_adjustor_no, A.loss_examiner, A.loss_cost_containment, A.loss_paid_or_resv_amt, A.loss_bank_number, A.loss_draft_amount, A.loss_draft_no, A.loss_draft_check_ind, A.loss_transaction_date, A.loss_draft_pay_to_1, A.loss_draft_pay_to_2, A.loss_draft_pay_to_3, A.loss_draft_mail_to, A.loss_net_change_dollars, A.loss_account_entered_date, A.loss_average_reserve_code, A.loss_handling_office, A.loss_start_yr, A.loss_start_mo, A.loss_start_da, A.loss_fault_code, A.tc, A.ia, A.loss_payment_rate, A.loss_frequency, A.loss_period_pay, A.loss_sub_line, A.loss_payee_phrase, A.loss_memo_phrase, A.iws_origin_indicator, A.loss_aia_codes_1_2, A.loss_aia_codes_3_4, A.loss_aia_codes_5_6, A.loss_aia_sub_code, A.loss_accident_state, A.loss_handling_branch, A.loss_1099_number, A.loss_claim_payee, A.loss_claim_payee_name, A.loss_notes_draft_payee, A.loss_claim_number, A.loss_type_claim_payee, A.loss_zpcd_inj_loc, A.loss_special_use_1, A.loss_special_use_2, A.loss_time, A.loss_type_disability, A.loss_claims_made_ind, A.loss_misc_adjustor_ind, A.loss_pms_future_use, A.loss_offset_onset_ind, A.loss_sub_cont_id, A.loss_rpt_year, A.loss_rpt_mon, A.loss_rpt_day, A.loss_s3_transaction_date, A.loss_rr_reported_date, A.loss_yr2000_cust_use, A.loss_duplicate_key_sequence, A.inf_action, A.logical_flag, A.extract_date, A.as_of_date, A.record_count, A.source_system_id 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_4578_stage A
	where loss_part= 8 
	and LTRIM(RTRIM(logical_flag)) in ('0','-1')
	AND A.loss_transaction NOT IN('90', '91')
),
Union_stage_90_91 AS (
	SELECT pif_4578_stage_id, pif_symbol, pif_policy_number, pif_module, loss_rec_length, loss_action_code, loss_file_id, loss_id, loss_insurance_line, loss_location_number, loss_sub_location_number, loss_risk_unit_group, loss_class_code_group, loss_class_code_member, loss_unit, loss_sequence_risk_unit, loss_type_exposure, loss_major_peril, loss_major_peril_seq, loss_year_item_effective, loss_month_item_effective, loss_day_item_effective, loss_part, loss_year, loss_month, loss_day, loss_occurence, loss_claimant, loss_member, loss_disability, loss_reserve_category, loss_layer, loss_reins_key_id, loss_reins_co_no, loss_reins_broker, loss_base_transaction, loss_transaction, loss_draft_control_seq, loss_sub_part_code, loss_segment_status, loss_entry_operator, loss_transaction_category, loss_year_reported, loss_month_reported, loss_day_reported, loss_cause, loss_adjustor_no, loss_examiner, loss_cost_containment, loss_paid_or_resv_amt, loss_bank_number, loss_draft_amount, loss_draft_no, loss_draft_check_ind, loss_transaction_date, loss_draft_pay_to_1, loss_draft_pay_to_2, loss_draft_pay_to_3, loss_draft_mail_to, loss_net_change_dollars, loss_account_entered_date, loss_average_reserve_code, loss_handling_office, loss_start_yr, loss_start_mo, loss_start_da, loss_fault_code, tc, ia, loss_payment_rate, loss_frequency, loss_period_pay, loss_sub_line, loss_payee_phrase, loss_memo_phrase, iws_origin_indicator, loss_aia_codes_1_2, loss_aia_codes_3_4, loss_aia_codes_5_6, loss_aia_sub_code, loss_accident_state, loss_handling_branch, loss_1099_number, loss_claim_payee, loss_claim_payee_name, loss_notes_draft_payee, loss_claim_number, loss_type_claim_payee, loss_zpcd_inj_loc, loss_special_use_1, loss_special_use_2, loss_time, loss_type_disability, loss_claims_made_ind, loss_misc_adjustor_ind, loss_pms_future_use, loss_offset_onset_ind, loss_sub_cont_id, loss_rpt_year, loss_rpt_mon, loss_rpt_day, loss_s3_transaction_date, loss_rr_reported_date, loss_yr2000_cust_use, loss_duplicate_key_sequence, inf_action, extract_date, as_of_date, record_count, source_system_id, logical_flag AS Logical_Flag
	FROM AGG_amts_on_90_91
	UNION
	SELECT pif_4578_stage_id, pif_symbol, pif_policy_number, pif_module, loss_rec_length, loss_action_code, loss_file_id, loss_id, loss_insurance_line, loss_location_number, loss_sub_location_number, loss_risk_unit_group, loss_class_code_group, loss_class_code_member, loss_unit, loss_sequence_risk_unit, loss_type_exposure, loss_major_peril, loss_major_peril_seq, loss_year_item_effective, loss_month_item_effective, loss_day_item_effective, loss_part, loss_year, loss_month, loss_day, loss_occurence, loss_claimant, loss_member, loss_disability, loss_reserve_category, loss_layer, loss_reins_key_id, loss_reins_co_no, loss_reins_broker, loss_base_transaction, loss_transaction, loss_draft_control_seq, loss_sub_part_code, loss_segment_status, loss_entry_operator, loss_transaction_category, loss_year_reported, loss_month_reported, loss_day_reported, loss_cause, loss_adjustor_no, loss_examiner, loss_cost_containment, loss_paid_or_resv_amt, loss_bank_number, loss_draft_amount, loss_draft_no, loss_draft_check_ind, loss_transaction_date, loss_draft_pay_to_1, loss_draft_pay_to_2, loss_draft_pay_to_3, loss_draft_mail_to, loss_net_change_dollars, loss_account_entered_date, loss_average_reserve_code, loss_handling_office, loss_start_yr, loss_start_mo, loss_start_da, loss_fault_code, tc, ia, loss_payment_rate, loss_frequency, loss_period_pay, loss_sub_line, loss_payee_phrase, loss_memo_phrase, iws_origin_indicator, loss_aia_codes_1_2, loss_aia_codes_3_4, loss_aia_codes_5_6, loss_aia_sub_code, loss_accident_state, loss_handling_branch, loss_1099_number, loss_claim_payee, loss_claim_payee_name, loss_notes_draft_payee, loss_claim_number, loss_type_claim_payee, loss_zpcd_inj_loc, loss_special_use_1, loss_special_use_2, loss_time, loss_type_disability, loss_claims_made_ind, loss_misc_adjustor_ind, loss_pms_future_use, loss_offset_onset_ind, loss_sub_cont_id, loss_rpt_year, loss_rpt_mon, loss_rpt_day, loss_s3_transaction_date, loss_rr_reported_date, loss_yr2000_cust_use, loss_duplicate_key_sequence, inf_action, extract_date, as_of_date, record_count, source_system_id, logical_flag AS Logical_Flag
	FROM SQ_pifmstr_PIF_4578_stage
),
EXP_CLAIM_TRANSACTION_VALIDATE AS (
	SELECT
	pif_4578_stage_id AS PIF_4578_stage_id,
	pif_symbol AS PIF_SYMBOL,
	pif_policy_number AS PIF_POLICY_NUMBER,
	pif_module AS PIF_MODULE,
	loss_id AS LOSS_ID,
	loss_insurance_line AS LOSS_INSURANCE_LINE,
	loss_location_number AS LOSS_LOCATION_NUMBER,
	loss_sub_location_number AS LOSS_SUB_LOCATION_NUMBER,
	loss_risk_unit_group AS LOSS_RISK_UNIT_GROUP,
	loss_class_code_group AS LOSS_CLASS_CODE_GROUP,
	loss_class_code_member AS LOSS_CLASS_CODE_MEMBER,
	loss_unit AS LOSS_UNIT,
	loss_sequence_risk_unit AS LOSS_SEQUENCE_RISK_UNIT,
	loss_type_exposure AS LOSS_TYPE_EXPOSURE,
	loss_major_peril AS LOSS_MAJOR_PERIL,
	loss_major_peril_seq AS LOSS_MAJOR_PERIL_SEQ,
	loss_year AS LOSS_YEAR,
	loss_month AS LOSS_MONTH,
	loss_day AS LOSS_DAY,
	loss_occurence AS LOSS_OCCURENCE,
	loss_claimant AS LOSS_CLAIMANT,
	loss_member AS LOSS_MEMBER,
	loss_disability AS LOSS_DISABILITY,
	loss_reserve_category AS LOSS_RESERVE_CATEGORY,
	loss_reins_key_id AS LOSS_REINS_KEY_ID,
	loss_reins_co_no AS LOSS_REINS_CO_NO,
	loss_base_transaction AS LOSS_BASE_TRANSACTION,
	loss_transaction AS LOSS_TRANSACTION,
	loss_entry_operator AS LOSS_ENTRY_OPERATOR,
	loss_transaction_category AS LOSS_TRANSACTION_CATEGORY,
	loss_cause AS LOSS_CAUSE,
	loss_adjustor_no AS LOSS_ADJUSTOR_NO,
	loss_paid_or_resv_amt AS LOSS_PAID_OR_RESV_AMT,
	loss_draft_amount AS LOSS_DRAFT_AMOUNT,
	loss_draft_no AS LOSS_DRAFT_NO,
	loss_transaction_date AS LOSS_TRANSACTION_DATE,
	loss_draft_pay_to_1 AS LOSS_DRAFT_PAY_TO_1,
	loss_draft_pay_to_2 AS LOSS_DRAFT_PAY_TO_2,
	loss_draft_pay_to_3 AS LOSS_DRAFT_PAY_TO_3,
	loss_draft_mail_to AS LOSS_DRAFT_MAIL_TO,
	loss_net_change_dollars AS LOSS_NET_CHANGE_DOLLARS,
	loss_account_entered_date AS LOSS_ACCOUNT_ENTERED_DATE,
	loss_notes_draft_payee AS LOSS_NOTES_DRAFT_PAYEE,
	loss_claim_number AS LOSS_CLAIM_NUMBER,
	loss_type_disability AS LOSS_TYPE_DISABILITY,
	loss_offset_onset_ind AS LOSS_OFFSET_ONSET_IND,
	loss_time AS LOSS_TIME,
	Logical_Flag AS LOGICAL_FLAG,
	source_system_id AS SOURCE_SYSTEM_ID,
	loss_claim_payee_name,
	'1' AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id
	FROM Union_stage_90_91
),
EXP_CLAIMS_TRANSACTION_DEFAULT AS (
	SELECT
	LOSS_DRAFT_PAY_TO_1 AS IN_LOSS_DRAFT_PAY_TO_1,
	LOSS_DRAFT_PAY_TO_2 AS IN_LOSS_DRAFT_PAY_TO_2,
	LOSS_DRAFT_PAY_TO_3 AS IN_LOSS_DRAFT_PAY_TO_3,
	LOSS_DRAFT_MAIL_TO AS IN_LOSS_DRAFT_MAIL_TO,
	PIF_4578_stage_id AS IN_PIF_4578_stage_id,
	PIF_SYMBOL AS IN_PIF_SYMBOL,
	PIF_POLICY_NUMBER AS IN_PIF_POLICY_NUMBER,
	PIF_MODULE AS IN_PIF_MODULE,
	LOSS_ID AS IN_LOSS_ID,
	LOSS_INSURANCE_LINE AS IN_LOSS_INSURANCE_LINE,
	LOSS_LOCATION_NUMBER AS IN_LOSS_LOCATION_NUMBER,
	LOSS_SUB_LOCATION_NUMBER AS IN_LOSS_SUB_LOCATION_NUMBER,
	LOSS_RISK_UNIT_GROUP AS IN_LOSS_RISK_UNIT_GROUP,
	LOSS_CLASS_CODE_GROUP AS IN_LOSS_CLASS_CODE_GROUP,
	LOSS_CLASS_CODE_MEMBER AS IN_LOSS_CLASS_CODE_MEMBER,
	LOSS_UNIT AS IN_LOSS_UNIT,
	LOSS_SEQUENCE_RISK_UNIT AS IN_LOSS_SEQUENCE_RISK_UNIT,
	LOSS_TYPE_EXPOSURE AS IN_LOSS_TYPE_EXPOSURE,
	LOSS_MAJOR_PERIL AS IN_LOSS_MAJOR_PERIL,
	LOSS_MAJOR_PERIL_SEQ AS IN_LOSS_MAJOR_PERIL_SEQ,
	LOSS_YEAR AS IN_LOSS_YEAR,
	LOSS_MONTH AS IN_LOSS_MONTH,
	LOSS_DAY AS IN_LOSS_DAY,
	LOSS_OCCURENCE AS IN_LOSS_OCCURENCE,
	LOSS_CLAIMANT AS IN_LOSS_CLAIMANT,
	LOSS_MEMBER AS IN_LOSS_MEMBER,
	LOSS_DISABILITY AS IN_LOSS_DISABILITY,
	LOSS_RESERVE_CATEGORY AS IN_LOSS_RESERVE_CATEGORY,
	LOSS_TRANSACTION AS IN_LOSS_TRANSACTION,
	LOSS_ENTRY_OPERATOR AS IN_LOSS_ENTRY_OPERATOR,
	LOSS_TRANSACTION_CATEGORY AS IN_LOSS_TRANSACTION_CATEGORY,
	LOSS_CAUSE AS IN_LOSS_CAUSE,
	LOSS_PAID_OR_RESV_AMT AS IN_LOSS_PAID_OR_RESV_AMT,
	LOSS_DRAFT_NO AS IN_LOSS_DRAFT_NO,
	LOSS_DRAFT_AMOUNT AS IN_LOSS_DRAFT_AMOUNT,
	LOSS_TRANSACTION_DATE AS IN_LOSS_TRANSACTION_DATE,
	LOSS_NET_CHANGE_DOLLARS AS IN_LOSS_NET_CHANGE_DOLLARS,
	LOSS_ACCOUNT_ENTERED_DATE AS IN_LOSS_ACCOUNT_ENTERED_DATE,
	LOSS_TYPE_DISABILITY AS IN_LOSS_TYPE_DISABILITY,
	LOSS_OFFSET_ONSET_IND AS IN_LOSS_OFFSET_ONSET_IND,
	SOURCE_SYSTEM_ID AS IN_SOURCE_SYSTEM_ID,
	LOSS_REINS_CO_NO AS In_LOSS_REINS_CO_NO,
	LOSS_REINS_KEY_ID AS IN_LOSS_REINS_KEY_ID,
	LOSS_BASE_TRANSACTION AS IN_LOSS_BASE_TRANSACTION,
	LOSS_TIME AS IN_LOSS_TIME,
	LOGICAL_FLAG AS IN_LOGICAL_FLAG,
	crrnt_snpsht_flag AS IN_crrnt_snpsht_flag,
	audit_id AS IN_audit_id,
	LOSS_ADJUSTOR_NO AS IN_LOSS_ADJUSTOR_NO,
	LOSS_NOTES_DRAFT_PAYEE AS IN_LOSS_NOTES_DRAFT_PAYEE,
	LOSS_CLAIM_NUMBER AS IN_LOSS_CLAIM_NUMBER,
	loss_claim_payee_name AS IN_loss_claim_payee_name,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_CLAIM_NUMBER))),'N/A',IIF(IS_SPACES(IN_LOSS_CLAIM_NUMBER),'N/A',LTRIM(RTRIM(IN_LOSS_CLAIM_NUMBER))))
	IFF(LTRIM(RTRIM(IN_LOSS_CLAIM_NUMBER)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_CLAIM_NUMBER), 'N/A', LTRIM(RTRIM(IN_LOSS_CLAIM_NUMBER)))) AS LOSS_CLAIM_NUMBER,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_loss_claim_payee_name))),'N/A',IIF(IS_SPACES(IN_loss_claim_payee_name),'N/A',LTRIM(RTRIM(IN_loss_claim_payee_name))))
	IFF(LTRIM(RTRIM(IN_loss_claim_payee_name)) IS NULL, 'N/A', IFF(IS_SPACES(IN_loss_claim_payee_name), 'N/A', LTRIM(RTRIM(IN_loss_claim_payee_name)))) AS Claim_Reins_Broker_name,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_NOTES_DRAFT_PAYEE))),'N/A',IIF(IS_SPACES(IN_LOSS_NOTES_DRAFT_PAYEE),'N/A',LTRIM(RTRIM(IN_LOSS_NOTES_DRAFT_PAYEE))))
	IFF(LTRIM(RTRIM(IN_LOSS_NOTES_DRAFT_PAYEE)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_NOTES_DRAFT_PAYEE), 'N/A', LTRIM(RTRIM(IN_LOSS_NOTES_DRAFT_PAYEE)))) AS LOSS_NOTES_DRAFT_PAYEE,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_ADJUSTOR_NO))),'N/A',IIF(IS_SPACES(IN_LOSS_ADJUSTOR_NO),'N/A',LTRIM(RTRIM(IN_LOSS_ADJUSTOR_NO))))
	IFF(LTRIM(RTRIM(IN_LOSS_ADJUSTOR_NO)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_ADJUSTOR_NO), 'N/A', LTRIM(RTRIM(IN_LOSS_ADJUSTOR_NO)))) AS LOSS_ADJUSTOR_NO,
	-- *INF*: IIF(ISNULL(IN_LOSS_BASE_TRANSACTION),'N/A',TO_CHAR(IN_LOSS_BASE_TRANSACTION))
	-- 
	IFF(IN_LOSS_BASE_TRANSACTION IS NULL, 'N/A', TO_CHAR(IN_LOSS_BASE_TRANSACTION)) AS LOSS_BASE_TRANSACTION,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_REINS_KEY_ID))),'N/A',IIF(IS_SPACES(IN_LOSS_REINS_KEY_ID),'N/A',LTRIM(RTRIM(IN_LOSS_REINS_KEY_ID))))
	IFF(LTRIM(RTRIM(IN_LOSS_REINS_KEY_ID)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_REINS_KEY_ID), 'N/A', LTRIM(RTRIM(IN_LOSS_REINS_KEY_ID)))) AS LOSS_REINS_KEY_ID,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(In_LOSS_REINS_CO_NO))),'N/A',IIF(IS_SPACES(In_LOSS_REINS_CO_NO),'N/A',LTRIM(RTRIM(In_LOSS_REINS_CO_NO))))
	IFF(LTRIM(RTRIM(In_LOSS_REINS_CO_NO)) IS NULL, 'N/A', IFF(IS_SPACES(In_LOSS_REINS_CO_NO), 'N/A', LTRIM(RTRIM(In_LOSS_REINS_CO_NO)))) AS LOSS_REINS_CO_NO,
	IN_PIF_4578_stage_id AS PIF_4578_stage_id,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_PIF_SYMBOL))),'N/A',IIF(IS_SPACES(IN_PIF_SYMBOL),'N/A',LTRIM(RTRIM(IN_PIF_SYMBOL))))
	IFF(LTRIM(RTRIM(IN_PIF_SYMBOL)) IS NULL, 'N/A', IFF(IS_SPACES(IN_PIF_SYMBOL), 'N/A', LTRIM(RTRIM(IN_PIF_SYMBOL)))) AS PIF_SYMBOL,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_PIF_POLICY_NUMBER))),'N/A',IIF(IS_SPACES(IN_PIF_POLICY_NUMBER),'N/A',LTRIM(RTRIM(IN_PIF_POLICY_NUMBER))))
	IFF(LTRIM(RTRIM(IN_PIF_POLICY_NUMBER)) IS NULL, 'N/A', IFF(IS_SPACES(IN_PIF_POLICY_NUMBER), 'N/A', LTRIM(RTRIM(IN_PIF_POLICY_NUMBER)))) AS PIF_POLICY_NUMBER,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_PIF_MODULE))),'N/A',IIF(IS_SPACES(IN_PIF_MODULE),'N/A',LTRIM(RTRIM(IN_PIF_MODULE))))
	IFF(LTRIM(RTRIM(IN_PIF_MODULE)) IS NULL, 'N/A', IFF(IS_SPACES(IN_PIF_MODULE), 'N/A', LTRIM(RTRIM(IN_PIF_MODULE)))) AS PIF_MODULE,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_ID))),'N/A',IIF(IS_SPACES(IN_LOSS_ID),'N/A',LTRIM(RTRIM(IN_LOSS_ID))))
	IFF(LTRIM(RTRIM(IN_LOSS_ID)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_ID), 'N/A', LTRIM(RTRIM(IN_LOSS_ID)))) AS LOSS_ID,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_INSURANCE_LINE))) OR IS_SPACES(IN_LOSS_INSURANCE_LINE),'N/A',LTRIM(RTRIM(IN_LOSS_INSURANCE_LINE)))
	IFF(LTRIM(RTRIM(IN_LOSS_INSURANCE_LINE)) IS NULL OR IS_SPACES(IN_LOSS_INSURANCE_LINE), 'N/A', LTRIM(RTRIM(IN_LOSS_INSURANCE_LINE))) AS LOSS_INSURANCE_LINE,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_INSURANCE_LINE))) OR IS_SPACES(IN_LOSS_INSURANCE_LINE),'N/A',LTRIM(RTRIM(IN_LOSS_INSURANCE_LINE)))
	IFF(LTRIM(RTRIM(IN_LOSS_INSURANCE_LINE)) IS NULL OR IS_SPACES(IN_LOSS_INSURANCE_LINE), 'N/A', LTRIM(RTRIM(IN_LOSS_INSURANCE_LINE))) AS LOSS_INSURANCE_LINE_lkp,
	-- *INF*: IIF(ISNULL(IN_LOSS_LOCATION_NUMBER),0,IN_LOSS_LOCATION_NUMBER)
	IFF(IN_LOSS_LOCATION_NUMBER IS NULL, 0, IN_LOSS_LOCATION_NUMBER) AS v_LOSS_LOCATION_NUMBER,
	-- *INF*: IIF(ISNULL(IN_LOSS_LOCATION_NUMBER),'0000',LTRIM(RTRIM(LPAD(TO_CHAR(IN_LOSS_LOCATION_NUMBER),4,'0'))))
	IFF(IN_LOSS_LOCATION_NUMBER IS NULL, '0000', LTRIM(RTRIM(LPAD(TO_CHAR(IN_LOSS_LOCATION_NUMBER), 4, '0')))) AS LOSS_LOCATION_NUMBER,
	-- *INF*: IIF(ISNULL(IN_LOSS_SUB_LOCATION_NUMBER),0,IN_LOSS_SUB_LOCATION_NUMBER)
	IFF(IN_LOSS_SUB_LOCATION_NUMBER IS NULL, 0, IN_LOSS_SUB_LOCATION_NUMBER) AS v_LOSS_SUB_LOCATION_NUMBER,
	-- *INF*: IIF(ISNULL(IN_LOSS_SUB_LOCATION_NUMBER),'000',LTRIM(RTRIM(LPAD(TO_CHAR(IN_LOSS_SUB_LOCATION_NUMBER),3,'0'))))
	IFF(IN_LOSS_SUB_LOCATION_NUMBER IS NULL, '000', LTRIM(RTRIM(LPAD(TO_CHAR(IN_LOSS_SUB_LOCATION_NUMBER), 3, '0')))) AS LOSS_SUB_LOCATION_NUMBER,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_RISK_UNIT_GROUP))) OR IS_SPACES(IN_LOSS_RISK_UNIT_GROUP),'N/A',LTRIM(RTRIM(IN_LOSS_RISK_UNIT_GROUP)))
	IFF(LTRIM(RTRIM(IN_LOSS_RISK_UNIT_GROUP)) IS NULL OR IS_SPACES(IN_LOSS_RISK_UNIT_GROUP), 'N/A', LTRIM(RTRIM(IN_LOSS_RISK_UNIT_GROUP))) AS LOSS_RISK_UNIT_GROUP,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_RISK_UNIT_GROUP))),'0',IIF(IS_SPACES(IN_LOSS_RISK_UNIT_GROUP),'0',LTRIM(RTRIM(IN_LOSS_RISK_UNIT_GROUP))))
	IFF(LTRIM(RTRIM(IN_LOSS_RISK_UNIT_GROUP)) IS NULL, '0', IFF(IS_SPACES(IN_LOSS_RISK_UNIT_GROUP), '0', LTRIM(RTRIM(IN_LOSS_RISK_UNIT_GROUP)))) AS LOSS_RISK_UNIT_GROUP_lkp,
	IN_LOSS_CLASS_CODE_GROUP AS LOSS_CLASS_CODE_GROUP,
	-- *INF*: IIF(ISNULL(IN_LOSS_CLASS_CODE_GROUP), 0, IN_LOSS_CLASS_CODE_GROUP)
	IFF(IN_LOSS_CLASS_CODE_GROUP IS NULL, 0, IN_LOSS_CLASS_CODE_GROUP) AS LOSS_CLASS_CODE_GROUP_lkp,
	IN_LOSS_CLASS_CODE_MEMBER AS LOSS_CLASS_CODE_MEMBER,
	-- *INF*: IIF(ISNULL(IN_LOSS_CLASS_CODE_MEMBER), 0, IN_LOSS_CLASS_CODE_MEMBER)
	IFF(IN_LOSS_CLASS_CODE_MEMBER IS NULL, 0, IN_LOSS_CLASS_CODE_MEMBER) AS LOSS_CLASS_CODE_MEMBER_lkp,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_UNIT))),'N/A',IIF(IS_SPACES(IN_LOSS_UNIT),'N/A',LTRIM(RTRIM(IN_LOSS_UNIT))))
	IFF(LTRIM(RTRIM(IN_LOSS_UNIT)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_UNIT), 'N/A', LTRIM(RTRIM(IN_LOSS_UNIT)))) AS LOSS_UNIT,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_UNIT))) OR IS_SPACES(IN_LOSS_UNIT),'N/A',LTRIM(RTRIM(IN_LOSS_UNIT)))
	IFF(LTRIM(RTRIM(IN_LOSS_UNIT)) IS NULL OR IS_SPACES(IN_LOSS_UNIT), 'N/A', LTRIM(RTRIM(IN_LOSS_UNIT))) AS LOSS_UNIT_lkp,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_SEQUENCE_RISK_UNIT))) or IS_SPACES(IN_LOSS_SEQUENCE_RISK_UNIT),'0',LTRIM(RTRIM(IN_LOSS_SEQUENCE_RISK_UNIT)))
	-- 
	-- 
	-- ----IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_SEQUENCE_RISK_UNIT))) OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_SEQUENCE_RISK_UNIT))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_SEQUENCE_RISK_UNIT)))  = 0 ,'N/A',LTRIM(RTRIM(IN_LOSS_SEQUENCE_RISK_UNIT)))
	IFF(LTRIM(RTRIM(IN_LOSS_SEQUENCE_RISK_UNIT)) IS NULL OR IS_SPACES(IN_LOSS_SEQUENCE_RISK_UNIT), '0', LTRIM(RTRIM(IN_LOSS_SEQUENCE_RISK_UNIT))) AS LOSS_SEQUENCE_RISK_UNIT,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_SEQUENCE_RISK_UNIT))),'0',IIF(IS_SPACES(IN_LOSS_SEQUENCE_RISK_UNIT),'0',LTRIM(RTRIM(IN_LOSS_SEQUENCE_RISK_UNIT))))
	IFF(LTRIM(RTRIM(IN_LOSS_SEQUENCE_RISK_UNIT)) IS NULL, '0', IFF(IS_SPACES(IN_LOSS_SEQUENCE_RISK_UNIT), '0', LTRIM(RTRIM(IN_LOSS_SEQUENCE_RISK_UNIT)))) AS LOSS_SEQUENCE_RISK_UNIT_lkp,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_TYPE_EXPOSURE))),'N/A',IIF(IS_SPACES(IN_LOSS_TYPE_EXPOSURE),'N/A',LTRIM(RTRIM(IN_LOSS_TYPE_EXPOSURE))))
	IFF(LTRIM(RTRIM(IN_LOSS_TYPE_EXPOSURE)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_TYPE_EXPOSURE), 'N/A', LTRIM(RTRIM(IN_LOSS_TYPE_EXPOSURE)))) AS LOSS_TYPE_EXPOSURE,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_MAJOR_PERIL))),'N/A',IIF(IS_SPACES(IN_LOSS_MAJOR_PERIL),'N/A',LTRIM(RTRIM(IN_LOSS_MAJOR_PERIL))))
	IFF(LTRIM(RTRIM(IN_LOSS_MAJOR_PERIL)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_MAJOR_PERIL), 'N/A', LTRIM(RTRIM(IN_LOSS_MAJOR_PERIL)))) AS LOSS_MAJOR_PERIL,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_MAJOR_PERIL))),'0',IIF(IS_SPACES(IN_LOSS_MAJOR_PERIL),'0',LTRIM(RTRIM(IN_LOSS_MAJOR_PERIL))))
	IFF(LTRIM(RTRIM(IN_LOSS_MAJOR_PERIL)) IS NULL, '0', IFF(IS_SPACES(IN_LOSS_MAJOR_PERIL), '0', LTRIM(RTRIM(IN_LOSS_MAJOR_PERIL)))) AS LOSS_MAJOR_PERIL_lkp,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_MAJOR_PERIL_SEQ))),'N/A',IIF(IS_SPACES(IN_LOSS_MAJOR_PERIL_SEQ),'N/A',LTRIM(RTRIM(IN_LOSS_MAJOR_PERIL_SEQ))))
	IFF(LTRIM(RTRIM(IN_LOSS_MAJOR_PERIL_SEQ)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_MAJOR_PERIL_SEQ), 'N/A', LTRIM(RTRIM(IN_LOSS_MAJOR_PERIL_SEQ)))) AS LOSS_MAJOR_PERIL_SEQ,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_MAJOR_PERIL_SEQ))),'0',IIF(IS_SPACES(IN_LOSS_MAJOR_PERIL_SEQ),'0',LTRIM(RTRIM(IN_LOSS_MAJOR_PERIL_SEQ))))
	IFF(LTRIM(RTRIM(IN_LOSS_MAJOR_PERIL_SEQ)) IS NULL, '0', IFF(IS_SPACES(IN_LOSS_MAJOR_PERIL_SEQ), '0', LTRIM(RTRIM(IN_LOSS_MAJOR_PERIL_SEQ)))) AS LOSS_MAJOR_PERIL_SEQ_lkp,
	-- *INF*: IIF(ISNULL(IN_LOSS_YEAR),1800,IN_LOSS_YEAR)
	IFF(IN_LOSS_YEAR IS NULL, 1800, IN_LOSS_YEAR) AS LOSS_YEAR,
	-- *INF*: IIF(ISNULL(IN_LOSS_MONTH),01,IN_LOSS_MONTH)
	IFF(IN_LOSS_MONTH IS NULL, 01, IN_LOSS_MONTH) AS LOSS_MONTH,
	-- *INF*: IIF(ISNULL(IN_LOSS_DAY),01,IN_LOSS_DAY)
	IFF(IN_LOSS_DAY IS NULL, 01, IN_LOSS_DAY) AS LOSS_DAY,
	-- *INF*: IIF(ISNULL(IN_LOSS_OCCURENCE),'000',IN_LOSS_OCCURENCE)
	IFF(IN_LOSS_OCCURENCE IS NULL, '000', IN_LOSS_OCCURENCE) AS LOSS_OCCURENCE,
	-- *INF*: IIF(LENGTH(IN_LOSS_OCCURENCE)<=2,'0',SUBSTR(IN_LOSS_OCCURENCE,1,1))
	IFF(LENGTH(IN_LOSS_OCCURENCE) <= 2, '0', SUBSTR(IN_LOSS_OCCURENCE, 1, 1)) AS ipfcx6_loss_occurence_fdigit,
	-- *INF*: IIF(LENGTH(IN_LOSS_OCCURENCE)>2,SUBSTR(IN_LOSS_OCCURENCE,2,2),TO_CHAR(IN_LOSS_OCCURENCE))
	-- 
	-- 
	IFF(LENGTH(IN_LOSS_OCCURENCE) > 2, SUBSTR(IN_LOSS_OCCURENCE, 2, 2), TO_CHAR(IN_LOSS_OCCURENCE)) AS ipfcx6_usr_loss_occurence,
	-- *INF*: IIF(ISNULL(IN_LOSS_CLAIMANT),'000',IN_LOSS_CLAIMANT)
	IFF(IN_LOSS_CLAIMANT IS NULL, '000', IN_LOSS_CLAIMANT) AS LOSS_CLAIMANT,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_MEMBER))),'N/A',IIF(IS_SPACES(IN_LOSS_MEMBER),'N/A',LTRIM(RTRIM(IN_LOSS_MEMBER))))
	IFF(LTRIM(RTRIM(IN_LOSS_MEMBER)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_MEMBER), 'N/A', LTRIM(RTRIM(IN_LOSS_MEMBER)))) AS LOSS_MEMBER,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_MEMBER))),'0',IIF(IS_SPACES(IN_LOSS_MEMBER),'0',LTRIM(RTRIM(IN_LOSS_MEMBER))))
	IFF(LTRIM(RTRIM(IN_LOSS_MEMBER)) IS NULL, '0', IFF(IS_SPACES(IN_LOSS_MEMBER), '0', LTRIM(RTRIM(IN_LOSS_MEMBER)))) AS LOSS_MEMBER_lkp,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_DISABILITY))),'N/A',IIF(IS_SPACES(IN_LOSS_DISABILITY),'N/A',LTRIM(RTRIM(IN_LOSS_DISABILITY))))
	IFF(LTRIM(RTRIM(IN_LOSS_DISABILITY)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_DISABILITY), 'N/A', LTRIM(RTRIM(IN_LOSS_DISABILITY)))) AS LOSS_DISABILITY,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_DISABILITY))),'0',IIF(IS_SPACES(IN_LOSS_DISABILITY),'0',LTRIM(RTRIM(IN_LOSS_DISABILITY))))
	IFF(LTRIM(RTRIM(IN_LOSS_DISABILITY)) IS NULL, '0', IFF(IS_SPACES(IN_LOSS_DISABILITY), '0', LTRIM(RTRIM(IN_LOSS_DISABILITY)))) AS LOSS_DISABILITY_lkp,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_RESERVE_CATEGORY))),'N/A',IIF(IS_SPACES(IN_LOSS_RESERVE_CATEGORY),'N/A',LTRIM(RTRIM(IN_LOSS_RESERVE_CATEGORY))))
	IFF(LTRIM(RTRIM(IN_LOSS_RESERVE_CATEGORY)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_RESERVE_CATEGORY), 'N/A', LTRIM(RTRIM(IN_LOSS_RESERVE_CATEGORY)))) AS LOSS_RESERVE_CATEGORY,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_RESERVE_CATEGORY))),'0',IIF(IS_SPACES(IN_LOSS_RESERVE_CATEGORY),'0',LTRIM(RTRIM(IN_LOSS_RESERVE_CATEGORY))))
	IFF(LTRIM(RTRIM(IN_LOSS_RESERVE_CATEGORY)) IS NULL, '0', IFF(IS_SPACES(IN_LOSS_RESERVE_CATEGORY), '0', LTRIM(RTRIM(IN_LOSS_RESERVE_CATEGORY)))) AS LOSS_RESERVE_CATEGORY_lkp,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_TRANSACTION))),'N/A',IIF(IS_SPACES(IN_LOSS_TRANSACTION),'N/A',LTRIM(RTRIM(IN_LOSS_TRANSACTION))))
	IFF(LTRIM(RTRIM(IN_LOSS_TRANSACTION)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_TRANSACTION), 'N/A', LTRIM(RTRIM(IN_LOSS_TRANSACTION)))) AS LOSS_TRANSACTION,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_ENTRY_OPERATOR))),'N/A',IIF(IS_SPACES(IN_LOSS_ENTRY_OPERATOR),'N/A',LTRIM(RTRIM(IN_LOSS_ENTRY_OPERATOR))))
	IFF(LTRIM(RTRIM(IN_LOSS_ENTRY_OPERATOR)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_ENTRY_OPERATOR), 'N/A', LTRIM(RTRIM(IN_LOSS_ENTRY_OPERATOR)))) AS LOSS_ENTRY_OPERATOR,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_TRANSACTION_CATEGORY))),'N/A',IIF(IS_SPACES(IN_LOSS_TRANSACTION_CATEGORY),'N/A',LTRIM(RTRIM(IN_LOSS_TRANSACTION_CATEGORY))))
	IFF(LTRIM(RTRIM(IN_LOSS_TRANSACTION_CATEGORY)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_TRANSACTION_CATEGORY), 'N/A', LTRIM(RTRIM(IN_LOSS_TRANSACTION_CATEGORY)))) AS LOSS_TRANSACTION_CATEGORY,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_CAUSE))),'N/A',IIF(IS_SPACES(IN_LOSS_CAUSE),'N/A',LTRIM(RTRIM(IN_LOSS_CAUSE))))
	IFF(LTRIM(RTRIM(IN_LOSS_CAUSE)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_CAUSE), 'N/A', LTRIM(RTRIM(IN_LOSS_CAUSE)))) AS LOSS_CAUSE,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_CAUSE))),'0',IIF(IS_SPACES(IN_LOSS_CAUSE),'0',LTRIM(RTRIM(IN_LOSS_CAUSE))))
	IFF(LTRIM(RTRIM(IN_LOSS_CAUSE)) IS NULL, '0', IFF(IS_SPACES(IN_LOSS_CAUSE), '0', LTRIM(RTRIM(IN_LOSS_CAUSE)))) AS LOSS_CAUSE_lkp,
	-- *INF*: IIF(ISNULL(IN_LOSS_PAID_OR_RESV_AMT),0,IN_LOSS_PAID_OR_RESV_AMT)
	IFF(IN_LOSS_PAID_OR_RESV_AMT IS NULL, 0, IN_LOSS_PAID_OR_RESV_AMT) AS LOSS_PAID_OR_RESV_AMT,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_DRAFT_NO))),'N/A',IIF(IS_SPACES(IN_LOSS_DRAFT_NO),'N/A',LTRIM(RTRIM(IN_LOSS_DRAFT_NO))))
	IFF(LTRIM(RTRIM(IN_LOSS_DRAFT_NO)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_DRAFT_NO), 'N/A', LTRIM(RTRIM(IN_LOSS_DRAFT_NO)))) AS LOSS_DRAFT_NO,
	-- *INF*: IIF(ISNULL(IN_LOSS_NET_CHANGE_DOLLARS),0,IN_LOSS_NET_CHANGE_DOLLARS)
	IFF(IN_LOSS_NET_CHANGE_DOLLARS IS NULL, 0, IN_LOSS_NET_CHANGE_DOLLARS) AS LOSS_NET_CHANGE_DOLLARS,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_ACCOUNT_ENTERED_DATE))),'180001',IIF(IS_SPACES(IN_LOSS_ACCOUNT_ENTERED_DATE),'180001',LTRIM(RTRIM(IN_LOSS_ACCOUNT_ENTERED_DATE))))
	IFF(LTRIM(RTRIM(IN_LOSS_ACCOUNT_ENTERED_DATE)) IS NULL, '180001', IFF(IS_SPACES(IN_LOSS_ACCOUNT_ENTERED_DATE), '180001', LTRIM(RTRIM(IN_LOSS_ACCOUNT_ENTERED_DATE)))) AS LOSS_ACCOUNT_ENTERED_DATE,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_TYPE_DISABILITY))),'N/A',IIF(IS_SPACES(IN_LOSS_TYPE_DISABILITY),'N/A',
	-- LTRIM(RTRIM(IN_LOSS_TYPE_DISABILITY))))
	IFF(LTRIM(RTRIM(IN_LOSS_TYPE_DISABILITY)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_TYPE_DISABILITY), 'N/A', LTRIM(RTRIM(IN_LOSS_TYPE_DISABILITY)))) AS LOSS_TYPE_DISABILITY,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_OFFSET_ONSET_IND))),'N/A',IIF(IS_SPACES(IN_LOSS_OFFSET_ONSET_IND),'N/A',LTRIM(RTRIM(IN_LOSS_OFFSET_ONSET_IND))))
	IFF(LTRIM(RTRIM(IN_LOSS_OFFSET_ONSET_IND)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_OFFSET_ONSET_IND), 'N/A', LTRIM(RTRIM(IN_LOSS_OFFSET_ONSET_IND)))) AS LOSS_OFFSET_ONSET_IND,
	IN_LOSS_DRAFT_PAY_TO_1||IN_LOSS_DRAFT_PAY_TO_2||IN_LOSS_DRAFT_PAY_TO_3 AS v_pay_to_code,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE))), substr(ltrim(rtrim(IN_LOSS_ACCOUNT_ENTERED_DATE)),5 ,2),
	-- IS_SPACES(LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE))), substr(ltrim(rtrim(IN_LOSS_ACCOUNT_ENTERED_DATE)),5 ,2),
	-- length(LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE)))= 0 , substr(ltrim(rtrim(IN_LOSS_ACCOUNT_ENTERED_DATE)),5 ,2),
	-- substr(IN_LOSS_TRANSACTION_DATE,5,2)
	-- )
	-- 
	-- 
	-- 
	DECODE(TRUE,
		LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE)) IS NULL, substr(ltrim(rtrim(IN_LOSS_ACCOUNT_ENTERED_DATE)), 5, 2),
		IS_SPACES(LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE))), substr(ltrim(rtrim(IN_LOSS_ACCOUNT_ENTERED_DATE)), 5, 2),
		length(LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE))) = 0, substr(ltrim(rtrim(IN_LOSS_ACCOUNT_ENTERED_DATE)), 5, 2),
		substr(IN_LOSS_TRANSACTION_DATE, 5, 2)) AS V_Loss_transaction_month,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE))), '01',
	-- IS_SPACES(LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE))),'01',
	-- length(LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE)))= 0 , '01',
	-- substr(IN_LOSS_TRANSACTION_DATE,7,2)
	-- )
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	DECODE(TRUE,
		LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE)) IS NULL, '01',
		IS_SPACES(LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE))), '01',
		length(LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE))) = 0, '01',
		substr(IN_LOSS_TRANSACTION_DATE, 7, 2)) AS V_loss_transaction_day,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE))), substr(ltrim(rtrim(IN_LOSS_ACCOUNT_ENTERED_DATE)),1 ,4),
	-- IS_SPACES(LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE))), substr(ltrim(rtrim(IN_LOSS_ACCOUNT_ENTERED_DATE)),1 ,4),
	-- length(LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE)))= 0 , substr(ltrim(rtrim(IN_LOSS_ACCOUNT_ENTERED_DATE)),1 ,4),
	-- substr(IN_LOSS_TRANSACTION_DATE,1,4)
	-- )
	-- 
	-- 
	-- 
	DECODE(TRUE,
		LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE)) IS NULL, substr(ltrim(rtrim(IN_LOSS_ACCOUNT_ENTERED_DATE)), 1, 4),
		IS_SPACES(LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE))), substr(ltrim(rtrim(IN_LOSS_ACCOUNT_ENTERED_DATE)), 1, 4),
		length(LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE))) = 0, substr(ltrim(rtrim(IN_LOSS_ACCOUNT_ENTERED_DATE)), 1, 4),
		substr(IN_LOSS_TRANSACTION_DATE, 1, 4)) AS V_loss_trasaction_year,
	-- *INF*: Concat (V_Loss_transaction_month ,Concat ('/' , Concat (V_loss_transaction_day,Concat ('/', V_loss_trasaction_year))))
	Concat(V_Loss_transaction_month, Concat('/', Concat(V_loss_transaction_day, Concat('/', V_loss_trasaction_year)))) AS OUT_Loss_transaction_date,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_TIME))),'N/A',IIF(IS_SPACES(IN_LOSS_TIME),'N/A',LTRIM(RTRIM(IN_LOSS_TIME))))
	IFF(LTRIM(RTRIM(IN_LOSS_TIME)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_TIME), 'N/A', LTRIM(RTRIM(IN_LOSS_TIME)))) AS LOSS_TIME,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOGICAL_FLAG))),'N/A',IIF(IS_SPACES(IN_LOGICAL_FLAG),'N/A',LTRIM(RTRIM(IN_LOGICAL_FLAG))))
	IFF(LTRIM(RTRIM(IN_LOGICAL_FLAG)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOGICAL_FLAG), 'N/A', LTRIM(RTRIM(IN_LOGICAL_FLAG)))) AS LOGICAL_FLAG
	FROM EXP_CLAIM_TRANSACTION_VALIDATE
),
LKP_PIF_42GP AS (
	SELECT
	ipfcgp_claim_number,
	pif_symbol,
	pif_policy_number,
	pif_module,
	ipfcgp_year_of_loss,
	ipfcgp_month_of_loss,
	ipfcgp_day_of_loss,
	ipfcgp_loss_occurence
	FROM (
		SELECT ltrim(rtrim(pif_42gp_stage.ipfcgp_claim_number)) as ipfcgp_claim_number, 
		ltrim(rtrim(pif_42gp_stage.pif_symbol)) as pif_symbol, 
		ltrim(rtrim(pif_42gp_stage.pif_policy_number)) as pif_policy_number,
		ltrim(rtrim(pif_42gp_stage.pif_module)) as pif_module,
		pif_42gp_stage.ipfcgp_year_of_loss as ipfcgp_year_of_loss,
		pif_42gp_stage.ipfcgp_month_of_loss as ipfcgp_month_of_loss,
		pif_42gp_stage.ipfcgp_day_of_loss as ipfcgp_day_of_loss, 
		ltrim(rtrim(pif_42gp_stage.ipfcgp_loss_occurence)) as ipfcgp_loss_occurence 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_42gp_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module,ipfcgp_year_of_loss,ipfcgp_month_of_loss,ipfcgp_day_of_loss,ipfcgp_loss_occurence ORDER BY ipfcgp_claim_number) = 1
),
LKP_Claim_Occurrence_Exceed AS (
	SELECT
	claim_occurrence_ak_id,
	claim_occurrence_type_code,
	s3p_claim_num
	FROM (
		SELECT 
		claim_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id, 
		LTRIM(RTRIM(claim_occurrence.claim_occurrence_type_code)) as claim_occurrence_type_code, 
		LTRIM(RTRIM(claim_occurrence.s3p_claim_num)) as s3p_claim_num 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY s3p_claim_num ORDER BY claim_occurrence_ak_id) = 1
),
LKP_pif_42x6_stage AS (
	SELECT
	ipfcx6_year_process,
	ipfcx6_month_process,
	ipfcx6_day_process,
	pif_symbol,
	pif_policy_number,
	pif_module,
	ipfcx6_insurance_line,
	ipfcx6_location_number,
	ipfcx6_sub_location_number,
	ipfcx6_year_of_loss,
	ipfcx6_month_of_loss,
	ipfcx6_day_of_loss,
	ipfcx6_loss_occ_fdigit,
	ipfcx6_usr_loss_occurence,
	ipfcx6_risk_unit_group,
	ipfcx6_class_code_group,
	ipfcx6_class_code_member,
	ipfcx6_loss_unit,
	ipfcx6_risk_sequence,
	ipfcx6_major_peril,
	ipfcx6_sequence_type_exposure,
	ipfcx6_loss_disability,
	ipfcx6_member,
	ipfcx6_reserve_category,
	ipfcx6_loss_cause
	FROM (
		SELECT pif_42x6_stage.ipfcx6_year_process as ipfcx6_year_process, 
		pif_42x6_stage.ipfcx6_month_process as ipfcx6_month_process, 
		pif_42x6_stage.ipfcx6_day_process as ipfcx6_day_process, 
		CASE LEN(LTRIM(RTRIM(COALESCE(pif_symbol,' ')))) WHEN 0 THEN 'N/A' ELSE LTRIM(RTRIM(pif_symbol)) END AS pif_symbol, 
		CASE LEN(LTRIM(RTRIM(COALESCE(pif_policy_number,' ')))) WHEN 0 THEN '0' ELSE LTRIM(RTRIM(pif_policy_number)) END AS pif_policy_number, 
		CASE LEN(LTRIM(RTRIM(COALESCE(pif_module, ' ')))) WHEN 0 THEN '0' ELSE LTRIM(RTRIM(pif_module)) END AS pif_module, 
		CASE LEN(COALESCE(ipfcx6_year_of_loss, '1800')) WHEN 0 THEN '1800' ELSE COALESCE(ipfcx6_year_of_loss, '1800') END AS ipfcx6_year_of_loss,
		CASE LEN(COALESCE(ipfcx6_month_of_loss,'1')) WHEN 0 THEN '1' ELSE COALESCE(ipfcx6_month_of_loss,'1') END AS ipfcx6_month_of_loss,
		CASE LEN(COALESCE(ipfcx6_day_of_loss, '1')) WHEN 0 THEN '1' ELSE COALESCE(ipfcx6_day_of_loss,'1') END AS ipfcx6_day_of_loss,
		CASE LEN(COALESCE(ipfcx6_loss_occ_fdigit,'0')) WHEN 0 THEN '0' ELSE COALESCE(ipfcx6_loss_occ_fdigit,'0') END AS ipfcx6_loss_occ_fdigit,
		CASE LEN(COALESCE(ipfcx6_usr_loss_occurence,'0')) WHEN 0 THEN '0' ELSE COALESCE(ipfcx6_usr_loss_occurence,'0') END AS ipfcx6_usr_loss_occurence,
		CASE LEN(COALESCE(ipfcx6_loss_claimant,'0')) WHEN 0 THEN '0' ELSE COALESCE(ipfcx6_loss_claimant,'0') END AS ipfcx6_loss_claimant,
		CASE LEN(LTRIM(RTRIM(COALESCE(ipfcx6_insurance_line,' ')))) WHEN 0 THEN 'N/A' ELSE LTRIM(RTRIM(ipfcx6_insurance_line)) END AS ipfcx6_insurance_line, 
		CASE LEN(COALESCE(ipfcx6_location_number,'0')) WHEN 0 THEN '0' ELSE COALESCE(ipfcx6_location_number,'0') END AS ipfcx6_location_number,
		CASE LEN(COALESCE(ipfcx6_sub_location_number,'0')) WHEN 0 THEN '0' ELSE COALESCE(ipfcx6_sub_location_number,'0') END AS ipfcx6_sub_location_number,
		CASE LEN(LTRIM(RTRIM(COALESCE(ipfcx6_risk_unit_group,' ')))) WHEN 0 THEN '0' ELSE LTRIM(RTRIM(ipfcx6_risk_unit_group)) END AS ipfcx6_risk_unit_group, 
		CASE LEN(COALESCE(ipfcx6_class_code_group,'0')) WHEN 0 THEN '0' ELSE COALESCE(ipfcx6_class_code_group,'0') END AS ipfcx6_class_code_group,
		CASE LEN(COALESCE(ipfcx6_class_code_member,'0')) WHEN 0 THEN '0' ELSE COALESCE(ipfcx6_class_code_member,'0') END AS ipfcx6_class_code_member, 
		CASE LEN(LTRIM(RTRIM(COALESCE(ipfcx6_loss_unit,' ')))) WHEN 0 THEN '0' ELSE LTRIM(RTRIM(ipfcx6_loss_unit)) END AS ipfcx6_loss_unit, 
		CASE LEN(COALESCE(ipfcx6_risk_sequence,'0')) WHEN 0 THEN '0' ELSE COALESCE(ipfcx6_risk_sequence,'0') END AS ipfcx6_risk_sequence, 
		CASE LEN(LTRIM(RTRIM(COALESCE(ipfcx6_major_peril,' ')))) WHEN 0 THEN '0' ELSE LTRIM(RTRIM(ipfcx6_major_peril)) END AS ipfcx6_major_peril, 
		CASE LEN(LTRIM(RTRIM(COALESCE(ipfcx6_sequence_type_exposure, ' ')))) WHEN 0 THEN '0' ELSE LTRIM(RTRIM(ipfcx6_sequence_type_exposure)) END AS ipfcx6_sequence_type_exposure, 
		CASE LEN(LTRIM(RTRIM(COALESCE(ipfcx6_loss_disability,' ')))) WHEN 0 THEN '0' ELSE LTRIM(RTRIM(ipfcx6_loss_disability)) END AS ipfcx6_loss_disability, 
		CASE LEN(LTRIM(RTRIM(COALESCE(ipfcx6_member,' ')))) WHEN 0 THEN '0' ELSE LTRIM(RTRIM(ipfcx6_member)) END AS ipfcx6_member, 
		CASE LEN(LTRIM(RTRIM(COALESCE(ipfcx6_reserve_category,' ')))) WHEN 0 THEN '0' ELSE LTRIM(RTRIM( ipfcx6_reserve_category)) END AS ipfcx6_reserve_category, 
		CASE LEN(LTRIM(RTRIM(COALESCE(ipfcx6_loss_cause,' ')))) WHEN 0 THEN '0' ELSE LTRIM(RTRIM( ipfcx6_loss_cause)) END AS ipfcx6_loss_cause 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_42x6_stage
		order by ipfcx6_year_process, 
		ipfcx6_month_process, 
		ipfcx6_day_process --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module,ipfcx6_insurance_line,ipfcx6_location_number,ipfcx6_sub_location_number,ipfcx6_year_of_loss,ipfcx6_month_of_loss,ipfcx6_day_of_loss,ipfcx6_loss_occ_fdigit,ipfcx6_usr_loss_occurence,ipfcx6_risk_unit_group,ipfcx6_class_code_group,ipfcx6_class_code_member,ipfcx6_loss_unit,ipfcx6_risk_sequence,ipfcx6_major_peril,ipfcx6_sequence_type_exposure,ipfcx6_loss_disability,ipfcx6_member,ipfcx6_reserve_category,ipfcx6_loss_cause ORDER BY ipfcx6_year_process) = 1
),
LKP_sup_convert_pms_claim_transaction_code AS (
	SELECT
	edw_financial_type_code,
	edw_trans_code,
	edw_trans_ctgry_code,
	pms_trans_code
	FROM (
		SELECT sup_convert_pms_claim_transaction_code.edw_financial_type_code as edw_financial_type_code, sup_convert_pms_claim_transaction_code.edw_trans_code as edw_trans_code, sup_convert_pms_claim_transaction_code.edw_trans_ctgry_code as edw_trans_ctgry_code, sup_convert_pms_claim_transaction_code.pms_trans_code as pms_trans_code 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_convert_pms_claim_transaction_code
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pms_trans_code ORDER BY edw_financial_type_code) = 1
),
EXP_CLAIM_TRANSACTION_DETECT_CHANGES AS (
	SELECT
	EXP_CLAIMS_TRANSACTION_DEFAULT.PIF_SYMBOL,
	EXP_CLAIMS_TRANSACTION_DEFAULT.PIF_POLICY_NUMBER,
	EXP_CLAIMS_TRANSACTION_DEFAULT.PIF_MODULE,
	-- *INF*: LTRIM(RTRIM(PIF_SYMBOL  ||  PIF_POLICY_NUMBER || PIF_MODULE))
	LTRIM(RTRIM(PIF_SYMBOL || PIF_POLICY_NUMBER || PIF_MODULE)) AS V_Pol_Key,
	-- *INF*: :LKP.LKP_V2_POLICY(V_Pol_Key)
	LKP_V2_POLICY_V_Pol_Key.pol_ak_id AS V_pol_ak_id,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_INSURANCE_LINE,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_LOCATION_NUMBER,
	-- *INF*: to_char(LOSS_LOCATION_NUMBER)
	to_char(LOSS_LOCATION_NUMBER) AS V_LOSS_LOCATION_NUMBER,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_SUB_LOCATION_NUMBER,
	-- *INF*: to_char(LOSS_SUB_LOCATION_NUMBER)
	to_char(LOSS_SUB_LOCATION_NUMBER) AS V_LOSS_SUB_LOCATION_NUMBER,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_RISK_UNIT_GROUP,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_CLASS_CODE_GROUP,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_CLASS_CODE_MEMBER,
	-- *INF*: IIF(ISNULL(TO_CHAR(LOSS_CLASS_CODE_GROUP)||TO_CHAR(LOSS_CLASS_CODE_MEMBER)),'N/A',
	-- LPAD(TO_CHAR(LOSS_CLASS_CODE_GROUP)||TO_CHAR(LOSS_CLASS_CODE_MEMBER),3,'0'))
	IFF(TO_CHAR(LOSS_CLASS_CODE_GROUP) || TO_CHAR(LOSS_CLASS_CODE_MEMBER) IS NULL, 'N/A', LPAD(TO_CHAR(LOSS_CLASS_CODE_GROUP) || TO_CHAR(LOSS_CLASS_CODE_MEMBER), 3, '0')) AS V_RISK_UNIT_GRP_SEQ_NUM,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_UNIT,
	-- *INF*: LTRIM(RTRIM(LOSS_UNIT))
	LTRIM(RTRIM(LOSS_UNIT)) AS V_LOSS_UNIT,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_SEQUENCE_RISK_UNIT,
	-- *INF*: IIF(TO_DECIMAL(LOSS_SEQUENCE_RISK_UNIT) = 0, '0',to_char(TO_DECIMAL(LOSS_SEQUENCE_RISK_UNIT)) )
	-- 
	-- 
	-- ---IIF(ISNULL(claim_occurrence_ak_id_EXCEED) and LOSS_SEQUENCE_RISK_UNIT = 'N/A','0',LOSS_SEQUENCE_RISK_UNIT)
	-- 
	-- 
	-- 
	IFF(TO_DECIMAL(LOSS_SEQUENCE_RISK_UNIT) = 0, '0', to_char(TO_DECIMAL(LOSS_SEQUENCE_RISK_UNIT))) AS V_LOSS_SEQUENCE_RISK_UNIT,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_TYPE_EXPOSURE,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_MAJOR_PERIL,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_RESERVE_CATEGORY,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_CAUSE,
	-- *INF*: IIF((LOSS_MAJOR_PERIL = '032' AND LOSS_CAUSE = '07'),'06',LOSS_CAUSE)
	IFF(( LOSS_MAJOR_PERIL = '032' AND LOSS_CAUSE = '07' ), '06', LOSS_CAUSE) AS v_LOSS_CAUSE,
	v_LOSS_CAUSE AS o_LOSS_CAUSE,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_MAJOR_PERIL_SEQ,
	-- *INF*: LTRIM(RTRIM(LOSS_MAJOR_PERIL_SEQ))
	LTRIM(RTRIM(LOSS_MAJOR_PERIL_SEQ)) AS V_LOSS_MAJOR_PERIL_SEQ,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_YEAR,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_MONTH,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_DAY,
	-- *INF*: TO_CHAR(LOSS_YEAR)
	TO_CHAR(LOSS_YEAR) AS V_LOSS_YEAR,
	-- *INF*: TO_CHAR(LOSS_MONTH)
	TO_CHAR(LOSS_MONTH) AS V_LOSS_MONTH,
	-- *INF*: TO_CHAR(LOSS_DAY)
	TO_CHAR(LOSS_DAY) AS V_LOSS_DAY,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_OCCURENCE,
	-- *INF*: IIF ( LENGTH(V_LOSS_MONTH) = 1, '0' || V_LOSS_MONTH, V_LOSS_MONTH)
	-- ||  
	-- IIF ( LENGTH(V_LOSS_DAY ) = 1, '0' || V_LOSS_DAY, V_LOSS_DAY )
	-- ||  
	-- V_LOSS_YEAR
	IFF(LENGTH(V_LOSS_MONTH) = 1, '0' || V_LOSS_MONTH, V_LOSS_MONTH) || IFF(LENGTH(V_LOSS_DAY) = 1, '0' || V_LOSS_DAY, V_LOSS_DAY) || V_LOSS_YEAR AS V_LOSS_DATE,
	-- *INF*: PIF_SYMBOL || PIF_POLICY_NUMBER || PIF_MODULE || V_LOSS_DATE || TO_CHAR(LOSS_OCCURENCE)
	PIF_SYMBOL || PIF_POLICY_NUMBER || PIF_MODULE || V_LOSS_DATE || TO_CHAR(LOSS_OCCURENCE) AS V_LOSS_OCCURENCE_KEY,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_CLAIMANT,
	'CMT' AS V_PARTY_ROLE_CODE,
	-- *INF*: V_LOSS_OCCURENCE_KEY||TO_CHAR(LOSS_CLAIMANT)||V_PARTY_ROLE_CODE
	V_LOSS_OCCURENCE_KEY || TO_CHAR(LOSS_CLAIMANT) || V_PARTY_ROLE_CODE AS V_LOSS_PARTY_KEY,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_MEMBER,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_DISABILITY,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_ID,
	EXP_CLAIMS_TRANSACTION_DEFAULT.PIF_4578_stage_id,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_REINS_KEY_ID,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_REINS_CO_NO,
	-- *INF*: LTRIM(RTRIM(LOSS_REINS_CO_NO))
	LTRIM(RTRIM(LOSS_REINS_CO_NO)) AS v_LOSS_REINS_CO_NO,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_BASE_TRANSACTION,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_NET_CHANGE_DOLLARS,
	-- *INF*: IIF(LOSS_NET_CHANGE_DOLLARS=0,'39','38')
	IFF(LOSS_NET_CHANGE_DOLLARS = 0, '39', '38') AS TRANS_TYPE_CODE_1,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_PAID_OR_RESV_AMT,
	-- *INF*: IIF(IN(LOSS_TRANSACTION, '90', '91', '92', '95', '97', '98', '99'), 0, LOSS_PAID_OR_RESV_AMT)
	IFF(IN(LOSS_TRANSACTION, '90', '91', '92', '95', '97', '98', '99'), 0, LOSS_PAID_OR_RESV_AMT) AS OUT_LOSS_PAID_OR_RESV_AMT_FOR_LKP,
	-- *INF*: IIF(LOSS_PAID_OR_RESV_AMT=0,'40','30')
	IFF(LOSS_PAID_OR_RESV_AMT = 0, '40', '30') AS TRANS_TYPE_CODE_2,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_TRANSACTION,
	-- *INF*: IIF(IN(LOSS_TRANSACTION, '91', '92'), '90', LOSS_TRANSACTION) 
	IFF(IN(LOSS_TRANSACTION, '91', '92'), '90', LOSS_TRANSACTION) AS OUT_LOSS_TRANSACTION_FOR_LKP,
	LKP_sup_convert_pms_claim_transaction_code.edw_financial_type_code AS FINANCIAL_TYPE_CODE,
	LKP_sup_convert_pms_claim_transaction_code.edw_trans_code AS IN_TRANS_TYPE_CODE,
	-- *INF*: DECODE(TRUE,
	-- LOSS_TRANSACTION='76',TRANS_TYPE_CODE_1,
	-- LOSS_TRANSACTION='26',TRANS_TYPE_CODE_1,
	-- LOSS_TRANSACTION='27',TRANS_TYPE_CODE_2,
	-- LOSS_TRANSACTION='83',TRANS_TYPE_CODE_2,
	-- LOSS_TRANSACTION='88',TRANS_TYPE_CODE_1,
	-- LOSS_TRANSACTION='84',TRANS_TYPE_CODE_2,
	-- LOSS_TRANSACTION='89',TRANS_TYPE_CODE_1,IN_TRANS_TYPE_CODE)
	DECODE(TRUE,
		LOSS_TRANSACTION = '76', TRANS_TYPE_CODE_1,
		LOSS_TRANSACTION = '26', TRANS_TYPE_CODE_1,
		LOSS_TRANSACTION = '27', TRANS_TYPE_CODE_2,
		LOSS_TRANSACTION = '83', TRANS_TYPE_CODE_2,
		LOSS_TRANSACTION = '88', TRANS_TYPE_CODE_1,
		LOSS_TRANSACTION = '84', TRANS_TYPE_CODE_2,
		LOSS_TRANSACTION = '89', TRANS_TYPE_CODE_1,
		IN_TRANS_TYPE_CODE) AS V_TRANS_TYPE_CODE,
	V_TRANS_TYPE_CODE AS OUT_REINS_TRANS_CODE,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_ENTRY_OPERATOR,
	LKP_sup_convert_pms_claim_transaction_code.edw_trans_ctgry_code AS LKP_trans_ctgry_code,
	-- *INF*: IIF(ISNULL(LKP_trans_ctgry_code), 'N/A',  LKP_trans_ctgry_code)
	IFF(LKP_trans_ctgry_code IS NULL, 'N/A', LKP_trans_ctgry_code) AS trans_ctgry_code,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_TRANSACTION_CATEGORY,
	-- *INF*: IIF(LOSS_TRANSACTION_CATEGORY='WD' OR LOSS_TRANSACTION_CATEGORY='DR',LOSS_TRANSACTION_CATEGORY,IIF(ISNULL(LKP_trans_ctgry_code),'N/A',LKP_trans_ctgry_code))
	IFF(LOSS_TRANSACTION_CATEGORY = 'WD' OR LOSS_TRANSACTION_CATEGORY = 'DR', LOSS_TRANSACTION_CATEGORY, IFF(LKP_trans_ctgry_code IS NULL, 'N/A', LKP_trans_ctgry_code)) AS V_LOSS_TRANSACTION_CATEGORY,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_ADJUSTOR_NO AS LOSS_ADJUSTER_NO,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_DRAFT_NO,
	EXP_CLAIMS_TRANSACTION_DEFAULT.OUT_Loss_transaction_date AS LOSS_TRANSACTION_DATE,
	-- *INF*: TO_DATE(LOSS_TRANSACTION_DATE,'MM/DD/YYYY')
	TO_DATE(LOSS_TRANSACTION_DATE, 'MM/DD/YYYY') AS V_LOSS_TRANSACTION_DATE,
	V_LOSS_TRANSACTION_DATE AS LOSS_TRANSACTION_DATE_OP,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS S3P_UPDATED_DATE_OP,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS S3P_TO_PMS_TRANS_DATE_OP,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_ACCOUNT_ENTERED_DATE,
	-- *INF*: TO_DATE(LOSS_ACCOUNT_ENTERED_DATE,'YYYYMM')
	TO_DATE(LOSS_ACCOUNT_ENTERED_DATE, 'YYYYMM') AS LOSS_ACCOUNT_ENTERED_DATE_OP,
	'N/A' AS TRANS_BASE_TYPE_CODE_OP,
	'N/A' AS SINGLE_CHECK_IND_OP,
	'N/A' AS OFFSET_REISSUE_IND,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_NOTES_DRAFT_PAYEE,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_CLAIM_NUMBER,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_TYPE_DISABILITY,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_OFFSET_ONSET_IND,
	LKP_Claim_Occurrence_Exceed.claim_occurrence_ak_id AS claim_occurrence_ak_id_EXCEED,
	LKP_Claim_Occurrence_Exceed.claim_occurrence_type_code,
	-- *INF*: ---:LKP.LKP_PIF_42GP(LTRIM(RTRIM(PIF_SYMBOL)),LTRIM(RTRIM(PIF_POLICY_NUMBER)),LTRIM(RTRIM(PIF_MODULE)),LOSS_YEAR,LOSS_MONTH,LOSS_DAY,LTRIM(RTRIM(LOSS_OCCURENCE)))
	'' AS V_EXCEED_CLAIM_NUMBER_LKP,
	-- *INF*: ---IIF(isnull(V_EXCEED_CLAIM_NUMBER_LKP)  OR IS_SPACES(V_EXCEED_CLAIM_NUMBER_LKP) or (length (V_EXCEED_CLAIM_NUMBER_LKP)=0), 'N/A', V_EXCEED_CLAIM_NUMBER_LKP)
	'' AS V_EXCEED_CLAIM_NUMBER,
	-- *INF*: IIF(ISNULL(claim_occurrence_ak_id_EXCEED),:LKP.LKP_CLAIM_OCCURRENCE(V_LOSS_OCCURENCE_KEY),claim_occurrence_ak_id_EXCEED)
	-- 
	-- --IIF(V_EXCEED_CLAIM_NUMBER= 'N/A',:LKP.LKP_CLAIM_OCCURRENCE(V_LOSS_OCCURENCE_KEY),:LKP.LKP_CLAIM_OCCURRENCE_EXCEED(V_EXCEED_CLAIM_NUMBER))
	IFF(claim_occurrence_ak_id_EXCEED IS NULL, LKP_CLAIM_OCCURRENCE_V_LOSS_OCCURENCE_KEY.claim_occurrence_ak_id, claim_occurrence_ak_id_EXCEED) AS V_CLAIM_OCCURRENCE_AK_ID_LKP,
	-- *INF*: IIF(ISNULL(V_CLAIM_OCCURRENCE_AK_ID_LKP), 0, V_CLAIM_OCCURRENCE_AK_ID_LKP)
	-- 
	-- 
	-- 
	IFF(V_CLAIM_OCCURRENCE_AK_ID_LKP IS NULL, 0, V_CLAIM_OCCURRENCE_AK_ID_LKP) AS V_CLAIM_OCCURENCE_AK_ID,
	-- *INF*: IIF(ISNULL(claim_occurrence_ak_id_EXCEED),:LKP.LKP_CLAIM_PARTY(V_LOSS_PARTY_KEY),0)
	-- 
	-- --IIF(V_EXCEED_CLAIM_NUMBER = 'N/A',:LKP.LKP_CLAIM_PARTY(V_LOSS_PARTY_KEY),0)
	IFF(claim_occurrence_ak_id_EXCEED IS NULL, LKP_CLAIM_PARTY_V_LOSS_PARTY_KEY.claim_party_ak_id, 0) AS V_CLAIM_PARTY_AK_ID_LKP,
	-- *INF*: IIF(ISNULL(claim_occurrence_ak_id_EXCEED),IIF(ISNULL(V_CLAIM_PARTY_AK_ID_LKP), 0, V_CLAIM_PARTY_AK_ID_LKP) ,0)
	-- ---IIF(V_EXCEED_CLAIM_NUMBER = 'N/A',IIF(ISNULL(V_CLAIM_PARTY_AK_ID_LKP), 0, V_CLAIM_PARTY_AK_ID_LKP) ,0)
	-- 
	-- 
	-- 
	-- 
	IFF(claim_occurrence_ak_id_EXCEED IS NULL, IFF(V_CLAIM_PARTY_AK_ID_LKP IS NULL, 0, V_CLAIM_PARTY_AK_ID_LKP), 0) AS V_CLAIM_PARTY_AK_ID,
	-- *INF*: IIF(ISNULL(claim_occurrence_ak_id_EXCEED),:LKP.LKP_CLAIM_PARTY_OCCURRENCE(V_CLAIM_OCCURENCE_AK_ID,V_CLAIM_PARTY_AK_ID,V_PARTY_ROLE_CODE),0)
	-- 
	-- 
	IFF(claim_occurrence_ak_id_EXCEED IS NULL, LKP_CLAIM_PARTY_OCCURRENCE_V_CLAIM_OCCURENCE_AK_ID_V_CLAIM_PARTY_AK_ID_V_PARTY_ROLE_CODE.claim_party_occurrence_ak_id, 0) AS V_CLAIM_PARTY_OCCURRENCE_AK_ID_PMS,
	-- *INF*: IIF(NOT ISNULL(claim_occurrence_ak_id_EXCEED),:LKP.LKP_CLAIM_PARTY_OCCURRENCE_EXCEED(claim_occurrence_ak_id_EXCEED,LTRIM(RTRIM(LOSS_CLAIMANT))), 0)
	-- 
	IFF(NOT claim_occurrence_ak_id_EXCEED IS NULL, LKP_CLAIM_PARTY_OCCURRENCE_EXCEED_claim_occurrence_ak_id_EXCEED_LTRIM_RTRIM_LOSS_CLAIMANT.claim_party_occurrence_ak_id, 0) AS V_CLAIM_PARTY_OCCURRENCE_AK_ID_EXCEED,
	-- *INF*: IIF(ISNULL(claim_occurrence_ak_id_EXCEED),V_CLAIM_PARTY_OCCURRENCE_AK_ID_PMS,V_CLAIM_PARTY_OCCURRENCE_AK_ID_EXCEED)
	-- 
	-- 
	-- 
	IFF(claim_occurrence_ak_id_EXCEED IS NULL, V_CLAIM_PARTY_OCCURRENCE_AK_ID_PMS, V_CLAIM_PARTY_OCCURRENCE_AK_ID_EXCEED) AS V_CLAIM_PARTY_OCCURENCE_AK_ID,
	-- *INF*: IIF(ISNULL(claim_occurrence_ak_id_EXCEED),
	-- :LKP.LKP_CLAIMANT_DETAIL_COVERAGE_PMS(V_CLAIM_PARTY_OCCURENCE_AK_ID, V_LOSS_LOCATION_NUMBER, V_LOSS_SUB_LOCATION_NUMBER,  LOSS_INSURANCE_LINE, LOSS_RISK_UNIT_GROUP, V_RISK_UNIT_GRP_SEQ_NUM ,V_LOSS_UNIT, V_LOSS_SEQUENCE_RISK_UNIT, LOSS_MAJOR_PERIL, V_LOSS_MAJOR_PERIL_SEQ, LOSS_DISABILITY, LOSS_RESERVE_CATEGORY, v_LOSS_CAUSE, LOSS_MEMBER),
	-- :LKP.LKP_CLAIMANT_COVERAGE_DETAIL_EXCEED(V_CLAIM_PARTY_OCCURENCE_AK_ID,LOSS_INSURANCE_LINE,V_LOSS_LOCATION_NUMBER,V_LOSS_SUB_LOCATION_NUMBER, LOSS_RISK_UNIT_GROUP, V_RISK_UNIT_GRP_SEQ_NUM,V_LOSS_UNIT,SUBSTR(V_LOSS_SEQUENCE_RISK_UNIT,1,1) ,
	-- LOSS_MAJOR_PERIL, LOSS_RESERVE_CATEGORY,LOSS_CAUSE,LOSS_MAJOR_PERIL_SEQ))
	-- 
	-- 
	-- 
	-- 
	-- 
	IFF(claim_occurrence_ak_id_EXCEED IS NULL, LKP_CLAIMANT_DETAIL_COVERAGE_PMS_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_V_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_v_LOSS_CAUSE_LOSS_MEMBER.claimant_cov_det_ak_id, LKP_CLAIMANT_COVERAGE_DETAIL_EXCEED_V_CLAIM_PARTY_OCCURENCE_AK_ID_LOSS_INSURANCE_LINE_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_SUBSTR_V_LOSS_SEQUENCE_RISK_UNIT_1_1_LOSS_MAJOR_PERIL_LOSS_RESERVE_CATEGORY_LOSS_CAUSE_LOSS_MAJOR_PERIL_SEQ.claimant_cov_det_ak_id) AS V_CLAIMANT_COV_DET_AK_ID_LKP,
	V_CLAIMANT_COV_DET_AK_ID_LKP AS V_CLAIMANT_COV_DET_AK_ID,
	-- *INF*: IIF(ISNULL(V_CLAIMANT_COV_DET_AK_ID),-1,V_CLAIMANT_COV_DET_AK_ID)
	IFF(V_CLAIMANT_COV_DET_AK_ID IS NULL, - 1, V_CLAIMANT_COV_DET_AK_ID) AS OUT_CLAIMANT_COV_DET_AK_ID,
	LKP_pif_42x6_stage.ipfcx6_year_process,
	-- *INF*: to_char(ipfcx6_year_process)
	to_char(ipfcx6_year_process) AS V_ipfcx6_year_process,
	LKP_pif_42x6_stage.ipfcx6_month_process,
	-- *INF*: IIF(LENGTH(to_char(ipfcx6_month_process)) = 1, '0'||to_char(ipfcx6_month_process), to_char(ipfcx6_month_process))
	IFF(LENGTH(to_char(ipfcx6_month_process)) = 1, '0' || to_char(ipfcx6_month_process), to_char(ipfcx6_month_process)) AS v_ipfcx6_month_process,
	LKP_pif_42x6_stage.ipfcx6_day_process,
	-- *INF*: IIF(LENGTH(to_char(ipfcx6_day_process)) = 1, '0'||to_char(ipfcx6_day_process), to_char(ipfcx6_day_process))
	IFF(LENGTH(to_char(ipfcx6_day_process)) = 1, '0' || to_char(ipfcx6_day_process), to_char(ipfcx6_day_process)) AS v_ipfcx6_day_process,
	v_ipfcx6_month_process||v_ipfcx6_day_process||V_ipfcx6_year_process AS v_reprocess_date,
	-- *INF*: IIF(v_reprocess_date='00000',TO_DATE('01011800', 'MMDDYYYY'),
	--  to_date(LTRIM(RTRIM(v_reprocess_date)), 'MMDDYYYY'))
	IFF(v_reprocess_date = '00000', TO_DATE('01011800', 'MMDDYYYY'), to_date(LTRIM(RTRIM(v_reprocess_date)), 'MMDDYYYY')) AS v_reprocess_date_out,
	-- *INF*: IIF(LOSS_OFFSET_ONSET_IND = 'N/A' OR ISNULL(v_reprocess_date_out),  TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'), v_reprocess_date_out)
	IFF(LOSS_OFFSET_ONSET_IND = 'N/A' OR v_reprocess_date_out IS NULL, TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), v_reprocess_date_out) AS reprocess_date_out,
	EXP_CLAIMS_TRANSACTION_DEFAULT.Claim_Reins_Broker_name,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_TIME,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOGICAL_FLAG,
	-- *INF*: iif(isnull(:LKP.LKP_CLAIM_TRANSACTION(V_CLAIMANT_COV_DET_AK_ID)),1,0)
	IFF(LKP_CLAIM_TRANSACTION_V_CLAIMANT_COV_DET_AK_ID.claim_trans_id IS NULL, 1, 0) AS MISSING_FLAG,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS eff_from_dt,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_dt,
	sysdate AS created_date,
	sysdate AS modified_date,
	-- *INF*: :LKP.LKP_REINSURANCE_COVERAGE(V_pol_ak_id,v_LOSS_REINS_CO_NO)
	LKP_REINSURANCE_COVERAGE_V_pol_ak_id_v_LOSS_REINS_CO_NO.reins_cov_ak_id AS v_reins_cov_ak_id,
	v_reins_cov_ak_id AS reins_cov_ak_id
	FROM EXP_CLAIMS_TRANSACTION_DEFAULT
	LEFT JOIN LKP_Claim_Occurrence_Exceed
	ON LKP_Claim_Occurrence_Exceed.s3p_claim_num = LKP_PIF_42GP.ipfcgp_claim_number
	LEFT JOIN LKP_pif_42x6_stage
	ON LKP_pif_42x6_stage.pif_symbol = EXP_CLAIMS_TRANSACTION_DEFAULT.PIF_SYMBOL AND LKP_pif_42x6_stage.pif_policy_number = EXP_CLAIMS_TRANSACTION_DEFAULT.PIF_POLICY_NUMBER AND LKP_pif_42x6_stage.pif_module = EXP_CLAIMS_TRANSACTION_DEFAULT.PIF_MODULE AND LKP_pif_42x6_stage.ipfcx6_insurance_line = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_INSURANCE_LINE_lkp AND LKP_pif_42x6_stage.ipfcx6_location_number = EXP_CLAIMS_TRANSACTION_DEFAULT.v_LOSS_LOCATION_NUMBER AND LKP_pif_42x6_stage.ipfcx6_sub_location_number = EXP_CLAIMS_TRANSACTION_DEFAULT.v_LOSS_SUB_LOCATION_NUMBER AND LKP_pif_42x6_stage.ipfcx6_year_of_loss = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_YEAR AND LKP_pif_42x6_stage.ipfcx6_month_of_loss = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_MONTH AND LKP_pif_42x6_stage.ipfcx6_day_of_loss = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_DAY AND LKP_pif_42x6_stage.ipfcx6_loss_occ_fdigit = EXP_CLAIMS_TRANSACTION_DEFAULT.ipfcx6_loss_occurence_fdigit AND LKP_pif_42x6_stage.ipfcx6_usr_loss_occurence = EXP_CLAIMS_TRANSACTION_DEFAULT.ipfcx6_usr_loss_occurence AND LKP_pif_42x6_stage.ipfcx6_risk_unit_group = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_RISK_UNIT_GROUP_lkp AND LKP_pif_42x6_stage.ipfcx6_class_code_group = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_CLASS_CODE_GROUP_lkp AND LKP_pif_42x6_stage.ipfcx6_class_code_member = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_CLASS_CODE_MEMBER_lkp AND LKP_pif_42x6_stage.ipfcx6_loss_unit = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_UNIT_lkp AND LKP_pif_42x6_stage.ipfcx6_risk_sequence = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_SEQUENCE_RISK_UNIT_lkp AND LKP_pif_42x6_stage.ipfcx6_major_peril = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_MAJOR_PERIL_lkp AND LKP_pif_42x6_stage.ipfcx6_sequence_type_exposure = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_MAJOR_PERIL_SEQ_lkp AND LKP_pif_42x6_stage.ipfcx6_loss_disability = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_DISABILITY_lkp AND LKP_pif_42x6_stage.ipfcx6_member = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_MEMBER_lkp AND LKP_pif_42x6_stage.ipfcx6_reserve_category = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_RESERVE_CATEGORY_lkp AND LKP_pif_42x6_stage.ipfcx6_loss_cause = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_CAUSE_lkp
	LEFT JOIN LKP_sup_convert_pms_claim_transaction_code
	ON LKP_sup_convert_pms_claim_transaction_code.pms_trans_code = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_TRANSACTION
	LEFT JOIN LKP_V2_POLICY LKP_V2_POLICY_V_Pol_Key
	ON LKP_V2_POLICY_V_Pol_Key.pol_key = V_Pol_Key

	LEFT JOIN LKP_CLAIM_OCCURRENCE LKP_CLAIM_OCCURRENCE_V_LOSS_OCCURENCE_KEY
	ON LKP_CLAIM_OCCURRENCE_V_LOSS_OCCURENCE_KEY.claim_occurrence_key = V_LOSS_OCCURENCE_KEY

	LEFT JOIN LKP_CLAIM_PARTY LKP_CLAIM_PARTY_V_LOSS_PARTY_KEY
	ON LKP_CLAIM_PARTY_V_LOSS_PARTY_KEY.claim_party_key = V_LOSS_PARTY_KEY

	LEFT JOIN LKP_CLAIM_PARTY_OCCURRENCE LKP_CLAIM_PARTY_OCCURRENCE_V_CLAIM_OCCURENCE_AK_ID_V_CLAIM_PARTY_AK_ID_V_PARTY_ROLE_CODE
	ON LKP_CLAIM_PARTY_OCCURRENCE_V_CLAIM_OCCURENCE_AK_ID_V_CLAIM_PARTY_AK_ID_V_PARTY_ROLE_CODE.claim_occurrence_ak_id = V_CLAIM_OCCURENCE_AK_ID
	AND LKP_CLAIM_PARTY_OCCURRENCE_V_CLAIM_OCCURENCE_AK_ID_V_CLAIM_PARTY_AK_ID_V_PARTY_ROLE_CODE.claim_party_ak_id = V_CLAIM_PARTY_AK_ID
	AND LKP_CLAIM_PARTY_OCCURRENCE_V_CLAIM_OCCURENCE_AK_ID_V_CLAIM_PARTY_AK_ID_V_PARTY_ROLE_CODE.claim_party_role_code = V_PARTY_ROLE_CODE

	LEFT JOIN LKP_CLAIM_PARTY_OCCURRENCE_EXCEED LKP_CLAIM_PARTY_OCCURRENCE_EXCEED_claim_occurrence_ak_id_EXCEED_LTRIM_RTRIM_LOSS_CLAIMANT
	ON LKP_CLAIM_PARTY_OCCURRENCE_EXCEED_claim_occurrence_ak_id_EXCEED_LTRIM_RTRIM_LOSS_CLAIMANT.claim_occurrence_ak_id = claim_occurrence_ak_id_EXCEED
	AND LKP_CLAIM_PARTY_OCCURRENCE_EXCEED_claim_occurrence_ak_id_EXCEED_LTRIM_RTRIM_LOSS_CLAIMANT.claimant_num = LTRIM(RTRIM(LOSS_CLAIMANT))

	LEFT JOIN LKP_CLAIMANT_DETAIL_COVERAGE_PMS LKP_CLAIMANT_DETAIL_COVERAGE_PMS_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_V_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_v_LOSS_CAUSE_LOSS_MEMBER
	ON LKP_CLAIMANT_DETAIL_COVERAGE_PMS_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_V_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_v_LOSS_CAUSE_LOSS_MEMBER.claim_party_occurrence_ak_id = V_CLAIM_PARTY_OCCURENCE_AK_ID
	AND LKP_CLAIMANT_DETAIL_COVERAGE_PMS_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_V_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_v_LOSS_CAUSE_LOSS_MEMBER.loc_unit_num = V_LOSS_LOCATION_NUMBER
	AND LKP_CLAIMANT_DETAIL_COVERAGE_PMS_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_V_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_v_LOSS_CAUSE_LOSS_MEMBER.sub_loc_unit_num = V_LOSS_SUB_LOCATION_NUMBER
	AND LKP_CLAIMANT_DETAIL_COVERAGE_PMS_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_V_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_v_LOSS_CAUSE_LOSS_MEMBER.ins_line = LOSS_INSURANCE_LINE
	AND LKP_CLAIMANT_DETAIL_COVERAGE_PMS_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_V_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_v_LOSS_CAUSE_LOSS_MEMBER.risk_unit_grp = LOSS_RISK_UNIT_GROUP
	AND LKP_CLAIMANT_DETAIL_COVERAGE_PMS_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_V_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_v_LOSS_CAUSE_LOSS_MEMBER.risk_unit_grp_seq_num = V_RISK_UNIT_GRP_SEQ_NUM
	AND LKP_CLAIMANT_DETAIL_COVERAGE_PMS_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_V_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_v_LOSS_CAUSE_LOSS_MEMBER.risk_unit = V_LOSS_UNIT
	AND LKP_CLAIMANT_DETAIL_COVERAGE_PMS_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_V_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_v_LOSS_CAUSE_LOSS_MEMBER.risk_unit_seq_num = V_LOSS_SEQUENCE_RISK_UNIT
	AND LKP_CLAIMANT_DETAIL_COVERAGE_PMS_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_V_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_v_LOSS_CAUSE_LOSS_MEMBER.major_peril_code = LOSS_MAJOR_PERIL
	AND LKP_CLAIMANT_DETAIL_COVERAGE_PMS_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_V_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_v_LOSS_CAUSE_LOSS_MEMBER.major_peril_seq = V_LOSS_MAJOR_PERIL_SEQ
	AND LKP_CLAIMANT_DETAIL_COVERAGE_PMS_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_V_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_v_LOSS_CAUSE_LOSS_MEMBER.pms_loss_disability = LOSS_DISABILITY
	AND LKP_CLAIMANT_DETAIL_COVERAGE_PMS_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_V_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_v_LOSS_CAUSE_LOSS_MEMBER.reserve_ctgry = LOSS_RESERVE_CATEGORY
	AND LKP_CLAIMANT_DETAIL_COVERAGE_PMS_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_V_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_v_LOSS_CAUSE_LOSS_MEMBER.cause_of_loss = v_LOSS_CAUSE
	AND LKP_CLAIMANT_DETAIL_COVERAGE_PMS_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_V_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_v_LOSS_CAUSE_LOSS_MEMBER.pms_mbr = LOSS_MEMBER

	LEFT JOIN LKP_CLAIMANT_COVERAGE_DETAIL_EXCEED LKP_CLAIMANT_COVERAGE_DETAIL_EXCEED_V_CLAIM_PARTY_OCCURENCE_AK_ID_LOSS_INSURANCE_LINE_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_SUBSTR_V_LOSS_SEQUENCE_RISK_UNIT_1_1_LOSS_MAJOR_PERIL_LOSS_RESERVE_CATEGORY_LOSS_CAUSE_LOSS_MAJOR_PERIL_SEQ
	ON LKP_CLAIMANT_COVERAGE_DETAIL_EXCEED_V_CLAIM_PARTY_OCCURENCE_AK_ID_LOSS_INSURANCE_LINE_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_SUBSTR_V_LOSS_SEQUENCE_RISK_UNIT_1_1_LOSS_MAJOR_PERIL_LOSS_RESERVE_CATEGORY_LOSS_CAUSE_LOSS_MAJOR_PERIL_SEQ.claim_party_occurrence_ak_id = V_CLAIM_PARTY_OCCURENCE_AK_ID
	AND LKP_CLAIMANT_COVERAGE_DETAIL_EXCEED_V_CLAIM_PARTY_OCCURENCE_AK_ID_LOSS_INSURANCE_LINE_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_SUBSTR_V_LOSS_SEQUENCE_RISK_UNIT_1_1_LOSS_MAJOR_PERIL_LOSS_RESERVE_CATEGORY_LOSS_CAUSE_LOSS_MAJOR_PERIL_SEQ.ins_line = LOSS_INSURANCE_LINE
	AND LKP_CLAIMANT_COVERAGE_DETAIL_EXCEED_V_CLAIM_PARTY_OCCURENCE_AK_ID_LOSS_INSURANCE_LINE_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_SUBSTR_V_LOSS_SEQUENCE_RISK_UNIT_1_1_LOSS_MAJOR_PERIL_LOSS_RESERVE_CATEGORY_LOSS_CAUSE_LOSS_MAJOR_PERIL_SEQ.loc_unit_num = V_LOSS_LOCATION_NUMBER
	AND LKP_CLAIMANT_COVERAGE_DETAIL_EXCEED_V_CLAIM_PARTY_OCCURENCE_AK_ID_LOSS_INSURANCE_LINE_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_SUBSTR_V_LOSS_SEQUENCE_RISK_UNIT_1_1_LOSS_MAJOR_PERIL_LOSS_RESERVE_CATEGORY_LOSS_CAUSE_LOSS_MAJOR_PERIL_SEQ.sub_loc_unit_num = V_LOSS_SUB_LOCATION_NUMBER
	AND LKP_CLAIMANT_COVERAGE_DETAIL_EXCEED_V_CLAIM_PARTY_OCCURENCE_AK_ID_LOSS_INSURANCE_LINE_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_SUBSTR_V_LOSS_SEQUENCE_RISK_UNIT_1_1_LOSS_MAJOR_PERIL_LOSS_RESERVE_CATEGORY_LOSS_CAUSE_LOSS_MAJOR_PERIL_SEQ.risk_unit_grp = LOSS_RISK_UNIT_GROUP
	AND LKP_CLAIMANT_COVERAGE_DETAIL_EXCEED_V_CLAIM_PARTY_OCCURENCE_AK_ID_LOSS_INSURANCE_LINE_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_SUBSTR_V_LOSS_SEQUENCE_RISK_UNIT_1_1_LOSS_MAJOR_PERIL_LOSS_RESERVE_CATEGORY_LOSS_CAUSE_LOSS_MAJOR_PERIL_SEQ.risk_unit_grp_seq_num = V_RISK_UNIT_GRP_SEQ_NUM
	AND LKP_CLAIMANT_COVERAGE_DETAIL_EXCEED_V_CLAIM_PARTY_OCCURENCE_AK_ID_LOSS_INSURANCE_LINE_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_SUBSTR_V_LOSS_SEQUENCE_RISK_UNIT_1_1_LOSS_MAJOR_PERIL_LOSS_RESERVE_CATEGORY_LOSS_CAUSE_LOSS_MAJOR_PERIL_SEQ.risk_unit = V_LOSS_UNIT
	AND LKP_CLAIMANT_COVERAGE_DETAIL_EXCEED_V_CLAIM_PARTY_OCCURENCE_AK_ID_LOSS_INSURANCE_LINE_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_SUBSTR_V_LOSS_SEQUENCE_RISK_UNIT_1_1_LOSS_MAJOR_PERIL_LOSS_RESERVE_CATEGORY_LOSS_CAUSE_LOSS_MAJOR_PERIL_SEQ.risk_unit_seq_num = SUBSTR(V_LOSS_SEQUENCE_RISK_UNIT, 1, 1)
	AND LKP_CLAIMANT_COVERAGE_DETAIL_EXCEED_V_CLAIM_PARTY_OCCURENCE_AK_ID_LOSS_INSURANCE_LINE_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_SUBSTR_V_LOSS_SEQUENCE_RISK_UNIT_1_1_LOSS_MAJOR_PERIL_LOSS_RESERVE_CATEGORY_LOSS_CAUSE_LOSS_MAJOR_PERIL_SEQ.major_peril_code = LOSS_MAJOR_PERIL
	AND LKP_CLAIMANT_COVERAGE_DETAIL_EXCEED_V_CLAIM_PARTY_OCCURENCE_AK_ID_LOSS_INSURANCE_LINE_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_SUBSTR_V_LOSS_SEQUENCE_RISK_UNIT_1_1_LOSS_MAJOR_PERIL_LOSS_RESERVE_CATEGORY_LOSS_CAUSE_LOSS_MAJOR_PERIL_SEQ.reserve_ctgry = LOSS_RESERVE_CATEGORY
	AND LKP_CLAIMANT_COVERAGE_DETAIL_EXCEED_V_CLAIM_PARTY_OCCURENCE_AK_ID_LOSS_INSURANCE_LINE_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_SUBSTR_V_LOSS_SEQUENCE_RISK_UNIT_1_1_LOSS_MAJOR_PERIL_LOSS_RESERVE_CATEGORY_LOSS_CAUSE_LOSS_MAJOR_PERIL_SEQ.cause_of_loss = LOSS_CAUSE
	AND LKP_CLAIMANT_COVERAGE_DETAIL_EXCEED_V_CLAIM_PARTY_OCCURENCE_AK_ID_LOSS_INSURANCE_LINE_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_RISK_UNIT_GROUP_V_RISK_UNIT_GRP_SEQ_NUM_V_LOSS_UNIT_SUBSTR_V_LOSS_SEQUENCE_RISK_UNIT_1_1_LOSS_MAJOR_PERIL_LOSS_RESERVE_CATEGORY_LOSS_CAUSE_LOSS_MAJOR_PERIL_SEQ.major_peril_seq = LOSS_MAJOR_PERIL_SEQ

	LEFT JOIN LKP_CLAIM_TRANSACTION LKP_CLAIM_TRANSACTION_V_CLAIMANT_COV_DET_AK_ID
	ON LKP_CLAIM_TRANSACTION_V_CLAIMANT_COV_DET_AK_ID.claimant_cov_det_ak_id = V_CLAIMANT_COV_DET_AK_ID

	LEFT JOIN LKP_REINSURANCE_COVERAGE LKP_REINSURANCE_COVERAGE_V_pol_ak_id_v_LOSS_REINS_CO_NO
	ON LKP_REINSURANCE_COVERAGE_V_pol_ak_id_v_LOSS_REINS_CO_NO.pol_ak_id = V_pol_ak_id
	AND LKP_REINSURANCE_COVERAGE_V_pol_ak_id_v_LOSS_REINS_CO_NO.reins_co_num = v_LOSS_REINS_CO_NO

),
LKP_Claim_Reinsurance_Transaction AS (
	SELECT
	claim_reins_trans_id,
	claim_reins_acct_entered_date,
	claim_reins_trans_hist_amt,
	claimant_cov_det_ak_id,
	reins_cov_ak_id,
	claim_reins_pms_trans_code,
	claim_reins_trans_base_type_code,
	claim_reins_trans_amt,
	claim_reins_trans_date,
	pms_claim_loss_time
	FROM (
		SELECT CRT.claim_reins_trans_id as claim_reins_trans_id, 
		CRT.claim_reins_acct_entered_date as claim_reins_acct_entered_date, 
		CRT.claim_reins_trans_hist_amt as claim_reins_trans_hist_amt, 
		CRT.claimant_cov_det_ak_id as claimant_cov_det_ak_id, 
		CRT.reins_cov_ak_id as reins_cov_ak_id, 
		CASE CRT.claim_reins_pms_trans_code 
		 WHEN '91' THEN '90' 
		WHEN '92' THEN '90' 
		ELSE CRT.claim_reins_pms_trans_code END as claim_reins_pms_trans_code, 
		CRT.claim_reins_trans_base_type_code as claim_reins_trans_base_type_code, 
		 CASE  CRT.claim_reins_trans_code  
		WHEN 90 THEN 0.00 
		WHEN 91 THEN 0.00
		WHEN 92 THEN 0.00 
		WHEN 95 THEN 0.00
		WHEN 97 THEN 0.00
		WHEN 98 THEN 0.00
		WHEN 99 THEN 0.00
		ELSE CRT.claim_reins_trans_amt END as claim_reins_trans_amt, 
		CRT.claim_reins_trans_date as claim_reins_trans_date, 
		CRT.pms_claim_loss_time as pms_claim_loss_time 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_reinsurance_transaction CRT
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,reins_cov_ak_id,claim_reins_pms_trans_code,claim_reins_trans_base_type_code,claim_reins_trans_amt,claim_reins_trans_date,pms_claim_loss_time ORDER BY claim_reins_trans_id) = 1
),
RTR_claim_reinsurance_transaction AS (
	SELECT
	LKP_Claim_Reinsurance_Transaction.claim_reins_trans_id AS CLAIM_REINS_TRANS_ID,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.OUT_CLAIMANT_COV_DET_AK_ID AS CLAIMANT_COV_DET_AK_ID,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.LOSS_ID,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.LOSS_REINS_KEY_ID AS CLAIM_REINS_CODE,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.LOSS_TRANSACTION AS CLAIM_REINS_PMS_TRANS_CODE,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.LOSS_BASE_TRANSACTION AS CLAIM_REINS_TRANS_BASE_TYPE_CODE,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.FINANCIAL_TYPE_CODE,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.OUT_REINS_TRANS_CODE AS CLAIM_REINS_TRANS_CODE,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.LOSS_PAID_OR_RESV_AMT AS CLAIM_REINS_TRANS_AMT,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.LOSS_NET_CHANGE_DOLLARS AS CLAIM_REINS_TRANS_HIST_AMT,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.LOSS_TRANSACTION_DATE_OP AS CLAIM_REINS_TRANS_DATE,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.LOSS_ADJUSTER_NO AS CLAIM_REINS_TRANS_PAY_NUM,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.LOSS_DRAFT_NO AS CLAIM_REINS_DRAFT_NUM,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.LOSS_REINS_CO_NO AS CLAIM_REINS_BROKER_NUM,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.Claim_Reins_Broker_name AS CLAIM_REINS_BROKER_NAME,
	LKP_Claim_Reinsurance_Transaction.claim_reins_trans_hist_amt AS LKP_REINS_TRANS_HIST_AMT,
	LKP_Claim_Reinsurance_Transaction.claim_reins_acct_entered_date AS LKP_REINS_ACCT_ENTERED_DATE,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.LOSS_NOTES_DRAFT_PAYEE AS CLAIM_REINS_PAYEE_NOTE,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.LOSS_CLAIM_NUMBER AS CLAIM_REINS_CESSION_NUM,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.LOSS_ACCOUNT_ENTERED_DATE_OP AS REINS_ACCT_ENTERED_DATE,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.LOSS_OFFSET_ONSET_IND,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.reprocess_date_out AS REPROCESS_DATE,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.LOSS_TIME,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.LOGICAL_FLAG,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.crrnt_snpsht_flag AS CRRNT_SNPSHT_FLAG,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.audit_id AS AUDIT_ID,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.eff_from_dt AS EFF_FROM_DATE,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.eff_to_dt AS EFF_TO_DATE,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.SOURCE_SYSTEM_ID AS SOURCE_SYS_ID,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.created_date AS CREATED_DATE,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.modified_date AS MODIFIED_DATE,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.PIF_4578_stage_id,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.MISSING_FLAG,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.trans_ctgry_code AS TRANS_CTGRY_CODE,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.o_LOSS_CAUSE AS LOSS_CAUSE,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.LOSS_RESERVE_CATEGORY,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.LOSS_TYPE_DISABILITY,
	EXP_CLAIM_TRANSACTION_DETECT_CHANGES.reins_cov_ak_id
	FROM EXP_CLAIM_TRANSACTION_DETECT_CHANGES
	LEFT JOIN LKP_Claim_Reinsurance_Transaction
	ON LKP_Claim_Reinsurance_Transaction.claimant_cov_det_ak_id = EXP_CLAIM_TRANSACTION_DETECT_CHANGES.OUT_CLAIMANT_COV_DET_AK_ID AND LKP_Claim_Reinsurance_Transaction.reins_cov_ak_id = EXP_CLAIM_TRANSACTION_DETECT_CHANGES.reins_cov_ak_id AND LKP_Claim_Reinsurance_Transaction.claim_reins_pms_trans_code = EXP_CLAIM_TRANSACTION_DETECT_CHANGES.OUT_LOSS_TRANSACTION_FOR_LKP AND LKP_Claim_Reinsurance_Transaction.claim_reins_trans_base_type_code = EXP_CLAIM_TRANSACTION_DETECT_CHANGES.LOSS_BASE_TRANSACTION AND LKP_Claim_Reinsurance_Transaction.claim_reins_trans_amt = EXP_CLAIM_TRANSACTION_DETECT_CHANGES.OUT_LOSS_PAID_OR_RESV_AMT_FOR_LKP AND LKP_Claim_Reinsurance_Transaction.claim_reins_trans_date = EXP_CLAIM_TRANSACTION_DETECT_CHANGES.LOSS_TRANSACTION_DATE_OP AND LKP_Claim_Reinsurance_Transaction.pms_claim_loss_time = EXP_CLAIM_TRANSACTION_DETECT_CHANGES.LOSS_TIME
),
RTR_claim_reinsurance_transaction_INSERT AS (SELECT * FROM RTR_claim_reinsurance_transaction WHERE ISNULL(CLAIM_REINS_TRANS_ID)),
RTR_claim_reinsurance_transaction_UPDATE AS (SELECT * FROM RTR_claim_reinsurance_transaction WHERE DECODE(TRUE, 
NOT ISNULL(CLAIM_REINS_TRANS_ID) AND 
(LKP_REINS_ACCT_ENTERED_DATE <> REINS_ACCT_ENTERED_DATE
OR LKP_REINS_TRANS_HIST_AMT <> CLAIM_REINS_TRANS_HIST_AMT),TRUE ,
NOT ISNULL(CLAIM_REINS_TRANS_ID) AND IN(CLAIM_REINS_PMS_TRANS_CODE, '90', '91', '92', '95', '97', '98', '99'), TRUE,
FALSE
)),
UPD_claim_reinsurance_transaction_UPDATE AS (
	SELECT
	CLAIM_REINS_TRANS_ID AS claim_reins_trans_id2, 
	CLAIMANT_COV_DET_AK_ID, 
	CLAIM_REINS_CODE AS CLAIM_REINS_CODE2, 
	CLAIM_REINS_TRANS_BASE_TYPE_CODE AS CLAIM_REINS_TRANS_BASE_TYPE_CODE2, 
	CLAIM_REINS_TRANS_CODE AS CLAIM_REINS_TRANS_CODE2, 
	CLAIM_REINS_TRANS_AMT AS CLAIM_REINS_TRANS_AMT2, 
	CLAIM_REINS_TRANS_HIST_AMT AS CLAIM_REINS_TRANS_HIST_AMT2, 
	CLAIM_REINS_TRANS_DATE AS CLAIM_REINS_TRANS_DATE2, 
	CLAIM_REINS_TRANS_PAY_NUM AS CLAIM_REINS_TRANS_PAY_NUM2, 
	CLAIM_REINS_DRAFT_NUM AS CLAIM_REINS_DRAFT_NUM2, 
	CLAIM_REINS_BROKER_NUM AS CLAIM_REINS_BROKER_NUM2, 
	CLAIM_REINS_BROKER_NAME AS CLAIM_REINS_BROKER_NAME2, 
	CLAIM_REINS_PAYEE_NOTE AS claim_reins_payee_note, 
	CLAIM_REINS_CESSION_NUM AS CLAIM_REINS_CESSION_NUM2, 
	REINS_ACCT_ENTERED_DATE AS REINS_ACCT_ENTERED_DATE2, 
	LOSS_OFFSET_ONSET_IND AS LOSS_OFFSET_ONSET_IND2, 
	REPROCESS_DATE AS reprocess_date_out2, 
	CRRNT_SNPSHT_FLAG AS crrnt_snpsht_flag2, 
	AUDIT_ID AS audit_id2, 
	EFF_FROM_DATE AS eff_from_date2, 
	EFF_TO_DATE AS eff_to_date2, 
	SOURCE_SYS_ID AS source_sys_id2, 
	CREATED_DATE AS created_date2, 
	MODIFIED_DATE AS modified_date2, 
	FINANCIAL_TYPE_CODE AS FINANCIAL_TYPE_CODE1, 
	CLAIM_REINS_PMS_TRANS_CODE AS LOSS_TRANSACTION2, 
	PIF_4578_stage_id, 
	MISSING_FLAG, 
	TRANS_CTGRY_CODE AS TRANS_CTGRY_CODE3
	FROM RTR_claim_reinsurance_transaction_UPDATE
),
claim_reinsurance_transaction_UPDATE AS (
	MERGE INTO claim_reinsurance_transaction AS T
	USING UPD_claim_reinsurance_transaction_UPDATE AS S
	ON T.claim_reins_trans_id = S.claim_reins_trans_id2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.audit_id = S.audit_id2, T.modified_date = S.modified_date2, T.claim_reins_pms_trans_code = S.LOSS_TRANSACTION2, T.trans_ctgry_code = S.TRANS_CTGRY_CODE3, T.claim_reins_trans_code = S.CLAIM_REINS_TRANS_CODE2, T.claim_reins_trans_amt = S.CLAIM_REINS_TRANS_AMT2, T.claim_reins_trans_hist_amt = S.CLAIM_REINS_TRANS_HIST_AMT2, T.offset_onset_ind = S.LOSS_OFFSET_ONSET_IND2, T.reprocess_date = S.reprocess_date_out2, T.wc_stage_pk_id = S.PIF_4578_stage_id, T.missing_flag = S.MISSING_FLAG
),
SEQ_claim_reinsurance_transaction AS (
	CREATE SEQUENCE SEQ_claim_reinsurance_transaction
	START = 0
	INCREMENT = 1;
),
UPD_claim_reinsurance_transaction_INSERT AS (
	SELECT
	CLAIM_REINS_TRANS_ID AS claim_reins_trans_id1, 
	CLAIMANT_COV_DET_AK_ID, 
	CLAIM_REINS_CODE AS CLAIM_REINS_CODE1, 
	CLAIM_REINS_TRANS_BASE_TYPE_CODE AS CLAIM_REINS_TRANS_BASE_TYPE_CODE1, 
	CLAIM_REINS_TRANS_CODE AS CLAIM_REINS_TRANS_CODE1, 
	CLAIM_REINS_TRANS_AMT AS CLAIM_REINS_TRANS_AMT1, 
	CLAIM_REINS_TRANS_HIST_AMT AS CLAIM_REINS_TRANS_HIST_AMT1, 
	CLAIM_REINS_TRANS_DATE AS CLAIM_REINS_TRANS_DATE1, 
	REINS_ACCT_ENTERED_DATE AS REINS_ACCT_ENTERED_DATE1, 
	LOSS_OFFSET_ONSET_IND AS LOSS_OFFSET_ONSET_IND1, 
	REPROCESS_DATE AS reprocess_date_out1, 
	CRRNT_SNPSHT_FLAG AS crrnt_snpsht_flag1, 
	AUDIT_ID AS audit_id1, 
	EFF_FROM_DATE AS eff_from_date1, 
	EFF_TO_DATE AS eff_to_date1, 
	SOURCE_SYS_ID AS source_sys_id1, 
	CREATED_DATE AS created_date1, 
	MODIFIED_DATE AS modified_date1, 
	CLAIM_REINS_PMS_TRANS_CODE AS LOSS_TRANSACTION, 
	FINANCIAL_TYPE_CODE AS FINANCIAL_TYPE_CODE1, 
	LOSS_TIME, 
	LOGICAL_FLAG, 
	PIF_4578_stage_id, 
	MISSING_FLAG, 
	TRANS_CTGRY_CODE AS TRANS_CTGRY_CODE1, 
	LOSS_ID AS LOSS_ID1, 
	LOSS_CAUSE AS LOSS_CAUSE1, 
	LOSS_RESERVE_CATEGORY, 
	LOSS_TYPE_DISABILITY, 
	reins_cov_ak_id
	FROM RTR_claim_reinsurance_transaction_INSERT
),
claim_reinsurance_transaction_INSERT AS (
	INSERT INTO claim_reinsurance_transaction
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, logical_flag, claim_reins_trans_ak_id, claimant_cov_det_ak_id, reins_cov_ak_id, sar_id, cause_of_loss, reserve_ctgry, type_disability, claim_reins_pms_trans_code, claim_reins_trans_base_type_code, claim_reins_financial_type_code, trans_ctgry_code, claim_reins_trans_code, claim_reins_trans_amt, claim_reins_trans_hist_amt, claim_reins_trans_date, claim_reins_acct_entered_date, offset_onset_ind, reprocess_date, pms_claim_loss_time, wc_stage_pk_id, missing_flag)
	SELECT 
	crrnt_snpsht_flag1 AS CRRNT_SNPSHT_FLAG, 
	audit_id1 AS AUDIT_ID, 
	eff_from_date1 AS EFF_FROM_DATE, 
	eff_to_date1 AS EFF_TO_DATE, 
	source_sys_id1 AS SOURCE_SYS_ID, 
	created_date1 AS CREATED_DATE, 
	modified_date1 AS MODIFIED_DATE, 
	LOGICAL_FLAG AS LOGICAL_FLAG, 
	SEQ_claim_reinsurance_transaction.NEXTVAL AS CLAIM_REINS_TRANS_AK_ID, 
	CLAIMANT_COV_DET_AK_ID AS CLAIMANT_COV_DET_AK_ID, 
	REINS_COV_AK_ID, 
	LOSS_ID1 AS SAR_ID, 
	LOSS_CAUSE1 AS CAUSE_OF_LOSS, 
	LOSS_RESERVE_CATEGORY AS RESERVE_CTGRY, 
	LOSS_TYPE_DISABILITY AS TYPE_DISABILITY, 
	LOSS_TRANSACTION AS CLAIM_REINS_PMS_TRANS_CODE, 
	CLAIM_REINS_TRANS_BASE_TYPE_CODE1 AS CLAIM_REINS_TRANS_BASE_TYPE_CODE, 
	FINANCIAL_TYPE_CODE1 AS CLAIM_REINS_FINANCIAL_TYPE_CODE, 
	TRANS_CTGRY_CODE1 AS TRANS_CTGRY_CODE, 
	CLAIM_REINS_TRANS_CODE1 AS CLAIM_REINS_TRANS_CODE, 
	CLAIM_REINS_TRANS_AMT1 AS CLAIM_REINS_TRANS_AMT, 
	CLAIM_REINS_TRANS_HIST_AMT1 AS CLAIM_REINS_TRANS_HIST_AMT, 
	CLAIM_REINS_TRANS_DATE1 AS CLAIM_REINS_TRANS_DATE, 
	REINS_ACCT_ENTERED_DATE1 AS CLAIM_REINS_ACCT_ENTERED_DATE, 
	LOSS_OFFSET_ONSET_IND1 AS OFFSET_ONSET_IND, 
	reprocess_date_out1 AS REPROCESS_DATE, 
	LOSS_TIME AS PMS_CLAIM_LOSS_TIME, 
	PIF_4578_stage_id AS WC_STAGE_PK_ID, 
	MISSING_FLAG AS MISSING_FLAG
	FROM UPD_claim_reinsurance_transaction_INSERT
),
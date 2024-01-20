WITH
pif_42gq_lit_stage AS (
	SELECT
	pif_42gq_lit_stage_id,
	ipfcgq_loss_claimant,
	pif_symbol,
	pif_policy_number,
	pif_module,
	ipfcgq_year_of_loss,
	ipfcgq_month_of_loss,
	ipfcgq_day_of_loss,
	ipfcgq_loss_occurence
	FROM (
		SELECT 
			pif_42gq_lit_stage_id,
			ipfcgq_loss_claimant,
			pif_symbol,
			pif_policy_number,
			pif_module,
			ipfcgq_year_of_loss,
			ipfcgq_month_of_loss,
			ipfcgq_day_of_loss,
			ipfcgq_loss_occurence
		FROM pif_42gq_lit_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module,ipfcgq_year_of_loss,ipfcgq_month_of_loss,ipfcgq_day_of_loss,ipfcgq_loss_occurence,ipfcgq_loss_claimant ORDER BY pif_42gq_lit_stage_id) = 1
),
SQ_pif_4578_stage AS (
	SELECT
		pif_4578_stage_id,
		pif_symbol,
		pif_policy_number,
		pif_module,
		loss_rec_length,
		loss_action_code,
		loss_file_id,
		loss_id,
		loss_insurance_line,
		loss_location_number,
		loss_sub_location_number,
		loss_risk_unit_group,
		loss_class_code_group,
		loss_class_code_member,
		loss_unit,
		loss_sequence_risk_unit,
		loss_type_exposure,
		loss_major_peril,
		loss_major_peril_seq,
		loss_year_item_effective,
		loss_month_item_effective,
		loss_day_item_effective,
		loss_part,
		loss_year,
		loss_month,
		loss_day,
		loss_occurence,
		loss_claimant,
		loss_member,
		loss_disability,
		loss_reserve_category,
		loss_layer,
		loss_reins_key_id,
		loss_reins_co_no,
		loss_reins_broker,
		loss_base_transaction,
		loss_transaction,
		loss_draft_control_seq,
		loss_sub_part_code,
		loss_segment_status,
		loss_entry_operator,
		loss_transaction_category,
		loss_year_reported,
		loss_month_reported,
		loss_day_reported,
		loss_cause,
		loss_adjustor_no,
		loss_examiner,
		loss_cost_containment,
		loss_paid_or_resv_amt,
		loss_bank_number,
		loss_draft_amount,
		loss_draft_no,
		loss_draft_check_ind,
		loss_transaction_date,
		loss_draft_pay_to_1,
		loss_draft_pay_to_2,
		loss_draft_pay_to_3,
		loss_draft_mail_to,
		loss_net_change_dollars,
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
		loss_offset_onset_ind,
		loss_sub_cont_id,
		loss_rpt_year,
		loss_rpt_mon,
		loss_rpt_day,
		loss_s3_transaction_date,
		loss_rr_reported_date,
		loss_yr2000_cust_use,
		loss_duplicate_key_sequence,
		inf_action,
		inf_timestamp,
		logical_flag,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM pif_4578_stage
),
EXPTRANS AS (
	SELECT
	pif_4578_stage_id,
	pif_symbol,
	pif_policy_number,
	pif_module,
	loss_year,
	loss_month,
	loss_day,
	loss_occurence,
	loss_claimant,
	-- *INF*: :LKP.PIF_42GQ_LIT_STAGE(pif_symbol,pif_policy_number,pif_module,loss_year,loss_month,loss_day,loss_occurence,loss_claimant)
	PIF_42GQ_LIT_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence_loss_claimant.pif_42gq_lit_stage_id AS pif_42gq_lit_stage_id,
	logical_flag,
	source_system_id
	FROM SQ_pif_4578_stage
	LEFT JOIN PIF_42GQ_LIT_STAGE PIF_42GQ_LIT_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence_loss_claimant
	ON PIF_42GQ_LIT_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence_loss_claimant.pif_symbol = pif_symbol
	AND PIF_42GQ_LIT_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence_loss_claimant.pif_policy_number = pif_policy_number
	AND PIF_42GQ_LIT_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence_loss_claimant.pif_module = pif_module
	AND PIF_42GQ_LIT_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence_loss_claimant.ipfcgq_year_of_loss = loss_year
	AND PIF_42GQ_LIT_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence_loss_claimant.ipfcgq_month_of_loss = loss_month
	AND PIF_42GQ_LIT_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence_loss_claimant.ipfcgq_day_of_loss = loss_day
	AND PIF_42GQ_LIT_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence_loss_claimant.ipfcgq_loss_occurence = loss_occurence
	AND PIF_42GQ_LIT_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence_loss_claimant.ipfcgq_loss_claimant = loss_claimant

),
UPDTRANS AS (
	SELECT
	pif_42gq_lit_stage_id, 
	logical_flag
	FROM EXPTRANS
),
PIF_42GQ_LIT_stage1 AS (
	MERGE INTO PIF_42GQ_LIT_stage AS T
	USING UPDTRANS AS S
	ON T.pif_42gq_lit_stage_id = S.pif_42gq_lit_stage_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.logical_flag = S.logical_flag
),
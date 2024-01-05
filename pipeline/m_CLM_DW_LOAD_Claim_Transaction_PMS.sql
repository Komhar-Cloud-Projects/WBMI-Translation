WITH
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
LKP_CLAIMANT_DETAIL_COVERAGE AS (
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
	pms_mbr,
	pms_type_exposure
	FROM (
		SELECT claimant_coverage_detail.claimant_cov_det_ak_id as claimant_cov_det_ak_id, claimant_coverage_detail.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, convert(decimal,LTRIM(RTRIM(claimant_coverage_detail.loc_unit_num))) as loc_unit_num, convert(decimal,LTRIM(RTRIM(claimant_coverage_detail.sub_loc_unit_num))) as sub_loc_unit_num, claimant_coverage_detail.ins_line as ins_line, LTRIM(RTRIM(claimant_coverage_detail.risk_unit_grp)) as risk_unit_grp, LTRIM(RTRIM(claimant_coverage_detail.risk_unit_grp_seq_num)) as risk_unit_grp_seq_num, LTRIM(RTRIM(claimant_coverage_detail.risk_unit)) as risk_unit, CONVERT(DECIMAL,claimant_coverage_detail.risk_unit_seq_num) as risk_unit_seq_num, LTRIM(RTRIM(claimant_coverage_detail.major_peril_code)) as major_peril_code, claimant_coverage_detail.major_peril_seq as major_peril_seq, claimant_coverage_detail.pms_loss_disability as pms_loss_disability, claimant_coverage_detail.reserve_ctgry as reserve_ctgry, claimant_coverage_detail.cause_of_loss as cause_of_loss, claimant_coverage_detail.pms_mbr as pms_mbr, claimant_coverage_detail.pms_type_exposure as pms_type_exposure FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_detail
		  WHERE     (source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}')  AND (CRRNT_SNPSHT_FLAG='1')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id,loc_unit_num,sub_loc_unit_num,ins_line,risk_unit_grp,risk_unit_grp_seq_num,risk_unit,risk_unit_seq_num,major_peril_code,major_peril_seq,pms_loss_disability,reserve_ctgry,cause_of_loss,pms_mbr,pms_type_exposure ORDER BY claimant_cov_det_ak_id) = 1
),
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
LKP_Claim_Payment AS (
	SELECT
	claim_pay_ak_id,
	pms_claimant_cov_det_ak_id,
	micro_ecd_draft_num,
	total_pay_amt,
	pay_issued_date
	FROM (
		SELECT 
		claim_payment.claim_pay_ak_id as claim_pay_ak_id, 
		claim_payment.pms_claimant_cov_det_ak_id as pms_claimant_cov_det_ak_id, 
		LTRIM(RTRIM(claim_payment.micro_ecd_draft_num)) as micro_ecd_draft_num, 
		claim_payment.total_pay_amt as total_pay_amt, 
		claim_payment.pay_issued_date as pay_issued_date 
		FROM 
		 @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_payment
		WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pms_claimant_cov_det_ak_id,micro_ecd_draft_num,total_pay_amt,pay_issued_date ORDER BY claim_pay_ak_id) = 1
),
LKP_Sup_Claim_Transaction_Code AS (
	SELECT
	sup_claim_trans_code_id,
	trans_code
	FROM (
		SELECT 
			sup_claim_trans_code_id,
			trans_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_transaction_code
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY trans_code ORDER BY sup_claim_trans_code_id) = 1
),
LKP_Sup_Convert_PMS_Claim_Transaction_Code_trans_code_id AS (
	SELECT
	sup_convert_claim_trans_code_id,
	pms_trans_code
	FROM (
		SELECT 
			sup_convert_claim_trans_code_id,
			pms_trans_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_convert_pms_claim_transaction_code
		WHERE crrnt_snpsht_flag = 1 AND source_sys_id = 'PMS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pms_trans_code ORDER BY sup_convert_claim_trans_code_id) = 1
),
LKP_Claimant_Coverage_Detail_90_91_92 AS (
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
	pms_mbr,
	pms_type_exposure
	FROM (
		SELECT CCD.claimant_cov_det_ak_id                           AS claimant_cov_det_ak_id,
		       CCD.claim_party_occurrence_ak_id                     AS claim_party_occurrence_ak_id,
		       CONVERT(DECIMAL, Ltrim(Rtrim(CCD.loc_unit_num)))     AS loc_unit_num,
		       CONVERT(DECIMAL, Ltrim(Rtrim(CCD.sub_loc_unit_num))) AS sub_loc_unit_num,
		       CCD.ins_line                                         AS ins_line,
		       Ltrim(Rtrim(CCD.risk_unit_grp))                      AS risk_unit_grp,
		       Ltrim(Rtrim(CCD.risk_unit_grp_seq_num))              AS risk_unit_grp_seq_num,
		       Ltrim(Rtrim(CCD.risk_unit))                          AS risk_unit,
		       CONVERT(DECIMAL, CCD.risk_unit_seq_num)              AS risk_unit_seq_num,
		       Ltrim(Rtrim(CCD.major_peril_code))                   AS major_peril_code,
		       CCD.major_peril_seq                                  AS major_peril_seq,
		       CCD.pms_loss_disability                              AS pms_loss_disability,
		       CCD.reserve_ctgry                                    AS reserve_ctgry,
		       CCD.cause_of_loss         as orig_cause_of_loss,
		       CASE CCD.major_peril_code WHEN '032' 
					THEN  CASE CCD.cause_of_loss
								WHEN '07' THEN '06'
								ELSE CCD.cause_of_loss END
					 ELSE CCD.cause_of_loss     END  AS cause_of_loss,
		       CCD.pms_mbr                                          AS pms_mbr,
		       CCD.pms_type_exposure                                AS pms_type_exposure
		FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_detail CCD
		WHERE  ( source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' )
		       AND ( CRRNT_SNPSHT_FLAG = '1' )
		ORDER BY CCD.claimant_cov_det_ak_id  --
		--- Added Order by to pickup cause of loss 06 coverage record always.
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id,loc_unit_num,sub_loc_unit_num,ins_line,risk_unit_grp,risk_unit_grp_seq_num,risk_unit,risk_unit_seq_num,major_peril_code,major_peril_seq,pms_loss_disability,reserve_ctgry,cause_of_loss,pms_mbr,pms_type_exposure ORDER BY claimant_cov_det_ak_id DESC) = 1
),
SQ_pifmstr_PIF_4578_stage_90_91 AS (
	SELECT A.pif_4578_stage_id,
	       A.pif_symbol,
	       A.pif_policy_number,
	       A.pif_module,
	       A.loss_rec_length,
	       A.loss_action_code,
	       A.loss_file_id,
	       A.loss_id,
	       A.loss_insurance_line,
	       A.loss_location_number,
	       A.loss_sub_location_number,
	       A.loss_risk_unit_group,
	       A.loss_class_code_group,
	       A.loss_class_code_member,
	       A.loss_unit,
	       A.loss_sequence_risk_unit,
	       A.loss_type_exposure,
	       A.loss_major_peril,
	       A.loss_major_peril_seq,
	       A.loss_year_item_effective,
	       A.loss_month_item_effective,
	       A.loss_day_item_effective,
	       A.loss_part,
	       A.loss_year,
	       A.loss_month,
	       A.loss_day,
	       A.loss_occurence,
	       A.loss_claimant,
	       A.loss_member,
	       A.loss_disability,
	       A.loss_reserve_category,
	       A.loss_layer,
	       A.loss_reins_key_id,
	       A.loss_reins_co_no,
	       A.loss_reins_broker,
	       A.loss_base_transaction,
	       A.loss_transaction,
	       A.loss_draft_control_seq,
	       A.loss_sub_part_code,
	       A.loss_segment_status,
	       A.loss_entry_operator,
	       A.loss_transaction_category,
	       A.loss_year_reported,
	       A.loss_month_reported,
	       A.loss_day_reported,
	       A.loss_cause,
	       A.loss_adjustor_no,
	       A.loss_examiner,
	       A.loss_cost_containment,
	       A.loss_paid_or_resv_amt,
	       A.loss_bank_number,
	       A.loss_draft_amount,
	       A.loss_draft_no,
	       A.loss_draft_check_ind,
	       A.loss_transaction_date,
	       A.loss_draft_pay_to_1,
	       A.loss_draft_pay_to_2,
	       A.loss_draft_pay_to_3,
	       A.loss_draft_mail_to,
	       A.loss_net_change_dollars,
	       A.loss_account_entered_date,
	       A.loss_average_reserve_code,
	       A.loss_handling_office,
	       A.loss_start_yr,
	       A.loss_start_mo,
	       A.loss_start_da,
	       A.loss_fault_code,
	       A.tc,
	       A.ia,
	       A.loss_payment_rate,
	       A.loss_frequency,
	       A.loss_period_pay,
	       A.loss_sub_line,
	       A.loss_payee_phrase,
	       A.loss_memo_phrase,
	       A.iws_origin_indicator,
	       A.loss_aia_codes_1_2,
	       A.loss_aia_codes_3_4,
	       A.loss_aia_codes_5_6,
	       A.loss_aia_sub_code,
	       A.loss_accident_state,
	       A.loss_handling_branch,
	       A.loss_1099_number,
	       A.loss_claim_payee,
	       A.loss_claim_payee_name,
	       A.loss_notes_draft_payee,
	       A.loss_claim_number,
	       A.loss_type_claim_payee,
	       A.loss_zpcd_inj_loc,
	       A.loss_special_use_1,
	       A.loss_special_use_2,
	       A.loss_time,
	       A.loss_type_disability,
	       A.loss_claims_made_ind,
	       A.loss_misc_adjustor_ind,
	       A.loss_pms_future_use,
	       A.loss_offset_onset_ind,
	       A.loss_sub_cont_id,
	       A.loss_rpt_year,
	       A.loss_rpt_mon,
	       A.loss_rpt_day,
	       A.loss_s3_transaction_date,
	       A.loss_rr_reported_date,
	       A.loss_yr2000_cust_use,
	       A.loss_duplicate_key_sequence,
	       A.inf_action,
	       A.extract_date,
	       A.as_of_date,
	       A.record_count,
	       A.source_system_id
	FROM   pif_4578_stage A
	WHERE  ( A.loss_part = '7' )
	       AND ( A.logical_flag = '0' )
	       AND A.loss_transaction IN ( '90', '91' )
	      AND A.inf_action <> 'A'
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
	loss_reins_co_no, 
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
	SUM(in_loss_paid_or_resv_amt) AS loss_paid_or_resv_amt, 
	loss_bank_number, 
	loss_draft_amount, 
	loss_draft_check_ind, 
	loss_draft_pay_to_1, 
	loss_draft_pay_to_2, 
	loss_draft_pay_to_3, 
	loss_draft_mail_to, 
	loss_net_change_dollars AS in_loss_net_change_dollars, 
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
	extract_date, 
	as_of_date, 
	record_count, 
	source_system_id
	FROM SQ_pifmstr_PIF_4578_stage_90_91
	GROUP BY pif_symbol, pif_policy_number, pif_module, loss_year, loss_month, loss_day, loss_occurence, loss_claimant, loss_location_number, loss_sub_location_number, loss_insurance_line, loss_risk_unit_group, loss_unit, loss_major_peril, loss_major_peril_seq, loss_disability, loss_reserve_category, loss_cause, loss_member, loss_type_exposure, loss_offset_onset_ind, loss_transaction_date, loss_transaction_category, loss_draft_no, loss_sequence_risk_unit, loss_transaction
),
SQ_pifmstr_PIF_4578_stage AS (
	SELECT A.pif_4578_stage_id,
	       A.pif_symbol,
	       A.pif_policy_number,
	       A.pif_module,
	       A.loss_rec_length,
	       A.loss_action_code,
	       A.loss_file_id,
	       A.loss_id,
	       A.loss_insurance_line,
	       A.loss_location_number,
	       A.loss_sub_location_number,
	       A.loss_risk_unit_group,
	       A.loss_class_code_group,
	       A.loss_class_code_member,
	       A.loss_unit,
	       A.loss_sequence_risk_unit,
	       A.loss_type_exposure,
	       A.loss_major_peril,
	       A.loss_major_peril_seq,
	       A.loss_year_item_effective,
	       A.loss_month_item_effective,
	       A.loss_day_item_effective,
	       A.loss_part,
	       A.loss_year,
	       A.loss_month,
	       A.loss_day,
	       A.loss_occurence,
	       A.loss_claimant,
	       A.loss_member,
	       A.loss_disability,
	       A.loss_reserve_category,
	       A.loss_layer,
	       A.loss_reins_key_id,
	       A.loss_reins_co_no,
	       A.loss_reins_broker,
	       A.loss_base_transaction,
	       A.loss_transaction,
	       A.loss_draft_control_seq,
	       A.loss_sub_part_code,
	       A.loss_segment_status,
	       A.loss_entry_operator,
	       A.loss_transaction_category,
	       A.loss_year_reported,
	       A.loss_month_reported,
	       A.loss_day_reported,
	       A.loss_cause,
	       A.loss_adjustor_no,
	       A.loss_examiner,
	       A.loss_cost_containment,
	       A.loss_paid_or_resv_amt,
	       A.loss_bank_number,
	       A.loss_draft_amount,
	       A.loss_draft_no,
	       A.loss_draft_check_ind,
	       A.loss_transaction_date,
	       A.loss_draft_pay_to_1,
	       A.loss_draft_pay_to_2,
	       A.loss_draft_pay_to_3,
	       A.loss_draft_mail_to,
	       A.loss_net_change_dollars,
	       A.loss_account_entered_date,
	       A.loss_average_reserve_code,
	       A.loss_handling_office,
	       A.loss_start_yr,
	       A.loss_start_mo,
	       A.loss_start_da,
	       A.loss_fault_code,
	       A.tc,
	       A.ia,
	       A.loss_payment_rate,
	       A.loss_frequency,
	       A.loss_period_pay,
	       A.loss_sub_line,
	       A.loss_payee_phrase,
	       A.loss_memo_phrase,
	       A.iws_origin_indicator,
	       A.loss_aia_codes_1_2,
	       A.loss_aia_codes_3_4,
	       A.loss_aia_codes_5_6,
	       A.loss_aia_sub_code,
	       A.loss_accident_state,
	       A.loss_handling_branch,
	       A.loss_1099_number,
	       A.loss_claim_payee,
	       A.loss_claim_payee_name,
	       A.loss_notes_draft_payee,
	       A.loss_claim_number,
	       A.loss_type_claim_payee,
	       A.loss_zpcd_inj_loc,
	       A.loss_special_use_1,
	       A.loss_special_use_2,
	       A.loss_time,
	       A.loss_type_disability,
	       A.loss_claims_made_ind,
	       A.loss_misc_adjustor_ind,
	       A.loss_pms_future_use,
	       A.loss_offset_onset_ind,
	       A.loss_sub_cont_id,
	       A.loss_rpt_year,
	       A.loss_rpt_mon,
	       A.loss_rpt_day,
	       A.loss_s3_transaction_date,
	       A.loss_rr_reported_date,
	       A.loss_yr2000_cust_use,
	       A.loss_duplicate_key_sequence,
	       A.inf_action,
	       A.extract_date,
	       A.as_of_date,
	       A.record_count,
	       A.source_system_id
	FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_4578_stage A
	WHERE  (A.loss_part = '7' )
	       AND (A.logical_flag = '0' )
	       AND A.loss_transaction NOT IN ( '90', '91' )
	      AND  A.inf_action <> 'A'
),
Union_Stage_90_91 AS (
	SELECT pif_4578_stage_id, pif_symbol, pif_policy_number, pif_module, loss_rec_length, loss_action_code, loss_file_id, loss_id, loss_insurance_line, loss_location_number, loss_sub_location_number, loss_risk_unit_group, loss_class_code_group, loss_class_code_member, loss_unit, loss_sequence_risk_unit, loss_type_exposure, loss_major_peril, loss_major_peril_seq, loss_year_item_effective, loss_month_item_effective, loss_day_item_effective, loss_part, loss_year, loss_month, loss_day, loss_occurence, loss_claimant, loss_member, loss_disability, loss_reserve_category, loss_layer, loss_reins_key_id, loss_reins_co_no, loss_reins_broker, loss_base_transaction, loss_transaction, loss_draft_control_seq, loss_sub_part_code, loss_segment_status, loss_entry_operator, loss_transaction_category, loss_year_reported, loss_month_reported, loss_day_reported, loss_cause, loss_adjustor_no, loss_examiner, loss_cost_containment, loss_paid_or_resv_amt, loss_bank_number, loss_draft_amount, loss_draft_no, loss_draft_check_ind, loss_transaction_date, loss_draft_pay_to_1, loss_draft_pay_to_2, loss_draft_pay_to_3, loss_draft_mail_to, loss_net_change_dollars, loss_account_entered_date, loss_average_reserve_code, loss_handling_office, loss_start_yr, loss_start_mo, loss_start_da, loss_fault_code, tc, ia, loss_payment_rate, loss_frequency, loss_period_pay, loss_sub_line, loss_payee_phrase, loss_memo_phrase, iws_origin_indicator, loss_aia_codes_1_2, loss_aia_codes_3_4, loss_aia_codes_5_6, loss_aia_sub_code, loss_accident_state, loss_handling_branch, loss_1099_number, loss_claim_payee, loss_claim_payee_name, loss_notes_draft_payee, loss_claim_number, loss_type_claim_payee, loss_zpcd_inj_loc, loss_special_use_1, loss_special_use_2, loss_time, loss_type_disability, loss_claims_made_ind, loss_misc_adjustor_ind, loss_pms_future_use, loss_offset_onset_ind, loss_sub_cont_id, loss_rpt_year, loss_rpt_mon, loss_rpt_day, loss_s3_transaction_date, loss_rr_reported_date, loss_yr2000_cust_use, loss_duplicate_key_sequence, inf_action, extract_date, as_of_date, record_count, source_system_id
	FROM AGG_amts_on_90_91
	UNION
	SELECT pif_4578_stage_id, pif_symbol, pif_policy_number, pif_module, loss_rec_length, loss_action_code, loss_file_id, loss_id, loss_insurance_line, loss_location_number, loss_sub_location_number, loss_risk_unit_group, loss_class_code_group, loss_class_code_member, loss_unit, loss_sequence_risk_unit, loss_type_exposure, loss_major_peril, loss_major_peril_seq, loss_year_item_effective, loss_month_item_effective, loss_day_item_effective, loss_part, loss_year, loss_month, loss_day, loss_occurence, loss_claimant, loss_member, loss_disability, loss_reserve_category, loss_layer, loss_reins_key_id, loss_reins_co_no, loss_reins_broker, loss_base_transaction, loss_transaction, loss_draft_control_seq, loss_sub_part_code, loss_segment_status, loss_entry_operator, loss_transaction_category, loss_year_reported, loss_month_reported, loss_day_reported, loss_cause, loss_adjustor_no, loss_examiner, loss_cost_containment, loss_paid_or_resv_amt, loss_bank_number, loss_draft_amount, loss_draft_no, loss_draft_check_ind, loss_transaction_date, loss_draft_pay_to_1, loss_draft_pay_to_2, loss_draft_pay_to_3, loss_draft_mail_to, loss_net_change_dollars, loss_account_entered_date, loss_average_reserve_code, loss_handling_office, loss_start_yr, loss_start_mo, loss_start_da, loss_fault_code, tc, ia, loss_payment_rate, loss_frequency, loss_period_pay, loss_sub_line, loss_payee_phrase, loss_memo_phrase, iws_origin_indicator, loss_aia_codes_1_2, loss_aia_codes_3_4, loss_aia_codes_5_6, loss_aia_sub_code, loss_accident_state, loss_handling_branch, loss_1099_number, loss_claim_payee, loss_claim_payee_name, loss_notes_draft_payee, loss_claim_number, loss_type_claim_payee, loss_zpcd_inj_loc, loss_special_use_1, loss_special_use_2, loss_time, loss_type_disability, loss_claims_made_ind, loss_misc_adjustor_ind, loss_pms_future_use, loss_offset_onset_ind, loss_sub_cont_id, loss_rpt_year, loss_rpt_mon, loss_rpt_day, loss_s3_transaction_date, loss_rr_reported_date, loss_yr2000_cust_use, loss_duplicate_key_sequence, inf_action, extract_date, as_of_date, record_count, source_system_id
	FROM SQ_pifmstr_PIF_4578_stage
),
EXP_CLAIM_TRANSACTION_VALIDATE AS (
	SELECT
	pif_4578_stage_id AS PIF_4578_stage_id,
	pif_symbol AS PIF_SYMBOL,
	pif_policy_number AS PIF_POLICY_NUMBER,
	pif_module AS PIF_MODULE,
	loss_rec_length AS LOSS_REC_LENGTH,
	loss_action_code AS LOSS_ACTION_CODE,
	loss_file_id AS LOSS_FILE_ID,
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
	loss_year_item_effective AS LOSS_YEAR_ITEM_EFFECTIVE,
	loss_month_item_effective AS LOSS_MONTH_ITEM_EFFECTIVE,
	loss_day_item_effective AS LOSS_DAY_ITEM_EFFECTIVE,
	loss_part AS LOSS_PART,
	loss_year AS LOSS_YEAR,
	loss_month AS LOSS_MONTH,
	loss_day AS LOSS_DAY,
	loss_occurence AS LOSS_OCCURENCE,
	loss_claimant AS LOSS_CLAIMANT,
	loss_member AS LOSS_MEMBER,
	loss_disability AS LOSS_DISABILITY,
	loss_reserve_category AS LOSS_RESERVE_CATEGORY,
	loss_layer AS LOSS_LAYER,
	loss_reins_key_id AS LOSS_REINS_KEY_ID,
	loss_reins_co_no AS LOSS_REINS_CO_NO,
	loss_reins_broker AS LOSS_REINS_BROKER,
	loss_base_transaction AS LOSS_BASE_TRANSACTION,
	loss_transaction AS LOSS_TRANSACTION,
	loss_draft_control_seq AS LOSS_DRAFT_CONTROL_SEQ,
	loss_sub_part_code AS LOSS_SUB_PART_CODE,
	loss_segment_status AS LOSS_SEGMENT_STATUS,
	loss_entry_operator AS LOSS_ENTRY_OPERATOR,
	loss_transaction_category AS LOSS_TRANSACTION_CATEGORY,
	loss_year_reported AS LOSS_YEAR_REPORTED,
	loss_month_reported AS LOSS_MONTH_REPORTED,
	loss_day_reported AS LOSS_DAY_REPORTED,
	loss_cause AS LOSS_CAUSE,
	loss_adjustor_no AS LOSS_ADJUSTOR_NO,
	loss_examiner AS LOSS_EXAMINER,
	loss_cost_containment AS LOSS_COST_CONTAINMENT,
	loss_paid_or_resv_amt AS LOSS_PAID_OR_RESV_AMT,
	loss_bank_number AS LOSS_BANK_NUMBER,
	loss_draft_amount AS LOSS_DRAFT_AMOUNT,
	loss_draft_no AS LOSS_DRAFT_NO,
	loss_draft_check_ind AS LOSS_DRAFT_CHECK_IND,
	loss_transaction_date AS LOSS_TRANSACTION_DATE,
	loss_draft_pay_to_1 AS LOSS_DRAFT_PAY_TO_1,
	loss_draft_pay_to_2 AS LOSS_DRAFT_PAY_TO_2,
	loss_draft_pay_to_3 AS LOSS_DRAFT_PAY_TO_3,
	loss_draft_mail_to AS LOSS_DRAFT_MAIL_TO,
	loss_net_change_dollars AS LOSS_NET_CHANGE_DOLLARS,
	loss_account_entered_date AS LOSS_ACCOUNT_ENTERED_DATE,
	loss_average_reserve_code AS LOSS_AVERAGE_RESERVE_CODE,
	loss_handling_office AS LOSS_HANDLING_OFFICE,
	loss_start_yr AS LOSS_START_YR,
	loss_start_mo AS LOSS_START_MO,
	loss_start_da AS LOSS_START_DA,
	loss_fault_code,
	tc,
	ia,
	loss_payment_rate AS LOSS_PAYMENT_RATE,
	loss_frequency AS LOSS_FREQUENCY,
	loss_period_pay AS LOSS_PERIOD_PAY,
	loss_sub_line AS LOSS_SUB_LINE,
	loss_payee_phrase AS LOSS_PAYEE_PHRASE,
	loss_memo_phrase AS LOSS_MEMO_PHRASE,
	iws_origin_indicator AS IWS_ORIGIN_INDICATOR,
	loss_aia_codes_1_2 AS LOSS_AIA_CODES_1_2,
	loss_aia_codes_3_4 AS LOSS_AIA_CODES_3_4,
	loss_aia_codes_5_6 AS LOSS_AIA_CODES_5_6,
	loss_aia_sub_code AS LOSS_AIA_SUB_CODE,
	loss_accident_state AS LOSS_ACCIDENT_STATE,
	loss_handling_branch AS LOSS_HANDLING_BRANCH,
	loss_1099_number AS LOSS_1099_NUMBER,
	loss_claim_payee AS LOSS_CLAIM_PAYEE,
	loss_claim_payee_name AS LOSS_CLAIM_PAYEE_NAME,
	loss_notes_draft_payee AS LOSS_NOTES_DRAFT_PAYEE,
	loss_claim_number AS LOSS_CLAIM_NUMBER,
	loss_type_claim_payee AS LOSS_TYPE_CLAIM_PAYEE,
	loss_zpcd_inj_loc AS LOSS_ZPCD_INJ_LOC,
	loss_special_use_1 AS LOSS_SPECIAL_USE_1,
	loss_special_use_2 AS LOSS_SPECIAL_USE_2,
	loss_type_disability AS LOSS_TYPE_DISABILITY,
	loss_claims_made_ind AS LOSS_CLAIMS_MADE_IND,
	loss_misc_adjustor_ind AS LOSS_MISC_ADJUSTOR_IND,
	loss_pms_future_use AS LOSS_PMS_FUTURE_USE,
	loss_offset_onset_ind AS LOSS_OFFSET_ONSET_IND,
	loss_sub_cont_id AS LOSS_SUB_CONT_ID,
	loss_rpt_year AS LOSS_RPT_YEAR,
	loss_rpt_mon AS LOSS_RPT_MON,
	loss_rpt_day AS LOSS_RPT_DAY,
	loss_s3_transaction_date AS LOSS_S3_TRANSACTION_DATE,
	loss_rr_reported_date AS LOSS_RR_REPORTED_DATE,
	loss_yr2000_cust_use AS LOSS_YR2000_CUST_USE,
	loss_duplicate_key_sequence AS LOSS_DUPLICATE_KEY_SEQUENCE,
	inf_action,
	extract_date AS EXTRACT_DATE,
	as_of_date AS AS_OF_DATE,
	record_count AS RECORD_COUNT,
	source_system_id AS SOURCE_SYSTEM_ID,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id
	FROM Union_Stage_90_91
),
EXP_CLAIMS_TRANSACTION_DEFAULT AS (
	SELECT
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
	LOSS_BASE_TRANSACTION AS IN_LOSS_BASE_TRANSACTION,
	LOSS_TRANSACTION AS IN_LOSS_TRANSACTION,
	LOSS_ENTRY_OPERATOR AS IN_LOSS_ENTRY_OPERATOR,
	LOSS_TRANSACTION_CATEGORY AS IN_LOSS_TRANSACTION_CATEGORY,
	LOSS_CAUSE AS IN_LOSS_CAUSE,
	LOSS_ADJUSTOR_NO AS IN_LOSS_ADJUSTOR_NO,
	LOSS_PAID_OR_RESV_AMT AS IN_LOSS_PAID_OR_RESV_AMT,
	LOSS_DRAFT_NO AS IN_LOSS_DRAFT_NO,
	LOSS_DRAFT_AMOUNT AS IN_LOSS_DRAFT_AMOUNT,
	LOSS_TRANSACTION_DATE AS IN_LOSS_TRANSACTION_DATE,
	LOSS_NET_CHANGE_DOLLARS AS IN_LOSS_NET_CHANGE_DOLLARS,
	LOSS_ACCOUNT_ENTERED_DATE AS IN_LOSS_ACCOUNT_ENTERED_DATE,
	LOSS_TYPE_DISABILITY AS IN_LOSS_TYPE_DISABILITY,
	LOSS_OFFSET_ONSET_IND AS IN_LOSS_OFFSET_ONSET_IND,
	LOSS_DRAFT_PAY_TO_1 AS IN_LOSS_DRAFT_PAY_TO_1,
	LOSS_DRAFT_PAY_TO_2 AS IN_LOSS_DRAFT_PAY_TO_2,
	LOSS_DRAFT_PAY_TO_3 AS IN_LOSS_DRAFT_PAY_TO_3,
	LOSS_DRAFT_MAIL_TO AS IN_LOSS_DRAFT_MAIL_TO,
	inf_action AS IN_inf_action,
	SOURCE_SYSTEM_ID AS IN_SOURCE_SYSTEM_ID,
	crrnt_snpsht_flag AS IN_crrnt_snpsht_flag,
	audit_id AS IN_audit_id,
	IN_PIF_4578_stage_id AS PIF_4578_stage_id,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_PIF_SYMBOL))),'N/A',IIF(IS_SPACES(IN_PIF_SYMBOL),'N/A',LTRIM(RTRIM(IN_PIF_SYMBOL))))
	IFF(LTRIM(RTRIM(IN_PIF_SYMBOL)) IS NULL, 'N/A', IFF(IS_SPACES(IN_PIF_SYMBOL), 'N/A', LTRIM(RTRIM(IN_PIF_SYMBOL)))) AS PIF_SYMBOL,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_PIF_POLICY_NUMBER))),'N/A',IIF(IS_SPACES(IN_PIF_POLICY_NUMBER),'N/A',LTRIM(RTRIM(IN_PIF_POLICY_NUMBER))))
	IFF(LTRIM(RTRIM(IN_PIF_POLICY_NUMBER)) IS NULL, 'N/A', IFF(IS_SPACES(IN_PIF_POLICY_NUMBER), 'N/A', LTRIM(RTRIM(IN_PIF_POLICY_NUMBER)))) AS PIF_POLICY_NUMBER,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_PIF_MODULE))),'N/A',IIF(IS_SPACES(IN_PIF_MODULE),'N/A',LTRIM(RTRIM(IN_PIF_MODULE))))
	IFF(LTRIM(RTRIM(IN_PIF_MODULE)) IS NULL, 'N/A', IFF(IS_SPACES(IN_PIF_MODULE), 'N/A', LTRIM(RTRIM(IN_PIF_MODULE)))) AS PIF_MODULE,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_ID))),'N/A',IIF(IS_SPACES(IN_LOSS_ID),'N/A',LTRIM(RTRIM(IN_LOSS_ID))))
	IFF(LTRIM(RTRIM(IN_LOSS_ID)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_ID), 'N/A', LTRIM(RTRIM(IN_LOSS_ID)))) AS LOSS_ID,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_INSURANCE_LINE))),'N/A',IIF(IS_SPACES(IN_LOSS_INSURANCE_LINE),'N/A',LTRIM(RTRIM(IN_LOSS_INSURANCE_LINE))))
	IFF(LTRIM(RTRIM(IN_LOSS_INSURANCE_LINE)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_INSURANCE_LINE), 'N/A', LTRIM(RTRIM(IN_LOSS_INSURANCE_LINE)))) AS LOSS_INSURANCE_LINE,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_INSURANCE_LINE))),'0',IIF(IS_SPACES(IN_LOSS_INSURANCE_LINE),'0',LTRIM(RTRIM(IN_LOSS_INSURANCE_LINE))))
	IFF(LTRIM(RTRIM(IN_LOSS_INSURANCE_LINE)) IS NULL, '0', IFF(IS_SPACES(IN_LOSS_INSURANCE_LINE), '0', LTRIM(RTRIM(IN_LOSS_INSURANCE_LINE)))) AS LOSS_INSURANCE_LINE_lkp,
	-- *INF*: IIF(ISNULL(IN_LOSS_LOCATION_NUMBER),0,IN_LOSS_LOCATION_NUMBER)
	IFF(IN_LOSS_LOCATION_NUMBER IS NULL, 0, IN_LOSS_LOCATION_NUMBER) AS LOSS_LOCATION_NUMBER,
	-- *INF*: IIF(ISNULL(IN_LOSS_SUB_LOCATION_NUMBER),0,IN_LOSS_SUB_LOCATION_NUMBER)
	IFF(IN_LOSS_SUB_LOCATION_NUMBER IS NULL, 0, IN_LOSS_SUB_LOCATION_NUMBER) AS LOSS_SUB_LOCATION_NUMBER,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_RISK_UNIT_GROUP))),'N/A',IIF(IS_SPACES(IN_LOSS_RISK_UNIT_GROUP),'N/A',LTRIM(RTRIM(IN_LOSS_RISK_UNIT_GROUP))))
	IFF(LTRIM(RTRIM(IN_LOSS_RISK_UNIT_GROUP)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_RISK_UNIT_GROUP), 'N/A', LTRIM(RTRIM(IN_LOSS_RISK_UNIT_GROUP)))) AS LOSS_RISK_UNIT_GROUP,
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
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_UNIT))),'0',IIF(IS_SPACES(IN_LOSS_UNIT),'0',LTRIM(RTRIM(IN_LOSS_UNIT))))
	IFF(LTRIM(RTRIM(IN_LOSS_UNIT)) IS NULL, '0', IFF(IS_SPACES(IN_LOSS_UNIT), '0', LTRIM(RTRIM(IN_LOSS_UNIT)))) AS LOSS_UNIT_lkp,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_SEQUENCE_RISK_UNIT))),'0',IIF(IS_SPACES(IN_LOSS_SEQUENCE_RISK_UNIT),'0',LTRIM(RTRIM(IN_LOSS_SEQUENCE_RISK_UNIT))))
	IFF(LTRIM(RTRIM(IN_LOSS_SEQUENCE_RISK_UNIT)) IS NULL, '0', IFF(IS_SPACES(IN_LOSS_SEQUENCE_RISK_UNIT), '0', LTRIM(RTRIM(IN_LOSS_SEQUENCE_RISK_UNIT)))) AS LOSS_SEQUENCE_RISK_UNIT,
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
	-- *INF*: IIF(ISNULL(IN_LOSS_BASE_TRANSACTION),'N/A',TO_CHAR(IN_LOSS_BASE_TRANSACTION))
	IFF(IN_LOSS_BASE_TRANSACTION IS NULL, 'N/A', TO_CHAR(IN_LOSS_BASE_TRANSACTION)) AS LOSS_BASE_TRANSACTION,
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
	-- *INF*: IIF(ISNULL(IN_LOSS_DRAFT_AMOUNT), 0, IN_LOSS_DRAFT_AMOUNT)
	IFF(IN_LOSS_DRAFT_AMOUNT IS NULL, 0, IN_LOSS_DRAFT_AMOUNT) AS LOSS_DRAFT_AMOUNT,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE))),'18000101',LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE)))
	IFF(LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE)) IS NULL, '18000101', LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE))) AS LOSS_TRANSACTION_DATE,
	-- *INF*: IIF(ISNULL(IN_LOSS_TRANSACTION_DATE),TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),to_date(IN_LOSS_TRANSACTION_DATE, 'YYYYMMDD'))
	IFF(IN_LOSS_TRANSACTION_DATE IS NULL, TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), to_date(IN_LOSS_TRANSACTION_DATE, 'YYYYMMDD')) AS LOSS_TRANSACTION_DATE_payment,
	-- *INF*: IIF(ISNULL(IN_LOSS_NET_CHANGE_DOLLARS),0,IN_LOSS_NET_CHANGE_DOLLARS)
	IFF(IN_LOSS_NET_CHANGE_DOLLARS IS NULL, 0, IN_LOSS_NET_CHANGE_DOLLARS) AS LOSS_NET_CHANGE_DOLLARS,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_ACCOUNT_ENTERED_DATE))),'180001',IIF(IS_SPACES(IN_LOSS_ACCOUNT_ENTERED_DATE),'180001',LTRIM(RTRIM(IN_LOSS_ACCOUNT_ENTERED_DATE))))
	IFF(LTRIM(RTRIM(IN_LOSS_ACCOUNT_ENTERED_DATE)) IS NULL, '180001', IFF(IS_SPACES(IN_LOSS_ACCOUNT_ENTERED_DATE), '180001', LTRIM(RTRIM(IN_LOSS_ACCOUNT_ENTERED_DATE)))) AS LOSS_ACCOUNT_ENTERED_DATE,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_TYPE_DISABILITY))),'N/A',IIF(IS_SPACES(IN_LOSS_TYPE_DISABILITY),'N/A',
	-- LTRIM(RTRIM(IN_LOSS_TYPE_DISABILITY))))
	IFF(LTRIM(RTRIM(IN_LOSS_TYPE_DISABILITY)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_TYPE_DISABILITY), 'N/A', LTRIM(RTRIM(IN_LOSS_TYPE_DISABILITY)))) AS LOSS_TYPE_DISABILITY,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_OFFSET_ONSET_IND))),'N/A',IIF(IS_SPACES(IN_LOSS_OFFSET_ONSET_IND),'N/A',LTRIM(RTRIM(IN_LOSS_OFFSET_ONSET_IND))))
	IFF(LTRIM(RTRIM(IN_LOSS_OFFSET_ONSET_IND)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_OFFSET_ONSET_IND), 'N/A', LTRIM(RTRIM(IN_LOSS_OFFSET_ONSET_IND)))) AS LOSS_OFFSET_ONSET_IND,
	-- *INF*: LTRIM(RTRIM(IN_LOSS_DRAFT_PAY_TO_1))||LTRIM(RTRIM(IN_LOSS_DRAFT_PAY_TO_2))||LTRIM(RTRIM(IN_LOSS_DRAFT_PAY_TO_3))
	LTRIM(RTRIM(IN_LOSS_DRAFT_PAY_TO_1)) || LTRIM(RTRIM(IN_LOSS_DRAFT_PAY_TO_2)) || LTRIM(RTRIM(IN_LOSS_DRAFT_PAY_TO_3)) AS v_pay_to_code,
	-- *INF*: IIF(ISNULL(v_pay_to_code) OR IS_SPACES(LTRIM(RTRIM(v_pay_to_code))) OR LENGTH(LTRIM(RTRIM(v_pay_to_code))) = 0, 'N/A', LTRIM(RTRIM(v_pay_to_code)))
	IFF(v_pay_to_code IS NULL OR IS_SPACES(LTRIM(RTRIM(v_pay_to_code))) OR LENGTH(LTRIM(RTRIM(v_pay_to_code))) = 0, 'N/A', LTRIM(RTRIM(v_pay_to_code))) AS pay_to_code,
	-- *INF*: IIF(ISNULL(IN_LOSS_DRAFT_MAIL_TO) OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_DRAFT_MAIL_TO))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_DRAFT_MAIL_TO))) = 0, 'N/A', LTRIM(RTRIM(IN_LOSS_DRAFT_MAIL_TO)))
	IFF(IN_LOSS_DRAFT_MAIL_TO IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_DRAFT_MAIL_TO))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_DRAFT_MAIL_TO))) = 0, 'N/A', LTRIM(RTRIM(IN_LOSS_DRAFT_MAIL_TO))) AS LOSS_DRAFT_MAIL_TO,
	-- *INF*: IIF(ISNULL(IN_inf_action),'N/A',IN_inf_action)
	IFF(IN_inf_action IS NULL, 'N/A', IN_inf_action) AS inf_action,
	IN_SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID,
	IN_crrnt_snpsht_flag AS crrnt_snpsht_flag,
	IN_audit_id AS audit_id,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_ADJUSTOR_NO))),'N/A',IIF(IS_SPACES(IN_LOSS_ADJUSTOR_NO),'N/A',LTRIM(RTRIM(IN_LOSS_ADJUSTOR_NO))))
	IFF(LTRIM(RTRIM(IN_LOSS_ADJUSTOR_NO)) IS NULL, 'N/A', IFF(IS_SPACES(IN_LOSS_ADJUSTOR_NO), 'N/A', LTRIM(RTRIM(IN_LOSS_ADJUSTOR_NO)))) AS LOSS_ADJUSTOR_NO
	FROM EXP_CLAIM_TRANSACTION_VALIDATE
),
LKP_42x6_Stage_Process_Date AS (
	SELECT
	ipfcx6_year_process,
	ipfcx6_month_process,
	ipfcx6_day_process,
	pif_symbol,
	pif_policy_number,
	pif_module,
	ipfcx6_year_of_loss,
	ipfcx6_month_of_loss,
	ipfcx6_day_of_loss,
	ipfcx6_loss_occ_fdigit,
	ipfcx6_usr_loss_occurence,
	ipfcx6_loss_claimant,
	ipfcx6_insurance_line,
	ipfcx6_location_number,
	ipfcx6_sub_location_number,
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
		FROM pif_42x6_stage
		order by ipfcx6_year_process, 
		ipfcx6_month_process, 
		ipfcx6_day_process --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module,ipfcx6_year_of_loss,ipfcx6_month_of_loss,ipfcx6_day_of_loss,ipfcx6_loss_occ_fdigit,ipfcx6_usr_loss_occurence,ipfcx6_loss_claimant,ipfcx6_insurance_line,ipfcx6_location_number,ipfcx6_sub_location_number,ipfcx6_risk_unit_group,ipfcx6_class_code_group,ipfcx6_class_code_member,ipfcx6_loss_unit,ipfcx6_risk_sequence,ipfcx6_major_peril,ipfcx6_sequence_type_exposure,ipfcx6_loss_disability,ipfcx6_member,ipfcx6_reserve_category,ipfcx6_loss_cause ORDER BY ipfcx6_year_process) = 1
),
LKP_PMS_ADJUSTOR_MASTER_STAGE AS (
	SELECT
	adnm_taxid_ssn,
	adnm_adjustor_nbr
	FROM (
		SELECT
		 LTRIM(RTRIM(pms_adjuster_master_stage.adnm_taxid_ssn)) as adnm_taxid_ssn,
		 LTRIM(RTRIM(pms_adjuster_master_stage.adnm_adjustor_nbr)) as adnm_adjustor_nbr
		 FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.pms_adjuster_master_stage
		WHERE 
		pms_adjuster_master_stage.adnm_adjustor_nbr not in ('QUE', 'QUR','TEN')
		and
		substring(pms_adjuster_master_stage.adnm_adjustor_nbr,1,1)<>'X'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY adnm_adjustor_nbr ORDER BY adnm_taxid_ssn) = 1
),
LKP_Claim_Master_1099_List AS (
	SELECT
	claim_master_1099_list_ak_id,
	adnm_tax_id,
	tax_id
	FROM (
		SELECT 
		claim_master_1099_list.claim_master_1099_list_ak_id as claim_master_1099_list_ak_id, 
		LTRIM(RTRIM(claim_master_1099_list.tax_id)) as tax_id 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_master_1099_list
		where
		claim_master_1099_list.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY tax_id ORDER BY claim_master_1099_list_ak_id) = 1
),
LKP_Sup_Convert_Pms_Claim_Transaction_Code AS (
	SELECT
	edw_financial_type_code,
	edw_trans_code,
	edw_trans_ctgry_code,
	pms_trans_code
	FROM (
		SELECT 
			edw_financial_type_code,
			edw_trans_code,
			edw_trans_ctgry_code,
			pms_trans_code
		FROM sup_convert_pms_claim_transaction_code
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pms_trans_code ORDER BY edw_financial_type_code) = 1
),
EXP_Claim_Transaction_Detect_Changes AS (
	SELECT
	EXP_CLAIMS_TRANSACTION_DEFAULT.PIF_SYMBOL,
	EXP_CLAIMS_TRANSACTION_DEFAULT.PIF_POLICY_NUMBER,
	EXP_CLAIMS_TRANSACTION_DEFAULT.PIF_MODULE,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_INSURANCE_LINE,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_LOCATION_NUMBER,
	-- *INF*: IIF(LOSS_LOCATION_NUMBER = 0, '0', to_char(LOSS_LOCATION_NUMBER))
	IFF(LOSS_LOCATION_NUMBER = 0, '0', to_char(LOSS_LOCATION_NUMBER)) AS V_LOSS_LOCATION_NUMBER,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_SUB_LOCATION_NUMBER,
	-- *INF*: IIF(LOSS_SUB_LOCATION_NUMBER = 0, '0', to_char(LOSS_SUB_LOCATION_NUMBER))
	IFF(LOSS_SUB_LOCATION_NUMBER = 0, '0', to_char(LOSS_SUB_LOCATION_NUMBER)) AS V_LOSS_SUB_LOCATION_NUMBER,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_RISK_UNIT_GROUP,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_CLASS_CODE_GROUP,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_CLASS_CODE_MEMBER,
	-- *INF*: IIF(ISNULL(TO_CHAR(LOSS_CLASS_CODE_GROUP)||TO_CHAR(LOSS_CLASS_CODE_MEMBER)) ,'N/A',TO_CHAR(LOSS_CLASS_CODE_GROUP)||TO_CHAR(LOSS_CLASS_CODE_MEMBER))
	IFF(TO_CHAR(LOSS_CLASS_CODE_GROUP) || TO_CHAR(LOSS_CLASS_CODE_MEMBER) IS NULL, 'N/A', TO_CHAR(LOSS_CLASS_CODE_GROUP) || TO_CHAR(LOSS_CLASS_CODE_MEMBER)) AS V_risk_unit_grp_seq_num_1,
	-- *INF*: LPAD(V_risk_unit_grp_seq_num_1,3,'0')
	LPAD(V_risk_unit_grp_seq_num_1, 3, '0') AS V_risk_unit_grp_seq_num,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_UNIT,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_SEQUENCE_RISK_UNIT,
	-- *INF*: IIF(TO_DECIMAL(LOSS_SEQUENCE_RISK_UNIT) = 0, '0',
	-- to_char(TO_DECIMAL(LOSS_SEQUENCE_RISK_UNIT)) )
	IFF(TO_DECIMAL(LOSS_SEQUENCE_RISK_UNIT) = 0, '0', to_char(TO_DECIMAL(LOSS_SEQUENCE_RISK_UNIT))) AS V_LOSS_SEQUENCE_RISK_UNIT,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_TYPE_EXPOSURE,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_MAJOR_PERIL,
	LOSS_MAJOR_PERIL AS LKP_LOSS_MAJOR_PERIL,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_MAJOR_PERIL_SEQ,
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
	LOSS_ID AS SAR_ID,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_RESERVE_CATEGORY,
	'N/A' AS s3p_trans_code,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_NET_CHANGE_DOLLARS,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_TRANSACTION AS IN_LOSS_TRANSACTION,
	-- *INF*: :LKP.LKP_Sup_Convert_PMS_Claim_Transaction_Code_trans_code_id(IN_LOSS_TRANSACTION)
	LKP_SUP_CONVERT_PMS_CLAIM_TRANSACTION_CODE_TRANS_CODE_ID_IN_LOSS_TRANSACTION.sup_convert_claim_trans_code_id AS LKP_PMSTransactionCodeID,
	-- *INF*: IIF(LOSS_NET_CHANGE_DOLLARS=0.0,'39','38')
	IFF(LOSS_NET_CHANGE_DOLLARS = 0.0, '39', '38') AS TRANS_TYPE_CODE_1,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_PAID_OR_RESV_AMT AS IN_LOSS_PAID_OR_RESV_AMT,
	-- *INF*: IIF(IN(IN_LOSS_TRANSACTION, '90', '91', '92', '95', '97', '98', '99'), 0.0, IN_LOSS_PAID_OR_RESV_AMT)
	IFF(IN(IN_LOSS_TRANSACTION, '90', '91', '92', '95', '97', '98', '99'), 0.0, IN_LOSS_PAID_OR_RESV_AMT) AS LOSS_PAID_OR_RESV_AMT,
	-- *INF*: IIF(IN_LOSS_PAID_OR_RESV_AMT=0.0,'40','30')
	IFF(IN_LOSS_PAID_OR_RESV_AMT = 0.0, '40', '30') AS TRANS_TYPE_CODE_2,
	-- *INF*: IIF(IN(IN_LOSS_TRANSACTION, '91', '92'), '90', IN_LOSS_TRANSACTION) 
	-- 
	-- 
	IFF(IN(IN_LOSS_TRANSACTION, '91', '92'), '90', IN_LOSS_TRANSACTION) AS LOSS_TRANSACTION,
	-- *INF*: DECODE (IN_LOSS_TRANSACTION, '91','90',
	--  '92','90',
	--  '97','27',
	--  '95','40',
	--  '98','83',
	-- '99','84',
	-- IN_LOSS_TRANSACTION)
	-- 
	-- 
	-- ---IIF(IN(IN_LOSS_TRANSACTION, '91', '92'), '90', IN_LOSS_TRANSACTION) 
	-- 
	-- 
	DECODE(IN_LOSS_TRANSACTION,
	'91', '90',
	'92', '90',
	'97', '27',
	'95', '40',
	'98', '83',
	'99', '84',
	IN_LOSS_TRANSACTION) AS LOSS_TRANSACTION_83_84_40_27,
	LKP_Sup_Convert_Pms_Claim_Transaction_Code.edw_financial_type_code AS FINANCIAL_TYPE_CODE,
	LKP_Sup_Convert_Pms_Claim_Transaction_Code.edw_trans_code AS IN_TRANS_TYPE_CODE,
	-- *INF*: DECODE(TRUE,
	-- IN_LOSS_TRANSACTION='76',TRANS_TYPE_CODE_1,
	-- IN_LOSS_TRANSACTION='26',TRANS_TYPE_CODE_1,
	-- IN_LOSS_TRANSACTION='27',TRANS_TYPE_CODE_2,
	-- IN_LOSS_TRANSACTION='83',TRANS_TYPE_CODE_2,
	-- IN_LOSS_TRANSACTION='88',TRANS_TYPE_CODE_1,
	-- IN_LOSS_TRANSACTION='84',TRANS_TYPE_CODE_2,
	-- IN_LOSS_TRANSACTION='89',TRANS_TYPE_CODE_1,IN_TRANS_TYPE_CODE)
	DECODE(TRUE,
	IN_LOSS_TRANSACTION = '76', TRANS_TYPE_CODE_1,
	IN_LOSS_TRANSACTION = '26', TRANS_TYPE_CODE_1,
	IN_LOSS_TRANSACTION = '27', TRANS_TYPE_CODE_2,
	IN_LOSS_TRANSACTION = '83', TRANS_TYPE_CODE_2,
	IN_LOSS_TRANSACTION = '88', TRANS_TYPE_CODE_1,
	IN_LOSS_TRANSACTION = '84', TRANS_TYPE_CODE_2,
	IN_LOSS_TRANSACTION = '89', TRANS_TYPE_CODE_1,
	IN_TRANS_TYPE_CODE) AS V_TRANS_TYPE_CODE,
	V_TRANS_TYPE_CODE AS TRANS_TYPE_CODE_OP,
	-- *INF*: :LKP.LKP_SUP_CLAIM_TRANSACTION_CODE(V_TRANS_TYPE_CODE)
	LKP_SUP_CLAIM_TRANSACTION_CODE_V_TRANS_TYPE_CODE.sup_claim_trans_code_id AS LKP_TransactionCodeID,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_ENTRY_OPERATOR,
	LKP_Sup_Convert_Pms_Claim_Transaction_Code.edw_trans_ctgry_code AS LKP_trans_ctgry_code,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_TRANSACTION_CATEGORY,
	-- *INF*: IIF(LOSS_TRANSACTION_CATEGORY='WD' OR LOSS_TRANSACTION_CATEGORY='DR',LOSS_TRANSACTION_CATEGORY,IIF(ISNULL(LKP_trans_ctgry_code),'N/A',LKP_trans_ctgry_code))
	IFF(LOSS_TRANSACTION_CATEGORY = 'WD' OR LOSS_TRANSACTION_CATEGORY = 'DR', LOSS_TRANSACTION_CATEGORY, IFF(LKP_trans_ctgry_code IS NULL, 'N/A', LKP_trans_ctgry_code)) AS V_LOSS_TRANSACTION_CATEGORY,
	V_LOSS_TRANSACTION_CATEGORY AS OP_LOSS_TRANSACTION_CATEGORY,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_CAUSE,
	-- *INF*: IIF(LOSS_MAJOR_PERIL = '032'  AND LOSS_CAUSE = '07','06',LOSS_CAUSE)
	IFF(LOSS_MAJOR_PERIL = '032' AND LOSS_CAUSE = '07', '06', LOSS_CAUSE) AS V_LOSS_CAUSE,
	V_LOSS_CAUSE AS OP_LOSS_CAUSE,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_DRAFT_NO,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_TRANSACTION_DATE,
	-- *INF*: TO_DATE(LOSS_TRANSACTION_DATE,'YYYYMMDD')
	TO_DATE(LOSS_TRANSACTION_DATE, 'YYYYMMDD') AS V_LOSS_TRANSACTION_DATE,
	V_LOSS_TRANSACTION_DATE AS LOSS_TRANSACTION_DATE_OP,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS S3P_UPDATED_DATE_OP,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS S3P_TO_PMS_TRANS_DATE_OP,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_ACCOUNT_ENTERED_DATE,
	-- *INF*: TO_DATE(LOSS_ACCOUNT_ENTERED_DATE,'YYYYMM')
	TO_DATE(LOSS_ACCOUNT_ENTERED_DATE, 'YYYYMM') AS LOSS_ACCOUNT_ENTERED_DATE_OP,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_BASE_TRANSACTION,
	LOSS_BASE_TRANSACTION AS TRANS_BASE_TYPE_CODE_OP,
	'N/A' AS TRANS_RSN_OP,
	'N/A' AS SINGLE_CHECK_IND_OP,
	'N/A' AS OFFSET_REISSUE_IND,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_TYPE_DISABILITY,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_OFFSET_ONSET_IND,
	-- *INF*: :LKP.LKP_CLAIM_OCCURRENCE(V_LOSS_OCCURENCE_KEY)
	LKP_CLAIM_OCCURRENCE_V_LOSS_OCCURENCE_KEY.claim_occurrence_ak_id AS V_CLAIM_OCCURENCE_AK_ID,
	V_CLAIM_OCCURENCE_AK_ID AS o_CLAIM_OCCURENCE_AK_ID,
	-- *INF*: :LKP.LKP_CLAIM_PARTY(V_LOSS_PARTY_KEY)
	LKP_CLAIM_PARTY_V_LOSS_PARTY_KEY.claim_party_ak_id AS V_CLAIM_PARTY_AK_ID,
	-- *INF*: :LKP.LKP_CLAIM_PARTY_OCCURRENCE(V_CLAIM_OCCURENCE_AK_ID,V_CLAIM_PARTY_AK_ID,V_PARTY_ROLE_CODE)
	LKP_CLAIM_PARTY_OCCURRENCE_V_CLAIM_OCCURENCE_AK_ID_V_CLAIM_PARTY_AK_ID_V_PARTY_ROLE_CODE.claim_party_occurrence_ak_id AS V_CLAIM_PARTY_OCCURENCE_AK_ID,
	-- *INF*: :LKP.LKP_CLAIMANT_DETAIL_COVERAGE(V_CLAIM_PARTY_OCCURENCE_AK_ID, V_LOSS_LOCATION_NUMBER, 
	-- V_LOSS_SUB_LOCATION_NUMBER,  LOSS_INSURANCE_LINE, LOSS_RISK_UNIT_GROUP, V_risk_unit_grp_seq_num ,
	-- LOSS_UNIT, V_LOSS_SEQUENCE_RISK_UNIT, LOSS_MAJOR_PERIL, LOSS_MAJOR_PERIL_SEQ, LOSS_DISABILITY, 
	-- LOSS_RESERVE_CATEGORY, V_LOSS_CAUSE, LOSS_MEMBER, LOSS_TYPE_EXPOSURE)
	LKP_CLAIMANT_DETAIL_COVERAGE_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_risk_unit_grp_seq_num_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_V_LOSS_CAUSE_LOSS_MEMBER_LOSS_TYPE_EXPOSURE.claimant_cov_det_ak_id AS V_CLAIMANT_COV_DET_AK_ID,
	-- *INF*: IIF(isnull(V_CLAIMANT_COV_DET_AK_ID),-1,V_CLAIMANT_COV_DET_AK_ID)
	IFF(V_CLAIMANT_COV_DET_AK_ID IS NULL, - 1, V_CLAIMANT_COV_DET_AK_ID) AS CLAIMANT_COV_DET_AK_ID_OP,
	LKP_42x6_Stage_Process_Date.ipfcx6_year_process,
	LKP_42x6_Stage_Process_Date.ipfcx6_month_process,
	-- *INF*: IIF(LENGTH(to_char(ipfcx6_month_process)) = 1, '0'||to_char(ipfcx6_month_process), to_char(ipfcx6_month_process))
	IFF(LENGTH(to_char(ipfcx6_month_process)) = 1, '0' || to_char(ipfcx6_month_process), to_char(ipfcx6_month_process)) AS v_ipfcx6_month_process,
	LKP_42x6_Stage_Process_Date.ipfcx6_day_process,
	-- *INF*: IIF(LENGTH(to_char(ipfcx6_day_process)) = 1, '0'||to_char(ipfcx6_day_process), to_char(ipfcx6_day_process))
	IFF(LENGTH(to_char(ipfcx6_day_process)) = 1, '0' || to_char(ipfcx6_day_process), to_char(ipfcx6_day_process)) AS v_ipfcx6_day_process,
	-- *INF*: v_ipfcx6_month_process||v_ipfcx6_day_process||to_char(ipfcx6_year_process)
	v_ipfcx6_month_process || v_ipfcx6_day_process || to_char(ipfcx6_year_process) AS v_reprocess_date,
	-- *INF*: IIF(v_reprocess_date='00000',TO_DATE('01011800', 'MMDDYYYY'),
	--  to_date(v_reprocess_date, 'MMDDYYYY'))
	-- 
	-- --TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	IFF(v_reprocess_date = '00000', TO_DATE('01011800', 'MMDDYYYY'), to_date(v_reprocess_date, 'MMDDYYYY')) AS v_reprocess_date_out,
	-- *INF*: IIF(LOSS_OFFSET_ONSET_IND = 'N/A' OR ISNULL(v_reprocess_date_out),  TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'), v_reprocess_date_out)
	IFF(LOSS_OFFSET_ONSET_IND = 'N/A' OR v_reprocess_date_out IS NULL, TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), v_reprocess_date_out) AS reprocess_date_out,
	EXP_CLAIMS_TRANSACTION_DEFAULT.inf_action,
	EXP_CLAIMS_TRANSACTION_DEFAULT.SOURCE_SYSTEM_ID,
	EXP_CLAIMS_TRANSACTION_DEFAULT.crrnt_snpsht_flag,
	EXP_CLAIMS_TRANSACTION_DEFAULT.audit_id,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS eff_from_dt,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_dt,
	sysdate AS created_date,
	sysdate AS modified_date,
	EXP_CLAIMS_TRANSACTION_DEFAULT.IN_LOSS_DRAFT_AMOUNT AS LOSS_DRAFT_AMOUNT,
	-- *INF*: IIF(ISNULL(LOSS_DRAFT_AMOUNT), 0, LOSS_DRAFT_AMOUNT)
	IFF(LOSS_DRAFT_AMOUNT IS NULL, 0, LOSS_DRAFT_AMOUNT) AS v_LOSS_DRAFT_AMOUNT,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_TRANSACTION_DATE_payment,
	EXP_CLAIMS_TRANSACTION_DEFAULT.pay_to_code,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_DRAFT_MAIL_TO,
	-- *INF*: :LKP.LKP_CLAIM_PAYMENT(V_CLAIMANT_COV_DET_AK_ID,LTRIM(RTRIM(LOSS_DRAFT_NO)), v_LOSS_DRAFT_AMOUNT, LOSS_TRANSACTION_DATE_payment )
	LKP_CLAIM_PAYMENT_V_CLAIMANT_COV_DET_AK_ID_LTRIM_RTRIM_LOSS_DRAFT_NO_v_LOSS_DRAFT_AMOUNT_LOSS_TRANSACTION_DATE_payment.claim_pay_ak_id AS v_claim_pay_ak_id,
	-- *INF*: IIF(ISNULL(v_claim_pay_ak_id), -1, v_claim_pay_ak_id)
	IFF(v_claim_pay_ak_id IS NULL, - 1, v_claim_pay_ak_id) AS claim_pay_ak_id,
	0 AS err_flag,
	LKP_Claim_Master_1099_List.claim_master_1099_list_ak_id AS LKP_claim_master_1099_list_ak_id,
	LKP_Claim_Master_1099_List.adnm_tax_id AS LKP_adnm_taxid_ssn,
	-- *INF*: IIF(ISNULL(LKP_claim_master_1099_list_ak_id),-1,LKP_claim_master_1099_list_ak_id)
	IFF(LKP_claim_master_1099_list_ak_id IS NULL, - 1, LKP_claim_master_1099_list_ak_id) AS v_claim_master_1099_list_ak_id,
	-- *INF*: iif(isnull(LKP_adnm_taxid_ssn)  OR length(LKP_adnm_taxid_ssn)= 0 OR IS_SPACES(LKP_adnm_taxid_ssn),'000000000',ltrim(rtrim(LKP_adnm_taxid_ssn)) )
	IFF(LKP_adnm_taxid_ssn IS NULL OR length(LKP_adnm_taxid_ssn) = 0 OR IS_SPACES(LKP_adnm_taxid_ssn), '000000000', ltrim(rtrim(LKP_adnm_taxid_ssn))) AS V_LKP_adnm_taxid_ssn,
	-- *INF*: LTRIM(RTRIM(V_LKP_adnm_taxid_ssn))
	LTRIM(RTRIM(V_LKP_adnm_taxid_ssn)) AS V_TRIM_LKP_TAXID_SSN,
	-- *INF*: IIF(V_TRIM_LKP_TAXID_SSN != '000000000', IIF(SUBSTR(V_TRIM_LKP_TAXID_SSN,3,1) = '-' AND INSTR(V_TRIM_LKP_TAXID_SSN,' ')=0 AND LENGTH(V_TRIM_LKP_TAXID_SSN)=10,V_TRIM_LKP_TAXID_SSN,IIF(SUBSTR(V_TRIM_LKP_TAXID_SSN,4,1)='-' AND SUBSTR(V_TRIM_LKP_TAXID_SSN,7,1)='-' AND INSTR(V_TRIM_LKP_TAXID_SSN,' ')=0 AND LENGTH(V_TRIM_LKP_TAXID_SSN)=11,V_TRIM_LKP_TAXID_SSN,'000000000')),'000000000')
	IFF(V_TRIM_LKP_TAXID_SSN != '000000000', IFF(SUBSTR(V_TRIM_LKP_TAXID_SSN, 3, 1) = '-' AND INSTR(V_TRIM_LKP_TAXID_SSN, ' ') = 0 AND LENGTH(V_TRIM_LKP_TAXID_SSN) = 10, V_TRIM_LKP_TAXID_SSN, IFF(SUBSTR(V_TRIM_LKP_TAXID_SSN, 4, 1) = '-' AND SUBSTR(V_TRIM_LKP_TAXID_SSN, 7, 1) = '-' AND INSTR(V_TRIM_LKP_TAXID_SSN, ' ') = 0 AND LENGTH(V_TRIM_LKP_TAXID_SSN) = 11, V_TRIM_LKP_TAXID_SSN, '000000000')), '000000000') AS V_ADNM_TAXID_SSN,
	V_ADNM_TAXID_SSN AS tax_id,
	v_claim_master_1099_list_ak_id AS claim_master_1099_list_ak_id,
	'N/A' AS trans_offset_onset_ind,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS s3p_created_date,
	PIF_SYMBOL || PIF_POLICY_NUMBER || PIF_MODULE AS o_pol_key
	FROM EXP_CLAIMS_TRANSACTION_DEFAULT
	LEFT JOIN LKP_42x6_Stage_Process_Date
	ON LKP_42x6_Stage_Process_Date.pif_symbol = EXP_CLAIMS_TRANSACTION_DEFAULT.PIF_SYMBOL AND LKP_42x6_Stage_Process_Date.pif_policy_number = EXP_CLAIMS_TRANSACTION_DEFAULT.PIF_POLICY_NUMBER AND LKP_42x6_Stage_Process_Date.pif_module = EXP_CLAIMS_TRANSACTION_DEFAULT.PIF_MODULE AND LKP_42x6_Stage_Process_Date.ipfcx6_year_of_loss = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_YEAR AND LKP_42x6_Stage_Process_Date.ipfcx6_month_of_loss = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_MONTH AND LKP_42x6_Stage_Process_Date.ipfcx6_day_of_loss = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_DAY AND LKP_42x6_Stage_Process_Date.ipfcx6_loss_occ_fdigit = EXP_CLAIMS_TRANSACTION_DEFAULT.ipfcx6_loss_occurence_fdigit AND LKP_42x6_Stage_Process_Date.ipfcx6_usr_loss_occurence = EXP_CLAIMS_TRANSACTION_DEFAULT.ipfcx6_usr_loss_occurence AND LKP_42x6_Stage_Process_Date.ipfcx6_loss_claimant = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_CLAIMANT AND LKP_42x6_Stage_Process_Date.ipfcx6_insurance_line = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_INSURANCE_LINE AND LKP_42x6_Stage_Process_Date.ipfcx6_location_number = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_LOCATION_NUMBER AND LKP_42x6_Stage_Process_Date.ipfcx6_sub_location_number = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_SUB_LOCATION_NUMBER AND LKP_42x6_Stage_Process_Date.ipfcx6_risk_unit_group = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_RISK_UNIT_GROUP_lkp AND LKP_42x6_Stage_Process_Date.ipfcx6_class_code_group = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_CLASS_CODE_GROUP_lkp AND LKP_42x6_Stage_Process_Date.ipfcx6_class_code_member = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_CLASS_CODE_MEMBER_lkp AND LKP_42x6_Stage_Process_Date.ipfcx6_loss_unit = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_UNIT_lkp AND LKP_42x6_Stage_Process_Date.ipfcx6_risk_sequence = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_SEQUENCE_RISK_UNIT_lkp AND LKP_42x6_Stage_Process_Date.ipfcx6_major_peril = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_MAJOR_PERIL_lkp AND LKP_42x6_Stage_Process_Date.ipfcx6_sequence_type_exposure = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_MAJOR_PERIL_SEQ_lkp AND LKP_42x6_Stage_Process_Date.ipfcx6_loss_disability = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_DISABILITY_lkp AND LKP_42x6_Stage_Process_Date.ipfcx6_member = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_MEMBER_lkp AND LKP_42x6_Stage_Process_Date.ipfcx6_reserve_category = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_RESERVE_CATEGORY_lkp AND LKP_42x6_Stage_Process_Date.ipfcx6_loss_cause = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_CAUSE_lkp
	LEFT JOIN LKP_Claim_Master_1099_List
	ON LKP_Claim_Master_1099_List.tax_id = LKP_PMS_ADJUSTOR_MASTER_STAGE.adnm_taxid_ssn
	LEFT JOIN LKP_Sup_Convert_Pms_Claim_Transaction_Code
	ON LKP_Sup_Convert_Pms_Claim_Transaction_Code.pms_trans_code = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_TRANSACTION
	LEFT JOIN LKP_SUP_CONVERT_PMS_CLAIM_TRANSACTION_CODE_TRANS_CODE_ID LKP_SUP_CONVERT_PMS_CLAIM_TRANSACTION_CODE_TRANS_CODE_ID_IN_LOSS_TRANSACTION
	ON LKP_SUP_CONVERT_PMS_CLAIM_TRANSACTION_CODE_TRANS_CODE_ID_IN_LOSS_TRANSACTION.pms_trans_code = IN_LOSS_TRANSACTION

	LEFT JOIN LKP_SUP_CLAIM_TRANSACTION_CODE LKP_SUP_CLAIM_TRANSACTION_CODE_V_TRANS_TYPE_CODE
	ON LKP_SUP_CLAIM_TRANSACTION_CODE_V_TRANS_TYPE_CODE.trans_code = V_TRANS_TYPE_CODE

	LEFT JOIN LKP_CLAIM_OCCURRENCE LKP_CLAIM_OCCURRENCE_V_LOSS_OCCURENCE_KEY
	ON LKP_CLAIM_OCCURRENCE_V_LOSS_OCCURENCE_KEY.claim_occurrence_key = V_LOSS_OCCURENCE_KEY

	LEFT JOIN LKP_CLAIM_PARTY LKP_CLAIM_PARTY_V_LOSS_PARTY_KEY
	ON LKP_CLAIM_PARTY_V_LOSS_PARTY_KEY.claim_party_key = V_LOSS_PARTY_KEY

	LEFT JOIN LKP_CLAIM_PARTY_OCCURRENCE LKP_CLAIM_PARTY_OCCURRENCE_V_CLAIM_OCCURENCE_AK_ID_V_CLAIM_PARTY_AK_ID_V_PARTY_ROLE_CODE
	ON LKP_CLAIM_PARTY_OCCURRENCE_V_CLAIM_OCCURENCE_AK_ID_V_CLAIM_PARTY_AK_ID_V_PARTY_ROLE_CODE.claim_occurrence_ak_id = V_CLAIM_OCCURENCE_AK_ID
	AND LKP_CLAIM_PARTY_OCCURRENCE_V_CLAIM_OCCURENCE_AK_ID_V_CLAIM_PARTY_AK_ID_V_PARTY_ROLE_CODE.claim_party_ak_id = V_CLAIM_PARTY_AK_ID
	AND LKP_CLAIM_PARTY_OCCURRENCE_V_CLAIM_OCCURENCE_AK_ID_V_CLAIM_PARTY_AK_ID_V_PARTY_ROLE_CODE.claim_party_role_code = V_PARTY_ROLE_CODE

	LEFT JOIN LKP_CLAIMANT_DETAIL_COVERAGE LKP_CLAIMANT_DETAIL_COVERAGE_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_risk_unit_grp_seq_num_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_V_LOSS_CAUSE_LOSS_MEMBER_LOSS_TYPE_EXPOSURE
	ON LKP_CLAIMANT_DETAIL_COVERAGE_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_risk_unit_grp_seq_num_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_V_LOSS_CAUSE_LOSS_MEMBER_LOSS_TYPE_EXPOSURE.claim_party_occurrence_ak_id = V_CLAIM_PARTY_OCCURENCE_AK_ID
	AND LKP_CLAIMANT_DETAIL_COVERAGE_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_risk_unit_grp_seq_num_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_V_LOSS_CAUSE_LOSS_MEMBER_LOSS_TYPE_EXPOSURE.loc_unit_num = V_LOSS_LOCATION_NUMBER
	AND LKP_CLAIMANT_DETAIL_COVERAGE_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_risk_unit_grp_seq_num_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_V_LOSS_CAUSE_LOSS_MEMBER_LOSS_TYPE_EXPOSURE.sub_loc_unit_num = V_LOSS_SUB_LOCATION_NUMBER
	AND LKP_CLAIMANT_DETAIL_COVERAGE_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_risk_unit_grp_seq_num_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_V_LOSS_CAUSE_LOSS_MEMBER_LOSS_TYPE_EXPOSURE.ins_line = LOSS_INSURANCE_LINE
	AND LKP_CLAIMANT_DETAIL_COVERAGE_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_risk_unit_grp_seq_num_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_V_LOSS_CAUSE_LOSS_MEMBER_LOSS_TYPE_EXPOSURE.risk_unit_grp = LOSS_RISK_UNIT_GROUP
	AND LKP_CLAIMANT_DETAIL_COVERAGE_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_risk_unit_grp_seq_num_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_V_LOSS_CAUSE_LOSS_MEMBER_LOSS_TYPE_EXPOSURE.risk_unit_grp_seq_num = V_risk_unit_grp_seq_num
	AND LKP_CLAIMANT_DETAIL_COVERAGE_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_risk_unit_grp_seq_num_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_V_LOSS_CAUSE_LOSS_MEMBER_LOSS_TYPE_EXPOSURE.risk_unit = LOSS_UNIT
	AND LKP_CLAIMANT_DETAIL_COVERAGE_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_risk_unit_grp_seq_num_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_V_LOSS_CAUSE_LOSS_MEMBER_LOSS_TYPE_EXPOSURE.risk_unit_seq_num = V_LOSS_SEQUENCE_RISK_UNIT
	AND LKP_CLAIMANT_DETAIL_COVERAGE_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_risk_unit_grp_seq_num_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_V_LOSS_CAUSE_LOSS_MEMBER_LOSS_TYPE_EXPOSURE.major_peril_code = LOSS_MAJOR_PERIL
	AND LKP_CLAIMANT_DETAIL_COVERAGE_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_risk_unit_grp_seq_num_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_V_LOSS_CAUSE_LOSS_MEMBER_LOSS_TYPE_EXPOSURE.major_peril_seq = LOSS_MAJOR_PERIL_SEQ
	AND LKP_CLAIMANT_DETAIL_COVERAGE_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_risk_unit_grp_seq_num_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_V_LOSS_CAUSE_LOSS_MEMBER_LOSS_TYPE_EXPOSURE.pms_loss_disability = LOSS_DISABILITY
	AND LKP_CLAIMANT_DETAIL_COVERAGE_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_risk_unit_grp_seq_num_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_V_LOSS_CAUSE_LOSS_MEMBER_LOSS_TYPE_EXPOSURE.reserve_ctgry = LOSS_RESERVE_CATEGORY
	AND LKP_CLAIMANT_DETAIL_COVERAGE_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_risk_unit_grp_seq_num_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_V_LOSS_CAUSE_LOSS_MEMBER_LOSS_TYPE_EXPOSURE.cause_of_loss = V_LOSS_CAUSE
	AND LKP_CLAIMANT_DETAIL_COVERAGE_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_risk_unit_grp_seq_num_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_V_LOSS_CAUSE_LOSS_MEMBER_LOSS_TYPE_EXPOSURE.pms_mbr = LOSS_MEMBER
	AND LKP_CLAIMANT_DETAIL_COVERAGE_V_CLAIM_PARTY_OCCURENCE_AK_ID_V_LOSS_LOCATION_NUMBER_V_LOSS_SUB_LOCATION_NUMBER_LOSS_INSURANCE_LINE_LOSS_RISK_UNIT_GROUP_V_risk_unit_grp_seq_num_LOSS_UNIT_V_LOSS_SEQUENCE_RISK_UNIT_LOSS_MAJOR_PERIL_LOSS_MAJOR_PERIL_SEQ_LOSS_DISABILITY_LOSS_RESERVE_CATEGORY_V_LOSS_CAUSE_LOSS_MEMBER_LOSS_TYPE_EXPOSURE.pms_type_exposure = LOSS_TYPE_EXPOSURE

	LEFT JOIN LKP_CLAIM_PAYMENT LKP_CLAIM_PAYMENT_V_CLAIMANT_COV_DET_AK_ID_LTRIM_RTRIM_LOSS_DRAFT_NO_v_LOSS_DRAFT_AMOUNT_LOSS_TRANSACTION_DATE_payment
	ON LKP_CLAIM_PAYMENT_V_CLAIMANT_COV_DET_AK_ID_LTRIM_RTRIM_LOSS_DRAFT_NO_v_LOSS_DRAFT_AMOUNT_LOSS_TRANSACTION_DATE_payment.pms_claimant_cov_det_ak_id = V_CLAIMANT_COV_DET_AK_ID
	AND LKP_CLAIM_PAYMENT_V_CLAIMANT_COV_DET_AK_ID_LTRIM_RTRIM_LOSS_DRAFT_NO_v_LOSS_DRAFT_AMOUNT_LOSS_TRANSACTION_DATE_payment.micro_ecd_draft_num = LTRIM(RTRIM(LOSS_DRAFT_NO))
	AND LKP_CLAIM_PAYMENT_V_CLAIMANT_COV_DET_AK_ID_LTRIM_RTRIM_LOSS_DRAFT_NO_v_LOSS_DRAFT_AMOUNT_LOSS_TRANSACTION_DATE_payment.total_pay_amt = v_LOSS_DRAFT_AMOUNT
	AND LKP_CLAIM_PAYMENT_V_CLAIMANT_COV_DET_AK_ID_LTRIM_RTRIM_LOSS_DRAFT_NO_v_LOSS_DRAFT_AMOUNT_LOSS_TRANSACTION_DATE_payment.pay_issued_date = LOSS_TRANSACTION_DATE_payment

),
LKP_CLAIM_TRANSACTION AS (
	SELECT
	cause_of_loss,
	claim_trans_id,
	type_disability,
	sar_id,
	pms_acct_entered_date,
	trans_hist_amt,
	trans_entry_oper_id,
	claimant_cov_det_ak_id,
	reserve_ctgry,
	pms_trans_code,
	trans_date,
	trans_ctgry_code,
	trans_amt,
	draft_num
	FROM (
		SELECT 
		CT.claim_trans_id as claim_trans_id, 
		CT.type_disability as type_disability, 
		CT.sar_id as sar_id, 
		CT.pms_acct_entered_date as pms_acct_entered_date, 
		CT.trans_hist_amt as trans_hist_amt, 
		CT.trans_entry_oper_id as trans_entry_oper_id, 
		CT.claimant_cov_det_ak_id as claimant_cov_det_ak_id, 
		CT.cause_of_loss as cause_of_loss, 
		CT.reserve_ctgry as reserve_ctgry, 
		CT.offset_onset_ind as offset_onset_ind, 
		CASE CT.pms_trans_code 
		WHEN 91 THEN 90 
		WHEN 92 THEN 90 
		ELSE CT.pms_trans_code END as pms_trans_code,
		CT.trans_date as trans_date, 
		CT.trans_ctgry_code as trans_ctgry_code, 
		CASE CT.pms_trans_code 
		WHEN 90 THEN 0.0 
		WHEN 91 THEN 0.0 
		WHEN 92 THEN 0.0 
		WHEN 95 THEN 0.0
		WHEN 97 THEN 0.0
		WHEN 98 THEN 0.0
		WHEN 99 THEN 0.0
		ELSE CT.trans_amt END as trans_amt,
		CT.draft_num as draft_num 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_transaction CT, 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_detail CCD
		WHERE 
		 CT.claimant_cov_det_ak_id = CCD.claimant_cov_det_ak_id
		AND CT.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		AND CCD.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		AND CCD.crrnt_snpsht_flag = 1
		AND CT.crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,cause_of_loss,reserve_ctgry,pms_trans_code,trans_date,trans_ctgry_code,trans_amt,draft_num ORDER BY cause_of_loss) = 1
),
LKP_Claim_Transaction_83_84_27_40 AS (
	SELECT
	claim_trans_id,
	type_disability,
	sar_id,
	pms_acct_entered_date,
	trans_hist_amt,
	trans_entry_oper_id,
	claimant_cov_det_ak_id,
	cause_of_loss,
	reserve_ctgry,
	pms_trans_code,
	trans_ctgry_code
	FROM (
		SELECT claim_transaction.claim_trans_id as claim_trans_id, 
		claim_transaction.type_disability as type_disability, 
		claim_transaction.sar_id as sar_id, 
		claim_transaction.pms_acct_entered_date as pms_acct_entered_date, 
		claim_transaction.trans_entry_oper_id as trans_entry_oper_id, 
		claim_transaction.claimant_cov_det_ak_id as claimant_cov_det_ak_id, 
		claim_transaction.cause_of_loss as cause_of_loss, 
		claim_transaction.reserve_ctgry as reserve_ctgry, 
		---claim_transaction.offset_onset_ind as offset_onset_ind, 
		CASE claim_transaction.pms_trans_code 
		WHEN 95 THEN 40
		WHEN 97 THEN 27
		WHEN 98 THEN 83
		WHEN 99 THEN 84
		ELSE claim_transaction.pms_trans_code END AS pms_trans_code,
		claim_transaction.trans_ctgry_code as trans_ctgry_code, 
		claim_transaction.trans_hist_amt as trans_hist_amt 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_transaction
		WHERE SOURCE_SYS_ID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		
		----AND claim_transaction.pms_trans_code  in ('99','98','97','95')
		 -----('84','83','27','40','99','98','97','95')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,cause_of_loss,reserve_ctgry,pms_trans_code,trans_ctgry_code,trans_hist_amt ORDER BY claim_trans_id DESC) = 1
),
LKP_V2_Policy AS (
	SELECT
	pms_pol_lob_code,
	pol_key
	FROM (
		SELECT 
			pms_pol_lob_code,
			pol_key
		FROM V2.policy
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pms_pol_lob_code) = 1
),
LKP_Sup_CauseOfLoss AS (
	SELECT
	CauseOfLossId,
	LineOfBusiness,
	MajorPeril,
	CauseOfLoss
	FROM (
		SELECT 
			CauseOfLossId,
			LineOfBusiness,
			MajorPeril,
			CauseOfLoss
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_CauseOfLoss
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY LineOfBusiness,MajorPeril,CauseOfLoss ORDER BY CauseOfLossId) = 1
),
LKP_Sup_Claim_Financial_Code AS (
	SELECT
	sup_claim_financial_code_id,
	financial_code
	FROM (
		SELECT 
			sup_claim_financial_code_id,
			financial_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_financial_code
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY financial_code ORDER BY sup_claim_financial_code_id) = 1
),
LKP_Sup_Claim_Reserve_Category AS (
	SELECT
	sup_claim_reserve_ctgry_id,
	reserve_ctgry_code
	FROM (
		SELECT 
			sup_claim_reserve_ctgry_id,
			reserve_ctgry_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_reserve_category
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY reserve_ctgry_code ORDER BY sup_claim_reserve_ctgry_id) = 1
),
LKP_Sup_Claim_Transaction_Category AS (
	SELECT
	sup_claim_trans_catetory_id,
	trans_ctgry_code
	FROM (
		SELECT 
			sup_claim_trans_catetory_id,
			trans_ctgry_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_transaction_category
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY trans_ctgry_code ORDER BY sup_claim_trans_catetory_id) = 1
),
EXP_Evaluate AS (
	SELECT
	LKP_CLAIM_TRANSACTION.claim_trans_id AS LKP_claim_trans_id,
	LKP_CLAIM_TRANSACTION.type_disability AS LKP_type_disability,
	LKP_CLAIM_TRANSACTION.sar_id AS LKP_sar_id,
	LKP_CLAIM_TRANSACTION.pms_acct_entered_date AS LKP_pms_acct_entered_date,
	LKP_CLAIM_TRANSACTION.trans_hist_amt AS LKP_trans_hist_amt,
	LKP_CLAIM_TRANSACTION.trans_entry_oper_id AS LKP_trans_entry_oper_id,
	EXP_Claim_Transaction_Detect_Changes.inf_action,
	LKP_Claim_Transaction_83_84_27_40.claim_trans_id AS claim_trans_id_lkp_83_84_27_40,
	LKP_Claim_Transaction_83_84_27_40.type_disability AS type_disability_lkp,
	LKP_Claim_Transaction_83_84_27_40.sar_id AS sar_id_lkp,
	LKP_Claim_Transaction_83_84_27_40.pms_acct_entered_date AS pms_acct_entered_date_lkp,
	LKP_Claim_Transaction_83_84_27_40.trans_hist_amt AS trans_hist_amt_lkp,
	LKP_Claim_Transaction_83_84_27_40.trans_entry_oper_id AS trans_entry_oper_id_lkp,
	EXP_Claim_Transaction_Detect_Changes.TRANS_BASE_TYPE_CODE_OP,
	-- *INF*: IIF(IN(LOSS_TRANSACTION,'27','40','83','84') AND inf_action = '4' ,
	-- claim_trans_id_lkp_83_84_27_40,LKP_claim_trans_id)
	IFF(IN(LOSS_TRANSACTION, '27', '40', '83', '84') AND inf_action = '4', claim_trans_id_lkp_83_84_27_40, LKP_claim_trans_id) AS V_Claim_Trans_ID,
	V_Claim_Trans_ID AS Claim_Trans_ID_Out,
	EXP_Claim_Transaction_Detect_Changes.PIF_4578_stage_id,
	EXP_Claim_Transaction_Detect_Changes.SAR_ID AS SAR_ID1,
	EXP_Claim_Transaction_Detect_Changes.LOSS_RESERVE_CATEGORY,
	EXP_Claim_Transaction_Detect_Changes.s3p_trans_code,
	EXP_Claim_Transaction_Detect_Changes.LOSS_NET_CHANGE_DOLLARS,
	EXP_Claim_Transaction_Detect_Changes.IN_LOSS_PAID_OR_RESV_AMT,
	EXP_Claim_Transaction_Detect_Changes.IN_LOSS_TRANSACTION AS LOSS_TRANSACTION,
	EXP_Claim_Transaction_Detect_Changes.FINANCIAL_TYPE_CODE,
	EXP_Claim_Transaction_Detect_Changes.TRANS_TYPE_CODE_OP,
	EXP_Claim_Transaction_Detect_Changes.LOSS_ENTRY_OPERATOR,
	EXP_Claim_Transaction_Detect_Changes.OP_LOSS_TRANSACTION_CATEGORY,
	EXP_Claim_Transaction_Detect_Changes.OP_LOSS_CAUSE AS LOSS_CAUSE,
	EXP_Claim_Transaction_Detect_Changes.LOSS_DRAFT_NO,
	EXP_Claim_Transaction_Detect_Changes.LOSS_TRANSACTION_DATE_OP,
	EXP_Claim_Transaction_Detect_Changes.S3P_UPDATED_DATE_OP,
	EXP_Claim_Transaction_Detect_Changes.S3P_TO_PMS_TRANS_DATE_OP,
	EXP_Claim_Transaction_Detect_Changes.LOSS_ACCOUNT_ENTERED_DATE_OP,
	EXP_Claim_Transaction_Detect_Changes.TRANS_RSN_OP,
	EXP_Claim_Transaction_Detect_Changes.SINGLE_CHECK_IND_OP,
	EXP_Claim_Transaction_Detect_Changes.OFFSET_REISSUE_IND,
	EXP_Claim_Transaction_Detect_Changes.LOSS_TYPE_DISABILITY,
	EXP_Claim_Transaction_Detect_Changes.LOSS_OFFSET_ONSET_IND,
	EXP_Claim_Transaction_Detect_Changes.CLAIMANT_COV_DET_AK_ID_OP,
	-- *INF*: IIF(ISNULL(CLAIMANT_COV_DET_AK_ID_OP),-1,CLAIMANT_COV_DET_AK_ID_OP)
	IFF(CLAIMANT_COV_DET_AK_ID_OP IS NULL, - 1, CLAIMANT_COV_DET_AK_ID_OP) AS o_CLAIMANT_COV_DET_AK_ID_OP,
	EXP_Claim_Transaction_Detect_Changes.reprocess_date_out,
	EXP_Claim_Transaction_Detect_Changes.SOURCE_SYSTEM_ID,
	EXP_Claim_Transaction_Detect_Changes.crrnt_snpsht_flag,
	EXP_Claim_Transaction_Detect_Changes.audit_id,
	EXP_Claim_Transaction_Detect_Changes.eff_from_dt,
	EXP_Claim_Transaction_Detect_Changes.eff_to_dt,
	EXP_Claim_Transaction_Detect_Changes.created_date,
	EXP_Claim_Transaction_Detect_Changes.modified_date,
	EXP_Claim_Transaction_Detect_Changes.claim_pay_ak_id,
	EXP_Claim_Transaction_Detect_Changes.err_flag,
	EXP_Claim_Transaction_Detect_Changes.tax_id,
	EXP_Claim_Transaction_Detect_Changes.claim_master_1099_list_ak_id,
	EXP_Claim_Transaction_Detect_Changes.trans_offset_onset_ind,
	EXP_Claim_Transaction_Detect_Changes.s3p_created_date,
	LKP_Sup_Claim_Financial_Code.sup_claim_financial_code_id AS LKP_sup_claim_financial_code_id,
	-- *INF*: IIF(ISNULL(LKP_sup_claim_financial_code_id),-1,LKP_sup_claim_financial_code_id)
	IFF(LKP_sup_claim_financial_code_id IS NULL, - 1, LKP_sup_claim_financial_code_id) AS o_FinancialTypeCodeID,
	EXP_Claim_Transaction_Detect_Changes.LKP_PMSTransactionCodeID,
	-- *INF*: IIF(ISNULL(LKP_PMSTransactionCodeID),-1,LKP_PMSTransactionCodeID)
	IFF(LKP_PMSTransactionCodeID IS NULL, - 1, LKP_PMSTransactionCodeID) AS o_PMSTransactionCodeID,
	EXP_Claim_Transaction_Detect_Changes.LKP_TransactionCodeID,
	-- *INF*: IIF(ISNULL(LKP_TransactionCodeID),-1,LKP_TransactionCodeID)
	IFF(LKP_TransactionCodeID IS NULL, - 1, LKP_TransactionCodeID) AS o_TransactionCodeID,
	LKP_Sup_CauseOfLoss.CauseOfLossId AS LKP_CauseOfLossId,
	-- *INF*: IIF(ISNULL(LKP_CauseOfLossId),-1,LKP_CauseOfLossId)
	IFF(LKP_CauseOfLossId IS NULL, - 1, LKP_CauseOfLossId) AS o_CauseOfLossID,
	LKP_Sup_Claim_Reserve_Category.sup_claim_reserve_ctgry_id AS LKP_sup_claim_reserve_ctgry_id,
	-- *INF*: IIF(ISNULL(LKP_sup_claim_reserve_ctgry_id),-1,LKP_sup_claim_reserve_ctgry_id)
	IFF(LKP_sup_claim_reserve_ctgry_id IS NULL, - 1, LKP_sup_claim_reserve_ctgry_id) AS o_SupReserveCategoryCodeID,
	-1 AS o_S3PTransactionCodeID,
	LKP_Sup_Claim_Transaction_Category.sup_claim_trans_catetory_id AS LKP_sup_claim_trans_catetory_id,
	-- *INF*: IIF(ISNULL(LKP_sup_claim_trans_catetory_id),-1,LKP_sup_claim_trans_catetory_id)
	IFF(LKP_sup_claim_trans_catetory_id IS NULL, - 1, LKP_sup_claim_trans_catetory_id) AS o_SupTransactionCategoryCode
	FROM EXP_Claim_Transaction_Detect_Changes
	LEFT JOIN LKP_CLAIM_TRANSACTION
	ON LKP_CLAIM_TRANSACTION.claimant_cov_det_ak_id = EXP_Claim_Transaction_Detect_Changes.CLAIMANT_COV_DET_AK_ID_OP AND LKP_CLAIM_TRANSACTION.cause_of_loss = EXP_Claim_Transaction_Detect_Changes.OP_LOSS_CAUSE AND LKP_CLAIM_TRANSACTION.reserve_ctgry = EXP_Claim_Transaction_Detect_Changes.LOSS_RESERVE_CATEGORY AND LKP_CLAIM_TRANSACTION.pms_trans_code = EXP_Claim_Transaction_Detect_Changes.LOSS_TRANSACTION AND LKP_CLAIM_TRANSACTION.trans_date = EXP_Claim_Transaction_Detect_Changes.LOSS_TRANSACTION_DATE_OP AND LKP_CLAIM_TRANSACTION.trans_ctgry_code = EXP_Claim_Transaction_Detect_Changes.OP_LOSS_TRANSACTION_CATEGORY AND LKP_CLAIM_TRANSACTION.trans_amt = EXP_Claim_Transaction_Detect_Changes.LOSS_PAID_OR_RESV_AMT AND LKP_CLAIM_TRANSACTION.draft_num = EXP_Claim_Transaction_Detect_Changes.LOSS_DRAFT_NO
	LEFT JOIN LKP_Claim_Transaction_83_84_27_40
	ON LKP_Claim_Transaction_83_84_27_40.claimant_cov_det_ak_id = EXP_Claim_Transaction_Detect_Changes.CLAIMANT_COV_DET_AK_ID_OP AND LKP_Claim_Transaction_83_84_27_40.cause_of_loss = EXP_Claim_Transaction_Detect_Changes.OP_LOSS_CAUSE AND LKP_Claim_Transaction_83_84_27_40.reserve_ctgry = EXP_Claim_Transaction_Detect_Changes.LOSS_RESERVE_CATEGORY AND LKP_Claim_Transaction_83_84_27_40.pms_trans_code = EXP_Claim_Transaction_Detect_Changes.LOSS_TRANSACTION_83_84_40_27 AND LKP_Claim_Transaction_83_84_27_40.trans_ctgry_code = EXP_Claim_Transaction_Detect_Changes.OP_LOSS_TRANSACTION_CATEGORY AND LKP_Claim_Transaction_83_84_27_40.trans_hist_amt = EXP_Claim_Transaction_Detect_Changes.LOSS_NET_CHANGE_DOLLARS
	LEFT JOIN LKP_Sup_CauseOfLoss
	ON LKP_Sup_CauseOfLoss.LineOfBusiness = LKP_V2_Policy.pms_pol_lob_code AND LKP_Sup_CauseOfLoss.MajorPeril = EXP_Claim_Transaction_Detect_Changes.LKP_LOSS_MAJOR_PERIL AND LKP_Sup_CauseOfLoss.CauseOfLoss = EXP_Claim_Transaction_Detect_Changes.OP_LOSS_CAUSE
	LEFT JOIN LKP_Sup_Claim_Financial_Code
	ON LKP_Sup_Claim_Financial_Code.financial_code = EXP_Claim_Transaction_Detect_Changes.FINANCIAL_TYPE_CODE
	LEFT JOIN LKP_Sup_Claim_Reserve_Category
	ON LKP_Sup_Claim_Reserve_Category.reserve_ctgry_code = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_RESERVE_CATEGORY
	LEFT JOIN LKP_Sup_Claim_Transaction_Category
	ON LKP_Sup_Claim_Transaction_Category.trans_ctgry_code = EXP_Claim_Transaction_Detect_Changes.OP_LOSS_TRANSACTION_CATEGORY
),
RTR_CLAIM_TRANSACTION AS (
	SELECT
	Claim_Trans_ID_Out AS LKP_claim_trans_id,
	LKP_type_disability,
	LKP_sar_id,
	LKP_pms_acct_entered_date,
	LKP_trans_hist_amt,
	LKP_trans_entry_oper_id,
	PIF_4578_stage_id,
	SAR_ID1 AS SAR_ID,
	LOSS_RESERVE_CATEGORY,
	s3p_trans_code,
	LOSS_NET_CHANGE_DOLLARS,
	IN_LOSS_PAID_OR_RESV_AMT AS LOSS_PAID_OR_RESV_AMT,
	LOSS_TRANSACTION,
	FINANCIAL_TYPE_CODE,
	TRANS_TYPE_CODE_OP,
	LOSS_ENTRY_OPERATOR,
	OP_LOSS_TRANSACTION_CATEGORY,
	LOSS_CAUSE,
	LOSS_DRAFT_NO,
	LOSS_TRANSACTION_DATE_OP,
	S3P_UPDATED_DATE_OP,
	S3P_TO_PMS_TRANS_DATE_OP,
	LOSS_ACCOUNT_ENTERED_DATE_OP,
	TRANS_BASE_TYPE_CODE_OP,
	TRANS_RSN_OP,
	SINGLE_CHECK_IND_OP,
	OFFSET_REISSUE_IND,
	LOSS_TYPE_DISABILITY,
	LOSS_OFFSET_ONSET_IND,
	o_CLAIMANT_COV_DET_AK_ID_OP AS CLAIMANT_COV_DET_AK_ID_OP,
	reprocess_date_out AS reprocess_date,
	SOURCE_SYSTEM_ID,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_dt,
	eff_to_dt,
	created_date,
	modified_date,
	claim_pay_ak_id,
	err_flag,
	tax_id,
	claim_master_1099_list_ak_id,
	trans_offset_onset_ind,
	s3p_created_date,
	o_FinancialTypeCodeID AS FinancialTypeCodeID,
	o_PMSTransactionCodeID AS PMSTransactionCodeID,
	o_TransactionCodeID AS TransactionCodeID,
	o_CauseOfLossID AS CauseOfLossID,
	o_SupReserveCategoryCodeID AS SupReserveCategoryCodeID,
	o_S3PTransactionCodeID AS S3PTransactionCodeID,
	o_SupTransactionCategoryCode AS SupTransactionCategoryCode
	FROM EXP_Evaluate
),
RTR_CLAIM_TRANSACTION_INSERT AS (SELECT * FROM RTR_CLAIM_TRANSACTION WHERE ISNULL(LKP_claim_trans_id)),
RTR_CLAIM_TRANSACTION_UPDATE AS (SELECT * FROM RTR_CLAIM_TRANSACTION WHERE NOT ISNULL(LKP_claim_trans_id)
AND (LKP_type_disability <>TO_INTEGER( LTRIM(RTRIM(LOSS_TYPE_DISABILITY)))
OR LTRIM(RTRIM(LKP_sar_id)) <> LTRIM(RTRIM(SAR_ID))
OR LKP_pms_acct_entered_date <> LOSS_ACCOUNT_ENTERED_DATE_OP
OR LKP_trans_hist_amt <> LOSS_NET_CHANGE_DOLLARS
OR LTRIM(RTRIM(LKP_trans_entry_oper_id)) <> LTRIM(RTRIM(LOSS_ENTRY_OPERATOR))
OR IN(LOSS_TRANSACTION, '90', '91', '92', '95', '97', '98', '99'))),
UPD_CLAIM_TRANSACTION_UPDATE AS (
	SELECT
	LKP_claim_trans_id AS CLAIM_TRANS_ID, 
	SAR_ID, 
	LOSS_ENTRY_OPERATOR, 
	LOSS_NET_CHANGE_DOLLARS, 
	LOSS_ACCOUNT_ENTERED_DATE_OP AS LOSS_ACCOUNT_ENTERED_DATE, 
	LOSS_TYPE_DISABILITY, 
	CLAIMANT_COV_DET_AK_ID_OP AS CLAIMANT_COV_DET_AK_ID_OP3, 
	LOSS_CAUSE AS LOSS_CAUSE3, 
	LOSS_RESERVE_CATEGORY AS LOSS_RESERVE_CATEGORY3, 
	FINANCIAL_TYPE_CODE AS FINANCIAL_TYPE_CODE3, 
	LOSS_OFFSET_ONSET_IND AS LOSS_OFFSET_ONSET_IND3, 
	LOSS_TRANSACTION AS LOSS_TRANSACTION3, 
	TRANS_TYPE_CODE_OP AS TRANS_TYPE_CODE_OP3, 
	LOSS_TRANSACTION_DATE_OP AS LOSS_TRANSACTION_DATE_OP3, 
	LOSS_DRAFT_NO AS LOSS_DRAFT_NO3, 
	LOSS_PAID_OR_RESV_AMT AS LOSS_PAID_OR_RESV_AMT3, 
	OP_LOSS_TRANSACTION_CATEGORY AS OP_LOSS_TRANSACTION_CATEGORY3, 
	PIF_4578_stage_id AS PIF_4578_stage_id3, 
	audit_id AS audit_id3, 
	modified_date, 
	tax_id AS tax_id3, 
	claim_master_1099_list_ak_id AS claim_master_1099_list_ak_id3, 
	FinancialTypeCodeID AS FinancialTypeCodeID3, 
	PMSTransactionCodeID AS PMSTransactionCodeID3, 
	TransactionCodeID AS TransactionCodeID3, 
	CauseOfLossID AS CauseOfLossID3, 
	SupReserveCategoryCodeID AS SupReserveCategoryCodeID3, 
	S3PTransactionCodeID AS S3PTransactionCodeID3, 
	SupTransactionCategoryCode AS SupTransactionCategoryCode3
	FROM RTR_CLAIM_TRANSACTION_UPDATE
),
claim_transaction_update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_transaction AS T
	USING UPD_CLAIM_TRANSACTION_UPDATE AS S
	ON T.claim_trans_id = S.CLAIM_TRANS_ID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.claimant_cov_det_ak_id = S.CLAIMANT_COV_DET_AK_ID_OP3, T.cause_of_loss = S.LOSS_CAUSE3, T.reserve_ctgry = S.LOSS_RESERVE_CATEGORY3, T.type_disability = S.LOSS_TYPE_DISABILITY, T.sar_id = S.SAR_ID, T.offset_onset_ind = S.LOSS_OFFSET_ONSET_IND3, T.pms_trans_code = S.LOSS_TRANSACTION3, T.trans_code = S.TRANS_TYPE_CODE_OP3, T.trans_date = S.LOSS_TRANSACTION_DATE_OP3, T.pms_acct_entered_date = S.LOSS_ACCOUNT_ENTERED_DATE, T.trans_ctgry_code = S.OP_LOSS_TRANSACTION_CATEGORY3, T.trans_amt = S.LOSS_PAID_OR_RESV_AMT3, T.trans_hist_amt = S.LOSS_NET_CHANGE_DOLLARS, T.draft_num = S.LOSS_DRAFT_NO3, T.trans_entry_oper_id = S.LOSS_ENTRY_OPERATOR, T.wc_stage_pk_id = S.PIF_4578_stage_id3, T.audit_id = S.audit_id3, T.modified_date = S.modified_date, T.tax_id = S.tax_id3, T.claim_master_1099_list_ak_id = S.claim_master_1099_list_ak_id3, T.CauseOfLossID = S.CauseOfLossID3, T.SupReserveCategoryCodeID = S.SupReserveCategoryCodeID3, T.FinancialTypeCodeID = S.FinancialTypeCodeID3, T.S3PTransactionCodeID = S.S3PTransactionCodeID3, T.PMSTransactionCodeID = S.PMSTransactionCodeID3, T.TransactionCodeID = S.TransactionCodeID3, T.SupTransactionCategoryCodeID = S.SupTransactionCategoryCode3
),
SEQ_Claim_Transaction AS (
	CREATE SEQUENCE SEQ_Claim_Transaction
	START = 0
	INCREMENT = 1;
),
UPD_CLAIM_TRANSACTION_INSERT AS (
	SELECT
	SEQ_Claim_Transaction.NEXTVAL AS CLAIM_TRANS_AK_ID, 
	CLAIMANT_COV_DET_AK_ID_OP AS CLAIMANT_COV_DET_AK_ID, 
	LOSS_CAUSE, 
	LOSS_RESERVE_CATEGORY, 
	LOSS_TYPE_DISABILITY, 
	SAR_ID, 
	LOSS_OFFSET_ONSET_IND, 
	FINANCIAL_TYPE_CODE, 
	s3p_trans_code, 
	LOSS_TRANSACTION, 
	TRANS_TYPE_CODE_OP AS TRANS_TYPE_CODE, 
	LOSS_TRANSACTION_DATE_OP AS LOSS_TRANSACTION_DATE, 
	S3P_UPDATED_DATE_OP, 
	S3P_TO_PMS_TRANS_DATE_OP, 
	LOSS_ACCOUNT_ENTERED_DATE_OP AS LOSS_ACCOUNT_ENTERED_DATE, 
	TRANS_BASE_TYPE_CODE_OP, 
	OP_LOSS_TRANSACTION_CATEGORY AS LOSS_TRANSACTION_CATEGORY, 
	LOSS_PAID_OR_RESV_AMT, 
	LOSS_NET_CHANGE_DOLLARS, 
	TRANS_RSN_OP, 
	LOSS_DRAFT_NO, 
	SINGLE_CHECK_IND_OP, 
	OFFSET_REISSUE_IND, 
	LOSS_ENTRY_OPERATOR, 
	PIF_4578_stage_id, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_dt, 
	eff_to_dt, 
	SOURCE_SYSTEM_ID, 
	created_date, 
	modified_date, 
	reprocess_date, 
	claim_pay_ak_id AS claim_pay_ak_id1, 
	err_flag AS err_flag1, 
	tax_id AS tax_id1, 
	claim_master_1099_list_ak_id AS claim_master_1099_list_ak_id1, 
	trans_offset_onset_ind, 
	s3p_created_date, 
	FinancialTypeCodeID AS FinancialTypeCodeID1, 
	PMSTransactionCodeID AS PMSTransactionCodeID1, 
	TransactionCodeID AS TransactionCodeID1, 
	CauseOfLossID AS CauseOfLossID1, 
	SupReserveCategoryCodeID AS SupReserveCategoryCodeID1, 
	S3PTransactionCodeID AS S3PTransactionCodeID1, 
	SupTransactionCategoryCode AS SupTransactionCategoryCode1
	FROM RTR_CLAIM_TRANSACTION_INSERT
),
claim_transaction_insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_transaction
	(claim_trans_ak_id, claimant_cov_det_ak_id, claim_pay_ak_id, cause_of_loss, reserve_ctgry, type_disability, sar_id, offset_onset_ind, financial_type_code, s3p_trans_code, pms_trans_code, trans_code, trans_date, s3p_updated_date, s3p_to_pms_trans_date, pms_acct_entered_date, trans_base_type_code, trans_ctgry_code, trans_amt, trans_hist_amt, trans_rsn, draft_num, single_check_ind, offset_reissue_ind, reprocess_date, trans_entry_oper_id, wc_stage_pk_id, err_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, tax_id, claim_master_1099_list_ak_id, trans_offset_onset_ind, s3p_created_date, CauseOfLossID, SupReserveCategoryCodeID, FinancialTypeCodeID, S3PTransactionCodeID, PMSTransactionCodeID, TransactionCodeID, SupTransactionCategoryCodeID)
	SELECT 
	CLAIM_TRANS_AK_ID AS CLAIM_TRANS_AK_ID, 
	CLAIMANT_COV_DET_AK_ID AS CLAIMANT_COV_DET_AK_ID, 
	claim_pay_ak_id1 AS CLAIM_PAY_AK_ID, 
	LOSS_CAUSE AS CAUSE_OF_LOSS, 
	LOSS_RESERVE_CATEGORY AS RESERVE_CTGRY, 
	LOSS_TYPE_DISABILITY AS TYPE_DISABILITY, 
	SAR_ID AS SAR_ID, 
	LOSS_OFFSET_ONSET_IND AS OFFSET_ONSET_IND, 
	FINANCIAL_TYPE_CODE AS FINANCIAL_TYPE_CODE, 
	S3P_TRANS_CODE, 
	LOSS_TRANSACTION AS PMS_TRANS_CODE, 
	TRANS_TYPE_CODE AS TRANS_CODE, 
	LOSS_TRANSACTION_DATE AS TRANS_DATE, 
	S3P_UPDATED_DATE_OP AS S3P_UPDATED_DATE, 
	S3P_TO_PMS_TRANS_DATE_OP AS S3P_TO_PMS_TRANS_DATE, 
	LOSS_ACCOUNT_ENTERED_DATE AS PMS_ACCT_ENTERED_DATE, 
	TRANS_BASE_TYPE_CODE_OP AS TRANS_BASE_TYPE_CODE, 
	LOSS_TRANSACTION_CATEGORY AS TRANS_CTGRY_CODE, 
	LOSS_PAID_OR_RESV_AMT AS TRANS_AMT, 
	LOSS_NET_CHANGE_DOLLARS AS TRANS_HIST_AMT, 
	TRANS_RSN_OP AS TRANS_RSN, 
	LOSS_DRAFT_NO AS DRAFT_NUM, 
	SINGLE_CHECK_IND_OP AS SINGLE_CHECK_IND, 
	OFFSET_REISSUE_IND AS OFFSET_REISSUE_IND, 
	REPROCESS_DATE, 
	LOSS_ENTRY_OPERATOR AS TRANS_ENTRY_OPER_ID, 
	PIF_4578_stage_id AS WC_STAGE_PK_ID, 
	err_flag1 AS ERR_FLAG, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	eff_from_dt AS EFF_FROM_DATE, 
	eff_to_dt AS EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	tax_id1 AS TAX_ID, 
	claim_master_1099_list_ak_id1 AS CLAIM_MASTER_1099_LIST_AK_ID, 
	TRANS_OFFSET_ONSET_IND, 
	S3P_CREATED_DATE, 
	CauseOfLossID1 AS CAUSEOFLOSSID, 
	SupReserveCategoryCodeID1 AS SUPRESERVECATEGORYCODEID, 
	FinancialTypeCodeID1 AS FINANCIALTYPECODEID, 
	S3PTransactionCodeID1 AS S3PTRANSACTIONCODEID, 
	PMSTransactionCodeID1 AS PMSTRANSACTIONCODEID, 
	TransactionCodeID1 AS TRANSACTIONCODEID, 
	SupTransactionCategoryCode1 AS SUPTRANSACTIONCATEGORYCODEID
	FROM UPD_CLAIM_TRANSACTION_INSERT
),
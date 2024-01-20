WITH
SQ_PIF_42GQ_ATY_stage AS (
	SELECT
		pif_42gq_aty_stage_id,
		pif_symbol,
		pif_policy_number,
		pif_module,
		ipfcgq_rec_length,
		ipfcgq_action_code,
		ipfcgq_file_id,
		ipfcgq_segment_id,
		ipfcgq_segment_level_code,
		ipfcgq_segment_part_code,
		ipfcgq_sub_part_code,
		ipfcgq_year_of_loss,
		ipfcgq_month_of_loss,
		ipfcgq_day_of_loss,
		ipfcgq_loss_occurence,
		ipfcgq_loss_claimant,
		ipfcgq_claimant_use_code,
		ipfcgq_claimant_use_seq,
		ipfcgq_year_process,
		ipfcgq_month_process,
		ipfcgq_day_process,
		ipfcgq_year_change_entry,
		ipfcgq_month_change_entry,
		ipfcgq_day_change_entry,
		ipfcgq_sequence_change_entry,
		ipfcgq_segment_status,
		ipfcgq_entry_operator,
		ipfcgq_plaintiff_1,
		ipfcgq_plaintiff_2,
		ipfcgq_defendant_1,
		ipfcgq_defendant_2,
		ipfcgq_attorney_name_1,
		ipfcgq_attorney_type_1,
		ipfcgq_attorney_seq_1,
		ipfcgq_attorney_name_2,
		ipfcgq_attorney_type_2,
		ipfcgq_attorney_seq_2,
		ipfcgq_attorney_name_3,
		ipfcgq_attorney_type_3,
		ipfcgq_attorney_seq_3,
		ipfcgq_number_of_part78,
		ipfcgq_offset_onset_ind,
		ipfcgq_date_hire,
		ipfcgq_pms_future_use_gq,
		ipfcgq_direct_reporting,
		ipfcgq_cust_spl_use_gq,
		ipfcgq_yr2000_cust_use,
		inf_action,
		inf_timestamp,
		logical_flag,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM PIF_42GQ_ATY_stage
),
EXP_get_vlaues AS (
	SELECT
	pif_42gq_aty_stage_id,
	pif_symbol,
	pif_policy_number,
	pif_module,
	ipfcgq_year_of_loss,
	ipfcgq_month_of_loss,
	ipfcgq_day_of_loss,
	ipfcgq_loss_occurence,
	ipfcgq_loss_claimant
	FROM SQ_PIF_42GQ_ATY_stage
),
LKP_4578_stage AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module,
	loss_year,
	loss_month,
	loss_day,
	loss_occurence,
	loss_claimant,
	logical_flag
	FROM (
		SELECT MAX(pif_4578_stage.logical_flag) as logical_flag, pif_4578_stage.pif_symbol as pif_symbol, pif_4578_stage.pif_policy_number as pif_policy_number, pif_4578_stage.pif_module as pif_module, pif_4578_stage.loss_year as loss_year, pif_4578_stage.loss_month as loss_month, pif_4578_stage.loss_day as loss_day, pif_4578_stage.loss_occurence as loss_occurence, pif_4578_stage.loss_claimant as loss_claimant FROM pif_4578_stage
		GROUP BY pif_4578_stage.pif_symbol, pif_4578_stage.pif_policy_number, pif_4578_stage.pif_module, pif_4578_stage.loss_year, pif_4578_stage.loss_month, pif_4578_stage.loss_day, pif_4578_stage.loss_occurence, pif_4578_stage.loss_claimant
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module,loss_year,loss_month,loss_day,loss_occurence,loss_claimant ORDER BY pif_symbol) = 1
),
FILTRANS AS (
	SELECT
	EXP_get_vlaues.pif_42gq_aty_stage_id, 
	LKP_4578_stage.logical_flag
	FROM EXP_get_vlaues
	LEFT JOIN LKP_4578_stage
	ON LKP_4578_stage.pif_symbol = EXP_get_vlaues.pif_symbol AND LKP_4578_stage.pif_policy_number = EXP_get_vlaues.pif_policy_number AND LKP_4578_stage.pif_module = EXP_get_vlaues.pif_module AND LKP_4578_stage.loss_year = EXP_get_vlaues.ipfcgq_year_of_loss AND LKP_4578_stage.loss_month = EXP_get_vlaues.ipfcgq_month_of_loss AND LKP_4578_stage.loss_day = EXP_get_vlaues.ipfcgq_day_of_loss AND LKP_4578_stage.loss_occurence = EXP_get_vlaues.ipfcgq_loss_occurence AND LKP_4578_stage.loss_claimant = EXP_get_vlaues.ipfcgq_loss_claimant
	WHERE NOT ISNULL(logical_flag)
),
UPD_42GQ_ATY AS (
	SELECT
	pif_42gq_aty_stage_id, 
	logical_flag
	FROM FILTRANS
),
PIF_42GQ_ATY_stage_update AS (
	MERGE INTO PIF_42GQ_ATY_stage AS T
	USING UPD_42GQ_ATY AS S
	ON T.pif_42gq_aty_stage_id = S.pif_42gq_aty_stage_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.logical_flag = S.logical_flag
),
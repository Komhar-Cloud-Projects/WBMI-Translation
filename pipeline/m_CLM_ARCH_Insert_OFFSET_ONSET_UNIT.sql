WITH
SQ_offset_onset_unit_stage AS (
	SELECT
		offset_onset_unit_stage_id,
		tch_claim_nbr,
		off_onset_ts,
		unit_seq_nbr,
		off_sar_id,
		off_ins_line,
		off_loc_unit_num,
		off_risk_unit_grp,
		off_rsk_unt_gr_seq,
		off_risk_unit,
		off_risk_type_ind,
		off_sub_loc_num,
		off_seq_risk_unit,
		off_class_code,
		off_sr_seq,
		off_unit_desc,
		off_class_desc,
		off_unit_type_cd,
		off_spp_use_cd,
		on_sar_id,
		on_ins_line,
		on_loc_unit_num,
		on_risk_unit_grp,
		on_rsk_unt_gr_seq,
		on_risk_unit,
		on_risk_type_ind,
		on_sub_loc_num,
		on_seq_risk_unit,
		on_class_code,
		on_sr_seq,
		on_unit_desc,
		on_class_desc,
		on_unit_type_cd,
		on_spp_use_cd,
		object_type_cd,
		object_seq_nbr,
		logical_flag,
		extract_date,
		as_of_date,
		record_count,
		source_system_id,
		off_coverage_form,
		off_coverage_type,
		off_risk_type,
		on_coverage_form,
		on_coverage_type,
		on_risk_type
	FROM offset_onset_unit_stage
	WHERE offset_onset_unit_stage.off_onset_ts >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_arch_med_provider_stage AS (
	SELECT
	offset_onset_unit_stage_id,
	tch_claim_nbr,
	off_onset_ts,
	unit_seq_nbr,
	off_sar_id,
	off_ins_line,
	off_loc_unit_num,
	off_risk_unit_grp,
	off_rsk_unt_gr_seq,
	off_risk_unit,
	off_risk_type_ind,
	off_sub_loc_num,
	off_seq_risk_unit,
	off_class_code,
	off_sr_seq,
	off_unit_desc,
	off_class_desc,
	off_unit_type_cd,
	off_spp_use_cd,
	on_sar_id,
	on_ins_line,
	on_loc_unit_num,
	on_risk_unit_grp,
	on_rsk_unt_gr_seq,
	on_risk_unit,
	on_risk_type_ind,
	on_sub_loc_num,
	on_seq_risk_unit,
	on_class_code,
	on_sr_seq,
	on_unit_desc,
	on_class_desc,
	on_unit_type_cd,
	on_spp_use_cd,
	object_type_cd,
	object_seq_nbr,
	logical_flag,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	off_coverage_form,
	off_coverage_type,
	off_risk_type,
	on_coverage_form,
	on_coverage_type,
	on_risk_type,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_offset_onset_unit_stage
),
arch_offset_onset_unit_stage AS (
	INSERT INTO arch_offset_onset_unit_stage
	(offset_onset_unit_stage_id, tch_claim_nbr, off_onset_ts, unit_seq_nbr, off_sar_id, off_ins_line, off_loc_unit_num, off_risk_unit_grp, off_rsk_unt_gr_seq, off_risk_unit, off_risk_type_ind, off_sub_loc_num, off_seq_risk_unit, off_class_code, off_sr_seq, off_unit_desc, off_class_desc, off_unit_type_cd, off_spp_use_cd, on_sar_id, on_ins_line, on_loc_unit_num, on_risk_unit_grp, on_rsk_unt_gr_seq, on_risk_unit, on_risk_type_ind, on_sub_loc_num, on_seq_risk_unit, on_class_code, on_sr_seq, on_unit_desc, on_class_desc, on_unit_type_cd, on_spp_use_cd, object_type_cd, object_seq_nbr, logical_flag, extract_date, as_of_date, record_count, source_system_id, audit_id, off_coverage_form, off_coverage_type, off_risk_type, on_coverage_form, on_coverage_type, on_risk_type)
	SELECT 
	OFFSET_ONSET_UNIT_STAGE_ID, 
	TCH_CLAIM_NBR, 
	OFF_ONSET_TS, 
	UNIT_SEQ_NBR, 
	OFF_SAR_ID, 
	OFF_INS_LINE, 
	OFF_LOC_UNIT_NUM, 
	OFF_RISK_UNIT_GRP, 
	OFF_RSK_UNT_GR_SEQ, 
	OFF_RISK_UNIT, 
	OFF_RISK_TYPE_IND, 
	OFF_SUB_LOC_NUM, 
	OFF_SEQ_RISK_UNIT, 
	OFF_CLASS_CODE, 
	OFF_SR_SEQ, 
	OFF_UNIT_DESC, 
	OFF_CLASS_DESC, 
	OFF_UNIT_TYPE_CD, 
	OFF_SPP_USE_CD, 
	ON_SAR_ID, 
	ON_INS_LINE, 
	ON_LOC_UNIT_NUM, 
	ON_RISK_UNIT_GRP, 
	ON_RSK_UNT_GR_SEQ, 
	ON_RISK_UNIT, 
	ON_RISK_TYPE_IND, 
	ON_SUB_LOC_NUM, 
	ON_SEQ_RISK_UNIT, 
	ON_CLASS_CODE, 
	ON_SR_SEQ, 
	ON_UNIT_DESC, 
	ON_CLASS_DESC, 
	ON_UNIT_TYPE_CD, 
	ON_SPP_USE_CD, 
	OBJECT_TYPE_CD, 
	OBJECT_SEQ_NBR, 
	LOGICAL_FLAG, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID, 
	OFF_COVERAGE_FORM, 
	OFF_COVERAGE_TYPE, 
	OFF_RISK_TYPE, 
	ON_COVERAGE_FORM, 
	ON_COVERAGE_TYPE, 
	ON_RISK_TYPE
	FROM EXP_arch_med_provider_stage
),
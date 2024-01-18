WITH
LKP_42X6_STAGE AS (
	SELECT
	pif_42x6_stage_id,
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
		SELECT pif_42x6_stage.pif_42x6_stage_id as pif_42x6_stage_id, 
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
		CASE LEN(LTRIM(RTRIM(COALESCE(ipfcx6_loss_cause,' ')))) WHEN 0 THEN '0' ELSE LTRIM(RTRIM( ipfcx6_loss_cause)) END AS ipfcx6_loss_cause, 
		CASE LEN(LTRIM(RTRIM(COALESCE(ipfcx6_offset_onset_ind,' ')))) WHEN 0 THEN '0' ELSE LTRIM(RTRIM(ipfcx6_offset_onset_ind)) END AS ipfcx6_offset_onset_ind
		FROM pif_42x6_stage
		WHERE     (ipfcx6_year_process >= 1998)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module,ipfcx6_year_of_loss,ipfcx6_month_of_loss,ipfcx6_day_of_loss,ipfcx6_loss_occ_fdigit,ipfcx6_usr_loss_occurence,ipfcx6_loss_claimant,ipfcx6_insurance_line,ipfcx6_location_number,ipfcx6_sub_location_number,ipfcx6_risk_unit_group,ipfcx6_class_code_group,ipfcx6_class_code_member,ipfcx6_loss_unit,ipfcx6_risk_sequence,ipfcx6_major_peril,ipfcx6_sequence_type_exposure,ipfcx6_loss_disability,ipfcx6_member,ipfcx6_reserve_category,ipfcx6_loss_cause ORDER BY pif_42x6_stage_id) = 1
),
SQ_CLM_STG_Update_42GP AS (
	SELECT pif_4578_stage.pif_symbol, pif_4578_stage.pif_policy_number, pif_4578_stage.pif_module, pif_4578_stage.loss_insurance_line, pif_4578_stage.loss_location_number, pif_4578_stage.loss_sub_location_number, pif_4578_stage.loss_risk_unit_group, pif_4578_stage.loss_class_code_group, pif_4578_stage.loss_class_code_member, pif_4578_stage.loss_unit, pif_4578_stage.loss_sequence_risk_unit, pif_4578_stage.loss_major_peril, pif_4578_stage.loss_major_peril_seq, pif_4578_stage.loss_year, pif_4578_stage.loss_month, pif_4578_stage.loss_day, pif_4578_stage.loss_occurence, pif_4578_stage.loss_claimant, pif_4578_stage.loss_member, pif_4578_stage.loss_disability, pif_4578_stage.loss_reserve_category, pif_4578_stage.loss_cause, pif_4578_stage.loss_offset_onset_ind, MAX(pif_4578_stage.logical_flag) , pif_4578_stage.source_system_id 
	FROM
	 pif_4578_stage
	group by
	pif_4578_stage.pif_symbol, pif_4578_stage.pif_policy_number, pif_4578_stage.pif_module, pif_4578_stage.loss_insurance_line, pif_4578_stage.loss_location_number, pif_4578_stage.loss_sub_location_number, pif_4578_stage.loss_risk_unit_group, pif_4578_stage.loss_class_code_group, pif_4578_stage.loss_class_code_member, pif_4578_stage.loss_unit, pif_4578_stage.loss_sequence_risk_unit, pif_4578_stage.loss_major_peril, pif_4578_stage.loss_major_peril_seq, pif_4578_stage.loss_year, pif_4578_stage.loss_month, pif_4578_stage.loss_day, pif_4578_stage.loss_occurence, pif_4578_stage.loss_claimant, pif_4578_stage.loss_member, pif_4578_stage.loss_disability, pif_4578_stage.loss_reserve_category, pif_4578_stage.loss_cause, pif_4578_stage.loss_offset_onset_ind, pif_4578_stage.source_system_id
),
EXP_CLM_STG_Update_42GP AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module,
	loss_insurance_line,
	-- *INF*: IIF(ISNULL(loss_insurance_line) OR IS_SPACES(loss_insurance_line),'N/A',LTRIM(RTRIM(loss_insurance_line)))
	IFF(
	    loss_insurance_line IS NULL OR LENGTH(loss_insurance_line)>0 AND TRIM(loss_insurance_line)='',
	    'N/A',
	    LTRIM(RTRIM(loss_insurance_line))
	) AS V_loss_insurance_line,
	V_loss_insurance_line AS out_loss_insurance_line,
	loss_location_number,
	-- *INF*: IIF(ISNULL(loss_location_number),0,loss_location_number)
	IFF(loss_location_number IS NULL, 0, loss_location_number) AS V_loss_location_number,
	V_loss_location_number AS out_loss_location_number,
	loss_sub_location_number,
	-- *INF*: IIF(ISNULL(loss_sub_location_number),0,loss_sub_location_number)
	IFF(loss_sub_location_number IS NULL, 0, loss_sub_location_number) AS V_loss_sub_location_number,
	V_loss_sub_location_number AS out_loss_sub_location_number,
	loss_risk_unit_group,
	-- *INF*: IIF(ISNULL(loss_risk_unit_group) OR IS_SPACES(loss_risk_unit_group),'0',LTRIM(RTRIM(loss_risk_unit_group)))
	IFF(
	    loss_risk_unit_group IS NULL
	    or LENGTH(loss_risk_unit_group)>0
	    and TRIM(loss_risk_unit_group)='',
	    '0',
	    LTRIM(RTRIM(loss_risk_unit_group))
	) AS V_loss_risk_unit_group,
	V_loss_risk_unit_group AS out_loss_risk_unit_group,
	loss_class_code_group,
	-- *INF*: IIF(ISNULL(loss_class_code_group),0,loss_class_code_group)
	IFF(loss_class_code_group IS NULL, 0, loss_class_code_group) AS V_loss_class_code_group,
	V_loss_class_code_group AS out_loss_class_code_group,
	loss_class_code_member,
	-- *INF*: IIF(ISNULL(loss_class_code_member),0,loss_class_code_member)
	IFF(loss_class_code_member IS NULL, 0, loss_class_code_member) AS V_loss_class_code_member,
	V_loss_class_code_member AS out_loss_class_code_member,
	loss_unit,
	-- *INF*: IIF(ISNULL(loss_unit) OR IS_SPACES(loss_unit),'0',LTRIM(RTRIM(loss_unit)))
	IFF(
	    loss_unit IS NULL OR LENGTH(loss_unit)>0 AND TRIM(loss_unit)='', '0',
	    LTRIM(RTRIM(loss_unit))
	) AS V_loss_unit,
	V_loss_unit AS out_loss_unit,
	loss_sequence_risk_unit,
	-- *INF*: IIF(ISNULL(loss_sequence_risk_unit) OR IS_SPACES(loss_sequence_risk_unit),'0',LTRIM(RTRIM(loss_sequence_risk_unit)))
	IFF(
	    loss_sequence_risk_unit IS NULL
	    or LENGTH(loss_sequence_risk_unit)>0
	    and TRIM(loss_sequence_risk_unit)='',
	    '0',
	    LTRIM(RTRIM(loss_sequence_risk_unit))
	) AS V_loss_sequence_risk_unit,
	V_loss_sequence_risk_unit AS out_loss_sequence_risk_unit,
	loss_major_peril,
	-- *INF*: IIF(ISNULL(loss_major_peril) OR IS_SPACES(loss_major_peril),'0',LTRIM(RTRIM(loss_major_peril)))
	IFF(
	    loss_major_peril IS NULL OR LENGTH(loss_major_peril)>0 AND TRIM(loss_major_peril)='', '0',
	    LTRIM(RTRIM(loss_major_peril))
	) AS V_loss_major_peril,
	V_loss_major_peril AS out_loss_major_peril,
	loss_major_peril_seq,
	-- *INF*: IIF(ISNULL(loss_major_peril_seq) OR IS_SPACES(loss_major_peril_seq),'0',LTRIM(RTRIM(loss_major_peril_seq)))
	IFF(
	    loss_major_peril_seq IS NULL
	    or LENGTH(loss_major_peril_seq)>0
	    and TRIM(loss_major_peril_seq)='',
	    '0',
	    LTRIM(RTRIM(loss_major_peril_seq))
	) AS V_loss_major_peril_seq,
	V_loss_major_peril_seq AS out_loss_major_peril_seq,
	loss_year,
	-- *INF*: IIF(ISNULL(loss_year),1800,loss_year)
	IFF(loss_year IS NULL, 1800, loss_year) AS V_loss_year,
	V_loss_year AS out_loss_year,
	loss_month,
	-- *INF*: IIF(ISNULL(loss_month),1,loss_month)
	IFF(loss_month IS NULL, 1, loss_month) AS V_loss_month,
	V_loss_month AS out_loss_month,
	loss_day,
	-- *INF*: IIF(ISNULL(loss_day),1,loss_day)
	IFF(loss_day IS NULL, 1, loss_day) AS V_loss_day,
	V_loss_day AS out_loss_day,
	loss_occurence,
	-- *INF*: IIF(LENGTH(loss_occurence)<=2,'0',SUBSTR(loss_occurence,1,1))
	IFF(LENGTH(loss_occurence) <= 2, '0', SUBSTR(loss_occurence, 1, 1)) AS ipfcx6_loss_occ_fdigit,
	ipfcx6_loss_occ_fdigit AS out_ipfcx6_loss_occ_fdigit,
	-- *INF*: IIF(LENGTH(loss_occurence)>2,SUBSTR(loss_occurence,2,2),TO_CHAR(loss_occurence))
	IFF(LENGTH(loss_occurence) > 2, SUBSTR(loss_occurence, 2, 2), TO_CHAR(loss_occurence)) AS ipfcx6_usr_loss_occurence,
	ipfcx6_usr_loss_occurence AS out_ipfcx6_usr_loss_occurence,
	loss_claimant,
	-- *INF*: IIF(ISNULL(loss_claimant),'0',loss_claimant)
	IFF(loss_claimant IS NULL, '0', loss_claimant) AS V_loss_claimant,
	V_loss_claimant AS out_loss_claimant,
	loss_member,
	-- *INF*: IIF(ISNULL(loss_member) OR IS_SPACES(loss_member),'0',LTRIM(RTRIM(loss_member)))
	IFF(
	    loss_member IS NULL OR LENGTH(loss_member)>0 AND TRIM(loss_member)='', '0',
	    LTRIM(RTRIM(loss_member))
	) AS V_loss_member,
	V_loss_member AS out_loss_member,
	loss_disability,
	-- *INF*: IIF(ISNULL(loss_disability) OR IS_SPACES(loss_disability),'0',LTRIM(RTRIM(loss_disability)))
	IFF(
	    loss_disability IS NULL OR LENGTH(loss_disability)>0 AND TRIM(loss_disability)='', '0',
	    LTRIM(RTRIM(loss_disability))
	) AS V_loss_disability,
	V_loss_disability AS out_loss_disability,
	loss_reserve_category,
	-- *INF*: IIF(ISNULL(loss_reserve_category) OR IS_SPACES(loss_reserve_category),'0',LTRIM(RTRIM(loss_reserve_category)))
	IFF(
	    loss_reserve_category IS NULL
	    or LENGTH(loss_reserve_category)>0
	    and TRIM(loss_reserve_category)='',
	    '0',
	    LTRIM(RTRIM(loss_reserve_category))
	) AS V_loss_reserve_category,
	V_loss_reserve_category AS out_loss_reserve_category,
	loss_cause,
	-- *INF*: IIF(ISNULL(loss_cause) OR IS_SPACES(loss_cause),'0',LTRIM(RTRIM(loss_cause)))
	IFF(
	    loss_cause IS NULL OR LENGTH(loss_cause)>0 AND TRIM(loss_cause)='', '0',
	    LTRIM(RTRIM(loss_cause))
	) AS V_loss_cause,
	V_loss_cause AS out_loss_cause,
	loss_offset_onset_ind,
	-- *INF*: IIF(ISNULL(loss_offset_onset_ind) OR IS_SPACES(loss_offset_onset_ind),'0',LTRIM(RTRIM(loss_offset_onset_ind)))
	IFF(
	    loss_offset_onset_ind IS NULL
	    or LENGTH(loss_offset_onset_ind)>0
	    and TRIM(loss_offset_onset_ind)='',
	    '0',
	    LTRIM(RTRIM(loss_offset_onset_ind))
	) AS V_loss_offset_onset_ind,
	-- *INF*: :LKP.LKP_42X6_STAGE(pif_symbol, pif_policy_number, pif_module, V_loss_year, V_loss_month, V_loss_day, ipfcx6_loss_occ_fdigit, ipfcx6_usr_loss_occurence, V_loss_claimant, V_loss_insurance_line, V_loss_location_number, V_loss_sub_location_number, V_loss_risk_unit_group, V_loss_class_code_group, V_loss_class_code_member, V_loss_unit,  V_loss_sequence_risk_unit, V_loss_major_peril, V_loss_major_peril_seq, V_loss_disability, V_loss_member, V_loss_reserve_category, V_loss_cause)
	LKP_42X6_STAGE_pif_symbol_pif_policy_number_pif_module_V_loss_year_V_loss_month_V_loss_day_ipfcx6_loss_occ_fdigit_ipfcx6_usr_loss_occurence_V_loss_claimant_V_loss_insurance_line_V_loss_location_number_V_loss_sub_location_number_V_loss_risk_unit_group_V_loss_class_code_group_V_loss_class_code_member_V_loss_unit_V_loss_sequence_risk_unit_V_loss_major_peril_V_loss_major_peril_seq_V_loss_disability_V_loss_member_V_loss_reserve_category_V_loss_cause.pif_42x6_stage_id AS v_pif_42x6_stage_id,
	v_pif_42x6_stage_id AS pif_42x6_stage_id,
	logical_flag,
	-- *INF*: DECODE(logical_flag,
	-- '0','1',
	-- '-1','2',
	-- '-2','3',
	-- '-3','4')
	DECODE(
	    logical_flag,
	    '0', '1',
	    '-1', '2',
	    '-2', '3',
	    '-3', '4'
	) AS logical_flag_insert,
	source_system_id
	FROM SQ_CLM_STG_Update_42GP
	LEFT JOIN LKP_42X6_STAGE LKP_42X6_STAGE_pif_symbol_pif_policy_number_pif_module_V_loss_year_V_loss_month_V_loss_day_ipfcx6_loss_occ_fdigit_ipfcx6_usr_loss_occurence_V_loss_claimant_V_loss_insurance_line_V_loss_location_number_V_loss_sub_location_number_V_loss_risk_unit_group_V_loss_class_code_group_V_loss_class_code_member_V_loss_unit_V_loss_sequence_risk_unit_V_loss_major_peril_V_loss_major_peril_seq_V_loss_disability_V_loss_member_V_loss_reserve_category_V_loss_cause
	ON LKP_42X6_STAGE_pif_symbol_pif_policy_number_pif_module_V_loss_year_V_loss_month_V_loss_day_ipfcx6_loss_occ_fdigit_ipfcx6_usr_loss_occurence_V_loss_claimant_V_loss_insurance_line_V_loss_location_number_V_loss_sub_location_number_V_loss_risk_unit_group_V_loss_class_code_group_V_loss_class_code_member_V_loss_unit_V_loss_sequence_risk_unit_V_loss_major_peril_V_loss_major_peril_seq_V_loss_disability_V_loss_member_V_loss_reserve_category_V_loss_cause.pif_symbol = pif_symbol
	AND LKP_42X6_STAGE_pif_symbol_pif_policy_number_pif_module_V_loss_year_V_loss_month_V_loss_day_ipfcx6_loss_occ_fdigit_ipfcx6_usr_loss_occurence_V_loss_claimant_V_loss_insurance_line_V_loss_location_number_V_loss_sub_location_number_V_loss_risk_unit_group_V_loss_class_code_group_V_loss_class_code_member_V_loss_unit_V_loss_sequence_risk_unit_V_loss_major_peril_V_loss_major_peril_seq_V_loss_disability_V_loss_member_V_loss_reserve_category_V_loss_cause.pif_policy_number = pif_policy_number
	AND LKP_42X6_STAGE_pif_symbol_pif_policy_number_pif_module_V_loss_year_V_loss_month_V_loss_day_ipfcx6_loss_occ_fdigit_ipfcx6_usr_loss_occurence_V_loss_claimant_V_loss_insurance_line_V_loss_location_number_V_loss_sub_location_number_V_loss_risk_unit_group_V_loss_class_code_group_V_loss_class_code_member_V_loss_unit_V_loss_sequence_risk_unit_V_loss_major_peril_V_loss_major_peril_seq_V_loss_disability_V_loss_member_V_loss_reserve_category_V_loss_cause.pif_module = pif_module
	AND LKP_42X6_STAGE_pif_symbol_pif_policy_number_pif_module_V_loss_year_V_loss_month_V_loss_day_ipfcx6_loss_occ_fdigit_ipfcx6_usr_loss_occurence_V_loss_claimant_V_loss_insurance_line_V_loss_location_number_V_loss_sub_location_number_V_loss_risk_unit_group_V_loss_class_code_group_V_loss_class_code_member_V_loss_unit_V_loss_sequence_risk_unit_V_loss_major_peril_V_loss_major_peril_seq_V_loss_disability_V_loss_member_V_loss_reserve_category_V_loss_cause.ipfcx6_year_of_loss = V_loss_year
	AND LKP_42X6_STAGE_pif_symbol_pif_policy_number_pif_module_V_loss_year_V_loss_month_V_loss_day_ipfcx6_loss_occ_fdigit_ipfcx6_usr_loss_occurence_V_loss_claimant_V_loss_insurance_line_V_loss_location_number_V_loss_sub_location_number_V_loss_risk_unit_group_V_loss_class_code_group_V_loss_class_code_member_V_loss_unit_V_loss_sequence_risk_unit_V_loss_major_peril_V_loss_major_peril_seq_V_loss_disability_V_loss_member_V_loss_reserve_category_V_loss_cause.ipfcx6_month_of_loss = V_loss_month
	AND LKP_42X6_STAGE_pif_symbol_pif_policy_number_pif_module_V_loss_year_V_loss_month_V_loss_day_ipfcx6_loss_occ_fdigit_ipfcx6_usr_loss_occurence_V_loss_claimant_V_loss_insurance_line_V_loss_location_number_V_loss_sub_location_number_V_loss_risk_unit_group_V_loss_class_code_group_V_loss_class_code_member_V_loss_unit_V_loss_sequence_risk_unit_V_loss_major_peril_V_loss_major_peril_seq_V_loss_disability_V_loss_member_V_loss_reserve_category_V_loss_cause.ipfcx6_day_of_loss = V_loss_day
	AND LKP_42X6_STAGE_pif_symbol_pif_policy_number_pif_module_V_loss_year_V_loss_month_V_loss_day_ipfcx6_loss_occ_fdigit_ipfcx6_usr_loss_occurence_V_loss_claimant_V_loss_insurance_line_V_loss_location_number_V_loss_sub_location_number_V_loss_risk_unit_group_V_loss_class_code_group_V_loss_class_code_member_V_loss_unit_V_loss_sequence_risk_unit_V_loss_major_peril_V_loss_major_peril_seq_V_loss_disability_V_loss_member_V_loss_reserve_category_V_loss_cause.ipfcx6_loss_occ_fdigit = ipfcx6_loss_occ_fdigit
	AND LKP_42X6_STAGE_pif_symbol_pif_policy_number_pif_module_V_loss_year_V_loss_month_V_loss_day_ipfcx6_loss_occ_fdigit_ipfcx6_usr_loss_occurence_V_loss_claimant_V_loss_insurance_line_V_loss_location_number_V_loss_sub_location_number_V_loss_risk_unit_group_V_loss_class_code_group_V_loss_class_code_member_V_loss_unit_V_loss_sequence_risk_unit_V_loss_major_peril_V_loss_major_peril_seq_V_loss_disability_V_loss_member_V_loss_reserve_category_V_loss_cause.ipfcx6_usr_loss_occurence = ipfcx6_usr_loss_occurence
	AND LKP_42X6_STAGE_pif_symbol_pif_policy_number_pif_module_V_loss_year_V_loss_month_V_loss_day_ipfcx6_loss_occ_fdigit_ipfcx6_usr_loss_occurence_V_loss_claimant_V_loss_insurance_line_V_loss_location_number_V_loss_sub_location_number_V_loss_risk_unit_group_V_loss_class_code_group_V_loss_class_code_member_V_loss_unit_V_loss_sequence_risk_unit_V_loss_major_peril_V_loss_major_peril_seq_V_loss_disability_V_loss_member_V_loss_reserve_category_V_loss_cause.ipfcx6_loss_claimant = V_loss_claimant
	AND LKP_42X6_STAGE_pif_symbol_pif_policy_number_pif_module_V_loss_year_V_loss_month_V_loss_day_ipfcx6_loss_occ_fdigit_ipfcx6_usr_loss_occurence_V_loss_claimant_V_loss_insurance_line_V_loss_location_number_V_loss_sub_location_number_V_loss_risk_unit_group_V_loss_class_code_group_V_loss_class_code_member_V_loss_unit_V_loss_sequence_risk_unit_V_loss_major_peril_V_loss_major_peril_seq_V_loss_disability_V_loss_member_V_loss_reserve_category_V_loss_cause.ipfcx6_insurance_line = V_loss_insurance_line
	AND LKP_42X6_STAGE_pif_symbol_pif_policy_number_pif_module_V_loss_year_V_loss_month_V_loss_day_ipfcx6_loss_occ_fdigit_ipfcx6_usr_loss_occurence_V_loss_claimant_V_loss_insurance_line_V_loss_location_number_V_loss_sub_location_number_V_loss_risk_unit_group_V_loss_class_code_group_V_loss_class_code_member_V_loss_unit_V_loss_sequence_risk_unit_V_loss_major_peril_V_loss_major_peril_seq_V_loss_disability_V_loss_member_V_loss_reserve_category_V_loss_cause.ipfcx6_location_number = V_loss_location_number
	AND LKP_42X6_STAGE_pif_symbol_pif_policy_number_pif_module_V_loss_year_V_loss_month_V_loss_day_ipfcx6_loss_occ_fdigit_ipfcx6_usr_loss_occurence_V_loss_claimant_V_loss_insurance_line_V_loss_location_number_V_loss_sub_location_number_V_loss_risk_unit_group_V_loss_class_code_group_V_loss_class_code_member_V_loss_unit_V_loss_sequence_risk_unit_V_loss_major_peril_V_loss_major_peril_seq_V_loss_disability_V_loss_member_V_loss_reserve_category_V_loss_cause.ipfcx6_sub_location_number = V_loss_sub_location_number
	AND LKP_42X6_STAGE_pif_symbol_pif_policy_number_pif_module_V_loss_year_V_loss_month_V_loss_day_ipfcx6_loss_occ_fdigit_ipfcx6_usr_loss_occurence_V_loss_claimant_V_loss_insurance_line_V_loss_location_number_V_loss_sub_location_number_V_loss_risk_unit_group_V_loss_class_code_group_V_loss_class_code_member_V_loss_unit_V_loss_sequence_risk_unit_V_loss_major_peril_V_loss_major_peril_seq_V_loss_disability_V_loss_member_V_loss_reserve_category_V_loss_cause.ipfcx6_risk_unit_group = V_loss_risk_unit_group
	AND LKP_42X6_STAGE_pif_symbol_pif_policy_number_pif_module_V_loss_year_V_loss_month_V_loss_day_ipfcx6_loss_occ_fdigit_ipfcx6_usr_loss_occurence_V_loss_claimant_V_loss_insurance_line_V_loss_location_number_V_loss_sub_location_number_V_loss_risk_unit_group_V_loss_class_code_group_V_loss_class_code_member_V_loss_unit_V_loss_sequence_risk_unit_V_loss_major_peril_V_loss_major_peril_seq_V_loss_disability_V_loss_member_V_loss_reserve_category_V_loss_cause.ipfcx6_class_code_group = V_loss_class_code_group
	AND LKP_42X6_STAGE_pif_symbol_pif_policy_number_pif_module_V_loss_year_V_loss_month_V_loss_day_ipfcx6_loss_occ_fdigit_ipfcx6_usr_loss_occurence_V_loss_claimant_V_loss_insurance_line_V_loss_location_number_V_loss_sub_location_number_V_loss_risk_unit_group_V_loss_class_code_group_V_loss_class_code_member_V_loss_unit_V_loss_sequence_risk_unit_V_loss_major_peril_V_loss_major_peril_seq_V_loss_disability_V_loss_member_V_loss_reserve_category_V_loss_cause.ipfcx6_class_code_member = V_loss_class_code_member
	AND LKP_42X6_STAGE_pif_symbol_pif_policy_number_pif_module_V_loss_year_V_loss_month_V_loss_day_ipfcx6_loss_occ_fdigit_ipfcx6_usr_loss_occurence_V_loss_claimant_V_loss_insurance_line_V_loss_location_number_V_loss_sub_location_number_V_loss_risk_unit_group_V_loss_class_code_group_V_loss_class_code_member_V_loss_unit_V_loss_sequence_risk_unit_V_loss_major_peril_V_loss_major_peril_seq_V_loss_disability_V_loss_member_V_loss_reserve_category_V_loss_cause.ipfcx6_loss_unit = V_loss_unit
	AND LKP_42X6_STAGE_pif_symbol_pif_policy_number_pif_module_V_loss_year_V_loss_month_V_loss_day_ipfcx6_loss_occ_fdigit_ipfcx6_usr_loss_occurence_V_loss_claimant_V_loss_insurance_line_V_loss_location_number_V_loss_sub_location_number_V_loss_risk_unit_group_V_loss_class_code_group_V_loss_class_code_member_V_loss_unit_V_loss_sequence_risk_unit_V_loss_major_peril_V_loss_major_peril_seq_V_loss_disability_V_loss_member_V_loss_reserve_category_V_loss_cause.ipfcx6_risk_sequence = V_loss_sequence_risk_unit
	AND LKP_42X6_STAGE_pif_symbol_pif_policy_number_pif_module_V_loss_year_V_loss_month_V_loss_day_ipfcx6_loss_occ_fdigit_ipfcx6_usr_loss_occurence_V_loss_claimant_V_loss_insurance_line_V_loss_location_number_V_loss_sub_location_number_V_loss_risk_unit_group_V_loss_class_code_group_V_loss_class_code_member_V_loss_unit_V_loss_sequence_risk_unit_V_loss_major_peril_V_loss_major_peril_seq_V_loss_disability_V_loss_member_V_loss_reserve_category_V_loss_cause.ipfcx6_major_peril = V_loss_major_peril
	AND LKP_42X6_STAGE_pif_symbol_pif_policy_number_pif_module_V_loss_year_V_loss_month_V_loss_day_ipfcx6_loss_occ_fdigit_ipfcx6_usr_loss_occurence_V_loss_claimant_V_loss_insurance_line_V_loss_location_number_V_loss_sub_location_number_V_loss_risk_unit_group_V_loss_class_code_group_V_loss_class_code_member_V_loss_unit_V_loss_sequence_risk_unit_V_loss_major_peril_V_loss_major_peril_seq_V_loss_disability_V_loss_member_V_loss_reserve_category_V_loss_cause.ipfcx6_sequence_type_exposure = V_loss_major_peril_seq
	AND LKP_42X6_STAGE_pif_symbol_pif_policy_number_pif_module_V_loss_year_V_loss_month_V_loss_day_ipfcx6_loss_occ_fdigit_ipfcx6_usr_loss_occurence_V_loss_claimant_V_loss_insurance_line_V_loss_location_number_V_loss_sub_location_number_V_loss_risk_unit_group_V_loss_class_code_group_V_loss_class_code_member_V_loss_unit_V_loss_sequence_risk_unit_V_loss_major_peril_V_loss_major_peril_seq_V_loss_disability_V_loss_member_V_loss_reserve_category_V_loss_cause.ipfcx6_loss_disability = V_loss_disability
	AND LKP_42X6_STAGE_pif_symbol_pif_policy_number_pif_module_V_loss_year_V_loss_month_V_loss_day_ipfcx6_loss_occ_fdigit_ipfcx6_usr_loss_occurence_V_loss_claimant_V_loss_insurance_line_V_loss_location_number_V_loss_sub_location_number_V_loss_risk_unit_group_V_loss_class_code_group_V_loss_class_code_member_V_loss_unit_V_loss_sequence_risk_unit_V_loss_major_peril_V_loss_major_peril_seq_V_loss_disability_V_loss_member_V_loss_reserve_category_V_loss_cause.ipfcx6_member = V_loss_member
	AND LKP_42X6_STAGE_pif_symbol_pif_policy_number_pif_module_V_loss_year_V_loss_month_V_loss_day_ipfcx6_loss_occ_fdigit_ipfcx6_usr_loss_occurence_V_loss_claimant_V_loss_insurance_line_V_loss_location_number_V_loss_sub_location_number_V_loss_risk_unit_group_V_loss_class_code_group_V_loss_class_code_member_V_loss_unit_V_loss_sequence_risk_unit_V_loss_major_peril_V_loss_major_peril_seq_V_loss_disability_V_loss_member_V_loss_reserve_category_V_loss_cause.ipfcx6_reserve_category = V_loss_reserve_category
	AND LKP_42X6_STAGE_pif_symbol_pif_policy_number_pif_module_V_loss_year_V_loss_month_V_loss_day_ipfcx6_loss_occ_fdigit_ipfcx6_usr_loss_occurence_V_loss_claimant_V_loss_insurance_line_V_loss_location_number_V_loss_sub_location_number_V_loss_risk_unit_group_V_loss_class_code_group_V_loss_class_code_member_V_loss_unit_V_loss_sequence_risk_unit_V_loss_major_peril_V_loss_major_peril_seq_V_loss_disability_V_loss_member_V_loss_reserve_category_V_loss_cause.ipfcx6_loss_cause = V_loss_cause

),
LKP_arch_pif_42x6 AS (
	SELECT
	arch_pif_42x6_stage_id,
	pif_42x6_stage_id,
	pif_symbol,
	pif_policy_number,
	pif_module,
	ipfcx6_rec_length,
	ipfcx6_action_code,
	ipfcx6_file_id,
	ipfcx6_segment_id,
	ipfcx6_segment_level_code,
	ipfcx6_segment_part_code,
	ipfcx6_sub_part_code,
	ipfcx6_insurance_line,
	ipfcx6_location_number,
	ipfcx6_sub_location_number,
	ipfcx6_risk_unit_group,
	ipfcx6_class_code_group,
	ipfcx6_class_code_member,
	ipfcx6_loss_unit,
	ipfcx6_risk_sequence,
	ipfcx6_risk_type_ind,
	ipfcx6_type_exposure,
	ipfcx6_major_peril,
	ipfcx6_sequence_type_exposure,
	ipfcx6_year_item_effective,
	ipfcx6_month_item_effective,
	ipfcx6_day_item_effective,
	ipfcx6_year_of_loss,
	ipfcx6_month_of_loss,
	ipfcx6_day_of_loss,
	ipfcx6_loss_occ_fdigit,
	ipfcx6_usr_loss_occurence,
	ipfcx6_loss_claimant,
	ipfcx6_member,
	ipfcx6_loss_disability,
	ipfcx6_reserve_category,
	ipfcx6_layer,
	ipfcx6_reins_key_id,
	ipfcx6_reins_co_no,
	ipfcx6_reins_broker,
	ipfcx6_year_process,
	ipfcx6_month_process,
	ipfcx6_day_process,
	ipfcx6_year_change_entry,
	ipfcx6_month_change_entry,
	ipfcx6_day_change_entry,
	ipfcx6_sequence_change_entry,
	ipfcx6_segment_status,
	ipfcx6_entry_operator,
	ipfcx6_claim_type,
	ipfcx6_loss_reported_year,
	ipfcx6_loss_reported_month,
	ipfcx6_loss_reported_day,
	ipfcx6_loss_cause,
	ipfcx6_loss_adjustor_no,
	ipfcx6_loss_adjustor_branch,
	ipfcx6_loss_examiner,
	ipfcx6_other_assignment,
	ipfcx6_account_entered_date,
	ipfcx6_average_reserve_code,
	ipfcx6_loss_handling_office,
	ipfcx6_loss_settling_office,
	ipfcx6_reserve_change_ind,
	ipfcx6_iws_claim_indicator,
	ipfcx6_loss_handling_branch,
	ipfcx6_loss_plant_location,
	ipfcx6_loss_accident_state,
	ipfcx6_loss_aia_codes_1_2,
	ipfcx6_loss_aia_codes_3_4,
	ipfcx6_loss_aia_codes_5_6,
	ipfcx6_aia_sub_code,
	ipfcx6_loss_next_review_date,
	ipfcx6_claim_number_9digit,
	ipfcx6_loss_claim_payee,
	ipfcx6_excess_loss_ind,
	ipfcx6_loss_deductible,
	ipfcx6_loss_start_yr,
	ipfcx6_loss_start_mo,
	ipfcx6_loss_start_da,
	ipfcx6_loss_weekly_wage,
	ipfcx6_loss_payment_rate,
	ipfcx6_loss_frequency,
	ipfcx6_loss_period_pay,
	ipfcx6_sub_line,
	ipfcx6_form_number,
	ipfcx6_prior_direct_loss_stat,
	ipfcx6_chs_scheduled_payment,
	sub_contractor_indicator,
	ipfcx6_loss_payment_made,
	ipfcx6_denial_year,
	ipfcx6_denial_month,
	ipfcx6_denial_day,
	ipfcx6_direct_loss_status,
	ipfcx6_expense_status,
	ipfcx6_loss_recovery_status,
	ipfcx6_salvage_status,
	ipfcx6_subrogation_status,
	ipfcx6_loss_status_year,
	ipfcx6_loss_status_month,
	ipfcx6_loss_status_day,
	ipfcx6_loss_original_reserve,
	ipfcx6_os_loss_reserve,
	ipfcx6_os_expense_reserve,
	ipfcx6_total_loss,
	ipfcx6_total_expense,
	ipfcx6_total_recoveries,
	ipfcx6_beg_os_loss_reserve,
	ipfcx6_seg_id_from_x1,
	ipfcx6_type_claim_payee,
	ipfcx6_claims_made_ind,
	ipfcx6_type_disability,
	ipfcx6_number_of_part78,
	ipfcx6_bank_number,
	ipfcx6_prev_reserve_status,
	ipfcx6_offset_onset_ind,
	ipfcx6_pms_future_use_sub1,
	ipfcx6_cust_spl_use_sub1,
	inf_action,
	inf_timestamp,
	logical_flag,
	IN_pif_symbol,
	IN_pif_policy_number,
	IN_pif_module,
	IN_loss_year,
	IN_loss_month,
	IN_loss_day,
	IN_ipfcx6_loss_occ_fdigit,
	IN_ipfcx6_usr_loss_occurence,
	IN_loss_claimant,
	IN_loss_insurance_line,
	IN_loss_location_number,
	IN_loss_sub_location_number,
	IN_loss_risk_unit_group,
	IN_loss_class_code_group,
	IN_loss_class_code_member,
	IN_loss_unit,
	IN_loss_sequence_risk_unit,
	IN_loss_major_peril,
	IN_loss_major_peril_seq,
	IN_loss_disability,
	IN_loss_member,
	IN_loss_reserve_category,
	IN_loss_cause
	FROM (
		SELECT 
		B.arch_pif_42x6_stage_id as arch_pif_42x6_stage_id, 
		B.pif_42x6_stage_id as pif_42x6_stage_id, 
		B.ipfcx6_rec_length as ipfcx6_rec_length, 
		B.ipfcx6_action_code as ipfcx6_action_code, 
		B.ipfcx6_file_id as ipfcx6_file_id, 
		B.ipfcx6_segment_id as ipfcx6_segment_id, 
		B.ipfcx6_segment_level_code as ipfcx6_segment_level_code, 
		B.ipfcx6_segment_part_code as ipfcx6_segment_part_code, 
		B.ipfcx6_sub_part_code as ipfcx6_sub_part_code, 
		B.ipfcx6_risk_type_ind as ipfcx6_risk_type_ind, 
		B.ipfcx6_type_exposure as ipfcx6_type_exposure, 
		B.ipfcx6_year_item_effective as ipfcx6_year_item_effective, 
		B.ipfcx6_month_item_effective as ipfcx6_month_item_effective,
		B.ipfcx6_day_item_effective as ipfcx6_day_item_effective, 
		B.ipfcx6_layer as ipfcx6_layer, 
		B.ipfcx6_reins_key_id as ipfcx6_reins_key_id, 
		B.ipfcx6_reins_co_no as ipfcx6_reins_co_no, 
		B.ipfcx6_reins_broker as ipfcx6_reins_broker, 
		B.ipfcx6_year_process as ipfcx6_year_process, 
		B.ipfcx6_month_process as ipfcx6_month_process, 
		B.ipfcx6_day_process as ipfcx6_day_process, 
		B.ipfcx6_year_change_entry as ipfcx6_year_change_entry, 
		B.ipfcx6_month_change_entry as ipfcx6_month_change_entry, 
		B.ipfcx6_day_change_entry as ipfcx6_day_change_entry, 
		B.ipfcx6_sequence_change_entry as ipfcx6_sequence_change_entry, 
		B.ipfcx6_segment_status as ipfcx6_segment_status, 
		B.ipfcx6_entry_operator as ipfcx6_entry_operator, 
		B.ipfcx6_claim_type as ipfcx6_claim_type, 
		B.ipfcx6_loss_reported_year as ipfcx6_loss_reported_year, 
		B.ipfcx6_loss_reported_month as ipfcx6_loss_reported_month, 
		B.ipfcx6_loss_reported_day as ipfcx6_loss_reported_day, 
		B.ipfcx6_loss_adjustor_no as ipfcx6_loss_adjustor_no, 
		B.ipfcx6_loss_adjustor_branch as ipfcx6_loss_adjustor_branch, 
		B.ipfcx6_loss_examiner as ipfcx6_loss_examiner, 
		B.ipfcx6_other_assignment as ipfcx6_other_assignment, 
		B.ipfcx6_account_entered_date as ipfcx6_account_entered_date, 
		B.ipfcx6_average_reserve_code as ipfcx6_average_reserve_code, 
		B.ipfcx6_loss_handling_office as ipfcx6_loss_handling_office, 
		B.ipfcx6_loss_settling_office as ipfcx6_loss_settling_office, 
		B.ipfcx6_reserve_change_ind as ipfcx6_reserve_change_ind, 
		B.ipfcx6_iws_claim_indicator as ipfcx6_iws_claim_indicator, 
		B.ipfcx6_loss_handling_branch as ipfcx6_loss_handling_branch, 
		B.ipfcx6_loss_plant_location as ipfcx6_loss_plant_location, 
		B.ipfcx6_loss_accident_state as ipfcx6_loss_accident_state, 
		B.ipfcx6_loss_aia_codes_1_2 as ipfcx6_loss_aia_codes_1_2, 
		B.ipfcx6_loss_aia_codes_3_4 as ipfcx6_loss_aia_codes_3_4, 
		B.ipfcx6_loss_aia_codes_5_6 as ipfcx6_loss_aia_codes_5_6, 
		B.ipfcx6_aia_sub_code as ipfcx6_aia_sub_code, 
		B.ipfcx6_loss_next_review_date as ipfcx6_loss_next_review_date, 
		B.ipfcx6_claim_number_9digit as ipfcx6_claim_number_9digit, 
		B.ipfcx6_loss_claim_payee as ipfcx6_loss_claim_payee, 
		B.ipfcx6_excess_loss_ind as ipfcx6_excess_loss_ind, 
		B.ipfcx6_loss_deductible as ipfcx6_loss_deductible, 
		B.ipfcx6_loss_start_yr as ipfcx6_loss_start_yr, 
		B.ipfcx6_loss_start_mo as ipfcx6_loss_start_mo, 
		B.ipfcx6_loss_start_da as ipfcx6_loss_start_da, 
		B.ipfcx6_loss_weekly_wage as ipfcx6_loss_weekly_wage, 
		B.ipfcx6_loss_payment_rate as ipfcx6_loss_payment_rate, 
		B.ipfcx6_loss_frequency as ipfcx6_loss_frequency, 
		B.ipfcx6_loss_period_pay as ipfcx6_loss_period_pay, 
		B.ipfcx6_sub_line as ipfcx6_sub_line, 
		B.ipfcx6_form_number as ipfcx6_form_number, 
		B.ipfcx6_prior_direct_loss_stat as ipfcx6_prior_direct_loss_stat, 
		B.ipfcx6_chs_scheduled_payment as ipfcx6_chs_scheduled_payment, 
		B.sub_contractor_indicator as sub_contractor_indicator, 
		B.ipfcx6_loss_payment_made as ipfcx6_loss_payment_made, 
		B.ipfcx6_denial_year as ipfcx6_denial_year, 
		B.ipfcx6_denial_month as ipfcx6_denial_month, 
		B.ipfcx6_denial_day as ipfcx6_denial_day, 
		B.ipfcx6_direct_loss_status as ipfcx6_direct_loss_status, 
		B.ipfcx6_expense_status as ipfcx6_expense_status, 
		B.ipfcx6_loss_recovery_status as ipfcx6_loss_recovery_status, 
		B.ipfcx6_salvage_status as ipfcx6_salvage_status, 
		B.ipfcx6_subrogation_status as ipfcx6_subrogation_status, 
		B.ipfcx6_loss_status_year as ipfcx6_loss_status_year, 
		B.ipfcx6_loss_status_month as ipfcx6_loss_status_month, 
		B.ipfcx6_loss_status_day as ipfcx6_loss_status_day, 
		B.ipfcx6_loss_original_reserve as ipfcx6_loss_original_reserve, 
		B.ipfcx6_os_loss_reserve as ipfcx6_os_loss_reserve, 
		B.ipfcx6_os_expense_reserve as ipfcx6_os_expense_reserve, 
		B.ipfcx6_total_loss as ipfcx6_total_loss, 
		B.ipfcx6_total_expense as ipfcx6_total_expense, 
		B.ipfcx6_total_recoveries as ipfcx6_total_recoveries, 
		B.ipfcx6_beg_os_loss_reserve as ipfcx6_beg_os_loss_reserve, 
		B.ipfcx6_seg_id_from_x1 as ipfcx6_seg_id_from_x1, 
		B.ipfcx6_type_claim_payee as ipfcx6_type_claim_payee, 
		B.ipfcx6_claims_made_ind as ipfcx6_claims_made_ind, 
		B.ipfcx6_type_disability as ipfcx6_type_disability, 
		B.ipfcx6_number_of_part78 as ipfcx6_number_of_part78, 
		B.ipfcx6_bank_number as ipfcx6_bank_number, 
		B.ipfcx6_prev_reserve_status as ipfcx6_prev_reserve_status, 
		B.ipfcx6_offset_onset_ind as ipfcx6_offset_onset_ind, 
		B.ipfcx6_pms_future_use_sub1 as ipfcx6_pms_future_use_sub1, 
		B.ipfcx6_cust_spl_use_sub1 as ipfcx6_cust_spl_use_sub1, 
		B.inf_action as inf_action, B.inf_timestamp as inf_timestamp, 
		B.logical_flag as logical_flag, 
		CASE LEN(LTRIM(RTRIM(COALESCE(pif_symbol,' '))))
		                WHEN 0
		                THEN 'N/A'
		                ELSE LTRIM(RTRIM(pif_symbol))
		        END AS pif_symbol,
		        CASE LEN(LTRIM(RTRIM(COALESCE(pif_policy_number,' '))))
		                WHEN 0
		                THEN '0'
		                ELSE LTRIM(RTRIM(pif_policy_number))
		        END AS pif_policy_number,
		        CASE LEN(LTRIM(RTRIM(COALESCE(pif_module, ' '))))
		                WHEN 0
		                THEN '0'
		                ELSE LTRIM(RTRIM(pif_module))
		        END AS pif_module,
		        CASE LEN(COALESCE(ipfcx6_year_of_loss, '1800'))
		                WHEN 0
		                THEN '1800'
		                ELSE COALESCE(ipfcx6_year_of_loss, '1800')
		        END AS ipfcx6_year_of_loss,
		        CASE LEN(COALESCE(ipfcx6_month_of_loss,'1'))
		                WHEN 0
		                THEN '1'
		                ELSE COALESCE(ipfcx6_month_of_loss,'1')
		        END AS ipfcx6_month_of_loss,
		        CASE LEN(COALESCE(ipfcx6_day_of_loss, '1'))
		                WHEN 0
		                THEN '1'
		                ELSE COALESCE(ipfcx6_day_of_loss,'1')
		        END AS ipfcx6_day_of_loss,
		        CASE LEN(COALESCE(ipfcx6_loss_occ_fdigit,'0'))
		                WHEN 0
		                THEN '0'
		                ELSE COALESCE(ipfcx6_loss_occ_fdigit,'0')
		        END AS ipfcx6_loss_occ_fdigit,
		        CASE LEN(COALESCE(ipfcx6_usr_loss_occurence,'0'))
		                WHEN 0
		                THEN '0'
		                ELSE COALESCE(ipfcx6_usr_loss_occurence,'0')
		        END AS ipfcx6_usr_loss_occurence,
		        CASE LEN(COALESCE(ipfcx6_loss_claimant,'0'))
		                WHEN 0
		                THEN '0'
		                ELSE COALESCE(ipfcx6_loss_claimant,'0')
		        END AS ipfcx6_loss_claimant,
		        CASE LEN(LTRIM(RTRIM(COALESCE(ipfcx6_insurance_line,' '))))
		                WHEN 0
		                THEN 'N/A'
		                ELSE LTRIM(RTRIM(ipfcx6_insurance_line))
		        END AS ipfcx6_insurance_line,
		        CASE LEN(COALESCE(ipfcx6_location_number,'0'))
		                WHEN 0
		                THEN '0'
		                ELSE COALESCE(ipfcx6_location_number,'0')
		        END AS ipfcx6_location_number,
		        CASE LEN(COALESCE(ipfcx6_sub_location_number,'0'))
		                WHEN 0
		                THEN '0'
		                ELSE COALESCE(ipfcx6_sub_location_number,'0')
		        END AS ipfcx6_sub_location_number,
		        CASE LEN(LTRIM(RTRIM(COALESCE(ipfcx6_risk_unit_group,' '))))
		                WHEN 0
		                THEN '0'
		                ELSE LTRIM(RTRIM(ipfcx6_risk_unit_group))
		        END AS ipfcx6_risk_unit_group,
		        CASE LEN(COALESCE(ipfcx6_class_code_group,'0'))
		                WHEN 0
		                THEN '0'
		                ELSE COALESCE(ipfcx6_class_code_group,'0')
		        END AS ipfcx6_class_code_group,
		        CASE LEN(COALESCE(ipfcx6_class_code_member,'0'))
		                WHEN 0
		                THEN '0'
		                ELSE COALESCE(ipfcx6_class_code_member,'0')
		        END AS ipfcx6_class_code_member,
		        CASE LEN(LTRIM(RTRIM(COALESCE(ipfcx6_loss_unit,' '))))
		                WHEN 0
		                THEN '0'
		                ELSE LTRIM(RTRIM(ipfcx6_loss_unit))
		        END AS ipfcx6_loss_unit,
		        CASE LEN(COALESCE(ipfcx6_risk_sequence,'0'))
		                WHEN 0
		                THEN '0'
		                ELSE COALESCE(ipfcx6_risk_sequence,'0')
		        END AS ipfcx6_risk_sequence,
		        CASE LEN(LTRIM(RTRIM(COALESCE(ipfcx6_major_peril,' '))))
		                WHEN 0
		                THEN '0'
		                ELSE LTRIM(RTRIM(ipfcx6_major_peril))
		        END AS ipfcx6_major_peril,
		        CASE LEN(LTRIM(RTRIM(COALESCE(ipfcx6_sequence_type_exposure, ' '))))
		                WHEN 0
		                THEN '0'
		                ELSE LTRIM(RTRIM(ipfcx6_sequence_type_exposure))
		        END AS ipfcx6_sequence_type_exposure,
		        CASE LEN(LTRIM(RTRIM(COALESCE(ipfcx6_loss_disability,' '))))
		                WHEN 0
		                THEN '0'
		                ELSE LTRIM(RTRIM(ipfcx6_loss_disability))
		        END AS ipfcx6_loss_disability,
		        CASE LEN(LTRIM(RTRIM(COALESCE(ipfcx6_member,' '))))
		                WHEN 0
		                THEN '0'
		                ELSE LTRIM(RTRIM(ipfcx6_member))
		        END AS ipfcx6_member,
		        CASE LEN(LTRIM(RTRIM(COALESCE(ipfcx6_reserve_category,' '))))
		                WHEN 0
		                THEN '0'
		                ELSE LTRIM(RTRIM( ipfcx6_reserve_category))
		        END AS ipfcx6_reserve_category,
		        CASE LEN(LTRIM(RTRIM(COALESCE(ipfcx6_loss_cause,' '))))
		                WHEN 0
		                THEN '0'
		                ELSE LTRIM(RTRIM( ipfcx6_loss_cause))
		        END AS ipfcx6_loss_cause FROM arch_pif_42x6_stage B
		WHERE 
		B.pif_symbol+B.pif_policy_number+B.pif_module IN 
		(SELECT DISTINCT A.pif_symbol+A.pif_policy_number+A.pif_module FROM pif_4578_stage A)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module,ipfcx6_year_of_loss,ipfcx6_month_of_loss,ipfcx6_day_of_loss,ipfcx6_loss_occ_fdigit,ipfcx6_usr_loss_occurence,ipfcx6_loss_claimant,ipfcx6_insurance_line,ipfcx6_location_number,ipfcx6_sub_location_number,ipfcx6_risk_unit_group,ipfcx6_class_code_group,ipfcx6_class_code_member,ipfcx6_loss_unit,ipfcx6_risk_sequence,ipfcx6_major_peril,ipfcx6_sequence_type_exposure,ipfcx6_loss_disability,ipfcx6_member,ipfcx6_reserve_category,ipfcx6_loss_cause ORDER BY arch_pif_42x6_stage_id) = 1
),
RTR_CLM_STG_Update AS (
	SELECT
	EXP_CLM_STG_Update_42GP.pif_42x6_stage_id,
	LKP_arch_pif_42x6.pif_42x6_stage_id AS lkp_pif_42x6_stage_id,
	LKP_arch_pif_42x6.logical_flag AS lkp_logical_flag,
	EXP_CLM_STG_Update_42GP.pif_symbol,
	EXP_CLM_STG_Update_42GP.pif_policy_number,
	EXP_CLM_STG_Update_42GP.pif_module,
	EXP_CLM_STG_Update_42GP.loss_insurance_line,
	EXP_CLM_STG_Update_42GP.loss_location_number,
	EXP_CLM_STG_Update_42GP.loss_sub_location_number,
	EXP_CLM_STG_Update_42GP.loss_risk_unit_group,
	EXP_CLM_STG_Update_42GP.loss_class_code_group,
	EXP_CLM_STG_Update_42GP.loss_class_code_member,
	EXP_CLM_STG_Update_42GP.loss_unit,
	EXP_CLM_STG_Update_42GP.loss_sequence_risk_unit,
	EXP_CLM_STG_Update_42GP.loss_major_peril,
	EXP_CLM_STG_Update_42GP.loss_major_peril_seq,
	EXP_CLM_STG_Update_42GP.loss_year,
	EXP_CLM_STG_Update_42GP.loss_month,
	EXP_CLM_STG_Update_42GP.loss_day,
	EXP_CLM_STG_Update_42GP.loss_occurence,
	EXP_CLM_STG_Update_42GP.loss_claimant,
	EXP_CLM_STG_Update_42GP.loss_member,
	EXP_CLM_STG_Update_42GP.loss_disability,
	EXP_CLM_STG_Update_42GP.loss_reserve_category,
	EXP_CLM_STG_Update_42GP.loss_cause,
	EXP_CLM_STG_Update_42GP.loss_offset_onset_ind,
	EXP_CLM_STG_Update_42GP.logical_flag,
	EXP_CLM_STG_Update_42GP.logical_flag_insert,
	EXP_CLM_STG_Update_42GP.source_system_id,
	LKP_arch_pif_42x6.pif_symbol AS lkp_pif_symbol,
	LKP_arch_pif_42x6.pif_policy_number AS lkp_pif_policy_number,
	LKP_arch_pif_42x6.pif_module AS lkp_pif_module,
	LKP_arch_pif_42x6.ipfcx6_rec_length AS lkp_ipfcx6_rec_length,
	LKP_arch_pif_42x6.ipfcx6_action_code AS lkp_ipfcx6_action_code,
	LKP_arch_pif_42x6.ipfcx6_file_id AS lkp_ipfcx6_file_id,
	LKP_arch_pif_42x6.ipfcx6_segment_id AS lkp_ipfcx6_segment_id,
	LKP_arch_pif_42x6.ipfcx6_segment_level_code AS lkp_ipfcx6_segment_level_code,
	LKP_arch_pif_42x6.ipfcx6_segment_part_code AS lkp_ipfcx6_segment_part_code,
	LKP_arch_pif_42x6.ipfcx6_sub_part_code AS lkp_ipfcx6_sub_part_code,
	LKP_arch_pif_42x6.ipfcx6_insurance_line AS lkp_ipfcx6_insurance_line,
	LKP_arch_pif_42x6.ipfcx6_location_number AS lkp_ipfcx6_location_number,
	LKP_arch_pif_42x6.ipfcx6_sub_location_number AS lkp_ipfcx6_sub_location_number,
	LKP_arch_pif_42x6.ipfcx6_risk_unit_group AS lkp_ipfcx6_risk_unit_group,
	LKP_arch_pif_42x6.ipfcx6_class_code_group AS lkp_ipfcx6_class_code_group,
	LKP_arch_pif_42x6.ipfcx6_class_code_member AS lkp_ipfcx6_class_code_member,
	LKP_arch_pif_42x6.ipfcx6_loss_unit AS lkp_ipfcx6_loss_unit,
	LKP_arch_pif_42x6.ipfcx6_risk_sequence AS lkp_ipfcx6_risk_sequence,
	LKP_arch_pif_42x6.ipfcx6_risk_type_ind AS lkp_ipfcx6_risk_type_ind,
	LKP_arch_pif_42x6.ipfcx6_type_exposure AS lkp_ipfcx6_type_exposure,
	LKP_arch_pif_42x6.ipfcx6_major_peril AS lkp_ipfcx6_major_peril,
	LKP_arch_pif_42x6.ipfcx6_sequence_type_exposure AS lkp_ipfcx6_sequence_type_exposure,
	LKP_arch_pif_42x6.ipfcx6_year_item_effective AS lkp_ipfcx6_year_item_effective,
	LKP_arch_pif_42x6.ipfcx6_month_item_effective AS lkp_ipfcx6_month_item_effective,
	LKP_arch_pif_42x6.ipfcx6_day_item_effective AS lkp_ipfcx6_day_item_effective,
	LKP_arch_pif_42x6.ipfcx6_year_of_loss AS lkp_ipfcx6_year_of_loss,
	LKP_arch_pif_42x6.ipfcx6_month_of_loss AS lkp_ipfcx6_month_of_loss,
	LKP_arch_pif_42x6.ipfcx6_day_of_loss AS lkp_ipfcx6_day_of_loss,
	LKP_arch_pif_42x6.ipfcx6_loss_occ_fdigit AS lkp_ipfcx6_loss_occ_fdigit,
	LKP_arch_pif_42x6.ipfcx6_usr_loss_occurence AS lkp_ipfcx6_usr_loss_occurence,
	LKP_arch_pif_42x6.ipfcx6_loss_claimant AS lkp_ipfcx6_loss_claimant,
	LKP_arch_pif_42x6.ipfcx6_member AS lkp_ipfcx6_member,
	LKP_arch_pif_42x6.ipfcx6_loss_disability AS lkp_ipfcx6_loss_disability,
	LKP_arch_pif_42x6.ipfcx6_reserve_category AS lkp_ipfcx6_reserve_category,
	LKP_arch_pif_42x6.ipfcx6_layer AS lkp_ipfcx6_layer,
	LKP_arch_pif_42x6.ipfcx6_reins_key_id AS lkp_ipfcx6_reins_key_id,
	LKP_arch_pif_42x6.ipfcx6_reins_co_no AS lkp_ipfcx6_reins_co_no,
	LKP_arch_pif_42x6.ipfcx6_reins_broker AS lkp_ipfcx6_reins_broker,
	LKP_arch_pif_42x6.ipfcx6_year_process AS lkp_ipfcx6_year_process,
	LKP_arch_pif_42x6.ipfcx6_month_process AS lkp_ipfcx6_month_process,
	LKP_arch_pif_42x6.ipfcx6_day_process AS lkp_ipfcx6_day_process,
	LKP_arch_pif_42x6.ipfcx6_year_change_entry AS lkp_ipfcx6_year_change_entry,
	LKP_arch_pif_42x6.ipfcx6_month_change_entry AS lkp_ipfcx6_month_change_entry,
	LKP_arch_pif_42x6.ipfcx6_day_change_entry AS lkp_ipfcx6_day_change_entry,
	LKP_arch_pif_42x6.ipfcx6_sequence_change_entry AS lkp_ipfcx6_sequence_change_entry,
	LKP_arch_pif_42x6.ipfcx6_segment_status AS lkp_ipfcx6_segment_status,
	LKP_arch_pif_42x6.ipfcx6_entry_operator AS lkp_ipfcx6_entry_operator,
	LKP_arch_pif_42x6.ipfcx6_claim_type AS lkp_ipfcx6_claim_type,
	LKP_arch_pif_42x6.ipfcx6_loss_reported_year AS lkp_ipfcx6_loss_reported_year,
	LKP_arch_pif_42x6.ipfcx6_loss_reported_month AS lkp_ipfcx6_loss_reported_month,
	LKP_arch_pif_42x6.ipfcx6_loss_reported_day AS lkp_ipfcx6_loss_reported_day,
	LKP_arch_pif_42x6.ipfcx6_loss_cause AS lkp_ipfcx6_loss_cause,
	LKP_arch_pif_42x6.ipfcx6_loss_adjustor_no AS lkp_ipfcx6_loss_adjustor_no,
	LKP_arch_pif_42x6.ipfcx6_loss_adjustor_branch AS lkp_ipfcx6_loss_adjustor_branch,
	LKP_arch_pif_42x6.ipfcx6_loss_examiner AS lkp_ipfcx6_loss_examiner,
	LKP_arch_pif_42x6.ipfcx6_other_assignment AS lkp_ipfcx6_other_assignment,
	LKP_arch_pif_42x6.ipfcx6_account_entered_date AS lkp_ipfcx6_account_entered_date,
	LKP_arch_pif_42x6.ipfcx6_average_reserve_code AS lkp_ipfcx6_average_reserve_code,
	LKP_arch_pif_42x6.ipfcx6_loss_handling_office AS lkp_ipfcx6_loss_handling_office,
	LKP_arch_pif_42x6.ipfcx6_loss_settling_office AS lkp_ipfcx6_loss_settling_office,
	LKP_arch_pif_42x6.ipfcx6_reserve_change_ind AS lkp_ipfcx6_reserve_change_ind,
	LKP_arch_pif_42x6.ipfcx6_iws_claim_indicator AS lkp_ipfcx6_iws_claim_indicator,
	LKP_arch_pif_42x6.ipfcx6_loss_handling_branch AS lkp_ipfcx6_loss_handling_branch,
	LKP_arch_pif_42x6.ipfcx6_loss_plant_location AS lkp_ipfcx6_loss_plant_location,
	LKP_arch_pif_42x6.ipfcx6_loss_accident_state AS lkp_ipfcx6_loss_accident_state,
	LKP_arch_pif_42x6.ipfcx6_loss_aia_codes_1_2 AS lkp_ipfcx6_loss_aia_codes_1_2,
	LKP_arch_pif_42x6.ipfcx6_loss_aia_codes_3_4 AS lkp_ipfcx6_loss_aia_codes_3_4,
	LKP_arch_pif_42x6.ipfcx6_loss_aia_codes_5_6 AS lkp_ipfcx6_loss_aia_codes_5_6,
	LKP_arch_pif_42x6.ipfcx6_aia_sub_code AS lkp_ipfcx6_aia_sub_code,
	LKP_arch_pif_42x6.ipfcx6_loss_next_review_date AS lkp_ipfcx6_loss_next_review_date,
	LKP_arch_pif_42x6.ipfcx6_claim_number_9digit AS lkp_ipfcx6_claim_number_9digit,
	LKP_arch_pif_42x6.ipfcx6_loss_claim_payee AS lkp_ipfcx6_loss_claim_payee,
	LKP_arch_pif_42x6.ipfcx6_excess_loss_ind AS lkp_ipfcx6_excess_loss_ind,
	LKP_arch_pif_42x6.ipfcx6_loss_deductible AS lkp_ipfcx6_loss_deductible,
	LKP_arch_pif_42x6.ipfcx6_loss_start_yr AS lkp_ipfcx6_loss_start_yr,
	LKP_arch_pif_42x6.ipfcx6_loss_start_mo AS lkp_ipfcx6_loss_start_mo,
	LKP_arch_pif_42x6.ipfcx6_loss_start_da AS lkp_ipfcx6_loss_start_da,
	LKP_arch_pif_42x6.ipfcx6_loss_weekly_wage AS lkp_ipfcx6_loss_weekly_wage,
	LKP_arch_pif_42x6.ipfcx6_loss_payment_rate AS lkp_ipfcx6_loss_payment_rate,
	LKP_arch_pif_42x6.ipfcx6_loss_frequency AS lkp_ipfcx6_loss_frequency,
	LKP_arch_pif_42x6.ipfcx6_loss_period_pay AS lkp_ipfcx6_loss_period_pay,
	LKP_arch_pif_42x6.ipfcx6_sub_line AS lkp_ipfcx6_sub_line,
	LKP_arch_pif_42x6.ipfcx6_form_number AS lkp_ipfcx6_form_number,
	LKP_arch_pif_42x6.ipfcx6_prior_direct_loss_stat AS lkp_ipfcx6_prior_direct_loss_stat,
	LKP_arch_pif_42x6.ipfcx6_chs_scheduled_payment AS lkp_ipfcx6_chs_scheduled_payment,
	LKP_arch_pif_42x6.sub_contractor_indicator AS lkp_sub_contractor_indicator,
	LKP_arch_pif_42x6.ipfcx6_loss_payment_made AS lkp_ipfcx6_loss_payment_made,
	LKP_arch_pif_42x6.ipfcx6_denial_year AS lkp_ipfcx6_denial_year,
	LKP_arch_pif_42x6.ipfcx6_denial_month AS lkp_ipfcx6_denial_month,
	LKP_arch_pif_42x6.ipfcx6_denial_day AS lkp_ipfcx6_denial_day,
	LKP_arch_pif_42x6.ipfcx6_direct_loss_status AS lkp_ipfcx6_direct_loss_status,
	LKP_arch_pif_42x6.ipfcx6_expense_status AS lkp_ipfcx6_expense_status,
	LKP_arch_pif_42x6.ipfcx6_loss_recovery_status AS lkp_ipfcx6_loss_recovery_status,
	LKP_arch_pif_42x6.ipfcx6_salvage_status AS lkp_ipfcx6_salvage_status,
	LKP_arch_pif_42x6.ipfcx6_subrogation_status AS lkp_ipfcx6_subrogation_status,
	LKP_arch_pif_42x6.ipfcx6_loss_status_year AS lkp_ipfcx6_loss_status_year,
	LKP_arch_pif_42x6.ipfcx6_loss_status_month AS lkp_ipfcx6_loss_status_month,
	LKP_arch_pif_42x6.ipfcx6_loss_status_day AS lkp_ipfcx6_loss_status_day,
	LKP_arch_pif_42x6.ipfcx6_loss_original_reserve AS lkp_ipfcx6_loss_original_reserve,
	LKP_arch_pif_42x6.ipfcx6_os_loss_reserve AS lkp_ipfcx6_os_loss_reserve,
	LKP_arch_pif_42x6.ipfcx6_os_expense_reserve AS lkp_ipfcx6_os_expense_reserve,
	LKP_arch_pif_42x6.ipfcx6_total_loss AS lkp_ipfcx6_total_loss,
	LKP_arch_pif_42x6.ipfcx6_total_expense AS lkp_ipfcx6_total_expense,
	LKP_arch_pif_42x6.ipfcx6_total_recoveries AS lkp_ipfcx6_total_recoveries,
	LKP_arch_pif_42x6.ipfcx6_beg_os_loss_reserve AS lkp_ipfcx6_beg_os_loss_reserve,
	LKP_arch_pif_42x6.ipfcx6_seg_id_from_x1 AS lkp_ipfcx6_seg_id_from_x1,
	LKP_arch_pif_42x6.ipfcx6_type_claim_payee AS lkp_ipfcx6_type_claim_payee,
	LKP_arch_pif_42x6.ipfcx6_claims_made_ind AS lkp_ipfcx6_claims_made_ind,
	LKP_arch_pif_42x6.ipfcx6_type_disability AS lkp_ipfcx6_type_disability,
	LKP_arch_pif_42x6.ipfcx6_number_of_part78 AS lkp_ipfcx6_number_of_part78,
	LKP_arch_pif_42x6.ipfcx6_bank_number AS lkp_ipfcx6_bank_number,
	LKP_arch_pif_42x6.ipfcx6_prev_reserve_status AS lkp_ipfcx6_prev_reserve_status,
	LKP_arch_pif_42x6.ipfcx6_offset_onset_ind AS lkp_ipfcx6_offset_onset_ind,
	LKP_arch_pif_42x6.ipfcx6_pms_future_use_sub1 AS lkp_ipfcx6_pms_future_use_sub1,
	LKP_arch_pif_42x6.ipfcx6_cust_spl_use_sub1 AS lkp_ipfcx6_cust_spl_use_sub1,
	LKP_arch_pif_42x6.inf_action AS lkp_inf_action,
	LKP_arch_pif_42x6.inf_timestamp AS lkp_inf_timestamp
	FROM EXP_CLM_STG_Update_42GP
	LEFT JOIN LKP_arch_pif_42x6
	ON LKP_arch_pif_42x6.pif_symbol = EXP_CLM_STG_Update_42GP.pif_symbol AND LKP_arch_pif_42x6.pif_policy_number = EXP_CLM_STG_Update_42GP.pif_policy_number AND LKP_arch_pif_42x6.pif_module = EXP_CLM_STG_Update_42GP.pif_module AND LKP_arch_pif_42x6.ipfcx6_year_of_loss = EXP_CLM_STG_Update_42GP.out_loss_year AND LKP_arch_pif_42x6.ipfcx6_month_of_loss = EXP_CLM_STG_Update_42GP.out_loss_month AND LKP_arch_pif_42x6.ipfcx6_day_of_loss = EXP_CLM_STG_Update_42GP.out_loss_day AND LKP_arch_pif_42x6.ipfcx6_loss_occ_fdigit = EXP_CLM_STG_Update_42GP.out_ipfcx6_loss_occ_fdigit AND LKP_arch_pif_42x6.ipfcx6_usr_loss_occurence = EXP_CLM_STG_Update_42GP.out_ipfcx6_usr_loss_occurence AND LKP_arch_pif_42x6.ipfcx6_loss_claimant = EXP_CLM_STG_Update_42GP.out_loss_claimant AND LKP_arch_pif_42x6.ipfcx6_insurance_line = EXP_CLM_STG_Update_42GP.out_loss_insurance_line AND LKP_arch_pif_42x6.ipfcx6_location_number = EXP_CLM_STG_Update_42GP.out_loss_location_number AND LKP_arch_pif_42x6.ipfcx6_sub_location_number = EXP_CLM_STG_Update_42GP.out_loss_sub_location_number AND LKP_arch_pif_42x6.ipfcx6_risk_unit_group = EXP_CLM_STG_Update_42GP.out_loss_risk_unit_group AND LKP_arch_pif_42x6.ipfcx6_class_code_group = EXP_CLM_STG_Update_42GP.out_loss_class_code_group AND LKP_arch_pif_42x6.ipfcx6_class_code_member = EXP_CLM_STG_Update_42GP.out_loss_class_code_member AND LKP_arch_pif_42x6.ipfcx6_loss_unit = EXP_CLM_STG_Update_42GP.out_loss_unit AND LKP_arch_pif_42x6.ipfcx6_risk_sequence = EXP_CLM_STG_Update_42GP.out_loss_sequence_risk_unit AND LKP_arch_pif_42x6.ipfcx6_major_peril = EXP_CLM_STG_Update_42GP.out_loss_major_peril AND LKP_arch_pif_42x6.ipfcx6_sequence_type_exposure = EXP_CLM_STG_Update_42GP.out_loss_major_peril_seq AND LKP_arch_pif_42x6.ipfcx6_loss_disability = EXP_CLM_STG_Update_42GP.out_loss_disability AND LKP_arch_pif_42x6.ipfcx6_member = EXP_CLM_STG_Update_42GP.out_loss_member AND LKP_arch_pif_42x6.ipfcx6_reserve_category = EXP_CLM_STG_Update_42GP.out_loss_reserve_category AND LKP_arch_pif_42x6.ipfcx6_loss_cause = EXP_CLM_STG_Update_42GP.out_loss_cause
),
RTR_CLM_STG_Update_INSERT_DUMMY AS (SELECT * FROM RTR_CLM_STG_Update WHERE ISNULL(pif_42x6_stage_id) AND (ISNULL(lkp_pif_42x6_stage_id) OR NOT IN(lkp_logical_flag, '0', '1'))),
RTR_CLM_STG_Update_UPDATE AS (SELECT * FROM RTR_CLM_STG_Update WHERE NOT ISNULL(pif_42x6_stage_id)),
RTR_CLM_STG_Update_INSERT AS (SELECT * FROM RTR_CLM_STG_Update WHERE FALSE
--ISNULL(pif_42x6_stage_id) AND NOT ISNULL(lkp_pif_42x6_stage_id) AND NOT IN(lkp_logical_flag, '0', '1' )),
EXP_CLM_STG_insert AS (
	SELECT
	logical_flag AS logical_flag_4578,
	lkp_pif_symbol,
	lkp_pif_policy_number,
	lkp_pif_module,
	lkp_ipfcx6_rec_length,
	lkp_ipfcx6_action_code,
	lkp_ipfcx6_file_id,
	lkp_ipfcx6_segment_id,
	lkp_ipfcx6_segment_level_code,
	lkp_ipfcx6_segment_part_code,
	lkp_ipfcx6_sub_part_code,
	lkp_ipfcx6_insurance_line,
	lkp_ipfcx6_location_number,
	lkp_ipfcx6_sub_location_number,
	lkp_ipfcx6_risk_unit_group,
	lkp_ipfcx6_class_code_group,
	lkp_ipfcx6_class_code_member,
	lkp_ipfcx6_loss_unit,
	lkp_ipfcx6_risk_sequence,
	lkp_ipfcx6_risk_type_ind,
	lkp_ipfcx6_type_exposure,
	lkp_ipfcx6_major_peril,
	lkp_ipfcx6_sequence_type_exposure,
	lkp_ipfcx6_year_item_effective,
	lkp_ipfcx6_month_item_effective,
	lkp_ipfcx6_day_item_effective,
	lkp_ipfcx6_year_of_loss,
	lkp_ipfcx6_month_of_loss,
	lkp_ipfcx6_day_of_loss,
	lkp_ipfcx6_loss_occ_fdigit,
	lkp_ipfcx6_usr_loss_occurence,
	lkp_ipfcx6_loss_claimant,
	lkp_ipfcx6_member,
	lkp_ipfcx6_loss_disability,
	lkp_ipfcx6_reserve_category,
	lkp_ipfcx6_layer,
	lkp_ipfcx6_reins_key_id,
	lkp_ipfcx6_reins_co_no,
	lkp_ipfcx6_reins_broker,
	lkp_ipfcx6_year_process,
	lkp_ipfcx6_month_process,
	lkp_ipfcx6_day_process,
	lkp_ipfcx6_year_change_entry,
	lkp_ipfcx6_month_change_entry,
	lkp_ipfcx6_day_change_entry,
	lkp_ipfcx6_sequence_change_entry,
	lkp_ipfcx6_segment_status,
	lkp_ipfcx6_entry_operator,
	lkp_ipfcx6_claim_type,
	lkp_ipfcx6_loss_reported_year,
	lkp_ipfcx6_loss_reported_month,
	lkp_ipfcx6_loss_reported_day,
	lkp_ipfcx6_loss_cause,
	lkp_ipfcx6_loss_adjustor_no,
	lkp_ipfcx6_loss_adjustor_branch,
	lkp_ipfcx6_loss_examiner,
	lkp_ipfcx6_other_assignment,
	lkp_ipfcx6_account_entered_date,
	lkp_ipfcx6_average_reserve_code,
	lkp_ipfcx6_loss_handling_office,
	lkp_ipfcx6_loss_settling_office,
	lkp_ipfcx6_reserve_change_ind,
	lkp_ipfcx6_iws_claim_indicator,
	lkp_ipfcx6_loss_handling_branch,
	lkp_ipfcx6_loss_plant_location,
	lkp_ipfcx6_loss_accident_state,
	lkp_ipfcx6_loss_aia_codes_1_2,
	lkp_ipfcx6_loss_aia_codes_3_ AS lkp_ipfcx6_loss_aia_codes_3_4,
	lkp_ipfcx6_loss_aia_codes_5_6,
	lkp_ipfcx6_aia_sub_code,
	lkp_ipfcx6_loss_next_review_date,
	lkp_ipfcx6_claim_number_9digit,
	lkp_ipfcx6_loss_claim_payee,
	lkp_ipfcx6_excess_loss_ind,
	lkp_ipfcx6_loss_deductible,
	lkp_ipfcx6_loss_start_yr,
	lkp_ipfcx6_loss_start_mo,
	lkp_ipfcx6_loss_start_da,
	lkp_ipfcx6_loss_weekly_wage,
	lkp_ipfcx6_loss_payment_rate,
	lkp_ipfcx6_loss_frequency,
	lkp_ipfcx6_loss_period_pay,
	lkp_ipfcx6_sub_line,
	lkp_ipfcx6_form_number,
	lkp_ipfcx6_prior_direct_loss_stat,
	lkp_ipfcx6_chs_scheduled_payment,
	lkp_sub_contractor_indicator,
	lkp_ipfcx6_loss_payment_made,
	lkp_ipfcx6_denial_year,
	lkp_ipfcx6_denial_month,
	lkp_ipfcx6_denial_day,
	lkp_ipfcx6_direct_loss_status,
	lkp_ipfcx6_expense_status,
	lkp_ipfcx6_loss_recovery_status,
	lkp_ipfcx6_salvage_status,
	lkp_ipfcx6_subrogation_status,
	lkp_ipfcx6_loss_status_year,
	lkp_ipfcx6_loss_status_month,
	lkp_ipfcx6_loss_status_day,
	lkp_ipfcx6_loss_original_reserve,
	lkp_ipfcx6_os_loss_reserve,
	lkp_ipfcx6_os_expense_reserve,
	lkp_ipfcx6_total_loss,
	lkp_ipfcx6_total_expense,
	lkp_ipfcx6_total_recoveries,
	lkp_ipfcx6_beg_os_loss_reserve,
	lkp_ipfcx6_seg_id_from_x1,
	lkp_ipfcx6_type_claim_payee,
	lkp_ipfcx6_claims_made_ind,
	lkp_ipfcx6_type_disability,
	lkp_ipfcx6_number_of_part78,
	lkp_ipfcx6_bank_number,
	lkp_ipfcx6_prev_reserve_status,
	lkp_ipfcx6_offset_onset_ind,
	lkp_ipfcx6_pms_future_use_sub1,
	lkp_ipfcx6_cust_spl_use_sub1,
	lkp_inf_action,
	lkp_inf_timestamp,
	SYSDATE AS extract_date,
	SYSDATE AS as_of_date,
	source_system_id AS source_system_id4
	FROM RTR_CLM_STG_Update_INSERT
),
UPD_42x6_insert AS (
	SELECT
	logical_flag_4578, 
	lkp_pif_symbol, 
	lkp_pif_policy_number, 
	lkp_pif_module, 
	lkp_ipfcx6_rec_length, 
	lkp_ipfcx6_action_code, 
	lkp_ipfcx6_file_id, 
	lkp_ipfcx6_segment_id, 
	lkp_ipfcx6_segment_level_code, 
	lkp_ipfcx6_segment_part_code, 
	lkp_ipfcx6_sub_part_code, 
	lkp_ipfcx6_insurance_line, 
	lkp_ipfcx6_location_number, 
	lkp_ipfcx6_sub_location_number, 
	lkp_ipfcx6_risk_unit_group, 
	lkp_ipfcx6_class_code_group, 
	lkp_ipfcx6_class_code_member, 
	lkp_ipfcx6_loss_unit, 
	lkp_ipfcx6_risk_sequence, 
	lkp_ipfcx6_risk_type_ind, 
	lkp_ipfcx6_type_exposure, 
	lkp_ipfcx6_major_peril, 
	lkp_ipfcx6_sequence_type_exposure, 
	lkp_ipfcx6_year_item_effective, 
	lkp_ipfcx6_month_item_effective, 
	lkp_ipfcx6_day_item_effective, 
	lkp_ipfcx6_year_of_loss, 
	lkp_ipfcx6_month_of_loss, 
	lkp_ipfcx6_day_of_loss, 
	lkp_ipfcx6_loss_occ_fdigit, 
	lkp_ipfcx6_usr_loss_occurence, 
	lkp_ipfcx6_loss_claimant, 
	lkp_ipfcx6_member, 
	lkp_ipfcx6_loss_disability, 
	lkp_ipfcx6_reserve_category, 
	lkp_ipfcx6_layer, 
	lkp_ipfcx6_reins_key_id, 
	lkp_ipfcx6_reins_co_no, 
	lkp_ipfcx6_reins_broker, 
	lkp_ipfcx6_year_process, 
	lkp_ipfcx6_month_process, 
	lkp_ipfcx6_day_process, 
	lkp_ipfcx6_year_change_entry, 
	lkp_ipfcx6_month_change_entry, 
	lkp_ipfcx6_day_change_entry, 
	lkp_ipfcx6_sequence_change_entry, 
	lkp_ipfcx6_segment_status, 
	lkp_ipfcx6_entry_operator, 
	lkp_ipfcx6_claim_type, 
	lkp_ipfcx6_loss_reported_year, 
	lkp_ipfcx6_loss_reported_month, 
	lkp_ipfcx6_loss_reported_day, 
	lkp_ipfcx6_loss_cause, 
	lkp_ipfcx6_loss_adjustor_no, 
	lkp_ipfcx6_loss_adjustor_branch, 
	lkp_ipfcx6_loss_examiner, 
	lkp_ipfcx6_other_assignment, 
	lkp_ipfcx6_account_entered_date, 
	lkp_ipfcx6_average_reserve_code, 
	lkp_ipfcx6_loss_handling_office, 
	lkp_ipfcx6_loss_settling_office, 
	lkp_ipfcx6_reserve_change_ind, 
	lkp_ipfcx6_iws_claim_indicator, 
	lkp_ipfcx6_loss_handling_branch, 
	lkp_ipfcx6_loss_plant_location, 
	lkp_ipfcx6_loss_accident_state, 
	lkp_ipfcx6_loss_aia_codes_1_2, 
	lkp_ipfcx6_loss_aia_codes_3_4, 
	lkp_ipfcx6_loss_aia_codes_5_6, 
	lkp_ipfcx6_aia_sub_code, 
	lkp_ipfcx6_loss_next_review_date, 
	lkp_ipfcx6_claim_number_9digit, 
	lkp_ipfcx6_loss_claim_payee, 
	lkp_ipfcx6_excess_loss_ind, 
	lkp_ipfcx6_loss_deductible, 
	lkp_ipfcx6_loss_start_yr, 
	lkp_ipfcx6_loss_start_mo, 
	lkp_ipfcx6_loss_start_da, 
	lkp_ipfcx6_loss_weekly_wage, 
	lkp_ipfcx6_loss_payment_rate, 
	lkp_ipfcx6_loss_frequency, 
	lkp_ipfcx6_loss_period_pay, 
	lkp_ipfcx6_sub_line, 
	lkp_ipfcx6_form_number, 
	lkp_ipfcx6_prior_direct_loss_stat, 
	lkp_ipfcx6_chs_scheduled_payment, 
	lkp_sub_contractor_indicator, 
	lkp_ipfcx6_loss_payment_made, 
	lkp_ipfcx6_denial_year, 
	lkp_ipfcx6_denial_month, 
	lkp_ipfcx6_denial_day, 
	lkp_ipfcx6_direct_loss_status, 
	lkp_ipfcx6_expense_status, 
	lkp_ipfcx6_loss_recovery_status, 
	lkp_ipfcx6_salvage_status, 
	lkp_ipfcx6_subrogation_status, 
	lkp_ipfcx6_loss_status_year, 
	lkp_ipfcx6_loss_status_month, 
	lkp_ipfcx6_loss_status_day, 
	lkp_ipfcx6_loss_original_reserve, 
	lkp_ipfcx6_os_loss_reserve, 
	lkp_ipfcx6_os_expense_reserve, 
	lkp_ipfcx6_total_loss, 
	lkp_ipfcx6_total_expense, 
	lkp_ipfcx6_total_recoveries, 
	lkp_ipfcx6_beg_os_loss_reserve, 
	lkp_ipfcx6_seg_id_from_x1, 
	lkp_ipfcx6_type_claim_payee, 
	lkp_ipfcx6_claims_made_ind, 
	lkp_ipfcx6_type_disability, 
	lkp_ipfcx6_number_of_part78, 
	lkp_ipfcx6_bank_number, 
	lkp_ipfcx6_prev_reserve_status, 
	lkp_ipfcx6_offset_onset_ind, 
	lkp_ipfcx6_pms_future_use_sub1, 
	lkp_ipfcx6_cust_spl_use_sub1, 
	lkp_inf_action, 
	lkp_inf_timestamp, 
	extract_date, 
	as_of_date, 
	source_system_id4
	FROM EXP_CLM_STG_insert
),
PIF_42X6_stage_Insert AS (
	INSERT INTO PIF_42X6_stage
	(pif_symbol, pif_policy_number, pif_module, ipfcx6_rec_length, ipfcx6_action_code, ipfcx6_file_id, ipfcx6_segment_id, ipfcx6_segment_level_code, ipfcx6_segment_part_code, ipfcx6_sub_part_code, ipfcx6_insurance_line, ipfcx6_location_number, ipfcx6_sub_location_number, ipfcx6_risk_unit_group, ipfcx6_class_code_group, ipfcx6_class_code_member, ipfcx6_loss_unit, ipfcx6_risk_sequence, ipfcx6_risk_type_ind, ipfcx6_type_exposure, ipfcx6_major_peril, ipfcx6_sequence_type_exposure, ipfcx6_year_item_effective, ipfcx6_month_item_effective, ipfcx6_day_item_effective, ipfcx6_year_of_loss, ipfcx6_month_of_loss, ipfcx6_day_of_loss, ipfcx6_loss_occ_fdigit, ipfcx6_usr_loss_occurence, ipfcx6_loss_claimant, ipfcx6_member, ipfcx6_loss_disability, ipfcx6_reserve_category, ipfcx6_layer, ipfcx6_reins_key_id, ipfcx6_reins_co_no, ipfcx6_reins_broker, ipfcx6_year_process, ipfcx6_month_process, ipfcx6_day_process, ipfcx6_year_change_entry, ipfcx6_month_change_entry, ipfcx6_day_change_entry, ipfcx6_sequence_change_entry, ipfcx6_segment_status, ipfcx6_entry_operator, ipfcx6_claim_type, ipfcx6_loss_reported_year, ipfcx6_loss_reported_month, ipfcx6_loss_reported_day, ipfcx6_loss_cause, ipfcx6_loss_adjustor_no, ipfcx6_loss_adjustor_branch, ipfcx6_loss_examiner, ipfcx6_other_assignment, ipfcx6_account_entered_date, ipfcx6_average_reserve_code, ipfcx6_loss_handling_office, ipfcx6_loss_settling_office, ipfcx6_reserve_change_ind, ipfcx6_iws_claim_indicator, ipfcx6_loss_handling_branch, ipfcx6_loss_plant_location, ipfcx6_loss_accident_state, ipfcx6_loss_aia_codes_1_2, ipfcx6_loss_aia_codes_3_4, ipfcx6_loss_aia_codes_5_6, ipfcx6_aia_sub_code, ipfcx6_loss_next_review_date, ipfcx6_claim_number_9digit, ipfcx6_loss_claim_payee, ipfcx6_excess_loss_ind, ipfcx6_loss_deductible, ipfcx6_loss_start_yr, ipfcx6_loss_start_mo, ipfcx6_loss_start_da, ipfcx6_loss_weekly_wage, ipfcx6_loss_payment_rate, ipfcx6_loss_frequency, ipfcx6_loss_period_pay, ipfcx6_sub_line, ipfcx6_form_number, ipfcx6_prior_direct_loss_stat, ipfcx6_chs_scheduled_payment, sub_contractor_indicator, ipfcx6_loss_payment_made, ipfcx6_denial_year, ipfcx6_denial_month, ipfcx6_denial_day, ipfcx6_direct_loss_status, ipfcx6_expense_status, ipfcx6_loss_recovery_status, ipfcx6_salvage_status, ipfcx6_subrogation_status, ipfcx6_loss_status_year, ipfcx6_loss_status_month, ipfcx6_loss_status_day, ipfcx6_loss_original_reserve, ipfcx6_os_loss_reserve, ipfcx6_os_expense_reserve, ipfcx6_total_loss, ipfcx6_total_expense, ipfcx6_total_recoveries, ipfcx6_beg_os_loss_reserve, ipfcx6_seg_id_from_x1, ipfcx6_type_claim_payee, ipfcx6_claims_made_ind, ipfcx6_type_disability, ipfcx6_number_of_part78, ipfcx6_bank_number, ipfcx6_prev_reserve_status, ipfcx6_offset_onset_ind, ipfcx6_pms_future_use_sub1, ipfcx6_cust_spl_use_sub1, inf_action, inf_timestamp, logical_flag, extract_date, as_of_date, source_system_id)
	SELECT 
	lkp_pif_symbol AS PIF_SYMBOL, 
	lkp_pif_policy_number AS PIF_POLICY_NUMBER, 
	lkp_pif_module AS PIF_MODULE, 
	lkp_ipfcx6_rec_length AS IPFCX6_REC_LENGTH, 
	lkp_ipfcx6_action_code AS IPFCX6_ACTION_CODE, 
	lkp_ipfcx6_file_id AS IPFCX6_FILE_ID, 
	lkp_ipfcx6_segment_id AS IPFCX6_SEGMENT_ID, 
	lkp_ipfcx6_segment_level_code AS IPFCX6_SEGMENT_LEVEL_CODE, 
	lkp_ipfcx6_segment_part_code AS IPFCX6_SEGMENT_PART_CODE, 
	lkp_ipfcx6_sub_part_code AS IPFCX6_SUB_PART_CODE, 
	lkp_ipfcx6_insurance_line AS IPFCX6_INSURANCE_LINE, 
	lkp_ipfcx6_location_number AS IPFCX6_LOCATION_NUMBER, 
	lkp_ipfcx6_sub_location_number AS IPFCX6_SUB_LOCATION_NUMBER, 
	lkp_ipfcx6_risk_unit_group AS IPFCX6_RISK_UNIT_GROUP, 
	lkp_ipfcx6_class_code_group AS IPFCX6_CLASS_CODE_GROUP, 
	lkp_ipfcx6_class_code_member AS IPFCX6_CLASS_CODE_MEMBER, 
	lkp_ipfcx6_loss_unit AS IPFCX6_LOSS_UNIT, 
	lkp_ipfcx6_risk_sequence AS IPFCX6_RISK_SEQUENCE, 
	lkp_ipfcx6_risk_type_ind AS IPFCX6_RISK_TYPE_IND, 
	lkp_ipfcx6_type_exposure AS IPFCX6_TYPE_EXPOSURE, 
	lkp_ipfcx6_major_peril AS IPFCX6_MAJOR_PERIL, 
	lkp_ipfcx6_sequence_type_exposure AS IPFCX6_SEQUENCE_TYPE_EXPOSURE, 
	lkp_ipfcx6_year_item_effective AS IPFCX6_YEAR_ITEM_EFFECTIVE, 
	lkp_ipfcx6_month_item_effective AS IPFCX6_MONTH_ITEM_EFFECTIVE, 
	lkp_ipfcx6_day_item_effective AS IPFCX6_DAY_ITEM_EFFECTIVE, 
	lkp_ipfcx6_year_of_loss AS IPFCX6_YEAR_OF_LOSS, 
	lkp_ipfcx6_month_of_loss AS IPFCX6_MONTH_OF_LOSS, 
	lkp_ipfcx6_day_of_loss AS IPFCX6_DAY_OF_LOSS, 
	lkp_ipfcx6_loss_occ_fdigit AS IPFCX6_LOSS_OCC_FDIGIT, 
	lkp_ipfcx6_usr_loss_occurence AS IPFCX6_USR_LOSS_OCCURENCE, 
	lkp_ipfcx6_loss_claimant AS IPFCX6_LOSS_CLAIMANT, 
	lkp_ipfcx6_member AS IPFCX6_MEMBER, 
	lkp_ipfcx6_loss_disability AS IPFCX6_LOSS_DISABILITY, 
	lkp_ipfcx6_reserve_category AS IPFCX6_RESERVE_CATEGORY, 
	lkp_ipfcx6_layer AS IPFCX6_LAYER, 
	lkp_ipfcx6_reins_key_id AS IPFCX6_REINS_KEY_ID, 
	lkp_ipfcx6_reins_co_no AS IPFCX6_REINS_CO_NO, 
	lkp_ipfcx6_reins_broker AS IPFCX6_REINS_BROKER, 
	lkp_ipfcx6_year_process AS IPFCX6_YEAR_PROCESS, 
	lkp_ipfcx6_month_process AS IPFCX6_MONTH_PROCESS, 
	lkp_ipfcx6_day_process AS IPFCX6_DAY_PROCESS, 
	lkp_ipfcx6_year_change_entry AS IPFCX6_YEAR_CHANGE_ENTRY, 
	lkp_ipfcx6_month_change_entry AS IPFCX6_MONTH_CHANGE_ENTRY, 
	lkp_ipfcx6_day_change_entry AS IPFCX6_DAY_CHANGE_ENTRY, 
	lkp_ipfcx6_sequence_change_entry AS IPFCX6_SEQUENCE_CHANGE_ENTRY, 
	lkp_ipfcx6_segment_status AS IPFCX6_SEGMENT_STATUS, 
	lkp_ipfcx6_entry_operator AS IPFCX6_ENTRY_OPERATOR, 
	lkp_ipfcx6_claim_type AS IPFCX6_CLAIM_TYPE, 
	lkp_ipfcx6_loss_reported_year AS IPFCX6_LOSS_REPORTED_YEAR, 
	lkp_ipfcx6_loss_reported_month AS IPFCX6_LOSS_REPORTED_MONTH, 
	lkp_ipfcx6_loss_reported_day AS IPFCX6_LOSS_REPORTED_DAY, 
	lkp_ipfcx6_loss_cause AS IPFCX6_LOSS_CAUSE, 
	lkp_ipfcx6_loss_adjustor_no AS IPFCX6_LOSS_ADJUSTOR_NO, 
	lkp_ipfcx6_loss_adjustor_branch AS IPFCX6_LOSS_ADJUSTOR_BRANCH, 
	lkp_ipfcx6_loss_examiner AS IPFCX6_LOSS_EXAMINER, 
	lkp_ipfcx6_other_assignment AS IPFCX6_OTHER_ASSIGNMENT, 
	lkp_ipfcx6_account_entered_date AS IPFCX6_ACCOUNT_ENTERED_DATE, 
	lkp_ipfcx6_average_reserve_code AS IPFCX6_AVERAGE_RESERVE_CODE, 
	lkp_ipfcx6_loss_handling_office AS IPFCX6_LOSS_HANDLING_OFFICE, 
	lkp_ipfcx6_loss_settling_office AS IPFCX6_LOSS_SETTLING_OFFICE, 
	lkp_ipfcx6_reserve_change_ind AS IPFCX6_RESERVE_CHANGE_IND, 
	lkp_ipfcx6_iws_claim_indicator AS IPFCX6_IWS_CLAIM_INDICATOR, 
	lkp_ipfcx6_loss_handling_branch AS IPFCX6_LOSS_HANDLING_BRANCH, 
	lkp_ipfcx6_loss_plant_location AS IPFCX6_LOSS_PLANT_LOCATION, 
	lkp_ipfcx6_loss_accident_state AS IPFCX6_LOSS_ACCIDENT_STATE, 
	lkp_ipfcx6_loss_aia_codes_1_2 AS IPFCX6_LOSS_AIA_CODES_1_2, 
	lkp_ipfcx6_loss_aia_codes_3_4 AS IPFCX6_LOSS_AIA_CODES_3_4, 
	lkp_ipfcx6_loss_aia_codes_5_6 AS IPFCX6_LOSS_AIA_CODES_5_6, 
	lkp_ipfcx6_aia_sub_code AS IPFCX6_AIA_SUB_CODE, 
	lkp_ipfcx6_loss_next_review_date AS IPFCX6_LOSS_NEXT_REVIEW_DATE, 
	lkp_ipfcx6_claim_number_9digit AS IPFCX6_CLAIM_NUMBER_9DIGIT, 
	lkp_ipfcx6_loss_claim_payee AS IPFCX6_LOSS_CLAIM_PAYEE, 
	lkp_ipfcx6_excess_loss_ind AS IPFCX6_EXCESS_LOSS_IND, 
	lkp_ipfcx6_loss_deductible AS IPFCX6_LOSS_DEDUCTIBLE, 
	lkp_ipfcx6_loss_start_yr AS IPFCX6_LOSS_START_YR, 
	lkp_ipfcx6_loss_start_mo AS IPFCX6_LOSS_START_MO, 
	lkp_ipfcx6_loss_start_da AS IPFCX6_LOSS_START_DA, 
	lkp_ipfcx6_loss_weekly_wage AS IPFCX6_LOSS_WEEKLY_WAGE, 
	lkp_ipfcx6_loss_payment_rate AS IPFCX6_LOSS_PAYMENT_RATE, 
	lkp_ipfcx6_loss_frequency AS IPFCX6_LOSS_FREQUENCY, 
	lkp_ipfcx6_loss_period_pay AS IPFCX6_LOSS_PERIOD_PAY, 
	lkp_ipfcx6_sub_line AS IPFCX6_SUB_LINE, 
	lkp_ipfcx6_form_number AS IPFCX6_FORM_NUMBER, 
	lkp_ipfcx6_prior_direct_loss_stat AS IPFCX6_PRIOR_DIRECT_LOSS_STAT, 
	lkp_ipfcx6_chs_scheduled_payment AS IPFCX6_CHS_SCHEDULED_PAYMENT, 
	lkp_sub_contractor_indicator AS SUB_CONTRACTOR_INDICATOR, 
	lkp_ipfcx6_loss_payment_made AS IPFCX6_LOSS_PAYMENT_MADE, 
	lkp_ipfcx6_denial_year AS IPFCX6_DENIAL_YEAR, 
	lkp_ipfcx6_denial_month AS IPFCX6_DENIAL_MONTH, 
	lkp_ipfcx6_denial_day AS IPFCX6_DENIAL_DAY, 
	lkp_ipfcx6_direct_loss_status AS IPFCX6_DIRECT_LOSS_STATUS, 
	lkp_ipfcx6_expense_status AS IPFCX6_EXPENSE_STATUS, 
	lkp_ipfcx6_loss_recovery_status AS IPFCX6_LOSS_RECOVERY_STATUS, 
	lkp_ipfcx6_salvage_status AS IPFCX6_SALVAGE_STATUS, 
	lkp_ipfcx6_subrogation_status AS IPFCX6_SUBROGATION_STATUS, 
	lkp_ipfcx6_loss_status_year AS IPFCX6_LOSS_STATUS_YEAR, 
	lkp_ipfcx6_loss_status_month AS IPFCX6_LOSS_STATUS_MONTH, 
	lkp_ipfcx6_loss_status_day AS IPFCX6_LOSS_STATUS_DAY, 
	lkp_ipfcx6_loss_original_reserve AS IPFCX6_LOSS_ORIGINAL_RESERVE, 
	lkp_ipfcx6_os_loss_reserve AS IPFCX6_OS_LOSS_RESERVE, 
	lkp_ipfcx6_os_expense_reserve AS IPFCX6_OS_EXPENSE_RESERVE, 
	lkp_ipfcx6_total_loss AS IPFCX6_TOTAL_LOSS, 
	lkp_ipfcx6_total_expense AS IPFCX6_TOTAL_EXPENSE, 
	lkp_ipfcx6_total_recoveries AS IPFCX6_TOTAL_RECOVERIES, 
	lkp_ipfcx6_beg_os_loss_reserve AS IPFCX6_BEG_OS_LOSS_RESERVE, 
	lkp_ipfcx6_seg_id_from_x1 AS IPFCX6_SEG_ID_FROM_X1, 
	lkp_ipfcx6_type_claim_payee AS IPFCX6_TYPE_CLAIM_PAYEE, 
	lkp_ipfcx6_claims_made_ind AS IPFCX6_CLAIMS_MADE_IND, 
	lkp_ipfcx6_type_disability AS IPFCX6_TYPE_DISABILITY, 
	lkp_ipfcx6_number_of_part78 AS IPFCX6_NUMBER_OF_PART78, 
	lkp_ipfcx6_bank_number AS IPFCX6_BANK_NUMBER, 
	lkp_ipfcx6_prev_reserve_status AS IPFCX6_PREV_RESERVE_STATUS, 
	lkp_ipfcx6_offset_onset_ind AS IPFCX6_OFFSET_ONSET_IND, 
	lkp_ipfcx6_pms_future_use_sub1 AS IPFCX6_PMS_FUTURE_USE_SUB1, 
	lkp_ipfcx6_cust_spl_use_sub1 AS IPFCX6_CUST_SPL_USE_SUB1, 
	lkp_inf_action AS INF_ACTION, 
	lkp_inf_timestamp AS INF_TIMESTAMP, 
	logical_flag_4578 AS LOGICAL_FLAG, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	source_system_id4 AS SOURCE_SYSTEM_ID
	FROM UPD_42x6_insert
),
EXP_CLM_Stg_Update AS (
	SELECT
	pif_42x6_stage_id AS pif_42x6_stage_id3,
	logical_flag AS logical_flag3
	FROM RTR_CLM_STG_Update_UPDATE
),
UPD_42x6_update AS (
	SELECT
	pif_42x6_stage_id3, 
	logical_flag3
	FROM EXP_CLM_Stg_Update
),
PIF_42X6_stage_Update AS (
	MERGE INTO PIF_42X6_stage AS T
	USING UPD_42x6_update AS S
	ON T.pif_42x6_stage_id = S.pif_42x6_stage_id3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.logical_flag = S.logical_flag3
),
EXP_CLM_STG_Insert_dummy AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module,
	loss_insurance_line,
	loss_location_number,
	loss_sub_location_number,
	loss_risk_unit_group,
	loss_class_code_group,
	loss_class_code_member,
	loss_unit,
	loss_sequence_risk_unit,
	loss_major_peril,
	loss_major_peril_seq,
	loss_year,
	loss_month,
	loss_day,
	loss_occurence,
	-- *INF*: IIF(LENGTH(loss_occurence)<=2,'0',SUBSTR(loss_occurence,1,1))
	IFF(LENGTH(loss_occurence) <= 2, '0', SUBSTR(loss_occurence, 1, 1)) AS ipfcx6_loss_occ_fdigit,
	-- *INF*: IIF(LENGTH(loss_occurence)>2,SUBSTR(loss_occurence,2,2),TO_CHAR(loss_occurence))
	IFF(LENGTH(loss_occurence) > 2, SUBSTR(loss_occurence, 2, 2), TO_CHAR(loss_occurence)) AS ipfcx6_usr_loss_occurence,
	loss_claimant,
	loss_member,
	loss_disability,
	loss_reserve_category,
	loss_cause,
	loss_offset_onset_ind,
	logical_flag_insert AS logical_flag_op_insert,
	sysdate AS extract_date,
	sysdate AS as_of_date,
	source_system_id,
	1800 AS Date_year_default,
	1 AS Date_month_default,
	1 AS Date_day_default,
	'N/A' AS char_default,
	0 AS number_default
	FROM RTR_CLM_STG_Update_INSERT_DUMMY
),
UPD_42X6_stage_Insert_dummy AS (
	SELECT
	pif_symbol, 
	pif_policy_number, 
	pif_module, 
	loss_insurance_line, 
	loss_location_number, 
	loss_sub_location_number, 
	loss_risk_unit_group, 
	loss_class_code_group, 
	loss_class_code_member, 
	loss_unit, 
	loss_sequence_risk_unit, 
	loss_major_peril, 
	loss_major_peril_seq, 
	loss_year, 
	loss_month, 
	loss_day, 
	ipfcx6_loss_occ_fdigit, 
	ipfcx6_usr_loss_occurence, 
	loss_claimant, 
	loss_member, 
	loss_disability, 
	loss_reserve_category, 
	loss_cause, 
	loss_offset_onset_ind, 
	logical_flag_op_insert, 
	extract_date, 
	as_of_date, 
	source_system_id, 
	Date_year_default, 
	Date_month_default, 
	Date_day_default, 
	char_default, 
	number_default
	FROM EXP_CLM_STG_Insert_dummy
),
PIF_42X6_stage_Insert_dummy AS (
	INSERT INTO PIF_42X6_stage
	(pif_symbol, pif_policy_number, pif_module, ipfcx6_insurance_line, ipfcx6_location_number, ipfcx6_sub_location_number, ipfcx6_risk_unit_group, ipfcx6_class_code_group, ipfcx6_class_code_member, ipfcx6_loss_unit, ipfcx6_risk_sequence, ipfcx6_risk_type_ind, ipfcx6_type_exposure, ipfcx6_major_peril, ipfcx6_sequence_type_exposure, ipfcx6_year_item_effective, ipfcx6_month_item_effective, ipfcx6_day_item_effective, ipfcx6_year_of_loss, ipfcx6_month_of_loss, ipfcx6_day_of_loss, ipfcx6_loss_occ_fdigit, ipfcx6_usr_loss_occurence, ipfcx6_loss_claimant, ipfcx6_member, ipfcx6_loss_disability, ipfcx6_reserve_category, ipfcx6_loss_cause, ipfcx6_offset_onset_ind, logical_flag, extract_date, as_of_date, source_system_id)
	SELECT 
	PIF_SYMBOL, 
	PIF_POLICY_NUMBER, 
	PIF_MODULE, 
	loss_insurance_line AS IPFCX6_INSURANCE_LINE, 
	loss_location_number AS IPFCX6_LOCATION_NUMBER, 
	loss_sub_location_number AS IPFCX6_SUB_LOCATION_NUMBER, 
	loss_risk_unit_group AS IPFCX6_RISK_UNIT_GROUP, 
	loss_class_code_group AS IPFCX6_CLASS_CODE_GROUP, 
	loss_class_code_member AS IPFCX6_CLASS_CODE_MEMBER, 
	loss_unit AS IPFCX6_LOSS_UNIT, 
	loss_sequence_risk_unit AS IPFCX6_RISK_SEQUENCE, 
	number_default AS IPFCX6_RISK_TYPE_IND, 
	char_default AS IPFCX6_TYPE_EXPOSURE, 
	loss_major_peril AS IPFCX6_MAJOR_PERIL, 
	loss_major_peril_seq AS IPFCX6_SEQUENCE_TYPE_EXPOSURE, 
	Date_year_default AS IPFCX6_YEAR_ITEM_EFFECTIVE, 
	Date_month_default AS IPFCX6_MONTH_ITEM_EFFECTIVE, 
	Date_day_default AS IPFCX6_DAY_ITEM_EFFECTIVE, 
	loss_year AS IPFCX6_YEAR_OF_LOSS, 
	loss_month AS IPFCX6_MONTH_OF_LOSS, 
	loss_day AS IPFCX6_DAY_OF_LOSS, 
	IPFCX6_LOSS_OCC_FDIGIT, 
	IPFCX6_USR_LOSS_OCCURENCE, 
	loss_claimant AS IPFCX6_LOSS_CLAIMANT, 
	loss_member AS IPFCX6_MEMBER, 
	loss_disability AS IPFCX6_LOSS_DISABILITY, 
	loss_reserve_category AS IPFCX6_RESERVE_CATEGORY, 
	loss_cause AS IPFCX6_LOSS_CAUSE, 
	loss_offset_onset_ind AS IPFCX6_OFFSET_ONSET_IND, 
	logical_flag_op_insert AS LOGICAL_FLAG, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	SOURCE_SYSTEM_ID
	FROM UPD_42X6_stage_Insert_dummy
),
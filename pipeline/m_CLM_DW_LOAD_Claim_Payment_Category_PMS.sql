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
SQ_pifmstr_PIF_4578_stage AS (
	SELECT 
	LTRIM(RTRIM(pif_4578_stage.pif_symbol)), 
	LTRIM(RTRIM(pif_4578_stage.pif_policy_number)),  
	LTRIM(RTRIM(pif_4578_stage.pif_module)) ,  
	LTRIM(RTRIM(pif_4578_stage.loss_insurance_line)), 
	LTRIM(RTRIM(pif_4578_stage.loss_location_number)), 
	LTRIM(RTRIM(pif_4578_stage.loss_sub_location_number)), 
	LTRIM(RTRIM(pif_4578_stage.loss_risk_unit_group)), 
	LTRIM(RTRIM(pif_4578_stage.loss_class_code_group)), 
	LTRIM(RTRIM(pif_4578_stage.loss_class_code_member)), 
	LTRIM(RTRIM(pif_4578_stage.loss_unit)), 
	LTRIM(RTRIM(pif_4578_stage.loss_sequence_risk_unit)), 
	LTRIM(RTRIM(pif_4578_stage.loss_type_exposure)), 
	LTRIM(RTRIM(pif_4578_stage.loss_major_peril)), 
	LTRIM(RTRIM(pif_4578_stage.loss_major_peril_seq)), 
	LTRIM(RTRIM(pif_4578_stage.loss_year)), 
	LTRIM(RTRIM(pif_4578_stage.loss_month)), 
	LTRIM(RTRIM(pif_4578_stage.loss_day)), 
	LTRIM(RTRIM(pif_4578_stage.loss_occurence)), 
	LTRIM(RTRIM(pif_4578_stage.loss_claimant)), 
	LTRIM(RTRIM(pif_4578_stage.loss_member)), 
	LTRIM(RTRIM(pif_4578_stage.loss_disability)), 
	LTRIM(RTRIM(pif_4578_stage.loss_reserve_category)), 
	LTRIM(RTRIM(pif_4578_stage.loss_transaction)), 
	LTRIM(RTRIM(pif_4578_stage.loss_entry_operator)), 
	LTRIM(RTRIM(pif_4578_stage.loss_cause)), 
	LTRIM(RTRIM(pif_4578_stage.loss_cost_containment)), 
	LTRIM(RTRIM(pif_4578_stage.loss_draft_amount)), 
	LTRIM(RTRIM(pif_4578_stage.loss_draft_no)), 
	LTRIM(RTRIM(pif_4578_stage.loss_transaction_date)), 
	LTRIM(RTRIM(pif_4578_stage.loss_draft_pay_to_1)), 
	LTRIM(RTRIM(pif_4578_stage.loss_draft_pay_to_2)), 
	LTRIM(RTRIM(pif_4578_stage.loss_draft_pay_to_3)), 
	LTRIM(RTRIM(pif_4578_stage.loss_draft_mail_to)), 
	LTRIM(RTRIM(pif_4578_stage.source_system_id)) 
	FROM
	 pif_4578_stage 
	WHERE
	 (pif_4578_stage.loss_part = '7') 
	AND  (pif_4578_stage.logical_flag ='0')
	AND (pif_4578_stage.loss_cost_containment IS NOT NULL) 
	AND (pif_4578_stage.loss_cost_containment <> 0.00 )
	AND LEN(LTRIM(RTRIM(pif_4578_stage.loss_draft_no))) > 0
),
EXP_CLAIM_TRANSACTION_VALIDATE AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module,
	loss_insurance_line,
	-- *INF*: IIF(ISNULL(loss_insurance_line) OR LENGTH(LTRIM(RTRIM(loss_insurance_line))) = 0, 'N/A', LTRIM(RTRIM(loss_insurance_line)))
	IFF(loss_insurance_line IS NULL 
		OR LENGTH(LTRIM(RTRIM(loss_insurance_line
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(loss_insurance_line
			)
		)
	) AS v_loss_insurance_line,
	loss_location_number,
	-- *INF*: IIF(ISNULL(loss_location_number), '0', to_char(loss_location_number))
	IFF(loss_location_number IS NULL,
		'0',
		to_char(loss_location_number
		)
	) AS v_loss_location_number,
	loss_sub_location_number,
	-- *INF*: IIF(ISNULL(loss_sub_location_number), '0', to_char(loss_sub_location_number))
	IFF(loss_sub_location_number IS NULL,
		'0',
		to_char(loss_sub_location_number
		)
	) AS v_loss_sub_location_number,
	loss_risk_unit_group,
	-- *INF*: IIF(ISNULL(loss_risk_unit_group) OR LENGTH(LTRIM(RTRIM(loss_risk_unit_group))) = 0, 'N/A', LTRIM(RTRIM(loss_risk_unit_group)))
	IFF(loss_risk_unit_group IS NULL 
		OR LENGTH(LTRIM(RTRIM(loss_risk_unit_group
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(loss_risk_unit_group
			)
		)
	) AS v_loss_risk_unit_group,
	loss_class_code_group,
	loss_class_code_member,
	loss_unit,
	-- *INF*: IIF(ISNULL(loss_unit) OR LENGTH(LTRIM(RTRIM(loss_unit))) = 0, 'N/A', LTRIM(RTRIM(loss_unit)))
	IFF(loss_unit IS NULL 
		OR LENGTH(LTRIM(RTRIM(loss_unit
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(loss_unit
			)
		)
	) AS v_loss_unit,
	loss_sequence_risk_unit,
	-- *INF*: IIF(TO_DECIMAL(loss_sequence_risk_unit) = 0 OR ISNULL(LTRIM(RTRIM(loss_sequence_risk_unit))), '0',
	-- to_char(TO_DECIMAL(loss_sequence_risk_unit)) )
	IFF(CAST(loss_sequence_risk_unit AS FLOAT) = 0 
		OR LTRIM(RTRIM(loss_sequence_risk_unit
			)
		) IS NULL,
		'0',
		to_char(CAST(loss_sequence_risk_unit AS FLOAT)
		)
	) AS v_loss_sequence_risk_unit,
	loss_type_exposure,
	-- *INF*: IIF(ISNULL(loss_type_exposure) OR LENGTH(LTRIM(RTRIM(loss_type_exposure))) = 0, 'N/A', LTRIM(RTRIM(loss_type_exposure)))
	IFF(loss_type_exposure IS NULL 
		OR LENGTH(LTRIM(RTRIM(loss_type_exposure
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(loss_type_exposure
			)
		)
	) AS v_loss_type_exposure,
	loss_major_peril,
	-- *INF*: IIF(ISNULL(loss_major_peril) OR LENGTH(LTRIM(RTRIM(loss_major_peril))) = 0, 'N/A', LTRIM(RTRIM(loss_major_peril)))
	IFF(loss_major_peril IS NULL 
		OR LENGTH(LTRIM(RTRIM(loss_major_peril
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(loss_major_peril
			)
		)
	) AS v_loss_major_peril,
	loss_major_peril_seq,
	-- *INF*: IIF(ISNULL(loss_major_peril_seq) OR LENGTH(LTRIM(RTRIM(loss_major_peril_seq))) = 0, 'N/A', LTRIM(RTRIM(loss_major_peril_seq)))
	IFF(loss_major_peril_seq IS NULL 
		OR LENGTH(LTRIM(RTRIM(loss_major_peril_seq
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(loss_major_peril_seq
			)
		)
	) AS v_loss_major_peril_seq,
	-- *INF*: IIF(ISNULL(TO_CHAR(loss_class_code_group)||TO_CHAR(loss_class_code_member)) ,'N/A',TO_CHAR(loss_class_code_group)||TO_CHAR(loss_class_code_member))
	IFF(TO_CHAR(loss_class_code_group
		) || TO_CHAR(loss_class_code_member
		) IS NULL,
		'N/A',
		TO_CHAR(loss_class_code_group
		) || TO_CHAR(loss_class_code_member
		)
	) AS V_risk_unit_grp_seq_num_1,
	-- *INF*: LPAD(V_risk_unit_grp_seq_num_1,3,'0')
	LPAD(V_risk_unit_grp_seq_num_1, 3, '0'
	) AS V_risk_unit_grp_seq_num,
	loss_year,
	loss_month,
	loss_day,
	-- *INF*: TO_CHAR(loss_year)
	TO_CHAR(loss_year
	) AS V_loss_year,
	-- *INF*: to_char(loss_month)
	to_char(loss_month
	) AS V_loss_month,
	-- *INF*: to_char(loss_day)
	to_char(loss_day
	) AS V_loss_day,
	-- *INF*: IIF ( LENGTH(V_loss_month) = 1, '0' || V_loss_month, V_loss_month)
	-- ||  
	-- IIF ( LENGTH(V_loss_day ) = 1, '0' || V_loss_day, V_loss_day )
	-- ||  
	-- V_loss_year
	IFF(LENGTH(V_loss_month
		) = 1,
		'0' || V_loss_month,
		V_loss_month
	) || IFF(LENGTH(V_loss_day
		) = 1,
		'0' || V_loss_day,
		V_loss_day
	) || V_loss_year AS v_loss_date,
	loss_occurence,
	pif_symbol || pif_policy_number || pif_module || v_loss_date || loss_occurence AS v_loss_occurence_key,
	loss_claimant,
	'CMT' AS v_party_role_code,
	-- *INF*: v_loss_occurence_key||TO_CHAR(loss_claimant)||v_party_role_code
	v_loss_occurence_key || TO_CHAR(loss_claimant
	) || v_party_role_code AS v_loss_party_key,
	v_loss_party_key AS loss_party_key_out,
	loss_member,
	-- *INF*: IIF(ISNULL(loss_member) OR LENGTH(LTRIM(RTRIM(loss_member))) = 0, 'N/A', loss_member)
	IFF(loss_member IS NULL 
		OR LENGTH(LTRIM(RTRIM(loss_member
				)
			)
		) = 0,
		'N/A',
		loss_member
	) AS v_loss_member,
	loss_disability,
	-- *INF*: IIF(ISNULL(loss_disability) OR LENGTH(LTRIM(RTRIM(loss_disability))) = 0, 'N/A', loss_disability)
	IFF(loss_disability IS NULL 
		OR LENGTH(LTRIM(RTRIM(loss_disability
				)
			)
		) = 0,
		'N/A',
		loss_disability
	) AS v_loss_disability,
	loss_reserve_category,
	-- *INF*: IIF(ISNULL(loss_reserve_category) OR LENGTH(LTRIM(RTRIM(loss_reserve_category))) = 0, 'N/A', loss_reserve_category)
	IFF(loss_reserve_category IS NULL 
		OR LENGTH(LTRIM(RTRIM(loss_reserve_category
				)
			)
		) = 0,
		'N/A',
		loss_reserve_category
	) AS v_loss_reserve_category,
	loss_transaction,
	-- *INF*: decode(true,
	-- ISNULL(loss_cause), 'N/A',
	-- LENGTH(LTRIM(RTRIM(loss_cause))) = 0, 'N/A',
	-- (v_loss_major_peril = '032' AND loss_cause = '07'), '06',
	-- LTRIM(RTRIM(loss_cause)))
	-- 
	-- 
	-- --IIF(ISNULL(loss_cause) OR LENGTH(LTRIM(RTRIM(loss_cause))) = 0, 'N/A',loss_cause)
	decode(true,
		loss_cause IS NULL, 'N/A',
		LENGTH(LTRIM(RTRIM(loss_cause
				)
			)
		) = 0, 'N/A',
		( v_loss_major_peril = '032' 
			AND loss_cause = '07' 
		), '06',
		LTRIM(RTRIM(loss_cause
			)
		)
	) AS v_loss_cause,
	loss_entry_operator,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(loss_entry_operator))) OR IS_SPACES(LTRIM(RTRIM(loss_entry_operator))) OR LENGTH(LTRIM(RTRIM(loss_entry_operator)))  = 0,'N/A',LTRIM(RTRIM(loss_entry_operator)))
	IFF(LTRIM(RTRIM(loss_entry_operator
			)
		) IS NULL 
		OR LENGTH(LTRIM(RTRIM(loss_entry_operator
			)
		))>0 AND TRIM(LTRIM(RTRIM(loss_entry_operator
			)
		))='' 
		OR LENGTH(LTRIM(RTRIM(loss_entry_operator
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(loss_entry_operator
			)
		)
	) AS Out_loss_entry_operator,
	loss_cause,
	-- *INF*: :LKP.LKP_CLAIM_OCCURRENCE(v_loss_occurence_key)
	-- 
	-- --:LKP.LKPTRANS(v_loss_occurence_key)
	LKP_CLAIM_OCCURRENCE_v_loss_occurence_key.claim_occurrence_ak_id AS v_claim_occurence_ak_id,
	-- *INF*: :LKP.LKP_CLAIM_PARTY(v_loss_party_key)
	LKP_CLAIM_PARTY_v_loss_party_key.claim_party_ak_id AS v_claim_party_ak_id,
	-- *INF*: :LKP.LKP_CLAIM_PARTY_OCCURRENCE(v_claim_occurence_ak_id,v_claim_party_ak_id,v_party_role_code)
	LKP_CLAIM_PARTY_OCCURRENCE_v_claim_occurence_ak_id_v_claim_party_ak_id_v_party_role_code.claim_party_occurrence_ak_id AS v_claim_party_occurence_ak_id,
	-- *INF*: :LKP.LKP_CLAIMANT_DETAIL_COVERAGE(v_claim_party_occurence_ak_id, v_loss_location_number, v_loss_sub_location_number,  v_loss_insurance_line, v_loss_risk_unit_group, V_risk_unit_grp_seq_num ,v_loss_unit, v_loss_sequence_risk_unit, v_loss_major_peril, v_loss_major_peril_seq, v_loss_disability, v_loss_reserve_category, v_loss_cause, v_loss_member, v_loss_type_exposure)
	LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.claimant_cov_det_ak_id AS v_claimant_coverage_detail_ak_id,
	v_claimant_coverage_detail_ak_id AS claimant_coverage_detail_ak_id,
	loss_cost_containment,
	loss_draft_amount,
	-- *INF*: IIF(ISNULL(loss_draft_amount),0,loss_draft_amount)
	IFF(loss_draft_amount IS NULL,
		0,
		loss_draft_amount
	) AS Out_loss_draft_amount,
	loss_draft_no,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(loss_draft_no))) OR IS_SPACES(LTRIM(RTRIM(loss_draft_no))) OR LENGTH(LTRIM(RTRIM(loss_draft_no))) = 0, 'N/A', loss_draft_no)
	IFF(LTRIM(RTRIM(loss_draft_no
			)
		) IS NULL 
		OR LENGTH(LTRIM(RTRIM(loss_draft_no
			)
		))>0 AND TRIM(LTRIM(RTRIM(loss_draft_no
			)
		))='' 
		OR LENGTH(LTRIM(RTRIM(loss_draft_no
				)
			)
		) = 0,
		'N/A',
		loss_draft_no
	) AS Out_loss_draft_no,
	loss_transaction_date,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(loss_transaction_date))),TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),to_date(LTRIM(RTRIM(loss_transaction_date)),'YYYYMMDD'))
	IFF(LTRIM(RTRIM(loss_transaction_date
			)
		) IS NULL,
		TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		to_date(LTRIM(RTRIM(loss_transaction_date
				)
			), 'YYYYMMDD'
		)
	) AS Out_loss_transaction_date,
	loss_draft_pay_to_1,
	loss_draft_pay_to_2,
	loss_draft_pay_to_3,
	loss_draft_pay_to_1 || loss_draft_pay_to_2 || loss_draft_pay_to_3 AS V_pay_to_code,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(V_pay_to_code))) OR IS_SPACES(LTRIM(RTRIM(V_pay_to_code))) OR LENGTH(LTRIM(RTRIM(V_pay_to_code)))  = 0,'N/A',LTRIM(RTRIM(V_pay_to_code)))
	IFF(LTRIM(RTRIM(V_pay_to_code
			)
		) IS NULL 
		OR LENGTH(LTRIM(RTRIM(V_pay_to_code
			)
		))>0 AND TRIM(LTRIM(RTRIM(V_pay_to_code
			)
		))='' 
		OR LENGTH(LTRIM(RTRIM(V_pay_to_code
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(V_pay_to_code
			)
		)
	) AS pay_to_code,
	loss_draft_mail_to,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(loss_draft_mail_to))) OR IS_SPACES(LTRIM(RTRIM(loss_draft_mail_to))) OR LENGTH(LTRIM(RTRIM(loss_draft_mail_to)))  = 0,'N/A',LTRIM(RTRIM(loss_draft_mail_to)))
	IFF(LTRIM(RTRIM(loss_draft_mail_to
			)
		) IS NULL 
		OR LENGTH(LTRIM(RTRIM(loss_draft_mail_to
			)
		))>0 AND TRIM(LTRIM(RTRIM(loss_draft_mail_to
			)
		))='' 
		OR LENGTH(LTRIM(RTRIM(loss_draft_mail_to
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(loss_draft_mail_to
			)
		)
	) AS Out_loss_draft_mail_to,
	source_system_id AS SOURCE_SYSTEM_ID
	FROM SQ_pifmstr_PIF_4578_stage
	LEFT JOIN LKP_CLAIM_OCCURRENCE LKP_CLAIM_OCCURRENCE_v_loss_occurence_key
	ON LKP_CLAIM_OCCURRENCE_v_loss_occurence_key.claim_occurrence_key = v_loss_occurence_key

	LEFT JOIN LKP_CLAIM_PARTY LKP_CLAIM_PARTY_v_loss_party_key
	ON LKP_CLAIM_PARTY_v_loss_party_key.claim_party_key = v_loss_party_key

	LEFT JOIN LKP_CLAIM_PARTY_OCCURRENCE LKP_CLAIM_PARTY_OCCURRENCE_v_claim_occurence_ak_id_v_claim_party_ak_id_v_party_role_code
	ON LKP_CLAIM_PARTY_OCCURRENCE_v_claim_occurence_ak_id_v_claim_party_ak_id_v_party_role_code.claim_occurrence_ak_id = v_claim_occurence_ak_id
	AND LKP_CLAIM_PARTY_OCCURRENCE_v_claim_occurence_ak_id_v_claim_party_ak_id_v_party_role_code.claim_party_ak_id = v_claim_party_ak_id
	AND LKP_CLAIM_PARTY_OCCURRENCE_v_claim_occurence_ak_id_v_claim_party_ak_id_v_party_role_code.claim_party_role_code = v_party_role_code

	LEFT JOIN LKP_CLAIMANT_DETAIL_COVERAGE LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure
	ON LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.claim_party_occurrence_ak_id = v_claim_party_occurence_ak_id
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.loc_unit_num = v_loss_location_number
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.sub_loc_unit_num = v_loss_sub_location_number
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.ins_line = v_loss_insurance_line
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.risk_unit_grp = v_loss_risk_unit_group
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.risk_unit_grp_seq_num = V_risk_unit_grp_seq_num
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.risk_unit = v_loss_unit
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.risk_unit_seq_num = v_loss_sequence_risk_unit
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.major_peril_code = v_loss_major_peril
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.major_peril_seq = v_loss_major_peril_seq
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.pms_loss_disability = v_loss_disability
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.reserve_ctgry = v_loss_reserve_category
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.cause_of_loss = v_loss_cause
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.pms_mbr = v_loss_member
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.pms_type_exposure = v_loss_type_exposure

),
AGG_Cost_Containment_Amount AS (
	SELECT
	claimant_coverage_detail_ak_id,
	Out_loss_draft_amount,
	Out_loss_draft_no,
	Out_loss_transaction_date,
	Out_loss_entry_operator AS loss_entry_operator,
	pay_to_code,
	Out_loss_draft_mail_to AS loss_draft_mail_to,
	loss_cost_containment,
	-- *INF*: SUM(loss_cost_containment)
	SUM(loss_cost_containment
	) AS Out_loss_cost_containment,
	loss_transaction
	FROM EXP_CLAIM_TRANSACTION_VALIDATE
	GROUP BY claimant_coverage_detail_ak_id, Out_loss_draft_amount, Out_loss_draft_no, Out_loss_transaction_date
),
LKP_Claim_Payment_Ak_id AS (
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
		claim_payment.micro_ecd_draft_num as micro_ecd_draft_num, 
		claim_payment.total_pay_amt as total_pay_amt, 
		claim_payment.pay_issued_date as pay_issued_date 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_payment
		WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pms_claimant_cov_det_ak_id,micro_ecd_draft_num,total_pay_amt,pay_issued_date ORDER BY claim_pay_ak_id) = 1
),
LKP_Financial_Type_Code AS (
	SELECT
	edw_financial_type_code,
	pms_trans_code
	FROM (
		SELECT 
			edw_financial_type_code,
			pms_trans_code
		FROM sup_convert_pms_claim_transaction_code
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pms_trans_code ORDER BY edw_financial_type_code) = 1
),
EXP_set_defaults_txn AS (
	SELECT
	LKP_Claim_Payment_Ak_id.claim_pay_ak_id AS in_claim_pay_ak_id,
	-- *INF*: in_claim_pay_ak_id
	-- 
	-- --IIF(ISNULL(in_claim_pay_ak_id), -1, in_claim_pay_ak_id)
	in_claim_pay_ak_id AS claim_pay_ak_id,
	'CC' AS claim_pay_ctgry_type,
	-1 AS claim_pay_ctgry_seq_num,
	LKP_Financial_Type_Code.edw_financial_type_code,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(edw_financial_type_code))) OR LENGTH(LTRIM(RTRIM(edw_financial_type_code))) = 0, 'N/A', LTRIM(RTRIM(edw_financial_type_code)))
	IFF(LTRIM(RTRIM(edw_financial_type_code
			)
		) IS NULL 
		OR LENGTH(LTRIM(RTRIM(edw_financial_type_code
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(edw_financial_type_code
			)
		)
	) AS financial_type_code,
	AGG_Cost_Containment_Amount.Out_loss_cost_containment AS loss_cost_containment,
	-- *INF*: IIF(ISNULL(loss_cost_containment), 0, loss_cost_containment)
	IFF(loss_cost_containment IS NULL,
		0,
		loss_cost_containment
	) AS cost_containment_saving_amt,
	0 AS AMT_default,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
	) AS claim_pay_ctgry_start_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS claim_pay_ctgry_end_date,
	'N/A' AS char_default,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	AGG_Cost_Containment_Amount.claimant_coverage_detail_ak_id,
	AGG_Cost_Containment_Amount.Out_loss_transaction_date,
	AGG_Cost_Containment_Amount.Out_loss_draft_amount,
	AGG_Cost_Containment_Amount.Out_loss_draft_no,
	'N/A' AS claim_pay_ctgry_litigated_ind,
	'N/A' AS claim_pay_ctgry_lump_sum_ind,
	'N/A' AS cov_ctgry_code,
	'0' AS BenefitOffsetCode,
	0.00 AS BenefitOffsetAmount
	FROM AGG_Cost_Containment_Amount
	LEFT JOIN LKP_Claim_Payment_Ak_id
	ON LKP_Claim_Payment_Ak_id.pms_claimant_cov_det_ak_id = AGG_Cost_Containment_Amount.claimant_coverage_detail_ak_id AND LKP_Claim_Payment_Ak_id.micro_ecd_draft_num = AGG_Cost_Containment_Amount.Out_loss_draft_no AND LKP_Claim_Payment_Ak_id.total_pay_amt = AGG_Cost_Containment_Amount.Out_loss_draft_amount AND LKP_Claim_Payment_Ak_id.pay_issued_date = AGG_Cost_Containment_Amount.Out_loss_transaction_date
	LEFT JOIN LKP_Financial_Type_Code
	ON LKP_Financial_Type_Code.pms_trans_code = AGG_Cost_Containment_Amount.loss_transaction
),
LKP_Claim_Payment_Ctgry AS (
	SELECT
	claim_pay_ctgry_id,
	claim_pay_ak_id,
	claim_pay_ctgry_type,
	claim_pay_ctgry_seq_num
	FROM (
		SELECT 
		claim_payment_category.claim_pay_ctgry_id as claim_pay_ctgry_id, 
		claim_payment_category.claim_pay_ak_id as claim_pay_ak_id, 
		claim_payment_category.claim_pay_ctgry_type as claim_pay_ctgry_type, 
		claim_payment_category.claim_pay_ctgry_seq_num as claim_pay_ctgry_seq_num 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_payment_category
		WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_pay_ak_id,claim_pay_ctgry_type,claim_pay_ctgry_seq_num ORDER BY claim_pay_ctgry_id) = 1
),
RTR_claim_payment_category_txn AS (
	SELECT
	LKP_Claim_Payment_Ctgry.claim_pay_ctgry_id,
	EXP_set_defaults_txn.claim_pay_ak_id,
	EXP_set_defaults_txn.claim_pay_ctgry_type,
	EXP_set_defaults_txn.claim_pay_ctgry_seq_num,
	EXP_set_defaults_txn.financial_type_code,
	EXP_set_defaults_txn.cost_containment_saving_amt,
	EXP_set_defaults_txn.AMT_default,
	EXP_set_defaults_txn.claim_pay_ctgry_start_date,
	EXP_set_defaults_txn.claim_pay_ctgry_end_date,
	EXP_set_defaults_txn.char_default,
	EXP_set_defaults_txn.crrnt_snpsht_flag,
	EXP_set_defaults_txn.audit_id,
	EXP_set_defaults_txn.eff_from_date,
	EXP_set_defaults_txn.eff_to_date,
	EXP_set_defaults_txn.source_sys_id,
	EXP_set_defaults_txn.created_date,
	EXP_set_defaults_txn.modified_date,
	EXP_set_defaults_txn.claim_pay_ctgry_litigated_ind,
	EXP_set_defaults_txn.claim_pay_ctgry_lump_sum_ind,
	EXP_set_defaults_txn.cov_ctgry_code,
	EXP_set_defaults_txn.BenefitOffsetCode,
	EXP_set_defaults_txn.BenefitOffsetAmount
	FROM EXP_set_defaults_txn
	LEFT JOIN LKP_Claim_Payment_Ctgry
	ON LKP_Claim_Payment_Ctgry.claim_pay_ak_id = EXP_set_defaults_txn.claim_pay_ak_id AND LKP_Claim_Payment_Ctgry.claim_pay_ctgry_type = EXP_set_defaults_txn.claim_pay_ctgry_type AND LKP_Claim_Payment_Ctgry.claim_pay_ctgry_seq_num = EXP_set_defaults_txn.claim_pay_ctgry_seq_num
),
RTR_claim_payment_category_txn_Insert AS (SELECT * FROM RTR_claim_payment_category_txn WHERE ISNULL(claim_pay_ctgry_id)),
RTR_claim_payment_category_txn_Update AS (SELECT * FROM RTR_claim_payment_category_txn WHERE NOT ISNULL(claim_pay_ctgry_id)),
UPD_claim_payment_category_txn_update AS (
	SELECT
	claim_pay_ctgry_id AS claim_pay_ctgry_id3, 
	claim_pay_ak_id AS claim_pay_ak_id3, 
	claim_pay_ctgry_type, 
	claim_pay_ctgry_seq_num, 
	financial_type_code, 
	cost_containment_saving_amt, 
	AMT_default AS AMT_default3, 
	claim_pay_ctgry_start_date, 
	claim_pay_ctgry_end_date, 
	char_default AS char_default3, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag3, 
	audit_id AS audit_id3, 
	eff_from_date AS eff_from_date3, 
	eff_to_date AS eff_to_date3, 
	source_sys_id AS source_sys_id3, 
	created_date AS created_date3, 
	modified_date AS modified_date3, 
	claim_pay_ctgry_litigated_ind AS claim_pay_ctgry_litigated_ind3, 
	claim_pay_ctgry_lump_sum_ind AS claim_pay_ctgry_lump_sum_ind3, 
	cov_ctgry_code, 
	BenefitOffsetCode AS BenefitOffsetCode3, 
	BenefitOffsetAmount AS BenefitOffsetAmount3
	FROM RTR_claim_payment_category_txn_Update
),
claim_payment_category_txn_update AS (
	MERGE INTO claim_payment_category AS T
	USING UPD_claim_payment_category_txn_update AS S
	ON T.claim_pay_ctgry_id = S.claim_pay_ctgry_id3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.cost_containment_saving_amt = S.cost_containment_saving_amt, T.modified_date = S.modified_date3, T.cov_ctgry_code = S.cov_ctgry_code, T.BenefitOffsetCode = S.BenefitOffsetCode3, T.BenefitOffsetAmount = S.BenefitOffsetAmount3
),
SEQ_Claim_payment_category_AK_ID AS (
	CREATE SEQUENCE SEQ_Claim_payment_category_AK_ID
	START = 0
	INCREMENT = 1;
),
UPD_claim_payment_category_txn_insert AS (
	SELECT
	claim_pay_ctgry_id AS claim_pay_ctgry_id1, 
	claim_pay_ak_id AS claim_pay_ak_id1, 
	claim_pay_ctgry_type, 
	claim_pay_ctgry_seq_num, 
	financial_type_code, 
	cost_containment_saving_amt, 
	AMT_default AS AMT_default1, 
	claim_pay_ctgry_start_date, 
	claim_pay_ctgry_end_date, 
	char_default AS char_default1, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag1, 
	audit_id AS audit_id1, 
	eff_from_date AS eff_from_date1, 
	eff_to_date AS eff_to_date1, 
	source_sys_id AS source_sys_id1, 
	created_date AS created_date1, 
	modified_date AS modified_date1, 
	claim_pay_ctgry_litigated_ind AS claim_pay_ctgry_litigated_ind1, 
	claim_pay_ctgry_lump_sum_ind AS claim_pay_ctgry_lump_sum_ind1, 
	cov_ctgry_code, 
	BenefitOffsetCode AS BenefitOffsetCode1, 
	BenefitOffsetAmount AS BenefitOffsetAmount1
	FROM RTR_claim_payment_category_txn_Insert
),
claim_payment_category_txn_insert AS (
	INSERT INTO claim_payment_category
	(claim_pay_ctgry_ak_id, claim_pay_ak_id, claim_pay_ctgry_type, claim_pay_ctgry_seq_num, claim_pay_ctgry_amt, claim_pay_ctgry_earned_amt, claim_pay_ctgry_billed_amt, claim_pay_ctgry_start_date, claim_pay_ctgry_end_date, financial_type_code, invc_num, cost_containment_saving_amt, cost_containment_red_amt, cost_containment_ppo_amt, attorney_fee_amt, attorney_cost_amt, attorney_file_num, hourly_rate, hours_worked, num_of_days, num_of_weeks, tpd_rate, tpd_rate_fac, tpd_wage_loss, tpd_wkly_wage, claim_pay_ctgry_comment, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, claim_pay_ctgry_litigated_ind, claim_pay_ctgry_lump_sum_ind, cov_ctgry_code, BenefitOffsetCode, BenefitOffsetAmount)
	SELECT 
	SEQ_Claim_payment_category_AK_ID.NEXTVAL AS CLAIM_PAY_CTGRY_AK_ID, 
	claim_pay_ak_id1 AS CLAIM_PAY_AK_ID, 
	CLAIM_PAY_CTGRY_TYPE, 
	CLAIM_PAY_CTGRY_SEQ_NUM, 
	AMT_default1 AS CLAIM_PAY_CTGRY_AMT, 
	AMT_default1 AS CLAIM_PAY_CTGRY_EARNED_AMT, 
	AMT_default1 AS CLAIM_PAY_CTGRY_BILLED_AMT, 
	CLAIM_PAY_CTGRY_START_DATE, 
	CLAIM_PAY_CTGRY_END_DATE, 
	FINANCIAL_TYPE_CODE, 
	char_default1 AS INVC_NUM, 
	COST_CONTAINMENT_SAVING_AMT, 
	AMT_default1 AS COST_CONTAINMENT_RED_AMT, 
	AMT_default1 AS COST_CONTAINMENT_PPO_AMT, 
	AMT_default1 AS ATTORNEY_FEE_AMT, 
	AMT_default1 AS ATTORNEY_COST_AMT, 
	char_default1 AS ATTORNEY_FILE_NUM, 
	AMT_default1 AS HOURLY_RATE, 
	AMT_default1 AS HOURS_WORKED, 
	claim_pay_ctgry_seq_num AS NUM_OF_DAYS, 
	claim_pay_ctgry_seq_num AS NUM_OF_WEEKS, 
	AMT_default1 AS TPD_RATE, 
	AMT_default1 AS TPD_RATE_FAC, 
	AMT_default1 AS TPD_WAGE_LOSS, 
	AMT_default1 AS TPD_WKLY_WAGE, 
	char_default1 AS CLAIM_PAY_CTGRY_COMMENT, 
	crrnt_snpsht_flag1 AS CRRNT_SNPSHT_FLAG, 
	audit_id1 AS AUDIT_ID, 
	eff_from_date1 AS EFF_FROM_DATE, 
	eff_to_date1 AS EFF_TO_DATE, 
	source_sys_id1 AS SOURCE_SYS_ID, 
	created_date1 AS CREATED_DATE, 
	modified_date1 AS MODIFIED_DATE, 
	claim_pay_ctgry_litigated_ind1 AS CLAIM_PAY_CTGRY_LITIGATED_IND, 
	claim_pay_ctgry_lump_sum_ind1 AS CLAIM_PAY_CTGRY_LUMP_SUM_IND, 
	COV_CTGRY_CODE, 
	BenefitOffsetCode1 AS BENEFITOFFSETCODE, 
	BenefitOffsetAmount1 AS BENEFITOFFSETAMOUNT
	FROM UPD_claim_payment_category_txn_insert
),
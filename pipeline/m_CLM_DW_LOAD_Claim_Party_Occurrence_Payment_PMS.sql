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
LKP_PMS_Adjustor AS (
	SELECT
	adnm_type_adjustor,
	in_Claim_Adjustor_Nbr,
	adnm_adjustor_nbr
	FROM (
		SELECT 
			adnm_type_adjustor,
			in_Claim_Adjustor_Nbr,
			adnm_adjustor_nbr
		FROM pms_adjuster_master_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY adnm_adjustor_nbr ORDER BY adnm_type_adjustor) = 1
),
SQ_pifmstr_PIF_4578_stage AS (
	SELECT LTRIM(RTRIM(pif_4578_stage.pif_symbol)), 
	LTRIM(RTRIM(pif_4578_stage.pif_policy_number)), 
	LTRIM(RTRIM(pif_4578_stage.pif_module)), 
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
	LTRIM(RTRIM(pif_4578_stage.loss_entry_operator)), 
	LTRIM(RTRIM(pif_4578_stage.loss_cause)), 
	LTRIM(RTRIM(pif_4578_stage.loss_adjustor_no)), 
	LTRIM(RTRIM(pif_4578_stage.loss_bank_number)), 
	LTRIM(RTRIM(pif_4578_stage.loss_draft_amount)), 
	LTRIM(RTRIM(pif_4578_stage.loss_draft_no)), 
	LTRIM(RTRIM(pif_4578_stage.loss_draft_check_ind)), 
	LTRIM(RTRIM(pif_4578_stage.loss_transaction_date)), 
	LTRIM(RTRIM(pif_4578_stage.loss_draft_pay_to_1)), 
	LTRIM(RTRIM(pif_4578_stage.loss_draft_pay_to_2)), 
	LTRIM(RTRIM(pif_4578_stage.loss_draft_pay_to_3)), 
	LTRIM(RTRIM(pif_4578_stage.loss_draft_mail_to)), 
	LTRIM(RTRIM(pif_4578_stage.loss_payee_phrase)), 
	LTRIM(RTRIM(pif_4578_stage.loss_memo_phrase)), 
	LTRIM(RTRIM(pif_4578_stage.loss_1099_number)), 
	LTRIM(RTRIM(pif_4578_stage.loss_claim_payee_name)), 
	LTRIM(RTRIM(pif_4578_stage.source_system_id)) 
	FROM
	 pif_4578_stage 
	WHERE
	 (pif_4578_stage.loss_part = '7') AND 
	(pif_4578_stage.logical_flag ='0')
	AND LEN(LTRIM(RTRIM(pif_4578_stage.loss_draft_no))) > 0
),
EXP_CLAIM_TRANSACTION_VALIDATE AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module,
	loss_insurance_line,
	-- *INF*: IIF(ISNULL(loss_insurance_line) OR LENGTH(LTRIM(RTRIM(loss_insurance_line))) = 0, 'N/A', LTRIM(RTRIM(loss_insurance_line)))
	IFF(loss_insurance_line IS NULL OR LENGTH(LTRIM(RTRIM(loss_insurance_line))) = 0, 'N/A', LTRIM(RTRIM(loss_insurance_line))) AS v_loss_insurance_line,
	loss_location_number,
	-- *INF*: IIF(ISNULL(loss_location_number), '0', to_char(loss_location_number))
	IFF(loss_location_number IS NULL, '0', to_char(loss_location_number)) AS v_loss_location_number,
	loss_sub_location_number,
	-- *INF*: IIF(ISNULL(loss_sub_location_number), '0', to_char(loss_sub_location_number))
	IFF(loss_sub_location_number IS NULL, '0', to_char(loss_sub_location_number)) AS v_loss_sub_location_number,
	loss_risk_unit_group,
	-- *INF*: IIF(ISNULL(loss_risk_unit_group) OR LENGTH(LTRIM(RTRIM(loss_risk_unit_group))) = 0, 'N/A', LTRIM(RTRIM(loss_risk_unit_group)))
	IFF(loss_risk_unit_group IS NULL OR LENGTH(LTRIM(RTRIM(loss_risk_unit_group))) = 0, 'N/A', LTRIM(RTRIM(loss_risk_unit_group))) AS v_loss_risk_unit_group,
	loss_class_code_group,
	loss_class_code_member,
	loss_unit,
	-- *INF*: IIF(ISNULL(loss_unit) OR LENGTH(LTRIM(RTRIM(loss_unit))) = 0, 'N/A', LTRIM(RTRIM(loss_unit)))
	IFF(loss_unit IS NULL OR LENGTH(LTRIM(RTRIM(loss_unit))) = 0, 'N/A', LTRIM(RTRIM(loss_unit))) AS v_loss_unit,
	loss_sequence_risk_unit,
	-- *INF*: IIF(TO_DECIMAL(loss_sequence_risk_unit) = 0 OR ISNULL(LTRIM(RTRIM(loss_sequence_risk_unit))), '0',
	-- to_char(TO_DECIMAL(loss_sequence_risk_unit)) )
	IFF(TO_DECIMAL(loss_sequence_risk_unit) = 0 OR LTRIM(RTRIM(loss_sequence_risk_unit)) IS NULL, '0', to_char(TO_DECIMAL(loss_sequence_risk_unit))) AS v_loss_sequence_risk_unit,
	loss_type_exposure,
	-- *INF*: IIF(ISNULL(loss_type_exposure) OR LENGTH(LTRIM(RTRIM(loss_type_exposure))) = 0, 'N/A', LTRIM(RTRIM(loss_type_exposure)))
	IFF(loss_type_exposure IS NULL OR LENGTH(LTRIM(RTRIM(loss_type_exposure))) = 0, 'N/A', LTRIM(RTRIM(loss_type_exposure))) AS v_loss_type_exposure,
	loss_major_peril,
	-- *INF*: IIF(ISNULL(loss_major_peril) OR LENGTH(LTRIM(RTRIM(loss_major_peril))) = 0, 'N/A', LTRIM(RTRIM(loss_major_peril)))
	IFF(loss_major_peril IS NULL OR LENGTH(LTRIM(RTRIM(loss_major_peril))) = 0, 'N/A', LTRIM(RTRIM(loss_major_peril))) AS v_loss_major_peril,
	loss_major_peril_seq,
	-- *INF*: IIF(ISNULL(loss_major_peril_seq) OR LENGTH(LTRIM(RTRIM(loss_major_peril_seq))) = 0, 'N/A', LTRIM(RTRIM(loss_major_peril_seq)))
	IFF(loss_major_peril_seq IS NULL OR LENGTH(LTRIM(RTRIM(loss_major_peril_seq))) = 0, 'N/A', LTRIM(RTRIM(loss_major_peril_seq))) AS v_loss_major_peril_seq,
	-- *INF*: IIF(ISNULL(TO_CHAR(loss_class_code_group)||TO_CHAR(loss_class_code_member)) ,'N/A',TO_CHAR(loss_class_code_group)||TO_CHAR(loss_class_code_member))
	IFF(TO_CHAR(loss_class_code_group) || TO_CHAR(loss_class_code_member) IS NULL, 'N/A', TO_CHAR(loss_class_code_group) || TO_CHAR(loss_class_code_member)) AS V_risk_unit_grp_seq_num_1,
	-- *INF*: LPAD(V_risk_unit_grp_seq_num_1,3,'0')
	LPAD(V_risk_unit_grp_seq_num_1, 3, '0') AS V_risk_unit_grp_seq_num,
	loss_year,
	loss_month,
	loss_day,
	-- *INF*: TO_CHAR(loss_year)
	TO_CHAR(loss_year) AS V_loss_year,
	-- *INF*: to_char(loss_month)
	to_char(loss_month) AS V_loss_month,
	-- *INF*: to_char(loss_day)
	to_char(loss_day) AS V_loss_day,
	-- *INF*: IIF ( LENGTH(V_loss_month) = 1, '0' || V_loss_month, V_loss_month)
	-- ||  
	-- IIF ( LENGTH(V_loss_day ) = 1, '0' || V_loss_day, V_loss_day )
	-- ||  
	-- V_loss_year
	IFF(LENGTH(V_loss_month) = 1, '0' || V_loss_month, V_loss_month) || IFF(LENGTH(V_loss_day) = 1, '0' || V_loss_day, V_loss_day) || V_loss_year AS v_loss_date,
	loss_occurence,
	pif_symbol || pif_policy_number || pif_module || v_loss_date || loss_occurence AS v_loss_occurence_key,
	loss_claimant,
	-- *INF*: :LKP.LKP_PMS_ADJUSTOR(IN_loss_adjustor_no)
	-- 
	-- --'CMT'
	LKP_PMS_ADJUSTOR_IN_loss_adjustor_no.adnm_type_adjustor AS v_party_role_code_payee,
	v_party_role_code_payee AS o_party_role_code_payee,
	'CMT' AS v_party_role_code_claimant,
	IN_loss_adjustor_no || v_party_role_code_payee AS v_loss_party_key_payee,
	-- *INF*: v_loss_occurence_key||TO_CHAR(loss_claimant)||v_party_role_code_claimant
	v_loss_occurence_key || TO_CHAR(loss_claimant) || v_party_role_code_claimant AS v_loss_party_key_claimant,
	loss_member,
	-- *INF*: IIF(ISNULL(loss_member) OR LENGTH(LTRIM(RTRIM(loss_member))) = 0, 'N/A', loss_member)
	IFF(loss_member IS NULL OR LENGTH(LTRIM(RTRIM(loss_member))) = 0, 'N/A', loss_member) AS v_loss_member,
	loss_disability,
	-- *INF*: IIF(ISNULL(loss_disability) OR LENGTH(LTRIM(RTRIM(loss_disability))) = 0, 'N/A', loss_disability)
	IFF(loss_disability IS NULL OR LENGTH(LTRIM(RTRIM(loss_disability))) = 0, 'N/A', loss_disability) AS v_loss_disability,
	loss_reserve_category,
	-- *INF*: IIF(ISNULL(loss_reserve_category) OR LENGTH(LTRIM(RTRIM(loss_reserve_category))) = 0, 'N/A', loss_reserve_category)
	IFF(loss_reserve_category IS NULL OR LENGTH(LTRIM(RTRIM(loss_reserve_category))) = 0, 'N/A', loss_reserve_category) AS v_loss_reserve_category,
	loss_cause,
	-- *INF*: decode(true,
	-- ISNULL(loss_cause), 'N/A',
	-- LENGTH(LTRIM(RTRIM(loss_cause))) = 0, 'N/A',
	-- (v_loss_major_peril = '032' AND loss_cause = '07'), '06',
	-- LTRIM(RTRIM(loss_cause)))
	-- 
	-- 
	-- 
	-- --IIF(ISNULL(loss_cause) OR LENGTH(LTRIM(RTRIM(loss_cause))) = 0, 'N/A',loss_cause)
	decode(true,
	loss_cause IS NULL, 'N/A',
	LENGTH(LTRIM(RTRIM(loss_cause))) = 0, 'N/A',
	( v_loss_major_peril = '032' AND loss_cause = '07' ), '06',
	LTRIM(RTRIM(loss_cause))) AS v_loss_cause,
	-- *INF*: :LKP.LKP_CLAIM_OCCURRENCE(v_loss_occurence_key)
	-- 
	-- --:LKP.LKPTRANS(v_loss_occurence_key)
	LKP_CLAIM_OCCURRENCE_v_loss_occurence_key.claim_occurrence_ak_id AS v_claim_occurence_ak_id,
	-- *INF*: :LKP.LKP_CLAIM_PARTY(v_loss_party_key_payee)
	LKP_CLAIM_PARTY_v_loss_party_key_payee.claim_party_ak_id AS v_claim_party_ak_id_payee,
	-- *INF*: :LKP.LKP_CLAIM_PARTY_OCCURRENCE(v_claim_occurence_ak_id,v_claim_party_ak_id_payee,v_party_role_code_payee)
	LKP_CLAIM_PARTY_OCCURRENCE_v_claim_occurence_ak_id_v_claim_party_ak_id_payee_v_party_role_code_payee.claim_party_occurrence_ak_id AS v_claim_party_occurence_ak_id_payee,
	v_claim_party_occurence_ak_id_payee AS o_claim_party_occurence_ak_id_payee,
	-- *INF*: :LKP.LKP_CLAIM_PARTY(v_loss_party_key_claimant)
	LKP_CLAIM_PARTY_v_loss_party_key_claimant.claim_party_ak_id AS v_claim_party_ak_id_claimant,
	-- *INF*: :LKP.LKP_CLAIM_PARTY_OCCURRENCE(v_claim_occurence_ak_id,v_claim_party_ak_id_claimant,v_party_role_code_claimant)
	LKP_CLAIM_PARTY_OCCURRENCE_v_claim_occurence_ak_id_v_claim_party_ak_id_claimant_v_party_role_code_claimant.claim_party_occurrence_ak_id AS v_claim_party_occurence_ak_id_claimant,
	-- *INF*: :LKP.LKP_CLAIMANT_DETAIL_COVERAGE(v_claim_party_occurence_ak_id_claimant, v_loss_location_number, v_loss_sub_location_number,  v_loss_insurance_line, v_loss_risk_unit_group, V_risk_unit_grp_seq_num ,v_loss_unit, v_loss_sequence_risk_unit, v_loss_major_peril, v_loss_major_peril_seq, v_loss_disability, v_loss_reserve_category, v_loss_cause, v_loss_member, v_loss_type_exposure)
	LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_claimant_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.claimant_cov_det_ak_id AS v_claimant_coverage_detail_ak_id,
	v_claimant_coverage_detail_ak_id AS claimant_coverage_detail_ak_id,
	loss_entry_operator AS IN_LOSS_ENTRY_OPERATOR,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_ENTRY_OPERATOR))) OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_ENTRY_OPERATOR))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_ENTRY_OPERATOR))) = 0, 'N/A', IN_LOSS_ENTRY_OPERATOR)
	-- 
	-- --IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_ENTRY_OPERATOR))),'N/A',IIF(IS_SPACES(IN_LOSS_ENTRY_OPERATOR),'N/A',LTRIM(RTRIM(IN_LOSS_ENTRY_OPERATOR))))
	IFF(LTRIM(RTRIM(IN_LOSS_ENTRY_OPERATOR)) IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_ENTRY_OPERATOR))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_ENTRY_OPERATOR))) = 0, 'N/A', IN_LOSS_ENTRY_OPERATOR) AS LOSS_ENTRY_OPERATOR,
	loss_bank_number AS IN_LOSS_BANK_NUMBER,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_BANK_NUMBER))) OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_BANK_NUMBER))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_BANK_NUMBER))) = 0, 'N/A',LTRIM(RTRIM(IN_LOSS_BANK_NUMBER)))
	IFF(LTRIM(RTRIM(IN_LOSS_BANK_NUMBER)) IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_BANK_NUMBER))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_BANK_NUMBER))) = 0, 'N/A', LTRIM(RTRIM(IN_LOSS_BANK_NUMBER))) AS LOSS_BANK_NUMBER,
	loss_draft_amount AS IN_LOSS_DRAFT_AMOUNT,
	-- *INF*: IIF(ISNULL(IN_LOSS_DRAFT_AMOUNT),0,IN_LOSS_DRAFT_AMOUNT)
	IFF(IN_LOSS_DRAFT_AMOUNT IS NULL, 0, IN_LOSS_DRAFT_AMOUNT) AS LOSS_DRAFT_AMOUNT,
	loss_draft_no AS IN_LOSS_DRAFT_NO,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_DRAFT_NO))) OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_DRAFT_NO))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_DRAFT_NO))) = 0, 'N/A', LTRIM(RTRIM(IN_LOSS_DRAFT_NO)))
	IFF(LTRIM(RTRIM(IN_LOSS_DRAFT_NO)) IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_DRAFT_NO))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_DRAFT_NO))) = 0, 'N/A', LTRIM(RTRIM(IN_LOSS_DRAFT_NO))) AS LOSS_DRAFT_NO,
	loss_draft_check_ind AS IN_LOSS_DRAFT_CHECK_IND,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_DRAFT_CHECK_IND))) OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_DRAFT_CHECK_IND))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_DRAFT_CHECK_IND))) = 0, 'N/A',LTRIM(RTRIM(IN_LOSS_DRAFT_CHECK_IND)))
	IFF(LTRIM(RTRIM(IN_LOSS_DRAFT_CHECK_IND)) IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_DRAFT_CHECK_IND))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_DRAFT_CHECK_IND))) = 0, 'N/A', LTRIM(RTRIM(IN_LOSS_DRAFT_CHECK_IND))) AS LOSS_DRAFT_CHECK_IND,
	loss_transaction_date AS IN_LOSS_TRANSACTION_DATE,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE))),TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),to_date(LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE)),'YYYYMMDD'))
	IFF(LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE)) IS NULL, TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), to_date(LTRIM(RTRIM(IN_LOSS_TRANSACTION_DATE)), 'YYYYMMDD')) AS LOSS_TRANSACTION_DATE,
	loss_draft_pay_to_1 AS IN_LOSS_DRAFT_PAY_TO_1,
	loss_draft_pay_to_2 AS IN_LOSS_DRAFT_PAY_TO_2,
	loss_draft_pay_to_3 AS IN_LOSS_DRAFT_PAY_TO_3,
	IN_LOSS_DRAFT_PAY_TO_1||IN_LOSS_DRAFT_PAY_TO_2||IN_LOSS_DRAFT_PAY_TO_3 AS v_pay_to_code,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(v_pay_to_code))) OR IS_SPACES(LTRIM(RTRIM(v_pay_to_code))) OR LENGTH(LTRIM(RTRIM(v_pay_to_code))) = 0, 'N/A',LTRIM(RTRIM(v_pay_to_code)))
	IFF(LTRIM(RTRIM(v_pay_to_code)) IS NULL OR IS_SPACES(LTRIM(RTRIM(v_pay_to_code))) OR LENGTH(LTRIM(RTRIM(v_pay_to_code))) = 0, 'N/A', LTRIM(RTRIM(v_pay_to_code))) AS pay_to_code,
	loss_draft_mail_to AS IN_LOSS_DRAFT_MAIL_TO,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_DRAFT_MAIL_TO))) OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_DRAFT_MAIL_TO))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_DRAFT_MAIL_TO))) = 0, 'N/A',LTRIM(RTRIM(IN_LOSS_DRAFT_MAIL_TO)))
	IFF(LTRIM(RTRIM(IN_LOSS_DRAFT_MAIL_TO)) IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_DRAFT_MAIL_TO))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_DRAFT_MAIL_TO))) = 0, 'N/A', LTRIM(RTRIM(IN_LOSS_DRAFT_MAIL_TO))) AS LOSS_DRAFT_MAIL_TO,
	loss_payee_phrase AS IN_LOSS_PAYEE_PHRASE,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_PAYEE_PHRASE))) OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_PAYEE_PHRASE))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_PAYEE_PHRASE))) = 0, 'N/A',LTRIM(RTRIM(IN_LOSS_PAYEE_PHRASE)))
	IFF(LTRIM(RTRIM(IN_LOSS_PAYEE_PHRASE)) IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_PAYEE_PHRASE))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_PAYEE_PHRASE))) = 0, 'N/A', LTRIM(RTRIM(IN_LOSS_PAYEE_PHRASE))) AS LOSS_PAYEE_PHRASE,
	loss_memo_phrase AS IN_LOSS_MEMO_PHRASE,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_MEMO_PHRASE))) OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_MEMO_PHRASE))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_MEMO_PHRASE))) = 0, 'N/A',LTRIM(RTRIM(IN_LOSS_MEMO_PHRASE)))
	IFF(LTRIM(RTRIM(IN_LOSS_MEMO_PHRASE)) IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_MEMO_PHRASE))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_MEMO_PHRASE))) = 0, 'N/A', LTRIM(RTRIM(IN_LOSS_MEMO_PHRASE))) AS LOSS_MEMO_PHRASE,
	loss_1099_number AS IN_LOSS_1099_NUMBER,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_1099_NUMBER))) OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_1099_NUMBER))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_1099_NUMBER))) = 0, 'N/A',LTRIM(RTRIM(IN_LOSS_1099_NUMBER)))
	IFF(LTRIM(RTRIM(IN_LOSS_1099_NUMBER)) IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_1099_NUMBER))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_1099_NUMBER))) = 0, 'N/A', LTRIM(RTRIM(IN_LOSS_1099_NUMBER))) AS LOSS_1099_NUMBER,
	loss_claim_payee_name AS IN_LOSS_CLAIM_PAYEE_NAME,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_CLAIM_PAYEE_NAME))) OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_CLAIM_PAYEE_NAME))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_CLAIM_PAYEE_NAME))) = 0, 'N/A',LTRIM(RTRIM(IN_LOSS_CLAIM_PAYEE_NAME)))
	IFF(LTRIM(RTRIM(IN_LOSS_CLAIM_PAYEE_NAME)) IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_CLAIM_PAYEE_NAME))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_CLAIM_PAYEE_NAME))) = 0, 'N/A', LTRIM(RTRIM(IN_LOSS_CLAIM_PAYEE_NAME))) AS LOSS_CLAIM_PAYEE_NAME,
	loss_adjustor_no AS IN_loss_adjustor_no,
	-- *INF*: IIF(ISNULL(IN_loss_adjustor_no) OR IS_SPACES(LTRIM(RTRIM(IN_loss_adjustor_no))) OR LENGTH(LTRIM(RTRIM(IN_loss_adjustor_no))) = 0, 'N/A', LTRIM(RTRIM(IN_loss_adjustor_no)))
	IFF(IN_loss_adjustor_no IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_loss_adjustor_no))) OR LENGTH(LTRIM(RTRIM(IN_loss_adjustor_no))) = 0, 'N/A', LTRIM(RTRIM(IN_loss_adjustor_no))) AS v_loss_adjustor_no,
	v_loss_adjustor_no AS o_loss_adjustor_no,
	source_system_id
	FROM SQ_pifmstr_PIF_4578_stage
	LEFT JOIN LKP_PMS_ADJUSTOR LKP_PMS_ADJUSTOR_IN_loss_adjustor_no
	ON LKP_PMS_ADJUSTOR_IN_loss_adjustor_no.adnm_adjustor_nbr = IN_loss_adjustor_no

	LEFT JOIN LKP_CLAIM_OCCURRENCE LKP_CLAIM_OCCURRENCE_v_loss_occurence_key
	ON LKP_CLAIM_OCCURRENCE_v_loss_occurence_key.claim_occurrence_key = v_loss_occurence_key

	LEFT JOIN LKP_CLAIM_PARTY LKP_CLAIM_PARTY_v_loss_party_key_payee
	ON LKP_CLAIM_PARTY_v_loss_party_key_payee.claim_party_key = v_loss_party_key_payee

	LEFT JOIN LKP_CLAIM_PARTY_OCCURRENCE LKP_CLAIM_PARTY_OCCURRENCE_v_claim_occurence_ak_id_v_claim_party_ak_id_payee_v_party_role_code_payee
	ON LKP_CLAIM_PARTY_OCCURRENCE_v_claim_occurence_ak_id_v_claim_party_ak_id_payee_v_party_role_code_payee.claim_occurrence_ak_id = v_claim_occurence_ak_id
	AND LKP_CLAIM_PARTY_OCCURRENCE_v_claim_occurence_ak_id_v_claim_party_ak_id_payee_v_party_role_code_payee.claim_party_ak_id = v_claim_party_ak_id_payee
	AND LKP_CLAIM_PARTY_OCCURRENCE_v_claim_occurence_ak_id_v_claim_party_ak_id_payee_v_party_role_code_payee.claim_party_role_code = v_party_role_code_payee

	LEFT JOIN LKP_CLAIM_PARTY LKP_CLAIM_PARTY_v_loss_party_key_claimant
	ON LKP_CLAIM_PARTY_v_loss_party_key_claimant.claim_party_key = v_loss_party_key_claimant

	LEFT JOIN LKP_CLAIM_PARTY_OCCURRENCE LKP_CLAIM_PARTY_OCCURRENCE_v_claim_occurence_ak_id_v_claim_party_ak_id_claimant_v_party_role_code_claimant
	ON LKP_CLAIM_PARTY_OCCURRENCE_v_claim_occurence_ak_id_v_claim_party_ak_id_claimant_v_party_role_code_claimant.claim_occurrence_ak_id = v_claim_occurence_ak_id
	AND LKP_CLAIM_PARTY_OCCURRENCE_v_claim_occurence_ak_id_v_claim_party_ak_id_claimant_v_party_role_code_claimant.claim_party_ak_id = v_claim_party_ak_id_claimant
	AND LKP_CLAIM_PARTY_OCCURRENCE_v_claim_occurence_ak_id_v_claim_party_ak_id_claimant_v_party_role_code_claimant.claim_party_role_code = v_party_role_code_claimant

	LEFT JOIN LKP_CLAIMANT_DETAIL_COVERAGE LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_claimant_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure
	ON LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_claimant_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.claim_party_occurrence_ak_id = v_claim_party_occurence_ak_id_claimant
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_claimant_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.loc_unit_num = v_loss_location_number
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_claimant_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.sub_loc_unit_num = v_loss_sub_location_number
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_claimant_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.ins_line = v_loss_insurance_line
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_claimant_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.risk_unit_grp = v_loss_risk_unit_group
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_claimant_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.risk_unit_grp_seq_num = V_risk_unit_grp_seq_num
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_claimant_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.risk_unit = v_loss_unit
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_claimant_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.risk_unit_seq_num = v_loss_sequence_risk_unit
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_claimant_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.major_peril_code = v_loss_major_peril
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_claimant_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.major_peril_seq = v_loss_major_peril_seq
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_claimant_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.pms_loss_disability = v_loss_disability
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_claimant_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.reserve_ctgry = v_loss_reserve_category
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_claimant_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.cause_of_loss = v_loss_cause
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_claimant_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.pms_mbr = v_loss_member
	AND LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_claimant_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.pms_type_exposure = v_loss_type_exposure

),
AGG_claim_transaction_stage AS (
	SELECT
	claimant_coverage_detail_ak_id, 
	LOSS_DRAFT_NO, 
	LOSS_DRAFT_AMOUNT, 
	LOSS_TRANSACTION_DATE, 
	pay_to_code, 
	LOSS_ENTRY_OPERATOR, 
	LOSS_DRAFT_MAIL_TO, 
	LOSS_BANK_NUMBER, 
	LOSS_DRAFT_CHECK_IND, 
	LOSS_PAYEE_PHRASE, 
	LOSS_MEMO_PHRASE, 
	LOSS_1099_NUMBER, 
	LOSS_CLAIM_PAYEE_NAME, 
	source_system_id AS SOURCE_SYSTEM_ID, 
	o_claim_party_occurence_ak_id_payee, 
	o_loss_adjustor_no AS loss_adjustor_no, 
	o_party_role_code_payee AS o_party_role_code
	FROM EXP_CLAIM_TRANSACTION_VALIDATE
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_coverage_detail_ak_id, LOSS_DRAFT_NO, LOSS_DRAFT_AMOUNT, LOSS_TRANSACTION_DATE ORDER BY NULL) = 1
),
EXP_get_values AS (
	SELECT
	LOSS_ENTRY_OPERATOR,
	LOSS_DRAFT_NO,
	LOSS_DRAFT_AMOUNT,
	LOSS_TRANSACTION_DATE AS LOSS_TRANSACTION_DATE_payment,
	loss_adjustor_no AS LOSS_ADJUSTOR_NO,
	o_party_role_code AS PARTY_ROLE_CODE,
	pay_to_code,
	LOSS_DRAFT_MAIL_TO AS loss_draft_mail_to,
	SOURCE_SYSTEM_ID,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	claimant_coverage_detail_ak_id,
	o_claim_party_occurence_ak_id_payee
	FROM AGG_claim_transaction_stage
),
LKP_claim_payment AS (
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
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_payment
		WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pms_claimant_cov_det_ak_id,micro_ecd_draft_num,total_pay_amt,pay_issued_date ORDER BY claim_pay_ak_id) = 1
),
FIL_remove_claim_payment AS (
	SELECT
	LKP_claim_payment.claim_pay_ak_id, 
	EXP_get_values.LOSS_ENTRY_OPERATOR, 
	EXP_get_values.LOSS_DRAFT_NO, 
	EXP_get_values.LOSS_DRAFT_AMOUNT, 
	EXP_get_values.LOSS_TRANSACTION_DATE_payment, 
	EXP_get_values.LOSS_ADJUSTOR_NO, 
	EXP_get_values.pay_to_code, 
	EXP_get_values.loss_draft_mail_to, 
	EXP_get_values.SOURCE_SYSTEM_ID, 
	EXP_get_values.crrnt_snpsht_flag, 
	EXP_get_values.audit_id, 
	EXP_get_values.o_claim_party_occurence_ak_id_payee
	FROM EXP_get_values
	LEFT JOIN LKP_claim_payment
	ON LKP_claim_payment.pms_claimant_cov_det_ak_id = EXP_get_values.claimant_coverage_detail_ak_id AND LKP_claim_payment.micro_ecd_draft_num = EXP_get_values.LOSS_DRAFT_NO AND LKP_claim_payment.total_pay_amt = EXP_get_values.LOSS_DRAFT_AMOUNT AND LKP_claim_payment.pay_issued_date = EXP_get_values.LOSS_TRANSACTION_DATE_payment
	WHERE IIF(ISNULL(claim_pay_ak_id), FALSE, TRUE)
),
EXP_Claim_party_occ_payment_get_values AS (
	SELECT
	o_claim_party_occurence_ak_id_payee,
	claim_pay_ak_id,
	LOSS_ADJUSTOR_NO,
	'N/A' AS Payee_type,
	'N/A' AS Payee_type_seq_nbr,
	SOURCE_SYSTEM_ID,
	crrnt_snpsht_flag,
	audit_id,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS eff_from_dt,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_dt,
	sysdate AS created_date,
	sysdate AS modified_date
	FROM FIL_remove_claim_payment
),
LKP_claim_party_occ_payment AS (
	SELECT
	claim_party_occurrence_pay_id,
	claim_pay_ak_id,
	claim_party_occurrence_ak_id,
	payee_code,
	IN_claim_pay_ak_id,
	o_claim_party_occurence_ak_id_payee,
	IN_LOSS_ADJUSTOR_NO
	FROM (
		SELECT 
			claim_party_occurrence_pay_id,
			claim_pay_ak_id,
			claim_party_occurrence_ak_id,
			payee_code,
			IN_claim_pay_ak_id,
			o_claim_party_occurence_ak_id_payee,
			IN_LOSS_ADJUSTOR_NO
		FROM claim_party_occurrence_payment
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_pay_ak_id,claim_party_occurrence_ak_id,payee_code ORDER BY claim_party_occurrence_pay_id) = 1
),
RTR_Claim_party_occ_payment AS (
	SELECT
	LKP_claim_party_occ_payment.claim_party_occurrence_pay_id,
	EXP_Claim_party_occ_payment_get_values.claim_pay_ak_id,
	EXP_Claim_party_occ_payment_get_values.o_claim_party_occurence_ak_id_payee,
	EXP_Claim_party_occ_payment_get_values.Payee_type,
	EXP_Claim_party_occ_payment_get_values.LOSS_ADJUSTOR_NO,
	EXP_Claim_party_occ_payment_get_values.Payee_type_seq_nbr,
	EXP_Claim_party_occ_payment_get_values.SOURCE_SYSTEM_ID,
	EXP_Claim_party_occ_payment_get_values.crrnt_snpsht_flag,
	EXP_Claim_party_occ_payment_get_values.audit_id,
	EXP_Claim_party_occ_payment_get_values.eff_from_dt,
	EXP_Claim_party_occ_payment_get_values.eff_to_dt,
	EXP_Claim_party_occ_payment_get_values.created_date,
	EXP_Claim_party_occ_payment_get_values.modified_date
	FROM EXP_Claim_party_occ_payment_get_values
	LEFT JOIN LKP_claim_party_occ_payment
	ON LKP_claim_party_occ_payment.claim_pay_ak_id = EXP_Claim_party_occ_payment_get_values.claim_pay_ak_id AND LKP_claim_party_occ_payment.claim_party_occurrence_ak_id = EXP_Claim_party_occ_payment_get_values.o_claim_party_occurence_ak_id_payee AND LKP_claim_party_occ_payment.payee_code = EXP_Claim_party_occ_payment_get_values.LOSS_ADJUSTOR_NO
),
RTR_Claim_party_occ_payment_INSERT AS (SELECT * FROM RTR_Claim_party_occ_payment WHERE ISNULL(claim_party_occurrence_pay_id)),
RTR_Claim_party_occ_payment_UPDATE AS (SELECT * FROM RTR_Claim_party_occ_payment WHERE NOT ISNULL(claim_party_occurrence_pay_id)),
UPD_Claim_party_occ_update AS (
	SELECT
	claim_party_occurrence_pay_id AS claim_party_occurrence_pay_id3, 
	claim_pay_ak_id AS claim_pay_ak_id3, 
	o_claim_party_occurence_ak_id_payee, 
	Payee_type AS Payee_type3, 
	LOSS_ADJUSTOR_NO AS LOSS_ADJUSTOR_NO3, 
	Payee_type_seq_nbr AS Payee_type_seq_nbr3, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID3, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag3, 
	audit_id AS audit_id3, 
	eff_from_dt AS eff_from_dt3, 
	eff_to_dt AS eff_to_dt3, 
	created_date AS created_date3, 
	modified_date AS modified_date3
	FROM RTR_Claim_party_occ_payment_UPDATE
),
claim_party_occurrence_payment_update AS (
	MERGE INTO claim_party_occurrence_payment AS T
	USING UPD_Claim_party_occ_update AS S
	ON T.claim_party_occurrence_pay_id = S.claim_party_occurrence_pay_id3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.payee_code = S.LOSS_ADJUSTOR_NO3, T.modified_date = S.modified_date3
),
SEQ_Claim_Party_Occurrence_Payment_AK AS (
	CREATE SEQUENCE SEQ_Claim_Party_Occurrence_Payment_AK
	START = 0
	INCREMENT = 1;
),
UPD_Claim_party_occ_insert AS (
	SELECT
	claim_pay_ak_id AS claim_pay_ak_id1, 
	o_claim_party_occurence_ak_id_payee, 
	Payee_type AS Payee_type1, 
	LOSS_ADJUSTOR_NO AS LOSS_ADJUSTOR_NO1, 
	Payee_type_seq_nbr AS Payee_type_seq_nbr1, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID1, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag1, 
	audit_id AS audit_id1, 
	eff_from_dt AS eff_from_dt1, 
	eff_to_dt AS eff_to_dt1, 
	created_date AS created_date1, 
	modified_date AS modified_date1
	FROM RTR_Claim_party_occ_payment_INSERT
),
claim_party_occurrence_payment_insert AS (
	INSERT INTO claim_party_occurrence_payment
	(claim_party_occurrence_pay_ak_id, claim_pay_ak_id, claim_party_occurrence_ak_id, payee_type, payee_code, claim_payee_seq_num, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	SEQ_Claim_Party_Occurrence_Payment_AK.NEXTVAL AS CLAIM_PARTY_OCCURRENCE_PAY_AK_ID, 
	claim_pay_ak_id1 AS CLAIM_PAY_AK_ID, 
	o_claim_party_occurence_ak_id_payee AS CLAIM_PARTY_OCCURRENCE_AK_ID, 
	Payee_type1 AS PAYEE_TYPE, 
	LOSS_ADJUSTOR_NO1 AS PAYEE_CODE, 
	Payee_type_seq_nbr1 AS CLAIM_PAYEE_SEQ_NUM, 
	crrnt_snpsht_flag1 AS CRRNT_SNPSHT_FLAG, 
	audit_id1 AS AUDIT_ID, 
	eff_from_dt1 AS EFF_FROM_DATE, 
	eff_to_dt1 AS EFF_TO_DATE, 
	SOURCE_SYSTEM_ID1 AS SOURCE_SYS_ID, 
	created_date1 AS CREATED_DATE, 
	modified_date1 AS MODIFIED_DATE
	FROM UPD_Claim_party_occ_insert
),
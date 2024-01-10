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
LKP_gtam_tc09_stage AS (
	SELECT
	memo_phrase_verbiage,
	memo_phrase_on_pucl
	FROM (
		SELECT 
			memo_phrase_verbiage,
			memo_phrase_on_pucl
		FROM gtam_tc09_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY memo_phrase_on_pucl ORDER BY memo_phrase_verbiage) = 1
),
LKP_gtam_tc08_stage AS (
	SELECT
	payee_phrase_verbiage,
	code_entered_on_pucl
	FROM (
		SELECT 
			payee_phrase_verbiage,
			code_entered_on_pucl
		FROM gtam_tc08_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY code_entered_on_pucl ORDER BY payee_phrase_verbiage) = 1
),
LKP_Adjustor_Name AS (
	SELECT
	adnm_name,
	adnm_adjustor_nbr
	FROM (
		SELECT 
			adnm_name,
			adnm_adjustor_nbr
		FROM pms_adjuster_master_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY adnm_adjustor_nbr ORDER BY adnm_name) = 1
),
LKP_sup_payment_system AS (
	SELECT
	sup_payment_system_id,
	payment_system
	FROM (
		SELECT 
			sup_payment_system_id,
			payment_system
		FROM sup_payment_system
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY payment_system ORDER BY sup_payment_system_id) = 1
),
LKP_sup_payment_method AS (
	SELECT
	sup_payment_method_id,
	payment_method
	FROM (
		SELECT 
			sup_payment_method_id,
			payment_method
		FROM sup_payment_method
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY payment_method ORDER BY sup_payment_method_id) = 1
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
	ORDER BY pif_4578_stage.loss_transaction_date
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
	'CMT' AS v_party_role_code,
	-- *INF*: v_loss_occurence_key||TO_CHAR(loss_claimant)||v_party_role_code
	v_loss_occurence_key || TO_CHAR(loss_claimant) || v_party_role_code AS v_loss_party_key,
	v_loss_party_key AS loss_party_key_out,
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
	-- *INF*: :LKP.LKP_CLAIM_PARTY(v_loss_party_key)
	LKP_CLAIM_PARTY_v_loss_party_key.claim_party_ak_id AS v_claim_party_ak_id,
	-- *INF*: :LKP.LKP_CLAIM_PARTY_OCCURRENCE(v_claim_occurence_ak_id,v_claim_party_ak_id,v_party_role_code)
	LKP_CLAIM_PARTY_OCCURRENCE_v_claim_occurence_ak_id_v_claim_party_ak_id_v_party_role_code.claim_party_occurrence_ak_id AS v_claim_party_occurence_ak_id,
	-- *INF*: :LKP.LKP_CLAIMANT_DETAIL_COVERAGE(v_claim_party_occurence_ak_id, v_loss_location_number, v_loss_sub_location_number,  v_loss_insurance_line, v_loss_risk_unit_group, V_risk_unit_grp_seq_num ,v_loss_unit, v_loss_sequence_risk_unit, v_loss_major_peril, v_loss_major_peril_seq, v_loss_disability, v_loss_reserve_category, v_loss_cause, v_loss_member, v_loss_type_exposure)
	LKP_CLAIMANT_DETAIL_COVERAGE_v_claim_party_occurence_ak_id_v_loss_location_number_v_loss_sub_location_number_v_loss_insurance_line_v_loss_risk_unit_group_V_risk_unit_grp_seq_num_v_loss_unit_v_loss_sequence_risk_unit_v_loss_major_peril_v_loss_major_peril_seq_v_loss_disability_v_loss_reserve_category_v_loss_cause_v_loss_member_v_loss_type_exposure.claimant_cov_det_ak_id AS v_claimant_coverage_detail_ak_id,
	v_claimant_coverage_detail_ak_id AS claimant_coverage_detail_ak_id,
	loss_entry_operator AS IN_LOSS_ENTRY_OPERATOR,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_ENTRY_OPERATOR))) OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_ENTRY_OPERATOR))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_ENTRY_OPERATOR))) = 0, 'N/A', IN_LOSS_ENTRY_OPERATOR)
	-- 
	-- --IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_ENTRY_OPERATOR))),'N/A',IIF(IS_SPACES(IN_LOSS_ENTRY_OPERATOR),'N/A',LTRIM(RTRIM(IN_LOSS_ENTRY_OPERATOR))))
	IFF(LTRIM(RTRIM(IN_LOSS_ENTRY_OPERATOR)) IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_ENTRY_OPERATOR))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_ENTRY_OPERATOR))) = 0, 'N/A', IN_LOSS_ENTRY_OPERATOR) AS LOSS_ENTRY_OPERATOR,
	loss_adjustor_no AS in_loss_adjustor_no,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(in_loss_adjustor_no))) OR IS_SPACES(LTRIM(RTRIM(in_loss_adjustor_no))) OR LENGTH(LTRIM(RTRIM(in_loss_adjustor_no))) = 0, 'N/A',LTRIM(RTRIM(in_loss_adjustor_no)))
	IFF(LTRIM(RTRIM(in_loss_adjustor_no)) IS NULL OR IS_SPACES(LTRIM(RTRIM(in_loss_adjustor_no))) OR LENGTH(LTRIM(RTRIM(in_loss_adjustor_no))) = 0, 'N/A', LTRIM(RTRIM(in_loss_adjustor_no))) AS loss_adjustor_no,
	loss_bank_number AS IN_LOSS_BANK_NUMBER,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_BANK_NUMBER))) OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_BANK_NUMBER))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_BANK_NUMBER))) = 0, 'N/A',LTRIM(RTRIM(IN_LOSS_BANK_NUMBER)))
	IFF(LTRIM(RTRIM(IN_LOSS_BANK_NUMBER)) IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_BANK_NUMBER))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_BANK_NUMBER))) = 0, 'N/A', LTRIM(RTRIM(IN_LOSS_BANK_NUMBER))) AS LOSS_BANK_NUMBER,
	loss_draft_amount AS IN_LOSS_DRAFT_AMOUNT,
	-- *INF*: IIF(ISNULL(IN_LOSS_DRAFT_AMOUNT),0,IN_LOSS_DRAFT_AMOUNT)
	IFF(IN_LOSS_DRAFT_AMOUNT IS NULL, 0, IN_LOSS_DRAFT_AMOUNT) AS LOSS_DRAFT_AMOUNT,
	loss_draft_no AS IN_LOSS_DRAFT_NO,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_LOSS_DRAFT_NO))) OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_DRAFT_NO))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_DRAFT_NO))) = 0, 'N/A', IN_LOSS_DRAFT_NO)
	IFF(LTRIM(RTRIM(IN_LOSS_DRAFT_NO)) IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_LOSS_DRAFT_NO))) OR LENGTH(LTRIM(RTRIM(IN_LOSS_DRAFT_NO))) = 0, 'N/A', IN_LOSS_DRAFT_NO) AS OUT_LOSS_DRAFT_NO,
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
	source_system_id AS SOURCE_SYSTEM_ID,
	-- *INF*: DECODE(TRUE,
	-- 	IN_LOSS_DRAFT_CHECK_IND = 'D',
	-- 		'Manual Draft',
	-- 	IN_LOSS_DRAFT_CHECK_IND = 'C',
	-- 		'PMS',
	-- 	ISNULL(IN_LOSS_DRAFT_AMOUNT) OR IN_LOSS_DRAFT_AMOUNT = 0.0,
	-- 		'PMS',
	-- 	'Manual Draft')
	DECODE(TRUE,
		IN_LOSS_DRAFT_CHECK_IND = 'D', 'Manual Draft',
		IN_LOSS_DRAFT_CHECK_IND = 'C', 'PMS',
		IN_LOSS_DRAFT_AMOUNT IS NULL OR IN_LOSS_DRAFT_AMOUNT = 0.0, 'PMS',
		'Manual Draft') AS v_payment_system,
	-- *INF*: :LKP.LKP_SUP_PAYMENT_SYSTEM(v_payment_system)
	LKP_SUP_PAYMENT_SYSTEM_v_payment_system.sup_payment_system_id AS v_sup_payment_system_id,
	-- *INF*: IIF(ISNULL(v_sup_payment_system_id), -1, v_sup_payment_system_id)
	IFF(v_sup_payment_system_id IS NULL, - 1, v_sup_payment_system_id) AS o_sup_payment_system_id,
	-- *INF*: DECODE(TRUE,
	-- 	LENGTH(RTRIM(:UDF.DEFAULT_VALUE_FOR_STRINGS(IN_LOSS_DRAFT_NO))) < 3, 
	-- 		'Check',
	-- 	UPPER(SUBSTR(:UDF.DEFAULT_VALUE_FOR_STRINGS(IN_LOSS_DRAFT_NO),1,3)) = 'EFT',
	-- 		'EFT',
	-- 	'Check')
	DECODE(TRUE,
		LENGTH(RTRIM(:UDF.DEFAULT_VALUE_FOR_STRINGS(IN_LOSS_DRAFT_NO))) < 3, 'Check',
		UPPER(SUBSTR(:UDF.DEFAULT_VALUE_FOR_STRINGS(IN_LOSS_DRAFT_NO), 1, 3)) = 'EFT', 'EFT',
		'Check') AS v_payment_method,
	-- *INF*: :LKP.LKP_SUP_PAYMENT_METHOD(v_payment_method)
	LKP_SUP_PAYMENT_METHOD_v_payment_method.sup_payment_method_id AS v_sup_payment_method_id,
	-- *INF*: IIF(ISNULL(v_sup_payment_method_id), -1, v_sup_payment_method_id)
	IFF(v_sup_payment_method_id IS NULL, - 1, v_sup_payment_method_id) AS o_sup_payment_method_id
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

	LEFT JOIN LKP_SUP_PAYMENT_SYSTEM LKP_SUP_PAYMENT_SYSTEM_v_payment_system
	ON LKP_SUP_PAYMENT_SYSTEM_v_payment_system.payment_system = v_payment_system

	LEFT JOIN LKP_SUP_PAYMENT_METHOD LKP_SUP_PAYMENT_METHOD_v_payment_method
	ON LKP_SUP_PAYMENT_METHOD_v_payment_method.payment_method = v_payment_method

),
AGG_claim_transaction_stage AS (
	SELECT
	claimant_coverage_detail_ak_id,
	OUT_LOSS_DRAFT_NO AS LOSS_DRAFT_NO,
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
	SOURCE_SYSTEM_ID,
	loss_party_key_out,
	loss_adjustor_no,
	o_sup_payment_system_id AS sup_payment_system_id,
	o_sup_payment_method_id AS sup_payment_method_id
	FROM EXP_CLAIM_TRANSACTION_VALIDATE
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_coverage_detail_ak_id, LOSS_DRAFT_NO, LOSS_DRAFT_AMOUNT, LOSS_TRANSACTION_DATE ORDER BY NULL) = 1
),
EXP_CLAIMS_TRANSACTION_DEFAULT AS (
	SELECT
	claimant_coverage_detail_ak_id,
	LOSS_ENTRY_OPERATOR,
	LOSS_DRAFT_NO,
	LOSS_TRANSACTION_DATE,
	LOSS_BANK_NUMBER,
	LOSS_DRAFT_AMOUNT,
	pay_to_code,
	LOSS_CLAIM_PAYEE_NAME,
	LOSS_DRAFT_CHECK_IND,
	LOSS_1099_NUMBER,
	LOSS_PAYEE_PHRASE,
	-- *INF*: ----:LKP.LKP_GTAM_TC08_STAGE(LOSS_PAYEE_PHRASE)
	'' AS v_PAYEE_PHRASE_COMMENT,
	-- *INF*: 'N/A'
	-- 
	-- 
	-- ---IIF(ISNULL(LTRIM(RTRIM(v_PAYEE_PHRASE_COMMENT))) OR IS_SPACES(LTRIM(RTRIM(v_PAYEE_PHRASE_COMMENT))) OR LENGTH(LTRIM(RTRIM(v_PAYEE_PHRASE_COMMENT))) = 0, 'N/A',LTRIM(RTRIM(v_PAYEE_PHRASE_COMMENT)))
	-- 
	-- --IIF(ISNULL(v_PAYEE_PHRASE_COMMENT), 'N/A', LTRIM(RTRIM(v_PAYEE_PHRASE_COMMENT)))
	'N/A' AS PAYEE_PHRASE_COMMENT,
	LOSS_MEMO_PHRASE,
	-- *INF*: :LKP.LKP_GTAM_TC09_STAGE(LOSS_MEMO_PHRASE)
	LKP_GTAM_TC09_STAGE_LOSS_MEMO_PHRASE.memo_phrase_verbiage AS v_MEMO_PHRASE_COMMENT,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(v_MEMO_PHRASE_COMMENT))) OR IS_SPACES(LTRIM(RTRIM(v_MEMO_PHRASE_COMMENT))) OR LENGTH(LTRIM(RTRIM(v_MEMO_PHRASE_COMMENT))) = 0, 'N/A',LTRIM(RTRIM(v_MEMO_PHRASE_COMMENT)))
	-- 
	-- --IIF(ISNULL(v_MEMO_PHRASE_COMMENT), 'N/A', LTRIM(RTRIM(v_MEMO_PHRASE_COMMENT)))
	IFF(LTRIM(RTRIM(v_MEMO_PHRASE_COMMENT)) IS NULL OR IS_SPACES(LTRIM(RTRIM(v_MEMO_PHRASE_COMMENT))) OR LENGTH(LTRIM(RTRIM(v_MEMO_PHRASE_COMMENT))) = 0, 'N/A', LTRIM(RTRIM(v_MEMO_PHRASE_COMMENT))) AS MEMO_PHRASE_COMMENT,
	LOSS_DRAFT_MAIL_TO,
	SOURCE_SYSTEM_ID,
	'N/A' AS default,
	-- *INF*: TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') AS default_date,
	'1' AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	loss_party_key_out,
	loss_adjustor_no,
	-- *INF*: ltrim(rtrim(:LKP.LKP_ADJUSTOR_NAME(loss_adjustor_no)))
	ltrim(rtrim(LKP_ADJUSTOR_NAME_loss_adjustor_no.adnm_name)) AS prim_payee_name,
	-- *INF*: iif(isnull(prim_payee_name) or substr(loss_adjustor_no,1,1)  = 'X'
	-- 	,ltrim(rtrim(LOSS_CLAIM_PAYEE_NAME))
	-- 	,prim_payee_name)
	IFF(prim_payee_name IS NULL OR substr(loss_adjustor_no, 1, 1) = 'X', ltrim(rtrim(LOSS_CLAIM_PAYEE_NAME)), prim_payee_name) AS o_prim_payee_name,
	sup_payment_system_id,
	sup_payment_method_id,
	-1 AS default_int,
	0 AS default_int_0
	FROM AGG_claim_transaction_stage
	LEFT JOIN LKP_GTAM_TC09_STAGE LKP_GTAM_TC09_STAGE_LOSS_MEMO_PHRASE
	ON LKP_GTAM_TC09_STAGE_LOSS_MEMO_PHRASE.memo_phrase_on_pucl = LOSS_MEMO_PHRASE

	LEFT JOIN LKP_ADJUSTOR_NAME LKP_ADJUSTOR_NAME_loss_adjustor_no
	ON LKP_ADJUSTOR_NAME_loss_adjustor_no.adnm_adjustor_nbr = loss_adjustor_no

),
LKP_claim_payment AS (
	SELECT
	claim_pay_id,
	pms_claimant_cov_det_ak_id,
	micro_ecd_draft_num,
	total_pay_amt,
	pay_issued_date
	FROM (
		SELECT claim_payment.claim_pay_id as claim_pay_id, 
		claim_payment.pms_claimant_cov_det_ak_id as pms_claimant_cov_det_ak_id, 
		LTRIM(RTRIM(claim_payment.micro_ecd_draft_num)) as micro_ecd_draft_num, 
		claim_payment.total_pay_amt as total_pay_amt, 
		claim_payment.pay_issued_date as pay_issued_date 
		 FROM claim_payment
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pms_claimant_cov_det_ak_id,micro_ecd_draft_num,total_pay_amt,pay_issued_date ORDER BY claim_pay_id) = 1
),
RTR_CLAIM_TRANSACTION AS (
	SELECT
	LKP_claim_payment.claim_pay_id,
	EXP_CLAIMS_TRANSACTION_DEFAULT.default,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_DRAFT_NO,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_BANK_NUMBER,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_DRAFT_AMOUNT,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_TRANSACTION_DATE,
	EXP_CLAIMS_TRANSACTION_DEFAULT.default_date,
	EXP_CLAIMS_TRANSACTION_DEFAULT.pay_to_code,
	EXP_CLAIMS_TRANSACTION_DEFAULT.o_prim_payee_name AS LOSS_CLAIM_PAYEE_NAME,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_DRAFT_CHECK_IND,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_ENTRY_OPERATOR,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_1099_NUMBER,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_PAYEE_PHRASE,
	EXP_CLAIMS_TRANSACTION_DEFAULT.PAYEE_PHRASE_COMMENT,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_MEMO_PHRASE,
	EXP_CLAIMS_TRANSACTION_DEFAULT.MEMO_PHRASE_COMMENT,
	EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_DRAFT_MAIL_TO,
	EXP_CLAIMS_TRANSACTION_DEFAULT.SOURCE_SYSTEM_ID,
	EXP_CLAIMS_TRANSACTION_DEFAULT.crrnt_snpsht_flag,
	EXP_CLAIMS_TRANSACTION_DEFAULT.audit_id,
	EXP_CLAIMS_TRANSACTION_DEFAULT.eff_from_date,
	EXP_CLAIMS_TRANSACTION_DEFAULT.eff_to_date,
	EXP_CLAIMS_TRANSACTION_DEFAULT.created_date,
	EXP_CLAIMS_TRANSACTION_DEFAULT.modified_date,
	EXP_CLAIMS_TRANSACTION_DEFAULT.claimant_coverage_detail_ak_id,
	EXP_CLAIMS_TRANSACTION_DEFAULT.loss_party_key_out,
	EXP_CLAIMS_TRANSACTION_DEFAULT.sup_payment_system_id,
	EXP_CLAIMS_TRANSACTION_DEFAULT.sup_payment_method_id,
	EXP_CLAIMS_TRANSACTION_DEFAULT.default_int,
	EXP_CLAIMS_TRANSACTION_DEFAULT.default_int_0
	FROM EXP_CLAIMS_TRANSACTION_DEFAULT
	LEFT JOIN LKP_claim_payment
	ON LKP_claim_payment.pms_claimant_cov_det_ak_id = EXP_CLAIMS_TRANSACTION_DEFAULT.claimant_coverage_detail_ak_id AND LKP_claim_payment.micro_ecd_draft_num = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_DRAFT_NO AND LKP_claim_payment.total_pay_amt = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_DRAFT_AMOUNT AND LKP_claim_payment.pay_issued_date = EXP_CLAIMS_TRANSACTION_DEFAULT.LOSS_TRANSACTION_DATE
),
RTR_CLAIM_TRANSACTION_INSERT AS (SELECT * FROM RTR_CLAIM_TRANSACTION WHERE ISNULL(claim_pay_id)),
RTR_CLAIM_TRANSACTION_UPDATE AS (SELECT * FROM RTR_CLAIM_TRANSACTION WHERE NOT ISNULL(claim_pay_id)),
SEQ_Claim_Payments_AK AS (
	CREATE SEQUENCE SEQ_Claim_Payments_AK
	START = 0
	INCREMENT = 1;
),
UPD_CLAIM_TRANSACTION_INSERT AS (
	SELECT
	SEQ_Claim_Payments_AK.NEXTVAL, 
	default AS default1, 
	LOSS_DRAFT_NO AS LOSS_DRAFT_NO1, 
	LOSS_BANK_NUMBER AS LOSS_BANK_NUMBER1, 
	LOSS_DRAFT_AMOUNT AS LOSS_DRAFT_AMOUNT1, 
	LOSS_TRANSACTION_DATE AS LOSS_TRANSACTION_DATE1, 
	default_date AS default_date1, 
	pay_to_code AS pay_to_code1, 
	LOSS_CLAIM_PAYEE_NAME AS LOSS_CLAIM_PAYEE_NAME1, 
	LOSS_DRAFT_CHECK_IND AS LOSS_DRAFT_CHECK_IND1, 
	LOSS_ENTRY_OPERATOR AS LOSS_ENTRY_OPERATOR1, 
	LOSS_1099_NUMBER AS LOSS_1099_NUMBER3, 
	LOSS_PAYEE_PHRASE AS LOSS_PAYEE_PHRASE3, 
	PAYEE_PHRASE_COMMENT AS PAYEE_PHRASE_COMMENT1, 
	LOSS_MEMO_PHRASE AS LOSS_MEMO_PHRASE3, 
	MEMO_PHRASE_COMMENT AS MEMO_PHRASE_COMMENT1, 
	LOSS_DRAFT_MAIL_TO AS LOSS_DRAFT_MAIL_TO1, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID1, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag1, 
	audit_id AS audit_id1, 
	eff_from_date AS eff_from_date1, 
	eff_to_date AS eff_to_date1, 
	created_date AS created_date1, 
	modified_date AS modified_date1, 
	claimant_coverage_detail_ak_id AS claimant_coverage_detail_ak_id2, 
	loss_party_key_out AS loss_party_key_out1, 
	sup_payment_system_id AS sup_payment_system_id1, 
	sup_payment_method_id AS sup_payment_method_id1, 
	default_int AS default_int1, 
	default_int_0 AS default_int_01
	FROM RTR_CLAIM_TRANSACTION_INSERT
),
claim_payment_insert AS (
	INSERT INTO claim_payment
	(claim_pay_ak_id, pms_claimant_cov_det_ak_id, claim_pay_num, micro_ecd_draft_num, bank_acct_num, pay_delete_ind, total_pay_amt, pay_issued_date, pay_cashed_date, pay_to_code, payee_note, pay_ind, pay_type_code, pay_entry_oper_id, pay_entry_oper_role_code, pay_disbursement_date, pay_disbursement_status, pay_disbursement_loc_code, reported_to_irs_ind, pay_voided_date, pay_reposted_date, new_claim_num, new_draft_num, payee_phrase_code, pay_to_the_order_of_name, memo_phrase_code, memo_phrase_comment, mail_to_code, mail_to_name, mail_to_addr, mail_to_city, mail_to_state, mail_to_zip, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, prim_payee_name, add_payee_name1, add_payee_name2, add_payee_name3, add_payee_name4, add_payee_name5, add_payee_name6, sup_payment_system_id, sup_payment_method_id, approval_status, approval_by_user_id, approval_date, denial_reason, special_processing, payee_category, sup_claim_payment_workflow_id, attached_document_count)
	SELECT 
	NEXTVAL AS CLAIM_PAY_AK_ID, 
	claimant_coverage_detail_ak_id2 AS PMS_CLAIMANT_COV_DET_AK_ID, 
	default1 AS CLAIM_PAY_NUM, 
	LOSS_DRAFT_NO1 AS MICRO_ECD_DRAFT_NUM, 
	LOSS_BANK_NUMBER1 AS BANK_ACCT_NUM, 
	default1 AS PAY_DELETE_IND, 
	LOSS_DRAFT_AMOUNT1 AS TOTAL_PAY_AMT, 
	LOSS_TRANSACTION_DATE1 AS PAY_ISSUED_DATE, 
	default_date1 AS PAY_CASHED_DATE, 
	pay_to_code1 AS PAY_TO_CODE, 
	LOSS_CLAIM_PAYEE_NAME1 AS PAYEE_NOTE, 
	LOSS_DRAFT_CHECK_IND1 AS PAY_IND, 
	default1 AS PAY_TYPE_CODE, 
	LOSS_ENTRY_OPERATOR1 AS PAY_ENTRY_OPER_ID, 
	default1 AS PAY_ENTRY_OPER_ROLE_CODE, 
	default_date1 AS PAY_DISBURSEMENT_DATE, 
	default1 AS PAY_DISBURSEMENT_STATUS, 
	default1 AS PAY_DISBURSEMENT_LOC_CODE, 
	LOSS_1099_NUMBER3 AS REPORTED_TO_IRS_IND, 
	default_date1 AS PAY_VOIDED_DATE, 
	default_date1 AS PAY_REPOSTED_DATE, 
	default1 AS NEW_CLAIM_NUM, 
	default1 AS NEW_DRAFT_NUM, 
	LOSS_PAYEE_PHRASE3 AS PAYEE_PHRASE_CODE, 
	PAYEE_PHRASE_COMMENT1 AS PAY_TO_THE_ORDER_OF_NAME, 
	LOSS_MEMO_PHRASE3 AS MEMO_PHRASE_CODE, 
	MEMO_PHRASE_COMMENT1 AS MEMO_PHRASE_COMMENT, 
	LOSS_DRAFT_MAIL_TO1 AS MAIL_TO_CODE, 
	default1 AS MAIL_TO_NAME, 
	default1 AS MAIL_TO_ADDR, 
	default1 AS MAIL_TO_CITY, 
	default1 AS MAIL_TO_STATE, 
	default1 AS MAIL_TO_ZIP, 
	crrnt_snpsht_flag1 AS CRRNT_SNPSHT_FLAG, 
	audit_id1 AS AUDIT_ID, 
	eff_from_date1 AS EFF_FROM_DATE, 
	eff_to_date1 AS EFF_TO_DATE, 
	SOURCE_SYSTEM_ID1 AS SOURCE_SYS_ID, 
	created_date1 AS CREATED_DATE, 
	modified_date1 AS MODIFIED_DATE, 
	LOSS_CLAIM_PAYEE_NAME1 AS PRIM_PAYEE_NAME, 
	default1 AS ADD_PAYEE_NAME1, 
	default1 AS ADD_PAYEE_NAME2, 
	default1 AS ADD_PAYEE_NAME3, 
	default1 AS ADD_PAYEE_NAME4, 
	default1 AS ADD_PAYEE_NAME5, 
	default1 AS ADD_PAYEE_NAME6, 
	sup_payment_system_id1 AS SUP_PAYMENT_SYSTEM_ID, 
	sup_payment_method_id1 AS SUP_PAYMENT_METHOD_ID, 
	default1 AS APPROVAL_STATUS, 
	default1 AS APPROVAL_BY_USER_ID, 
	default_date1 AS APPROVAL_DATE, 
	default1 AS DENIAL_REASON, 
	default1 AS SPECIAL_PROCESSING, 
	default1 AS PAYEE_CATEGORY, 
	default_int1 AS SUP_CLAIM_PAYMENT_WORKFLOW_ID, 
	default_int_01 AS ATTACHED_DOCUMENT_COUNT
	FROM UPD_CLAIM_TRANSACTION_INSERT
),
UPD_CLAIM_TRANSACTION_UPDATE AS (
	SELECT
	claim_pay_id AS claim_pay_id3, 
	default AS default3, 
	LOSS_DRAFT_NO AS LOSS_DRAFT_NO3, 
	LOSS_BANK_NUMBER AS LOSS_BANK_NUMBER3, 
	LOSS_DRAFT_AMOUNT AS LOSS_DRAFT_AMOUNT3, 
	LOSS_TRANSACTION_DATE AS LOSS_TRANSACTION_DATE3, 
	default_date AS default_date3, 
	pay_to_code AS pay_to_code3, 
	LOSS_CLAIM_PAYEE_NAME AS LOSS_CLAIM_PAYEE_NAME3, 
	LOSS_DRAFT_CHECK_IND AS LOSS_DRAFT_CHECK_IND3, 
	LOSS_ENTRY_OPERATOR AS LOSS_ENTRY_OPERATOR3, 
	LOSS_1099_NUMBER AS LOSS_1099_NUMBER3, 
	LOSS_PAYEE_PHRASE AS LOSS_PAYEE_PHRASE3, 
	PAYEE_PHRASE_COMMENT AS PAYEE_PHRASE_COMMENT3, 
	LOSS_MEMO_PHRASE AS LOSS_MEMO_PHRASE3, 
	MEMO_PHRASE_COMMENT AS MEMO_PHRASE_COMMENT3, 
	LOSS_DRAFT_MAIL_TO AS LOSS_DRAFT_MAIL_TO3, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID3, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag3, 
	audit_id AS audit_id3, 
	eff_from_date AS eff_from_date3, 
	eff_to_date AS eff_to_date3, 
	created_date AS created_date3, 
	modified_date AS modified_date3, 
	sup_payment_system_id AS sup_payment_system_id3, 
	sup_payment_method_id AS sup_payment_method_id3
	FROM RTR_CLAIM_TRANSACTION_UPDATE
),
claim_payment_update AS (
	MERGE INTO claim_payment AS T
	USING UPD_CLAIM_TRANSACTION_UPDATE AS S
	ON T.claim_pay_id = S.claim_pay_id3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.bank_acct_num = S.LOSS_BANK_NUMBER3, T.payee_note = S.LOSS_CLAIM_PAYEE_NAME3, T.pay_ind = S.LOSS_DRAFT_CHECK_IND3, T.reported_to_irs_ind = S.LOSS_1099_NUMBER3, T.payee_phrase_code = S.LOSS_PAYEE_PHRASE3, T.pay_to_the_order_of_name = S.PAYEE_PHRASE_COMMENT3, T.memo_phrase_code = S.LOSS_MEMO_PHRASE3, T.memo_phrase_comment = S.MEMO_PHRASE_COMMENT3, T.modified_date = S.modified_date3, T.sup_payment_system_id = S.sup_payment_system_id3, T.sup_payment_method_id = S.sup_payment_method_id3
),
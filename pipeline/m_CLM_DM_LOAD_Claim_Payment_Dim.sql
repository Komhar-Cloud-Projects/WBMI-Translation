WITH
LKP_Claim_Party_Occurrence_Payment AS (
	SELECT
	claim_party_occurrence_ak_id,
	claim_pay_ak_id,
	payee_type
	FROM (
		SELECT 
		a.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id
		, a.claim_pay_ak_id as claim_pay_ak_id
		, a.payee_type as payee_type 
		FROM claim_party_occurrence_payment a
		where a.payee_type IN ('P','N/A')
		and a.crrnt_snpsht_flag = 1
		
		-- 'P' FOR EXCEED AND 'N/A' FOR PMS
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_pay_ak_id,payee_type ORDER BY claim_party_occurrence_ak_id) = 1
),
LKP_Claim_Party_Occurrence AS (
	SELECT
	claim_party_role_code,
	claim_party_occurrence_ak_id
	FROM (
		SELECT 
		a.claim_party_role_code as claim_party_role_code
		, a.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id 
		FROM claim_party_occurrence a
		where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id ORDER BY claim_party_role_code) = 1
),
SQ_claim_payment AS (
	SELECT DISTINCT
	CPA.claim_pay_id
	, CPA.claim_pay_ak_id
	, CPA.claim_pay_num
	, CPA.micro_ecd_draft_num
	, CPA.bank_acct_num
	, CPA.pay_delete_ind
	, CPA.total_pay_amt
	, CPA.pay_issued_date
	, CPA.pay_cashed_date
	, CPA.pay_to_code
	, CPA.payee_note
	, CPA.pay_ind
	, CPA.pay_type_code
	, CPA.pay_entry_oper_id
	, CPA.pay_entry_oper_role_code
	, CPA.pay_disbursement_date
	, CPA.pay_disbursement_status
	, CPA.pay_disbursement_loc_code
	, CPA.reported_to_irs_ind
	, CPA.pay_voided_date
	, CPA.pay_reposted_date
	, CPA.new_claim_num
	, CPA.new_draft_num
	, CPA.payee_phrase_code
	, CPA.pay_to_the_order_of_name
	, CPA.memo_phrase_code
	, CPA.memo_phrase_comment
	, CPA.mail_to_code
	, CPA.mail_to_name
	, CPA.mail_to_addr
	, CPA.mail_to_city
	, CPA.mail_to_state
	, CPA.mail_to_zip
	, CPA.source_sys_id
	, CPA.prim_payee_name
	, CPA.add_payee_name1
	, CPA.add_payee_name2
	, CPA.add_payee_name3
	, CPA.add_payee_name4
	, CPA.add_payee_name5
	, CPA.add_payee_name6
	, CP.claim_party_addr
	, CP.claim_party_city
	, CP.claim_party_county
	, CP.claim_party_state
	, CP.claim_party_zip
	, CP.addr_type
	, CP.tax_ssn_id
	, CP.tax_fed_id
	, CPA.eff_from_date
	, CPOP.claim_party_occurrence_pay_id
	, CPOP.PAYEE_TYPE
	, CPOP.PAYEE_CODE
	, CPOP.CLAIM_PAYEE_SEQ_NUM
	, CPA.sup_payment_system_id
	, CPA.sup_payment_method_id 
	, CPA.approval_status
	, CPA.approval_by_user_id
	, CPA.approval_date
	, CPA.denial_reason
	, CPA.special_processing
	, CPA.payee_category
	, CPA.sup_claim_payment_workflow_id
	,CPA.attached_document_count
	FROM claim_payment CPA
	inner join claim_party_occurrence_payment CPOP with (nolock) on CPOP.claim_pay_ak_id = CPA.claim_pay_ak_id 
		and CPOP.crrnt_snpsht_flag = 1 and CPOP.payee_type <> 'A'	
	inner join claim_party_occurrence CPO with (nolock) on CPO.claim_party_occurrence_ak_id = CPOP.claim_party_occurrence_ak_id 
		and CPO.crrnt_snpsht_flag = 1
	inner join claim_party CP with (nolock) on CP.claim_party_ak_id = CPO.claim_party_ak_id 
		and CP.crrnt_snpsht_flag = 1
	inner join claim_occurrence CO with (nolock) on CPO.claim_occurrence_ak_id = CO.claim_occurrence_ak_id 
		and CO.crrnt_snpsht_flag = 1
	where (
		CPA.MODIFIED_DATE >=  '@{pipeline().parameters.SELECTION_START_TS}' or
		CP.MODIFIED_DATE >=  '@{pipeline().parameters.SELECTION_START_TS}' or
		CPOP.MODIFIED_DATE >=  '@{pipeline().parameters.SELECTION_START_TS}'
		)
),
EXP_get_values AS (
	SELECT
	claim_pay_id,
	claim_pay_ak_id,
	claim_pay_num,
	micro_ecd_draft_num,
	bank_acct_num,
	pay_delete_ind,
	total_pay_amt,
	pay_issued_date,
	pay_cashed_date,
	pay_to_code AS IN_pay_to_code,
	-- *INF*: IIF(ISNULL(IN_pay_to_code) OR LENGTH(LTRIM(RTRIM(IN_pay_to_code))) = 0, 'N/A', LTRIM(RTRIM(IN_pay_to_code)) )
	IFF(
	    IN_pay_to_code IS NULL OR LENGTH(LTRIM(RTRIM(IN_pay_to_code))) = 0, 'N/A',
	    LTRIM(RTRIM(IN_pay_to_code))
	) AS pay_to_code,
	payee_note,
	pay_ind,
	pay_type_code AS IN_pay_type_code,
	-- *INF*: IIF(ISNULL(IN_pay_type_code) OR LENGTH(LTRIM(RTRIM(IN_pay_type_code))) = 0, 'N/A', LTRIM(RTRIM(IN_pay_type_code)) )
	IFF(
	    IN_pay_type_code IS NULL OR LENGTH(LTRIM(RTRIM(IN_pay_type_code))) = 0, 'N/A',
	    LTRIM(RTRIM(IN_pay_type_code))
	) AS pay_type_code,
	pay_entry_oper_id,
	pay_entry_oper_role_code AS IN_pay_entry_oper_role_code,
	-- *INF*: IIF(ISNULL(IN_pay_entry_oper_role_code) OR LENGTH(LTRIM(RTRIM(IN_pay_entry_oper_role_code))) = 0, 'N/A', LTRIM(RTRIM(IN_pay_entry_oper_role_code)) )
	IFF(
	    IN_pay_entry_oper_role_code IS NULL OR LENGTH(LTRIM(RTRIM(IN_pay_entry_oper_role_code))) = 0,
	    'N/A',
	    LTRIM(RTRIM(IN_pay_entry_oper_role_code))
	) AS pay_entry_oper_role_code,
	pay_disbursement_date,
	pay_disbursement_status,
	pay_disbursement_loc_code,
	reported_to_irs_ind,
	pay_voided_date,
	pay_reposted_date,
	new_claim_num,
	new_draft_num,
	payee_phrase_code AS IN_payee_phrase_code,
	-- *INF*: IIF(ISNULL(IN_payee_phrase_code) OR LENGTH(LTRIM(RTRIM(IN_payee_phrase_code))) = 0, 'N/A', LTRIM(RTRIM(IN_payee_phrase_code)) )
	IFF(
	    IN_payee_phrase_code IS NULL OR LENGTH(LTRIM(RTRIM(IN_payee_phrase_code))) = 0, 'N/A',
	    LTRIM(RTRIM(IN_payee_phrase_code))
	) AS payee_phrase_code,
	pay_to_the_order_of_name,
	memo_phrase_code AS IN_memo_phrase_code,
	-- *INF*: IIF(ISNULL(IN_memo_phrase_code) OR LENGTH(LTRIM(RTRIM(IN_memo_phrase_code))) = 0, 'N/A', LTRIM(RTRIM(IN_memo_phrase_code)) )
	IFF(
	    IN_memo_phrase_code IS NULL OR LENGTH(LTRIM(RTRIM(IN_memo_phrase_code))) = 0, 'N/A',
	    LTRIM(RTRIM(IN_memo_phrase_code))
	) AS memo_phrase_code,
	memo_phrase_comment,
	mail_to_code AS IN_mail_to_code,
	-- *INF*: IIF(ISNULL(IN_mail_to_code) OR LENGTH(LTRIM(RTRIM(IN_mail_to_code))) = 0, 'N/A', LTRIM(RTRIM(IN_mail_to_code)) )
	IFF(
	    IN_mail_to_code IS NULL OR LENGTH(LTRIM(RTRIM(IN_mail_to_code))) = 0, 'N/A',
	    LTRIM(RTRIM(IN_mail_to_code))
	) AS mail_to_code,
	mail_to_name,
	mail_to_addr,
	mail_to_city,
	mail_to_state,
	mail_to_zip,
	source_sys_id,
	-- *INF*: DECODE(source_sys_id
	--     , 'EXCEED', 'P'
	--     , 'DCT', 'P'
	-- -- otherwise default to
	--     , 'N/A'
	-- )
	-- 
	-- -- was IIF(source_sys_id = 'EXCEED', 'P', 'N/A')
	DECODE(
	    source_sys_id,
	    'EXCEED', 'P',
	    'DCT', 'P',
	    'N/A'
	) AS v_payee_type,
	v_payee_type AS payee_type,
	prim_payee_name,
	add_payee_name1,
	add_payee_name2,
	add_payee_name3,
	add_payee_name4,
	add_payee_name5,
	add_payee_name6,
	-- *INF*: :LKP.LKP_CLAIM_PARTY_OCCURRENCE(:LKP.LKP_CLAIM_PARTY_OCCURRENCE_PAYMENT(claim_pay_ak_id,v_payee_type))
	LKP_CLAIM_PARTY_OCCURRENCE_LKP_CLAIM_PARTY_OCCURRENCE_PAYMENT_claim_pay_ak_id_v_payee_type.claim_party_role_code AS claim_party_role_code,
	-- *INF*: iif(isnull(claim_party_role_code)
	-- ,'N/A'
	-- ,claim_party_role_code)
	IFF(claim_party_role_code IS NULL, 'N/A', claim_party_role_code) AS o_claim_party_role_code,
	claim_party_addr,
	claim_party_city,
	claim_party_county,
	claim_party_state,
	claim_party_zip,
	addr_type,
	tax_ssn_id,
	tax_fed_id,
	-- *INF*: IIF(tax_ssn_id = 'N/A',tax_fed_id,tax_ssn_id)
	IFF(tax_ssn_id = 'N/A', tax_fed_id, tax_ssn_id) AS Tax_id,
	eff_from_date2,
	claim_party_occurrence_pay_id,
	payee_type AS payee_type1,
	payee_code,
	claim_payee_seq_num,
	sup_payment_system_id,
	sup_payment_method_id,
	approval_status,
	approval_by_user_id,
	approval_date,
	denial_reason,
	special_processing,
	payee_category,
	sup_claim_payment_workflow_id,
	attached_document_count
	FROM SQ_claim_payment
	LEFT JOIN LKP_CLAIM_PARTY_OCCURRENCE_PAYMENT LKP_CLAIM_PARTY_OCCURRENCE_PAYMENT_claim_pay_ak_id_v_payee_type
	ON LKP_CLAIM_PARTY_OCCURRENCE_PAYMENT_claim_pay_ak_id_v_payee_type.claim_pay_ak_id = claim_pay_ak_id
	AND LKP_CLAIM_PARTY_OCCURRENCE_PAYMENT_claim_pay_ak_id_v_payee_type.payee_type = v_payee_type

	LEFT JOIN LKP_CLAIM_PARTY_OCCURRENCE LKP_CLAIM_PARTY_OCCURRENCE_LKP_CLAIM_PARTY_OCCURRENCE_PAYMENT_claim_pay_ak_id_v_payee_type
	ON LKP_CLAIM_PARTY_OCCURRENCE_LKP_CLAIM_PARTY_OCCURRENCE_PAYMENT_claim_pay_ak_id_v_payee_type.claim_party_occurrence_ak_id = LKP_CLAIM_PARTY_OCCURRENCE_PAYMENT_claim_pay_ak_id_v_payee_type.claim_party_occurrence_ak_id

),
LKP_Pay_disbursement_status_description AS (
	SELECT
	pay_disbursement_status_descript,
	pay_disbursement_status
	FROM (
		SELECT 
			pay_disbursement_status_descript,
			pay_disbursement_status
		FROM sup_claim_payment_disbursement_status
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pay_disbursement_status ORDER BY pay_disbursement_status_descript) = 1
),
LKP_sup_claim_mail_to_code AS (
	SELECT
	mail_to_code_descript,
	mail_to_code
	FROM (
		SELECT LTRIM(RTRIM(sup_claim_mail_to_code.mail_to_code_descript)) as mail_to_code_descript, LTRIM(RTRIM(sup_claim_mail_to_code.mail_to_code)) as mail_to_code
		 FROM sup_claim_mail_to_code
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY mail_to_code ORDER BY mail_to_code_descript) = 1
),
LKP_sup_claim_memo_phrase AS (
	SELECT
	memo_phrase_descript,
	memo_phrase_code
	FROM (
		SELECT LTRIM(RTRIM(sup_claim_memo_phrase.memo_phrase_descript)) as memo_phrase_descript, LTRIM(RTRIM(sup_claim_memo_phrase.memo_phrase_code)) as memo_phrase_code 
		FROM sup_claim_memo_phrase
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY memo_phrase_code ORDER BY memo_phrase_descript) = 1
),
LKP_sup_claim_party_role_code_desc AS (
	SELECT
	claim_party_role_descript,
	claim_party_role_code
	FROM (
		SELECT 
			claim_party_role_descript,
			claim_party_role_code
		FROM sup_claim_party_role_code
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_role_code ORDER BY claim_party_role_descript) = 1
),
LKP_sup_claim_payee_phrase AS (
	SELECT
	payee_phrase_descript,
	payee_phrase_code
	FROM (
		SELECT LTRIM(RTRIM(sup_claim_payee_phrase.payee_phrase_descript)) as payee_phrase_descript, LTRIM(RTRIM(sup_claim_payee_phrase.payee_phrase_code)) as payee_phrase_code 
		FROM sup_claim_payee_phrase
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY payee_phrase_code ORDER BY payee_phrase_descript) = 1
),
LKP_sup_claim_payment_entry_operator_role_code AS (
	SELECT
	pay_entry_oper_role_code_descript,
	pay_entry_oper_role_code
	FROM (
		SELECT LTRIM(RTRIM(sup_claim_payment_entry_operator_role_code.pay_entry_oper_role_code_descript)) as pay_entry_oper_role_code_descript, LTRIM(RTRIM(sup_claim_payment_entry_operator_role_code.pay_entry_oper_role_code)) as pay_entry_oper_role_code FROM sup_claim_payment_entry_operator_role_code
		where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pay_entry_oper_role_code ORDER BY pay_entry_oper_role_code_descript) = 1
),
LKP_sup_claim_payment_type_code AS (
	SELECT
	pay_type_code_descript,
	pay_type_code
	FROM (
		SELECT LTRIM(RTRIM(sup_claim_payment_type_code.pay_type_code_descript)) as pay_type_code_descript, 
		LTRIM(RTRIM(sup_claim_payment_type_code.pay_type_code)) as pay_type_code FROM sup_claim_payment_type_code
		where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pay_type_code ORDER BY pay_type_code_descript) = 1
),
LKP_sup_claim_payment_workflow AS (
	SELECT
	payment_workflow,
	sup_claim_payment_workflow_id
	FROM (
		SELECT 
			payment_workflow,
			sup_claim_payment_workflow_id
		FROM sup_claim_payment_workflow
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_claim_payment_workflow_id ORDER BY payment_workflow) = 1
),
LKP_sup_pay_to_code AS (
	SELECT
	pay_to_code_descript,
	pay_to_code
	FROM (
		SELECT LTRIM(RTRIM(sup_claim_pay_to_code.pay_to_code_descript)) as pay_to_code_descript, LTRIM(RTRIM(sup_claim_pay_to_code.pay_to_code)) as pay_to_code 
		FROM sup_claim_pay_to_code
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pay_to_code ORDER BY pay_to_code_descript) = 1
),
LKP_sup_payment_method AS (
	SELECT
	payment_method,
	sup_payment_method_id
	FROM (
		SELECT 
			payment_method,
			sup_payment_method_id
		FROM sup_payment_method
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_payment_method_id ORDER BY payment_method) = 1
),
LKP_sup_payment_system AS (
	SELECT
	payment_system,
	sup_payment_system_id
	FROM (
		SELECT 
			payment_system,
			sup_payment_system_id
		FROM sup_payment_system
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_payment_system_id ORDER BY payment_system) = 1
),
EXP_get_descriptions AS (
	SELECT
	EXP_get_values.claim_pay_id,
	EXP_get_values.claim_party_occurrence_pay_id,
	EXP_get_values.claim_pay_ak_id,
	EXP_get_values.claim_pay_num,
	EXP_get_values.micro_ecd_draft_num,
	EXP_get_values.bank_acct_num,
	EXP_get_values.pay_delete_ind,
	EXP_get_values.total_pay_amt,
	EXP_get_values.pay_issued_date,
	EXP_get_values.pay_cashed_date,
	EXP_get_values.pay_to_code,
	LKP_sup_pay_to_code.pay_to_code_descript AS IN_pay_to_code_descript,
	-- *INF*: IIF(ISNULL(IN_pay_to_code_descript), 'N/A', IN_pay_to_code_descript)
	IFF(IN_pay_to_code_descript IS NULL, 'N/A', IN_pay_to_code_descript) AS pay_to_code_descript,
	EXP_get_values.payee_note,
	EXP_get_values.pay_ind,
	EXP_get_values.pay_type_code,
	LKP_sup_claim_payment_type_code.pay_type_code_descript AS IN_pay_type_code_descript,
	-- *INF*: IIF(ISNULL(IN_pay_type_code_descript), 'N/A', IN_pay_type_code_descript)
	IFF(IN_pay_type_code_descript IS NULL, 'N/A', IN_pay_type_code_descript) AS pay_type_code_descript,
	EXP_get_values.pay_entry_oper_id,
	EXP_get_values.pay_entry_oper_role_code,
	LKP_sup_claim_payment_entry_operator_role_code.pay_entry_oper_role_code_descript AS IN_pay_entry_oper_role_code_descript,
	-- *INF*: IIF(ISNULL(IN_pay_entry_oper_role_code_descript), 'N/A', IN_pay_entry_oper_role_code_descript)
	IFF(
	    IN_pay_entry_oper_role_code_descript IS NULL, 'N/A', IN_pay_entry_oper_role_code_descript
	) AS pay_entry_oper_role_code_descript,
	EXP_get_values.pay_disbursement_date,
	EXP_get_values.pay_disbursement_status,
	LKP_Pay_disbursement_status_description.pay_disbursement_status_descript,
	-- *INF*: iif(isnull(pay_disbursement_status_descript),'N/A',pay_disbursement_status_descript)
	IFF(pay_disbursement_status_descript IS NULL, 'N/A', pay_disbursement_status_descript) AS o_pay_disbursement_status_descript,
	EXP_get_values.pay_disbursement_loc_code,
	EXP_get_values.reported_to_irs_ind,
	EXP_get_values.pay_voided_date,
	EXP_get_values.pay_reposted_date,
	EXP_get_values.new_claim_num,
	EXP_get_values.new_draft_num,
	EXP_get_values.payee_phrase_code,
	LKP_sup_claim_payee_phrase.payee_phrase_descript AS IN_payee_phrase_descript,
	-- *INF*: IIF(ISNULL(IN_payee_phrase_descript), 'N/A', IN_payee_phrase_descript)
	IFF(IN_payee_phrase_descript IS NULL, 'N/A', IN_payee_phrase_descript) AS payee_phrase_descript,
	EXP_get_values.pay_to_the_order_of_name,
	EXP_get_values.memo_phrase_code,
	LKP_sup_claim_memo_phrase.memo_phrase_descript AS IN_memo_phrase_descript,
	-- *INF*: IIF(ISNULL(IN_memo_phrase_descript), 'N/A', IN_memo_phrase_descript)
	IFF(IN_memo_phrase_descript IS NULL, 'N/A', IN_memo_phrase_descript) AS memo_phrase_descript,
	EXP_get_values.memo_phrase_comment,
	EXP_get_values.mail_to_code,
	LKP_sup_claim_mail_to_code.mail_to_code_descript AS IN_mail_to_code_descript,
	-- *INF*: IIF(ISNULL(IN_mail_to_code_descript), 'N/A', IN_mail_to_code_descript)
	IFF(IN_mail_to_code_descript IS NULL, 'N/A', IN_mail_to_code_descript) AS mail_to_code_descript,
	EXP_get_values.mail_to_name,
	EXP_get_values.mail_to_addr,
	EXP_get_values.mail_to_city,
	EXP_get_values.mail_to_state,
	EXP_get_values.mail_to_zip,
	EXP_get_values.payee_type1 AS payee_type,
	EXP_get_values.claim_payee_seq_num,
	EXP_get_values.payee_code,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	EXP_get_values.eff_from_date2 AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	EXP_get_values.prim_payee_name,
	EXP_get_values.add_payee_name1,
	EXP_get_values.add_payee_name2,
	EXP_get_values.add_payee_name3,
	EXP_get_values.add_payee_name4,
	EXP_get_values.add_payee_name5,
	EXP_get_values.add_payee_name6,
	EXP_get_values.o_claim_party_role_code AS claim_party_role_code,
	LKP_sup_claim_party_role_code_desc.claim_party_role_descript,
	-- *INF*: iif(isnull(claim_party_role_descript)
	-- ,'N/A'
	-- ,claim_party_role_descript)
	IFF(claim_party_role_descript IS NULL, 'N/A', claim_party_role_descript) AS claim_party_role_descript_out,
	EXP_get_values.claim_party_addr,
	EXP_get_values.claim_party_city,
	EXP_get_values.claim_party_county,
	EXP_get_values.claim_party_state,
	EXP_get_values.claim_party_zip,
	EXP_get_values.addr_type,
	EXP_get_values.Tax_id AS tax_ssn_id,
	LKP_sup_payment_system.payment_system AS in_payment_system,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_payment_system)
	UDF_DEFAULT_VALUE_FOR_STRINGS(in_payment_system) AS o_payment_system,
	LKP_sup_payment_method.payment_method AS in_payment_method,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_payment_method)
	UDF_DEFAULT_VALUE_FOR_STRINGS(in_payment_method) AS o_payment_method,
	EXP_get_values.approval_status,
	EXP_get_values.approval_by_user_id,
	EXP_get_values.approval_date,
	EXP_get_values.denial_reason,
	EXP_get_values.special_processing,
	EXP_get_values.payee_category,
	LKP_sup_claim_payment_workflow.payment_workflow AS in_payment_workflow,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_payment_workflow)
	UDF_DEFAULT_VALUE_FOR_STRINGS(in_payment_workflow) AS o_payment_workflow,
	EXP_get_values.attached_document_count,
	-- *INF*: iif(isnull(attached_document_count),0,attached_document_count)
	IFF(attached_document_count IS NULL, 0, attached_document_count) AS o_attached_document_count
	FROM EXP_get_values
	LEFT JOIN LKP_Pay_disbursement_status_description
	ON LKP_Pay_disbursement_status_description.pay_disbursement_status = EXP_get_values.pay_disbursement_status
	LEFT JOIN LKP_sup_claim_mail_to_code
	ON LKP_sup_claim_mail_to_code.mail_to_code = EXP_get_values.mail_to_code
	LEFT JOIN LKP_sup_claim_memo_phrase
	ON LKP_sup_claim_memo_phrase.memo_phrase_code = EXP_get_values.memo_phrase_code
	LEFT JOIN LKP_sup_claim_party_role_code_desc
	ON LKP_sup_claim_party_role_code_desc.claim_party_role_code = EXP_get_values.o_claim_party_role_code
	LEFT JOIN LKP_sup_claim_payee_phrase
	ON LKP_sup_claim_payee_phrase.payee_phrase_code = EXP_get_values.payee_phrase_code
	LEFT JOIN LKP_sup_claim_payment_entry_operator_role_code
	ON LKP_sup_claim_payment_entry_operator_role_code.pay_entry_oper_role_code = EXP_get_values.pay_entry_oper_role_code
	LEFT JOIN LKP_sup_claim_payment_type_code
	ON LKP_sup_claim_payment_type_code.pay_type_code = EXP_get_values.pay_type_code
	LEFT JOIN LKP_sup_claim_payment_workflow
	ON LKP_sup_claim_payment_workflow.sup_claim_payment_workflow_id = EXP_get_values.sup_claim_payment_workflow_id
	LEFT JOIN LKP_sup_pay_to_code
	ON LKP_sup_pay_to_code.pay_to_code = EXP_get_values.pay_to_code
	LEFT JOIN LKP_sup_payment_method
	ON LKP_sup_payment_method.sup_payment_method_id = EXP_get_values.sup_payment_method_id
	LEFT JOIN LKP_sup_payment_system
	ON LKP_sup_payment_system.sup_payment_system_id = EXP_get_values.sup_payment_system_id
),
LKP_claim_payment_dim AS (
	SELECT
	claim_pay_dim_id,
	edw_claim_pay_pk_id,
	edw_claim_party_occurrence_pay_pk_id
	FROM (
		SELECT 
			claim_pay_dim_id,
			edw_claim_pay_pk_id,
			edw_claim_party_occurrence_pay_pk_id
		FROM claim_payment_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_pay_pk_id,edw_claim_party_occurrence_pay_pk_id ORDER BY claim_pay_dim_id) = 1
),
RTR_claim_payment_dim AS (
	SELECT
	EXP_get_descriptions.claim_pay_id,
	LKP_claim_payment_dim.claim_pay_dim_id,
	EXP_get_descriptions.claim_party_occurrence_pay_id,
	EXP_get_descriptions.claim_pay_ak_id,
	EXP_get_descriptions.claim_pay_num,
	EXP_get_descriptions.micro_ecd_draft_num,
	EXP_get_descriptions.bank_acct_num,
	EXP_get_descriptions.pay_delete_ind,
	EXP_get_descriptions.total_pay_amt,
	EXP_get_descriptions.pay_issued_date,
	EXP_get_descriptions.pay_cashed_date,
	EXP_get_descriptions.pay_to_code,
	EXP_get_descriptions.payee_note,
	EXP_get_descriptions.pay_ind,
	EXP_get_descriptions.pay_type_code,
	EXP_get_descriptions.pay_type_code_descript,
	EXP_get_descriptions.pay_entry_oper_id,
	EXP_get_descriptions.pay_entry_oper_role_code,
	EXP_get_descriptions.pay_entry_oper_role_code_descript,
	EXP_get_descriptions.pay_disbursement_date,
	EXP_get_descriptions.pay_disbursement_status,
	EXP_get_descriptions.o_pay_disbursement_status_descript,
	EXP_get_descriptions.reported_to_irs_ind,
	EXP_get_descriptions.pay_voided_date,
	EXP_get_descriptions.pay_reposted_date,
	EXP_get_descriptions.pay_to_code_descript,
	EXP_get_descriptions.new_claim_num,
	EXP_get_descriptions.new_draft_num,
	EXP_get_descriptions.payee_phrase_code,
	EXP_get_descriptions.payee_phrase_descript,
	EXP_get_descriptions.pay_to_the_order_of_name,
	EXP_get_descriptions.memo_phrase_code,
	EXP_get_descriptions.memo_phrase_descript,
	EXP_get_descriptions.memo_phrase_comment,
	EXP_get_descriptions.mail_to_code,
	EXP_get_descriptions.mail_to_code_descript,
	EXP_get_descriptions.mail_to_name,
	EXP_get_descriptions.mail_to_addr,
	EXP_get_descriptions.mail_to_city,
	EXP_get_descriptions.mail_to_state,
	EXP_get_descriptions.mail_to_zip,
	EXP_get_descriptions.payee_type,
	EXP_get_descriptions.claim_payee_seq_num,
	EXP_get_descriptions.payee_code,
	EXP_get_descriptions.crrnt_snpsht_flag,
	EXP_get_descriptions.audit_id,
	EXP_get_descriptions.eff_from_date,
	EXP_get_descriptions.eff_to_date,
	EXP_get_descriptions.created_date,
	EXP_get_descriptions.modified_date,
	EXP_get_descriptions.prim_payee_name,
	EXP_get_descriptions.add_payee_name1,
	EXP_get_descriptions.add_payee_name2,
	EXP_get_descriptions.add_payee_name3,
	EXP_get_descriptions.add_payee_name4,
	EXP_get_descriptions.add_payee_name5,
	EXP_get_descriptions.add_payee_name6,
	EXP_get_descriptions.claim_party_role_code,
	EXP_get_descriptions.claim_party_role_descript_out AS claim_party_role_descript,
	EXP_get_descriptions.claim_party_addr,
	EXP_get_descriptions.claim_party_city,
	EXP_get_descriptions.claim_party_county,
	EXP_get_descriptions.claim_party_state,
	EXP_get_descriptions.claim_party_zip,
	EXP_get_descriptions.addr_type,
	EXP_get_descriptions.tax_ssn_id,
	EXP_get_descriptions.o_payment_system AS payment_system,
	EXP_get_descriptions.o_payment_method AS payment_method,
	EXP_get_descriptions.approval_status,
	EXP_get_descriptions.approval_by_user_id,
	EXP_get_descriptions.approval_date,
	EXP_get_descriptions.denial_reason,
	EXP_get_descriptions.special_processing,
	EXP_get_descriptions.payee_category,
	EXP_get_descriptions.o_payment_workflow AS payment_workflow,
	EXP_get_descriptions.o_attached_document_count AS O_attached_document_count
	FROM EXP_get_descriptions
	LEFT JOIN LKP_claim_payment_dim
	ON LKP_claim_payment_dim.edw_claim_pay_pk_id = EXP_get_descriptions.claim_pay_id AND LKP_claim_payment_dim.edw_claim_party_occurrence_pay_pk_id = EXP_get_descriptions.claim_party_occurrence_pay_id
),
RTR_claim_payment_dim_Insert AS (SELECT * FROM RTR_claim_payment_dim WHERE ISNULL(claim_pay_dim_id)),
RTR_claim_payment_dim_Update AS (SELECT * FROM RTR_claim_payment_dim WHERE NOT ISNULL(claim_pay_dim_id)),
UPD_claim_payment_dim_insert AS (
	SELECT
	claim_pay_id AS claim_pay_id1, 
	claim_party_occurrence_pay_id AS claim_party_occurrence_pay_id1, 
	claim_pay_ak_id AS claim_pay_ak_id1, 
	claim_pay_num AS claim_pay_num1, 
	micro_ecd_draft_num AS micro_ecd_draft_num1, 
	bank_acct_num AS bank_acct_num1, 
	pay_delete_ind AS pay_delete_ind1, 
	total_pay_amt AS total_pay_amt1, 
	pay_issued_date AS pay_issued_date1, 
	pay_cashed_date AS pay_cashed_date1, 
	pay_to_code AS pay_to_code1, 
	pay_to_code_descript AS pay_to_code_descript1, 
	payee_note AS payee_note1, 
	pay_ind AS pay_ind1, 
	pay_type_code AS pay_type_code1, 
	pay_type_code_descript AS pay_type_code_descript1, 
	pay_entry_oper_id AS pay_entry_oper_id1, 
	pay_entry_oper_role_code AS pay_entry_oper_role_code1, 
	pay_entry_oper_role_code_descript AS pay_entry_oper_role_code_descript1, 
	pay_disbursement_date AS pay_disbursement_date1, 
	pay_disbursement_status AS pay_disbursement_status1, 
	o_pay_disbursement_status_descript AS o_pay_disbursement_status_descript1, 
	reported_to_irs_ind AS reported_to_irs_ind1, 
	pay_voided_date AS pay_voided_date1, 
	pay_reposted_date AS pay_reposted_date1, 
	new_claim_num AS new_claim_num1, 
	new_draft_num AS new_draft_num1, 
	payee_phrase_code AS payee_phrase_code1, 
	payee_phrase_descript AS payee_phrase_descript1, 
	pay_to_the_order_of_name, 
	memo_phrase_code AS memo_phrase_code1, 
	memo_phrase_descript AS memo_phrase_descript1, 
	memo_phrase_comment AS memo_phrase_comment1, 
	mail_to_code AS mail_to_code1, 
	mail_to_code_descript AS mail_to_code_descript1, 
	mail_to_name AS mail_to_name1, 
	mail_to_addr AS mail_to_addr1, 
	mail_to_city AS mail_to_city1, 
	mail_to_state AS mail_to_state1, 
	mail_to_zip AS mail_to_zip1, 
	payee_type AS payee_type1, 
	claim_payee_seq_num AS claim_payee_seq_num1, 
	payee_code AS payee_code1, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag1, 
	audit_id AS audit_id1, 
	eff_from_date AS eff_from_date1, 
	eff_to_date AS eff_to_date1, 
	created_date AS created_date1, 
	modified_date AS modified_date1, 
	prim_payee_name AS prim_payee_name1, 
	add_payee_name AS add_payee_name1, 
	add_payee_name2 AS add_payee_name21, 
	add_payee_name3 AS add_payee_name31, 
	add_payee_name4 AS add_payee_name41, 
	add_payee_name5 AS add_payee_name51, 
	add_payee_name6 AS add_payee_name61, 
	claim_party_role_code AS claim_party_role_code1, 
	claim_party_role_descript AS claim_party_role_descript1, 
	claim_party_addr AS claim_party_addr1, 
	claim_party_city AS claim_party_city1, 
	claim_party_county AS claim_party_county1, 
	claim_party_state AS claim_party_state1, 
	claim_party_zip AS claim_party_zip1, 
	addr_type AS addr_type1, 
	tax_ssn_id AS tax_ssn_id1, 
	payment_system, 
	payment_method, 
	approval_status AS approval_status1, 
	approval_by_user_id AS approval_by_user_id1, 
	approval_date AS approval_date1, 
	denial_reason AS denial_reason1, 
	special_processing AS special_processing1, 
	payee_category AS payee_category1, 
	payment_workflow AS payment_workflow1, 
	O_attached_document_count
	FROM RTR_claim_payment_dim_Insert
),
claim_payment_dim_insert AS (
	INSERT INTO claim_payment_dim
	(edw_claim_pay_pk_id, edw_claim_party_occurrence_pay_pk_id, edw_claim_pay_ak_id, claim_pay_num, micro_ecd_draft_num, bank_acct_num, pay_delete_ind, total_pay_amt, pay_issued_date, pay_cashed_date, pay_to_code, pay_to_code_descript, payee_note, pay_ind, pay_type_code, pay_type_code_descript, pay_entry_oper_id, pay_entry_oper_role_code, pay_entry_oper_role_code_descript, pay_disbursement_date, pay_disbursement_status, reported_to_irs_ind, pay_voided_date, pay_reposted_date, new_claim_num, new_draft_num, payee_phrase_code, payee_phrase_code_descript, pay_to_the_order_of_name, memo_phrase_code, memo_phrase_code_descript, memo_phrase_comment, mail_to_code, mail_to_code_descript, mail_to_name, mail_to_addr, mail_to_city, mail_to_state, mail_to_zip, payee_type, claim_payee_seq_num, payee_code, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date, prim_payee_name, add_payee_name1, add_payee_name2, add_payee_name3, add_payee_name4, add_payee_name5, add_payee_name6, prim_payee_role_code, prim_payee_role_code_descript, prim_payee_addr, prim_payee_city, prim_payee_county, prim_payee_state, prim_payee_zip, prim_payee_addr_type, prim_payee_tax_id, pay_disbursement_status_descript, payment_system, payment_method, approval_status, approval_by_user_id, approval_date, denial_reason, special_processing, payee_category, payment_workflow, attached_document_count)
	SELECT 
	claim_pay_id1 AS EDW_CLAIM_PAY_PK_ID, 
	claim_party_occurrence_pay_id1 AS EDW_CLAIM_PARTY_OCCURRENCE_PAY_PK_ID, 
	claim_pay_ak_id1 AS EDW_CLAIM_PAY_AK_ID, 
	claim_pay_num1 AS CLAIM_PAY_NUM, 
	micro_ecd_draft_num1 AS MICRO_ECD_DRAFT_NUM, 
	bank_acct_num1 AS BANK_ACCT_NUM, 
	pay_delete_ind1 AS PAY_DELETE_IND, 
	total_pay_amt1 AS TOTAL_PAY_AMT, 
	pay_issued_date1 AS PAY_ISSUED_DATE, 
	pay_cashed_date1 AS PAY_CASHED_DATE, 
	pay_to_code1 AS PAY_TO_CODE, 
	pay_to_code_descript1 AS PAY_TO_CODE_DESCRIPT, 
	payee_note1 AS PAYEE_NOTE, 
	pay_ind1 AS PAY_IND, 
	pay_type_code1 AS PAY_TYPE_CODE, 
	pay_type_code_descript1 AS PAY_TYPE_CODE_DESCRIPT, 
	pay_entry_oper_id1 AS PAY_ENTRY_OPER_ID, 
	pay_entry_oper_role_code1 AS PAY_ENTRY_OPER_ROLE_CODE, 
	pay_entry_oper_role_code_descript1 AS PAY_ENTRY_OPER_ROLE_CODE_DESCRIPT, 
	pay_disbursement_date1 AS PAY_DISBURSEMENT_DATE, 
	pay_disbursement_status1 AS PAY_DISBURSEMENT_STATUS, 
	reported_to_irs_ind1 AS REPORTED_TO_IRS_IND, 
	pay_voided_date1 AS PAY_VOIDED_DATE, 
	pay_reposted_date1 AS PAY_REPOSTED_DATE, 
	new_claim_num1 AS NEW_CLAIM_NUM, 
	new_draft_num1 AS NEW_DRAFT_NUM, 
	payee_phrase_code1 AS PAYEE_PHRASE_CODE, 
	payee_phrase_descript1 AS PAYEE_PHRASE_CODE_DESCRIPT, 
	PAY_TO_THE_ORDER_OF_NAME, 
	memo_phrase_code1 AS MEMO_PHRASE_CODE, 
	memo_phrase_descript1 AS MEMO_PHRASE_CODE_DESCRIPT, 
	memo_phrase_comment1 AS MEMO_PHRASE_COMMENT, 
	mail_to_code1 AS MAIL_TO_CODE, 
	mail_to_code_descript1 AS MAIL_TO_CODE_DESCRIPT, 
	mail_to_name1 AS MAIL_TO_NAME, 
	mail_to_addr1 AS MAIL_TO_ADDR, 
	mail_to_city1 AS MAIL_TO_CITY, 
	mail_to_state1 AS MAIL_TO_STATE, 
	mail_to_zip1 AS MAIL_TO_ZIP, 
	payee_type1 AS PAYEE_TYPE, 
	claim_payee_seq_num1 AS CLAIM_PAYEE_SEQ_NUM, 
	payee_code1 AS PAYEE_CODE, 
	crrnt_snpsht_flag1 AS CRRNT_SNPSHT_FLAG, 
	audit_id1 AS AUDIT_ID, 
	eff_from_date1 AS EFF_FROM_DATE, 
	eff_to_date1 AS EFF_TO_DATE, 
	created_date1 AS CREATED_DATE, 
	modified_date1 AS MODIFIED_DATE, 
	prim_payee_name1 AS PRIM_PAYEE_NAME, 
	ADD_PAYEE_NAME1, 
	add_payee_name21 AS ADD_PAYEE_NAME2, 
	add_payee_name31 AS ADD_PAYEE_NAME3, 
	add_payee_name41 AS ADD_PAYEE_NAME4, 
	add_payee_name51 AS ADD_PAYEE_NAME5, 
	add_payee_name61 AS ADD_PAYEE_NAME6, 
	claim_party_role_code1 AS PRIM_PAYEE_ROLE_CODE, 
	claim_party_role_descript1 AS PRIM_PAYEE_ROLE_CODE_DESCRIPT, 
	claim_party_addr1 AS PRIM_PAYEE_ADDR, 
	claim_party_city1 AS PRIM_PAYEE_CITY, 
	claim_party_county1 AS PRIM_PAYEE_COUNTY, 
	claim_party_state1 AS PRIM_PAYEE_STATE, 
	claim_party_zip1 AS PRIM_PAYEE_ZIP, 
	addr_type1 AS PRIM_PAYEE_ADDR_TYPE, 
	tax_ssn_id1 AS PRIM_PAYEE_TAX_ID, 
	o_pay_disbursement_status_descript1 AS PAY_DISBURSEMENT_STATUS_DESCRIPT, 
	PAYMENT_SYSTEM, 
	PAYMENT_METHOD, 
	approval_status1 AS APPROVAL_STATUS, 
	approval_by_user_id1 AS APPROVAL_BY_USER_ID, 
	approval_date1 AS APPROVAL_DATE, 
	denial_reason1 AS DENIAL_REASON, 
	special_processing1 AS SPECIAL_PROCESSING, 
	payee_category1 AS PAYEE_CATEGORY, 
	payment_workflow1 AS PAYMENT_WORKFLOW, 
	O_attached_document_count AS ATTACHED_DOCUMENT_COUNT
	FROM UPD_claim_payment_dim_insert
),
UPD_claim_payment_dim_update AS (
	SELECT
	claim_pay_dim_id AS claim_pay_dim_id3, 
	claim_party_occurrence_pay_id AS claim_party_occurrence_pay_id3, 
	claim_pay_ak_id AS claim_pay_ak_id3, 
	claim_pay_num AS claim_pay_num3, 
	micro_ecd_draft_num AS micro_ecd_draft_num3, 
	bank_acct_num AS bank_acct_num3, 
	pay_delete_ind AS pay_delete_ind3, 
	total_pay_amt AS total_pay_amt3, 
	pay_issued_date AS pay_issued_date3, 
	pay_cashed_date AS pay_cashed_date3, 
	pay_to_code AS pay_to_code3, 
	pay_to_code_descript AS pay_to_code_descript3, 
	payee_note AS payee_note3, 
	pay_ind AS pay_ind3, 
	pay_type_code AS pay_type_code3, 
	pay_type_code_descript AS pay_type_code_descript3, 
	pay_entry_oper_id AS pay_entry_oper_id3, 
	pay_entry_oper_role_code AS pay_entry_oper_role_code3, 
	pay_entry_oper_role_code_descript AS pay_entry_oper_role_code_descript3, 
	pay_disbursement_date AS pay_disbursement_date3, 
	pay_disbursement_status AS pay_disbursement_status3, 
	o_pay_disbursement_status_descript AS o_pay_disbursement_status_descript3, 
	reported_to_irs_ind AS reported_to_irs_ind3, 
	pay_voided_date AS pay_voided_date3, 
	pay_reposted_date AS pay_reposted_date3, 
	new_claim_num AS new_claim_num3, 
	new_draft_num AS new_draft_num3, 
	payee_phrase_code AS payee_phrase_code3, 
	payee_phrase_descript AS payee_phrase_descript3, 
	pay_to_the_order_of_name, 
	memo_phrase_code AS memo_phrase_code3, 
	memo_phrase_descript AS memo_phrase_descript3, 
	memo_phrase_comment AS memo_phrase_comment3, 
	mail_to_code AS mail_to_code3, 
	mail_to_code_descript AS mail_to_code_descript3, 
	mail_to_name AS mail_to_name3, 
	mail_to_addr AS mail_to_addr3, 
	mail_to_city AS mail_to_city3, 
	mail_to_state AS mail_to_state3, 
	mail_to_zip AS mail_to_zip3, 
	payee_type AS payee_type3, 
	claim_payee_seq_num AS claim_payee_seq_num3, 
	payee_code AS payee_code3, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag3, 
	audit_id AS audit_id3, 
	eff_from_date AS eff_from_date3, 
	eff_to_date AS eff_to_date3, 
	created_date AS created_date3, 
	modified_date AS modified_date3, 
	prim_payee_name AS prim_payee_name3, 
	add_payee_name1, 
	add_payee_name2 AS add_payee_name23, 
	add_payee_name AS add_payee_name33, 
	add_payee_name4 AS add_payee_name43, 
	add_payee_name5 AS add_payee_name53, 
	add_payee_name6 AS add_payee_name63, 
	claim_party_role_code AS claim_party_role_code3, 
	claim_party_role_descript AS claim_party_role_descript3, 
	claim_party_addr AS claim_party_addr3, 
	claim_party_city AS claim_party_city3, 
	claim_party_county AS claim_party_county3, 
	claim_party_state AS claim_party_state3, 
	claim_party_zip AS claim_party_zip3, 
	addr_type AS addr_type3, 
	tax_ssn_id AS tax_ssn_id3, 
	payment_system, 
	payment_method, 
	approval_status AS approval_status3, 
	approval_by_user_id AS approval_by_user_id3, 
	approval_date AS approval_date3, 
	denial_reason AS denial_reason3, 
	special_processing AS special_processing3, 
	payee_category AS payee_category3, 
	payment_workflow AS payment_workflow3, 
	O_attached_document_count
	FROM RTR_claim_payment_dim_Update
),
claim_payment_dim_update AS (
	MERGE INTO claim_payment_dim AS T
	USING UPD_claim_payment_dim_update AS S
	ON T.claim_pay_dim_id = S.claim_pay_dim_id3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.claim_pay_num = S.claim_pay_num3, T.micro_ecd_draft_num = S.micro_ecd_draft_num3, T.bank_acct_num = S.bank_acct_num3, T.pay_delete_ind = S.pay_delete_ind3, T.total_pay_amt = S.total_pay_amt3, T.pay_issued_date = S.pay_issued_date3, T.pay_cashed_date = S.pay_cashed_date3, T.pay_to_code = S.pay_to_code3, T.pay_to_code_descript = S.pay_to_code_descript3, T.payee_note = S.payee_note3, T.pay_ind = S.pay_ind3, T.pay_type_code = S.pay_type_code3, T.pay_type_code_descript = S.pay_type_code_descript3, T.pay_entry_oper_id = S.pay_entry_oper_id3, T.pay_entry_oper_role_code = S.pay_entry_oper_role_code3, T.pay_entry_oper_role_code_descript = S.pay_entry_oper_role_code_descript3, T.pay_disbursement_date = S.pay_disbursement_date3, T.pay_disbursement_status = S.pay_disbursement_status3, T.reported_to_irs_ind = S.reported_to_irs_ind3, T.pay_voided_date = S.pay_voided_date3, T.pay_reposted_date = S.pay_reposted_date3, T.new_claim_num = S.new_claim_num3, T.new_draft_num = S.new_draft_num3, T.payee_phrase_code = S.payee_phrase_code3, T.payee_phrase_code_descript = S.payee_phrase_descript3, T.pay_to_the_order_of_name = S.pay_to_the_order_of_name, T.memo_phrase_code = S.memo_phrase_code3, T.memo_phrase_code_descript = S.memo_phrase_descript3, T.memo_phrase_comment = S.memo_phrase_comment3, T.mail_to_code = S.mail_to_code3, T.mail_to_code_descript = S.mail_to_code_descript3, T.mail_to_name = S.mail_to_name3, T.mail_to_addr = S.mail_to_addr3, T.mail_to_city = S.mail_to_city3, T.mail_to_state = S.mail_to_state3, T.mail_to_zip = S.mail_to_zip3, T.payee_type = S.payee_type3, T.claim_payee_seq_num = S.claim_payee_seq_num3, T.payee_code = S.payee_code3, T.audit_id = S.audit_id3, T.modified_date = S.modified_date3, T.prim_payee_name = S.prim_payee_name3, T.add_payee_name1 = S.add_payee_name1, T.add_payee_name2 = S.add_payee_name23, T.add_payee_name3 = S.add_payee_name33, T.add_payee_name4 = S.add_payee_name43, T.add_payee_name5 = S.add_payee_name53, T.add_payee_name6 = S.add_payee_name63, T.prim_payee_role_code = S.claim_party_role_code3, T.prim_payee_role_code_descript = S.claim_party_role_descript3, T.prim_payee_addr = S.claim_party_addr3, T.prim_payee_city = S.claim_party_city3, T.prim_payee_county = S.claim_party_county3, T.prim_payee_state = S.claim_party_state3, T.prim_payee_zip = S.claim_party_zip3, T.prim_payee_addr_type = S.addr_type3, T.prim_payee_tax_id = S.tax_ssn_id3, T.pay_disbursement_status_descript = S.o_pay_disbursement_status_descript3, T.payment_system = S.payment_system, T.payment_method = S.payment_method, T.approval_status = S.approval_status3, T.approval_by_user_id = S.approval_by_user_id3, T.approval_date = S.approval_date3, T.denial_reason = S.denial_reason3, T.special_processing = S.special_processing3, T.payee_category = S.payee_category3, T.payment_workflow = S.payment_workflow3, T.attached_document_count = S.O_attached_document_count
),
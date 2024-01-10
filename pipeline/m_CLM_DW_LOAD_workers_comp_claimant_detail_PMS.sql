WITH
LKP_CLAIM_COVERAGE AS (
	SELECT
	PIF_42X6_stage_id,
	PIF_SYMBOL,
	PIF_POLICY_NUMBER,
	PIF_MODULE,
	IPFCX6_INSURANCE_LINE
	FROM (
		SELECT 
			PIF_42X6_stage_id,
			PIF_SYMBOL,
			PIF_POLICY_NUMBER,
			PIF_MODULE,
			IPFCX6_INSURANCE_LINE
		FROM PIF_42X6_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PIF_SYMBOL,PIF_POLICY_NUMBER,PIF_MODULE,IPFCX6_INSURANCE_LINE ORDER BY PIF_42X6_stage_id) = 1
),
LKP_42GQ_MS2 AS (
	SELECT
	IPFCGQ_OCCUPATION,
	PIF_SYMBOL,
	PIF_POLICY_NUMBER,
	PIF_MODULE,
	IPFCGQ_YEAR_OF_LOSS,
	IPFCGQ_MONTH_OF_LOSS,
	IPFCGQ_DAY_OF_LOSS,
	IPFCGQ_LOSS_OCCURENCE,
	IPFCGQ_LOSS_CLAIMANT
	FROM (
		SELECT 
			IPFCGQ_OCCUPATION,
			PIF_SYMBOL,
			PIF_POLICY_NUMBER,
			PIF_MODULE,
			IPFCGQ_YEAR_OF_LOSS,
			IPFCGQ_MONTH_OF_LOSS,
			IPFCGQ_DAY_OF_LOSS,
			IPFCGQ_LOSS_OCCURENCE,
			IPFCGQ_LOSS_CLAIMANT
		FROM PIF_42GQ_MS2_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PIF_SYMBOL,PIF_POLICY_NUMBER,PIF_MODULE,IPFCGQ_YEAR_OF_LOSS,IPFCGQ_MONTH_OF_LOSS,IPFCGQ_DAY_OF_LOSS,IPFCGQ_LOSS_OCCURENCE,IPFCGQ_LOSS_CLAIMANT ORDER BY IPFCGQ_OCCUPATION) = 1
),
LKP_Sup_State AS (
	SELECT
	state_code,
	state_abbrev
	FROM (
		SELECT 
		ltrim(rtrim(sup_state.state_code)) as state_code, 
		ltrim(rtrim(sup_state.state_abbrev)) as state_abbrev 
		FROM sup_state
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_abbrev ORDER BY state_code) = 1
),
LKP_PIF_42GP AS (
	SELECT
	ipfcgp_loss_accident_state,
	pif_symbol,
	pif_policy_number,
	pif_module,
	ipfcgp_year_of_loss,
	ipfcgp_month_of_loss,
	ipfcgp_day_of_loss,
	ipfcgp_loss_occurence
	FROM (
		SELECT 
			ipfcgp_loss_accident_state,
			pif_symbol,
			pif_policy_number,
			pif_module,
			ipfcgp_year_of_loss,
			ipfcgp_month_of_loss,
			ipfcgp_day_of_loss,
			ipfcgp_loss_occurence
		FROM pif_42gp_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module,ipfcgp_year_of_loss,ipfcgp_month_of_loss,ipfcgp_day_of_loss,ipfcgp_loss_occurence ORDER BY ipfcgp_loss_accident_state) = 1
),
SQ_PIF_42GQ_CMT_stage AS (
	SELECT A.pif_42gq_cmt_stage_id, A.pif_symbol, A.pif_policy_number, A.pif_module, A.ipfcgq_rec_length, A.ipfcgq_action_code, A.ipfcgq_file_id, A.ipfcgq_segment_id, A.ipfcgq_segment_level_code, A.ipfcgq_segment_part_code, A.ipfcgq_sub_part_code, A.ipfcgq_year_of_loss, A.ipfcgq_month_of_loss, A.ipfcgq_day_of_loss, A.ipfcgq_loss_occurence, A.ipfcgq_loss_claimant, A.ipfcgq_claimant_use_code, A.ipfcgq_claimant_use_seq, A.ipfcgq_year_process, A.ipfcgq_month_process, A.ipfcgq_day_process, A.ipfcgq_year_change_entry, A.ipfcgq_month_change_entry, A.ipfcgq_day_change_entry, A.ipfcgq_sequence_change_entry, A.ipfcgq_segment_status, A.ipfcgq_entry_operator, A.ipfcgq_loss_payee_num, A.ipfcgq_loss_payee_type, A.ipfcgq_reinsurance_indicator, A.ipfcgq_loss_adjustor_no, A.ipfcgq_loss_examiner, A.ipfcgq_other_assignment, A.ipfcgq_account_entered_date, A.ipfcgq_time_employed_years, A.ipfcgq_time_employed_months, A.ipfcgq_claim_number, A.ipfcgq_claim_filler, A.ipfcgq_claim_status, A.ipfcgq_loss_original_reserve, A.ipfcgq_os_loss_reserve, A.ipfcgq_os_expense_reserve, A.ipfcgq_total_loss, A.ipfcgq_total_expense, A.ipfcgq_total_recoveries, A.ipfcgq_beg_os_loss_reserve, A.ipfcgq_contact_year, A.ipfcgq_contact_month, A.ipfcgq_contact_day, A.ipfcgq_denial_year, A.ipfcgq_denial_month, A.ipfcgq_denial_day, A.ipfcgq_loss_handling_office, A.ipfcgq_loss_handling_branch, A.ipfcgq_loss_aia_codes_1_2, A.ipfcgq_loss_aia_codes_3_4, A.ipfcgq_loss_aia_codes_5_6, A.ipfcgq_aia_sub_code, A.ipfcgq_loss_age, A.ipfcgq_claimant_type, A.ipfcgq_loss_claimant_name, A.ipfcgq_claimant_birth_year, A.ipfcgq_claimant_birth_month, A.ipfcgq_claimant_birth_day, A.ipfcgq_loss_sex, A.ipfcgq_marital_status, A.ipfcgq_accident_cov_state, A.ipfcgq_cib_report_ind, A.ipfcgq_official_report_ind, A.ipfcgq_death_indicator, A.ipfcgq_excess_loss_ind, A.ipfcgq_loss_suit, A.ipfcgq_other_insurance, A.ipfcgq_recorded_statement, A.ipfcgq_loss_cause, A.ipfcgq_loss_coverage_code, A.ipfcgq_catastrophe_no, A.ipfcgq_excess_catastrophe_no, A.ipfcgq_type_disability, A.ipfcgq_liab_over_ind, A.ipfcgq_leg_exp_clmts, A.ipfcgq_percent_disability, A.ipfcgq_loss_cov_code_2, A.ipfcgq_event_cat_code, A.ipfcgq_claim_code_basis, A.ipfcgq_acct_state_only, A.ipfcgq_mngd_care_org, A.ipfcgq_number_of_part78, A.ipfcgq_offset_onset_ind, A.ipfcgq_date_hire, A.ipfcgq_pms_future_use_gq, A.ipfcgq_direct_reporting, A.ipfcgq_cust_spl_use_gq, A.ipfcgq_yr2000_cust_use, A.logical_flag, A.extract_date, A.as_of_date, A.record_count, A.source_system_id 
	FROM
	 pif_42gq_cmt_stage A
	where A.logical_flag in ('0','1')
),
EXP_VALIDATE_workers_comp_claimant_detail_PMS AS (
	SELECT
	PIF_42GQ_CMT_stage_id,
	PIF_SYMBOL,
	PIF_POLICY_NUMBER,
	PIF_MODULE,
	IPFCGQ_REC_LENGTH,
	IPFCGQ_ACTION_CODE,
	IPFCGQ_FILE_ID,
	IPFCGQ_SEGMENT_ID,
	IPFCGQ_SEGMENT_LEVEL_CODE,
	IPFCGQ_SEGMENT_PART_CODE,
	IPFCGQ_SUB_PART_CODE,
	IPFCGQ_YEAR_OF_LOSS,
	IPFCGQ_MONTH_OF_LOSS,
	IPFCGQ_DAY_OF_LOSS,
	IPFCGQ_LOSS_OCCURENCE,
	IPFCGQ_LOSS_CLAIMANT,
	IPFCGQ_CLAIMANT_USE_CODE,
	IPFCGQ_CLAIMANT_USE_SEQ,
	IPFCGQ_YEAR_PROCESS,
	IPFCGQ_MONTH_PROCESS,
	IPFCGQ_DAY_PROCESS,
	IPFCGQ_YEAR_CHANGE_ENTRY,
	IPFCGQ_MONTH_CHANGE_ENTRY,
	IPFCGQ_DAY_CHANGE_ENTRY,
	IPFCGQ_SEQUENCE_CHANGE_ENTRY,
	IPFCGQ_SEGMENT_STATUS,
	IPFCGQ_ENTRY_OPERATOR,
	IPFCGQ_LOSS_PAYEE_NUM,
	IPFCGQ_LOSS_PAYEE_TYPE,
	IPFCGQ_REINSURANCE_INDICATOR,
	IPFCGQ_LOSS_ADJUSTOR_NO,
	IPFCGQ_LOSS_EXAMINER,
	IPFCGQ_OTHER_ASSIGNMENT,
	IPFCGQ_ACCOUNT_ENTERED_DATE,
	IPFCGQ_TIME_EMPLOYED_YEARS,
	IPFCGQ_TIME_EMPLOYED_MONTHS,
	IPFCGQ_CLAIM_NUMBER,
	IPFCGQ_CLAIM_FILLER,
	IPFCGQ_CLAIM_STATUS,
	IPFCGQ_LOSS_ORIGINAL_RESERVE,
	IPFCGQ_OS_LOSS_RESERVE,
	IPFCGQ_OS_EXPENSE_RESERVE,
	IPFCGQ_TOTAL_LOSS,
	IPFCGQ_TOTAL_EXPENSE,
	IPFCGQ_TOTAL_RECOVERIES,
	IPFCGQ_BEG_OS_LOSS_RESERVE,
	IPFCGQ_CONTACT_YEAR,
	IPFCGQ_CONTACT_MONTH,
	IPFCGQ_CONTACT_DAY,
	IPFCGQ_DENIAL_YEAR,
	IPFCGQ_DENIAL_MONTH,
	IPFCGQ_DENIAL_DAY,
	IPFCGQ_LOSS_HANDLING_OFFICE,
	IPFCGQ_LOSS_HANDLING_BRANCH,
	IPFCGQ_LOSS_AIA_CODES_1_2,
	IPFCGQ_LOSS_AIA_CODES_3_4,
	IPFCGQ_LOSS_AIA_CODES_5_6,
	IPFCGQ_AIA_SUB_CODE,
	IPFCGQ_LOSS_AGE,
	IPFCGQ_CLAIMANT_TYPE,
	IPFCGQ_LOSS_CLAIMANT_NAME,
	IPFCGQ_CLAIMANT_BIRTH_YEAR,
	IPFCGQ_CLAIMANT_BIRTH_MONTH,
	IPFCGQ_CLAIMANT_BIRTH_DAY,
	IPFCGQ_LOSS_SEX,
	IPFCGQ_MARITAL_STATUS,
	IPFCGQ_ACCIDENT_COV_STATE,
	IPFCGQ_CIB_REPORT_IND,
	IPFCGQ_OFFICIAL_REPORT_IND,
	IPFCGQ_DEATH_INDICATOR,
	IPFCGQ_EXCESS_LOSS_IND,
	IPFCGQ_LOSS_SUIT,
	IPFCGQ_OTHER_INSURANCE,
	IPFCGQ_RECORDED_STATEMENT,
	IPFCGQ_LOSS_CAUSE,
	IPFCGQ_LOSS_COVERAGE_CODE,
	IPFCGQ_CATASTROPHE_NO,
	IPFCGQ_EXCESS_CATASTROPHE_NO,
	IPFCGQ_TYPE_DISABILITY,
	IPFCGQ_LIAB_OVER_IND,
	IPFCGQ_LEG_EXP_CLMTS,
	IPFCGQ_PERCENT_DISABILITY,
	IPFCGQ_LOSS_COV_CODE_2,
	IPFCGQ_EVENT_CAT_CODE,
	IPFCGQ_CLAIM_CODE_BASIS,
	IPFCGQ_ACCT_STATE_ONLY,
	IPFCGQ_MNGD_CARE_ORG,
	IPFCGQ_NUMBER_OF_PART78,
	IPFCGQ_OFFSET_ONSET_IND,
	IPFCGQ_DATE_HIRE,
	IPFCGQ_PMS_FUTURE_USE_GQ,
	IPFCGQ_DIRECT_REPORTING,
	IPFCGQ_CUST_SPL_USE_GQ,
	IPFCGQ_YR2000_CUST_USE,
	logical_flag,
	EXTRACT_DATE,
	AS_OF_DATE,
	RECORD_COUNT,
	SOURCE_SYSTEM_ID,
	'WC' AS V_INS_LINE,
	-- *INF*: :LKP.LKP_CLAIM_COVERAGE(PIF_SYMBOL,PIF_POLICY_NUMBER,PIF_MODULE,V_INS_LINE)
	LKP_CLAIM_COVERAGE_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_V_INS_LINE.PIF_42X6_stage_id AS V_FILTER_FLAG,
	-- *INF*: IIF(ISNULL(V_FILTER_FLAG),'N','Y')
	IFF(V_FILTER_FLAG IS NULL,
		'N',
		'Y'
	) AS FILTER_FLAG
	FROM SQ_PIF_42GQ_CMT_stage
	LEFT JOIN LKP_CLAIM_COVERAGE LKP_CLAIM_COVERAGE_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_V_INS_LINE
	ON LKP_CLAIM_COVERAGE_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_V_INS_LINE.PIF_SYMBOL = PIF_SYMBOL
	AND LKP_CLAIM_COVERAGE_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_V_INS_LINE.PIF_POLICY_NUMBER = PIF_POLICY_NUMBER
	AND LKP_CLAIM_COVERAGE_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_V_INS_LINE.PIF_MODULE = PIF_MODULE
	AND LKP_CLAIM_COVERAGE_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_V_INS_LINE.IPFCX6_INSURANCE_LINE = V_INS_LINE

),
FIL_workers_comp_claimant_detail_PMS AS (
	SELECT
	PIF_42GQ_CMT_stage_id, 
	PIF_SYMBOL, 
	PIF_POLICY_NUMBER, 
	PIF_MODULE, 
	IPFCGQ_REC_LENGTH, 
	IPFCGQ_ACTION_CODE, 
	IPFCGQ_FILE_ID, 
	IPFCGQ_SEGMENT_ID, 
	IPFCGQ_SEGMENT_LEVEL_CODE, 
	IPFCGQ_SEGMENT_PART_CODE, 
	IPFCGQ_SUB_PART_CODE, 
	IPFCGQ_YEAR_OF_LOSS, 
	IPFCGQ_MONTH_OF_LOSS, 
	IPFCGQ_DAY_OF_LOSS, 
	IPFCGQ_LOSS_OCCURENCE, 
	IPFCGQ_LOSS_CLAIMANT, 
	IPFCGQ_CLAIMANT_USE_CODE, 
	IPFCGQ_CLAIMANT_USE_SEQ, 
	IPFCGQ_YEAR_PROCESS, 
	IPFCGQ_MONTH_PROCESS, 
	IPFCGQ_DAY_PROCESS, 
	IPFCGQ_YEAR_CHANGE_ENTRY, 
	IPFCGQ_MONTH_CHANGE_ENTRY, 
	IPFCGQ_DAY_CHANGE_ENTRY, 
	IPFCGQ_SEQUENCE_CHANGE_ENTRY, 
	IPFCGQ_SEGMENT_STATUS, 
	IPFCGQ_ENTRY_OPERATOR, 
	IPFCGQ_LOSS_PAYEE_NUM, 
	IPFCGQ_LOSS_PAYEE_TYPE, 
	IPFCGQ_REINSURANCE_INDICATOR, 
	IPFCGQ_LOSS_ADJUSTOR_NO, 
	IPFCGQ_LOSS_EXAMINER, 
	IPFCGQ_OTHER_ASSIGNMENT, 
	IPFCGQ_ACCOUNT_ENTERED_DATE, 
	IPFCGQ_TIME_EMPLOYED_YEARS, 
	IPFCGQ_TIME_EMPLOYED_MONTHS, 
	IPFCGQ_CLAIM_NUMBER, 
	IPFCGQ_CLAIM_FILLER, 
	IPFCGQ_CLAIM_STATUS, 
	IPFCGQ_LOSS_ORIGINAL_RESERVE, 
	IPFCGQ_OS_LOSS_RESERVE, 
	IPFCGQ_OS_EXPENSE_RESERVE, 
	IPFCGQ_TOTAL_LOSS, 
	IPFCGQ_TOTAL_EXPENSE, 
	IPFCGQ_TOTAL_RECOVERIES, 
	IPFCGQ_BEG_OS_LOSS_RESERVE, 
	IPFCGQ_CONTACT_YEAR, 
	IPFCGQ_CONTACT_MONTH, 
	IPFCGQ_CONTACT_DAY, 
	IPFCGQ_DENIAL_YEAR, 
	IPFCGQ_DENIAL_MONTH, 
	IPFCGQ_DENIAL_DAY, 
	IPFCGQ_LOSS_HANDLING_OFFICE, 
	IPFCGQ_LOSS_HANDLING_BRANCH, 
	IPFCGQ_LOSS_AIA_CODES_1_2, 
	IPFCGQ_LOSS_AIA_CODES_3_4, 
	IPFCGQ_LOSS_AIA_CODES_5_6, 
	IPFCGQ_AIA_SUB_CODE, 
	IPFCGQ_LOSS_AGE, 
	IPFCGQ_CLAIMANT_TYPE, 
	IPFCGQ_LOSS_CLAIMANT_NAME, 
	IPFCGQ_CLAIMANT_BIRTH_YEAR, 
	IPFCGQ_CLAIMANT_BIRTH_MONTH, 
	IPFCGQ_CLAIMANT_BIRTH_DAY, 
	IPFCGQ_LOSS_SEX, 
	IPFCGQ_MARITAL_STATUS, 
	IPFCGQ_ACCIDENT_COV_STATE, 
	IPFCGQ_CIB_REPORT_IND, 
	IPFCGQ_OFFICIAL_REPORT_IND, 
	IPFCGQ_DEATH_INDICATOR, 
	IPFCGQ_EXCESS_LOSS_IND, 
	IPFCGQ_LOSS_SUIT, 
	IPFCGQ_OTHER_INSURANCE, 
	IPFCGQ_RECORDED_STATEMENT, 
	IPFCGQ_LOSS_CAUSE, 
	IPFCGQ_LOSS_COVERAGE_CODE, 
	IPFCGQ_CATASTROPHE_NO, 
	IPFCGQ_EXCESS_CATASTROPHE_NO, 
	IPFCGQ_TYPE_DISABILITY, 
	IPFCGQ_LIAB_OVER_IND, 
	IPFCGQ_LEG_EXP_CLMTS, 
	IPFCGQ_PERCENT_DISABILITY, 
	IPFCGQ_LOSS_COV_CODE_2, 
	IPFCGQ_EVENT_CAT_CODE, 
	IPFCGQ_CLAIM_CODE_BASIS, 
	IPFCGQ_ACCT_STATE_ONLY, 
	IPFCGQ_MNGD_CARE_ORG, 
	IPFCGQ_NUMBER_OF_PART78, 
	IPFCGQ_OFFSET_ONSET_IND, 
	IPFCGQ_DATE_HIRE, 
	IPFCGQ_PMS_FUTURE_USE_GQ, 
	IPFCGQ_DIRECT_REPORTING, 
	IPFCGQ_CUST_SPL_USE_GQ, 
	IPFCGQ_YR2000_CUST_USE, 
	logical_flag, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	FILTER_FLAG
	FROM EXP_VALIDATE_workers_comp_claimant_detail_PMS
	WHERE FILTER_FLAG='Y'
),
EXP_Lkp_Values_workers_comp_claimant_detail_PMS AS (
	SELECT
	PIF_SYMBOL AS IN_PIF_SYMBOL,
	PIF_POLICY_NUMBER AS IN_PIF_POLICY_NUMBER,
	PIF_MODULE AS IN_PIF_MODULE,
	IPFCGQ_YEAR_OF_LOSS AS IN_IPFCGQ_YEAR_OF_LOSS,
	IPFCGQ_MONTH_OF_LOSS AS IN_IPFCGQ_MONTH_OF_LOSS,
	IPFCGQ_DAY_OF_LOSS AS IN_IPFCGQ_DAY_OF_LOSS,
	IPFCGQ_LOSS_OCCURENCE AS IN_IPFCGQ_LOSS_OCCURENCE,
	IPFCGQ_LOSS_CLAIMANT AS IN_IPFCGQ_LOSS_CLAIMANT,
	IPFCGQ_MARITAL_STATUS AS IN_IPFCGQ_MARITAL_STATUS,
	IPFCGQ_LOSS_AIA_CODES_1_2 AS IN_IPFCGQ_LOSS_AIA_CODES_1_2,
	IPFCGQ_LOSS_AIA_CODES_3_4 AS IN_IPFCGQ_LOSS_AIA_CODES_3_4,
	IPFCGQ_LOSS_AIA_CODES_5_6 AS IN_IPFCGQ_LOSS_AIA_CODES_5_6,
	IPFCGQ_DEATH_INDICATOR AS IN_IPFCGQ_DEATH_INDICATOR,
	IPFCGQ_LOSS_COV_CODE_2 AS IN_IPFCGQ_LOSS_COV_CODE_2,
	IPFCGQ_TYPE_DISABILITY AS IN_IPFCGQ_TYPE_DISABILITY,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_PIF_SYMBOL))),'N/A',IIF(IS_SPACES(IN_PIF_SYMBOL),'N/A',LTRIM(RTRIM(IN_PIF_SYMBOL))))
	IFF(LTRIM(RTRIM(IN_PIF_SYMBOL
			)
		) IS NULL,
		'N/A',
		IFF(LENGTH(IN_PIF_SYMBOL)>0 AND TRIM(IN_PIF_SYMBOL)='',
			'N/A',
			LTRIM(RTRIM(IN_PIF_SYMBOL
				)
			)
		)
	) AS V_PIF_SYMBOL,
	V_PIF_SYMBOL AS PIF_SYMBOL,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_PIF_POLICY_NUMBER))),'N/A',IIF(IS_SPACES(IN_PIF_POLICY_NUMBER),'N/A',LTRIM(RTRIM(IN_PIF_POLICY_NUMBER))))
	IFF(LTRIM(RTRIM(IN_PIF_POLICY_NUMBER
			)
		) IS NULL,
		'N/A',
		IFF(LENGTH(IN_PIF_POLICY_NUMBER)>0 AND TRIM(IN_PIF_POLICY_NUMBER)='',
			'N/A',
			LTRIM(RTRIM(IN_PIF_POLICY_NUMBER
				)
			)
		)
	) AS V_PIF_POLICY_NUMBER,
	V_PIF_POLICY_NUMBER AS PIF_POLICY_NUMBER,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_PIF_MODULE))),'N/A',IIF(IS_SPACES(IN_PIF_MODULE),'N/A',LTRIM(RTRIM(IN_PIF_MODULE))))
	IFF(LTRIM(RTRIM(IN_PIF_MODULE
			)
		) IS NULL,
		'N/A',
		IFF(LENGTH(IN_PIF_MODULE)>0 AND TRIM(IN_PIF_MODULE)='',
			'N/A',
			LTRIM(RTRIM(IN_PIF_MODULE
				)
			)
		)
	) AS V_PIF_MODULE,
	V_PIF_MODULE AS PIF_MODULE,
	-- *INF*: IIF(ISNULL(IN_IPFCGQ_YEAR_OF_LOSS),1800,IN_IPFCGQ_YEAR_OF_LOSS)
	IFF(IN_IPFCGQ_YEAR_OF_LOSS IS NULL,
		1800,
		IN_IPFCGQ_YEAR_OF_LOSS
	) AS V_IPFCGQ_YEAR_OF_LOSS,
	V_IPFCGQ_YEAR_OF_LOSS AS IPFCGQ_YEAR_OF_LOSS,
	-- *INF*: IIF(ISNULL(IN_IPFCGQ_MONTH_OF_LOSS),01,IN_IPFCGQ_MONTH_OF_LOSS)
	IFF(IN_IPFCGQ_MONTH_OF_LOSS IS NULL,
		01,
		IN_IPFCGQ_MONTH_OF_LOSS
	) AS V_IPFCGQ_MONTH_OF_LOSS,
	V_IPFCGQ_MONTH_OF_LOSS AS IPFCGQ_MONTH_OF_LOSS,
	-- *INF*: IIF(ISNULL(IN_IPFCGQ_DAY_OF_LOSS),01,IN_IPFCGQ_DAY_OF_LOSS)
	IFF(IN_IPFCGQ_DAY_OF_LOSS IS NULL,
		01,
		IN_IPFCGQ_DAY_OF_LOSS
	) AS V_IPFCGQ_DAY_OF_LOSS,
	V_IPFCGQ_DAY_OF_LOSS AS IPFCGQ_DAY_OF_LOSS,
	-- *INF*: IIF(ISNULL(IN_IPFCGQ_LOSS_OCCURENCE),'000',IN_IPFCGQ_LOSS_OCCURENCE)
	IFF(IN_IPFCGQ_LOSS_OCCURENCE IS NULL,
		'000',
		IN_IPFCGQ_LOSS_OCCURENCE
	) AS V_IPFCGQ_LOSS_OCCURENCE,
	V_IPFCGQ_LOSS_OCCURENCE AS IPFCGQ_LOSS_OCCURENCE,
	-- *INF*: IIF(ISNULL(IN_IPFCGQ_LOSS_CLAIMANT),'000',IN_IPFCGQ_LOSS_CLAIMANT)
	IFF(IN_IPFCGQ_LOSS_CLAIMANT IS NULL,
		'000',
		IN_IPFCGQ_LOSS_CLAIMANT
	) AS V_IPFCGQ_LOSS_CLAIMANT,
	V_IPFCGQ_LOSS_CLAIMANT AS IPFCGQ_LOSS_CLAIMANT,
	-- *INF*: TO_CHAR(V_IPFCGQ_YEAR_OF_LOSS)
	TO_CHAR(V_IPFCGQ_YEAR_OF_LOSS
	) AS V_LOSS_YEAR,
	-- *INF*: TO_CHAR(V_IPFCGQ_MONTH_OF_LOSS)
	TO_CHAR(V_IPFCGQ_MONTH_OF_LOSS
	) AS V_LOSS_MONTH,
	-- *INF*: TO_CHAR(V_IPFCGQ_DAY_OF_LOSS)
	TO_CHAR(V_IPFCGQ_DAY_OF_LOSS
	) AS V_LOSS_DAY,
	-- *INF*: IIF ( LENGTH(V_LOSS_MONTH) = 1, '0' || V_LOSS_MONTH, V_LOSS_MONTH)
	-- ||  
	-- IIF ( LENGTH(V_LOSS_DAY ) = 1, '0' || V_LOSS_DAY, V_LOSS_DAY )
	-- ||  
	-- V_LOSS_YEAR
	IFF(LENGTH(V_LOSS_MONTH
		) = 1,
		'0' || V_LOSS_MONTH,
		V_LOSS_MONTH
	) || IFF(LENGTH(V_LOSS_DAY
		) = 1,
		'0' || V_LOSS_DAY,
		V_LOSS_DAY
	) || V_LOSS_YEAR AS V_LOSS_DATE,
	-- *INF*: V_PIF_SYMBOL || V_PIF_POLICY_NUMBER || V_PIF_MODULE || V_LOSS_DATE || TO_CHAR(V_IPFCGQ_LOSS_OCCURENCE)
	V_PIF_SYMBOL || V_PIF_POLICY_NUMBER || V_PIF_MODULE || V_LOSS_DATE || TO_CHAR(V_IPFCGQ_LOSS_OCCURENCE
	) AS V_OCCURRENCE_KEY,
	V_OCCURRENCE_KEY AS CLAIM_OCCURRENCE_KEY,
	'CMT' AS V_PARTY_ROLE_CODE,
	-- *INF*: V_OCCURRENCE_KEY||TO_CHAR(V_IPFCGQ_LOSS_CLAIMANT)||V_PARTY_ROLE_CODE
	V_OCCURRENCE_KEY || TO_CHAR(V_IPFCGQ_LOSS_CLAIMANT
	) || V_PARTY_ROLE_CODE AS V_LOSS_PARTY_KEY,
	V_LOSS_PARTY_KEY AS CLAIM_PARTY_KEY,
	-- *INF*: ---:LKP.LKP_CLAIM_OCCURRENCE(V_OCCURRENCE_KEY)
	'' AS V_CLAIM_OCCURRENCE_AK_ID,
	-- *INF*: ----:LKP.LKP_CLAIM_PARTY(V_LOSS_PARTY_KEY)
	'' AS V_CLAIM_PARTY_AK_ID,
	-- *INF*: -----:LKP.LKP_CLAIM_PARTY_OCCURRENCE(V_CLAIM_OCCURRENCE_AK_ID,V_CLAIM_PARTY_AK_ID,V_PARTY_ROLE_CODE)
	'' AS V_CLAIM_PARTY_OCCURRENCE_AK_ID,
	V_CLAIM_PARTY_OCCURRENCE_AK_ID AS CLAIM_PARTY_OCCURRENCE_AK_ID,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_IPFCGQ_MARITAL_STATUS))),'N/A',IIF(IS_SPACES(IN_IPFCGQ_MARITAL_STATUS),'N/A',LTRIM(RTRIM(IN_IPFCGQ_MARITAL_STATUS))))
	IFF(LTRIM(RTRIM(IN_IPFCGQ_MARITAL_STATUS
			)
		) IS NULL,
		'N/A',
		IFF(LENGTH(IN_IPFCGQ_MARITAL_STATUS)>0 AND TRIM(IN_IPFCGQ_MARITAL_STATUS)='',
			'N/A',
			LTRIM(RTRIM(IN_IPFCGQ_MARITAL_STATUS
				)
			)
		)
	) AS IPFCGQ_MARITAL_STATUS,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_IPFCGQ_LOSS_AIA_CODES_1_2))),'N/A',IIF(IS_SPACES(IN_IPFCGQ_LOSS_AIA_CODES_1_2),'N/A',LTRIM(RTRIM(IN_IPFCGQ_LOSS_AIA_CODES_1_2))))
	IFF(LTRIM(RTRIM(IN_IPFCGQ_LOSS_AIA_CODES_1_2
			)
		) IS NULL,
		'N/A',
		IFF(LENGTH(IN_IPFCGQ_LOSS_AIA_CODES_1_2)>0 AND TRIM(IN_IPFCGQ_LOSS_AIA_CODES_1_2)='',
			'N/A',
			LTRIM(RTRIM(IN_IPFCGQ_LOSS_AIA_CODES_1_2
				)
			)
		)
	) AS IPFCGQ_LOSS_AIA_CODES_1_2,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_IPFCGQ_LOSS_AIA_CODES_3_4))),'N/A',IIF(IS_SPACES(IN_IPFCGQ_LOSS_AIA_CODES_3_4),'N/A',LTRIM(RTRIM(IN_IPFCGQ_LOSS_AIA_CODES_3_4))))
	IFF(LTRIM(RTRIM(IN_IPFCGQ_LOSS_AIA_CODES_3_4
			)
		) IS NULL,
		'N/A',
		IFF(LENGTH(IN_IPFCGQ_LOSS_AIA_CODES_3_4)>0 AND TRIM(IN_IPFCGQ_LOSS_AIA_CODES_3_4)='',
			'N/A',
			LTRIM(RTRIM(IN_IPFCGQ_LOSS_AIA_CODES_3_4
				)
			)
		)
	) AS IPFCGQ_LOSS_AIA_CODES_3_4,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_IPFCGQ_LOSS_AIA_CODES_5_6))),'N/A',IIF(IS_SPACES(IN_IPFCGQ_LOSS_AIA_CODES_5_6),'N/A',LTRIM(RTRIM(IN_IPFCGQ_LOSS_AIA_CODES_5_6))))
	IFF(LTRIM(RTRIM(IN_IPFCGQ_LOSS_AIA_CODES_5_6
			)
		) IS NULL,
		'N/A',
		IFF(LENGTH(IN_IPFCGQ_LOSS_AIA_CODES_5_6)>0 AND TRIM(IN_IPFCGQ_LOSS_AIA_CODES_5_6)='',
			'N/A',
			LTRIM(RTRIM(IN_IPFCGQ_LOSS_AIA_CODES_5_6
				)
			)
		)
	) AS IPFCGQ_LOSS_AIA_CODES_5_6,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_IPFCGQ_DEATH_INDICATOR))),'N/A',IIF(IS_SPACES(IN_IPFCGQ_DEATH_INDICATOR),'N/A',LTRIM(RTRIM(IN_IPFCGQ_DEATH_INDICATOR))))
	IFF(LTRIM(RTRIM(IN_IPFCGQ_DEATH_INDICATOR
			)
		) IS NULL,
		'N/A',
		IFF(LENGTH(IN_IPFCGQ_DEATH_INDICATOR)>0 AND TRIM(IN_IPFCGQ_DEATH_INDICATOR)='',
			'N/A',
			LTRIM(RTRIM(IN_IPFCGQ_DEATH_INDICATOR
				)
			)
		)
	) AS IPFCGQ_DEATH_INDICATOR,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_IPFCGQ_LOSS_COV_CODE_2))),'N/A',IIF(IS_SPACES(IN_IPFCGQ_LOSS_COV_CODE_2),'N/A',LTRIM(RTRIM(IN_IPFCGQ_LOSS_COV_CODE_2))))
	IFF(LTRIM(RTRIM(IN_IPFCGQ_LOSS_COV_CODE_2
			)
		) IS NULL,
		'N/A',
		IFF(LENGTH(IN_IPFCGQ_LOSS_COV_CODE_2)>0 AND TRIM(IN_IPFCGQ_LOSS_COV_CODE_2)='',
			'N/A',
			LTRIM(RTRIM(IN_IPFCGQ_LOSS_COV_CODE_2
				)
			)
		)
	) AS IPFCGQ_LOSS_COV_CODE_2,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_IPFCGQ_TYPE_DISABILITY))),'N/A',IIF(IS_SPACES(IN_IPFCGQ_TYPE_DISABILITY),'N/A',LTRIM(RTRIM(IN_IPFCGQ_TYPE_DISABILITY))))
	IFF(LTRIM(RTRIM(IN_IPFCGQ_TYPE_DISABILITY
			)
		) IS NULL,
		'N/A',
		IFF(LENGTH(IN_IPFCGQ_TYPE_DISABILITY)>0 AND TRIM(IN_IPFCGQ_TYPE_DISABILITY)='',
			'N/A',
			LTRIM(RTRIM(IN_IPFCGQ_TYPE_DISABILITY
				)
			)
		)
	) AS IPFCGQ_TYPE_DISABILITY,
	logical_flag,
	SOURCE_SYSTEM_ID
	FROM FIL_workers_comp_claimant_detail_PMS
),
LKP_42GQ_MS3 AS (
	SELECT
	IPFCGQ_NUMBER_DEPENDENTS,
	IPFCGQ_DATE_RPTD_EMPLOYER,
	IPFCGQ_SURGERY,
	IPFCGQ_ATTORNEY_AUTH_REP,
	IPFCGQ_CONTROVERTED_CASE,
	IPFCGQ_DATE_OF_DISABILITY,
	IPFCGQ_JURISDICTION_STATE,
	IPFCGQ_DATE_HIRE,
	PIF_SYMBOL,
	PIF_POLICY_NUMBER,
	PIF_MODULE,
	IPFCGQ_YEAR_OF_LOSS,
	IPFCGQ_MONTH_OF_LOSS,
	IPFCGQ_DAY_OF_LOSS,
	IPFCGQ_LOSS_OCCURENCE,
	IPFCGQ_LOSS_CLAIMANT
	FROM (
		SELECT 
			IPFCGQ_NUMBER_DEPENDENTS,
			IPFCGQ_DATE_RPTD_EMPLOYER,
			IPFCGQ_SURGERY,
			IPFCGQ_ATTORNEY_AUTH_REP,
			IPFCGQ_CONTROVERTED_CASE,
			IPFCGQ_DATE_OF_DISABILITY,
			IPFCGQ_JURISDICTION_STATE,
			IPFCGQ_DATE_HIRE,
			PIF_SYMBOL,
			PIF_POLICY_NUMBER,
			PIF_MODULE,
			IPFCGQ_YEAR_OF_LOSS,
			IPFCGQ_MONTH_OF_LOSS,
			IPFCGQ_DAY_OF_LOSS,
			IPFCGQ_LOSS_OCCURENCE,
			IPFCGQ_LOSS_CLAIMANT
		FROM PIF_42GQ_MS3_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PIF_SYMBOL,PIF_POLICY_NUMBER,PIF_MODULE,IPFCGQ_YEAR_OF_LOSS,IPFCGQ_MONTH_OF_LOSS,IPFCGQ_DAY_OF_LOSS,IPFCGQ_LOSS_OCCURENCE,IPFCGQ_LOSS_CLAIMANT ORDER BY IPFCGQ_NUMBER_DEPENDENTS) = 1
),
LKP_42GQ_WC1 AS (
	SELECT
	IPFCGQ_EMPLOYMENT_STATUS,
	IPFCGQ_LAST_DAY_WORKED,
	IPFCGQ_RETURN_TO_WORK_DATE,
	IPFCGQ_EMPLOYEE_DEATH_DATE,
	IPFCGQ_HOSPITAL_COSTS,
	IPFCGQ_DOCTOR_COSTS,
	IPFCGQ_OTHER_MED_COSTS,
	PIF_SYMBOL,
	PIF_POLICY_NUMBER,
	PIF_MODULE,
	IPFCGQ_YEAR_OF_LOSS,
	IPFCGQ_MONTH_OF_LOSS,
	IPFCGQ_DAY_OF_LOSS,
	IPFCGQ_LOSS_OCCURENCE,
	IPFCGQ_LOSS_CLAIMANT
	FROM (
		SELECT 
			IPFCGQ_EMPLOYMENT_STATUS,
			IPFCGQ_LAST_DAY_WORKED,
			IPFCGQ_RETURN_TO_WORK_DATE,
			IPFCGQ_EMPLOYEE_DEATH_DATE,
			IPFCGQ_HOSPITAL_COSTS,
			IPFCGQ_DOCTOR_COSTS,
			IPFCGQ_OTHER_MED_COSTS,
			PIF_SYMBOL,
			PIF_POLICY_NUMBER,
			PIF_MODULE,
			IPFCGQ_YEAR_OF_LOSS,
			IPFCGQ_MONTH_OF_LOSS,
			IPFCGQ_DAY_OF_LOSS,
			IPFCGQ_LOSS_OCCURENCE,
			IPFCGQ_LOSS_CLAIMANT
		FROM PIF_42GQ_WC1_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PIF_SYMBOL,PIF_POLICY_NUMBER,PIF_MODULE,IPFCGQ_YEAR_OF_LOSS,IPFCGQ_MONTH_OF_LOSS,IPFCGQ_DAY_OF_LOSS,IPFCGQ_LOSS_OCCURENCE,IPFCGQ_LOSS_CLAIMANT ORDER BY IPFCGQ_EMPLOYMENT_STATUS) = 1
),
LKP_42GQ_WC2 AS (
	SELECT
	IPFCGQ_SUBROGATION_CODE,
	IPFCGQ_WAGE_METHOD,
	IPFCGQ_PRE_INJ_WAGE,
	PIF_SYMBOL,
	PIF_POLICY_NUMBER,
	PIF_MODULE,
	IPFCGQ_YEAR_OF_LOSS,
	IPFCGQ_MONTH_OF_LOSS,
	IPFCGQ_DAY_OF_LOSS,
	IPFCGQ_LOSS_OCCURENCE,
	IPFCGQ_LOSS_CLAIMANT
	FROM (
		SELECT 
			IPFCGQ_SUBROGATION_CODE,
			IPFCGQ_WAGE_METHOD,
			IPFCGQ_PRE_INJ_WAGE,
			PIF_SYMBOL,
			PIF_POLICY_NUMBER,
			PIF_MODULE,
			IPFCGQ_YEAR_OF_LOSS,
			IPFCGQ_MONTH_OF_LOSS,
			IPFCGQ_DAY_OF_LOSS,
			IPFCGQ_LOSS_OCCURENCE,
			IPFCGQ_LOSS_CLAIMANT
		FROM PIF_42GQ_WC2_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PIF_SYMBOL,PIF_POLICY_NUMBER,PIF_MODULE,IPFCGQ_YEAR_OF_LOSS,IPFCGQ_MONTH_OF_LOSS,IPFCGQ_DAY_OF_LOSS,IPFCGQ_LOSS_OCCURENCE,IPFCGQ_LOSS_CLAIMANT ORDER BY IPFCGQ_SUBROGATION_CODE) = 1
),
LKP_42GQ_WC3 AS (
	SELECT
	IPFCGQ_MANGD_CARE_ORG_IND,
	IPFCGQ_TYPE_COVG,
	IPFCGQ_TYPE_SETL,
	PIF_SYMBOL,
	PIF_POLICY_NUMBER,
	PIF_MODULE,
	IPFCGQ_YEAR_OF_LOSS,
	IPFCGQ_MONTH_OF_LOSS,
	IPFCGQ_DAY_OF_LOSS,
	IPFCGQ_LOSS_OCCURENCE,
	IPFCGQ_LOSS_CLAIMANT
	FROM (
		SELECT 
			IPFCGQ_MANGD_CARE_ORG_IND,
			IPFCGQ_TYPE_COVG,
			IPFCGQ_TYPE_SETL,
			PIF_SYMBOL,
			PIF_POLICY_NUMBER,
			PIF_MODULE,
			IPFCGQ_YEAR_OF_LOSS,
			IPFCGQ_MONTH_OF_LOSS,
			IPFCGQ_DAY_OF_LOSS,
			IPFCGQ_LOSS_OCCURENCE,
			IPFCGQ_LOSS_CLAIMANT
		FROM PIF_42GQ_WC3_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PIF_SYMBOL,PIF_POLICY_NUMBER,PIF_MODULE,IPFCGQ_YEAR_OF_LOSS,IPFCGQ_MONTH_OF_LOSS,IPFCGQ_DAY_OF_LOSS,IPFCGQ_LOSS_OCCURENCE,IPFCGQ_LOSS_CLAIMANT ORDER BY IPFCGQ_MANGD_CARE_ORG_IND) = 1
),
LKP_Claim_Party_Occurrence_AK_ID AS (
	SELECT
	claim_party_occurrence_ak_id,
	claimant_num,
	claim_party_role_code
	FROM (
		SELECT CPO.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, 
		CO.claim_occurrence_type_code as offset_onset_ind,
		LTRIM(RTRIM(CO.claim_occurrence_key)) as claimant_num, 
		LTRIM(RTRIM(CP.claim_party_key)) as claim_party_role_code
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence CPO,
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party CP, 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence CO 
		WHERE CO.claim_occurrence_ak_id = CPO.claim_occurrence_ak_id  AND CP.claim_party_ak_id = CPO.claim_party_ak_id 
		AND CO.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND CP.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND CPO.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND CPO.claim_party_role_code = 'CMT'
		AND CO.crrnt_snpsht_flag = 1
		AND CP.crrnt_snpsht_flag = 1
		AND CPO.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_num,claim_party_role_code ORDER BY claim_party_occurrence_ak_id) = 1
),
LKP_workers_comp_claimant_detail AS (
	SELECT
	wc_claimant_det_id,
	wc_claimant_det_ak_id,
	claim_party_occurrence_ak_id,
	jurisdiction_state_code,
	emplyr_notified_date,
	reported_to_carrier_date,
	jurisdiction_claim_num,
	care_directed_ind,
	care_directed_by,
	hired_state_code,
	hired_date,
	tax_filing_status,
	occuptn_code,
	emplymnt_status_code,
	len_of_time_in_crrnt_job,
	emp_dept_name,
	emp_shift_num,
	marital_status,
	num_of_dependents,
	num_of_dependent_children,
	num_of_other_dependents,
	num_of_exemptions,
	exemption_type,
	emp_blind_ind,
	emp_over_65_ind,
	spouse_blind_ind,
	spouse_over_65_ind,
	education_lvl,
	med_auth_ind,
	auth_to_release_ssn_ind,
	emp_id_num,
	emp_id_type,
	emp_part_time_hour_week,
	emp_dept_num,
	emp_part_time_hourly_week_rate_amt,
	wage_rate_amt,
	wage_period_code,
	wage_eff_date,
	weeks_worked,
	gross_amt_type,
	gross_wage_amt_excluding_tips,
	piece_work_num_of_weeks_excluding_overtime,
	emp_rec_meals,
	emp_rec_room,
	emp_rec_tips,
	overtime_amt,
	overtime_after_hour_in_a_week,
	overtime_after_hour_in_a_day,
	full_pay_inj_day_ind,
	salary_paid_ind,
	avg_full_time_days_week,
	avg_full_time_hours_day,
	avg_full_time_hours_week,
	avg_wkly_wage,
	num_of_full_time_emplymnt_same_job,
	num_of_part_time_emplymnt_same_job,
	ttd_rate,
	ppd_rate,
	ptd_rate,
	dtd_rate,
	wkly_attorney_fee,
	first_rpt_inj_date,
	supplementary_rpt_inj_date,
	fringe_bnft_discontinued_amt,
	emp_start_time,
	emp_hour_day,
	emp_hour_week,
	emp_day_week,
	inj_work_day_begin_time,
	disability_date,
	phys_restriction_ind,
	pre_exst_disability_ind,
	premises_code,
	work_process_descript,
	task_descript,
	body_part_code,
	nature_inj_code,
	cause_inj_code,
	safeguard_not_used_ind,
	inj_substance_abuse_ind,
	sfty_device_not_used_ind,
	inj_rules_not_obeyed_ind,
	inj_result_occupational_inj_ind,
	inj_result_occupational_disease_ind,
	inj_result_death_ind,
	unsafe_act_descript,
	responsible_for_inj_descript,
	hazard_condition_descript,
	death_date,
	emplyr_nature_bus_descript,
	emplyr_type_code,
	insd_type_code,
	subrogation_statute_exp_date,
	managed_care_org_type,
	subrogation_code,
	loss_condition,
	attorney_or_au_rep_ind,
	hospital_cost,
	doctor_cost,
	other_med_cost,
	controverted_case_code,
	surgery_ind,
	emplyr_loc_descript,
	inj_loc_comment,
	claim_ctgry_code,
	act_status_code,
	investigate_ind,
	sic_code,
	hospitalized_ind,
	wage_method_code,
	pms_occuptn_descript,
	pms_type_disability,
	ncci_type_cov,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date
	FROM (
		SELECT workers_comp_claimant_detail.wc_claimant_det_id as wc_claimant_det_id, workers_comp_claimant_detail.wc_claimant_det_ak_id as wc_claimant_det_ak_id, workers_comp_claimant_detail.jurisdiction_state_code as jurisdiction_state_code, workers_comp_claimant_detail.emplyr_notified_date as emplyr_notified_date, workers_comp_claimant_detail.reported_to_carrier_date as reported_to_carrier_date, workers_comp_claimant_detail.jurisdiction_claim_num as jurisdiction_claim_num, workers_comp_claimant_detail.care_directed_ind as care_directed_ind, workers_comp_claimant_detail.care_directed_by as care_directed_by, workers_comp_claimant_detail.hired_state_code as hired_state_code, workers_comp_claimant_detail.hired_date as hired_date, workers_comp_claimant_detail.tax_filing_status as tax_filing_status, workers_comp_claimant_detail.occuptn_code as occuptn_code, workers_comp_claimant_detail.emplymnt_status_code as emplymnt_status_code, workers_comp_claimant_detail.len_of_time_in_crrnt_job as len_of_time_in_crrnt_job, workers_comp_claimant_detail.emp_dept_name as emp_dept_name, workers_comp_claimant_detail.emp_shift_num as emp_shift_num, workers_comp_claimant_detail.marital_status as marital_status, workers_comp_claimant_detail.num_of_dependents as num_of_dependents, workers_comp_claimant_detail.num_of_dependent_children as num_of_dependent_children, workers_comp_claimant_detail.num_of_other_dependents as num_of_other_dependents, workers_comp_claimant_detail.num_of_exemptions as num_of_exemptions, workers_comp_claimant_detail.exemption_type as exemption_type, workers_comp_claimant_detail.emp_blind_ind as emp_blind_ind, workers_comp_claimant_detail.emp_over_65_ind as emp_over_65_ind, workers_comp_claimant_detail.spouse_blind_ind as spouse_blind_ind, workers_comp_claimant_detail.spouse_over_65_ind as spouse_over_65_ind, workers_comp_claimant_detail.education_lvl as education_lvl, workers_comp_claimant_detail.med_auth_ind as med_auth_ind, workers_comp_claimant_detail.auth_to_release_ssn_ind as auth_to_release_ssn_ind, workers_comp_claimant_detail.emp_id_num as emp_id_num, workers_comp_claimant_detail.emp_id_type as emp_id_type, workers_comp_claimant_detail.emp_part_time_hour_week as emp_part_time_hour_week, workers_comp_claimant_detail.emp_dept_num as emp_dept_num, workers_comp_claimant_detail.emp_part_time_hourly_week_rate_amt as emp_part_time_hourly_week_rate_amt, workers_comp_claimant_detail.wage_rate_amt as wage_rate_amt, workers_comp_claimant_detail.wage_period_code as wage_period_code, workers_comp_claimant_detail.wage_eff_date as wage_eff_date, workers_comp_claimant_detail.weeks_worked as weeks_worked, workers_comp_claimant_detail.gross_amt_type as gross_amt_type, workers_comp_claimant_detail.gross_wage_amt_excluding_tips as gross_wage_amt_excluding_tips, workers_comp_claimant_detail.piece_work_num_of_weeks_excluding_overtime as piece_work_num_of_weeks_excluding_overtime, workers_comp_claimant_detail.emp_rec_meals as emp_rec_meals, workers_comp_claimant_detail.emp_rec_room as emp_rec_room, workers_comp_claimant_detail.emp_rec_tips as emp_rec_tips, workers_comp_claimant_detail.overtime_amt as overtime_amt, workers_comp_claimant_detail.overtime_after_hour_in_a_week as overtime_after_hour_in_a_week, workers_comp_claimant_detail.overtime_after_hour_in_a_day as overtime_after_hour_in_a_day, workers_comp_claimant_detail.full_pay_inj_day_ind as full_pay_inj_day_ind, workers_comp_claimant_detail.salary_paid_ind as salary_paid_ind, workers_comp_claimant_detail.avg_full_time_days_week as avg_full_time_days_week, workers_comp_claimant_detail.avg_full_time_hours_day as avg_full_time_hours_day, workers_comp_claimant_detail.avg_full_time_hours_week as avg_full_time_hours_week, workers_comp_claimant_detail.avg_wkly_wage as avg_wkly_wage, workers_comp_claimant_detail.num_of_full_time_emplymnt_same_job as num_of_full_time_emplymnt_same_job, workers_comp_claimant_detail.num_of_part_time_emplymnt_same_job as num_of_part_time_emplymnt_same_job, workers_comp_claimant_detail.ttd_rate as ttd_rate, workers_comp_claimant_detail.ppd_rate as ppd_rate, workers_comp_claimant_detail.ptd_rate as ptd_rate, workers_comp_claimant_detail.dtd_rate as dtd_rate, workers_comp_claimant_detail.wkly_attorney_fee as wkly_attorney_fee, workers_comp_claimant_detail.first_rpt_inj_date as first_rpt_inj_date, workers_comp_claimant_detail.supplementary_rpt_inj_date as supplementary_rpt_inj_date, workers_comp_claimant_detail.fringe_bnft_discontinued_amt as fringe_bnft_discontinued_amt, workers_comp_claimant_detail.emp_start_time as emp_start_time, workers_comp_claimant_detail.emp_hour_day as emp_hour_day, workers_comp_claimant_detail.emp_hour_week as emp_hour_week, workers_comp_claimant_detail.emp_day_week as emp_day_week, workers_comp_claimant_detail.inj_work_day_begin_time as inj_work_day_begin_time, workers_comp_claimant_detail.disability_date as disability_date, workers_comp_claimant_detail.phys_restriction_ind as phys_restriction_ind, workers_comp_claimant_detail.pre_exst_disability_ind as pre_exst_disability_ind, workers_comp_claimant_detail.premises_code as premises_code, workers_comp_claimant_detail.work_process_descript as work_process_descript, workers_comp_claimant_detail.task_descript as task_descript, workers_comp_claimant_detail.body_part_code as body_part_code, workers_comp_claimant_detail.nature_inj_code as nature_inj_code, workers_comp_claimant_detail.cause_inj_code as cause_inj_code, workers_comp_claimant_detail.safeguard_not_used_ind as safeguard_not_used_ind, workers_comp_claimant_detail.inj_substance_abuse_ind as inj_substance_abuse_ind, workers_comp_claimant_detail.sfty_device_not_used_ind as sfty_device_not_used_ind, workers_comp_claimant_detail.inj_rules_not_obeyed_ind as inj_rules_not_obeyed_ind, workers_comp_claimant_detail.inj_result_occupational_inj_ind as inj_result_occupational_inj_ind, workers_comp_claimant_detail.inj_result_occupational_disease_ind as inj_result_occupational_disease_ind, workers_comp_claimant_detail.inj_result_death_ind as inj_result_death_ind, workers_comp_claimant_detail.unsafe_act_descript as unsafe_act_descript, workers_comp_claimant_detail.responsible_for_inj_descript as responsible_for_inj_descript, workers_comp_claimant_detail.hazard_condition_descript as hazard_condition_descript, workers_comp_claimant_detail.death_date as death_date,  
		workers_comp_claimant_detail.emplyr_nature_bus_descript as emplyr_nature_bus_descript, workers_comp_claimant_detail.emplyr_type_code as emplyr_type_code, workers_comp_claimant_detail.insd_type_code as insd_type_code, workers_comp_claimant_detail.subrogation_statute_exp_date as subrogation_statute_exp_date, workers_comp_claimant_detail.managed_care_org_type as managed_care_org_type, workers_comp_claimant_detail.subrogation_code as subrogation_code, workers_comp_claimant_detail.loss_condition as loss_condition, workers_comp_claimant_detail.attorney_or_au_rep_ind as attorney_or_au_rep_ind, workers_comp_claimant_detail.hospital_cost as hospital_cost, workers_comp_claimant_detail.doctor_cost as doctor_cost, workers_comp_claimant_detail.other_med_cost as other_med_cost, workers_comp_claimant_detail.controverted_case_code as controverted_case_code, workers_comp_claimant_detail.surgery_ind as surgery_ind, workers_comp_claimant_detail.emplyr_loc_descript as emplyr_loc_descript, workers_comp_claimant_detail.inj_loc_comment as inj_loc_comment, workers_comp_claimant_detail.claim_ctgry_code as claim_ctgry_code, workers_comp_claimant_detail.act_status_code as act_status_code, workers_comp_claimant_detail.investigate_ind as investigate_ind, workers_comp_claimant_detail.sic_code as sic_code, workers_comp_claimant_detail.hospitalized_ind as hospitalized_ind, workers_comp_claimant_detail.wage_method_code as wage_method_code, workers_comp_claimant_detail.pms_occuptn_descript as pms_occuptn_descript, workers_comp_claimant_detail.pms_type_disability as pms_type_disability, workers_comp_claimant_detail.ncci_type_cov as ncci_type_cov, workers_comp_claimant_detail.crrnt_snpsht_flag as crrnt_snpsht_flag, workers_comp_claimant_detail.audit_id as audit_id, workers_comp_claimant_detail.eff_from_date as eff_from_date, workers_comp_claimant_detail.eff_to_date as eff_to_date, workers_comp_claimant_detail.source_sys_id as source_sys_id, workers_comp_claimant_detail.created_date as created_date, workers_comp_claimant_detail.modified_date as modified_date, workers_comp_claimant_detail.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id FROM workers_comp_claimant_detail
		WHERE (source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}') AND (CRRNT_SNPSHT_FLAG='1')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id ORDER BY wc_claimant_det_id) = 1
),
EXP_DETECT_CHANGES_workers_comp_claimant_detail AS (
	SELECT
	EXP_Lkp_Values_workers_comp_claimant_detail_PMS.PIF_SYMBOL,
	EXP_Lkp_Values_workers_comp_claimant_detail_PMS.PIF_POLICY_NUMBER,
	EXP_Lkp_Values_workers_comp_claimant_detail_PMS.PIF_MODULE,
	EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_YEAR_OF_LOSS,
	EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_MONTH_OF_LOSS,
	EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_DAY_OF_LOSS,
	EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_LOSS_OCCURENCE,
	EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_LOSS_CLAIMANT,
	LKP_workers_comp_claimant_detail.wc_claimant_det_id AS LKP_wc_claimant_detail_id,
	LKP_workers_comp_claimant_detail.wc_claimant_det_ak_id AS LKP_wc_claimant_det_ak_id,
	LKP_workers_comp_claimant_detail.claim_party_occurrence_ak_id AS LKP_claim_party_occurrence_ak_id,
	LKP_workers_comp_claimant_detail.jurisdiction_state_code AS LKP_jurisdiction_state_code,
	LKP_workers_comp_claimant_detail.emplyr_notified_date AS LKP_emplyr_notified_date,
	LKP_workers_comp_claimant_detail.reported_to_carrier_date AS LKP_rpted_to_carrier_date,
	LKP_workers_comp_claimant_detail.jurisdiction_claim_num AS LKP_jurisdiction_claim_num,
	LKP_workers_comp_claimant_detail.care_directed_ind AS LKP_care_directed_ind,
	LKP_workers_comp_claimant_detail.care_directed_by AS LKP_care_directed_by,
	LKP_workers_comp_claimant_detail.hired_state_code AS LKP_hired_state_code,
	LKP_workers_comp_claimant_detail.hired_date AS LKP_hired_date,
	LKP_workers_comp_claimant_detail.tax_filing_status AS LKP_tax_filing_status,
	LKP_workers_comp_claimant_detail.occuptn_code AS LKP_occuptn_code,
	LKP_workers_comp_claimant_detail.emplymnt_status_code AS LKP_employement_status_code,
	LKP_workers_comp_claimant_detail.len_of_time_in_crrnt_job AS LKP_len_of_time_in_crrnt_job,
	LKP_workers_comp_claimant_detail.emp_dept_name AS LKP_emp_dept_name,
	LKP_workers_comp_claimant_detail.emp_shift_num AS LKP_emp_shift_num,
	LKP_workers_comp_claimant_detail.marital_status AS LKP_marital_status,
	LKP_workers_comp_claimant_detail.num_of_dependents AS LKP_num_of_dependents,
	LKP_workers_comp_claimant_detail.num_of_dependent_children AS LKP_num_of_dependent_children,
	LKP_workers_comp_claimant_detail.num_of_other_dependents AS LKP_num_of_other_dependents,
	LKP_workers_comp_claimant_detail.num_of_exemptions AS LKP_num_of_exemptions,
	LKP_workers_comp_claimant_detail.exemption_type AS LKP_exemption_type,
	LKP_workers_comp_claimant_detail.emp_blind_ind AS LKP_emp_blind_ind,
	LKP_workers_comp_claimant_detail.emp_over_65_ind AS LKP_emp_over_65_ind,
	LKP_workers_comp_claimant_detail.spouse_blind_ind AS LKP_spouse_blind_ind,
	LKP_workers_comp_claimant_detail.spouse_over_65_ind AS LKP_spouse_over_65_ind,
	LKP_workers_comp_claimant_detail.education_lvl AS LKP_education_lvl,
	LKP_workers_comp_claimant_detail.med_auth_ind AS LKP_med_auth_ind,
	LKP_workers_comp_claimant_detail.auth_to_release_ssn_ind AS LKP_auth_to_release_ssn_ind,
	LKP_workers_comp_claimant_detail.emp_id_num AS LKP_emp_id_num,
	LKP_workers_comp_claimant_detail.emp_id_type AS LKP_emp_id_type,
	LKP_workers_comp_claimant_detail.emp_part_time_hour_week AS LKP_emp_part_time_hour_week,
	LKP_workers_comp_claimant_detail.emp_dept_num AS LKP_emp_dept_num,
	LKP_workers_comp_claimant_detail.emp_part_time_hourly_week_rate_amt AS LKP_emp_part_time_hourly_week_rate_amt,
	LKP_workers_comp_claimant_detail.wage_rate_amt AS LKP_wage_rate_amt,
	LKP_workers_comp_claimant_detail.wage_period_code AS LKP_wage_period_code,
	LKP_workers_comp_claimant_detail.wage_eff_date AS LKP_wage_eff_date,
	LKP_workers_comp_claimant_detail.weeks_worked AS LKP_weeks_worked,
	LKP_workers_comp_claimant_detail.gross_amt_type AS LKP_gross_amt_type,
	LKP_workers_comp_claimant_detail.gross_wage_amt_excluding_tips AS LKP_gross_wage_amt_excluding_tips,
	LKP_workers_comp_claimant_detail.piece_work_num_of_weeks_excluding_overtime AS LKP_piece_work_num_of_weeks_excluding_overtime,
	LKP_workers_comp_claimant_detail.emp_rec_meals AS LKP_emp_rec_meals,
	LKP_workers_comp_claimant_detail.emp_rec_room AS LKP_emp_rec_room,
	LKP_workers_comp_claimant_detail.emp_rec_tips AS LKP_emp_rec_tips,
	LKP_workers_comp_claimant_detail.overtime_amt AS LKP_overtime_amt,
	LKP_workers_comp_claimant_detail.overtime_after_hour_in_a_week AS LKP_overtime_after_hour_in_a_week,
	LKP_workers_comp_claimant_detail.overtime_after_hour_in_a_day AS LKP_overtime_after_hour_in_a_day,
	LKP_workers_comp_claimant_detail.full_pay_inj_day_ind AS LKP_full_pay_inj_day_ind,
	LKP_workers_comp_claimant_detail.salary_paid_ind AS LKP_salary_paid_ind,
	LKP_workers_comp_claimant_detail.avg_full_time_days_week AS LKP_avg_full_time_days_week,
	LKP_workers_comp_claimant_detail.avg_full_time_hours_day AS LKP_avg_full_time_hours_day,
	LKP_workers_comp_claimant_detail.avg_full_time_hours_week AS LKP_avg_full_time_hours_week,
	LKP_workers_comp_claimant_detail.avg_wkly_wage AS LKP_avg_wkly_wage,
	LKP_workers_comp_claimant_detail.num_of_full_time_emplymnt_same_job AS LKP_num_of_full_time_emplymnt_same_job,
	LKP_workers_comp_claimant_detail.num_of_part_time_emplymnt_same_job AS LKP_num_of_part_time_emplymnt_same_job,
	LKP_workers_comp_claimant_detail.ttd_rate AS LKP_ttd_rate,
	LKP_workers_comp_claimant_detail.ppd_rate AS LKP_ppd_rate,
	LKP_workers_comp_claimant_detail.ptd_rate AS LKP_ptd_rate,
	LKP_workers_comp_claimant_detail.dtd_rate AS LKP_dtd_rate,
	LKP_workers_comp_claimant_detail.wkly_attorney_fee AS LKP_wkly_attorney_fee,
	LKP_workers_comp_claimant_detail.first_rpt_inj_date AS LKP_first_rpt_inj_date,
	LKP_workers_comp_claimant_detail.supplementary_rpt_inj_date AS LKP_supplementary_rpt_inj_date,
	LKP_workers_comp_claimant_detail.fringe_bnft_discontinued_amt AS LKP_fringe_bnft_discontinued_amt,
	LKP_workers_comp_claimant_detail.emp_start_time AS LKP_emp_start_time,
	LKP_workers_comp_claimant_detail.emp_hour_day AS LKP_emp_hour_day,
	LKP_workers_comp_claimant_detail.emp_hour_week AS LKP_emp_hour_week,
	LKP_workers_comp_claimant_detail.emp_day_week AS LKP_emp_day_week,
	LKP_workers_comp_claimant_detail.inj_work_day_begin_time AS LKP_inj_work_day_begin_time,
	LKP_workers_comp_claimant_detail.disability_date AS LKP_disability_date,
	LKP_workers_comp_claimant_detail.phys_restriction_ind AS LKP_phys_restriction_ind,
	LKP_workers_comp_claimant_detail.pre_exst_disability_ind AS LKP_pre_exst_disability_ind,
	LKP_workers_comp_claimant_detail.premises_code AS LKP_premises_code,
	LKP_workers_comp_claimant_detail.work_process_descript AS LKP_work_process_descript,
	LKP_workers_comp_claimant_detail.task_descript AS LKP_task_descript,
	LKP_workers_comp_claimant_detail.body_part_code AS LKP_body_part_code,
	LKP_workers_comp_claimant_detail.nature_inj_code AS LKP_nature_inj_code,
	LKP_workers_comp_claimant_detail.cause_inj_code AS LKP_cause_inj_code,
	LKP_workers_comp_claimant_detail.safeguard_not_used_ind AS LKP_safeguard_not_used_ind,
	LKP_workers_comp_claimant_detail.inj_substance_abuse_ind AS LKP_inj_substance_abuse_ind,
	LKP_workers_comp_claimant_detail.sfty_device_not_used_ind AS LKP_sfty_device_not_used_ind,
	LKP_workers_comp_claimant_detail.inj_rules_not_obeyed_ind AS LKP_inj_rules_not_obeyed_ind,
	LKP_workers_comp_claimant_detail.inj_result_occupational_inj_ind AS LKP_inj_result_occuptnal_inj_ind,
	LKP_workers_comp_claimant_detail.inj_result_occupational_disease_ind AS LKP_inj_result_occuptnal_disease_ndicator,
	LKP_workers_comp_claimant_detail.inj_result_death_ind AS LKP_inj_result_death_ind,
	LKP_workers_comp_claimant_detail.unsafe_act_descript AS LKP_unsafe_act_descript,
	LKP_workers_comp_claimant_detail.responsible_for_inj_descript AS LKP_responsible_for_inj_descript,
	LKP_workers_comp_claimant_detail.hazard_condition_descript AS LKP_hazard_condition_descript,
	LKP_workers_comp_claimant_detail.death_date AS LKP_death_date,
	LKP_workers_comp_claimant_detail.emplyr_nature_bus_descript AS LKP_emplyr_nature_bus_descript,
	LKP_workers_comp_claimant_detail.emplyr_type_code AS LKP_emplyr_type_code,
	LKP_workers_comp_claimant_detail.insd_type_code AS LKP_insd_type_code,
	LKP_workers_comp_claimant_detail.subrogation_statute_exp_date AS LKP_subrogation_statute_exp_date,
	LKP_workers_comp_claimant_detail.managed_care_org_type AS LKP_managed_care_org_type,
	LKP_workers_comp_claimant_detail.subrogation_code AS LKP_subrogation_code,
	LKP_workers_comp_claimant_detail.loss_condition AS LKP_loss_condition,
	LKP_workers_comp_claimant_detail.attorney_or_au_rep_ind AS LKP_attorney_or_au_rep_ind,
	LKP_workers_comp_claimant_detail.hospital_cost AS LKP_hospital_cost,
	LKP_workers_comp_claimant_detail.doctor_cost AS LKP_doctor_cost,
	LKP_workers_comp_claimant_detail.other_med_cost AS LKP_other_med_cost,
	LKP_workers_comp_claimant_detail.controverted_case_code AS LKP_controverted_case_code,
	LKP_workers_comp_claimant_detail.surgery_ind AS LKP_surgery_ind,
	LKP_workers_comp_claimant_detail.emplyr_loc_descript AS LKP_emplyr_loc_descript,
	LKP_workers_comp_claimant_detail.inj_loc_comment AS LKP_inj_loc_comment,
	LKP_workers_comp_claimant_detail.claim_ctgry_code AS LKP_claim_ctgry_code,
	LKP_workers_comp_claimant_detail.act_status_code AS LKP_act_status_code,
	LKP_workers_comp_claimant_detail.investigate_ind AS LKP_investigate_ind,
	LKP_workers_comp_claimant_detail.sic_code AS LKP_emplyr_standard_industry_code,
	LKP_workers_comp_claimant_detail.hospitalized_ind AS LKP_hospitalized_ind,
	LKP_workers_comp_claimant_detail.wage_method_code AS LKP_wage_method_code,
	LKP_workers_comp_claimant_detail.pms_occuptn_descript AS LKP_pms_occuptn_descript,
	LKP_workers_comp_claimant_detail.pms_type_disability AS LKP_pms_type_disability,
	LKP_workers_comp_claimant_detail.ncci_type_cov AS LKP_ncci_type_cov,
	LKP_workers_comp_claimant_detail.crrnt_snpsht_flag AS LKP_crrnt_snpsht_flag,
	LKP_workers_comp_claimant_detail.audit_id AS LKP_audit_id,
	LKP_workers_comp_claimant_detail.eff_from_date AS LKP_eff_from_date,
	LKP_workers_comp_claimant_detail.eff_to_date AS LKP_eff_to_date,
	LKP_workers_comp_claimant_detail.source_sys_id AS LKP_source_sys_id,
	LKP_workers_comp_claimant_detail.created_date AS LKP_created_date,
	LKP_workers_comp_claimant_detail.modified_date AS LKP_modified_date,
	LKP_42GQ_MS3.IPFCGQ_JURISDICTION_STATE,
	-- *INF*: :LKP.LKP_PIF_42GP(PIF_SYMBOL,PIF_POLICY_NUMBER,PIF_MODULE,IPFCGQ_YEAR_OF_LOSS,IPFCGQ_MONTH_OF_LOSS,IPFCGQ_DAY_OF_LOSS,IPFCGQ_LOSS_OCCURENCE)
	LKP_PIF_42GP_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFCGQ_YEAR_OF_LOSS_IPFCGQ_MONTH_OF_LOSS_IPFCGQ_DAY_OF_LOSS_IPFCGQ_LOSS_OCCURENCE.ipfcgp_loss_accident_state AS V_Loss_Accident_State,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IPFCGQ_JURISDICTION_STATE))) OR 
	-- 	IS_SPACES(LTRIM(RTRIM(IPFCGQ_JURISDICTION_STATE))) OR
	-- 	LENGTH(LTRIM(RTRIM(IPFCGQ_JURISDICTION_STATE)))=0,
	-- LTRIM(RTRIM(V_Loss_Accident_State)), LTRIM(RTRIM(IPFCGQ_JURISDICTION_STATE))
	-- )
	-- 
	-- 
	-- --IIF(ISNULL(LTRIM(RTRIM(IPFCGQ_JURISDICTION_STATE))),'N/A',IIF(IS_SPACES(IPFCGQ_JURISDICTION_STATE),'N/A',
	-- --:LKP.LKP_Sup_State(LTRIM(RTRIM(IPFCGQ_JURISDICTION_STATE)))))
	-- 
	-- --get the alpha state code from numeric
	IFF(LTRIM(RTRIM(IPFCGQ_JURISDICTION_STATE
			)
		) IS NULL 
		OR LENGTH(LTRIM(RTRIM(IPFCGQ_JURISDICTION_STATE
			)
		))>0 AND TRIM(LTRIM(RTRIM(IPFCGQ_JURISDICTION_STATE
			)
		))='' 
		OR LENGTH(LTRIM(RTRIM(IPFCGQ_JURISDICTION_STATE
				)
			)
		) = 0,
		LTRIM(RTRIM(V_Loss_Accident_State
			)
		),
		LTRIM(RTRIM(IPFCGQ_JURISDICTION_STATE
			)
		)
	) AS V_jurisdiction_state_code,
	-- *INF*: IIF(ISNULL(V_jurisdiction_state_code) OR IS_SPACES(V_jurisdiction_state_code) OR LENGTH(V_jurisdiction_state_code) =0,'N/A',
	-- :LKP.LKP_Sup_State(LTRIM(RTRIM(V_jurisdiction_state_code)))
	-- )
	-- 
	-- --get the alpha state code from numeric
	IFF(V_jurisdiction_state_code IS NULL 
		OR LENGTH(V_jurisdiction_state_code)>0 AND TRIM(V_jurisdiction_state_code)='' 
		OR LENGTH(V_jurisdiction_state_code
		) = 0,
		'N/A',
		LKP_SUP_STATE_LTRIM_RTRIM_V_jurisdiction_state_code.state_code
	) AS V_jurisdiction_state_code_Actual,
	V_jurisdiction_state_code_Actual AS jurisdiction_state_code_OP,
	LKP_42GQ_MS3.IPFCGQ_DATE_RPTD_EMPLOYER,
	-- *INF*: IIF(ISNULL(IPFCGQ_DATE_RPTD_EMPLOYER) OR IPFCGQ_DATE_RPTD_EMPLOYER=0,TO_DATE('1800/01/01','YYYY/MM/DD'),TO_DATE(TO_CHAR(IPFCGQ_DATE_RPTD_EMPLOYER),'YYYYMMDD'))
	IFF(IPFCGQ_DATE_RPTD_EMPLOYER IS NULL 
		OR IPFCGQ_DATE_RPTD_EMPLOYER = 0,
		TO_DATE('1800/01/01', 'YYYY/MM/DD'
		),
		TO_DATE(TO_CHAR(IPFCGQ_DATE_RPTD_EMPLOYER
			), 'YYYYMMDD'
		)
	) AS V_emplyr_notified_date,
	V_emplyr_notified_date AS emplyr_notified_date,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
	) AS V_rpted_to_carrier_date,
	V_rpted_to_carrier_date AS rpted_to_carrier_date,
	'N/A' AS V_jurisdiction_claim_num,
	V_jurisdiction_claim_num AS jurisdiction_claim_num,
	'N/A' AS V_care_directed_ind,
	V_care_directed_ind AS care_directed_ind,
	'N/A' AS V_care_directed_by,
	V_care_directed_by AS care_directed_by,
	'N/A' AS V_hired_state_code,
	V_hired_state_code AS hired_state_code,
	LKP_42GQ_MS3.IPFCGQ_DATE_HIRE,
	-- *INF*: IIF(ISNULL(IPFCGQ_DATE_HIRE) OR IPFCGQ_DATE_HIRE=0 OR LENGTH (TO_CHAR(IPFCGQ_DATE_HIRE)) < 8 ,TO_DATE('1800/01/01','YYYY/MM/DD'),TO_DATE(TO_CHAR(IPFCGQ_DATE_HIRE),'YYYYMMDD'))
	IFF(IPFCGQ_DATE_HIRE IS NULL 
		OR IPFCGQ_DATE_HIRE = 0 
		OR LENGTH(TO_CHAR(IPFCGQ_DATE_HIRE
			)
		) < 8,
		TO_DATE('1800/01/01', 'YYYY/MM/DD'
		),
		TO_DATE(TO_CHAR(IPFCGQ_DATE_HIRE
			), 'YYYYMMDD'
		)
	) AS V_hired_date,
	V_hired_date AS hired_date,
	'N/A' AS V_tax_filing_status,
	V_tax_filing_status AS tax_filing_status,
	'N/A' AS V_occuptn_code,
	V_occuptn_code AS occuptn_code,
	LKP_42GQ_WC1.IPFCGQ_EMPLOYMENT_STATUS,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IPFCGQ_EMPLOYMENT_STATUS))),'N/A',IIF(IS_SPACES(IPFCGQ_EMPLOYMENT_STATUS),'N/A',LTRIM(RTRIM(IPFCGQ_EMPLOYMENT_STATUS))))
	IFF(LTRIM(RTRIM(IPFCGQ_EMPLOYMENT_STATUS
			)
		) IS NULL,
		'N/A',
		IFF(LENGTH(IPFCGQ_EMPLOYMENT_STATUS)>0 AND TRIM(IPFCGQ_EMPLOYMENT_STATUS)='',
			'N/A',
			LTRIM(RTRIM(IPFCGQ_EMPLOYMENT_STATUS
				)
			)
		)
	) AS V_employement_status_code,
	V_employement_status_code AS employement_status_code,
	'N/A' AS V_len_of_time_in_crrnt_job,
	V_len_of_time_in_crrnt_job AS len_of_time_in_crrnt_job,
	'N/A' AS V_emp_dept_name,
	V_emp_dept_name AS emp_dept_name,
	'N/A' AS V_emp_shift_num,
	V_emp_shift_num AS emp_shift_num,
	EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_MARITAL_STATUS AS marital_status,
	LKP_42GQ_MS3.IPFCGQ_NUMBER_DEPENDENTS,
	-- *INF*: IIF(ISNULL(IPFCGQ_NUMBER_DEPENDENTS),0,IPFCGQ_NUMBER_DEPENDENTS)
	IFF(IPFCGQ_NUMBER_DEPENDENTS IS NULL,
		0,
		IPFCGQ_NUMBER_DEPENDENTS
	) AS V_num_of_dependents,
	V_num_of_dependents AS num_of_dependents,
	0 AS V_num_of_dependent_children,
	V_num_of_dependent_children AS num_of_dependent_children,
	0 AS V_num_of_other_dependents,
	V_num_of_other_dependents AS num_of_other_dependents,
	0 AS V_num_of_exemptions,
	V_num_of_exemptions AS num_of_exemptions,
	'N/A' AS V_exemption_type,
	V_exemption_type AS exemption_type,
	'N/A' AS V_emp_blind_ind,
	V_emp_blind_ind AS emp_blind_ind,
	'N/A' AS V_emp_over_65_ind,
	V_emp_over_65_ind AS emp_over_65_ind,
	'N/A' AS V_spouse_blind_ind,
	V_spouse_blind_ind AS spouse_blind_ind,
	'N/A' AS V_spouse_over_65_ind,
	V_spouse_over_65_ind AS spouse_over_65_ind,
	'N/A' AS V_education_lvl,
	V_education_lvl AS education_lvl,
	'N/A' AS V_med_auth_ind,
	V_med_auth_ind AS med_auth_ind,
	'N/A' AS V_auth_to_release_ssn_ind,
	V_auth_to_release_ssn_ind AS auth_to_release_ssn_ind,
	'N/A' AS V_emp_id_num,
	V_emp_id_num AS emp_id_num,
	'N/A' AS V_emp_id_type,
	V_emp_id_type AS emp_id_type,
	0 AS V_emp_part_time_hour_week,
	V_emp_part_time_hour_week AS emp_part_time_hour_week,
	'N/A' AS V_emp_dept_num,
	V_emp_dept_num AS emp_dept_num,
	0 AS V_emp_part_time_hourly_week_rate_amt,
	V_emp_part_time_hourly_week_rate_amt AS emp_part_time_hourly_week_rate_amt,
	0 AS V_wage_rate_amt,
	V_wage_rate_amt AS wage_rate_amt,
	'N/A' AS V_wage_period_code,
	V_wage_period_code AS wage_period_code,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
	) AS V_wage_eff_date,
	V_wage_eff_date AS wage_eff_date,
	0 AS V_weeks_worked,
	V_weeks_worked AS weeks_worked,
	'N/A' AS V_gross_amt_type,
	V_gross_amt_type AS gross_amt_type,
	0 AS V_gross_wage_amt_excluding_tips,
	V_gross_wage_amt_excluding_tips AS gross_wage_amt_excluding_tips,
	0 AS V_piece_work_num_of_weeks_excluding_overtime,
	V_piece_work_num_of_weeks_excluding_overtime AS piece_work_num_of_weeks_excluding_overtime,
	0 AS V_emp_rec_meals,
	V_emp_rec_meals AS emp_rec_meals,
	0 AS V_emp_rec_room,
	V_emp_rec_room AS emp_rec_room,
	0 AS V_emp_rec_tips,
	V_emp_rec_tips AS emp_rec_tips,
	0 AS V_overtime_amt,
	V_overtime_amt AS overtime_amt,
	0 AS V_overtime_after_hour_in_a_week,
	V_overtime_after_hour_in_a_week AS overtime_after_hour_in_a_week,
	0 AS V_overtime_after_hour_in_a_day,
	V_overtime_after_hour_in_a_day AS overtime_after_hour_in_a_day,
	'N/A' AS V_full_pay_inj_day_ind,
	V_full_pay_inj_day_ind AS full_pay_inj_day_ind,
	'N/A' AS V_salary_paid_ind,
	V_salary_paid_ind AS salary_paid_ind,
	0 AS V_avg_full_time_days_week,
	V_avg_full_time_days_week AS avg_full_time_days_week,
	0 AS V_avg_full_time_hours_day,
	V_avg_full_time_hours_day AS avg_full_time_hours_day,
	0 AS V_avg_full_time_hours_week,
	V_avg_full_time_hours_week AS avg_full_time_hours_week,
	LKP_42GQ_WC2.IPFCGQ_PRE_INJ_WAGE,
	-- *INF*: IIF(ISNULL(TO_DECIMAL(IPFCGQ_PRE_INJ_WAGE)),0,TO_DECIMAL(IPFCGQ_PRE_INJ_WAGE))
	IFF(CAST(IPFCGQ_PRE_INJ_WAGE AS FLOAT) IS NULL,
		0,
		CAST(IPFCGQ_PRE_INJ_WAGE AS FLOAT)
	) AS V_avg_wkly_wage,
	V_avg_wkly_wage AS avg_wkly_wage,
	0 AS V_num_of_full_time_emplymnt_same_job,
	V_num_of_full_time_emplymnt_same_job AS num_of_full_time_emplymnt_same_job,
	0 AS V_num_of_part_time_emplymnt_same_job,
	V_num_of_part_time_emplymnt_same_job AS num_of_part_time_emplymnt_same_job,
	0 AS V_ttd_rate,
	V_ttd_rate AS ttd_rate,
	0 AS V_ppd_rate,
	V_ppd_rate AS ppd_rate,
	0 AS V_ptd_rate,
	V_ptd_rate AS ptd_rate,
	0 AS V_dtd_rate,
	V_dtd_rate AS dtd_rate,
	0 AS V_wkly_attorney_fee,
	V_wkly_attorney_fee AS wkly_attorney_fee,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
	) AS V_first_rpt_inj_date,
	V_first_rpt_inj_date AS first_rpt_inj_date,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
	) AS V_supplementary_rpt_inj_date,
	V_supplementary_rpt_inj_date AS supplementary_rpt_inj_date,
	0 AS V_fringe_bnft_discontinued_amt,
	V_fringe_bnft_discontinued_amt AS fringe_bnft_discontinued_amt,
	'00:00:00' AS V_emp_start_time,
	V_emp_start_time AS emp_start_time,
	0 AS V_emp_hour_day,
	V_emp_hour_day AS emp_hour_day,
	0 AS V_emp_hour_week,
	V_emp_hour_week AS emp_hour_week,
	0 AS V_emp_day_week,
	V_emp_day_week AS emp_day_week,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
	) AS V_inj_work_day_begin_time,
	V_inj_work_day_begin_time AS inj_work_day_begin_time,
	LKP_42GQ_MS3.IPFCGQ_DATE_OF_DISABILITY,
	-- *INF*: IIF(ISNULL(IPFCGQ_DATE_OF_DISABILITY) OR IPFCGQ_DATE_OF_DISABILITY=0,TO_DATE('1800/01/01','YYYY/MM/DD'),TO_DATE(TO_CHAR(IPFCGQ_DATE_OF_DISABILITY),'YYYYMMDD'))
	IFF(IPFCGQ_DATE_OF_DISABILITY IS NULL 
		OR IPFCGQ_DATE_OF_DISABILITY = 0,
		TO_DATE('1800/01/01', 'YYYY/MM/DD'
		),
		TO_DATE(TO_CHAR(IPFCGQ_DATE_OF_DISABILITY
			), 'YYYYMMDD'
		)
	) AS V_disability_date,
	V_disability_date AS disability_date,
	'N/A' AS V_phys_restriction_ind,
	V_phys_restriction_ind AS phys_restriction_ind,
	'N/A' AS V_pre_exst_disability_ind,
	V_pre_exst_disability_ind AS pre_exst_disability_ind,
	'N/A' AS V_premises_code,
	V_premises_code AS premises_code,
	'N/A' AS V_work_process_descript,
	V_work_process_descript AS work_process_descript,
	'N/A' AS V_task_descript,
	V_task_descript AS task_descript,
	EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_LOSS_AIA_CODES_1_2 AS body_part_code,
	EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_LOSS_AIA_CODES_3_4 AS nature_inj_code,
	EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_LOSS_AIA_CODES_5_6 AS cause_inj_code,
	'N/A' AS V_safeguard_not_used_ind,
	V_safeguard_not_used_ind AS safeguard_not_used_ind,
	'N/A' AS V_inj_substance_abuse_ind,
	V_inj_substance_abuse_ind AS inj_substance_abuse_ind,
	'N/A' AS V_sfty_device_not_used_ind,
	V_sfty_device_not_used_ind AS sfty_device_not_used_ind,
	'N/A' AS V_inj_rules_not_obeyed_ind,
	V_inj_rules_not_obeyed_ind AS inj_rules_not_obeyed_ind,
	'N/A' AS V_inj_result_occuptnal_inj_ind,
	V_inj_result_occuptnal_inj_ind AS inj_result_occuptnal_inj_ind,
	'N/A' AS V_inj_result_occuptnal_disease_ndicator,
	V_inj_result_occuptnal_disease_ndicator AS inj_result_occuptnal_disease_ndicator,
	EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_DEATH_INDICATOR AS inj_result_death_ind,
	'N/A' AS V_unsafe_act_descript,
	V_unsafe_act_descript AS unsafe_act_descript,
	'N/A' AS V_responsible_for_inj_descript,
	V_responsible_for_inj_descript AS responsible_for_inj_descript,
	'N/A' AS V_hazard_condition_descript,
	V_hazard_condition_descript AS hazard_condition_descript,
	LKP_42GQ_WC1.IPFCGQ_EMPLOYEE_DEATH_DATE,
	-- *INF*: IIF(ISNULL(IPFCGQ_EMPLOYEE_DEATH_DATE) OR IS_SPACES(IPFCGQ_EMPLOYEE_DEATH_DATE),TO_DATE('1800/01/01','YYYY/MM/DD'),TO_DATE(IPFCGQ_EMPLOYEE_DEATH_DATE,'YYYYMMDD'))
	IFF(IPFCGQ_EMPLOYEE_DEATH_DATE IS NULL 
		OR LENGTH(IPFCGQ_EMPLOYEE_DEATH_DATE)>0 AND TRIM(IPFCGQ_EMPLOYEE_DEATH_DATE)='',
		TO_DATE('1800/01/01', 'YYYY/MM/DD'
		),
		TO_DATE(IPFCGQ_EMPLOYEE_DEATH_DATE, 'YYYYMMDD'
		)
	) AS V_death_date,
	V_death_date AS death_date,
	'N/A' AS V_emplyr_nature_bus_descript,
	V_emplyr_nature_bus_descript AS emplyr_nature_bus_descript,
	'N/A' AS V_emplyr_type_code,
	V_emplyr_type_code AS emplyr_type_code,
	'N/A' AS V_insd_type_code,
	V_insd_type_code AS insd_type_code,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
	) AS V_subrogation_statute_exp_date,
	V_subrogation_statute_exp_date AS subrogation_statute_exp_date,
	LKP_42GQ_WC3.IPFCGQ_MANGD_CARE_ORG_IND,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IPFCGQ_MANGD_CARE_ORG_IND))),'N/A',IIF(IS_SPACES(IPFCGQ_MANGD_CARE_ORG_IND),'N/A',LTRIM(RTRIM(IPFCGQ_MANGD_CARE_ORG_IND))))
	IFF(LTRIM(RTRIM(IPFCGQ_MANGD_CARE_ORG_IND
			)
		) IS NULL,
		'N/A',
		IFF(LENGTH(IPFCGQ_MANGD_CARE_ORG_IND)>0 AND TRIM(IPFCGQ_MANGD_CARE_ORG_IND)='',
			'N/A',
			LTRIM(RTRIM(IPFCGQ_MANGD_CARE_ORG_IND
				)
			)
		)
	) AS V_managed_care_org_type,
	V_managed_care_org_type AS managed_care_org_type,
	LKP_42GQ_WC2.IPFCGQ_SUBROGATION_CODE,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IPFCGQ_SUBROGATION_CODE))),'N/A',IIF(IS_SPACES(IPFCGQ_SUBROGATION_CODE),'N/A',LTRIM(RTRIM(IPFCGQ_SUBROGATION_CODE))))
	IFF(LTRIM(RTRIM(IPFCGQ_SUBROGATION_CODE
			)
		) IS NULL,
		'N/A',
		IFF(LENGTH(IPFCGQ_SUBROGATION_CODE)>0 AND TRIM(IPFCGQ_SUBROGATION_CODE)='',
			'N/A',
			LTRIM(RTRIM(IPFCGQ_SUBROGATION_CODE
				)
			)
		)
	) AS V_subrogation_code,
	V_subrogation_code AS subrogation_code,
	EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_LOSS_COV_CODE_2,
	LKP_42GQ_WC3.IPFCGQ_TYPE_COVG,
	LKP_42GQ_WC3.IPFCGQ_TYPE_SETL,
	-- *INF*: IPFCGQ_LOSS_COV_CODE_2||TO_CHAR(IPFCGQ_TYPE_COVG)||TO_CHAR(IPFCGQ_TYPE_SETL)
	IPFCGQ_LOSS_COV_CODE_2 || TO_CHAR(IPFCGQ_TYPE_COVG
	) || TO_CHAR(IPFCGQ_TYPE_SETL
	) AS V_loss_condition,
	V_loss_condition AS loss_condition,
	LKP_42GQ_MS3.IPFCGQ_ATTORNEY_AUTH_REP,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IPFCGQ_ATTORNEY_AUTH_REP))),'N/A',IIF(IS_SPACES(IPFCGQ_ATTORNEY_AUTH_REP),'N/A',LTRIM(RTRIM(IPFCGQ_ATTORNEY_AUTH_REP))))
	IFF(LTRIM(RTRIM(IPFCGQ_ATTORNEY_AUTH_REP
			)
		) IS NULL,
		'N/A',
		IFF(LENGTH(IPFCGQ_ATTORNEY_AUTH_REP)>0 AND TRIM(IPFCGQ_ATTORNEY_AUTH_REP)='',
			'N/A',
			LTRIM(RTRIM(IPFCGQ_ATTORNEY_AUTH_REP
				)
			)
		)
	) AS V_attorney_or_au_rep_ind,
	V_attorney_or_au_rep_ind AS attorney_or_au_rep_ind,
	LKP_42GQ_WC1.IPFCGQ_HOSPITAL_COSTS,
	-- *INF*: IIF(ISNULL(IPFCGQ_HOSPITAL_COSTS),0,IPFCGQ_HOSPITAL_COSTS)
	IFF(IPFCGQ_HOSPITAL_COSTS IS NULL,
		0,
		IPFCGQ_HOSPITAL_COSTS
	) AS V_hospital_cost,
	V_hospital_cost AS hospital_cost,
	LKP_42GQ_WC1.IPFCGQ_DOCTOR_COSTS,
	-- *INF*: IIF(ISNULL(IPFCGQ_DOCTOR_COSTS),0,IPFCGQ_DOCTOR_COSTS)
	IFF(IPFCGQ_DOCTOR_COSTS IS NULL,
		0,
		IPFCGQ_DOCTOR_COSTS
	) AS V_doctor_cost,
	V_doctor_cost AS doctor_cost,
	LKP_42GQ_WC1.IPFCGQ_OTHER_MED_COSTS,
	-- *INF*: IIF(ISNULL(IPFCGQ_OTHER_MED_COSTS),0,IPFCGQ_OTHER_MED_COSTS)
	IFF(IPFCGQ_OTHER_MED_COSTS IS NULL,
		0,
		IPFCGQ_OTHER_MED_COSTS
	) AS V_other_med_cost,
	V_other_med_cost AS other_med_cost,
	LKP_42GQ_MS3.IPFCGQ_CONTROVERTED_CASE,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IPFCGQ_CONTROVERTED_CASE))),'N/A',IIF(IS_SPACES(IPFCGQ_CONTROVERTED_CASE),'N/A',LTRIM(RTRIM(IPFCGQ_CONTROVERTED_CASE))))
	IFF(LTRIM(RTRIM(IPFCGQ_CONTROVERTED_CASE
			)
		) IS NULL,
		'N/A',
		IFF(LENGTH(IPFCGQ_CONTROVERTED_CASE)>0 AND TRIM(IPFCGQ_CONTROVERTED_CASE)='',
			'N/A',
			LTRIM(RTRIM(IPFCGQ_CONTROVERTED_CASE
				)
			)
		)
	) AS V_controverted_case_code,
	V_controverted_case_code AS controverted_case_code,
	LKP_42GQ_MS3.IPFCGQ_SURGERY,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IPFCGQ_SURGERY))),'N/A',IIF(IS_SPACES(IPFCGQ_SURGERY),'N/A',LTRIM(RTRIM(IPFCGQ_SURGERY))))
	IFF(LTRIM(RTRIM(IPFCGQ_SURGERY
			)
		) IS NULL,
		'N/A',
		IFF(LENGTH(IPFCGQ_SURGERY)>0 AND TRIM(IPFCGQ_SURGERY)='',
			'N/A',
			LTRIM(RTRIM(IPFCGQ_SURGERY
				)
			)
		)
	) AS V_surgery_ind,
	V_surgery_ind AS surgery_ind,
	'N/A' AS V_emplyr_loc_descript,
	V_emplyr_loc_descript AS emplyr_loc_descript,
	'N/A' AS V_inj_loc_comment,
	V_inj_loc_comment AS inj_loc_comment,
	'N/A' AS V_claim_ctgry_code,
	V_claim_ctgry_code AS claim_ctgry_code,
	'N/A' AS V_act_status_code,
	V_act_status_code AS act_status_code,
	'N/A' AS V_investigate_ind,
	V_investigate_ind AS investigate_ind,
	'N/A' AS V_emplyr_standard_industry_code,
	V_emplyr_standard_industry_code AS emplyr_standard_industry_code,
	'N/A' AS V_hospitalized_ind,
	V_hospitalized_ind AS hospitalized_ind,
	LKP_42GQ_WC2.IPFCGQ_WAGE_METHOD,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IPFCGQ_WAGE_METHOD))),'N/A',IIF(IS_SPACES(IPFCGQ_WAGE_METHOD),'N/A',LTRIM(RTRIM(IPFCGQ_WAGE_METHOD))))
	IFF(LTRIM(RTRIM(IPFCGQ_WAGE_METHOD
			)
		) IS NULL,
		'N/A',
		IFF(LENGTH(IPFCGQ_WAGE_METHOD)>0 AND TRIM(IPFCGQ_WAGE_METHOD)='',
			'N/A',
			LTRIM(RTRIM(IPFCGQ_WAGE_METHOD
				)
			)
		)
	) AS V_wage_method_code,
	V_wage_method_code AS wage_method_code,
	-- *INF*: :LKP.LKP_42GQ_MS2(PIF_SYMBOL,PIF_POLICY_NUMBER,PIF_MODULE,IPFCGQ_YEAR_OF_LOSS,IPFCGQ_MONTH_OF_LOSS,IPFCGQ_DAY_OF_LOSS,IPFCGQ_LOSS_OCCURENCE,IPFCGQ_LOSS_CLAIMANT)
	LKP_42GQ_MS2_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFCGQ_YEAR_OF_LOSS_IPFCGQ_MONTH_OF_LOSS_IPFCGQ_DAY_OF_LOSS_IPFCGQ_LOSS_OCCURENCE_IPFCGQ_LOSS_CLAIMANT.IPFCGQ_OCCUPATION AS V_LKP_pms_occuptn_descript,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(V_LKP_pms_occuptn_descript))),'N/A',IIF(IS_SPACES(V_LKP_pms_occuptn_descript),'N/A',LTRIM(RTRIM(V_LKP_pms_occuptn_descript))))
	IFF(LTRIM(RTRIM(V_LKP_pms_occuptn_descript
			)
		) IS NULL,
		'N/A',
		IFF(LENGTH(V_LKP_pms_occuptn_descript)>0 AND TRIM(V_LKP_pms_occuptn_descript)='',
			'N/A',
			LTRIM(RTRIM(V_LKP_pms_occuptn_descript
				)
			)
		)
	) AS V_pms_occuptn_descript,
	V_pms_occuptn_descript AS pms_occuptn_descript,
	EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_TYPE_DISABILITY AS pms_type_disability,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(TO_CHAR(IPFCGQ_TYPE_COVG)))),'N/A',IIF(IS_SPACES(TO_CHAR(IPFCGQ_TYPE_COVG)),'N/A',LTRIM(RTRIM(TO_CHAR(IPFCGQ_TYPE_COVG)))))
	IFF(LTRIM(RTRIM(TO_CHAR(IPFCGQ_TYPE_COVG
				)
			)
		) IS NULL,
		'N/A',
		IFF(LENGTH(TO_CHAR(IPFCGQ_TYPE_COVG
			))>0 AND TRIM(TO_CHAR(IPFCGQ_TYPE_COVG
			))='',
			'N/A',
			LTRIM(RTRIM(TO_CHAR(IPFCGQ_TYPE_COVG
					)
				)
			)
		)
	) AS V_ncci_type_cov,
	V_ncci_type_cov AS ncci_type_cov,
	-- *INF*: IIF(ISNULL(LKP_claim_party_occurrence_ak_id),'NEW',
	-- IIF((ltrim(rtrim(LKP_jurisdiction_state_code)) <> ltrim(rtrim(V_jurisdiction_state_code_Actual))) OR 
	-- LKP_emplyr_notified_date <> V_emplyr_notified_date OR 
	-- LKP_rpted_to_carrier_date <> V_rpted_to_carrier_date OR 
	-- (ltrim(rtrim(LKP_jurisdiction_claim_num)) <> ltrim(rtrim(V_jurisdiction_claim_num))) OR 
	-- (ltrim(rtrim(LKP_care_directed_ind)) <> ltrim(rtrim(V_care_directed_ind))) OR 
	-- (ltrim(rtrim(LKP_care_directed_by)) <> ltrim(rtrim(V_care_directed_by))) OR 
	-- (ltrim(rtrim(LKP_hired_state_code)) <> ltrim(rtrim(V_hired_state_code))) OR 
	-- LKP_hired_date <> V_hired_date OR 
	-- (ltrim(rtrim(LKP_tax_filing_status)) <> ltrim(rtrim(V_tax_filing_status))) OR 
	-- (ltrim(rtrim(LKP_occuptn_code)) <> ltrim(rtrim(V_occuptn_code))) OR 
	-- (ltrim(rtrim(LKP_employement_status_code)) <> ltrim(rtrim(V_employement_status_code))) OR 
	-- LKP_len_of_time_in_crrnt_job <> V_len_of_time_in_crrnt_job OR 
	-- (ltrim(rtrim(LKP_emp_dept_name)) <> ltrim(rtrim(V_emp_dept_name))) OR 
	-- (ltrim(rtrim(LKP_emp_shift_num)) <> ltrim(rtrim(V_emp_shift_num))) OR 
	-- (ltrim(rtrim(LKP_marital_status)) <> ltrim(rtrim(marital_status))) OR 
	-- LKP_num_of_dependents <> V_num_of_dependents OR 
	-- LKP_num_of_dependent_children <> V_num_of_dependent_children OR 
	-- LKP_num_of_other_dependents <> V_num_of_other_dependents OR 
	-- LKP_num_of_exemptions <> V_num_of_exemptions OR 
	-- (ltrim(rtrim(LKP_exemption_type)) <> ltrim(rtrim(V_exemption_type))) OR 
	-- (ltrim(rtrim(LKP_emp_blind_ind)) <> ltrim(rtrim(V_emp_blind_ind))) OR 
	-- (ltrim(rtrim(LKP_emp_over_65_ind)) <> ltrim(rtrim(V_emp_over_65_ind))) OR 
	-- (ltrim(rtrim(LKP_spouse_blind_ind)) <> ltrim(rtrim(V_spouse_blind_ind))) OR 
	-- (ltrim(rtrim(LKP_spouse_over_65_ind)) <> ltrim(rtrim(V_spouse_over_65_ind))) OR 
	-- (ltrim(rtrim(LKP_education_lvl)) <> ltrim(rtrim(V_education_lvl))) OR 
	-- (ltrim(rtrim(LKP_med_auth_ind)) <> ltrim(rtrim(V_med_auth_ind))) OR 
	-- (ltrim(rtrim(LKP_auth_to_release_ssn_ind)) <> ltrim(rtrim(V_auth_to_release_ssn_ind))) OR 
	-- (ltrim(rtrim(LKP_emp_id_num)) <> ltrim(rtrim(V_emp_id_num))) OR 
	-- (ltrim(rtrim(LKP_emp_id_type)) <> ltrim(rtrim(V_emp_id_type))) OR 
	-- LKP_emp_part_time_hour_week <> V_emp_part_time_hour_week OR 
	-- (ltrim(rtrim(LKP_emp_dept_num)) <> ltrim(rtrim(V_emp_dept_num))) OR 
	-- LKP_emp_part_time_hourly_week_rate_amt <> V_emp_part_time_hourly_week_rate_amt OR 
	-- LKP_wage_rate_amt <> V_wage_rate_amt OR 
	-- (ltrim(rtrim(LKP_wage_period_code)) <> ltrim(rtrim(V_wage_period_code))) OR 
	-- LKP_wage_eff_date <> V_wage_eff_date OR 
	-- LKP_weeks_worked <> V_weeks_worked OR 
	-- (ltrim(rtrim(LKP_gross_amt_type)) <> ltrim(rtrim(V_gross_amt_type))) OR 
	-- LKP_gross_wage_amt_excluding_tips <> V_gross_wage_amt_excluding_tips OR 
	-- LKP_piece_work_num_of_weeks_excluding_overtime <>V_piece_work_num_of_weeks_excluding_overtime OR 
	-- LKP_emp_rec_meals <> V_emp_rec_meals OR 
	-- LKP_emp_rec_room <> V_emp_rec_room OR 
	-- LKP_emp_rec_tips <> V_emp_rec_tips OR 
	-- LKP_overtime_amt <> V_overtime_amt OR 
	-- LKP_overtime_after_hour_in_a_week <> V_overtime_after_hour_in_a_week OR 
	-- LKP_overtime_after_hour_in_a_day <> V_overtime_after_hour_in_a_day OR 
	-- (ltrim(rtrim(LKP_full_pay_inj_day_ind)) <> ltrim(rtrim(V_full_pay_inj_day_ind))) OR 
	-- (ltrim(rtrim(LKP_salary_paid_ind)) <> ltrim(rtrim(V_salary_paid_ind))) OR 
	-- LKP_avg_full_time_days_week <> V_avg_full_time_days_week OR 
	-- LKP_avg_full_time_hours_day <> V_avg_full_time_hours_day OR 
	-- LKP_avg_full_time_hours_week <> V_avg_full_time_hours_week OR 
	-- LKP_avg_wkly_wage <> V_avg_wkly_wage OR 
	-- LKP_num_of_full_time_emplymnt_same_job <> V_num_of_full_time_emplymnt_same_job OR 
	-- LKP_num_of_part_time_emplymnt_same_job <> V_num_of_part_time_emplymnt_same_job OR 
	-- LKP_ttd_rate <> V_ttd_rate OR 
	-- LKP_ppd_rate <> V_ppd_rate OR 
	-- LKP_ptd_rate <> V_ptd_rate OR 
	-- LKP_dtd_rate <> V_dtd_rate OR 
	-- LKP_wkly_attorney_fee <> V_wkly_attorney_fee OR 
	-- LKP_first_rpt_inj_date <> V_first_rpt_inj_date OR 
	-- LKP_supplementary_rpt_inj_date <> V_supplementary_rpt_inj_date OR 
	-- LKP_fringe_bnft_discontinued_amt <> V_fringe_bnft_discontinued_amt OR 
	-- LKP_emp_start_time <> V_emp_start_time OR 
	-- LKP_emp_hour_day <> V_emp_hour_day OR 
	-- LKP_emp_hour_week <> V_emp_hour_week OR 
	-- LKP_emp_day_week <> V_emp_day_week OR 
	-- LKP_inj_work_day_begin_time <> V_inj_work_day_begin_time OR 
	-- LKP_disability_date <> V_disability_date OR 
	-- (ltrim(rtrim(LKP_phys_restriction_ind)) <> ltrim(rtrim(V_phys_restriction_ind))) OR 
	-- (ltrim(rtrim(LKP_pre_exst_disability_ind)) <> ltrim(rtrim(V_pre_exst_disability_ind))) OR 
	-- (ltrim(rtrim(LKP_premises_code)) <> ltrim(rtrim(V_premises_code))) OR 
	-- (ltrim(rtrim(LKP_work_process_descript)) <> ltrim(rtrim(V_work_process_descript))) OR 
	-- (ltrim(rtrim(LKP_task_descript)) <> ltrim(rtrim(V_task_descript))) OR 
	-- (ltrim(rtrim(LKP_body_part_code)) <> ltrim(rtrim(body_part_code))) OR 
	-- (ltrim(rtrim(LKP_nature_inj_code)) <> ltrim(rtrim(nature_inj_code))) OR 
	-- (ltrim(rtrim(LKP_cause_inj_code)) <> ltrim(rtrim(cause_inj_code))) OR 
	-- (ltrim(rtrim(LKP_safeguard_not_used_ind)) <> ltrim(rtrim(V_safeguard_not_used_ind))) OR 
	-- (ltrim(rtrim(LKP_inj_substance_abuse_ind)) <> ltrim(rtrim(V_inj_substance_abuse_ind))) OR 
	-- (ltrim(rtrim(LKP_sfty_device_not_used_ind)) <> ltrim(rtrim(V_sfty_device_not_used_ind))) OR 
	-- (ltrim(rtrim(LKP_inj_rules_not_obeyed_ind)) <> ltrim(rtrim(V_inj_rules_not_obeyed_ind))) OR 
	-- (ltrim(rtrim(LKP_inj_result_occuptnal_inj_ind)) <> ltrim(rtrim(V_inj_result_occuptnal_inj_ind))) OR 
	-- (ltrim(rtrim(LKP_inj_result_occuptnal_disease_ndicator)) <> ltrim(rtrim(V_inj_result_occuptnal_disease_ndicator))) OR 
	-- (ltrim(rtrim(LKP_inj_result_death_ind)) <> ltrim(rtrim(inj_result_death_ind))) OR 
	-- (ltrim(rtrim(LKP_unsafe_act_descript)) <> ltrim(rtrim(V_unsafe_act_descript))) OR 
	-- (ltrim(rtrim(LKP_responsible_for_inj_descript)) <> ltrim(rtrim(V_responsible_for_inj_descript))) OR 
	-- (ltrim(rtrim(LKP_hazard_condition_descript)) <> ltrim(rtrim(V_hazard_condition_descript))) OR  
	-- LKP_death_date <> V_death_date OR 
	--  (ltrim(rtrim(LKP_emplyr_nature_bus_descript)) <> ltrim(rtrim(V_emplyr_nature_bus_descript))) OR 
	-- (ltrim(rtrim(LKP_emplyr_type_code)) <> ltrim(rtrim(V_emplyr_type_code))) OR 
	-- (ltrim(rtrim(LKP_insd_type_code)) <> ltrim(rtrim(V_insd_type_code))) OR 
	-- LKP_subrogation_statute_exp_date <> V_subrogation_statute_exp_date OR 
	-- (ltrim(rtrim(LKP_managed_care_org_type)) <> ltrim(rtrim(V_managed_care_org_type))) OR 
	-- (ltrim(rtrim(LKP_subrogation_code)) <> ltrim(rtrim(V_subrogation_code))) OR 
	-- (ltrim(rtrim(LKP_loss_condition)) <> ltrim(rtrim(V_loss_condition))) OR 
	-- (ltrim(rtrim(LKP_attorney_or_au_rep_ind)) <> ltrim(rtrim(V_attorney_or_au_rep_ind))) OR 
	-- LKP_hospital_cost <> V_hospital_cost OR 
	-- LKP_doctor_cost  <> V_doctor_cost OR 
	-- LKP_other_med_cost <> V_other_med_cost OR 
	-- (ltrim(rtrim(LKP_controverted_case_code)) <> ltrim(rtrim(V_controverted_case_code))) OR 
	-- (ltrim(rtrim(LKP_surgery_ind)) <> ltrim(rtrim(V_surgery_ind))) OR 
	-- (ltrim(rtrim(LKP_emplyr_loc_descript)) <> ltrim(rtrim(V_emplyr_loc_descript))) OR 
	-- (ltrim(rtrim(LKP_inj_loc_comment)) <> ltrim(rtrim(V_inj_loc_comment))) OR 
	-- (ltrim(rtrim(LKP_claim_ctgry_code)) <> ltrim(rtrim(V_claim_ctgry_code))) OR 
	-- (ltrim(rtrim(LKP_act_status_code)) <> ltrim(rtrim(V_act_status_code))) OR 
	-- (ltrim(rtrim(LKP_investigate_ind)) <> ltrim(rtrim(V_investigate_ind))) OR 
	-- (ltrim(rtrim(LKP_emplyr_standard_industry_code)) <> ltrim(rtrim(V_emplyr_standard_industry_code))) OR 
	-- (ltrim(rtrim(LKP_hospitalized_ind)) <> ltrim(rtrim(V_hospitalized_ind))) OR 
	-- (ltrim(rtrim(LKP_wage_method_code)) <> ltrim(rtrim(V_wage_method_code))) OR 
	-- (ltrim(rtrim(LKP_pms_occuptn_descript)) <> ltrim(rtrim(V_pms_occuptn_descript))) OR 
	-- (ltrim(rtrim(LKP_pms_type_disability)) <> ltrim(rtrim(pms_type_disability))) OR 
	-- (ltrim(rtrim(LKP_ncci_type_cov)) <> ltrim(rtrim(V_ncci_type_cov))),
	-- 'UPDATE','NOCHANGE'))
	IFF(LKP_claim_party_occurrence_ak_id IS NULL,
		'NEW',
		IFF(( ltrim(rtrim(LKP_jurisdiction_state_code
					)
				) <> ltrim(rtrim(V_jurisdiction_state_code_Actual
					)
				) 
			) 
			OR LKP_emplyr_notified_date <> V_emplyr_notified_date 
			OR LKP_rpted_to_carrier_date <> V_rpted_to_carrier_date 
			OR ( ltrim(rtrim(LKP_jurisdiction_claim_num
					)
				) <> ltrim(rtrim(V_jurisdiction_claim_num
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_care_directed_ind
					)
				) <> ltrim(rtrim(V_care_directed_ind
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_care_directed_by
					)
				) <> ltrim(rtrim(V_care_directed_by
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_hired_state_code
					)
				) <> ltrim(rtrim(V_hired_state_code
					)
				) 
			) 
			OR LKP_hired_date <> V_hired_date 
			OR ( ltrim(rtrim(LKP_tax_filing_status
					)
				) <> ltrim(rtrim(V_tax_filing_status
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_occuptn_code
					)
				) <> ltrim(rtrim(V_occuptn_code
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_employement_status_code
					)
				) <> ltrim(rtrim(V_employement_status_code
					)
				) 
			) 
			OR LKP_len_of_time_in_crrnt_job <> V_len_of_time_in_crrnt_job 
			OR ( ltrim(rtrim(LKP_emp_dept_name
					)
				) <> ltrim(rtrim(V_emp_dept_name
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_emp_shift_num
					)
				) <> ltrim(rtrim(V_emp_shift_num
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_marital_status
					)
				) <> ltrim(rtrim(marital_status
					)
				) 
			) 
			OR LKP_num_of_dependents <> V_num_of_dependents 
			OR LKP_num_of_dependent_children <> V_num_of_dependent_children 
			OR LKP_num_of_other_dependents <> V_num_of_other_dependents 
			OR LKP_num_of_exemptions <> V_num_of_exemptions 
			OR ( ltrim(rtrim(LKP_exemption_type
					)
				) <> ltrim(rtrim(V_exemption_type
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_emp_blind_ind
					)
				) <> ltrim(rtrim(V_emp_blind_ind
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_emp_over_65_ind
					)
				) <> ltrim(rtrim(V_emp_over_65_ind
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_spouse_blind_ind
					)
				) <> ltrim(rtrim(V_spouse_blind_ind
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_spouse_over_65_ind
					)
				) <> ltrim(rtrim(V_spouse_over_65_ind
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_education_lvl
					)
				) <> ltrim(rtrim(V_education_lvl
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_med_auth_ind
					)
				) <> ltrim(rtrim(V_med_auth_ind
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_auth_to_release_ssn_ind
					)
				) <> ltrim(rtrim(V_auth_to_release_ssn_ind
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_emp_id_num
					)
				) <> ltrim(rtrim(V_emp_id_num
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_emp_id_type
					)
				) <> ltrim(rtrim(V_emp_id_type
					)
				) 
			) 
			OR LKP_emp_part_time_hour_week <> V_emp_part_time_hour_week 
			OR ( ltrim(rtrim(LKP_emp_dept_num
					)
				) <> ltrim(rtrim(V_emp_dept_num
					)
				) 
			) 
			OR LKP_emp_part_time_hourly_week_rate_amt <> V_emp_part_time_hourly_week_rate_amt 
			OR LKP_wage_rate_amt <> V_wage_rate_amt 
			OR ( ltrim(rtrim(LKP_wage_period_code
					)
				) <> ltrim(rtrim(V_wage_period_code
					)
				) 
			) 
			OR LKP_wage_eff_date <> V_wage_eff_date 
			OR LKP_weeks_worked <> V_weeks_worked 
			OR ( ltrim(rtrim(LKP_gross_amt_type
					)
				) <> ltrim(rtrim(V_gross_amt_type
					)
				) 
			) 
			OR LKP_gross_wage_amt_excluding_tips <> V_gross_wage_amt_excluding_tips 
			OR LKP_piece_work_num_of_weeks_excluding_overtime <> V_piece_work_num_of_weeks_excluding_overtime 
			OR LKP_emp_rec_meals <> V_emp_rec_meals 
			OR LKP_emp_rec_room <> V_emp_rec_room 
			OR LKP_emp_rec_tips <> V_emp_rec_tips 
			OR LKP_overtime_amt <> V_overtime_amt 
			OR LKP_overtime_after_hour_in_a_week <> V_overtime_after_hour_in_a_week 
			OR LKP_overtime_after_hour_in_a_day <> V_overtime_after_hour_in_a_day 
			OR ( ltrim(rtrim(LKP_full_pay_inj_day_ind
					)
				) <> ltrim(rtrim(V_full_pay_inj_day_ind
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_salary_paid_ind
					)
				) <> ltrim(rtrim(V_salary_paid_ind
					)
				) 
			) 
			OR LKP_avg_full_time_days_week <> V_avg_full_time_days_week 
			OR LKP_avg_full_time_hours_day <> V_avg_full_time_hours_day 
			OR LKP_avg_full_time_hours_week <> V_avg_full_time_hours_week 
			OR LKP_avg_wkly_wage <> V_avg_wkly_wage 
			OR LKP_num_of_full_time_emplymnt_same_job <> V_num_of_full_time_emplymnt_same_job 
			OR LKP_num_of_part_time_emplymnt_same_job <> V_num_of_part_time_emplymnt_same_job 
			OR LKP_ttd_rate <> V_ttd_rate 
			OR LKP_ppd_rate <> V_ppd_rate 
			OR LKP_ptd_rate <> V_ptd_rate 
			OR LKP_dtd_rate <> V_dtd_rate 
			OR LKP_wkly_attorney_fee <> V_wkly_attorney_fee 
			OR LKP_first_rpt_inj_date <> V_first_rpt_inj_date 
			OR LKP_supplementary_rpt_inj_date <> V_supplementary_rpt_inj_date 
			OR LKP_fringe_bnft_discontinued_amt <> V_fringe_bnft_discontinued_amt 
			OR LKP_emp_start_time <> V_emp_start_time 
			OR LKP_emp_hour_day <> V_emp_hour_day 
			OR LKP_emp_hour_week <> V_emp_hour_week 
			OR LKP_emp_day_week <> V_emp_day_week 
			OR LKP_inj_work_day_begin_time <> V_inj_work_day_begin_time 
			OR LKP_disability_date <> V_disability_date 
			OR ( ltrim(rtrim(LKP_phys_restriction_ind
					)
				) <> ltrim(rtrim(V_phys_restriction_ind
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_pre_exst_disability_ind
					)
				) <> ltrim(rtrim(V_pre_exst_disability_ind
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_premises_code
					)
				) <> ltrim(rtrim(V_premises_code
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_work_process_descript
					)
				) <> ltrim(rtrim(V_work_process_descript
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_task_descript
					)
				) <> ltrim(rtrim(V_task_descript
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_body_part_code
					)
				) <> ltrim(rtrim(body_part_code
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_nature_inj_code
					)
				) <> ltrim(rtrim(nature_inj_code
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_cause_inj_code
					)
				) <> ltrim(rtrim(cause_inj_code
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_safeguard_not_used_ind
					)
				) <> ltrim(rtrim(V_safeguard_not_used_ind
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_inj_substance_abuse_ind
					)
				) <> ltrim(rtrim(V_inj_substance_abuse_ind
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_sfty_device_not_used_ind
					)
				) <> ltrim(rtrim(V_sfty_device_not_used_ind
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_inj_rules_not_obeyed_ind
					)
				) <> ltrim(rtrim(V_inj_rules_not_obeyed_ind
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_inj_result_occuptnal_inj_ind
					)
				) <> ltrim(rtrim(V_inj_result_occuptnal_inj_ind
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_inj_result_occuptnal_disease_ndicator
					)
				) <> ltrim(rtrim(V_inj_result_occuptnal_disease_ndicator
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_inj_result_death_ind
					)
				) <> ltrim(rtrim(inj_result_death_ind
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_unsafe_act_descript
					)
				) <> ltrim(rtrim(V_unsafe_act_descript
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_responsible_for_inj_descript
					)
				) <> ltrim(rtrim(V_responsible_for_inj_descript
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_hazard_condition_descript
					)
				) <> ltrim(rtrim(V_hazard_condition_descript
					)
				) 
			) 
			OR LKP_death_date <> V_death_date 
			OR ( ltrim(rtrim(LKP_emplyr_nature_bus_descript
					)
				) <> ltrim(rtrim(V_emplyr_nature_bus_descript
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_emplyr_type_code
					)
				) <> ltrim(rtrim(V_emplyr_type_code
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_insd_type_code
					)
				) <> ltrim(rtrim(V_insd_type_code
					)
				) 
			) 
			OR LKP_subrogation_statute_exp_date <> V_subrogation_statute_exp_date 
			OR ( ltrim(rtrim(LKP_managed_care_org_type
					)
				) <> ltrim(rtrim(V_managed_care_org_type
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_subrogation_code
					)
				) <> ltrim(rtrim(V_subrogation_code
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_loss_condition
					)
				) <> ltrim(rtrim(V_loss_condition
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_attorney_or_au_rep_ind
					)
				) <> ltrim(rtrim(V_attorney_or_au_rep_ind
					)
				) 
			) 
			OR LKP_hospital_cost <> V_hospital_cost 
			OR LKP_doctor_cost <> V_doctor_cost 
			OR LKP_other_med_cost <> V_other_med_cost 
			OR ( ltrim(rtrim(LKP_controverted_case_code
					)
				) <> ltrim(rtrim(V_controverted_case_code
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_surgery_ind
					)
				) <> ltrim(rtrim(V_surgery_ind
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_emplyr_loc_descript
					)
				) <> ltrim(rtrim(V_emplyr_loc_descript
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_inj_loc_comment
					)
				) <> ltrim(rtrim(V_inj_loc_comment
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_claim_ctgry_code
					)
				) <> ltrim(rtrim(V_claim_ctgry_code
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_act_status_code
					)
				) <> ltrim(rtrim(V_act_status_code
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_investigate_ind
					)
				) <> ltrim(rtrim(V_investigate_ind
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_emplyr_standard_industry_code
					)
				) <> ltrim(rtrim(V_emplyr_standard_industry_code
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_hospitalized_ind
					)
				) <> ltrim(rtrim(V_hospitalized_ind
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_wage_method_code
					)
				) <> ltrim(rtrim(V_wage_method_code
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_pms_occuptn_descript
					)
				) <> ltrim(rtrim(V_pms_occuptn_descript
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_pms_type_disability
					)
				) <> ltrim(rtrim(pms_type_disability
					)
				) 
			) 
			OR ( ltrim(rtrim(LKP_ncci_type_cov
					)
				) <> ltrim(rtrim(V_ncci_type_cov
					)
				) 
			),
			'UPDATE',
			'NOCHANGE'
		)
	) AS V_CHANGE_FLAG,
	V_CHANGE_FLAG AS CHANGE_FLAG_OP,
	EXP_Lkp_Values_workers_comp_claimant_detail_PMS.logical_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: IIF(V_CHANGE_FLAG='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	TO_DATE(TO_CHAR(SYSDATE,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS'))
	IFF(V_CHANGE_FLAG = 'NEW',
		TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		TO_DATE(TO_CHAR(SYSDATE, 'MM/DD/YYYY HH24:MI:SS'
			), 'MM/DD/YYYY HH24:MI:SS'
		)
	) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	LKP_Claim_Party_Occurrence_AK_ID.claim_party_occurrence_ak_id AS CLAIM_PARTY_OCCURRENCE_AK_ID
	FROM EXP_Lkp_Values_workers_comp_claimant_detail_PMS
	LEFT JOIN LKP_42GQ_MS3
	ON LKP_42GQ_MS3.PIF_SYMBOL = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.PIF_SYMBOL AND LKP_42GQ_MS3.PIF_POLICY_NUMBER = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.PIF_POLICY_NUMBER AND LKP_42GQ_MS3.PIF_MODULE = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.PIF_MODULE AND LKP_42GQ_MS3.IPFCGQ_YEAR_OF_LOSS = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_YEAR_OF_LOSS AND LKP_42GQ_MS3.IPFCGQ_MONTH_OF_LOSS = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_MONTH_OF_LOSS AND LKP_42GQ_MS3.IPFCGQ_DAY_OF_LOSS = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_DAY_OF_LOSS AND LKP_42GQ_MS3.IPFCGQ_LOSS_OCCURENCE = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_LOSS_OCCURENCE AND LKP_42GQ_MS3.IPFCGQ_LOSS_CLAIMANT = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_LOSS_CLAIMANT
	LEFT JOIN LKP_42GQ_WC1
	ON LKP_42GQ_WC1.PIF_SYMBOL = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.PIF_SYMBOL AND LKP_42GQ_WC1.PIF_POLICY_NUMBER = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.PIF_POLICY_NUMBER AND LKP_42GQ_WC1.PIF_MODULE = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.PIF_MODULE AND LKP_42GQ_WC1.IPFCGQ_YEAR_OF_LOSS = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_YEAR_OF_LOSS AND LKP_42GQ_WC1.IPFCGQ_MONTH_OF_LOSS = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_MONTH_OF_LOSS AND LKP_42GQ_WC1.IPFCGQ_DAY_OF_LOSS = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_DAY_OF_LOSS AND LKP_42GQ_WC1.IPFCGQ_LOSS_OCCURENCE = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_LOSS_OCCURENCE AND LKP_42GQ_WC1.IPFCGQ_LOSS_CLAIMANT = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_LOSS_CLAIMANT
	LEFT JOIN LKP_42GQ_WC2
	ON LKP_42GQ_WC2.PIF_SYMBOL = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.PIF_SYMBOL AND LKP_42GQ_WC2.PIF_POLICY_NUMBER = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.PIF_POLICY_NUMBER AND LKP_42GQ_WC2.PIF_MODULE = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.PIF_MODULE AND LKP_42GQ_WC2.IPFCGQ_YEAR_OF_LOSS = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_YEAR_OF_LOSS AND LKP_42GQ_WC2.IPFCGQ_MONTH_OF_LOSS = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_MONTH_OF_LOSS AND LKP_42GQ_WC2.IPFCGQ_DAY_OF_LOSS = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_DAY_OF_LOSS AND LKP_42GQ_WC2.IPFCGQ_LOSS_OCCURENCE = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_LOSS_OCCURENCE AND LKP_42GQ_WC2.IPFCGQ_LOSS_CLAIMANT = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_LOSS_CLAIMANT
	LEFT JOIN LKP_42GQ_WC3
	ON LKP_42GQ_WC3.PIF_SYMBOL = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.PIF_SYMBOL AND LKP_42GQ_WC3.PIF_POLICY_NUMBER = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.PIF_POLICY_NUMBER AND LKP_42GQ_WC3.PIF_MODULE = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.PIF_MODULE AND LKP_42GQ_WC3.IPFCGQ_YEAR_OF_LOSS = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_YEAR_OF_LOSS AND LKP_42GQ_WC3.IPFCGQ_MONTH_OF_LOSS = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_MONTH_OF_LOSS AND LKP_42GQ_WC3.IPFCGQ_DAY_OF_LOSS = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_DAY_OF_LOSS AND LKP_42GQ_WC3.IPFCGQ_LOSS_OCCURENCE = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_LOSS_OCCURENCE AND LKP_42GQ_WC3.IPFCGQ_LOSS_CLAIMANT = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.IPFCGQ_LOSS_CLAIMANT
	LEFT JOIN LKP_Claim_Party_Occurrence_AK_ID
	ON LKP_Claim_Party_Occurrence_AK_ID.claimant_num = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.CLAIM_OCCURRENCE_KEY AND LKP_Claim_Party_Occurrence_AK_ID.claim_party_role_code = EXP_Lkp_Values_workers_comp_claimant_detail_PMS.CLAIM_PARTY_KEY
	LEFT JOIN LKP_workers_comp_claimant_detail
	ON LKP_workers_comp_claimant_detail.claim_party_occurrence_ak_id = LKP_Claim_Party_Occurrence_AK_ID.claim_party_occurrence_ak_id
	LEFT JOIN LKP_PIF_42GP LKP_PIF_42GP_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFCGQ_YEAR_OF_LOSS_IPFCGQ_MONTH_OF_LOSS_IPFCGQ_DAY_OF_LOSS_IPFCGQ_LOSS_OCCURENCE
	ON LKP_PIF_42GP_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFCGQ_YEAR_OF_LOSS_IPFCGQ_MONTH_OF_LOSS_IPFCGQ_DAY_OF_LOSS_IPFCGQ_LOSS_OCCURENCE.pif_symbol = PIF_SYMBOL
	AND LKP_PIF_42GP_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFCGQ_YEAR_OF_LOSS_IPFCGQ_MONTH_OF_LOSS_IPFCGQ_DAY_OF_LOSS_IPFCGQ_LOSS_OCCURENCE.pif_policy_number = PIF_POLICY_NUMBER
	AND LKP_PIF_42GP_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFCGQ_YEAR_OF_LOSS_IPFCGQ_MONTH_OF_LOSS_IPFCGQ_DAY_OF_LOSS_IPFCGQ_LOSS_OCCURENCE.pif_module = PIF_MODULE
	AND LKP_PIF_42GP_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFCGQ_YEAR_OF_LOSS_IPFCGQ_MONTH_OF_LOSS_IPFCGQ_DAY_OF_LOSS_IPFCGQ_LOSS_OCCURENCE.ipfcgp_year_of_loss = IPFCGQ_YEAR_OF_LOSS
	AND LKP_PIF_42GP_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFCGQ_YEAR_OF_LOSS_IPFCGQ_MONTH_OF_LOSS_IPFCGQ_DAY_OF_LOSS_IPFCGQ_LOSS_OCCURENCE.ipfcgp_month_of_loss = IPFCGQ_MONTH_OF_LOSS
	AND LKP_PIF_42GP_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFCGQ_YEAR_OF_LOSS_IPFCGQ_MONTH_OF_LOSS_IPFCGQ_DAY_OF_LOSS_IPFCGQ_LOSS_OCCURENCE.ipfcgp_day_of_loss = IPFCGQ_DAY_OF_LOSS
	AND LKP_PIF_42GP_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFCGQ_YEAR_OF_LOSS_IPFCGQ_MONTH_OF_LOSS_IPFCGQ_DAY_OF_LOSS_IPFCGQ_LOSS_OCCURENCE.ipfcgp_loss_occurence = IPFCGQ_LOSS_OCCURENCE

	LEFT JOIN LKP_SUP_STATE LKP_SUP_STATE_LTRIM_RTRIM_V_jurisdiction_state_code
	ON LKP_SUP_STATE_LTRIM_RTRIM_V_jurisdiction_state_code.state_abbrev = LTRIM(RTRIM(V_jurisdiction_state_code
		)
	)

	LEFT JOIN LKP_42GQ_MS2 LKP_42GQ_MS2_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFCGQ_YEAR_OF_LOSS_IPFCGQ_MONTH_OF_LOSS_IPFCGQ_DAY_OF_LOSS_IPFCGQ_LOSS_OCCURENCE_IPFCGQ_LOSS_CLAIMANT
	ON LKP_42GQ_MS2_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFCGQ_YEAR_OF_LOSS_IPFCGQ_MONTH_OF_LOSS_IPFCGQ_DAY_OF_LOSS_IPFCGQ_LOSS_OCCURENCE_IPFCGQ_LOSS_CLAIMANT.PIF_SYMBOL = PIF_SYMBOL
	AND LKP_42GQ_MS2_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFCGQ_YEAR_OF_LOSS_IPFCGQ_MONTH_OF_LOSS_IPFCGQ_DAY_OF_LOSS_IPFCGQ_LOSS_OCCURENCE_IPFCGQ_LOSS_CLAIMANT.PIF_POLICY_NUMBER = PIF_POLICY_NUMBER
	AND LKP_42GQ_MS2_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFCGQ_YEAR_OF_LOSS_IPFCGQ_MONTH_OF_LOSS_IPFCGQ_DAY_OF_LOSS_IPFCGQ_LOSS_OCCURENCE_IPFCGQ_LOSS_CLAIMANT.PIF_MODULE = PIF_MODULE
	AND LKP_42GQ_MS2_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFCGQ_YEAR_OF_LOSS_IPFCGQ_MONTH_OF_LOSS_IPFCGQ_DAY_OF_LOSS_IPFCGQ_LOSS_OCCURENCE_IPFCGQ_LOSS_CLAIMANT.IPFCGQ_YEAR_OF_LOSS = IPFCGQ_YEAR_OF_LOSS
	AND LKP_42GQ_MS2_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFCGQ_YEAR_OF_LOSS_IPFCGQ_MONTH_OF_LOSS_IPFCGQ_DAY_OF_LOSS_IPFCGQ_LOSS_OCCURENCE_IPFCGQ_LOSS_CLAIMANT.IPFCGQ_MONTH_OF_LOSS = IPFCGQ_MONTH_OF_LOSS
	AND LKP_42GQ_MS2_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFCGQ_YEAR_OF_LOSS_IPFCGQ_MONTH_OF_LOSS_IPFCGQ_DAY_OF_LOSS_IPFCGQ_LOSS_OCCURENCE_IPFCGQ_LOSS_CLAIMANT.IPFCGQ_DAY_OF_LOSS = IPFCGQ_DAY_OF_LOSS
	AND LKP_42GQ_MS2_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFCGQ_YEAR_OF_LOSS_IPFCGQ_MONTH_OF_LOSS_IPFCGQ_DAY_OF_LOSS_IPFCGQ_LOSS_OCCURENCE_IPFCGQ_LOSS_CLAIMANT.IPFCGQ_LOSS_OCCURENCE = IPFCGQ_LOSS_OCCURENCE
	AND LKP_42GQ_MS2_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFCGQ_YEAR_OF_LOSS_IPFCGQ_MONTH_OF_LOSS_IPFCGQ_DAY_OF_LOSS_IPFCGQ_LOSS_OCCURENCE_IPFCGQ_LOSS_CLAIMANT.IPFCGQ_LOSS_CLAIMANT = IPFCGQ_LOSS_CLAIMANT

),
FIL_INSERT_workers_comp_claimant_detail_PMS AS (
	SELECT
	CHANGE_FLAG_OP, 
	CLAIM_PARTY_OCCURRENCE_AK_ID AS claim_party_occurrence_ak_id, 
	jurisdiction_state_code_OP, 
	emplyr_notified_date, 
	rpted_to_carrier_date, 
	jurisdiction_claim_num, 
	care_directed_ind, 
	care_directed_by, 
	hired_state_code, 
	hired_date, 
	tax_filing_status, 
	occuptn_code, 
	employement_status_code, 
	len_of_time_in_crrnt_job, 
	emp_dept_name, 
	emp_shift_num, 
	marital_status, 
	num_of_dependents, 
	num_of_dependent_children, 
	num_of_other_dependents, 
	num_of_exemptions, 
	exemption_type, 
	emp_blind_ind, 
	emp_over_65_ind, 
	spouse_blind_ind, 
	spouse_over_65_ind, 
	education_lvl, 
	med_auth_ind, 
	auth_to_release_ssn_ind, 
	emp_id_num, 
	emp_id_type, 
	emp_part_time_hour_week, 
	emp_dept_num, 
	emp_part_time_hourly_week_rate_amt, 
	wage_rate_amt, 
	wage_period_code, 
	wage_eff_date, 
	weeks_worked, 
	gross_amt_type, 
	gross_wage_amt_excluding_tips, 
	piece_work_num_of_weeks_excluding_overtime, 
	emp_rec_meals, 
	emp_rec_room, 
	emp_rec_tips, 
	overtime_amt, 
	overtime_after_hour_in_a_week, 
	overtime_after_hour_in_a_day, 
	full_pay_inj_day_ind, 
	salary_paid_ind, 
	avg_full_time_days_week, 
	avg_full_time_hours_day, 
	avg_full_time_hours_week, 
	avg_wkly_wage, 
	num_of_full_time_emplymnt_same_job, 
	num_of_part_time_emplymnt_same_job, 
	ttd_rate, 
	ppd_rate, 
	ptd_rate, 
	dtd_rate, 
	wkly_attorney_fee, 
	first_rpt_inj_date, 
	supplementary_rpt_inj_date, 
	fringe_bnft_discontinued_amt, 
	emp_start_time, 
	emp_hour_day, 
	emp_hour_week, 
	emp_day_week, 
	inj_work_day_begin_time, 
	disability_date, 
	phys_restriction_ind, 
	pre_exst_disability_ind, 
	premises_code, 
	work_process_descript, 
	task_descript, 
	body_part_code, 
	nature_inj_code, 
	cause_inj_code, 
	safeguard_not_used_ind, 
	inj_substance_abuse_ind, 
	sfty_device_not_used_ind, 
	inj_rules_not_obeyed_ind, 
	inj_result_occuptnal_inj_ind, 
	inj_result_occuptnal_disease_ndicator, 
	inj_result_death_ind, 
	unsafe_act_descript, 
	responsible_for_inj_descript, 
	hazard_condition_descript, 
	death_date, 
	emplyr_nature_bus_descript, 
	emplyr_type_code, 
	insd_type_code, 
	subrogation_statute_exp_date, 
	managed_care_org_type, 
	subrogation_code, 
	loss_condition, 
	attorney_or_au_rep_ind, 
	hospital_cost, 
	doctor_cost, 
	other_med_cost, 
	controverted_case_code, 
	surgery_ind, 
	emplyr_loc_descript, 
	inj_loc_comment, 
	claim_ctgry_code, 
	act_status_code, 
	investigate_ind, 
	emplyr_standard_industry_code, 
	hospitalized_ind, 
	wage_method_code, 
	pms_occuptn_descript, 
	pms_type_disability, 
	ncci_type_cov, 
	logical_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date, 
	LKP_wc_claimant_det_ak_id AS wc_claimant_det_ak_id
	FROM EXP_DETECT_CHANGES_workers_comp_claimant_detail
	WHERE CHANGE_FLAG_OP<>'NOCHANGE'
),
SEQ_Workers_Comp_Claimant_Detail AS (
	CREATE SEQUENCE SEQ_Workers_Comp_Claimant_Detail
	START = 0
	INCREMENT = 1;
),
EXP_INSERT AS (
	SELECT
	wc_claimant_det_ak_id AS wc_claimant_det_ak_id_IN,
	CHANGE_FLAG_OP,
	claim_party_occurrence_ak_id,
	SEQ_Workers_Comp_Claimant_Detail.NEXTVAL,
	-- *INF*: IIF(CHANGE_FLAG_OP='NEW', NEXTVAL, wc_claimant_det_ak_id_IN)
	IFF(CHANGE_FLAG_OP = 'NEW',
		NEXTVAL,
		wc_claimant_det_ak_id_IN
	) AS wc_claimant_det_ak_id,
	jurisdiction_state_code_OP,
	emplyr_notified_date,
	rpted_to_carrier_date,
	jurisdiction_claim_num,
	care_directed_ind,
	care_directed_by,
	hired_state_code,
	hired_date,
	tax_filing_status,
	occuptn_code,
	employement_status_code,
	len_of_time_in_crrnt_job,
	emp_dept_name,
	emp_shift_num,
	marital_status,
	num_of_dependents,
	num_of_dependent_children,
	num_of_other_dependents,
	num_of_exemptions,
	exemption_type,
	emp_blind_ind,
	emp_over_65_ind,
	spouse_blind_ind,
	spouse_over_65_ind,
	education_lvl,
	med_auth_ind,
	auth_to_release_ssn_ind,
	emp_id_num,
	emp_id_type,
	emp_part_time_hour_week,
	emp_dept_num,
	emp_part_time_hourly_week_rate_amt,
	wage_rate_amt,
	wage_period_code,
	wage_eff_date,
	weeks_worked,
	gross_amt_type,
	gross_wage_amt_excluding_tips,
	piece_work_num_of_weeks_excluding_overtime,
	emp_rec_meals,
	emp_rec_room,
	emp_rec_tips,
	overtime_amt,
	overtime_after_hour_in_a_week,
	overtime_after_hour_in_a_day,
	full_pay_inj_day_ind,
	salary_paid_ind,
	avg_full_time_days_week,
	avg_full_time_hours_day,
	avg_full_time_hours_week,
	avg_wkly_wage,
	num_of_full_time_emplymnt_same_job,
	num_of_part_time_emplymnt_same_job,
	ttd_rate,
	ppd_rate,
	ptd_rate,
	dtd_rate,
	wkly_attorney_fee,
	first_rpt_inj_date,
	supplementary_rpt_inj_date,
	fringe_bnft_discontinued_amt,
	emp_start_time,
	emp_hour_day,
	emp_hour_week,
	emp_day_week,
	inj_work_day_begin_time,
	disability_date,
	phys_restriction_ind,
	pre_exst_disability_ind,
	premises_code,
	work_process_descript,
	task_descript,
	body_part_code,
	nature_inj_code,
	cause_inj_code,
	safeguard_not_used_ind,
	inj_substance_abuse_ind,
	sfty_device_not_used_ind,
	inj_rules_not_obeyed_ind,
	inj_result_occuptnal_inj_ind,
	inj_result_occuptnal_disease_ndicator,
	inj_result_death_ind,
	unsafe_act_descript,
	responsible_for_inj_descript,
	hazard_condition_descript,
	death_date,
	emplyr_nature_bus_descript,
	emplyr_type_code,
	insd_type_code,
	subrogation_statute_exp_date,
	managed_care_org_type,
	subrogation_code,
	loss_condition,
	attorney_or_au_rep_ind,
	hospital_cost,
	doctor_cost,
	other_med_cost,
	controverted_case_code,
	surgery_ind,
	emplyr_loc_descript,
	inj_loc_comment,
	claim_ctgry_code,
	act_status_code,
	investigate_ind,
	emplyr_standard_industry_code,
	hospitalized_ind,
	wage_method_code,
	pms_occuptn_descript,
	pms_type_disability,
	ncci_type_cov,
	logical_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	'N/A' AS DummyString,
	-- *INF*: to_date('01/01/1800','mm/dd/yyyy')
	to_date('01/01/1800', 'mm/dd/yyyy'
	) AS DummyDate,
	0.00 AS DummyDecimal,
	0 AS DummnNum,
	'N/A' AS FROIclaimType,
	-- *INF*: to_date('12/31/2100','mm/dd/yyyy')
	to_date('12/31/2100', 'mm/dd/yyyy'
	) AS DefaultHighDate
	FROM FIL_INSERT_workers_comp_claimant_detail_PMS
),
workers_comp_claimant_detail_insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.workers_comp_claimant_detail
	(wc_claimant_det_ak_id, claim_party_occurrence_ak_id, jurisdiction_state_code, emplyr_notified_date, reported_to_carrier_date, jurisdiction_claim_num, care_directed_ind, care_directed_by, hired_state_code, hired_date, tax_filing_status, occuptn_code, emplymnt_status_code, len_of_time_in_crrnt_job, emp_dept_name, emp_shift_num, marital_status, num_of_dependents, num_of_dependent_children, num_of_other_dependents, num_of_exemptions, exemption_type, emp_blind_ind, emp_over_65_ind, spouse_blind_ind, spouse_over_65_ind, education_lvl, med_auth_ind, auth_to_release_ssn_ind, emp_id_num, emp_id_type, emp_part_time_hour_week, emp_dept_num, emp_part_time_hourly_week_rate_amt, wage_rate_amt, wage_period_code, wage_eff_date, weeks_worked, gross_amt_type, gross_wage_amt_excluding_tips, piece_work_num_of_weeks_excluding_overtime, emp_rec_meals, emp_rec_room, emp_rec_tips, overtime_amt, overtime_after_hour_in_a_week, overtime_after_hour_in_a_day, full_pay_inj_day_ind, salary_paid_ind, avg_full_time_days_week, avg_full_time_hours_day, avg_full_time_hours_week, avg_wkly_wage, num_of_full_time_emplymnt_same_job, num_of_part_time_emplymnt_same_job, ttd_rate, ppd_rate, ptd_rate, dtd_rate, wkly_attorney_fee, first_rpt_inj_date, supplementary_rpt_inj_date, fringe_bnft_discontinued_amt, emp_start_time, emp_hour_day, emp_hour_week, emp_day_week, inj_work_day_begin_time, disability_date, phys_restriction_ind, pre_exst_disability_ind, premises_code, work_process_descript, task_descript, body_part_code, nature_inj_code, cause_inj_code, safeguard_not_used_ind, inj_substance_abuse_ind, sfty_device_not_used_ind, inj_rules_not_obeyed_ind, inj_result_occupational_inj_ind, inj_result_occupational_disease_ind, inj_result_death_ind, unsafe_act_descript, responsible_for_inj_descript, hazard_condition_descript, death_date, emplyr_nature_bus_descript, emplyr_type_code, insd_type_code, subrogation_statute_exp_date, managed_care_org_type, subrogation_code, loss_condition, attorney_or_au_rep_ind, hospital_cost, doctor_cost, other_med_cost, controverted_case_code, surgery_ind, emplyr_loc_descript, inj_loc_comment, claim_ctgry_code, act_status_code, investigate_ind, sic_code, hospitalized_ind, wage_method_code, pms_occuptn_descript, pms_type_disability, ncci_type_cov, logical_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, inital_treatment_code, send_to_state_ind, send_to_state_time, maint_type_code, state_claim_num, max_med_improvement_date, obtained_lgl_representation_date, sroi_send_to_state_ind, sroi_send_to_state_time, sroi_maint_type_code, sroi_state_transfer_out_ind, pts_rate, sroi_start_date, wc_claimant_num, work_week_type, work_week_days, lost_time_on_doi_ind, emp_security_id, emplyr_lost_time_notified_date, NaicsCode, FullDenialReasonCode, IAIABCLossTypeCode, FROILateReasonCode, ManualClassificationCode, FROIClaimType, AutomaticAdjudicationClaimIndicator, SupCompensableClaimCode, FROICancelReasonCode, BodyPartLocationCode, FingersToesLocationCode, FROICancelReasonNarrative, FROIFullDenialReasonNarrative, FirstDisabilityDateAfterWaitingPeriod, InjurySeverityType, LossofEarningsCapacityPercentage, PreExistingDisabilityPercentage, ActLossConditionCode, TemporaryDisabilityBenefitExtinguishmentCode, CurrentDateDisabilityBegan, DateEmployerHadKnowledgeofCurrentDateofDisability, DateWBMIHadKnowledgeofCurrentDateofDisability)
	SELECT 
	WC_CLAIMANT_DET_AK_ID, 
	CLAIM_PARTY_OCCURRENCE_AK_ID, 
	jurisdiction_state_code_OP AS JURISDICTION_STATE_CODE, 
	EMPLYR_NOTIFIED_DATE, 
	rpted_to_carrier_date AS REPORTED_TO_CARRIER_DATE, 
	JURISDICTION_CLAIM_NUM, 
	CARE_DIRECTED_IND, 
	CARE_DIRECTED_BY, 
	HIRED_STATE_CODE, 
	HIRED_DATE, 
	TAX_FILING_STATUS, 
	OCCUPTN_CODE, 
	employement_status_code AS EMPLYMNT_STATUS_CODE, 
	LEN_OF_TIME_IN_CRRNT_JOB, 
	EMP_DEPT_NAME, 
	EMP_SHIFT_NUM, 
	MARITAL_STATUS, 
	NUM_OF_DEPENDENTS, 
	NUM_OF_DEPENDENT_CHILDREN, 
	NUM_OF_OTHER_DEPENDENTS, 
	NUM_OF_EXEMPTIONS, 
	EXEMPTION_TYPE, 
	EMP_BLIND_IND, 
	EMP_OVER_65_IND, 
	SPOUSE_BLIND_IND, 
	SPOUSE_OVER_65_IND, 
	EDUCATION_LVL, 
	MED_AUTH_IND, 
	AUTH_TO_RELEASE_SSN_IND, 
	EMP_ID_NUM, 
	EMP_ID_TYPE, 
	EMP_PART_TIME_HOUR_WEEK, 
	EMP_DEPT_NUM, 
	EMP_PART_TIME_HOURLY_WEEK_RATE_AMT, 
	WAGE_RATE_AMT, 
	WAGE_PERIOD_CODE, 
	WAGE_EFF_DATE, 
	WEEKS_WORKED, 
	GROSS_AMT_TYPE, 
	GROSS_WAGE_AMT_EXCLUDING_TIPS, 
	PIECE_WORK_NUM_OF_WEEKS_EXCLUDING_OVERTIME, 
	EMP_REC_MEALS, 
	EMP_REC_ROOM, 
	EMP_REC_TIPS, 
	OVERTIME_AMT, 
	OVERTIME_AFTER_HOUR_IN_A_WEEK, 
	OVERTIME_AFTER_HOUR_IN_A_DAY, 
	FULL_PAY_INJ_DAY_IND, 
	SALARY_PAID_IND, 
	AVG_FULL_TIME_DAYS_WEEK, 
	AVG_FULL_TIME_HOURS_DAY, 
	AVG_FULL_TIME_HOURS_WEEK, 
	AVG_WKLY_WAGE, 
	NUM_OF_FULL_TIME_EMPLYMNT_SAME_JOB, 
	NUM_OF_PART_TIME_EMPLYMNT_SAME_JOB, 
	TTD_RATE, 
	PPD_RATE, 
	PTD_RATE, 
	DTD_RATE, 
	WKLY_ATTORNEY_FEE, 
	FIRST_RPT_INJ_DATE, 
	SUPPLEMENTARY_RPT_INJ_DATE, 
	FRINGE_BNFT_DISCONTINUED_AMT, 
	EMP_START_TIME, 
	EMP_HOUR_DAY, 
	EMP_HOUR_WEEK, 
	EMP_DAY_WEEK, 
	INJ_WORK_DAY_BEGIN_TIME, 
	DISABILITY_DATE, 
	PHYS_RESTRICTION_IND, 
	PRE_EXST_DISABILITY_IND, 
	PREMISES_CODE, 
	WORK_PROCESS_DESCRIPT, 
	TASK_DESCRIPT, 
	BODY_PART_CODE, 
	NATURE_INJ_CODE, 
	CAUSE_INJ_CODE, 
	SAFEGUARD_NOT_USED_IND, 
	INJ_SUBSTANCE_ABUSE_IND, 
	SFTY_DEVICE_NOT_USED_IND, 
	INJ_RULES_NOT_OBEYED_IND, 
	inj_result_occuptnal_inj_ind AS INJ_RESULT_OCCUPATIONAL_INJ_IND, 
	inj_result_occuptnal_disease_ndicator AS INJ_RESULT_OCCUPATIONAL_DISEASE_IND, 
	INJ_RESULT_DEATH_IND, 
	UNSAFE_ACT_DESCRIPT, 
	RESPONSIBLE_FOR_INJ_DESCRIPT, 
	HAZARD_CONDITION_DESCRIPT, 
	DEATH_DATE, 
	EMPLYR_NATURE_BUS_DESCRIPT, 
	EMPLYR_TYPE_CODE, 
	INSD_TYPE_CODE, 
	SUBROGATION_STATUTE_EXP_DATE, 
	MANAGED_CARE_ORG_TYPE, 
	SUBROGATION_CODE, 
	LOSS_CONDITION, 
	ATTORNEY_OR_AU_REP_IND, 
	HOSPITAL_COST, 
	DOCTOR_COST, 
	OTHER_MED_COST, 
	CONTROVERTED_CASE_CODE, 
	SURGERY_IND, 
	EMPLYR_LOC_DESCRIPT, 
	INJ_LOC_COMMENT, 
	CLAIM_CTGRY_CODE, 
	ACT_STATUS_CODE, 
	INVESTIGATE_IND, 
	emplyr_standard_industry_code AS SIC_CODE, 
	HOSPITALIZED_IND, 
	WAGE_METHOD_CODE, 
	PMS_OCCUPTN_DESCRIPT, 
	PMS_TYPE_DISABILITY, 
	NCCI_TYPE_COV, 
	LOGICAL_FLAG, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	DummyString AS INITAL_TREATMENT_CODE, 
	DummyString AS SEND_TO_STATE_IND, 
	DummyDate AS SEND_TO_STATE_TIME, 
	DummyString AS MAINT_TYPE_CODE, 
	DummyString AS STATE_CLAIM_NUM, 
	DummyDate AS MAX_MED_IMPROVEMENT_DATE, 
	DummyDate AS OBTAINED_LGL_REPRESENTATION_DATE, 
	DummyString AS SROI_SEND_TO_STATE_IND, 
	DummyDate AS SROI_SEND_TO_STATE_TIME, 
	DummyString AS SROI_MAINT_TYPE_CODE, 
	DummyString AS SROI_STATE_TRANSFER_OUT_IND, 
	DummnNum AS PTS_RATE, 
	DummyDate AS SROI_START_DATE, 
	DummyString AS WC_CLAIMANT_NUM, 
	DummyString AS WORK_WEEK_TYPE, 
	DummyString AS WORK_WEEK_DAYS, 
	DummyString AS LOST_TIME_ON_DOI_IND, 
	DummyString AS EMP_SECURITY_ID, 
	DummyDate AS EMPLYR_LOST_TIME_NOTIFIED_DATE, 
	DummyString AS NAICSCODE, 
	DummyString AS FULLDENIALREASONCODE, 
	DummyString AS IAIABCLOSSTYPECODE, 
	DummyString AS FROILATEREASONCODE, 
	DummyString AS MANUALCLASSIFICATIONCODE, 
	FROIclaimType AS FROICLAIMTYPE, 
	DummyString AS AUTOMATICADJUDICATIONCLAIMINDICATOR, 
	DummyString AS SUPCOMPENSABLECLAIMCODE, 
	DummyString AS FROICANCELREASONCODE, 
	DummyString AS BODYPARTLOCATIONCODE, 
	DummyString AS FINGERSTOESLOCATIONCODE, 
	DummyString AS FROICANCELREASONNARRATIVE, 
	DummyString AS FROIFULLDENIALREASONNARRATIVE, 
	DummyDate AS FIRSTDISABILITYDATEAFTERWAITINGPERIOD, 
	DummyString AS INJURYSEVERITYTYPE, 
	DummyDecimal AS LOSSOFEARNINGSCAPACITYPERCENTAGE, 
	DummyDecimal AS PREEXISTINGDISABILITYPERCENTAGE, 
	DummyString AS ACTLOSSCONDITIONCODE, 
	DummyString AS TEMPORARYDISABILITYBENEFITEXTINGUISHMENTCODE, 
	DefaultHighDate AS CURRENTDATEDISABILITYBEGAN, 
	DefaultHighDate AS DATEEMPLOYERHADKNOWLEDGEOFCURRENTDATEOFDISABILITY, 
	DefaultHighDate AS DATEWBMIHADKNOWLEDGEOFCURRENTDATEOFDISABILITY
	FROM EXP_INSERT
),
SQ_workers_comp_claimant_detail AS (
	SELECT a.wc_claimant_det_id, a.wc_claimant_det_ak_id, a.eff_from_date, a.eff_to_date 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.workers_comp_claimant_detail a
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.workers_comp_claimant_detail b
			WHERE b.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND b.crrnt_snpsht_flag = 1
			AND a.wc_claimant_det_ak_id = b.wc_claimant_det_ak_id
			GROUP BY wc_claimant_det_ak_id
			HAVING COUNT(*) > 1)
	ORDER BY wc_claimant_det_ak_id, eff_from_date  DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	wc_claimant_det_id,
	wc_claimant_det_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: IIF(wc_claimant_det_ak_id = v_PREV_ROW_wc_claimant_det_ak_id, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),orig_eff_to_date)
	IFF(wc_claimant_det_ak_id = v_PREV_ROW_wc_claimant_det_ak_id,
		DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	wc_claimant_det_ak_id AS v_PREV_ROW_wc_claimant_det_ak_id,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_workers_comp_claimant_detail
),
FIL_FirstRowInAKGroup AS (
	SELECT
	wc_claimant_det_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <> eff_to_date

--If these two dates equal each other we are dealing with the first row in an AK group.  This row
--does not need to be expired or updated for any reason thus it can be filtered out
-- but we must source it to capture the eff_from_date of this row 
--so that we can properly expire the subsequent row
),
UPD_workers_comp_claimant_detail_PMS AS (
	SELECT
	wc_claimant_det_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FIL_FirstRowInAKGroup
),
workers_comp_claimant_detail_update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.workers_comp_claimant_detail AS T
	USING UPD_workers_comp_claimant_detail_PMS AS S
	ON T.wc_claimant_det_id = S.wc_claimant_det_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),
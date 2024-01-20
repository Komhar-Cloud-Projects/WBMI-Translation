WITH
LKP_Claim_Party_Occurrence_AK_ID AS (
	SELECT
	claim_party_occurrence_ak_id,
	claimant_num
	FROM (
		SELECT CPO.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, 
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
		AND CPO.claim_party_role_code = 'CLMT'
		AND CO.crrnt_snpsht_flag = 1
		AND CP.crrnt_snpsht_flag = 1
		AND CPO.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_num ORDER BY claim_party_occurrence_ak_id) = 1
),
SEQ_Claim_payment_category_AK_ID1 AS (
	CREATE SEQUENCE SEQ_Claim_payment_category_AK_ID1
	START = 0
	INCREMENT = 1;
),
SQ_CLAIM_TRANSACTION_STAGE AS (
	SELECT CT.ctx_fin_type_cd, CT.ctx_draft_nbr,CT.ctx_trs_cat_cd, CT.ctx_trs_amt ,
	CASE 
	 WHEN ctx_trs_amt < 0  THEN -(ctx_cost_cont_sav)  ELSE ctx_cost_cont_sav END as ctx_cost_cont_sav 
	FROM
	 claim_transaction_stage CT
	WHERE CT.ctx_cost_cont_sav IS NOT NULL and CT.ctx_cost_cont_sav <> 0
),
EXP_get_vlayes_txn AS (
	SELECT
	ctx_trs_cat_cd,
	ctx_fin_type_cd,
	ctx_trs_amt,
	ctx_draft_nbr,
	ctx_cost_cont_sav
	FROM SQ_CLAIM_TRANSACTION_STAGE
),
AGG_txn AS (
	SELECT
	ctx_fin_type_cd,
	ctx_draft_nbr,
	ctx_cost_cont_sav,
	-- *INF*: SUM(ctx_cost_cont_sav)
	SUM(ctx_cost_cont_sav) AS ctx_cost_cont_sav_out
	FROM EXP_get_vlayes_txn
	GROUP BY ctx_fin_type_cd, ctx_draft_nbr
),
LKP_claim_payment_exd_txn AS (
	SELECT
	claim_pay_ak_id,
	claim_pay_num,
	IN_CTX_DRAFT_NBR
	FROM (
		SELECT claim_payment.claim_pay_ak_id as claim_pay_ak_id, claim_payment.claim_pay_num as claim_pay_num FROM claim_payment
		where source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_pay_num ORDER BY claim_pay_ak_id) = 1
),
EXP_set_defaults_txn AS (
	SELECT
	LKP_claim_payment_exd_txn.claim_pay_ak_id AS in_claim_pay_ak_id,
	-- *INF*: IIF(ISNULL(in_claim_pay_ak_id), -1, in_claim_pay_ak_id)
	IFF(in_claim_pay_ak_id IS NULL, - 1, in_claim_pay_ak_id) AS claim_pay_ak_id,
	'CC' AS ctx_trs_cat_cd,
	-1 AS CTB_BENE_SEQ_NUM,
	AGG_txn.ctx_fin_type_cd AS IN_CTB_FIN_TYPE_CD,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_CTB_FIN_TYPE_CD))) OR LENGTH(LTRIM(RTRIM(IN_CTB_FIN_TYPE_CD))) = 0, 'N/A', LTRIM(RTRIM(IN_CTB_FIN_TYPE_CD)))
	IFF(
	    LTRIM(RTRIM(IN_CTB_FIN_TYPE_CD)) IS NULL OR LENGTH(LTRIM(RTRIM(IN_CTB_FIN_TYPE_CD))) = 0,
	    'N/A',
	    LTRIM(RTRIM(IN_CTB_FIN_TYPE_CD))
	) AS CTB_FIN_TYPE_CD,
	AGG_txn.ctx_cost_cont_sav_out AS IN_CTB_COST_CONT_SAV,
	-- *INF*: IIF(ISNULL(IN_CTB_COST_CONT_SAV), 0, IN_CTB_COST_CONT_SAV)
	IFF(IN_CTB_COST_CONT_SAV IS NULL, 0, IN_CTB_COST_CONT_SAV) AS CTB_COST_CONT_SAV,
	0 AS AMT_default,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS CTB_BENEFIT_START,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS CTB_BENEFIT_END,
	'N/A' AS char_default,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	'N/A' AS claim_pay_ctgry_litigated_ind,
	'N/A' AS claim_pay_ctgry_lump_sum_ind,
	'N/A' AS cov_ctgry_code,
	'0' AS BenefitTypeCode,
	0.0 AS BenefitOffsetAmount
	FROM AGG_txn
	LEFT JOIN LKP_claim_payment_exd_txn
	ON LKP_claim_payment_exd_txn.claim_pay_num = AGG_txn.ctx_draft_nbr
),
LKP_payment_category_txn AS (
	SELECT
	claim_pay_ctgry_id,
	claim_pay_ak_id,
	claim_pay_ctgry_type,
	claim_pay_ctgry_seq_num,
	IN_claim_pay_ak_id,
	IN_CTB_BENEFIT_TYPE,
	IN_CTB_BENE_SEQ_NUM
	FROM (
		SELECT 
			claim_pay_ctgry_id,
			claim_pay_ak_id,
			claim_pay_ctgry_type,
			claim_pay_ctgry_seq_num,
			IN_claim_pay_ak_id,
			IN_CTB_BENEFIT_TYPE,
			IN_CTB_BENE_SEQ_NUM
		FROM claim_payment_category
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_pay_ak_id,claim_pay_ctgry_type,claim_pay_ctgry_seq_num ORDER BY claim_pay_ctgry_id) = 1
),
RTR_claim_payment_category_txn AS (
	SELECT
	LKP_payment_category_txn.claim_pay_ctgry_id,
	EXP_set_defaults_txn.claim_pay_ak_id,
	EXP_set_defaults_txn.ctx_trs_cat_cd,
	EXP_set_defaults_txn.CTB_BENE_SEQ_NUM,
	EXP_set_defaults_txn.CTB_FIN_TYPE_CD,
	EXP_set_defaults_txn.CTB_COST_CONT_SAV,
	EXP_set_defaults_txn.AMT_default,
	EXP_set_defaults_txn.CTB_BENEFIT_START,
	EXP_set_defaults_txn.CTB_BENEFIT_END,
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
	EXP_set_defaults_txn.BenefitTypeCode,
	EXP_set_defaults_txn.BenefitOffsetAmount
	FROM EXP_set_defaults_txn
	LEFT JOIN LKP_payment_category_txn
	ON LKP_payment_category_txn.claim_pay_ak_id = EXP_set_defaults_txn.claim_pay_ak_id AND LKP_payment_category_txn.claim_pay_ctgry_type = EXP_set_defaults_txn.ctx_trs_cat_cd AND LKP_payment_category_txn.claim_pay_ctgry_seq_num = EXP_set_defaults_txn.CTB_BENE_SEQ_NUM
),
RTR_claim_payment_category_txn_Insert AS (SELECT * FROM RTR_claim_payment_category_txn WHERE ISNULL(claim_pay_ctgry_id)),
RTR_claim_payment_category_txn_Update AS (SELECT * FROM RTR_claim_payment_category_txn WHERE NOT ISNULL(claim_pay_ctgry_id)),
UPD_claim_payment_category_txn_insert AS (
	SELECT
	claim_pay_ctgry_id AS claim_pay_ctgry_id1, 
	claim_pay_ak_id AS claim_pay_ak_id1, 
	ctx_trs_cat_cd AS ctx_trs_cat_cd1, 
	CTB_BENE_SEQ_NUM AS CTB_BENE_SEQ_NUM1, 
	CTB_FIN_TYPE_CD AS CTB_FIN_TYPE_CD1, 
	CTB_COST_CONT_SAV AS CTB_COST_CONT_SAV1, 
	AMT_default AS AMT_default1, 
	CTB_BENEFIT_START AS CTB_BENEFIT_START1, 
	CTB_BENEFIT_END AS CTB_BENEFIT_END1, 
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
	BenefitTypeCode AS BenefitTypeCode1, 
	BenefitOffsetAmount AS BenefitOffsetAmount1
	FROM RTR_claim_payment_category_txn_Insert
),
claim_payment_category_txn_insert AS (
	INSERT INTO claim_payment_category
	(claim_pay_ctgry_ak_id, claim_pay_ak_id, claim_pay_ctgry_type, claim_pay_ctgry_seq_num, claim_pay_ctgry_amt, claim_pay_ctgry_earned_amt, claim_pay_ctgry_billed_amt, claim_pay_ctgry_start_date, claim_pay_ctgry_end_date, financial_type_code, invc_num, cost_containment_saving_amt, cost_containment_red_amt, cost_containment_ppo_amt, attorney_fee_amt, attorney_cost_amt, attorney_file_num, hourly_rate, hours_worked, num_of_days, num_of_weeks, tpd_rate, tpd_rate_fac, tpd_wage_loss, tpd_wkly_wage, claim_pay_ctgry_comment, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, claim_pay_ctgry_litigated_ind, claim_pay_ctgry_lump_sum_ind, cov_ctgry_code, BenefitOffsetCode, BenefitOffsetAmount)
	SELECT 
	SEQ_Claim_payment_category_AK_ID1.NEXTVAL AS CLAIM_PAY_CTGRY_AK_ID, 
	claim_pay_ak_id1 AS CLAIM_PAY_AK_ID, 
	ctx_trs_cat_cd1 AS CLAIM_PAY_CTGRY_TYPE, 
	CTB_BENE_SEQ_NUM1 AS CLAIM_PAY_CTGRY_SEQ_NUM, 
	AMT_default1 AS CLAIM_PAY_CTGRY_AMT, 
	AMT_default1 AS CLAIM_PAY_CTGRY_EARNED_AMT, 
	AMT_default1 AS CLAIM_PAY_CTGRY_BILLED_AMT, 
	CTB_BENEFIT_START1 AS CLAIM_PAY_CTGRY_START_DATE, 
	CTB_BENEFIT_END1 AS CLAIM_PAY_CTGRY_END_DATE, 
	CTB_FIN_TYPE_CD1 AS FINANCIAL_TYPE_CODE, 
	char_default1 AS INVC_NUM, 
	CTB_COST_CONT_SAV1 AS COST_CONTAINMENT_SAVING_AMT, 
	AMT_default1 AS COST_CONTAINMENT_RED_AMT, 
	AMT_default1 AS COST_CONTAINMENT_PPO_AMT, 
	AMT_default1 AS ATTORNEY_FEE_AMT, 
	AMT_default1 AS ATTORNEY_COST_AMT, 
	char_default1 AS ATTORNEY_FILE_NUM, 
	AMT_default1 AS HOURLY_RATE, 
	AMT_default1 AS HOURS_WORKED, 
	CTB_BENE_SEQ_NUM1 AS NUM_OF_DAYS, 
	CTB_BENE_SEQ_NUM1 AS NUM_OF_WEEKS, 
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
	BenefitTypeCode1 AS BENEFITOFFSETCODE, 
	BenefitOffsetAmount1 AS BENEFITOFFSETAMOUNT
	FROM UPD_claim_payment_category_txn_insert
),
UPD_claim_payment_category_txn_update AS (
	SELECT
	claim_pay_ctgry_id AS claim_pay_ctgry_id3, 
	claim_pay_ak_id AS claim_pay_ak_id3, 
	ctx_trs_cat_cd AS ctx_trs_cat_cd3, 
	CTB_BENE_SEQ_NUM AS CTB_BENE_SEQ_NUM3, 
	CTB_FIN_TYPE_CD AS CTB_FIN_TYPE_CD3, 
	CTB_COST_CONT_SAV AS CTB_COST_CONT_SAV3, 
	AMT_default AS AMT_default3, 
	CTB_BENEFIT_START AS CTB_BENEFIT_START3, 
	CTB_BENEFIT_END AS CTB_BENEFIT_END3, 
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
	cov_ctgry_code
	FROM RTR_claim_payment_category_txn_Update
),
claim_payment_category_txn_update AS (
	MERGE INTO claim_payment_category AS T
	USING UPD_claim_payment_category_txn_update AS S
	ON T.claim_pay_ctgry_id = S.claim_pay_ctgry_id3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.cost_containment_saving_amt = S.CTB_COST_CONT_SAV3, T.modified_date = S.modified_date3
),
SQ_CLM_TRANS_BENEFIT_STAGE AS (
	SELECT 
	CTB.CTB_CLAIM_NBR, 
	CTB.CTB_DRAFT_NBR, 
	CTB.CTB_BENEFIT_TYPE,
	 CTB.CTB_BENE_SEQ_NUM,  
	CTB.CTB_FIN_TYPE_CD, 
	CTB.CTB_CREATE_TS as CTB_CREATE_TS ,
	CTB.CTB_BENEFIT_START, CTB.CTB_BENEFIT_END, CTB.CTB_BENEFIT_AMT, CTB.CTB_NUM_WEEKS, 
	CTB.CTB_NUM_DAYS, CTB.CTB_HOURS_WORKED, CTB.CTB_HOURLY_RATE, CTB.CTB_TPD_WKLY_WAGE, 
	CTB.CTB_TPD_WAGE_LOSS, CTB.CTB_TPD_RATE_FCTR, CTB.CTB_TPD_RATE, CTB.CTB_INVOICE_NBR, 
	CTB.CTB_COST_CONT_SAV, CTB.CTB_BILLED_AMT, CTB.CTB_COST_CONT_PPO, CTB.CTB_COST_CONT_RED, 
	CTB.CTB_ATY_FILE_NUM, CTB.CTB_ATY_FEE_AMT, CTB.CTB_ATY_COSTS_AMT, CTB.CTB_AMT_EARNED, 
	CTB.CTB_BENE_MIS_CMT ,
	CTB.CTB_BENE_LITIGATED,
	CTB.CTB_LUMP_SUM_IND,
	CTB.CTB_BENEFIT_OFFSET_CD, 
	CTB.CTB_BENEFIT_OFFSET_AMT 
	FROM
	 CLM_TRANS_BENEFIT_STAGE CTB
),
EXP_get_values_benefit AS (
	SELECT
	CTB_CLAIM_NBR,
	CTB_DRAFT_NBR AS IN_CTB_DRAFT_NBR,
	-- *INF*: IIF(ISNULL(IN_CTB_DRAFT_NBR) OR IS_SPACES(LTRIM(RTRIM(IN_CTB_DRAFT_NBR))) OR LENGTH(LTRIM(RTRIM(IN_CTB_DRAFT_NBR))) = 0, 'N/A', IN_CTB_DRAFT_NBR)
	IFF(
	    IN_CTB_DRAFT_NBR IS NULL
	    or LENGTH(LTRIM(RTRIM(IN_CTB_DRAFT_NBR)))>0
	    and TRIM(LTRIM(RTRIM(IN_CTB_DRAFT_NBR)))=''
	    or LENGTH(LTRIM(RTRIM(IN_CTB_DRAFT_NBR))) = 0,
	    'N/A',
	    IN_CTB_DRAFT_NBR
	) AS CTB_DRAFT_NBR,
	CTB_BENEFIT_TYPE,
	CTB_BENE_SEQ_NUM,
	CTB_BENEFIT_AMT,
	CTB_AMT_EARNED,
	CTB_BILLED_AMT,
	CTB_BENEFIT_START,
	CTB_BENEFIT_END,
	CTB_FIN_TYPE_CD,
	CTB_INVOICE_NBR,
	CTB_COST_CONT_SAV,
	CTB_COST_CONT_RED,
	CTB_COST_CONT_PPO,
	CTB_ATY_FEE_AMT,
	CTB_ATY_COSTS_AMT,
	CTB_ATY_FILE_NUM,
	CTB_HOURLY_RATE,
	CTB_HOURS_WORKED,
	CTB_NUM_DAYS,
	CTB_NUM_WEEKS,
	CTB_TPD_RATE,
	CTB_TPD_RATE_FCTR,
	CTB_TPD_WAGE_LOSS,
	CTB_TPD_WKLY_WAGE,
	CTB_BENE_MIS_CMT,
	ctb_bene_litigated AS IN_ctb_bene_litigated,
	-- *INF*: IIF(ISNULL(IN_ctb_bene_litigated) OR IS_SPACES(LTRIM(RTRIM(IN_ctb_bene_litigated))) OR LENGTH(LTRIM(RTRIM(IN_ctb_bene_litigated))) = 0, 'N/A', IN_ctb_bene_litigated)
	-- 
	-- 
	-- 
	IFF(
	    IN_ctb_bene_litigated IS NULL
	    or LENGTH(LTRIM(RTRIM(IN_ctb_bene_litigated)))>0
	    and TRIM(LTRIM(RTRIM(IN_ctb_bene_litigated)))=''
	    or LENGTH(LTRIM(RTRIM(IN_ctb_bene_litigated))) = 0,
	    'N/A',
	    IN_ctb_bene_litigated
	) AS CLAIM_PAY_CTGRY_LITIGATED_IND,
	CTB_CREATE_TS AS IN_CTB_CREATE_TS,
	-- *INF*: IIF(ISNULL(IN_CTB_CREATE_TS),TO_DATE('1/1/1800','MM/DD/YYYY'),IN_CTB_CREATE_TS)
	IFF(IN_CTB_CREATE_TS IS NULL, TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'), IN_CTB_CREATE_TS) AS CTB_CREATE_TS,
	ctb_lump_sum_ind AS IN_ctb_lump_sum_ind,
	-- *INF*: IIF(ISNULL(IN_ctb_lump_sum_ind) OR IS_SPACES(LTRIM(RTRIM(IN_ctb_lump_sum_ind))) OR LENGTH(LTRIM(RTRIM(IN_ctb_lump_sum_ind))) = 0, 'N/A', IN_ctb_lump_sum_ind)
	-- 
	-- 
	-- 
	IFF(
	    IN_ctb_lump_sum_ind IS NULL
	    or LENGTH(LTRIM(RTRIM(IN_ctb_lump_sum_ind)))>0
	    and TRIM(LTRIM(RTRIM(IN_ctb_lump_sum_ind)))=''
	    or LENGTH(LTRIM(RTRIM(IN_ctb_lump_sum_ind))) = 0,
	    'N/A',
	    IN_ctb_lump_sum_ind
	) AS ctb_lump_sum_ind,
	ctb_benefit_offset_cd,
	-- *INF*: IIF(ISNULL(ctb_benefit_offset_cd),'0', ctb_benefit_offset_cd )
	-- 
	-- 
	-- --DECODE(TRUE,
	-- --ISNULL(ctb_benefit_offset_cd),'N/A',
	-- --ctb_benefit_offset_cd = '0','UNKNOWN',
	-- --ctb_benefit_offset_cd = '1','NONE',
	-- --ctb_benefit_offset_cd = '2','SSDI',
	-- --ctb_benefit_offset_cd = '3','OTHER',
	-- --'N/A
	IFF(ctb_benefit_offset_cd IS NULL, '0', ctb_benefit_offset_cd) AS o_ctb_benefit_offset_cd,
	ctb_benefit_offset_amt,
	-- *INF*: IIF(ISNULL(ctb_benefit_offset_amt), 0.00, ctb_benefit_offset_amt)
	IFF(ctb_benefit_offset_amt IS NULL, 0.00, ctb_benefit_offset_amt) AS o_ctb_benefit_offset_amt
	FROM SQ_CLM_TRANS_BENEFIT_STAGE
),
LKP_CLAIM_OCCURRENCE AS (
	SELECT
	claim_occurrence_key
	FROM (
		SELECT 
		      claim_occurrence.claim_occurrence_key as claim_occurrence_key 
		FROM 
		   claim_occurrence
		WHERE
		   source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_key ORDER BY claim_occurrence_key DESC) = 1
),
LKP_COV_CTGRY_CODE AS (
	SELECT
	category_code,
	claim_nbr
	FROM (
		select distinct a.cvr_claim_nbr as  claim_nbr ,
		 g.cov_category_code                as  category_code
		from  claim_coverage_stage a  ,  --- new used
		          clm_trans_benefit_stage c,  --- not used
		          cause_of_loss_stage d,      ---- MOD  - New column
		           sup_benefit_type_stage e, --- NOT new used
		           pc_benefit_filter_stage f,--- new used
		           pc_bnft_fltr_typs_stage g  --- new used
		WHERE a.cvr_claim_nbr =c.ctb_claim_nbr   ----------------------
		 AND a.cvr_line_of_bus_cd = d.line_of_business  
		AND   c.ctb_cov_type_cd = d.major_peril  
		AND   substring(c.ctb_bur_cause_loss,1,2) = d.cause_of_loss
		AND   c.ctb_benefit_type = e.code
		AND   e.code = f.benefit_code  
		AND   f.filter_type = g.filter_type 
		AND   c.ctb_fin_type_cd = g.fin_type_cd
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_nbr ORDER BY category_code DESC) = 1
),
LKP_claim_payment_exd_benefit AS (
	SELECT
	claim_pay_ak_id,
	claim_pay_num,
	IN_CTX_DRAFT_NBR
	FROM (
		SELECT claim_payment.claim_pay_ak_id as claim_pay_ak_id, claim_payment.claim_pay_num as claim_pay_num FROM claim_payment
		where source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_pay_num ORDER BY claim_pay_ak_id) = 1
),
EXP_set_defaults_benefit AS (
	SELECT
	LKP_claim_payment_exd_benefit.claim_pay_ak_id AS in_claim_pay_ak_id,
	-- *INF*: IIF(ISNULL(in_claim_pay_ak_id), -1, in_claim_pay_ak_id)
	IFF(in_claim_pay_ak_id IS NULL, - 1, in_claim_pay_ak_id) AS claim_pay_ak_id,
	EXP_get_values_benefit.CTB_BENEFIT_TYPE AS IN_CTB_BENEFIT_TYPE,
	-- *INF*: IIF(ISNULL(IN_CTB_BENEFIT_TYPE) OR IS_SPACES(LTRIM(RTRIM(IN_CTB_BENEFIT_TYPE))) OR LENGTH(LTRIM(RTRIM(IN_CTB_BENEFIT_TYPE))) = 0, 'N/A', LTRIM(RTRIM(IN_CTB_BENEFIT_TYPE)))
	IFF(
	    IN_CTB_BENEFIT_TYPE IS NULL
	    or LENGTH(LTRIM(RTRIM(IN_CTB_BENEFIT_TYPE)))>0
	    and TRIM(LTRIM(RTRIM(IN_CTB_BENEFIT_TYPE)))=''
	    or LENGTH(LTRIM(RTRIM(IN_CTB_BENEFIT_TYPE))) = 0,
	    'N/A',
	    LTRIM(RTRIM(IN_CTB_BENEFIT_TYPE))
	) AS CTB_BENEFIT_TYPE,
	EXP_get_values_benefit.CTB_BENE_SEQ_NUM AS IN_CTB_BENE_SEQ_NUM,
	-- *INF*: IIF(ISNULL(IN_CTB_BENE_SEQ_NUM), -1, IN_CTB_BENE_SEQ_NUM)
	IFF(IN_CTB_BENE_SEQ_NUM IS NULL, - 1, IN_CTB_BENE_SEQ_NUM) AS CTB_BENE_SEQ_NUM,
	EXP_get_values_benefit.CTB_BENEFIT_AMT AS IN_CTB_BENEFIT_AMT,
	-- *INF*: IIF(ISNULL(IN_CTB_BENEFIT_AMT), 0, IN_CTB_BENEFIT_AMT)
	IFF(IN_CTB_BENEFIT_AMT IS NULL, 0, IN_CTB_BENEFIT_AMT) AS CTB_BENEFIT_AMT,
	EXP_get_values_benefit.CTB_AMT_EARNED AS IN_CTB_AMT_EARNED,
	-- *INF*: IIF(ISNULL(IN_CTB_AMT_EARNED), 0, IN_CTB_AMT_EARNED)
	IFF(IN_CTB_AMT_EARNED IS NULL, 0, IN_CTB_AMT_EARNED) AS CTB_AMT_EARNED,
	EXP_get_values_benefit.CTB_BILLED_AMT AS IN_CTB_BILLED_AMT,
	-- *INF*: IIF(ISNULL(IN_CTB_BILLED_AMT), 0, IN_CTB_BILLED_AMT)
	IFF(IN_CTB_BILLED_AMT IS NULL, 0, IN_CTB_BILLED_AMT) AS CTB_BILLED_AMT,
	EXP_get_values_benefit.CTB_BENEFIT_START AS IN_CTB_BENEFIT_START,
	-- *INF*: IIF(ISNULL(IN_CTB_BENEFIT_START), TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'), IN_CTB_BENEFIT_START)
	-- 
	-- --OR NOT IS_DATE(IN_CTB_BENEFIT_START)
	IFF(
	    IN_CTB_BENEFIT_START IS NULL, TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    IN_CTB_BENEFIT_START
	) AS CTB_BENEFIT_START,
	EXP_get_values_benefit.CTB_BENEFIT_END AS IN_CTB_BENEFIT_END,
	-- *INF*: IIF(ISNULL(IN_CTB_BENEFIT_END), TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS'), IN_CTB_BENEFIT_END)
	IFF(
	    IN_CTB_BENEFIT_END IS NULL, TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'),
	    IN_CTB_BENEFIT_END
	) AS CTB_BENEFIT_END,
	EXP_get_values_benefit.CTB_FIN_TYPE_CD AS IN_CTB_FIN_TYPE_CD,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_CTB_FIN_TYPE_CD))) OR LENGTH(LTRIM(RTRIM(IN_CTB_FIN_TYPE_CD))) = 0, 'N/A', LTRIM(RTRIM(IN_CTB_FIN_TYPE_CD)))
	IFF(
	    LTRIM(RTRIM(IN_CTB_FIN_TYPE_CD)) IS NULL OR LENGTH(LTRIM(RTRIM(IN_CTB_FIN_TYPE_CD))) = 0,
	    'N/A',
	    LTRIM(RTRIM(IN_CTB_FIN_TYPE_CD))
	) AS CTB_FIN_TYPE_CD,
	EXP_get_values_benefit.CTB_INVOICE_NBR AS IN_CTB_INVOICE_NBR,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_CTB_INVOICE_NBR))) OR LENGTH(LTRIM(RTRIM(IN_CTB_INVOICE_NBR))) = 0, 'N/A', LTRIM(RTRIM(IN_CTB_INVOICE_NBR)))
	-- 
	-- 
	-- 
	IFF(
	    LTRIM(RTRIM(IN_CTB_INVOICE_NBR)) IS NULL OR LENGTH(LTRIM(RTRIM(IN_CTB_INVOICE_NBR))) = 0,
	    'N/A',
	    LTRIM(RTRIM(IN_CTB_INVOICE_NBR))
	) AS CTB_INVOICE_NBR,
	EXP_get_values_benefit.CTB_COST_CONT_SAV AS IN_CTB_COST_CONT_SAV,
	-- *INF*: IIF(ISNULL(IN_CTB_COST_CONT_SAV), 0, IN_CTB_COST_CONT_SAV)
	IFF(IN_CTB_COST_CONT_SAV IS NULL, 0, IN_CTB_COST_CONT_SAV) AS CTB_COST_CONT_SAV,
	EXP_get_values_benefit.CTB_COST_CONT_RED AS IN_CTB_COST_CONT_RED,
	-- *INF*: IIF(ISNULL(IN_CTB_COST_CONT_RED), 0, IN_CTB_COST_CONT_RED)
	IFF(IN_CTB_COST_CONT_RED IS NULL, 0, IN_CTB_COST_CONT_RED) AS CTB_COST_CONT_RED,
	EXP_get_values_benefit.CTB_COST_CONT_PPO AS IN_CTB_COST_CONT_PPO,
	-- *INF*: IIF(ISNULL(IN_CTB_COST_CONT_PPO), 0, IN_CTB_COST_CONT_PPO)
	IFF(IN_CTB_COST_CONT_PPO IS NULL, 0, IN_CTB_COST_CONT_PPO) AS CTB_COST_CONT_PPO,
	EXP_get_values_benefit.CTB_ATY_FEE_AMT AS IN_CTB_ATY_FEE_AMT,
	-- *INF*: IIF(ISNULL(IN_CTB_ATY_FEE_AMT), 0, IN_CTB_ATY_FEE_AMT)
	IFF(IN_CTB_ATY_FEE_AMT IS NULL, 0, IN_CTB_ATY_FEE_AMT) AS CTB_ATY_FEE_AMT,
	EXP_get_values_benefit.CTB_ATY_COSTS_AMT AS IN_CTB_ATY_COSTS_AMT,
	-- *INF*: IIF(ISNULL(IN_CTB_ATY_COSTS_AMT), 0, IN_CTB_ATY_COSTS_AMT)
	IFF(IN_CTB_ATY_COSTS_AMT IS NULL, 0, IN_CTB_ATY_COSTS_AMT) AS CTB_ATY_COSTS_AMT,
	EXP_get_values_benefit.CTB_ATY_FILE_NUM AS IN_CTB_ATY_FILE_NUM,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_CTB_ATY_FILE_NUM))) OR LENGTH(LTRIM(RTRIM(IN_CTB_ATY_FILE_NUM))) = 0, 'N/A', LTRIM(RTRIM(IN_CTB_ATY_FILE_NUM)))
	-- 
	-- 
	-- 
	IFF(
	    LTRIM(RTRIM(IN_CTB_ATY_FILE_NUM)) IS NULL OR LENGTH(LTRIM(RTRIM(IN_CTB_ATY_FILE_NUM))) = 0,
	    'N/A',
	    LTRIM(RTRIM(IN_CTB_ATY_FILE_NUM))
	) AS CTB_ATY_FILE_NUM,
	EXP_get_values_benefit.CTB_HOURLY_RATE AS IN_CTB_HOURLY_RATE,
	-- *INF*: IIF(ISNULL(IN_CTB_HOURLY_RATE), 0, IN_CTB_HOURLY_RATE)
	IFF(IN_CTB_HOURLY_RATE IS NULL, 0, IN_CTB_HOURLY_RATE) AS CTB_HOURLY_RATE,
	EXP_get_values_benefit.CTB_HOURS_WORKED AS IN_CTB_HOURS_WORKED,
	-- *INF*: IIF(ISNULL(IN_CTB_HOURS_WORKED), 0, IN_CTB_HOURS_WORKED)
	IFF(IN_CTB_HOURS_WORKED IS NULL, 0, IN_CTB_HOURS_WORKED) AS CTB_HOURS_WORKED,
	EXP_get_values_benefit.CTB_NUM_DAYS AS IN_CTB_NUM_DAYS,
	-- *INF*: IIF(ISNULL(IN_CTB_NUM_DAYS), -1, IN_CTB_NUM_DAYS)
	IFF(IN_CTB_NUM_DAYS IS NULL, - 1, IN_CTB_NUM_DAYS) AS CTB_NUM_DAYS,
	EXP_get_values_benefit.CTB_NUM_WEEKS AS IN_CTB_NUM_WEEKS,
	-- *INF*: IIF(ISNULL(IN_CTB_NUM_WEEKS), -1, IN_CTB_NUM_WEEKS)
	IFF(IN_CTB_NUM_WEEKS IS NULL, - 1, IN_CTB_NUM_WEEKS) AS CTB_NUM_WEEKS,
	EXP_get_values_benefit.CTB_TPD_RATE AS IN_CTB_TPD_RATE,
	-- *INF*: IIF(ISNULL(IN_CTB_TPD_RATE), 0, IN_CTB_TPD_RATE)
	IFF(IN_CTB_TPD_RATE IS NULL, 0, IN_CTB_TPD_RATE) AS CTB_TPD_RATE,
	EXP_get_values_benefit.CTB_TPD_RATE_FCTR AS IN_CTB_TPD_RATE_FCTR,
	-- *INF*: IIF(ISNULL(IN_CTB_TPD_RATE_FCTR), 0, IN_CTB_TPD_RATE_FCTR)
	IFF(IN_CTB_TPD_RATE_FCTR IS NULL, 0, IN_CTB_TPD_RATE_FCTR) AS CTB_TPD_RATE_FCTR,
	EXP_get_values_benefit.CTB_TPD_WAGE_LOSS AS IN_CTB_TPD_WAGE_LOSS,
	-- *INF*: IIF(ISNULL(IN_CTB_TPD_WAGE_LOSS), 0, IN_CTB_TPD_WAGE_LOSS)
	IFF(IN_CTB_TPD_WAGE_LOSS IS NULL, 0, IN_CTB_TPD_WAGE_LOSS) AS CTB_TPD_WAGE_LOSS,
	EXP_get_values_benefit.CTB_TPD_WKLY_WAGE AS IN_CTB_TPD_WKLY_WAGE,
	-- *INF*: IIF(ISNULL(IN_CTB_TPD_WKLY_WAGE), 0, IN_CTB_TPD_WKLY_WAGE)
	IFF(IN_CTB_TPD_WKLY_WAGE IS NULL, 0, IN_CTB_TPD_WKLY_WAGE) AS CTB_TPD_WKLY_WAGE,
	EXP_get_values_benefit.CTB_BENE_MIS_CMT AS IN_CTB_BENE_MIS_CMT,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_CTB_BENE_MIS_CMT))) OR LENGTH(LTRIM(RTRIM(IN_CTB_BENE_MIS_CMT))) = 0, 'N/A', LTRIM(RTRIM  ( SUBSTR(IN_CTB_BENE_MIS_CMT,1,250))))
	-- 
	-- 
	IFF(
	    LTRIM(RTRIM(IN_CTB_BENE_MIS_CMT)) IS NULL OR LENGTH(LTRIM(RTRIM(IN_CTB_BENE_MIS_CMT))) = 0,
	    'N/A',
	    LTRIM(RTRIM(SUBSTR(IN_CTB_BENE_MIS_CMT, 1, 250)))
	) AS CTB_BENE_MIS_CMT,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS modified_date,
	EXP_get_values_benefit.CLAIM_PAY_CTGRY_LITIGATED_IND AS IN_CLAIM_PAY_CTGRY_LITIGATED_IND,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_CLAIM_PAY_CTGRY_LITIGATED_IND))) OR LENGTH(LTRIM(RTRIM(IN_CLAIM_PAY_CTGRY_LITIGATED_IND))) = 0, 'N/A', LTRIM(RTRIM(IN_CLAIM_PAY_CTGRY_LITIGATED_IND)))
	IFF(
	    LTRIM(RTRIM(IN_CLAIM_PAY_CTGRY_LITIGATED_IND)) IS NULL
	    or LENGTH(LTRIM(RTRIM(IN_CLAIM_PAY_CTGRY_LITIGATED_IND))) = 0,
	    'N/A',
	    LTRIM(RTRIM(IN_CLAIM_PAY_CTGRY_LITIGATED_IND))
	) AS CLAIM_PAY_CTGRY_LITIGATED_IND,
	EXP_get_values_benefit.CTB_CREATE_TS AS IN_CTB_CREATE_TS,
	IN_CTB_CREATE_TS AS created_date,
	EXP_get_values_benefit.ctb_lump_sum_ind,
	LKP_COV_CTGRY_CODE.category_code AS cov_ctgry_code,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(cov_ctgry_code))) OR LENGTH(LTRIM(RTRIM(cov_ctgry_code))) = 0, 'N/A',cov_ctgry_code  )
	IFF(
	    LTRIM(RTRIM(cov_ctgry_code)) IS NULL OR LENGTH(LTRIM(RTRIM(cov_ctgry_code))) = 0, 'N/A',
	    cov_ctgry_code
	) AS o_cov_ctgry_code,
	EXP_get_values_benefit.o_ctb_benefit_offset_cd AS ctb_benefit_offset_cd,
	ctb_benefit_offset_cd AS BenefitOffsetCode,
	EXP_get_values_benefit.o_ctb_benefit_offset_amt AS ctb_benefit_offset_amt,
	ctb_benefit_offset_amt AS BenefitOffsetAmount
	FROM EXP_get_values_benefit
	LEFT JOIN LKP_COV_CTGRY_CODE
	ON LKP_COV_CTGRY_CODE.claim_nbr = LKP_CLAIM_OCCURRENCE.claim_occurrence_key
	LEFT JOIN LKP_claim_payment_exd_benefit
	ON LKP_claim_payment_exd_benefit.claim_pay_num = EXP_get_values_benefit.CTB_DRAFT_NBR
),
LKP_payment_category_benefit AS (
	SELECT
	claim_pay_ctgry_id,
	claim_pay_ak_id,
	claim_pay_ctgry_type,
	claim_pay_ctgry_seq_num,
	IN_claim_pay_ak_id,
	IN_CTB_BENEFIT_TYPE,
	IN_CTB_BENE_SEQ_NUM
	FROM (
		SELECT 
			claim_pay_ctgry_id,
			claim_pay_ak_id,
			claim_pay_ctgry_type,
			claim_pay_ctgry_seq_num,
			IN_claim_pay_ak_id,
			IN_CTB_BENEFIT_TYPE,
			IN_CTB_BENE_SEQ_NUM
		FROM claim_payment_category
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_pay_ak_id,claim_pay_ctgry_type,claim_pay_ctgry_seq_num ORDER BY claim_pay_ctgry_id) = 1
),
RTR_claim_payment_category_benefit AS (
	SELECT
	LKP_payment_category_benefit.claim_pay_ctgry_id,
	EXP_set_defaults_benefit.claim_pay_ak_id,
	EXP_set_defaults_benefit.CTB_BENEFIT_TYPE,
	EXP_set_defaults_benefit.CTB_BENE_SEQ_NUM,
	EXP_set_defaults_benefit.CTB_BENEFIT_AMT,
	EXP_set_defaults_benefit.CTB_AMT_EARNED,
	EXP_set_defaults_benefit.CTB_BILLED_AMT,
	EXP_set_defaults_benefit.CTB_BENEFIT_START,
	EXP_set_defaults_benefit.CTB_BENEFIT_END,
	EXP_set_defaults_benefit.CTB_FIN_TYPE_CD,
	EXP_set_defaults_benefit.CTB_INVOICE_NBR,
	EXP_set_defaults_benefit.CTB_COST_CONT_SAV,
	EXP_set_defaults_benefit.CTB_COST_CONT_RED,
	EXP_set_defaults_benefit.CTB_COST_CONT_PPO,
	EXP_set_defaults_benefit.CTB_ATY_FEE_AMT,
	EXP_set_defaults_benefit.CTB_ATY_COSTS_AMT,
	EXP_set_defaults_benefit.CTB_ATY_FILE_NUM,
	EXP_set_defaults_benefit.CTB_HOURLY_RATE,
	EXP_set_defaults_benefit.CTB_HOURS_WORKED,
	EXP_set_defaults_benefit.CTB_NUM_DAYS,
	EXP_set_defaults_benefit.CTB_NUM_WEEKS,
	EXP_set_defaults_benefit.CTB_TPD_RATE,
	EXP_set_defaults_benefit.CTB_TPD_RATE_FCTR,
	EXP_set_defaults_benefit.CTB_TPD_WAGE_LOSS,
	EXP_set_defaults_benefit.CTB_TPD_WKLY_WAGE,
	EXP_set_defaults_benefit.CTB_BENE_MIS_CMT,
	EXP_set_defaults_benefit.crrnt_snpsht_flag,
	EXP_set_defaults_benefit.audit_id,
	EXP_set_defaults_benefit.eff_from_date,
	EXP_set_defaults_benefit.eff_to_date,
	EXP_set_defaults_benefit.source_sys_id,
	EXP_set_defaults_benefit.created_date,
	EXP_set_defaults_benefit.modified_date,
	EXP_set_defaults_benefit.CLAIM_PAY_CTGRY_LITIGATED_IND,
	EXP_set_defaults_benefit.ctb_lump_sum_ind,
	EXP_set_defaults_benefit.o_cov_ctgry_code AS cov_ctgry_code,
	EXP_set_defaults_benefit.BenefitOffsetCode,
	EXP_set_defaults_benefit.BenefitOffsetAmount
	FROM EXP_set_defaults_benefit
	LEFT JOIN LKP_payment_category_benefit
	ON LKP_payment_category_benefit.claim_pay_ak_id = EXP_set_defaults_benefit.claim_pay_ak_id AND LKP_payment_category_benefit.claim_pay_ctgry_type = EXP_set_defaults_benefit.CTB_BENEFIT_TYPE AND LKP_payment_category_benefit.claim_pay_ctgry_seq_num = EXP_set_defaults_benefit.CTB_BENE_SEQ_NUM
),
RTR_claim_payment_category_benefit_INSERT AS (SELECT * FROM RTR_claim_payment_category_benefit WHERE ISNULL(claim_pay_ctgry_id)),
RTR_claim_payment_category_benefit_UPDATE AS (SELECT * FROM RTR_claim_payment_category_benefit WHERE NOT ISNULL(claim_pay_ctgry_id)),
UPD_claim_payment_category_benefit_update AS (
	SELECT
	claim_pay_ctgry_id AS claim_pay_ctgry_id3, 
	claim_pay_ak_id AS claim_pay_ak_id3, 
	CTB_BENEFIT_TYPE AS CTB_BENEFIT_TYPE3, 
	CTB_BENE_SEQ_NUM AS CTB_BENE_SEQ_NUM3, 
	CTB_BENEFIT_AMT AS CTB_BENEFIT_AMT3, 
	CTB_AMT_EARNED AS CTB_AMT_EARNED3, 
	CTB_BILLED_AMT AS CTB_BILLED_AMT3, 
	CTB_BENEFIT_START AS CTB_BENEFIT_START3, 
	CTB_BENEFIT_END AS CTB_BENEFIT_END3, 
	CTB_FIN_TYPE_CD AS CTB_FIN_TYPE_CD3, 
	CTB_INVOICE_NBR AS CTB_INVOICE_NBR3, 
	CTB_COST_CONT_SAV AS CTB_COST_CONT_SAV3, 
	CTB_COST_CONT_RED AS CTB_COST_CONT_RED3, 
	CTB_COST_CONT_PPO AS CTB_COST_CONT_PPO3, 
	CTB_ATY_FEE_AMT AS CTB_ATY_FEE_AMT3, 
	CTB_ATY_COSTS_AMT AS CTB_ATY_COSTS_AMT3, 
	CTB_ATY_FILE_NUM AS CTB_ATY_FILE_NUM3, 
	CTB_HOURLY_RATE AS CTB_HOURLY_RATE3, 
	CTB_HOURS_WORKED AS CTB_HOURS_WORKED3, 
	CTB_NUM_DAYS AS CTB_NUM_DAYS3, 
	CTB_NUM_WEEKS AS CTB_NUM_WEEKS3, 
	CTB_TPD_RATE AS CTB_TPD_RATE3, 
	CTB_TPD_RATE_FCTR AS CTB_TPD_RATE_FCTR3, 
	CTB_TPD_WAGE_LOSS AS CTB_TPD_WAGE_LOSS3, 
	CTB_TPD_WKLY_WAGE AS CTB_TPD_WKLY_WAGE3, 
	CTB_BENE_MIS_CMT AS CTB_BENE_MIS_CMT3, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag3, 
	audit_id AS audit_id3, 
	eff_from_date AS eff_from_date3, 
	eff_to_date AS eff_to_date3, 
	source_sys_id AS source_sys_id3, 
	created_date AS created_date3, 
	modified_date AS modified_date3, 
	CLAIM_PAY_CTGRY_LITIGATED_IND AS CLAIM_PAY_CTGRY_LITIGATED_IND3, 
	ctb_lump_sum_ind AS ctb_lump_sum_ind3, 
	cov_ctgry_code AS COV_CTGRY_CODE3, 
	BenefitOffsetCode AS BenefitOffsetCode3, 
	BenefitOffsetAmount AS BenefitOffsetAmount3
	FROM RTR_claim_payment_category_benefit_UPDATE
),
claim_payment_category_benefit_update AS (
	MERGE INTO claim_payment_category AS T
	USING UPD_claim_payment_category_benefit_update AS S
	ON T.claim_pay_ctgry_id = S.claim_pay_ctgry_id3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.claim_pay_ctgry_amt = S.CTB_BENEFIT_AMT3, T.claim_pay_ctgry_earned_amt = S.CTB_AMT_EARNED3, T.claim_pay_ctgry_billed_amt = S.CTB_BILLED_AMT3, T.claim_pay_ctgry_start_date = S.CTB_BENEFIT_START3, T.claim_pay_ctgry_end_date = S.CTB_BENEFIT_END3, T.financial_type_code = S.CTB_FIN_TYPE_CD3, T.invc_num = S.CTB_INVOICE_NBR3, T.cost_containment_saving_amt = S.CTB_COST_CONT_SAV3, T.cost_containment_red_amt = S.CTB_COST_CONT_RED3, T.cost_containment_ppo_amt = S.CTB_COST_CONT_PPO3, T.attorney_fee_amt = S.CTB_ATY_FEE_AMT3, T.attorney_cost_amt = S.CTB_ATY_COSTS_AMT3, T.attorney_file_num = S.CTB_ATY_FILE_NUM3, T.hourly_rate = S.CTB_HOURLY_RATE3, T.hours_worked = S.CTB_HOURS_WORKED3, T.num_of_days = S.CTB_NUM_DAYS3, T.num_of_weeks = S.CTB_NUM_WEEKS3, T.tpd_rate = S.CTB_TPD_RATE3, T.tpd_rate_fac = S.CTB_TPD_RATE_FCTR3, T.tpd_wage_loss = S.CTB_TPD_WAGE_LOSS3, T.tpd_wkly_wage = S.CTB_TPD_WKLY_WAGE3, T.claim_pay_ctgry_comment = S.CTB_BENE_MIS_CMT3, T.audit_id = S.audit_id3, T.modified_date = S.modified_date3, T.claim_pay_ctgry_litigated_ind = S.CLAIM_PAY_CTGRY_LITIGATED_IND3, T.claim_pay_ctgry_lump_sum_ind = S.ctb_lump_sum_ind3, T.cov_ctgry_code = S.COV_CTGRY_CODE3, T.BenefitOffsetCode = S.BenefitOffsetCode3, T.BenefitOffsetAmount = S.BenefitOffsetAmount3
),
SEQ_Claim_payment_category_AK_ID AS (
	CREATE SEQUENCE SEQ_Claim_payment_category_AK_ID
	START = 0
	INCREMENT = 1;
),
UPD_claim_payment_category_benefit_insert AS (
	SELECT
	claim_pay_ctgry_id AS claim_pay_ctgry_id1, 
	claim_pay_ak_id AS claim_pay_ak_id1, 
	CTB_BENEFIT_TYPE AS CTB_BENEFIT_TYPE1, 
	CTB_BENE_SEQ_NUM AS CTB_BENE_SEQ_NUM1, 
	CTB_BENEFIT_AMT AS CTB_BENEFIT_AMT1, 
	CTB_AMT_EARNED AS CTB_AMT_EARNED1, 
	CTB_BILLED_AMT AS CTB_BILLED_AMT1, 
	CTB_BENEFIT_START AS CTB_BENEFIT_START1, 
	CTB_BENEFIT_END AS CTB_BENEFIT_END1, 
	CTB_FIN_TYPE_CD AS CTB_FIN_TYPE_CD1, 
	CTB_INVOICE_NBR AS CTB_INVOICE_NBR1, 
	CTB_COST_CONT_SAV AS CTB_COST_CONT_SAV1, 
	CTB_COST_CONT_RED AS CTB_COST_CONT_RED1, 
	CTB_COST_CONT_PPO AS CTB_COST_CONT_PPO1, 
	CTB_ATY_FEE_AMT AS CTB_ATY_FEE_AMT1, 
	CTB_ATY_COSTS_AMT AS CTB_ATY_COSTS_AMT1, 
	CTB_ATY_FILE_NUM AS CTB_ATY_FILE_NUM1, 
	CTB_HOURLY_RATE AS CTB_HOURLY_RATE1, 
	CTB_HOURS_WORKED AS CTB_HOURS_WORKED1, 
	CTB_NUM_DAYS AS CTB_NUM_DAYS1, 
	CTB_NUM_WEEKS AS CTB_NUM_WEEKS1, 
	CTB_TPD_RATE AS CTB_TPD_RATE1, 
	CTB_TPD_RATE_FCTR AS CTB_TPD_RATE_FCTR1, 
	CTB_TPD_WAGE_LOSS AS CTB_TPD_WAGE_LOSS1, 
	CTB_TPD_WKLY_WAGE AS CTB_TPD_WKLY_WAGE1, 
	CTB_BENE_MIS_CMT AS CTB_BENE_MIS_CMT1, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag1, 
	audit_id AS audit_id1, 
	eff_from_date AS eff_from_date1, 
	eff_to_date AS eff_to_date1, 
	source_sys_id AS source_sys_id1, 
	created_date AS created_date1, 
	modified_date AS modified_date1, 
	CLAIM_PAY_CTGRY_LITIGATED_IND AS CLAIM_PAY_CTGRY_LITIGATED_IND1, 
	ctb_lump_sum_ind AS ctb_lump_sum_ind1, 
	cov_ctgry_code AS COV_CTGRY_CODE, 
	BenefitOffsetCode AS BenefitOffsetCode1, 
	BenefitOffsetAmount AS BenefitOffsetAmount1
	FROM RTR_claim_payment_category_benefit_INSERT
),
claim_payment_category_benefit_insert AS (
	INSERT INTO claim_payment_category
	(claim_pay_ctgry_ak_id, claim_pay_ak_id, claim_pay_ctgry_type, claim_pay_ctgry_seq_num, claim_pay_ctgry_amt, claim_pay_ctgry_earned_amt, claim_pay_ctgry_billed_amt, claim_pay_ctgry_start_date, claim_pay_ctgry_end_date, financial_type_code, invc_num, cost_containment_saving_amt, cost_containment_red_amt, cost_containment_ppo_amt, attorney_fee_amt, attorney_cost_amt, attorney_file_num, hourly_rate, hours_worked, num_of_days, num_of_weeks, tpd_rate, tpd_rate_fac, tpd_wage_loss, tpd_wkly_wage, claim_pay_ctgry_comment, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, claim_pay_ctgry_litigated_ind, claim_pay_ctgry_lump_sum_ind, cov_ctgry_code, BenefitOffsetCode, BenefitOffsetAmount)
	SELECT 
	SEQ_Claim_payment_category_AK_ID.NEXTVAL AS CLAIM_PAY_CTGRY_AK_ID, 
	claim_pay_ak_id1 AS CLAIM_PAY_AK_ID, 
	CTB_BENEFIT_TYPE1 AS CLAIM_PAY_CTGRY_TYPE, 
	CTB_BENE_SEQ_NUM1 AS CLAIM_PAY_CTGRY_SEQ_NUM, 
	CTB_BENEFIT_AMT1 AS CLAIM_PAY_CTGRY_AMT, 
	CTB_AMT_EARNED1 AS CLAIM_PAY_CTGRY_EARNED_AMT, 
	CTB_BILLED_AMT1 AS CLAIM_PAY_CTGRY_BILLED_AMT, 
	CTB_BENEFIT_START1 AS CLAIM_PAY_CTGRY_START_DATE, 
	CTB_BENEFIT_END1 AS CLAIM_PAY_CTGRY_END_DATE, 
	CTB_FIN_TYPE_CD1 AS FINANCIAL_TYPE_CODE, 
	CTB_INVOICE_NBR1 AS INVC_NUM, 
	CTB_COST_CONT_SAV1 AS COST_CONTAINMENT_SAVING_AMT, 
	CTB_COST_CONT_RED1 AS COST_CONTAINMENT_RED_AMT, 
	CTB_COST_CONT_PPO1 AS COST_CONTAINMENT_PPO_AMT, 
	CTB_ATY_FEE_AMT1 AS ATTORNEY_FEE_AMT, 
	CTB_ATY_COSTS_AMT1 AS ATTORNEY_COST_AMT, 
	CTB_ATY_FILE_NUM1 AS ATTORNEY_FILE_NUM, 
	CTB_HOURLY_RATE1 AS HOURLY_RATE, 
	CTB_HOURS_WORKED1 AS HOURS_WORKED, 
	CTB_NUM_DAYS1 AS NUM_OF_DAYS, 
	CTB_NUM_WEEKS1 AS NUM_OF_WEEKS, 
	CTB_TPD_RATE1 AS TPD_RATE, 
	CTB_TPD_RATE_FCTR1 AS TPD_RATE_FAC, 
	CTB_TPD_WAGE_LOSS1 AS TPD_WAGE_LOSS, 
	CTB_TPD_WKLY_WAGE1 AS TPD_WKLY_WAGE, 
	CTB_BENE_MIS_CMT1 AS CLAIM_PAY_CTGRY_COMMENT, 
	crrnt_snpsht_flag1 AS CRRNT_SNPSHT_FLAG, 
	audit_id1 AS AUDIT_ID, 
	eff_from_date1 AS EFF_FROM_DATE, 
	eff_to_date1 AS EFF_TO_DATE, 
	source_sys_id1 AS SOURCE_SYS_ID, 
	created_date1 AS CREATED_DATE, 
	modified_date1 AS MODIFIED_DATE, 
	CLAIM_PAY_CTGRY_LITIGATED_IND1 AS CLAIM_PAY_CTGRY_LITIGATED_IND, 
	ctb_lump_sum_ind1 AS CLAIM_PAY_CTGRY_LUMP_SUM_IND, 
	COV_CTGRY_CODE AS COV_CTGRY_CODE, 
	BenefitOffsetCode1 AS BENEFITOFFSETCODE, 
	BenefitOffsetAmount1 AS BENEFITOFFSETAMOUNT
	FROM UPD_claim_payment_category_benefit_insert
),
WITH
LKP_PolicyOfferingCode AS (
	SELECT
	PolicyOfferingCode,
	SequenceNumber,
	Identifiers
	FROM (
		SELECT PolicyOfferingCode as PolicyOfferingCode,
		SequenceNumber as SequenceNumber,
		(
		CASE WHEN SequenceNumber=1 THEN PolicySymbol
		WHEN SequenceNumber=2 THEN PolicySymbol
		WHEN SequenceNumber=3 THEN PolicySymbol+'&'+RiskUnitGroup
		WHEN SequenceNumber=4 THEN PolicySymbol+'&'+RiskUnitGroup+'&'+InsuranceLine
		WHEN SequenceNumber=5 THEN PolicySymbol+'&'+SublineCode+'&'+InsuranceLine+'&'+ClassCode
		WHEN SequenceNumber=6 THEN PolicySymbol+'&'+InsuranceLine+'&'+MajorPerilCode
		WHEN SequenceNumber=7 THEN PolicySymbol
		WHEN SequenceNumber=8 THEN PolicySymbol
		WHEN SequenceNumber=9 THEN PolicySymbol+'&'+SublineCode+'&'+InsuranceLine
		WHEN SequenceNumber IN (10 ,11) THEN PolicySymbol+'&'+InsuranceLine+'&'+ RiskUnitGroup
		WHEN SequenceNumber=12 THEN PolicySymbol
		END
		) as Identifiers
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupPolicyOfferingRules
		WHERE CurrentSnapshotFlag=1 and SourceCode='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SequenceNumber,Identifiers ORDER BY PolicyOfferingCode) = 1
),
LKP_StrategicProfitCenter AS (
	SELECT
	StrategicProfitCenterAbbreviation,
	StrategicProfitCenterAKId
	FROM (
		SELECT 
			StrategicProfitCenterAbbreviation,
			StrategicProfitCenterAKId
		FROM StrategicProfitCenter
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StrategicProfitCenterAKId ORDER BY StrategicProfitCenterAbbreviation) = 1
),
LKP_PolicyOfferingAKId AS (
	SELECT
	PolicyOfferingAKId,
	PolicyOfferingCode
	FROM (
		SELECT 
			PolicyOfferingAKId,
			PolicyOfferingCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyOffering
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyOfferingCode ORDER BY PolicyOfferingAKId) = 1
),
LKP_Pif02_MaxDate AS (
	SELECT
	pif_date_time_stamp,
	ID
	FROM (
		select max(substring(convert(varchar(16),pif_date_time_stamp),1,8)) as pif_date_time_stamp, 1 as id
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_02_stage--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ID ORDER BY pif_date_time_stamp) = 1
),
LKP_sup_policy_issue_code AS (
	SELECT
	sup_pol_issue_code_id,
	pol_issue_code
	FROM (
		SELECT 
			sup_pol_issue_code_id,
			pol_issue_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_policy_issue_code
		WHERE crrnt_snpsht_flag=1 AND source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_issue_code ORDER BY sup_pol_issue_code_id) = 1
),
SQ_pif_02_stage AS (
	SELECT pif_symbol, 
	       pif_policy_number, 
	       pif_module, 
	       pif_master_co_number_a, 
	       pif_location_a, 
	       pif_amend_number, 
	       pif_eff_yr_a, 
	       pif_eff_mo_a, 
	       pif_eff_da_a, 
	       pif_exp_yr_a, 
	       pif_exp_mo_a, 
	       pif_exp_da_a, 
	       pif_installment_term_a, 
	       pif_risk_state_prov, 
	       pif_company_number, 
	       pif_full_agency_number, 
	       pif_ent_yr_a, 
	       pif_ent_mo_a, 
	       pif_ent_da_a, 
	       pif_line_business, 
	       pif_issue_code, 
	       pif_company_line, 
	       pif_pay_service_code,
	       pif_audit_code, 
	       pif_variation_code, 
	       pif_producer_code, 
	       pif_review_code, 
	       pif_mvr_report_year, 
	       pif_risk_grade_guide, 
	       pif_renewal_code, 
	       pif_reason_amended,
	       pif_renew_policy_symbol, 
	       pif_renew_policy_number, 
	       pif_original_incept, 
	       wb_class_of_business, 
	       pif_guarantee_ind, 
	       wb_renewal_discount, 
	       wb_inview_indicator, 
	       pif_occupation, 
	       pif_final_audit_ind, 
	       pif_excess_claim_ind, 
	       pif_anniversary_rerate, 
	       pif_policy_status_on_pif, 
	       pif_uk_postal_code, 
	       pif_nonsmoker_discount, 
	       pif_upload_audit, 
	       pif_zip_ind, 
	       pif_prgm_id, 
	       pif_wbc_county, 
	       pif_orig_act_date, 
	       pif_fn_date, 
	       pif_terrorism_risk, 
	       pif_target_market_code, 
	       pif_completed_audit_date, 
	       pif_seg_id, 
	       pif_service_center,
	       pif_renewal_safe_record_ctr,
	       pif_dc_bill_ind,
	       row_number() over (partition by pif_symbol, pif_policy_number, pif_module order by sar_acct_entrd_date,pif_4514_stage_id ASC) RN_sar_acct_entrd_date_MaxToMin,
	       sar_acct_entrd_date,
	       sar_insurance_line,
	       sar_risk_unit_group,
	       sar_major_peril,
	       sar_class_1_4,
	       sar_class_5_6 ,
	       sar_sub_line
	from (
	SELECT distinct pif_02_stage.pif_symbol, 
	       pif_02_stage.pif_policy_number, 
	       pif_02_stage.pif_module, 
	       pif_02_stage.pif_master_co_number_a, 
	       pif_02_stage.pif_location_a, 
	       pif_02_stage.pif_amend_number, 
	       pif_02_stage.pif_eff_yr_a, 
	       pif_02_stage.pif_eff_mo_a, 
	       pif_02_stage.pif_eff_da_a, 
	       pif_02_stage.pif_exp_yr_a, 
	       pif_02_stage.pif_exp_mo_a, 
	       pif_02_stage.pif_exp_da_a, 
	       pif_02_stage.pif_installment_term_a, 
	       pif_02_stage.pif_risk_state_prov, 
	       pif_02_stage.pif_company_number, 
	       pif_02_stage.pif_full_agency_number, 
	       pif_02_stage.pif_ent_yr_a, 
	       pif_02_stage.pif_ent_mo_a, 
	       pif_02_stage.pif_ent_da_a, 
	       pif_02_stage.pif_line_business, 
	       pif_02_stage.pif_issue_code, 
	       pif_02_stage.pif_company_line, 
	       pif_02_stage.pif_pay_service_code,
	       pif_02_stage.pif_audit_code, 
	       pif_02_stage.pif_variation_code, 
	       pif_02_stage.pif_producer_code, 
	       pif_02_stage.pif_review_code, 
	       pif_02_stage.pif_mvr_report_year, 
	       pif_02_stage.pif_risk_grade_guide, 
	       pif_02_stage.pif_renewal_code, 
	       pif_02_stage.pif_reason_amended,
	       pif_02_stage.pif_renew_policy_symbol, 
	       pif_02_stage.pif_renew_policy_number, 
	       pif_02_stage.pif_original_incept, 
	       pif_02_stage.wb_class_of_business, 
	       pif_02_stage.pif_guarantee_ind, 
	       pif_02_stage.wb_renewal_discount, 
	       pif_02_stage.wb_inview_indicator, 
	       pif_02_stage.pif_occupation, 
	       pif_02_stage.pif_final_audit_ind, 
	       pif_02_stage.pif_excess_claim_ind, 
	       pif_02_stage.pif_anniversary_rerate, 
	       pif_02_stage.pif_policy_status_on_pif, 
	       pif_02_stage.pif_uk_postal_code, 
	       pif_02_stage.pif_nonsmoker_discount, 
	       pif_02_stage.pif_upload_audit, 
	       pif_02_stage.pif_zip_ind, 
	       pif_02_stage.pif_prgm_id, 
	       pif_02_stage.pif_wbc_county, 
	       pif_02_stage.pif_orig_act_date, 
	       pif_02_stage.pif_fn_date, 
	       pif_02_stage.pif_terrorism_risk, 
	       pif_02_stage.pif_target_market_code, 
	       pif_02_stage.pif_completed_audit_date, 
	       pif_02_stage.pif_seg_id, 
	       pif_02_stage.pif_service_center,
	       pif_02_stage.pif_date_time_stamp,
	       pif_02_stage.pif_renewal_safe_record_ctr,
	       pif_02_stage.pif_dc_bill_ind,
	       pif_4514_stage.pif_4514_stage_id,
	       pif_4514_stage.sar_acct_entrd_date,
	       pif_4514_stage.sar_insurance_line,
	       pif_4514_stage.sar_risk_unit_group,
	       pif_4514_stage.sar_major_peril,
	       pif_4514_stage.sar_class_1_4,
	       pif_4514_stage.sar_class_5_6 ,
	       pif_4514_stage.sar_sub_line
	       FROM
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_02_stage 
	       left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_4514_stage 
		 on pif_02_stage.pif_symbol=pif_4514_stage.pif_symbol and pif_02_stage.pif_policy_number=pif_4514_stage.pif_policy_number and pif_02_stage.pif_module=pif_4514_stage.pif_module	
	       WHERE 
	        (LTRIM(RTRIM(pif_02_stage.pif_eff_yr_a)) <> '9999'or 
		LTRIM(RTRIM(pif_02_stage.pif_exp_yr_a)) <> '9999')and
		LTRIM(RTRIM(SUBSTRING(CAST(pif_02_stage.pif_full_agency_number AS char(7)),1,2)+SUBSTRING(CAST(pif_02_stage.pif_full_agency_number AS char(7)),5,3)))<>'99999'
	---------------------------------------------
	--AND pif_02_stage.pif_symbol  + pif_02_stage.pif_policy_number  +     pif_02_stage.pif_module  IN  ('A0S189678900')
	---------------------------------------------
	) Src
	ORDER BY pif_symbol, 
	       pif_policy_number, 
	       pif_module, 
	       sar_acct_entrd_date
	
	
	--  We are reading those rows from .pif_02_stage where (pif_eff_yr_a or pif_exp_yr_a) <> '9999' and pif_full_agency_number 
	-- <>'99999'
),
LKP_PIF_04Stage AS (
	SELECT
	UNDND_UNDERWRITER_ID,
	PIF_SYMBOL,
	PIF_POLICY_NUMBER,
	PIF_MODULE
	FROM (
		SELECT PIF_04Stage.pif_symbol AS pif_symbol,
			PIF_04Stage.pif_policy_number AS pif_policy_number,
		       PIF_04Stage.pif_module AS pif_module,
		       PIF_04Stage.UNDND_UNDERWRITER_ID AS UNDND_UNDERWRITER_ID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PIF_04Stage
		ORDER BY pif_symbol,pif_policy_number,pif_module,PIF04StageId--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PIF_SYMBOL,PIF_POLICY_NUMBER,PIF_MODULE ORDER BY UNDND_UNDERWRITER_ID DESC) = 1
),
LKP_pif_03_stage AS (
	SELECT
	comments_reason_suspended,
	comments_area,
	pif_symbol,
	pif_policy_number,
	pif_module
	FROM (
		SELECT pif_03_stage.pif_symbol AS pif_symbol,
		       pif_03_stage.pif_policy_number AS pif_policy_number,
		       pif_03_stage.pif_module AS pif_module,
		       pif_03_stage.comments_reason_suspended AS comments_reason_suspended,
		       pif_03_stage.comments_area AS comments_area
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_03_stage
		ORDER BY pif_symbol,pif_policy_number,pif_module,pif_03_stage_id--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module ORDER BY comments_reason_suspended DESC) = 1
),
LKP_pif_43gj_stage AS (
	SELECT
	pmd4j_use_code,
	pmd4j_address_line_1,
	pmd4j_addr_lin_2_pos_1,
	pmd4j_addr_lin_2_pos_2_30,
	pif_symbol,
	pif_policy_number,
	pif_module
	FROM (
		SELECT pif_43gj_stage.pif_symbol AS pif_symbol,
		       pif_43gj_stage.pif_policy_number AS pif_policy_number,
		       pif_43gj_stage.pif_module AS pif_module,
		       pif_43gj_stage.pmd4j_use_code AS pmd4j_use_code,
		       pif_43gj_stage.pmd4j_address_line_1 AS pmd4j_address_line_1,
		       pif_43gj_stage.pmd4j_addr_lin_2_pos_1 AS pmd4j_addr_lin_2_pos_1,
		       pif_43gj_stage.pmd4j_addr_lin_2_pos_2_30 AS pmd4j_addr_lin_2_pos_2_30
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_43gj_stage
		ORDER BY pif_symbol,pif_policy_number,pif_module,pif_43gj_stage_id--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module ORDER BY pmd4j_use_code DESC) = 1
),
EXP_PolicyOfferingAKId AS (
	SELECT
	SQ_pif_02_stage.pif_symbol,
	SQ_pif_02_stage.pif_policy_number,
	SQ_pif_02_stage.pif_module,
	SQ_pif_02_stage.pif_master_co_number_a,
	SQ_pif_02_stage.pif_location_a,
	SQ_pif_02_stage.pif_amend_number,
	SQ_pif_02_stage.pif_eff_yr_a,
	SQ_pif_02_stage.pif_eff_mo_a,
	SQ_pif_02_stage.pif_eff_da_a,
	SQ_pif_02_stage.pif_exp_yr_a,
	SQ_pif_02_stage.pif_exp_mo_a,
	SQ_pif_02_stage.pif_exp_da_a,
	SQ_pif_02_stage.pif_installment_term_a,
	SQ_pif_02_stage.pif_risk_state_prov,
	SQ_pif_02_stage.pif_company_number,
	SQ_pif_02_stage.pif_full_agency_number,
	SQ_pif_02_stage.pif_ent_yr_a,
	SQ_pif_02_stage.pif_ent_mo_a,
	SQ_pif_02_stage.pif_ent_da_a,
	SQ_pif_02_stage.pif_line_business,
	SQ_pif_02_stage.pif_issue_code,
	SQ_pif_02_stage.pif_company_line,
	SQ_pif_02_stage.pif_pay_service_code,
	SQ_pif_02_stage.pif_audit_code,
	SQ_pif_02_stage.pif_variation_code,
	SQ_pif_02_stage.pif_producer_code,
	SQ_pif_02_stage.pif_review_code,
	SQ_pif_02_stage.pif_mvr_report_year,
	SQ_pif_02_stage.pif_risk_grade_guide,
	SQ_pif_02_stage.pif_renewal_code,
	SQ_pif_02_stage.pif_reason_amended,
	SQ_pif_02_stage.pif_renew_policy_symbol,
	SQ_pif_02_stage.pif_renew_policy_number,
	SQ_pif_02_stage.pif_original_incept,
	SQ_pif_02_stage.wb_class_of_business,
	SQ_pif_02_stage.pif_guarantee_ind,
	SQ_pif_02_stage.wb_renewal_discount,
	SQ_pif_02_stage.wb_inview_indicator,
	SQ_pif_02_stage.pif_occupation,
	SQ_pif_02_stage.pif_final_audit_ind,
	SQ_pif_02_stage.pif_excess_claim_ind,
	SQ_pif_02_stage.pif_anniversary_rerate,
	SQ_pif_02_stage.pif_policy_status_on_pif,
	SQ_pif_02_stage.pif_uk_postal_code,
	SQ_pif_02_stage.pif_nonsmoker_discount,
	SQ_pif_02_stage.pif_upload_audit,
	SQ_pif_02_stage.pif_zip_ind,
	SQ_pif_02_stage.pif_prgm_id,
	SQ_pif_02_stage.pif_wbc_county,
	SQ_pif_02_stage.pif_orig_act_date,
	SQ_pif_02_stage.pif_fn_date,
	SQ_pif_02_stage.pif_terrorism_risk,
	SQ_pif_02_stage.pif_target_market_code,
	SQ_pif_02_stage.pif_completed_audit_date,
	SQ_pif_02_stage.pif_seg_id,
	SQ_pif_02_stage.pif_renewal_safe_record_ctr,
	SQ_pif_02_stage.pif_dc_bill_ind,
	SQ_pif_02_stage.RN_sar_acct_entrd_date_MaxToMin AS in_RN_sar_acct_entrd_date_MaxToMin,
	SQ_pif_02_stage.sar_acct_entrd_date AS in_sar_acct_entrd_date,
	SQ_pif_02_stage.sar_insurance_line AS in_sar_insurance_line_4514,
	SQ_pif_02_stage.sar_risk_unit_group AS in_sar_risk_unit_group_4514,
	SQ_pif_02_stage.sar_major_peril AS in_sar_major_peril_4514,
	SQ_pif_02_stage.sar_class_1_4 AS in_sar_class_1_4_4514,
	SQ_pif_02_stage.sar_class_5_6 AS in_sar_class_5_6_4514,
	SQ_pif_02_stage.sar_sub_line AS in_sar_sub_line_4514,
	LKP_pif_43gj_stage.pmd4j_use_code AS in_pmd4j_use_code,
	LKP_pif_43gj_stage.pmd4j_address_line_1 AS in_pmd4j_address_line_1,
	LKP_pif_43gj_stage.pmd4j_addr_lin_2_pos_1 AS in_pmd4j_addr_lin_2_pos_1,
	LKP_pif_43gj_stage.pmd4j_addr_lin_2_pos_2_30 AS in_pmd4j_addr_lin_2_pos_2_30,
	LKP_pif_03_stage.comments_reason_suspended AS in_comments_reason_suspended,
	LKP_pif_03_stage.comments_area AS in_comments_area,
	LKP_PIF_04Stage.UNDND_UNDERWRITER_ID AS in_UNDND_UNDERWRITER_ID,
	-- *INF*: LTRIM(RTRIM(in_UNDND_UNDERWRITER_ID))
	LTRIM(RTRIM(in_UNDND_UNDERWRITER_ID)) AS v_UNDND_UNDERWRITER_ID,
	-- *INF*: SUBSTR(ltrim(rtrim(pif_symbol)), 1, 1)
	SUBSTR(ltrim(rtrim(pif_symbol)), 1, 1) AS v_pif_symbol_02_F1,
	-- *INF*: SUBSTR(ltrim(rtrim(pif_symbol)), 1, 2)
	SUBSTR(ltrim(rtrim(pif_symbol)), 1, 2) AS v_pif_symbol_02_F2,
	-- *INF*: ltrim(rtrim(in_sar_insurance_line_4514))
	ltrim(rtrim(in_sar_insurance_line_4514)) AS v_sar_insurance_line_4514,
	-- *INF*: ltrim(rtrim(in_sar_risk_unit_group_4514))
	ltrim(rtrim(in_sar_risk_unit_group_4514)) AS v_sar_risk_unit_group_4514,
	-- *INF*: ltrim(rtrim(in_sar_major_peril_4514))
	ltrim(rtrim(in_sar_major_peril_4514)) AS v_sar_major_peril_4514,
	-- *INF*: ltrim(rtrim(in_sar_class_1_4_4514))
	ltrim(rtrim(in_sar_class_1_4_4514)) AS v_sar_class_1_4_4514,
	-- *INF*: ltrim(rtrim(in_sar_class_5_6_4514))
	ltrim(rtrim(in_sar_class_5_6_4514)) AS v_sar_class_5_6_4514,
	-- *INF*: ltrim(rtrim(in_sar_sub_line_4514))
	ltrim(rtrim(in_sar_sub_line_4514)) AS v_sar_sub_line_4514,
	-- *INF*: substr(in_sar_class_1_4_4514 || in_sar_class_5_6_4514,1,5)
	substr(in_sar_class_1_4_4514 || in_sar_class_5_6_4514, 1, 5) AS v_ClassCode,
	-- *INF*: IIF(pif_symbol = v_prev_pif_symbol  AND  pif_policy_number = v_prev_pif_policy_number  AND pif_module = v_prev_pif_module AND v_prev_PolicyOfferingCode!='000', v_prev_PolicyOfferingCode, :LKP.LKP_PolicyOfferingCode(1, v_pif_symbol_02_F2))
	IFF(pif_symbol = v_prev_pif_symbol AND pif_policy_number = v_prev_pif_policy_number AND pif_module = v_prev_pif_module AND v_prev_PolicyOfferingCode != '000', v_prev_PolicyOfferingCode, LKP_POLICYOFFERINGCODE_1_v_pif_symbol_02_F2.PolicyOfferingCode) AS v_PolicyOfferingCode_SN1,
	-- *INF*: IIF( NOT ISNULL(v_PolicyOfferingCode_SN1), v_PolicyOfferingCode_SN1, :LKP.LKP_PolicyOfferingCode(2, v_pif_symbol_02_F1))
	IFF(NOT v_PolicyOfferingCode_SN1 IS NULL, v_PolicyOfferingCode_SN1, LKP_POLICYOFFERINGCODE_2_v_pif_symbol_02_F1.PolicyOfferingCode) AS v_PolicyOfferingCode_SN2,
	-- *INF*: IIF( NOT ISNULL(v_PolicyOfferingCode_SN2), v_PolicyOfferingCode_SN2, :LKP.LKP_PolicyOfferingCode(3, v_pif_symbol_02_F2 || '&' || in_sar_risk_unit_group_4514))
	IFF(NOT v_PolicyOfferingCode_SN2 IS NULL, v_PolicyOfferingCode_SN2, LKP_POLICYOFFERINGCODE_3_v_pif_symbol_02_F2_in_sar_risk_unit_group_4514.PolicyOfferingCode) AS v_PolicyOfferingCode_SN3,
	-- *INF*: IIF( NOT ISNULL(v_PolicyOfferingCode_SN3), v_PolicyOfferingCode_SN3, :LKP.LKP_PolicyOfferingCode(4, v_pif_symbol_02_F2 || '&' || in_sar_risk_unit_group_4514 ||'&' ||  in_sar_insurance_line_4514))
	IFF(NOT v_PolicyOfferingCode_SN3 IS NULL, v_PolicyOfferingCode_SN3, LKP_POLICYOFFERINGCODE_4_v_pif_symbol_02_F2_in_sar_risk_unit_group_4514_in_sar_insurance_line_4514.PolicyOfferingCode) AS v_PolicyOfferingCode_SN4,
	-- *INF*: IIF( NOT ISNULL(v_PolicyOfferingCode_SN4), v_PolicyOfferingCode_SN4, :LKP.LKP_PolicyOfferingCode(5, v_pif_symbol_02_F2 ||'&' ||  in_sar_sub_line_4514 ||'&' ||  in_sar_insurance_line_4514 || '&' || v_ClassCode))
	IFF(NOT v_PolicyOfferingCode_SN4 IS NULL, v_PolicyOfferingCode_SN4, LKP_POLICYOFFERINGCODE_5_v_pif_symbol_02_F2_in_sar_sub_line_4514_in_sar_insurance_line_4514_v_ClassCode.PolicyOfferingCode) AS v_PolicyOfferingCode_SN5,
	-- *INF*: IIF( NOT ISNULL(v_PolicyOfferingCode_SN5), v_PolicyOfferingCode_SN5, :LKP.LKP_PolicyOfferingCode(6, v_pif_symbol_02_F2 || '&' ||  in_sar_insurance_line_4514 ||'&' ||  in_sar_major_peril_4514))
	IFF(NOT v_PolicyOfferingCode_SN5 IS NULL, v_PolicyOfferingCode_SN5, LKP_POLICYOFFERINGCODE_6_v_pif_symbol_02_F2_in_sar_insurance_line_4514_in_sar_major_peril_4514.PolicyOfferingCode) AS v_PolicyOfferingCode_SN6,
	-- *INF*: DECODE(TRUE,
	--  NOT ISNULL(v_PolicyOfferingCode_SN6), v_PolicyOfferingCode_SN6, 
	-- in_sar_major_peril_4514<>'517' AND in_sar_sub_line_4514<>'365'  AND NOT IN(in_sar_risk_unit_group_4514, '345', '346', '355') , 
	-- :LKP.LKP_PolicyOfferingCode(7, v_pif_symbol_02_F2), NULL)
	DECODE(TRUE,
	NOT v_PolicyOfferingCode_SN6 IS NULL, v_PolicyOfferingCode_SN6,
	in_sar_major_peril_4514 <> '517' AND in_sar_sub_line_4514 <> '365' AND NOT IN(in_sar_risk_unit_group_4514, '345', '346', '355'), LKP_POLICYOFFERINGCODE_7_v_pif_symbol_02_F2.PolicyOfferingCode,
	NULL) AS v_PolicyOfferingCode_SN7,
	-- *INF*: DECODE(TRUE,
	--  NOT ISNULL(v_PolicyOfferingCode_SN7), 
	-- v_PolicyOfferingCode_SN7, 
	-- NOT IN(in_sar_risk_unit_group_4514, '345', '355'), :LKP.LKP_PolicyOfferingCode(8, v_pif_symbol_02_F2), NULL)
	DECODE(TRUE,
	NOT v_PolicyOfferingCode_SN7 IS NULL, v_PolicyOfferingCode_SN7,
	NOT IN(in_sar_risk_unit_group_4514, '345', '355'), LKP_POLICYOFFERINGCODE_8_v_pif_symbol_02_F2.PolicyOfferingCode,
	NULL) AS v_PolicyOfferingCode_SN8,
	-- *INF*: IIF( NOT ISNULL(v_PolicyOfferingCode_SN8), v_PolicyOfferingCode_SN8, :LKP.LKP_PolicyOfferingCode(9, v_pif_symbol_02_F2 || '&' || in_sar_sub_line_4514 ||'&' ||  in_sar_insurance_line_4514))
	IFF(NOT v_PolicyOfferingCode_SN8 IS NULL, v_PolicyOfferingCode_SN8, LKP_POLICYOFFERINGCODE_9_v_pif_symbol_02_F2_in_sar_sub_line_4514_in_sar_insurance_line_4514.PolicyOfferingCode) AS v_PolicyOfferingCode_SN9,
	-- *INF*: IIF( NOT ISNULL(v_PolicyOfferingCode_SN9), v_PolicyOfferingCode_SN9,:LKP.LKP_PolicyOfferingCode(10, v_pif_symbol_02_F2 || '&' ||  in_sar_insurance_line_4514 || '&' || in_sar_risk_unit_group_4514))
	IFF(NOT v_PolicyOfferingCode_SN9 IS NULL, v_PolicyOfferingCode_SN9, LKP_POLICYOFFERINGCODE_10_v_pif_symbol_02_F2_in_sar_insurance_line_4514_in_sar_risk_unit_group_4514.PolicyOfferingCode) AS v_PolicyOfferingCode_SN10,
	-- *INF*: IIF( NOT ISNULL(v_PolicyOfferingCode_SN10), v_PolicyOfferingCode_SN10,:LKP.LKP_PolicyOfferingCode(11, v_pif_symbol_02_F2 || '&' ||  in_sar_insurance_line_4514 || '&' || in_sar_risk_unit_group_4514))
	IFF(NOT v_PolicyOfferingCode_SN10 IS NULL, v_PolicyOfferingCode_SN10, LKP_POLICYOFFERINGCODE_11_v_pif_symbol_02_F2_in_sar_insurance_line_4514_in_sar_risk_unit_group_4514.PolicyOfferingCode) AS v_PolicyOfferingCode_SN11,
	-- *INF*: IIF(NOT ISNULL(v_PolicyOfferingCode_SN11)
	-- 	, v_PolicyOfferingCode_SN11
	-- 	, :LKP.LKP_PolicyOfferingCode(12, ltrim(rtrim(pif_symbol))))
	IFF(NOT v_PolicyOfferingCode_SN11 IS NULL, v_PolicyOfferingCode_SN11, LKP_POLICYOFFERINGCODE_12_ltrim_rtrim_pif_symbol.PolicyOfferingCode) AS v_PolicyOfferingCode_SN12,
	-- *INF*: IIF(NOT ISNULL(v_PolicyOfferingCode_SN12)
	-- 	, v_PolicyOfferingCode_SN12
	-- 	, IIF(v_pif_symbol_02_F1 = 'S' AND pif_line_business = 'WCP',
	-- 		'100'))
	IFF(NOT v_PolicyOfferingCode_SN12 IS NULL, v_PolicyOfferingCode_SN12, IFF(v_pif_symbol_02_F1 = 'S' AND pif_line_business = 'WCP', '100')) AS v_PolicyOfferingCode_SN13,
	-- *INF*: IIF(NOT ISNULL(v_PolicyOfferingCode_SN13)
	-- 	, v_PolicyOfferingCode_SN13
	-- 	, DECODE(TRUE
	-- 		, (v_pif_symbol_02_F2 = 'XA' AND pif_line_business = 'APV'), '810'
	-- 		, (v_pif_symbol_02_F2 = 'XA' AND pif_line_business = 'HAP'), '800'
	-- 		, (v_pif_symbol_02_F2 = 'XA' AND pif_line_business = 'BO'), '400'
	-- 		, (v_pif_symbol_02_F2 = 'XA' AND pif_line_business = 'SMP'), '440'
	-- 		, (v_pif_symbol_02_F2 = 'XA' AND IN (pif_line_business, 'ACV','AFV','GL')), '500'
	-- 		, (v_pif_symbol_02_F2 = 'XA' AND pif_line_business = 'WC'), '100'
	-- 		, (v_pif_symbol_02_F2 = 'XX' AND pif_line_business = 'APV'), '810'
	-- 		, (v_pif_symbol_02_F2 = 'XX' AND pif_line_business = 'HAP'), '800'
	-- 		, (v_pif_symbol_02_F2 = 'XX' AND pif_line_business = 'BO'), '400'
	-- 		, (v_pif_symbol_02_F2 = 'XX' AND pif_line_business = 'SMP'), '440'
	-- 		, (v_pif_symbol_02_F2 = 'XX' AND IN (pif_line_business, 'ACV','AFV','GL')), '500'
	-- 		, (v_pif_symbol_02_F2 = 'XX' AND pif_line_business = 'WC'), '100'
	-- 		)
	-- 	)
	IFF(NOT v_PolicyOfferingCode_SN13 IS NULL, v_PolicyOfferingCode_SN13, DECODE(TRUE,
	( v_pif_symbol_02_F2 = 'XA' AND pif_line_business = 'APV' ), '810',
	( v_pif_symbol_02_F2 = 'XA' AND pif_line_business = 'HAP' ), '800',
	( v_pif_symbol_02_F2 = 'XA' AND pif_line_business = 'BO' ), '400',
	( v_pif_symbol_02_F2 = 'XA' AND pif_line_business = 'SMP' ), '440',
	( v_pif_symbol_02_F2 = 'XA' AND IN(pif_line_business, 'ACV', 'AFV', 'GL') ), '500',
	( v_pif_symbol_02_F2 = 'XA' AND pif_line_business = 'WC' ), '100',
	( v_pif_symbol_02_F2 = 'XX' AND pif_line_business = 'APV' ), '810',
	( v_pif_symbol_02_F2 = 'XX' AND pif_line_business = 'HAP' ), '800',
	( v_pif_symbol_02_F2 = 'XX' AND pif_line_business = 'BO' ), '400',
	( v_pif_symbol_02_F2 = 'XX' AND pif_line_business = 'SMP' ), '440',
	( v_pif_symbol_02_F2 = 'XX' AND IN(pif_line_business, 'ACV', 'AFV', 'GL') ), '500',
	( v_pif_symbol_02_F2 = 'XX' AND pif_line_business = 'WC' ), '100')) AS v_PolicyOfferingCode_SN14,
	-- *INF*: DECODE(TRUE,
	-- 	NOT ISNULL(v_PolicyOfferingCode_SN14)
	-- 	, v_PolicyOfferingCode_SN14
	-- 	, '000')
	DECODE(TRUE,
	NOT v_PolicyOfferingCode_SN14 IS NULL, v_PolicyOfferingCode_SN14,
	'000') AS v_PolicyOfferingCode,
	-- *INF*: DECODE(TRUE, pif_symbol != v_prev_pif_symbol  OR  pif_policy_number != v_prev_pif_policy_number  OR pif_module != v_prev_pif_module, null,
	-- ISNULL(v_prev_PolicyOfferingCode_First) AND v_prev_PolicyOfferingCode != '000',v_prev_PolicyOfferingCode,
	-- v_prev_PolicyOfferingCode_First
	--  )
	DECODE(TRUE,
	pif_symbol != v_prev_pif_symbol OR pif_policy_number != v_prev_pif_policy_number OR pif_module != v_prev_pif_module, null,
	v_prev_PolicyOfferingCode_First IS NULL AND v_prev_PolicyOfferingCode != '000', v_prev_PolicyOfferingCode,
	v_prev_PolicyOfferingCode_First) AS v_prev_PolicyOfferingCode_First,
	-- *INF*: DECODE(TRUE, ISNULL(v_prev_PolicyOfferingCode_First) AND v_PolicyOfferingCode != '000', 
	-- '1',
	-- ISNULL(v_prev_PolicyOfferingCode_First) AND v_PolicyOfferingCode='000' AND in_RN_sar_acct_entrd_date_MaxToMin=1,'1',
	-- '0')
	DECODE(TRUE,
	v_prev_PolicyOfferingCode_First IS NULL AND v_PolicyOfferingCode != '000', '1',
	v_prev_PolicyOfferingCode_First IS NULL AND v_PolicyOfferingCode = '000' AND in_RN_sar_acct_entrd_date_MaxToMin = 1, '1',
	'0') AS v_PolicyOfferingCode_Flag,
	pif_symbol AS v_prev_pif_symbol,
	pif_policy_number AS v_prev_pif_policy_number,
	pif_module AS v_prev_pif_module,
	v_PolicyOfferingCode AS v_prev_PolicyOfferingCode,
	-- *INF*: :LKP.LKP_POLICYOFFERINGAKID(v_PolicyOfferingCode)
	LKP_POLICYOFFERINGAKID_v_PolicyOfferingCode.PolicyOfferingAKId AS v_PolicyOfferingAKId,
	-- *INF*: UPPER(LTRIM(RTRIM(in_pmd4j_use_code)))
	UPPER(LTRIM(RTRIM(in_pmd4j_use_code))) AS v_pmd4j_use_code,
	-- *INF*: IIF(ISNULL(in_pmd4j_address_line_1) OR IS_SPACES(in_pmd4j_address_line_1) OR LENGTH(in_pmd4j_address_line_1)=0, '', LTRIM(RTRIM(in_pmd4j_address_line_1)))
	IFF(in_pmd4j_address_line_1 IS NULL OR IS_SPACES(in_pmd4j_address_line_1) OR LENGTH(in_pmd4j_address_line_1) = 0, '', LTRIM(RTRIM(in_pmd4j_address_line_1))) AS v_pmd4j_address_line_1,
	-- *INF*: IIF(ISNULL(in_pmd4j_addr_lin_2_pos_1) OR IS_SPACES(in_pmd4j_addr_lin_2_pos_1) OR LENGTH(in_pmd4j_addr_lin_2_pos_1)=0, '', LTRIM(RTRIM(in_pmd4j_addr_lin_2_pos_1)))
	IFF(in_pmd4j_addr_lin_2_pos_1 IS NULL OR IS_SPACES(in_pmd4j_addr_lin_2_pos_1) OR LENGTH(in_pmd4j_addr_lin_2_pos_1) = 0, '', LTRIM(RTRIM(in_pmd4j_addr_lin_2_pos_1))) AS v_pmd4j_addr_lin_2_pos_1,
	-- *INF*: IIF(ISNULL(in_pmd4j_addr_lin_2_pos_2_30) OR IS_SPACES(in_pmd4j_addr_lin_2_pos_2_30) OR LENGTH(in_pmd4j_addr_lin_2_pos_2_30)=0, '', LTRIM(RTRIM(in_pmd4j_addr_lin_2_pos_2_30)))
	IFF(in_pmd4j_addr_lin_2_pos_2_30 IS NULL OR IS_SPACES(in_pmd4j_addr_lin_2_pos_2_30) OR LENGTH(in_pmd4j_addr_lin_2_pos_2_30) = 0, '', LTRIM(RTRIM(in_pmd4j_addr_lin_2_pos_2_30))) AS v_pmd4j_addr_lin_2_pos_2_30,
	-- *INF*: IIF(v_pmd4j_use_code='OB', v_pmd4j_address_line_1 ||' ' ||  v_pmd4j_addr_lin_2_pos_1 || v_pmd4j_addr_lin_2_pos_2_30, 'N/A')
	IFF(v_pmd4j_use_code = 'OB', v_pmd4j_address_line_1 || ' ' || v_pmd4j_addr_lin_2_pos_1 || v_pmd4j_addr_lin_2_pos_2_30, 'N/A') AS v_ObligeeName,
	-- *INF*: UPPER(LTRIM(RTRIM(in_comments_reason_suspended)))
	UPPER(LTRIM(RTRIM(in_comments_reason_suspended))) AS v_comments_reason_suspended,
	-- *INF*: UPPER(LTRIM(RTRIM(in_comments_area)))
	UPPER(LTRIM(RTRIM(in_comments_area))) AS v_comments_area,
	v_PolicyOfferingCode_Flag AS o_PolicyOfferingCode_Flag,
	v_PolicyOfferingCode AS o_PolicyOfferingCode,
	-- *INF*: IIF(ISNULL(v_PolicyOfferingAKId), 26, v_PolicyOfferingAKId)
	IFF(v_PolicyOfferingAKId IS NULL, 26, v_PolicyOfferingAKId) AS o_PolicyOfferingAKId,
	-- *INF*: IIF(v_ObligeeName=' ', 'N/A', v_ObligeeName)
	IFF(v_ObligeeName = ' ', 'N/A', v_ObligeeName) AS o_ObligeeName,
	-- *INF*: IIF(v_UNDND_UNDERWRITER_ID='9999','Y','N')
	IFF(v_UNDND_UNDERWRITER_ID = '9999', 'Y', 'N') AS o_AutomatedUnderwritingServicesIndicator,
	-- *INF*: IIF(v_comments_reason_suspended='ZZ' AND IN(v_comments_area, 'SBOP AUTOMATIC') AND pif_risk_state_prov != '16' AND IN(pif_renewal_code, '1', '2'), '1', '0')
	IFF(v_comments_reason_suspended = 'ZZ' AND IN(v_comments_area, 'SBOP AUTOMATIC') AND pif_risk_state_prov != '16' AND IN(pif_renewal_code, '1', '2'), '1', '0') AS o_AutomaticRenewalIndicator,
	SQ_pif_02_stage.pif_service_center
	FROM SQ_pif_02_stage
	LEFT JOIN LKP_PIF_04Stage
	ON LKP_PIF_04Stage.PIF_SYMBOL = SQ_pif_02_stage.pif_symbol AND LKP_PIF_04Stage.PIF_POLICY_NUMBER = SQ_pif_02_stage.pif_policy_number AND LKP_PIF_04Stage.PIF_MODULE = SQ_pif_02_stage.pif_module
	LEFT JOIN LKP_pif_03_stage
	ON LKP_pif_03_stage.pif_symbol = SQ_pif_02_stage.pif_symbol AND LKP_pif_03_stage.pif_policy_number = SQ_pif_02_stage.pif_policy_number AND LKP_pif_03_stage.pif_module = SQ_pif_02_stage.pif_module
	LEFT JOIN LKP_pif_43gj_stage
	ON LKP_pif_43gj_stage.pif_symbol = SQ_pif_02_stage.pif_symbol AND LKP_pif_43gj_stage.pif_policy_number = SQ_pif_02_stage.pif_policy_number AND LKP_pif_43gj_stage.pif_module = SQ_pif_02_stage.pif_module
	LEFT JOIN LKP_POLICYOFFERINGCODE LKP_POLICYOFFERINGCODE_1_v_pif_symbol_02_F2
	ON LKP_POLICYOFFERINGCODE_1_v_pif_symbol_02_F2.SequenceNumber = 1
	AND LKP_POLICYOFFERINGCODE_1_v_pif_symbol_02_F2.Identifiers = v_pif_symbol_02_F2

	LEFT JOIN LKP_POLICYOFFERINGCODE LKP_POLICYOFFERINGCODE_2_v_pif_symbol_02_F1
	ON LKP_POLICYOFFERINGCODE_2_v_pif_symbol_02_F1.SequenceNumber = 2
	AND LKP_POLICYOFFERINGCODE_2_v_pif_symbol_02_F1.Identifiers = v_pif_symbol_02_F1

	LEFT JOIN LKP_POLICYOFFERINGCODE LKP_POLICYOFFERINGCODE_3_v_pif_symbol_02_F2_in_sar_risk_unit_group_4514
	ON LKP_POLICYOFFERINGCODE_3_v_pif_symbol_02_F2_in_sar_risk_unit_group_4514.SequenceNumber = 3
	AND LKP_POLICYOFFERINGCODE_3_v_pif_symbol_02_F2_in_sar_risk_unit_group_4514.Identifiers = v_pif_symbol_02_F2 || '&' || in_sar_risk_unit_group_4514

	LEFT JOIN LKP_POLICYOFFERINGCODE LKP_POLICYOFFERINGCODE_4_v_pif_symbol_02_F2_in_sar_risk_unit_group_4514_in_sar_insurance_line_4514
	ON LKP_POLICYOFFERINGCODE_4_v_pif_symbol_02_F2_in_sar_risk_unit_group_4514_in_sar_insurance_line_4514.SequenceNumber = 4
	AND LKP_POLICYOFFERINGCODE_4_v_pif_symbol_02_F2_in_sar_risk_unit_group_4514_in_sar_insurance_line_4514.Identifiers = v_pif_symbol_02_F2 || '&' || in_sar_risk_unit_group_4514 || '&' || in_sar_insurance_line_4514

	LEFT JOIN LKP_POLICYOFFERINGCODE LKP_POLICYOFFERINGCODE_5_v_pif_symbol_02_F2_in_sar_sub_line_4514_in_sar_insurance_line_4514_v_ClassCode
	ON LKP_POLICYOFFERINGCODE_5_v_pif_symbol_02_F2_in_sar_sub_line_4514_in_sar_insurance_line_4514_v_ClassCode.SequenceNumber = 5
	AND LKP_POLICYOFFERINGCODE_5_v_pif_symbol_02_F2_in_sar_sub_line_4514_in_sar_insurance_line_4514_v_ClassCode.Identifiers = v_pif_symbol_02_F2 || '&' || in_sar_sub_line_4514 || '&' || in_sar_insurance_line_4514 || '&' || v_ClassCode

	LEFT JOIN LKP_POLICYOFFERINGCODE LKP_POLICYOFFERINGCODE_6_v_pif_symbol_02_F2_in_sar_insurance_line_4514_in_sar_major_peril_4514
	ON LKP_POLICYOFFERINGCODE_6_v_pif_symbol_02_F2_in_sar_insurance_line_4514_in_sar_major_peril_4514.SequenceNumber = 6
	AND LKP_POLICYOFFERINGCODE_6_v_pif_symbol_02_F2_in_sar_insurance_line_4514_in_sar_major_peril_4514.Identifiers = v_pif_symbol_02_F2 || '&' || in_sar_insurance_line_4514 || '&' || in_sar_major_peril_4514

	LEFT JOIN LKP_POLICYOFFERINGCODE LKP_POLICYOFFERINGCODE_7_v_pif_symbol_02_F2
	ON LKP_POLICYOFFERINGCODE_7_v_pif_symbol_02_F2.SequenceNumber = 7
	AND LKP_POLICYOFFERINGCODE_7_v_pif_symbol_02_F2.Identifiers = v_pif_symbol_02_F2

	LEFT JOIN LKP_POLICYOFFERINGCODE LKP_POLICYOFFERINGCODE_8_v_pif_symbol_02_F2
	ON LKP_POLICYOFFERINGCODE_8_v_pif_symbol_02_F2.SequenceNumber = 8
	AND LKP_POLICYOFFERINGCODE_8_v_pif_symbol_02_F2.Identifiers = v_pif_symbol_02_F2

	LEFT JOIN LKP_POLICYOFFERINGCODE LKP_POLICYOFFERINGCODE_9_v_pif_symbol_02_F2_in_sar_sub_line_4514_in_sar_insurance_line_4514
	ON LKP_POLICYOFFERINGCODE_9_v_pif_symbol_02_F2_in_sar_sub_line_4514_in_sar_insurance_line_4514.SequenceNumber = 9
	AND LKP_POLICYOFFERINGCODE_9_v_pif_symbol_02_F2_in_sar_sub_line_4514_in_sar_insurance_line_4514.Identifiers = v_pif_symbol_02_F2 || '&' || in_sar_sub_line_4514 || '&' || in_sar_insurance_line_4514

	LEFT JOIN LKP_POLICYOFFERINGCODE LKP_POLICYOFFERINGCODE_10_v_pif_symbol_02_F2_in_sar_insurance_line_4514_in_sar_risk_unit_group_4514
	ON LKP_POLICYOFFERINGCODE_10_v_pif_symbol_02_F2_in_sar_insurance_line_4514_in_sar_risk_unit_group_4514.SequenceNumber = 10
	AND LKP_POLICYOFFERINGCODE_10_v_pif_symbol_02_F2_in_sar_insurance_line_4514_in_sar_risk_unit_group_4514.Identifiers = v_pif_symbol_02_F2 || '&' || in_sar_insurance_line_4514 || '&' || in_sar_risk_unit_group_4514

	LEFT JOIN LKP_POLICYOFFERINGCODE LKP_POLICYOFFERINGCODE_11_v_pif_symbol_02_F2_in_sar_insurance_line_4514_in_sar_risk_unit_group_4514
	ON LKP_POLICYOFFERINGCODE_11_v_pif_symbol_02_F2_in_sar_insurance_line_4514_in_sar_risk_unit_group_4514.SequenceNumber = 11
	AND LKP_POLICYOFFERINGCODE_11_v_pif_symbol_02_F2_in_sar_insurance_line_4514_in_sar_risk_unit_group_4514.Identifiers = v_pif_symbol_02_F2 || '&' || in_sar_insurance_line_4514 || '&' || in_sar_risk_unit_group_4514

	LEFT JOIN LKP_POLICYOFFERINGCODE LKP_POLICYOFFERINGCODE_12_ltrim_rtrim_pif_symbol
	ON LKP_POLICYOFFERINGCODE_12_ltrim_rtrim_pif_symbol.SequenceNumber = 12
	AND LKP_POLICYOFFERINGCODE_12_ltrim_rtrim_pif_symbol.Identifiers = ltrim(rtrim(pif_symbol))

	LEFT JOIN LKP_POLICYOFFERINGAKID LKP_POLICYOFFERINGAKID_v_PolicyOfferingCode
	ON LKP_POLICYOFFERINGAKID_v_PolicyOfferingCode.PolicyOfferingCode = v_PolicyOfferingCode

),
FIL_MIN AS (
	SELECT
	pif_symbol, 
	pif_policy_number, 
	pif_module, 
	pif_master_co_number_a, 
	pif_location_a, 
	pif_amend_number, 
	pif_eff_yr_a, 
	pif_eff_mo_a, 
	pif_eff_da_a, 
	pif_exp_yr_a, 
	pif_exp_mo_a, 
	pif_exp_da_a, 
	pif_installment_term_a, 
	pif_risk_state_prov, 
	pif_company_number, 
	pif_full_agency_number, 
	pif_ent_yr_a, 
	pif_ent_mo_a, 
	pif_ent_da_a, 
	pif_line_business, 
	pif_issue_code, 
	pif_company_line, 
	pif_pay_service_code, 
	pif_audit_code, 
	pif_variation_code, 
	pif_producer_code, 
	pif_review_code, 
	pif_mvr_report_year, 
	pif_risk_grade_guide, 
	pif_renewal_code, 
	pif_reason_amended, 
	pif_renew_policy_symbol, 
	pif_renew_policy_number, 
	pif_original_incept, 
	wb_class_of_business, 
	pif_guarantee_ind, 
	wb_renewal_discount, 
	wb_inview_indicator, 
	pif_occupation, 
	pif_final_audit_ind, 
	pif_excess_claim_ind, 
	pif_anniversary_rerate, 
	pif_policy_status_on_pif, 
	pif_uk_postal_code, 
	pif_nonsmoker_discount, 
	pif_upload_audit, 
	pif_zip_ind, 
	pif_prgm_id, 
	pif_wbc_county, 
	pif_orig_act_date, 
	pif_fn_date, 
	pif_terrorism_risk, 
	pif_target_market_code, 
	pif_completed_audit_date, 
	pif_seg_id, 
	pif_renewal_safe_record_ctr, 
	o_PolicyOfferingCode_Flag AS PolicyOfferingCode_Flag, 
	o_PolicyOfferingCode AS PolicyOfferingCode, 
	o_PolicyOfferingAKId AS PolicyOfferingAKId, 
	o_ObligeeName AS ObligeeName, 
	o_AutomatedUnderwritingServicesIndicator AS AutomatedUnderwritingServicesIndicator, 
	o_AutomaticRenewalIndicator AS AutomaticRenewalIndicator, 
	pif_service_center, 
	pif_dc_bill_ind
	FROM EXP_PolicyOfferingAKId
	WHERE PolicyOfferingCode_Flag='1'
),
EXP_values AS (
	SELECT
	pif_symbol AS in_pif_symbol,
	-- *INF*: in_pif_symbol
	-- --ltrim(rtrim(in_pif_symbol))
	in_pif_symbol AS pif_symbol,
	-- *INF*: SUBSTR(in_pif_symbol,1,1)
	SUBSTR(in_pif_symbol, 1, 1) AS pif_symbol_1,
	pif_policy_number AS in_pif_policy_number,
	-- *INF*: ltrim(rtrim(in_pif_policy_number))
	ltrim(rtrim(in_pif_policy_number)) AS v_pif_pol_number,
	v_pif_pol_number AS pif_pol_number,
	-- *INF*: SUBSTR(v_pif_pol_number,1,1)
	SUBSTR(v_pif_pol_number, 1, 1) AS pif_pol_number_1,
	pif_module AS in_pif_module,
	-- *INF*: ltrim(rtrim(in_pif_module))
	ltrim(rtrim(in_pif_module)) AS pif_module,
	-- *INF*: ltrim(rtrim(in_pif_symbol)) || ltrim(rtrim(in_pif_policy_number)) || ltrim(rtrim(in_pif_module))
	ltrim(rtrim(in_pif_symbol)) || ltrim(rtrim(in_pif_policy_number)) || ltrim(rtrim(in_pif_module)) AS v_policy_key,
	-- *INF*: ltrim(rtrim(v_policy_key))
	ltrim(rtrim(v_policy_key)) AS policy_key,
	pif_master_co_number_a,
	-- *INF*: iif(isnull(pif_master_co_number_a) or is_spaces(pif_master_co_number_a) or LENGTH(pif_master_co_number_a)=0,'N/A',LTRIM(RTRIM(pif_master_co_number_a)))
	IFF(pif_master_co_number_a IS NULL OR is_spaces(pif_master_co_number_a) OR LENGTH(pif_master_co_number_a) = 0, 'N/A', LTRIM(RTRIM(pif_master_co_number_a))) AS mco,
	pif_eff_yr_a,
	-- *INF*: IIF(pif_eff_yr_a = '9999', '1800', pif_eff_yr_a)
	IFF(pif_eff_yr_a = '9999', '1800', pif_eff_yr_a) AS v_pif_eff_yr_a,
	pif_eff_mo_a,
	-- *INF*: IIF(pif_eff_mo_a > '12', '01', pif_eff_mo_a)
	-- --IIF(TO_INTEGER(pif_eff_mo_a) > 12, '01', pif_eff_mo_a)
	IFF(pif_eff_mo_a > '12', '01', pif_eff_mo_a) AS v_pif_eff_mo_a,
	pif_eff_da_a,
	-- *INF*: IIF(pif_eff_da_a > '31', '01', pif_eff_da_a)
	-- --IIF(TO_INTEGER(pif_eff_da_a) > 31, '01', pif_eff_da_a)
	IFF(pif_eff_da_a > '31', '01', pif_eff_da_a) AS v_pif_eff_da_a,
	-- *INF*: TO_DATE(v_pif_eff_yr_a ||'-' || v_pif_eff_mo_a || '-' || v_pif_eff_da_a,'YYYY-MM-DD')
	TO_DATE(v_pif_eff_yr_a || '-' || v_pif_eff_mo_a || '-' || v_pif_eff_da_a, 'YYYY-MM-DD') AS v_pol_eff_date,
	v_pol_eff_date AS pol_eff_date,
	pif_exp_yr_a,
	-- *INF*: IIF(pif_exp_yr_a = '9999', '2100', pif_exp_yr_a)
	IFF(pif_exp_yr_a = '9999', '2100', pif_exp_yr_a) AS v_pif_exp_yr_a,
	pif_exp_mo_a,
	-- *INF*: IIF(TO_INTEGER(pif_exp_mo_a) > 12, '12', pif_exp_mo_a)
	IFF(TO_INTEGER(pif_exp_mo_a) > 12, '12', pif_exp_mo_a) AS v_pif_exp_mo_a,
	pif_exp_da_a,
	-- *INF*: IIF(TO_INTEGER(pif_exp_da_a) > 31, '31', pif_exp_da_a)
	IFF(TO_INTEGER(pif_exp_da_a) > 31, '31', pif_exp_da_a) AS v_pif_exp_da_a,
	-- *INF*: TO_DATE(v_pif_exp_yr_a ||'-' || v_pif_exp_mo_a || '-' || v_pif_exp_da_a,'YYYY-MM-DD')
	TO_DATE(v_pif_exp_yr_a || '-' || v_pif_exp_mo_a || '-' || v_pif_exp_da_a, 'YYYY-MM-DD') AS v_pol_exp_date,
	-- *INF*: TO_DATE( v_pif_exp_yr_a ||'-' || v_pif_exp_mo_a || '-' ||  v_pif_exp_da_a,'YYYY-MM-DD')
	TO_DATE(v_pif_exp_yr_a || '-' || v_pif_exp_mo_a || '-' || v_pif_exp_da_a, 'YYYY-MM-DD') AS pol_exp_date,
	pif_installment_term_a AS in_pif_installment_term_a,
	-- *INF*: iif(isnull(in_pif_installment_term_a) or is_spaces(in_pif_installment_term_a) or LENGTH(in_pif_installment_term_a)=0,'N/A',LTRIM(RTRIM(in_pif_installment_term_a)))
	IFF(in_pif_installment_term_a IS NULL OR is_spaces(in_pif_installment_term_a) OR LENGTH(in_pif_installment_term_a) = 0, 'N/A', LTRIM(RTRIM(in_pif_installment_term_a))) AS pol_term,
	pif_risk_state_prov AS in_pif_risk_state_prov,
	-- *INF*: iif(isnull(in_pif_risk_state_prov) or is_spaces(in_pif_risk_state_prov) or LENGTH(in_pif_risk_state_prov)=0,'N/A' ,LTRIM(RTRIM(in_pif_risk_state_prov)))
	IFF(in_pif_risk_state_prov IS NULL OR is_spaces(in_pif_risk_state_prov) OR LENGTH(in_pif_risk_state_prov) = 0, 'N/A', LTRIM(RTRIM(in_pif_risk_state_prov))) AS state_of_domicile_code,
	-- *INF*: iif(isnull(in_pif_risk_state_prov) or is_spaces(in_pif_risk_state_prov) or LENGTH(in_pif_risk_state_prov)=0,'N/A',IIF(LTRIM(RTRIM(in_pif_risk_state_prov))='00', '0', LTRIM(LTRIM(RTRIM(in_pif_risk_state_prov)), '0')))
	IFF(in_pif_risk_state_prov IS NULL OR is_spaces(in_pif_risk_state_prov) OR LENGTH(in_pif_risk_state_prov) = 0, 'N/A', IFF(LTRIM(RTRIM(in_pif_risk_state_prov)) = '00', '0', LTRIM(LTRIM(RTRIM(in_pif_risk_state_prov)), '0'))) AS state_of_domicile_code_without0,
	pif_company_number AS in_pif_company_number,
	-- *INF*: iif(isnull(in_pif_company_number) or is_spaces(in_pif_company_number) or LENGTH(in_pif_company_number)=0,'N/A',LTRIM(RTRIM(in_pif_company_number)))
	IFF(in_pif_company_number IS NULL OR is_spaces(in_pif_company_number) OR LENGTH(in_pif_company_number) = 0, 'N/A', LTRIM(RTRIM(in_pif_company_number))) AS pol_co_num,
	pif_ent_yr_a,
	pif_ent_mo_a,
	pif_ent_da_a,
	pif_line_business AS in_pif_line_business,
	-- *INF*: iif(isnull(in_pif_line_business) or is_spaces(in_pif_line_business) or LENGTH(in_pif_line_business)=0,'N/A',LTRIM(RTRIM(in_pif_line_business)))
	IFF(in_pif_line_business IS NULL OR is_spaces(in_pif_line_business) OR LENGTH(in_pif_line_business) = 0, 'N/A', LTRIM(RTRIM(in_pif_line_business))) AS pif_lob_code,
	pif_company_line,
	-- *INF*: iif(isnull(pif_company_line) or is_spaces(pif_company_line) or LENGTH(pif_company_line)=0,'N/A',LTRIM(RTRIM(pif_company_line)))
	IFF(pif_company_line IS NULL OR is_spaces(pif_company_line) OR LENGTH(pif_company_line) = 0, 'N/A', LTRIM(RTRIM(pif_company_line))) AS pol_co_line_code,
	pif_pay_service_code AS in_pif_pay_service_code,
	-- *INF*:  DECODE(:UDF.DEFAULT_VALUE_FOR_STRINGS(in_pif_pay_service_code),
	--                       'R' ,'BCMS billing'  ,
	--                       'A', 'Agency Billing ' ,
	--                       'D' , 'PMS Direct Bill',
	--                      'E' , 'Mortgage Bill',
	--                      'P', 'Premium Pay',
	--                      'N/A ')
	--  
	--  
	DECODE(:UDF.DEFAULT_VALUE_FOR_STRINGS(in_pif_pay_service_code),
	'R', 'BCMS billing',
	'A', 'Agency Billing ',
	'D', 'PMS Direct Bill',
	'E', 'Mortgage Bill',
	'P', 'Premium Pay',
	'N/A ') AS BillingType,
	pif_renewal_code AS in_pif_renewal_code,
	pif_reason_amended AS in_pif_reason_amended,
	-- *INF*: iif(in_pif_renewal_code='9','Y','N')
	IFF(in_pif_renewal_code = '9', 'Y', 'N') AS pol_cancellation_ind,
	-- *INF*: iif(isnull(in_pif_renewal_code) or is_spaces(in_pif_renewal_code) or LENGTH(in_pif_renewal_code)=0,'N/A',LTRIM(RTRIM(in_pif_renewal_code)))
	IFF(in_pif_renewal_code IS NULL OR is_spaces(in_pif_renewal_code) OR LENGTH(in_pif_renewal_code) = 0, 'N/A', LTRIM(RTRIM(in_pif_renewal_code))) AS renl_code,
	pif_original_incept AS in_pif_original_incept,
	-- *INF*: iif(length(ltrim(rtrim(substr(in_pif_original_incept,1,4))))<>4,'1800',substr(in_pif_original_incept,1,4))
	IFF(length(ltrim(rtrim(substr(in_pif_original_incept, 1, 4)))) <> 4, '1800', substr(in_pif_original_incept, 1, 4)) AS v_orig_incep_date_year,
	-- *INF*: iif(substr(in_pif_original_incept,5,2)>'12','12',substr(in_pif_original_incept,5,2))
	IFF(substr(in_pif_original_incept, 5, 2) > '12', '12', substr(in_pif_original_incept, 5, 2)) AS v_orig_incep_date_month,
	-- *INF*: iif(IS_DATE(v_orig_incep_date_year ||'-' || v_orig_incep_date_month || '-' || '01','YYYY-MM-DD'),TO_DATE(v_orig_incep_date_year ||'-' || v_orig_incep_date_month || '-' || '01','YYYY-MM-DD'),to_date('01/01/1800','MM/DD/YYYY'))
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- --TO_DATE(v_orig_incep_date_year ||'-' || v_orig_incep_date_month || '-' || '01','YYYY-MM-DD')
	IFF(IS_DATE(v_orig_incep_date_year || '-' || v_orig_incep_date_month || '-' || '01', 'YYYY-MM-DD'), TO_DATE(v_orig_incep_date_year || '-' || v_orig_incep_date_month || '-' || '01', 'YYYY-MM-DD'), to_date('01/01/1800', 'MM/DD/YYYY')) AS v_orig_incep_date,
	v_orig_incep_date AS orig_incep_date,
	wb_inview_indicator AS in_wb_inview_indicator,
	-- *INF*: iif(isnull(in_wb_inview_indicator) or is_spaces(in_wb_inview_indicator) or LENGTH(in_wb_inview_indicator)=0,'N/A',LTRIM(RTRIM(in_wb_inview_indicator)))
	IFF(in_wb_inview_indicator IS NULL OR is_spaces(in_wb_inview_indicator) OR LENGTH(in_wb_inview_indicator) = 0, 'N/A', LTRIM(RTRIM(in_wb_inview_indicator))) AS wbconnect_upload_code,
	pif_occupation AS in_pif_occupation,
	-- *INF*: to_char(TO_INTEGER(in_pif_occupation))
	to_char(TO_INTEGER(in_pif_occupation)) AS v_pif_occupation,
	-- *INF*: iif(isnull(v_pif_occupation) or is_spaces(v_pif_occupation) or LENGTH(v_pif_occupation)=0,'N/A',LTRIM(RTRIM(LPAD(v_pif_occupation,5,'0'))))
	IFF(v_pif_occupation IS NULL OR is_spaces(v_pif_occupation) OR LENGTH(v_pif_occupation) = 0, 'N/A', LTRIM(RTRIM(LPAD(v_pif_occupation, 5, '0')))) AS prim_bus_class_code,
	pif_terrorism_risk AS in_pif_terrorism_risk,
	-- *INF*: iif(isnull(in_pif_terrorism_risk) or is_spaces(in_pif_terrorism_risk) or LENGTH(in_pif_terrorism_risk)=0,'N/A',LTRIM(RTRIM(in_pif_terrorism_risk)))
	IFF(in_pif_terrorism_risk IS NULL OR is_spaces(in_pif_terrorism_risk) OR LENGTH(in_pif_terrorism_risk) = 0, 'N/A', LTRIM(RTRIM(in_pif_terrorism_risk))) AS terrorism_risk_ind,
	pif_renew_policy_symbol AS in_pif_renew_policy_symbol,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_pif_renew_policy_symbol)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_pif_renew_policy_symbol) AS out_pif_renew_policy_symbol,
	pif_renew_policy_number AS in_pif_renew_policy_number,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_pif_renew_policy_number)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_pif_renew_policy_number) AS out_pif_renew_policy_number,
	pif_uk_postal_code AS in_pif_uk_postal_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(substr(in_pif_uk_postal_code,1,2))
	:UDF.DEFAULT_VALUE_FOR_STRINGS(substr(in_pif_uk_postal_code, 1, 2)) AS RenewalPolicyMod,
	-- *INF*: in_pif_renew_policy_symbol||in_pif_renew_policy_number||substr(in_pif_uk_postal_code,1,2)
	in_pif_renew_policy_symbol || in_pif_renew_policy_number || substr(in_pif_uk_postal_code, 1, 2) AS v_prior_pol_key,
	-- *INF*: iif(isnull(v_prior_pol_key) or is_spaces(v_prior_pol_key) or LENGTH(v_prior_pol_key)=0,'N/A',LTRIM(RTRIM(v_prior_pol_key)))
	IFF(v_prior_pol_key IS NULL OR is_spaces(v_prior_pol_key) OR LENGTH(v_prior_pol_key) = 0, 'N/A', LTRIM(RTRIM(v_prior_pol_key))) AS prior_pol_key,
	pif_issue_code AS in_pif_issue_code,
	-- *INF*: IIF(
	--   in_pif_issue_code='M' OR in_pif_issue_code='N',
	--   '1'
	-- )
	-- --iif(in_pif_issue_code='M','1',IIF(in_pif_issue_code='N','1'))
	IFF(in_pif_issue_code = 'M' OR in_pif_issue_code = 'N', '1') AS v_pif_issue_code,
	-- *INF*: iif((in_pif_original_incept=(pif_eff_yr_a || pif_eff_mo_a)) and v_pif_issue_code='1','N','R')
	IFF(( in_pif_original_incept = ( pif_eff_yr_a || pif_eff_mo_a ) ) AND v_pif_issue_code = '1', 'N', 'R') AS pol_issue_code,
	-- *INF*: iif(GET_DATE_PART(v_orig_incep_date,'YYYY')=1800,-1,DATE_DIFF(trunc(v_pol_eff_date,'YYYY'),trunc(v_orig_incep_date,'YYYY'),'YYYY'))
	-- 
	-- 
	-- 
	-- --DATE_DIFF(trunc(v_pol_eff_date,'YYYY'),trunc(v_orig_incep_date,'YYYY'),'YYYY')
	IFF(GET_DATE_PART(v_orig_incep_date, 'YYYY') = 1800, - 1, DATE_DIFF(trunc(v_pol_eff_date, 'YYYY'), trunc(v_orig_incep_date, 'YYYY'), 'YYYY')) AS v_pol_age,
	v_pol_age AS v_v_pol_age,
	-- *INF*: abs(v_pol_age)
	abs(v_pol_age) AS pol_age,
	pif_risk_grade_guide AS in_pif_risk_grade_guide,
	-- *INF*: iif(isnull(in_pif_risk_grade_guide) or is_spaces(in_pif_risk_grade_guide) or LENGTH(in_pif_risk_grade_guide)=0,'N/A',LTRIM(RTRIM(in_pif_risk_grade_guide)))
	IFF(in_pif_risk_grade_guide IS NULL OR is_spaces(in_pif_risk_grade_guide) OR LENGTH(in_pif_risk_grade_guide) = 0, 'N/A', LTRIM(RTRIM(in_pif_risk_grade_guide))) AS industry_risk_grade_code,
	pif_amend_number AS in_pif_amend_number,
	-- *INF*: iif(isnull(in_pif_amend_number) or is_spaces(in_pif_amend_number) or LENGTH(in_pif_amend_number)=0,'N/A',LTRIM(RTRIM(in_pif_amend_number)))
	IFF(in_pif_amend_number IS NULL OR is_spaces(in_pif_amend_number) OR LENGTH(in_pif_amend_number) = 0, 'N/A', LTRIM(RTRIM(in_pif_amend_number))) AS amend_num,
	pif_anniversary_rerate AS in_pif_anniversary_rerate,
	-- *INF*: iif(isnull(in_pif_anniversary_rerate) or is_spaces(in_pif_anniversary_rerate) or LENGTH(in_pif_anniversary_rerate)=0,'N/A',LTRIM(RTRIM(in_pif_anniversary_rerate)))
	IFF(in_pif_anniversary_rerate IS NULL OR is_spaces(in_pif_anniversary_rerate) OR LENGTH(in_pif_anniversary_rerate) = 0, 'N/A', LTRIM(RTRIM(in_pif_anniversary_rerate))) AS anniversary_rerate_code,
	pif_audit_code AS in_pif_audit_code,
	-- *INF*: iif(isnull(in_pif_audit_code) or is_spaces(in_pif_audit_code) or LENGTH(in_pif_audit_code)=0,'N/A',LTRIM(RTRIM(in_pif_audit_code)))
	IFF(in_pif_audit_code IS NULL OR is_spaces(in_pif_audit_code) OR LENGTH(in_pif_audit_code) = 0, 'N/A', LTRIM(RTRIM(in_pif_audit_code))) AS pol_audit_frqncy,
	pif_final_audit_ind AS in_pif_final_audit_ind,
	-- *INF*: iif(isnull(in_pif_final_audit_ind) or is_spaces(in_pif_final_audit_ind) or LENGTH(in_pif_final_audit_ind)=0,'N/A',LTRIM(RTRIM(in_pif_final_audit_ind)))
	IFF(in_pif_final_audit_ind IS NULL OR is_spaces(in_pif_final_audit_ind) OR LENGTH(in_pif_final_audit_ind) = 0, 'N/A', LTRIM(RTRIM(in_pif_final_audit_ind))) AS final_audit_code,
	pif_zip_ind AS in_pif_zip_ind,
	-- *INF*: iif(isnull(in_pif_zip_ind) or is_spaces(in_pif_zip_ind) or LENGTH(in_pif_zip_ind)=0,'N/A',LTRIM(RTRIM(in_pif_zip_ind)))
	IFF(in_pif_zip_ind IS NULL OR is_spaces(in_pif_zip_ind) OR LENGTH(in_pif_zip_ind) = 0, 'N/A', LTRIM(RTRIM(in_pif_zip_ind))) AS zip_ind,
	pif_prgm_id AS in_pif_prgm_id,
	-- *INF*: iif(isnull(in_pif_prgm_id) or is_spaces(in_pif_prgm_id) or LENGTH(in_pif_prgm_id)=0,'N/A',LTRIM(RTRIM(in_pif_prgm_id)))
	IFF(in_pif_prgm_id IS NULL OR is_spaces(in_pif_prgm_id) OR LENGTH(in_pif_prgm_id) = 0, 'N/A', LTRIM(RTRIM(in_pif_prgm_id))) AS v_pif_prgm_id,
	pif_guarantee_ind AS in_pif_guarantee_ind,
	-- *INF*: iif(isnull(in_pif_guarantee_ind) or is_spaces(in_pif_guarantee_ind) or LENGTH(in_pif_guarantee_ind)=0,'N/A',LTRIM(RTRIM(in_pif_guarantee_ind)))
	IFF(in_pif_guarantee_ind IS NULL OR is_spaces(in_pif_guarantee_ind) OR LENGTH(in_pif_guarantee_ind) = 0, 'N/A', LTRIM(RTRIM(in_pif_guarantee_ind))) AS gutantee_ind,
	pif_variation_code AS in_pif_variation_code,
	-- *INF*: iif(isnull(in_pif_variation_code) or is_spaces(in_pif_variation_code) or LENGTH(in_pif_variation_code)=0,'N/A',LTRIM(RTRIM(in_pif_variation_code)))
	IFF(in_pif_variation_code IS NULL OR is_spaces(in_pif_variation_code) OR LENGTH(in_pif_variation_code) = 0, 'N/A', LTRIM(RTRIM(in_pif_variation_code))) AS variation_code,
	pif_wbc_county AS in_pif_wbc_county,
	-- *INF*: iif(isnull(in_pif_wbc_county) or is_spaces(in_pif_wbc_county) or LENGTH(in_pif_wbc_county)=0,'N/A',LTRIM(RTRIM(in_pif_wbc_county)))
	IFF(in_pif_wbc_county IS NULL OR is_spaces(in_pif_wbc_county) OR LENGTH(in_pif_wbc_county) = 0, 'N/A', LTRIM(RTRIM(in_pif_wbc_county))) AS county,
	pif_nonsmoker_discount AS in_pif_nonsmoker_discount,
	-- *INF*: iif(isnull(in_pif_nonsmoker_discount) or is_spaces(in_pif_nonsmoker_discount) or LENGTH(in_pif_nonsmoker_discount)=0,'N/A',LTRIM(RTRIM(in_pif_nonsmoker_discount)))
	IFF(in_pif_nonsmoker_discount IS NULL OR is_spaces(in_pif_nonsmoker_discount) OR LENGTH(in_pif_nonsmoker_discount) = 0, 'N/A', LTRIM(RTRIM(in_pif_nonsmoker_discount))) AS non_smoker_disc_code,
	wb_renewal_discount AS in_wb_renewal_discount,
	-- *INF*: iif(isnull(in_wb_renewal_discount),0,in_wb_renewal_discount)
	IFF(in_wb_renewal_discount IS NULL, 0, in_wb_renewal_discount) AS renl_disc,
	pif_upload_audit AS in_pif_upload_audit,
	-- *INF*: iif(isnull(in_pif_upload_audit) or is_spaces(in_pif_upload_audit) or LENGTH(in_pif_upload_audit)=0,'N/A',in_pif_upload_audit)
	IFF(in_pif_upload_audit IS NULL OR is_spaces(in_pif_upload_audit) OR LENGTH(in_pif_upload_audit) = 0, 'N/A', in_pif_upload_audit) AS pif_upload_audit,
	pif_seg_id AS in_pif_seg_id,
	-- *INF*: iif(isnull(in_pif_seg_id) or is_spaces(in_pif_seg_id) or LENGTH(in_pif_seg_id)=0,'N/A',in_pif_seg_id)
	IFF(in_pif_seg_id IS NULL OR is_spaces(in_pif_seg_id) OR LENGTH(in_pif_seg_id) = 0, 'N/A', in_pif_seg_id) AS pif_seg_id,
	pif_renewal_safe_record_ctr AS in_pif_renewal_safe_record_ctr,
	-- *INF*: iif(isnull(in_pif_renewal_safe_record_ctr) ,-1,TO_INTEGER(in_pif_renewal_safe_record_ctr))
	IFF(in_pif_renewal_safe_record_ctr IS NULL, - 1, TO_INTEGER(in_pif_renewal_safe_record_ctr)) AS renl_safe_driver_disc_count,
	pif_fn_date AS in_pif_fn_date,
	-- *INF*: iif(isnull(in_pif_fn_date) or is_spaces(in_pif_fn_date) or LENGTH(in_pif_fn_date)=0,'21001231',in_pif_fn_date)
	IFF(in_pif_fn_date IS NULL OR is_spaces(in_pif_fn_date) OR LENGTH(in_pif_fn_date) = 0, '21001231', in_pif_fn_date) AS v_pif_fn_date,
	-- *INF*: to_date(substr(v_pif_fn_date,1,4) || '-' || substr(v_pif_fn_date,5,2) || '-' || substr(v_pif_fn_date,7,2),'YYYY-MM-DD')
	-- 
	-- 
	-- 
	to_date(substr(v_pif_fn_date, 1, 4) || '-' || substr(v_pif_fn_date, 5, 2) || '-' || substr(v_pif_fn_date, 7, 2), 'YYYY-MM-DD') AS fn_date,
	pif_completed_audit_date AS in_pif_completed_audit_date,
	-- *INF*: iif(isnull(in_pif_completed_audit_date) or is_spaces(in_pif_completed_audit_date) or LENGTH(in_pif_completed_audit_date)=0,'21001231',in_pif_completed_audit_date)
	IFF(in_pif_completed_audit_date IS NULL OR is_spaces(in_pif_completed_audit_date) OR LENGTH(in_pif_completed_audit_date) = 0, '21001231', in_pif_completed_audit_date) AS v_in_pif_completed_audit_date,
	-- *INF*: to_date(substr(v_in_pif_completed_audit_date,1,4) || '-' || substr(v_in_pif_completed_audit_date,5,2) || '-' || substr(v_in_pif_completed_audit_date,7,2),'YYYY-MM-DD')
	to_date(substr(v_in_pif_completed_audit_date, 1, 4) || '-' || substr(v_in_pif_completed_audit_date, 5, 2) || '-' || substr(v_in_pif_completed_audit_date, 7, 2), 'YYYY-MM-DD') AS audit_complt_date,
	pif_orig_act_date AS in_pif_orig_act_date,
	-- *INF*: iif(isnull(in_pif_orig_act_date) or is_spaces(in_pif_orig_act_date) or LENGTH(in_pif_orig_act_date)=0
	--         ,'21001231'
	--         ,iif(LENGTH(ltrim(rtrim(in_pif_orig_act_date)))=7
	--              ,iif(is_date(ltrim(rtrim(in_pif_orig_act_date)) || '0','yyyymmdd')
	--                    ,ltrim(rtrim(in_pif_orig_act_date)) || '0'
	--                    ,ltrim(rtrim(in_pif_orig_act_date)) || '1'
	--                   )
	--              ,in_pif_orig_act_date)
	-- )
	IFF(in_pif_orig_act_date IS NULL OR is_spaces(in_pif_orig_act_date) OR LENGTH(in_pif_orig_act_date) = 0, '21001231', IFF(LENGTH(ltrim(rtrim(in_pif_orig_act_date))) = 7, IFF(is_date(ltrim(rtrim(in_pif_orig_act_date)) || '0', 'yyyymmdd'), ltrim(rtrim(in_pif_orig_act_date)) || '0', ltrim(rtrim(in_pif_orig_act_date)) || '1'), in_pif_orig_act_date)) AS v_pif_orig_act_date,
	-- *INF*: to_date(substr(v_pif_orig_act_date,1,4) || '-' || substr(v_pif_orig_act_date,5,2) || '-' || substr(v_pif_orig_act_date,7,2),'YYYY-MM-DD')
	to_date(substr(v_pif_orig_act_date, 1, 4) || '-' || substr(v_pif_orig_act_date, 5, 2) || '-' || substr(v_pif_orig_act_date, 7, 2), 'YYYY-MM-DD') AS original_account_date,
	pif_ent_yr_a AS in_pif_ent_yr_a1,
	pif_ent_mo_a AS in_pif_ent_mo_a1,
	pif_ent_da_a AS in_pif_ent_da_a1,
	-- *INF*: iif(isnull(in_pif_ent_yr_a1) or is_spaces(in_pif_ent_yr_a1) or LENGTH(in_pif_ent_yr_a1)=0,'1800',LTRIM(RTRIM(in_pif_ent_yr_a1)))
	IFF(in_pif_ent_yr_a1 IS NULL OR is_spaces(in_pif_ent_yr_a1) OR LENGTH(in_pif_ent_yr_a1) = 0, '1800', LTRIM(RTRIM(in_pif_ent_yr_a1))) AS v_pif_ent_yr,
	-- *INF*: iif(isnull(in_pif_ent_mo_a1) or is_spaces(in_pif_ent_mo_a1) or LENGTH(in_pif_ent_mo_a1)=0,'01',LTRIM(RTRIM(in_pif_ent_mo_a1)))
	IFF(in_pif_ent_mo_a1 IS NULL OR is_spaces(in_pif_ent_mo_a1) OR LENGTH(in_pif_ent_mo_a1) = 0, '01', LTRIM(RTRIM(in_pif_ent_mo_a1))) AS v_pif_ent_mo,
	-- *INF*: iif(isnull(in_pif_ent_da_a1) or is_spaces(in_pif_ent_da_a1) or LENGTH(in_pif_ent_da_a1)=0,'01',LTRIM(RTRIM(in_pif_ent_da_a1)))
	IFF(in_pif_ent_da_a1 IS NULL OR is_spaces(in_pif_ent_da_a1) OR LENGTH(in_pif_ent_da_a1) = 0, '01', LTRIM(RTRIM(in_pif_ent_da_a1))) AS v_pif_ent_da,
	-- *INF*: TO_DATE(v_pif_ent_yr ||'-' || v_pif_ent_mo || '-' || v_pif_ent_da,'YYYY-MM-DD')
	TO_DATE(v_pif_ent_yr || '-' || v_pif_ent_mo || '-' || v_pif_ent_da, 'YYYY-MM-DD') AS pol_enter_date,
	pif_location_a AS in_pif_location_a,
	-- *INF*: iif(isnull(in_pif_location_a) or is_spaces(in_pif_location_a) or LENGTH(in_pif_location_a)=0,'N/A',LTRIM(RTRIM(in_pif_location_a)))
	IFF(in_pif_location_a IS NULL OR is_spaces(in_pif_location_a) OR LENGTH(in_pif_location_a) = 0, 'N/A', LTRIM(RTRIM(in_pif_location_a))) AS loc,
	pif_excess_claim_ind AS in_pif_excess_claim_ind,
	-- *INF*: iif(isnull(in_pif_excess_claim_ind) or is_spaces(in_pif_excess_claim_ind) or LENGTH(in_pif_excess_claim_ind)=0,'N/A',LTRIM(RTRIM(in_pif_excess_claim_ind)))
	IFF(in_pif_excess_claim_ind IS NULL OR is_spaces(in_pif_excess_claim_ind) OR LENGTH(in_pif_excess_claim_ind) = 0, 'N/A', LTRIM(RTRIM(in_pif_excess_claim_ind))) AS excess_claim_code,
	pif_policy_status_on_pif AS in_pif_policy_status_on_pif,
	-- *INF*: iif(isnull(in_pif_policy_status_on_pif) or is_spaces(in_pif_policy_status_on_pif) or LENGTH(in_pif_policy_status_on_pif)=0,'N/A',LTRIM(RTRIM(in_pif_policy_status_on_pif)))
	IFF(in_pif_policy_status_on_pif IS NULL OR is_spaces(in_pif_policy_status_on_pif) OR LENGTH(in_pif_policy_status_on_pif) = 0, 'N/A', LTRIM(RTRIM(in_pif_policy_status_on_pif))) AS pif_policy_status_on,
	pif_service_center,
	'N/A' AS reins_code,
	-- *INF*: ----IIF( SUBSTR(in_pif_reason_amended,1,1) = 'P' and in_pif_renewal_code = '9', SYSDATE,
	-- --TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS'))
	'' AS v_pol_cancellation_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS pol_cancellation_date,
	'N/A' AS pol_cancellation_rsn_code,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(pif_service_center) OR IS_SPACES(pif_service_center) OR LENGTH(pif_service_center) = 0 OR LTRIM(RTRIM(pif_service_center)) = '','N/A',
	-- pif_service_center )
	-- 
	-- --IIF(ISNULL(pif_service_center),'N/A',pif_service_center)
	DECODE(TRUE,
	pif_service_center IS NULL OR IS_SPACES(pif_service_center) OR LENGTH(pif_service_center) = 0 OR LTRIM(RTRIM(pif_service_center)) = '', 'N/A',
	pif_service_center) AS serv_center_support_code,
	pif_review_code AS in_pif_review_code,
	-- *INF*: iif(isnull(in_pif_review_code) or IS_SPACES(in_pif_review_code) or LENGTH(in_pif_review_code)=0,'N/A',LTRIM(RTRIM(in_pif_review_code)))
	IFF(in_pif_review_code IS NULL OR IS_SPACES(in_pif_review_code) OR LENGTH(in_pif_review_code) = 0, 'N/A', LTRIM(RTRIM(in_pif_review_code))) AS uw_review_yr,
	pif_mvr_report_year AS in_pif_mvr_report_year,
	-- *INF*: iif(isnull(in_pif_mvr_report_year) or IS_SPACES(in_pif_mvr_report_year) or LENGTH(in_pif_mvr_report_year)=0,'N/A',LTRIM(RTRIM(in_pif_mvr_report_year)))
	IFF(in_pif_mvr_report_year IS NULL OR IS_SPACES(in_pif_mvr_report_year) OR LENGTH(in_pif_mvr_report_year) = 0, 'N/A', LTRIM(RTRIM(in_pif_mvr_report_year))) AS mvr_request_code,
	pif_full_agency_number AS in_pif_full_agency_number,
	-- *INF*: SUBSTR(in_pif_full_agency_number,1,2) || SUBSTR(in_pif_full_agency_number,5,3)
	SUBSTR(in_pif_full_agency_number, 1, 2) || SUBSTR(in_pif_full_agency_number, 5, 3) AS v_agency_key,
	v_agency_key AS agency_key,
	pif_producer_code AS in_pif_producer_code,
	-- *INF*: iif(isnull(in_pif_producer_code) or IS_SPACES(in_pif_producer_code) or LENGTH(in_pif_producer_code)=0,'N/A',LTRIM(RTRIM(in_pif_producer_code)))
	IFF(in_pif_producer_code IS NULL OR IS_SPACES(in_pif_producer_code) OR LENGTH(in_pif_producer_code) = 0, 'N/A', LTRIM(RTRIM(in_pif_producer_code))) AS producer_code,
	-- *INF*: substr(in_pif_symbol,1,2)
	substr(in_pif_symbol, 1, 2) AS pkg_code,
	wb_class_of_business AS in_wb_class_of_business,
	-- *INF*: LTRIM(RTRIM(in_wb_class_of_business))
	LTRIM(RTRIM(in_wb_class_of_business)) AS v_wb_class_of_business1,
	-- *INF*: iif(isnull(v_wb_class_of_business1) or IS_SPACES(v_wb_class_of_business1) or LENGTH(v_wb_class_of_business1)=0,'N/A',v_wb_class_of_business1)
	IFF(v_wb_class_of_business1 IS NULL OR IS_SPACES(v_wb_class_of_business1) OR LENGTH(v_wb_class_of_business1) = 0, 'N/A', v_wb_class_of_business1) AS v_wb_class_of_business2,
	pif_target_market_code AS in_pif_target_market_code,
	-- *INF*: iif(isnull(in_pif_target_market_code) or IS_SPACES(in_pif_target_market_code) or LENGTH(in_pif_target_market_code)=0,'N/A',LTRIM(RTRIM(in_pif_target_market_code)))
	IFF(in_pif_target_market_code IS NULL OR IS_SPACES(in_pif_target_market_code) OR LENGTH(in_pif_target_market_code) = 0, 'N/A', LTRIM(RTRIM(in_pif_target_market_code))) AS pif_target_market_code,
	-- *INF*: --:LKP.LKP_MARKETING_AK_ID(pif_target_market_code,pkg_code,prog_code)
	'' AS v_marketing_ak_id,
	-1 AS emp_id,
	-- *INF*: --:LKP.LKP_PRODUCER_CODE_AK_ID(v_producer_code,v_agency_key,emp_id)
	'' AS v_producer_code_ak_id,
	-- *INF*: iif(isnull(v_marketing_ak_id),-1,v_marketing_ak_id)
	IFF(v_marketing_ak_id IS NULL, - 1, v_marketing_ak_id) AS mrktng_ak_id,
	-- *INF*: iif(isnull(v_producer_code_ak_id),-1,v_producer_code_ak_id)
	IFF(v_producer_code_ak_id IS NULL, - 1, v_producer_code_ak_id) AS producer_code_ak_id,
	-- *INF*: --:LKP.LKP_SUB_ASSOCIATION_PROGRAM_CODE(v_wb_class_of_business)
	'' AS v_sub_association_program_type,
	v_wb_class_of_business2 AS ClassOfBusiness,
	-- *INF*: IIF(v_pif_prgm_id='N/A',v_wb_class_of_business2,v_pif_prgm_id)
	IFF(v_pif_prgm_id = 'N/A', v_wb_class_of_business2, v_pif_prgm_id) AS ProgramCode,
	-- *INF*:   IIF(  SIGN(DATE_DIFF(v_pol_eff_date ,sysdate , 'DD')) <= 0 AND (
	--      SIGN(DATE_DIFF(sysdate,v_pol_exp_date , 'DD'))  <= 0 OR    SIGN(DATE_DIFF(sysdate,v_pol_cancellation_date , 'DD'))  <= 0 ), 'I',
	-- IIF(
	--  SIGN(DATE_DIFF(v_pol_cancellation_date ,sysdate , 'DD')) < 0 , 'C',
	-- IIF(
	--  SIGN(DATE_DIFF(v_pol_exp_date ,sysdate , 'DD')) <= 0
	-- and   ( TO_INTEGER(in_pif_renewal_code) < 9)
	-- , 'E', 'O'
	-- ) ))
	--  
	-- --- Not used at this time.
	-- -- Set to ""Inforce"" when policy effective date <= Current ?Date  AND Minimum of the Policy Expiration Date or Policy Cancellation Date => Current Date
	-- ---  Set to ""Cancelled""  when Policy Cancellation Date < Current Date 
	-- --- Set to ""Expired"" when Policy Expiration Date <= Current Date AND  PIF-RENEWAL-CODE < 9
	-- --- Set to ""Other"" for all policies that don't meet the criteria listed above.
	IFF(SIGN(DATE_DIFF(v_pol_eff_date, sysdate, 'DD')) <= 0 AND ( SIGN(DATE_DIFF(sysdate, v_pol_exp_date, 'DD')) <= 0 OR SIGN(DATE_DIFF(sysdate, v_pol_cancellation_date, 'DD')) <= 0 ), 'I', IFF(SIGN(DATE_DIFF(v_pol_cancellation_date, sysdate, 'DD')) < 0, 'C', IFF(SIGN(DATE_DIFF(v_pol_exp_date, sysdate, 'DD')) <= 0 AND ( TO_INTEGER(in_pif_renewal_code) < 9 ), 'E', 'O'))) AS v_policy_status_code1,
	'N/A' AS v_policy_status_code2,
	v_policy_status_code2 AS policy_status_code,
	PolicyOfferingCode,
	PolicyOfferingAKId,
	ObligeeName,
	AutomatedUnderwritingServicesIndicator,
	AutomaticRenewalIndicator,
	v_wb_class_of_business1 AS o_ClassOfBusiness,
	pif_dc_bill_ind
	FROM FIL_MIN
),
LKP_AgencyAKId AS (
	SELECT
	AgencyAKId,
	AgencyCode
	FROM (
		SELECT 
			AgencyAKId,
			AgencyCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Agency
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyCode ORDER BY AgencyAKId) = 1
),
LKP_AgencyEmployee AS (
	SELECT
	AgencyEmployeeAKID,
	AgencyAKID,
	ProducerCode
	FROM (
		SELECT 
		AgencyEmployee.AgencyEmployeeAKID as AgencyEmployeeAKID, 
		AgencyEmployee.AgencyEmployeeRole as AgencyEmployeeRole, 
		AgencyEmployee.AgencyAKID as AgencyAKID, 
		AgencyEmployee.ProducerCode as ProducerCode 
		FROM 
		AgencyEmployee
		where CurrentSnapshotFlag=1
		order by AgencyEmployeeAKID, ProducerCode, case when AgencyEmployeeRole = 'Principal' then 1 else 2 END, AgencyEmployeeID --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyAKID,ProducerCode ORDER BY AgencyEmployeeAKID) = 1
),
LKP_Association AS (
	SELECT
	AssociationCode
	FROM (
		SELECT 
			AssociationCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.Association
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AssociationCode ORDER BY AssociationCode) = 1
),
LKP_Policy AS (
	SELECT
	pol_id,
	pol_ak_id,
	contract_cust_ak_id,
	agency_ak_id,
	AgencyAKId,
	mco,
	pol_co_num,
	pol_eff_date,
	pol_exp_date,
	orig_incptn_date,
	prim_bus_class_code,
	reins_code,
	pms_pol_lob_code,
	pol_co_line_code,
	pol_cancellation_ind,
	pol_cancellation_date,
	pol_cancellation_rsn_code,
	state_of_domicile_code,
	wbconnect_upload_code,
	serv_center_support_code,
	pol_term,
	terrorism_risk_ind,
	prior_pol_key,
	pol_status_code,
	pol_issue_code,
	pol_age,
	industry_risk_grade_code,
	uw_review_yr,
	mvr_request_code,
	renl_code,
	amend_num,
	anniversary_rerate_code,
	pol_audit_frqncy,
	final_audit_code,
	zip_ind,
	guarantee_ind,
	variation_code,
	county,
	non_smoker_disc_code,
	renl_disc,
	renl_safe_driver_disc_count,
	nonrenewal_flag_date,
	audit_complt_date,
	orig_acct_date,
	pol_enter_date,
	excess_claim_code,
	pol_status_on_pif,
	target_mrkt_code,
	pkg_code,
	bus_seg_code,
	pif_upload_audit_ind,
	producer_code_ak_id,
	prdcr_code,
	ClassOfBusiness,
	strtgc_bus_dvsn_ak_id,
	RenewalPolicyNumber,
	RenewalPolicySymbol,
	RenewalPolicyMod,
	BillingType,
	strtgc_bus_dvsn_code,
	PolicyOfferingCode,
	ProgramCode,
	producer_code_id,
	strtgc_bus_dvsn_id,
	sup_bus_class_code_id,
	sup_pol_term_id,
	sup_pol_status_code_id,
	sup_pol_issue_code_id,
	sup_pol_audit_frqncy_id,
	sup_industry_risk_grade_code_id,
	sup_state_id,
	SurchargeExemptCode,
	SupSurchargeExemptID,
	StrategicProfitCenterAKId,
	InsuranceSegmentAKId,
	PolicyOfferingAKId,
	ProgramAKId,
	ObligeeName,
	AutomatedUnderwritingServicesIndicator,
	AutomaticRenewalIndicator,
	AssociationCode,
	AgencyEmployeeAKId,
	pol_key
	FROM (
		SELECT 
			pol_id,
			pol_ak_id,
			contract_cust_ak_id,
			agency_ak_id,
			AgencyAKId,
			mco,
			pol_co_num,
			pol_eff_date,
			pol_exp_date,
			orig_incptn_date,
			prim_bus_class_code,
			reins_code,
			pms_pol_lob_code,
			pol_co_line_code,
			pol_cancellation_ind,
			pol_cancellation_date,
			pol_cancellation_rsn_code,
			state_of_domicile_code,
			wbconnect_upload_code,
			serv_center_support_code,
			pol_term,
			terrorism_risk_ind,
			prior_pol_key,
			pol_status_code,
			pol_issue_code,
			pol_age,
			industry_risk_grade_code,
			uw_review_yr,
			mvr_request_code,
			renl_code,
			amend_num,
			anniversary_rerate_code,
			pol_audit_frqncy,
			final_audit_code,
			zip_ind,
			guarantee_ind,
			variation_code,
			county,
			non_smoker_disc_code,
			renl_disc,
			renl_safe_driver_disc_count,
			nonrenewal_flag_date,
			audit_complt_date,
			orig_acct_date,
			pol_enter_date,
			excess_claim_code,
			pol_status_on_pif,
			target_mrkt_code,
			pkg_code,
			bus_seg_code,
			pif_upload_audit_ind,
			producer_code_ak_id,
			prdcr_code,
			ClassOfBusiness,
			strtgc_bus_dvsn_ak_id,
			RenewalPolicyNumber,
			RenewalPolicySymbol,
			RenewalPolicyMod,
			BillingType,
			strtgc_bus_dvsn_code,
			PolicyOfferingCode,
			ProgramCode,
			producer_code_id,
			strtgc_bus_dvsn_id,
			sup_bus_class_code_id,
			sup_pol_term_id,
			sup_pol_status_code_id,
			sup_pol_issue_code_id,
			sup_pol_audit_frqncy_id,
			sup_industry_risk_grade_code_id,
			sup_state_id,
			SurchargeExemptCode,
			SupSurchargeExemptID,
			StrategicProfitCenterAKId,
			InsuranceSegmentAKId,
			PolicyOfferingAKId,
			ProgramAKId,
			ObligeeName,
			AutomatedUnderwritingServicesIndicator,
			AutomaticRenewalIndicator,
			AssociationCode,
			AgencyEmployeeAKId,
			pol_key
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy
		WHERE crrnt_snpsht_flag=1 AND source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_id) = 1
),
LKP_sup_association_program_code AS (
	SELECT
	assoc_prog_code
	FROM (
		SELECT 
			assoc_prog_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_association_program_code
		WHERE crrnt_snpsht_flag=1 AND source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND assoc_prog_type='Program'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY assoc_prog_code ORDER BY assoc_prog_code) = 1
),
LKP_ProgramAKId AS (
	SELECT
	ProgramAKId,
	in_ProgramCode,
	ProgramCode
	FROM (
		SELECT 
			ProgramAKId,
			in_ProgramCode,
			ProgramCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.Program
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProgramCode ORDER BY ProgramAKId) = 1
),
LKP_PIF_12_stage AS (
	SELECT
	use_code,
	pif_symbol,
	pif_policy_number,
	pif_module
	FROM (
		SELECT 
			use_code,
			pif_symbol,
			pif_policy_number,
			pif_module
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_12_stage
		WHERE use_code in ('EA','EP')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module ORDER BY use_code) = 1
),
exp_user_code AS (
	SELECT
	use_code AS in_use_code,
	-- *INF*: IIF(IN(in_use_code,'EA','EP'),in_use_code,'N/A')
	IFF(IN(in_use_code, 'EA', 'EP'), in_use_code, 'N/A') AS out_use_code
	FROM LKP_PIF_12_stage
),
LKP_SupSurchargeExempt AS (
	SELECT
	SupSurchargeExemptId,
	SurchargeExemptCode
	FROM (
		SELECT 
			SupSurchargeExemptId,
			SurchargeExemptCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupSurchargeExempt
		WHERE CurrentSnapshotFlag=1 AND SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SurchargeExemptCode ORDER BY SupSurchargeExemptId) = 1
),
LKP_agency_ak_id AS (
	SELECT
	agency_ak_id,
	agency_key
	FROM (
		SELECT 
			agency_ak_id,
			agency_key
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.agency
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY agency_key ORDER BY agency_ak_id) = 1
),
LKP_contract_customer_key AS (
	SELECT
	contract_cust_ak_id,
	contract_key
	FROM (
		SELECT 
		contract_customer.contract_cust_ak_id as contract_cust_ak_id, 
		ltrim(rtrim(contract_customer.contract_key)) as contract_key 
		FROM 
		contract_customer
		WHERE contract_customer.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY contract_key ORDER BY contract_cust_ak_id DESC) = 1
),
LKP_producer_code_ak_id AS (
	SELECT
	producer_code_id,
	prdcr_code_ak_id,
	producer_code,
	agency_key
	FROM (
		SELECT 
		a.producer_code_id as producer_code_id,
		a.prdcr_code_ak_id as prdcr_code_ak_id, 
		ltrim(rtrim(a.producer_code)) as producer_code, 
		ltrim(rtrim(a.agency_key)) as agency_key
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.producer_code a
		WHERE a.crrnt_snpsht_flag=1 AND a.source_sys_id = '@{pipeline().parameters.AGENCY_SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY producer_code,agency_key ORDER BY producer_code_id DESC) = 1
),
LKP_sup_business_classification_code AS (
	SELECT
	sup_bus_class_code_id,
	bus_class_code
	FROM (
		SELECT 
			sup_bus_class_code_id,
			bus_class_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_business_classification_code
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY bus_class_code ORDER BY sup_bus_class_code_id) = 1
),
LKP_sup_industry_risk_grade_code AS (
	SELECT
	sup_industry_risk_grade_code_id,
	industry_risk_grade_code
	FROM (
		SELECT 
			sup_industry_risk_grade_code_id,
			industry_risk_grade_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_industry_risk_grade_code
		WHERE crrnt_snpsht_flag=1 AND source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY industry_risk_grade_code ORDER BY sup_industry_risk_grade_code_id) = 1
),
LKP_sup_policy_audit_frequency AS (
	SELECT
	sup_pol_audit_frqncy_id,
	pol_audit_frqncy
	FROM (
		SELECT 
			sup_pol_audit_frqncy_id,
			pol_audit_frqncy
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_policy_audit_frequency
		WHERE crrnt_snpsht_flag=1 AND source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_audit_frqncy ORDER BY sup_pol_audit_frqncy_id) = 1
),
LKP_sup_policy_status_code AS (
	SELECT
	sup_pol_status_code_id,
	pol_status_code
	FROM (
		SELECT 
			sup_pol_status_code_id,
			pol_status_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_policy_status_code
		WHERE crrnt_snpsht_flag=1 AND source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_status_code ORDER BY sup_pol_status_code_id) = 1
),
LKP_sup_policy_term AS (
	SELECT
	sup_pol_term_id,
	pol_term
	FROM (
		SELECT 
			sup_pol_term_id,
			pol_term
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_policy_term
		WHERE crrnt_snpsht_flag=1 AND source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_term ORDER BY sup_pol_term_id) = 1
),
LKP_sup_state AS (
	SELECT
	sup_state_id,
	state_abbrev
	FROM (
		SELECT 
			sup_state_id,
			state_abbrev
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_state
		WHERE crrnt_snpsht_flag = 1 AND source_sys_id = 'EXCEED'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_abbrev ORDER BY sup_state_id) = 1
),
mplt_PMS_StrategicProfitCenterInsuranceSegment AS (WITH
	INPUT AS (
		
	),
	EXP_Compute_AS400_Values AS (
		SELECT
		pif_symbol AS in_pif_symbol,
		pif_pol_number AS in_pif_pol_number,
		pif_lob_code AS in_pif_lob_code,
		-- *INF*: DECODE(TRUE,
		-- 	IN(in_pif_symbol,'HXX','HXY') AND IN (in_pif_lob_code,'HAP','HP'),
		-- 	'1',
		-- 	IN(in_pif_symbol,'PXX','PXY') AND IN (in_pif_lob_code,'APV','HAP','HP'),
		-- 	'1',
		-- 	IN(in_pif_symbol,'PXX','PXY') AND IN (in_pif_lob_code,'ACV','AFV','BO','CF','SMP'),
		-- 	'2',
		-- --Modifying the ETL code to assign strategicProfitCenter as "WB-CL" if the policy Symbol start with "SM" and Policies LOB ='SMP'
		--       IN(SUBSTR(in_pif_symbol,1,2),'SM') AND IN(in_pif_lob_code,'SMP'),
		--       '2',
		-- 	IN(in_pif_symbol,'WMM','WMY','WXX','WXY') AND IN (in_pif_lob_code,'WC'),
		-- 	'2',
		-- 	IN(in_pif_symbol,'XAA','XAY','XXX','XXY') AND IN (in_pif_lob_code,'APV','HAP'),
		-- 	'1',
		-- 	IN(in_pif_symbol,'XAA','XAY','XXX','XXY') AND IN (in_pif_lob_code,'ACV','AFV','BO','GL','SMP','WC'),
		-- 	'2',
		-- 	'N/A'
		-- )
		DECODE(TRUE,
		IN(in_pif_symbol, 'HXX', 'HXY') AND IN(in_pif_lob_code, 'HAP', 'HP'), '1',
		IN(in_pif_symbol, 'PXX', 'PXY') AND IN(in_pif_lob_code, 'APV', 'HAP', 'HP'), '1',
		IN(in_pif_symbol, 'PXX', 'PXY') AND IN(in_pif_lob_code, 'ACV', 'AFV', 'BO', 'CF', 'SMP'), '2',
		IN(SUBSTR(in_pif_symbol, 1, 2), 'SM') AND IN(in_pif_lob_code, 'SMP'), '2',
		IN(in_pif_symbol, 'WMM', 'WMY', 'WXX', 'WXY') AND IN(in_pif_lob_code, 'WC'), '2',
		IN(in_pif_symbol, 'XAA', 'XAY', 'XXX', 'XXY') AND IN(in_pif_lob_code, 'APV', 'HAP'), '1',
		IN(in_pif_symbol, 'XAA', 'XAY', 'XXX', 'XXY') AND IN(in_pif_lob_code, 'ACV', 'AFV', 'BO', 'GL', 'SMP', 'WC'), '2',
		'N/A') AS v_AS400_StrategicProfitCenterCode,
		-- *INF*: DECODE(TRUE,
		-- 	IN(in_pif_symbol,'HXX','HXY') AND IN (in_pif_lob_code,'HAP','HP'),
		-- 	'1',
		-- 	IN(in_pif_symbol,'PXX','PXY') AND IN (in_pif_lob_code,'APV','HAP','HP'),
		-- 	'1',
		-- 	IN(in_pif_symbol,'PXX','PXY') AND IN (in_pif_lob_code,'ACV','AFV','BO','CF','SMP'),
		-- 	'2',
		-- --Modifying the ETL code to assign InsuranceSegment as "Commercial-LIne"  if the policy Symbol start with "SM" and Policies LOB ='SMP'
		--       IN(SUBSTR(in_pif_symbol,1,2),'SM') AND IN(in_pif_lob_code,'SMP'),
		--       '2',
		-- 	IN(in_pif_symbol,'WMM','WMY','WXX','WXY') AND IN (in_pif_lob_code,'WC'),
		-- 	'2',
		-- 	IN(in_pif_symbol,'XAA','XAY','XXX','XXY') AND IN (in_pif_lob_code,'APV','HAP'),
		-- 	'1',
		-- 	IN(in_pif_symbol,'XAA','XAY','XXX','XXY') AND IN (in_pif_lob_code,'ACV','AFV','BO','GL','SMP','WC'),
		-- 	'2',
		-- 	'N/A'
		-- )
		DECODE(TRUE,
		IN(in_pif_symbol, 'HXX', 'HXY') AND IN(in_pif_lob_code, 'HAP', 'HP'), '1',
		IN(in_pif_symbol, 'PXX', 'PXY') AND IN(in_pif_lob_code, 'APV', 'HAP', 'HP'), '1',
		IN(in_pif_symbol, 'PXX', 'PXY') AND IN(in_pif_lob_code, 'ACV', 'AFV', 'BO', 'CF', 'SMP'), '2',
		IN(SUBSTR(in_pif_symbol, 1, 2), 'SM') AND IN(in_pif_lob_code, 'SMP'), '2',
		IN(in_pif_symbol, 'WMM', 'WMY', 'WXX', 'WXY') AND IN(in_pif_lob_code, 'WC'), '2',
		IN(in_pif_symbol, 'XAA', 'XAY', 'XXX', 'XXY') AND IN(in_pif_lob_code, 'APV', 'HAP'), '1',
		IN(in_pif_symbol, 'XAA', 'XAY', 'XXX', 'XXY') AND IN(in_pif_lob_code, 'ACV', 'AFV', 'BO', 'GL', 'SMP', 'WC'), '2',
		'N/A') AS v_AS400_InsuranceSegmentCode,
		v_AS400_StrategicProfitCenterCode AS out_AS400_StrategicProfitCenterCode1,
		v_AS400_InsuranceSegmentCode AS out_AS400_InsuranceSegmentCode1,
		-- *INF*: IIF(v_AS400_StrategicProfitCenterCode= 'N/A',
		-- SUBSTR(in_pif_symbol,1,1),
		-- '')
		-- 
		-- 
		IFF(v_AS400_StrategicProfitCenterCode = 'N/A', SUBSTR(in_pif_symbol, 1, 1), '') AS out_PMS_pol_sym_1,
		-- *INF*: IIF(v_AS400_StrategicProfitCenterCode= 'N/A',
		-- SUBSTR(in_pif_pol_number,1,1),
		-- '')
		IFF(v_AS400_StrategicProfitCenterCode = 'N/A', SUBSTR(in_pif_pol_number, 1, 1), '') AS out_PMS_pol_num_1
		FROM INPUT
	),
	LKP_SupStrategicProfitCenterInsuranceSegment AS (
		SELECT
		StrategicProfitCenterCode,
		InsuranceSegmentCode,
		PolicySymbol1,
		PolicyNumber1
		FROM (
			SELECT 
				StrategicProfitCenterCode,
				InsuranceSegmentCode,
				PolicySymbol1,
				PolicyNumber1
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupStrategicProfitCenterInsuranceSegment
			WHERE CurrentSnapshotFlag=1 AND SourceCode='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicySymbol1,PolicyNumber1 ORDER BY StrategicProfitCenterCode) = 1
	),
	EXP_Select_StrategicProfitCenter_InsuranceSegment AS (
		SELECT
		EXP_Compute_AS400_Values.out_AS400_StrategicProfitCenterCode1 AS in_AS400_StrategicProfitCenterCode,
		EXP_Compute_AS400_Values.out_AS400_InsuranceSegmentCode1 AS in_AS400_InsuranceSegmentCode,
		LKP_SupStrategicProfitCenterInsuranceSegment.StrategicProfitCenterCode AS in_PMS_StrategicProfitCenterCode,
		LKP_SupStrategicProfitCenterInsuranceSegment.InsuranceSegmentCode AS in_PMS_InsuranceSegmentCode,
		-- *INF*: IIF(in_AS400_StrategicProfitCenterCode= 'N/A',
		-- in_PMS_StrategicProfitCenterCode,
		-- in_AS400_StrategicProfitCenterCode
		-- )
		IFF(in_AS400_StrategicProfitCenterCode = 'N/A', in_PMS_StrategicProfitCenterCode, in_AS400_StrategicProfitCenterCode) AS v_StrategicProfitCenterCode,
		-- *INF*: IIF(in_AS400_InsuranceSegmentCode='N/A',
		-- in_PMS_InsuranceSegmentCode,
		-- in_AS400_InsuranceSegmentCode
		-- )
		IFF(in_AS400_InsuranceSegmentCode = 'N/A', in_PMS_InsuranceSegmentCode, in_AS400_InsuranceSegmentCode) AS v_InsuranceSegmentCode,
		v_StrategicProfitCenterCode AS out_StrategicProfitCenterCode,
		v_InsuranceSegmentCode AS out_InsuranceSegmentCode
		FROM EXP_Compute_AS400_Values
		LEFT JOIN LKP_SupStrategicProfitCenterInsuranceSegment
		ON LKP_SupStrategicProfitCenterInsuranceSegment.PolicySymbol1 = EXP_Compute_AS400_Values.out_PMS_pol_sym_1 AND LKP_SupStrategicProfitCenterInsuranceSegment.PolicyNumber1 = EXP_Compute_AS400_Values.out_PMS_pol_num_1
	),
	LKP_InsuranceSegmentAKId AS (
		SELECT
		InsuranceSegmentAKId,
		InsuranceSegmentCode
		FROM (
			SELECT 
				InsuranceSegmentAKId,
				InsuranceSegmentCode
			FROM InsuranceSegment
			WHERE CurrentSnapshotFlag=1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceSegmentCode ORDER BY InsuranceSegmentAKId) = 1
	),
	LKP_StrategicProfitCenterAKId AS (
		SELECT
		StrategicProfitCenterAKId,
		StrategicProfitCenterCode
		FROM (
			SELECT 
				StrategicProfitCenterAKId,
				StrategicProfitCenterCode
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.StrategicProfitCenter
			WHERE CurrentSnapshotFlag=1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY StrategicProfitCenterCode ORDER BY StrategicProfitCenterAKId) = 1
	),
	EXP_Null_Check AS (
		SELECT
		LKP_StrategicProfitCenterAKId.StrategicProfitCenterAKId AS in_StrategicProfitCenterAKId,
		LKP_InsuranceSegmentAKId.InsuranceSegmentAKId AS in_InsuranceSegmentAKId,
		-- *INF*: IIF(ISNULL(in_StrategicProfitCenterAKId),
		-- -1,
		-- in_StrategicProfitCenterAKId)
		IFF(in_StrategicProfitCenterAKId IS NULL, - 1, in_StrategicProfitCenterAKId) AS v_StrategicProfitCenterAKId,
		-- *INF*: IIF(ISNULL(in_InsuranceSegmentAKId),
		-- -1,
		-- in_InsuranceSegmentAKId)
		IFF(in_InsuranceSegmentAKId IS NULL, - 1, in_InsuranceSegmentAKId) AS v_InsuranceSegmentAKId,
		v_StrategicProfitCenterAKId AS out_StrategicProfitCenterAKId,
		v_InsuranceSegmentAKId AS out_InsuranceSegmentAKId
		FROM 
		LEFT JOIN LKP_InsuranceSegmentAKId
		ON LKP_InsuranceSegmentAKId.InsuranceSegmentCode = EXP_Select_StrategicProfitCenter_InsuranceSegment.out_InsuranceSegmentCode
		LEFT JOIN LKP_StrategicProfitCenterAKId
		ON LKP_StrategicProfitCenterAKId.StrategicProfitCenterCode = EXP_Select_StrategicProfitCenter_InsuranceSegment.out_StrategicProfitCenterCode
	),
	OUTPUT AS (
		SELECT
		out_StrategicProfitCenterAKId AS StrategicProfitCenterAKId, 
		out_InsuranceSegmentAKId AS InsuranceSegmentAKId
		FROM EXP_Null_Check
	),
),
mplt_Strategic_Business_Division AS (WITH
	INPUT_Strategic_Business_Division AS (
		
	),
	EXP_inputs AS (
		SELECT
		policy_symbol,
		policy_number,
		policy_eff_date AS policy_eff_date_in,
		-- *INF*: IIF(:UDF.DEFAULT_VALUE_FOR_STRINGS(policy_symbol) = 'N/A','N/A',substr(ltrim(rtrim(policy_symbol)),1,1))
		IFF(:UDF.DEFAULT_VALUE_FOR_STRINGS(policy_symbol) = 'N/A', 'N/A', substr(ltrim(rtrim(policy_symbol)), 1, 1)) AS policy_symbol_position_1,
		-- *INF*: IIF(:UDF.DEFAULT_VALUE_FOR_STRINGS(policy_number)='N/A','N/A',substr(ltrim(rtrim(policy_number)),1,1))
		IFF(:UDF.DEFAULT_VALUE_FOR_STRINGS(policy_number) = 'N/A', 'N/A', substr(ltrim(rtrim(policy_number)), 1, 1)) AS policy_number_position_1,
		-- *INF*: IIF(isnull(policy_eff_date_in),SYSDATE,policy_eff_date_in)
		IFF(policy_eff_date_in IS NULL, SYSDATE, policy_eff_date_in) AS policy_eff_date
		FROM INPUT_Strategic_Business_Division
	),
	LKP_Strategic_Business_Division AS (
		SELECT
		strtgc_bus_dvsn_id,
		strtgc_bus_dvsn_ak_id,
		strtgc_bus_dvsn_code,
		strtgc_bus_dvsn_code_descript,
		pol_sym_1,
		pol_num_1,
		pol_eff_date,
		pol_exp_date
		FROM (
			SELECT 
				strtgc_bus_dvsn_id,
				strtgc_bus_dvsn_ak_id,
				strtgc_bus_dvsn_code,
				strtgc_bus_dvsn_code_descript,
				pol_sym_1,
				pol_num_1,
				pol_eff_date,
				pol_exp_date
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.strategic_business_division
			WHERE crrnt_snpsht_flag = 1 AND source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_sym_1,pol_num_1,pol_eff_date,pol_exp_date ORDER BY strtgc_bus_dvsn_id) = 1
	),
	EXP_check_outputs AS (
		SELECT
		strtgc_bus_dvsn_id,
		strtgc_bus_dvsn_ak_id,
		strtgc_bus_dvsn_code,
		strtgc_bus_dvsn_code_descript,
		-- *INF*: IIF(isnull(strtgc_bus_dvsn_id),-1,strtgc_bus_dvsn_id)
		IFF(strtgc_bus_dvsn_id IS NULL, - 1, strtgc_bus_dvsn_id) AS strtgc_bus_dvsn_id_out,
		-- *INF*: IIF(isnull(strtgc_bus_dvsn_ak_id),-1,strtgc_bus_dvsn_ak_id)
		IFF(strtgc_bus_dvsn_ak_id IS NULL, - 1, strtgc_bus_dvsn_ak_id) AS strtgc_bus_dvsn_ak_id_out,
		-- *INF*: IIF(isnull(strtgc_bus_dvsn_code),'N/A',strtgc_bus_dvsn_code)
		IFF(strtgc_bus_dvsn_code IS NULL, 'N/A', strtgc_bus_dvsn_code) AS strtgc_bus_dvsn_code_out,
		-- *INF*: IIF(isnull(strtgc_bus_dvsn_code_descript),'N/A',strtgc_bus_dvsn_code_descript)
		IFF(strtgc_bus_dvsn_code_descript IS NULL, 'N/A', strtgc_bus_dvsn_code_descript) AS strtgc_bus_dvsn_code_descript_out
		FROM LKP_Strategic_Business_Division
	),
	OUTPUT_return_Strategic_Business_Division AS (
		SELECT
		strtgc_bus_dvsn_id_out AS strtgc_bus_dvsn_id, 
		strtgc_bus_dvsn_ak_id_out AS strtgc_bus_dvsn_ak_id, 
		strtgc_bus_dvsn_code_out AS strtgc_bus_dvsn_code, 
		strtgc_bus_dvsn_code_descript_out AS strtgc_bus_dvsn_code_descript
		FROM EXP_check_outputs
	),
),
EXP_Detect_Changes AS (
	SELECT
	LKP_Policy.pol_id AS lkp_pol_id,
	LKP_Policy.pol_ak_id AS lkp_pol_ak_id,
	LKP_Policy.contract_cust_ak_id AS lkp_contract_cust_ak_id,
	LKP_Policy.agency_ak_id AS lkp_agency_ak_id,
	LKP_Policy.AgencyAKId AS lkp_AgencyAKId,
	LKP_Policy.mco AS lkp_mco,
	LKP_Policy.pol_co_num AS lkp_pol_co_num,
	LKP_Policy.pol_eff_date AS lkp_pol_eff_date,
	LKP_Policy.pol_exp_date AS lkp_pol_exp_date,
	LKP_Policy.orig_incptn_date AS lkp_orig_incptn_date,
	LKP_Policy.prim_bus_class_code AS lkp_prim_bus_class_code,
	LKP_Policy.reins_code AS lkp_reins_code,
	LKP_Policy.pms_pol_lob_code AS lkp_pms_pol_lob_code,
	LKP_Policy.pol_co_line_code AS lkp_pol_co_line_code,
	LKP_Policy.pol_cancellation_ind AS lkp_pol_cancellation_ind,
	LKP_Policy.pol_cancellation_date AS lkp_pol_cancellation_date,
	LKP_Policy.pol_cancellation_rsn_code AS lkp_pol_cancellation_rsn_code,
	LKP_Policy.state_of_domicile_code AS lkp_state_of_domicile_code,
	LKP_Policy.wbconnect_upload_code AS lkp_wbconnect_upload_code,
	LKP_Policy.serv_center_support_code AS lkp_serv_center_support_code,
	LKP_Policy.pol_term AS lkp_pol_term,
	LKP_Policy.terrorism_risk_ind AS lkp_terrorism_risk_ind,
	LKP_Policy.prior_pol_key AS lkp_prior_pol_key,
	LKP_Policy.pol_status_code AS lkp_pol_status_code,
	LKP_Policy.pol_issue_code AS lkp_pol_issue_code,
	LKP_Policy.pol_age AS lkp_pol_age,
	LKP_Policy.industry_risk_grade_code AS lkp_industry_risk_grade_code,
	LKP_Policy.uw_review_yr AS lkp_uw_review_yr,
	LKP_Policy.mvr_request_code AS lkp_mvr_request_code,
	LKP_Policy.renl_code AS lkp_renl_code,
	LKP_Policy.amend_num AS lkp_amend_num,
	LKP_Policy.anniversary_rerate_code AS lkp_anniversary_rerate_code,
	LKP_Policy.pol_audit_frqncy AS lkp_pol_audit_frqncy,
	LKP_Policy.final_audit_code AS lkp_final_audit_code,
	LKP_Policy.zip_ind AS lkp_zip_ind,
	LKP_Policy.guarantee_ind AS lkp_guarantee_ind,
	LKP_Policy.variation_code AS lkp_variation_code,
	LKP_Policy.county AS lkp_county,
	LKP_Policy.non_smoker_disc_code AS lkp_non_smoker_disc_code,
	LKP_Policy.renl_disc AS lkp_renl_disc,
	LKP_Policy.renl_safe_driver_disc_count AS lkp_renl_safe_driver_disc_count,
	LKP_Policy.nonrenewal_flag_date AS lkp_nonrenewal_flag_date,
	LKP_Policy.audit_complt_date AS lkp_audit_complt_date,
	LKP_Policy.orig_acct_date AS lkp_orig_acct_date,
	LKP_Policy.pol_enter_date AS lkp_pol_enter_date,
	LKP_Policy.excess_claim_code AS lkp_excess_claim_code,
	LKP_Policy.pol_status_on_pif AS lkp_pol_status_on_pif,
	LKP_Policy.target_mrkt_code AS lkp_target_mrkt_code,
	LKP_Policy.pkg_code AS lkp_pkg_code,
	LKP_Policy.bus_seg_code AS lkp_bus_seg_code,
	LKP_Policy.pif_upload_audit_ind AS lkp_pif_upload_audit_ind,
	LKP_Policy.producer_code_ak_id AS lkp_producer_code_ak_id,
	LKP_Policy.prdcr_code AS lkp_prdcr_code,
	LKP_Policy.ClassOfBusiness AS lkp_ClassOfBusiness,
	LKP_Policy.strtgc_bus_dvsn_ak_id AS lkp_strtgc_bus_dvsn_ak_id,
	LKP_Policy.RenewalPolicyNumber AS lkp_RenewalPolicyNumber,
	LKP_Policy.RenewalPolicySymbol AS lkp_RenewalPolicySymbol,
	LKP_Policy.RenewalPolicyMod AS lkp_RenewalPolicyMod,
	LKP_Policy.BillingType AS lkp_BillingType,
	LKP_Policy.producer_code_id AS lkp_producer_code_id,
	LKP_Policy.sup_bus_class_code_id AS lkp_sup_bus_class_code_id,
	LKP_Policy.sup_pol_term_id AS lkp_sup_pol_term_id,
	LKP_Policy.sup_pol_status_code_id AS lkp_sup_pol_status_code_id,
	LKP_Policy.sup_pol_issue_code_id AS lkp_sup_pol_issue_code_id,
	LKP_Policy.sup_pol_audit_frqncy_id AS lkp_sup_pol_audit_frqncy_id,
	LKP_Policy.sup_industry_risk_grade_code_id AS lkp_sup_industry_risk_grade_code_id,
	LKP_Policy.sup_state_id AS lkp_sup_state_id,
	LKP_Policy.SurchargeExemptCode AS lkp_SurchargeExemptCode,
	LKP_Policy.SupSurchargeExemptID AS lkp_SupSurchargeExemptID,
	LKP_Policy.StrategicProfitCenterAKId AS lkp_StrategicProfitCenterAKId,
	LKP_Policy.InsuranceSegmentAKId AS lkp_InsuranceSegmentAKId,
	LKP_Policy.PolicyOfferingAKId AS lkp_PolicyOfferingAKId,
	LKP_Policy.ProgramAKId AS lkp_ProgramAKId,
	LKP_Policy.ObligeeName AS lkp_ObligeeName,
	LKP_Policy.AutomatedUnderwritingServicesIndicator AS lkp_AutomatedUnderwritingServicesIndicator,
	LKP_Policy.AutomaticRenewalIndicator AS lkp_AutomaticRenewalIndicator,
	LKP_Policy.AssociationCode AS lkp_AssociationCode,
	LKP_Policy.AgencyEmployeeAKId AS lkp_AgencyEmployeeAKId,
	LKP_contract_customer_key.contract_cust_ak_id AS in_cust_ak_id,
	LKP_agency_ak_id.agency_ak_id AS in_agency_ak_id,
	LKP_AgencyAKId.AgencyAKId AS in_AgencyAKID,
	LKP_producer_code_ak_id.producer_code_id AS in_producer_code_id,
	LKP_producer_code_ak_id.prdcr_code_ak_id AS in_producer_code_ak_id,
	mplt_Strategic_Business_Division.strtgc_bus_dvsn_ak_id AS in_strtgc_bus_dvsn_ak_id,
	LKP_sup_business_classification_code.sup_bus_class_code_id AS in_sup_bus_class_code_id,
	LKP_sup_policy_term.sup_pol_term_id AS in_sup_pol_term_id,
	LKP_sup_policy_status_code.sup_pol_status_code_id AS in_sup_pol_status_code_id,
	LKP_sup_policy_audit_frequency.sup_pol_audit_frqncy_id AS in_sup_pol_audit_frqncy_id,
	LKP_sup_industry_risk_grade_code.sup_industry_risk_grade_code_id AS in_sup_industry_risk_grade_code_id,
	LKP_sup_state.sup_state_id AS in_sup_state_id,
	mplt_PMS_StrategicProfitCenterInsuranceSegment.StrategicProfitCenterAKId AS in_StrategicProfitCenterAKId,
	mplt_PMS_StrategicProfitCenterInsuranceSegment.InsuranceSegmentAKId AS in_InsuranceSegmentAKId,
	LKP_ProgramAKId.ProgramAKId AS in_ProgramAKId,
	LKP_AgencyEmployee.AgencyEmployeeAKID AS in_AgencyEmployeeAKID,
	EXP_values.pif_symbol,
	EXP_values.pif_pol_number AS pif_policy_number,
	EXP_values.pif_module,
	EXP_values.policy_key,
	EXP_values.pol_eff_date,
	EXP_values.pol_exp_date,
	EXP_values.mco,
	EXP_values.pol_term,
	EXP_values.state_of_domicile_code,
	EXP_values.pol_co_num,
	EXP_values.pif_lob_code AS pif_pol_lob_code,
	EXP_values.pol_co_line_code,
	EXP_values.pol_cancellation_ind,
	EXP_values.renl_code,
	EXP_values.orig_incep_date,
	EXP_values.wbconnect_upload_code,
	EXP_values.prim_bus_class_code,
	EXP_values.terrorism_risk_ind,
	EXP_values.out_pif_renew_policy_number AS renewalpolicynumber,
	EXP_values.out_pif_renew_policy_symbol AS renewalpolicysymbol,
	EXP_values.prior_pol_key,
	EXP_values.pol_issue_code AS in_pol_issue_code,
	EXP_values.pol_age,
	EXP_values.industry_risk_grade_code,
	EXP_values.amend_num,
	EXP_values.anniversary_rerate_code,
	EXP_values.pol_audit_frqncy,
	EXP_values.final_audit_code,
	EXP_values.zip_ind,
	EXP_values.gutantee_ind,
	EXP_values.variation_code,
	EXP_values.county,
	EXP_values.non_smoker_disc_code,
	EXP_values.renl_disc,
	EXP_values.pif_upload_audit,
	EXP_values.pif_seg_id,
	EXP_values.renl_safe_driver_disc_count,
	EXP_values.fn_date AS non_renewal_flag_date,
	EXP_values.audit_complt_date,
	EXP_values.original_account_date,
	EXP_values.pol_enter_date,
	EXP_values.excess_claim_code,
	EXP_values.pif_policy_status_on,
	EXP_values.reins_code,
	EXP_values.pol_cancellation_date,
	EXP_values.pol_cancellation_rsn_code,
	EXP_values.serv_center_support_code,
	EXP_values.policy_status_code AS pol_status_code,
	EXP_values.uw_review_yr,
	EXP_values.mvr_request_code,
	EXP_values.pif_target_market_code,
	EXP_values.pkg_code,
	EXP_values.RenewalPolicyMod,
	EXP_values.BillingType,
	EXP_values.producer_code,
	EXP_values.ClassOfBusiness,
	exp_user_code.out_use_code AS SurchargeExemptCode,
	LKP_SupSurchargeExempt.SupSurchargeExemptId AS SupSurchargeExemptID,
	EXP_values.PolicyOfferingCode AS in_PolicyOfferingCode,
	EXP_values.PolicyOfferingAKId AS in_PolicyOfferingAKId,
	EXP_values.ObligeeName,
	EXP_values.AutomatedUnderwritingServicesIndicator,
	EXP_values.AutomaticRenewalIndicator,
	LKP_Association.AssociationCode AS in_AssociationCode,
	-- *INF*: IIF(
	--   ISNULL( in_cust_ak_id ),
	--   -1,
	--   in_cust_ak_id
	-- )
	IFF(in_cust_ak_id IS NULL, - 1, in_cust_ak_id) AS v_cust_ak_id,
	-- *INF*: IIF(
	--   ISNULL(in_agency_ak_id),
	--   -1,
	-- in_agency_ak_id
	-- )
	IFF(in_agency_ak_id IS NULL, - 1, in_agency_ak_id) AS v_agency_ak_id,
	-- *INF*: IIF(
	--   ISNULL(in_AgencyAKID ),
	--   -1,
	--  in_AgencyAKID
	-- )
	IFF(in_AgencyAKID IS NULL, - 1, in_AgencyAKID) AS v_AgencyAKId,
	-- *INF*: IIF(
	--   ISNULL( in_producer_code_id ),
	--   -1,
	--   in_producer_code_id
	-- )
	IFF(in_producer_code_id IS NULL, - 1, in_producer_code_id) AS v_producer_code_id,
	-- *INF*: IIF(
	--   ISNULL(in_producer_code_ak_id ),
	--   -1,
	--   in_producer_code_ak_id
	-- )
	IFF(in_producer_code_ak_id IS NULL, - 1, in_producer_code_ak_id) AS v_producer_code_ak_id,
	-- *INF*: IIF(
	--   ISNULL( in_strtgc_bus_dvsn_ak_id ),
	--   -1,
	--   in_strtgc_bus_dvsn_ak_id
	-- )
	IFF(in_strtgc_bus_dvsn_ak_id IS NULL, - 1, in_strtgc_bus_dvsn_ak_id) AS v_strtgc_bus_dvsn_ak_id,
	-- *INF*: IIF(
	--   ISNULL(in_sup_bus_class_code_id),
	--   -1,
	--   in_sup_bus_class_code_id
	-- )
	IFF(in_sup_bus_class_code_id IS NULL, - 1, in_sup_bus_class_code_id) AS v_sup_bus_class_code_id,
	-- *INF*: IIF(
	--   ISNULL(in_sup_pol_term_id),
	--   -1,
	--   in_sup_pol_term_id
	-- )
	IFF(in_sup_pol_term_id IS NULL, - 1, in_sup_pol_term_id) AS v_sup_pol_term_id,
	-- *INF*: IIF(
	--   ISNULL(in_sup_pol_status_code_id),
	--   -1,
	--   in_sup_pol_status_code_id
	-- )
	IFF(in_sup_pol_status_code_id IS NULL, - 1, in_sup_pol_status_code_id) AS v_sup_pol_status_code_id,
	-- *INF*: IIF(
	--   ISNULL(in_sup_pol_audit_frqncy_id),
	--   -1,
	--   in_sup_pol_audit_frqncy_id
	-- )
	IFF(in_sup_pol_audit_frqncy_id IS NULL, - 1, in_sup_pol_audit_frqncy_id) AS v_sup_pol_audit_frqncy_id,
	-- *INF*: IIF(
	--   ISNULL(in_sup_industry_risk_grade_code_id),
	--   -1,
	--   in_sup_industry_risk_grade_code_id
	-- )
	IFF(in_sup_industry_risk_grade_code_id IS NULL, - 1, in_sup_industry_risk_grade_code_id) AS v_sup_industry_risk_grade_code_id,
	-- *INF*: IIF(ISNULL(in_sup_state_id), -1, in_sup_state_id)
	IFF(in_sup_state_id IS NULL, - 1, in_sup_state_id) AS v_sup_state_id,
	-- *INF*: IIF(ISNULL(in_StrategicProfitCenterAKId), -1, in_StrategicProfitCenterAKId)
	IFF(in_StrategicProfitCenterAKId IS NULL, - 1, in_StrategicProfitCenterAKId) AS v_StrategicProfitCenterAKId,
	-- *INF*: substr(pif_symbol,1,2)
	substr(pif_symbol, 1, 2) AS v_pif_symbol_02_F2,
	-- *INF*: substr(pif_symbol,1,1)
	substr(pif_symbol, 1, 1) AS v_pif_symbol_02_F1,
	-- *INF*: IIF(ISNULL(in_InsuranceSegmentAKId), -1, in_InsuranceSegmentAKId)
	IFF(in_InsuranceSegmentAKId IS NULL, - 1, in_InsuranceSegmentAKId) AS v_InsuranceSegmentAKId,
	-- *INF*: IIF(
	--   ISNULL(in_ProgramAKId),
	--   -1,
	--   in_ProgramAKId
	-- )
	IFF(in_ProgramAKId IS NULL, - 1, in_ProgramAKId) AS v_ProgramAKId,
	in_PolicyOfferingAKId AS v_PolicyOfferingAKId,
	-- *INF*: :LKP.LKP_POLICYOFFERINGAKID('000')
	LKP_POLICYOFFERINGAKID__000.PolicyOfferingAKId AS v_PolicyOfferingAKId_default,
	-- *INF*: DECODE(lkp_AutomaticRenewalIndicator, 'T', '1', 'F', '0', NULL)
	DECODE(lkp_AutomaticRenewalIndicator,
	'T', '1',
	'F', '0',
	NULL) AS v_lkp_AutomaticRenewalIndicator,
	-- *INF*: iif(isnull(in_AssociationCode ) or IS_SPACES(in_AssociationCode ) or LENGTH(in_AssociationCode )=0,'N/A',in_AssociationCode )
	IFF(in_AssociationCode IS NULL OR IS_SPACES(in_AssociationCode) OR LENGTH(in_AssociationCode) = 0, 'N/A', in_AssociationCode) AS v_AssociationCode,
	-- *INF*: IIF(ISNULL(in_AgencyEmployeeAKID), -1, in_AgencyEmployeeAKID)
	IFF(in_AgencyEmployeeAKID IS NULL, - 1, in_AgencyEmployeeAKID) AS v_AgencyEmployeeAKId,
	LKP_ProgramAKId.in_ProgramCode,
	-- *INF*: IIF(ISNULL(:LKP.LKP_StrategicProfitCenter(v_StrategicProfitCenterAKId)), 'N/A', :LKP.LKP_StrategicProfitCenter(v_StrategicProfitCenterAKId))
	IFF(LKP_STRATEGICPROFITCENTER_v_StrategicProfitCenterAKId.StrategicProfitCenterAbbreviation IS NULL, 'N/A', LKP_STRATEGICPROFITCENTER_v_StrategicProfitCenterAKId.StrategicProfitCenterAbbreviation) AS v_StrategicProfitCenterAbbrev,
	-- *INF*: DECODE(TRUE, 
	-- SUBSTR(pif_symbol, 1, 2) = 'NC', 'N',
	-- in_ProgramCode = 'XD', 'N',
	-- v_StrategicProfitCenterAbbrev = 'WB - PL' AND in_pol_issue_code = 'R' AND pif_module = '00', 'N',
	-- in_pol_issue_code)
	DECODE(TRUE,
	SUBSTR(pif_symbol, 1, 2) = 'NC', 'N',
	in_ProgramCode = 'XD', 'N',
	v_StrategicProfitCenterAbbrev = 'WB - PL' AND in_pol_issue_code = 'R' AND pif_module = '00', 'N',
	in_pol_issue_code) AS v_pol_issue_code,
	-- *INF*: IIF(
	--   ISNULL(:LKP.LKP_sup_policy_issue_code(v_pol_issue_code)),
	--   -1,
	--   :LKP.LKP_sup_policy_issue_code(v_pol_issue_code)
	-- )
	IFF(LKP_SUP_POLICY_ISSUE_CODE_v_pol_issue_code.sup_pol_issue_code_id IS NULL, - 1, LKP_SUP_POLICY_ISSUE_CODE_v_pol_issue_code.sup_pol_issue_code_id) AS v_sup_pol_issue_code_id,
	-- *INF*: IIF(ISNULL(lkp_pol_ak_id), 'NEW', 
	-- IIF(lkp_contract_cust_ak_id != v_cust_ak_id OR 
	-- lkp_agency_ak_id<>in_agency_ak_id OR
	-- lkp_AgencyAKId != v_AgencyAKId OR 
	-- LTRIM(RTRIM(lkp_mco)) != LTRIM(RTRIM(mco)) OR 
	-- LTRIM(RTRIM(lkp_pol_co_num)) != LTRIM(RTRIM(pol_co_num)) OR 
	-- lkp_pol_eff_date != pol_eff_date OR 
	-- lkp_pol_exp_date != pol_exp_date OR 
	-- lkp_orig_incptn_date != orig_incep_date OR 
	-- LTRIM(RTRIM(lkp_prim_bus_class_code)) != LTRIM(RTRIM(prim_bus_class_code)) OR 
	-- LTRIM(RTRIM(lkp_reins_code)) != LTRIM(RTRIM(reins_code)) OR 
	-- LTRIM(RTRIM(lkp_pms_pol_lob_code)) != LTRIM(RTRIM(pif_pol_lob_code)) OR 
	-- LTRIM(RTRIM(lkp_pol_co_line_code)) != LTRIM(RTRIM(pol_co_line_code)) OR 
	-- LTRIM(RTRIM(lkp_pol_cancellation_ind)) != LTRIM(RTRIM(pol_cancellation_ind)) OR 
	-- LTRIM(RTRIM(lkp_pol_cancellation_rsn_code)) != LTRIM(RTRIM(pol_cancellation_rsn_code)) OR 
	-- LTRIM(RTRIM(lkp_state_of_domicile_code)) != LTRIM(RTRIM(state_of_domicile_code)) OR 
	-- LTRIM(RTRIM(lkp_wbconnect_upload_code)) != LTRIM(RTRIM(wbconnect_upload_code)) OR 
	-- LTRIM(RTRIM(lkp_serv_center_support_code)) != LTRIM(RTRIM(serv_center_support_code)) OR 
	-- LTRIM(RTRIM(lkp_pol_term)) != LTRIM(RTRIM(pol_term)) OR 
	-- LTRIM(RTRIM(lkp_terrorism_risk_ind)) != LTRIM(RTRIM(terrorism_risk_ind)) OR 
	-- LTRIM(RTRIM(lkp_prior_pol_key)) != LTRIM(RTRIM(prior_pol_key)) OR 
	-- LTRIM(RTRIM(lkp_pol_issue_code)) != LTRIM(RTRIM(v_pol_issue_code)) OR 
	-- lkp_pol_age != pol_age OR 
	-- LTRIM(RTRIM(lkp_industry_risk_grade_code)) != LTRIM(RTRIM(industry_risk_grade_code)) OR 
	-- LTRIM(RTRIM(lkp_uw_review_yr)) != LTRIM(RTRIM(uw_review_yr)) OR 
	-- LTRIM(RTRIM(lkp_mvr_request_code)) != LTRIM(RTRIM(mvr_request_code)) OR 
	-- LTRIM(RTRIM(lkp_renl_code)) != LTRIM(RTRIM(renl_code)) OR 
	-- LTRIM(RTRIM(lkp_amend_num)) != LTRIM(RTRIM(amend_num)) OR 
	-- LTRIM(RTRIM(lkp_anniversary_rerate_code)) != LTRIM(RTRIM(anniversary_rerate_code)) OR 
	-- LTRIM(RTRIM(lkp_pol_audit_frqncy)) != LTRIM(RTRIM(pol_audit_frqncy)) OR 
	-- LTRIM(RTRIM(lkp_final_audit_code)) != LTRIM(RTRIM(final_audit_code)) OR 
	-- LTRIM(RTRIM(lkp_zip_ind)) != LTRIM(RTRIM(zip_ind)) OR 
	-- LTRIM(RTRIM(lkp_guarantee_ind)) != LTRIM(RTRIM(gutantee_ind)) OR 
	-- LTRIM(RTRIM(lkp_variation_code)) != LTRIM(RTRIM(variation_code)) OR 
	-- LTRIM(RTRIM(lkp_county)) != LTRIM(RTRIM(county)) OR 
	-- LTRIM(RTRIM(lkp_non_smoker_disc_code)) != LTRIM(RTRIM(non_smoker_disc_code)) OR 
	-- lkp_renl_disc != renl_disc OR 
	-- lkp_renl_safe_driver_disc_count != renl_safe_driver_disc_count OR 
	-- lkp_nonrenewal_flag_date != non_renewal_flag_date OR 
	-- lkp_audit_complt_date != audit_complt_date OR 
	-- lkp_orig_acct_date != original_account_date OR 
	-- lkp_pol_enter_date != pol_enter_date OR 
	-- LTRIM(RTRIM(lkp_excess_claim_code)) != LTRIM(RTRIM(excess_claim_code)) OR 
	-- LTRIM(RTRIM(lkp_pol_status_on_pif)) != LTRIM(RTRIM(pif_policy_status_on)) OR 
	-- LTRIM(RTRIM(lkp_target_mrkt_code)) != LTRIM(RTRIM(pif_target_market_code)) OR 
	-- LTRIM(RTRIM(lkp_pkg_code)) != LTRIM(RTRIM(pkg_code)) OR 
	-- LTRIM(RTRIM(lkp_bus_seg_code)) != LTRIM(RTRIM(pif_seg_id)) OR 
	-- LTRIM(RTRIM(lkp_pif_upload_audit_ind)) != LTRIM(RTRIM(pif_upload_audit)) OR 
	-- lkp_producer_code_ak_id != v_producer_code_ak_id OR 
	-- LTRIM(RTRIM(lkp_prdcr_code)) != LTRIM(RTRIM(producer_code)) OR 
	-- LTRIM(RTRIM(lkp_ClassOfBusiness)) != LTRIM(RTRIM(ClassOfBusiness)) OR 
	-- lkp_strtgc_bus_dvsn_ak_id != v_strtgc_bus_dvsn_ak_id OR 
	-- LTRIM(RTRIM(lkp_RenewalPolicyNumber)) != LTRIM(RTRIM(renewalpolicynumber)) OR 
	-- LTRIM(RTRIM(lkp_RenewalPolicySymbol)) != LTRIM(RTRIM(renewalpolicysymbol)) OR 
	-- LTRIM(RTRIM(lkp_RenewalPolicyMod)) != LTRIM(RTRIM(RenewalPolicyMod)) OR 
	-- LTRIM(RTRIM(lkp_BillingType)) != LTRIM(RTRIM(BillingType)) OR 
	-- --LTRIM(RTRIM(lkp_strtgc_bus_dvsn_code)) != LTRIM(RTRIM(v_strtgc_bus_dvsn_code)) OR  ***
	-- --LTRIM(RTRIM(lkp_PolicyOfferingCode)) != LTRIM(RTRIM(v_PolicyOfferingCode)) OR ***
	-- --LTRIM(RTRIM(lkp_ProgramCode)) != LTRIM(RTRIM(v_ProgramCode)) OR ***
	-- lkp_producer_code_id != v_producer_code_id OR 
	-- --lkp_strtgc_bus_dvsn_id != v_strtgc_bus_dvsn_id OR  ***
	-- ---------------------------------------------------------------------------------------------
	-- lkp_sup_bus_class_code_id != v_sup_bus_class_code_id OR 
	-- lkp_sup_pol_term_id != v_sup_pol_term_id OR 
	-- lkp_sup_pol_issue_code_id != v_sup_pol_issue_code_id OR 
	-- lkp_sup_pol_audit_frqncy_id != v_sup_pol_audit_frqncy_id OR 
	-- lkp_sup_industry_risk_grade_code_id != v_sup_industry_risk_grade_code_id OR 
	-- lkp_sup_state_id != v_sup_state_id OR 
	-- LTRIM(RTRIM(lkp_SurchargeExemptCode)) != LTRIM(RTRIM(SurchargeExemptCode)) OR
	-- lkp_SupSurchargeExemptID != SupSurchargeExemptID OR
	-- lkp_StrategicProfitCenterAKId!=v_StrategicProfitCenterAKId OR
	-- lkp_InsuranceSegmentAKId != v_InsuranceSegmentAKId  OR 
	-- LTRIM(RTRIM(lkp_AssociationCode))!=LTRIM(RTRIM(v_AssociationCode)) OR
	-- lkp_ProgramAKId != v_ProgramAKId OR 
	-- (lkp_PolicyOfferingAKId !=v_PolicyOfferingAKId AND  (lkp_PolicyOfferingAKId=-1 OR lkp_PolicyOfferingAKId=v_PolicyOfferingAKId_default)) OR 
	-- lkp_ObligeeName  != ObligeeName OR 
	-- lkp_AutomatedUnderwritingServicesIndicator != AutomatedUnderwritingServicesIndicator OR 
	-- ---------------------------------------------------------------------------------------
	-- v_lkp_AutomaticRenewalIndicator != AutomaticRenewalIndicator OR 
	-- lkp_AgencyEmployeeAKId != in_AgencyEmployeeAKID OR
	-- 1!=1 ,
	-- 'UPDATE', 'NOCHANGE'))
	-- 
	-- --OR  LTRIM(RTRIM(lkp_InsuranceSegmentCode)) != LTRIM(RTRIM(v_InsuranceSegmentCode))
	IFF(lkp_pol_ak_id IS NULL, 'NEW', IFF(lkp_contract_cust_ak_id != v_cust_ak_id OR lkp_agency_ak_id <> in_agency_ak_id OR lkp_AgencyAKId != v_AgencyAKId OR LTRIM(RTRIM(lkp_mco)) != LTRIM(RTRIM(mco)) OR LTRIM(RTRIM(lkp_pol_co_num)) != LTRIM(RTRIM(pol_co_num)) OR lkp_pol_eff_date != pol_eff_date OR lkp_pol_exp_date != pol_exp_date OR lkp_orig_incptn_date != orig_incep_date OR LTRIM(RTRIM(lkp_prim_bus_class_code)) != LTRIM(RTRIM(prim_bus_class_code)) OR LTRIM(RTRIM(lkp_reins_code)) != LTRIM(RTRIM(reins_code)) OR LTRIM(RTRIM(lkp_pms_pol_lob_code)) != LTRIM(RTRIM(pif_pol_lob_code)) OR LTRIM(RTRIM(lkp_pol_co_line_code)) != LTRIM(RTRIM(pol_co_line_code)) OR LTRIM(RTRIM(lkp_pol_cancellation_ind)) != LTRIM(RTRIM(pol_cancellation_ind)) OR LTRIM(RTRIM(lkp_pol_cancellation_rsn_code)) != LTRIM(RTRIM(pol_cancellation_rsn_code)) OR LTRIM(RTRIM(lkp_state_of_domicile_code)) != LTRIM(RTRIM(state_of_domicile_code)) OR LTRIM(RTRIM(lkp_wbconnect_upload_code)) != LTRIM(RTRIM(wbconnect_upload_code)) OR LTRIM(RTRIM(lkp_serv_center_support_code)) != LTRIM(RTRIM(serv_center_support_code)) OR LTRIM(RTRIM(lkp_pol_term)) != LTRIM(RTRIM(pol_term)) OR LTRIM(RTRIM(lkp_terrorism_risk_ind)) != LTRIM(RTRIM(terrorism_risk_ind)) OR LTRIM(RTRIM(lkp_prior_pol_key)) != LTRIM(RTRIM(prior_pol_key)) OR LTRIM(RTRIM(lkp_pol_issue_code)) != LTRIM(RTRIM(v_pol_issue_code)) OR lkp_pol_age != pol_age OR LTRIM(RTRIM(lkp_industry_risk_grade_code)) != LTRIM(RTRIM(industry_risk_grade_code)) OR LTRIM(RTRIM(lkp_uw_review_yr)) != LTRIM(RTRIM(uw_review_yr)) OR LTRIM(RTRIM(lkp_mvr_request_code)) != LTRIM(RTRIM(mvr_request_code)) OR LTRIM(RTRIM(lkp_renl_code)) != LTRIM(RTRIM(renl_code)) OR LTRIM(RTRIM(lkp_amend_num)) != LTRIM(RTRIM(amend_num)) OR LTRIM(RTRIM(lkp_anniversary_rerate_code)) != LTRIM(RTRIM(anniversary_rerate_code)) OR LTRIM(RTRIM(lkp_pol_audit_frqncy)) != LTRIM(RTRIM(pol_audit_frqncy)) OR LTRIM(RTRIM(lkp_final_audit_code)) != LTRIM(RTRIM(final_audit_code)) OR LTRIM(RTRIM(lkp_zip_ind)) != LTRIM(RTRIM(zip_ind)) OR LTRIM(RTRIM(lkp_guarantee_ind)) != LTRIM(RTRIM(gutantee_ind)) OR LTRIM(RTRIM(lkp_variation_code)) != LTRIM(RTRIM(variation_code)) OR LTRIM(RTRIM(lkp_county)) != LTRIM(RTRIM(county)) OR LTRIM(RTRIM(lkp_non_smoker_disc_code)) != LTRIM(RTRIM(non_smoker_disc_code)) OR lkp_renl_disc != renl_disc OR lkp_renl_safe_driver_disc_count != renl_safe_driver_disc_count OR lkp_nonrenewal_flag_date != non_renewal_flag_date OR lkp_audit_complt_date != audit_complt_date OR lkp_orig_acct_date != original_account_date OR lkp_pol_enter_date != pol_enter_date OR LTRIM(RTRIM(lkp_excess_claim_code)) != LTRIM(RTRIM(excess_claim_code)) OR LTRIM(RTRIM(lkp_pol_status_on_pif)) != LTRIM(RTRIM(pif_policy_status_on)) OR LTRIM(RTRIM(lkp_target_mrkt_code)) != LTRIM(RTRIM(pif_target_market_code)) OR LTRIM(RTRIM(lkp_pkg_code)) != LTRIM(RTRIM(pkg_code)) OR LTRIM(RTRIM(lkp_bus_seg_code)) != LTRIM(RTRIM(pif_seg_id)) OR LTRIM(RTRIM(lkp_pif_upload_audit_ind)) != LTRIM(RTRIM(pif_upload_audit)) OR lkp_producer_code_ak_id != v_producer_code_ak_id OR LTRIM(RTRIM(lkp_prdcr_code)) != LTRIM(RTRIM(producer_code)) OR LTRIM(RTRIM(lkp_ClassOfBusiness)) != LTRIM(RTRIM(ClassOfBusiness)) OR lkp_strtgc_bus_dvsn_ak_id != v_strtgc_bus_dvsn_ak_id OR LTRIM(RTRIM(lkp_RenewalPolicyNumber)) != LTRIM(RTRIM(renewalpolicynumber)) OR LTRIM(RTRIM(lkp_RenewalPolicySymbol)) != LTRIM(RTRIM(renewalpolicysymbol)) OR LTRIM(RTRIM(lkp_RenewalPolicyMod)) != LTRIM(RTRIM(RenewalPolicyMod)) OR LTRIM(RTRIM(lkp_BillingType)) != LTRIM(RTRIM(BillingType)) OR lkp_producer_code_id != v_producer_code_id OR lkp_sup_bus_class_code_id != v_sup_bus_class_code_id OR lkp_sup_pol_term_id != v_sup_pol_term_id OR lkp_sup_pol_issue_code_id != v_sup_pol_issue_code_id OR lkp_sup_pol_audit_frqncy_id != v_sup_pol_audit_frqncy_id OR lkp_sup_industry_risk_grade_code_id != v_sup_industry_risk_grade_code_id OR lkp_sup_state_id != v_sup_state_id OR LTRIM(RTRIM(lkp_SurchargeExemptCode)) != LTRIM(RTRIM(SurchargeExemptCode)) OR lkp_SupSurchargeExemptID != SupSurchargeExemptID OR lkp_StrategicProfitCenterAKId != v_StrategicProfitCenterAKId OR lkp_InsuranceSegmentAKId != v_InsuranceSegmentAKId OR LTRIM(RTRIM(lkp_AssociationCode)) != LTRIM(RTRIM(v_AssociationCode)) OR lkp_ProgramAKId != v_ProgramAKId OR ( lkp_PolicyOfferingAKId != v_PolicyOfferingAKId AND ( lkp_PolicyOfferingAKId = - 1 OR lkp_PolicyOfferingAKId = v_PolicyOfferingAKId_default ) ) OR lkp_ObligeeName != ObligeeName OR lkp_AutomatedUnderwritingServicesIndicator != AutomatedUnderwritingServicesIndicator OR v_lkp_AutomaticRenewalIndicator != AutomaticRenewalIndicator OR lkp_AgencyEmployeeAKId != in_AgencyEmployeeAKID OR 1 != 1, 'UPDATE', 'NOCHANGE')) AS v_changed_flag,
	-- *INF*: :LKP.LKP_PIF02_MAXDATE(1)
	LKP_PIF02_MAXDATE_1.pif_date_time_stamp AS lkp_pif_date_time_stamp,
	v_changed_flag AS out_changed_flag,
	1 AS out_crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS out_audit_id,
	-- *INF*: DECODE(TRUE,
	-- v_changed_flag='NEW',	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- IS_DATE(lkp_pif_date_time_stamp,'YYYYMMDD'),TO_DATE(lkp_pif_date_time_stamp||' 23:59:59','YYYYMMDD HH24:MI:SS'))
	DECODE(TRUE,
	v_changed_flag = 'NEW', to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	IS_DATE(lkp_pif_date_time_stamp, 'YYYYMMDD'), TO_DATE(lkp_pif_date_time_stamp || ' 23:59:59', 'YYYYMMDD HH24:MI:SS')) AS out_eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS out_eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS out_source_sys_id,
	SYSDATE AS out_created_date,
	SYSDATE AS out_modified_date,
	v_cust_ak_id AS out_cust_ak_id,
	v_agency_ak_id AS out_agency_ak_id,
	v_AgencyAKId AS out_AgencyAKId,
	v_producer_code_ak_id AS out_producer_code_ak_id,
	v_strtgc_bus_dvsn_ak_id AS out_strtgc_bus_dvsn_ak_id,
	v_producer_code_id AS out_producer_code_id,
	v_sup_bus_class_code_id AS out_sup_bus_class_code_id,
	v_sup_pol_term_id AS out_sup_pol_term_id,
	v_sup_pol_status_code_id AS out_sup_pol_status_code_id,
	v_sup_pol_issue_code_id AS out_sup_pol_issue_code_id,
	v_sup_pol_audit_frqncy_id AS out_sup_pol_audit_frqncy_id,
	v_sup_industry_risk_grade_code_id AS out_sup_industry_risk_grade_code_id,
	v_sup_state_id AS out_sup_state_id,
	v_StrategicProfitCenterAKId AS out_StrategicProfitCenterAKId,
	v_InsuranceSegmentAKId AS out_InsuranceSegmentAKId,
	-- *INF*: IIF(ISNULL(lkp_PolicyOfferingAKId) OR lkp_PolicyOfferingAKId=v_PolicyOfferingAKId_default OR lkp_PolicyOfferingAKId=-1, in_PolicyOfferingAKId, lkp_PolicyOfferingAKId)
	IFF(lkp_PolicyOfferingAKId IS NULL OR lkp_PolicyOfferingAKId = v_PolicyOfferingAKId_default OR lkp_PolicyOfferingAKId = - 1, in_PolicyOfferingAKId, lkp_PolicyOfferingAKId) AS out_PolicyOfferingAKId,
	v_ProgramAKId AS out_ProgramAKId,
	v_AssociationCode AS out_AssociationCode,
	v_AgencyEmployeeAKId AS out_AgencyEmployeeAKId,
	v_pol_issue_code AS o_pol_issue_code,
	EXP_values.pif_dc_bill_ind
	FROM EXP_values
	 -- Manually join with exp_user_code
	 -- Manually join with mplt_PMS_StrategicProfitCenterInsuranceSegment
	 -- Manually join with mplt_Strategic_Business_Division
	LEFT JOIN LKP_AgencyAKId
	ON LKP_AgencyAKId.AgencyCode = EXP_values.agency_key
	LEFT JOIN LKP_AgencyEmployee
	ON LKP_AgencyEmployee.AgencyAKID = LKP_AgencyAKId.AgencyAKId AND LKP_AgencyEmployee.ProducerCode = EXP_values.producer_code
	LEFT JOIN LKP_Association
	ON LKP_Association.AssociationCode = EXP_values.o_ClassOfBusiness
	LEFT JOIN LKP_Policy
	ON LKP_Policy.pol_key = EXP_values.policy_key
	LEFT JOIN LKP_ProgramAKId
	ON LKP_ProgramAKId.ProgramCode = LKP_sup_association_program_code.assoc_prog_code
	LEFT JOIN LKP_SupSurchargeExempt
	ON LKP_SupSurchargeExempt.SurchargeExemptCode = exp_user_code.out_use_code
	LEFT JOIN LKP_agency_ak_id
	ON LKP_agency_ak_id.agency_key = EXP_values.agency_key
	LEFT JOIN LKP_contract_customer_key
	ON LKP_contract_customer_key.contract_key = EXP_values.policy_key
	LEFT JOIN LKP_producer_code_ak_id
	ON LKP_producer_code_ak_id.producer_code = EXP_values.producer_code AND LKP_producer_code_ak_id.agency_key = EXP_values.agency_key
	LEFT JOIN LKP_sup_business_classification_code
	ON LKP_sup_business_classification_code.bus_class_code = EXP_values.prim_bus_class_code
	LEFT JOIN LKP_sup_industry_risk_grade_code
	ON LKP_sup_industry_risk_grade_code.industry_risk_grade_code = EXP_values.industry_risk_grade_code
	LEFT JOIN LKP_sup_policy_audit_frequency
	ON LKP_sup_policy_audit_frequency.pol_audit_frqncy = EXP_values.pol_audit_frqncy
	LEFT JOIN LKP_sup_policy_status_code
	ON LKP_sup_policy_status_code.pol_status_code = EXP_values.policy_status_code
	LEFT JOIN LKP_sup_policy_term
	ON LKP_sup_policy_term.pol_term = EXP_values.pol_term
	LEFT JOIN LKP_sup_state
	ON LKP_sup_state.state_abbrev = EXP_values.state_of_domicile_code
	LEFT JOIN LKP_POLICYOFFERINGAKID LKP_POLICYOFFERINGAKID__000
	ON LKP_POLICYOFFERINGAKID__000.PolicyOfferingCode = '000'

	LEFT JOIN LKP_STRATEGICPROFITCENTER LKP_STRATEGICPROFITCENTER_v_StrategicProfitCenterAKId
	ON LKP_STRATEGICPROFITCENTER_v_StrategicProfitCenterAKId.StrategicProfitCenterAKId = v_StrategicProfitCenterAKId

	LEFT JOIN LKP_SUP_POLICY_ISSUE_CODE LKP_SUP_POLICY_ISSUE_CODE_v_pol_issue_code
	ON LKP_SUP_POLICY_ISSUE_CODE_v_pol_issue_code.pol_issue_code = v_pol_issue_code

	LEFT JOIN LKP_PIF02_MAXDATE LKP_PIF02_MAXDATE_1
	ON LKP_PIF02_MAXDATE_1.ID = 1

),
FIL_insert AS (
	SELECT
	lkp_pol_ak_id, 
	pif_symbol, 
	pif_policy_number, 
	pif_module, 
	policy_key, 
	pol_eff_date, 
	pol_exp_date, 
	mco, 
	pol_term, 
	state_of_domicile_code, 
	pol_co_num, 
	pif_pol_lob_code, 
	pol_co_line_code, 
	pol_cancellation_ind, 
	renl_code, 
	orig_incep_date, 
	wbconnect_upload_code, 
	prim_bus_class_code, 
	terrorism_risk_ind, 
	prior_pol_key, 
	o_pol_issue_code AS pol_issue_code, 
	pol_age, 
	industry_risk_grade_code, 
	amend_num, 
	anniversary_rerate_code, 
	pol_audit_frqncy, 
	final_audit_code, 
	zip_ind, 
	gutantee_ind, 
	variation_code, 
	county, 
	non_smoker_disc_code, 
	renl_disc, 
	pif_upload_audit, 
	pif_seg_id, 
	renl_safe_driver_disc_count, 
	non_renewal_flag_date, 
	audit_complt_date, 
	original_account_date, 
	pol_enter_date, 
	excess_claim_code, 
	pif_policy_status_on, 
	out_cust_ak_id AS cust_ak_id, 
	out_agency_ak_id AS agency_ak_id, 
	out_AgencyAKId AS AgencyAKId, 
	out_producer_code_ak_id AS producer_code_ak_id, 
	reins_code, 
	pol_cancellation_date, 
	pol_cancellation_rsn_code, 
	serv_center_support_code, 
	pol_status_code, 
	uw_review_yr, 
	mvr_request_code, 
	pif_target_market_code, 
	pkg_code, 
	out_changed_flag AS changed_flag, 
	out_crrnt_snpsht_flag AS crrnt_snpsht_flag, 
	out_audit_id AS audit_id, 
	out_eff_from_date AS eff_from_date, 
	out_eff_to_date AS eff_to_date, 
	out_source_sys_id AS source_sys_id, 
	out_created_date AS created_date, 
	out_modified_date AS modified_date, 
	producer_code, 
	ClassOfBusiness, 
	out_strtgc_bus_dvsn_ak_id AS strtgc_bus_dvsn_ak_id, 
	renewalpolicynumber, 
	renewalpolicysymbol, 
	RenewalPolicyMod, 
	BillingType, 
	out_producer_code_id AS producer_code_id, 
	out_sup_bus_class_code_id AS sup_bus_class_code_id, 
	out_sup_pol_term_id AS sup_pol_term_id, 
	out_sup_pol_status_code_id AS sup_pol_status_code_id, 
	out_sup_pol_issue_code_id AS sup_pol_issue_code_id, 
	out_sup_pol_audit_frqncy_id AS sup_pol_audit_frqncy_id, 
	out_sup_industry_risk_grade_code_id AS sup_industry_risk_grade_code_id, 
	out_sup_state_id AS sup_state_id, 
	SurchargeExemptCode, 
	SupSurchargeExemptID, 
	out_StrategicProfitCenterAKId AS StrategicProfitCenterAKId, 
	out_InsuranceSegmentAKId AS InsuranceSegmentAKId, 
	out_PolicyOfferingAKId AS PolicyOfferingAKId, 
	out_ProgramAKId AS ProgramAKId, 
	ObligeeName, 
	AutomatedUnderwritingServicesIndicator, 
	AutomaticRenewalIndicator, 
	out_AssociationCode, 
	out_AgencyEmployeeAKId AS AgencyEmployeeAKId, 
	pif_dc_bill_ind
	FROM EXP_Detect_Changes
	WHERE changed_flag='NEW'or changed_flag='UPDATE'
),
SEQ_policy_cus_ak_id AS (
	CREATE SEQUENCE SEQ_policy_cus_ak_id
	START = 0
	INCREMENT = 1;
),
EXP_policy_ak_id AS (
	SELECT
	lkp_pol_ak_id,
	SEQ_policy_cus_ak_id.NEXTVAL,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	cust_ak_id,
	agency_ak_id,
	AgencyAKId,
	producer_code_ak_id,
	pif_symbol,
	pif_policy_number,
	pif_module,
	policy_key,
	pol_eff_date,
	pol_exp_date,
	mco,
	pol_term,
	state_of_domicile_code,
	pol_co_num,
	pif_pol_lob_code,
	pol_co_line_code,
	pol_cancellation_ind,
	renl_code,
	orig_incep_date,
	wbconnect_upload_code,
	prim_bus_class_code,
	terrorism_risk_ind,
	prior_pol_key,
	pol_issue_code,
	pol_age,
	industry_risk_grade_code,
	amend_num,
	anniversary_rerate_code,
	pol_audit_frqncy,
	final_audit_code,
	zip_ind,
	gutantee_ind,
	variation_code,
	county,
	non_smoker_disc_code,
	renl_disc,
	pif_upload_audit,
	pif_seg_id,
	renl_safe_driver_disc_count,
	non_renewal_flag_date,
	audit_complt_date,
	original_account_date,
	pol_enter_date,
	excess_claim_code,
	pif_policy_status_on,
	reins_code,
	pol_cancellation_date,
	pol_cancellation_rsn_code,
	serv_center_support_code,
	pol_status_code,
	uw_review_yr,
	mvr_request_code,
	pif_target_market_code,
	pkg_code,
	producer_code,
	ClassOfBusiness,
	strtgc_bus_dvsn_ak_id,
	renewalpolicynumber,
	renewalpolicysymbol,
	RenewalPolicyMod,
	BillingType,
	-- *INF*: iif(isnull(lkp_pol_ak_id),NEXTVAL,lkp_pol_ak_id)
	IFF(lkp_pol_ak_id IS NULL, NEXTVAL, lkp_pol_ak_id) AS pol_ak_id,
	'D' AS pol_kind_code,
	0 AS Default_Int,
	producer_code_id,
	sup_bus_class_code_id,
	sup_pol_term_id,
	sup_pol_status_code_id,
	sup_pol_issue_code_id,
	sup_pol_audit_frqncy_id,
	sup_industry_risk_grade_code_id,
	sup_state_id,
	SurchargeExemptCode,
	SupSurchargeExemptID,
	StrategicProfitCenterAKId,
	InsuranceSegmentAKId,
	PolicyOfferingAKId,
	ProgramAKId,
	-1 AS o_UnderwritingAssociateAKId,
	ObligeeName,
	AutomatedUnderwritingServicesIndicator,
	AutomaticRenewalIndicator,
	out_AssociationCode,
	0 AS RolloverPolicyIndicator,
	'N/A' AS RolloverPriorCarrier,
	'0' AS MailToInsuredFlag,
	AgencyEmployeeAKId,
	0 AS PolicyIssueCodeOverride,
	pif_dc_bill_ind,
	-- *INF*: DECODE( TRUE,
	-- pif_dc_bill_ind = 'Y',1,
	-- 0)
	DECODE(TRUE,
	pif_dc_bill_ind = 'Y', 1,
	0) AS DCBillFlag,
	'N/A' AS IssuedUWID,
	'N/A' AS IssuedUnderwriter
	FROM FIL_insert
),
TGT_policy_INSERT AS (

	------------ PRE SQL ----------
	exec [spSetIndexStatus] @Enable = 0, @Schema = 'V2', @TableName = 'policy', @IndexWildcard = 'Ak3Policy'
	-------------------------------


	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, pol_ak_id, contract_cust_ak_id, agency_ak_id, pol_sym, pol_num, pol_mod, pol_key, mco, pol_co_num, pol_eff_date, pol_exp_date, orig_incptn_date, prim_bus_class_code, reins_code, pms_pol_lob_code, pol_co_line_code, pol_cancellation_ind, pol_cancellation_date, pol_cancellation_rsn_code, state_of_domicile_code, wbconnect_upload_code, serv_center_support_code, pol_term, terrorism_risk_ind, prior_pol_key, pol_status_code, pol_issue_code, pol_age, industry_risk_grade_code, uw_review_yr, mvr_request_code, renl_code, amend_num, anniversary_rerate_code, pol_audit_frqncy, final_audit_code, zip_ind, guarantee_ind, variation_code, county, non_smoker_disc_code, renl_disc, renl_safe_driver_disc_count, nonrenewal_flag_date, audit_complt_date, orig_acct_date, pol_enter_date, excess_claim_code, pol_status_on_pif, target_mrkt_code, pkg_code, pol_kind_code, bus_seg_code, pif_upload_audit_ind, err_flag_bal_txn, err_flag_bal_reins, producer_code_ak_id, prdcr_code, ClassOfBusiness, strtgc_bus_dvsn_ak_id, ErrorFlagBalancePremiumTransaction, RenewalPolicyNumber, RenewalPolicySymbol, RenewalPolicyMod, BillingType, producer_code_id, sup_bus_class_code_id, sup_pol_term_id, sup_pol_status_code_id, sup_pol_issue_code_id, sup_pol_audit_frqncy_id, sup_industry_risk_grade_code_id, sup_state_id, SurchargeExemptCode, SupSurchargeExemptID, StrategicProfitCenterAKId, InsuranceSegmentAKId, PolicyOfferingAKId, ProgramAKId, AgencyAKId, UnderwritingAssociateAKId, ObligeeName, AutomatedUnderwritingServicesIndicator, AutomaticRenewalIndicator, AssociationCode, RolloverPolicyIndicator, RolloverPriorCarrier, MailToInsuredFlag, AgencyEmployeeAKId, PolicyIssueCodeOverride, DCBillFlag, IssuedUWID, IssuedUnderwriter)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	POL_AK_ID, 
	cust_ak_id AS CONTRACT_CUST_AK_ID, 
	AGENCY_AK_ID, 
	pif_symbol AS POL_SYM, 
	pif_policy_number AS POL_NUM, 
	pif_module AS POL_MOD, 
	policy_key AS POL_KEY, 
	MCO, 
	POL_CO_NUM, 
	POL_EFF_DATE, 
	POL_EXP_DATE, 
	orig_incep_date AS ORIG_INCPTN_DATE, 
	PRIM_BUS_CLASS_CODE, 
	REINS_CODE, 
	pif_pol_lob_code AS PMS_POL_LOB_CODE, 
	POL_CO_LINE_CODE, 
	POL_CANCELLATION_IND, 
	POL_CANCELLATION_DATE, 
	POL_CANCELLATION_RSN_CODE, 
	STATE_OF_DOMICILE_CODE, 
	WBCONNECT_UPLOAD_CODE, 
	SERV_CENTER_SUPPORT_CODE, 
	POL_TERM, 
	TERRORISM_RISK_IND, 
	PRIOR_POL_KEY, 
	POL_STATUS_CODE, 
	POL_ISSUE_CODE, 
	POL_AGE, 
	INDUSTRY_RISK_GRADE_CODE, 
	UW_REVIEW_YR, 
	MVR_REQUEST_CODE, 
	RENL_CODE, 
	AMEND_NUM, 
	ANNIVERSARY_RERATE_CODE, 
	POL_AUDIT_FRQNCY, 
	FINAL_AUDIT_CODE, 
	ZIP_IND, 
	gutantee_ind AS GUARANTEE_IND, 
	VARIATION_CODE, 
	COUNTY, 
	NON_SMOKER_DISC_CODE, 
	RENL_DISC, 
	RENL_SAFE_DRIVER_DISC_COUNT, 
	non_renewal_flag_date AS NONRENEWAL_FLAG_DATE, 
	AUDIT_COMPLT_DATE, 
	original_account_date AS ORIG_ACCT_DATE, 
	POL_ENTER_DATE, 
	EXCESS_CLAIM_CODE, 
	pif_policy_status_on AS POL_STATUS_ON_PIF, 
	pif_target_market_code AS TARGET_MRKT_CODE, 
	PKG_CODE, 
	POL_KIND_CODE, 
	pif_seg_id AS BUS_SEG_CODE, 
	pif_upload_audit AS PIF_UPLOAD_AUDIT_IND, 
	Default_Int AS ERR_FLAG_BAL_TXN, 
	Default_Int AS ERR_FLAG_BAL_REINS, 
	PRODUCER_CODE_AK_ID, 
	producer_code AS PRDCR_CODE, 
	CLASSOFBUSINESS, 
	STRTGC_BUS_DVSN_AK_ID, 
	Default_Int AS ERRORFLAGBALANCEPREMIUMTRANSACTION, 
	renewalpolicynumber AS RENEWALPOLICYNUMBER, 
	renewalpolicysymbol AS RENEWALPOLICYSYMBOL, 
	RENEWALPOLICYMOD, 
	BILLINGTYPE, 
	PRODUCER_CODE_ID, 
	SUP_BUS_CLASS_CODE_ID, 
	SUP_POL_TERM_ID, 
	SUP_POL_STATUS_CODE_ID, 
	SUP_POL_ISSUE_CODE_ID, 
	SUP_POL_AUDIT_FRQNCY_ID, 
	SUP_INDUSTRY_RISK_GRADE_CODE_ID, 
	SUP_STATE_ID, 
	SURCHARGEEXEMPTCODE, 
	SUPSURCHARGEEXEMPTID, 
	STRATEGICPROFITCENTERAKID, 
	INSURANCESEGMENTAKID, 
	POLICYOFFERINGAKID, 
	PROGRAMAKID, 
	AGENCYAKID, 
	o_UnderwritingAssociateAKId AS UNDERWRITINGASSOCIATEAKID, 
	OBLIGEENAME, 
	AUTOMATEDUNDERWRITINGSERVICESINDICATOR, 
	AUTOMATICRENEWALINDICATOR, 
	out_AssociationCode AS ASSOCIATIONCODE, 
	ROLLOVERPOLICYINDICATOR, 
	ROLLOVERPRIORCARRIER, 
	MAILTOINSUREDFLAG, 
	AGENCYEMPLOYEEAKID, 
	POLICYISSUECODEOVERRIDE, 
	DCBILLFLAG, 
	ISSUEDUWID, 
	ISSUEDUNDERWRITER
	FROM EXP_policy_ak_id
),
SQ_policy AS (
	SELECT 
		a.pol_id, 
		a.eff_from_date,
		a.eff_to_date, 
		a.pol_ak_id  
	FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy a
	WHERE  a.pol_ak_id  IN
		( SELECT pol_ak_id  FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy
		WHERE CRRNT_SNPSHT_FLAG = 1 GROUP BY pol_ak_id HAVING count(*) > 1) 
	AND source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	ORDER BY a.pol_ak_id ,a.eff_from_date DESC
	
	
	
	--IN Subquery exists to pick AK ID column values that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag 
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
),
EXP_Lag_eff_from_date AS (
	SELECT
	pol_id,
	eff_from_date,
	eff_to_date AS eff_to_date1,
	pol_ak_id,
	-- *INF*: DECODE(TRUE,
	-- pol_ak_id = v_prev_pol_ak_id ,
	-- ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),eff_to_date1)
	DECODE(TRUE,
	pol_ak_id = v_prev_pol_ak_id, ADD_TO_DATE(v_prev_eff_from_date, 'SS', - 1),
	eff_to_date1) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	pol_ak_id AS v_prev_pol_ak_id,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_policy
),
FIL_FirstRowInAKGroup AS (
	SELECT
	pol_id, 
	eff_to_date1, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_eff_from_date
	WHERE eff_to_date1 != eff_to_date
),
UPD_policy AS (
	SELECT
	pol_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_FirstRowInAKGroup
),
TGT_policy_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy AS T
	USING UPD_policy AS S
	ON T.pol_id = S.pol_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date

	------------ POST SQL ----------
	exec [spSetIndexStatus] @Enable = 1, @Schema = 'V2', @TableName = 'policy', @IndexWildcard = 'Ak3Policy'
	-------------------------------


),
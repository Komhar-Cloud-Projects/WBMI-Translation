WITH
LKP_sup_cms_relation_indicator AS (
	SELECT
	cms_relation_file_code,
	cms_party_type,
	cms_relation_ind,
	is_cms_party_individ
	FROM (
		SELECT 
		sup_cms_relation_indicator.cms_relation_file_code as cms_relation_file_code, 
		sup_cms_relation_indicator.cms_party_type as cms_party_type, 
		sup_cms_relation_indicator.cms_relation_ind as cms_relation_ind, 
		sup_cms_relation_indicator.is_cms_party_individ as is_cms_party_individ 
		FROM sup_cms_relation_indicator
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY cms_party_type,cms_relation_ind,is_cms_party_individ ORDER BY cms_relation_file_code) = 1
),
LKP_sup_cms_tin_office_TIN_By_Dummy_Id AS (
	SELECT
	office_tin_num,
	dummy_integer
	FROM (
		SELECT 
		sup_cms_tin_office.office_tin_num as office_tin_num, 
		1 as dummy_integer 
		FROM sup_cms_tin_office
		where crrnt_snpsht_flag='1'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY dummy_integer ORDER BY office_tin_num) = 1
),
SQ_claim_medical_patient_diagnosis_additional AS (
	SELECT claim_medical_patient_diagnosis_additional.claim_med_patient_diag_add_ak_id, claim_medical_patient_diagnosis_additional.claim_med_ak_id, claim_medical_patient_diagnosis_additional.patient_add_code, claim_medical_patient_diagnosis_additional.patient_diag_code 
	FROM
	 claim_medical_patient_diagnosis_additional 
	WHERE
	 crrnt_snpsht_flag = 1
),
SRT_Source AS (
	SELECT
	claim_med_patient_diag_add_ak_id, 
	claim_med_ak_id, 
	patient_add_code, 
	patient_diag_code
	FROM SQ_claim_medical_patient_diagnosis_additional
	ORDER BY claim_med_patient_diag_add_ak_id ASC
),
AGG_1 AS (
	SELECT
	claim_med_patient_diag_add_ak_id,
	claim_med_ak_id,
	patient_add_code,
	patient_diag_code,
	-- *INF*: FIRST(patient_diag_code,patient_add_code=2)
	FIRST(patient_diag_code, patient_add_code = 2) AS patient_diag_code2,
	-- *INF*: FIRST(patient_diag_code,patient_add_code=3)
	FIRST(patient_diag_code, patient_add_code = 3) AS patient_diag_code3,
	-- *INF*: FIRST(patient_diag_code,patient_add_code=4)
	FIRST(patient_diag_code, patient_add_code = 4) AS patient_diag_code4,
	-- *INF*: FIRST(patient_diag_code,patient_add_code=5)
	FIRST(patient_diag_code, patient_add_code = 5) AS patient_diag_code5,
	-- *INF*: FIRST(patient_diag_code,patient_add_code=6)
	FIRST(patient_diag_code, patient_add_code = 6) AS patient_diag_code6,
	-- *INF*: FIRST(patient_diag_code,patient_add_code=7)
	FIRST(patient_diag_code, patient_add_code = 7) AS patient_diag_code7,
	-- *INF*: FIRST(patient_diag_code,patient_add_code=8)
	FIRST(patient_diag_code, patient_add_code = 8) AS patient_diag_code8,
	-- *INF*: FIRST(patient_diag_code,patient_add_code=9)
	FIRST(patient_diag_code, patient_add_code = 9) AS patient_diag_code9,
	-- *INF*: FIRST(patient_diag_code,patient_add_code=10)
	FIRST(patient_diag_code, patient_add_code = 10) AS patient_diag_code10,
	-- *INF*: FIRST(patient_diag_code,patient_add_code=11)
	FIRST(patient_diag_code, patient_add_code = 11) AS patient_diag_code11,
	-- *INF*: FIRST(patient_diag_code,patient_add_code=12)
	FIRST(patient_diag_code, patient_add_code = 12) AS patient_diag_code12,
	-- *INF*: FIRST(patient_diag_code,patient_add_code=13)
	FIRST(patient_diag_code, patient_add_code = 13) AS patient_diag_code13,
	-- *INF*: FIRST(patient_diag_code,patient_add_code=14)
	FIRST(patient_diag_code, patient_add_code = 14) AS patient_diag_code14,
	-- *INF*: FIRST(patient_diag_code,patient_add_code=15)
	FIRST(patient_diag_code, patient_add_code = 15) AS patient_diag_code15,
	-- *INF*: FIRST(patient_diag_code,patient_add_code=16)
	FIRST(patient_diag_code, patient_add_code = 16) AS patient_diag_code16,
	-- *INF*: FIRST(patient_diag_code,patient_add_code=17)
	FIRST(patient_diag_code, patient_add_code = 17) AS patient_diag_code17,
	-- *INF*: FIRST(patient_diag_code,patient_add_code=18)
	FIRST(patient_diag_code, patient_add_code = 18) AS patient_diag_code18,
	-- *INF*: FIRST(patient_diag_code,patient_add_code=19)
	FIRST(patient_diag_code, patient_add_code = 19) AS patient_diag_code19
	FROM SRT_Source
	GROUP BY claim_med_ak_id
),
EXP_Diag_Code AS (
	SELECT
	claim_med_patient_diag_add_ak_id,
	claim_med_ak_id,
	patient_diag_code2,
	patient_diag_code3,
	patient_diag_code4,
	patient_diag_code5,
	patient_diag_code6,
	patient_diag_code7,
	patient_diag_code8,
	patient_diag_code9,
	patient_diag_code10,
	patient_diag_code11,
	patient_diag_code12,
	patient_diag_code13,
	patient_diag_code14,
	patient_diag_code15,
	patient_diag_code16,
	patient_diag_code17,
	patient_diag_code18,
	patient_diag_code19
	FROM AGG_1
),
SQ_Sources AS (
	SELECT 
	claim_medical.claim_med_ak_id, 
	claim_medical.claim_party_occurrence_ak_id, 
	claim_medical_plan.claim_med_plan_ak_id, 
	claim_occurrence.claim_occurrence_ak_id, 
	claim_party.claim_party_ak_id,
	claim_party.claim_party_key,
	claim_party.source_sys_id,
	--'NGCD' as Record_identifier,
	claim_medical_plan.cms_document_cntl_num,
	cms_control_tab_stage.cms_action_type,
	claim_medical.medicare_hicn,
	CASE claim_medical.medicare_hicn
		WHEN 'N/A' THEN claim_party.tax_ssn_id
		ELSE 'N/A'
	END as tax_ssn_id,
	claim_party.claim_party_last_name,
	claim_party.claim_party_first_name,
	CASE claim_party.claim_party_gndr 
		WHEN 'M' THEN '1' 
		WHEN 'F' THEN '2'
		WHEN '1' THEN '1'
		WHEN '2' THEN '2'
	    ELSE '0'
	END AS claim_party_gndr,
	REPLACE(CONVERT(VARCHAR(8), claim_party.claim_party_birthdate, 112),'/','') AS claim_party_birthdate,
	REPLACE(CONVERT(VARCHAR(8), claim_medical.cms_incdnt_date, 112),'/','') AS cms_incdnt_date,
	REPLACE(CONVERT(VARCHAR(8), claim_occurrence.claim_loss_date, 112),'/','') AS claim_loss_date,
	claim_medical.patient_cause_code,
	claim_medical_plan.state_venue,
	claim_medical.patient_diag_code,
	claim_medical.prdct_liab_ind,
	claim_medical.prdct_generic_name,
	claim_medical.prdct_brand_name,
	claim_medical.prdct_mfr,
	claim_medical.prdct_alleged_harm,
	claim_medical.self_insd_ind,
	claim_medical.self_insd_type,
	claim_medical.self_insd_last_name,
	claim_medical.self_insd_first_name,
	claim_medical.self_insd_dba_name,	
	claim_medical.self_insd_lgl_name,
	claim_medical_plan.wbmi_plan_ins_type,
	claim_occurrence.pol_key ,
	CASE claim_occurrence.source_sys_id 
		WHEN 'EXCEED' THEN claim_occurrence.s3p_claim_num
		WHEN 'PMS' THEN claim_occurrence.claim_occurrence_key
	    ELSE 'N/A'
	END AS claim_occurrence_key,
	claim_medical_plan.no_fault_ins_lmt,
	REPLACE(CONVERT(VARCHAR(8), claim_medical_plan.exhaust_lmt_date, 112),'/','') AS exhaust_lmt_date,
	claim_medical.injured_party_rep_firm, 
	claim_medical_plan.med_obligation_to_claimant,
	REPLACE(CONVERT(VARCHAR(8), claim_medical_plan.orm_termination_date, 112),'/','') AS orm_termination_date,
	claim_medical.claimant1_rep_firm,claim_medical.last_cms_hicn,claim_medical.ICDCodeVersion
	FROM
	claim_occurrence claim_occurrence 
	inner join claim_party_occurrence on claim_party_occurrence.claim_occurrence_ak_id = claim_occurrence.claim_occurrence_ak_id
	  AND claim_party_occurrence.crrnt_snpsht_flag = 1
	inner join claim_party claim_party on claim_party_occurrence.claim_party_ak_id = claim_party.claim_party_ak_id  
		AND claim_party.crrnt_snpsht_flag = 1
	inner join claim_medical claim_medical on claim_medical.claim_party_occurrence_ak_id = claim_party_occurrence.claim_party_occurrence_ak_ID  
		AND claim_medical.crrnt_snpsht_flag = 1
	inner join claim_medical_plan claim_medical_plan on claim_medical.claim_med_ak_id = claim_medical_plan.claim_med_ak_id  
		AND claim_medical_plan.crrnt_snpsht_flag = 1
	inner join wc_stage.dbo.cms_control_tab_stage cms_control_tab_stage on claim_medical_plan.cms_document_cntl_num = cms_control_tab_stage.cms_doc_cntl_num 
	AND claim_occurrence.crrnt_snpsht_flag = 1
	AND cms_control_tab_stage.cms_report_status = 'T' 
	@{pipeline().parameters.WHERE_EXCEED_PMS}
	AND claim_occurrence.source_sys_id in('EXCEED', 'PMS')
	
	
	--AND cms_control_tab_stage.cms_action_type IN ('0','1','2','3')
	-- for testing use
	--AND cms_control_tab_stage.cms_report_status = 'S'
	UNION
	SELECT 
	claim_medical.claim_med_ak_id, 
	claim_medical.claim_party_occurrence_ak_id, 
	claim_medical_plan.claim_med_plan_ak_id, 
	claim_occurrence.claim_occurrence_ak_id, 
	claim_party.claim_party_ak_id,
	claim_party.claim_party_key,
	claim_party.source_sys_id,
	--'NGCD' as Record_identifier,
	claim_medical_plan.cms_document_cntl_num,
	CMSControlTableStage.CMSActionType,
	claim_medical.medicare_hicn,
	CASE claim_medical.medicare_hicn
		WHEN 'N/A' THEN claim_party.tax_ssn_id
		ELSE 'N/A'
	END as tax_ssn_id,
	claim_party.claim_party_last_name,
	claim_party.claim_party_first_name,
	CASE claim_party.claim_party_gndr 
		WHEN 'M' THEN '1' 
		WHEN 'F' THEN '2'
		WHEN '1' THEN '1'
		WHEN '2' THEN '2'
	    ELSE '0'
	END AS claim_party_gndr,
	REPLACE(CONVERT(VARCHAR(8), claim_party.claim_party_birthdate, 112),'/','') AS claim_party_birthdate,
	REPLACE(CONVERT(VARCHAR(8), claim_medical.cms_incdnt_date, 112),'/','') AS cms_incdnt_date,
	REPLACE(CONVERT(VARCHAR(8), claim_occurrence.claim_loss_date, 112),'/','') AS claim_loss_date,
	claim_medical.patient_cause_code,
	claim_medical_plan.state_venue,
	claim_medical.patient_diag_code,
	claim_medical.prdct_liab_ind,
	claim_medical.prdct_generic_name,
	claim_medical.prdct_brand_name,
	claim_medical.prdct_mfr,
	claim_medical.prdct_alleged_harm,
	claim_medical.self_insd_ind,
	claim_medical.self_insd_type,
	claim_medical.self_insd_last_name,
	claim_medical.self_insd_first_name,
	claim_medical.self_insd_dba_name,	
	claim_medical.self_insd_lgl_name,
	claim_medical_plan.wbmi_plan_ins_type,
	claim_occurrence.pol_key ,
	CASE claim_occurrence.source_sys_id 
		WHEN 'DCT' THEN claim_occurrence.s3p_claim_num
		ELSE 'N/A'
	END AS claim_occurrence_key,
	claim_medical_plan.no_fault_ins_lmt,
	REPLACE(CONVERT(VARCHAR(8), claim_medical_plan.exhaust_lmt_date, 112),'/','') AS exhaust_lmt_date,
	claim_medical.injured_party_rep_firm, 
	claim_medical_plan.med_obligation_to_claimant,
	REPLACE(CONVERT(VARCHAR(8), claim_medical_plan.orm_termination_date, 112),'/','') AS orm_termination_date,
	claim_medical.claimant1_rep_firm,claim_medical.last_cms_hicn,claim_medical.ICDCodeVersion
	FROM
	claim_occurrence claim_occurrence 
	inner join claim_party_occurrence on claim_party_occurrence.claim_occurrence_ak_id = claim_occurrence.claim_occurrence_ak_id
	  AND claim_party_occurrence.crrnt_snpsht_flag = 1
	inner join claim_party claim_party on claim_party_occurrence.claim_party_ak_id = claim_party.claim_party_ak_id  
		AND claim_party.crrnt_snpsht_flag = 1
	inner join claim_medical claim_medical on claim_medical.claim_party_occurrence_ak_id = claim_party_occurrence.claim_party_occurrence_ak_ID  
		AND claim_medical.crrnt_snpsht_flag = 1
	inner join claim_medical_plan claim_medical_plan on claim_medical.claim_med_ak_id = claim_medical_plan.claim_med_ak_id  
		AND claim_medical_plan.crrnt_snpsht_flag = 1
	inner join wc_stage.dbo.CMSControlTableStage CMSControlTableStage on claim_medical_plan.cms_document_cntl_num = CMSControlTableStage.CMSDocControlNum 
	AND claim_occurrence.crrnt_snpsht_flag = 1
	AND CMSControlTableStage.cmsreportstatus = 'T'
	@{pipeline().parameters.WHERE_DCT}
	AND claim_occurrence.source_sys_id ='DCT'
),
EXP_Source AS (
	SELECT
	claim_med_ak_id,
	claim_party_occurrence_ak_id,
	claim_med_plan_ak_id,
	claim_occurrence_ak_id,
	claim_party_ak_id,
	claim_party_key,
	source_sys_id,
	cms_document_cntl_num,
	cms_action_type AS action_type,
	medicare_hicn,
	tax_ssn_id,
	claim_party_last_name,
	claim_party_first_name,
	claim_party_gndr,
	claim_party_birthdate,
	cms_incdnt_date,
	claim_loss_date,
	patient_cause_code,
	state_venue,
	patient_diag_code,
	prdct_liab_ind,
	prdct_generic_name,
	prdct_brand_name,
	prdct_mfr,
	prdct_alleged_harm,
	self_insd_ind,
	self_insd_type,
	self_insd_last_name,
	self_insd_first_name,
	self_insd_dba_name,
	self_insd_lgl_name,
	wbmi_plan_ins_type,
	pol_key,
	claim_occurrence_key,
	no_fault_ins_lmt,
	exhaust_lmt_date,
	injured_party_rep_firm,
	med_obligation_to_claimant,
	orm_termination_date,
	claimant1_rep_firm,
	last_cms_hicn,
	ICDCodeVersion,
	'MIJA' AS injured_party_rep_type,
	'MCT1' AS claimant_1_type,
	'MCA1' AS claimant_1_rep_type,
	'1' AS tpoc_code_1
	FROM SQ_Sources
),
LKP_Claim_Party_Relation_Claimant_1 AS (
	SELECT
	claim_party_relation_from_ak_id,
	claim_party_relation_role_code,
	is_cms_party_individ,
	claim_party_occurrence_ak_id,
	claim_party_relation_to_ak_id,
	cms_party_type
	FROM (
		SELECT 
			claim_party_relation_from_ak_id,
			claim_party_relation_role_code,
			is_cms_party_individ,
			claim_party_occurrence_ak_id,
			claim_party_relation_to_ak_id,
			cms_party_type
		FROM claim_party_relation
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id,claim_party_relation_to_ak_id,cms_party_type ORDER BY claim_party_relation_from_ak_id) = 1
),
LKP_Claim_Party_Relation_Claimant_1_Rep AS (
	SELECT
	claim_party_relation_from_ak_id,
	claim_party_relation_role_code,
	is_cms_party_individ,
	claim_party_occurrence_ak_id,
	claim_party_relation_to_ak_id,
	cms_party_type
	FROM (
		SELECT 
			claim_party_relation_from_ak_id,
			claim_party_relation_role_code,
			is_cms_party_individ,
			claim_party_occurrence_ak_id,
			claim_party_relation_to_ak_id,
			cms_party_type
		FROM claim_party_relation
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id,claim_party_relation_to_ak_id,cms_party_type ORDER BY claim_party_relation_from_ak_id) = 1
),
LKP_Claim_Party_Relation_Inj_Party_Rep AS (
	SELECT
	claim_party_relation_from_ak_id,
	claim_party_relation_role_code,
	is_cms_party_individ,
	claim_party_occurrence_ak_id,
	claim_party_relation_to_ak_id,
	cms_party_type
	FROM (
		SELECT 
			claim_party_relation_from_ak_id,
			claim_party_relation_role_code,
			is_cms_party_individ,
			claim_party_occurrence_ak_id,
			claim_party_relation_to_ak_id,
			cms_party_type
		FROM claim_party_relation
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id,claim_party_relation_to_ak_id,cms_party_type ORDER BY claim_party_relation_from_ak_id) = 1
),
LKP_claim_medical_plan_tpoc1 AS (
	SELECT
	tpoc_date,
	tpoc_amt,
	tpoc_fund_delay_date,
	claim_med_plan_ak_id,
	tpoc_code
	FROM (
		SELECT 
		REPLACE(CONVERT(VARCHAR(8), a.tpoc_date, 112),'/','') as tpoc_date,
		a.tpoc_amt as tpoc_amt, 
		REPLACE(CONVERT(VARCHAR(8), a.tpoc_fund_delay_date, 112),'/','') as tpoc_fund_delay_date,
		a.claim_med_plan_ak_id as claim_med_plan_ak_id, 
		LTRIM(RTRIM(a.tpoc_code)) as tpoc_code 
		FROM claim_medical_plan_tpoc a
		WHERE 
		crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_med_plan_ak_id,tpoc_code ORDER BY tpoc_date DESC) = 1
),
LKP_cms_pms_relation_stage AS (
	SELECT
	last_name,
	first_name,
	client_key
	FROM (
		SELECT cms_pms_relation_stage.last_name as last_name, cms_pms_relation_stage.first_name as first_name, 
		(pms_policy_sym + pms_policy_num + pms_policy_mod + 
		replace(CONVERT(VARCHAR(10), pms_date_of_loss, 101),'/','') + 
		pms_loss_occurence + pms_loss_claimant + 'CMT') as client_key
		FROM
		 wc_stage.dbo.cms_pms_relation_stage cms_pms_relation_stage, wc_stage.dbo.claim_medical_stage claim_medical_stage
		WHERE
		claim_medical_stage.injured_party_id=cms_pms_relation_stage.injured_party_id
		AND cms_pms_relation_stage.cms_party_type = 'MINJ'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY client_key ORDER BY last_name DESC) = 1
),
EXP_Values AS (
	SELECT
	EXP_Source.claim_med_ak_id,
	EXP_Source.claim_party_occurrence_ak_id,
	EXP_Source.claim_med_plan_ak_id,
	EXP_Source.claim_occurrence_ak_id,
	EXP_Source.claim_party_ak_id,
	EXP_Source.source_sys_id,
	'NGCD' AS record_identifier,
	EXP_Source.cms_document_cntl_num,
	EXP_Source.action_type,
	EXP_Source.medicare_hicn,
	EXP_Source.last_cms_hicn,
	EXP_Source.ICDCodeVersion,
	EXP_Source.tax_ssn_id,
	LKP_cms_pms_relation_stage.last_name,
	LKP_cms_pms_relation_stage.first_name,
	EXP_Source.claim_party_last_name AS in_claim_party_last_name,
	EXP_Source.claim_party_first_name AS in_claim_party_first_name,
	-- *INF*: IIF(source_sys_id='PMS',last_name,in_claim_party_last_name)
	-- 
	-- ---for PMS get first and last name of Injured party from cms_pms_relation_stage
	IFF(source_sys_id = 'PMS', last_name, in_claim_party_last_name) AS claim_party_last_name,
	-- *INF*: IIF(source_sys_id='PMS',first_name,in_claim_party_first_name)
	-- 
	-- ---for PMS get first and last name of Injured party from cms_pms_relation_stage
	IFF(source_sys_id = 'PMS', first_name, in_claim_party_first_name) AS claim_party_first_name,
	EXP_Source.claim_party_gndr,
	EXP_Source.claim_party_birthdate,
	EXP_Source.cms_incdnt_date,
	EXP_Source.claim_loss_date,
	EXP_Source.patient_cause_code,
	EXP_Source.state_venue,
	EXP_Source.patient_diag_code,
	EXP_Source.prdct_liab_ind,
	EXP_Source.prdct_generic_name,
	EXP_Source.prdct_brand_name,
	EXP_Source.prdct_mfr,
	EXP_Source.prdct_alleged_harm,
	EXP_Source.self_insd_ind,
	EXP_Source.self_insd_type,
	EXP_Source.self_insd_last_name,
	EXP_Source.self_insd_first_name,
	EXP_Source.self_insd_dba_name,
	EXP_Source.self_insd_lgl_name,
	EXP_Source.wbmi_plan_ins_type,
	EXP_Source.pol_key,
	EXP_Source.claim_occurrence_key,
	EXP_Source.no_fault_ins_lmt,
	EXP_Source.exhaust_lmt_date,
	EXP_Source.med_obligation_to_claimant,
	EXP_Source.orm_termination_date,
	EXP_Source.injured_party_rep_type,
	LKP_Claim_Party_Relation_Inj_Party_Rep.claim_party_relation_role_code AS injured_party_rep_role_code,
	LKP_Claim_Party_Relation_Inj_Party_Rep.is_cms_party_individ AS injured_party_rep_is_individual,
	-- *INF*: LTRIM(RTRIM(:LKP.LKP_sup_cms_relation_indicator (injured_party_rep_type, injured_party_rep_role_code,'Y')))
	-- 
	-- --we should be able to hard-code either Y or N for the join used for MIJA and MCA1-MCA4 for in_individual column since their Claimant Representative Indicator values dont change in sup_cms_relation_indicator table whether the related party is an individual or not. 
	-- 
	-- --LTRIM(RTRIM(:LKP.LKP_sup_cms_relation_indicator (injured_party_rep_type, injured_party_rep_role_code, injured_party_rep_is_individual)))
	LTRIM(RTRIM(LKP_SUP_CMS_RELATION_INDICATOR_injured_party_rep_type_injured_party_rep_role_code_Y.cms_relation_file_code)) AS injured_party_rep_Ind,
	LKP_Claim_Party_Relation_Inj_Party_Rep.claim_party_relation_from_ak_id AS injured_party_rep_ak_id,
	EXP_Source.claimant_1_type,
	LKP_Claim_Party_Relation_Claimant_1.claim_party_relation_role_code AS claimant_1_role_code,
	LKP_Claim_Party_Relation_Claimant_1.is_cms_party_individ AS claimant_1_is_individual,
	-- *INF*: LTRIM(RTRIM(:LKP.LKP_sup_cms_relation_indicator
	-- (claimant_1_type, claimant_1_role_code, claimant_1_is_individual)))
	LTRIM(RTRIM(LKP_SUP_CMS_RELATION_INDICATOR_claimant_1_type_claimant_1_role_code_claimant_1_is_individual.cms_relation_file_code)) AS claimant_1_Ind,
	LKP_Claim_Party_Relation_Claimant_1.claim_party_relation_from_ak_id AS claimant_1_ak_id,
	EXP_Source.claimant_1_rep_type,
	LKP_Claim_Party_Relation_Claimant_1_Rep.claim_party_relation_role_code AS claimant_1_rep_role_code,
	LKP_Claim_Party_Relation_Claimant_1_Rep.is_cms_party_individ AS claimant_1_rep_is_individual,
	-- *INF*: LTRIM(RTRIM(:LKP.LKP_sup_cms_relation_indicator (claimant_1_rep_type, claimant_1_rep_role_code,'Y')))
	-- 
	-- --we should be able to hard-code either Y or N for the join used for MIJA and MCA1-MCA4 for in_individual column since their Claimant Representative Indicator values dont change in sup_cms_relation_indicator table whether the related party is an individual or not. 
	-- 
	-- --LTRIM(RTRIM(:LKP.LKP_sup_cms_relation_indicator (claimant_1_rep_type, claimant_1_rep_role_code, claimant_1_rep_is_individual)))
	LTRIM(RTRIM(LKP_SUP_CMS_RELATION_INDICATOR_claimant_1_rep_type_claimant_1_rep_role_code_Y.cms_relation_file_code)) AS claimant_1_rep_Ind,
	LKP_Claim_Party_Relation_Claimant_1_Rep.claim_party_relation_from_ak_id AS claimant_1_rep_ak_id,
	LKP_claim_medical_plan_tpoc1.tpoc_date,
	LKP_claim_medical_plan_tpoc1.tpoc_amt,
	LKP_claim_medical_plan_tpoc1.tpoc_fund_delay_date,
	EXP_Source.injured_party_rep_firm,
	EXP_Source.claimant1_rep_firm
	FROM EXP_Source
	LEFT JOIN LKP_Claim_Party_Relation_Claimant_1
	ON LKP_Claim_Party_Relation_Claimant_1.claim_party_occurrence_ak_id = EXP_Source.claim_party_occurrence_ak_id AND LKP_Claim_Party_Relation_Claimant_1.claim_party_relation_to_ak_id = EXP_Source.claim_party_ak_id AND LKP_Claim_Party_Relation_Claimant_1.cms_party_type = EXP_Source.claimant_1_type
	LEFT JOIN LKP_Claim_Party_Relation_Claimant_1_Rep
	ON LKP_Claim_Party_Relation_Claimant_1_Rep.claim_party_occurrence_ak_id = EXP_Source.claim_party_occurrence_ak_id AND LKP_Claim_Party_Relation_Claimant_1_Rep.claim_party_relation_to_ak_id = EXP_Source.claim_party_ak_id AND LKP_Claim_Party_Relation_Claimant_1_Rep.cms_party_type = EXP_Source.claimant_1_rep_type
	LEFT JOIN LKP_Claim_Party_Relation_Inj_Party_Rep
	ON LKP_Claim_Party_Relation_Inj_Party_Rep.claim_party_occurrence_ak_id = EXP_Source.claim_party_occurrence_ak_id AND LKP_Claim_Party_Relation_Inj_Party_Rep.claim_party_relation_to_ak_id = EXP_Source.claim_party_ak_id AND LKP_Claim_Party_Relation_Inj_Party_Rep.cms_party_type = EXP_Source.injured_party_rep_type
	LEFT JOIN LKP_claim_medical_plan_tpoc1
	ON LKP_claim_medical_plan_tpoc1.claim_med_plan_ak_id = EXP_Source.claim_med_plan_ak_id AND LKP_claim_medical_plan_tpoc1.tpoc_code = EXP_Source.tpoc_code_1
	LEFT JOIN LKP_cms_pms_relation_stage
	ON LKP_cms_pms_relation_stage.client_key = EXP_Source.claim_party_key
	LEFT JOIN LKP_SUP_CMS_RELATION_INDICATOR LKP_SUP_CMS_RELATION_INDICATOR_injured_party_rep_type_injured_party_rep_role_code_Y
	ON LKP_SUP_CMS_RELATION_INDICATOR_injured_party_rep_type_injured_party_rep_role_code_Y.cms_party_type = injured_party_rep_type
	AND LKP_SUP_CMS_RELATION_INDICATOR_injured_party_rep_type_injured_party_rep_role_code_Y.cms_relation_ind = injured_party_rep_role_code
	AND LKP_SUP_CMS_RELATION_INDICATOR_injured_party_rep_type_injured_party_rep_role_code_Y.is_cms_party_individ = 'Y'

	LEFT JOIN LKP_SUP_CMS_RELATION_INDICATOR LKP_SUP_CMS_RELATION_INDICATOR_claimant_1_type_claimant_1_role_code_claimant_1_is_individual
	ON LKP_SUP_CMS_RELATION_INDICATOR_claimant_1_type_claimant_1_role_code_claimant_1_is_individual.cms_party_type = claimant_1_type
	AND LKP_SUP_CMS_RELATION_INDICATOR_claimant_1_type_claimant_1_role_code_claimant_1_is_individual.cms_relation_ind = claimant_1_role_code
	AND LKP_SUP_CMS_RELATION_INDICATOR_claimant_1_type_claimant_1_role_code_claimant_1_is_individual.is_cms_party_individ = claimant_1_is_individual

	LEFT JOIN LKP_SUP_CMS_RELATION_INDICATOR LKP_SUP_CMS_RELATION_INDICATOR_claimant_1_rep_type_claimant_1_rep_role_code_Y
	ON LKP_SUP_CMS_RELATION_INDICATOR_claimant_1_rep_type_claimant_1_rep_role_code_Y.cms_party_type = claimant_1_rep_type
	AND LKP_SUP_CMS_RELATION_INDICATOR_claimant_1_rep_type_claimant_1_rep_role_code_Y.cms_relation_ind = claimant_1_rep_role_code
	AND LKP_SUP_CMS_RELATION_INDICATOR_claimant_1_rep_type_claimant_1_rep_role_code_Y.is_cms_party_individ = 'Y'

),
LKP_Claim_Party_claimant_1 AS (
	SELECT
	tax_id,
	claim_party_last_name,
	claim_party_first_name,
	claim_party_mid_name,
	claim_party_addr,
	claim_party_city,
	claim_party_state,
	claim_party_zip,
	claim_party_zip4,
	ph_num,
	ph_extension,
	claim_party_ak_id
	FROM (
		SELECT 
		CASE tax_fed_id  WHEN 'N/A' THEN replace(tax_ssn_id,'-','')  ELSE replace(tax_fed_id,'-','')  END as tax_id, 
		claim_party.claim_party_last_name as claim_party_last_name, 
		claim_party.claim_party_first_name as claim_party_first_name, 
		CASE claim_party.claim_party_mid_name 
			WHEN 'N/A' THEN '' ELSE SUBSTRING(claim_party.claim_party_mid_name,1,1) END as claim_party_mid_name, 
		claim_party.claim_party_addr as claim_party_addr, 
		claim_party.claim_party_city as claim_party_city, 
		claim_party.claim_party_state as claim_party_state, 
		CASE SUBSTRING(claim_party_zip,1,5) WHEN '-' THEN '' ELSE SUBSTRING(claim_party_zip,1,5) END as claim_party_zip,
		SUBSTRING(claim_party_zip,7,4) as claim_party_zip4,
		claim_party.ph_num as ph_num, 
		claim_party.ph_extension as ph_extension, 
		claim_party.claim_party_ak_id as claim_party_ak_id 
		FROM 
			claim_party
		WHERE
			crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_ak_id ORDER BY tax_id) = 1
),
LKP_Claim_Party_claimant_1_rep AS (
	SELECT
	tax_id,
	claim_party_last_name,
	claim_party_first_name,
	claim_party_mid_name,
	claim_party_addr,
	claim_party_city,
	claim_party_state,
	claim_party_zip,
	claim_party_zip4,
	ph_num,
	ph_extension,
	claim_party_ak_id
	FROM (
		SELECT 
		CASE tax_fed_id  WHEN 'N/A' THEN replace(tax_ssn_id,'-','')  ELSE replace(tax_fed_id,'-','')  END as tax_id, 
		claim_party.claim_party_last_name as claim_party_last_name, 
		claim_party.claim_party_first_name as claim_party_first_name, 
		CASE claim_party.claim_party_mid_name 
			WHEN 'N/A' THEN '' ELSE SUBSTRING(claim_party.claim_party_mid_name,1,1) END as claim_party_mid_name, 
		claim_party.claim_party_addr as claim_party_addr, 
		claim_party.claim_party_city as claim_party_city, 
		claim_party.claim_party_state as claim_party_state, 
		CASE SUBSTRING(claim_party_zip,1,5) WHEN '-' THEN '' ELSE SUBSTRING(claim_party_zip,1,5) END as claim_party_zip,
		SUBSTRING(claim_party_zip,7,4) as claim_party_zip4,
		claim_party.ph_num as ph_num, 
		claim_party.ph_extension as ph_extension, 
		claim_party.claim_party_ak_id as claim_party_ak_id 
		FROM 
			claim_party
		WHERE
			crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_ak_id ORDER BY tax_id) = 1
),
LKP_Claim_Party_injured_party_rep AS (
	SELECT
	tax_id,
	claim_party_last_name,
	claim_party_first_name,
	claim_party_mid_name,
	claim_party_addr,
	claim_party_city,
	claim_party_state,
	claim_party_zip,
	claim_party_zip4,
	ph_num,
	ph_extension,
	claim_party_ak_id
	FROM (
		SELECT 
		CASE tax_fed_id  WHEN 'N/A' THEN replace(tax_ssn_id,'-','')  ELSE replace(tax_fed_id,'-','')  END as tax_id, 
		claim_party.claim_party_last_name as claim_party_last_name, 
		claim_party.claim_party_first_name as claim_party_first_name, 
		CASE claim_party.claim_party_mid_name 
			WHEN 'N/A' THEN '' ELSE SUBSTRING(claim_party.claim_party_mid_name,1,1) END as claim_party_mid_name, 
		claim_party.claim_party_addr as claim_party_addr, 
		claim_party.claim_party_city as claim_party_city, 
		claim_party.claim_party_state as claim_party_state, 
		CASE SUBSTRING(claim_party_zip,1,5) WHEN '-' THEN '' ELSE SUBSTRING(claim_party_zip,1,5) END as claim_party_zip,
		SUBSTRING(claim_party_zip,7,4) as claim_party_zip4,
		claim_party.ph_num as ph_num, 
		claim_party.ph_extension as ph_extension, 
		claim_party.claim_party_ak_id as claim_party_ak_id 
		FROM 
			claim_party
		WHERE
			crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_ak_id ORDER BY tax_id) = 1
),
EXP_Values1 AS (
	SELECT
	EXP_Values.claim_med_ak_id,
	EXP_Values.claim_party_occurrence_ak_id,
	EXP_Values.claim_med_plan_ak_id,
	EXP_Values.claim_occurrence_ak_id,
	EXP_Values.claim_party_ak_id,
	EXP_Values.record_identifier,
	EXP_Values.cms_document_cntl_num,
	EXP_Values.action_type,
	EXP_Values.medicare_hicn,
	EXP_Values.tax_ssn_id,
	EXP_Values.claim_party_last_name,
	EXP_Values.claim_party_first_name,
	EXP_Values.claim_party_gndr,
	EXP_Values.claim_party_birthdate,
	EXP_Values.cms_incdnt_date,
	EXP_Values.claim_loss_date,
	EXP_Values.patient_cause_code,
	EXP_Values.state_venue,
	EXP_Values.patient_diag_code,
	EXP_Values.prdct_liab_ind,
	EXP_Values.prdct_generic_name,
	EXP_Values.prdct_brand_name,
	EXP_Values.prdct_mfr,
	EXP_Values.prdct_alleged_harm,
	EXP_Values.self_insd_ind,
	EXP_Values.self_insd_type,
	EXP_Values.self_insd_last_name,
	EXP_Values.self_insd_first_name,
	EXP_Values.self_insd_dba_name,
	EXP_Values.self_insd_lgl_name,
	EXP_Values.wbmi_plan_ins_type,
	EXP_Values.pol_key,
	EXP_Values.claim_occurrence_key,
	EXP_Values.no_fault_ins_lmt,
	EXP_Values.exhaust_lmt_date,
	EXP_Values.injured_party_rep_Ind AS injured_party_rep_Ind_MIJA,
	-- *INF*: IIF( IN ( source_sys_id, 'EXCEED' , 'DCT')  and injured_party_rep_is_individual ='N',1,0)
	-- 
	-- 
	-- 
	-- -- jira prod 2781. if exceed source and  injured part rep is not an individual and is not nothing then it is a firm name.
	IFF(source_sys_id IN ('EXCEED','DCT') and injured_party_rep_is_individual = 'N', 1, 0) AS v_apply_Exceed_Inj_party_firm_name_rules,
	LKP_Claim_Party_injured_party_rep.claim_party_last_name AS claim_party_last_name_MIJA,
	-- *INF*: IIF(v_apply_Exceed_Inj_party_firm_name_rules=1,'N/A',claim_party_last_name_MIJA)
	-- 
	-- -- jira prod 2781. blank out names if the last name is really the firm name.
	IFF(v_apply_Exceed_Inj_party_firm_name_rules = 1, 'N/A', claim_party_last_name_MIJA) AS claim_party_last_name_MIJA_OUT,
	LKP_Claim_Party_injured_party_rep.claim_party_first_name AS claim_party_first_name_MIJA,
	-- *INF*: IIF(v_apply_Exceed_Inj_party_firm_name_rules=1,'N/A', claim_party_first_name_MIJA)
	-- 
	-- -- jira prod 2781. blank out names if the last name is really the firm name.
	IFF(v_apply_Exceed_Inj_party_firm_name_rules = 1, 'N/A', claim_party_first_name_MIJA) AS claim_party_first_name_MIJA_OUT,
	LKP_Claim_Party_injured_party_rep.tax_id AS tax_id_MIJA,
	LKP_Claim_Party_injured_party_rep.claim_party_addr AS claim_party_addr_MIJA,
	LKP_Claim_Party_injured_party_rep.claim_party_city AS claim_party_city_MIJA,
	LKP_Claim_Party_injured_party_rep.claim_party_state AS claim_party_state_MIJA,
	LKP_Claim_Party_injured_party_rep.claim_party_zip AS claim_party_zip_MIJA,
	LKP_Claim_Party_injured_party_rep.claim_party_zip4 AS claim_party_zip4_MIJA,
	LKP_Claim_Party_injured_party_rep.ph_num AS ph_num_MIJA,
	LKP_Claim_Party_injured_party_rep.ph_extension AS ph_extension_MIJA,
	EXP_Values.med_obligation_to_claimant,
	EXP_Values.orm_termination_date,
	EXP_Values.tpoc_date,
	EXP_Values.tpoc_amt,
	EXP_Values.tpoc_fund_delay_date,
	EXP_Values.claimant_1_Ind AS claimant_1_Ind_MCT1,
	LKP_Claim_Party_claimant_1.tax_id AS tax_id_MCT1,
	LKP_Claim_Party_claimant_1.claim_party_last_name AS claim_party_last_name_MCT1,
	LKP_Claim_Party_claimant_1.claim_party_first_name AS claim_party_first_name_MCT1,
	LKP_Claim_Party_claimant_1.claim_party_mid_name AS claim_party_mid_name_MCT1,
	LKP_Claim_Party_claimant_1.claim_party_addr AS claim_party_addr_MCT1,
	LKP_Claim_Party_claimant_1.claim_party_city AS claim_party_city_MCT1,
	LKP_Claim_Party_claimant_1.claim_party_state AS claim_party_state_MCT1,
	LKP_Claim_Party_claimant_1.claim_party_zip AS claim_party_zip_MCT1,
	LKP_Claim_Party_claimant_1.claim_party_zip4 AS claim_party_zip4_MCT1,
	LKP_Claim_Party_claimant_1.ph_num AS ph_num_MCT1,
	LKP_Claim_Party_claimant_1.ph_extension AS ph_extension_MCT1,
	EXP_Values.claimant_1_rep_Ind AS claimant_1_rep_Ind_MCA1,
	-- *INF*: IIF( IN ( source_sys_id, 'EXCEED' , 'DCT') and claimant_1_rep_is_individual='N',1,0)
	-- 
	-- --Jira-prod-2905 expand the firm name rules to claimant rep1
	IFF(source_sys_id IN ('EXCEED','DCT') and claimant_1_rep_is_individual = 'N', 1, 0) AS v_apply_Exceed_claim_party_rep_firm_name_rules,
	LKP_Claim_Party_claimant_1_rep.claim_party_last_name AS claim_party_last_name_MCA1,
	-- *INF*: IIF(v_apply_Exceed_claim_party_rep_firm_name_rules=1,'N/A',claim_party_last_name_MCA1)
	IFF(v_apply_Exceed_claim_party_rep_firm_name_rules = 1, 'N/A', claim_party_last_name_MCA1) AS claim_party_last_name_MCA1_OUT,
	LKP_Claim_Party_claimant_1_rep.claim_party_first_name AS claim_party_first_name_MCA1,
	-- *INF*: IIF(v_apply_Exceed_claim_party_rep_firm_name_rules=1,'N/A',claim_party_first_name_MCA1)
	IFF(v_apply_Exceed_claim_party_rep_firm_name_rules = 1, 'N/A', claim_party_first_name_MCA1) AS claim_party_first_name_MCA1_OUT,
	LKP_Claim_Party_claimant_1_rep.tax_id AS tax_id_MCA1,
	LKP_Claim_Party_claimant_1_rep.claim_party_addr AS claim_party_addr_MCA1,
	LKP_Claim_Party_claimant_1_rep.claim_party_city AS claim_party_city_MCA1,
	LKP_Claim_Party_claimant_1_rep.claim_party_state AS claim_party_state_MCA1,
	LKP_Claim_Party_claimant_1_rep.claim_party_zip AS claim_party_zip_MCA1,
	LKP_Claim_Party_claimant_1_rep.claim_party_zip4 AS claim_party_zip4_MCA1,
	LKP_Claim_Party_claimant_1_rep.ph_num AS ph_num_MCA1,
	LKP_Claim_Party_claimant_1_rep.ph_extension AS ph_extension_MCA1,
	EXP_Values.injured_party_rep_firm,
	-- *INF*: IIF(
	-- v_apply_Exceed_Inj_party_firm_name_rules=1
	-- , claim_party_last_name_MIJA
	-- , injured_party_rep_firm)
	-- 
	-- -- jira prod 2781. override firm name with injured party rep last name in certain cases.
	IFF(
	    v_apply_Exceed_Inj_party_firm_name_rules = 1, claim_party_last_name_MIJA,
	    injured_party_rep_firm
	) AS injured_party_rep_firm_override_OUT,
	EXP_Values.claimant1_rep_firm,
	-- *INF*: IIF(
	-- v_apply_Exceed_claim_party_rep_firm_name_rules=1
	-- , claim_party_last_name_MCA1
	-- , claimant1_rep_firm)
	IFF(
	    v_apply_Exceed_claim_party_rep_firm_name_rules = 1, claim_party_last_name_MCA1,
	    claimant1_rep_firm
	) AS claimant1_rep_firm_override_OUT,
	EXP_Values.last_cms_hicn,
	EXP_Values.ICDCodeVersion AS i_ICDCodeVersion,
	-- *INF*: iif(LTRIM(RTRIM(i_ICDCodeVersion)) ='10','0','9' )
	IFF(LTRIM(RTRIM(i_ICDCodeVersion)) = '10', '0', '9') AS o_ICDCodeVersion,
	EXP_Values.source_sys_id,
	EXP_Values.injured_party_rep_is_individual,
	EXP_Values.claimant_1_rep_is_individual
	FROM EXP_Values
	LEFT JOIN LKP_Claim_Party_claimant_1
	ON LKP_Claim_Party_claimant_1.claim_party_ak_id = EXP_Values.claimant_1_ak_id
	LEFT JOIN LKP_Claim_Party_claimant_1_rep
	ON LKP_Claim_Party_claimant_1_rep.claim_party_ak_id = EXP_Values.claimant_1_rep_ak_id
	LEFT JOIN LKP_Claim_Party_injured_party_rep
	ON LKP_Claim_Party_injured_party_rep.claim_party_ak_id = EXP_Values.injured_party_rep_ak_id
),
JNR_Values AS (SELECT
	EXP_Values1.claim_med_ak_id, 
	EXP_Values1.claim_party_occurrence_ak_id, 
	EXP_Values1.claim_med_plan_ak_id, 
	EXP_Values1.claim_occurrence_ak_id, 
	EXP_Values1.claim_party_ak_id, 
	EXP_Diag_Code.claim_med_patient_diag_add_ak_id, 
	EXP_Diag_Code.claim_med_ak_id AS claim_med_ak_id1, 
	EXP_Values1.record_identifier, 
	EXP_Values1.cms_document_cntl_num, 
	EXP_Values1.action_type, 
	EXP_Values1.medicare_hicn, 
	EXP_Values1.tax_ssn_id, 
	EXP_Values1.claim_party_last_name, 
	EXP_Values1.claim_party_first_name, 
	EXP_Values1.claim_party_gndr, 
	EXP_Values1.claim_party_birthdate, 
	EXP_Values1.cms_incdnt_date, 
	EXP_Values1.claim_loss_date, 
	EXP_Values1.patient_cause_code, 
	EXP_Values1.state_venue, 
	EXP_Values1.patient_diag_code, 
	EXP_Diag_Code.patient_diag_code2, 
	EXP_Diag_Code.patient_diag_code3, 
	EXP_Diag_Code.patient_diag_code4, 
	EXP_Diag_Code.patient_diag_code5, 
	EXP_Diag_Code.patient_diag_code6, 
	EXP_Diag_Code.patient_diag_code7, 
	EXP_Diag_Code.patient_diag_code8, 
	EXP_Diag_Code.patient_diag_code9, 
	EXP_Diag_Code.patient_diag_code10, 
	EXP_Diag_Code.patient_diag_code11, 
	EXP_Diag_Code.patient_diag_code12, 
	EXP_Diag_Code.patient_diag_code13, 
	EXP_Diag_Code.patient_diag_code14, 
	EXP_Diag_Code.patient_diag_code15, 
	EXP_Diag_Code.patient_diag_code16, 
	EXP_Diag_Code.patient_diag_code17, 
	EXP_Diag_Code.patient_diag_code18, 
	EXP_Diag_Code.patient_diag_code19, 
	EXP_Values1.prdct_liab_ind, 
	EXP_Values1.prdct_generic_name, 
	EXP_Values1.prdct_brand_name, 
	EXP_Values1.prdct_mfr, 
	EXP_Values1.prdct_alleged_harm, 
	EXP_Values1.self_insd_ind, 
	EXP_Values1.self_insd_type, 
	EXP_Values1.self_insd_last_name, 
	EXP_Values1.self_insd_first_name, 
	EXP_Values1.self_insd_dba_name, 
	EXP_Values1.self_insd_lgl_name, 
	EXP_Values1.wbmi_plan_ins_type, 
	EXP_Values1.pol_key, 
	EXP_Values1.claim_occurrence_key, 
	EXP_Values1.no_fault_ins_lmt, 
	EXP_Values1.exhaust_lmt_date, 
	EXP_Values1.injured_party_rep_Ind_MIJA, 
	EXP_Values1.claim_party_last_name_MIJA_OUT AS claim_party_last_name_MIJA, 
	EXP_Values1.claim_party_first_name_MIJA_OUT AS claim_party_first_name_MIJA, 
	EXP_Values1.tax_id_MIJA, 
	EXP_Values1.claim_party_addr_MIJA, 
	EXP_Values1.claim_party_city_MIJA, 
	EXP_Values1.claim_party_state_MIJA, 
	EXP_Values1.claim_party_zip_MIJA, 
	EXP_Values1.claim_party_zip4_MIJA, 
	EXP_Values1.ph_num_MIJA, 
	EXP_Values1.ph_extension_MIJA, 
	EXP_Values1.med_obligation_to_claimant, 
	EXP_Values1.orm_termination_date, 
	EXP_Values1.tpoc_date, 
	EXP_Values1.tpoc_amt, 
	EXP_Values1.tpoc_fund_delay_date, 
	EXP_Values1.claimant_1_Ind_MCT1, 
	EXP_Values1.tax_id_MCT1, 
	EXP_Values1.claim_party_last_name_MCT1, 
	EXP_Values1.claim_party_first_name_MCT1, 
	EXP_Values1.claim_party_mid_name_MCT1, 
	EXP_Values1.claim_party_addr_MCT1, 
	EXP_Values1.claim_party_city_MCT1, 
	EXP_Values1.claim_party_state_MCT1, 
	EXP_Values1.claim_party_zip_MCT1, 
	EXP_Values1.claim_party_zip4_MCT1, 
	EXP_Values1.ph_num_MCT1, 
	EXP_Values1.ph_extension_MCT1, 
	EXP_Values1.claimant_1_rep_Ind_MCA1, 
	EXP_Values1.claim_party_last_name_MCA1_OUT AS claim_party_last_name_MCA1, 
	EXP_Values1.claim_party_first_name_MCA1_OUT AS claim_party_first_name_MCA1, 
	EXP_Values1.tax_id_MCA1, 
	EXP_Values1.claim_party_addr_MCA1, 
	EXP_Values1.claim_party_city_MCA1, 
	EXP_Values1.claim_party_state_MCA1, 
	EXP_Values1.claim_party_zip_MCA1, 
	EXP_Values1.claim_party_zip4_MCA1, 
	EXP_Values1.ph_num_MCA1, 
	EXP_Values1.ph_extension_MCA1, 
	EXP_Values1.injured_party_rep_firm_override_OUT AS injured_party_rep_firm, 
	EXP_Values1.claimant1_rep_firm_override_OUT AS claimant1_rep_firm, 
	EXP_Values1.last_cms_hicn, 
	EXP_Values1.o_ICDCodeVersion AS ICDCodeVersion
	FROM EXP_Values1
	LEFT OUTER JOIN EXP_Diag_Code
	ON EXP_Diag_Code.claim_med_ak_id = EXP_Values1.claim_med_ak_id
),
RTR_Add_Update_Delete AS (
	SELECT
	claim_med_ak_id,
	claim_party_occurrence_ak_id,
	claim_med_plan_ak_id,
	claim_occurrence_ak_id,
	claim_party_ak_id,
	claim_med_patient_diag_add_ak_id,
	claim_med_ak_id1 AS claim_med_ak_id5,
	record_identifier,
	cms_document_cntl_num,
	action_type,
	medicare_hicn,
	tax_ssn_id,
	claim_party_last_name,
	claim_party_first_name,
	claim_party_gndr,
	claim_party_birthdate,
	cms_incdnt_date,
	claim_loss_date,
	patient_cause_code,
	state_venue,
	patient_diag_code,
	patient_diag_code2,
	patient_diag_code3,
	patient_diag_code4,
	patient_diag_code5,
	patient_diag_code6,
	patient_diag_code7,
	patient_diag_code8,
	patient_diag_code9,
	patient_diag_code10,
	patient_diag_code11,
	patient_diag_code12,
	patient_diag_code13,
	patient_diag_code14,
	patient_diag_code15,
	patient_diag_code16,
	patient_diag_code17,
	patient_diag_code18,
	patient_diag_code19,
	prdct_liab_ind,
	prdct_generic_name,
	prdct_brand_name,
	prdct_mfr,
	prdct_alleged_harm,
	self_insd_ind,
	self_insd_type,
	self_insd_last_name,
	self_insd_first_name,
	self_insd_dba_name,
	self_insd_lgl_name,
	wbmi_plan_ins_type,
	pol_key,
	claim_occurrence_key,
	no_fault_ins_lmt,
	exhaust_lmt_date,
	injured_party_rep_Ind_MIJA,
	claim_party_last_name_MIJA,
	claim_party_first_name_MIJA,
	tax_id_MIJA,
	claim_party_addr_MIJA,
	claim_party_city_MIJA,
	claim_party_state_MIJA,
	claim_party_zip_MIJA,
	claim_party_zip4_MIJA,
	ph_num_MIJA,
	ph_extension_MIJA,
	med_obligation_to_claimant,
	orm_termination_date,
	tpoc_date,
	tpoc_amt,
	tpoc_fund_delay_date,
	claimant_1_Ind_MCT1,
	tax_id_MCT1,
	claim_party_last_name_MCT1,
	claim_party_first_name_MCT1,
	claim_party_mid_name_MCT1,
	claim_party_addr_MCT1,
	claim_party_city_MCT1,
	claim_party_state_MCT1,
	claim_party_zip_MCT1,
	claim_party_zip4_MCT1,
	ph_num_MCT1,
	ph_extension_MCT1,
	claimant_1_rep_Ind_MCA1,
	claim_party_last_name_MCA1,
	claim_party_first_name_MCA1,
	tax_id_MCA1,
	claim_party_addr_MCA1,
	claim_party_city_MCA1,
	claim_party_state_MCA1,
	claim_party_zip_MCA1,
	claim_party_zip4_MCA1,
	ph_num_MCA1,
	ph_extension_MCA1,
	injured_party_rep_firm,
	claimant1_rep_firm,
	last_cms_hicn,
	ICDCodeVersion
	FROM JNR_Values
),
RTR_Add_Update_Delete_Action_Type_ADD_UPDATE AS (SELECT * FROM RTR_Add_Update_Delete WHERE action_type = '0' OR action_type = '3'

--0 means ADD
--1 means DELETE
--2 means UPDATE 
--3 means ADDDELETE so we need to send one Add row and one DELETE row),
RTR_Add_Update_Delete_Action_Type_DELETE AS (SELECT * FROM RTR_Add_Update_Delete WHERE action_type = '1' OR action_type = '3' 

--0 means ADD
--1 means DELETE
--2 means UPDATE 
--3 means ADDDELETE so we need to send one Add row and one DELETE row),
RTR_Add_Update_Delete_Action_Type_UPDATE AS (SELECT * FROM RTR_Add_Update_Delete WHERE action_type='2'),
EXP_ActionType_ADD AS (
	SELECT
	claim_med_ak_id,
	claim_party_occurrence_ak_id,
	claim_med_plan_ak_id,
	claim_occurrence_ak_id,
	claim_party_ak_id,
	claim_med_patient_diag_add_ak_id,
	claim_med_ak_id5 AS claim_med_ak_id1,
	record_identifier,
	cms_document_cntl_num,
	action_type,
	medicare_hicn,
	tax_ssn_id,
	claim_party_last_name,
	claim_party_first_name,
	claim_party_gndr,
	claim_party_birthdate,
	cms_incdnt_date,
	claim_loss_date,
	patient_cause_code,
	state_venue,
	patient_diag_code,
	patient_diag_code2,
	patient_diag_code3,
	patient_diag_code4,
	patient_diag_code5,
	patient_diag_code6,
	patient_diag_code7,
	patient_diag_code8,
	patient_diag_code9,
	patient_diag_code10,
	patient_diag_code AS patient_diag_code11,
	patient_diag_code12,
	patient_diag_code13,
	patient_diag_code14,
	patient_diag_code15,
	patient_diag_code16,
	patient_diag_code17,
	patient_diag_code18,
	patient_diag_code19,
	prdct_liab_ind,
	prdct_generic_name,
	prdct_brand_name,
	prdct_mfr,
	prdct_alleged_harm,
	self_insd_ind,
	self_insd_type,
	self_insd_last_name,
	self_insd_first_name,
	self_insd_dba_name,
	self_insd_lgl_name,
	wbmi_plan_ins_type,
	pol_key,
	claim_occurrence_key,
	no_fault_ins_lmt,
	exhaust_lmt_date,
	injured_party_rep_Ind_MIJA,
	claim_party_last_name_MIJA,
	claim_party_first_name_MIJA,
	tax_id_MIJA,
	claim_party_addr_MIJA,
	claim_party_city_MIJA,
	claim_party_state_MIJA,
	claim_party_zip_MIJA,
	claim_party_zip4_MIJA,
	ph_num_MIJA,
	ph_extension_MIJA,
	med_obligation_to_claimant,
	orm_termination_date,
	tpoc_date,
	tpoc_amt,
	tpoc_fund_delay_date,
	claimant_1_Ind_MCT AS claimant_1_Ind_MCT1,
	tax_id_MCT AS tax_id_MCT1,
	claim_party_last_name_MCT AS claim_party_last_name_MCT1,
	claim_party_first_name_MCT AS claim_party_first_name_MCT1,
	claim_party_mid_name_MCT AS claim_party_mid_name_MCT1,
	claim_party_addr_MCT AS claim_party_addr_MCT1,
	claim_party_city_MCT AS claim_party_city_MCT1,
	claim_party_state_MCT AS claim_party_state_MCT1,
	claim_party_zip_MCT AS claim_party_zip_MCT1,
	claim_party_zip4_MCT AS claim_party_zip4_MCT1,
	ph_num_MCT AS ph_num_MCT1,
	ph_extension_MCT AS ph_extension_MCT1,
	claimant_1_rep_Ind_MCA AS claimant_1_rep_Ind_MCA1,
	claim_party_last_name_MCA AS claim_party_last_name_MCA1,
	claim_party_first_name_MCA AS claim_party_first_name_MCA1,
	tax_id_MCA AS tax_id_MCA1,
	claim_party_addr_MCA AS claim_party_addr_MCA1,
	claim_party_city_MCA AS claim_party_city_MCA1,
	claim_party_state_MCA AS claim_party_state_MCA1,
	claim_party_zip_MCA AS claim_party_zip_MCA1,
	claim_party_zip4_MCA AS claim_party_zip4_MCA1,
	ph_num_MCA AS ph_num_MCA1,
	ph_extension_MCA AS ph_extension_MCA1,
	injured_party_rep_firm AS injured_party_rep_firm_MIJA,
	claimant1_rep_firm AS claimant1_rep_firm_MCA1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(record_identifier)
	-- 
	-- 
	UDF_DEFAULT_VALUE_TO_BLANKS(record_identifier) AS record_identifier1,
	cms_document_cntl_num AS cms_document_cntl_num1,
	-- *INF*: IIF(ISNULL(action_type),'',
	-- IIF(action_type='3','0',action_type))
	-- 
	-- --action type 3 means ADDDELETE so we need to convert it to 0 (means ADD) as CMS doesn't allow value of 3
	IFF(action_type IS NULL, '', IFF(
	        action_type = '3', '0', action_type
	    )) AS action_type1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(medicare_hicn)
	UDF_DEFAULT_VALUE_TO_BLANKS(medicare_hicn) AS medicare_hicn1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(tax_ssn_id)
	UDF_DEFAULT_VALUE_TO_BLANKS(tax_ssn_id) AS tax_ssn_id1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_last_name)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_last_name) AS claim_party_last_name1,
	-- *INF*: :UDF.REPLACE_NON_ALPHA_WITH_BLANKS(:UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_first_name))
	-- 
	UDF_REPLACE_NON_ALPHA_WITH_BLANKS(UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_first_name)) AS claim_party_first_name1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_gndr)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_gndr) AS claim_party_gndr1,
	-- *INF*: IIF(claim_party_birthdate='99991231' OR claim_party_birthdate='21001231' ,'00000000',claim_party_birthdate)
	-- 
	IFF(
	    claim_party_birthdate = '99991231' OR claim_party_birthdate = '21001231', '00000000',
	    claim_party_birthdate
	) AS claim_party_birthdate1,
	-- *INF*: IIF(cms_incdnt_date='18000101','00000000',cms_incdnt_date)
	IFF(cms_incdnt_date = '18000101', '00000000', cms_incdnt_date) AS cms_incdnt_date1,
	-- *INF*: IIF(claim_loss_date='18000101','00000000',claim_loss_date)
	IFF(claim_loss_date = '18000101', '00000000', claim_loss_date) AS claim_loss_date1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_cause_code)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_cause_code) AS patient_cause_code1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(state_venue)
	UDF_DEFAULT_VALUE_TO_BLANKS(state_venue) AS state_venue1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code) AS patient_diag_code1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code2)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code2) AS patient_diag_code_21,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code3)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code3) AS patient_diag_code_31,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code4)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code4) AS patient_diag_code_41,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code5)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code5) AS patient_diag_code_51,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code6)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code6) AS patient_diag_code_61,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code7)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code7) AS patient_diag_code_71,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code8)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code8) AS patient_diag_code_81,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code9)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code9) AS patient_diag_code_91,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code10)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code10) AS patient_diag_code_101,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code11)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code11) AS patient_diag_code_111,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code12)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code12) AS patient_diag_code_121,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code13)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code13) AS patient_diag_code_131,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code14)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code14) AS patient_diag_code_141,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code15)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code15) AS patient_diag_code_151,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code16)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code16) AS patient_diag_code_161,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code17)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code17) AS patient_diag_code_171,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code18)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code18) AS patient_diag_code_181,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code19)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code19) AS patient_diag_code_191,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(prdct_liab_ind)
	UDF_DEFAULT_VALUE_TO_BLANKS(prdct_liab_ind) AS prdct_liab_ind1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(prdct_generic_name)
	UDF_DEFAULT_VALUE_TO_BLANKS(prdct_generic_name) AS prdct_generic_name1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(prdct_brand_name)
	UDF_DEFAULT_VALUE_TO_BLANKS(prdct_brand_name) AS prdct_brand_name1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(prdct_mfr)
	UDF_DEFAULT_VALUE_TO_BLANKS(prdct_mfr) AS prdct_mfr1,
	-- *INF*: REPLACECHR(1,REPLACECHR(1,prdct_alleged_harm,chr(10),'') ,chr(13),'') 
	-- 
	-- --this to remove new line characters
	REGEXP_REPLACE(REGEXP_REPLACE(prdct_alleged_harm,chr(10),''),chr(13),'') AS v_prdct_alleged_harm1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(v_prdct_alleged_harm1)
	UDF_DEFAULT_VALUE_TO_BLANKS(v_prdct_alleged_harm1) AS prdct_alleged_harm1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(self_insd_ind)
	UDF_DEFAULT_VALUE_TO_BLANKS(self_insd_ind) AS self_insd_ind1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(self_insd_type)
	UDF_DEFAULT_VALUE_TO_BLANKS(self_insd_type) AS self_insd_type1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(self_insd_last_name)
	UDF_DEFAULT_VALUE_TO_BLANKS(self_insd_last_name) AS self_insd_last_name1,
	-- *INF*: :UDF.REPLACE_NON_ALPHA_WITH_BLANKS(:UDF.DEFAULT_VALUE_TO_BLANKS(self_insd_first_name))
	UDF_REPLACE_NON_ALPHA_WITH_BLANKS(UDF_DEFAULT_VALUE_TO_BLANKS(self_insd_first_name)) AS self_insd_first_name1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(self_insd_dba_name)
	UDF_DEFAULT_VALUE_TO_BLANKS(self_insd_dba_name) AS self_insd_dba_name1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(self_insd_lgl_name)
	UDF_DEFAULT_VALUE_TO_BLANKS(self_insd_lgl_name) AS self_insd_lgl_name1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(wbmi_plan_ins_type)
	UDF_DEFAULT_VALUE_TO_BLANKS(wbmi_plan_ins_type) AS wbmi_plan_ins_type1,
	pol_key AS pol_key1,
	claim_occurrence_key AS claim_occurrence_key1,
	-- *INF*: IIF(wbmi_plan_ins_type='E' OR wbmi_plan_ins_type='L', '00000000000',
	-- IIF(no_fault_ins_lmt=0,'99999999999',
	-- 	lpad(TO_CHAR(ROUND(no_fault_ins_lmt,2)*100),11,'0')))
	-- 
	-- 
	-- 
	IFF(
	    wbmi_plan_ins_type = 'E' OR wbmi_plan_ins_type = 'L', '00000000000',
	    IFF(
	        no_fault_ins_lmt = 0, '99999999999',
	        lpad(TO_CHAR(ROUND(no_fault_ins_lmt, 2) * 100), 11, '0')
	    )
	) AS no_fault_ins_lmt1,
	-- *INF*: IIF(wbmi_plan_ins_type='E' OR wbmi_plan_ins_type='L', '00000000',
	-- IIF(exhaust_lmt_date='18000101','00000000',
	-- 	exhaust_lmt_date))
	IFF(
	    wbmi_plan_ins_type = 'E' OR wbmi_plan_ins_type = 'L', '00000000',
	    IFF(
	        exhaust_lmt_date = '18000101', '00000000', exhaust_lmt_date
	    )
	) AS exhaust_lmt_date1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(injured_party_rep_Ind_MIJA)
	UDF_DEFAULT_VALUE_TO_BLANKS(injured_party_rep_Ind_MIJA) AS injured_party_rep_Ind_MIJA1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_last_name_MIJA)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_last_name_MIJA) AS claim_party_last_name_MIJA1,
	-- *INF*: :UDF.REPLACE_NON_ALPHA_WITH_BLANKS(:UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_first_name_MIJA))
	UDF_REPLACE_NON_ALPHA_WITH_BLANKS(UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_first_name_MIJA)) AS claim_party_first_name_MIJA1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(injured_party_rep_firm_MIJA)
	UDF_DEFAULT_VALUE_TO_BLANKS(injured_party_rep_firm_MIJA) AS injured_party_rep_firm_MIJA1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(tax_id_MIJA)
	-- 
	-- 
	-- 
	UDF_DEFAULT_VALUE_TO_BLANKS(tax_id_MIJA) AS tax_id_MIJA1,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MIJA)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_addr_MIJA))
	-- 
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MIJA) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_addr_MIJA)
	) AS claim_party_addr_MIJA1,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MIJA)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(substr(claim_party_addr_MIJA ,51)))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MIJA) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(substr(claim_party_addr_MIJA, 51))
	) AS claim_party_addr_Ln2_MIJA1,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MIJA)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_city_MIJA))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MIJA) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_city_MIJA)
	) AS claim_party_city_MIJA1,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MIJA)='FC','FC',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_state_MIJA))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MIJA) = 'FC', 'FC',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_state_MIJA)
	) AS claim_party_state_MIJA1,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MIJA)='FC','0000000000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(claim_party_zip_MIJA))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MIJA) = 'FC', '0000000000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(claim_party_zip_MIJA)
	) AS claim_party_zip_MIJA1,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MIJA)='FC','0000000000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(claim_party_zip4_MIJA))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MIJA) = 'FC', '0000000000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(claim_party_zip4_MIJA)
	) AS claim_party_zip4_MIJA1,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MIJA)='FC','0000000000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(ph_num_MIJA))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MIJA) = 'FC', '0000000000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(ph_num_MIJA)
	) AS ph_num_MIJA1,
	-- *INF*: IIF(:UDF.DEFAULT_VALUE_TO_BLANKS(injured_party_rep_Ind_MIJA)='','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(ph_extension_MIJA))
	IFF(
	    UDF_DEFAULT_VALUE_TO_BLANKS(injured_party_rep_Ind_MIJA) = '', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(ph_extension_MIJA)
	) AS ph_extension_MIJA1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(med_obligation_to_claimant)
	UDF_DEFAULT_VALUE_TO_BLANKS(med_obligation_to_claimant) AS med_obligation_to_claimant1,
	-- *INF*: concat(CONCAT(GET_DATE_PART(ADD_TO_DATE(SYSDATE,'MM',6),'YYYY'), 
	-- LPAD(GET_DATE_PART(ADD_TO_DATE(SYSDATE,'MM',6),'MM'),2,'0')
	-- ),'08')
	concat(CONCAT(DATE_PART(DATEADD(MONTH,6,CURRENT_TIMESTAMP), 'YYYY'), LPAD(DATE_PART(DATEADD(MONTH,6,CURRENT_TIMESTAMP), 'MM'), 2, '0')), '08') AS var_orm_checkdate,
	-- *INF*: IIF(orm_termination_date='18000101','00000000',IIF( TO_DATE( orm_termination_date,'YYYYMMDD') > TO_DATE(var_orm_checkdate,'YYYYMMDD'),'00000000',orm_termination_date))
	IFF(
	    orm_termination_date = '18000101', '00000000',
	    IFF(
	        TO_TIMESTAMP(orm_termination_date, 'YYYYMMDD') > TO_TIMESTAMP(var_orm_checkdate, 'YYYYMMDD'),
	        '00000000',
	        orm_termination_date
	    )
	) AS orm_termination_date1,
	-- *INF*: IIF(ISNULL(tpoc_date) OR tpoc_date='18000101','00000000',tpoc_date)
	IFF(tpoc_date IS NULL OR tpoc_date = '18000101', '00000000', tpoc_date) AS tpoc_date1,
	-- *INF*: IIF(ISNULL(tpoc_amt) OR tpoc_amt=0,'00000000000',lpad(TO_CHAR(ROUND(tpoc_amt,2)*100),11,'0'))
	-- 
	-- 
	-- 
	-- 
	IFF(
	    tpoc_amt IS NULL OR tpoc_amt = 0, '00000000000',
	    lpad(TO_CHAR(ROUND(tpoc_amt, 2) * 100), 11, '0')
	) AS tpoc_amt1,
	-- *INF*: IIF(ISNULL(tpoc_fund_delay_date) OR tpoc_fund_delay_date='18000101','00000000',tpoc_fund_delay_date)
	IFF(
	    tpoc_fund_delay_date IS NULL OR tpoc_fund_delay_date = '18000101', '00000000',
	    tpoc_fund_delay_date
	) AS tpoc_fund_delay_date1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claimant_1_Ind_MCT1)
	UDF_DEFAULT_VALUE_TO_BLANKS(claimant_1_Ind_MCT1) AS claimant_1_Ind_MCT11,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(tax_id_MCT1)
	UDF_DEFAULT_VALUE_TO_BLANKS(tax_id_MCT1) AS tax_id_MCT11,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_last_name_MCT1)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_last_name_MCT1) AS claim_party_last_name_MCT11,
	-- *INF*: :UDF.REPLACE_NON_ALPHA_WITH_BLANKS(:UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_first_name_MCT1))
	UDF_REPLACE_NON_ALPHA_WITH_BLANKS(UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_first_name_MCT1)) AS claim_party_first_name_MCT11,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_mid_name_MCT1)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_mid_name_MCT1) AS claim_party_mid_name_MCT11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT1)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_addr_MCT1))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT1) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_addr_MCT1)
	) AS claim_party_addr_MCT11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT1)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(substr(claim_party_addr_MCT1,51)))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT1) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(substr(claim_party_addr_MCT1, 51))
	) AS claim_party_addr_Ln2_MCT11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT1)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_city_MCT1))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT1) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_city_MCT1)
	) AS claim_party_city_MCT11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT1)='FC','FC',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_state_MCT1))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT1) = 'FC', 'FC',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_state_MCT1)
	) AS claim_party_state_MCT11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT1)='FC','00000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(claim_party_zip_MCT1))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT1) = 'FC', '00000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(claim_party_zip_MCT1)
	) AS claim_party_zip_MCT11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT1)='FC','0000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(claim_party_zip4_MCT1))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT1) = 'FC', '0000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(claim_party_zip4_MCT1)
	) AS claim_party_zip4_MCT11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT1)='FC','0000000000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(ph_num_MCT1))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT1) = 'FC', '0000000000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(ph_num_MCT1)
	) AS ph_num_MCT11,
	-- *INF*: IIF(:UDF.DEFAULT_VALUE_TO_BLANKS(claimant_1_Ind_MCT1)='','',:UDF.DEFAULT_VALUE_TO_BLANKS(ph_extension_MCT1))
	IFF(
	    UDF_DEFAULT_VALUE_TO_BLANKS(claimant_1_Ind_MCT1) = '', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(ph_extension_MCT1)
	) AS ph_extension_MCT11,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claimant_1_rep_Ind_MCA1)
	UDF_DEFAULT_VALUE_TO_BLANKS(claimant_1_rep_Ind_MCA1) AS claimant_1_rep_Ind_MCA11,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_last_name_MCA1)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_last_name_MCA1) AS claim_party_last_name_MCA11,
	-- *INF*: :UDF.REPLACE_NON_ALPHA_WITH_BLANKS(:UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_first_name_MCA1))
	UDF_REPLACE_NON_ALPHA_WITH_BLANKS(UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_first_name_MCA1)) AS claim_party_first_name_MCA11,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claimant1_rep_firm_MCA1)
	UDF_DEFAULT_VALUE_TO_BLANKS(claimant1_rep_firm_MCA1) AS claimant1_rep_firm_MCA11,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(tax_id_MCA1)
	UDF_DEFAULT_VALUE_TO_BLANKS(tax_id_MCA1) AS tax_id_MCA11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA1)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_addr_MCA1))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA1) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_addr_MCA1)
	) AS claim_party_addr_MCA11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA1)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(substr(claim_party_addr_MCA1,51)))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA1) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(substr(claim_party_addr_MCA1, 51))
	) AS claim_party_addr_Ln2_MCA11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA1)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_city_MCA1))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA1) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_city_MCA1)
	) AS claim_party_city_MCA11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA1)='FC','FC',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_state_MCA1))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA1) = 'FC', 'FC',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_state_MCA1)
	) AS claim_party_state_MCA11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA1)='FC','00000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(claim_party_zip_MCA1))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA1) = 'FC', '00000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(claim_party_zip_MCA1)
	) AS claim_party_zip_MCA11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA1)='FC','0000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(claim_party_zip4_MCA1))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA1) = 'FC', '0000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(claim_party_zip4_MCA1)
	) AS claim_party_zip4_MCA11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA1)='FC','0000000000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(ph_num_MCA1))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA1) = 'FC', '0000000000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(ph_num_MCA1)
	) AS ph_num_MCA11,
	-- *INF*: IIF(:UDF.DEFAULT_VALUE_TO_BLANKS(claimant_1_rep_Ind_MCA1)='','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(ph_extension_MCA1))
	IFF(
	    UDF_DEFAULT_VALUE_TO_BLANKS(claimant_1_rep_Ind_MCA1) = '', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(ph_extension_MCA1)
	) AS ph_extension_MCA11,
	'' AS DEFAULT_BLANKS,
	1 AS DEFAULT_INTEGER_1,
	'0000000000' AS DEFAULT_ZEROS,
	SYSDATE AS CURRENT_DATE,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID,
	last_cms_hicn AS last_cms_hicn1,
	ICDCodeVersion
	FROM RTR_Add_Update_Delete_Action_Type_ADD_UPDATE
),
LKP_sup_cms_tin_office_REUSABLE AS (
	SELECT
	dummy_integer,
	cms_rre_id,
	office_tin_num,
	office_code,
	office_name,
	office_mail_address1,
	office_mail_address2,
	office_mail_city,
	office_mail_state,
	office_mail_zip,
	office_mail_zip4
	FROM (
		SELECT 
		sup_cms_tin_office.cms_rre_id as cms_rre_id, sup_cms_tin_office.office_tin_num as office_tin_num, sup_cms_tin_office.office_code as office_code, sup_cms_tin_office.office_name as office_name, sup_cms_tin_office.office_mail_address1 as office_mail_address1, sup_cms_tin_office.office_mail_address2 as office_mail_address2, sup_cms_tin_office.office_mail_city as office_mail_city, sup_cms_tin_office.office_mail_state as office_mail_state, sup_cms_tin_office.office_mail_zip as office_mail_zip, sup_cms_tin_office.office_mail_zip4 as office_mail_zip4, 
		1 as dummy_integer 
		FROM sup_cms_tin_office
		WHERE crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY dummy_integer ORDER BY dummy_integer) = 1
),
work_claim_cms_detail_extract_ADD AS (
	INSERT INTO work_claim_cms_detail_extract
	(claim_med_ak_id, claim_party_occurrence_ak_id, claim_med_plan_ak_id, claim_occurrence_ak_id, claim_party_ak_id, claim_med_patient_diag_add_ak_id, record_identifier, dcn, action_type, injured_party_hicn, injured_party_ssn, injured_party_last_name, injured_party_first_name, injured_party_middle_ini, injured_party_gender, injured_party_dob, reserved_11, cms_date_of_incident, industry_date_of_incident, reserved_14, alleged_cause_of_injury, reserved_16, state_of_venue, icd_indicator, diag_code_1, reserved_20, diag_code_2, reserved_22, diag_code_3, reserved_24, diag_code_4, reserved_26, diag_code_5, reserved_28, diag_code_6, reserved_30, diag_code_7, reserved_32, diag_code_8, reserved_34, diag_code_9, reserved_36, diag_code_10, reserved_38, diag_code_11, reserved_40, diag_code_12, reserved_42, diag_code_13, reserved_44, diag_code_14, reserved_46, diag_code_15, reserved_48, diag_code_16, reserved_50, diag_code_17, reserved_52, diag_code_18, reserved_54, diag_code_19, reserved_56, descript_of_illness, product_liability_ind, product_generic_name, product_brand_name, product_manufacturer, product_alleged_harm, reserved_63, self_insured_ind, self_insured_type, policyholder_last_name, policyholder_first_name, dba_name, legal_name, reserved_70, plan_insurance_type, tin, office_code, policy_number, claim_number, plan_contact_department_name, plan_contact_last_name, plan_contact_first_name, plan_contact_ph, plan_contact_ph_extension, no_fault_insurance_limit, exhaust_date_for_dollar_limit, reserved_83, inj_party_rep_ind, rep_last_name, rep_first_name, rep_firm_name, rep_tin, rep_mail_addr_1, rep_mail_addr_2, rep_city, rep_state, rep_mail_zip_code, rep_mail_zip4, rep_ph, rep_ph_extension, reserved_97, orm_ind, orm_termination_date, tpoc_date, tpoc_amount, funding_delayed_beyond_tpoc_start_date, reserved_103, c1_relationship, c1_tin, c1_last_name, c1_first_name, c1_middle_initial, c1_mail_addr_1, c1_mail_addr_2, c1_city, c1_state, c1_zip, c1_zip4, c1_ph, c1_ph_extension, reserved_117, c1_rep_ind, c1_rep_last_name, c1_rep_first_name, c1_rep_firm_name, c1_rep_tin, c1_rep_mail_addr_1, c1_rep_mail_addr_2, c1_rep_mail_city, c1_rep_state, c1_rep_zip, c1_rep_zip4, c1_rep_ph, c1_rep_ph_extension, reserved_131, created_date, modified_date, audit_id)
	SELECT 
	EXP_ActionType_ADD.CLAIM_MED_AK_ID, 
	EXP_ActionType_ADD.CLAIM_PARTY_OCCURRENCE_AK_ID, 
	EXP_ActionType_ADD.CLAIM_MED_PLAN_AK_ID, 
	EXP_ActionType_ADD.CLAIM_OCCURRENCE_AK_ID, 
	EXP_ActionType_ADD.CLAIM_PARTY_AK_ID, 
	EXP_ActionType_ADD.CLAIM_MED_PATIENT_DIAG_ADD_AK_ID, 
	EXP_ActionType_ADD.record_identifier1 AS RECORD_IDENTIFIER, 
	EXP_ActionType_ADD.cms_document_cntl_num1 AS DCN, 
	EXP_ActionType_ADD.action_type1 AS ACTION_TYPE, 
	EXP_ActionType_ADD.medicare_hicn1 AS INJURED_PARTY_HICN, 
	EXP_ActionType_ADD.tax_ssn_id1 AS INJURED_PARTY_SSN, 
	EXP_ActionType_ADD.claim_party_last_name1 AS INJURED_PARTY_LAST_NAME, 
	EXP_ActionType_ADD.claim_party_first_name1 AS INJURED_PARTY_FIRST_NAME, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS INJURED_PARTY_MIDDLE_INI, 
	EXP_ActionType_ADD.claim_party_gndr1 AS INJURED_PARTY_GENDER, 
	EXP_ActionType_ADD.claim_party_birthdate1 AS INJURED_PARTY_DOB, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_11, 
	EXP_ActionType_ADD.cms_incdnt_date1 AS CMS_DATE_OF_INCIDENT, 
	EXP_ActionType_ADD.claim_loss_date1 AS INDUSTRY_DATE_OF_INCIDENT, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_14, 
	EXP_ActionType_ADD.patient_cause_code1 AS ALLEGED_CAUSE_OF_INJURY, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_16, 
	EXP_ActionType_ADD.state_venue1 AS STATE_OF_VENUE, 
	EXP_ActionType_ADD.ICDCodeVersion AS ICD_INDICATOR, 
	EXP_ActionType_ADD.patient_diag_code1 AS DIAG_CODE_1, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_20, 
	EXP_ActionType_ADD.patient_diag_code_21 AS DIAG_CODE_2, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_22, 
	EXP_ActionType_ADD.patient_diag_code_31 AS DIAG_CODE_3, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_24, 
	EXP_ActionType_ADD.patient_diag_code_41 AS DIAG_CODE_4, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_26, 
	EXP_ActionType_ADD.patient_diag_code_51 AS DIAG_CODE_5, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_28, 
	EXP_ActionType_ADD.patient_diag_code_61 AS DIAG_CODE_6, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_30, 
	EXP_ActionType_ADD.patient_diag_code_71 AS DIAG_CODE_7, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_32, 
	EXP_ActionType_ADD.patient_diag_code_81 AS DIAG_CODE_8, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_34, 
	EXP_ActionType_ADD.patient_diag_code_91 AS DIAG_CODE_9, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_36, 
	EXP_ActionType_ADD.patient_diag_code_101 AS DIAG_CODE_10, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_38, 
	EXP_ActionType_ADD.patient_diag_code_111 AS DIAG_CODE_11, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_40, 
	EXP_ActionType_ADD.patient_diag_code_121 AS DIAG_CODE_12, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_42, 
	EXP_ActionType_ADD.patient_diag_code_131 AS DIAG_CODE_13, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_44, 
	EXP_ActionType_ADD.patient_diag_code_141 AS DIAG_CODE_14, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_46, 
	EXP_ActionType_ADD.patient_diag_code_151 AS DIAG_CODE_15, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_48, 
	EXP_ActionType_ADD.patient_diag_code_161 AS DIAG_CODE_16, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_50, 
	EXP_ActionType_ADD.patient_diag_code_171 AS DIAG_CODE_17, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_52, 
	EXP_ActionType_ADD.patient_diag_code_181 AS DIAG_CODE_18, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_54, 
	EXP_ActionType_ADD.patient_diag_code_191 AS DIAG_CODE_19, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_56, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS DESCRIPT_OF_ILLNESS, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS PRODUCT_LIABILITY_IND, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS PRODUCT_GENERIC_NAME, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS PRODUCT_BRAND_NAME, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS PRODUCT_MANUFACTURER, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS PRODUCT_ALLEGED_HARM, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_63, 
	EXP_ActionType_ADD.self_insd_ind1 AS SELF_INSURED_IND, 
	EXP_ActionType_ADD.self_insd_type1 AS SELF_INSURED_TYPE, 
	EXP_ActionType_ADD.self_insd_last_name1 AS POLICYHOLDER_LAST_NAME, 
	EXP_ActionType_ADD.self_insd_first_name1 AS POLICYHOLDER_FIRST_NAME, 
	EXP_ActionType_ADD.self_insd_dba_name1 AS DBA_NAME, 
	EXP_ActionType_ADD.self_insd_lgl_name1 AS LEGAL_NAME, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_70, 
	EXP_ActionType_ADD.wbmi_plan_ins_type1 AS PLAN_INSURANCE_TYPE, 
	LKP_sup_cms_tin_office_REUSABLE.office_tin_num AS TIN, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS OFFICE_CODE, 
	EXP_ActionType_ADD.pol_key1 AS POLICY_NUMBER, 
	EXP_ActionType_ADD.claim_occurrence_key1 AS CLAIM_NUMBER, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS PLAN_CONTACT_DEPARTMENT_NAME, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS PLAN_CONTACT_LAST_NAME, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS PLAN_CONTACT_FIRST_NAME, 
	EXP_ActionType_ADD.DEFAULT_ZEROS AS PLAN_CONTACT_PH, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS PLAN_CONTACT_PH_EXTENSION, 
	EXP_ActionType_ADD.no_fault_ins_lmt1 AS NO_FAULT_INSURANCE_LIMIT, 
	EXP_ActionType_ADD.exhaust_lmt_date1 AS EXHAUST_DATE_FOR_DOLLAR_LIMIT, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_83, 
	EXP_ActionType_ADD.injured_party_rep_Ind_MIJA1 AS INJ_PARTY_REP_IND, 
	EXP_ActionType_ADD.claim_party_last_name_MIJA1 AS REP_LAST_NAME, 
	EXP_ActionType_ADD.claim_party_first_name_MIJA1 AS REP_FIRST_NAME, 
	EXP_ActionType_ADD.injured_party_rep_firm_MIJA1 AS REP_FIRM_NAME, 
	EXP_ActionType_ADD.tax_id_MIJA1 AS REP_TIN, 
	EXP_ActionType_ADD.claim_party_addr_MIJA1 AS REP_MAIL_ADDR_1, 
	EXP_ActionType_ADD.claim_party_addr_Ln2_MIJA1 AS REP_MAIL_ADDR_2, 
	EXP_ActionType_ADD.claim_party_city_MIJA1 AS REP_CITY, 
	EXP_ActionType_ADD.claim_party_state_MIJA1 AS REP_STATE, 
	EXP_ActionType_ADD.claim_party_zip_MIJA1 AS REP_MAIL_ZIP_CODE, 
	EXP_ActionType_ADD.claim_party_zip4_MIJA1 AS REP_MAIL_ZIP4, 
	EXP_ActionType_ADD.ph_num_MIJA1 AS REP_PH, 
	EXP_ActionType_ADD.ph_extension_MIJA1 AS REP_PH_EXTENSION, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_97, 
	EXP_ActionType_ADD.med_obligation_to_claimant1 AS ORM_IND, 
	EXP_ActionType_ADD.orm_termination_date1 AS ORM_TERMINATION_DATE, 
	EXP_ActionType_ADD.tpoc_date1 AS TPOC_DATE, 
	EXP_ActionType_ADD.tpoc_amt1 AS TPOC_AMOUNT, 
	EXP_ActionType_ADD.tpoc_fund_delay_date1 AS FUNDING_DELAYED_BEYOND_TPOC_START_DATE, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_103, 
	EXP_ActionType_ADD.claimant_1_Ind_MCT11 AS C1_RELATIONSHIP, 
	EXP_ActionType_ADD.tax_id_MCT11 AS C1_TIN, 
	EXP_ActionType_ADD.claim_party_last_name_MCT11 AS C1_LAST_NAME, 
	EXP_ActionType_ADD.claim_party_first_name_MCT11 AS C1_FIRST_NAME, 
	EXP_ActionType_ADD.claim_party_mid_name_MCT11 AS C1_MIDDLE_INITIAL, 
	EXP_ActionType_ADD.claim_party_addr_MCT11 AS C1_MAIL_ADDR_1, 
	EXP_ActionType_ADD.claim_party_addr_Ln2_MCT11 AS C1_MAIL_ADDR_2, 
	EXP_ActionType_ADD.claim_party_city_MCT11 AS C1_CITY, 
	EXP_ActionType_ADD.claim_party_state_MCT11 AS C1_STATE, 
	EXP_ActionType_ADD.claim_party_zip_MCT11 AS C1_ZIP, 
	EXP_ActionType_ADD.claim_party_zip4_MCT11 AS C1_ZIP4, 
	EXP_ActionType_ADD.ph_num_MCT11 AS C1_PH, 
	EXP_ActionType_ADD.ph_extension_MCT11 AS C1_PH_EXTENSION, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_117, 
	EXP_ActionType_ADD.claimant_1_rep_Ind_MCA11 AS C1_REP_IND, 
	EXP_ActionType_ADD.claim_party_last_name_MCA11 AS C1_REP_LAST_NAME, 
	EXP_ActionType_ADD.claim_party_first_name_MCA11 AS C1_REP_FIRST_NAME, 
	EXP_ActionType_ADD.claimant1_rep_firm_MCA11 AS C1_REP_FIRM_NAME, 
	EXP_ActionType_ADD.tax_id_MCA11 AS C1_REP_TIN, 
	EXP_ActionType_ADD.claim_party_addr_MCA11 AS C1_REP_MAIL_ADDR_1, 
	EXP_ActionType_ADD.claim_party_addr_Ln2_MCA11 AS C1_REP_MAIL_ADDR_2, 
	EXP_ActionType_ADD.claim_party_city_MCA11 AS C1_REP_MAIL_CITY, 
	EXP_ActionType_ADD.claim_party_state_MCA11 AS C1_REP_STATE, 
	EXP_ActionType_ADD.claim_party_zip_MCA11 AS C1_REP_ZIP, 
	EXP_ActionType_ADD.claim_party_zip4_MCA11 AS C1_REP_ZIP4, 
	EXP_ActionType_ADD.ph_num_MCA11 AS C1_REP_PH, 
	EXP_ActionType_ADD.ph_extension_MCA11 AS C1_REP_PH_EXTENSION, 
	EXP_ActionType_ADD.DEFAULT_BLANKS AS RESERVED_131, 
	EXP_ActionType_ADD.CURRENT_DATE AS CREATED_DATE, 
	EXP_ActionType_ADD.CURRENT_DATE AS MODIFIED_DATE, 
	EXP_ActionType_ADD.AUDIT_ID AS AUDIT_ID
	FROM EXP_ActionType_ADD
),
LKP_work_claim_cms_detail_extract AS (
	SELECT
	work_claim_cms_detail_extract_id,
	claim_med_ak_id,
	claim_party_occurrence_ak_id,
	claim_med_plan_ak_id,
	claim_occurrence_ak_id,
	claim_party_ak_id,
	claim_med_patient_diag_add_ak_id,
	record_identifier,
	dcn,
	action_type,
	injured_party_hicn,
	injured_party_ssn,
	injured_party_last_name,
	injured_party_first_name,
	injured_party_middle_ini,
	injured_party_gender,
	injured_party_dob,
	reserved_11,
	cms_date_of_incident,
	industry_date_of_incident,
	reserved_14,
	alleged_cause_of_injury,
	reserved_16,
	state_of_venue,
	icd_indicator,
	diag_code_1,
	reserved_20,
	diag_code_2,
	reserved_22,
	diag_code_3,
	reserved_24,
	diag_code_4,
	reserved_26,
	diag_code_5,
	reserved_28,
	diag_code_6,
	reserved_30,
	diag_code_7,
	reserved_32,
	diag_code_8,
	reserved_34,
	diag_code_9,
	reserved_36,
	diag_code_10,
	reserved_38,
	diag_code_11,
	reserved_40,
	diag_code_12,
	reserved_42,
	diag_code_13,
	reserved_44,
	diag_code_14,
	reserved_46,
	diag_code_15,
	reserved_48,
	diag_code_16,
	reserved_50,
	diag_code_17,
	reserved_52,
	diag_code_18,
	reserved_54,
	diag_code_19,
	reserved_56,
	descript_of_illness,
	product_liability_ind,
	product_generic_name,
	product_brand_name,
	product_manufacturer,
	product_alleged_harm,
	reserved_63,
	self_insured_ind,
	self_insured_type,
	policyholder_last_name,
	policyholder_first_name,
	dba_name,
	legal_name,
	reserved_70,
	plan_insurance_type,
	tin,
	office_code,
	policy_number,
	claim_number,
	plan_contact_department_name,
	plan_contact_last_name,
	plan_contact_first_name,
	plan_contact_ph,
	plan_contact_ph_extension,
	no_fault_insurance_limit,
	exhaust_date_for_dollar_limit,
	reserved_83,
	inj_party_rep_ind,
	rep_last_name,
	rep_first_name,
	rep_firm_name,
	rep_tin,
	rep_mail_addr_1,
	rep_mail_addr_2,
	rep_city,
	rep_state,
	rep_mail_zip_code,
	rep_mail_zip4,
	rep_ph,
	rep_ph_extension,
	reserved_97,
	orm_ind,
	orm_termination_date,
	tpoc_date,
	tpoc_amount,
	funding_delayed_beyond_tpoc_start_date,
	reserved_103,
	c1_relationship,
	c1_tin,
	c1_last_name,
	c1_first_name,
	c1_middle_initial,
	c1_mail_addr_1,
	c1_mail_addr_2,
	c1_city,
	c1_state,
	c1_zip,
	c1_zip4,
	c1_ph,
	c1_ph_extension,
	reserved_117,
	c1_rep_ind,
	c1_rep_last_name,
	c1_rep_first_name,
	c1_rep_firm_name,
	c1_rep_tin,
	c1_rep_mail_addr_1,
	c1_rep_mail_addr_2,
	c1_rep_mail_city,
	c1_rep_state,
	c1_rep_zip,
	c1_rep_zip4,
	c1_rep_ph,
	c1_rep_ph_extension,
	reserved_131,
	created_date,
	modified_date,
	audit_id,
	applied_disposition_code,
	reject_flag
	FROM (
		SELECT work_claim_cms_detail_extract.work_claim_cms_detail_extract_id       AS work_claim_cms_detail_extract_id,
		       work_claim_cms_detail_extract.claim_med_ak_id                        AS claim_med_ak_id,
		       work_claim_cms_detail_extract.claim_party_occurrence_ak_id           AS claim_party_occurrence_ak_id,
		       work_claim_cms_detail_extract.claim_med_plan_ak_id                   AS claim_med_plan_ak_id,
		       work_claim_cms_detail_extract.claim_occurrence_ak_id                 AS claim_occurrence_ak_id,
		       work_claim_cms_detail_extract.claim_party_ak_id                      AS claim_party_ak_id,
		       work_claim_cms_detail_extract.claim_med_patient_diag_add_ak_id       AS claim_med_patient_diag_add_ak_id,
		       work_claim_cms_detail_extract.record_identifier                      AS record_identifier,
		       work_claim_cms_detail_extract.action_type                            AS action_type,
		       work_claim_cms_detail_extract.injured_party_hicn                     AS injured_party_hicn,
		       work_claim_cms_detail_extract.injured_party_ssn                      AS injured_party_ssn,
		       work_claim_cms_detail_extract.injured_party_last_name                AS injured_party_last_name,
		       work_claim_cms_detail_extract.injured_party_first_name               AS injured_party_first_name,
		       work_claim_cms_detail_extract.injured_party_middle_ini               AS injured_party_middle_ini,
		       work_claim_cms_detail_extract.injured_party_gender                   AS injured_party_gender,
		       work_claim_cms_detail_extract.injured_party_dob                      AS injured_party_dob,
		       work_claim_cms_detail_extract.reserved_11                            AS reserved_11,
		       work_claim_cms_detail_extract.cms_date_of_incident                   AS cms_date_of_incident,
		       work_claim_cms_detail_extract.industry_date_of_incident              AS industry_date_of_incident,
		       work_claim_cms_detail_extract.reserved_14                            AS reserved_14,
		       work_claim_cms_detail_extract.alleged_cause_of_injury                AS alleged_cause_of_injury,
		       work_claim_cms_detail_extract.reserved_16                            AS reserved_16,
		       work_claim_cms_detail_extract.state_of_venue                         AS state_of_venue,
		       work_claim_cms_detail_extract.icd_indicator                          AS icd_indicator,
		       work_claim_cms_detail_extract.diag_code_1                  AS diag_code_1,
		       work_claim_cms_detail_extract.reserved_20                            AS reserved_20,
		       work_claim_cms_detail_extract.diag_code_2                  AS diag_code_2,
		       work_claim_cms_detail_extract.reserved_22                            AS reserved_22,
		       work_claim_cms_detail_extract.diag_code_3                  AS diag_code_3,
		       work_claim_cms_detail_extract.reserved_24                            AS reserved_24,
		       work_claim_cms_detail_extract.diag_code_4                  AS diag_code_4,
		       work_claim_cms_detail_extract.reserved_26                            AS reserved_26,
		       work_claim_cms_detail_extract.diag_code_5                  AS diag_code_5,
		       work_claim_cms_detail_extract.reserved_28                            AS reserved_28,
		       work_claim_cms_detail_extract.diag_code_6                  AS diag_code_6,
		       work_claim_cms_detail_extract.reserved_30                            AS reserved_30,
		       work_claim_cms_detail_extract.diag_code_7                  AS diag_code_7,
		       work_claim_cms_detail_extract.reserved_32                            AS reserved_32,
		       work_claim_cms_detail_extract.diag_code_8                  AS diag_code_8,
		       work_claim_cms_detail_extract.reserved_34                            AS reserved_34,
		       work_claim_cms_detail_extract.diag_code_9                  AS diag_code_9,
		       work_claim_cms_detail_extract.reserved_36                            AS reserved_36,
		       work_claim_cms_detail_extract.diag_code_10                 AS diag_code_10,
		       work_claim_cms_detail_extract.reserved_38                            AS reserved_38,
		       work_claim_cms_detail_extract.diag_code_11                 AS diag_code_11,
		       work_claim_cms_detail_extract.reserved_40                            AS reserved_40,
		       work_claim_cms_detail_extract.diag_code_12                 AS diag_code_12,
		       work_claim_cms_detail_extract.reserved_42                            AS reserved_42,
		       work_claim_cms_detail_extract.diag_code_13                 AS diag_code_13,
		       work_claim_cms_detail_extract.reserved_44                            AS reserved_44,
		       work_claim_cms_detail_extract.diag_code_14                 AS diag_code_14,
		       work_claim_cms_detail_extract.reserved_46                            AS reserved_46,
		       work_claim_cms_detail_extract.diag_code_15                 AS diag_code_15,
		       work_claim_cms_detail_extract.reserved_48                            AS reserved_48,
		       work_claim_cms_detail_extract.diag_code_16                 AS diag_code_16,
		       work_claim_cms_detail_extract.reserved_50                            AS reserved_50,
		       work_claim_cms_detail_extract.diag_code_17                 AS diag_code_17,
		       work_claim_cms_detail_extract.reserved_52                            AS reserved_52,
		       work_claim_cms_detail_extract.diag_code_18                 AS diag_code_18,
		       work_claim_cms_detail_extract.reserved_54                            AS reserved_54,
		       work_claim_cms_detail_extract.diag_code_19                 AS diag_code_19,
		       work_claim_cms_detail_extract.reserved_56                            AS reserved_56,
		       work_claim_cms_detail_extract.descript_of_illness                    AS descript_of_illness,
		       work_claim_cms_detail_extract.product_liability_ind                  AS product_liability_ind,
		       work_claim_cms_detail_extract.product_generic_name                   AS product_generic_name,
		       work_claim_cms_detail_extract.product_brand_name                     AS product_brand_name,
		       work_claim_cms_detail_extract.product_manufacturer                   AS product_manufacturer,
		       work_claim_cms_detail_extract.product_alleged_harm                   AS product_alleged_harm,
		       work_claim_cms_detail_extract.reserved_63                            AS reserved_63,
		       work_claim_cms_detail_extract.self_insured_ind                       AS self_insured_ind,
		       work_claim_cms_detail_extract.self_insured_type                      AS self_insured_type,
		       work_claim_cms_detail_extract.policyholder_last_name                 AS policyholder_last_name,
		       work_claim_cms_detail_extract.policyholder_first_name                AS policyholder_first_name,
		       work_claim_cms_detail_extract.dba_name                               AS dba_name,
		       work_claim_cms_detail_extract.legal_name                             AS legal_name,
		       work_claim_cms_detail_extract.reserved_70                            AS reserved_70,
		       work_claim_cms_detail_extract.plan_insurance_type                    AS plan_insurance_type,
		       work_claim_cms_detail_extract.tin                                    AS tin,
		       work_claim_cms_detail_extract.office_code                            AS office_code,
		       work_claim_cms_detail_extract.policy_number                          AS policy_number,
		       work_claim_cms_detail_extract.claim_number                           AS claim_number,
		       work_claim_cms_detail_extract.plan_contact_department_name           AS plan_contact_department_name,
		       work_claim_cms_detail_extract.plan_contact_last_name                 AS plan_contact_last_name,
		       work_claim_cms_detail_extract.plan_contact_first_name                AS plan_contact_first_name,
		       work_claim_cms_detail_extract.plan_contact_ph                        AS plan_contact_ph,
		       work_claim_cms_detail_extract.plan_contact_ph_extension              AS plan_contact_ph_extension,
		       work_claim_cms_detail_extract.no_fault_insurance_limit               AS no_fault_insurance_limit,
		       work_claim_cms_detail_extract.exhaust_date_for_dollar_limit          AS exhaust_date_for_dollar_limit,
		       work_claim_cms_detail_extract.reserved_83                            AS reserved_83,
		       work_claim_cms_detail_extract.inj_party_rep_ind                      AS inj_party_rep_ind,
		       work_claim_cms_detail_extract.rep_last_name                          AS rep_last_name,
		       work_claim_cms_detail_extract.rep_first_name                         AS rep_first_name,
		       work_claim_cms_detail_extract.rep_firm_name                          AS rep_firm_name,
		       work_claim_cms_detail_extract.rep_tin                                AS rep_tin,
		       work_claim_cms_detail_extract.rep_mail_addr_1                        AS rep_mail_addr_1,
		       work_claim_cms_detail_extract.rep_mail_addr_2                        AS rep_mail_addr_2,
		       work_claim_cms_detail_extract.rep_city                               AS rep_city,
		       work_claim_cms_detail_extract.rep_state                              AS rep_state,
		       work_claim_cms_detail_extract.rep_mail_zip_code                      AS rep_mail_zip_code,
		       work_claim_cms_detail_extract.rep_mail_zip4                          AS rep_mail_zip4,
		       work_claim_cms_detail_extract.rep_ph                                 AS rep_ph,
		       work_claim_cms_detail_extract.rep_ph_extension                       AS rep_ph_extension,
		       work_claim_cms_detail_extract.reserved_97                            AS reserved_97,
		       work_claim_cms_detail_extract.orm_ind                                AS orm_ind,
		       work_claim_cms_detail_extract.orm_termination_date                   AS orm_termination_date,
		       work_claim_cms_detail_extract.tpoc_date                              AS tpoc_date,
		       work_claim_cms_detail_extract.tpoc_amount                            AS tpoc_amount,
		       work_claim_cms_detail_extract.funding_delayed_beyond_tpoc_start_date AS funding_delayed_beyond_tpoc_start_date,
		       work_claim_cms_detail_extract.reserved_103                           AS reserved_103,
		       work_claim_cms_detail_extract.c1_relationship                        AS c1_relationship,
		       work_claim_cms_detail_extract.c1_tin                                 AS c1_tin,
		       work_claim_cms_detail_extract.c1_last_name                           AS c1_last_name,
		       work_claim_cms_detail_extract.c1_first_name                          AS c1_first_name,
		       work_claim_cms_detail_extract.c1_middle_initial                      AS c1_middle_initial,
		       work_claim_cms_detail_extract.c1_mail_addr_1                         AS c1_mail_addr_1,
		       work_claim_cms_detail_extract.c1_mail_addr_2                         AS c1_mail_addr_2,
		       work_claim_cms_detail_extract.c1_city                                AS c1_city,
		       work_claim_cms_detail_extract.c1_state                               AS c1_state,
		       work_claim_cms_detail_extract.c1_zip                                 AS c1_zip,
		       work_claim_cms_detail_extract.c1_zip4                                AS c1_zip4,
		       work_claim_cms_detail_extract.c1_ph                                  AS c1_ph,
		       work_claim_cms_detail_extract.c1_ph_extension                        AS c1_ph_extension,
		       work_claim_cms_detail_extract.reserved_117                           AS reserved_117,
		       work_claim_cms_detail_extract.c1_rep_ind                             AS c1_rep_ind,
		       work_claim_cms_detail_extract.c1_rep_last_name                       AS c1_rep_last_name,
		       work_claim_cms_detail_extract.c1_rep_first_name                      AS c1_rep_first_name,
		       work_claim_cms_detail_extract.c1_rep_firm_name                       AS c1_rep_firm_name,
		       work_claim_cms_detail_extract.c1_rep_tin                             AS c1_rep_tin,
		       work_claim_cms_detail_extract.c1_rep_mail_addr_1                     AS c1_rep_mail_addr_1,
		       work_claim_cms_detail_extract.c1_rep_mail_addr_2                     AS c1_rep_mail_addr_2,
		       work_claim_cms_detail_extract.c1_rep_mail_city                       AS c1_rep_mail_city,
		       work_claim_cms_detail_extract.c1_rep_state                           AS c1_rep_state,
		       work_claim_cms_detail_extract.c1_rep_zip                             AS c1_rep_zip,
		       work_claim_cms_detail_extract.c1_rep_zip4                            AS c1_rep_zip4,
		       work_claim_cms_detail_extract.c1_rep_ph                              AS c1_rep_ph,
		       work_claim_cms_detail_extract.c1_rep_ph_extension                    AS c1_rep_ph_extension,
		       work_claim_cms_detail_extract.reserved_131                           AS reserved_131,
		       work_claim_cms_detail_extract.created_date                           AS created_date,
		       work_claim_cms_detail_extract.modified_date                          AS modified_date,
		       work_claim_cms_detail_extract.audit_id                               AS audit_id,
		       Rtrim(work_claim_cms_detail_extract.dcn)                             AS dcn
		FROM   work_claim_cms_detail_extract
		WHERE  audit_id <> @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
		       AND ( reject_flag IS NULL
		              OR reject_flag = 'false' )
		ORDER  BY created_date ASC --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY dcn ORDER BY work_claim_cms_detail_extract_id DESC) = 1
),
EXP_ActionType_DELETE AS (
	SELECT
	LKP_work_claim_cms_detail_extract.claim_med_ak_id,
	LKP_work_claim_cms_detail_extract.claim_party_occurrence_ak_id,
	LKP_work_claim_cms_detail_extract.claim_med_plan_ak_id,
	LKP_work_claim_cms_detail_extract.claim_occurrence_ak_id,
	LKP_work_claim_cms_detail_extract.claim_party_ak_id,
	LKP_work_claim_cms_detail_extract.claim_med_patient_diag_add_ak_id,
	RTR_Add_Update_Delete_Action_Type_DELETE.record_identifier,
	RTR_Add_Update_Delete_Action_Type_DELETE.cms_document_cntl_num AS dcn,
	RTR_Add_Update_Delete_Action_Type_DELETE.action_type,
	-- *INF*: IIF(ISNULL(action_type),'',
	-- IIF(action_type='3','1',action_type))
	-- 
	-- --action type 3 means ADDDELETE so we need to convert it to 1 (means DELETE) as CMS doesn't allow value of 3
	IFF(action_type IS NULL, '', IFF(
	        action_type = '3', '1', action_type
	    )) AS action_type_out,
	LKP_work_claim_cms_detail_extract.injured_party_hicn,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(rtrim(ltrim(injured_party_hicn)))
	-- 
	-- -- this needs to be trimmed or the extra spaces nullify the UDF.
	UDF_DEFAULT_VALUE_TO_BLANKS(rtrim(ltrim(injured_party_hicn))) AS v_injured_party_hicn,
	RTR_Add_Update_Delete_Action_Type_DELETE.last_cms_hicn AS last_cms_hicn3,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(last_cms_hicn3)
	UDF_DEFAULT_VALUE_TO_BLANKS(last_cms_hicn3) AS v_last_cms_hicn,
	-- *INF*: DECODE(TRUE,
	-- length(v_last_cms_hicn)>0,v_last_cms_hicn,
	-- length(v_injured_party_hicn)>0,v_injured_party_hicn,
	-- ''
	-- )
	DECODE(
	    TRUE,
	    length(v_last_cms_hicn) > 0, v_last_cms_hicn,
	    length(v_injured_party_hicn) > 0, v_injured_party_hicn,
	    ''
	) AS v_hicn_out,
	v_hicn_out AS hicn_out,
	LKP_work_claim_cms_detail_extract.injured_party_ssn,
	-- *INF*: IIF(length(v_hicn_out)= 0, injured_party_ssn,'')
	IFF(length(v_hicn_out) = 0, injured_party_ssn, '') AS injured_party_ssn_out,
	LKP_work_claim_cms_detail_extract.injured_party_last_name,
	LKP_work_claim_cms_detail_extract.injured_party_first_name,
	LKP_work_claim_cms_detail_extract.injured_party_middle_ini,
	LKP_work_claim_cms_detail_extract.injured_party_gender,
	LKP_work_claim_cms_detail_extract.injured_party_dob,
	LKP_work_claim_cms_detail_extract.reserved_11,
	LKP_work_claim_cms_detail_extract.cms_date_of_incident,
	LKP_work_claim_cms_detail_extract.industry_date_of_incident,
	LKP_work_claim_cms_detail_extract.reserved_14,
	LKP_work_claim_cms_detail_extract.alleged_cause_of_injury,
	LKP_work_claim_cms_detail_extract.reserved_16,
	LKP_work_claim_cms_detail_extract.state_of_venue,
	LKP_work_claim_cms_detail_extract.icd_indicator,
	LKP_work_claim_cms_detail_extract.diag_code_1,
	LKP_work_claim_cms_detail_extract.reserved_20,
	LKP_work_claim_cms_detail_extract.diag_code_2,
	LKP_work_claim_cms_detail_extract.reserved_22,
	LKP_work_claim_cms_detail_extract.diag_code_3,
	LKP_work_claim_cms_detail_extract.reserved_24,
	LKP_work_claim_cms_detail_extract.diag_code_4,
	LKP_work_claim_cms_detail_extract.reserved_26,
	LKP_work_claim_cms_detail_extract.diag_code_5,
	LKP_work_claim_cms_detail_extract.reserved_28,
	LKP_work_claim_cms_detail_extract.diag_code_6,
	LKP_work_claim_cms_detail_extract.reserved_30,
	LKP_work_claim_cms_detail_extract.diag_code_7,
	LKP_work_claim_cms_detail_extract.reserved_32,
	LKP_work_claim_cms_detail_extract.diag_code_8,
	LKP_work_claim_cms_detail_extract.reserved_34,
	LKP_work_claim_cms_detail_extract.diag_code_9,
	LKP_work_claim_cms_detail_extract.reserved_36,
	LKP_work_claim_cms_detail_extract.diag_code_10,
	LKP_work_claim_cms_detail_extract.reserved_38,
	LKP_work_claim_cms_detail_extract.diag_code_11,
	LKP_work_claim_cms_detail_extract.reserved_40,
	LKP_work_claim_cms_detail_extract.diag_code_12,
	LKP_work_claim_cms_detail_extract.reserved_42,
	LKP_work_claim_cms_detail_extract.diag_code_13,
	LKP_work_claim_cms_detail_extract.reserved_44,
	LKP_work_claim_cms_detail_extract.diag_code_14,
	LKP_work_claim_cms_detail_extract.reserved_46,
	LKP_work_claim_cms_detail_extract.diag_code_15,
	LKP_work_claim_cms_detail_extract.reserved_48,
	LKP_work_claim_cms_detail_extract.diag_code_16,
	LKP_work_claim_cms_detail_extract.reserved_50,
	LKP_work_claim_cms_detail_extract.diag_code_17,
	LKP_work_claim_cms_detail_extract.reserved_52,
	LKP_work_claim_cms_detail_extract.diag_code_18,
	LKP_work_claim_cms_detail_extract.reserved_54,
	LKP_work_claim_cms_detail_extract.diag_code_19,
	LKP_work_claim_cms_detail_extract.reserved_56,
	LKP_work_claim_cms_detail_extract.descript_of_illness,
	LKP_work_claim_cms_detail_extract.product_liability_ind,
	LKP_work_claim_cms_detail_extract.product_generic_name,
	LKP_work_claim_cms_detail_extract.product_brand_name,
	LKP_work_claim_cms_detail_extract.product_manufacturer,
	LKP_work_claim_cms_detail_extract.product_alleged_harm,
	LKP_work_claim_cms_detail_extract.reserved_63,
	LKP_work_claim_cms_detail_extract.self_insured_ind,
	LKP_work_claim_cms_detail_extract.self_insured_type,
	LKP_work_claim_cms_detail_extract.policyholder_last_name,
	LKP_work_claim_cms_detail_extract.policyholder_first_name,
	LKP_work_claim_cms_detail_extract.dba_name,
	LKP_work_claim_cms_detail_extract.legal_name,
	LKP_work_claim_cms_detail_extract.reserved_70,
	LKP_work_claim_cms_detail_extract.plan_insurance_type,
	LKP_work_claim_cms_detail_extract.tin,
	LKP_work_claim_cms_detail_extract.office_code,
	LKP_work_claim_cms_detail_extract.policy_number,
	LKP_work_claim_cms_detail_extract.claim_number,
	LKP_work_claim_cms_detail_extract.plan_contact_department_name,
	LKP_work_claim_cms_detail_extract.plan_contact_last_name,
	LKP_work_claim_cms_detail_extract.plan_contact_first_name,
	LKP_work_claim_cms_detail_extract.plan_contact_ph,
	LKP_work_claim_cms_detail_extract.plan_contact_ph_extension,
	LKP_work_claim_cms_detail_extract.no_fault_insurance_limit,
	LKP_work_claim_cms_detail_extract.exhaust_date_for_dollar_limit,
	LKP_work_claim_cms_detail_extract.reserved_83,
	LKP_work_claim_cms_detail_extract.inj_party_rep_ind,
	LKP_work_claim_cms_detail_extract.rep_last_name,
	LKP_work_claim_cms_detail_extract.rep_first_name,
	LKP_work_claim_cms_detail_extract.rep_firm_name,
	LKP_work_claim_cms_detail_extract.rep_tin,
	LKP_work_claim_cms_detail_extract.rep_mail_addr_1,
	LKP_work_claim_cms_detail_extract.rep_mail_addr_2,
	LKP_work_claim_cms_detail_extract.rep_city,
	LKP_work_claim_cms_detail_extract.rep_state,
	LKP_work_claim_cms_detail_extract.rep_mail_zip_code,
	LKP_work_claim_cms_detail_extract.rep_mail_zip4,
	LKP_work_claim_cms_detail_extract.rep_ph,
	LKP_work_claim_cms_detail_extract.rep_ph_extension,
	LKP_work_claim_cms_detail_extract.reserved_97,
	LKP_work_claim_cms_detail_extract.orm_ind,
	LKP_work_claim_cms_detail_extract.orm_termination_date,
	LKP_work_claim_cms_detail_extract.tpoc_date,
	LKP_work_claim_cms_detail_extract.tpoc_amount,
	LKP_work_claim_cms_detail_extract.funding_delayed_beyond_tpoc_start_date,
	LKP_work_claim_cms_detail_extract.reserved_103,
	LKP_work_claim_cms_detail_extract.c1_relationship,
	LKP_work_claim_cms_detail_extract.c1_tin,
	LKP_work_claim_cms_detail_extract.c1_last_name,
	LKP_work_claim_cms_detail_extract.c1_first_name,
	LKP_work_claim_cms_detail_extract.c1_middle_initial,
	LKP_work_claim_cms_detail_extract.c1_mail_addr_1,
	LKP_work_claim_cms_detail_extract.c1_mail_addr_2,
	LKP_work_claim_cms_detail_extract.c1_city,
	LKP_work_claim_cms_detail_extract.c1_state,
	LKP_work_claim_cms_detail_extract.c1_zip,
	LKP_work_claim_cms_detail_extract.c1_zip4,
	LKP_work_claim_cms_detail_extract.c1_ph,
	LKP_work_claim_cms_detail_extract.c1_ph_extension,
	LKP_work_claim_cms_detail_extract.reserved_117,
	LKP_work_claim_cms_detail_extract.c1_rep_ind,
	LKP_work_claim_cms_detail_extract.c1_rep_last_name,
	LKP_work_claim_cms_detail_extract.c1_rep_first_name,
	LKP_work_claim_cms_detail_extract.c1_rep_firm_name,
	LKP_work_claim_cms_detail_extract.c1_rep_tin,
	LKP_work_claim_cms_detail_extract.c1_rep_mail_addr_1,
	LKP_work_claim_cms_detail_extract.c1_rep_mail_addr_2,
	LKP_work_claim_cms_detail_extract.c1_rep_mail_city,
	LKP_work_claim_cms_detail_extract.c1_rep_state,
	LKP_work_claim_cms_detail_extract.c1_rep_zip,
	LKP_work_claim_cms_detail_extract.c1_rep_zip4,
	LKP_work_claim_cms_detail_extract.c1_rep_ph,
	LKP_work_claim_cms_detail_extract.c1_rep_ph_extension,
	LKP_work_claim_cms_detail_extract.reserved_131,
	SYSDATE AS created_date,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id
	FROM RTR_Add_Update_Delete_Action_Type_DELETE
	LEFT JOIN LKP_work_claim_cms_detail_extract
	ON LKP_work_claim_cms_detail_extract.dcn = RTR_Add_Update_Delete.cms_document_cntl_num3
),
work_claim_cms_detail_extract_DELETE AS (
	INSERT INTO work_claim_cms_detail_extract
	(claim_med_ak_id, claim_party_occurrence_ak_id, claim_med_plan_ak_id, claim_occurrence_ak_id, claim_party_ak_id, claim_med_patient_diag_add_ak_id, record_identifier, dcn, action_type, injured_party_hicn, injured_party_ssn, injured_party_last_name, injured_party_first_name, injured_party_middle_ini, injured_party_gender, injured_party_dob, reserved_11, cms_date_of_incident, industry_date_of_incident, reserved_14, alleged_cause_of_injury, reserved_16, state_of_venue, icd_indicator, diag_code_1, reserved_20, diag_code_2, reserved_22, diag_code_3, reserved_24, diag_code_4, reserved_26, diag_code_5, reserved_28, diag_code_6, reserved_30, diag_code_7, reserved_32, diag_code_8, reserved_34, diag_code_9, reserved_36, diag_code_10, reserved_38, diag_code_11, reserved_40, diag_code_12, reserved_42, diag_code_13, reserved_44, diag_code_14, reserved_46, diag_code_15, reserved_48, diag_code_16, reserved_50, diag_code_17, reserved_52, diag_code_18, reserved_54, diag_code_19, reserved_56, descript_of_illness, product_liability_ind, product_generic_name, product_brand_name, product_manufacturer, product_alleged_harm, reserved_63, self_insured_ind, self_insured_type, policyholder_last_name, policyholder_first_name, dba_name, legal_name, reserved_70, plan_insurance_type, tin, office_code, policy_number, claim_number, plan_contact_department_name, plan_contact_last_name, plan_contact_first_name, plan_contact_ph, plan_contact_ph_extension, no_fault_insurance_limit, exhaust_date_for_dollar_limit, reserved_83, inj_party_rep_ind, rep_last_name, rep_first_name, rep_firm_name, rep_tin, rep_mail_addr_1, rep_mail_addr_2, rep_city, rep_state, rep_mail_zip_code, rep_mail_zip4, rep_ph, rep_ph_extension, reserved_97, orm_ind, orm_termination_date, tpoc_date, tpoc_amount, funding_delayed_beyond_tpoc_start_date, reserved_103, c1_relationship, c1_tin, c1_last_name, c1_first_name, c1_middle_initial, c1_mail_addr_1, c1_mail_addr_2, c1_city, c1_state, c1_zip, c1_zip4, c1_ph, c1_ph_extension, reserved_117, c1_rep_ind, c1_rep_last_name, c1_rep_first_name, c1_rep_firm_name, c1_rep_tin, c1_rep_mail_addr_1, c1_rep_mail_addr_2, c1_rep_mail_city, c1_rep_state, c1_rep_zip, c1_rep_zip4, c1_rep_ph, c1_rep_ph_extension, reserved_131, created_date, modified_date, audit_id)
	SELECT 
	CLAIM_MED_AK_ID, 
	CLAIM_PARTY_OCCURRENCE_AK_ID, 
	CLAIM_MED_PLAN_AK_ID, 
	CLAIM_OCCURRENCE_AK_ID, 
	CLAIM_PARTY_AK_ID, 
	CLAIM_MED_PATIENT_DIAG_ADD_AK_ID, 
	RECORD_IDENTIFIER, 
	DCN, 
	action_type_out AS ACTION_TYPE, 
	hicn_out AS INJURED_PARTY_HICN, 
	injured_party_ssn_out AS INJURED_PARTY_SSN, 
	INJURED_PARTY_LAST_NAME, 
	INJURED_PARTY_FIRST_NAME, 
	INJURED_PARTY_MIDDLE_INI, 
	INJURED_PARTY_GENDER, 
	INJURED_PARTY_DOB, 
	RESERVED_11, 
	CMS_DATE_OF_INCIDENT, 
	INDUSTRY_DATE_OF_INCIDENT, 
	RESERVED_14, 
	ALLEGED_CAUSE_OF_INJURY, 
	RESERVED_16, 
	STATE_OF_VENUE, 
	ICD_INDICATOR, 
	DIAG_CODE_1, 
	RESERVED_20, 
	DIAG_CODE_2, 
	RESERVED_22, 
	DIAG_CODE_3, 
	RESERVED_24, 
	DIAG_CODE_4, 
	RESERVED_26, 
	DIAG_CODE_5, 
	RESERVED_28, 
	DIAG_CODE_6, 
	RESERVED_30, 
	DIAG_CODE_7, 
	RESERVED_32, 
	DIAG_CODE_8, 
	RESERVED_34, 
	DIAG_CODE_9, 
	RESERVED_36, 
	DIAG_CODE_10, 
	RESERVED_38, 
	DIAG_CODE_11, 
	RESERVED_40, 
	DIAG_CODE_12, 
	RESERVED_42, 
	DIAG_CODE_13, 
	RESERVED_44, 
	DIAG_CODE_14, 
	RESERVED_46, 
	DIAG_CODE_15, 
	RESERVED_48, 
	DIAG_CODE_16, 
	RESERVED_50, 
	DIAG_CODE_17, 
	RESERVED_52, 
	DIAG_CODE_18, 
	RESERVED_54, 
	DIAG_CODE_19, 
	RESERVED_56, 
	DESCRIPT_OF_ILLNESS, 
	PRODUCT_LIABILITY_IND, 
	PRODUCT_GENERIC_NAME, 
	PRODUCT_BRAND_NAME, 
	PRODUCT_MANUFACTURER, 
	PRODUCT_ALLEGED_HARM, 
	RESERVED_63, 
	SELF_INSURED_IND, 
	SELF_INSURED_TYPE, 
	POLICYHOLDER_LAST_NAME, 
	POLICYHOLDER_FIRST_NAME, 
	DBA_NAME, 
	LEGAL_NAME, 
	RESERVED_70, 
	PLAN_INSURANCE_TYPE, 
	TIN, 
	OFFICE_CODE, 
	POLICY_NUMBER, 
	CLAIM_NUMBER, 
	PLAN_CONTACT_DEPARTMENT_NAME, 
	PLAN_CONTACT_LAST_NAME, 
	PLAN_CONTACT_FIRST_NAME, 
	PLAN_CONTACT_PH, 
	PLAN_CONTACT_PH_EXTENSION, 
	NO_FAULT_INSURANCE_LIMIT, 
	EXHAUST_DATE_FOR_DOLLAR_LIMIT, 
	RESERVED_83, 
	INJ_PARTY_REP_IND, 
	REP_LAST_NAME, 
	REP_FIRST_NAME, 
	REP_FIRM_NAME, 
	REP_TIN, 
	REP_MAIL_ADDR_1, 
	REP_MAIL_ADDR_2, 
	REP_CITY, 
	REP_STATE, 
	REP_MAIL_ZIP_CODE, 
	REP_MAIL_ZIP4, 
	REP_PH, 
	REP_PH_EXTENSION, 
	RESERVED_97, 
	ORM_IND, 
	ORM_TERMINATION_DATE, 
	TPOC_DATE, 
	TPOC_AMOUNT, 
	FUNDING_DELAYED_BEYOND_TPOC_START_DATE, 
	RESERVED_103, 
	C1_RELATIONSHIP, 
	C1_TIN, 
	C1_LAST_NAME, 
	C1_FIRST_NAME, 
	C1_MIDDLE_INITIAL, 
	C1_MAIL_ADDR_1, 
	C1_MAIL_ADDR_2, 
	C1_CITY, 
	C1_STATE, 
	C1_ZIP, 
	C1_ZIP4, 
	C1_PH, 
	C1_PH_EXTENSION, 
	RESERVED_117, 
	C1_REP_IND, 
	C1_REP_LAST_NAME, 
	C1_REP_FIRST_NAME, 
	C1_REP_FIRM_NAME, 
	C1_REP_TIN, 
	C1_REP_MAIL_ADDR_1, 
	C1_REP_MAIL_ADDR_2, 
	C1_REP_MAIL_CITY, 
	C1_REP_STATE, 
	C1_REP_ZIP, 
	C1_REP_ZIP4, 
	C1_REP_PH, 
	C1_REP_PH_EXTENSION, 
	RESERVED_131, 
	CREATED_DATE, 
	created_date AS MODIFIED_DATE, 
	AUDIT_ID
	FROM EXP_ActionType_DELETE
),
LKP_work_claim_cms_detail_extract_Update_Keys_Only AS (
	SELECT
	work_claim_cms_detail_extract_id,
	claim_med_ak_id,
	claim_party_occurrence_ak_id,
	claim_med_plan_ak_id,
	claim_occurrence_ak_id,
	claim_party_ak_id,
	claim_med_patient_diag_add_ak_id,
	record_identifier,
	dcn,
	action_type,
	injured_party_hicn,
	injured_party_ssn,
	injured_party_last_name,
	injured_party_first_name,
	injured_party_middle_ini,
	injured_party_gender,
	injured_party_dob,
	reserved_11,
	cms_date_of_incident,
	industry_date_of_incident,
	reserved_14,
	alleged_cause_of_injury,
	reserved_16,
	state_of_venue,
	reserved_18,
	diag_code_1,
	reserved_20,
	diag_code_2,
	reserved_22,
	diag_code_3,
	reserved_24,
	diag_code_4,
	reserved_26,
	diag_code_5,
	reserved_28,
	diag_code_6,
	reserved_30,
	diag_code_7,
	reserved_32,
	diag_code_8,
	reserved_34,
	diag_code_9,
	reserved_36,
	diag_code_10,
	reserved_38,
	diag_code_11,
	reserved_40,
	diag_code_12,
	reserved_42,
	diag_code_13,
	reserved_44,
	diag_code_14,
	reserved_46,
	diag_code_15,
	reserved_48,
	diag_code_16,
	reserved_50,
	diag_code_17,
	reserved_52,
	diag_code_18,
	reserved_54,
	diag_code_19,
	reserved_56,
	descript_of_illness,
	product_liability_ind,
	product_generic_name,
	product_brand_name,
	product_manufacturer,
	product_alleged_harm,
	reserved_63,
	self_insured_ind,
	self_insured_type,
	policyholder_last_name,
	policyholder_first_name,
	dba_name,
	legal_name,
	reserved_70,
	plan_insurance_type,
	tin,
	office_code,
	policy_number,
	claim_number,
	plan_contact_department_name,
	plan_contact_last_name,
	plan_contact_first_name,
	plan_contact_ph,
	plan_contact_ph_extension,
	no_fault_insurance_limit,
	exhaust_date_for_dollar_limit,
	reserved_83,
	inj_party_rep_ind,
	rep_last_name,
	rep_first_name,
	rep_firm_name,
	rep_tin,
	rep_mail_addr_1,
	rep_mail_addr_2,
	rep_city,
	rep_state,
	rep_mail_zip_code,
	rep_mail_zip4,
	rep_ph,
	rep_ph_extension,
	reserved_97,
	orm_ind,
	orm_termination_date,
	tpoc_date,
	tpoc_amount,
	funding_delayed_beyond_tpoc_start_date,
	reserved_103,
	c1_relationship,
	c1_tin,
	c1_last_name,
	c1_first_name,
	c1_middle_initial,
	c1_mail_addr_1,
	c1_mail_addr_2,
	c1_city,
	c1_state,
	c1_zip,
	c1_zip4,
	c1_ph,
	c1_ph_extension,
	reserved_117,
	c1_rep_ind,
	c1_rep_last_name,
	c1_rep_first_name,
	c1_rep_firm_name,
	c1_rep_tin,
	c1_rep_mail_addr_1,
	c1_rep_mail_addr_2,
	c1_rep_mail_city,
	c1_rep_state,
	c1_rep_zip,
	c1_rep_zip4,
	c1_rep_ph,
	c1_rep_ph_extension,
	reserved_131,
	created_date,
	modified_date,
	audit_id,
	applied_disposition_code,
	reject_flag
	FROM (
		SELECT work_claim_cms_detail_extract.work_claim_cms_detail_extract_id       AS work_claim_cms_detail_extract_id,
		       work_claim_cms_detail_extract.claim_med_ak_id                        AS claim_med_ak_id,
		       work_claim_cms_detail_extract.claim_party_occurrence_ak_id           AS claim_party_occurrence_ak_id,
		       work_claim_cms_detail_extract.claim_med_plan_ak_id                   AS claim_med_plan_ak_id,
		       work_claim_cms_detail_extract.claim_occurrence_ak_id                 AS claim_occurrence_ak_id,
		       work_claim_cms_detail_extract.claim_party_ak_id                      AS claim_party_ak_id,
		       work_claim_cms_detail_extract.claim_med_patient_diag_add_ak_id       AS claim_med_patient_diag_add_ak_id,
		       work_claim_cms_detail_extract.record_identifier                      AS record_identifier,
		       work_claim_cms_detail_extract.action_type                            AS action_type,
		       work_claim_cms_detail_extract.injured_party_hicn                     AS injured_party_hicn,
		       work_claim_cms_detail_extract.injured_party_ssn                      AS injured_party_ssn,
		       work_claim_cms_detail_extract.injured_party_last_name                AS injured_party_last_name,
		       work_claim_cms_detail_extract.injured_party_first_name               AS injured_party_first_name,
		       work_claim_cms_detail_extract.injured_party_middle_ini               AS injured_party_middle_ini,
		       work_claim_cms_detail_extract.injured_party_gender                   AS injured_party_gender,
		       work_claim_cms_detail_extract.injured_party_dob                      AS injured_party_dob,
		       work_claim_cms_detail_extract.reserved_11                            AS reserved_11,
		       work_claim_cms_detail_extract.cms_date_of_incident                   AS cms_date_of_incident,
		       work_claim_cms_detail_extract.industry_date_of_incident              AS industry_date_of_incident,
		       work_claim_cms_detail_extract.reserved_14                            AS reserved_14,
		       work_claim_cms_detail_extract.alleged_cause_of_injury                AS alleged_cause_of_injury,
		       work_claim_cms_detail_extract.reserved_16                            AS reserved_16,
		       work_claim_cms_detail_extract.state_of_venue                         AS state_of_venue,
		       work_claim_cms_detail_extract.reserved_18                            AS reserved_18,
		       work_claim_cms_detail_extract.diag_code_1                  AS diag_code_1,
		       work_claim_cms_detail_extract.reserved_20                            AS reserved_20,
		       work_claim_cms_detail_extract.diag_code_2                  AS diag_code_2,
		       work_claim_cms_detail_extract.reserved_22                            AS reserved_22,
		       work_claim_cms_detail_extract.diag_code_3                  AS diag_code_3,
		       work_claim_cms_detail_extract.reserved_24                            AS reserved_24,
		       work_claim_cms_detail_extract.diag_code_4                  AS diag_code_4,
		       work_claim_cms_detail_extract.reserved_26                            AS reserved_26,
		       work_claim_cms_detail_extract.diag_code_5                  AS diag_code_5,
		       work_claim_cms_detail_extract.reserved_28                            AS reserved_28,
		       work_claim_cms_detail_extract.diag_code_6                  AS diag_code_6,
		       work_claim_cms_detail_extract.reserved_30                            AS reserved_30,
		       work_claim_cms_detail_extract.diag_code_7                  AS diag_code_7,
		       work_claim_cms_detail_extract.reserved_32                            AS reserved_32,
		       work_claim_cms_detail_extract.diag_code_8                  AS diag_code_8,
		       work_claim_cms_detail_extract.reserved_34                            AS reserved_34,
		       work_claim_cms_detail_extract.diag_code_9                  AS diag_code_9,
		       work_claim_cms_detail_extract.reserved_36                            AS reserved_36,
		       work_claim_cms_detail_extract.diag_code_10                 AS diag_code_10,
		       work_claim_cms_detail_extract.reserved_38                            AS reserved_38,
		       work_claim_cms_detail_extract.diag_code_11                 AS diag_code_11,
		       work_claim_cms_detail_extract.reserved_40                            AS reserved_40,
		       work_claim_cms_detail_extract.diag_code_12                 AS diag_code_12,
		       work_claim_cms_detail_extract.reserved_42                            AS reserved_42,
		       work_claim_cms_detail_extract.diag_code_13                 AS diag_code_13,
		       work_claim_cms_detail_extract.reserved_44                            AS reserved_44,
		       work_claim_cms_detail_extract.diag_code_14                 AS diag_code_14,
		       work_claim_cms_detail_extract.reserved_46                            AS reserved_46,
		       work_claim_cms_detail_extract.diag_code_15                 AS diag_code_15,
		       work_claim_cms_detail_extract.reserved_48                            AS reserved_48,
		       work_claim_cms_detail_extract.diag_code_16                 AS diag_code_16,
		       work_claim_cms_detail_extract.reserved_50                            AS reserved_50,
		       work_claim_cms_detail_extract.diag_code_17                 AS diag_code_17,
		       work_claim_cms_detail_extract.reserved_52                            AS reserved_52,
		       work_claim_cms_detail_extract.diag_code_18                 AS diag_code_18,
		       work_claim_cms_detail_extract.reserved_54                            AS reserved_54,
		       work_claim_cms_detail_extract.diag_code_19                 AS diag_code_19,
		       work_claim_cms_detail_extract.reserved_56                            AS reserved_56,
		       work_claim_cms_detail_extract.descript_of_illness                    AS descript_of_illness,
		       work_claim_cms_detail_extract.product_liability_ind                  AS product_liability_ind,
		       work_claim_cms_detail_extract.product_generic_name                   AS product_generic_name,
		       work_claim_cms_detail_extract.product_brand_name                     AS product_brand_name,
		       work_claim_cms_detail_extract.product_manufacturer                   AS product_manufacturer,
		       work_claim_cms_detail_extract.product_alleged_harm                   AS product_alleged_harm,
		       work_claim_cms_detail_extract.reserved_63                            AS reserved_63,
		       work_claim_cms_detail_extract.self_insured_ind                       AS self_insured_ind,
		       work_claim_cms_detail_extract.self_insured_type                      AS self_insured_type,
		       work_claim_cms_detail_extract.policyholder_last_name                 AS policyholder_last_name,
		       work_claim_cms_detail_extract.policyholder_first_name                AS policyholder_first_name,
		       work_claim_cms_detail_extract.dba_name                               AS dba_name,
		       work_claim_cms_detail_extract.legal_name                             AS legal_name,
		       work_claim_cms_detail_extract.reserved_70                            AS reserved_70,
		       work_claim_cms_detail_extract.plan_insurance_type                    AS plan_insurance_type,
		       work_claim_cms_detail_extract.tin                                    AS tin,
		       work_claim_cms_detail_extract.office_code                            AS office_code,
		       work_claim_cms_detail_extract.policy_number                          AS policy_number,
		       work_claim_cms_detail_extract.claim_number                           AS claim_number,
		       work_claim_cms_detail_extract.plan_contact_department_name           AS plan_contact_department_name,
		       work_claim_cms_detail_extract.plan_contact_last_name                 AS plan_contact_last_name,
		       work_claim_cms_detail_extract.plan_contact_first_name                AS plan_contact_first_name,
		       work_claim_cms_detail_extract.plan_contact_ph                        AS plan_contact_ph,
		       work_claim_cms_detail_extract.plan_contact_ph_extension              AS plan_contact_ph_extension,
		       work_claim_cms_detail_extract.no_fault_insurance_limit               AS no_fault_insurance_limit,
		       work_claim_cms_detail_extract.exhaust_date_for_dollar_limit          AS exhaust_date_for_dollar_limit,
		       work_claim_cms_detail_extract.reserved_83                            AS reserved_83,
		       work_claim_cms_detail_extract.inj_party_rep_ind                      AS inj_party_rep_ind,
		       work_claim_cms_detail_extract.rep_last_name                          AS rep_last_name,
		       work_claim_cms_detail_extract.rep_first_name                         AS rep_first_name,
		       work_claim_cms_detail_extract.rep_firm_name                          AS rep_firm_name,
		       work_claim_cms_detail_extract.rep_tin                                AS rep_tin,
		       work_claim_cms_detail_extract.rep_mail_addr_1                        AS rep_mail_addr_1,
		       work_claim_cms_detail_extract.rep_mail_addr_2                        AS rep_mail_addr_2,
		       work_claim_cms_detail_extract.rep_city                               AS rep_city,
		       work_claim_cms_detail_extract.rep_state                              AS rep_state,
		       work_claim_cms_detail_extract.rep_mail_zip_code                      AS rep_mail_zip_code,
		       work_claim_cms_detail_extract.rep_mail_zip4                          AS rep_mail_zip4,
		       work_claim_cms_detail_extract.rep_ph                                 AS rep_ph,
		       work_claim_cms_detail_extract.rep_ph_extension                       AS rep_ph_extension,
		       work_claim_cms_detail_extract.reserved_97                            AS reserved_97,
		       work_claim_cms_detail_extract.orm_ind                                AS orm_ind,
		       work_claim_cms_detail_extract.orm_termination_date                   AS orm_termination_date,
		       work_claim_cms_detail_extract.tpoc_date                              AS tpoc_date,
		       work_claim_cms_detail_extract.tpoc_amount                            AS tpoc_amount,
		       work_claim_cms_detail_extract.funding_delayed_beyond_tpoc_start_date AS funding_delayed_beyond_tpoc_start_date,
		       work_claim_cms_detail_extract.reserved_103                           AS reserved_103,
		       work_claim_cms_detail_extract.c1_relationship                        AS c1_relationship,
		       work_claim_cms_detail_extract.c1_tin                                 AS c1_tin,
		       work_claim_cms_detail_extract.c1_last_name                           AS c1_last_name,
		       work_claim_cms_detail_extract.c1_first_name                          AS c1_first_name,
		       work_claim_cms_detail_extract.c1_middle_initial                      AS c1_middle_initial,
		       work_claim_cms_detail_extract.c1_mail_addr_1                         AS c1_mail_addr_1,
		       work_claim_cms_detail_extract.c1_mail_addr_2                         AS c1_mail_addr_2,
		       work_claim_cms_detail_extract.c1_city                                AS c1_city,
		       work_claim_cms_detail_extract.c1_state                               AS c1_state,
		       work_claim_cms_detail_extract.c1_zip                                 AS c1_zip,
		       work_claim_cms_detail_extract.c1_zip4                                AS c1_zip4,
		       work_claim_cms_detail_extract.c1_ph                                  AS c1_ph,
		       work_claim_cms_detail_extract.c1_ph_extension                        AS c1_ph_extension,
		       work_claim_cms_detail_extract.reserved_117                           AS reserved_117,
		       work_claim_cms_detail_extract.c1_rep_ind                             AS c1_rep_ind,
		       work_claim_cms_detail_extract.c1_rep_last_name                       AS c1_rep_last_name,
		       work_claim_cms_detail_extract.c1_rep_first_name                      AS c1_rep_first_name,
		       work_claim_cms_detail_extract.c1_rep_firm_name                       AS c1_rep_firm_name,
		       work_claim_cms_detail_extract.c1_rep_tin                             AS c1_rep_tin,
		       work_claim_cms_detail_extract.c1_rep_mail_addr_1                     AS c1_rep_mail_addr_1,
		       work_claim_cms_detail_extract.c1_rep_mail_addr_2                     AS c1_rep_mail_addr_2,
		       work_claim_cms_detail_extract.c1_rep_mail_city                       AS c1_rep_mail_city,
		       work_claim_cms_detail_extract.c1_rep_state                           AS c1_rep_state,
		       work_claim_cms_detail_extract.c1_rep_zip                             AS c1_rep_zip,
		       work_claim_cms_detail_extract.c1_rep_zip4                            AS c1_rep_zip4,
		       work_claim_cms_detail_extract.c1_rep_ph                              AS c1_rep_ph,
		       work_claim_cms_detail_extract.c1_rep_ph_extension                    AS c1_rep_ph_extension,
		       work_claim_cms_detail_extract.reserved_131                           AS reserved_131,
		       work_claim_cms_detail_extract.created_date                           AS created_date,
		       work_claim_cms_detail_extract.modified_date                          AS modified_date,
		       work_claim_cms_detail_extract.audit_id                               AS audit_id,
		       Rtrim(work_claim_cms_detail_extract.dcn)                             AS dcn
		FROM   work_claim_cms_detail_extract
		WHERE  audit_id <> @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
		       AND ( reject_flag IS NULL
		              OR reject_flag = 'false' )
		ORDER  BY created_date ASC --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY dcn ORDER BY work_claim_cms_detail_extract_id DESC) = 1
),
EXP_ActionType_UPDATE AS (
	SELECT
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_med_ak_id,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_occurrence_ak_id,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_med_plan_ak_id,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_occurrence_ak_id,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_ak_id,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_med_patient_diag_add_ak_id,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_med_ak_id5 AS claim_med_ak_id1,
	RTR_Add_Update_Delete_Action_Type_UPDATE.record_identifier,
	RTR_Add_Update_Delete_Action_Type_UPDATE.cms_document_cntl_num,
	RTR_Add_Update_Delete_Action_Type_UPDATE.action_type,
	RTR_Add_Update_Delete_Action_Type_UPDATE.medicare_hicn,
	RTR_Add_Update_Delete_Action_Type_UPDATE.tax_ssn_id,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_last_name,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_first_name,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_gndr,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_birthdate,
	RTR_Add_Update_Delete_Action_Type_UPDATE.cms_incdnt_date,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_loss_date,
	RTR_Add_Update_Delete_Action_Type_UPDATE.patient_cause_code,
	RTR_Add_Update_Delete_Action_Type_UPDATE.state_venue,
	RTR_Add_Update_Delete_Action_Type_UPDATE.patient_diag_code25 AS patient_diag_code,
	RTR_Add_Update_Delete_Action_Type_UPDATE.patient_diag_code26 AS patient_diag_code2,
	RTR_Add_Update_Delete_Action_Type_UPDATE.patient_diag_code3,
	RTR_Add_Update_Delete_Action_Type_UPDATE.patient_diag_code AS patient_diag_code4,
	RTR_Add_Update_Delete_Action_Type_UPDATE.patient_diag_code5,
	RTR_Add_Update_Delete_Action_Type_UPDATE.patient_diag_code6,
	RTR_Add_Update_Delete_Action_Type_UPDATE.patient_diag_code7,
	RTR_Add_Update_Delete_Action_Type_UPDATE.patient_diag_code8,
	RTR_Add_Update_Delete_Action_Type_UPDATE.patient_diag_code9,
	RTR_Add_Update_Delete_Action_Type_UPDATE.patient_diag_code10,
	RTR_Add_Update_Delete_Action_Type_UPDATE.patient_diag_code11,
	RTR_Add_Update_Delete_Action_Type_UPDATE.patient_diag_code12,
	RTR_Add_Update_Delete_Action_Type_UPDATE.patient_diag_code13,
	RTR_Add_Update_Delete_Action_Type_UPDATE.patient_diag_code1 AS patient_diag_code14,
	RTR_Add_Update_Delete_Action_Type_UPDATE.patient_diag_code15,
	RTR_Add_Update_Delete_Action_Type_UPDATE.patient_diag_code16,
	RTR_Add_Update_Delete_Action_Type_UPDATE.patient_diag_code17,
	RTR_Add_Update_Delete_Action_Type_UPDATE.patient_diag_code18,
	RTR_Add_Update_Delete_Action_Type_UPDATE.patient_diag_code19,
	RTR_Add_Update_Delete_Action_Type_UPDATE.prdct_liab_ind,
	RTR_Add_Update_Delete_Action_Type_UPDATE.prdct_generic_name,
	RTR_Add_Update_Delete_Action_Type_UPDATE.prdct_brand_name,
	RTR_Add_Update_Delete_Action_Type_UPDATE.prdct_mfr,
	RTR_Add_Update_Delete_Action_Type_UPDATE.prdct_alleged_harm,
	RTR_Add_Update_Delete_Action_Type_UPDATE.self_insd_ind,
	RTR_Add_Update_Delete_Action_Type_UPDATE.self_insd_type,
	RTR_Add_Update_Delete_Action_Type_UPDATE.self_insd_last_name,
	RTR_Add_Update_Delete_Action_Type_UPDATE.self_insd_first_name,
	RTR_Add_Update_Delete_Action_Type_UPDATE.self_insd_dba_name,
	RTR_Add_Update_Delete_Action_Type_UPDATE.self_insd_lgl_name,
	RTR_Add_Update_Delete_Action_Type_UPDATE.wbmi_plan_ins_type,
	RTR_Add_Update_Delete_Action_Type_UPDATE.pol_key,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_occurrence_key,
	RTR_Add_Update_Delete_Action_Type_UPDATE.no_fault_ins_lmt,
	RTR_Add_Update_Delete_Action_Type_UPDATE.exhaust_lmt_date,
	RTR_Add_Update_Delete_Action_Type_UPDATE.injured_party_rep_Ind_MIJA,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_last_name_MIJA,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_first_name_MIJA,
	RTR_Add_Update_Delete_Action_Type_UPDATE.tax_id_MIJA,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_addr_MIJA,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_city_MIJA,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_state_MIJA,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_zip_MIJA,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_zip4_MIJA,
	RTR_Add_Update_Delete_Action_Type_UPDATE.ph_num_MIJA,
	RTR_Add_Update_Delete_Action_Type_UPDATE.ph_extension_MIJA,
	RTR_Add_Update_Delete_Action_Type_UPDATE.med_obligation_to_claimant,
	RTR_Add_Update_Delete_Action_Type_UPDATE.orm_termination_date,
	RTR_Add_Update_Delete_Action_Type_UPDATE.tpoc_date,
	RTR_Add_Update_Delete_Action_Type_UPDATE.tpoc_amt,
	RTR_Add_Update_Delete_Action_Type_UPDATE.tpoc_fund_delay_date,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claimant_1_Ind_MCT1,
	RTR_Add_Update_Delete_Action_Type_UPDATE.tax_id_MCT1,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_last_name_MCT1,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_first_name_MCT1,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_mid_name_MCT1,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_addr_MCT1,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_city_MCT1,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_state_MCT1,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_zip_MCT1,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_zip4_MCT1,
	RTR_Add_Update_Delete_Action_Type_UPDATE.ph_num_MCT1,
	RTR_Add_Update_Delete_Action_Type_UPDATE.ph_extension_MCT1,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claimant_1_rep_Ind_MCA1,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_last_name_MCA1,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_first_name_MCA1,
	RTR_Add_Update_Delete_Action_Type_UPDATE.tax_id_MCA1,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_addr_MCA1,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_city_MCA1,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_state_MCA1,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_zip_MCA1,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claim_party_zip4_MCA1,
	RTR_Add_Update_Delete_Action_Type_UPDATE.ph_num_MCA1,
	RTR_Add_Update_Delete_Action_Type_UPDATE.ph_extension_MCA1,
	RTR_Add_Update_Delete_Action_Type_UPDATE.injured_party_rep_firm AS injured_party_rep_firm_MIJA,
	RTR_Add_Update_Delete_Action_Type_UPDATE.claimant1_rep_firm AS claimant1_rep_firm_MCA1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(record_identifier)
	-- 
	-- 
	UDF_DEFAULT_VALUE_TO_BLANKS(record_identifier) AS record_identifier1,
	cms_document_cntl_num AS cms_document_cntl_num1,
	-- *INF*: IIF(ISNULL(action_type),'',
	-- IIF(action_type='3','0',action_type))
	-- 
	-- --action type 3 means ADDDELETE so we need to convert it to 0 (means ADD) as CMS doesn't allow value of 3
	IFF(action_type IS NULL, '', IFF(
	        action_type = '3', '0', action_type
	    )) AS action_type1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(rtrim(ltrim(medicare_hicn)))
	UDF_DEFAULT_VALUE_TO_BLANKS(rtrim(ltrim(medicare_hicn))) AS medicare_hicn1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(tax_ssn_id)
	UDF_DEFAULT_VALUE_TO_BLANKS(tax_ssn_id) AS tax_ssn_id1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_last_name)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_last_name) AS claim_party_last_name1,
	-- *INF*: :UDF.REPLACE_NON_ALPHA_WITH_BLANKS(:UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_first_name))
	UDF_REPLACE_NON_ALPHA_WITH_BLANKS(UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_first_name)) AS claim_party_first_name1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_gndr)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_gndr) AS claim_party_gndr1,
	-- *INF*: IIF(claim_party_birthdate='99991231' OR claim_party_birthdate='21001231' ,'00000000',claim_party_birthdate)
	-- 
	IFF(
	    claim_party_birthdate = '99991231' OR claim_party_birthdate = '21001231', '00000000',
	    claim_party_birthdate
	) AS claim_party_birthdate1,
	-- *INF*: IIF(cms_incdnt_date='18000101','00000000',cms_incdnt_date)
	IFF(cms_incdnt_date = '18000101', '00000000', cms_incdnt_date) AS cms_incdnt_date1,
	-- *INF*: IIF(claim_loss_date='18000101','00000000',claim_loss_date)
	IFF(claim_loss_date = '18000101', '00000000', claim_loss_date) AS claim_loss_date1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_cause_code)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_cause_code) AS patient_cause_code1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(state_venue)
	UDF_DEFAULT_VALUE_TO_BLANKS(state_venue) AS state_venue1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code) AS patient_diagnosis_code1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code2)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code2) AS diag_code_21,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code3)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code3) AS diag_code_31,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code4)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code4) AS diag_code_41,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code5)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code5) AS diag_code_51,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code6)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code6) AS diag_code_61,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code7)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code7) AS diag_code_71,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code8)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code8) AS diag_code_81,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code9)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code9) AS diag_code_91,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code10)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code10) AS diag_code_101,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code11)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code11) AS diag_code_111,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code12)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code12) AS diag_code_121,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code13)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code13) AS diag_code_131,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code14)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code14) AS diag_code_141,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code15)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code15) AS diag_code_151,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code16)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code16) AS diag_code_161,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code17)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code17) AS diag_code_171,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code18)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code18) AS diag_code_181,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(patient_diag_code19)
	UDF_DEFAULT_VALUE_TO_BLANKS(patient_diag_code19) AS diag_code_191,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(prdct_liab_ind)
	UDF_DEFAULT_VALUE_TO_BLANKS(prdct_liab_ind) AS prdct_liab_ind1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(prdct_generic_name)
	UDF_DEFAULT_VALUE_TO_BLANKS(prdct_generic_name) AS prdct_generic_name1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(prdct_brand_name)
	UDF_DEFAULT_VALUE_TO_BLANKS(prdct_brand_name) AS prdct_brand_name1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(prdct_mfr)
	UDF_DEFAULT_VALUE_TO_BLANKS(prdct_mfr) AS prdct_mfr1,
	-- *INF*: REPLACECHR(1,REPLACECHR(1,prdct_alleged_harm,chr(10),'') ,chr(13),'') 
	-- 
	-- --this to remove new line characters
	REGEXP_REPLACE(REGEXP_REPLACE(prdct_alleged_harm,chr(10),''),chr(13),'') AS v_prdct_alleged_harm1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(v_prdct_alleged_harm1)
	UDF_DEFAULT_VALUE_TO_BLANKS(v_prdct_alleged_harm1) AS prdct_alleged_harm1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(self_insd_ind)
	UDF_DEFAULT_VALUE_TO_BLANKS(self_insd_ind) AS self_insd_ind1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(self_insd_type)
	UDF_DEFAULT_VALUE_TO_BLANKS(self_insd_type) AS self_insd_type1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(self_insd_last_name)
	UDF_DEFAULT_VALUE_TO_BLANKS(self_insd_last_name) AS self_insd_last_name1,
	-- *INF*: :UDF.REPLACE_NON_ALPHA_WITH_BLANKS(:UDF.DEFAULT_VALUE_TO_BLANKS(self_insd_first_name))
	UDF_REPLACE_NON_ALPHA_WITH_BLANKS(UDF_DEFAULT_VALUE_TO_BLANKS(self_insd_first_name)) AS self_insd_first_name1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(self_insd_dba_name)
	UDF_DEFAULT_VALUE_TO_BLANKS(self_insd_dba_name) AS self_insd_dba_name1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(self_insd_lgl_name)
	UDF_DEFAULT_VALUE_TO_BLANKS(self_insd_lgl_name) AS self_insd_lgl_name1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(wbmi_plan_ins_type)
	UDF_DEFAULT_VALUE_TO_BLANKS(wbmi_plan_ins_type) AS wbmi_plan_ins_type1,
	pol_key AS pol_key1,
	claim_occurrence_key AS claim_occurrence_key1,
	-- *INF*: IIF(wbmi_plan_ins_type='E' OR wbmi_plan_ins_type='L', '00000000000',
	-- IIF(no_fault_ins_lmt=0,'99999999999',
	-- 	lpad(TO_CHAR(ROUND(no_fault_ins_lmt,2)*100),11,'0')))
	-- 
	-- 
	-- 
	IFF(
	    wbmi_plan_ins_type = 'E' OR wbmi_plan_ins_type = 'L', '00000000000',
	    IFF(
	        no_fault_ins_lmt = 0, '99999999999',
	        lpad(TO_CHAR(ROUND(no_fault_ins_lmt, 2) * 100), 11, '0')
	    )
	) AS no_fault_ins_lmt1,
	-- *INF*: IIF(wbmi_plan_ins_type='E' OR wbmi_plan_ins_type='L', '00000000',
	-- IIF(exhaust_lmt_date='18000101','00000000',
	-- 	exhaust_lmt_date))
	IFF(
	    wbmi_plan_ins_type = 'E' OR wbmi_plan_ins_type = 'L', '00000000',
	    IFF(
	        exhaust_lmt_date = '18000101', '00000000', exhaust_lmt_date
	    )
	) AS exhaust_lmt_date1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(injured_party_rep_Ind_MIJA)
	UDF_DEFAULT_VALUE_TO_BLANKS(injured_party_rep_Ind_MIJA) AS injured_party_rep_Ind_MIJA1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_last_name_MIJA)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_last_name_MIJA) AS claim_party_last_name_MIJA1,
	-- *INF*: :UDF.REPLACE_NON_ALPHA_WITH_BLANKS(:UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_first_name_MIJA))
	UDF_REPLACE_NON_ALPHA_WITH_BLANKS(UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_first_name_MIJA)) AS claim_party_first_name_MIJA1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(injured_party_rep_firm_MIJA)
	UDF_DEFAULT_VALUE_TO_BLANKS(injured_party_rep_firm_MIJA) AS injured_party_rep_firm_MIJA1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(tax_id_MIJA)
	-- 
	-- 
	-- 
	UDF_DEFAULT_VALUE_TO_BLANKS(tax_id_MIJA) AS tax_id_MIJA1,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MIJA)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_addr_MIJA))
	-- 
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MIJA) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_addr_MIJA)
	) AS claim_party_addr_MIJA1,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MIJA)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(substr(claim_party_addr_MIJA ,51)))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MIJA) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(substr(claim_party_addr_MIJA, 51))
	) AS claim_party_addr_Ln2_MIJA1,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MIJA)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_city_MIJA))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MIJA) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_city_MIJA)
	) AS claim_party_city_MIJA1,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MIJA)='FC','FC',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_state_MIJA))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MIJA) = 'FC', 'FC',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_state_MIJA)
	) AS claim_party_state_MIJA1,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MIJA)='FC','0000000000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(claim_party_zip_MIJA))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MIJA) = 'FC', '0000000000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(claim_party_zip_MIJA)
	) AS claim_party_zip_MIJA1,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MIJA)='FC','0000000000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(claim_party_zip4_MIJA))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MIJA) = 'FC', '0000000000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(claim_party_zip4_MIJA)
	) AS claim_party_zip4_MIJA1,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MIJA)='FC','0000000000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(ph_num_MIJA))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MIJA) = 'FC', '0000000000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(ph_num_MIJA)
	) AS ph_num_MIJA1,
	-- *INF*: IIF(:UDF.DEFAULT_VALUE_TO_BLANKS(injured_party_rep_Ind_MIJA)='','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(ph_extension_MIJA))
	IFF(
	    UDF_DEFAULT_VALUE_TO_BLANKS(injured_party_rep_Ind_MIJA) = '', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(ph_extension_MIJA)
	) AS ph_extension_MIJA1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(med_obligation_to_claimant)
	UDF_DEFAULT_VALUE_TO_BLANKS(med_obligation_to_claimant) AS med_obligation_to_claimant1,
	-- *INF*: concat(CONCAT(GET_DATE_PART(ADD_TO_DATE(SYSDATE,'MM',6),'YYYY'), 
	-- LPAD(GET_DATE_PART(ADD_TO_DATE(SYSDATE,'MM',6),'MM'),2,'0')
	-- ),'08')
	concat(CONCAT(DATE_PART(DATEADD(MONTH,6,CURRENT_TIMESTAMP), 'YYYY'), LPAD(DATE_PART(DATEADD(MONTH,6,CURRENT_TIMESTAMP), 'MM'), 2, '0')), '08') AS var_orm_checkdate,
	-- *INF*: IIF(orm_termination_date='18000101','00000000',IIF( TO_DATE( orm_termination_date,'YYYYMMDD') > TO_DATE(var_orm_checkdate,'YYYYMMDD'),'0000000000',orm_termination_date))
	IFF(
	    orm_termination_date = '18000101', '00000000',
	    IFF(
	        TO_TIMESTAMP(orm_termination_date, 'YYYYMMDD') > TO_TIMESTAMP(var_orm_checkdate, 'YYYYMMDD'),
	        '0000000000',
	        orm_termination_date
	    )
	) AS orm_termination_date1,
	-- *INF*: IIF(ISNULL(tpoc_date) OR tpoc_date='18000101','00000000',tpoc_date)
	IFF(tpoc_date IS NULL OR tpoc_date = '18000101', '00000000', tpoc_date) AS tpoc_date1,
	-- *INF*: IIF(ISNULL(tpoc_amt) OR tpoc_amt=0,'00000000000',lpad(TO_CHAR(ROUND(tpoc_amt,2)*100),11,'0'))
	IFF(
	    tpoc_amt IS NULL OR tpoc_amt = 0, '00000000000',
	    lpad(TO_CHAR(ROUND(tpoc_amt, 2) * 100), 11, '0')
	) AS tpoc_amt1,
	-- *INF*: IIF(ISNULL(tpoc_fund_delay_date) OR tpoc_fund_delay_date='18000101','00000000',tpoc_fund_delay_date)
	IFF(
	    tpoc_fund_delay_date IS NULL OR tpoc_fund_delay_date = '18000101', '00000000',
	    tpoc_fund_delay_date
	) AS tpoc_fund_delay_date1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claimant_1_Ind_MCT1)
	UDF_DEFAULT_VALUE_TO_BLANKS(claimant_1_Ind_MCT1) AS claimant_1_Ind_MCT11,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(tax_id_MCT1)
	UDF_DEFAULT_VALUE_TO_BLANKS(tax_id_MCT1) AS tax_id_MCT11,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_last_name_MCT1)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_last_name_MCT1) AS claim_party_last_name_MCT11,
	-- *INF*: :UDF.REPLACE_NON_ALPHA_WITH_BLANKS(:UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_first_name_MCT1))
	UDF_REPLACE_NON_ALPHA_WITH_BLANKS(UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_first_name_MCT1)) AS claim_party_first_name_MCT11,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_mid_name_MCT1)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_mid_name_MCT1) AS claim_party_mid_name_MCT11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT1)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_addr_MCT1))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT1) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_addr_MCT1)
	) AS claim_party_addr_MCT11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT1)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(substr(claim_party_addr_MCT1,51)))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT1) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(substr(claim_party_addr_MCT1, 51))
	) AS claim_party_addr_Ln2_MCT11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT1)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_city_MCT1))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT1) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_city_MCT1)
	) AS claim_party_city_MCT11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT1)='FC','FC',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_state_MCT1))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT1) = 'FC', 'FC',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_state_MCT1)
	) AS claim_party_state_MCT11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT1)='FC','00000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(claim_party_zip_MCT1))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT1) = 'FC', '00000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(claim_party_zip_MCT1)
	) AS claim_party_zip_MCT11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT1)='FC','0000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(claim_party_zip4_MCT1))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT1) = 'FC', '0000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(claim_party_zip4_MCT1)
	) AS claim_party_zip4_MCT11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT1)='FC','0000000000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(ph_num_MCT1))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT1) = 'FC', '0000000000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(ph_num_MCT1)
	) AS ph_num_MCT11,
	-- *INF*: IIF(:UDF.DEFAULT_VALUE_TO_BLANKS(claimant_1_Ind_MCT1)='','',:UDF.DEFAULT_VALUE_TO_BLANKS(ph_extension_MCT1))
	IFF(
	    UDF_DEFAULT_VALUE_TO_BLANKS(claimant_1_Ind_MCT1) = '', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(ph_extension_MCT1)
	) AS ph_extension_MCT11,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claimant_1_rep_Ind_MCA1)
	UDF_DEFAULT_VALUE_TO_BLANKS(claimant_1_rep_Ind_MCA1) AS claimant_1_rep_Ind_MCA11,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_last_name_MCA1)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_last_name_MCA1) AS claim_party_last_name_MCA11,
	-- *INF*: :UDF.REPLACE_NON_ALPHA_WITH_BLANKS(:UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_first_name_MCA1))
	UDF_REPLACE_NON_ALPHA_WITH_BLANKS(UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_first_name_MCA1)) AS claim_party_first_name_MCA11,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claimant1_rep_firm_MCA1)
	UDF_DEFAULT_VALUE_TO_BLANKS(claimant1_rep_firm_MCA1) AS claimant1_rep_firm_MCA11,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(tax_id_MCA1)
	UDF_DEFAULT_VALUE_TO_BLANKS(tax_id_MCA1) AS tax_id_MCA11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA1)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_addr_MCA1))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA1) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_addr_MCA1)
	) AS claim_party_addr_MCA11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA1)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(substr(claim_party_addr_MCA1,51)))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA1) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(substr(claim_party_addr_MCA1, 51))
	) AS claim_party_addr_Ln2_MCA11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA1)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_city_MCA1))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA1) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_city_MCA1)
	) AS claim_party_city_MCA11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA1)='FC','FC',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_state_MCA1))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA1) = 'FC', 'FC',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_state_MCA1)
	) AS claim_party_state_MCA11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA1)='FC','00000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(claim_party_zip_MCA1))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA1) = 'FC', '00000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(claim_party_zip_MCA1)
	) AS claim_party_zip_MCA11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA1)='FC','0000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(claim_party_zip4_MCA1))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA1) = 'FC', '0000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(claim_party_zip4_MCA1)
	) AS claim_party_zip4_MCA11,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA1)='FC','0000000000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(ph_num_MCA1))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA1) = 'FC', '0000000000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(ph_num_MCA1)
	) AS ph_num_MCA11,
	-- *INF*: IIF(:UDF.DEFAULT_VALUE_TO_BLANKS(claimant_1_rep_Ind_MCA1)='','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(ph_extension_MCA1))
	IFF(
	    UDF_DEFAULT_VALUE_TO_BLANKS(claimant_1_rep_Ind_MCA1) = '', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(ph_extension_MCA1)
	) AS ph_extension_MCA11,
	'' AS DEFAULT_BLANKS,
	1 AS DEFAULT_INTEGER_1,
	'0000000000' AS DEFAULT_ZEROS,
	SYSDATE AS CURRENT_DATE,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID,
	LKP_work_claim_cms_detail_extract_Update_Keys_Only.injured_party_hicn AS LKP_Hicn,
	-- *INF*: :LKP.LKP_SUP_CMS_TIN_OFFICE_TIN_BY_DUMMY_ID(1)
	LKP_SUP_CMS_TIN_OFFICE_TIN_BY_DUMMY_ID_1.office_tin_num AS LKP_Office_Tin,
	LKP_work_claim_cms_detail_extract_Update_Keys_Only.injured_party_ssn AS LKP_injured_party_ssn,
	-- *INF*: IIF(:UDF.DEFAULT_VALUE_TO_BLANKS(LKP_Hicn)='',LKP_injured_party_ssn,'')
	-- 
	IFF(UDF_DEFAULT_VALUE_TO_BLANKS(LKP_Hicn) = '', LKP_injured_party_ssn, '') AS injured_party_ssn_out,
	LKP_work_claim_cms_detail_extract_Update_Keys_Only.cms_date_of_incident AS LKP_cms_date_of_incident,
	LKP_work_claim_cms_detail_extract_Update_Keys_Only.orm_ind AS LKP_orm_ind,
	RTR_Add_Update_Delete_Action_Type_UPDATE.last_cms_hicn AS last_cms_hicn4,
	RTR_Add_Update_Delete_Action_Type_UPDATE.ICDCodeVersion
	FROM RTR_Add_Update_Delete_Action_Type_UPDATE
	LEFT JOIN LKP_work_claim_cms_detail_extract_Update_Keys_Only
	ON LKP_work_claim_cms_detail_extract_Update_Keys_Only.dcn = RTR_Add_Update_Delete.cms_document_cntl_num4
	LEFT JOIN LKP_SUP_CMS_TIN_OFFICE_TIN_BY_DUMMY_ID LKP_SUP_CMS_TIN_OFFICE_TIN_BY_DUMMY_ID_1
	ON LKP_SUP_CMS_TIN_OFFICE_TIN_BY_DUMMY_ID_1.dummy_integer = 1

),
work_claim_cms_detail_extract_UPDATE AS (
	INSERT INTO work_claim_cms_detail_extract
	(claim_med_ak_id, claim_party_occurrence_ak_id, claim_med_plan_ak_id, claim_occurrence_ak_id, claim_party_ak_id, claim_med_patient_diag_add_ak_id, record_identifier, dcn, action_type, injured_party_hicn, injured_party_ssn, injured_party_last_name, injured_party_first_name, injured_party_middle_ini, injured_party_gender, injured_party_dob, reserved_11, cms_date_of_incident, industry_date_of_incident, reserved_14, alleged_cause_of_injury, reserved_16, state_of_venue, icd_indicator, diag_code_1, reserved_20, diag_code_2, reserved_22, diag_code_3, reserved_24, diag_code_4, reserved_26, diag_code_5, reserved_28, diag_code_6, reserved_30, diag_code_7, reserved_32, diag_code_8, reserved_34, diag_code_9, reserved_36, diag_code_10, reserved_38, diag_code_11, reserved_40, diag_code_12, reserved_42, diag_code_13, reserved_44, diag_code_14, reserved_46, diag_code_15, reserved_48, diag_code_16, reserved_50, diag_code_17, reserved_52, diag_code_18, reserved_54, diag_code_19, reserved_56, descript_of_illness, product_liability_ind, product_generic_name, product_brand_name, product_manufacturer, product_alleged_harm, reserved_63, self_insured_ind, self_insured_type, policyholder_last_name, policyholder_first_name, dba_name, legal_name, reserved_70, plan_insurance_type, tin, office_code, policy_number, claim_number, plan_contact_department_name, plan_contact_last_name, plan_contact_first_name, plan_contact_ph, plan_contact_ph_extension, no_fault_insurance_limit, exhaust_date_for_dollar_limit, reserved_83, inj_party_rep_ind, rep_last_name, rep_first_name, rep_firm_name, rep_tin, rep_mail_addr_1, rep_mail_addr_2, rep_city, rep_state, rep_mail_zip_code, rep_mail_zip4, rep_ph, rep_ph_extension, reserved_97, orm_ind, orm_termination_date, tpoc_date, tpoc_amount, funding_delayed_beyond_tpoc_start_date, reserved_103, c1_relationship, c1_tin, c1_last_name, c1_first_name, c1_middle_initial, c1_mail_addr_1, c1_mail_addr_2, c1_city, c1_state, c1_zip, c1_zip4, c1_ph, c1_ph_extension, reserved_117, c1_rep_ind, c1_rep_last_name, c1_rep_first_name, c1_rep_firm_name, c1_rep_tin, c1_rep_mail_addr_1, c1_rep_mail_addr_2, c1_rep_mail_city, c1_rep_state, c1_rep_zip, c1_rep_zip4, c1_rep_ph, c1_rep_ph_extension, reserved_131, created_date, modified_date, audit_id)
	SELECT 
	CLAIM_MED_AK_ID, 
	CLAIM_PARTY_OCCURRENCE_AK_ID, 
	CLAIM_MED_PLAN_AK_ID, 
	CLAIM_OCCURRENCE_AK_ID, 
	CLAIM_PARTY_AK_ID, 
	CLAIM_MED_PATIENT_DIAG_ADD_AK_ID, 
	record_identifier1 AS RECORD_IDENTIFIER, 
	cms_document_cntl_num1 AS DCN, 
	action_type1 AS ACTION_TYPE, 
	LKP_Hicn AS INJURED_PARTY_HICN, 
	injured_party_ssn_out AS INJURED_PARTY_SSN, 
	claim_party_last_name1 AS INJURED_PARTY_LAST_NAME, 
	claim_party_first_name1 AS INJURED_PARTY_FIRST_NAME, 
	DEFAULT_BLANKS AS INJURED_PARTY_MIDDLE_INI, 
	claim_party_gndr1 AS INJURED_PARTY_GENDER, 
	claim_party_birthdate1 AS INJURED_PARTY_DOB, 
	DEFAULT_BLANKS AS RESERVED_11, 
	LKP_cms_date_of_incident AS CMS_DATE_OF_INCIDENT, 
	claim_loss_date1 AS INDUSTRY_DATE_OF_INCIDENT, 
	DEFAULT_BLANKS AS RESERVED_14, 
	patient_cause_code1 AS ALLEGED_CAUSE_OF_INJURY, 
	DEFAULT_BLANKS AS RESERVED_16, 
	state_venue1 AS STATE_OF_VENUE, 
	ICDCodeVersion AS ICD_INDICATOR, 
	patient_diagnosis_code1 AS DIAG_CODE_1, 
	DEFAULT_BLANKS AS RESERVED_20, 
	diag_code_21 AS DIAG_CODE_2, 
	DEFAULT_BLANKS AS RESERVED_22, 
	diag_code_31 AS DIAG_CODE_3, 
	DEFAULT_BLANKS AS RESERVED_24, 
	diag_code_41 AS DIAG_CODE_4, 
	DEFAULT_BLANKS AS RESERVED_26, 
	diag_code_51 AS DIAG_CODE_5, 
	DEFAULT_BLANKS AS RESERVED_28, 
	diag_code_61 AS DIAG_CODE_6, 
	DEFAULT_BLANKS AS RESERVED_30, 
	diag_code_71 AS DIAG_CODE_7, 
	DEFAULT_BLANKS AS RESERVED_32, 
	diag_code_81 AS DIAG_CODE_8, 
	DEFAULT_BLANKS AS RESERVED_34, 
	diag_code_91 AS DIAG_CODE_9, 
	DEFAULT_BLANKS AS RESERVED_36, 
	diag_code_101 AS DIAG_CODE_10, 
	DEFAULT_BLANKS AS RESERVED_38, 
	diag_code_111 AS DIAG_CODE_11, 
	DEFAULT_BLANKS AS RESERVED_40, 
	diag_code_121 AS DIAG_CODE_12, 
	DEFAULT_BLANKS AS RESERVED_42, 
	diag_code_131 AS DIAG_CODE_13, 
	DEFAULT_BLANKS AS RESERVED_44, 
	diag_code_141 AS DIAG_CODE_14, 
	DEFAULT_BLANKS AS RESERVED_46, 
	diag_code_151 AS DIAG_CODE_15, 
	DEFAULT_BLANKS AS RESERVED_48, 
	diag_code_161 AS DIAG_CODE_16, 
	DEFAULT_BLANKS AS RESERVED_50, 
	diag_code_171 AS DIAG_CODE_17, 
	DEFAULT_BLANKS AS RESERVED_52, 
	diag_code_181 AS DIAG_CODE_18, 
	DEFAULT_BLANKS AS RESERVED_54, 
	diag_code_191 AS DIAG_CODE_19, 
	DEFAULT_BLANKS AS RESERVED_56, 
	DEFAULT_BLANKS AS DESCRIPT_OF_ILLNESS, 
	DEFAULT_BLANKS AS PRODUCT_LIABILITY_IND, 
	DEFAULT_BLANKS AS PRODUCT_GENERIC_NAME, 
	DEFAULT_BLANKS AS PRODUCT_BRAND_NAME, 
	DEFAULT_BLANKS AS PRODUCT_MANUFACTURER, 
	DEFAULT_BLANKS AS PRODUCT_ALLEGED_HARM, 
	DEFAULT_BLANKS AS RESERVED_63, 
	self_insd_ind1 AS SELF_INSURED_IND, 
	self_insd_type1 AS SELF_INSURED_TYPE, 
	self_insd_last_name1 AS POLICYHOLDER_LAST_NAME, 
	self_insd_first_name1 AS POLICYHOLDER_FIRST_NAME, 
	self_insd_dba_name1 AS DBA_NAME, 
	self_insd_lgl_name1 AS LEGAL_NAME, 
	DEFAULT_BLANKS AS RESERVED_70, 
	wbmi_plan_ins_type1 AS PLAN_INSURANCE_TYPE, 
	LKP_Office_Tin AS TIN, 
	DEFAULT_BLANKS AS OFFICE_CODE, 
	pol_key1 AS POLICY_NUMBER, 
	claim_occurrence_key1 AS CLAIM_NUMBER, 
	DEFAULT_BLANKS AS PLAN_CONTACT_DEPARTMENT_NAME, 
	DEFAULT_BLANKS AS PLAN_CONTACT_LAST_NAME, 
	DEFAULT_BLANKS AS PLAN_CONTACT_FIRST_NAME, 
	DEFAULT_ZEROS AS PLAN_CONTACT_PH, 
	DEFAULT_BLANKS AS PLAN_CONTACT_PH_EXTENSION, 
	no_fault_ins_lmt1 AS NO_FAULT_INSURANCE_LIMIT, 
	exhaust_lmt_date1 AS EXHAUST_DATE_FOR_DOLLAR_LIMIT, 
	DEFAULT_BLANKS AS RESERVED_83, 
	injured_party_rep_Ind_MIJA1 AS INJ_PARTY_REP_IND, 
	claim_party_last_name_MIJA1 AS REP_LAST_NAME, 
	claim_party_first_name_MIJA1 AS REP_FIRST_NAME, 
	injured_party_rep_firm_MIJA1 AS REP_FIRM_NAME, 
	tax_id_MIJA1 AS REP_TIN, 
	claim_party_addr_MIJA1 AS REP_MAIL_ADDR_1, 
	claim_party_addr_Ln2_MIJA1 AS REP_MAIL_ADDR_2, 
	claim_party_city_MIJA1 AS REP_CITY, 
	claim_party_state_MIJA1 AS REP_STATE, 
	claim_party_zip_MIJA1 AS REP_MAIL_ZIP_CODE, 
	claim_party_zip4_MIJA1 AS REP_MAIL_ZIP4, 
	ph_num_MIJA1 AS REP_PH, 
	ph_extension_MIJA1 AS REP_PH_EXTENSION, 
	DEFAULT_BLANKS AS RESERVED_97, 
	LKP_orm_ind AS ORM_IND, 
	orm_termination_date1 AS ORM_TERMINATION_DATE, 
	tpoc_date1 AS TPOC_DATE, 
	tpoc_amt1 AS TPOC_AMOUNT, 
	tpoc_fund_delay_date1 AS FUNDING_DELAYED_BEYOND_TPOC_START_DATE, 
	DEFAULT_BLANKS AS RESERVED_103, 
	claimant_1_Ind_MCT11 AS C1_RELATIONSHIP, 
	tax_id_MCT11 AS C1_TIN, 
	claim_party_last_name_MCT11 AS C1_LAST_NAME, 
	claim_party_first_name_MCT11 AS C1_FIRST_NAME, 
	claim_party_mid_name_MCT11 AS C1_MIDDLE_INITIAL, 
	claim_party_addr_MCT11 AS C1_MAIL_ADDR_1, 
	claim_party_addr_Ln2_MCT11 AS C1_MAIL_ADDR_2, 
	claim_party_city_MCT11 AS C1_CITY, 
	claim_party_state_MCT11 AS C1_STATE, 
	claim_party_zip_MCT11 AS C1_ZIP, 
	claim_party_zip4_MCT11 AS C1_ZIP4, 
	ph_num_MCT11 AS C1_PH, 
	ph_extension_MCT11 AS C1_PH_EXTENSION, 
	DEFAULT_BLANKS AS RESERVED_117, 
	claimant_1_rep_Ind_MCA11 AS C1_REP_IND, 
	claim_party_last_name_MCA11 AS C1_REP_LAST_NAME, 
	claim_party_first_name_MCA11 AS C1_REP_FIRST_NAME, 
	claimant1_rep_firm_MCA11 AS C1_REP_FIRM_NAME, 
	tax_id_MCA11 AS C1_REP_TIN, 
	claim_party_addr_MCA11 AS C1_REP_MAIL_ADDR_1, 
	claim_party_addr_Ln2_MCA11 AS C1_REP_MAIL_ADDR_2, 
	claim_party_city_MCA11 AS C1_REP_MAIL_CITY, 
	claim_party_state_MCA11 AS C1_REP_STATE, 
	claim_party_zip_MCA11 AS C1_REP_ZIP, 
	claim_party_zip4_MCA11 AS C1_REP_ZIP4, 
	ph_num_MCA11 AS C1_REP_PH, 
	ph_extension_MCA11 AS C1_REP_PH_EXTENSION, 
	DEFAULT_BLANKS AS RESERVED_131, 
	CURRENT_DATE AS CREATED_DATE, 
	CURRENT_DATE AS MODIFIED_DATE, 
	AUDIT_ID AS AUDIT_ID
	FROM EXP_ActionType_UPDATE
),
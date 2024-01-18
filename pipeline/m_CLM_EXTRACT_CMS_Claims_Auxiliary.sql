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
	claim_medical.medicare_hicn,
	CASE claim_medical.medicare_hicn
		WHEN 'N/A' THEN REPLACE(claim_party.tax_ssn_id,'-','') 
		ELSE 'N/A'
	END as tax_ssn_id,
	claim_party.claim_party_last_name,
	claim_party.claim_party_first_name,
	claim_medical.claimant2_rep_firm, 
	claim_medical.claimant3_rep_firm, 
	claim_medical.claimant4_rep_firm
	FROM
	claim_occurrence claim_occurrence inner join claim_party_occurrence on 
	claim_party_occurrence.claim_occurrence_ak_id = claim_occurrence.claim_occurrence_ak_id  
	inner join claim_party claim_party on 
	claim_party_occurrence.claim_party_ak_id = claim_party.claim_party_ak_id  
	inner join claim_medical claim_medical on 
	claim_medical.claim_party_occurrence_ak_id = claim_party_occurrence.claim_party_occurrence_ak_ID  
	inner join claim_medical_plan claim_medical_plan on
	claim_medical.claim_med_ak_id = claim_medical_plan.claim_med_ak_id  
	inner join wc_stage.dbo.cms_control_tab_stage cms_control_tab_stage on 
	claim_medical_plan.cms_document_cntl_num = cms_control_tab_stage.cms_doc_cntl_num 
	AND claim_party.crrnt_snpsht_flag = 1
	AND claim_party_occurrence.crrnt_snpsht_flag = 1
	AND claim_medical.crrnt_snpsht_flag = 1
	AND claim_medical_plan.crrnt_snpsht_flag = 1
	AND cms_control_tab_stage.cms_report_status = 'T' 
	AND cms_control_tab_stage.cms_action_type IN ('0','2','3')
	where claim_occurrence.source_sys_id  IN ('PMS', 'EXCEED')
	AND claim_occurrence.crrnt_snpsht_flag = 1
	@{pipeline().parameters.WHERE_EXCEED_PMS}
	
	Union
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
	claim_medical.medicare_hicn,
	CASE claim_medical.medicare_hicn
	                WHEN 'N/A' THEN REPLACE(claim_party.tax_ssn_id,'-','') 
	                ELSE 'N/A'
	END as tax_ssn_id,
	claim_party.claim_party_last_name,
	claim_party.claim_party_first_name,
	claim_medical.claimant2_rep_firm, 
	claim_medical.claimant3_rep_firm, 
	claim_medical.claimant4_rep_firm
	FROM
	claim_occurrence claim_occurrence inner join claim_party_occurrence on 
	claim_party_occurrence.claim_occurrence_ak_id = claim_occurrence.claim_occurrence_ak_id  
	inner join claim_party claim_party on 
	claim_party_occurrence.claim_party_ak_id = claim_party.claim_party_ak_id  
	inner join claim_medical claim_medical on 
	claim_medical.claim_party_occurrence_ak_id = claim_party_occurrence.claim_party_occurrence_ak_ID  
	inner join claim_medical_plan claim_medical_plan on
	claim_medical.claim_med_ak_id = claim_medical_plan.claim_med_ak_id  
	inner join wc_stage.dbo.CMSControlTableStage CMSControlTableStage on 
	claim_medical_plan.cms_document_cntl_num = CMSControlTableStage.CMSDocControlNum 
	AND claim_party.crrnt_snpsht_flag = 1
	AND claim_party_occurrence.crrnt_snpsht_flag = 1
	AND claim_medical.crrnt_snpsht_flag = 1
	AND claim_medical_plan.crrnt_snpsht_flag = 1
	AND CMSControlTableStage.CMSReportStatus = 'T' 
	AND CMSControlTableStage.CMSActionType IN ('0','2','3')
	where claim_occurrence.source_sys_id  IN ('DCT')
	AND claim_occurrence.crrnt_snpsht_flag = 1
	@{pipeline().parameters.WHERE_DCT}
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
	medicare_hicn,
	tax_ssn_id,
	claim_party_last_name,
	claim_party_first_name,
	claimant2_rep_firm,
	claimant3_rep_firm,
	claimant4_rep_firm,
	'MCT2' AS claimant_2_type,
	'MCA2' AS claimant_2_rep_type,
	'MCT3' AS claimant_3_type,
	'MCA3' AS claimant_3_rep_type,
	'MCT4' AS claimant_4_type1,
	'MCA4' AS claimant_4_rep_type
	FROM SQ_Sources
),
LKP_Claim_Party_Relation_Claimant_2 AS (
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
LKP_Claim_Party_Relation_Claimant_2_Rep AS (
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
LKP_Claim_Party_Relation_Claimant_3 AS (
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
LKP_Claim_Party_Relation_Claimant_3_Rep AS (
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
LKP_Claim_Party_Relation_Claimant_4 AS (
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
LKP_Claim_Party_Relation_Claimant_4_Rep AS (
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
	'NGCE' AS record_identifier,
	EXP_Source.cms_document_cntl_num,
	EXP_Source.medicare_hicn,
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
	EXP_Source.claimant_2_type,
	LKP_Claim_Party_Relation_Claimant_2.claim_party_relation_role_code AS claimant_2_role_code,
	LKP_Claim_Party_Relation_Claimant_2.is_cms_party_individ AS claimant_2_is_individual,
	-- *INF*: LTRIM(RTRIM(:LKP.LKP_sup_cms_relation_indicator(claimant_2_type,claimant_2_role_code,
	-- claimant_2_is_individual)))
	LTRIM(RTRIM(LKP_SUP_CMS_RELATION_INDICATOR_claimant_2_type_claimant_2_role_code_claimant_2_is_individual.cms_relation_file_code)) AS claimant_2_Ind,
	LKP_Claim_Party_Relation_Claimant_2.claim_party_relation_from_ak_id AS claimant_2_ak_id,
	EXP_Source.claimant_2_rep_type,
	LKP_Claim_Party_Relation_Claimant_2_Rep.claim_party_relation_role_code AS claimant_2_rep_role_code,
	LKP_Claim_Party_Relation_Claimant_2_Rep.is_cms_party_individ AS claimant_2_rep_is_individual,
	-- *INF*: LTRIM(RTRIM(:LKP.LKP_sup_cms_relation_indicator(claimant_2_rep_type,claimant_2_rep_role_code, 'Y')))
	-- 
	-- --we should be able to hard-code either Y or N for the join used for MIJA and MCA1-MCA4 for in_individual column since their Claimant Representative Indicator values dont change in sup_cms_relation_indicator table whether the related party is an individual or not. 
	-- 
	-- --LTRIM(RTRIM(:LKP.LKP_sup_cms_relation_indicator(claimant_2_rep_type,claimant_2_rep_role_code, claimant_2_rep_is_individual)))
	LTRIM(RTRIM(LKP_SUP_CMS_RELATION_INDICATOR_claimant_2_rep_type_claimant_2_rep_role_code_Y.cms_relation_file_code)) AS claimant_2_rep_Ind,
	LKP_Claim_Party_Relation_Claimant_2_Rep.claim_party_relation_from_ak_id AS claimant_2_rep_ak_id,
	EXP_Source.claimant_3_type,
	LKP_Claim_Party_Relation_Claimant_3.claim_party_relation_role_code AS claimant_3_role_code,
	LKP_Claim_Party_Relation_Claimant_3.is_cms_party_individ AS claimant_3_is_individual,
	-- *INF*: LTRIM(RTRIM(:LKP.LKP_sup_cms_relation_indicator(claimant_3_type,claimant_3_role_code,
	-- claimant_3_is_individual)))
	LTRIM(RTRIM(LKP_SUP_CMS_RELATION_INDICATOR_claimant_3_type_claimant_3_role_code_claimant_3_is_individual.cms_relation_file_code)) AS claimant_3_Ind,
	LKP_Claim_Party_Relation_Claimant_3.claim_party_relation_from_ak_id AS claimant_3_ak_id,
	EXP_Source.claimant_3_rep_type,
	LKP_Claim_Party_Relation_Claimant_3_Rep.claim_party_relation_role_code AS claimant_3_rep_role_code,
	LKP_Claim_Party_Relation_Claimant_3_Rep.is_cms_party_individ AS claimant_3_rep_is_individual,
	-- *INF*: LTRIM(RTRIM(:LKP.LKP_sup_cms_relation_indicator(claimant_3_rep_type,claimant_3_rep_role_code,'Y')))
	-- 
	-- --we should be able to hard-code either Y or N for the join used for MIJA and MCA1-MCA4 for in_individual column since their Claimant Representative Indicator values dont change in sup_cms_relation_indicator table whether the related party is an individual or not. 
	-- 
	-- --LTRIM(RTRIM(:LKP.LKP_sup_cms_relation_indicator(claimant_3_rep_type,claimant_3_rep_role_code,claimant_3_rep_is_individual)))
	LTRIM(RTRIM(LKP_SUP_CMS_RELATION_INDICATOR_claimant_3_rep_type_claimant_3_rep_role_code_Y.cms_relation_file_code)) AS claimant_3_rep_Ind,
	LKP_Claim_Party_Relation_Claimant_3_Rep.claim_party_relation_from_ak_id AS claimant_3_rep_ak_id,
	EXP_Source.claimant_4_type1 AS claimant_4_type,
	LKP_Claim_Party_Relation_Claimant_4.claim_party_relation_role_code AS claimant_4_role_code,
	LKP_Claim_Party_Relation_Claimant_4.is_cms_party_individ AS claimant_4_is_individual,
	-- *INF*: LTRIM(RTRIM(:LKP.LKP_sup_cms_relation_indicator(claimant_4_type,claimant_4_role_code,
	-- claimant_4_is_individual)))
	LTRIM(RTRIM(LKP_SUP_CMS_RELATION_INDICATOR_claimant_4_type_claimant_4_role_code_claimant_4_is_individual.cms_relation_file_code)) AS claimant_4_Ind,
	LKP_Claim_Party_Relation_Claimant_4.claim_party_relation_from_ak_id AS claimant_4_ak_id,
	EXP_Source.claimant_4_rep_type,
	LKP_Claim_Party_Relation_Claimant_4_Rep.claim_party_relation_role_code AS claimant_4_rep_role_code,
	LKP_Claim_Party_Relation_Claimant_4_Rep.is_cms_party_individ AS claimant_4_rep_is_individual,
	-- *INF*: LTRIM(RTRIM(:LKP.LKP_sup_cms_relation_indicator(claimant_4_rep_type,claimant_4_rep_role_code, 'Y')))
	-- 
	-- --we should be able to hard-code either Y or N for the join used for MIJA and MCA1-MCA4 for in_individual column since their Claimant Representative Indicator values dont change in sup_cms_relation_indicator table whether the related party is an individual or not. 
	-- 
	-- --LTRIM(RTRIM(:LKP.LKP_sup_cms_relation_indicator(claimant_4_rep_type,claimant_4_rep_role_code, claimant_4_rep_is_individual)))
	LTRIM(RTRIM(LKP_SUP_CMS_RELATION_INDICATOR_claimant_4_rep_type_claimant_4_rep_role_code_Y.cms_relation_file_code)) AS claimant_4_rep_Ind,
	LKP_Claim_Party_Relation_Claimant_4_Rep.claim_party_relation_from_ak_id AS claimant_4_rep_ak_id,
	EXP_Source.claimant2_rep_firm,
	EXP_Source.claimant3_rep_firm,
	EXP_Source.claimant4_rep_firm,
	'2' AS tpoc_code_2,
	'3' AS tpoc_code_3,
	'4' AS tpoc_code_4,
	'5' AS tpoc_code_5
	FROM EXP_Source
	LEFT JOIN LKP_Claim_Party_Relation_Claimant_2
	ON LKP_Claim_Party_Relation_Claimant_2.claim_party_occurrence_ak_id = EXP_Source.claim_party_occurrence_ak_id AND LKP_Claim_Party_Relation_Claimant_2.claim_party_relation_to_ak_id = EXP_Source.claim_party_ak_id AND LKP_Claim_Party_Relation_Claimant_2.cms_party_type = EXP_Source.claimant_2_type
	LEFT JOIN LKP_Claim_Party_Relation_Claimant_2_Rep
	ON LKP_Claim_Party_Relation_Claimant_2_Rep.claim_party_occurrence_ak_id = EXP_Source.claim_party_occurrence_ak_id AND LKP_Claim_Party_Relation_Claimant_2_Rep.claim_party_relation_to_ak_id = EXP_Source.claim_party_ak_id AND LKP_Claim_Party_Relation_Claimant_2_Rep.cms_party_type = EXP_Source.claimant_2_rep_type
	LEFT JOIN LKP_Claim_Party_Relation_Claimant_3
	ON LKP_Claim_Party_Relation_Claimant_3.claim_party_occurrence_ak_id = EXP_Source.claim_party_occurrence_ak_id AND LKP_Claim_Party_Relation_Claimant_3.claim_party_relation_to_ak_id = EXP_Source.claim_party_ak_id AND LKP_Claim_Party_Relation_Claimant_3.cms_party_type = EXP_Source.claimant_3_type
	LEFT JOIN LKP_Claim_Party_Relation_Claimant_3_Rep
	ON LKP_Claim_Party_Relation_Claimant_3_Rep.claim_party_occurrence_ak_id = EXP_Source.claim_party_occurrence_ak_id AND LKP_Claim_Party_Relation_Claimant_3_Rep.claim_party_relation_to_ak_id = EXP_Source.claim_party_ak_id AND LKP_Claim_Party_Relation_Claimant_3_Rep.cms_party_type = EXP_Source.claimant_3_rep_type
	LEFT JOIN LKP_Claim_Party_Relation_Claimant_4
	ON LKP_Claim_Party_Relation_Claimant_4.claim_party_occurrence_ak_id = EXP_Source.claim_party_occurrence_ak_id AND LKP_Claim_Party_Relation_Claimant_4.claim_party_relation_to_ak_id = EXP_Source.claim_party_ak_id AND LKP_Claim_Party_Relation_Claimant_4.cms_party_type = EXP_Source.claimant_4_type1
	LEFT JOIN LKP_Claim_Party_Relation_Claimant_4_Rep
	ON LKP_Claim_Party_Relation_Claimant_4_Rep.claim_party_occurrence_ak_id = EXP_Source.claim_party_occurrence_ak_id AND LKP_Claim_Party_Relation_Claimant_4_Rep.claim_party_relation_to_ak_id = EXP_Source.claim_party_ak_id AND LKP_Claim_Party_Relation_Claimant_4_Rep.cms_party_type = EXP_Source.claimant_4_rep_type
	LEFT JOIN LKP_cms_pms_relation_stage
	ON LKP_cms_pms_relation_stage.client_key = EXP_Source.claim_party_key
	LEFT JOIN LKP_SUP_CMS_RELATION_INDICATOR LKP_SUP_CMS_RELATION_INDICATOR_claimant_2_type_claimant_2_role_code_claimant_2_is_individual
	ON LKP_SUP_CMS_RELATION_INDICATOR_claimant_2_type_claimant_2_role_code_claimant_2_is_individual.cms_party_type = claimant_2_type
	AND LKP_SUP_CMS_RELATION_INDICATOR_claimant_2_type_claimant_2_role_code_claimant_2_is_individual.cms_relation_ind = claimant_2_role_code
	AND LKP_SUP_CMS_RELATION_INDICATOR_claimant_2_type_claimant_2_role_code_claimant_2_is_individual.is_cms_party_individ = claimant_2_is_individual

	LEFT JOIN LKP_SUP_CMS_RELATION_INDICATOR LKP_SUP_CMS_RELATION_INDICATOR_claimant_2_rep_type_claimant_2_rep_role_code_Y
	ON LKP_SUP_CMS_RELATION_INDICATOR_claimant_2_rep_type_claimant_2_rep_role_code_Y.cms_party_type = claimant_2_rep_type
	AND LKP_SUP_CMS_RELATION_INDICATOR_claimant_2_rep_type_claimant_2_rep_role_code_Y.cms_relation_ind = claimant_2_rep_role_code
	AND LKP_SUP_CMS_RELATION_INDICATOR_claimant_2_rep_type_claimant_2_rep_role_code_Y.is_cms_party_individ = 'Y'

	LEFT JOIN LKP_SUP_CMS_RELATION_INDICATOR LKP_SUP_CMS_RELATION_INDICATOR_claimant_3_type_claimant_3_role_code_claimant_3_is_individual
	ON LKP_SUP_CMS_RELATION_INDICATOR_claimant_3_type_claimant_3_role_code_claimant_3_is_individual.cms_party_type = claimant_3_type
	AND LKP_SUP_CMS_RELATION_INDICATOR_claimant_3_type_claimant_3_role_code_claimant_3_is_individual.cms_relation_ind = claimant_3_role_code
	AND LKP_SUP_CMS_RELATION_INDICATOR_claimant_3_type_claimant_3_role_code_claimant_3_is_individual.is_cms_party_individ = claimant_3_is_individual

	LEFT JOIN LKP_SUP_CMS_RELATION_INDICATOR LKP_SUP_CMS_RELATION_INDICATOR_claimant_3_rep_type_claimant_3_rep_role_code_Y
	ON LKP_SUP_CMS_RELATION_INDICATOR_claimant_3_rep_type_claimant_3_rep_role_code_Y.cms_party_type = claimant_3_rep_type
	AND LKP_SUP_CMS_RELATION_INDICATOR_claimant_3_rep_type_claimant_3_rep_role_code_Y.cms_relation_ind = claimant_3_rep_role_code
	AND LKP_SUP_CMS_RELATION_INDICATOR_claimant_3_rep_type_claimant_3_rep_role_code_Y.is_cms_party_individ = 'Y'

	LEFT JOIN LKP_SUP_CMS_RELATION_INDICATOR LKP_SUP_CMS_RELATION_INDICATOR_claimant_4_type_claimant_4_role_code_claimant_4_is_individual
	ON LKP_SUP_CMS_RELATION_INDICATOR_claimant_4_type_claimant_4_role_code_claimant_4_is_individual.cms_party_type = claimant_4_type
	AND LKP_SUP_CMS_RELATION_INDICATOR_claimant_4_type_claimant_4_role_code_claimant_4_is_individual.cms_relation_ind = claimant_4_role_code
	AND LKP_SUP_CMS_RELATION_INDICATOR_claimant_4_type_claimant_4_role_code_claimant_4_is_individual.is_cms_party_individ = claimant_4_is_individual

	LEFT JOIN LKP_SUP_CMS_RELATION_INDICATOR LKP_SUP_CMS_RELATION_INDICATOR_claimant_4_rep_type_claimant_4_rep_role_code_Y
	ON LKP_SUP_CMS_RELATION_INDICATOR_claimant_4_rep_type_claimant_4_rep_role_code_Y.cms_party_type = claimant_4_rep_type
	AND LKP_SUP_CMS_RELATION_INDICATOR_claimant_4_rep_type_claimant_4_rep_role_code_Y.cms_relation_ind = claimant_4_rep_role_code
	AND LKP_SUP_CMS_RELATION_INDICATOR_claimant_4_rep_type_claimant_4_rep_role_code_Y.is_cms_party_individ = 'Y'

),
LKP_Claim_Party_claimant_2 AS (
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
LKP_Claim_Party_claimant_2_rep AS (
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
LKP_Claim_Party_claimant_3 AS (
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
LKP_Claim_Party_claimant_3_rep AS (
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
LKP_Claim_Party_claimant_4 AS (
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
LKP_Claim_Party_claimant_4_rep AS (
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
LKP_claim_medical_plan_tpoc2 AS (
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
LKP_claim_medical_plan_tpoc3 AS (
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
LKP_claim_medical_plan_tpoc4 AS (
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
LKP_claim_medical_plan_tpoc5 AS (
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
EXP_Values1 AS (
	SELECT
	EXP_Values.claim_med_ak_id,
	EXP_Values.claim_party_occurrence_ak_id,
	EXP_Values.claim_med_plan_ak_id,
	EXP_Values.claim_occurrence_ak_id,
	EXP_Values.claim_party_ak_id,
	EXP_Values.source_sys_id,
	EXP_Values.record_identifier,
	EXP_Values.cms_document_cntl_num,
	EXP_Values.medicare_hicn,
	EXP_Values.tax_ssn_id,
	EXP_Values.claim_party_last_name,
	EXP_Values.claim_party_first_name,
	EXP_Values.claimant_2_Ind AS claimant_2_Ind_MCT2,
	LKP_Claim_Party_claimant_2.tax_id AS tax_id_MCT2,
	LKP_Claim_Party_claimant_2.claim_party_last_name AS claim_party_last_name_MCT2,
	LKP_Claim_Party_claimant_2.claim_party_first_name AS claim_party_first_name_MCT2,
	LKP_Claim_Party_claimant_2.claim_party_mid_name AS claim_party_mid_name_MCT2,
	LKP_Claim_Party_claimant_2.claim_party_addr AS claim_party_addr_MCT2,
	LKP_Claim_Party_claimant_2.claim_party_city AS claim_party_city_MCT2,
	LKP_Claim_Party_claimant_2.claim_party_state AS claim_party_state_MCT2,
	LKP_Claim_Party_claimant_2.claim_party_zip AS claim_party_zip_MCT2,
	LKP_Claim_Party_claimant_2.claim_party_zip4 AS claim_party_zip4_MCT2,
	LKP_Claim_Party_claimant_2.ph_num AS ph_num_MCT2,
	LKP_Claim_Party_claimant_2.ph_extension AS ph_extension_MCT2,
	EXP_Values.claimant_2_rep_is_individual,
	-- *INF*: IIF(IN(source_sys_id,'EXCEED','DCT') and claimant_2_rep_is_individual='N',1,0)
	-- 
	IFF(source_sys_id IN ('EXCEED','DCT') and claimant_2_rep_is_individual = 'N', 1, 0) AS v_apply_Exceed_firm_name_rule_MCA2,
	EXP_Values.claimant_2_rep_Ind AS claimant_2_Rep_Ind_MCA2,
	LKP_Claim_Party_claimant_2_rep.tax_id AS tax_id_MCA2,
	LKP_Claim_Party_claimant_2_rep.claim_party_last_name AS claim_party_last_name_MCA2,
	-- *INF*: IIF(v_apply_Exceed_firm_name_rule_MCA2=1,'N/A',claim_party_last_name_MCA2)
	IFF(v_apply_Exceed_firm_name_rule_MCA2 = 1, 'N/A', claim_party_last_name_MCA2) AS claim_party_last_name_MCA2_OUT,
	LKP_Claim_Party_claimant_2_rep.claim_party_first_name AS claim_party_first_name_MCA2,
	-- *INF*: IIF(v_apply_Exceed_firm_name_rule_MCA2=1,'N/A',claim_party_first_name_MCA2)
	IFF(v_apply_Exceed_firm_name_rule_MCA2 = 1, 'N/A', claim_party_first_name_MCA2) AS claim_party_first_name_MCA2_OUT,
	EXP_Values.claimant2_rep_firm,
	-- *INF*: IIF(
	-- v_apply_Exceed_firm_name_rule_MCA2=1
	-- , claim_party_last_name_MCA2, claimant2_rep_firm)
	IFF(v_apply_Exceed_firm_name_rule_MCA2 = 1, claim_party_last_name_MCA2, claimant2_rep_firm) AS claimant2_rep_firm_OUT,
	LKP_Claim_Party_claimant_2_rep.claim_party_mid_name AS claim_party_mid_name_MCA2,
	LKP_Claim_Party_claimant_2_rep.claim_party_addr AS claim_party_addr_MCA2,
	LKP_Claim_Party_claimant_2_rep.claim_party_city AS claim_party_city_MCA2,
	LKP_Claim_Party_claimant_2_rep.claim_party_state AS claim_party_state_MCA2,
	LKP_Claim_Party_claimant_2_rep.claim_party_zip AS claim_party_zip_MCA2,
	LKP_Claim_Party_claimant_2_rep.claim_party_zip4 AS claim_party_zip4_MCA2,
	LKP_Claim_Party_claimant_2_rep.ph_num AS ph_num_MCA2,
	LKP_Claim_Party_claimant_2_rep.ph_extension AS ph_extension_MCA2,
	EXP_Values.claimant_3_Ind AS claimant_3_Ind_MCT3,
	LKP_Claim_Party_claimant_3.tax_id AS tax_id_MCT3,
	LKP_Claim_Party_claimant_3.claim_party_last_name AS claim_party_last_name_MCT3,
	LKP_Claim_Party_claimant_3.claim_party_first_name AS claim_party_first_name_MCT3,
	LKP_Claim_Party_claimant_3.claim_party_mid_name AS claim_party_mid_name_MCT3,
	LKP_Claim_Party_claimant_3.claim_party_addr AS claim_party_addr_MCT3,
	LKP_Claim_Party_claimant_3.claim_party_city AS claim_party_city_MCT3,
	LKP_Claim_Party_claimant_3.claim_party_state AS claim_party_state_MCT3,
	LKP_Claim_Party_claimant_3.claim_party_zip AS claim_party_zip_MCT3,
	LKP_Claim_Party_claimant_3.claim_party_zip4 AS claim_party_zip4_MCT3,
	LKP_Claim_Party_claimant_3.ph_num AS ph_num_MCT3,
	LKP_Claim_Party_claimant_3.ph_extension AS ph_extension_MCT3,
	EXP_Values.claimant_3_rep_is_individual,
	-- *INF*: IIF(IN(source_sys_id,'EXCEED','DCT') and claimant_3_rep_is_individual='N',1,0)
	IFF(source_sys_id IN ('EXCEED','DCT') and claimant_3_rep_is_individual = 'N', 1, 0) AS v_apply_Exceed_firm_name_rule_MCA3,
	EXP_Values.claimant_3_rep_Ind AS claimant_3_Rep_Ind_MCA3,
	LKP_Claim_Party_claimant_3_rep.tax_id AS tax_id_MCA3,
	LKP_Claim_Party_claimant_3_rep.claim_party_last_name AS claim_party_last_name_MCA3,
	-- *INF*: IIF(v_apply_Exceed_firm_name_rule_MCA3=1,'N/A',claim_party_last_name_MCA3)
	IFF(v_apply_Exceed_firm_name_rule_MCA3 = 1, 'N/A', claim_party_last_name_MCA3) AS claim_party_last_name_MCA3_OUT,
	LKP_Claim_Party_claimant_3_rep.claim_party_first_name AS claim_party_first_name_MCA3,
	-- *INF*: IIF(v_apply_Exceed_firm_name_rule_MCA3=1,'N/A',claim_party_first_name_MCA3)
	IFF(v_apply_Exceed_firm_name_rule_MCA3 = 1, 'N/A', claim_party_first_name_MCA3) AS claim_party_first_name_MCA3_OUT,
	EXP_Values.claimant3_rep_firm,
	-- *INF*: IIF(
	-- v_apply_Exceed_firm_name_rule_MCA3=1
	-- , claim_party_last_name_MCA3, claimant3_rep_firm)
	IFF(v_apply_Exceed_firm_name_rule_MCA3 = 1, claim_party_last_name_MCA3, claimant3_rep_firm) AS claimant3_rep_firm_OUT,
	LKP_Claim_Party_claimant_3_rep.claim_party_mid_name AS claim_party_mid_name_MCA3,
	LKP_Claim_Party_claimant_3_rep.claim_party_addr AS claim_party_addr_MCA3,
	LKP_Claim_Party_claimant_3_rep.claim_party_city AS claim_party_city_MCA3,
	LKP_Claim_Party_claimant_3_rep.claim_party_state AS claim_party_state_MCA3,
	LKP_Claim_Party_claimant_3_rep.claim_party_zip AS claim_party_zip_MCA3,
	LKP_Claim_Party_claimant_3_rep.claim_party_zip4 AS claim_party_zip4_MCA3,
	LKP_Claim_Party_claimant_3_rep.ph_num AS ph_num_MCA3,
	LKP_Claim_Party_claimant_3_rep.ph_extension AS ph_extension_MCA3,
	EXP_Values.claimant_4_Ind AS claimant_4_Ind_MCT4,
	LKP_Claim_Party_claimant_4.tax_id AS tax_id_MCT4,
	LKP_Claim_Party_claimant_4.claim_party_last_name AS claim_party_last_name_MCT4,
	LKP_Claim_Party_claimant_4.claim_party_first_name AS claim_party_first_name_MCT4,
	LKP_Claim_Party_claimant_4.claim_party_mid_name AS claim_party_mid_name_MCT4,
	LKP_Claim_Party_claimant_4.claim_party_addr AS claim_party_addr_MCT4,
	LKP_Claim_Party_claimant_4.claim_party_city AS claim_party_city_MCT4,
	LKP_Claim_Party_claimant_4.claim_party_state AS claim_party_state_MCT4,
	LKP_Claim_Party_claimant_4.claim_party_zip AS claim_party_zip_MCT4,
	LKP_Claim_Party_claimant_4.claim_party_zip4 AS claim_party_zip4_MCT4,
	LKP_Claim_Party_claimant_4.ph_num AS ph_num_MCT4,
	LKP_Claim_Party_claimant_4.ph_extension AS ph_extension_MCT4,
	EXP_Values.claimant_4_rep_is_individual,
	-- *INF*: IIF(IN(source_sys_id,'EXCEED','DCT') and claimant_4_rep_is_individual='N',1,0)
	IFF(source_sys_id IN ('EXCEED','DCT') and claimant_4_rep_is_individual = 'N', 1, 0) AS v_apply_Exceed_firm_name_rule_MCA4,
	EXP_Values.claimant_4_rep_Ind AS claimant_4_rep_Ind_MCA4,
	LKP_Claim_Party_claimant_4_rep.tax_id AS tax_id_MCA4,
	LKP_Claim_Party_claimant_4_rep.claim_party_last_name AS claim_party_last_name_MCA4,
	-- *INF*: IIF(v_apply_Exceed_firm_name_rule_MCA4=1,'N/A',claim_party_last_name_MCA4)
	IFF(v_apply_Exceed_firm_name_rule_MCA4 = 1, 'N/A', claim_party_last_name_MCA4) AS claim_party_last_name_MCA4_OUT,
	LKP_Claim_Party_claimant_4_rep.claim_party_first_name AS claim_party_first_name_MCA4,
	-- *INF*: IIF(v_apply_Exceed_firm_name_rule_MCA4=1,'N/A',claim_party_first_name_MCA4)
	IFF(v_apply_Exceed_firm_name_rule_MCA4 = 1, 'N/A', claim_party_first_name_MCA4) AS claim_party_first_name_MCA4_OUT,
	EXP_Values.claimant4_rep_firm,
	-- *INF*: IIF(
	-- v_apply_Exceed_firm_name_rule_MCA4=1
	-- , claim_party_last_name_MCA4, claimant4_rep_firm)
	IFF(v_apply_Exceed_firm_name_rule_MCA4 = 1, claim_party_last_name_MCA4, claimant4_rep_firm) AS claimant4_rep_firm_OUT,
	LKP_Claim_Party_claimant_4_rep.claim_party_mid_name AS claim_party_addr_MCA4,
	LKP_Claim_Party_claimant_4_rep.claim_party_addr AS claim_party_city_MCA4,
	LKP_Claim_Party_claimant_4_rep.claim_party_city AS claim_party_state_MCA4,
	LKP_Claim_Party_claimant_4_rep.claim_party_state AS claim_party_zip_MCA4,
	LKP_Claim_Party_claimant_4_rep.claim_party_zip AS claim_party_zip4_MCA4,
	LKP_Claim_Party_claimant_4_rep.claim_party_zip4 AS ph_num_MCA4,
	LKP_Claim_Party_claimant_4_rep.ph_num AS ph_extension_MCA4,
	LKP_claim_medical_plan_tpoc2.tpoc_date AS tpoc_date2,
	LKP_claim_medical_plan_tpoc2.tpoc_amt AS tpoc_amt2,
	LKP_claim_medical_plan_tpoc2.tpoc_fund_delay_date AS tpoc_fund_delay_date2,
	LKP_claim_medical_plan_tpoc3.tpoc_date AS tpoc_date3,
	LKP_claim_medical_plan_tpoc3.tpoc_amt AS tpoc_amt3,
	LKP_claim_medical_plan_tpoc3.tpoc_fund_delay_date AS tpoc_fund_delay_date3,
	LKP_claim_medical_plan_tpoc4.tpoc_date AS tpoc_date4,
	LKP_claim_medical_plan_tpoc4.tpoc_amt AS tpoc_amt4,
	LKP_claim_medical_plan_tpoc4.tpoc_fund_delay_date AS tpoc_fund_delay_date4,
	LKP_claim_medical_plan_tpoc5.tpoc_date AS tpoc_date5,
	LKP_claim_medical_plan_tpoc5.tpoc_amt AS tpoc_amt5,
	LKP_claim_medical_plan_tpoc5.tpoc_fund_delay_date AS tpoc_fund_delay_date5
	FROM EXP_Values
	LEFT JOIN LKP_Claim_Party_claimant_2
	ON LKP_Claim_Party_claimant_2.claim_party_ak_id = EXP_Values.claimant_2_ak_id
	LEFT JOIN LKP_Claim_Party_claimant_2_rep
	ON LKP_Claim_Party_claimant_2_rep.claim_party_ak_id = EXP_Values.claimant_2_rep_ak_id
	LEFT JOIN LKP_Claim_Party_claimant_3
	ON LKP_Claim_Party_claimant_3.claim_party_ak_id = EXP_Values.claimant_3_ak_id
	LEFT JOIN LKP_Claim_Party_claimant_3_rep
	ON LKP_Claim_Party_claimant_3_rep.claim_party_ak_id = EXP_Values.claimant_3_rep_ak_id
	LEFT JOIN LKP_Claim_Party_claimant_4
	ON LKP_Claim_Party_claimant_4.claim_party_ak_id = EXP_Values.claimant_4_ak_id
	LEFT JOIN LKP_Claim_Party_claimant_4_rep
	ON LKP_Claim_Party_claimant_4_rep.claim_party_ak_id = EXP_Values.claimant_4_rep_ak_id
	LEFT JOIN LKP_claim_medical_plan_tpoc2
	ON LKP_claim_medical_plan_tpoc2.claim_med_plan_ak_id = EXP_Values.claim_med_plan_ak_id AND LKP_claim_medical_plan_tpoc2.tpoc_code = EXP_Values.tpoc_code_2
	LEFT JOIN LKP_claim_medical_plan_tpoc3
	ON LKP_claim_medical_plan_tpoc3.claim_med_plan_ak_id = EXP_Values.claim_med_plan_ak_id AND LKP_claim_medical_plan_tpoc3.tpoc_code = EXP_Values.tpoc_code_3
	LEFT JOIN LKP_claim_medical_plan_tpoc4
	ON LKP_claim_medical_plan_tpoc4.claim_med_plan_ak_id = EXP_Values.claim_med_plan_ak_id AND LKP_claim_medical_plan_tpoc4.tpoc_code = EXP_Values.tpoc_code_4
	LEFT JOIN LKP_claim_medical_plan_tpoc5
	ON LKP_claim_medical_plan_tpoc5.claim_med_plan_ak_id = EXP_Values.claim_med_plan_ak_id AND LKP_claim_medical_plan_tpoc5.tpoc_code = EXP_Values.tpoc_code_5
),
FIL_Valid_Rows AS (
	SELECT
	claim_med_ak_id, 
	claim_party_occurrence_ak_id, 
	claim_med_plan_ak_id, 
	claim_occurrence_ak_id, 
	claim_party_ak_id, 
	record_identifier, 
	cms_document_cntl_num, 
	medicare_hicn, 
	tax_ssn_id, 
	claim_party_last_name, 
	claim_party_first_name, 
	claimant_2_Ind_MCT2, 
	tax_id_MCT2, 
	claim_party_last_name_MCT2, 
	claim_party_first_name_MCT2, 
	claim_party_mid_name_MCT2, 
	claim_party_addr_MCT2, 
	claim_party_city_MCT2, 
	claim_party_state_MCT2, 
	claim_party_zip_MCT2, 
	claim_party_zip4_MCT2, 
	ph_num_MCT2, 
	ph_extension_MCT2, 
	claimant_2_Rep_Ind_MCA2, 
	tax_id_MCA2, 
	claim_party_last_name_MCA2_OUT AS claim_party_last_name_MCA2, 
	claim_party_first_name_MCA2_OUT AS claim_party_first_name_MCA2, 
	claimant2_rep_firm_OUT AS claimant2_rep_firm, 
	claim_party_mid_name_MCA2, 
	claim_party_addr_MCA2, 
	claim_party_city_MCA2, 
	claim_party_state_MCA2, 
	claim_party_zip_MCA2, 
	claim_party_zip4_MCA2, 
	ph_num_MCA2, 
	ph_extension_MCA2, 
	claimant_3_Ind_MCT3, 
	tax_id_MCT3, 
	claim_party_last_name_MCT3, 
	claim_party_first_name_MCT3, 
	claim_party_mid_name_MCT3, 
	claim_party_addr_MCT3, 
	claim_party_city_MCT3, 
	claim_party_state_MCT3, 
	claim_party_zip_MCT3, 
	claim_party_zip4_MCT3, 
	ph_num_MCT3, 
	ph_extension_MCT3, 
	claimant_3_Rep_Ind_MCA3, 
	tax_id_MCA3, 
	claim_party_last_name_MCA3_OUT AS claim_party_last_name_MCA3, 
	claim_party_first_name_MCA3_OUT AS claim_party_first_name_MCA3, 
	claimant3_rep_firm_OUT AS claimant3_rep_firm, 
	claim_party_mid_name_MCA3, 
	claim_party_addr_MCA3, 
	claim_party_city_MCA3, 
	claim_party_state_MCA3, 
	claim_party_zip_MCA3, 
	claim_party_zip4_MCA3, 
	ph_num_MCA3, 
	ph_extension_MCA3, 
	claimant_4_Ind_MCT4, 
	tax_id_MCT4, 
	claim_party_last_name_MCT4, 
	claim_party_first_name_MCT4, 
	claim_party_mid_name_MCT4, 
	claim_party_addr_MCT4, 
	claim_party_city_MCT4, 
	claim_party_state_MCT4, 
	claim_party_zip_MCT4, 
	claim_party_zip4_MCT4, 
	ph_num_MCT4, 
	ph_extension_MCT4, 
	claimant_4_rep_Ind_MCA4, 
	tax_id_MCA4, 
	claim_party_last_name_MCA4_OUT AS claim_party_last_name_MCA4, 
	claim_party_first_name_MCA4_OUT AS claim_party_first_name_MCA4, 
	claimant4_rep_firm_OUT AS claimant4_rep_firm, 
	claim_party_addr_MCA4, 
	claim_party_city_MCA4, 
	claim_party_state_MCA4, 
	claim_party_zip_MCA4, 
	claim_party_zip4_MCA4, 
	ph_num_MCA4, 
	ph_extension_MCA4, 
	tpoc_date2, 
	tpoc_amt2, 
	tpoc_fund_delay_date2, 
	tpoc_date3, 
	tpoc_amt3, 
	tpoc_fund_delay_date3, 
	tpoc_date4, 
	tpoc_amt4, 
	tpoc_fund_delay_date4, 
	tpoc_date5, 
	tpoc_amt5, 
	tpoc_fund_delay_date5
	FROM EXP_Values1
	WHERE IIF(ISNULL(
tax_id_MCT2 || claim_party_last_name_MCT2 || claim_party_first_name_MCT2 ||
tax_id_MCA2 || claim_party_last_name_MCA2 || claim_party_first_name_MCA2 || 
tax_id_MCT3 || claim_party_last_name_MCT3 || claim_party_first_name_MCT3 || 
tax_id_MCA3 || claim_party_last_name_MCA3 || claim_party_first_name_MCA3 || 
tax_id_MCT4 || claim_party_last_name_MCT4 || claim_party_first_name_MCT4 || 
tax_id_MCA4 || claim_party_last_name_MCA4 || claim_party_first_name_MCA4 || 
tpoc_date2 || tpoc_amt2 || tpoc_fund_delay_date2 || 
tpoc_date3 || tpoc_amt3 || tpoc_fund_delay_date3 || 
tpoc_date4 || tpoc_amt4 || tpoc_fund_delay_date4 || 
tpoc_date5 || tpoc_amt5 || tpoc_fund_delay_date5),
FALSE,TRUE)
),
EXP_Target AS (
	SELECT
	claim_med_ak_id,
	claim_party_occurrence_ak_id,
	claim_med_plan_ak_id,
	claim_occurrence_ak_id,
	claim_party_ak_id,
	record_identifier,
	cms_document_cntl_num,
	medicare_hicn,
	tax_ssn_id,
	claim_party_last_name,
	claim_party_first_name,
	claimant_2_Ind_MCT2,
	tax_id_MCT2,
	claim_party_last_name_MCT2,
	claim_party_first_name_MCT2,
	claim_party_mid_name_MCT2,
	claim_party_addr_MCT2,
	claim_party_city_MCT2,
	claim_party_state_MCT2,
	claim_party_zip_MCT2,
	claim_party_zip4_MCT2,
	ph_num_MCT2,
	ph_extension_MCT2,
	claimant_2_Rep_Ind_MCA2,
	tax_id_MCA2,
	claim_party_last_name_MCA2,
	claim_party_first_name_MCA2,
	claimant2_rep_firm AS claimant2_rep_firm_MCA2,
	claim_party_mid_name_MCA2,
	claim_party_addr_MCA2,
	claim_party_city_MCA2,
	claim_party_state_MCA2,
	claim_party_zip_MCA2,
	claim_party_zip4_MCA2,
	ph_num_MCA2,
	ph_extension_MCA2,
	claimant_3_Ind_MCT3,
	tax_id_MCT3,
	claim_party_last_name_MCT3,
	claim_party_first_name_MCT3,
	claim_party_mid_name_MCT3,
	claim_party_addr_MCT3,
	claim_party_city_MCT3,
	claim_party_state_MCT3,
	claim_party_zip_MCT3,
	claim_party_zip4_MCT3,
	ph_num_MCT3,
	ph_extension_MCT3,
	claimant_3_Rep_Ind_MCA3,
	tax_id_MCA3,
	claim_party_last_name_MCA3,
	claim_party_first_name_MCA3,
	claimant3_rep_firm AS claimant3_rep_firm_MCA3,
	claim_party_mid_name_MCA3,
	claim_party_addr_MCA3,
	claim_party_city_MCA3,
	claim_party_state_MCA3,
	claim_party_zip_MCA3,
	claim_party_zip4_MCA3,
	ph_num_MCA3,
	ph_extension_MCA3,
	claimant_4_Ind_MCT4,
	tax_id_MCT4,
	claim_party_last_name_MCT4,
	claim_party_first_name_MCT4,
	claim_party_mid_name_MCT4,
	claim_party_addr_MCT4,
	claim_party_city_MCT4,
	claim_party_state_MCT4,
	claim_party_zip_MCT4,
	claim_party_zip4_MCT4,
	ph_num_MCT4,
	ph_extension_MCT4,
	claimant_4_rep_Ind_MCA4,
	tax_id_MCA4,
	claim_party_last_name_MCA4,
	claim_party_first_name_MCA4,
	claimant4_rep_firm AS claimant4_rep_firm_MCA4,
	claim_party_addr_MCA4,
	claim_party_city_MCA4,
	claim_party_state_MCA4,
	claim_party_zip_MCA4,
	claim_party_zip4_MCA4,
	ph_num_MCA4,
	ph_extension_MCA4,
	tpoc_date2,
	tpoc_amt2,
	tpoc_fund_delay_date2,
	tpoc_date3,
	tpoc_amt3,
	tpoc_fund_delay_date3,
	tpoc_date4,
	tpoc_amt4,
	tpoc_fund_delay_date4,
	tpoc_date5,
	tpoc_amt5,
	tpoc_fund_delay_date5,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(record_identifier)
	UDF_DEFAULT_VALUE_TO_BLANKS(record_identifier) AS record_identifier1,
	cms_document_cntl_num AS cms_document_cntl_num1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(medicare_hicn)
	UDF_DEFAULT_VALUE_TO_BLANKS(medicare_hicn) AS medicare_hicn1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(tax_ssn_id)
	UDF_DEFAULT_VALUE_TO_BLANKS(tax_ssn_id) AS tax_ssn_id1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_last_name)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_last_name) AS claim_party_last_name1,
	-- *INF*: :UDF.REPLACE_NON_ALPHA_WITH_BLANKS(:UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_first_name))
	UDF_REPLACE_NON_ALPHA_WITH_BLANKS(UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_first_name)) AS claim_party_first_name1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claimant_2_Ind_MCT2)
	UDF_DEFAULT_VALUE_TO_BLANKS(claimant_2_Ind_MCT2) AS claimant_2_Ind_MCT21,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(tax_id_MCT2)
	UDF_DEFAULT_VALUE_TO_BLANKS(tax_id_MCT2) AS tax_id_MCT21,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_last_name_MCT2)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_last_name_MCT2) AS claim_party_last_name_MCT21,
	-- *INF*: :UDF.REPLACE_NON_ALPHA_WITH_BLANKS(:UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_first_name_MCT2))
	UDF_REPLACE_NON_ALPHA_WITH_BLANKS(UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_first_name_MCT2)) AS claim_party_first_name_MCT21,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_mid_name_MCT2)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_mid_name_MCT2) AS claim_party_mid_name_MCT21,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT2)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_addr_MCT2))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT2) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_addr_MCT2)
	) AS claim_party_addr_MCT21,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT2)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(substr(claim_party_addr_MCT2,51)))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT2) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(substr(claim_party_addr_MCT2, 51))
	) AS claim_party_addr_Ln2_MCT21,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT2)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_city_MCT2))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT2) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_city_MCT2)
	) AS claim_party_city_MCT21,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT2)='FC','FC',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_state_MCT2))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT2) = 'FC', 'FC',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_state_MCT2)
	) AS claim_party_state_MCT21,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT2)='FC','00000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(claim_party_zip_MCT2))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT2) = 'FC', '00000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(claim_party_zip_MCT2)
	) AS claim_party_zip_MCT21,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT2)='FC','0000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(claim_party_zip4_MCT2))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT2) = 'FC', '0000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(claim_party_zip4_MCT2)
	) AS claim_party_zip4_MCT21,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT2)='FC','0000000000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(ph_num_MCT2))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT2) = 'FC', '0000000000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(ph_num_MCT2)
	) AS ph_num_MCT21,
	-- *INF*: IIF(:UDF.DEFAULT_VALUE_TO_BLANKS(claimant_2_Ind_MCT2)='','',:UDF.DEFAULT_VALUE_TO_BLANKS(ph_extension_MCT2))
	-- 
	-- 
	IFF(
	    UDF_DEFAULT_VALUE_TO_BLANKS(claimant_2_Ind_MCT2) = '', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(ph_extension_MCT2)
	) AS ph_extension_MCT21,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claimant_2_Rep_Ind_MCA2)
	UDF_DEFAULT_VALUE_TO_BLANKS(claimant_2_Rep_Ind_MCA2) AS claimant_2_Rep_Ind_MCA21,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(tax_id_MCA2)
	UDF_DEFAULT_VALUE_TO_BLANKS(tax_id_MCA2) AS tax_id_MCA21,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_last_name_MCA2)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_last_name_MCA2) AS claim_party_last_name_MCA21,
	-- *INF*: :UDF.REPLACE_NON_ALPHA_WITH_BLANKS(:UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_first_name_MCA2))
	UDF_REPLACE_NON_ALPHA_WITH_BLANKS(UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_first_name_MCA2)) AS claim_party_first_name_MCA21,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claimant2_rep_firm_MCA2)
	UDF_DEFAULT_VALUE_TO_BLANKS(claimant2_rep_firm_MCA2) AS claimant2_rep_firm_MCA21,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_mid_name_MCA2)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_mid_name_MCA2) AS claim_party_mid_name_MCA21,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA2)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_addr_MCA2))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA2) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_addr_MCA2)
	) AS claim_party_addr_MCA21,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA2)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(substr(claim_party_addr_MCA2,51)))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA2) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(substr(claim_party_addr_MCA2, 51))
	) AS claim_party_addr_Ln2_MCA21,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA2)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_city_MCA2))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA2) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_city_MCA2)
	) AS claim_party_city_MCA21,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA2)='FC','FC',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_state_MCA2))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA2) = 'FC', 'FC',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_state_MCA2)
	) AS claim_party_state_MCA21,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA2)='FC','00000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(claim_party_zip_MCA2))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA2) = 'FC', '00000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(claim_party_zip_MCA2)
	) AS claim_party_zip_MCA21,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA2)='FC','0000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(claim_party_zip4_MCA2))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA2) = 'FC', '0000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(claim_party_zip4_MCA2)
	) AS claim_party_zip4_MCA21,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA2)='FC','0000000000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(ph_num_MCA2))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA2) = 'FC', '0000000000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(ph_num_MCA2)
	) AS ph_num_MCA21,
	-- *INF*: IIF(:UDF.DEFAULT_VALUE_TO_BLANKS
	-- (claimant_2_Rep_Ind_MCA2)='','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(ph_extension_MCA2))
	IFF(
	    UDF_DEFAULT_VALUE_TO_BLANKS(claimant_2_Rep_Ind_MCA2) = '', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(ph_extension_MCA2)
	) AS ph_extension_MCA21,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claimant_3_Ind_MCT3)
	UDF_DEFAULT_VALUE_TO_BLANKS(claimant_3_Ind_MCT3) AS claimant_3_Ind_MCT31,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(tax_id_MCT3)
	UDF_DEFAULT_VALUE_TO_BLANKS(tax_id_MCT3) AS tax_id_MCT31,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_last_name_MCT3)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_last_name_MCT3) AS claim_party_last_name_MCT31,
	-- *INF*: :UDF.REPLACE_NON_ALPHA_WITH_BLANKS(:UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_first_name_MCT3))
	UDF_REPLACE_NON_ALPHA_WITH_BLANKS(UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_first_name_MCT3)) AS claim_party_first_name_MCT31,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_mid_name_MCT3)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_mid_name_MCT3) AS claim_party_mid_name_MCT31,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT3)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_addr_MCT3))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT3) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_addr_MCT3)
	) AS claim_party_addr_MCT31,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT3)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(substr(claim_party_addr_MCT3,51)))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT3) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(substr(claim_party_addr_MCT3, 51))
	) AS claim_party_addr_Ln2_MCT31,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT3)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_city_MCT3))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT3) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_city_MCT3)
	) AS claim_party_city_MCT31,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT3)='FC','FC',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_state_MCT3))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT3) = 'FC', 'FC',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_state_MCT3)
	) AS claim_party_state_MCT31,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT3)='FC','00000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(claim_party_zip_MCT3))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT3) = 'FC', '00000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(claim_party_zip_MCT3)
	) AS claim_party_zip_MCT31,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT3)='FC','0000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(claim_party_zip4_MCT3))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT3) = 'FC', '0000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(claim_party_zip4_MCT3)
	) AS claim_party_zip4_MCT31,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT3)='FC','0000000000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(ph_num_MCT3))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT3) = 'FC', '0000000000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(ph_num_MCT3)
	) AS ph_num_MCT31,
	-- *INF*: IIF(:UDF.DEFAULT_VALUE_TO_BLANKS(claimant_3_Ind_MCT3)='','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(ph_extension_MCT3))
	IFF(
	    UDF_DEFAULT_VALUE_TO_BLANKS(claimant_3_Ind_MCT3) = '', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(ph_extension_MCT3)
	) AS ph_extension_MCT31,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claimant_3_Rep_Ind_MCA3)
	UDF_DEFAULT_VALUE_TO_BLANKS(claimant_3_Rep_Ind_MCA3) AS claimant_3_Rep_Ind_MCA31,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(tax_id_MCA3)
	UDF_DEFAULT_VALUE_TO_BLANKS(tax_id_MCA3) AS tax_id_MCA31,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_last_name_MCA3)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_last_name_MCA3) AS claim_party_last_name_MCA31,
	-- *INF*: :UDF.REPLACE_NON_ALPHA_WITH_BLANKS(:UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_first_name_MCA3))
	UDF_REPLACE_NON_ALPHA_WITH_BLANKS(UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_first_name_MCA3)) AS claim_party_first_name_MCA31,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claimant3_rep_firm_MCA3)
	UDF_DEFAULT_VALUE_TO_BLANKS(claimant3_rep_firm_MCA3) AS claimant3_rep_firm_MCA31,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_mid_name_MCA3)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_mid_name_MCA3) AS claim_party_mid_name_MCA31,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA3)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_addr_MCA3))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA3) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_addr_MCA3)
	) AS claim_party_addr_MCA31,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA3)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(substr(claim_party_addr_MCA3,51)))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA3) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(substr(claim_party_addr_MCA3, 51))
	) AS claim_party_addr_Ln2_MCA31,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA3)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_city_MCA3))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA3) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_city_MCA3)
	) AS claim_party_city_MCA31,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA3)='FC','FC',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_state_MCA3))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA3) = 'FC', 'FC',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_state_MCA3)
	) AS claim_party_state_MCA31,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA3)='FC','00000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(claim_party_zip_MCA3))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA3) = 'FC', '00000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(claim_party_zip_MCA3)
	) AS claim_party_zip_MCA31,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA3)='FC','0000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(claim_party_zip4_MCA3))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA3) = 'FC', '0000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(claim_party_zip4_MCA3)
	) AS claim_party_zip4_MCA31,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA3)='FC','0000000000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(ph_num_MCA3))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA3) = 'FC', '0000000000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(ph_num_MCA3)
	) AS ph_num_MCA31,
	-- *INF*: IIF(:UDF.DEFAULT_VALUE_TO_BLANKS(claimant3_rep_firm_MCA3)='','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(ph_extension_MCA3))
	IFF(
	    UDF_DEFAULT_VALUE_TO_BLANKS(claimant3_rep_firm_MCA3) = '', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(ph_extension_MCA3)
	) AS ph_extension_MCA31,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claimant_4_Ind_MCT4)
	UDF_DEFAULT_VALUE_TO_BLANKS(claimant_4_Ind_MCT4) AS claimant_4_Ind_MCT41,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(tax_id_MCT4)
	UDF_DEFAULT_VALUE_TO_BLANKS(tax_id_MCT4) AS tax_id_MCT41,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_last_name_MCT4)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_last_name_MCT4) AS claim_party_last_name_MCT41,
	-- *INF*: :UDF.REPLACE_NON_ALPHA_WITH_BLANKS(:UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_first_name_MCT4))
	UDF_REPLACE_NON_ALPHA_WITH_BLANKS(UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_first_name_MCT4)) AS claim_party_first_name_MCT41,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_mid_name_MCT4)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_mid_name_MCT4) AS claim_party_mid_name_MCT41,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT4)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_addr_MCT4))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT4) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_addr_MCT4)
	) AS claim_party_addr_MCT41,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT4)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(substr(claim_party_addr_MCT4,51)))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT4) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(substr(claim_party_addr_MCT4, 51))
	) AS claim_party_addr_Ln2_MCT41,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT4)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_city_MCT4))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT4) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_city_MCT4)
	) AS claim_party_city_MCT41,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT4)='FC','FC',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_state_MCT4))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT4) = 'FC', 'FC',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_state_MCT4)
	) AS claim_party_state_MCT41,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT4)='FC','00000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(claim_party_zip_MCT4))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT4) = 'FC', '00000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(claim_party_zip_MCT4)
	) AS claim_party_zip_MCT41,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT4)='FC','0000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(claim_party_zip4_MCT4))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT4) = 'FC', '0000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(claim_party_zip4_MCT4)
	) AS claim_party_zip4_MCT41,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCT4)='FC','0000000000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(ph_num_MCT4))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCT4) = 'FC', '0000000000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(ph_num_MCT4)
	) AS ph_num_MCT41,
	-- *INF*: IIF(:UDF.DEFAULT_VALUE_TO_BLANKS(claimant_4_Ind_MCT4)='','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(ph_extension_MCT4))
	IFF(
	    UDF_DEFAULT_VALUE_TO_BLANKS(claimant_4_Ind_MCT4) = '', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(ph_extension_MCT4)
	) AS ph_extension_MCT41,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claimant_4_rep_Ind_MCA4)
	UDF_DEFAULT_VALUE_TO_BLANKS(claimant_4_rep_Ind_MCA4) AS claimant_4_rep_Ind_MCA41,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(tax_id_MCA4)
	UDF_DEFAULT_VALUE_TO_BLANKS(tax_id_MCA4) AS tax_id_MCA41,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_last_name_MCA4)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_last_name_MCA4) AS claim_party_last_name_MCA41,
	-- *INF*: :UDF.REPLACE_NON_ALPHA_WITH_BLANKS(:UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_first_name_MCA4))
	UDF_REPLACE_NON_ALPHA_WITH_BLANKS(UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_first_name_MCA4)) AS claim_party_first_name_MCA41,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claimant4_rep_firm_MCA4)
	UDF_DEFAULT_VALUE_TO_BLANKS(claimant4_rep_firm_MCA4) AS claimant4_rep_firm_MCA41,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA4)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_addr_MCA4))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA4) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_addr_MCA4)
	) AS claim_party_addr_MCA41,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA4)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(substr(claim_party_addr_MCA4,51)))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA4) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(substr(claim_party_addr_MCA4, 51))
	) AS claim_party_addr_Ln4_MCA41,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA4)='FC','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_city_MCA4))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA4) = 'FC', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_city_MCA4)
	) AS claim_party_city_MCA41,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA4)='FC','FC',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_state_MCA4))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA4) = 'FC', 'FC',
	    UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_state_MCA4)
	) AS claim_party_state_MCA41,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA4)='FC','00000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(claim_party_zip_MCA4))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA4) = 'FC', '00000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(claim_party_zip_MCA4)
	) AS claim_party_zip_MCA41,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA4)='FC','0000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(claim_party_zip4_MCA4))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA4) = 'FC', '0000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(claim_party_zip4_MCA4)
	) AS claim_party_zip4_MCA41,
	-- *INF*: IIF(:UDF.STATE_VALUE(claim_party_state_MCA4)='FC','0000000000',
	-- :UDF.DEFAULT_VALUE_TO_ZEROS(ph_num_MCA4))
	IFF(
	    UDF_STATE_VALUE(claim_party_state_MCA4) = 'FC', '0000000000',
	    UDF_DEFAULT_VALUE_TO_ZEROS(ph_num_MCA4)
	) AS ph_num_MCA41,
	-- *INF*: IIF(:UDF.DEFAULT_VALUE_TO_BLANKS(claimant_4_rep_Ind_MCA4)='','',
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(ph_extension_MCA4))
	IFF(
	    UDF_DEFAULT_VALUE_TO_BLANKS(claimant_4_rep_Ind_MCA4) = '', '',
	    UDF_DEFAULT_VALUE_TO_BLANKS(ph_extension_MCA4)
	) AS ph_extension_MCA41,
	-- *INF*: IIF(ISNULL(tpoc_date2) OR tpoc_date2='18000101','00000000',tpoc_date2)
	IFF(tpoc_date2 IS NULL OR tpoc_date2 = '18000101', '00000000', tpoc_date2) AS tpoc_date21,
	-- *INF*: IIF(ISNULL(tpoc_amt2) OR tpoc_amt2=0,'00000000000',lpad(TO_CHAR(ROUND(tpoc_amt2,2)*100),11,'0'))
	IFF(
	    tpoc_amt2 IS NULL OR tpoc_amt2 = 0, '00000000000',
	    lpad(TO_CHAR(ROUND(tpoc_amt2, 2) * 100), 11, '0')
	) AS tpoc_amt21,
	-- *INF*: IIF(ISNULL(tpoc_fund_delay_date2) OR tpoc_fund_delay_date2='18000101','00000000',tpoc_fund_delay_date2)
	IFF(
	    tpoc_fund_delay_date2 IS NULL OR tpoc_fund_delay_date2 = '18000101', '00000000',
	    tpoc_fund_delay_date2
	) AS tpoc_fund_delay_date21,
	-- *INF*: IIF(ISNULL(tpoc_date3) OR tpoc_date3='18000101','00000000',tpoc_date3)
	IFF(tpoc_date3 IS NULL OR tpoc_date3 = '18000101', '00000000', tpoc_date3) AS tpoc_date31,
	-- *INF*: IIF(ISNULL(tpoc_amt3) OR tpoc_amt3=0,'00000000000',lpad(TO_CHAR(ROUND(tpoc_amt3,2)*100),11,'0'))
	IFF(
	    tpoc_amt3 IS NULL OR tpoc_amt3 = 0, '00000000000',
	    lpad(TO_CHAR(ROUND(tpoc_amt3, 2) * 100), 11, '0')
	) AS tpoc_amt31,
	-- *INF*: IIF(ISNULL(tpoc_fund_delay_date3) OR tpoc_fund_delay_date3='18000101','00000000',tpoc_fund_delay_date3)
	IFF(
	    tpoc_fund_delay_date3 IS NULL OR tpoc_fund_delay_date3 = '18000101', '00000000',
	    tpoc_fund_delay_date3
	) AS tpoc_fund_delay_date31,
	-- *INF*: IIF(ISNULL(tpoc_date4) OR tpoc_date4='18000101','00000000',tpoc_date4)
	IFF(tpoc_date4 IS NULL OR tpoc_date4 = '18000101', '00000000', tpoc_date4) AS tpoc_date41,
	-- *INF*: IIF(ISNULL(tpoc_amt4) OR tpoc_amt4=0,'00000000000',lpad(TO_CHAR(ROUND(tpoc_amt4,2)*100),11,'0'))
	IFF(
	    tpoc_amt4 IS NULL OR tpoc_amt4 = 0, '00000000000',
	    lpad(TO_CHAR(ROUND(tpoc_amt4, 2) * 100), 11, '0')
	) AS tpoc_amt41,
	-- *INF*: IIF(ISNULL(tpoc_fund_delay_date4) OR tpoc_fund_delay_date4='18000101','00000000',tpoc_fund_delay_date4)
	IFF(
	    tpoc_fund_delay_date4 IS NULL OR tpoc_fund_delay_date4 = '18000101', '00000000',
	    tpoc_fund_delay_date4
	) AS tpoc_fund_delay_date41,
	-- *INF*: IIF(ISNULL(tpoc_date5) OR tpoc_date5='18000101','00000000',tpoc_date5)
	IFF(tpoc_date5 IS NULL OR tpoc_date5 = '18000101', '00000000', tpoc_date5) AS tpoc_date51,
	-- *INF*: IIF(ISNULL(tpoc_amt5) OR tpoc_amt5=0,'00000000000',lpad(TO_CHAR(ROUND(tpoc_amt5,2)*100),11,'0'))
	IFF(
	    tpoc_amt5 IS NULL OR tpoc_amt5 = 0, '00000000000',
	    lpad(TO_CHAR(ROUND(tpoc_amt5, 2) * 100), 11, '0')
	) AS tpoc_amt51,
	-- *INF*: IIF(ISNULL(tpoc_fund_delay_date5) OR tpoc_fund_delay_date5='18000101','00000000',tpoc_fund_delay_date5)
	IFF(
	    tpoc_fund_delay_date5 IS NULL OR tpoc_fund_delay_date5 = '18000101', '00000000',
	    tpoc_fund_delay_date5
	) AS tpoc_fund_delay_date51,
	'' AS DEFAULT_BLANKS,
	SYSDATE AS CURRENT_DATE,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID
	FROM FIL_Valid_Rows
),
work_claim_cms_auxiliary_extract AS (
	INSERT INTO work_claim_cms_auxiliary_extract
	(claim_med_ak_id, claim_party_occurrence_ak_id, claim_med_plan_ak_id, claim_occurrence_ak_id, claim_party_ak_id, record_identifier, dcn, injured_party_hicn, injured_party_ssn, injured_party_last_name, injured_party_first_name, c2_relationship, c2_tin, c2_last_name, c2_first_name, c2_middle_initial, c2_mail_addr_1, c2_mail_addr_2, c2_city, c2_state, c2_zip, c2_zip4, c2_ph, c2_ph_extension, reserved_20, c2_rep_ind, c2_rep_last_name, c2_rep_first_name, c2_rep_firm_name, c2_rep_tin, c2_rep_mail_addr_1, c2_rep_mail_addr_2, c2_rep_city, c2_rep_state, c2_rep_zip, c2_rep_zip4, c2_rep_ph, c2_rep_ph_extension, reserved_34, c3_relationship, c3_tin, c3_last_name, c3_first_name, c3_middle_initial, c3_mail_addr_1, c3_mail_addr_2, c3_city, c3_state, c3_zip, c3_zip4, c3_ph, c3_ph_extension, reserved_48, c3_rep_ind, c3_rep_last_name, c3_rep_first_name, c3_rep_firm_name, c3_rep_tin, c3_rep_mail_addr_1, c3_rep_mail_addr_2, c3_rep_city, c3_rep_state, c3_rep_zip, c3_rep_zip4, c3_rep_ph, c3_rep_ph_extension, reserved_62, c4_relationship, c4_tin, c4_last_name, c4_first_name, c4_middle_initial, c4_mail_addr_1, c4_mail_addr_2, c4_city, c4_state, c4_zip, c4_zip4, c4_ph, c4_ph_extension, reserved_76, c4_rep_ind, c4_rep_last_name, c4_rep_first_name, c4_rep_firm_name, c4_rep_tin, c4_rep_mail_addr_1, c4_rep_mail_addr_2, c4_rep_city, c4_rep_state, c4_rep_zip, c4_rep_zip4, c4_rep_ph, c4_rep_ph_extension, tpoc_date_2, tpoc_amount_2, funding_dld_tpoc_st_date_2, tpoc_date_3, tpoc_amount_3, funding_dld_tpoc_st_date_3, tpoc_date_4, tpoc_amount_4, funding_dld_tpoc_st_date_4, tpoc_date_5, tpoc_amount_5, funding_dld_tpoc_st_date_5, reserved_102, created_date, modified_date, audit_id)
	SELECT 
	CLAIM_MED_AK_ID, 
	CLAIM_PARTY_OCCURRENCE_AK_ID, 
	CLAIM_MED_PLAN_AK_ID, 
	CLAIM_OCCURRENCE_AK_ID, 
	CLAIM_PARTY_AK_ID, 
	record_identifier1 AS RECORD_IDENTIFIER, 
	cms_document_cntl_num1 AS DCN, 
	medicare_hicn1 AS INJURED_PARTY_HICN, 
	tax_ssn_id1 AS INJURED_PARTY_SSN, 
	claim_party_last_name1 AS INJURED_PARTY_LAST_NAME, 
	claim_party_first_name1 AS INJURED_PARTY_FIRST_NAME, 
	claimant_2_Ind_MCT21 AS C2_RELATIONSHIP, 
	tax_id_MCT21 AS C2_TIN, 
	claim_party_last_name_MCT21 AS C2_LAST_NAME, 
	claim_party_first_name_MCT21 AS C2_FIRST_NAME, 
	claim_party_mid_name_MCT21 AS C2_MIDDLE_INITIAL, 
	claim_party_addr_MCT21 AS C2_MAIL_ADDR_1, 
	claim_party_addr_Ln2_MCT21 AS C2_MAIL_ADDR_2, 
	claim_party_city_MCT21 AS C2_CITY, 
	claim_party_state_MCT21 AS C2_STATE, 
	claim_party_zip_MCT21 AS C2_ZIP, 
	claim_party_zip4_MCT21 AS C2_ZIP4, 
	ph_num_MCT21 AS C2_PH, 
	ph_extension_MCT21 AS C2_PH_EXTENSION, 
	DEFAULT_BLANKS AS RESERVED_20, 
	claimant_2_Rep_Ind_MCA21 AS C2_REP_IND, 
	claim_party_last_name_MCA21 AS C2_REP_LAST_NAME, 
	claim_party_first_name_MCA21 AS C2_REP_FIRST_NAME, 
	claimant2_rep_firm_MCA21 AS C2_REP_FIRM_NAME, 
	tax_id_MCA21 AS C2_REP_TIN, 
	claim_party_addr_MCA21 AS C2_REP_MAIL_ADDR_1, 
	claim_party_addr_Ln2_MCA21 AS C2_REP_MAIL_ADDR_2, 
	claim_party_city_MCA21 AS C2_REP_CITY, 
	claim_party_state_MCA21 AS C2_REP_STATE, 
	claim_party_zip_MCA21 AS C2_REP_ZIP, 
	claim_party_zip4_MCA21 AS C2_REP_ZIP4, 
	ph_num_MCA21 AS C2_REP_PH, 
	ph_extension_MCA21 AS C2_REP_PH_EXTENSION, 
	DEFAULT_BLANKS AS RESERVED_34, 
	claimant_3_Ind_MCT31 AS C3_RELATIONSHIP, 
	tax_id_MCT31 AS C3_TIN, 
	claim_party_last_name_MCT31 AS C3_LAST_NAME, 
	claim_party_first_name_MCT31 AS C3_FIRST_NAME, 
	claim_party_mid_name_MCT31 AS C3_MIDDLE_INITIAL, 
	claim_party_addr_MCT31 AS C3_MAIL_ADDR_1, 
	claim_party_addr_Ln2_MCT31 AS C3_MAIL_ADDR_2, 
	claim_party_city_MCT31 AS C3_CITY, 
	claim_party_state_MCT31 AS C3_STATE, 
	claim_party_zip_MCT31 AS C3_ZIP, 
	claim_party_zip4_MCT31 AS C3_ZIP4, 
	ph_num_MCT31 AS C3_PH, 
	ph_extension_MCT31 AS C3_PH_EXTENSION, 
	DEFAULT_BLANKS AS RESERVED_48, 
	claimant_3_Rep_Ind_MCA31 AS C3_REP_IND, 
	claim_party_last_name_MCA31 AS C3_REP_LAST_NAME, 
	claim_party_first_name_MCA31 AS C3_REP_FIRST_NAME, 
	claimant3_rep_firm_MCA31 AS C3_REP_FIRM_NAME, 
	tax_id_MCA31 AS C3_REP_TIN, 
	claim_party_addr_MCA31 AS C3_REP_MAIL_ADDR_1, 
	claim_party_addr_Ln2_MCA31 AS C3_REP_MAIL_ADDR_2, 
	claim_party_city_MCA31 AS C3_REP_CITY, 
	claim_party_state_MCA31 AS C3_REP_STATE, 
	claim_party_zip_MCA31 AS C3_REP_ZIP, 
	claim_party_zip4_MCA31 AS C3_REP_ZIP4, 
	ph_num_MCA31 AS C3_REP_PH, 
	ph_extension_MCA31 AS C3_REP_PH_EXTENSION, 
	DEFAULT_BLANKS AS RESERVED_62, 
	claimant_4_Ind_MCT41 AS C4_RELATIONSHIP, 
	tax_id_MCT41 AS C4_TIN, 
	claim_party_last_name_MCT41 AS C4_LAST_NAME, 
	claim_party_first_name_MCT41 AS C4_FIRST_NAME, 
	claim_party_mid_name_MCT41 AS C4_MIDDLE_INITIAL, 
	claim_party_addr_MCT41 AS C4_MAIL_ADDR_1, 
	claim_party_addr_Ln2_MCT41 AS C4_MAIL_ADDR_2, 
	claim_party_city_MCT41 AS C4_CITY, 
	claim_party_state_MCT41 AS C4_STATE, 
	claim_party_zip_MCT41 AS C4_ZIP, 
	claim_party_zip4_MCT41 AS C4_ZIP4, 
	ph_num_MCT41 AS C4_PH, 
	ph_extension_MCT41 AS C4_PH_EXTENSION, 
	DEFAULT_BLANKS AS RESERVED_76, 
	claimant_4_rep_Ind_MCA41 AS C4_REP_IND, 
	claim_party_last_name_MCA41 AS C4_REP_LAST_NAME, 
	claim_party_first_name_MCA41 AS C4_REP_FIRST_NAME, 
	claimant4_rep_firm_MCA41 AS C4_REP_FIRM_NAME, 
	tax_id_MCA41 AS C4_REP_TIN, 
	claim_party_addr_MCA41 AS C4_REP_MAIL_ADDR_1, 
	claim_party_addr_Ln4_MCA41 AS C4_REP_MAIL_ADDR_2, 
	claim_party_city_MCA41 AS C4_REP_CITY, 
	claim_party_state_MCA41 AS C4_REP_STATE, 
	claim_party_zip_MCA41 AS C4_REP_ZIP, 
	claim_party_zip4_MCA41 AS C4_REP_ZIP4, 
	ph_num_MCA41 AS C4_REP_PH, 
	ph_extension_MCA41 AS C4_REP_PH_EXTENSION, 
	tpoc_date21 AS TPOC_DATE_2, 
	tpoc_amt21 AS TPOC_AMOUNT_2, 
	tpoc_fund_delay_date21 AS FUNDING_DLD_TPOC_ST_DATE_2, 
	tpoc_date31 AS TPOC_DATE_3, 
	tpoc_amt31 AS TPOC_AMOUNT_3, 
	tpoc_fund_delay_date31 AS FUNDING_DLD_TPOC_ST_DATE_3, 
	tpoc_date41 AS TPOC_DATE_4, 
	tpoc_amt41 AS TPOC_AMOUNT_4, 
	tpoc_fund_delay_date41 AS FUNDING_DLD_TPOC_ST_DATE_4, 
	tpoc_date51 AS TPOC_DATE_5, 
	tpoc_amt51 AS TPOC_AMOUNT_5, 
	tpoc_fund_delay_date51 AS FUNDING_DLD_TPOC_ST_DATE_5, 
	DEFAULT_BLANKS AS RESERVED_102, 
	CURRENT_DATE AS CREATED_DATE, 
	CURRENT_DATE AS MODIFIED_DATE, 
	AUDIT_ID AS AUDIT_ID
	FROM EXP_Target
),
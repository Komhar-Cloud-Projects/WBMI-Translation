WITH
LKP_All_Support_tables AS (
	SELECT
	descript,
	source_sys_id,
	tablename,
	code
	FROM (
		SELECT 
			bill_plan_use_code_descript as descript,
			'sup_bill_plan_use_code' AS tablename ,
			LTRIM(RTRIM(bill_plan_use_code)) as code
		FROM 	sup_bill_plan_use_code 
		WHERE   sup_bill_plan_use_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			bill_type_code_descript as descript,
			'sup_bill_type_code' AS tablename ,
			LTRIM(RTRIM(bill_type_code)) as code
		FROM 	sup_bill_type_code 
		WHERE   sup_bill_type_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			bill_class_code_descript as descript,
			'sup_bill_class_code' AS tablename ,
			LTRIM(RTRIM(bill_class_code)) as code
		FROM 	sup_bill_class_code 
		WHERE   sup_bill_class_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			bill_trans_code_descript as descript,
			'sup_bill_transaction_code' AS tablename ,
			LTRIM(RTRIM(bill_trans_code)) as code	
		FROM 	sup_bill_transaction_code 
		WHERE   sup_bill_transaction_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			ars_type_code_descript as descript,
			'sup_bill_ars_type_code' AS tablename ,
			LTRIM(RTRIM(ars_type_code)) as code
		FROM 	sup_bill_ars_type_code 
		WHERE   sup_bill_ars_type_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			payby_code_descript as descript,
			'sup_payby_code' AS tablename ,
			LTRIM(RTRIM(payby_code)) as code
		FROM 	sup_payby_code 
		WHERE   sup_payby_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			mrktng_pkg_description as descript,
			'sup_marketing_package_code' AS tablename ,
			LTRIM(RTRIM(mrktng_pkg_code)) as code
		FROM 	sup_marketing_package_code
		WHERE   sup_marketing_package_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			excess_claim_code_descript as descript,
			'sup_excess_claim_code' AS tablename ,
			LTRIM(RTRIM(excess_claim_code)) as code
		FROM 	sup_excess_claim_code
		WHERE   sup_excess_claim_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			non_smoker_disc_code_descript as descript,
			'sup_non_smoker_discount_code' AS tablename ,
			LTRIM(RTRIM(non_smoker_disc_code)) as code
		FROM 	sup_non_smoker_discount_code
		WHERE   sup_non_smoker_discount_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			variation_code_descript as descript,
			'sup_policy_variation_code' AS tablename ,
			LTRIM(RTRIM(variation_code)) as code	
		FROM 	sup_policy_variation_code
		WHERE   sup_policy_variation_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			pol_audit_frqncy_descript as descript,
			'sup_policy_audit_frequency' AS tablename ,
			LTRIM(RTRIM(pol_audit_frqncy)) as code
		FROM 	sup_policy_audit_frequency
		WHERE   sup_policy_audit_frequency.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			renl_code_descript as descript,
			'sup_policy_renewal_code' AS tablename ,
			LTRIM(RTRIM(renl_code)) as code
		FROM 	sup_policy_renewal_code
		WHERE   sup_policy_renewal_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			mvr_request_code_descript as descript,
			'sup_mvr_request_code' AS tablename ,
			LTRIM(RTRIM(mvr_request_code)) as code	
		FROM 	sup_mvr_request_code
		WHERE   sup_mvr_request_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			industry_risk_grade_code_descript as descript,
			'sup_industry_risk_grade_code' AS tablename ,
			LTRIM(RTRIM(industry_risk_grade_code)) as code	
		FROM 	sup_industry_risk_grade_code
		WHERE   sup_industry_risk_grade_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			pol_issue_code_descript as descript,
			'sup_policy_issue_code' AS tablename ,
			LTRIM(RTRIM(pol_issue_code)) as code
		FROM 	sup_policy_issue_code
		WHERE   sup_policy_issue_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT
			pol_status_code_descript as descript, 
			'sup_policy_status_code' AS tablename ,
			LTRIM(RTRIM(pol_status_code)) as code
		FROM 	sup_policy_status_code
		WHERE   sup_policy_status_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT
			pol_term_descript as descript, 
			'sup_policy_term' AS tablename ,
			LTRIM(RTRIM(pol_term)) as code
		FROM 	sup_policy_term
		WHERE   sup_policy_term.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			serv_center_support_code_descript as descript,
			'sup_service_center_support_code' AS tablename ,
			LTRIM(RTRIM(serv_center_support_code)) as code
		FROM 	sup_service_center_support_code
		WHERE   sup_service_center_support_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			wbconnect_upload_code_descript as descript,
			'sup_wbconnect_upload_code' AS tablename ,
			LTRIM(RTRIM(wbconnect_upload_code)) as code
		FROM 	sup_wbconnect_upload_code
		WHERE   sup_wbconnect_upload_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			state_descript as descript,
			'sup_state' AS tablename ,
			LTRIM(RTRIM(state_abbrev)) as code
		FROM 	sup_state
		WHERE   sup_state.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			state_descript as descript,
			'sup_state' AS tablename ,
			LTRIM(RTRIM(state_code)) as code
		FROM 	sup_state
		WHERE   sup_state.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			state_code as descript,
			'sup_state_abbrev' AS tablename ,
			LTRIM(RTRIM(state_abbrev)) as code
		FROM 	sup_state
		WHERE   sup_state.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			pol_co_num_descript as descript,
			'sup_policy_company_number' AS tablename ,
			LTRIM(RTRIM(pol_co_num)) as code
		FROM 	sup_policy_company_number
		WHERE   sup_policy_company_number.crrnt_snpsht_flag=1
		
		UNION ALL
		SELECT 
			pol_co_line_code_descript as descript,
			'sup_policy_company_line_code' AS tablename ,
			LTRIM(RTRIM(pol_co_line_code)) as code
		FROM 	sup_policy_company_line_code
		WHERE   sup_policy_company_line_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			reins_code_descript as descript,
			'sup_reinsurance_code' AS tablename ,
			LTRIM(RTRIM(reins_code)) as code
		FROM 	sup_reinsurance_code
		WHERE   sup_reinsurance_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			bus_class_code_descript as descript,
			'sup_business_classification_code' AS tablename ,
			LTRIM(RTRIM(bus_class_code)) as code
		FROM 	sup_business_classification_code
		WHERE   sup_business_classification_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			target_mrkt_code_descript as descript,
			'sup_target_market_code' AS tablename ,
			LTRIM(RTRIM(target_mrkt_code)) as code
		FROM 	sup_target_market_code
		WHERE   sup_target_market_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			assoc_prog_code_descript as descript,
			'sup_association_program_code' AS tablename ,
			LTRIM(RTRIM(assoc_prog_code)) as code
		FROM 	sup_association_program_code
		WHERE   sup_association_program_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			lgl_ent_code_descript as descript,
			'sup_legal_entity_code' AS tablename ,
			LTRIM(RTRIM(lgl_ent_code)) as code
		FROM 	sup_legal_entity_code
		WHERE   sup_legal_entity_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			cdt_rating_score_descript as descript,
			'sup_credit_rating_score' AS tablename ,
			LTRIM(RTRIM(cdt_rating_score)) as code
		FROM 	sup_credit_rating_score
		WHERE   sup_credit_rating_score.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			sic_code_descript as descript,
			'sup_sic_code' AS tablename ,
			LTRIM(RTRIM(sic_code)) as code
		FROM 	sup_sic_code
		WHERE   sup_sic_code.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY source_sys_id,tablename,code ORDER BY descript DESC) = 1
),
SQ_contract_customer_sources AS (

------------ PRE SQL ----------
truncate table dbo.work_contract_customer

insert dbo.work_contract_customer (contract_customer_ak_id ,
                    EFF_FROM_DATE)
SELECT    CONTRACT_CUST_AK_ID ,
                    EFF_FROM_DATE
          FROM      dbo.CONTRACT_CUSTOMER
          WHERE     CREATED_DATE >= '@{pipeline().parameters.SELECTION_START_TS}'
          UNION
          SELECT    CONTRACT_CUST_AK_ID ,
                    EFF_FROM_DATE
          FROM      dbo.CONTRACT_CUSTOMER_ADDRESS
          WHERE     CREATED_DATE >= '@{pipeline().parameters.SELECTION_START_TS}'
----------------------


	SELECT 
		CONT_CUST.contract_cust_id,
		CONT_CUST.source_sys_id,	
		CONT_CUST.contract_cust_ak_id,	
		CONT_CUST.cust_num,	
		CONT_CUST.contract_key,	
		CONT_CUST.cust_role,		
		CONT_CUST.name,	
		CONT_CUST.fed_tax_id,		
		CONT_CUST.doing_bus_as,		
		CONT_CUST.sic_code,	
		CONT_CUST.naics_code,		
		CONT_CUST.yr_in_bus,		
		CONT_CUST.ph_num_full,		
		CONT_CUST.ph_area_code,		
		CONT_CUST.ph_exchange,		
		CONT_CUST.ph_num,		
		CONT_CUST.ph_extension,
		CONT_CUST.bus_email_addr,
		CONT_CUST.sort_name,
		CONT_CUST_ADDR_M.contract_cust_addr_id AS contract_cust_addr_id_M,		
		CONT_CUST_ADDR_M.loc_unit_num AS loc_unit_num_M,
		CONT_CUST_ADDR_M.addr_line_1 AS addr_line_1_M ,			
		CONT_CUST_ADDR_M.addr_line_2 AS addr_line_2_M,
		CONT_CUST_ADDR_M.addr_line_3 AS addr_line_3_M,			
		CONT_CUST_ADDR_M.city_name AS city_name_M,
		CONT_CUST_ADDR_M.state_prov_code AS state_prov_code_M,		
		CONT_CUST_ADDR_M.zip_postal_code AS zip_postal_code_M,
		CONT_CUST_ADDR_M.zip_postal_code_extension AS zip_postal_code_extension_M,	
		CONT_CUST_ADDR_M.county_parish_name AS county_parish_name_M,
		CONT_CUST_ADDR_M.country_name AS country_name_M,
		CONT_CUST_ADDR_O.contract_cust_addr_id AS  contract_cust_addr_id_O,		
		CONT_CUST_ADDR_O.loc_unit_num AS loc_unit_num_O,
		CONT_CUST_ADDR_O.addr_line_1 AS addr_line_1_O,			
		CONT_CUST_ADDR_O.addr_line_2 AS addr_line_2_O,
		CONT_CUST_ADDR_O.addr_line_3 AS addr_line_3_O,			
		CONT_CUST_ADDR_O.city_name AS city_name_O,
		CONT_CUST_ADDR_O.state_prov_code AS state_prov_code_O,		
		CONT_CUST_ADDR_O.zip_postal_code AS zip_postal_code_O,
		CONT_CUST_ADDR_O.zip_postal_code_extension AS zip_postal_code_extension_O,	
		CONT_CUST_ADDR_O.county_parish_name AS county_parish_name_O,
		CONT_CUST_ADDR_O.country_name AS country_name_O,
		DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE AS DIST_EFF_FROM_DATE,
		CONT_CUST.sup_lgl_ent_code_id
	FROM dbo.work_contract_customer AS DISTINCT_EFF_FROM_DATES
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CONTRACT_CUSTOMER CONT_CUST
	ON DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN CONT_CUST.EFF_FROM_DATE AND CONT_CUST.EFF_TO_DATE
	AND DISTINCT_EFF_FROM_DATES.contract_customer_ak_id = CONT_CUST.CONTRACT_CUST_AK_ID
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CONTRACT_CUSTOMER_ADDRESS CONT_CUST_ADDR_M
	ON DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN CONT_CUST_ADDR_M.EFF_FROM_DATE AND CONT_CUST_ADDR_M.EFF_TO_DATE
	AND CONT_CUST.CONTRACT_CUST_AK_ID = CONT_CUST_ADDR_M.CONTRACT_CUST_AK_ID AND CONT_CUST_ADDR_M.ADDR_TYPE='MAILING'
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CONTRACT_CUSTOMER_ADDRESS CONT_CUST_ADDR_O
	ON DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN CONT_CUST_ADDR_O.EFF_FROM_DATE AND CONT_CUST_ADDR_O.EFF_TO_DATE
	AND CONT_CUST.CONTRACT_CUST_AK_ID = CONT_CUST_ADDR_O.CONTRACT_CUST_AK_ID AND CONT_CUST_ADDR_O.ADDR_TYPE='OTHER'
	
	WHERE CONT_CUST.contract_cust_id IS NOT NULL
),
LKP_v2_policy AS (
	SELECT
	ProgramAKId,
	contract_cust_ak_id
	FROM (
		SELECT 
		p.ProgramAKId as ProgramAKId, 
		p.contract_cust_ak_id 
		as contract_cust_ak_id
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy p
		WHERE p.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY contract_cust_ak_id ORDER BY ProgramAKId DESC) = 1
),
LKP_Program AS (
	SELECT
	ProgramCode,
	ProgramDescription,
	ProgramAKId
	FROM (
		SELECT 
			ProgramCode,
			ProgramDescription,
			ProgramAKId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Program
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProgramAKId ORDER BY ProgramCode) = 1
),
LKP_sup_legal_entity_code AS (
	SELECT
	StandardLegalEntityCode,
	StandardLegalEntityDescription,
	sup_lgl_ent_code_id,
	source_sys_id
	FROM (
		SELECT 
			StandardLegalEntityCode,
			StandardLegalEntityDescription,
			sup_lgl_ent_code_id,
			source_sys_id
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_legal_entity_code
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_lgl_ent_code_id,source_sys_id ORDER BY StandardLegalEntityCode) = 1
),
EXP_values AS (
	SELECT
	SQ_contract_customer_sources.contract_cust_id,
	SQ_contract_customer_sources.source_sys_id,
	SQ_contract_customer_sources.contract_cust_ak_id,
	SQ_contract_customer_sources.cust_num,
	SQ_contract_customer_sources.contract_key,
	SQ_contract_customer_sources.cust_role,
	SQ_contract_customer_sources.name,
	SQ_contract_customer_sources.fed_tax_id,
	SQ_contract_customer_sources.doing_bus_as,
	SQ_contract_customer_sources.sic_code,
	SQ_contract_customer_sources.naics_code,
	LKP_sup_legal_entity_code.StandardLegalEntityCode AS lgl_ent_code,
	LKP_sup_legal_entity_code.StandardLegalEntityDescription AS lgl_ent_code_descript,
	SQ_contract_customer_sources.yr_in_bus,
	SQ_contract_customer_sources.ph_num_full,
	SQ_contract_customer_sources.ph_area_code,
	SQ_contract_customer_sources.ph_exchange,
	SQ_contract_customer_sources.ph_num,
	SQ_contract_customer_sources.ph_extension,
	SQ_contract_customer_sources.bus_email_addr,
	SQ_contract_customer_sources.sort_name,
	SQ_contract_customer_sources.contract_cust_addr_id_M AS in_contract_cust_addr_id_M,
	SQ_contract_customer_sources.loc_unit_num_M AS in_loc_unit_num_M,
	SQ_contract_customer_sources.addr_line_1_M AS in_addr_line_1_M,
	SQ_contract_customer_sources.addr_line_2_M AS in_addr_line_2_M,
	SQ_contract_customer_sources.addr_line_3_M AS in_addr_line_3_M,
	SQ_contract_customer_sources.city_name_M AS in_city_name_M,
	SQ_contract_customer_sources.state_prov_code_M AS in_state_prov_code_M,
	SQ_contract_customer_sources.zip_postal_code_M AS in_zip_postal_code_M,
	SQ_contract_customer_sources.zip_postal_code_extension_M AS in_zip_postal_code_extension_M,
	SQ_contract_customer_sources.county_parish_name_M AS in_county_parish_name_M,
	SQ_contract_customer_sources.country_name_M AS in_country_name_M,
	SQ_contract_customer_sources.contract_cust_addr_id_O AS in_contract_cust_addr_id_O,
	SQ_contract_customer_sources.loc_unit_num_O AS in_loc_unit_num_O,
	SQ_contract_customer_sources.addr_line_1_O AS in_addr_line_1_O,
	SQ_contract_customer_sources.addr_line_2_O AS in_addr_line_2_O,
	SQ_contract_customer_sources.addr_line_3_O AS in_addr_line_3_O,
	SQ_contract_customer_sources.city_name_O AS in_city_name_O,
	SQ_contract_customer_sources.state_prov_code_O AS in_state_prov_code_O,
	SQ_contract_customer_sources.zip_postal_code_O AS in_zip_postal_code_O,
	SQ_contract_customer_sources.zip_postal_code_extension_O AS in_zip_postal_code_extension_O,
	SQ_contract_customer_sources.county_parish_name_O AS in_county_parish_name_O,
	SQ_contract_customer_sources.country_name_O AS in_country_name_O,
	LKP_Program.ProgramCode AS in_ProgramCode,
	LKP_Program.ProgramDescription AS in_ProgramDescription,
	-- *INF*: IIF(ISNULL(in_ProgramCode),'N/A',in_ProgramCode)
	IFF(in_ProgramCode IS NULL, 'N/A', in_ProgramCode) AS out_ProgramCode,
	-- *INF*: IIF(ISNULL(in_ProgramDescription),'N/A',in_ProgramDescription)
	IFF(in_ProgramDescription IS NULL, 'N/A', in_ProgramDescription) AS out_ProgramDescription,
	-- *INF*: IIF(ISNULL(in_contract_cust_addr_id_M),-1,in_contract_cust_addr_id_M)
	IFF(in_contract_cust_addr_id_M IS NULL, - 1, in_contract_cust_addr_id_M) AS out_contract_cust_addr_id_M,
	-- *INF*: IIF(ISNULL(in_loc_unit_num_M),'N/A',in_loc_unit_num_M)
	IFF(in_loc_unit_num_M IS NULL, 'N/A', in_loc_unit_num_M) AS out_loc_unit_num_M,
	-- *INF*: IIF(ISNULL(in_addr_line_1_M),'N/A',in_addr_line_1_M)
	IFF(in_addr_line_1_M IS NULL, 'N/A', in_addr_line_1_M) AS out_addr_line_1_M,
	-- *INF*: IIF(ISNULL(in_addr_line_2_M),'N/A',in_addr_line_2_M)
	IFF(in_addr_line_2_M IS NULL, 'N/A', in_addr_line_2_M) AS out_addr_line_2_M,
	-- *INF*: IIF(ISNULL(in_addr_line_3_M),'N/A',in_addr_line_3_M)
	IFF(in_addr_line_3_M IS NULL, 'N/A', in_addr_line_3_M) AS out_addr_line_3_M,
	-- *INF*: IIF(ISNULL(in_city_name_M),'N/A',in_city_name_M)
	IFF(in_city_name_M IS NULL, 'N/A', in_city_name_M) AS out_city_name_M,
	-- *INF*: IIF(ISNULL(in_state_prov_code_M),'N/A',in_state_prov_code_M)
	IFF(in_state_prov_code_M IS NULL, 'N/A', in_state_prov_code_M) AS out_state_prov_code_M,
	-- *INF*: IIF(ISNULL(in_zip_postal_code_M),'N/A',in_zip_postal_code_M)
	IFF(in_zip_postal_code_M IS NULL, 'N/A', in_zip_postal_code_M) AS out_zip_postal_code_M,
	-- *INF*: IIF(ISNULL(in_zip_postal_code_extension_M),'N/A',in_zip_postal_code_extension_M)
	IFF(in_zip_postal_code_extension_M IS NULL, 'N/A', in_zip_postal_code_extension_M) AS out_zip_postal_code_extension_M,
	-- *INF*: IIF(ISNULL(in_county_parish_name_M),'N/A',in_county_parish_name_M)
	IFF(in_county_parish_name_M IS NULL, 'N/A', in_county_parish_name_M) AS out_county_parish_name_M,
	-- *INF*: IIF(ISNULL(in_country_name_M),'N/A',in_country_name_M)
	IFF(in_country_name_M IS NULL, 'N/A', in_country_name_M) AS out_country_name_M,
	-- *INF*: IIF(ISNULL(in_contract_cust_addr_id_O),-1,in_contract_cust_addr_id_O)
	IFF(in_contract_cust_addr_id_O IS NULL, - 1, in_contract_cust_addr_id_O) AS out_contract_cust_addr_id_O,
	-- *INF*: IIF(ISNULL(in_loc_unit_num_O),'N/A',in_loc_unit_num_O)
	IFF(in_loc_unit_num_O IS NULL, 'N/A', in_loc_unit_num_O) AS out_loc_unit_num_O,
	-- *INF*: IIF(ISNULL(in_addr_line_1_O),'N/A',in_addr_line_1_O)
	IFF(in_addr_line_1_O IS NULL, 'N/A', in_addr_line_1_O) AS out_addr_line_1_O,
	-- *INF*: IIF(ISNULL(in_addr_line_2_O),'N/A',in_addr_line_2_O)
	IFF(in_addr_line_2_O IS NULL, 'N/A', in_addr_line_2_O) AS out_addr_line_2_O,
	-- *INF*: IIF(ISNULL(in_addr_line_3_O),'N/A',in_addr_line_3_O)
	IFF(in_addr_line_3_O IS NULL, 'N/A', in_addr_line_3_O) AS out_addr_line_3_O,
	-- *INF*: IIF(ISNULL(in_city_name_O),'N/A',in_city_name_O)
	IFF(in_city_name_O IS NULL, 'N/A', in_city_name_O) AS out_city_name_O,
	-- *INF*: IIF(ISNULL(in_state_prov_code_O),'N/A',in_state_prov_code_O)
	IFF(in_state_prov_code_O IS NULL, 'N/A', in_state_prov_code_O) AS out_state_prov_code_O,
	-- *INF*: IIF(ISNULL(in_zip_postal_code_O),'N/A',in_zip_postal_code_O)
	IFF(in_zip_postal_code_O IS NULL, 'N/A', in_zip_postal_code_O) AS out_zip_postal_code_O,
	-- *INF*: IIF(ISNULL(in_zip_postal_code_extension_O),'N/A',in_zip_postal_code_extension_O)
	IFF(in_zip_postal_code_extension_O IS NULL, 'N/A', in_zip_postal_code_extension_O) AS out_zip_postal_code_extension_O,
	-- *INF*: IIF(ISNULL(in_county_parish_name_O),'N/A',in_county_parish_name_O)
	IFF(in_county_parish_name_O IS NULL, 'N/A', in_county_parish_name_O) AS out_county_parish_name_O,
	-- *INF*: IIF(ISNULL(in_country_name_O),'N/A',in_country_name_O)
	IFF(in_country_name_O IS NULL, 'N/A', in_country_name_O) AS out_country_name_O,
	SQ_contract_customer_sources.DIST_EFF_FROM_DATE
	FROM SQ_contract_customer_sources
	LEFT JOIN LKP_Program
	ON LKP_Program.ProgramAKId = LKP_v2_policy.ProgramAKId
	LEFT JOIN LKP_sup_legal_entity_code
	ON LKP_sup_legal_entity_code.sup_lgl_ent_code_id = SQ_contract_customer_sources.sup_lgl_ent_code_id AND LKP_sup_legal_entity_code.source_sys_id = SQ_contract_customer_sources.source_sys_id
),
LKP_SupNaicsCode AS (
	SELECT
	SupNaicsCodeId,
	CreatedDate,
	ModifiedDate,
	SourceSystemId,
	NAICSCode,
	NAICSCodeDesc,
	NAICSIndustryGroupCode,
	NAICSIndustryGroupCodeDesc,
	NAICSSubsectorCode,
	NAICSSubsectorCodeDesc,
	NAICSSectorCode,
	NAICSSectorCodeDesc
	FROM (
		SELECT 
			SupNaicsCodeId,
			CreatedDate,
			ModifiedDate,
			SourceSystemId,
			NAICSCode,
			NAICSCodeDesc,
			NAICSIndustryGroupCode,
			NAICSIndustryGroupCodeDesc,
			NAICSSubsectorCode,
			NAICSSubsectorCodeDesc,
			NAICSSectorCode,
			NAICSSectorCodeDesc
		FROM SupNaicsCode
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY NAICSCode ORDER BY SupNaicsCodeId) = 1
),
EXP_sup_description AS (
	SELECT
	EXP_values.contract_cust_id,
	EXP_values.source_sys_id,
	EXP_values.contract_cust_ak_id,
	EXP_values.cust_num,
	EXP_values.contract_key,
	EXP_values.cust_role,
	EXP_values.name,
	EXP_values.fed_tax_id,
	EXP_values.doing_bus_as,
	EXP_values.sic_code,
	-- *INF*: :LKP.LKP_ALL_SUPPORT_TABLES('N/A','sup_sic_code',sic_code)
	-- 
	LKP_ALL_SUPPORT_TABLES__N_A_sup_sic_code_sic_code.descript AS v_sic_code_descript,
	-- *INF*: IIF(ISNULL(v_sic_code_descript) or IS_SPACES(v_sic_code_descript)  or LENGTH(v_sic_code_descript)=0,'N/A',LTRIM(RTRIM(v_sic_code_descript)))
	IFF(v_sic_code_descript IS NULL OR IS_SPACES(v_sic_code_descript) OR LENGTH(v_sic_code_descript) = 0, 'N/A', LTRIM(RTRIM(v_sic_code_descript))) AS sic_code_descript,
	EXP_values.naics_code,
	LKP_SupNaicsCode.NAICSCodeDesc AS IN_NAICSCodeDesc,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(IN_NAICSCodeDesc)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(IN_NAICSCodeDesc) AS naics_code_description,
	EXP_values.lgl_ent_code,
	EXP_values.lgl_ent_code_descript,
	EXP_values.yr_in_bus,
	EXP_values.ph_num_full,
	EXP_values.ph_area_code,
	EXP_values.ph_exchange,
	EXP_values.ph_num,
	EXP_values.ph_extension,
	EXP_values.bus_email_addr,
	EXP_values.sort_name,
	EXP_values.out_ProgramCode AS ProgramCode,
	EXP_values.out_ProgramDescription AS ProgramCodeDescription,
	EXP_values.out_contract_cust_addr_id_M AS contract_cust_addr_id_M,
	EXP_values.out_loc_unit_num_M AS loc_unit_num_M,
	EXP_values.out_addr_line_1_M AS addr_line_1_M,
	EXP_values.out_addr_line_2_M AS addr_line_2_M,
	EXP_values.out_addr_line_3_M AS addr_line_3_M,
	EXP_values.out_city_name_M AS city_name_M,
	EXP_values.out_state_prov_code_M AS state_prov_code_M,
	-- *INF*: :LKP.LKP_ALL_SUPPORT_TABLES('EXCEED','sup_state',state_prov_code_M)
	LKP_ALL_SUPPORT_TABLES__EXCEED_sup_state_state_prov_code_M.descript AS v_state_prov_code_description_M,
	-- *INF*: IIF(ISNULL(v_state_prov_code_description_M) or IS_SPACES(v_state_prov_code_description_M)  or LENGTH(v_state_prov_code_description_M)=0,'N/A',LTRIM(RTRIM(v_state_prov_code_description_M)))
	IFF(v_state_prov_code_description_M IS NULL OR IS_SPACES(v_state_prov_code_description_M) OR LENGTH(v_state_prov_code_description_M) = 0, 'N/A', LTRIM(RTRIM(v_state_prov_code_description_M))) AS state_prov_code_description_M,
	EXP_values.out_zip_postal_code_M AS zip_postal_code_M,
	EXP_values.out_zip_postal_code_extension_M AS zip_postal_code_extension_M,
	EXP_values.out_county_parish_name_M AS county_parish_name_M,
	EXP_values.out_country_name_M AS country_name_M,
	EXP_values.out_contract_cust_addr_id_O AS contract_cust_addr_id_O,
	EXP_values.out_loc_unit_num_O AS loc_unit_num_O,
	EXP_values.out_addr_line_1_O AS addr_line_1_O,
	EXP_values.out_addr_line_2_O AS addr_line_2_O,
	EXP_values.out_addr_line_3_O AS addr_line_3_O,
	EXP_values.out_city_name_O AS city_name_O,
	EXP_values.out_state_prov_code_O AS state_prov_code_O,
	-- *INF*: :LKP.LKP_ALL_SUPPORT_TABLES('EXCEED','sup_state',state_prov_code_O)
	LKP_ALL_SUPPORT_TABLES__EXCEED_sup_state_state_prov_code_O.descript AS v_state_prov_code_description_O,
	-- *INF*: IIF(ISNULL(v_state_prov_code_description_O) or IS_SPACES(v_state_prov_code_description_O)  or LENGTH(v_state_prov_code_description_O)=0,'N/A',LTRIM(RTRIM(v_state_prov_code_description_O)))
	IFF(v_state_prov_code_description_O IS NULL OR IS_SPACES(v_state_prov_code_description_O) OR LENGTH(v_state_prov_code_description_O) = 0, 'N/A', LTRIM(RTRIM(v_state_prov_code_description_O))) AS state_prov_code_description_O,
	EXP_values.out_zip_postal_code_O AS zip_postal_code_O,
	EXP_values.out_zip_postal_code_extension_O AS zip_postal_code_extension_O,
	EXP_values.out_county_parish_name_O AS county_parish_name_O,
	EXP_values.out_country_name_O AS country_name_O,
	EXP_values.DIST_EFF_FROM_DATE,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	LKP_SupNaicsCode.NAICSIndustryGroupCode AS IN_NAICSIndustryGroupCode,
	LKP_SupNaicsCode.NAICSIndustryGroupCodeDesc AS IN_NAICSIndustryGroupCodeDesc,
	LKP_SupNaicsCode.NAICSSubsectorCode AS IN_NAICSSubsectorCode,
	LKP_SupNaicsCode.NAICSSubsectorCodeDesc AS IN_NAICSSubsectorCodeDesc,
	LKP_SupNaicsCode.NAICSSectorCode AS IN_NAICSSectorCode,
	LKP_SupNaicsCode.NAICSSectorCodeDesc AS IN_NAICSSectorCodeDesc,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(IN_NAICSIndustryGroupCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(IN_NAICSIndustryGroupCode) AS NAICSIndustryGroupCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(IN_NAICSIndustryGroupCodeDesc)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(IN_NAICSIndustryGroupCodeDesc) AS NAICSIndustryGroupCodeDesc,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(IN_NAICSSubsectorCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(IN_NAICSSubsectorCode) AS NAICSSubsectorCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(IN_NAICSSubsectorCodeDesc)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(IN_NAICSSubsectorCodeDesc) AS NAICSSubsectorCodeDesc,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(IN_NAICSSectorCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(IN_NAICSSectorCode) AS NAICSSectorCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(IN_NAICSSectorCodeDesc)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(IN_NAICSSectorCodeDesc) AS NAICSSectorCodeDesc
	FROM EXP_values
	LEFT JOIN LKP_SupNaicsCode
	ON LKP_SupNaicsCode.NAICSCode = EXP_values.naics_code
	LEFT JOIN LKP_ALL_SUPPORT_TABLES LKP_ALL_SUPPORT_TABLES__N_A_sup_sic_code_sic_code
	ON LKP_ALL_SUPPORT_TABLES__N_A_sup_sic_code_sic_code.source_sys_id = 'N/A'
	AND LKP_ALL_SUPPORT_TABLES__N_A_sup_sic_code_sic_code.tablename = 'sup_sic_code'
	AND LKP_ALL_SUPPORT_TABLES__N_A_sup_sic_code_sic_code.code = sic_code

	LEFT JOIN LKP_ALL_SUPPORT_TABLES LKP_ALL_SUPPORT_TABLES__EXCEED_sup_state_state_prov_code_M
	ON LKP_ALL_SUPPORT_TABLES__EXCEED_sup_state_state_prov_code_M.source_sys_id = 'EXCEED'
	AND LKP_ALL_SUPPORT_TABLES__EXCEED_sup_state_state_prov_code_M.tablename = 'sup_state'
	AND LKP_ALL_SUPPORT_TABLES__EXCEED_sup_state_state_prov_code_M.code = state_prov_code_M

	LEFT JOIN LKP_ALL_SUPPORT_TABLES LKP_ALL_SUPPORT_TABLES__EXCEED_sup_state_state_prov_code_O
	ON LKP_ALL_SUPPORT_TABLES__EXCEED_sup_state_state_prov_code_O.source_sys_id = 'EXCEED'
	AND LKP_ALL_SUPPORT_TABLES__EXCEED_sup_state_state_prov_code_O.tablename = 'sup_state'
	AND LKP_ALL_SUPPORT_TABLES__EXCEED_sup_state_state_prov_code_O.code = state_prov_code_O

),
LKP_contract_customer_dim AS (
	SELECT
	contract_cust_dim_id,
	edw_contract_cust_pk_id,
	edw_contract_cust_addr_mailing_pk_id,
	edw_contract_cust_addr_other_pk_id
	FROM (
		SELECT 
			contract_cust_dim_id,
			edw_contract_cust_pk_id,
			edw_contract_cust_addr_mailing_pk_id,
			edw_contract_cust_addr_other_pk_id
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_contract_cust_pk_id,edw_contract_cust_addr_mailing_pk_id,edw_contract_cust_addr_other_pk_id ORDER BY contract_cust_dim_id DESC) = 1
),
RTR_contract_cust_dim_INS_UPD AS (
	SELECT
	LKP_contract_customer_dim.contract_cust_dim_id AS lkp_contract_cust_dim_id,
	EXP_sup_description.contract_cust_id,
	EXP_sup_description.contract_cust_ak_id,
	EXP_sup_description.cust_num,
	EXP_sup_description.contract_key,
	EXP_sup_description.cust_role,
	EXP_sup_description.name,
	EXP_sup_description.fed_tax_id,
	EXP_sup_description.doing_bus_as,
	EXP_sup_description.sic_code,
	EXP_sup_description.sic_code_descript,
	EXP_sup_description.naics_code,
	EXP_sup_description.naics_code_description,
	EXP_sup_description.lgl_ent_code,
	EXP_sup_description.lgl_ent_code_descript,
	EXP_sup_description.yr_in_bus,
	EXP_sup_description.ph_num_full,
	EXP_sup_description.ph_area_code,
	EXP_sup_description.ph_exchange,
	EXP_sup_description.ph_num,
	EXP_sup_description.ph_extension,
	EXP_sup_description.bus_email_addr,
	EXP_sup_description.sort_name,
	EXP_sup_description.ProgramCode,
	EXP_sup_description.ProgramCodeDescription,
	EXP_sup_description.contract_cust_addr_id_M,
	EXP_sup_description.loc_unit_num_M,
	EXP_sup_description.addr_line_1_M,
	EXP_sup_description.addr_line_2_M,
	EXP_sup_description.addr_line_3_M,
	EXP_sup_description.city_name_M,
	EXP_sup_description.state_prov_code_M,
	EXP_sup_description.state_prov_code_description_M,
	EXP_sup_description.zip_postal_code_M,
	EXP_sup_description.zip_postal_code_extension_M,
	EXP_sup_description.county_parish_name_M,
	EXP_sup_description.country_name_M,
	EXP_sup_description.contract_cust_addr_id_O,
	EXP_sup_description.loc_unit_num_O,
	EXP_sup_description.addr_line_1_O,
	EXP_sup_description.addr_line_2_O,
	EXP_sup_description.addr_line_3_O,
	EXP_sup_description.city_name_O,
	EXP_sup_description.state_prov_code_O,
	EXP_sup_description.state_prov_code_description_O,
	EXP_sup_description.zip_postal_code_O,
	EXP_sup_description.zip_postal_code_extension_O,
	EXP_sup_description.county_parish_name_O,
	EXP_sup_description.country_name_O,
	EXP_sup_description.DIST_EFF_FROM_DATE,
	EXP_sup_description.crrnt_snpsht_flag,
	EXP_sup_description.audit_id,
	EXP_sup_description.eff_to_date,
	EXP_sup_description.created_date,
	EXP_sup_description.modified_date,
	EXP_sup_description.NAICSIndustryGroupCode,
	EXP_sup_description.NAICSIndustryGroupCodeDesc,
	EXP_sup_description.NAICSSubsectorCode,
	EXP_sup_description.NAICSSubsectorCodeDesc,
	EXP_sup_description.NAICSSectorCode,
	EXP_sup_description.NAICSSectorCodeDesc
	FROM EXP_sup_description
	LEFT JOIN LKP_contract_customer_dim
	ON LKP_contract_customer_dim.edw_contract_cust_pk_id = EXP_sup_description.contract_cust_id AND LKP_contract_customer_dim.edw_contract_cust_addr_mailing_pk_id = EXP_sup_description.contract_cust_addr_id_M AND LKP_contract_customer_dim.edw_contract_cust_addr_other_pk_id = EXP_sup_description.contract_cust_addr_id_O
),
RTR_contract_cust_dim_INS_UPD_INSERT AS (SELECT * FROM RTR_contract_cust_dim_INS_UPD WHERE isnull(lkp_contract_cust_dim_id)),
RTR_contract_cust_dim_INS_UPD_DEFAULT1 AS (SELECT * FROM RTR_contract_cust_dim_INS_UPD WHERE NOT ( (isnull(lkp_contract_cust_dim_id)) )),
TGT_contract_customer_dim_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer_dim
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date, edw_contract_cust_pk_id, edw_contract_cust_addr_mailing_pk_id, edw_contract_cust_addr_other_pk_id, edw_contract_cust_ak_id, cust_num, contract_key, cust_role, name, fed_tax_id, doing_bus_as, sic_code, sic_code_descript, naics_code, naics_code_descript, lgl_ent_code, lgl_ent_code_descript, yr_in_bus, ph_num_full, ph_area_code, ph_exchange, ph_num, ph_extension, bus_email_addr, mailing_loc_unit_num, mailing_addr_line_1, mailing_addr_line_2, mailing_addr_line_3, mailing_city_name, mailing_state_prov_code, mailing_state_prov_code_descript, mailing_zip_postal_code, mailing_zip_postal_code_extension, mailing_county_parish_name, mailing_country_name, other_loc_unit_num, other_addr_line_1, other_addr_line_2, other_addr_line_3, other_city_name, other_state_prov_code, other_state_prov_code_descript, other_zip_postal_code, other_zip_postal_code_extension, other_county_parish_name, other_country_name, sort_name, ProgramCode, ProgramCodeDescription, NAICSIndustryGroupCode, NAICSIndustryGroupCodeDescription, NAICSSubsectorCode, NAICSSubsectorCodeDescription, NAICSSectorCode, NAICSSectorCodeDescription)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	DIST_EFF_FROM_DATE AS EFF_FROM_DATE, 
	EFF_TO_DATE, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	contract_cust_id AS EDW_CONTRACT_CUST_PK_ID, 
	contract_cust_addr_id_M AS EDW_CONTRACT_CUST_ADDR_MAILING_PK_ID, 
	contract_cust_addr_id_O AS EDW_CONTRACT_CUST_ADDR_OTHER_PK_ID, 
	contract_cust_ak_id AS EDW_CONTRACT_CUST_AK_ID, 
	CUST_NUM, 
	CONTRACT_KEY, 
	CUST_ROLE, 
	NAME, 
	FED_TAX_ID, 
	DOING_BUS_AS, 
	SIC_CODE, 
	SIC_CODE_DESCRIPT, 
	NAICS_CODE, 
	naics_code_description AS NAICS_CODE_DESCRIPT, 
	LGL_ENT_CODE, 
	LGL_ENT_CODE_DESCRIPT, 
	YR_IN_BUS, 
	PH_NUM_FULL, 
	PH_AREA_CODE, 
	PH_EXCHANGE, 
	PH_NUM, 
	PH_EXTENSION, 
	BUS_EMAIL_ADDR, 
	loc_unit_num_M AS MAILING_LOC_UNIT_NUM, 
	addr_line_1_M AS MAILING_ADDR_LINE_1, 
	addr_line_2_M AS MAILING_ADDR_LINE_2, 
	addr_line_3_M AS MAILING_ADDR_LINE_3, 
	city_name_M AS MAILING_CITY_NAME, 
	state_prov_code_M AS MAILING_STATE_PROV_CODE, 
	state_prov_code_description_M AS MAILING_STATE_PROV_CODE_DESCRIPT, 
	zip_postal_code_M AS MAILING_ZIP_POSTAL_CODE, 
	zip_postal_code_extension_M AS MAILING_ZIP_POSTAL_CODE_EXTENSION, 
	county_parish_name_M AS MAILING_COUNTY_PARISH_NAME, 
	country_name_M AS MAILING_COUNTRY_NAME, 
	loc_unit_num_O AS OTHER_LOC_UNIT_NUM, 
	addr_line_1_O AS OTHER_ADDR_LINE_1, 
	addr_line_2_O AS OTHER_ADDR_LINE_2, 
	addr_line_3_O AS OTHER_ADDR_LINE_3, 
	city_name_O AS OTHER_CITY_NAME, 
	state_prov_code_O AS OTHER_STATE_PROV_CODE, 
	state_prov_code_description_O AS OTHER_STATE_PROV_CODE_DESCRIPT, 
	zip_postal_code_O AS OTHER_ZIP_POSTAL_CODE, 
	zip_postal_code_extension_O AS OTHER_ZIP_POSTAL_CODE_EXTENSION, 
	county_parish_name_O AS OTHER_COUNTY_PARISH_NAME, 
	country_name_O AS OTHER_COUNTRY_NAME, 
	SORT_NAME, 
	PROGRAMCODE, 
	PROGRAMCODEDESCRIPTION, 
	NAICSINDUSTRYGROUPCODE, 
	NAICSIndustryGroupCodeDesc AS NAICSINDUSTRYGROUPCODEDESCRIPTION, 
	NAICSSUBSECTORCODE, 
	NAICSSubsectorCodeDesc AS NAICSSUBSECTORCODEDESCRIPTION, 
	NAICSSECTORCODE, 
	NAICSSectorCodeDesc AS NAICSSECTORCODEDESCRIPTION
	FROM RTR_contract_cust_dim_INS_UPD_INSERT
),
UPD_contract_cust_dim AS (
	SELECT
	lkp_contract_cust_dim_id AS lkp_contract_cust_dim_id2, 
	contract_cust_id AS contract_cust_id2, 
	contract_cust_ak_id AS contract_cust_ak_id2, 
	cust_num AS cust_num2, 
	contract_key AS contract_key2, 
	cust_role AS cust_role2, 
	name AS name2, 
	fed_tax_id AS fed_tax_id2, 
	doing_bus_as AS doing_bus_as2, 
	sic_code AS sic_code2, 
	sic_code_descript AS sic_code_descript2, 
	naics_code AS naics_code2, 
	naics_code_description AS naics_code_description2, 
	lgl_ent_code AS lgl_ent_code2, 
	lgl_ent_code_descript AS lgl_ent_code_descript2, 
	yr_in_bus AS yr_in_bus2, 
	ph_num_full AS ph_num_full2, 
	ph_area_code AS ph_area_code2, 
	ph_exchange AS ph_exchange2, 
	ph_num AS ph_num2, 
	ph_extension AS ph_extension2, 
	bus_email_addr AS bus_email_addr2, 
	contract_cust_addr_id_M AS contract_cust_addr_id_M2, 
	loc_unit_num_M AS loc_unit_num_M2, 
	addr_line_1_M AS addr_line_1_M2, 
	addr_line_2_M AS addr_line_2_M2, 
	addr_line_3_M AS addr_line_3_M2, 
	city_name_M AS city_name_M2, 
	state_prov_code_M AS state_prov_code_M2, 
	state_prov_code_description_M AS state_prov_code_description_M2, 
	zip_postal_code_M AS zip_postal_code_M2, 
	zip_postal_code_extension_M AS zip_postal_code_extension_M2, 
	county_parish_name_M AS county_parish_name_M2, 
	country_name_M AS country_name_M2, 
	contract_cust_addr_id_O AS contract_cust_addr_id_O2, 
	loc_unit_num_O AS loc_unit_num_O2, 
	addr_line_1_O AS addr_line_1_O2, 
	addr_line_2_O AS addr_line_2_O2, 
	addr_line_3_O AS addr_line_3_O2, 
	city_name_O AS city_name_O2, 
	state_prov_code_O AS state_prov_code_O2, 
	state_prov_code_description_O AS state_prov_code_description_O2, 
	zip_postal_code_O AS zip_postal_code_O2, 
	zip_postal_code_extension_O AS zip_postal_code_extension_O2, 
	county_parish_name_O AS county_parish_name_O2, 
	country_name_O AS country_name_O2, 
	sort_name AS sort_name2, 
	ProgramCode AS ProgramCode2, 
	ProgramCodeDescription, 
	DIST_EFF_FROM_DATE AS DIST_EFF_FROM_DATE2, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag2, 
	audit_id AS audit_id2, 
	eff_to_date AS eff_to_date2, 
	created_date AS created_date2, 
	modified_date AS modified_date2, 
	NAICSIndustryGroupCode AS NAICSIndustryGroupCode2, 
	NAICSIndustryGroupCodeDesc AS NAICSIndustryGroupCodeDesc2, 
	NAICSSubsectorCode AS NAICSSubsectorCode2, 
	NAICSSubsectorCodeDesc AS NAICSSubsectorCodeDesc2, 
	NAICSSectorCode AS NAICSSectorCode2, 
	NAICSSectorCodeDesc AS NAICSSectorCodeDesc2
	FROM RTR_contract_cust_dim_INS_UPD_DEFAULT1
),
TGT_contract_customer_dim_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer_dim AS T
	USING UPD_contract_cust_dim AS S
	ON T.contract_cust_dim_id = S.lkp_contract_cust_dim_id2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.eff_from_date = S.DIST_EFF_FROM_DATE2, T.modified_date = S.modified_date2, T.cust_num = S.cust_num2, T.contract_key = S.contract_key2, T.cust_role = S.cust_role2, T.name = S.name2, T.fed_tax_id = S.fed_tax_id2, T.doing_bus_as = S.doing_bus_as2, T.sic_code = S.sic_code2, T.sic_code_descript = S.sic_code_descript2, T.naics_code = S.naics_code2, T.naics_code_descript = S.naics_code_description2, T.lgl_ent_code = S.lgl_ent_code2, T.lgl_ent_code_descript = S.lgl_ent_code_descript2, T.yr_in_bus = S.yr_in_bus2, T.ph_num_full = S.ph_num_full2, T.ph_area_code = S.ph_area_code2, T.ph_exchange = S.ph_exchange2, T.ph_num = S.ph_num2, T.ph_extension = S.ph_extension2, T.bus_email_addr = S.bus_email_addr2, T.mailing_loc_unit_num = S.loc_unit_num_M2, T.mailing_addr_line_1 = S.addr_line_1_M2, T.mailing_addr_line_2 = S.addr_line_2_M2, T.mailing_addr_line_3 = S.addr_line_3_M2, T.mailing_city_name = S.city_name_M2, T.mailing_state_prov_code = S.state_prov_code_M2, T.mailing_state_prov_code_descript = S.state_prov_code_description_M2, T.mailing_zip_postal_code = S.zip_postal_code_M2, T.mailing_zip_postal_code_extension = S.zip_postal_code_extension_M2, T.mailing_county_parish_name = S.county_parish_name_M2, T.mailing_country_name = S.country_name_M2, T.other_loc_unit_num = S.loc_unit_num_O2, T.other_addr_line_1 = S.addr_line_1_O2, T.other_addr_line_2 = S.addr_line_2_O2, T.other_addr_line_3 = S.addr_line_3_O2, T.other_city_name = S.city_name_O2, T.other_state_prov_code = S.state_prov_code_O2, T.other_state_prov_code_descript = S.state_prov_code_description_O2, T.other_zip_postal_code = S.zip_postal_code_O2, T.other_zip_postal_code_extension = S.zip_postal_code_extension_O2, T.other_county_parish_name = S.county_parish_name_O2, T.other_country_name = S.country_name_O2, T.sort_name = S.sort_name2, T.ProgramCode = S.ProgramCode2, T.ProgramCodeDescription = S.ProgramCodeDescription, T.NAICSIndustryGroupCode = S.NAICSIndustryGroupCode2, T.NAICSIndustryGroupCodeDescription = S.NAICSIndustryGroupCodeDesc2, T.NAICSSubsectorCode = S.NAICSSubsectorCode2, T.NAICSSubsectorCodeDescription = S.NAICSSubsectorCodeDesc2, T.NAICSSectorCode = S.NAICSSectorCode2, T.NAICSSectorCodeDescription = S.NAICSSectorCodeDesc2
),
SQ_contract_customer_dim AS (
	SELECT 
		contract_cust_dim_id, 
		eff_from_date, 
		eff_to_date, 
		edw_contract_cust_ak_id 
	FROM
		@{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer_dim 
	WHERE edw_contract_cust_ak_id   IN 
		   (SELECT edw_contract_cust_ak_id   FROM @{pipeline().parameters.TARGET_TABLE_OWNER}. contract_customer_dim
	           WHERE crrnt_snpsht_flag = 1 GROUP BY edw_contract_cust_ak_id   HAVING count(*) > 1)
	ORDER BY edw_contract_cust_ak_id,eff_from_date  DESC
	
	
	--IN Subquery exists to pick AK ID column values that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag 
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
),
EXP_Lag_eff_from_date AS (
	SELECT
	contract_cust_dim_id,
	eff_from_date AS in_eff_from_date,
	eff_to_date AS orig_eff_to_date,
	edw_contract_cust_ak_id,
	-- *INF*: DECODE(TRUE,
	-- edw_contract_cust_ak_id = v_prev_edw_contract_cust_ak_id  ,
	-- ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),orig_eff_to_date)
	DECODE(TRUE,
	edw_contract_cust_ak_id = v_prev_edw_contract_cust_ak_id, ADD_TO_DATE(v_prev_eff_from_date, 'SS', - 1),
	orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	edw_contract_cust_ak_id AS v_prev_edw_contract_cust_ak_id,
	in_eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_contract_customer_dim
),
FIL_FirstRowInAKGroup AS (
	SELECT
	contract_cust_dim_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_contract_cust_dim_expire AS (
	SELECT
	contract_cust_dim_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_FirstRowInAKGroup
),
TGT_contract_customer_dim_EXP_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer_dim AS T
	USING UPD_contract_cust_dim_expire AS S
	ON T.contract_cust_dim_id = S.contract_cust_dim_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),
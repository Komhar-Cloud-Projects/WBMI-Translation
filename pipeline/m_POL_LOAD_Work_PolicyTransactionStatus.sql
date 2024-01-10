WITH
SQ_arch_pif_4514_stage AS (
	SELECT LTRIM(RTRIM(arch.pif_symbol)),
	       arch.pif_policy_number,
	       arch.pif_module,
	       arch.sar_id,
	       LTRIM(RTRIM(arch.sar_insurance_line)),
	       LTRIM(RTRIM(arch.sar_location_x)),
	       LTRIM(RTRIM(arch.sar_sub_location_x)),
	       LTRIM(RTRIM(arch.sar_risk_unit_group)),
	       LTRIM(RTRIM(arch.sar_class_code_grp_x)),
	       LTRIM(RTRIM(arch.sar_class_code_mem_x)),
	       LTRIM(RTRIM(arch.sar_unit)),
	       LTRIM(RTRIM(arch.sar_risk_unit_continued)),
	       LTRIM(RTRIM(arch.sar_seq_rsk_unt_a)),
	       LTRIM(RTRIM(arch.sar_major_peril)),
	       LTRIM(RTRIM(arch.sar_seq_no)),
	       arch.sar_cov_eff_year,
	       arch.sar_cov_eff_month,
	       arch.sar_cov_eff_day,
	       sar_part_code,
	       LTRIM(RTRIM(arch.sar_entrd_date)),
	       arch.sar_transaction,
	       arch.sar_premium,
	       arch.sar_agents_comm_rate,
	       LTRIM(RTRIM(arch.sar_state)),
	       LTRIM(RTRIM(arch.sar_loc_prov_territory)),
	       CASE WHEN LEN(LTRIM(RTRIM(arch.sar_county_first_two)) + LTRIM(RTRIM(arch.sar_county_last_one)) + LTRIM(RTRIM(arch.sar_city))) < 6 THEN '000000' ELSE 
	       	    LTRIM(RTRIM(arch.sar_county_first_two)) + LTRIM(RTRIM(arch.sar_county_last_one)) + LTRIM(RTRIM(arch.sar_city)) END as sar_city,
	       LTRIM(RTRIM(arch.sar_section)),
	       LTRIM(RTRIM(arch.sar_class_1_4)),
	       LTRIM(RTRIM(arch.sar_class_5_6)),
	       LTRIM(RTRIM(arch.sar_sub_line)),
	       LTRIM(RTRIM(sar_zip_postal_code)),
	       Hashbytes('MD5', ( LTRIM(RTRIM(arch.sar_id)) + LTRIM(RTRIM(arch.sar_insurance_line)) + LTRIM(RTRIM(arch.sar_location_x)) + LTRIM(RTRIM(arch.sar_unit)) + LTRIM(RTRIM(arch.sar_sub_location_x)) + LTRIM(RTRIM(arch.sar_risk_unit_group)) + LTRIM(RTRIM(arch.sar_class_code_grp_x)) + LTRIM(RTRIM(arch.sar_class_code_mem_x)) + LTRIM(RTRIM(arch.sar_risk_unit_continued)) + LTRIM(RTRIM(arch.sar_seq_rsk_unt_a)) + LTRIM(RTRIM(arch.sar_type_exposure)) + LTRIM(RTRIM(arch.sar_major_peril)) + LTRIM(RTRIM(arch.sar_seq_no)) + LTRIM(RTRIM(arch.sar_cov_eff_year)) + LTRIM(RTRIM(arch.sar_cov_eff_month)) + LTRIM(RTRIM(arch.sar_cov_eff_day)) + LTRIM(RTRIM(arch.sar_part_code)) + LTRIM(RTRIM(arch.sar_trans_eff_year)) + LTRIM(RTRIM(arch.sar_trans_eff_month)) + LTRIM(RTRIM(arch.sar_trans_eff_day)) + LTRIM(RTRIM(arch.sar_reinsurance_company_no)) + LTRIM(RTRIM(arch.sar_entrd_date)) + LTRIM(RTRIM(arch.sar_exp_year)) + LTRIM(RTRIM(arch.sar_exp_month)) + LTRIM(RTRIM(arch.sar_exp_day)) + Ltrim(
	                          Rtrim(arch.sar_transaction))
	                          + LTRIM(RTRIM(arch.sar_premium)) + LTRIM(RTRIM(arch.sar_original_prem)) + LTRIM(RTRIM(arch.sar_agents_comm_rate)) + LTRIM(RTRIM(arch.sar_acct_entrd_date)) + LTRIM(RTRIM(arch.sar_annual_state_line)) + LTRIM(RTRIM(arch.sar_state)) + LTRIM(RTRIM(arch.sar_loc_prov_territory)) + LTRIM(RTRIM(arch.sar_county_first_two)) + LTRIM(RTRIM(arch.sar_county_last_one)) + LTRIM(RTRIM(arch.sar_city)) + LTRIM(RTRIM(arch.sar_rsn_amend_one)) + LTRIM(RTRIM(arch.sar_rsn_amend_two)) + LTRIM(RTRIM(arch.sar_rsn_amend_three)) + LTRIM(RTRIM(arch.sar_special_use)) + LTRIM(RTRIM(arch.sar_stat_breakdown_line)) + LTRIM(RTRIM(arch.sar_user_line)) + LTRIM(RTRIM(arch.sar_section)) + LTRIM(RTRIM(arch.sar_rating_date_ind)) + LTRIM(RTRIM(arch.sar_type_bureau)) + LTRIM(RTRIM(arch.sar_class_1_4)) + LTRIM(RTRIM(arch.sar_class_5_6)) + LTRIM(RTRIM(arch.sar_exposure)) + LTRIM(RTRIM(arch.sar_sub_line)) + LTRIM(RTRIM(arch.sar_code_1)) + LTRIM(RTRIM(arch.sar_code_2)) + Ltrim(Rtrim(arch.sar_code_3)) + LTRIM(RTRIM(arch.sar_code_4)) + LTRIM(RTRIM(arch.sar_code_5)) + LTRIM(RTRIM(arch.sar_code_6)) + LTRIM(RTRIM(arch.sar_code_7)) + LTRIM(RTRIM(arch.sar_code_8)) + LTRIM(RTRIM(arch.sar_code_9)) + LTRIM(RTRIM(arch.sar_code_10)) + LTRIM(RTRIM(arch.sar_code_11)) + LTRIM(RTRIM(arch.sar_code_12)) + LTRIM(RTRIM(arch.sar_code_13)) + LTRIM(RTRIM(arch.sar_code_14)) + LTRIM(RTRIM(arch.sar_code_15)) + LTRIM(RTRIM(arch.sar_zip_postal_code)) + LTRIM(RTRIM(arch.sar_audit_reinst_ind)) )) AS sar_yr2000_cust_use
	FROM   @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME_ARCH} arch
	       INNER JOIN (SELECT pif_symbol,
	                          pif_policy_number,
	                          pif_module,
	                          MAX(audit_id) AS audit_id
	                   FROM   @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME_ARCH}                   
	                    WHERE  logical_flag IN ('0','1','2','3')  AND @{pipeline().parameters.WHERE_CLAUSE_2}             
	GROUP  BY pif_symbol,pif_policy_number,pif_module)A
	         ON arch.pif_symbol = a.pif_symbol
	            AND arch.pif_policy_number = a.pif_policy_number
	            AND arch.pif_module = a.pif_module
	            AND arch.audit_id = a.audit_id
	WHERE  EXISTS(SELECT DISTINCT pif_symbol,
	                              pif_policy_number,
	                              pif_module
	              FROM   @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME_STAGE} stage
	              WHERE logical_flag IN ('0','1','2','3')  
				AND stage.pif_symbol = arch.pif_symbol
	                   AND stage.pif_policy_number = arch.pif_policy_number
	                   AND stage.pif_module = arch.pif_module)
	AND @{pipeline().parameters.WHERE_CLAUSE_ARCH}
),
EXP_Default_Archive AS (
	SELECT
	pif_symbol AS arch_pif_symbol,
	pif_policy_number AS arch_pif_policy_number,
	pif_module AS arch_pif_module,
	sar_id AS arch_sar_id,
	sar_insurance_line AS arch_sar_insurance_line,
	sar_location_x AS arch_sar_location_x,
	sar_sub_location_x AS arch_sar_sub_location_x,
	sar_risk_unit_group AS arch_sar_risk_unit_group,
	sar_class_code_grp_x AS arch_sar_class_code_grp_x,
	sar_class_code_mem_x AS arch_sar_class_code_mem_x,
	sar_unit AS arch_sar_unit,
	sar_risk_unit_continued AS arch_sar_risk_unit_continued,
	sar_seq_rsk_unt_a AS arch_sar_seq_rsk_unt_a,
	sar_major_peril AS arch_sar_major_peril,
	sar_seq_no AS arch_sar_seq_no,
	sar_cov_eff_year AS arch_sar_cov_eff_year,
	sar_cov_eff_month AS arch_sar_cov_eff_month,
	sar_cov_eff_day AS arch_sar_cov_eff_day,
	sar_part_code AS arch_sar_part_code,
	sar_entrd_date AS arch_sar_entrd_date,
	sar_transaction AS arch_sar_transaction,
	sar_premium AS arch_sar_premium,
	sar_agents_comm_rate AS arch_sar_agents_comm_rate,
	sar_state AS arch_sar_state,
	sar_loc_prov_territory AS arch_sar_loc_prov_territory,
	sar_city AS arch_sar_city,
	sar_section AS arch_sar_section,
	sar_class_1_4 AS arch_sar_class_1_4,
	sar_class_5_6 AS arch_sar_class_5_6,
	arch_sar_class_1_4  ||  arch_sar_class_5_6 AS arch_sar_class_code,
	sar_sub_line AS arch_sar_sub_line,
	sar_zip_postal_code AS arch_sar_zip_postal_code,
	sar_yr2000_cust_use AS arch_sar_yr2000_cust_use
	FROM SQ_arch_pif_4514_stage
),
SQ_pif_4514_stage AS (
	SELECT LTRIM(RTRIM(pif_symbol)),
	       pif_policy_number,
	       pif_module,
	       sar_id,
	       LTRIM(RTRIM(sar_insurance_line)),
	       LTRIM(RTRIM(sar_location_x)),
	       LTRIM(RTRIM(sar_sub_location_x)),
	       LTRIM(RTRIM(sar_risk_unit_group)),
	       LTRIM(RTRIM(sar_class_code_grp_x)),
	       LTRIM(RTRIM(sar_class_code_mem_x)),
	       LTRIM(RTRIM(sar_unit)),
	       LTRIM(RTRIM(sar_risk_unit_continued)),
	       LTRIM(RTRIM(sar_seq_rsk_unt_a)),
	       LTRIM(RTRIM(sar_major_peril)),
	       LTRIM(RTRIM(sar_seq_no)),
	       sar_cov_eff_year,
	       sar_cov_eff_month,
	       sar_cov_eff_day,
	       sar_part_code,
	       LTRIM(RTRIM(sar_entrd_date)),
	       sar_transaction,
	       sar_premium,
	       sar_agents_comm_rate,
	       LTRIM(RTRIM(sar_state)),
	       LTRIM(RTRIM(sar_loc_prov_territory)),
	       CASE WHEN LEN(LTRIM(RTRIM(sar_county_first_two)) + LTRIM(RTRIM(sar_county_last_one)) + LTRIM(RTRIM(sar_city))) < 6 THEN '000000' ELSE 
	       	    LTRIM(RTRIM(sar_county_first_two)) + LTRIM(RTRIM(sar_county_last_one)) + LTRIM(RTRIM(sar_city)) END as sar_city,
	       LTRIM(RTRIM(sar_section)),
	       LTRIM(RTRIM(sar_class_1_4)),
	       LTRIM(RTRIM(sar_class_5_6)),
	       LTRIM(RTRIM(sar_sub_line)),
	       LTRIM(RTRIM(sar_zip_postal_code)),
	       Hashbytes('MD5', ( LTRIM(RTRIM(sar_id)) + LTRIM(RTRIM(sar_insurance_line)) + LTRIM(RTRIM(sar_location_x)) + LTRIM(RTRIM(sar_unit)) 
	                          + LTRIM(RTRIM(sar_sub_location_x)) + LTRIM(RTRIM(sar_risk_unit_group)) + LTRIM(RTRIM(sar_class_code_grp_x)) + 
	                          LTRIM(RTRIM(sar_class_code_mem_x)) + LTRIM(RTRIM(sar_risk_unit_continued)) + LTRIM(RTRIM(sar_seq_rsk_unt_a)) + 
	                          LTRIM(RTRIM(sar_type_exposure)) + LTRIM(RTRIM(sar_major_peril)) + LTRIM(RTRIM(sar_seq_no)) + 
	                          LTRIM(RTRIM(sar_cov_eff_year)) + LTRIM(RTRIM(sar_cov_eff_month)) + LTRIM(RTRIM(sar_cov_eff_day)) + 
	                          LTRIM(RTRIM(sar_part_code)) + LTRIM(RTRIM(sar_trans_eff_year)) + LTRIM(RTRIM(sar_trans_eff_month)) + 
	                          LTRIM(RTRIM(sar_trans_eff_day)) + LTRIM(RTRIM(sar_reinsurance_company_no)) + LTRIM(RTRIM(sar_entrd_date)) + 
	                          LTRIM(RTRIM(sar_exp_year)) + LTRIM(RTRIM(sar_exp_month)) + LTRIM(RTRIM(sar_exp_day)) +
	                          LTRIM(RTRIM(sar_transaction)) + LTRIM(RTRIM(sar_premium)) + LTRIM(RTRIM(sar_original_prem)) + 
	                          LTRIM(RTRIM(sar_agents_comm_rate)) + LTRIM(RTRIM(sar_acct_entrd_date)) + LTRIM(RTRIM(sar_annual_state_line)) + 
	                          LTRIM(RTRIM(sar_state)) + LTRIM(RTRIM(sar_loc_prov_territory)) + LTRIM(RTRIM(sar_county_first_two)) + 
	                          LTRIM(RTRIM(sar_county_last_one)) + LTRIM(RTRIM(sar_city)) + LTRIM(RTRIM(sar_rsn_amend_one)) + LTRIM(RTRIM(sar_rsn_amend_two)) + LTRIM(RTRIM(sar_rsn_amend_three)) + LTRIM(RTRIM(sar_special_use)) + LTRIM(RTRIM(sar_stat_breakdown_line)) + LTRIM(RTRIM(sar_user_line)) + LTRIM(RTRIM(sar_section)) + LTRIM(RTRIM(sar_rating_date_ind)) + LTRIM(RTRIM(sar_type_bureau)) + LTRIM(RTRIM(sar_class_1_4)) + LTRIM(RTRIM(sar_class_5_6)) + LTRIM(RTRIM(sar_exposure)) + LTRIM(RTRIM(sar_sub_line)) + Ltrim(
	                          Rtrim(sar_code_1)) + LTRIM(RTRIM(sar_code_2)) + LTRIM(RTRIM(sar_code_3)) + LTRIM(RTRIM(sar_code_4)) + LTRIM(RTRIM(sar_code_5)) + LTRIM(RTRIM(sar_code_6)) + LTRIM(RTRIM(sar_code_7)) + LTRIM(RTRIM(sar_code_8)) + LTRIM(RTRIM(sar_code_9)) + LTRIM(RTRIM(sar_code_10)) + LTRIM(RTRIM(sar_code_11)) + LTRIM(RTRIM(sar_code_12)) + LTRIM(RTRIM(sar_code_13)) + LTRIM(RTRIM(sar_code_14)) + LTRIM(RTRIM(sar_code_15)) + LTRIM(RTRIM(sar_zip_postal_code)) + LTRIM(RTRIM(sar_audit_reinst_ind)) ))                                                                                                                                                                                                                                                                                                                                                                AS sar_yr2000_cust_use
	FROM  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME_STAGE}
	WHERE logical_flag in ('0','1','2','3') AND @{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Default_Stage AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module,
	sar_id,
	sar_insurance_line,
	sar_location_x,
	sar_sub_location_x,
	sar_risk_unit_group,
	sar_class_code_grp_x,
	sar_class_code_mem_x,
	sar_unit,
	sar_risk_unit_continued,
	sar_seq_rsk_unt_a,
	sar_major_peril,
	sar_seq_no,
	sar_cov_eff_year,
	sar_cov_eff_month,
	sar_cov_eff_day,
	sar_part_code,
	sar_entrd_date,
	sar_transaction,
	sar_premium,
	sar_agents_comm_rate,
	sar_state,
	sar_loc_prov_territory,
	sar_city,
	sar_section,
	sar_class_1_4,
	sar_class_5_6,
	sar_class_1_4  ||  sar_class_5_6 AS sar_class_code,
	sar_sub_line,
	sar_zip_postal_code,
	sar_yr2000_cust_use
	FROM SQ_pif_4514_stage
),
JNR_Stage_Archive_Data AS (SELECT
	EXP_Default_Archive.arch_pif_symbol, 
	EXP_Default_Archive.arch_pif_policy_number, 
	EXP_Default_Archive.arch_pif_module, 
	EXP_Default_Archive.arch_sar_id, 
	EXP_Default_Archive.arch_sar_insurance_line, 
	EXP_Default_Archive.arch_sar_location_x, 
	EXP_Default_Archive.arch_sar_sub_location_x, 
	EXP_Default_Archive.arch_sar_risk_unit_group, 
	EXP_Default_Archive.arch_sar_class_code_grp_x, 
	EXP_Default_Archive.arch_sar_class_code_mem_x, 
	EXP_Default_Archive.arch_sar_unit, 
	EXP_Default_Archive.arch_sar_risk_unit_continued, 
	EXP_Default_Archive.arch_sar_seq_rsk_unt_a, 
	EXP_Default_Archive.arch_sar_major_peril, 
	EXP_Default_Archive.arch_sar_seq_no, 
	EXP_Default_Archive.arch_sar_cov_eff_year, 
	EXP_Default_Archive.arch_sar_cov_eff_month, 
	EXP_Default_Archive.arch_sar_cov_eff_day, 
	EXP_Default_Archive.arch_sar_part_code, 
	EXP_Default_Archive.arch_sar_entrd_date, 
	EXP_Default_Archive.arch_sar_transaction, 
	EXP_Default_Archive.arch_sar_premium, 
	EXP_Default_Archive.arch_sar_agents_comm_rate, 
	EXP_Default_Archive.arch_sar_state, 
	EXP_Default_Archive.arch_sar_loc_prov_territory, 
	EXP_Default_Archive.arch_sar_city, 
	EXP_Default_Archive.arch_sar_section, 
	EXP_Default_Archive.arch_sar_class_code, 
	EXP_Default_Archive.arch_sar_sub_line, 
	EXP_Default_Archive.arch_sar_zip_postal_code, 
	EXP_Default_Archive.arch_sar_yr2000_cust_use, 
	EXP_Default_Stage.pif_symbol, 
	EXP_Default_Stage.pif_policy_number, 
	EXP_Default_Stage.pif_module, 
	EXP_Default_Stage.sar_id, 
	EXP_Default_Stage.sar_insurance_line, 
	EXP_Default_Stage.sar_location_x, 
	EXP_Default_Stage.sar_sub_location_x, 
	EXP_Default_Stage.sar_risk_unit_group, 
	EXP_Default_Stage.sar_class_code_grp_x, 
	EXP_Default_Stage.sar_class_code_mem_x, 
	EXP_Default_Stage.sar_unit, 
	EXP_Default_Stage.sar_risk_unit_continued, 
	EXP_Default_Stage.sar_seq_rsk_unt_a, 
	EXP_Default_Stage.sar_major_peril, 
	EXP_Default_Stage.sar_seq_no, 
	EXP_Default_Stage.sar_cov_eff_year, 
	EXP_Default_Stage.sar_cov_eff_month, 
	EXP_Default_Stage.sar_cov_eff_day, 
	EXP_Default_Stage.sar_part_code, 
	EXP_Default_Stage.sar_entrd_date, 
	EXP_Default_Stage.sar_transaction, 
	EXP_Default_Stage.sar_premium, 
	EXP_Default_Stage.sar_agents_comm_rate, 
	EXP_Default_Stage.sar_state, 
	EXP_Default_Stage.sar_loc_prov_territory, 
	EXP_Default_Stage.sar_city, 
	EXP_Default_Stage.sar_section, 
	EXP_Default_Stage.sar_class_code, 
	EXP_Default_Stage.sar_sub_line, 
	EXP_Default_Stage.sar_zip_postal_code, 
	EXP_Default_Stage.sar_yr2000_cust_use
	FROM EXP_Default_Archive
	RIGHT OUTER JOIN EXP_Default_Stage
	ON EXP_Default_Stage.pif_symbol = EXP_Default_Archive.arch_pif_symbol AND EXP_Default_Stage.pif_policy_number = EXP_Default_Archive.arch_pif_policy_number AND EXP_Default_Stage.pif_module = EXP_Default_Archive.arch_pif_module AND EXP_Default_Stage.sar_id = EXP_Default_Archive.arch_sar_id AND EXP_Default_Stage.sar_insurance_line = EXP_Default_Archive.arch_sar_insurance_line AND EXP_Default_Stage.sar_location_x = EXP_Default_Archive.arch_sar_location_x AND EXP_Default_Stage.sar_sub_location_x = EXP_Default_Archive.arch_sar_sub_location_x AND EXP_Default_Stage.sar_risk_unit_group = EXP_Default_Archive.arch_sar_risk_unit_group AND EXP_Default_Stage.sar_class_code_grp_x = EXP_Default_Archive.arch_sar_class_code_grp_x AND EXP_Default_Stage.sar_class_code_mem_x = EXP_Default_Archive.arch_sar_class_code_mem_x AND EXP_Default_Stage.sar_unit = EXP_Default_Archive.arch_sar_unit AND EXP_Default_Stage.sar_risk_unit_continued = EXP_Default_Archive.arch_sar_risk_unit_continued AND EXP_Default_Stage.sar_seq_rsk_unt_a = EXP_Default_Archive.arch_sar_seq_rsk_unt_a AND EXP_Default_Stage.sar_major_peril = EXP_Default_Archive.arch_sar_major_peril AND EXP_Default_Stage.sar_seq_no = EXP_Default_Archive.arch_sar_seq_no AND EXP_Default_Stage.sar_cov_eff_year = EXP_Default_Archive.arch_sar_cov_eff_year AND EXP_Default_Stage.sar_cov_eff_month = EXP_Default_Archive.arch_sar_cov_eff_month AND EXP_Default_Stage.sar_cov_eff_day = EXP_Default_Archive.arch_sar_cov_eff_day AND EXP_Default_Stage.sar_part_code = EXP_Default_Archive.arch_sar_part_code AND EXP_Default_Stage.sar_entrd_date = EXP_Default_Archive.arch_sar_entrd_date AND EXP_Default_Stage.sar_transaction = EXP_Default_Archive.arch_sar_transaction AND EXP_Default_Stage.sar_premium = EXP_Default_Archive.arch_sar_premium AND EXP_Default_Stage.sar_state = EXP_Default_Archive.arch_sar_state
),
EXP_Evaluate AS (
	SELECT
	arch_pif_symbol,
	arch_pif_policy_number,
	arch_pif_module,
	arch_sar_location_x,
	arch_sar_sub_location_x,
	arch_sar_unit,
	arch_sar_major_peril,
	arch_sar_seq_no,
	arch_sar_entrd_date,
	arch_sar_transaction,
	arch_sar_premium,
	arch_sar_agents_comm_rate,
	arch_sar_state,
	arch_sar_loc_prov_territory,
	arch_sar_city,
	arch_sar_section,
	arch_sar_class_code,
	arch_sar_sub_line,
	arch_sar_zip_postal_code,
	arch_sar_yr2000_cust_use,
	pif_symbol,
	pif_policy_number,
	pif_module,
	pif_symbol  ||  pif_policy_number  || pif_module AS PolicyKey,
	sar_location_x,
	sar_sub_location_x,
	sar_unit,
	sar_major_peril,
	sar_seq_no,
	sar_entrd_date,
	sar_transaction,
	sar_premium,
	sar_agents_comm_rate,
	sar_state,
	sar_loc_prov_territory,
	sar_city,
	sar_section,
	sar_class_code,
	sar_sub_line,
	sar_zip_postal_code,
	sar_yr2000_cust_use,
	-- *INF*: DECODE(TRUE,arch_sar_yr2000_cust_use = sar_yr2000_cust_use, 'NOCHANGE',
	-- ISNULL(arch_sar_yr2000_cust_use), 'NEWTRANSACTION',
	-- arch_sar_yr2000_cust_use <> sar_yr2000_cust_use AND 
	-- (arch_sar_state <> sar_state OR 
	-- arch_sar_loc_prov_territory <> sar_loc_prov_territory OR 
	-- arch_sar_city <> sar_city OR 
	-- arch_sar_zip_postal_code <> sar_zip_postal_code), 'RISKLOCATIONLEVELCHANGE',
	-- arch_sar_yr2000_cust_use <> sar_yr2000_cust_use AND 
	-- (arch_sar_agents_comm_rate <> sar_agents_comm_rate  OR 
	-- arch_sar_section <> sar_section OR
	-- arch_sar_class_code  <> sar_class_code OR 
	-- arch_sar_sub_line <> sar_sub_line  ), 'COVERAGEDETAILLEVELCHANGE'  )
	DECODE(TRUE,
		arch_sar_yr2000_cust_use = sar_yr2000_cust_use, 'NOCHANGE',
		arch_sar_yr2000_cust_use IS NULL, 'NEWTRANSACTION',
		arch_sar_yr2000_cust_use <> sar_yr2000_cust_use AND ( arch_sar_state <> sar_state OR arch_sar_loc_prov_territory <> sar_loc_prov_territory OR arch_sar_city <> sar_city OR arch_sar_zip_postal_code <> sar_zip_postal_code ), 'RISKLOCATIONLEVELCHANGE',
		arch_sar_yr2000_cust_use <> sar_yr2000_cust_use AND ( arch_sar_agents_comm_rate <> sar_agents_comm_rate OR arch_sar_section <> sar_section OR arch_sar_class_code <> sar_class_code OR arch_sar_sub_line <> sar_sub_line ), 'COVERAGEDETAILLEVELCHANGE') AS v_Status_flag,
	-- *INF*: IIF(ISNULL(v_Status_flag),'UNKNOWN',v_Status_flag)
	IFF(v_Status_flag IS NULL, 'UNKNOWN', v_Status_flag) AS StatusFlag
	FROM JNR_Stage_Archive_Data
),
FIL_Records AS (
	SELECT
	PolicyKey, 
	StatusFlag AS Flag, 
	arch_sar_location_x, 
	arch_sar_sub_location_x, 
	arch_sar_unit, 
	arch_sar_major_peril, 
	arch_sar_seq_no, 
	arch_sar_entrd_date, 
	arch_sar_transaction, 
	arch_sar_premium, 
	arch_sar_agents_comm_rate, 
	arch_sar_state, 
	arch_sar_loc_prov_territory, 
	arch_sar_city, 
	arch_sar_section, 
	arch_sar_class_code, 
	arch_sar_sub_line, 
	arch_sar_zip_postal_code, 
	sar_location_x, 
	sar_sub_location_x, 
	sar_unit, 
	sar_major_peril, 
	sar_seq_no, 
	sar_entrd_date, 
	sar_transaction, 
	sar_premium, 
	sar_agents_comm_rate, 
	sar_state, 
	sar_loc_prov_territory, 
	sar_city, 
	sar_section, 
	sar_class_code, 
	sar_sub_line, 
	sar_zip_postal_code
	FROM EXP_Evaluate
	WHERE TRUE
),
SRT_Sort_Policies AS (
	SELECT
	PolicyKey, 
	Flag
	FROM FIL_Records
	ORDER BY PolicyKey ASC, Flag ASC
),
EXP_Values AS (
	SELECT
	PolicyKey,
	Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate
	FROM SRT_Sort_Policies
),
Work_PolicyTransactionStatus AS (
	INSERT INTO Work_PolicyTransactionStatus
	(PolicyKey, PolicyStatus, AuditID, CreatedDate, ModifiedDate)
	SELECT 
	POLICYKEY, 
	Flag AS POLICYSTATUS, 
	AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE
	FROM EXP_Values
),
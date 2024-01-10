WITH
SQ_pif_4514_stage AS (
	SELECT DISTINCT AB.pif_symbol,
	                AB.pif_policy_number,
	                AB.pif_module,
	                AB.sar_id,
	                AB.sar_insurance_line,
	                AB.sar_location_x,
	                AB.sar_sub_location_x,
	                AB.sar_risk_unit_group,
	                AB.sar_class_code_grp_x,
	                AB.sar_class_code_mem_x,
	               (sar_unit + sar_risk_unit_continued) as sar_unit,  
	                CASE Len(Ltrim(Rtrim(Coalesce(sar_seq_rsk_unt_a, '')))) WHEN '0' THEN 'N/A'  ELSE 
					CASE Len(Ltrim(Rtrim(Coalesce(sar_seq_rsk_unt_a, '')))) WHEN '1' THEN Ltrim(Rtrim(sar_seq_rsk_unt_a)) + '0' 
					ELSE Ltrim(Rtrim(sar_seq_rsk_unt_a))   END   END     AS sar_seq_rsk_unt_a,
	                AB.sar_type_exposure,
	                AB.sar_major_peril,
	                AB.sar_seq_no,
	                AB.sar_cov_eff_year,
	                AB.sar_cov_eff_month,
	                AB.sar_cov_eff_day,
	                AB.sar_part_code,
	                AB.sar_annual_state_line,
	                AB.sar_state,
	                AB.sar_loc_prov_territory,
	                AB.sar_county_first_two,
	                AB.sar_county_last_one,
	                AB.sar_city,
	                AB.sar_rsn_amend_one,
	                AB.sar_rsn_amend_two,
	                AB.sar_rsn_amend_three,
	                AB.sar_special_use,
	                AB.sar_stat_breakdown_line,
	                AB.sar_user_line,
	                AB.sar_section,
	                AB.sar_rating_date_ind,
	                AB.sar_class_1_4,
	                AB.sar_class_5_6,
	                AB.sar_exposure, 
	                AB.sar_sub_line,
	                AB.sar_code_1,
	                AB.sar_code_2,
	                AB.sar_code_3,
	                AB.sar_code_4,
	                AB.sar_code_5,
	                AB.sar_code_6,
	                AB.sar_code_7,
	                AB.sar_code_8,
	                AB.sar_code_9,
	                AB.sar_code_10,
	                AB.sar_code_11,
	                AB.sar_code_12,
	                AB.sar_code_13,
	                AB.sar_code_14,
	                AB.sar_code_15,
	                AB.sar_zip_postal_code
	FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_4514_stage AB, @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_02_stage B
	WHERE AB.pif_symbol + AB.pif_policy_number + AB.pif_module = B.pif_symbol + B.pif_policy_number + B.pif_module 
	AND (LTRIM(RTRIM(pif_eff_yr_a)) <> '9999' or LTRIM(RTRIM(pif_exp_yr_a)) <> '9999') AND
	LTRIM(RTRIM(SUBSTRING(CAST(pif_full_agency_number AS char(7)),1,2)+ SUBSTRING(CAST(pif_full_agency_number AS char(7)),5,3))) <> '99999'
	
	
	---- By joining to PIF_02_stage table we are not reading the data from PIF_4514_Stage where policy_eff_year or policy_exp_year <> '9999' or Policy which belongs to --- internal agency.
),
EXP_Default AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module,
	-- *INF*: (pif_symbol  || pif_policy_number || pif_module)
	( pif_symbol || pif_policy_number || pif_module 
	) AS Pol_Key,
	sar_id,
	sar_insurance_line,
	sar_location_x,
	sar_sub_location_x,
	sar_risk_unit_group,
	sar_class_code_grp_x,
	sar_class_code_mem_x,
	sar_unit,
	sar_seq_rsk_unt_a,
	sar_type_exposure,
	sar_major_peril,
	sar_seq_no,
	sar_cov_eff_year,
	sar_cov_eff_month,
	sar_cov_eff_day,
	sar_part_code,
	sar_annual_state_line,
	sar_state,
	sar_loc_prov_territory,
	sar_county_first_two,
	sar_county_last_one,
	sar_city,
	sar_rsn_amend_one,
	sar_rsn_amend_two,
	sar_rsn_amend_three,
	sar_special_use,
	sar_stat_breakdown_line,
	sar_user_line,
	sar_section,
	sar_rating_date_ind,
	sar_class_1_4,
	sar_class_5_6,
	sar_exposure,
	sar_sub_line,
	sar_code_1,
	sar_code_2,
	sar_code_3,
	sar_code_4,
	sar_code_5,
	sar_code_6,
	sar_code_7,
	sar_code_8,
	sar_code_9,
	sar_code_10,
	sar_code_11,
	sar_code_12,
	sar_code_13,
	sar_code_14,
	sar_code_15,
	sar_zip_postal_code
	FROM SQ_pif_4514_stage
),
EXP_Values AS (
	SELECT
	Pol_Key,
	sar_id,
	sar_insurance_line,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_insurance_line)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_insurance_line
	) AS sar_insurance_line_Out,
	sar_location_x,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_location_x)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_location_x
	) AS sar_location_Out,
	sar_sub_location_x,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_sub_location_x)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_sub_location_x
	) AS sar_sub_location_x1,
	sar_risk_unit_group,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_risk_unit_group)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_risk_unit_group
	) AS sar_risk_unit_group_Out,
	sar_class_code_grp_x,
	sar_class_code_mem_x,
	-- *INF*:  ( sar_class_code_grp_x || sar_class_code_mem_x)
	( sar_class_code_grp_x || sar_class_code_mem_x 
	) AS v_risk_unit_group_seq,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_risk_unit_group_seq)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_risk_unit_group_seq
	) AS risk_unit_group_seq_Out,
	sar_unit,
	-- *INF*: IIF(LENGTH(LTRIM(RTRIM(sar_unit)))= 0 OR IS_SPACES(LTRIM(RTRIM(sar_unit))), '000000',sar_unit)
	IFF(LENGTH(LTRIM(RTRIM(sar_unit
				)
			)
		) = 0 
		OR LENGTH(LTRIM(RTRIM(sar_unit
			)
		))>0 AND TRIM(LTRIM(RTRIM(sar_unit
			)
		))='',
		'000000',
		sar_unit
	) AS v_sar_unit,
	v_sar_unit AS sar_risk_unit,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_risk_unit)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_risk_unit
	) AS sar_risk_unit_Out,
	sar_seq_rsk_unt_a,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_seq_rsk_unt_a)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_seq_rsk_unt_a
	) AS sar_rsk_unit_seq_out,
	sar_type_exposure,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_type_exposure)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_type_exposure
	) AS sar_type_exposure_out,
	sar_major_peril,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_major_peril)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_major_peril
	) AS sar_major_peril_out,
	sar_seq_no,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_seq_no)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_seq_no
	) AS sar_major_peril_seq_no,
	sar_cov_eff_year,
	-- *INF*: TO_CHAR(sar_cov_eff_year)
	TO_CHAR(sar_cov_eff_year
	) AS v_sar_cov_eff_year,
	sar_cov_eff_month,
	-- *INF*: LPAD(TO_CHAR(sar_cov_eff_month),2,'0')
	LPAD(TO_CHAR(sar_cov_eff_month
		), 2, '0'
	) AS v_sar_cov_eff_month,
	sar_cov_eff_day,
	-- *INF*: LPAD(TO_CHAR(sar_cov_eff_day),2,'0')
	LPAD(TO_CHAR(sar_cov_eff_day
		), 2, '0'
	) AS v_sar_cov_eff_day,
	-- *INF*: LPAD(TO_CHAR(sar_cov_eff_month),2,'0') || '/' || LPAD(TO_CHAR(sar_cov_eff_day),2,'0')	||	'/'	||
	-- TO_CHAR(sar_cov_eff_year)
	LPAD(TO_CHAR(sar_cov_eff_month
		), 2, '0'
	) || '/' || LPAD(TO_CHAR(sar_cov_eff_day
		), 2, '0'
	) || '/' || TO_CHAR(sar_cov_eff_year
	) AS v_sar_cov_eff_date,
	-- *INF*: TO_DATE(v_sar_cov_eff_month  || '/'  || v_sar_cov_eff_day  || '/'  || v_sar_cov_eff_year, 'MM/DD/YYYY')
	TO_DATE(v_sar_cov_eff_month || '/' || v_sar_cov_eff_day || '/' || v_sar_cov_eff_year, 'MM/DD/YYYY'
	) AS sar_cov_eff_date,
	sar_part_code,
	sar_state,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_state)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_state
	) AS sar_state_Out,
	sar_loc_prov_territory,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_loc_prov_territory)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_loc_prov_territory
	) AS sar_loc_prov_territory_Out,
	sar_special_use,
	-- *INF*: IIF(IS_SPACES(sar_special_use) OR ISNULL(sar_special_use) OR LENGTH(sar_special_use)= 0,'N/A',sar_special_use)
	-- 
	-- --- User Defined Function is triming the spaces on statistical code which we dont want to do since this field is used for Loss Master Bureau Reporting 
	-- 
	-- --:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_special_use)
	IFF(LENGTH(sar_special_use)>0 AND TRIM(sar_special_use)='' 
		OR sar_special_use IS NULL 
		OR LENGTH(sar_special_use
		) = 0,
		'N/A',
		sar_special_use
	) AS sar_special_use_out,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_special_use)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_special_use
	) AS sar_special_use_lkp,
	sar_class_1_4,
	sar_class_5_6,
	-- *INF*: (sar_class_1_4  || sar_class_5_6)
	( sar_class_1_4 || sar_class_5_6 
	) AS v_sar_class_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_sar_class_code)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_sar_class_code
	) AS sar_class_code_out,
	sar_exposure,
	sar_sub_line,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_sub_line)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_sub_line
	) AS sar_sub_line_out,
	sar_zip_postal_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_zip_postal_code)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_zip_postal_code
	) AS sar_zip_postal_code_out,
	sar_annual_state_line,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_annual_state_line)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_annual_state_line
	) AS sar_annual_state_line_out,
	sar_county_first_two,
	sar_county_last_one,
	sar_city,
	-- *INF*: (sar_county_first_two  || sar_county_last_one || sar_city)
	( sar_county_first_two || sar_county_last_one || sar_city 
	) AS v_sar_tax_location,
	-- *INF*: IIF(IS_SPACES(v_sar_tax_location) OR ISNULL(v_sar_tax_location) OR LENGTH(v_sar_tax_location)= 0,'N/A',v_sar_tax_location)
	-- 
	-- --- User Defined Function is triming the spaces on statistical code which we dont want to do since this field is used for Loss Master Bureau Reporting 
	-- 
	-- --:UDF.DEFAULT_VALUE_FOR_STRINGS(v_sar_tax_location)
	IFF(LENGTH(v_sar_tax_location)>0 AND TRIM(v_sar_tax_location)='' 
		OR v_sar_tax_location IS NULL 
		OR LENGTH(v_sar_tax_location
		) = 0,
		'N/A',
		v_sar_tax_location
	) AS sar_tax_location_out,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_sar_tax_location)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_sar_tax_location
	) AS sar_tax_location_lkp,
	sar_stat_breakdown_line,
	sar_user_line,
	-- *INF*: (sar_stat_breakdown_line  ||  sar_user_line)
	( sar_stat_breakdown_line || sar_user_line 
	) AS v_sar_product_line,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_sar_product_line)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_sar_product_line
	) AS sar_product_line,
	sar_section,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_section)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_section
	) AS sar_section_Out,
	sar_rating_date_ind,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_rating_date_ind)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_rating_date_ind
	) AS sar_rating_date_ind_Out,
	'N/A' AS default_NA,
	-- *INF*: TO_DATE('1/1/1800 00:00:00' , 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('1/1/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59' , 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	SYSDATE AS created_date,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	sar_rsn_amend_one,
	sar_rsn_amend_two,
	sar_rsn_amend_three,
	-- *INF*: (sar_rsn_amend_one  ||  sar_rsn_amend_two  || sar_rsn_amend_three)
	( sar_rsn_amend_one || sar_rsn_amend_two || sar_rsn_amend_three 
	) AS V_sar_rsn_amend_Code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(V_sar_rsn_amend_Code)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(V_sar_rsn_amend_Code
	) AS sar_rsn_amend_code,
	sar_code_1,
	sar_code_2,
	sar_code_3,
	sar_code_4,
	sar_code_5,
	sar_code_6,
	sar_code_7,
	sar_code_8,
	sar_code_9,
	sar_code_10,
	sar_code_11,
	sar_code_12,
	sar_code_13,
	sar_code_14,
	sar_code_15,
	-- *INF*: (sar_code_1  ||  sar_code_2  ||  sar_code_3  || sar_code_4  ||  sar_code_5 ||  sar_code_6  || sar_code_7  || sar_code_8  || sar_code_9  || sar_code_10  || sar_code_11  || sar_code_12  || sar_code_13 || sar_code_14  || sar_code_15)
	( sar_code_1 || sar_code_2 || sar_code_3 || sar_code_4 || sar_code_5 || sar_code_6 || sar_code_7 || sar_code_8 || sar_code_9 || sar_code_10 || sar_code_11 || sar_code_12 || sar_code_13 || sar_code_14 || sar_code_15 
	) AS v_sar_statistical_code,
	-- *INF*: IIF(IS_SPACES(v_sar_statistical_code) OR ISNULL(v_sar_statistical_code) OR LENGTH(v_sar_statistical_code)= 0,'N/A',v_sar_statistical_code)
	-- 
	-- --- User Defined Function is triming the spaces on statistical code which we dont want to do since this field is used for Loss Master Bureau Reporting 
	-- 
	-- --:UDF.DEFAULT_VALUE_FOR_STRINGS(v_sar_statistical_code)
	IFF(LENGTH(v_sar_statistical_code)>0 AND TRIM(v_sar_statistical_code)='' 
		OR v_sar_statistical_code IS NULL 
		OR LENGTH(v_sar_statistical_code
		) = 0,
		'N/A',
		v_sar_statistical_code
	) AS sar_statistical_code_out,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_sar_statistical_code)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_sar_statistical_code
	) AS sar_statistical_code_lkp
	FROM EXP_Default
),
LKP_V2_Coverage AS (
	SELECT
	cov_ak_id,
	ins_line,
	loc_unit_num,
	sub_loc_unit_num,
	risk_unit_grp,
	risk_unit_grp_seq_num,
	risk_unit,
	risk_unit_seq_num,
	major_peril_code,
	major_peril_seq_num,
	pms_type_exposure,
	cov_eff_date,
	pol_key
	FROM (
		SELECT C.cov_ak_id                    AS cov_ak_id,
		       Rtrim(C.ins_line)              AS ins_line,
		       Rtrim(C.loc_unit_num)          AS loc_unit_num,
		       Rtrim(C.sub_loc_unit_num)      AS sub_loc_unit_num,
		       Rtrim(C.risk_unit_grp)         AS risk_unit_grp,
		       Rtrim(C.risk_unit_grp_seq_num) AS risk_unit_grp_seq_num,
		       Rtrim(C.risk_unit)             AS risk_unit,
		       Rtrim(C.risk_unit_seq_num)     AS risk_unit_seq_num,
		       Rtrim(C.major_peril_code)      AS major_peril_code,
		       Rtrim(C.major_peril_seq_num)   AS major_peril_seq_num,
		       Rtrim(C.pms_type_exposure)     AS pms_type_exposure,
		       C.cov_eff_date                 AS cov_eff_date,
		       P.pol_key                          AS pol_key
		FROM   
		@{pipeline().parameters.TARGET_TABLE_OWNER}.coverage C, 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.policy P 
		WHERE 
		C.pol_ak_id = P.pol_ak_id
		AND  C.crrnt_snpsht_flag =1 
		AND P.crrnt_snpsht_flag =1
		AND P.pol_key 
		IN 
		(SELECT DISTINCT (A.pif_symbol + A.pif_policy_number + A.pif_module) 
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}..pif_4514_stage A)
		ORDER BY P.pol_key ----
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ins_line,loc_unit_num,sub_loc_unit_num,risk_unit_grp,risk_unit_grp_seq_num,risk_unit,risk_unit_seq_num,major_peril_code,major_peril_seq_num,pms_type_exposure,cov_eff_date,pol_key ORDER BY cov_ak_id DESC) = 1
),
EXP_Lookup_Values AS (
	SELECT
	LKP_V2_Coverage.cov_ak_id,
	EXP_Values.crrnt_snpsht_flag,
	EXP_Values.audit_id,
	EXP_Values.eff_from_date,
	EXP_Values.eff_to_date,
	EXP_Values.source_sys_id,
	EXP_Values.created_date,
	EXP_Values.created_date AS modified_date,
	EXP_Values.sar_id,
	EXP_Values.sar_section_Out,
	EXP_Values.sar_part_code,
	EXP_Values.sar_state_Out,
	EXP_Values.sar_loc_prov_territory_Out,
	EXP_Values.sar_special_use_lkp,
	EXP_Values.sar_special_use_out,
	EXP_Values.sar_class_code_out,
	EXP_Values.sar_sub_line_out,
	EXP_Values.sar_zip_postal_code_out,
	EXP_Values.sar_annual_state_line_out,
	EXP_Values.sar_statistical_code_lkp,
	EXP_Values.sar_statistical_code_out,
	EXP_Values.sar_tax_location_lkp,
	EXP_Values.sar_tax_location_out,
	EXP_Values.sar_product_line,
	EXP_Values.sar_rsn_amend_code,
	EXP_Values.sar_exposure,
	EXP_Values.default_NA,
	EXP_Values.sar_rating_date_ind_Out
	FROM EXP_Values
	LEFT JOIN LKP_V2_Coverage
	ON LKP_V2_Coverage.ins_line = EXP_Values.sar_insurance_line_Out AND LKP_V2_Coverage.loc_unit_num = EXP_Values.sar_location_Out AND LKP_V2_Coverage.sub_loc_unit_num = EXP_Values.sar_sub_location_x1 AND LKP_V2_Coverage.risk_unit_grp = EXP_Values.sar_risk_unit_group_Out AND LKP_V2_Coverage.risk_unit_grp_seq_num = EXP_Values.risk_unit_group_seq_Out AND LKP_V2_Coverage.risk_unit = EXP_Values.sar_risk_unit_Out AND LKP_V2_Coverage.risk_unit_seq_num = EXP_Values.sar_rsk_unit_seq_out AND LKP_V2_Coverage.major_peril_code = EXP_Values.sar_major_peril_out AND LKP_V2_Coverage.major_peril_seq_num = EXP_Values.sar_major_peril_seq_no AND LKP_V2_Coverage.pms_type_exposure = EXP_Values.sar_type_exposure_out AND LKP_V2_Coverage.cov_eff_date = EXP_Values.sar_cov_eff_date AND LKP_V2_Coverage.pol_key = EXP_Values.Pol_Key
),
LKP_Tgt_Temp_Policy_Transaction AS (
	SELECT
	temp_pol_trans_id,
	temp_pol_trans_ak_id,
	cov_ak_id,
	sar_id,
	part_code,
	section_code,
	risk_state_prov_code,
	risk_zip_code,
	terr_code,
	tax_loc,
	class_code,
	exposure,
	sub_line_code,
	source_sar_asl,
	source_sar_prdct_line,
	source_sar_sp_use_code,
	source_statistical_code,
	rsn_amended_code,
	rating_date_ind
	FROM (
		SELECT TPT.temp_pol_trans_id       AS temp_pol_trans_id,
		       TPT.temp_pol_trans_ak_id    AS temp_pol_trans_ak_id,
		       LTRIM(RTRIM(TPT.risk_zip_code))           AS risk_zip_code,
			 LTRIM(RTRIM(TPT.terr_code))               AS terr_code,
		       LTRIM(RTRIM(TPT.tax_loc))                 AS tax_loc,
		       LTRIM(RTRIM(TPT.class_code))              AS class_code,
		       TPT.exposure                                   AS exposure,
		       LTRIM(RTRIM(TPT.sub_line_code))           AS sub_line_code,
		       LTRIM(RTRIM(TPT.source_sar_asl))          AS source_sar_asl,
		       LTRIM(RTRIM(TPT.source_sar_prdct_line))   AS source_sar_prdct_line,
		       LTRIM(RTRIM(TPT.source_sar_sp_use_code))  AS source_sar_sp_use_code,
		       LTRIM(RTRIM(TPT.source_statistical_code)) AS source_statistical_code,
		       LTRIM(RTRIM(TPT.rsn_amended_code))        AS rsn_amended_code,
		       TPT.cov_ak_id               AS cov_ak_id,
		       LTRIM(RTRIM(TPT.sar_id))                  AS sar_id,
		       LTRIM(RTRIM(TPT.part_code))               AS part_code,
		       LTRIM(RTRIM(TPT.section_code))            AS section_code,
		       LTRIM(RTRIM(TPT.risk_state_prov_code))    AS risk_state_prov_code,
		       LTRIM(RTRIM(TPT.rating_date_ind)) as rating_date_ind 
		FROM   
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.Temp_policy_transaction TPT, 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.coverage C, 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.policy P 
		WHERE 
		TPT.cov_ak_id = C.cov_ak_id 
		AND  C.pol_ak_id = P.pol_ak_id
		AND  C.crrnt_snpsht_flag =1 
		AND P.crrnt_snpsht_flag =1
		AND TPT.crrnt_snpsht_flag = 1
		AND P.pol_key 
		IN 
		(SELECT DISTINCT (A.pif_symbol + A.pif_policy_number + A.pif_module) 
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}..pif_4514_stage A)
		ORDER BY TPT.cov_ak_id  ---
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY cov_ak_id,sar_id,part_code,section_code,risk_state_prov_code,risk_zip_code,terr_code,tax_loc,class_code,exposure,sub_line_code,source_sar_asl,source_sar_prdct_line,source_sar_sp_use_code,source_statistical_code,rsn_amended_code,rating_date_ind ORDER BY temp_pol_trans_id DESC) = 1
),
RTR_Insert_Update AS (
	SELECT
	LKP_Tgt_Temp_Policy_Transaction.temp_pol_trans_id,
	LKP_Tgt_Temp_Policy_Transaction.temp_pol_trans_ak_id,
	EXP_Lookup_Values.cov_ak_id,
	EXP_Lookup_Values.crrnt_snpsht_flag,
	EXP_Lookup_Values.audit_id,
	EXP_Lookup_Values.eff_from_date,
	EXP_Lookup_Values.eff_to_date,
	EXP_Lookup_Values.source_sys_id,
	EXP_Lookup_Values.created_date,
	EXP_Lookup_Values.modified_date,
	EXP_Lookup_Values.sar_id,
	EXP_Lookup_Values.sar_section_Out,
	EXP_Lookup_Values.sar_rsn_amend_code,
	EXP_Lookup_Values.sar_part_code,
	EXP_Lookup_Values.sar_state_Out,
	EXP_Lookup_Values.sar_zip_postal_code_out,
	EXP_Lookup_Values.sar_loc_prov_territory_Out,
	EXP_Lookup_Values.sar_tax_location_out,
	EXP_Lookup_Values.sar_class_code_out,
	EXP_Lookup_Values.sar_exposure,
	EXP_Lookup_Values.sar_sub_line_out,
	EXP_Lookup_Values.sar_annual_state_line_out,
	EXP_Lookup_Values.sar_product_line,
	EXP_Lookup_Values.sar_special_use_out,
	EXP_Lookup_Values.sar_statistical_code_out,
	EXP_Lookup_Values.default_NA,
	EXP_Lookup_Values.sar_rating_date_ind_Out
	FROM EXP_Lookup_Values
	LEFT JOIN LKP_Tgt_Temp_Policy_Transaction
	ON LKP_Tgt_Temp_Policy_Transaction.cov_ak_id = EXP_Lookup_Values.cov_ak_id AND LKP_Tgt_Temp_Policy_Transaction.sar_id = EXP_Lookup_Values.sar_id AND LKP_Tgt_Temp_Policy_Transaction.part_code = EXP_Lookup_Values.sar_part_code AND LKP_Tgt_Temp_Policy_Transaction.section_code = EXP_Lookup_Values.sar_section_Out AND LKP_Tgt_Temp_Policy_Transaction.risk_state_prov_code = EXP_Lookup_Values.sar_state_Out AND LKP_Tgt_Temp_Policy_Transaction.risk_zip_code = EXP_Lookup_Values.sar_zip_postal_code_out AND LKP_Tgt_Temp_Policy_Transaction.terr_code = EXP_Lookup_Values.sar_loc_prov_territory_Out AND LKP_Tgt_Temp_Policy_Transaction.tax_loc = EXP_Lookup_Values.sar_tax_location_lkp AND LKP_Tgt_Temp_Policy_Transaction.class_code = EXP_Lookup_Values.sar_class_code_out AND LKP_Tgt_Temp_Policy_Transaction.exposure = EXP_Lookup_Values.sar_exposure AND LKP_Tgt_Temp_Policy_Transaction.sub_line_code = EXP_Lookup_Values.sar_sub_line_out AND LKP_Tgt_Temp_Policy_Transaction.source_sar_asl = EXP_Lookup_Values.sar_annual_state_line_out AND LKP_Tgt_Temp_Policy_Transaction.source_sar_prdct_line = EXP_Lookup_Values.sar_product_line AND LKP_Tgt_Temp_Policy_Transaction.source_sar_sp_use_code = EXP_Lookup_Values.sar_special_use_lkp AND LKP_Tgt_Temp_Policy_Transaction.source_statistical_code = EXP_Lookup_Values.sar_statistical_code_lkp AND LKP_Tgt_Temp_Policy_Transaction.rsn_amended_code = EXP_Lookup_Values.sar_rsn_amend_code AND LKP_Tgt_Temp_Policy_Transaction.rating_date_ind = EXP_Lookup_Values.sar_rating_date_ind_Out
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE IIF( ISNULL(temp_pol_trans_id),TRUE,FALSE)),
RTR_Insert_Update_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE IIF( NOT ISNULL(temp_pol_trans_id),TRUE,FALSE)),
SEQ_Temp_Trans_AK_ID AS (
	CREATE SEQUENCE SEQ_Temp_Trans_AK_ID
	START = 0
	INCREMENT = 1;
),
temp_policy_transaction_Insert AS (
	INSERT INTO @{pipeline().parameters.SOURCE_TABLE_OWNER}.temp_policy_transaction
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, temp_pol_trans_ak_id, cov_ak_id, sar_id, section_code, rsn_amended_code, part_code, risk_state_prov_code, risk_zip_code, terr_code, tax_loc, class_code, exposure, sub_line_code, source_sar_asl, source_sar_prdct_line, source_sar_sp_use_code, source_statistical_code, rating_date_ind)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	SEQ_Temp_Trans_AK_ID.NEXTVAL AS TEMP_POL_TRANS_AK_ID, 
	COV_AK_ID, 
	SAR_ID, 
	sar_section_Out AS SECTION_CODE, 
	sar_rsn_amend_code AS RSN_AMENDED_CODE, 
	sar_part_code AS PART_CODE, 
	sar_state_Out AS RISK_STATE_PROV_CODE, 
	sar_zip_postal_code_out AS RISK_ZIP_CODE, 
	sar_loc_prov_territory_Out AS TERR_CODE, 
	sar_tax_location_out AS TAX_LOC, 
	sar_class_code_out AS CLASS_CODE, 
	sar_exposure AS EXPOSURE, 
	sar_sub_line_out AS SUB_LINE_CODE, 
	sar_annual_state_line_out AS SOURCE_SAR_ASL, 
	sar_product_line AS SOURCE_SAR_PRDCT_LINE, 
	sar_special_use_out AS SOURCE_SAR_SP_USE_CODE, 
	sar_statistical_code_out AS SOURCE_STATISTICAL_CODE, 
	sar_rating_date_ind_Out AS RATING_DATE_IND
	FROM RTR_Insert_Update_INSERT
),
UPD_Update AS (
	SELECT
	temp_pol_trans_id AS temp_pol_trans_id3, 
	cov_ak_id AS cov_ak_id3, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag3, 
	audit_id AS audit_id3, 
	eff_from_date AS eff_from_date3, 
	eff_to_date AS eff_to_date3, 
	source_sys_id AS source_sys_id3, 
	created_date AS created_date3, 
	modified_date AS modified_date3, 
	sar_id AS sar_id3, 
	sar_section_Out AS sar_section_Out3, 
	sar_rsn_amend_code AS sar_rsn_amend_code3, 
	sar_part_code AS sar_part_code3, 
	sar_state_Out AS sar_state_Out3, 
	sar_zip_postal_code_out AS sar_zip_postal_code_out3, 
	sar_loc_prov_territory_Out AS sar_loc_prov_territory_Out3, 
	sar_tax_location_out AS sar_tax_location_out3, 
	sar_class_code_out AS sar_class_code_out3, 
	sar_exposure AS sar_exposure3, 
	sar_sub_line_out AS sar_sub_line_out3, 
	sar_annual_state_line_out AS sar_annual_state_line_out3, 
	sar_product_line AS sar_product_line3, 
	sar_special_use_out AS sar_special_use_out3, 
	sar_statistical_code_out AS sar_statistical_code_out3, 
	sar_rating_date_ind_Out AS sar_rating_date_ind_Out3
	FROM RTR_Insert_Update_UPDATE
),
temp_policy_transaction_Update AS (
	MERGE INTO temp_policy_transaction AS T
	USING UPD_Update AS S
	ON T.temp_pol_trans_id = S.temp_pol_trans_id3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag3, T.eff_from_date = S.eff_from_date3, T.eff_to_date = S.eff_to_date3, T.source_sys_id = S.source_sys_id3, T.modified_date = S.modified_date3, T.sar_id = S.sar_id3, T.section_code = S.sar_section_Out3, T.rsn_amended_code = S.sar_rsn_amend_code3, T.part_code = S.sar_part_code3, T.risk_state_prov_code = S.sar_state_Out3, T.risk_zip_code = S.sar_zip_postal_code_out3, T.terr_code = S.sar_loc_prov_territory_Out3, T.tax_loc = S.sar_tax_location_out3, T.class_code = S.sar_class_code_out3, T.exposure = S.sar_exposure3, T.sub_line_code = S.sar_sub_line_out3, T.source_sar_asl = S.sar_annual_state_line_out3, T.source_sar_prdct_line = S.sar_product_line3, T.source_sar_sp_use_code = S.sar_special_use_out3, T.source_statistical_code = S.sar_statistical_code_out3, T.rating_date_ind = S.sar_rating_date_ind_Out3
),
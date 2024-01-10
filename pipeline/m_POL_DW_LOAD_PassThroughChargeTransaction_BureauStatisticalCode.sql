WITH
LKP_RiskLocation_RiskLocationAKID AS (
	SELECT
	RiskLocationAKID,
	RiskLocationID,
	CurrentSnapshotFlag,
	PolicyAKID,
	LocationUnitNumber,
	RiskTerritory,
	StateProvinceCode,
	ZipPostalCode,
	TaxLocation
	FROM (
		SELECT RiskLocationAKID   AS RiskLocationAKID,
		       PolicyAKID         AS PolicyAKID,
		LOC.RiskLocationID as RiskLocationID,
		LOC.CurrentSnapshotFlag AS CurrentSnapshotFlag,
		       LTRIM(RTRIM(LocationUnitNumber)) AS LocationUnitNumber,
		       LTRIM(RTRIM(RiskTerritory))      AS RiskTerritory,
		       LTRIM(RTRIM(StateProvinceCode))  AS StateProvinceCode,
		       LTRIM(RTRIM(ZipPostalCode))      AS ZipPostalCode,
		       LTRIM(RTRIM(TaxLocation))        AS TaxLocation
		FROM	@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation LOC
		INNER JOIN  @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Policy POL
		ON	LOC.PolicyAKID = POL.pol_ak_id
		WHERE POL.crrnt_snpsht_flag = 1 AND LOC.CurrentSnapshotFlag =1
		       AND POL.SOURCE_SYS_ID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		       AND  EXISTS  (SELECT DISTINCT PolicyKey FROM  
									@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPolicyKeyList
									WHERE POL.pol_key = PolicyKey AND @{pipeline().parameters.WHERE_CLAUSE_EDW})
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID,LocationUnitNumber,RiskTerritory,StateProvinceCode,ZipPostalCode,TaxLocation ORDER BY RiskLocationAKID DESC) = 1
),
LKP_PolicyCoverage_PolicyCoverageAKID AS (
	SELECT
	PolicyCoverageAKID,
	PolicyCoverageHashKey
	FROM (
		SELECT PolicyCoverageAKID    AS PolicyCoverageAKID,
		       	      PolicyCoverageHashKey AS PolicyCoverageHashKey
		FROM	@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation LOC 
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Policy POL
		         ON LOC.PolicyAKID = POL.pol_ak_id 
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage POLCOV
		ON LOC.RiskLocationAKID = POLCOV.RiskLocationAKID
		WHERE  POL.crrnt_snpsht_flag = 1
		       AND LOC.CurrentSnapshotFlag = 1
		       AND POLCOV.CurrentSnapshotFlag = 1 
		       AND POL.SOURCE_SYS_ID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		       AND  EXISTS  (SELECT DISTINCT PolicyKey FROM  
									@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPolicyKeyList
									WHERE POL.pol_key = PolicyKey AND @{pipeline().parameters.WHERE_CLAUSE_EDW})
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyCoverageHashKey ORDER BY PolicyCoverageAKID DESC) = 1
),
LKP_StatisticalCoverage_StatisticalCoverageAKID AS (
	SELECT
	StatisticalCoverageAKID,
	StatisticalCoverageHashKey
	FROM (
		SELECT StatisticalCoverageAKID AS StatisticalCoverageAKID,
		       StatisticalCoverageHashKey AS StatisticalCoverageHashKey
		FROM	@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation LOC
		INNER JOIN  @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Policy POL
		ON LOC.PolicyAKID = POL.pol_ak_id
		INNER JOIN 	@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage POLCOV
		ON LOC.RiskLocationAKID = POLCOV.RiskLocationAKID
		INNER JOIN 	@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage STATCOV
		ON POLCOV.PolicyCoverageAKID = STATCOV.PolicyCoverageAKID
		WHERE	POL.crrnt_snpsht_flag = 1 AND LOC.CurrentSnapshotFlag =1
				AND STATCOV.CurrentSnapshotFlag =1
				AND POLCOV.CurrentSnapshotFlag =1
		        	AND POL.SOURCE_SYS_ID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		        	AND  EXISTS  (SELECT DISTINCT PolicyKey FROM  
									@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPolicyKeyList
									WHERE POL.pol_key = PolicyKey AND @{pipeline().parameters.WHERE_CLAUSE_EDW})
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StatisticalCoverageHashKey ORDER BY StatisticalCoverageAKID DESC) = 1
),
SQ_pif_4514_stage AS (
	SELECT pif_4514_stage_id,
		RTRIM(pif_symbol),
	       pif_policy_number,
	       pif_module,
	       sar_id,
	       LTRIM(RTRIM(sar_insurance_line)) as sar_insurance_line,
	       CASE LEN(sar_location_x) WHEN '0' THEN LTRIM(RTRIM(sar_unit)) ELSE LTRIM(RTRIM(sar_location_x)) END as sar_location_x,
	       LTRIM(RTRIM(sar_sub_location_x)) as sar_sub_location_x,
	       LTRIM(RTRIM(sar_risk_unit_group)) as sar_risk_unit_group,
	       LTRIM(RTRIM(sar_class_code_grp_x + sar_class_code_mem_x)) as sar_class_code_grp_x,
	       LTRIM(RTRIM(sar_unit + sar_risk_unit_continued))      AS sar_unit,
	       CASE LEN(LTRIM(RTRIM(COALESCE(sar_seq_rsk_unt_a, ''))))
	            WHEN '0' THEN 'N/A'
	                      ELSE LTRIM(RTRIM(sar_seq_rsk_unt_a))
	             END                                         AS sar_seq_rsk_unt_a,
			LTRIM(RTRIM(sar_type_exposure)) as sar_type_exposure,
	        LTRIM(RTRIM(sar_major_peril)) as sar_major_peril,
	        LTRIM(RTRIM(sar_seq_no)) as sar_seq_no,
	       sar_cov_eff_year,
	       sar_cov_eff_month,
	       sar_cov_eff_day,
	       sar_part_code,
	       sar_trans_eff_year,
	       sar_trans_eff_month,
	       sar_trans_eff_day,
	       LTRIM(RTRIM(sar_reinsurance_company_no)),
	       sar_entrd_date,
	       sar_exp_year,
	       sar_exp_month,
	       sar_exp_day,
	       sar_transaction,
	       sar_premium,
	       sar_subpay_amt,
	       sar_original_prem,
	       sar_agents_comm_rate,
	       sar_acct_entrd_date,
	       sar_annual_state_line,
	       LTRIM(RTRIM(sar_state)) as sar_state,
	       LTRIM(RTRIM(sar_loc_prov_territory)) as sar_loc_prov_territory,
	       CASE WHEN LEN(LTRIM(RTRIM(sar_county_first_two)) + LTRIM(RTRIM(sar_county_last_one)) + LTRIM(RTRIM(sar_city))) < 6 THEN '000000' ELSE 
	       LTRIM(RTRIM(sar_county_first_two)) + LTRIM(RTRIM(sar_county_last_one)) + LTRIM(RTRIM(sar_city)) END as sar_city,
	       sar_rsn_amend_one,
	       sar_rsn_amend_two,
	       sar_rsn_amend_three,       
	       sar_special_use,						
	       sar_stat_breakdown_line,				
	       sar_user_line,						
	       LTRIM(RTRIM(sar_section)) as sar_section,
		sar_rating_date_ind,					
	       LTRIM(RTRIM(sar_type_bureau)) as sar_type_bureau,
	       LTRIM(RTRIM(sar_class_1_4 + sar_class_5_6))  AS sar_class_1_4,
	       CASE sar_exposure WHEN 10000000 THEN 0 ELSE 
			   CASE sar_exposure WHEN -9999999999 THEN 0 ELSE 
					CASE sar_exposure WHEN 9999999999 THEN 0 ELSE sar_exposure END END END AS sar_exposure,
	       LTRIM(RTRIM(sar_sub_line)) as sar_sub_line,
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
	       LTRIM(RTRIM(sar_zip_postal_code)) as sar_zip_postal_code,
	       sar_audit_reinst_ind,
	       sar_ky_tax_percentage,
	       logical_flag
	FROM  
	@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME_4514} A 
	@{pipeline().parameters.JOIN_CONDITION}
	(SELECT DISTINCT Policykey FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.work_policytransactionstatus 
	WHERE AuditID = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AND PolicyStatus <> 'NOCHANGE')  B
	ON  A.policykey = B.policykey
	WHERE A.sar_major_peril IN ('078', '088', '089', '183', '255', '499', '256', '257', '258', '259', '898', '899') 
	AND A.logical_flag IN ('0','1','2','3')
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Default AS (
	SELECT
	pif_4514_stage_id,
	pif_symbol,
	pif_policy_number,
	pif_module,
	pif_symbol  ||  pif_policy_number  || pif_module AS Policy_Key,
	sar_id,
	sar_insurance_line,
	sar_location_x,
	sar_sub_location_x,
	sar_risk_unit_group,
	sar_class_code_grp_x,
	sar_unit,
	sar_seq_rsk_unt_a,
	sar_type_exposure,
	sar_major_peril,
	sar_seq_no,
	sar_cov_eff_year,
	sar_cov_eff_month,
	sar_cov_eff_day,
	sar_part_code,
	sar_trans_eff_year,
	sar_trans_eff_month,
	sar_trans_eff_day,
	sar_reinsurance_company_no,
	sar_entrd_date,
	sar_exp_year,
	sar_exp_month,
	sar_exp_day,
	sar_transaction,
	-- *INF*: IIF(
	--   ISNULL(sar_transaction) OR LENGTH(LTRIM(RTRIM(sar_transaction)))=0,
	--   'N/A',
	--   LTRIM(RTRIM(sar_transaction))
	-- )
	IFF(sar_transaction IS NULL 
		OR LENGTH(LTRIM(RTRIM(sar_transaction
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(sar_transaction
			)
		)
	) AS o_sar_transaction,
	sar_premium,
	sar_subpay_amt,
	sar_original_prem,
	sar_agents_comm_rate,
	sar_acct_entrd_date,
	sar_state,
	sar_loc_prov_territory,
	sar_city,
	sar_rsn_amend_one,
	sar_rsn_amend_two,
	sar_rsn_amend_three,
	sar_section,
	sar_type_bureau,
	sar_class_1_4,
	sar_exposure,
	sar_sub_line,
	sar_zip_postal_code,
	sar_ky_tax_percentage,
	-- *INF*: IIF(ISNULL(sar_ky_tax_percentage),0.000,sar_ky_tax_percentage)
	IFF(sar_ky_tax_percentage IS NULL,
		0.000,
		sar_ky_tax_percentage
	) AS sar_ky_tax_percentage_out,
	logical_flag,
	sar_special_use,
	sar_stat_breakdown_line,
	sar_user_line,
	sar_rating_date_ind,
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
	sar_audit_reinst_ind,
	sar_annual_state_line
	FROM SQ_pif_4514_stage
),
LKP_Policy_PolicyAKID_by_Key AS (
	SELECT
	pol_ak_id,
	SupSurchargeExemptID,
	pol_key
	FROM (
		SELECT
		policy.pol_ak_id as pol_ak_id,
		policy.SupSurchargeExemptID as SupSurchargeExemptID,
		ltrim(rtrim(policy.pol_key)) as pol_key
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy
		WHERE crrnt_snpsht_flag =1 AND source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_ak_id) = 1
),
EXP_Values AS (
	SELECT
	LKP_Policy_PolicyAKID_by_Key.pol_ak_id,
	LKP_Policy_PolicyAKID_by_Key.SupSurchargeExemptID,
	EXP_Default.Policy_Key,
	EXP_Default.sar_location_x,
	-- *INF*: LTRIM(RTRIM(sar_location_x))
	LTRIM(RTRIM(sar_location_x
		)
	) AS v_RiskLocation_Unit,
	EXP_Default.sar_state,
	-- *INF*: LTRIM(RTRIM(sar_state))
	LTRIM(RTRIM(sar_state
		)
	) AS v_sar_state,
	EXP_Default.sar_loc_prov_territory,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_loc_prov_territory)
	-- 
	-- --IIF(ISNULL(sar_loc_prov_territory) OR IS_SPACES(sar_loc_prov_territory) OR LENGTH(sar_loc_prov_territory) = 0, 'N/A',
	-- -- LTRIM(RTRIM(sar_loc_prov_territory)))
	-- 
	-- 
	-- 
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_loc_prov_territory
	) AS v_sar_loc_prov_territory,
	EXP_Default.sar_city,
	-- *INF*: LTRIM(RTRIM(sar_city))
	-- 
	-- --IIF(IS_SPACES(LTRIM(RTRIM(sar_city)))  OR ISNULL(LTRIM(RTRIM(sar_city))) OR LENGTH(LTRIM(RTRIM(sar_city))) < 3, '000', LTRIM(RTRIM(sar_city)))
	-- 
	-- 
	LTRIM(RTRIM(sar_city
		)
	) AS v_sar_city,
	-- *INF*: IIF(REG_MATCH(:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_city) ,'(\d{6})')
	-- ,:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_city)
	-- ,'000000')
	-- 
	-- --v_sar_county_first_two  ||  v_sar_county_last_one  ||  v_sar_city
	-- 
	-- --IIF(ISNULL(Tax_Location)  OR IS_SPACES(Tax_Location)  OR LENGTH(Tax_Location) = 0 , '000000', Tax_Location)
	IFF(REGEXP_LIKE(:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_city
			), '(\d{6})'
		),
		:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_city
		),
		'000000'
	) AS v_Tax_Location,
	EXP_Default.sar_zip_postal_code,
	-- *INF*: IIF(ISNULL(sar_zip_postal_code)  OR IS_SPACES(sar_zip_postal_code)  OR LENGTH(sar_zip_postal_code) = 0 , 'N/A', LTRIM(RTRIM(sar_zip_postal_code)))
	IFF(sar_zip_postal_code IS NULL 
		OR LENGTH(sar_zip_postal_code)>0 AND TRIM(sar_zip_postal_code)='' 
		OR LENGTH(sar_zip_postal_code
		) = 0,
		'N/A',
		LTRIM(RTRIM(sar_zip_postal_code
			)
		)
	) AS v_sar_zip_postal_code,
	-- *INF*: :LKP.LKP_RISKLOCATION_RISKLOCATIONAKID(pol_ak_id, v_RiskLocation_Unit, v_sar_loc_prov_territory, v_sar_state, v_sar_zip_postal_code, v_Tax_Location)
	LKP_RISKLOCATION_RISKLOCATIONAKID_pol_ak_id_v_RiskLocation_Unit_v_sar_loc_prov_territory_v_sar_state_v_sar_zip_postal_code_v_Tax_Location.RiskLocationAKID AS v_RiskLocationAKID,
	-- *INF*: IIF(
	--   ISNULL(v_RiskLocationAKID),
	--   -1,
	--   v_RiskLocationAKID
	-- )
	IFF(v_RiskLocationAKID IS NULL,
		- 1,
		v_RiskLocationAKID
	) AS o_RiskLocationAKID,
	EXP_Default.sar_insurance_line,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_insurance_line)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_insurance_line
	) AS v_sar_insurance_line,
	v_sar_insurance_line AS o_sar_insurance_line,
	EXP_Default.sar_type_bureau,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_type_bureau)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_type_bureau
	) AS v_sar_type_bureau,
	EXP_Default.sar_sub_location_x,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_sub_location_x)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_sub_location_x
	) AS v_sar_sub_location_x,
	EXP_Default.sar_risk_unit_group,
	-- *INF*: IIF(REG_MATCH(:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_risk_unit_group) ,'(\d{3})')
	-- ,:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_risk_unit_group)
	-- ,'N/A')
	IFF(REGEXP_LIKE(:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_risk_unit_group
			), '(\d{3})'
		),
		:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_risk_unit_group
		),
		'N/A'
	) AS v_sar_risk_unit_group,
	EXP_Default.sar_class_code_grp_x,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_class_code_grp_x)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_class_code_grp_x
	) AS v_sar_class_code_grp_x,
	EXP_Default.sar_unit,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_unit)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_unit
	) AS v_sar_unit,
	EXP_Default.sar_seq_rsk_unt_a,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_seq_rsk_unt_a)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_seq_rsk_unt_a
	) AS v_sar_seq_rsk_unt_a,
	EXP_Default.sar_type_exposure,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_type_exposure)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_type_exposure
	) AS v_sar_type_exposure,
	EXP_Default.sar_major_peril,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_major_peril)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_major_peril
	) AS v_sar_major_peril,
	EXP_Default.sar_seq_no,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_seq_no)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_seq_no
	) AS v_sar_seq_no,
	EXP_Default.sar_cov_eff_year,
	-- *INF*: TO_CHAR(sar_cov_eff_year)
	TO_CHAR(sar_cov_eff_year
	) AS v_sar_cov_eff_year,
	EXP_Default.sar_cov_eff_month,
	-- *INF*: TO_CHAR(sar_cov_eff_month)
	TO_CHAR(sar_cov_eff_month
	) AS v_sar_cov_eff_month,
	EXP_Default.sar_cov_eff_day,
	-- *INF*: TO_CHAR(sar_cov_eff_day)
	TO_CHAR(sar_cov_eff_day
	) AS v_sar_cov_eff_day,
	-- *INF*: TO_DATE(v_sar_cov_eff_month || '/' || v_sar_cov_eff_day || '/'|| v_sar_cov_eff_year ,'MM/DD/YYYY')
	TO_DATE(v_sar_cov_eff_month || '/' || v_sar_cov_eff_day || '/' || v_sar_cov_eff_year, 'MM/DD/YYYY'
	) AS v_sar_cov_eff_date,
	EXP_Default.sar_agents_comm_rate,
	-- *INF*: IIF(ISNULL(sar_agents_comm_rate) , 0.00000 , sar_agents_comm_rate)
	IFF(sar_agents_comm_rate IS NULL,
		0.00000,
		sar_agents_comm_rate
	) AS v_sar_agents_comm_rate,
	-- *INF*: MD5(TO_CHAR(pol_ak_id)  || 
	--  TO_CHAR(v_RiskLocationAKID)  || 
	--  TO_CHAR(v_sar_insurance_line)  || 
	--  TO_CHAR(v_sar_type_bureau)
	-- )
	MD5(TO_CHAR(pol_ak_id
		) || TO_CHAR(v_RiskLocationAKID
		) || TO_CHAR(v_sar_insurance_line
		) || TO_CHAR(v_sar_type_bureau
		)
	) AS v_PolicyCoverageHashKey,
	-- *INF*: :LKP.LKP_POLICYCOVERAGE_POLICYCOVERAGEAKID(v_PolicyCoverageHashKey)
	LKP_POLICYCOVERAGE_POLICYCOVERAGEAKID_v_PolicyCoverageHashKey.PolicyCoverageAKID AS v_PolicyCoverageAKID,
	-- *INF*: IIF(
	--   ISNULL(v_PolicyCoverageAKID),
	--   -1,
	--   v_PolicyCoverageAKID
	-- )
	IFF(v_PolicyCoverageAKID IS NULL,
		- 1,
		v_PolicyCoverageAKID
	) AS o_PolicyCoverageAKID,
	EXP_Default.sar_section,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_section)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_section
	) AS v_sar_section,
	EXP_Default.sar_class_1_4 AS sar_class_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_class_code)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_class_code
	) AS v_sar_class_code,
	EXP_Default.sar_exposure,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_exposure)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_exposure
	) AS v_sar_exposure,
	EXP_Default.sar_sub_line,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_sub_line)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_sub_line
	) AS v_sar_sub_line,
	-- *INF*: MD5(
	-- TO_CHAR(v_PolicyCoverageAKID)   || 
	-- v_sar_sub_location_x   || 
	-- v_sar_risk_unit_group   || 
	-- v_sar_class_code_grp_x   || 
	-- v_sar_unit   || 
	-- v_sar_seq_rsk_unt_a   || 
	-- v_sar_major_peril   || 
	-- v_sar_seq_no   || 
	-- v_sar_sub_line   || 
	-- v_sar_type_exposure   || 
	-- v_sar_class_code   || 
	-- v_sar_section
	-- )
	MD5(TO_CHAR(v_PolicyCoverageAKID
		) || v_sar_sub_location_x || v_sar_risk_unit_group || v_sar_class_code_grp_x || v_sar_unit || v_sar_seq_rsk_unt_a || v_sar_major_peril || v_sar_seq_no || v_sar_sub_line || v_sar_type_exposure || v_sar_class_code || v_sar_section
	) AS v_StatisticalCoverageHashKey,
	-- *INF*: :LKP.LKP_STATISTICALCOVERAGE_STATISTICALCOVERAGEAKID(v_StatisticalCoverageHashKey)
	LKP_STATISTICALCOVERAGE_STATISTICALCOVERAGEAKID_v_StatisticalCoverageHashKey.StatisticalCoverageAKID AS v_StatisticalCoverageAKID,
	-- *INF*: IIF(
	--   ISNULL(v_StatisticalCoverageAKID),
	--   -1,
	--   v_StatisticalCoverageAKID
	-- )
	IFF(v_StatisticalCoverageAKID IS NULL,
		- 1,
		v_StatisticalCoverageAKID
	) AS StatisticalCoverageAKID,
	EXP_Default.sar_id,
	EXP_Default.sar_part_code,
	EXP_Default.sar_trans_eff_year,
	-- *INF*: TO_CHAR(sar_trans_eff_year)
	TO_CHAR(sar_trans_eff_year
	) AS v_sar_trans_eff_year,
	EXP_Default.sar_trans_eff_month,
	-- *INF*: IIF(TO_CHAR(sar_trans_eff_month) = '0','1',TO_CHAR(sar_trans_eff_month)
	-- )
	IFF(TO_CHAR(sar_trans_eff_month
		) = '0',
		'1',
		TO_CHAR(sar_trans_eff_month
		)
	) AS v_sar_trans_eff_month,
	EXP_Default.sar_trans_eff_day,
	-- *INF*: IIF(TO_CHAR(sar_trans_eff_day) ='0','1',TO_CHAR(sar_trans_eff_day))
	IFF(TO_CHAR(sar_trans_eff_day
		) = '0',
		'1',
		TO_CHAR(sar_trans_eff_day
		)
	) AS v_sar_trans_eff_day,
	-- *INF*: TO_DATE(v_sar_trans_eff_month || '/' || v_sar_trans_eff_day || '/'|| v_sar_trans_eff_year ,'MM/DD/YYYY')
	TO_DATE(v_sar_trans_eff_month || '/' || v_sar_trans_eff_day || '/' || v_sar_trans_eff_year, 'MM/DD/YYYY'
	) AS v_sar_trans_eff_date,
	v_sar_trans_eff_date AS Trans_eff_date,
	EXP_Default.sar_reinsurance_company_no,
	EXP_Default.sar_entrd_date,
	-- *INF*: TO_DATE(sar_entrd_date,'YYYYMMDD')
	TO_DATE(sar_entrd_date, 'YYYYMMDD'
	) AS v_sar_entrd_date,
	v_sar_entrd_date AS Trans_entered_date,
	EXP_Default.sar_exp_year,
	-- *INF*: TO_CHAR(sar_exp_year)
	TO_CHAR(sar_exp_year
	) AS v_sar_exp_year,
	EXP_Default.sar_exp_month,
	-- *INF*: TO_CHAR(sar_exp_month)
	TO_CHAR(sar_exp_month
	) AS v_sar_exp_month,
	EXP_Default.sar_exp_day,
	-- *INF*: TO_CHAR(sar_exp_day)
	TO_CHAR(sar_exp_day
	) AS v_sar_exp_day,
	-- *INF*: TO_DATE(v_sar_exp_month || '/' || v_sar_exp_day || '/'|| v_sar_exp_year ,'MM/DD/YYYY')
	TO_DATE(v_sar_exp_month || '/' || v_sar_exp_day || '/' || v_sar_exp_year, 'MM/DD/YYYY'
	) AS v_sar_exp_date,
	v_sar_exp_date AS Trans_expiration_date,
	EXP_Default.o_sar_transaction AS sar_transaction,
	EXP_Default.sar_premium,
	EXP_Default.sar_subpay_amt,
	EXP_Default.sar_original_prem,
	EXP_Default.sar_acct_entrd_date,
	-- *INF*: TO_DATE('01'  || sar_acct_entrd_date, 'DDYYYYMM')
	TO_DATE('01' || sar_acct_entrd_date, 'DDYYYYMM'
	) AS v_sar_acct_entrd_date,
	-- *INF*: SET_DATE_PART(
	--     SET_DATE_PART(
	--          SET_DATE_PART(
	--                      SET_DATE_PART(
	--                              LAST_DAY(v_sar_acct_entrd_date)
	--                    , 'HH', 23) 
	--            ,'MI',59)
	--     ,'SS',59)
	-- ,'MS', 000)
	DATEADD(,000-DATE_PART(,DATEADD(SECOND,59-DATE_PART(SECOND,DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,LAST_DAY(v_sar_acct_entrd_date
	)),LAST_DAY(v_sar_acct_entrd_date
	))),DATEADD(HOUR,23-DATE_PART(HOUR,LAST_DAY(v_sar_acct_entrd_date
	)),LAST_DAY(v_sar_acct_entrd_date
	)))),DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,LAST_DAY(v_sar_acct_entrd_date
	)),LAST_DAY(v_sar_acct_entrd_date
	))),DATEADD(HOUR,23-DATE_PART(HOUR,LAST_DAY(v_sar_acct_entrd_date
	)),LAST_DAY(v_sar_acct_entrd_date
	))))),DATEADD(SECOND,59-DATE_PART(SECOND,DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,LAST_DAY(v_sar_acct_entrd_date
	)),LAST_DAY(v_sar_acct_entrd_date
	))),DATEADD(HOUR,23-DATE_PART(HOUR,LAST_DAY(v_sar_acct_entrd_date
	)),LAST_DAY(v_sar_acct_entrd_date
	)))),DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,LAST_DAY(v_sar_acct_entrd_date
	)),LAST_DAY(v_sar_acct_entrd_date
	))),DATEADD(HOUR,23-DATE_PART(HOUR,LAST_DAY(v_sar_acct_entrd_date
	)),LAST_DAY(v_sar_acct_entrd_date
	))))) AS Trans_Booked_date,
	EXP_Default.sar_rsn_amend_one,
	EXP_Default.sar_rsn_amend_two,
	EXP_Default.sar_rsn_amend_three,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_rsn_amend_one  ||  sar_rsn_amend_two || sar_rsn_amend_three)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_rsn_amend_one || sar_rsn_amend_two || sar_rsn_amend_three
	) AS v_sar_rsn_amend_code,
	v_sar_rsn_amend_code AS Reason_amend_code,
	-- *INF*: Policy_Key  ||  TO_CHAR(v_PolicyCoverageAKID)
	Policy_Key || TO_CHAR(v_PolicyCoverageAKID
	) AS v_CoverageKey,
	-- *INF*: Policy_Key  ||  TO_CHAR(v_PolicyCoverageAKID)
	Policy_Key || TO_CHAR(v_PolicyCoverageAKID
	) AS CoverageKey,
	-- *INF*: MD5(
	-- TO_CHAR(v_StatisticalCoverageAKID)  ||  
	-- TO_CHAR(sar_transaction)  ||  
	-- TO_CHAR(v_sar_entrd_date)  ||  
	-- TO_CHAR(v_sar_trans_eff_date)  ||  
	-- TO_CHAR(v_sar_exp_date)  ||  
	-- TO_CHAR(v_sar_acct_entrd_date)  ||  
	-- TO_CHAR(sar_premium)  ||  
	-- TO_CHAR(sar_original_prem)  ||
	-- TO_CHAR(sar_subpay_amt)  ||  
	-- TO_CHAR(sar_ky_tax_percentage)  ||  
	-- TO_CHAR(v_sar_rsn_amend_code) 
	-- )
	MD5(TO_CHAR(v_StatisticalCoverageAKID
		) || TO_CHAR(sar_transaction
		) || TO_CHAR(v_sar_entrd_date
		) || TO_CHAR(v_sar_trans_eff_date
		) || TO_CHAR(v_sar_exp_date
		) || TO_CHAR(v_sar_acct_entrd_date
		) || TO_CHAR(sar_premium
		) || TO_CHAR(sar_original_prem
		) || TO_CHAR(sar_subpay_amt
		) || TO_CHAR(sar_ky_tax_percentage
		) || TO_CHAR(v_sar_rsn_amend_code
		)
	) AS v_PassThroughChargeTransactionHashKey,
	v_PassThroughChargeTransactionHashKey AS PassThroughChargeTransactionHashKey,
	-- *INF*: IIF(Policy_Key = v_prev_row_Pol_Key, v_prev_row_Premium_Sequence + 1,1)
	IFF(Policy_Key = v_prev_row_Pol_Key,
		v_prev_row_Premium_Sequence + 1,
		1
	) AS v_premium_sequence,
	v_premium_sequence AS PremiumLoadSequence,
	v_premium_sequence AS v_prev_row_Premium_Sequence,
	Policy_Key AS v_prev_row_Pol_Key,
	-- *INF*: IIF(v_prev_row_Statistical_Coverage_AK_ID = v_StatisticalCoverageAKID AND 
	-- v_prev_row_trans_eff_date = v_sar_trans_eff_date AND 
	-- v_prev_row_trans_entered_date = v_sar_entrd_date AND 
	-- v_prev_row_trans_exp_date = v_sar_exp_date AND 
	-- v_prev_row_trans_booked_date = v_sar_acct_entrd_date AND 
	-- v_prev_row_sar_transaction  = sar_transaction AND 
	-- v_prev_row_sar_premium = sar_premium AND 
	-- v_prev_row_sar_subpay_amt = sar_subpay_amt AND 
	-- v_prev_row_sar_original_prem = sar_original_prem AND 
	-- v_prev_row_Reason_amend_code = v_sar_rsn_amend_code, v_prev_row_Duplicate_Sequence + 1,1)
	IFF(v_prev_row_Statistical_Coverage_AK_ID = v_StatisticalCoverageAKID 
		AND v_prev_row_trans_eff_date = v_sar_trans_eff_date 
		AND v_prev_row_trans_entered_date = v_sar_entrd_date 
		AND v_prev_row_trans_exp_date = v_sar_exp_date 
		AND v_prev_row_trans_booked_date = v_sar_acct_entrd_date 
		AND v_prev_row_sar_transaction = sar_transaction 
		AND v_prev_row_sar_premium = sar_premium 
		AND v_prev_row_sar_subpay_amt = sar_subpay_amt 
		AND v_prev_row_sar_original_prem = sar_original_prem 
		AND v_prev_row_Reason_amend_code = v_sar_rsn_amend_code,
		v_prev_row_Duplicate_Sequence + 1,
		1
	) AS v_Duplicate_Sequence,
	v_Duplicate_Sequence AS Duplicate_Sequence,
	v_StatisticalCoverageAKID AS v_prev_row_Statistical_Coverage_AK_ID,
	v_sar_trans_eff_date AS v_prev_row_trans_eff_date,
	v_sar_entrd_date AS v_prev_row_trans_entered_date,
	v_sar_exp_date AS v_prev_row_trans_exp_date,
	v_sar_acct_entrd_date AS v_prev_row_trans_booked_date,
	sar_transaction AS v_prev_row_sar_transaction,
	sar_premium AS v_prev_row_sar_premium,
	sar_subpay_amt AS v_prev_row_sar_subpay_amt,
	sar_original_prem AS v_prev_row_sar_original_prem,
	v_sar_rsn_amend_code AS v_prev_row_Reason_amend_code,
	v_Duplicate_Sequence AS v_prev_row_Duplicate_Sequence,
	-1 AS o_RatingCoverageAKId,
	EXP_Default.logical_flag,
	EXP_Default.sar_ky_tax_percentage_out AS sar_ky_tax_percentage,
	EXP_Default.sar_special_use,
	EXP_Default.sar_stat_breakdown_line,
	EXP_Default.sar_user_line,
	EXP_Default.sar_rating_date_ind,
	EXP_Default.sar_code_1,
	EXP_Default.sar_code_2,
	EXP_Default.sar_code_3,
	EXP_Default.sar_code_4,
	EXP_Default.sar_code_5,
	EXP_Default.sar_code_6,
	EXP_Default.sar_code_7,
	EXP_Default.sar_code_8,
	EXP_Default.sar_code_9,
	EXP_Default.sar_code_10,
	EXP_Default.sar_code_11,
	EXP_Default.sar_code_12,
	EXP_Default.sar_code_13,
	EXP_Default.sar_code_14,
	EXP_Default.sar_code_15,
	EXP_Default.sar_audit_reinst_ind,
	EXP_Default.sar_annual_state_line
	FROM EXP_Default
	LEFT JOIN LKP_Policy_PolicyAKID_by_Key
	ON LKP_Policy_PolicyAKID_by_Key.pol_key = EXP_Default.Policy_Key
	LEFT JOIN LKP_RISKLOCATION_RISKLOCATIONAKID LKP_RISKLOCATION_RISKLOCATIONAKID_pol_ak_id_v_RiskLocation_Unit_v_sar_loc_prov_territory_v_sar_state_v_sar_zip_postal_code_v_Tax_Location
	ON LKP_RISKLOCATION_RISKLOCATIONAKID_pol_ak_id_v_RiskLocation_Unit_v_sar_loc_prov_territory_v_sar_state_v_sar_zip_postal_code_v_Tax_Location.PolicyAKID = pol_ak_id
	AND LKP_RISKLOCATION_RISKLOCATIONAKID_pol_ak_id_v_RiskLocation_Unit_v_sar_loc_prov_territory_v_sar_state_v_sar_zip_postal_code_v_Tax_Location.LocationUnitNumber = v_RiskLocation_Unit
	AND LKP_RISKLOCATION_RISKLOCATIONAKID_pol_ak_id_v_RiskLocation_Unit_v_sar_loc_prov_territory_v_sar_state_v_sar_zip_postal_code_v_Tax_Location.RiskTerritory = v_sar_loc_prov_territory
	AND LKP_RISKLOCATION_RISKLOCATIONAKID_pol_ak_id_v_RiskLocation_Unit_v_sar_loc_prov_territory_v_sar_state_v_sar_zip_postal_code_v_Tax_Location.StateProvinceCode = v_sar_state
	AND LKP_RISKLOCATION_RISKLOCATIONAKID_pol_ak_id_v_RiskLocation_Unit_v_sar_loc_prov_territory_v_sar_state_v_sar_zip_postal_code_v_Tax_Location.ZipPostalCode = v_sar_zip_postal_code
	AND LKP_RISKLOCATION_RISKLOCATIONAKID_pol_ak_id_v_RiskLocation_Unit_v_sar_loc_prov_territory_v_sar_state_v_sar_zip_postal_code_v_Tax_Location.TaxLocation = v_Tax_Location

	LEFT JOIN LKP_POLICYCOVERAGE_POLICYCOVERAGEAKID LKP_POLICYCOVERAGE_POLICYCOVERAGEAKID_v_PolicyCoverageHashKey
	ON LKP_POLICYCOVERAGE_POLICYCOVERAGEAKID_v_PolicyCoverageHashKey.PolicyCoverageHashKey = v_PolicyCoverageHashKey

	LEFT JOIN LKP_STATISTICALCOVERAGE_STATISTICALCOVERAGEAKID LKP_STATISTICALCOVERAGE_STATISTICALCOVERAGEAKID_v_StatisticalCoverageHashKey
	ON LKP_STATISTICALCOVERAGE_STATISTICALCOVERAGEAKID_v_StatisticalCoverageHashKey.StatisticalCoverageHashKey = v_StatisticalCoverageHashKey

),
LKP_PassThroughChargeTransaction AS (
	SELECT
	PassThroughChargeTransactionAKID,
	PassThroughChargeTransactionHashKey,
	DuplicateSequence
	FROM (
		SELECT PassThroughChargetransactionAKID as PassThroughChargetransactionAKID,
		                  PassThroughChargetransactionHashKey as PassThroughChargetransactionHashKey,
					DuplicateSequence as DuplicateSequence
		FROM	@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation LOC, 
					@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Policy POL,
					@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage POLCOV,
					@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage COVDET,
		                   @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PassThroughChargetransaction PTPREM
		WHERE	LOC.PolicyAKID = POL.pol_ak_id
				AND LOC.RiskLocationAKID = POLCOV.RiskLocationAKID
				AND POLCOV.PolicyCoverageAKID = COVDET.PolicyCoverageAKID
		             AND COVDET.StatisticalCoverageAKID = PTPREM.StatisticalCoverageAKID
				AND POL.crrnt_snpsht_flag = 1 AND LOC.CurrentSnapshotFlag =1
				AND COVDET.CurrentSnapshotFlag =1 
				AND POLCOV.CurrentSnapshotFlag =1 
				AND PTPREM.CurrentSnapshotFlag =1 
		            AND PTPREM.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
				AND POL.pol_key  IN (SELECT DISTINCT RTRIM(pif_symbol) + pif_policy_number + pif_module FROM  
									@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME_4514}
									WHERE logical_flag IN ('0','1','2','3') 
									@{pipeline().parameters.WHERE_CLAUSE} )
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PassThroughChargeTransactionHashKey,DuplicateSequence ORDER BY PassThroughChargeTransactionAKID DESC) = 1
),
LKP_SupLGTLineOfInsurance AS (
	SELECT
	SupLGTLineOfInsuranceId,
	LGTLineOfInsuranceCode
	FROM (
		SELECT 
			SupLGTLineOfInsuranceId,
			LGTLineOfInsuranceCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupLGTLineOfInsurance
		WHERE SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}' and CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY LGTLineOfInsuranceCode ORDER BY SupLGTLineOfInsuranceId) = 1
),
LKP_StatisticalCoverage_MajorPerilCode AS (
	SELECT
	MajorPerilCode,
	StatisticalCoverageAKID
	FROM (
		SELECT StatisticalCoverageAKID AS StatisticalCoverageAKID,
		LTRIM(RTRIM(MajorPerilCode)) AS MajorPerilCode
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage
		WHERE CurrentSnapshotFlag=1 AND SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StatisticalCoverageAKID ORDER BY MajorPerilCode) = 1
),
LKP_SupPassThroughChargeType AS (
	SELECT
	SupPassThroughChargeTypeID,
	MajorPerilCode
	FROM (
		SELECT 
			SupPassThroughChargeTypeID,
			MajorPerilCode
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupPassThroughChargeType
		WHERE CurrentSnapshotFlag=1 AND SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY MajorPerilCode ORDER BY SupPassThroughChargeTypeID) = 1
),
LKP_sup_premium_transaction_code AS (
	SELECT
	sup_prem_trans_code_id,
	prem_trans_code
	FROM (
		SELECT 
			sup_prem_trans_code_id,
			prem_trans_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_premium_transaction_code
		WHERE source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY prem_trans_code ORDER BY sup_prem_trans_code_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_PassThroughChargeTransaction.PassThroughChargeTransactionAKID,
	EXP_Values.sar_exposure AS TotalAnnualPremiumSubjectToTax,
	EXP_Values.StatisticalCoverageAKID,
	0 AS logicalIndicator,
	EXP_Values.PassThroughChargeTransactionHashKey,
	EXP_Values.CoverageKey,
	'1' AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'
	) AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	SYSDATE AS CreateDate,
	SYSDATE AS ModifiedDate,
	EXP_Values.Trans_eff_date,
	EXP_Values.Trans_entered_date,
	EXP_Values.Trans_expiration_date,
	EXP_Values.sar_transaction,
	EXP_Values.sar_premium,
	EXP_Values.sar_original_prem,
	EXP_Values.sar_subpay_amt,
	EXP_Values.Trans_Booked_date,
	EXP_Values.Reason_amend_code,
	EXP_Values.PremiumLoadSequence,
	EXP_Values.Duplicate_Sequence,
	EXP_Values.logical_flag,
	-- *INF*: TO_INTEGER(logical_flag)
	CAST(logical_flag AS INTEGER) AS logical_flag_out,
	EXP_Values.sar_ky_tax_percentage,
	LKP_sup_premium_transaction_code.sup_prem_trans_code_id,
	EXP_Values.o_RiskLocationAKID AS RiskLocationAKID,
	EXP_Values.pol_ak_id,
	LKP_SupLGTLineOfInsurance.SupLGTLineOfInsuranceId,
	EXP_Values.o_PolicyCoverageAKID AS PolicyCoverageAKID,
	LKP_Policy_PolicyAKID_by_Key.SupSurchargeExemptID,
	LKP_SupPassThroughChargeType.SupPassThroughChargeTypeID,
	EXP_Values.o_RatingCoverageAKId AS RatingCoverageAKId,
	EXP_Values.sar_special_use,
	EXP_Values.sar_stat_breakdown_line,
	EXP_Values.sar_user_line,
	EXP_Values.sar_rating_date_ind,
	EXP_Values.sar_code_1,
	EXP_Values.sar_code_2,
	EXP_Values.sar_code_3,
	EXP_Values.sar_code_4,
	EXP_Values.sar_code_5,
	EXP_Values.sar_code_6,
	EXP_Values.sar_code_7,
	EXP_Values.sar_code_8,
	EXP_Values.sar_code_9,
	EXP_Values.sar_code_10,
	EXP_Values.sar_code_11,
	EXP_Values.sar_code_12,
	EXP_Values.sar_code_13,
	EXP_Values.sar_code_14,
	EXP_Values.sar_code_15,
	EXP_Values.sar_audit_reinst_ind,
	EXP_Values.sar_annual_state_line
	FROM EXP_Values
	LEFT JOIN LKP_PassThroughChargeTransaction
	ON LKP_PassThroughChargeTransaction.PassThroughChargeTransactionHashKey = EXP_Values.PassThroughChargeTransactionHashKey AND LKP_PassThroughChargeTransaction.DuplicateSequence = EXP_Values.Duplicate_Sequence
	LEFT JOIN LKP_Policy_PolicyAKID_by_Key
	ON LKP_Policy_PolicyAKID_by_Key.pol_key = EXP_Default.Policy_Key
	LEFT JOIN LKP_SupLGTLineOfInsurance
	ON LKP_SupLGTLineOfInsurance.LGTLineOfInsuranceCode = EXP_Values.o_sar_insurance_line
	LEFT JOIN LKP_SupPassThroughChargeType
	ON LKP_SupPassThroughChargeType.MajorPerilCode = LKP_StatisticalCoverage_MajorPerilCode.MajorPerilCode
	LEFT JOIN LKP_sup_premium_transaction_code
	ON LKP_sup_premium_transaction_code.prem_trans_code = EXP_Values.sar_transaction
),
FIL_Insert_rows AS (
	SELECT
	PassThroughChargeTransactionAKID, 
	TotalAnnualPremiumSubjectToTax, 
	StatisticalCoverageAKID, 
	logical_flag_out AS logicalIndicator, 
	PassThroughChargeTransactionHashKey, 
	CoverageKey, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemID, 
	CreateDate, 
	ModifiedDate, 
	Trans_eff_date, 
	Trans_entered_date, 
	Trans_expiration_date, 
	sar_transaction, 
	sar_premium, 
	sar_original_prem, 
	sar_subpay_amt, 
	Trans_Booked_date, 
	Reason_amend_code, 
	PremiumLoadSequence, 
	Duplicate_Sequence, 
	sar_ky_tax_percentage, 
	sup_prem_trans_code_id, 
	RiskLocationAKID, 
	pol_ak_id, 
	SupLGTLineOfInsuranceId, 
	PolicyCoverageAKID, 
	SupSurchargeExemptID, 
	SupPassThroughChargeTypeID, 
	RatingCoverageAKId, 
	sar_special_use, 
	sar_stat_breakdown_line, 
	sar_user_line, 
	sar_rating_date_ind, 
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
	sar_audit_reinst_ind, 
	sar_annual_state_line
	FROM EXP_Detect_Changes
	WHERE IIF(ISNULL(PassThroughChargeTransactionAKID), TRUE, FALSE)
),
SEQ_PassThroughChargeTransactionAKID AS (
	CREATE SEQUENCE SEQ_PassThroughChargeTransactionAKID
	START = 0
	INCREMENT = 1;
),
EXP_Detemine_AK_ID AS (
	SELECT
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreateDate,
	ModifiedDate,
	logicalIndicator,
	1 AS LogicalDeleteFlag,
	PassThroughChargeTransactionAKID,
	SEQ_PassThroughChargeTransactionAKID.NEXTVAL,
	PassThroughChargeTransactionHashKey,
	PremiumLoadSequence,
	Duplicate_Sequence,
	-- *INF*: IIF(ISNULL(PassThroughChargeTransactionAKID), NEXTVAL, PassThroughChargeTransactionAKID)
	IFF(PassThroughChargeTransactionAKID IS NULL,
		NEXTVAL,
		PassThroughChargeTransactionAKID
	) AS PassThroughChargeTransactionAKID_Out,
	StatisticalCoverageAKID,
	CoverageKey,
	sar_transaction,
	Trans_entered_date,
	Trans_eff_date,
	Trans_expiration_date,
	Trans_Booked_date,
	sar_premium,
	sar_original_prem,
	sar_subpay_amt,
	sar_ky_tax_percentage,
	Reason_amend_code,
	sup_prem_trans_code_id,
	RiskLocationAKID,
	pol_ak_id,
	SupLGTLineOfInsuranceId,
	PolicyCoverageAKID,
	SupSurchargeExemptID,
	SupPassThroughChargeTypeID,
	TotalAnnualPremiumSubjectToTax,
	'N/A' AS DCTTaxCode,
	'N/A' AS OffsetOnsetCode,
	1 AS LoadSequence,
	'N/A' AS NegateRestateCode,
	RatingCoverageAKId,
	sar_special_use,
	sar_stat_breakdown_line,
	sar_user_line,
	sar_rating_date_ind,
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
	sar_audit_reinst_ind,
	sar_annual_state_line
	FROM FIL_Insert_rows
),
EXP_Pre_BureauCodeLkp AS (
	SELECT
	PassThroughChargeTransactionAKID_Out AS PassThroughChargeTransactionAKID,
	logicalIndicator,
	CoverageKey,
	CurrentSnapshotFlag,
	AuditID,
	-1 AS v_PremiumTransactionAKID,
	v_PremiumTransactionAKID AS PremiumTransactionAKID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreateDate,
	ModifiedDate,
	sar_stat_breakdown_line,
	sar_user_line,
	sar_code_1 AS BureauCode1,
	sar_code_2 AS BureauCode2,
	sar_code_3 AS BureauCode3,
	sar_code_4 AS BureauCode4,
	sar_code_5 AS BureauCode5,
	sar_code_6 AS BureauCode6,
	sar_code_7 AS BureauCode7,
	sar_code_8 AS BureauCode8,
	sar_code_9 AS BureauCode9,
	sar_code_10 AS BureauCode10,
	sar_code_11 AS BureauCode11,
	sar_code_12 AS BureauCode12,
	sar_code_13 AS BureauCode13,
	sar_code_14 AS BureauCode14,
	sar_code_15 AS BureauCode15,
	sar_special_use AS BureauSpecialUseCode,
	sar_annual_state_line AS PMSAnnualStatementLine,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(PMSAnnualStatementLine)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(PMSAnnualStatementLine
	) AS v_PMSAnnualStatementLine,
	v_PMSAnnualStatementLine AS PMSAnnualStatementLine_out,
	sar_rating_date_ind AS RatingDateIndicator,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(RatingDateIndicator)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(RatingDateIndicator
	) AS v_RatingDateIndicator,
	v_RatingDateIndicator AS RatingDateIndicator_out,
	sar_stat_breakdown_line || sar_user_line AS v_BureauStatisticalUserLine,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_BureauStatisticalUserLine)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_BureauStatisticalUserLine
	) AS BureauStatisticalUserLine,
	sar_audit_reinst_ind AS AuditReinstatementIndicator,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(AuditReinstatementIndicator)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(AuditReinstatementIndicator
	) AS v_AuditReinstatementIndicator,
	v_AuditReinstatementIndicator AS AuditReinstatementIndicator_out,
	-- *INF*: MD5(
	-- TO_CHAR(v_PremiumTransactionAKID) || 
	-- TO_CHAR(PassThroughChargeTransactionAKID)  ||  
	-- BureauCode1  ||  
	-- BureauCode2  ||  
	-- BureauCode3  ||  
	-- BureauCode4  ||  
	-- BureauCode5  ||  
	-- BureauCode6  ||  
	-- BureauCode7  ||  
	-- BureauCode8  ||  
	-- BureauCode9  ||  
	-- BureauCode10  ||  
	-- BureauCode11  ||  
	-- BureauCode12  ||  
	-- BureauCode13  ||  
	-- BureauCode14  ||  
	-- BureauCode15  ||  
	-- BureauSpecialUseCode  ||  
	-- PMSAnnualStatementLine  ||  
	-- RatingDateIndicator  ||  
	-- v_BureauStatisticalUserLine  ||
	-- AuditReinstatementIndicator )
	-- 
	MD5(TO_CHAR(v_PremiumTransactionAKID
		) || TO_CHAR(PassThroughChargeTransactionAKID
		) || BureauCode1 || BureauCode2 || BureauCode3 || BureauCode4 || BureauCode5 || BureauCode6 || BureauCode7 || BureauCode8 || BureauCode9 || BureauCode10 || BureauCode11 || BureauCode12 || BureauCode13 || BureauCode14 || BureauCode15 || BureauSpecialUseCode || PMSAnnualStatementLine || RatingDateIndicator || v_BureauStatisticalUserLine || AuditReinstatementIndicator
	) AS v_BureauStatisticalCodeHashKey,
	v_BureauStatisticalCodeHashKey AS BureauStatisticalCodeHashKey
	FROM EXP_Detemine_AK_ID
),
LKP_BureauStatisticalCode AS (
	SELECT
	BureauStatisticalCodeAKID,
	BureauStatisticalCodeHashKey
	FROM (
		SELECT BureauStatisticalCodeAKID as BureauStatisticalCodeAKID,
		                  BureauStatisticalCodeHashKey as BureauStatisticalCodeHashKey
		FROM	@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation LOC, 
					@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Policy POL,
					@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage POLCOV,
					@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage STATCOV,
		                   @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PassThroughChargeTransaction PT,
		                   @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.BureauStatisticalCode STATCD
		WHERE	LOC.PolicyAKID = POL.pol_ak_id
				AND LOC.RiskLocationAKID = POLCOV.RiskLocationAKID
				AND POLCOV.PolicyCoverageAKID = STATCOV.PolicyCoverageAKID
		             AND STATCOV.StatisticalCoverageAKID = PT.StatisticalCoverageAKID
		             AND PT.PassThroughChargeTransactionAKID = STATCD.PassThroughChargeTransactionAKID
				AND POL.crrnt_snpsht_flag = 1 AND LOC.CurrentSnapshotFlag =1
				AND STATCOV.CurrentSnapshotFlag =1 
				AND POLCOV.CurrentSnapshotFlag =1 
				AND PT.CurrentSnapshotFlag =1 
		             AND STATCD.CurrentSnapshotFlag =1 
		            AND STATCD.PassThroughChargeTransactionAKID <> -1
		            AND STATCD.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
				AND POL.pol_key  IN (SELECT DISTINCT RTRIM(pif_symbol) + pif_policy_number + pif_module FROM  
									@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME_4514}
									WHERE logical_flag IN ('0','1','2','3') 
									@{pipeline().parameters.WHERE_CLAUSE} )
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY BureauStatisticalCodeHashKey ORDER BY BureauStatisticalCodeAKID DESC) = 1
),
FIL_Insert_BureauStatisticalCode_Rows AS (
	SELECT
	EXP_Pre_BureauCodeLkp.PremiumTransactionAKID, 
	LKP_BureauStatisticalCode.BureauStatisticalCodeAKID, 
	EXP_Pre_BureauCodeLkp.CurrentSnapshotFlag, 
	EXP_Pre_BureauCodeLkp.AuditID, 
	EXP_Pre_BureauCodeLkp.EffectiveDate, 
	EXP_Pre_BureauCodeLkp.ExpirationDate, 
	EXP_Pre_BureauCodeLkp.SourceSystemID, 
	EXP_Pre_BureauCodeLkp.CreateDate, 
	EXP_Pre_BureauCodeLkp.ModifiedDate, 
	EXP_Pre_BureauCodeLkp.logicalIndicator, 
	EXP_Pre_BureauCodeLkp.BureauStatisticalCodeHashKey, 
	EXP_Pre_BureauCodeLkp.PassThroughChargeTransactionAKID, 
	EXP_Pre_BureauCodeLkp.BureauCode1, 
	EXP_Pre_BureauCodeLkp.BureauCode2, 
	EXP_Pre_BureauCodeLkp.BureauCode3, 
	EXP_Pre_BureauCodeLkp.BureauCode4, 
	EXP_Pre_BureauCodeLkp.BureauCode5, 
	EXP_Pre_BureauCodeLkp.BureauCode6, 
	EXP_Pre_BureauCodeLkp.BureauCode7, 
	EXP_Pre_BureauCodeLkp.BureauCode8, 
	EXP_Pre_BureauCodeLkp.BureauCode9, 
	EXP_Pre_BureauCodeLkp.BureauCode10, 
	EXP_Pre_BureauCodeLkp.BureauCode11, 
	EXP_Pre_BureauCodeLkp.BureauCode12, 
	EXP_Pre_BureauCodeLkp.BureauCode13, 
	EXP_Pre_BureauCodeLkp.BureauCode14, 
	EXP_Pre_BureauCodeLkp.BureauCode15, 
	EXP_Pre_BureauCodeLkp.BureauSpecialUseCode, 
	EXP_Pre_BureauCodeLkp.PMSAnnualStatementLine_out AS PMSAnnualStatementLine, 
	EXP_Pre_BureauCodeLkp.RatingDateIndicator_out AS RatingDateIndicator, 
	EXP_Pre_BureauCodeLkp.BureauStatisticalUserLine, 
	EXP_Pre_BureauCodeLkp.AuditReinstatementIndicator_out AS AuditReinstatementIndicator
	FROM EXP_Pre_BureauCodeLkp
	LEFT JOIN LKP_BureauStatisticalCode
	ON LKP_BureauStatisticalCode.BureauStatisticalCodeHashKey = EXP_Pre_BureauCodeLkp.BureauStatisticalCodeHashKey
	WHERE IIF(ISNULL(BureauStatisticalCodeAKID), TRUE, FALSE)
),
SEQ_BureauStatisticalCode_AKID AS (
	CREATE SEQUENCE SEQ_BureauStatisticalCode_AKID
	START = 0
	INCREMENT = 1;
),
EXP_Determine_BureauCode_AKID AS (
	SELECT
	BureauStatisticalCodeAKID,
	SEQ_BureauStatisticalCode_AKID.NEXTVAL,
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreateDate,
	ModifiedDate,
	logicalIndicator,
	1 AS LogicalDeleteFlag,
	BureauStatisticalCodeHashKey,
	-- *INF*: IIF(ISNULL(BureauStatisticalCodeAKID), NEXTVAL, BureauStatisticalCodeAKID)
	IFF(BureauStatisticalCodeAKID IS NULL,
		NEXTVAL,
		BureauStatisticalCodeAKID
	) AS BureauStatisticalCodeAKID_Out,
	PremiumTransactionAKID,
	PassThroughChargeTransactionAKID,
	BureauCode1,
	BureauCode2,
	BureauCode3,
	BureauCode4,
	BureauCode5,
	BureauCode6,
	BureauCode7,
	BureauCode8,
	BureauCode9,
	BureauCode10,
	BureauCode11,
	BureauCode12,
	BureauCode13,
	BureauCode14,
	BureauCode15,
	BureauSpecialUseCode,
	PMSAnnualStatementLine,
	RatingDateIndicator,
	BureauStatisticalUserLine,
	AuditReinstatementIndicator
	FROM FIL_Insert_BureauStatisticalCode_Rows
),
TGT_BureauStatisticalCode AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.BureauStatisticalCode
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, LogicalIndicator, LogicalDeleteFlag, BureauStatisticalCodeHashKey, BureauStatisticalCodeAKID, PremiumTransactionAKID, PassThroughChargeTransactionAKID, BureauCode1, BureauCode2, BureauCode3, BureauCode4, BureauCode5, BureauCode6, BureauCode7, BureauCode8, BureauCode9, BureauCode10, BureauCode11, BureauCode12, BureauCode13, BureauCode14, BureauCode15, BureauSpecialUseCode, PMSAnnualStatementLine, RatingDateIndicator, BureauStatisticalUserLine, AuditReinstatementIndicator)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CreateDate AS CREATEDDATE, 
	MODIFIEDDATE, 
	logicalIndicator AS LOGICALINDICATOR, 
	LOGICALDELETEFLAG, 
	BUREAUSTATISTICALCODEHASHKEY, 
	BureauStatisticalCodeAKID_Out AS BUREAUSTATISTICALCODEAKID, 
	PREMIUMTRANSACTIONAKID, 
	PASSTHROUGHCHARGETRANSACTIONAKID, 
	BUREAUCODE1, 
	BUREAUCODE2, 
	BUREAUCODE3, 
	BUREAUCODE4, 
	BUREAUCODE5, 
	BUREAUCODE6, 
	BUREAUCODE7, 
	BUREAUCODE8, 
	BUREAUCODE9, 
	BUREAUCODE10, 
	BUREAUCODE11, 
	BUREAUCODE12, 
	BUREAUCODE13, 
	BUREAUCODE14, 
	BUREAUCODE15, 
	BUREAUSPECIALUSECODE, 
	PMSANNUALSTATEMENTLINE, 
	RATINGDATEINDICATOR, 
	BUREAUSTATISTICALUSERLINE, 
	AUDITREINSTATEMENTINDICATOR
	FROM EXP_Determine_BureauCode_AKID
),
TGT_PassThroughChargeTransaction AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PassThroughChargeTransaction
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, LogicalIndicator, LogicalDeleteFlag, DuplicateSequence, PassThroughChargeTransactionHashKey, PassThroughChargeTransactionAKID, StatisticalCoverageAKID, PassThroughChargeTransactionCode, PassThroughChargeTransactionEnteredDate, PassThroughChargeTransactionEffectiveDate, PassThroughChargeTransactionExpirationDate, PassThroughChargeTransactionBookedDate, PassThroughChargeTransactionAmount, FullTermPremium, FullTaxAmount, TaxPercentageRate, ReasonAmendedCode, PassThroughChargeTransactionCodeId, RiskLocationAKID, PolicyAKID, SupLGTLineOfInsuranceID, PolicyCoverageAKID, SupSurchargeExemptID, SupPassThroughChargeTypeID, TotalAnnualPremiumSubjectToTax, DCTTaxCode, OffsetOnsetCode, LoadSequence, NegateRestateCode)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CreateDate AS CREATEDDATE, 
	MODIFIEDDATE, 
	logicalIndicator AS LOGICALINDICATOR, 
	LOGICALDELETEFLAG, 
	Duplicate_Sequence AS DUPLICATESEQUENCE, 
	PASSTHROUGHCHARGETRANSACTIONHASHKEY, 
	PassThroughChargeTransactionAKID_Out AS PASSTHROUGHCHARGETRANSACTIONAKID, 
	STATISTICALCOVERAGEAKID, 
	sar_transaction AS PASSTHROUGHCHARGETRANSACTIONCODE, 
	Trans_entered_date AS PASSTHROUGHCHARGETRANSACTIONENTEREDDATE, 
	Trans_eff_date AS PASSTHROUGHCHARGETRANSACTIONEFFECTIVEDATE, 
	Trans_expiration_date AS PASSTHROUGHCHARGETRANSACTIONEXPIRATIONDATE, 
	Trans_Booked_date AS PASSTHROUGHCHARGETRANSACTIONBOOKEDDATE, 
	sar_premium AS PASSTHROUGHCHARGETRANSACTIONAMOUNT, 
	sar_original_prem AS FULLTERMPREMIUM, 
	sar_subpay_amt AS FULLTAXAMOUNT, 
	sar_ky_tax_percentage AS TAXPERCENTAGERATE, 
	Reason_amend_code AS REASONAMENDEDCODE, 
	sup_prem_trans_code_id AS PASSTHROUGHCHARGETRANSACTIONCODEID, 
	RISKLOCATIONAKID, 
	pol_ak_id AS POLICYAKID, 
	SupLGTLineOfInsuranceId AS SUPLGTLINEOFINSURANCEID, 
	POLICYCOVERAGEAKID, 
	SUPSURCHARGEEXEMPTID, 
	SUPPASSTHROUGHCHARGETYPEID, 
	TOTALANNUALPREMIUMSUBJECTTOTAX, 
	DCTTAXCODE, 
	OFFSETONSETCODE, 
	LOADSEQUENCE, 
	NEGATERESTATECODE
	FROM EXP_Detemine_AK_ID
),
WITH
SQ_pif_40_stage AS (
	SELECT DISTINCT pif_40_stage.pif_symbol,
	                pif_40_stage.pif_policy_number,
	                pif_40_stage.pif_module,
	                pif_40_stage.reins_section_code,
	                pif_40_stage.reins_insurance_line,
	                pif_40_stage.reins_location_number,
	                pif_40_stage.reins_sub_location_number,
	                pif_40_stage.reins_risk_unit_group,
	                pif_40_stage.reins_seq_rsk_unt_grp,
	                pif_40_stage.reins_location,
	                pif_40_stage.reins_risk_sequence,
	                pif_40_stage.reins_company_no,
	                pif_40_stage.reins_eff_year,
	                pif_40_stage.reins_eff_month,
	                pif_40_stage.reins_eff_day,
	                pif_40_stage.reins_percent_prem_ceded,
	                pif_40_stage.reins_percent_loss_ceded,
	                pif_40_stage.reins_percent_fac_comm,
	                pif_40_stage.reins_exp_year,
	                pif_40_stage.reins_exp_month,
	                pif_40_stage.reins_exp_day,
	                pif_40_stage.reins_type,
	                pif_40_stage.reins_ent_year,
	                pif_40_stage.reins_ent_month,
	                pif_40_stage.reins_ent_day,
	                pif_40_stage.reins_excess_amt,
	                pif_40_stage.reins_occur_limit,
	                pif_40_stage.reins_aggregate_limit
	FROM   pif_40_stage
	--WHERE ISNULL(pif_40_stage.inf_action, 'N/A') <> 'A'
),
EXP_Value AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module,
	pif_symbol || pif_policy_number || pif_module AS pol_Key,
	reins_insurance_line AS in_reins_insurance_line,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_reins_insurance_line)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_reins_insurance_line
	) AS reins_insurance_line,
	reins_location_number AS in_reins_location_number,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(TO_CHAR(in_reins_location_number))
	:UDF.DEFAULT_VALUE_FOR_STRINGS(TO_CHAR(in_reins_location_number
		)
	) AS reins_location_number,
	reins_sub_location_number AS in_reins_sub_location_number,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(TO_CHAR(in_reins_sub_location_number))
	:UDF.DEFAULT_VALUE_FOR_STRINGS(TO_CHAR(in_reins_sub_location_number
		)
	) AS reins_sub_location_number,
	reins_risk_unit_group AS in_reins_risk_unit_group,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(TO_CHAR(in_reins_risk_unit_group))
	:UDF.DEFAULT_VALUE_FOR_STRINGS(TO_CHAR(in_reins_risk_unit_group
		)
	) AS reins_risk_unit_group,
	reins_seq_rsk_unt_grp AS in_reins_seq_rsk_unt_grp,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(TO_CHAR(in_reins_seq_rsk_unt_grp))
	:UDF.DEFAULT_VALUE_FOR_STRINGS(TO_CHAR(in_reins_seq_rsk_unt_grp
		)
	) AS reins_seq_rsk_unt_grp,
	reins_location AS in_reins_location,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_reins_location)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_reins_location
	) AS reins_location,
	reins_risk_sequence AS in_reins_risk_sequence,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(TO_CHAR(in_reins_risk_sequence))
	:UDF.DEFAULT_VALUE_FOR_STRINGS(TO_CHAR(in_reins_risk_sequence
		)
	) AS reins_risk_sequence,
	reins_section_code AS in_reins_section_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_reins_section_code)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_reins_section_code
	) AS reins_section_code,
	reins_company_no AS in_reins_company_no,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_reins_company_no)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_reins_company_no
	) AS reins_company_no,
	reins_eff_year,
	reins_eff_month,
	reins_eff_day,
	-- *INF*: IIF ( LENGTH(reins_eff_month) = 1, '0' || reins_eff_month, reins_eff_month)
	-- || '/'  || 
	-- IIF ( LENGTH(reins_eff_day ) = 1, '0' || reins_eff_day, reins_eff_day)
	-- ||  '/' || 
	-- reins_eff_year
	IFF(LENGTH(reins_eff_month
		) = 1,
		'0' || reins_eff_month,
		reins_eff_month
	) || '/' || IFF(LENGTH(reins_eff_day
		) = 1,
		'0' || reins_eff_day,
		reins_eff_day
	) || '/' || reins_eff_year AS v_reins_eff_date,
	-- *INF*: IIF ((ISNULL(reins_eff_month) OR ISNULL(reins_eff_day) OR ISNULL(reins_eff_year))
	-- , TO_DATE ('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	-- ,TO_DATE(v_reins_eff_date , 'MM/DD/YYYY')
	-- )
	IFF(( reins_eff_month IS NULL 
			OR reins_eff_day IS NULL 
			OR reins_eff_year IS NULL 
		),
		TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		TO_DATE(v_reins_eff_date, 'MM/DD/YYYY'
		)
	) AS reins_eff_date,
	reins_exp_year,
	reins_exp_month,
	reins_exp_day,
	-- *INF*: IIF ( LENGTH(reins_exp_month) = 1, '0' || reins_exp_month, reins_exp_month)
	-- ||  '/' || 
	-- IIF ( LENGTH(reins_exp_day ) = 1, '0' || reins_exp_day, reins_exp_day)
	-- ||  '/' || 
	-- reins_exp_year
	IFF(LENGTH(reins_exp_month
		) = 1,
		'0' || reins_exp_month,
		reins_exp_month
	) || '/' || IFF(LENGTH(reins_exp_day
		) = 1,
		'0' || reins_exp_day,
		reins_exp_day
	) || '/' || reins_exp_year AS v_reins_exp_date,
	-- *INF*: IIF ((ISNULL(reins_exp_month) OR ISNULL(reins_exp_day) OR ISNULL(reins_exp_year))
	-- , TO_DATE ('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	-- ,TO_DATE(v_reins_exp_date , 'MM/DD/YYYY')
	-- )
	IFF(( reins_exp_month IS NULL 
			OR reins_exp_day IS NULL 
			OR reins_exp_year IS NULL 
		),
		TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		TO_DATE(v_reins_exp_date, 'MM/DD/YYYY'
		)
	) AS reins_exp_date,
	reins_ent_year,
	reins_ent_month,
	reins_ent_day,
	-- *INF*: IIF ( LENGTH(reins_ent_month) = 1, '0' || reins_ent_month, reins_ent_month)
	-- ||  '/' || 
	-- IIF ( LENGTH(reins_ent_day ) = 1, '0' || reins_ent_day, reins_ent_day)
	-- ||  '/' || 
	-- reins_ent_year
	IFF(LENGTH(reins_ent_month
		) = 1,
		'0' || reins_ent_month,
		reins_ent_month
	) || '/' || IFF(LENGTH(reins_ent_day
		) = 1,
		'0' || reins_ent_day,
		reins_ent_day
	) || '/' || reins_ent_year AS v_reins_ent_date,
	-- *INF*: IIF ((ISNULL(reins_ent_month) OR ISNULL(reins_ent_day) OR ISNULL(reins_ent_year))
	-- , TO_DATE ('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	-- ,TO_DATE(v_reins_ent_date , 'MM/DD/YYYY')
	-- )
	IFF(( reins_ent_month IS NULL 
			OR reins_ent_day IS NULL 
			OR reins_ent_year IS NULL 
		),
		TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		TO_DATE(v_reins_ent_date, 'MM/DD/YYYY'
		)
	) AS reins_ent_date,
	reins_type AS in_reins_type,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_reins_type)
	-- 
	-- --'N/A' (backed out this change for April release)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_reins_type
	) AS reins_type,
	reins_percent_prem_ceded,
	reins_percent_loss_ceded,
	reins_percent_fac_comm,
	reins_excess_amt AS in_reins_excess_amt,
	-- *INF*: IIF(ISNULL(in_reins_excess_amt),00000000,in_reins_excess_amt)
	-- 
	IFF(in_reins_excess_amt IS NULL,
		00000000,
		in_reins_excess_amt
	) AS reins_excess_amt,
	reins_occur_limit AS in_reins_occur_limit,
	-- *INF*: IIF(ISNULL(in_reins_occur_limit),00000000,in_reins_occur_limit)
	-- 
	IFF(in_reins_occur_limit IS NULL,
		00000000,
		in_reins_occur_limit
	) AS reins_occur_limit,
	reins_aggregate_limit AS in_reins_aggregate_limit,
	-- *INF*: IIF(ISNULL(in_reins_aggregate_limit),00000000,in_reins_aggregate_limit)
	IFF(in_reins_aggregate_limit IS NULL,
		00000000,
		in_reins_aggregate_limit
	) AS reins_aggregate_limit,
	-- *INF*: DECODE(in_reins_type, '2', 'Facultative', '3', 'Treaty', 'N/A')
	DECODE(in_reins_type,
		'2', 'Facultative',
		'3', 'Treaty',
		'N/A'
	) AS ReinsuranceMethod
	FROM SQ_pif_40_stage
),
LKP_Policy AS (
	SELECT
	pol_ak_id,
	pol_key
	FROM (
		SELECT 
			pol_ak_id,
			pol_key
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy
		WHERE crrnt_snpsht_flag = 1 and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_ak_id) = 1
),
LKP_SupReinsuranceMaster AS (
	SELECT
	ReinsuranceMasterReinsuranceCompanyName,
	ReinsuranceMasterReinsuranceCompanyNumber
	FROM (
		SELECT 
			ReinsuranceMasterReinsuranceCompanyName,
			ReinsuranceMasterReinsuranceCompanyNumber
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupReinsuranceMaster
		WHERE CurrentSnapshotFlag='1' and SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ReinsuranceMasterReinsuranceCompanyNumber ORDER BY ReinsuranceMasterReinsuranceCompanyName) = 1
),
LKP_sup_insurance_line AS (
	SELECT
	sup_ins_line_id,
	ins_line_code
	FROM (
		SELECT 
			sup_ins_line_id,
			ins_line_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_insurance_line
		WHERE crrnt_snpsht_flag='1' and source_sys_id='@{pipeline().parameters.MERGED_SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ins_line_code ORDER BY sup_ins_line_id) = 1
),
LKP_sup_risk_unit AS (
	SELECT
	sup_risk_unit_id,
	risk_unit_code
	FROM (
		SELECT 
			sup_risk_unit_id,
			risk_unit_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_risk_unit
		WHERE crrnt_snpsht_flag='1' and source_sys_id='@{pipeline().parameters.MERGED_SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY risk_unit_code ORDER BY sup_risk_unit_id) = 1
),
LKP_sup_risk_unit_group AS (
	SELECT
	sup_risk_unit_grp_id,
	risk_unit_grp_code
	FROM (
		SELECT 
			sup_risk_unit_grp_id,
			risk_unit_grp_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_risk_unit_group
		WHERE crrnt_snpsht_flag='1' and source_sys_id='@{pipeline().parameters.MERGED_SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY risk_unit_grp_code ORDER BY sup_risk_unit_grp_id) = 1
),
EXP_Lkp_Values AS (
	SELECT
	LKP_Policy.pol_ak_id AS pol_key_ak_id,
	EXP_Value.reins_insurance_line,
	EXP_Value.reins_location_number,
	EXP_Value.reins_sub_location_number,
	EXP_Value.reins_risk_unit_group,
	EXP_Value.reins_seq_rsk_unt_grp,
	EXP_Value.reins_location,
	EXP_Value.reins_risk_sequence,
	EXP_Value.reins_section_code,
	EXP_Value.reins_company_no,
	LKP_SupReinsuranceMaster.ReinsuranceMasterReinsuranceCompanyName AS in_reins_co_name,
	-- *INF*: IIF(ISNULL(in_reins_co_name),'N/A',in_reins_co_name)
	IFF(in_reins_co_name IS NULL,
		'N/A',
		in_reins_co_name
	) AS out_reins_co_name,
	EXP_Value.reins_eff_date,
	EXP_Value.reins_exp_date,
	EXP_Value.reins_ent_date,
	EXP_Value.reins_type,
	EXP_Value.reins_percent_prem_ceded,
	EXP_Value.reins_percent_loss_ceded,
	EXP_Value.reins_percent_fac_comm,
	EXP_Value.reins_excess_amt,
	EXP_Value.reins_occur_limit,
	EXP_Value.reins_aggregate_limit,
	LKP_sup_insurance_line.sup_ins_line_id AS in_sup_ins_line_id,
	-- *INF*: IIF(ISNULL(in_sup_ins_line_id),-1,in_sup_ins_line_id)
	IFF(in_sup_ins_line_id IS NULL,
		- 1,
		in_sup_ins_line_id
	) AS out_sup_ins_line_id,
	LKP_sup_risk_unit.sup_risk_unit_id AS in_sup_risk_unit_id,
	-- *INF*: IIF(ISNULL(in_sup_risk_unit_id),-1,in_sup_risk_unit_id)
	IFF(in_sup_risk_unit_id IS NULL,
		- 1,
		in_sup_risk_unit_id
	) AS out_sup_risk_unit_id,
	LKP_sup_risk_unit_group.sup_risk_unit_grp_id AS in_sup_risk_unit_grp_id,
	-- *INF*: IIF(ISNULL(in_sup_risk_unit_grp_id),-1,in_sup_risk_unit_grp_id)
	IFF(in_sup_risk_unit_grp_id IS NULL,
		- 1,
		in_sup_risk_unit_grp_id
	) AS out_sup_risk_unit_grp_id,
	-1 AS out_SupReinsuranceMasterId,
	-1 AS out_sup_reins_type_id,
	EXP_Value.ReinsuranceMethod
	FROM EXP_Value
	LEFT JOIN LKP_Policy
	ON LKP_Policy.pol_key = EXP_Value.pol_Key
	LEFT JOIN LKP_SupReinsuranceMaster
	ON LKP_SupReinsuranceMaster.ReinsuranceMasterReinsuranceCompanyNumber = EXP_Value.reins_company_no
	LEFT JOIN LKP_sup_insurance_line
	ON LKP_sup_insurance_line.ins_line_code = EXP_Value.reins_insurance_line
	LEFT JOIN LKP_sup_risk_unit
	ON LKP_sup_risk_unit.risk_unit_code = EXP_Value.reins_location
	LEFT JOIN LKP_sup_risk_unit_group
	ON LKP_sup_risk_unit_group.risk_unit_grp_code = EXP_Value.reins_risk_unit_group
),
LKP_reinsurance_coverage AS (
	SELECT
	reins_cov_id,
	reins_cov_ak_id,
	reins_co_name,
	reins_exp_date,
	reins_type,
	reins_prcnt_prem_ceded,
	reins_prcnt_loss_ceded,
	reins_prcnt_facultative_commssn,
	reins_excess_amt,
	reins_occurrence_lmt,
	reins_agg_lmt,
	pol_ak_id,
	reins_ins_line,
	reins_loc_unit_num,
	reins_sub_loc_unit_num,
	reins_risk_unit_grp,
	reins_risk_unit_grp_seq_num,
	reins_risk_unit,
	reins_risk_unit_seq_num,
	reins_section_code,
	reins_co_num,
	reins_eff_date,
	reins_enter_date
	FROM (
		SELECT reins_cov_id                       AS reins_cov_id,
		       reins_cov_ak_id                    AS reins_cov_ak_id,
		       Rtrim(reins_co_name)               AS reins_co_name,
		       reins_exp_date                     AS reins_exp_date,
		       Rtrim(reins_type)                  AS reins_type,
		       reins_prcnt_prem_ceded             AS reins_prcnt_prem_ceded,
		       reins_prcnt_loss_ceded             AS reins_prcnt_loss_ceded,
		       reins_prcnt_facultative_commssn    AS reins_prcnt_facultative_commssn,
		       reins_excess_amt                   AS reins_excess_amt,
		       reins_occurrence_lmt               AS reins_occurrence_lmt,
		       reins_agg_lmt                      AS reins_agg_lmt,
		       pol_ak_id                          AS pol_ak_id,
		       Rtrim(reins_ins_line)              AS reins_ins_line,
		       Rtrim(reins_loc_unit_num)          AS reins_loc_unit_num,
		       Rtrim(reins_sub_loc_unit_num)      AS reins_sub_loc_unit_num,
		       Rtrim(reins_risk_unit_grp)         AS reins_risk_unit_grp,
		       Rtrim(reins_risk_unit_grp_seq_num) AS reins_risk_unit_grp_seq_num,
		       Rtrim(reins_risk_unit)             AS reins_risk_unit,
		       Rtrim(reins_risk_unit_seq_num)     AS reins_risk_unit_seq_num,
		       Rtrim(reins_section_code)          AS reins_section_code,
		       Rtrim(reins_co_num)                AS reins_co_num,
		       reins_eff_date                     AS reins_eff_date,
		       reins_enter_date                   AS reins_enter_date
		FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.reinsurance_coverage
		WHERE  crrnt_snpsht_flag = 1 and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id,reins_ins_line,reins_loc_unit_num,reins_sub_loc_unit_num,reins_risk_unit_grp,reins_risk_unit_grp_seq_num,reins_risk_unit,reins_risk_unit_seq_num,reins_section_code,reins_co_num,reins_eff_date,reins_enter_date ORDER BY reins_cov_id DESC) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_reinsurance_coverage.reins_cov_id AS lkp_reins_cov_id,
	LKP_reinsurance_coverage.reins_cov_ak_id AS lkp_reins_cov_ak_id,
	LKP_reinsurance_coverage.reins_co_name AS lkp_reins_co_name,
	LKP_reinsurance_coverage.reins_exp_date AS lkp_reins_exp_date,
	LKP_reinsurance_coverage.reins_type AS lkp_reins_type,
	LKP_reinsurance_coverage.reins_prcnt_prem_ceded AS lkp_reins_prcnt_prem_ceded,
	LKP_reinsurance_coverage.reins_prcnt_loss_ceded AS lkp_reins_prcnt_loss_ceded,
	LKP_reinsurance_coverage.reins_prcnt_facultative_commssn AS lkp_reins_prcnt_facultative_commssn,
	LKP_reinsurance_coverage.reins_excess_amt AS lkp_reins_excess_amt,
	LKP_reinsurance_coverage.reins_occurrence_lmt AS lkp_reins_occurrence_lmt,
	LKP_reinsurance_coverage.reins_agg_lmt AS lkp_reins_agg_lmt,
	EXP_Lkp_Values.pol_key_ak_id,
	EXP_Lkp_Values.reins_insurance_line,
	EXP_Lkp_Values.reins_location_number,
	EXP_Lkp_Values.reins_sub_location_number,
	EXP_Lkp_Values.reins_risk_unit_group,
	EXP_Lkp_Values.reins_seq_rsk_unt_grp,
	EXP_Lkp_Values.reins_location,
	EXP_Lkp_Values.reins_risk_sequence,
	EXP_Lkp_Values.reins_section_code,
	EXP_Lkp_Values.reins_company_no,
	EXP_Lkp_Values.out_reins_co_name AS reins_co_name,
	EXP_Lkp_Values.reins_eff_date,
	EXP_Lkp_Values.reins_exp_date,
	EXP_Lkp_Values.reins_ent_date,
	EXP_Lkp_Values.reins_type,
	EXP_Lkp_Values.reins_percent_prem_ceded,
	EXP_Lkp_Values.reins_percent_loss_ceded,
	EXP_Lkp_Values.reins_percent_fac_comm,
	EXP_Lkp_Values.reins_excess_amt,
	EXP_Lkp_Values.reins_occur_limit,
	EXP_Lkp_Values.reins_aggregate_limit,
	EXP_Lkp_Values.out_sup_ins_line_id AS sup_ins_line_id,
	EXP_Lkp_Values.out_sup_risk_unit_id AS sup_risk_unit_id,
	EXP_Lkp_Values.out_sup_risk_unit_grp_id AS sup_risk_unit_grp_id,
	EXP_Lkp_Values.out_sup_reins_type_id AS sup_reins_type_id,
	EXP_Lkp_Values.out_SupReinsuranceMasterId AS SupReinsuranceMasterId,
	-- *INF*: iif(isnull(lkp_reins_cov_id),'NEW',
	-- 	iif (
	-- 	ltrim(rtrim(lkp_reins_co_name)) <> ltrim(rtrim(reins_co_name)) or
	-- 	(lkp_reins_exp_date <> reins_exp_date) or
	-- 	(ltrim(rtrim(lkp_reins_type))  <> ltrim(rtrim(reins_type))) or
	-- 	lkp_reins_prcnt_prem_ceded <> reins_percent_prem_ceded or
	-- 	lkp_reins_prcnt_loss_ceded <> reins_percent_loss_ceded or
	-- 	lkp_reins_prcnt_facultative_commssn <> reins_percent_fac_comm or
	-- 	lkp_reins_excess_amt <> reins_excess_amt or
	-- 	lkp_reins_occurrence_lmt <> reins_occur_limit or
	-- 	lkp_reins_agg_lmt <> reins_aggregate_limit
	--   	,'UPDATE'
	-- 	,'NOCHANGE'))
	-- 
	IFF(lkp_reins_cov_id IS NULL,
		'NEW',
		IFF(ltrim(rtrim(lkp_reins_co_name
				)
			) <> ltrim(rtrim(reins_co_name
				)
			) 
			OR ( lkp_reins_exp_date <> reins_exp_date 
			) 
			OR ( ltrim(rtrim(lkp_reins_type
					)
				) <> ltrim(rtrim(reins_type
					)
				) 
			) 
			OR lkp_reins_prcnt_prem_ceded <> reins_percent_prem_ceded 
			OR lkp_reins_prcnt_loss_ceded <> reins_percent_loss_ceded 
			OR lkp_reins_prcnt_facultative_commssn <> reins_percent_fac_comm 
			OR lkp_reins_excess_amt <> reins_excess_amt 
			OR lkp_reins_occurrence_lmt <> reins_occur_limit 
			OR lkp_reins_agg_lmt <> reins_aggregate_limit,
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_Changed_Flag,
	-1 AS out_sup_reins_company_name_id,
	1 AS Crrnt_Snpsht_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_Id,
	-- *INF*: IIF(v_Changed_Flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	SYSDATE)
	IFF(v_Changed_Flag = 'NEW',
		TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		SYSDATE
	) AS Eff_From_Date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS Eff_To_Date,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID,
	SYSDATE AS Created_Date,
	SYSDATE AS Modified_Date,
	'0' AS logical_flag,
	EXP_Lkp_Values.ReinsuranceMethod
	FROM EXP_Lkp_Values
	LEFT JOIN LKP_reinsurance_coverage
	ON LKP_reinsurance_coverage.pol_ak_id = EXP_Lkp_Values.pol_key_ak_id AND LKP_reinsurance_coverage.reins_ins_line = EXP_Lkp_Values.reins_insurance_line AND LKP_reinsurance_coverage.reins_loc_unit_num = EXP_Lkp_Values.reins_location_number AND LKP_reinsurance_coverage.reins_sub_loc_unit_num = EXP_Lkp_Values.reins_sub_location_number AND LKP_reinsurance_coverage.reins_risk_unit_grp = EXP_Lkp_Values.reins_risk_unit_group AND LKP_reinsurance_coverage.reins_risk_unit_grp_seq_num = EXP_Lkp_Values.reins_seq_rsk_unt_grp AND LKP_reinsurance_coverage.reins_risk_unit = EXP_Lkp_Values.reins_location AND LKP_reinsurance_coverage.reins_risk_unit_seq_num = EXP_Lkp_Values.reins_risk_sequence AND LKP_reinsurance_coverage.reins_section_code = EXP_Lkp_Values.reins_section_code AND LKP_reinsurance_coverage.reins_co_num = EXP_Lkp_Values.reins_company_no AND LKP_reinsurance_coverage.reins_eff_date = EXP_Lkp_Values.reins_eff_date AND LKP_reinsurance_coverage.reins_enter_date = EXP_Lkp_Values.reins_ent_date
),
FIL_Insert AS (
	SELECT
	lkp_reins_cov_ak_id, 
	Changed_Flag, 
	pol_key_ak_id, 
	reins_insurance_line, 
	reins_location_number, 
	reins_sub_location_number, 
	reins_risk_unit_group, 
	reins_seq_rsk_unt_grp, 
	reins_location, 
	reins_risk_sequence, 
	reins_section_code, 
	reins_company_no, 
	reins_co_name, 
	reins_eff_date, 
	reins_exp_date, 
	reins_ent_date, 
	reins_type, 
	reins_percent_prem_ceded, 
	reins_percent_loss_ceded, 
	reins_percent_fac_comm, 
	reins_excess_amt, 
	reins_occur_limit, 
	reins_aggregate_limit, 
	sup_ins_line_id, 
	sup_risk_unit_id, 
	sup_risk_unit_grp_id, 
	sup_reins_type_id, 
	out_sup_reins_company_name_id AS sup_reins_company_name_id, 
	SupReinsuranceMasterId, 
	Crrnt_Snpsht_Flag, 
	Audit_Id, 
	Eff_From_Date, 
	Eff_To_Date, 
	SOURCE_SYSTEM_ID, 
	Created_Date, 
	Modified_Date, 
	logical_flag, 
	ReinsuranceMethod
	FROM EXP_Detect_Changes
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
SEQ_Reins_Cov_AK_ID AS (
	CREATE SEQUENCE SEQ_Reins_Cov_AK_ID
	START = 0
	INCREMENT = 1;
),
EXP_Determine_AK AS (
	SELECT
	Crrnt_Snpsht_Flag,
	Audit_Id,
	Eff_From_Date,
	Eff_To_Date,
	SOURCE_SYSTEM_ID AS Source_Sys_Id,
	Created_Date,
	Modified_Date,
	SEQ_Reins_Cov_AK_ID.NEXTVAL,
	Changed_Flag,
	lkp_reins_cov_ak_id,
	-- *INF*: IIF(Changed_Flag='NEW',
	-- NEXTVAL,
	-- lkp_reins_cov_ak_id)
	IFF(Changed_Flag = 'NEW',
		NEXTVAL,
		lkp_reins_cov_ak_id
	) AS reins_cov_ak_id,
	pol_key_ak_id,
	reins_insurance_line,
	reins_location_number,
	reins_sub_location_number,
	reins_risk_unit_group,
	reins_seq_rsk_unt_grp,
	reins_location,
	reins_risk_sequence,
	reins_section_code,
	reins_company_no,
	reins_co_name,
	reins_eff_date,
	reins_exp_date,
	reins_ent_date,
	reins_type,
	reins_percent_prem_ceded,
	reins_percent_loss_ceded,
	reins_percent_fac_comm,
	reins_excess_amt,
	reins_occur_limit,
	reins_aggregate_limit,
	sup_ins_line_id,
	sup_risk_unit_id,
	sup_risk_unit_grp_id,
	sup_reins_type_id,
	sup_reins_company_name_id,
	SupReinsuranceMasterId,
	logical_flag,
	ReinsuranceMethod
	FROM FIL_Insert
),
reinsurance_coverage_INSERT AS (
	INSERT INTO reinsurance_coverage
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, logical_flag, reins_cov_ak_id, pol_ak_id, reins_ins_line, reins_loc_unit_num, reins_sub_loc_unit_num, reins_risk_unit_grp, reins_risk_unit_grp_seq_num, reins_risk_unit, reins_risk_unit_seq_num, reins_section_code, reins_co_num, reins_co_name, reins_eff_date, reins_exp_date, reins_enter_date, reins_type, reins_prcnt_prem_ceded, reins_prcnt_loss_ceded, reins_prcnt_facultative_commssn, reins_excess_amt, reins_occurrence_lmt, reins_agg_lmt, SupInsuranceLineId, SupRiskUnitId, SupRiskUnitGroupId, ReinsuranceMethod)
	SELECT 
	Crrnt_Snpsht_Flag AS CRRNT_SNPSHT_FLAG, 
	Audit_Id AS AUDIT_ID, 
	Eff_From_Date AS EFF_FROM_DATE, 
	Eff_To_Date AS EFF_TO_DATE, 
	Source_Sys_Id AS SOURCE_SYS_ID, 
	Created_Date AS CREATED_DATE, 
	Modified_Date AS MODIFIED_DATE, 
	LOGICAL_FLAG, 
	REINS_COV_AK_ID, 
	pol_key_ak_id AS POL_AK_ID, 
	reins_insurance_line AS REINS_INS_LINE, 
	reins_location_number AS REINS_LOC_UNIT_NUM, 
	reins_sub_location_number AS REINS_SUB_LOC_UNIT_NUM, 
	reins_risk_unit_group AS REINS_RISK_UNIT_GRP, 
	reins_seq_rsk_unt_grp AS REINS_RISK_UNIT_GRP_SEQ_NUM, 
	reins_location AS REINS_RISK_UNIT, 
	reins_risk_sequence AS REINS_RISK_UNIT_SEQ_NUM, 
	REINS_SECTION_CODE, 
	reins_company_no AS REINS_CO_NUM, 
	REINS_CO_NAME, 
	REINS_EFF_DATE, 
	REINS_EXP_DATE, 
	reins_ent_date AS REINS_ENTER_DATE, 
	REINS_TYPE, 
	reins_percent_prem_ceded AS REINS_PRCNT_PREM_CEDED, 
	reins_percent_loss_ceded AS REINS_PRCNT_LOSS_CEDED, 
	reins_percent_fac_comm AS REINS_PRCNT_FACULTATIVE_COMMSSN, 
	REINS_EXCESS_AMT, 
	reins_occur_limit AS REINS_OCCURRENCE_LMT, 
	reins_aggregate_limit AS REINS_AGG_LMT, 
	sup_ins_line_id AS SUPINSURANCELINEID, 
	sup_risk_unit_id AS SUPRISKUNITID, 
	sup_risk_unit_grp_id AS SUPRISKUNITGROUPID, 
	REINSURANCEMETHOD
	FROM EXP_Determine_AK
),
SQ_reinsurance_coverage AS (
	SELECT 
		 A.reins_cov_id,
	       A.eff_from_date,
	       A.eff_to_date,
	       A.source_sys_id,
	       A.pol_ak_id,
	       A.reins_ins_line,
	       A.reins_loc_unit_num,
	       A.reins_sub_loc_unit_num,
	       A.reins_risk_unit_grp,
	       A.reins_risk_unit_grp_seq_num,
	       A.reins_risk_unit,
	       A.reins_risk_unit_seq_num,
	       A.reins_section_code,
	       A.reins_co_num,
	       A.reins_eff_date,
	       A.reins_enter_date
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.reinsurance_coverage a
	where a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	and EXISTS (SELECT 1			
		FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.reinsurance_coverage b
		WHERE b.crrnt_snpsht_flag = 1
	      AND a.reins_cov_ak_id = b.reins_cov_ak_id
	      AND a.source_sys_id = b.source_sys_id
		GROUP BY b.reins_cov_ak_id
		HAVING COUNT(*) > 1)
	order by a.reins_cov_ak_id, a.eff_from_date desc
	
	--EXISTS Subquery exists to pick AK Groups that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag and all columns of the AK
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
	
	--ORDER BY of main query orders all rows first by the AK and then by the eff_from_date in a DESC format
	--the descending order is important because this allows us to avoid another lookup and properly apply the eff_to_date by utilizing a local variable to keep track
),
EXP_Expire_Rows AS (
	SELECT
	reins_cov_id,
	pol_ak_id,
	reins_ins_line,
	reins_loc_unit_num,
	reins_sub_loc_unit_num,
	reins_risk_unit_grp,
	reins_risk_unit_grp_seq_num,
	reins_risk_unit,
	reins_risk_unit_seq_num,
	reins_section_code,
	reins_co_num,
	reins_eff_date,
	reins_enter_date,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	source_sys_id,
	-- *INF*: DECODE (TRUE, 
	-- pol_ak_id = v_PREV_ROW_pol_ak_id1 AND
	-- reins_ins_line = v_PREV_ROW_reins_ins_line1 AND
	-- reins_loc_unit_num = v_PREV_ROW_reins_loc_unit_num1 AND
	-- reins_sub_loc_unit_num = v_PREV_ROW_reins_sub_loc_unit_num1 AND
	-- reins_risk_unit_grp = v_PREV_ROW_reins_risk_unit_grp1 AND
	-- reins_risk_unit_grp_seq_num = v_PREV_ROW_reins_risk_unit_grp_seq_num1 AND
	-- reins_risk_unit = v_PREV_ROW_reins_risk_unit1 AND
	-- reins_risk_unit_seq_num = v_PREV_ROW_reins_risk_unit_seq_num1 AND
	-- reins_section_code = v_PREV_ROW_reins_section_code1 AND
	-- reins_co_num = v_PREV_ROW_reins_co_num1 AND
	-- reins_eff_date = v_PREV_ROW_reins_eff_date1 AND
	-- IIF(reins_section_code='N/A', 1=1, reins_enter_date = v_PREV_ROW_reins_enter_date1) AND
	-- source_sys_id = v_PREV_ROW_source_sys_id1
	-- , ADD_TO_DATE(v_PREV_ROW_eff_from_date1,'SS',-1)
	-- ,orig_eff_to_date)
	DECODE(TRUE,
		pol_ak_id = v_PREV_ROW_pol_ak_id1 
		AND reins_ins_line = v_PREV_ROW_reins_ins_line1 
		AND reins_loc_unit_num = v_PREV_ROW_reins_loc_unit_num1 
		AND reins_sub_loc_unit_num = v_PREV_ROW_reins_sub_loc_unit_num1 
		AND reins_risk_unit_grp = v_PREV_ROW_reins_risk_unit_grp1 
		AND reins_risk_unit_grp_seq_num = v_PREV_ROW_reins_risk_unit_grp_seq_num1 
		AND reins_risk_unit = v_PREV_ROW_reins_risk_unit1 
		AND reins_risk_unit_seq_num = v_PREV_ROW_reins_risk_unit_seq_num1 
		AND reins_section_code = v_PREV_ROW_reins_section_code1 
		AND reins_co_num = v_PREV_ROW_reins_co_num1 
		AND reins_eff_date = v_PREV_ROW_reins_eff_date1 
		AND IFF(reins_section_code = 'N/A',
			1 = 1,
			reins_enter_date = v_PREV_ROW_reins_enter_date1
		) 
		AND source_sys_id = v_PREV_ROW_source_sys_id1, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date1),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	pol_ak_id AS v_PREV_ROW_pol_ak_id1,
	reins_ins_line AS v_PREV_ROW_reins_ins_line1,
	reins_loc_unit_num AS v_PREV_ROW_reins_loc_unit_num1,
	reins_sub_loc_unit_num AS v_PREV_ROW_reins_sub_loc_unit_num1,
	reins_risk_unit_grp AS v_PREV_ROW_reins_risk_unit_grp1,
	reins_risk_unit_grp_seq_num AS v_PREV_ROW_reins_risk_unit_grp_seq_num1,
	reins_risk_unit AS v_PREV_ROW_reins_risk_unit1,
	reins_risk_unit_seq_num AS v_PREV_ROW_reins_risk_unit_seq_num1,
	reins_section_code AS v_PREV_ROW_reins_section_code1,
	reins_co_num AS v_PREV_ROW_reins_co_num1,
	reins_eff_date AS v_PREV_ROW_reins_eff_date1,
	reins_enter_date AS v_PREV_ROW_reins_enter_date1,
	source_sys_id AS v_PREV_ROW_source_sys_id1,
	eff_from_date AS v_PREV_ROW_eff_from_date1,
	0 AS crrnt_snapshot_flag,
	sysdate AS modified_date
	FROM SQ_reinsurance_coverage
),
FIL_reinsurance_coverage AS (
	SELECT
	reins_cov_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snapshot_flag, 
	modified_date
	FROM EXP_Expire_Rows
	WHERE orig_eff_to_date != eff_to_date
),
UPD_Update_Target AS (
	SELECT
	reins_cov_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snapshot_flag, 
	modified_date
	FROM FIL_reinsurance_coverage
),
reinsurance_coverage_UPDATE AS (
	MERGE INTO reinsurance_coverage AS T
	USING UPD_Update_Target AS S
	ON T.reins_cov_id = S.reins_cov_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snapshot_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),
WITH
SQ_reinsurance_coverage AS (
	SELECT
		reins_cov_id,
		eff_from_date,
		source_sys_id,
		created_date,
		reins_cov_ak_id,
		reins_loc_unit_num,
		reins_sub_loc_unit_num,
		reins_risk_unit_grp_seq_num,
		reins_risk_unit_seq_num,
		reins_section_code,
		reins_co_num,
		reins_co_name,
		reins_eff_date,
		reins_exp_date,
		reins_enter_date,
		reins_type,
		reins_prcnt_prem_ceded,
		reins_prcnt_loss_ceded,
		reins_prcnt_facultative_commssn,
		reins_excess_amt,
		reins_occurrence_lmt,
		reins_agg_lmt,
		SupInsuranceLineId,
		SupRiskUnitId,
		SupRiskUnitGroupId,
		ReinsuranceMethod
	FROM reinsurance_coverage
	WHERE created_date >= '@{pipeline().parameters.SELECTION_START_TS}'
),
LKP_Reinsurance_Coverage_Dim AS (
	SELECT
	reins_cov_dim_id,
	edw_reins_cov_ak_id,
	eff_from_date
	FROM (
		SELECT 
			reins_cov_dim_id,
			edw_reins_cov_ak_id,
			eff_from_date
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.reinsurance_coverage_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_reins_cov_ak_id,eff_from_date ORDER BY reins_cov_dim_id) = 1
),
LKP_Sup_Insurance_Line AS (
	SELECT
	StandardInsuranceLineCode,
	sup_ins_line_id
	FROM (
		SELECT 
			StandardInsuranceLineCode,
			sup_ins_line_id
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_ins_line_id ORDER BY StandardInsuranceLineCode) = 1
),
LKP_Sup_Risk_Unit AS (
	SELECT
	StandardRiskUnitCode,
	sup_risk_unit_id
	FROM (
		SELECT 
			StandardRiskUnitCode,
			sup_risk_unit_id
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_risk_unit
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_risk_unit_id ORDER BY StandardRiskUnitCode) = 1
),
LKP_Sup_Risk_Unit_Group AS (
	SELECT
	risk_unit_grp_code,
	sup_risk_unit_grp_id
	FROM (
		SELECT 
			risk_unit_grp_code,
			sup_risk_unit_grp_id
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_risk_unit_group
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_risk_unit_grp_id ORDER BY risk_unit_grp_code) = 1
),
EXP_Get_Values AS (
	SELECT
	SQ_reinsurance_coverage.reins_cov_id,
	SQ_reinsurance_coverage.eff_from_date,
	SQ_reinsurance_coverage.source_sys_id,
	SQ_reinsurance_coverage.reins_cov_ak_id,
	SQ_reinsurance_coverage.reins_loc_unit_num,
	SQ_reinsurance_coverage.reins_sub_loc_unit_num,
	SQ_reinsurance_coverage.reins_risk_unit_grp_seq_num,
	SQ_reinsurance_coverage.reins_risk_unit_seq_num,
	SQ_reinsurance_coverage.reins_section_code,
	SQ_reinsurance_coverage.reins_co_num,
	SQ_reinsurance_coverage.reins_co_name,
	SQ_reinsurance_coverage.reins_eff_date,
	SQ_reinsurance_coverage.reins_exp_date,
	SQ_reinsurance_coverage.reins_enter_date,
	SQ_reinsurance_coverage.reins_type,
	SQ_reinsurance_coverage.reins_prcnt_prem_ceded,
	SQ_reinsurance_coverage.reins_prcnt_loss_ceded,
	SQ_reinsurance_coverage.reins_prcnt_facultative_commssn,
	SQ_reinsurance_coverage.reins_excess_amt,
	SQ_reinsurance_coverage.reins_occurrence_lmt,
	SQ_reinsurance_coverage.reins_agg_lmt,
	SQ_reinsurance_coverage.ReinsuranceMethod,
	LKP_Reinsurance_Coverage_Dim.reins_cov_dim_id AS lkp_reins_cov_dim_id,
	LKP_Sup_Insurance_Line.StandardInsuranceLineCode AS lkp_StandardInsuranceLineCode,
	LKP_Sup_Risk_Unit_Group.risk_unit_grp_code AS lkp_StandardRiskUnitGroupCode,
	LKP_Sup_Risk_Unit.StandardRiskUnitCode AS lkp_StandardRiskUnitCode,
	-- *INF*: DECODE(source_sys_id,'DCT', reins_type, 'N/A')
	DECODE(source_sys_id,
		'DCT', reins_type,
		'N/A'
	) AS o_ReinsuranceType,
	ReinsuranceMethod AS o_ReinsuranceMethod,
	-- *INF*: IIF(ISNULL(lkp_StandardInsuranceLineCode) OR IS_SPACES(lkp_StandardInsuranceLineCode) OR LENGTH(lkp_StandardInsuranceLineCode)=0,'N/A',LTRIM(RTRIM(lkp_StandardInsuranceLineCode)))
	IFF(lkp_StandardInsuranceLineCode IS NULL 
		OR LENGTH(lkp_StandardInsuranceLineCode)>0 AND TRIM(lkp_StandardInsuranceLineCode)='' 
		OR LENGTH(lkp_StandardInsuranceLineCode
		) = 0,
		'N/A',
		LTRIM(RTRIM(lkp_StandardInsuranceLineCode
			)
		)
	) AS o_reins_ins_line,
	-- *INF*: IIF(ISNULL(lkp_StandardRiskUnitGroupCode) OR IS_SPACES(lkp_StandardRiskUnitGroupCode) OR LENGTH(lkp_StandardRiskUnitGroupCode)=0,'N/A',LTRIM(RTRIM(lkp_StandardRiskUnitGroupCode)))
	IFF(lkp_StandardRiskUnitGroupCode IS NULL 
		OR LENGTH(lkp_StandardRiskUnitGroupCode)>0 AND TRIM(lkp_StandardRiskUnitGroupCode)='' 
		OR LENGTH(lkp_StandardRiskUnitGroupCode
		) = 0,
		'N/A',
		LTRIM(RTRIM(lkp_StandardRiskUnitGroupCode
			)
		)
	) AS o_reins_risk_unit_grp,
	-- *INF*: IIF(ISNULL(lkp_StandardRiskUnitCode) OR IS_SPACES(lkp_StandardRiskUnitCode) OR LENGTH(lkp_StandardRiskUnitCode)=0,'N/A',LTRIM(RTRIM(lkp_StandardRiskUnitCode)))
	IFF(lkp_StandardRiskUnitCode IS NULL 
		OR LENGTH(lkp_StandardRiskUnitCode)>0 AND TRIM(lkp_StandardRiskUnitCode)='' 
		OR LENGTH(lkp_StandardRiskUnitCode
		) = 0,
		'N/A',
		LTRIM(RTRIM(lkp_StandardRiskUnitCode
			)
		)
	) AS o_reins_risk_unit,
	-- *INF*: DECODE(LTRIM(RTRIM(reins_co_num)),
	-- '0005','N',
	-- '0007','N',
	-- '0009','N',
	-- '0015','N',
	-- '0016','N',
	-- '0031','N',
	-- '0032','N',
	-- '0033','N',
	-- '0034','N',
	-- '0035','N',
	-- '0039','N',
	-- '0055','N',
	-- 'F')
	DECODE(LTRIM(RTRIM(reins_co_num
			)
		),
		'0005', 'N',
		'0007', 'N',
		'0009', 'N',
		'0015', 'N',
		'0016', 'N',
		'0031', 'N',
		'0032', 'N',
		'0033', 'N',
		'0034', 'N',
		'0035', 'N',
		'0039', 'N',
		'0055', 'N',
		'F'
	) AS o_facultative_ind,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS o_eff_to_Date,
	1 AS o_crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_Audit_id,
	SYSDATE AS o_Created_date
	FROM SQ_reinsurance_coverage
	LEFT JOIN LKP_Reinsurance_Coverage_Dim
	ON LKP_Reinsurance_Coverage_Dim.edw_reins_cov_ak_id = SQ_reinsurance_coverage.reins_cov_ak_id AND LKP_Reinsurance_Coverage_Dim.eff_from_date = SQ_reinsurance_coverage.eff_from_date
	LEFT JOIN LKP_Sup_Insurance_Line
	ON LKP_Sup_Insurance_Line.sup_ins_line_id = SQ_reinsurance_coverage.SupInsuranceLineId
	LEFT JOIN LKP_Sup_Risk_Unit
	ON LKP_Sup_Risk_Unit.sup_risk_unit_id = SQ_reinsurance_coverage.SupRiskUnitId
	LEFT JOIN LKP_Sup_Risk_Unit_Group
	ON LKP_Sup_Risk_Unit_Group.sup_risk_unit_grp_id = SQ_reinsurance_coverage.SupRiskUnitGroupId
),
RTR_Reinsurance_Coverage_Dim AS (
	SELECT
	lkp_reins_cov_dim_id AS reins_cov_dim_id,
	reins_cov_id,
	eff_from_date,
	reins_cov_ak_id,
	o_reins_ins_line AS reins_ins_line,
	reins_loc_unit_num,
	reins_sub_loc_unit_num,
	o_reins_risk_unit_grp AS reins_risk_unit_grp,
	reins_risk_unit_grp_seq_num,
	o_reins_risk_unit AS reins_risk_unit,
	reins_risk_unit_seq_num,
	reins_section_code,
	reins_co_num,
	reins_co_name,
	reins_eff_date,
	reins_exp_date,
	reins_enter_date,
	reins_type,
	reins_prcnt_prem_ceded,
	reins_prcnt_loss_ceded,
	reins_prcnt_facultative_commssn,
	reins_excess_amt,
	reins_occurrence_lmt,
	reins_agg_lmt,
	o_facultative_ind AS facultative_ind,
	o_ReinsuranceType AS ReinsuranceType,
	ReinsuranceMethod,
	o_eff_to_Date AS eff_to_Date,
	o_Audit_id AS Audit_id,
	o_Created_date AS Created_date,
	o_crrnt_snpsht_flag AS crrnt_snpsht_flag
	FROM EXP_Get_Values
),
RTR_Reinsurance_Coverage_Dim_INSERT AS (SELECT * FROM RTR_Reinsurance_Coverage_Dim WHERE ISNULL(reins_cov_dim_id)),
RTR_Reinsurance_Coverage_Dim_DEFAULT1 AS (SELECT * FROM RTR_Reinsurance_Coverage_Dim WHERE NOT ( (ISNULL(reins_cov_dim_id)) )),
UPD_Reins_Coverage_Dim AS (
	SELECT
	reins_cov_dim_id AS reins_cov_dim_id2, 
	reins_cov_id AS reins_cov_id2, 
	eff_from_date AS eff_from_date2, 
	reins_cov_ak_id AS reins_cov_ak_id2, 
	reins_ins_line AS reins_ins_line2, 
	reins_loc_unit_num AS reins_loc_unit_num2, 
	reins_sub_loc_unit_num AS reins_sub_loc_unit_num2, 
	reins_risk_unit_grp AS reins_risk_unit_grp2, 
	reins_risk_unit_grp_seq_num AS reins_risk_unit_grp_seq_num2, 
	reins_risk_unit AS reins_risk_unit2, 
	reins_risk_unit_seq_num AS reins_risk_unit_seq_num2, 
	reins_section_code AS reins_section_code2, 
	reins_co_num AS reins_co_num2, 
	reins_co_name AS reins_co_name2, 
	reins_eff_date AS reins_eff_date2, 
	reins_exp_date AS reins_exp_date2, 
	reins_enter_date AS reins_enter_date2, 
	reins_type AS reins_type2, 
	reins_prcnt_prem_ceded AS reins_prcnt_prem_ceded2, 
	reins_prcnt_loss_ceded AS reins_prcnt_loss_ceded2, 
	reins_prcnt_facultative_commssn AS reins_prcnt_facultative_commssn2, 
	reins_excess_amt AS reins_excess_amt2, 
	reins_occurrence_lmt AS reins_occurrence_lmt2, 
	reins_agg_lmt AS reins_agg_lmt2, 
	eff_to_Date AS eff_to_Date2, 
	Audit_id AS Audit_id2, 
	Created_date AS Created_date2, 
	facultative_ind AS facultative_ind2, 
	ReinsuranceType, 
	ReinsuranceMethod AS ReinsuranceMethod2
	FROM RTR_Reinsurance_Coverage_Dim_DEFAULT1
),
reinsurance_coverage_dim_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.reinsurance_coverage_dim AS T
	USING UPD_Reins_Coverage_Dim AS S
	ON T.reins_cov_dim_id = S.reins_cov_dim_id2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.audit_id = S.Audit_id2, T.eff_from_date = S.eff_from_date2, T.eff_to_date = S.eff_to_Date2, T.modified_date = S.Created_date2, T.edw_reins_cov_pk_id = S.reins_cov_id2, T.edw_reins_cov_ak_id = S.reins_cov_ak_id2, T.reins_ins_line = S.reins_ins_line2, T.reins_loc_unit_num = S.reins_loc_unit_num2, T.reins_sub_loc_unit_num = S.reins_sub_loc_unit_num2, T.reins_risk_unit_grp = S.reins_risk_unit_grp2, T.reins_risk_unit_grp_seq_num = S.reins_risk_unit_grp_seq_num2, T.reins_risk_unit = S.reins_risk_unit2, T.reins_risk_unit_seq_num = S.reins_risk_unit_seq_num2, T.reins_section_code = S.reins_section_code2, T.reins_co_num = S.reins_co_num2, T.reins_co_name = S.reins_co_name2, T.reins_eff_date = S.reins_eff_date2, T.reins_exp_date = S.reins_exp_date2, T.reins_enter_date = S.reins_enter_date2, T.reins_type = S.reins_type2, T.reins_prcnt_prem_ceded = S.reins_prcnt_prem_ceded2, T.reins_prcnt_loss_ceded = S.reins_prcnt_loss_ceded2, T.reins_prcnt_facultative_commssn = S.reins_prcnt_facultative_commssn2, T.reins_excess_amt = S.reins_excess_amt2, T.reins_occurrence_lmt = S.reins_occurrence_lmt2, T.reins_agg_lmt = S.reins_agg_lmt2, T.facultative_ind = S.facultative_ind2, T.ReinsuranceType = S.ReinsuranceType, T.ReinsuranceMethod = S.ReinsuranceMethod2
),
reinsurance_coverage_dim_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.reinsurance_coverage_dim
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date, edw_reins_cov_pk_id, edw_reins_cov_ak_id, reins_ins_line, reins_loc_unit_num, reins_sub_loc_unit_num, reins_risk_unit_grp, reins_risk_unit_grp_seq_num, reins_risk_unit, reins_risk_unit_seq_num, reins_section_code, reins_co_num, reins_co_name, reins_eff_date, reins_exp_date, reins_enter_date, reins_type, reins_prcnt_prem_ceded, reins_prcnt_loss_ceded, reins_prcnt_facultative_commssn, reins_excess_amt, reins_occurrence_lmt, reins_agg_lmt, facultative_ind, ReinsuranceType, ReinsuranceMethod)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	Audit_id AS AUDIT_ID, 
	EFF_FROM_DATE, 
	eff_to_Date AS EFF_TO_DATE, 
	Created_date AS CREATED_DATE, 
	Created_date AS MODIFIED_DATE, 
	reins_cov_id AS EDW_REINS_COV_PK_ID, 
	reins_cov_ak_id AS EDW_REINS_COV_AK_ID, 
	REINS_INS_LINE, 
	REINS_LOC_UNIT_NUM, 
	REINS_SUB_LOC_UNIT_NUM, 
	REINS_RISK_UNIT_GRP, 
	REINS_RISK_UNIT_GRP_SEQ_NUM, 
	REINS_RISK_UNIT, 
	REINS_RISK_UNIT_SEQ_NUM, 
	REINS_SECTION_CODE, 
	REINS_CO_NUM, 
	REINS_CO_NAME, 
	REINS_EFF_DATE, 
	REINS_EXP_DATE, 
	REINS_ENTER_DATE, 
	REINS_TYPE, 
	REINS_PRCNT_PREM_CEDED, 
	REINS_PRCNT_LOSS_CEDED, 
	REINS_PRCNT_FACULTATIVE_COMMSSN, 
	REINS_EXCESS_AMT, 
	REINS_OCCURRENCE_LMT, 
	REINS_AGG_LMT, 
	FACULTATIVE_IND, 
	REINSURANCETYPE, 
	REINSURANCEMETHOD
	FROM RTR_Reinsurance_Coverage_Dim_INSERT
),
SQ_reinsurance_coverage_dim AS (
	SELECT reinsurance_coverage_dim.reins_cov_dim_id,
	       reinsurance_coverage_dim.eff_from_date,
	       reinsurance_coverage_dim.eff_to_date,
	       reinsurance_coverage_dim.edw_reins_cov_ak_id
	FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.reinsurance_coverage_dim reinsurance_coverage_dim
	WHERE EXISTS
	(
	SELECT 1 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.reinsurance_coverage_dim  reinsurance_coverage_dim2
	WHERE crrnt_snpsht_flag =1 AND reinsurance_coverage_dim.edw_reins_cov_ak_id = reinsurance_coverage_dim2.edw_reins_cov_ak_id
	GROUP BY reinsurance_coverage_dim2.edw_reins_cov_ak_id HAVING COUNT(*) > 1
	)
	ORDER BY reinsurance_coverage_dim.edw_reins_cov_ak_id , reinsurance_coverage_dim.eff_from_date DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	reins_cov_dim_id,
	edw_reins_cov_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	edw_reins_cov_ak_id = v_PREV_ROW_occurrence_key, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(TRUE,
		edw_reins_cov_ak_id = v_PREV_ROW_occurrence_key, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	edw_reins_cov_ak_id AS v_PREV_ROW_occurrence_key,
	v_eff_to_date AS eff_to_date,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_reinsurance_coverage_dim
),
FIL_reinsurance_coverage_dim_update AS (
	SELECT
	reins_cov_dim_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_reinsurance_coverage_dim AS (
	SELECT
	reins_cov_dim_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_reinsurance_coverage_dim_update
),
reinsurance_coverage_dim_eff_date_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.reinsurance_coverage_dim AS T
	USING UPD_reinsurance_coverage_dim AS S
	ON T.reins_cov_dim_id = S.reins_cov_dim_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),
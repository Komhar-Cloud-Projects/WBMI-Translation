WITH
SQ_pif_4514_stage AS (
	SELECT DISTINCT A.pif_symbol,
	                A.pif_policy_number,
	                A.pif_module,
	                sar_insurance_line,
	                sar_location_x,
	                sar_sub_location_x,
	                sar_risk_unit_group,
	                sar_class_code_grp_x,
	                sar_class_code_mem_x,
	                (sar_unit + sar_risk_unit_continued) as sar_unit, 
	                CASE Len(Ltrim(Rtrim(Coalesce(sar_seq_rsk_unt_a, '')))) WHEN '0' THEN 'N/A'  ELSE 
			        CASE Len(Ltrim(Rtrim(Coalesce(sar_seq_rsk_unt_a, '')))) WHEN '1' THEN Ltrim(Rtrim(sar_seq_rsk_unt_a)) + '0' 
			        ELSE Ltrim(Rtrim(sar_seq_rsk_unt_a))   END   END     AS sar_seq_rsk_unt_a,
	                sar_type_exposure,
	                sar_major_peril,
	                sar_seq_no,
	                sar_cov_eff_year,
	                sar_cov_eff_month,
	                sar_cov_eff_day,
	                sar_type_bureau
	FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_4514_stage A, @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_02_stage B
	WHERE A.pif_symbol + A.pif_policy_number + A.pif_module = B.pif_symbol + B.pif_policy_number + B.pif_module 
	AND (LTRIM(RTRIM(pif_eff_yr_a)) <> '9999' OR LTRIM(RTRIM(pif_exp_yr_a)) <> '9999') AND
	LTRIM(RTRIM(SUBSTRING(CAST(pif_full_agency_number AS char(7)),1,2)+SUBSTRING(CAST(pif_full_agency_number AS char(7)),5,3))) <> '99999'
	
	
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
	sar_insurance_line,
	sar_location_x,
	sar_sub_location_x,
	sar_risk_unit_group,
	sar_class_code_grp_x,
	sar_class_code_mem_x,
	sar_unit_complete,
	sar_seq_rsk_unt_a,
	sar_type_exposure,
	sar_major_peril,
	sar_seq_no,
	sar_cov_eff_year,
	sar_cov_eff_month,
	sar_cov_eff_day,
	sar_type_bureau
	FROM SQ_pif_4514_stage
),
LKP_Policy AS (
	SELECT
	pol_ak_id,
	pol_key
	FROM (
		SELECT policy.pol_ak_id as pol_ak_id, policy.pol_key as pol_key FROM V2.policy
		WHERE crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_ak_id DESC) = 1
),
EXP_Values AS (
	SELECT
	LKP_Policy.pol_ak_id,
	EXP_Default.sar_insurance_line,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_insurance_line)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_insurance_line
	) AS sar_insurance_line_Out,
	EXP_Default.sar_location_x,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_location_x)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_location_x
	) AS sar_location_Out,
	EXP_Default.sar_sub_location_x,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_sub_location_x)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_sub_location_x
	) AS sar_sub_location_x1,
	EXP_Default.sar_risk_unit_group,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_risk_unit_group)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_risk_unit_group
	) AS sar_risk_unit_group_Out,
	EXP_Default.sar_class_code_grp_x,
	EXP_Default.sar_class_code_mem_x,
	-- *INF*:  ( sar_class_code_grp_x || sar_class_code_mem_x)
	( sar_class_code_grp_x || sar_class_code_mem_x 
	) AS v_risk_unit_group_seq,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_risk_unit_group_seq)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_risk_unit_group_seq
	) AS risk_unit_group_seq_Out,
	EXP_Default.sar_unit_complete,
	-- *INF*: IIF(LENGTH(LTRIM(RTRIM(sar_unit_complete)))= 0 OR IS_SPACES(LTRIM(RTRIM(sar_unit_complete))), '000000',sar_unit_complete)
	IFF(LENGTH(LTRIM(RTRIM(sar_unit_complete
				)
			)
		) = 0 
		OR LENGTH(LTRIM(RTRIM(sar_unit_complete
			)
		))>0 AND TRIM(LTRIM(RTRIM(sar_unit_complete
			)
		))='',
		'000000',
		sar_unit_complete
	) AS v_sar_unit,
	v_sar_unit AS sar_risk_unit,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_risk_unit)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_risk_unit
	) AS sar_risk_unit_Out,
	EXP_Default.sar_seq_rsk_unt_a,
	-- *INF*: RPAD(LTRIM(RTRIM(sar_seq_rsk_unt_a)),2,'0')
	RPAD(LTRIM(RTRIM(sar_seq_rsk_unt_a
			)
		), 2, '0'
	) AS v_sar_seq_rsk_unt_a,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_seq_rsk_unt_a)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_seq_rsk_unt_a
	) AS sar_rsk_unit_seq_out,
	EXP_Default.sar_type_exposure,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_type_exposure)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_type_exposure
	) AS sar_type_exposure_out,
	EXP_Default.sar_major_peril,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_major_peril)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_major_peril
	) AS sar_major_peril_out,
	EXP_Default.sar_seq_no,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_seq_no)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_seq_no
	) AS sar_major_peril_seq_no,
	EXP_Default.sar_cov_eff_year,
	-- *INF*: TO_CHAR(sar_cov_eff_year)
	TO_CHAR(sar_cov_eff_year
	) AS v_sar_cov_eff_year,
	EXP_Default.sar_cov_eff_month,
	-- *INF*: LPAD(TO_CHAR(sar_cov_eff_month),2,'0')
	LPAD(TO_CHAR(sar_cov_eff_month
		), 2, '0'
	) AS v_sar_cov_eff_month,
	EXP_Default.sar_cov_eff_day,
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
	EXP_Default.sar_type_bureau,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_type_bureau)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_type_bureau
	) AS sar_type_bureau_out,
	'N/A' AS default_NA,
	-- *INF*: TO_DATE('12/31/2100 23:59:59' , 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	SYSDATE AS created_date,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id
	FROM EXP_Default
	LEFT JOIN LKP_Policy
	ON LKP_Policy.pol_key = EXP_Default.Pol_Key
),
LKP_TGT_V2_Coverage AS (
	SELECT
	cov_id,
	cov_ak_id,
	pol_ak_id,
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
	type_bureau_code,
	cov_eff_date
	FROM (
		SELECT C.cov_id                 AS cov_id,
		       C.crrnt_snpsht_flag      AS crrnt_snpsht_flag,
		       C.audit_id               AS audit_id,
		       C.eff_from_date          AS eff_from_date,
		       C.eff_to_date            AS eff_to_date,
		       C.source_sys_id          AS source_sys_id,
		       C.created_date           AS created_date,
		       C.modified_date          AS modified_date,
		       C.cov_ak_id              AS cov_ak_id,
		       RTRIM(C.type_bureau_code)       AS type_bureau_code,
		       C.pol_ak_id              AS pol_ak_id,
		       RTRIM(C.ins_line)               AS ins_line,
		       RTRIM(C.loc_unit_num)           AS loc_unit_num,
		       RTRIM(C.sub_loc_unit_num)       AS sub_loc_unit_num,
		       RTRIM(C.risk_unit_grp)          AS risk_unit_grp,
		       RTRIM(C.risk_unit_grp_seq_num)  AS risk_unit_grp_seq_num,
		       RTRIM(C.risk_unit)              AS risk_unit,
		       RTRIM(C.risk_unit_seq_num)      AS risk_unit_seq_num,
		       RTRIM(C.major_peril_code)       AS major_peril_code,
		       RTRIM(C.major_peril_seq_num)    AS major_peril_seq_num,
		       RTRIM(C.pms_type_exposure)      AS pms_type_exposure,
		       C.cov_eff_date           AS cov_eff_date
		FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.coverage C, @{pipeline().parameters.TARGET_TABLE_OWNER}.policy P 
		WHERE C.pol_ak_id = P.pol_ak_id
		AND  C.crrnt_snpsht_flag =1 and P.crrnt_snpsht_flag =1
		AND P.pol_key IN (SELECT DISTINCT (A.pif_symbol + A.pif_policy_number + A.pif_module) 
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}..pif_4514_stage A)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id,ins_line,loc_unit_num,sub_loc_unit_num,risk_unit_grp,risk_unit_grp_seq_num,risk_unit,risk_unit_seq_num,major_peril_code,major_peril_seq_num,pms_type_exposure,cov_eff_date ORDER BY cov_id DESC) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_TGT_V2_Coverage.cov_id AS lkp_cov_id,
	LKP_TGT_V2_Coverage.cov_ak_id AS lkp_cov_ak_id,
	LKP_TGT_V2_Coverage.pol_ak_id AS lkp_pol_ak_id,
	LKP_TGT_V2_Coverage.ins_line AS lkp_ins_line,
	LKP_TGT_V2_Coverage.loc_unit_num AS lkp_loc_unit_num,
	LKP_TGT_V2_Coverage.sub_loc_unit_num AS lkp_sub_loc_unit_num,
	LKP_TGT_V2_Coverage.risk_unit_grp AS lkp_risk_unit_grp,
	LKP_TGT_V2_Coverage.risk_unit_grp_seq_num AS lkp_risk_unit_grp_seq_num,
	LKP_TGT_V2_Coverage.risk_unit AS lkp_risk_unit,
	LKP_TGT_V2_Coverage.risk_unit_seq_num AS lkp_risk_unit_seq_num,
	LKP_TGT_V2_Coverage.major_peril_code AS lkp_major_peril_code,
	LKP_TGT_V2_Coverage.major_peril_seq_num AS lkp_major_peril_seq_num,
	LKP_TGT_V2_Coverage.pms_type_exposure AS lkp_pms_type_exposure,
	LKP_TGT_V2_Coverage.type_bureau_code AS lkp_type_bureau_code,
	LKP_TGT_V2_Coverage.cov_eff_date AS lkp_cov_eff_date,
	EXP_Values.pol_ak_id,
	EXP_Values.sar_insurance_line_Out,
	EXP_Values.sar_location_Out,
	EXP_Values.sar_sub_location_x1,
	EXP_Values.sar_risk_unit_group_Out,
	EXP_Values.risk_unit_group_seq_Out,
	EXP_Values.sar_risk_unit_Out,
	EXP_Values.sar_rsk_unit_seq_out,
	EXP_Values.sar_type_exposure_out,
	EXP_Values.sar_major_peril_out,
	EXP_Values.sar_major_peril_seq_no,
	EXP_Values.sar_cov_eff_date,
	EXP_Values.sar_type_bureau_out,
	-- *INF*: IIF(ISNULL(lkp_cov_id),'NEW',
	-- 	IIF (
	-- 
	-- lkp_type_bureau_code <>sar_type_bureau_out  ,
	-- 
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(lkp_cov_id IS NULL,
		'NEW',
		IFF(lkp_type_bureau_code <> sar_type_bureau_out,
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_Changed_Flag,
	v_Changed_Flag AS Changed_Flag,
	EXP_Values.default_NA,
	-- *INF*: IIF(v_Changed_Flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	SYSDATE)
	IFF(v_Changed_Flag = 'NEW',
		TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		SYSDATE
	) AS v_eff_from_date,
	v_eff_from_date AS eff_from_date,
	EXP_Values.eff_to_date,
	EXP_Values.created_date,
	EXP_Values.crrnt_snpsht_flag,
	EXP_Values.audit_id,
	EXP_Values.source_sys_id
	FROM EXP_Values
	LEFT JOIN LKP_TGT_V2_Coverage
	ON LKP_TGT_V2_Coverage.pol_ak_id = EXP_Values.pol_ak_id AND LKP_TGT_V2_Coverage.ins_line = EXP_Values.sar_insurance_line_Out AND LKP_TGT_V2_Coverage.loc_unit_num = EXP_Values.sar_location_Out AND LKP_TGT_V2_Coverage.sub_loc_unit_num = EXP_Values.sar_sub_location_x1 AND LKP_TGT_V2_Coverage.risk_unit_grp = EXP_Values.sar_risk_unit_group_Out AND LKP_TGT_V2_Coverage.risk_unit_grp_seq_num = EXP_Values.risk_unit_group_seq_Out AND LKP_TGT_V2_Coverage.risk_unit = EXP_Values.sar_risk_unit_Out AND LKP_TGT_V2_Coverage.risk_unit_seq_num = EXP_Values.sar_rsk_unit_seq_out AND LKP_TGT_V2_Coverage.major_peril_code = EXP_Values.sar_major_peril_out AND LKP_TGT_V2_Coverage.major_peril_seq_num = EXP_Values.sar_major_peril_seq_no AND LKP_TGT_V2_Coverage.pms_type_exposure = EXP_Values.sar_type_exposure_out AND LKP_TGT_V2_Coverage.cov_eff_date = EXP_Values.sar_cov_eff_date
),
FIL_Insert_rows AS (
	SELECT
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	created_date AS modified_date, 
	lkp_cov_ak_id, 
	pol_ak_id, 
	sar_insurance_line_Out AS ins_line, 
	sar_location_Out AS loc_unit_num, 
	sar_sub_location_x1 AS sub_loc_unit_num, 
	sar_risk_unit_group_Out AS risk_unit_grp, 
	risk_unit_group_seq_Out AS risk_unit_grp_seq_num, 
	sar_risk_unit_Out AS risk_unit, 
	sar_rsk_unit_seq_out AS risk_unit_seq_num, 
	sar_major_peril_out AS major_peril_code, 
	sar_major_peril_seq_no AS major_peril_seq_num, 
	sar_type_exposure_out AS pms_type_exposure, 
	sar_type_bureau_out AS type_bureau_code, 
	sar_cov_eff_date AS cov_eff_date, 
	Changed_Flag
	FROM EXP_Detect_Changes
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
SEQ_Cov_AK_ID AS (
	CREATE SEQUENCE SEQ_Cov_AK_ID
	START = 0
	INCREMENT = 1;
),
EXP_Detemine_AK_ID AS (
	SELECT
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	lkp_cov_ak_id,
	-- *INF*: IIF(ISNULL(lkp_cov_ak_id),NEXTVAL,lkp_cov_ak_id)
	IFF(lkp_cov_ak_id IS NULL,
		NEXTVAL,
		lkp_cov_ak_id
	) AS cov_ak_id_out,
	pol_ak_id,
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
	type_bureau_code,
	cov_eff_date,
	SEQ_Cov_AK_ID.NEXTVAL
	FROM FIL_Insert_rows
),
coverage_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.coverage
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, cov_ak_id, pol_ak_id, ins_line, loc_unit_num, sub_loc_unit_num, risk_unit_grp, risk_unit_grp_seq_num, risk_unit, risk_unit_seq_num, major_peril_code, major_peril_seq_num, pms_type_exposure, cov_eff_date, type_bureau_code)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	cov_ak_id_out AS COV_AK_ID, 
	POL_AK_ID, 
	INS_LINE, 
	LOC_UNIT_NUM, 
	SUB_LOC_UNIT_NUM, 
	RISK_UNIT_GRP, 
	RISK_UNIT_GRP_SEQ_NUM, 
	RISK_UNIT, 
	RISK_UNIT_SEQ_NUM, 
	MAJOR_PERIL_CODE, 
	MAJOR_PERIL_SEQ_NUM, 
	PMS_TYPE_EXPOSURE, 
	COV_EFF_DATE, 
	TYPE_BUREAU_CODE
	FROM EXP_Detemine_AK_ID
),
SQ_coverage AS (
	SELECT 
	a.cov_id,
	a.eff_from_date,
	a.eff_to_date,
	a.ins_line,
	a.loc_unit_num,
	a.sub_loc_unit_num,
	a.risk_unit_grp,
	a.risk_unit_grp_seq_num,
	a.risk_unit,
	a.risk_unit_seq_num,
	a.major_peril_code,
	a.major_peril_seq_num,
	a.pms_type_exposure,
	a.cov_eff_date
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.coverage a
	where a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	and EXISTS (SELECT 1			
		FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.coverage b
		WHERE b.crrnt_snpsht_flag = 1
	      AND a.cov_ak_id = b.cov_ak_id
	      AND a.source_sys_id = b.source_sys_id
		GROUP BY b.cov_ak_id
		HAVING COUNT(*) > 1)
	order by a.cov_ak_id, a.eff_from_date desc
),
EXP_Expire_Rows AS (
	SELECT
	cov_id,
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
	eff_from_date,
	eff_to_date AS Orig_eff_to_date,
	-- *INF*: DECODE (TRUE, 
	-- ins_line = v_PREV_ROW_ins_line  AND 
	-- loc_unit_num  = v_PREV_ROW_loc_unit_num  AND 
	-- sub_loc_unit_num  =  v_PREV_ROW_sub_loc_unit_num  AND  
	-- risk_unit_grp  =  v_PREV_ROW_risk_unit_grp  AND 
	-- risk_unit_grp_seq_num  = v_PREV_ROW_risk_unit_grp_seq_num  AND 
	-- risk_unit  = v_PREV_ROW_risk_unit  AND 
	-- risk_unit_seq_num  = v_PREV_ROW_risk_unit_seq_num  AND 
	-- major_peril_code  = v_PREV_ROW_major_peril_code  AND 
	-- major_peril_seq_num  = v_PEV_ROW_major_peril_seq_num  AND 
	-- pms_type_exposure  =  v_PEV_ROW_pms_type_exposure  AND 
	-- cov_eff_date  =  v_PEV_ROW_cov_eff_date
	-- 
	-- , ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1)
	-- ,Orig_eff_to_date)
	DECODE(TRUE,
		ins_line = v_PREV_ROW_ins_line 
		AND loc_unit_num = v_PREV_ROW_loc_unit_num 
		AND sub_loc_unit_num = v_PREV_ROW_sub_loc_unit_num 
		AND risk_unit_grp = v_PREV_ROW_risk_unit_grp 
		AND risk_unit_grp_seq_num = v_PREV_ROW_risk_unit_grp_seq_num 
		AND risk_unit = v_PREV_ROW_risk_unit 
		AND risk_unit_seq_num = v_PREV_ROW_risk_unit_seq_num 
		AND major_peril_code = v_PREV_ROW_major_peril_code 
		AND major_peril_seq_num = v_PEV_ROW_major_peril_seq_num 
		AND pms_type_exposure = v_PEV_ROW_pms_type_exposure 
		AND cov_eff_date = v_PEV_ROW_cov_eff_date, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
		Orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	0 AS crrnt_snapshot_flag,
	sysdate AS modified_date,
	ins_line AS v_PREV_ROW_ins_line,
	loc_unit_num AS v_PREV_ROW_loc_unit_num,
	sub_loc_unit_num AS v_PREV_ROW_sub_loc_unit_num,
	risk_unit_grp AS v_PREV_ROW_risk_unit_grp,
	risk_unit_grp_seq_num AS v_PREV_ROW_risk_unit_grp_seq_num,
	risk_unit AS v_PREV_ROW_risk_unit,
	risk_unit_seq_num AS v_PREV_ROW_risk_unit_seq_num,
	major_peril_code AS v_PREV_ROW_major_peril_code,
	major_peril_seq_num AS v_PEV_ROW_major_peril_seq_num,
	pms_type_exposure AS v_PEV_ROW_pms_type_exposure,
	cov_eff_date AS v_PEV_ROW_cov_eff_date,
	eff_from_date AS v_PREV_ROW_eff_from_date
	FROM SQ_coverage
),
FIL_Coverage AS (
	SELECT
	cov_id, 
	Orig_eff_to_date AS orig_eff_to_date, 
	eff_to_date, 
	crrnt_snapshot_flag, 
	modified_date
	FROM EXP_Expire_Rows
	WHERE orig_eff_to_date != eff_to_date
),
UPD_Update_Target AS (
	SELECT
	cov_id, 
	eff_to_date, 
	crrnt_snapshot_flag AS crrnt_snapsht_flag, 
	modified_date
	FROM FIL_Coverage
),
coverage_EXPIRE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.coverage AS T
	USING UPD_Update_Target AS S
	ON T.cov_id = S.cov_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snapsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),
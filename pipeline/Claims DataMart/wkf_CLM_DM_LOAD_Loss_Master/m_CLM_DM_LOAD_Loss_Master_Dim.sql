WITH
SQ_loss_master_calculation AS (
	SELECT DISTINCT
	rtrim(A.variation_code),
	A.pol_type, 
	rtrim(A.auto_reins_facility), 
	rtrim(A.statistical_brkdwn_line), 
	A.statistical_code1,
	A.statistical_code2, 
	A.statistical_code3, 
	rtrim(A.statistical_line), 
	rtrim(A.loss_master_cov_code), 
	rtrim(A.risk_state_prov_code), 
	rtrim(A.risk_zip_code), 
	rtrim(A.terr_code), 
	A.tax_loc, 
	rtrim(A.class_code), 
	A.exposure,
	rtrim(A.sub_line_code), 
	rtrim(A.source_sar_asl),
	rtrim(A.source_sar_prdct_line), 
	A.source_sar_sp_use_code
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}. loss_master_calculation A
	WHERE 
	A.created_date > '@{pipeline().parameters.SELECTION_START_TS}' 
	@{pipeline().parameters.WHERECLAUSE}
),
EXP_Source AS (
	SELECT
	risk_state_prov_code,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(risk_state_prov_code),'N/A',
	-- IS_SPACES(risk_state_prov_code),'N/A',
	-- LENGTH(risk_state_prov_code)=0,'N/A',
	-- LTRIM(RTRIM(risk_state_prov_code)))
	DECODE(TRUE,
	risk_state_prov_code IS NULL, 'N/A',
	IS_SPACES(risk_state_prov_code), 'N/A',
	LENGTH(risk_state_prov_code) = 0, 'N/A',
	LTRIM(RTRIM(risk_state_prov_code))) AS risk_state_prov_code_lkp,
	risk_zip_code,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(risk_zip_code),'N/A',
	-- IS_SPACES(risk_zip_code),'N/A',
	-- LENGTH(risk_zip_code)=0,'N/A',
	-- LTRIM(RTRIM(risk_zip_code)))
	DECODE(TRUE,
	risk_zip_code IS NULL, 'N/A',
	IS_SPACES(risk_zip_code), 'N/A',
	LENGTH(risk_zip_code) = 0, 'N/A',
	LTRIM(RTRIM(risk_zip_code))) AS risk_zip_code_lkp,
	terr_code,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(terr_code),'N/A',
	-- IS_SPACES(terr_code),'N/A',
	-- LENGTH(terr_code)=0,'N/A',
	-- LTRIM(RTRIM(terr_code)))
	DECODE(TRUE,
	terr_code IS NULL, 'N/A',
	IS_SPACES(terr_code), 'N/A',
	LENGTH(terr_code) = 0, 'N/A',
	LTRIM(RTRIM(terr_code))) AS terr_code_lkp,
	tax_loc,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRING_NUMERIC(tax_loc)
	:UDF.DEFAULT_VALUE_FOR_STRING_NUMERIC(tax_loc) AS tax_loc_lkp,
	-- *INF*: IIF(ISNULL(tax_loc) OR IS_SPACES(tax_loc) OR LENGTH(tax_loc)=0 OR NOT IS_NUMBER(LTRIM(RTRIM(tax_loc))),'N/A',
	-- tax_loc)
	-- 
	-- -- we are not using LTRIM ,RTRIM  functions because we need to spaces as it is as they are used for IBS Bureau Reporting.
	IFF(tax_loc IS NULL OR IS_SPACES(tax_loc) OR LENGTH(tax_loc) = 0 OR NOT IS_NUMBER(LTRIM(RTRIM(tax_loc))), 'N/A', tax_loc) AS tax_loc_out,
	class_code,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(class_code),'N/A',
	-- IS_SPACES(class_code),'N/A',
	-- LENGTH(class_code)=0,'N/A',
	-- LTRIM(RTRIM(class_code)))
	DECODE(TRUE,
	class_code IS NULL, 'N/A',
	IS_SPACES(class_code), 'N/A',
	LENGTH(class_code) = 0, 'N/A',
	LTRIM(RTRIM(class_code))) AS class_code_lkp,
	exposure,
	sub_line_code,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(sub_line_code),'N/A',
	-- IS_SPACES(sub_line_code),'N/A',
	-- LENGTH(sub_line_code)=0,'N/A',
	-- LTRIM(RTRIM(sub_line_code)))
	DECODE(TRUE,
	sub_line_code IS NULL, 'N/A',
	IS_SPACES(sub_line_code), 'N/A',
	LENGTH(sub_line_code) = 0, 'N/A',
	LTRIM(RTRIM(sub_line_code))) AS sub_line_code_lkp,
	source_sar_asl,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(source_sar_asl),'N/A',
	-- IS_SPACES(source_sar_asl),'N/A',
	-- LENGTH(source_sar_asl)=0,'N/A',
	-- LTRIM(RTRIM(REPLACECHR(TRUE, source_sar_asl, '.' , ''))))
	-- 
	-- 
	DECODE(TRUE,
	source_sar_asl IS NULL, 'N/A',
	IS_SPACES(source_sar_asl), 'N/A',
	LENGTH(source_sar_asl) = 0, 'N/A',
	LTRIM(RTRIM(REPLACECHR(TRUE, source_sar_asl, '.', '')))) AS source_sar_asl_lkp,
	source_sar_prdct_line,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(source_sar_prdct_line),'N/A',
	-- IS_SPACES(source_sar_prdct_line),'N/A',
	-- LENGTH(source_sar_prdct_line)=0,'N/A',
	-- LTRIM(RTRIM(source_sar_prdct_line)))
	DECODE(TRUE,
	source_sar_prdct_line IS NULL, 'N/A',
	IS_SPACES(source_sar_prdct_line), 'N/A',
	LENGTH(source_sar_prdct_line) = 0, 'N/A',
	LTRIM(RTRIM(source_sar_prdct_line))) AS source_sar_prdct_line_lkp,
	source_sar_sp_use_code,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(source_sar_sp_use_code),'N/A',
	-- IS_SPACES(source_sar_sp_use_code),'N/A',
	-- LENGTH(source_sar_sp_use_code)=0,'N/A',
	-- LTRIM(RTRIM(source_sar_sp_use_code)))
	DECODE(TRUE,
	source_sar_sp_use_code IS NULL, 'N/A',
	IS_SPACES(source_sar_sp_use_code), 'N/A',
	LENGTH(source_sar_sp_use_code) = 0, 'N/A',
	LTRIM(RTRIM(source_sar_sp_use_code))) AS source_sar_sp_use_code_lkp,
	-- *INF*: IIF(ISNULL(source_sar_sp_use_code) OR IS_SPACES(source_sar_sp_use_code) OR LENGTH(source_sar_sp_use_code)=0,'N/A',
	-- source_sar_sp_use_code)
	-- 
	-- -- we are not using LTRIM ,RTRIM  functions because we need to spaces as it is as they are used for IBS Bureau Reporting.
	IFF(source_sar_sp_use_code IS NULL OR IS_SPACES(source_sar_sp_use_code) OR LENGTH(source_sar_sp_use_code) = 0, 'N/A', source_sar_sp_use_code) AS source_sar_sp_use_code_Out,
	statistical_line AS source_statistical_line,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(source_statistical_line),'N/A',
	-- IS_SPACES(source_statistical_line),'N/A',
	-- LENGTH(source_statistical_line)=0,'N/A',
	-- LTRIM(RTRIM(source_statistical_line)))
	DECODE(TRUE,
	source_statistical_line IS NULL, 'N/A',
	IS_SPACES(source_statistical_line), 'N/A',
	LENGTH(source_statistical_line) = 0, 'N/A',
	LTRIM(RTRIM(source_statistical_line))) AS source_statistical_line_lkp,
	variation_code,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(variation_code),'N/A',
	-- IS_SPACES(variation_code),'N/A',
	-- LENGTH(variation_code)=0,'N/A',
	-- LTRIM(RTRIM(variation_code)))
	DECODE(TRUE,
	variation_code IS NULL, 'N/A',
	IS_SPACES(variation_code), 'N/A',
	LENGTH(variation_code) = 0, 'N/A',
	LTRIM(RTRIM(variation_code))) AS variation_code_lkp,
	pol_type,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(pol_type),'N/A',
	-- IS_SPACES(pol_type),'N/A',
	-- LENGTH(pol_type)=0,'N/A',
	-- LTRIM(RTRIM(pol_type)))
	DECODE(TRUE,
	pol_type IS NULL, 'N/A',
	IS_SPACES(pol_type), 'N/A',
	LENGTH(pol_type) = 0, 'N/A',
	LTRIM(RTRIM(pol_type))) AS pol_type_lkp,
	-- *INF*: IIF(ISNULL(pol_type) OR IS_SPACES(pol_type) OR LENGTH(pol_type)=0,'N/A',
	-- pol_type)
	-- 
	-- -- we are not using LTRIM ,RTRIM  functions because we need to spaces as it is as they are used for IBS Bureau Reporting.
	IFF(pol_type IS NULL OR IS_SPACES(pol_type) OR LENGTH(pol_type) = 0, 'N/A', pol_type) AS pol_type_out,
	auto_reins_facility,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(auto_reins_facility),'N/A',
	-- IS_SPACES(auto_reins_facility),'N/A',
	-- LENGTH(auto_reins_facility)=0,'N/A',
	-- LTRIM(RTRIM(auto_reins_facility)))
	DECODE(TRUE,
	auto_reins_facility IS NULL, 'N/A',
	IS_SPACES(auto_reins_facility), 'N/A',
	LENGTH(auto_reins_facility) = 0, 'N/A',
	LTRIM(RTRIM(auto_reins_facility))) AS auto_reins_facility_lkp,
	statistical_brkdwn_line,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(statistical_brkdwn_line),'N/A',
	-- IS_SPACES(statistical_brkdwn_line),'N/A',
	-- LENGTH(statistical_brkdwn_line)=0,'N/A',
	-- LTRIM(RTRIM(statistical_brkdwn_line)))
	DECODE(TRUE,
	statistical_brkdwn_line IS NULL, 'N/A',
	IS_SPACES(statistical_brkdwn_line), 'N/A',
	LENGTH(statistical_brkdwn_line) = 0, 'N/A',
	LTRIM(RTRIM(statistical_brkdwn_line))) AS statistical_brkdwn_line_lkp,
	statistical_code1,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(statistical_code1),'N/A',
	-- IS_SPACES(statistical_code1),'N/A',
	-- LENGTH(statistical_code1)=0,'N/A',
	-- LTRIM(RTRIM(statistical_code1)))
	-- 
	-- -- We are using LTRIM ,RTRIM  functions to match on  Target lookup values, since these are string fields
	DECODE(TRUE,
	statistical_code1 IS NULL, 'N/A',
	IS_SPACES(statistical_code1), 'N/A',
	LENGTH(statistical_code1) = 0, 'N/A',
	LTRIM(RTRIM(statistical_code1))) AS statistical_code1_lkp,
	-- *INF*: IIF(ISNULL(statistical_code1) OR IS_SPACES(statistical_code1) OR LENGTH(statistical_code1)=0,'N/A',
	-- statistical_code1)
	-- 
	-- -- we are not using LTRIM ,RTRIM  functions because we need to spaces as it is as they are used for IBS Bureau Reporting.
	IFF(statistical_code1 IS NULL OR IS_SPACES(statistical_code1) OR LENGTH(statistical_code1) = 0, 'N/A', statistical_code1) AS statistical_code1_Out,
	statistical_code2,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(statistical_code2),'N/A',
	-- IS_SPACES(statistical_code2),'N/A',
	-- LENGTH(statistical_code2)=0,'N/A',
	-- LTRIM(RTRIM(statistical_code2)))
	-- 
	-- 
	-- -- We are using LTRIM ,RTRIM  functions to match on  Target lookup values, since these are string fields
	DECODE(TRUE,
	statistical_code2 IS NULL, 'N/A',
	IS_SPACES(statistical_code2), 'N/A',
	LENGTH(statistical_code2) = 0, 'N/A',
	LTRIM(RTRIM(statistical_code2))) AS statistical_code2_lkp,
	-- *INF*: IIF(ISNULL(statistical_code2) OR IS_SPACES(statistical_code2) OR LENGTH(statistical_code2)=0,'N/A',
	-- statistical_code2)
	-- 
	-- 
	-- -- we are not using LTRIM ,RTRIM  functions because we need to spaces as it is as they are used for IBS Bureau Reporting.
	IFF(statistical_code2 IS NULL OR IS_SPACES(statistical_code2) OR LENGTH(statistical_code2) = 0, 'N/A', statistical_code2) AS statistical_code2_Out,
	statistical_code3,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(statistical_code3),'N/A',
	-- IS_SPACES(statistical_code3),'N/A',
	-- LENGTH(statistical_code3)=0,'N/A',
	-- LTRIM(RTRIM(statistical_code3))) 
	-- 
	-- -- We are using LTRIM ,RTRIM  functions to match on  Target lookup values, since these are string fields
	DECODE(TRUE,
	statistical_code3 IS NULL, 'N/A',
	IS_SPACES(statistical_code3), 'N/A',
	LENGTH(statistical_code3) = 0, 'N/A',
	LTRIM(RTRIM(statistical_code3))) AS statistical_code3_lkp,
	-- *INF*: IIF(ISNULL(statistical_code3) OR IS_SPACES(statistical_code3) OR LENGTH(statistical_code3)=0,'N/A',
	-- statistical_code3)
	-- 
	-- 
	-- -- we are not using LTRIM ,RTRIM  functions because we need to spaces as it is as they are used for IBS Bureau Reporting.
	IFF(statistical_code3 IS NULL OR IS_SPACES(statistical_code3) OR LENGTH(statistical_code3) = 0, 'N/A', statistical_code3) AS statistical_code3_Out,
	loss_master_cov_code,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(loss_master_cov_code),'N/A',
	-- IS_SPACES(loss_master_cov_code),'N/A',
	-- LENGTH(loss_master_cov_code)=0,'N/A',
	-- LTRIM(RTRIM(loss_master_cov_code)))
	DECODE(TRUE,
	loss_master_cov_code IS NULL, 'N/A',
	IS_SPACES(loss_master_cov_code), 'N/A',
	LENGTH(loss_master_cov_code) = 0, 'N/A',
	LTRIM(RTRIM(loss_master_cov_code))) AS loss_master_cov_code_lkp
	FROM SQ_loss_master_calculation
),
LKP_loss_master_dim AS (
	SELECT
	loss_master_dim_id,
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
	source_statistical_line,
	variation_code,
	pol_type,
	auto_reins_facility,
	statistical_brkdwn_line,
	statistical_code1,
	statistical_code2,
	statistical_code3,
	loss_master_cov_code
	FROM (
		SELECT loss_master_dim_id      AS loss_master_dim_id,
		       LTRIM(RTRIM(risk_state_prov_code))    AS risk_state_prov_code,
		       LTRIM(RTRIM(risk_zip_code))           AS risk_zip_code,
		       LTRIM(RTRIM(terr_code))               AS terr_code,
		       LTRIM(RTRIM(tax_loc))                 AS tax_loc,
		       LTRIM(RTRIM(class_code))              AS class_code,
		       exposure                AS exposure,
		       LTRIM(RTRIM(sub_line_code))           AS sub_line_code,
		       LTRIM(RTRIM(source_sar_asl))          AS source_sar_asl,
		       LTRIM(RTRIM(source_sar_prdct_line))   AS source_sar_prdct_line,
		       LTRIM(RTRIM(source_sar_sp_use_code))  AS source_sar_sp_use_code,
		       LTRIM(RTRIM(source_statistical_line)) AS source_statistical_line,
		       LTRIM(RTRIM(variation_code))          AS variation_code,
		       LTRIM(RTRIM(pol_type))                AS pol_type,
		       LTRIM(RTRIM(auto_reins_facility))     AS auto_reins_facility,
		       LTRIM(RTRIM(statistical_brkdwn_line)) AS statistical_brkdwn_line,
		       LTRIM(RTRIM(statistical_code1))       AS statistical_code1,
		       LTRIM(RTRIM(statistical_code2))       AS statistical_code2,
		       LTRIM(RTRIM(statistical_code3))       AS statistical_code3,
		       LTRIM(RTRIM(loss_master_cov_code))    AS loss_master_cov_code
		FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY risk_state_prov_code,risk_zip_code,terr_code,tax_loc,class_code,exposure,sub_line_code,source_sar_asl,source_sar_prdct_line,source_sar_sp_use_code,source_statistical_line,variation_code,pol_type,auto_reins_facility,statistical_brkdwn_line,statistical_code1,statistical_code2,statistical_code3,loss_master_cov_code ORDER BY loss_master_dim_id DESC) = 1
),
EXP_Values AS (
	SELECT
	LKP_loss_master_dim.loss_master_dim_id AS lkp_loss_master_dim_id,
	EXP_Source.risk_state_prov_code_lkp AS risk_state_prov_code,
	EXP_Source.risk_zip_code_lkp AS risk_zip_code,
	EXP_Source.terr_code_lkp AS terr_code,
	EXP_Source.tax_loc_out AS tax_loc,
	EXP_Source.class_code_lkp AS class_code,
	EXP_Source.exposure,
	EXP_Source.sub_line_code_lkp AS sub_line_code,
	EXP_Source.source_sar_asl_lkp AS source_sar_asl,
	EXP_Source.source_sar_prdct_line_lkp AS source_sar_prdct_line,
	EXP_Source.source_sar_sp_use_code_Out AS source_sar_sp_use_code,
	EXP_Source.source_statistical_line_lkp AS source_statistical_line,
	EXP_Source.variation_code_lkp AS variation_code,
	EXP_Source.pol_type_out AS pol_type,
	EXP_Source.auto_reins_facility_lkp AS auto_reins_facility,
	EXP_Source.statistical_brkdwn_line_lkp AS statistical_brkdwn_line,
	EXP_Source.statistical_code1_Out AS statistical_code1,
	EXP_Source.statistical_code2_Out AS statistical_code2,
	EXP_Source.statistical_code3_Out AS statistical_code3,
	EXP_Source.loss_master_cov_code_lkp AS loss_master_cov_code
	FROM EXP_Source
	LEFT JOIN LKP_loss_master_dim
	ON LKP_loss_master_dim.risk_state_prov_code = EXP_Source.risk_state_prov_code_lkp AND LKP_loss_master_dim.risk_zip_code = EXP_Source.risk_zip_code_lkp AND LKP_loss_master_dim.terr_code = EXP_Source.terr_code_lkp AND LKP_loss_master_dim.tax_loc = EXP_Source.tax_loc_lkp AND LKP_loss_master_dim.class_code = EXP_Source.class_code_lkp AND LKP_loss_master_dim.exposure = EXP_Source.exposure AND LKP_loss_master_dim.sub_line_code = EXP_Source.sub_line_code_lkp AND LKP_loss_master_dim.source_sar_asl = EXP_Source.source_sar_asl_lkp AND LKP_loss_master_dim.source_sar_prdct_line = EXP_Source.source_sar_prdct_line_lkp AND LKP_loss_master_dim.source_sar_sp_use_code = EXP_Source.source_sar_sp_use_code_lkp AND LKP_loss_master_dim.source_statistical_line = EXP_Source.source_statistical_line_lkp AND LKP_loss_master_dim.variation_code = EXP_Source.variation_code_lkp AND LKP_loss_master_dim.pol_type = EXP_Source.pol_type_lkp AND LKP_loss_master_dim.auto_reins_facility = EXP_Source.auto_reins_facility_lkp AND LKP_loss_master_dim.statistical_brkdwn_line = EXP_Source.statistical_brkdwn_line_lkp AND LKP_loss_master_dim.statistical_code1 = EXP_Source.statistical_code1_lkp AND LKP_loss_master_dim.statistical_code2 = EXP_Source.statistical_code2_lkp AND LKP_loss_master_dim.statistical_code3 = EXP_Source.statistical_code3_lkp AND LKP_loss_master_dim.loss_master_cov_code = EXP_Source.loss_master_cov_code_lkp
),
RTR_Insert_Update AS (
	SELECT
	lkp_loss_master_dim_id,
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
	source_statistical_line,
	variation_code,
	pol_type,
	auto_reins_facility,
	statistical_brkdwn_line,
	statistical_code1,
	statistical_code2,
	statistical_code3,
	loss_master_cov_code
	FROM EXP_Values
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE IIF(ISNULL(lkp_loss_master_dim_id),TRUE,FALSE)),
RTR_Insert_Update_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE IIF(NOT ISNULL(lkp_loss_master_dim_id),TRUE,FALSE)),
UPD_Update AS (
	SELECT
	lkp_loss_master_dim_id AS lkp_loss_master_dim_id3, 
	risk_state_prov_code AS risk_state_prov_code3, 
	risk_zip_code AS risk_zip_code3, 
	terr_code AS terr_code3, 
	tax_loc AS tax_loc3, 
	class_code AS class_code3, 
	exposure AS exposure3, 
	sub_line_code AS sub_line_code3, 
	source_sar_asl AS source_sar_asl3, 
	source_sar_prdct_line AS source_sar_prdct_line3, 
	source_sar_sp_use_code AS source_sar_sp_use_code3, 
	source_statistical_line AS source_statistical_line3, 
	variation_code AS variation_code3, 
	pol_type AS pol_type3, 
	auto_reins_facility AS auto_reins_facility3, 
	statistical_brkdwn_line AS statistical_brkdwn_line3, 
	statistical_code1 AS statistical_code13, 
	statistical_code2 AS statistical_code23, 
	statistical_code AS statistical_code33, 
	loss_master_cov_code AS loss_master_cov_code3
	FROM RTR_Insert_Update_UPDATE
),
loss_master_dim_Update AS (
	MERGE INTO loss_master_dim AS T
	USING UPD_Update AS S
	ON T.loss_master_dim_id = S.lkp_loss_master_dim_id3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.risk_state_prov_code = S.risk_state_prov_code3, T.risk_zip_code = S.risk_zip_code3, T.terr_code = S.terr_code3, T.tax_loc = S.tax_loc3, T.class_code = S.class_code3, T.exposure = S.exposure3, T.sub_line_code = S.sub_line_code3, T.source_sar_asl = S.source_sar_asl3, T.source_sar_prdct_line = S.source_sar_prdct_line3, T.source_sar_sp_use_code = S.source_sar_sp_use_code3, T.source_statistical_line = S.source_statistical_line3, T.variation_code = S.variation_code3, T.pol_type = S.pol_type3, T.auto_reins_facility = S.auto_reins_facility3, T.statistical_brkdwn_line = S.statistical_brkdwn_line3, T.statistical_code1 = S.statistical_code13, T.statistical_code2 = S.statistical_code23, T.statistical_code3 = S.statistical_code33, T.loss_master_cov_code = S.loss_master_cov_code3
),
EXP_Target AS (
	SELECT
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
	source_statistical_line,
	variation_code,
	pol_type,
	auto_reins_facility,
	statistical_brkdwn_line,
	statistical_code AS statistical_code1,
	statistical_code2,
	statistical_code3,
	loss_master_cov_code,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM RTR_Insert_Update_INSERT
),
loss_master_dim_Insert AS (
	INSERT INTO loss_master_dim
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date, risk_state_prov_code, risk_zip_code, terr_code, tax_loc, class_code, exposure, sub_line_code, source_sar_asl, source_sar_prdct_line, source_sar_sp_use_code, source_statistical_line, variation_code, pol_type, auto_reins_facility, statistical_brkdwn_line, statistical_code1, statistical_code2, statistical_code3, loss_master_cov_code)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	RISK_STATE_PROV_CODE, 
	RISK_ZIP_CODE, 
	TERR_CODE, 
	TAX_LOC, 
	CLASS_CODE, 
	EXPOSURE, 
	SUB_LINE_CODE, 
	SOURCE_SAR_ASL, 
	SOURCE_SAR_PRDCT_LINE, 
	SOURCE_SAR_SP_USE_CODE, 
	SOURCE_STATISTICAL_LINE, 
	VARIATION_CODE, 
	POL_TYPE, 
	AUTO_REINS_FACILITY, 
	STATISTICAL_BRKDWN_LINE, 
	STATISTICAL_CODE1, 
	STATISTICAL_CODE2, 
	STATISTICAL_CODE3, 
	LOSS_MASTER_COV_CODE
	FROM EXP_Target
),
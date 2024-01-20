WITH
lkp_42gj_stage AS (
	SELECT
	pif_42gj_stage_id,
	pif_symbol,
	pif_policy_number,
	pif_module,
	ipfc4j_loss_year,
	ipfc4j_loss_month,
	ipfc4j_loss_day,
	ipfc4j_loss_occurence,
	ipfc4j_loss_claimant,
	ipfc4j_use_code
	FROM (
		SELECT 
			pif_42gj_stage_id,
			pif_symbol,
			pif_policy_number,
			pif_module,
			ipfc4j_loss_year,
			ipfc4j_loss_month,
			ipfc4j_loss_day,
			ipfc4j_loss_occurence,
			ipfc4j_loss_claimant,
			ipfc4j_use_code
		FROM pif_42gj_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module,ipfc4j_loss_year,ipfc4j_loss_month,ipfc4j_loss_day,ipfc4j_loss_occurence,ipfc4j_loss_claimant,ipfc4j_use_code ORDER BY pif_42gj_stage_id) = 1
),
SQ_CLM_STG_Update_42GJ AS (
	SELECT pif_4578_stage.pif_symbol, pif_4578_stage.pif_policy_number, pif_4578_stage.pif_module, pif_4578_stage.loss_year, pif_4578_stage.loss_month, pif_4578_stage.loss_day, pif_4578_stage.loss_occurence, pif_4578_stage.loss_claimant, MAX(pif_4578_stage.logical_flag) , pif_4578_stage.source_system_id 
	FROM
	pif_4578_stage 
	GROUP BY pif_4578_stage.pif_symbol, pif_4578_stage.pif_policy_number, pif_4578_stage.pif_module, pif_4578_stage.loss_year, pif_4578_stage.loss_month, pif_4578_stage.loss_day, pif_4578_stage.loss_occurence, pif_4578_stage.loss_claimant, pif_4578_stage.source_system_id
),
EXP_CLM_STG_Update_42GJ AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module,
	loss_year,
	loss_month,
	loss_day,
	loss_occurence,
	loss_claimant,
	-- *INF*: :LKP.LKP_42GJ_STAGE(pif_symbol,pif_policy_number,pif_module,loss_year,loss_month,loss_day,loss_occurence,loss_claimant,'CMT')
	LKP_42GJ_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence_loss_claimant_CMT.pif_42gj_stage_id AS pif_42gj_stage_id,
	logical_flag,
	-- *INF*: DECODE(logical_flag,
	-- '0','1',
	-- '-1','2',
	-- '-2','3',
	-- '-3','4')
	DECODE(
	    logical_flag,
	    '0', '1',
	    '-1', '2',
	    '-2', '3',
	    '-3', '4'
	) AS logical_flag_insert,
	source_system_id
	FROM SQ_CLM_STG_Update_42GJ
	LEFT JOIN LKP_42GJ_STAGE LKP_42GJ_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence_loss_claimant_CMT
	ON LKP_42GJ_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence_loss_claimant_CMT.pif_symbol = pif_symbol
	AND LKP_42GJ_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence_loss_claimant_CMT.pif_policy_number = pif_policy_number
	AND LKP_42GJ_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence_loss_claimant_CMT.pif_module = pif_module
	AND LKP_42GJ_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence_loss_claimant_CMT.ipfc4j_loss_year = loss_year
	AND LKP_42GJ_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence_loss_claimant_CMT.ipfc4j_loss_month = loss_month
	AND LKP_42GJ_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence_loss_claimant_CMT.ipfc4j_loss_day = loss_day
	AND LKP_42GJ_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence_loss_claimant_CMT.ipfc4j_loss_occurence = loss_occurence
	AND LKP_42GJ_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence_loss_claimant_CMT.ipfc4j_loss_claimant = loss_claimant
	AND LKP_42GJ_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence_loss_claimant_CMT.ipfc4j_use_code = 'CMT'

),
RTR_CLM_STG_Update AS (
	SELECT
	pif_42gj_stage_id,
	pif_symbol,
	pif_policy_number,
	pif_module,
	loss_year,
	loss_month,
	loss_day,
	loss_occurence,
	loss_claimant,
	logical_flag,
	logical_flag_insert,
	source_system_id
	FROM EXP_CLM_STG_Update_42GJ
),
RTR_CLM_STG_Update_INSERT AS (SELECT * FROM RTR_CLM_STG_Update WHERE ISNULL(pif_42gj_stage_id)),
RTR_CLM_STG_Update_UPDATE AS (SELECT * FROM RTR_CLM_STG_Update WHERE NOT ISNULL(pif_42gj_stage_id)),
EXP_CLM_STG_Insert AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module,
	loss_year,
	loss_month,
	loss_day,
	loss_occurence,
	loss_claimant,
	'CMT' AS ipfc4j_use_code,
	logical_flag_insert AS logical_flag_op_insert,
	sysdate AS extract_date,
	sysdate AS as_of_date,
	source_system_id
	FROM RTR_CLM_STG_Update_INSERT
),
UPD_CLM_STG_Insert AS (
	SELECT
	pif_symbol, 
	pif_policy_number, 
	pif_module, 
	loss_year, 
	loss_month, 
	loss_day, 
	loss_occurence, 
	loss_claimant, 
	ipfc4j_use_code, 
	logical_flag_op_insert, 
	extract_date, 
	as_of_date, 
	source_system_id
	FROM EXP_CLM_STG_Insert
),
PIF_42GJ_stage_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PIF_42GJ_stage
	(pif_symbol, pif_policy_number, pif_module, ipfc4j_loss_year, ipfc4j_loss_month, ipfc4j_loss_day, ipfc4j_loss_occurence, ipfc4j_loss_claimant, ipfc4j_use_code, logical_flag, extract_date, as_of_date, source_system_id)
	SELECT 
	PIF_SYMBOL, 
	PIF_POLICY_NUMBER, 
	PIF_MODULE, 
	loss_year AS IPFC4J_LOSS_YEAR, 
	loss_month AS IPFC4J_LOSS_MONTH, 
	loss_day AS IPFC4J_LOSS_DAY, 
	loss_occurence AS IPFC4J_LOSS_OCCURENCE, 
	loss_claimant AS IPFC4J_LOSS_CLAIMANT, 
	IPFC4J_USE_CODE, 
	logical_flag_op_insert AS LOGICAL_FLAG, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	SOURCE_SYSTEM_ID
	FROM UPD_CLM_STG_Insert
),
EXP_CLM_STG_Update AS (
	SELECT
	pif_42gj_stage_id AS pif_42gj_stage_id3,
	logical_flag
	FROM RTR_CLM_STG_Update_UPDATE
),
UPD_CLM_STG_Update AS (
	SELECT
	pif_42gj_stage_id3, 
	logical_flag
	FROM EXP_CLM_STG_Update
),
PIF_42GJ_stage_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PIF_42GJ_stage AS T
	USING UPD_CLM_STG_Update AS S
	ON T.pif_42gj_stage_id = S.pif_42gj_stage_id3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.logical_flag = S.logical_flag
),
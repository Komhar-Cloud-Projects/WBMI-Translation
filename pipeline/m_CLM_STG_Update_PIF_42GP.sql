WITH
lkp_42gp_stage AS (
	SELECT
	pif_42gp_stage_id,
	ipfcgp_month_of_loss,
	ipfcgp_day_of_loss,
	ipfcgp_loss_occurence,
	pif_symbol,
	pif_policy_number,
	pif_module,
	ipfcgp_year_of_loss
	FROM (
		SELECT 
			pif_42gp_stage_id,
			ipfcgp_month_of_loss,
			ipfcgp_day_of_loss,
			ipfcgp_loss_occurence,
			pif_symbol,
			pif_policy_number,
			pif_module,
			ipfcgp_year_of_loss
		FROM pif_42gp_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module,ipfcgp_year_of_loss,ipfcgp_month_of_loss,ipfcgp_day_of_loss,ipfcgp_loss_occurence ORDER BY pif_42gp_stage_id) = 1
),
SQ_CLM_STG_Update_42GP AS (
	SELECT     pif_4578_stage.pif_symbol, pif_4578_stage.pif_policy_number, pif_4578_stage.pif_module, 
	                      pif_4578_stage.loss_year, pif_4578_stage.loss_month, pif_4578_stage.loss_day, pif_4578_stage.loss_occurence, MAX(pif_4578_stage.logical_flag) , 
	                      pif_4578_stage.source_system_id
	FROM         pif_4578_stage
	GROUP BY pif_4578_stage.pif_symbol, pif_4578_stage.pif_policy_number, pif_4578_stage.pif_module, 
	                      pif_4578_stage.loss_year, pif_4578_stage.loss_month, pif_4578_stage.loss_day, pif_4578_stage.loss_occurence,  
	                      pif_4578_stage.source_system_id
),
EXP_CLM_STG_Update_42GP AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module,
	loss_year,
	loss_month,
	loss_day,
	loss_occurence,
	-- *INF*: :LKP.LKP_42GP_STAGE(pif_symbol, pif_policy_number, pif_module, loss_year, loss_month, loss_day, loss_occurence)
	LKP_42GP_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence.pif_42gp_stage_id AS pif_42gp_stage_id,
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
	FROM SQ_CLM_STG_Update_42GP
	LEFT JOIN LKP_42GP_STAGE LKP_42GP_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence
	ON LKP_42GP_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence.pif_symbol = pif_symbol
	AND LKP_42GP_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence.pif_policy_number = pif_policy_number
	AND LKP_42GP_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence.pif_module = pif_module
	AND LKP_42GP_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence.ipfcgp_year_of_loss = loss_year
	AND LKP_42GP_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence.ipfcgp_month_of_loss = loss_month
	AND LKP_42GP_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence.ipfcgp_day_of_loss = loss_day
	AND LKP_42GP_STAGE_pif_symbol_pif_policy_number_pif_module_loss_year_loss_month_loss_day_loss_occurence.ipfcgp_loss_occurence = loss_occurence

),
RTR_CLM_STG_Update AS (
	SELECT
	pif_42gp_stage_id,
	pif_symbol,
	pif_policy_number,
	pif_module,
	loss_year,
	loss_month,
	loss_day,
	loss_occurence,
	logical_flag,
	logical_flag_insert,
	source_system_id
	FROM EXP_CLM_STG_Update_42GP
),
RTR_CLM_STG_Update_INSERT AS (SELECT * FROM RTR_CLM_STG_Update WHERE ISNULL(pif_42gp_stage_id)),
RTR_CLM_STG_Update_UPDATE AS (SELECT * FROM RTR_CLM_STG_Update WHERE NOT ISNULL(pif_42gp_stage_id)),
EXP_CLM_STG_Update AS (
	SELECT
	pif_42gp_stage_id,
	logical_flag
	FROM RTR_CLM_STG_Update_UPDATE
),
UPD_42GP_STAGE_UPDATE AS (
	SELECT
	pif_42gp_stage_id, 
	logical_flag
	FROM EXP_CLM_STG_Update
),
PIF_42GP_STAGE_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PIF_42GP_STAGE AS T
	USING UPD_42GP_STAGE_UPDATE AS S
	ON T.pif_42gp_stage_id = S.pif_42gp_stage_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.logical_flag = S.logical_flag
),
EXP_CLM_STG_Insert AS (
	SELECT
	pif_42gp_stage_id,
	pif_symbol,
	pif_policy_number,
	pif_module,
	loss_year,
	loss_month,
	loss_day,
	loss_occurence,
	logical_flag_insert AS logical_flag_op_insert,
	sysdate AS extract_date,
	sysdate AS as_of_date,
	source_system_id
	FROM RTR_CLM_STG_Update_INSERT
),
UPD_42GP_STAGE_INSERT AS (
	SELECT
	pif_symbol, 
	pif_policy_number, 
	pif_module, 
	loss_year, 
	loss_month, 
	loss_day, 
	loss_occurence, 
	logical_flag_op_insert, 
	extract_date, 
	as_of_date, 
	source_system_id
	FROM EXP_CLM_STG_Insert
),
PIF_42GP_STAGE_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PIF_42GP_STAGE
	(pif_symbol, pif_policy_number, pif_module, ipfcgp_year_of_loss, ipfcgp_month_of_loss, ipfcgp_day_of_loss, ipfcgp_loss_occurence, logical_flag, extract_date, as_of_date, source_system_id)
	SELECT 
	PIF_SYMBOL, 
	PIF_POLICY_NUMBER, 
	PIF_MODULE, 
	loss_year AS IPFCGP_YEAR_OF_LOSS, 
	loss_month AS IPFCGP_MONTH_OF_LOSS, 
	loss_day AS IPFCGP_DAY_OF_LOSS, 
	loss_occurence AS IPFCGP_LOSS_OCCURENCE, 
	logical_flag_op_insert AS LOGICAL_FLAG, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	SOURCE_SYSTEM_ID
	FROM UPD_42GP_STAGE_INSERT
),
WITH
SQ_PIF_42GJ_stage AS (
	SELECT pif_42gj_stage.pif_42gj_stage_id, pif_42gj_stage.pif_symbol, pif_42gj_stage.pif_policy_number, pif_42gj_stage.pif_module, pif_42gj_stage.ipfc4j_loss_year, pif_42gj_stage.ipfc4j_loss_month, pif_42gj_stage.ipfc4j_loss_day, pif_42gj_stage.ipfc4j_loss_occurence, pif_42gj_stage.ipfc4j_loss_claimant 
	FROM
	 pif_42gj_stage
	
	where  pif_42gj_stage.ipfc4j_use_code != 'CMT'
),
EXP_get_values AS (
	SELECT
	pif_42gj_stage_id,
	pif_symbol,
	pif_policy_number,
	pif_module,
	ipfc4j_loss_year,
	ipfc4j_loss_month,
	ipfc4j_loss_day,
	ipfc4j_loss_occurence,
	ipfc4j_loss_claimant
	FROM SQ_PIF_42GJ_stage
),
LKP_4578_stage AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module,
	loss_year,
	loss_month,
	loss_day,
	loss_occurence,
	loss_claimant,
	logical_flag
	FROM (
		SELECT MAX(pif_4578_stage.logical_flag) as logical_flag, pif_4578_stage.pif_symbol as pif_symbol, pif_4578_stage.pif_policy_number as pif_policy_number, pif_4578_stage.pif_module as pif_module, pif_4578_stage.loss_year as loss_year, pif_4578_stage.loss_month as loss_month, pif_4578_stage.loss_day as loss_day, pif_4578_stage.loss_occurence as loss_occurence, pif_4578_stage.loss_claimant as loss_claimant FROM pif_4578_stage
		GROUP BY pif_4578_stage.pif_symbol, pif_4578_stage.pif_policy_number, pif_4578_stage.pif_module, pif_4578_stage.loss_year, pif_4578_stage.loss_month, pif_4578_stage.loss_day, pif_4578_stage.loss_occurence, pif_4578_stage.loss_claimant
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module,loss_year,loss_month,loss_day,loss_occurence,loss_claimant ORDER BY pif_symbol) = 1
),
FILTRANS AS (
	SELECT
	EXP_get_values.pif_42gj_stage_id, 
	LKP_4578_stage.logical_flag
	FROM EXP_get_values
	LEFT JOIN LKP_4578_stage
	ON LKP_4578_stage.pif_symbol = EXP_get_values.pif_symbol AND LKP_4578_stage.pif_policy_number = EXP_get_values.pif_policy_number AND LKP_4578_stage.pif_module = EXP_get_values.pif_module AND LKP_4578_stage.loss_year = EXP_get_values.ipfc4j_loss_year AND LKP_4578_stage.loss_month = EXP_get_values.ipfc4j_loss_month AND LKP_4578_stage.loss_day = EXP_get_values.ipfc4j_loss_day AND LKP_4578_stage.loss_occurence = EXP_get_values.ipfc4j_loss_occurence AND LKP_4578_stage.loss_claimant = EXP_get_values.ipfc4j_loss_claimant
	WHERE NOT ISNULL(logical_flag)
),
UPD_42GJ AS (
	SELECT
	pif_42gj_stage_id, 
	logical_flag,
	IFF(NOT logical_flag IS NULL, 1, 3) AS update_strategy_flag
	FROM FILTRANS
),
PIF_42GJ_stage1 AS (
	MERGE INTO PIF_42GJ_stage AS T
	USING UPD_42GJ AS S
	ON (T.pif_42gj_stage_id = S.pif_42gj_stage_id) AND update_strategy_flag = 1  -- DD_UPDATE = 1
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.logical_flag = S.logical_flag
),
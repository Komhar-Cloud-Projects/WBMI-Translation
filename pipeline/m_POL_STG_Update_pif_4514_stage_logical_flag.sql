WITH
LKP_arch_pif_4578_stage AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module
	FROM (
		SELECT DISTINCT pif_symbol        AS pif_symbol,
		       pif_policy_number AS pif_policy_number,
		       pif_module        AS pif_module
		FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_pif_4578_stage 
		WHERE logical_flag in ('0')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module ORDER BY pif_symbol DESC) = 1
),
LKP_Exceed_Claim_exists AS (
	SELECT
	con_claim_nbr,
	con_policy_id
	FROM (
		SELECT con_claim_nbr AS con_claim_nbr,
		       SUBSTRING(con_policy_id,1,12)  AS con_policy_id
		FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.clm_occurrence_nbr_stage
		WHERE LEN(LTRIM(con_claim_nbr)) > 15
		
		---- Pulling on the claims information as EXCEED Claim Key is a 20 character field.
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY con_policy_id ORDER BY con_claim_nbr DESC) = 1
),
LKP_Arch_PIF_02_Stage AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module
	FROM (
		SELECT DISTINCT arch_pif_02_stage.pif_symbol as pif_symbol, arch_pif_02_stage.pif_policy_number as pif_policy_number, arch_pif_02_stage.pif_module as pif_module 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_pif_02_stage
		WHERE SUBSTRING(cast(pif_full_agency_number as varchar(7)),1,2) + SUBSTRING(cast(pif_full_agency_number as varchar(7)),5,3) ='99999'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module ORDER BY pif_symbol DESC) = 1
),
LKP_pif_4514_stage AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module
	FROM (
		SELECT DISTINCT pif_symbol        AS pif_symbol,
		       pif_policy_number AS pif_policy_number,
		       pif_module        AS pif_module
		FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.pif_4514_stage 
		WHERE sar_entrd_date >= '19980101'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module ORDER BY pif_symbol) = 1
),
SQ_pif_4514_stage AS (
	SELECT pif_4514_stage.pif_4514_stage_id, pif_4514_stage.pif_symbol, pif_4514_stage.pif_policy_number, pif_4514_stage.pif_module, pif_4514_stage.sar_entrd_date, pif_4514_stage.sar_acct_entrd_date 
	FROM
	 pif_4514_stage
	WHERE sar_entrd_date < '19980101'
),
EXP_Evaluate AS (
	SELECT
	pif_4514_stage_id,
	pif_symbol,
	pif_policy_number,
	pif_module,
	sar_entrd_date,
	sar_acct_entrd_date,
	-- *INF*: DECODE(TRUE,
	-- sar_entrd_date >= '19980101','0',
	-- NOT ISNULL(:LKP.LKP_ARCH_PIF_02_STAGE(pif_symbol, pif_policy_number, pif_module)),'4',
	-- NOT ISNULL(:LKP.LKP_PIF_4514_STAGE(pif_symbol,pif_policy_number,pif_module)),'1',
	-- NOT ISNULL(:LKP.LKP_EXCEED_CLAIM_EXISTS(pif_symbol  || pif_policy_number || pif_module)),'2',
	-- NOT ISNULL(:LKP.LKP_ARCH_PIF_4578_STAGE(pif_symbol, pif_policy_number, pif_module)),'3',
	-- '-1')
	-- 
	-- 
	-- -- Logical Flag value of 0, transactions with sar_entrd_date >= '19980101'
	-- -- Logical Flag value of 1, transactions where sar_entrd_date < '19980101' but also has transactions with booked_date >= '19980101'
	-- -- Logical Flag value of 2, transactions with sar_entrd_date < '19980101' but has EXCEED Claim to the policy
	-- -- Logical Flag value of 3, transactions with sar_entrd_date < '19980101' but has PMS Only Claim on the policy
	-- -- Logical Flag value of 4, Policy transactions which has Agency Number of  99999
	-- -- Logical Flag value of -1, transactions with sar_entrd_date < '19980101'
	DECODE(
	    TRUE,
	    sar_entrd_date >= '19980101', '0',
	    LKP_ARCH_PIF_02_STAGE_pif_symbol_pif_policy_number_pif_module.pif_symbol IS NOT NULL, '4',
	    LKP_PIF_4514_STAGE_pif_symbol_pif_policy_number_pif_module.pif_symbol IS NOT NULL, '1',
	    LKP_EXCEED_CLAIM_EXISTS_pif_symbol_pif_policy_number_pif_module.con_claim_nbr IS NOT NULL, '2',
	    LKP_ARCH_PIF_4578_STAGE_pif_symbol_pif_policy_number_pif_module.pif_symbol IS NOT NULL, '3',
	    '-1'
	) AS v_logical_flag,
	v_logical_flag AS logical_flag
	FROM SQ_pif_4514_stage
	LEFT JOIN LKP_ARCH_PIF_02_STAGE LKP_ARCH_PIF_02_STAGE_pif_symbol_pif_policy_number_pif_module
	ON LKP_ARCH_PIF_02_STAGE_pif_symbol_pif_policy_number_pif_module.pif_symbol = pif_symbol
	AND LKP_ARCH_PIF_02_STAGE_pif_symbol_pif_policy_number_pif_module.pif_policy_number = pif_policy_number
	AND LKP_ARCH_PIF_02_STAGE_pif_symbol_pif_policy_number_pif_module.pif_module = pif_module

	LEFT JOIN LKP_PIF_4514_STAGE LKP_PIF_4514_STAGE_pif_symbol_pif_policy_number_pif_module
	ON LKP_PIF_4514_STAGE_pif_symbol_pif_policy_number_pif_module.pif_symbol = pif_symbol
	AND LKP_PIF_4514_STAGE_pif_symbol_pif_policy_number_pif_module.pif_policy_number = pif_policy_number
	AND LKP_PIF_4514_STAGE_pif_symbol_pif_policy_number_pif_module.pif_module = pif_module

	LEFT JOIN LKP_EXCEED_CLAIM_EXISTS LKP_EXCEED_CLAIM_EXISTS_pif_symbol_pif_policy_number_pif_module
	ON LKP_EXCEED_CLAIM_EXISTS_pif_symbol_pif_policy_number_pif_module.con_policy_id = pif_symbol || pif_policy_number || pif_module

	LEFT JOIN LKP_ARCH_PIF_4578_STAGE LKP_ARCH_PIF_4578_STAGE_pif_symbol_pif_policy_number_pif_module
	ON LKP_ARCH_PIF_4578_STAGE_pif_symbol_pif_policy_number_pif_module.pif_symbol = pif_symbol
	AND LKP_ARCH_PIF_4578_STAGE_pif_symbol_pif_policy_number_pif_module.pif_policy_number = pif_policy_number
	AND LKP_ARCH_PIF_4578_STAGE_pif_symbol_pif_policy_number_pif_module.pif_module = pif_module

),
UPD_Logical_flag AS (
	SELECT
	pif_4514_stage_id, 
	logical_flag
	FROM EXP_Evaluate
),
pif_4514_stage_update AS (
	MERGE INTO pif_4514_stage AS T
	USING UPD_Logical_flag AS S
	ON T.pif_4514_stage_id = S.pif_4514_stage_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.logical_flag = S.logical_flag
),
WITH
SQ_pif_42gq_lit_stage AS (
	SELECT 
	LTRIM(RTRIM(pif_42gq_lit_stage.pif_symbol)) as pif_symbol,
	LTRIM(RTRIM(pif_42gq_lit_stage.pif_policy_number)) as pif_policy_number, 
	LTRIM(RTRIM(pif_42gq_lit_stage.pif_module)) as pif_module, 
	LTRIM(RTRIM(pif_42gq_lit_stage.ipfcgq_year_of_loss)) as loss_year, 
	LTRIM(RTRIM(pif_42gq_lit_stage.ipfcgq_month_of_loss)) as loss_month, 
	LTRIM(RTRIM(pif_42gq_lit_stage.ipfcgq_day_of_loss)) as loss_day, 
	LTRIM(RTRIM(pif_42gq_lit_stage.ipfcgq_loss_occurence)) as loss_occurrence, 
	LTRIM(RTRIM(pif_42gq_lit_stage.ipfcgq_loss_claimant)) as loss_claimant, 
	LTRIM(RTRIM(pif_42gq_lit_stage.ipfcgq_last_offer_date)) as last_offer_date,
	LTRIM(RTRIM(pif_42gq_lit_stage.ipfcgq_trial_date)) as trail_date,
	LTRIM(RTRIM(pif_42gq_lit_stage.ipfcgq_suit_amount)) as suit_amount,
	LTRIM(RTRIM(pif_42gq_lit_stage.ipfcgq_demand_date_1)) as demand_date_1, 
	LTRIM(RTRIM(pif_42gq_lit_stage.ipfcgq_offer_date_1)) as offer_date_1, 
	LTRIM(RTRIM(pif_42gq_lit_stage.ipfcgq_docket_number)) as docket_number, 
	LTRIM(RTRIM(pif_42gq_lit_stage.ipfcgq_suit_state_county)) as suit_statte_county,
	LTRIM(RTRIM(pif_42gq_lit_stage.ipfcgq_court)) as court 
	FROM 
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_42gq_lit_stage
	WHERE pif_42gq_lit_stage.logical_flag  = 0
),
EXP_Stage_Validate AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module,
	ipfcgq_year_of_loss,
	-- *INF*: TO_CHAR(ipfcgq_year_of_loss)
	TO_CHAR(ipfcgq_year_of_loss) AS v_ipfcgq_year_of_loss,
	ipfcgq_month_of_loss,
	-- *INF*: TO_CHAR(ipfcgq_month_of_loss)
	TO_CHAR(ipfcgq_month_of_loss) AS v_ipfcgq_month_of_loss,
	ipfcgq_day_of_loss,
	-- *INF*: TO_CHAR(ipfcgq_day_of_loss)
	TO_CHAR(ipfcgq_day_of_loss) AS v_ipfcgq_day_of_loss,
	ipfcgq_loss_occurence,
	-- *INF*: TO_INTEGER(ipfcgq_loss_occurence)
	TO_INTEGER(ipfcgq_loss_occurence) AS v_ipfcgq_loss_occurence,
	ipfcgq_loss_claimant,
	-- *INF*: TO_INTEGER(ipfcgq_loss_claimant)
	TO_INTEGER(ipfcgq_loss_claimant) AS v_ipfcgq_loss_claimant,
	pif_symbol || pif_policy_number || pif_module AS v_sym_num_mod,
	-- *INF*: IIF(LENGTH(v_ipfcgq_month_of_loss)=1,'0'||v_ipfcgq_month_of_loss,v_ipfcgq_month_of_loss) 
	-- || 
	-- IIF(LENGTH(v_ipfcgq_day_of_loss)=1,'0' || v_ipfcgq_day_of_loss,v_ipfcgq_day_of_loss) 
	-- || v_ipfcgq_year_of_loss
	IFF(LENGTH(v_ipfcgq_month_of_loss) = 1, '0' || v_ipfcgq_month_of_loss, v_ipfcgq_month_of_loss) || IFF(LENGTH(v_ipfcgq_day_of_loss) = 1, '0' || v_ipfcgq_day_of_loss, v_ipfcgq_day_of_loss) || v_ipfcgq_year_of_loss AS v_date_of_loss,
	v_sym_num_mod || v_date_of_loss || ipfcgq_loss_occurence || ipfcgq_loss_claimant AS V_Claim_Case_Key,
	V_Claim_Case_Key AS Claim_Case_Key,
	ipfcgq_last_offer_date AS in_ipfcgq_last_offer_date,
	-- *INF*: DECODE(TRUE,SUBSTR(in_ipfcgq_last_offer_date,1,2)='00','20'||SUBSTR(in_ipfcgq_last_offer_date,3,6),in_ipfcgq_last_offer_date)
	DECODE(TRUE,
		SUBSTR(in_ipfcgq_last_offer_date, 1, 2) = '00', '20' || SUBSTR(in_ipfcgq_last_offer_date, 3, 6),
		in_ipfcgq_last_offer_date) AS v_ipfcgq_last_offer_date,
	-- *INF*: IIF(ISNULL(v_ipfcgq_last_offer_date) or IS_SPACES(LTRIM(RTRIM(v_ipfcgq_last_offer_date))) OR LENGTH(LTRIM(RTRIM(v_ipfcgq_last_offer_date)))  = 0 ,
	-- TO_DATE('12/31/2100 00:00:00','MM/DD/YYYY HH24:MI:SS'),TO_DATE(v_ipfcgq_last_offer_date,'YYYYMMDD'))
	IFF(v_ipfcgq_last_offer_date IS NULL OR IS_SPACES(LTRIM(RTRIM(v_ipfcgq_last_offer_date))) OR LENGTH(LTRIM(RTRIM(v_ipfcgq_last_offer_date))) = 0, TO_DATE('12/31/2100 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE(v_ipfcgq_last_offer_date, 'YYYYMMDD')) AS out_last_offer_date,
	ipfcgq_trial_date AS in_ipfcgq_trail_date,
	-- *INF*: IIF(ISNULL(in_ipfcgq_trail_date) or IS_SPACES(LTRIM(RTRIM(in_ipfcgq_trail_date))) OR LENGTH(LTRIM(RTRIM(in_ipfcgq_trail_date))) = 0,
	-- TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),TO_DATE(in_ipfcgq_trail_date,'YYYYMMDD'))
	-- 
	-- 
	IFF(in_ipfcgq_trail_date IS NULL OR IS_SPACES(LTRIM(RTRIM(in_ipfcgq_trail_date))) OR LENGTH(LTRIM(RTRIM(in_ipfcgq_trail_date))) = 0, TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE(in_ipfcgq_trail_date, 'YYYYMMDD')) AS trail_date,
	ipfcgq_suit_amount AS in_ipfcgq_suit_amount,
	-- *INF*: IIF(ISNULL(in_ipfcgq_suit_amount),0,in_ipfcgq_suit_amount)
	IFF(in_ipfcgq_suit_amount IS NULL, 0, in_ipfcgq_suit_amount) AS suit_amount,
	ipfcgq_demand_date_1 AS in_ipfcgq_demand_date_1,
	-- *INF*: DECODE(TRUE,SUBSTR(in_ipfcgq_demand_date_1,1,2)='00','20'||SUBSTR(in_ipfcgq_demand_date_1,3,6),in_ipfcgq_demand_date_1)
	DECODE(TRUE,
		SUBSTR(in_ipfcgq_demand_date_1, 1, 2) = '00', '20' || SUBSTR(in_ipfcgq_demand_date_1, 3, 6),
		in_ipfcgq_demand_date_1) AS v_ipfcgq_demand_date_1,
	v_ipfcgq_demand_date_1 AS out_ipfcgq_demand_date_1,
	V_Claim_Case_Key||'ATTY' AS v_CLAIM_CASE_KEY_ATTY,
	-- *INF*: --:LKP.LKP_PRIM_LIT_HANDLER_AK_ID(v_CLAIM_CASE_KEY_ATTY)
	'' AS v_Prim_Lit_Handler_ak_id,
	-- *INF*: --IIF(ISNULL(v_Prim_Lit_Handler_ak_id),-1,v_Prim_Lit_Handler_ak_id)
	-- 
	-- 
	-- --IIF(ISNULL(v_Prim_Lit_Handler_Name),'N/A',v_Prim_Lit_Handler_Name)
	-- 
	-- --V_PRIM_LIT_HANDLER_NAME
	'' AS Prim_Lit_Handler_ak_id,
	-- *INF*: IIF(ISNULL(v_ipfcgq_demand_date_1) or IS_SPACES(LTRIM(RTRIM(v_ipfcgq_demand_date_1))) OR LENGTH(LTRIM(RTRIM(v_ipfcgq_demand_date_1))) = 0
	-- ,TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),TO_DATE(v_ipfcgq_demand_date_1,'YYYYMMDD'))
	IFF(v_ipfcgq_demand_date_1 IS NULL OR IS_SPACES(LTRIM(RTRIM(v_ipfcgq_demand_date_1))) OR LENGTH(LTRIM(RTRIM(v_ipfcgq_demand_date_1))) = 0, TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE(v_ipfcgq_demand_date_1, 'YYYYMMDD')) AS suit_open_date,
	ipfcgq_offer_date_1 AS in_ipfcgq_offer_date_1,
	-- *INF*: DECODE(TRUE,SUBSTR(in_ipfcgq_offer_date_1,1,2)='00','20'||SUBSTR(in_ipfcgq_offer_date_1,3,6),in_ipfcgq_offer_date_1)
	DECODE(TRUE,
		SUBSTR(in_ipfcgq_offer_date_1, 1, 2) = '00', '20' || SUBSTR(in_ipfcgq_offer_date_1, 3, 6),
		in_ipfcgq_offer_date_1) AS v_ipfcgq_offer_date_1,
	-- *INF*: IIF(ISNULL(v_ipfcgq_offer_date_1)  OR LENGTH(RTRIM(LTRIM(v_ipfcgq_offer_date_1)))  =  0 ,
	--           IIF(ISNULL(v_ipfcgq_last_offer_date) OR LENGTH(RTRIM(LTRIM(v_ipfcgq_last_offer_date)))  =  0 ,TO_DATE('12/31/2100 00:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 			TO_DATE(v_ipfcgq_last_offer_date,'YYYYMMDD')),TO_DATE(v_ipfcgq_offer_date_1,'YYYYMMDD'))
	-- 
	-- 
	-- 
	IFF(v_ipfcgq_offer_date_1 IS NULL OR LENGTH(RTRIM(LTRIM(v_ipfcgq_offer_date_1))) = 0, IFF(v_ipfcgq_last_offer_date IS NULL OR LENGTH(RTRIM(LTRIM(v_ipfcgq_last_offer_date))) = 0, TO_DATE('12/31/2100 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE(v_ipfcgq_last_offer_date, 'YYYYMMDD')), TO_DATE(v_ipfcgq_offer_date_1, 'YYYYMMDD')) AS suit_close_date,
	ipfcgq_docket_number AS in_ipfcgq_docket_number,
	-- *INF*: SUBSTR(in_ipfcgq_docket_number,0,INSTR(in_ipfcgq_docket_number,'"'))
	SUBSTR(in_ipfcgq_docket_number, 0, INSTR(in_ipfcgq_docket_number, '"')) AS v_ipfcgq_docket_number,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(v_ipfcgq_docket_number))) OR IS_SPACES(LTRIM(RTRIM(v_ipfcgq_docket_number))) OR LENGTH(LTRIM(RTRIM(v_ipfcgq_docket_number))) = 0 OR LTRIM(RTRIM(v_ipfcgq_docket_number)) = '"' ,'N/A',LTRIM(RTRIM(v_ipfcgq_docket_number)))
	IFF(LTRIM(RTRIM(v_ipfcgq_docket_number)) IS NULL OR IS_SPACES(LTRIM(RTRIM(v_ipfcgq_docket_number))) OR LENGTH(LTRIM(RTRIM(v_ipfcgq_docket_number))) = 0 OR LTRIM(RTRIM(v_ipfcgq_docket_number)) = '"', 'N/A', LTRIM(RTRIM(v_ipfcgq_docket_number))) AS claim_case_num,
	ipfcgq_suit_state_county AS in_ipfcgq_suit_state_county,
	-- *INF*: SUBSTR(in_ipfcgq_suit_state_county,0,2)
	SUBSTR(in_ipfcgq_suit_state_county, 0, 2) AS v_ipfcgq_suit_state_county,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(v_ipfcgq_suit_state_county))) OR  IS_SPACES(LTRIM(RTRIM(v_ipfcgq_suit_state_county)))  OR LENGTH(LTRIM(RTRIM(v_ipfcgq_suit_state_county))) = 0 OR LTRIM(RTRIM(v_ipfcgq_suit_state_county)) ='"', 'N/A',LTRIM(RTRIM(v_ipfcgq_suit_state_county)))
	IFF(LTRIM(RTRIM(v_ipfcgq_suit_state_county)) IS NULL OR IS_SPACES(LTRIM(RTRIM(v_ipfcgq_suit_state_county))) OR LENGTH(LTRIM(RTRIM(v_ipfcgq_suit_state_county))) = 0 OR LTRIM(RTRIM(v_ipfcgq_suit_state_county)) = '"', 'N/A', LTRIM(RTRIM(v_ipfcgq_suit_state_county))) AS suit_state_county,
	ipfcgq_court AS in_ipfcgq_court,
	-- *INF*: SUBSTR(in_ipfcgq_court,0,INSTR(in_ipfcgq_court,'"'))
	SUBSTR(in_ipfcgq_court, 0, INSTR(in_ipfcgq_court, '"')) AS v_ipfcgq_court,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(v_ipfcgq_court))) OR  IS_SPACES(LTRIM(RTRIM(v_ipfcgq_court)))  OR LENGTH(LTRIM(RTRIM(v_ipfcgq_court))) = 0 OR v_ipfcgq_court='"', 'N/A',LTRIM(RTRIM(v_ipfcgq_court)))
	IFF(LTRIM(RTRIM(v_ipfcgq_court)) IS NULL OR IS_SPACES(LTRIM(RTRIM(v_ipfcgq_court))) OR LENGTH(LTRIM(RTRIM(v_ipfcgq_court))) = 0 OR v_ipfcgq_court = '"', 'N/A', LTRIM(RTRIM(v_ipfcgq_court))) AS court
	FROM SQ_pif_42gq_lit_stage
),
LKP_prim_lit_handler_role_code AS (
	SELECT
	ipfcgq_attorney_type_1,
	ipfcgq_attorney_seq_1,
	pif_symbol,
	pif_policy_number,
	pif_module,
	ipfcgq_year_of_loss,
	ipfcgq_month_of_loss,
	ipfcgq_day_of_loss,
	ipfcgq_loss_occurence,
	ipfcgq_loss_claimant
	FROM (
		SELECT 
		A.ipfcgq_attorney_type_1 as ipfcgq_attorney_type_1, 
		A.ipfcgq_attorney_seq_1 as ipfcgq_attorney_seq_1, 
		A.pif_symbol as pif_symbol, 
		A.pif_policy_number as pif_policy_number, 
		A.pif_module as pif_module, 
		A.ipfcgq_year_of_loss as ipfcgq_year_of_loss, 
		A.ipfcgq_month_of_loss as ipfcgq_month_of_loss, 
		A.ipfcgq_day_of_loss as ipfcgq_day_of_loss, 
		A.ipfcgq_loss_occurence as ipfcgq_loss_occurence, 
		A.ipfcgq_loss_claimant as ipfcgq_loss_claimant 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_42gq_aty_stage A
		WHERE A.logical_flag = 0
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module,ipfcgq_year_of_loss,ipfcgq_month_of_loss,ipfcgq_day_of_loss,ipfcgq_loss_occurence,ipfcgq_loss_claimant ORDER BY ipfcgq_attorney_type_1) = 1
),
LKP_Prim_Lit_Handler_ak_id AS (
	SELECT
	claim_party_ak_id,
	claim_party_key
	FROM (
		SELECT 
		CP.claim_party_ak_id as claim_party_ak_id, 
		SUBSTRING(CP.claim_party_key,1,3) as claim_party_key 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party CP
		WHERE LEN(CP.claim_party_key) < 5
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_key ORDER BY claim_party_ak_id) = 1
),
LKP_suit_status_code AS (
	SELECT
	ipfcgq_loss_suit,
	pif_symbol,
	pif_policy_number,
	pif_module,
	ipfcgq_year_of_loss,
	ipfcgq_month_of_loss,
	ipfcgq_day_of_loss,
	ipfcgq_loss_occurence,
	ipfcgq_loss_claimant
	FROM (
		SELECT 	a.ipfcgq_loss_suit as ipfcgq_loss_suit, 
		a.pif_symbol as pif_symbol, 
		a.pif_policy_number as pif_policy_number, 
		a.pif_module as pif_module, 
		a.ipfcgq_year_of_loss as ipfcgq_year_of_loss, 
		a.ipfcgq_month_of_loss as ipfcgq_month_of_loss, 
		a.ipfcgq_day_of_loss as ipfcgq_day_of_loss, 
		a.ipfcgq_loss_occurence as ipfcgq_loss_occurence, 
		a.ipfcgq_loss_claimant as ipfcgq_loss_claimant 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_42gq_cmt_stage a
		WHERE 	logical_flag in (0,1)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module,ipfcgq_year_of_loss,ipfcgq_month_of_loss,ipfcgq_day_of_loss,ipfcgq_loss_occurence,ipfcgq_loss_claimant ORDER BY ipfcgq_loss_suit) = 1
),
EXP_LKP_Values AS (
	SELECT
	EXP_Stage_Validate.Claim_Case_Key AS claim_case_key,
	EXP_Stage_Validate.trail_date,
	LKP_suit_status_code.ipfcgq_loss_suit AS suit_status,
	-- *INF*: IIF(ISNULL(suit_status) OR IS_SPACES(suit_status) OR LENGTH(suit_status) = 0,'N/A',suit_status)
	IFF(suit_status IS NULL OR IS_SPACES(suit_status) OR LENGTH(suit_status) = 0, 'N/A', suit_status) AS Out_suit_status,
	EXP_Stage_Validate.suit_amount,
	LKP_Prim_Lit_Handler_ak_id.claim_party_ak_id AS Prim_Lit_Handler_ak_id,
	-- *INF*: IIF(ISNULL(Prim_Lit_Handler_ak_id),-1,Prim_Lit_Handler_ak_id)
	IFF(Prim_Lit_Handler_ak_id IS NULL, - 1, Prim_Lit_Handler_ak_id) AS Prim_Lit_Handler_ak_id_Out,
	LKP_prim_lit_handler_role_code.ipfcgq_attorney_type_1 AS prim_litigation_handler_role_code,
	-- *INF*: IIF(ISNULL(prim_litigation_handler_role_code) OR IS_SPACES(prim_litigation_handler_role_code) OR LENGTH(prim_litigation_handler_role_code) = 0,'N/A',prim_litigation_handler_role_code)
	IFF(prim_litigation_handler_role_code IS NULL OR IS_SPACES(prim_litigation_handler_role_code) OR LENGTH(prim_litigation_handler_role_code) = 0, 'N/A', prim_litigation_handler_role_code) AS out_prim_litigation_handler_role_code,
	EXP_Stage_Validate.suit_open_date,
	EXP_Stage_Validate.suit_close_date,
	EXP_Stage_Validate.claim_case_num,
	EXP_Stage_Validate.suit_state_county,
	EXP_Stage_Validate.court,
	EXP_Stage_Validate.trail_date AS arbitration_open_date,
	EXP_Stage_Validate.out_last_offer_date AS arbitration_close_date
	FROM EXP_Stage_Validate
	LEFT JOIN LKP_Prim_Lit_Handler_ak_id
	ON LKP_Prim_Lit_Handler_ak_id.claim_party_key = LKP_prim_lit_handler_role_code.ipfcgq_attorney_seq_1
	LEFT JOIN LKP_prim_lit_handler_role_code
	ON LKP_prim_lit_handler_role_code.pif_symbol = EXP_Stage_Validate.pif_symbol AND LKP_prim_lit_handler_role_code.pif_policy_number = EXP_Stage_Validate.pif_policy_number AND LKP_prim_lit_handler_role_code.pif_module = EXP_Stage_Validate.pif_module AND LKP_prim_lit_handler_role_code.ipfcgq_year_of_loss = EXP_Stage_Validate.ipfcgq_year_of_loss AND LKP_prim_lit_handler_role_code.ipfcgq_month_of_loss = EXP_Stage_Validate.ipfcgq_month_of_loss AND LKP_prim_lit_handler_role_code.ipfcgq_day_of_loss = EXP_Stage_Validate.ipfcgq_day_of_loss AND LKP_prim_lit_handler_role_code.ipfcgq_loss_occurence = EXP_Stage_Validate.ipfcgq_loss_occurence AND LKP_prim_lit_handler_role_code.ipfcgq_loss_claimant = EXP_Stage_Validate.ipfcgq_loss_claimant
	LEFT JOIN LKP_suit_status_code
	ON LKP_suit_status_code.pif_symbol = EXP_Stage_Validate.pif_symbol AND LKP_suit_status_code.pif_policy_number = EXP_Stage_Validate.pif_policy_number AND LKP_suit_status_code.pif_module = EXP_Stage_Validate.pif_module AND LKP_suit_status_code.ipfcgq_year_of_loss = EXP_Stage_Validate.ipfcgq_year_of_loss AND LKP_suit_status_code.ipfcgq_month_of_loss = EXP_Stage_Validate.ipfcgq_month_of_loss AND LKP_suit_status_code.ipfcgq_day_of_loss = EXP_Stage_Validate.ipfcgq_day_of_loss AND LKP_suit_status_code.ipfcgq_loss_occurence = EXP_Stage_Validate.ipfcgq_loss_occurence AND LKP_suit_status_code.ipfcgq_loss_claimant = EXP_Stage_Validate.ipfcgq_loss_claimant
),
LKP_Claim_Case_EDW AS (
	SELECT
	claim_case_id,
	claim_case_ak_id,
	claim_case_name,
	claim_case_num,
	suit_county,
	suit_state,
	trial_date,
	suit_status_code,
	prim_litigation_handler_ak_id,
	prim_litigation_handler_role_code,
	suit_open_date,
	suit_close_date,
	suit_pay_amt,
	arbitration_open_date,
	arbitration_close_date,
	claim_case_key
	FROM (
		SELECT A.claim_case_id as claim_case_id, A.claim_case_ak_id as claim_case_ak_id, A.claim_case_name as claim_case_name, A.claim_case_num as claim_case_num, A.suit_county as suit_county, A.suit_state as suit_state, A.trial_date as trial_date, A.suit_status_code as suit_status_code, A.prim_litigation_handler_ak_id as prim_litigation_handler_ak_id, A.prim_litigation_handler_role_code as prim_litigation_handler_role_code, A.suit_open_date as suit_open_date, A.suit_close_date as suit_close_date, A.suit_pay_amt as suit_pay_amt, A.arbitration_open_date as arbitration_open_date, A.arbitration_close_date as arbitration_close_date, A.claim_case_key as claim_case_key FROM claim_case A
		WHERE A.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and A.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_case_key ORDER BY claim_case_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	EXP_LKP_Values.claim_case_key,
	EXP_LKP_Values.trail_date,
	EXP_LKP_Values.Out_suit_status AS suit_status,
	EXP_LKP_Values.suit_amount,
	EXP_LKP_Values.Prim_Lit_Handler_ak_id_Out AS Prim_Lit_Handler_ak_id,
	EXP_LKP_Values.out_prim_litigation_handler_role_code AS prim_litigation_handler_role_code,
	EXP_LKP_Values.suit_open_date,
	EXP_LKP_Values.suit_close_date,
	EXP_LKP_Values.claim_case_num,
	EXP_LKP_Values.suit_state_county,
	EXP_LKP_Values.court,
	EXP_LKP_Values.arbitration_open_date,
	EXP_LKP_Values.arbitration_close_date,
	LKP_Claim_Case_EDW.claim_case_id AS OLD_claim_case_id,
	LKP_Claim_Case_EDW.claim_case_ak_id AS OLD_claim_case_ak_id,
	LKP_Claim_Case_EDW.claim_case_name AS OLD_claim_case_key,
	LKP_Claim_Case_EDW.claim_case_num AS OLD_claim_case_num,
	LKP_Claim_Case_EDW.suit_county AS OLD_suit_county,
	LKP_Claim_Case_EDW.suit_state AS OLD_suit_state,
	LKP_Claim_Case_EDW.trial_date AS OLD_trial_date,
	LKP_Claim_Case_EDW.suit_status_code AS OLD_suit_status_code,
	LKP_Claim_Case_EDW.prim_litigation_handler_ak_id AS Old_prim_litigation_handler_ak_id,
	LKP_Claim_Case_EDW.prim_litigation_handler_role_code AS Old_prim_litigation_handler_role_code,
	LKP_Claim_Case_EDW.suit_open_date AS OLD_suit_open_date,
	LKP_Claim_Case_EDW.suit_close_date AS OLD_suit_close_date,
	LKP_Claim_Case_EDW.suit_pay_amt AS OLD_suit_pay_amt,
	LKP_Claim_Case_EDW.arbitration_open_date AS Old_arbitration_open_date,
	LKP_Claim_Case_EDW.arbitration_close_date AS Old_arbitration_close_date,
	'N/A' AS DEFAULT_CHAR,
	0 AS DEFAULT_INT,
	-- *INF*: TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') AS DEFAULT_DATE,
	-- *INF*: IIF(ISNULL(OLD_claim_case_id),'NEW',
	-- IIF(LTRIM(RTRIM(claim_case_num))<>LTRIM(RTRIM(OLD_claim_case_num)) OR 
	--        LTRIM(RTRIM(court))<>LTRIM(RTRIM(OLD_suit_county)) OR
	--        LTRIM(RTRIM(suit_state_county))<>LTRIM(RTRIM(OLD_suit_state)) OR 
	--        trail_date<>OLD_trial_date OR 
	--        LTRIM(RTRIM(suit_status))<>LTRIM(RTRIM(OLD_suit_status_code)) OR 
	--        suit_open_date<>OLD_suit_open_date OR
	--        suit_close_date<>OLD_suit_close_date OR  
	--        suit_amount <> OLD_suit_pay_amt  OR
	--        Prim_Lit_Handler_ak_id <>Old_prim_litigation_handler_ak_id OR
	--        LTRIM(RTRIM(prim_litigation_handler_role_code))<>LTRIM(RTRIM(Old_prim_litigation_handler_role_code)) OR
	--       arbitration_open_date <> Old_arbitration_open_date  OR
	--       arbitration_close_date <>Old_arbitration_close_date, 
	-- 'UPDATE','NOCHANGE')) 
	-- 
	IFF(OLD_claim_case_id IS NULL, 'NEW', IFF(LTRIM(RTRIM(claim_case_num)) <> LTRIM(RTRIM(OLD_claim_case_num)) OR LTRIM(RTRIM(court)) <> LTRIM(RTRIM(OLD_suit_county)) OR LTRIM(RTRIM(suit_state_county)) <> LTRIM(RTRIM(OLD_suit_state)) OR trail_date <> OLD_trial_date OR LTRIM(RTRIM(suit_status)) <> LTRIM(RTRIM(OLD_suit_status_code)) OR suit_open_date <> OLD_suit_open_date OR suit_close_date <> OLD_suit_close_date OR suit_amount <> OLD_suit_pay_amt OR Prim_Lit_Handler_ak_id <> Old_prim_litigation_handler_ak_id OR LTRIM(RTRIM(prim_litigation_handler_role_code)) <> LTRIM(RTRIM(Old_prim_litigation_handler_role_code)) OR arbitration_open_date <> Old_arbitration_open_date OR arbitration_close_date <> Old_arbitration_close_date, 'UPDATE', 'NOCHANGE')) AS v_changed_flag,
	v_changed_flag AS CHANGED_FLAG,
	1 AS CRRNT_SNPSHT_FLAG,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYS_ID,
	-- *INF*: IIF(v_changed_flag = 'NEW',TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),SYSDATE)
	IFF(v_changed_flag = 'NEW', TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS EFF_FROM_DATE,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS EFF_TO_DATE,
	SYSDATE AS CREATED_DATE,
	SYSDATE AS MODIFIED_DATE,
	0.0 AS Demand_at_initial_Litigation
	FROM EXP_LKP_Values
	LEFT JOIN LKP_Claim_Case_EDW
	ON LKP_Claim_Case_EDW.claim_case_key = EXP_LKP_Values.claim_case_key
),
FIL_Changes AS (
	SELECT
	OLD_claim_case_ak_id, 
	claim_case_key, 
	claim_case_num, 
	court, 
	suit_state_county, 
	trail_date, 
	suit_status, 
	Prim_Lit_Handler_ak_id, 
	prim_litigation_handler_role_code, 
	suit_open_date, 
	suit_close_date, 
	suit_amount, 
	arbitration_open_date, 
	arbitration_close_date, 
	DEFAULT_CHAR, 
	DEFAULT_INT, 
	DEFAULT_DATE, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	CHANGED_FLAG, 
	Demand_at_initial_Litigation
	FROM EXP_Detect_Changes
	WHERE CHANGED_FLAG='NEW' OR CHANGED_FLAG='UPDATE'
),
SEQ_Claim_Case_AK_ID AS (
	CREATE SEQUENCE SEQ_Claim_Case_AK_ID
	START = 0
	INCREMENT = 1;
),
EXP_Determine_AK AS (
	SELECT
	OLD_claim_case_ak_id,
	-- *INF*: IIF(Changed_flag = 'NEW',NEXTVAL,OLD_claim_case_ak_id)
	IFF(Changed_flag = 'NEW', NEXTVAL, OLD_claim_case_ak_id) AS claim_case_ak_id,
	claim_case_key,
	claim_case_num,
	court,
	suit_state_county,
	trail_date,
	suit_status,
	Prim_Lit_Handler_ak_id,
	prim_litigation_handler_role_code,
	suit_open_date,
	suit_close_date,
	suit_amount,
	arbitration_open_date,
	arbitration_close_date,
	DEFAULT_CHAR,
	DEFAULT_INT,
	DEFAULT_DATE,
	CRRNT_SNPSHT_FLAG,
	AUDIT_ID,
	EFF_FROM_DATE,
	EFF_TO_DATE,
	SOURCE_SYS_ID,
	CREATED_DATE,
	MODIFIED_DATE,
	CHANGED_FLAG AS Changed_flag,
	Demand_at_initial_Litigation,
	'N/A' AS SettlementTypeCode,
	SEQ_Claim_Case_AK_ID.NEXTVAL
	FROM FIL_Changes
),
claim_case_insert AS (
	INSERT INTO claim_case
	(claim_case_ak_id, prim_litigation_handler_ak_id, claim_case_key, claim_case_name, claim_case_num, suit_county, suit_state, trial_date, first_notice_law_suit_ind, declaratory_action_ind, suit_status_code, suit_denial_date, prim_litigation_handler_role_code, suit_open_date, suit_close_date, suit_how_claim_closed, reins_reported_ind, commercl_umb_reserve, suit_pay_amt, arbitration_open_date, arbitration_close_date, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, demand_at_initial_litigation, SettlementTypeCode)
	SELECT 
	CLAIM_CASE_AK_ID, 
	Prim_Lit_Handler_ak_id AS PRIM_LITIGATION_HANDLER_AK_ID, 
	CLAIM_CASE_KEY, 
	DEFAULT_CHAR AS CLAIM_CASE_NAME, 
	CLAIM_CASE_NUM, 
	court AS SUIT_COUNTY, 
	suit_state_county AS SUIT_STATE, 
	trail_date AS TRIAL_DATE, 
	DEFAULT_CHAR AS FIRST_NOTICE_LAW_SUIT_IND, 
	DEFAULT_CHAR AS DECLARATORY_ACTION_IND, 
	suit_status AS SUIT_STATUS_CODE, 
	DEFAULT_DATE AS SUIT_DENIAL_DATE, 
	PRIM_LITIGATION_HANDLER_ROLE_CODE, 
	SUIT_OPEN_DATE, 
	SUIT_CLOSE_DATE, 
	DEFAULT_CHAR AS SUIT_HOW_CLAIM_CLOSED, 
	DEFAULT_CHAR AS REINS_REPORTED_IND, 
	DEFAULT_INT AS COMMERCL_UMB_RESERVE, 
	suit_amount AS SUIT_PAY_AMT, 
	ARBITRATION_OPEN_DATE, 
	ARBITRATION_CLOSE_DATE, 
	CRRNT_SNPSHT_FLAG AS CRRNT_SNPSHT_FLAG, 
	AUDIT_ID AS AUDIT_ID, 
	EFF_FROM_DATE AS EFF_FROM_DATE, 
	EFF_TO_DATE AS EFF_TO_DATE, 
	SOURCE_SYS_ID AS SOURCE_SYS_ID, 
	CREATED_DATE AS CREATED_DATE, 
	MODIFIED_DATE AS MODIFIED_DATE, 
	Demand_at_initial_Litigation AS DEMAND_AT_INITIAL_LITIGATION, 
	SETTLEMENTTYPECODE
	FROM EXP_Determine_AK
),
SQ_claim_case AS (
	SELECT 
	a.claim_case_id, 
	a.claim_case_key,
	a.eff_from_date, 
	a.eff_to_date 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_case a
	WHERE 
	a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND
	EXISTS(SELECT 1 
	                 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_case b
	                 WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and crrnt_snpsht_flag = 1
	                 AND a.claim_case_key = b.claim_case_key
	 	           GROUP BY b.claim_case_key
	                 HAVING COUNT(*) >1) 
	ORDER BY a.claim_case_key, a.eff_from_date DESC
),
EXP_Expire_Row AS (
	SELECT
	claim_case_id,
	claim_case_key,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,claim_case_key = v_prev_row_claim_case_key,ADD_TO_DATE(v_prev_row_eff_from_date,'SS',-1),orig_eff_to_date)
	-- 
	-- 
	-- 
	DECODE(TRUE,
		claim_case_key = v_prev_row_claim_case_key, ADD_TO_DATE(v_prev_row_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_prev_row_eff_from_date,
	claim_case_key AS v_prev_row_claim_case_key,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_claim_case
),
FIL_Claim_Case_FirstRow_In_AKGroup AS (
	SELECT
	claim_case_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Expire_Row
	WHERE orig_eff_to_date <>eff_to_date
),
UPD_Crrnt_Snpsht_Flag AS (
	SELECT
	claim_case_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FIL_Claim_Case_FirstRow_In_AKGroup
),
claim_case_crrnt_snpsht_flag_update AS (
	MERGE INTO claim_case AS T
	USING UPD_Crrnt_Snpsht_Flag AS S
	ON T.claim_case_id = S.claim_case_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),
WITH
SQ_curr_row_count AS (
	-- check multiple snapshot flag values of 1
	SELECT 
		count(@{pipeline().parameters.SOURCE_TABLE_AK_ID}) as EDW_count,
		'Count of @{pipeline().parameters.SOURCE_TABLE_AK_ID} from @{pipeline().parameters.SOURCE_TABLE_NAME} with more than one record with crrnt snpsht flag 1 = ' + convert(varchar,count(@{pipeline().parameters.SOURCE_TABLE_AK_ID})) as check_out_message,
		'@{pipeline().parameters.SOURCE_TABLE_NAME}' as target_name
	FROM 
		(SELECT @{pipeline().parameters.SOURCE_TABLE_AK_ID}, COUNT(@{pipeline().parameters.SOURCE_TABLE_AK_ID}) AS Expr1
	             FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME}
	             WHERE  (crrnt_snpsht_flag = 1)
	             GROUP BY @{pipeline().parameters.SOURCE_TABLE_AK_ID}
	             HAVING (COUNT(@{pipeline().parameters.SOURCE_TABLE_AK_ID}) > 1)) Generic
	
	
	UNION
	
	
	-- continuity of records check
	SELECT 
		COUNT(@{pipeline().parameters.SOURCE_TABLE_AK_ID}) as EDW_count, 
		'Count of @{pipeline().parameters.SOURCE_TABLE_AK_ID} from @{pipeline().parameters.SOURCE_TABLE_NAME} table where @{pipeline().parameters.SOURCE_TABLE_AK_ID} have no continuity in eff_from_date & eff_to_date = ' + convert(varchar,count(@{pipeline().parameters.SOURCE_TABLE_AK_ID})) as Check_Out_Message, 
		'@{pipeline().parameters.SOURCE_TABLE_NAME}' as Target_Name 
	FROM 
		(SELECT @{pipeline().parameters.SOURCE_TABLE_AK_ID} 
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME} a 
			WHERE not exists 
			(SELECT 1 
				FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME} b 
				where a.@{pipeline().parameters.SOURCE_TABLE_AK_ID} = b.@{pipeline().parameters.SOURCE_TABLE_AK_ID} and 
				      b.eff_from_date = dateadd(ss,1,a.eff_to_date)) and
				      (a.eff_to_date <> '2100-12-31 23:59:59.000'))CP
	
	UNION
	
	-- effective to date check
	
	SELECT 
		COUNT(@{pipeline().parameters.SOURCE_TABLE_AK_ID}) as EDW_count, 
		'Count of @{pipeline().parameters.SOURCE_TABLE_AK_ID} in @{pipeline().parameters.SOURCE_TABLE_NAME} table with crrnt_snpsht_flag = 1 that has Eff To Date not as 12/31/2100 = ' + CONVERT(varchar,count(@{pipeline().parameters.SOURCE_TABLE_AK_ID})) as check_out_message,
		'@{pipeline().parameters.SOURCE_TABLE_NAME}' as target_name 
	FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME} 
		WHERE 
			crrnt_snpsht_flag = 1 and 
			eff_to_date <> '2100-12-31 23:59:59.000'
	
	UNION
	-- check that no record with snapshot=0 has max eff to date
	
	SELECT 
		COUNT(@{pipeline().parameters.SOURCE_TABLE_AK_ID}) as EDW_count, 
		'Count of @{pipeline().parameters.SOURCE_TABLE_AK_ID} in @{pipeline().parameters.SOURCE_TABLE_NAME} table with crrnt_snpsht_flag = 0 that has Eff To Date as 12/31/2100 = ' + CONVERT(varchar,count(@{pipeline().parameters.SOURCE_TABLE_AK_ID})) as check_out_message,
		'@{pipeline().parameters.SOURCE_TABLE_NAME}' as target_name 
	FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME} 
		WHERE 
			crrnt_snpsht_flag = 0 and 
			eff_to_date = '2100-12-31 23:59:59.000'
	
	
	
	-- specific conditional checks moved to new pipeline as they were would not return a value with the Union statements above
),
EXP_AddDefaultInformation AS (
	SELECT
	EDW_count,
	check_out_message,
	@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID} AS wbmi_session_control_run_id,
	'InformS' AS created_user_id,
	SYSDATE AS created_date,
	'InformS' AS modified_user_id,
	SYSDATE AS modified_date,
	target_name,
	-- *INF*: 'E'
	-- 
	-- //E - Error, W - Warning
	'E' AS checkout_type_code
	FROM SQ_curr_row_count
),
FIL_FilterByEDWRecordCount AS (
	SELECT
	EDW_count, 
	check_out_message, 
	wbmi_session_control_run_id, 
	created_user_id, 
	created_date, 
	modified_user_id, 
	modified_date, 
	target_name, 
	checkout_type_code
	FROM EXP_AddDefaultInformation
	WHERE EDW_count>0
),
wbmi_checkout_curr_row_count AS (
	INSERT INTO wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, target_name, target_count, created_user_id, created_date, modified_user_id, modified_date)
	SELECT 
	WBMI_SESSION_CONTROL_RUN_ID, 
	CHECKOUT_TYPE_CODE, 
	check_out_message AS CHECKOUT_MESSAGE, 
	TARGET_NAME, 
	EDW_count AS TARGET_COUNT, 
	CREATED_USER_ID, 
	CREATED_DATE, 
	MODIFIED_USER_ID, 
	MODIFIED_DATE
	FROM FIL_FilterByEDWRecordCount
),
SQ_curr_row_count1 AS (
	IF '@{pipeline().parameters.SOURCE_TABLE_NAME}' = 'Claim_Party' 
	Begin 
	---- Count of Claim_Party_ak_id from Claim_Party with different Party_key for same Ak_id
	SELECT count(Claim_Party_ak_id) as EDW_count,'Count of Claim_Party_ak_id from Claim_Party with different Claim_Party_key for same Ak_id = ' + convert(varchar,count(Claim_Party_ak_id)) as check_out_message,'Claim_Party' as target_name
	FROM (SELECT Claim_Party_ak_id,count(Claim_Party_key) as Exp1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Party 
	      WHERE crrnt_snpsht_flag =1 
	      GROUP BY  Claim_Party_ak_id
	      HAVING count(Claim_Party_key) > 1) CP1
	End
	
	IF '@{pipeline().parameters.SOURCE_TABLE_NAME}' = 'Claim_Case'
	Begin 
	---- Count of Claim_Case_Ak_id from Claim_Case with different Claim_Case_key for same ak_id
	SELECT count(Claim_Case_Ak_id) as EDW_count,'Count of Claim_Case_ak_id from Claim_Case with different Claim_Case_key for same Ak_id = ' + convert(varchar,count(Claim_Case_Ak_id)) as check_out_message,'Claim_Case' as target_name
	FROM (SELECT Claim_Case_ak_id,count(Claim_Case_key) as Exp1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Case
	      WHERE crrnt_snpsht_flag =1 
	      GROUP BY  Claim_Case_ak_id
	      HAVING count(Claim_Case_key) > 1) CCase
	End
	
	IF '@{pipeline().parameters.SOURCE_TABLE_NAME}' = 'Claim_Occurrence'
	Begin 
	SELECT count(Claim_Occurrence_ak_id) as EDW_count,'Count of Claim_Occurrence_ak_id from Claim_Occurrence with different Claim_Occurrence_key for same Ak_id = ' + convert(varchar,count(Claim_Occurrence_ak_id)) as check_out_message,'Claim_Occurrence' as target_name
	FROM (SELECT Claim_Occurrence_ak_id,count(Claim_Occurrence_key) as Exp1 FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Occurrence 
	                WHERE crrnt_snpsht_flag =1 
	                GROUP BY Claim_Occurrence_ak_id
	                HAVING count(Claim_Occurrence_key) > 1) CO1
	End
	
	
	--Claim party occurrence where Claim occurrence ak id is 0
	IF '@{pipeline().parameters.SOURCE_TABLE_NAME}' = 'Claim_Party_Occurrence'
	Begin
	SELECT COUNT(*) as EDW_Count, 'Count of Claim_Party_Occurrence_ak_id from Claim_Party_Occurrence table where the Claim_Occurrence_ak_id = 0 is ' + CONVERT(varchar,COUNT(*))
	 as Check_Out_Message, 'Claim_Party_Occurrence' as Target_Name  FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Party_Occurrence WHERE claim_occurrence_ak_id = 0
	End
),
EXP_AddDefaultInformation1 AS (
	SELECT
	EDW_count,
	check_out_message,
	@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID} AS wbmi_session_control_run_id,
	'InformS' AS created_user_id,
	SYSDATE AS created_date,
	'InformS' AS modified_user_id,
	SYSDATE AS modified_date,
	target_name,
	-- *INF*: 'E'
	-- 
	-- //E - Error, W - Warning
	'E' AS checkout_type_code
	FROM SQ_curr_row_count1
),
FIL_FilterByEDWRecordCount1 AS (
	SELECT
	EDW_count, 
	check_out_message, 
	wbmi_session_control_run_id, 
	created_user_id, 
	created_date, 
	modified_user_id, 
	modified_date, 
	target_name, 
	checkout_type_code
	FROM EXP_AddDefaultInformation1
	WHERE EDW_count>0
),
wbmi_checkout_curr_row_count1 AS (
	INSERT INTO wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, target_name, target_count, created_user_id, created_date, modified_user_id, modified_date)
	SELECT 
	WBMI_SESSION_CONTROL_RUN_ID, 
	CHECKOUT_TYPE_CODE, 
	check_out_message AS CHECKOUT_MESSAGE, 
	TARGET_NAME, 
	EDW_count AS TARGET_COUNT, 
	CREATED_USER_ID, 
	CREATED_DATE, 
	MODIFIED_USER_ID, 
	MODIFIED_DATE
	FROM FIL_FilterByEDWRecordCount1
),
SQ_wbmi_checkout1 AS (
	select 
	wbmi_checkout.checkout_message + ' <BR> <BR> ',
	wbmi_batch_control_run.email_address
	from 
	dbo.wbmi_checkout wbmi_checkout,
	dbo.wbmi_session_control_run wbmi_session_control_run,
	dbo.wbmi_batch_control_run wbmi_batch_control_run
	where
	wbmi_checkout.checkout_type_code = 'E' and 
	wbmi_checkout.wbmi_session_control_run_id = wbmi_session_control_run.wbmi_session_control_run_id and
	wbmi_session_control_run.current_ind = 'Y'  and 
	wbmi_session_control_run.wbmi_batch_control_run_id = wbmi_batch_control_run.wbmi_batch_control_run_id and
	wbmi_checkout.wbmi_session_control_run_id = @{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID}
	order by wbmi_checkout_id
),
EXP_Email_Subject1 AS (
	SELECT
	email_address,
	checkout_message,
	-- *INF*: Abort('There are issues with the EDW data')
	Abort('There are issues with the EDW data'
	) AS error
	FROM SQ_wbmi_checkout1
),
FIL_STOP_PROCESSING AS (
	SELECT
	checkout_message, 
	error
	FROM EXP_Email_Subject1
	WHERE FALSE
),
wbmi_checkout_dummy_target AS (
	INSERT INTO wbmi_checkout
	(checkout_message)
	SELECT 
	CHECKOUT_MESSAGE
	FROM FIL_STOP_PROCESSING
),
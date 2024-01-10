WITH
SQ_EDW_current_row_count AS (
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
	
	
	
	-- specific conditional checks
	
	-- multi-key need one for each table because key field will come in blank and invalidate the SQ otherwise
	----Count of Pol_ak_id from Policy where there are different Pol_key for same Pol_ak_id
	
	IF '@{pipeline().parameters.SOURCE_TABLE_NAME}' = 'Policy'
	Begin
	
	SELECT count( Pol_Ak_id) as EDW_count,'Count of Pol_Ak_id from Policy with different pol_key for same Ak_id = ' + convert(varchar,count( Pol_Ak_id)) as check_out_message,'Policy' as target_name
	FROM (select pol_ak_id,count(pol_key) as EXp1 from V2.policy 
	      where crrnt_snpsht_flag =1 
	      group by pol_ak_id
	      having count(pol_key) > 1) PK
	end
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
	FROM SQ_EDW_current_row_count
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
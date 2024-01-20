WITH
SQ_SapiensReinsuranceHeaderExtract1 AS (
	SELECT COUNT(1) AS CNT , MIN(SOURCE_SEQ_NUM) AS MIN_SOURCE_SEQ_NUM ,MAX(SOURCE_SEQ_NUM) AS MAX_SOURCE_SEQ_NUM
	 FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SapiensReinsuranceHeaderExtract --
),
EXP_PASSTHROUGH AS (
	SELECT
	CNT,
	MIN_SOURCE_SEQ_NUM,
	MAX_SOURCE_SEQ_NUM
	FROM SQ_SapiensReinsuranceHeaderExtract1
),
SQL_Header_COUNTS AS (-- SQL_Header_COUNTS

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
EXP_TGT_PREP AS (
	SELECT
	-1 AS o_wbmi_session_control_run_id,
	'SapiensReinsuranceHeaderExtract' AS o_source_name,
	'RISRCINTRF' AS o_target_name,
	CNT_output AS SRC_CNT,
	TGT_CNT,
	-- *INF*: IIF(ISNULL(TGT_CNT) OR TGT_CNT = 0 , 1,TGT_CNT)/
	-- IIF(ISNULL(SRC_CNT) OR SRC_CNT = 0 ,1 ,SRC_CNT)*100
	IFF(TGT_CNT IS NULL OR TGT_CNT = 0, 1, TGT_CNT) / IFF(SRC_CNT IS NULL OR SRC_CNT = 0, 1, SRC_CNT) * 100 AS v_SRC_VS_TGT_CNT,
	CURRENT_TIMESTAMP AS o_source_dt,
	-- *INF*: IIF(v_SRC_VS_TGT_CNT>=@{pipeline().parameters.RISRCINTRF_THRESHOLD},'I','W')
	IFF(v_SRC_VS_TGT_CNT >= @{pipeline().parameters.RISRCINTRF_THRESHOLD}, 'I', 'W') AS o_checkout_type_code,
	'Percentage of records processed into target layer Sapiens compared with Source Datafeed Mart Layer is ' || v_SRC_VS_TGT_CNT AS o_checkout_message,
	CURRENT_TIMESTAMP AS o_target_dt,
	'InformS' AS o_created_user_id,
	CURRENT_TIMESTAMP AS o_created_date,
	'InformS' AS o_modified_user_id,
	CURRENT_TIMESTAMP AS o_modified_date,
	-1 AS o_AuditID,
	@{pipeline().parameters.RULE_ID_RISRCINTRF} AS o_WBMIChecksAndBalancingRuleID
	FROM SQL_Header_COUNTS
),
wbmi_checkout AS (
	INSERT INTO wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, source_name, target_name, source_count, target_count, source_dt, target_dt, created_user_id, created_date, modified_user_id, modified_date, AuditID, WBMIChecksAndBalancingRuleID)
	SELECT 
	o_wbmi_session_control_run_id AS WBMI_SESSION_CONTROL_RUN_ID, 
	o_checkout_type_code AS CHECKOUT_TYPE_CODE, 
	o_checkout_message AS CHECKOUT_MESSAGE, 
	o_source_name AS SOURCE_NAME, 
	o_target_name AS TARGET_NAME, 
	SRC_CNT AS SOURCE_COUNT, 
	TGT_CNT AS TARGET_COUNT, 
	o_source_dt AS SOURCE_DT, 
	o_target_dt AS TARGET_DT, 
	o_created_user_id AS CREATED_USER_ID, 
	o_created_date AS CREATED_DATE, 
	o_modified_user_id AS MODIFIED_USER_ID, 
	o_modified_date AS MODIFIED_DATE, 
	o_AuditID AS AUDITID, 
	o_WBMIChecksAndBalancingRuleID AS WBMICHECKSANDBALANCINGRULEID
	FROM EXP_TGT_PREP
),
SQ_SapiensReinsurancePaymentsExtract AS (
	SELECT COUNT(1) AS CNT , MIN(SOURCE_SEQ_NUM) AS MIN_SOURCE_SEQ_NUM ,MAX(SOURCE_SEQ_NUM) AS MAX_SOURCE_SEQ_NUM
	 FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SapiensReinsurancePaymentsExtract --
),
EXP_PASSTHROUGH_RISAIPINT AS (
	SELECT
	CNT,
	MIN_SOURCE_SEQ_NUM,
	MAX_SOURCE_SEQ_NUM
	FROM SQ_SapiensReinsurancePaymentsExtract
),
SQL_RISAIPINT_COUNTS AS (-- SQL_RISAIPINT_COUNTS

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
EXP_TGT_PREP_RISAIPINT AS (
	SELECT
	-1 AS o_wbmi_session_control_run_id,
	'SapiensReinsurancePaymentsExtract' AS o_source_name,
	'RISAIPINT' AS o_target_name,
	CNT_output AS SRC_CNT,
	TGT_CNT,
	-- *INF*: IIF(ISNULL(TGT_CNT) OR TGT_CNT = 0 , 1,TGT_CNT)/
	-- IIF(ISNULL(SRC_CNT) OR SRC_CNT = 0 ,1 ,SRC_CNT)*100
	IFF(TGT_CNT IS NULL OR TGT_CNT = 0, 1, TGT_CNT) / IFF(SRC_CNT IS NULL OR SRC_CNT = 0, 1, SRC_CNT) * 100 AS v_SRC_VS_TGT_CNT,
	-- *INF*: 'Percentage of records processed into target layer Sapiens compared with Source Datafeed Mart Layer is ' || v_SRC_VS_TGT_CNT
	-- 
	-- --'Count of Sapiens Payment table RISAIPINT  from datafeedmart SapiensReinsurancePaymentsExtract table'
	'Percentage of records processed into target layer Sapiens compared with Source Datafeed Mart Layer is ' || v_SRC_VS_TGT_CNT AS o_checkout_message,
	-- *INF*: iif(v_SRC_VS_TGT_CNT>=@{pipeline().parameters.RISAIPINT_THRESHOLD},'I','W')
	IFF(v_SRC_VS_TGT_CNT >= @{pipeline().parameters.RISAIPINT_THRESHOLD}, 'I', 'W') AS o_checkout_type_code,
	CURRENT_TIMESTAMP AS o_source_dt,
	CURRENT_TIMESTAMP AS o_target_dt,
	'InformS' AS o_created_user_id,
	CURRENT_TIMESTAMP AS o_created_date,
	'InformS' AS o_modified_user_id,
	CURRENT_TIMESTAMP AS o_modified_date,
	-1 AS o_AuditID,
	@{pipeline().parameters.RULE_ID_RISAIPINT} AS o_WBMIChecksAndBalancingRuleID
	FROM SQL_RISAIPINT_COUNTS
),
wbmi_checkout_RISAIPINT AS (
	INSERT INTO wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, source_name, target_name, source_count, target_count, source_dt, target_dt, created_user_id, created_date, modified_user_id, modified_date, AuditID, WBMIChecksAndBalancingRuleID)
	SELECT 
	o_wbmi_session_control_run_id AS WBMI_SESSION_CONTROL_RUN_ID, 
	o_checkout_type_code AS CHECKOUT_TYPE_CODE, 
	o_checkout_message AS CHECKOUT_MESSAGE, 
	o_source_name AS SOURCE_NAME, 
	o_target_name AS TARGET_NAME, 
	SRC_CNT AS SOURCE_COUNT, 
	TGT_CNT AS TARGET_COUNT, 
	o_source_dt AS SOURCE_DT, 
	o_target_dt AS TARGET_DT, 
	o_created_user_id AS CREATED_USER_ID, 
	o_created_date AS CREATED_DATE, 
	o_modified_user_id AS MODIFIED_USER_ID, 
	o_modified_date AS MODIFIED_DATE, 
	o_AuditID AS AUDITID, 
	o_WBMIChecksAndBalancingRuleID AS WBMICHECKSANDBALANCINGRULEID
	FROM EXP_TGT_PREP_RISAIPINT
),
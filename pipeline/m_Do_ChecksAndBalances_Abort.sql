WITH
SQ_WBMIChecksAndBalancingRule AS (
	SELECT WCABR.WBMIChecksAndBalancingRuleId, WCABR.WBMIBalancingSubjectAreaID, WCABR.WBMIBalancingLayerID, WCABR.WBMIActionStepID, WCABR.WBMIBalancingTypeID, WCABR.WBMIBalancingRuleDescription, WCABR.InformaticaSourceServerConnection, WCABR.WBMISourceSystemNameID, WCABR.SourceTable, WCABR.SourceSQL, WCABR.SourceSQLDescription, WCABR.InformaticaTargetServerConnection, WCABR.WBMITargetSystemNameID, WCABR.TargetTable, WCABR.TargetSQL, WCABR.TargetSQLDescription, WCABR.WBMIThresholdValue, WCABR.Frequency, WCABR.ActiveFlag, WCABR.CreatedDate, WCABR.ModifiedDate,
	WAS.WBMIActionStep,
	WBSA.WBMISubjectArea,
	WBT.WBMIBalancingType,
	WBL.WBMIBalancingLayer 
	FROM
	WBMIChecksAndBalancingRule WCABR
	INNER JOIN WBMIActionStep WAS on 
	WCABR.ActiveFlag=1 AND
	WCABR.WBMIActionStepID=WAS. WBMIActionStepID
	INNER JOIN WBMIBalancingSubjectArea WBSA on
	WCABR.WBMIBalancingSubjectAreaID=WBSA.WBMIBalancingSubjectAreaID 
	INNER JOIN WBMIBalancingType WBT 
	on WCABR.WBMIBalancingTypeID=WBT.WBMIBalancingTypeID
	INNER JOIN WBMIBalancingLayer WBL
	on WCABR.WBMIBalancingLayerID=WBL.WBMIBalancingLayerID
	@{pipeline().parameters.WHERE}
),
EXP_Get_Values AS (
	SELECT
	WBMIChecksAndBalancingRuleId,
	WBMISubjectAreaID,
	WBMIBalancingLayerID,
	WBMIActionStepID,
	WBMIBalancingTypeID,
	WBMIBalancingRuleDescription,
	InformaticaSourceServerConnection,
	WBMISourceSystemNameID,
	SourceTable,
	SourceSQL,
	-- *INF*: REPLACESTR(1, SourceSQL, '@{pipeline().parameters.NO_OF_DAY}', @{pipeline().parameters.NO_OF_DAY})
	REGEXP_REPLACE(SourceSQL,'@{pipeline().parameters.NO_OF_DAY}',@{pipeline().parameters.NO_OF_DAY}) AS v_SourceSQL,
	-- *INF*: REPLACESTR(1, v_SourceSQL, '@{pipeline().parameters.NO_OF_MONTH}', @{pipeline().parameters.NO_OF_MONTH})
	REGEXP_REPLACE(v_SourceSQL,'@{pipeline().parameters.NO_OF_MONTH}',@{pipeline().parameters.NO_OF_MONTH}) AS o_SourceSQL,
	SourceSQLDescription,
	InformaticaTargetServerConnection,
	WBMITargetSystemNameID,
	TargetTable,
	TargetSQL,
	-- *INF*: IIF(NOT ISNULL(TargetSQL),REPLACESTR(1,TargetSQL,'@{pipeline().parameters.NO_OF_DAY}',@{pipeline().parameters.NO_OF_DAY}) ,NULL)
	-- 
	IFF(TargetSQL IS NOT NULL,
		REGEXP_REPLACE(TargetSQL,'@{pipeline().parameters.NO_OF_DAY}',@{pipeline().parameters.NO_OF_DAY}),
		NULL
	) AS v_TargetSQL,
	-- *INF*: IIF(NOT ISNULL(v_TargetSQL),REPLACESTR(1,v_TargetSQL,'@{pipeline().parameters.NO_OF_MONTH}',@{pipeline().parameters.NO_OF_MONTH}) ,'select 0')
	-- 
	-- 
	-- --IIF(NOT ISNULL(TargetSQL),TargetSQL,'select 0')
	IFF(v_TargetSQL IS NOT NULL,
		REGEXP_REPLACE(v_TargetSQL,'@{pipeline().parameters.NO_OF_MONTH}',@{pipeline().parameters.NO_OF_MONTH}),
		'select 0'
	) AS o_TargetSQL,
	TargetSQLDescription,
	WBMIThresholdValue,
	Frequency,
	ActiveFlag,
	CreatedDate,
	ModifiedDate,
	WBMIActionStepCode,
	WBMISubjectArea,
	WBMIBalancingType,
	WBMIBalancingLayer
	FROM SQ_WBMIChecksAndBalancingRule
),
SQL_SourceCountActiveORDeleted AS (-- SQL_SourceCountActiveORDeleted

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
SQL_TargetCountActiveORDeleted AS (-- SQL_TargetCountActiveORDeleted

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
JNR_CountActiveORDeleted AS (SELECT
	SQL_SourceCountActiveORDeleted.WBMIChecksAndBalancingRuleId3_output AS SourceWBMIChecksAndBalancingRuleId, 
	SQL_SourceCountActiveORDeleted.WBMISubjectAreaID3_output AS SourceWBMISubjectAreaID, 
	SQL_SourceCountActiveORDeleted.WBMIBalancingLayerID3_output AS SourceWBMIBalancingLayerID, 
	SQL_SourceCountActiveORDeleted.WBMIActionStepID3_output AS SourceWBMIActionStepID, 
	SQL_SourceCountActiveORDeleted.WBMIBalancingTypeID3_output AS SourceWBMIBalancingTypeID, 
	SQL_SourceCountActiveORDeleted.WBMIActionStepCode3_output AS SourceWBMIActionStepCode, 
	SQL_SourceCountActiveORDeleted.WBMIBalancingType_output AS WBMIBalancingType, 
	SQL_SourceCountActiveORDeleted.WBMIThresholdValue3_output AS WBMIThresholdValue, 
	SQL_SourceCountActiveORDeleted.WBMIBalancingRuleDescription3_output AS WBMIBalancingRuleDescription, 
	SQL_SourceCountActiveORDeleted.SourceTable3_output AS SourceTable, 
	SQL_SourceCountActiveORDeleted.SourceOutPut, 
	SQL_TargetCountActiveORDeleted.WBMIChecksAndBalancingRuleId3_output AS TargetWBMIChecksAndBalancingRuleId, 
	SQL_TargetCountActiveORDeleted.WBMISubjectAreaID3_output AS TargetWBMISubjectAreaID, 
	SQL_TargetCountActiveORDeleted.WBMIBalancingLayerID3_output AS TargetWBMIBalancingLayerID, 
	SQL_TargetCountActiveORDeleted.WBMIActionStepID3_output AS TargetWBMIActionStepID, 
	SQL_TargetCountActiveORDeleted.WBMIBalancingTypeID3_output AS TargetWBMIBalancingTypeID, 
	SQL_TargetCountActiveORDeleted.TargetTable3_output AS TargetTable, 
	SQL_TargetCountActiveORDeleted.TargetOutput AS TargetOutPut, 
	SQL_SourceCountActiveORDeleted.SQLError AS SourceSQLError, 
	SQL_TargetCountActiveORDeleted.SQLError AS TargetSQLError
	FROM SQL_TargetCountActiveORDeleted
	INNER JOIN SQL_SourceCountActiveORDeleted
	ON SQL_SourceCountActiveORDeleted.WBMIChecksAndBalancingRuleId3_output = SQL_TargetCountActiveORDeleted.WBMIChecksAndBalancingRuleId3_output
),
EXP_CountActiveORDeleted AS (
	SELECT
	SourceWBMIChecksAndBalancingRuleId,
	TargetWBMIChecksAndBalancingRuleId,
	SourceTable,
	-- *INF*: IIF(in(WBMIBalancingType,'SupportID','Count','AKID','ActiveOrDeleted'),TO_DECIMAL(SourceOutPut),NULL)
	IFF(WBMIBalancingType IN ('SupportID','Count','AKID','ActiveOrDeleted'),
		CAST(SourceOutPut AS FLOAT),
		NULL
	) AS SourceCount,
	-- *INF*: IIF(WBMIBalancingType='Amount',TO_DECIMAL(SourceOutPut),NULL)
	IFF(WBMIBalancingType = 'Amount',
		CAST(SourceOutPut AS FLOAT),
		NULL
	) AS SourceAmount,
	TargetTable,
	-- *INF*: IIF( ISNULL(TargetTable),NULL,
	-- IIF(in(WBMIBalancingType,'SupportID','Count','AKID','ActiveOrDeleted'),TO_DECIMAL(TargetOutPut),NULL))
	IFF(TargetTable IS NULL,
		NULL,
		IFF(WBMIBalancingType IN ('SupportID','Count','AKID','ActiveOrDeleted'),
			CAST(TargetOutPut AS FLOAT),
			NULL
		)
	) AS TargetCount,
	-- *INF*: IIF( ISNULL(TargetTable),NULL,IIF(WBMIBalancingType='Amount',TO_DECIMAL(TargetOutPut),NULL))
	IFF(TargetTable IS NULL,
		NULL,
		IFF(WBMIBalancingType = 'Amount',
			CAST(TargetOutPut AS FLOAT),
			NULL
		)
	) AS TargetAmount,
	SourceWBMIActionStepCode AS WBMIActionStepCode,
	WBMIThresholdValue,
	WBMIBalancingType,
	WBMIBalancingRuleDescription,
	TargetOutPut-SourceOutPut AS v_TargetCount_SourceCount_diff,
	-- *INF*: IIF(ABS(v_TargetCount_SourceCount_diff)<=ABS(TO_DECIMAL(WBMIThresholdValue)),'I',WBMIActionStepCode)
	IFF(ABS(v_TargetCount_SourceCount_diff
		) <= ABS(CAST(WBMIThresholdValue AS FLOAT)
		),
		'I',
		WBMIActionStepCode
	) AS v_CheckOutTypeCode,
	v_CheckOutTypeCode AS CheckOutTypeCode,
	-- *INF*: ' For ' || TO_CHAR(SYSDATE,'MM/DD/YYYY') || WBMIBalancingRuleDescription|| v_TargetCount_SourceCount_diff
	-- 
	' For ' || TO_CHAR(SYSDATE, 'MM/DD/YYYY'
	) || WBMIBalancingRuleDescription || v_TargetCount_SourceCount_diff AS CheckOutMessage,
	-- *INF*: IIF(ISNULL(@{pipeline().parameters.NO_OF_MONTH}),NULL ,TRUNC(LAST_DAY(ADD_TO_DATE(SYSDATE,'MM',-TO_INTEGER(@{pipeline().parameters.NO_OF_MONTH}))),'DD'))
	IFF(@{pipeline().parameters.NO_OF_MONTH} IS NULL,
		NULL,
		CAST(TRUNC(LAST_DAY(DATEADD(MONTH,- CAST(@{pipeline().parameters.NO_OF_MONTH} AS INTEGER),SYSDATE)
		), 'DAY') AS TIMESTAMP_NTZ(0))
	) AS SourceDate,
	'InformS' AS CreatedModifiedUserID,
	SYSDATE AS CreatedModiFiedDate,
	@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID} AS wbmi_session_control_run_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SourceOutPut,
	TargetOutPut,
	SourceWBMIBalancingTypeID AS WBMIBalancingTypeID,
	SourceSQLError,
	-- *INF*: iif(not isnull(SourceSQLError),error(SourceSQLError))
	IFF(SourceSQLError IS NOT NULL,
		error(SourceSQLError
		)
	) AS v_SourceSQLError,
	TargetSQLError,
	-- *INF*: iif(not isnull(TargetSQLError),error(TargetSQLError))
	IFF(TargetSQLError IS NOT NULL,
		error(TargetSQLError
		)
	) AS v_TargetSQLError
	FROM JNR_CountActiveORDeleted
),
wbmi_checkout AS (
	INSERT INTO wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, source_name, target_name, source_amt, target_amt, source_count, target_count, source_dt, target_dt, created_user_id, created_date, modified_user_id, modified_date, auditId, wbmichecksandbalancingruleId)
	SELECT 
	WBMI_SESSION_CONTROL_RUN_ID, 
	CheckOutTypeCode AS CHECKOUT_TYPE_CODE, 
	CheckOutMessage AS CHECKOUT_MESSAGE, 
	SourceTable AS SOURCE_NAME, 
	TargetTable AS TARGET_NAME, 
	SourceAmount AS SOURCE_AMT, 
	TargetAmount AS TARGET_AMT, 
	SourceCount AS SOURCE_COUNT, 
	TargetCount AS TARGET_COUNT, 
	SourceDate AS SOURCE_DT, 
	SourceDate AS TARGET_DT, 
	CreatedModifiedUserID AS CREATED_USER_ID, 
	CreatedModiFiedDate AS CREATED_DATE, 
	CreatedModifiedUserID AS MODIFIED_USER_ID, 
	CreatedModiFiedDate AS MODIFIED_DATE, 
	AuditId AS AUDITID, 
	SourceWBMIChecksAndBalancingRuleId AS WBMICHECKSANDBALANCINGRULEID
	FROM EXP_CountActiveORDeleted
),
SQ_wbmi_checkout AS (
	select 
	wbmi_checkout.checkout_message + ' <BR> <BR> ',
	wbmi_batch_control_run.email_address
	from 
	dbo.wbmi_checkout wbmi_checkout,
	dbo.wbmi_session_control_run wbmi_session_control_run,
	dbo.wbmi_batch_control_run wbmi_batch_control_run
	where
	wbmi_checkout.checkout_type_code in ( 'E','W') and WBMIChecksAndBalancingRuleID is not null and
	wbmi_checkout.wbmi_session_control_run_id = wbmi_session_control_run.wbmi_session_control_run_id and
	wbmi_session_control_run.current_ind = 'Y'  and 
	wbmi_session_control_run.wbmi_batch_control_run_id = wbmi_batch_control_run.wbmi_batch_control_run_id and
	wbmi_checkout.wbmi_session_control_run_id = @{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID}
	order by wbmi_checkout_id
),
EXP_Email_Subject AS (
	SELECT
	email_address,
	checkout_message,
	-- *INF*: 'There are errors in the EDW Premium Financial data. Execution aborted (' || sysdate || ')'
	'There are errors in the EDW Premium Financial data. Execution aborted (' || sysdate || ')' AS email_subject
	FROM SQ_wbmi_checkout
),
AGG_Distinct_Email_Id AS (
	SELECT
	email_address,
	email_subject
	FROM EXP_Email_Subject
	QUALIFY ROW_NUMBER() OVER (PARTITION BY email_address, email_subject ORDER BY NULL) = 1
),
email_address AS (
	INSERT INTO email_address
	(FIELD1)
	SELECT 
	email_address AS FIELD1
	FROM AGG_Distinct_Email_Id
),
email_body AS (
	INSERT INTO email_body
	(FIELD1)
	SELECT 
	checkout_message AS FIELD1
	FROM EXP_Email_Subject
),
email_subject AS (
	INSERT INTO email_subject
	(FIELD1)
	SELECT 
	email_subject AS FIELD1
	FROM AGG_Distinct_Email_Id
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
	wbmi_checkout.checkout_type_code='E' and WBMIChecksAndBalancingRuleID is not null and
	wbmi_checkout.wbmi_session_control_run_id = wbmi_session_control_run.wbmi_session_control_run_id and
	wbmi_session_control_run.current_ind = 'Y'  and 
	wbmi_session_control_run.wbmi_batch_control_run_id = wbmi_batch_control_run.wbmi_batch_control_run_id and
	wbmi_checkout.wbmi_session_control_run_id = @{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID}
	order by wbmi_checkout_id
),
EXP_Email_Subject1 AS (
	SELECT
	email_address,
	checkout_type_code,
	checkout_message,
	-- *INF*: ABORT('There are issues with the EDW data')
	ABORT('There are issues with the EDW data'
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
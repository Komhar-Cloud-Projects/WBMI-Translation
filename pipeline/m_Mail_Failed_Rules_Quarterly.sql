WITH
SQ_wbmi_checkout AS (
	SELECT wbmi_checkout.wbmi_checkout_id
		,wbmi_checkout.checkout_message + ' <BR> <BR> '
		,wbmi_checkout.WBMIChecksAndBalancingRuleID
		,WBMIChecksAndBalancingRule.Frequency
	      ,WBMIBalancingSubjectArea.WBMISubjectArea
	      , WBMIBalancingLayer.WBMIBalancingLayer 
	      ,WBMIChecksAndBalancingRule.RuleLabel
	FROM dbo.wbmi_checkout
		,dbo.WBMIChecksAndBalancingRule
		,dbo.WBMIBalancingSubjectArea
		,dbo.WBMIBalancingLayer
	WHERE wbmi_checkout.WBMIChecksAndBalancingRuleID = WBMIChecksAndBalancingRule.WBMIChecksAndBalancingRuleId
		AND WBMIChecksAndBalancingRule.ActiveFlag = 1
	      AND wbmi_checkout.checkout_type_code in ('W', 'E')
		AND WBMIChecksAndBalancingRule.Frequency = '@{pipeline().parameters.FREQUENCY}'
		AND wbmi_checkout.wbmi_checkout_id > @{pipeline().parameters.WBMI_CHECKOUT_ID}
		AND WBMIBalancingSubjectArea.WBMIBalancingSubjectAreaID=WBMIChecksAndBalancingRule.WBMIBalancingSubjectAreaID
	AND WBMIChecksAndBalancingRule.WBMIBalancingLayerID = WBMIBalancingLayer.WBMIBalancingLayerID
	      @{pipeline().parameters.WHERE_CLAUSE}
	UNION ALL
	SELECT @{pipeline().parameters.WBMI_CHECKOUT_ID},'N/A',-1,'@{pipeline().parameters.FREQUENCY}','@{pipeline().parameters.WBMISUBJECTAREA}','',''
	UNION ALL
	SELECT @{pipeline().parameters.WBMI_CHECKOUT_ID},' If no error message, it suggests no rules failed during this period.',-1,'@{pipeline().parameters.FREQUENCY}','@{pipeline().parameters.WBMISUBJECTAREA}','',''
),
EXP_Failure_Message AS (
	SELECT
	WBMIChecksAndBalancingRuleID AS i_WBMIChecksAndBalancingRuleID,
	wbmi_checkout_id,
	Frequency,
	WBMISubjectArea,
	checkout_message AS checkout_message1,
	WBMIBalancingLayer,
	checkout_message,
	RuleLabel,
	-- *INF*: IIF(i_WBMIChecksAndBalancingRuleID=-1, checkout_message,
	-- 'SubjectArea: '  || WBMISubjectArea || ' , Rule Label: ' || RuleLabel || ' , Balancing Layer: '  || WBMIBalancingLayer   || 
	-- ' , '  ||  ' <BR> '  || 'Failure Rule ID: ' || TO_CHAR(i_WBMIChecksAndBalancingRuleID) ||' , Message: '|| checkout_message)
	IFF(
	    i_WBMIChecksAndBalancingRuleID = - 1, checkout_message,
	    'SubjectArea: ' || WBMISubjectArea || ' , Rule Label: ' || RuleLabel || ' , Balancing Layer: ' || WBMIBalancingLayer || ' , ' || ' <BR> ' || 'Failure Rule ID: ' || TO_CHAR(i_WBMIChecksAndBalancingRuleID) || ' , Message: ' || checkout_message
	) AS o_message
	FROM SQ_wbmi_checkout
),
AGG_Get_Max_CheckoutID AS (
	SELECT
	wbmi_checkout_id AS i_wbmi_checkout_id,
	Frequency,
	WBMISubjectArea,
	-- *INF*: MAX(i_wbmi_checkout_id)
	MAX(i_wbmi_checkout_id) AS o_wbmi_checkout_id
	FROM EXP_Failure_Message
	GROUP BY 
),
EXP_Orgnize_Parameters AS (
	SELECT
	Frequency,
	WBMISubjectArea AS i_WBMISubjectArea,
	o_wbmi_checkout_id AS i_wbmi_checkout_id,
	@{pipeline().parameters.WBMISUBJECTAREA} AS v_subject_area,
	-- *INF*: DECODE(TRUE,
	-- Frequency='D' AND i_WBMISubjectArea='DM-Sapiens-Daily-Policy','s_m_Mail_Failed_Rules_Datamart_Sapiens_Policy_Daily',
	-- Frequency='D' AND i_WBMISubjectArea='DM-Sapiens-Daily-Claim','s_m_Mail_Failed_Rules_Datamart_Sapiens_Claims_Daily',
	-- Frequency='M' AND i_WBMISubjectArea='SapiensPolicyMonthly','s_m_Mail_Failed_Rules_DataMart_Sapiens_Policy_Monthly',
	-- Frequency='D' AND i_WBMISubjectArea='DM-Sapiens-Daily-Validation','s_m_Mail_Failed_Rules_Datamart_Sapiens_Validation_Daily',
	-- Frequency='M' AND i_WBMISubjectArea='SapiensValidationMonthly','s_m_Mail_Failed_Rules_DataMart_Sapiens_Validation_Monthly',
	-- Frequency='D' AND i_WBMISubjectArea='DCTPolicy_DM','s_m_Mail_Failed_Rules_DCTPolicy_DM_Daily',
	-- Frequency='M' AND i_WBMISubjectArea='DCTPolicy_DM','s_m_Mail_Failed_Rules_DCTPolicy_DM_Monthly',
	-- Frequency='D' AND i_WBMISubjectArea='DCTPolicy_IDO','s_m_Mail_Failed_Rules_DCTPolicy_IDO',
	-- Frequency='D' AND IN(i_WBMISubjectArea,'DCT_Billing','Billing_Billing'),'s_m_Mail_Failed_Rules_DCT_To_Billing_Daily',
	-- Frequency='D' AND i_WBMISubjectArea='ODS-Billing-Daily','s_m_Mail_Failed_Rules_BillingODS_Daily',
	-- Frequency='M' AND i_WBMISubjectArea='ODS-Billing-Monthly','s_m_Mail_Failed_Rules_BillingODS_Monthly',
	-- Frequency='D' AND i_WBMISubjectArea='ReinsuranceODS','s_m_Mail_Failed_Rules_ReinsuranceODS',
	-- Frequency='D' and v_subject_area='UnassignedRules','s_m_Mail_Failed_Rules_Daily_UnassignedRules',
	-- Frequency='D','s_m_Mail_Failed_Rules_Daily',
	-- Frequency='W','s_m_Mail_Failed_Rules_Weekly',
	-- Frequency='M' and v_subject_area='UnassignedRules', 's_m_Mail_Failed_Rules_Monthly_UnassignedRules',
	-- Frequency='M','s_m_Mail_Failed_Rules_Monthly',
	-- Frequency='A','s_m_Mail_Failed_Rules_Historical',
	-- Frequency='Q','s_m_Mail_Failed_Rules_Quarterly'
	-- )
	DECODE(
	    TRUE,
	    Frequency = 'D' AND i_WBMISubjectArea = 'DM-Sapiens-Daily-Policy', 's_m_Mail_Failed_Rules_Datamart_Sapiens_Policy_Daily',
	    Frequency = 'D' AND i_WBMISubjectArea = 'DM-Sapiens-Daily-Claim', 's_m_Mail_Failed_Rules_Datamart_Sapiens_Claims_Daily',
	    Frequency = 'M' AND i_WBMISubjectArea = 'SapiensPolicyMonthly', 's_m_Mail_Failed_Rules_DataMart_Sapiens_Policy_Monthly',
	    Frequency = 'D' AND i_WBMISubjectArea = 'DM-Sapiens-Daily-Validation', 's_m_Mail_Failed_Rules_Datamart_Sapiens_Validation_Daily',
	    Frequency = 'M' AND i_WBMISubjectArea = 'SapiensValidationMonthly', 's_m_Mail_Failed_Rules_DataMart_Sapiens_Validation_Monthly',
	    Frequency = 'D' AND i_WBMISubjectArea = 'DCTPolicy_DM', 's_m_Mail_Failed_Rules_DCTPolicy_DM_Daily',
	    Frequency = 'M' AND i_WBMISubjectArea = 'DCTPolicy_DM', 's_m_Mail_Failed_Rules_DCTPolicy_DM_Monthly',
	    Frequency = 'D' AND i_WBMISubjectArea = 'DCTPolicy_IDO', 's_m_Mail_Failed_Rules_DCTPolicy_IDO',
	    Frequency = 'D' AND i_WBMISubjectArea IN ('DCT_Billing','Billing_Billing'), 's_m_Mail_Failed_Rules_DCT_To_Billing_Daily',
	    Frequency = 'D' AND i_WBMISubjectArea = 'ODS-Billing-Daily', 's_m_Mail_Failed_Rules_BillingODS_Daily',
	    Frequency = 'M' AND i_WBMISubjectArea = 'ODS-Billing-Monthly', 's_m_Mail_Failed_Rules_BillingODS_Monthly',
	    Frequency = 'D' AND i_WBMISubjectArea = 'ReinsuranceODS', 's_m_Mail_Failed_Rules_ReinsuranceODS',
	    Frequency = 'D' and v_subject_area = 'UnassignedRules', 's_m_Mail_Failed_Rules_Daily_UnassignedRules',
	    Frequency = 'D', 's_m_Mail_Failed_Rules_Daily',
	    Frequency = 'W', 's_m_Mail_Failed_Rules_Weekly',
	    Frequency = 'M' and v_subject_area = 'UnassignedRules', 's_m_Mail_Failed_Rules_Monthly_UnassignedRules',
	    Frequency = 'M', 's_m_Mail_Failed_Rules_Monthly',
	    Frequency = 'A', 's_m_Mail_Failed_Rules_Historical',
	    Frequency = 'Q', 's_m_Mail_Failed_Rules_Quarterly'
	) AS v_session_name,
	-- *INF*: '['||v_session_name||']'||CHR(10)||
	-- '@{pipeline().parameters.WBMI_CHECKOUT_ID}='||i_wbmi_checkout_id||CHR(10)||
	-- '@{pipeline().parameters.FREQUENCY}='||Frequency||CHR(10)||
	-- '@{pipeline().parameters.DBCONNECTION_SOURCE}='||@{pipeline().parameters.DBCONNECTION_SOURCE}||CHR(10)||
	-- '@{pipeline().parameters.ENV}='||@{pipeline().parameters.ENV}||CHR(10)||
	-- '@{pipeline().parameters.DBCONNECTION_SOURCE}='||@{pipeline().parameters.DBCONNECTION_SOURCE}||CHR(10)||
	-- '@{pipeline().parameters.PMFAILUREEMAILUSER}='||@{pipeline().parameters.PMFAILUREEMAILUSER}||CHR(10)||
	-- '@{pipeline().parameters.WBMISUBJECTAREA}='||@{pipeline().parameters.WBMISUBJECTAREA}||CHR(10)||
	-- '@{pipeline().parameters.WHERE_CLAUSE}='||@{pipeline().parameters.WHERE_CLAUSE}
	-- 
	'[' || v_session_name || ']' || CHR(10) || '@{pipeline().parameters.WBMI_CHECKOUT_ID}=' || i_wbmi_checkout_id || CHR(10) || '@{pipeline().parameters.FREQUENCY}=' || Frequency || CHR(10) || '@{pipeline().parameters.DBCONNECTION_SOURCE}=' || @{pipeline().parameters.DBCONNECTION_SOURCE} || CHR(10) || '@{pipeline().parameters.ENV}=' || @{pipeline().parameters.ENV} || CHR(10) || '@{pipeline().parameters.DBCONNECTION_SOURCE}=' || @{pipeline().parameters.DBCONNECTION_SOURCE} || CHR(10) || '@{pipeline().parameters.PMFAILUREEMAILUSER}=' || @{pipeline().parameters.PMFAILUREEMAILUSER} || CHR(10) || '@{pipeline().parameters.WBMISUBJECTAREA}=' || @{pipeline().parameters.WBMISUBJECTAREA} || CHR(10) || '@{pipeline().parameters.WHERE_CLAUSE}=' || @{pipeline().parameters.WHERE_CLAUSE} AS o_Param,
	@{pipeline().parameters.PMFAILUREEMAILUSER} AS o_EmailUser,
	-- *INF*: @{pipeline().parameters.ENV}||' : '||
	-- DECODE(TRUE,
	-- Frequency='D', 'Daily',
	-- Frequency='W', 'Weekly',
	-- Frequency='M', 'Monthly',
	-- Frequency='A', 'Historical',
	-- Frequency='Q', 'Quarterly',
	-- '')
	-- ||' Validation Report ('||SYSDATE||')'
	-- ||DECODE(TRUE,
	-- @{pipeline().parameters.WBMISUBJECTAREA}<>'',' - '||@{pipeline().parameters.WBMISUBJECTAREA},
	-- '')
	-- 
	@{pipeline().parameters.ENV} || ' : ' || DECODE(
	    TRUE,
	    Frequency = 'D', 'Daily',
	    Frequency = 'W', 'Weekly',
	    Frequency = 'M', 'Monthly',
	    Frequency = 'A', 'Historical',
	    Frequency = 'Q', 'Quarterly',
	    ''
	) || ' Validation Report (' || CURRENT_TIMESTAMP || ')' || DECODE(
	    TRUE,
	    @{pipeline().parameters.WBMISUBJECTAREA} <> '', ' - ' || @{pipeline().parameters.WBMISUBJECTAREA},
	    ''
	) AS o_EmailSubject
	FROM AGG_Get_Max_CheckoutID
),
email_subject AS (
	INSERT INTO email_subject
	(FIELD1)
	SELECT 
	o_EmailSubject AS FIELD1
	FROM EXP_Orgnize_Parameters
),
FIL_Valid_Message AS (
	SELECT
	checkout_message, 
	o_message
	FROM EXP_Failure_Message
	WHERE checkout_message<>'N/A'
),
email_body AS (
	INSERT INTO email_body
	(FIELD1)
	SELECT 
	o_message AS FIELD1
	FROM FIL_Valid_Message
),
email_address AS (
	INSERT INTO email_address
	(FIELD1)
	SELECT 
	o_EmailUser AS FIELD1
	FROM EXP_Orgnize_Parameters
),
Update_Parm AS (
	INSERT INTO Batch_Name_Parm
	(Parm_Data1)
	SELECT 
	o_Param AS PARM_DATA1
	FROM EXP_Orgnize_Parameters
),
WITH
SQ_PassThroughChargeTransaction1 AS (
	select W.WorkDCTDataRepairPolicyId,
	B.pol_key,
	A.PassThroughChargeTransactionID
	from @{pipeline().parameters.TARGET_TABLE_OWNER}.PassThroughChargeTransaction A
	inner join @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy B
	on A.PolicyAKID=B.pol_ak_id
	and B.source_sys_id='DCT'
	and A.SourceSystemID='DCT'
	and B.crrnt_snpsht_flag=1
	inner join StrategicProfitCenter spc on spc.StrategicProfitCenterAKId = B.StrategicProfitCenterAKId
	INNER JOIN @{pipeline().parameters.STAGE_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkDCTDataRepairPolicy W 
	ON W.PolicyKey = B.pol_key 
	AND W.createddate > '@{pipeline().parameters.SELECTION_START_TS}' 
	where ReasonAmendedCode not in ('CWO','Claw Back') AND spc.StrategicProfitCenterAbbreviation <> 'WB - PL' 
	and A.createddate>= '@{pipeline().parameters.SELECTION_END_TS}'
	and A.NegateRestateCode in ('Negate','Restate')
),
EXP_PassThroughChargeTransaction AS (
	SELECT
	WorkDCTDataRepairPolicyId,
	pol_key,
	PassThroughChargeTransactionID
	FROM SQ_PassThroughChargeTransaction1
),
SQ_WB_EDWInceptionToDate AS (
	Select B.PolicyKey, A.DCTTaxesChange
	from @{pipeline().parameters.DCT_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWInceptionToDate A
	inner join @{pipeline().parameters.DCT_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWDCTDataRepairPolicy B
	on A.PolicyNumber+A.PolicyVersion=B.PolicyKey
),
EXP_DCT_Tax AS (
	SELECT
	PolicyKey,
	DCTTaxesChange
	FROM SQ_WB_EDWInceptionToDate
),
SQ_PassThroughChargeTransaction AS (
	select B.Pol_key,sum(A.PassThroughChargeTransactionAmount) PassThroughChargeTransactionAmount 
	from @{pipeline().parameters.TARGET_TABLE_OWNER}.PassThroughChargeTransaction A
	inner join @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy B
	on A.PolicyAKID=B.pol_ak_id
	and B.source_sys_id='DCT'
	and A.SourceSystemID='DCT'
	and B.crrnt_snpsht_flag=1
	inner join StrategicProfitCenter spc on spc.StrategicProfitCenterAKId = B.StrategicProfitCenterAKId
	INNER JOIN @{pipeline().parameters.STAGE_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkDCTDataRepairPolicy W 
	ON W.PolicyKey = B.pol_key 
	AND W.createddate > '@{pipeline().parameters.SELECTION_START_TS}'
	where ReasonAmendedCode not in ('CWO','Claw Back') AND spc.StrategicProfitCenterAbbreviation <> 'WB - PL' 
	group by B.Pol_key
),
EXP_EDW_Tax AS (
	SELECT
	pol_key,
	PassThroughChargeTransactionAmount
	FROM SQ_PassThroughChargeTransaction
),
JNR_DCT_EDW AS (SELECT
	EXP_DCT_Tax.PolicyKey, 
	EXP_DCT_Tax.DCTTaxesChange, 
	EXP_EDW_Tax.pol_key, 
	EXP_EDW_Tax.PassThroughChargeTransactionAmount
	FROM EXP_DCT_Tax
	INNER JOIN EXP_EDW_Tax
	ON EXP_EDW_Tax.pol_key = EXP_DCT_Tax.PolicyKey
),
EXP_Balance_Flag AS (
	SELECT
	PolicyKey,
	DCTTaxesChange,
	pol_key,
	PassThroughChargeTransactionAmount,
	-- *INF*: IIF(abs(PassThroughChargeTransactionAmount-DCTTaxesChange)>@{pipeline().parameters.LIMIT},'UnBalanced','Balanced')
	IFF(abs(PassThroughChargeTransactionAmount - DCTTaxesChange
		) > @{pipeline().parameters.LIMIT},
		'UnBalanced',
		'Balanced'
	) AS Balance_Flag
	FROM JNR_DCT_EDW
),
RTR_DCT_EDW AS (
	SELECT
	PolicyKey,
	DCTTaxesChange,
	pol_key,
	PassThroughChargeTransactionAmount,
	Balance_Flag
	FROM EXP_Balance_Flag
),
RTR_DCT_EDW_Balanced AS (SELECT * FROM RTR_DCT_EDW WHERE Balance_Flag = 'Balanced'),
RTR_DCT_EDW_UnBalanced AS (SELECT * FROM RTR_DCT_EDW WHERE Balance_Flag = 'UnBalanced'),
EXP_Unbalanced_Policy AS (
	SELECT
	PolicyKey
	FROM RTR_DCT_EDW_UnBalanced
),
JNR_UnBalanced_PassThroughChargeTransaction AS (SELECT
	EXP_Unbalanced_Policy.PolicyKey, 
	EXP_PassThroughChargeTransaction.pol_key, 
	EXP_PassThroughChargeTransaction.PassThroughChargeTransactionID, 
	EXP_PassThroughChargeTransaction.WorkDCTDataRepairPolicyId
	FROM EXP_PassThroughChargeTransaction
	INNER JOIN EXP_Unbalanced_Policy
	ON EXP_Unbalanced_Policy.PolicyKey = EXP_PassThroughChargeTransaction.pol_key
),
EXP_UnBalance_PassThroughChargeTransactionid AS (
	SELECT
	PassThroughChargeTransactionID,
	WorkDCTDataRepairPolicyId
	FROM JNR_UnBalanced_PassThroughChargeTransaction
),
UPD_Delete_PassThroughChargeTransactionid AS (
	SELECT
	PassThroughChargeTransactionID
	FROM EXP_UnBalance_PassThroughChargeTransactionid
),
PassThroughChargeTransaction2 AS (
	DELETE FROM PassThroughChargeTransaction
	WHERE (PassThroughChargeTransactionID) IN (SELECT  PASSTHROUGHCHARGETRANSACTIONID FROM UPD_Delete_PassThroughChargeTransactionid)
),
AGG_DataRepair AS (
	SELECT
	WorkDCTDataRepairPolicyId
	FROM EXP_UnBalance_PassThroughChargeTransactionid
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WorkDCTDataRepairPolicyId ORDER BY NULL) = 1
),
UPD_Data_Repair_Delete AS (
	SELECT
	WorkDCTDataRepairPolicyId
	FROM AGG_DataRepair
),
WorkDCTDataRepairPolicy AS (
	DELETE FROM WorkDCTDataRepairPolicy
	WHERE (WorkDCTDataRepairPolicyId) IN (SELECT  WORKDCTDATAREPAIRPOLICYID FROM UPD_Data_Repair_Delete)
),
EDW_Data_Repair_Tax AS (
	INSERT INTO EDW_Data_Repair_Tax
	(PolicyKey, DCTTaxesChange, pol_key, PassThroughChargeTransactionAmount, Balance_Flag)
	SELECT 
	POLICYKEY, 
	DCTTAXESCHANGE, 
	POL_KEY, 
	PASSTHROUGHCHARGETRANSACTIONAMOUNT, 
	BALANCE_FLAG
	FROM EXP_Balance_Flag
),
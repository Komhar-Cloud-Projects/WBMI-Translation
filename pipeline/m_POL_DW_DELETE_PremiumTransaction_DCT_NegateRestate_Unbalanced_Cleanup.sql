WITH
SQ_WB_EDWInceptionToDate AS (
	Select B.PolicyKey, A.DCTWrittenChange
	from @{pipeline().parameters.DCT_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWInceptionToDate A
	inner join @{pipeline().parameters.DCT_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWDCTDataRepairPolicy B
	on A.PolicyNumber+A.PolicyVersion=B.PolicyKey
),
EXP_SRC_DCT AS (
	SELECT
	PolicyKey,
	DCTWrittenChange
	FROM SQ_WB_EDWInceptionToDate
),
SQ_PremiumTransaction AS (
	SELECT p.pol_key,
	SUM(PT.PremiumTransactionAmount) EDWDirectWrittenPremium
	FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT
	inner join @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy P
	on  P.pol_ak_id=cast(substring(PT.PremiumTransactionKey,1,charindex('~',PT.PremiumTransactionKey,1)-1) as bigint)
	and charindex('~',PT.PremiumTransactionKey,1)>0
	and len(substring(PT.PremiumTransactionKey,1,charindex('~',PT.PremiumTransactionKey,1)-1))>0
	and P.source_sys_id='DCT'
	and P.crrnt_snpsht_flag=1
	INNER JOIN @{pipeline().parameters.STAGE_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkDCTDataRepairPolicy W 
	ON W.PolicyKey = P.pol_key 
	AND W.createddate > '@{pipeline().parameters.SELECTION_START_TS}'
	WHERE  PT.SourceSystemID='DCT' AND PT.ReasonAmendedCode not in ('CWO','Claw Back')
	GROUP BY p.pol_key
),
EXP_SRC_EDW AS (
	SELECT
	pol_key,
	EDWDirectWrittenPremium
	FROM SQ_PremiumTransaction
),
JNR_EDW_DCT AS (SELECT
	EXP_SRC_EDW.pol_key, 
	EXP_SRC_EDW.EDWDirectWrittenPremium, 
	EXP_SRC_DCT.PolicyKey, 
	EXP_SRC_DCT.DCTWrittenChange
	FROM EXP_SRC_EDW
	INNER JOIN EXP_SRC_DCT
	ON EXP_SRC_DCT.PolicyKey = EXP_SRC_EDW.pol_key
),
EXP_Balance_Flag AS (
	SELECT
	pol_key,
	EDWDirectWrittenPremium,
	PolicyKey,
	DCTWrittenChange,
	-- *INF*: IIF(abs(EDWDirectWrittenPremium-DCTWrittenChange)>@{pipeline().parameters.LIMIT},'UnBalanced','Balanced')
	IFF(abs(EDWDirectWrittenPremium - DCTWrittenChange
		) > @{pipeline().parameters.LIMIT},
		'UnBalanced',
		'Balanced'
	) AS Balance_Flag
	FROM JNR_EDW_DCT
),
RTR_EDW_DCT AS (
	SELECT
	pol_key,
	EDWDirectWrittenPremium,
	PolicyKey,
	DCTWrittenChange,
	Balance_Flag
	FROM EXP_Balance_Flag
),
RTR_EDW_DCT_Balanced AS (SELECT * FROM RTR_EDW_DCT WHERE Balance_Flag='Balanced'),
RTR_EDW_DCT_Unbalanced AS (SELECT * FROM RTR_EDW_DCT WHERE Balance_Flag='UnBalanced'),
EXP_EDW_DCT AS (
	SELECT
	pol_key
	FROM RTR_EDW_DCT_Unbalanced
),
SQ_PremiumTransaction_Delete AS (
	select PT.PremiumTransactionid,PT.PremiumTransactionAkid,WPT.WorkPremiumTransactionid,AWPT.ArchWorkPremiumTransactionid,P.pol_key,W.WorkDCTDataRepairPolicyId,WPTN.WorkPremiumTransactionDataRepairNegateId,AWPTN.ArchWorkPremiumTransactionDataRepairNegateId
	FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT
	inner join @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy P
	on  P.pol_ak_id=cast(substring(PT.PremiumTransactionKey,1,charindex('~',PT.PremiumTransactionKey,1)-1) as bigint)
	and charindex('~',PT.PremiumTransactionKey,1)>0
	and len(substring(PT.PremiumTransactionKey,1,charindex('~',PT.PremiumTransactionKey,1)-1))>0
	and P.source_sys_id='DCT'
	and P.crrnt_snpsht_flag=1
	INNER JOIN @{pipeline().parameters.STAGE_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkDCTDataRepairPolicy W 
	ON W.PolicyKey = P.pol_key 
	AND W.createddate > '@{pipeline().parameters.SELECTION_START_TS}' 
	LEFT JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransaction WPT
	on PT.PremiumTransactionAkid=WPT.PremiumTransactionAkid
	INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWorkPremiumTransaction AWPT
	on PT.PremiumTransactionAkid=AWPT.PremiumTransactionAkid
	LEFT JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransactionDataRepairNegate WPTN
	on PT.PremiumTransactionAkid=WPTN.NewNegatePremiumTransactionAKID
	LEFT JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWorkPremiumTransactionDataRepairNegate AWPTN
	on PT.PremiumTransactionAkid=AWPTN.NewNegatePremiumTransactionAKID
	WHERE  PT.SourceSystemID='DCT' AND PT.ReasonAmendedCode not in ('CWO','Claw Back')
	and PT.createddate>= '@{pipeline().parameters.SELECTION_END_TS}'
	and AWPT.CreatedDate>= '@{pipeline().parameters.SELECTION_END_TS}'
	and PT.NegateRestateCode in ('Negate','Restate')
),
JNR_Delete_Transactions AS (SELECT
	EXP_EDW_DCT.pol_key, 
	SQ_PremiumTransaction_Delete.PremiumTransactionID, 
	SQ_PremiumTransaction_Delete.PremiumTransactionAKID, 
	SQ_PremiumTransaction_Delete.WorkPremiumTransactionId, 
	SQ_PremiumTransaction_Delete.ArchWorkPremiumTransactionId, 
	SQ_PremiumTransaction_Delete.pol_key AS pol_key1, 
	SQ_PremiumTransaction_Delete.WorkDCTDataRepairPolicyId, 
	SQ_PremiumTransaction_Delete.WorkPremiumTransactionDataRepairNegateId, 
	SQ_PremiumTransaction_Delete.ArchWorkPremiumTransactionDataRepairNegateId
	FROM EXP_EDW_DCT
	INNER JOIN SQ_PremiumTransaction_Delete
	ON SQ_PremiumTransaction_Delete.pol_key = EXP_EDW_DCT.pol_key
),
EXP_Delete_Transactions AS (
	SELECT
	PremiumTransactionID,
	PremiumTransactionAKID,
	WorkPremiumTransactionId,
	ArchWorkPremiumTransactionId,
	WorkDCTDataRepairPolicyId,
	WorkPremiumTransactionDataRepairNegateId,
	ArchWorkPremiumTransactionDataRepairNegateId
	FROM JNR_Delete_Transactions
),
UPD_Delete AS (
	SELECT
	PremiumTransactionID, 
	PremiumTransactionAKID, 
	WorkPremiumTransactionId, 
	ArchWorkPremiumTransactionId
	FROM EXP_Delete_Transactions
),
WorkDCTPremiumTransactionTracking AS (
	DELETE FROM WorkDCTPremiumTransactionTracking
	WHERE (PremiumTransactionID) IN (SELECT  PREMIUMTRANSACTIONID FROM UPD_Delete)
),
EDW_Data_Repair AS (
	INSERT INTO EDW_Data_Repair
	(pol_key, EDWDirectWrittenPremium, PolicyKey, DCTWrittenChange, Balance_Flag)
	SELECT 
	POL_KEY, 
	EDWDIRECTWRITTENPREMIUM, 
	POLICYKEY, 
	DCTWRITTENCHANGE, 
	BALANCE_FLAG
	FROM EXP_Balance_Flag
),
FIL_WorkPremiumTransactionDataRepairNegate AS (
	SELECT
	WorkPremiumTransactionDataRepairNegateId
	FROM EXP_Delete_Transactions
	WHERE NOT ISNULL(WorkPremiumTransactionDataRepairNegateId)
),
UPD_WorkPremiumTransactionDataRepairNegate_Delete AS (
	SELECT
	WorkPremiumTransactionDataRepairNegateId
	FROM FIL_WorkPremiumTransactionDataRepairNegate
),
WorkPremiumTransactionDataRepairNegate AS (
	DELETE FROM WorkPremiumTransactionDataRepairNegate
	WHERE (WorkPremiumTransactionDataRepairNegateId) IN (SELECT  WORKPREMIUMTRANSACTIONDATAREPAIRNEGATEID FROM UPD_WorkPremiumTransactionDataRepairNegate_Delete)
),
SRT_WorkDCTDataRepairPolicyID AS (
	SELECT
	WorkDCTDataRepairPolicyId
	FROM EXP_Delete_Transactions
	ORDER BY WorkDCTDataRepairPolicyId ASC
),
AGG_WorkDCTDataRepairPolicyID AS (
	SELECT
	WorkDCTDataRepairPolicyId
	FROM SRT_WorkDCTDataRepairPolicyID
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WorkDCTDataRepairPolicyId ORDER BY NULL) = 1
),
UPD_WorkDCTDataRepairPolicyID_Delete AS (
	SELECT
	WorkDCTDataRepairPolicyId
	FROM AGG_WorkDCTDataRepairPolicyID
),
WorkDCTDataRepairPolicy AS (
	DELETE FROM WorkDCTDataRepairPolicy
	WHERE (WorkDCTDataRepairPolicyId) IN (SELECT  WORKDCTDATAREPAIRPOLICYID FROM UPD_WorkDCTDataRepairPolicyID_Delete)
),
PremiumTransaction AS (
	DELETE FROM PremiumTransaction
	WHERE (PremiumTransactionID) IN (SELECT  PREMIUMTRANSACTIONID FROM UPD_Delete)
),
FIL_ArchWorkPremiumTransactionDataRepairNegate AS (
	SELECT
	ArchWorkPremiumTransactionDataRepairNegateId
	FROM EXP_Delete_Transactions
	WHERE NOT ISNULL(ArchWorkPremiumTransactionDataRepairNegateId)
),
UPD_ArchWorkPremiumTransactionDataRepairNegate_Delete AS (
	SELECT
	ArchWorkPremiumTransactionDataRepairNegateId
	FROM FIL_ArchWorkPremiumTransactionDataRepairNegate
),
ArchWorkPremiumTransactionDataRepairNegate AS (
	DELETE FROM ArchWorkPremiumTransactionDataRepairNegate
	WHERE (ArchWorkPremiumTransactionDataRepairNegateId) IN (SELECT  ARCHWORKPREMIUMTRANSACTIONDATAREPAIRNEGATEID FROM UPD_ArchWorkPremiumTransactionDataRepairNegate_Delete)
),
ArchWorkPremiumTransaction AS (
	DELETE FROM ArchWorkPremiumTransaction
	WHERE (ArchWorkPremiumTransactionId) IN (SELECT  ARCHWORKPREMIUMTRANSACTIONID FROM UPD_Delete)
),
FIL_WorkPremiumTransaction AS (
	SELECT
	WorkPremiumTransactionId
	FROM EXP_Delete_Transactions
	WHERE NOT ISNULL(WorkPremiumTransactionId)
),
UPD_WorkPremiumTransaction AS (
	SELECT
	WorkPremiumTransactionId
	FROM FIL_WorkPremiumTransaction
),
WorkPremiumTransaction AS (
	DELETE FROM WorkPremiumTransaction
	WHERE (WorkPremiumTransactionId) IN (SELECT  WORKPREMIUMTRANSACTIONID FROM UPD_WorkPremiumTransaction)
),
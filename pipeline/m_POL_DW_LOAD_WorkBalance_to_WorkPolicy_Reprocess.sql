WITH
SQ_WorkBalanceStageToEDWPolicyTransaction AS (
	SELECT DISTINCT WBSPT.StagePolicyKey, 
	WBSPT.AuditId
	FROM
	 @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkBalanceStageToEDWPolicyTransaction WBSPT
	WHERE WBSPT.AuditId=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Status_Default AS (
	SELECT
	StagePolicyKey,
	'REPROCESS' AS PolicyStatus,
	AuditId,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate
	FROM SQ_WorkBalanceStageToEDWPolicyTransaction
),
Work_PolicyTransactionStatus AS (
	INSERT INTO @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.Work_PolicyTransactionStatus
	(PolicyKey, PolicyStatus, AuditID, CreatedDate, ModifiedDate)
	SELECT 
	StagePolicyKey AS POLICYKEY, 
	POLICYSTATUS, 
	AuditId AS AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE
	FROM EXP_Status_Default
),
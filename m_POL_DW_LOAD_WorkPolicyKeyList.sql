WITH
SQ_pif_4514_stage AS (
	SELECT DISTINCT RTRIM(pif_symbol),
	       pif_policy_number,
	       pif_module,
	       logical_flag
	FROM  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME}
	WHERE logical_flag IN ('0','1','2','3')
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Values AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module,
	pif_symbol  ||  pif_policy_number  || pif_module AS PolicyKey,
	logical_flag,
	-- *INF*: TO_INTEGER(logical_flag)
	TO_INTEGER(logical_flag) AS logical_flag_Out,
	@{pipeline().parameters.SOURCE_TABLE_NAME} AS StageTableName,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate
	FROM SQ_pif_4514_stage
),
WorkPolicyKeyList AS (
	TRUNCATE TABLE WorkPolicyKeyList;
	INSERT INTO WorkPolicyKeyList
	(StageTableName, PolicyKey, LogicalFlag, AuditId, CreatedDate)
	SELECT 
	STAGETABLENAME, 
	POLICYKEY, 
	logical_flag_Out AS LOGICALFLAG, 
	AuditID AS AUDITID, 
	CREATEDDATE
	FROM EXP_Values
),
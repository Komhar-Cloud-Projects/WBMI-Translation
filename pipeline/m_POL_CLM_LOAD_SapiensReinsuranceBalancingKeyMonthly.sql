WITH
SQ_ArchSapiensReinsurancePolicy AS (
	select distinct PolicyKey as EntityKey,
	'Policy' as EntityValue
	from
	ArchSapiensReinsurancePolicy
	where auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_PreTarget AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	sysdate AS CreatedDate,
	sysdate AS ModifiedDate,
	EntityKey,
	EntityType
	FROM SQ_ArchSapiensReinsurancePolicy
),
SapiensReinsuranceBalancingKeyMonthly_Sapiens AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SapiensReinsuranceBalancingKeyMonthly;
	INSERT INTO @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SapiensReinsuranceBalancingKeyMonthly
	(AuditId, CreatedDate, ModifiedDate, EntityKey, EntityType)
	SELECT 
	AuditID AS AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	ENTITYKEY, 
	ENTITYTYPE
	FROM EXP_PreTarget
),
SapiensReinsuranceBalancingKeyMonthly_DM AS (
	TRUNCATE TABLE @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.SapiensReinsuranceBalancingKeyMonthly;
	INSERT INTO @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.SapiensReinsuranceBalancingKeyMonthly
	(AuditId, CreatedDate, ModifiedDate, EntityKey, EntityType)
	SELECT 
	AuditID AS AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	ENTITYKEY, 
	ENTITYTYPE
	FROM EXP_PreTarget
),
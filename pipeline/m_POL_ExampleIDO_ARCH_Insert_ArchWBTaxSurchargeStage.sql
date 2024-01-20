WITH
SQ_WBTaxSurchargeStage AS (
	SELECT
		WBTaxSurchargeStageId,
		ExtractDate,
		SourceSyStemId,
		TaxSurchargeId,
		WBTaxSurchargeId,
		SessionId,
		ChangeAttr,
		WrittenAttr,
		fValue,
		EntityType,
		premium
	FROM WBTaxSurchargeStage
),
EXP_Metadata AS (
	SELECT
	WBTaxSurchargeStageId,
	ExtractDate,
	SourceSyStemId,
	TaxSurchargeId,
	WBTaxSurchargeId,
	SessionId,
	ChangeAttr,
	WrittenAttr,
	fValue,
	EntityType,
	premium,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBTaxSurchargeStage
),
ArchWBTaxSurchargeStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBTaxSurchargeStage
	(WBTaxSurchargeStageId, ExtractDate, SourceSyStemId, AuditId, TaxSurchargeId, WBTaxSurchargeId, SessionId, ChangeAttr, WrittenAttr, fValue, EntityType, premium)
	SELECT 
	WBTAXSURCHARGESTAGEID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	TAXSURCHARGEID, 
	WBTAXSURCHARGEID, 
	SESSIONID, 
	CHANGEATTR, 
	WRITTENATTR, 
	FVALUE, 
	ENTITYTYPE, 
	PREMIUM
	FROM EXP_Metadata
),
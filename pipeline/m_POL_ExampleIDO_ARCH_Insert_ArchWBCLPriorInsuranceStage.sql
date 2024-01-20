WITH
SQ_WBCLPriorInsuranceStage AS (
	SELECT
		WBCLPriorInsuranceStageId,
		ExtractDate,
		SourceSystemId,
		WBPriorInsuranceId,
		WBCLPriorInsuranceId,
		SessionId,
		PriorCarrierProduct,
		PolicySymbol,
		PolicyMod
	FROM WBCLPriorInsuranceStage
),
EXP_Metadata AS (
	SELECT
	WBCLPriorInsuranceStageId,
	ExtractDate,
	SourceSystemId,
	WBPriorInsuranceId,
	WBCLPriorInsuranceId,
	SessionId,
	PriorCarrierProduct,
	PolicySymbol,
	PolicyMod,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBCLPriorInsuranceStage
),
ArchWBCLPriorInsuranceStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBCLPriorInsuranceStage
	(ExtractDate, SourceSystemId, AuditId, WBCLPriorInsuranceStageId, WBPriorInsuranceId, WBCLPriorInsuranceId, SessionId, PriorCarrierProduct, PolicySymbol, PolicyMod)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBCLPRIORINSURANCESTAGEID, 
	WBPRIORINSURANCEID, 
	WBCLPRIORINSURANCEID, 
	SESSIONID, 
	PRIORCARRIERPRODUCT, 
	POLICYSYMBOL, 
	POLICYMOD
	FROM EXP_Metadata
),
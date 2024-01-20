WITH
SQ_WBPriorInsuranceStage AS (
	SELECT
		WBPriorInsuranceStageId,
		PriorInsuranceId,
		WBPriorInsuranceId,
		SessionId,
		CarrierNameOther,
		ExperienceMod,
		LineOfBusiness,
		NoPriorInsurance2,
		ExtractDate,
		SourceSystemId
	FROM WBPriorInsuranceStage
),
EXP_ArchWBPriorInsuranceStage AS (
	SELECT
	WBPriorInsuranceStageId,
	PriorInsuranceId,
	WBPriorInsuranceId,
	SessionId,
	CarrierNameOther,
	ExperienceMod,
	LineOfBusiness,
	NoPriorInsurance2 AS i_NoPriorInsurance2,
	-- *INF*: DECODE(TRUE,
	-- i_NoPriorInsurance2='T',1,
	-- i_NoPriorInsurance2='F',0)
	DECODE(
	    TRUE,
	    i_NoPriorInsurance2 = 'T', 1,
	    i_NoPriorInsurance2 = 'F', 0
	) AS o_NoPriorInsurance2,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBPriorInsuranceStage
),
ArchWBPriorInsuranceStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBPriorInsuranceStage
	(WBPriorInsuranceStageId, PriorInsuranceId, WBPriorInsuranceId, SessionId, CarrierNameOther, ExperienceMod, LineOfBusiness, NoPriorInsurance2, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	WBPRIORINSURANCESTAGEID, 
	PRIORINSURANCEID, 
	WBPRIORINSURANCEID, 
	SESSIONID, 
	CARRIERNAMEOTHER, 
	EXPERIENCEMOD, 
	LINEOFBUSINESS, 
	o_NoPriorInsurance2 AS NOPRIORINSURANCE2, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_ArchWBPriorInsuranceStage
),
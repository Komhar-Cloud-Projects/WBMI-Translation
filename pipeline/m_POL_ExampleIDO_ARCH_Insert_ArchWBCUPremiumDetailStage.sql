WITH
SQ_WBCUPremiumDetailStage AS (
	SELECT
		WBCUPremiumDetailStageId,
		LineId,
		WBCUPremiumDetailId,
		SessionId,
		Type,
		Indicator,
		Million,
		ReinsurerForCL,
		ReinsurerForNSI,
		CommissionRate,
		PercentCeded,
		Override,
		RevisedFinalPremium,
		Include,
		Exclude,
		CertificateReceived,
		ReinsuranceEffectiveDate,
		ReinsuranceExpirationDate,
		FinalPremium,
		FinalPremiumWritten,
		FinalPremiumChange,
		ReinsurerPremium,
		ReinsurerFinalPremiumDisplay,
		TypeDuplicate,
		ExtractDate,
		SourceSystemId
	FROM WBCUPremiumDetailStage
),
EXP_Metadata AS (
	SELECT
	WBCUPremiumDetailStageId,
	LineId,
	WBCUPremiumDetailId,
	SessionId,
	Type,
	Indicator,
	-- *INF*: DECODE(Indicator, 'T', 1, 'F', 0, NULL)
	DECODE(
	    Indicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Indicator,
	Million,
	ReinsurerForCL,
	ReinsurerForNSI,
	CommissionRate,
	PercentCeded,
	Override,
	-- *INF*: DECODE(Override, 'T', 1, 'F', 0, NULL)
	DECODE(
	    Override,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Override,
	RevisedFinalPremium,
	Include,
	Exclude,
	CertificateReceived,
	ReinsuranceEffectiveDate,
	ReinsuranceExpirationDate,
	FinalPremium,
	FinalPremiumWritten,
	FinalPremiumChange,
	ReinsurerPremium,
	ReinsurerFinalPremiumDisplay,
	TypeDuplicate,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_WBCUPremiumDetailStage
),
ArchWBCUPremiumDetailStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBCUPremiumDetailStage
	(WBCUPremiumDetailStageId, LineId, WBCUPremiumDetailId, SessionId, Type, Indicator, Million, ReinsurerForCL, ReinsurerForNSI, CommissionRate, PercentCeded, Override, RevisedFinalPremium, Include, Exclude, CertificateReceived, ReinsuranceEffectiveDate, ReinsuranceExpirationDate, FinalPremium, FinalPremiumWritten, FinalPremiumChange, ReinsurerPremium, ReinsurerFinalPremiumDisplay, TypeDuplicate, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	WBCUPREMIUMDETAILSTAGEID, 
	LINEID, 
	WBCUPREMIUMDETAILID, 
	SESSIONID, 
	TYPE, 
	o_Indicator AS INDICATOR, 
	MILLION, 
	REINSURERFORCL, 
	REINSURERFORNSI, 
	COMMISSIONRATE, 
	PERCENTCEDED, 
	o_Override AS OVERRIDE, 
	REVISEDFINALPREMIUM, 
	INCLUDE, 
	EXCLUDE, 
	CERTIFICATERECEIVED, 
	REINSURANCEEFFECTIVEDATE, 
	REINSURANCEEXPIRATIONDATE, 
	FINALPREMIUM, 
	FINALPREMIUMWRITTEN, 
	FINALPREMIUMCHANGE, 
	REINSURERPREMIUM, 
	REINSURERFINALPREMIUMDISPLAY, 
	TYPEDUPLICATE, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID
	FROM EXP_Metadata
),
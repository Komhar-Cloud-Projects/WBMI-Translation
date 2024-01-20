WITH
SQ_DCCFCoveragePierOrWharfStaging AS (
	SELECT
		CoverageId,
		CF_CoveragePierOrWharfId,
		SessionId,
		PierOrWharfCauseOfLoss,
		PremiumBLDG,
		PremiumPP,
		PremiumPO,
		PremiumTIME,
		PremiumEE,
		ExtractDate,
		SourceSystemId
	FROM DCCFCoveragePierOrWharfStaging
),
EXP_Metadata AS (
	SELECT
	CoverageId,
	CF_CoveragePierOrWharfId,
	SessionId,
	PierOrWharfCauseOfLoss,
	PremiumBLDG,
	PremiumPP,
	PremiumPO,
	PremiumTIME,
	PremiumEE,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCFCoveragePierOrWharfStaging
),
archDCCFCoveragePierOrWharfStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCCFCoveragePierOrWharfStaging
	(CoverageId, CF_CoveragePierOrWharfId, SessionId, PierOrWharfCauseOfLoss, PremiumBLDG, PremiumPP, PremiumPO, PremiumTIME, PremiumEE, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	COVERAGEID, 
	CF_COVERAGEPIERORWHARFID, 
	SESSIONID, 
	PIERORWHARFCAUSEOFLOSS, 
	PREMIUMBLDG, 
	PREMIUMPP, 
	PREMIUMPO, 
	PREMIUMTIME, 
	PREMIUMEE, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),
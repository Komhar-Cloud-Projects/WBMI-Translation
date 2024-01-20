WITH
SQ_WBWCCoverageManualPremiumStage AS (
	SELECT
		WBWCCoverageManualPremiumStageId,
		ExtractDate,
		SourceSystemId,
		WCCoverageManualPremiumId,
		WBWCCoverageManualPremiumId,
		SessionId,
		ConsentToRate,
		CurrentRate,
		RateOverride
	FROM WBWCCoverageManualPremiumStage
),
EXP_Metadata AS (
	SELECT
	WBWCCoverageManualPremiumStageId AS i_WBWCCoverageManualPremiumStageId,
	ExtractDate AS i_ExtractDate,
	SourceSystemId AS i_SourceSystemId,
	WCCoverageManualPremiumId AS i_WCCoverageManualPremiumId,
	WBWCCoverageManualPremiumId AS i_WBWCCoverageManualPremiumId,
	SessionId AS i_SessionId,
	ConsentToRate AS i_ConsentToRate,
	CurrentRate AS i_CurrentRate,
	RateOverride AS i_RateOverride,
	i_WBWCCoverageManualPremiumStageId AS o_WBWCCoverageManualPremiumStageId,
	i_ExtractDate AS o_ExtractDate,
	i_SourceSystemId AS o_SourceSystemId,
	i_WCCoverageManualPremiumId AS o_WCCoverageManualPremiumId,
	i_WBWCCoverageManualPremiumId AS o_WBWCCoverageManualPremiumId,
	i_SessionId AS o_SessionId,
	-- *INF*: DECODE(i_ConsentToRate, 'T', 1, 'F', 0, NULL)
	DECODE(
	    i_ConsentToRate,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ConsentToRate,
	i_CurrentRate AS o_CurrentRate,
	i_RateOverride AS o_RateOverride,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBWCCoverageManualPremiumStage
),
ArchWBWCCoverageManualPremiumStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBWCCoverageManualPremiumStage
	(ExtractDate, SourceSystemId, AuditId, WBWCCoverageManualPremiumStageId, WCCoverageManualPremiumId, WBWCCoverageManualPremiumId, SessionId, ConsentToRate, CurrentRate, RateOverride)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	o_WBWCCoverageManualPremiumStageId AS WBWCCOVERAGEMANUALPREMIUMSTAGEID, 
	o_WCCoverageManualPremiumId AS WCCOVERAGEMANUALPREMIUMID, 
	o_WBWCCoverageManualPremiumId AS WBWCCOVERAGEMANUALPREMIUMID, 
	o_SessionId AS SESSIONID, 
	o_ConsentToRate AS CONSENTTORATE, 
	o_CurrentRate AS CURRENTRATE, 
	o_RateOverride AS RATEOVERRIDE
	FROM EXP_Metadata
),
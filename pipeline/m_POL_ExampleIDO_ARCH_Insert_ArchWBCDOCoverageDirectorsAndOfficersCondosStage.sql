WITH
SQ_WBCDOCoverageDirectorsAndOfficersCondosStage AS (
	SELECT
		WBCDOCoverageDirectorsAndOfficersCondosStageId,
		ExtractDate,
		SourceSystemId,
		CoverageId,
		WB_CDO_CoverageDirectorsAndOfficersCondosId,
		SessionId,
		RiskType,
		NumberOfUnits,
		RetroactiveDate
	FROM WBCDOCoverageDirectorsAndOfficersCondosStage
),
EXP_Metadata AS (
	SELECT
	WBCDOCoverageDirectorsAndOfficersCondosStageId,
	ExtractDate,
	SourceSystemId,
	CoverageId,
	WB_CDO_CoverageDirectorsAndOfficersCondosId,
	SessionId,
	RiskType,
	NumberOfUnits,
	RetroactiveDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBCDOCoverageDirectorsAndOfficersCondosStage
),
ArchWBCDOCoverageDirectorsAndOfficersCondosStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBCDOCoverageDirectorsAndOfficersCondosStage
	(ExtractDate, SourceSystemId, AuditId, WBCDOCoverageDirectorsAndOfficersCondosStageId, CoverageId, WB_CDO_CoverageDirectorsAndOfficersCondosId, SessionId, RiskType, NumberOfUnits, RetroactiveDate)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBCDOCOVERAGEDIRECTORSANDOFFICERSCONDOSSTAGEID, 
	COVERAGEID, 
	WB_CDO_COVERAGEDIRECTORSANDOFFICERSCONDOSID, 
	SESSIONID, 
	RISKTYPE, 
	NUMBEROFUNITS, 
	RETROACTIVEDATE
	FROM EXP_Metadata
),
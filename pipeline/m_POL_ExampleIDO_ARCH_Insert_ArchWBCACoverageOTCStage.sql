WITH
SQ_WBCACoverageOTCStage AS (
	SELECT
		WBCACoverageOTCStageId,
		ExtractDate,
		SourceSystemId,
		CA_CoverageOTCId,
		WB_CA_CoverageOTCId,
		SessionId,
		AntiTheftDeviceDiscountKY,
		AntiTheftDeviceDiscountMN,
		AcceptOTCCoverageSoftMsg,
		ReplacementCost,
		FullSafetyGlassCoverage,
		DeductibleType
	FROM WBCACoverageOTCStage
),
EXPTRANS AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	WBCACoverageOTCStageId,
	ExtractDate,
	SourceSystemId,
	CA_CoverageOTCId,
	WB_CA_CoverageOTCId,
	SessionId,
	AntiTheftDeviceDiscountKY,
	AntiTheftDeviceDiscountMN,
	AcceptOTCCoverageSoftMsg,
	ReplacementCost AS i_ReplacementCost,
	-- *INF*: decode(i_ReplacementCost,'T',1,'F',0,NULL)
	decode(
	    i_ReplacementCost,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ReplacementCost,
	FullSafetyGlassCoverage AS i_FullSafetyGlassCoverage,
	-- *INF*: decode(i_FullSafetyGlassCoverage,'T',1,'F',0,NULL)
	decode(
	    i_FullSafetyGlassCoverage,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_FullSafetyGlassCoverage,
	DeductibleType
	FROM SQ_WBCACoverageOTCStage
),
ArchWBCACoverageOTCStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBCACoverageOTCStage
	(ExtractDate, SourceSystemId, AuditId, WBCACoverageOTCStageId, CA_CoverageOTCId, WB_CA_CoverageOTCId, SessionId, AntiTheftDeviceDiscountKY, AntiTheftDeviceDiscountMN, AcceptOTCCoverageSoftMsg, ReplacementCost, FullSafetyGlassCoverage, DeductibleType)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBCACOVERAGEOTCSTAGEID, 
	CA_COVERAGEOTCID, 
	WB_CA_COVERAGEOTCID, 
	SESSIONID, 
	ANTITHEFTDEVICEDISCOUNTKY, 
	ANTITHEFTDEVICEDISCOUNTMN, 
	ACCEPTOTCCOVERAGESOFTMSG, 
	o_ReplacementCost AS REPLACEMENTCOST, 
	o_FullSafetyGlassCoverage AS FULLSAFETYGLASSCOVERAGE, 
	DEDUCTIBLETYPE
	FROM EXPTRANS
),
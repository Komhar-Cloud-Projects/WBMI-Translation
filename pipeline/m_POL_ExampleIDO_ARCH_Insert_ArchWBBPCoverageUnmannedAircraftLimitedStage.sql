WITH
SQ_WBBPCoverageUnmannedAircraftLimitedStage AS (
	SELECT
		WBBPCoverageUnmannedAircraftLimitedStageId,
		ExtractDate,
		SourceSystemid,
		CoverageId,
		WB_BP_CoverageUnmannedAircraftLimitedId,
		SessionId,
		BodilyInjuryAndPropertyDamageLimited,
		PersonalAndAdvertisingInjuryLimited
	FROM WBBPCoverageUnmannedAircraftLimitedStage
),
EXP_MetaData AS (
	SELECT
	WBBPCoverageUnmannedAircraftLimitedStageId,
	ExtractDate,
	SourceSystemid,
	CoverageId,
	WB_BP_CoverageUnmannedAircraftLimitedId,
	SessionId,
	BodilyInjuryAndPropertyDamageLimited,
	PersonalAndAdvertisingInjuryLimited,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBBPCoverageUnmannedAircraftLimitedStage
),
ArchWBBPCoverageUnmannedAircraftLimitedStage AS (
	INSERT INTO ArchWBBPCoverageUnmannedAircraftLimitedStage
	(ExtractDate, SourceSystemId, AuditId, WBBPCoverageUnmannedAircraftLimitedStageId, CoverageId, WB_BP_CoverageUnmannedAircraftLimitedId, SessionId, BodilyInjuryAndPropertyDamageLimited, PersonalAndAdvertisingInjuryLimited)
	SELECT 
	EXTRACTDATE, 
	SourceSystemid AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBBPCOVERAGEUNMANNEDAIRCRAFTLIMITEDSTAGEID, 
	COVERAGEID, 
	WB_BP_COVERAGEUNMANNEDAIRCRAFTLIMITEDID, 
	SESSIONID, 
	BODILYINJURYANDPROPERTYDAMAGELIMITED, 
	PERSONALANDADVERTISINGINJURYLIMITED
	FROM EXP_MetaData
),
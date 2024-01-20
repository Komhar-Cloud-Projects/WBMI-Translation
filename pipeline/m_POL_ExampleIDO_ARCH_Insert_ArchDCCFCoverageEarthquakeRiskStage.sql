WITH
SQ_DCCFCoverageEarthquakeRiskStage AS (
	SELECT
		DCCFCoverageEarthquakeRiskStageId,
		ExtractDate,
		SourceSystemId,
		CoverageId,
		CF_CoverageEarthquakeRiskId,
		SessionId,
		AdditionalEarthquake,
		ARate,
		BaseRate,
		LimitedEarthquake,
		NetRate,
		NetRateEE,
		Prem,
		PremiumRatingGroup,
		SteelFrame
	FROM DCCFCoverageEarthquakeRiskStage
),
EXP_Metadata AS (
	SELECT
	DCCFCoverageEarthquakeRiskStageId,
	ExtractDate,
	SourceSystemId,
	CoverageId,
	CF_CoverageEarthquakeRiskId,
	SessionId,
	AdditionalEarthquake AS i_AdditionalEarthquake,
	-- *INF*: DECODE(i_AdditionalEarthquake,'T','1','F','0')
	DECODE(
	    i_AdditionalEarthquake,
	    'T', '1',
	    'F', '0'
	) AS o_AdditionalEarthquake,
	ARate,
	BaseRate,
	LimitedEarthquake AS i_LimitedEarthquake,
	-- *INF*: DECODE(i_LimitedEarthquake,'T','1','F','0')
	DECODE(
	    i_LimitedEarthquake,
	    'T', '1',
	    'F', '0'
	) AS o_LimitedEarthquake,
	NetRate,
	NetRateEE,
	Prem,
	PremiumRatingGroup,
	SteelFrame AS i_SteelFrame,
	-- *INF*: DECODE(i_SteelFrame,'T','1','F','0')
	DECODE(
	    i_SteelFrame,
	    'T', '1',
	    'F', '0'
	) AS o_SteelFrame,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCFCoverageEarthquakeRiskStage
),
ArchDCCFCoverageEarthquakeRiskStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCFCoverageEarthquakeRiskStage
	(ExtractDate, SourceSystemId, AuditId, DCCFCoverageEarthquakeRiskStageId, CoverageId, CF_CoverageEarthquakeRiskId, SessionId, AdditionalEarthquake, ARate, BaseRate, LimitedEarthquake, NetRate, NetRateEE, Prem, PremiumRatingGroup, SteelFrame)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCCFCOVERAGEEARTHQUAKERISKSTAGEID, 
	COVERAGEID, 
	CF_COVERAGEEARTHQUAKERISKID, 
	SESSIONID, 
	o_AdditionalEarthquake AS ADDITIONALEARTHQUAKE, 
	ARATE, 
	BASERATE, 
	o_LimitedEarthquake AS LIMITEDEARTHQUAKE, 
	NETRATE, 
	NETRATEEE, 
	PREM, 
	PREMIUMRATINGGROUP, 
	o_SteelFrame AS STEELFRAME
	FROM EXP_Metadata
),
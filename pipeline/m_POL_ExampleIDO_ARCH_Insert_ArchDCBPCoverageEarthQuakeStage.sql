WITH
SQ_DCBPCoverageEarthQuakeStage AS (
	SELECT
		DCBPCoverageEarthQuakeStageId,
		ExtractDate,
		SourceSystemId,
		CoverageId,
		BP_CoverageEarthquakeId,
		SessionId,
		Additional,
		ARateAdditionalPremium,
		BlanketBuildingPremium,
		BlanketPersonalPropertyPremium,
		BuildingClass,
		ContentsGrade,
		Earthquake,
		EQMasonry,
		Limited,
		RoofTank,
		SubLimitBPPPremium,
		SubLimitPremium,
		SusceptibilityGrade,
		Territory,
		Zone
	FROM DCBPCoverageEarthQuakeStage
),
EXP_Metadata AS (
	SELECT
	DCBPCoverageEarthQuakeStageId,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	CoverageId,
	BP_CoverageEarthquakeId,
	SessionId,
	Additional,
	ARateAdditionalPremium,
	BlanketBuildingPremium,
	BlanketPersonalPropertyPremium,
	BuildingClass,
	ContentsGrade,
	Earthquake,
	EQMasonry AS i_EQMasonry,
	-- *INF*: IIF(i_EQMasonry='T','1','0')
	IFF(i_EQMasonry = 'T', '1', '0') AS o_EQMasonry,
	Limited AS i_Limited,
	-- *INF*: IIF(i_Limited='T','1','0')
	IFF(i_Limited = 'T', '1', '0') AS o_Limited,
	RoofTank AS i_RoofTank,
	-- *INF*: IIF(i_RoofTank='T','1','0')
	IFF(i_RoofTank = 'T', '1', '0') AS o_RoofTank,
	SubLimitBPPPremium,
	SubLimitPremium,
	SusceptibilityGrade,
	Territory,
	Zone
	FROM SQ_DCBPCoverageEarthQuakeStage
),
ArchDCBPCoverageEarthQuakeStage AS (
	INSERT INTO ArchDCBPCoverageEarthQuakeStage
	(ExtractDate, SourceSystemId, AuditId, DCBPCoverageEarthQuakeStageId, CoverageId, BP_CoverageEarthquakeId, SessionId, Additional, ARateAdditionalPremium, BlanketBuildingPremium, BlanketPersonalPropertyPremium, BuildingClass, ContentsGrade, Earthquake, EQMasonry, Limited, RoofTank, SubLimitBPPPremium, SubLimitPremium, SusceptibilityGrade, Territory, Zone)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCBPCOVERAGEEARTHQUAKESTAGEID, 
	COVERAGEID, 
	BP_COVERAGEEARTHQUAKEID, 
	SESSIONID, 
	ADDITIONAL, 
	ARATEADDITIONALPREMIUM, 
	BLANKETBUILDINGPREMIUM, 
	BLANKETPERSONALPROPERTYPREMIUM, 
	BUILDINGCLASS, 
	CONTENTSGRADE, 
	EARTHQUAKE, 
	o_EQMasonry AS EQMASONRY, 
	o_Limited AS LIMITED, 
	o_RoofTank AS ROOFTANK, 
	SUBLIMITBPPPREMIUM, 
	SUBLIMITPREMIUM, 
	SUSCEPTIBILITYGRADE, 
	TERRITORY, 
	ZONE
	FROM EXP_Metadata
),
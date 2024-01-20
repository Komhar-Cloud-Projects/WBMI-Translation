WITH
SQ_DC_BP_CoverageEarthquake AS (
	WITH cte_DCBPCoverageEarthquake(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.BP_CoverageEarthquakeId, 
	X.SessionId, 
	X.Additional, 
	X.ARateAdditionalPremium, 
	X.BlanketBuildingPremium, 
	X.BlanketPersonalPropertyPremium, 
	X.BuildingClass, 
	X.ContentsGrade, 
	X.Earthquake, 
	X.EQMasonry, 
	X.Limited, 
	X.RoofTank, 
	X.SubLimitBPPPremium, 
	X.SubLimitPremium, 
	X.SusceptibilityGrade, 
	X.Territory, 
	X.Zone 
	FROM
	 DC_BP_CoverageEarthquake X
	inner join
	cte_DCBPCoverageEarthquake Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
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
	IFF(i_EQMasonry = 'T', '1', '0') AS o_EQMasonry1,
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
	FROM SQ_DC_BP_CoverageEarthquake
),
DCBPCoverageEarthQuakeStage AS (
	TRUNCATE TABLE DCBPCoverageEarthQuakeStage;
	INSERT INTO DCBPCoverageEarthQuakeStage
	(ExtractDate, SourceSystemId, CoverageId, BP_CoverageEarthquakeId, SessionId, Additional, ARateAdditionalPremium, BlanketBuildingPremium, BlanketPersonalPropertyPremium, BuildingClass, ContentsGrade, Earthquake, EQMasonry, Limited, RoofTank, SubLimitBPPPremium, SubLimitPremium, SusceptibilityGrade, Territory, Zone)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
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
	o_EQMasonry1 AS EQMASONRY, 
	o_Limited AS LIMITED, 
	o_RoofTank AS ROOFTANK, 
	SUBLIMITBPPPREMIUM, 
	SUBLIMITPREMIUM, 
	SUSCEPTIBILITYGRADE, 
	TERRITORY, 
	ZONE
	FROM EXP_Metadata
),
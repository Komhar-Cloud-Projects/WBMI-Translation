WITH
SQ_DCBPRiskStage AS (
	SELECT
		DCBPRiskStageId,
		LineId,
		BPRiskId,
		SessionId,
		Id,
		Deleted,
		BeautyHairSalon,
		BuildingDescription,
		ComputerFraudAndFundsTransfer,
		ContractorsOneOrMoreResidences,
		DemolitionCost,
		Description,
		EarthquakeRiskType,
		EmployeeDishonesty,
		EndSpoilageSelected,
		IsOrdinanceOrLaw,
		OccupancyOccupied,
		OccupancyPercentage,
		RatingBasis,
		RatingBasisBuilding,
		RatingBasisPersonalProperty,
		Sinkhole,
		UsePredominantClassCode,
		WindHailExclusionType,
		BPBuildingXmlId,
		ExtractDate,
		SourceSystemId,
		BPBuildingID
	FROM DCBPRiskStage
),
EXP_Metadata AS (
	SELECT
	DCBPRiskStageId,
	LineId,
	BPRiskId,
	SessionId,
	Id,
	Deleted AS i_Deleted,
	-- *INF*: IIF(i_Deleted='T','1','0')
	IFF(i_Deleted = 'T', '1', '0') AS o_Deleted,
	BeautyHairSalon,
	BuildingDescription,
	ComputerFraudAndFundsTransfer AS i_ComputerFraudAndFundsTransfer,
	-- *INF*: IIF(i_ComputerFraudAndFundsTransfer='T','1','0')
	IFF(i_ComputerFraudAndFundsTransfer = 'T', '1', '0') AS o_ComputerFraudAndFundsTransfer,
	ContractorsOneOrMoreResidences AS i_ContractorsOneOrMoreResidences,
	-- *INF*: IIF(i_ContractorsOneOrMoreResidences='T','1','0')
	IFF(i_ContractorsOneOrMoreResidences = 'T', '1', '0') AS o_ContractorsOneOrMoreResidences,
	DemolitionCost,
	Description,
	EarthquakeRiskType,
	EmployeeDishonesty AS i_EmployeeDishonesty,
	-- *INF*: IIF(i_EmployeeDishonesty='T','1','0')
	IFF(i_EmployeeDishonesty = 'T', '1', '0') AS o_EmployeeDishonesty,
	EndSpoilageSelected AS i_EndSpoilageSelected,
	-- *INF*: IIF(i_EndSpoilageSelected='T','1','0')
	IFF(i_EndSpoilageSelected = 'T', '1', '0') AS o_EndSpoilageSelected,
	IsOrdinanceOrLaw AS i_IsOrdinanceOrLaw,
	-- *INF*: IIF(i_IsOrdinanceOrLaw='T','1','0')
	IFF(i_IsOrdinanceOrLaw = 'T', '1', '0') AS o_IsOrdinanceOrLaw,
	OccupancyOccupied,
	OccupancyPercentage,
	RatingBasis,
	RatingBasisBuilding,
	RatingBasisPersonalProperty,
	Sinkhole AS i_Sinkhole,
	-- *INF*: IIF(i_Sinkhole='T','1','0')
	IFF(i_Sinkhole = 'T', '1', '0') AS o_Sinkhole,
	UsePredominantClassCode AS i_UsePredominantClassCode,
	-- *INF*: IIF(i_UsePredominantClassCode='T','1','0')
	IFF(i_UsePredominantClassCode = 'T', '1', '0') AS o_UsePredominantClassCode,
	WindHailExclusionType,
	BPBuildingXmlId,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	BPBuildingID
	FROM SQ_DCBPRiskStage
),
ArchDCBPRiskStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCBPRiskStage
	(DCBPRiskStageId, LineId, BPRiskId, SessionId, Id, Deleted, BeautyHairSalon, BuildingDescription, ComputerFraudAndFundsTransfer, ContractorsOneOrMoreResidences, DemolitionCost, Description, EarthquakeRiskType, EmployeeDishonesty, EndSpoilageSelected, IsOrdinanceOrLaw, OccupancyOccupied, OccupancyPercentage, RatingBasis, RatingBasisBuilding, RatingBasisPersonalProperty, Sinkhole, UsePredominantClassCode, WindHailExclusionType, BPBuildingXmlId, ExtractDate, SourceSystemId, AuditId, BPBuildingID)
	SELECT 
	DCBPRISKSTAGEID, 
	LINEID, 
	BPRISKID, 
	SESSIONID, 
	ID, 
	o_Deleted AS DELETED, 
	BEAUTYHAIRSALON, 
	BUILDINGDESCRIPTION, 
	o_ComputerFraudAndFundsTransfer AS COMPUTERFRAUDANDFUNDSTRANSFER, 
	o_ContractorsOneOrMoreResidences AS CONTRACTORSONEORMORERESIDENCES, 
	DEMOLITIONCOST, 
	DESCRIPTION, 
	EARTHQUAKERISKTYPE, 
	o_EmployeeDishonesty AS EMPLOYEEDISHONESTY, 
	o_EndSpoilageSelected AS ENDSPOILAGESELECTED, 
	o_IsOrdinanceOrLaw AS ISORDINANCEORLAW, 
	OCCUPANCYOCCUPIED, 
	OCCUPANCYPERCENTAGE, 
	RATINGBASIS, 
	RATINGBASISBUILDING, 
	RATINGBASISPERSONALPROPERTY, 
	o_Sinkhole AS SINKHOLE, 
	o_UsePredominantClassCode AS USEPREDOMINANTCLASSCODE, 
	WINDHAILEXCLUSIONTYPE, 
	BPBUILDINGXMLID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	BPBUILDINGID
	FROM EXP_Metadata
),
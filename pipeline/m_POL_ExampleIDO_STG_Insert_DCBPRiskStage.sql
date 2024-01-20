WITH
SQ_DC_BP_Risk AS (
	WITH cte_DCBPRisk(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.BP_BuildingId, 
	X.BP_RiskId, 
	X.SessionId, 
	X.Id, 
	X.BeautyHairSalon, 
	X.BuildingDescription, 
	X.ComputerFraudAndFundsTransfer, 
	X.ContractorsOneOrMoreResidences, 
	X.DemolitionCost, 
	X.Description, 
	X.EarthquakeRiskType, 
	X.EmployeeDishonesty, 
	X.EndSpoilageSelected, 
	X.IsOrdinanceOrLaw, 
	X.OccupancyOccupied, 
	X.OccupancyPercentage, 
	X.RatingBasis, 
	X.RatingBasisBuilding, 
	X.RatingBasisPersonalProperty, 
	X.Sinkhole, 
	X.UsePredominantClassCode, 
	X.WindHailExclusionType, 
	X.BP_BuildingXmlId, 
	X.Deleted 
	FROM
	DC_BP_Risk X
	inner join
	cte_DCBPRisk Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	LineId,
	BP_BuildingId,
	BP_RiskId,
	SessionId,
	Id,
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
	BP_BuildingXmlId,
	Deleted AS i_Deleted,
	-- *INF*: IIF(i_Deleted='T','1','0')
	IFF(i_Deleted = 'T', '1', '0') AS o_Deleted,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_BP_Risk
),
DCBPRiskStage4 AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCBPRiskStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCBPRiskStage
	(LineId, BPRiskId, SessionId, Id, Deleted, BeautyHairSalon, BuildingDescription, ComputerFraudAndFundsTransfer, ContractorsOneOrMoreResidences, DemolitionCost, Description, EarthquakeRiskType, EmployeeDishonesty, EndSpoilageSelected, IsOrdinanceOrLaw, OccupancyOccupied, OccupancyPercentage, RatingBasis, RatingBasisBuilding, RatingBasisPersonalProperty, Sinkhole, UsePredominantClassCode, WindHailExclusionType, BPBuildingXmlId, ExtractDate, SourceSystemId, BPBuildingID)
	SELECT 
	LINEID, 
	BP_RiskId AS BPRISKID, 
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
	BP_BuildingXmlId AS BPBUILDINGXMLID, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	BP_BuildingId AS BPBUILDINGID
	FROM EXP_Metadata
),
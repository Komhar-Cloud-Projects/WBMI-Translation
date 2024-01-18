WITH
SQ_DC_BP_Location AS (
	WITH cte_DCBPLocation(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.BP_LocationId, 
	X.SessionId, 
	X.Id, 
	X.BuildingAutomaticIncrease, 
	X.BuildingCodeEffectivenessGrading, 
	X.ComputerFraudApplicable, 
	X.Description, 
	X.DesignatedLimitApplicable, 
	X.ElectronicCommerceApplicable, 
	X.EmployeeDishonestyApplicable, 
	X.FLCatastrophicGroundCoverCollapseCounty, 
	X.Territory, 
	X.TerrorismTerr,
	X.Number   
	FROM
	DC_BP_Location X
	inner join
	cte_DCBPLocation Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXPTRANS AS (
	SELECT
	BP_LocationId,
	SessionId,
	Id,
	BuildingAutomaticIncrease,
	BuildingCodeEffectivenessGrading,
	ComputerFraudApplicable AS i_ComputerFraudApplicable,
	-- *INF*: IIF(i_ComputerFraudApplicable='T',1,0)
	IFF(i_ComputerFraudApplicable = 'T', 1, 0) AS o_ComputerFraudApplicable,
	Description,
	DesignatedLimitApplicable AS i_DesignatedLimitApplicable,
	-- *INF*: IIF(i_DesignatedLimitApplicable='T',1,0)
	IFF(i_DesignatedLimitApplicable = 'T', 1, 0) AS o_DesignatedLimitApplicable,
	ElectronicCommerceApplicable AS i_ElectronicCommerceApplicable,
	-- *INF*: IIF(i_ElectronicCommerceApplicable='T',1,0)
	IFF(i_ElectronicCommerceApplicable = 'T', 1, 0) AS o_ElectronicCommerceApplicable,
	EmployeeDishonestyApplicable AS i_EmployeeDishonestyApplicable,
	-- *INF*: IIF(i_EmployeeDishonestyApplicable='T',1,0)
	IFF(i_EmployeeDishonestyApplicable = 'T', 1, 0) AS o_EmployeeDishonestyApplicable,
	FLCatastrophicGroundCoverCollapseCounty,
	Territory,
	TerrorismTerr,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	Number
	FROM SQ_DC_BP_Location
),
DCBPLocationStage AS (
	TRUNCATE TABLE DCBPLocationStage;
	INSERT INTO DCBPLocationStage
	(ExtractDate, SourceSystemId, BPLocationId, SessionId, Id, BuildingAutomaticIncrease, BuildingCodeEffectivenessGrading, ComputerFraudApplicable, Description, DesignatedLimitApplicable, ElectronicCommerceApplicable, EmployeeDishonestyApplicable, FLCatastrophicGroundCoverCollapseCounty, Territory, TerrorismTerr, Number)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	BP_LocationId AS BPLOCATIONID, 
	SESSIONID, 
	ID, 
	BUILDINGAUTOMATICINCREASE, 
	BUILDINGCODEEFFECTIVENESSGRADING, 
	o_ComputerFraudApplicable AS COMPUTERFRAUDAPPLICABLE, 
	DESCRIPTION, 
	o_DesignatedLimitApplicable AS DESIGNATEDLIMITAPPLICABLE, 
	o_ElectronicCommerceApplicable AS ELECTRONICCOMMERCEAPPLICABLE, 
	o_EmployeeDishonestyApplicable AS EMPLOYEEDISHONESTYAPPLICABLE, 
	FLCATASTROPHICGROUNDCOVERCOLLAPSECOUNTY, 
	TERRITORY, 
	TERRORISMTERR, 
	NUMBER
	FROM EXPTRANS
),
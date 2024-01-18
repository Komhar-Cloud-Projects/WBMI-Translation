WITH
SQ_DCBPLocationStage AS (
	SELECT
		DCBPLocationStageId,
		BPLocationId,
		SessionId,
		Id,
		BuildingAutomaticIncrease,
		BuildingCodeEffectivenessGrading,
		ComputerFraudApplicable,
		Description,
		DesignatedLimitApplicable,
		ElectronicCommerceApplicable,
		EmployeeDishonestyApplicable,
		FLCatastrophicGroundCoverCollapseCounty,
		Territory,
		TerrorismTerr,
		ExtractDate,
		SourceSystemId,
		Number
	FROM DCBPLocationStage
),
EXPTRANS AS (
	SELECT
	DCBPLocationStageId,
	BPLocationId,
	SessionId,
	Id,
	BuildingAutomaticIncrease,
	BuildingCodeEffectivenessGrading,
	ComputerFraudApplicable AS i_ComputerFraudApplicable,
	-- *INF*: IIF(i_ComputerFraudApplicable='T','1','0')
	IFF(i_ComputerFraudApplicable = 'T', '1', '0') AS o_ComputerFraudApplicable,
	Description,
	DesignatedLimitApplicable AS i_DesignatedLimitApplicable,
	-- *INF*: IIF(i_DesignatedLimitApplicable='T','1','0')
	IFF(i_DesignatedLimitApplicable = 'T', '1', '0') AS o_DesignatedLimitApplicable,
	ElectronicCommerceApplicable AS i_ElectronicCommerceApplicable,
	-- *INF*: IIF(i_ElectronicCommerceApplicable='T','1','0')
	IFF(i_ElectronicCommerceApplicable = 'T', '1', '0') AS o_ElectronicCommerceApplicable,
	EmployeeDishonestyApplicable AS i_EmployeeDishonestyApplicable,
	-- *INF*: IIF(i_EmployeeDishonestyApplicable='T','1','0')
	IFF(i_EmployeeDishonestyApplicable = 'T', '1', '0') AS o_EmployeeDishonestyApplicable,
	FLCatastrophicGroundCoverCollapseCounty,
	Territory,
	TerrorismTerr,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	Number
	FROM SQ_DCBPLocationStage
),
ArchDCBPLocationStage AS (
	INSERT INTO ArchDCBPLocationStage
	(ExtractDate, SourceSystemId, AuditId, DCBPLocationStageId, BPLocationId, SessionId, Id, BuildingAutomaticIncrease, BuildingCodeEffectivenessGrading, ComputerFraudApplicable, Description, DesignatedLimitApplicable, ElectronicCommerceApplicable, EmployeeDishonestyApplicable, FLCatastrophicGroundCoverCollapseCounty, Territory, TerrorismTerr, Number)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCBPLOCATIONSTAGEID, 
	BPLOCATIONID, 
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
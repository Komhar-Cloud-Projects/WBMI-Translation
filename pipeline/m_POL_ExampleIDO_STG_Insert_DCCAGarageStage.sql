WITH
SQ_DC_CA_Garage AS (
	WITH cte_DCCAGarage(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CA_RiskId, 
	X.CA_GarageId, 
	X.SessionId, 
	X.Id, 
	X.Auditable, 
	X.AutoServicesClassification, 
	X.CoverageType, 
	X.DamageToRentedPremisesLiability, 
	X.GarageKeepersServiceOperationType, 
	X.GarageType, 
	X.OwnerOfPremesisName, 
	X.PickupOrDeliveryOfAutos, 
	X.UseBlanketCollisionAveragedValues, 
	X.ValetParking 
	FROM
	DC_CA_Garage X
	inner join
	cte_DCCAGarage Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CA_RiskId,
	CA_GarageId,
	SessionId,
	Id,
	Auditable,
	AutoServicesClassification,
	CoverageType,
	DamageToRentedPremisesLiability,
	GarageKeepersServiceOperationType,
	GarageType,
	OwnerOfPremesisName,
	PickupOrDeliveryOfAutos,
	UseBlanketCollisionAveragedValues,
	ValetParking
	FROM SQ_DC_CA_Garage
),
DCCAGarageStage AS (
	TRUNCATE TABLE DCCAGarageStage;
	INSERT INTO DCCAGarageStage
	(ExtractDate, SourceSystemId, CARiskId, CAGarageId, SessionId, Id, Auditable, AutoServicesClassification, CoverageType, DamageToRentedPremisesLiability, GarageKeepersServiceOperationType, GarageType, OwnerOfPremesisName, PickupOrDeliveryOfAutos, UseBlanketCollisionAveragedValues, ValetParking)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CA_RiskId AS CARISKID, 
	CA_GarageId AS CAGARAGEID, 
	SESSIONID, 
	ID, 
	AUDITABLE, 
	AUTOSERVICESCLASSIFICATION, 
	COVERAGETYPE, 
	DAMAGETORENTEDPREMISESLIABILITY, 
	GARAGEKEEPERSSERVICEOPERATIONTYPE, 
	GARAGETYPE, 
	OWNEROFPREMESISNAME, 
	PICKUPORDELIVERYOFAUTOS, 
	USEBLANKETCOLLISIONAVERAGEDVALUES, 
	VALETPARKING
	FROM EXP_Metadata
),
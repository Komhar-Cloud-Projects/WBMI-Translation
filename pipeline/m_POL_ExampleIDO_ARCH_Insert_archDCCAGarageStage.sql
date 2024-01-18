WITH
SQ_DCCAGarageStage AS (
	SELECT
		DCCAGarageStageId,
		ExtractDate,
		SourceSystemId,
		CARiskId,
		CAGarageId,
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
	FROM DCCAGarageStage
),
EXP_Metadata AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	DCCAGarageStageId,
	ExtractDate,
	SourceSystemId,
	CARiskId,
	CAGarageId,
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
	FROM SQ_DCCAGarageStage
),
ArchDCCAGarageStage AS (
	INSERT INTO ArchDCCAGarageStage
	(ExtractDate, SourceSystemId, AuditId, DCCAGarageStageId, CARiskId, CAGarageId, SessionId, Id, Auditable, AutoServicesClassification, CoverageType, DamageToRentedPremisesLiability, GarageKeepersServiceOperationType, GarageType, OwnerOfPremesisName, PickupOrDeliveryOfAutos, UseBlanketCollisionAveragedValues, ValetParking)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCCAGARAGESTAGEID, 
	CARISKID, 
	CAGARAGEID, 
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
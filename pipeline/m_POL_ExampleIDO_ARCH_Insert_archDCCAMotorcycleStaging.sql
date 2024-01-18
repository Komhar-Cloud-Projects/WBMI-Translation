WITH
SQ_DCCAMotorcycleStaging AS (
	SELECT
		DCCAMotorcycleStagingId,
		ExtractDate,
		SourceSystemId,
		CA_VehicleId,
		CA_MotorcycleId,
		SessionId,
		Id,
		EngineSize
	FROM DCCAMotorcycleStaging
),
EXP_Metadata AS (
	SELECT
	DCCAMotorcycleStagingId,
	ExtractDate,
	SourceSystemId,
	CA_VehicleId,
	CA_MotorcycleId,
	SessionId,
	Id,
	EngineSize,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCAMotorcycleStaging
),
ArchDCCAMotorcycleStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCAMotorcycleStaging
	(ExtractDate, SourceSystemId, AuditId, CA_VehicleId, CA_MotorcycleId, SessionId, Id, EngineSize)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	CA_VEHICLEID, 
	CA_MOTORCYCLEID, 
	SESSIONID, 
	ID, 
	ENGINESIZE
	FROM EXP_Metadata
),
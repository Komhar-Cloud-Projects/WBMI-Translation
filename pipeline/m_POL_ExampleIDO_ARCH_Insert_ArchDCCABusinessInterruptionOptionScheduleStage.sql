WITH
SQ_DCCABusinessInterruptionOptionScheduleStage AS (
	SELECT
		DCCABusinessInterruptionOptionScheduleStageId,
		ExtractDate,
		SourceSystemid,
		CA_BusinessInterruptionOptionId,
		CA_BusinessInterruptionOptionScheduleId,
		SessionId,
		Id,
		DescriptionOfScheduledProperty
	FROM DCCABusinessInterruptionOptionScheduleStage
),
EXP_MetaData AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	DCCABusinessInterruptionOptionScheduleStageId,
	ExtractDate,
	SourceSystemid,
	CA_BusinessInterruptionOptionId,
	CA_BusinessInterruptionOptionScheduleId,
	SessionId,
	Id,
	DescriptionOfScheduledProperty
	FROM SQ_DCCABusinessInterruptionOptionScheduleStage
),
ArchDCCABusinessInterruptionOptionScheduleStage AS (
	INSERT INTO ArchDCCABusinessInterruptionOptionScheduleStage
	(ExtractDate, SourceSystemId, AuditId, DCCABusinessInterruptionOptionScheduleStageId, CA_BusinessInterruptionOptionId, CA_BusinessInterruptionOptionScheduleId, SessionId, Id, DescriptionOfScheduledProperty)
	SELECT 
	EXTRACTDATE, 
	SourceSystemid AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCCABUSINESSINTERRUPTIONOPTIONSCHEDULESTAGEID, 
	CA_BUSINESSINTERRUPTIONOPTIONID, 
	CA_BUSINESSINTERRUPTIONOPTIONSCHEDULEID, 
	SESSIONID, 
	ID, 
	DESCRIPTIONOFSCHEDULEDPROPERTY
	FROM EXP_MetaData
),
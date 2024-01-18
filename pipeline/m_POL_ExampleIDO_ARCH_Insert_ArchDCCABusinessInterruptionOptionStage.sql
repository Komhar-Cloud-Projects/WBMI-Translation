WITH
SQ_DCCABusinessInterruptionOptionStage AS (
	SELECT
		DCCABusinessInterruptionOptionStageId,
		ExtractDate,
		SourceSystemid,
		CA_BusinessInterruptionEndorsementId,
		CA_BusinessInterruptionOptionId,
		SessionId,
		Id,
		Deleted,
		OptionType,
		OptionDescription,
		TotalExposureOptionB
	FROM DCCABusinessInterruptionOptionStage
),
EXP_MetaData AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	DCCABusinessInterruptionOptionStageId,
	ExtractDate,
	SourceSystemid,
	CA_BusinessInterruptionEndorsementId,
	CA_BusinessInterruptionOptionId,
	SessionId,
	Id,
	Deleted,
	OptionType,
	OptionDescription,
	TotalExposureOptionB
	FROM SQ_DCCABusinessInterruptionOptionStage
),
ArchDCCABusinessInterruptionOptionStage AS (
	INSERT INTO ArchDCCABusinessInterruptionOptionStage
	(ExtractDate, SourceSystemId, AuditId, DCCABusinessInterruptionOptionStageId, CA_BusinessInterruptionEndorsementId, CA_BusinessInterruptionOptionId, SessionId, Id, Deleted, OptionType, OptionDescription, TotalExposureOptionB)
	SELECT 
	EXTRACTDATE, 
	SourceSystemid AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCCABUSINESSINTERRUPTIONOPTIONSTAGEID, 
	CA_BUSINESSINTERRUPTIONENDORSEMENTID, 
	CA_BUSINESSINTERRUPTIONOPTIONID, 
	SESSIONID, 
	ID, 
	DELETED, 
	OPTIONTYPE, 
	OPTIONDESCRIPTION, 
	TOTALEXPOSUREOPTIONB
	FROM EXP_MetaData
),
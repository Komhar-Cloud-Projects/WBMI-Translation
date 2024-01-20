WITH
SQ_DCCABusinessInterruptionEndorsementStage AS (
	SELECT
		DCCABusinessInterruptionEndorsementStageId,
		ExtractDate,
		SourceSystemid,
		LineId,
		CA_BusinessInterruptionEndorsementId,
		SessionId,
		Id,
		Deleted,
		CollisionCoverage,
		DescriptionOfBusinessActivities,
		DurationOfWaitingPeriod,
		ExtendedAdditionalCoverage,
		ExtraExpenseCoverage,
		FormSelection,
		OTCCausesOfLoss
	FROM DCCABusinessInterruptionEndorsementStage
),
EXP_MetaData AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	DCCABusinessInterruptionEndorsementStageId,
	ExtractDate,
	SourceSystemid,
	LineId,
	CA_BusinessInterruptionEndorsementId,
	SessionId,
	Id,
	Deleted,
	CollisionCoverage,
	DescriptionOfBusinessActivities,
	DurationOfWaitingPeriod,
	ExtendedAdditionalCoverage,
	ExtraExpenseCoverage,
	FormSelection,
	OTCCausesOfLoss
	FROM SQ_DCCABusinessInterruptionEndorsementStage
),
ArchDCCABusinessInterruptionEndorsementStage AS (
	INSERT INTO ArchDCCABusinessInterruptionEndorsementStage
	(ExtractDate, SourceSystemId, AuditId, DCCABusinessInterruptionEndorsementStageId, LineId, CA_BusinessInterruptionEndorsementId, SessionId, Id, Deleted, CollisionCoverage, DescriptionOfBusinessActivities, DurationOfWaitingPeriod, ExtendedAdditionalCoverage, ExtraExpenseCoverage, FormSelection, OTCCausesOfLoss)
	SELECT 
	EXTRACTDATE, 
	SourceSystemid AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCCABUSINESSINTERRUPTIONENDORSEMENTSTAGEID, 
	LINEID, 
	CA_BUSINESSINTERRUPTIONENDORSEMENTID, 
	SESSIONID, 
	ID, 
	DELETED, 
	COLLISIONCOVERAGE, 
	DESCRIPTIONOFBUSINESSACTIVITIES, 
	DURATIONOFWAITINGPERIOD, 
	EXTENDEDADDITIONALCOVERAGE, 
	EXTRAEXPENSECOVERAGE, 
	FORMSELECTION, 
	OTCCAUSESOFLOSS
	FROM EXP_MetaData
),
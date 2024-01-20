WITH
SQ_DCIndividualsIncludedExcludedStage AS (
	SELECT
		DCIndividualsIncludedExcludedStageId,
		LineId,
		IndividualsIncludedExcludedId,
		SessionId,
		Id,
		IncludedExcluded,
		OwnershipPercentage,
		Duties,
		RemunerationPayroll,
		TitleRelationship,
		ExtractDate,
		SourceSystemId
	FROM DCIndividualsIncludedExcludedStage1
),
EXP_Metadata AS (
	SELECT
	DCIndividualsIncludedExcludedStageId,
	LineId,
	IndividualsIncludedExcludedId,
	SessionId,
	Id,
	IncludedExcluded,
	OwnershipPercentage,
	Duties,
	RemunerationPayroll,
	TitleRelationship,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCIndividualsIncludedExcludedStage
),
ArchDCIndividualsIncludedExcludedStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCIndividualsIncludedExcludedStage
	(DCIndividualsIncludedExcludedStageId, LineId, IndividualsIncludedExcludedId, SessionId, Id, IncludedExcluded, OwnershipPercentage, Duties, RemunerationPayroll, TitleRelationship, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	DCINDIVIDUALSINCLUDEDEXCLUDEDSTAGEID, 
	LINEID, 
	INDIVIDUALSINCLUDEDEXCLUDEDID, 
	SESSIONID, 
	ID, 
	INCLUDEDEXCLUDED, 
	OWNERSHIPPERCENTAGE, 
	DUTIES, 
	REMUNERATIONPAYROLL, 
	TITLERELATIONSHIP, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),
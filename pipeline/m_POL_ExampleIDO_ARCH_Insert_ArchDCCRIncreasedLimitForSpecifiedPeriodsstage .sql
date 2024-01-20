WITH
SQ_DCCRIncreasedLimitForSpecifiedPeriodsStage AS (
	SELECT
		DCCRIncreasedLimitForSpecifiedPeriodsStageId,
		CREndorsementId,
		CRBuildingId,
		CRIncreasedLimitForSpecifiedPeriodsId,
		SessionId,
		Id,
		Deleted,
		CRBuildingXmlId,
		IncreaseLimitForSpecifiedPeriodsEffectiveDate,
		IncreaseLimitForSpecifiedPeriodsExpirationDate,
		IncreaseLimitForSpecifiedPeriodsNumberOfPremises,
		ExtractDate,
		SourceSystemId
	FROM DCCRIncreasedLimitForSpecifiedPeriodsStage
),
EXP_Metadata AS (
	SELECT
	DCCRIncreasedLimitForSpecifiedPeriodsStageId,
	CREndorsementId,
	CRBuildingId,
	CRIncreasedLimitForSpecifiedPeriodsId,
	SessionId,
	Id,
	Deleted,
	CRBuildingXmlId,
	IncreaseLimitForSpecifiedPeriodsEffectiveDate,
	IncreaseLimitForSpecifiedPeriodsExpirationDate,
	IncreaseLimitForSpecifiedPeriodsNumberOfPremises,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCRIncreasedLimitForSpecifiedPeriodsStage
),
ArchDCCRIncreasedLimitForSpecifiedPeriodsStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCRIncreasedLimitForSpecifiedPeriodsStage
	(DCCRIncreasedLimitForSpecifiedPeriodsStageId, CREndorsementId, CRBuildingId, CRIncreasedLimitForSpecifiedPeriodsId, SessionId, Id, Deleted, CRBuildingXmlId, IncreaseLimitForSpecifiedPeriodsEffectiveDate, IncreaseLimitForSpecifiedPeriodsExpirationDate, IncreaseLimitForSpecifiedPeriodsNumberOfPremises, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	DCCRINCREASEDLIMITFORSPECIFIEDPERIODSSTAGEID, 
	CRENDORSEMENTID, 
	CRBUILDINGID, 
	CRINCREASEDLIMITFORSPECIFIEDPERIODSID, 
	SESSIONID, 
	ID, 
	DELETED, 
	CRBUILDINGXMLID, 
	INCREASELIMITFORSPECIFIEDPERIODSEFFECTIVEDATE, 
	INCREASELIMITFORSPECIFIEDPERIODSEXPIRATIONDATE, 
	INCREASELIMITFORSPECIFIEDPERIODSNUMBEROFPREMISES, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),
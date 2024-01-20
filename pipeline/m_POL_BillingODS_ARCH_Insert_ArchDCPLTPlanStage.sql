WITH
SQ_DCPLTPlanStage AS (
	SELECT
		DCPLTPlanStageId,
		ExtractDate,
		SourceSystemId,
		PlanId,
		AgencyId,
		LineOfBusinessCode,
		MasterCompanyCode,
		PlanActivationDate,
		PlanExpirationDate,
		PolicyInceptionDate,
		ProductCode,
		StateCode,
		UserKey1,
		UserKey2,
		UserKey3,
		UserKey4,
		UserKey5,
		PlanClassCode,
		PlanTypeCode,
		PlanData,
		LastUpdatedTimestamp,
		LastUpdatedUserId
	FROM DCPLTPlanStage
),
EXP_Metadata AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	DCPLTPlanStageId,
	ExtractDate,
	SourceSystemId,
	PlanId,
	AgencyId,
	LineOfBusinessCode,
	MasterCompanyCode,
	PlanActivationDate,
	PlanExpirationDate,
	PolicyInceptionDate,
	ProductCode,
	StateCode,
	UserKey1,
	UserKey2,
	UserKey3,
	UserKey4,
	UserKey5,
	PlanClassCode,
	PlanTypeCode,
	PlanData,
	LastUpdatedTimestamp,
	LastUpdatedUserId
	FROM SQ_DCPLTPlanStage
),
LKP_ArchExist AS (
	SELECT
	ArchDCPLTPlanStageId,
	PlanId
	FROM (
		SELECT 
			ArchDCPLTPlanStageId,
			PlanId
		FROM ArchDCPLTPlanStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PlanId ORDER BY ArchDCPLTPlanStageId) = 1
),
FIL_Exist AS (
	SELECT
	LKP_ArchExist.ArchDCPLTPlanStageId AS lkp_ArchDCPLTPlanStageId, 
	EXP_Metadata.o_AuditId AS AuditId, 
	EXP_Metadata.DCPLTPlanStageId, 
	EXP_Metadata.ExtractDate, 
	EXP_Metadata.SourceSystemId, 
	EXP_Metadata.PlanId, 
	EXP_Metadata.AgencyId, 
	EXP_Metadata.LineOfBusinessCode, 
	EXP_Metadata.MasterCompanyCode, 
	EXP_Metadata.PlanActivationDate, 
	EXP_Metadata.PlanExpirationDate, 
	EXP_Metadata.PolicyInceptionDate, 
	EXP_Metadata.ProductCode, 
	EXP_Metadata.StateCode, 
	EXP_Metadata.UserKey1, 
	EXP_Metadata.UserKey2, 
	EXP_Metadata.UserKey3, 
	EXP_Metadata.UserKey4, 
	EXP_Metadata.UserKey5, 
	EXP_Metadata.PlanClassCode, 
	EXP_Metadata.PlanTypeCode, 
	EXP_Metadata.PlanData, 
	EXP_Metadata.LastUpdatedTimestamp, 
	EXP_Metadata.LastUpdatedUserId
	FROM EXP_Metadata
	LEFT JOIN LKP_ArchExist
	ON LKP_ArchExist.PlanId = EXP_Metadata.PlanId
	WHERE ISNULL(lkp_ArchDCPLTPlanStageId)
),
ArchDCPLTPlanStage AS (
	INSERT INTO ArchDCPLTPlanStage
	(ExtractDate, SourceSystemId, AuditId, DCPLTPlanStageId, PlanId, AgencyId, LineOfBusinessCode, MasterCompanyCode, PlanActivationDate, PlanExpirationDate, PolicyInceptionDate, ProductCode, StateCode, UserKey1, UserKey2, UserKey3, UserKey4, UserKey5, PlanClassCode, PlanTypeCode, PlanData, LastUpdatedTimestamp, LastUpdatedUserId)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID, 
	DCPLTPLANSTAGEID, 
	PLANID, 
	AGENCYID, 
	LINEOFBUSINESSCODE, 
	MASTERCOMPANYCODE, 
	PLANACTIVATIONDATE, 
	PLANEXPIRATIONDATE, 
	POLICYINCEPTIONDATE, 
	PRODUCTCODE, 
	STATECODE, 
	USERKEY1, 
	USERKEY2, 
	USERKEY3, 
	USERKEY4, 
	USERKEY5, 
	PLANCLASSCODE, 
	PLANTYPECODE, 
	PLANDATA, 
	LASTUPDATEDTIMESTAMP, 
	LASTUPDATEDUSERID
	FROM FIL_Exist
),
WITH
SQ_DCBPCoverageEmployeeDishonestyStage AS (
	SELECT
		DCBPCoverageEmployeeDishonestyStageId,
		ExtractDate,
		SourceSystemid,
		CoverageId,
		BP_CoverageEmployeeDishonestyId,
		SessionId,
		Arate,
		Employees
	FROM DCBPCoverageEmployeeDishonestyStage
),
EXP_MetaData AS (
	SELECT
	DCBPCoverageEmployeeDishonestyStageId,
	ExtractDate,
	SourceSystemid,
	CoverageId,
	BP_CoverageEmployeeDishonestyId,
	SessionId,
	Arate,
	Employees,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCBPCoverageEmployeeDishonestyStage
),
ArchDCBPCoverageEmployeeDishonestyStage AS (
	INSERT INTO ArchDCBPCoverageEmployeeDishonestyStage
	(ExtractDate, SourceSystemId, AuditId, DCBPCoverageEmployeeDishonestyStageId, CoverageId, BP_CoverageEmployeeDishonestyId, SessionId, Arate, Employees)
	SELECT 
	EXTRACTDATE, 
	SourceSystemid AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCBPCOVERAGEEMPLOYEEDISHONESTYSTAGEID, 
	COVERAGEID, 
	BP_COVERAGEEMPLOYEEDISHONESTYID, 
	SESSIONID, 
	ARATE, 
	EMPLOYEES
	FROM EXP_MetaData
),
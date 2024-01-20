WITH
SQ_WorkDCTCoverageTransaction AS (
	SELECT
		WorkDCTCoverageTransactionId,
		ExtractDate,
		SourceSystemId,
		SessionId,
		ParentCoverageObjectId,
		ParentCoverageObjectName,
		CoverageId,
		CoverageGUID,
		CoverageType,
		Change,
		Premium,
		ParentCoverageType,
		CoverageDeleteFlag,
		Written,
		Prior,
		BaseRate,
		IncreasedLimitFactor,
		SubCoverageType
	FROM WorkDCTCoverageTransaction
),
EXp_Default AS (
	SELECT
	WorkDCTCoverageTransactionId,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	SessionId,
	ParentCoverageObjectId,
	ParentCoverageObjectName,
	CoverageId,
	CoverageGUID,
	CoverageType,
	Change,
	Premium,
	ParentCoverageType,
	CoverageDeleteFlag,
	Written,
	Prior,
	BaseRate,
	IncreasedLimitFactor,
	SubCoverageType
	FROM SQ_WorkDCTCoverageTransaction
),
ArchWorkDCTCoverageTransaction AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWorkDCTCoverageTransaction
	(ExtractDate, SourceSystemId, AuditId, WorkDCTCoverageTransactionId, SessionId, ParentCoverageObjectId, ParentCoverageObjectName, CoverageId, CoverageGUID, CoverageType, Change, Premium, ParentCoverageType, CoverageDeleteFlag, Written, Prior, BaseRate, IncreasedLimitFactor, SubCoverageType)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	audit_id AS AUDITID, 
	WORKDCTCOVERAGETRANSACTIONID, 
	SESSIONID, 
	PARENTCOVERAGEOBJECTID, 
	PARENTCOVERAGEOBJECTNAME, 
	COVERAGEID, 
	COVERAGEGUID, 
	COVERAGETYPE, 
	CHANGE, 
	PREMIUM, 
	PARENTCOVERAGETYPE, 
	COVERAGEDELETEFLAG, 
	WRITTEN, 
	PRIOR, 
	BASERATE, 
	INCREASEDLIMITFACTOR, 
	SUBCOVERAGETYPE
	FROM EXp_Default
),
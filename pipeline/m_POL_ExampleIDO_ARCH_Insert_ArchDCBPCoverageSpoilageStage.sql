WITH
SQ_DCBPCoverageSpoilageStage AS (
	SELECT
		DCBPCoverageSpoilageStageId,
		ExtractDate,
		SourceSystemId,
		CoverageId,
		BP_CoverageSpoilageId,
		SessionId,
		Agreement,
		ARate,
		ClassGroup,
		Type
	FROM DCBPCoverageSpoilageStage
),
EXP_DCBPCoverageSpoilageStage AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	DCBPCoverageSpoilageStageId,
	CoverageId,
	BP_CoverageSpoilageId,
	SessionId,
	Agreement AS i_Agreement,
	-- *INF*: IIF(i_Agreement='T',1,0)
	IFF(i_Agreement = 'T', 1, 0) AS o_Agreement,
	ARate,
	ClassGroup,
	Type
	FROM SQ_DCBPCoverageSpoilageStage
),
ArchDCBPCoverageSpoilageStage AS (
	INSERT INTO ArchDCBPCoverageSpoilageStage
	(ExtractDate, SourceSystemId, AuditId, DCBPCoverageSpoilageStageId, CoverageId, BP_CoverageSpoilageId, SessionId, Agreement, ARate, ClassGroup, Type)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCBPCOVERAGESPOILAGESTAGEID, 
	COVERAGEID, 
	BP_COVERAGESPOILAGEID, 
	SESSIONID, 
	o_Agreement AS AGREEMENT, 
	ARATE, 
	CLASSGROUP, 
	TYPE
	FROM EXP_DCBPCoverageSpoilageStage
),
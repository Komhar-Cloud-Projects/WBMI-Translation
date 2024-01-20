WITH
SQ_ClaimRelationshipStage AS (
	SELECT
		ClaimRelationshipStageId,
		ExtractDate,
		SourceSystemId,
		TchClaimNbr,
		RelationshipId,
		CreatedDate,
		CreatedUserId,
		ModifiedDate,
		ModifiedUserId
	FROM ClaimRelationshipStage
),
EXP_ClaimRelationshipStage AS (
	SELECT
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	ClaimRelationshipStageId,
	TchClaimNbr,
	RelationshipId,
	CreatedDate,
	CreatedUserId,
	ModifiedDate,
	ModifiedUserId
	FROM SQ_ClaimRelationshipStage
),
ArchClaimRelationshipStage AS (
	INSERT INTO ArchClaimRelationshipStage
	(ExtractDate, SourceSystemId, AuditId, ClaimRelationshipStageId, TchClaimNbr, RelationshipId, CreatedDate, CreatedUserId, ModifiedDate, ModifiedUserId)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	CLAIMRELATIONSHIPSTAGEID, 
	TCHCLAIMNBR, 
	RELATIONSHIPID, 
	CREATEDDATE, 
	CREATEDUSERID, 
	MODIFIEDDATE, 
	MODIFIEDUSERID
	FROM EXP_ClaimRelationshipStage
),
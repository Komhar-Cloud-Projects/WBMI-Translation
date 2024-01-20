WITH
SQ_AgencyRelationshipStage AS (
	SELECT
		AgencyRelationshipStageID,
		AgencyODSSourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AgencyID,
		AgencyCode,
		RelatedAgencyID,
		RelatedToAgencyCode,
		RelationshipType,
		EffectiveDate,
		ExpirationDate,
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemID
	FROM AgencyRelationshipStage
),
LKP_ExistingArchive AS (
	SELECT
	HashKey,
	in_AgencyID,
	in_RelatedAgencyID,
	in_RelationshipType,
	ModifiedDate,
	AgencyID,
	RelatedAgencyID,
	RelationshipType
	FROM (
		select	a.HashKey as HashKey,
				a.ModifiedDate as ModifiedDate,
				a.AgencyID as AgencyID, 
				a.RelatedAgencyID as RelatedAgencyID,
				a.RelationshipType as RelationshipType
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchAgencyRelationshipStage a
		inner join (
					select AgencyID, RelatedAgencyID, RelationshipType, max(ModifiedDate) as ModifiedDate
					from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchAgencyRelationshipStage 
					group by AgencyID, RelatedAgencyID, RelationshipType) b
		on  a.AgencyID = b.AgencyID
		and a.RelatedAgencyID = b.RelatedAgencyID
		and a.RelationshipType = b.RelationshipType
		and a.ModifiedDate = b.ModifiedDate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyID,RelatedAgencyID,RelationshipType ORDER BY HashKey) = 1
),
EXP_AddAuditID AS (
	SELECT
	SQ_AgencyRelationshipStage.AgencyRelationshipStageID,
	SQ_AgencyRelationshipStage.AgencyODSSourceSystemID,
	SQ_AgencyRelationshipStage.HashKey,
	SQ_AgencyRelationshipStage.ModifiedUserID,
	SQ_AgencyRelationshipStage.ModifiedDate,
	SQ_AgencyRelationshipStage.AgencyID,
	SQ_AgencyRelationshipStage.AgencyCode,
	SQ_AgencyRelationshipStage.RelatedAgencyID,
	SQ_AgencyRelationshipStage.RelatedToAgencyCode,
	SQ_AgencyRelationshipStage.RelationshipType,
	SQ_AgencyRelationshipStage.EffectiveDate,
	SQ_AgencyRelationshipStage.ExpirationDate,
	SQ_AgencyRelationshipStage.ExtractDate,
	SQ_AgencyRelationshipStage.AsOfDate,
	SQ_AgencyRelationshipStage.RecordCount,
	SQ_AgencyRelationshipStage.SourceSystemID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS OUT_AUDIT_ID,
	LKP_ExistingArchive.HashKey AS lkp_HashKey,
	-- *INF*: Decode(true,
	-- HashKey = lkp_HashKey, 'IGNORE',
	-- IsNull(lkp_HashKey), 'INSERT',
	-- 'UPDATE')
	Decode(
	    true,
	    HashKey = lkp_HashKey, 'IGNORE',
	    lkp_HashKey IS NULL, 'INSERT',
	    'UPDATE'
	) AS v_ChangeFlag,
	v_ChangeFlag AS o_ChangeFlag
	FROM SQ_AgencyRelationshipStage
	LEFT JOIN LKP_ExistingArchive
	ON LKP_ExistingArchive.AgencyID = SQ_AgencyRelationshipStage.AgencyID AND LKP_ExistingArchive.RelatedAgencyID = SQ_AgencyRelationshipStage.RelatedAgencyID AND LKP_ExistingArchive.RelationshipType = SQ_AgencyRelationshipStage.RelationshipType
),
FIL_ChangesOnly AS (
	SELECT
	AgencyRelationshipStageID, 
	AgencyODSSourceSystemID, 
	HashKey, 
	ModifiedUserID, 
	ModifiedDate, 
	AgencyID, 
	AgencyCode, 
	RelatedAgencyID, 
	RelatedToAgencyCode, 
	RelationshipType, 
	EffectiveDate, 
	ExpirationDate, 
	ExtractDate, 
	AsOfDate, 
	RecordCount, 
	SourceSystemID, 
	OUT_AUDIT_ID, 
	o_ChangeFlag
	FROM EXP_AddAuditID
	WHERE o_ChangeFlag = 'INSERT' OR o_ChangeFlag = 'UPDATE'
),
ArchAgencyRelationshipStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchAgencyRelationshipStage
	(AgencyRelationshipStageID, AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, AgencyID, AgencyCode, RelatedAgencyID, RelatedToAgencyCode, RelationshipType, EffectiveDate, ExpirationDate, ExtractDate, AsOfDate, RecordCount, SourceSystemID, AuditID)
	SELECT 
	AGENCYRELATIONSHIPSTAGEID, 
	AGENCYODSSOURCESYSTEMID, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	AGENCYID, 
	AGENCYCODE, 
	RELATEDAGENCYID, 
	RELATEDTOAGENCYCODE, 
	RELATIONSHIPTYPE, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	EXTRACTDATE, 
	ASOFDATE, 
	RECORDCOUNT, 
	SOURCESYSTEMID, 
	OUT_AUDIT_ID AS AUDITID
	FROM FIL_ChangesOnly
),
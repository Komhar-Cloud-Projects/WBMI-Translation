WITH
SQ_UnderwritingRegionRelationshipStage AS (
	SELECT
		UnderwritingRegionRelationshipStageID,
		AgencyODSSourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AssociateID,
		WestBendAssociateID,
		UnderwritingRegionID,
		UnderwritingRegionCode,
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemID
	FROM UnderwritingRegionRelationshipStage
),
LKP_ExistingArchive AS (
	SELECT
	HashKey,
	AssociateID,
	UnderwritingRegionID
	FROM (
		select	a.HashKey as HashKey,
				a.ModifiedDate as ModifiedDate,
				a.AssociateID as AssociateID,
				a.UnderwritingRegionID as UnderwritingRegionID
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchUnderwritingRegionRelationshipStage a
		inner join (
					select AssociateID, UnderwritingRegionID, max(ModifiedDate) as ModifiedDate
					from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchUnderwritingRegionRelationshipStage
					group by AssociateID, UnderwritingRegionID) b
		on  a.AssociateID = b.AssociateID
		and a.UnderwritingRegionID = b.UnderwritingRegionID
		and a.ModifiedDate = b.ModifiedDate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AssociateID,UnderwritingRegionID ORDER BY HashKey) = 1
),
EXP_AddAuditID AS (
	SELECT
	SQ_UnderwritingRegionRelationshipStage.UnderwritingRegionRelationshipStageID,
	SQ_UnderwritingRegionRelationshipStage.AgencyODSSourceSystemID,
	SQ_UnderwritingRegionRelationshipStage.HashKey,
	SQ_UnderwritingRegionRelationshipStage.ModifiedUserID,
	SQ_UnderwritingRegionRelationshipStage.ModifiedDate,
	SQ_UnderwritingRegionRelationshipStage.AssociateID,
	SQ_UnderwritingRegionRelationshipStage.WestBendAssociateID,
	SQ_UnderwritingRegionRelationshipStage.UnderwritingRegionID,
	SQ_UnderwritingRegionRelationshipStage.UnderwritingRegionCode,
	SQ_UnderwritingRegionRelationshipStage.ExtractDate,
	SQ_UnderwritingRegionRelationshipStage.AsOfDate,
	SQ_UnderwritingRegionRelationshipStage.RecordCount,
	SQ_UnderwritingRegionRelationshipStage.SourceSystemID,
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
	FROM SQ_UnderwritingRegionRelationshipStage
	LEFT JOIN LKP_ExistingArchive
	ON LKP_ExistingArchive.AssociateID = SQ_UnderwritingRegionRelationshipStage.AssociateID AND LKP_ExistingArchive.UnderwritingRegionID = SQ_UnderwritingRegionRelationshipStage.UnderwritingRegionID
),
FIL_ChangesOnly AS (
	SELECT
	UnderwritingRegionRelationshipStageID, 
	AgencyODSSourceSystemID, 
	HashKey, 
	ModifiedUserID, 
	ModifiedDate, 
	AssociateID, 
	WestBendAssociateID, 
	UnderwritingRegionID, 
	UnderwritingRegionCode, 
	ExtractDate, 
	AsOfDate, 
	RecordCount, 
	SourceSystemID, 
	OUT_AUDIT_ID, 
	o_ChangeFlag
	FROM EXP_AddAuditID
	WHERE o_ChangeFlag = 'INSERT' OR o_ChangeFlag = 'UPDATE'
),
ArchUnderwritingRegionRelationshipStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchUnderwritingRegionRelationshipStage
	(UnderwritingRegionRelationshipStageID, AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, AssociateID, WestBendAssociateID, UnderwritingRegionID, UnderwritingRegionCode, ExtractDate, AsOfDate, RecordCount, SourceSystemID, AuditID)
	SELECT 
	UNDERWRITINGREGIONRELATIONSHIPSTAGEID, 
	AGENCYODSSOURCESYSTEMID, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	ASSOCIATEID, 
	WESTBENDASSOCIATEID, 
	UNDERWRITINGREGIONID, 
	UNDERWRITINGREGIONCODE, 
	EXTRACTDATE, 
	ASOFDATE, 
	RECORDCOUNT, 
	SOURCESYSTEMID, 
	OUT_AUDIT_ID AS AUDITID
	FROM FIL_ChangesOnly
),
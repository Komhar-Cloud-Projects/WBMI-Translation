WITH
SQ_RegionalSalesManagerRelationshipStage AS (
	SELECT
		RegionalSalesManagerRelationshipStageID,
		AgencyODSSourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AgencyID,
		AgencyCode,
		AssociateID,
		WestBendAssociateID,
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemID
	FROM RegionalSalesManagerRelationshipStage
),
LKP_ExistingArchive AS (
	SELECT
	HashKey,
	AgencyID,
	AssociateID
	FROM (
		select	a.HashKey as HashKey,
				a.ModifiedDate as ModifiedDate,
				a.AgencyID as AgencyID,
				a.AssociateID as AssociateID
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchRegionalSalesManagerRelationshipStage a
		inner join (
					select AgencyID, AssociateID, max(ModifiedDate) as ModifiedDate
					from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchRegionalSalesManagerRelationshipStage
					group by AgencyID, AssociateID) b
		on  a.AgencyID = b.AgencyID
		and a.AssociateID = b.AssociateID
		and a.ModifiedDate = b.ModifiedDate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyID,AssociateID ORDER BY HashKey) = 1
),
EXP_AddAuditID AS (
	SELECT
	SQ_RegionalSalesManagerRelationshipStage.RegionalSalesManagerRelationshipStageID,
	SQ_RegionalSalesManagerRelationshipStage.AgencyODSSourceSystemID,
	SQ_RegionalSalesManagerRelationshipStage.HashKey,
	SQ_RegionalSalesManagerRelationshipStage.ModifiedUserID,
	SQ_RegionalSalesManagerRelationshipStage.ModifiedDate,
	SQ_RegionalSalesManagerRelationshipStage.AgencyID,
	SQ_RegionalSalesManagerRelationshipStage.AgencyCode,
	SQ_RegionalSalesManagerRelationshipStage.AssociateID,
	SQ_RegionalSalesManagerRelationshipStage.WestBendAssociateID,
	SQ_RegionalSalesManagerRelationshipStage.ExtractDate,
	SQ_RegionalSalesManagerRelationshipStage.AsOfDate,
	SQ_RegionalSalesManagerRelationshipStage.RecordCount,
	SQ_RegionalSalesManagerRelationshipStage.SourceSystemID,
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
	FROM SQ_RegionalSalesManagerRelationshipStage
	LEFT JOIN LKP_ExistingArchive
	ON LKP_ExistingArchive.AgencyID = SQ_RegionalSalesManagerRelationshipStage.AgencyID AND LKP_ExistingArchive.AssociateID = SQ_RegionalSalesManagerRelationshipStage.AssociateID
),
FIL_ChangesOnly AS (
	SELECT
	RegionalSalesManagerRelationshipStageID, 
	AgencyODSSourceSystemID, 
	HashKey, 
	ModifiedUserID, 
	ModifiedDate, 
	AgencyID, 
	AgencyCode, 
	AssociateID, 
	WestBendAssociateID, 
	ExtractDate, 
	AsOfDate, 
	RecordCount, 
	SourceSystemID, 
	OUT_AUDIT_ID, 
	o_ChangeFlag
	FROM EXP_AddAuditID
	WHERE o_ChangeFlag = 'INSERT' OR o_ChangeFlag = 'UPDATE'
),
ArchRegionalSalesManagerRelationshipStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchRegionalSalesManagerRelationshipStage
	(RegionalSalesManagerRelationshipStageID, AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, AgencyID, AgencyCode, AssociateID, WestBendAssociateID, ExtractDate, AsOfDate, RecordCount, SourceSystemID, AuditID)
	SELECT 
	REGIONALSALESMANAGERRELATIONSHIPSTAGEID, 
	AGENCYODSSOURCESYSTEMID, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	AGENCYID, 
	AGENCYCODE, 
	ASSOCIATEID, 
	WESTBENDASSOCIATEID, 
	EXTRACTDATE, 
	ASOFDATE, 
	RECORDCOUNT, 
	SOURCESYSTEMID, 
	OUT_AUDIT_ID AS AUDITID
	FROM FIL_ChangesOnly
),
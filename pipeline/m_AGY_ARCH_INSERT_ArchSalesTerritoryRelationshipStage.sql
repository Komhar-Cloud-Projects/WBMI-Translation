WITH
SQ_SalesTerritoryRelationshipStage AS (
	SELECT
		SalesTerritoryRelationshipStageID,
		AgencyODSSourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AgencyID,
		AgencyCode,
		SalesTerritoryID,
		SalesTerritoryCode,
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemID
	FROM SalesTerritoryRelationshipStage
),
LKP_ExistingArchive AS (
	SELECT
	HashKey,
	SalesTerritoryID,
	AgencyID
	FROM (
		select	a.ModifiedDate as ModifiedDate,
				a.HashKey as HashKey,
				a.SalesTerritoryID as SalesTerritoryID,
				a.AgencyID as AgencyID
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchSalesTerritoryRelationshipStage a
		inner join (
					select SalesTerritoryID, AgencyID, max(ModifiedDate) as ModifiedDate
					from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchSalesTerritoryRelationshipStage
					group by SalesTerritoryID, AgencyID) b
		on  a.SalesTerritoryID = b.SalesTerritoryID
		and a.AgencyID = b.AgencyID
		and a.ModifiedDate = b.ModifiedDate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SalesTerritoryID,AgencyID ORDER BY HashKey) = 1
),
EXP_AddAuditID AS (
	SELECT
	SQ_SalesTerritoryRelationshipStage.SalesTerritoryRelationshipStageID,
	SQ_SalesTerritoryRelationshipStage.AgencyODSSourceSystemID,
	SQ_SalesTerritoryRelationshipStage.HashKey,
	SQ_SalesTerritoryRelationshipStage.ModifiedUserID,
	SQ_SalesTerritoryRelationshipStage.ModifiedDate,
	SQ_SalesTerritoryRelationshipStage.AgencyID,
	SQ_SalesTerritoryRelationshipStage.AgencyCode,
	SQ_SalesTerritoryRelationshipStage.SalesTerritoryID,
	SQ_SalesTerritoryRelationshipStage.SalesTerritoryCode,
	SQ_SalesTerritoryRelationshipStage.ExtractDate,
	SQ_SalesTerritoryRelationshipStage.AsOfDate,
	SQ_SalesTerritoryRelationshipStage.RecordCount,
	SQ_SalesTerritoryRelationshipStage.SourceSystemID,
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
	FROM SQ_SalesTerritoryRelationshipStage
	LEFT JOIN LKP_ExistingArchive
	ON LKP_ExistingArchive.SalesTerritoryID = SQ_SalesTerritoryRelationshipStage.SalesTerritoryID AND LKP_ExistingArchive.AgencyID = SQ_SalesTerritoryRelationshipStage.AgencyID
),
FIL_ChangesOnly AS (
	SELECT
	SalesTerritoryRelationshipStageID, 
	AgencyODSSourceSystemID, 
	HashKey, 
	ModifiedUserID, 
	ModifiedDate, 
	AgencyID, 
	AgencyCode, 
	SalesTerritoryID, 
	SalesTerritoryCode, 
	ExtractDate, 
	AsOfDate, 
	RecordCount, 
	SourceSystemID, 
	OUT_AUDIT_ID, 
	o_ChangeFlag
	FROM EXP_AddAuditID
	WHERE o_ChangeFlag = 'INSERT' OR o_ChangeFlag = 'UPDATE'
),
ArchSalesTerritoryRelationshipStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSalesTerritoryRelationshipStage
	(SalesTerritoryRelationshipStageID, AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, AgencyID, AgencyCode, SalesTerritoryID, SalesTerritoryCode, ExtractDate, AsOfDate, RecordCount, SourceSystemID, AuditID)
	SELECT 
	SALESTERRITORYRELATIONSHIPSTAGEID, 
	AGENCYODSSOURCESYSTEMID, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	AGENCYID, 
	AGENCYCODE, 
	SALESTERRITORYID, 
	SALESTERRITORYCODE, 
	EXTRACTDATE, 
	ASOFDATE, 
	RECORDCOUNT, 
	SOURCESYSTEMID, 
	OUT_AUDIT_ID AS AUDITID
	FROM FIL_ChangesOnly
),
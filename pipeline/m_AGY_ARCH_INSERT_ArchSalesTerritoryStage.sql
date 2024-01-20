WITH
SQ_SalesTerritoryStage AS (
	SELECT
		SalesTerritoryStageID,
		AgencyODSSourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		SalesTerritoryCode,
		SalesTerritoryCodeDescription,
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemID
	FROM SalesTerritoryStage
),
LKP_ExistingArchive AS (
	SELECT
	HashKey,
	SalesTerritoryCode
	FROM (
		select	a.HashKey as HashKey,
				a.ModifiedDate as ModifiedDate,
				a.SalesTerritoryCode as SalesTerritoryCode
		from dbo.ArchSalesTerritoryStage a
		inner join (
					select SalesTerritoryCode, max(ModifiedDate) as ModifiedDate
					from dbo.ArchSalesTerritoryStage 
					group by SalesTerritoryCode) b
		on  a.SalesTerritoryCode = b.SalesTerritoryCode
		and a.ModifiedDate = b.ModifiedDate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SalesTerritoryCode ORDER BY HashKey) = 1
),
EXP_AddAuditID AS (
	SELECT
	SQ_SalesTerritoryStage.SalesTerritoryStageID,
	SQ_SalesTerritoryStage.AgencyODSSourceSystemID,
	SQ_SalesTerritoryStage.HashKey,
	SQ_SalesTerritoryStage.ModifiedUserID,
	SQ_SalesTerritoryStage.ModifiedDate,
	SQ_SalesTerritoryStage.SalesTerritoryCode,
	SQ_SalesTerritoryStage.SalesTerritoryCodeDescription,
	SQ_SalesTerritoryStage.ExtractDate,
	SQ_SalesTerritoryStage.AsOfDate,
	SQ_SalesTerritoryStage.RecordCount,
	SQ_SalesTerritoryStage.SourceSystemID,
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
	FROM SQ_SalesTerritoryStage
	LEFT JOIN LKP_ExistingArchive
	ON LKP_ExistingArchive.SalesTerritoryCode = SQ_SalesTerritoryStage.SalesTerritoryCode
),
FIL_ChangesOnly AS (
	SELECT
	SalesTerritoryStageID, 
	AgencyODSSourceSystemID, 
	HashKey, 
	ModifiedUserID, 
	ModifiedDate, 
	SalesTerritoryCode, 
	SalesTerritoryCodeDescription, 
	ExtractDate, 
	AsOfDate, 
	RecordCount, 
	SourceSystemID, 
	OUT_AUDIT_ID, 
	o_ChangeFlag
	FROM EXP_AddAuditID
	WHERE o_ChangeFlag = 'INSERT' OR o_ChangeFlag = 'UPDATE'
),
ArchSalesTerritoryStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSalesTerritoryStage
	(SalesTerritoryStageID, AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, SalesTerritoryCode, SalesTerritoryCodeDescription, ExtractDate, AsOfDate, RecordCount, SourceSystemID, AuditID)
	SELECT 
	SALESTERRITORYSTAGEID, 
	AGENCYODSSOURCESYSTEMID, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	SALESTERRITORYCODE, 
	SALESTERRITORYCODEDESCRIPTION, 
	EXTRACTDATE, 
	ASOFDATE, 
	RECORDCOUNT, 
	SOURCESYSTEMID, 
	OUT_AUDIT_ID AS AUDITID
	FROM FIL_ChangesOnly
),
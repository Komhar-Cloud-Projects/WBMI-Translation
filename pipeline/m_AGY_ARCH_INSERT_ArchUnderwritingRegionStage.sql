WITH
SQ_UnderwritingRegionStage AS (
	SELECT
		UnderwritingRegionStageID,
		AgencyODSSourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		UnderwritingRegionCode,
		UnderwritingRegionCodeDescription,
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemID
	FROM UnderwritingRegionStage
),
LKP_ExistingArchive AS (
	SELECT
	HashKey,
	UnderwritingRegionCode
	FROM (
		select	a.HashKey as HashKey,
				a.ModifiedDate as ModifiedDate,
				a.UnderwritingRegionCode as UnderwritingRegionCode
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchUnderwritingRegionStage a
		inner join (
					select UnderwritingRegionCode, max(ModifiedDate) as ModifiedDate
					from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchUnderwritingRegionStage
					group by UnderwritingRegionCode) b
		on  a.UnderwritingRegionCode = b.UnderwritingRegionCode
		and a.ModifiedDate = b.ModifiedDate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY UnderwritingRegionCode ORDER BY HashKey) = 1
),
EXP_AddAuditID AS (
	SELECT
	SQ_UnderwritingRegionStage.UnderwritingRegionStageID,
	SQ_UnderwritingRegionStage.AgencyODSSourceSystemID,
	SQ_UnderwritingRegionStage.HashKey,
	SQ_UnderwritingRegionStage.ModifiedUserID,
	SQ_UnderwritingRegionStage.ModifiedDate,
	SQ_UnderwritingRegionStage.UnderwritingRegionCode,
	SQ_UnderwritingRegionStage.UnderwritingRegionCodeDescription,
	SQ_UnderwritingRegionStage.ExtractDate,
	SQ_UnderwritingRegionStage.AsOfDate,
	SQ_UnderwritingRegionStage.RecordCount,
	SQ_UnderwritingRegionStage.SourceSystemID,
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
	FROM SQ_UnderwritingRegionStage
	LEFT JOIN LKP_ExistingArchive
	ON LKP_ExistingArchive.UnderwritingRegionCode = SQ_UnderwritingRegionStage.UnderwritingRegionCode
),
FIL_ChangesOnly AS (
	SELECT
	UnderwritingRegionStageID, 
	AgencyODSSourceSystemID, 
	HashKey, 
	ModifiedUserID, 
	ModifiedDate, 
	UnderwritingRegionCode, 
	UnderwritingRegionCodeDescription, 
	ExtractDate, 
	AsOfDate, 
	RecordCount, 
	SourceSystemID, 
	OUT_AUDIT_ID, 
	o_ChangeFlag
	FROM EXP_AddAuditID
	WHERE o_ChangeFlag = 'INSERT' OR o_ChangeFlag = 'UPDATE'
),
ArchUnderwritingRegionStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchUnderwritingRegionStage
	(UnderwritingRegionStageID, AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, UnderwritingRegionCode, UnderwritingRegionCodeDescription, ExtractDate, AsOfDate, RecordCount, SourceSystemID, AuditID)
	SELECT 
	UNDERWRITINGREGIONSTAGEID, 
	AGENCYODSSOURCESYSTEMID, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	UNDERWRITINGREGIONCODE, 
	UNDERWRITINGREGIONCODEDESCRIPTION, 
	EXTRACTDATE, 
	ASOFDATE, 
	RECORDCOUNT, 
	SOURCESYSTEMID, 
	OUT_AUDIT_ID AS AUDITID
	FROM FIL_ChangesOnly
),
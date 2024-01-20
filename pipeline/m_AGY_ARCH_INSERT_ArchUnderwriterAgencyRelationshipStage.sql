WITH
SQ_UnderwriterAgencyRelationshipStage AS (
	SELECT
		UnderwriterAgencyRelationshipStageID,
		AgencyODSSourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AgencyID,
		AgencyCode,
		AssociateID,
		WestBendAssociateID,
		StrategicProfitCenterCode,
		StrategicProfitCenterDescription AS StrategicProfitCenterDescriptiong,
		AgencyODSRelationshipId,
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemID
	FROM UnderwriterAgencyRelationshipStage
),
LKP_ExistingArchive AS (
	SELECT
	HashKey,
	AgencyODSRelationshipId
	FROM (
		select	a.ModifiedDate as ModifiedDate,
				a.HashKey as HashKey,
				a.AgencyODSRelationshipId as AgencyODSRelationshipId
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchUnderwriterAgencyRelationshipStage a
		inner join (
					select AgencyODSRelationshipId, max(ModifiedDate) as ModifiedDate
					from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchUnderwriterAgencyRelationshipStage
					group by AgencyODSRelationshipId) b
		on  a.AgencyODSRelationshipId = b.AgencyODSRelationshipId
		and a.ModifiedDate = b.ModifiedDate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyODSRelationshipId ORDER BY HashKey) = 1
),
EXP_AddAuditID AS (
	SELECT
	SQ_UnderwriterAgencyRelationshipStage.UnderwriterAgencyRelationshipStageID,
	SQ_UnderwriterAgencyRelationshipStage.AgencyODSSourceSystemID,
	SQ_UnderwriterAgencyRelationshipStage.HashKey,
	SQ_UnderwriterAgencyRelationshipStage.ModifiedUserID,
	SQ_UnderwriterAgencyRelationshipStage.ModifiedDate,
	SQ_UnderwriterAgencyRelationshipStage.AgencyID,
	SQ_UnderwriterAgencyRelationshipStage.AgencyCode,
	SQ_UnderwriterAgencyRelationshipStage.AssociateID,
	SQ_UnderwriterAgencyRelationshipStage.WestBendAssociateID,
	SQ_UnderwriterAgencyRelationshipStage.StrategicProfitCenterCode,
	SQ_UnderwriterAgencyRelationshipStage.StrategicProfitCenterDescriptiong,
	SQ_UnderwriterAgencyRelationshipStage.AgencyODSRelationshipId,
	SQ_UnderwriterAgencyRelationshipStage.ExtractDate,
	SQ_UnderwriterAgencyRelationshipStage.AsOfDate,
	SQ_UnderwriterAgencyRelationshipStage.RecordCount,
	SQ_UnderwriterAgencyRelationshipStage.SourceSystemID,
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
	FROM SQ_UnderwriterAgencyRelationshipStage
	LEFT JOIN LKP_ExistingArchive
	ON LKP_ExistingArchive.AgencyODSRelationshipId = SQ_UnderwriterAgencyRelationshipStage.AgencyODSRelationshipId
),
FIL_ChangesOnly AS (
	SELECT
	UnderwriterAgencyRelationshipStageID, 
	AgencyODSSourceSystemID, 
	HashKey, 
	ModifiedUserID, 
	ModifiedDate, 
	AgencyID, 
	AgencyCode, 
	AssociateID, 
	WestBendAssociateID, 
	StrategicProfitCenterCode, 
	StrategicProfitCenterDescriptiong, 
	AgencyODSRelationshipId, 
	ExtractDate, 
	AsOfDate, 
	RecordCount, 
	SourceSystemID, 
	OUT_AUDIT_ID, 
	o_ChangeFlag
	FROM EXP_AddAuditID
	WHERE o_ChangeFlag = 'INSERT' OR o_ChangeFlag = 'UPDATE'
),
ArchUnderwriterAgencyRelationshipStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchUnderwriterAgencyRelationshipStage
	(UnderwriterAgencyRelationshipStageID, AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, AgencyID, AgencyCode, AssociateID, WestBendAssociateID, StrategicProfitCenterCode, StrategicProfitCenterDescription, AgencyODSRelationshipId, ExtractDate, AsOfDate, RecordCount, SourceSystemID, AuditID)
	SELECT 
	UNDERWRITERAGENCYRELATIONSHIPSTAGEID, 
	AGENCYODSSOURCESYSTEMID, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	AGENCYID, 
	AGENCYCODE, 
	ASSOCIATEID, 
	WESTBENDASSOCIATEID, 
	STRATEGICPROFITCENTERCODE, 
	StrategicProfitCenterDescriptiong AS STRATEGICPROFITCENTERDESCRIPTION, 
	AGENCYODSRELATIONSHIPID, 
	EXTRACTDATE, 
	ASOFDATE, 
	RECORDCOUNT, 
	SOURCESYSTEMID, 
	OUT_AUDIT_ID AS AUDITID
	FROM FIL_ChangesOnly
),
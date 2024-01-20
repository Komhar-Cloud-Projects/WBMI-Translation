WITH
SQ_UnderwritingReportingRelationshipStage AS (
	SELECT
		UnderwritingReportingRelationshipStageID,
		AgencyODSSourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AssociateID,
		WestBendAssociateID,
		ReportToAssociateID,
		ReportToWestBendAssociateID,
		RelationshipType,
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemID
	FROM UnderwritingReportingRelationshipStage
),
LKP_ExistingArchive AS (
	SELECT
	HashKey,
	AssociateID,
	ReportToAssociateID
	FROM (
		select	a.HashKey as HashKey,
				a.ModifiedDate as ModifiedDate,
				a.AssociateID as AssociateID,
				a.ReportToAssociateID as ReportToAssociateID
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchUnderwritingReportingRelationshipStage a
		inner join (
					select AssociateID, ReportToAssociateID, max(ModifiedDate) as ModifiedDate
					from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchUnderwritingReportingRelationshipStage
					group by AssociateID, ReportToAssociateID) b
		on  a.AssociateID = b.AssociateID
		and a.ReportToAssociateID = b.ReportToAssociateID
		and a.ModifiedDate = b.ModifiedDate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AssociateID,ReportToAssociateID ORDER BY HashKey) = 1
),
EXP_AddAuditID AS (
	SELECT
	SQ_UnderwritingReportingRelationshipStage.UnderwritingReportingRelationshipStageID,
	SQ_UnderwritingReportingRelationshipStage.AgencyODSSourceSystemID,
	SQ_UnderwritingReportingRelationshipStage.HashKey,
	SQ_UnderwritingReportingRelationshipStage.ModifiedUserID,
	SQ_UnderwritingReportingRelationshipStage.ModifiedDate,
	SQ_UnderwritingReportingRelationshipStage.AssociateID,
	SQ_UnderwritingReportingRelationshipStage.WestBendAssociateID,
	SQ_UnderwritingReportingRelationshipStage.ReportToAssociateID,
	SQ_UnderwritingReportingRelationshipStage.ReportToWestBendAssociateID,
	SQ_UnderwritingReportingRelationshipStage.RelationshipType,
	SQ_UnderwritingReportingRelationshipStage.ExtractDate,
	SQ_UnderwritingReportingRelationshipStage.AsOfDate,
	SQ_UnderwritingReportingRelationshipStage.RecordCount,
	SQ_UnderwritingReportingRelationshipStage.SourceSystemID,
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
	FROM SQ_UnderwritingReportingRelationshipStage
	LEFT JOIN LKP_ExistingArchive
	ON LKP_ExistingArchive.AssociateID = SQ_UnderwritingReportingRelationshipStage.AssociateID AND LKP_ExistingArchive.ReportToAssociateID = SQ_UnderwritingReportingRelationshipStage.ReportToAssociateID
),
FIL_ChangesOnly AS (
	SELECT
	UnderwritingReportingRelationshipStageID, 
	AgencyODSSourceSystemID, 
	HashKey, 
	ModifiedUserID, 
	ModifiedDate, 
	AssociateID, 
	WestBendAssociateID, 
	ReportToAssociateID, 
	ReportToWestBendAssociateID, 
	RelationshipType, 
	ExtractDate, 
	AsOfDate, 
	RecordCount, 
	SourceSystemID, 
	OUT_AUDIT_ID, 
	o_ChangeFlag
	FROM EXP_AddAuditID
	WHERE o_ChangeFlag = 'INSERT' OR o_ChangeFlag = 'UPDATE'
),
ArchUnderwritingReportingRelationshipStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchUnderwritingReportingRelationshipStage
	(UnderwritingReportingRelationshipStageID, AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, AssociateID, WestBendAssociateID, ReportToAssociateID, ReportToWestBendAssociateID, RelationshipType, ExtractDate, AsOfDate, RecordCount, SourceSystemID, AuditID)
	SELECT 
	UNDERWRITINGREPORTINGRELATIONSHIPSTAGEID, 
	AGENCYODSSOURCESYSTEMID, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	ASSOCIATEID, 
	WESTBENDASSOCIATEID, 
	REPORTTOASSOCIATEID, 
	REPORTTOWESTBENDASSOCIATEID, 
	RELATIONSHIPTYPE, 
	EXTRACTDATE, 
	ASOFDATE, 
	RECORDCOUNT, 
	SOURCESYSTEMID, 
	OUT_AUDIT_ID AS AUDITID
	FROM FIL_ChangesOnly
),
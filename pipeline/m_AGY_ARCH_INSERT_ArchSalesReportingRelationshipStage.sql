WITH
SQ_SalesReportingRelationshipStage AS (
	SELECT
		SalesReportingRelationshipStageID,
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
	FROM SalesReportingRelationshipStage
),
LKP_ExistingArchive AS (
	SELECT
	HashKey,
	AssociateID,
	ReportToAssociateID
	FROM (
		select	a.ModifiedDate as ModifiedDate,
				a.HashKey as HashKey,
				a.AssociateID as AssociateID,
				a.ReportToAssociateID as ReportToAssociateID
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchSalesReportingRelationshipStage a
		inner join (
					select AssociateID, ReportToAssociateID, max(ModifiedDate) as ModifiedDate
					from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchSalesReportingRelationshipStage
					group by AssociateID, ReportToAssociateID) b
		on  a.AssociateID = b.AssociateID
		and a.ReportToAssociateID = b.ReportToAssociateID
		and a.ModifiedDate = b.ModifiedDate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AssociateID,ReportToAssociateID ORDER BY HashKey) = 1
),
EXP_CheckForChange AS (
	SELECT
	SQ_SalesReportingRelationshipStage.SalesReportingRelationshipStageID,
	SQ_SalesReportingRelationshipStage.AgencyODSSourceSystemID,
	SQ_SalesReportingRelationshipStage.HashKey,
	SQ_SalesReportingRelationshipStage.ModifiedUserID,
	SQ_SalesReportingRelationshipStage.ModifiedDate,
	SQ_SalesReportingRelationshipStage.AssociateID,
	SQ_SalesReportingRelationshipStage.WestBendAssociateID,
	SQ_SalesReportingRelationshipStage.ReportToAssociateID,
	SQ_SalesReportingRelationshipStage.ReportToWestBendAssociateID,
	SQ_SalesReportingRelationshipStage.RelationshipType,
	SQ_SalesReportingRelationshipStage.ExtractDate,
	SQ_SalesReportingRelationshipStage.AsOfDate,
	SQ_SalesReportingRelationshipStage.RecordCount,
	SQ_SalesReportingRelationshipStage.SourceSystemID,
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
	FROM SQ_SalesReportingRelationshipStage
	LEFT JOIN LKP_ExistingArchive
	ON LKP_ExistingArchive.AssociateID = SQ_SalesReportingRelationshipStage.AssociateID AND LKP_ExistingArchive.ReportToAssociateID = SQ_SalesReportingRelationshipStage.ReportToAssociateID
),
FIL_ChangesOnly AS (
	SELECT
	SalesReportingRelationshipStageID, 
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
	FROM EXP_CheckForChange
	WHERE o_ChangeFlag = 'INSERT' OR o_ChangeFlag = 'UPDATE'
),
ArchSalesReportingRelationshipStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSalesReportingRelationshipStage
	(SalesReportingRelationshipStageID, AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, AssociateID, WestBendAssociateID, ReportToAssociateID, ReportToWestBendAssociateID, RelationshipType, ExtractDate, AsOfDate, RecordCount, SourceSystemID, AuditID)
	SELECT 
	SALESREPORTINGRELATIONSHIPSTAGEID, 
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
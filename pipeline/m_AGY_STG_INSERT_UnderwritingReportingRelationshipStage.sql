WITH
SQ_UnderwritingReportingRelationship_ODS AS (
	SELECT
		UnderwritingReportingRelationshipID,
		SourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AssociateID,
		ReportAssociateID,
		RelationshipType
	FROM UnderwritingReportingRelationship_ODS
),
LKP_ReportToWestBendAssociateID AS (
	SELECT
	WestBendAssociateID,
	in_ReportAssociateID,
	AssociateID
	FROM (
		SELECT 
			WestBendAssociateID,
			in_ReportAssociateID,
			AssociateID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Associate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AssociateID ORDER BY WestBendAssociateID) = 1
),
LKP_WestBendAssociateID AS (
	SELECT
	WestBendAssociateID,
	AssociateID
	FROM (
		SELECT 
			WestBendAssociateID,
			AssociateID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Associate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AssociateID ORDER BY WestBendAssociateID) = 1
),
EXP_Add_MetaDataFields AS (
	SELECT
	SQ_UnderwritingReportingRelationship_ODS.UnderwritingReportingRelationshipID,
	SQ_UnderwritingReportingRelationship_ODS.SourceSystemID,
	SQ_UnderwritingReportingRelationship_ODS.HashKey,
	SQ_UnderwritingReportingRelationship_ODS.ModifiedUserID,
	SQ_UnderwritingReportingRelationship_ODS.ModifiedDate,
	SQ_UnderwritingReportingRelationship_ODS.AssociateID,
	SQ_UnderwritingReportingRelationship_ODS.ReportAssociateID,
	SQ_UnderwritingReportingRelationship_ODS.RelationshipType,
	LKP_WestBendAssociateID.WestBendAssociateID AS lkp_WestBendAssociateID,
	LKP_ReportToWestBendAssociateID.WestBendAssociateID AS lkp_ReportToWestBendAssociateID,
	sysdate AS Extract_Date,
	sysdate AS As_of_Date,
	1 AS Record_Count,
	@{pipeline().parameters.SOURCESYSTEMID} AS Source_System_ID
	FROM SQ_UnderwritingReportingRelationship_ODS
	LEFT JOIN LKP_ReportToWestBendAssociateID
	ON LKP_ReportToWestBendAssociateID.AssociateID = SQ_UnderwritingReportingRelationship_ODS.ReportAssociateID
	LEFT JOIN LKP_WestBendAssociateID
	ON LKP_WestBendAssociateID.AssociateID = SQ_UnderwritingReportingRelationship_ODS.AssociateID
),
UnderwritingReportingRelationshipStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingReportingRelationshipStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingReportingRelationshipStage
	(AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, AssociateID, WestBendAssociateID, ReportToAssociateID, ReportToWestBendAssociateID, RelationshipType, ExtractDate, AsOfDate, RecordCount, SourceSystemID)
	SELECT 
	SourceSystemID AS AGENCYODSSOURCESYSTEMID, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	ASSOCIATEID, 
	lkp_WestBendAssociateID AS WESTBENDASSOCIATEID, 
	ReportAssociateID AS REPORTTOASSOCIATEID, 
	lkp_ReportToWestBendAssociateID AS REPORTTOWESTBENDASSOCIATEID, 
	RELATIONSHIPTYPE, 
	Extract_Date AS EXTRACTDATE, 
	As_of_Date AS ASOFDATE, 
	Record_Count AS RECORDCOUNT, 
	Source_System_ID AS SOURCESYSTEMID
	FROM EXP_Add_MetaDataFields
),
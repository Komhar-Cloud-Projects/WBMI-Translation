WITH
SQ_SalesReportingRelationship_ODS AS (
	SELECT
		SalesReportingRelationshipID,
		SourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AssociateID,
		ReportAssociateID,
		RelationshipType
	FROM SalesReportingRelationship_ODS
),
LKP_ReportToWestBendAssociateID AS (
	SELECT
	WestBendAssociateID,
	in_ReportToAssociateID,
	AssociateID
	FROM (
		SELECT 
			WestBendAssociateID,
			in_ReportToAssociateID,
			AssociateID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Associate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AssociateID ORDER BY WestBendAssociateID) = 1
),
LKP_WestBendAssociateID AS (
	SELECT
	WestBendAssociateID,
	in_AssociateID,
	AssociateID
	FROM (
		SELECT 
			WestBendAssociateID,
			in_AssociateID,
			AssociateID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Associate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AssociateID ORDER BY WestBendAssociateID) = 1
),
EXP_Add_MetaDataFields AS (
	SELECT
	SQ_SalesReportingRelationship_ODS.SalesReportingRelationshipID,
	SQ_SalesReportingRelationship_ODS.SourceSystemID,
	SQ_SalesReportingRelationship_ODS.HashKey,
	SQ_SalesReportingRelationship_ODS.ModifiedUserID,
	SQ_SalesReportingRelationship_ODS.ModifiedDate,
	SQ_SalesReportingRelationship_ODS.AssociateID,
	SQ_SalesReportingRelationship_ODS.ReportAssociateID,
	SQ_SalesReportingRelationship_ODS.RelationshipType,
	LKP_WestBendAssociateID.WestBendAssociateID AS lkp_WestBendAssociateID,
	LKP_ReportToWestBendAssociateID.WestBendAssociateID AS lkp_ReportToWestBendAssociateID,
	sysdate AS Extract_Date,
	sysdate AS As_of_Date,
	1 AS Record_Count,
	@{pipeline().parameters.SOURCESYSTEMID} AS Source_System_ID
	FROM SQ_SalesReportingRelationship_ODS
	LEFT JOIN LKP_ReportToWestBendAssociateID
	ON LKP_ReportToWestBendAssociateID.AssociateID = SQ_SalesReportingRelationship_ODS.ReportAssociateID
	LEFT JOIN LKP_WestBendAssociateID
	ON LKP_WestBendAssociateID.AssociateID = SQ_SalesReportingRelationship_ODS.AssociateID
),
SalesReportingRelationshipStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.SalesReportingRelationshipStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SalesReportingRelationshipStage
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
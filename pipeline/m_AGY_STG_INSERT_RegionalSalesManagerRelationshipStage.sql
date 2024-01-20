WITH
SQ_RegionalSalesManagerRelationship_ODS AS (
	SELECT
		RegionalSalesManagerRelationshipID,
		SourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AgencyID,
		AssociateID
	FROM RegionalSalesManagerRelationship_ODS
),
LKP_AgencyCode AS (
	SELECT
	AgencyCode,
	AgencyID
	FROM (
		SELECT 
			AgencyCode,
			AgencyID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Agency
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyID ORDER BY AgencyCode) = 1
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
	SQ_RegionalSalesManagerRelationship_ODS.RegionalSalesManagerRelationshipID,
	SQ_RegionalSalesManagerRelationship_ODS.SourceSystemID,
	SQ_RegionalSalesManagerRelationship_ODS.HashKey,
	SQ_RegionalSalesManagerRelationship_ODS.ModifiedUserID,
	SQ_RegionalSalesManagerRelationship_ODS.ModifiedDate,
	SQ_RegionalSalesManagerRelationship_ODS.AgencyID,
	SQ_RegionalSalesManagerRelationship_ODS.AssociateID,
	LKP_AgencyCode.AgencyCode AS lkp_AgencyCode,
	LKP_WestBendAssociateID.WestBendAssociateID AS lkp_WestBendAssociateID,
	sysdate AS Extract_Date,
	Sysdate AS As_of_Date,
	1 AS Record_Count,
	@{pipeline().parameters.SOURCESYSTEMID} AS Source_System_ID
	FROM SQ_RegionalSalesManagerRelationship_ODS
	LEFT JOIN LKP_AgencyCode
	ON LKP_AgencyCode.AgencyID = SQ_RegionalSalesManagerRelationship_ODS.AgencyID
	LEFT JOIN LKP_WestBendAssociateID
	ON LKP_WestBendAssociateID.AssociateID = SQ_RegionalSalesManagerRelationship_ODS.AssociateID
),
RegionalSalesManagerRelationshipStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.RegionalSalesManagerRelationshipStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.RegionalSalesManagerRelationshipStage
	(AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, AgencyID, AgencyCode, AssociateID, WestBendAssociateID, ExtractDate, AsOfDate, RecordCount, SourceSystemID)
	SELECT 
	SourceSystemID AS AGENCYODSSOURCESYSTEMID, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	AGENCYID, 
	lkp_AgencyCode AS AGENCYCODE, 
	ASSOCIATEID, 
	lkp_WestBendAssociateID AS WESTBENDASSOCIATEID, 
	Extract_Date AS EXTRACTDATE, 
	As_of_Date AS ASOFDATE, 
	Record_Count AS RECORDCOUNT, 
	Source_System_ID AS SOURCESYSTEMID
	FROM EXP_Add_MetaDataFields
),
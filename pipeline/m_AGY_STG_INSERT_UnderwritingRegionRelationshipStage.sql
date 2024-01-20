WITH
SQ_UnderwritingRegionRelationship_ODS AS (
	SELECT
		UnderwritingRegionRelationshipID,
		SourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AssociateID,
		UnderwritingRegionID
	FROM UnderwritingRegionRelationship_ODS
),
LKP_UnderwritingRegionCode AS (
	SELECT
	UnderwritingRegionCode,
	UnderwritingRegionID
	FROM (
		SELECT 
			UnderwritingRegionCode,
			UnderwritingRegionID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwritingRegion
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY UnderwritingRegionID ORDER BY UnderwritingRegionCode) = 1
),
LKP_WestBendAssociateID AS (
	SELECT
	in_AssociateID,
	WestBendAssociateID,
	AssociateID
	FROM (
		SELECT 
			in_AssociateID,
			WestBendAssociateID,
			AssociateID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Associate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AssociateID ORDER BY in_AssociateID) = 1
),
EXP_Add_MetaDataFields AS (
	SELECT
	SQ_UnderwritingRegionRelationship_ODS.UnderwritingRegionRelationshipID,
	SQ_UnderwritingRegionRelationship_ODS.SourceSystemID,
	SQ_UnderwritingRegionRelationship_ODS.HashKey,
	SQ_UnderwritingRegionRelationship_ODS.ModifiedUserID,
	SQ_UnderwritingRegionRelationship_ODS.ModifiedDate,
	SQ_UnderwritingRegionRelationship_ODS.AssociateID,
	SQ_UnderwritingRegionRelationship_ODS.UnderwritingRegionID,
	LKP_WestBendAssociateID.WestBendAssociateID AS lkp_WestBendAssociateID,
	LKP_UnderwritingRegionCode.UnderwritingRegionCode,
	sysdate AS Extract_Date,
	Sysdate AS As_of_Date,
	1 AS Record_Count,
	@{pipeline().parameters.SOURCESYSTEMID} AS Source_System_ID
	FROM SQ_UnderwritingRegionRelationship_ODS
	LEFT JOIN LKP_UnderwritingRegionCode
	ON LKP_UnderwritingRegionCode.UnderwritingRegionID = SQ_UnderwritingRegionRelationship_ODS.UnderwritingRegionID
	LEFT JOIN LKP_WestBendAssociateID
	ON LKP_WestBendAssociateID.AssociateID = SQ_UnderwritingRegionRelationship_ODS.AssociateID
),
UnderwritingRegionRelationshipStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingRegionRelationshipStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingRegionRelationshipStage
	(AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, AssociateID, WestBendAssociateID, UnderwritingRegionID, UnderwritingRegionCode, ExtractDate, AsOfDate, RecordCount, SourceSystemID)
	SELECT 
	SourceSystemID AS AGENCYODSSOURCESYSTEMID, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	ASSOCIATEID, 
	lkp_WestBendAssociateID AS WESTBENDASSOCIATEID, 
	UNDERWRITINGREGIONID, 
	UNDERWRITINGREGIONCODE, 
	Extract_Date AS EXTRACTDATE, 
	As_of_Date AS ASOFDATE, 
	Record_Count AS RECORDCOUNT, 
	Source_System_ID AS SOURCESYSTEMID
	FROM EXP_Add_MetaDataFields
),
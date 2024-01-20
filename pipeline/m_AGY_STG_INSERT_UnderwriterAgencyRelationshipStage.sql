WITH
SQ_UnderwriterAgencyRelationship AS (
	SELECT
		UnderwriterAgencyRelationshipId,
		SourceSystemId,
		HashKey,
		ModifiedUserId,
		ModifiedDate,
		AgencyId,
		AssociateId,
		StrategicProfitCenterCode,
		StrategicProfitCenterDescription
	FROM UnderwriterAgencyRelationship
),
LKP_AgencyCode AS (
	SELECT
	AgencyCode,
	in_AgencyID,
	AgencyID
	FROM (
		SELECT 
			AgencyCode,
			in_AgencyID,
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
	SQ_UnderwriterAgencyRelationship.UnderwriterAgencyRelationshipId AS UnderwriterAgencyRelationshipID,
	SQ_UnderwriterAgencyRelationship.SourceSystemId AS SourceSystemID,
	SQ_UnderwriterAgencyRelationship.HashKey,
	SQ_UnderwriterAgencyRelationship.ModifiedUserId AS ModifiedUserID,
	SQ_UnderwriterAgencyRelationship.ModifiedDate,
	SQ_UnderwriterAgencyRelationship.AgencyId AS AgencyID,
	SQ_UnderwriterAgencyRelationship.AssociateId AS AssociateID,
	SQ_UnderwriterAgencyRelationship.StrategicProfitCenterCode,
	SQ_UnderwriterAgencyRelationship.StrategicProfitCenterDescription,
	sysdate AS Extract_Date,
	sysdate AS As_of_Date,
	1 AS Record_Count,
	@{pipeline().parameters.SOURCESYSTEMID} AS Source_System_ID,
	LKP_AgencyCode.AgencyCode AS lkp_AgencyCode,
	LKP_WestBendAssociateID.WestBendAssociateID AS lkp_WestBendAssociateID
	FROM SQ_UnderwriterAgencyRelationship
	LEFT JOIN LKP_AgencyCode
	ON LKP_AgencyCode.AgencyID = SQ_UnderwriterAgencyRelationship.AgencyId
	LEFT JOIN LKP_WestBendAssociateID
	ON LKP_WestBendAssociateID.AssociateID = SQ_UnderwriterAgencyRelationship.AssociateId
),
UnderwriterAgencyRelationshipStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwriterAgencyRelationshipStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwriterAgencyRelationshipStage
	(AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, AgencyID, AgencyCode, AssociateID, WestBendAssociateID, StrategicProfitCenterCode, StrategicProfitCenterDescription, AgencyODSRelationshipId, ExtractDate, AsOfDate, RecordCount, SourceSystemID)
	SELECT 
	SourceSystemID AS AGENCYODSSOURCESYSTEMID, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	AGENCYID, 
	lkp_AgencyCode AS AGENCYCODE, 
	ASSOCIATEID, 
	lkp_WestBendAssociateID AS WESTBENDASSOCIATEID, 
	STRATEGICPROFITCENTERCODE, 
	STRATEGICPROFITCENTERDESCRIPTION, 
	UnderwriterAgencyRelationshipID AS AGENCYODSRELATIONSHIPID, 
	Extract_Date AS EXTRACTDATE, 
	As_of_Date AS ASOFDATE, 
	Record_Count AS RECORDCOUNT, 
	Source_System_ID AS SOURCESYSTEMID
	FROM EXP_Add_MetaDataFields
),
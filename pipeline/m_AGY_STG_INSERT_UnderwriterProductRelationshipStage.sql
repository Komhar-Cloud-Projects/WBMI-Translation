WITH
SQ_UnderwriterProductRelationship AS (
	SELECT
		UnderwriterProductRelationshipId,
		SourceSystemId,
		HashKey,
		ModifiedUserId,
		ModifiedDate,
		AssociateId,
		StrategicProfitCenterCode,
		PolicyOfferingCode,
		ProgramCode,
		PolicyAmountMinimum,
		PolicyAmountMaximum,
		InsuranceSegmentCode,
		BondCategory
	FROM UnderwriterProductRelationship
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
	SQ_UnderwriterProductRelationship.UnderwriterProductRelationshipId,
	SQ_UnderwriterProductRelationship.SourceSystemId AS AgencyODSSourceSystemID,
	SQ_UnderwriterProductRelationship.HashKey,
	SQ_UnderwriterProductRelationship.ModifiedUserId AS ModifiedUserID,
	SQ_UnderwriterProductRelationship.ModifiedDate,
	SQ_UnderwriterProductRelationship.AssociateId AS AssociateID,
	LKP_WestBendAssociateID.WestBendAssociateID AS lkp_WestBendAssociateID,
	SQ_UnderwriterProductRelationship.StrategicProfitCenterCode,
	SQ_UnderwriterProductRelationship.PolicyOfferingCode,
	SQ_UnderwriterProductRelationship.ProgramCode,
	SQ_UnderwriterProductRelationship.PolicyAmountMinimum,
	SQ_UnderwriterProductRelationship.PolicyAmountMaximum,
	SQ_UnderwriterProductRelationship.InsuranceSegmentCode,
	SQ_UnderwriterProductRelationship.BondCategory,
	sysdate AS Extract_Date,
	sysdate AS As_of_Date,
	1 AS Record_Count,
	@{pipeline().parameters.SOURCESYSTEMID} AS Source_System_ID
	FROM SQ_UnderwriterProductRelationship
	LEFT JOIN LKP_WestBendAssociateID
	ON LKP_WestBendAssociateID.AssociateID = SQ_UnderwriterProductRelationship.AssociateId
),
UnderwriterProductRelationshipStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwriterProductRelationshipStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwriterProductRelationshipStage
	(AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, AssociateID, WestBendAssociateID, StrategicProfitCenterCode, PolicyOfferingCode, ProgramCode, PolicyAmountMinimum, PolicyAmountMaximum, AgencyODSRelationshipId, ExtractDate, AsOfDate, RecordCount, SourceSystemID, InsuranceSegmentCode, BondCategory)
	SELECT 
	AGENCYODSSOURCESYSTEMID, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	ASSOCIATEID, 
	lkp_WestBendAssociateID AS WESTBENDASSOCIATEID, 
	STRATEGICPROFITCENTERCODE, 
	POLICYOFFERINGCODE, 
	PROGRAMCODE, 
	POLICYAMOUNTMINIMUM, 
	POLICYAMOUNTMAXIMUM, 
	UnderwriterProductRelationshipId AS AGENCYODSRELATIONSHIPID, 
	Extract_Date AS EXTRACTDATE, 
	As_of_Date AS ASOFDATE, 
	Record_Count AS RECORDCOUNT, 
	Source_System_ID AS SOURCESYSTEMID, 
	INSURANCESEGMENTCODE, 
	BONDCATEGORY
	FROM EXP_Add_MetaDataFields
),
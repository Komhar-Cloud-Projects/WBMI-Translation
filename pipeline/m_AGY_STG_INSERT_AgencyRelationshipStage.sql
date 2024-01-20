WITH
SQ_AgencyRelationship_ODS AS (
	SELECT
		AgencyRelationshipID,
		SourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AgencyID,
		RelatedAgencyID,
		RelationshipType,
		EffectiveDate,
		ExpirationDate
	FROM AgencyRelationship_ODS
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
LKP_RelatedToAgencyCode AS (
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
EXP_Add_MetaDataFields AS (
	SELECT
	SQ_AgencyRelationship_ODS.SourceSystemID,
	SQ_AgencyRelationship_ODS.HashKey,
	SQ_AgencyRelationship_ODS.ModifiedUserID,
	SQ_AgencyRelationship_ODS.ModifiedDate,
	SQ_AgencyRelationship_ODS.AgencyID,
	SQ_AgencyRelationship_ODS.RelatedAgencyID,
	SQ_AgencyRelationship_ODS.RelationshipType,
	SQ_AgencyRelationship_ODS.EffectiveDate,
	SQ_AgencyRelationship_ODS.ExpirationDate,
	LKP_AgencyCode.AgencyCode AS lkp_AgencyCode,
	LKP_RelatedToAgencyCode.AgencyCode AS lkp_RelatedToAgencyCode,
	sysdate AS Extract_Date,
	sysdate AS As_of_Date,
	1 AS Record_Count,
	@{pipeline().parameters.SOURCESYSTEMID} AS Source_System_ID
	FROM SQ_AgencyRelationship_ODS
	LEFT JOIN LKP_AgencyCode
	ON LKP_AgencyCode.AgencyID = SQ_AgencyRelationship_ODS.AgencyID
	LEFT JOIN LKP_RelatedToAgencyCode
	ON LKP_RelatedToAgencyCode.AgencyID = SQ_AgencyRelationship_ODS.RelatedAgencyID
),
AgencyRelationshipStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyRelationshipStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyRelationshipStage
	(AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, AgencyID, AgencyCode, RelatedAgencyID, RelatedToAgencyCode, RelationshipType, EffectiveDate, ExpirationDate, ExtractDate, AsOfDate, RecordCount, SourceSystemID)
	SELECT 
	SourceSystemID AS AGENCYODSSOURCESYSTEMID, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	AGENCYID, 
	lkp_AgencyCode AS AGENCYCODE, 
	RELATEDAGENCYID, 
	lkp_RelatedToAgencyCode AS RELATEDTOAGENCYCODE, 
	RELATIONSHIPTYPE, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	Extract_Date AS EXTRACTDATE, 
	As_of_Date AS ASOFDATE, 
	Record_Count AS RECORDCOUNT, 
	Source_System_ID AS SOURCESYSTEMID
	FROM EXP_Add_MetaDataFields
),
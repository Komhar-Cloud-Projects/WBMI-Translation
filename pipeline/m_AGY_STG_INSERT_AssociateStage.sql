WITH
SQ_Associate AS (
	SELECT
		AssociateID,
		SourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		WestBendAssociateID,
		AssociateRole,
		RoleSpecificUserCode,
		DisplayName,
		LastName,
		FirstName,
		MiddleName,
		Suffix,
		EmailAddress,
		UserId,
		StrategicProfitCenterCode,
		StrategicProfitCenterDescription
	FROM Associate
),
EXP_Add_MetaDataFields AS (
	SELECT
	AssociateID,
	SourceSystemID,
	HashKey,
	ModifiedUserID,
	ModifiedDate,
	WestBendAssociateID,
	AssociateRole,
	RoleSpecificUserCode,
	DisplayName,
	LastName,
	FirstName,
	MiddleName,
	Suffix,
	EmailAddress,
	UserId,
	StrategicProfitCenterCode,
	StrategicProfitCenterDescription,
	sysdate AS Extract_Date,
	sysdate AS As_of_Date,
	1 AS Record_Count,
	@{pipeline().parameters.SOURCESYSTEMID} AS Source_System_ID
	FROM SQ_Associate
),
AssociateStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.AssociateStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AssociateStage
	(AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, WestBendAssociateID, AssociateRole, RoleSpecificUserCode, DisplayName, LastName, FirstName, MiddleName, Suffix, EmailAddress, ExtractDate, AsOfDate, RecordCount, SourceSystemID, UserId, StrategicProfitCenterCode, StrategicProfitCenterDescription)
	SELECT 
	SourceSystemID AS AGENCYODSSOURCESYSTEMID, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	WESTBENDASSOCIATEID, 
	ASSOCIATEROLE, 
	ROLESPECIFICUSERCODE, 
	DISPLAYNAME, 
	LASTNAME, 
	FIRSTNAME, 
	MIDDLENAME, 
	SUFFIX, 
	EMAILADDRESS, 
	Extract_Date AS EXTRACTDATE, 
	As_of_Date AS ASOFDATE, 
	Record_Count AS RECORDCOUNT, 
	Source_System_ID AS SOURCESYSTEMID, 
	USERID, 
	STRATEGICPROFITCENTERCODE, 
	STRATEGICPROFITCENTERDESCRIPTION
	FROM EXP_Add_MetaDataFields
),
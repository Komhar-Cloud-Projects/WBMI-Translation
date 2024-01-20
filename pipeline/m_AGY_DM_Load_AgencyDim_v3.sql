WITH
LKP_Agency_V2 AS (
	SELECT
	AgencyCode,
	AgencyAKID
	FROM (
		SELECT 
			AgencyCode,
			AgencyAKID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Agency
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyAKID ORDER BY AgencyCode) = 1
),
SQ_Agency_V2 AS (
	SELECT
		AgencyID,
		CurrentSnapshotFlag,
		AuditID,
		EffectiveDate,
		ExpirationDate,
		SourceSystemID,
		CreatedDate,
		ModifiedDate,
		HashKey,
		AgencyAKID,
		SalesTerritoryAKID,
		RegionalSalesManagerAKID,
		AgencyCode,
		LegalName,
		DoingBusinessAsName,
		PrimaryPhoneNumber,
		PrimaryFaxNumber,
		PrimaryEmailAddress,
		StatusCode,
		StatusDescription,
		AppointedDate,
		TerminatedDate,
		CustomerCareStatus,
		FederalTaxId,
		ProfitSharingGuaranteeFlag,
		LicensedIndicator,
		AbbreviatedName,
		AssignedStateCode,
		ClosedDate
	FROM Agency_V2
	WHERE CurrentSnapshotFlag = 1
),
LKP_LegalParent AS (
	SELECT
	RelatedAgencyAKID,
	AgencyAKID
	FROM (
		SELECT AgencyRelationship.RelatedAgencyAKID as RelatedAgencyAKID, AgencyRelationship.AgencyAKID as AgencyAKID FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.AgencyRelationship AgencyRelationship
		WHERE AgencyRelationship.CurrentSnapshotFlag = 1 and AgencyRelationship.RelationshipType in ('LEGAL SPECIAL ACCOUNT', 'LEGAL BRANCH', 'LEGAL TERMINATED', 'LEGAL MERGED')
		ORDER BY AgencyRelationship.AgencyAKID,AgencyRelationship.AgencyRelationshipExpirationDate ---
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyAKID ORDER BY RelatedAgencyAKID DESC) = 1
),
LKP_LegalParent_deleted AS (
	SELECT
	RelatedAgencyAKID,
	AgencyAKID
	FROM (
		SELECT 
			RelatedAgencyAKID,
			AgencyAKID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.AgencyRelationship
		WHERE CurrentSnapshotFlag = 1 and RelationshipType in ('LEGAL DELETED')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyAKID ORDER BY RelatedAgencyAKID DESC) = 1
),
LKP_MailingAddress AS (
	SELECT
	AgencyAddressID,
	AgencyAddressAKID,
	AddressLine1,
	AddressLine2,
	AddressLine3,
	City,
	ZipCode,
	CountyCode,
	CountyName,
	StateAbbreviation,
	CountryAbbreviation,
	Latitude,
	Longitude,
	AgencyAKID
	FROM (
		SELECT 
			AgencyAddressID,
			AgencyAddressAKID,
			AddressLine1,
			AddressLine2,
			AddressLine3,
			City,
			ZipCode,
			CountyCode,
			CountyName,
			StateAbbreviation,
			CountryAbbreviation,
			Latitude,
			Longitude,
			AgencyAKID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.AgencyAddress
		WHERE CurrentSnapshotFlag = 1 and AddressType = 'Mailing'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyAKID ORDER BY AgencyAddressID DESC) = 1
),
LKP_PhysicalAddress AS (
	SELECT
	AgencyAddressID,
	AgencyAddressAKID,
	AddressLine1,
	AddressLine2,
	AddressLine3,
	City,
	ZipCode,
	CountyCode,
	CountyName,
	StateAbbreviation,
	CountryAbbreviation,
	Latitude,
	Longitude,
	AgencyAKID
	FROM (
		SELECT 
			AgencyAddressID,
			AgencyAddressAKID,
			AddressLine1,
			AddressLine2,
			AddressLine3,
			City,
			ZipCode,
			CountyCode,
			CountyName,
			StateAbbreviation,
			CountryAbbreviation,
			Latitude,
			Longitude,
			AgencyAKID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.AgencyAddress
		WHERE CurrentSnapshotFlag = 1 and AddressType = 'Physical'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyAKID ORDER BY AgencyAddressID DESC) = 1
),
EXP_CleanupData AS (
	SELECT
	LKP_PhysicalAddress.AgencyAddressID AS lkp_physical_AgencyAddressID,
	LKP_PhysicalAddress.AgencyAddressAKID AS lkp_physical_AgencyAddressAKID,
	LKP_PhysicalAddress.AddressLine1 AS lkp_physical_AddressLine1,
	LKP_PhysicalAddress.AddressLine2 AS lkp_physical_AddressLine2,
	LKP_PhysicalAddress.AddressLine3 AS lkp_physical_AddressLine3,
	LKP_PhysicalAddress.City AS lkp_physical_City,
	LKP_PhysicalAddress.ZipCode AS lkp_physical_ZipCode,
	LKP_PhysicalAddress.CountyCode AS lkp_physical_CountyCode,
	LKP_PhysicalAddress.CountyName AS lkp_physical_CountyName,
	LKP_PhysicalAddress.StateAbbreviation AS lkp_physical_StateAbbreviation,
	LKP_PhysicalAddress.CountryAbbreviation AS lkp_physical_CountryAbbreviation,
	LKP_PhysicalAddress.Latitude AS lkp_physical_Latitude,
	LKP_PhysicalAddress.Longitude AS lkp_physical_Longitude,
	LKP_MailingAddress.AgencyAddressID AS lkp_mail_AgencyAddressID,
	LKP_MailingAddress.AgencyAddressAKID AS lkp_mail_AgencyAddressAKID,
	LKP_MailingAddress.AddressLine1 AS lkp_mail_AddressLine1,
	LKP_MailingAddress.AddressLine2 AS lkp_mail_AddressLine2,
	LKP_MailingAddress.AddressLine3 AS lkp_mail_AddressLine3,
	LKP_MailingAddress.City AS lkp_mail_City,
	LKP_MailingAddress.ZipCode AS lkp_mail_ZipCode,
	LKP_MailingAddress.CountyCode AS lkp_mail_CountyCode,
	LKP_MailingAddress.CountyName AS lkp_mail_CountyName,
	LKP_MailingAddress.StateAbbreviation AS lkp_mail_StateAbbreviation,
	LKP_MailingAddress.CountryAbbreviation AS lkp_mail_CountryAbbreviation,
	LKP_MailingAddress.Latitude AS lkp_mail_Latitude,
	LKP_MailingAddress.Longitude AS lkp_mail_Longitude,
	LKP_LegalParent.RelatedAgencyAKID AS lkp_LegalParentAgencyAKID,
	LKP_LegalParent_deleted.RelatedAgencyAKID AS lkp_LegalParentAgencyAKID_Deleted,
	SQ_Agency_V2.AgencyID AS AgencyPKID,
	SQ_Agency_V2.AgencyAKID,
	SQ_Agency_V2.AgencyCode,
	SQ_Agency_V2.LegalName,
	SQ_Agency_V2.DoingBusinessAsName,
	SQ_Agency_V2.PrimaryPhoneNumber,
	SQ_Agency_V2.PrimaryFaxNumber,
	SQ_Agency_V2.PrimaryEmailAddress,
	SQ_Agency_V2.StatusCode,
	SQ_Agency_V2.StatusDescription,
	SQ_Agency_V2.AppointedDate,
	SQ_Agency_V2.TerminatedDate,
	SQ_Agency_V2.CustomerCareStatus,
	SQ_Agency_V2.LicensedIndicator,
	-- *INF*: Decode(true,
	-- LicensedIndicator = 'Y', '1', 
	-- LicensedIndicator = 'N', '0', 
	-- '1')
	Decode(
	    true,
	    LicensedIndicator = 'Y', '1',
	    LicensedIndicator = 'N', '0',
	    '1'
	) AS o_LicensedIndicator,
	SQ_Agency_V2.AbbreviatedName,
	SQ_Agency_V2.AssignedStateCode,
	'NA' AS v_DefaultCharCode,
	-1 AS v_DefaultInt,
	'N/A' AS v_DefaultChar,
	0 AS v_DefaultDecimal,
	-- *INF*: :LKP.LKP_AGENCY_V2(lkp_LegalParentAgencyAKID)
	LKP_AGENCY_V2_lkp_LegalParentAgencyAKID.AgencyCode AS v_LegalPrimaryAgencyCode,
	-- *INF*: IIF(IsNull(lkp_mail_AgencyAddressID), v_DefaultInt, lkp_mail_AgencyAddressID)
	IFF(lkp_mail_AgencyAddressID IS NULL, v_DefaultInt, lkp_mail_AgencyAddressID) AS o_mail_AgencyAddressID,
	-- *INF*: IIF(IsNull(lkp_mail_AgencyAddressID), v_DefaultInt, lkp_mail_AgencyAddressAKID)
	IFF(lkp_mail_AgencyAddressID IS NULL, v_DefaultInt, lkp_mail_AgencyAddressAKID) AS o_mail_AgencyAddressAKID,
	-- *INF*: IIF(IsNull(lkp_physical_AgencyAddressID), v_DefaultInt, lkp_physical_AgencyAddressID)
	IFF(lkp_physical_AgencyAddressID IS NULL, v_DefaultInt, lkp_physical_AgencyAddressID) AS o_physical_AgencyAddressID,
	-- *INF*: IIF(IsNull(lkp_physical_AgencyAddressID), v_DefaultInt, lkp_physical_AgencyAddressAKID)
	IFF(lkp_physical_AgencyAddressID IS NULL, v_DefaultInt, lkp_physical_AgencyAddressAKID) AS o_physical_AgencyAddressAKID,
	-- *INF*: IIF(IsNull(lkp_LegalParentAgencyAKID), 'Yes', 'No')
	IFF(lkp_LegalParentAgencyAKID IS NULL, 'Yes', 'No') AS o_PrimaryAgencyIndicator,
	-- *INF*: IIF(IsNull(lkp_physical_AgencyAddressID), v_DefaultChar, lkp_physical_AddressLine1)
	IFF(lkp_physical_AgencyAddressID IS NULL, v_DefaultChar, lkp_physical_AddressLine1) AS o_physical_AddressLine1,
	-- *INF*: IIF(IsNull(lkp_physical_AgencyAddressID), v_DefaultChar, lkp_physical_AddressLine2)
	IFF(lkp_physical_AgencyAddressID IS NULL, v_DefaultChar, lkp_physical_AddressLine2) AS o_physical_AddressLine2,
	-- *INF*: IIF(IsNull(lkp_physical_AgencyAddressID), v_DefaultChar, lkp_physical_AddressLine3)
	IFF(lkp_physical_AgencyAddressID IS NULL, v_DefaultChar, lkp_physical_AddressLine3) AS o_physical_AddressLine3,
	-- *INF*: IIF(IsNull(lkp_physical_AgencyAddressID), v_DefaultChar, lkp_physical_City)
	IFF(lkp_physical_AgencyAddressID IS NULL, v_DefaultChar, lkp_physical_City) AS o_physical_City,
	-- *INF*: IIF(IsNull(lkp_physical_AgencyAddressID), v_DefaultChar, lkp_physical_ZipCode)
	IFF(lkp_physical_AgencyAddressID IS NULL, v_DefaultChar, lkp_physical_ZipCode) AS o_physical_ZipCode,
	-- *INF*: IIF(IsNull(lkp_physical_AgencyAddressID), v_DefaultChar, lkp_physical_CountyCode)
	IFF(lkp_physical_AgencyAddressID IS NULL, v_DefaultChar, lkp_physical_CountyCode) AS o_physical_CountyCode,
	-- *INF*: IIF(IsNull(lkp_physical_AgencyAddressID), v_DefaultChar, lkp_physical_CountyName)
	IFF(lkp_physical_AgencyAddressID IS NULL, v_DefaultChar, lkp_physical_CountyName) AS o_physical_CountyName,
	-- *INF*: IIF(IsNull(lkp_physical_AgencyAddressID), v_DefaultCharCode, lkp_physical_StateAbbreviation)
	IFF(lkp_physical_AgencyAddressID IS NULL, v_DefaultCharCode, lkp_physical_StateAbbreviation) AS o_physical_StateAbbreviation,
	-- *INF*: IIF(IsNull(lkp_physical_AgencyAddressID), v_DefaultChar, lkp_physical_CountryAbbreviation)
	IFF(lkp_physical_AgencyAddressID IS NULL, v_DefaultChar, lkp_physical_CountryAbbreviation) AS o_physical_CountryAbbreviation,
	-- *INF*: IIF(IsNull(lkp_physical_AgencyAddressID), v_DefaultDecimal, lkp_physical_Latitude)
	IFF(lkp_physical_AgencyAddressID IS NULL, v_DefaultDecimal, lkp_physical_Latitude) AS o_physical_Latitude,
	-- *INF*: IIF(IsNull(lkp_physical_AgencyAddressID), v_DefaultDecimal, lkp_physical_Longitude)
	IFF(lkp_physical_AgencyAddressID IS NULL, v_DefaultDecimal, lkp_physical_Longitude) AS o_physical_Longitude,
	-- *INF*: IIF(IsNull(lkp_mail_AgencyAddressID), v_DefaultChar, lkp_mail_AddressLine1)
	IFF(lkp_mail_AgencyAddressID IS NULL, v_DefaultChar, lkp_mail_AddressLine1) AS o_mail_AddressLine1,
	-- *INF*: IIF(IsNull(lkp_mail_AgencyAddressID), v_DefaultChar, lkp_mail_AddressLine2)
	IFF(lkp_mail_AgencyAddressID IS NULL, v_DefaultChar, lkp_mail_AddressLine2) AS o_mail_AddressLine2,
	-- *INF*: IIF(IsNull(lkp_mail_AgencyAddressID), v_DefaultChar, lkp_mail_AddressLine3)
	IFF(lkp_mail_AgencyAddressID IS NULL, v_DefaultChar, lkp_mail_AddressLine3) AS o_mail_AddressLine3,
	-- *INF*: IIF(IsNull(lkp_mail_AgencyAddressID), v_DefaultChar, lkp_mail_City)
	IFF(lkp_mail_AgencyAddressID IS NULL, v_DefaultChar, lkp_mail_City) AS o_mail_City,
	-- *INF*: IIF(IsNull(lkp_mail_AgencyAddressID), v_DefaultChar, lkp_mail_ZipCode)
	IFF(lkp_mail_AgencyAddressID IS NULL, v_DefaultChar, lkp_mail_ZipCode) AS o_mail_ZipCode,
	-- *INF*: IIF(IsNull(lkp_mail_AgencyAddressID), v_DefaultChar, lkp_mail_CountyCode)
	IFF(lkp_mail_AgencyAddressID IS NULL, v_DefaultChar, lkp_mail_CountyCode) AS o_mail_CountyCode,
	-- *INF*: IIF(IsNull(lkp_mail_AgencyAddressID), v_DefaultChar, lkp_mail_CountyName)
	IFF(lkp_mail_AgencyAddressID IS NULL, v_DefaultChar, lkp_mail_CountyName) AS o_mail_CountyName,
	-- *INF*: IIF(IsNull(lkp_mail_AgencyAddressID), v_DefaultCharCode, lkp_mail_StateAbbreviation)
	IFF(lkp_mail_AgencyAddressID IS NULL, v_DefaultCharCode, lkp_mail_StateAbbreviation) AS o_mail_StateAbbreviation,
	-- *INF*: IIF(IsNull(lkp_mail_AgencyAddressID), v_DefaultChar, lkp_mail_CountryAbbreviation)
	IFF(lkp_mail_AgencyAddressID IS NULL, v_DefaultChar, lkp_mail_CountryAbbreviation) AS o_mail_CountryAbbreviation,
	-- *INF*: IIF(IsNull(lkp_mail_AgencyAddressID), v_DefaultDecimal, lkp_mail_Latitude)
	IFF(lkp_mail_AgencyAddressID IS NULL, v_DefaultDecimal, lkp_mail_Latitude) AS o_mail_Latitude,
	-- *INF*: IIF(IsNull(lkp_mail_AgencyAddressID), v_DefaultDecimal, lkp_mail_Longitude)
	IFF(lkp_mail_AgencyAddressID IS NULL, v_DefaultDecimal, lkp_mail_Longitude) AS o_mail_Longitude,
	-- *INF*: IIF(IsNull(v_LegalPrimaryAgencyCode) OR lkp_LegalParentAgencyAKID = lkp_LegalParentAgencyAKID_Deleted,AgencyCode,v_LegalPrimaryAgencyCode)
	IFF(
	    v_LegalPrimaryAgencyCode IS NULL
	    or lkp_LegalParentAgencyAKID = lkp_LegalParentAgencyAKID_Deleted,
	    AgencyCode,
	    v_LegalPrimaryAgencyCode
	) AS o_LegalPrimaryAgencyCode,
	SQ_Agency_V2.CurrentSnapshotFlag,
	SQ_Agency_V2.AuditID,
	SQ_Agency_V2.EffectiveDate,
	SQ_Agency_V2.ExpirationDate,
	SQ_Agency_V2.SourceSystemID,
	SQ_Agency_V2.CreatedDate,
	SQ_Agency_V2.ModifiedDate,
	SQ_Agency_V2.HashKey,
	SQ_Agency_V2.SalesTerritoryAKID,
	SQ_Agency_V2.RegionalSalesManagerAKID,
	SQ_Agency_V2.FederalTaxId,
	SQ_Agency_V2.ProfitSharingGuaranteeFlag,
	SQ_Agency_V2.ClosedDate
	FROM SQ_Agency_V2
	LEFT JOIN LKP_LegalParent
	ON LKP_LegalParent.AgencyAKID = SQ_Agency_V2.AgencyAKID
	LEFT JOIN LKP_LegalParent_deleted
	ON LKP_LegalParent_deleted.AgencyAKID = SQ_Agency_V2.AgencyAKID
	LEFT JOIN LKP_MailingAddress
	ON LKP_MailingAddress.AgencyAKID = SQ_Agency_V2.AgencyAKID
	LEFT JOIN LKP_PhysicalAddress
	ON LKP_PhysicalAddress.AgencyAKID = SQ_Agency_V2.AgencyAKID
	LEFT JOIN LKP_AGENCY_V2 LKP_AGENCY_V2_lkp_LegalParentAgencyAKID
	ON LKP_AGENCY_V2_lkp_LegalParentAgencyAKID.AgencyAKID = lkp_LegalParentAgencyAKID

),
LKP_AgencyDim_ExistingData AS (
	SELECT
	AgencyDimID,
	AgencyDimHashKey,
	EDWAgencyPKID,
	EDWAgencyAddressPhysicalPKID,
	EDWAgencyAddressMailingPKID,
	SalesDivisionDimID,
	AgencyClosedDate,
	EDWAgencyAKID
	FROM (
		SELECT 
			AgencyDimID,
			AgencyDimHashKey,
			EDWAgencyPKID,
			EDWAgencyAddressPhysicalPKID,
			EDWAgencyAddressMailingPKID,
			SalesDivisionDimID,
			AgencyClosedDate,
			EDWAgencyAKID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyDim
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWAgencyAKID ORDER BY AgencyDimID DESC) = 1
),
EXP_CheckForChange AS (
	SELECT
	LKP_AgencyDim_ExistingData.AgencyDimID AS lkp_ExistingAgencyDimId,
	LKP_AgencyDim_ExistingData.AgencyDimHashKey AS lkp_AgencyDimHashKey,
	LKP_AgencyDim_ExistingData.EDWAgencyPKID AS lkp_ExistingAgencyPKID,
	LKP_AgencyDim_ExistingData.EDWAgencyAddressPhysicalPKID AS lkp_ExistingAgencyAddressPhysicalPKID,
	LKP_AgencyDim_ExistingData.EDWAgencyAddressMailingPKID AS lkp_ExistingAgencyAddressMailingPKID,
	LKP_AgencyDim_ExistingData.SalesDivisionDimID AS lkp_SalesDivisionDimID,
	LKP_AgencyDim_ExistingData.AgencyClosedDate AS lkp_AgencyClosedDate,
	EXP_CleanupData.AgencyPKID AS i_AgencyPKID,
	EXP_CleanupData.AgencyAKID AS i_AgencyAKID,
	EXP_CleanupData.AgencyCode AS i_AgencyCode,
	EXP_CleanupData.LegalName AS i_LegalName,
	EXP_CleanupData.DoingBusinessAsName AS i_DoingBusinessAsName,
	EXP_CleanupData.PrimaryPhoneNumber AS i_PrimaryPhoneNumber,
	EXP_CleanupData.PrimaryFaxNumber AS i_PrimaryFaxNumber,
	EXP_CleanupData.PrimaryEmailAddress AS i_PrimaryEmailAddress,
	EXP_CleanupData.StatusCode AS i_StatusCode,
	EXP_CleanupData.StatusDescription AS i_StatusDescription,
	EXP_CleanupData.AppointedDate AS i_AppointedDate,
	EXP_CleanupData.TerminatedDate AS i_TerminatedDate,
	EXP_CleanupData.CustomerCareStatus AS i_CustomerCareStatus,
	EXP_CleanupData.o_mail_AgencyAddressID AS i_mail_AgencyAddressPKID,
	EXP_CleanupData.o_mail_AgencyAddressAKID AS i_mail_AgencyAddressAKID,
	EXP_CleanupData.o_physical_AgencyAddressID AS i_physical_AgencyAddressPKID,
	EXP_CleanupData.o_physical_AgencyAddressAKID AS i_physical_AgencyAddressAKID,
	EXP_CleanupData.o_PrimaryAgencyIndicator AS i_PrimaryAgencyIndicator,
	EXP_CleanupData.o_physical_AddressLine1 AS i_physical_AddressLine1,
	EXP_CleanupData.o_physical_AddressLine2 AS i_physical_AddressLine2,
	EXP_CleanupData.o_physical_AddressLine3 AS i_physical_AddressLine3,
	EXP_CleanupData.o_physical_City AS i_physical_City,
	EXP_CleanupData.o_physical_ZipCode AS i_physical_ZipCode,
	EXP_CleanupData.o_physical_CountyCode AS i_physical_CountyCode,
	EXP_CleanupData.o_physical_CountyName AS i_physical_CountyName,
	EXP_CleanupData.o_physical_StateAbbreviation AS i_physical_StateAbbreviation,
	EXP_CleanupData.o_physical_CountryAbbreviation AS i_physical_CountryAbbreviation,
	EXP_CleanupData.o_physical_Latitude AS i_physical_Latitude,
	EXP_CleanupData.o_physical_Longitude AS i_physical_Longitude,
	EXP_CleanupData.o_mail_AddressLine1 AS i_mail_AddressLine1,
	EXP_CleanupData.o_mail_AddressLine2 AS i_mail_AddressLine2,
	EXP_CleanupData.o_mail_AddressLine3 AS i_mail_AddressLine3,
	EXP_CleanupData.o_mail_City AS i_mail_City,
	EXP_CleanupData.o_mail_ZipCode AS i_mail_ZipCode,
	EXP_CleanupData.o_mail_CountyCode AS i_mail_CountyCode,
	EXP_CleanupData.o_mail_CountyName AS i_mail_CountyName,
	EXP_CleanupData.o_mail_StateAbbreviation AS i_mail_StateAbbreviation,
	EXP_CleanupData.o_mail_CountryAbbreviation AS i_mail_CountryAbbreviation,
	EXP_CleanupData.o_mail_Latitude AS i_mail_Latitude,
	EXP_CleanupData.o_mail_Longitude AS i_mail_Longitude,
	EXP_CleanupData.o_LegalPrimaryAgencyCode AS i_LegalPrimaryAgencyCode,
	EXP_CleanupData.o_LicensedIndicator AS LicensedIndicator,
	EXP_CleanupData.AbbreviatedName,
	EXP_CleanupData.AssignedStateCode,
	EXP_CleanupData.ClosedDate,
	-1 AS SalesDivisionDimID,
	-- *INF*: MD5(i_AgencyCode ||'&'|| i_LegalName ||'&'|| i_StatusCode ||'&'|| TO_CHAR(i_AppointedDate) || '&'||TO_CHAR(i_TerminatedDate) ||'&'|| i_PrimaryAgencyIndicator ||'&'|| i_physical_ZipCode||'&'||i_LegalPrimaryAgencyCode ||'&'|| TO_CHAR(LicensedIndicator) || '&'||TO_CHAR(AbbreviatedName) || '&'||TO_CHAR(AssignedStateCode))
	-- 
	-- 
	-- 
	-- 
	-- --MD5(AgencyCode || LegalName || StatusCode || to_char(AppointedDate) || to_char(TerminatedDate) || to_char(LegalParentAgencyAKID) || to_char(physical_AgencyAddressAKID) || to_char(mail_AgencyAddressAKID) || physical_ZipCode)
	MD5(i_AgencyCode || '&' || i_LegalName || '&' || i_StatusCode || '&' || TO_CHAR(i_AppointedDate) || '&' || TO_CHAR(i_TerminatedDate) || '&' || i_PrimaryAgencyIndicator || '&' || i_physical_ZipCode || '&' || i_LegalPrimaryAgencyCode || '&' || TO_CHAR(LicensedIndicator) || '&' || TO_CHAR(AbbreviatedName) || '&' || TO_CHAR(AssignedStateCode)) AS v_new_Type2HashKey,
	-- *INF*: DECODE(TRUE, 
	-- i_AgencyPKID <> lkp_ExistingAgencyPKID, 'Y',
	-- i_mail_AgencyAddressPKID <> lkp_ExistingAgencyAddressMailingPKID, 'Y',
	-- i_physical_AgencyAddressPKID <> lkp_ExistingAgencyAddressPhysicalPKID, 'Y',
	-- lkp_AgencyClosedDate<>ClosedDate,'Y',
	-- 'N')
	DECODE(
	    TRUE,
	    i_AgencyPKID <> lkp_ExistingAgencyPKID, 'Y',
	    i_mail_AgencyAddressPKID <> lkp_ExistingAgencyAddressMailingPKID, 'Y',
	    i_physical_AgencyAddressPKID <> lkp_ExistingAgencyAddressPhysicalPKID, 'Y',
	    lkp_AgencyClosedDate <> ClosedDate, 'Y',
	    'N'
	) AS v_ChangeToEDWRecord,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_AgencyDimHashKey), 'Insert',
	-- (lkp_AgencyDimHashKey = v_new_Type2HashKey) and (v_ChangeToEDWRecord = 'N'), 'Ignore',
	-- (lkp_AgencyDimHashKey <> v_new_Type2HashKey), 'Expire',
	-- (lkp_AgencyDimHashKey = v_new_Type2HashKey) and (v_ChangeToEDWRecord = 'Y'), 'Update',
	-- 'Ignore')
	-- 
	-- -- If the existing record is not found based on the AKID, it's always an insert
	-- -- If there are no changes, we ignore the record
	-- -- If one of the type 2 attributes changed, we expire the old record (also inserts a new record, see router)
	-- -- If there was no change to the type 2 attributes AND there was a change to the PKID in the EDW then we update the record.  Important to have the logic comparing the hash keys, otherwise we might attempt to update records where we are already expiring and inserting a new record.
	-- 	
	-- 
	-- 
	DECODE(
	    TRUE,
	    lkp_AgencyDimHashKey IS NULL, 'Insert',
	    (lkp_AgencyDimHashKey = v_new_Type2HashKey) and (v_ChangeToEDWRecord = 'N'), 'Ignore',
	    (lkp_AgencyDimHashKey <> v_new_Type2HashKey), 'Expire',
	    (lkp_AgencyDimHashKey = v_new_Type2HashKey) and (v_ChangeToEDWRecord = 'Y'), 'Update',
	    'Ignore'
	) AS v_InsertUpdateExpireOrIgnore,
	1 AS o_CurrentSnapshotFlag,
	0 AS o_ExpireSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: DECODE(v_InsertUpdateExpireOrIgnore, 
	-- 'Insert', TO_DATE('1800-01-01 01:00:00', 'YYYY-MM-DD HH24:MI:SS'),
	-- SYSDATE)
	-- 
	-- 
	-- 
	-- 
	-- 
	-- --Decode(v_InsertUpdateExpireOrIgnore, 'Insert', trunc(sysdate, 'DD'), lkp_ExistingEffectiveDate)
	DECODE(
	    v_InsertUpdateExpireOrIgnore,
	    'Insert', TO_TIMESTAMP('1800-01-01 01:00:00', 'YYYY-MM-DD HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
	-- 
	-- 
	-- 
	-- 
	-- --Decode(v_InsertUpdateExpireOrIgnore, 'Expire', add_to_date(trunc(sysdate, 'DD'), 'MS', -1 ), to_date('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'))
	TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') AS o_ExpirationDate,
	-- *INF*: ADD_TO_DATE(SYSDATE, 'SS', -1 )
	-- 
	-- 
	-- --ADD_TO_DATE(TRUNC(SYSDATE, 'DD'), 'MS', -1 )
	DATEADD(SECOND,- 1,CURRENT_TIMESTAMP) AS o_ExpirationDate_ForExpire,
	SYSDATE AS o_CurrentDate,
	v_new_Type2HashKey AS o_HashKey,
	i_AgencyPKID AS o_AgencyPKID,
	v_InsertUpdateExpireOrIgnore AS o_InsertUpdateExpireOrIgnore
	FROM EXP_CleanupData
	LEFT JOIN LKP_AgencyDim_ExistingData
	ON LKP_AgencyDim_ExistingData.EDWAgencyAKID = EXP_CleanupData.AgencyAKID
),
RTR_InsertUpdateOrExpire AS (
	SELECT
	lkp_ExistingAgencyDimId AS AgencyDimId_Existing,
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	o_ExpireSnapshotFlag AS ExpireSnapshotFlag,
	o_AuditID AS AuditID,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_ExpirationDate_ForExpire AS ExpirationDate_ForExpire,
	o_CurrentDate AS CreatedDate,
	o_CurrentDate AS ModifiedDate,
	o_HashKey AS HashKey,
	o_AgencyPKID AS AgencyPKID,
	i_physical_AgencyAddressPKID AS PhysicalAgencyAddressPKID,
	i_mail_AgencyAddressPKID AS MailAgencyAddressPKID,
	i_AgencyAKID AS AgencyAKID,
	i_physical_AgencyAddressAKID AS PhysicalAgencyAddressAKID,
	i_mail_AgencyAddressAKID AS MailAgencyAddressAKID,
	i_AgencyCode AS AgencyCode,
	i_LegalName AS LegalName,
	i_DoingBusinessAsName AS DoingBusinessAsName,
	i_PrimaryPhoneNumber AS PrimaryPhoneNumber,
	i_PrimaryFaxNumber AS PrimaryFaxNumber,
	i_PrimaryEmailAddress AS PrimaryEmailAddress,
	i_StatusCode AS StatusCode,
	i_StatusDescription AS StatusDescription,
	i_AppointedDate AS AppointedDate,
	i_TerminatedDate AS TerminatedDate,
	i_CustomerCareStatus AS CustomerCareStatus,
	i_PrimaryAgencyIndicator AS PrimaryAgencyIndicator,
	i_physical_AddressLine1 AS PhysicalAddressLine1,
	i_physical_AddressLine2 AS PhysicalAddressLine2,
	i_physical_AddressLine3 AS PhysicalAddressLine3,
	i_physical_City AS PhysicalCity,
	i_physical_ZipCode AS PhysicalZipCode,
	i_physical_CountyCode AS PhysicalCountyCode,
	i_physical_CountyName AS PhysicalCountyName,
	i_physical_StateAbbreviation AS PhysicalStateAbbreviation,
	i_physical_CountryAbbreviation AS PhysicalCountryAbbreviation,
	i_physical_Latitude AS PhysicalLatitude,
	i_physical_Longitude AS PhysicalLongitude,
	i_mail_AddressLine1 AS MailAddressLine1,
	i_mail_AddressLine2 AS MailAddressLine2,
	i_mail_AddressLine3 AS MailAddressLine3,
	i_mail_City AS MailCity,
	i_mail_ZipCode AS MailZipCode,
	i_mail_CountyCode AS MailCountyCode,
	i_mail_CountyName AS MailCountyName,
	i_mail_StateAbbreviation AS MailStateAbbreviation,
	i_mail_CountryAbbreviation AS MailCountryAbbreviation,
	i_mail_Latitude AS MailLatitude,
	i_mail_Longitude AS MailLongitude,
	i_LegalPrimaryAgencyCode AS LegalPrimaryAgencyCode,
	o_InsertUpdateExpireOrIgnore AS InsertUpdateExpireOrIgnore,
	LicensedIndicator,
	AbbreviatedName,
	AssignedStateCode,
	SalesDivisionDimID,
	ClosedDate
	FROM EXP_CheckForChange
),
RTR_InsertUpdateOrExpire_Expire AS (SELECT * FROM RTR_InsertUpdateOrExpire WHERE InsertUpdateExpireOrIgnore = 'Expire'),
RTR_InsertUpdateOrExpire_Insert AS (SELECT * FROM RTR_InsertUpdateOrExpire WHERE InsertUpdateExpireOrIgnore = 'Insert' or InsertUpdateExpireOrIgnore = 'Expire'),
RTR_InsertUpdateOrExpire_Update AS (SELECT * FROM RTR_InsertUpdateOrExpire WHERE InsertUpdateExpireOrIgnore = 'Update'),
UPD_Inserts AS (
	SELECT
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	CreatedDate, 
	ModifiedDate, 
	HashKey, 
	AgencyPKID, 
	PhysicalAgencyAddressPKID, 
	MailAgencyAddressPKID, 
	AgencyAKID, 
	PhysicalAgencyAddressAKID, 
	MailAgencyAddressAKID, 
	AgencyCode, 
	LegalName, 
	DoingBusinessAsName, 
	PrimaryPhoneNumber, 
	PrimaryFaxNumber, 
	PrimaryEmailAddress, 
	StatusCode, 
	StatusDescription, 
	AppointedDate, 
	TerminatedDate, 
	CustomerCareStatus, 
	PrimaryAgencyIndicator, 
	PhysicalAddressLine1, 
	PhysicalAddressLine2, 
	PhysicalAddressLine AS PhysicalAddressLine3, 
	PhysicalCity, 
	PhysicalZipCode, 
	PhysicalCountyCode, 
	PhysicalCountyName, 
	PhysicalStateAbbreviation, 
	PhysicalCountryAbbreviation, 
	PhysicalLatitude, 
	PhysicalLongitude, 
	MailAddressLine1, 
	MailAddressLine2, 
	MailAddressLine AS MailAddressLine3, 
	MailCity, 
	MailZipCode, 
	MailCountyCode, 
	MailCountyName, 
	MailStateAbbreviation, 
	MailCountryAbbreviation, 
	MailLatitude, 
	MailLongitude, 
	LegalPrimaryAgencyCode, 
	LicensedIndicator, 
	AbbreviatedName, 
	AssignedStateCode, 
	SalesDivisionDimID AS SalesDivisionDimID3, 
	ClosedDate
	FROM RTR_InsertUpdateOrExpire_Insert
),
TGT_AgencyDim_InsertNew AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyDim
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, AgencyDimHashKey, EDWAgencyPKID, EDWAgencyAddressPhysicalPKID, EDWAgencyAddressMailingPKID, EDWAgencyAKID, EDWAgencyAddressPhysicalAKID, EDWAgencyAddressMailingAKID, AgencyCode, AgencyLegalName, AgencyDoingBusinessAsName, AgencyPrimaryPhoneNumber, AgencyPrimaryFaxNumber, AgencyPrimaryEmailAddress, AgencyStatusCode, AgencyStatusDescription, AgencyAppointedDate, AgencyTerminatedDate, AgencyCustomerCareStatus, PrimaryAgencyIndicator, PhysicalAddressLine1, PhysicalAddressLine2, PhysicalAddressLine3, PhysicalCity, PhysicalZipCode, PhysicalCountyCode, PhysicalCountyName, PhysicalStateAbbreviation, PhysicalCountryAbbreviation, PhysicalLatitude, PhysicalLongitude, MailingAddressLine1, MailingAddressLine2, MailingAddressLine3, MailingCity, MailingZipCode, MailingCountyCode, MailingCountyName, MailingStateAbbreviation, MailingCountryAbbreviation, MailingLatitude, MailingLongitude, LegalPrimaryAgencyCode, LicensedIndicator, AbbreviatedName, AssignedStateCode, SalesDivisionDimId, AgencyClosedDate)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	HashKey AS AGENCYDIMHASHKEY, 
	AgencyPKID AS EDWAGENCYPKID, 
	PhysicalAgencyAddressPKID AS EDWAGENCYADDRESSPHYSICALPKID, 
	MailAgencyAddressPKID AS EDWAGENCYADDRESSMAILINGPKID, 
	AgencyAKID AS EDWAGENCYAKID, 
	PhysicalAgencyAddressAKID AS EDWAGENCYADDRESSPHYSICALAKID, 
	MailAgencyAddressAKID AS EDWAGENCYADDRESSMAILINGAKID, 
	AGENCYCODE, 
	LegalName AS AGENCYLEGALNAME, 
	DoingBusinessAsName AS AGENCYDOINGBUSINESSASNAME, 
	PrimaryPhoneNumber AS AGENCYPRIMARYPHONENUMBER, 
	PrimaryFaxNumber AS AGENCYPRIMARYFAXNUMBER, 
	PrimaryEmailAddress AS AGENCYPRIMARYEMAILADDRESS, 
	StatusCode AS AGENCYSTATUSCODE, 
	StatusDescription AS AGENCYSTATUSDESCRIPTION, 
	AppointedDate AS AGENCYAPPOINTEDDATE, 
	TerminatedDate AS AGENCYTERMINATEDDATE, 
	CustomerCareStatus AS AGENCYCUSTOMERCARESTATUS, 
	PRIMARYAGENCYINDICATOR, 
	PHYSICALADDRESSLINE1, 
	PHYSICALADDRESSLINE2, 
	PHYSICALADDRESSLINE3, 
	PHYSICALCITY, 
	PHYSICALZIPCODE, 
	PHYSICALCOUNTYCODE, 
	PHYSICALCOUNTYNAME, 
	PHYSICALSTATEABBREVIATION, 
	PHYSICALCOUNTRYABBREVIATION, 
	PHYSICALLATITUDE, 
	PHYSICALLONGITUDE, 
	MailAddressLine1 AS MAILINGADDRESSLINE1, 
	MailAddressLine2 AS MAILINGADDRESSLINE2, 
	MailAddressLine3 AS MAILINGADDRESSLINE3, 
	MailCity AS MAILINGCITY, 
	MailZipCode AS MAILINGZIPCODE, 
	MailCountyCode AS MAILINGCOUNTYCODE, 
	MailCountyName AS MAILINGCOUNTYNAME, 
	MailStateAbbreviation AS MAILINGSTATEABBREVIATION, 
	MailCountryAbbreviation AS MAILINGCOUNTRYABBREVIATION, 
	MailLatitude AS MAILINGLATITUDE, 
	MailLongitude AS MAILINGLONGITUDE, 
	LEGALPRIMARYAGENCYCODE, 
	LICENSEDINDICATOR, 
	ABBREVIATEDNAME, 
	ASSIGNEDSTATECODE, 
	SalesDivisionDimID3 AS SALESDIVISIONDIMID, 
	ClosedDate AS AGENCYCLOSEDDATE
	FROM UPD_Inserts
),
UPD_Updates AS (
	SELECT
	AgencyDimId_Existing AS AgencyDimId, 
	AuditID, 
	ModifiedDate, 
	AgencyPKID, 
	PhysicalAgencyAddressPKID, 
	MailAgencyAddressPKID, 
	DoingBusinessAsName, 
	PrimaryPhoneNumber, 
	PrimaryFaxNumber, 
	PrimaryEmailAddress, 
	StatusDescription, 
	CustomerCareStatus, 
	PhysicalAddressLine1, 
	PhysicalAddressLine2, 
	PhysicalAddressLine3, 
	PhysicalCity, 
	PhysicalZipCode, 
	PhysicalCountyCode, 
	PhysicalCountyName, 
	PhysicalStateAbbreviation, 
	PhysicalCountryAbbreviation, 
	PhysicalLatitude, 
	PhysicalLongitude, 
	MailAddressLine1, 
	MailAddressLine2, 
	MailAddressLine3, 
	MailCity, 
	MailZipCode, 
	MailCountyCode, 
	MailCountyName, 
	MailStateAbbreviation, 
	MailCountryAbbreviation, 
	MailLatitude, 
	MailLongitude, 
	SalesDivisionDimID AS SalesDivisionDimID4, 
	ClosedDate
	FROM RTR_InsertUpdateOrExpire_Update
),
TGT_AgencyDim_UpdateExisting AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyDim AS T
	USING UPD_Updates AS S
	ON T.AgencyDimID = S.AgencyDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditID = S.AuditID, T.ModifiedDate = S.ModifiedDate, T.EDWAgencyPKID = S.AgencyPKID, T.EDWAgencyAddressPhysicalPKID = S.PhysicalAgencyAddressPKID, T.EDWAgencyAddressMailingPKID = S.MailAgencyAddressPKID, T.AgencyDoingBusinessAsName = S.DoingBusinessAsName, T.AgencyPrimaryPhoneNumber = S.PrimaryPhoneNumber, T.AgencyPrimaryFaxNumber = S.PrimaryFaxNumber, T.AgencyPrimaryEmailAddress = S.PrimaryEmailAddress, T.AgencyStatusDescription = S.StatusDescription, T.AgencyCustomerCareStatus = S.CustomerCareStatus, T.PhysicalAddressLine1 = S.PhysicalAddressLine1, T.PhysicalAddressLine2 = S.PhysicalAddressLine2, T.PhysicalAddressLine3 = S.PhysicalAddressLine3, T.PhysicalCity = S.PhysicalCity, T.PhysicalZipCode = S.PhysicalZipCode, T.PhysicalCountyCode = S.PhysicalCountyCode, T.PhysicalCountyName = S.PhysicalCountyName, T.PhysicalStateAbbreviation = S.PhysicalStateAbbreviation, T.PhysicalCountryAbbreviation = S.PhysicalCountryAbbreviation, T.PhysicalLatitude = S.PhysicalLatitude, T.PhysicalLongitude = S.PhysicalLongitude, T.MailingAddressLine1 = S.MailAddressLine1, T.MailingAddressLine2 = S.MailAddressLine2, T.MailingAddressLine3 = S.MailAddressLine3, T.MailingCity = S.MailCity, T.MailingZipCode = S.MailZipCode, T.MailingCountyCode = S.MailCountyCode, T.MailingCountyName = S.MailCountyName, T.MailingStateAbbreviation = S.MailStateAbbreviation, T.MailingCountryAbbreviation = S.MailCountryAbbreviation, T.MailingLatitude = S.MailLatitude, T.MailingLongitude = S.MailLongitude, T.AgencyClosedDate = S.ClosedDate
),
UPD_ExpireOld AS (
	SELECT
	AgencyDimId_Existing AS AgencyDimId, 
	ExpireSnapshotFlag AS ExpireCurrentSnapshotFlag, 
	ExpirationDate_ForExpire AS ExpirationDate, 
	ModifiedDate
	FROM RTR_InsertUpdateOrExpire_Expire
),
TGT_AgencyDim_ExpireOld AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyDim AS T
	USING UPD_ExpireOld AS S
	ON T.AgencyDimID = S.AgencyDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.ExpireCurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),
SQ_Agency_SalesDivisionDim AS (
	SELECT
		AgencyAKID,
		SalesTerritoryAKID,
		RegionalSalesManagerAKID
	FROM Agency_V2
	WHERE CurrentSnapshotFlag=1
),
LKP_RegionalSalesManager AS (
	SELECT
	SalesDirectorAKID,
	RegionalSalesManagerAKID
	FROM (
		SELECT 
			SalesDirectorAKID,
			RegionalSalesManagerAKID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.RegionalSalesManager
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RegionalSalesManagerAKID ORDER BY SalesDirectorAKID) = 1
),
LKP_SalesDivisionDim AS (
	SELECT
	SalesDivisionDimID,
	EDWRegionalSalesManagerAKID,
	EDWSalesDirectorAKID,
	EDWSalesTerritoryAKID
	FROM (
		SELECT 
			SalesDivisionDimID,
			EDWRegionalSalesManagerAKID,
			EDWSalesDirectorAKID,
			EDWSalesTerritoryAKID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SalesDivisionDim
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWRegionalSalesManagerAKID,EDWSalesDirectorAKID,EDWSalesTerritoryAKID ORDER BY SalesDivisionDimID) = 1
),
UPD_AgencyDim_SalesDivisionDimId AS (
	SELECT
	SQ_Agency_SalesDivisionDim.AgencyAKID AS EDWAgencyAKID, 
	LKP_SalesDivisionDim.SalesDivisionDimID AS SalesDivisionDimId
	FROM SQ_Agency_SalesDivisionDim
	LEFT JOIN LKP_SalesDivisionDim
	ON LKP_SalesDivisionDim.EDWRegionalSalesManagerAKID = SQ_Agency_SalesDivisionDim.RegionalSalesManagerAKID AND LKP_SalesDivisionDim.EDWSalesDirectorAKID = LKP_RegionalSalesManager.SalesDirectorAKID AND LKP_SalesDivisionDim.EDWSalesTerritoryAKID = SQ_Agency_SalesDivisionDim.SalesTerritoryAKID
),
AgencyDim_UpdateHistory AS (
	UPDATE @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyDim SET SalesDivisionDimId = S.SalesDivisionDimId 
	WHERE EDWAgencyAKID = S.EDWAgencyAKID
	FROM UPD_AgencyDim_SalesDivisionDimId S
),
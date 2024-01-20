WITH
SQ_AgencyODSStage AS (
	SELECT
		ModifiedUserID,
		ModifiedDate,
		AgencyID,
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
		SourceSystemID,
		CustomerCareStatus,
		FederalTaxID,
		ProfitSharingGuaranteeFlag,
		LicensedIndicator,
		AbbreviatedName,
		AssignedStateCode,
		ClosedDate
	FROM AgencyODSStage
),
LKP_RSMRelationship AS (
	SELECT
	in_AgencyCode,
	AgencyCode,
	AgencyID,
	AssociateID,
	WestBendAssociateID
	FROM (
		SELECT 
			in_AgencyCode,
			AgencyCode,
			AgencyID,
			AssociateID,
			WestBendAssociateID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.RegionalSalesManagerRelationshipStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyCode ORDER BY in_AgencyCode) = 1
),
LKP_SalesTerritoryRelationship AS (
	SELECT
	in_AgencyCode,
	AgencyCode,
	AgencyID,
	SalesTerritoryID,
	SalesTerritoryCode
	FROM (
		SELECT 
			in_AgencyCode,
			AgencyCode,
			AgencyID,
			SalesTerritoryID,
			SalesTerritoryCode
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SalesTerritoryRelationshipStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyCode ORDER BY in_AgencyCode) = 1
),
EXP_CleanupData AS (
	SELECT
	SQ_AgencyODSStage.AgencyCode,
	SQ_AgencyODSStage.LegalName,
	-- *INF*: IIF(IsNull(LegalName), 'N/A', LegalName)
	IFF(LegalName IS NULL, 'N/A', LegalName) AS o_LegalName,
	SQ_AgencyODSStage.DoingBusinessAsName,
	-- *INF*: IIF(IsNull(DoingBusinessAsName), 'N/A', DoingBusinessAsName)
	IFF(DoingBusinessAsName IS NULL, 'N/A', DoingBusinessAsName) AS o_DoingBusinessAsName,
	SQ_AgencyODSStage.PrimaryPhoneNumber,
	-- *INF*: IIF(IsNull(PrimaryPhoneNumber), '000-000-0000', PrimaryPhoneNumber)
	IFF(PrimaryPhoneNumber IS NULL, '000-000-0000', PrimaryPhoneNumber) AS o_PrimaryPhoneNumber,
	SQ_AgencyODSStage.PrimaryFaxNumber,
	-- *INF*: IIF(IsNull(PrimaryFaxNumber), '000-000-0000', PrimaryFaxNumber)
	IFF(PrimaryFaxNumber IS NULL, '000-000-0000', PrimaryFaxNumber) AS o_PrimaryFaxNumber,
	SQ_AgencyODSStage.PrimaryEmailAddress,
	-- *INF*: IIF(IsNull(PrimaryEmailAddress), 'N/A', PrimaryEmailAddress)
	IFF(PrimaryEmailAddress IS NULL, 'N/A', PrimaryEmailAddress) AS o_PrimaryEmailAddress,
	SQ_AgencyODSStage.StatusCode,
	-- *INF*: IIF(IsNull(StatusCode), '?', StatusCode)
	IFF(StatusCode IS NULL, '?', StatusCode) AS o_StatusCode,
	SQ_AgencyODSStage.StatusDescription,
	-- *INF*: IIF(IsNull(StatusDescription), 'N/A', StatusDescription)
	IFF(StatusDescription IS NULL, 'N/A', StatusDescription) AS o_StatusDescription,
	SQ_AgencyODSStage.AppointedDate,
	-- *INF*: IIF(IsNull(AppointedDate), to_date('1900-01-01', 'YYYY-MM-DD'), AppointedDate)
	IFF(AppointedDate IS NULL, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD'), AppointedDate) AS o_AppointedDate,
	SQ_AgencyODSStage.TerminatedDate,
	-- *INF*: IIF(IsNull(TerminatedDate), to_date('2999-12-31', 'YYYY-MM-DD'), TerminatedDate)
	IFF(TerminatedDate IS NULL, TO_TIMESTAMP('2999-12-31', 'YYYY-MM-DD'), TerminatedDate) AS o_TerminatedDate,
	SQ_AgencyODSStage.SourceSystemID,
	SQ_AgencyODSStage.CustomerCareStatus,
	-- *INF*: IIF(IsNull(CustomerCareStatus), 'N/A', CustomerCareStatus)
	IFF(CustomerCareStatus IS NULL, 'N/A', CustomerCareStatus) AS o_CustomerCareStatus,
	SQ_AgencyODSStage.FederalTaxID,
	-- *INF*: IIF(IsNull(FederalTaxID), 'N/A', FederalTaxID)
	IFF(FederalTaxID IS NULL, 'N/A', FederalTaxID) AS o_FederalTaxID,
	LKP_SalesTerritoryRelationship.SalesTerritoryCode AS lkp_SalesTerritoryCode,
	LKP_RSMRelationship.WestBendAssociateID AS lkp_WestBendAssociateID,
	SQ_AgencyODSStage.ProfitSharingGuaranteeFlag,
	SQ_AgencyODSStage.LicensedIndicator,
	SQ_AgencyODSStage.AbbreviatedName,
	SQ_AgencyODSStage.AssignedStateCode,
	SQ_AgencyODSStage.ClosedDate
	FROM SQ_AgencyODSStage
	LEFT JOIN LKP_RSMRelationship
	ON LKP_RSMRelationship.AgencyCode = SQ_AgencyODSStage.AgencyCode
	LEFT JOIN LKP_SalesTerritoryRelationship
	ON LKP_SalesTerritoryRelationship.AgencyCode = SQ_AgencyODSStage.AgencyCode
),
LKP_RegionalSalesManager AS (
	SELECT
	in_WestBendAssociateID,
	RegionalSalesManagerAKID,
	WestBendAssociateID
	FROM (
		SELECT 
			in_WestBendAssociateID,
			RegionalSalesManagerAKID,
			WestBendAssociateID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RegionalSalesManager
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WestBendAssociateID ORDER BY in_WestBendAssociateID) = 1
),
LKP_SalesTerritoryAKID AS (
	SELECT
	in_SalesTerritoryCode,
	SalesTerritoryAKID,
	SalesTerritoryCode
	FROM (
		SELECT 
			in_SalesTerritoryCode,
			SalesTerritoryAKID,
			SalesTerritoryCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SalesTerritory
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SalesTerritoryCode ORDER BY in_SalesTerritoryCode) = 1
),
EXP_GetAKIDs AS (
	SELECT
	EXP_CleanupData.AgencyCode,
	EXP_CleanupData.o_LegalName AS LegalName,
	EXP_CleanupData.o_DoingBusinessAsName AS DoingBusinessAsName,
	EXP_CleanupData.o_PrimaryPhoneNumber AS PrimaryPhoneNumber,
	EXP_CleanupData.o_PrimaryFaxNumber AS PrimaryFaxNumber,
	EXP_CleanupData.o_PrimaryEmailAddress AS PrimaryEmailAddress,
	EXP_CleanupData.o_StatusCode AS StatusCode,
	EXP_CleanupData.o_StatusDescription AS StatusDescription,
	EXP_CleanupData.o_AppointedDate AS AppointedDate,
	EXP_CleanupData.o_TerminatedDate AS TerminatedDate,
	EXP_CleanupData.SourceSystemID,
	EXP_CleanupData.o_CustomerCareStatus AS CustomerCareStatus,
	EXP_CleanupData.o_FederalTaxID AS FederalTaxID,
	LKP_SalesTerritoryAKID.SalesTerritoryAKID AS lkp_SalesTerritoryAKID,
	-- *INF*: IIF(IsNull(lkp_SalesTerritoryAKID), -1, lkp_SalesTerritoryAKID)
	IFF(lkp_SalesTerritoryAKID IS NULL, - 1, lkp_SalesTerritoryAKID) AS o_SalesTerritoryAKID,
	LKP_RegionalSalesManager.RegionalSalesManagerAKID AS lkp_RegionalSalesManagerAKID,
	-- *INF*: IIF(IsNull(lkp_RegionalSalesManagerAKID), -1, lkp_RegionalSalesManagerAKID)
	IFF(lkp_RegionalSalesManagerAKID IS NULL, - 1, lkp_RegionalSalesManagerAKID) AS o_RegionalSalesManagerAKID,
	EXP_CleanupData.ProfitSharingGuaranteeFlag AS in_ProfitSharingGuaranteeFlag,
	-- *INF*: IIF(ISNULL(in_ProfitSharingGuaranteeFlag),'0',in_ProfitSharingGuaranteeFlag)
	IFF(in_ProfitSharingGuaranteeFlag IS NULL, '0', in_ProfitSharingGuaranteeFlag) AS out_ProfitSharingGuaranteeFlag,
	EXP_CleanupData.LicensedIndicator AS in_LicensedIndicator,
	-- *INF*: IIF(ISNULL(in_LicensedIndicator),'0',in_LicensedIndicator)
	IFF(in_LicensedIndicator IS NULL, '0', in_LicensedIndicator) AS out_LicensedIndicator,
	EXP_CleanupData.AbbreviatedName AS in_AbbreviatedName,
	-- *INF*: IIF(ISNULL(in_AbbreviatedName),'N/A',in_AbbreviatedName)
	IFF(in_AbbreviatedName IS NULL, 'N/A', in_AbbreviatedName) AS out_AbbreviatedName,
	EXP_CleanupData.AssignedStateCode AS in_AssignedStateCode,
	-- *INF*: IIF(ISNULL(in_AssignedStateCode),'N/A',in_AssignedStateCode)
	IFF(in_AssignedStateCode IS NULL, 'N/A', in_AssignedStateCode) AS out_AssignedStateCode,
	EXP_CleanupData.ClosedDate
	FROM EXP_CleanupData
	LEFT JOIN LKP_RegionalSalesManager
	ON LKP_RegionalSalesManager.WestBendAssociateID = EXP_CleanupData.lkp_WestBendAssociateID
	LEFT JOIN LKP_SalesTerritoryAKID
	ON LKP_SalesTerritoryAKID.SalesTerritoryCode = EXP_CleanupData.lkp_SalesTerritoryCode
),
LKP_ExistingAgency AS (
	SELECT
	HashKey,
	AgencyAKID,
	ClosedDate,
	AgencyCode
	FROM (
		SELECT 
			HashKey,
			AgencyAKID,
			ClosedDate,
			AgencyCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Agency
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyCode ORDER BY HashKey) = 1
),
EXP_Detect_Changes AS (
	SELECT
	EXP_GetAKIDs.AgencyCode,
	EXP_GetAKIDs.LegalName,
	EXP_GetAKIDs.DoingBusinessAsName,
	EXP_GetAKIDs.PrimaryPhoneNumber,
	EXP_GetAKIDs.PrimaryFaxNumber,
	EXP_GetAKIDs.PrimaryEmailAddress,
	EXP_GetAKIDs.StatusCode,
	EXP_GetAKIDs.StatusDescription,
	EXP_GetAKIDs.AppointedDate,
	EXP_GetAKIDs.TerminatedDate,
	EXP_GetAKIDs.CustomerCareStatus,
	LKP_ExistingAgency.HashKey AS lkp_HashKey,
	LKP_ExistingAgency.AgencyAKID AS lkp_AgencyAKID,
	LKP_ExistingAgency.ClosedDate AS lkp_ClosedDate,
	EXP_GetAKIDs.o_SalesTerritoryAKID AS SalesTerritoryAKID,
	EXP_GetAKIDs.o_RegionalSalesManagerAKID AS RegionalSalesManagerAKID,
	EXP_GetAKIDs.FederalTaxID,
	EXP_GetAKIDs.out_ProfitSharingGuaranteeFlag AS ProfitSharingGuaranteeFlag,
	EXP_GetAKIDs.out_LicensedIndicator AS LicensedIndicator,
	EXP_GetAKIDs.out_AbbreviatedName AS AbbreviatedName,
	EXP_GetAKIDs.out_AssignedStateCode AS AssignedStateCode,
	-- *INF*: MD5(to_char(SalesTerritoryAKID) || '&' ||  to_char(RegionalSalesManagerAKID) || '&' ||  LegalName || '&' ||  DoingBusinessAsName || '&' ||  StatusCode || '&' ||  StatusDescription || '&' ||  to_char(AppointedDate) || '&' ||  to_char(TerminatedDate) || '&' ||  CustomerCareStatus || '&' ||  PrimaryPhoneNumber || '&' ||  PrimaryFaxNumber || '&' ||  PrimaryEmailAddress || '&' ||  FederalTaxID || '&' || to_char(ProfitSharingGuaranteeFlag) || '&' ||  to_char(LicensedIndicator) || '&' ||   AbbreviatedName || '&' ||   to_char(AssignedStateCode))
	-- 
	MD5(to_char(SalesTerritoryAKID) || '&' || to_char(RegionalSalesManagerAKID) || '&' || LegalName || '&' || DoingBusinessAsName || '&' || StatusCode || '&' || StatusDescription || '&' || to_char(AppointedDate) || '&' || to_char(TerminatedDate) || '&' || CustomerCareStatus || '&' || PrimaryPhoneNumber || '&' || PrimaryFaxNumber || '&' || PrimaryEmailAddress || '&' || FederalTaxID || '&' || to_char(ProfitSharingGuaranteeFlag) || '&' || to_char(LicensedIndicator) || '&' || AbbreviatedName || '&' || to_char(AssignedStateCode)) AS v_NewHashKey,
	EXP_GetAKIDs.ClosedDate,
	v_NewHashKey AS o_NewHashKey,
	-- *INF*: IIF(IsNull(lkp_AgencyAKID), 'NEW', 
	-- IIF((lkp_HashKey <> v_NewHashKey or lkp_ClosedDate<>ClosedDate), 'UPDATE' ,
	-- 'NOCHANGE'))
	IFF(
	    lkp_AgencyAKID IS NULL, 'NEW',
	    IFF(
	        (lkp_HashKey <> v_NewHashKey
	    or lkp_ClosedDate <> ClosedDate), 'UPDATE', 'NOCHANGE'
	    )
	) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: iif(v_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(
	    v_changed_flag = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS EffectiveFromDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS EffectiveToDate,
	EXP_GetAKIDs.SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate
	FROM EXP_GetAKIDs
	LEFT JOIN LKP_ExistingAgency
	ON LKP_ExistingAgency.AgencyCode = EXP_GetAKIDs.AgencyCode
),
FIL_insert AS (
	SELECT
	lkp_AgencyAKID, 
	changed_flag AS ChangedFlag, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveFromDate, 
	EffectiveToDate, 
	SourceSystemID, 
	CreatedDate, 
	ModifiedDate, 
	o_NewHashKey AS HashKey, 
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
	CustomerCareStatus AS CustomerCareStatus1, 
	FederalTaxID, 
	ProfitSharingGuaranteeFlag, 
	LicensedIndicator, 
	AbbreviatedName, 
	AssignedStateCode, 
	ClosedDate
	FROM EXP_Detect_Changes
	WHERE ChangedFlag='NEW'or ChangedFlag='UPDATE'
),
SEQ_V2Agency_AKID AS (
	CREATE SEQUENCE SEQ_V2Agency_AKID
	START = 0
	INCREMENT = 1;
),
EXP_Assign_AKID AS (
	SELECT
	CurrentSnapshotFlag,
	AuditID,
	EffectiveFromDate AS EffectiveDate,
	EffectiveToDate AS ExpirationDate,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	HashKey,
	lkp_AgencyAKID,
	SEQ_V2Agency_AKID.NEXTVAL,
	-- *INF*: iif(isnull(lkp_AgencyAKID),NEXTVAL,lkp_AgencyAKID)
	IFF(lkp_AgencyAKID IS NULL, NEXTVAL, lkp_AgencyAKID) AS o_AgencyAKID,
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
	CustomerCareStatus1,
	FederalTaxID,
	ProfitSharingGuaranteeFlag,
	LicensedIndicator,
	AbbreviatedName,
	AssignedStateCode,
	ClosedDate
	FROM FIL_insert
),
Agency_InsertNew AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Agency
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, HashKey, AgencyAKID, SalesTerritoryAKID, RegionalSalesManagerAKID, AgencyCode, LegalName, DoingBusinessAsName, PrimaryPhoneNumber, PrimaryFaxNumber, PrimaryEmailAddress, StatusCode, StatusDescription, AppointedDate, TerminatedDate, CustomerCareStatus, FederalTaxId, ProfitSharingGuaranteeFlag, LicensedIndicator, AbbreviatedName, AssignedStateCode, ClosedDate)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	HASHKEY, 
	o_AgencyAKID AS AGENCYAKID, 
	SALESTERRITORYAKID, 
	REGIONALSALESMANAGERAKID, 
	AGENCYCODE, 
	LEGALNAME, 
	DOINGBUSINESSASNAME, 
	PRIMARYPHONENUMBER, 
	PRIMARYFAXNUMBER, 
	PRIMARYEMAILADDRESS, 
	STATUSCODE, 
	STATUSDESCRIPTION, 
	APPOINTEDDATE, 
	TERMINATEDDATE, 
	CustomerCareStatus1 AS CUSTOMERCARESTATUS, 
	FederalTaxID AS FEDERALTAXID, 
	PROFITSHARINGGUARANTEEFLAG, 
	LICENSEDINDICATOR, 
	ABBREVIATEDNAME, 
	ASSIGNEDSTATECODE, 
	CLOSEDDATE
	FROM EXP_Assign_AKID
),
SQ_Agency_V2 AS (
	SELECT 
		a.AgencyID, 
		a.EffectiveDate,
		a.ExpirationDate, 
		a.AgencyAKID
	FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Agency a
	WHERE  a.AgencyAKID  IN
		( SELECT AgencyAKID  FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Agency
		WHERE CurrentSnapshotFlag = 1 GROUP BY AgencyAKID HAVING count(*) > 1) 
	ORDER BY a.AgencyAKID, a.EffectiveDate DESC
	
	
	
	--IN Subquery exists to pick AK ID column values that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag 
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
),
EXP_Lag_eff_from_date AS (
	SELECT
	AgencyID,
	EffectiveDate AS EffectiveFromDate,
	ExpirationDate AS OriginalEffectiveToDate,
	AgencyAKID,
	-- *INF*: DECODE(TRUE,
	-- AgencyAKID = v_prev_AKID , ADD_TO_DATE(v_prev_EffectiveFromDate,'SS',-1),
	-- OriginalEffectiveToDate)
	DECODE(
	    TRUE,
	    AgencyAKID = v_prev_AKID, DATEADD(SECOND,- 1,v_prev_EffectiveFromDate),
	    OriginalEffectiveToDate
	) AS v_EffectiveToDate,
	v_EffectiveToDate AS o_EffectiveToDate,
	AgencyAKID AS v_prev_AKID,
	EffectiveFromDate AS v_prev_EffectiveFromDate,
	0 AS CurrentSnapshotFlag,
	SYSDATE AS ModifiedDate
	FROM SQ_Agency_V2
),
FIL_FirstRowInAKGroup AS (
	SELECT
	AgencyID, 
	OriginalEffectiveToDate, 
	o_EffectiveToDate AS NewEffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM EXP_Lag_eff_from_date
	WHERE OriginalEffectiveToDate != NewEffectiveToDate
),
UPD_OldRecord AS (
	SELECT
	AgencyID, 
	NewEffectiveToDate AS EffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM FIL_FirstRowInAKGroup
),
Agency_Expire AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Agency AS T
	USING UPD_OldRecord AS S
	ON T.AgencyID = S.AgencyID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.EffectiveToDate, T.ModifiedDate = S.ModifiedDate
),
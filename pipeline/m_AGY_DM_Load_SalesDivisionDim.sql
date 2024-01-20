WITH
SQ_JoinSalesDivisionSources AS (
	SELECT DISTINCT a.SalesTerritoryAKID, 
	a.RegionalSalesManagerAKID,
	rsm.RegionalSalesManagerID, 
	rsm.RegionalSalesManagerAKID, 
	rsm.SalesDirectorAKID, 
	rsm.WestBendAssociateID, 
	rsm.RegionalSalesManagerCode, 
	rsm.DisplayName, 
	rsm.LastName, 
	rsm.FirstName, 
	rsm.MiddleName, 
	rsm.Suffix, 
	rsm.EmailAddress, 
	st.SalesTerritoryID,
	st.SalesTerritoryAKID,
	st.SalesTerritoryCode,
	st.SalesTerritoryCodeDescription
	FROM
	 	@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Agency a
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RegionalSalesManager rsm
		ON a.RegionalSalesManagerAKID = rsm.RegionalSalesManagerAKID
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.SalesTerritory st
		ON a.SalesTerritoryAKID = st.SalesTerritoryAKID
	WHERE a.CurrentSnapshotFlag=1 
	AND rsm.CurrentSnapshotFlag = 1
	AND st.CurrentSnapshotFlag = 1
	AND (rsm.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}'  or st.ModifiedDate >= '@{pipeline().parameters.SELECTION_START_TS}' or a.ModifiedDate>= '@{pipeline().parameters.SELECTION_START_TS}')
),
EXP_CollectData AS (
	SELECT
	a_SalesTerritoryAKID,
	a_RegionalSalesManagerAKID,
	rsm_RegionalSalesManagerID,
	rsm_RegionalSalesManagerAKID,
	rsm_SalesDirectorAKID,
	rsm_WestBendAssociateID,
	rsm_RegionalSalesManagerCode,
	rsm_DisplayName,
	rsm_LastName,
	rsm_FirstName,
	rsm_MiddleName,
	rsm_Suffix,
	rsm_EmailAddress,
	st_SalesTerritoryID,
	st_SalesTerritoryAKID,
	st_SalesTerritoryCode,
	st_SalesTerritoryCodeDescription
	FROM SQ_JoinSalesDivisionSources
),
LKP_SalesDirector AS (
	SELECT
	SalesDirectorID,
	WestBendAssociateID,
	SalesDirectorCode,
	DisplayName,
	LastName,
	FirstName,
	MiddleName,
	Suffix,
	EmailAddress,
	SalesDirectorAKID
	FROM (
		SELECT 
			SalesDirectorID,
			WestBendAssociateID,
			SalesDirectorCode,
			DisplayName,
			LastName,
			FirstName,
			MiddleName,
			Suffix,
			EmailAddress,
			SalesDirectorAKID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SalesDirector
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SalesDirectorAKID ORDER BY SalesDirectorID) = 1
),
EXP_GetCleanup AS (
	SELECT
	LKP_SalesDirector.SalesDirectorID AS in_SalesDirectorID,
	LKP_SalesDirector.WestBendAssociateID AS in_WestBendAssociateID,
	LKP_SalesDirector.SalesDirectorCode AS in_SalesDirectorCode,
	LKP_SalesDirector.DisplayName AS in_DisplayName,
	LKP_SalesDirector.LastName AS in_LastName,
	LKP_SalesDirector.FirstName AS in_FirstName,
	LKP_SalesDirector.MiddleName AS in_MiddleName,
	LKP_SalesDirector.Suffix AS in_Suffix,
	LKP_SalesDirector.EmailAddress AS in_EmailAddress,
	EXP_CollectData.rsm_RegionalSalesManagerID AS in_RegionalSalesManagerID,
	EXP_CollectData.rsm_RegionalSalesManagerAKID AS in_RegionalSalesManagerAKID,
	EXP_CollectData.rsm_SalesDirectorAKID AS in_SalesDirectorAKID,
	EXP_CollectData.rsm_WestBendAssociateID AS in_RegionalSalesManagerWestBendAssociateID,
	EXP_CollectData.rsm_RegionalSalesManagerCode AS in_RegionalSalesManagerCode,
	EXP_CollectData.rsm_DisplayName AS in_RegionalSalesManagerDisplayName,
	EXP_CollectData.rsm_LastName AS in_RegionalSalesManagerLastName,
	EXP_CollectData.rsm_FirstName AS in_RegionalSalesManagerFirstName,
	EXP_CollectData.rsm_MiddleName AS in_RegionalSalesManagerMiddleName,
	EXP_CollectData.rsm_Suffix AS in_RegionalSalesManagerSuffix,
	EXP_CollectData.rsm_EmailAddress AS in_RegionalSalesManagerEmailAddress,
	EXP_CollectData.st_SalesTerritoryID AS in_SalesTerritoryID,
	EXP_CollectData.st_SalesTerritoryAKID AS in_SalesTerritoryAKID,
	EXP_CollectData.st_SalesTerritoryCode AS in_SalesTerritoryCode,
	EXP_CollectData.st_SalesTerritoryCodeDescription AS in_SalesTerritoryCodeDescription,
	in_RegionalSalesManagerID AS o_EDWRegionalSalesManagerPKID,
	-- *INF*: IIF(ISNULL(in_SalesDirectorID),-1,in_SalesDirectorID)
	IFF(in_SalesDirectorID IS NULL, - 1, in_SalesDirectorID) AS o_EDWSalesDirectorPKID,
	-- *INF*: IIF(ISNULL(in_SalesTerritoryID),-1,in_SalesTerritoryID)
	IFF(in_SalesTerritoryID IS NULL, - 1, in_SalesTerritoryID) AS o_EDWSalesTerritoryPKID,
	in_RegionalSalesManagerAKID AS o_EDWRegionalSalesManagerAKID,
	in_SalesDirectorAKID AS o_EDWSalesDirectorAKID,
	-- *INF*: IIF(ISNULL(in_SalesTerritoryAKID),-1,in_SalesTerritoryAKID)
	IFF(in_SalesTerritoryAKID IS NULL, - 1, in_SalesTerritoryAKID) AS o_EDWSalesTerritoryAKID,
	in_RegionalSalesManagerWestBendAssociateID AS o_RegionalSalesManagerWestBendAssociateID,
	in_RegionalSalesManagerCode AS o_RegionalSalesManagerCode,
	in_RegionalSalesManagerDisplayName AS o_RegionalSalesManagerDisplayName,
	in_RegionalSalesManagerLastName AS o_RegionalSalesManagerLastName,
	in_RegionalSalesManagerFirstName AS o_RegionalSalesManagerFirstName,
	in_RegionalSalesManagerMiddleName AS o_RegionalSalesManagerMiddleName,
	in_RegionalSalesManagerSuffix AS o_RegionalSalesManagerSuffix,
	in_RegionalSalesManagerEmailAddress AS o_RegionalSalesManagerEmailAddress,
	-- *INF*: IIF(ISNULL(in_WestBendAssociateID),'N/A',in_WestBendAssociateID)
	IFF(in_WestBendAssociateID IS NULL, 'N/A', in_WestBendAssociateID) AS o_SalesDirectorWestBendAssociateID,
	-- *INF*: IIF(ISNULL(in_SalesDirectorCode),'N/A',in_SalesDirectorCode)
	IFF(in_SalesDirectorCode IS NULL, 'N/A', in_SalesDirectorCode) AS o_SalesDirectorCode,
	-- *INF*: IIF(ISNULL(in_DisplayName),'N/A',in_DisplayName)
	IFF(in_DisplayName IS NULL, 'N/A', in_DisplayName) AS o_SalesDirectorDisplayName,
	-- *INF*: IIF(ISNULL(in_LastName),'N/A',in_LastName)
	IFF(in_LastName IS NULL, 'N/A', in_LastName) AS o_SalesDirectorLastName,
	-- *INF*: IIF(ISNULL(in_FirstName),'N/A',in_FirstName)
	IFF(in_FirstName IS NULL, 'N/A', in_FirstName) AS o_SalesDirectorFirstName,
	-- *INF*: IIF(ISNULL(in_MiddleName),'N/A',in_MiddleName)
	IFF(in_MiddleName IS NULL, 'N/A', in_MiddleName) AS o_SalesDirectorMiddleName,
	-- *INF*: IIF(ISNULL(in_Suffix),'N/A',in_Suffix)
	IFF(in_Suffix IS NULL, 'N/A', in_Suffix) AS o_SalesDirectorSuffix,
	-- *INF*: IIF(ISNULL(in_EmailAddress),'N/A',in_EmailAddress)
	IFF(in_EmailAddress IS NULL, 'N/A', in_EmailAddress) AS o_SalesDirectorEmailAddress,
	-- *INF*: IIF(ISNULL(in_SalesTerritoryCode),'N/A',in_SalesTerritoryCode)
	IFF(in_SalesTerritoryCode IS NULL, 'N/A', in_SalesTerritoryCode) AS o_SalesTerritoryCode,
	-- *INF*: IIF(ISNULL(in_SalesTerritoryCodeDescription),'N/A',in_SalesTerritoryCodeDescription)
	IFF(in_SalesTerritoryCodeDescription IS NULL, 'N/A', in_SalesTerritoryCodeDescription) AS o_SalesTerritoryCodeDescription
	FROM EXP_CollectData
	LEFT JOIN LKP_SalesDirector
	ON LKP_SalesDirector.SalesDirectorAKID = EXP_CollectData.rsm_SalesDirectorAKID
),
LKP_Existing AS (
	SELECT
	SalesDivisionDimHashKey,
	EDWRegionalSalesManagerPKID,
	EDWSalesDirectorPKID,
	EDWSalesTerritoryPKID,
	SalesDivisionDimID,
	EDWSalesTerritoryAKID,
	EDWRegionalSalesManagerAKID
	FROM (
		SELECT 
			SalesDivisionDimHashKey,
			EDWRegionalSalesManagerPKID,
			EDWSalesDirectorPKID,
			EDWSalesTerritoryPKID,
			SalesDivisionDimID,
			EDWSalesTerritoryAKID,
			EDWRegionalSalesManagerAKID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SalesDivisionDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWSalesTerritoryAKID,EDWRegionalSalesManagerAKID ORDER BY SalesDivisionDimHashKey) = 1
),
EXP_CheckForChange AS (
	SELECT
	LKP_Existing.SalesDivisionDimHashKey AS in_SalesDivisionDimHashKey,
	LKP_Existing.EDWRegionalSalesManagerPKID AS in_RegionalSalesManagerPKID,
	LKP_Existing.EDWSalesDirectorPKID AS in_SalesDirectorPKID,
	LKP_Existing.EDWSalesTerritoryPKID AS in_SalesTerritoryPKID,
	LKP_Existing.SalesDivisionDimID AS Existing_SalesDivisionDimId,
	EXP_GetCleanup.o_EDWRegionalSalesManagerPKID AS EDWRegionalSalesManagerPKID,
	EXP_GetCleanup.o_EDWSalesDirectorPKID AS EDWSalesDirectorPKID,
	EXP_GetCleanup.o_EDWSalesTerritoryPKID AS EDWSalesTerritoryPKID,
	EXP_GetCleanup.o_EDWRegionalSalesManagerAKID AS EDWRegionalSalesManagerAKID,
	EXP_GetCleanup.o_EDWSalesDirectorAKID AS EDWSalesDirectorAKID,
	EXP_GetCleanup.o_EDWSalesTerritoryAKID AS EDWSalesTerritoryAKID,
	EXP_GetCleanup.o_RegionalSalesManagerWestBendAssociateID AS RegionalSalesManagerWestBendAssociateID,
	EXP_GetCleanup.o_RegionalSalesManagerCode AS RegionalSalesManagerCode,
	EXP_GetCleanup.o_RegionalSalesManagerDisplayName AS RegionalSalesManagerDisplayName,
	EXP_GetCleanup.o_RegionalSalesManagerLastName AS RegionalSalesManagerLastName,
	EXP_GetCleanup.o_RegionalSalesManagerFirstName AS RegionalSalesManagerFirstName,
	EXP_GetCleanup.o_RegionalSalesManagerMiddleName AS RegionalSalesManagerMiddleName,
	EXP_GetCleanup.o_RegionalSalesManagerSuffix AS RegionalSalesManagerSuffix,
	EXP_GetCleanup.o_RegionalSalesManagerEmailAddress AS RegionalSalesManagerEmailAddress,
	EXP_GetCleanup.o_SalesDirectorWestBendAssociateID AS SalesDirectorWestBendAssociateID,
	EXP_GetCleanup.o_SalesDirectorCode AS SalesDirectorCode,
	EXP_GetCleanup.o_SalesDirectorDisplayName AS SalesDirectorDisplayName,
	EXP_GetCleanup.o_SalesDirectorLastName AS SalesDirectorLastName,
	EXP_GetCleanup.o_SalesDirectorFirstName AS SalesDirectorFirstName,
	EXP_GetCleanup.o_SalesDirectorMiddleName AS SalesDirectorMiddleName,
	EXP_GetCleanup.o_SalesDirectorSuffix AS SalesDirectorSuffix,
	EXP_GetCleanup.o_SalesDirectorEmailAddress AS SalesDirectorEmailAddress,
	EXP_GetCleanup.o_SalesTerritoryCode AS SalesTerritoryCode,
	EXP_GetCleanup.o_SalesTerritoryCodeDescription AS SalesTerritoryCodeDescription,
	-- *INF*: MD5(RegionalSalesManagerCode || '&' ||RegionalSalesManagerWestBendAssociateID || '&' || SalesTerritoryCode || '&' || SalesDirectorCode || '&' || SalesDirectorWestBendAssociateID)
	-- 
	-- 
	-- 
	MD5(RegionalSalesManagerCode || '&' || RegionalSalesManagerWestBendAssociateID || '&' || SalesTerritoryCode || '&' || SalesDirectorCode || '&' || SalesDirectorWestBendAssociateID) AS v_NewHashKey,
	-- *INF*: Decode(true,
	-- in_SalesTerritoryPKID <> EDWSalesTerritoryPKID, 'Y',
	-- in_RegionalSalesManagerPKID <> EDWRegionalSalesManagerPKID, 'Y',
	-- in_SalesDirectorPKID <> EDWSalesDirectorPKID, 'Y',
	-- 'N')
	-- 
	-- 
	Decode(
	    true,
	    in_SalesTerritoryPKID <> EDWSalesTerritoryPKID, 'Y',
	    in_RegionalSalesManagerPKID <> EDWRegionalSalesManagerPKID, 'Y',
	    in_SalesDirectorPKID <> EDWSalesDirectorPKID, 'Y',
	    'N'
	) AS v_ChangeToEDWRecord,
	-- *INF*: Decode(true,
	-- IsNull(in_SalesDivisionDimHashKey), 'Insert',
	-- in_SalesDivisionDimHashKey = v_NewHashKey and v_ChangeToEDWRecord = 'N', 'Ignore',
	-- in_SalesDivisionDimHashKey <> v_NewHashKey or  v_ChangeToEDWRecord = 'Y', 'Update',
	-- 'Ignore')
	-- 
	-- -- If the existing record is not found based on the AKID, it's always an insert
	-- -- If there are no changes, we ignore the record
	-- -- If one of the type 2 attributes changed, we expire the old record (also inserts a new record, see router)
	-- -- If there was no change to the type 2 attributes AND there was a change to the PKID in the EDW then we update the record.  Important to have the logic comparing the hash keys, otherwise we might attempt to update records where we are already expiring and inserting a new record.
	-- 	
	Decode(
	    true,
	    in_SalesDivisionDimHashKey IS NULL, 'Insert',
	    in_SalesDivisionDimHashKey = v_NewHashKey and v_ChangeToEDWRecord = 'N', 'Ignore',
	    in_SalesDivisionDimHashKey <> v_NewHashKey or v_ChangeToEDWRecord = 'Y', 'Update',
	    'Ignore'
	) AS v_InsertUpdateOrIgnore,
	1 AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('1800-01-01 01:00:00','YYYY-MM-DD HH24:MI:SS')
	-- 
	-- --Decode(v_InsertUpdateOrIgnore,
	-- --'Insert',TO_DATE( '1800-01-01 01:00:00','YYYY-MM-DD HH24:MI:SS'),
	-- --SYSDATE)
	-- 
	-- --Decode(v_InsertUpdateExpireOrIgnore,
	-- --'Insert', trunc(sysdate, 'DD'),
	-- --in_ExistingEffectiveDate)
	TO_TIMESTAMP('1800-01-01 01:00:00', 'YYYY-MM-DD HH24:MI:SS') AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
	-- 
	-- --Decode(v_InsertUpdateExpireOrIgnore,
	-- --'Expire', add_to_date(trunc(sysdate, 'DD'), 'MS', -1 ),
	-- --to_date('2099-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'))
	TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') AS o_ExpirationDate,
	SYSDATE AS o_CurrentDate,
	SYSDATE AS o_ModifiedDate,
	v_NewHashKey AS o_SalesDivisionDimHashKey,
	v_InsertUpdateOrIgnore AS o_InsertUpdateOrIgnore
	FROM EXP_GetCleanup
	LEFT JOIN LKP_Existing
	ON LKP_Existing.EDWSalesTerritoryAKID = EXP_GetCleanup.o_EDWSalesTerritoryAKID AND LKP_Existing.EDWRegionalSalesManagerAKID = EXP_GetCleanup.o_EDWRegionalSalesManagerAKID
),
RTR_InsertUpdate AS (
	SELECT
	Existing_SalesDivisionDimId,
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	o_AuditID AS AuditID,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_CurrentDate AS CurrentDate,
	o_ModifiedDate AS ModifiedDate,
	o_SalesDivisionDimHashKey AS SalesDivisionDimHashKey,
	EDWRegionalSalesManagerPKID,
	EDWSalesDirectorPKID,
	EDWSalesTerritoryPKID,
	EDWRegionalSalesManagerAKID,
	EDWSalesDirectorAKID,
	EDWSalesTerritoryAKID,
	RegionalSalesManagerWestBendAssociateID,
	RegionalSalesManagerCode,
	RegionalSalesManagerDisplayName,
	RegionalSalesManagerLastName,
	RegionalSalesManagerFirstName,
	RegionalSalesManagerMiddleName,
	RegionalSalesManagerSuffix,
	RegionalSalesManagerEmailAddress,
	SalesDirectorWestBendAssociateID,
	SalesDirectorCode,
	SalesDirectorDisplayName,
	SalesDirectorLastName,
	SalesDirectorFirstName,
	SalesDirectorMiddleName,
	SalesDirectorSuffix,
	SalesDirectorEmailAddress,
	SalesTerritoryCode,
	SalesTerritoryCodeDescription,
	o_InsertUpdateOrIgnore AS InsertUpdateIgnore
	FROM EXP_CheckForChange
),
RTR_InsertUpdate_InsertNew AS (SELECT * FROM RTR_InsertUpdate WHERE InsertUpdateIgnore = 'Insert'),
RTR_InsertUpdate_UpdateExisting AS (SELECT * FROM RTR_InsertUpdate WHERE InsertUpdateIgnore = 'Update'),
UPD_InsertNew AS (
	SELECT
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	CurrentDate AS CreatedDate, 
	ModifiedDate, 
	SalesDivisionDimHashKey, 
	EDWRegionalSalesManagerPKID, 
	EDWSalesDirectorPKID, 
	EDWSalesTerritoryPKID, 
	EDWRegionalSalesManagerAKID, 
	EDWSalesDirectorAKID, 
	EDWSalesTerritoryAKID, 
	RegionalSalesManagerWestBendAssociateID, 
	RegionalSalesManagerCode, 
	RegionalSalesManagerDisplayName, 
	RegionalSalesManagerLastName, 
	RegionalSalesManagerFirstName, 
	RegionalSalesManagerMiddleName, 
	RegionalSalesManagerSuffix, 
	RegionalSalesManagerEmailAddress, 
	SalesDirectorWestBendAssociateID, 
	SalesDirectorCode, 
	SalesDirectorDisplayName, 
	SalesDirectorLastName, 
	SalesDirectorFirstName, 
	SalesDirectorMiddleName, 
	SalesDirectorSuffix, 
	SalesDirectorEmailAddress, 
	SalesTerritoryCode, 
	SalesTerritoryCodeDescription
	FROM RTR_InsertUpdate_InsertNew
),
TGT_SalesDivisionDim_Inserts AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SalesDivisionDim
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, SalesDivisionDimHashKey, EDWRegionalSalesManagerPKID, EDWSalesDirectorPKID, EDWSalesTerritoryPKID, EDWRegionalSalesManagerAKID, EDWSalesDirectorAKID, EDWSalesTerritoryAKID, RegionalSalesManagerWestBendAssociateID, RegionalSalesManagerCode, RegionalSalesManagerDisplayName, RegionalSalesManagerLastName, RegionalSalesManagerFirstName, RegionalSalesManagerMiddleName, RegionalSalesManagerSuffix, RegionalSalesManagerEmailAddress, SalesDirectorWestBendAssociateID, SalesDirectorCode, SalesDirectorDisplayName, SalesDirectorLastName, SalesDirectorFirstName, SalesDirectorMiddleName, SalesDirectorSuffix, SalesDirectorEmailAddress, SalesTerritoryCode, SalesTerritoryCodeDescription)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	SALESDIVISIONDIMHASHKEY, 
	EDWREGIONALSALESMANAGERPKID, 
	EDWSALESDIRECTORPKID, 
	EDWSALESTERRITORYPKID, 
	EDWREGIONALSALESMANAGERAKID, 
	EDWSALESDIRECTORAKID, 
	EDWSALESTERRITORYAKID, 
	REGIONALSALESMANAGERWESTBENDASSOCIATEID, 
	REGIONALSALESMANAGERCODE, 
	REGIONALSALESMANAGERDISPLAYNAME, 
	REGIONALSALESMANAGERLASTNAME, 
	REGIONALSALESMANAGERFIRSTNAME, 
	REGIONALSALESMANAGERMIDDLENAME, 
	REGIONALSALESMANAGERSUFFIX, 
	REGIONALSALESMANAGEREMAILADDRESS, 
	SALESDIRECTORWESTBENDASSOCIATEID, 
	SALESDIRECTORCODE, 
	SALESDIRECTORDISPLAYNAME, 
	SALESDIRECTORLASTNAME, 
	SALESDIRECTORFIRSTNAME, 
	SALESDIRECTORMIDDLENAME, 
	SALESDIRECTORSUFFIX, 
	SALESDIRECTOREMAILADDRESS, 
	SALESTERRITORYCODE, 
	SALESTERRITORYCODEDESCRIPTION
	FROM UPD_InsertNew
),
UPD_UpdateExisting AS (
	SELECT
	Existing_SalesDivisionDimId AS SalesDivisionDimId, 
	ModifiedDate, 
	SalesDivisionDimHashKey, 
	EDWRegionalSalesManagerPKID, 
	EDWSalesDirectorPKID, 
	EDWSalesTerritoryPKID, 
	EDWRegionalSalesManagerAKID, 
	EDWSalesDirectorAKID, 
	EDWSalesTerritoryAKID, 
	RegionalSalesManagerWestBendAssociateID, 
	RegionalSalesManagerCode, 
	RegionalSalesManagerDisplayName, 
	RegionalSalesManagerLastName, 
	RegionalSalesManagerFirstName, 
	RegionalSalesManagerMiddleName, 
	RegionalSalesManagerSuffix, 
	RegionalSalesManagerEmailAddress, 
	SalesDirectorWestBendAssociateID, 
	SalesDirectorCode, 
	SalesDirectorDisplayName, 
	SalesDirectorLastName, 
	SalesDirectorFirstName, 
	SalesDirectorMiddleName, 
	SalesDirectorSuffix, 
	SalesDirectorEmailAddress, 
	SalesTerritoryCode, 
	SalesTerritoryCodeDescription
	FROM RTR_InsertUpdate_UpdateExisting
),
TGT_SalesDivisionDim_Updates AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SalesDivisionDim AS T
	USING UPD_UpdateExisting AS S
	ON T.SalesDivisionDimID = S.SalesDivisionDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.SalesDivisionDimHashKey = S.SalesDivisionDimHashKey, T.EDWRegionalSalesManagerPKID = S.EDWRegionalSalesManagerPKID, T.EDWSalesDirectorPKID = S.EDWSalesDirectorPKID, T.EDWSalesTerritoryPKID = S.EDWSalesTerritoryPKID, T.EDWRegionalSalesManagerAKID = S.EDWRegionalSalesManagerAKID, T.EDWSalesDirectorAKID = S.EDWSalesDirectorAKID, T.EDWSalesTerritoryAKID = S.EDWSalesTerritoryAKID, T.RegionalSalesManagerWestBendAssociateID = S.RegionalSalesManagerWestBendAssociateID, T.RegionalSalesManagerCode = S.RegionalSalesManagerCode, T.RegionalSalesManagerDisplayName = S.RegionalSalesManagerDisplayName, T.RegionalSalesManagerLastName = S.RegionalSalesManagerLastName, T.RegionalSalesManagerFirstName = S.RegionalSalesManagerFirstName, T.RegionalSalesManagerMiddleName = S.RegionalSalesManagerMiddleName, T.RegionalSalesManagerSuffix = S.RegionalSalesManagerSuffix, T.RegionalSalesManagerEmailAddress = S.RegionalSalesManagerEmailAddress, T.SalesDirectorWestBendAssociateID = S.SalesDirectorWestBendAssociateID, T.SalesDirectorCode = S.SalesDirectorCode, T.SalesDirectorDisplayName = S.SalesDirectorDisplayName, T.SalesDirectorLastName = S.SalesDirectorLastName, T.SalesDirectorFirstName = S.SalesDirectorFirstName, T.SalesDirectorMiddleName = S.SalesDirectorMiddleName, T.SalesDirectorSuffix = S.SalesDirectorSuffix, T.SalesDirectorEmailAddress = S.SalesDirectorEmailAddress, T.SalesTerritoryCode = S.SalesTerritoryCode, T.SalesTerritoryCodeDescription = S.SalesTerritoryCodeDescription
),
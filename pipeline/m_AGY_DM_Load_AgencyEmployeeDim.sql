WITH
SQ_AgencyEmployee AS (
	SELECT
		AgencyEmployeeID,
		AgencyEmployeeAKID,
		AgencyAKID,
		AgencyEmployeeCode,
		AgencyEmployeeRole,
		ProducerCode,
		LastName,
		FirstName,
		MiddleName,
		Suffix,
		NickName,
		PhoneNumber,
		FaxNumber,
		EmailAddress,
		StatusCode,
		StatusCodeDescription,
		ListedDate,
		TerminatedDate,
		UserID
	FROM AgencyEmployee
	WHERE CurrentSnapshotFlag = 1 and ModifiedDate >='@{pipeline().parameters.SELECTION_START_TS}'
),
LKP_AgencyCode AS (
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
EXP_CollectData AS (
	SELECT
	LKP_AgencyCode.AgencyCode AS i_lkp_AgencyCode,
	-- *INF*: IIF(ISNULL(i_lkp_AgencyCode), 'N/A',i_lkp_AgencyCode)
	IFF(i_lkp_AgencyCode IS NULL, 'N/A', i_lkp_AgencyCode) AS o_lkp_AgencyCode,
	SQ_AgencyEmployee.AgencyEmployeeID AS AgencyEmployeePKID,
	SQ_AgencyEmployee.AgencyEmployeeAKID,
	SQ_AgencyEmployee.AgencyAKID,
	SQ_AgencyEmployee.AgencyEmployeeCode,
	SQ_AgencyEmployee.AgencyEmployeeRole,
	SQ_AgencyEmployee.ProducerCode,
	SQ_AgencyEmployee.LastName,
	SQ_AgencyEmployee.FirstName,
	SQ_AgencyEmployee.MiddleName,
	SQ_AgencyEmployee.Suffix,
	SQ_AgencyEmployee.NickName,
	SQ_AgencyEmployee.PhoneNumber,
	SQ_AgencyEmployee.FaxNumber,
	SQ_AgencyEmployee.EmailAddress,
	SQ_AgencyEmployee.StatusCode,
	SQ_AgencyEmployee.StatusCodeDescription,
	SQ_AgencyEmployee.ListedDate,
	SQ_AgencyEmployee.TerminatedDate,
	SQ_AgencyEmployee.UserID
	FROM SQ_AgencyEmployee
	LEFT JOIN LKP_AgencyCode
	ON LKP_AgencyCode.AgencyAKID = SQ_AgencyEmployee.AgencyAKID
),
LKP_Existing AS (
	SELECT
	in_AgencyEmployeeAKID,
	EDWAgencyEmployeeAKID,
	AgencyEmployeeDimID,
	AgencyEmployeeDimHashKey,
	EDWAgencyEmployeePKID
	FROM (
		SELECT 
			in_AgencyEmployeeAKID,
			EDWAgencyEmployeeAKID,
			AgencyEmployeeDimID,
			AgencyEmployeeDimHashKey,
			EDWAgencyEmployeePKID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyEmployeeDim
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWAgencyEmployeeAKID ORDER BY in_AgencyEmployeeAKID) = 1
),
EXP_CheckForChange AS (
	SELECT
	LKP_Existing.AgencyEmployeeDimID AS lkp_ExistingAgencyEmployeeDimID,
	LKP_Existing.AgencyEmployeeDimHashKey AS lkp_ExistingAgencyEmployeeDimHashKey,
	LKP_Existing.EDWAgencyEmployeePKID AS lkp_ExistingEDWAgencyEmployeePKID,
	EXP_CollectData.o_lkp_AgencyCode AS i_AgencyCode,
	EXP_CollectData.AgencyEmployeePKID AS i_AgencyEmployeePKID,
	EXP_CollectData.AgencyEmployeeAKID AS i_AgencyEmployeeAKID,
	EXP_CollectData.AgencyAKID AS i_AgencyAKID,
	EXP_CollectData.AgencyEmployeeCode AS i_AgencyEmployeeCode,
	EXP_CollectData.AgencyEmployeeRole AS i_AgencyEmployeeRole,
	EXP_CollectData.ProducerCode AS i_ProducerCode,
	EXP_CollectData.LastName AS i_LastName,
	EXP_CollectData.FirstName AS i_FirstName,
	EXP_CollectData.MiddleName AS i_MiddleName,
	EXP_CollectData.Suffix AS i_Suffix,
	EXP_CollectData.NickName AS i_NickName,
	EXP_CollectData.PhoneNumber AS i_PhoneNumber,
	EXP_CollectData.FaxNumber AS i_FaxNumber,
	EXP_CollectData.EmailAddress AS i_EmailAddress,
	EXP_CollectData.StatusCode AS i_StatusCode,
	EXP_CollectData.StatusCodeDescription AS i_StatusCodeDescription,
	EXP_CollectData.ListedDate AS i_ListedDate,
	EXP_CollectData.TerminatedDate AS i_TerminatedDate,
	EXP_CollectData.UserID AS i_UserID,
	-- *INF*: MD5(i_AgencyCode || '&' || i_AgencyEmployeeCode || '&' || i_AgencyEmployeeRole || '&' || i_ProducerCode || '&' || i_StatusCode || '&' || TO_CHAR(i_ListedDate) || '&' || TO_CHAR(i_TerminatedDate))
	MD5(i_AgencyCode || '&' || i_AgencyEmployeeCode || '&' || i_AgencyEmployeeRole || '&' || i_ProducerCode || '&' || i_StatusCode || '&' || TO_CHAR(i_ListedDate) || '&' || TO_CHAR(i_TerminatedDate)) AS v_new_Type2HashKey,
	-- *INF*: DECODE(true,
	-- i_AgencyEmployeePKID <> lkp_ExistingEDWAgencyEmployeePKID, 'Y',
	-- 'N')
	DECODE(true,
	i_AgencyEmployeePKID <> lkp_ExistingEDWAgencyEmployeePKID, 'Y',
	'N') AS v_ChangeToEDWRecord,
	-- *INF*: DECODE(true,
	-- ISNULL(lkp_ExistingAgencyEmployeeDimHashKey), 'Insert',
	-- (lkp_ExistingAgencyEmployeeDimHashKey = v_new_Type2HashKey) and (v_ChangeToEDWRecord = 'N'), 'Ignore',
	-- (lkp_ExistingAgencyEmployeeDimHashKey <> v_new_Type2HashKey), 'Expire',
	-- (lkp_ExistingAgencyEmployeeDimHashKey = v_new_Type2HashKey) and (v_ChangeToEDWRecord = 'Y'), 'Update',
	-- 'Ignore')
	-- 
	-- 
	-- -- If the existing record is not found based on the AKID, it's always an insert
	-- -- If there are no changes, we ignore the record
	-- -- If one of the type 2 attributes changed, we expire the old record (also inserts a new record, see router)
	-- -- If there was no change to the type 2 attributes AND there was a change to the PKID in the EDW then we update the record.  Important to have the logic comparing the hash keys, otherwise we might attempt to update records where we are already expiring and inserting a new record.
	-- 	
	DECODE(true,
	lkp_ExistingAgencyEmployeeDimHashKey IS NULL, 'Insert',
	( lkp_ExistingAgencyEmployeeDimHashKey = v_new_Type2HashKey ) AND ( v_ChangeToEDWRecord = 'N' ), 'Ignore',
	( lkp_ExistingAgencyEmployeeDimHashKey <> v_new_Type2HashKey ), 'Expire',
	( lkp_ExistingAgencyEmployeeDimHashKey = v_new_Type2HashKey ) AND ( v_ChangeToEDWRecord = 'Y' ), 'Update',
	'Ignore') AS v_InsertUpdateExpireOrIgnore,
	lkp_ExistingAgencyEmployeeDimID AS o_lkp_ExistingAgencyEmployeeDimID,
	1 AS o_CurrentSnapshotFlag,
	0 AS o_ExpireSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: DECODE(v_InsertUpdateExpireOrIgnore,
	-- 'Insert',   TO_DATE('1800-01-01 01:00:00.000', 'YYYY-MM-DD HH24:MI:SS.MS'),
	-- SYSDATE)
	DECODE(v_InsertUpdateExpireOrIgnore,
	'Insert', TO_DATE('1800-01-01 01:00:00.000', 'YYYY-MM-DD HH24:MI:SS.MS'),
	SYSDATE) AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.MS')
	TO_DATE('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.MS') AS o_ExpirationDate,
	-- *INF*: ADD_TO_DATE(SYSDATE, 'MS', -1)
	-- 
	ADD_TO_DATE(SYSDATE, 'MS', - 1) AS o_ExpirationDate_ForExpire,
	SYSDATE AS o_CurrentDate,
	v_new_Type2HashKey AS o_Type2HashKey,
	v_InsertUpdateExpireOrIgnore AS o_InsertUpdateExpireOrIgnore
	FROM EXP_CollectData
	LEFT JOIN LKP_Existing
	ON LKP_Existing.EDWAgencyEmployeeAKID = EXP_CollectData.AgencyEmployeeAKID
),
RTR_InsertUpdateOrExpire AS (
	SELECT
	o_lkp_ExistingAgencyEmployeeDimID AS ExistingAgencyEmployeeDimID,
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	o_ExpireSnapshotFlag AS ExpireSnapshotFlag,
	o_AuditID AS AuditID,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_ExpirationDate_ForExpire AS ExpirationDate_ForExpire,
	o_CurrentDate AS CurrentDate,
	o_Type2HashKey AS AgencyEmployeeDimHashKey,
	i_AgencyEmployeePKID AS EDWAgencyEmployeePKID,
	i_AgencyEmployeeAKID AS EDWAgencyEmployeeAKID,
	i_AgencyCode AS AgencyCode,
	i_AgencyEmployeeCode AS AgencyEmployeeCode,
	i_AgencyEmployeeRole AS AgencyEmployeeRole,
	i_ProducerCode AS ProducerCode,
	i_LastName AS AgencyEmployeeLastName,
	i_FirstName AS AgencyEmployeeFirstName,
	i_MiddleName AS AgencyEmployeeMiddleName,
	i_Suffix AS AgencyEmployeeSuffix,
	i_NickName AS AgencyEmployeeNickName,
	i_PhoneNumber AS AgencyEmployeePhoneNumber,
	i_FaxNumber AS AgencyEmployeeFaxNumber,
	i_EmailAddress AS AgencyEmployeeEmailAddress,
	i_StatusCode AS AgencyEmployeeStatusCode,
	i_StatusCodeDescription AS AgencyEmployeeStatusCodeDescription,
	i_ListedDate AS AgencyEmployeeListedDate,
	i_TerminatedDate AS AgencyEmployeeTerminatedDate,
	i_UserID AS UserID,
	o_InsertUpdateExpireOrIgnore AS InsertUpdateIgnoreOrExpire
	FROM EXP_CheckForChange
),
RTR_InsertUpdateOrExpire_Expire AS (SELECT * FROM RTR_InsertUpdateOrExpire WHERE InsertUpdateIgnoreOrExpire = 'Expire'),
RTR_InsertUpdateOrExpire_Insert AS (SELECT * FROM RTR_InsertUpdateOrExpire WHERE InsertUpdateIgnoreOrExpire = 'Insert' or InsertUpdateIgnoreOrExpire = 'Expire'),
RTR_InsertUpdateOrExpire_Update AS (SELECT * FROM RTR_InsertUpdateOrExpire WHERE InsertUpdateIgnoreOrExpire = 'Update'),
UPD_ExpireOld AS (
	SELECT
	ExistingAgencyEmployeeDimID AS AgencyEmployeeDimID, 
	ExpireSnapshotFlag AS CurrentSnapshotFlag, 
	ExpirationDate_ForExpire AS ExpirationDate, 
	CurrentDate AS ModifiedDate
	FROM RTR_InsertUpdateOrExpire_Expire
),
TGT_AgencyEmployeeDim_Expire AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyEmployeeDim AS T
	USING UPD_ExpireOld AS S
	ON T.AgencyEmployeeDimID = S.AgencyEmployeeDimID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),
UPD_InsertNew AS (
	SELECT
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	CurrentDate AS CreatedDate, 
	CurrentDate AS ModifiedDate, 
	AgencyEmployeeDimHashKey, 
	EDWAgencyEmployeePKID, 
	EDWAgencyEmployeeAKID, 
	AgencyCode, 
	AgencyEmployeeCode, 
	AgencyEmployeeRole, 
	ProducerCode, 
	AgencyEmployeeLastName, 
	AgencyEmployeeFirstName, 
	AgencyEmployeeMiddleName, 
	AgencyEmployeeSuffix, 
	AgencyEmployeeNickName, 
	AgencyEmployeePhoneNumber, 
	AgencyEmployeeFaxNumber, 
	AgencyEmployeeEmailAddress, 
	AgencyEmployeeStatusCode, 
	AgencyEmployeeStatusCodeDescription, 
	AgencyEmployeeListedDate, 
	AgencyEmployeeTerminatedDate, 
	UserID
	FROM RTR_InsertUpdateOrExpire_Insert
),
TGT_AgencyEmployeeDim_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyEmployeeDim
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, AgencyEmployeeDimHashKey, EDWAgencyEmployeePKID, EDWAgencyEmployeeAKID, AgencyCode, AgencyEmployeeCode, AgencyEmployeeRole, ProducerCode, AgencyEmployeeLastName, AgencyEmployeeFirstName, AgencyEmployeeMiddleName, AgencyEmployeeSuffix, AgencyEmployeeNickName, AgencyEmployeePhoneNumber, AgencyEmployeeFaxNumber, AgencyEmployeeEmailAddress, AgencyEmployeeStatusCode, AgencyEmployeeStatusCodeDescription, AgencyEmployeeListedDate, AgencyEmployeeTerminatedDate, AgencyEmployeeUserID)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	AGENCYEMPLOYEEDIMHASHKEY, 
	EDWAGENCYEMPLOYEEPKID, 
	EDWAGENCYEMPLOYEEAKID, 
	AGENCYCODE, 
	AGENCYEMPLOYEECODE, 
	AGENCYEMPLOYEEROLE, 
	PRODUCERCODE, 
	AGENCYEMPLOYEELASTNAME, 
	AGENCYEMPLOYEEFIRSTNAME, 
	AGENCYEMPLOYEEMIDDLENAME, 
	AGENCYEMPLOYEESUFFIX, 
	AGENCYEMPLOYEENICKNAME, 
	AGENCYEMPLOYEEPHONENUMBER, 
	AGENCYEMPLOYEEFAXNUMBER, 
	AGENCYEMPLOYEEEMAILADDRESS, 
	AGENCYEMPLOYEESTATUSCODE, 
	AGENCYEMPLOYEESTATUSCODEDESCRIPTION, 
	AGENCYEMPLOYEELISTEDDATE, 
	AGENCYEMPLOYEETERMINATEDDATE, 
	UserID AS AGENCYEMPLOYEEUSERID
	FROM UPD_InsertNew
),
UPD_Updates AS (
	SELECT
	ExistingAgencyEmployeeDimID AS AgencyEmployeeDimID, 
	CurrentDate AS ModifiedDate, 
	EDWAgencyEmployeePKID, 
	AgencyEmployeeLastName, 
	AgencyEmployeeFirstName, 
	AgencyEmployeeMiddleName, 
	AgencyEmployeeSuffix, 
	AgencyEmployeeNickName, 
	AgencyEmployeePhoneNumber, 
	AgencyEmployeeFaxNumber, 
	AgencyEmployeeEmailAddress, 
	AgencyEmployeeStatusCodeDescription, 
	UserID
	FROM RTR_InsertUpdateOrExpire_Update
),
TGT_AgencyEmployeeDim_Updates AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyEmployeeDim AS T
	USING UPD_Updates AS S
	ON T.AgencyEmployeeDimID = S.AgencyEmployeeDimID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.EDWAgencyEmployeePKID = S.EDWAgencyEmployeePKID, T.AgencyEmployeeLastName = S.AgencyEmployeeLastName, T.AgencyEmployeeFirstName = S.AgencyEmployeeFirstName, T.AgencyEmployeeMiddleName = S.AgencyEmployeeMiddleName, T.AgencyEmployeeSuffix = S.AgencyEmployeeSuffix, T.AgencyEmployeeNickName = S.AgencyEmployeeNickName, T.AgencyEmployeePhoneNumber = S.AgencyEmployeePhoneNumber, T.AgencyEmployeeFaxNumber = S.AgencyEmployeeFaxNumber, T.AgencyEmployeeEmailAddress = S.AgencyEmployeeEmailAddress, T.AgencyEmployeeStatusCodeDescription = S.AgencyEmployeeStatusCodeDescription, T.AgencyEmployeeUserID = S.UserID
),
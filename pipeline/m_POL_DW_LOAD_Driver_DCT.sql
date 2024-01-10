WITH
SQ_DCDriverStaging AS (
	SELECT 
	DCCADriverStaging.SessionId, 
	DCCADriverStaging.DriversLicenseNumber, 
	DCCADriverStaging.StateLicensed, 
	WBCADriverStaging.DateOfBirth, 
	WBCADriverStaging.Name, 
	WBCADriverStaging.MiddleInitial, 
	WBCADriverStaging.LastName, 
	WBCADriverStaging.Gender, 
	WBCADriverStaging.ExcludeDriver, 
	WBCADriverStaging.MVRStatus, 
	DCPolicyStaging.Id, 
	DCPolicyStaging.PolicyNumber,
	WBPolicyStaging.PolicyVersion 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.DCPolicyStaging
	inner join
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCADriverStaging
	on
	DCPolicyStaging.SessionId = DCCADriverStaging.SessionID
	and
	DCPolicyStaging.Status<>'Quote'
	and
	LEN(DCPolicyStaging.PolicyNumber)=7
	inner join
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.DCTransactionStaging
	on
	DCPolicyStaging.SessionId = DCTransactionStaging.SessionId
	and
	DCTransactionStaging.State='Committed'
	inner join
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCADriverStaging
	on
	DCPolicyStaging.SessionId = WBCADriverStaging.SessionID
	and
	DCCADriverStaging.CA_DriverID = WBCADriverStaging.CA_DriverID
	inner join
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.WBPolicyStaging
	on
	DCPolicyStaging.SessionId = WBPolicyStaging.SessionID
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Source AS (
	SELECT
	SessionId,
	DriversLicenseNumber,
	StateLicensed,
	DateOfBirth,
	Name,
	MiddleInitial,
	LastName,
	Gender,
	ExcludeDriver,
	MVRStatus,
	Id,
	PolicyNumber,
	PolicyVersion
	FROM SQ_DCDriverStaging
),
AGG_Remove_Duplicates AS (
	SELECT
	SessionId AS i_SessionId,
	Id AS i_Id,
	DriversLicenseNumber AS i_DriversLicenseNumber,
	StateLicensed AS i_StateLicensed,
	PolicyVersion AS i_PolicyVersion,
	DateOfBirth,
	Name,
	MiddleInitial,
	LastName,
	Gender,
	ExcludeDriver,
	MVRStatus,
	i_SessionId AS o_SessionId,
	-- *INF*: IIF(ISNULL(i_Id) or IS_SPACES(i_Id) or LENGTH(i_Id)=0, 'N/A', LTRIM(RTRIM(i_Id)))
	IFF(i_Id IS NULL OR IS_SPACES(i_Id) OR LENGTH(i_Id) = 0, 'N/A', LTRIM(RTRIM(i_Id))) AS o_Id,
	-- *INF*: IIF(ISNULL(i_DriversLicenseNumber) or IS_SPACES(i_DriversLicenseNumber) or LENGTH(i_DriversLicenseNumber)=0, 'N/A', LTRIM(RTRIM(i_DriversLicenseNumber)))
	IFF(i_DriversLicenseNumber IS NULL OR IS_SPACES(i_DriversLicenseNumber) OR LENGTH(i_DriversLicenseNumber) = 0, 'N/A', LTRIM(RTRIM(i_DriversLicenseNumber))) AS o_DriversLicenseNumber,
	-- *INF*: IIF(ISNULL(i_StateLicensed) or IS_SPACES(i_StateLicensed) or LENGTH(i_StateLicensed)=0, 'N/A', LTRIM(RTRIM(i_StateLicensed)))
	IFF(i_StateLicensed IS NULL OR IS_SPACES(i_StateLicensed) OR LENGTH(i_StateLicensed) = 0, 'N/A', LTRIM(RTRIM(i_StateLicensed))) AS o_LicenseState,
	PolicyNumber,
	-- *INF*: IIF(ISNULL(i_PolicyVersion), '00', LPAD(TO_CHAR(i_PolicyVersion),2,'0'))
	IFF(i_PolicyVersion IS NULL, '00', LPAD(TO_CHAR(i_PolicyVersion), 2, '0')) AS o_PolicyVersion
	FROM EXP_Source
	GROUP BY o_Id, o_DriversLicenseNumber, o_LicenseState, PolicyNumber, o_PolicyVersion
),
EXP_PostAggregator AS (
	SELECT
	o_SessionId AS i_SessionId,
	o_DriversLicenseNumber AS DriversLicenseNumber,
	o_LicenseState AS LicenseState,
	DateOfBirth,
	Name,
	MiddleInitial,
	LastName,
	Gender,
	ExcludeDriver,
	MVRStatus,
	o_Id AS i_Id,
	PolicyNumber AS i_PolicyNumber,
	o_PolicyVersion AS i_PolicyVersion,
	i_PolicyNumber||i_PolicyVersion AS o_PolicyKey
	FROM AGG_Remove_Duplicates
),
LKP_policy AS (
	SELECT
	pol_ak_id,
	pol_key
	FROM (
		SELECT 
			pol_ak_id,
			pol_key
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy
		WHERE crrnt_snpsht_flag='1' and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_ak_id) = 1
),
EXP_Pre_Target_Lookup AS (
	SELECT
	LKP_policy.pol_ak_id AS i_PolicyAKID,
	-- *INF*: IIF(ISNULL(i_PolicyAKID), -1, i_PolicyAKID)
	IFF(i_PolicyAKID IS NULL, - 1, i_PolicyAKID) AS v_PolicyAKID,
	v_PolicyAKID AS o_PolicyAKID,
	EXP_PostAggregator.LicenseState,
	EXP_PostAggregator.DriversLicenseNumber,
	EXP_PostAggregator.LastName,
	EXP_PostAggregator.Name AS FirstName,
	EXP_PostAggregator.MiddleInitial,
	EXP_PostAggregator.Gender,
	EXP_PostAggregator.ExcludeDriver,
	EXP_PostAggregator.MVRStatus,
	-- *INF*: IIF(ISNULL(LastName) or IS_SPACES(LastName)  or LENGTH(LastName)=0,'N/A',LTRIM(RTRIM(LastName)))
	IFF(LastName IS NULL OR IS_SPACES(LastName) OR LENGTH(LastName) = 0, 'N/A', LTRIM(RTRIM(LastName))) AS o_LastName,
	-- *INF*: IIF(ISNULL(FirstName) or IS_SPACES(FirstName)  or LENGTH(FirstName)=0,'N/A',LTRIM(RTRIM(FirstName)))
	IFF(FirstName IS NULL OR IS_SPACES(FirstName) OR LENGTH(FirstName) = 0, 'N/A', LTRIM(RTRIM(FirstName))) AS o_FirstName,
	-- *INF*: IIF(ISNULL(MiddleInitial) or IS_SPACES(MiddleInitial)  or LENGTH(MiddleInitial)=0,'N/A',LTRIM(RTRIM(MiddleInitial)))
	IFF(MiddleInitial IS NULL OR IS_SPACES(MiddleInitial) OR LENGTH(MiddleInitial) = 0, 'N/A', LTRIM(RTRIM(MiddleInitial))) AS o_MiddleInitial,
	-- *INF*: DECODE
	-- (LTRIM(RTRIM(Gender)),
	-- 'Male','M',
	-- 'Female','F',
	-- 'U')
	DECODE(LTRIM(RTRIM(Gender)),
		'Male', 'M',
		'Female', 'F',
		'U') AS o_Gender,
	-- *INF*: IIF(ISNULL(ExcludeDriver) or IS_SPACES(ExcludeDriver)  or LENGTH(ExcludeDriver)=0,'N/A',LTRIM(RTRIM(ExcludeDriver)))
	IFF(ExcludeDriver IS NULL OR IS_SPACES(ExcludeDriver) OR LENGTH(ExcludeDriver) = 0, 'N/A', LTRIM(RTRIM(ExcludeDriver))) AS o_ExcludeDriver,
	-- *INF*: IIF(ISNULL(MVRStatus) or IS_SPACES(MVRStatus)  or LENGTH(MVRStatus)=0,'N/A',LTRIM(RTRIM(MVRStatus)))
	IFF(MVRStatus IS NULL OR IS_SPACES(MVRStatus) OR LENGTH(MVRStatus) = 0, 'N/A', LTRIM(RTRIM(MVRStatus))) AS o_MVRStatus,
	EXP_PostAggregator.DateOfBirth
	FROM EXP_PostAggregator
	LEFT JOIN LKP_policy
	ON LKP_policy.pol_key = EXP_PostAggregator.o_PolicyKey
),
LKP_Driver AS (
	SELECT
	DriverId,
	DriverAKId,
	PolicyAKId,
	LicenseState,
	LicenseNumber,
	LastName,
	FirstName,
	MiddleName,
	GenderCode,
	Birthdate,
	ExcludeDriver,
	MVRStatus
	FROM (
		SELECT 
			DriverId,
			DriverAKId,
			PolicyAKId,
			LicenseState,
			LicenseNumber,
			LastName,
			FirstName,
			MiddleName,
			GenderCode,
			Birthdate,
			ExcludeDriver,
			MVRStatus
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.Driver
		WHERE CurrentSnapshotFlag = 1 and SourcesystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKId,LicenseState,LicenseNumber ORDER BY DriverId DESC) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_Driver.DriverAKId AS lkp_DriverAKId,
	LKP_Driver.PolicyAKId AS lkp_PolicyAKId,
	LKP_Driver.LicenseState AS lkp_LicenseState,
	LKP_Driver.LicenseNumber AS lkp_LicenseNumber,
	LKP_Driver.LastName AS lkp_LastName,
	LKP_Driver.FirstName AS lkp_FirstName,
	LKP_Driver.MiddleName AS lkp_MiddleName,
	LKP_Driver.GenderCode AS lkp_GenderCode,
	LKP_Driver.Birthdate AS lkp_Birthdate,
	LKP_Driver.ExcludeDriver AS lkp_ExcludeDriver,
	LKP_Driver.MVRStatus AS lkp_MVRStatus,
	EXP_Pre_Target_Lookup.o_PolicyAKID AS i_PolicyAKID,
	EXP_Pre_Target_Lookup.LicenseState AS i_LicenseState,
	EXP_Pre_Target_Lookup.DriversLicenseNumber AS i_LicenseNumber,
	EXP_Pre_Target_Lookup.o_LastName AS i_LastName,
	EXP_Pre_Target_Lookup.o_FirstName AS i_FirstName,
	EXP_Pre_Target_Lookup.o_MiddleInitial AS i_MiddleInitial,
	EXP_Pre_Target_Lookup.o_Gender AS i_Gender,
	EXP_Pre_Target_Lookup.DateOfBirth AS i_DateOfBirth,
	EXP_Pre_Target_Lookup.o_ExcludeDriver AS i_ExcludeDriver,
	EXP_Pre_Target_Lookup.o_MVRStatus AS i_MVRStatus,
	-- *INF*: IIF(ISNULL(i_PolicyAKID), -1, i_PolicyAKID)
	IFF(i_PolicyAKID IS NULL, - 1, i_PolicyAKID) AS v_pol_ak_id,
	v_pol_ak_id AS o_PolicyAKID,
	-- *INF*: IIF(ISNULL(lkp_DriverAKId), 'NEW', 
	-- IIF(
	-- LTRIM(RTRIM(lkp_LastName)) != LTRIM(RTRIM(i_LastName)) OR
	-- LTRIM(RTRIM(lkp_FirstName)) != LTRIM(RTRIM(i_FirstName)) OR 
	-- LTRIM(RTRIM(lkp_MiddleName)) != LTRIM(RTRIM(i_MiddleInitial)) OR lkp_Birthdate !=i_DateOfBirth OR LTRIM(RTRIM(lkp_GenderCode)) != LTRIM(RTRIM(i_Gender)) OR 
	-- LTRIM(RTRIM(lkp_ExcludeDriver)) != LTRIM(RTRIM(i_ExcludeDriver)) OR
	-- LTRIM(RTRIM(lkp_MVRStatus)) != LTRIM(RTRIM(i_MVRStatus)),
	-- 'UPDATE', 'NOCHANGE'))
	IFF(lkp_DriverAKId IS NULL, 'NEW', IFF(LTRIM(RTRIM(lkp_LastName)) != LTRIM(RTRIM(i_LastName)) OR LTRIM(RTRIM(lkp_FirstName)) != LTRIM(RTRIM(i_FirstName)) OR LTRIM(RTRIM(lkp_MiddleName)) != LTRIM(RTRIM(i_MiddleInitial)) OR lkp_Birthdate != i_DateOfBirth OR LTRIM(RTRIM(lkp_GenderCode)) != LTRIM(RTRIM(i_Gender)) OR LTRIM(RTRIM(lkp_ExcludeDriver)) != LTRIM(RTRIM(i_ExcludeDriver)) OR LTRIM(RTRIM(lkp_MVRStatus)) != LTRIM(RTRIM(i_MVRStatus)), 'UPDATE', 'NOCHANGE')) AS v_Changed_Flag,
	v_Changed_Flag AS o_Changed_Flag
	FROM EXP_Pre_Target_Lookup
	LEFT JOIN LKP_Driver
	ON LKP_Driver.PolicyAKId = EXP_Pre_Target_Lookup.o_PolicyAKID AND LKP_Driver.LicenseState = EXP_Pre_Target_Lookup.LicenseState AND LKP_Driver.LicenseNumber = EXP_Pre_Target_Lookup.DriversLicenseNumber
),
FIL_Insert AS (
	SELECT
	o_Changed_Flag AS Changed_Flag, 
	lkp_DriverAKId AS DriverAKID, 
	i_PolicyAKID AS PolicyAKID, 
	i_LicenseNumber AS DriversLicenseNumber, 
	i_LicenseState AS LicenseState, 
	i_DateOfBirth AS DateOfBirth, 
	i_FirstName AS Name, 
	i_MiddleInitial AS MiddleInitial, 
	i_LastName AS LastName, 
	i_Gender AS Gender, 
	i_ExcludeDriver AS ExcludeDriver, 
	i_MVRStatus AS MVRStatus
	FROM EXP_Detect_Changes
	WHERE (Changed_Flag='NEW' OR Changed_Flag='UPDATE')
),
SEQ_DriverAKID AS (
	CREATE SEQUENCE SEQ_DriverAKID
	START = 0
	INCREMENT = 1;
),
EXP_Detemine_AK_ID AS (
	SELECT
	SEQ_DriverAKID.NEXTVAL,
	Changed_Flag AS i_Changed_Flag,
	DriverAKID AS i_DriverAKID,
	-- *INF*: IIF(i_Changed_Flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),SYSDATE)
	IFF(i_Changed_Flag = 'NEW', TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS v_EffectiveDate,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	v_EffectiveDate AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	PolicyAKID,
	-- *INF*: IIF(ISNULL(i_DriverAKID),NEXTVAL,i_DriverAKID)
	IFF(i_DriverAKID IS NULL, NEXTVAL, i_DriverAKID) AS DriverAKID,
	DriversLicenseNumber,
	LicenseState,
	DateOfBirth,
	Name,
	MiddleInitial,
	LastName,
	Gender,
	ExcludeDriver,
	MVRStatus
	FROM FIL_Insert
),
Target_Driver_Insert AS (
	INSERT INTO Driver
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, DriverAKId, PolicyAKId, LicenseState, LicenseNumber, LastName, FirstName, MiddleName, GenderCode, Birthdate, ExcludeDriver, MVRStatus)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AuditID AS AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SourceSystemID AS SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	DriverAKID AS DRIVERAKID, 
	PolicyAKID AS POLICYAKID, 
	LICENSESTATE, 
	DriversLicenseNumber AS LICENSENUMBER, 
	LASTNAME, 
	Name AS FIRSTNAME, 
	MiddleInitial AS MIDDLENAME, 
	Gender AS GENDERCODE, 
	DateOfBirth AS BIRTHDATE, 
	EXCLUDEDRIVER, 
	MVRSTATUS
	FROM EXP_Detemine_AK_ID
),
SQ_Driver AS (
	SELECT 
		DriverID,
		EffectiveDate, 
		ExpirationDate,
		DriverAKID
	FROM
		@{pipeline().parameters.TARGET_TABLE_OWNER}.Driver a
	WHERE  EXISTS
		 (SELECT 1
		 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.Driver b 
		   WHERE CurrentSnapshotFlag = 1 and SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		    and a.DriverAKID = b.DriverAKID
	GROUP BY  DriverAKID  HAVING count(*) > 1)
	AND SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
	ORDER BY  DriverAKID ,EffectiveDate  DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	DriverId AS DriverID,
	EffectiveDate AS i_eff_from_date,
	ExpirationDate AS orig_eff_to_date,
	DriverAKId AS i_DriverAKID,
	-- *INF*: DECODE(TRUE,
	-- i_DriverAKID = v_PrevDriverAKID ,
	-- ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),orig_eff_to_date)
	DECODE(TRUE,
		i_DriverAKID = v_PrevDriverAKID, ADD_TO_DATE(v_prev_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	i_DriverAKID AS v_PrevDriverAKID,
	i_eff_from_date AS v_prev_eff_from_date,
	0 AS o_crrnt_snpsht_flag,
	v_eff_to_date AS o_eff_to_date,
	SYSDATE AS o_modified_date
	FROM SQ_Driver
),
FIL_FirstRowInAKGroup AS (
	SELECT
	DriverID, 
	orig_eff_to_date, 
	o_crrnt_snpsht_flag AS crrnt_snpsht_flag, 
	o_eff_to_date AS eff_to_date, 
	o_modified_date AS modified_date
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_Driver AS (
	SELECT
	DriverID, 
	crrnt_snpsht_flag, 
	eff_to_date, 
	modified_date
	FROM FIL_FirstRowInAKGroup
),
Target_Driver_Update AS (
	MERGE INTO Driver AS T
	USING UPD_Driver AS S
	ON T.DriverId = S.DriverID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.crrnt_snpsht_flag, T.ExpirationDate = S.eff_to_date, T.ModifiedDate = S.modified_date
),
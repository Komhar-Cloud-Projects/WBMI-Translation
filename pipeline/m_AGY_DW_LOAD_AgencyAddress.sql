WITH
SQ_AgencyAddressODSStage AS (
	SELECT
		AgencyCode,
		AddressType,
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
		SourceSystemID
	FROM AgencyAddressODSStage
),
LKP_AgencyAKID AS (
	SELECT
	AgencyAKID,
	AgencyCode
	FROM (
		SELECT 
			AgencyAKID,
			AgencyCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Agency
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyCode ORDER BY AgencyAKID) = 1
),
EXP_CleanupValues AS (
	SELECT
	SQ_AgencyAddressODSStage.AgencyCode,
	LKP_AgencyAKID.AgencyAKID,
	SQ_AgencyAddressODSStage.AddressType,
	SQ_AgencyAddressODSStage.AddressLine1,
	-- *INF*: IIF(IsNull(AddressLine1), 'N/A', AddressLine1)
	IFF(AddressLine1 IS NULL, 'N/A', AddressLine1) AS o_AddressLine1,
	SQ_AgencyAddressODSStage.AddressLine2,
	-- *INF*: IIF(IsNull(AddressLine2), 'N/A', AddressLine2)
	IFF(AddressLine2 IS NULL, 'N/A', AddressLine2) AS o_AddressLine2,
	SQ_AgencyAddressODSStage.AddressLine3,
	-- *INF*: IIF(IsNull(AddressLine3), 'N/A', AddressLine3)
	IFF(AddressLine3 IS NULL, 'N/A', AddressLine3) AS o_AddressLine3,
	SQ_AgencyAddressODSStage.City,
	-- *INF*: IIF(IsNull(City), 'N/A', City)
	IFF(City IS NULL, 'N/A', City) AS o_City,
	SQ_AgencyAddressODSStage.ZipCode,
	-- *INF*: IIF(IsNull(ZipCode), 'N/A', ZipCode)
	IFF(ZipCode IS NULL, 'N/A', ZipCode) AS o_ZipCode,
	SQ_AgencyAddressODSStage.CountyCode,
	-- *INF*: IIF(IsNull(CountyCode), 'N/A', CountyCode)
	IFF(CountyCode IS NULL, 'N/A', CountyCode) AS o_CountyCode,
	SQ_AgencyAddressODSStage.CountyName,
	-- *INF*: IIF(IsNull(CountyName), 'N/A', CountyName)
	IFF(CountyName IS NULL, 'N/A', CountyName) AS o_CountyName,
	SQ_AgencyAddressODSStage.StateAbbreviation,
	-- *INF*: IIF(IsNull(StateAbbreviation), 'NA', StateAbbreviation)
	IFF(StateAbbreviation IS NULL, 'NA', StateAbbreviation) AS o_StateAbbreviation,
	SQ_AgencyAddressODSStage.CountryAbbreviation,
	-- *INF*: IIF(IsNull(CountryAbbreviation), 'USA', CountryAbbreviation)
	IFF(CountryAbbreviation IS NULL, 'USA', CountryAbbreviation) AS o_CountryAbbreviation,
	SQ_AgencyAddressODSStage.Latitude,
	-- *INF*: IIF(IsNull(Latitude), 000.000000, Latitude)
	IFF(Latitude IS NULL, 000.000000, Latitude) AS o_Latitude,
	SQ_AgencyAddressODSStage.Longitude,
	-- *INF*: IIF(IsNull(Longitude), 000.000000, Longitude)
	IFF(Longitude IS NULL, 000.000000, Longitude) AS o_Longitude,
	SQ_AgencyAddressODSStage.SourceSystemID
	FROM SQ_AgencyAddressODSStage
	LEFT JOIN LKP_AgencyAKID
	ON LKP_AgencyAKID.AgencyCode = SQ_AgencyAddressODSStage.AgencyCode
),
lkp_ExistingAddress AS (
	SELECT
	HashKey,
	AgencyAddressAKID,
	AgencyAKID,
	AddressType
	FROM (
		SELECT 
			HashKey,
			AgencyAddressAKID,
			AgencyAKID,
			AddressType
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyAddress
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyAKID,AddressType ORDER BY HashKey) = 1
),
EXP_Detect_Changes AS (
	SELECT
	lkp_ExistingAddress.HashKey AS lkp_HashKey,
	lkp_ExistingAddress.AgencyAddressAKID AS lkp_AgencyAddressAKID,
	EXP_CleanupValues.AgencyAKID,
	EXP_CleanupValues.AddressType,
	EXP_CleanupValues.o_AddressLine1 AS AddressLine1,
	EXP_CleanupValues.o_AddressLine2 AS AddressLine2,
	EXP_CleanupValues.o_AddressLine3 AS AddressLine3,
	EXP_CleanupValues.o_City AS City,
	EXP_CleanupValues.o_ZipCode AS ZipCode,
	EXP_CleanupValues.o_CountyCode AS CountyCode,
	EXP_CleanupValues.o_CountyName AS CountyName,
	EXP_CleanupValues.o_StateAbbreviation AS StateAbbreviation,
	EXP_CleanupValues.o_CountryAbbreviation AS CountryAbbreviation,
	EXP_CleanupValues.o_Latitude AS Latitude,
	EXP_CleanupValues.o_Longitude AS Longitude,
	-- *INF*: MD5(AddressLine1 || AddressLine2 || AddressLine3 || City || ZipCode || CountyCode || CountyName || StateAbbreviation || CountryAbbreviation || to_char(Latitude) || to_char(Longitude))
	MD5(AddressLine1 || AddressLine2 || AddressLine3 || City || ZipCode || CountyCode || CountyName || StateAbbreviation || CountryAbbreviation || to_char(Latitude) || to_char(Longitude)) AS v_NewHashKey,
	v_NewHashKey AS o_HashKey,
	-- *INF*: Decode(true,
	-- IsNull(AgencyAKID), 'IGNORE',
	-- IsNull(lkp_AgencyAddressAKID), 'NEW', 
	-- (lkp_HashKey <> v_NewHashKey), 'UPDATE' ,
	-- 'NOCHANGE')
	Decode(true,
		AgencyAKID IS NULL, 'IGNORE',
		lkp_AgencyAddressAKID IS NULL, 'NEW',
		( lkp_HashKey <> v_NewHashKey ), 'UPDATE',
		'NOCHANGE') AS v_changed_flag,
	v_changed_flag AS changed_flag,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: iif(v_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_changed_flag = 'NEW', to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), sysdate) AS EffectiveFromDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS EffectiveToDate,
	EXP_CleanupValues.SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate
	FROM EXP_CleanupValues
	LEFT JOIN lkp_ExistingAddress
	ON lkp_ExistingAddress.AgencyAKID = EXP_CleanupValues.AgencyAKID AND lkp_ExistingAddress.AddressType = EXP_CleanupValues.AddressType
),
FIL_insert AS (
	SELECT
	lkp_AgencyAddressAKID, 
	changed_flag AS ChangedFlag, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveFromDate, 
	EffectiveToDate, 
	SourceSystemID, 
	CreatedDate, 
	ModifiedDate, 
	AgencyAKID, 
	AddressType, 
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
	o_HashKey
	FROM EXP_Detect_Changes
	WHERE ChangedFlag='NEW'or ChangedFlag='UPDATE'
),
SEQ_AgencyAddress_AKID AS (
	CREATE SEQUENCE SEQ_AgencyAddress_AKID
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
	o_HashKey AS HashKey,
	lkp_AgencyAddressAKID,
	SEQ_AgencyAddress_AKID.NEXTVAL,
	-- *INF*: iif(isnull(lkp_AgencyAddressAKID),NEXTVAL,lkp_AgencyAddressAKID)
	IFF(lkp_AgencyAddressAKID IS NULL, NEXTVAL, lkp_AgencyAddressAKID) AS o_AgencyAddressAKID,
	AgencyAKID,
	AddressType,
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
	Longitude
	FROM FIL_insert
),
AgencyAddress_Inserts AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyAddress
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, HashKey, AgencyAddressAKID, AgencyAKID, AddressType, AddressLine1, AddressLine2, AddressLine3, City, ZipCode, CountyCode, CountyName, StateAbbreviation, CountryAbbreviation, Latitude, Longitude)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	HASHKEY, 
	o_AgencyAddressAKID AS AGENCYADDRESSAKID, 
	AGENCYAKID, 
	ADDRESSTYPE, 
	ADDRESSLINE1, 
	ADDRESSLINE2, 
	ADDRESSLINE3, 
	CITY, 
	ZIPCODE, 
	COUNTYCODE, 
	COUNTYNAME, 
	STATEABBREVIATION, 
	COUNTRYABBREVIATION, 
	LATITUDE, 
	LONGITUDE
	FROM EXP_Assign_AKID
),
SQ_AgencyAddress AS (
	SELECT 
		a.AgencyAddressID, 
		a.EffectiveDate,
		a.ExpirationDate, 
		a.AgencyAddressAKID
	FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyAddress a
	WHERE  a.AgencyAddressAKID  IN
		( SELECT AgencyAddressAKID  FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyAddress
		WHERE CurrentSnapshotFlag = 1 GROUP BY AgencyAddressAKID HAVING count(*) > 1) 
	ORDER BY a.AgencyAddressAKID, a.EffectiveDate DESC
	
	
	
	--IN Subquery exists to pick AK ID column values that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag 
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
),
EXP_Lag_eff_from_date AS (
	SELECT
	AgencyAddressID,
	EffectiveDate AS EffectiveFromDate,
	ExpirationDate AS OriginalEffectiveToDate,
	AgencyAddressAKID,
	-- *INF*: DECODE(TRUE,
	-- AgencyAddressAKID = v_prev_AKID , ADD_TO_DATE(v_prev_EffectiveFromDate,'SS',-1),
	-- OriginalEffectiveToDate)
	DECODE(TRUE,
		AgencyAddressAKID = v_prev_AKID, ADD_TO_DATE(v_prev_EffectiveFromDate, 'SS', - 1),
		OriginalEffectiveToDate) AS v_EffectiveToDate,
	v_EffectiveToDate AS o_EffectiveToDate,
	AgencyAddressAKID AS v_prev_AKID,
	EffectiveFromDate AS v_prev_EffectiveFromDate,
	0 AS CurrentSnapshotFlag,
	SYSDATE AS ModifiedDate
	FROM SQ_AgencyAddress
),
FIL_FirstRowInAKGroup AS (
	SELECT
	AgencyAddressID AS AgencyAddressId, 
	OriginalEffectiveToDate, 
	o_EffectiveToDate AS NewEffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM EXP_Lag_eff_from_date
	WHERE OriginalEffectiveToDate != NewEffectiveToDate
),
UPD_OldRecord AS (
	SELECT
	AgencyAddressId AS AgencyAddressID, 
	NewEffectiveToDate AS EffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM FIL_FirstRowInAKGroup
),
AgencyAddress_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyAddress AS T
	USING UPD_OldRecord AS S
	ON T.AgencyAddressID = S.AgencyAddressID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.EffectiveToDate, T.ModifiedDate = S.ModifiedDate
),
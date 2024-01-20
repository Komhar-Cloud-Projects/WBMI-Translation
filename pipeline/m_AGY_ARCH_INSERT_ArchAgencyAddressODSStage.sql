WITH
SQ_AgencyAddressODSStage AS (
	SELECT
		AgencyAddressODSStageID,
		AgencyODSSourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AgencyID,
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
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemID
	FROM AgencyAddressODSStage
),
LKP_ExistingArchive AS (
	SELECT
	HashKey,
	in_AgencyID,
	in_AddressType,
	AgencyID,
	AddressType,
	ModifiedDate
	FROM (
		select	a.ModifiedDate as ModifiedDate,
				a.HashKey as HashKey,
				a.AgencyID as AgencyID, 
				a.AddressType as AddressType
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchAgencyAddressODSStage a
		inner join (
					select AgencyID, AddressType, max(ModifiedDate) as ModifiedDate
					from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchAgencyAddressODSStage 
					group by AgencyID, AddressType) b
		on a.AgencyID = b.AgencyID
		and a.AddressType = b.AddressType
		and a.ModifiedDate = b.ModifiedDate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyID,AddressType ORDER BY HashKey) = 1
),
EXP_AddAuditID AS (
	SELECT
	SQ_AgencyAddressODSStage.AgencyAddressODSStageID,
	SQ_AgencyAddressODSStage.AgencyODSSourceSystemID,
	SQ_AgencyAddressODSStage.HashKey,
	SQ_AgencyAddressODSStage.ModifiedUserID,
	SQ_AgencyAddressODSStage.ModifiedDate,
	SQ_AgencyAddressODSStage.AgencyID,
	SQ_AgencyAddressODSStage.AgencyCode,
	SQ_AgencyAddressODSStage.AddressType,
	SQ_AgencyAddressODSStage.AddressLine1,
	SQ_AgencyAddressODSStage.AddressLine2,
	SQ_AgencyAddressODSStage.AddressLine3,
	SQ_AgencyAddressODSStage.City,
	SQ_AgencyAddressODSStage.ZipCode,
	SQ_AgencyAddressODSStage.CountyCode,
	SQ_AgencyAddressODSStage.CountyName,
	SQ_AgencyAddressODSStage.StateAbbreviation,
	SQ_AgencyAddressODSStage.CountryAbbreviation,
	SQ_AgencyAddressODSStage.Latitude,
	SQ_AgencyAddressODSStage.Longitude,
	SQ_AgencyAddressODSStage.ExtractDate,
	SQ_AgencyAddressODSStage.AsOfDate,
	SQ_AgencyAddressODSStage.RecordCount,
	SQ_AgencyAddressODSStage.SourceSystemID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	LKP_ExistingArchive.HashKey AS lkp_HashKey,
	-- *INF*: Decode(true,
	-- HashKey = lkp_HashKey, 'IGNORE',
	-- IsNull(lkp_HashKey), 'INSERT',
	-- 'UPDATE')
	Decode(
	    true,
	    HashKey = lkp_HashKey, 'IGNORE',
	    lkp_HashKey IS NULL, 'INSERT',
	    'UPDATE'
	) AS v_ChangeFlag,
	v_ChangeFlag AS o_ChangeFlag
	FROM SQ_AgencyAddressODSStage
	LEFT JOIN LKP_ExistingArchive
	ON LKP_ExistingArchive.AgencyID = SQ_AgencyAddressODSStage.AgencyID AND LKP_ExistingArchive.AddressType = SQ_AgencyAddressODSStage.AddressType
),
FIL_ChangesOnly AS (
	SELECT
	AgencyAddressODSStageID, 
	AgencyODSSourceSystemID, 
	HashKey, 
	ModifiedUserID, 
	ModifiedDate, 
	AgencyID, 
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
	ExtractDate, 
	AsOfDate, 
	RecordCount, 
	SourceSystemID, 
	o_AuditID AS OUT_AUDIT_ID, 
	o_ChangeFlag
	FROM EXP_AddAuditID
	WHERE o_ChangeFlag = 'INSERT' OR o_ChangeFlag = 'UPDATE'
),
ArchAgencyAddressODSStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchAgencyAddressODSStage
	(AgencyAddressODSStageID, AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, AgencyID, AgencyCode, AddressType, AddressLine1, AddressLine2, AddressLine3, City, ZipCode, CountyCode, CountyName, StateAbbreviation, CountryAbbreviation, Latitude, Longitude, ExtractDate, AsOfDate, RecordCount, SourceSystemID, AuditID)
	SELECT 
	AGENCYADDRESSODSSTAGEID, 
	AGENCYODSSOURCESYSTEMID, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	AGENCYID, 
	AGENCYCODE, 
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
	LONGITUDE, 
	EXTRACTDATE, 
	ASOFDATE, 
	RECORDCOUNT, 
	SOURCESYSTEMID, 
	OUT_AUDIT_ID AS AUDITID
	FROM FIL_ChangesOnly
),
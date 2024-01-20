WITH
SQ_AgencyODSStage AS (
	 SELECT
		AgencyODSStageID,
		AgencyODSSourceSystemID,
		HashKey,
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
		CustomerCareStatus,
		FederalTaxID,
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemID,
		ProfitSharingGuaranteeFlag,
		LicensedIndicator,
		AbbreviatedName,
		AssignedStateCode,
		ClosedDate
	FROM AgencyODSStage
),
LKP_ExistingArchive AS (
	SELECT
	HashKey,
	ClosedDate,
	AgencyCode
	FROM (
		select 	a.HashKey as HashKey, 
					a.ModifiedDate as ModifiedDate,  
					a.AgencyCode as AgencyCode,
					a.ClosedDate as ClosedDate,
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchAgencyODSStage a
		inner join (	
					select AgencyCode, MAX(ModifiedDate) as ModifiedDate
					from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchAgencyODSStage
					group by AgencyCode) b
		on a.AgencyCode = b.AgencyCode
		and a.ModifiedDate = b.ModifiedDate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyCode ORDER BY HashKey) = 1
),
EXP_CheckForChange AS (
	SELECT
	SQ_AgencyODSStage.AgencyODSStageID,
	SQ_AgencyODSStage.AgencyODSSourceSystemID,
	SQ_AgencyODSStage.HashKey,
	SQ_AgencyODSStage.ModifiedUserID,
	SQ_AgencyODSStage.ModifiedDate,
	SQ_AgencyODSStage.AgencyID,
	SQ_AgencyODSStage.AgencyCode,
	SQ_AgencyODSStage.LegalName,
	SQ_AgencyODSStage.DoingBusinessAsName,
	SQ_AgencyODSStage.PrimaryPhoneNumber,
	SQ_AgencyODSStage.PrimaryFaxNumber,
	SQ_AgencyODSStage.PrimaryEmailAddress,
	SQ_AgencyODSStage.StatusCode,
	SQ_AgencyODSStage.StatusDescription,
	SQ_AgencyODSStage.AppointedDate,
	SQ_AgencyODSStage.TerminatedDate,
	SQ_AgencyODSStage.CustomerCareStatus,
	SQ_AgencyODSStage.ExtractDate,
	SQ_AgencyODSStage.AsOfDate,
	SQ_AgencyODSStage.RecordCount,
	SQ_AgencyODSStage.SourceSystemID,
	SQ_AgencyODSStage.FederalTaxID,
	SQ_AgencyODSStage.ProfitSharingGuaranteeFlag,
	SQ_AgencyODSStage.LicensedIndicator,
	SQ_AgencyODSStage.AbbreviatedName,
	SQ_AgencyODSStage.AssignedStateCode,
	LKP_ExistingArchive.HashKey AS lkp_HashKey,
	LKP_ExistingArchive.ClosedDate AS lkp_ClosedDate,
	SQ_AgencyODSStage.ClosedDate,
	-- *INF*: Decode(true,
	-- HashKey = lkp_HashKey and lkp_ClosedDate=ClosedDate, 'IGNORE',
	-- IsNull(lkp_HashKey), 'INSERT',
	-- 'UPDATE')
	Decode(
	    true,
	    HashKey = lkp_HashKey and lkp_ClosedDate = ClosedDate, 'IGNORE',
	    lkp_HashKey IS NULL, 'INSERT',
	    'UPDATE'
	) AS v_ChangeFlag,
	v_ChangeFlag AS o_ChangeFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS OUT_AUDIT_ID
	FROM SQ_AgencyODSStage
	LEFT JOIN LKP_ExistingArchive
	ON LKP_ExistingArchive.AgencyCode = SQ_AgencyODSStage.AgencyCode
),
FIL_InsertChangeRecordsOnly AS (
	SELECT
	AgencyODSStageID, 
	AgencyODSSourceSystemID, 
	HashKey, 
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
	CustomerCareStatus, 
	FederalTaxID, 
	ExtractDate, 
	AsOfDate, 
	RecordCount, 
	SourceSystemID, 
	OUT_AUDIT_ID, 
	o_ChangeFlag, 
	ProfitSharingGuaranteeFlag, 
	LicensedIndicator, 
	AbbreviatedName, 
	AssignedStateCode, 
	ClosedDate
	FROM EXP_CheckForChange
	WHERE o_ChangeFlag = 'INSERT' OR o_ChangeFlag = 'UPDATE'
),
ArchAgencyODSStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchAgencyODSStage
	(AgencyODSStageID, AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, AgencyID, AgencyCode, LegalName, DoingBusinessAsName, PrimaryPhoneNumber, PrimaryFaxNumber, PrimaryEmailAddress, StatusCode, StatusDescription, AppointedDate, TerminatedDate, CustomerCareStatus, FederalTaxID, ExtractDate, AsOfDate, RecordCount, SourceSystemID, AuditID, ProfitSharingGuaranteeFlag, LicensedIndicator, AbbreviatedName, AssignedStateCode, ClosedDate)
	SELECT 
	AGENCYODSSTAGEID, 
	AGENCYODSSOURCESYSTEMID, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	AGENCYID, 
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
	CUSTOMERCARESTATUS, 
	FEDERALTAXID, 
	EXTRACTDATE, 
	ASOFDATE, 
	RECORDCOUNT, 
	SOURCESYSTEMID, 
	OUT_AUDIT_ID AS AUDITID, 
	PROFITSHARINGGUARANTEEFLAG, 
	LICENSEDINDICATOR, 
	ABBREVIATEDNAME, 
	ASSIGNEDSTATECODE, 
	CLOSEDDATE
	FROM FIL_InsertChangeRecordsOnly
),
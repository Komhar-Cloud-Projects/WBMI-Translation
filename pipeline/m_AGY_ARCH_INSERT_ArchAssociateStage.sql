WITH
SQ_AssociateStage AS (
	SELECT
		AssociateStageID,
		AgencyODSSourceSystemID,
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
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemID,
		UserId,
		StrategicProfitCenterCode,
		StrategicProfitCenterDescription
	FROM AssociateStage
),
LKP_ExistingArchive AS (
	SELECT
	in_WestBendAssociateID,
	WestBendAssociateID,
	HashKey,
	ModifiedDate
	FROM (
		select	a.HashKey as HashKey,
		  		a.ModifiedDate as ModifiedDate,
				a.WestBendAssociateID as WestBendAssociateID
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchAssociateStage a
		inner join (
					select WestBendAssociateID, max(ModifiedDate) as ModifiedDate
					from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchAssociateStage 
					group by WestBendAssociateID) b
		on  a.WestBendAssociateID = b.WestBendAssociateID
		and a.ModifiedDate = b.ModifiedDate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WestBendAssociateID ORDER BY in_WestBendAssociateID) = 1
),
EXP_AddAuditID AS (
	SELECT
	SQ_AssociateStage.AssociateStageID,
	SQ_AssociateStage.AgencyODSSourceSystemID,
	SQ_AssociateStage.HashKey,
	SQ_AssociateStage.ModifiedUserID,
	SQ_AssociateStage.ModifiedDate,
	SQ_AssociateStage.WestBendAssociateID,
	SQ_AssociateStage.AssociateRole,
	SQ_AssociateStage.RoleSpecificUserCode,
	SQ_AssociateStage.DisplayName,
	SQ_AssociateStage.LastName,
	SQ_AssociateStage.FirstName,
	SQ_AssociateStage.MiddleName,
	SQ_AssociateStage.Suffix,
	SQ_AssociateStage.EmailAddress,
	SQ_AssociateStage.ExtractDate,
	SQ_AssociateStage.AsOfDate,
	SQ_AssociateStage.RecordCount,
	SQ_AssociateStage.SourceSystemID,
	SQ_AssociateStage.UserId,
	SQ_AssociateStage.StrategicProfitCenterCode,
	SQ_AssociateStage.StrategicProfitCenterDescription,
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
	v_ChangeFlag AS o_ChangeFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS OUT_AUDIT_ID
	FROM SQ_AssociateStage
	LEFT JOIN LKP_ExistingArchive
	ON LKP_ExistingArchive.WestBendAssociateID = SQ_AssociateStage.WestBendAssociateID
),
FIL_ChangesOnly AS (
	SELECT
	AssociateStageID, 
	AgencyODSSourceSystemID, 
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
	ExtractDate, 
	AsOfDate, 
	RecordCount, 
	SourceSystemID, 
	UserId, 
	OUT_AUDIT_ID, 
	StrategicProfitCenterCode, 
	StrategicProfitCenterDescription, 
	o_ChangeFlag
	FROM EXP_AddAuditID
	WHERE o_ChangeFlag = 'INSERT' OR o_ChangeFlag = 'UPDATE'
),
ArchAssociateStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchAssociateStage
	(AssociateStageID, AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, WestBendAssociateID, AssociateRole, RoleSpecificUserCode, DisplayName, LastName, FirstName, MiddleName, Suffix, EmailAddress, ExtractDate, AsOfDate, RecordCount, SourceSystemID, AuditID, UserId, StrategicProfitCenterCode, StrategicProfitCenterDescription)
	SELECT 
	ASSOCIATESTAGEID, 
	AGENCYODSSOURCESYSTEMID, 
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
	EXTRACTDATE, 
	ASOFDATE, 
	RECORDCOUNT, 
	SOURCESYSTEMID, 
	OUT_AUDIT_ID AS AUDITID, 
	USERID, 
	STRATEGICPROFITCENTERCODE, 
	STRATEGICPROFITCENTERDESCRIPTION
	FROM FIL_ChangesOnly
),
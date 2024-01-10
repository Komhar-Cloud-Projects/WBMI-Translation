WITH
SQ_AssociateStage AS (
	SELECT
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
	WHERE AssociateStage.AssociateRole =  'REGIONAL SALES MANAGER'
),
SQ_SalesReportingRelationshipStage AS (
	SELECT
		SalesReportingRelationshipStageID,
		AgencyODSSourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AssociateID,
		WestBendAssociateID,
		ReportToAssociateID,
		ReportToWestBendAssociateID,
		RelationshipType,
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemID
	FROM SalesReportingRelationshipStage
),
JNR_OuterToReportingRelationship AS (SELECT
	SQ_AssociateStage.WestBendAssociateID, 
	SQ_AssociateStage.RoleSpecificUserCode, 
	SQ_AssociateStage.DisplayName, 
	SQ_AssociateStage.LastName, 
	SQ_AssociateStage.FirstName, 
	SQ_AssociateStage.MiddleName, 
	SQ_AssociateStage.Suffix, 
	SQ_AssociateStage.EmailAddress, 
	SQ_AssociateStage.SourceSystemID, 
	SQ_AssociateStage.UserId, 
	SQ_SalesReportingRelationshipStage.WestBendAssociateID AS rpt_WestBendAssociateID, 
	SQ_SalesReportingRelationshipStage.ReportToWestBendAssociateID AS rpt_ReportToWestBendAssociateID
	FROM SQ_SalesReportingRelationshipStage
	RIGHT OUTER JOIN SQ_AssociateStage
	ON SQ_AssociateStage.WestBendAssociateID = SQ_SalesReportingRelationshipStage.WestBendAssociateID
),
LKP_RegionalSalesManager AS (
	SELECT
	in_WestBendAssociateID,
	HashKey,
	RegionalSalesManagerAKID,
	WestBendAssociateID
	FROM (
		SELECT 
			in_WestBendAssociateID,
			HashKey,
			RegionalSalesManagerAKID,
			WestBendAssociateID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RegionalSalesManager
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WestBendAssociateID ORDER BY in_WestBendAssociateID) = 1
),
lkp_SalesDirectorAKID AS (
	SELECT
	SalesDirectorAKID,
	WestBendAssociateID
	FROM (
		SELECT 
			SalesDirectorAKID,
			WestBendAssociateID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SalesDirector
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WestBendAssociateID ORDER BY SalesDirectorAKID) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_RegionalSalesManager.RegionalSalesManagerAKID AS lkp_RegionalSalesManagerAKID,
	LKP_RegionalSalesManager.HashKey AS lkp_HashKey,
	lkp_SalesDirectorAKID.SalesDirectorAKID AS lkp_SalesDirectorAKID,
	-- *INF*: IIF(IsNull(lkp_SalesDirectorAKID), -1, lkp_SalesDirectorAKID)
	IFF(lkp_SalesDirectorAKID IS NULL, - 1, lkp_SalesDirectorAKID) AS o_SalesDirectorAKID,
	JNR_OuterToReportingRelationship.WestBendAssociateID,
	JNR_OuterToReportingRelationship.DisplayName,
	JNR_OuterToReportingRelationship.LastName,
	JNR_OuterToReportingRelationship.FirstName,
	JNR_OuterToReportingRelationship.MiddleName,
	JNR_OuterToReportingRelationship.Suffix,
	JNR_OuterToReportingRelationship.EmailAddress,
	-- *INF*: IIF(IsNull(EmailAddress), 'N/A', EmailAddress)
	IFF(EmailAddress IS NULL, 'N/A', EmailAddress) AS o_EmailAddress,
	JNR_OuterToReportingRelationship.RoleSpecificUserCode AS RSMCode,
	JNR_OuterToReportingRelationship.UserId,
	-- *INF*: IIF(IsNull(UserId), 'N/A', UserId)
	IFF(UserId IS NULL, 'N/A', UserId) AS o_UserId,
	-- *INF*: MD5(DisplayName || LastName || FirstName || MiddleName || Suffix || EmailAddress || RSMCode || UserId || to_char(lkp_SalesDirectorAKID))
	MD5(DisplayName || LastName || FirstName || MiddleName || Suffix || EmailAddress || RSMCode || UserId || to_char(lkp_SalesDirectorAKID)) AS v_NewHashKey,
	v_NewHashKey AS o_NewHashKey,
	-- *INF*: IIF(ISNULL(lkp_HashKey), 'NEW', 
	-- IIF((lkp_HashKey <> v_NewHashKey), 'UPDATE', 'NOCHANGE'))
	IFF(lkp_HashKey IS NULL, 'NEW', IFF(( lkp_HashKey <> v_NewHashKey ), 'UPDATE', 'NOCHANGE')) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: iif(v_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_changed_flag = 'NEW', to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), sysdate) AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	JNR_OuterToReportingRelationship.SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate
	FROM JNR_OuterToReportingRelationship
	LEFT JOIN LKP_RegionalSalesManager
	ON LKP_RegionalSalesManager.WestBendAssociateID = JNR_OuterToReportingRelationship.WestBendAssociateID
	LEFT JOIN lkp_SalesDirectorAKID
	ON lkp_SalesDirectorAKID.WestBendAssociateID = JNR_OuterToReportingRelationship.rpt_ReportToWestBendAssociateID
),
FIL_insert AS (
	SELECT
	changed_flag, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemID, 
	CreatedDate, 
	ModifiedDate, 
	lkp_RegionalSalesManagerAKID AS RegionalSalesManagerAKID, 
	o_NewHashKey AS HashKey, 
	o_SalesDirectorAKID AS SalesDirectorAKID, 
	WestBendAssociateID, 
	DisplayName, 
	LastName, 
	FirstName, 
	MiddleName, 
	Suffix, 
	o_EmailAddress AS EmailAddress, 
	RSMCode, 
	o_UserId AS UserId
	FROM EXP_Detect_Changes
	WHERE changed_flag='NEW'or changed_flag='UPDATE'
),
SEQ_RegionalSalesManager_AKID AS (
	CREATE SEQUENCE SEQ_RegionalSalesManager_AKID
	START = 0
	INCREMENT = 1;
),
EXP_Assign_AKID AS (
	SELECT
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	RegionalSalesManagerAKID,
	HashKey,
	SEQ_RegionalSalesManager_AKID.NEXTVAL,
	-- *INF*: iif(isnull(RegionalSalesManagerAKID),NEXTVAL,RegionalSalesManagerAKID)
	IFF(RegionalSalesManagerAKID IS NULL, NEXTVAL, RegionalSalesManagerAKID) AS o_RegionalSalesManagerAKID,
	SalesDirectorAKID,
	WestBendAssociateID,
	DisplayName,
	LastName,
	FirstName,
	MiddleName,
	Suffix,
	EmailAddress,
	RSMCode,
	UserId
	FROM FIL_insert
),
RegionalSalesManager_Inserts AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.RegionalSalesManager
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, HashKey, RegionalSalesManagerAKID, SalesDirectorAKID, WestBendAssociateID, RegionalSalesManagerCode, DisplayName, LastName, FirstName, MiddleName, Suffix, EmailAddress, UserId)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	HASHKEY, 
	o_RegionalSalesManagerAKID AS REGIONALSALESMANAGERAKID, 
	SALESDIRECTORAKID, 
	WESTBENDASSOCIATEID, 
	RSMCode AS REGIONALSALESMANAGERCODE, 
	DISPLAYNAME, 
	LASTNAME, 
	FIRSTNAME, 
	MIDDLENAME, 
	SUFFIX, 
	EMAILADDRESS, 
	USERID
	FROM EXP_Assign_AKID
),
SQ_RegionalSalesManager AS (
	SELECT 
		a.RegionalSalesManagerID, 
		a.EffectiveDate,
		a.ExpirationDate, 
		a.RegionalSalesManagerAKID
	FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.RegionalSalesManager a
	WHERE  a.RegionalSalesManagerAKID  IN
		( SELECT RegionalSalesManagerAKID  FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RegionalSalesManager
		WHERE CurrentSnapshotFlag = 1 GROUP BY RegionalSalesManagerAKID HAVING count(*) > 1) 
	ORDER BY a.RegionalSalesManagerAKID ,a.EffectiveDate DESC
	
	
	
	--IN Subquery exists to pick AK ID column values that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag 
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
),
EXP_Lag_eff_from_date AS (
	SELECT
	RegionalSalesManagerID,
	EffectiveDate AS EffectiveFromDate,
	ExpirationDate AS OriginalEffectiveToDate,
	RegionalSalesManagerAKID,
	-- *INF*: DECODE(TRUE,
	-- RegionalSalesManagerAKID = v_prev_AKID , ADD_TO_DATE(v_prev_EffectiveFromDate,'SS',-1),
	-- OriginalEffectiveToDate)
	DECODE(TRUE,
		RegionalSalesManagerAKID = v_prev_AKID, ADD_TO_DATE(v_prev_EffectiveFromDate, 'SS', - 1),
		OriginalEffectiveToDate) AS v_EffectiveToDate,
	v_EffectiveToDate AS o_EffectiveToDate,
	RegionalSalesManagerAKID AS v_prev_AKID,
	EffectiveFromDate AS v_prev_EffectiveFromDate,
	0 AS CurrentSnapshotFlag,
	SYSDATE AS ModifiedDate
	FROM SQ_RegionalSalesManager
),
FIL_FirstRowInAKGroup AS (
	SELECT
	RegionalSalesManagerID, 
	OriginalEffectiveToDate, 
	o_EffectiveToDate AS NewEffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM EXP_Lag_eff_from_date
	WHERE OriginalEffectiveToDate != NewEffectiveToDate
),
UPD_OldRecord AS (
	SELECT
	RegionalSalesManagerID, 
	NewEffectiveToDate AS EffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM FIL_FirstRowInAKGroup
),
RegionalSalesManager_Updates AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.RegionalSalesManager AS T
	USING UPD_OldRecord AS S
	ON T.RegionalSalesManagerID = S.RegionalSalesManagerID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.EffectiveToDate, T.ModifiedDate = S.ModifiedDate
),
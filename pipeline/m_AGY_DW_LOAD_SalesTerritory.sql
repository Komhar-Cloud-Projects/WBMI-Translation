WITH
SQ_SalesTerritoryStage AS (
	SELECT
		SalesTerritoryStageID,
		AgencyODSSourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		SalesTerritoryCode,
		SalesTerritoryCodeDescription,
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemID
	FROM SalesTerritoryStage
),
LKP_ExistingSalesTerritory AS (
	SELECT
	in_SalesTerritoryCode,
	HashKey,
	SalesTerritoryAKID,
	SalesTerritoryCode
	FROM (
		SELECT 
			in_SalesTerritoryCode,
			HashKey,
			SalesTerritoryAKID,
			SalesTerritoryCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SalesTerritory
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SalesTerritoryCode ORDER BY in_SalesTerritoryCode) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_ExistingSalesTerritory.HashKey AS lkp_HashKey,
	LKP_ExistingSalesTerritory.SalesTerritoryAKID AS lkp_SalesTerritoryAKID,
	SQ_SalesTerritoryStage.SalesTerritoryCode,
	SQ_SalesTerritoryStage.SalesTerritoryCodeDescription,
	-- *INF*: MD5(SalesTerritoryCode || SalesTerritoryCodeDescription)
	MD5(SalesTerritoryCode || SalesTerritoryCodeDescription) AS v_NewHashKey,
	v_NewHashKey AS o_NewHashKey,
	-- *INF*: IIF(ISNULL(lkp_SalesTerritoryAKID), 'NEW', IIF((lkp_HashKey <> v_NewHashKey), 'UPDATE', 'NOCHANGE'))
	IFF(lkp_SalesTerritoryAKID IS NULL, 'NEW', IFF(( lkp_HashKey <> v_NewHashKey ), 'UPDATE', 'NOCHANGE')) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: iif(v_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_changed_flag = 'NEW', to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), sysdate) AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	SQ_SalesTerritoryStage.SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate
	FROM SQ_SalesTerritoryStage
	LEFT JOIN LKP_ExistingSalesTerritory
	ON LKP_ExistingSalesTerritory.SalesTerritoryCode = SQ_SalesTerritoryStage.SalesTerritoryCode
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
	o_NewHashKey AS HashKey, 
	SalesTerritoryCode, 
	SalesTerritoryCodeDescription
	FROM EXP_Detect_Changes
	WHERE changed_flag='NEW'or changed_flag='UPDATE'
),
SEQ_SalesTerritory_AKID AS (
	CREATE SEQUENCE SEQ_SalesTerritory_AKID
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
	lkp_SalesTerritoryAKID,
	SEQ_SalesTerritory_AKID.NEXTVAL,
	-- *INF*: iif(isnull(lkp_SalesTerritoryAKID),NEXTVAL,lkp_SalesTerritoryAKID)
	IFF(lkp_SalesTerritoryAKID IS NULL, NEXTVAL, lkp_SalesTerritoryAKID) AS SalesTerritoryAKID,
	HashKey,
	SalesTerritoryCode,
	SalesTerritoryCodeDescription
	FROM FIL_insert
),
SalesTerritory_Inserts AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SalesTerritory
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, HashKey, SalesTerritoryAKID, SalesTerritoryCode, SalesTerritoryCodeDescription)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	HASHKEY, 
	SALESTERRITORYAKID, 
	SALESTERRITORYCODE, 
	SALESTERRITORYCODEDESCRIPTION
	FROM EXP_Assign_AKID
),
SQ_SalesTerritory AS (
	SELECT 
		a.SalesTerritoryID, 
		a.EffectiveDate,
		a.ExpirationDate, 
		a.SalesTerritoryAKID
	FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.SalesTerritory a
	WHERE  a.SalesTerritoryAKID  IN
		( SELECT SalesTerritoryAKID  FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SalesTerritory
		WHERE CurrentSnapshotFlag = 1 GROUP BY SalesTerritoryAKID HAVING count(*) > 1) 
	ORDER BY a.SalesTerritoryAKID, a.EffectiveDate DESC
	
	
	
	--IN Subquery exists to pick AK ID column values that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag 
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
),
EXP_Lag_eff_from_date AS (
	SELECT
	SalesTerritoryID,
	EffectiveDate AS EffectiveFromDate,
	ExpirationDate AS OriginalEffectiveToDate,
	SalesTerritoryAKID,
	-- *INF*: DECODE(TRUE,
	-- SalesTerritoryAKID = v_prev_AKID , ADD_TO_DATE(v_prev_EffectiveFromDate,'SS',-1),
	-- OriginalEffectiveToDate)
	DECODE(TRUE,
	SalesTerritoryAKID = v_prev_AKID, ADD_TO_DATE(v_prev_EffectiveFromDate, 'SS', - 1),
	OriginalEffectiveToDate) AS v_EffectiveToDate,
	v_EffectiveToDate AS o_EffectiveToDate,
	SalesTerritoryAKID AS v_prev_AKID,
	EffectiveFromDate AS v_prev_EffectiveFromDate,
	0 AS CurrentSnapshotFlag,
	SYSDATE AS ModifiedDate
	FROM SQ_SalesTerritory
),
FIL_FirstRowInAKGroup AS (
	SELECT
	SalesTerritoryID, 
	OriginalEffectiveToDate, 
	o_EffectiveToDate AS NewEffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM EXP_Lag_eff_from_date
	WHERE OriginalEffectiveToDate != NewEffectiveToDate
),
UPD_OldRecord AS (
	SELECT
	SalesTerritoryID, 
	NewEffectiveToDate AS EffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM FIL_FirstRowInAKGroup
),
SalesTerritory_Updates AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SalesTerritory AS T
	USING UPD_OldRecord AS S
	ON T.SalesTerritoryID = S.SalesTerritoryID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.EffectiveToDate, T.ModifiedDate = S.ModifiedDate
),
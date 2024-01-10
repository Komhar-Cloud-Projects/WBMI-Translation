WITH
SQ_AgencyRelationshipStage AS (
	SELECT
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AgencyID,
		AgencyCode,
		RelatedAgencyID,
		RelatedToAgencyCode,
		RelationshipType,
		EffectiveDate,
		ExpirationDate,
		AgencyODSSourceSystemID AS SourceSystemID
	FROM AgencyRelationshipStage
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
LKP_RelatedToAgencyAKID AS (
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
EXP_GetAKIDs AS (
	SELECT
	LKP_AgencyAKID.AgencyAKID AS lkp_AgencyAKID,
	LKP_RelatedToAgencyAKID.AgencyAKID AS lkp_RelatedAgencyAKID,
	SQ_AgencyRelationshipStage.RelationshipType,
	SQ_AgencyRelationshipStage.EffectiveDate AS AgencyRelationshipEffectiveDate,
	-- *INF*: IIF(RelationshipType= 'LEGAL DELETED', TO_DATE('1800/01/01', 'YYYY/MM/DD'), AgencyRelationshipEffectiveDate)
	IFF(RelationshipType = 'LEGAL DELETED', TO_DATE('1800/01/01', 'YYYY/MM/DD'), AgencyRelationshipEffectiveDate) AS o_AgencyRelationshipEffectiveDate,
	SQ_AgencyRelationshipStage.ExpirationDate AS AgencyRelationshipExpirationDate,
	-- *INF*: IIF(RelationshipType= 'LEGAL DELETED', TO_DATE('1800/01/01', 'YYYY/MM/DD'), AgencyRelationshipExpirationDate)
	IFF(RelationshipType = 'LEGAL DELETED', TO_DATE('1800/01/01', 'YYYY/MM/DD'), AgencyRelationshipExpirationDate) AS o_AgencyRelationshipExpirationDate,
	SQ_AgencyRelationshipStage.SourceSystemID
	FROM SQ_AgencyRelationshipStage
	LEFT JOIN LKP_AgencyAKID
	ON LKP_AgencyAKID.AgencyCode = SQ_AgencyRelationshipStage.AgencyCode
	LEFT JOIN LKP_RelatedToAgencyAKID
	ON LKP_RelatedToAgencyAKID.AgencyCode = SQ_AgencyRelationshipStage.RelatedToAgencyCode
),
LKP_ILExistingAgencyRelationship AS (
	SELECT
	AgencyRelationshipID,
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	HashKey,
	AgencyRelationshipAKID,
	AgencyAKID,
	RelatedAgencyAKID,
	RelationshipType,
	AgencyRelationshipEffectiveDate,
	AgencyRelationshipExpirationDate,
	lkp_AgencyAKID,
	lkp_RelatedAgencyAKID,
	in_SourceSystemID
	FROM (
		SELECT 
			AgencyRelationshipID,
			CurrentSnapshotFlag,
			AuditID,
			EffectiveDate,
			ExpirationDate,
			SourceSystemID,
			CreatedDate,
			ModifiedDate,
			HashKey,
			AgencyRelationshipAKID,
			AgencyAKID,
			RelatedAgencyAKID,
			RelationshipType,
			AgencyRelationshipEffectiveDate,
			AgencyRelationshipExpirationDate,
			lkp_AgencyAKID,
			lkp_RelatedAgencyAKID,
			in_SourceSystemID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyRelationship
		WHERE currentsnapshotflag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyAKID,RelatedAgencyAKID,SourceSystemID ORDER BY AgencyRelationshipID) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_ILExistingAgencyRelationship.AgencyRelationshipAKID AS lkp_AgencyRelationshipAKID,
	EXP_GetAKIDs.lkp_AgencyAKID AS AgencyAKID,
	EXP_GetAKIDs.lkp_RelatedAgencyAKID AS RelatedAgencyAKID,
	LKP_ILExistingAgencyRelationship.HashKey AS lkp_HashKey,
	EXP_GetAKIDs.RelationshipType,
	EXP_GetAKIDs.o_AgencyRelationshipEffectiveDate AS AgencyRelationshipEffectiveDate,
	EXP_GetAKIDs.o_AgencyRelationshipExpirationDate AS AgencyRelationshipExpirationDate,
	-- *INF*: MD5(AgencyAKID||RelatedAgencyAKID||RelationshipType || to_char(AgencyRelationshipEffectiveDate) || to_char(AgencyRelationshipExpirationDate))
	MD5(AgencyAKID || RelatedAgencyAKID || RelationshipType || to_char(AgencyRelationshipEffectiveDate) || to_char(AgencyRelationshipExpirationDate)) AS v_NewHashKey,
	v_NewHashKey AS o_NewHashKey,
	-- *INF*: IIF(ISNULL(lkp_AgencyRelationshipAKID), 'NEW', 
	-- IIF((v_NewHashKey <> lkp_HashKey), 'UPDATE', 'NOCHANGE'))
	IFF(lkp_AgencyRelationshipAKID IS NULL, 'NEW', IFF(( v_NewHashKey <> lkp_HashKey ), 'UPDATE', 'NOCHANGE')) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: iif(v_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_changed_flag = 'NEW', to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), sysdate) AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	EXP_GetAKIDs.SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate
	FROM EXP_GetAKIDs
	LEFT JOIN LKP_ILExistingAgencyRelationship
	ON LKP_ILExistingAgencyRelationship.AgencyAKID = EXP_GetAKIDs.lkp_AgencyAKID AND LKP_ILExistingAgencyRelationship.RelatedAgencyAKID = EXP_GetAKIDs.lkp_RelatedAgencyAKID AND LKP_ILExistingAgencyRelationship.SourceSystemID = EXP_GetAKIDs.SourceSystemID
),
FIL_insert AS (
	SELECT
	lkp_AgencyRelationshipAKID, 
	changed_flag, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemID, 
	CreatedDate, 
	ModifiedDate, 
	o_NewHashKey AS HashKey, 
	AgencyAKID, 
	RelatedAgencyAKID, 
	RelationshipType, 
	AgencyRelationshipEffectiveDate, 
	AgencyRelationshipExpirationDate
	FROM EXP_Detect_Changes
	WHERE changed_flag='NEW'or changed_flag='UPDATE'
),
SEQ_AgencyRelationship_AKID AS (
	CREATE SEQUENCE SEQ_AgencyRelationship_AKID
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
	lkp_AgencyRelationshipAKID,
	SEQ_AgencyRelationship_AKID.NEXTVAL,
	-- *INF*: iif(isnull(lkp_AgencyRelationshipAKID),NEXTVAL,lkp_AgencyRelationshipAKID)
	IFF(lkp_AgencyRelationshipAKID IS NULL, NEXTVAL, lkp_AgencyRelationshipAKID) AS AgencyRelationshipAKID,
	0 AS Default_Int,
	'N/A' AS Default_char,
	HashKey,
	AgencyAKID,
	RelatedAgencyAKID,
	RelationshipType,
	AgencyRelationshipEffectiveDate,
	AgencyRelationshipExpirationDate
	FROM FIL_insert
),
AgencyRelationship_Inserts AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyRelationship
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, HashKey, AgencyRelationshipAKID, AgencyAKID, RelatedAgencyAKID, RelationshipType, AgencyRelationshipEffectiveDate, AgencyRelationshipExpirationDate)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	HASHKEY, 
	AGENCYRELATIONSHIPAKID, 
	AGENCYAKID, 
	RELATEDAGENCYAKID, 
	RELATIONSHIPTYPE, 
	AGENCYRELATIONSHIPEFFECTIVEDATE, 
	AGENCYRELATIONSHIPEXPIRATIONDATE
	FROM EXP_Assign_AKID
),
SQ_AgencyRelationship AS (
	SELECT 
		a.AgencyRelationshipID, 
		a.EffectiveDate,
		a.ExpirationDate, 
		a.AgencyRelationshipAKID  
	FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyRelationship a
	WHERE  a.AgencyRelationshipAKID    IN
		( SELECT AgencyRelationshipAKID    FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyRelationship
		WHERE CurrentSnapshotFlag = 1 GROUP BY AgencyRelationshipAKID   HAVING count(*) > 1) 
	ORDER BY a.AgencyRelationshipAKID, a.EffectiveDate DESC
	
	
	
	--IN Subquery exists to pick AK ID column values that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag 
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
),
EXP_Lag_eff_from_date AS (
	SELECT
	AgencyRelationshipID,
	EffectiveDate AS EffectiveFromDate,
	ExpirationDate AS OriginalEffectiveToDate,
	AgencyRelationshipAKID,
	-- *INF*: DECODE(TRUE,
	-- AgencyRelationshipAKID = v_prev_AKID , ADD_TO_DATE(v_prev_EffectiveFromDate,'SS',-1),
	-- OriginalEffectiveToDate)
	DECODE(TRUE,
		AgencyRelationshipAKID = v_prev_AKID, ADD_TO_DATE(v_prev_EffectiveFromDate, 'SS', - 1),
		OriginalEffectiveToDate) AS v_EffectiveToDate,
	v_EffectiveToDate AS o_EffectiveToDate,
	AgencyRelationshipAKID AS v_prev_AKID,
	EffectiveFromDate AS v_prev_EffectiveFromDate,
	0 AS CurrentSnapshotFlag,
	SYSDATE AS ModifiedDate
	FROM SQ_AgencyRelationship
),
FIL_FirstRowInAKGroup AS (
	SELECT
	AgencyRelationshipID, 
	OriginalEffectiveToDate, 
	o_EffectiveToDate AS NewEffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM EXP_Lag_eff_from_date
	WHERE OriginalEffectiveToDate != NewEffectiveToDate
),
UPD_OldRecord AS (
	SELECT
	AgencyRelationshipID, 
	NewEffectiveToDate AS EffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM FIL_FirstRowInAKGroup
),
AgencyRelationship_Updates AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyRelationship AS T
	USING UPD_OldRecord AS S
	ON T.AgencyRelationshipID = S.AgencyRelationshipID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.EffectiveToDate, T.ModifiedDate = S.ModifiedDate
),
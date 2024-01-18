WITH
SQ_UnderwriterAgencyRelationshipStage AS (
	SELECT
		UnderwriterAgencyRelationshipStageID,
		AgencyODSSourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AgencyID,
		AgencyCode,
		AssociateID,
		WestBendAssociateID,
		StrategicProfitCenterCode,
		StrategicProfitCenterDescription AS StrategicProfitCenterDescriptiong,
		AgencyODSRelationshipId,
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemID
	FROM UnderwriterAgencyRelationshipStage
	WHERE rtrim(ltrim(StrategicProfitCenterCode)) <> 'X'
),
LKP_Agency AS (
	SELECT
	in_AgencyCode,
	AgencyAKID,
	AgencyCode
	FROM (
		SELECT 
			in_AgencyCode,
			AgencyAKID,
			AgencyCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Agency
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyCode ORDER BY in_AgencyCode DESC) = 1
),
LKP_StrategicProfitCenter AS (
	SELECT
	in_StrategicProfitCenterCode,
	EnterpriseGroupId,
	InsuranceReferenceLegalEntityId,
	StrategicProfitCenterAKId,
	StrategicProfitCenterCode
	FROM (
		SELECT 
			in_StrategicProfitCenterCode,
			EnterpriseGroupId,
			InsuranceReferenceLegalEntityId,
			StrategicProfitCenterAKId,
			StrategicProfitCenterCode
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StrategicProfitCenterCode ORDER BY in_StrategicProfitCenterCode DESC) = 1
),
lkp_UnderwritingAssociate AS (
	SELECT
	UnderwritingAssociateAKID,
	WestBendAssociateID
	FROM (
		SELECT 
			UnderwritingAssociateAKID,
			WestBendAssociateID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingAssociate
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WestBendAssociateID ORDER BY UnderwritingAssociateAKID DESC) = 1
),
EXP_GetAKIDs AS (
	SELECT
	lkp_UnderwritingAssociate.UnderwritingAssociateAKID AS lkp_UnderwritingAssociateAKID,
	LKP_Agency.AgencyAKID AS lkp_AgencyAKID,
	SQ_UnderwriterAgencyRelationshipStage.StrategicProfitCenterCode,
	SQ_UnderwriterAgencyRelationshipStage.SourceSystemID,
	LKP_StrategicProfitCenter.StrategicProfitCenterAKId,
	SQ_UnderwriterAgencyRelationshipStage.AgencyODSRelationshipId
	FROM SQ_UnderwriterAgencyRelationshipStage
	LEFT JOIN LKP_Agency
	ON LKP_Agency.AgencyCode = SQ_UnderwriterAgencyRelationshipStage.AgencyCode
	LEFT JOIN LKP_StrategicProfitCenter
	ON LKP_StrategicProfitCenter.StrategicProfitCenterCode = SQ_UnderwriterAgencyRelationshipStage.StrategicProfitCenterCode
	LEFT JOIN lkp_UnderwritingAssociate
	ON lkp_UnderwritingAssociate.WestBendAssociateID = SQ_UnderwriterAgencyRelationshipStage.WestBendAssociateID
),
LKP_ExistingRelationship AS (
	SELECT
	in_AgencyODSRelationshipId,
	AgencyODSRelationshipId,
	HashKey,
	UnderwriterAgencyRelationshipAKID
	FROM (
		SELECT 
			in_AgencyODSRelationshipId,
			AgencyODSRelationshipId,
			HashKey,
			UnderwriterAgencyRelationshipAKID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwriterAgencyRelationship
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyODSRelationshipId ORDER BY in_AgencyODSRelationshipId DESC) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_ExistingRelationship.UnderwriterAgencyRelationshipAKID AS lkp_UnderwriterAgencyRelationshipAKID,
	EXP_GetAKIDs.lkp_AgencyAKID AS AgencyAKID,
	EXP_GetAKIDs.lkp_UnderwritingAssociateAKID AS UnderwritingAssociateAKID,
	EXP_GetAKIDs.StrategicProfitCenterCode,
	EXP_GetAKIDs.StrategicProfitCenterAKId,
	EXP_GetAKIDs.AgencyODSRelationshipId,
	-- *INF*: MD5(to_char(AgencyAKID) || '&'|| to_char(UnderwritingAssociateAKID) || '&'|| to_char(StrategicProfitCenterAKId))
	MD5(to_char(AgencyAKID) || '&' || to_char(UnderwritingAssociateAKID) || '&' || to_char(StrategicProfitCenterAKId)) AS v_NewHashKey,
	v_NewHashKey AS o_NewHashKey,
	LKP_ExistingRelationship.HashKey AS lkp_HashKey,
	-- *INF*: IIF(ISNULL(lkp_UnderwriterAgencyRelationshipAKID), 'NEW', 
	-- IIF((v_NewHashKey <> lkp_HashKey), 'UPDATE', 'NOCHANGE'))
	IFF(
	    lkp_UnderwriterAgencyRelationshipAKID IS NULL, 'NEW',
	    IFF(
	        (v_NewHashKey <> lkp_HashKey), 'UPDATE', 'NOCHANGE'
	    )
	) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: iif(v_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(
	    v_changed_flag = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	EXP_GetAKIDs.SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate
	FROM EXP_GetAKIDs
	LEFT JOIN LKP_ExistingRelationship
	ON LKP_ExistingRelationship.AgencyODSRelationshipId = EXP_GetAKIDs.AgencyODSRelationshipId
),
FIL_insert AS (
	SELECT
	lkp_UnderwriterAgencyRelationshipAKID AS lkp_UnderwriterRelationshipAKID, 
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
	UnderwritingAssociateAKID, 
	StrategicProfitCenterAKId, 
	AgencyODSRelationshipId
	FROM EXP_Detect_Changes
	WHERE changed_flag='NEW'or changed_flag='UPDATE'
),
SEQ_UnderwriterAgencyRelationship_AKID AS (
	CREATE SEQUENCE SEQ_UnderwriterAgencyRelationship_AKID
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
	HashKey,
	lkp_UnderwriterRelationshipAKID,
	SEQ_UnderwriterAgencyRelationship_AKID.NEXTVAL,
	-- *INF*: iif(isnull(lkp_UnderwriterRelationshipAKID),NEXTVAL,lkp_UnderwriterRelationshipAKID)
	IFF(lkp_UnderwriterRelationshipAKID IS NULL, NEXTVAL, lkp_UnderwriterRelationshipAKID) AS UnderwriterRelationshipAKID,
	AgencyAKID,
	UnderwritingAssociateAKID,
	StrategicProfitCenterAKId,
	AgencyODSRelationshipId
	FROM FIL_insert
),
UnderwriterAgencyRelationship_Inserts AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwriterAgencyRelationship;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwriterAgencyRelationship
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, HashKey, UnderwriterAgencyRelationshipAKId, AgencyAKID, UnderwritingAssociateAKID, StrategicProfitCenterAKId, AgencyODSRelationshipId)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	HASHKEY, 
	UnderwriterRelationshipAKID AS UNDERWRITERAGENCYRELATIONSHIPAKID, 
	AGENCYAKID, 
	UNDERWRITINGASSOCIATEAKID, 
	STRATEGICPROFITCENTERAKID, 
	AGENCYODSRELATIONSHIPID
	FROM EXP_Assign_AKID
),
SQ_UnderwriterAgencyRelationship AS (
	SELECT 
		a.UnderwriterAgencyRelationshipID, 
		a.EffectiveDate,
		a.ExpirationDate, 
		a.UnderwriterAgencyRelationshipAKID
	FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwriterAgencyRelationship a
	WHERE  a.UnderwriterAgencyRelationshipAKID  IN
		( SELECT UnderwriterAgencyRelationshipAKID  FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwriterAgencyRelationship
		WHERE CurrentSnapshotFlag = 1 GROUP BY UnderwriterAgencyRelationshipAKID HAVING count(*) > 1) 
	ORDER BY a.UnderwriterAgencyRelationshipAKID ,a.EffectiveDate DESC
	
	
	
	--IN Subquery exists to pick AK ID column values that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag 
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
),
EXP_Lag_eff_from_date AS (
	SELECT
	UnderwriterAgencyRelationshipId AS UnderwriterAgencyRelationshipID,
	EffectiveDate AS EffectiveFromDate,
	ExpirationDate AS OriginalEffectiveToDate,
	UnderwriterAgencyRelationshipAKId AS UnderwriterAgencyRelationshipAKID,
	-- *INF*: DECODE(TRUE,
	-- UnderwriterAgencyRelationshipAKID = v_prev_AKID , ADD_TO_DATE(v_prev_EffectiveFromDate,'SS',-1),
	-- OriginalEffectiveToDate)
	DECODE(
	    TRUE,
	    UnderwriterAgencyRelationshipAKID = v_prev_AKID, DATEADD(SECOND,- 1,v_prev_EffectiveFromDate),
	    OriginalEffectiveToDate
	) AS v_EffectiveToDate,
	v_EffectiveToDate AS o_EffectiveToDate,
	UnderwriterAgencyRelationshipAKID AS v_prev_AKID,
	EffectiveFromDate AS v_prev_EffectiveFromDate,
	0 AS CurrentSnapshotFlag,
	SYSDATE AS ModifiedDate
	FROM SQ_UnderwriterAgencyRelationship
),
FIL_FirstRowInAKGroup AS (
	SELECT
	UnderwriterAgencyRelationshipID, 
	OriginalEffectiveToDate, 
	o_EffectiveToDate AS NewEffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM EXP_Lag_eff_from_date
	WHERE OriginalEffectiveToDate != NewEffectiveToDate
),
UPD_OldRecord AS (
	SELECT
	UnderwriterAgencyRelationshipID, 
	NewEffectiveToDate AS EffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM FIL_FirstRowInAKGroup
),
UnderwriterAgencyRelationship_Expire AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwriterAgencyRelationship AS T
	USING UPD_OldRecord AS S
	ON T.UnderwriterAgencyRelationshipId = S.UnderwriterAgencyRelationshipID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.EffectiveToDate, T.ModifiedDate = S.ModifiedDate
),
WITH
LKP_GetAgencyCodes AS (
	SELECT
	AgencyCode,
	EDWAgencyAKID
	FROM (
		SELECT 
			AgencyCode,
			EDWAgencyAKID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V3}.AgencyDim
		WHERE currentsnapshotflag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWAgencyAKID ORDER BY AgencyCode) = 1
),
SQ_AgencyRelationship AS (
	SELECT AgencyRelationship.AgencyRelationshipID, AgencyRelationship.CurrentSnapshotFlag, AgencyRelationship.AuditID, AgencyRelationship.EffectiveDate, AgencyRelationship.ExpirationDate, AgencyRelationship.SourceSystemID, AgencyRelationship.CreatedDate, AgencyRelationship.ModifiedDate, AgencyRelationship.HashKey, AgencyRelationship.AgencyRelationshipAKID, AgencyRelationship.AgencyAKID, AgencyRelationship.RelatedAgencyAKID, AgencyRelationship.RelationshipType, AgencyRelationship.AgencyRelationshipEffectiveDate, AgencyRelationship.AgencyRelationshipExpirationDate 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.AgencyRelationship AgencyRelationship
	where 
	AgencyRelationship.CreatedDate > '@{pipeline().parameters.SELECTION_START_TS}'
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_AgencyRelationship AS (
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
	-- *INF*: :LKP.LKP_GETAGENCYCODES(AgencyAKID)
	LKP_GETAGENCYCODES_AgencyAKID.AgencyCode AS AgencyCode,
	-- *INF*: :LKP.LKP_GETAGENCYCODES(RelatedAgencyAKID)
	LKP_GETAGENCYCODES_RelatedAgencyAKID.AgencyCode AS RelatedAgencyCode
	FROM SQ_AgencyRelationship
	LEFT JOIN LKP_GETAGENCYCODES LKP_GETAGENCYCODES_AgencyAKID
	ON LKP_GETAGENCYCODES_AgencyAKID.EDWAgencyAKID = AgencyAKID

	LEFT JOIN LKP_GETAGENCYCODES LKP_GETAGENCYCODES_RelatedAgencyAKID
	ON LKP_GETAGENCYCODES_RelatedAgencyAKID.EDWAgencyAKID = RelatedAgencyAKID

),
EXP_GetAKIDs AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	AgencyRelationshipAKID,
	RelationshipType,
	AgencyRelationshipEffectiveDate,
	AgencyRelationshipExpirationDate,
	RelatedAgencyAKID AS RelatedEDWAgencyAKID,
	AgencyAKID AS EDWAgencyAKID,
	AgencyCode AS Agencycode,
	RelatedAgencyCode AS Legalprimarycode
	FROM EXP_AgencyRelationship
),
LKP_Agencyrelationshipdim AS (
	SELECT
	EDWAgencyRelationshipAKId,
	AgencyRelationshipDimHashKey,
	EDWAgencyAKId,
	EDWLegalPrimaryAgencyAKId
	FROM (
		SELECT 
			EDWAgencyRelationshipAKId,
			AgencyRelationshipDimHashKey,
			EDWAgencyAKId,
			EDWLegalPrimaryAgencyAKId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyRelationshipDim
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWAgencyAKId,EDWLegalPrimaryAgencyAKId ORDER BY EDWAgencyRelationshipAKId) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_Agencyrelationshipdim.EDWAgencyRelationshipAKId AS lkp_EDWAgencyRelationshipAKId,
	LKP_Agencyrelationshipdim.AgencyRelationshipDimHashKey AS lkp_AgencyRelationshipDimHashKey,
	EXP_GetAKIDs.RelatedEDWAgencyAKID,
	EXP_GetAKIDs.AgencyRelationshipAKID,
	EXP_GetAKIDs.EDWAgencyAKID,
	EXP_GetAKIDs.RelationshipType,
	EXP_GetAKIDs.AgencyRelationshipEffectiveDate,
	EXP_GetAKIDs.AgencyRelationshipExpirationDate,
	EXP_GetAKIDs.AuditID,
	EXP_GetAKIDs.Agencycode,
	EXP_GetAKIDs.Legalprimarycode,
	-- *INF*: MD5(EDWAgencyAKID||RelatedEDWAgencyAKID||RelationshipType||to_char(AgencyRelationshipEffectiveDate)||to_char(AgencyRelationshipExpirationDate))
	MD5(EDWAgencyAKID || RelatedEDWAgencyAKID || RelationshipType || to_char(AgencyRelationshipEffectiveDate) || to_char(AgencyRelationshipExpirationDate)) AS v_NewHashKey,
	v_NewHashKey AS o_NewHashKey,
	-- *INF*: IIF(ISNULL(lkp_EDWAgencyRelationshipAKId), 'NEW', 
	-- IIF((v_NewHashKey <> lkp_AgencyRelationshipDimHashKey), 'UPDATE', 'NOCHANGE'))
	IFF(
	    lkp_EDWAgencyRelationshipAKId IS NULL, 'NEW',
	    IFF(
	        (v_NewHashKey <> lkp_AgencyRelationshipDimHashKey), 'UPDATE', 'NOCHANGE'
	    )
	) AS v_changed_flag,
	v_changed_flag AS o_changed_flag,
	1 AS CurrentSnapshotFlag,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModiifiedDate,
	-- *INF*: iif(v_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(
	    v_changed_flag = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate
	FROM EXP_GetAKIDs
	LEFT JOIN LKP_Agencyrelationshipdim
	ON LKP_Agencyrelationshipdim.EDWAgencyAKId = EXP_GetAKIDs.EDWAgencyAKID AND LKP_Agencyrelationshipdim.EDWLegalPrimaryAgencyAKId = EXP_GetAKIDs.RelatedEDWAgencyAKID
),
FLT_Agencyrelationshipdim AS (
	SELECT
	o_changed_flag, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	CreatedDate, 
	ModiifiedDate, 
	AgencyRelationshipEffectiveDate, 
	AgencyRelationshipExpirationDate, 
	o_NewHashKey AS AgencyRelationShipDimHashKey, 
	AgencyRelationshipAKID, 
	EDWAgencyAKID AS AgencyAKID, 
	RelatedEDWAgencyAKID AS RelatedAgencyAKID, 
	RelationshipType, 
	Agencycode AS o_Agencycode, 
	Legalprimarycode AS o_Legalprimarycode, 
	lkp_EDWAgencyRelationshipAKId
	FROM EXP_Detect_Changes
	WHERE o_changed_flag='NEW'or o_changed_flag='UPDATE'
),
EXP_Assign_AKID AS (
	SELECT
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	CreatedDate AS CreateDate,
	ModiifiedDate AS ModifiedDate,
	AgencyAKID AS EDWAgencyAKId,
	RelatedAgencyAKID AS EDWLegalPrimaryAgencyAKId,
	AgencyRelationShipDimHashKey AS AgencyRelationshipDimHashKey,
	o_Agencycode AS AgencyCode,
	o_Legalprimarycode AS LegalPrimaryAgencyCode,
	AgencyRelationshipEffectiveDate,
	AgencyRelationshipExpirationDate,
	RelationshipType,
	lkp_EDWAgencyRelationshipAKId,
	AgencyRelationshipAKID
	FROM FLT_Agencyrelationshipdim
),
AgencyRelationshipDim_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.Shortcut_to_AgencyRelationshipDim
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, CreateDate, ModifiedDate, EDWAgencyAKId, EDWLegalPrimaryAgencyAKId, EDWAgencyRelationshipAKId, AgencyRelationshipDimHashKey, AgencyCode, LegalPrimaryAgencyCode, AgencyRelationshipEffectiveDate, AgencyRelationshipExpirationDate, RelationshipType)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CREATEDATE, 
	MODIFIEDDATE, 
	EDWAGENCYAKID, 
	EDWLEGALPRIMARYAGENCYAKID, 
	AgencyRelationshipAKID AS EDWAGENCYRELATIONSHIPAKID, 
	AGENCYRELATIONSHIPDIMHASHKEY, 
	AGENCYCODE, 
	LEGALPRIMARYAGENCYCODE, 
	AGENCYRELATIONSHIPEFFECTIVEDATE, 
	AGENCYRELATIONSHIPEXPIRATIONDATE, 
	RELATIONSHIPTYPE
	FROM EXP_Assign_AKID
),
SQ_AgencyRelationshipDim AS (
	SELECT 
		a.AgencyRelationshipdimID, 
		a.EffectiveDate,
		a.ExpirationDate, 
		a.EDWAgencyRelationshipAKId  
	FROM 
	@{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyRelationshipdim a
	WHERE  a.EDWAgencyRelationshipAKId    IN
		( SELECT EDWAgencyRelationshipAKId    FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyRelationshipdim
		WHERE CurrentSnapshotFlag = 1 GROUP BY EDWAgencyRelationshipAKId   HAVING count(*) > 1) 
	ORDER BY a.EDWAgencyRelationshipAKId, a.EffectiveDate DESC
),
EXP_Agencyrelationshipdates AS (
	SELECT
	AgencyRelationshipDimId,
	EffectiveDate AS EffectivefromDate,
	ExpirationDate AS OrginaleffectivetoDate,
	EDWAgencyRelationshipAKId,
	-- *INF*: DECODE(TRUE,
	-- EDWAgencyRelationshipAKId = v_prev_AKID , ADD_TO_DATE(v_prev_Effectivefromdate,'SS',-1),
	-- OrginaleffectivetoDate)
	DECODE(
	    TRUE,
	    EDWAgencyRelationshipAKId = v_prev_AKID, DATEADD(SECOND,- 1,v_prev_Effectivefromdate),
	    OrginaleffectivetoDate
	) AS V_effectivetodate,
	V_effectivetodate AS o_effectivetodate,
	EDWAgencyRelationshipAKId AS v_prev_AKID,
	EffectivefromDate AS v_prev_Effectivefromdate,
	0 AS CurrentSnapshotFlag,
	sysdate AS ModifiedDate
	FROM SQ_AgencyRelationshipDim
),
FLT_Agencyrelationshipdate AS (
	SELECT
	AgencyRelationshipDimId, 
	OrginaleffectivetoDate, 
	o_effectivetodate AS NewEffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM EXP_Agencyrelationshipdates
	WHERE OrginaleffectivetoDate != NewEffectiveToDate
),
UPD_OldRecords AS (
	SELECT
	AgencyRelationshipDimId, 
	NewEffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM FLT_Agencyrelationshipdate
),
AgencyRelationshipDim_Updates AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.Shortcut_to_AgencyRelationshipDim AS T
	USING UPD_OldRecords AS S
	ON T.AgencyRelationshipDimId = S.AgencyRelationshipDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.NewEffectiveToDate, T.ModifiedDate = S.ModifiedDate
),
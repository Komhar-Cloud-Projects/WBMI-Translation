WITH
SQ_UnderwriterProductRelationship AS (
	SELECT 
		a.UnderwriterProductRelationshipID, 
		a.EffectiveDate,
		a.ExpirationDate, 
		a.UnderwriterProductRelationshipAKID
	FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwriterProductRelationship a
	WHERE  a.UnderwriterProductRelationshipAKID  IN
		( SELECT UnderwriterProductRelationshipAKID  FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwriterProductRelationship
		WHERE CurrentSnapshotFlag = 1 GROUP BY UnderwriterProductRelationshipAKID HAVING count(*) > 1) 
	ORDER BY a.UnderwriterProductRelationshipAKID ,a.EffectiveDate DESC
	
	
	
	--IN Subquery exists to pick AK ID column values that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag 
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
),
EXP_Lag_eff_from_date AS (
	SELECT
	UnderwriterProductRelationshipId,
	EffectiveDate AS EffectiveFromDate,
	ExpirationDate AS OriginalEffectiveToDate,
	UnderwriterProductRelationshipAKId,
	-- *INF*: DECODE(TRUE,
	-- UnderwriterProductRelationshipAKId = v_prev_AKID , ADD_TO_DATE(v_prev_EffectiveFromDate,'SS',-1),
	-- OriginalEffectiveToDate)
	DECODE(TRUE,
		UnderwriterProductRelationshipAKId = v_prev_AKID, DATEADD(SECOND,- 1,v_prev_EffectiveFromDate),
		OriginalEffectiveToDate
	) AS v_EffectiveToDate,
	v_EffectiveToDate AS o_EffectiveToDate,
	UnderwriterProductRelationshipAKId AS v_prev_AKID,
	EffectiveFromDate AS v_prev_EffectiveFromDate,
	0 AS CurrentSnapshotFlag,
	SYSDATE AS ModifiedDate
	FROM SQ_UnderwriterProductRelationship
),
FIL_FirstRowInAKGroup AS (
	SELECT
	UnderwriterProductRelationshipId, 
	OriginalEffectiveToDate, 
	o_EffectiveToDate AS NewEffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM EXP_Lag_eff_from_date
	WHERE OriginalEffectiveToDate != NewEffectiveToDate
),
UPD_OldRecord AS (
	SELECT
	UnderwriterProductRelationshipId AS UnderwriterProductRelationshipID, 
	NewEffectiveToDate AS EffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM FIL_FirstRowInAKGroup
),
UnderwriterProductRelationship_Expire AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwriterProductRelationship AS T
	USING UPD_OldRecord AS S
	ON T.UnderwriterProductRelationshipId = S.UnderwriterProductRelationshipID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.EffectiveToDate, T.ModifiedDate = S.ModifiedDate
),
SQ_UnderwriterProductRelationshipStage AS (
	SELECT
		UnderwriterProductRelationshipStageID,
		AgencyODSSourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AssociateID,
		WestBendAssociateID,
		StrategicProfitCenterCode,
		PolicyOfferingCode,
		ProgramCode,
		PolicyAmountMinimum,
		PolicyAmountMaximum,
		AgencyODSRelationshipId,
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemID,
		InsuranceSegmentCode,
		BondCategory
	FROM UnderwriterProductRelationshipStage
),
LKP_InsuranceSegment AS (
	SELECT
	InsuranceSegmentAKId,
	InsuranceSegmentCode
	FROM (
		SELECT 
			InsuranceSegmentAKId,
			InsuranceSegmentCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceSegment
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceSegmentCode ORDER BY InsuranceSegmentAKId) = 1
),
LKP_PolicyOffering AS (
	SELECT
	PolicyOfferingAKId,
	PolicyOfferingCode
	FROM (
		SELECT 
			PolicyOfferingAKId,
			PolicyOfferingCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyOffering
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyOfferingCode ORDER BY PolicyOfferingAKId DESC) = 1
),
LKP_Program AS (
	SELECT
	in_ProgramCode,
	ProgramAKId,
	ProgramCode
	FROM (
		SELECT 
			in_ProgramCode,
			ProgramAKId,
			ProgramCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.Program
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProgramCode ORDER BY in_ProgramCode DESC) = 1
),
LKP_StrategicProfitCenter AS (
	SELECT
	in_StrategicProfitCenterCode,
	StrategicProfitCenterAKId,
	StrategicProfitCenterCode
	FROM (
		SELECT 
			in_StrategicProfitCenterCode,
			StrategicProfitCenterAKId,
			StrategicProfitCenterCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.StrategicProfitCenter
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
	SQ_UnderwriterProductRelationshipStage.AgencyODSSourceSystemID AS SourceSystemID,
	LKP_StrategicProfitCenter.StrategicProfitCenterAKId,
	LKP_PolicyOffering.PolicyOfferingAKId,
	LKP_Program.ProgramAKId,
	SQ_UnderwriterProductRelationshipStage.WestBendAssociateID,
	SQ_UnderwriterProductRelationshipStage.StrategicProfitCenterCode,
	SQ_UnderwriterProductRelationshipStage.PolicyOfferingCode,
	SQ_UnderwriterProductRelationshipStage.ProgramCode,
	SQ_UnderwriterProductRelationshipStage.PolicyAmountMinimum,
	SQ_UnderwriterProductRelationshipStage.PolicyAmountMaximum,
	SQ_UnderwriterProductRelationshipStage.AgencyODSRelationshipId,
	LKP_InsuranceSegment.InsuranceSegmentAKId,
	SQ_UnderwriterProductRelationshipStage.BondCategory
	FROM SQ_UnderwriterProductRelationshipStage
	LEFT JOIN LKP_InsuranceSegment
	ON LKP_InsuranceSegment.InsuranceSegmentCode = SQ_UnderwriterProductRelationshipStage.InsuranceSegmentCode
	LEFT JOIN LKP_PolicyOffering
	ON LKP_PolicyOffering.PolicyOfferingCode = SQ_UnderwriterProductRelationshipStage.PolicyOfferingCode
	LEFT JOIN LKP_Program
	ON LKP_Program.ProgramCode = SQ_UnderwriterProductRelationshipStage.ProgramCode
	LEFT JOIN LKP_StrategicProfitCenter
	ON LKP_StrategicProfitCenter.StrategicProfitCenterCode = SQ_UnderwriterProductRelationshipStage.StrategicProfitCenterCode
	LEFT JOIN lkp_UnderwritingAssociate
	ON lkp_UnderwritingAssociate.WestBendAssociateID = SQ_UnderwriterProductRelationshipStage.WestBendAssociateID
),
LKP_Existing AS (
	SELECT
	in_AgencyODSRelationshipId,
	UnderwriterProductRelationshipAKId,
	HashKey,
	InsuranceSegmentAKId,
	BondCategory,
	AgencyODSRelationshipId
	FROM (
		SELECT 
			in_AgencyODSRelationshipId,
			UnderwriterProductRelationshipAKId,
			HashKey,
			InsuranceSegmentAKId,
			BondCategory,
			AgencyODSRelationshipId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwriterProductRelationship
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyODSRelationshipId ORDER BY in_AgencyODSRelationshipId DESC) = 1
),
EXP_Detect_Changes AS (
	SELECT
	EXP_GetAKIDs.StrategicProfitCenterAKId,
	EXP_GetAKIDs.PolicyOfferingAKId,
	EXP_GetAKIDs.ProgramAKId,
	EXP_GetAKIDs.InsuranceSegmentAKId AS i_InsuranceSegmentAKId,
	LKP_Existing.InsuranceSegmentAKId AS lkp_InsuranceSegmentAKId,
	LKP_Existing.BondCategory AS lkp_BondCategory,
	LKP_Existing.UnderwriterProductRelationshipAKId AS lkp_UnderwriterProductRelationshipAKId,
	EXP_GetAKIDs.lkp_UnderwritingAssociateAKID AS UnderwritingAssociateAKID,
	EXP_GetAKIDs.PolicyAmountMinimum,
	EXP_GetAKIDs.PolicyAmountMaximum,
	EXP_GetAKIDs.AgencyODSRelationshipId,
	EXP_GetAKIDs.BondCategory,
	LKP_Existing.HashKey AS lkp_HashKey,
	-- *INF*: IIF(ISNULL(i_InsuranceSegmentAKId),-1,i_InsuranceSegmentAKId)
	IFF(i_InsuranceSegmentAKId IS NULL,
		- 1,
		i_InsuranceSegmentAKId
	) AS v_InsuranceSegmentAKId,
	-- *INF*: MD5(to_char(UnderwritingAssociateAKID) || '&' || to_char(StrategicProfitCenterAKId) || '&' || to_char(PolicyOfferingAKId) || '&' || to_char(ProgramAKId) || '&' || to_char(PolicyAmountMinimum) || '&' || to_char(PolicyAmountMaximum))
	MD5(to_char(UnderwritingAssociateAKID
		) || '&' || to_char(StrategicProfitCenterAKId
		) || '&' || to_char(PolicyOfferingAKId
		) || '&' || to_char(ProgramAKId
		) || '&' || to_char(PolicyAmountMinimum
		) || '&' || to_char(PolicyAmountMaximum
		)
	) AS v_NewHashKey,
	-- *INF*: IIF(IsNull(lkp_UnderwriterProductRelationshipAKId), 'Insert', 'Update')
	IFF(lkp_UnderwriterProductRelationshipAKId IS NULL,
		'Insert',
		'Update'
	) AS v_InsertOrUpdate,
	-- *INF*: IIF(IsNull(StrategicProfitCenterAKId), -1, StrategicProfitCenterAKId)
	IFF(StrategicProfitCenterAKId IS NULL,
		- 1,
		StrategicProfitCenterAKId
	) AS o_StrategicProfitCenterAKID,
	-- *INF*: IIF(IsNull(PolicyOfferingAKId), -1, PolicyOfferingAKId)
	IFF(PolicyOfferingAKId IS NULL,
		- 1,
		PolicyOfferingAKId
	) AS o_PolicyOfferingAKID,
	-- *INF*: IIF(IsNull(ProgramAKId), -1, ProgramAKId)
	IFF(ProgramAKId IS NULL,
		- 1,
		ProgramAKId
	) AS o_ProgramAKID,
	v_NewHashKey AS o_NewHashKey,
	-- *INF*: IIF(lkp_HashKey = v_NewHashKey 
	-- AND lkp_InsuranceSegmentAKId=v_InsuranceSegmentAKId
	-- AND lkp_BondCategory=BondCategory, 
	-- 'Ignore', v_InsertOrUpdate)
	IFF(lkp_HashKey = v_NewHashKey 
		AND lkp_InsuranceSegmentAKId = v_InsuranceSegmentAKId 
		AND lkp_BondCategory = BondCategory,
		'Ignore',
		v_InsertOrUpdate
	) AS o_InsertUpdateOrIgnore,
	v_InsuranceSegmentAKId AS o_InsuranceSegmentAKId,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: iif(v_InsertOrUpdate='Insert',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_InsertOrUpdate = 'Insert',
		to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		sysdate
	) AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate
	FROM EXP_GetAKIDs
	LEFT JOIN LKP_Existing
	ON LKP_Existing.AgencyODSRelationshipId = EXP_GetAKIDs.AgencyODSRelationshipId
),
FIL_insert AS (
	SELECT
	lkp_UnderwriterProductRelationshipAKId AS UnderwriterProductRelationshipAKId, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemID, 
	CreatedDate, 
	ModifiedDate, 
	o_NewHashKey AS HashKey, 
	UnderwritingAssociateAKID, 
	o_StrategicProfitCenterAKID AS StrategicProfitCenterAKId, 
	o_PolicyOfferingAKID AS ProductAKId, 
	o_ProgramAKID AS ProgramAKId, 
	PolicyAmountMinimum, 
	PolicyAmountMaximum, 
	o_InsuranceSegmentAKId AS InsuranceSegmentAKId, 
	BondCategory, 
	o_InsertUpdateOrIgnore AS InsertUpdateOrIgnore, 
	AgencyODSRelationshipId
	FROM EXP_Detect_Changes
	WHERE InsertUpdateOrIgnore='Insert'or InsertUpdateOrIgnore='Update'
),
SEQ_UnderwriterProductRelationship_AKID AS (
	CREATE SEQUENCE SEQ_UnderwriterProductRelationship_AKID
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
	UnderwriterProductRelationshipAKId,
	SEQ_UnderwriterProductRelationship_AKID.NEXTVAL,
	-- *INF*: iif(isnull(UnderwriterProductRelationshipAKId),NEXTVAL,UnderwriterProductRelationshipAKId)
	IFF(UnderwriterProductRelationshipAKId IS NULL,
		NEXTVAL,
		UnderwriterProductRelationshipAKId
	) AS o_UnderwriterProductRelationshipAKID,
	UnderwritingAssociateAKID,
	StrategicProfitCenterAKId,
	ProductAKId,
	ProgramAKId,
	PolicyAmountMinimum,
	PolicyAmountMaximum,
	InsuranceSegmentAKId,
	BondCategory,
	AgencyODSRelationshipId
	FROM FIL_insert
),
UnderwriterProductRelationship_Inserts AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwriterProductRelationship;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwriterProductRelationship
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, HashKey, UnderwriterProductRelationshipAKId, UnderwritingAssociateAKID, StrategicProfitCenterAKId, ProgramAKId, PolicyOfferingAKId, PolicyAmountMinimum, PolicyAmountMaximum, AgencyODSRelationshipId, InsuranceSegmentAKId, BondCategory)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	HASHKEY, 
	o_UnderwriterProductRelationshipAKID AS UNDERWRITERPRODUCTRELATIONSHIPAKID, 
	UNDERWRITINGASSOCIATEAKID, 
	STRATEGICPROFITCENTERAKID, 
	PROGRAMAKID, 
	ProductAKId AS POLICYOFFERINGAKID, 
	POLICYAMOUNTMINIMUM, 
	POLICYAMOUNTMAXIMUM, 
	AGENCYODSRELATIONSHIPID, 
	INSURANCESEGMENTAKID, 
	BONDCATEGORY
	FROM EXP_Assign_AKID
),
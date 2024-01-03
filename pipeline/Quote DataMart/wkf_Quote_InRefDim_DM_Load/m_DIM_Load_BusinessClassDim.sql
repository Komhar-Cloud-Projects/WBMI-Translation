WITH
SQ_sup_business_classfication_code AS (
	SELECT
		sup_bus_class_code_id,
		crrnt_snpsht_flag,
		audit_id,
		eff_from_date,
		eff_to_date,
		source_sys_id,
		created_date,
		modified_date,
		bus_class_code,
		bus_class_code_descript,
		StandardBusinessClassCode,
		StandardBusinessClassCodeDescription,
		BusinessSegmentCode,
		BusinessSegmentDescription,
		StrategicBusinessGroupCode,
		StrategicBusinessGroupDescription,
		ArgentBusinessSegmentCode,
		ArgentBusinessSegmentDescription
	FROM sup_business_classification_code
	WHERE sup_business_classification_code.modified_date> '@{pipeline().parameters.SELECTION_START_TS}'
),
LKP_BusinessClassDim AS (
	SELECT
	BusinessClassDimId,
	CurrentSnapshotFlag,
	AuditId,
	EffectiveDate,
	ExpirationDate,
	SourceSystemId,
	CreatedDate,
	ModifiedDate,
	BusinessClassCode,
	BusinessClassDescription,
	BusinessSegmentCode,
	BusinessSegmentDescription,
	StrategicBusinessGroupCode,
	StrategicBusinessGroupDescription,
	ArgentBusinessSegmentCode,
	ArgentBusinessSegmentDescription
	FROM (
		SELECT 
			BusinessClassDimId,
			CurrentSnapshotFlag,
			AuditId,
			EffectiveDate,
			ExpirationDate,
			SourceSystemId,
			CreatedDate,
			ModifiedDate,
			BusinessClassCode,
			BusinessClassDescription,
			BusinessSegmentCode,
			BusinessSegmentDescription,
			StrategicBusinessGroupCode,
			StrategicBusinessGroupDescription,
			ArgentBusinessSegmentCode,
			ArgentBusinessSegmentDescription
		FROM BusinessClassDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY BusinessClassCode,EffectiveDate ORDER BY BusinessClassDimId) = 1
),
EXP_GetValue AS (
	SELECT
	LKP_BusinessClassDim.BusinessClassDimId AS i_BusinessClassDimId,
	-- *INF*: IIF(ISNULL(i_BusinessClassDimId),-1,i_BusinessClassDimId)
	IFF(i_BusinessClassDimId IS NULL, - 1, i_BusinessClassDimId) AS o_BusinessClassDimId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	SQ_sup_business_classfication_code.eff_from_date AS EffectiveDate,
	SQ_sup_business_classfication_code.eff_to_date AS ExpirationDate,
	SQ_sup_business_classfication_code.source_sys_id AS SourceSystemId,
	SQ_sup_business_classfication_code.created_date AS CreatedDate,
	SQ_sup_business_classfication_code.modified_date AS ModifiedDate,
	SQ_sup_business_classfication_code.bus_class_code AS BusinessClassCode,
	SQ_sup_business_classfication_code.bus_class_code_descript AS i_BusinessClassDescription,
	-- *INF*: IIF(ISNULL(i_BusinessClassDescription),'N/A',i_BusinessClassDescription)
	IFF(i_BusinessClassDescription IS NULL, 'N/A', i_BusinessClassDescription) AS o_BusinessClassDescription,
	SQ_sup_business_classfication_code.BusinessSegmentCode AS i_BusinessSegmentCode,
	-- *INF*: iif(isnull(i_BusinessSegmentCode), 'N/A',i_BusinessSegmentCode)
	IFF(i_BusinessSegmentCode IS NULL, 'N/A', i_BusinessSegmentCode) AS o_BusinessSegmentCode,
	SQ_sup_business_classfication_code.BusinessSegmentDescription AS i_BusinessSegmentDescription,
	-- *INF*: iif(isnull(i_BusinessSegmentDescription),'N/A',i_BusinessSegmentDescription)
	IFF(i_BusinessSegmentDescription IS NULL, 'N/A', i_BusinessSegmentDescription) AS o_BusinessSegmentDescription,
	SQ_sup_business_classfication_code.StrategicBusinessGroupCode AS i_StrategicBusinessGroupCode,
	-- *INF*: iif(isnull(i_StrategicBusinessGroupCode),'N/A',i_StrategicBusinessGroupCode)
	IFF(i_StrategicBusinessGroupCode IS NULL, 'N/A', i_StrategicBusinessGroupCode) AS o_StrategicBusinessGroupCode,
	SQ_sup_business_classfication_code.StrategicBusinessGroupDescription AS i_StrategicBusinessGroupDescription,
	-- *INF*: iif(isnull(i_StrategicBusinessGroupDescription),'N/A',i_StrategicBusinessGroupDescription)
	-- 
	IFF(i_StrategicBusinessGroupDescription IS NULL, 'N/A', i_StrategicBusinessGroupDescription) AS o_StrategicBusinessGroupDescription,
	SQ_sup_business_classfication_code.ArgentBusinessSegmentCode AS i_ArgentBusinessSegmentCode,
	-- *INF*: iif(isnull(i_ArgentBusinessSegmentCode),'N/A',i_ArgentBusinessSegmentCode)
	-- 
	IFF(i_ArgentBusinessSegmentCode IS NULL, 'N/A', i_ArgentBusinessSegmentCode) AS o_ArgentBusinessSegmentCode,
	SQ_sup_business_classfication_code.ArgentBusinessSegmentDescription AS i_ArgentBusinessSegmentDescription,
	-- *INF*: iif(isnull(i_ArgentBusinessSegmentDescription),'N/A',i_ArgentBusinessSegmentDescription)
	IFF(i_ArgentBusinessSegmentDescription IS NULL, 'N/A', i_ArgentBusinessSegmentDescription) AS o_ArgentBusinessSegmentDescription,
	LKP_BusinessClassDim.ExpirationDate AS lkp_ExpirationDate,
	LKP_BusinessClassDim.BusinessClassDescription AS lkp_BusinessClassDescription,
	LKP_BusinessClassDim.BusinessSegmentCode AS lkp_BusinessSegmentCode,
	LKP_BusinessClassDim.BusinessSegmentDescription AS lkp_BusinessSegmentDescription,
	LKP_BusinessClassDim.StrategicBusinessGroupCode AS lkp_StrategicBusinessGroupCode,
	LKP_BusinessClassDim.StrategicBusinessGroupDescription AS lkp_StrategicBusinessGroupDescription,
	LKP_BusinessClassDim.ArgentBusinessSegmentCode AS lkp_ArgentBusinessSegmentCode,
	LKP_BusinessClassDim.ArgentBusinessSegmentDescription AS lkp_ArgentBusinessSegmentDescription,
	-- *INF*: DECODE(TRUE,ExpirationDate != lkp_ExpirationDate  OR i_BusinessClassDescription != lkp_BusinessClassDescription  OR i_BusinessSegmentCode!= lkp_BusinessSegmentCode OR i_BusinessSegmentDescription != lkp_BusinessSegmentDescription OR
	-- i_StrategicBusinessGroupCode != lkp_StrategicBusinessGroupCode OR i_StrategicBusinessGroupDescription!=lkp_StrategicBusinessGroupDescription OR
	-- i_ArgentBusinessSegmentCode != lkp_ArgentBusinessSegmentCode OR
	-- i_ArgentBusinessSegmentDescription != lkp_ArgentBusinessSegmentDescription
	-- ,'1','0')
	DECODE(TRUE,
	ExpirationDate != lkp_ExpirationDate OR i_BusinessClassDescription != lkp_BusinessClassDescription OR i_BusinessSegmentCode != lkp_BusinessSegmentCode OR i_BusinessSegmentDescription != lkp_BusinessSegmentDescription OR i_StrategicBusinessGroupCode != lkp_StrategicBusinessGroupCode OR i_StrategicBusinessGroupDescription != lkp_StrategicBusinessGroupDescription OR i_ArgentBusinessSegmentCode != lkp_ArgentBusinessSegmentCode OR i_ArgentBusinessSegmentDescription != lkp_ArgentBusinessSegmentDescription, '1',
	'0') AS v_changeForUpdate,
	-- *INF*: IIF(ISNULL(i_BusinessClassDimId),'New',IIF(v_changeForUpdate='1','Update'))
	IFF(i_BusinessClassDimId IS NULL, 'New', IFF(v_changeForUpdate = '1', 'Update')) AS v_NewFlag,
	v_NewFlag AS o_NewFlag,
	-- *INF*: decode(v_NewFlag,'New',1,'Update',0)
	decode(v_NewFlag,
	'New', 1,
	'Update', 0) AS CurrentSnapshotFlag
	FROM SQ_sup_business_classfication_code
	LEFT JOIN LKP_BusinessClassDim
	ON LKP_BusinessClassDim.BusinessClassCode = SQ_sup_business_classfication_code.bus_class_code AND LKP_BusinessClassDim.EffectiveDate = SQ_sup_business_classfication_code.eff_from_date
),
RTR_NEW AS (
	SELECT
	CurrentSnapshotFlag,
	o_NewFlag AS NewFlag,
	o_BusinessClassDimId AS BusinessClassDimId,
	o_AuditId AS AuditId,
	EffectiveDate,
	ExpirationDate,
	SourceSystemId,
	CreatedDate,
	ModifiedDate,
	BusinessClassCode,
	o_BusinessClassDescription AS BusinessClassDescription,
	o_BusinessSegmentCode AS BusinessSegmentCode,
	o_BusinessSegmentDescription AS BusinessSegmentDescription,
	o_StrategicBusinessGroupCode AS StrategicBusinessGroupCode,
	o_StrategicBusinessGroupDescription AS StrategicBusinessGroupDescription,
	o_ArgentBusinessSegmentCode AS ArgentBusinessSegmentCode,
	o_ArgentBusinessSegmentDescription AS ArgentBusinessSegmentDescription
	FROM EXP_GetValue
),
RTR_NEW_NEW AS (SELECT * FROM RTR_NEW WHERE NewFlag='New'),
RTR_NEW_UPDATE AS (SELECT * FROM RTR_NEW WHERE NewFlag='Update'),
UPD_update AS (
	SELECT
	BusinessClassDimId AS BusinessClassDimId1, 
	CurrentSnapshotFlag AS CurrentSnapShotFlag, 
	ExpirationDate AS ExpirationDate1, 
	ModifiedDate AS ModifiedDate1, 
	BusinessClassDescription AS BusinessClassDescription1, 
	BusinessSegmentCode AS BusinessSegmentCode1, 
	BusinessSegmentDescription AS BusinessSegmentDescription1, 
	StrategicBusinessGroupCode AS StrategicBusinessGroupCode1, 
	StrategicBusinessGroupDescription AS StrategicBusinessGroupDescription1, 
	ArgentBusinessSegmentCode, 
	ArgentBusinessSegmentDescription
	FROM RTR_NEW_UPDATE
),
TGT_Update_BusinessClassDim AS (
	MERGE INTO BusinessClassDim AS T
	USING UPD_update AS S
	ON T.BusinessClassDimId = S.BusinessClassDimId1
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ExpirationDate = S.ExpirationDate1, T.ModifiedDate = S.ModifiedDate1, T.BusinessClassDescription = S.BusinessClassDescription1, T.BusinessSegmentCode = S.BusinessSegmentCode1, T.BusinessSegmentDescription = S.BusinessSegmentDescription1, T.StrategicBusinessGroupCode = S.StrategicBusinessGroupCode1, T.StrategicBusinessGroupDescription = S.StrategicBusinessGroupDescription1, T.ArgentBusinessSegmentCode = S.ArgentBusinessSegmentCode, T.ArgentBusinessSegmentDescription = S.ArgentBusinessSegmentDescription
),
UPD_New AS (
	SELECT
	CurrentSnapshotFlag AS CurrentSnapshotFlag3, 
	AuditId, 
	EffectiveDate AS EffectiveDate1, 
	ExpirationDate AS ExpirationDate1, 
	SourceSystemId AS SourceSystemId1, 
	CreatedDate AS CreatedDate1, 
	ModifiedDate AS ModifiedDate1, 
	BusinessClassCode AS BusinessClassCode1, 
	BusinessClassDescription AS BusinessClassDescription1, 
	BusinessSegmentCode AS BusinessSegmentCode1, 
	BusinessSegmentDescription AS BusinessSegmentDescription1, 
	StrategicBusinessGroupCode AS StrategicBusinessGroupCode1, 
	StrategicBusinessGroupDescription AS StrategicBusinessGroupDescription1, 
	ArgentBusinessSegmentCode AS ArgentBusinessSegmentCode1, 
	ArgentBusinessSegmentDescription AS ArgentBusinessSegmentDescription1
	FROM RTR_NEW_NEW
),
TGT_Insert_BusinessClassDim AS (
	INSERT INTO BusinessClassDim
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, BusinessClassCode, BusinessClassDescription, BusinessSegmentCode, BusinessSegmentDescription, StrategicBusinessGroupCode, StrategicBusinessGroupDescription, ArgentBusinessSegmentCode, ArgentBusinessSegmentDescription)
	SELECT 
	CurrentSnapshotFlag3 AS CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EffectiveDate1 AS EFFECTIVEDATE, 
	ExpirationDate1 AS EXPIRATIONDATE, 
	SourceSystemId1 AS SOURCESYSTEMID, 
	CreatedDate1 AS CREATEDDATE, 
	ModifiedDate1 AS MODIFIEDDATE, 
	BusinessClassCode1 AS BUSINESSCLASSCODE, 
	BusinessClassDescription1 AS BUSINESSCLASSDESCRIPTION, 
	BusinessSegmentCode1 AS BUSINESSSEGMENTCODE, 
	BusinessSegmentDescription1 AS BUSINESSSEGMENTDESCRIPTION, 
	StrategicBusinessGroupCode1 AS STRATEGICBUSINESSGROUPCODE, 
	StrategicBusinessGroupDescription1 AS STRATEGICBUSINESSGROUPDESCRIPTION, 
	ArgentBusinessSegmentCode1 AS ARGENTBUSINESSSEGMENTCODE, 
	ArgentBusinessSegmentDescription1 AS ARGENTBUSINESSSEGMENTDESCRIPTION
	FROM UPD_New
),
SQ_BusinessClassDim AS (
	SELECT 
	a.BusinessClassDimId,
	a.EffectiveDate, 
	'0' CurrentSnapshotFlag,
	a.BusinessClassCode
	
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.BusinessClassDim a
	WHERE a.EffectiveDate not in 
	(SELECT max(EffectiveDate)  FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.BusinessClassDim b
	       WHERE a.BusinessClassCode = b.BusinessClassCode )
),
Exp_SetValue AS (
	SELECT
	BusinessClassDimId,
	EffectiveDate AS i_EffectiveDate,
	0 AS CurrentSnapshotFlag,
	BusinessClassCode AS i_BusinessClassCode,
	SYSDATE AS ModifiedDate
	FROM SQ_BusinessClassDim
),
UPD_SetFlag AS (
	SELECT
	BusinessClassDimId, 
	i_EffectiveDate AS EffectiveDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM Exp_SetValue
),
BusinessClassDim AS (
	MERGE INTO BusinessClassDim AS T
	USING UPD_SetFlag AS S
	ON T.BusinessClassDimId = S.BusinessClassDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ModifiedDate = S.ModifiedDate
),
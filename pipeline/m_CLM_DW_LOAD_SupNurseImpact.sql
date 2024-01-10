WITH
SQ_sup_nurse_impact_stage AS (
	SELECT
		SupNurseImpactStageId,
		impact_type,
		impact_category,
		description,
		created_date,
		created_user_id,
		modified_date,
		modified_user_id,
		expiration_date,
		ExtractDate,
		SourceSystemId
	FROM sup_nurse_impact_stage
),
Exp_Src_Values AS (
	SELECT
	impact_type,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(impact_type),'N/A',
	-- IS_SPACES(impact_type),'N/A',
	-- LENGTH(impact_type)=0,'N/A',
	-- LTRIM(RTRIM(impact_type)))
	DECODE(TRUE,
		impact_type IS NULL, 'N/A',
		IS_SPACES(impact_type), 'N/A',
		LENGTH(impact_type) = 0, 'N/A',
		LTRIM(RTRIM(impact_type))) AS o_impact_type,
	impact_category,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(impact_category),'N/A',
	-- IS_SPACES(impact_category),'N/A',
	-- LENGTH(impact_category)=0,'N/A',
	-- LTRIM(RTRIM(impact_category)))
	DECODE(TRUE,
		impact_category IS NULL, 'N/A',
		IS_SPACES(impact_category), 'N/A',
		LENGTH(impact_category) = 0, 'N/A',
		LTRIM(RTRIM(impact_category))) AS o_impact_category,
	description,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(description),'N/A',
	-- IS_SPACES(description),'N/A',
	-- LENGTH(description)=0,'N/A',
	-- LTRIM(RTRIM(description)))
	DECODE(TRUE,
		description IS NULL, 'N/A',
		IS_SPACES(description), 'N/A',
		LENGTH(description) = 0, 'N/A',
		LTRIM(RTRIM(description))) AS o_description,
	expiration_date,
	-- *INF*: iif(isnull(ltrim(rtrim(expiration_date))),TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS'),expiration_date)
	IFF(ltrim(rtrim(expiration_date)) IS NULL, TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'), expiration_date) AS o_expiration_date
	FROM SQ_sup_nurse_impact_stage
),
LKP_Sup_Nurse_Impact_Target AS (
	SELECT
	NurseImpactId,
	ImpactType,
	ImpactCategory,
	ImpactCategoryExpirationDate,
	Description
	FROM (
		SELECT
		LTRIM(RTRIM(S.ImpactType)) as ImpactType,
		LTRIM(RTRIM(S.ImpactCategory)) as ImpactCategory,
		S.NurseImpactId as NurseImpactId,
		S.ImpactCategoryExpirationDate as ImpactCategoryExpirationDate,
		S.Description as Description 
		
		FROM
		@{pipeline().parameters.TARGET_TABLE_OWNER}.SupNurseImpact S
		where 
		CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ImpactType,ImpactCategory ORDER BY NurseImpactId) = 1
),
EXP_TargetLkp_Detect_Changes AS (
	SELECT
	LKP_Sup_Nurse_Impact_Target.NurseImpactId AS Lkp_NurseImpactId,
	LKP_Sup_Nurse_Impact_Target.ImpactType AS Lkp_impact_type,
	LKP_Sup_Nurse_Impact_Target.ImpactCategory AS Lkp_Impact_category,
	LKP_Sup_Nurse_Impact_Target.ImpactCategoryExpirationDate AS Lkp_ImpactCategoryExpirationDate,
	LKP_Sup_Nurse_Impact_Target.Description AS Lkp_Description,
	Exp_Src_Values.o_impact_type AS impact_type,
	Exp_Src_Values.o_impact_category AS impact_category,
	Exp_Src_Values.o_description AS description,
	-- *INF*: iif(isnull(Lkp_NurseImpactId),'NEW',
	-- 
	--         iif(
	--         LTRIM(RTRIM(Lkp_impact_type)) != LTRIM(RTRIM(impact_type)) 
	-- 
	-- or
	--   
	--         LTRIM(RTRIM(Lkp_Impact_category)) != LTRIM(RTRIM(impact_category))
	-- 
	-- or
	--   
	--         LTRIM(RTRIM(Lkp_ImpactCategoryExpirationDate)) != LTRIM(RTRIM(ImpactCategoryExpirationDate))
	-- 
	-- or
	--   
	--         LTRIM(RTRIM(Lkp_Description)) != LTRIM(RTRIM(description)),
	--         
	--         'UPDATE', 'NOCHANGE')
	--    )
	IFF(Lkp_NurseImpactId IS NULL, 'NEW', IFF(LTRIM(RTRIM(Lkp_impact_type)) != LTRIM(RTRIM(impact_type)) OR LTRIM(RTRIM(Lkp_Impact_category)) != LTRIM(RTRIM(impact_category)) OR LTRIM(RTRIM(Lkp_ImpactCategoryExpirationDate)) != LTRIM(RTRIM(ImpactCategoryExpirationDate)) OR LTRIM(RTRIM(Lkp_Description)) != LTRIM(RTRIM(description)), 'UPDATE', 'NOCHANGE')) AS v_CHANGED_FLAG,
	v_CHANGED_FLAG AS CHANGED_FLAG,
	1 AS CurrentSnapShotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	-- *INF*: iif(v_CHANGED_FLAG='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_CHANGED_FLAG = 'NEW', to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), sysdate) AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemId,
	SYSDATE AS CreateDate,
	SYSDATE AS ModifiedDate,
	Exp_Src_Values.o_expiration_date AS ImpactCategoryExpirationDate
	FROM Exp_Src_Values
	LEFT JOIN LKP_Sup_Nurse_Impact_Target
	ON LKP_Sup_Nurse_Impact_Target.ImpactType = Exp_Src_Values.o_impact_type AND LKP_Sup_Nurse_Impact_Target.ImpactCategory = Exp_Src_Values.o_impact_category
),
FIL_Lkp_Records AS (
	SELECT
	impact_type, 
	impact_category, 
	description, 
	CHANGED_FLAG, 
	CurrentSnapShotFlag, 
	AuditId, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemId, 
	CreateDate, 
	ModifiedDate, 
	ImpactCategoryExpirationDate
	FROM EXP_TargetLkp_Detect_Changes
	WHERE CHANGED_FLAG = 'NEW' or CHANGED_FLAG = 'UPDATE'
),
SupNurseImpact_Insert AS (
	INSERT INTO SupNurseImpact
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, ImpactType, ImpactCategory, Description, ImpactCategoryExpirationDate)
	SELECT 
	CurrentSnapShotFlag AS CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CreateDate AS CREATEDDATE, 
	MODIFIEDDATE, 
	impact_type AS IMPACTTYPE, 
	impact_category AS IMPACTCATEGORY, 
	description AS DESCRIPTION, 
	IMPACTCATEGORYEXPIRATIONDATE
	FROM FIL_Lkp_Records
),
SQ_SupNurseImpact AS (
	SELECT 
	A.NurseImpactId,
	A.ImpactType, 
	A.EffectiveDate, 
	A.ExpirationDate, 
	A.ImpactCategory
	
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.SupNurseImpact A 
	
	WHERE EXISTS 
	      ( 
	SELECT 1
	FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}. SupNurseImpact B
	
	WHERE
	B.CurrentSnapshotFlag= 1
	AND
	A.ImpactType = B.ImpactType
	AND
	A.ImpactCategory= B.ImpactCategory
	            
	GROUP BY 
	B.ImpactType, 
	B.ImpactCategory
	  
	HAVING 
	COUNT(*) > 1
	    )
	
	AND
	A.CurrentSnapshotFlag = 1
	
	ORDER BY 
	A.ImpactType, 
	A.EffectiveDate  DESC
),
EXP_Lag_ExpirationDate AS (
	SELECT
	NurseImpactId,
	ImpactType,
	ImpactCategory,
	EffectiveDate,
	ExpirationDate AS orig_ExpirationDate,
	-- *INF*: DECODE(TRUE,
	-- 	ImpactType= v_prev_row_ImpactType, ADD_TO_DATE(v_prev_EffectiveDate,'SS',-1),
	--        ImpactCategory = v_prev_row_ImpactCategory, ADD_TO_DATE(v_prev_EffectiveDate,'SS',-1),
	-- 	orig_ExpirationDate)
	DECODE(TRUE,
		ImpactType = v_prev_row_ImpactType, ADD_TO_DATE(v_prev_EffectiveDate, 'SS', - 1),
		ImpactCategory = v_prev_row_ImpactCategory, ADD_TO_DATE(v_prev_EffectiveDate, 'SS', - 1),
		orig_ExpirationDate) AS v_ExpirationDate,
	v_ExpirationDate AS o_ExpirationDate,
	ImpactType AS v_prev_row_ImpactType,
	ImpactCategory AS v_prev_row_ImpactCategory,
	EffectiveDate AS v_prev_EffectiveDate,
	0 AS o_CurrentSnapShotFlag,
	SYSDATE AS o_ModifiedDate
	FROM SQ_SupNurseImpact
),
FIL_FirstRowAKId AS (
	SELECT
	NurseImpactId, 
	orig_ExpirationDate, 
	o_ExpirationDate AS ExpirationDate, 
	o_CurrentSnapShotFlag AS CurrentSnapShotFlag, 
	o_ModifiedDate AS ModifiedDate
	FROM EXP_Lag_ExpirationDate
	WHERE orig_ExpirationDate != ExpirationDate
),
UPD_SupNurseImpact AS (
	SELECT
	NurseImpactId, 
	ExpirationDate, 
	CurrentSnapShotFlag, 
	ModifiedDate
	FROM FIL_FirstRowAKId
),
SupNurseImpact_Update AS (
	MERGE INTO SupNurseImpact AS T
	USING UPD_SupNurseImpact AS S
	ON T.NurseImpactId = S.NurseImpactId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapShotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),
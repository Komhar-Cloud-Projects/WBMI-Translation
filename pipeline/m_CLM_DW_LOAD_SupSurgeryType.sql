WITH
SQ_CLAIM_SUPPORT_01_STAGE AS (
	SELECT
	CS.CS01_CODE, CS.CS01_CODE_DES, CS.SOURCE_SYSTEM_ID 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_SUPPORT_01_STAGE CS
	WHERE
	CS.CS01_TABLE_ID = 'W028'
),
EXP_Src_Values AS (
	SELECT
	CS01_CODE,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(CS01_CODE),'N/A',
	-- IS_SPACES(CS01_CODE),'N/A',
	-- LENGTH(CS01_CODE)=0,'N/A',
	-- LTRIM(RTRIM(CS01_CODE)))
	DECODE(TRUE,
		CS01_CODE IS NULL, 'N/A',
		LENGTH(CS01_CODE)>0 AND TRIM(CS01_CODE)='', 'N/A',
		LENGTH(CS01_CODE
		) = 0, 'N/A',
		LTRIM(RTRIM(CS01_CODE
			)
		)
	) AS o_CS01_CODE,
	CS01_CODE_DES,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(CS01_CODE_DES),'N/A',
	-- IS_SPACES(CS01_CODE_DES),'N/A',
	-- LENGTH(CS01_CODE_DES)=0,'N/A',
	-- LTRIM(RTRIM(CS01_CODE_DES)))
	DECODE(TRUE,
		CS01_CODE_DES IS NULL, 'N/A',
		LENGTH(CS01_CODE_DES)>0 AND TRIM(CS01_CODE_DES)='', 'N/A',
		LENGTH(CS01_CODE_DES
		) = 0, 'N/A',
		LTRIM(RTRIM(CS01_CODE_DES
			)
		)
	) AS o_CS01_CODE_DES,
	SOURCE_SYSTEM_ID,
	-- *INF*: ltrim(rtrim(SOURCE_SYSTEM_ID))
	ltrim(rtrim(SOURCE_SYSTEM_ID
		)
	) AS o_SourceSystemId
	FROM SQ_CLAIM_SUPPORT_01_STAGE
),
LKP_SupSurgeryType AS (
	SELECT
	SupSurgeryTypeId,
	SurgeryTypeCode,
	SurgeryTypeDescription
	FROM (
		SELECT
		SupSurgeryType.SupSurgeryTypeId as SupSurgeryTypeId, SupSurgeryType.SurgeryTypeDescription as SurgeryTypeDescription, SupSurgeryType.SurgeryTypeCode as SurgeryTypeCode FROM
		 @{pipeline().parameters.TARGET_TABLE_OWNER}.SupSurgeryType 
		
		where 
		CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SurgeryTypeCode ORDER BY SupSurgeryTypeId) = 1
),
EXP_TargetLkp_Detect_Changes AS (
	SELECT
	LKP_SupSurgeryType.SupSurgeryTypeId AS Lkp_SupSurgeryTypeId,
	LKP_SupSurgeryType.SurgeryTypeCode AS Lkp_SurgeryTypeCode,
	LKP_SupSurgeryType.SurgeryTypeDescription AS Lkp_SurgeryTypeDescription,
	EXP_Src_Values.o_CS01_CODE AS CS01_CODE,
	EXP_Src_Values.o_CS01_CODE_DES AS CS01_CODE_DES,
	-- *INF*: iif(isnull(Lkp_SupSurgeryTypeId),'NEW',
	-- 
	--         iif(
	-- 
	--         LTRIM(RTRIM(CS01_CODE)) != LTRIM(RTRIM(Lkp_SurgeryTypeCode)) 
	-- 
	-- or
	--   
	--         LTRIM(RTRIM(CS01_CODE_DES)) != LTRIM(RTRIM(Lkp_SurgeryTypeDescription)),
	-- 
	--        'UPDATE', 'NOCHANGE')
	-- 
	--    )
	--   
	IFF(Lkp_SupSurgeryTypeId IS NULL,
		'NEW',
		IFF(LTRIM(RTRIM(CS01_CODE
				)
			) != LTRIM(RTRIM(Lkp_SurgeryTypeCode
				)
			) 
			OR LTRIM(RTRIM(CS01_CODE_DES
				)
			) != LTRIM(RTRIM(Lkp_SurgeryTypeDescription
				)
			),
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_ChangedFlag,
	v_ChangedFlag AS ChangeFlag,
	-- *INF*: iif(v_ChangedFlag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_ChangedFlag = 'NEW',
		to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		sysdate
	) AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS ExpirationDate,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	1 AS CurrentSnapshotFlag,
	EXP_Src_Values.o_SourceSystemId AS SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM EXP_Src_Values
	LEFT JOIN LKP_SupSurgeryType
	ON LKP_SupSurgeryType.SurgeryTypeCode = EXP_Src_Values.o_CS01_CODE
),
FIL_Lkp_Target AS (
	SELECT
	CS01_CODE AS o_CSO1_CODE, 
	CS01_CODE_DES AS o_CS01_CODE_DES, 
	ChangeFlag, 
	EffectiveDate, 
	ExpirationDate, 
	CreatedDate, 
	ModifiedDate, 
	CurrentSnapshotFlag, 
	SourceSystemId, 
	AuditId
	FROM EXP_TargetLkp_Detect_Changes
	WHERE ChangeFlag =  'NEW'  or ChangeFlag = 'UPDATE'
),
SupSurgeryType_Insert AS (
	INSERT INTO SupSurgeryType
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, SurgeryTypeCode, SurgeryTypeDescription)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	o_CSO1_CODE AS SURGERYTYPECODE, 
	o_CS01_CODE_DES AS SURGERYTYPEDESCRIPTION
	FROM FIL_Lkp_Target
),
SQ_SupSurgeryType AS (
	SELECT 
	A.SupSurgeryTypeId,
	A.EffectiveDate,
	A.SurgeryTypeCode,
	A.SurgeryTypeDescription 
	
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.SupSurgeryType A
	
	WHERE EXISTS 
	    ( 
	SELECT 1
	FROM  
	@{pipeline().parameters.TARGET_TABLE_OWNER}.SupSurgeryType B
	
	where		
	B.CurrentSnapshotFlag= 1
	AND 
	A.SurgeryTypeCode = B.SurgeryTypeCode
	            
	GROUP BY 
	B.SurgeryTypeCode
	
	HAVING 
	COUNT(*) > 1
	    )
	
	AND
	A.CurrentSnapshotFlag = 1
	
	ORDER BY 
	A.SurgeryTypeCode , 
	A.EffectiveDate  DESC
),
EXP_Lag_ExpirationDate AS (
	SELECT
	SupSurgeryTypeId,
	0 AS CurrentSnapshotFlag,
	EffectiveDate,
	-- *INF*: DECODE(TRUE,
	-- 	SurgeryTypeCode =
	-- v_PREV_ROW_SurgeryTypeCode, ADD_TO_DATE(v_PREV_ROW_EffectiveDate,'SS',-1), 
	-- --SurgeryTypeDescription=
	-- --v_PREV_ROW_SurgeryTypeDescription, ADD_TO_DATE(v_PREV_ROW_EffectiveDate,'SS',-1),
	-- Orig_ExpirationDate)
	DECODE(TRUE,
		SurgeryTypeCode = v_PREV_ROW_SurgeryTypeCode, DATEADD(SECOND,- 1,v_PREV_ROW_EffectiveDate),
		Orig_ExpirationDate
	) AS v_ExpirationDate,
	v_ExpirationDate AS ExpirationDate,
	v_PREV_ROW_EffectiveDate,
	v_PREV_ROW_SurgeryTypeCode,
	v_PREV_ROW_SurgeryTypeDescription,
	sysdate AS ModifiedDate,
	SurgeryTypeCode,
	SurgeryTypeDescription
	FROM SQ_SupSurgeryType
),
FIL_FirstRowAkId AS (
	SELECT
	SupSurgeryTypeId, 
	CurrentSnapshotFlag, 
	Orig_ExpirationDate, 
	ExpirationDate, 
	ModifiedDate
	FROM EXP_Lag_ExpirationDate
	WHERE Orig_ExpirationDate != ExpirationDate
),
UPD_SupSurgeryType AS (
	SELECT
	SupSurgeryTypeId, 
	CurrentSnapshotFlag, 
	ExpirationDate, 
	ModifiedDate
	FROM FIL_FirstRowAkId
),
SupSurgeryType_Update AS (
	MERGE INTO SupSurgeryType AS T
	USING UPD_SupSurgeryType AS S
	ON T.SupSurgeryTypeId = S.SupSurgeryTypeId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),
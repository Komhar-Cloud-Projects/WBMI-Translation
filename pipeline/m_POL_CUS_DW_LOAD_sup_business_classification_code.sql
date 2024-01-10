WITH
SQ_BCCBusinessSegmentSBG AS (
	SELECT
		BCCBusinessSegmentSBGId,
		ModifiedUserId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		BusinessClassificationCode,
		BusinessClassificationDescription,
		BusinessSegmentCode,
		BusinessSegmentDescription,
		StrategicBusinessGroupCode,
		StrategicBusinessGroupDescription,
		ArgentBusinessSegmentCode,
		ArgentBusinessSegmentDescription
	FROM BCCBusinessSegmentSBG
	WHERE BCCBusinessSegmentSBG.ModifiedDate>'@{pipeline().parameters.SELECTION_START_TS}'
),
Exp_BCCBusinessSegmentSBG AS (
	SELECT
	BCCBusinessSegmentSBGId,
	ModifiedUserId,
	ModifiedDate,
	EffectiveDate,
	ExpirationDate,
	-- *INF*: to_date(to_char(ExpirationDate, 'YYYY-MM-DD HH:MI:SS'), 'YYYY-MM-DD HH:MI:SS')
	to_date(to_char(ExpirationDate, 'YYYY-MM-DD HH:MI:SS'
		), 'YYYY-MM-DD HH:MI:SS'
	) AS o_ExpirationDate,
	BusinessClassificationCode,
	-- *INF*: LPAD(BusinessClassificationCode,5,'0')
	LPAD(BusinessClassificationCode, 5, '0'
	) AS o_BusinessClassificationCode,
	BusinessClassificationDescription,
	BusinessSegmentCode,
	BusinessSegmentDescription,
	StrategicBusinessGroupCode,
	StrategicBusinessGroupDescription,
	ArgentBusinessSegmentCode,
	ArgentBusinessSegmentDescription
	FROM SQ_BCCBusinessSegmentSBG
),
AGG_RemoveDuplicate AS (
	SELECT
	BCCBusinessSegmentSBGId,
	ModifiedUserId,
	ModifiedDate,
	EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_BusinessClassificationCode AS BusinessClassificationCode,
	BusinessClassificationDescription,
	BusinessSegmentCode,
	BusinessSegmentDescription,
	StrategicBusinessGroupCode,
	StrategicBusinessGroupDescription,
	ArgentBusinessSegmentCode,
	ArgentBusinessSegmentDescription
	FROM Exp_BCCBusinessSegmentSBG
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EffectiveDate, BusinessClassificationCode ORDER BY NULL) = 1
),
LKP_Target AS (
	SELECT
	in_EffectiveDate,
	in_BusinessClassificationCode,
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
	FROM (
		SELECT 
			in_EffectiveDate,
			in_BusinessClassificationCode,
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
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY eff_from_date,bus_class_code ORDER BY in_EffectiveDate) = 1
),
Exp_GetValue AS (
	SELECT
	LKP_Target.sup_bus_class_code_id,
	'1' AS o_CurrenctSnapShotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS Source_sys_id,
	LKP_Target.created_date AS i_created_date,
	-- *INF*: IIF(ISNULL(i_created_date), SYSDATE, i_created_date)
	IFF(i_created_date IS NULL,
		SYSDATE,
		i_created_date
	) AS CreatedDate,
	SYSDATE AS ModifiedDate,
	AGG_RemoveDuplicate.EffectiveDate,
	AGG_RemoveDuplicate.ExpirationDate,
	AGG_RemoveDuplicate.BusinessClassificationCode,
	AGG_RemoveDuplicate.BusinessClassificationDescription AS i_BusinessClassificationDescription,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_BusinessClassificationDescription)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_BusinessClassificationDescription
	) AS o_BusinessClassificationDescription,
	AGG_RemoveDuplicate.BusinessSegmentCode AS i_BusinessSegmentCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_BusinessSegmentCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_BusinessSegmentCode
	) AS o_BusinessSegmentCode,
	AGG_RemoveDuplicate.BusinessSegmentDescription AS i_BusinessSegmentDescription,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_BusinessSegmentDescription)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_BusinessSegmentDescription
	) AS o_BusinessSegmentDescription,
	AGG_RemoveDuplicate.StrategicBusinessGroupCode AS i_StrategicBusinessGroupCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_StrategicBusinessGroupCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_StrategicBusinessGroupCode
	) AS o_StrategicBusinessGroupCode,
	AGG_RemoveDuplicate.StrategicBusinessGroupDescription AS i_StrategicBusinessGroupDescription,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_StrategicBusinessGroupDescription)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_StrategicBusinessGroupDescription
	) AS o_StrategicBusinessGroupDescription,
	AGG_RemoveDuplicate.ArgentBusinessSegmentCode AS i_ArgentBusinessSegmentCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ArgentBusinessSegmentCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_ArgentBusinessSegmentCode
	) AS o_ArgentBusinessSegmentCode,
	AGG_RemoveDuplicate.ArgentBusinessSegmentDescription AS i_ArgentBusinessSegmentDescription,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ArgentBusinessSegmentDescription)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_ArgentBusinessSegmentDescription
	) AS o_ArgentBusinessSegmentDescription,
	LKP_Target.eff_to_date AS lkp_eff_to_date,
	LKP_Target.bus_class_code_descript AS lkp_bus_class_code_descript,
	LKP_Target.BusinessSegmentCode AS lkp_BusinessSegmentCode,
	LKP_Target.BusinessSegmentDescription AS lkp_BusinessSegmentDescription,
	LKP_Target.StrategicBusinessGroupCode AS lkp_StrategicBusinessGroupCode,
	LKP_Target.StrategicBusinessGroupDescription AS lkp_StrategicBusinessGroupDescription,
	LKP_Target.ArgentBusinessSegmentCode AS lkp_ArgentBusinessSegmentCode,
	LKP_Target.ArgentBusinessSegmentDescription AS lkp_ArgentBusinessSegmentDescription,
	-- *INF*: DECODE(TRUE,ExpirationDate != lkp_eff_to_date OR i_BusinessClassificationDescription != lkp_bus_class_code_descript OR i_BusinessSegmentCode!= lkp_BusinessSegmentCode OR i_BusinessSegmentDescription != lkp_BusinessSegmentDescription OR
	-- i_StrategicBusinessGroupCode != lkp_StrategicBusinessGroupCode OR i_StrategicBusinessGroupDescription!=lkp_StrategicBusinessGroupDescription OR
	-- i_ArgentBusinessSegmentCode != lkp_ArgentBusinessSegmentCode OR
	-- i_ArgentBusinessSegmentDescription != lkp_ArgentBusinessSegmentDescription
	-- ,'1','0')
	DECODE(TRUE,
		ExpirationDate != lkp_eff_to_date 
		OR i_BusinessClassificationDescription != lkp_bus_class_code_descript 
		OR i_BusinessSegmentCode != lkp_BusinessSegmentCode 
		OR i_BusinessSegmentDescription != lkp_BusinessSegmentDescription 
		OR i_StrategicBusinessGroupCode != lkp_StrategicBusinessGroupCode 
		OR i_StrategicBusinessGroupDescription != lkp_StrategicBusinessGroupDescription 
		OR i_ArgentBusinessSegmentCode != lkp_ArgentBusinessSegmentCode 
		OR i_ArgentBusinessSegmentDescription != lkp_ArgentBusinessSegmentDescription, '1',
		'0'
	) AS v_ChangeForUpdate,
	-- *INF*: IIF(ISNULL(sup_bus_class_code_id),'NEW',IIF(v_ChangeForUpdate='1','UPDATE'))
	IFF(sup_bus_class_code_id IS NULL,
		'NEW',
		IFF(v_ChangeForUpdate = '1',
			'UPDATE'
		)
	) AS v_ChangeFlag,
	v_ChangeFlag AS o_ChangeFlag
	FROM AGG_RemoveDuplicate
	LEFT JOIN LKP_Target
	ON LKP_Target.eff_from_date = AGG_RemoveDuplicate.EffectiveDate AND LKP_Target.bus_class_code = AGG_RemoveDuplicate.BusinessClassificationCode
),
RTR_Insert_Update AS (
	SELECT
	AuditId,
	sup_bus_class_code_id,
	o_CurrenctSnapShotFlag,
	Source_sys_id,
	CreatedDate AS Created_Date,
	ModifiedDate AS Modified_Date,
	EffectiveDate,
	ExpirationDate AS ExpriationDate,
	BusinessClassificationCode AS bus_class_code,
	i_BusinessClassificationDescription AS bus_class_code_descript,
	BusinessClassificationCode AS StandardBusinessClassCode,
	o_BusinessClassificationDescription AS StandardBusinessClassCodeDescription,
	o_BusinessSegmentCode AS BusinessSegmentCode,
	o_BusinessSegmentDescription AS BusinessSegmentDescription,
	o_StrategicBusinessGroupCode AS StrategicBusinessGroupCode,
	o_StrategicBusinessGroupDescription AS StrategicBusinessGroupDescription,
	o_ArgentBusinessSegmentCode AS ArgentBusinessSegmentCode,
	o_ArgentBusinessSegmentDescription AS ArgentBusinessSegmentDescription,
	o_ChangeFlag
	FROM Exp_GetValue
),
RTR_Insert_Update_Insert AS (SELECT * FROM RTR_Insert_Update WHERE o_ChangeFlag = 'NEW'),
RTR_Insert_Update_Update AS (SELECT * FROM RTR_Insert_Update WHERE o_ChangeFlag = 'UPDATE'),
UPD_INSERT AS (
	SELECT
	o_CurrenctSnapShotFlag AS o_CurrenctSnapShotFlag1, 
	AuditId AS AuditId1, 
	EffectiveDate AS eff_from_date1, 
	ExpriationDate AS eff_to_date1, 
	Source_sys_id AS Source_sys_id1, 
	Created_Date AS Created_Date1, 
	Modified_Date AS Modified_Date1, 
	bus_class_code AS bus_class_code1, 
	bus_class_code_descript AS bus_class_code_descript1, 
	StandardBusinessClassCode AS StandardBusinessClassCode1, 
	StandardBusinessClassCodeDescription AS StandardBusinessClassCodeDescription1, 
	BusinessSegmentCode AS BusinessSegmentCode1, 
	BusinessSegmentDescription AS BusinessSegmentDescription1, 
	StrategicBusinessGroupCode AS StrategicBusinessGroupCode1, 
	StrategicBusinessGroupDescription AS StrategicBusinessGroupDescription1, 
	ArgentBusinessSegmentCode AS ArgentBusinessSegmentCode1, 
	ArgentBusinessSegmentDescription AS ArgentBusinessSegmentDescription1
	FROM RTR_Insert_Update_Insert
),
TGT_New_sup_business_classification_code AS (
	INSERT INTO sup_business_classification_code
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, bus_class_code, bus_class_code_descript, StandardBusinessClassCode, StandardBusinessClassCodeDescription, BusinessSegmentCode, BusinessSegmentDescription, StrategicBusinessGroupCode, StrategicBusinessGroupDescription, ArgentBusinessSegmentCode, ArgentBusinessSegmentDescription)
	SELECT 
	o_CurrenctSnapShotFlag1 AS CRRNT_SNPSHT_FLAG, 
	AuditId1 AS AUDIT_ID, 
	eff_from_date1 AS EFF_FROM_DATE, 
	eff_to_date1 AS EFF_TO_DATE, 
	Source_sys_id1 AS SOURCE_SYS_ID, 
	Created_Date1 AS CREATED_DATE, 
	Modified_Date1 AS MODIFIED_DATE, 
	bus_class_code1 AS BUS_CLASS_CODE, 
	bus_class_code_descript1 AS BUS_CLASS_CODE_DESCRIPT, 
	StandardBusinessClassCode1 AS STANDARDBUSINESSCLASSCODE, 
	StandardBusinessClassCodeDescription1 AS STANDARDBUSINESSCLASSCODEDESCRIPTION, 
	BusinessSegmentCode1 AS BUSINESSSEGMENTCODE, 
	BusinessSegmentDescription1 AS BUSINESSSEGMENTDESCRIPTION, 
	StrategicBusinessGroupCode1 AS STRATEGICBUSINESSGROUPCODE, 
	StrategicBusinessGroupDescription1 AS STRATEGICBUSINESSGROUPDESCRIPTION, 
	ArgentBusinessSegmentCode1 AS ARGENTBUSINESSSEGMENTCODE, 
	ArgentBusinessSegmentDescription1 AS ARGENTBUSINESSSEGMENTDESCRIPTION
	FROM UPD_INSERT
),
UPD_UPDATE AS (
	SELECT
	sup_bus_class_code_id AS sup_bus_class_code_id3, 
	ExpriationDate AS eff_to_date3, 
	bus_class_code_descript AS bus_class_code_descript3, 
	BusinessSegmentCode AS BusinessSegmentCode3, 
	BusinessSegmentDescription AS BusinessSegmentDescription3, 
	StrategicBusinessGroupCode AS StrategicBusinessGroupCode3, 
	StrategicBusinessGroupDescription AS StrategicBusinessGroupDescription3, 
	Modified_Date, 
	o_CurrenctSnapShotFlag AS CurrentSnapShotFlag, 
	Source_sys_id, 
	ArgentBusinessSegmentCode, 
	ArgentBusinessSegmentDescription
	FROM RTR_Insert_Update_Update
),
TGT_Update_sup_business_classification_code AS (
	MERGE INTO sup_business_classification_code AS T
	USING UPD_UPDATE AS S
	ON T.sup_bus_class_code_id = S.sup_bus_class_code_id3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.CurrentSnapShotFlag, T.eff_to_date = S.eff_to_date3, T.source_sys_id = S.Source_sys_id, T.modified_date = S.Modified_Date, T.bus_class_code_descript = S.bus_class_code_descript3, T.StandardBusinessClassCodeDescription = S.bus_class_code_descript3, T.BusinessSegmentCode = S.BusinessSegmentCode3, T.BusinessSegmentDescription = S.BusinessSegmentDescription3, T.StrategicBusinessGroupCode = S.StrategicBusinessGroupCode3, T.StrategicBusinessGroupDescription = S.StrategicBusinessGroupDescription3, T.ArgentBusinessSegmentCode = S.ArgentBusinessSegmentCode, T.ArgentBusinessSegmentDescription = S.ArgentBusinessSegmentDescription
),
SQ_sup_business_classification_code_setflag AS (
	SELECT 
	 	eff_from_date, 
	      crrnt_snpsht_flag,
		bus_class_code 
	FROM
	 	@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_business_classification_code
	WHERE  bus_class_code IN 
		(SELECT bus_class_code  FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_business_classification_code
	       WHERE crrnt_snpsht_flag = 1 GROUP BY bus_class_code HAVING count(*) > 1)
	ORDER BY  bus_class_code, eff_from_date  DESC
),
Exp_SetValue AS (
	SELECT
	eff_from_date AS in_eff_from_date,
	crrnt_snpsht_flag AS in_crrnt_snpsht_flag,
	bus_class_code AS in_bus_class_code,
	in_eff_from_date AS v_eff_from_date,
	in_bus_class_code AS v_bus_class_code,
	-- *INF*: DECODE(TRUE,in_bus_class_code = v_bus_class_code ,'0',in_crrnt_snpsht_flag)
	DECODE(TRUE,
		in_bus_class_code = v_bus_class_code, '0',
		in_crrnt_snpsht_flag
	) AS CurrentSnapShotFlag,
	SYSDATE AS Modified_Date,
	sup_bus_class_code_id
	FROM SQ_sup_business_classification_code_setflag
),
UPD_Flag AS (
	SELECT
	CurrentSnapShotFlag, 
	Modified_Date, 
	sup_bus_class_code_id
	FROM Exp_SetValue
),
TGT_Setvalue_sup_business_classification_code AS (
	MERGE INTO sup_business_classification_code AS T
	USING UPD_Flag AS S
	ON T.sup_bus_class_code_id = S.sup_bus_class_code_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.CurrentSnapShotFlag, T.modified_date = S.Modified_Date
),
WITH
SQ_GTAMTL500AStage AS (
	SELECT
		GTAMTL500AStageId,
		FormNumber,
		FormDate,
		FormVersionID,
		FormExtensionNumber,
		FormLanguageIndicator,
		FormDescriptionDate,
		FormExpirationDate,
		FormDescription,
		ExtractDate,
		SourceSystemId
	FROM GTAMTL500AStage
	WHERE LEN(LTRIM(RTRIM(GTAMTL500AStage.FormDate)))=4
	OR ISNUMERIC(LTRIM(RTRIM(GTAMTL500AStage.FormDate)))=0
	OR (LEN(LTRIM(RTRIM(GTAMTL500AStage.FormDate)))<4 AND ISNUMERIC(LTRIM(RTRIM(GTAMTL500AStage.FormDate)))=1
	AND NOT EXISTS(
	SELECT 1 FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.GTAMTL500AStage T
	WHERE GTAMTL500AStage.FormNumber=T.FormNumber
	AND LTRIM(RTRIM(T.FormDate))>LTRIM(RTRIM(GTAMTL500AStage.FormDate))
	AND LEN(LTRIM(RTRIM(T.FormDate)))<4 AND ISNUMERIC(LTRIM(RTRIM(T.FormDate)))=1
	))
),
EXP_TL500AStage AS (
	SELECT
	FormNumber,
	FormDate,
	FormVersionID,
	FormExpirationDate,
	FormDescriptionDate,
	FormDescription,
	-- *INF*: IIF(FormDescriptionDate = '99999999',
	-- TO_DATE( '12/31/2100','mm/dd/yyyy'), 
	--  IIF(IS_DATE(FormDescriptionDate , 'YYYYMMDD'), TO_DATE(FormDescriptionDate , 'YYYYMMDD' ), TO_DATE( '12/31/2100','mm/dd/yyyy')))
	IFF(FormDescriptionDate = '99999999', TO_DATE('12/31/2100', 'mm/dd/yyyy'), IFF(IS_DATE(FormDescriptionDate, 'YYYYMMDD'), TO_DATE(FormDescriptionDate, 'YYYYMMDD'), TO_DATE('12/31/2100', 'mm/dd/yyyy'))) AS v_FormDescriptionDate,
	-- *INF*: IIF(FormExpirationDate = '99999999', 
	-- TO_DATE( '12/31/2100','mm/dd/yyyy'),
	--  IIF(IS_DATE(FormExpirationDate, 'YYYYMMDD'), TO_DATE(FormExpirationDate, 'YYYYMMDD' ), TO_DATE( '12/31/2100','mm/dd/yyyy')))
	IFF(FormExpirationDate = '99999999', TO_DATE('12/31/2100', 'mm/dd/yyyy'), IFF(IS_DATE(FormExpirationDate, 'YYYYMMDD'), TO_DATE(FormExpirationDate, 'YYYYMMDD'), TO_DATE('12/31/2100', 'mm/dd/yyyy'))) AS v_FormExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	sysdate AS o_CreatedDate,
	sysdate AS o_ModifiedDate,
	-- *INF*: LTRIM(RTRIM(FormNumber)) || LTRIM(RTRIM(FormVersionID))
	LTRIM(RTRIM(FormNumber)) || LTRIM(RTRIM(FormVersionID)) AS o_FormNumber,
	'99' AS o_StateProvinceCode,
	'N/A' AS o_InsuranceLineCode,
	-- *INF*: IIF(IS_DATE(FormDate, 'MMRR'),TO_DATE(FormDate, 'MMRR'), TO_DATE('01011800', 'MMDDYYYY'))
	IFF(IS_DATE(FormDate, 'MMRR'), TO_DATE(FormDate, 'MMRR'), TO_DATE('01011800', 'MMDDYYYY')) AS o_FormEditionDate,
	-- *INF*: TO_DATE('01-01-1800', 'MM-DD-YYYY')
	TO_DATE('01-01-1800', 'MM-DD-YYYY') AS o_FormEffectiveDate,
	-- *INF*: IIF(SUBSTR(FormNumber, 1, 2) = 'WC', v_FormExpirationDate, v_FormDescriptionDate)
	IFF(SUBSTR(FormNumber, 1, 2) = 'WC', v_FormExpirationDate, v_FormDescriptionDate) AS o_FormExpirationDate,
	-- *INF*: IIF(IS_NUMBER(SUBSTR(RTRIM(FormDescription), -3, 2)) AND TO_INTEGER(SUBSTR(RTRIM(FormDescription), -3, 2)) > 12, 
	-- SUBSTR(RTRIM(FormDescription), -6, 2) || SUBSTR(RTRIM(FormDescription), -3, 2),
	-- SUBSTR(RTRIM(FormDescription), -3, 2) || SUBSTR(RTRIM(FormDescription), -6, 2) )
	IFF(IS_NUMBER(SUBSTR(RTRIM(FormDescription), - 3, 2)) AND TO_INTEGER(SUBSTR(RTRIM(FormDescription), - 3, 2)) > 12, SUBSTR(RTRIM(FormDescription), - 6, 2) || SUBSTR(RTRIM(FormDescription), - 3, 2), SUBSTR(RTRIM(FormDescription), - 3, 2) || SUBSTR(RTRIM(FormDescription), - 6, 2)) AS v_Sort,
	-- *INF*: IIF(IS_NUMBER(v_Sort),0,1)
	IFF(IS_NUMBER(v_Sort), 0, 1) AS o_SortFlag,
	-- *INF*: LENGTH(FormDescription)
	LENGTH(FormDescription) AS o_LenOfFormName
	FROM SQ_GTAMTL500AStage
),
SRT_TL500A AS (
	SELECT
	FormDescription, 
	o_SourceSystemId AS SourceSystemId, 
	o_AuditID AS AuditID, 
	o_CreatedDate AS CreatedDate, 
	o_ModifiedDate AS ModifiedDate, 
	o_FormNumber AS FormNumber, 
	o_StateProvinceCode AS StateProvinceCode, 
	o_InsuranceLineCode AS InsuranceLineCode, 
	o_FormEditionDate AS FormEditionDate, 
	o_FormEffectiveDate AS FormEffectiveDate, 
	o_FormExpirationDate AS FormExpirationDate, 
	o_SortFlag AS SortFlag, 
	o_LenOfFormName AS LenOfFormName
	FROM EXP_TL500AStage
	ORDER BY FormNumber ASC, StateProvinceCode ASC, InsuranceLineCode ASC, FormEditionDate ASC, FormEffectiveDate ASC, FormExpirationDate ASC, SortFlag ASC, LenOfFormName DESC
),
AGG_TL500A AS (
	SELECT
	FormDescription AS i_FormDescription, 
	SourceSystemId, 
	AuditID, 
	CreatedDate, 
	ModifiedDate, 
	FIRST(i_FormDescription) AS o_FormName, 
	FormNumber, 
	StateProvinceCode, 
	InsuranceLineCode, 
	FormEditionDate, 
	FormEffectiveDate, 
	FormExpirationDate
	FROM SRT_TL500A
	GROUP BY FormNumber, StateProvinceCode, InsuranceLineCode, FormEditionDate, FormEffectiveDate, FormExpirationDate
),
SQ_GTAMWBSBFMStage AS (
	SELECT
		GTAMWBSBFMStageId,
		PolicyCompanyNumber,
		LineOfBusiness,
		InsuranceLine,
		FormNumber,
		StateCode,
		ExpirationDate,
		NameOfForm,
		ExtractDate,
		SourceSystemId
	FROM GTAMWBSBFMStage
),
EXP_WBSBFM AS (
	SELECT
	FormNumber,
	StateCode,
	InsuranceLine,
	NameOfForm,
	ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	sysdate AS o_CreatedDate,
	sysdate AS o_ModifiedDate,
	-- *INF*: TO_DATE('01-01-1800', 'MM-DD-YYYY')
	TO_DATE('01-01-1800', 'MM-DD-YYYY') AS o_FormEditionDate,
	-- *INF*: TO_DATE('01-01-1800', 'MM-DD-YYYY')
	TO_DATE('01-01-1800', 'MM-DD-YYYY') AS o_FormEffectiveDate,
	ExpirationDate AS o_FormExpirationDate
	FROM SQ_GTAMWBSBFMStage
),
UnionAll AS (
	SELECT SourceSystemId, AuditID, CreatedDate, ModifiedDate, StateProvinceCode, InsuranceLineCode, o_FormName AS FormName, FormNumber, FormEditionDate, FormEffectiveDate, FormExpirationDate
	FROM AGG_TL500A
	UNION
	SELECT o_SourceSystemId AS SourceSystemId, o_AuditID AS AuditID, o_CreatedDate AS CreatedDate, o_ModifiedDate AS ModifiedDate, StateCode AS StateProvinceCode, InsuranceLine AS InsuranceLineCode, NameOfForm AS FormName, FormNumber, o_FormEditionDate AS FormEditionDate, o_FormEffectiveDate AS FormEffectiveDate, o_FormExpirationDate AS FormExpirationDate
	FROM EXP_WBSBFM
),
AGG_Form AS (
	SELECT
	SourceSystemId, 
	AuditID, 
	CreatedDate, 
	ModifiedDate, 
	StateProvinceCode, 
	InsuranceLineCode, 
	FormName, 
	FormNumber, 
	FormEditionDate, 
	FormEffectiveDate, 
	FormExpirationDate
	FROM UnionAll
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StateProvinceCode, InsuranceLineCode, FormName, FormNumber, FormEditionDate, FormEffectiveDate, FormExpirationDate ORDER BY NULL) = 1
),
LKP_Form AS (
	SELECT
	FormId,
	SourceSystemID,
	AuditID,
	CreatedDate,
	ModifiedDate,
	StateProvinceCode,
	InsuranceLineCode,
	FormName,
	FormNumber,
	FormEditionDate,
	FormEffectiveDate,
	FormExpirationDate
	FROM (
		SELECT 
			FormId,
			SourceSystemID,
			AuditID,
			CreatedDate,
			ModifiedDate,
			StateProvinceCode,
			InsuranceLineCode,
			FormName,
			FormNumber,
			FormEditionDate,
			FormEffectiveDate,
			FormExpirationDate
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.Form
		WHERE SourceSystemID='PMS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StateProvinceCode,InsuranceLineCode,FormNumber,FormEditionDate,FormEffectiveDate,FormExpirationDate ORDER BY FormId) = 1
),
EXP_DetectChanges AS (
	SELECT
	AGG_Form.SourceSystemId,
	AGG_Form.AuditID,
	AGG_Form.CreatedDate,
	AGG_Form.ModifiedDate,
	AGG_Form.StateProvinceCode,
	AGG_Form.InsuranceLineCode,
	AGG_Form.FormName,
	AGG_Form.FormNumber,
	AGG_Form.FormEditionDate,
	AGG_Form.FormEffectiveDate,
	AGG_Form.FormExpirationDate,
	LKP_Form.FormId,
	LKP_Form.StateProvinceCode AS lkp_StateProvinceCode,
	LKP_Form.InsuranceLineCode AS lkp_InsuranceLineCode,
	LKP_Form.FormName AS lkp_FormName,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(FormId), 'NEW',
	-- lkp_StateProvinceCode <> StateProvinceCode
	-- OR lkp_InsuranceLineCode <> InsuranceLineCode
	-- OR lkp_FormName <> FormName, 'UPDATE',
	-- 'NO CHANGE'
	-- )
	DECODE(TRUE,
	FormId IS NULL, 'NEW',
	lkp_StateProvinceCode <> StateProvinceCode OR lkp_InsuranceLineCode <> InsuranceLineCode OR lkp_FormName <> FormName, 'UPDATE',
	'NO CHANGE') AS o_changeflag
	FROM AGG_Form
	LEFT JOIN LKP_Form
	ON LKP_Form.StateProvinceCode = AGG_Form.StateProvinceCode AND LKP_Form.InsuranceLineCode = AGG_Form.InsuranceLineCode AND LKP_Form.FormNumber = AGG_Form.FormNumber AND LKP_Form.FormEditionDate = AGG_Form.FormEditionDate AND LKP_Form.FormEffectiveDate = AGG_Form.FormEffectiveDate AND LKP_Form.FormExpirationDate = AGG_Form.FormExpirationDate
),
RTR_Form AS (
	SELECT
	FormId,
	SourceSystemId,
	AuditID,
	CreatedDate,
	ModifiedDate,
	StateProvinceCode,
	InsuranceLineCode,
	FormName,
	FormNumber,
	FormEditionDate,
	FormEffectiveDate,
	FormExpirationDate,
	o_changeflag AS changeflag
	FROM EXP_DetectChanges
),
RTR_Form_Insert AS (SELECT * FROM RTR_Form WHERE changeflag = 'NEW'),
RTR_Form_Update AS (SELECT * FROM RTR_Form WHERE changeflag = 'UPDATE'),
Update AS (
	SELECT
	FormId, 
	SourceSystemId, 
	AuditID, 
	CreatedDate, 
	ModifiedDate, 
	StateProvinceCode, 
	InsuranceLineCode, 
	FormName, 
	FormNumber, 
	FormEditionDate, 
	FormEffectiveDate, 
	FormExpirationDate
	FROM RTR_Form_Update
),
Form_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.Form AS T
	USING Update AS S
	ON T.FormId = S.FormId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.FormName = S.FormName
),
Form_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.Form
	(SourceSystemID, AuditID, CreatedDate, ModifiedDate, FormName, FormNumber, FormEditionDate, FormEffectiveDate, FormExpirationDate)
	SELECT 
	SourceSystemId AS SOURCESYSTEMID, 
	AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	FORMNAME, 
	FORMNUMBER, 
	FORMEDITIONDATE, 
	FORMEFFECTIVEDATE, 
	FORMEXPIRATIONDATE
	FROM RTR_Form_Insert
),
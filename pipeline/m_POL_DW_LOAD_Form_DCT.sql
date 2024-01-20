WITH
SQ_WBCLPrintDocStage AS (
	SELECT DISTINCT RTRIM(LTRIM(WBCLPrintDocStage.Caption)) Caption, RTRIM(LTRIM(WBCLPrintDocStage.FormName)) FormName
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCLPrintDocStage 
	WHERE
	 SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	AND WBCLPrintDocStage.FormName @{pipeline().parameters.EXCLUDE_FORM}
),
EXP_Form_DCT AS (
	SELECT
	FormName AS i_FormNumber,
	Caption AS i_FormName,
	-- *INF*: LTRIM(RTRIM(i_FormNumber))
	LTRIM(RTRIM(i_FormNumber)) AS v_FormNumberTrim,
	-- *INF*: REG_REPLACE(v_FormNumberTrim,'[^0-9]','')
	REGEXP_REPLACE(v_FormNumberTrim, '[^0-9]', '') AS v_FormNumberRemoveChar,
	-- *INF*: SUBSTR(v_FormNumberRemoveChar,LENGTH(v_FormNumberRemoveChar)-3,4)
	SUBSTR(v_FormNumberRemoveChar, LENGTH(v_FormNumberRemoveChar) - 3, 4) AS v_FormEditionDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	sysdate AS o_CreatedDate,
	sysdate AS o_ModifiedDate,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_FormName)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_FormName) AS o_FormName,
	-- *INF*: LTRIM(RTRIM(i_FormNumber))
	LTRIM(RTRIM(i_FormNumber)) AS o_FormNumber,
	-- *INF*: TO_DATE(LPAD(v_FormEditionDate,4,'0'),'MMRR')
	TO_TIMESTAMP(LPAD(v_FormEditionDate, 4, '0'), 'MMRR') AS o_FormEditionDate,
	-- *INF*: TO_DATE('01/01/1800', 'MM/DD/YYYY')
	TO_TIMESTAMP('01/01/1800', 'MM/DD/YYYY') AS o_FormEffectiveDate,
	-- *INF*: TO_DATE('12/31/2100', 'MM/DD/YYYY')
	TO_TIMESTAMP('12/31/2100', 'MM/DD/YYYY') AS o_FormExpirationDate
	FROM SQ_WBCLPrintDocStage
),
LKP_Form AS (
	SELECT
	FormId,
	FormName,
	FormNumber,
	FormEditionDate
	FROM (
		SELECT 
			FormId,
			FormName,
			FormNumber,
			FormEditionDate
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.Form
		WHERE SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY FormName,FormNumber,FormEditionDate ORDER BY FormId) = 1
),
FIL_EXISTING AS (
	SELECT
	LKP_Form.FormId AS lkp_FormId, 
	EXP_Form_DCT.o_SourceSystemID AS SourceSystemID, 
	EXP_Form_DCT.o_AuditID AS AuditID, 
	EXP_Form_DCT.o_CreatedDate AS CreatedDate, 
	EXP_Form_DCT.o_ModifiedDate AS ModifiedDate, 
	EXP_Form_DCT.o_FormName AS FormName, 
	EXP_Form_DCT.o_FormNumber AS FormNumber, 
	EXP_Form_DCT.o_FormEditionDate AS FormEditionDate, 
	EXP_Form_DCT.o_FormEffectiveDate AS FormEffectiveDate, 
	EXP_Form_DCT.o_FormExpirationDate AS FormExpirationDate
	FROM EXP_Form_DCT
	LEFT JOIN LKP_Form
	ON LKP_Form.FormName = EXP_Form_DCT.o_FormName AND LKP_Form.FormNumber = EXP_Form_DCT.o_FormNumber AND LKP_Form.FormEditionDate = EXP_Form_DCT.o_FormEditionDate
	WHERE ISNULL(lkp_FormId)
),
Form AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.Form
	(SourceSystemID, AuditID, CreatedDate, ModifiedDate, FormName, FormNumber, FormEditionDate, FormEffectiveDate, FormExpirationDate)
	SELECT 
	SOURCESYSTEMID, 
	AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	FORMNAME, 
	FORMNUMBER, 
	FORMEDITIONDATE, 
	FORMEFFECTIVEDATE, 
	FORMEXPIRATIONDATE
	FROM FIL_EXISTING
),
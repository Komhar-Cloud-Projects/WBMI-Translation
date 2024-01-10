WITH
SQ_Form AS (
	SELECT
		FormName,
		FormNumber,
		FormEditionDate,
		FormEffectiveDate,
		FormExpirationDate
	FROM Form
	WHERE ModifiedDate >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_Form AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('18000101','YYYYMMDD')
	TO_DATE('18000101', 'YYYYMMDD'
	) AS o_EffectiveDate,
	-- *INF*: TO_DATE('21001231','YYYYMMDD')
	TO_DATE('21001231', 'YYYYMMDD'
	) AS o_ExpirationDate,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	FormName,
	FormNumber,
	FormEditionDate,
	FormEffectiveDate,
	FormExpirationDate
	FROM SQ_Form
),
LKP_FormDim AS (
	SELECT
	FormDimId,
	FormName,
	FormNumber,
	FormEditionDate,
	FormEffectiveDate,
	FormExpirationDate
	FROM (
		SELECT 
			FormDimId,
			FormName,
			FormNumber,
			FormEditionDate,
			FormEffectiveDate,
			FormExpirationDate
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.FormDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY FormName,FormNumber,FormEditionDate,FormEffectiveDate,FormExpirationDate ORDER BY FormDimId) = 1
),
FIL_FormDim AS (
	SELECT
	LKP_FormDim.FormDimId AS lkp_FormDimId, 
	EXP_Form.o_AuditID, 
	EXP_Form.o_EffectiveDate, 
	EXP_Form.o_ExpirationDate, 
	EXP_Form.o_CreatedDate, 
	EXP_Form.o_ModifiedDate, 
	EXP_Form.FormName, 
	EXP_Form.FormNumber, 
	EXP_Form.FormEditionDate, 
	EXP_Form.FormEffectiveDate, 
	EXP_Form.FormExpirationDate
	FROM EXP_Form
	LEFT JOIN LKP_FormDim
	ON LKP_FormDim.FormName = EXP_Form.FormName AND LKP_FormDim.FormNumber = EXP_Form.FormNumber AND LKP_FormDim.FormEditionDate = EXP_Form.FormEditionDate AND LKP_FormDim.FormEffectiveDate = EXP_Form.FormEffectiveDate AND LKP_FormDim.FormExpirationDate = EXP_Form.FormExpirationDate
	WHERE ISNULL(lkp_FormDimId)
),
FormDim AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.FormDim
	(AuditID, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, FormName, FormNumber, FormEditionDate, FormEffectiveDate, FormExpirationDate)
	SELECT 
	o_AuditID AS AUDITID, 
	o_EffectiveDate AS EFFECTIVEDATE, 
	o_ExpirationDate AS EXPIRATIONDATE, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	FORMNAME, 
	FORMNUMBER, 
	FORMEDITIONDATE, 
	FORMEFFECTIVEDATE, 
	FORMEXPIRATIONDATE
	FROM FIL_FormDim
),
WITH
SQ_claim_occurrence AS (
	SELECT distinct claim_cat_code,
	claim_cat_start_date,
	claim_cat_end_date
	 FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence WHERE claim_cat_code<>'N/A' and Modified_Date>='@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_GetValues AS (
	SELECT
	claim_cat_code AS i_claim_cat_code,
	claim_cat_start_date AS i_claim_cat_start_date,
	claim_cat_end_date AS i_claim_cat_end_date,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_claim_cat_code AS o_claim_cat_code,
	-- *INF*: TO_CHAR(i_claim_cat_start_date,'YYYY-MM-DD')
	TO_CHAR(i_claim_cat_start_date, 'YYYY-MM-DD'
	) AS o_claim_cat_start_date,
	-- *INF*: TO_CHAR(i_claim_cat_end_date,'YYYY-MM-DD')
	TO_CHAR(i_claim_cat_end_date, 'YYYY-MM-DD'
	) AS o_claim_cat_end_date
	FROM SQ_claim_occurrence
),
LKP_CatastropheDim AS (
	SELECT
	CatastropheDimId,
	CatastropheCode,
	CatastropheStartDate,
	CatastropheEndDate
	FROM (
		SELECT 
			CatastropheDimId,
			CatastropheCode,
			CatastropheStartDate,
			CatastropheEndDate
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CatastropheDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CatastropheCode,CatastropheStartDate,CatastropheEndDate ORDER BY CatastropheDimId) = 1
),
RTR_Insert AS (
	SELECT
	LKP_CatastropheDim.CatastropheDimId,
	EXP_GetValues.o_AuditID AS AuditID,
	EXP_GetValues.o_CreatedDate AS CreatedDate,
	EXP_GetValues.o_ModifiedDate AS ModifiedDate,
	EXP_GetValues.o_claim_cat_code AS claim_cat_code,
	EXP_GetValues.o_claim_cat_start_date AS claim_cat_start_date,
	EXP_GetValues.o_claim_cat_end_date AS claim_cat_end_date
	FROM EXP_GetValues
	LEFT JOIN LKP_CatastropheDim
	ON LKP_CatastropheDim.CatastropheCode = EXP_GetValues.o_claim_cat_code AND LKP_CatastropheDim.CatastropheStartDate = EXP_GetValues.o_claim_cat_start_date AND LKP_CatastropheDim.CatastropheEndDate = EXP_GetValues.o_claim_cat_end_date
),
RTR_Insert_INSERT AS (SELECT * FROM RTR_Insert WHERE ISNULL(CatastropheDimId)),
TGT_CatastropheDim_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CatastropheDim
	(AuditID, CreatedDate, ModifiedDate, CatastropheCode, CatastropheStartDate, CatastropheEndDate)
	SELECT 
	AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	claim_cat_code AS CATASTROPHECODE, 
	claim_cat_start_date AS CATASTROPHESTARTDATE, 
	claim_cat_end_date AS CATASTROPHEENDDATE
	FROM RTR_Insert_INSERT
),
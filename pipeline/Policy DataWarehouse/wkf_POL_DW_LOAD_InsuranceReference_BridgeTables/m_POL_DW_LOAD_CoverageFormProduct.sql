WITH
SQ_CoverageFormProduct AS (
	SELECT
		CoverageFormProductId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		CoverageFormId,
		ProductId
	FROM CoverageFormProduct
),
EXP_DateValues AS (
	SELECT
	ModifiedDate AS i_ModifiedDate,
	EffectiveDate AS i_EffectiveDate,
	ExpirationDate AS i_ExpirationDate,
	-- *INF*: IIF(ISNULL(i_EffectiveDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_EffectiveDate)
	IFF(i_EffectiveDate IS NULL, TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'), i_EffectiveDate) AS o_EffectiveDate,
	-- *INF*: IIF(ISNULL(i_ExpirationDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_ExpirationDate)
	IFF(i_ExpirationDate IS NULL, TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'), i_ExpirationDate) AS o_ExpirationDate,
	-- *INF*: IIF(ISNULL(i_ModifiedDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_ModifiedDate)
	IFF(i_ModifiedDate IS NULL, TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'), i_ModifiedDate) AS o_ModifiedDate
	FROM SQ_CoverageFormProduct
),
EXP_NumericValues AS (
	SELECT
	CoverageFormProductId AS i_CoverageFormProductId,
	CoverageFormId AS i_CoverageFormId,
	ProductId AS i_ProductId,
	-- *INF*: IIF(ISNULL(i_CoverageFormProductId),-1,i_CoverageFormProductId)
	IFF(i_CoverageFormProductId IS NULL, - 1, i_CoverageFormProductId) AS o_CoverageFormProductId,
	-- *INF*: IIF(ISNULL(i_CoverageFormId),-1,i_CoverageFormId)
	IFF(i_CoverageFormId IS NULL, - 1, i_CoverageFormId) AS o_CoverageFormId,
	-- *INF*: IIF(ISNULL(i_ProductId),-1,i_ProductId)
	IFF(i_ProductId IS NULL, - 1, i_ProductId) AS o_ProductId
	FROM SQ_CoverageFormProduct
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate
	FROM SQ_CoverageFormProduct
),
TGT_CoverageFormProduct_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageFormProduct AS T
	USING EXP_StringValues AS S
	ON T.CoverageFormProductId = S.o_CoverageFormProductId
	WHEN MATCHED THEN
	UPDATE SET T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.CoverageFormId = S.o_CoverageFormId, T.ProductId = S.o_ProductId
	WHEN NOT MATCHED THEN
	INSERT (CoverageFormProductId, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, CoverageFormId, ProductId)
	VALUES (
	EXP_NumericValues.o_CoverageFormProductId AS COVERAGEFORMPRODUCTID, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_CoverageFormId AS COVERAGEFORMID, 
	EXP_NumericValues.o_ProductId AS PRODUCTID)
),
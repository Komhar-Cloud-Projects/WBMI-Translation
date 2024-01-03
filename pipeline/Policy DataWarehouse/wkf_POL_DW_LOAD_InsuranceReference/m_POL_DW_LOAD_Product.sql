WITH
SQ_Product AS (
	SELECT
		ProductId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		ProductAKId,
		ProductCode,
		ProductAbbreviation,
		ProductDescription
	FROM Product
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
	FROM SQ_Product
),
EXP_NumericValues AS (
	SELECT
	ProductId AS i_ProductId,
	ProductAKId AS i_ProductAKId,
	-- *INF*: IIF(ISNULL(i_ProductId),-1,i_ProductId)
	IFF(i_ProductId IS NULL, - 1, i_ProductId) AS o_ProductId,
	-- *INF*: IIF(ISNULL(i_ProductAKId),-1,i_ProductAKId)
	IFF(i_ProductAKId IS NULL, - 1, i_ProductAKId) AS o_ProductAKId
	FROM SQ_Product
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	ProductCode AS i_ProductCode,
	ProductAbbreviation AS i_ProductAbbreviation,
	ProductDescription AS i_ProductDescription,
	-- *INF*: IIF(TRUNC(i_ExpirationDate)=TO_DATE('2100-12-31','YYYY-MM-DD'),1,0)
	IFF(TRUNC(i_ExpirationDate) = TO_DATE('2100-12-31', 'YYYY-MM-DD'), 1, 0) AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	-- *INF*: IIF(ISNULL(i_ProductCode) OR LENGTH(i_ProductCode)=0 OR IS_SPACES(i_ProductCode),'N/A',LTRIM(RTRIM(i_ProductCode)))
	IFF(i_ProductCode IS NULL OR LENGTH(i_ProductCode) = 0 OR IS_SPACES(i_ProductCode), 'N/A', LTRIM(RTRIM(i_ProductCode))) AS o_ProductCode,
	-- *INF*: IIF(ISNULL(i_ProductAbbreviation) OR LENGTH(i_ProductAbbreviation)=0 OR IS_SPACES(i_ProductAbbreviation),'N/A',LTRIM(RTRIM(i_ProductAbbreviation)))
	IFF(i_ProductAbbreviation IS NULL OR LENGTH(i_ProductAbbreviation) = 0 OR IS_SPACES(i_ProductAbbreviation), 'N/A', LTRIM(RTRIM(i_ProductAbbreviation))) AS o_ProductAbbreviation,
	-- *INF*: IIF(ISNULL(i_ProductDescription) OR LENGTH(i_ProductDescription)=0 OR IS_SPACES(i_ProductDescription),'N/A',LTRIM(RTRIM(i_ProductDescription)))
	IFF(i_ProductDescription IS NULL OR LENGTH(i_ProductDescription) = 0 OR IS_SPACES(i_ProductDescription), 'N/A', LTRIM(RTRIM(i_ProductDescription))) AS o_ProductDescription
	FROM SQ_Product
),
TGT_Product_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.Product AS T
	USING EXP_StringValues AS S
	ON T.ProductId = S.o_ProductId
	WHEN MATCHED THEN
	UPDATE SET T.CurrentSnapshotFlag = S.o_CurrentSnapshotFlag, T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.ProductAKId = S.o_ProductAKId, T.ProductCode = S.o_ProductCode, T.ProductAbbreviation = S.o_ProductAbbreviation, T.ProductDescription = S.o_ProductDescription
	WHEN NOT MATCHED THEN
	INSERT (ProductId, CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, ProductAKId, ProductCode, ProductAbbreviation, ProductDescription)
	VALUES (
	EXP_NumericValues.o_ProductId AS PRODUCTID, 
	EXP_StringValues.o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_ProductAKId AS PRODUCTAKID, 
	EXP_StringValues.o_ProductCode AS PRODUCTCODE, 
	EXP_StringValues.o_ProductAbbreviation AS PRODUCTABBREVIATION, 
	EXP_StringValues.o_ProductDescription AS PRODUCTDESCRIPTION)
),
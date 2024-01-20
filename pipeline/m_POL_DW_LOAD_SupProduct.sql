WITH
SQ_SupProduct AS (
	SELECT
		SupProductId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		SupProductAKId,
		SourceCode,
		ProductCode,
		SourceProductCode
	FROM SupProduct
),
EXP_DateValues AS (
	SELECT
	ModifiedDate AS i_ModifiedDate,
	EffectiveDate AS i_EffectiveDate,
	ExpirationDate AS i_ExpirationDate,
	-- *INF*: IIF(ISNULL(i_EffectiveDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_EffectiveDate)
	IFF(
	    i_EffectiveDate IS NULL, TO_TIMESTAMP('21001231235959', 'YYYYMMDDHH24MISS'), i_EffectiveDate
	) AS o_EffectiveDate,
	-- *INF*: IIF(ISNULL(i_ExpirationDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_ExpirationDate)
	IFF(
	    i_ExpirationDate IS NULL, TO_TIMESTAMP('21001231235959', 'YYYYMMDDHH24MISS'),
	    i_ExpirationDate
	) AS o_ExpirationDate,
	-- *INF*: IIF(ISNULL(i_ModifiedDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_ModifiedDate)
	IFF(
	    i_ModifiedDate IS NULL, TO_TIMESTAMP('21001231235959', 'YYYYMMDDHH24MISS'), i_ModifiedDate
	) AS o_ModifiedDate
	FROM SQ_SupProduct
),
EXP_NumericValues AS (
	SELECT
	SupProductId AS i_SupProductId,
	SupProductAKId AS i_SupProductAKId,
	-- *INF*: IIF(ISNULL(i_SupProductId),-1,i_SupProductId)
	IFF(i_SupProductId IS NULL, - 1, i_SupProductId) AS o_SupProductId,
	-- *INF*: IIF(ISNULL(i_SupProductAKId),-1,i_SupProductAKId)
	IFF(i_SupProductAKId IS NULL, - 1, i_SupProductAKId) AS o_SupProductAKId
	FROM SQ_SupProduct
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	SourceCode AS i_SourceCode,
	ProductCode AS i_ProductCode,
	SourceProductCode AS i_SourceProductCode,
	-- *INF*: IIF(i_ExpirationDate>=TO_DATE('21001231','YYYYMMDD'),1,0)
	IFF(i_ExpirationDate >= TO_TIMESTAMP('21001231', 'YYYYMMDD'), 1, 0) AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	-- *INF*: IIF(ISNULL(i_SourceCode) OR LENGTH(i_SourceCode)=0 OR IS_SPACES(i_SourceCode),'N/A',LTRIM(RTRIM(i_SourceCode)))
	IFF(
	    i_SourceCode IS NULL
	    or LENGTH(i_SourceCode) = 0
	    or LENGTH(i_SourceCode)>0
	    and TRIM(i_SourceCode)='',
	    'N/A',
	    LTRIM(RTRIM(i_SourceCode))
	) AS o_SourceCode,
	-- *INF*: IIF(ISNULL(i_ProductCode) OR LENGTH(i_ProductCode)=0 OR IS_SPACES(i_ProductCode),'N/A',LTRIM(RTRIM(i_ProductCode)))
	IFF(
	    i_ProductCode IS NULL
	    or LENGTH(i_ProductCode) = 0
	    or LENGTH(i_ProductCode)>0
	    and TRIM(i_ProductCode)='',
	    'N/A',
	    LTRIM(RTRIM(i_ProductCode))
	) AS o_ProductCode,
	-- *INF*: IIF(ISNULL(i_SourceProductCode) OR LENGTH(i_SourceProductCode)=0 OR IS_SPACES(i_SourceProductCode),'N/A',LTRIM(RTRIM(i_SourceProductCode)))
	IFF(
	    i_SourceProductCode IS NULL
	    or LENGTH(i_SourceProductCode) = 0
	    or LENGTH(i_SourceProductCode)>0
	    and TRIM(i_SourceProductCode)='',
	    'N/A',
	    LTRIM(RTRIM(i_SourceProductCode))
	) AS o_SourceProductCode
	FROM SQ_SupProduct
),
TGT_SupProduct_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SupProduct AS T
	USING EXP_NumericValues AS S
	ON T.SupProductId = S.o_SupProductId
	WHEN MATCHED THEN
	UPDATE SET T.CurrentSnapshotFlag = S.o_CurrentSnapshotFlag, T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.SupProductAKId = S.o_SupProductAKId, T.SourceCode = S.o_SourceCode, T.ProductCode = S.o_ProductCode, T.SourceProductCode = S.o_SourceProductCode
	WHEN NOT MATCHED THEN
	INSERT (SupProductId, CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, SupProductAKId, SourceCode, ProductCode, SourceProductCode)
	VALUES (
	EXP_NumericValues.o_SupProductId AS SUPPRODUCTID, 
	EXP_StringValues.o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_SupProductAKId AS SUPPRODUCTAKID, 
	EXP_StringValues.o_SourceCode AS SOURCECODE, 
	EXP_StringValues.o_ProductCode AS PRODUCTCODE, 
	EXP_StringValues.o_SourceProductCode AS SOURCEPRODUCTCODE)
),
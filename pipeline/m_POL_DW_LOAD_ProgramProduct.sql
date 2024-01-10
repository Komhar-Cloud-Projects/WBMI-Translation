WITH
SQ_ProgramProduct AS (
	SELECT
		ProgramProductId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		ProgramId,
		ProductId
	FROM ProgramProduct
),
EXP_DateValues AS (
	SELECT
	ModifiedDate AS i_ModifiedDate,
	EffectiveDate AS i_EffectiveDate,
	ExpirationDate AS i_ExpirationDate,
	-- *INF*: IIF(ISNULL(i_EffectiveDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_EffectiveDate)
	IFF(i_EffectiveDate IS NULL,
		TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'
		),
		i_EffectiveDate
	) AS o_EffectiveDate,
	-- *INF*: IIF(ISNULL(i_ExpirationDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_ExpirationDate)
	IFF(i_ExpirationDate IS NULL,
		TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'
		),
		i_ExpirationDate
	) AS o_ExpirationDate,
	-- *INF*: IIF(ISNULL(i_ModifiedDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_ModifiedDate)
	IFF(i_ModifiedDate IS NULL,
		TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'
		),
		i_ModifiedDate
	) AS o_ModifiedDate
	FROM SQ_ProgramProduct
),
EXP_NumericValues AS (
	SELECT
	ProgramProductId AS i_ProgramProductId,
	ProgramId AS i_ProgramId,
	ProductId AS i_ProductId,
	-- *INF*: IIF(ISNULL(i_ProgramProductId),-1,i_ProgramProductId)
	IFF(i_ProgramProductId IS NULL,
		- 1,
		i_ProgramProductId
	) AS o_ProgramProductId,
	-- *INF*: IIF(ISNULL(i_ProgramId),-1,i_ProgramId)
	IFF(i_ProgramId IS NULL,
		- 1,
		i_ProgramId
	) AS o_ProgramId,
	-- *INF*: IIF(ISNULL(i_ProductId),-1,i_ProductId)
	IFF(i_ProductId IS NULL,
		- 1,
		i_ProductId
	) AS o_ProductId
	FROM SQ_ProgramProduct
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate
	FROM SQ_ProgramProduct
),
TGT_ProgramProduct_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ProgramProduct AS T
	USING EXP_DateValues AS S
	ON T.ProgramProductId = S.o_ProgramProductId
	WHEN MATCHED THEN
	UPDATE SET T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.ProgramId = S.o_ProgramId, T.ProductId = S.o_ProductId
	WHEN NOT MATCHED THEN
	INSERT (ProgramProductId, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, ProgramId, ProductId)
	VALUES (
	EXP_NumericValues.o_ProgramProductId AS PROGRAMPRODUCTID, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_ProgramId AS PROGRAMID, 
	EXP_NumericValues.o_ProductId AS PRODUCTID)
),
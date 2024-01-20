WITH
SQ_AssociationProduct AS (
	SELECT
		AssociationProductId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		AssociationId,
		ProductId
	FROM AssociationProduct
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
	FROM SQ_AssociationProduct
),
EXP_NumericValues AS (
	SELECT
	AssociationProductId AS i_AssociationProductId,
	AssociationId AS i_AssociationId,
	ProductId AS i_ProductId,
	-- *INF*: IIF(ISNULL(i_AssociationProductId),-1,i_AssociationProductId)
	IFF(i_AssociationProductId IS NULL, - 1, i_AssociationProductId) AS o_AssociationProductId,
	-- *INF*: IIF(ISNULL(i_AssociationId),-1,i_AssociationId)
	IFF(i_AssociationId IS NULL, - 1, i_AssociationId) AS o_AssociationId,
	-- *INF*: IIF(ISNULL(i_ProductId),-1,i_ProductId)
	IFF(i_ProductId IS NULL, - 1, i_ProductId) AS o_ProductId
	FROM SQ_AssociationProduct
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate
	FROM SQ_AssociationProduct
),
TGT_AssociationProduct_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AssociationProduct AS T
	USING EXP_StringValues AS S
	ON T.AssociationProductId = S.o_AssociationProductId
	WHEN MATCHED THEN
	UPDATE SET T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.AssociationId = S.o_AssociationId, T.ProductId = S.o_ProductId
	WHEN NOT MATCHED THEN
	INSERT (AssociationProductId, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, AssociationId, ProductId)
	VALUES (
	EXP_NumericValues.o_AssociationProductId AS ASSOCIATIONPRODUCTID, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_AssociationId AS ASSOCIATIONID, 
	EXP_NumericValues.o_ProductId AS PRODUCTID)
),
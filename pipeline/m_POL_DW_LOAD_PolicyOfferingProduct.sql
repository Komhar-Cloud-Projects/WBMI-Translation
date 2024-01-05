WITH
SQ_PolicyOfferingProduct AS (
	SELECT
		PolicyOfferingProductId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		PolicyOfferingId,
		ProductId
	FROM PolicyOfferingProduct
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
	FROM SQ_PolicyOfferingProduct
),
EXP_NumericValues AS (
	SELECT
	PolicyOfferingProductId AS i_PolicyOfferingProductId,
	PolicyOfferingId AS i_PolicyOfferingId,
	ProductId AS i_ProductId,
	-- *INF*: IIF(ISNULL(i_PolicyOfferingProductId),-1,i_PolicyOfferingProductId)
	IFF(i_PolicyOfferingProductId IS NULL, - 1, i_PolicyOfferingProductId) AS o_PolicyOfferingProductId,
	-- *INF*: IIF(ISNULL(i_PolicyOfferingId),-1,i_PolicyOfferingId)
	IFF(i_PolicyOfferingId IS NULL, - 1, i_PolicyOfferingId) AS o_PolicyOfferingId,
	-- *INF*: IIF(ISNULL(i_ProductId),-1,i_ProductId)
	IFF(i_ProductId IS NULL, - 1, i_ProductId) AS o_ProductId
	FROM SQ_PolicyOfferingProduct
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate
	FROM SQ_PolicyOfferingProduct
),
TGT_PolicyOfferingProduct_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyOfferingProduct AS T
	USING EXP_StringValues AS S
	ON T.PolicyOfferingProductId = S.o_PolicyOfferingProductId
	WHEN MATCHED THEN
	UPDATE SET T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.PolicyOfferingId = S.o_PolicyOfferingId, T.ProductId = S.o_ProductId
	WHEN NOT MATCHED THEN
	INSERT (PolicyOfferingProductId, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, PolicyOfferingId, ProductId)
	VALUES (
	EXP_NumericValues.o_PolicyOfferingProductId AS POLICYOFFERINGPRODUCTID, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_PolicyOfferingId AS POLICYOFFERINGID, 
	EXP_NumericValues.o_ProductId AS PRODUCTID)
),
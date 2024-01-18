WITH
SQ_InsuranceSegmentProduct AS (
	SELECT
		InsuranceSegmentProductId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		InsuranceSegmentId,
		ProductId
	FROM InsuranceSegmentProduct
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
	FROM SQ_InsuranceSegmentProduct
),
EXP_NumericValues AS (
	SELECT
	InsuranceSegmentProductId AS i_InsuranceSegmentProductId,
	InsuranceSegmentId AS i_InsuranceSegmentId,
	ProductId AS i_ProductId,
	-- *INF*: IIF(ISNULL(i_InsuranceSegmentProductId),-1,i_InsuranceSegmentProductId)
	IFF(i_InsuranceSegmentProductId IS NULL, - 1, i_InsuranceSegmentProductId) AS o_InsuranceSegmentProductId,
	-- *INF*: IIF(ISNULL(i_InsuranceSegmentId),-1,i_InsuranceSegmentId)
	IFF(i_InsuranceSegmentId IS NULL, - 1, i_InsuranceSegmentId) AS o_InsuranceSegmentId,
	-- *INF*: IIF(ISNULL(i_ProductId),-1,i_ProductId)
	IFF(i_ProductId IS NULL, - 1, i_ProductId) AS o_ProductId
	FROM SQ_InsuranceSegmentProduct
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate
	FROM SQ_InsuranceSegmentProduct
),
TGT_InsuranceSegmentProduct_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceSegmentProduct AS T
	USING EXP_DateValues AS S
	ON T.InsuranceSegmentProductId = S.o_InsuranceSegmentProductId
	WHEN MATCHED THEN
	UPDATE SET T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.InsuranceSegmentId = S.o_InsuranceSegmentId, T.ProductId = S.o_ProductId
	WHEN NOT MATCHED THEN
	INSERT (InsuranceSegmentProductId, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, InsuranceSegmentId, ProductId)
	VALUES (
	EXP_NumericValues.o_InsuranceSegmentProductId AS INSURANCESEGMENTPRODUCTID, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_InsuranceSegmentId AS INSURANCESEGMENTID, 
	EXP_NumericValues.o_ProductId AS PRODUCTID)
),
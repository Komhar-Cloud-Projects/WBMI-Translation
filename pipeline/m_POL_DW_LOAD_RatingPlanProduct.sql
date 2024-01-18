WITH
SQ_RatingPlanProduct AS (
	SELECT
		RatingPlanProductId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		RatingPlanId,
		ProductId
	FROM RatingPlanProduct
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
	FROM SQ_RatingPlanProduct
),
EXP_NumericValues AS (
	SELECT
	RatingPlanProductId AS i_RatingPlanProductId,
	RatingPlanId AS i_RatingPlanId,
	ProductId AS i_ProductId,
	-- *INF*: IIF(ISNULL(i_RatingPlanProductId),-1,i_RatingPlanProductId)
	IFF(i_RatingPlanProductId IS NULL, - 1, i_RatingPlanProductId) AS o_RatingPlanProductId,
	-- *INF*: IIF(ISNULL(i_RatingPlanId),-1,i_RatingPlanId)
	IFF(i_RatingPlanId IS NULL, - 1, i_RatingPlanId) AS o_RatingPlanId,
	-- *INF*: IIF(ISNULL(i_ProductId),-1,i_ProductId)
	IFF(i_ProductId IS NULL, - 1, i_ProductId) AS o_ProductId
	FROM SQ_RatingPlanProduct
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate
	FROM SQ_RatingPlanProduct
),
TGT_RatingPlanProduct_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingPlanProduct AS T
	USING EXP_DateValues AS S
	ON T.RatingPlanProductId = S.o_RatingPlanProductId
	WHEN MATCHED THEN
	UPDATE SET T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.RatingPlanId = S.o_RatingPlanId, T.ProductId = S.o_ProductId
	WHEN NOT MATCHED THEN
	INSERT (RatingPlanProductId, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, RatingPlanId, ProductId)
	VALUES (
	EXP_NumericValues.o_RatingPlanProductId AS RATINGPLANPRODUCTID, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_RatingPlanId AS RATINGPLANID, 
	EXP_NumericValues.o_ProductId AS PRODUCTID)
),
WITH
SQ_StrategicProfitCenterProduct AS (
	SELECT
		StrategicProfitCenterProductId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		StrategicProfitCenterId,
		ProductId
	FROM StrategicProfitCenterProduct
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
	FROM SQ_StrategicProfitCenterProduct
),
EXP_NumericValues AS (
	SELECT
	StrategicProfitCenterProductId AS i_StrategicProfitCenterProductId,
	StrategicProfitCenterId AS i_StrategicProfitCenterId,
	ProductId AS i_ProductId,
	-- *INF*: IIF(ISNULL(i_StrategicProfitCenterProductId),-1,i_StrategicProfitCenterProductId)
	IFF(i_StrategicProfitCenterProductId IS NULL, - 1, i_StrategicProfitCenterProductId) AS o_StrategicProfitCenterProductId,
	-- *INF*: IIF(ISNULL(i_StrategicProfitCenterId),-1,i_StrategicProfitCenterId)
	IFF(i_StrategicProfitCenterId IS NULL, - 1, i_StrategicProfitCenterId) AS o_StrategicProfitCenterId,
	-- *INF*: IIF(ISNULL(i_ProductId),-1,i_ProductId)
	IFF(i_ProductId IS NULL, - 1, i_ProductId) AS o_ProductId
	FROM SQ_StrategicProfitCenterProduct
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate
	FROM SQ_StrategicProfitCenterProduct
),
TGT_StrategicProfitCenterProduct_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.StrategicProfitCenterProduct AS T
	USING EXP_NumericValues AS S
	ON T.StrategicProfitCenterProductId = S.o_StrategicProfitCenterProductId
	WHEN MATCHED THEN
	UPDATE SET T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.StrategicProfitCenterId = S.o_StrategicProfitCenterId, T.ProductId = S.o_ProductId
	WHEN NOT MATCHED THEN
	INSERT (StrategicProfitCenterProductId, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, StrategicProfitCenterId, ProductId)
	VALUES (
	EXP_NumericValues.o_StrategicProfitCenterProductId AS STRATEGICPROFITCENTERPRODUCTID, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_StrategicProfitCenterId AS STRATEGICPROFITCENTERID, 
	EXP_NumericValues.o_ProductId AS PRODUCTID)
),
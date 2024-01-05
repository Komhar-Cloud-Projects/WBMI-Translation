WITH
SQ_InsuranceReferenceBridge AS (
	SELECT
		InsuranceReferenceBridgeId,
		ModifiedUserId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		StrategicProfitCenterId,
		InsuranceSegmentId,
		PolicyOfferingId,
		LineOfBusinessId,
		ProductId
	FROM InsuranceReferenceBridge
),
EXP_DateValues AS (
	SELECT
	ModifiedDate AS i_ModifiedDate,
	EffectiveDate AS i_EffectiveDate,
	ExpirationDate AS i_ExpirationDate,
	-- *INF*: IIF(ISNULL(i_ModifiedDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_ModifiedDate)
	IFF(i_ModifiedDate IS NULL, TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'), i_ModifiedDate) AS o_ModifiedDate,
	-- *INF*: IIF(ISNULL(i_EffectiveDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_EffectiveDate)
	IFF(i_EffectiveDate IS NULL, TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'), i_EffectiveDate) AS o_EffectiveDate,
	-- *INF*: IIF(ISNULL(i_ExpirationDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_ExpirationDate)
	IFF(i_ExpirationDate IS NULL, TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'), i_ExpirationDate) AS o_ExpirationDate
	FROM SQ_InsuranceReferenceBridge
),
EXP_NumericValues AS (
	SELECT
	InsuranceReferenceBridgeId AS i_InsuranceReferenceBridgeId,
	StrategicProfitCenterId AS i_StrategicProfitCenterId,
	InsuranceSegmentId AS i_InsuranceSegmentId,
	PolicyOfferingId AS i_PolicyOfferingId,
	LineOfBusinessId AS i_LineOfBusinessId,
	ProductId AS i_ProductId,
	ModifiedUserId,
	-- *INF*: IIF(ISNULL(i_InsuranceReferenceBridgeId),-1,i_InsuranceReferenceBridgeId)
	IFF(i_InsuranceReferenceBridgeId IS NULL, - 1, i_InsuranceReferenceBridgeId) AS o_InsuranceReferenceBridgeId,
	-- *INF*: IIF(ISNULL(i_StrategicProfitCenterId),-1,i_StrategicProfitCenterId)
	IFF(i_StrategicProfitCenterId IS NULL, - 1, i_StrategicProfitCenterId) AS o_StrategicProfitCenterId,
	-- *INF*: IIF(ISNULL(i_InsuranceSegmentId),-1,i_InsuranceSegmentId)
	IFF(i_InsuranceSegmentId IS NULL, - 1, i_InsuranceSegmentId) AS o_InsuranceSegmentId,
	-- *INF*: IIF(ISNULL(i_PolicyOfferingId),-1,i_PolicyOfferingId)
	IFF(i_PolicyOfferingId IS NULL, - 1, i_PolicyOfferingId) AS o_PolicyOfferingId,
	-- *INF*: IIF(ISNULL(i_LineOfBusinessId),-1,i_LineOfBusinessId)
	IFF(i_LineOfBusinessId IS NULL, - 1, i_LineOfBusinessId) AS o_LineOfBusinessId,
	-- *INF*: IIF(ISNULL(i_ProductId),-1,i_ProductId)
	IFF(i_ProductId IS NULL, - 1, i_ProductId) AS o_ProductId
	FROM SQ_InsuranceReferenceBridge
),
TGT_InsuranceReferenceBridge_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceBridge AS T
	USING EXP_NumericValues AS S
	ON T.InsuranceReferenceBridgeId = S.o_InsuranceReferenceBridgeId
	WHEN MATCHED THEN
	UPDATE SET T.ModifiedUserId = S.ModifiedUserId, T.ModifiedDate = S.o_ModifiedDate, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.StrategicProfitCenterId = S.o_StrategicProfitCenterId, T.InsuranceSegmentId = S.o_InsuranceSegmentId, T.PolicyOfferingId = S.o_PolicyOfferingId, T.InsuranceReferenceLineOfBusinessId = S.o_LineOfBusinessId, T.ProductId = S.o_ProductId
	WHEN NOT MATCHED THEN
	INSERT (InsuranceReferenceBridgeId, ModifiedUserId, ModifiedDate, EffectiveDate, ExpirationDate, StrategicProfitCenterId, InsuranceSegmentId, PolicyOfferingId, InsuranceReferenceLineOfBusinessId, ProductId)
	VALUES (
	EXP_NumericValues.o_InsuranceReferenceBridgeId AS INSURANCEREFERENCEBRIDGEID, 
	EXP_NumericValues.MODIFIEDUSERID, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_NumericValues.o_StrategicProfitCenterId AS STRATEGICPROFITCENTERID, 
	EXP_NumericValues.o_InsuranceSegmentId AS INSURANCESEGMENTID, 
	EXP_NumericValues.o_PolicyOfferingId AS POLICYOFFERINGID, 
	EXP_NumericValues.o_LineOfBusinessId AS INSURANCEREFERENCELINEOFBUSINESSID, 
	EXP_NumericValues.o_ProductId AS PRODUCTID)
),
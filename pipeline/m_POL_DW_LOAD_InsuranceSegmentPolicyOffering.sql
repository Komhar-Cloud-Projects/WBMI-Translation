WITH
SQ_InsuranceSegmentPolicyOffering AS (
	SELECT
		InsuranceSegmentPolicyOfferingId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		InsuranceSegmentId,
		PolicyOfferingId
	FROM InsuranceSegmentPolicyOffering
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
	FROM SQ_InsuranceSegmentPolicyOffering
),
EXP_NumericValues AS (
	SELECT
	InsuranceSegmentPolicyOfferingId AS i_InsuranceSegmentPolicyOfferingId,
	InsuranceSegmentId AS i_InsuranceSegmentId,
	PolicyOfferingId AS i_PolicyOfferingId,
	-- *INF*: IIF(ISNULL(i_InsuranceSegmentPolicyOfferingId),-1,i_InsuranceSegmentPolicyOfferingId)
	IFF(i_InsuranceSegmentPolicyOfferingId IS NULL, - 1, i_InsuranceSegmentPolicyOfferingId) AS o_InsuranceSegmentPolicyOfferingId,
	-- *INF*: IIF(ISNULL(i_InsuranceSegmentId),-1,i_InsuranceSegmentId)
	IFF(i_InsuranceSegmentId IS NULL, - 1, i_InsuranceSegmentId) AS o_InsuranceSegmentId,
	-- *INF*: IIF(ISNULL(i_PolicyOfferingId),-1,i_PolicyOfferingId)
	IFF(i_PolicyOfferingId IS NULL, - 1, i_PolicyOfferingId) AS o_PolicyOfferingId
	FROM SQ_InsuranceSegmentPolicyOffering
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate
	FROM SQ_InsuranceSegmentPolicyOffering
),
TGT_InsuranceSegmentPolicyOffering_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceSegmentPolicyOffering AS T
	USING EXP_NumericValues AS S
	ON T.InsuranceSegmentPolicyOfferingId = S.o_InsuranceSegmentPolicyOfferingId
	WHEN MATCHED THEN
	UPDATE SET T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.InsuranceSegmentId = S.o_InsuranceSegmentId, T.PolicyOfferingId = S.o_PolicyOfferingId
	WHEN NOT MATCHED THEN
	INSERT (InsuranceSegmentPolicyOfferingId, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, InsuranceSegmentId, PolicyOfferingId)
	VALUES (
	EXP_NumericValues.o_InsuranceSegmentPolicyOfferingId AS INSURANCESEGMENTPOLICYOFFERINGID, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_InsuranceSegmentId AS INSURANCESEGMENTID, 
	EXP_NumericValues.o_PolicyOfferingId AS POLICYOFFERINGID)
),
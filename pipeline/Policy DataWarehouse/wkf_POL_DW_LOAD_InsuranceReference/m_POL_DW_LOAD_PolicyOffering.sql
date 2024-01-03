WITH
SQ_PolicyOffering AS (
	SELECT
		PolicyOfferingId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		PolicyOfferingAKId,
		PolicyOfferingCode,
		PolicyOfferingAbbreviation,
		PolicyOfferingDescription
	FROM PolicyOffering
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
	FROM SQ_PolicyOffering
),
EXP_NumericValues AS (
	SELECT
	PolicyOfferingId AS i_PolicyOfferingId,
	PolicyOfferingAKId AS i_PolicyOfferingAKId,
	-- *INF*: IIF(ISNULL(i_PolicyOfferingId),-1,i_PolicyOfferingId)
	IFF(i_PolicyOfferingId IS NULL, - 1, i_PolicyOfferingId) AS o_PolicyOfferingId,
	-- *INF*: IIF(ISNULL(i_PolicyOfferingAKId),-1,i_PolicyOfferingAKId)
	IFF(i_PolicyOfferingAKId IS NULL, - 1, i_PolicyOfferingAKId) AS o_PolicyOfferingAKId
	FROM SQ_PolicyOffering
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	PolicyOfferingCode AS i_PolicyOfferingCode,
	PolicyOfferingAbbreviation AS i_PolicyOfferingAbbreviation,
	PolicyOfferingDescription AS i_PolicyOfferingDescription,
	-- *INF*: IIF(TRUNC(i_ExpirationDate)=TO_DATE('2100-12-31','YYYY-MM-DD'),1,0)
	IFF(TRUNC(i_ExpirationDate) = TO_DATE('2100-12-31', 'YYYY-MM-DD'), 1, 0) AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	-- *INF*: IIF(ISNULL(i_PolicyOfferingCode) OR LENGTH(i_PolicyOfferingCode)=0 OR IS_SPACES(i_PolicyOfferingCode),'N/A',LTRIM(RTRIM(i_PolicyOfferingCode)))
	IFF(i_PolicyOfferingCode IS NULL OR LENGTH(i_PolicyOfferingCode) = 0 OR IS_SPACES(i_PolicyOfferingCode), 'N/A', LTRIM(RTRIM(i_PolicyOfferingCode))) AS o_PolicyOfferingCode,
	-- *INF*: IIF(ISNULL(i_PolicyOfferingAbbreviation) OR LENGTH(i_PolicyOfferingAbbreviation)=0 OR IS_SPACES(i_PolicyOfferingAbbreviation),'N/A',LTRIM(RTRIM(i_PolicyOfferingAbbreviation)))
	IFF(i_PolicyOfferingAbbreviation IS NULL OR LENGTH(i_PolicyOfferingAbbreviation) = 0 OR IS_SPACES(i_PolicyOfferingAbbreviation), 'N/A', LTRIM(RTRIM(i_PolicyOfferingAbbreviation))) AS o_PolicyOfferingAbbreviation,
	-- *INF*: IIF(ISNULL(i_PolicyOfferingDescription) OR LENGTH(i_PolicyOfferingDescription)=0 OR IS_SPACES(i_PolicyOfferingDescription),'N/A',LTRIM(RTRIM(i_PolicyOfferingDescription)))
	IFF(i_PolicyOfferingDescription IS NULL OR LENGTH(i_PolicyOfferingDescription) = 0 OR IS_SPACES(i_PolicyOfferingDescription), 'N/A', LTRIM(RTRIM(i_PolicyOfferingDescription))) AS o_PolicyOfferingDescription
	FROM SQ_PolicyOffering
),
TGT_PolicyOffering_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyOffering AS T
	USING EXP_StringValues AS S
	ON T.PolicyOfferingId = S.o_PolicyOfferingId
	WHEN MATCHED THEN
	UPDATE SET T.CurrentSnapshotFlag = S.o_CurrentSnapshotFlag, T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.PolicyOfferingAKId = S.o_PolicyOfferingAKId, T.PolicyOfferingCode = S.o_PolicyOfferingCode, T.PolicyOfferingAbbreviation = S.o_PolicyOfferingAbbreviation, T.PolicyOfferingDescription = S.o_PolicyOfferingDescription
	WHEN NOT MATCHED THEN
	INSERT (PolicyOfferingId, CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, PolicyOfferingAKId, PolicyOfferingCode, PolicyOfferingAbbreviation, PolicyOfferingDescription)
	VALUES (
	EXP_NumericValues.o_PolicyOfferingId AS POLICYOFFERINGID, 
	EXP_StringValues.o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_PolicyOfferingAKId AS POLICYOFFERINGAKID, 
	EXP_StringValues.o_PolicyOfferingCode AS POLICYOFFERINGCODE, 
	EXP_StringValues.o_PolicyOfferingAbbreviation AS POLICYOFFERINGABBREVIATION, 
	EXP_StringValues.o_PolicyOfferingDescription AS POLICYOFFERINGDESCRIPTION)
),
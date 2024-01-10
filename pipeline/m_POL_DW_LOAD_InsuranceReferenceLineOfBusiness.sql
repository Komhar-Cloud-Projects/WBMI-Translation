WITH
SQ_LineOfBusiness AS (
	SELECT
		LineOfBusinessId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		LineOfBusinessAKId,
		LineOfBusinessCode,
		LineOfBusinessAbbreviation,
		LineOfBusinessDescription
	FROM LineOfBusiness
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
	FROM SQ_LineOfBusiness
),
EXP_NumericValues AS (
	SELECT
	LineOfBusinessId AS i_LineOfBusinessId,
	LineOfBusinessAKId AS i_LineOfBusinessAKId,
	-- *INF*: IIF(ISNULL(i_LineOfBusinessId),-1,i_LineOfBusinessId)
	IFF(i_LineOfBusinessId IS NULL, - 1, i_LineOfBusinessId) AS o_LineOfBusinessId,
	-- *INF*: IIF(ISNULL(i_LineOfBusinessAKId),-1,i_LineOfBusinessAKId)
	IFF(i_LineOfBusinessAKId IS NULL, - 1, i_LineOfBusinessAKId) AS o_LineOfBusinessAKId
	FROM SQ_LineOfBusiness
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	LineOfBusinessCode AS i_LineOfBusinessCode,
	LineOfBusinessAbbreviation AS i_LineOfBusinessAbbreviation,
	LineOfBusinessDescription AS i_LineOfBusinessDescription,
	-- *INF*: IIF(TRUNC(i_ExpirationDate)=TO_DATE('2100-12-31','YYYY-MM-DD'),1,0)
	IFF(TRUNC(i_ExpirationDate) = TO_DATE('2100-12-31', 'YYYY-MM-DD'), 1, 0) AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	-- *INF*: IIF(ISNULL(i_LineOfBusinessCode) OR LENGTH(i_LineOfBusinessCode)=0 OR IS_SPACES(i_LineOfBusinessCode),'N/A',LTRIM(RTRIM(i_LineOfBusinessCode)))
	IFF(i_LineOfBusinessCode IS NULL OR LENGTH(i_LineOfBusinessCode) = 0 OR IS_SPACES(i_LineOfBusinessCode), 'N/A', LTRIM(RTRIM(i_LineOfBusinessCode))) AS o_LineOfBusinessCode,
	-- *INF*: IIF(ISNULL(i_LineOfBusinessAbbreviation) OR LENGTH(i_LineOfBusinessAbbreviation)=0 OR IS_SPACES(i_LineOfBusinessAbbreviation),'N/A',LTRIM(RTRIM(i_LineOfBusinessAbbreviation)))
	IFF(i_LineOfBusinessAbbreviation IS NULL OR LENGTH(i_LineOfBusinessAbbreviation) = 0 OR IS_SPACES(i_LineOfBusinessAbbreviation), 'N/A', LTRIM(RTRIM(i_LineOfBusinessAbbreviation))) AS o_LineOfBusinessAbbreviation,
	-- *INF*: IIF(ISNULL(i_LineOfBusinessDescription) OR LENGTH(i_LineOfBusinessDescription)=0 OR IS_SPACES(i_LineOfBusinessDescription),'N/A',LTRIM(RTRIM(i_LineOfBusinessDescription)))
	IFF(i_LineOfBusinessDescription IS NULL OR LENGTH(i_LineOfBusinessDescription) = 0 OR IS_SPACES(i_LineOfBusinessDescription), 'N/A', LTRIM(RTRIM(i_LineOfBusinessDescription))) AS o_LineOfBusinessDescription
	FROM SQ_LineOfBusiness
),
TGT_InsuranceReferenceLineOfBusiness_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceLineOfBusiness AS T
	USING EXP_StringValues AS S
	ON T.InsuranceReferenceLineOfBusinessId = S.o_LineOfBusinessId
	WHEN MATCHED THEN
	UPDATE SET T.CurrentSnapshotFlag = S.o_CurrentSnapshotFlag, T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.InsuranceReferenceLineOfBusinessAKId = S.o_LineOfBusinessAKId, T.InsuranceReferenceLineOfBusinessCode = S.o_LineOfBusinessCode, T.InsuranceReferenceLineOfBusinessAbbreviation = S.o_LineOfBusinessAbbreviation, T.InsuranceReferenceLineOfBusinessDescription = S.o_LineOfBusinessDescription
	WHEN NOT MATCHED THEN
	INSERT (InsuranceReferenceLineOfBusinessId, CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, InsuranceReferenceLineOfBusinessAKId, InsuranceReferenceLineOfBusinessCode, InsuranceReferenceLineOfBusinessAbbreviation, InsuranceReferenceLineOfBusinessDescription)
	VALUES (
	EXP_NumericValues.o_LineOfBusinessId AS INSURANCEREFERENCELINEOFBUSINESSID, 
	EXP_StringValues.o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_LineOfBusinessAKId AS INSURANCEREFERENCELINEOFBUSINESSAKID, 
	EXP_StringValues.o_LineOfBusinessCode AS INSURANCEREFERENCELINEOFBUSINESSCODE, 
	EXP_StringValues.o_LineOfBusinessAbbreviation AS INSURANCEREFERENCELINEOFBUSINESSABBREVIATION, 
	EXP_StringValues.o_LineOfBusinessDescription AS INSURANCEREFERENCELINEOFBUSINESSDESCRIPTION)
),
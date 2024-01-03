WITH
SQ_SupLineOfBusiness AS (
	SELECT
		SupLineOfBusinessId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		SupLineOfBusinessAKId,
		SourceCode,
		LineOfBusinessCode,
		SourceLineOfBusinessCode
	FROM SupLineOfBusiness
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
	FROM SQ_SupLineOfBusiness
),
EXP_NumericValues AS (
	SELECT
	SupLineOfBusinessId,
	SupLineOfBusinessAKId,
	-- *INF*: IIF(ISNULL(SupLineOfBusinessId),-1,SupLineOfBusinessId)
	IFF(SupLineOfBusinessId IS NULL, - 1, SupLineOfBusinessId) AS o_SupLineOfBusinessId,
	-- *INF*: IIF(ISNULL(SupLineOfBusinessAKId),-1,SupLineOfBusinessAKId)
	IFF(SupLineOfBusinessAKId IS NULL, - 1, SupLineOfBusinessAKId) AS o_SupLineOfBusinessAKId
	FROM SQ_SupLineOfBusiness
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	SourceCode AS i_SourceCode,
	LineOfBusinessCode AS i_LineOfBusinessCode,
	SourceLineOfBusinessCode AS i_SourceLineOfBusinessCode,
	-- *INF*: IIF(TRUNC(i_ExpirationDate)=TO_DATE('2100-12-31','YYYY-MM-DD'),1,0)
	IFF(TRUNC(i_ExpirationDate) = TO_DATE('2100-12-31', 'YYYY-MM-DD'), 1, 0) AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	-- *INF*: IIF(ISNULL(i_SourceCode) OR LENGTH(i_SourceCode)=0 OR IS_SPACES(i_SourceCode),'N/A',LTRIM(RTRIM(i_SourceCode)))
	IFF(i_SourceCode IS NULL OR LENGTH(i_SourceCode) = 0 OR IS_SPACES(i_SourceCode), 'N/A', LTRIM(RTRIM(i_SourceCode))) AS o_SourceCode,
	-- *INF*: IIF(ISNULL(i_LineOfBusinessCode) OR LENGTH(i_LineOfBusinessCode)=0 OR IS_SPACES(i_LineOfBusinessCode),'N/A',LTRIM(RTRIM(i_LineOfBusinessCode)))
	IFF(i_LineOfBusinessCode IS NULL OR LENGTH(i_LineOfBusinessCode) = 0 OR IS_SPACES(i_LineOfBusinessCode), 'N/A', LTRIM(RTRIM(i_LineOfBusinessCode))) AS o_LineOfBusinessCode,
	-- *INF*: IIF(ISNULL(i_SourceLineOfBusinessCode) OR LENGTH(i_SourceLineOfBusinessCode)=0 OR IS_SPACES(i_SourceLineOfBusinessCode),'N/A',LTRIM(RTRIM(i_SourceLineOfBusinessCode)))
	IFF(i_SourceLineOfBusinessCode IS NULL OR LENGTH(i_SourceLineOfBusinessCode) = 0 OR IS_SPACES(i_SourceLineOfBusinessCode), 'N/A', LTRIM(RTRIM(i_SourceLineOfBusinessCode))) AS o_SourceLineOfBusinessCode
	FROM SQ_SupLineOfBusiness
),
TGT_SupInsuranceReferenceLineOfBusiness_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SupInsuranceReferenceLineOfBusiness AS T
	USING EXP_StringValues AS S
	ON T.SupInsuranceReferenceLineOfBusinessId = S.o_SupLineOfBusinessId
	WHEN MATCHED THEN
	UPDATE SET T.CurrentSnapshotFlag = S.o_CurrentSnapshotFlag, T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.SupInsuranceReferenceLineOfBusinessAKId = S.o_SupLineOfBusinessAKId, T.SourceCode = S.o_SourceCode, T.InsuranceReferenceLineOfBusinessCode = S.o_LineOfBusinessCode, T.SourceInsuranceReferenceLineOfBusinessCode = S.o_SourceLineOfBusinessCode
	WHEN NOT MATCHED THEN
	INSERT (SupInsuranceReferenceLineOfBusinessId, CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, SupInsuranceReferenceLineOfBusinessAKId, SourceCode, InsuranceReferenceLineOfBusinessCode, SourceInsuranceReferenceLineOfBusinessCode)
	VALUES (
	EXP_NumericValues.o_SupLineOfBusinessId AS SUPINSURANCEREFERENCELINEOFBUSINESSID, 
	EXP_StringValues.o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_SupLineOfBusinessAKId AS SUPINSURANCEREFERENCELINEOFBUSINESSAKID, 
	EXP_StringValues.o_SourceCode AS SOURCECODE, 
	EXP_StringValues.o_LineOfBusinessCode AS INSURANCEREFERENCELINEOFBUSINESSCODE, 
	EXP_StringValues.o_SourceLineOfBusinessCode AS SOURCEINSURANCEREFERENCELINEOFBUSINESSCODE)
),
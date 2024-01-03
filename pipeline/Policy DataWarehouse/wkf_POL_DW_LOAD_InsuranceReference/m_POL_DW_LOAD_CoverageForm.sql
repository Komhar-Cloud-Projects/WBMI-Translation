WITH
SQ_CoverageForm AS (
	SELECT
		CoverageFormId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		CoverageFormAKId,
		CoverageForm
	FROM CoverageForm
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
	FROM SQ_CoverageForm
),
EXP_NumericValues AS (
	SELECT
	CoverageFormId AS i_CoverageFormId,
	CoverageFormAKId AS i_CoverageFormAKId,
	-- *INF*: IIF(ISNULL(i_CoverageFormId),-1,i_CoverageFormId)
	IFF(i_CoverageFormId IS NULL, - 1, i_CoverageFormId) AS o_CoverageFormId,
	-- *INF*: IIF(ISNULL(i_CoverageFormAKId),-1,i_CoverageFormAKId)
	IFF(i_CoverageFormAKId IS NULL, - 1, i_CoverageFormAKId) AS o_CoverageFormAKId
	FROM SQ_CoverageForm
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	CoverageForm AS i_CoverageForm,
	-- *INF*: IIF(TRUNC(i_ExpirationDate)=TO_DATE('2100-12-31','YYYY-MM-DD'),1,0)
	IFF(TRUNC(i_ExpirationDate) = TO_DATE('2100-12-31', 'YYYY-MM-DD'), 1, 0) AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	-- *INF*: IIF(ISNULL(i_CoverageForm) OR LENGTH(i_CoverageForm)=0 OR IS_SPACES(i_CoverageForm),'N/A',LTRIM(RTRIM(i_CoverageForm)))
	IFF(i_CoverageForm IS NULL OR LENGTH(i_CoverageForm) = 0 OR IS_SPACES(i_CoverageForm), 'N/A', LTRIM(RTRIM(i_CoverageForm))) AS o_CoverageForm
	FROM SQ_CoverageForm
),
TGT_CoverageForm_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageForm AS T
	USING EXP_StringValues AS S
	ON T.CoverageFormId = S.o_CoverageFormId
	WHEN MATCHED THEN
	UPDATE SET T.CurrentSnapshotFlag = S.o_CurrentSnapshotFlag, T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.CoverageFormAKId = S.o_CoverageFormAKId, T.CoverageForm = S.o_CoverageForm
	WHEN NOT MATCHED THEN
	INSERT (CoverageFormId, CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, CoverageFormAKId, CoverageForm)
	VALUES (
	EXP_NumericValues.o_CoverageFormId AS COVERAGEFORMID, 
	EXP_StringValues.o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_CoverageFormAKId AS COVERAGEFORMAKID, 
	EXP_StringValues.o_CoverageForm AS COVERAGEFORM)
),
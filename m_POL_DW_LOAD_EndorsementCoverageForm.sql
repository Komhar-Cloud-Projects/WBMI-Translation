WITH
SQ_EndorsementCoverageForm AS (
	SELECT
		EndorsementCoverageFormId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		EndorsementCoverageFormAKId,
		CoverageFormId,
		EndorsementCoverageForm
	FROM EndorsementCoverageForm
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
	FROM SQ_EndorsementCoverageForm
),
EXP_NumericValues AS (
	SELECT
	EndorsementCoverageFormId AS i_EndorsementCoverageFormId,
	EndorsementCoverageFormAKId AS i_EndorsementCoverageFormAKId,
	CoverageFormId AS i_CoverageFormId,
	-- *INF*: IIF(ISNULL(i_EndorsementCoverageFormId),-1,i_EndorsementCoverageFormId)
	IFF(i_EndorsementCoverageFormId IS NULL, - 1, i_EndorsementCoverageFormId) AS o_EndorsementCoverageFormId,
	-- *INF*: IIF(ISNULL(i_EndorsementCoverageFormAKId),-1,i_EndorsementCoverageFormAKId)
	IFF(i_EndorsementCoverageFormAKId IS NULL, - 1, i_EndorsementCoverageFormAKId) AS o_EndorsementCoverageFormAKId,
	-- *INF*: IIF(ISNULL(i_CoverageFormId),-1,i_CoverageFormId)
	IFF(i_CoverageFormId IS NULL, - 1, i_CoverageFormId) AS o_CoverageFormId
	FROM SQ_EndorsementCoverageForm
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	EndorsementCoverageForm AS i_EndorsementCoverageForm,
	-- *INF*: IIF(TRUNC(i_ExpirationDate)=TO_DATE('2100-12-31','YYYY-MM-DD'),1,0)
	IFF(TRUNC(i_ExpirationDate) = TO_DATE('2100-12-31', 'YYYY-MM-DD'), 1, 0) AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	-- *INF*: IIF(ISNULL(i_EndorsementCoverageForm) OR LENGTH(i_EndorsementCoverageForm)=0 OR IS_SPACES(i_EndorsementCoverageForm),'N/A',LTRIM(RTRIM(i_EndorsementCoverageForm)))
	IFF(i_EndorsementCoverageForm IS NULL OR LENGTH(i_EndorsementCoverageForm) = 0 OR IS_SPACES(i_EndorsementCoverageForm), 'N/A', LTRIM(RTRIM(i_EndorsementCoverageForm))) AS o_EndorsementCoverageForm
	FROM SQ_EndorsementCoverageForm
),
TGT_EndorsementCoverageForm_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.EndorsementCoverageForm AS T
	USING EXP_StringValues AS S
	ON T.EndorsementCoverageFormId = S.o_EndorsementCoverageFormId
	WHEN MATCHED THEN
	UPDATE SET T.CurrentSnapshotFlag = S.o_CurrentSnapshotFlag, T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.EndorsementCoverageFormAKId = S.o_EndorsementCoverageFormAKId, T.CoverageFormId = S.o_CoverageFormId, T.EndorsementCoverageForm = S.o_EndorsementCoverageForm
	WHEN NOT MATCHED THEN
	INSERT (EndorsementCoverageFormId, CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, EndorsementCoverageFormAKId, CoverageFormId, EndorsementCoverageForm)
	VALUES (
	EXP_NumericValues.o_EndorsementCoverageFormId AS ENDORSEMENTCOVERAGEFORMID, 
	EXP_StringValues.o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_EndorsementCoverageFormAKId AS ENDORSEMENTCOVERAGEFORMAKID, 
	EXP_NumericValues.o_CoverageFormId AS COVERAGEFORMID, 
	EXP_StringValues.o_EndorsementCoverageForm AS ENDORSEMENTCOVERAGEFORM)
),
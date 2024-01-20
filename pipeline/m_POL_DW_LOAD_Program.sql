WITH
SQ_Program AS (
	SELECT
		ProgramId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		ProgramAKId,
		ProgramCode,
		ProgramDescription
	FROM Program
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
	FROM SQ_Program
),
EXP_NumericValues AS (
	SELECT
	ProgramId AS i_ProgramId,
	ProgramAKId AS i_ProgramAKId,
	-- *INF*: IIF(ISNULL(i_ProgramId),-1,i_ProgramId)
	IFF(i_ProgramId IS NULL, - 1, i_ProgramId) AS o_ProgramId,
	-- *INF*: IIF(ISNULL(i_ProgramAKId),-1,i_ProgramAKId)
	IFF(i_ProgramAKId IS NULL, - 1, i_ProgramAKId) AS o_ProgramAKId
	FROM SQ_Program
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	ProgramCode AS i_ProgramCode,
	ProgramDescription AS i_ProgramDescription,
	-- *INF*: IIF(TRUNC(i_ExpirationDate)=TO_DATE('2100-12-31','YYYY-MM-DD'),1,0)
	IFF(TRUNC(i_ExpirationDate) = TO_TIMESTAMP('2100-12-31', 'YYYY-MM-DD'), 1, 0) AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	-- *INF*: IIF(ISNULL(i_ProgramCode) OR LENGTH(i_ProgramCode)=0 OR IS_SPACES(i_ProgramCode),'N/A',LTRIM(RTRIM(i_ProgramCode)))
	IFF(
	    i_ProgramCode IS NULL
	    or LENGTH(i_ProgramCode) = 0
	    or LENGTH(i_ProgramCode)>0
	    and TRIM(i_ProgramCode)='',
	    'N/A',
	    LTRIM(RTRIM(i_ProgramCode))
	) AS o_ProgramCode,
	-- *INF*: IIF(ISNULL(i_ProgramDescription) OR LENGTH(i_ProgramDescription)=0 OR IS_SPACES(i_ProgramDescription),'N/A',LTRIM(RTRIM(i_ProgramDescription)))
	IFF(
	    i_ProgramDescription IS NULL
	    or LENGTH(i_ProgramDescription) = 0
	    or LENGTH(i_ProgramDescription)>0
	    and TRIM(i_ProgramDescription)='',
	    'N/A',
	    LTRIM(RTRIM(i_ProgramDescription))
	) AS o_ProgramDescription
	FROM SQ_Program
),
TGT_Program_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.Program AS T
	USING EXP_StringValues AS S
	ON T.ProgramId = S.o_ProgramId
	WHEN MATCHED THEN
	UPDATE SET T.CurrentSnapshotFlag = S.o_CurrentSnapshotFlag, T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.ProgramAKId = S.o_ProgramAKId, T.ProgramCode = S.o_ProgramCode, T.ProgramDescription = S.o_ProgramDescription
	WHEN NOT MATCHED THEN
	INSERT (ProgramId, CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, ProgramAKId, ProgramCode, ProgramDescription)
	VALUES (
	EXP_NumericValues.o_ProgramId AS PROGRAMID, 
	EXP_StringValues.o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_ProgramAKId AS PROGRAMAKID, 
	EXP_StringValues.o_ProgramCode AS PROGRAMCODE, 
	EXP_StringValues.o_ProgramDescription AS PROGRAMDESCRIPTION)
),
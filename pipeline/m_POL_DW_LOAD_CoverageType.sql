WITH
SQ_CoverageType AS (
	SELECT
		CoverageTypeId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		CoverageTypeAKId,
		CoverageFormId,
		EndorsementCoverageFormId,
		CoverageType
	FROM CoverageType
),
EXP_DateValues AS (
	SELECT
	ModifiedDate AS i_ModifiedDate,
	EffectiveDate AS i_EffectiveDate,
	ExpirationDate AS i_ExpirationDate,
	-- *INF*: IIF(ISNULL(i_EffectiveDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_EffectiveDate)
	IFF(i_EffectiveDate IS NULL,
		TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'
		),
		i_EffectiveDate
	) AS o_EffectiveDate,
	-- *INF*: IIF(ISNULL(i_ExpirationDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_ExpirationDate)
	IFF(i_ExpirationDate IS NULL,
		TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'
		),
		i_ExpirationDate
	) AS o_ExpirationDate,
	-- *INF*: IIF(ISNULL(i_ModifiedDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_ModifiedDate)
	IFF(i_ModifiedDate IS NULL,
		TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'
		),
		i_ModifiedDate
	) AS o_ModifiedDate
	FROM SQ_CoverageType
),
EXP_NumericValues AS (
	SELECT
	CoverageTypeId AS i_CoverageTypeId,
	CoverageTypeAKId AS i_CoverageTypeAKId,
	CoverageFormId AS i_CoverageFormId,
	EndorsementCoverageFormId AS i_EndorsementCoverageFormId,
	-- *INF*: IIF(ISNULL(i_CoverageTypeId),-1,i_CoverageTypeId)
	IFF(i_CoverageTypeId IS NULL,
		- 1,
		i_CoverageTypeId
	) AS o_CoverageTypeId,
	-- *INF*: IIF(ISNULL(i_CoverageTypeAKId),-1,i_CoverageTypeAKId)
	IFF(i_CoverageTypeAKId IS NULL,
		- 1,
		i_CoverageTypeAKId
	) AS o_CoverageTypeAKId,
	-- *INF*: IIF(ISNULL(i_CoverageFormId),-1,i_CoverageFormId)
	IFF(i_CoverageFormId IS NULL,
		- 1,
		i_CoverageFormId
	) AS o_CoverageFormId,
	-- *INF*: IIF(ISNULL(i_EndorsementCoverageFormId),-1,i_EndorsementCoverageFormId)
	IFF(i_EndorsementCoverageFormId IS NULL,
		- 1,
		i_EndorsementCoverageFormId
	) AS o_EndorsementCoverageFormId
	FROM SQ_CoverageType
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	CoverageType AS i_CoverageType,
	-- *INF*: IIF(TRUNC(i_ExpirationDate)=TO_DATE('2100-12-31','YYYY-MM-DD'),1,0)
	IFF(TRUNC(i_ExpirationDate) = TO_DATE('2100-12-31', 'YYYY-MM-DD'
		),
		1,
		0
	) AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	-- *INF*: IIF(ISNULL(i_CoverageType) OR LENGTH(i_CoverageType)=0 OR IS_SPACES(i_CoverageType),'N/A',LTRIM(RTRIM(i_CoverageType)))
	IFF(i_CoverageType IS NULL 
		OR LENGTH(i_CoverageType
		) = 0 
		OR LENGTH(i_CoverageType)>0 AND TRIM(i_CoverageType)='',
		'N/A',
		LTRIM(RTRIM(i_CoverageType
			)
		)
	) AS o_CoverageType
	FROM SQ_CoverageType
),
TGT_CoverageType_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageType AS T
	USING EXP_DateValues AS S
	ON T.CoverageTypeId = S.o_CoverageTypeId
	WHEN MATCHED THEN
	UPDATE SET T.CurrentSnapshotFlag = S.o_CurrentSnapshotFlag, T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.CoverageTypeAKId = S.o_CoverageTypeAKId, T.CoverageFormId = S.o_CoverageFormId, T.EndorsementCoverageFormId = S.o_EndorsementCoverageFormId, T.CoverageType = S.o_CoverageType
	WHEN NOT MATCHED THEN
	INSERT (CoverageTypeId, CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, CoverageTypeAKId, CoverageFormId, EndorsementCoverageFormId, CoverageType)
	VALUES (
	EXP_NumericValues.o_CoverageTypeId AS COVERAGETYPEID, 
	EXP_StringValues.o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_CoverageTypeAKId AS COVERAGETYPEAKID, 
	EXP_NumericValues.o_CoverageFormId AS COVERAGEFORMID, 
	EXP_NumericValues.o_EndorsementCoverageFormId AS ENDORSEMENTCOVERAGEFORMID, 
	EXP_StringValues.o_CoverageType AS COVERAGETYPE)
),
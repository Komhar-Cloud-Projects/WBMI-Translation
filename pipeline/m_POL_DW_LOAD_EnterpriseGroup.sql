WITH
SQ_EnterpriseGroup AS (
	SELECT
		EnterpriseGroupId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		EnterpriseGroupAKId,
		EnterpriseGroupCode,
		EnterpriseGroupDescription,
		EnterpriseGroupAbbreviation
	FROM EnterpriseGroup
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
	FROM SQ_EnterpriseGroup
),
EXP_NumericValues AS (
	SELECT
	EnterpriseGroupId AS i_EnterpriseGroupId,
	EnterpriseGroupAKId AS i_EnterpriseGroupAKId,
	-- *INF*: IIF(ISNULL(i_EnterpriseGroupId),-1,i_EnterpriseGroupId)
	IFF(i_EnterpriseGroupId IS NULL, - 1, i_EnterpriseGroupId) AS o_EnterpriseGroupId,
	-- *INF*: IIF(ISNULL(i_EnterpriseGroupAKId),-1,i_EnterpriseGroupAKId)
	IFF(i_EnterpriseGroupAKId IS NULL, - 1, i_EnterpriseGroupAKId) AS o_EnterpriseGroupAKId
	FROM SQ_EnterpriseGroup
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	EnterpriseGroupCode AS i_EnterpriseGroupCode,
	EnterpriseGroupDescription AS i_EnterpriseGroupDescription,
	EnterpriseGroupAbbreviation AS i_EnterpriseGroupAbbreviation,
	-- *INF*: IIF(TRUNC(i_ExpirationDate)=TO_DATE('2100-12-31','YYYY-MM-DD'),1,0)
	IFF(TRUNC(i_ExpirationDate) = TO_DATE('2100-12-31', 'YYYY-MM-DD'), 1, 0) AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	-- *INF*: IIF(ISNULL(i_EnterpriseGroupCode) OR LENGTH(i_EnterpriseGroupCode)=0 OR IS_SPACES(i_EnterpriseGroupCode),'N/A',LTRIM(RTRIM(i_EnterpriseGroupCode)))
	IFF(i_EnterpriseGroupCode IS NULL OR LENGTH(i_EnterpriseGroupCode) = 0 OR IS_SPACES(i_EnterpriseGroupCode), 'N/A', LTRIM(RTRIM(i_EnterpriseGroupCode))) AS o_EnterpriseGroupCode,
	-- *INF*: IIF(ISNULL(i_EnterpriseGroupDescription) OR LENGTH(i_EnterpriseGroupDescription)=0 OR IS_SPACES(i_EnterpriseGroupDescription),'N/A',LTRIM(RTRIM(i_EnterpriseGroupDescription)))
	IFF(i_EnterpriseGroupDescription IS NULL OR LENGTH(i_EnterpriseGroupDescription) = 0 OR IS_SPACES(i_EnterpriseGroupDescription), 'N/A', LTRIM(RTRIM(i_EnterpriseGroupDescription))) AS o_EnterpriseGroupDescription,
	-- *INF*: IIF(ISNULL(i_EnterpriseGroupAbbreviation) OR LENGTH(i_EnterpriseGroupAbbreviation)=0 OR IS_SPACES(i_EnterpriseGroupAbbreviation),'N/A',LTRIM(RTRIM(i_EnterpriseGroupAbbreviation)))
	IFF(i_EnterpriseGroupAbbreviation IS NULL OR LENGTH(i_EnterpriseGroupAbbreviation) = 0 OR IS_SPACES(i_EnterpriseGroupAbbreviation), 'N/A', LTRIM(RTRIM(i_EnterpriseGroupAbbreviation))) AS o_EnterpriseGroupAbbreviation
	FROM SQ_EnterpriseGroup
),
TGT_EnterpriseGroup_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.EnterpriseGroup AS T
	USING EXP_StringValues AS S
	ON T.EnterpriseGroupId = S.o_EnterpriseGroupId
	WHEN MATCHED THEN
	UPDATE SET T.CurrentSnapshotFlag = S.o_CurrentSnapshotFlag, T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.EnterpriseGroupAKId = S.o_EnterpriseGroupAKId, T.EnterpriseGroupCode = S.o_EnterpriseGroupCode, T.EnterpriseGroupDescription = S.o_EnterpriseGroupDescription, T.EnterpriseGroupAbbreviation = S.o_EnterpriseGroupAbbreviation
	WHEN NOT MATCHED THEN
	INSERT (EnterpriseGroupId, CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, EnterpriseGroupAKId, EnterpriseGroupCode, EnterpriseGroupDescription, EnterpriseGroupAbbreviation)
	VALUES (
	EXP_NumericValues.o_EnterpriseGroupId AS ENTERPRISEGROUPID, 
	EXP_StringValues.o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_EnterpriseGroupAKId AS ENTERPRISEGROUPAKID, 
	EXP_StringValues.o_EnterpriseGroupCode AS ENTERPRISEGROUPCODE, 
	EXP_StringValues.o_EnterpriseGroupDescription AS ENTERPRISEGROUPDESCRIPTION, 
	EXP_StringValues.o_EnterpriseGroupAbbreviation AS ENTERPRISEGROUPABBREVIATION)
),
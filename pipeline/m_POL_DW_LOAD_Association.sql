WITH
SQ_Association AS (
	SELECT
		AssociationId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		AssociationAKId,
		AssociationCode,
		AssociationDescription
	FROM Association
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
	FROM SQ_Association
),
EXP_NumericValues AS (
	SELECT
	AssociationId AS i_AssociationId,
	AssociationAKId AS i_AssociationAKId,
	-- *INF*: IIF(ISNULL(i_AssociationId),-1,i_AssociationId)
	IFF(i_AssociationId IS NULL,
		- 1,
		i_AssociationId
	) AS o_AssociationId,
	-- *INF*: IIF(ISNULL(i_AssociationAKId),-1,i_AssociationAKId)
	IFF(i_AssociationAKId IS NULL,
		- 1,
		i_AssociationAKId
	) AS o_AssociationAKId
	FROM SQ_Association
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	AssociationCode AS i_AssociationCode,
	AssociationDescription AS i_AssociationDescription,
	-- *INF*: IIF(TRUNC(i_ExpirationDate)=TO_DATE('2100-12-31','YYYY-MM-DD'),1,0)
	IFF(TRUNC(i_ExpirationDate) = TO_DATE('2100-12-31', 'YYYY-MM-DD'
		),
		1,
		0
	) AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	-- *INF*: IIF(ISNULL(i_AssociationCode) OR LENGTH(i_AssociationCode)=0 OR IS_SPACES(i_AssociationCode),'N/A',LTRIM(RTRIM(i_AssociationCode)))
	IFF(i_AssociationCode IS NULL 
		OR LENGTH(i_AssociationCode
		) = 0 
		OR LENGTH(i_AssociationCode)>0 AND TRIM(i_AssociationCode)='',
		'N/A',
		LTRIM(RTRIM(i_AssociationCode
			)
		)
	) AS o_AssociationCode,
	-- *INF*: IIF(ISNULL(i_AssociationDescription) OR LENGTH(i_AssociationDescription)=0 OR IS_SPACES(i_AssociationDescription),'N/A',LTRIM(RTRIM(i_AssociationDescription)))
	IFF(i_AssociationDescription IS NULL 
		OR LENGTH(i_AssociationDescription
		) = 0 
		OR LENGTH(i_AssociationDescription)>0 AND TRIM(i_AssociationDescription)='',
		'N/A',
		LTRIM(RTRIM(i_AssociationDescription
			)
		)
	) AS o_AssociationDescription
	FROM SQ_Association
),
TGT_Association_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.Association AS T
	USING EXP_StringValues AS S
	ON T.AssociationId = S.o_AssociationId
	WHEN MATCHED THEN
	UPDATE SET T.CurrentSnapshotFlag = S.o_CurrentSnapshotFlag, T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.AssociationAKId = S.o_AssociationAKId, T.AssociationCode = S.o_AssociationCode, T.AssociationDescription = S.o_AssociationDescription
	WHEN NOT MATCHED THEN
	INSERT (AssociationId, CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, AssociationAKId, AssociationCode, AssociationDescription)
	VALUES (
	EXP_NumericValues.o_AssociationId AS ASSOCIATIONID, 
	EXP_StringValues.o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_AssociationAKId AS ASSOCIATIONAKID, 
	EXP_StringValues.o_AssociationCode AS ASSOCIATIONCODE, 
	EXP_StringValues.o_AssociationDescription AS ASSOCIATIONDESCRIPTION)
),
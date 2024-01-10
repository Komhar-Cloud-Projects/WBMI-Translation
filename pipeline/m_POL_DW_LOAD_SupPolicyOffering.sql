WITH
SQ_SupPolicyOffering AS (
	SELECT
		SupPolicyOfferingId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		SupPolicyOfferingAKId,
		SourceCode,
		PolicyOfferingCode,
		SourcePolicyOfferingCode
	FROM SupPolicyOffering
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
	FROM SQ_SupPolicyOffering
),
EXP_NumericValues AS (
	SELECT
	SupPolicyOfferingId AS i_SupPolicyOfferingId,
	SupPolicyOfferingAKId AS i_SupPolicyOfferingAKId,
	-- *INF*: IIF(ISNULL(i_SupPolicyOfferingId),-1,i_SupPolicyOfferingId)
	IFF(i_SupPolicyOfferingId IS NULL,
		- 1,
		i_SupPolicyOfferingId
	) AS o_SupPolicyOfferingId,
	-- *INF*: IIF(ISNULL(i_SupPolicyOfferingAKId),-1,i_SupPolicyOfferingAKId)
	IFF(i_SupPolicyOfferingAKId IS NULL,
		- 1,
		i_SupPolicyOfferingAKId
	) AS o_SupPolicyOfferingAKId
	FROM SQ_SupPolicyOffering
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	SourceCode AS i_SourceCode,
	PolicyOfferingCode AS i_PolicyOfferingCode,
	SourcePolicyOfferingCode AS i_SourcePolicyOfferingCode,
	-- *INF*: IIF(i_ExpirationDate>=TO_DATE('21001231','YYYYMMDD'),1,0)
	IFF(i_ExpirationDate >= TO_DATE('21001231', 'YYYYMMDD'
		),
		1,
		0
	) AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	-- *INF*: IIF(ISNULL(i_SourceCode) OR LENGTH(i_SourceCode)=0 OR IS_SPACES(i_SourceCode),'N/A',LTRIM(RTRIM(i_SourceCode)))
	IFF(i_SourceCode IS NULL 
		OR LENGTH(i_SourceCode
		) = 0 
		OR LENGTH(i_SourceCode)>0 AND TRIM(i_SourceCode)='',
		'N/A',
		LTRIM(RTRIM(i_SourceCode
			)
		)
	) AS o_SourceCode,
	-- *INF*: IIF(ISNULL(i_PolicyOfferingCode) OR LENGTH(i_PolicyOfferingCode)=0 OR IS_SPACES(i_PolicyOfferingCode),'N/A',LTRIM(RTRIM(i_PolicyOfferingCode)))
	IFF(i_PolicyOfferingCode IS NULL 
		OR LENGTH(i_PolicyOfferingCode
		) = 0 
		OR LENGTH(i_PolicyOfferingCode)>0 AND TRIM(i_PolicyOfferingCode)='',
		'N/A',
		LTRIM(RTRIM(i_PolicyOfferingCode
			)
		)
	) AS o_PolicyOfferingCode,
	-- *INF*: IIF(ISNULL(i_SourcePolicyOfferingCode) OR LENGTH(i_SourcePolicyOfferingCode)=0 OR IS_SPACES(i_SourcePolicyOfferingCode),'N/A',LTRIM(RTRIM(i_SourcePolicyOfferingCode)))
	IFF(i_SourcePolicyOfferingCode IS NULL 
		OR LENGTH(i_SourcePolicyOfferingCode
		) = 0 
		OR LENGTH(i_SourcePolicyOfferingCode)>0 AND TRIM(i_SourcePolicyOfferingCode)='',
		'N/A',
		LTRIM(RTRIM(i_SourcePolicyOfferingCode
			)
		)
	) AS o_SourcePolicyOfferingCode
	FROM SQ_SupPolicyOffering
),
TGT_SupPolicyOffering_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SupPolicyOffering AS T
	USING EXP_StringValues AS S
	ON T.SupPolicyOfferingId = S.o_SupPolicyOfferingId
	WHEN MATCHED THEN
	UPDATE SET T.CurrentSnapshotFlag = S.o_CurrentSnapshotFlag, T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.SupPolicyOfferingAKId = S.o_SupPolicyOfferingAKId, T.SourceCode = S.o_SourceCode, T.PolicyOfferingCode = S.o_PolicyOfferingCode, T.SourcePolicyOfferingCode = S.o_SourcePolicyOfferingCode
	WHEN NOT MATCHED THEN
	INSERT (SupPolicyOfferingId, CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, SupPolicyOfferingAKId, SourceCode, PolicyOfferingCode, SourcePolicyOfferingCode)
	VALUES (
	EXP_NumericValues.o_SupPolicyOfferingId AS SUPPOLICYOFFERINGID, 
	EXP_StringValues.o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_SupPolicyOfferingAKId AS SUPPOLICYOFFERINGAKID, 
	EXP_StringValues.o_SourceCode AS SOURCECODE, 
	EXP_StringValues.o_PolicyOfferingCode AS POLICYOFFERINGCODE, 
	EXP_StringValues.o_SourcePolicyOfferingCode AS SOURCEPOLICYOFFERINGCODE)
),
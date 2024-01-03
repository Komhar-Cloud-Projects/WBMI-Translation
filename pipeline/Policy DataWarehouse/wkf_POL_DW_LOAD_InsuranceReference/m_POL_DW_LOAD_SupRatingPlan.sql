WITH
SQ_SupRatingPlan AS (
	SELECT
		SupRatingPlanId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		SupRatingPlanAKId,
		SourceCode,
		RatingPlanCode,
		SourceRatingPlanCode
	FROM SupRatingPlan
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
	FROM SQ_SupRatingPlan
),
EXP_NumericValues AS (
	SELECT
	SupRatingPlanId AS i_SupRatingPlanId,
	SupRatingPlanAKId AS i_SupRatingPlanAKId,
	-- *INF*: IIF(ISNULL(i_SupRatingPlanId),-1,i_SupRatingPlanId)
	IFF(i_SupRatingPlanId IS NULL, - 1, i_SupRatingPlanId) AS o_SupRatingPlanId,
	-- *INF*: IIF(ISNULL(i_SupRatingPlanAKId),-1,i_SupRatingPlanAKId)
	IFF(i_SupRatingPlanAKId IS NULL, - 1, i_SupRatingPlanAKId) AS o_SupRatingPlanAKId
	FROM SQ_SupRatingPlan
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	SourceCode AS i_SourceCode,
	RatingPlanCode AS i_RatingPlanCode,
	SourceRatingPlanCode AS i_SourceRatingPlanCode,
	-- *INF*: IIF(i_ExpirationDate>=TO_DATE('21001231','YYYYMMDD'),1,0)
	IFF(i_ExpirationDate >= TO_DATE('21001231', 'YYYYMMDD'), 1, 0) AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	-- *INF*: IIF(ISNULL(i_SourceCode) OR LENGTH(i_SourceCode)=0 OR IS_SPACES(i_SourceCode),'N/A',LTRIM(RTRIM(i_SourceCode)))
	IFF(i_SourceCode IS NULL OR LENGTH(i_SourceCode) = 0 OR IS_SPACES(i_SourceCode), 'N/A', LTRIM(RTRIM(i_SourceCode))) AS o_SourceCode,
	-- *INF*: IIF(ISNULL(i_RatingPlanCode) OR LENGTH(i_RatingPlanCode)=0 OR IS_SPACES(i_RatingPlanCode),'N/A',LTRIM(RTRIM(i_RatingPlanCode)))
	IFF(i_RatingPlanCode IS NULL OR LENGTH(i_RatingPlanCode) = 0 OR IS_SPACES(i_RatingPlanCode), 'N/A', LTRIM(RTRIM(i_RatingPlanCode))) AS o_RatingPlanCode,
	-- *INF*: IIF(ISNULL(i_SourceRatingPlanCode) OR LENGTH(i_SourceRatingPlanCode)=0 OR IS_SPACES(i_SourceRatingPlanCode),'N/A',LTRIM(RTRIM(i_SourceRatingPlanCode)))
	IFF(i_SourceRatingPlanCode IS NULL OR LENGTH(i_SourceRatingPlanCode) = 0 OR IS_SPACES(i_SourceRatingPlanCode), 'N/A', LTRIM(RTRIM(i_SourceRatingPlanCode))) AS o_SourceRatingPlanCode
	FROM SQ_SupRatingPlan
),
TGT_SupRatingPlan_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SupRatingPlan AS T
	USING EXP_StringValues AS S
	ON T.SupRatingPlanId = S.o_SupRatingPlanId
	WHEN MATCHED THEN
	UPDATE SET T.CurrentSnapshotFlag = S.o_CurrentSnapshotFlag, T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.SupRatingPlanAKId = S.o_SupRatingPlanAKId, T.SourceCode = S.o_SourceCode, T.RatingPlanCode = S.o_RatingPlanCode, T.SourceRatingPlanCode = S.o_SourceRatingPlanCode
	WHEN NOT MATCHED THEN
	INSERT (SupRatingPlanId, CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, SupRatingPlanAKId, SourceCode, RatingPlanCode, SourceRatingPlanCode)
	VALUES (
	EXP_NumericValues.o_SupRatingPlanId AS SUPRATINGPLANID, 
	EXP_StringValues.o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_SupRatingPlanAKId AS SUPRATINGPLANAKID, 
	EXP_StringValues.o_SourceCode AS SOURCECODE, 
	EXP_StringValues.o_RatingPlanCode AS RATINGPLANCODE, 
	EXP_StringValues.o_SourceRatingPlanCode AS SOURCERATINGPLANCODE)
),
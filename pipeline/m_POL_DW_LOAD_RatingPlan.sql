WITH
SQ_RatingPlan AS (
	SELECT
		RatingPlanId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		RatingPlanAKId,
		RatingPlanCode,
		RatingPlanDescription,
		RatingPlanAbbreviation
	FROM RatingPlan
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
	FROM SQ_RatingPlan
),
EXP_NumericValues AS (
	SELECT
	RatingPlanId AS i_RatingPlanId,
	RatingPlanAKId AS i_RatingPlanAKId,
	-- *INF*: IIF(ISNULL(i_RatingPlanId),-1,i_RatingPlanId)
	IFF(i_RatingPlanId IS NULL, - 1, i_RatingPlanId) AS o_RatingPlanId,
	-- *INF*: IIF(ISNULL(i_RatingPlanAKId),-1,i_RatingPlanAKId)
	IFF(i_RatingPlanAKId IS NULL, - 1, i_RatingPlanAKId) AS o_RatingPlanAKId
	FROM SQ_RatingPlan
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	RatingPlanCode AS i_RatingPlanCode,
	RatingPlanDescription AS i_RatingPlanDescription,
	-- *INF*: IIF(TRUNC(i_ExpirationDate)=TO_DATE('2100-12-31','YYYY-MM-DD'),1,0)
	IFF(TRUNC(i_ExpirationDate) = TO_DATE('2100-12-31', 'YYYY-MM-DD'), 1, 0) AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	-- *INF*: IIF(ISNULL(i_RatingPlanCode) OR LENGTH(i_RatingPlanCode)=0 OR IS_SPACES(i_RatingPlanCode),'N/A',LTRIM(RTRIM(i_RatingPlanCode)))
	IFF(i_RatingPlanCode IS NULL OR LENGTH(i_RatingPlanCode) = 0 OR IS_SPACES(i_RatingPlanCode), 'N/A', LTRIM(RTRIM(i_RatingPlanCode))) AS o_RatingPlanCode,
	-- *INF*: IIF(ISNULL(i_RatingPlanDescription) OR LENGTH(i_RatingPlanDescription)=0 OR IS_SPACES(i_RatingPlanDescription),'N/A',LTRIM(RTRIM(i_RatingPlanDescription)))
	IFF(i_RatingPlanDescription IS NULL OR LENGTH(i_RatingPlanDescription) = 0 OR IS_SPACES(i_RatingPlanDescription), 'N/A', LTRIM(RTRIM(i_RatingPlanDescription))) AS o_RatingPlanDescription,
	RatingPlanAbbreviation
	FROM SQ_RatingPlan
),
TGT_RatingPlan_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingPlan AS T
	USING EXP_StringValues AS S
	ON T.RatingPlanId = S.o_RatingPlanId
	WHEN MATCHED THEN
	UPDATE SET T.CurrentSnapshotFlag = S.o_CurrentSnapshotFlag, T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.RatingPlanAKId = S.o_RatingPlanAKId, T.RatingPlanCode = S.o_RatingPlanCode, T.RatingPlanDescription = S.o_RatingPlanDescription, T.RatingPlanAbbreviation = S.RatingPlanAbbreviation
	WHEN NOT MATCHED THEN
	INSERT (RatingPlanId, CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, RatingPlanAKId, RatingPlanCode, RatingPlanDescription, RatingPlanAbbreviation)
	VALUES (
	EXP_NumericValues.o_RatingPlanId AS RATINGPLANID, 
	EXP_StringValues.o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_RatingPlanAKId AS RATINGPLANAKID, 
	EXP_StringValues.o_RatingPlanCode AS RATINGPLANCODE, 
	EXP_StringValues.o_RatingPlanDescription AS RATINGPLANDESCRIPTION, 
	EXP_StringValues.RATINGPLANABBREVIATION)
),
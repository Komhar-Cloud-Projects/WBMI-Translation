WITH
SQ_AggregatableLimitType AS (
	select distinct SRC.StandardLimitType  as StandardLimitType 
	from
	(select StandardLimitType 
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupLimitType
	where AdditiveFlag=1
	union all select 'PolicyPerOccurenceLimit' 
	union all select 'PolicyAggregateLimit'
	union all select 'PolicyProductAggregateLimit'
	union all select 'PolicyPerAccidentLimit'
	union all select 'PolicyPerDiseaseLimit'
	union all select 'PolicyPerClaimLimit'
	union all select 'CostNew'
	union all select 'StatedAmount'
	
	union all
	select distinct CoverageLimitType as StandardLimitType 
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit
	where not exists (select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupLimitType where StandardLimitType = CoverageLimitType and AdditiveFlag=0)
	and CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}'
	) SRC
),
LKP_LimitTypeDim AS (
	SELECT
	LimitTypeDimID,
	i_StandardLimitType,
	LimitType
	FROM (
		SELECT LTD.LimitTypeDimID as LimitTypeDimID,
		LTD.LimitType as LimitType 
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.LimitTypeDim LTD
		WHERE LTD.CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY LimitType ORDER BY LimitTypeDimID) = 1
),
FIL_GetNew AS (
	SELECT
	LimitTypeDimID, 
	i_StandardLimitType AS StandardLimitType
	FROM LKP_LimitTypeDim
	WHERE ISNULL(LimitTypeDimID)
),
EXP_DefaultValue AS (
	SELECT
	StandardLimitType AS i_StandardLimitType,
	1 AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	SYSDATE AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS o_ExpirationDate,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_StandardLimitType AS o_LimitType
	FROM FIL_GetNew
),
TGT_LimitTypeDim_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.LimitTypeDim
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, LimitType)
	SELECT 
	o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	o_AuditID AS AUDITID, 
	o_EffectiveDate AS EFFECTIVEDATE, 
	o_ExpirationDate AS EXPIRATIONDATE, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_LimitType AS LIMITTYPE
	FROM EXP_DefaultValue
),
SQ_LimitTypeDim AS (
	select LTD.LimitTypeDimID AS LimitTypeDimID
	from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.LimitTypeDim LTD
	where exists( select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupLimitType SLT
	where SLT.AdditiveFlag=0 and SLT.StandardLimitType=LTD.LimitType)
	and LTD.CurrentSnapshotFlag=1
),
EXP_Calculate AS (
	SELECT
	LimitTypeDimID,
	0 AS o_CurrentSnapshotFlag,
	SYSDATE AS o_ExpirationDate,
	SYSDATE AS o_ModifiedDate
	FROM SQ_LimitTypeDim
),
UPD_Expirate AS (
	SELECT
	LimitTypeDimID, 
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag, 
	o_ExpirationDate AS ExpirationDate, 
	o_ModifiedDate AS ModifiedDate
	FROM EXP_Calculate
),
TGT_LimitTypeDim_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.LimitTypeDim AS T
	USING UPD_Expirate AS S
	ON T.LimitTypeDimID = S.LimitTypeDimID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),
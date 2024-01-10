WITH
SQ_SupISOSpecialCauseOfLossCategoryRule AS (
	SELECT
		SupISOSpecialCauseOfLossCategoryRuleId,
		CurrentSnapshotFlag,
		AuditId,
		EffectiveDate,
		ExpirationDate,
		SourceSystemId,
		CreatedDate,
		ModifiedDate,
		ClassCode,
		ISOSpecialCauseOfLossCategoryCode
	FROM SupISOSpecialCauseOfLossCategoryRule
),
EXP_METADATA AS (
	SELECT
	EffectiveDate AS i_EffectiveDate,
	ExpirationDate AS i_ExpirationDate,
	SourceSystemId AS i_SourceSystemId,
	ClassCode AS i_ClassCode,
	ISOSpecialCauseOfLossCategoryCode AS i_ISOSpecialCauseOfLossCategoryCode,
	1 AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	-- *INF*: i_EffectiveDate
	-- --TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	i_EffectiveDate AS o_EffectiveDate,
	-- *INF*: i_ExpirationDate
	-- --TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	i_ExpirationDate AS o_ExpirationDate,
	i_SourceSystemId AS o_SourceSystemID,
	sysdate AS o_CreatedDate,
	sysdate AS o_ModifiedDate,
	i_ClassCode AS o_ClassCode,
	-- *INF*: LTRIM(RTRIM(i_ISOSpecialCauseOfLossCategoryCode))
	LTRIM(RTRIM(i_ISOSpecialCauseOfLossCategoryCode
		)
	) AS o_ISOSpecialCauseOfLossCategoryCode
	FROM SQ_SupISOSpecialCauseOfLossCategoryRule
),
SupISOSpecialCauseOfLossCategoryRule1 AS (
	TRUNCATE TABLE SupISOSpecialCauseOfLossCategoryRule;
	INSERT INTO SupISOSpecialCauseOfLossCategoryRule
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, ClassCode, ISOSpecialCauseOfLossCategoryCode)
	SELECT 
	o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	o_AuditId AS AUDITID, 
	o_EffectiveDate AS EFFECTIVEDATE, 
	o_ExpirationDate AS EXPIRATIONDATE, 
	o_SourceSystemID AS SOURCESYSTEMID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_ClassCode AS CLASSCODE, 
	o_ISOSpecialCauseOfLossCategoryCode AS ISOSPECIALCAUSEOFLOSSCATEGORYCODE
	FROM EXP_METADATA
),
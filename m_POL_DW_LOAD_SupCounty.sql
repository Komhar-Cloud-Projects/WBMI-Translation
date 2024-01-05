WITH
SQ_SupCounty AS (
	SELECT
		SupCounty.SupCountyId,
		SupCounty.ModifiedUserId,
		SupCounty.ModifiedDate,
		SupCounty.SupStateId,
		SupCounty.CountyName,
		SupCounty.TaxLocationCountyCode,
		SupState.SupStateId AS SupStateId1,
		SupState.StateAbbreviation
	FROM SupCounty
	INNER JOIN SupState
	ON SupCounty.SupStateId=SupState.SupStateId
),
EXP_DetectChange AS (
	SELECT
	1 AS o_CurrentSnapshotflag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	-- *INF*: TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_Sysdate,
	CountyName,
	TaxLocationCountyCode,
	StateAbbreviation
	FROM SQ_SupCounty
),
TGT_SupCounty_INSERT AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.SupCounty;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SupCounty
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, StateAbbreviation, CountyName, TaxLocationCountyCode)
	SELECT 
	o_CurrentSnapshotflag AS CURRENTSNAPSHOTFLAG, 
	o_AuditId AS AUDITID, 
	o_EffectiveDate AS EFFECTIVEDATE, 
	o_ExpirationDate AS EXPIRATIONDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_Sysdate AS CREATEDDATE, 
	o_Sysdate AS MODIFIEDDATE, 
	STATEABBREVIATION, 
	COUNTYNAME, 
	TAXLOCATIONCOUNTYCODE
	FROM EXP_DetectChange
),
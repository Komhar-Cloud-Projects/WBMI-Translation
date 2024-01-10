WITH
SQ_Dividend AS (
	SELECT
		DividendId,
		CurrentSnapshotFlag,
		AuditID,
		EffectiveDate,
		ExpirationDate,
		SourceSystemId,
		CreatedDate,
		ModifiedDate,
		DividendAKId,
		PolicyAKId,
		DividendTransactionEnteredDate,
		DividendRunDate,
		StateCode,
		DividendPlan,
		DividendType,
		SupStateId,
		SupDividendTypeId
	FROM Dividend
	WHERE CurrentSnapshotFlag=1 and
	ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}'
),
AGG_REMOVE_DUPLICATE AS (
	SELECT
	DividendPlan,
	DividendType,
	-- *INF*: IIF(ISNULL(DividendType) OR LTRIM(RTRIM(DividendType))<>'N/A','Dividend','CFA')
	IFF(DividendType IS NULL OR LTRIM(RTRIM(DividendType)) <> 'N/A', 'Dividend', 'CFA') AS DividendCategory
	FROM SQ_Dividend
	GROUP BY DividendPlan, DividendType
),
LKP_DividendTypeDim AS (
	SELECT
	DividendTypeDimId,
	DividendCategory,
	DividendPlan,
	DividendType
	FROM (
		SELECT 
			DividendTypeDimId,
			DividendCategory,
			DividendPlan,
			DividendType
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.DividendTypeDim
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY DividendPlan,DividendType ORDER BY DividendTypeDimId) = 1
),
EXP_Metadata AS (
	SELECT
	LKP_DividendTypeDim.DividendTypeDimId AS lkp_DividendTypeDimId,
	LKP_DividendTypeDim.DividendCategory AS lkp_DividendCategory,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_DividendTypeDimId), 'NEW', 
	-- lkp_DividendCategory <>DividendCategory,'UPDATE',
	-- 'NOCHANGE')
	DECODE(TRUE,
		lkp_DividendTypeDimId IS NULL, 'NEW',
		lkp_DividendCategory <> DividendCategory, 'UPDATE',
		'NOCHANGE') AS Change_Flag,
	AGG_REMOVE_DUPLICATE.DividendPlan,
	AGG_REMOVE_DUPLICATE.DividendType,
	AGG_REMOVE_DUPLICATE.DividendCategory,
	1 AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	-- *INF*: TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
	TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
	TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') AS o_ExpirationDate,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate
	FROM AGG_REMOVE_DUPLICATE
	LEFT JOIN LKP_DividendTypeDim
	ON LKP_DividendTypeDim.DividendPlan = AGG_REMOVE_DUPLICATE.DividendPlan AND LKP_DividendTypeDim.DividendType = AGG_REMOVE_DUPLICATE.DividendType
),
RTR_Target AS (
	SELECT
	lkp_DividendTypeDimId AS DividendTypeDimId,
	Change_Flag,
	DividendPlan,
	DividendType,
	DividendCategory,
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	o_AuditId AS AuditId,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate
	FROM EXP_Metadata
),
RTR_Target_INSERT AS (SELECT * FROM RTR_Target WHERE Change_Flag='NEW'),
RTR_Target_UPDATE AS (SELECT * FROM RTR_Target WHERE Change_Flag='UPDATE'),
UPD_UPDATE AS (
	SELECT
	DividendTypeDimId, 
	Change_Flag, 
	DividendPlan, 
	DividendType, 
	DividendCategory, 
	CurrentSnapshotFlag, 
	AuditId, 
	EffectiveDate, 
	ExpirationDate, 
	CreatedDate, 
	ModifiedDate
	FROM RTR_Target_UPDATE
),
DividendTypeDim_UPDATE AS (
	MERGE INTO DividendTypeDim AS T
	USING UPD_UPDATE AS S
	ON T.DividendTypeDimId = S.DividendTypeDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AuditId, T.ModifiedDate = S.ModifiedDate, T.DividendCategory = S.DividendCategory
),
DividendTypeDim_INSERT AS (
	INSERT INTO DividendTypeDim
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, DividendType, DividendPlan, DividendCategory)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	DIVIDENDTYPE, 
	DIVIDENDPLAN, 
	DIVIDENDCATEGORY
	FROM RTR_Target_INSERT
),
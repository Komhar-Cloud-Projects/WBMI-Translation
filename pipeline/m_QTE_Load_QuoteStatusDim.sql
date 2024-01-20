WITH
SQ_Quote AS (
	SELECT
		QuoteStatusCode
	FROM Quote
	WHERE Quote.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}'
),
LKP_QuoteStatusDim AS (
	SELECT
	QuoteStatusDimID,
	QuoteStatusCode
	FROM (
		SELECT 
			QuoteStatusDimID,
			QuoteStatusCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.QuoteStatusDim
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY QuoteStatusCode ORDER BY QuoteStatusDimID) = 1
),
EXP_ExistingChecking AS (
	SELECT
	LKP_QuoteStatusDim.QuoteStatusDimID AS lkp_QuoteStatusDimID,
	-- *INF*: IIF(ISNULL(lkp_QuoteStatusDimID), 'Insert', 
	--  'Update')
	IFF(lkp_QuoteStatusDimID IS NULL, 'Insert', 'Update') AS v_ChangeFlag,
	v_ChangeFlag AS ChangeFlag,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	-- *INF*: TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')
	TO_TIMESTAMP('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AS EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
	TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') AS ExpirationDate,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	-1 AS QuoteStatusAKId,
	SQ_Quote.QuoteStatusCode AS i_StandardQuoteStatusCode,
	-- *INF*: IIF(ISNULL(i_StandardQuoteStatusCode ), 'N/A', i_StandardQuoteStatusCode )
	IFF(i_StandardQuoteStatusCode IS NULL, 'N/A', i_StandardQuoteStatusCode) AS v_StandardQuoteStatusCode,
	v_StandardQuoteStatusCode AS o_StandardQuoteStatusCode,
	v_StandardQuoteStatusCode AS o_StandardQuoteStatusDescription
	FROM SQ_Quote
	LEFT JOIN LKP_QuoteStatusDim
	ON LKP_QuoteStatusDim.QuoteStatusCode = SQ_Quote.QuoteStatusCode
),
RTR_InsertUpdate AS (
	SELECT
	lkp_QuoteStatusDimID AS QuoteStatusDimID,
	ChangeFlag,
	CurrentSnapshotFlag,
	AuditId,
	EffectiveDate,
	ExpirationDate,
	CreatedDate,
	ModifiedDate,
	QuoteStatusAKId,
	o_StandardQuoteStatusCode AS StandardQuoteStatusCode,
	o_StandardQuoteStatusDescription AS StandardQuoteStatusDescription
	FROM EXP_ExistingChecking
),
RTR_InsertUpdate_Insert AS (SELECT * FROM RTR_InsertUpdate WHERE ChangeFlag = 'Insert'),
RTR_InsertUpdate_DEFAULT1 AS (SELECT * FROM RTR_InsertUpdate WHERE NOT ( (ChangeFlag = 'Insert') )),
UPD_QuoteStatusDim_Insert AS (
	SELECT
	CurrentSnapshotFlag, 
	AuditId, 
	EffectiveDate, 
	ExpirationDate, 
	CreatedDate, 
	ModifiedDate, 
	QuoteStatusAKId, 
	StandardQuoteStatusCode, 
	StandardQuoteStatusDescription
	FROM RTR_InsertUpdate_Insert
),
TGT_QuoteStatusDim_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.QuoteStatusDim
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, EDWQuoteStatusAKId, QuoteStatusCode, QuoteStatusDescription)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	QuoteStatusAKId AS EDWQUOTESTATUSAKID, 
	StandardQuoteStatusCode AS QUOTESTATUSCODE, 
	StandardQuoteStatusDescription AS QUOTESTATUSDESCRIPTION
	FROM UPD_QuoteStatusDim_Insert
),
UPD_QuoteStatusDim_Update AS (
	SELECT
	QuoteStatusDimID, 
	CurrentSnapshotFlag, 
	AuditId, 
	EffectiveDate, 
	ExpirationDate, 
	CreatedDate, 
	ModifiedDate, 
	QuoteStatusAKId AS QuoteStatusAKId2, 
	StandardQuoteStatusCode, 
	StandardQuoteStatusDescription
	FROM RTR_InsertUpdate_DEFAULT1
),
TGT_QuoteStatusDim_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.QuoteStatusDim AS T
	USING UPD_QuoteStatusDim_Update AS S
	ON T.QuoteStatusDimId = S.QuoteStatusDimID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.EDWQuoteStatusAKId = S.QuoteStatusAKId2, T.QuoteStatusCode = S.StandardQuoteStatusCode, T.QuoteStatusDescription = S.StandardQuoteStatusDescription
),
WITH
SQ_Join_Quote_QuoteStatus AS (
	SELECT
		QuoteId,
		QuoteAKId,
		QuoteNumber,
		ReasonCode,
		OtherReasonComment
	FROM Quote
	WHERE Quote.QuoteStatusCode='Declined' and 
	Quote.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_Values AS (
	SELECT
	QuoteId AS i_QuoteId,
	QuoteAKId AS i_QuoteAKId,
	QuoteNumber AS i_QuoteNumber,
	ReasonCode AS i_ReasonCode,
	OtherReasonComment AS i_OtherReasonComment,
	i_QuoteId AS o_QuoteId,
	i_QuoteAKId AS o_QuoteAKId,
	-- *INF*: IIF(ISNULL(i_QuoteNumber) or IS_SPACES(i_QuoteNumber)  or LENGTH(i_QuoteNumber)=0,'N/A',LTRIM(RTRIM(i_QuoteNumber)))
	-- 
	IFF(i_QuoteNumber IS NULL OR IS_SPACES(i_QuoteNumber) OR LENGTH(i_QuoteNumber) = 0, 'N/A', LTRIM(RTRIM(i_QuoteNumber))) AS o_QuoteNumber,
	-- *INF*: IIF(ISNULL(i_ReasonCode) or IS_SPACES(i_ReasonCode)  or LENGTH(i_ReasonCode)=0,'N/A',LTRIM(RTRIM(i_ReasonCode)))
	-- 
	IFF(i_ReasonCode IS NULL OR IS_SPACES(i_ReasonCode) OR LENGTH(i_ReasonCode) = 0, 'N/A', LTRIM(RTRIM(i_ReasonCode))) AS o_ReasonCode,
	-- *INF*: LTRIM(RTRIM(i_OtherReasonComment))
	LTRIM(RTRIM(i_OtherReasonComment)) AS o_OtherReasonComment,
	-- *INF*: IIF(ISNULL(i_ReasonCode),'N/A',LTRIM(RTRIM(i_ReasonCode)))
	-- 
	-- 
	IFF(i_ReasonCode IS NULL, 'N/A', LTRIM(RTRIM(i_ReasonCode))) AS o_StandardReasonCode
	FROM SQ_Join_Quote_QuoteStatus
),
LKP_DeclinedQuoteDim AS (
	SELECT
	DeclinedQuoteDimId,
	EDWQuotePKID
	FROM (
		SELECT 
			DeclinedQuoteDimId,
			EDWQuotePKID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.DeclinedQuoteDim
		WHERE EXISTS (
		SELECT 1 FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote q
		WHERE q.QuoteAKId=DeclinedQuoteDim.EDWQuoteAKId
		AND q.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWQuotePKID ORDER BY DeclinedQuoteDimId) = 1
),
LKP_SupQuoteDeclinedReason AS (
	SELECT
	StandardQuoteDeclinedReasonDescription,
	QuoteDeclinedReasonCode
	FROM (
		SELECT 
			StandardQuoteDeclinedReasonDescription,
			QuoteDeclinedReasonCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupQuoteDeclinedReason
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY QuoteDeclinedReasonCode ORDER BY StandardQuoteDeclinedReasonDescription) = 1
),
EXP_ExistingChecking AS (
	SELECT
	LKP_DeclinedQuoteDim.DeclinedQuoteDimId AS lkp_DeclinedQuoteDimId,
	EXP_Values.o_QuoteId AS i_QuoteId,
	EXP_Values.o_QuoteAKId AS i_QuoteAKId,
	EXP_Values.o_QuoteNumber AS i_QuoteNumber,
	EXP_Values.o_ReasonCode AS i_ReasonCode,
	EXP_Values.o_OtherReasonComment AS i_OtherReasonComment,
	EXP_Values.o_StandardReasonCode AS i_StandardReasonCode,
	LKP_SupQuoteDeclinedReason.StandardQuoteDeclinedReasonDescription AS i_StandardReasonDescription,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_DeclinedQuoteDimId), 'Insert',
	-- 'Update'
	-- )
	DECODE(TRUE,
		lkp_DeclinedQuoteDimId IS NULL, 'Insert',
		'Update') AS v_ChangeFlag,
	v_ChangeFlag AS ChangeFlag,
	1 AS CurrentSnapshotFalg,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')
	TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AS EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
	TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') AS ExpirationDate,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	i_QuoteNumber AS QuoteNumber,
	i_StandardReasonCode AS StandardReasonCode,
	-- *INF*: IIF(NOT ISNULL(i_StandardReasonDescription),i_StandardReasonDescription,'N/A')
	IFF(NOT i_StandardReasonDescription IS NULL, i_StandardReasonDescription, 'N/A') AS StandardReasonDescription,
	-- *INF*: IIF(i_ReasonCode='8', i_OtherReasonComment, 'N/A')
	IFF(i_ReasonCode = '8', i_OtherReasonComment, 'N/A') AS Comments,
	i_QuoteAKId AS EDWQuoteAKID,
	i_QuoteId AS EDWQuotePKID
	FROM EXP_Values
	LEFT JOIN LKP_DeclinedQuoteDim
	ON LKP_DeclinedQuoteDim.EDWQuotePKID = EXP_Values.o_QuoteId
	LEFT JOIN LKP_SupQuoteDeclinedReason
	ON LKP_SupQuoteDeclinedReason.QuoteDeclinedReasonCode = EXP_Values.o_StandardReasonCode
),
RTR_InsertUpdate AS (
	SELECT
	lkp_DeclinedQuoteDimId,
	ChangeFlag,
	CurrentSnapshotFalg,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	CreatedDate,
	ModifiedDate,
	QuoteNumber,
	StandardReasonCode,
	StandardReasonDescription,
	Comments,
	EDWQuoteAKID,
	EDWQuotePKID
	FROM EXP_ExistingChecking
),
RTR_InsertUpdate_Insert AS (SELECT * FROM RTR_InsertUpdate WHERE ChangeFlag = 'Insert'),
RTR_InsertUpdate_DEFAULT1 AS (SELECT * FROM RTR_InsertUpdate WHERE NOT ( (ChangeFlag = 'Insert') )),
UPD_Insert AS (
	SELECT
	CurrentSnapshotFalg, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	CreatedDate, 
	ModifiedDate, 
	QuoteNumber, 
	StandardReasonCode, 
	StandardReasonDescription, 
	Comments, 
	EDWQuoteAKID, 
	EDWQuotePKID
	FROM RTR_InsertUpdate_Insert
),
TGT_DeclinedQuoteDim_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DeclinedQuoteDim
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, QuoteNumber, ReasonDeclinedCode, ReasonDeclinedDescription, Comments, EDWQuoteAKId, EDWQuotePKId)
	SELECT 
	CurrentSnapshotFalg AS CURRENTSNAPSHOTFLAG, 
	AuditID AS AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	QUOTENUMBER, 
	StandardReasonCode AS REASONDECLINEDCODE, 
	StandardReasonDescription AS REASONDECLINEDDESCRIPTION, 
	COMMENTS, 
	EDWQuoteAKID AS EDWQUOTEAKID, 
	EDWQuotePKID AS EDWQUOTEPKID
	FROM UPD_Insert
),
UPD_Update AS (
	SELECT
	lkp_DeclinedQuoteDimId AS DeclinedQuoteDimId, 
	CurrentSnapshotFalg, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	CreatedDate, 
	ModifiedDate, 
	QuoteNumber, 
	StandardReasonCode, 
	StandardReasonDescription, 
	Comments, 
	EDWQuoteAKID, 
	EDWQuotePKID
	FROM RTR_InsertUpdate_DEFAULT1
),
TGT_DeclinedQuoteDim_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DeclinedQuoteDim AS T
	USING UPD_Update AS S
	ON T.DeclinedQuoteDimId = S.DeclinedQuoteDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.QuoteNumber = S.QuoteNumber, T.ReasonDeclinedCode = S.StandardReasonCode, T.ReasonDeclinedDescription = S.StandardReasonDescription, T.Comments = S.Comments, T.EDWQuoteAKId = S.EDWQuoteAKID, T.EDWQuotePKId = S.EDWQuotePKID
),
WITH
SQ_Quote AS (
	SELECT
		QuoteId,
		QuoteAKId,
		QuoteNumber,
		QuoteReasonClosedCode,
		QuoteReasonClosedComments
	FROM Quote
	WHERE Quote.QuoteStatusCode='Closed' and 
	Quote.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_Values AS (
	SELECT
	QuoteId AS i_QuoteId,
	QuoteAKId AS i_QuoteAKId,
	QuoteNumber AS i_QuoteNumber,
	QuoteReasonClosedCode AS i_QuoteReasonClosedCode,
	QuoteReasonClosedComments AS i_QuoteReasonClosedComments,
	i_QuoteId AS o_QuoteId,
	i_QuoteAKId AS o_QuoteAKId,
	-- *INF*: IIF(ISNULL(i_QuoteNumber) or IS_SPACES(i_QuoteNumber)  or LENGTH(i_QuoteNumber)=0,'N/A',LTRIM(RTRIM(i_QuoteNumber)))
	IFF(i_QuoteNumber IS NULL 
		OR LENGTH(i_QuoteNumber)>0 AND TRIM(i_QuoteNumber)='' 
		OR LENGTH(i_QuoteNumber
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_QuoteNumber
			)
		)
	) AS o_QuoteNumber,
	-- *INF*: IIF(ISNULL(i_QuoteReasonClosedCode) or IS_SPACES(i_QuoteReasonClosedCode)   or LENGTH(i_QuoteReasonClosedCode) =0,'-1',LTRIM(RTRIM(i_QuoteReasonClosedCode)))
	IFF(i_QuoteReasonClosedCode IS NULL 
		OR LENGTH(i_QuoteReasonClosedCode)>0 AND TRIM(i_QuoteReasonClosedCode)='' 
		OR LENGTH(i_QuoteReasonClosedCode
		) = 0,
		'-1',
		LTRIM(RTRIM(i_QuoteReasonClosedCode
			)
		)
	) AS o_QuoteReasonClosedCode,
	-- *INF*: IIF(ISNULL(i_QuoteReasonClosedComments) or IS_SPACES(i_QuoteReasonClosedComments)   or LENGTH(i_QuoteReasonClosedComments) =0,'N/A',LTRIM(RTRIM(i_QuoteReasonClosedComments)))
	IFF(i_QuoteReasonClosedComments IS NULL 
		OR LENGTH(i_QuoteReasonClosedComments)>0 AND TRIM(i_QuoteReasonClosedComments)='' 
		OR LENGTH(i_QuoteReasonClosedComments
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_QuoteReasonClosedComments
			)
		)
	) AS o_QuoteReasonClosedComments
	FROM SQ_Quote
),
LKP_ClosedQuoteDim AS (
	SELECT
	ClosedQuoteDimId,
	EDWQuotePKId
	FROM (
		SELECT 
			ClosedQuoteDimId,
			EDWQuotePKId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.ClosedQuoteDim
		WHERE EXISTS (
		SELECT 1 FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote q
		WHERE q.QuoteAKId=ClosedQuoteDim.EDWQuoteAKId
		AND q.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWQuotePKId ORDER BY ClosedQuoteDimId) = 1
),
LKP_SupQuoteClosedReason AS (
	SELECT
	QuoteReasonClosedDescription,
	QuoteReasonClosedCode
	FROM (
		SELECT 
			QuoteReasonClosedDescription,
			QuoteReasonClosedCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupQuoteClosedReason
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY QuoteReasonClosedCode ORDER BY QuoteReasonClosedDescription) = 1
),
EXP_ExistingChecking AS (
	SELECT
	LKP_ClosedQuoteDim.ClosedQuoteDimId AS lkp_ClosedQuoteDimId,
	EXP_Values.o_QuoteId AS i_QuoteId,
	EXP_Values.o_QuoteAKId AS i_QuoteAKId,
	EXP_Values.o_QuoteNumber AS i_QuoteNumber,
	EXP_Values.o_QuoteReasonClosedCode AS i_QuoteReasonClosedCode,
	EXP_Values.o_QuoteReasonClosedComments AS i_QuoteReasonClosedComments,
	LKP_SupQuoteClosedReason.QuoteReasonClosedDescription AS i_QuoteReasonClosedDescription,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_ClosedQuoteDimId), 'Insert',
	-- 'Update'
	-- )
	DECODE(TRUE,
		lkp_ClosedQuoteDimId IS NULL, 'Insert',
		'Update'
	) AS v_ChangeFlag,
	v_ChangeFlag AS ChangeFlag,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')
	TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'
	) AS EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
	TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'
	) AS ExpirationDate,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	i_QuoteNumber AS QuoteNumber,
	i_QuoteReasonClosedCode AS QuoteReasonClosedCode,
	-- *INF*: IIF(NOT ISNULL(i_QuoteReasonClosedDescription),i_QuoteReasonClosedDescription,'N/A')
	-- 
	-- 
	IFF(i_QuoteReasonClosedDescription IS NOT NULL,
		i_QuoteReasonClosedDescription,
		'N/A'
	) AS QuoteReasonClosedDescription,
	i_QuoteReasonClosedComments AS QuoteReasonClosedComments,
	i_QuoteAKId AS EDWQuoteAKID,
	i_QuoteId AS EDWQuotePKID
	FROM EXP_Values
	LEFT JOIN LKP_ClosedQuoteDim
	ON LKP_ClosedQuoteDim.EDWQuotePKId = EXP_Values.o_QuoteId
	LEFT JOIN LKP_SupQuoteClosedReason
	ON LKP_SupQuoteClosedReason.QuoteReasonClosedCode = EXP_Values.o_QuoteReasonClosedCode
),
RTR_InsertUpdate AS (
	SELECT
	lkp_ClosedQuoteDimId,
	ChangeFlag,
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	CreatedDate,
	ModifiedDate,
	QuoteNumber,
	QuoteReasonClosedCode,
	QuoteReasonClosedDescription,
	QuoteReasonClosedComments,
	EDWQuoteAKID,
	EDWQuotePKID
	FROM EXP_ExistingChecking
),
RTR_InsertUpdate_Insert AS (SELECT * FROM RTR_InsertUpdate WHERE ChangeFlag = 'Insert'),
RTR_InsertUpdate_Update AS (SELECT * FROM RTR_InsertUpdate WHERE ChangeFlag = 'Update'),
UPD_Insert AS (
	SELECT
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	CreatedDate, 
	ModifiedDate, 
	QuoteNumber, 
	QuoteReasonClosedCode, 
	QuoteReasonClosedDescription, 
	QuoteReasonClosedComments, 
	EDWQuoteAKID, 
	EDWQuotePKID
	FROM RTR_InsertUpdate_Insert
),
TGT_ClosedQuoteDim_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ClosedQuoteDim
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, QuoteNumber, QuoteReasonClosedCode, QuoteReasonClosedDescription, QuoteReasonClosedComments, EDWQuoteAKId, EDWQuotePKId)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AuditID AS AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	QUOTENUMBER, 
	QUOTEREASONCLOSEDCODE, 
	QUOTEREASONCLOSEDDESCRIPTION, 
	QUOTEREASONCLOSEDCOMMENTS, 
	EDWQuoteAKID AS EDWQUOTEAKID, 
	EDWQuotePKID AS EDWQUOTEPKID
	FROM UPD_Insert
),
UPD_Update AS (
	SELECT
	lkp_ClosedQuoteDimId AS ClosedQuoteDimId, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	CreatedDate, 
	ModifiedDate, 
	QuoteNumber, 
	QuoteReasonClosedCode, 
	QuoteReasonClosedDescription, 
	QuoteReasonClosedComments, 
	EDWQuoteAKID, 
	EDWQuotePKID
	FROM RTR_InsertUpdate_Update
),
TGT_ClosedQuoteDim_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ClosedQuoteDim AS T
	USING UPD_Update AS S
	ON T.ClosedQuoteDimId = S.ClosedQuoteDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.QuoteNumber = S.QuoteNumber, T.QuoteReasonClosedCode = S.QuoteReasonClosedCode, T.QuoteReasonClosedDescription = S.QuoteReasonClosedDescription, T.QuoteReasonClosedComments = S.QuoteReasonClosedComments, T.EDWQuoteAKId = S.EDWQuoteAKID, T.EDWQuotePKId = S.EDWQuotePKID
),
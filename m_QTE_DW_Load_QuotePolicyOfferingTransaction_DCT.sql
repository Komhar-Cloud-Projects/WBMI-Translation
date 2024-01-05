WITH
SQ_DCTWorkTables AS (
	SELECT WorkDCTPolicy.PolicyGUId, WorkDCTPolicy.QuoteActionTimeStamp, WorkDCTPolicy.SessionId, WorkDCTPolicy.Division, WorkDCTPolicy.WBProduct, WorkDCTPolicy.WBProductType, WorkDCTInsuranceLine.LineType, WorkDCTPolicy.PolicyNumber, WorkDCTPolicy.PolicyEffectiveDate, WorkDCTPolicy.PolicyExpirationDate 
	FROM
	 WorkDCTPolicy, WorkDCTInsuranceLine 
	WHERE
	 WorkDCTPolicy.PolicyId=WorkDCTInsuranceLine.PolicyId
	and
	WorkDCTPolicy.QuoteActionTimeStamp is not null
	and WorkDCTPolicy.TransactionType='New'
	@{pipeline().parameters.WHERE_CLAUSE}
	Order By 1,2
),
EXP_Source_Fields AS (
	SELECT
	PolicyGUId,
	QuoteActionTimeStamp,
	SessionId,
	Division,
	WBProduct,
	WBProductType,
	LineType,
	PolicyNumber,
	PolicyEffectiveDate,
	PolicyExpirationDate
	FROM SQ_DCTWorkTables
),
LKP_HistoryStage AS (
	SELECT
	WrittenPremium,
	PolicyNumber,
	PolicyEffectiveDate,
	ExpirationDate
	FROM (
		SELECT 
			WrittenPremium,
			PolicyNumber,
			PolicyEffectiveDate,
			ExpirationDate
		FROM HistoryStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber,PolicyEffectiveDate,ExpirationDate ORDER BY WrittenPremium) = 1
),
AGG_RemoveDuplicates AS (
	SELECT
	EXP_Source_Fields.CoverageGUID AS i_CoverageGUID, 
	EXP_Source_Fields.PolicyGUId AS QuoteKey, 
	EXP_Source_Fields.QuoteActionTimeStamp AS StatusDate, 
	EXP_Source_Fields.SessionId AS i_SessionId, 
	EXP_Source_Fields.Division, 
	EXP_Source_Fields.WBProduct, 
	EXP_Source_Fields.WBProductType, 
	EXP_Source_Fields.LineType AS LType, 
	LKP_HistoryStage.WrittenPremium AS Written
	FROM EXP_Source_Fields
	QUALIFY ROW_NUMBER() OVER (PARTITION BY i_CoverageGUID, QuoteKey, StatusDate ORDER BY NULL) = 1
),
EXP_GetValues AS (
	SELECT
	QuoteKey AS i_QuoteKey,
	StatusDate AS i_StatusDate,
	Division AS i_Division,
	WBProduct AS i_WBProduct,
	WBProductType AS i_WBProductType,
	LType AS i_LType,
	Written AS i_Written,
	-- *INF*: LTRIM(RTRIM(i_QuoteKey))
	LTRIM(RTRIM(i_QuoteKey)) AS o_QuoteKey,
	i_StatusDate AS o_StatusDate,
	-- *INF*: IIF(ISNULL(i_Division) OR IS_SPACES(i_Division) OR LENGTH(i_Division)=0, 'N/A', LTRIM(RTRIM(i_Division)))
	IFF(i_Division IS NULL OR IS_SPACES(i_Division) OR LENGTH(i_Division) = 0, 'N/A', LTRIM(RTRIM(i_Division))) AS o_Division,
	-- *INF*: IIF(ISNULL(i_WBProduct) OR IS_SPACES(i_WBProduct) OR LENGTH(i_WBProduct)=0, 'N/A', LTRIM(RTRIM(i_WBProduct)))
	IFF(i_WBProduct IS NULL OR IS_SPACES(i_WBProduct) OR LENGTH(i_WBProduct) = 0, 'N/A', LTRIM(RTRIM(i_WBProduct))) AS o_WBProduct,
	-- *INF*: IIF(ISNULL(i_WBProductType) OR IS_SPACES(i_WBProductType) OR LENGTH(i_WBProductType)=0, 'N/A', LTRIM(RTRIM(i_WBProductType)))
	IFF(i_WBProductType IS NULL OR IS_SPACES(i_WBProductType) OR LENGTH(i_WBProductType) = 0, 'N/A', LTRIM(RTRIM(i_WBProductType))) AS o_WBProductType,
	-- *INF*: IIF(ISNULL(i_LType) OR IS_SPACES(i_LType) OR LENGTH(i_LType)=0, 'N/A', LTRIM(RTRIM(i_LType)))
	IFF(i_LType IS NULL OR IS_SPACES(i_LType) OR LENGTH(i_LType) = 0, 'N/A', LTRIM(RTRIM(i_LType))) AS o_LType,
	-- *INF*: IIF(ISNULL(i_Written),0,i_Written)
	IFF(i_Written IS NULL, 0, i_Written) AS o_WrittenPremium
	FROM AGG_RemoveDuplicates
),
LKP_Quote AS (
	SELECT
	QuoteAKId,
	QuoteId,
	StatusDate,
	QuoteKey
	FROM (
		Select 
		b.QuoteAKId as QuoteAKId,
		b.QuoteId as QuoteId,
		b.QuoteKey as QuoteKey,
		b.StatusDate as StatusDate
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.Quote b
		where b.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and exists (select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy w
		where w.PolicyGUId=b.QuoteKey)
		order by b.StatusDate,b.QuoteKey
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StatusDate,QuoteKey ORDER BY QuoteAKId) = 1
),
EXP_NewFlag AS (
	SELECT
	LKP_Quote.QuoteAKId AS i_QuoteAKId,
	LKP_Quote.QuoteId AS i_QuoteId,
	EXP_GetValues.o_StatusDate AS StatusDate,
	EXP_GetValues.o_WrittenPremium AS WrittenPremium,
	-- *INF*: IIF(ISNULL(i_QuoteAKId),-1,i_QuoteAKId)
	IFF(i_QuoteAKId IS NULL, - 1, i_QuoteAKId) AS o_QuoteAKId
	FROM EXP_GetValues
	LEFT JOIN LKP_Quote
	ON LKP_Quote.StatusDate = EXP_GetValues.o_StatusDate AND LKP_Quote.QuoteKey = EXP_GetValues.o_QuoteKey
),
AGG_SumPremium AS (
	SELECT
	WrittenPremium AS i_WrittenPremium, 
	SUM(i_WrittenPremium) AS WrittenPremium, 
	o_QuoteAKId AS QuoteAKId, 
	StatusDate, 
	i_QuoteId
	FROM EXP_NewFlag
	GROUP BY QuoteAKId, StatusDate, i_QuoteId
),
lkp_QuotePolicyOfferingTransaction AS (
	SELECT
	QuotePolicyOfferingTransactionId,
	QuotePolicyOfferingTransactionAKID,
	QuoteAKID,
	QuoteID
	FROM (
		SELECT 
			QuotePolicyOfferingTransactionId,
			QuotePolicyOfferingTransactionAKID,
			QuoteAKID,
			QuoteID
		FROM QuotePolicyOfferingTransaction
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY QuoteAKID,QuoteID ORDER BY QuotePolicyOfferingTransactionId) = 1
),
FIL_NewFlag AS (
	SELECT
	lkp_QuotePolicyOfferingTransaction.QuotePolicyOfferingTransactionId, 
	AGG_SumPremium.WrittenPremium, 
	AGG_SumPremium.QuoteAKId, 
	AGG_SumPremium.StatusDate, 
	AGG_SumPremium.i_QuoteId, 
	lkp_QuotePolicyOfferingTransaction.QuotePolicyOfferingTransactionAKID
	FROM AGG_SumPremium
	LEFT JOIN lkp_QuotePolicyOfferingTransaction
	ON lkp_QuotePolicyOfferingTransaction.QuoteAKID = AGG_SumPremium.QuoteAKId AND lkp_QuotePolicyOfferingTransaction.QuoteID = AGG_SumPremium.i_QuoteId
	WHERE ISNULL(QuotePolicyOfferingTransactionId)
),
SEQ_QuotePolicyOfferingTransactionAKID AS (
	CREATE SEQUENCE SEQ_QuotePolicyOfferingTransactionAKID
	START = 0
	INCREMENT = 1;
),
EXP_GetSupportIds AS (
	SELECT
	QuotePolicyOfferingTransactionAKID AS lkp_QuotePolicyOfferingTransactionAKID,
	SEQ_QuotePolicyOfferingTransactionAKID.NEXTVAL AS i_NEXTVAL,
	WrittenPremium AS i_WrittenPremium,
	QuoteAKId AS i_QuoteAKId,
	StatusDate AS i_StatusDate,
	i_QuoteId,
	-- *INF*: IIF(i_QuoteAKId=v_prev_QuoteAKId,v_NEXTVAL,i_NEXTVAL)
	IFF(i_QuoteAKId = v_prev_QuoteAKId, v_NEXTVAL, i_NEXTVAL) AS v_NEXTVAL,
	i_QuoteAKId AS v_prev_QuoteAKId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	-- *INF*: iif(isnull(lkp_QuotePolicyOfferingTransactionAKID),i_NEXTVAL,lkp_QuotePolicyOfferingTransactionAKID)
	IFF(lkp_QuotePolicyOfferingTransactionAKID IS NULL, i_NEXTVAL, lkp_QuotePolicyOfferingTransactionAKID) AS o_QuotePolicyOfferingTransactionAKID,
	i_QuoteId AS o_QuoteId,
	i_QuoteAKId AS o_QuoteAKId,
	i_WrittenPremium AS o_WrittenPremium
	FROM FIL_NewFlag
),
UPD_INSERT_QuotepolicyOfferingTransaction AS (
	SELECT
	o_AuditID, 
	o_SourceSystemID, 
	o_CreatedDate, 
	o_ModifiedDate, 
	o_QuotePolicyOfferingTransactionAKID, 
	o_QuoteId, 
	o_QuoteAKId, 
	o_WrittenPremium
	FROM EXP_GetSupportIds
),
QuotePolicyOfferingTransaction AS (
	INSERT INTO QuotePolicyOfferingTransaction
	(AuditID, SourceSystemID, CreatedDate, ModifiedDate, QuotePolicyOfferingTransactionAKID, QuoteID, QuoteAKID, QuotePremium)
	SELECT 
	o_AuditID AS AUDITID, 
	o_SourceSystemID AS SOURCESYSTEMID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_QuotePolicyOfferingTransactionAKID AS QUOTEPOLICYOFFERINGTRANSACTIONAKID, 
	o_QuoteId AS QUOTEID, 
	o_QuoteAKId AS QUOTEAKID, 
	o_WrittenPremium AS QUOTEPREMIUM
	FROM UPD_INSERT_QuotepolicyOfferingTransaction
),
SQ_QuotePolicyOfferingTransaction AS (
	SELECT QuotePolicyOfferingTransaction.QuotePolicyOfferingTransactionId, QuotePolicyOfferingTransaction.QuoteID, QuotePolicyOfferingTransaction.QuoteAKID, QuotePolicyOfferingTransaction.QuotePremium, Quote.QuoteNumber, Quote.QuoteEffectiveDate, Quote.QuoteExpirationDate 
	 FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.QuotePolicyOfferingTransaction
	 inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.quote on quote.QuoteAKID = QuotePolicyOfferingTransaction.QuoteAKId and quote.CurrentSnapshotFlag =1
),
EXP_Source_Columns AS (
	SELECT
	QuotePolicyOfferingTransactionId,
	QuoteID,
	QuoteAKID,
	QuotePremium,
	QuoteNumber,
	QuoteEffectiveDate,
	QuoteExpirationDate
	FROM SQ_QuotePolicyOfferingTransaction
),
LKP_HistoryStage_update AS (
	SELECT
	WrittenPremium,
	PolicyNumber,
	PolicyEffectiveDate,
	ExpirationDate
	FROM (
		SELECT 
			WrittenPremium,
			PolicyNumber,
			PolicyEffectiveDate,
			ExpirationDate
		FROM HistoryStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber,PolicyEffectiveDate,ExpirationDate ORDER BY WrittenPremium) = 1
),
EXP_Default AS (
	SELECT
	EXP_Source_Columns.QuotePolicyOfferingTransactionId,
	EXP_Source_Columns.QuotePremium,
	LKP_HistoryStage_update.WrittenPremium,
	-- *INF*: IIF(ISNULL(WrittenPremium), 0.0, WrittenPremium)
	IFF(WrittenPremium IS NULL, 0.0, WrittenPremium) AS o_WrittenPremium
	FROM EXP_Source_Columns
	LEFT JOIN LKP_HistoryStage_update
	ON LKP_HistoryStage_update.PolicyNumber = EXP_Source_Columns.QuoteNumber AND LKP_HistoryStage_update.PolicyEffectiveDate = EXP_Source_Columns.QuoteEffectiveDate AND LKP_HistoryStage_update.ExpirationDate = EXP_Source_Columns.QuoteExpirationDate
),
FLTR_Update_records AS (
	SELECT
	QuotePolicyOfferingTransactionId, 
	QuotePremium, 
	o_WrittenPremium AS WrittenPremium
	FROM EXP_Default
	WHERE QuotePremium != WrittenPremium
),
UPD_Update_QuotePolicyOfferingTransaction AS (
	SELECT
	QuotePolicyOfferingTransactionId, 
	WrittenPremium
	FROM FLTR_Update_records
),
QuotePolicyOfferingTransaction_Update AS (
	MERGE INTO QuotePolicyOfferingTransaction AS T
	USING UPD_Update_QuotePolicyOfferingTransaction AS S
	ON T.QuotePolicyOfferingTransactionId = S.QuotePolicyOfferingTransactionId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.QuotePremium = S.WrittenPremium
),
WITH
SQ_DC_Transaction AS (
	SELECT DC_Transaction.TransactionId, DC_Transaction.SessionId, DC_Transaction.Id, DC_Transaction.Type, DC_Transaction.State, DC_Transaction.EffectiveDate, DC_Transaction.ScheduleDate, DC_Transaction.CreatedDate, DC_Transaction.CreatedUser, DC_Transaction.OriginalCharge, DC_Transaction.Charge, DC_Transaction.ProRateFactor, DC_Transaction.TermPremium, DC_Transaction.PriorPremium, DC_Transaction.NewPremium, DC_Transaction.HistoryID, DC_Transaction.ConvertedTransactionType, DC_Transaction.CancellationDate, DC_Transaction.TransactionDate, DC_Transaction.ExpirationDate, DC_Transaction.Deposit, DC_Transaction.AuditCharge, DC_Transaction.AuditPremium, DC_Transaction.StatusUserContext, DC_Transaction.StatusUser, DC_Transaction.PolicyStatus, DC_Transaction.IssuedDate, DC_Transaction.IssuedUserName 
	FROM
	DC_Transaction
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session on
	@{pipeline().parameters.SOURCE_TABLE_OWNER}. DC_Transaction.SessionId=@{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session.SessionId
	WHERE
	DC_Session.CreateDateTime >=  '@{pipeline().parameters.SELECTION_START_TS}'
	and 
	DC_Session.CreateDateTime <  '@{pipeline().parameters.SELECTION_END_TS}'
	ORDER BY
	DC_Transaction.SessionId
),
Exp_DC_Transaction AS (
	SELECT
	TransactionId,
	SessionId,
	Id,
	Type,
	State,
	EffectiveDate,
	ScheduleDate,
	CreatedDate,
	CreatedUser,
	OriginalCharge,
	Charge,
	ProRateFactor,
	TermPremium,
	PriorPremium,
	NewPremium,
	HistoryID,
	ConvertedTransactionType,
	CancellationDate,
	TransactionDate,
	ExpirationDate,
	Deposit,
	AuditCharge,
	AuditPremium,
	StatusUserContext,
	StatusUser,
	PolicyStatus,
	IssuedDate,
	IssuedUserName,
	Sysdate AS ExtractDate,
	'DCT' AS SourceSystemID
	FROM SQ_DC_Transaction
),
DCTransactionStage AS (
	INSERT INTO Shortcut_to_DCTransactionStage
	(ExtractDate, SourceSystemid, TransactionId, SessionId, Id, Type, State, EffectiveDate, ScheduleDate, CreatedDate, CreatedUser, OriginalCharge, Charge, ProRateFactor, TermPremium, PriorPremium, NewPremium, HistoryID, ConvertedTransactionType, CancellationDate, TransactionDate, ExpirationDate, Deposit, AuditCharge, AuditPremium, StatusUserContext, StatusUser, PolicyStatus, IssuedDate, IssuedUserName)
	SELECT 
	EXTRACTDATE, 
	SourceSystemID AS SOURCESYSTEMID, 
	TRANSACTIONID, 
	SESSIONID, 
	ID, 
	TYPE, 
	STATE, 
	EFFECTIVEDATE, 
	SCHEDULEDATE, 
	CREATEDDATE, 
	CREATEDUSER, 
	ORIGINALCHARGE, 
	CHARGE, 
	PRORATEFACTOR, 
	TERMPREMIUM, 
	PRIORPREMIUM, 
	NEWPREMIUM, 
	HISTORYID, 
	CONVERTEDTRANSACTIONTYPE, 
	CANCELLATIONDATE, 
	TRANSACTIONDATE, 
	EXPIRATIONDATE, 
	DEPOSIT, 
	AUDITCHARGE, 
	AUDITPREMIUM, 
	STATUSUSERCONTEXT, 
	STATUSUSER, 
	POLICYSTATUS, 
	ISSUEDDATE, 
	ISSUEDUSERNAME
	FROM Exp_DC_Transaction
),
WITH
SQ_DCTransactionStaging AS (
	WITH cte_DCTransaction(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.TransactionId, 
	X.SessionId, 
	X.Id, 
	X.Type, 
	X.State, 
	X.EffectiveDate, 
	X.ScheduleDate, 
	X.CreatedDate, 
	X.CreatedUser, 
	X.OriginalCharge, 
	X.Charge, 
	X.ProRateFactor,
	X.ShortRateFactor, 
	X.TermPremium, 
	X.PriorPremium, 
	X.NewPremium, 
	X.HistoryID, 
	X.ConvertedTransactionType, 
	X.CancellationDate, 
	X.TransactionDate, 
	X.ExpirationDate, 
	X.Deposit, 
	X.AuditCharge, 
	X.AuditPremium, 
	X.StatusUserContext, 
	X.StatusUser, 
	X.PolicyStatus, 
	X.IssuedDate, 
	X.IssuedUserName 
	FROM
	DC_Transaction X
	inner join
	cte_DCTransaction Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
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
	ShortRateFactor,
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
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DCTransactionStaging
),
DCTransactionStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCTransactionStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCTransactionStaging
	(TransactionId, SessionId, Id, Type, State, EffectiveDate, ScheduleDate, CreatedDate, CreatedUser, OriginalCharge, Charge, ProRateFactor, TermPremium, PriorPremium, NewPremium, HistoryID, ConvertedTransactionType, CancellationDate, TransactionDate, ExpirationDate, Deposit, AuditCharge, AuditPremium, StatusUserContext, StatusUser, PolicyStatus, IssuedDate, IssuedUserName, ExtractDate, SourceSystemId, ShortRateFactor)
	SELECT 
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
	ISSUEDUSERNAME, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	SHORTRATEFACTOR
	FROM EXP_Metadata

	------------ POST SQL ----------
	delete from DCTransactionStaging where SessionId in (select SessionId from DCTransactionStaging where ISNULL(HistoryID,0)=0)
	-------------------------------


),
WITH
SQ_DC_Policy AS (
	WITH cte_DCPolicy(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.PolicyId, 
	X.SessionId, 
	X.Id, 
	X.EffectiveDate, 
	X.ExpirationDate, 
	X.LineOfBusiness, 
	X.Term, 
	X.PrimaryRatingState, 
	X.Product, 
	X.HonorRates, 
	X.AuditPeriod, 
	X.SICCode, 
	X.SICCodeDesc, 
	X.NAICSCode, 
	X.NAICSCodeDesc, 
	X.QuoteNumber, 
	X.TermFactor, 
	X.CancellationDate, 
	X.Description, 
	X.PolicyNumber, 
	X.Status, 
	X.TransactionDate, 
	X.TransactionDateTime, 
	X.PreviousPolicyNumber, 
	X.InceptionDate, 
	X.PolicyTermID, 
	X.AccountID, 
	X.TaxesSurcharges, 
	X.Auditable 
	FROM
	DC_Policy X
	inner join
	cte_DCPolicy Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	PolicyId,
	SessionId,
	Id,
	EffectiveDate,
	ExpirationDate,
	LineOfBusiness,
	Term,
	PrimaryRatingState,
	Product,
	HonorRates,
	AuditPeriod,
	SICCode,
	SICCodeDesc,
	NAICSCode,
	NAICSCodeDesc,
	QuoteNumber,
	TermFactor,
	CancellationDate,
	Description,
	PolicyNumber,
	Status,
	TransactionDate,
	TransactionDateTime,
	PreviousPolicyNumber,
	InceptionDate,
	PolicyTermID,
	AccountID,
	TaxesSurcharges,
	Auditable,
	-- *INF*: DECODE(HonorRates, 'T', 1, 'F', 0, NULL)
	DECODE(
	    HonorRates,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_HonorRates,
	-- *INF*: DECODE(Auditable, 'T', 1, 'F', 0, NULL)
	DECODE(
	    Auditable,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Auditable,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_Policy
),
DCPolicyStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCPolicyStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCPolicyStaging
	(PolicyId, SessionId, Id, EffectiveDate, ExpirationDate, LineOfBusiness, Term, PrimaryRatingState, Product, HonorRates, AuditPeriod, SICCode, SICCodeDesc, NAICSCode, NAICSCodeDesc, QuoteNumber, TermFactor, CancellationDate, Description, PolicyNumber, Status, TransactionDate, TransactionDateTime, PreviousPolicyNumber, InceptionDate, PolicyTermID, AccountID, TaxesSurcharges, Auditable, ExtractDate, SourceSystemId)
	SELECT 
	POLICYID, 
	SESSIONID, 
	ID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	LINEOFBUSINESS, 
	TERM, 
	PRIMARYRATINGSTATE, 
	PRODUCT, 
	o_HonorRates AS HONORRATES, 
	AUDITPERIOD, 
	SICCODE, 
	SICCODEDESC, 
	NAICSCODE, 
	NAICSCODEDESC, 
	QUOTENUMBER, 
	TERMFACTOR, 
	CANCELLATIONDATE, 
	DESCRIPTION, 
	POLICYNUMBER, 
	STATUS, 
	TRANSACTIONDATE, 
	TRANSACTIONDATETIME, 
	PREVIOUSPOLICYNUMBER, 
	INCEPTIONDATE, 
	POLICYTERMID, 
	ACCOUNTID, 
	TAXESSURCHARGES, 
	o_Auditable AS AUDITABLE, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),
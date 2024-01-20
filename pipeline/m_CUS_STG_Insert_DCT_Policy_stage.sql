WITH
SQ_DC_Policy AS (
	SELECT DC_Policy.PolicyId, DC_Policy.SessionId, DC_Policy.Id, DC_Policy.EffectiveDate, DC_Policy.ExpirationDate, DC_Policy.LineOfBusiness, DC_Policy.Term, DC_Policy.PrimaryRatingState, DC_Policy.Product, DC_Policy.HonorRates, DC_Policy.AuditPeriod, DC_Policy.SICCode, DC_Policy.SICCodeDesc, DC_Policy.NAICSCode, DC_Policy.NAICSCodeDesc, DC_Policy.QuoteNumber, DC_Policy.TermFactor, DC_Policy.CancellationDate, DC_Policy.Description, DC_Policy.PolicyNumber, DC_Policy.Status, DC_Policy.TransactionDate, DC_Policy.TransactionDateTime, DC_Policy.PreviousPolicyNumber, DC_Policy.InceptionDate, DC_Policy.PolicyTermID, DC_Policy.AccountID, DC_Policy.TaxesSurcharges, DC_Policy.Auditable 
	FROM
	DC_Policy
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session on
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy.SessionId=@{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session.SessionId
	WHERE
	DC_Session.CreateDateTime >=  '@{pipeline().parameters.SELECTION_START_TS}'
	and 
	DC_Session.CreateDateTime <  '@{pipeline().parameters.SELECTION_END_TS}'
	ORDER BY
	DC_Policy.SessionId
),
Exp_Policy AS (
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
	Sysdate AS ExtractDate,
	'DCT' AS SourceSystemID
	FROM SQ_DC_Policy
),
DCPolicyStage AS (
	INSERT INTO Shortcut_to_DCPolicyStage
	(ExtractDate, SourceSystemid, PolicyId, SessionId, Id, EffectiveDate, ExpirationDate, LineOfBusiness, Term, PrimaryRatingState, Product, HonorRates, AuditPeriod, SICCode, SICCodeDesc, NAICSCode, NAICSCodeDesc, QuoteNumber, TermFactor, CancellationDate, Description, PolicyNumber, Status, TransactionDate, TransactionDateTime, PreviousPolicyNumber, InceptionDate, PolicyTermID, AccountID, TaxesSurcharges, Auditable)
	SELECT 
	EXTRACTDATE, 
	SourceSystemID AS SOURCESYSTEMID, 
	POLICYID, 
	SESSIONID, 
	ID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	LINEOFBUSINESS, 
	TERM, 
	PRIMARYRATINGSTATE, 
	PRODUCT, 
	HONORRATES, 
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
	AUDITABLE
	FROM Exp_Policy
),
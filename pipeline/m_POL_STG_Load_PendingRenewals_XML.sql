WITH
SQ_History AS (
	SELECT h.PolicyNumber
	FROM  @{pipeline().parameters.SOURCE_TABLE_OWNER}.History h, @{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote q WITH (NOLOCK)
	where h.PolicyNumber = q.PolicyNumber
	and q.Status <> 'Cancelled'
	and h.QuoteID = q.QuoteID
	and h.PolicyStatus = 'InForce'
	and (h.PolicyEffectiveDate > GetDate() + 50 AND h.PolicyEffectiveDate < GetDate() + 51)
	and h.Type = 'Renew'
	and h.TransactionStatus = 'Scheduled'
	and h.DeprecatedBy IS NULL
	and h.Deleted = 0
	and h.DuplicateRow = 0
	@{pipeline().parameters.WHERE_CLAUSE}
	Order by h.PolicyNumber
),
EXP_History AS (
	SELECT
	PolicyNumber
	FROM SQ_History
),
PendingRenewals_XML AS (
	INSERT INTO PendingRenewals_XML
	(PolicyNumber)
	SELECT 
	POLICYNUMBER
	FROM EXP_History
),
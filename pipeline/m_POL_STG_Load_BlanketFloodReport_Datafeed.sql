WITH
SQ_BlanketFlood AS (
	select distinct dcp.PolicyNumber as PolicyNumber
	,wbp.PolicyVersionFormatted as PolicyMod
	,sc.Value as Subline
	,t.Type as TransactionType
	,t.TransactionDate as TransactionDate
	,t.EffectiveDate as TransactionEffectiveDate
	,wbp.TotalFloodLimit as TotalFloodLimit
	,wbp.TotalFloodDeductible as TotalFloodDeductible
	,cfline.TotalFloodPremium as TotalFloodPremium
	,cfline.TotalFloodChangePremium as TotalFloodChangePremium
	,cfline.TotalFloodWrittenPremium as TotalFloodWrittenPremium
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy dcp with(nolock)
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy wbp with(nolock) 
	on dcp.PolicyId = wbp.PolicyId
	and wbp.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_CF_Line cfline with(nolock)
	on cfline.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction t with(nolock)
	on t.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_StatCode sc with(nolock)
	on sc.SessionId = dcp.SessionId
	and sc.ObjectName = 'DC_Coverage'
	and sc.Type = 'Subline'
	where wbp.WBProduct = 'Commercial Package'
	and datepart(quarter, t.TransactionDate) = @{pipeline().parameters.PREV_QUARTER}
	and year(t.TransactionDate) = @{pipeline().parameters.PREV_QUARTER_YEAR}
	and sc.Value in ('060', '061')
	@{pipeline().parameters.WHERE_CLAUSE}
),
SRT_BlanketFlood AS (
	SELECT
	PolicyNumber, 
	PolicyMod, 
	Subline, 
	TransactionType, 
	TransactionDate, 
	TransactionEffectiveDate, 
	TotalFloodLimit, 
	TotalFloodDeductible, 
	TotalFloodPremium, 
	TotalFloodChangePremium, 
	TotalFloodWrittenPremium
	FROM SQ_BlanketFlood
	ORDER BY PolicyNumber ASC, PolicyMod ASC, TransactionEffectiveDate ASC
),
EXP_BlanketFlood AS (
	SELECT
	PolicyNumber AS i_PolicyNumber,
	-- *INF*: CHR(39) || i_PolicyNumber || CHR(39)
	CHR(39) || i_PolicyNumber || CHR(39) AS o_PolicyNumber,
	PolicyMod AS i_PolicyMod,
	-- *INF*: CHR(39) || i_PolicyMod || CHR(39)
	CHR(39) || i_PolicyMod || CHR(39) AS o_PolicyMod,
	Subline AS i_Subline,
	-- *INF*: CHR(39) || i_Subline || CHR(39)
	CHR(39) || i_Subline || CHR(39) AS o_Subline,
	TransactionType,
	TransactionDate,
	TransactionEffectiveDate,
	TotalFloodLimit,
	TotalFloodDeductible,
	TotalFloodPremium,
	TotalFloodChangePremium,
	TotalFloodWrittenPremium
	FROM SRT_BlanketFlood
),
BlanketFloodReport_Datafeed AS (
	INSERT INTO BlanketFloodReport_Datafeed
	(PolicyNumber, PolicyMod, Subline, TransactionType, TransactionDate, TransactionEffectiveDate, TotalFloodLimit, TotalFloodDeductible, TotalFloodPremium, TotalFloodChangePremium, TotalFloodWrittenPremium)
	SELECT 
	o_PolicyNumber AS POLICYNUMBER, 
	o_PolicyMod AS POLICYMOD, 
	o_Subline AS SUBLINE, 
	TRANSACTIONTYPE, 
	TRANSACTIONDATE, 
	TRANSACTIONEFFECTIVEDATE, 
	TOTALFLOODLIMIT, 
	TOTALFLOODDEDUCTIBLE, 
	TOTALFLOODPREMIUM, 
	TOTALFLOODCHANGEPREMIUM, 
	TOTALFLOODWRITTENPREMIUM
	FROM EXP_BlanketFlood
),
WITH
DCTDividendFact AS (
	select DCTDividendFact.DividendPaidAmount, 
	AgencyDim.AgencyCode, 
	policy_dim.pol_key, 
	InsuranceReferenceDim.StrategicProfitCenterAbbreviation, 
	calendar_dim.clndr_id , calendar_dim.clndr_date
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCTDividendFact
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Policy_dim on DCTDividendFact.PolicyDimId = Policy_dim.pol_dim_id 
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim on DCTDividendFact.InsuranceReferenceDimId = InsuranceReferenceDim.InsuranceReferenceDimId 
	inner join @{pipeline().parameters.TARGET_TABLE_OWNER_V3}.AgencyDim on AgencyDim.AgencyDimID = DCTDividendFact.AgencyDimID 
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim on DCTDividendFact.DividendRunDateId = calendar_dim.clndr_id
	@{pipeline().parameters.WHERE_CLAUSE}
),
AGG_Transactions_ByPolicyAgency AS (
	SELECT
	DividendPaidAmount, 
	sum(DividendPaidAmount) AS o_DividendPaidAmount, 
	AgencyCode, 
	pol_key, 
	StrategicProfitCenterAbbreviation, 
	clndr_date
	FROM DCTDividendFact
	GROUP BY pol_key, clndr_date
),
EXP_Unsupress_zeros_PolicyKey AS (
	SELECT
	StrategicProfitCenterAbbreviation,
	pol_key,
	-- *INF*: to_char('"'||pol_key)
	-- --CONCAT('"',pol_key)
	-- --pol_key
	-- 
	-- --chr(34)||pol_key||chr(34)
	to_char('"' || pol_key) AS v_pol_key,
	-- *INF*: --replacestr(2, v_pol_key,'"','')
	-- v_pol_key
	v_pol_key AS o_pol_key,
	o_DividendPaidAmount,
	AgencyCode,
	clndr_date,
	-- *INF*: --IIF(ISNULL(clndr_date),'1800-01-01','"' || clndr_date )
	-- IIF(ISNULL(clndr_date),'1800-01-01', to_char(clndr_date ))
	IFF(clndr_date IS NULL, '1800-01-01', to_char(clndr_date)) AS v_clndr_date
	FROM AGG_Transactions_ByPolicyAgency
),
YTDDCTDividendExtract AS (
	INSERT INTO YTDDCTDividendExtract
	(StrategicProfitCenter, PolicyKey, DividendPaid, AgencyCode, DividendRunDate)
	SELECT 
	StrategicProfitCenterAbbreviation AS STRATEGICPROFITCENTER, 
	o_pol_key AS POLICYKEY, 
	o_DividendPaidAmount AS DIVIDENDPAID, 
	AGENCYCODE, 
	clndr_date AS DIVIDENDRUNDATE
	FROM EXP_Unsupress_zeros_PolicyKey
),
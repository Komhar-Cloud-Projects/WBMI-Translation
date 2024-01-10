WITH
SQ_RiskGradeCode_DWP AS (
	Declare @Date1 as datetime
	
	Set @Date1=DATEADD(DD,-1,DATEADD(mm, DATEDIFF(m,0,GETDATE())-(@{pipeline().parameters.NO_OF_MONTHS}),0))
	
	
	select
	PMF.PremiumMasterDirectWrittenPremium,
	CDD.RiskGradeCode,
	IRD.StrategicProfitCenterCode,
	P.pol_mod ,
	P.pol_issue_code
	from 
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterFact PMF
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailDim CDD on PMF.CoverageDetailDimId = CDD.CoverageDetailDimId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim IRD on PMF.InsuranceReferenceDimId = IRD.InsuranceReferenceDimId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim P on PMF.policydimid = P.pol_dim_id 
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCurrentStatusDim pcsd on PCSD.EDWPolicyAKId = P.edw_pol_ak_id and PCSD.PolicyStatusDescription = 'Inforce'
	where IRD.StrategicProfitCenterAbbreviation  IN  ( 'WB - CL' ) and IRD.InsuranceSegmentDescription != 'Pool Services'
	and PCSD.RunDate = @Date1
),
EXP_RiskGradeCode_DWP_New_Renewal AS (
	SELECT
	PremiumMasterDirectWrittenPremium,
	RiskGradeCode,
	StrategicProfitCenterCode,
	pol_mod,
	pol_issue_code,
	-- *INF*: DECODE(TRUE, pol_issue_code='N', 'New',
	-- 'Renewal')
	DECODE(TRUE,
		pol_issue_code = 'N', 'New',
		'Renewal') AS v_PolicyType,
	-- *INF*: IIF(v_PolicyType='New',PremiumMasterDirectWrittenPremium,0)
	IFF(v_PolicyType = 'New', PremiumMasterDirectWrittenPremium, 0) AS NewPolicyPremium,
	-- *INF*: IIF(v_PolicyType='Renewal', PremiumMasterDirectWrittenPremium, 0)
	IFF(v_PolicyType = 'Renewal', PremiumMasterDirectWrittenPremium, 0) AS RenewalPolicyPremium
	FROM SQ_RiskGradeCode_DWP
),
AGG_New_and_Renewal AS (
	SELECT
	RiskGradeCode,
	NewPolicyPremium,
	RenewalPolicyPremium,
	-- *INF*: round(Sum(NewPolicyPremium),2)
	round(Sum(NewPolicyPremium), 2) AS TotalNewPremium,
	-- *INF*: round(SUM(RenewalPolicyPremium),2)
	round(SUM(RenewalPolicyPremium), 2) AS TotalRenewalPremium
	FROM EXP_RiskGradeCode_DWP_New_Renewal
	GROUP BY RiskGradeCode
),
EXP_PassValues AS (
	SELECT
	RiskGradeCode,
	TotalNewPremium,
	TotalRenewalPremium
	FROM AGG_New_and_Renewal
),
RTR_Detail_and_Summary AS (
	SELECT
	RiskGradeCode,
	TotalNewPremium,
	TotalRenewalPremium
	FROM EXP_PassValues
),
RTR_Detail_and_Summary_Detail AS (SELECT * FROM RTR_Detail_and_Summary WHERE TRUE),
RTR_Detail_and_Summary_Summary AS (SELECT * FROM RTR_Detail_and_Summary WHERE TRUE),
EXP_Transform_summary AS (
	SELECT
	RiskGradeCode,
	'Total' AS SummaryCode,
	TotalNewPremium,
	TotalRenewalPremium
	FROM RTR_Detail_and_Summary_Summary
),
AGG_Summary AS (
	SELECT
	SummaryCode,
	TotalNewPremium,
	-- *INF*: Round(Sum(TotalNewPremium),2)
	Round(Sum(TotalNewPremium), 2) AS Sum_TotalNewPremium,
	TotalRenewalPremium,
	-- *INF*: Round(Sum(TotalRenewalPremium),2)
	Round(Sum(TotalRenewalPremium), 2) AS Sum_TotalRenewalPremium
	FROM EXP_Transform_summary
	GROUP BY 
),
Union AS (
	SELECT RiskGradeCode, TotalNewPremium, TotalRenewalPremium
	FROM RTR_Detail_and_Summary_Detail
	UNION
	SELECT SummaryCode AS RiskGradeCode, Sum_TotalNewPremium AS TotalNewPremium, Sum_TotalRenewalPremium AS TotalRenewalPremium
	FROM AGG_Summary
),
EXP_Pass_Val AS (
	SELECT
	RiskGradeCode,
	TotalNewPremium,
	TotalRenewalPremium
	FROM Union
),
SRT_RiskgradeCode AS (
	SELECT
	RiskGradeCode, 
	TotalNewPremium, 
	TotalRenewalPremium
	FROM EXP_Pass_Val
	ORDER BY RiskGradeCode ASC
),
RiskGradeDWPExtract AS (
	INSERT INTO RiskGradeDWPExtract
	(IndustryRiskGradeCode, NewDirectWrittenPremium, RenewalDirectWrittenPremium)
	SELECT 
	RiskGradeCode AS INDUSTRYRISKGRADECODE, 
	TotalNewPremium AS NEWDIRECTWRITTENPREMIUM, 
	TotalRenewalPremium AS RENEWALDIRECTWRITTENPREMIUM
	FROM SRT_RiskgradeCode
),
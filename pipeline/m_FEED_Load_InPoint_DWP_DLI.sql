WITH
SQ_Loss AS (
	declare @ProcessDate as datetime
	
	
	set @ProcessDate=cast(DATEADD(s,-1,DATEADD(mm, DATEDIFF(m,-1,GETDATE()) + @{pipeline().parameters.NO_OF_MONTH},0)) as date);
	
	-- Below Temp table have Last month Policies and if it has Inforce & Future Inforce, We will take only Future Inforce information else Inforce information.
	
	with temp_policy as(select * from(
	select B.pol_sym,B.pol_num,A.EDWPolicyAKId,
	B.pol_mod,A.PolicyStatusDescription,A.Policykey,B.pol_dim_id,Row_number() over(Partition by B.pol_num order by B.pol_mod Desc) 
	Policy_Rank,
	isnull(lead(A.PolicyKey) over(Partition by B.pol_num order by B.pol_mod Desc),A.PolicyKey) PriorPolicyKey,
	dateadd(YYYY,-3,isnull(lead(B.pol_exp_date) over(Partition by B.pol_num order by B.pol_mod Desc),B.Pol_exp_date) )PriorPolicyExpirationDate
	from PolicyCurrentStatusDim A
	inner join policy_dim B
	on A.PolicyDimId=B.pol_dim_id 
	where A.Rundate=@ProcessDate
	and A.PolicyStatusDescription in ('Inforce')  
	)A
	where Policy_Rank=1) 
	
	-- Result of the temp table policies used as main source for below SQL and Calculated Current Year DirectWrittenPremium for current mod policy andLast three years Premiums for previous mod.
	
	select 
	f.cust_num,
	b.pol_num,
	g.PolicyOfferingDescription,
	f.name,
	b.pol_eff_date,
	case when B.pol_key=C.PriorPolicyKey then A.DirectLossIncurredIR else 0 end DirectLossIncurredIR,
	case when b.pol_eff_date>=c.PriorPolicyExpirationDate then A.DirectLossIncurredIR else 0 end DirectLossIncurredIR3Yrs,
	g.StrategicProfitCenterDescription,
	 b.ProgramCode,
	b.ProgramDescription,
	cdd.RiskGradeCode,
	k.AgencyCode,  
	k.AbbreviatedName , 
	j.UnderwritingRegionCodeDescription,
	b.pol_issue_code,
	b.prim_bus_class_code,
	b.prim_bus_class_code_descript 
	from dbo.vwLossMasterFact A inner join policy_dim B on A.pol_dim_id=B.pol_dim_id 
	inner join temp_policy C on  B.pol_num=C.Pol_num  
	inner join calendar_dim D on A.loss_master_run_date_id=D.clndr_id
	INNER JOIN dbo.AgencyEmployeeDim e ON (e.AgencyEmployeeDimID=b.AgencyEmployeeDimID)
	INNER JOIN dbo.contract_customer_dim f ON (f.contract_cust_dim_id=a.contract_cust_dim_id)  
	INNER JOIN dbo.InsuranceReferenceDim g ON (g.InsuranceReferenceDimId=a.InsuranceReferenceDimId)
	INNER JOIN V3.AgencyDim h ON (h.AgencyDimID=a.AgencyDimId)
	INNER JOIN dbo.SalesDivisionDim i ON (i.SalesDivisionDimID=h.SalesDivisionDimId)
	INNER JOIN DBO.underwritingdivisiondim J ON J.underwritingdivisiondimid = b.underwritingdivisiondimid
	inner join dbo.CoverageDetailDim cdd on cdd.CoverageDetailDimId = a.CoverageDetailDimId
	inner join V3.AgencyDim k ON (k.EDWAgencyAKID=h.EDWAgencyAKID and  @ProcessDate between k.effectivedate and k.expirationdate)
	
	where (
	   g.EnterpriseGroupDescription  =  'West Bend Mutual Insurance Group'
	   AND 
	   g.InsuranceSegmentDescription='Commercial Lines' and
	   g.StrategicProfitCenterAbbreviation  IN  ('WB - CL', 'NSI','Argent' )) 
	    and @ProcessDate >= d.CalendarDate 
	order by 1,2
),
Exp_PassThroughLoos AS (
	SELECT
	CustomerNumber,
	PolicyNumber,
	PolicyOfferingDescription,
	FirstNamedInsured,
	PolicyEffectiveDate,
	DirectLossIncurredIR,
	DirectLossIncurredIR3Yrs,
	StrategicProfitCenterDescription,
	ProgramCode,
	ProgramDescription,
	IndustryRiskGradeCode,
	AgencyCode,
	AbbreviatedAgencyName,
	UnderwritingRegionCodeDescription,
	PolicyIssueCode,
	prim_bus_class_code,
	prim_bus_class_code_descript
	FROM SQ_Loss
),
Agg_DirectLoss AS (
	SELECT
	CustomerNumber,
	PolicyNumber,
	PolicyOfferingDescription,
	FirstNamedInsured,
	PolicyEffectiveDate,
	DirectLossIncurredIR,
	-- *INF*: sum(DirectLossIncurredIR)
	sum(DirectLossIncurredIR) AS o_DirectLossIncurredIR,
	DirectLossIncurredIR3Yrs,
	-- *INF*: sum(DirectLossIncurredIR3Yrs)
	sum(DirectLossIncurredIR3Yrs) AS o_DirectLossIncurredIR3Yrs,
	StrategicProfitCenterDescription,
	ProgramCode,
	ProgramDescription,
	IndustryRiskGradeCode,
	AgencyCode,
	AbbreviatedAgencyName,
	UnderwritingRegionCodeDescription,
	PolicyIssueCode,
	prim_bus_class_code,
	prim_bus_class_code_descript
	FROM Exp_PassThroughLoos
	GROUP BY CustomerNumber, PolicyNumber
),
SQ_DWP AS (
	declare @ProcessDate as datetime
	
	
	set @ProcessDate=cast(DATEADD(s,-1,DATEADD(mm, DATEDIFF(m,-1,GETDATE()) + @{pipeline().parameters.NO_OF_MONTH},0)) as date);
	
	-- Below Temp table have Last month Policies and if it has Inforce & Future Inforce, We will take only Future Inforce information else Inforce information.
	
	with temp_policy as(select * from(
	select B.pol_sym,B.pol_num,A.EDWPolicyAKId,
	B.pol_mod,A.PolicyStatusDescription,A.Policykey,B.pol_dim_id,Row_number() over(Partition by B.pol_num order by B.pol_mod Desc) 
	Policy_Rank,
	isnull(lead(A.PolicyKey) over(Partition by B.pol_num order by B.pol_mod Desc),A.PolicyKey) PriorPolicyKey,
	dateadd(YYYY,-3,isnull(lead(B.pol_exp_date) over(Partition by B.pol_num order by B.pol_mod Desc),B.Pol_exp_date) )PriorPolicyExpirationDate
	from PolicyCurrentStatusDim A
	inner join policy_dim B
	on A.PolicyDimId=B.pol_dim_id 
	where A.Rundate=@ProcessDate
	and A.PolicyStatusDescription in ('Inforce')  
	)A
	where Policy_Rank=1)  
	
	-- Result of the temp table policies used as main source for below SQL and Calculated Current Year DirectWrittenPremium for current mod policy andLast three years Premiums for previous mod.
	
	select 
	First_Value(f.cust_num)  OVER (PARTITION BY b.pol_num,f.cust_num ORDER BY b.pol_eff_date desc)  cust_num,
	b.pol_num,
	First_Value(g.PolicyOfferingDescription)  OVER (PARTITION BY b.pol_num,f.cust_num ORDER BY b.pol_eff_date desc) PolicyOfferingDescription,
	f.name,
	FIRST_VALUE(b.pol_eff_date) OVER (PARTITION BY b.pol_num,f.cust_num ORDER BY b.pol_eff_date desc) pol_eff_date,
	case when B.pol_key=C.PolicyKey then A.PremiumMasterDirectWrittenPremium else 0 end Written_Premium,
	case when B.pol_key=C.PriorPolicyKey then A.MonthlyChangeInDirectEarnedPremium else 0 end InforceEarnedPremium, 
	case when b.pol_eff_date>=PriorPolicyExpirationDate then A.MonthlyChangeInDirectEarnedPremium else 0 end MonthlyChangeInDirectEarnedPremium3Yrs,
	g.StrategicProfitCenterDescription,
	FIRST_VALUE(b.ProgramCode) OVER (PARTITION BY b.pol_num,f.cust_num ORDER BY b.pol_eff_date desc) ProgramCode,
	FIRST_VALUE(b.ProgramDescription) OVER (PARTITION BY b.pol_num,f.cust_num ORDER BY b.pol_eff_date desc) ProgramDescription,
	--FIRST_VALUE(b.industry_risk_grade_code) OVER (PARTITION BY b.pol_num,f.cust_num ORDER BY b.pol_eff_date desc) industry_risk_grade_code,
	c.EDWPolicyAKId,
	FIRST_VALUE(k.AgencyCode) OVER (PARTITION BY b.pol_num,f.cust_num ORDER BY b.pol_eff_date desc)  AgencyCode ,  
	FIRST_VALUE(k.AbbreviatedName) OVER (PARTITION BY b.pol_num,f.cust_num ORDER BY b.pol_eff_date desc)  AbbreviatedName, 
	FIRST_VALUE(j.UnderwritingRegionCodeDescription) OVER (PARTITION BY b.pol_num,f.cust_num ORDER BY b.pol_eff_date desc)  UnderwritingRegionCodeDescription, 
	FIRST_VALUE(B.pol_issue_code) OVER (PARTITION BY b.pol_num,f.cust_num ORDER BY b.pol_eff_date desc) pol_issue_code,
	FIRST_VALUE(B.prim_bus_class_code) OVER (PARTITION BY b.pol_num,f.cust_num ORDER BY b.pol_eff_date desc) prim_bus_class_code,
	FIRST_VALUE(B.prim_bus_class_code_descript) OVER (PARTITION BY b.pol_num,f.cust_num ORDER BY b.pol_eff_date desc) prim_bus_class_code_descript,
	g.InsuranceReferenceLineOfBusinessDescription,
	FIRST_VALUE(j.UnderwriterDisplayName) OVER (PARTITION BY b.pol_num,f.cust_num ORDER BY b.pol_eff_date desc)  UnderWriterName, 
	FIRST_VALUE(j.UnderwriterManagerDisplayName) OVER (PARTITION BY b.pol_num,f.cust_num ORDER BY b.pol_eff_date desc)  UnderWriterManagerName,
	i.SalesTerritoryCodeDescription,
	FIRST_VALUE(j.UnderwriterManagerEmailAddress) OVER (PARTITION BY b.pol_num,f.cust_num ORDER BY b.pol_eff_date desc)  UnderwriterManagerEmailAddress,
	FIRST_VALUE(j.UnderwriterManagerCode) OVER (PARTITION BY b.pol_num,f.cust_num ORDER BY b.pol_eff_date desc)  UnderwriterManagerCode,
	FIRST_VALUE(j.UnderwriterEmailAddress) OVER (PARTITION BY b.pol_num,f.cust_num ORDER BY b.pol_eff_date desc)  UnderwriterEmailAddress,
	FIRST_VALUE(j.UnderwriterCode) OVER (PARTITION BY b.pol_num,f.cust_num ORDER BY b.pol_eff_date desc)  UnderwriterCode
	FROM 
	PremiumMonthlySummaryFact A inner join policy_dim B on A.PolicyDimId=B.pol_dim_id 
	inner join temp_policy C on  B.pol_num=C.Pol_num 
	inner join calendar_dim D on A.SnapshotDateId=D.clndr_id
	INNER JOIN dbo.AgencyEmployeeDim e ON (e.AgencyEmployeeDimID=b.AgencyEmployeeDimID)
	INNER JOIN dbo.contract_customer_dim f ON (f.contract_cust_dim_id=a.ContractCustomerDimId)  
	INNER JOIN dbo.InsuranceReferenceDim g ON (g.InsuranceReferenceDimId=a.InsuranceReferenceDimId)
	INNER JOIN V3.AgencyDim h ON (h.AgencyDimID=a.AgencyDimId)
	INNER JOIN dbo.SalesDivisionDim i ON (i.SalesDivisionDimID=h.SalesDivisionDimId)
	INNER JOIN dBO.underwritingdivisiondim J ON J.underwritingdivisiondimid = B.underwritingdivisiondimid
	inner join V3.AgencyDim k ON (k.EDWAgencyAKID=h.EDWAgencyAKID and  @ProcessDate between k.effectivedate and k.expirationdate)
	where (
	   g.EnterpriseGroupDescription  =  'West Bend Mutual Insurance Group'
	   AND 
	   g.InsuranceSegmentDescription='Commercial Lines' and
	   g.StrategicProfitCenterAbbreviation  IN  ('WB - CL', 'NSI' ,'Argent')) and @ProcessDate >= d.CalendarDate
	order by 1,2
),
Exp_PassThrough AS (
	SELECT
	CustomerNumber,
	PolicyNumber,
	PolicyOfferingDescription,
	FirstNamedInsured,
	PolicyEffectiveDate,
	DirectWrittenPremium,
	InforceEarnedPremium,
	MonthlyChangeInDirectEarnedPremium3Yrs,
	StrategicProfitCenterDescription,
	ProgramCode,
	ProgramDescription,
	EDWPolicyAKId,
	AgencyCode,
	AbbreviatedAgencyName,
	UnderwritingRegionCodeDescription,
	PolicyIssueCode,
	prim_bus_class_code,
	prim_bus_class_code_descript,
	InsuranceReferenceLineOfBusinessDescription,
	UnderWriterName,
	UnderWriterManagerName,
	SalesTerritoryCodeDescription,
	UnderwriterManagerEmailAddress,
	UnderwriterManagerCode,
	UnderwriterEmailAddress,
	UnderwriterCode
	FROM SQ_DWP
),
SQ_policy_dim AS (
	declare @ProcessDate as datetime
	set @ProcessDate=cast(DATEADD(s,-1,DATEADD(mm, DATEDIFF(m,-1,GETDATE()) + @{pipeline().parameters.NO_OF_MONTH},0)) as date);
	select EDWPolicyAKId from(
	select B.pol_sym,B.pol_num,A.EDWPolicyAKId,
	B.pol_mod,A.PolicyStatusDescription,A.Policykey,B.pol_dim_id,Row_number() over(Partition by B.pol_num order by B.pol_mod Desc) 
	Policy_Rank,
	isnull(lead(A.PolicyKey) over(Partition by B.pol_num order by B.pol_mod Desc),A.PolicyKey) PriorPolicyKey,
	dateadd(YYYY,-3,isnull(lead(B.pol_exp_date) over(Partition by B.pol_num order by B.pol_mod Desc),B.Pol_exp_date) )PriorPolicyExpirationDate
	from PolicyCurrentStatusDim A
	inner join policy_dim B
	on A.PolicyDimId=B.pol_dim_id 
	where A.Rundate=@ProcessDate
	and A.PolicyStatusDescription in ('Inforce')  
	)A
	where Policy_Rank=1
),
mplt_Determine_RiskGrade_Code_and_Description AS (WITH
	INPUT AS (
		
	),
	LKP_PMF_CDD_RiskGradeCode AS (
		SELECT
		PremiumMasterPremium,
		RiskGradeCode,
		InsuranceLineCode,
		in_PolicyAKID,
		edw_pol_ak_id
		FROM (
			SELECT 
			SUM(pmf.PremiumMasterPremium) as PremiumMasterPremium,
			cdd.RiskGradeCode as RiskGradeCode,
			ircd.InsuranceLineCode as InsuranceLineCode,
			pd.edw_pol_ak_id as edw_pol_ak_id 
			FROM policy_dim pd
			inner join dbo.PremiumMasterFact pmf on pmf.PolicyDimID = pd.pol_dim_id
			inner join dbo.InsuranceReferenceCoverageDim ircd on ircd.InsuranceReferenceCoverageDimId = pmf.InsuranceReferenceCoverageDimId
			inner join dbo.CoverageDetailDim cdd on cdd.CoverageDetailDimId = pmf.CoverageDetailDimId
			group by cdd.RiskGradeCode,ircd.InsuranceLineCode,pd.edw_pol_ak_id
			order by pd.edw_pol_ak_id,ircd.InsuranceLineCode,cdd.RiskGradeCode
			--
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_pol_ak_id ORDER BY PremiumMasterPremium) = 1
	),
	AGG_Pol_InsuranceLine_RiskGrade AS (
		SELECT
		in_PolicyAKID,
		InsuranceLineCode,
		RiskGradeCode,
		PremiumMasterPremium,
		-- *INF*: ROUND(SUM(PremiumMasterPremium),4)
		ROUND(SUM(PremiumMasterPremium), 4) AS NetPremium
		FROM LKP_PMF_CDD_RiskGradeCode
		GROUP BY in_PolicyAKID, InsuranceLineCode, RiskGradeCode
	),
	FLT_LOB_Premium_not_zero AS (
		SELECT
		in_PolicyAKID, 
		InsuranceLineCode, 
		RiskGradeCode, 
		NetPremium
		FROM AGG_Pol_InsuranceLine_RiskGrade
		WHERE NetPremium<>0.0
	),
	EXP_Translate_RiskGradeCode AS (
		SELECT
		in_PolicyAKID,
		InsuranceLineCode,
		RiskGradeCode,
		-- *INF*: DECODE (TRUE, RiskGradeCode='1' , 1 ,
		-- RiskGradeCode='2' , 2 ,
		-- RiskGradeCode='3' , 3 ,
		-- RiskGradeCode='4' , 4 ,
		-- RiskGradeCode='5' , 5 ,
		-- RiskGradeCode='6' , 6 ,
		-- RiskGradeCode='7' , 7 ,
		-- RiskGradeCode='8' , 8 ,
		-- RiskGradeCode='9' , 9 ,
		-- RiskGradeCode='0' , 0 ,
		-- RiskGradeCode='D' , 10 ,
		-- RiskGradeCode='N/A' , -3 ,
		-- RiskGradeCode='DNW' , 11 ,
		-- RiskGradeCode='NSI' , -1 ,
		-- RiskGradeCode='Argent' , -2 ,-3)
		DECODE(
		    TRUE,
		    RiskGradeCode = '1', 1,
		    RiskGradeCode = '2', 2,
		    RiskGradeCode = '3', 3,
		    RiskGradeCode = '4', 4,
		    RiskGradeCode = '5', 5,
		    RiskGradeCode = '6', 6,
		    RiskGradeCode = '7', 7,
		    RiskGradeCode = '8', 8,
		    RiskGradeCode = '9', 9,
		    RiskGradeCode = '0', 0,
		    RiskGradeCode = 'D', 10,
		    RiskGradeCode = 'N/A', - 3,
		    RiskGradeCode = 'DNW', 11,
		    RiskGradeCode = 'NSI', - 1,
		    RiskGradeCode = 'Argent', - 2,
		    - 3
		) AS RiskGradeValue
		FROM FLT_LOB_Premium_not_zero
	),
	AGG_Max_RiskGrade_Pol AS (
		SELECT
		in_PolicyAKID,
		InsuranceLineCode,
		RiskGradeValue,
		-- *INF*: MAX(RiskGradeValue)
		MAX(RiskGradeValue) AS o_maxRiskGradeValue
		FROM EXP_Translate_RiskGradeCode
		GROUP BY in_PolicyAKID
	),
	EXP_Decode_RiskGradeValue AS (
		SELECT
		in_PolicyAKID,
		o_maxRiskGradeValue,
		-- *INF*: DECODE (TRUE,o_maxRiskGradeValue=1 , '1' ,
		-- o_maxRiskGradeValue=2 , '2' ,
		-- o_maxRiskGradeValue=3 , '3' ,
		-- o_maxRiskGradeValue=4 , '4' ,
		-- o_maxRiskGradeValue=5 , '5' ,
		-- o_maxRiskGradeValue=6 , '6' ,
		-- o_maxRiskGradeValue=7 , '7' ,
		-- o_maxRiskGradeValue=8 , '8' ,
		-- o_maxRiskGradeValue=9 , '9' ,
		-- o_maxRiskGradeValue=0 , '0' ,
		-- o_maxRiskGradeValue=10 , 'D' ,
		-- o_maxRiskGradeValue=-3 , 'N/A' ,
		-- o_maxRiskGradeValue=11 , 'DNW' ,
		-- o_maxRiskGradeValue=-1 , 'NSI' ,
		-- o_maxRiskGradeValue=-2 , 'Argent' ,'N/A')
		DECODE(
		    TRUE,
		    o_maxRiskGradeValue = 1, '1',
		    o_maxRiskGradeValue = 2, '2',
		    o_maxRiskGradeValue = 3, '3',
		    o_maxRiskGradeValue = 4, '4',
		    o_maxRiskGradeValue = 5, '5',
		    o_maxRiskGradeValue = 6, '6',
		    o_maxRiskGradeValue = 7, '7',
		    o_maxRiskGradeValue = 8, '8',
		    o_maxRiskGradeValue = 9, '9',
		    o_maxRiskGradeValue = 0, '0',
		    o_maxRiskGradeValue = 10, 'D',
		    o_maxRiskGradeValue = - 3, 'N/A',
		    o_maxRiskGradeValue = 11, 'DNW',
		    o_maxRiskGradeValue = - 1, 'NSI',
		    o_maxRiskGradeValue = - 2, 'Argent',
		    'N/A'
		) AS RiskGradeCode,
		-- *INF*: DECODE (TRUE,o_maxRiskGradeValue=1 , 'Excellent' ,
		-- o_maxRiskGradeValue=2 , 'Excellent' ,
		-- o_maxRiskGradeValue=3 , 'Good' ,
		-- o_maxRiskGradeValue=4 , 'Good' ,
		-- o_maxRiskGradeValue=5 , 'Average' ,
		-- o_maxRiskGradeValue=6 , 'Marginal' ,
		-- o_maxRiskGradeValue=7 , 'Marginal' ,
		-- o_maxRiskGradeValue=8 , 'Poor' ,
		-- o_maxRiskGradeValue=9 , 'Poor' ,
		-- o_maxRiskGradeValue=0 , 'NSI Bonds' ,
		-- o_maxRiskGradeValue=10 , 'Do Not Write' ,
		-- o_maxRiskGradeValue=-3 , 'Not Available' ,
		-- o_maxRiskGradeValue=11 , 'Do Not Write' ,
		-- o_maxRiskGradeValue=-1 , 'NSI' ,
		-- o_maxRiskGradeValue=-2 , 'Argent' , 'Not Available')
		DECODE(
		    TRUE,
		    o_maxRiskGradeValue = 1, 'Excellent',
		    o_maxRiskGradeValue = 2, 'Excellent',
		    o_maxRiskGradeValue = 3, 'Good',
		    o_maxRiskGradeValue = 4, 'Good',
		    o_maxRiskGradeValue = 5, 'Average',
		    o_maxRiskGradeValue = 6, 'Marginal',
		    o_maxRiskGradeValue = 7, 'Marginal',
		    o_maxRiskGradeValue = 8, 'Poor',
		    o_maxRiskGradeValue = 9, 'Poor',
		    o_maxRiskGradeValue = 0, 'NSI Bonds',
		    o_maxRiskGradeValue = 10, 'Do Not Write',
		    o_maxRiskGradeValue = - 3, 'Not Available',
		    o_maxRiskGradeValue = 11, 'Do Not Write',
		    o_maxRiskGradeValue = - 1, 'NSI',
		    o_maxRiskGradeValue = - 2, 'Argent',
		    'Not Available'
		) AS RiskGradeDescription
		FROM AGG_Max_RiskGrade_Pol
	),
	OUTPUT AS (
		SELECT
		in_PolicyAKID, 
		RiskGradeCode, 
		RiskGradeDescription
		FROM EXP_Decode_RiskGradeValue
	),
),
JNR_Mapplet_IndustryRiskGeadeCode AS (SELECT
	Exp_PassThrough.CustomerNumber, 
	Exp_PassThrough.PolicyNumber, 
	Exp_PassThrough.PolicyOfferingDescription, 
	Exp_PassThrough.FirstNamedInsured, 
	Exp_PassThrough.PolicyEffectiveDate, 
	Exp_PassThrough.DirectWrittenPremium, 
	Exp_PassThrough.InforceEarnedPremium, 
	Exp_PassThrough.MonthlyChangeInDirectEarnedPremium3Yrs, 
	Exp_PassThrough.StrategicProfitCenterDescription, 
	Exp_PassThrough.ProgramCode, 
	Exp_PassThrough.ProgramDescription, 
	Exp_PassThrough.EDWPolicyAKId, 
	Exp_PassThrough.AgencyCode, 
	Exp_PassThrough.AbbreviatedAgencyName, 
	Exp_PassThrough.UnderwritingRegionCodeDescription, 
	Exp_PassThrough.PolicyIssueCode, 
	Exp_PassThrough.prim_bus_class_code, 
	Exp_PassThrough.prim_bus_class_code_descript, 
	Exp_PassThrough.InsuranceReferenceLineOfBusinessDescription, 
	mplt_Determine_RiskGrade_Code_and_Description.in_PolicyAKID1, 
	mplt_Determine_RiskGrade_Code_and_Description.RiskGradeCode, 
	mplt_Determine_RiskGrade_Code_and_Description.RiskGradeDescription, 
	Exp_PassThrough.UnderWriterName, 
	Exp_PassThrough.UnderWriterManagerName, 
	Exp_PassThrough.SalesTerritoryCodeDescription, 
	Exp_PassThrough.UnderwriterManagerEmailAddress, 
	Exp_PassThrough.UnderwriterManagerCode, 
	Exp_PassThrough.UnderwriterEmailAddress, 
	Exp_PassThrough.UnderwriterCode
	FROM Exp_PassThrough
	INNER JOIN mplt_Determine_RiskGrade_Code_and_Description
	ON mplt_Determine_RiskGrade_Code_and_Description.in_PolicyAKID1 = Exp_PassThrough.EDWPolicyAKId
),
Agg_DirectWrittenPremium AS (
	SELECT
	CustomerNumber,
	PolicyNumber,
	PolicyOfferingDescription,
	FirstNamedInsured,
	PolicyEffectiveDate,
	DirectWrittenPremium,
	-- *INF*: Sum(DirectWrittenPremium)
	Sum(DirectWrittenPremium) AS o_DirectWrittenPremium,
	InforceEarnedPremium,
	-- *INF*: Sum(InforceEarnedPremium)
	Sum(InforceEarnedPremium) AS o_InforceEarnedPremium,
	MonthlyChangeInDirectEarnedPremium3Yrs,
	-- *INF*: Sum(MonthlyChangeInDirectEarnedPremium3Yrs)
	Sum(MonthlyChangeInDirectEarnedPremium3Yrs) AS o_MonthlyChangeInDirectEarnedPremium3Yrs,
	StrategicProfitCenterDescription,
	ProgramCode,
	ProgramDescription,
	EDWPolicyAKId,
	AgencyCode,
	AbbreviatedAgencyName,
	UnderwritingRegionCodeDescription,
	PolicyIssueCode,
	prim_bus_class_code,
	prim_bus_class_code_descript,
	RiskGradeCode,
	UnderWriterName,
	UnderWriterManagerName,
	SalesTerritoryCodeDescription,
	UnderwriterManagerEmailAddress,
	UnderwriterManagerCode,
	UnderwriterEmailAddress,
	UnderwriterCode
	FROM JNR_Mapplet_IndustryRiskGeadeCode
	GROUP BY CustomerNumber, PolicyNumber
),
JNR_DWP_DLI AS (SELECT
	Agg_DirectLoss.CustomerNumber, 
	Agg_DirectLoss.PolicyNumber, 
	Agg_DirectLoss.PolicyOfferingDescription, 
	Agg_DirectLoss.FirstNamedInsured, 
	Agg_DirectLoss.PolicyEffectiveDate, 
	Agg_DirectLoss.o_DirectLossIncurredIR AS DirectLossIncurredIR, 
	Agg_DirectLoss.o_DirectLossIncurredIR3Yrs AS DirectLossIncurredIR3Yrs, 
	Agg_DirectLoss.StrategicProfitCenterDescription, 
	Agg_DirectLoss.ProgramCode, 
	Agg_DirectLoss.ProgramDescription, 
	Agg_DirectLoss.IndustryRiskGradeCode, 
	Agg_DirectLoss.AgencyCode, 
	Agg_DirectLoss.AbbreviatedAgencyName, 
	Agg_DirectLoss.UnderwritingRegionCodeDescription, 
	Agg_DirectLoss.PolicyIssueCode, 
	Agg_DirectWrittenPremium.CustomerNumber AS DWP_CustomerNumber, 
	Agg_DirectWrittenPremium.PolicyNumber AS DWP_PolicyNumber, 
	Agg_DirectWrittenPremium.PolicyOfferingDescription AS DWP_PolicyOfferingDescription, 
	Agg_DirectWrittenPremium.FirstNamedInsured AS DWP_FirstNamedInsured, 
	Agg_DirectWrittenPremium.PolicyEffectiveDate AS DWP_PolicyEffectiveDate, 
	Agg_DirectWrittenPremium.o_DirectWrittenPremium AS DWP_DirectWrittenPremium, 
	Agg_DirectWrittenPremium.o_InforceEarnedPremium AS DWP_InforceEarnedPremium, 
	Agg_DirectWrittenPremium.o_MonthlyChangeInDirectEarnedPremium3Yrs AS DWP_MonthlyChangeInDirectEarnedPremium3Yrs, 
	Agg_DirectWrittenPremium.StrategicProfitCenterDescription AS DWP_StrategicProfitCenterDescription, 
	Agg_DirectWrittenPremium.ProgramCode AS DWP_ProgramCode, 
	Agg_DirectWrittenPremium.ProgramDescription AS DWP_ProgramDescription, 
	Agg_DirectWrittenPremium.RiskGradeCode AS DWP_IndustryRiskGradeCode, 
	Agg_DirectWrittenPremium.AgencyCode AS DWP_AgencyCode, 
	Agg_DirectWrittenPremium.AbbreviatedAgencyName AS DWP_AbbreviatedAgencyName, 
	Agg_DirectWrittenPremium.UnderwritingRegionCodeDescription AS DWP_UnderwritingRegionCodeDescription, 
	Agg_DirectWrittenPremium.PolicyIssueCode AS DWP_PolicyIssueCode, 
	Agg_DirectWrittenPremium.prim_bus_class_code AS DWP_prim_bus_class_code, 
	Agg_DirectWrittenPremium.prim_bus_class_code_descript AS DWP_prim_bus_class_code_descript, 
	Agg_DirectWrittenPremium.UnderWriterName AS DWP_UnderWriterName, 
	Agg_DirectWrittenPremium.UnderWriterManagerName AS DWP_UnderWriterManagerName, 
	Agg_DirectWrittenPremium.SalesTerritoryCodeDescription AS DWP_SalesTerritoryCodeDescription, 
	Agg_DirectWrittenPremium.UnderwriterManagerEmailAddress, 
	Agg_DirectWrittenPremium.UnderwriterManagerCode, 
	Agg_DirectWrittenPremium.UnderwriterEmailAddress, 
	Agg_DirectWrittenPremium.UnderwriterCode
	FROM Agg_DirectLoss
	RIGHT OUTER JOIN Agg_DirectWrittenPremium
	ON Agg_DirectWrittenPremium.CustomerNumber = Agg_DirectLoss.CustomerNumber AND Agg_DirectWrittenPremium.PolicyNumber = Agg_DirectLoss.PolicyNumber
),
Fil_OldRecords AS (
	SELECT
	DWP_CustomerNumber AS CustomerNumber, 
	DWP_PolicyNumber AS PolicyNumber, 
	DWP_PolicyOfferingDescription AS PolicyOfferingDescription, 
	DWP_FirstNamedInsured AS FirstNamedInsured, 
	DWP_PolicyEffectiveDate AS PolicyEffectiveDate, 
	DirectLossIncurredIR, 
	DirectLossIncurredIR3Yrs, 
	DWP_ProgramCode AS ProgramCode, 
	DWP_ProgramDescription AS ProgramDescription, 
	DWP_IndustryRiskGradeCode AS IndustryRiskGradeCode, 
	DWP_AgencyCode AS AgencyCode, 
	DWP_AbbreviatedAgencyName AS AbbreviatedAgencyName, 
	DWP_UnderwritingRegionCodeDescription AS UnderwritingRegionCodeDescription, 
	DWP_PolicyIssueCode AS PolicyIssueCode, 
	DWP_DirectWrittenPremium AS DirectWrittenPremium, 
	DWP_InforceEarnedPremium AS InforceEarnedPremium, 
	DWP_MonthlyChangeInDirectEarnedPremium3Yrs AS MonthlyChangeInDirectEarnedPremium3Yrs, 
	DWP_prim_bus_class_code AS o_prim_bus_class_code, 
	DWP_prim_bus_class_code_descript AS o_prim_bus_class_code_descript, 
	DWP_StrategicProfitCenterDescription AS StrategicProfitCenterDescription, 
	DWP_UnderWriterName AS UnderWriterName, 
	DWP_UnderWriterManagerName AS UnderWriterManagerName, 
	DWP_SalesTerritoryCodeDescription AS SalesTerritoryCodeDescription, 
	UnderwriterManagerEmailAddress, 
	UnderwriterManagerCode, 
	UnderwriterEmailAddress, 
	UnderwriterCode
	FROM JNR_DWP_DLI
	WHERE GET_DATE_PART(PolicyEffectiveDate,'YYYY')>GET_DATE_PART ( Sysdate, 'YYYY' )-2
),
Srt_InsuranceLineofBusiness AS (
	SELECT
	CustomerNumber, 
	PolicyNumber, 
	InsuranceReferenceLineOfBusinessDescription
	FROM Exp_PassThrough
	ORDER BY CustomerNumber ASC, PolicyNumber ASC, InsuranceReferenceLineOfBusinessDescription ASC
),
m_InsuranceLineofBusiness AS (WITH
	Map_InsuranceLineofBusiness AS (
		
	),
	Exp_InsuranceLineofBusiness AS (
		SELECT
		CustomerNumber,
		PolicyNumber,
		InsuranceReferenceLineOfBusinessDescription,
		-- *INF*: IIF(CustomerNumber=v_Prev_CustomerNumber and
		-- PolicyNumber=v_Prev_PolicyNumber
		-- ,InsuranceReferenceLineOfBusinessDescription||';'||v_InsuranceReferenceLineOfBusinessDescription,InsuranceReferenceLineOfBusinessDescription)
		IFF(
		    CustomerNumber = v_Prev_CustomerNumber and PolicyNumber = v_Prev_PolicyNumber,
		    InsuranceReferenceLineOfBusinessDescription || ';' || v_InsuranceReferenceLineOfBusinessDescription,
		    InsuranceReferenceLineOfBusinessDescription
		) AS v_InsuranceReferenceLineOfBusinessDescription,
		CustomerNumber AS v_Prev_CustomerNumber,
		PolicyNumber AS v_Prev_PolicyNumber,
		v_InsuranceReferenceLineOfBusinessDescription AS o_InsuranceReferenceLineOfBusinessDescription
		FROM Map_InsuranceLineofBusiness
	),
	agg AS (
		SELECT
		CustomerNumber,
		PolicyNumber,
		o_InsuranceReferenceLineOfBusinessDescription
		FROM Exp_InsuranceLineofBusiness
		QUALIFY ROW_NUMBER() OVER (PARTITION BY CustomerNumber, PolicyNumber ORDER BY NULL) = 1
	),
	InsuranceLineofBusiness AS (
		SELECT
		CustomerNumber, 
		PolicyNumber, 
		o_InsuranceReferenceLineOfBusinessDescription
		FROM agg
	),
),
Jnr_InsuranceLineOfBusiness AS (SELECT
	Fil_OldRecords.CustomerNumber, 
	Fil_OldRecords.PolicyNumber, 
	Fil_OldRecords.PolicyOfferingDescription, 
	Fil_OldRecords.FirstNamedInsured, 
	Fil_OldRecords.PolicyEffectiveDate, 
	Fil_OldRecords.DirectWrittenPremium, 
	Fil_OldRecords.DirectLossIncurredIR, 
	Fil_OldRecords.InforceEarnedPremium, 
	Fil_OldRecords.MonthlyChangeInDirectEarnedPremium3Yrs, 
	Fil_OldRecords.DirectLossIncurredIR3Yrs, 
	Fil_OldRecords.ProgramCode, 
	Fil_OldRecords.ProgramDescription, 
	Fil_OldRecords.IndustryRiskGradeCode, 
	Fil_OldRecords.AgencyCode, 
	Fil_OldRecords.AbbreviatedAgencyName, 
	Fil_OldRecords.UnderwritingRegionCodeDescription, 
	Fil_OldRecords.PolicyIssueCode, 
	Fil_OldRecords.o_prim_bus_class_code AS prim_bus_class_code, 
	Fil_OldRecords.o_prim_bus_class_code_descript AS prim_bus_class_code_descript, 
	Fil_OldRecords.StrategicProfitCenterDescription, 
	m_InsuranceLineofBusiness.CustomerNumber1 AS InsuranceLOB_CustomerNumber, 
	m_InsuranceLineofBusiness.PolicyNumber1 AS InsuranceLOB_PolicyNumber, 
	m_InsuranceLineofBusiness.o_InsuranceReferenceLineOfBusinessDescription AS InsuranceReferenceLineOfBusinessDescription, 
	Fil_OldRecords.UnderWriterName, 
	Fil_OldRecords.UnderWriterManagerName, 
	Fil_OldRecords.SalesTerritoryCodeDescription, 
	Fil_OldRecords.UnderwriterManagerEmailAddress, 
	Fil_OldRecords.UnderwriterManagerCode, 
	Fil_OldRecords.UnderwriterEmailAddress, 
	Fil_OldRecords.UnderwriterCode
	FROM Fil_OldRecords
	LEFT OUTER JOIN m_InsuranceLineofBusiness
	ON m_InsuranceLineofBusiness.CustomerNumber1 = Fil_OldRecords.CustomerNumber AND m_InsuranceLineofBusiness.PolicyNumber1 = Fil_OldRecords.PolicyNumber
),
LKP_BusinessClassDim AS (
	SELECT
	BusinessClassCode,
	StrategicBusinessGroupDescription,
	PrimaryBusinessClassificationCode
	FROM (
		SELECT 
			BusinessClassCode,
			StrategicBusinessGroupDescription,
			PrimaryBusinessClassificationCode
		FROM BusinessClassDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY BusinessClassCode ORDER BY BusinessClassCode) = 1
),
Exp_Derived AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	SysDate AS ModifiedDate,
	Jnr_InsuranceLineOfBusiness.CustomerNumber,
	Jnr_InsuranceLineOfBusiness.PolicyNumber,
	Jnr_InsuranceLineOfBusiness.PolicyOfferingDescription,
	Jnr_InsuranceLineOfBusiness.FirstNamedInsured,
	Jnr_InsuranceLineOfBusiness.PolicyEffectiveDate,
	Jnr_InsuranceLineOfBusiness.InforceEarnedPremium,
	-- *INF*: IIF(ISNULL(InforceEarnedPremium),0,InforceEarnedPremium)
	IFF(InforceEarnedPremium IS NULL, 0, InforceEarnedPremium) AS v_InforceEarnedPremium,
	Jnr_InsuranceLineOfBusiness.MonthlyChangeInDirectEarnedPremium3Yrs,
	-- *INF*: IIF(ISNULL(MonthlyChangeInDirectEarnedPremium3Yrs),0,MonthlyChangeInDirectEarnedPremium3Yrs)
	IFF(
	    MonthlyChangeInDirectEarnedPremium3Yrs IS NULL, 0, MonthlyChangeInDirectEarnedPremium3Yrs
	) AS v_MonthlyChangeInDirectEarnedPremium3Yrs,
	Jnr_InsuranceLineOfBusiness.DirectWrittenPremium,
	-- *INF*: IIF(ISNULL(DirectWrittenPremium),0,DirectWrittenPremium)
	IFF(DirectWrittenPremium IS NULL, 0, DirectWrittenPremium) AS o_DirectWrittenPremium,
	Jnr_InsuranceLineOfBusiness.DirectLossIncurredIR,
	-- *INF*: IIF( ISNULL(DirectLossIncurredIR) ,0,
	-- IIF(v_InforceEarnedPremium=0,0,(DirectLossIncurredIR/v_InforceEarnedPremium) * 100 ))
	IFF(
	    DirectLossIncurredIR IS NULL, 0,
	    IFF(
	        v_InforceEarnedPremium = 0, 0, (DirectLossIncurredIR / v_InforceEarnedPremium) * 100
	    )
	) AS DirectLossIncurredRatio,
	Jnr_InsuranceLineOfBusiness.DirectLossIncurredIR3Yrs,
	-- *INF*: IIF(ISNULL(DirectLossIncurredIR3Yrs),0,
	-- IIF(v_MonthlyChangeInDirectEarnedPremium3Yrs=0,0,
	-- (DirectLossIncurredIR3Yrs/v_MonthlyChangeInDirectEarnedPremium3Yrs) * 100))
	IFF(
	    DirectLossIncurredIR3Yrs IS NULL, 0,
	    IFF(
	        v_MonthlyChangeInDirectEarnedPremium3Yrs = 0, 0,
	        (DirectLossIncurredIR3Yrs / v_MonthlyChangeInDirectEarnedPremium3Yrs) * 100
	    )
	) AS DirectLossIncurredRatio_3Yr,
	Jnr_InsuranceLineOfBusiness.prim_bus_class_code,
	Jnr_InsuranceLineOfBusiness.prim_bus_class_code_descript,
	Jnr_InsuranceLineOfBusiness.StrategicProfitCenterDescription,
	Jnr_InsuranceLineOfBusiness.ProgramCode,
	Jnr_InsuranceLineOfBusiness.ProgramDescription,
	Jnr_InsuranceLineOfBusiness.IndustryRiskGradeCode,
	Jnr_InsuranceLineOfBusiness.AgencyCode,
	Jnr_InsuranceLineOfBusiness.AbbreviatedAgencyName,
	Jnr_InsuranceLineOfBusiness.UnderwritingRegionCodeDescription,
	-- *INF*: Decode(TRUE, ltrim(rtrim(upper(UnderwritingRegionCodeDescription)))='EASTERN WISCONSIN' ,'EWI',
	-- ltrim(rtrim(upper(UnderwritingRegionCodeDescription)))='WESTERN WISCONSIN' ,'WWI' ,
	-- ltrim(rtrim(upper(UnderwritingRegionCodeDescription)))='METRO MILWAUKEE' ,'MKE',
	-- upper(SUBSTR(UnderwritingRegionCodeDescription,1,3))
	--   )
	Decode(
	    TRUE,
	    ltrim(rtrim(upper(UnderwritingRegionCodeDescription))) = 'EASTERN WISCONSIN', 'EWI',
	    ltrim(rtrim(upper(UnderwritingRegionCodeDescription))) = 'WESTERN WISCONSIN', 'WWI',
	    ltrim(rtrim(upper(UnderwritingRegionCodeDescription))) = 'METRO MILWAUKEE', 'MKE',
	    upper(SUBSTR(UnderwritingRegionCodeDescription, 1, 3))
	) AS o_UnderwritingRegionCodeDescription,
	Jnr_InsuranceLineOfBusiness.PolicyIssueCode,
	-- *INF*: ADD_TO_DATE(SYSDATE,'MM',-1)
	DATEADD(MONTH,- 1,CURRENT_TIMESTAMP) AS RunDate,
	-- *INF*: IIF (DirectWrittenPremium > 150000 ,'1','0')
	IFF(DirectWrittenPremium > 150000, '1', '0') AS AcctOver150K,
	Jnr_InsuranceLineOfBusiness.InsuranceReferenceLineOfBusinessDescription AS i_InsuranceReferenceLineOfBusinessDescription,
	-- *INF*: i_InsuranceReferenceLineOfBusinessDescription  -- vendor can't exceed 250 chars
	i_InsuranceReferenceLineOfBusinessDescription AS o_InsuranceReferenceLineOfBusinessDescription,
	Jnr_InsuranceLineOfBusiness.UnderWriterName,
	Jnr_InsuranceLineOfBusiness.UnderWriterManagerName,
	LKP_BusinessClassDim.StrategicBusinessGroupDescription,
	Jnr_InsuranceLineOfBusiness.SalesTerritoryCodeDescription,
	Jnr_InsuranceLineOfBusiness.UnderwriterManagerEmailAddress AS i_UnderwriterManagerEmailAddress,
	-- *INF*: lower(i_UnderwriterManagerEmailAddress)
	lower(i_UnderwriterManagerEmailAddress) AS o_UnderwriterManagerEmailAddress,
	Jnr_InsuranceLineOfBusiness.UnderwriterManagerCode,
	Jnr_InsuranceLineOfBusiness.UnderwriterEmailAddress AS i_UnderwriterEmailAddress,
	-- *INF*: lower(i_UnderwriterEmailAddress)
	lower(i_UnderwriterEmailAddress) AS o_UnderwriterEmailAddress,
	Jnr_InsuranceLineOfBusiness.UnderwriterCode
	FROM Jnr_InsuranceLineOfBusiness
	LEFT JOIN LKP_BusinessClassDim
	ON LKP_BusinessClassDim.BusinessClassCode = Jnr_InsuranceLineOfBusiness.prim_bus_class_code
),
CommercialProductManagementExtract AS (
	TRUNCATE TABLE CommercialProductManagementExtract;
	INSERT INTO CommercialProductManagementExtract
	(AuditId, ModifiedDate, CustomerNumber, PolicyNumber, PolicyOfferingDescription, FirstNamedInsured, PolicyEffectiveDate, DirectWrittenPremium, DirectLossIncurred, DirectLossIncurredRatio, DirectLossIncurred3Yrs, DirectLossIncurredRatio3Yrs, PrimaryBusinessClassCode, PrimaryBusinessClassDescription, StrategicProfitCenterDescription, ProgramCode, ProgramDescription, IndustryRiskGradeCode, AgencyCode, AbbreviatedAgencyName, UnderwritingRegionCodeDescription, PolicyIssueCode, RunDate, AccountOver150KFlag, InsuranceReferenceLineOfBusinessDescription, UnderWriterName, UnderWriterManagerName, StrategicBusinessGroupDescription, SalesTerritoryDescription, UnderwriterManagerEmailAddress, UnderwriterManagerCode, UnderwriterEmailAddress, UnderwriterCode)
	SELECT 
	AuditID AS AUDITID, 
	MODIFIEDDATE, 
	CUSTOMERNUMBER, 
	POLICYNUMBER, 
	POLICYOFFERINGDESCRIPTION, 
	FIRSTNAMEDINSURED, 
	POLICYEFFECTIVEDATE, 
	o_DirectWrittenPremium AS DIRECTWRITTENPREMIUM, 
	DirectLossIncurredIR AS DIRECTLOSSINCURRED, 
	DIRECTLOSSINCURREDRATIO, 
	DirectLossIncurredIR3Yrs AS DIRECTLOSSINCURRED3YRS, 
	DirectLossIncurredRatio_3Yr AS DIRECTLOSSINCURREDRATIO3YRS, 
	prim_bus_class_code AS PRIMARYBUSINESSCLASSCODE, 
	prim_bus_class_code_descript AS PRIMARYBUSINESSCLASSDESCRIPTION, 
	STRATEGICPROFITCENTERDESCRIPTION, 
	PROGRAMCODE, 
	PROGRAMDESCRIPTION, 
	INDUSTRYRISKGRADECODE, 
	AGENCYCODE, 
	ABBREVIATEDAGENCYNAME, 
	o_UnderwritingRegionCodeDescription AS UNDERWRITINGREGIONCODEDESCRIPTION, 
	POLICYISSUECODE, 
	RUNDATE, 
	AcctOver150K AS ACCOUNTOVER150KFLAG, 
	o_InsuranceReferenceLineOfBusinessDescription AS INSURANCEREFERENCELINEOFBUSINESSDESCRIPTION, 
	UNDERWRITERNAME, 
	UNDERWRITERMANAGERNAME, 
	STRATEGICBUSINESSGROUPDESCRIPTION, 
	SalesTerritoryCodeDescription AS SALESTERRITORYDESCRIPTION, 
	o_UnderwriterManagerEmailAddress AS UNDERWRITERMANAGEREMAILADDRESS, 
	UNDERWRITERMANAGERCODE, 
	o_UnderwriterEmailAddress AS UNDERWRITEREMAILADDRESS, 
	UNDERWRITERCODE
	FROM Exp_Derived
),
WITH
LKP_BusinessClassDim AS (
	SELECT
	StrategicBusinessGroupDescription,
	BusinessClassCode,
	PrimaryBusinessClassificationCode
	FROM (
		SELECT 
			StrategicBusinessGroupDescription,
			BusinessClassCode,
			PrimaryBusinessClassificationCode
		FROM BusinessClassDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY BusinessClassCode ORDER BY StrategicBusinessGroupDescription) = 1
),
SQ_PremiumTransactionFact AS (
	declare @ProcessDate as datetime
	
	
	set @ProcessDate=cast(DATEADD(s,-1,DATEADD(mm, DATEDIFF(m,-1,GETDATE()) + @{pipeline().parameters.NO_OF_MONTH},0)) as date);
	
	select  
	contract_customer_dim.cust_num,
	policy_dim.pol_num,
	policy_dim.pol_key,
	InsuranceReferenceDim.PolicyOfferingDescription,
	contract_customer_dim.name,
	policy_dim.pol_eff_date,
	dbo.PremiumTransactionFact.DirectWrittenPremium,
	policy_dim.prim_bus_class_code,
	policy_dim.prim_bus_class_code_descript,
	policy_dim.ProgramCode,
	policy_dim.ProgramDescription,
	CDD.RiskGradeCode,
	AgencyDim.AgencyCode,
	AgencyDim.AbbreviatedName,
	policy_dim.pol_issue_code,
	underwritingdivisiondim.UnderwritingRegionCodeDescription,
	SUM(DirectWrittenPremium) OVER (PARTITION BY pol_num,cust_num) as AcctOver150K,
	dbo.InsuranceReferenceDim.InsuranceReferenceLineOfBusinessDescription,
	
	InsuranceReferenceDim.StrategicProfitCenterDescription,
	SalesDivisionDim.SalesTerritoryCodeDescription,
	UnderwritingDivisionDim.UnderwriterDisplayName as UnderWriterName,
	UnderwritingDivisionDim.UnderwriterManagerDisplayName as UnderwriterManagerName,
	UnderwritingDivisionDim.UnderwriterManagerEmailAddress as UnderwriterManagerEmailAddress,
	UnderwritingDivisionDim.UnderwriterManagerCode as UnderwriterManagerCode,
	UnderwritingDivisionDim.UnderwriterEmailAddress as UnderwriterEmailAddress,
	UnderwritingDivisionDim.UnderwriterCode as UnderwriterCode
	
	FROM
	(
	select DISTINCT edw_pol_ak_id from (
	 select 
	pol.pol_dim_id,edw_pol_ak_id,pol_key,pol_eff_date,pol_exp_date,
	ROW_NUMBER() over(partition by pol.pol_num
	 order by edw_pol_ak_id desc,rundate desc) Record_Rank
	 from  PolicyCurrentStatusDim st join Policy_dim pol on pol.pol_dim_id=st.policydimid 
	  where PolicyFutureStatusDescription<>'FutureCancellation'
	   ) a where Record_Rank=1
	 ) pd INNER JOIN dbo.policy_Dim policy_dim on policy_dim.edw_pol_ak_id=pd.edw_pol_ak_id
	 INNER JOIN  dbo.PremiumTransactionFact ON (policy_dim.pol_dim_id=dbo.PremiumTransactionFact.PolicyDimID)
	   INNER JOIN calendar_dim  PremBookedDateDim ON (dbo.PremiumTransactionFact.PremiumTransactionBookedDateID=PremBookedDateDim.clndr_id)
	   INNER JOIN calendar_dim  Transaction_Date_dim ON (dbo.PremiumTransactionFact.PremiumTransactionBookedDateID=Transaction_Date_dim.clndr_id)
	   INNER JOIN dbo.InsuranceReferenceDim ON (dbo.PremiumTransactionFact.InsuranceReferenceDimId=dbo.InsuranceReferenceDim.InsuranceReferenceDimId)
	   INNER JOIN V3.AgencyDim ON (V3.AgencyDim.AgencyDimID=dbo.PremiumTransactionFact.AgencyDimID)
	   INNER JOIN dbo.SalesDivisionDim ON (dbo.SalesDivisionDim.SalesDivisionDimID=V3.AgencyDim.SalesDivisionDimId)
	   INNER JOIN dbo.AgencyEmployeeDim ON (dbo.AgencyEmployeeDim.AgencyEmployeeDimID=policy_dim.AgencyEmployeeDimID)
	   INNER JOIN dbo.underwritingdivisiondim ON policy_dim.underwritingdivisiondimid = underwritingdivisiondim.underwritingdivisiondimid
	   INNER JOIN dbo.contract_customer_dim ON (dbo.contract_customer_dim.contract_cust_dim_id=dbo.PremiumTransactionFact.ContractCustomerDimID)
	   INNER JOIN DBO.CoverageDetailDim CDD ON (CDD.CoverageDetailDimId = dbo.PremiumTransactionFact.CoverageDetailDimId)
	   where
	    dbo.InsuranceReferenceDim.StrategicProfitCenterDescription  IN  ( 'WB - CL','West Bend Commercial Lines','NSI','Argent' )    
		AND policy_dim.pol_issue_code  IN  ( 'N','R' )  and policy_dim.pol_status_code <> 'C'
	 	and dbo.InsuranceReferenceDim.InsuranceSegmentDescription  IN  ('Commercial Lines')  
	  and policy_dim.pol_eff_date > @ProcessDate order by 1,2
),
Exp_passThrough AS (
	SELECT
	CustomerNumber,
	PolicyNumber,
	PolicyKey,
	PolicyOfferingDescription,
	CustomerName,
	PolicyEffectiveDate,
	PremiumMasterDirectWrittenPremium,
	PrimaryBusinessClassCode,
	-- *INF*: :Lkp.LKP_BusinessClassDim(PrimaryBusinessClassCode)
	'' AS o_StrategicBusinessGroupDescription,
	PrimaryBusinessClassCodeDescription,
	ProgramCode,
	ProgramDescription,
	IndustryRiskGradeCode,
	-- *INF*: DECODE (TRUE, IndustryRiskGradeCode='1' , 1 ,
	-- IndustryRiskGradeCode='2' , 2 ,
	-- IndustryRiskGradeCode='3' , 3 ,
	-- IndustryRiskGradeCode='4' , 4 ,
	-- IndustryRiskGradeCode='5' , 5 ,
	-- IndustryRiskGradeCode='6' , 6 ,
	-- IndustryRiskGradeCode='7' , 7 ,
	-- IndustryRiskGradeCode='8' , 8 ,
	-- IndustryRiskGradeCode='9' , 9 ,
	-- IndustryRiskGradeCode='0' , 0 ,
	-- IndustryRiskGradeCode='D' , 10 ,
	-- IndustryRiskGradeCode='N/A' , -3 ,
	-- IndustryRiskGradeCode='DNW' , 11 ,
	-- IndustryRiskGradeCode='NSI' , -1 ,
	-- IndustryRiskGradeCode='Argent' , -2 ,-3)
	DECODE(
	    TRUE,
	    IndustryRiskGradeCode = '1', 1,
	    IndustryRiskGradeCode = '2', 2,
	    IndustryRiskGradeCode = '3', 3,
	    IndustryRiskGradeCode = '4', 4,
	    IndustryRiskGradeCode = '5', 5,
	    IndustryRiskGradeCode = '6', 6,
	    IndustryRiskGradeCode = '7', 7,
	    IndustryRiskGradeCode = '8', 8,
	    IndustryRiskGradeCode = '9', 9,
	    IndustryRiskGradeCode = '0', 0,
	    IndustryRiskGradeCode = 'D', 10,
	    IndustryRiskGradeCode = 'N/A', - 3,
	    IndustryRiskGradeCode = 'DNW', 11,
	    IndustryRiskGradeCode = 'NSI', - 1,
	    IndustryRiskGradeCode = 'Argent', - 2,
	    - 3
	) AS v_IndustryRiskGradeCode,
	v_IndustryRiskGradeCode AS o_IndustryRiskGradeCode,
	AgencyCode,
	AbbreviatedName,
	PolicyIssueCode,
	StateOfDomicileCodeDescription,
	-- *INF*: Decode(TRUE, ltrim(rtrim(upper(StateOfDomicileCodeDescription)))='EASTERN WISCONSIN' ,'EWI',
	-- ltrim(rtrim(upper(StateOfDomicileCodeDescription)))='WESTERN WISCONSIN' ,'WWI' ,
	-- ltrim(rtrim(upper(StateOfDomicileCodeDescription)))='METRO MILWAUKEE' ,'MKE',
	-- upper(SUBSTR(StateOfDomicileCodeDescription,1,3))
	--   )
	Decode(
	    TRUE,
	    ltrim(rtrim(upper(StateOfDomicileCodeDescription))) = 'EASTERN WISCONSIN', 'EWI',
	    ltrim(rtrim(upper(StateOfDomicileCodeDescription))) = 'WESTERN WISCONSIN', 'WWI',
	    ltrim(rtrim(upper(StateOfDomicileCodeDescription))) = 'METRO MILWAUKEE', 'MKE',
	    upper(SUBSTR(StateOfDomicileCodeDescription, 1, 3))
	) AS UWRegion,
	AcctOver150K,
	-- *INF*: IIF(AcctOver150K >150000,1,0)
	IFF(AcctOver150K > 150000, 1, 0) AS o_AcctOver150K,
	PolicyKey AS v_PolicyKey,
	CustomerNumber AS v_CustomerNumber,
	-- *INF*: IIF(PolicyKey=v_PolicyKey and CustomerNumber=v_CustomerNumber ,
	-- LineofBusinessDescription||','||v_LineofBusinessDescription,LineofBusinessDescription)
	IFF(
	    PolicyKey = v_PolicyKey and CustomerNumber = v_CustomerNumber,
	    LineofBusinessDescription || ',' || v_LineofBusinessDescription,
	    LineofBusinessDescription
	) AS v_LineofBusinessDescription,
	v_LineofBusinessDescription AS o_LineofBusinessDescription,
	StrategicProfitCenterDescription,
	SalesTerritoryCodeDescription,
	UnderWriterName,
	UnderWriterManagerName,
	UnderwriterManagerEmailAddress,
	UnderwriterManagerCode,
	UnderwriterEmailAddress,
	UnderwriterCode
	FROM SQ_PremiumTransactionFact
),
AGG_PolNum_RiskGrade AS (
	SELECT
	CustomerNumber,
	PolicyNumber,
	PolicyKey,
	o_IndustryRiskGradeCode,
	PolicyOfferingDescription,
	CustomerName,
	PolicyEffectiveDate,
	PremiumMasterDirectWrittenPremium,
	-- *INF*: Round(SUM(PremiumMasterDirectWrittenPremium),4)
	Round(SUM(PremiumMasterDirectWrittenPremium), 4) AS o_PremiumMasterDirectWrittenPremium,
	PrimaryBusinessClassCode,
	o_StrategicBusinessGroupDescription,
	PrimaryBusinessClassCodeDescription,
	ProgramCode,
	ProgramDescription,
	AgencyCode,
	AbbreviatedName,
	PolicyIssueCode,
	UWRegion,
	o_AcctOver150K,
	o_LineofBusinessDescription,
	StrategicProfitCenterDescription,
	SalesTerritoryCodeDescription,
	UnderWriterName,
	UnderWriterManagerName,
	UnderwriterManagerEmailAddress,
	UnderwriterManagerCode,
	UnderwriterEmailAddress,
	UnderwriterCode
	FROM Exp_passThrough
	GROUP BY CustomerNumber, PolicyNumber, o_IndustryRiskGradeCode
),
FIL_Premium_not_Zero AS (
	SELECT
	CustomerNumber, 
	PolicyNumber, 
	PolicyKey, 
	PolicyOfferingDescription, 
	CustomerName, 
	PolicyEffectiveDate, 
	o_PremiumMasterDirectWrittenPremium AS PremiumMasterDirectWrittenPremium, 
	PrimaryBusinessClassCode, 
	o_StrategicBusinessGroupDescription, 
	PrimaryBusinessClassCodeDescription, 
	ProgramCode, 
	ProgramDescription, 
	o_IndustryRiskGradeCode, 
	AgencyCode, 
	AbbreviatedName, 
	PolicyIssueCode, 
	UWRegion, 
	o_AcctOver150K, 
	o_LineofBusinessDescription, 
	StrategicProfitCenterDescription, 
	SalesTerritoryCodeDescription, 
	UnderWriterName, 
	UnderWriterManagerName, 
	UnderwriterManagerEmailAddress, 
	UnderwriterManagerCode, 
	UnderwriterEmailAddress, 
	UnderwriterCode
	FROM AGG_PolNum_RiskGrade
	WHERE PremiumMasterDirectWrittenPremium<>0.00
),
RTR_New_Renewal AS (
	SELECT
	CustomerNumber,
	PolicyNumber,
	PolicyKey,
	PolicyOfferingDescription,
	CustomerName,
	PolicyEffectiveDate,
	PremiumMasterDirectWrittenPremium,
	PrimaryBusinessClassCode,
	o_StrategicBusinessGroupDescription AS StrategicBusinessGroupDescription,
	PrimaryBusinessClassCodeDescription,
	ProgramCode,
	ProgramDescription,
	o_IndustryRiskGradeCode AS IndustryRiskGradeCode,
	AgencyCode,
	AbbreviatedName,
	PolicyIssueCode,
	UWRegion,
	o_AcctOver150K AS AcctOver150K,
	o_LineofBusinessDescription AS LineofBusinessDescription,
	StrategicProfitCenterDescription,
	SalesTerritoryCodeDescription,
	UnderWriterName,
	UnderWriterManagerName,
	UnderwriterManagerEmailAddress,
	UnderwriterManagerCode,
	UnderwriterEmailAddress,
	UnderwriterCode
	FROM FIL_Premium_not_Zero
),
RTR_New_Renewal_New AS (SELECT * FROM RTR_New_Renewal WHERE PolicyIssueCode='N'),
RTR_New_Renewal_Renewal AS (SELECT * FROM RTR_New_Renewal WHERE PolicyIssueCode='R'),
Lkp_CommercialProductManagementExtract AS (
	SELECT
	CustomerNumber,
	PolicyNumber,
	in_CustomerNumber,
	in_PolicyNumber
	FROM (
		SELECT 
			CustomerNumber,
			PolicyNumber,
			in_CustomerNumber,
			in_PolicyNumber
		FROM CommercialProductManagementExtract
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CustomerNumber,PolicyNumber ORDER BY CustomerNumber) = 1
),
Exp_Insert_Upate AS (
	SELECT
	Lkp_CommercialProductManagementExtract.PolicyNumber AS lkp_PolicyNumber,
	-- *INF*: IIF(isnull(lkp_PolicyNumber),0,1)
	IFF(lkp_PolicyNumber IS NULL, 0, 1) AS Flag,
	RTR_New_Renewal_New.CustomerNumber,
	RTR_New_Renewal_New.PolicyNumber,
	RTR_New_Renewal_New.PolicyKey,
	RTR_New_Renewal_New.PolicyOfferingDescription,
	RTR_New_Renewal_New.CustomerName,
	RTR_New_Renewal_New.PolicyEffectiveDate,
	RTR_New_Renewal_New.PremiumMasterDirectWrittenPremium,
	RTR_New_Renewal_New.PrimaryBusinessClassCode,
	RTR_New_Renewal_New.PrimaryBusinessClassCodeDescription,
	RTR_New_Renewal_New.ProgramCode,
	RTR_New_Renewal_New.ProgramDescription,
	RTR_New_Renewal_New.IndustryRiskGradeCode,
	RTR_New_Renewal_New.AgencyCode,
	RTR_New_Renewal_New.AbbreviatedName,
	RTR_New_Renewal_New.PolicyIssueCode,
	RTR_New_Renewal_New.UWRegion,
	RTR_New_Renewal_New.StrategicBusinessGroupDescription,
	RTR_New_Renewal_New.AcctOver150K,
	RTR_New_Renewal_New.StrategicProfitCenterDescription AS StrategicProfitCenterDescription1,
	RTR_New_Renewal_New.SalesTerritoryCodeDescription AS SalesTerritoryCodeDescription1,
	RTR_New_Renewal_New.UnderWriterName AS UnderWriterName1,
	RTR_New_Renewal_New.UnderWriterManagerName AS UnderWriterManagerName1,
	RTR_New_Renewal_New.UnderwriterManagerEmailAddress AS UnderwriterManagerEmailAddress1,
	RTR_New_Renewal_New.UnderwriterManagerCode AS UnderwriterManagerCode1,
	RTR_New_Renewal_New.UnderwriterEmailAddress AS UnderwriterEmailAddress1,
	RTR_New_Renewal_New.UnderwriterCode AS UnderwriterCode1
	FROM RTR_New_Renewal_New
	LEFT JOIN Lkp_CommercialProductManagementExtract
	ON Lkp_CommercialProductManagementExtract.CustomerNumber = RTR_New_Renewal.CustomerNumber1 AND Lkp_CommercialProductManagementExtract.PolicyNumber = RTR_New_Renewal.PolicyNumber1
),
RTR_Insert_Update AS (
	SELECT
	Flag,
	CustomerNumber,
	PolicyNumber,
	PolicyKey,
	PolicyOfferingDescription,
	CustomerName,
	PolicyEffectiveDate,
	PremiumMasterDirectWrittenPremium,
	PrimaryBusinessClassCode,
	PrimaryBusinessClassCodeDescription,
	ProgramCode,
	ProgramDescription,
	IndustryRiskGradeCode,
	AgencyCode,
	AbbreviatedName,
	PolicyIssueCode,
	UWRegion,
	StrategicBusinessGroupDescription,
	AcctOver150K,
	StrategicProfitCenterDescription1,
	SalesTerritoryCodeDescription1,
	UnderWriterName1,
	UnderWriterManagerName1,
	UnderwriterManagerEmailAddress1,
	UnderwriterManagerCode1,
	UnderwriterEmailAddress1,
	UnderwriterCode1
	FROM Exp_Insert_Upate
),
RTR_Insert_Update_Insert AS (SELECT * FROM RTR_Insert_Update WHERE Flag=0),
RTR_Insert_Update_Update AS (SELECT * FROM RTR_Insert_Update WHERE Flag=1),
Agg_DWP_New AS (
	SELECT
	CustomerNumber,
	PolicyNumber,
	PolicyKey,
	PolicyOfferingDescription,
	CustomerName,
	PolicyEffectiveDate,
	PremiumMasterDirectWrittenPremium,
	PrimaryBusinessClassCode,
	PrimaryBusinessClassCodeDescription,
	ProgramCode,
	ProgramDescription,
	IndustryRiskGradeCode,
	-- *INF*: max(IndustryRiskGradeCode)
	max(IndustryRiskGradeCode) AS o_IndustryRiskGradeCode,
	AgencyCode,
	AbbreviatedName,
	PolicyIssueCode,
	UWRegion AS UnderwritingRegionCodeDescription,
	StrategicBusinessGroupDescription,
	-- *INF*: sum(PremiumMasterDirectWrittenPremium)
	sum(PremiumMasterDirectWrittenPremium) AS o_DWP,
	-- *INF*: IIF(sum(PremiumMasterDirectWrittenPremium) >150000,1,0)
	IFF(sum(PremiumMasterDirectWrittenPremium) > 150000, 1, 0) AS AcctOver150K,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	Sysdate AS CreateDate,
	sysdate AS ModifiedDate,
	-- *INF*: ADD_TO_DATE(SYSDATE,'MM',-1)
	DATEADD(MONTH,- 1,CURRENT_TIMESTAMP) AS RunDate,
	0 AS DirectLossIncurred,
	0 AS DirectLossIncurredRatio,
	0 AS DirectLossIncurred3Yrs,
	0 AS DirectLossIncurredRatio3Yrs,
	StrategicProfitCenterDescription,
	SalesTerritoryCodeDescription,
	UnderWriterName,
	UnderWriterManagerName,
	UnderwriterManagerEmailAddress AS UnderwriterManagerEmailAddress11,
	UnderwriterManagerCode AS UnderwriterManagerCode11,
	UnderwriterEmailAddress AS UnderwriterEmailAddress11,
	UnderwriterCode AS UnderwriterCode11
	FROM RTR_Insert_Update_Insert
	GROUP BY CustomerNumber, PolicyNumber
),
SRT_InsuranceLineofBusiness AS (
	SELECT
	CustomerNumber, 
	PolicyNumber, 
	InsuranceReferenceLineOfBusinessDescription
	FROM SQ_PremiumTransactionFact
	ORDER BY CustomerNumber ASC, PolicyNumber ASC, InsuranceReferenceLineOfBusinessDescription ASC
),
mplt_InsuranceLineofBusiness AS (WITH
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
JNR_New_DWP AS (SELECT
	Agg_DWP_New.CustomerNumber, 
	Agg_DWP_New.PolicyNumber, 
	Agg_DWP_New.PolicyKey, 
	Agg_DWP_New.PolicyOfferingDescription, 
	Agg_DWP_New.CustomerName, 
	Agg_DWP_New.PolicyEffectiveDate, 
	Agg_DWP_New.PrimaryBusinessClassCode, 
	Agg_DWP_New.PrimaryBusinessClassCodeDescription, 
	Agg_DWP_New.ProgramCode, 
	Agg_DWP_New.ProgramDescription, 
	Agg_DWP_New.o_IndustryRiskGradeCode AS IndustryRiskGradeCode, 
	Agg_DWP_New.AgencyCode, 
	Agg_DWP_New.AbbreviatedName, 
	Agg_DWP_New.PolicyIssueCode, 
	Agg_DWP_New.UnderwritingRegionCodeDescription, 
	Agg_DWP_New.StrategicBusinessGroupDescription, 
	Agg_DWP_New.AcctOver150K, 
	Agg_DWP_New.o_DWP AS DWP, 
	Agg_DWP_New.AuditID, 
	Agg_DWP_New.CreateDate, 
	Agg_DWP_New.ModifiedDate, 
	Agg_DWP_New.RunDate, 
	Agg_DWP_New.DirectLossIncurred, 
	Agg_DWP_New.DirectLossIncurredRatio, 
	Agg_DWP_New.DirectLossIncurred3Yrs, 
	Agg_DWP_New.DirectLossIncurredRatio3Yrs, 
	mplt_InsuranceLineofBusiness.CustomerNumber1, 
	mplt_InsuranceLineofBusiness.PolicyNumber1, 
	mplt_InsuranceLineofBusiness.o_InsuranceReferenceLineOfBusinessDescription, 
	Agg_DWP_New.StrategicProfitCenterDescription AS StrategicProfitCenterDescription11, 
	Agg_DWP_New.SalesTerritoryCodeDescription AS SalesTerritoryCodeDescription11, 
	Agg_DWP_New.UnderWriterName AS UnderWriterName11, 
	Agg_DWP_New.UnderWriterManagerName AS UnderWriterManagerName11, 
	Agg_DWP_New.UnderwriterManagerEmailAddress11, 
	Agg_DWP_New.UnderwriterManagerCode11, 
	Agg_DWP_New.UnderwriterEmailAddress11, 
	Agg_DWP_New.UnderwriterCode11
	FROM Agg_DWP_New
	LEFT OUTER JOIN mplt_InsuranceLineofBusiness
	ON mplt_InsuranceLineofBusiness.CustomerNumber1 = Agg_DWP_New.CustomerNumber AND mplt_InsuranceLineofBusiness.PolicyNumber1 = Agg_DWP_New.PolicyNumber
),
EXP_Decoding_Industry_riskgrade_New AS (
	SELECT
	CustomerNumber,
	PolicyNumber,
	PolicyKey,
	PolicyOfferingDescription,
	CustomerName,
	PolicyEffectiveDate,
	PrimaryBusinessClassCode,
	PrimaryBusinessClassCodeDescription,
	ProgramCode,
	ProgramDescription,
	IndustryRiskGradeCode,
	-- *INF*: DECODE (TRUE,IndustryRiskGradeCode=1 , '1' ,
	-- IndustryRiskGradeCode=2 , '2' ,
	-- IndustryRiskGradeCode=3 , '3' ,
	-- IndustryRiskGradeCode=4 , '4' ,
	-- IndustryRiskGradeCode=5 , '5' ,
	-- IndustryRiskGradeCode=6 , '6' ,
	-- IndustryRiskGradeCode=7 , '7' ,
	-- IndustryRiskGradeCode=8 , '8' ,
	-- IndustryRiskGradeCode=9 , '9' ,
	-- IndustryRiskGradeCode=0 , '0' ,
	-- IndustryRiskGradeCode=10 , 'D' ,
	-- IndustryRiskGradeCode=-3 , 'N/A' ,
	-- IndustryRiskGradeCode=11 , 'DNW' ,
	-- IndustryRiskGradeCode=-1 , 'NSI' ,
	-- IndustryRiskGradeCode=-2 , 'Argent' ,'N/A')
	DECODE(
	    TRUE,
	    IndustryRiskGradeCode = 1, '1',
	    IndustryRiskGradeCode = 2, '2',
	    IndustryRiskGradeCode = 3, '3',
	    IndustryRiskGradeCode = 4, '4',
	    IndustryRiskGradeCode = 5, '5',
	    IndustryRiskGradeCode = 6, '6',
	    IndustryRiskGradeCode = 7, '7',
	    IndustryRiskGradeCode = 8, '8',
	    IndustryRiskGradeCode = 9, '9',
	    IndustryRiskGradeCode = 0, '0',
	    IndustryRiskGradeCode = 10, 'D',
	    IndustryRiskGradeCode = - 3, 'N/A',
	    IndustryRiskGradeCode = 11, 'DNW',
	    IndustryRiskGradeCode = - 1, 'NSI',
	    IndustryRiskGradeCode = - 2, 'Argent',
	    'N/A'
	) AS o_IndustryRiskGradeCode,
	AgencyCode,
	AbbreviatedName,
	PolicyIssueCode,
	UnderwritingRegionCodeDescription,
	StrategicBusinessGroupDescription,
	AcctOver150K,
	DWP,
	AuditID,
	CreateDate,
	ModifiedDate,
	RunDate,
	DirectLossIncurred,
	DirectLossIncurredRatio,
	DirectLossIncurred3Yrs,
	DirectLossIncurredRatio3Yrs,
	o_InsuranceReferenceLineOfBusinessDescription,
	StrategicProfitCenterDescription11,
	SalesTerritoryCodeDescription11,
	UnderWriterName11,
	UnderWriterManagerName11,
	UnderwriterManagerEmailAddress11,
	UnderwriterManagerCode11,
	UnderwriterEmailAddress11,
	UnderwriterCode11
	FROM JNR_New_DWP
),
Upd_Insert AS (
	SELECT
	CustomerNumber, 
	PolicyNumber, 
	PolicyKey, 
	PolicyOfferingDescription, 
	CustomerName, 
	PolicyEffectiveDate, 
	PrimaryBusinessClassCode, 
	PrimaryBusinessClassCodeDescription, 
	ProgramCode, 
	ProgramDescription, 
	o_IndustryRiskGradeCode AS IndustryRiskGradeCode, 
	AgencyCode, 
	AbbreviatedName, 
	PolicyIssueCode, 
	UnderwritingRegionCodeDescription, 
	StrategicBusinessGroupDescription, 
	AcctOver150K, 
	DWP, 
	AuditID, 
	CreateDate, 
	ModifiedDate, 
	RunDate, 
	o_InsuranceReferenceLineOfBusinessDescription AS LineofBusinessDescription, 
	DirectLossIncurred, 
	DirectLossIncurredRatio, 
	DirectLossIncurred3Yrs, 
	DirectLossIncurredRatio3Yrs, 
	StrategicProfitCenterDescription11, 
	SalesTerritoryCodeDescription11, 
	UnderWriterName11, 
	UnderWriterManagerName11, 
	UnderwriterManagerEmailAddress11, 
	UnderwriterManagerCode11, 
	UnderwriterEmailAddress11, 
	UnderwriterCode11
	FROM EXP_Decoding_Industry_riskgrade_New
),
CommercialProductManagementExtract_New_Insert AS (
	INSERT INTO CommercialProductManagementExtract
	(AuditId, ModifiedDate, CustomerNumber, PolicyNumber, PolicyOfferingDescription, FirstNamedInsured, PolicyEffectiveDate, DirectWrittenPremium, DirectLossIncurred, DirectLossIncurredRatio, DirectLossIncurred3Yrs, DirectLossIncurredRatio3Yrs, PrimaryBusinessClassCode, PrimaryBusinessClassDescription, StrategicProfitCenterDescription, ProgramCode, ProgramDescription, IndustryRiskGradeCode, AgencyCode, AbbreviatedAgencyName, UnderwritingRegionCodeDescription, PolicyIssueCode, RunDate, AccountOver150KFlag, InsuranceReferenceLineOfBusinessDescription, UnderWriterName, UnderWriterManagerName, StrategicBusinessGroupDescription, SalesTerritoryDescription, UnderwriterManagerEmailAddress, UnderwriterManagerCode, UnderwriterEmailAddress, UnderwriterCode)
	SELECT 
	AuditID AS AUDITID, 
	CreateDate AS MODIFIEDDATE, 
	CUSTOMERNUMBER, 
	POLICYNUMBER, 
	POLICYOFFERINGDESCRIPTION, 
	CustomerName AS FIRSTNAMEDINSURED, 
	POLICYEFFECTIVEDATE, 
	DWP AS DIRECTWRITTENPREMIUM, 
	DIRECTLOSSINCURRED, 
	DIRECTLOSSINCURREDRATIO, 
	DIRECTLOSSINCURRED3YRS, 
	DIRECTLOSSINCURREDRATIO3YRS, 
	PRIMARYBUSINESSCLASSCODE, 
	PrimaryBusinessClassCodeDescription AS PRIMARYBUSINESSCLASSDESCRIPTION, 
	StrategicProfitCenterDescription11 AS STRATEGICPROFITCENTERDESCRIPTION, 
	PROGRAMCODE, 
	PROGRAMDESCRIPTION, 
	INDUSTRYRISKGRADECODE, 
	AGENCYCODE, 
	AbbreviatedName AS ABBREVIATEDAGENCYNAME, 
	UNDERWRITINGREGIONCODEDESCRIPTION, 
	POLICYISSUECODE, 
	RUNDATE, 
	AcctOver150K AS ACCOUNTOVER150KFLAG, 
	LineofBusinessDescription AS INSURANCEREFERENCELINEOFBUSINESSDESCRIPTION, 
	UnderWriterName11 AS UNDERWRITERNAME, 
	UnderWriterManagerName11 AS UNDERWRITERMANAGERNAME, 
	STRATEGICBUSINESSGROUPDESCRIPTION, 
	SalesTerritoryCodeDescription11 AS SALESTERRITORYDESCRIPTION, 
	UnderwriterManagerEmailAddress11 AS UNDERWRITERMANAGEREMAILADDRESS, 
	UnderwriterManagerCode11 AS UNDERWRITERMANAGERCODE, 
	UnderwriterEmailAddress11 AS UNDERWRITEREMAILADDRESS, 
	UnderwriterCode11 AS UNDERWRITERCODE
	FROM Upd_Insert
),
Agg_DWP AS (
	SELECT
	CustomerNumber,
	PolicyNumber,
	PolicyOfferingDescription,
	CustomerName,
	PolicyEffectiveDate,
	PremiumMasterDirectWrittenPremium,
	PrimaryBusinessClassCode,
	PrimaryBusinessClassCodeDescription,
	ProgramCode,
	ProgramDescription,
	IndustryRiskGradeCode,
	-- *INF*: max(IndustryRiskGradeCode)
	max(IndustryRiskGradeCode) AS o_IndustryRiskGradeCode,
	AgencyCode,
	AbbreviatedName,
	PolicyIssueCode,
	UWRegion AS UnderwritingRegionCodeDescription,
	StrategicBusinessGroupDescription,
	-- *INF*: sum(PremiumMasterDirectWrittenPremium)
	sum(PremiumMasterDirectWrittenPremium) AS o_DWP,
	-- *INF*: IIF(sum(PremiumMasterDirectWrittenPremium) >150000,1,0)
	IFF(sum(PremiumMasterDirectWrittenPremium) > 150000, 1, 0) AS AcctOver150K,
	0 AS DirectLossIncurred,
	0 AS DirectLossIncurredRatio,
	0 AS DirectLossIncurred3Yrs,
	0 AS DirectLossIncurredRatio3Yrs,
	StrategicProfitCenterDescription,
	SalesTerritoryCodeDescription,
	UnderWriterName,
	UnderWriterManagerName,
	UnderwriterManagerEmailAddress AS UnderwriterManagerEmailAddress3,
	UnderwriterManagerCode AS UnderwriterManagerCode3,
	UnderwriterEmailAddress AS UnderwriterEmailAddress3,
	UnderwriterCode AS UnderwriterCode3
	FROM RTR_New_Renewal_Renewal
	GROUP BY CustomerNumber, PolicyNumber
),
JNR_Renewal_Insurance AS (SELECT
	Agg_DWP.CustomerNumber, 
	Agg_DWP.PolicyNumber, 
	Agg_DWP.PolicyKey, 
	Agg_DWP.PolicyOfferingDescription, 
	Agg_DWP.CustomerName, 
	Agg_DWP.PolicyEffectiveDate, 
	Agg_DWP.PremiumMasterDirectWrittenPremium, 
	Agg_DWP.PrimaryBusinessClassCode, 
	Agg_DWP.PrimaryBusinessClassCodeDescription, 
	Agg_DWP.ProgramCode, 
	Agg_DWP.ProgramDescription, 
	Agg_DWP.o_IndustryRiskGradeCode AS IndustryRiskGradeCode, 
	Agg_DWP.AgencyCode, 
	Agg_DWP.AbbreviatedName, 
	Agg_DWP.PolicyIssueCode, 
	Agg_DWP.UnderwritingRegionCodeDescription, 
	Agg_DWP.StrategicBusinessGroupDescription, 
	Agg_DWP.AcctOver150K, 
	Agg_DWP.o_DWP, 
	Agg_DWP.DirectLossIncurred, 
	Agg_DWP.DirectLossIncurredRatio, 
	Agg_DWP.DirectLossIncurred3Yrs, 
	Agg_DWP.DirectLossIncurredRatio3Yrs, 
	Agg_DWP.StrategicProfitCenterDescription, 
	Agg_DWP.SalesTerritoryCodeDescription, 
	Agg_DWP.UnderWriterName, 
	Agg_DWP.UnderWriterManagerName, 
	Agg_DWP.UnderwriterManagerEmailAddress3, 
	Agg_DWP.UnderwriterManagerCode3, 
	Agg_DWP.UnderwriterEmailAddress3, 
	Agg_DWP.UnderwriterCode3, 
	mplt_InsuranceLineofBusiness.CustomerNumber1 AS InsuranceLOB_CustomerNumber, 
	mplt_InsuranceLineofBusiness.PolicyNumber1 AS InsuranceLOB_PolicyNumber, 
	mplt_InsuranceLineofBusiness.o_InsuranceReferenceLineOfBusinessDescription AS InsuranceLOB_InsuranceReferenceLineOfBusinessDescription
	FROM Agg_DWP
	LEFT OUTER JOIN mplt_InsuranceLineofBusiness
	ON mplt_InsuranceLineofBusiness.CustomerNumber1 = Agg_DWP.CustomerNumber AND mplt_InsuranceLineofBusiness.PolicyNumber1 = Agg_DWP.PolicyNumber
),
EXP_Decoding_Industry_riskgrade AS (
	SELECT
	CustomerNumber,
	PolicyNumber,
	PolicyOfferingDescription,
	CustomerName,
	PolicyEffectiveDate,
	PrimaryBusinessClassCode,
	PrimaryBusinessClassCodeDescription,
	ProgramCode,
	ProgramDescription,
	IndustryRiskGradeCode,
	-- *INF*: DECODE (TRUE,IndustryRiskGradeCode=1 , '1' ,
	-- IndustryRiskGradeCode=2 , '2' ,
	-- IndustryRiskGradeCode=3 , '3' ,
	-- IndustryRiskGradeCode=4 , '4' ,
	-- IndustryRiskGradeCode=5 , '5' ,
	-- IndustryRiskGradeCode=6 , '6' ,
	-- IndustryRiskGradeCode=7 , '7' ,
	-- IndustryRiskGradeCode=8 , '8' ,
	-- IndustryRiskGradeCode=9 , '9' ,
	-- IndustryRiskGradeCode=0 , '0' ,
	-- IndustryRiskGradeCode=10 , 'D' ,
	-- IndustryRiskGradeCode=-3 , 'N/A' ,
	-- IndustryRiskGradeCode=11 , 'DNW' ,
	-- IndustryRiskGradeCode=-1 , 'NSI' ,
	-- IndustryRiskGradeCode=-2 , 'Argent' ,'N/A')
	DECODE(
	    TRUE,
	    IndustryRiskGradeCode = 1, '1',
	    IndustryRiskGradeCode = 2, '2',
	    IndustryRiskGradeCode = 3, '3',
	    IndustryRiskGradeCode = 4, '4',
	    IndustryRiskGradeCode = 5, '5',
	    IndustryRiskGradeCode = 6, '6',
	    IndustryRiskGradeCode = 7, '7',
	    IndustryRiskGradeCode = 8, '8',
	    IndustryRiskGradeCode = 9, '9',
	    IndustryRiskGradeCode = 0, '0',
	    IndustryRiskGradeCode = 10, 'D',
	    IndustryRiskGradeCode = - 3, 'N/A',
	    IndustryRiskGradeCode = 11, 'DNW',
	    IndustryRiskGradeCode = - 1, 'NSI',
	    IndustryRiskGradeCode = - 2, 'Argent',
	    'N/A'
	) AS o_IndustryRiskGradeCode,
	AgencyCode,
	AbbreviatedName,
	PolicyIssueCode,
	UnderwritingRegionCodeDescription,
	StrategicBusinessGroupDescription,
	AcctOver150K,
	o_DWP,
	InsuranceLOB_InsuranceReferenceLineOfBusinessDescription,
	StrategicProfitCenterDescription,
	SalesTerritoryCodeDescription,
	UnderWriterName,
	UnderWriterManagerName,
	UnderwriterManagerEmailAddress3,
	UnderwriterManagerCode3,
	UnderwriterEmailAddress3,
	UnderwriterCode3
	FROM JNR_Renewal_Insurance
),
Upd_Update_Renewal AS (
	SELECT
	CustomerNumber, 
	PolicyNumber, 
	PolicyOfferingDescription, 
	CustomerName, 
	PolicyEffectiveDate, 
	PrimaryBusinessClassCode, 
	PrimaryBusinessClassCodeDescription, 
	ProgramCode, 
	ProgramDescription, 
	o_IndustryRiskGradeCode AS IndustryRiskGradeCode, 
	AgencyCode, 
	AbbreviatedName, 
	PolicyIssueCode, 
	UnderwritingRegionCodeDescription, 
	StrategicProfitCenterDescription, 
	AcctOver150K, 
	o_DWP AS DWP, 
	InsuranceLOB_InsuranceReferenceLineOfBusinessDescription AS InsuranceReferenceLineOfBusinessDescription, 
	StrategicBusinessGroupDescription, 
	SalesTerritoryCodeDescription, 
	UnderWriterName, 
	UnderWriterManagerName, 
	UnderwriterManagerEmailAddress3, 
	UnderwriterManagerCode3, 
	UnderwriterEmailAddress3, 
	UnderwriterCode3
	FROM EXP_Decoding_Industry_riskgrade
),
CommercialProductManagementExtract_Renwal_Update AS (
	MERGE INTO CommercialProductManagementExtract AS T
	USING Upd_Update_Renewal AS S
	ON T.CustomerNumber = S.CustomerNumber
	AND T.PolicyNumber = S.PolicyNumber
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.PolicyOfferingDescription = S.PolicyOfferingDescription, T.FirstNamedInsured = S.CustomerName, T.PolicyEffectiveDate = S.PolicyEffectiveDate, T.DirectWrittenPremium = S.DWP, T.PrimaryBusinessClassCode = S.PrimaryBusinessClassCode, T.PrimaryBusinessClassDescription = S.PrimaryBusinessClassCodeDescription, T.StrategicProfitCenterDescription = S.StrategicProfitCenterDescription, T.ProgramCode = S.ProgramCode, T.ProgramDescription = S.ProgramDescription, T.IndustryRiskGradeCode = S.IndustryRiskGradeCode, T.AgencyCode = S.AgencyCode, T.AbbreviatedAgencyName = S.AbbreviatedName, T.UnderwritingRegionCodeDescription = S.UnderwritingRegionCodeDescription, T.PolicyIssueCode = S.PolicyIssueCode, T.AccountOver150KFlag = S.AcctOver150K, T.InsuranceReferenceLineOfBusinessDescription = S.InsuranceReferenceLineOfBusinessDescription, T.UnderWriterName = S.UnderWriterName, T.UnderWriterManagerName = S.UnderWriterManagerName, T.StrategicBusinessGroupDescription = S.StrategicBusinessGroupDescription, T.SalesTerritoryDescription = S.SalesTerritoryCodeDescription, T.UnderwriterManagerEmailAddress = S.UnderwriterManagerEmailAddress3, T.UnderwriterManagerCode = S.UnderwriterManagerCode3, T.UnderwriterEmailAddress = S.UnderwriterEmailAddress3, T.UnderwriterCode = S.UnderwriterCode3
),
Agg_DWP_Update AS (
	SELECT
	CustomerNumber,
	PolicyNumber,
	PolicyKey,
	PolicyOfferingDescription,
	CustomerName,
	PolicyEffectiveDate,
	PremiumMasterDirectWrittenPremium,
	PrimaryBusinessClassCode,
	PrimaryBusinessClassCodeDescription,
	ProgramCode,
	ProgramDescription,
	IndustryRiskGradeCode,
	-- *INF*: max(IndustryRiskGradeCode)
	max(IndustryRiskGradeCode) AS o_IndustryRiskGradeCode,
	AgencyCode,
	AbbreviatedName,
	PolicyIssueCode,
	UWRegion AS StateOfDomicileCodeDescription,
	StrategicBusinessGroupDescription,
	-- *INF*: sum(PremiumMasterDirectWrittenPremium)
	sum(PremiumMasterDirectWrittenPremium) AS o_DWP,
	-- *INF*: IIF(sum(PremiumMasterDirectWrittenPremium) >150000,1,0)
	IFF(sum(PremiumMasterDirectWrittenPremium) > 150000, 1, 0) AS AcctOver150K,
	StrategicProfitCenterDescription1 AS StrategicProfitCenterDescription,
	SalesTerritoryCodeDescription1 AS SalesTerritoryCodeDescription,
	UnderWriterName1 AS UnderWriterName,
	UnderWriterManagerName1 AS UnderWriterManagerName,
	UnderwriterManagerEmailAddress1 AS UnderwriterManagerEmailAddress13,
	UnderwriterManagerCode1 AS UnderwriterManagerCode13,
	UnderwriterEmailAddress1 AS UnderwriterEmailAddress13,
	UnderwriterCode1 AS UnderwriterCode13
	FROM RTR_Insert_Update_Update
	GROUP BY CustomerNumber, PolicyNumber
),
JNR_New_DWP_Update AS (SELECT
	Agg_DWP_Update.CustomerNumber, 
	Agg_DWP_Update.PolicyNumber, 
	Agg_DWP_Update.PolicyKey, 
	Agg_DWP_Update.PolicyOfferingDescription, 
	Agg_DWP_Update.CustomerName, 
	Agg_DWP_Update.PolicyEffectiveDate, 
	Agg_DWP_Update.PrimaryBusinessClassCode, 
	Agg_DWP_Update.PrimaryBusinessClassCodeDescription, 
	Agg_DWP_Update.ProgramCode, 
	Agg_DWP_Update.ProgramDescription, 
	Agg_DWP_Update.o_IndustryRiskGradeCode AS IndustryRiskGradeCode, 
	Agg_DWP_Update.AgencyCode, 
	Agg_DWP_Update.AbbreviatedName, 
	Agg_DWP_Update.PolicyIssueCode, 
	Agg_DWP_Update.StateOfDomicileCodeDescription, 
	Agg_DWP_Update.StrategicBusinessGroupDescription, 
	Agg_DWP_Update.AcctOver150K, 
	Agg_DWP_Update.o_DWP AS DWP, 
	mplt_InsuranceLineofBusiness.CustomerNumber1, 
	mplt_InsuranceLineofBusiness.PolicyNumber1, 
	mplt_InsuranceLineofBusiness.o_InsuranceReferenceLineOfBusinessDescription, 
	Agg_DWP_Update.StrategicProfitCenterDescription, 
	Agg_DWP_Update.SalesTerritoryCodeDescription, 
	Agg_DWP_Update.UnderWriterName, 
	Agg_DWP_Update.UnderWriterManagerName, 
	Agg_DWP_Update.UnderwriterManagerEmailAddress13, 
	Agg_DWP_Update.UnderwriterManagerCode13, 
	Agg_DWP_Update.UnderwriterEmailAddress13, 
	Agg_DWP_Update.UnderwriterCode13
	FROM Agg_DWP_Update
	LEFT OUTER JOIN mplt_InsuranceLineofBusiness
	ON mplt_InsuranceLineofBusiness.CustomerNumber1 = Agg_DWP_Update.CustomerNumber AND mplt_InsuranceLineofBusiness.PolicyNumber1 = Agg_DWP_Update.PolicyNumber
),
EXP_Decoding_Industry_riskgrade_update AS (
	SELECT
	CustomerNumber,
	PolicyNumber,
	PolicyKey,
	PolicyOfferingDescription,
	CustomerName,
	PolicyEffectiveDate,
	PrimaryBusinessClassCode,
	PrimaryBusinessClassCodeDescription,
	ProgramCode,
	ProgramDescription,
	IndustryRiskGradeCode,
	-- *INF*: DECODE (TRUE,IndustryRiskGradeCode=1 , '1' ,
	-- IndustryRiskGradeCode=2 , '2' ,
	-- IndustryRiskGradeCode=3 , '3' ,
	-- IndustryRiskGradeCode=4 , '4' ,
	-- IndustryRiskGradeCode=5 , '5' ,
	-- IndustryRiskGradeCode=6 , '6' ,
	-- IndustryRiskGradeCode=7 , '7' ,
	-- IndustryRiskGradeCode=8 , '8' ,
	-- IndustryRiskGradeCode=9 , '9' ,
	-- IndustryRiskGradeCode=0 , '0' ,
	-- IndustryRiskGradeCode=10 , 'D' ,
	-- IndustryRiskGradeCode=-3 , 'N/A' ,
	-- IndustryRiskGradeCode=11 , 'DNW' ,
	-- IndustryRiskGradeCode=-1 , 'NSI' ,
	-- IndustryRiskGradeCode=-2 , 'Argent' ,'N/A')
	DECODE(
	    TRUE,
	    IndustryRiskGradeCode = 1, '1',
	    IndustryRiskGradeCode = 2, '2',
	    IndustryRiskGradeCode = 3, '3',
	    IndustryRiskGradeCode = 4, '4',
	    IndustryRiskGradeCode = 5, '5',
	    IndustryRiskGradeCode = 6, '6',
	    IndustryRiskGradeCode = 7, '7',
	    IndustryRiskGradeCode = 8, '8',
	    IndustryRiskGradeCode = 9, '9',
	    IndustryRiskGradeCode = 0, '0',
	    IndustryRiskGradeCode = 10, 'D',
	    IndustryRiskGradeCode = - 3, 'N/A',
	    IndustryRiskGradeCode = 11, 'DNW',
	    IndustryRiskGradeCode = - 1, 'NSI',
	    IndustryRiskGradeCode = - 2, 'Argent',
	    'N/A'
	) AS o_IndustryRiskGradeCode,
	AgencyCode,
	AbbreviatedName,
	PolicyIssueCode,
	StateOfDomicileCodeDescription,
	StrategicBusinessGroupDescription,
	AcctOver150K,
	DWP,
	o_InsuranceReferenceLineOfBusinessDescription,
	StrategicProfitCenterDescription,
	SalesTerritoryCodeDescription,
	UnderWriterName,
	UnderWriterManagerName,
	UnderwriterManagerEmailAddress13,
	UnderwriterManagerCode13,
	UnderwriterEmailAddress13,
	UnderwriterCode13
	FROM JNR_New_DWP_Update
),
Upd_Update AS (
	SELECT
	CustomerNumber, 
	PolicyNumber, 
	PolicyKey, 
	PolicyOfferingDescription, 
	CustomerName, 
	PolicyEffectiveDate, 
	PrimaryBusinessClassCode, 
	PrimaryBusinessClassCodeDescription, 
	ProgramCode, 
	ProgramDescription, 
	o_IndustryRiskGradeCode AS IndustryRiskGradeCode, 
	AgencyCode, 
	AbbreviatedName, 
	PolicyIssueCode, 
	StateOfDomicileCodeDescription, 
	StrategicBusinessGroupDescription, 
	AcctOver150K, 
	DWP, 
	o_InsuranceReferenceLineOfBusinessDescription AS InsuranceReferenceLineOfBusinessDescription, 
	StrategicProfitCenterDescription, 
	SalesTerritoryCodeDescription, 
	UnderWriterName, 
	UnderWriterManagerName, 
	UnderwriterManagerEmailAddress13, 
	UnderwriterManagerCode13, 
	UnderwriterEmailAddress13, 
	UnderwriterCode13
	FROM EXP_Decoding_Industry_riskgrade_update
),
CommercialProductManagementExtract_New_Update AS (
	MERGE INTO CommercialProductManagementExtract AS T
	USING Upd_Update AS S
	ON T.CustomerNumber = S.CustomerNumber
	AND T.PolicyNumber = S.PolicyNumber
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.PolicyOfferingDescription = S.PolicyOfferingDescription, T.FirstNamedInsured = S.CustomerName, T.PolicyEffectiveDate = S.PolicyEffectiveDate, T.PrimaryBusinessClassCode = S.PrimaryBusinessClassCode, T.PrimaryBusinessClassDescription = S.PrimaryBusinessClassCodeDescription, T.StrategicProfitCenterDescription = S.StrategicProfitCenterDescription, T.ProgramCode = S.ProgramCode, T.ProgramDescription = S.ProgramDescription, T.IndustryRiskGradeCode = S.IndustryRiskGradeCode, T.AgencyCode = S.AgencyCode, T.AbbreviatedAgencyName = S.AbbreviatedName, T.UnderwritingRegionCodeDescription = S.StateOfDomicileCodeDescription, T.PolicyIssueCode = S.PolicyIssueCode, T.AccountOver150KFlag = S.AcctOver150K, T.InsuranceReferenceLineOfBusinessDescription = S.InsuranceReferenceLineOfBusinessDescription, T.UnderWriterName = S.UnderWriterName, T.UnderWriterManagerName = S.UnderWriterManagerName, T.StrategicBusinessGroupDescription = S.StrategicBusinessGroupDescription, T.SalesTerritoryDescription = S.SalesTerritoryCodeDescription, T.UnderwriterManagerEmailAddress = S.UnderwriterManagerEmailAddress13, T.UnderwriterManagerCode = S.UnderwriterManagerCode13, T.UnderwriterEmailAddress = S.UnderwriterEmailAddress13, T.UnderwriterCode = S.UnderwriterCode13
),
SQ_CommercialProductManagementExtract AS (
	select CommercialProductManagementExtractId
	,AuditId
	,ModifiedDate
	,CustomerNumber
	,PolicyNumber
	,PolicyOfferingDescription
	,FirstNamedInsured
	,PolicyEffectiveDate
	,DirectWrittenPremium
	,DirectLossIncurred
	,DirectLossIncurredRatio
	,DirectLossIncurred3Yrs
	,DirectLossIncurredRatio3Yrs
	,PrimaryBusinessClassCode
	,PrimaryBusinessClassDescription
	,StrategicProfitCenterDescription
	,ProgramCode
	,ProgramDescription
	,IndustryRiskGradeCode
	,AgencyCode
	,AbbreviatedAgencyName
	,UnderwritingRegionCodeDescription
	,PolicyIssueCode
	,RunDate
	,AccountOver150KFlag
	,InsuranceReferenceLineOfBusinessDescription
	,UnderWriterName
	,UnderWriterManagerName
	,StrategicBusinessGroupDescription
	,SalesTerritoryDescription
	,UnderwriterManagerEmailAddress
	,UnderwriterManagerCode
	,UnderwriterEmailAddress
	,UnderwriterCode
	,case
		when SUM(DirectWrittenPremium) OVER (PARTITION BY CustomerNumber, StrategicProfitCenterDescription) between 0 and 4999.99
		then 'XS'
		when SUM(DirectWrittenPremium) OVER (PARTITION BY CustomerNumber, StrategicProfitCenterDescription) between 5000 and 24999.99
		then 'S'
		when SUM(DirectWrittenPremium) OVER (PARTITION BY CustomerNumber, StrategicProfitCenterDescription) between 25000 and 74999.99
		then 'M'
		when SUM(DirectWrittenPremium) OVER (PARTITION BY CustomerNumber, StrategicProfitCenterDescription) between 75000 and 249999.99
		then 'L'
		when SUM(DirectWrittenPremium) OVER (PARTITION BY CustomerNumber, StrategicProfitCenterDescription) >= 250000
		then 'H'
	end as AccountSize
	from dbo.CommercialProductManagementExtract with (nolock)
),
EXP_CommercialProductManagementExtract AS (
	SELECT
	CommercialProductManagementExtractId,
	AuditId,
	ModifiedDate,
	CustomerNumber,
	PolicyNumber,
	PolicyOfferingDescription,
	FirstNamedInsured,
	PolicyEffectiveDate,
	DirectWrittenPremium,
	DirectLossIncurred,
	DirectLossIncurredRatio,
	DirectLossIncurred3Yrs,
	DirectLossIncurredRatio3Yrs,
	PrimaryBusinessClassCode,
	PrimaryBusinessClassDescription,
	StrategicProfitCenterDescription,
	ProgramCode,
	ProgramDescription,
	IndustryRiskGradeCode,
	AgencyCode,
	AbbreviatedAgencyName,
	UnderwritingRegionCodeDescription,
	PolicyIssueCode,
	RunDate,
	AccountOver150KFlag,
	InsuranceReferenceLineOfBusinessDescription,
	UnderWriterName,
	UnderWriterManagerName,
	StrategicBusinessGroupDescription,
	SalesTerritoryDescription,
	UnderwriterManagerEmailAddress,
	UnderwriterManagerCode,
	UnderwriterEmailAddress,
	UnderwriterCode,
	AccountSize
	FROM SQ_CommercialProductManagementExtract
),
UPD_CommercialProductManagementExtract AS (
	SELECT
	CommercialProductManagementExtractId, 
	AccountSize
	FROM EXP_CommercialProductManagementExtract
),
CommercialProductManagementExtract AS (
	MERGE INTO CommercialProductManagementExtract AS T
	USING UPD_CommercialProductManagementExtract AS S
	ON T.CommercialProductManagementExtractId = S.CommercialProductManagementExtractId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AccountSize = S.AccountSize
),
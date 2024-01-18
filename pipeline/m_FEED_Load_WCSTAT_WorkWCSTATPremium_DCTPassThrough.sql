WITH
LKP_PremiumTransaction_pol_ak_id AS (
	SELECT
	pol_ak_id
	FROM (
		SELECT DISTINCT  P.pol_ak_id AS pol_ak_id
		   from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
		on RC.RatingCoverageAKID=PT.RatingCoverageAKId
		and RC.EffectiveDate=PT.EffectiveDate and PT.CurrentSnapshotFlag=1
		
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
		and PC.CurrentSnapshotFlag=1
		
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
		on PC.RiskLocationAKID=RL.RiskLocationAKID
		and RL.CurrentSnapshotFlag=1
		
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P
		on P.pol_ak_id=RL.PolicyAKID 
		and P.crrnt_snpsht_flag=1 
		
		where RC.SourceSystemID='DCT' and PC.TypeBureauCode in ('WC','WP','WorkersCompensation')  and PT.PremiumType='D'
		and PT.ReasonAmendedCode NOT IN ('CWO','Claw Back')
		and DATEDIFF(MM,P.pol_eff_date, DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}, 0) )  > 18
		and PT.PremiumTransactionBookedDate>=DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}, 0)
		and PT.PremiumTransactionBookedDate<DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}+1, 0)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id ORDER BY pol_ak_id) = 1
),
SQ_Premium_mnsc AS (
	DECLARE @startdate as datetime, 
	        @enddate as datetime
	
	SET @startdate = DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) +@{pipeline().parameters.NUM_OF_MONTHS}+1-18, 0) 
	SET @enddate =  DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}+1, 0)
	
	
	--DCT MN surcharge
	select 
	'DCT PT' SourceSystem
	,case when mnsc.PremiumTransactionId is null then -1 else mnsc.PremiumTransactionId end as PremiumTransactionId
	,case when mnsc.PassThroughChargeTransactionBookedDate is null then mnsc.pol_eff_date else mnsc.PassThroughChargeTransactionBookedDate end as PremiumTransactionBookedDate
	,mnsc.StateRatingEffectiveDate
	,mnsc.WCRetrospectiveRatingIndicator
	,mnsc.ExperienceModificationFactor
	,mnsc.ExperienceModificationEffectiveDate
	,case when mnsc.PassThroughChargeTransactionAmount is null then 0 else mnsc.PassThroughChargeTransactionAmount end as PremiumTransactionAmount
	,mnsc.BaseRate
	,mnsc.TypeBureauCode
	,mnsc.pol_sym
	,mnsc.pol_num
	,mnsc.pol_mod
	,mnsc.pol_key
	,mnsc.CoverageType
	,mnsc.PremiumTransactionCode
	,mnsc.CoverageGUID
	,mnsc.RatingCoverageAKID
	, mnsc.Period_start_date
	, mnsc.Period_end_date
	, mnsc.AnyARDIndicator
	 , mnsc.ExperienceRated
	 , mnsc.TermType
	,mnsc.StateProvinceCode
	,mnsc.pol_eff_date
	,mnsc.pol_exp_date
	,mnsc.pol_cancellation_date
	,mnsc.pol_cancellation_ind
	,mnsc.InterstateRiskId
	,mnsc.pol_id
	,mnsc.fed_tax_id
	,mnsc.pol_term
	,mnsc.pol_ak_id
	,mnsc.InsuranceSegmentAbbreviation
	,mnsc.cust_role
	,mnsc.name
	,mnsc.addr_line_1
	,mnsc.city_name
	,mnsc.state_prov_code
	,mnsc.zip_postal_code
	,mnsc.ClassCode
	,mnsc.WrittenExposure
	,mnsc.DeductibleAmount
	,mnsc.InsuranceLine
	--,SUM(mnsc.PassThroughChargeTransactionAmount) OVER (PARTITION BY mnsc.pol_key) as PolicyPremiumTotal
	,SUM(0)  OVER (PARTITION BY mnsc.pol_key,mnsc.StateProvinceCode) as ManualPremiumInd
	,SUM(0) OVER (PARTITION BY mnsc.pol_key,mnsc.StateProvinceCode) as PolicyStateManualPremiumTotal
	,mnsc.BalMinPremiumTotal
	,mnsc.ReasonAmendedCode
	,mnsc.StrategicProfitCenterAbbreviation
	,mnsc.pol_status_code
	--,mnsc.PremiumTransactionEnteredDate
	,case when PremiumTransactionEnteredDate is null then '' else PremiumTransactionEnteredDate end
	,mnsc.OffsetOnsetCode
	from  
	(select 
	distinct
	pt.PassThroughChargeTransactionID as PremiumTransactionId
	,pt.PassThroughChargeTransactionBookedDate
	,'01-01-1800 00:00:00' as StateRatingEffectiveDate
	,case when RPDT.RatingPlanCode='2' then '1' else '0' end as WCRetrospectiveRatingIndicator
	,0 as ExperienceModificationFactor
	,'12-31-2100 00:00:00' as ExperienceModificationEffectiveDate
	,pt.PassThroughChargeTransactionAmount
	,0 as BaseRate
	,PolicyCoverage.TypeBureauCode
	,'' as pol_sym
	,policy.pol_num
	,policy.pol_mod
	,'N/A' AS CoverageType
	,'N/A' AS PremiumTransactionCode
	,'N/A' AS CoverageGUID
	,  -1 AS  RatingCoverageAKID  
	  ,'1800-01-01 00:00:00.000' AS  Period_start_date
	,'1800-01-01 00:00:00.000' AS Period_end_date
	,'' AS   AnyARDIndicator
	, '' AS ExperienceRated
	,'N/A' AS TermType 
	,RiskLocation.StateProvinceCode
	,policy.pol_eff_date
	,policy.pol_exp_date
	,policy.pol_cancellation_date
	,policy.pol_cancellation_ind
	,PolicyCoverage.InterstateRiskId
	,policy.pol_id
	,contract_customer.fed_tax_id
	,policy.pol_term
	,policy.pol_ak_id
	,InsuranceSegment.InsuranceSegmentAbbreviation
	,contract_customer.cust_role
	,contract_customer.name
	,contract_customer_address.addr_line_1
	,contract_customer_address.city_name
	,contract_customer_address.state_prov_code
	,contract_customer_address.zip_postal_code
	,CASE 
	WHEN ISNUMERIC (isnull(RatingCoverage.ClassCode,'XXXX')) = 1
	THEN cast(RatingCoverage.ClassCode as varchar)
	WHEN RiskLocation.StateProvinceCode = '22'
	THEN '0174'
	ELSE '0000'
	END as ClassCode
	,0 as WrittenExposure
	,0 as DeductibleAmount
	,PolicyCoverage.InsuranceLine
	,99999999 as BalMinPremiumTotal
	,policy.pol_key
	,pt.ReasonAmendedCode
	,StrategicProfitCenter.StrategicProfitCenterAbbreviation
	,policy.pol_status_code
	,pt.PassThroughChargeTransactionEnteredDate as PremiumTransactionEnteredDate
	,pt.OffsetOnsetCode
	
	FROM       dbo.passthroughchargetransaction pt
	
	INNER JOIN dbo.policycoverage 
			ON         pt.policycoverageakid = policycoverage.policycoverageakid
			AND        pt.currentsnapshotflag = 1 
			AND        pt.sourcesystemid = 'DCT' 
			AND        policycoverage.typebureaucode IN ('WC','WP','WorkersCompensation') 
	
	INNER JOIN dbo.suppassthroughchargetype spc 
			ON         pt.suppassthroughchargetypeid = spc.suppassthroughchargetypeid 
			AND        spc.passthroughchargetype IN ('MN Second Injury Fund', 
													 'MN Special Compensation Fund Surcharge', 
													 'MT WC Regulatory Assessment Surcharge', 
													 'MT Subsequent Injury Fund', 
													 'MT Occupational Safety and Health Surcharge', 
													 'MT Stay At Work/Return To Work Surcharge', 
													 'PA Employer Assessment Surcharge', 
													 'NJ Second Injury Fund Surcharge', 
													 'NJ Uninsured Employers Fund', 
													 'NY State Assessment', 
													 'NY WC Security Fund Assessment')--,'IN Second Injury Fund') 
	
	INNER JOIN dbo.risklocation 
			ON         risklocation.risklocationakid=policycoverage.risklocationakid 
			AND        policycoverage.currentsnapshotflag = 1 
			AND        risklocation.currentsnapshotflag = 1 
	
	INNER JOIN v2.policy 
			ON         policy.pol_ak_id=risklocation.policyakid 
			AND        risklocation.currentsnapshotflag = 1 
			AND        policy.crrnt_snpsht_flag = 1 
	
	INNER JOIN dbo.strategicprofitcenter 
			ON         policy.strategicprofitcenterakid=strategicprofitcenter.strategicprofitcenterakid 
			AND        strategicprofitcenter.currentsnapshotflag=1 
	
	INNER JOIN dbo.contract_customer 
			ON         contract_customer.contract_cust_ak_id = policy.contract_cust_ak_id 
			AND        contract_customer.crrnt_snpsht_flag = 1 
	
	INNER JOIN dbo.contract_customer_address 
			ON         contract_customer_address.contract_cust_ak_id = contract_customer.contract_cust_ak_id
			AND        contract_customer_address.crrnt_snpsht_flag = 1 
	
	INNER JOIN dbo.insurancesegment -- DOES HAVING AN INNER JOIN CHANGE OUTPUT?
			ON         insurancesegment.insurancesegmentakid = policy.insurancesegmentakid 
			AND        insurancesegment.currentsnapshotflag = 1 
	
	LEFT JOIN  dbo.ratingplan RPDT 
			ON         policycoverage.ratingplanakid=RPDT.ratingplanakid 
			AND        RPDT.currentsnapshotflag=1 
	
	LEFT JOIN  dbo.ratingcoverage 
			ON         pt.ratingcoverageakid = ratingcoverage.ratingcoverageakid 
			AND        pt.passthroughchargetransactionentereddate = ratingcoverage.effectivedate 
	
	where  policy.source_sys_id='DCT' 
	and policy.crrnt_snpsht_flag = 1 
	and policy.crrnt_snpsht_flag=1 
	and policy.pol_eff_date < @startdate
	and pt.PassThroughChargeTransactionBookedDate< @enddate
	@{pipeline().parameters.PREMIUM_WHERE_CLAUSE_PTC}
	) mnsc
),
EXP_Premium_mnsc AS (
	SELECT
	SourceSystem,
	PremiumMasterCalculationID,
	PremiumMasterRunDate,
	StateRatingEffectiveDate,
	WCRetrospectiveRatingIndicator,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	PremiumMasterPremium,
	BaseRate,
	TypeBureauCode,
	pol_sym,
	pol_num,
	pol_mod,
	pol_key,
	CoverageType,
	PremiumTransactionCode,
	CoverageGUID,
	RatingCoverageAKID,
	Period_start_date,
	Period_end_date,
	AnyARDIndicator,
	ExperienceRated,
	TermType,
	StateProvinceCode,
	pol_eff_date,
	pol_exp_date,
	pol_cancellation_date,
	pol_cancellation_ind,
	InterstateRiskId,
	pol_id,
	fed_tax_id,
	pol_term,
	pol_ak_id,
	InsuranceSegmentAbbreviation,
	cust_role,
	name,
	addr_line_1,
	city_name,
	state_prov_code,
	zip_postal_code,
	ClassCode,
	WrittenExposure,
	DeductibleAmount,
	InsuranceLine,
	ManualPremiumInd,
	PolicyStateManualPremiumTotal,
	BalMinPremiumTotal,
	ReasonAmendedCode,
	StrategicProfitCenterAbbreviation,
	pol_status_code,
	PremiumTransactionEnteredDate,
	OffsetOnsetCode
	FROM SQ_Premium_mnsc
),
SQ_Premium_INSIF AS (
	DECLARE @startdate as datetime, 
	        @enddate as datetime
	
	SET @startdate = DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) +@{pipeline().parameters.NUM_OF_MONTHS}+1-18, 0) 
	SET @enddate =  DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}+1, 0)
	
	
	--DCT IN Second Injury Fund
	select 
	'DCT PT' SourceSystem
	,case when INSIF.PremiumTransactionId is null then -1 else INSIF.PremiumTransactionId end as PremiumTransactionId
	,case when INSIF.PassThroughChargeTransactionBookedDate is null then INSIF.pol_eff_date else INSIF.PassThroughChargeTransactionBookedDate end as PremiumTransactionBookedDate
	,INSIF.StateRatingEffectiveDate
	,INSIF.WCRetrospectiveRatingIndicator
	,INSIF.ExperienceModificationFactor
	,INSIF.ExperienceModificationEffectiveDate
	,case when INSIF.PassThroughChargeTransactionAmount is null then 0 else INSIF.PassThroughChargeTransactionAmount end as PremiumTransactionAmount
	,INSIF.BaseRate
	,INSIF.TypeBureauCode
	,INSIF.pol_sym
	,INSIF.pol_num
	,INSIF.pol_mod
	,INSIF.pol_key
	,INSIF.CoverageType
	,INSIF.PremiumTransactionCode
	,INSIF.CoverageGUID
	,INSIF.RatingCoverageAKID
	, INSIF.Period_start_date
	, INSIF.Period_end_date
	, INSIF.AnyARDIndicator
	 , INSIF.ExperienceRated
	 , INSIF.TermType
	,INSIF.StateProvinceCode
	,INSIF.pol_eff_date
	,INSIF.pol_exp_date
	,INSIF.pol_cancellation_date
	,INSIF.pol_cancellation_ind
	,INSIF.InterstateRiskId
	,INSIF.pol_id
	,INSIF.fed_tax_id
	,INSIF.pol_term
	,INSIF.pol_ak_id
	,INSIF.InsuranceSegmentAbbreviation
	,INSIF.cust_role
	,INSIF.name
	,INSIF.addr_line_1
	,INSIF.city_name
	,INSIF.state_prov_code
	,INSIF.zip_postal_code
	,INSIF.ClassCode
	,INSIF.WrittenExposure
	,INSIF.DeductibleAmount
	,INSIF.InsuranceLine
	--,SUM(INSIF.PassThroughChargeTransactionAmount) OVER (PARTITION BY INSIF.pol_key) as PolicyPremiumTotal
	,SUM(0)  OVER (PARTITION BY INSIF.pol_key,INSIF.StateProvinceCode) as ManualPremiumInd
	,SUM(0) OVER (PARTITION BY INSIF.pol_key,INSIF.StateProvinceCode) as PolicyStateManualPremiumTotal
	,INSIF.BalMinPremiumTotal
	,INSIF.ReasonAmendedCode
	,INSIF.StrategicProfitCenterAbbreviation
	,INSIF.pol_status_code
	--,INSIF.PremiumTransactionEnteredDate
	,case when PremiumTransactionEnteredDate is null then '' else PremiumTransactionEnteredDate end
	,INSIF.OffsetOnsetCode
	from  
	(
	select 
	distinct
	pt.PassThroughChargeTransactionID as PremiumTransactionId
	,pt.PassThroughChargeTransactionBookedDate
	,'01-01-1800 00:00:00' as StateRatingEffectiveDate
	,case when RPDT.RatingPlanCode='2' then '1' else '0' end as WCRetrospectiveRatingIndicator
	,0 as ExperienceModificationFactor
	,'12-31-2100 00:00:00' as ExperienceModificationEffectiveDate
	,pt.PassThroughChargeTransactionAmount
	,0 as BaseRate
	,PolicyCoverage.TypeBureauCode
	,'' as pol_sym
	,policy.pol_num
	,policy.pol_mod
	,'N/A' AS CoverageType
	,'N/A' AS PremiumTransactionCode
	,'N/A' AS CoverageGUID
	,  -1 AS  RatingCoverageAKID  
	  ,'1800-01-01 00:00:00.000' AS  Period_start_date
	,'1800-01-01 00:00:00.000' AS Period_end_date
	,'' AS   AnyARDIndicator
	, '' AS ExperienceRated
	,'N/A' AS TermType 
	,RiskLocation.StateProvinceCode
	,policy.pol_eff_date
	,policy.pol_exp_date
	,policy.pol_cancellation_date
	,policy.pol_cancellation_ind
	,PolicyCoverage.InterstateRiskId
	,policy.pol_id
	,contract_customer.fed_tax_id
	,policy.pol_term
	,policy.pol_ak_id
	,InsuranceSegment.InsuranceSegmentAbbreviation
	,contract_customer.cust_role
	,contract_customer.name
	,contract_customer_address.addr_line_1
	,contract_customer_address.city_name
	,contract_customer_address.state_prov_code
	,contract_customer_address.zip_postal_code
	,CASE 
	WHEN ISNUMERIC (isnull(RatingCoverage.ClassCode,'XXXX')) = 1
	THEN cast(RatingCoverage.ClassCode as varchar)
	ELSE '0935'
	END as ClassCode
	,0 as WrittenExposure
	,0 as DeductibleAmount
	,PolicyCoverage.InsuranceLine
	,99999999 as BalMinPremiumTotal
	,policy.pol_key
	,pt.ReasonAmendedCode
	,StrategicProfitCenter.StrategicProfitCenterAbbreviation
	,policy.pol_status_code
	,pt.PassThroughChargeTransactionEnteredDate as PremiumTransactionEnteredDate
	,pt.OffsetOnsetCode
	
	FROM       dbo.passthroughchargetransaction pt
	
	INNER JOIN dbo.policycoverage 
			ON         pt.policycoverageakid = policycoverage.policycoverageakid
			AND        pt.currentsnapshotflag = 1 
			AND        pt.sourcesystemid = 'DCT' 
			AND        policycoverage.typebureaucode IN ('WC','WP','WorkersCompensation') 
	
	INNER JOIN dbo.suppassthroughchargetype spc 
			ON         pt.suppassthroughchargetypeid = spc.suppassthroughchargetypeid 
			AND        spc.passthroughchargetype IN ('IN Second Injury Fund')  
	
	INNER JOIN dbo.risklocation 
			ON         risklocation.risklocationakid=policycoverage.risklocationakid 
			AND        policycoverage.currentsnapshotflag = 1 
			AND        risklocation.currentsnapshotflag = 1 
	
	INNER JOIN v2.policy 
			ON         policy.pol_ak_id=risklocation.policyakid 
			AND        risklocation.currentsnapshotflag = 1 
			AND        policy.crrnt_snpsht_flag = 1 
	
	INNER JOIN dbo.strategicprofitcenter 
			ON         policy.strategicprofitcenterakid=strategicprofitcenter.strategicprofitcenterakid 
			AND        strategicprofitcenter.currentsnapshotflag=1 
	
	INNER JOIN dbo.contract_customer 
			ON         contract_customer.contract_cust_ak_id = policy.contract_cust_ak_id 
			AND        contract_customer.crrnt_snpsht_flag = 1 
	
	INNER JOIN dbo.contract_customer_address 
			ON         contract_customer_address.contract_cust_ak_id = contract_customer.contract_cust_ak_id
			AND        contract_customer_address.crrnt_snpsht_flag = 1 
	
	INNER JOIN dbo.insurancesegment -- DOES HAVING AN INNER JOIN CHANGE OUTPUT?
			ON         insurancesegment.insurancesegmentakid = policy.insurancesegmentakid 
			AND        insurancesegment.currentsnapshotflag = 1 
	
	LEFT JOIN  dbo.ratingplan RPDT 
			ON         policycoverage.ratingplanakid=RPDT.ratingplanakid 
			AND        RPDT.currentsnapshotflag=1 
	
	LEFT JOIN  dbo.ratingcoverage 
			ON         pt.ratingcoverageakid = ratingcoverage.ratingcoverageakid 
			AND        pt.passthroughchargetransactionentereddate = ratingcoverage.effectivedate 
	
	where  policy.source_sys_id='DCT'
	and policy.crrnt_snpsht_flag = 1 and RiskLocation.StateProvinceCode = '13'
	and policy.crrnt_snpsht_flag=1 
	and policy.pol_eff_date < @startdate
	and pt.PassThroughChargeTransactionBookedDate< @enddate
	
	) INSIF
),
EXP_Premium_mnsc1 AS (
	SELECT
	SourceSystem,
	PremiumMasterCalculationID,
	PremiumMasterRunDate,
	StateRatingEffectiveDate,
	WCRetrospectiveRatingIndicator,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	PremiumMasterPremium,
	BaseRate,
	TypeBureauCode,
	pol_sym,
	pol_num,
	pol_mod,
	pol_key,
	CoverageType,
	PremiumTransactionCode,
	CoverageGUID,
	RatingCoverageAKID,
	Period_start_date,
	Period_end_date,
	AnyARDIndicator,
	ExperienceRated,
	TermType,
	StateProvinceCode,
	pol_eff_date,
	pol_exp_date,
	pol_cancellation_date,
	pol_cancellation_ind,
	InterstateRiskId,
	pol_id,
	fed_tax_id,
	pol_term,
	pol_ak_id,
	InsuranceSegmentAbbreviation,
	cust_role,
	name,
	addr_line_1,
	city_name,
	state_prov_code,
	zip_postal_code,
	ClassCode,
	WrittenExposure,
	DeductibleAmount,
	InsuranceLine,
	ManualPremiumInd,
	PolicyStateManualPremiumTotal,
	BalMinPremiumTotal,
	ReasonAmendedCode,
	StrategicProfitCenterAbbreviation,
	pol_status_code,
	PremiumTransactionEnteredDate,
	OffsetOnsetCode
	FROM SQ_Premium_INSIF
),
Union_all AS (
	SELECT SourceSystem, PremiumMasterCalculationID, PremiumMasterRunDate, StateRatingEffectiveDate, WCRetrospectiveRatingIndicator, ExperienceModificationFactor, ExperienceModificationEffectiveDate, PremiumMasterPremium, BaseRate, TypeBureauCode, pol_sym, pol_num, pol_mod, pol_key, CoverageType, PremiumTransactionCode, CoverageGUID, RatingCoverageAKID, Period_start_date, Period_end_date, AnyARDIndicator, ExperienceRated, TermType, StateProvinceCode, pol_eff_date, pol_exp_date, pol_cancellation_date, pol_cancellation_ind, InterstateRiskId, pol_id, fed_tax_id, pol_term, pol_ak_id, InsuranceSegmentAbbreviation, cust_role, name, addr_line_1, city_name, state_prov_code, zip_postal_code, ClassCode, WrittenExposure, DeductibleAmount, InsuranceLine, ManualPremiumInd, PolicyStateManualPremiumTotal, BalMinPremiumTotal, ReasonAmendedCode, StrategicProfitCenterAbbreviation, pol_status_code, PremiumTransactionEnteredDate, OffsetOnsetCode
	FROM EXP_Premium_mnsc
	UNION
	SELECT SourceSystem, PremiumMasterCalculationID, PremiumMasterRunDate, StateRatingEffectiveDate, WCRetrospectiveRatingIndicator, ExperienceModificationFactor, ExperienceModificationEffectiveDate, PremiumMasterPremium, BaseRate, TypeBureauCode, pol_sym, pol_num, pol_mod, pol_key, CoverageType, PremiumTransactionCode, CoverageGUID, RatingCoverageAKID, Period_start_date, Period_end_date, AnyARDIndicator, ExperienceRated, TermType, StateProvinceCode, pol_eff_date, pol_exp_date, pol_cancellation_date, pol_cancellation_ind, InterstateRiskId, pol_id, fed_tax_id, pol_term, pol_ak_id, InsuranceSegmentAbbreviation, cust_role, name, addr_line_1, city_name, state_prov_code, zip_postal_code, ClassCode, WrittenExposure, DeductibleAmount, InsuranceLine, ManualPremiumInd, PolicyStateManualPremiumTotal, BalMinPremiumTotal, ReasonAmendedCode, StrategicProfitCenterAbbreviation, pol_status_code, PremiumTransactionEnteredDate, OffsetOnsetCode
	FROM EXP_Premium_mnsc1
),
EXP_identify_records_Wc_stat AS (
	SELECT
	SourceSystem,
	PremiumMasterCalculationID,
	PremiumMasterRunDate,
	StateRatingEffectiveDate,
	WCRetrospectiveRatingIndicator,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	PremiumMasterPremium,
	BaseRate,
	TypeBureauCode,
	pol_sym,
	pol_num,
	pol_mod,
	StateProvinceCode,
	pol_eff_date,
	pol_exp_date,
	pol_cancellation_date,
	pol_cancellation_ind,
	InterstateRiskId,
	pol_id,
	fed_tax_id,
	pol_term,
	pol_ak_id,
	InsuranceSegmentAbbreviation,
	cust_role,
	name,
	addr_line_1,
	city_name,
	state_prov_code,
	zip_postal_code,
	ClassCode,
	WrittenExposure AS Exposure,
	DeductibleAmount,
	InsuranceLine,
	-- *INF*: :LKP.LKP_PREMIUMTRANSACTION_POL_AK_ID(pol_ak_id)
	LKP_PREMIUMTRANSACTION_POL_AK_ID_pol_ak_id.pol_ak_id AS lkp_pol_ak_id,
	-- *INF*: SET_DATE_PART(ADD_TO_DATE(TRUNC(SYSDATE),'MM',@{pipeline().parameters.NUM_OF_MONTHS}),'DD',1)
	DATEADD(DAY,1-DATE_PART(DAY,DATEADD(MONTH,@{pipeline().parameters.NUM_OF_MONTHS},TRUNC(CURRENT_TIMESTAMP))),DATEADD(MONTH,@{pipeline().parameters.NUM_OF_MONTHS},TRUNC(CURRENT_TIMESTAMP))) AS v_ProcessRunDate,
	-- *INF*: SET_DATE_PART(pol_eff_date,'DD',1)
	DATEADD(DAY,1-DATE_PART(DAY,pol_eff_date),pol_eff_date) AS v_BeginingOfMonthForPolicyEffectiveDate,
	-- *INF*: LAST_DAY(pol_eff_date)
	LAST_DAY(pol_eff_date) AS v_EndOfMonthForPolicyEffectiveDate,
	-- *INF*: DATE_DIFF(v_ProcessRunDate,v_BeginingOfMonthForPolicyEffectiveDate,'MM')
	DATEDIFF(MONTH,v_ProcessRunDate,v_BeginingOfMonthForPolicyEffectiveDate) AS v_AgeOfPolicy,
	-- *INF*: IIF(v_AgeOfPolicy =18 OR NOT ISNULL(lkp_pol_ak_id),'VALID','INVALID')
	-- 
	-- --IIF(v_AgeOfPolicy =18 OR (v_AgeOfPolicy > 18 and PremiumMasterRunDate>= v_ProcessRunDate),'VALID','INVALID')
	IFF(v_AgeOfPolicy = 18 OR lkp_pol_ak_id IS NOT NULL, 'VALID', 'INVALID') AS o_ValidRecordForWCSTat,
	-- *INF*: IIF(DATE_DIFF(v_ProcessRunDate,PremiumMasterRunDate,'MM')=0,1,0)
	IFF(DATEDIFF(MONTH,v_ProcessRunDate,PremiumMasterRunDate) = 0, 1, 0) AS o_RunMonthAuditTransFlag,
	v_AgeOfPolicy AS o_AgeOfPolicy,
	ManualPremiumInd,
	PolicyStateManualPremiumTotal,
	BalMinPremiumTotal,
	ReasonAmendedCode,
	StrategicProfitCenterAbbreviation,
	pol_status_code,
	PremiumTransactionEnteredDate,
	OffsetOnsetCode,
	pol_key,
	CoverageType,
	PremiumTransactionCode,
	CoverageGUID,
	RatingCoverageAKID,
	Period_start_date,
	Period_end_date,
	AnyARDIndicator,
	ExperienceRated,
	TermType,
	-- *INF*: IIF(CoverageType='ManualPremium',Period_start_date,pol_eff_date)
	-- 
	-- --DECODE(True,NOT ISNULL(Period_start_date), Period_start_date, pol_eff_date)
	IFF(CoverageType = 'ManualPremium', Period_start_date, pol_eff_date) AS RateEffectiveDate
	FROM Union_all
	LEFT JOIN LKP_PREMIUMTRANSACTION_POL_AK_ID LKP_PREMIUMTRANSACTION_POL_AK_ID_pol_ak_id
	ON LKP_PREMIUMTRANSACTION_POL_AK_ID_pol_ak_id.pol_ak_id = pol_ak_id

),
FIL_FIlterrecordsthatarenotvalid AS (
	SELECT
	SourceSystem, 
	PremiumMasterCalculationID, 
	PremiumMasterRunDate, 
	StateRatingEffectiveDate, 
	WCRetrospectiveRatingIndicator, 
	ExperienceModificationFactor, 
	ExperienceModificationEffectiveDate, 
	PremiumMasterPremium, 
	BaseRate, 
	TypeBureauCode, 
	pol_sym, 
	pol_num, 
	pol_mod, 
	StateProvinceCode, 
	pol_eff_date, 
	pol_exp_date, 
	pol_cancellation_date, 
	pol_cancellation_ind, 
	InterstateRiskId, 
	pol_id, 
	fed_tax_id, 
	pol_term, 
	pol_ak_id, 
	InsuranceSegmentAbbreviation, 
	cust_role, 
	name, 
	addr_line_1, 
	city_name, 
	state_prov_code, 
	zip_postal_code, 
	ClassCode, 
	Exposure, 
	DeductibleAmount, 
	InsuranceLine, 
	o_ValidRecordForWCSTat, 
	o_RunMonthAuditTransFlag, 
	o_AgeOfPolicy, 
	ManualPremiumInd, 
	PolicyStateManualPremiumTotal, 
	BalMinPremiumTotal, 
	RateEffectiveDate, 
	ReasonAmendedCode, 
	StrategicProfitCenterAbbreviation, 
	pol_status_code, 
	PremiumTransactionEnteredDate, 
	OffsetOnsetCode, 
	pol_key, 
	CoverageType, 
	PremiumTransactionCode, 
	CoverageGUID, 
	RatingCoverageAKID, 
	Period_start_date, 
	Period_end_date, 
	AnyARDIndicator, 
	ExperienceRated, 
	TermType
	FROM EXP_identify_records_Wc_stat
	WHERE IIF(o_ValidRecordForWCSTat='VALID',TRUE,FALSE)
),
EXP_Target_Load AS (
	SELECT
	PremiumMasterCalculationID,
	PremiumMasterRunDate,
	StateRatingEffectiveDate,
	WCRetrospectiveRatingIndicator,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	PremiumMasterPremium,
	BaseRate,
	TypeBureauCode,
	pol_sym,
	pol_num,
	pol_mod,
	StateProvinceCode,
	pol_eff_date,
	pol_exp_date,
	pol_cancellation_date,
	pol_cancellation_ind,
	InterstateRiskId,
	pol_id,
	fed_tax_id,
	pol_term,
	pol_ak_id,
	InsuranceSegmentAbbreviation,
	cust_role,
	name,
	addr_line_1,
	city_name,
	state_prov_code,
	zip_postal_code,
	ClassCode,
	Exposure,
	DeductibleAmount,
	InsuranceLine,
	o_ValidRecordForWCSTat,
	o_RunMonthAuditTransFlag,
	o_AgeOfPolicy,
	ManualPremiumInd,
	PolicyStateManualPremiumTotal,
	BalMinPremiumTotal,
	RateEffectiveDate,
	SourceSystem,
	ReasonAmendedCode,
	StrategicProfitCenterAbbreviation,
	pol_status_code,
	PremiumTransactionEnteredDate,
	OffsetOnsetCode,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	pol_key,
	CoverageType,
	PremiumTransactionCode,
	CoverageGUID,
	RatingCoverageAKID,
	Period_start_date,
	Period_end_date,
	AnyARDIndicator,
	ExperienceRated,
	TermType,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	0 AS o_PolicyPremiumTotal,
	-- *INF*: TO_DATE('1800-01-01', 'YYYY-MM-DD')
	TO_TIMESTAMP('1800-01-01', 'YYYY-MM-DD') AS PremiumTransactionEffectiveDate,
	'00' AS LossesSubjectToDeductibleCode,
	'00' AS BasisOfDeductibleCalculationCode
	FROM FIL_FIlterrecordsthatarenotvalid
),
WorkWCSTATPremium AS (

	------------ PRE SQL ----------
	@{pipeline().parameters.DELETE_PRESQL}
	-------------------------------


	INSERT INTO WorkWCSTATPremium
	(AuditId, CreatedDate, ModifiedDate, SourceSystemId, PremiumMasterCalculationID, PremiumMasterRunDate, StateRatingEffectiveDate, WCRetrospectiveRatingIndicator, ExperienceModificationFactor, ExperienceModificationEffectiveDate, PremiumMasterPremium, BaseRate, TypeBureauCode, PolicySymbol, PolicyNumber, PolicyModulus, StateProvinceCode, PolicyEffectiveDate, PolicyExpiryDate, PolicyCancellationDate, PolicyCancellationIndicator, InterstateRiskId, FederalTaxId, PolicyTerm, PolicyAKId, InsuranceSegmentAbbreviation, CustomerRole, Name, AddressLine1, CityName, StateProvCodeContractCustomerAddress, ZipPostalCode, ClassCode, Exposure, DeductibleAmount, InsuranceLine, BalanceMinimumPremiumTotal, RateEffectiveDate, ReasonAmendedCode, StrategicProfitCenterAbbreviation, PolicyStatusCode, PremiumTransactionEnteredDate, OffsetOnsetCode, PolicyKey, CoverageType, PremiumTransactionCode, CoverageGUID, RatingCoverageAKID, RunMonthAuditTransactionFlag, AgeOfPolicy, TermType, PeriodStartDate, PeriodEndDate, AnyARDIndicator, ExperienceRated, PremiumTransactionEffectiveDate, LossesSubjectToDeductibleCode, BasisOfDeductibleCalculationCode)
	SELECT 
	AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	SourceSystem AS SOURCESYSTEMID, 
	PREMIUMMASTERCALCULATIONID, 
	PREMIUMMASTERRUNDATE, 
	STATERATINGEFFECTIVEDATE, 
	WCRETROSPECTIVERATINGINDICATOR, 
	EXPERIENCEMODIFICATIONFACTOR, 
	EXPERIENCEMODIFICATIONEFFECTIVEDATE, 
	PREMIUMMASTERPREMIUM, 
	BASERATE, 
	TYPEBUREAUCODE, 
	pol_sym AS POLICYSYMBOL, 
	pol_num AS POLICYNUMBER, 
	pol_mod AS POLICYMODULUS, 
	STATEPROVINCECODE, 
	pol_eff_date AS POLICYEFFECTIVEDATE, 
	pol_exp_date AS POLICYEXPIRYDATE, 
	pol_cancellation_date AS POLICYCANCELLATIONDATE, 
	pol_cancellation_ind AS POLICYCANCELLATIONINDICATOR, 
	INTERSTATERISKID, 
	fed_tax_id AS FEDERALTAXID, 
	pol_term AS POLICYTERM, 
	pol_ak_id AS POLICYAKID, 
	INSURANCESEGMENTABBREVIATION, 
	cust_role AS CUSTOMERROLE, 
	name AS NAME, 
	addr_line_1 AS ADDRESSLINE1, 
	city_name AS CITYNAME, 
	state_prov_code AS STATEPROVCODECONTRACTCUSTOMERADDRESS, 
	zip_postal_code AS ZIPPOSTALCODE, 
	CLASSCODE, 
	EXPOSURE, 
	DEDUCTIBLEAMOUNT, 
	INSURANCELINE, 
	BalMinPremiumTotal AS BALANCEMINIMUMPREMIUMTOTAL, 
	RATEEFFECTIVEDATE, 
	REASONAMENDEDCODE, 
	STRATEGICPROFITCENTERABBREVIATION, 
	pol_status_code AS POLICYSTATUSCODE, 
	PREMIUMTRANSACTIONENTEREDDATE, 
	OFFSETONSETCODE, 
	pol_key AS POLICYKEY, 
	COVERAGETYPE, 
	PREMIUMTRANSACTIONCODE, 
	COVERAGEGUID, 
	RATINGCOVERAGEAKID, 
	o_RunMonthAuditTransFlag AS RUNMONTHAUDITTRANSACTIONFLAG, 
	o_AgeOfPolicy AS AGEOFPOLICY, 
	TERMTYPE, 
	Period_start_date AS PERIODSTARTDATE, 
	Period_end_date AS PERIODENDDATE, 
	ANYARDINDICATOR, 
	EXPERIENCERATED, 
	PREMIUMTRANSACTIONEFFECTIVEDATE, 
	LOSSESSUBJECTTODEDUCTIBLECODE, 
	BASISOFDEDUCTIBLECALCULATIONCODE
	FROM EXP_Target_Load
),
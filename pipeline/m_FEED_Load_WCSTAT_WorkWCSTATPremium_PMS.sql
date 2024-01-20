WITH
LKP_PremiumTransaction_pol_ak_id AS (
	SELECT
	pol_ak_id
	FROM (
		SELECT DISTINCT P.pol_ak_id AS pol_ak_id
		   from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
		on SC.StatisticalCoverageAKID=PT.StatisticalCoverageAKID
		and SC.CurrentSnapshotFlag=1 and  PT.CurrentSnapshotFlag=1
		
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on PC.PolicyCoverageAKID=SC.PolicyCoverageAKID
		and PC.CurrentSnapshotFlag=1
		
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
		on PC.RiskLocationAKID=RL.RiskLocationAKID
		and RL.CurrentSnapshotFlag=1
		
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P
		on P.pol_ak_id=RL.PolicyAKID 
		and P.crrnt_snpsht_flag=1 
		
		where SC.SourceSystemID='PMS'  
		and PC.TypeBureauCode in ('WC','WP','WorkersCompensation')  and  PT.PremiumType='D'   and PT.ReasonAmendedCode != 'CWO'
		and DATEDIFF(MM,P.pol_eff_date, DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}, 0) )  > 18
		and PT.PremiumTransactionBookedDate>=DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}, 0)
		and PT.PremiumTransactionBookedDate<DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}+1, 0)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id ORDER BY pol_ak_id) = 1
),
SQ_Premium_PMS AS (
	DECLARE @startdate as datetime, 
	        @enddate as datetime
	
	SET @startdate = DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) +@{pipeline().parameters.NUM_OF_MONTHS}+1-18, 0) 
	SET @enddate =  DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}+1, 0)
	
	--PMS
	select 
	'PMS' SourceSystem
	,PremiumTransaction.PremiumTransactionID
	,PremiumTransaction.PremiumTransactionBookedDate
	,'01-01-1800 00:00:00' as StateRatingEffectiveDate
	,case when RPDT.RatingPlanCode='2' then '1' else '0' end as WCRetrospectiveRatingIndicator
	,PremiumTransaction.ExperienceModificationFactor
	,PremiumTransaction.ExperienceModificationEffectiveDate
	,PremiumTransaction.PremiumTransactionamount
	,PremiumTransaction.BaseRate
	,PolicyCoverage.TypeBureauCode
	,policy.pol_sym
	,policy.pol_num
	,policy.pol_mod
	 ,policy.pol_key AS pol_key
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
	--,PolicyCoverage.AuditableIndicator
	,InsuranceSegment.InsuranceSegmentAbbreviation
	,contract_customer.cust_role
	,contract_customer.name
	,contract_customer_address.addr_line_1
	,contract_customer_address.city_name
	,contract_customer_address.state_prov_code
	,contract_customer_address.zip_postal_code
	,StatisticalCoverage.ClassCode
	,PremiumTransaction.WrittenExposure
	,PremiumTransaction.DeductibleAmount
	,PolicyCoverage.InsuranceLine
	--,SUM(PremiumTransaction.PremiumTransactionamount) OVER (PARTITION BY policy.pol_key) as PolicyPremiumTotal
	--,SUM(CASE WHEN DATALENGTH(StatisticalCoverage.ClassCode) = 6 THEN 1 ELSE 0 END)  OVER (PARTITION BY policy.pol_key,RiskLocation.StateProvinceCode) as ManualPremiumInd
	--,SUM(CASE WHEN DATALENGTH(StatisticalCoverage.ClassCode) = 6 THEN PremiumTransaction.PremiumTransactionamount ELSE 0 END) OVER (PARTITION BY policy.pol_key,RiskLocation.StateProvinceCode) as PolicyStateManualPremiumTotal
	--,SUM(CASE WHEN StatisticalCoverage.ClassCode = '0990' THEN PremiumTransaction.PremiumTransactionamount ELSE 0 END) OVER (PARTITION BY policy.pol_key,RiskLocation.StateProvinceCode) as BalMinPremiumTotal
	--, policy.pol_key as policy_pol_key
	,PremiumTransaction.ReasonAmendedCode
	,StrategicProfitCenter.StrategicProfitCenterAbbreviation
	,policy.pol_status_code
	,PremiumTransaction.PremiumTransactionEnteredDate
	,PremiumTransaction.OffsetOnsetCode
	from    @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction
	
	inner join        @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage
	on StatisticalCoverage.StatisticalCoverageAKID=PremiumTransaction.StatisticalCoverageAKID
	and StatisticalCoverage.CurrentSnapshotFlag=1 and  PremiumTransaction.CurrentSnapshotFlag=1
	
	
	inner join        @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage
	on PolicyCoverage.PolicyCoverageAKID=StatisticalCoverage.PolicyCoverageAKID
	and PolicyCoverage.CurrentSnapshotFlag=1
	
	inner join        @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation
	on PolicyCoverage.RiskLocationAKID=RiskLocation.RiskLocationAKID
	and RiskLocation.CurrentSnapshotFlag=1
	
	inner join  @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy
	on policy.pol_ak_id=RiskLocation.PolicyAKID 
	and policy.crrnt_snpsht_flag=1
	
	
	inner join        @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter
	on policy.StrategicProfitCenterAKId=StrategicProfitCenter.StrategicProfitCenterAKId
	and StrategicProfitCenter.CurrentSnapshotFlag=1
	
	inner join        @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer
	ON contract_customer.contract_cust_ak_id = policy.contract_cust_ak_id and contract_customer.crrnt_snpsht_flag = 1
	
	inner join        @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_address
	ON contract_customer_address.contract_cust_ak_id = contract_customer.contract_cust_ak_id and contract_customer_address.crrnt_snpsht_flag = 1
	
	left join         @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment
	ON InsuranceSegment.InsuranceSegmentAKId = policy.InsuranceSegmentAKId and InsuranceSegment.CurrentSnapshotFlag = 1
	
	left join         @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingPlan RPDT
	ON PolicyCoverage.RatingPlanAKId=RPDT.RatingPlanAKId and RPDT.CurrentSnapshotFlag=1
	
	where StatisticalCoverage.SourceSystemID='PMS'  
	and PolicyCoverage.TypeBureauCode in ('WC','WP','WorkersCompensation')  and  PremiumTransaction.PremiumType='D'   and PremiumTransaction.ReasonAmendedCode != 'CWO'
	and policy.pol_eff_date<@startdate
	and PremiumTransaction.PremiumTransactionBookedDate<@enddate
	@{pipeline().parameters.PREMIUM_WHERE_CLAUSE}
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
	FROM SQ_Premium_PMS
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
	0 AS o_ManualPremiumIndicator,
	0 AS o_PolicyStateManualPremiumTotal,
	0 AS o_BalanceMinimumPremiumTotal,
	-- *INF*: TO_DATE('1800-01-01', 'YYYY-MM-DD')
	TO_TIMESTAMP('1800-01-01', 'YYYY-MM-DD') AS o_PremiumTransactionEffectiveDate,
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
	o_BalanceMinimumPremiumTotal AS BALANCEMINIMUMPREMIUMTOTAL, 
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
	o_PremiumTransactionEffectiveDate AS PREMIUMTRANSACTIONEFFECTIVEDATE, 
	LOSSESSUBJECTTODEDUCTIBLECODE, 
	BASISOFDEDUCTIBLECALCULATIONCODE
	FROM EXP_Target_Load
),
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
SQ_Premium_DCT AS (
	DECLARE @startdate as datetime, 
	        @enddate as datetime
	
	SET @startdate = DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) +@{pipeline().parameters.NUM_OF_MONTHS}+1-18, 0) 
	SET @enddate =  DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}+1, 0)
	
	--DCT
	select 
	'DCT' SourceSystem
	,PremiumTransaction.PremiumTransactionID
	,PremiumTransaction.PremiumTransactionBookedDate
	,'01-01-1800 00:00:00' as StateRatingEffectiveDate
	,case when RPDT.RatingPlanCode='2' then '1' else '0' end as WCRetrospectiveRatingIndicator
	,PremiumTransaction.ExperienceModificationFactor
	,PremiumTransaction.ExperienceModificationEffectiveDate
	,PremiumTransaction.PremiumTransactionAmount
	,CASE WHEN (RatingCoverage.CoverageType = 'ManualPremium')  OR (RatingCoverage.ClassCode in ('0771', '7445', '7453', '9046','9108')) THEN PremiumTransaction.BaseRate ELSE 0 END as BaseRate
	,PolicyCoverage.TypeBureauCode
	,'' AS pol_sym
	,policy.pol_num
	,policy.pol_mod
	,policy.pol_key
	,RatingCoverage.CoverageType
	,PremiumTransaction.PremiumTransactionCode
	,RatingCoverage.CoverageGUID
	,RatingCoverage.RatingCoverageAKID
	, CoverageDetailWorkersCompensation.TermStartDate
	, CoverageDetailWorkersCompensation.TermEndDate
	, CoverageDetailWorkersCompensation.ARDIndicatorFlag
	 , CoverageDetailWorkersCompensation.ExperienceRatedFlag
	 , CoverageDetailWorkersCompensation.TermType
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
	,RatingCoverage.ClassCode
	--,RatingCoverage.Exposure
	,PremiumTransaction.WrittenExposure
	,PremiumTransaction.DeductibleAmount
	,PolicyCoverage.InsuranceLine
	--,SUM(PremiumTransaction.PremiumTransactionamount) OVER (PARTITION BY policy.pol_key) as PolicyPremiumTotal
	--,SUM(CASE WHEN RatingCoverage.CoverageType = 'ManualPremium' THEN 1 ELSE 0 END)  OVER (PARTITION BY policy.pol_key,RiskLocation.StateProvinceCode) as ManualPremiumInd
	--,SUM(CASE WHEN RatingCoverage.CoverageType = 'ManualPremium' THEN PremiumTransaction.PremiumTransactionamount ELSE 0 END) OVER (PARTITION BY policy.pol_key,RiskLocation.StateProvinceCode) as PolicyStateManualPremiumTotal
	,99999999 as BalMinPremiumTotal
	,PremiumTransaction.ReasonAmendedCode
	,StrategicProfitCenter.StrategicProfitCenterAbbreviation
	,policy.pol_status_code
	,PremiumTransaction.PremiumTransactionEnteredDate
	,PremiumTransaction.OffsetOnsetCode
	,PremiumTransaction.PremiumTransactionEffectiveDate
	,CASE WHEN PremiumTransaction.DeductibleAmount <> 0 THEN MAX(CoverageDetailWorkersCompensation.DeductibleType) OVER (PARTITION BY policy.pol_key, RiskLocation.StateProvinceCode) ELSE '00' END AS DeductibleType
	,CASE WHEN PremiumTransaction.DeductibleAmount <> 0 THEN MAX(CoverageDetailWorkersCompensation.DeductibleBasis) OVER (PARTITION BY policy.pol_key, RiskLocation.StateProvinceCode) ELSE '00' END AS DeductibleBasis
	from     @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction
	 
	 inner join        @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage
	on RatingCoverage.RatingCoverageAKID=PremiumTransaction.RatingCoverageAKId
	and RatingCoverage.EffectiveDate=PremiumTransaction.EffectiveDate and PremiumTransaction.CurrentSnapshotFlag=1
	
	inner join        @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage
	on PolicyCoverage.PolicyCoverageAKID=RatingCoverage.PolicyCoverageAKID
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
	
	LEFT JOIN   @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailWorkersCompensation 
	ON  CoverageDetailWorkersCompensation.PremiumTransactionID=PremiumTransaction.PremiumTransactionID
	
	where  RatingCoverage.SourceSystemID='DCT' and PolicyCoverage.TypeBureauCode in ('WC','WP','WorkersCompensation')   
	and PremiumTransaction.PremiumType='D' 
	and PremiumTransaction.ReasonAmendedCode NOT IN ('CWO','Claw Back')
	and policy.pol_eff_date<@startdate
	and PremiumTransaction.PremiumTransactionBookedDate<@enddate
	@{pipeline().parameters.PREMIUM_WHERE_CLAUSE_DCT}
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
	-- *INF*: IIF(NOT ISNULL(Period_start_date),Period_start_date,pol_eff_date)
	-- 
	-- 
	-- 
	-- --DECODE(True,NOT ISNULL(Period_start_date), Period_start_date, pol_eff_date)
	IFF(Period_start_date IS NOT NULL, Period_start_date, pol_eff_date) AS RateEffectiveDate,
	PremiumTransactionEffectiveDate,
	DeductibleType,
	DeductibleBasis
	FROM SQ_Premium_DCT
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
	TermType, 
	PremiumTransactionEffectiveDate, 
	DeductibleType, 
	DeductibleBasis
	FROM EXP_identify_records_Wc_stat
	WHERE IIF(o_ValidRecordForWCSTat='VALID',TRUE,FALSE)
),
EXP_Target_Load AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
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
	BalMinPremiumTotal,
	RateEffectiveDate,
	-- *INF*: iif(ISNULL(RateEffectiveDate),pol_eff_date,RateEffectiveDate)
	IFF(RateEffectiveDate IS NULL, pol_eff_date, RateEffectiveDate) AS o_RateEffectiveDate,
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
	o_RunMonthAuditTransFlag,
	o_AgeOfPolicy,
	TermType,
	-- *INF*: IIF(ISNULL(TermType),'N/A',TermType)
	IFF(TermType IS NULL, 'N/A', TermType) AS o_TermType,
	Period_start_date,
	-- *INF*: iif(ISNULL(Period_start_date),pol_eff_date,Period_start_date)
	IFF(Period_start_date IS NULL, pol_eff_date, Period_start_date) AS o_Period_start_date,
	Period_end_date,
	-- *INF*: iif(ISNULL(Period_end_date),pol_exp_date,Period_end_date)
	IFF(Period_end_date IS NULL, pol_exp_date, Period_end_date) AS o_Period_end_date,
	AnyARDIndicator,
	-- *INF*: iif(ISNULL(AnyARDIndicator),'0',AnyARDIndicator)
	IFF(AnyARDIndicator IS NULL, '0', AnyARDIndicator) AS o_AnyARDIndicator,
	ExperienceRated,
	-- *INF*: IIF(ISNULL(ExperienceRated),'0',ExperienceRated)
	IFF(ExperienceRated IS NULL, '0', ExperienceRated) AS o_ExperienceRated,
	PremiumTransactionEffectiveDate,
	DeductibleType,
	-- *INF*: DECODE(TRUE,
	-- DeductibleAmount != '0' AND StateProvinceCode = '14', '01',
	-- StateProvinceCode = '09' AND DeductibleAmount != '0' AND (DeductibleType = '00' OR ISNULL(DeductibleType)) , '03',
	-- ISNULL(DeductibleType), '00',
	-- DeductibleType)
	-- 
	-- --IIF(ISNULL(DeductibleType), '00', DeductibleType)
	DECODE(
	    TRUE,
	    DeductibleAmount != '0' AND StateProvinceCode = '14', '01',
	    StateProvinceCode = '09' AND DeductibleAmount != '0' AND (DeductibleType = '00' OR DeductibleType IS NULL), '03',
	    DeductibleType IS NULL, '00',
	    DeductibleType
	) AS o_LossesSubjectToDeductibleCode,
	DeductibleBasis,
	-- *INF*: DECODE(TRUE,
	-- DeductibleAmount != '0' AND StateProvinceCode = '14', '01',
	-- StateProvinceCode = '09' AND DeductibleAmount != '0' AND (DeductibleBasis = '00' OR ISNULL(DeductibleBasis)), '01',
	-- ISNULL(DeductibleBasis), '00',
	-- DeductibleBasis)
	-- 
	-- --IIF(ISNULL(DeductibleBasis), '00', DeductibleBasis)
	DECODE(
	    TRUE,
	    DeductibleAmount != '0' AND StateProvinceCode = '14', '01',
	    StateProvinceCode = '09' AND DeductibleAmount != '0' AND (DeductibleBasis = '00' OR DeductibleBasis IS NULL), '01',
	    DeductibleBasis IS NULL, '00',
	    DeductibleBasis
	) AS o_BasisOfDeductibleCalculationCode
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
	o_RateEffectiveDate AS RATEEFFECTIVEDATE, 
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
	o_TermType AS TERMTYPE, 
	o_Period_start_date AS PERIODSTARTDATE, 
	o_Period_end_date AS PERIODENDDATE, 
	o_AnyARDIndicator AS ANYARDINDICATOR, 
	o_ExperienceRated AS EXPERIENCERATED, 
	PREMIUMTRANSACTIONEFFECTIVEDATE, 
	o_LossesSubjectToDeductibleCode AS LOSSESSUBJECTTODEDUCTIBLECODE, 
	o_BasisOfDeductibleCalculationCode AS BASISOFDEDUCTIBLECALCULATIONCODE
	FROM EXP_Target_Load
),
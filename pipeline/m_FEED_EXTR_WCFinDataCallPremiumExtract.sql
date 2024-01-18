WITH
SQ_WorkPremiumWorkersCompensationDataCallExtract AS (
	SELECT 
	WorkPremiumWorkersCompensationDataCallExtract.WorkPremiumWorkersCompensationDataCallExtractId, 
	WorkPremiumWorkersCompensationDataCallExtract.AuditId, WorkPremiumWorkersCompensationDataCallExtract.CreatedDate, 
	WorkPremiumWorkersCompensationDataCallExtract.EDWEarnedPremiumMonthlyCalculationPKID, WorkPremiumWorkersCompensationDataCallExtract.RunDate, 
	WorkPremiumWorkersCompensationDataCallExtract.EarnedPremiumRunDate, WorkPremiumWorkersCompensationDataCallExtract.PolicyKey,
	WorkPremiumWorkersCompensationDataCallExtract.PolicyEffectiveDate, WorkPremiumWorkersCompensationDataCallExtract.StateCode, WorkPremiumWorkersCompensationDataCallExtract.NCCIClassCode, workPremiumWorkersCompensationDataCallExtract.StrategicProfitCenterAbbreviation,
	WorkPremiumWorkersCompensationDataCallExtract.InsuranceSegmentDescription, WorkPremiumWorkersCompensationDataCallExtract.PolicyOfferingDescription,
	 WorkPremiumWorkersCompensationDataCallExtract.WorkersCompensationPremiumAdjustmentFactorEffectiveDate,
	WorkPremiumWorkersCompensationDataCallExtract.WorkersCompensationPremiumAdjustmentFactor, WorkPremiumWorkersCompensationDataCallExtract.WorkersCompensationPremiumAdjustmentType, 
		WorkPremiumWorkersCompensationDataCallExtract.ConsentToRateFlag, WorkPremiumWorkersCompensationDataCallExtract.RateOverride, WorkPremiumWorkersCompensationDataCallExtract.BaseRate, 
		WorkPremiumWorkersCompensationDataCallExtract.RatingStateType, WorkPremiumWorkersCompensationDataCallExtract.DirectEarnedPremium, 
		WorkPremiumWorkersCompensationDataCallExtract.RatingCompanyLevelEarnedPremium, WorkPremiumWorkersCompensationDataCallExtract.RatingDSRLevelEarnedPremium,
		WorkPremiumWorkersCompensationDataCallExtract.LossCostCompanyLevelEarnedPremium, WorkPremiumWorkersCompensationDataCallExtract.LossCostDSRLevelEarnedPremium 
	FROM
	WorkPremiumWorkersCompensationDataCallExtract
	where 
	( 
	  (@{pipeline().parameters.RUN_YEAR} !=0 AND
	YEAR(CAST(WorkPremiumWorkersCompensationDataCallExtract.RunDate AS DATE))=@{pipeline().parameters.RUN_YEAR})
	    OR
	  (@{pipeline().parameters.RUN_YEAR} =0 AND YEAR(CAST(WorkPremiumWorkersCompensationDataCallExtract.RunDate as DATE))=(SELECT YEAR(CAST(MAX(RunDate)as DATE)) from WorkPremiumWorkersCompensationDataCallExtract)))      
	order by statecode,PolicyEffectiveDate,EarnedPremiumRunDate,InsuranceSegmentDescription,PolicyOfferingDescription
),
EXP_WCFinDataCallPremiumExtractPassThrough AS (
	SELECT
	WorkPremiumWorkersCompensationDataCallExtractId,
	AuditId,
	CreatedDate,
	EDWEarnedPremiumMonthlyCalculationPKID,
	RunDate,
	EarnedPremiumRunDate,
	-- *INF*: GET_DATE_PART(EarnedPremiumRunDate, 'YYYY')
	DATE_PART(EarnedPremiumRunDate, 'YYYY') AS EPRunYear,
	PolicyKey,
	PolicyEffectiveDate,
	-- *INF*: GET_DATE_PART(PolicyEffectiveDate, 'YYYY')
	DATE_PART(PolicyEffectiveDate, 'YYYY') AS PolicyYear,
	StateCode,
	NCCIClassCode,
	StrategicProfitCenterAbbreviation,
	InsuranceSegmentDescription,
	PolicyOfferingDescription,
	WorkersCompensationPremiumAdjustmentFactorEffectiveDate,
	WorkersCompensationPremiumAdjustmentFactor,
	WorkersCompensationPremiumAdjustmentType,
	ConsentToRateFlag,
	-- *INF*: DECODE (TRUE,
	-- ConsentToRateFlag = 'F','0',
	-- ConsentToRateFlag = 'T','1',
	-- '0')
	-- --INC0020061: Fix reading of bit flag to activate override logic
	DECODE(
	    TRUE,
	    ConsentToRateFlag = 'F', '0',
	    ConsentToRateFlag = 'T', '1',
	    '0'
	) AS v_ConsentToRateFlag,
	RateOverride,
	BaseRate,
	RatingStateType,
	DirectEarnedPremium,
	RatingCompanyLevelEarnedPremium,
	RatingDSRLevelEarnedPremium,
	-- *INF*: IIF(WorkersCompensationPremiumAdjustmentType = 'Deviation' and v_ConsentToRateFlag ='1', (DirectEarnedPremium*  ( BaseRate / RateOverride) * WorkersCompensationPremiumAdjustmentFactor),RatingDSRLevelEarnedPremium)
	IFF(
	    WorkersCompensationPremiumAdjustmentType = 'Deviation' and v_ConsentToRateFlag = '1',
	    (DirectEarnedPremium * (BaseRate / RateOverride) * WorkersCompensationPremiumAdjustmentFactor),
	    RatingDSRLevelEarnedPremium
	) AS o_RatingDSRLevelEarnedPremium,
	LossCostCompanyLevelEarnedPremium,
	LossCostDSRLevelEarnedPremium,
	-- *INF*: IIF(WorkersCompensationPremiumAdjustmentType = 'LCM' and v_ConsentToRateFlag ='1', (DirectEarnedPremium*  ( BaseRate / RateOverride) * WorkersCompensationPremiumAdjustmentFactor),LossCostDSRLevelEarnedPremium)
	IFF(
	    WorkersCompensationPremiumAdjustmentType = 'LCM' and v_ConsentToRateFlag = '1',
	    (DirectEarnedPremium * (BaseRate / RateOverride) * WorkersCompensationPremiumAdjustmentFactor),
	    LossCostDSRLevelEarnedPremium
	) AS o_LossCostDSRLevelEarnedPremium
	FROM SQ_WorkPremiumWorkersCompensationDataCallExtract
),
AGGTRANS AS (
	SELECT
	WorkPremiumWorkersCompensationDataCallExtractId,
	AuditId,
	CreatedDate,
	EDWEarnedPremiumMonthlyCalculationPKID,
	RunDate,
	EPRunYear AS EarnedPremiumRunYear,
	PolicyKey,
	PolicyYear,
	StateCode,
	NCCIClassCode,
	StrategicProfitCenterAbbreviation,
	InsuranceSegmentDescription,
	PolicyOfferingDescription,
	WorkersCompensationPremiumAdjustmentFactorEffectiveDate,
	WorkersCompensationPremiumAdjustmentFactor,
	WorkersCompensationPremiumAdjustmentType,
	RateOverride,
	BaseRate,
	RatingStateType,
	DirectEarnedPremium,
	-- *INF*: sum(DirectEarnedPremium)
	sum(DirectEarnedPremium) AS o_DirectEarnedPremium,
	RatingCompanyLevelEarnedPremium,
	-- *INF*: sum(RatingCompanyLevelEarnedPremium)
	sum(RatingCompanyLevelEarnedPremium) AS o_RatingCompanyLevelEarnedPremium,
	o_RatingDSRLevelEarnedPremium AS RatingDSRLevelEarnedPremium,
	-- *INF*: sum(RatingDSRLevelEarnedPremium)
	sum(RatingDSRLevelEarnedPremium) AS o_RatingDSRLevelEarnedPremium,
	LossCostCompanyLevelEarnedPremium,
	-- *INF*: sum(LossCostCompanyLevelEarnedPremium)
	sum(LossCostCompanyLevelEarnedPremium) AS o_LossCostCompanyLevelEarnedPremium,
	o_LossCostDSRLevelEarnedPremium AS LossCostDSRLevelEarnedPremium,
	-- *INF*: sum(LossCostDSRLevelEarnedPremium)
	sum(LossCostDSRLevelEarnedPremium) AS o_LossCostDSRLevelEarnedPremium
	FROM EXP_WCFinDataCallPremiumExtractPassThrough
	GROUP BY EarnedPremiumRunYear, PolicyYear, StateCode, StrategicProfitCenterAbbreviation, InsuranceSegmentDescription, PolicyOfferingDescription
),
AGGTRANS1 AS (
	SELECT
	RunDate,
	EarnedPremiumRunYear,
	PolicyKey,
	PolicyYear,
	StateCode,
	StrategicProfitCenterAbbreviation,
	InsuranceSegmentDescription,
	PolicyOfferingDescription,
	o_DirectEarnedPremium AS DirectEarnedPremium,
	o_RatingCompanyLevelEarnedPremium AS RatingCompanyLevelEarnedPremium,
	o_RatingDSRLevelEarnedPremium AS RatingDSRLevelEarnedPremium,
	o_LossCostCompanyLevelEarnedPremium AS LossCostCompanyLevelEarnedPremium,
	o_LossCostDSRLevelEarnedPremium AS LossCostDSRLevelEarnedPremium,
	-- *INF*: SUM(RatingCompanyLevelEarnedPremium+LossCostCompanyLevelEarnedPremium)
	SUM(RatingCompanyLevelEarnedPremium + LossCostCompanyLevelEarnedPremium) AS o_CompanyLevelEP,
	-- *INF*: sum(RatingDSRLevelEarnedPremium+LossCostDSRLevelEarnedPremium)
	sum(RatingDSRLevelEarnedPremium + LossCostDSRLevelEarnedPremium) AS o_DSRLevelEP
	FROM AGGTRANS
	GROUP BY EarnedPremiumRunYear, PolicyYear, StateCode, StrategicProfitCenterAbbreviation, InsuranceSegmentDescription, PolicyOfferingDescription
),
SRT_EXTR AS (
	SELECT
	StateCode, 
	PolicyYear, 
	EarnedPremiumRunYear, 
	StrategicProfitCenterAbbreviation, 
	InsuranceSegmentDescription, 
	PolicyOfferingDescription, 
	RunDate, 
	DirectEarnedPremium, 
	o_CompanyLevelEP, 
	o_DSRLevelEP
	FROM AGGTRANS1
	ORDER BY StateCode ASC, PolicyYear ASC, EarnedPremiumRunYear ASC, StrategicProfitCenterAbbreviation ASC, InsuranceSegmentDescription ASC, PolicyOfferingDescription ASC
),
WorkPremiumWorkersCompensationExtractFile AS (
	INSERT INTO WorkPremiumWorkersCompensationExtractFile
	(RunDate, StateCode, StrategicProfitCenterAbbreviation, InsuranceSegmentDescription, PolicyOfferingDescription, PolicyYear, CalendarYear, DirectEarnedPremium, CompanyLevelEarnedPremium, DSRLevelEarnedPremium)
	SELECT 
	RUNDATE, 
	STATECODE, 
	STRATEGICPROFITCENTERABBREVIATION, 
	INSURANCESEGMENTDESCRIPTION, 
	POLICYOFFERINGDESCRIPTION, 
	POLICYYEAR, 
	EarnedPremiumRunYear AS CALENDARYEAR, 
	DIRECTEARNEDPREMIUM, 
	o_CompanyLevelEP AS COMPANYLEVELEARNEDPREMIUM, 
	o_DSRLevelEP AS DSRLEVELEARNEDPREMIUM
	FROM SRT_EXTR
),
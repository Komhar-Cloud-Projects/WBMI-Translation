WITH
SQ_WorkClaimWorkersCompensationDataCallExtract AS (
	SELECT WorkClaimWorkersCompensationDataCallExtract.WorkClaimWorkersCompensationDataCallExtractId, WorkClaimWorkersCompensationDataCallExtract.AuditId, 
	WorkClaimWorkersCompensationDataCallExtract.CreatedDate, WorkClaimWorkersCompensationDataCallExtract.RunDate, WorkClaimWorkersCompensationDataCallExtract.StrategicProfitCenterAbbreviation, 
	WorkClaimWorkersCompensationDataCallExtract.InsuranceSegmentDescription, WorkClaimWorkersCompensationDataCallExtract.PolicyOfferingDescription,
	 WorkClaimWorkersCompensationDataCallExtract.PolicyKey, WorkClaimWorkersCompensationDataCallExtract.PolicyEffectiveDate, WorkClaimWorkersCompensationDataCallExtract.ClaimOccurrenceKey,
	  WorkClaimWorkersCompensationDataCallExtract.StateCode, WorkClaimWorkersCompensationDataCallExtract.ClaimLossDate, WorkClaimWorkersCompensationDataCallExtract.IndemnityOpenClaimCount,
	  WorkClaimWorkersCompensationDataCallExtract.IndemnityClosedWithPayClaimCount, WorkClaimWorkersCompensationDataCallExtract.DirectLossPaidToDate, 
	  WorkClaimWorkersCompensationDataCallExtract.OutstandingAmountToDate 
	FROM
	WorkClaimWorkersCompensationDataCallExtract
	where 
	( 
	   (@{pipeline().parameters.RUN_YEAR} !=0 AND YEAR(CAST(WorkClaimWorkersCompensationDataCallExtract.RunDate as DATE))=@{pipeline().parameters.RUN_YEAR})
	  OR
	  (@{pipeline().parameters.RUN_YEAR} =0 AND YEAR(CAST(WorkClaimWorkersCompensationDataCallExtract.RunDate as DATE))=(SELECT YEAR(CAST(MAX(RunDate)as DATE)) from WorkClaimWorkersCompensationDataCallExtract))
	) 
	
	order by PolicyEffectiveDate,StateCode
),
EXP_WCFinDataCallClaimExtract AS (
	SELECT
	WorkClaimWorkersCompensationDataCallExtractId,
	AuditId,
	CreatedDate,
	RunDate,
	StrategicProfitCenterAbbreviation,
	InsuranceSegmentDescription,
	PolicyOfferingDescription,
	PolicyKey,
	PolicyEffectiveDate,
	-- *INF*: GET_DATE_PART(PolicyEffectiveDate, 'YYYY')
	DATE_PART(PolicyEffectiveDate, 'YYYY') AS PolicyYear,
	ClaimOccurrenceKey,
	StateCode,
	ClaimLossDate,
	-- *INF*: GET_DATE_PART(ClaimLossDate, 'YYYY')
	DATE_PART(ClaimLossDate, 'YYYY') AS ClaimLossYear,
	IndemnityOpenClaimCount,
	IndemnityClosedWithPayClaimCount,
	DirectLossPaidToDate,
	OutstandingAmountToDate
	FROM SQ_WorkClaimWorkersCompensationDataCallExtract
),
AGG_WCFinDataCallClaimExtract AS (
	SELECT
	WorkClaimWorkersCompensationDataCallExtractId,
	AuditId,
	CreatedDate,
	RunDate,
	StrategicProfitCenterAbbreviation,
	InsuranceSegmentDescription,
	PolicyOfferingDescription,
	PolicyKey,
	PolicyYear,
	ClaimOccurrenceKey,
	StateCode,
	ClaimLossYear,
	IndemnityOpenClaimCount,
	-- *INF*: sum(IndemnityOpenClaimCount)
	sum(IndemnityOpenClaimCount) AS o_OpenClaimCount,
	IndemnityClosedWithPayClaimCount,
	-- *INF*: sum(IndemnityClosedWithPayClaimCount)
	sum(IndemnityClosedWithPayClaimCount) AS o_ClosedClaimCount,
	DirectLossPaidToDate,
	OutstandingAmountToDate
	FROM EXP_WCFinDataCallClaimExtract
	GROUP BY StrategicProfitCenterAbbreviation, InsuranceSegmentDescription, PolicyOfferingDescription, PolicyKey, PolicyYear, StateCode, ClaimLossYear
),
SRT_EXTR AS (
	SELECT
	StateCode, 
	PolicyKey, 
	PolicyYear, 
	ClaimLossYear, 
	StrategicProfitCenterAbbreviation, 
	InsuranceSegmentDescription, 
	PolicyOfferingDescription, 
	RunDate, 
	o_OpenClaimCount, 
	o_ClosedClaimCount
	FROM AGG_WCFinDataCallClaimExtract
	ORDER BY StateCode ASC, PolicyKey ASC, PolicyYear ASC, ClaimLossYear ASC, StrategicProfitCenterAbbreviation ASC, InsuranceSegmentDescription ASC, PolicyOfferingDescription ASC
),
WorkClaimWorkersCompensationExtractFile AS (
	INSERT INTO WorkClaimWorkersCompensationExtractFile
	(RunDate, StateCode, StrategicProfitCenterAbbreviation, InsuranceSegmentDescription, PolicyOfferingDescription, PolicyYear, AccidentYear, IndemnityOpenCount, IndemnityCWPCount)
	SELECT 
	RUNDATE, 
	STATECODE, 
	STRATEGICPROFITCENTERABBREVIATION, 
	INSURANCESEGMENTDESCRIPTION, 
	POLICYOFFERINGDESCRIPTION, 
	POLICYYEAR, 
	ClaimLossYear AS ACCIDENTYEAR, 
	o_OpenClaimCount AS INDEMNITYOPENCOUNT, 
	o_ClosedClaimCount AS INDEMNITYCWPCOUNT
	FROM SRT_EXTR
),
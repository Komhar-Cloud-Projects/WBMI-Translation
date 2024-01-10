WITH
LKP_calender_dim AS (
	SELECT
	clndr_id,
	clndr_date
	FROM (
		SELECT 
			clndr_id,
			clndr_date
		FROM calendar_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY clndr_date ORDER BY clndr_id) = 1
),
SQ_PremiumMasterFact AS (
	SELECT 
	AnnualStatementLineDimID
	,AnnualStatementLineProductCodeDimID
	,AgencyDimID
	,PolicyDimID
	,ContractCustomerDimID
	,RiskLocationDimID
	,ReinsuranceCoverageDimID
	,pmf.PremiumTransactionTypeDimID
	,EDWPremiumMasterCalculationPKID
	,EDWPremiumTransactionPKID
	,StagePremiumMasterPKID
	,PremiumMasterPolicyEffectiveDateID
	,PremiumMasterPolicyExpirationDateID
	,PremiumMasterCoverageEffectiveDateID
	,PremiumMasterCoverageExpirationDateID
	,PremiumMasterRunDateID
	,PremiumMasterPremium
	,pmf.InsuranceReferenceDimId
	,SalesDivisionDimId
	,pmf.InsuranceReferenceCoverageDimId
	,CoverageDetailDimId
	,i.PolicyOfferingAbbreviation
	,i.InsuranceSegmentAbbreviation
	,pmf.PolicyAuditDimId
	,pmf.PremiumTransactionEnteredDateId
	,p.pol_key, pmf.DeclaredEventFlag
	FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterFact pmf
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim cal on pmf.PremiumMasterRunDateID= cal.clndr_id
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.insurancereferencedim i on pmf.insurancereferencedimid = i.insurancereferencedimid
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim p on p.pol_dim_id = pmf.policydimid
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransactionTypeDim ptd on pmf.PremiumTransactionTypeDimID = ptd.PremiumTransactionTypeDimID
	where p.pol_sym='000'
	and i.PolicyOfferingAbbreviation = 'WC' and i.InsuranceSegmentAbbreviation in ( 'CL','POOL') and pmf.AuditID not in (-2,-3) 
	and ptd.ReasonAmendedCode not in ('CWO','CWB')
	and cal.CalendarDate >= DATEADD(mm, DATEDIFF(mm,0,getdate()) + @{pipeline().parameters.NUM_OF_MONTH} ,0)
	and cal.CalendarDate < DATEADD(mm, DATEDIFF(mm,0,getdate()) + @{pipeline().parameters.NUM_OF_MONTH} +1 ,0)
	and i.InsuranceSegmentCode = '3'
	@{pipeline().parameters.WHERE_CLAUSE}
	order by p.pol_key, PremiumMasterFactID
),
AGG_Policy AS (
	SELECT
	AnnualStatementLineDimID,
	AnnualStatementLineProductCodeDimID,
	AgencyDimID,
	PolicyDimID,
	ContractCustomerDimID,
	RiskLocationDimID,
	ReinsuranceCoverageDimID,
	PremiumTransactionTypeDimID,
	EDWPremiumMasterCalculationPKID,
	EDWPremiumTransactionPKID,
	StagePremiumMasterPKID,
	PremiumMasterPolicyEffectiveDateID,
	PremiumMasterPolicyExpirationDateID,
	PremiumMasterCoverageEffectiveDateID,
	PremiumMasterCoverageExpirationDateID,
	PremiumMasterRunDateID,
	PremiumMasterPremium,
	-- *INF*: sum(PremiumMasterPremium)
	sum(PremiumMasterPremium
	) AS o_TotalAmountMonthly,
	InsuranceReferenceDimId,
	SalesDivisionDimId,
	InsuranceReferenceCoverageDimId,
	CoverageDetailDimId,
	PolicyOfferingAbbreviation,
	InsuranceSegmentAbbreviation,
	PolicyAuditDimId,
	PremiumTransactionEnteredDateId,
	pol_key,
	DeclaredEventFlag
	FROM SQ_PremiumMasterFact
	GROUP BY pol_key
),
LKP_GetHistoryAmount AS (
	SELECT
	HistoryAmount,
	pol_key
	FROM (
		select 
		p.pol_key as pol_key, 
		sum(pmf.PremiumMasterPremium) as HistoryAmount
		from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterFact pmf
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim cd
		on pmf.PremiumMasterRunDateID = cd.clndr_id
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.insurancereferencedim i on pmf.insurancereferencedimid = i.insurancereferencedimid
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim p on p.pol_dim_id = pmf.policydimid
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransactionTypeDim ptd on pmf.PremiumTransactionTypeDimID = ptd.PremiumTransactionTypeDimID
		where p.pol_sym='000'
		and i.PolicyOfferingAbbreviation = 'WC' and i.InsuranceSegmentAbbreviation in ( 'CL','POOL')
		and ptd.ReasonAmendedCode not in ('CWO','CWB') and pmf.AuditID not in ( -2,-3)
		and cd.CalendarDate < DATEADD(mm, DATEDIFF(mm,0,getdate()) + @{pipeline().parameters.NUM_OF_MONTH} ,0)
		and i.InsuranceSegmentCode = '3'
		group by p.pol_key
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY HistoryAmount) = 1
),
EXP_CalculateCommission AS (
	SELECT
	LKP_GetHistoryAmount.HistoryAmount AS i_HistoryAmount,
	AGG_Policy.AnnualStatementLineDimID,
	AGG_Policy.AnnualStatementLineProductCodeDimID,
	AGG_Policy.AgencyDimID,
	AGG_Policy.PolicyDimID,
	AGG_Policy.ContractCustomerDimID,
	AGG_Policy.RiskLocationDimID,
	AGG_Policy.ReinsuranceCoverageDimID,
	AGG_Policy.PremiumTransactionTypeDimID,
	AGG_Policy.EDWPremiumMasterCalculationPKID,
	AGG_Policy.EDWPremiumTransactionPKID,
	AGG_Policy.StagePremiumMasterPKID,
	AGG_Policy.PremiumMasterPolicyEffectiveDateID,
	AGG_Policy.PremiumMasterPolicyExpirationDateID,
	AGG_Policy.PremiumMasterCoverageEffectiveDateID,
	AGG_Policy.PremiumMasterCoverageExpirationDateID,
	AGG_Policy.PremiumMasterRunDateID,
	AGG_Policy.o_TotalAmountMonthly AS i_TotalAmountMonthly,
	AGG_Policy.InsuranceReferenceDimId,
	AGG_Policy.SalesDivisionDimId,
	AGG_Policy.InsuranceReferenceCoverageDimId,
	AGG_Policy.CoverageDetailDimId,
	AGG_Policy.PolicyOfferingAbbreviation,
	AGG_Policy.InsuranceSegmentAbbreviation,
	-- *INF*: IIF(ISNULL(i_TotalAmountMonthly),0,i_TotalAmountMonthly)
	IFF(i_TotalAmountMonthly IS NULL,
		0,
		i_TotalAmountMonthly
	) AS v_TotalAmountMonthly,
	-- *INF*: IIF(ISNULL(i_HistoryAmount),0,i_HistoryAmount)
	IFF(i_HistoryAmount IS NULL,
		0,
		i_HistoryAmount
	) AS v_HistoryAmount,
	v_TotalAmountMonthly + v_HistoryAmount AS v_TotalAmount,
	5000 AS v_GraduatedAmount_CL,
	10000 AS v_GraduatedAmount_Pool,
	0.1 AS v_level1_rate_CL,
	0.04 AS v_level1_rate_Pool,
	0.05 AS v_level2_rate_CL,
	0.01 AS v_level2_rate_Pool,
	-- *INF*: DECODE(TRUE ,InsuranceSegmentAbbreviation='CL',
	-- DECODE(TRUE ,v_TotalAmount - v_HistoryAmount=0,0,
	-- v_TotalAmount>0 AND v_HistoryAmount=0 AND v_TotalAmount-v_HistoryAmount <= v_GraduatedAmount_CL, v_TotalAmount,
	-- v_TotalAmount>0 AND v_HistoryAmount=0 AND v_TotalAmount-v_HistoryAmount  > v_GraduatedAmount_CL , v_GraduatedAmount_CL,
	-- v_TotalAmount>v_GraduatedAmount_CL AND v_HistoryAmount <= v_GraduatedAmount_CL AND v_TotalAmount - v_HistoryAmount  > 0 , v_GraduatedAmount_CL - v_HistoryAmount,
	-- v_TotalAmount<=v_GraduatedAmount_CL AND v_HistoryAmount <= v_GraduatedAmount_CL AND v_TotalAmount - v_HistoryAmount  <> 0 , v_TotalAmount - v_HistoryAmount,
	-- v_TotalAmount<v_GraduatedAmount_CL AND v_HistoryAmount > v_GraduatedAmount_CL AND v_TotalAmount - v_HistoryAmount  < 0 , v_TotalAmount - v_GraduatedAmount_CL,
	-- v_TotalAmount>=v_GraduatedAmount_CL AND v_HistoryAmount > v_GraduatedAmount_CL AND v_TotalAmount - v_HistoryAmount  <> 0 , 0,
	-- 0),
	-- InsuranceSegmentAbbreviation='Pool',
	-- DECODE(TRUE ,v_TotalAmount - v_HistoryAmount=0,0,
	-- v_TotalAmount>0 AND v_HistoryAmount=0 AND v_TotalAmount-v_HistoryAmount <= v_GraduatedAmount_Pool, v_TotalAmount,
	-- v_TotalAmount>0 AND v_HistoryAmount=0 AND v_TotalAmount-v_HistoryAmount  > v_GraduatedAmount_Pool , v_GraduatedAmount_Pool,
	-- v_TotalAmount>v_GraduatedAmount_Pool AND v_HistoryAmount <= v_GraduatedAmount_Pool AND v_TotalAmount - v_HistoryAmount  > 0 , v_GraduatedAmount_Pool - v_HistoryAmount,
	-- v_TotalAmount<=v_GraduatedAmount_Pool AND v_HistoryAmount <= v_GraduatedAmount_Pool AND v_TotalAmount - v_HistoryAmount  <> 0 , v_TotalAmount - v_HistoryAmount,
	-- v_TotalAmount<v_GraduatedAmount_Pool AND v_HistoryAmount > v_GraduatedAmount_Pool AND v_TotalAmount - v_HistoryAmount  < 0 , v_TotalAmount - v_GraduatedAmount_Pool,
	-- v_TotalAmount>=v_GraduatedAmount_Pool AND v_HistoryAmount > v_GraduatedAmount_Pool AND v_TotalAmount - v_HistoryAmount  <> 0 , 0,
	-- 0),
	-- 0)
	DECODE(TRUE,
		InsuranceSegmentAbbreviation = 'CL', DECODE(TRUE,
		v_TotalAmount - v_HistoryAmount = 0, 0,
		v_TotalAmount > 0 
			AND v_HistoryAmount = 0 
			AND v_TotalAmount - v_HistoryAmount <= v_GraduatedAmount_CL, v_TotalAmount,
		v_TotalAmount > 0 
			AND v_HistoryAmount = 0 
			AND v_TotalAmount - v_HistoryAmount > v_GraduatedAmount_CL, v_GraduatedAmount_CL,
		v_TotalAmount > v_GraduatedAmount_CL 
			AND v_HistoryAmount <= v_GraduatedAmount_CL 
			AND v_TotalAmount - v_HistoryAmount > 0, v_GraduatedAmount_CL - v_HistoryAmount,
		v_TotalAmount <= v_GraduatedAmount_CL 
			AND v_HistoryAmount <= v_GraduatedAmount_CL 
			AND v_TotalAmount - v_HistoryAmount <> 0, v_TotalAmount - v_HistoryAmount,
		v_TotalAmount < v_GraduatedAmount_CL 
			AND v_HistoryAmount > v_GraduatedAmount_CL 
			AND v_TotalAmount - v_HistoryAmount < 0, v_TotalAmount - v_GraduatedAmount_CL,
		v_TotalAmount >= v_GraduatedAmount_CL 
			AND v_HistoryAmount > v_GraduatedAmount_CL 
			AND v_TotalAmount - v_HistoryAmount <> 0, 0,
		0
		),
		InsuranceSegmentAbbreviation = 'Pool', DECODE(TRUE,
		v_TotalAmount - v_HistoryAmount = 0, 0,
		v_TotalAmount > 0 
			AND v_HistoryAmount = 0 
			AND v_TotalAmount - v_HistoryAmount <= v_GraduatedAmount_Pool, v_TotalAmount,
		v_TotalAmount > 0 
			AND v_HistoryAmount = 0 
			AND v_TotalAmount - v_HistoryAmount > v_GraduatedAmount_Pool, v_GraduatedAmount_Pool,
		v_TotalAmount > v_GraduatedAmount_Pool 
			AND v_HistoryAmount <= v_GraduatedAmount_Pool 
			AND v_TotalAmount - v_HistoryAmount > 0, v_GraduatedAmount_Pool - v_HistoryAmount,
		v_TotalAmount <= v_GraduatedAmount_Pool 
			AND v_HistoryAmount <= v_GraduatedAmount_Pool 
			AND v_TotalAmount - v_HistoryAmount <> 0, v_TotalAmount - v_HistoryAmount,
		v_TotalAmount < v_GraduatedAmount_Pool 
			AND v_HistoryAmount > v_GraduatedAmount_Pool 
			AND v_TotalAmount - v_HistoryAmount < 0, v_TotalAmount - v_GraduatedAmount_Pool,
		v_TotalAmount >= v_GraduatedAmount_Pool 
			AND v_HistoryAmount > v_GraduatedAmount_Pool 
			AND v_TotalAmount - v_HistoryAmount <> 0, 0,
		0
		),
		0
	) AS v_PremiumForLevel1,
	-- *INF*: DECODE(TRUE ,InsuranceSegmentAbbreviation='CL',
	-- DECODE(TRUE ,v_TotalAmount - v_HistoryAmount=0,0,
	-- v_TotalAmount>0 AND v_HistoryAmount=0 AND v_TotalAmount-v_HistoryAmount <= v_GraduatedAmount_CL, 0,
	-- v_TotalAmount>0 AND v_HistoryAmount=0 AND v_TotalAmount-v_HistoryAmount  > v_GraduatedAmount_CL , v_TotalAmount -v_GraduatedAmount_CL,
	-- v_TotalAmount>v_GraduatedAmount_CL AND v_HistoryAmount <= v_GraduatedAmount_CL AND v_TotalAmount - v_HistoryAmount  > 0 , v_TotalAmount-v_GraduatedAmount_CL,
	-- v_TotalAmount<=v_GraduatedAmount_CL AND v_HistoryAmount <= v_GraduatedAmount_CL AND v_TotalAmount - v_HistoryAmount  <> 0 , 0,
	-- v_TotalAmount<v_GraduatedAmount_CL AND v_HistoryAmount > v_GraduatedAmount_CL AND v_TotalAmount - v_HistoryAmount  < 0 , v_GraduatedAmount_CL-v_HistoryAmount,
	-- v_TotalAmount>=v_GraduatedAmount_CL AND v_HistoryAmount > v_GraduatedAmount_CL AND v_TotalAmount - v_HistoryAmount  <> 0 , v_TotalAmount-v_HistoryAmount,
	-- 0),
	-- InsuranceSegmentAbbreviation='Pool',
	-- DECODE(TRUE ,v_TotalAmount - v_HistoryAmount=0,0,
	-- v_TotalAmount>0 AND v_HistoryAmount=0 AND v_TotalAmount-v_HistoryAmount <= v_GraduatedAmount_Pool, 0,
	-- v_TotalAmount>0 AND v_HistoryAmount=0 AND v_TotalAmount-v_HistoryAmount  > v_GraduatedAmount_Pool , v_TotalAmount-v_GraduatedAmount_Pool,
	-- v_TotalAmount>v_GraduatedAmount_Pool AND v_HistoryAmount <= v_GraduatedAmount_Pool AND v_TotalAmount - v_HistoryAmount  > 0 , v_TotalAmount-v_GraduatedAmount_Pool,
	-- v_TotalAmount<=v_GraduatedAmount_Pool AND v_HistoryAmount <= v_GraduatedAmount_Pool AND v_TotalAmount - v_HistoryAmount  <> 0 , 0,
	-- v_TotalAmount<v_GraduatedAmount_Pool AND v_HistoryAmount > v_GraduatedAmount_Pool AND v_TotalAmount - v_HistoryAmount  < 0 , v_GraduatedAmount_Pool-v_HistoryAmount,
	-- v_TotalAmount>=v_GraduatedAmount_Pool AND v_HistoryAmount > v_GraduatedAmount_Pool AND v_TotalAmount - v_HistoryAmount  <> 0 , v_TotalAmount - v_HistoryAmount,
	-- 0),
	-- 0)
	DECODE(TRUE,
		InsuranceSegmentAbbreviation = 'CL', DECODE(TRUE,
		v_TotalAmount - v_HistoryAmount = 0, 0,
		v_TotalAmount > 0 
			AND v_HistoryAmount = 0 
			AND v_TotalAmount - v_HistoryAmount <= v_GraduatedAmount_CL, 0,
		v_TotalAmount > 0 
			AND v_HistoryAmount = 0 
			AND v_TotalAmount - v_HistoryAmount > v_GraduatedAmount_CL, v_TotalAmount - v_GraduatedAmount_CL,
		v_TotalAmount > v_GraduatedAmount_CL 
			AND v_HistoryAmount <= v_GraduatedAmount_CL 
			AND v_TotalAmount - v_HistoryAmount > 0, v_TotalAmount - v_GraduatedAmount_CL,
		v_TotalAmount <= v_GraduatedAmount_CL 
			AND v_HistoryAmount <= v_GraduatedAmount_CL 
			AND v_TotalAmount - v_HistoryAmount <> 0, 0,
		v_TotalAmount < v_GraduatedAmount_CL 
			AND v_HistoryAmount > v_GraduatedAmount_CL 
			AND v_TotalAmount - v_HistoryAmount < 0, v_GraduatedAmount_CL - v_HistoryAmount,
		v_TotalAmount >= v_GraduatedAmount_CL 
			AND v_HistoryAmount > v_GraduatedAmount_CL 
			AND v_TotalAmount - v_HistoryAmount <> 0, v_TotalAmount - v_HistoryAmount,
		0
		),
		InsuranceSegmentAbbreviation = 'Pool', DECODE(TRUE,
		v_TotalAmount - v_HistoryAmount = 0, 0,
		v_TotalAmount > 0 
			AND v_HistoryAmount = 0 
			AND v_TotalAmount - v_HistoryAmount <= v_GraduatedAmount_Pool, 0,
		v_TotalAmount > 0 
			AND v_HistoryAmount = 0 
			AND v_TotalAmount - v_HistoryAmount > v_GraduatedAmount_Pool, v_TotalAmount - v_GraduatedAmount_Pool,
		v_TotalAmount > v_GraduatedAmount_Pool 
			AND v_HistoryAmount <= v_GraduatedAmount_Pool 
			AND v_TotalAmount - v_HistoryAmount > 0, v_TotalAmount - v_GraduatedAmount_Pool,
		v_TotalAmount <= v_GraduatedAmount_Pool 
			AND v_HistoryAmount <= v_GraduatedAmount_Pool 
			AND v_TotalAmount - v_HistoryAmount <> 0, 0,
		v_TotalAmount < v_GraduatedAmount_Pool 
			AND v_HistoryAmount > v_GraduatedAmount_Pool 
			AND v_TotalAmount - v_HistoryAmount < 0, v_GraduatedAmount_Pool - v_HistoryAmount,
		v_TotalAmount >= v_GraduatedAmount_Pool 
			AND v_HistoryAmount > v_GraduatedAmount_Pool 
			AND v_TotalAmount - v_HistoryAmount <> 0, v_TotalAmount - v_HistoryAmount,
		0
		),
		0
	) AS v_PremiumForLevel2,
	-- *INF*: DECODE(TRUE , InsuranceSegmentAbbreviation='CL',v_level1_rate_CL,
	-- InsuranceSegmentAbbreviation='Pool',v_level1_rate_Pool,
	-- 0)
	DECODE(TRUE,
		InsuranceSegmentAbbreviation = 'CL', v_level1_rate_CL,
		InsuranceSegmentAbbreviation = 'Pool', v_level1_rate_Pool,
		0
	) AS v_CommissionRateLevel1,
	-- *INF*: DECODE(TRUE , InsuranceSegmentAbbreviation='CL',v_level2_rate_CL,
	-- InsuranceSegmentAbbreviation='Pool',v_level2_rate_Pool,
	-- 0)
	DECODE(TRUE,
		InsuranceSegmentAbbreviation = 'CL', v_level2_rate_CL,
		InsuranceSegmentAbbreviation = 'Pool', v_level2_rate_Pool,
		0
	) AS v_CommissionRateLevel2,
	v_PremiumForLevel1 * v_CommissionRateLevel1 AS v_CommissionAmountLevel1,
	v_PremiumForLevel2 * v_CommissionRateLevel2 AS v_CommissionAmountLevel2,
	v_PremiumForLevel1 AS o_PremiumForLevel1,
	v_PremiumForLevel2 AS o_PremiumForLevel2,
	v_CommissionRateLevel1 AS o_CommissionRateLevel1,
	v_CommissionRateLevel2 AS o_CommissionRateLevel2,
	v_CommissionAmountLevel1 AS o_CommissionAmountLevel1,
	v_CommissionAmountLevel2 AS o_CommissionAmountLevel2,
	-- *INF*: SET_DATE_PART(
	--          SET_DATE_PART(
	--                      SET_DATE_PART(last_day(add_to_date(sysdate,'MM',@{pipeline().parameters.NUM_OF_MONTH})), 'HH', 23) 
	--                                           ,'MI',59)
	--                                ,'SS',59)
	DATEADD(SECOND,59-DATE_PART(SECOND,DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,last_day(DATEADD(MONTH,@{pipeline().parameters.NUM_OF_MONTH},sysdate)
	)),last_day(DATEADD(MONTH,@{pipeline().parameters.NUM_OF_MONTH},sysdate)
	))),DATEADD(HOUR,23-DATE_PART(HOUR,last_day(DATEADD(MONTH,@{pipeline().parameters.NUM_OF_MONTH},sysdate)
	)),last_day(DATEADD(MONTH,@{pipeline().parameters.NUM_OF_MONTH},sysdate)
	)))),DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,last_day(DATEADD(MONTH,@{pipeline().parameters.NUM_OF_MONTH},sysdate)
	)),last_day(DATEADD(MONTH,@{pipeline().parameters.NUM_OF_MONTH},sysdate)
	))),DATEADD(HOUR,23-DATE_PART(HOUR,last_day(DATEADD(MONTH,@{pipeline().parameters.NUM_OF_MONTH},sysdate)
	)),last_day(DATEADD(MONTH,@{pipeline().parameters.NUM_OF_MONTH},sysdate)
	)))) AS v_PremiumMasterBookedDate,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(v_PremiumMasterBookedDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_v_PremiumMasterBookedDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PremiumMasterBookedDateID,
	-- *INF*: IIF(NOT ISNULL(v_PremiumMasterBookedDateID),v_PremiumMasterBookedDateID,-1)
	-- 
	IFF(v_PremiumMasterBookedDateID IS NOT NULL,
		v_PremiumMasterBookedDateID,
		- 1
	) AS o_PremiumMasterBookedDateID,
	AGG_Policy.PolicyAuditDimId,
	AGG_Policy.PremiumTransactionEnteredDateId,
	AGG_Policy.pol_key,
	AGG_Policy.DeclaredEventFlag
	FROM AGG_Policy
	LEFT JOIN LKP_GetHistoryAmount
	ON LKP_GetHistoryAmount.pol_key = AGG_Policy.pol_key
	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_v_PremiumMasterBookedDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_v_PremiumMasterBookedDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(v_PremiumMasterBookedDate, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

),
RTRTRANS AS (
	SELECT
	AnnualStatementLineDimID,
	AnnualStatementLineProductCodeDimID,
	AgencyDimID,
	PolicyDimID,
	ContractCustomerDimID,
	RiskLocationDimID,
	ReinsuranceCoverageDimID,
	PremiumTransactionTypeDimID,
	EDWPremiumMasterCalculationPKID,
	EDWPremiumTransactionPKID,
	StagePremiumMasterPKID,
	PremiumMasterPolicyEffectiveDateID,
	PremiumMasterPolicyExpirationDateID,
	PremiumMasterCoverageEffectiveDateID,
	PremiumMasterCoverageExpirationDateID,
	PremiumMasterRunDateID,
	InsuranceReferenceDimId,
	SalesDivisionDimId,
	InsuranceReferenceCoverageDimId,
	CoverageDetailDimId,
	o_PremiumForLevel1,
	o_PremiumForLevel2,
	o_CommissionRateLevel1,
	o_CommissionRateLevel2,
	o_CommissionAmountLevel1,
	o_CommissionAmountLevel2,
	o_PremiumMasterBookedDateID,
	PolicyAuditDimId,
	PremiumTransactionEnteredDateId,
	pol_key,
	DeclaredEventFlag
	FROM EXP_CalculateCommission
),
RTRTRANS_CommissionLevel1 AS (SELECT * FROM RTRTRANS WHERE TRUE),
RTRTRANS_CommissionLevel2 AS (SELECT * FROM RTRTRANS WHERE TRUE),
FIL_Level1 AS (
	SELECT
	AnnualStatementLineDimID AS AnnualStatementLineDimID1, 
	AnnualStatementLineProductCodeDimID AS AnnualStatementLineProductCodeDimID1, 
	AgencyDimID AS AgencyDimID1, 
	PolicyDimID AS PolicyDimID1, 
	ContractCustomerDimID AS ContractCustomerDimID1, 
	RiskLocationDimID AS RiskLocationDimID1, 
	ReinsuranceCoverageDimID AS ReinsuranceCoverageDimID1, 
	PremiumTransactionTypeDimID AS PremiumTransactionTypeDimID1, 
	EDWPremiumMasterCalculationPKID AS EDWPremiumMasterCalculationPKID1, 
	EDWPremiumTransactionPKID AS EDWPremiumTransactionPKID1, 
	StagePremiumMasterPKID AS StagePremiumMasterPKID1, 
	PremiumMasterPolicyEffectiveDateID AS PremiumMasterPolicyEffectiveDateID1, 
	PremiumMasterPolicyExpirationDateID AS PremiumMasterPolicyExpirationDateID1, 
	PremiumMasterCoverageEffectiveDateID AS PremiumMasterCoverageEffectiveDateID1, 
	PremiumMasterCoverageExpirationDateID AS PremiumMasterCoverageExpirationDateID1, 
	PremiumMasterRunDateID AS PremiumMasterRunDateID1, 
	InsuranceReferenceDimId AS InsuranceReferenceDimId1, 
	SalesDivisionDimId AS SalesDivisionDimId1, 
	InsuranceReferenceCoverageDimId AS InsuranceReferenceCoverageDimId1, 
	CoverageDetailDimId AS CoverageDetailDimId1, 
	o_PremiumForLevel AS o_PremiumForLevel11, 
	o_CommissionRateLevel AS o_CommissionRateLevel11, 
	o_CommissionAmountLevel AS o_CommissionAmountLevel11, 
	o_PremiumMasterBookedDateID AS o_PremiumMasterBookedDateID2, 
	PolicyAuditDimId AS PolicyAuditDimId1, 
	PremiumTransactionEnteredDateId AS PremiumTransactionEnteredDateId1, 
	pol_key AS pol_key1, 
	DeclaredEventFlag AS DeclaredEventFlag1
	FROM RTRTRANS_CommissionLevel1
	WHERE o_PremiumForLevel11<>0
),
SEQ_EDWPremiumMasterCalculationPKID AS (
	CREATE SEQUENCE SEQ_EDWPremiumMasterCalculationPKID
	START = 0
	INCREMENT = 1;
),
EXP_Level1 AS (
	SELECT
	AnnualStatementLineDimID1,
	AnnualStatementLineProductCodeDimID1,
	AgencyDimID1,
	PolicyDimID1,
	ContractCustomerDimID1,
	RiskLocationDimID1,
	ReinsuranceCoverageDimID1,
	PremiumTransactionTypeDimID1,
	SEQ_EDWPremiumMasterCalculationPKID.NEXTVAL AS EDWPremiumMasterCalculationPKID1,
	-EDWPremiumMasterCalculationPKID1 AS o_EDWPremiumMasterCalculationPKID1,
	EDWPremiumTransactionPKID1,
	StagePremiumMasterPKID1,
	PremiumMasterPolicyEffectiveDateID1,
	PremiumMasterPolicyExpirationDateID1,
	PremiumMasterCoverageEffectiveDateID1,
	PremiumMasterCoverageExpirationDateID1,
	PremiumMasterRunDateID1,
	InsuranceReferenceDimId1,
	SalesDivisionDimId1,
	InsuranceReferenceCoverageDimId1,
	CoverageDetailDimId1,
	o_PremiumForLevel11,
	o_CommissionRateLevel11,
	o_CommissionAmountLevel11,
	o_PremiumMasterBookedDateID2,
	-2 AS o_AuditID1,
	PolicyAuditDimId1,
	PremiumTransactionEnteredDateId1,
	pol_key1,
	DeclaredEventFlag1
	FROM FIL_Level1
),
FIL_Level2 AS (
	SELECT
	AnnualStatementLineDimID AS AnnualStatementLineDimID3, 
	AnnualStatementLineProductCodeDimID AS AnnualStatementLineProductCodeDimID3, 
	AgencyDimID AS AgencyDimID3, 
	PolicyDimID AS PolicyDimID3, 
	ContractCustomerDimID AS ContractCustomerDimID3, 
	RiskLocationDimID AS RiskLocationDimID3, 
	ReinsuranceCoverageDimID AS ReinsuranceCoverageDimID3, 
	PremiumTransactionTypeDimID AS PremiumTransactionTypeDimID3, 
	EDWPremiumMasterCalculationPKID AS EDWPremiumMasterCalculationPKID3, 
	EDWPremiumTransactionPKID AS EDWPremiumTransactionPKID3, 
	StagePremiumMasterPKID AS StagePremiumMasterPKID3, 
	PremiumMasterPolicyEffectiveDateID AS PremiumMasterPolicyEffectiveDateID3, 
	PremiumMasterPolicyExpirationDateID AS PremiumMasterPolicyExpirationDateID3, 
	PremiumMasterCoverageEffectiveDateID AS PremiumMasterCoverageEffectiveDateID3, 
	PremiumMasterCoverageExpirationDateID AS PremiumMasterCoverageExpirationDateID3, 
	PremiumMasterRunDateID AS PremiumMasterRunDateID3, 
	InsuranceReferenceDimId AS InsuranceReferenceDimId3, 
	SalesDivisionDimId AS SalesDivisionDimId3, 
	InsuranceReferenceCoverageDimId AS InsuranceReferenceCoverageDimId3, 
	CoverageDetailDimId AS CoverageDetailDimId3, 
	o_PremiumForLevel2 AS o_PremiumForLevel23, 
	o_CommissionRateLevel2 AS o_CommissionRateLevel23, 
	o_CommissionAmountLevel2 AS o_CommissionAmountLevel23, 
	o_PremiumMasterBookedDateID AS o_PremiumMasterBookedDateID2, 
	PolicyAuditDimId AS PolicyAuditDimId3, 
	PremiumTransactionEnteredDateId AS PremiumTransactionEnteredDateId3, 
	pol_key AS pol_key3, 
	DeclaredEventFlag AS DeclaredEventFlag3
	FROM RTRTRANS_CommissionLevel2
	WHERE o_PremiumForLevel23  <> 0
),
EXP_Level2 AS (
	SELECT
	AnnualStatementLineDimID3,
	AnnualStatementLineProductCodeDimID3,
	AgencyDimID3,
	PolicyDimID3,
	ContractCustomerDimID3,
	RiskLocationDimID3,
	ReinsuranceCoverageDimID3,
	PremiumTransactionTypeDimID3,
	SEQ_EDWPremiumMasterCalculationPKID.NEXTVAL AS EDWPremiumMasterCalculationPKID3,
	-EDWPremiumMasterCalculationPKID3 AS o_EDWPremiumMasterCalculationPKID3,
	EDWPremiumTransactionPKID3,
	StagePremiumMasterPKID3,
	PremiumMasterPolicyEffectiveDateID3,
	PremiumMasterPolicyExpirationDateID3,
	PremiumMasterCoverageEffectiveDateID3,
	PremiumMasterCoverageExpirationDateID3,
	PremiumMasterRunDateID3,
	InsuranceReferenceDimId3,
	SalesDivisionDimId3,
	InsuranceReferenceCoverageDimId3,
	CoverageDetailDimId3,
	o_PremiumForLevel23,
	o_CommissionRateLevel23,
	o_CommissionAmountLevel23,
	o_PremiumMasterBookedDateID2,
	-3 AS o_AuditID2,
	PolicyAuditDimId3,
	PremiumTransactionEnteredDateId3,
	pol_key3,
	DeclaredEventFlag3
	FROM FIL_Level2
),
Union_Level1_Level2 AS (
	SELECT AnnualStatementLineDimID1, AnnualStatementLineProductCodeDimID1, AgencyDimID1, PolicyDimID1, ContractCustomerDimID1, RiskLocationDimID1, ReinsuranceCoverageDimID1, PremiumTransactionTypeDimID1, o_EDWPremiumMasterCalculationPKID1 AS EDWPremiumMasterCalculationPKID1, EDWPremiumTransactionPKID1, StagePremiumMasterPKID1, PremiumMasterPolicyEffectiveDateID1, PremiumMasterPolicyExpirationDateID1, PremiumMasterCoverageEffectiveDateID1, PremiumMasterCoverageExpirationDateID1, PremiumMasterRunDateID1, InsuranceReferenceDimId1, SalesDivisionDimId1, InsuranceReferenceCoverageDimId1, CoverageDetailDimId1, o_PremiumForLevel11, o_CommissionRateLevel11, o_CommissionAmountLevel11, o_PremiumMasterBookedDateID2 AS o_PremiumMasterBookedDateID, o_AuditID1 AS o_AuditID, PolicyAuditDimId1, PremiumTransactionEnteredDateId1, pol_key1, DeclaredEventFlag1 AS DeclaredEventFlag
	FROM EXP_Level1
	UNION
	SELECT AnnualStatementLineDimID3 AS AnnualStatementLineDimID1, AnnualStatementLineProductCodeDimID3 AS AnnualStatementLineProductCodeDimID1, AgencyDimID3 AS AgencyDimID1, PolicyDimID3 AS PolicyDimID1, ContractCustomerDimID3 AS ContractCustomerDimID1, RiskLocationDimID3 AS RiskLocationDimID1, ReinsuranceCoverageDimID3 AS ReinsuranceCoverageDimID1, PremiumTransactionTypeDimID3 AS PremiumTransactionTypeDimID1, o_EDWPremiumMasterCalculationPKID3 AS EDWPremiumMasterCalculationPKID1, EDWPremiumTransactionPKID3 AS EDWPremiumTransactionPKID1, StagePremiumMasterPKID3 AS StagePremiumMasterPKID1, PremiumMasterPolicyEffectiveDateID3 AS PremiumMasterPolicyEffectiveDateID1, PremiumMasterPolicyExpirationDateID3 AS PremiumMasterPolicyExpirationDateID1, PremiumMasterCoverageEffectiveDateID3 AS PremiumMasterCoverageEffectiveDateID1, PremiumMasterCoverageExpirationDateID3 AS PremiumMasterCoverageExpirationDateID1, PremiumMasterRunDateID3 AS PremiumMasterRunDateID1, InsuranceReferenceDimId3 AS InsuranceReferenceDimId1, SalesDivisionDimId3 AS SalesDivisionDimId1, InsuranceReferenceCoverageDimId3 AS InsuranceReferenceCoverageDimId1, CoverageDetailDimId3 AS CoverageDetailDimId1, o_PremiumForLevel23 AS o_PremiumForLevel11, o_CommissionRateLevel23 AS o_CommissionRateLevel11, o_CommissionAmountLevel23 AS o_CommissionAmountLevel11, o_PremiumMasterBookedDateID2 AS o_PremiumMasterBookedDateID, o_AuditID2 AS o_AuditID, PolicyAuditDimId3 AS PolicyAuditDimId1, PremiumTransactionEnteredDateId3 AS PremiumTransactionEnteredDateId1, pol_key3 AS pol_key1, DeclaredEventFlag3 AS DeclaredEventFlag
	FROM EXP_Level2
),
EXP_format AS (
	SELECT
	AnnualStatementLineDimID1 AS AnnualStatementLineDimID,
	AnnualStatementLineProductCodeDimID1 AS AnnualStatementLineProductCodeDimID,
	AgencyDimID1 AS AgencyDimID,
	PolicyDimID1 AS PolicyDimID,
	ContractCustomerDimID1 AS ContractCustomerDimID,
	RiskLocationDimID1 AS RiskLocationDimID,
	ReinsuranceCoverageDimID1 AS ReinsuranceCoverageDimID,
	PremiumTransactionTypeDimID1 AS PremiumTransactionTypeDimID,
	EDWPremiumMasterCalculationPKID1 AS EDWPremiumMasterCalculationPKID,
	EDWPremiumTransactionPKID1 AS EDWPremiumTransactionPKID,
	StagePremiumMasterPKID1 AS StagePremiumMasterPKID,
	PremiumMasterPolicyEffectiveDateID1 AS PremiumMasterPolicyEffectiveDateID,
	PremiumMasterPolicyExpirationDateID1 AS PremiumMasterPolicyExpirationDateID,
	PremiumMasterCoverageEffectiveDateID1 AS PremiumMasterCoverageEffectiveDateID,
	PremiumMasterCoverageExpirationDateID1 AS PremiumMasterCoverageExpirationDateID,
	PremiumMasterRunDateID1 AS PremiumMasterRunDateID,
	InsuranceReferenceDimId1 AS InsuranceReferenceDimId,
	SalesDivisionDimId1 AS SalesDivisionDimId,
	InsuranceReferenceCoverageDimId1 AS InsuranceReferenceCoverageDimId,
	CoverageDetailDimId1 AS CoverageDetailDimId,
	0 AS PremiumMasterPremium,
	o_CommissionRateLevel11 AS o_CommissionRateLevel1,
	o_CommissionAmountLevel11 AS o_CommissionAmountLevel1,
	o_PremiumMasterBookedDateID AS o_PremiumMasterRunDateID,
	o_AuditID,
	0 AS PremiumMasterFullTermPremium,
	0 AS PremiumMasterDirectWrittenPremium,
	0 AS PremiumMasterCededWrittenPremium,
	0 AS PremiumMasterNetWrittenPremium,
	0 AS PremiumMasterAgencyCededWrittenCommission,
	o_CommissionAmountLevel1 AS PremiumMasterAgencyNetWrittenCommission,
	0 AS PremiumMasterAuditPremium,
	0 AS PremiumMasterReturnedPremium,
	0 AS PremiumMasterCollectionWriteOffPremium,
	0 AS ExposureAmount,
	PolicyAuditDimId1 AS PolicyAuditDimId,
	PremiumTransactionEnteredDateId1 AS PremiumTransactionEnteredDateId,
	0 AS CreditDirectWrittenPremium,
	0 AS DebitDirectWrittenPremium,
	pol_key1,
	DeclaredEventFlag,
	-- *INF*: DECODE(DeclaredEventFlag, 'T', 1, 'F', 0,Null)
	DECODE(DeclaredEventFlag,
		'T', 1,
		'F', 0,
		Null
	) AS o_DeclaredEventFlag
	FROM Union_Level1_Level2
),
LKP_PremiumMasterFact AS (
	SELECT
	PremiumMasterFactID,
	PremiumMasterAgencyDirectWrittenCommission,
	PremiumMasterAgencyCommissionRate,
	IN_PolicyKey,
	IN_PremiumMasterRunDateID,
	IN_AuditID,
	PolicyKey,
	AuditID,
	PremiumMasterRunDateID
	FROM (
		select 
		p.pol_key as PolicyKey,
		f.PremiumMasterRunDateID as PremiumMasterRunDateID,
		f.AuditId as AuditId,
		f.PremiumMasterFactID as PremiumMasterFactID,
		f.PremiumMasterAgencyDirectWrittenCommission as PremiumMasterAgencyDirectWrittenCommission,
		f.PremiumMasterAgencyCommissionRate as PremiumMasterAgencyCommissionRate
		from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterFact f
		join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim p
		on f.PolicyDimId=p.pol_dim_id
		where f.auditId in (-2,-3)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,AuditID,PremiumMasterRunDateID ORDER BY PremiumMasterFactID) = 1
),
RTRTRANS1 AS (
	SELECT
	EXP_format.AnnualStatementLineDimID,
	EXP_format.AnnualStatementLineProductCodeDimID,
	EXP_format.AgencyDimID,
	EXP_format.PolicyDimID,
	EXP_format.ContractCustomerDimID,
	EXP_format.RiskLocationDimID,
	EXP_format.ReinsuranceCoverageDimID,
	EXP_format.PremiumTransactionTypeDimID,
	EXP_format.EDWPremiumMasterCalculationPKID,
	EXP_format.EDWPremiumTransactionPKID,
	EXP_format.StagePremiumMasterPKID,
	EXP_format.PremiumMasterPolicyEffectiveDateID,
	EXP_format.PremiumMasterPolicyExpirationDateID,
	EXP_format.PremiumMasterCoverageEffectiveDateID,
	EXP_format.PremiumMasterCoverageExpirationDateID,
	EXP_format.InsuranceReferenceDimId,
	EXP_format.SalesDivisionDimId,
	EXP_format.InsuranceReferenceCoverageDimId,
	EXP_format.CoverageDetailDimId,
	EXP_format.PremiumMasterPremium,
	EXP_format.o_CommissionRateLevel1,
	EXP_format.o_CommissionAmountLevel1,
	EXP_format.o_PremiumMasterRunDateID,
	EXP_format.o_AuditID,
	EXP_format.PremiumMasterFullTermPremium,
	EXP_format.PremiumMasterDirectWrittenPremium,
	EXP_format.PremiumMasterCededWrittenPremium,
	EXP_format.PremiumMasterNetWrittenPremium,
	EXP_format.PremiumMasterAgencyCededWrittenCommission,
	EXP_format.PremiumMasterAgencyNetWrittenCommission,
	EXP_format.PremiumMasterAuditPremium,
	EXP_format.PremiumMasterReturnedPremium,
	EXP_format.PremiumMasterCollectionWriteOffPremium,
	EXP_format.ExposureAmount,
	EXP_format.PolicyAuditDimId,
	EXP_format.PremiumTransactionEnteredDateId,
	EXP_format.CreditDirectWrittenPremium,
	EXP_format.DebitDirectWrittenPremium,
	EXP_format.ExposureAmount AS WrittenExposure,
	LKP_PremiumMasterFact.PremiumMasterFactID AS LKP_PremiumMasterFactID,
	LKP_PremiumMasterFact.PremiumMasterAgencyDirectWrittenCommission AS LKP_PremiumMasterAgencyDirectWrittenCommission,
	LKP_PremiumMasterFact.PremiumMasterAgencyCommissionRate AS LKP_PremiumMasterAgencyCommissionRate,
	EXP_format.o_DeclaredEventFlag AS DeclaredEventFlag
	FROM EXP_format
	LEFT JOIN LKP_PremiumMasterFact
	ON LKP_PremiumMasterFact.PolicyKey = EXP_format.pol_key1 AND LKP_PremiumMasterFact.AuditID = EXP_format.o_AuditID AND LKP_PremiumMasterFact.PremiumMasterRunDateID = EXP_format.o_PremiumMasterRunDateID
),
RTRTRANS1_INSERT AS (SELECT * FROM RTRTRANS1 WHERE ISNULL(LKP_PremiumMasterFactID)),
RTRTRANS1_DEFAULT1 AS (SELECT * FROM RTRTRANS1 WHERE NOT ( (ISNULL(LKP_PremiumMasterFactID)) )),
TGT_PremiumMasterFact_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumMasterFact
	(AuditID, AnnualStatementLineDimID, AnnualStatementLineProductCodeDimID, AgencyDimID, PolicyDimID, ContractCustomerDimID, RiskLocationDimID, ReinsuranceCoverageDimID, PremiumTransactionTypeDimID, EDWPremiumMasterCalculationPKID, EDWPremiumTransactionPKID, StagePremiumMasterPKID, PremiumMasterPolicyEffectiveDateID, PremiumMasterPolicyExpirationDateID, PremiumMasterCoverageEffectiveDateID, PremiumMasterCoverageExpirationDateID, PremiumMasterRunDateID, PremiumMasterPremium, PremiumMasterFullTermPremium, PremiumMasterDirectWrittenPremium, PremiumMasterCededWrittenPremium, PremiumMasterNetWrittenPremium, PremiumMasterAgencyCommissionRate, PremiumMasterAgencyDirectWrittenCommission, PremiumMasterAgencyCededWrittenCommission, PremiumMasterAgencyNetWrittenCommission, PremiumMasterAuditPremium, PremiumMasterReturnedPremium, PremiumMasterCollectionWriteOffPremium, InsuranceReferenceDimId, SalesDivisionDimId, InsuranceReferenceCoverageDimId, CoverageDetailDimId, ExposureAmount, PolicyAuditDimId, PremiumTransactionEnteredDateId, CreditDirectWrittenPremium, DebitDirectWrittenPremium, WrittenExposure, DeclaredEventFlag)
	SELECT 
	o_AuditID AS AUDITID, 
	ANNUALSTATEMENTLINEDIMID, 
	ANNUALSTATEMENTLINEPRODUCTCODEDIMID, 
	AGENCYDIMID, 
	POLICYDIMID, 
	CONTRACTCUSTOMERDIMID, 
	RISKLOCATIONDIMID, 
	REINSURANCECOVERAGEDIMID, 
	PREMIUMTRANSACTIONTYPEDIMID, 
	EDWPREMIUMMASTERCALCULATIONPKID, 
	EDWPREMIUMTRANSACTIONPKID, 
	STAGEPREMIUMMASTERPKID, 
	PREMIUMMASTERPOLICYEFFECTIVEDATEID, 
	PREMIUMMASTERPOLICYEXPIRATIONDATEID, 
	PREMIUMMASTERCOVERAGEEFFECTIVEDATEID, 
	PREMIUMMASTERCOVERAGEEXPIRATIONDATEID, 
	o_PremiumMasterRunDateID AS PREMIUMMASTERRUNDATEID, 
	PREMIUMMASTERPREMIUM, 
	PREMIUMMASTERFULLTERMPREMIUM, 
	PREMIUMMASTERDIRECTWRITTENPREMIUM, 
	PREMIUMMASTERCEDEDWRITTENPREMIUM, 
	PREMIUMMASTERNETWRITTENPREMIUM, 
	o_CommissionRateLevel AS PREMIUMMASTERAGENCYCOMMISSIONRATE, 
	o_CommissionAmountLevel AS PREMIUMMASTERAGENCYDIRECTWRITTENCOMMISSION, 
	PREMIUMMASTERAGENCYCEDEDWRITTENCOMMISSION, 
	PREMIUMMASTERAGENCYNETWRITTENCOMMISSION, 
	PREMIUMMASTERAUDITPREMIUM, 
	PREMIUMMASTERRETURNEDPREMIUM, 
	PREMIUMMASTERCOLLECTIONWRITEOFFPREMIUM, 
	INSURANCEREFERENCEDIMID, 
	SALESDIVISIONDIMID, 
	INSURANCEREFERENCECOVERAGEDIMID, 
	COVERAGEDETAILDIMID, 
	EXPOSUREAMOUNT, 
	POLICYAUDITDIMID, 
	PREMIUMTRANSACTIONENTEREDDATEID, 
	CREDITDIRECTWRITTENPREMIUM, 
	DEBITDIRECTWRITTENPREMIUM, 
	WRITTENEXPOSURE, 
	DECLAREDEVENTFLAG
	FROM RTRTRANS1_INSERT
),
UPDTRANS AS (
	SELECT
	LKP_PremiumMasterFactID AS LKP_PremiumMasterFactID2, 
	o_CommissionRateLevel1 AS o_CommissionRateLevel12, 
	o_CommissionAmountLevel1 AS o_CommissionAmountLevel12, 
	PremiumMasterAgencyNetWrittenCommission AS PremiumMasterAgencyNetWrittenCommission2
	FROM RTRTRANS1_DEFAULT1
),
TGT_PremiumMasterFact_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumMasterFact AS T
	USING UPDTRANS AS S
	ON T.PremiumMasterFactID = S.LKP_PremiumMasterFactID2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.PremiumMasterAgencyCommissionRate = S.o_CommissionRateLevel12, T.PremiumMasterAgencyDirectWrittenCommission = S.o_CommissionAmountLevel12, T.PremiumMasterAgencyNetWrittenCommission = S.PremiumMasterAgencyNetWrittenCommission2
),
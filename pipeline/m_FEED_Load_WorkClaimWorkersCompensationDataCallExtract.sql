WITH
SQ_vwLossMasterFact_OpenCount AS (
	SELECT
	  CoverageDetailDim.RatingStateProvinceCode,
	  InsuranceReferenceDim.StrategicProfitCenterAbbreviation,
	  InsuranceReferenceDim.InsuranceSegmentDescription,
	  InsuranceReferenceDim.PolicyOfferingDescription,
	  policy_dim.pol_eff_date,
	  Clm_Loss_Date_dim.clndr_yr Clm_clndr_yr,
	  dbo.policy_dim.pol_key,
	  Clm_Loss_Date_dim.clndr_date Clm_clndr_date,
	loss_master_fact.outstanding_amt,
	claim_occurrence_dim.claim_occurrence_key
	FROM
	  @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_fact ON (policy_dim.pol_dim_id=loss_master_fact.pol_dim_id)
	   INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_dim ON (claim_occurrence_dim.claim_occurrence_dim_id=loss_master_fact.claim_occurrence_dim_id)
	   INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction_type_dim ON (claim_transaction_type_dim.claim_trans_type_dim_id=loss_master_fact.claim_trans_type_dim_id)
	   INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_dim ON (claimant_coverage_dim.claimant_cov_dim_id=loss_master_fact.claimant_cov_dim_id)
	   INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim  Pol_Eff_Date_dim ON (loss_master_fact.pol_eff_date_id=Pol_Eff_Date_dim.clndr_id)
	   INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim  Clm_Loss_Date_dim ON (loss_master_fact.claim_loss_date_id=Clm_Loss_Date_dim.clndr_id)
	   INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim  Monthly_Run_Date_dim ON (loss_master_fact.loss_master_run_date_id=Monthly_Run_Date_dim.clndr_id)
	   INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim ON (InsuranceReferenceDim.InsuranceReferenceDimId=loss_master_fact.InsuranceReferenceDimId)
	   INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailDim ON (CoverageDetailDim.CoverageDetailDimId=loss_master_fact.CoverageDetailDimId)
	  
	WHERE
	  ( 
	  claim_transaction_type_dim.trans_kind_code  =  'D'
	  AND  Monthly_Run_Date_dim.clndr_yr  = ( YEAR(GETDATE())-@{pipeline().parameters.NOOFYEAR})  AND Monthly_Run_Date_dim.clndr_month = @{pipeline().parameters.MONTH}
	  AND  InsuranceReferenceDim.PolicyOfferingDescription  =  'Workers Compensation'
	  AND  claimant_coverage_dim.cause_of_loss  IN  ('05', '75') 
	AND claim_transaction_type_dim.pms_trans_code not in ('97','98','99') 
	)
),
EXPTRANS AS (
	SELECT
	RatingStateProvinceCode,
	StrategicProfitCenterAbbreviation,
	InsuranceSegmentDescription,
	PolicyOfferingDescription,
	pol_eff_date,
	Clm_clndr_yr,
	pol_key,
	Clm_clndr_date,
	outstanding_amt,
	claim_occurrence_key,
	0 AS O_Paid_To_Date
	FROM SQ_vwLossMasterFact_OpenCount
),
SQ_vwLossMasterFact AS (
	SELECT
	CoverageDetailDim.RatingStateProvinceCode, 
	--loss_master_dim.risk_state_prov_code, 
	policy_dim.pol_key,
	policy_dim.pol_eff_date, 
	claim_occurrence_dim.claim_loss_date,
	claim_occurrence_dim.claim_occurrence_key,
	InsuranceReferenceDim.StrategicProfitCenterAbbreviation, 
	InsuranceReferenceDim.InsuranceSegmentDescription, 
	InsuranceReferenceDim.PolicyOfferingDescription, 
	vwLossMasterFact.ChangeInOutstandingAmount, 
	vwLossMasterFact.DirectLossPaidER, 
	vwLossMasterFact.DirectALAEPaidER, 
	calendar_dim.clndr_date, 
	claim_transaction_type_dim.pms_trans_code, 
	CoverageDetailDim.RatingStateProvinceCode, 
	vwLossMasterFact.claim_loss_date_id 
	FROM 
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_dim, 
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim, 
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_dim, 
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim, 
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction_type_dim, 
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.vwLossMasterFact, policy_dim, 
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailDim, loss_master_dim , 
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim as calendar_dim1
	WHERE vwLossMasterFact.pol_dim_id = policy_dim.pol_dim_id
	AND vwLossMasterFact.claim_occurrence_dim_id = claim_occurrence_dim.claim_occurrence_dim_id
	AND vwLossMasterFact.claimant_cov_dim_id = claimant_coverage_dim.claimant_cov_dim_id
	AND vwLossMasterFact.claim_loss_date_id = calendar_dim.clndr_id
	AND vwLossMasterFact.InsuranceReferenceDimId = InsuranceReferenceDim.InsuranceReferenceDimId
	AND vwLossMasterFact.loss_master_dim_id = loss_master_dim.loss_master_dim_id
	AND vwLossMasterFact.claim_trans_type_dim_id = claim_transaction_type_dim.claim_trans_type_dim_id
	AND vwLossMasterFact.CoverageDetailDimId = CoverageDetailDim.CoverageDetailDimId 
	AND vwLossMasterFact.loss_master_run_date_id = calendar_dim1.clndr_id
	AND InsuranceReferenceDim.PolicyOfferingDescription =  'Workers Compensation'
	AND claimant_coverage_dim.cause_of_loss in ('05', '75')
	AND ( (calendar_dim1.CalendarYear <= CONVERT(CHAR(4), GETDATE(), 120)-1 AND @{pipeline().parameters.RUN_YEAR}= 0)
	   OR (calendar_dim1.CalendarYear <= @{pipeline().parameters.RUN_YEAR} AND @{pipeline().parameters.RUN_YEAR} != 0)
	           )
	AND claim_transaction_type_dim.pms_trans_code not in ('97','98','99')
	AND claim_transaction_type_dim.trans_kind_code  =  'D'
),
EXP_STAGE AS (
	SELECT
	pol_key,
	pol_eff_date AS i_pol_eff_date,
	claim_loss_date AS i_claim_loss_date,
	-- *INF*: TRUNC(i_claim_loss_date)
	TRUNC(i_claim_loss_date) AS o_claim_loss_date,
	claim_occurrence_key,
	StrategicProfitCenterAbbreviation,
	InsuranceSegmentDescription,
	PolicyOfferingDescription,
	ChangeInOutstandingAmount,
	DirectLossPaidER AS i_DirectLossPaidER,
	DirectALAEPaidER AS i_DirectALAEPaidER,
	risk_state_prov_code,
	i_DirectLossPaidER AS o_PaidToDate,
	0 AS o_ChangeInOutstandingAmount_Closed,
	pms_trans_code,
	RatingStateProvinceCode,
	claim_loss_date_id1,
	clndr_date
	FROM SQ_vwLossMasterFact
),
Union AS (
	SELECT RatingStateProvinceCode, StrategicProfitCenterAbbreviation, InsuranceSegmentDescription, PolicyOfferingDescription, pol_eff_date, pol_key, claim_occurrence_key, Clm_clndr_date, outstanding_amt, O_Paid_To_Date AS Paid_To_Date
	FROM EXPTRANS
	UNION
	SELECT risk_state_prov_code AS RatingStateProvinceCode, StrategicProfitCenterAbbreviation, InsuranceSegmentDescription, PolicyOfferingDescription, i_pol_eff_date AS pol_eff_date, pol_key, claim_occurrence_key, clndr_date AS Clm_clndr_date, o_ChangeInOutstandingAmount_Closed AS outstanding_amt, o_PaidToDate AS Paid_To_Date
	FROM EXP_STAGE
),
AGG_Claim_Occurrence_Year AS (
	SELECT
	RatingStateProvinceCode AS risk_state_prov_code,
	pol_key,
	pol_eff_date,
	Clm_clndr_date AS claim_loss_date,
	-- *INF*: TRUNC(claim_loss_date,'YYYY')
	CAST(TRUNC(claim_loss_date, 'YEAR') AS TIMESTAMP_NTZ(0)) AS claim_loss_year,
	claim_occurrence_key,
	StrategicProfitCenterAbbreviation,
	InsuranceSegmentDescription,
	PolicyOfferingDescription,
	outstanding_amt AS i_ChangeInOutstandingAmount,
	Paid_To_Date AS i_PaidToDate,
	-- *INF*: SUM(i_ChangeInOutstandingAmount)
	SUM(i_ChangeInOutstandingAmount) AS o_ChangeInOutstandingAmount,
	-- *INF*: SUM(i_PaidToDate)
	SUM(i_PaidToDate) AS o_PaidToDate,
	-- *INF*: LAST(claim_loss_date)
	LAST(claim_loss_date) AS o_claim_loss_date
	FROM Union
	GROUP BY risk_state_prov_code, pol_key, pol_eff_date, claim_loss_year, claim_occurrence_key, StrategicProfitCenterAbbreviation, InsuranceSegmentDescription, PolicyOfferingDescription
),
EXP_Metadata AS (
	SELECT
	pol_key,
	pol_eff_date,
	o_claim_loss_date AS claim_loss_date,
	claim_occurrence_key,
	StrategicProfitCenterAbbreviation,
	InsuranceSegmentDescription,
	PolicyOfferingDescription,
	risk_state_prov_code,
	o_ChangeInOutstandingAmount AS TotalChangeInOutstandingAmount,
	o_PaidToDate AS TotalPaidToDate,
	-- *INF*: IIF(TotalChangeInOutstandingAmount>0, 1, 0)
	IFF(TotalChangeInOutstandingAmount > 0, 1, 0) AS o_OpenClaimCount,
	-- *INF*: IIF(TotalChangeInOutstandingAmount <= 0 AND TotalPaidToDate>0, 1, 0)
	IFF(TotalChangeInOutstandingAmount <= 0 AND TotalPaidToDate > 0, 1, 0) AS o_ClosedWithPayClaimCount,
	-- *INF*: IIF(@{pipeline().parameters.RUN_YEAR} = 0, 
	--        TO_DATE('12/31/'  || TO_CHAR(GET_DATE_PART(SYSDATE, 'YYYY') -1), 'MM/DD/YYYY'), 
	--        TO_DATE('12/31/'  || TO_CHAR(@{pipeline().parameters.RUN_YEAR}), 'MM/DD/YYYY')
	--       )
	IFF(
	    @{pipeline().parameters.RUN_YEAR} = 0,
	    TO_TIMESTAMP('12/31/' || TO_CHAR(DATE_PART(CURRENT_TIMESTAMP, 'YYYY') - 1), 'MM/DD/YYYY'),
	    TO_TIMESTAMP('12/31/' || TO_CHAR(@{pipeline().parameters.RUN_YEAR}), 'MM/DD/YYYY')
	) AS o_RunDateYear,
	sysdate AS o_CreatedDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	claim_loss_year
	FROM AGG_Claim_Occurrence_Year
),
LKP_WorkClaimWorkersCompensationDataCallExtract AS (
	SELECT
	WorkClaimWorkersCompensationDataCallExtractId,
	PolicyKey,
	ClaimOccurrenceKey,
	StateCode,
	ClaimLossDate,
	RunDate
	FROM (
		SELECT 
			WorkClaimWorkersCompensationDataCallExtractId,
			PolicyKey,
			ClaimOccurrenceKey,
			StateCode,
			ClaimLossDate,
			RunDate
		FROM WorkClaimWorkersCompensationDataCallExtract
		WHERE RunDate = CASE WHEN @{pipeline().parameters.RUN_YEAR} =0  then convert(datetime,'12/31/' + cast(convert(char(4), getdate(), 120)-1 as char(4)) ,101)
		                                   ELSE convert(datetime,'12/31/' + cast(@{pipeline().parameters.RUN_YEAR} as char(4)),101)
		                        END
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,ClaimOccurrenceKey,StateCode,ClaimLossDate,RunDate ORDER BY WorkClaimWorkersCompensationDataCallExtractId) = 1
),
RTRTRANS AS (
	SELECT
	LKP_WorkClaimWorkersCompensationDataCallExtract.WorkClaimWorkersCompensationDataCallExtractId,
	EXP_Metadata.pol_key,
	EXP_Metadata.pol_eff_date,
	EXP_Metadata.claim_loss_date,
	EXP_Metadata.claim_occurrence_key,
	EXP_Metadata.StrategicProfitCenterAbbreviation,
	EXP_Metadata.InsuranceSegmentDescription,
	EXP_Metadata.PolicyOfferingDescription,
	EXP_Metadata.risk_state_prov_code,
	EXP_Metadata.TotalChangeInOutstandingAmount,
	EXP_Metadata.TotalPaidToDate,
	EXP_Metadata.o_OpenClaimCount AS OpenClaimCount,
	EXP_Metadata.o_ClosedWithPayClaimCount AS ClosedWithPayClaimCount,
	EXP_Metadata.o_RunDateYear AS RunDateYear,
	EXP_Metadata.o_CreatedDate AS CreatedDate,
	EXP_Metadata.o_AuditId AS AuditId
	FROM EXP_Metadata
	LEFT JOIN LKP_WorkClaimWorkersCompensationDataCallExtract
	ON LKP_WorkClaimWorkersCompensationDataCallExtract.PolicyKey = EXP_Metadata.pol_key AND LKP_WorkClaimWorkersCompensationDataCallExtract.ClaimOccurrenceKey = EXP_Metadata.claim_occurrence_key AND LKP_WorkClaimWorkersCompensationDataCallExtract.StateCode = EXP_Metadata.risk_state_prov_code AND LKP_WorkClaimWorkersCompensationDataCallExtract.ClaimLossDate = EXP_Metadata.claim_loss_date AND LKP_WorkClaimWorkersCompensationDataCallExtract.RunDate = EXP_Metadata.o_RunDateYear
),
RTRTRANS_INSERT AS (SELECT * FROM RTRTRANS WHERE ISNULL(WorkClaimWorkersCompensationDataCallExtractId)),
RTRTRANS_UPDATE AS (SELECT * FROM RTRTRANS WHERE NOT ISNULL(WorkClaimWorkersCompensationDataCallExtractId)),
UPD_WorkClaimWorkersCompensationDataCallExtract_UPDATE AS (
	SELECT
	WorkClaimWorkersCompensationDataCallExtractId AS WorkClaimWorkersCompensationDataCallExtractId3, 
	pol_key AS pol_key3, 
	pol_eff_date AS pol_eff_date3, 
	claim_loss_date AS claim_loss_date3, 
	claim_occurrence_key AS claim_occurrence_key3, 
	StrategicProfitCenterAbbreviation AS StrategicProfitCenterAbbreviation3, 
	InsuranceSegmentDescription AS InsuranceSegmentDescription3, 
	PolicyOfferingDescription AS PolicyOfferingDescription3, 
	risk_state_prov_code AS risk_state_prov_code3, 
	TotalChangeInOutstandingAmount AS TotalChangeInOutstandingAmount3, 
	TotalPaidToDate AS TotalPaidToDate3, 
	OpenClaimCount AS OpenClaimCount3, 
	ClosedWithPayClaimCount AS ClosedWithPayClaimCount3, 
	RunDateYear AS RunDateYear3, 
	CreatedDate AS CreatedDate3, 
	AuditId AS AuditId3
	FROM RTRTRANS_UPDATE
),
WorkClaimWorkersCompensationDataCallExtract_UPDATE AS (
	MERGE INTO WorkClaimWorkersCompensationDataCallExtract AS T
	USING UPD_WorkClaimWorkersCompensationDataCallExtract_UPDATE AS S
	ON T.WorkClaimWorkersCompensationDataCallExtractId = S.WorkClaimWorkersCompensationDataCallExtractId3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AuditId3, T.CreatedDate = S.CreatedDate3, T.RunDate = S.RunDateYear3, T.StrategicProfitCenterAbbreviation = S.StrategicProfitCenterAbbreviation3, T.InsuranceSegmentDescription = S.InsuranceSegmentDescription3, T.PolicyOfferingDescription = S.PolicyOfferingDescription3, T.PolicyKey = S.pol_key3, T.PolicyEffectiveDate = S.pol_eff_date3, T.ClaimOccurrenceKey = S.claim_occurrence_key3, T.StateCode = S.risk_state_prov_code3, T.ClaimLossDate = S.claim_loss_date3, T.IndemnityOpenClaimCount = S.OpenClaimCount3, T.IndemnityClosedWithPayClaimCount = S.ClosedWithPayClaimCount3, T.DirectLossPaidToDate = S.TotalPaidToDate3, T.OutstandingAmountToDate = S.TotalChangeInOutstandingAmount3
),
UPD_WorkClaimWorkersCompensationDataCallExtract_INSERT AS (
	SELECT
	WorkClaimWorkersCompensationDataCallExtractId AS WorkClaimWorkersCompensationDataCallExtractId1, 
	pol_key AS pol_key1, 
	pol_eff_date AS pol_eff_date1, 
	claim_loss_date AS claim_loss_date1, 
	claim_occurrence_key AS claim_occurrence_key1, 
	StrategicProfitCenterAbbreviation AS StrategicProfitCenterAbbreviation1, 
	InsuranceSegmentDescription AS InsuranceSegmentDescription1, 
	PolicyOfferingDescription AS PolicyOfferingDescription1, 
	risk_state_prov_code AS risk_state_prov_code1, 
	TotalChangeInOutstandingAmount AS TotalChangeInOutstandingAmount1, 
	TotalPaidToDate AS TotalPaidToDate1, 
	OpenClaimCount AS OpenClaimCount1, 
	ClosedWithPayClaimCount AS ClosedWithPayClaimCount1, 
	RunDateYear AS RunDateYear1, 
	CreatedDate AS CreatedDate1, 
	AuditId AS AuditId1
	FROM RTRTRANS_INSERT
),
WorkClaimWorkersCompensationDataCallExtract_INSERT AS (
	TRUNCATE TABLE WorkClaimWorkersCompensationDataCallExtract;
	INSERT INTO WorkClaimWorkersCompensationDataCallExtract
	(AuditId, CreatedDate, RunDate, StrategicProfitCenterAbbreviation, InsuranceSegmentDescription, PolicyOfferingDescription, PolicyKey, PolicyEffectiveDate, ClaimOccurrenceKey, StateCode, ClaimLossDate, IndemnityOpenClaimCount, IndemnityClosedWithPayClaimCount, DirectLossPaidToDate, OutstandingAmountToDate)
	SELECT 
	AuditId1 AS AUDITID, 
	CreatedDate1 AS CREATEDDATE, 
	RunDateYear1 AS RUNDATE, 
	StrategicProfitCenterAbbreviation1 AS STRATEGICPROFITCENTERABBREVIATION, 
	InsuranceSegmentDescription1 AS INSURANCESEGMENTDESCRIPTION, 
	PolicyOfferingDescription1 AS POLICYOFFERINGDESCRIPTION, 
	pol_key1 AS POLICYKEY, 
	pol_eff_date1 AS POLICYEFFECTIVEDATE, 
	claim_occurrence_key1 AS CLAIMOCCURRENCEKEY, 
	risk_state_prov_code1 AS STATECODE, 
	claim_loss_date1 AS CLAIMLOSSDATE, 
	OpenClaimCount1 AS INDEMNITYOPENCLAIMCOUNT, 
	ClosedWithPayClaimCount1 AS INDEMNITYCLOSEDWITHPAYCLAIMCOUNT, 
	TotalPaidToDate1 AS DIRECTLOSSPAIDTODATE, 
	TotalChangeInOutstandingAmount1 AS OUTSTANDINGAMOUNTTODATE
	FROM UPD_WorkClaimWorkersCompensationDataCallExtract_INSERT
),
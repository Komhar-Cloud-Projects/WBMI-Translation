WITH
LKP_AuditSchedule AS (
	SELECT
	AuditStatus,
	PolicyKey,
	InsuranceLine,
	AuditEffectiveDate,
	AuditExpirationDate
	FROM (
		SELECT 
			AuditStatus,
			PolicyKey,
			InsuranceLine,
			AuditEffectiveDate,
			AuditExpirationDate
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME_EDW}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.AuditSchedule
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,InsuranceLine,AuditEffectiveDate,AuditExpirationDate ORDER BY AuditStatus) = 1
),
SQ_Loss AS (
	DECLARE @MonthStart as datetime, 
	@MonthEnd as datetime
	
	SET @MonthStart = DATEADD(s,0,DATEADD(mm, DATEDIFF(m,0,getdate()) - @{pipeline().parameters.NO_OF_MONTHS_START},0))            
	SET @MonthEnd = DATEADD(s,-1,DATEADD(mm, DATEDIFF(m,0,GETDATE()) - @{pipeline().parameters.NO_OF_MONTHS_END} +1,0))
	
	SELECT 
	A.claimant_full_name as claimant_full_name,
	A.claimant_num as claimant_num,
	A.pol_key as pol_key, 
	A.pol_eff_date as pol_eff_date, 
	A.claim_num as claim_num, 
	A.trans_close_date as trans_close_date, 
	A.claim_loss_date as claim_loss_date, 
	A.RatingStateProvinceAbbreviation as RatingStateProvinceAbbreviation,
	A.cust_num as cust_num,
	A.pol_sym as pol_sym,
	A.pol_num as pol_num,
	A.SelectionMonthStart as SelectionMonthStart,
	A.SelectionMonthEnd as SelectionMonthEnd
	FROM
	(
	SELECT DISTINCT
	d.claimant_full_name as claimant_full_name,
	d.claimant_num as claimant_num,
	 pol.pol_key as pol_key, 
	 pol.pol_eff_date as pol_eff_date, 
	 b.claim_num as claim_num, 
	 c.clndr_date as trans_close_date, 
	 b.claim_loss_date as claim_loss_date, 
	cdd.RatingStateProvinceAbbreviation as RatingStateProvinceAbbreviation,
	cust.cust_num as cust_num,
	pol.pol_sym as pol_sym,
	pol.pol_num as pol_num,
	@MonthStart as SelectionMonthStart,
	@MonthEnd as SelectionMonthEnd,
	case 
	 when cdd.RatingStateProvinceAbbreviation='WI' and datediff(month,pol.pol_eff_date, c.clndr_date) between 19 and 25 then 1
	 when cdd.RatingStateProvinceAbbreviation='WI' and datediff(month,pol.pol_eff_date, c.clndr_date) between 31 and 37 then 1
	 when cdd.RatingStateProvinceAbbreviation='WI' and datediff(month,pol.pol_eff_date, c.clndr_date) between 43 and 49 then 1
	 when  cdd.RatingStateProvinceAbbreviation in ('MN','MI','FL','MA') and datediff(month,pol.pol_eff_date, c.clndr_date) between 19 and 24 then 1
	 when  cdd.RatingStateProvinceAbbreviation in ('MN','MI','FL','MA') and datediff(month,pol.pol_eff_date, c.clndr_date) between 31 and 36 then 1
	 when  cdd.RatingStateProvinceAbbreviation in ('MN','MI','FL','MA') and datediff(month,pol.pol_eff_date, c.clndr_date) between 43 and 48 then 1
	else 0 end as ValidRecord
	FROM
	@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_loss_transaction_fact a 
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_dim b 
		on a.claim_occurrence_dim_id=b.claim_occurrence_dim_id
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim c 
		on a.claim_trans_date_id=c.clndr_id
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_dim d 
		on a.claimant_dim_id=d.claimant_dim_id
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim ir 
		on a.InsuranceReferenceDimId=ir.InsuranceReferenceDimId
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction_type_dim tt 
		on a.claim_trans_type_dim_id=tt.claim_trans_type_dim_id
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol 
		on a.pol_dim_id=pol.pol_dim_id
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.coveragedetaildim cdd
		on a.coveragedetaildimid=cdd.coveragedetaildimid
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_dim cust 
		on a.contract_cust_dim_id=cust.contract_cust_dim_id
	WHERE
	tt.trans_code_descript in (@{pipeline().parameters.TRANS_CODE})
	and c.clndr_date between @MonthStart AND @MonthEnd 
	and ir.InsuranceSegmentAbbreviation in ('CL','Pool')
	and ir.PolicyOfferingAbbreviation='WC'
	and Pol.renl_code NOT in ('7','9') and Pol.pol_cancellation_date >= '2100-12-31'
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	WHERE A.ValidRecord=1
),
EXP_Input AS (
	SELECT
	claimant_full_name,
	claimant_num,
	pol_key,
	pol_eff_date,
	claim_num,
	trans_close_date,
	claim_loss_date,
	RatingStateProvinceAbbreviation,
	cust_num,
	pol_sym,
	pol_num,
	SelectionMonthStart,
	SelectionMonthEnd
	FROM SQ_Loss
),
LKP_EstimatedAuditCodeKeys_DCT AS (
	SELECT
	PolicyKey,
	InsuranceSegmentAbbreviation,
	ReasonAmendedCode,
	StrategicProfitCenterAbbreviation,
	pol_status_code,
	PremiumTransactionEffectiveDate,
	SeqNum,
	in_polkey,
	in_SelectionMonthEnd
	FROM (
		Select 
		A.PolicyKey as PolicyKey,
		A.InsuranceSegmentAbbreviation  as InsuranceSegmentAbbreviation,
		A.ReasonAmendedCode as ReasonAmendedCode,
		A.StrategicProfitCenterAbbreviation as StrategicProfitCenterAbbreviation,
		A.pol_status_code as pol_status_code,
		A.PremiumTransactionEffectiveDate as PremiumTransactionEffectiveDate,
		A.SeqNum as SeqNum
		FROM (
		SELECT DISTINCT 
		P.pol_key as PolicyKey,
		InsSeg.InsuranceSegmentAbbreviation as InsuranceSegmentAbbreviation ,
		A.ReasonAmendedCode as ReasonAmendedCode,
		SPC.StrategicProfitCenterAbbreviation as StrategicProfitCenterAbbreviation,
		P.pol_status_code as pol_status_code,
		A.PremiumTransactionEffectiveDate,
		case 
			when premiumtransactioncode = 'RevisedFinalAudit' then 0 
			when premiumtransactioncode = 'FinalAudit' then 1 else 2 
		end as SeqNum
		FROM
		@{pipeline().parameters.SOURCE_DATABASE_NAME_EDW}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction A 
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME_EDW}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC on A.RatingCoverageAKId=RC.RatingCoverageAKID and A.EffectiveDate=RC.EffectiveDate
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME_EDW}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC on RC.PolicyCoverageAKID=pc.PolicyCoverageAKID and PC.CurrentSnapshotFlag=1
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME_EDW}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy P on P.pol_ak_id=PC.PolicyAKID and P.crrnt_snpsht_flag=1
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME_EDW}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment InsSeg on InsSeg.InsuranceSegmentAKId=P.InsuranceSegmentAKId and InsSeg.CurrentSnapshotFlag=1
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME_EDW}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter SPC on P.StrategicProfitCenterAKId=SPC.StrategicProfitCenterAKId and SPC.CurrentSnapshotFlag=1
		and A.ExposureBasis='Payroll') A
		 order by A.SeqNum --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,PremiumTransactionEffectiveDate ORDER BY PolicyKey) = 1
),
EXP_Premium AS (
	SELECT
	EXP_Input.pol_key AS i_Policykey,
	EXP_Input.RatingStateProvinceAbbreviation AS i_StateProvinceCode,
	EXP_Input.pol_eff_date AS i_pol_eff_date,
	EXP_Input.pol_num AS i_pol_sym,
	LKP_EstimatedAuditCodeKeys_DCT.InsuranceSegmentAbbreviation AS lkp_InsuranceSegmentAbbreviation,
	LKP_EstimatedAuditCodeKeys_DCT.ReasonAmendedCode AS lkp_ReasonAmendedCode,
	LKP_EstimatedAuditCodeKeys_DCT.StrategicProfitCenterAbbreviation AS lkp_StrategicProfitCenterAbbreviation,
	LKP_EstimatedAuditCodeKeys_DCT.pol_status_code AS lkp_pol_status_code,
	LKP_EstimatedAuditCodeKeys_DCT.SeqNum AS lkp_SeqNum,
	'WC' AS v_InsuranceLine,
	-- *INF*: IIF(i_pol_sym='000', 'DCT','PMS')
	IFF(i_pol_sym = '000', 'DCT', 'PMS') AS v_SourceSystem,
	-- *INF*: :LKP.LKP_AUDITSCHEDULE(i_Policykey, v_InsuranceLine, i_pol_eff_date)
	LKP_AUDITSCHEDULE_i_Policykey_v_InsuranceLine_i_pol_eff_date.AuditStatus AS v_AuditStatus,
	-- *INF*: DECODE(TRUE,
	--   UPPER(lkp_ReasonAmendedCode) = 'ESTIMATED',
	--     DECODE(TRUE,
	--       (IN(i_StateProvinceCode, '21', '48') OR UPPER(lkp_InsuranceSegmentAbbreviation)='POOL'),
	--         'U',
	--         'Y'
	--       ),
	--   IN(UPPER(lkp_ReasonAmendedCode),'CANCELLATION','SEEDETAIL'),
	--     'N',
	--   (UPPER(lkp_StrategicProfitCenterAbbreviation)='ARGENT' AND ISNULL(lkp_ReasonAmendedCode) AND UPPER(lkp_InsuranceSegmentAbbreviation)='CL'),
	--     'Y',
	--   (UPPER(lkp_StrategicProfitCenterAbbreviation)='WB - CL' AND ISNULL(lkp_ReasonAmendedCode) AND UPPER(lkp_InsuranceSegmentAbbreviation)='POOL' AND IN(lkp_pol_status_code,'N','C')),
	--     'Y',
	--   (UPPER(lkp_StrategicProfitCenterAbbreviation)='WB - CL' AND ISNULL(lkp_ReasonAmendedCode) AND UPPER(lkp_InsuranceSegmentAbbreviation)='CL' AND lkp_pol_status_code ='N'),
	--     'Y',
	--     'N'
	-- )
	DECODE(
	    TRUE,
	    UPPER(lkp_ReasonAmendedCode) = 'ESTIMATED', DECODE(
	        TRUE,
	        (i_StateProvinceCode IN ('21','48') OR UPPER(lkp_InsuranceSegmentAbbreviation) = 'POOL'), 'U',
	        'Y'
	    ),
	    UPPER(lkp_ReasonAmendedCode) IN ('CANCELLATION','SEEDETAIL'), 'N',
	    (UPPER(lkp_StrategicProfitCenterAbbreviation) = 'ARGENT' AND lkp_ReasonAmendedCode IS NULL AND UPPER(lkp_InsuranceSegmentAbbreviation) = 'CL'), 'Y',
	    (UPPER(lkp_StrategicProfitCenterAbbreviation) = 'WB - CL' AND lkp_ReasonAmendedCode IS NULL AND UPPER(lkp_InsuranceSegmentAbbreviation) = 'POOL' AND lkp_pol_status_code IN ('N','C')), 'Y',
	    (UPPER(lkp_StrategicProfitCenterAbbreviation) = 'WB - CL' AND lkp_ReasonAmendedCode IS NULL AND UPPER(lkp_InsuranceSegmentAbbreviation) = 'CL' AND lkp_pol_status_code = 'N'), 'Y',
	    'N'
	) AS v_DCT_EstimatedAuditCode,
	-- *INF*: DECODE(TRUE,
	-- IN(UPPER(v_AuditStatus), 'BYPASSED', 'REVERSED', 'OVERDUE'),'Y',
	-- IN(i_StateProvinceCode, '21', '48') AND UPPER(v_AuditStatus)='ESTIMATED','U',
	-- UPPER(v_AuditStatus)='ESTIMATED','Y',
	-- 'N'
	--  )
	DECODE(
	    TRUE,
	    UPPER(v_AuditStatus) IN ('BYPASSED','REVERSED','OVERDUE'), 'Y',
	    i_StateProvinceCode IN ('21','48') AND UPPER(v_AuditStatus) = 'ESTIMATED', 'U',
	    UPPER(v_AuditStatus) = 'ESTIMATED', 'Y',
	    'N'
	) AS v_PMS_EstimatedAuditCode,
	-- *INF*: IIF(v_SourceSystem='DCT',v_DCT_EstimatedAuditCode, v_PMS_EstimatedAuditCode)
	IFF(v_SourceSystem = 'DCT', v_DCT_EstimatedAuditCode, v_PMS_EstimatedAuditCode) AS o_EstimatedAuditCode
	FROM EXP_Input
	LEFT JOIN LKP_EstimatedAuditCodeKeys_DCT
	ON LKP_EstimatedAuditCodeKeys_DCT.PolicyKey = EXP_Input.pol_key AND LKP_EstimatedAuditCodeKeys_DCT.PremiumTransactionEffectiveDate <= EXP_Input.SelectionMonthEnd
	LEFT JOIN LKP_AUDITSCHEDULE LKP_AUDITSCHEDULE_i_Policykey_v_InsuranceLine_i_pol_eff_date
	ON LKP_AUDITSCHEDULE_i_Policykey_v_InsuranceLine_i_pol_eff_date.PolicyKey = i_Policykey
	AND LKP_AUDITSCHEDULE_i_Policykey_v_InsuranceLine_i_pol_eff_date.InsuranceLine = v_InsuranceLine
	AND LKP_AUDITSCHEDULE_i_Policykey_v_InsuranceLine_i_pol_eff_date.AuditEffectiveDate = i_pol_eff_date

),
LKP_CLTF_SubroCheck AS (
	SELECT
	claim_num,
	CLTFCount,
	in_claim_num
	FROM (
		select
		b.claim_num as claim_num,
		count(1) as CLTFCount
		from 
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_loss_transaction_fact a 
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_dim b on a.claim_occurrence_dim_id=b.claim_occurrence_dim_id
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction_type_dim tt on a.claim_trans_type_dim_id=tt.claim_trans_type_dim_id
		where
		tt.pms_trans_code in ('81','82','83','84','85','86','87','88','89','99')
		group by b.claim_num having count(1) > 0
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_num ORDER BY claim_num) = 1
),
LKP_Direct_Written_Premium AS (
	SELECT
	policykey,
	PremiumMasterDirectWrittenPremium,
	in_pol_key,
	in_SelectionMonthEnd
	FROM (
		select A.policykey as policykey, A.PremiumMasterDirectWrittenPremium as PremiumMasterDirectWrittenPremium  from (
		select pol.pol_key as policykey, sum(PremiumMasterDirectWrittenPremium) as  PremiumMasterDirectWrittenPremium
		from @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.PremiumMasterFact a with (nolock)
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.InsuranceReferenceCoverageDim b with (nolock) on a.InsuranceReferenceCoverageDimId=b.InsuranceReferenceCoverageDimId
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.policy_dim pol with (nolock) on a.PolicyDimID=pol.pol_dim_id
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.CoverageDetailDim CDD with (nolock) on a.CoverageDetailDimId=CDD.CoverageDetailDimId
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.calendar_dim CAL on a.PremiumMasterRunDateID=CAL.clndr_id
		where 
		b.coveragedescription='Work Comp' AND
		CDD.ExposureBasis = 'Payroll' AND 
		pol.pol_sym ='000'
		group by pol.pol_key
		having sum(PremiumMasterDirectWrittenPremium)>5000
		
		Union
		
		select pol.pol_key as policykey, sum(PremiumMasterDirectWrittenPremium) as  PremiumMasterDirectWrittenPremium
		from @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.PremiumMasterFact a with (nolock)
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.InsuranceReferenceCoverageDim b with (nolock) on a.InsuranceReferenceCoverageDimId=b.InsuranceReferenceCoverageDimId
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.policy_dim pol with (nolock) on a.PolicyDimID=pol.pol_dim_id
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.CoverageDetailDim CDD with (nolock) on a.CoverageDetailDimId=CDD.CoverageDetailDimId
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.calendar_dim CAL on a.PremiumMasterRunDateID=CAL.clndr_id
		where 
		b.coveragedescription='Work Comp' AND
		pol.pol_sym !='000' 
		group by pol.pol_key
		having sum(PremiumMasterDirectWrittenPremium)>5000
		) A 
		Order by A.PremiumMasterDirectWrittenPremium Desc --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY policykey ORDER BY policykey) = 1
),
LKP_Paid_Outstanding_Amounts AS (
	SELECT
	PaidIndemnityAmount,
	PaidMedicalAmount,
	outstanding_expense_reserve,
	subro_outstanding,
	in_claim_num,
	in_claimant_num,
	in_SelectionMonthEnd,
	claim_num,
	claimant_num
	FROM (
		SELECT 
		SUM((case claimant_coverage_dim.cause_of_loss when  '05' then  claim_loss_transaction_fact.direct_loss_paid_including_recoveries else 0 end)) as PaidIndemnityAmount,
		SUM((case claimant_coverage_dim.cause_of_loss 
		    when '06' then 
		        (case when trans_ctgry_code not in ('DX') then
					claim_loss_transaction_fact.direct_loss_paid_including_recoveries else 0 end ) 
		        +
		        (case when trans_ctgry_code in ('WD','DR') then ABS(claim_loss_transaction_fact.direct_other_recovery_paid) else 0 end)
		    else 0 end)) as PaidMedicalAmount,
		--claimant_coverage_dim.claimant_cov_dim_id as claimant_cov_dim_id 
		e.claim_num as claim_num,
		claimant_dim.claimant_num as claimant_num,
		sum(claim_loss_transaction_fact.direct_alae_outstanding_including_recoveries) AS outstanding_expense_reserve,
			sum(claim_loss_transaction_fact.direct_subrogation_paid) AS subro_outstanding
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.claim_loss_transaction_fact 
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.claim_payment_category_type_dim on claim_payment_category_type_dim.claim_pay_ctgry_type_dim_id = claim_loss_transaction_fact.claim_pay_ctgry_type_dim_id
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.claimant_coverage_dim on claimant_coverage_dim.claimant_cov_dim_id = claim_loss_transaction_fact.claimant_cov_dim_id
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.claim_transaction_type_dim on claim_loss_transaction_fact.claim_trans_type_dim_id = claim_transaction_type_dim.claim_trans_type_dim_id 
		 and trans_kind_code = 'D' 
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.claimant_dim claimant_dim on claimant_dim.claimant_dim_id  = claim_loss_transaction_fact.claimant_dim_id
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.calendar_dim c on claim_loss_transaction_fact.claim_trans_date_id=c.clndr_id
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.claim_occurrence_dim e on claim_loss_transaction_fact.claim_occurrence_dim_id=e.claim_occurrence_dim_id
		WHERE
		c.clndr_date <= DATEADD(s,-1,DATEADD(mm, DATEDIFF(m,0,GETDATE()) - @{pipeline().parameters.NO_OF_MONTHS_END} +1,0))
		group by e.claim_num, claimant_dim.claimant_num
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_num,claimant_num ORDER BY PaidIndemnityAmount) = 1
),
LKP_PolicyDim_IsCustomerStillInsured AS (
	SELECT
	cust_num,
	pol_key,
	pol_exp_date,
	eff_from_date,
	eff_to_date,
	in_cust_num,
	in_SelectionMonthEnd
	FROM (
		select distinct  A.pol_key AS pol_key,  A.cust_num AS cust_num,  A.eff_from_date AS eff_from_date,  A.eff_to_date AS eff_to_date,  A.pol_exp_date AS pol_exp_date 
		from 
		(
		SELECT  distinct  P.pol_key AS pol_key,  A.cust_num AS cust_num,  P.eff_from_date AS eff_from_date,  P.eff_to_date AS eff_to_date,  P.pol_exp_date AS pol_exp_date 
		FROM 
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_dim A 
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterFact B on B.ContractCustomerDimID=A.contract_cust_dim_id
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim P ON P.pol_dim_id=B.PolicyDimID
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim IR on B.InsuranceReferenceDimId=IR.InsuranceReferenceDimId
		where P.pol_cancellation_date >='2100-12-31'
		and ir.InsuranceSegmentAbbreviation in ('CL','Pool')
		and ir.PolicyOfferingAbbreviation='WC'
		and P.pol_exp_date >  DATEADD(s,0,DATEADD(mm, DATEDIFF(m,0,getdate()) - @{pipeline().parameters.NO_OF_MONTHS_START},0))
		UNION
		SELECT  distinct  P.pol_key AS pol_key,  A.cust_num AS cust_num,  P.eff_from_date AS eff_from_date,  P.eff_to_date AS eff_to_date,  P.pol_exp_date AS pol_exp_date 
		FROM 
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_dim A 
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_loss_transaction_fact B on B.contract_cust_dim_id=A.contract_cust_dim_id
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim P ON P.pol_dim_id=B.pol_dim_id
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim IR on B.InsuranceReferenceDimId=IR.InsuranceReferenceDimId
		where P.pol_cancellation_date >='2100-12-31'
		and ir.InsuranceSegmentAbbreviation in ('CL','Pool') 
		and ir.PolicyOfferingAbbreviation='WC'
		and P.pol_exp_date >  DATEADD(s,0,DATEADD(mm, DATEDIFF(m,0,getdate()) - @{pipeline().parameters.NO_OF_MONTHS_START},0))
		) A
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY cust_num,eff_from_date,eff_to_date,pol_exp_date ORDER BY cust_num) = 1
),
LKP_Reopened_Claims AS (
	SELECT
	claim_num,
	claimant_num,
	transaction_date,
	in_claimant_num,
	in_claim_num,
	in_clndr_date
	FROM (
		SELECT b.claim_num as claim_num, c.claimant_num as claimant_num, e.clndr_date as transaction_date 
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.claim_loss_transaction_fact a, 
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.claim_occurrence_dim b, 
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.claimant_dim c, 
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.claim_transaction_type_dim d,
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.calendar_dim e
		where a.claim_occurrence_dim_id=b.claim_occurrence_dim_id
		and a.claimant_dim_id=c.claimant_dim_id
		and a.claim_trans_type_dim_id=d.claim_trans_type_dim_id
		and a.claim_trans_date_id=e.clndr_id
		and d.trans_code_descript in ('Reopen closed claim', 'Record a reopened claim')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_num,claimant_num,transaction_date ORDER BY claim_num) = 1
),
LKP_WorkWCStatExtract AS (
	SELECT
	CreatedDate,
	IncurredIndemnityAmount,
	IncurredMedicalAmount,
	in_claim_num,
	in_SelectionMonthEnd,
	ClaimNumber
	FROM (
		SELECT WorkWCSTATExtract.IncurredIndemnityAmount as IncurredIndemnityAmount, WorkWCSTATExtract.IncurredMedicalAmount as IncurredMedicalAmount, WorkWCSTATExtract.ClaimNumber as ClaimNumber, WorkWCSTATExtract.CreatedDate as CreatedDate FROM WorkWCSTATExtract WorkWCSTATExtract
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClaimNumber,CreatedDate ORDER BY CreatedDate DESC) = 1
),
EXP_Loss_Paid_Details AS (
	SELECT
	EXP_Input.claimant_full_name,
	EXP_Input.claimant_num,
	EXP_Input.pol_key,
	EXP_Input.pol_eff_date,
	EXP_Input.claim_num,
	EXP_Input.claim_loss_date,
	EXP_Input.trans_close_date AS clndr_date,
	LKP_Paid_Outstanding_Amounts.PaidIndemnityAmount,
	LKP_Paid_Outstanding_Amounts.PaidMedicalAmount,
	LKP_Paid_Outstanding_Amounts.outstanding_expense_reserve,
	LKP_Paid_Outstanding_Amounts.subro_outstanding,
	LKP_Reopened_Claims.claim_num AS lkp_claim_num,
	LKP_WorkWCStatExtract.CreatedDate AS BureauReportedDate,
	LKP_WorkWCStatExtract.IncurredIndemnityAmount,
	LKP_WorkWCStatExtract.IncurredMedicalAmount,
	EXP_Input.RatingStateProvinceAbbreviation,
	EXP_Input.cust_num AS CustomerNumber,
	LKP_Direct_Written_Premium.PremiumMasterDirectWrittenPremium,
	LKP_CLTF_SubroCheck.CLTFCount AS lkp_CLTFCountSubroCheck,
	LKP_PolicyDim_IsCustomerStillInsured.pol_key AS lkp_CurrentPolKey,
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL(lkp_CurrentPolKey),'Y',
	-- 'N')
	DECODE(
	    TRUE,
	    lkp_CurrentPolKey IS NOT NULL, 'Y',
	    'N'
	) AS v_IsInsuredCurrentCheck,
	-- *INF*: --DATE_DIFF(clndr_date,BureauReportedDate,'MM')
	-- IIF(ISNULL(clndr_date) OR ISNULL(BureauReportedDate), NULL,
	-- DATE_DIFF(
	-- TO_DATE('01'||'-'||TO_CHAR(clndr_date,'MON')||'-'||TO_CHAR(clndr_date,'YYYY'),'DD-MON-YYYY'), 
	-- TO_DATE('01'||'-'||TO_CHAR(BureauReportedDate,'MON')||'-'||TO_CHAR(BureauReportedDate,'YYYY'),'DD-MON-YYYY'),
	-- 'MM'
	-- )
	-- )
	IFF(
	    clndr_date IS NULL OR BureauReportedDate IS NULL, NULL,
	    DATEDIFF(MONTH,TO_TIMESTAMP('01' || '-' || TO_CHAR(clndr_date, 'MON') || '-' || TO_CHAR(clndr_date, 'YYYY'), 'DD-MON-YYYY'),TO_TIMESTAMP('01' || '-' || TO_CHAR(BureauReportedDate, 'MON') || '-' || TO_CHAR(BureauReportedDate, 'YYYY'), 'DD-MON-YYYY'))
	) AS MonthsSinceUnitStat,
	-- *INF*: DECODE(TRUE, IN(RatingStateProvinceAbbreviation,'MI','WI','MN','FL','MA'),'Y','N')
	DECODE(
	    TRUE,
	    RatingStateProvinceAbbreviation IN ('MI','WI','MN','FL','MA'), 'Y',
	    'N'
	) AS v_RatingStateProvinceCheck,
	-- *INF*: DECODE(TRUE, outstanding_expense_reserve=0,'Y','N')
	DECODE(
	    TRUE,
	    outstanding_expense_reserve = 0, 'Y',
	    'N'
	) AS v_ExpenseReserveCheck,
	-- *INF*: DECODE(TRUE, subro_outstanding=0,'Y','N')
	DECODE(
	    TRUE,
	    subro_outstanding = 0, 'Y',
	    'N'
	) AS v_SubrogationCheck,
	EXP_Premium.o_EstimatedAuditCode AS EstimatedAuditCode,
	-- *INF*: IIF(EstimatedAuditCode='N','Y','N')
	-- 
	-- -- in this case 'N' means it's good, so we map it to Y to pass the test
	IFF(EstimatedAuditCode = 'N', 'Y', 'N') AS v_EstimatedAuditCodeCheck,
	-- *INF*: IIF((PaidIndemnityAmount+PaidMedicalAmount)<(IncurredIndemnityAmount+IncurredMedicalAmount),'Y','N')
	IFF(
	    (PaidIndemnityAmount + PaidMedicalAmount) < (IncurredIndemnityAmount + IncurredMedicalAmount),
	    'Y',
	    'N'
	) AS v_PaidGreaterThanIncurredCheck,
	-- *INF*: IIF( PaidIndemnityAmount+PaidMedicalAmount > 0,'Y','N')
	IFF(PaidIndemnityAmount + PaidMedicalAmount > 0, 'Y', 'N') AS v_PaidGreaterThanZeroCheck,
	-- *INF*: IIF(ISNULL(lkp_claim_num),'Y','N')
	IFF(lkp_claim_num IS NULL, 'Y', 'N') AS v_CancelReopenCheck,
	-- *INF*: 'Y'
	-- --IIF(ISNULL(lkp_CLTFCountSubroCheck) or lkp_CLTFCountSubroCheck = 0, 'Y','N')
	-- 
	-- -- requirements change to no took at subro by transaction code.  Keeping lookup and check though because requirements changed back and forth enough that we may want to re-enable this check.
	'Y' AS v_SubroHistoryCheck,
	-- *INF*: IIF(PremiumMasterDirectWrittenPremium>0,'Y','N')
	IFF(PremiumMasterDirectWrittenPremium > 0, 'Y', 'N') AS v_DWPCheck,
	-- *INF*: IIF(
	-- v_PaidGreaterThanIncurredCheck='Y' AND
	-- v_PaidGreaterThanZeroCheck='Y' AND
	-- v_DWPCheck='Y' AND
	-- v_RatingStateProvinceCheck='Y' AND
	-- v_CancelReopenCheck='Y' AND
	-- v_ExpenseReserveCheck = 'Y' AND
	-- v_IsInsuredCurrentCheck='Y' AND
	-- v_SubrogationCheck='Y' AND
	-- v_EstimatedAuditCodeCheck='Y' AND
	-- v_SubroHistoryCheck='Y',
	-- 'Y','N')
	-- 
	-- --IIF((PaidIndemnityAmount+PaidMedicalAmount)<(IncurredIndemnityAmount+IncurredMedicalAmount) AND (PaidIndemnityAmount+PaidMedicalAmount)>0 AND 
	-- --PremiumMasterDirectWrittenPremium>0 AND v_RatingStateProvinceCheck='Y' AND 
	-- --ISNULL(lkp_claim_num) AND 
	-- --v_Expense_ReserveCheck='Y' AND
	-- --v_IsInsuredCurrent_check='Y' AND
	-- --(ISNULL(lkp_CLTFCount_subro_check) or lkp_CLTFCount_subro_check = 0) AND
	-- --EstimatedAuditCode='N' AND
	-- --v_SubrogationCheck='Y' ,'Y','N')
	IFF(
	    v_PaidGreaterThanIncurredCheck = 'Y'
	    and v_PaidGreaterThanZeroCheck = 'Y'
	    and v_DWPCheck = 'Y'
	    and v_RatingStateProvinceCheck = 'Y'
	    and v_CancelReopenCheck = 'Y'
	    and v_ExpenseReserveCheck = 'Y'
	    and v_IsInsuredCurrentCheck = 'Y'
	    and v_SubrogationCheck = 'Y'
	    and v_EstimatedAuditCodeCheck = 'Y'
	    and v_SubroHistoryCheck = 'Y',
	    'Y',
	    'N'
	) AS qualify_check,
	-- *INF*: DECODE(TRUE,
	-- v_PaidGreaterThanIncurredCheck='N', 'PaidGreaterThanIncurredCheck',
	-- v_PaidGreaterThanZeroCheck='N','PaidGreaterThanZeroCheck',
	-- v_DWPCheck='N','DWPCheck',
	-- v_RatingStateProvinceCheck='N','RatingStateProvinceCheck',
	-- v_CancelReopenCheck='N','CancelReopenCheck',
	-- v_ExpenseReserveCheck ='N', 'ExpenseReserveCheck',
	-- v_IsInsuredCurrentCheck='N','IsInsuredCurrentCheck',
	-- v_SubrogationCheck='N','SubrogationCheck',
	-- v_EstimatedAuditCodeCheck='N','EstimatedAuditCodeCheck',
	-- v_SubroHistoryCheck='N','SubroHistoryCheck',
	-- 'NoFailures')
	DECODE(
	    TRUE,
	    v_PaidGreaterThanIncurredCheck = 'N', 'PaidGreaterThanIncurredCheck',
	    v_PaidGreaterThanZeroCheck = 'N', 'PaidGreaterThanZeroCheck',
	    v_DWPCheck = 'N', 'DWPCheck',
	    v_RatingStateProvinceCheck = 'N', 'RatingStateProvinceCheck',
	    v_CancelReopenCheck = 'N', 'CancelReopenCheck',
	    v_ExpenseReserveCheck = 'N', 'ExpenseReserveCheck',
	    v_IsInsuredCurrentCheck = 'N', 'IsInsuredCurrentCheck',
	    v_SubrogationCheck = 'N', 'SubrogationCheck',
	    v_EstimatedAuditCodeCheck = 'N', 'EstimatedAuditCodeCheck',
	    v_SubroHistoryCheck = 'N', 'SubroHistoryCheck',
	    'NoFailures'
	) AS v_qualifyLog,
	-- *INF*: 'FileYearMonth,'||TO_CHAR(clndr_date,'YYYYMM')  ||',PolicyKey,' || pol_key||',ClaimNum,' || claim_num||',CustomerNumber,'||CustomerNumber ||',FirstFailure,'||v_qualifyLog
	'FileYearMonth,' || TO_CHAR(clndr_date, 'YYYYMM') || ',PolicyKey,' || pol_key || ',ClaimNum,' || claim_num || ',CustomerNumber,' || CustomerNumber || ',FirstFailure,' || v_qualifyLog AS v_RuleSummary,
	v_RuleSummary AS o_RuleSummary,
	-- *INF*: 'AggrevatedInequity_UnqualifiedClaims_'||TO_CHAR(clndr_date,'YYYYMM')||'.csv'
	'AggrevatedInequity_UnqualifiedClaims_' || TO_CHAR(clndr_date, 'YYYYMM') || '.csv' AS FileName
	FROM EXP_Input
	 -- Manually join with EXP_Premium
	LEFT JOIN LKP_CLTF_SubroCheck
	ON LKP_CLTF_SubroCheck.claim_num = EXP_Input.claim_num
	LEFT JOIN LKP_Direct_Written_Premium
	ON LKP_Direct_Written_Premium.policykey = EXP_Input.pol_key
	LEFT JOIN LKP_Paid_Outstanding_Amounts
	ON LKP_Paid_Outstanding_Amounts.claim_num = EXP_Input.claim_num AND LKP_Paid_Outstanding_Amounts.claimant_num = EXP_Input.claimant_num
	LEFT JOIN LKP_PolicyDim_IsCustomerStillInsured
	ON LKP_PolicyDim_IsCustomerStillInsured.cust_num = EXP_Input.cust_num AND LKP_PolicyDim_IsCustomerStillInsured.eff_from_date <= EXP_Input.SelectionMonthEnd AND LKP_PolicyDim_IsCustomerStillInsured.eff_to_date >= EXP_Input.SelectionMonthEnd AND LKP_PolicyDim_IsCustomerStillInsured.pol_exp_date >= EXP_Input.SelectionMonthEnd
	LEFT JOIN LKP_Reopened_Claims
	ON LKP_Reopened_Claims.claim_num = EXP_Input.claim_num AND LKP_Reopened_Claims.claimant_num = EXP_Input.claimant_num AND LKP_Reopened_Claims.transaction_date >= EXP_Input.trans_close_date
	LEFT JOIN LKP_WorkWCStatExtract
	ON LKP_WorkWCStatExtract.ClaimNumber = EXP_Input.claim_num AND LKP_WorkWCStatExtract.CreatedDate <= EXP_Input.SelectionMonthEnd
),
FIL_UnqualifiedClaims AS (
	SELECT
	qualify_check, 
	o_RuleSummary, 
	FileName
	FROM EXP_Loss_Paid_Details
	WHERE qualify_check='N'
),
AggrevatedInequity_UnqualifiedClaims AS (
	INSERT INTO GenericLoggingFlatFile
	(FileName, DataLogRow)
	SELECT 
	FILENAME, 
	o_RuleSummary AS DATALOGROW
	FROM FIL_UnqualifiedClaims
),
FIL_Qualified_claims AS (
	SELECT
	claimant_full_name, 
	claimant_num, 
	pol_key, 
	pol_eff_date, 
	claim_num, 
	claim_loss_date, 
	clndr_date, 
	PaidIndemnityAmount, 
	PaidMedicalAmount, 
	BureauReportedDate AS CreatedDate, 
	RatingStateProvinceAbbreviation, 
	CustomerNumber, 
	MonthsSinceUnitStat, 
	qualify_check
	FROM EXP_Loss_Paid_Details
	WHERE qualify_check='Y'
),
AGG_Target AS (
	SELECT
	claimant_full_name,
	claimant_num,
	pol_key,
	pol_eff_date,
	claim_num,
	claim_loss_date,
	clndr_date,
	-- *INF*: min(clndr_date)
	min(clndr_date) AS Claimant_Close_date,
	PaidIndemnityAmount,
	PaidMedicalAmount,
	CreatedDate AS BureauReportedDate,
	-- *INF*: min(BureauReportedDate)
	min(BureauReportedDate) AS out_BureauReportedDate,
	CustomerNumber,
	MonthsSinceUnitStat,
	-- *INF*: min(MonthsSinceUnitStat)
	min(MonthsSinceUnitStat) AS MonthsSinceUnitStat_out,
	RatingStateProvinceAbbreviation
	FROM FIL_Qualified_claims
	GROUP BY claimant_full_name, claimant_num, pol_key, pol_eff_date, claim_num, claim_loss_date, CustomerNumber, RatingStateProvinceAbbreviation
),
EXP_Passthrough AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	SYSDATE AS CreatedDate,
	pol_key,
	pol_eff_date,
	claim_num,
	PaidIndemnityAmount,
	PaidMedicalAmount,
	claimant_num,
	claimant_full_name,
	claim_loss_date,
	out_BureauReportedDate,
	Claimant_Close_date,
	MonthsSinceUnitStat_out,
	CustomerNumber,
	RatingStateProvinceAbbreviation
	FROM AGG_Target
),
Shortcut_to_WCAggravatedInequityExtract AS (

	------------ PRE SQL ----------
	@{pipeline().parameters.DELETE_PRESQL}
	-------------------------------


	INSERT INTO Shortcut_to_WCAggravatedInequityExtract
	(AuditId, CreatedDate, PolicyKey, PolicyEffectiveDate, ClaimNumber, PaidIndemnityAmount, PaidMedicalAmount, ClaimantNumber, ClaimantFullName, ClaimLossDate, BureauReportedDate, ClaimantCloseDate, MonthsSinceUnitStat, CustomerNumber, RatingState)
	SELECT 
	AuditID AS AUDITID, 
	CREATEDDATE, 
	pol_key AS POLICYKEY, 
	pol_eff_date AS POLICYEFFECTIVEDATE, 
	claim_num AS CLAIMNUMBER, 
	PAIDINDEMNITYAMOUNT, 
	PAIDMEDICALAMOUNT, 
	claimant_num AS CLAIMANTNUMBER, 
	claimant_full_name AS CLAIMANTFULLNAME, 
	claim_loss_date AS CLAIMLOSSDATE, 
	out_BureauReportedDate AS BUREAUREPORTEDDATE, 
	Claimant_Close_date AS CLAIMANTCLOSEDATE, 
	MonthsSinceUnitStat_out AS MONTHSSINCEUNITSTAT, 
	CUSTOMERNUMBER, 
	RatingStateProvinceAbbreviation AS RATINGSTATE
	FROM EXP_Passthrough
),
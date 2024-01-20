WITH
LKP_ReserveAmount AS (
	SELECT
	DirectLossOutstandingIR,
	claim_num,
	cause_of_loss_long_descript
	FROM (
		SELECT		
			claim_occurrence_dim.claim_num as claim_num,
			cause_of_loss_long_descript as cause_of_loss_long_descript,
			SUM(dbo.vwLossMasterFact.DirectLossOutstandingER) as DirectLossOutstandingIR
		FROM
			dbo.claim_occurrence_dim INNER JOIN dbo.vwLossMasterFact 
			ON (dbo.claim_occurrence_dim.claim_occurrence_dim_id=dbo.vwLossMasterFact.claim_occurrence_dim_id)
			INNER JOIN dbo.policy_dim 
			ON (dbo.policy_dim.pol_dim_id=dbo.vwLossMasterFact.pol_dim_id)
			INNER JOIN dbo.calendar_dim  Monthly_Run_Date_dim 
			ON (dbo.vwLossMasterFact.loss_master_run_date_id=Monthly_Run_Date_dim.clndr_id  
				AND  Monthly_Run_Date_dim.CalendarDate=Monthly_Run_Date_dim.CalendarEndOfMonthDate)
			INNER JOIN dbo.InsuranceReferenceDim 
			ON (dbo.InsuranceReferenceDim.InsuranceReferenceDimId=dbo.vwLossMasterFact.InsuranceReferenceDimId) 
			INNER JOIN dbo.claimant_coverage_dim CCD 
			ON dbo.vwLossMasterFact.claimant_Cov_dim_id = CCD.claimant_Cov_dim_id
		WHERE	
			dbo.InsuranceReferenceDim.InsuranceSegmentDescription  IN  ( 'Pool Services'  )
		--AND	Monthly_Run_Date_dim.clndr_date >= @{pipeline().parameters.STARTDATE}
		--CONVERT(DATE,(DATEADD(qq, DATEDIFF(qq, 0, GETDATE()) - 1, 0)))
		AND	Monthly_Run_Date_dim.clndr_date < @{pipeline().parameters.ENDDATE}	
		--CONVERT(DATE,DATEADD(qq, DATEDIFF(qq, 0, GETDATE()), 0))    
		GROUP BY 
			claim_occurrence_dim.claim_num,
			cause_of_loss_long_descript --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_num,cause_of_loss_long_descript ORDER BY DirectLossOutstandingIR DESC) = 1
),
SQ_WCRB_Loss AS (
	DECLARE @STARTDATE DATETIME = @{pipeline().parameters.STARTDATE},
			@ENDDATE DATETIME = @{pipeline().parameters.ENDDATE};
	
	SELECT distinct WCCD.wc_claimant_det_id,
		   CO.claim_occurrence_id,
		   CPO.claim_party_occurrence_id,
		   CCD.claimant_cov_det_id,
		   LMC.loss_master_calculation_id,
	         LMC.loss_master_run_date,
	         POL.pol_id,
		   POL.pol_sym,
		   POL.pol_num,
		   POL.pol_mod,
		   POL.pol_eff_date,
		   CC.name,
		   CO.claim_loss_date,
		   CO.s3p_claim_num,
		   COC.claim_occurrence_date,
		   COC.claim_occurrence_status_code,
		   LMC.Paid_Loss_Amt,
		   LMC.outstanding_amt,
	        CT.Cause_of_Loss, 
		   WCCD.cause_inj_code,
		   WCCD.body_part_code,
		   WCCD.nature_inj_code,
		   LMC.class_code,
	        -1,-- RL.RiskLocationID,
	        -1,-- PC.PolicyCoverageID,
	         -1,--RC.RatingCoverageID,
		LMC.claim_trans_amt,
	         LMC.FinancialTypeCode,
		LMC.claim_occurrence_ak_id,
		CT.pms_trans_code
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_calculation LMC
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction CT
	ON LMC.claim_trans_ak_id=CT.claim_trans_ak_id
	AND LMC.crrnt_snpsht_flag=1
	AND CT.crrnt_snpsht_flag=1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD
	ON CT.claimant_cov_det_ak_id=CCD.claimant_cov_det_ak_id
	AND CCD.crrnt_snpsht_flag=1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence CPO
	ON LMC.claim_party_occurrence_ak_id=CPO.claim_party_occurrence_ak_id
	AND CPO.crrnt_snpsht_flag=1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence CO
	ON LMC.claim_occurrence_ak_id=CO.claim_occurrence_ak_id
	AND CO.crrnt_snpsht_flag=1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_calculation COC
	ON COC.claim_occurrence_ak_id=CO.claim_occurrence_ak_id
	AND COC.crrnt_snpsht_flag=1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.workers_comp_claimant_detail WCCD
	ON WCCD.claim_party_occurrence_ak_id=CPO.claim_party_occurrence_ak_id 
	AND WCCD.crrnt_snpsht_flag=1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL
	on POL.pol_ak_id=CO.pol_key_ak_id
	and POL.crrnt_snpsht_flag=1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer CC
	on CC.contract_cust_ak_id=POL.contract_cust_ak_id
	and CC.crrnt_snpsht_flag=1
	/*INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	ON CCD.RatingCoverageAKID=RC.RatingCoverageAKID
	and RC.CurrentSnapshotFlag=1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	ON RC.PolicyCoverageAKId=PC.PolicyCoverageAKId
	and PC.CurrentSnapshotFlag=1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	ON RL.RiskLocationAKId=PC.RiskLocationAKId
	and RL.CurrentSnapshotFlag=1*/
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Insurancesegment ISG
	ON POL.InsuranceSegmentAKId= ISG.InsuranceSegmentAKId
	WHERE 
	CT.source_sys_id='EXCEED'
	and InsuranceSegmentAbbreviation = '@{pipeline().parameters.INSURANCESEGMENTABBREVIATION}'
	and lmc.trans_kind_code = '@{pipeline().parameters.TRANS_KIND_CODE}'
	and pol.pol_sym<>'@{pipeline().parameters.POL_SYM}'
	and LMC.loss_master_run_date >= @STARTDATE and LMC.loss_master_run_date < @ENDDATE
	
	
	--For PMS: SC.MajorPerilCode='032' is not required any more
),
EXP_Lossdata AS (
	SELECT
	wc_claimant_det_id AS i_wc_claimant_det_id,
	claim_occurrence_id AS i_claim_occurrence_id,
	claim_party_occurrence_id AS i_claim_party_occurrence_id,
	claimant_cov_det_id AS i_claimant_cov_det_id,
	loss_master_calculation_id AS i_loss_master_calculation_id,
	loss_master_run_date AS i_loss_master_run_date,
	pol_id AS i_pol_id,
	pol_sym AS i_pol_sym,
	pol_num AS i_pol_num,
	pol_mod AS i_pol_mod,
	pol_eff_date AS i_pol_eff_date,
	name AS i_name,
	claim_loss_date AS i_claim_loss_date,
	s3p_claim_num AS i_s3p_claim_num,
	claim_occurrence_date AS i_claim_occurrence_date,
	claim_occurrence_status_code AS i_claim_occurrence_status_code,
	paid_loss_amt AS i_paid_loss_amt,
	outstanding_amt AS i_outstanding_amt,
	cause_of_loss AS i_cause_of_loss,
	cause_inj_code AS i_cause_inj_code,
	body_part_code AS i_body_part_code,
	nature_inj_code AS i_nature_inj_code,
	class_code AS i_class_code,
	RiskLocationID AS i_RiskLocationID,
	PolicyCoverageID AS i_PolicyCoverageID,
	StatisticalCoverageID AS i_StatisticalCoverageID,
	claim_trans_amt AS i_claim_trans_amt,
	FinancialTypeCode AS i_FinancialTypeCode,
	claim_occurrence_ak_id AS i_claim_occurrence_ak_id,
	pms_trans_code AS i_pms_trans_code,
	i_wc_claimant_det_id AS o_wc_claimant_det_id,
	i_claim_occurrence_id AS o_claim_occurrence_id,
	i_claim_party_occurrence_id AS o_claim_party_occurrence_id,
	i_claimant_cov_det_id AS o_claimant_cov_det_id,
	i_loss_master_calculation_id AS o_loss_master_calculation_id,
	i_pol_id AS o_pol_id,
	i_RiskLocationID AS o_RiskLocationID,
	i_PolicyCoverageID AS o_PolicyCoverageID,
	i_StatisticalCoverageID AS o_StatisticalCoverageID,
	i_loss_master_run_date AS o_loss_master_run_date,
	i_pol_sym || i_pol_num || i_pol_mod AS o_PolKey,
	-- *INF*: TO_CHAR(i_pol_eff_date,'MMDDYYYY')
	TO_CHAR(i_pol_eff_date, 'MMDDYYYY') AS o_PolicyEffectiveDate,
	-- *INF*: SUBSTR(i_name,1,90)
	SUBSTR(i_name, 1, 90) AS o_NameOfInsured,
	'48' AS o_StateCode,
	-- *INF*: TO_CHAR(i_claim_loss_date,'YYYY')
	TO_CHAR(i_claim_loss_date, 'YYYY') AS o_AccidentYear,
	i_s3p_claim_num AS o_ClaimNum,
	-- *INF*: IIF(i_claim_occurrence_status_code='O','00000000',TO_CHAR(i_claim_occurrence_date,'MMDDYYYY'))
	IFF(
	    i_claim_occurrence_status_code = 'O', '00000000',
	    TO_CHAR(i_claim_occurrence_date, 'MMDDYYYY')
	) AS o_ClaimClosedDate,
	i_claim_occurrence_status_code AS o_ClaimOccurrenceStatusCode,
	i_paid_loss_amt AS o_PaidLossAmt,
	i_cause_of_loss AS o_TypeLossCode,
	-- *INF*: IIF(IN(i_cause_of_loss,'01','05') AND IN(i_FinancialTypeCode,'D','R') AND 
	-- IN(i_pms_trans_code,'21','22','23','24','25','26','27','28','29','37','86','87'),i_paid_loss_amt,0)
	IFF(
	    i_cause_of_loss IN ('01','05')
	    and i_FinancialTypeCode IN ('D','R')
	    and i_pms_trans_code IN ('21','22','23','24','25','26','27','28','29','37','86','87'),
	    i_paid_loss_amt,
	    0
	) AS o_IndemnityPaymentAmount,
	-- *INF*: IIF(i_cause_of_loss='06' AND IN(i_FinancialTypeCode,'D','R') AND 
	-- IN(i_pms_trans_code,'21','22','23','24','25','26','27','28','29','37','86','87'),i_paid_loss_amt,0)
	IFF(
	    i_cause_of_loss = '06'
	    and i_FinancialTypeCode IN ('D','R')
	    and i_pms_trans_code IN ('21','22','23','24','25','26','27','28','29','37','86','87'),
	    i_paid_loss_amt,
	    0
	) AS o_MedicalPaymentAmount,
	-- *INF*: IIF(IN(i_FinancialTypeCode,'B','S') AND IN(i_pms_trans_code,'81','82','83','84','88','89'),i_paid_loss_amt,0)
	IFF(
	    i_FinancialTypeCode IN ('B','S') AND i_pms_trans_code IN ('81','82','83','84','88','89'),
	    i_paid_loss_amt,
	    0
	) AS o_RecoveryAmount,
	-- *INF*: :LKP.LKP_RESERVEAMOUNT(
	-- (IIF(IN(i_cause_of_loss,'01','05'),
	-- RTRIM(i_s3p_claim_num),'0')),'INDEMNITY')
	LKP_RESERVEAMOUNT__IIF_IN_i_cause_of_loss_01_05_RTRIM_i_s3p_claim_num_0_INDEMNITY.DirectLossOutstandingIR AS o_IndemnityReserveAmount,
	-- *INF*: :LKP.LKP_RESERVEAMOUNT(
	-- (IIF(IN(i_cause_of_loss,'06'),
	-- RTRIM(i_s3p_claim_num),'0')),'MEDICAL')
	LKP_RESERVEAMOUNT__IIF_IN_i_cause_of_loss_06_RTRIM_i_s3p_claim_num_0_MEDICAL.DirectLossOutstandingIR AS o_MedicalReserveAmount,
	-- *INF*: IIF(i_cause_of_loss='06',i_paid_loss_amt+i_outstanding_amt,0)
	IFF(i_cause_of_loss = '06', i_paid_loss_amt + i_outstanding_amt, 0) AS o_TotalMedicalAmount,
	-- *INF*: IIF(IN(i_cause_of_loss,'01','05') ,i_paid_loss_amt+i_outstanding_amt,0)
	IFF(i_cause_of_loss IN ('01','05'), i_paid_loss_amt + i_outstanding_amt, 0) AS o_TotalIndemnityAmount,
	-- *INF*: LTRIM(RTRIM(i_cause_inj_code))
	LTRIM(RTRIM(i_cause_inj_code)) AS o_cause_of_inj_code,
	'' AS o_InjrDescLoss,
	-- *INF*: IIF(LTRIM(RTRIM(i_body_part_code))='N/A','',LTRIM(RTRIM(i_body_part_code)))
	IFF(LTRIM(RTRIM(i_body_part_code)) = 'N/A', '', LTRIM(RTRIM(i_body_part_code))) AS o_body_part_code,
	-- *INF*: IIF(LTRIM(RTRIM(i_nature_inj_code))='N/A','',LTRIM(RTRIM(i_nature_inj_code)))
	IFF(LTRIM(RTRIM(i_nature_inj_code)) = 'N/A', '', LTRIM(RTRIM(i_nature_inj_code))) AS o_nature_of_injury_code,
	'' AS o_InjrDescCauseOfLoss,
	-- *INF*: SUBSTR(i_class_code,1,4)
	SUBSTR(i_class_code, 1, 4) AS o_Classcode,
	i_outstanding_amt AS o_OutstandingAmount,
	i_FinancialTypeCode AS o_FinancialTypeCode,
	i_claim_trans_amt AS o_claim_trans_amt,
	i_claim_occurrence_ak_id AS o_claim_occurrence_ak_id
	FROM SQ_WCRB_Loss
	LEFT JOIN LKP_RESERVEAMOUNT LKP_RESERVEAMOUNT__IIF_IN_i_cause_of_loss_01_05_RTRIM_i_s3p_claim_num_0_INDEMNITY
	ON LKP_RESERVEAMOUNT__IIF_IN_i_cause_of_loss_01_05_RTRIM_i_s3p_claim_num_0_INDEMNITY.claim_num = (
    IFF(
        i_cause_of_loss IN ('01','05'), RTRIM(i_s3p_claim_num), '0'
    ))
	AND LKP_RESERVEAMOUNT__IIF_IN_i_cause_of_loss_01_05_RTRIM_i_s3p_claim_num_0_INDEMNITY.cause_of_loss_long_descript = 'INDEMNITY'

	LEFT JOIN LKP_RESERVEAMOUNT LKP_RESERVEAMOUNT__IIF_IN_i_cause_of_loss_06_RTRIM_i_s3p_claim_num_0_MEDICAL
	ON LKP_RESERVEAMOUNT__IIF_IN_i_cause_of_loss_06_RTRIM_i_s3p_claim_num_0_MEDICAL.claim_num = (
    IFF(
        i_cause_of_loss IN ('06'), RTRIM(i_s3p_claim_num), '0'
    ))
	AND LKP_RESERVEAMOUNT__IIF_IN_i_cause_of_loss_06_RTRIM_i_s3p_claim_num_0_MEDICAL.cause_of_loss_long_descript = 'MEDICAL'

),
SQ_DCBILPolicyTerm AS (
	--select distinct
	--pt.PolicyTermId as PolicyTermId,
	--LTRIM(RTRIM(pt.PolicyReference)) as PolicyReference,
	--SUM (ItemScheduleAmount - ItemClosedToCashAmount - ItemClosedToCreditAmount - ItemWrittenOffAmount - ItemRedistributedAmount) 
	--over (PARTITION by pt.PolicyReference,pt.PolicyTermId) as DeferredAmount,
	--pt.PolicyTermEffectiveDate as PolicyTermEffectiveDate,
	--pt.PolicyTermExpirationDate as PolicyTermExpirationDate
	--from DCBILPolicyTerm Pt 
	--Inner join DCBILBillItemSchedule BI
	--on pt.PolicyTermId = BI.PolicyTermId
	----where  
	----BI.InstallmentDate >= @{pipeline().parameters.STARTDATE}
	----and BI.InstallmentDate<@EndDate
	----and  pt.PolicyReference='A057558'
	----Query 1
	---Damodar:-commented old query for 17809 tickect
	
	
	
	DECLARE @EndDate DATETIME
	
	
	--SET @EndDate = DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 1))
	
	
	SET @EndDate = DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0))
	
	select q1.PolicyTermId,q1.PolicyReference,(isnull(q1.DeferredAmount,0)-isnull(q2.FutureCashAmount,0)-
	-isnull(q3.FutureCreditAmount,0)-isnull(q4.FutureWriteOff,0)-isnull(q5.FutureWriteOff,0))  DeferredAmount,
	q1.PolicyTermEffectiveDate,q1.PolicyTermExpirationDate, case when q1.policytermstatuscode in ('FC') then 'Y' else 'N' end flat_cancelled_ind from (
	
	select distinct
	pt.PolicyTermId as PolicyTermId,
	LTRIM(RTRIM(pt.PolicyReference)) as PolicyReference,
	SUM (ItemScheduleAmount - ItemClosedToCashAmount - ItemClosedToCreditAmount - 
	ItemWrittenOffAmount - ItemRedistributedAmount) 
	over (PARTITION by pt.PolicyReference,pt.PolicyTermId) as DeferredAmount,
	pt.PolicyTermEffectiveDate as PolicyTermEffectiveDate,
	pt.PolicyTermExpirationDate as PolicyTermExpirationDate,
	pt.policytermstatuscode
	from DCBILPolicyTerm Pt 
	Inner join DCBILBillItemSchedule BI
	on pt.PolicyTermId = BI.PolicyTermId
	        where 
			--pt.PolicyReference='0309083' and
			
			((ItemClosedIndicator = 'N')  or (ItemClosedIndicator = 'Y' and BI.LastUpdatedTimestamp >  @EndDate))
			--		not (BI.InstallmentDate< @EndDate  AND ItemClosedIndicator = 'Y')
	And
	--BI.FirstInvoiceId IS NULL AND
	((FirstInvoiceId IS NULL)
	or (FirstInvoiceId in	 	 	 
	 	(Select InvoiceId from DCBILInvoice
	 	 	where InvoiceProductionDateTime > @EndDate)))
	
	        AND ItemId in
	        (SELECT ItemId
	               FROM DCBILBillItem
	               WHERE CoverageReference <> 'WC_AuditNoncomplianceCharge' AND TransactionDate < @EndDate
				   and ItemEffectiveDate< @EndDate))Q1
	
	
	                     
	                    left  join 
	--Q2
	
	(
	SELECT pt.policytermid,COALESCE(SUM(Amount),0) as FutureCashAmount 
	   FROM DCBILPaymentAllocation
	   inner join DCBILPolicyTerm pt on pt.PolicyTermId=DCBILPaymentAllocation.PolicyTermId
	   WHERE 
	  -- pt.PolicyReference='0309083' and
	   AllocationStatusCode ='A'
	     AND AllocationDate > @EndDate
	     AND AllocationStatusCode = 'A'
	     AND AllocationReasonCode = 'PYMT'
	     AND DestinationId in
	          (SELECT ItemScheduleId
	                       FROM DCBILBillItemSchedule
	                     WHERE 
						 ((ItemClosedIndicator = 'N')  or (ItemClosedIndicator = 'Y' and LastUpdatedTimestamp >  @EndDate))
			--		not (BI.InstallmentDate< @EndDate  AND ItemClosedIndicator = 'Y')
	And
	--BI.FirstInvoiceId IS NULL AND
	((FirstInvoiceId IS NULL)
	or (FirstInvoiceId in	 	 	 
	 	(Select InvoiceId from DCBILInvoice
	 	 	where InvoiceProductionDateTime > @EndDate)))
	                         --NOT (LastUpdatedTimestamp < @EndDate AND ItemClosedIndicator = 'Y')
	                        AND ItemId in
	                              (SELECT ItemId
	                                FROM DCBILBillItem
	                               WHERE CoverageReference <> 'WC_AuditNoncomplianceCharge' AND TransactionDate < @EndDate
								   and ItemEffectiveDate< @EndDate))
	                                                   group by pt.PolicyTermId) Q2
	
	
	                                                   on q2.policytermid=q1.policytermid
	
	                                                   
	
	                                                left   join
	
	--Q3
	(SELECT pt.policytermid,COALESCE(SUM(Amount),0) as FutureCreditAmount 
	   FROM DCBILCreditAllocation
	   inner join DCBILPolicyTerm pt on pt.PolicyTermId=DCBILCreditAllocation.PolicyTermId
	   WHERE 
	   --pt.PolicyReference='0309083' and
	   AllocationDate > @EndDate
	     AND AllocationStatusIndicator = 'A'
	     AND ItemId in
	        (SELECT ItemId
	               FROM DCBILBillItem
	                 WHERE   CoverageReference <> 'WC_AuditNoncomplianceCharge' AND TransactionDate < @EndDate
					 and ItemEffectiveDate< @EndDate)
	group by pt.policytermid) q3
	
	on q3.PolicyTermId=q1.PolicyTermId
	
	left join 
	--Q4
	(
	SELECT pt.policytermid,COALESCE(SUM(WriteOffAmount),0) as FutureWriteOff
	       FROM DCBILReceivableWriteOff
		   inner join DCBILPolicyTerm pt on pt.policytermid=DCBILReceivableWriteOff.PolicyTermId
	       WHERE  
		  -- pt.PolicyReference='0309083' and
		   WriteOffProcessedDateTime > @EndDate
	group by pt.PolicyTermId) Q4
	
	on q4.PolicyTermId=q1.PolicyTermId
	
	left join
	--Q5
	(
	SELECT pt.policytermid,COALESCE(SUM(WriteOffAmount),0) as FutureWriteOff
	       FROM DCBILReceivableWriteOff
		    inner join DCBILPolicyTerm pt on pt.policytermid=DCBILReceivableWriteOff.PolicyTermId
	       WHERE 
		  -- pt.PolicyReference='0309083' and
		   ReversalDateTime > @EndDate
	group by pt.PolicyTermId) Q5
	
	on q5.PolicyTermId=q1.PolicyTermId
	
	
	-- Final output for amounts by policytermid --> Q1-Q2-Q3-Q4-Q5
),
AGG_RemoveDuplicates AS (
	SELECT
	PolicyTermId,
	PolicyReference,
	DeferredAmount,
	PolicyTermEffectiveDate,
	PolicyTermExpirationDate,
	Flat_Cancelled_ind,
	-- *INF*: SUM(DeferredAmount)
	SUM(DeferredAmount) AS o_DeferredAmount
	FROM SQ_DCBILPolicyTerm
	GROUP BY PolicyReference
),
SQ_WCRB_Premium AS (
	DECLARE @STARTDATE DATETIME = @{pipeline().parameters.STARTDATE},
			@ENDDATE DATETIME =  @{pipeline().parameters.ENDDATE};
			
		/*	with  PolicyAkIDPerQ as 
		(select distinct policyakid from EarnedPremiumMonthlyCalculation EP1 where
	 	ep1.SourceSystemID='DCT' and   ep1.RunDate >= @STARTDATE and EP1.RunDate< @ENDDATE) */
	
			--DCT Direct Writen Premium
		 SELECT distinct POL.pol_id,
			   -1 as RiskLocationID,--RL.RiskLocationID,
			   -1 as PolicyCoverageID,--PC.PolicyCoverageID,
			   -1 as RatingCoverageID, --RC.RatingCoverageID,
			   -1 as PremiumTransactionID, --PT.PremiumTransactionID,
			   BSC.BureauStatisticalCodeID,
			   PMC.PremiumMasterCalculationID,
			   PMC.PremiumMasterRunDate,
			   POL.pol_sym,
			   POL.pol_num,
			   POL.pol_mod,
			   POL.pol_eff_date,
			   pol.pol_exp_date,
			   CC.name,
			   case when RTRIM(PMC.PremiumMasterPremiumType) in ('C','D') and 
			   PMC.PremiumMasterReasonAmendedCode in('CWO' , 'COL', 'CWB') then 0 
	        when PMC.PremiumMasterPremiumType='D' and PMC.PremiumMasterReasonAmendedCode != 'CWO'
			and PMC.PremiumMasterReasonAmendedCode != 'CWB' then PMC.PremiumMasterPremium
	        end Derived_PremiumMasterDirectWrittenPremium,
			  0 as EarnedPremium,--ISNULL(EPMC.EarnedPremium,0) as EarnedPremium,
			  0 as UnearnedPremium,-- ISNULL(EPMC.UnearnedPremium,0) as UnearnedPremium,
			   PMC.PremiumMasterAgencyCommissionRate,
			   PMC.SourceSystemID,
			   CASE WHEN POL.pol_cancellation_ind='Y' THEN 'Y' ELSE 'N' END FLAT_CANCELLED_IND,
			   case 
	                            when RTRIM(PMC.PremiumMasterPremiumType) in ('C','D') and 
			                PMC.PremiumMasterReasonAmendedCode in('CWO' , 'COL', 'CWB') then 0
	                            when RTRIM(RC.CoverageType) <> 'AuditNoncomplianceCharge' then 0 
	                            when PMC.PremiumMasterPremiumType='D' and  PMC.PremiumMasterReasonAmendedCode != 'CWO'
	                  		and PMC.PremiumMasterReasonAmendedCode != 'CWB' 
	                          and RTRIM(RC.CoverageType) = 'AuditNoncomplianceCharge' 
	                          then PMC.PremiumMasterPremium
	                          else 0
	                 end AuditNonComplianceChargePremium
		FROM dbo.PremiumTransaction PT 	join 
		dbo.PremiumMasterCalculation PMC
		on PT.PremiumTransactionAKID=PMC.PremiumTransactionAKID
		and PT.CurrentSnapshotFlag=1	and PMC.CurrentSnapshotFlag=1
		join dbo.RatingCoverage RC
		on RC.RatingCoverageAKID=PT.RatingCoverageAKID
		and RC.EffectiveDate=PT.EffectiveDate
	      --and RC.CurrentSnapshotFlag=1
		--join @{pipeline().parameters.DBO}.PolicyCoverage PC
		--on PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
		--and PC.CurrentSnapshotFlag=1
		--join @{pipeline().parameters.DBO}.RiskLocation RL
		--on PC.RiskLocationAKID=RL.RiskLocationAKID
		--and RL.CurrentSnapshotFlag=1
		join v2.policy POL
		on POL.pol_ak_id=PMC.PolicyAKID
		and POL.crrnt_snpsht_flag=1
		join dbo.contract_customer CC
		on CC.contract_cust_ak_id=POL.contract_cust_ak_id
		and CC.crrnt_snpsht_flag=1
		join  dbo.InsuranceSegment ISG
		on POL.InsuranceSegmentAKId=ISG.InsuranceSegmentAKId and ISG.CurrentSnapshotFlag=1
		left join dbo.BureauStatisticalCode BSC
		on BSC.PremiumTransactionAKID=PMC.PremiumTransactionAKID
		and BSC.CurrentSnapshotFlag=1
		where 
		PMC.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and PMC.CurrentSnapshotFlag=1
		and InsuranceSegmentAbbreviation = 'Pool'
		and PMC.PremiumMasterPremiumType = 'D'
		and PMC.Premiummasterrundate >= @STARTDATE and PMC.Premiummasterrundate < @ENDDATE
		--and pol.pol_num in ('0315770')
		
		union all
		
		
		
	
		-- DCT for Earned Premium and Unearned Premium
		 SELECT  POL.pol_id,
			  -1 as RiskLocationID,--RL.RiskLocationID,
			   -1 as PolicyCoverageID,--PC.PolicyCoverageID,
			   -1 as RatingCoverageID, --RC.RatingCoverageID,
			   -1 as PremiumTransactionID, --PT.PremiumTransactionID,
			   BSC.BureauStatisticalCodeID,
			   PMC.PremiumMasterCalculationID,
				EPMC.RunDate as PremiumMasterRunDate,
			   POL.pol_sym,
			   POL.pol_num,
			   POL.pol_mod,
			   POL.pol_eff_date,
			   pol.pol_exp_date,
			   CC.name,
				0 as PremiumMasterPremium,
			   ISNULL(EPMC.EarnedPremium,0) as EarnedPremium,
			   ISNULL(EPMC.UnearnedPremium,0) as UnearnedPremium,
			   PMC.PremiumMasterAgencyCommissionRate,
			   PMC.SourceSystemID,
			   CASE WHEN POL.pol_cancellation_ind='Y' THEN 'Y' ELSE 'N' END FLAT_CANCELLED_IND,
	                0 as AuditNonComplianceChargePremium
		FROM 
		dbo.EarnedPremiumMonthlyCalculation EPMC
		inner join
			(select EP.Premiummastercalculationpkid,max(EP.rundate) rundate 
			from dbo.EarnedPremiumMonthlyCalculation EP
			where EP.RunDate< @ENDDATE and EP.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}' /*AND  exists (select 1 from PolicyAkIDPerQ EP1 where	 
			 ep.PolicyAKID =ep1.PolicyAKID)  */
			group by Premiummastercalculationpkid) B
				ON B.PremiumMasterCalculationPKID=EPMC.PremiumMasterCalculationPKID and B.rundate=EPMC.RunDate
		inner join dbo.PremiumMasterCalculation PMC on EPMC.PremiumMasterCalculationPKID=PMC.PremiumMasterCalculationID 
		and EPMC.CurrentSnapshotFlag=1 and PMC.CurrentSnapshotFlag=1 
		and EPMC.SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}' and PMC.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}' and EPMC.RunDate< @ENDDATE
		inner join dbo.PremiumTransaction PT 	
		on PT.PremiumTransactionAKID=PMC.PremiumTransactionAKID
		and PT.CurrentSnapshotFlag=1	and PMC.CurrentSnapshotFlag=1
		inner join dbo.RatingCoverage RC
		on RC.RatingCoverageAKID=PT.RatingCoverageAKID
		and RC.EffectiveDate=PT.EffectiveDate and RTRIM(RC.CoverageType) <> 'AuditNoncomplianceCharge'
	--	and EPMC.RunDate >= @STARTDATE   and EPMC.RunDate< @ENDDATE
		
		--	inner join @{pipeline().parameters.DBO}.PremiumMasterCalculation PMC on EPMC.PremiumMasterCalculationPKID=PMC.PremiumMasterCalculationID and EPMC.CurrentSnapshotFlag=1 and EPMC.SourceSystemId='DCT'
		--inner join @{pipeline().parameters.DBO}.PremiumTransaction PT on PT.PremiumTransactionAKID=PMC.PremiumTransactionAKID and PT.CurrentSnapshotFlag=1 and PMC.CurrentSnapshotFlag=1
		--join @{pipeline().parameters.DBO}.RatingCoverage RC
		--on RC.RatingCoverageAKID=PT.RatingCoverageAKID
		--and RC.EffectiveDate=PT.EffectiveDate--and RC.CurrentSnapshotFlag=1
		--join @{pipeline().parameters.DBO}.PolicyCoverage PC
		--on PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
		--and PC.CurrentSnapshotFlag=1
		--join @{pipeline().parameters.DBO}.RiskLocation RL
		--on PC.RiskLocationAKID=RL.RiskLocationAKID
		--and RL.CurrentSnapshotFlag=1
		join V2.policy POL
		on POL.pol_ak_id=PMC.PolicyAKID
		and POL.crrnt_snpsht_flag=1
		join dbo.contract_customer CC
		on CC.contract_cust_ak_id=POL.contract_cust_ak_id
		and CC.crrnt_snpsht_flag=1
		join  dbo.InsuranceSegment ISG
		on POL.InsuranceSegmentAKId=ISG.InsuranceSegmentAKId
		left join dbo.BureauStatisticalCode BSC
		on BSC.PremiumTransactionAKID=PMC.PremiumTransactionAKID
		and BSC.CurrentSnapshotFlag=1
		where 
		PMC.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and InsuranceSegmentAbbreviation = '@{pipeline().parameters.INSURANCESEGMENTABBREVIATION}'
		and PMC.PremiumMasterPremiumType = 'D'
),
JNR_PolicyTerm AS (SELECT
	SQ_WCRB_Premium.pol_id, 
	SQ_WCRB_Premium.RiskLocationID, 
	SQ_WCRB_Premium.PolicyCoverageID, 
	SQ_WCRB_Premium.StatisticalCoverageID, 
	SQ_WCRB_Premium.PremiumTransactionID, 
	SQ_WCRB_Premium.BureauStatisticalCodeID, 
	SQ_WCRB_Premium.PremiumMasterCalculationID, 
	SQ_WCRB_Premium.PremiumMasterRunDate, 
	SQ_WCRB_Premium.pol_sym, 
	SQ_WCRB_Premium.pol_num, 
	SQ_WCRB_Premium.pol_mod, 
	SQ_WCRB_Premium.pol_eff_date, 
	SQ_WCRB_Premium.pol_exp_date, 
	SQ_WCRB_Premium.name, 
	SQ_WCRB_Premium.PremiumMasterPremium, 
	SQ_WCRB_Premium.EarnedPremium, 
	SQ_WCRB_Premium.UnearnedPremium, 
	SQ_WCRB_Premium.PremiumMasterAgencyCommissionRate, 
	SQ_WCRB_Premium.SourceSystemID, 
	SQ_WCRB_Premium.Flat_Cancelled_Ind AS Flat_Cancelled_Ind_edw, 
	SQ_WCRB_Premium.AuditNonComplianceChargePremium, 
	AGG_RemoveDuplicates.PolicyReference, 
	AGG_RemoveDuplicates.PolicyTermEffectiveDate, 
	AGG_RemoveDuplicates.PolicyTermExpirationDate, 
	AGG_RemoveDuplicates.Flat_Cancelled_ind, 
	AGG_RemoveDuplicates.o_DeferredAmount AS DeferredAmount
	FROM SQ_WCRB_Premium
	LEFT OUTER JOIN AGG_RemoveDuplicates
	ON AGG_RemoveDuplicates.PolicyReference = SQ_WCRB_Premium.pol_num AND AGG_RemoveDuplicates.PolicyTermEffectiveDate = SQ_WCRB_Premium.pol_eff_date AND AGG_RemoveDuplicates.PolicyTermExpirationDate = SQ_WCRB_Premium.pol_exp_date AND AGG_RemoveDuplicates.Flat_Cancelled_ind = SQ_WCRB_Premium.Flat_Cancelled_Ind
),
EXP_Premiumdata AS (
	SELECT
	pol_id AS i_pol_id,
	RiskLocationID AS i_RiskLocationID,
	PolicyCoverageID AS i_PolicyCoverageID,
	StatisticalCoverageID AS i_StatisticalCoverageID,
	PremiumTransactionID AS i_PremiumTransactionID,
	BureauStatisticalCodeID AS i_BureauStatisticalCodeID,
	PremiumMasterCalculationID AS i_PremiumMasterCalculationID,
	PremiumMasterRunDate AS i_PremiumMasterRunDate,
	pol_sym AS i_pol_sym,
	pol_num AS i_pol_num,
	pol_mod AS i_pol_mod,
	pol_eff_date AS i_pol_eff_date,
	name AS i_name,
	PremiumMasterPremium AS i_PremiumMasterPremium,
	EarnedPremium AS i_EarnedPremium,
	UnearnedPremium AS i_UnearnedPremium,
	PremiumMasterAgencyCommissionRate AS i_PremiumMasterAgencyCommissionRate,
	SourceSystemID AS i_SourceSystemID,
	AuditNonComplianceChargePremium AS i_AuditNonComplianceChargePremium,
	DeferredAmount AS i_DeferredAmount,
	i_pol_id AS o_pol_id,
	i_RiskLocationID AS o_RiskLocationID,
	i_PolicyCoverageID AS o_PolicyCoverageID,
	i_StatisticalCoverageID AS o_StatisticalCoverageID,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	i_BureauStatisticalCodeID AS o_BureauStatisticalCodeID,
	i_PremiumMasterCalculationID AS o_PremiumMasterCalculationID,
	i_PremiumMasterRunDate AS o_PremiumMasterRunDate,
	i_pol_sym || i_pol_num || i_pol_mod AS o_PolKey,
	-- *INF*: TO_CHAR(i_pol_eff_date,'MMDDYYYY')
	TO_CHAR(i_pol_eff_date, 'MMDDYYYY') AS o_pol_eff_date,
	-- *INF*: SUBSTR(i_name,1,90)
	SUBSTR(i_name, 1, 90) AS o_name,
	'48' AS o_StateCode,
	i_PremiumMasterPremium AS o_PremiumMasterPremium,
	i_EarnedPremium AS o_EarnedPremium,
	i_UnearnedPremium AS o_UnearnedPremium,
	i_DeferredAmount AS o_DeferredPremium,
	i_PremiumMasterAgencyCommissionRate AS o_PremiumMasterAgencyCommissionRate,
	i_PremiumMasterPremium * i_PremiumMasterAgencyCommissionRate AS o_CommissionPaid,
	i_AuditNonComplianceChargePremium AS o_AuditNonComplianceChargePremium
	FROM JNR_PolicyTerm
),
LKP_body_part_sup AS (
	SELECT
	body_part_support_id,
	body_part_code
	FROM (
		SELECT body_part_support_id as body_part_support_id, 
		LTRIM(RTRIM(body_part_code)) as body_part_code 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.body_part_sup
		WHERE crrnt_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY body_part_code ORDER BY body_part_support_id) = 1
),
LKP_cause_of_injury_sup AS (
	SELECT
	cause_of_inj_support_id,
	cause_of_inj_code
	FROM (
		SELECT cause_of_inj_support_id as cause_of_inj_support_id, 
		LTRIM(RTRIM(cause_of_inj_code)) as cause_of_inj_code 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.cause_of_injury_sup
		WHERE crrnt_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY cause_of_inj_code ORDER BY cause_of_inj_support_id) = 1
),
LKP_nature_of_injury_sup AS (
	SELECT
	nature_of_inj_support_id,
	nature_of_inj_code
	FROM (
		SELECT nature_of_inj_support_id as nature_of_inj_support_id, 
		LTRIM(RTRIM(nature_of_inj_code)) as nature_of_inj_code 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.nature_of_injury_sup
		WHERE crrnt_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY nature_of_inj_code ORDER BY nature_of_inj_support_id) = 1
),
Union AS (
	SELECT o_pol_id AS pol_id, o_RiskLocationID AS RiskLocationID, o_PolicyCoverageID AS PolicyCoverageID, o_StatisticalCoverageID AS StatisticalCoverageID, o_PremiumTransactionID AS PremiumTransactionID, o_BureauStatisticalCodeID AS BureauStatisticalCodeID, o_PremiumMasterCalculationID AS PremiumMasterCalculationID, o_PremiumMasterRunDate AS PremiumMasterRunDate, o_PolKey AS PolKey, o_pol_eff_date AS pol_eff_date, o_name AS name, o_StateCode AS StateCode, o_PremiumMasterPremium AS PremiumMasterPremium, o_EarnedPremium AS EarnedPremium, o_UnearnedPremium AS UnearnedPremium, o_DeferredPremium AS DeferredPremium, o_PremiumMasterAgencyCommissionRate AS PremiumMasterAgencyCommissionRate, o_CommissionPaid AS CommissionPaid, o_AuditNonComplianceChargePremium AS AuditNonComplianceChargePremium
	FROM EXP_Premiumdata
	UNION
	SELECT o_wc_claimant_det_id AS wc_claimant_det_id, o_claim_occurrence_id AS Claim_Occurrence_Id, o_claim_party_occurrence_id AS Claim_Party_Occurrence_Id, o_claimant_cov_det_id AS Claimant_Cov_Det_Id, o_loss_master_calculation_id AS Loss_Master_Calculation_Id, o_pol_id AS pol_id, o_RiskLocationID AS RiskLocationID, o_PolicyCoverageID AS PolicyCoverageID, o_StatisticalCoverageID AS StatisticalCoverageID, o_loss_master_run_date AS PremiumMasterRunDate, o_PolKey AS PolKey, o_PolicyEffectiveDate AS pol_eff_date, o_NameOfInsured AS name, o_StateCode AS StateCode, o_AccidentYear AS AccidentYear, o_ClaimNum AS ClaimNum, o_ClaimClosedDate AS ClaimClosedDate, o_ClaimOccurrenceStatusCode AS ClaimOccurrenceStatusCode, o_PaidLossAmt AS PaidLossAmt, o_TypeLossCode AS TypeLossCode, o_IndemnityPaymentAmount AS IndemnityPaymentAmount, o_MedicalPaymentAmount AS MedicalPaymentAmount, o_RecoveryAmount AS RecoveryAmount, o_IndemnityReserveAmount AS IndemnityReserveAmount, o_MedicalReserveAmount AS MedicalReserveAmount, o_TotalMedicalAmount AS TotalMedicalAmount, o_TotalIndemnityAmount AS TotalIndemnityAmount, cause_of_inj_support_id AS CauseOfInjSupportId, o_InjrDescLoss AS InjrDescLoss, body_part_support_id AS BodyPartSupportId, o_body_part_code AS InjrDescPartOfBody, nature_of_inj_support_id AS NatureOfInjSupportId, o_nature_of_injury_code AS InjrDescNatureOfInjr, o_InjrDescCauseOfLoss AS InjrDescCauseOfLoss, o_Classcode AS Classcode, o_OutstandingAmount AS OutstandingAmount, o_claim_trans_amt AS claim_trans_amt, o_FinancialTypeCode AS FinancialTypeCode
	FROM EXP_Lossdata
	-- Manually join with LKP_body_part_sup
	-- Manually join with LKP_cause_of_injury_sup
	-- Manually join with LKP_nature_of_injury_sup
),
EXP_SetDefaultValue AS (
	SELECT
	wc_claimant_det_id,
	-1 AS o_ClaimantId,
	Claim_Occurrence_Id,
	Claim_Party_Occurrence_Id,
	Claimant_Cov_Det_Id,
	Loss_Master_Calculation_Id,
	pol_id,
	RiskLocationID,
	PolicyCoverageID,
	StatisticalCoverageID,
	PremiumTransactionID,
	BureauStatisticalCodeID,
	PremiumMasterCalculationID,
	PremiumMasterRunDate,
	PolKey,
	pol_eff_date,
	name,
	StateCode,
	PremiumMasterPremium,
	EarnedPremium,
	UnearnedPremium,
	DeferredPremium,
	AccidentYear,
	ClaimNum,
	ClaimClosedDate,
	ClaimOccurrenceStatusCode,
	PaidLossAmt,
	TypeLossCode,
	IndemnityPaymentAmount,
	MedicalPaymentAmount,
	RecoveryAmount,
	IndemnityReserveAmount,
	MedicalReserveAmount,
	TotalMedicalAmount,
	TotalIndemnityAmount,
	CauseOfInjSupportId,
	InjrDescLoss,
	BodyPartSupportId,
	InjrDescPartOfBody,
	NatureOfInjSupportId,
	InjrDescNatureOfInjr,
	TypeDisabilitySupportId,
	InjrDescCauseOfLoss,
	PremiumMasterAgencyCommissionRate,
	CommissionPaid,
	Classcode,
	OutstandingAmount,
	FinancialTypeCode,
	claim_trans_amt,
	-- *INF*: TRUNC(@{pipeline().parameters.EXTRACTDATE},'D')
	CAST(TRUNC(@{pipeline().parameters.EXTRACTDATE}, 'DAY') AS TIMESTAMP_NTZ(0)) AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	AuditNonComplianceChargePremium
	FROM Union
),
LKP_WcrbWorkTable AS (
	SELECT
	PremiumMasterCalculationID,
	LossMasterCalculationId
	FROM (
		SELECT 
			PremiumMasterCalculationID,
			LossMasterCalculationId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.WcrbWorkTable
		WHERE PremiumMasterRunDate>=@{pipeline().parameters.STARTDATE} and PremiumMasterRunDate<@{pipeline().parameters.ENDDATE}
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumMasterCalculationID,LossMasterCalculationId ORDER BY PremiumMasterCalculationID) = 1
),
FIL_data AS (
	SELECT
	LKP_WcrbWorkTable.PremiumMasterCalculationID AS Lkp_PremiumMasterCalculationID, 
	EXP_SetDefaultValue.wc_claimant_det_id, 
	EXP_SetDefaultValue.o_ClaimantId AS ClaimantId, 
	EXP_SetDefaultValue.Claim_Occurrence_Id, 
	EXP_SetDefaultValue.Claim_Party_Occurrence_Id, 
	EXP_SetDefaultValue.Claimant_Cov_Det_Id, 
	EXP_SetDefaultValue.Loss_Master_Calculation_Id, 
	EXP_SetDefaultValue.pol_id, 
	EXP_SetDefaultValue.RiskLocationID, 
	EXP_SetDefaultValue.PolicyCoverageID, 
	EXP_SetDefaultValue.StatisticalCoverageID, 
	EXP_SetDefaultValue.PremiumTransactionID, 
	EXP_SetDefaultValue.BureauStatisticalCodeID, 
	EXP_SetDefaultValue.PremiumMasterCalculationID, 
	EXP_SetDefaultValue.PremiumMasterRunDate, 
	EXP_SetDefaultValue.PolKey, 
	EXP_SetDefaultValue.pol_eff_date, 
	EXP_SetDefaultValue.name, 
	EXP_SetDefaultValue.StateCode, 
	EXP_SetDefaultValue.PremiumMasterPremium, 
	EXP_SetDefaultValue.EarnedPremium, 
	EXP_SetDefaultValue.UnearnedPremium, 
	EXP_SetDefaultValue.DeferredPremium, 
	EXP_SetDefaultValue.AccidentYear, 
	EXP_SetDefaultValue.ClaimNum, 
	EXP_SetDefaultValue.ClaimClosedDate, 
	EXP_SetDefaultValue.ClaimOccurrenceStatusCode, 
	EXP_SetDefaultValue.PaidLossAmt, 
	EXP_SetDefaultValue.TypeLossCode, 
	EXP_SetDefaultValue.IndemnityPaymentAmount, 
	EXP_SetDefaultValue.MedicalPaymentAmount, 
	EXP_SetDefaultValue.RecoveryAmount, 
	EXP_SetDefaultValue.IndemnityReserveAmount, 
	EXP_SetDefaultValue.MedicalReserveAmount, 
	EXP_SetDefaultValue.TotalMedicalAmount, 
	EXP_SetDefaultValue.TotalIndemnityAmount, 
	EXP_SetDefaultValue.CauseOfInjSupportId, 
	EXP_SetDefaultValue.InjrDescLoss, 
	EXP_SetDefaultValue.BodyPartSupportId, 
	EXP_SetDefaultValue.InjrDescPartOfBody, 
	EXP_SetDefaultValue.NatureOfInjSupportId, 
	EXP_SetDefaultValue.InjrDescNatureOfInjr, 
	EXP_SetDefaultValue.TypeDisabilitySupportId, 
	EXP_SetDefaultValue.InjrDescCauseOfLoss, 
	EXP_SetDefaultValue.PremiumMasterAgencyCommissionRate, 
	EXP_SetDefaultValue.CommissionPaid, 
	EXP_SetDefaultValue.Classcode, 
	EXP_SetDefaultValue.OutstandingAmount, 
	EXP_SetDefaultValue.FinancialTypeCode, 
	EXP_SetDefaultValue.claim_trans_amt, 
	EXP_SetDefaultValue.o_ExtractDate AS ExtractDate, 
	EXP_SetDefaultValue.o_SourceSystemID AS SourceSystemID, 
	EXP_SetDefaultValue.o_AuditID AS AuditID, 
	EXP_SetDefaultValue.AuditNonComplianceChargePremium
	FROM EXP_SetDefaultValue
	LEFT JOIN LKP_WcrbWorkTable
	ON LKP_WcrbWorkTable.PremiumMasterCalculationID = EXP_SetDefaultValue.PremiumMasterCalculationID AND LKP_WcrbWorkTable.LossMasterCalculationId = EXP_SetDefaultValue.Loss_Master_Calculation_Id
	WHERE ISNULL(Lkp_PremiumMasterCalculationID)
),
TGT_WcrbWorkTable_Insert AS (

	------------ PRE SQL ----------
	@{pipeline().parameters.DELETE_PRESQL}
	-------------------------------


	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WcrbWorkTable
	(WCClaimantDetId, ClaimantId, ClaimOccurrenceId, ClaimPartyOccurrenceId, ClaimantCovDetId, LossMasterCalculationId, PolId, RiskLocationID, PolicyCoverageID, StatisticalCoverageID, PremiumTransactionID, BureauStatisticalCodeID, PremiumMasterCalculationID, PremiumMasterRunDate, PolKey, PolicyEffectiveDae, NameOfInsured, StateCode, WrittenPremium, EarnedPremium, UneranedPremium, DeferredPremium, AccidentYear, ClaimNum, ClaimClosedDate, ClaimOccurrenceStatusCode, PaidLossAmt, TypeLossCode, IndemnityPaymentAmount, MedicalPaymentAmount, RecoveryAmount, IndemnityReserveAmount, MedicalReserveAmount, TotalMedicalAmount, TotalIndemnityAmount, CauseOfInjSupportId, InjrDescLoss, BodyPartSupportId, InjrDescPartOfBody, NatureOfInjSupportId, InjrDescNatureOfInjr, TypeDisabilitySupportId, InjrDescCauseOfLoss, PremiumMasterAgencyCommissionRate, CommissionPaid, ClassCode, OutstandingAmount, FinancialTypeCode, ClaimTransactionAmount, ExtractDate, SourceSystemId, AuditID, AuditNonComplianceChargePremium)
	SELECT 
	wc_claimant_det_id AS WCCLAIMANTDETID, 
	CLAIMANTID, 
	Claim_Occurrence_Id AS CLAIMOCCURRENCEID, 
	Claim_Party_Occurrence_Id AS CLAIMPARTYOCCURRENCEID, 
	Claimant_Cov_Det_Id AS CLAIMANTCOVDETID, 
	Loss_Master_Calculation_Id AS LOSSMASTERCALCULATIONID, 
	pol_id AS POLID, 
	RISKLOCATIONID, 
	POLICYCOVERAGEID, 
	STATISTICALCOVERAGEID, 
	PREMIUMTRANSACTIONID, 
	BUREAUSTATISTICALCODEID, 
	PREMIUMMASTERCALCULATIONID, 
	PREMIUMMASTERRUNDATE, 
	POLKEY, 
	pol_eff_date AS POLICYEFFECTIVEDAE, 
	name AS NAMEOFINSURED, 
	STATECODE, 
	PremiumMasterPremium AS WRITTENPREMIUM, 
	EARNEDPREMIUM, 
	UnearnedPremium AS UNERANEDPREMIUM, 
	DEFERREDPREMIUM, 
	ACCIDENTYEAR, 
	CLAIMNUM, 
	CLAIMCLOSEDDATE, 
	CLAIMOCCURRENCESTATUSCODE, 
	PAIDLOSSAMT, 
	TYPELOSSCODE, 
	INDEMNITYPAYMENTAMOUNT, 
	MEDICALPAYMENTAMOUNT, 
	RECOVERYAMOUNT, 
	INDEMNITYRESERVEAMOUNT, 
	MEDICALRESERVEAMOUNT, 
	TOTALMEDICALAMOUNT, 
	TOTALINDEMNITYAMOUNT, 
	CAUSEOFINJSUPPORTID, 
	INJRDESCLOSS, 
	BODYPARTSUPPORTID, 
	INJRDESCPARTOFBODY, 
	NATUREOFINJSUPPORTID, 
	INJRDESCNATUREOFINJR, 
	TYPEDISABILITYSUPPORTID, 
	INJRDESCCAUSEOFLOSS, 
	PREMIUMMASTERAGENCYCOMMISSIONRATE, 
	COMMISSIONPAID, 
	Classcode AS CLASSCODE, 
	OUTSTANDINGAMOUNT, 
	FINANCIALTYPECODE, 
	claim_trans_amt AS CLAIMTRANSACTIONAMOUNT, 
	EXTRACTDATE, 
	SourceSystemID AS SOURCESYSTEMID, 
	AUDITID, 
	AUDITNONCOMPLIANCECHARGEPREMIUM
	FROM FIL_data
),
SQ_DCBILCommissionAuthorizationStage AS (
	DECLARE @STARTDATE DATETIME = @{pipeline().parameters.STARTDATE},
			@ENDDATE DATETIME =  @{pipeline().parameters.ENDDATE};
	
	SELECT 
	SUM(CA.AuthorizedAmount) as AuthorizedAmount,
	PT.PolicyReference as PolicyReference,
	--replace(convert(char(10), PolicyTermEffectiveDate, 101), '/', '') as PolicyTermEffectiveDate
	PolicyTermEffectiveDate,
	@ENDDATE-1 as Rundate
	FROM wc_stage..DCBILCommissionAuthorizationStage CA
	INNER JOIN wc_stage..DCBILPolicyTermStage PT ON CA.PolicyTermId = PT.PolicyTermID 
	WHERE
	CA.AuthorizationTypeCode = '@{pipeline().parameters.AUTHORIZATIONTYPECODE}'
	AND CA.CommissionSchemeReference = '@{pipeline().parameters.COMMISSIONSCHEMEREFERENCE}'
	AND CA.LastUpdatedTimestamp >= @STARTDATE
	AND CA.LastUpdatedTimestamp <  @ENDDATE
	--and ca.AuthorizationDate >= @STARTDATE
	--and ca.AuthorizationDate<  @ENDDATE
	group by pt.PolicyReference,PT.PolicyTermEffectiveDate
),
EXP_CollectColumns AS (
	SELECT
	PolicyReference,
	PolicyTermEffectiveDate,
	AuthorizedAmount,
	Rundate
	FROM SQ_DCBILCommissionAuthorizationStage
),
lkp_V2Policy_EffctiveDate AS (
	SELECT
	pol_sym,
	pol_mod,
	pol_key,
	name,
	pol_num,
	pol_eff_date
	FROM (
		select 
		 pol.pol_sym as pol_sym,
		 pol.Pol_mod as pol_mod,
		  pol.pol_key as pol_key,
		  cc.name as name ,
		  pol.pol_num as pol_num,
		 pol.pol_eff_date as pol_eff_date
		  
		  from 
		 ( select distinct pol_sym,pol_num,pol_eff_date,max(pol_key) pol_key,max(Pol_mod) Pol_mod,max(contract_cust_ak_id) contract_cust_ak_id
		from  v2.policy where source_sys_id='DCT'    group by  pol_sym,pol_num,pol_eff_date ) pol
		 join contract_customer CC	
		on CC.contract_cust_ak_id=POL.contract_cust_ak_id and CC.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_num,pol_eff_date ORDER BY pol_sym) = 1
),
EXP_DerivePolicykey_Insured AS (
	SELECT
	EXP_CollectColumns.PolicyReference,
	EXP_CollectColumns.PolicyTermEffectiveDate,
	EXP_CollectColumns.AuthorizedAmount,
	lkp_V2Policy_EffctiveDate.pol_sym AS i_pol_sym,
	lkp_V2Policy_EffctiveDate.pol_num AS i_pol_num,
	lkp_V2Policy_EffctiveDate.pol_mod AS i_pol_mod,
	lkp_V2Policy_EffctiveDate.pol_key,
	i_pol_sym || i_pol_num || i_pol_mod AS o_pol_key,
	lkp_V2Policy_EffctiveDate.name AS i_name,
	-- *INF*: SUBSTR(i_name,1,90)
	SUBSTR(i_name, 1, 90) AS o_name,
	'48' AS o_StateCode,
	-- *INF*: TO_CHAR(PolicyTermEffectiveDate,'MMDDYYYY')
	TO_CHAR(PolicyTermEffectiveDate, 'MMDDYYYY') AS o_PolicyTermEffectiveDate,
	-1 AS o_default_ID,
	-999 AS o_default_commission_ID,
	EXP_CollectColumns.Rundate,
	-- *INF*: TRUNC(@{pipeline().parameters.EXTRACTDATE},'D')
	CAST(TRUNC(@{pipeline().parameters.EXTRACTDATE}, 'DAY') AS TIMESTAMP_NTZ(0)) AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	lkp_V2Policy_EffctiveDate.pol_eff_date,
	0 AS o_dummy,
	null AS o_dummy_null
	FROM EXP_CollectColumns
	LEFT JOIN lkp_V2Policy_EffctiveDate
	ON lkp_V2Policy_EffctiveDate.pol_num = EXP_CollectColumns.PolicyReference AND lkp_V2Policy_EffctiveDate.pol_eff_date = EXP_CollectColumns.PolicyTermEffectiveDate
),
LKP_WcrbWorkTable_Commission AS (
	SELECT
	PremiumMasterCalculationID,
	LossMasterCalculationId
	FROM (
		SELECT 
			PremiumMasterCalculationID,
			LossMasterCalculationId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.WcrbWorkTable
		WHERE PremiumMasterCalculationID=-999 and LossMasterCalculationId=-999 and PremiumMasterRunDate>=@{pipeline().parameters.STARTDATE} and PremiumMasterRunDate<@{pipeline().parameters.ENDDATE}
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumMasterCalculationID,LossMasterCalculationId ORDER BY PremiumMasterCalculationID) = 1
),
FIL_data1 AS (
	SELECT
	LKP_WcrbWorkTable_Commission.PremiumMasterCalculationID AS Lkp_PremiumMasterCalculationID, 
	EXP_DerivePolicykey_Insured.o_default_ID AS wc_claimant_det_id, 
	EXP_DerivePolicykey_Insured.o_default_ID AS ClaimantId, 
	EXP_DerivePolicykey_Insured.o_default_ID AS Claim_Occurrence_Id, 
	EXP_DerivePolicykey_Insured.o_default_ID AS Claim_Party_Occurrence_Id, 
	EXP_DerivePolicykey_Insured.o_default_ID AS Claimant_Cov_Det_Id, 
	EXP_DerivePolicykey_Insured.o_default_commission_ID AS Loss_Master_Calculation_Id, 
	EXP_DerivePolicykey_Insured.o_default_ID AS pol_id, 
	EXP_DerivePolicykey_Insured.o_default_ID AS RiskLocationID, 
	EXP_DerivePolicykey_Insured.o_default_ID AS PolicyCoverageID, 
	EXP_DerivePolicykey_Insured.o_default_ID AS StatisticalCoverageID, 
	EXP_DerivePolicykey_Insured.o_default_ID AS PremiumTransactionID, 
	EXP_DerivePolicykey_Insured.o_default_ID AS BureauStatisticalCodeID, 
	EXP_DerivePolicykey_Insured.o_default_commission_ID AS PremiumMasterCalculationID, 
	EXP_DerivePolicykey_Insured.Rundate AS PremiumMasterRunDate, 
	EXP_DerivePolicykey_Insured.o_pol_key AS PolKey, 
	EXP_DerivePolicykey_Insured.o_PolicyTermEffectiveDate AS pol_eff_date, 
	EXP_DerivePolicykey_Insured.o_name AS name, 
	EXP_DerivePolicykey_Insured.o_StateCode AS StateCode, 
	EXP_DerivePolicykey_Insured.o_dummy AS PremiumMasterPremium, 
	EXP_DerivePolicykey_Insured.o_dummy AS EarnedPremium, 
	EXP_DerivePolicykey_Insured.o_dummy AS UnearnedPremium, 
	EXP_DerivePolicykey_Insured.o_dummy AS DeferredPremium, 
	EXP_DerivePolicykey_Insured.o_dummy AS PaidLossAmt, 
	EXP_DerivePolicykey_Insured.o_dummy AS IndemnityPaymentAmount, 
	EXP_DerivePolicykey_Insured.o_dummy AS MedicalPaymentAmount, 
	EXP_DerivePolicykey_Insured.o_dummy AS RecoveryAmount, 
	EXP_DerivePolicykey_Insured.o_dummy AS IndemnityReserveAmount, 
	EXP_DerivePolicykey_Insured.o_dummy AS MedicalReserveAmount, 
	EXP_DerivePolicykey_Insured.o_dummy AS TotalMedicalAmount, 
	EXP_DerivePolicykey_Insured.o_dummy AS TotalIndemnityAmount, 
	EXP_DerivePolicykey_Insured.o_default_ID AS CauseOfInjSupportId, 
	EXP_DerivePolicykey_Insured.o_dummy_null AS InjrDescLoss, 
	EXP_DerivePolicykey_Insured.o_default_ID AS BodyPartSupportId, 
	EXP_DerivePolicykey_Insured.o_dummy_null AS InjrDescPartOfBody, 
	EXP_DerivePolicykey_Insured.o_default_ID AS NatureOfInjSupportId, 
	EXP_DerivePolicykey_Insured.o_dummy_null AS InjrDescNatureOfInjr, 
	EXP_DerivePolicykey_Insured.o_default_ID AS TypeDisabilitySupportId, 
	EXP_DerivePolicykey_Insured.o_dummy_null AS InjrDescCauseOfLoss, 
	EXP_DerivePolicykey_Insured.o_dummy AS PremiumMasterAgencyCommissionRate, 
	EXP_DerivePolicykey_Insured.AuthorizedAmount AS CommissionPaid, 
	EXP_DerivePolicykey_Insured.o_dummy_null AS Classcode, 
	EXP_DerivePolicykey_Insured.o_dummy AS OutstandingAmount, 
	EXP_DerivePolicykey_Insured.o_dummy_null AS FinancialTypeCode, 
	EXP_DerivePolicykey_Insured.o_dummy AS claim_trans_amt, 
	EXP_DerivePolicykey_Insured.o_ExtractDate AS ExtractDate, 
	EXP_DerivePolicykey_Insured.o_SourceSystemID AS SourceSystemID, 
	EXP_DerivePolicykey_Insured.o_AuditID AS AuditID, 
	EXP_DerivePolicykey_Insured.o_dummy AS AuditNonComplianceChargePremium
	FROM EXP_DerivePolicykey_Insured
	LEFT JOIN LKP_WcrbWorkTable_Commission
	ON LKP_WcrbWorkTable_Commission.PremiumMasterCalculationID = EXP_DerivePolicykey_Insured.o_default_commission_ID AND LKP_WcrbWorkTable_Commission.LossMasterCalculationId = EXP_DerivePolicykey_Insured.o_default_commission_ID
	WHERE ISNULL(Lkp_PremiumMasterCalculationID)
),
TGT_WcrbWorkTable_Insert_Commision AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WcrbWorkTable
	(WCClaimantDetId, ClaimantId, ClaimOccurrenceId, ClaimPartyOccurrenceId, ClaimantCovDetId, LossMasterCalculationId, PolId, RiskLocationID, PolicyCoverageID, StatisticalCoverageID, PremiumTransactionID, BureauStatisticalCodeID, PremiumMasterCalculationID, PremiumMasterRunDate, PolKey, PolicyEffectiveDae, NameOfInsured, StateCode, WrittenPremium, EarnedPremium, UneranedPremium, DeferredPremium, AccidentYear, ClaimNum, ClaimClosedDate, ClaimOccurrenceStatusCode, PaidLossAmt, TypeLossCode, IndemnityPaymentAmount, MedicalPaymentAmount, RecoveryAmount, IndemnityReserveAmount, MedicalReserveAmount, TotalMedicalAmount, TotalIndemnityAmount, CauseOfInjSupportId, InjrDescLoss, BodyPartSupportId, InjrDescPartOfBody, NatureOfInjSupportId, InjrDescNatureOfInjr, TypeDisabilitySupportId, InjrDescCauseOfLoss, PremiumMasterAgencyCommissionRate, CommissionPaid, ClassCode, OutstandingAmount, FinancialTypeCode, ClaimTransactionAmount, ExtractDate, SourceSystemId, AuditID, AuditNonComplianceChargePremium)
	SELECT 
	wc_claimant_det_id AS WCCLAIMANTDETID, 
	CLAIMANTID, 
	Claim_Occurrence_Id AS CLAIMOCCURRENCEID, 
	Claim_Party_Occurrence_Id AS CLAIMPARTYOCCURRENCEID, 
	Claimant_Cov_Det_Id AS CLAIMANTCOVDETID, 
	Loss_Master_Calculation_Id AS LOSSMASTERCALCULATIONID, 
	pol_id AS POLID, 
	RISKLOCATIONID, 
	POLICYCOVERAGEID, 
	STATISTICALCOVERAGEID, 
	PREMIUMTRANSACTIONID, 
	BUREAUSTATISTICALCODEID, 
	PREMIUMMASTERCALCULATIONID, 
	PREMIUMMASTERRUNDATE, 
	POLKEY, 
	pol_eff_date AS POLICYEFFECTIVEDAE, 
	name AS NAMEOFINSURED, 
	STATECODE, 
	PremiumMasterPremium AS WRITTENPREMIUM, 
	EARNEDPREMIUM, 
	UnearnedPremium AS UNERANEDPREMIUM, 
	DEFERREDPREMIUM, 
	ACCIDENTYEAR, 
	CLAIMNUM, 
	CLAIMCLOSEDDATE, 
	CLAIMOCCURRENCESTATUSCODE, 
	PAIDLOSSAMT, 
	TYPELOSSCODE, 
	INDEMNITYPAYMENTAMOUNT, 
	MEDICALPAYMENTAMOUNT, 
	RECOVERYAMOUNT, 
	INDEMNITYRESERVEAMOUNT, 
	MEDICALRESERVEAMOUNT, 
	TOTALMEDICALAMOUNT, 
	TOTALINDEMNITYAMOUNT, 
	CAUSEOFINJSUPPORTID, 
	INJRDESCLOSS, 
	BODYPARTSUPPORTID, 
	INJRDESCPARTOFBODY, 
	NATUREOFINJSUPPORTID, 
	INJRDESCNATUREOFINJR, 
	TYPEDISABILITYSUPPORTID, 
	INJRDESCCAUSEOFLOSS, 
	PREMIUMMASTERAGENCYCOMMISSIONRATE, 
	COMMISSIONPAID, 
	Classcode AS CLASSCODE, 
	OUTSTANDINGAMOUNT, 
	FINANCIALTYPECODE, 
	claim_trans_amt AS CLAIMTRANSACTIONAMOUNT, 
	EXTRACTDATE, 
	SourceSystemID AS SOURCESYSTEMID, 
	AUDITID, 
	AUDITNONCOMPLIANCECHARGEPREMIUM
	FROM FIL_data1
),
WITH
SQ_loss_master_fact AS (
	DECLARE @Rundate datetime
	
	SET @Rundate= dateadd(ss,-1,dateadd(MM,Datediff(MM,0,getdate())+@{pipeline().parameters.NO_OF_MONTHS},0));
	
	
	SELECT * FROM 
	(
	SELECT 
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	sum(outstanding_amt) as outstanding_amt,
	sum(paid_loss_amt) as paid_loss_amt,
	sum(paid_exp_amt) as paid_exp_amt,
	sum(ChangeInOutstandingAmount) as ChangeInOutstandingAmount,
	sum(DirectLossPaidER) as DirectLossPaidER,
	sum(DirectLossPaidIR) as DirectLossPaidIR,
	sum(DirectALAEPaidER) as DirectALAEPaidER,
	sum(DirectALAEPaidIR) as DirectALAEPaidIR,
	sum(DirectSalvagePaid) as DirectSalvagePaid,
	sum(DirectSubrogationPaid) as DirectSubrogationPaid,
	sum(DirectOtherRecoveryPaid) as DirectOtherRecoveryPaid,
	sum(DirectOtherRecoveryLossPaid) as DirectOtherRecoveryLossPaid,
	sum(DirectOtherRecoveryALAEPaid) as DirectOtherRecoveryALAEPaid,
	
	ISNULL(sum(DirectLossOutstandingER) - lag(sum(DirectLossOutstandingER)) over(order by T.EnterpriseGroupDescription,T.StrategicProfitCenterDescription,
	T.InsuranceReferenceLegalEntityDescription,T.PolicyOfferingDescription,T.ProductDescription,T.InsuranceReferenceLineOfBusinessDescription,T.clndr_date),
	sum(DirectLossOutstandingER)) as DirectLossOutstandingER,
	
	ISNULL(sum(DirectLossOutstandingIR) - lag(sum(DirectLossOutstandingIR)) over(order by T.EnterpriseGroupDescription,T.StrategicProfitCenterDescription,
	T.InsuranceReferenceLegalEntityDescription,T.PolicyOfferingDescription,T.ProductDescription,T.InsuranceReferenceLineOfBusinessDescription,T.clndr_date),
	sum(DirectLossOutstandingIR)) as DirectLossOutstandingIR,
	
	ISNULL(sum(DirectALAEOutstandingER) - lag(sum(DirectALAEOutstandingER)) over(order by T.EnterpriseGroupDescription,T.StrategicProfitCenterDescription,
	T.InsuranceReferenceLegalEntityDescription,T.PolicyOfferingDescription,T.ProductDescription,T.InsuranceReferenceLineOfBusinessDescription,T.clndr_date),
	sum(DirectALAEOutstandingER)) as DirectALAEOutstandingER,
	
	ISNULL(sum(DirectALAEOutstandingIR) - lag(sum(DirectALAEOutstandingIR)) over(order by T.EnterpriseGroupDescription,T.StrategicProfitCenterDescription,
	T.InsuranceReferenceLegalEntityDescription,T.PolicyOfferingDescription,T.ProductDescription,T.InsuranceReferenceLineOfBusinessDescription,T.clndr_date),
	sum(DirectALAEOutstandingIR)) as DirectALAEOutstandingIR,
	
	ISNULL(sum(DirectOtherRecoveryOutstanding) - lag(sum(DirectOtherRecoveryOutstanding)) over(order by T.EnterpriseGroupDescription,T.StrategicProfitCenterDescription,
	T.InsuranceReferenceLegalEntityDescription,T.PolicyOfferingDescription,T.ProductDescription,T.InsuranceReferenceLineOfBusinessDescription,T.clndr_date),
	sum(DirectOtherRecoveryOutstanding)) as DirectOtherRecoveryOutstanding,
	
	ISNULL(sum(DirectOtherRecoveryLossOutstanding) - lag(sum(DirectOtherRecoveryLossOutstanding)) over(order by T.EnterpriseGroupDescription,T.StrategicProfitCenterDescription,
	T.InsuranceReferenceLegalEntityDescription,T.PolicyOfferingDescription,T.ProductDescription,T.InsuranceReferenceLineOfBusinessDescription,T.clndr_date),
	sum(DirectOtherRecoveryLossOutstanding)) as DirectOtherRecoveryLossOutstanding,
	
	ISNULL(sum(DirectOtherRecoveryALAEOutstanding) - lag(sum(DirectOtherRecoveryALAEOutstanding)) over(order by T.EnterpriseGroupDescription,T.StrategicProfitCenterDescription,
	T.InsuranceReferenceLegalEntityDescription,T.PolicyOfferingDescription,T.ProductDescription,T.InsuranceReferenceLineOfBusinessDescription,T.clndr_date),
	sum(DirectOtherRecoveryALAEOutstanding)) as DirectOtherRecoveryALAEOutstanding,
	
	ISNULL(sum(DirectSubroOutstanding) - lag(sum(DirectSubroOutstanding)) over(order by T.EnterpriseGroupDescription,T.StrategicProfitCenterDescription,
	T.InsuranceReferenceLegalEntityDescription,T.PolicyOfferingDescription,T.ProductDescription,T.InsuranceReferenceLineOfBusinessDescription,T.clndr_date),
	sum(DirectSubroOutstanding)) as DirectSubroOutstanding,
	
	ISNULL(sum(DirectSalvageOutstanding) - lag(sum(DirectSalvageOutstanding)) over(order by T.EnterpriseGroupDescription,T.StrategicProfitCenterDescription,
	T.InsuranceReferenceLegalEntityDescription,T.PolicyOfferingDescription,T.ProductDescription,T.InsuranceReferenceLineOfBusinessDescription,T.clndr_date),
	sum(DirectSalvageOutstanding)) as DirectSalvageOutstanding,
	
	sum(DirectLossIncurredER) as DirectLossIncurredER,
	sum(DirectLossIncurredIR) as DirectLossIncurredIR,
	sum(DirectALAEIncurredER) as DirectALAEIncurredER,
	sum(DirectALAEIncurredIR) as DirectALAEIncurredIR
	
	FROM
	(
	select 
	S.clndr_date,
	S.EnterpriseGroupDescription,
	S.StrategicProfitCenterDescription,
	S.InsuranceReferenceLegalEntityDescription,
	S.PolicyOfferingDescription,
	S.ProductDescription,
	S.InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	paid_loss_amt,
	paid_exp_amt,
	ChangeInOutstandingAmount,
	DirectLossPaidER,
	DirectLossPaidIR,
	DirectALAEPaidER,
	DirectALAEPaidIR,
	DirectSalvagePaid,
	DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	ISNULL(P.DirectLossOutstandingER,S.DirectLossOutstandingER) DirectLossOutstandingER,
	
	ISNULL(P.DirectLossOutstandingIR,S.DirectLossOutstandingIR) DirectLossOutstandingIR,
	ISNULL(P.DirectALAEOutstandingER,S.DirectALAEOutstandingER) DirectALAEOutstandingER,
	ISNULL(P.DirectALAEOutstandingIR,S.DirectALAEOutstandingIR) DirectALAEOutstandingIR,
	iSNULL(P.DirectOtherRecoveryOutstanding,S.DirectOtherRecoveryOutstanding) DirectOtherRecoveryOutstanding,
	ISNULL(P.DirectOtherRecoveryLossOutstanding,S.DirectOtherRecoveryLossOutstanding) DirectOtherRecoveryLossOutstanding,
	ISNULL(P.DirectOtherRecoveryALAEOutstanding,S.DirectOtherRecoveryALAEOutstanding) DirectOtherRecoveryALAEOutstanding,
	ISNULL(P.DirectSubroOutstanding,S.DirectSubroOutstanding) DirectSubroOutstanding,
	ISNULL(P.DirectSalvageOutstanding,S.DirectSalvageOutstanding) DirectSalvageOutstanding,
	
	DirectLossIncurredER,
	DirectLossIncurredIR,
	DirectALAEIncurredER,
	DirectALAEIncurredIR
	from
	
	--SELECT * INTO #P FROM
	(
	select cd.clndr_date as clndr_date, 
	ird.EnterpriseGroupDescription as EnterpriseGroupDescription,
	ird.StrategicProfitCenterDescription as StrategicProfitCenterDescription,
	ird.InsuranceReferenceLegalEntityDescription as InsuranceReferenceLegalEntityDescription,
	ird.PolicyOfferingDescription as PolicyOfferingDescription,
	ird.ProductDescription as ProductDescription,
	ird.InsuranceReferenceLineOfBusinessDescription as InsuranceReferenceLineOfBusinessDescription,
	sum(f.outstanding_amt) as outstanding_amt,
	sum(f.paid_loss_amt) as paid_loss_amt,
	sum(f.paid_exp_amt) as paid_exp_amt,
	sum(f.ChangeInOutstandingAmount) as ChangeInOutstandingAmount,
	sum(f.DirectLossPaidER) as DirectLossPaidER,
	sum(f.DirectLossPaidIR) as DirectLossPaidIR,
	sum(f.DirectALAEPaidER) as DirectALAEPaidER,
	sum(f.DirectALAEPaidIR) as DirectALAEPaidIR,
	sum(f.DirectSalvagePaid) as DirectSalvagePaid,
	sum(f.DirectSubrogationPaid) as DirectSubrogationPaid,
	sum(f.DirectOtherRecoveryPaid) as DirectOtherRecoveryPaid,
	sum(f.DirectOtherRecoveryLossPaid) as DirectOtherRecoveryLossPaid,
	sum(f.DirectOtherRecoveryALAEPaid) as DirectOtherRecoveryALAEPaid,
	
	
	sum(f.DirectLossOutstandingER) as DirectLossOutstandingER,
	sum(f.DirectLossOutstandingIR) as DirectLossOutstandingIR,
	sum(f.DirectALAEOutstandingER)  as DirectALAEOutstandingER,
	sum(f.DirectALAEOutstandingIR)  as DirectALAEOutstandingIR,
	sum(f.DirectOtherRecoveryOutstanding)  as DirectOtherRecoveryOutstanding,
	sum(f.DirectOtherRecoveryLossOutstanding)  as DirectOtherRecoveryLossOutstanding,
	sum(f.DirectOtherRecoveryALAEOutstanding)  as DirectOtherRecoveryALAEOutstanding,
	sum(f.DirectSubroOutstanding)  as DirectSubroOutstanding,
	sum(f.DirectSalvageOutstanding)  as DirectSalvageOutstanding,
	
	
	
	sum(f.DirectLossIncurredER) as DirectLossIncurredER,
	sum(f.DirectLossIncurredIR) as DirectLossIncurredIR,
	sum(f.DirectALAEIncurredER) as DirectALAEIncurredER,
	sum(f.DirectALAEIncurredIR) as DirectALAEIncurredIR
	
	from (SELECT 
	LMF.loss_master_run_date_id,
	LMF.InsuranceReferenceDimId,
	LMF.outstanding_amt
	,LMF.paid_loss_amt
	,LMF.paid_exp_amt
	,LMF.ChangeInOutstandingAmount,
	(case when fin.financial_type_code = 'D' and TxnDim.trans_kind_code = 'D' then  LMF.paid_loss_amt else 0 end) DirectLossPaidER,
	(case when TxnDim.trans_kind_code = 'D' then  LMF.paid_loss_amt else 0 end) DirectLossPaidIR,
	(case when fin.financial_type_code = 'E' and TxnDim.trans_kind_code = 'D' then  LMF.paid_exp_amt else 0 end) DirectALAEPaidER,
	(case when TxnDim.trans_kind_code = 'D' then  LMF.paid_exp_amt else 0 end) DirectALAEPaidIR,
	(case when fin.financial_type_code = 'S'and TxnDim.trans_kind_code = 'D'  then LMF.paid_loss_amt else 0 end ) DirectSalvagePaid, 
	(case when fin.financial_type_code = 'B'and TxnDim.trans_kind_code = 'D' then LMF.paid_loss_amt else 0 end ) DirectSubrogationPaid,
	(case when fin.financial_type_code = 'R'and TxnDim.trans_kind_code = 'D'  and TxnDim.pms_trans_code in ('75','76')  then paid_exp_amt else 0 end )+
	(case when fin.financial_type_code = 'R'and TxnDim.trans_kind_code = 'D' and fin.financial_type_code = 'R' and TxnDim.pms_trans_code in ('26', '27', '37') then  LMF.paid_loss_amt else 0 end)  
	DirectOtherRecoveryPaid, 
	(case when fin.financial_type_code = 'R' and TxnDim.trans_kind_code = 'D' and TxnDim.pms_trans_code in ('26', '27', '37') then  LMF.paid_loss_amt else 0 end )as DirectOtherRecoveryLossPaid,
	(case when fin.financial_type_code = 'R' and TxnDim.trans_kind_code = 'D' and TxnDim.pms_trans_code in ('75','76')  then paid_exp_amt else 0 end )as DirectOtherRecoveryALAEPaid,
	(Case when TxnDim.trans_kind_code ='D' and TxnDim.pms_trans_code  not in ('97','98','99') Then LMF.outstanding_amt Else 0 End) As DirectLossOutstandingER,
	-- (Case when fin.financial_type_code = 'D' and TxnDim.trans_kind_code = 'D' Then LMF.ChangeInOutstandingAmount Else 0 End) As DirectLossOutstandingER_changein,
	(Case when TxnDim.trans_kind_code = 'D' Then LMF.outstanding_amt Else 0 End) As DirectLossOutstandingIR,
	-- (Case when TxnDim.trans_kind_code = 'D' Then LMF.ChangeInOutstandingAmount Else 0 End) As DirectLossOutstandingIR_changein,
	(Case when fin.financial_type_code = 'E' and TxnDim.trans_kind_code = 'D' Then LMF.eom_unpaid_loss_adjust_exp Else 0 End) As DirectALAEOutstandingER,
	-- (Case when fin.financial_type_code = 'E' and TxnDim.trans_kind_code = 'D' Then LMF.ChangeInEOMUnpaidLossAdjustmentExpense Else 0 End) As DirectALAEOutstandingER_Changein,
	(Case when TxnDim.trans_kind_code = 'D' Then LMF.eom_unpaid_loss_adjust_exp  Else 0 End) As DirectALAEOutstandingIR,
	-- (Case when TxnDim.trans_kind_code = 'D' Then LMF.ChangeInEOMUnpaidLossAdjustmentExpense  Else 0 End) As DirectALAEOutstandingIR_Changein,
	(Case when fin.financial_type_code = 'R' and TxnDim.trans_kind_code = 'D' Then LMF.outstanding_amt Else 0 End) As DirectOtherRecoveryOutstanding,
	-- (Case when fin.financial_type_code = 'R' and TxnDim.trans_kind_code = 'D' Then LMF.ChangeInOutstandingAmount Else 0 End) As DirectOtherRecoveryOutstanding_Changein,
	(Case when fin.financial_type_code = 'R' and TxnDim.trans_kind_code = 'D' and TxnDim.trans_ctgry_code <> 'EX' Then LMF.outstanding_amt Else 0 End) As DirectOtherRecoveryLossOutstanding,
	-- (Case when fin.financial_type_code = 'R' and TxnDim.trans_kind_code = 'D' and TxnDim.trans_ctgry_code <> 'EX' Then LMF.ChangeInOutstandingAmount Else 0 End) As DirectOtherRecoveryLossOutstanding_Changein,
	(Case when fin.financial_type_code = 'R' and TxnDim.trans_kind_code = 'D' and TxnDim.trans_ctgry_code = 'EX' Then LMF.outstanding_amt Else 0 End) As DirectOtherRecoveryALAEOutstanding,
	-- (Case when fin.financial_type_code = 'R' and TxnDim.trans_kind_code = 'D' and TxnDim.trans_ctgry_code = 'EX' Then LMF.ChangeInOutstandingAmount Else 0 End) As DirectOtherRecoveryALAEOutstanding_Changein,
	(Case when fin.financial_type_code = 'B' and  TxnDim.trans_kind_code = 'D' Then LMF.outstanding_amt Else 0 End) As DirectSubroOutstanding,
	-- (Case when fin.financial_type_code = 'B' and  TxnDim.trans_kind_code = 'D' Then LMF.ChangeInOutstandingAmount Else 0 End) As DirectSubroOutstanding_Changein,
	(Case when fin.financial_type_code = 'S' and TxnDim.trans_kind_code = 'D' Then LMF.outstanding_amt Else 0 End) As DirectSalvageOutstanding,
	-- (Case when fin.financial_type_code = 'S' and TxnDim.trans_kind_code = 'D' Then LMF.ChangeInOutstandingAmount Else 0 End) As DirectSalvageOutstanding_Changein,
	(case when fin.financial_type_code = 'D' and TxnDim.trans_kind_code = 'D' then  LMF.paid_loss_amt else 0 end) +
	(Case when fin.financial_type_code = 'D' and TxnDim.trans_kind_code = 'D' Then LMF.ChangeinOutStandingAmount Else 0 End) As DirectLossIncurredER,
	(case when TxnDim.trans_kind_code = 'D' then  LMF.paid_loss_amt else 0 end) + 
	(Case when TxnDim.trans_kind_code = 'D' and fin.financial_type_code = 'D'  Then LMF.ChangeinOutStandingAmount Else 0 End) As DirectLossIncurredIR,
	(case when fin.financial_type_code = 'E' and TxnDim.trans_kind_code = 'D' then  LMF.paid_exp_amt else 0 end) + 
	(Case when fin.financial_type_code = 'E' and TxnDim.trans_kind_code = 'D' Then LMF.ChangeInEOMUnpaidLossAdjustmentExpense Else 0 End) As DirectALAEIncurredER,
	(case when TxnDim.trans_kind_code = 'D' then  LMF.paid_exp_amt else 0 end)  +
	(Case when fin.financial_type_code = 'E' and TxnDim.trans_kind_code = 'D' Then LMF.ChangeInEOMUnpaidLossAdjustmentExpense Else 0 End) As DirectALAEIncurredIR
	FROM dbo.loss_master_fact LMF 
	inner join dbo.claim_transaction_type_dim TxnDim
	on LMF.claim_trans_type_dim_id = TxnDim.claim_trans_type_dim_id
	inner join dbo.claim_financial_type_dim  fin
	on  LMF.claimfinancialtypedimid = fin.claim_financial_type_dim_id 
	) f
	join calendar_dim  cd
	on f.loss_master_run_date_id=cd.clndr_id
	join InsuranceReferenceDim ird
	on f.InsuranceReferenceDimId=ird.InsuranceReferenceDimId
	where clndr_date<=@RunDate
	and year(clndr_date) in (year(@RunDate),year(@RunDate)-1)
	group by cd.clndr_date,
	ird.EnterpriseGroupDescription,
	ird.StrategicProfitCenterDescription,
	ird.InsuranceReferenceLegalEntityDescription,
	ird.PolicyOfferingDescription,
	ird.ProductDescription,
	ird.InsuranceReferenceLineOfBusinessDescription
	) P
	
	right outer join
	
	--SELECT * INTO #S FROM
	(
	SELECT distinct cd.clndr_date
	,ird.InsuranceReferenceLineOfBusinessDescription
	,ird.ProductDescription
	,ird.PolicyOfferingDescription
	,ird.InsuranceReferenceLegalEntityDescription
	,ird.StrategicProfitCenterDescription
	,ird.EnterpriseGroupDescription
	,0 as DirectLossOutstandingER
	,0 as DirectLossOutstandingIR
	,0 as DirectALAEOutstandingER
	,0 as DirectALAEOutstandingIR
	,0 as DirectOtherRecoveryOutstanding
	,0 as DirectOtherRecoveryLossOutstanding
	,0 as DirectOtherRecoveryALAEOutstanding
	,0 as DirectSubroOutstanding
	,0 as DirectSalvageOutstanding
	from (SELECT distinct clndr_date from calendar_dim C, loss_master_fact F where C.clndr_id = F.loss_master_run_date_id  
	and clndr_date<=@RunDate and year(clndr_date) in (year(@RunDate),year(@RunDate)-1)) cd
	,InsuranceReferenceDim ird
	) S
	
	on P.clndr_date = S.clndr_date
	and P.InsuranceReferenceLineOfBusinessDescription = S.InsuranceReferenceLineOfBusinessDescription
	and P.ProductDescription = S.ProductDescription
	and P.InsuranceReferenceLegalEntityDescription = S.InsuranceReferenceLegalEntityDescription
	and P.StrategicProfitCenterDescription = S.StrategicProfitCenterDescription
	and P.EnterpriseGroupDescription = S.EnterpriseGroupDescription
	and P.PolicyOfferingDescription = S.PolicyOfferingDescription
	
	) T
	GROUP BY 
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription
	)Final_LMF
	where year(clndr_date)=year(@RunDate)
	
	--@{pipeline().parameters.WHERE_CLAUSE1}
),
AGG_loss_master_fact AS (
	SELECT
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	-- *INF*: sum(outstanding_amt)
	sum(outstanding_amt) AS o_outstanding_amt,
	paid_loss_amt,
	-- *INF*: sum(paid_loss_amt)
	sum(paid_loss_amt) AS o_paid_loss_amt,
	paid_exp_amt,
	-- *INF*: sum(paid_exp_amt)
	sum(paid_exp_amt) AS o_paid_exp_amt,
	ChangeInOutstandingAmount,
	-- *INF*: sum(ChangeInOutstandingAmount)
	sum(ChangeInOutstandingAmount) AS o_ChangeInOutstandingAmount,
	DirectLossPaidER,
	-- *INF*: sum(DirectLossPaidER)
	sum(DirectLossPaidER) AS o_DirectLossPaidER,
	DirectLossPaidIR,
	-- *INF*: sum(DirectLossPaidIR)
	sum(DirectLossPaidIR) AS o_DirectLossPaidIR,
	DirectALAEPaidER,
	-- *INF*: sum(DirectALAEPaidER)
	sum(DirectALAEPaidER) AS o_DirectALAEPaidER,
	DirectALAEPaidIR,
	-- *INF*: sum(DirectALAEPaidIR)
	sum(DirectALAEPaidIR) AS o_DirectALAEPaidIR,
	DirectSalvagePaid,
	-- *INF*: sum(DirectSalvagePaid)
	sum(DirectSalvagePaid) AS o_DirectSalvagePaid,
	DirectSubrogationPaid,
	-- *INF*: sum(DirectSubrogationPaid)
	sum(DirectSubrogationPaid) AS o_DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	-- *INF*: sum(DirectOtherRecoveryPaid)
	sum(DirectOtherRecoveryPaid) AS o_DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	-- *INF*: sum(DirectOtherRecoveryLossPaid)
	sum(DirectOtherRecoveryLossPaid) AS o_DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	-- *INF*: sum(DirectOtherRecoveryALAEPaid)
	sum(DirectOtherRecoveryALAEPaid) AS o_DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	-- *INF*: sum(DirectLossOutstandingER)
	sum(DirectLossOutstandingER) AS o_DirectLossOutstandingER,
	DirectLossOutstandingIR,
	-- *INF*: sum(DirectLossOutstandingIR)
	sum(DirectLossOutstandingIR) AS o_DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	-- *INF*: sum(DirectALAEOutstandingER)
	sum(DirectALAEOutstandingER) AS o_DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	-- *INF*: sum(DirectALAEOutstandingIR)
	sum(DirectALAEOutstandingIR) AS o_DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	-- *INF*: sum(DirectOtherRecoveryOutstanding)
	sum(DirectOtherRecoveryOutstanding) AS o_DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	-- *INF*: sum(DirectOtherRecoveryLossOutstanding)
	sum(DirectOtherRecoveryLossOutstanding) AS o_DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	-- *INF*: sum(DirectOtherRecoveryALAEOutstanding)
	sum(DirectOtherRecoveryALAEOutstanding) AS o_DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	-- *INF*: sum(DirectSubroOutstanding)
	sum(DirectSubroOutstanding) AS o_DirectSubroOutstanding,
	DirectSalvageOutstanding,
	-- *INF*: sum(DirectSalvageOutstanding)
	sum(DirectSalvageOutstanding) AS o_DirectSalvageOutstanding,
	DirectLossIncurredER,
	-- *INF*: sum(DirectLossIncurredER)
	sum(DirectLossIncurredER) AS o_DirectLossIncurredER,
	DirectLossIncurredIR,
	-- *INF*: sum(DirectLossIncurredIR)
	sum(DirectLossIncurredIR) AS o_DirectLossIncurredIR,
	DirectALAEIncurredER,
	-- *INF*: sum(DirectALAEIncurredER)
	sum(DirectALAEIncurredER) AS o_DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: sum(DirectALAEIncurredIR)
	sum(DirectALAEIncurredIR) AS o_DirectALAEIncurredIR
	FROM SQ_loss_master_fact
	GROUP BY clndr_date
),
EXP_loss_master_fact AS (
	SELECT
	'loss_master_fact' AS table_name,
	clndr_date,
	o_outstanding_amt AS outstanding_amt,
	o_paid_loss_amt AS paid_loss_amt,
	o_paid_exp_amt AS paid_exp_amt,
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount,
	o_DirectLossPaidER AS DirectLossPaidER,
	o_DirectLossPaidIR AS DirectLossPaidIR,
	o_DirectALAEPaidER AS DirectALAEPaidER,
	o_DirectALAEPaidIR AS DirectALAEPaidIR,
	o_DirectSalvagePaid AS DirectSalvagePaid,
	o_DirectSubrogationPaid AS DirectSubrogationPaid,
	o_DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid,
	o_DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid,
	o_DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid,
	o_DirectLossOutstandingER AS DirectLossOutstandingER,
	o_DirectLossOutstandingIR AS DirectLossOutstandingIR,
	o_DirectALAEOutstandingER AS DirectALAEOutstandingER,
	o_DirectALAEOutstandingIR AS DirectALAEOutstandingIR,
	o_DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding,
	o_DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding,
	o_DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding,
	o_DirectSubroOutstanding AS DirectSubroOutstanding,
	o_DirectSalvageOutstanding AS DirectSalvageOutstanding,
	o_DirectLossIncurredER AS DirectLossIncurredER,
	o_DirectLossIncurredIR AS DirectLossIncurredIR,
	o_DirectALAEIncurredER AS DirectALAEIncurredER,
	o_DirectALAEIncurredIR AS DirectALAEIncurredIR
	FROM AGG_loss_master_fact
),
SQ_vwLossMasterFact AS (
	DECLARE @Rundate datetime
	
	SET @Rundate=dateadd(ss,-1,dateadd(MM,Datediff(MM,0,getdate())+@{pipeline().parameters.NO_OF_MONTHS},0));
	
	select cd.clndr_date as clndr_date, 
	ird.EnterpriseGroupDescription as EnterpriseGroupDescription,
	ird.StrategicProfitCenterDescription as StrategicProfitCenterDescription,
	ird.InsuranceReferenceLegalEntityDescription as InsuranceReferenceLegalEntityDescription,
	ird.PolicyOfferingDescription as PolicyOfferingDescription,
	ird.ProductDescription as ProductDescription,
	ird.InsuranceReferenceLineOfBusinessDescription as InsuranceReferenceLineOfBusinessDescription,
	sum(f.outstanding_amt) as outstanding_amt,
	sum(f.paid_loss_amt) as paid_loss_amt,
	sum(f.paid_exp_amt) as paid_exp_amt,
	sum(f.ChangeInOutstandingAmount) as ChangeInOutstandingAmount,
	sum(f.DirectLossPaidER) as DirectLossPaidER,
	sum(f.DirectLossPaidIR) as DirectLossPaidIR,
	sum(f.DirectALAEPaidER) as DirectALAEPaidER,
	sum(f.DirectALAEPaidIR) as DirectALAEPaidIR,
	sum(f.DirectSalvagePaid) as DirectSalvagePaid,
	sum(f.DirectSubrogationPaid) as DirectSubrogationPaid,
	sum(f.DirectOtherRecoveryPaid) as DirectOtherRecoveryPaid,
	sum(f.DirectOtherRecoveryLossPaid) as DirectOtherRecoveryLossPaid,
	sum(f.DirectOtherRecoveryALAEPaid) as DirectOtherRecoveryALAEPaid,
	
	sum(f.DirectLossOutstandingER) as DirectLossOutstandingER,
	sum(f.DirectLossOutstandingIR) as DirectLossOutstandingIR,
	sum(f.DirectALAEOutstandingER) as DirectALAEOutstandingER,
	sum(f.DirectALAEOutstandingIR) as DirectALAEOutstandingIR,
	sum(f.DirectOtherRecoveryOutstanding) as DirectOtherRecoveryOutstanding,
	sum(f.DirectOtherRecoveryLossOutstanding) as DirectOtherRecoveryLossOutstanding,
	sum(f.DirectOtherRecoveryALAEOutstanding) as DirectOtherRecoveryALAEOutstanding,
	sum(f.DirectSubroOutstanding) as DirectSubroOutstanding,
	sum(f.DirectSalvageOutstanding) as DirectSalvageOutstanding,
	
	sum(f.DirectLossIncurredER) as DirectLossIncurredER,
	sum(f.DirectLossIncurredIR) as DirectLossIncurredIR,
	sum(f.DirectALAEIncurredER) as DirectALAEIncurredER,
	sum(f.DirectALAEIncurredIR) as DirectALAEIncurredIR
	
	from vwLossMasterFact f
	join calendar_dim  cd
	on f.loss_master_run_date_id=cd.clndr_id
	join InsuranceReferenceDim ird
	on f.InsuranceReferenceDimId=ird.InsuranceReferenceDimId
	where cd.clndr_date<=@Rundate
	and year(cd.clndr_date)=year(@Rundate)
	@{pipeline().parameters.WHERE_CLAUSE2}
	group by cd.clndr_date,
	ird.EnterpriseGroupDescription,
	ird.StrategicProfitCenterDescription,
	ird.InsuranceReferenceLegalEntityDescription,
	ird.PolicyOfferingDescription,
	ird.ProductDescription,
	ird.InsuranceReferenceLineOfBusinessDescription
	order by cd.clndr_date,
	ird.EnterpriseGroupDescription,
	ird.StrategicProfitCenterDescription,
	ird.InsuranceReferenceLegalEntityDescription,
	ird.PolicyOfferingDescription,
	ird.ProductDescription,
	ird.InsuranceReferenceLineOfBusinessDescription
),
AGG_vwLossMasterFact AS (
	SELECT
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	-- *INF*: sum(outstanding_amt)
	sum(outstanding_amt) AS o_outstanding_amt,
	paid_loss_amt,
	-- *INF*: sum(paid_loss_amt)
	sum(paid_loss_amt) AS o_paid_loss_amt,
	paid_exp_amt,
	-- *INF*: sum(paid_exp_amt)
	sum(paid_exp_amt) AS o_paid_exp_amt,
	ChangeInOutstandingAmount,
	-- *INF*: sum(ChangeInOutstandingAmount)
	sum(ChangeInOutstandingAmount) AS o_ChangeInOutstandingAmount,
	DirectLossPaidER,
	-- *INF*: sum(DirectLossPaidER)
	sum(DirectLossPaidER) AS o_DirectLossPaidER,
	DirectLossPaidIR,
	-- *INF*: sum(DirectLossPaidIR)
	sum(DirectLossPaidIR) AS o_DirectLossPaidIR,
	DirectALAEPaidER,
	-- *INF*: sum(DirectALAEPaidER)
	sum(DirectALAEPaidER) AS o_DirectALAEPaidER,
	DirectALAEPaidIR,
	-- *INF*: sum(DirectALAEPaidIR)
	sum(DirectALAEPaidIR) AS o_DirectALAEPaidIR,
	DirectSalvagePaid,
	-- *INF*: sum(DirectSalvagePaid)
	sum(DirectSalvagePaid) AS o_DirectSalvagePaid,
	DirectSubrogationPaid,
	-- *INF*: sum(DirectSubrogationPaid)
	sum(DirectSubrogationPaid) AS o_DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	-- *INF*: sum(DirectOtherRecoveryPaid)
	sum(DirectOtherRecoveryPaid) AS o_DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	-- *INF*: sum(DirectOtherRecoveryLossPaid)
	sum(DirectOtherRecoveryLossPaid) AS o_DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	-- *INF*: sum(DirectOtherRecoveryALAEPaid)
	sum(DirectOtherRecoveryALAEPaid) AS o_DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	-- *INF*: sum(DirectLossOutstandingER)
	sum(DirectLossOutstandingER) AS o_DirectLossOutstandingER,
	DirectLossOutstandingIR,
	-- *INF*: sum(DirectLossOutstandingIR)
	sum(DirectLossOutstandingIR) AS o_DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	-- *INF*: sum(DirectALAEOutstandingER)
	sum(DirectALAEOutstandingER) AS o_DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	-- *INF*: sum(DirectALAEOutstandingIR)
	sum(DirectALAEOutstandingIR) AS o_DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	-- *INF*: sum(DirectOtherRecoveryOutstanding)
	sum(DirectOtherRecoveryOutstanding) AS o_DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	-- *INF*: sum(DirectOtherRecoveryLossOutstanding)
	sum(DirectOtherRecoveryLossOutstanding) AS o_DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	-- *INF*: sum(DirectOtherRecoveryALAEOutstanding)
	sum(DirectOtherRecoveryALAEOutstanding) AS o_DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	-- *INF*: sum(DirectSubroOutstanding)
	sum(DirectSubroOutstanding) AS o_DirectSubroOutstanding,
	DirectSalvageOutstanding,
	-- *INF*: sum(DirectSalvageOutstanding)
	sum(DirectSalvageOutstanding) AS o_DirectSalvageOutstanding,
	DirectLossIncurredER,
	-- *INF*: sum(DirectLossIncurredER)
	sum(DirectLossIncurredER) AS o_DirectLossIncurredER,
	DirectLossIncurredIR,
	-- *INF*: sum(DirectLossIncurredIR)
	sum(DirectLossIncurredIR) AS o_DirectLossIncurredIR,
	DirectALAEIncurredER,
	-- *INF*: sum(DirectALAEIncurredER)
	sum(DirectALAEIncurredER) AS o_DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: sum(DirectALAEIncurredIR)
	sum(DirectALAEIncurredIR) AS o_DirectALAEIncurredIR
	FROM SQ_vwLossMasterFact
	GROUP BY clndr_date
),
EXP_vwLossMasterFact AS (
	SELECT
	'vwLossMasterFact' AS table_name,
	clndr_date,
	o_outstanding_amt AS outstanding_amt,
	o_paid_loss_amt AS paid_loss_amt,
	o_paid_exp_amt AS paid_exp_amt,
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount,
	o_DirectLossPaidER AS DirectLossPaidER,
	o_DirectLossPaidIR AS DirectLossPaidIR,
	o_DirectALAEPaidER AS DirectALAEPaidER,
	o_DirectALAEPaidIR AS DirectALAEPaidIR,
	o_DirectSalvagePaid AS DirectSalvagePaid,
	o_DirectSubrogationPaid AS DirectSubrogationPaid,
	o_DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid,
	o_DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid,
	o_DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid,
	o_DirectLossOutstandingER AS DirectLossOutstandingER,
	o_DirectLossOutstandingIR AS DirectLossOutstandingIR,
	o_DirectALAEOutstandingER AS DirectALAEOutstandingER,
	o_DirectALAEOutstandingIR AS DirectALAEOutstandingIR,
	o_DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding,
	o_DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding,
	o_DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding,
	o_DirectSubroOutstanding AS DirectSubroOutstanding,
	o_DirectSalvageOutstanding AS DirectSalvageOutstanding,
	o_DirectLossIncurredER AS DirectLossIncurredER,
	o_DirectLossIncurredIR AS DirectLossIncurredIR,
	o_DirectALAEIncurredER AS DirectALAEIncurredER,
	o_DirectALAEIncurredIR AS DirectALAEIncurredIR
	FROM AGG_vwLossMasterFact
),
JNR_loss_master_fact_vwLossMasterFact AS (SELECT
	EXP_loss_master_fact.table_name, 
	EXP_loss_master_fact.clndr_date, 
	EXP_loss_master_fact.outstanding_amt, 
	EXP_loss_master_fact.paid_loss_amt, 
	EXP_loss_master_fact.paid_exp_amt, 
	EXP_loss_master_fact.ChangeInOutstandingAmount, 
	EXP_loss_master_fact.DirectLossPaidER, 
	EXP_loss_master_fact.DirectLossPaidIR, 
	EXP_loss_master_fact.DirectALAEPaidER, 
	EXP_loss_master_fact.DirectALAEPaidIR, 
	EXP_loss_master_fact.DirectSalvagePaid, 
	EXP_loss_master_fact.DirectSubrogationPaid, 
	EXP_loss_master_fact.DirectOtherRecoveryPaid, 
	EXP_loss_master_fact.DirectOtherRecoveryLossPaid, 
	EXP_loss_master_fact.DirectOtherRecoveryALAEPaid, 
	EXP_loss_master_fact.DirectLossOutstandingER, 
	EXP_loss_master_fact.DirectLossOutstandingIR, 
	EXP_loss_master_fact.DirectALAEOutstandingER, 
	EXP_loss_master_fact.DirectALAEOutstandingIR, 
	EXP_loss_master_fact.DirectOtherRecoveryOutstanding, 
	EXP_loss_master_fact.DirectOtherRecoveryLossOutstanding, 
	EXP_loss_master_fact.DirectOtherRecoveryALAEOutstanding, 
	EXP_loss_master_fact.DirectSubroOutstanding, 
	EXP_loss_master_fact.DirectSalvageOutstanding, 
	EXP_loss_master_fact.DirectLossIncurredER, 
	EXP_loss_master_fact.DirectLossIncurredIR, 
	EXP_loss_master_fact.DirectALAEIncurredER, 
	EXP_loss_master_fact.DirectALAEIncurredIR, 
	EXP_vwLossMasterFact.table_name AS table_name1, 
	EXP_vwLossMasterFact.clndr_date AS clndr_date1, 
	EXP_vwLossMasterFact.outstanding_amt AS outstanding_amt1, 
	EXP_vwLossMasterFact.paid_loss_amt AS paid_loss_amt1, 
	EXP_vwLossMasterFact.paid_exp_amt AS paid_exp_amt1, 
	EXP_vwLossMasterFact.ChangeInOutstandingAmount AS ChangeInOutstandingAmount1, 
	EXP_vwLossMasterFact.DirectLossPaidER AS DirectLossPaidER1, 
	EXP_vwLossMasterFact.DirectLossPaidIR AS DirectLossPaidIR1, 
	EXP_vwLossMasterFact.DirectALAEPaidER AS DirectALAEPaidER1, 
	EXP_vwLossMasterFact.DirectALAEPaidIR AS DirectALAEPaidIR1, 
	EXP_vwLossMasterFact.DirectSalvagePaid AS DirectSalvagePaid1, 
	EXP_vwLossMasterFact.DirectSubrogationPaid AS DirectSubrogationPaid1, 
	EXP_vwLossMasterFact.DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid1, 
	EXP_vwLossMasterFact.DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid1, 
	EXP_vwLossMasterFact.DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid1, 
	EXP_vwLossMasterFact.DirectLossOutstandingER AS DirectLossOutstandingER1, 
	EXP_vwLossMasterFact.DirectLossOutstandingIR AS DirectLossOutstandingIR1, 
	EXP_vwLossMasterFact.DirectALAEOutstandingER AS DirectALAEOutstandingER1, 
	EXP_vwLossMasterFact.DirectALAEOutstandingIR AS DirectALAEOutstandingIR1, 
	EXP_vwLossMasterFact.DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding1, 
	EXP_vwLossMasterFact.DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding1, 
	EXP_vwLossMasterFact.DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding1, 
	EXP_vwLossMasterFact.DirectSubroOutstanding AS DirectSubroOutstanding1, 
	EXP_vwLossMasterFact.DirectSalvageOutstanding AS DirectSalvageOutstanding1, 
	EXP_vwLossMasterFact.DirectLossIncurredER AS DirectLossIncurredER1, 
	EXP_vwLossMasterFact.DirectLossIncurredIR AS DirectLossIncurredIR1, 
	EXP_vwLossMasterFact.DirectALAEIncurredER AS DirectALAEIncurredER1, 
	EXP_vwLossMasterFact.DirectALAEIncurredIR AS DirectALAEIncurredIR1
	FROM EXP_loss_master_fact
	INNER JOIN EXP_vwLossMasterFact
	ON EXP_vwLossMasterFact.clndr_date = EXP_loss_master_fact.clndr_date
),
EXP_loss_master_fact_vwLossMasterFact AS (
	SELECT
	table_name,
	clndr_date,
	outstanding_amt,
	paid_loss_amt,
	paid_exp_amt,
	ChangeInOutstandingAmount,
	DirectLossPaidER,
	DirectLossPaidIR,
	DirectALAEPaidER,
	DirectALAEPaidIR,
	DirectSalvagePaid,
	DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	DirectSalvageOutstanding,
	DirectLossIncurredER,
	DirectLossIncurredIR,
	DirectALAEIncurredER,
	DirectALAEIncurredIR,
	table_name1,
	clndr_date1,
	outstanding_amt1,
	paid_loss_amt1,
	paid_exp_amt1,
	ChangeInOutstandingAmount1,
	DirectLossPaidER1,
	DirectLossPaidIR1,
	DirectALAEPaidER1,
	DirectALAEPaidIR1,
	DirectSalvagePaid1,
	DirectSubrogationPaid1,
	DirectOtherRecoveryPaid1,
	DirectOtherRecoveryLossPaid1,
	DirectOtherRecoveryALAEPaid1,
	DirectLossOutstandingER1,
	DirectLossOutstandingIR1,
	DirectALAEOutstandingER1,
	DirectALAEOutstandingIR1,
	DirectOtherRecoveryOutstanding1,
	DirectOtherRecoveryLossOutstanding1,
	DirectOtherRecoveryALAEOutstanding1,
	DirectSubroOutstanding1,
	DirectSalvageOutstanding1,
	DirectLossIncurredER1,
	DirectLossIncurredIR1,
	DirectALAEIncurredER1,
	DirectALAEIncurredIR1,
	'Difference' AS table_name11,
	clndr_date AS clndr_date11,
	outstanding_amt-outstanding_amt1 AS outstanding_amt11,
	paid_loss_amt-paid_loss_amt1 AS paid_loss_amt11,
	paid_exp_amt-paid_exp_amt1 AS paid_exp_amt11,
	ChangeInOutstandingAmount-ChangeInOutstandingAmount1 AS ChangeInOutstandingAmount11,
	DirectLossPaidER-DirectLossPaidER1 AS DirectLossPaidER11,
	DirectLossPaidIR-DirectLossPaidIR1 AS DirectLossPaidIR11,
	DirectALAEPaidER-DirectALAEPaidER1 AS DirectALAEPaidER11,
	DirectALAEPaidIR-DirectALAEPaidIR1 AS DirectALAEPaidIR11,
	DirectSalvagePaid-DirectSalvagePaid1 AS DirectSalvagePaid11,
	DirectSubrogationPaid-DirectSubrogationPaid1 AS DirectSubrogationPaid11,
	DirectOtherRecoveryPaid-DirectOtherRecoveryPaid1 AS DirectOtherRecoveryPaid11,
	DirectOtherRecoveryLossPaid-DirectOtherRecoveryLossPaid1 AS DirectOtherRecoveryLossPaid11,
	DirectOtherRecoveryALAEPaid-DirectOtherRecoveryALAEPaid1 AS DirectOtherRecoveryALAEPaid11,
	DirectLossOutstandingER-DirectLossOutstandingER1 AS DirectLossOutstandingER11,
	DirectLossOutstandingIR-DirectLossOutstandingIR1 AS DirectLossOutstandingIR11,
	DirectALAEOutstandingER-DirectALAEOutstandingER1 AS DirectALAEOutstandingER11,
	DirectALAEOutstandingIR-DirectALAEOutstandingIR1 AS DirectALAEOutstandingIR11,
	DirectOtherRecoveryOutstanding-DirectOtherRecoveryOutstanding1 AS DirectOtherRecoveryOutstanding11,
	DirectOtherRecoveryLossOutstanding-DirectOtherRecoveryLossOutstanding1 AS DirectOtherRecoveryLossOutstanding11,
	DirectOtherRecoveryALAEOutstanding-DirectOtherRecoveryALAEOutstanding1 AS DirectOtherRecoveryALAEOutstanding11,
	DirectSubroOutstanding-DirectSubroOutstanding1 AS DirectSubroOutstanding11,
	DirectSalvageOutstanding-DirectSalvageOutstanding1 AS DirectSalvageOutstanding11,
	DirectLossIncurredER-DirectLossIncurredER1 AS DirectLossIncurredER11,
	DirectLossIncurredIR-DirectLossIncurredIR1 AS DirectLossIncurredIR11,
	DirectALAEIncurredER-DirectALAEIncurredER1 AS DirectALAEIncurredER11,
	DirectALAEIncurredIR-DirectALAEIncurredIR1 AS DirectALAEIncurredIR11
	FROM JNR_loss_master_fact_vwLossMasterFact
),
Union AS (
	SELECT table_name, clndr_date, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR
	FROM 
	UNION
	SELECT table_name1 AS table_name, clndr_date1 AS clndr_date, outstanding_amt1 AS outstanding_amt, paid_loss_amt1 AS paid_loss_amt, paid_exp_amt1 AS paid_exp_amt, ChangeInOutstandingAmount1 AS ChangeInOutstandingAmount, DirectLossPaidER1 AS DirectLossPaidER, DirectLossPaidIR1 AS DirectLossPaidIR, DirectALAEPaidER1 AS DirectALAEPaidER, DirectALAEPaidIR1 AS DirectALAEPaidIR, DirectSalvagePaid1 AS DirectSalvagePaid, DirectSubrogationPaid1 AS DirectSubrogationPaid, DirectOtherRecoveryPaid1 AS DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid1 AS DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid1 AS DirectOtherRecoveryALAEPaid, DirectLossOutstandingER1 AS DirectLossOutstandingER, DirectLossOutstandingIR1 AS DirectLossOutstandingIR, DirectALAEOutstandingER1 AS DirectALAEOutstandingER, DirectALAEOutstandingIR1 AS DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding1 AS DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding1 AS DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding1 AS DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding1 AS DirectSubroOutstanding, DirectSalvageOutstanding1 AS DirectSalvageOutstanding, DirectLossIncurredER1 AS DirectLossIncurredER, DirectLossIncurredIR1 AS DirectLossIncurredIR, DirectALAEIncurredER1 AS DirectALAEIncurredER, DirectALAEIncurredIR1 AS DirectALAEIncurredIR
	FROM 
	UNION
	SELECT table_name11 AS table_name, clndr_date11 AS clndr_date, outstanding_amt11 AS outstanding_amt, paid_loss_amt11 AS paid_loss_amt, paid_exp_amt11 AS paid_exp_amt, ChangeInOutstandingAmount11 AS ChangeInOutstandingAmount, DirectLossPaidER11 AS DirectLossPaidER, DirectLossPaidIR11 AS DirectLossPaidIR, DirectALAEPaidER11 AS DirectALAEPaidER, DirectALAEPaidIR11 AS DirectALAEPaidIR, DirectSalvagePaid11 AS DirectSalvagePaid, DirectSubrogationPaid11 AS DirectSubrogationPaid, DirectOtherRecoveryPaid11 AS DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid11 AS DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid11 AS DirectOtherRecoveryALAEPaid, DirectLossOutstandingER11 AS DirectLossOutstandingER, DirectLossOutstandingIR11 AS DirectLossOutstandingIR, DirectALAEOutstandingER11 AS DirectALAEOutstandingER, DirectALAEOutstandingIR11 AS DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding11 AS DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding11 AS DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding11 AS DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding11 AS DirectSubroOutstanding, DirectSalvageOutstanding11 AS DirectSalvageOutstanding, DirectLossIncurredER11 AS DirectLossIncurredER, DirectLossIncurredIR11 AS DirectLossIncurredIR, DirectALAEIncurredER11 AS DirectALAEIncurredER, DirectALAEIncurredIR11 AS DirectALAEIncurredIR
	FROM 
),
EXPTRANS AS (
	SELECT
	'Balance loss_master_fact to vwLossMasterFact' AS BalancingDescription,
	table_name AS Table_name,
	clndr_date,
	outstanding_amt,
	paid_loss_amt,
	paid_exp_amt,
	ChangeInOutstandingAmount,
	DirectLossPaidER,
	DirectLossPaidIR,
	DirectALAEPaidER,
	DirectALAEPaidIR,
	DirectSalvagePaid,
	DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	DirectSalvageOutstanding,
	DirectLossIncurredER,
	DirectLossIncurredIR,
	DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: DECODE(TRUE,
	-- Table_name='loss_master_fact',1,
	-- Table_name='vwLossMasterFact',2,
	-- Table_name='Difference',3
	-- )
	DECODE(TRUE,
		Table_name = 'loss_master_fact', 1,
		Table_name = 'vwLossMasterFact', 2,
		Table_name = 'Difference', 3) AS OrderInd
	FROM Union
),
SRTTRANS AS (
	SELECT
	BalancingDescription, 
	Table_name AS table_name, 
	clndr_date, 
	outstanding_amt, 
	paid_loss_amt, 
	paid_exp_amt, 
	ChangeInOutstandingAmount, 
	DirectLossPaidER, 
	DirectLossPaidIR, 
	DirectALAEPaidER, 
	DirectALAEPaidIR, 
	DirectSalvagePaid, 
	DirectSubrogationPaid, 
	DirectOtherRecoveryPaid, 
	DirectOtherRecoveryLossPaid, 
	DirectOtherRecoveryALAEPaid, 
	DirectLossOutstandingER, 
	DirectLossOutstandingIR, 
	DirectALAEOutstandingER, 
	DirectALAEOutstandingIR, 
	DirectOtherRecoveryOutstanding, 
	DirectOtherRecoveryLossOutstanding, 
	DirectOtherRecoveryALAEOutstanding, 
	DirectSubroOutstanding, 
	DirectSalvageOutstanding, 
	DirectLossIncurredER, 
	DirectLossIncurredIR, 
	DirectALAEIncurredER, 
	DirectALAEIncurredIR, 
	OrderInd
	FROM EXPTRANS
	ORDER BY clndr_date ASC, OrderInd ASC
),
SQ_claim_loss_transaction_fact AS (
	DECLARE @Rundate datetime
	
	SET @Rundate=dateadd(ss,-1,dateadd(MM,Datediff(MM,0,getdate())+@{pipeline().parameters.NO_OF_MONTHS},0));
	
	select 
	DATEADD(d,-1,DATEADD(mm, DATEDIFF(m,0,ct.trans_date)+1,0)) as clndr_date,
	ird.EnterpriseGroupDescription AS EnterpriseGroupDescription,
	ird.StrategicProfitCenterDescription AS StrategicProfitCenterDescription,
	ird.InsuranceReferenceLegalEntityDescription AS InsuranceReferenceLegalEntityDescription,
	ird.PolicyOfferingDescription AS PolicyOfferingDescription,
	ird.ProductDescription AS ProductDescription,
	ird.InsuranceReferenceLineOfBusinessDescription AS InsuranceReferenceLineOfBusinessDescription,
	SUM(direct_loss_paid_excluding_recoveries) AS DirectLossPaidER,
	SUM(direct_loss_paid_including_recoveries) AS DirectLossPaidIR,
	SUM(direct_alae_paid_excluding_recoveries) AS DirectALAEPaidER,
	SUM(direct_alae_paid_including_recoveries) AS DirectALAEPaidIR,
	SUM(direct_salvage_paid) AS DirectSalvagePaid,
	SUM(direct_subrogation_paid) AS DirectSubrogationPaid,
	SUM(direct_other_recovery_paid) AS DirectOtherRecoveryPaid,
	SUM(direct_other_recovery_loss_paid) AS DirectOtherRecoveryLossPaid,
	SUM(direct_other_recovery_alae_paid) AS DirectOtherRecoveryALAEPaid,
	SUM(direct_loss_outstanding_excluding_recoveries) AS DirectLossOutstandingER,
	SUM(direct_loss_outstanding_including_recoveries) AS DirectLossOutstandingIR,
	SUM(direct_alae_outstanding_excluding_recoveries) AS DirectALAEOutstandingER,
	SUM(direct_alae_outstanding_including_recoveries) AS DirectALAEOutstandingIR,
	SUM(direct_other_recovery_outstanding) AS DirectOtherRecoveryOutstanding,
	SUM(direct_other_recovery_loss_outstanding) AS DirectOtherRecoveryLossOutstanding,
	SUM(direct_other_recovery_alae_outstanding) AS DirectOtherRecoveryALAEOutstanding,
	SUM(direct_subrogation_outstanding) AS DirectSubroOutstanding,
	SUM(direct_salvage_outstanding) AS DirectSalvageOutstanding,
	SUM(direct_loss_incurred_excluding_recoveries) AS DirectLossIncurredER,
	SUM(direct_loss_incurred_including_recoveries) AS DirectLossIncurredIR,
	SUM(direct_alae_incurred_excluding_recoveries) AS DirectALAEIncurredER,
	SUM(direct_alae_incurred_including_recoveries) AS DirectALAEIncurredIR
	from claim_loss_transaction_fact f
	join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_DATABASE_OWNER}.vw_claim_transaction ct
	on f.edw_claim_trans_pk_id=ct.claim_trans_id
	join InsuranceReferenceDim ird
	on f.InsuranceReferenceDimId=ird.InsuranceReferenceDimId
	where DATEADD(d,-1,DATEADD(mm, DATEDIFF(m,0,ct.trans_date)+1,0))<=@Rundate
	and year(DATEADD(d,-1,DATEADD(mm, DATEDIFF(m,0,ct.trans_date)+1,0)))=year(@Rundate)
	@{pipeline().parameters.WHERE_CLAUSE3}
	group by DATEADD(d,-1,DATEADD(mm, DATEDIFF(m,0,ct.trans_date)+1,0)),
	ird.EnterpriseGroupDescription,
	ird.StrategicProfitCenterDescription,
	ird.InsuranceReferenceLegalEntityDescription,
	ird.PolicyOfferingDescription,
	ird.ProductDescription,
	ird.InsuranceReferenceLineOfBusinessDescription
	order by DATEADD(d,-1,DATEADD(mm, DATEDIFF(m,0,ct.trans_date)+1,0)),
	ird.EnterpriseGroupDescription,
	ird.StrategicProfitCenterDescription,
	ird.InsuranceReferenceLegalEntityDescription,
	ird.PolicyOfferingDescription,
	ird.ProductDescription,
	ird.InsuranceReferenceLineOfBusinessDescription
),
AGG_claim_loss_transaction_fact AS (
	SELECT
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	DirectLossPaidER,
	-- *INF*: sum(DirectLossPaidER)
	sum(DirectLossPaidER) AS o_DirectLossPaidER,
	DirectLossPaidIR,
	-- *INF*: sum(DirectLossPaidIR)
	sum(DirectLossPaidIR) AS o_DirectLossPaidIR,
	DirectALAEPaidER,
	-- *INF*: sum(DirectALAEPaidER)
	sum(DirectALAEPaidER) AS o_DirectALAEPaidER,
	DirectALAEPaidIR,
	-- *INF*: sum(DirectALAEPaidIR)
	sum(DirectALAEPaidIR) AS o_DirectALAEPaidIR,
	DirectSalvagePaid,
	-- *INF*: sum(DirectSalvagePaid)
	sum(DirectSalvagePaid) AS o_DirectSalvagePaid,
	DirectSubrogationPaid,
	-- *INF*: sum(DirectSubrogationPaid)
	sum(DirectSubrogationPaid) AS o_DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	-- *INF*: sum(DirectOtherRecoveryPaid)
	sum(DirectOtherRecoveryPaid) AS o_DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	-- *INF*: sum(DirectOtherRecoveryLossPaid)
	sum(DirectOtherRecoveryLossPaid) AS o_DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	-- *INF*: sum(DirectOtherRecoveryALAEPaid)
	sum(DirectOtherRecoveryALAEPaid) AS o_DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	-- *INF*: sum(DirectLossOutstandingER)
	sum(DirectLossOutstandingER) AS o_DirectLossOutstandingER,
	DirectLossOutstandingIR,
	-- *INF*: sum(DirectLossOutstandingIR)
	sum(DirectLossOutstandingIR) AS o_DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	-- *INF*: sum(DirectALAEOutstandingER)
	sum(DirectALAEOutstandingER) AS o_DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	-- *INF*: sum(DirectALAEOutstandingIR)
	sum(DirectALAEOutstandingIR) AS o_DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	-- *INF*: sum(DirectOtherRecoveryOutstanding)
	sum(DirectOtherRecoveryOutstanding) AS o_DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	-- *INF*: sum(DirectOtherRecoveryLossOutstanding)
	sum(DirectOtherRecoveryLossOutstanding) AS o_DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	-- *INF*: sum(DirectOtherRecoveryALAEOutstanding)
	sum(DirectOtherRecoveryALAEOutstanding) AS o_DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	-- *INF*: sum(DirectSubroOutstanding)
	sum(DirectSubroOutstanding) AS o_DirectSubroOutstanding,
	DirectSalvageOutstanding,
	-- *INF*: sum(DirectSalvageOutstanding)
	sum(DirectSalvageOutstanding) AS o_DirectSalvageOutstanding,
	DirectLossIncurredER,
	-- *INF*: sum(DirectLossIncurredER)
	sum(DirectLossIncurredER) AS o_DirectLossIncurredER,
	DirectLossIncurredIR,
	-- *INF*: sum(DirectLossIncurredIR)
	sum(DirectLossIncurredIR) AS o_DirectLossIncurredIR,
	DirectALAEIncurredER,
	-- *INF*: sum(DirectALAEIncurredER)
	sum(DirectALAEIncurredER) AS o_DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: sum(DirectALAEIncurredIR)
	sum(DirectALAEIncurredIR) AS o_DirectALAEIncurredIR
	FROM SQ_claim_loss_transaction_fact
	GROUP BY clndr_date
),
EXP_claim_loss_transaction_fact AS (
	SELECT
	'claim_loss_transaction_fact' AS table_name,
	clndr_date,
	'N/A' AS outstanding_amt,
	'N/A' AS paid_loss_amt,
	'N/A' AS paid_exp_amt,
	'N/A' AS ChangeInOutstandingAmount,
	o_DirectLossPaidER AS DirectLossPaidER,
	o_DirectLossPaidIR AS DirectLossPaidIR,
	o_DirectALAEPaidER AS DirectALAEPaidER,
	o_DirectALAEPaidIR AS DirectALAEPaidIR,
	o_DirectSalvagePaid AS DirectSalvagePaid,
	o_DirectSubrogationPaid AS DirectSubrogationPaid,
	o_DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid,
	o_DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid,
	o_DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid,
	o_DirectLossOutstandingER AS DirectLossOutstandingER,
	o_DirectLossOutstandingIR AS DirectLossOutstandingIR,
	o_DirectALAEOutstandingER AS DirectALAEOutstandingER,
	o_DirectALAEOutstandingIR AS DirectALAEOutstandingIR,
	o_DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding,
	o_DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding,
	o_DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding,
	o_DirectSubroOutstanding AS DirectSubroOutstanding,
	o_DirectSalvageOutstanding AS DirectSalvageOutstanding,
	o_DirectLossIncurredER AS DirectLossIncurredER,
	o_DirectLossIncurredIR AS DirectLossIncurredIR,
	o_DirectALAEIncurredER AS DirectALAEIncurredER,
	o_DirectALAEIncurredIR AS DirectALAEIncurredIR
	FROM AGG_claim_loss_transaction_fact
),
JNR_loss_master_fact_claim_loss_transaction_fact AS (SELECT
	EXP_loss_master_fact.table_name, 
	EXP_loss_master_fact.clndr_date, 
	EXP_loss_master_fact.outstanding_amt, 
	EXP_loss_master_fact.paid_loss_amt, 
	EXP_loss_master_fact.paid_exp_amt, 
	EXP_loss_master_fact.ChangeInOutstandingAmount, 
	EXP_loss_master_fact.DirectLossPaidER, 
	EXP_loss_master_fact.DirectLossPaidIR, 
	EXP_loss_master_fact.DirectALAEPaidER, 
	EXP_loss_master_fact.DirectALAEPaidIR, 
	EXP_loss_master_fact.DirectSalvagePaid, 
	EXP_loss_master_fact.DirectSubrogationPaid, 
	EXP_loss_master_fact.DirectOtherRecoveryPaid, 
	EXP_loss_master_fact.DirectOtherRecoveryLossPaid, 
	EXP_loss_master_fact.DirectOtherRecoveryALAEPaid, 
	EXP_loss_master_fact.DirectLossOutstandingER, 
	EXP_loss_master_fact.DirectLossOutstandingIR, 
	EXP_loss_master_fact.DirectALAEOutstandingER, 
	EXP_loss_master_fact.DirectALAEOutstandingIR, 
	EXP_loss_master_fact.DirectOtherRecoveryOutstanding, 
	EXP_loss_master_fact.DirectOtherRecoveryLossOutstanding, 
	EXP_loss_master_fact.DirectOtherRecoveryALAEOutstanding, 
	EXP_loss_master_fact.DirectSubroOutstanding, 
	EXP_loss_master_fact.DirectSalvageOutstanding, 
	EXP_loss_master_fact.DirectLossIncurredER, 
	EXP_loss_master_fact.DirectLossIncurredIR, 
	EXP_loss_master_fact.DirectALAEIncurredER, 
	EXP_loss_master_fact.DirectALAEIncurredIR, 
	EXP_claim_loss_transaction_fact.table_name AS table_name1, 
	EXP_claim_loss_transaction_fact.clndr_date AS clndr_date1, 
	EXP_claim_loss_transaction_fact.outstanding_amt AS outstanding_amt1, 
	EXP_claim_loss_transaction_fact.paid_loss_amt AS paid_loss_amt1, 
	EXP_claim_loss_transaction_fact.paid_exp_amt AS paid_exp_amt1, 
	EXP_claim_loss_transaction_fact.ChangeInOutstandingAmount AS ChangeInOutstandingAmount1, 
	EXP_claim_loss_transaction_fact.DirectLossPaidER AS DirectLossPaidER1, 
	EXP_claim_loss_transaction_fact.DirectLossPaidIR AS DirectLossPaidIR1, 
	EXP_claim_loss_transaction_fact.DirectALAEPaidER AS DirectALAEPaidER1, 
	EXP_claim_loss_transaction_fact.DirectALAEPaidIR AS DirectALAEPaidIR1, 
	EXP_claim_loss_transaction_fact.DirectSalvagePaid AS DirectSalvagePaid1, 
	EXP_claim_loss_transaction_fact.DirectSubrogationPaid AS DirectSubrogationPaid1, 
	EXP_claim_loss_transaction_fact.DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid1, 
	EXP_claim_loss_transaction_fact.DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid1, 
	EXP_claim_loss_transaction_fact.DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid1, 
	EXP_claim_loss_transaction_fact.DirectLossOutstandingER AS DirectLossOutstandingER1, 
	EXP_claim_loss_transaction_fact.DirectLossOutstandingIR AS DirectLossOutstandingIR1, 
	EXP_claim_loss_transaction_fact.DirectALAEOutstandingER AS DirectALAEOutstandingER1, 
	EXP_claim_loss_transaction_fact.DirectALAEOutstandingIR AS DirectALAEOutstandingIR1, 
	EXP_claim_loss_transaction_fact.DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding1, 
	EXP_claim_loss_transaction_fact.DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding1, 
	EXP_claim_loss_transaction_fact.DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding1, 
	EXP_claim_loss_transaction_fact.DirectSubroOutstanding AS DirectSubroOutstanding1, 
	EXP_claim_loss_transaction_fact.DirectSalvageOutstanding AS DirectSalvageOutstanding1, 
	EXP_claim_loss_transaction_fact.DirectLossIncurredER AS DirectLossIncurredER1, 
	EXP_claim_loss_transaction_fact.DirectLossIncurredIR AS DirectLossIncurredIR1, 
	EXP_claim_loss_transaction_fact.DirectALAEIncurredER AS DirectALAEIncurredER1, 
	EXP_claim_loss_transaction_fact.DirectALAEIncurredIR AS DirectALAEIncurredIR1
	FROM EXP_loss_master_fact
	INNER JOIN EXP_claim_loss_transaction_fact
	ON EXP_claim_loss_transaction_fact.clndr_date = EXP_loss_master_fact.clndr_date
),
EXP_loss_master_fact_claim_loss_transaction_fact AS (
	SELECT
	table_name,
	clndr_date,
	outstanding_amt,
	paid_loss_amt,
	paid_exp_amt,
	ChangeInOutstandingAmount,
	DirectLossPaidER,
	DirectLossPaidIR,
	DirectALAEPaidER,
	DirectALAEPaidIR,
	DirectSalvagePaid,
	DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	DirectSalvageOutstanding,
	DirectLossIncurredER,
	DirectLossIncurredIR,
	DirectALAEIncurredER,
	DirectALAEIncurredIR,
	table_name1,
	clndr_date1,
	outstanding_amt1,
	paid_loss_amt1,
	paid_exp_amt1,
	ChangeInOutstandingAmount1,
	DirectLossPaidER1,
	DirectLossPaidIR1,
	DirectALAEPaidER1,
	DirectALAEPaidIR1,
	DirectSalvagePaid1,
	DirectSubrogationPaid1,
	DirectOtherRecoveryPaid1,
	DirectOtherRecoveryLossPaid1,
	DirectOtherRecoveryALAEPaid1,
	DirectLossOutstandingER1,
	DirectLossOutstandingIR1,
	DirectALAEOutstandingER1,
	DirectALAEOutstandingIR1,
	DirectOtherRecoveryOutstanding1,
	DirectOtherRecoveryLossOutstanding1,
	DirectOtherRecoveryALAEOutstanding1,
	DirectSubroOutstanding1,
	DirectSalvageOutstanding1,
	DirectLossIncurredER1,
	DirectLossIncurredIR1,
	DirectALAEIncurredER1,
	DirectALAEIncurredIR1,
	'Difference' AS table_name11,
	clndr_date AS clndr_date11,
	0 AS outstanding_amt11,
	0 AS paid_loss_amt11,
	0 AS paid_exp_amt11,
	0 AS ChangeInOutstandingAmount11,
	DirectLossPaidER-DirectLossPaidER1 AS DirectLossPaidER11,
	DirectLossPaidIR-DirectLossPaidIR1 AS DirectLossPaidIR11,
	DirectALAEPaidER-DirectALAEPaidER1 AS DirectALAEPaidER11,
	DirectALAEPaidIR-DirectALAEPaidIR1 AS DirectALAEPaidIR11,
	DirectSalvagePaid-DirectSalvagePaid1 AS DirectSalvagePaid11,
	DirectSubrogationPaid-DirectSubrogationPaid1 AS DirectSubrogationPaid11,
	DirectOtherRecoveryPaid-DirectOtherRecoveryPaid1 AS DirectOtherRecoveryPaid11,
	DirectOtherRecoveryLossPaid-DirectOtherRecoveryLossPaid1 AS DirectOtherRecoveryLossPaid11,
	DirectOtherRecoveryALAEPaid-DirectOtherRecoveryALAEPaid1 AS DirectOtherRecoveryALAEPaid11,
	DirectLossOutstandingER-DirectLossOutstandingER1 AS DirectLossOutstandingER11,
	DirectLossOutstandingIR-DirectLossOutstandingIR1 AS DirectLossOutstandingIR11,
	DirectALAEOutstandingER-DirectALAEOutstandingER1 AS DirectALAEOutstandingER11,
	DirectALAEOutstandingIR-DirectALAEOutstandingIR1 AS DirectALAEOutstandingIR11,
	DirectOtherRecoveryOutstanding-DirectOtherRecoveryOutstanding1 AS DirectOtherRecoveryOutstanding11,
	DirectOtherRecoveryLossOutstanding-DirectOtherRecoveryLossOutstanding1 AS DirectOtherRecoveryLossOutstanding11,
	DirectOtherRecoveryALAEOutstanding-DirectOtherRecoveryALAEOutstanding1 AS DirectOtherRecoveryALAEOutstanding11,
	DirectSubroOutstanding-DirectSubroOutstanding1 AS DirectSubroOutstanding11,
	DirectSalvageOutstanding-DirectSalvageOutstanding1 AS DirectSalvageOutstanding11,
	DirectLossIncurredER-DirectLossIncurredER1 AS DirectLossIncurredER11,
	DirectLossIncurredIR-DirectLossIncurredIR1 AS DirectLossIncurredIR11,
	DirectALAEIncurredER-DirectALAEIncurredER1 AS DirectALAEIncurredER11,
	DirectALAEIncurredIR-DirectALAEIncurredIR1 AS DirectALAEIncurredIR11
	FROM JNR_loss_master_fact_claim_loss_transaction_fact
),
Union1 AS (
	SELECT table_name, clndr_date, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR
	FROM 
	UNION
	SELECT table_name1 AS table_name, clndr_date1 AS clndr_date, outstanding_amt1 AS outstanding_amt, paid_loss_amt1 AS paid_loss_amt, paid_exp_amt1 AS paid_exp_amt, ChangeInOutstandingAmount1 AS ChangeInOutstandingAmount, DirectLossPaidER1 AS DirectLossPaidER, DirectLossPaidIR1 AS DirectLossPaidIR, DirectALAEPaidER1 AS DirectALAEPaidER, DirectALAEPaidIR1 AS DirectALAEPaidIR, DirectSalvagePaid1 AS DirectSalvagePaid, DirectSubrogationPaid1 AS DirectSubrogationPaid, DirectOtherRecoveryPaid1 AS DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid1 AS DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid1 AS DirectOtherRecoveryALAEPaid, DirectLossOutstandingER1 AS DirectLossOutstandingER, DirectLossOutstandingIR1 AS DirectLossOutstandingIR, DirectALAEOutstandingER1 AS DirectALAEOutstandingER, DirectALAEOutstandingIR1 AS DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding1 AS DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding1 AS DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding1 AS DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding1 AS DirectSubroOutstanding, DirectSalvageOutstanding1 AS DirectSalvageOutstanding, DirectLossIncurredER1 AS DirectLossIncurredER, DirectLossIncurredIR1 AS DirectLossIncurredIR, DirectALAEIncurredER1 AS DirectALAEIncurredER, DirectALAEIncurredIR1 AS DirectALAEIncurredIR
	FROM 
	UNION
	SELECT table_name11 AS table_name, clndr_date11 AS clndr_date, outstanding_amt11 AS outstanding_amt, paid_loss_amt11 AS paid_loss_amt, paid_exp_amt11 AS paid_exp_amt, ChangeInOutstandingAmount11 AS ChangeInOutstandingAmount, DirectLossPaidER11 AS DirectLossPaidER, DirectLossPaidIR11 AS DirectLossPaidIR, DirectALAEPaidER11 AS DirectALAEPaidER, DirectALAEPaidIR11 AS DirectALAEPaidIR, DirectSalvagePaid11 AS DirectSalvagePaid, DirectSubrogationPaid11 AS DirectSubrogationPaid, DirectOtherRecoveryPaid11 AS DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid11 AS DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid11 AS DirectOtherRecoveryALAEPaid, DirectLossOutstandingER11 AS DirectLossOutstandingER, DirectLossOutstandingIR11 AS DirectLossOutstandingIR, DirectALAEOutstandingER11 AS DirectALAEOutstandingER, DirectALAEOutstandingIR11 AS DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding11 AS DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding11 AS DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding11 AS DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding11 AS DirectSubroOutstanding, DirectSalvageOutstanding11 AS DirectSalvageOutstanding, DirectLossIncurredER11 AS DirectLossIncurredER, DirectLossIncurredIR11 AS DirectLossIncurredIR, DirectALAEIncurredER11 AS DirectALAEIncurredER, DirectALAEIncurredIR11 AS DirectALAEIncurredIR
	FROM 
),
EXPTRANS1 AS (
	SELECT
	'Balance loss_master_fact to claim_loss_transaction_fact' AS BalancingDescription,
	table_name,
	clndr_date,
	outstanding_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(outstanding_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(outstanding_amt)) AS o_outstanding_amt,
	paid_loss_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_loss_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_loss_amt)) AS o_paid_loss_amt,
	paid_exp_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_exp_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_exp_amt)) AS o_paid_exp_amt,
	ChangeInOutstandingAmount,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(ChangeInOutstandingAmount))
	-- 
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(ChangeInOutstandingAmount)) AS o_ChangeInOutstandingAmount,
	DirectLossPaidER,
	DirectLossPaidIR,
	DirectALAEPaidER,
	DirectALAEPaidIR,
	DirectSalvagePaid,
	DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	DirectSalvageOutstanding,
	DirectLossIncurredER,
	DirectLossIncurredIR,
	DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: DECODE(TRUE,
	-- table_name='loss_master_fact',1,
	-- table_name='claim_loss_transaction_fact',2,
	-- table_name='Difference',3
	-- )
	DECODE(TRUE,
		table_name = 'loss_master_fact', 1,
		table_name = 'claim_loss_transaction_fact', 2,
		table_name = 'Difference', 3) AS OrderInd
	FROM Union1
),
SRTTRANS1 AS (
	SELECT
	BalancingDescription, 
	table_name, 
	clndr_date, 
	o_outstanding_amt AS outstanding_amt, 
	o_paid_loss_amt AS paid_loss_amt, 
	o_paid_exp_amt AS paid_exp_amt, 
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount, 
	DirectLossPaidER, 
	DirectLossPaidIR, 
	DirectALAEPaidER, 
	DirectALAEPaidIR, 
	DirectSalvagePaid, 
	DirectSubrogationPaid, 
	DirectOtherRecoveryPaid, 
	DirectOtherRecoveryLossPaid, 
	DirectOtherRecoveryALAEPaid, 
	DirectLossOutstandingER, 
	DirectLossOutstandingIR, 
	DirectALAEOutstandingER, 
	DirectALAEOutstandingIR, 
	DirectOtherRecoveryOutstanding, 
	DirectOtherRecoveryLossOutstanding, 
	DirectOtherRecoveryALAEOutstanding, 
	DirectSubroOutstanding, 
	DirectSalvageOutstanding, 
	DirectLossIncurredER, 
	DirectLossIncurredIR, 
	DirectALAEIncurredER, 
	DirectALAEIncurredIR, 
	OrderInd
	FROM EXPTRANS1
	ORDER BY clndr_date ASC, OrderInd ASC
),
JNR_vwLossMasterFact_claim_loss_transaction_fact AS (SELECT
	EXP_vwLossMasterFact.table_name, 
	EXP_vwLossMasterFact.clndr_date, 
	EXP_vwLossMasterFact.outstanding_amt, 
	EXP_vwLossMasterFact.paid_loss_amt, 
	EXP_vwLossMasterFact.paid_exp_amt, 
	EXP_vwLossMasterFact.ChangeInOutstandingAmount, 
	EXP_vwLossMasterFact.DirectLossPaidER, 
	EXP_vwLossMasterFact.DirectLossPaidIR, 
	EXP_vwLossMasterFact.DirectALAEPaidER, 
	EXP_vwLossMasterFact.DirectALAEPaidIR, 
	EXP_vwLossMasterFact.DirectSalvagePaid, 
	EXP_vwLossMasterFact.DirectSubrogationPaid, 
	EXP_vwLossMasterFact.DirectOtherRecoveryPaid, 
	EXP_vwLossMasterFact.DirectOtherRecoveryLossPaid, 
	EXP_vwLossMasterFact.DirectOtherRecoveryALAEPaid, 
	EXP_vwLossMasterFact.DirectLossOutstandingER, 
	EXP_vwLossMasterFact.DirectLossOutstandingIR, 
	EXP_vwLossMasterFact.DirectALAEOutstandingER, 
	EXP_vwLossMasterFact.DirectALAEOutstandingIR, 
	EXP_vwLossMasterFact.DirectOtherRecoveryOutstanding, 
	EXP_vwLossMasterFact.DirectOtherRecoveryLossOutstanding, 
	EXP_vwLossMasterFact.DirectOtherRecoveryALAEOutstanding, 
	EXP_vwLossMasterFact.DirectSubroOutstanding, 
	EXP_vwLossMasterFact.DirectSalvageOutstanding, 
	EXP_vwLossMasterFact.DirectLossIncurredER, 
	EXP_vwLossMasterFact.DirectLossIncurredIR, 
	EXP_vwLossMasterFact.DirectALAEIncurredER, 
	EXP_vwLossMasterFact.DirectALAEIncurredIR, 
	EXP_claim_loss_transaction_fact.table_name AS table_name1, 
	EXP_claim_loss_transaction_fact.clndr_date AS clndr_date1, 
	EXP_claim_loss_transaction_fact.outstanding_amt AS outstanding_amt1, 
	EXP_claim_loss_transaction_fact.paid_loss_amt AS paid_loss_amt1, 
	EXP_claim_loss_transaction_fact.paid_exp_amt AS paid_exp_amt1, 
	EXP_claim_loss_transaction_fact.ChangeInOutstandingAmount AS ChangeInOutstandingAmount1, 
	EXP_claim_loss_transaction_fact.DirectLossPaidER AS DirectLossPaidER1, 
	EXP_claim_loss_transaction_fact.DirectLossPaidIR AS DirectLossPaidIR1, 
	EXP_claim_loss_transaction_fact.DirectALAEPaidER AS DirectALAEPaidER1, 
	EXP_claim_loss_transaction_fact.DirectALAEPaidIR AS DirectALAEPaidIR1, 
	EXP_claim_loss_transaction_fact.DirectSalvagePaid AS DirectSalvagePaid1, 
	EXP_claim_loss_transaction_fact.DirectSubrogationPaid AS DirectSubrogationPaid1, 
	EXP_claim_loss_transaction_fact.DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid1, 
	EXP_claim_loss_transaction_fact.DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid1, 
	EXP_claim_loss_transaction_fact.DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid1, 
	EXP_claim_loss_transaction_fact.DirectLossOutstandingER AS DirectLossOutstandingER1, 
	EXP_claim_loss_transaction_fact.DirectLossOutstandingIR AS DirectLossOutstandingIR1, 
	EXP_claim_loss_transaction_fact.DirectALAEOutstandingER AS DirectALAEOutstandingER1, 
	EXP_claim_loss_transaction_fact.DirectALAEOutstandingIR AS DirectALAEOutstandingIR1, 
	EXP_claim_loss_transaction_fact.DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding1, 
	EXP_claim_loss_transaction_fact.DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding1, 
	EXP_claim_loss_transaction_fact.DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding1, 
	EXP_claim_loss_transaction_fact.DirectSubroOutstanding AS DirectSubroOutstanding1, 
	EXP_claim_loss_transaction_fact.DirectSalvageOutstanding AS DirectSalvageOutstanding1, 
	EXP_claim_loss_transaction_fact.DirectLossIncurredER AS DirectLossIncurredER1, 
	EXP_claim_loss_transaction_fact.DirectLossIncurredIR AS DirectLossIncurredIR1, 
	EXP_claim_loss_transaction_fact.DirectALAEIncurredER AS DirectALAEIncurredER1, 
	EXP_claim_loss_transaction_fact.DirectALAEIncurredIR AS DirectALAEIncurredIR1
	FROM EXP_vwLossMasterFact
	INNER JOIN EXP_claim_loss_transaction_fact
	ON EXP_claim_loss_transaction_fact.clndr_date = EXP_vwLossMasterFact.clndr_date
),
EXP_vwLossMasterFact_claim_loss_transaction_fact AS (
	SELECT
	table_name,
	clndr_date,
	outstanding_amt,
	paid_loss_amt,
	paid_exp_amt,
	ChangeInOutstandingAmount,
	DirectLossPaidER,
	DirectLossPaidIR,
	DirectALAEPaidER,
	DirectALAEPaidIR,
	DirectSalvagePaid,
	DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	DirectSalvageOutstanding,
	DirectLossIncurredER,
	DirectLossIncurredIR,
	DirectALAEIncurredER,
	DirectALAEIncurredIR,
	table_name1,
	clndr_date1,
	outstanding_amt1,
	paid_loss_amt1,
	paid_exp_amt1,
	ChangeInOutstandingAmount1,
	DirectLossPaidER1,
	DirectLossPaidIR1,
	DirectALAEPaidER1,
	DirectALAEPaidIR1,
	DirectSalvagePaid1,
	DirectSubrogationPaid1,
	DirectOtherRecoveryPaid1,
	DirectOtherRecoveryLossPaid1,
	DirectOtherRecoveryALAEPaid1,
	DirectLossOutstandingER1,
	DirectLossOutstandingIR1,
	DirectALAEOutstandingER1,
	DirectALAEOutstandingIR1,
	DirectOtherRecoveryOutstanding1,
	DirectOtherRecoveryLossOutstanding1,
	DirectOtherRecoveryALAEOutstanding1,
	DirectSubroOutstanding1,
	DirectSalvageOutstanding1,
	DirectLossIncurredER1,
	DirectLossIncurredIR1,
	DirectALAEIncurredER1,
	DirectALAEIncurredIR1,
	'Difference' AS table_name11,
	clndr_date AS clndr_date11,
	0 AS outstanding_amt11,
	0 AS paid_loss_amt11,
	0 AS paid_exp_amt11,
	0 AS ChangeInOutstandingAmount11,
	DirectLossPaidER-DirectLossPaidER1 AS DirectLossPaidER11,
	DirectLossPaidIR-DirectLossPaidIR1 AS DirectLossPaidIR11,
	DirectALAEPaidER-DirectALAEPaidER1 AS DirectALAEPaidER11,
	DirectALAEPaidIR-DirectALAEPaidIR1 AS DirectALAEPaidIR11,
	DirectSalvagePaid-DirectSalvagePaid1 AS DirectSalvagePaid11,
	DirectSubrogationPaid-DirectSubrogationPaid1 AS DirectSubrogationPaid11,
	DirectOtherRecoveryPaid-DirectOtherRecoveryPaid1 AS DirectOtherRecoveryPaid11,
	DirectOtherRecoveryLossPaid-DirectOtherRecoveryLossPaid1 AS DirectOtherRecoveryLossPaid11,
	DirectOtherRecoveryALAEPaid-DirectOtherRecoveryALAEPaid1 AS DirectOtherRecoveryALAEPaid11,
	DirectLossOutstandingER-DirectLossOutstandingER1 AS DirectLossOutstandingER11,
	DirectLossOutstandingIR-DirectLossOutstandingIR1 AS DirectLossOutstandingIR11,
	DirectALAEOutstandingER-DirectALAEOutstandingER1 AS DirectALAEOutstandingER11,
	DirectALAEOutstandingIR-DirectALAEOutstandingIR1 AS DirectALAEOutstandingIR11,
	DirectOtherRecoveryOutstanding-DirectOtherRecoveryOutstanding1 AS DirectOtherRecoveryOutstanding11,
	DirectOtherRecoveryLossOutstanding-DirectOtherRecoveryLossOutstanding1 AS DirectOtherRecoveryLossOutstanding11,
	DirectOtherRecoveryALAEOutstanding-DirectOtherRecoveryALAEOutstanding1 AS DirectOtherRecoveryALAEOutstanding11,
	DirectSubroOutstanding-DirectSubroOutstanding1 AS DirectSubroOutstanding11,
	DirectSalvageOutstanding-DirectSalvageOutstanding1 AS DirectSalvageOutstanding11,
	DirectLossIncurredER-DirectLossIncurredER1 AS DirectLossIncurredER11,
	DirectLossIncurredIR-DirectLossIncurredIR1 AS DirectLossIncurredIR11,
	DirectALAEIncurredER-DirectALAEIncurredER1 AS DirectALAEIncurredER11,
	DirectALAEIncurredIR-DirectALAEIncurredIR1 AS DirectALAEIncurredIR11
	FROM JNR_vwLossMasterFact_claim_loss_transaction_fact
),
Union2 AS (
	SELECT table_name, clndr_date, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR
	FROM 
	UNION
	SELECT table_name1 AS table_name, clndr_date1 AS clndr_date, outstanding_amt1 AS outstanding_amt, paid_loss_amt1 AS paid_loss_amt, paid_exp_amt1 AS paid_exp_amt, ChangeInOutstandingAmount1 AS ChangeInOutstandingAmount, DirectLossPaidER1 AS DirectLossPaidER, DirectLossPaidIR1 AS DirectLossPaidIR, DirectALAEPaidER1 AS DirectALAEPaidER, DirectALAEPaidIR1 AS DirectALAEPaidIR, DirectSalvagePaid1 AS DirectSalvagePaid, DirectSubrogationPaid1 AS DirectSubrogationPaid, DirectOtherRecoveryPaid1 AS DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid1 AS DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid1 AS DirectOtherRecoveryALAEPaid, DirectLossOutstandingER1 AS DirectLossOutstandingER, DirectLossOutstandingIR1 AS DirectLossOutstandingIR, DirectALAEOutstandingER1 AS DirectALAEOutstandingER, DirectALAEOutstandingIR1 AS DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding1 AS DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding1 AS DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding1 AS DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding1 AS DirectSubroOutstanding, DirectSalvageOutstanding1 AS DirectSalvageOutstanding, DirectLossIncurredER1 AS DirectLossIncurredER, DirectLossIncurredIR1 AS DirectLossIncurredIR, DirectALAEIncurredER1 AS DirectALAEIncurredER, DirectALAEIncurredIR1 AS DirectALAEIncurredIR
	FROM 
	UNION
	SELECT table_name11 AS table_name, clndr_date11 AS clndr_date, outstanding_amt11 AS outstanding_amt, paid_loss_amt11 AS paid_loss_amt, paid_exp_amt11 AS paid_exp_amt, ChangeInOutstandingAmount11 AS ChangeInOutstandingAmount, DirectLossPaidER11 AS DirectLossPaidER, DirectLossPaidIR11 AS DirectLossPaidIR, DirectALAEPaidER11 AS DirectALAEPaidER, DirectALAEPaidIR11 AS DirectALAEPaidIR, DirectSalvagePaid11 AS DirectSalvagePaid, DirectSubrogationPaid11 AS DirectSubrogationPaid, DirectOtherRecoveryPaid11 AS DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid11 AS DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid11 AS DirectOtherRecoveryALAEPaid, DirectLossOutstandingER11 AS DirectLossOutstandingER, DirectLossOutstandingIR11 AS DirectLossOutstandingIR, DirectALAEOutstandingER11 AS DirectALAEOutstandingER, DirectALAEOutstandingIR11 AS DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding11 AS DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding11 AS DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding11 AS DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding11 AS DirectSubroOutstanding, DirectSalvageOutstanding11 AS DirectSalvageOutstanding, DirectLossIncurredER11 AS DirectLossIncurredER, DirectLossIncurredIR11 AS DirectLossIncurredIR, DirectALAEIncurredER11 AS DirectALAEIncurredER, DirectALAEIncurredIR11 AS DirectALAEIncurredIR
	FROM 
),
EXPTRANS2 AS (
	SELECT
	'Balance vwLossMasterFact to claim_loss_transaction_fact' AS BalancingDescription,
	table_name,
	clndr_date,
	outstanding_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(outstanding_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(outstanding_amt)) AS o_outstanding_amt,
	paid_loss_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_loss_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_loss_amt)) AS o_paid_loss_amt,
	paid_exp_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_exp_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_exp_amt)) AS o_paid_exp_amt,
	ChangeInOutstandingAmount,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(ChangeInOutstandingAmount))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(ChangeInOutstandingAmount)) AS o_ChangeInOutstandingAmount,
	DirectLossPaidER,
	DirectLossPaidIR,
	DirectALAEPaidER,
	DirectALAEPaidIR,
	DirectSalvagePaid,
	DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	DirectSalvageOutstanding,
	DirectLossIncurredER,
	DirectLossIncurredIR,
	DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: DECODE(TRUE,
	-- table_name='vwLossMasterFact',1,
	-- table_name='claim_loss_transaction_fact',2,
	-- table_name='Difference',3
	-- )
	DECODE(TRUE,
		table_name = 'vwLossMasterFact', 1,
		table_name = 'claim_loss_transaction_fact', 2,
		table_name = 'Difference', 3) AS OrderInd
	FROM Union2
),
SRTTRANS2 AS (
	SELECT
	BalancingDescription, 
	table_name, 
	clndr_date, 
	o_outstanding_amt AS outstanding_amt, 
	o_paid_loss_amt AS paid_loss_amt, 
	o_paid_exp_amt AS paid_exp_amt, 
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount, 
	DirectLossPaidER, 
	DirectLossPaidIR, 
	DirectALAEPaidER, 
	DirectALAEPaidIR, 
	DirectSalvagePaid, 
	DirectSubrogationPaid, 
	DirectOtherRecoveryPaid, 
	DirectOtherRecoveryLossPaid, 
	DirectOtherRecoveryALAEPaid, 
	DirectLossOutstandingER, 
	DirectLossOutstandingIR, 
	DirectALAEOutstandingER, 
	DirectALAEOutstandingIR, 
	DirectOtherRecoveryOutstanding, 
	DirectOtherRecoveryLossOutstanding, 
	DirectOtherRecoveryALAEOutstanding, 
	DirectSubroOutstanding, 
	DirectSalvageOutstanding, 
	DirectLossIncurredER, 
	DirectLossIncurredIR, 
	DirectALAEIncurredER, 
	DirectALAEIncurredIR, 
	OrderInd
	FROM EXPTRANS2
	ORDER BY clndr_date ASC, OrderInd ASC
),
Union_all AS (
	SELECT BalancingDescription, table_name, clndr_date, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR
	FROM SRTTRANS
	UNION
	SELECT BalancingDescription, table_name, clndr_date, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR
	FROM SRTTRANS1
	UNION
	SELECT BalancingDescription, table_name, clndr_date, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR
	FROM SRTTRANS2
),
ClaimFacts_Balancing AS (
	INSERT INTO ClaimFacts_Balancing
	(BalancingDescription, TableName, clndr_date, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR)
	SELECT 
	BALANCINGDESCRIPTION, 
	table_name AS TABLENAME, 
	CLNDR_DATE, 
	OUTSTANDING_AMT, 
	PAID_LOSS_AMT, 
	PAID_EXP_AMT, 
	CHANGEINOUTSTANDINGAMOUNT, 
	DIRECTLOSSPAIDER, 
	DIRECTLOSSPAIDIR, 
	DIRECTALAEPAIDER, 
	DIRECTALAEPAIDIR, 
	DIRECTSALVAGEPAID, 
	DIRECTSUBROGATIONPAID, 
	DIRECTOTHERRECOVERYPAID, 
	DIRECTOTHERRECOVERYLOSSPAID, 
	DIRECTOTHERRECOVERYALAEPAID, 
	DIRECTLOSSOUTSTANDINGER, 
	DIRECTLOSSOUTSTANDINGIR, 
	DIRECTALAEOUTSTANDINGER, 
	DIRECTALAEOUTSTANDINGIR, 
	DIRECTOTHERRECOVERYOUTSTANDING, 
	DIRECTOTHERRECOVERYLOSSOUTSTANDING, 
	DIRECTOTHERRECOVERYALAEOUTSTANDING, 
	DIRECTSUBROOUTSTANDING, 
	DIRECTSALVAGEOUTSTANDING, 
	DIRECTLOSSINCURREDER, 
	DIRECTLOSSINCURREDIR, 
	DIRECTALAEINCURREDER, 
	DIRECTALAEINCURREDIR
	FROM Union_all
),
EXP_loss_master_fact1 AS (
	SELECT
	'loss_master_fact' AS table_name,
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	paid_loss_amt,
	paid_exp_amt,
	ChangeInOutstandingAmount,
	DirectLossPaidER,
	DirectLossPaidIR,
	DirectALAEPaidER,
	DirectALAEPaidIR,
	DirectSalvagePaid,
	DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	DirectSalvageOutstanding,
	DirectLossIncurredER,
	DirectLossIncurredIR,
	DirectALAEIncurredER,
	DirectALAEIncurredIR
	FROM SQ_loss_master_fact
),
EXP_vwLossMasterFact1 AS (
	SELECT
	'vwLossMasterFact' AS table_name,
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	paid_loss_amt,
	paid_exp_amt,
	ChangeInOutstandingAmount,
	DirectLossPaidER,
	DirectLossPaidIR,
	DirectALAEPaidER,
	DirectALAEPaidIR,
	DirectSalvagePaid,
	DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	DirectSalvageOutstanding,
	DirectLossIncurredER,
	DirectLossIncurredIR,
	DirectALAEIncurredER,
	DirectALAEIncurredIR
	FROM SQ_vwLossMasterFact
),
JNR_loss_master_fact_vwLossMasterFact1 AS (SELECT
	EXP_loss_master_fact1.table_name, 
	EXP_loss_master_fact1.clndr_date, 
	EXP_loss_master_fact1.EnterpriseGroupDescription, 
	EXP_loss_master_fact1.StrategicProfitCenterDescription, 
	EXP_loss_master_fact1.InsuranceReferenceLegalEntityDescription, 
	EXP_loss_master_fact1.PolicyOfferingDescription, 
	EXP_loss_master_fact1.ProductDescription, 
	EXP_loss_master_fact1.InsuranceReferenceLineOfBusinessDescription, 
	EXP_loss_master_fact1.outstanding_amt, 
	EXP_loss_master_fact1.paid_loss_amt, 
	EXP_loss_master_fact1.paid_exp_amt, 
	EXP_loss_master_fact1.ChangeInOutstandingAmount, 
	EXP_loss_master_fact1.DirectLossPaidER, 
	EXP_loss_master_fact1.DirectLossPaidIR, 
	EXP_loss_master_fact1.DirectALAEPaidER, 
	EXP_loss_master_fact1.DirectALAEPaidIR, 
	EXP_loss_master_fact1.DirectSalvagePaid, 
	EXP_loss_master_fact1.DirectSubrogationPaid, 
	EXP_loss_master_fact1.DirectOtherRecoveryPaid, 
	EXP_loss_master_fact1.DirectOtherRecoveryLossPaid, 
	EXP_loss_master_fact1.DirectOtherRecoveryALAEPaid, 
	EXP_loss_master_fact1.DirectLossOutstandingER, 
	EXP_loss_master_fact1.DirectLossOutstandingIR, 
	EXP_loss_master_fact1.DirectALAEOutstandingER, 
	EXP_loss_master_fact1.DirectALAEOutstandingIR, 
	EXP_loss_master_fact1.DirectOtherRecoveryOutstanding, 
	EXP_loss_master_fact1.DirectOtherRecoveryLossOutstanding, 
	EXP_loss_master_fact1.DirectOtherRecoveryALAEOutstanding, 
	EXP_loss_master_fact1.DirectSubroOutstanding, 
	EXP_loss_master_fact1.DirectSalvageOutstanding, 
	EXP_loss_master_fact1.DirectLossIncurredER, 
	EXP_loss_master_fact1.DirectLossIncurredIR, 
	EXP_loss_master_fact1.DirectALAEIncurredER, 
	EXP_loss_master_fact1.DirectALAEIncurredIR, 
	EXP_vwLossMasterFact1.table_name AS table_name1, 
	EXP_vwLossMasterFact1.clndr_date AS clndr_date1, 
	EXP_vwLossMasterFact1.EnterpriseGroupDescription AS EnterpriseGroupDescription1, 
	EXP_vwLossMasterFact1.StrategicProfitCenterDescription AS StrategicProfitCenterDescription1, 
	EXP_vwLossMasterFact1.InsuranceReferenceLegalEntityDescription AS InsuranceReferenceLegalEntityDescription1, 
	EXP_vwLossMasterFact1.PolicyOfferingDescription AS PolicyOfferingDescription1, 
	EXP_vwLossMasterFact1.ProductDescription AS ProductDescription1, 
	EXP_vwLossMasterFact1.InsuranceReferenceLineOfBusinessDescription AS InsuranceReferenceLineOfBusinessDescription1, 
	EXP_vwLossMasterFact1.outstanding_amt AS outstanding_amt1, 
	EXP_vwLossMasterFact1.paid_loss_amt AS paid_loss_amt1, 
	EXP_vwLossMasterFact1.paid_exp_amt AS paid_exp_amt1, 
	EXP_vwLossMasterFact1.ChangeInOutstandingAmount AS ChangeInOutstandingAmount1, 
	EXP_vwLossMasterFact1.DirectLossPaidER AS DirectLossPaidER1, 
	EXP_vwLossMasterFact1.DirectLossPaidIR AS DirectLossPaidIR1, 
	EXP_vwLossMasterFact1.DirectALAEPaidER AS DirectALAEPaidER1, 
	EXP_vwLossMasterFact1.DirectALAEPaidIR AS DirectALAEPaidIR1, 
	EXP_vwLossMasterFact1.DirectSalvagePaid AS DirectSalvagePaid1, 
	EXP_vwLossMasterFact1.DirectSubrogationPaid AS DirectSubrogationPaid1, 
	EXP_vwLossMasterFact1.DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid1, 
	EXP_vwLossMasterFact1.DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid1, 
	EXP_vwLossMasterFact1.DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid1, 
	EXP_vwLossMasterFact1.DirectLossOutstandingER AS DirectLossOutstandingER1, 
	EXP_vwLossMasterFact1.DirectLossOutstandingIR AS DirectLossOutstandingIR1, 
	EXP_vwLossMasterFact1.DirectALAEOutstandingER AS DirectALAEOutstandingER1, 
	EXP_vwLossMasterFact1.DirectALAEOutstandingIR AS DirectALAEOutstandingIR1, 
	EXP_vwLossMasterFact1.DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding1, 
	EXP_vwLossMasterFact1.DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding1, 
	EXP_vwLossMasterFact1.DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding1, 
	EXP_vwLossMasterFact1.DirectSubroOutstanding AS DirectSubroOutstanding1, 
	EXP_vwLossMasterFact1.DirectSalvageOutstanding AS DirectSalvageOutstanding1, 
	EXP_vwLossMasterFact1.DirectLossIncurredER AS DirectLossIncurredER1, 
	EXP_vwLossMasterFact1.DirectLossIncurredIR AS DirectLossIncurredIR1, 
	EXP_vwLossMasterFact1.DirectALAEIncurredER AS DirectALAEIncurredER1, 
	EXP_vwLossMasterFact1.DirectALAEIncurredIR AS DirectALAEIncurredIR1
	FROM EXP_loss_master_fact1
	FULL OUTER JOIN EXP_vwLossMasterFact1
	ON EXP_vwLossMasterFact1.clndr_date = EXP_loss_master_fact1.clndr_date AND EXP_vwLossMasterFact1.EnterpriseGroupDescription = EXP_loss_master_fact1.EnterpriseGroupDescription AND EXP_vwLossMasterFact1.StrategicProfitCenterDescription = EXP_loss_master_fact1.StrategicProfitCenterDescription AND EXP_vwLossMasterFact1.InsuranceReferenceLegalEntityDescription = EXP_loss_master_fact1.InsuranceReferenceLegalEntityDescription AND EXP_vwLossMasterFact1.PolicyOfferingDescription = EXP_loss_master_fact1.PolicyOfferingDescription AND EXP_vwLossMasterFact1.ProductDescription = EXP_loss_master_fact1.ProductDescription AND EXP_vwLossMasterFact1.InsuranceReferenceLineOfBusinessDescription = EXP_loss_master_fact1.InsuranceReferenceLineOfBusinessDescription
),
EXP_loss_master_fact_vwLossMasterFact1 AS (
	SELECT
	'loss_master_fact' AS table_name,
	clndr_date,
	-- *INF*: IIF(NOT ISNULL(clndr_date),clndr_date,clndr_date1)
	IFF(NOT clndr_date IS NULL, clndr_date, clndr_date1) AS o_clndr_date,
	-- *INF*: IIF(NOT ISNULL(EnterpriseGroupDescription),EnterpriseGroupDescription,EnterpriseGroupDescription1)
	IFF(NOT EnterpriseGroupDescription IS NULL, EnterpriseGroupDescription, EnterpriseGroupDescription1) AS v_EnterpriseGroupDescription,
	-- *INF*: IIF(NOT ISNULL(StrategicProfitCenterDescription),StrategicProfitCenterDescription,StrategicProfitCenterDescription1)
	IFF(NOT StrategicProfitCenterDescription IS NULL, StrategicProfitCenterDescription, StrategicProfitCenterDescription1) AS v_StrategicProfitCenterDescription,
	-- *INF*: IIF(NOT ISNULL(InsuranceReferenceLegalEntityDescription),InsuranceReferenceLegalEntityDescription,InsuranceReferenceLegalEntityDescription1)
	IFF(NOT InsuranceReferenceLegalEntityDescription IS NULL, InsuranceReferenceLegalEntityDescription, InsuranceReferenceLegalEntityDescription1) AS v_InsuranceReferenceLegalEntityDescription,
	-- *INF*: IIF(NOT ISNULL(PolicyOfferingDescription),PolicyOfferingDescription,PolicyOfferingDescription1)
	IFF(NOT PolicyOfferingDescription IS NULL, PolicyOfferingDescription, PolicyOfferingDescription1) AS v_PolicyOfferingDescription,
	-- *INF*: IIF(NOT ISNULL(ProductDescription),ProductDescription,ProductDescription1)
	IFF(NOT ProductDescription IS NULL, ProductDescription, ProductDescription1) AS v_ProductDescription,
	-- *INF*: IIF(NOT ISNULL(InsuranceReferenceLineOfBusinessDescription),InsuranceReferenceLineOfBusinessDescription,InsuranceReferenceLineOfBusinessDescription1)
	IFF(NOT InsuranceReferenceLineOfBusinessDescription IS NULL, InsuranceReferenceLineOfBusinessDescription, InsuranceReferenceLineOfBusinessDescription1) AS v_InsuranceReferenceLineOfBusinessDescription,
	EnterpriseGroupDescription,
	v_EnterpriseGroupDescription AS o_EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	v_StrategicProfitCenterDescription AS o_StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	v_InsuranceReferenceLegalEntityDescription AS o_InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	v_PolicyOfferingDescription AS o_PolicyOfferingDescription,
	ProductDescription,
	v_ProductDescription AS o_ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	v_InsuranceReferenceLineOfBusinessDescription AS o_InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	-- *INF*: IIF(ISNULL(outstanding_amt),0,outstanding_amt)
	IFF(outstanding_amt IS NULL, 0, outstanding_amt) AS v_outstanding_amt,
	v_outstanding_amt AS o_outstanding_amt,
	paid_loss_amt,
	-- *INF*: IIF(ISNULL(paid_loss_amt),0,paid_loss_amt)
	IFF(paid_loss_amt IS NULL, 0, paid_loss_amt) AS v_paid_loss_amt,
	v_paid_loss_amt AS o_paid_loss_amt,
	paid_exp_amt,
	-- *INF*: IIF(ISNULL(paid_exp_amt),0,paid_exp_amt)
	IFF(paid_exp_amt IS NULL, 0, paid_exp_amt) AS v_paid_exp_amt,
	v_paid_exp_amt AS o_paid_exp_amt,
	ChangeInOutstandingAmount,
	-- *INF*: IIF(ISNULL(ChangeInOutstandingAmount),0,ChangeInOutstandingAmount)
	IFF(ChangeInOutstandingAmount IS NULL, 0, ChangeInOutstandingAmount) AS v_ChangeInOutstandingAmount,
	v_ChangeInOutstandingAmount AS o_ChangeInOutstandingAmount,
	DirectLossPaidER,
	-- *INF*: IIF(ISNULL(DirectLossPaidER),0,DirectLossPaidER)
	IFF(DirectLossPaidER IS NULL, 0, DirectLossPaidER) AS v_DirectLossPaidER,
	v_DirectLossPaidER AS o_DirectLossPaidER,
	DirectLossPaidIR,
	-- *INF*: IIF(ISNULL(DirectLossPaidIR),0,DirectLossPaidIR)
	IFF(DirectLossPaidIR IS NULL, 0, DirectLossPaidIR) AS v_DirectLossPaidIR,
	v_DirectLossPaidIR AS o_DirectLossPaidIR,
	DirectALAEPaidER,
	-- *INF*: IIF(ISNULL(DirectALAEPaidER),0,DirectALAEPaidER)
	IFF(DirectALAEPaidER IS NULL, 0, DirectALAEPaidER) AS v_DirectALAEPaidER,
	v_DirectALAEPaidER AS o_DirectALAEPaidER,
	DirectALAEPaidIR,
	-- *INF*: IIF(ISNULL(DirectALAEPaidIR),0,DirectALAEPaidIR)
	IFF(DirectALAEPaidIR IS NULL, 0, DirectALAEPaidIR) AS v_DirectALAEPaidIR,
	v_DirectALAEPaidIR AS o_DirectALAEPaidIR,
	DirectSalvagePaid,
	-- *INF*: IIF(ISNULL(DirectSalvagePaid),0,DirectSalvagePaid)
	IFF(DirectSalvagePaid IS NULL, 0, DirectSalvagePaid) AS v_DirectSalvagePaid,
	v_DirectSalvagePaid AS o_DirectSalvagePaid,
	DirectSubrogationPaid,
	-- *INF*: IIF(ISNULL(DirectSubrogationPaid),0,DirectSubrogationPaid)
	IFF(DirectSubrogationPaid IS NULL, 0, DirectSubrogationPaid) AS v_DirectSubrogationPaid,
	v_DirectSubrogationPaid AS o_DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryPaid),0,DirectOtherRecoveryPaid)
	IFF(DirectOtherRecoveryPaid IS NULL, 0, DirectOtherRecoveryPaid) AS v_DirectOtherRecoveryPaid,
	v_DirectOtherRecoveryPaid AS o_DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryLossPaid),0,DirectOtherRecoveryLossPaid)
	IFF(DirectOtherRecoveryLossPaid IS NULL, 0, DirectOtherRecoveryLossPaid) AS v_DirectOtherRecoveryLossPaid,
	v_DirectOtherRecoveryLossPaid AS o_DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryALAEPaid),0,DirectOtherRecoveryALAEPaid)
	IFF(DirectOtherRecoveryALAEPaid IS NULL, 0, DirectOtherRecoveryALAEPaid) AS v_DirectOtherRecoveryALAEPaid,
	v_DirectOtherRecoveryALAEPaid AS o_DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	-- *INF*: IIF(ISNULL(DirectLossOutstandingER),0,DirectLossOutstandingER)
	IFF(DirectLossOutstandingER IS NULL, 0, DirectLossOutstandingER) AS v_DirectLossOutstandingER,
	v_DirectLossOutstandingER AS o_DirectLossOutstandingER,
	DirectLossOutstandingIR,
	-- *INF*: IIF(ISNULL(DirectLossOutstandingIR),0,DirectLossOutstandingIR)
	IFF(DirectLossOutstandingIR IS NULL, 0, DirectLossOutstandingIR) AS v_DirectLossOutstandingIR,
	v_DirectLossOutstandingIR AS o_DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	-- *INF*: IIF(ISNULL(DirectALAEOutstandingER),0,DirectALAEOutstandingER)
	IFF(DirectALAEOutstandingER IS NULL, 0, DirectALAEOutstandingER) AS v_DirectALAEOutstandingER,
	v_DirectALAEOutstandingER AS o_DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	-- *INF*: IIF(ISNULL(DirectALAEOutstandingIR),0,DirectALAEOutstandingIR)
	IFF(DirectALAEOutstandingIR IS NULL, 0, DirectALAEOutstandingIR) AS v_DirectALAEOutstandingIR,
	v_DirectALAEOutstandingIR AS o_DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryOutstanding),0,DirectOtherRecoveryOutstanding)
	IFF(DirectOtherRecoveryOutstanding IS NULL, 0, DirectOtherRecoveryOutstanding) AS v_DirectOtherRecoveryOutstanding,
	v_DirectOtherRecoveryOutstanding AS o_DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryLossOutstanding),0,DirectOtherRecoveryLossOutstanding)
	IFF(DirectOtherRecoveryLossOutstanding IS NULL, 0, DirectOtherRecoveryLossOutstanding) AS v_DirectOtherRecoveryLossOutstanding,
	v_DirectOtherRecoveryLossOutstanding AS o_DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryALAEOutstanding),0,DirectOtherRecoveryALAEOutstanding)
	IFF(DirectOtherRecoveryALAEOutstanding IS NULL, 0, DirectOtherRecoveryALAEOutstanding) AS v_DirectOtherRecoveryALAEOutstanding,
	v_DirectOtherRecoveryALAEOutstanding AS o_DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	-- *INF*: IIF(ISNULL(DirectSubroOutstanding),0,DirectSubroOutstanding)
	IFF(DirectSubroOutstanding IS NULL, 0, DirectSubroOutstanding) AS v_DirectSubroOutstanding,
	v_DirectSubroOutstanding AS o_DirectSubroOutstanding,
	DirectSalvageOutstanding,
	-- *INF*: IIF(ISNULL(DirectSalvageOutstanding),0,DirectSalvageOutstanding)
	IFF(DirectSalvageOutstanding IS NULL, 0, DirectSalvageOutstanding) AS v_DirectSalvageOutstanding,
	v_DirectSalvageOutstanding AS o_DirectSalvageOutstanding,
	DirectLossIncurredER,
	-- *INF*: IIF(ISNULL(DirectLossIncurredER),0,DirectLossIncurredER)
	IFF(DirectLossIncurredER IS NULL, 0, DirectLossIncurredER) AS v_DirectLossIncurredER,
	v_DirectLossIncurredER AS o_DirectLossIncurredER,
	DirectLossIncurredIR,
	-- *INF*: IIF(ISNULL(DirectLossIncurredIR),0,DirectLossIncurredIR)
	IFF(DirectLossIncurredIR IS NULL, 0, DirectLossIncurredIR) AS v_DirectLossIncurredIR,
	v_DirectLossIncurredIR AS o_DirectLossIncurredIR,
	DirectALAEIncurredER,
	-- *INF*: IIF(ISNULL(DirectALAEIncurredER),0,DirectALAEIncurredER)
	IFF(DirectALAEIncurredER IS NULL, 0, DirectALAEIncurredER) AS v_DirectALAEIncurredER,
	v_DirectALAEIncurredER AS o_DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: IIF(ISNULL(DirectALAEIncurredIR),0,DirectALAEIncurredIR)
	IFF(DirectALAEIncurredIR IS NULL, 0, DirectALAEIncurredIR) AS v_DirectALAEIncurredIR,
	v_DirectALAEIncurredIR AS o_DirectALAEIncurredIR,
	'vwLossMasterFact' AS table_name1,
	clndr_date1,
	-- *INF*: IIF(NOT ISNULL(clndr_date1),clndr_date1,clndr_date)
	IFF(NOT clndr_date1 IS NULL, clndr_date1, clndr_date) AS o_clndr_date1,
	EnterpriseGroupDescription1,
	v_EnterpriseGroupDescription AS o_EnterpriseGroupDescription1,
	StrategicProfitCenterDescription1,
	v_StrategicProfitCenterDescription AS o_StrategicProfitCenterDescription1,
	InsuranceReferenceLegalEntityDescription1,
	v_InsuranceReferenceLegalEntityDescription AS o_InsuranceReferenceLegalEntityDescription1,
	PolicyOfferingDescription1,
	v_PolicyOfferingDescription AS o_PolicyOfferingDescription1,
	ProductDescription1,
	v_ProductDescription AS o_ProductDescription1,
	InsuranceReferenceLineOfBusinessDescription1,
	v_InsuranceReferenceLineOfBusinessDescription AS o_InsuranceReferenceLineOfBusinessDescription1,
	outstanding_amt1,
	-- *INF*: IIF(ISNULL(outstanding_amt1),0,outstanding_amt1)
	IFF(outstanding_amt1 IS NULL, 0, outstanding_amt1) AS v_outstanding_amt1,
	v_outstanding_amt1 AS o_outstanding_amt1,
	paid_loss_amt1,
	-- *INF*: IIF(ISNULL(paid_loss_amt),0,paid_loss_amt1)
	IFF(paid_loss_amt IS NULL, 0, paid_loss_amt1) AS v_paid_loss_amt1,
	v_paid_loss_amt1 AS o_paid_loss_amt1,
	paid_exp_amt1,
	-- *INF*: IIF(ISNULL(paid_exp_amt1),0,paid_exp_amt1)
	IFF(paid_exp_amt1 IS NULL, 0, paid_exp_amt1) AS v_paid_exp_amt1,
	v_paid_exp_amt1 AS o_paid_exp_amt1,
	ChangeInOutstandingAmount1,
	-- *INF*: IIF(ISNULL(ChangeInOutstandingAmount1),0,ChangeInOutstandingAmount1)
	IFF(ChangeInOutstandingAmount1 IS NULL, 0, ChangeInOutstandingAmount1) AS v_ChangeInOutstandingAmount1,
	v_ChangeInOutstandingAmount1 AS o_ChangeInOutstandingAmount1,
	DirectLossPaidER1,
	-- *INF*: IIF(ISNULL(DirectLossPaidER1),0,DirectLossPaidER1)
	IFF(DirectLossPaidER1 IS NULL, 0, DirectLossPaidER1) AS v_DirectLossPaidER1,
	v_DirectLossPaidER1 AS o_DirectLossPaidER1,
	DirectLossPaidIR1,
	-- *INF*: IIF(ISNULL(DirectLossPaidIR1),0,DirectLossPaidIR1)
	IFF(DirectLossPaidIR1 IS NULL, 0, DirectLossPaidIR1) AS v_DirectLossPaidIR1,
	v_DirectLossPaidIR1 AS o_DirectLossPaidIR1,
	DirectALAEPaidER1,
	-- *INF*: IIF(ISNULL(DirectALAEPaidER1),0,DirectALAEPaidER1)
	IFF(DirectALAEPaidER1 IS NULL, 0, DirectALAEPaidER1) AS v_DirectALAEPaidER1,
	v_DirectALAEPaidER1 AS o_DirectALAEPaidER1,
	DirectALAEPaidIR1,
	-- *INF*: IIF(ISNULL(DirectALAEPaidIR1),0,DirectALAEPaidIR1)
	IFF(DirectALAEPaidIR1 IS NULL, 0, DirectALAEPaidIR1) AS v_DirectALAEPaidIR1,
	v_DirectALAEPaidIR1 AS o_DirectALAEPaidIR1,
	DirectSalvagePaid1,
	-- *INF*: IIF(ISNULL(DirectSalvagePaid1),0,DirectSalvagePaid1)
	IFF(DirectSalvagePaid1 IS NULL, 0, DirectSalvagePaid1) AS v_DirectSalvagePaid1,
	v_DirectSalvagePaid1 AS o_DirectSalvagePaid1,
	DirectSubrogationPaid1,
	-- *INF*: IIF(ISNULL(DirectSubrogationPaid1),0,DirectSubrogationPaid1)
	IFF(DirectSubrogationPaid1 IS NULL, 0, DirectSubrogationPaid1) AS v_DirectSubrogationPaid1,
	v_DirectSubrogationPaid1 AS o_DirectSubrogationPaid1,
	DirectOtherRecoveryPaid1,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryPaid1),0,DirectOtherRecoveryPaid1)
	IFF(DirectOtherRecoveryPaid1 IS NULL, 0, DirectOtherRecoveryPaid1) AS v_DirectOtherRecoveryPaid1,
	v_DirectOtherRecoveryPaid1 AS o_DirectOtherRecoveryPaid1,
	DirectOtherRecoveryLossPaid1,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryLossPaid1),0,DirectOtherRecoveryLossPaid1)
	IFF(DirectOtherRecoveryLossPaid1 IS NULL, 0, DirectOtherRecoveryLossPaid1) AS v_DirectOtherRecoveryLossPaid1,
	v_DirectOtherRecoveryLossPaid1 AS o_DirectOtherRecoveryLossPaid1,
	DirectOtherRecoveryALAEPaid1,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryALAEPaid1),0,DirectOtherRecoveryALAEPaid1)
	IFF(DirectOtherRecoveryALAEPaid1 IS NULL, 0, DirectOtherRecoveryALAEPaid1) AS v_DirectOtherRecoveryALAEPaid1,
	v_DirectOtherRecoveryALAEPaid1 AS o_DirectOtherRecoveryALAEPaid1,
	DirectLossOutstandingER1,
	-- *INF*: IIF(ISNULL(DirectLossOutstandingER1),0,DirectLossOutstandingER1)
	IFF(DirectLossOutstandingER1 IS NULL, 0, DirectLossOutstandingER1) AS v_DirectLossOutstandingER1,
	v_DirectLossOutstandingER1 AS o_DirectLossOutstandingER1,
	DirectLossOutstandingIR1,
	-- *INF*: IIF(ISNULL(DirectLossOutstandingIR1),0,DirectLossOutstandingIR1)
	IFF(DirectLossOutstandingIR1 IS NULL, 0, DirectLossOutstandingIR1) AS v_DirectLossOutstandingIR1,
	v_DirectLossOutstandingIR1 AS o_DirectLossOutstandingIR1,
	DirectALAEOutstandingER1,
	-- *INF*: IIF(ISNULL(DirectALAEOutstandingER1),0,DirectALAEOutstandingER1)
	IFF(DirectALAEOutstandingER1 IS NULL, 0, DirectALAEOutstandingER1) AS v_DirectALAEOutstandingER1,
	v_DirectALAEOutstandingER1 AS o_DirectALAEOutstandingER1,
	DirectALAEOutstandingIR1,
	-- *INF*: IIF(ISNULL(DirectALAEOutstandingIR1),0,DirectALAEOutstandingIR1)
	IFF(DirectALAEOutstandingIR1 IS NULL, 0, DirectALAEOutstandingIR1) AS v_DirectALAEOutstandingIR1,
	v_DirectALAEOutstandingIR1 AS o_DirectALAEOutstandingIR1,
	DirectOtherRecoveryOutstanding1,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryOutstanding1),0,DirectOtherRecoveryOutstanding1)
	IFF(DirectOtherRecoveryOutstanding1 IS NULL, 0, DirectOtherRecoveryOutstanding1) AS v_DirectOtherRecoveryOutstanding1,
	v_DirectOtherRecoveryOutstanding1 AS o_DirectOtherRecoveryOutstanding1,
	DirectOtherRecoveryLossOutstanding1,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryLossOutstanding1),0,DirectOtherRecoveryLossOutstanding1)
	IFF(DirectOtherRecoveryLossOutstanding1 IS NULL, 0, DirectOtherRecoveryLossOutstanding1) AS v_DirectOtherRecoveryLossOutstanding1,
	v_DirectOtherRecoveryLossOutstanding1 AS o_DirectOtherRecoveryLossOutstanding1,
	DirectOtherRecoveryALAEOutstanding1,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryALAEOutstanding1),0,DirectOtherRecoveryALAEOutstanding1)
	IFF(DirectOtherRecoveryALAEOutstanding1 IS NULL, 0, DirectOtherRecoveryALAEOutstanding1) AS v_DirectOtherRecoveryALAEOutstanding1,
	v_DirectOtherRecoveryALAEOutstanding1 AS o_DirectOtherRecoveryALAEOutstanding1,
	DirectSubroOutstanding1,
	-- *INF*: IIF(ISNULL(DirectSubroOutstanding1),0,DirectSubroOutstanding1)
	IFF(DirectSubroOutstanding1 IS NULL, 0, DirectSubroOutstanding1) AS v_DirectSubroOutstanding1,
	v_DirectSubroOutstanding1 AS o_DirectSubroOutstanding1,
	DirectSalvageOutstanding1,
	-- *INF*: IIF(ISNULL(DirectSalvageOutstanding1),0,DirectSalvageOutstanding1)
	IFF(DirectSalvageOutstanding1 IS NULL, 0, DirectSalvageOutstanding1) AS v_DirectSalvageOutstanding1,
	v_DirectSalvageOutstanding1 AS o_DirectSalvageOutstanding1,
	DirectLossIncurredER1,
	-- *INF*: IIF(ISNULL(DirectLossIncurredER1),0,DirectLossIncurredER1)
	IFF(DirectLossIncurredER1 IS NULL, 0, DirectLossIncurredER1) AS v_DirectLossIncurredER1,
	v_DirectLossIncurredER1 AS o_DirectLossIncurredER1,
	DirectLossIncurredIR1,
	-- *INF*: IIF(ISNULL(DirectLossIncurredIR1),0,DirectLossIncurredIR1)
	IFF(DirectLossIncurredIR1 IS NULL, 0, DirectLossIncurredIR1) AS v_DirectLossIncurredIR1,
	v_DirectLossIncurredIR1 AS o_DirectLossIncurredIR1,
	DirectALAEIncurredER1,
	-- *INF*: IIF(ISNULL(DirectALAEIncurredER1),0,DirectALAEIncurredER1)
	IFF(DirectALAEIncurredER1 IS NULL, 0, DirectALAEIncurredER1) AS v_DirectALAEIncurredER1,
	v_DirectALAEIncurredER1 AS o_DirectALAEIncurredER1,
	DirectALAEIncurredIR1,
	-- *INF*: IIF(ISNULL(DirectALAEIncurredIR1),0,DirectALAEIncurredIR1)
	IFF(DirectALAEIncurredIR1 IS NULL, 0, DirectALAEIncurredIR1) AS v_DirectALAEIncurredIR1,
	v_DirectALAEIncurredIR1 AS o_DirectALAEIncurredIR1,
	'Difference' AS table_name11,
	-- *INF*: IIF(NOT ISNULL(clndr_date),clndr_date,clndr_date1)
	IFF(NOT clndr_date IS NULL, clndr_date, clndr_date1) AS clndr_date11,
	v_EnterpriseGroupDescription AS o_EnterpriseGroupDescription11,
	v_StrategicProfitCenterDescription AS o_StrategicProfitCenterDescription11,
	v_InsuranceReferenceLegalEntityDescription AS o_InsuranceReferenceLegalEntityDescription11,
	v_PolicyOfferingDescription AS o_PolicyOfferingDescription11,
	v_ProductDescription AS o_ProductDescription11,
	v_InsuranceReferenceLineOfBusinessDescription AS o_InsuranceReferenceLineOfBusinessDescription11,
	v_outstanding_amt-v_outstanding_amt1 AS outstanding_amt11,
	v_paid_loss_amt-v_paid_loss_amt1 AS paid_loss_amt11,
	v_paid_exp_amt-v_paid_exp_amt1 AS paid_exp_amt11,
	v_ChangeInOutstandingAmount-v_ChangeInOutstandingAmount1 AS ChangeInOutstandingAmount11,
	v_DirectLossPaidER-v_DirectLossPaidER1 AS DirectLossPaidER11,
	v_DirectLossPaidIR-v_DirectLossPaidIR1 AS DirectLossPaidIR11,
	v_DirectALAEPaidER-v_DirectALAEPaidER1 AS DirectALAEPaidER11,
	v_DirectALAEPaidIR-v_DirectALAEPaidIR1 AS DirectALAEPaidIR11,
	v_DirectSalvagePaid-v_DirectSalvagePaid1 AS DirectSalvagePaid11,
	v_DirectSubrogationPaid-v_DirectSubrogationPaid1 AS DirectSubrogationPaid11,
	v_DirectOtherRecoveryPaid-v_DirectOtherRecoveryPaid1 AS DirectOtherRecoveryPaid11,
	v_DirectOtherRecoveryLossPaid-v_DirectOtherRecoveryLossPaid1 AS DirectOtherRecoveryLossPaid11,
	v_DirectOtherRecoveryALAEPaid-v_DirectOtherRecoveryALAEPaid1 AS DirectOtherRecoveryALAEPaid11,
	v_DirectLossOutstandingER-v_DirectLossOutstandingER1 AS DirectLossOutstandingER11,
	v_DirectLossOutstandingIR-v_DirectLossOutstandingIR1 AS DirectLossOutstandingIR11,
	v_DirectALAEOutstandingER-v_DirectALAEOutstandingER1 AS DirectALAEOutstandingER11,
	v_DirectALAEOutstandingIR-v_DirectALAEOutstandingIR1 AS DirectALAEOutstandingIR11,
	v_DirectOtherRecoveryOutstanding-v_DirectOtherRecoveryOutstanding1 AS DirectOtherRecoveryOutstanding11,
	v_DirectOtherRecoveryLossOutstanding-v_DirectOtherRecoveryLossOutstanding1 AS DirectOtherRecoveryLossOutstanding11,
	v_DirectOtherRecoveryALAEOutstanding-v_DirectOtherRecoveryALAEOutstanding1 AS DirectOtherRecoveryALAEOutstanding11,
	v_DirectSubroOutstanding-v_DirectSubroOutstanding1 AS DirectSubroOutstanding11,
	v_DirectSalvageOutstanding-v_DirectSalvageOutstanding1 AS DirectSalvageOutstanding11,
	v_DirectLossIncurredER-v_DirectLossIncurredER1 AS DirectLossIncurredER11,
	v_DirectLossIncurredIR-v_DirectLossIncurredIR1 AS DirectLossIncurredIR11,
	v_DirectALAEIncurredER-v_DirectALAEIncurredER1 AS DirectALAEIncurredER11,
	v_DirectALAEIncurredIR-v_DirectALAEIncurredIR1 AS DirectALAEIncurredIR11
	FROM JNR_loss_master_fact_vwLossMasterFact1
),
Union3 AS (
	SELECT table_name, o_clndr_date AS clndr_date, o_EnterpriseGroupDescription AS EnterpriseGroupDescription, o_StrategicProfitCenterDescription AS StrategicProfitCenterDescription, o_InsuranceReferenceLegalEntityDescription AS InsuranceReferenceLegalEntityDescription, o_PolicyOfferingDescription AS PolicyOfferingDescription, o_ProductDescription AS ProductDescription, o_InsuranceReferenceLineOfBusinessDescription AS InsuranceReferenceLineOfBusinessDescription, o_outstanding_amt AS outstanding_amt, o_paid_loss_amt AS paid_loss_amt, o_paid_exp_amt AS paid_exp_amt, o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount, o_DirectLossPaidER AS DirectLossPaidER, o_DirectLossPaidIR AS DirectLossPaidIR, o_DirectALAEPaidER AS DirectALAEPaidER, o_DirectALAEPaidIR AS DirectALAEPaidIR, o_DirectSalvagePaid AS DirectSalvagePaid, o_DirectSubrogationPaid AS DirectSubrogationPaid, o_DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid, o_DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid, o_DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid, o_DirectLossOutstandingER AS DirectLossOutstandingER, o_DirectLossOutstandingIR AS DirectLossOutstandingIR, o_DirectALAEOutstandingER AS DirectALAEOutstandingER, o_DirectALAEOutstandingIR AS DirectALAEOutstandingIR, o_DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding, o_DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding, o_DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding, o_DirectSubroOutstanding AS DirectSubroOutstanding, o_DirectSalvageOutstanding AS DirectSalvageOutstanding, o_DirectLossIncurredER AS DirectLossIncurredER, o_DirectLossIncurredIR AS DirectLossIncurredIR, o_DirectALAEIncurredER AS DirectALAEIncurredER, o_DirectALAEIncurredIR AS DirectALAEIncurredIR
	FROM 
	UNION
	SELECT table_name1 AS table_name, o_clndr_date1 AS clndr_date, o_EnterpriseGroupDescription1 AS EnterpriseGroupDescription, o_StrategicProfitCenterDescription1 AS StrategicProfitCenterDescription, o_InsuranceReferenceLegalEntityDescription1 AS InsuranceReferenceLegalEntityDescription, o_PolicyOfferingDescription1 AS PolicyOfferingDescription, o_ProductDescription1 AS ProductDescription, o_InsuranceReferenceLineOfBusinessDescription1 AS InsuranceReferenceLineOfBusinessDescription, o_outstanding_amt1 AS outstanding_amt, o_paid_loss_amt1 AS paid_loss_amt, o_paid_exp_amt1 AS paid_exp_amt, o_ChangeInOutstandingAmount1 AS ChangeInOutstandingAmount, o_DirectLossPaidER1 AS DirectLossPaidER, o_DirectLossPaidIR1 AS DirectLossPaidIR, o_DirectALAEPaidER1 AS DirectALAEPaidER, o_DirectALAEPaidIR1 AS DirectALAEPaidIR, o_DirectSalvagePaid1 AS DirectSalvagePaid, o_DirectSubrogationPaid1 AS DirectSubrogationPaid, o_DirectOtherRecoveryPaid1 AS DirectOtherRecoveryPaid, o_DirectOtherRecoveryLossPaid1 AS DirectOtherRecoveryLossPaid, o_DirectOtherRecoveryALAEPaid1 AS DirectOtherRecoveryALAEPaid, o_DirectLossOutstandingER1 AS DirectLossOutstandingER, o_DirectLossOutstandingIR1 AS DirectLossOutstandingIR, o_DirectALAEOutstandingER1 AS DirectALAEOutstandingER, o_DirectALAEOutstandingIR1 AS DirectALAEOutstandingIR, o_DirectOtherRecoveryOutstanding1 AS DirectOtherRecoveryOutstanding, o_DirectOtherRecoveryLossOutstanding1 AS DirectOtherRecoveryLossOutstanding, o_DirectOtherRecoveryALAEOutstanding1 AS DirectOtherRecoveryALAEOutstanding, o_DirectSubroOutstanding1 AS DirectSubroOutstanding, o_DirectSalvageOutstanding1 AS DirectSalvageOutstanding, o_DirectLossIncurredER1 AS DirectLossIncurredER, o_DirectLossIncurredIR1 AS DirectLossIncurredIR, o_DirectALAEIncurredER1 AS DirectALAEIncurredER, o_DirectALAEIncurredIR1 AS DirectALAEIncurredIR
	FROM 
	UNION
	SELECT table_name11 AS table_name, clndr_date11 AS clndr_date, o_EnterpriseGroupDescription11 AS EnterpriseGroupDescription, o_StrategicProfitCenterDescription11 AS StrategicProfitCenterDescription, o_InsuranceReferenceLegalEntityDescription11 AS InsuranceReferenceLegalEntityDescription, o_PolicyOfferingDescription11 AS PolicyOfferingDescription, o_ProductDescription11 AS ProductDescription, o_InsuranceReferenceLineOfBusinessDescription11 AS InsuranceReferenceLineOfBusinessDescription, outstanding_amt11 AS outstanding_amt, paid_loss_amt11 AS paid_loss_amt, paid_exp_amt11 AS paid_exp_amt, ChangeInOutstandingAmount11 AS ChangeInOutstandingAmount, DirectLossPaidER11 AS DirectLossPaidER, DirectLossPaidIR11 AS DirectLossPaidIR, DirectALAEPaidER11 AS DirectALAEPaidER, DirectALAEPaidIR11 AS DirectALAEPaidIR, DirectSalvagePaid11 AS DirectSalvagePaid, DirectSubrogationPaid11 AS DirectSubrogationPaid, DirectOtherRecoveryPaid11 AS DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid11 AS DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid11 AS DirectOtherRecoveryALAEPaid, DirectLossOutstandingER11 AS DirectLossOutstandingER, DirectLossOutstandingIR11 AS DirectLossOutstandingIR, DirectALAEOutstandingER11 AS DirectALAEOutstandingER, DirectALAEOutstandingIR11 AS DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding11 AS DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding11 AS DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding11 AS DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding11 AS DirectSubroOutstanding, DirectSalvageOutstanding11 AS DirectSalvageOutstanding, DirectLossIncurredER11 AS DirectLossIncurredER, DirectLossIncurredIR11 AS DirectLossIncurredIR, DirectALAEIncurredER11 AS DirectALAEIncurredER, DirectALAEIncurredIR11 AS DirectALAEIncurredIR
	FROM 
),
AGG_ByEnterpriseGroup AS (
	SELECT
	table_name,
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	-- *INF*: sum(outstanding_amt)
	sum(outstanding_amt) AS o_outstanding_amt,
	paid_loss_amt,
	-- *INF*: sum(paid_loss_amt)
	sum(paid_loss_amt) AS o_paid_loss_amt,
	paid_exp_amt,
	-- *INF*: sum(paid_exp_amt)
	sum(paid_exp_amt) AS o_paid_exp_amt,
	ChangeInOutstandingAmount,
	-- *INF*: sum(ChangeInOutstandingAmount)
	sum(ChangeInOutstandingAmount) AS o_ChangeInOutstandingAmount,
	DirectLossPaidER,
	-- *INF*: sum(DirectLossPaidER)
	sum(DirectLossPaidER) AS o_DirectLossPaidER,
	DirectLossPaidIR,
	-- *INF*: sum(DirectLossPaidIR)
	sum(DirectLossPaidIR) AS o_DirectLossPaidIR,
	DirectALAEPaidER,
	-- *INF*: sum(DirectALAEPaidER)
	sum(DirectALAEPaidER) AS o_DirectALAEPaidER,
	DirectALAEPaidIR,
	-- *INF*: sum(DirectALAEPaidIR)
	sum(DirectALAEPaidIR) AS o_DirectALAEPaidIR,
	DirectSalvagePaid,
	-- *INF*: sum(DirectSalvagePaid)
	sum(DirectSalvagePaid) AS o_DirectSalvagePaid,
	DirectSubrogationPaid,
	-- *INF*: sum(DirectSubrogationPaid)
	sum(DirectSubrogationPaid) AS o_DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	-- *INF*: sum(DirectOtherRecoveryPaid)
	sum(DirectOtherRecoveryPaid) AS o_DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	-- *INF*: sum(DirectOtherRecoveryLossPaid)
	sum(DirectOtherRecoveryLossPaid) AS o_DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	-- *INF*: sum(DirectOtherRecoveryALAEPaid)
	sum(DirectOtherRecoveryALAEPaid) AS o_DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	-- *INF*: sum(DirectLossOutstandingER)
	sum(DirectLossOutstandingER) AS o_DirectLossOutstandingER,
	DirectLossOutstandingIR,
	-- *INF*: sum(DirectLossOutstandingIR)
	sum(DirectLossOutstandingIR) AS o_DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	-- *INF*: sum(DirectALAEOutstandingER)
	sum(DirectALAEOutstandingER) AS o_DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	-- *INF*: sum(DirectALAEOutstandingIR)
	sum(DirectALAEOutstandingIR) AS o_DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	-- *INF*: sum(DirectOtherRecoveryOutstanding)
	sum(DirectOtherRecoveryOutstanding) AS o_DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	-- *INF*: sum(DirectOtherRecoveryLossOutstanding)
	sum(DirectOtherRecoveryLossOutstanding) AS o_DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	-- *INF*: sum(DirectOtherRecoveryALAEOutstanding)
	sum(DirectOtherRecoveryALAEOutstanding) AS o_DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	-- *INF*: sum(DirectSubroOutstanding)
	sum(DirectSubroOutstanding) AS o_DirectSubroOutstanding,
	DirectSalvageOutstanding,
	-- *INF*: sum(DirectSalvageOutstanding)
	sum(DirectSalvageOutstanding) AS o_DirectSalvageOutstanding,
	DirectLossIncurredER,
	-- *INF*: sum(DirectLossIncurredER)
	sum(DirectLossIncurredER) AS o_DirectLossIncurredER,
	DirectLossIncurredIR,
	-- *INF*: sum(DirectLossIncurredIR)
	sum(DirectLossIncurredIR) AS o_DirectLossIncurredIR,
	DirectALAEIncurredER,
	-- *INF*: sum(DirectALAEIncurredER)
	sum(DirectALAEIncurredER) AS o_DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: sum(DirectALAEIncurredIR)
	sum(DirectALAEIncurredIR) AS o_DirectALAEIncurredIR
	FROM Union3
	GROUP BY table_name, clndr_date, EnterpriseGroupDescription
),
EXP_ByEnterpriseGroup AS (
	SELECT
	'Balance loss_master_fact to vwLossMasterFact' AS BalancingDescription,
	table_name,
	clndr_date,
	EnterpriseGroupDescription,
	o_outstanding_amt AS outstanding_amt,
	o_paid_loss_amt AS paid_loss_amt,
	o_paid_exp_amt AS paid_exp_amt,
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount,
	o_DirectLossPaidER AS DirectLossPaidER,
	o_DirectLossPaidIR AS DirectLossPaidIR,
	o_DirectALAEPaidER AS DirectALAEPaidER,
	o_DirectALAEPaidIR AS DirectALAEPaidIR,
	o_DirectSalvagePaid AS DirectSalvagePaid,
	o_DirectSubrogationPaid AS DirectSubrogationPaid,
	o_DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid,
	o_DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid,
	o_DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid,
	o_DirectLossOutstandingER AS DirectLossOutstandingER,
	o_DirectLossOutstandingIR AS DirectLossOutstandingIR,
	o_DirectALAEOutstandingER AS DirectALAEOutstandingER,
	o_DirectALAEOutstandingIR AS DirectALAEOutstandingIR,
	o_DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding,
	o_DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding,
	o_DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding,
	o_DirectSubroOutstanding AS DirectSubroOutstanding,
	o_DirectSalvageOutstanding AS DirectSalvageOutstanding,
	o_DirectLossIncurredER AS DirectLossIncurredER,
	o_DirectLossIncurredIR AS DirectLossIncurredIR,
	o_DirectALAEIncurredER AS DirectALAEIncurredER,
	o_DirectALAEIncurredIR AS DirectALAEIncurredIR,
	-- *INF*: DECODE(TRUE,
	-- Table_name='loss_master_fact',1,
	-- Table_name='vwLossMasterFact',2,
	-- Table_name='Difference',3
	-- )
	DECODE(TRUE,
		Table_name = 'loss_master_fact', 1,
		Table_name = 'vwLossMasterFact', 2,
		Table_name = 'Difference', 3) AS OrderInd
	FROM AGG_ByEnterpriseGroup
),
SRT_ByEnterpriseGroup AS (
	SELECT
	BalancingDescription, 
	table_name, 
	clndr_date, 
	EnterpriseGroupDescription, 
	outstanding_amt, 
	paid_loss_amt, 
	paid_exp_amt, 
	ChangeInOutstandingAmount, 
	DirectLossPaidER, 
	DirectLossPaidIR, 
	DirectALAEPaidER, 
	DirectALAEPaidIR, 
	DirectSalvagePaid, 
	DirectSubrogationPaid, 
	DirectOtherRecoveryPaid, 
	DirectOtherRecoveryLossPaid, 
	DirectOtherRecoveryALAEPaid, 
	DirectLossOutstandingER, 
	DirectLossOutstandingIR, 
	DirectALAEOutstandingER, 
	DirectALAEOutstandingIR, 
	DirectOtherRecoveryOutstanding, 
	DirectOtherRecoveryLossOutstanding, 
	DirectOtherRecoveryALAEOutstanding, 
	DirectSubroOutstanding, 
	DirectSalvageOutstanding, 
	DirectLossIncurredER, 
	DirectLossIncurredIR, 
	DirectALAEIncurredER, 
	DirectALAEIncurredIR, 
	OrderInd
	FROM EXP_ByEnterpriseGroup
	ORDER BY clndr_date ASC, EnterpriseGroupDescription ASC, OrderInd ASC
),
EXP_claim_loss_transaction_fact1 AS (
	SELECT
	'claim_loss_transaction_fact' AS table_name,
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	0 AS outstanding_amt,
	0 AS paid_loss_amt,
	0 AS paid_exp_amt,
	0 AS ChangeInOutstandingAmount,
	DirectLossPaidER,
	DirectLossPaidIR,
	DirectALAEPaidER,
	DirectALAEPaidIR,
	DirectSalvagePaid,
	DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	DirectSalvageOutstanding,
	DirectLossIncurredER,
	DirectLossIncurredIR,
	DirectALAEIncurredER,
	DirectALAEIncurredIR
	FROM SQ_claim_loss_transaction_fact
),
JNR_loss_master_fact_claim_loss_transaction_fact1 AS (SELECT
	EXP_loss_master_fact1.table_name, 
	EXP_loss_master_fact1.clndr_date, 
	EXP_loss_master_fact1.EnterpriseGroupDescription, 
	EXP_loss_master_fact1.StrategicProfitCenterDescription, 
	EXP_loss_master_fact1.InsuranceReferenceLegalEntityDescription, 
	EXP_loss_master_fact1.PolicyOfferingDescription, 
	EXP_loss_master_fact1.ProductDescription, 
	EXP_loss_master_fact1.InsuranceReferenceLineOfBusinessDescription, 
	EXP_loss_master_fact1.outstanding_amt, 
	EXP_loss_master_fact1.paid_loss_amt, 
	EXP_loss_master_fact1.paid_exp_amt, 
	EXP_loss_master_fact1.ChangeInOutstandingAmount, 
	EXP_loss_master_fact1.DirectLossPaidER, 
	EXP_loss_master_fact1.DirectLossPaidIR, 
	EXP_loss_master_fact1.DirectALAEPaidER, 
	EXP_loss_master_fact1.DirectALAEPaidIR, 
	EXP_loss_master_fact1.DirectSalvagePaid, 
	EXP_loss_master_fact1.DirectSubrogationPaid, 
	EXP_loss_master_fact1.DirectOtherRecoveryPaid, 
	EXP_loss_master_fact1.DirectOtherRecoveryLossPaid, 
	EXP_loss_master_fact1.DirectOtherRecoveryALAEPaid, 
	EXP_loss_master_fact1.DirectLossOutstandingER, 
	EXP_loss_master_fact1.DirectLossOutstandingIR, 
	EXP_loss_master_fact1.DirectALAEOutstandingER, 
	EXP_loss_master_fact1.DirectALAEOutstandingIR, 
	EXP_loss_master_fact1.DirectOtherRecoveryOutstanding, 
	EXP_loss_master_fact1.DirectOtherRecoveryLossOutstanding, 
	EXP_loss_master_fact1.DirectOtherRecoveryALAEOutstanding, 
	EXP_loss_master_fact1.DirectSubroOutstanding, 
	EXP_loss_master_fact1.DirectSalvageOutstanding, 
	EXP_loss_master_fact1.DirectLossIncurredER, 
	EXP_loss_master_fact1.DirectLossIncurredIR, 
	EXP_loss_master_fact1.DirectALAEIncurredER, 
	EXP_loss_master_fact1.DirectALAEIncurredIR, 
	EXP_claim_loss_transaction_fact1.table_name AS table_name1, 
	EXP_claim_loss_transaction_fact1.clndr_date AS clndr_date1, 
	EXP_claim_loss_transaction_fact1.EnterpriseGroupDescription AS EnterpriseGroupDescription1, 
	EXP_claim_loss_transaction_fact1.StrategicProfitCenterDescription AS StrategicProfitCenterDescription1, 
	EXP_claim_loss_transaction_fact1.InsuranceReferenceLegalEntityDescription AS InsuranceReferenceLegalEntityDescription1, 
	EXP_claim_loss_transaction_fact1.PolicyOfferingDescription AS PolicyOfferingDescription1, 
	EXP_claim_loss_transaction_fact1.ProductDescription AS ProductDescription1, 
	EXP_claim_loss_transaction_fact1.InsuranceReferenceLineOfBusinessDescription AS InsuranceReferenceLineOfBusinessDescription1, 
	EXP_claim_loss_transaction_fact1.outstanding_amt AS outstanding_amt1, 
	EXP_claim_loss_transaction_fact1.paid_loss_amt AS paid_loss_amt1, 
	EXP_claim_loss_transaction_fact1.paid_exp_amt AS paid_exp_amt1, 
	EXP_claim_loss_transaction_fact1.ChangeInOutstandingAmount AS ChangeInOutstandingAmount1, 
	EXP_claim_loss_transaction_fact1.DirectLossPaidER AS DirectLossPaidER1, 
	EXP_claim_loss_transaction_fact1.DirectLossPaidIR AS DirectLossPaidIR1, 
	EXP_claim_loss_transaction_fact1.DirectALAEPaidER AS DirectALAEPaidER1, 
	EXP_claim_loss_transaction_fact1.DirectALAEPaidIR AS DirectALAEPaidIR1, 
	EXP_claim_loss_transaction_fact1.DirectSalvagePaid AS DirectSalvagePaid1, 
	EXP_claim_loss_transaction_fact1.DirectSubrogationPaid AS DirectSubrogationPaid1, 
	EXP_claim_loss_transaction_fact1.DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid1, 
	EXP_claim_loss_transaction_fact1.DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid1, 
	EXP_claim_loss_transaction_fact1.DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid1, 
	EXP_claim_loss_transaction_fact1.DirectLossOutstandingER AS DirectLossOutstandingER1, 
	EXP_claim_loss_transaction_fact1.DirectLossOutstandingIR AS DirectLossOutstandingIR1, 
	EXP_claim_loss_transaction_fact1.DirectALAEOutstandingER AS DirectALAEOutstandingER1, 
	EXP_claim_loss_transaction_fact1.DirectALAEOutstandingIR AS DirectALAEOutstandingIR1, 
	EXP_claim_loss_transaction_fact1.DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding1, 
	EXP_claim_loss_transaction_fact1.DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding1, 
	EXP_claim_loss_transaction_fact1.DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding1, 
	EXP_claim_loss_transaction_fact1.DirectSubroOutstanding AS DirectSubroOutstanding1, 
	EXP_claim_loss_transaction_fact1.DirectSalvageOutstanding AS DirectSalvageOutstanding1, 
	EXP_claim_loss_transaction_fact1.DirectLossIncurredER AS DirectLossIncurredER1, 
	EXP_claim_loss_transaction_fact1.DirectLossIncurredIR AS DirectLossIncurredIR1, 
	EXP_claim_loss_transaction_fact1.DirectALAEIncurredER AS DirectALAEIncurredER1, 
	EXP_claim_loss_transaction_fact1.DirectALAEIncurredIR AS DirectALAEIncurredIR1
	FROM EXP_loss_master_fact1
	FULL OUTER JOIN EXP_claim_loss_transaction_fact1
	ON EXP_claim_loss_transaction_fact1.clndr_date = EXP_loss_master_fact1.clndr_date AND EXP_claim_loss_transaction_fact1.EnterpriseGroupDescription = EXP_loss_master_fact1.EnterpriseGroupDescription AND EXP_claim_loss_transaction_fact1.StrategicProfitCenterDescription = EXP_loss_master_fact1.StrategicProfitCenterDescription AND EXP_claim_loss_transaction_fact1.InsuranceReferenceLegalEntityDescription = EXP_loss_master_fact1.InsuranceReferenceLegalEntityDescription AND EXP_claim_loss_transaction_fact1.PolicyOfferingDescription = EXP_loss_master_fact1.PolicyOfferingDescription AND EXP_claim_loss_transaction_fact1.ProductDescription = EXP_loss_master_fact1.ProductDescription AND EXP_claim_loss_transaction_fact1.InsuranceReferenceLineOfBusinessDescription = EXP_loss_master_fact1.InsuranceReferenceLineOfBusinessDescription
),
EXP_loss_master_fact_claim_loss_transaction_fact1 AS (
	SELECT
	'loss_master_fact' AS table_name,
	clndr_date,
	-- *INF*: IIF(NOT ISNULL(clndr_date),clndr_date,clndr_date1)
	IFF(NOT clndr_date IS NULL, clndr_date, clndr_date1) AS o_clndr_date,
	-- *INF*: IIF(NOT ISNULL(EnterpriseGroupDescription),EnterpriseGroupDescription,EnterpriseGroupDescription1)
	IFF(NOT EnterpriseGroupDescription IS NULL, EnterpriseGroupDescription, EnterpriseGroupDescription1) AS v_EnterpriseGroupDescription,
	-- *INF*: IIF(NOT ISNULL(StrategicProfitCenterDescription),StrategicProfitCenterDescription,StrategicProfitCenterDescription1)
	IFF(NOT StrategicProfitCenterDescription IS NULL, StrategicProfitCenterDescription, StrategicProfitCenterDescription1) AS v_StrategicProfitCenterDescription,
	-- *INF*: IIF(NOT ISNULL(InsuranceReferenceLegalEntityDescription),InsuranceReferenceLegalEntityDescription,InsuranceReferenceLegalEntityDescription1)
	IFF(NOT InsuranceReferenceLegalEntityDescription IS NULL, InsuranceReferenceLegalEntityDescription, InsuranceReferenceLegalEntityDescription1) AS v_InsuranceReferenceLegalEntityDescription,
	-- *INF*: IIF(NOT ISNULL(PolicyOfferingDescription),PolicyOfferingDescription,PolicyOfferingDescription1)
	IFF(NOT PolicyOfferingDescription IS NULL, PolicyOfferingDescription, PolicyOfferingDescription1) AS v_PolicyOfferingDescription,
	-- *INF*: IIF(NOT ISNULL(ProductDescription),ProductDescription,ProductDescription1)
	IFF(NOT ProductDescription IS NULL, ProductDescription, ProductDescription1) AS v_ProductDescription,
	-- *INF*: IIF(NOT ISNULL(InsuranceReferenceLineOfBusinessDescription),InsuranceReferenceLineOfBusinessDescription,InsuranceReferenceLineOfBusinessDescription1)
	IFF(NOT InsuranceReferenceLineOfBusinessDescription IS NULL, InsuranceReferenceLineOfBusinessDescription, InsuranceReferenceLineOfBusinessDescription1) AS v_InsuranceReferenceLineOfBusinessDescription,
	EnterpriseGroupDescription,
	v_EnterpriseGroupDescription AS o_EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	v_StrategicProfitCenterDescription AS o_StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	v_InsuranceReferenceLegalEntityDescription AS o_InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	v_PolicyOfferingDescription AS o_PolicyOfferingDescription,
	ProductDescription,
	v_ProductDescription AS o_ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	v_InsuranceReferenceLineOfBusinessDescription AS o_InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	-- *INF*: IIF(ISNULL(outstanding_amt),0,outstanding_amt)
	IFF(outstanding_amt IS NULL, 0, outstanding_amt) AS v_outstanding_amt,
	v_outstanding_amt AS o_outstanding_amt,
	paid_loss_amt,
	-- *INF*: IIF(ISNULL(paid_loss_amt),0,paid_loss_amt)
	IFF(paid_loss_amt IS NULL, 0, paid_loss_amt) AS v_paid_loss_amt,
	v_paid_loss_amt AS o_paid_loss_amt,
	paid_exp_amt,
	-- *INF*: IIF(ISNULL(paid_exp_amt),0,paid_exp_amt)
	IFF(paid_exp_amt IS NULL, 0, paid_exp_amt) AS v_paid_exp_amt,
	v_paid_exp_amt AS o_paid_exp_amt,
	ChangeInOutstandingAmount,
	-- *INF*: IIF(ISNULL(ChangeInOutstandingAmount),0,ChangeInOutstandingAmount)
	IFF(ChangeInOutstandingAmount IS NULL, 0, ChangeInOutstandingAmount) AS v_ChangeInOutstandingAmount,
	v_ChangeInOutstandingAmount AS o_ChangeInOutstandingAmount,
	DirectLossPaidER,
	-- *INF*: IIF(ISNULL(DirectLossPaidER),0,DirectLossPaidER)
	IFF(DirectLossPaidER IS NULL, 0, DirectLossPaidER) AS v_DirectLossPaidER,
	v_DirectLossPaidER AS o_DirectLossPaidER,
	DirectLossPaidIR,
	-- *INF*: IIF(ISNULL(DirectLossPaidIR),0,DirectLossPaidIR)
	IFF(DirectLossPaidIR IS NULL, 0, DirectLossPaidIR) AS v_DirectLossPaidIR,
	v_DirectLossPaidIR AS o_DirectLossPaidIR,
	DirectALAEPaidER,
	-- *INF*: IIF(ISNULL(DirectALAEPaidER),0,DirectALAEPaidER)
	IFF(DirectALAEPaidER IS NULL, 0, DirectALAEPaidER) AS v_DirectALAEPaidER,
	v_DirectALAEPaidER AS o_DirectALAEPaidER,
	DirectALAEPaidIR,
	-- *INF*: IIF(ISNULL(DirectALAEPaidIR),0,DirectALAEPaidIR)
	IFF(DirectALAEPaidIR IS NULL, 0, DirectALAEPaidIR) AS v_DirectALAEPaidIR,
	v_DirectALAEPaidIR AS o_DirectALAEPaidIR,
	DirectSalvagePaid,
	-- *INF*: IIF(ISNULL(DirectSalvagePaid),0,DirectSalvagePaid)
	IFF(DirectSalvagePaid IS NULL, 0, DirectSalvagePaid) AS v_DirectSalvagePaid,
	v_DirectSalvagePaid AS o_DirectSalvagePaid,
	DirectSubrogationPaid,
	-- *INF*: IIF(ISNULL(DirectSubrogationPaid),0,DirectSubrogationPaid)
	IFF(DirectSubrogationPaid IS NULL, 0, DirectSubrogationPaid) AS v_DirectSubrogationPaid,
	v_DirectSubrogationPaid AS o_DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryPaid),0,DirectOtherRecoveryPaid)
	IFF(DirectOtherRecoveryPaid IS NULL, 0, DirectOtherRecoveryPaid) AS v_DirectOtherRecoveryPaid,
	v_DirectOtherRecoveryPaid AS o_DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryLossPaid),0,DirectOtherRecoveryLossPaid)
	IFF(DirectOtherRecoveryLossPaid IS NULL, 0, DirectOtherRecoveryLossPaid) AS v_DirectOtherRecoveryLossPaid,
	v_DirectOtherRecoveryLossPaid AS o_DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryALAEPaid),0,DirectOtherRecoveryALAEPaid)
	IFF(DirectOtherRecoveryALAEPaid IS NULL, 0, DirectOtherRecoveryALAEPaid) AS v_DirectOtherRecoveryALAEPaid,
	v_DirectOtherRecoveryALAEPaid AS o_DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	-- *INF*: IIF(ISNULL(DirectLossOutstandingER),0,DirectLossOutstandingER)
	IFF(DirectLossOutstandingER IS NULL, 0, DirectLossOutstandingER) AS v_DirectLossOutstandingER,
	v_DirectLossOutstandingER AS o_DirectLossOutstandingER,
	DirectLossOutstandingIR,
	-- *INF*: IIF(ISNULL(DirectLossOutstandingIR),0,DirectLossOutstandingIR)
	IFF(DirectLossOutstandingIR IS NULL, 0, DirectLossOutstandingIR) AS v_DirectLossOutstandingIR,
	v_DirectLossOutstandingIR AS o_DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	-- *INF*: IIF(ISNULL(DirectALAEOutstandingER),0,DirectALAEOutstandingER)
	IFF(DirectALAEOutstandingER IS NULL, 0, DirectALAEOutstandingER) AS v_DirectALAEOutstandingER,
	v_DirectALAEOutstandingER AS o_DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	-- *INF*: IIF(ISNULL(DirectALAEOutstandingIR),0,DirectALAEOutstandingIR)
	IFF(DirectALAEOutstandingIR IS NULL, 0, DirectALAEOutstandingIR) AS v_DirectALAEOutstandingIR,
	v_DirectALAEOutstandingIR AS o_DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryOutstanding),0,DirectOtherRecoveryOutstanding)
	IFF(DirectOtherRecoveryOutstanding IS NULL, 0, DirectOtherRecoveryOutstanding) AS v_DirectOtherRecoveryOutstanding,
	v_DirectOtherRecoveryOutstanding AS o_DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryLossOutstanding),0,DirectOtherRecoveryLossOutstanding)
	IFF(DirectOtherRecoveryLossOutstanding IS NULL, 0, DirectOtherRecoveryLossOutstanding) AS v_DirectOtherRecoveryLossOutstanding,
	v_DirectOtherRecoveryLossOutstanding AS o_DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryALAEOutstanding),0,DirectOtherRecoveryALAEOutstanding)
	IFF(DirectOtherRecoveryALAEOutstanding IS NULL, 0, DirectOtherRecoveryALAEOutstanding) AS v_DirectOtherRecoveryALAEOutstanding,
	v_DirectOtherRecoveryALAEOutstanding AS o_DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	-- *INF*: IIF(ISNULL(DirectSubroOutstanding),0,DirectSubroOutstanding)
	IFF(DirectSubroOutstanding IS NULL, 0, DirectSubroOutstanding) AS v_DirectSubroOutstanding,
	v_DirectSubroOutstanding AS o_DirectSubroOutstanding,
	DirectSalvageOutstanding,
	-- *INF*: IIF(ISNULL(DirectSalvageOutstanding),0,DirectSalvageOutstanding)
	IFF(DirectSalvageOutstanding IS NULL, 0, DirectSalvageOutstanding) AS v_DirectSalvageOutstanding,
	v_DirectSalvageOutstanding AS o_DirectSalvageOutstanding,
	DirectLossIncurredER,
	-- *INF*: IIF(ISNULL(DirectLossIncurredER),0,DirectLossIncurredER)
	IFF(DirectLossIncurredER IS NULL, 0, DirectLossIncurredER) AS v_DirectLossIncurredER,
	v_DirectLossIncurredER AS o_DirectLossIncurredER,
	DirectLossIncurredIR,
	-- *INF*: IIF(ISNULL(DirectLossIncurredIR),0,DirectLossIncurredIR)
	IFF(DirectLossIncurredIR IS NULL, 0, DirectLossIncurredIR) AS v_DirectLossIncurredIR,
	v_DirectLossIncurredIR AS o_DirectLossIncurredIR,
	DirectALAEIncurredER,
	-- *INF*: IIF(ISNULL(DirectALAEIncurredER),0,DirectALAEIncurredER)
	IFF(DirectALAEIncurredER IS NULL, 0, DirectALAEIncurredER) AS v_DirectALAEIncurredER,
	v_DirectALAEIncurredER AS o_DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: IIF(ISNULL(DirectALAEIncurredIR),0,DirectALAEIncurredIR)
	IFF(DirectALAEIncurredIR IS NULL, 0, DirectALAEIncurredIR) AS v_DirectALAEIncurredIR,
	v_DirectALAEIncurredIR AS o_DirectALAEIncurredIR,
	'claim_loss_transaction_fact' AS table_name1,
	clndr_date1,
	-- *INF*: IIF(NOT ISNULL(clndr_date1),clndr_date1,clndr_date)
	IFF(NOT clndr_date1 IS NULL, clndr_date1, clndr_date) AS o_clndr_date1,
	EnterpriseGroupDescription1,
	v_EnterpriseGroupDescription AS o_EnterpriseGroupDescription1,
	StrategicProfitCenterDescription1,
	v_StrategicProfitCenterDescription AS o_StrategicProfitCenterDescription1,
	InsuranceReferenceLegalEntityDescription1,
	v_InsuranceReferenceLegalEntityDescription AS o_InsuranceReferenceLegalEntityDescription1,
	PolicyOfferingDescription1,
	v_PolicyOfferingDescription AS o_PolicyOfferingDescription1,
	ProductDescription1,
	v_ProductDescription AS o_ProductDescription1,
	InsuranceReferenceLineOfBusinessDescription1,
	v_InsuranceReferenceLineOfBusinessDescription AS o_InsuranceReferenceLineOfBusinessDescription1,
	outstanding_amt1,
	-- *INF*: IIF(ISNULL(outstanding_amt1),0,outstanding_amt1)
	IFF(outstanding_amt1 IS NULL, 0, outstanding_amt1) AS v_outstanding_amt1,
	v_outstanding_amt1 AS o_outstanding_amt1,
	paid_loss_amt1,
	-- *INF*: IIF(ISNULL(paid_loss_amt),0,paid_loss_amt1)
	IFF(paid_loss_amt IS NULL, 0, paid_loss_amt1) AS v_paid_loss_amt1,
	v_paid_loss_amt1 AS o_paid_loss_amt1,
	paid_exp_amt1,
	-- *INF*: IIF(ISNULL(paid_exp_amt1),0,paid_exp_amt1)
	IFF(paid_exp_amt1 IS NULL, 0, paid_exp_amt1) AS v_paid_exp_amt1,
	v_paid_exp_amt1 AS o_paid_exp_amt1,
	ChangeInOutstandingAmount1,
	-- *INF*: IIF(ISNULL(ChangeInOutstandingAmount1),0,ChangeInOutstandingAmount1)
	IFF(ChangeInOutstandingAmount1 IS NULL, 0, ChangeInOutstandingAmount1) AS v_ChangeInOutstandingAmount1,
	v_ChangeInOutstandingAmount1 AS o_ChangeInOutstandingAmount1,
	DirectLossPaidER1,
	-- *INF*: IIF(ISNULL(DirectLossPaidER1),0,DirectLossPaidER1)
	IFF(DirectLossPaidER1 IS NULL, 0, DirectLossPaidER1) AS v_DirectLossPaidER1,
	v_DirectLossPaidER1 AS o_DirectLossPaidER1,
	DirectLossPaidIR1,
	-- *INF*: IIF(ISNULL(DirectLossPaidIR1),0,DirectLossPaidIR1)
	IFF(DirectLossPaidIR1 IS NULL, 0, DirectLossPaidIR1) AS v_DirectLossPaidIR1,
	v_DirectLossPaidIR1 AS o_DirectLossPaidIR1,
	DirectALAEPaidER1,
	-- *INF*: IIF(ISNULL(DirectALAEPaidER1),0,DirectALAEPaidER1)
	IFF(DirectALAEPaidER1 IS NULL, 0, DirectALAEPaidER1) AS v_DirectALAEPaidER1,
	v_DirectALAEPaidER1 AS o_DirectALAEPaidER1,
	DirectALAEPaidIR1,
	-- *INF*: IIF(ISNULL(DirectALAEPaidIR1),0,DirectALAEPaidIR1)
	IFF(DirectALAEPaidIR1 IS NULL, 0, DirectALAEPaidIR1) AS v_DirectALAEPaidIR1,
	v_DirectALAEPaidIR1 AS o_DirectALAEPaidIR1,
	DirectSalvagePaid1,
	-- *INF*: IIF(ISNULL(DirectSalvagePaid1),0,DirectSalvagePaid1)
	IFF(DirectSalvagePaid1 IS NULL, 0, DirectSalvagePaid1) AS v_DirectSalvagePaid1,
	v_DirectSalvagePaid1 AS o_DirectSalvagePaid1,
	DirectSubrogationPaid1,
	-- *INF*: IIF(ISNULL(DirectSubrogationPaid1),0,DirectSubrogationPaid1)
	IFF(DirectSubrogationPaid1 IS NULL, 0, DirectSubrogationPaid1) AS v_DirectSubrogationPaid1,
	v_DirectSubrogationPaid1 AS o_DirectSubrogationPaid1,
	DirectOtherRecoveryPaid1,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryPaid1),0,DirectOtherRecoveryPaid1)
	IFF(DirectOtherRecoveryPaid1 IS NULL, 0, DirectOtherRecoveryPaid1) AS v_DirectOtherRecoveryPaid1,
	v_DirectOtherRecoveryPaid1 AS o_DirectOtherRecoveryPaid1,
	DirectOtherRecoveryLossPaid1,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryLossPaid1),0,DirectOtherRecoveryLossPaid1)
	IFF(DirectOtherRecoveryLossPaid1 IS NULL, 0, DirectOtherRecoveryLossPaid1) AS v_DirectOtherRecoveryLossPaid1,
	v_DirectOtherRecoveryLossPaid1 AS o_DirectOtherRecoveryLossPaid1,
	DirectOtherRecoveryALAEPaid1,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryALAEPaid1),0,DirectOtherRecoveryALAEPaid1)
	IFF(DirectOtherRecoveryALAEPaid1 IS NULL, 0, DirectOtherRecoveryALAEPaid1) AS v_DirectOtherRecoveryALAEPaid1,
	v_DirectOtherRecoveryALAEPaid1 AS o_DirectOtherRecoveryALAEPaid1,
	DirectLossOutstandingER1,
	-- *INF*: IIF(ISNULL(DirectLossOutstandingER1),0,DirectLossOutstandingER1)
	IFF(DirectLossOutstandingER1 IS NULL, 0, DirectLossOutstandingER1) AS v_DirectLossOutstandingER1,
	v_DirectLossOutstandingER1 AS o_DirectLossOutstandingER1,
	DirectLossOutstandingIR1,
	-- *INF*: IIF(ISNULL(DirectLossOutstandingIR1),0,DirectLossOutstandingIR1)
	IFF(DirectLossOutstandingIR1 IS NULL, 0, DirectLossOutstandingIR1) AS v_DirectLossOutstandingIR1,
	v_DirectLossOutstandingIR1 AS o_DirectLossOutstandingIR1,
	DirectALAEOutstandingER1,
	-- *INF*: IIF(ISNULL(DirectALAEOutstandingER1),0,DirectALAEOutstandingER1)
	IFF(DirectALAEOutstandingER1 IS NULL, 0, DirectALAEOutstandingER1) AS v_DirectALAEOutstandingER1,
	v_DirectALAEOutstandingER1 AS o_DirectALAEOutstandingER1,
	DirectALAEOutstandingIR1,
	-- *INF*: IIF(ISNULL(DirectALAEOutstandingIR1),0,DirectALAEOutstandingIR1)
	IFF(DirectALAEOutstandingIR1 IS NULL, 0, DirectALAEOutstandingIR1) AS v_DirectALAEOutstandingIR1,
	v_DirectALAEOutstandingIR1 AS o_DirectALAEOutstandingIR1,
	DirectOtherRecoveryOutstanding1,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryOutstanding1),0,DirectOtherRecoveryOutstanding1)
	IFF(DirectOtherRecoveryOutstanding1 IS NULL, 0, DirectOtherRecoveryOutstanding1) AS v_DirectOtherRecoveryOutstanding1,
	v_DirectOtherRecoveryOutstanding1 AS o_DirectOtherRecoveryOutstanding1,
	DirectOtherRecoveryLossOutstanding1,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryLossOutstanding1),0,DirectOtherRecoveryLossOutstanding1)
	IFF(DirectOtherRecoveryLossOutstanding1 IS NULL, 0, DirectOtherRecoveryLossOutstanding1) AS v_DirectOtherRecoveryLossOutstanding1,
	v_DirectOtherRecoveryLossOutstanding1 AS o_DirectOtherRecoveryLossOutstanding1,
	DirectOtherRecoveryALAEOutstanding1,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryALAEOutstanding1),0,DirectOtherRecoveryALAEOutstanding1)
	IFF(DirectOtherRecoveryALAEOutstanding1 IS NULL, 0, DirectOtherRecoveryALAEOutstanding1) AS v_DirectOtherRecoveryALAEOutstanding1,
	v_DirectOtherRecoveryALAEOutstanding1 AS o_DirectOtherRecoveryALAEOutstanding1,
	DirectSubroOutstanding1,
	-- *INF*: IIF(ISNULL(DirectSubroOutstanding1),0,DirectSubroOutstanding1)
	IFF(DirectSubroOutstanding1 IS NULL, 0, DirectSubroOutstanding1) AS v_DirectSubroOutstanding1,
	v_DirectSubroOutstanding1 AS o_DirectSubroOutstanding1,
	DirectSalvageOutstanding1,
	-- *INF*: IIF(ISNULL(DirectSalvageOutstanding1),0,DirectSalvageOutstanding1)
	IFF(DirectSalvageOutstanding1 IS NULL, 0, DirectSalvageOutstanding1) AS v_DirectSalvageOutstanding1,
	v_DirectSalvageOutstanding1 AS o_DirectSalvageOutstanding1,
	DirectLossIncurredER1,
	-- *INF*: IIF(ISNULL(DirectLossIncurredER1),0,DirectLossIncurredER1)
	IFF(DirectLossIncurredER1 IS NULL, 0, DirectLossIncurredER1) AS v_DirectLossIncurredER1,
	v_DirectLossIncurredER1 AS o_DirectLossIncurredER1,
	DirectLossIncurredIR1,
	-- *INF*: IIF(ISNULL(DirectLossIncurredIR1),0,DirectLossIncurredIR1)
	IFF(DirectLossIncurredIR1 IS NULL, 0, DirectLossIncurredIR1) AS v_DirectLossIncurredIR1,
	v_DirectLossIncurredIR1 AS o_DirectLossIncurredIR1,
	DirectALAEIncurredER1,
	-- *INF*: IIF(ISNULL(DirectALAEIncurredER1),0,DirectALAEIncurredER1)
	IFF(DirectALAEIncurredER1 IS NULL, 0, DirectALAEIncurredER1) AS v_DirectALAEIncurredER1,
	v_DirectALAEIncurredER1 AS o_DirectALAEIncurredER1,
	DirectALAEIncurredIR1,
	-- *INF*: IIF(ISNULL(DirectALAEIncurredIR1),0,DirectALAEIncurredIR1)
	IFF(DirectALAEIncurredIR1 IS NULL, 0, DirectALAEIncurredIR1) AS v_DirectALAEIncurredIR1,
	v_DirectALAEIncurredIR1 AS o_DirectALAEIncurredIR1,
	'Difference' AS table_name11,
	-- *INF*: IIF(NOT ISNULL(clndr_date),clndr_date,clndr_date1)
	IFF(NOT clndr_date IS NULL, clndr_date, clndr_date1) AS clndr_date11,
	v_EnterpriseGroupDescription AS o_EnterpriseGroupDescription11,
	v_StrategicProfitCenterDescription AS o_StrategicProfitCenterDescription11,
	v_InsuranceReferenceLegalEntityDescription AS o_InsuranceReferenceLegalEntityDescription11,
	v_PolicyOfferingDescription AS o_PolicyOfferingDescription11,
	v_ProductDescription AS o_ProductDescription11,
	v_InsuranceReferenceLineOfBusinessDescription AS o_InsuranceReferenceLineOfBusinessDescription11,
	v_outstanding_amt-v_outstanding_amt1 AS outstanding_amt11,
	v_paid_loss_amt-v_paid_loss_amt1 AS paid_loss_amt11,
	v_paid_exp_amt-v_paid_exp_amt1 AS paid_exp_amt11,
	v_ChangeInOutstandingAmount-v_ChangeInOutstandingAmount1 AS ChangeInOutstandingAmount11,
	v_DirectLossPaidER-v_DirectLossPaidER1 AS DirectLossPaidER11,
	v_DirectLossPaidIR-v_DirectLossPaidIR1 AS DirectLossPaidIR11,
	v_DirectALAEPaidER-v_DirectALAEPaidER1 AS DirectALAEPaidER11,
	v_DirectALAEPaidIR-v_DirectALAEPaidIR1 AS DirectALAEPaidIR11,
	v_DirectSalvagePaid-v_DirectSalvagePaid1 AS DirectSalvagePaid11,
	v_DirectSubrogationPaid-v_DirectSubrogationPaid1 AS DirectSubrogationPaid11,
	v_DirectOtherRecoveryPaid-v_DirectOtherRecoveryPaid1 AS DirectOtherRecoveryPaid11,
	v_DirectOtherRecoveryLossPaid-v_DirectOtherRecoveryLossPaid1 AS DirectOtherRecoveryLossPaid11,
	v_DirectOtherRecoveryALAEPaid-v_DirectOtherRecoveryALAEPaid1 AS DirectOtherRecoveryALAEPaid11,
	v_DirectLossOutstandingER-v_DirectLossOutstandingER1 AS DirectLossOutstandingER11,
	v_DirectLossOutstandingIR-v_DirectLossOutstandingIR1 AS DirectLossOutstandingIR11,
	v_DirectALAEOutstandingER-v_DirectALAEOutstandingER1 AS DirectALAEOutstandingER11,
	v_DirectALAEOutstandingIR-v_DirectALAEOutstandingIR1 AS DirectALAEOutstandingIR11,
	v_DirectOtherRecoveryOutstanding-v_DirectOtherRecoveryOutstanding1 AS DirectOtherRecoveryOutstanding11,
	v_DirectOtherRecoveryLossOutstanding-v_DirectOtherRecoveryLossOutstanding1 AS DirectOtherRecoveryLossOutstanding11,
	v_DirectOtherRecoveryALAEOutstanding-v_DirectOtherRecoveryALAEOutstanding1 AS DirectOtherRecoveryALAEOutstanding11,
	v_DirectSubroOutstanding-v_DirectSubroOutstanding1 AS DirectSubroOutstanding11,
	v_DirectSalvageOutstanding-v_DirectSalvageOutstanding1 AS DirectSalvageOutstanding11,
	v_DirectLossIncurredER-v_DirectLossIncurredER1 AS DirectLossIncurredER11,
	v_DirectLossIncurredIR-v_DirectLossIncurredIR1 AS DirectLossIncurredIR11,
	v_DirectALAEIncurredER-v_DirectALAEIncurredER1 AS DirectALAEIncurredER11,
	v_DirectALAEIncurredIR-v_DirectALAEIncurredIR1 AS DirectALAEIncurredIR11
	FROM JNR_loss_master_fact_claim_loss_transaction_fact1
),
Union31 AS (
	SELECT table_name, o_clndr_date AS clndr_date, o_EnterpriseGroupDescription AS EnterpriseGroupDescription, o_StrategicProfitCenterDescription AS StrategicProfitCenterDescription, o_InsuranceReferenceLegalEntityDescription AS InsuranceReferenceLegalEntityDescription, o_PolicyOfferingDescription AS PolicyOfferingDescription, o_ProductDescription AS ProductDescription, o_InsuranceReferenceLineOfBusinessDescription AS InsuranceReferenceLineOfBusinessDescription, o_outstanding_amt AS outstanding_amt, o_paid_loss_amt AS paid_loss_amt, o_paid_exp_amt AS paid_exp_amt, o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount, o_DirectLossPaidER AS DirectLossPaidER, o_DirectLossPaidIR AS DirectLossPaidIR, o_DirectALAEPaidER AS DirectALAEPaidER, o_DirectALAEPaidIR AS DirectALAEPaidIR, o_DirectSalvagePaid AS DirectSalvagePaid, o_DirectSubrogationPaid AS DirectSubrogationPaid, o_DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid, o_DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid, o_DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid, o_DirectLossOutstandingER AS DirectLossOutstandingER, o_DirectLossOutstandingIR AS DirectLossOutstandingIR, o_DirectALAEOutstandingER AS DirectALAEOutstandingER, o_DirectALAEOutstandingIR AS DirectALAEOutstandingIR, o_DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding, o_DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding, o_DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding, o_DirectSubroOutstanding AS DirectSubroOutstanding, o_DirectSalvageOutstanding AS DirectSalvageOutstanding, o_DirectLossIncurredER AS DirectLossIncurredER, o_DirectLossIncurredIR AS DirectLossIncurredIR, o_DirectALAEIncurredER AS DirectALAEIncurredER, o_DirectALAEIncurredIR AS DirectALAEIncurredIR
	FROM 
	UNION
	SELECT table_name1 AS table_name, o_clndr_date1 AS clndr_date, o_EnterpriseGroupDescription1 AS EnterpriseGroupDescription, o_StrategicProfitCenterDescription1 AS StrategicProfitCenterDescription, o_InsuranceReferenceLegalEntityDescription1 AS InsuranceReferenceLegalEntityDescription, o_PolicyOfferingDescription1 AS PolicyOfferingDescription, o_ProductDescription1 AS ProductDescription, o_InsuranceReferenceLineOfBusinessDescription1 AS InsuranceReferenceLineOfBusinessDescription, o_outstanding_amt1 AS outstanding_amt, o_paid_loss_amt1 AS paid_loss_amt, o_paid_exp_amt1 AS paid_exp_amt, o_ChangeInOutstandingAmount1 AS ChangeInOutstandingAmount, o_DirectLossPaidER1 AS DirectLossPaidER, o_DirectLossPaidIR1 AS DirectLossPaidIR, o_DirectALAEPaidER1 AS DirectALAEPaidER, o_DirectALAEPaidIR1 AS DirectALAEPaidIR, o_DirectSalvagePaid1 AS DirectSalvagePaid, o_DirectSubrogationPaid1 AS DirectSubrogationPaid, o_DirectOtherRecoveryPaid1 AS DirectOtherRecoveryPaid, o_DirectOtherRecoveryLossPaid1 AS DirectOtherRecoveryLossPaid, o_DirectOtherRecoveryALAEPaid1 AS DirectOtherRecoveryALAEPaid, o_DirectLossOutstandingER1 AS DirectLossOutstandingER, o_DirectLossOutstandingIR1 AS DirectLossOutstandingIR, o_DirectALAEOutstandingER1 AS DirectALAEOutstandingER, o_DirectALAEOutstandingIR1 AS DirectALAEOutstandingIR, o_DirectOtherRecoveryOutstanding1 AS DirectOtherRecoveryOutstanding, o_DirectOtherRecoveryLossOutstanding1 AS DirectOtherRecoveryLossOutstanding, o_DirectOtherRecoveryALAEOutstanding1 AS DirectOtherRecoveryALAEOutstanding, o_DirectSubroOutstanding1 AS DirectSubroOutstanding, o_DirectSalvageOutstanding1 AS DirectSalvageOutstanding, o_DirectLossIncurredER1 AS DirectLossIncurredER, o_DirectLossIncurredIR1 AS DirectLossIncurredIR, o_DirectALAEIncurredER1 AS DirectALAEIncurredER, o_DirectALAEIncurredIR1 AS DirectALAEIncurredIR
	FROM 
	UNION
	SELECT table_name11 AS table_name, clndr_date11 AS clndr_date, o_EnterpriseGroupDescription11 AS EnterpriseGroupDescription, o_StrategicProfitCenterDescription11 AS StrategicProfitCenterDescription, o_InsuranceReferenceLegalEntityDescription11 AS InsuranceReferenceLegalEntityDescription, o_PolicyOfferingDescription11 AS PolicyOfferingDescription, o_ProductDescription11 AS ProductDescription, o_InsuranceReferenceLineOfBusinessDescription11 AS InsuranceReferenceLineOfBusinessDescription, outstanding_amt11 AS outstanding_amt, paid_loss_amt11 AS paid_loss_amt, paid_exp_amt11 AS paid_exp_amt, ChangeInOutstandingAmount11 AS ChangeInOutstandingAmount, DirectLossPaidER11 AS DirectLossPaidER, DirectLossPaidIR11 AS DirectLossPaidIR, DirectALAEPaidER11 AS DirectALAEPaidER, DirectALAEPaidIR11 AS DirectALAEPaidIR, DirectSalvagePaid11 AS DirectSalvagePaid, DirectSubrogationPaid11 AS DirectSubrogationPaid, DirectOtherRecoveryPaid11 AS DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid11 AS DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid11 AS DirectOtherRecoveryALAEPaid, DirectLossOutstandingER11 AS DirectLossOutstandingER, DirectLossOutstandingIR11 AS DirectLossOutstandingIR, DirectALAEOutstandingER11 AS DirectALAEOutstandingER, DirectALAEOutstandingIR11 AS DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding11 AS DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding11 AS DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding11 AS DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding11 AS DirectSubroOutstanding, DirectSalvageOutstanding11 AS DirectSalvageOutstanding, DirectLossIncurredER11 AS DirectLossIncurredER, DirectLossIncurredIR11 AS DirectLossIncurredIR, DirectALAEIncurredER11 AS DirectALAEIncurredER, DirectALAEIncurredIR11 AS DirectALAEIncurredIR
	FROM 
),
AGG_ByEnterpriseGroup1 AS (
	SELECT
	table_name,
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	-- *INF*: sum(outstanding_amt)
	sum(outstanding_amt) AS o_outstanding_amt,
	paid_loss_amt,
	-- *INF*: sum(paid_loss_amt)
	sum(paid_loss_amt) AS o_paid_loss_amt,
	paid_exp_amt,
	-- *INF*: sum(paid_exp_amt)
	sum(paid_exp_amt) AS o_paid_exp_amt,
	ChangeInOutstandingAmount,
	-- *INF*: sum(ChangeInOutstandingAmount)
	sum(ChangeInOutstandingAmount) AS o_ChangeInOutstandingAmount,
	DirectLossPaidER,
	-- *INF*: sum(DirectLossPaidER)
	sum(DirectLossPaidER) AS o_DirectLossPaidER,
	DirectLossPaidIR,
	-- *INF*: sum(DirectLossPaidIR)
	sum(DirectLossPaidIR) AS o_DirectLossPaidIR,
	DirectALAEPaidER,
	-- *INF*: sum(DirectALAEPaidER)
	sum(DirectALAEPaidER) AS o_DirectALAEPaidER,
	DirectALAEPaidIR,
	-- *INF*: sum(DirectALAEPaidIR)
	sum(DirectALAEPaidIR) AS o_DirectALAEPaidIR,
	DirectSalvagePaid,
	-- *INF*: sum(DirectSalvagePaid)
	sum(DirectSalvagePaid) AS o_DirectSalvagePaid,
	DirectSubrogationPaid,
	-- *INF*: sum(DirectSubrogationPaid)
	sum(DirectSubrogationPaid) AS o_DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	-- *INF*: sum(DirectOtherRecoveryPaid)
	sum(DirectOtherRecoveryPaid) AS o_DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	-- *INF*: sum(DirectOtherRecoveryLossPaid)
	sum(DirectOtherRecoveryLossPaid) AS o_DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	-- *INF*: sum(DirectOtherRecoveryALAEPaid)
	sum(DirectOtherRecoveryALAEPaid) AS o_DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	-- *INF*: sum(DirectLossOutstandingER)
	sum(DirectLossOutstandingER) AS o_DirectLossOutstandingER,
	DirectLossOutstandingIR,
	-- *INF*: sum(DirectLossOutstandingIR)
	sum(DirectLossOutstandingIR) AS o_DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	-- *INF*: sum(DirectALAEOutstandingER)
	sum(DirectALAEOutstandingER) AS o_DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	-- *INF*: sum(DirectALAEOutstandingIR)
	sum(DirectALAEOutstandingIR) AS o_DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	-- *INF*: sum(DirectOtherRecoveryOutstanding)
	sum(DirectOtherRecoveryOutstanding) AS o_DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	-- *INF*: sum(DirectOtherRecoveryLossOutstanding)
	sum(DirectOtherRecoveryLossOutstanding) AS o_DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	-- *INF*: sum(DirectOtherRecoveryALAEOutstanding)
	sum(DirectOtherRecoveryALAEOutstanding) AS o_DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	-- *INF*: sum(DirectSubroOutstanding)
	sum(DirectSubroOutstanding) AS o_DirectSubroOutstanding,
	DirectSalvageOutstanding,
	-- *INF*: sum(DirectSalvageOutstanding)
	sum(DirectSalvageOutstanding) AS o_DirectSalvageOutstanding,
	DirectLossIncurredER,
	-- *INF*: sum(DirectLossIncurredER)
	sum(DirectLossIncurredER) AS o_DirectLossIncurredER,
	DirectLossIncurredIR,
	-- *INF*: sum(DirectLossIncurredIR)
	sum(DirectLossIncurredIR) AS o_DirectLossIncurredIR,
	DirectALAEIncurredER,
	-- *INF*: sum(DirectALAEIncurredER)
	sum(DirectALAEIncurredER) AS o_DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: sum(DirectALAEIncurredIR)
	sum(DirectALAEIncurredIR) AS o_DirectALAEIncurredIR
	FROM Union31
	GROUP BY table_name, clndr_date, EnterpriseGroupDescription
),
EXP_ByEnterpriseGroup1 AS (
	SELECT
	'Balance loss_master_fact to claim_loss_transaction_fact' AS BalancingDescription,
	table_name,
	clndr_date,
	EnterpriseGroupDescription,
	o_outstanding_amt AS outstanding_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(outstanding_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(outstanding_amt)) AS o_outstanding_amt,
	o_paid_loss_amt AS paid_loss_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_loss_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_loss_amt)) AS o_paid_loss_amt,
	o_paid_exp_amt AS paid_exp_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_exp_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_exp_amt)) AS o_paid_exp_amt,
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(ChangeInOutstandingAmount))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(ChangeInOutstandingAmount)) AS o_ChangeInOutstandingAmount,
	o_DirectLossPaidER AS DirectLossPaidER,
	o_DirectLossPaidIR AS DirectLossPaidIR,
	o_DirectALAEPaidER AS DirectALAEPaidER,
	o_DirectALAEPaidIR AS DirectALAEPaidIR,
	o_DirectSalvagePaid AS DirectSalvagePaid,
	o_DirectSubrogationPaid AS DirectSubrogationPaid,
	o_DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid,
	o_DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid,
	o_DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid,
	o_DirectLossOutstandingER AS DirectLossOutstandingER,
	o_DirectLossOutstandingIR AS DirectLossOutstandingIR,
	o_DirectALAEOutstandingER AS DirectALAEOutstandingER,
	o_DirectALAEOutstandingIR AS DirectALAEOutstandingIR,
	o_DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding,
	o_DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding,
	o_DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding,
	o_DirectSubroOutstanding AS DirectSubroOutstanding,
	o_DirectSalvageOutstanding AS DirectSalvageOutstanding,
	o_DirectLossIncurredER AS DirectLossIncurredER,
	o_DirectLossIncurredIR AS DirectLossIncurredIR,
	o_DirectALAEIncurredER AS DirectALAEIncurredER,
	o_DirectALAEIncurredIR AS DirectALAEIncurredIR,
	-- *INF*: DECODE(TRUE,
	-- table_name='loss_master_fact',1,
	-- table_name='claim_loss_transaction_fact',2,
	-- table_name='Difference',3
	-- )
	DECODE(TRUE,
		table_name = 'loss_master_fact', 1,
		table_name = 'claim_loss_transaction_fact', 2,
		table_name = 'Difference', 3) AS OrderInd
	FROM AGG_ByEnterpriseGroup1
),
SRT_ByEnterpriseGroup1 AS (
	SELECT
	BalancingDescription, 
	table_name, 
	clndr_date, 
	EnterpriseGroupDescription, 
	o_outstanding_amt AS outstanding_amt, 
	o_paid_loss_amt AS paid_loss_amt, 
	o_paid_exp_amt AS paid_exp_amt, 
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount, 
	DirectLossPaidER, 
	DirectLossPaidIR, 
	DirectALAEPaidER, 
	DirectALAEPaidIR, 
	DirectSalvagePaid, 
	DirectSubrogationPaid, 
	DirectOtherRecoveryPaid, 
	DirectOtherRecoveryLossPaid, 
	DirectOtherRecoveryALAEPaid, 
	DirectLossOutstandingER, 
	DirectLossOutstandingIR, 
	DirectALAEOutstandingER, 
	DirectALAEOutstandingIR, 
	DirectOtherRecoveryOutstanding, 
	DirectOtherRecoveryLossOutstanding, 
	DirectOtherRecoveryALAEOutstanding, 
	DirectSubroOutstanding, 
	DirectSalvageOutstanding, 
	DirectLossIncurredER, 
	DirectLossIncurredIR, 
	DirectALAEIncurredER, 
	DirectALAEIncurredIR, 
	OrderInd
	FROM EXP_ByEnterpriseGroup1
	ORDER BY clndr_date ASC, EnterpriseGroupDescription ASC, OrderInd ASC
),
JNR_vwLossMasterFact_claim_loss_transaction_fact1 AS (SELECT
	EXP_vwLossMasterFact1.table_name, 
	EXP_vwLossMasterFact1.clndr_date, 
	EXP_vwLossMasterFact1.EnterpriseGroupDescription, 
	EXP_vwLossMasterFact1.StrategicProfitCenterDescription, 
	EXP_vwLossMasterFact1.InsuranceReferenceLegalEntityDescription, 
	EXP_vwLossMasterFact1.PolicyOfferingDescription, 
	EXP_vwLossMasterFact1.ProductDescription, 
	EXP_vwLossMasterFact1.InsuranceReferenceLineOfBusinessDescription, 
	EXP_vwLossMasterFact1.outstanding_amt, 
	EXP_vwLossMasterFact1.paid_loss_amt, 
	EXP_vwLossMasterFact1.paid_exp_amt, 
	EXP_vwLossMasterFact1.ChangeInOutstandingAmount, 
	EXP_vwLossMasterFact1.DirectLossPaidER, 
	EXP_vwLossMasterFact1.DirectLossPaidIR, 
	EXP_vwLossMasterFact1.DirectALAEPaidER, 
	EXP_vwLossMasterFact1.DirectALAEPaidIR, 
	EXP_vwLossMasterFact1.DirectSalvagePaid, 
	EXP_vwLossMasterFact1.DirectSubrogationPaid, 
	EXP_vwLossMasterFact1.DirectOtherRecoveryPaid, 
	EXP_vwLossMasterFact1.DirectOtherRecoveryLossPaid, 
	EXP_vwLossMasterFact1.DirectOtherRecoveryALAEPaid, 
	EXP_vwLossMasterFact1.DirectLossOutstandingER, 
	EXP_vwLossMasterFact1.DirectLossOutstandingIR, 
	EXP_vwLossMasterFact1.DirectALAEOutstandingER, 
	EXP_vwLossMasterFact1.DirectALAEOutstandingIR, 
	EXP_vwLossMasterFact1.DirectOtherRecoveryOutstanding, 
	EXP_vwLossMasterFact1.DirectOtherRecoveryLossOutstanding, 
	EXP_vwLossMasterFact1.DirectOtherRecoveryALAEOutstanding, 
	EXP_vwLossMasterFact1.DirectSubroOutstanding, 
	EXP_vwLossMasterFact1.DirectSalvageOutstanding, 
	EXP_vwLossMasterFact1.DirectLossIncurredER, 
	EXP_vwLossMasterFact1.DirectLossIncurredIR, 
	EXP_vwLossMasterFact1.DirectALAEIncurredER, 
	EXP_vwLossMasterFact1.DirectALAEIncurredIR, 
	EXP_claim_loss_transaction_fact1.table_name AS table_name1, 
	EXP_claim_loss_transaction_fact1.clndr_date AS clndr_date1, 
	EXP_claim_loss_transaction_fact1.EnterpriseGroupDescription AS EnterpriseGroupDescription1, 
	EXP_claim_loss_transaction_fact1.StrategicProfitCenterDescription AS StrategicProfitCenterDescription1, 
	EXP_claim_loss_transaction_fact1.InsuranceReferenceLegalEntityDescription AS InsuranceReferenceLegalEntityDescription1, 
	EXP_claim_loss_transaction_fact1.PolicyOfferingDescription AS PolicyOfferingDescription1, 
	EXP_claim_loss_transaction_fact1.ProductDescription AS ProductDescription1, 
	EXP_claim_loss_transaction_fact1.InsuranceReferenceLineOfBusinessDescription AS InsuranceReferenceLineOfBusinessDescription1, 
	EXP_claim_loss_transaction_fact1.outstanding_amt AS outstanding_amt1, 
	EXP_claim_loss_transaction_fact1.paid_loss_amt AS paid_loss_amt1, 
	EXP_claim_loss_transaction_fact1.paid_exp_amt AS paid_exp_amt1, 
	EXP_claim_loss_transaction_fact1.ChangeInOutstandingAmount AS ChangeInOutstandingAmount1, 
	EXP_claim_loss_transaction_fact1.DirectLossPaidER AS DirectLossPaidER1, 
	EXP_claim_loss_transaction_fact1.DirectLossPaidIR AS DirectLossPaidIR1, 
	EXP_claim_loss_transaction_fact1.DirectALAEPaidER AS DirectALAEPaidER1, 
	EXP_claim_loss_transaction_fact1.DirectALAEPaidIR AS DirectALAEPaidIR1, 
	EXP_claim_loss_transaction_fact1.DirectSalvagePaid AS DirectSalvagePaid1, 
	EXP_claim_loss_transaction_fact1.DirectSubrogationPaid AS DirectSubrogationPaid1, 
	EXP_claim_loss_transaction_fact1.DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid1, 
	EXP_claim_loss_transaction_fact1.DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid1, 
	EXP_claim_loss_transaction_fact1.DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid1, 
	EXP_claim_loss_transaction_fact1.DirectLossOutstandingER AS DirectLossOutstandingER1, 
	EXP_claim_loss_transaction_fact1.DirectLossOutstandingIR AS DirectLossOutstandingIR1, 
	EXP_claim_loss_transaction_fact1.DirectALAEOutstandingER AS DirectALAEOutstandingER1, 
	EXP_claim_loss_transaction_fact1.DirectALAEOutstandingIR AS DirectALAEOutstandingIR1, 
	EXP_claim_loss_transaction_fact1.DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding1, 
	EXP_claim_loss_transaction_fact1.DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding1, 
	EXP_claim_loss_transaction_fact1.DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding1, 
	EXP_claim_loss_transaction_fact1.DirectSubroOutstanding AS DirectSubroOutstanding1, 
	EXP_claim_loss_transaction_fact1.DirectSalvageOutstanding AS DirectSalvageOutstanding1, 
	EXP_claim_loss_transaction_fact1.DirectLossIncurredER AS DirectLossIncurredER1, 
	EXP_claim_loss_transaction_fact1.DirectLossIncurredIR AS DirectLossIncurredIR1, 
	EXP_claim_loss_transaction_fact1.DirectALAEIncurredER AS DirectALAEIncurredER1, 
	EXP_claim_loss_transaction_fact1.DirectALAEIncurredIR AS DirectALAEIncurredIR1
	FROM EXP_vwLossMasterFact1
	FULL OUTER JOIN EXP_claim_loss_transaction_fact1
	ON EXP_claim_loss_transaction_fact1.clndr_date = EXP_vwLossMasterFact1.clndr_date AND EXP_claim_loss_transaction_fact1.EnterpriseGroupDescription = EXP_vwLossMasterFact1.EnterpriseGroupDescription AND EXP_claim_loss_transaction_fact1.StrategicProfitCenterDescription = EXP_vwLossMasterFact1.StrategicProfitCenterDescription AND EXP_claim_loss_transaction_fact1.InsuranceReferenceLegalEntityDescription = EXP_vwLossMasterFact1.InsuranceReferenceLegalEntityDescription AND EXP_claim_loss_transaction_fact1.PolicyOfferingDescription = EXP_vwLossMasterFact1.PolicyOfferingDescription AND EXP_claim_loss_transaction_fact1.ProductDescription = EXP_vwLossMasterFact1.ProductDescription AND EXP_claim_loss_transaction_fact1.InsuranceReferenceLineOfBusinessDescription = EXP_vwLossMasterFact1.InsuranceReferenceLineOfBusinessDescription
),
EXP_vwLossMasterFact_claim_loss_transaction_fact1 AS (
	SELECT
	'vwLossMasterFact' AS table_name,
	clndr_date,
	-- *INF*: IIF(NOT ISNULL(clndr_date),clndr_date,clndr_date1)
	IFF(NOT clndr_date IS NULL, clndr_date, clndr_date1) AS o_clndr_date,
	-- *INF*: IIF(NOT ISNULL(EnterpriseGroupDescription),EnterpriseGroupDescription,EnterpriseGroupDescription1)
	IFF(NOT EnterpriseGroupDescription IS NULL, EnterpriseGroupDescription, EnterpriseGroupDescription1) AS v_EnterpriseGroupDescription,
	-- *INF*: IIF(NOT ISNULL(StrategicProfitCenterDescription),StrategicProfitCenterDescription,StrategicProfitCenterDescription1)
	IFF(NOT StrategicProfitCenterDescription IS NULL, StrategicProfitCenterDescription, StrategicProfitCenterDescription1) AS v_StrategicProfitCenterDescription,
	-- *INF*: IIF(NOT ISNULL(InsuranceReferenceLegalEntityDescription),InsuranceReferenceLegalEntityDescription,InsuranceReferenceLegalEntityDescription1)
	IFF(NOT InsuranceReferenceLegalEntityDescription IS NULL, InsuranceReferenceLegalEntityDescription, InsuranceReferenceLegalEntityDescription1) AS v_InsuranceReferenceLegalEntityDescription,
	-- *INF*: IIF(NOT ISNULL(PolicyOfferingDescription),PolicyOfferingDescription,PolicyOfferingDescription1)
	IFF(NOT PolicyOfferingDescription IS NULL, PolicyOfferingDescription, PolicyOfferingDescription1) AS v_PolicyOfferingDescription,
	-- *INF*: IIF(NOT ISNULL(ProductDescription),ProductDescription,ProductDescription1)
	IFF(NOT ProductDescription IS NULL, ProductDescription, ProductDescription1) AS v_ProductDescription,
	-- *INF*: IIF(NOT ISNULL(InsuranceReferenceLineOfBusinessDescription),InsuranceReferenceLineOfBusinessDescription,InsuranceReferenceLineOfBusinessDescription1)
	IFF(NOT InsuranceReferenceLineOfBusinessDescription IS NULL, InsuranceReferenceLineOfBusinessDescription, InsuranceReferenceLineOfBusinessDescription1) AS v_InsuranceReferenceLineOfBusinessDescription,
	EnterpriseGroupDescription,
	v_EnterpriseGroupDescription AS o_EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	v_StrategicProfitCenterDescription AS o_StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	v_InsuranceReferenceLegalEntityDescription AS o_InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	v_PolicyOfferingDescription AS o_PolicyOfferingDescription,
	ProductDescription,
	v_ProductDescription AS o_ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	v_InsuranceReferenceLineOfBusinessDescription AS o_InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	-- *INF*: IIF(ISNULL(outstanding_amt),0,outstanding_amt)
	IFF(outstanding_amt IS NULL, 0, outstanding_amt) AS v_outstanding_amt,
	v_outstanding_amt AS o_outstanding_amt,
	paid_loss_amt,
	-- *INF*: IIF(ISNULL(paid_loss_amt),0,paid_loss_amt)
	IFF(paid_loss_amt IS NULL, 0, paid_loss_amt) AS v_paid_loss_amt,
	v_paid_loss_amt AS o_paid_loss_amt,
	paid_exp_amt,
	-- *INF*: IIF(ISNULL(paid_exp_amt),0,paid_exp_amt)
	IFF(paid_exp_amt IS NULL, 0, paid_exp_amt) AS v_paid_exp_amt,
	v_paid_exp_amt AS o_paid_exp_amt,
	ChangeInOutstandingAmount,
	-- *INF*: IIF(ISNULL(ChangeInOutstandingAmount),0,ChangeInOutstandingAmount)
	IFF(ChangeInOutstandingAmount IS NULL, 0, ChangeInOutstandingAmount) AS v_ChangeInOutstandingAmount,
	v_ChangeInOutstandingAmount AS o_ChangeInOutstandingAmount,
	DirectLossPaidER,
	-- *INF*: IIF(ISNULL(DirectLossPaidER),0,DirectLossPaidER)
	IFF(DirectLossPaidER IS NULL, 0, DirectLossPaidER) AS v_DirectLossPaidER,
	v_DirectLossPaidER AS o_DirectLossPaidER,
	DirectLossPaidIR,
	-- *INF*: IIF(ISNULL(DirectLossPaidIR),0,DirectLossPaidIR)
	IFF(DirectLossPaidIR IS NULL, 0, DirectLossPaidIR) AS v_DirectLossPaidIR,
	v_DirectLossPaidIR AS o_DirectLossPaidIR,
	DirectALAEPaidER,
	-- *INF*: IIF(ISNULL(DirectALAEPaidER),0,DirectALAEPaidER)
	IFF(DirectALAEPaidER IS NULL, 0, DirectALAEPaidER) AS v_DirectALAEPaidER,
	v_DirectALAEPaidER AS o_DirectALAEPaidER,
	DirectALAEPaidIR,
	-- *INF*: IIF(ISNULL(DirectALAEPaidIR),0,DirectALAEPaidIR)
	IFF(DirectALAEPaidIR IS NULL, 0, DirectALAEPaidIR) AS v_DirectALAEPaidIR,
	v_DirectALAEPaidIR AS o_DirectALAEPaidIR,
	DirectSalvagePaid,
	-- *INF*: IIF(ISNULL(DirectSalvagePaid),0,DirectSalvagePaid)
	IFF(DirectSalvagePaid IS NULL, 0, DirectSalvagePaid) AS v_DirectSalvagePaid,
	v_DirectSalvagePaid AS o_DirectSalvagePaid,
	DirectSubrogationPaid,
	-- *INF*: IIF(ISNULL(DirectSubrogationPaid),0,DirectSubrogationPaid)
	IFF(DirectSubrogationPaid IS NULL, 0, DirectSubrogationPaid) AS v_DirectSubrogationPaid,
	v_DirectSubrogationPaid AS o_DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryPaid),0,DirectOtherRecoveryPaid)
	IFF(DirectOtherRecoveryPaid IS NULL, 0, DirectOtherRecoveryPaid) AS v_DirectOtherRecoveryPaid,
	v_DirectOtherRecoveryPaid AS o_DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryLossPaid),0,DirectOtherRecoveryLossPaid)
	IFF(DirectOtherRecoveryLossPaid IS NULL, 0, DirectOtherRecoveryLossPaid) AS v_DirectOtherRecoveryLossPaid,
	v_DirectOtherRecoveryLossPaid AS o_DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryALAEPaid),0,DirectOtherRecoveryALAEPaid)
	IFF(DirectOtherRecoveryALAEPaid IS NULL, 0, DirectOtherRecoveryALAEPaid) AS v_DirectOtherRecoveryALAEPaid,
	v_DirectOtherRecoveryALAEPaid AS o_DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	-- *INF*: IIF(ISNULL(DirectLossOutstandingER),0,DirectLossOutstandingER)
	IFF(DirectLossOutstandingER IS NULL, 0, DirectLossOutstandingER) AS v_DirectLossOutstandingER,
	v_DirectLossOutstandingER AS o_DirectLossOutstandingER,
	DirectLossOutstandingIR,
	-- *INF*: IIF(ISNULL(DirectLossOutstandingIR),0,DirectLossOutstandingIR)
	IFF(DirectLossOutstandingIR IS NULL, 0, DirectLossOutstandingIR) AS v_DirectLossOutstandingIR,
	v_DirectLossOutstandingIR AS o_DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	-- *INF*: IIF(ISNULL(DirectALAEOutstandingER),0,DirectALAEOutstandingER)
	IFF(DirectALAEOutstandingER IS NULL, 0, DirectALAEOutstandingER) AS v_DirectALAEOutstandingER,
	v_DirectALAEOutstandingER AS o_DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	-- *INF*: IIF(ISNULL(DirectALAEOutstandingIR),0,DirectALAEOutstandingIR)
	IFF(DirectALAEOutstandingIR IS NULL, 0, DirectALAEOutstandingIR) AS v_DirectALAEOutstandingIR,
	v_DirectALAEOutstandingIR AS o_DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryOutstanding),0,DirectOtherRecoveryOutstanding)
	IFF(DirectOtherRecoveryOutstanding IS NULL, 0, DirectOtherRecoveryOutstanding) AS v_DirectOtherRecoveryOutstanding,
	v_DirectOtherRecoveryOutstanding AS o_DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryLossOutstanding),0,DirectOtherRecoveryLossOutstanding)
	IFF(DirectOtherRecoveryLossOutstanding IS NULL, 0, DirectOtherRecoveryLossOutstanding) AS v_DirectOtherRecoveryLossOutstanding,
	v_DirectOtherRecoveryLossOutstanding AS o_DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryALAEOutstanding),0,DirectOtherRecoveryALAEOutstanding)
	IFF(DirectOtherRecoveryALAEOutstanding IS NULL, 0, DirectOtherRecoveryALAEOutstanding) AS v_DirectOtherRecoveryALAEOutstanding,
	v_DirectOtherRecoveryALAEOutstanding AS o_DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	-- *INF*: IIF(ISNULL(DirectSubroOutstanding),0,DirectSubroOutstanding)
	IFF(DirectSubroOutstanding IS NULL, 0, DirectSubroOutstanding) AS v_DirectSubroOutstanding,
	v_DirectSubroOutstanding AS o_DirectSubroOutstanding,
	DirectSalvageOutstanding,
	-- *INF*: IIF(ISNULL(DirectSalvageOutstanding),0,DirectSalvageOutstanding)
	IFF(DirectSalvageOutstanding IS NULL, 0, DirectSalvageOutstanding) AS v_DirectSalvageOutstanding,
	v_DirectSalvageOutstanding AS o_DirectSalvageOutstanding,
	DirectLossIncurredER,
	-- *INF*: IIF(ISNULL(DirectLossIncurredER),0,DirectLossIncurredER)
	IFF(DirectLossIncurredER IS NULL, 0, DirectLossIncurredER) AS v_DirectLossIncurredER,
	v_DirectLossIncurredER AS o_DirectLossIncurredER,
	DirectLossIncurredIR,
	-- *INF*: IIF(ISNULL(DirectLossIncurredIR),0,DirectLossIncurredIR)
	IFF(DirectLossIncurredIR IS NULL, 0, DirectLossIncurredIR) AS v_DirectLossIncurredIR,
	v_DirectLossIncurredIR AS o_DirectLossIncurredIR,
	DirectALAEIncurredER,
	-- *INF*: IIF(ISNULL(DirectALAEIncurredER),0,DirectALAEIncurredER)
	IFF(DirectALAEIncurredER IS NULL, 0, DirectALAEIncurredER) AS v_DirectALAEIncurredER,
	v_DirectALAEIncurredER AS o_DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: IIF(ISNULL(DirectALAEIncurredIR),0,DirectALAEIncurredIR)
	IFF(DirectALAEIncurredIR IS NULL, 0, DirectALAEIncurredIR) AS v_DirectALAEIncurredIR,
	v_DirectALAEIncurredIR AS o_DirectALAEIncurredIR,
	'claim_loss_transaction_fact' AS table_name1,
	clndr_date1,
	-- *INF*: IIF(NOT ISNULL(clndr_date1),clndr_date1,clndr_date)
	IFF(NOT clndr_date1 IS NULL, clndr_date1, clndr_date) AS o_clndr_date1,
	EnterpriseGroupDescription1,
	v_EnterpriseGroupDescription AS o_EnterpriseGroupDescription1,
	StrategicProfitCenterDescription1,
	v_StrategicProfitCenterDescription AS o_StrategicProfitCenterDescription1,
	InsuranceReferenceLegalEntityDescription1,
	v_InsuranceReferenceLegalEntityDescription AS o_InsuranceReferenceLegalEntityDescription1,
	PolicyOfferingDescription1,
	v_PolicyOfferingDescription AS o_PolicyOfferingDescription1,
	ProductDescription1,
	v_ProductDescription AS o_ProductDescription1,
	InsuranceReferenceLineOfBusinessDescription1,
	v_InsuranceReferenceLineOfBusinessDescription AS o_InsuranceReferenceLineOfBusinessDescription1,
	outstanding_amt1,
	-- *INF*: IIF(ISNULL(outstanding_amt1),0,outstanding_amt1)
	IFF(outstanding_amt1 IS NULL, 0, outstanding_amt1) AS v_outstanding_amt1,
	v_outstanding_amt1 AS o_outstanding_amt1,
	paid_loss_amt1,
	-- *INF*: IIF(ISNULL(paid_loss_amt),0,paid_loss_amt1)
	IFF(paid_loss_amt IS NULL, 0, paid_loss_amt1) AS v_paid_loss_amt1,
	v_paid_loss_amt1 AS o_paid_loss_amt1,
	paid_exp_amt1,
	-- *INF*: IIF(ISNULL(paid_exp_amt1),0,paid_exp_amt1)
	IFF(paid_exp_amt1 IS NULL, 0, paid_exp_amt1) AS v_paid_exp_amt1,
	v_paid_exp_amt1 AS o_paid_exp_amt1,
	ChangeInOutstandingAmount1,
	-- *INF*: IIF(ISNULL(ChangeInOutstandingAmount1),0,ChangeInOutstandingAmount1)
	IFF(ChangeInOutstandingAmount1 IS NULL, 0, ChangeInOutstandingAmount1) AS v_ChangeInOutstandingAmount1,
	v_ChangeInOutstandingAmount1 AS o_ChangeInOutstandingAmount1,
	DirectLossPaidER1,
	-- *INF*: IIF(ISNULL(DirectLossPaidER1),0,DirectLossPaidER1)
	IFF(DirectLossPaidER1 IS NULL, 0, DirectLossPaidER1) AS v_DirectLossPaidER1,
	v_DirectLossPaidER1 AS o_DirectLossPaidER1,
	DirectLossPaidIR1,
	-- *INF*: IIF(ISNULL(DirectLossPaidIR1),0,DirectLossPaidIR1)
	IFF(DirectLossPaidIR1 IS NULL, 0, DirectLossPaidIR1) AS v_DirectLossPaidIR1,
	v_DirectLossPaidIR1 AS o_DirectLossPaidIR1,
	DirectALAEPaidER1,
	-- *INF*: IIF(ISNULL(DirectALAEPaidER1),0,DirectALAEPaidER1)
	IFF(DirectALAEPaidER1 IS NULL, 0, DirectALAEPaidER1) AS v_DirectALAEPaidER1,
	v_DirectALAEPaidER1 AS o_DirectALAEPaidER1,
	DirectALAEPaidIR1,
	-- *INF*: IIF(ISNULL(DirectALAEPaidIR1),0,DirectALAEPaidIR1)
	IFF(DirectALAEPaidIR1 IS NULL, 0, DirectALAEPaidIR1) AS v_DirectALAEPaidIR1,
	v_DirectALAEPaidIR1 AS o_DirectALAEPaidIR1,
	DirectSalvagePaid1,
	-- *INF*: IIF(ISNULL(DirectSalvagePaid1),0,DirectSalvagePaid1)
	IFF(DirectSalvagePaid1 IS NULL, 0, DirectSalvagePaid1) AS v_DirectSalvagePaid1,
	v_DirectSalvagePaid1 AS o_DirectSalvagePaid1,
	DirectSubrogationPaid1,
	-- *INF*: IIF(ISNULL(DirectSubrogationPaid1),0,DirectSubrogationPaid1)
	IFF(DirectSubrogationPaid1 IS NULL, 0, DirectSubrogationPaid1) AS v_DirectSubrogationPaid1,
	v_DirectSubrogationPaid1 AS o_DirectSubrogationPaid1,
	DirectOtherRecoveryPaid1,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryPaid1),0,DirectOtherRecoveryPaid1)
	IFF(DirectOtherRecoveryPaid1 IS NULL, 0, DirectOtherRecoveryPaid1) AS v_DirectOtherRecoveryPaid1,
	v_DirectOtherRecoveryPaid1 AS o_DirectOtherRecoveryPaid1,
	DirectOtherRecoveryLossPaid1,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryLossPaid1),0,DirectOtherRecoveryLossPaid1)
	IFF(DirectOtherRecoveryLossPaid1 IS NULL, 0, DirectOtherRecoveryLossPaid1) AS v_DirectOtherRecoveryLossPaid1,
	v_DirectOtherRecoveryLossPaid1 AS o_DirectOtherRecoveryLossPaid1,
	DirectOtherRecoveryALAEPaid1,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryALAEPaid1),0,DirectOtherRecoveryALAEPaid1)
	IFF(DirectOtherRecoveryALAEPaid1 IS NULL, 0, DirectOtherRecoveryALAEPaid1) AS v_DirectOtherRecoveryALAEPaid1,
	v_DirectOtherRecoveryALAEPaid1 AS o_DirectOtherRecoveryALAEPaid1,
	DirectLossOutstandingER1,
	-- *INF*: IIF(ISNULL(DirectLossOutstandingER1),0,DirectLossOutstandingER1)
	IFF(DirectLossOutstandingER1 IS NULL, 0, DirectLossOutstandingER1) AS v_DirectLossOutstandingER1,
	v_DirectLossOutstandingER1 AS o_DirectLossOutstandingER1,
	DirectLossOutstandingIR1,
	-- *INF*: IIF(ISNULL(DirectLossOutstandingIR1),0,DirectLossOutstandingIR1)
	IFF(DirectLossOutstandingIR1 IS NULL, 0, DirectLossOutstandingIR1) AS v_DirectLossOutstandingIR1,
	v_DirectLossOutstandingIR1 AS o_DirectLossOutstandingIR1,
	DirectALAEOutstandingER1,
	-- *INF*: IIF(ISNULL(DirectALAEOutstandingER1),0,DirectALAEOutstandingER1)
	IFF(DirectALAEOutstandingER1 IS NULL, 0, DirectALAEOutstandingER1) AS v_DirectALAEOutstandingER1,
	v_DirectALAEOutstandingER1 AS o_DirectALAEOutstandingER1,
	DirectALAEOutstandingIR1,
	-- *INF*: IIF(ISNULL(DirectALAEOutstandingIR1),0,DirectALAEOutstandingIR1)
	IFF(DirectALAEOutstandingIR1 IS NULL, 0, DirectALAEOutstandingIR1) AS v_DirectALAEOutstandingIR1,
	v_DirectALAEOutstandingIR1 AS o_DirectALAEOutstandingIR1,
	DirectOtherRecoveryOutstanding1,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryOutstanding1),0,DirectOtherRecoveryOutstanding1)
	IFF(DirectOtherRecoveryOutstanding1 IS NULL, 0, DirectOtherRecoveryOutstanding1) AS v_DirectOtherRecoveryOutstanding1,
	v_DirectOtherRecoveryOutstanding1 AS o_DirectOtherRecoveryOutstanding1,
	DirectOtherRecoveryLossOutstanding1,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryLossOutstanding1),0,DirectOtherRecoveryLossOutstanding1)
	IFF(DirectOtherRecoveryLossOutstanding1 IS NULL, 0, DirectOtherRecoveryLossOutstanding1) AS v_DirectOtherRecoveryLossOutstanding1,
	v_DirectOtherRecoveryLossOutstanding1 AS o_DirectOtherRecoveryLossOutstanding1,
	DirectOtherRecoveryALAEOutstanding1,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveryALAEOutstanding1),0,DirectOtherRecoveryALAEOutstanding1)
	IFF(DirectOtherRecoveryALAEOutstanding1 IS NULL, 0, DirectOtherRecoveryALAEOutstanding1) AS v_DirectOtherRecoveryALAEOutstanding1,
	v_DirectOtherRecoveryALAEOutstanding1 AS o_DirectOtherRecoveryALAEOutstanding1,
	DirectSubroOutstanding1,
	-- *INF*: IIF(ISNULL(DirectSubroOutstanding1),0,DirectSubroOutstanding1)
	IFF(DirectSubroOutstanding1 IS NULL, 0, DirectSubroOutstanding1) AS v_DirectSubroOutstanding1,
	v_DirectSubroOutstanding1 AS o_DirectSubroOutstanding1,
	DirectSalvageOutstanding1,
	-- *INF*: IIF(ISNULL(DirectSalvageOutstanding1),0,DirectSalvageOutstanding1)
	IFF(DirectSalvageOutstanding1 IS NULL, 0, DirectSalvageOutstanding1) AS v_DirectSalvageOutstanding1,
	v_DirectSalvageOutstanding1 AS o_DirectSalvageOutstanding1,
	DirectLossIncurredER1,
	-- *INF*: IIF(ISNULL(DirectLossIncurredER1),0,DirectLossIncurredER1)
	IFF(DirectLossIncurredER1 IS NULL, 0, DirectLossIncurredER1) AS v_DirectLossIncurredER1,
	v_DirectLossIncurredER1 AS o_DirectLossIncurredER1,
	DirectLossIncurredIR1,
	-- *INF*: IIF(ISNULL(DirectLossIncurredIR1),0,DirectLossIncurredIR1)
	IFF(DirectLossIncurredIR1 IS NULL, 0, DirectLossIncurredIR1) AS v_DirectLossIncurredIR1,
	v_DirectLossIncurredIR1 AS o_DirectLossIncurredIR1,
	DirectALAEIncurredER1,
	-- *INF*: IIF(ISNULL(DirectALAEIncurredER1),0,DirectALAEIncurredER1)
	IFF(DirectALAEIncurredER1 IS NULL, 0, DirectALAEIncurredER1) AS v_DirectALAEIncurredER1,
	v_DirectALAEIncurredER1 AS o_DirectALAEIncurredER1,
	DirectALAEIncurredIR1,
	-- *INF*: IIF(ISNULL(DirectALAEIncurredIR1),0,DirectALAEIncurredIR1)
	IFF(DirectALAEIncurredIR1 IS NULL, 0, DirectALAEIncurredIR1) AS v_DirectALAEIncurredIR1,
	v_DirectALAEIncurredIR1 AS o_DirectALAEIncurredIR1,
	'Difference' AS table_name11,
	-- *INF*: IIF(NOT ISNULL(clndr_date),clndr_date,clndr_date1)
	IFF(NOT clndr_date IS NULL, clndr_date, clndr_date1) AS clndr_date11,
	v_EnterpriseGroupDescription AS o_EnterpriseGroupDescription11,
	v_StrategicProfitCenterDescription AS o_StrategicProfitCenterDescription11,
	v_InsuranceReferenceLegalEntityDescription AS o_InsuranceReferenceLegalEntityDescription11,
	v_PolicyOfferingDescription AS o_PolicyOfferingDescription11,
	v_ProductDescription AS o_ProductDescription11,
	v_InsuranceReferenceLineOfBusinessDescription AS o_InsuranceReferenceLineOfBusinessDescription11,
	v_outstanding_amt-v_outstanding_amt1 AS outstanding_amt11,
	v_paid_loss_amt-v_paid_loss_amt1 AS paid_loss_amt11,
	v_paid_exp_amt-v_paid_exp_amt1 AS paid_exp_amt11,
	v_ChangeInOutstandingAmount-v_ChangeInOutstandingAmount1 AS ChangeInOutstandingAmount11,
	v_DirectLossPaidER-v_DirectLossPaidER1 AS DirectLossPaidER11,
	v_DirectLossPaidIR-v_DirectLossPaidIR1 AS DirectLossPaidIR11,
	v_DirectALAEPaidER-v_DirectALAEPaidER1 AS DirectALAEPaidER11,
	v_DirectALAEPaidIR-v_DirectALAEPaidIR1 AS DirectALAEPaidIR11,
	v_DirectSalvagePaid-v_DirectSalvagePaid1 AS DirectSalvagePaid11,
	v_DirectSubrogationPaid-v_DirectSubrogationPaid1 AS DirectSubrogationPaid11,
	v_DirectOtherRecoveryPaid-v_DirectOtherRecoveryPaid1 AS DirectOtherRecoveryPaid11,
	v_DirectOtherRecoveryLossPaid-v_DirectOtherRecoveryLossPaid1 AS DirectOtherRecoveryLossPaid11,
	v_DirectOtherRecoveryALAEPaid-v_DirectOtherRecoveryALAEPaid1 AS DirectOtherRecoveryALAEPaid11,
	v_DirectLossOutstandingER-v_DirectLossOutstandingER1 AS DirectLossOutstandingER11,
	v_DirectLossOutstandingIR-v_DirectLossOutstandingIR1 AS DirectLossOutstandingIR11,
	v_DirectALAEOutstandingER-v_DirectALAEOutstandingER1 AS DirectALAEOutstandingER11,
	v_DirectALAEOutstandingIR-v_DirectALAEOutstandingIR1 AS DirectALAEOutstandingIR11,
	v_DirectOtherRecoveryOutstanding-v_DirectOtherRecoveryOutstanding1 AS DirectOtherRecoveryOutstanding11,
	v_DirectOtherRecoveryLossOutstanding-v_DirectOtherRecoveryLossOutstanding1 AS DirectOtherRecoveryLossOutstanding11,
	v_DirectOtherRecoveryALAEOutstanding-v_DirectOtherRecoveryALAEOutstanding1 AS DirectOtherRecoveryALAEOutstanding11,
	v_DirectSubroOutstanding-v_DirectSubroOutstanding1 AS DirectSubroOutstanding11,
	v_DirectSalvageOutstanding-v_DirectSalvageOutstanding1 AS DirectSalvageOutstanding11,
	v_DirectLossIncurredER-v_DirectLossIncurredER1 AS DirectLossIncurredER11,
	v_DirectLossIncurredIR-v_DirectLossIncurredIR1 AS DirectLossIncurredIR11,
	v_DirectALAEIncurredER-v_DirectALAEIncurredER1 AS DirectALAEIncurredER11,
	v_DirectALAEIncurredIR-v_DirectALAEIncurredIR1 AS DirectALAEIncurredIR11
	FROM JNR_vwLossMasterFact_claim_loss_transaction_fact1
),
Union311 AS (
	SELECT table_name, o_clndr_date AS clndr_date, o_EnterpriseGroupDescription AS EnterpriseGroupDescription, o_StrategicProfitCenterDescription AS StrategicProfitCenterDescription, o_InsuranceReferenceLegalEntityDescription AS InsuranceReferenceLegalEntityDescription, o_PolicyOfferingDescription AS PolicyOfferingDescription, o_ProductDescription AS ProductDescription, o_InsuranceReferenceLineOfBusinessDescription AS InsuranceReferenceLineOfBusinessDescription, o_outstanding_amt AS outstanding_amt, o_paid_loss_amt AS paid_loss_amt, o_paid_exp_amt AS paid_exp_amt, o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount, o_DirectLossPaidER AS DirectLossPaidER, o_DirectLossPaidIR AS DirectLossPaidIR, o_DirectALAEPaidER AS DirectALAEPaidER, o_DirectALAEPaidIR AS DirectALAEPaidIR, o_DirectSalvagePaid AS DirectSalvagePaid, o_DirectSubrogationPaid AS DirectSubrogationPaid, o_DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid, o_DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid, o_DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid, o_DirectLossOutstandingER AS DirectLossOutstandingER, o_DirectLossOutstandingIR AS DirectLossOutstandingIR, o_DirectALAEOutstandingER AS DirectALAEOutstandingER, o_DirectALAEOutstandingIR AS DirectALAEOutstandingIR, o_DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding, o_DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding, o_DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding, o_DirectSubroOutstanding AS DirectSubroOutstanding, o_DirectSalvageOutstanding AS DirectSalvageOutstanding, o_DirectLossIncurredER AS DirectLossIncurredER, o_DirectLossIncurredIR AS DirectLossIncurredIR, o_DirectALAEIncurredER AS DirectALAEIncurredER, o_DirectALAEIncurredIR AS DirectALAEIncurredIR
	FROM 
	UNION
	SELECT table_name1 AS table_name, o_clndr_date1 AS clndr_date, o_EnterpriseGroupDescription1 AS EnterpriseGroupDescription, o_StrategicProfitCenterDescription1 AS StrategicProfitCenterDescription, o_InsuranceReferenceLegalEntityDescription1 AS InsuranceReferenceLegalEntityDescription, o_PolicyOfferingDescription1 AS PolicyOfferingDescription, o_ProductDescription1 AS ProductDescription, o_InsuranceReferenceLineOfBusinessDescription1 AS InsuranceReferenceLineOfBusinessDescription, o_outstanding_amt1 AS outstanding_amt, o_paid_loss_amt1 AS paid_loss_amt, o_paid_exp_amt1 AS paid_exp_amt, o_ChangeInOutstandingAmount1 AS ChangeInOutstandingAmount, o_DirectLossPaidER1 AS DirectLossPaidER, o_DirectLossPaidIR1 AS DirectLossPaidIR, o_DirectALAEPaidER1 AS DirectALAEPaidER, o_DirectALAEPaidIR1 AS DirectALAEPaidIR, o_DirectSalvagePaid1 AS DirectSalvagePaid, o_DirectSubrogationPaid1 AS DirectSubrogationPaid, o_DirectOtherRecoveryPaid1 AS DirectOtherRecoveryPaid, o_DirectOtherRecoveryLossPaid1 AS DirectOtherRecoveryLossPaid, o_DirectOtherRecoveryALAEPaid1 AS DirectOtherRecoveryALAEPaid, o_DirectLossOutstandingER1 AS DirectLossOutstandingER, o_DirectLossOutstandingIR1 AS DirectLossOutstandingIR, o_DirectALAEOutstandingER1 AS DirectALAEOutstandingER, o_DirectALAEOutstandingIR1 AS DirectALAEOutstandingIR, o_DirectOtherRecoveryOutstanding1 AS DirectOtherRecoveryOutstanding, o_DirectOtherRecoveryLossOutstanding1 AS DirectOtherRecoveryLossOutstanding, o_DirectOtherRecoveryALAEOutstanding1 AS DirectOtherRecoveryALAEOutstanding, o_DirectSubroOutstanding1 AS DirectSubroOutstanding, o_DirectSalvageOutstanding1 AS DirectSalvageOutstanding, o_DirectLossIncurredER1 AS DirectLossIncurredER, o_DirectLossIncurredIR1 AS DirectLossIncurredIR, o_DirectALAEIncurredER1 AS DirectALAEIncurredER, o_DirectALAEIncurredIR1 AS DirectALAEIncurredIR
	FROM 
	UNION
	SELECT table_name11 AS table_name, clndr_date11 AS clndr_date, o_EnterpriseGroupDescription11 AS EnterpriseGroupDescription, o_StrategicProfitCenterDescription11 AS StrategicProfitCenterDescription, o_InsuranceReferenceLegalEntityDescription11 AS InsuranceReferenceLegalEntityDescription, o_PolicyOfferingDescription11 AS PolicyOfferingDescription, o_ProductDescription11 AS ProductDescription, o_InsuranceReferenceLineOfBusinessDescription11 AS InsuranceReferenceLineOfBusinessDescription, outstanding_amt11 AS outstanding_amt, paid_loss_amt11 AS paid_loss_amt, paid_exp_amt11 AS paid_exp_amt, ChangeInOutstandingAmount11 AS ChangeInOutstandingAmount, DirectLossPaidER11 AS DirectLossPaidER, DirectLossPaidIR11 AS DirectLossPaidIR, DirectALAEPaidER11 AS DirectALAEPaidER, DirectALAEPaidIR11 AS DirectALAEPaidIR, DirectSalvagePaid11 AS DirectSalvagePaid, DirectSubrogationPaid11 AS DirectSubrogationPaid, DirectOtherRecoveryPaid11 AS DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid11 AS DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid11 AS DirectOtherRecoveryALAEPaid, DirectLossOutstandingER11 AS DirectLossOutstandingER, DirectLossOutstandingIR11 AS DirectLossOutstandingIR, DirectALAEOutstandingER11 AS DirectALAEOutstandingER, DirectALAEOutstandingIR11 AS DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding11 AS DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding11 AS DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding11 AS DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding11 AS DirectSubroOutstanding, DirectSalvageOutstanding11 AS DirectSalvageOutstanding, DirectLossIncurredER11 AS DirectLossIncurredER, DirectLossIncurredIR11 AS DirectLossIncurredIR, DirectALAEIncurredER11 AS DirectALAEIncurredER, DirectALAEIncurredIR11 AS DirectALAEIncurredIR
	FROM 
),
AGG_ByEnterpriseGroup11 AS (
	SELECT
	table_name,
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	-- *INF*: sum(outstanding_amt)
	sum(outstanding_amt) AS o_outstanding_amt,
	paid_loss_amt,
	-- *INF*: sum(paid_loss_amt)
	sum(paid_loss_amt) AS o_paid_loss_amt,
	paid_exp_amt,
	-- *INF*: sum(paid_exp_amt)
	sum(paid_exp_amt) AS o_paid_exp_amt,
	ChangeInOutstandingAmount,
	-- *INF*: sum(ChangeInOutstandingAmount)
	sum(ChangeInOutstandingAmount) AS o_ChangeInOutstandingAmount,
	DirectLossPaidER,
	-- *INF*: sum(DirectLossPaidER)
	sum(DirectLossPaidER) AS o_DirectLossPaidER,
	DirectLossPaidIR,
	-- *INF*: sum(DirectLossPaidIR)
	sum(DirectLossPaidIR) AS o_DirectLossPaidIR,
	DirectALAEPaidER,
	-- *INF*: sum(DirectALAEPaidER)
	sum(DirectALAEPaidER) AS o_DirectALAEPaidER,
	DirectALAEPaidIR,
	-- *INF*: sum(DirectALAEPaidIR)
	sum(DirectALAEPaidIR) AS o_DirectALAEPaidIR,
	DirectSalvagePaid,
	-- *INF*: sum(DirectSalvagePaid)
	sum(DirectSalvagePaid) AS o_DirectSalvagePaid,
	DirectSubrogationPaid,
	-- *INF*: sum(DirectSubrogationPaid)
	sum(DirectSubrogationPaid) AS o_DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	-- *INF*: sum(DirectOtherRecoveryPaid)
	sum(DirectOtherRecoveryPaid) AS o_DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	-- *INF*: sum(DirectOtherRecoveryLossPaid)
	sum(DirectOtherRecoveryLossPaid) AS o_DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	-- *INF*: sum(DirectOtherRecoveryALAEPaid)
	sum(DirectOtherRecoveryALAEPaid) AS o_DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	-- *INF*: sum(DirectLossOutstandingER)
	sum(DirectLossOutstandingER) AS o_DirectLossOutstandingER,
	DirectLossOutstandingIR,
	-- *INF*: sum(DirectLossOutstandingIR)
	sum(DirectLossOutstandingIR) AS o_DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	-- *INF*: sum(DirectALAEOutstandingER)
	sum(DirectALAEOutstandingER) AS o_DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	-- *INF*: sum(DirectALAEOutstandingIR)
	sum(DirectALAEOutstandingIR) AS o_DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	-- *INF*: sum(DirectOtherRecoveryOutstanding)
	sum(DirectOtherRecoveryOutstanding) AS o_DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	-- *INF*: sum(DirectOtherRecoveryLossOutstanding)
	sum(DirectOtherRecoveryLossOutstanding) AS o_DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	-- *INF*: sum(DirectOtherRecoveryALAEOutstanding)
	sum(DirectOtherRecoveryALAEOutstanding) AS o_DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	-- *INF*: sum(DirectSubroOutstanding)
	sum(DirectSubroOutstanding) AS o_DirectSubroOutstanding,
	DirectSalvageOutstanding,
	-- *INF*: sum(DirectSalvageOutstanding)
	sum(DirectSalvageOutstanding) AS o_DirectSalvageOutstanding,
	DirectLossIncurredER,
	-- *INF*: sum(DirectLossIncurredER)
	sum(DirectLossIncurredER) AS o_DirectLossIncurredER,
	DirectLossIncurredIR,
	-- *INF*: sum(DirectLossIncurredIR)
	sum(DirectLossIncurredIR) AS o_DirectLossIncurredIR,
	DirectALAEIncurredER,
	-- *INF*: sum(DirectALAEIncurredER)
	sum(DirectALAEIncurredER) AS o_DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: sum(DirectALAEIncurredIR)
	sum(DirectALAEIncurredIR) AS o_DirectALAEIncurredIR
	FROM Union311
	GROUP BY table_name, clndr_date, EnterpriseGroupDescription
),
EXP_ByEnterpriseGroup11 AS (
	SELECT
	'Balance vwLossMasterFact to claim_loss_transaction_fact' AS BalancingDescription,
	table_name,
	clndr_date,
	EnterpriseGroupDescription,
	o_outstanding_amt AS outstanding_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(outstanding_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(outstanding_amt)) AS o_outstanding_amt,
	o_paid_loss_amt AS paid_loss_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_loss_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_loss_amt)) AS o_paid_loss_amt,
	o_paid_exp_amt AS paid_exp_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_exp_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_exp_amt)) AS o_paid_exp_amt,
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(ChangeInOutstandingAmount))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(ChangeInOutstandingAmount)) AS o_ChangeInOutstandingAmount,
	o_DirectLossPaidER AS DirectLossPaidER,
	o_DirectLossPaidIR AS DirectLossPaidIR,
	o_DirectALAEPaidER AS DirectALAEPaidER,
	o_DirectALAEPaidIR AS DirectALAEPaidIR,
	o_DirectSalvagePaid AS DirectSalvagePaid,
	o_DirectSubrogationPaid AS DirectSubrogationPaid,
	o_DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid,
	o_DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid,
	o_DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid,
	o_DirectLossOutstandingER AS DirectLossOutstandingER,
	o_DirectLossOutstandingIR AS DirectLossOutstandingIR,
	o_DirectALAEOutstandingER AS DirectALAEOutstandingER,
	o_DirectALAEOutstandingIR AS DirectALAEOutstandingIR,
	o_DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding,
	o_DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding,
	o_DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding,
	o_DirectSubroOutstanding AS DirectSubroOutstanding,
	o_DirectSalvageOutstanding AS DirectSalvageOutstanding,
	o_DirectLossIncurredER AS DirectLossIncurredER,
	o_DirectLossIncurredIR AS DirectLossIncurredIR,
	o_DirectALAEIncurredER AS DirectALAEIncurredER,
	o_DirectALAEIncurredIR AS DirectALAEIncurredIR,
	-- *INF*: DECODE(TRUE,
	-- table_name='vwLossMasterFact',1,
	-- table_name='claim_loss_transaction_fact',2,
	-- table_name='Difference',3
	-- )
	DECODE(TRUE,
		table_name = 'vwLossMasterFact', 1,
		table_name = 'claim_loss_transaction_fact', 2,
		table_name = 'Difference', 3) AS OrderInd
	FROM AGG_ByEnterpriseGroup11
),
SRT_ByEnterpriseGroup11 AS (
	SELECT
	BalancingDescription, 
	table_name, 
	clndr_date, 
	EnterpriseGroupDescription, 
	o_outstanding_amt AS outstanding_amt, 
	o_paid_loss_amt AS paid_loss_amt, 
	o_paid_exp_amt AS paid_exp_amt, 
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount, 
	DirectLossPaidER, 
	DirectLossPaidIR, 
	DirectALAEPaidER, 
	DirectALAEPaidIR, 
	DirectSalvagePaid, 
	DirectSubrogationPaid, 
	DirectOtherRecoveryPaid, 
	DirectOtherRecoveryLossPaid, 
	DirectOtherRecoveryALAEPaid, 
	DirectLossOutstandingER, 
	DirectLossOutstandingIR, 
	DirectALAEOutstandingER, 
	DirectALAEOutstandingIR, 
	DirectOtherRecoveryOutstanding, 
	DirectOtherRecoveryLossOutstanding, 
	DirectOtherRecoveryALAEOutstanding, 
	DirectSubroOutstanding, 
	DirectSalvageOutstanding, 
	DirectLossIncurredER, 
	DirectLossIncurredIR, 
	DirectALAEIncurredER, 
	DirectALAEIncurredIR, 
	OrderInd
	FROM EXP_ByEnterpriseGroup11
	ORDER BY clndr_date ASC, EnterpriseGroupDescription ASC, OrderInd ASC
),
Union_ByEnterpriseGroup AS (
	SELECT BalancingDescription, table_name, clndr_date, EnterpriseGroupDescription, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR
	FROM SRT_ByEnterpriseGroup
	UNION
	SELECT BalancingDescription, table_name, clndr_date, EnterpriseGroupDescription, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR
	FROM SRT_ByEnterpriseGroup1
	UNION
	SELECT BalancingDescription, table_name, clndr_date, EnterpriseGroupDescription, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR
	FROM SRT_ByEnterpriseGroup11
),
ClaimFacts_Balancing_EnterpriseGroup AS (
	INSERT INTO ClaimFacts_Balancing_EnterpriseGroup
	(BalancingDescription, TableName, clndr_date, EnterpriseGroupDescription, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR)
	SELECT 
	BALANCINGDESCRIPTION, 
	table_name AS TABLENAME, 
	CLNDR_DATE, 
	ENTERPRISEGROUPDESCRIPTION, 
	OUTSTANDING_AMT, 
	PAID_LOSS_AMT, 
	PAID_EXP_AMT, 
	CHANGEINOUTSTANDINGAMOUNT, 
	DIRECTLOSSPAIDER, 
	DIRECTLOSSPAIDIR, 
	DIRECTALAEPAIDER, 
	DIRECTALAEPAIDIR, 
	DIRECTSALVAGEPAID, 
	DIRECTSUBROGATIONPAID, 
	DIRECTOTHERRECOVERYPAID, 
	DIRECTOTHERRECOVERYLOSSPAID, 
	DIRECTOTHERRECOVERYALAEPAID, 
	DIRECTLOSSOUTSTANDINGER, 
	DIRECTLOSSOUTSTANDINGIR, 
	DIRECTALAEOUTSTANDINGER, 
	DIRECTALAEOUTSTANDINGIR, 
	DIRECTOTHERRECOVERYOUTSTANDING, 
	DIRECTOTHERRECOVERYLOSSOUTSTANDING, 
	DIRECTOTHERRECOVERYALAEOUTSTANDING, 
	DIRECTSUBROOUTSTANDING, 
	DIRECTSALVAGEOUTSTANDING, 
	DIRECTLOSSINCURREDER, 
	DIRECTLOSSINCURREDIR, 
	DIRECTALAEINCURREDER, 
	DIRECTALAEINCURREDIR
	FROM Union_ByEnterpriseGroup
),
AGG_ByStrategicProfitCenter AS (
	SELECT
	table_name,
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	-- *INF*: sum(outstanding_amt)
	sum(outstanding_amt) AS o_outstanding_amt,
	paid_loss_amt,
	-- *INF*: sum(paid_loss_amt)
	sum(paid_loss_amt) AS o_paid_loss_amt,
	paid_exp_amt,
	-- *INF*: sum(paid_exp_amt)
	sum(paid_exp_amt) AS o_paid_exp_amt,
	ChangeInOutstandingAmount,
	-- *INF*: sum(ChangeInOutstandingAmount)
	sum(ChangeInOutstandingAmount) AS o_ChangeInOutstandingAmount,
	DirectLossPaidER,
	-- *INF*: sum(DirectLossPaidER)
	sum(DirectLossPaidER) AS o_DirectLossPaidER,
	DirectLossPaidIR,
	-- *INF*: sum(DirectLossPaidIR)
	sum(DirectLossPaidIR) AS o_DirectLossPaidIR,
	DirectALAEPaidER,
	-- *INF*: sum(DirectALAEPaidER)
	sum(DirectALAEPaidER) AS o_DirectALAEPaidER,
	DirectALAEPaidIR,
	-- *INF*: sum(DirectALAEPaidIR)
	sum(DirectALAEPaidIR) AS o_DirectALAEPaidIR,
	DirectSalvagePaid,
	-- *INF*: sum(DirectSalvagePaid)
	sum(DirectSalvagePaid) AS o_DirectSalvagePaid,
	DirectSubrogationPaid,
	-- *INF*: sum(DirectSubrogationPaid)
	sum(DirectSubrogationPaid) AS o_DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	-- *INF*: sum(DirectOtherRecoveryPaid)
	sum(DirectOtherRecoveryPaid) AS o_DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	-- *INF*: sum(DirectOtherRecoveryLossPaid)
	sum(DirectOtherRecoveryLossPaid) AS o_DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	-- *INF*: sum(DirectOtherRecoveryALAEPaid)
	sum(DirectOtherRecoveryALAEPaid) AS o_DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	-- *INF*: sum(DirectLossOutstandingER)
	sum(DirectLossOutstandingER) AS o_DirectLossOutstandingER,
	DirectLossOutstandingIR,
	-- *INF*: sum(DirectLossOutstandingIR)
	sum(DirectLossOutstandingIR) AS o_DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	-- *INF*: sum(DirectALAEOutstandingER)
	sum(DirectALAEOutstandingER) AS o_DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	-- *INF*: sum(DirectALAEOutstandingIR)
	sum(DirectALAEOutstandingIR) AS o_DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	-- *INF*: sum(DirectOtherRecoveryOutstanding)
	sum(DirectOtherRecoveryOutstanding) AS o_DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	-- *INF*: sum(DirectOtherRecoveryLossOutstanding)
	sum(DirectOtherRecoveryLossOutstanding) AS o_DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	-- *INF*: sum(DirectOtherRecoveryALAEOutstanding)
	sum(DirectOtherRecoveryALAEOutstanding) AS o_DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	-- *INF*: sum(DirectSubroOutstanding)
	sum(DirectSubroOutstanding) AS o_DirectSubroOutstanding,
	DirectSalvageOutstanding,
	-- *INF*: sum(DirectSalvageOutstanding)
	sum(DirectSalvageOutstanding) AS o_DirectSalvageOutstanding,
	DirectLossIncurredER,
	-- *INF*: sum(DirectLossIncurredER)
	sum(DirectLossIncurredER) AS o_DirectLossIncurredER,
	DirectLossIncurredIR,
	-- *INF*: sum(DirectLossIncurredIR)
	sum(DirectLossIncurredIR) AS o_DirectLossIncurredIR,
	DirectALAEIncurredER,
	-- *INF*: sum(DirectALAEIncurredER)
	sum(DirectALAEIncurredER) AS o_DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: sum(DirectALAEIncurredIR)
	sum(DirectALAEIncurredIR) AS o_DirectALAEIncurredIR
	FROM Union3
	GROUP BY table_name, clndr_date, StrategicProfitCenterDescription
),
EXP_ByStrategicProfitCenter AS (
	SELECT
	'Balance loss_master_fact to vwLossMasterFact' AS BalancingDescription,
	table_name,
	clndr_date,
	StrategicProfitCenterDescription,
	o_outstanding_amt AS outstanding_amt,
	o_paid_loss_amt AS paid_loss_amt,
	o_paid_exp_amt AS paid_exp_amt,
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount,
	o_DirectLossPaidER AS DirectLossPaidER,
	o_DirectLossPaidIR AS DirectLossPaidIR,
	o_DirectALAEPaidER AS DirectALAEPaidER,
	o_DirectALAEPaidIR AS DirectALAEPaidIR,
	o_DirectSalvagePaid AS DirectSalvagePaid,
	o_DirectSubrogationPaid AS DirectSubrogationPaid,
	o_DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid,
	o_DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid,
	o_DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid,
	o_DirectLossOutstandingER AS DirectLossOutstandingER,
	o_DirectLossOutstandingIR AS DirectLossOutstandingIR,
	o_DirectALAEOutstandingER AS DirectALAEOutstandingER,
	o_DirectALAEOutstandingIR AS DirectALAEOutstandingIR,
	o_DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding,
	o_DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding,
	o_DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding,
	o_DirectSubroOutstanding AS DirectSubroOutstanding,
	o_DirectSalvageOutstanding AS DirectSalvageOutstanding,
	o_DirectLossIncurredER AS DirectLossIncurredER,
	o_DirectLossIncurredIR AS DirectLossIncurredIR,
	o_DirectALAEIncurredER AS DirectALAEIncurredER,
	o_DirectALAEIncurredIR AS DirectALAEIncurredIR,
	-- *INF*: DECODE(TRUE,
	-- table_name='loss_master_fact',1,
	-- table_name='vwLossMasterFact',2,
	-- table_name='Difference',3
	-- )
	DECODE(TRUE,
		table_name = 'loss_master_fact', 1,
		table_name = 'vwLossMasterFact', 2,
		table_name = 'Difference', 3) AS OrderInd
	FROM AGG_ByStrategicProfitCenter
),
SRT_ByStrategicProfitCenter AS (
	SELECT
	BalancingDescription, 
	table_name, 
	clndr_date, 
	StrategicProfitCenterDescription, 
	outstanding_amt, 
	paid_loss_amt, 
	paid_exp_amt, 
	ChangeInOutstandingAmount, 
	DirectLossPaidER, 
	DirectLossPaidIR, 
	DirectALAEPaidER, 
	DirectALAEPaidIR, 
	DirectSalvagePaid, 
	DirectSubrogationPaid, 
	DirectOtherRecoveryPaid, 
	DirectOtherRecoveryLossPaid, 
	DirectOtherRecoveryALAEPaid, 
	DirectLossOutstandingER, 
	DirectLossOutstandingIR, 
	DirectALAEOutstandingER, 
	DirectALAEOutstandingIR, 
	DirectOtherRecoveryOutstanding, 
	DirectOtherRecoveryLossOutstanding, 
	DirectOtherRecoveryALAEOutstanding, 
	DirectSubroOutstanding, 
	DirectSalvageOutstanding, 
	DirectLossIncurredER, 
	DirectLossIncurredIR, 
	DirectALAEIncurredER, 
	DirectALAEIncurredIR, 
	OrderInd
	FROM EXP_ByStrategicProfitCenter
	ORDER BY clndr_date ASC, StrategicProfitCenterDescription ASC, OrderInd ASC
),
AGG_ByStrategicProfitCenter1 AS (
	SELECT
	table_name,
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	-- *INF*: sum(outstanding_amt)
	sum(outstanding_amt) AS o_outstanding_amt,
	paid_loss_amt,
	-- *INF*: sum(paid_loss_amt)
	sum(paid_loss_amt) AS o_paid_loss_amt,
	paid_exp_amt,
	-- *INF*: sum(paid_exp_amt)
	sum(paid_exp_amt) AS o_paid_exp_amt,
	ChangeInOutstandingAmount,
	-- *INF*: sum(ChangeInOutstandingAmount)
	sum(ChangeInOutstandingAmount) AS o_ChangeInOutstandingAmount,
	DirectLossPaidER,
	-- *INF*: sum(DirectLossPaidER)
	sum(DirectLossPaidER) AS o_DirectLossPaidER,
	DirectLossPaidIR,
	-- *INF*: sum(DirectLossPaidIR)
	sum(DirectLossPaidIR) AS o_DirectLossPaidIR,
	DirectALAEPaidER,
	-- *INF*: sum(DirectALAEPaidER)
	sum(DirectALAEPaidER) AS o_DirectALAEPaidER,
	DirectALAEPaidIR,
	-- *INF*: sum(DirectALAEPaidIR)
	sum(DirectALAEPaidIR) AS o_DirectALAEPaidIR,
	DirectSalvagePaid,
	-- *INF*: sum(DirectSalvagePaid)
	sum(DirectSalvagePaid) AS o_DirectSalvagePaid,
	DirectSubrogationPaid,
	-- *INF*: sum(DirectSubrogationPaid)
	sum(DirectSubrogationPaid) AS o_DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	-- *INF*: sum(DirectOtherRecoveryPaid)
	sum(DirectOtherRecoveryPaid) AS o_DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	-- *INF*: sum(DirectOtherRecoveryLossPaid)
	sum(DirectOtherRecoveryLossPaid) AS o_DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	-- *INF*: sum(DirectOtherRecoveryALAEPaid)
	sum(DirectOtherRecoveryALAEPaid) AS o_DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	-- *INF*: sum(DirectLossOutstandingER)
	sum(DirectLossOutstandingER) AS o_DirectLossOutstandingER,
	DirectLossOutstandingIR,
	-- *INF*: sum(DirectLossOutstandingIR)
	sum(DirectLossOutstandingIR) AS o_DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	-- *INF*: sum(DirectALAEOutstandingER)
	sum(DirectALAEOutstandingER) AS o_DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	-- *INF*: sum(DirectALAEOutstandingIR)
	sum(DirectALAEOutstandingIR) AS o_DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	-- *INF*: sum(DirectOtherRecoveryOutstanding)
	sum(DirectOtherRecoveryOutstanding) AS o_DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	-- *INF*: sum(DirectOtherRecoveryLossOutstanding)
	sum(DirectOtherRecoveryLossOutstanding) AS o_DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	-- *INF*: sum(DirectOtherRecoveryALAEOutstanding)
	sum(DirectOtherRecoveryALAEOutstanding) AS o_DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	-- *INF*: sum(DirectSubroOutstanding)
	sum(DirectSubroOutstanding) AS o_DirectSubroOutstanding,
	DirectSalvageOutstanding,
	-- *INF*: sum(DirectSalvageOutstanding)
	sum(DirectSalvageOutstanding) AS o_DirectSalvageOutstanding,
	DirectLossIncurredER,
	-- *INF*: sum(DirectLossIncurredER)
	sum(DirectLossIncurredER) AS o_DirectLossIncurredER,
	DirectLossIncurredIR,
	-- *INF*: sum(DirectLossIncurredIR)
	sum(DirectLossIncurredIR) AS o_DirectLossIncurredIR,
	DirectALAEIncurredER,
	-- *INF*: sum(DirectALAEIncurredER)
	sum(DirectALAEIncurredER) AS o_DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: sum(DirectALAEIncurredIR)
	sum(DirectALAEIncurredIR) AS o_DirectALAEIncurredIR
	FROM Union31
	GROUP BY table_name, clndr_date, StrategicProfitCenterDescription
),
EXP_ByStrategicProfitCenter1 AS (
	SELECT
	'Balance loss_master_fact to claim_loss_transaction_fact' AS BalancingDescription,
	table_name,
	clndr_date,
	StrategicProfitCenterDescription,
	o_outstanding_amt AS outstanding_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(outstanding_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(outstanding_amt)) AS o_outstanding_amt,
	o_paid_loss_amt AS paid_loss_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_loss_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_loss_amt)) AS o_paid_loss_amt,
	o_paid_exp_amt AS paid_exp_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_exp_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_exp_amt)) AS o_paid_exp_amt,
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(ChangeInOutstandingAmount))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(ChangeInOutstandingAmount)) AS o_ChangeInOutstandingAmount,
	o_DirectLossPaidER AS DirectLossPaidER,
	o_DirectLossPaidIR AS DirectLossPaidIR,
	o_DirectALAEPaidER AS DirectALAEPaidER,
	o_DirectALAEPaidIR AS DirectALAEPaidIR,
	o_DirectSalvagePaid AS DirectSalvagePaid,
	o_DirectSubrogationPaid AS DirectSubrogationPaid,
	o_DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid,
	o_DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid,
	o_DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid,
	o_DirectLossOutstandingER AS DirectLossOutstandingER,
	o_DirectLossOutstandingIR AS DirectLossOutstandingIR,
	o_DirectALAEOutstandingER AS DirectALAEOutstandingER,
	o_DirectALAEOutstandingIR AS DirectALAEOutstandingIR,
	o_DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding,
	o_DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding,
	o_DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding,
	o_DirectSubroOutstanding AS DirectSubroOutstanding,
	o_DirectSalvageOutstanding AS DirectSalvageOutstanding,
	o_DirectLossIncurredER AS DirectLossIncurredER,
	o_DirectLossIncurredIR AS DirectLossIncurredIR,
	o_DirectALAEIncurredER AS DirectALAEIncurredER,
	o_DirectALAEIncurredIR AS DirectALAEIncurredIR,
	-- *INF*: DECODE(TRUE,
	-- table_name='loss_master_fact',1,
	-- table_name='claim_loss_transaction_fact',2,
	-- table_name='Difference',3
	-- )
	DECODE(TRUE,
		table_name = 'loss_master_fact', 1,
		table_name = 'claim_loss_transaction_fact', 2,
		table_name = 'Difference', 3) AS OrderInd
	FROM AGG_ByStrategicProfitCenter1
),
SRT_ByStrategicProfitCenter1 AS (
	SELECT
	BalancingDescription, 
	table_name, 
	clndr_date, 
	StrategicProfitCenterDescription, 
	o_outstanding_amt AS outstanding_amt, 
	o_paid_loss_amt AS paid_loss_amt, 
	o_paid_exp_amt AS paid_exp_amt, 
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount, 
	DirectLossPaidER, 
	DirectLossPaidIR, 
	DirectALAEPaidER, 
	DirectALAEPaidIR, 
	DirectSalvagePaid, 
	DirectSubrogationPaid, 
	DirectOtherRecoveryPaid, 
	DirectOtherRecoveryLossPaid, 
	DirectOtherRecoveryALAEPaid, 
	DirectLossOutstandingER, 
	DirectLossOutstandingIR, 
	DirectALAEOutstandingER, 
	DirectALAEOutstandingIR, 
	DirectOtherRecoveryOutstanding, 
	DirectOtherRecoveryLossOutstanding, 
	DirectOtherRecoveryALAEOutstanding, 
	DirectSubroOutstanding, 
	DirectSalvageOutstanding, 
	DirectLossIncurredER, 
	DirectLossIncurredIR, 
	DirectALAEIncurredER, 
	DirectALAEIncurredIR, 
	OrderInd
	FROM EXP_ByStrategicProfitCenter1
	ORDER BY clndr_date ASC, StrategicProfitCenterDescription ASC, OrderInd ASC
),
AGG_ByStrategicProfitCenter11 AS (
	SELECT
	table_name,
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	-- *INF*: sum(outstanding_amt)
	sum(outstanding_amt) AS o_outstanding_amt,
	paid_loss_amt,
	-- *INF*: sum(paid_loss_amt)
	sum(paid_loss_amt) AS o_paid_loss_amt,
	paid_exp_amt,
	-- *INF*: sum(paid_exp_amt)
	sum(paid_exp_amt) AS o_paid_exp_amt,
	ChangeInOutstandingAmount,
	-- *INF*: sum(ChangeInOutstandingAmount)
	sum(ChangeInOutstandingAmount) AS o_ChangeInOutstandingAmount,
	DirectLossPaidER,
	-- *INF*: sum(DirectLossPaidER)
	sum(DirectLossPaidER) AS o_DirectLossPaidER,
	DirectLossPaidIR,
	-- *INF*: sum(DirectLossPaidIR)
	sum(DirectLossPaidIR) AS o_DirectLossPaidIR,
	DirectALAEPaidER,
	-- *INF*: sum(DirectALAEPaidER)
	sum(DirectALAEPaidER) AS o_DirectALAEPaidER,
	DirectALAEPaidIR,
	-- *INF*: sum(DirectALAEPaidIR)
	sum(DirectALAEPaidIR) AS o_DirectALAEPaidIR,
	DirectSalvagePaid,
	-- *INF*: sum(DirectSalvagePaid)
	sum(DirectSalvagePaid) AS o_DirectSalvagePaid,
	DirectSubrogationPaid,
	-- *INF*: sum(DirectSubrogationPaid)
	sum(DirectSubrogationPaid) AS o_DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	-- *INF*: sum(DirectOtherRecoveryPaid)
	sum(DirectOtherRecoveryPaid) AS o_DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	-- *INF*: sum(DirectOtherRecoveryLossPaid)
	sum(DirectOtherRecoveryLossPaid) AS o_DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	-- *INF*: sum(DirectOtherRecoveryALAEPaid)
	sum(DirectOtherRecoveryALAEPaid) AS o_DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	-- *INF*: sum(DirectLossOutstandingER)
	sum(DirectLossOutstandingER) AS o_DirectLossOutstandingER,
	DirectLossOutstandingIR,
	-- *INF*: sum(DirectLossOutstandingIR)
	sum(DirectLossOutstandingIR) AS o_DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	-- *INF*: sum(DirectALAEOutstandingER)
	sum(DirectALAEOutstandingER) AS o_DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	-- *INF*: sum(DirectALAEOutstandingIR)
	sum(DirectALAEOutstandingIR) AS o_DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	-- *INF*: sum(DirectOtherRecoveryOutstanding)
	sum(DirectOtherRecoveryOutstanding) AS o_DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	-- *INF*: sum(DirectOtherRecoveryLossOutstanding)
	sum(DirectOtherRecoveryLossOutstanding) AS o_DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	-- *INF*: sum(DirectOtherRecoveryALAEOutstanding)
	sum(DirectOtherRecoveryALAEOutstanding) AS o_DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	-- *INF*: sum(DirectSubroOutstanding)
	sum(DirectSubroOutstanding) AS o_DirectSubroOutstanding,
	DirectSalvageOutstanding,
	-- *INF*: sum(DirectSalvageOutstanding)
	sum(DirectSalvageOutstanding) AS o_DirectSalvageOutstanding,
	DirectLossIncurredER,
	-- *INF*: sum(DirectLossIncurredER)
	sum(DirectLossIncurredER) AS o_DirectLossIncurredER,
	DirectLossIncurredIR,
	-- *INF*: sum(DirectLossIncurredIR)
	sum(DirectLossIncurredIR) AS o_DirectLossIncurredIR,
	DirectALAEIncurredER,
	-- *INF*: sum(DirectALAEIncurredER)
	sum(DirectALAEIncurredER) AS o_DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: sum(DirectALAEIncurredIR)
	sum(DirectALAEIncurredIR) AS o_DirectALAEIncurredIR
	FROM Union311
	GROUP BY table_name, clndr_date, StrategicProfitCenterDescription
),
EXP_ByStrategicProfitCenter11 AS (
	SELECT
	'Balance vwLossMasterFact to claim_loss_transaction_fact' AS BalancingDescription,
	table_name,
	clndr_date,
	StrategicProfitCenterDescription,
	o_outstanding_amt AS outstanding_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(outstanding_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(outstanding_amt)) AS o_outstanding_amt,
	o_paid_loss_amt AS paid_loss_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_loss_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_loss_amt)) AS o_paid_loss_amt,
	o_paid_exp_amt AS paid_exp_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_exp_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_exp_amt)) AS o_paid_exp_amt,
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(ChangeInOutstandingAmount))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(ChangeInOutstandingAmount)) AS o_ChangeInOutstandingAmount,
	o_DirectLossPaidER AS DirectLossPaidER,
	o_DirectLossPaidIR AS DirectLossPaidIR,
	o_DirectALAEPaidER AS DirectALAEPaidER,
	o_DirectALAEPaidIR AS DirectALAEPaidIR,
	o_DirectSalvagePaid AS DirectSalvagePaid,
	o_DirectSubrogationPaid AS DirectSubrogationPaid,
	o_DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid,
	o_DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid,
	o_DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid,
	o_DirectLossOutstandingER AS DirectLossOutstandingER,
	o_DirectLossOutstandingIR AS DirectLossOutstandingIR,
	o_DirectALAEOutstandingER AS DirectALAEOutstandingER,
	o_DirectALAEOutstandingIR AS DirectALAEOutstandingIR,
	o_DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding,
	o_DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding,
	o_DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding,
	o_DirectSubroOutstanding AS DirectSubroOutstanding,
	o_DirectSalvageOutstanding AS DirectSalvageOutstanding,
	o_DirectLossIncurredER AS DirectLossIncurredER,
	o_DirectLossIncurredIR AS DirectLossIncurredIR,
	o_DirectALAEIncurredER AS DirectALAEIncurredER,
	o_DirectALAEIncurredIR AS DirectALAEIncurredIR,
	-- *INF*: DECODE(TRUE,
	-- table_name='vwLossMasterFact',1,
	-- table_name='claim_loss_transaction_fact',2,
	-- table_name='Difference',3
	-- )
	DECODE(TRUE,
		table_name = 'vwLossMasterFact', 1,
		table_name = 'claim_loss_transaction_fact', 2,
		table_name = 'Difference', 3) AS OrderInd
	FROM AGG_ByStrategicProfitCenter11
),
SRT_ByStrategicProfitCenter11 AS (
	SELECT
	BalancingDescription, 
	table_name, 
	clndr_date, 
	StrategicProfitCenterDescription, 
	o_outstanding_amt AS outstanding_amt, 
	o_paid_loss_amt AS paid_loss_amt, 
	o_paid_exp_amt AS paid_exp_amt, 
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount, 
	DirectLossPaidER, 
	DirectLossPaidIR, 
	DirectALAEPaidER, 
	DirectALAEPaidIR, 
	DirectSalvagePaid, 
	DirectSubrogationPaid, 
	DirectOtherRecoveryPaid, 
	DirectOtherRecoveryLossPaid, 
	DirectOtherRecoveryALAEPaid, 
	DirectLossOutstandingER, 
	DirectLossOutstandingIR, 
	DirectALAEOutstandingER, 
	DirectALAEOutstandingIR, 
	DirectOtherRecoveryOutstanding, 
	DirectOtherRecoveryLossOutstanding, 
	DirectOtherRecoveryALAEOutstanding, 
	DirectSubroOutstanding, 
	DirectSalvageOutstanding, 
	DirectLossIncurredER, 
	DirectLossIncurredIR, 
	DirectALAEIncurredER, 
	DirectALAEIncurredIR, 
	OrderInd
	FROM EXP_ByStrategicProfitCenter11
	ORDER BY clndr_date ASC, StrategicProfitCenterDescription ASC, OrderInd ASC
),
Union_ByStrategicProfitCenter AS (
	SELECT BalancingDescription, table_name, clndr_date, StrategicProfitCenterDescription, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR
	FROM SRT_ByStrategicProfitCenter
	UNION
	SELECT BalancingDescription, table_name, clndr_date, StrategicProfitCenterDescription, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR
	FROM SRT_ByStrategicProfitCenter1
	UNION
	SELECT BalancingDescription, table_name, clndr_date, StrategicProfitCenterDescription, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR
	FROM SRT_ByStrategicProfitCenter11
),
ClaimFacts_Balancing_StrategicProfitCenter AS (
	INSERT INTO ClaimFacts_Balancing_StrategicProfitCenter
	(BalancingDescription, TableName, clndr_date, StrategicProfitCenterDescription, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR)
	SELECT 
	BALANCINGDESCRIPTION, 
	table_name AS TABLENAME, 
	CLNDR_DATE, 
	STRATEGICPROFITCENTERDESCRIPTION, 
	OUTSTANDING_AMT, 
	PAID_LOSS_AMT, 
	PAID_EXP_AMT, 
	CHANGEINOUTSTANDINGAMOUNT, 
	DIRECTLOSSPAIDER, 
	DIRECTLOSSPAIDIR, 
	DIRECTALAEPAIDER, 
	DIRECTALAEPAIDIR, 
	DIRECTSALVAGEPAID, 
	DIRECTSUBROGATIONPAID, 
	DIRECTOTHERRECOVERYPAID, 
	DIRECTOTHERRECOVERYLOSSPAID, 
	DIRECTOTHERRECOVERYALAEPAID, 
	DIRECTLOSSOUTSTANDINGER, 
	DIRECTLOSSOUTSTANDINGIR, 
	DIRECTALAEOUTSTANDINGER, 
	DIRECTALAEOUTSTANDINGIR, 
	DIRECTOTHERRECOVERYOUTSTANDING, 
	DIRECTOTHERRECOVERYLOSSOUTSTANDING, 
	DIRECTOTHERRECOVERYALAEOUTSTANDING, 
	DIRECTSUBROOUTSTANDING, 
	DIRECTSALVAGEOUTSTANDING, 
	DIRECTLOSSINCURREDER, 
	DIRECTLOSSINCURREDIR, 
	DIRECTALAEINCURREDER, 
	DIRECTALAEINCURREDIR
	FROM Union_ByStrategicProfitCenter
),
AGG_ByInsuranceReferenceLegalEntity AS (
	SELECT
	table_name,
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	-- *INF*: sum(outstanding_amt)
	sum(outstanding_amt) AS o_outstanding_amt,
	paid_loss_amt,
	-- *INF*: sum(paid_loss_amt)
	sum(paid_loss_amt) AS o_paid_loss_amt,
	paid_exp_amt,
	-- *INF*: sum(paid_exp_amt)
	sum(paid_exp_amt) AS o_paid_exp_amt,
	ChangeInOutstandingAmount,
	-- *INF*: sum(ChangeInOutstandingAmount)
	sum(ChangeInOutstandingAmount) AS o_ChangeInOutstandingAmount,
	DirectLossPaidER,
	-- *INF*: sum(DirectLossPaidER)
	sum(DirectLossPaidER) AS o_DirectLossPaidER,
	DirectLossPaidIR,
	-- *INF*: sum(DirectLossPaidIR)
	sum(DirectLossPaidIR) AS o_DirectLossPaidIR,
	DirectALAEPaidER,
	-- *INF*: sum(DirectALAEPaidER)
	sum(DirectALAEPaidER) AS o_DirectALAEPaidER,
	DirectALAEPaidIR,
	-- *INF*: sum(DirectALAEPaidIR)
	sum(DirectALAEPaidIR) AS o_DirectALAEPaidIR,
	DirectSalvagePaid,
	-- *INF*: sum(DirectSalvagePaid)
	sum(DirectSalvagePaid) AS o_DirectSalvagePaid,
	DirectSubrogationPaid,
	-- *INF*: sum(DirectSubrogationPaid)
	sum(DirectSubrogationPaid) AS o_DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	-- *INF*: sum(DirectOtherRecoveryPaid)
	sum(DirectOtherRecoveryPaid) AS o_DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	-- *INF*: sum(DirectOtherRecoveryLossPaid)
	sum(DirectOtherRecoveryLossPaid) AS o_DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	-- *INF*: sum(DirectOtherRecoveryALAEPaid)
	sum(DirectOtherRecoveryALAEPaid) AS o_DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	-- *INF*: sum(DirectLossOutstandingER)
	sum(DirectLossOutstandingER) AS o_DirectLossOutstandingER,
	DirectLossOutstandingIR,
	-- *INF*: sum(DirectLossOutstandingIR)
	sum(DirectLossOutstandingIR) AS o_DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	-- *INF*: sum(DirectALAEOutstandingER)
	sum(DirectALAEOutstandingER) AS o_DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	-- *INF*: sum(DirectALAEOutstandingIR)
	sum(DirectALAEOutstandingIR) AS o_DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	-- *INF*: sum(DirectOtherRecoveryOutstanding)
	sum(DirectOtherRecoveryOutstanding) AS o_DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	-- *INF*: sum(DirectOtherRecoveryLossOutstanding)
	sum(DirectOtherRecoveryLossOutstanding) AS o_DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	-- *INF*: sum(DirectOtherRecoveryALAEOutstanding)
	sum(DirectOtherRecoveryALAEOutstanding) AS o_DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	-- *INF*: sum(DirectSubroOutstanding)
	sum(DirectSubroOutstanding) AS o_DirectSubroOutstanding,
	DirectSalvageOutstanding,
	-- *INF*: sum(DirectSalvageOutstanding)
	sum(DirectSalvageOutstanding) AS o_DirectSalvageOutstanding,
	DirectLossIncurredER,
	-- *INF*: sum(DirectLossIncurredER)
	sum(DirectLossIncurredER) AS o_DirectLossIncurredER,
	DirectLossIncurredIR,
	-- *INF*: sum(DirectLossIncurredIR)
	sum(DirectLossIncurredIR) AS o_DirectLossIncurredIR,
	DirectALAEIncurredER,
	-- *INF*: sum(DirectALAEIncurredER)
	sum(DirectALAEIncurredER) AS o_DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: sum(DirectALAEIncurredIR)
	sum(DirectALAEIncurredIR) AS o_DirectALAEIncurredIR
	FROM Union3
	GROUP BY table_name, clndr_date, InsuranceReferenceLegalEntityDescription
),
EXP_ByInsuranceReferenceLegalEntity AS (
	SELECT
	'Balance loss_master_fact to vwLossMasterFact' AS BalancingDescription,
	table_name,
	clndr_date,
	InsuranceReferenceLegalEntityDescription,
	o_outstanding_amt AS outstanding_amt,
	o_paid_loss_amt AS paid_loss_amt,
	o_paid_exp_amt AS paid_exp_amt,
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount,
	o_DirectLossPaidER AS DirectLossPaidER,
	o_DirectLossPaidIR AS DirectLossPaidIR,
	o_DirectALAEPaidER AS DirectALAEPaidER,
	o_DirectALAEPaidIR AS DirectALAEPaidIR,
	o_DirectSalvagePaid AS DirectSalvagePaid,
	o_DirectSubrogationPaid AS DirectSubrogationPaid,
	o_DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid,
	o_DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid,
	o_DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid,
	o_DirectLossOutstandingER AS DirectLossOutstandingER,
	o_DirectLossOutstandingIR AS DirectLossOutstandingIR,
	o_DirectALAEOutstandingER AS DirectALAEOutstandingER,
	o_DirectALAEOutstandingIR AS DirectALAEOutstandingIR,
	o_DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding,
	o_DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding,
	o_DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding,
	o_DirectSubroOutstanding AS DirectSubroOutstanding,
	o_DirectSalvageOutstanding AS DirectSalvageOutstanding,
	o_DirectLossIncurredER AS DirectLossIncurredER,
	o_DirectLossIncurredIR AS DirectLossIncurredIR,
	o_DirectALAEIncurredER AS DirectALAEIncurredER,
	o_DirectALAEIncurredIR AS DirectALAEIncurredIR,
	-- *INF*: DECODE(TRUE,
	-- table_name='loss_master_fact',1,
	-- table_name='vwLossMasterFact',2,
	-- table_name='Difference',3
	-- )
	DECODE(TRUE,
		table_name = 'loss_master_fact', 1,
		table_name = 'vwLossMasterFact', 2,
		table_name = 'Difference', 3) AS OrderInd
	FROM AGG_ByInsuranceReferenceLegalEntity
),
SRT_ByInsuranceReferenceLegalEntity AS (
	SELECT
	BalancingDescription, 
	table_name, 
	clndr_date, 
	InsuranceReferenceLegalEntityDescription, 
	outstanding_amt, 
	paid_loss_amt, 
	paid_exp_amt, 
	ChangeInOutstandingAmount, 
	DirectLossPaidER, 
	DirectLossPaidIR, 
	DirectALAEPaidER, 
	DirectALAEPaidIR, 
	DirectSalvagePaid, 
	DirectSubrogationPaid, 
	DirectOtherRecoveryPaid, 
	DirectOtherRecoveryLossPaid, 
	DirectOtherRecoveryALAEPaid, 
	DirectLossOutstandingER, 
	DirectLossOutstandingIR, 
	DirectALAEOutstandingER, 
	DirectALAEOutstandingIR, 
	DirectOtherRecoveryOutstanding, 
	DirectOtherRecoveryLossOutstanding, 
	DirectOtherRecoveryALAEOutstanding, 
	DirectSubroOutstanding, 
	DirectSalvageOutstanding, 
	DirectLossIncurredER, 
	DirectLossIncurredIR, 
	DirectALAEIncurredER, 
	DirectALAEIncurredIR, 
	OrderInd
	FROM EXP_ByInsuranceReferenceLegalEntity
	ORDER BY clndr_date ASC, InsuranceReferenceLegalEntityDescription ASC, OrderInd ASC
),
AGG_ByInsuranceReferenceLegalEntity1 AS (
	SELECT
	table_name,
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	-- *INF*: sum(outstanding_amt)
	sum(outstanding_amt) AS o_outstanding_amt,
	paid_loss_amt,
	-- *INF*: sum(paid_loss_amt)
	sum(paid_loss_amt) AS o_paid_loss_amt,
	paid_exp_amt,
	-- *INF*: sum(paid_exp_amt)
	sum(paid_exp_amt) AS o_paid_exp_amt,
	ChangeInOutstandingAmount,
	-- *INF*: sum(ChangeInOutstandingAmount)
	sum(ChangeInOutstandingAmount) AS o_ChangeInOutstandingAmount,
	DirectLossPaidER,
	-- *INF*: sum(DirectLossPaidER)
	sum(DirectLossPaidER) AS o_DirectLossPaidER,
	DirectLossPaidIR,
	-- *INF*: sum(DirectLossPaidIR)
	sum(DirectLossPaidIR) AS o_DirectLossPaidIR,
	DirectALAEPaidER,
	-- *INF*: sum(DirectALAEPaidER)
	sum(DirectALAEPaidER) AS o_DirectALAEPaidER,
	DirectALAEPaidIR,
	-- *INF*: sum(DirectALAEPaidIR)
	sum(DirectALAEPaidIR) AS o_DirectALAEPaidIR,
	DirectSalvagePaid,
	-- *INF*: sum(DirectSalvagePaid)
	sum(DirectSalvagePaid) AS o_DirectSalvagePaid,
	DirectSubrogationPaid,
	-- *INF*: sum(DirectSubrogationPaid)
	sum(DirectSubrogationPaid) AS o_DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	-- *INF*: sum(DirectOtherRecoveryPaid)
	sum(DirectOtherRecoveryPaid) AS o_DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	-- *INF*: sum(DirectOtherRecoveryLossPaid)
	sum(DirectOtherRecoveryLossPaid) AS o_DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	-- *INF*: sum(DirectOtherRecoveryALAEPaid)
	sum(DirectOtherRecoveryALAEPaid) AS o_DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	-- *INF*: sum(DirectLossOutstandingER)
	sum(DirectLossOutstandingER) AS o_DirectLossOutstandingER,
	DirectLossOutstandingIR,
	-- *INF*: sum(DirectLossOutstandingIR)
	sum(DirectLossOutstandingIR) AS o_DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	-- *INF*: sum(DirectALAEOutstandingER)
	sum(DirectALAEOutstandingER) AS o_DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	-- *INF*: sum(DirectALAEOutstandingIR)
	sum(DirectALAEOutstandingIR) AS o_DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	-- *INF*: sum(DirectOtherRecoveryOutstanding)
	sum(DirectOtherRecoveryOutstanding) AS o_DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	-- *INF*: sum(DirectOtherRecoveryLossOutstanding)
	sum(DirectOtherRecoveryLossOutstanding) AS o_DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	-- *INF*: sum(DirectOtherRecoveryALAEOutstanding)
	sum(DirectOtherRecoveryALAEOutstanding) AS o_DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	-- *INF*: sum(DirectSubroOutstanding)
	sum(DirectSubroOutstanding) AS o_DirectSubroOutstanding,
	DirectSalvageOutstanding,
	-- *INF*: sum(DirectSalvageOutstanding)
	sum(DirectSalvageOutstanding) AS o_DirectSalvageOutstanding,
	DirectLossIncurredER,
	-- *INF*: sum(DirectLossIncurredER)
	sum(DirectLossIncurredER) AS o_DirectLossIncurredER,
	DirectLossIncurredIR,
	-- *INF*: sum(DirectLossIncurredIR)
	sum(DirectLossIncurredIR) AS o_DirectLossIncurredIR,
	DirectALAEIncurredER,
	-- *INF*: sum(DirectALAEIncurredER)
	sum(DirectALAEIncurredER) AS o_DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: sum(DirectALAEIncurredIR)
	sum(DirectALAEIncurredIR) AS o_DirectALAEIncurredIR
	FROM Union31
	GROUP BY table_name, clndr_date, InsuranceReferenceLegalEntityDescription
),
EXP_ByInsuranceReferenceLegalEntity1 AS (
	SELECT
	'Balance loss_master_fact to claim_loss_transaction_fact' AS BalancingDescription,
	table_name,
	clndr_date,
	InsuranceReferenceLegalEntityDescription,
	o_outstanding_amt AS outstanding_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(outstanding_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(outstanding_amt)) AS o_outstanding_amt,
	o_paid_loss_amt AS paid_loss_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_loss_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_loss_amt)) AS o_paid_loss_amt,
	o_paid_exp_amt AS paid_exp_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_exp_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_exp_amt)) AS o_paid_exp_amt,
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(ChangeInOutstandingAmount))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(ChangeInOutstandingAmount)) AS o_ChangeInOutstandingAmount,
	o_DirectLossPaidER AS DirectLossPaidER,
	o_DirectLossPaidIR AS DirectLossPaidIR,
	o_DirectALAEPaidER AS DirectALAEPaidER,
	o_DirectALAEPaidIR AS DirectALAEPaidIR,
	o_DirectSalvagePaid AS DirectSalvagePaid,
	o_DirectSubrogationPaid AS DirectSubrogationPaid,
	o_DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid,
	o_DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid,
	o_DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid,
	o_DirectLossOutstandingER AS DirectLossOutstandingER,
	o_DirectLossOutstandingIR AS DirectLossOutstandingIR,
	o_DirectALAEOutstandingER AS DirectALAEOutstandingER,
	o_DirectALAEOutstandingIR AS DirectALAEOutstandingIR,
	o_DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding,
	o_DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding,
	o_DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding,
	o_DirectSubroOutstanding AS DirectSubroOutstanding,
	o_DirectSalvageOutstanding AS DirectSalvageOutstanding,
	o_DirectLossIncurredER AS DirectLossIncurredER,
	o_DirectLossIncurredIR AS DirectLossIncurredIR,
	o_DirectALAEIncurredER AS DirectALAEIncurredER,
	o_DirectALAEIncurredIR AS DirectALAEIncurredIR,
	-- *INF*: DECODE(TRUE,
	-- table_name='loss_master_fact',1,
	-- table_name='claim_loss_transaction_fact',2,
	-- table_name='Difference',3
	-- )
	DECODE(TRUE,
		table_name = 'loss_master_fact', 1,
		table_name = 'claim_loss_transaction_fact', 2,
		table_name = 'Difference', 3) AS OrderInd
	FROM AGG_ByInsuranceReferenceLegalEntity1
),
SRT_ByInsuranceReferenceLegalEntity1 AS (
	SELECT
	BalancingDescription, 
	table_name, 
	clndr_date, 
	InsuranceReferenceLegalEntityDescription, 
	o_outstanding_amt AS outstanding_amt, 
	o_paid_loss_amt AS paid_loss_amt, 
	o_paid_exp_amt AS paid_exp_amt, 
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount, 
	DirectLossPaidER, 
	DirectLossPaidIR, 
	DirectALAEPaidER, 
	DirectALAEPaidIR, 
	DirectSalvagePaid, 
	DirectSubrogationPaid, 
	DirectOtherRecoveryPaid, 
	DirectOtherRecoveryLossPaid, 
	DirectOtherRecoveryALAEPaid, 
	DirectLossOutstandingER, 
	DirectLossOutstandingIR, 
	DirectALAEOutstandingER, 
	DirectALAEOutstandingIR, 
	DirectOtherRecoveryOutstanding, 
	DirectOtherRecoveryLossOutstanding, 
	DirectOtherRecoveryALAEOutstanding, 
	DirectSubroOutstanding, 
	DirectSalvageOutstanding, 
	DirectLossIncurredER, 
	DirectLossIncurredIR, 
	DirectALAEIncurredER, 
	DirectALAEIncurredIR, 
	OrderInd
	FROM EXP_ByInsuranceReferenceLegalEntity1
	ORDER BY clndr_date ASC, InsuranceReferenceLegalEntityDescription ASC, OrderInd ASC
),
AGG_ByInsuranceReferenceLegalEntity11 AS (
	SELECT
	table_name,
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	-- *INF*: sum(outstanding_amt)
	sum(outstanding_amt) AS o_outstanding_amt,
	paid_loss_amt,
	-- *INF*: sum(paid_loss_amt)
	sum(paid_loss_amt) AS o_paid_loss_amt,
	paid_exp_amt,
	-- *INF*: sum(paid_exp_amt)
	sum(paid_exp_amt) AS o_paid_exp_amt,
	ChangeInOutstandingAmount,
	-- *INF*: sum(ChangeInOutstandingAmount)
	sum(ChangeInOutstandingAmount) AS o_ChangeInOutstandingAmount,
	DirectLossPaidER,
	-- *INF*: sum(DirectLossPaidER)
	sum(DirectLossPaidER) AS o_DirectLossPaidER,
	DirectLossPaidIR,
	-- *INF*: sum(DirectLossPaidIR)
	sum(DirectLossPaidIR) AS o_DirectLossPaidIR,
	DirectALAEPaidER,
	-- *INF*: sum(DirectALAEPaidER)
	sum(DirectALAEPaidER) AS o_DirectALAEPaidER,
	DirectALAEPaidIR,
	-- *INF*: sum(DirectALAEPaidIR)
	sum(DirectALAEPaidIR) AS o_DirectALAEPaidIR,
	DirectSalvagePaid,
	-- *INF*: sum(DirectSalvagePaid)
	sum(DirectSalvagePaid) AS o_DirectSalvagePaid,
	DirectSubrogationPaid,
	-- *INF*: sum(DirectSubrogationPaid)
	sum(DirectSubrogationPaid) AS o_DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	-- *INF*: sum(DirectOtherRecoveryPaid)
	sum(DirectOtherRecoveryPaid) AS o_DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	-- *INF*: sum(DirectOtherRecoveryLossPaid)
	sum(DirectOtherRecoveryLossPaid) AS o_DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	-- *INF*: sum(DirectOtherRecoveryALAEPaid)
	sum(DirectOtherRecoveryALAEPaid) AS o_DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	-- *INF*: sum(DirectLossOutstandingER)
	sum(DirectLossOutstandingER) AS o_DirectLossOutstandingER,
	DirectLossOutstandingIR,
	-- *INF*: sum(DirectLossOutstandingIR)
	sum(DirectLossOutstandingIR) AS o_DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	-- *INF*: sum(DirectALAEOutstandingER)
	sum(DirectALAEOutstandingER) AS o_DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	-- *INF*: sum(DirectALAEOutstandingIR)
	sum(DirectALAEOutstandingIR) AS o_DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	-- *INF*: sum(DirectOtherRecoveryOutstanding)
	sum(DirectOtherRecoveryOutstanding) AS o_DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	-- *INF*: sum(DirectOtherRecoveryLossOutstanding)
	sum(DirectOtherRecoveryLossOutstanding) AS o_DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	-- *INF*: sum(DirectOtherRecoveryALAEOutstanding)
	sum(DirectOtherRecoveryALAEOutstanding) AS o_DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	-- *INF*: sum(DirectSubroOutstanding)
	sum(DirectSubroOutstanding) AS o_DirectSubroOutstanding,
	DirectSalvageOutstanding,
	-- *INF*: sum(DirectSalvageOutstanding)
	sum(DirectSalvageOutstanding) AS o_DirectSalvageOutstanding,
	DirectLossIncurredER,
	-- *INF*: sum(DirectLossIncurredER)
	sum(DirectLossIncurredER) AS o_DirectLossIncurredER,
	DirectLossIncurredIR,
	-- *INF*: sum(DirectLossIncurredIR)
	sum(DirectLossIncurredIR) AS o_DirectLossIncurredIR,
	DirectALAEIncurredER,
	-- *INF*: sum(DirectALAEIncurredER)
	sum(DirectALAEIncurredER) AS o_DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: sum(DirectALAEIncurredIR)
	sum(DirectALAEIncurredIR) AS o_DirectALAEIncurredIR
	FROM Union311
	GROUP BY table_name, clndr_date, InsuranceReferenceLegalEntityDescription
),
EXP_ByInsuranceReferenceLegalEntity11 AS (
	SELECT
	'Balance vwLossMasterFact to claim_loss_transaction_fact' AS BalancingDescription,
	table_name,
	clndr_date,
	InsuranceReferenceLegalEntityDescription,
	o_outstanding_amt AS outstanding_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(outstanding_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(outstanding_amt)) AS o_outstanding_amt,
	o_paid_loss_amt AS paid_loss_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_loss_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_loss_amt)) AS o_paid_loss_amt,
	o_paid_exp_amt AS paid_exp_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_exp_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_exp_amt)) AS o_paid_exp_amt,
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(ChangeInOutstandingAmount))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(ChangeInOutstandingAmount)) AS o_ChangeInOutstandingAmount,
	o_DirectLossPaidER AS DirectLossPaidER,
	o_DirectLossPaidIR AS DirectLossPaidIR,
	o_DirectALAEPaidER AS DirectALAEPaidER,
	o_DirectALAEPaidIR AS DirectALAEPaidIR,
	o_DirectSalvagePaid AS DirectSalvagePaid,
	o_DirectSubrogationPaid AS DirectSubrogationPaid,
	o_DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid,
	o_DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid,
	o_DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid,
	o_DirectLossOutstandingER AS DirectLossOutstandingER,
	o_DirectLossOutstandingIR AS DirectLossOutstandingIR,
	o_DirectALAEOutstandingER AS DirectALAEOutstandingER,
	o_DirectALAEOutstandingIR AS DirectALAEOutstandingIR,
	o_DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding,
	o_DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding,
	o_DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding,
	o_DirectSubroOutstanding AS DirectSubroOutstanding,
	o_DirectSalvageOutstanding AS DirectSalvageOutstanding,
	o_DirectLossIncurredER AS DirectLossIncurredER,
	o_DirectLossIncurredIR AS DirectLossIncurredIR,
	o_DirectALAEIncurredER AS DirectALAEIncurredER,
	o_DirectALAEIncurredIR AS DirectALAEIncurredIR,
	-- *INF*: DECODE(TRUE,
	-- table_name='vwLossMasterFact',1,
	-- table_name='claim_loss_transaction_fact',2,
	-- table_name='Difference',3
	-- )
	DECODE(TRUE,
		table_name = 'vwLossMasterFact', 1,
		table_name = 'claim_loss_transaction_fact', 2,
		table_name = 'Difference', 3) AS OrderInd
	FROM AGG_ByInsuranceReferenceLegalEntity11
),
SRT_ByInsuranceReferenceLegalEntity11 AS (
	SELECT
	BalancingDescription, 
	table_name, 
	clndr_date, 
	InsuranceReferenceLegalEntityDescription, 
	o_outstanding_amt AS outstanding_amt, 
	o_paid_loss_amt AS paid_loss_amt, 
	o_paid_exp_amt AS paid_exp_amt, 
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount, 
	DirectLossPaidER, 
	DirectLossPaidIR, 
	DirectALAEPaidER, 
	DirectALAEPaidIR, 
	DirectSalvagePaid, 
	DirectSubrogationPaid, 
	DirectOtherRecoveryPaid, 
	DirectOtherRecoveryLossPaid, 
	DirectOtherRecoveryALAEPaid, 
	DirectLossOutstandingER, 
	DirectLossOutstandingIR, 
	DirectALAEOutstandingER, 
	DirectALAEOutstandingIR, 
	DirectOtherRecoveryOutstanding, 
	DirectOtherRecoveryLossOutstanding, 
	DirectOtherRecoveryALAEOutstanding, 
	DirectSubroOutstanding, 
	DirectSalvageOutstanding, 
	DirectLossIncurredER, 
	DirectLossIncurredIR, 
	DirectALAEIncurredER, 
	DirectALAEIncurredIR, 
	OrderInd
	FROM EXP_ByInsuranceReferenceLegalEntity11
	ORDER BY clndr_date ASC, InsuranceReferenceLegalEntityDescription ASC, OrderInd ASC
),
Union_ByInsuranceReferenceLegalEntity AS (
	SELECT BalancingDescription, table_name, clndr_date, InsuranceReferenceLegalEntityDescription, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR
	FROM SRT_ByInsuranceReferenceLegalEntity
	UNION
	SELECT BalancingDescription, table_name, clndr_date, InsuranceReferenceLegalEntityDescription, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR
	FROM SRT_ByInsuranceReferenceLegalEntity1
	UNION
	SELECT BalancingDescription, table_name, clndr_date, InsuranceReferenceLegalEntityDescription, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR
	FROM SRT_ByInsuranceReferenceLegalEntity11
),
ClaimFacts_Balancing_InsuranceReferenceLegalEntity AS (
	INSERT INTO ClaimFacts_Balancing_InsuranceReferenceLegalEntity
	(BalancingDescription, TableName, clndr_date, InsuranceReferenceLegalEntityDescription, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR)
	SELECT 
	BALANCINGDESCRIPTION, 
	table_name AS TABLENAME, 
	CLNDR_DATE, 
	INSURANCEREFERENCELEGALENTITYDESCRIPTION, 
	OUTSTANDING_AMT, 
	PAID_LOSS_AMT, 
	PAID_EXP_AMT, 
	CHANGEINOUTSTANDINGAMOUNT, 
	DIRECTLOSSPAIDER, 
	DIRECTLOSSPAIDIR, 
	DIRECTALAEPAIDER, 
	DIRECTALAEPAIDIR, 
	DIRECTSALVAGEPAID, 
	DIRECTSUBROGATIONPAID, 
	DIRECTOTHERRECOVERYPAID, 
	DIRECTOTHERRECOVERYLOSSPAID, 
	DIRECTOTHERRECOVERYALAEPAID, 
	DIRECTLOSSOUTSTANDINGER, 
	DIRECTLOSSOUTSTANDINGIR, 
	DIRECTALAEOUTSTANDINGER, 
	DIRECTALAEOUTSTANDINGIR, 
	DIRECTOTHERRECOVERYOUTSTANDING, 
	DIRECTOTHERRECOVERYLOSSOUTSTANDING, 
	DIRECTOTHERRECOVERYALAEOUTSTANDING, 
	DIRECTSUBROOUTSTANDING, 
	DIRECTSALVAGEOUTSTANDING, 
	DIRECTLOSSINCURREDER, 
	DIRECTLOSSINCURREDIR, 
	DIRECTALAEINCURREDER, 
	DIRECTALAEINCURREDIR
	FROM Union_ByInsuranceReferenceLegalEntity
),
AGG_ByPolicyOffering AS (
	SELECT
	table_name,
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	-- *INF*: sum(outstanding_amt)
	sum(outstanding_amt) AS o_outstanding_amt,
	paid_loss_amt,
	-- *INF*: sum(paid_loss_amt)
	sum(paid_loss_amt) AS o_paid_loss_amt,
	paid_exp_amt,
	-- *INF*: sum(paid_exp_amt)
	sum(paid_exp_amt) AS o_paid_exp_amt,
	ChangeInOutstandingAmount,
	-- *INF*: sum(ChangeInOutstandingAmount)
	sum(ChangeInOutstandingAmount) AS o_ChangeInOutstandingAmount,
	DirectLossPaidER,
	-- *INF*: sum(DirectLossPaidER)
	sum(DirectLossPaidER) AS o_DirectLossPaidER,
	DirectLossPaidIR,
	-- *INF*: sum(DirectLossPaidIR)
	sum(DirectLossPaidIR) AS o_DirectLossPaidIR,
	DirectALAEPaidER,
	-- *INF*: sum(DirectALAEPaidER)
	sum(DirectALAEPaidER) AS o_DirectALAEPaidER,
	DirectALAEPaidIR,
	-- *INF*: sum(DirectALAEPaidIR)
	sum(DirectALAEPaidIR) AS o_DirectALAEPaidIR,
	DirectSalvagePaid,
	-- *INF*: sum(DirectSalvagePaid)
	sum(DirectSalvagePaid) AS o_DirectSalvagePaid,
	DirectSubrogationPaid,
	-- *INF*: sum(DirectSubrogationPaid)
	sum(DirectSubrogationPaid) AS o_DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	-- *INF*: sum(DirectOtherRecoveryPaid)
	sum(DirectOtherRecoveryPaid) AS o_DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	-- *INF*: sum(DirectOtherRecoveryLossPaid)
	sum(DirectOtherRecoveryLossPaid) AS o_DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	-- *INF*: sum(DirectOtherRecoveryALAEPaid)
	sum(DirectOtherRecoveryALAEPaid) AS o_DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	-- *INF*: sum(DirectLossOutstandingER)
	sum(DirectLossOutstandingER) AS o_DirectLossOutstandingER,
	DirectLossOutstandingIR,
	-- *INF*: sum(DirectLossOutstandingIR)
	sum(DirectLossOutstandingIR) AS o_DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	-- *INF*: sum(DirectALAEOutstandingER)
	sum(DirectALAEOutstandingER) AS o_DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	-- *INF*: sum(DirectALAEOutstandingIR)
	sum(DirectALAEOutstandingIR) AS o_DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	-- *INF*: sum(DirectOtherRecoveryOutstanding)
	sum(DirectOtherRecoveryOutstanding) AS o_DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	-- *INF*: sum(DirectOtherRecoveryLossOutstanding)
	sum(DirectOtherRecoveryLossOutstanding) AS o_DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	-- *INF*: sum(DirectOtherRecoveryALAEOutstanding)
	sum(DirectOtherRecoveryALAEOutstanding) AS o_DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	-- *INF*: sum(DirectSubroOutstanding)
	sum(DirectSubroOutstanding) AS o_DirectSubroOutstanding,
	DirectSalvageOutstanding,
	-- *INF*: sum(DirectSalvageOutstanding)
	sum(DirectSalvageOutstanding) AS o_DirectSalvageOutstanding,
	DirectLossIncurredER,
	-- *INF*: sum(DirectLossIncurredER)
	sum(DirectLossIncurredER) AS o_DirectLossIncurredER,
	DirectLossIncurredIR,
	-- *INF*: sum(DirectLossIncurredIR)
	sum(DirectLossIncurredIR) AS o_DirectLossIncurredIR,
	DirectALAEIncurredER,
	-- *INF*: sum(DirectALAEIncurredER)
	sum(DirectALAEIncurredER) AS o_DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: sum(DirectALAEIncurredIR)
	sum(DirectALAEIncurredIR) AS o_DirectALAEIncurredIR
	FROM Union3
	GROUP BY table_name, clndr_date, PolicyOfferingDescription
),
EXP_ByPolicyOffering AS (
	SELECT
	'Balance loss_master_fact to vwLossMasterFact' AS BalancingDescription,
	table_name,
	clndr_date,
	PolicyOfferingDescription,
	o_outstanding_amt AS outstanding_amt,
	o_paid_loss_amt AS paid_loss_amt,
	o_paid_exp_amt AS paid_exp_amt,
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount,
	o_DirectLossPaidER AS DirectLossPaidER,
	o_DirectLossPaidIR AS DirectLossPaidIR,
	o_DirectALAEPaidER AS DirectALAEPaidER,
	o_DirectALAEPaidIR AS DirectALAEPaidIR,
	o_DirectSalvagePaid AS DirectSalvagePaid,
	o_DirectSubrogationPaid AS DirectSubrogationPaid,
	o_DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid,
	o_DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid,
	o_DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid,
	o_DirectLossOutstandingER AS DirectLossOutstandingER,
	o_DirectLossOutstandingIR AS DirectLossOutstandingIR,
	o_DirectALAEOutstandingER AS DirectALAEOutstandingER,
	o_DirectALAEOutstandingIR AS DirectALAEOutstandingIR,
	o_DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding,
	o_DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding,
	o_DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding,
	o_DirectSubroOutstanding AS DirectSubroOutstanding,
	o_DirectSalvageOutstanding AS DirectSalvageOutstanding,
	o_DirectLossIncurredER AS DirectLossIncurredER,
	o_DirectLossIncurredIR AS DirectLossIncurredIR,
	o_DirectALAEIncurredER AS DirectALAEIncurredER,
	o_DirectALAEIncurredIR AS DirectALAEIncurredIR,
	-- *INF*: DECODE(TRUE,
	-- table_name='loss_master_fact',1,
	-- table_name='vwLossMasterFact',2,
	-- table_name='Difference',3
	-- )
	DECODE(TRUE,
		table_name = 'loss_master_fact', 1,
		table_name = 'vwLossMasterFact', 2,
		table_name = 'Difference', 3) AS OrderInd
	FROM AGG_ByPolicyOffering
),
SRT_ByPolicyOffering AS (
	SELECT
	BalancingDescription, 
	table_name, 
	clndr_date, 
	PolicyOfferingDescription, 
	outstanding_amt, 
	paid_loss_amt, 
	paid_exp_amt, 
	ChangeInOutstandingAmount, 
	DirectLossPaidER, 
	DirectLossPaidIR, 
	DirectALAEPaidER, 
	DirectALAEPaidIR, 
	DirectSalvagePaid, 
	DirectSubrogationPaid, 
	DirectOtherRecoveryPaid, 
	DirectOtherRecoveryLossPaid, 
	DirectOtherRecoveryALAEPaid, 
	DirectLossOutstandingER, 
	DirectLossOutstandingIR, 
	DirectALAEOutstandingER, 
	DirectALAEOutstandingIR, 
	DirectOtherRecoveryOutstanding, 
	DirectOtherRecoveryLossOutstanding, 
	DirectOtherRecoveryALAEOutstanding, 
	DirectSubroOutstanding, 
	DirectSalvageOutstanding, 
	DirectLossIncurredER, 
	DirectLossIncurredIR, 
	DirectALAEIncurredER, 
	DirectALAEIncurredIR, 
	OrderInd
	FROM EXP_ByPolicyOffering
	ORDER BY clndr_date ASC, PolicyOfferingDescription ASC, OrderInd ASC
),
AGG_ByPolicyOffering1 AS (
	SELECT
	table_name,
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	-- *INF*: sum(outstanding_amt)
	sum(outstanding_amt) AS o_outstanding_amt,
	paid_loss_amt,
	-- *INF*: sum(paid_loss_amt)
	sum(paid_loss_amt) AS o_paid_loss_amt,
	paid_exp_amt,
	-- *INF*: sum(paid_exp_amt)
	sum(paid_exp_amt) AS o_paid_exp_amt,
	ChangeInOutstandingAmount,
	-- *INF*: sum(ChangeInOutstandingAmount)
	sum(ChangeInOutstandingAmount) AS o_ChangeInOutstandingAmount,
	DirectLossPaidER,
	-- *INF*: sum(DirectLossPaidER)
	sum(DirectLossPaidER) AS o_DirectLossPaidER,
	DirectLossPaidIR,
	-- *INF*: sum(DirectLossPaidIR)
	sum(DirectLossPaidIR) AS o_DirectLossPaidIR,
	DirectALAEPaidER,
	-- *INF*: sum(DirectALAEPaidER)
	sum(DirectALAEPaidER) AS o_DirectALAEPaidER,
	DirectALAEPaidIR,
	-- *INF*: sum(DirectALAEPaidIR)
	sum(DirectALAEPaidIR) AS o_DirectALAEPaidIR,
	DirectSalvagePaid,
	-- *INF*: sum(DirectSalvagePaid)
	sum(DirectSalvagePaid) AS o_DirectSalvagePaid,
	DirectSubrogationPaid,
	-- *INF*: sum(DirectSubrogationPaid)
	sum(DirectSubrogationPaid) AS o_DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	-- *INF*: sum(DirectOtherRecoveryPaid)
	sum(DirectOtherRecoveryPaid) AS o_DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	-- *INF*: sum(DirectOtherRecoveryLossPaid)
	sum(DirectOtherRecoveryLossPaid) AS o_DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	-- *INF*: sum(DirectOtherRecoveryALAEPaid)
	sum(DirectOtherRecoveryALAEPaid) AS o_DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	-- *INF*: sum(DirectLossOutstandingER)
	sum(DirectLossOutstandingER) AS o_DirectLossOutstandingER,
	DirectLossOutstandingIR,
	-- *INF*: sum(DirectLossOutstandingIR)
	sum(DirectLossOutstandingIR) AS o_DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	-- *INF*: sum(DirectALAEOutstandingER)
	sum(DirectALAEOutstandingER) AS o_DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	-- *INF*: sum(DirectALAEOutstandingIR)
	sum(DirectALAEOutstandingIR) AS o_DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	-- *INF*: sum(DirectOtherRecoveryOutstanding)
	sum(DirectOtherRecoveryOutstanding) AS o_DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	-- *INF*: sum(DirectOtherRecoveryLossOutstanding)
	sum(DirectOtherRecoveryLossOutstanding) AS o_DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	-- *INF*: sum(DirectOtherRecoveryALAEOutstanding)
	sum(DirectOtherRecoveryALAEOutstanding) AS o_DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	-- *INF*: sum(DirectSubroOutstanding)
	sum(DirectSubroOutstanding) AS o_DirectSubroOutstanding,
	DirectSalvageOutstanding,
	-- *INF*: sum(DirectSalvageOutstanding)
	sum(DirectSalvageOutstanding) AS o_DirectSalvageOutstanding,
	DirectLossIncurredER,
	-- *INF*: sum(DirectLossIncurredER)
	sum(DirectLossIncurredER) AS o_DirectLossIncurredER,
	DirectLossIncurredIR,
	-- *INF*: sum(DirectLossIncurredIR)
	sum(DirectLossIncurredIR) AS o_DirectLossIncurredIR,
	DirectALAEIncurredER,
	-- *INF*: sum(DirectALAEIncurredER)
	sum(DirectALAEIncurredER) AS o_DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: sum(DirectALAEIncurredIR)
	sum(DirectALAEIncurredIR) AS o_DirectALAEIncurredIR
	FROM Union31
	GROUP BY table_name, clndr_date, PolicyOfferingDescription
),
EXP_ByPolicyOffering1 AS (
	SELECT
	'Balance loss_master_fact to claim_loss_transaction_fact' AS BalancingDescription,
	table_name,
	clndr_date,
	PolicyOfferingDescription,
	o_outstanding_amt AS outstanding_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(outstanding_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(outstanding_amt)) AS o_outstanding_amt,
	o_paid_loss_amt AS paid_loss_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_loss_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_loss_amt)) AS o_paid_loss_amt,
	o_paid_exp_amt AS paid_exp_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_exp_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_exp_amt)) AS o_paid_exp_amt,
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(ChangeInOutstandingAmount))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(ChangeInOutstandingAmount)) AS o_ChangeInOutstandingAmount,
	o_DirectLossPaidER AS DirectLossPaidER,
	o_DirectLossPaidIR AS DirectLossPaidIR,
	o_DirectALAEPaidER AS DirectALAEPaidER,
	o_DirectALAEPaidIR AS DirectALAEPaidIR,
	o_DirectSalvagePaid AS DirectSalvagePaid,
	o_DirectSubrogationPaid AS DirectSubrogationPaid,
	o_DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid,
	o_DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid,
	o_DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid,
	o_DirectLossOutstandingER AS DirectLossOutstandingER,
	o_DirectLossOutstandingIR AS DirectLossOutstandingIR,
	o_DirectALAEOutstandingER AS DirectALAEOutstandingER,
	o_DirectALAEOutstandingIR AS DirectALAEOutstandingIR,
	o_DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding,
	o_DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding,
	o_DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding,
	o_DirectSubroOutstanding AS DirectSubroOutstanding,
	o_DirectSalvageOutstanding AS DirectSalvageOutstanding,
	o_DirectLossIncurredER AS DirectLossIncurredER,
	o_DirectLossIncurredIR AS DirectLossIncurredIR,
	o_DirectALAEIncurredER AS DirectALAEIncurredER,
	o_DirectALAEIncurredIR AS DirectALAEIncurredIR,
	-- *INF*: DECODE(TRUE,
	-- table_name='loss_master_fact',1,
	-- table_name='claim_loss_transaction_fact',2,
	-- table_name='Difference',3
	-- )
	DECODE(TRUE,
		table_name = 'loss_master_fact', 1,
		table_name = 'claim_loss_transaction_fact', 2,
		table_name = 'Difference', 3) AS OrderInd
	FROM AGG_ByPolicyOffering1
),
SRT_ByPolicyOffering1 AS (
	SELECT
	BalancingDescription, 
	table_name, 
	clndr_date, 
	PolicyOfferingDescription, 
	o_outstanding_amt AS outstanding_amt, 
	o_paid_loss_amt AS paid_loss_amt, 
	o_paid_exp_amt AS paid_exp_amt, 
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount, 
	DirectLossPaidER, 
	DirectLossPaidIR, 
	DirectALAEPaidER, 
	DirectALAEPaidIR, 
	DirectSalvagePaid, 
	DirectSubrogationPaid, 
	DirectOtherRecoveryPaid, 
	DirectOtherRecoveryLossPaid, 
	DirectOtherRecoveryALAEPaid, 
	DirectLossOutstandingER, 
	DirectLossOutstandingIR, 
	DirectALAEOutstandingER, 
	DirectALAEOutstandingIR, 
	DirectOtherRecoveryOutstanding, 
	DirectOtherRecoveryLossOutstanding, 
	DirectOtherRecoveryALAEOutstanding, 
	DirectSubroOutstanding, 
	DirectSalvageOutstanding, 
	DirectLossIncurredER, 
	DirectLossIncurredIR, 
	DirectALAEIncurredER, 
	DirectALAEIncurredIR, 
	OrderInd
	FROM EXP_ByPolicyOffering1
	ORDER BY clndr_date ASC, PolicyOfferingDescription ASC, OrderInd ASC
),
AGG_ByPolicyOffering11 AS (
	SELECT
	table_name,
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	-- *INF*: sum(outstanding_amt)
	sum(outstanding_amt) AS o_outstanding_amt,
	paid_loss_amt,
	-- *INF*: sum(paid_loss_amt)
	sum(paid_loss_amt) AS o_paid_loss_amt,
	paid_exp_amt,
	-- *INF*: sum(paid_exp_amt)
	sum(paid_exp_amt) AS o_paid_exp_amt,
	ChangeInOutstandingAmount,
	-- *INF*: sum(ChangeInOutstandingAmount)
	sum(ChangeInOutstandingAmount) AS o_ChangeInOutstandingAmount,
	DirectLossPaidER,
	-- *INF*: sum(DirectLossPaidER)
	sum(DirectLossPaidER) AS o_DirectLossPaidER,
	DirectLossPaidIR,
	-- *INF*: sum(DirectLossPaidIR)
	sum(DirectLossPaidIR) AS o_DirectLossPaidIR,
	DirectALAEPaidER,
	-- *INF*: sum(DirectALAEPaidER)
	sum(DirectALAEPaidER) AS o_DirectALAEPaidER,
	DirectALAEPaidIR,
	-- *INF*: sum(DirectALAEPaidIR)
	sum(DirectALAEPaidIR) AS o_DirectALAEPaidIR,
	DirectSalvagePaid,
	-- *INF*: sum(DirectSalvagePaid)
	sum(DirectSalvagePaid) AS o_DirectSalvagePaid,
	DirectSubrogationPaid,
	-- *INF*: sum(DirectSubrogationPaid)
	sum(DirectSubrogationPaid) AS o_DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	-- *INF*: sum(DirectOtherRecoveryPaid)
	sum(DirectOtherRecoveryPaid) AS o_DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	-- *INF*: sum(DirectOtherRecoveryLossPaid)
	sum(DirectOtherRecoveryLossPaid) AS o_DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	-- *INF*: sum(DirectOtherRecoveryALAEPaid)
	sum(DirectOtherRecoveryALAEPaid) AS o_DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	-- *INF*: sum(DirectLossOutstandingER)
	sum(DirectLossOutstandingER) AS o_DirectLossOutstandingER,
	DirectLossOutstandingIR,
	-- *INF*: sum(DirectLossOutstandingIR)
	sum(DirectLossOutstandingIR) AS o_DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	-- *INF*: sum(DirectALAEOutstandingER)
	sum(DirectALAEOutstandingER) AS o_DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	-- *INF*: sum(DirectALAEOutstandingIR)
	sum(DirectALAEOutstandingIR) AS o_DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	-- *INF*: sum(DirectOtherRecoveryOutstanding)
	sum(DirectOtherRecoveryOutstanding) AS o_DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	-- *INF*: sum(DirectOtherRecoveryLossOutstanding)
	sum(DirectOtherRecoveryLossOutstanding) AS o_DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	-- *INF*: sum(DirectOtherRecoveryALAEOutstanding)
	sum(DirectOtherRecoveryALAEOutstanding) AS o_DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	-- *INF*: sum(DirectSubroOutstanding)
	sum(DirectSubroOutstanding) AS o_DirectSubroOutstanding,
	DirectSalvageOutstanding,
	-- *INF*: sum(DirectSalvageOutstanding)
	sum(DirectSalvageOutstanding) AS o_DirectSalvageOutstanding,
	DirectLossIncurredER,
	-- *INF*: sum(DirectLossIncurredER)
	sum(DirectLossIncurredER) AS o_DirectLossIncurredER,
	DirectLossIncurredIR,
	-- *INF*: sum(DirectLossIncurredIR)
	sum(DirectLossIncurredIR) AS o_DirectLossIncurredIR,
	DirectALAEIncurredER,
	-- *INF*: sum(DirectALAEIncurredER)
	sum(DirectALAEIncurredER) AS o_DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: sum(DirectALAEIncurredIR)
	sum(DirectALAEIncurredIR) AS o_DirectALAEIncurredIR
	FROM Union311
	GROUP BY table_name, clndr_date, PolicyOfferingDescription
),
EXP_ByPolicyOffering11 AS (
	SELECT
	'Balance vwLossMasterFact to claim_loss_transaction_fact' AS BalancingDescription,
	table_name,
	clndr_date,
	PolicyOfferingDescription,
	o_outstanding_amt AS outstanding_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(outstanding_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(outstanding_amt)) AS o_outstanding_amt,
	o_paid_loss_amt AS paid_loss_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_loss_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_loss_amt)) AS o_paid_loss_amt,
	o_paid_exp_amt AS paid_exp_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_exp_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_exp_amt)) AS o_paid_exp_amt,
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(ChangeInOutstandingAmount))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(ChangeInOutstandingAmount)) AS o_ChangeInOutstandingAmount,
	o_DirectLossPaidER AS DirectLossPaidER,
	o_DirectLossPaidIR AS DirectLossPaidIR,
	o_DirectALAEPaidER AS DirectALAEPaidER,
	o_DirectALAEPaidIR AS DirectALAEPaidIR,
	o_DirectSalvagePaid AS DirectSalvagePaid,
	o_DirectSubrogationPaid AS DirectSubrogationPaid,
	o_DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid,
	o_DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid,
	o_DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid,
	o_DirectLossOutstandingER AS DirectLossOutstandingER,
	o_DirectLossOutstandingIR AS DirectLossOutstandingIR,
	o_DirectALAEOutstandingER AS DirectALAEOutstandingER,
	o_DirectALAEOutstandingIR AS DirectALAEOutstandingIR,
	o_DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding,
	o_DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding,
	o_DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding,
	o_DirectSubroOutstanding AS DirectSubroOutstanding,
	o_DirectSalvageOutstanding AS DirectSalvageOutstanding,
	o_DirectLossIncurredER AS DirectLossIncurredER,
	o_DirectLossIncurredIR AS DirectLossIncurredIR,
	o_DirectALAEIncurredER AS DirectALAEIncurredER,
	o_DirectALAEIncurredIR AS DirectALAEIncurredIR,
	-- *INF*: DECODE(TRUE,
	-- table_name='vwLossMasterFact',1,
	-- table_name='claim_loss_transaction_fact',2,
	-- table_name='Difference',3
	-- )
	DECODE(TRUE,
		table_name = 'vwLossMasterFact', 1,
		table_name = 'claim_loss_transaction_fact', 2,
		table_name = 'Difference', 3) AS OrderInd
	FROM AGG_ByPolicyOffering11
),
SRT_ByPolicyOffering11 AS (
	SELECT
	BalancingDescription, 
	table_name, 
	clndr_date, 
	PolicyOfferingDescription, 
	o_outstanding_amt AS outstanding_amt, 
	o_paid_loss_amt AS paid_loss_amt, 
	o_paid_exp_amt AS paid_exp_amt, 
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount, 
	DirectLossPaidER, 
	DirectLossPaidIR, 
	DirectALAEPaidER, 
	DirectALAEPaidIR, 
	DirectSalvagePaid, 
	DirectSubrogationPaid, 
	DirectOtherRecoveryPaid, 
	DirectOtherRecoveryLossPaid, 
	DirectOtherRecoveryALAEPaid, 
	DirectLossOutstandingER, 
	DirectLossOutstandingIR, 
	DirectALAEOutstandingER, 
	DirectALAEOutstandingIR, 
	DirectOtherRecoveryOutstanding, 
	DirectOtherRecoveryLossOutstanding, 
	DirectOtherRecoveryALAEOutstanding, 
	DirectSubroOutstanding, 
	DirectSalvageOutstanding, 
	DirectLossIncurredER, 
	DirectLossIncurredIR, 
	DirectALAEIncurredER, 
	DirectALAEIncurredIR, 
	OrderInd
	FROM EXP_ByPolicyOffering11
	ORDER BY clndr_date ASC, PolicyOfferingDescription ASC, OrderInd ASC
),
Union_ByPolicyOffering AS (
	SELECT BalancingDescription, table_name, clndr_date, PolicyOfferingDescription, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR
	FROM SRT_ByPolicyOffering
	UNION
	SELECT BalancingDescription, table_name, clndr_date, PolicyOfferingDescription, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR
	FROM SRT_ByPolicyOffering1
	UNION
	SELECT BalancingDescription, table_name, clndr_date, PolicyOfferingDescription, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR
	FROM SRT_ByPolicyOffering11
),
ClaimFacts_Balancing_PolicyOffering AS (
	INSERT INTO ClaimFacts_Balancing_PolicyOffering
	(BalancingDescription, TableName, clndr_date, PolicyOfferingDescription, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR)
	SELECT 
	BALANCINGDESCRIPTION, 
	table_name AS TABLENAME, 
	CLNDR_DATE, 
	POLICYOFFERINGDESCRIPTION, 
	OUTSTANDING_AMT, 
	PAID_LOSS_AMT, 
	PAID_EXP_AMT, 
	CHANGEINOUTSTANDINGAMOUNT, 
	DIRECTLOSSPAIDER, 
	DIRECTLOSSPAIDIR, 
	DIRECTALAEPAIDER, 
	DIRECTALAEPAIDIR, 
	DIRECTSALVAGEPAID, 
	DIRECTSUBROGATIONPAID, 
	DIRECTOTHERRECOVERYPAID, 
	DIRECTOTHERRECOVERYLOSSPAID, 
	DIRECTOTHERRECOVERYALAEPAID, 
	DIRECTLOSSOUTSTANDINGER, 
	DIRECTLOSSOUTSTANDINGIR, 
	DIRECTALAEOUTSTANDINGER, 
	DIRECTALAEOUTSTANDINGIR, 
	DIRECTOTHERRECOVERYOUTSTANDING, 
	DIRECTOTHERRECOVERYLOSSOUTSTANDING, 
	DIRECTOTHERRECOVERYALAEOUTSTANDING, 
	DIRECTSUBROOUTSTANDING, 
	DIRECTSALVAGEOUTSTANDING, 
	DIRECTLOSSINCURREDER, 
	DIRECTLOSSINCURREDIR, 
	DIRECTALAEINCURREDER, 
	DIRECTALAEINCURREDIR
	FROM Union_ByPolicyOffering
),
AGG_ByProduct AS (
	SELECT
	table_name,
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	-- *INF*: sum(outstanding_amt)
	sum(outstanding_amt) AS o_outstanding_amt,
	paid_loss_amt,
	-- *INF*: sum(paid_loss_amt)
	sum(paid_loss_amt) AS o_paid_loss_amt,
	paid_exp_amt,
	-- *INF*: sum(paid_exp_amt)
	sum(paid_exp_amt) AS o_paid_exp_amt,
	ChangeInOutstandingAmount,
	-- *INF*: sum(ChangeInOutstandingAmount)
	sum(ChangeInOutstandingAmount) AS o_ChangeInOutstandingAmount,
	DirectLossPaidER,
	-- *INF*: sum(DirectLossPaidER)
	sum(DirectLossPaidER) AS o_DirectLossPaidER,
	DirectLossPaidIR,
	-- *INF*: sum(DirectLossPaidIR)
	sum(DirectLossPaidIR) AS o_DirectLossPaidIR,
	DirectALAEPaidER,
	-- *INF*: sum(DirectALAEPaidER)
	sum(DirectALAEPaidER) AS o_DirectALAEPaidER,
	DirectALAEPaidIR,
	-- *INF*: sum(DirectALAEPaidIR)
	sum(DirectALAEPaidIR) AS o_DirectALAEPaidIR,
	DirectSalvagePaid,
	-- *INF*: sum(DirectSalvagePaid)
	sum(DirectSalvagePaid) AS o_DirectSalvagePaid,
	DirectSubrogationPaid,
	-- *INF*: sum(DirectSubrogationPaid)
	sum(DirectSubrogationPaid) AS o_DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	-- *INF*: sum(DirectOtherRecoveryPaid)
	sum(DirectOtherRecoveryPaid) AS o_DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	-- *INF*: sum(DirectOtherRecoveryLossPaid)
	sum(DirectOtherRecoveryLossPaid) AS o_DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	-- *INF*: sum(DirectOtherRecoveryALAEPaid)
	sum(DirectOtherRecoveryALAEPaid) AS o_DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	-- *INF*: sum(DirectLossOutstandingER)
	sum(DirectLossOutstandingER) AS o_DirectLossOutstandingER,
	DirectLossOutstandingIR,
	-- *INF*: sum(DirectLossOutstandingIR)
	sum(DirectLossOutstandingIR) AS o_DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	-- *INF*: sum(DirectALAEOutstandingER)
	sum(DirectALAEOutstandingER) AS o_DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	-- *INF*: sum(DirectALAEOutstandingIR)
	sum(DirectALAEOutstandingIR) AS o_DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	-- *INF*: sum(DirectOtherRecoveryOutstanding)
	sum(DirectOtherRecoveryOutstanding) AS o_DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	-- *INF*: sum(DirectOtherRecoveryLossOutstanding)
	sum(DirectOtherRecoveryLossOutstanding) AS o_DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	-- *INF*: sum(DirectOtherRecoveryALAEOutstanding)
	sum(DirectOtherRecoveryALAEOutstanding) AS o_DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	-- *INF*: sum(DirectSubroOutstanding)
	sum(DirectSubroOutstanding) AS o_DirectSubroOutstanding,
	DirectSalvageOutstanding,
	-- *INF*: sum(DirectSalvageOutstanding)
	sum(DirectSalvageOutstanding) AS o_DirectSalvageOutstanding,
	DirectLossIncurredER,
	-- *INF*: sum(DirectLossIncurredER)
	sum(DirectLossIncurredER) AS o_DirectLossIncurredER,
	DirectLossIncurredIR,
	-- *INF*: sum(DirectLossIncurredIR)
	sum(DirectLossIncurredIR) AS o_DirectLossIncurredIR,
	DirectALAEIncurredER,
	-- *INF*: sum(DirectALAEIncurredER)
	sum(DirectALAEIncurredER) AS o_DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: sum(DirectALAEIncurredIR)
	sum(DirectALAEIncurredIR) AS o_DirectALAEIncurredIR
	FROM Union3
	GROUP BY table_name, clndr_date, ProductDescription
),
EXP_ByProduct AS (
	SELECT
	'Balance loss_master_fact to vwLossMasterFact' AS BalancingDescription,
	table_name,
	clndr_date,
	ProductDescription,
	o_outstanding_amt AS outstanding_amt,
	o_paid_loss_amt AS paid_loss_amt,
	o_paid_exp_amt AS paid_exp_amt,
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount,
	o_DirectLossPaidER AS DirectLossPaidER,
	o_DirectLossPaidIR AS DirectLossPaidIR,
	o_DirectALAEPaidER AS DirectALAEPaidER,
	o_DirectALAEPaidIR AS DirectALAEPaidIR,
	o_DirectSalvagePaid AS DirectSalvagePaid,
	o_DirectSubrogationPaid AS DirectSubrogationPaid,
	o_DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid,
	o_DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid,
	o_DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid,
	o_DirectLossOutstandingER AS DirectLossOutstandingER,
	o_DirectLossOutstandingIR AS DirectLossOutstandingIR,
	o_DirectALAEOutstandingER AS DirectALAEOutstandingER,
	o_DirectALAEOutstandingIR AS DirectALAEOutstandingIR,
	o_DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding,
	o_DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding,
	o_DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding,
	o_DirectSubroOutstanding AS DirectSubroOutstanding,
	o_DirectSalvageOutstanding AS DirectSalvageOutstanding,
	o_DirectLossIncurredER AS DirectLossIncurredER,
	o_DirectLossIncurredIR AS DirectLossIncurredIR,
	o_DirectALAEIncurredER AS DirectALAEIncurredER,
	o_DirectALAEIncurredIR AS DirectALAEIncurredIR,
	-- *INF*: DECODE(TRUE,
	-- table_name='loss_master_fact',1,
	-- table_name='vwLossMasterFact',2,
	-- table_name='Difference',3
	-- )
	DECODE(TRUE,
		table_name = 'loss_master_fact', 1,
		table_name = 'vwLossMasterFact', 2,
		table_name = 'Difference', 3) AS OrderInd
	FROM AGG_ByProduct
),
SRT_ByProduct AS (
	SELECT
	BalancingDescription, 
	table_name, 
	clndr_date, 
	ProductDescription, 
	outstanding_amt, 
	paid_loss_amt, 
	paid_exp_amt, 
	ChangeInOutstandingAmount, 
	DirectLossPaidER, 
	DirectLossPaidIR, 
	DirectALAEPaidER, 
	DirectALAEPaidIR, 
	DirectSalvagePaid, 
	DirectSubrogationPaid, 
	DirectOtherRecoveryPaid, 
	DirectOtherRecoveryLossPaid, 
	DirectOtherRecoveryALAEPaid, 
	DirectLossOutstandingER, 
	DirectLossOutstandingIR, 
	DirectALAEOutstandingER, 
	DirectALAEOutstandingIR, 
	DirectOtherRecoveryOutstanding, 
	DirectOtherRecoveryLossOutstanding, 
	DirectOtherRecoveryALAEOutstanding, 
	DirectSubroOutstanding, 
	DirectSalvageOutstanding, 
	DirectLossIncurredER, 
	DirectLossIncurredIR, 
	DirectALAEIncurredER, 
	DirectALAEIncurredIR, 
	OrderInd
	FROM EXP_ByProduct
	ORDER BY clndr_date ASC, ProductDescription ASC, OrderInd ASC
),
AGG_ByProduct1 AS (
	SELECT
	table_name,
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	-- *INF*: sum(outstanding_amt)
	sum(outstanding_amt) AS o_outstanding_amt,
	paid_loss_amt,
	-- *INF*: sum(paid_loss_amt)
	sum(paid_loss_amt) AS o_paid_loss_amt,
	paid_exp_amt,
	-- *INF*: sum(paid_exp_amt)
	sum(paid_exp_amt) AS o_paid_exp_amt,
	ChangeInOutstandingAmount,
	-- *INF*: sum(ChangeInOutstandingAmount)
	sum(ChangeInOutstandingAmount) AS o_ChangeInOutstandingAmount,
	DirectLossPaidER,
	-- *INF*: sum(DirectLossPaidER)
	sum(DirectLossPaidER) AS o_DirectLossPaidER,
	DirectLossPaidIR,
	-- *INF*: sum(DirectLossPaidIR)
	sum(DirectLossPaidIR) AS o_DirectLossPaidIR,
	DirectALAEPaidER,
	-- *INF*: sum(DirectALAEPaidER)
	sum(DirectALAEPaidER) AS o_DirectALAEPaidER,
	DirectALAEPaidIR,
	-- *INF*: sum(DirectALAEPaidIR)
	sum(DirectALAEPaidIR) AS o_DirectALAEPaidIR,
	DirectSalvagePaid,
	-- *INF*: sum(DirectSalvagePaid)
	sum(DirectSalvagePaid) AS o_DirectSalvagePaid,
	DirectSubrogationPaid,
	-- *INF*: sum(DirectSubrogationPaid)
	sum(DirectSubrogationPaid) AS o_DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	-- *INF*: sum(DirectOtherRecoveryPaid)
	sum(DirectOtherRecoveryPaid) AS o_DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	-- *INF*: sum(DirectOtherRecoveryLossPaid)
	sum(DirectOtherRecoveryLossPaid) AS o_DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	-- *INF*: sum(DirectOtherRecoveryALAEPaid)
	sum(DirectOtherRecoveryALAEPaid) AS o_DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	-- *INF*: sum(DirectLossOutstandingER)
	sum(DirectLossOutstandingER) AS o_DirectLossOutstandingER,
	DirectLossOutstandingIR,
	-- *INF*: sum(DirectLossOutstandingIR)
	sum(DirectLossOutstandingIR) AS o_DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	-- *INF*: sum(DirectALAEOutstandingER)
	sum(DirectALAEOutstandingER) AS o_DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	-- *INF*: sum(DirectALAEOutstandingIR)
	sum(DirectALAEOutstandingIR) AS o_DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	-- *INF*: sum(DirectOtherRecoveryOutstanding)
	sum(DirectOtherRecoveryOutstanding) AS o_DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	-- *INF*: sum(DirectOtherRecoveryLossOutstanding)
	sum(DirectOtherRecoveryLossOutstanding) AS o_DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	-- *INF*: sum(DirectOtherRecoveryALAEOutstanding)
	sum(DirectOtherRecoveryALAEOutstanding) AS o_DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	-- *INF*: sum(DirectSubroOutstanding)
	sum(DirectSubroOutstanding) AS o_DirectSubroOutstanding,
	DirectSalvageOutstanding,
	-- *INF*: sum(DirectSalvageOutstanding)
	sum(DirectSalvageOutstanding) AS o_DirectSalvageOutstanding,
	DirectLossIncurredER,
	-- *INF*: sum(DirectLossIncurredER)
	sum(DirectLossIncurredER) AS o_DirectLossIncurredER,
	DirectLossIncurredIR,
	-- *INF*: sum(DirectLossIncurredIR)
	sum(DirectLossIncurredIR) AS o_DirectLossIncurredIR,
	DirectALAEIncurredER,
	-- *INF*: sum(DirectALAEIncurredER)
	sum(DirectALAEIncurredER) AS o_DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: sum(DirectALAEIncurredIR)
	sum(DirectALAEIncurredIR) AS o_DirectALAEIncurredIR
	FROM Union31
	GROUP BY table_name, clndr_date, ProductDescription
),
EXP_ByProduct1 AS (
	SELECT
	'Balance loss_master_fact to claim_loss_transaction_fact' AS BalancingDescription,
	table_name,
	clndr_date,
	ProductDescription,
	o_outstanding_amt AS outstanding_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(outstanding_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(outstanding_amt)) AS o_outstanding_amt,
	o_paid_loss_amt AS paid_loss_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_loss_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_loss_amt)) AS o_paid_loss_amt,
	o_paid_exp_amt AS paid_exp_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_exp_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_exp_amt)) AS o_paid_exp_amt,
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(ChangeInOutstandingAmount))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(ChangeInOutstandingAmount)) AS o_ChangeInOutstandingAmount,
	o_DirectLossPaidER AS DirectLossPaidER,
	o_DirectLossPaidIR AS DirectLossPaidIR,
	o_DirectALAEPaidER AS DirectALAEPaidER,
	o_DirectALAEPaidIR AS DirectALAEPaidIR,
	o_DirectSalvagePaid AS DirectSalvagePaid,
	o_DirectSubrogationPaid AS DirectSubrogationPaid,
	o_DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid,
	o_DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid,
	o_DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid,
	o_DirectLossOutstandingER AS DirectLossOutstandingER,
	o_DirectLossOutstandingIR AS DirectLossOutstandingIR,
	o_DirectALAEOutstandingER AS DirectALAEOutstandingER,
	o_DirectALAEOutstandingIR AS DirectALAEOutstandingIR,
	o_DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding,
	o_DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding,
	o_DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding,
	o_DirectSubroOutstanding AS DirectSubroOutstanding,
	o_DirectSalvageOutstanding AS DirectSalvageOutstanding,
	o_DirectLossIncurredER AS DirectLossIncurredER,
	o_DirectLossIncurredIR AS DirectLossIncurredIR,
	o_DirectALAEIncurredER AS DirectALAEIncurredER,
	o_DirectALAEIncurredIR AS DirectALAEIncurredIR,
	-- *INF*: DECODE(TRUE,
	-- table_name='loss_master_fact',1,
	-- table_name='claim_loss_transaction_fact',2,
	-- table_name='Difference',3
	-- )
	DECODE(TRUE,
		table_name = 'loss_master_fact', 1,
		table_name = 'claim_loss_transaction_fact', 2,
		table_name = 'Difference', 3) AS OrderInd
	FROM AGG_ByProduct1
),
SRT_ByProduct1 AS (
	SELECT
	BalancingDescription, 
	table_name, 
	clndr_date, 
	ProductDescription, 
	o_outstanding_amt AS outstanding_amt, 
	o_paid_loss_amt AS paid_loss_amt, 
	o_paid_exp_amt AS paid_exp_amt, 
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount, 
	DirectLossPaidER, 
	DirectLossPaidIR, 
	DirectALAEPaidER, 
	DirectALAEPaidIR, 
	DirectSalvagePaid, 
	DirectSubrogationPaid, 
	DirectOtherRecoveryPaid, 
	DirectOtherRecoveryLossPaid, 
	DirectOtherRecoveryALAEPaid, 
	DirectLossOutstandingER, 
	DirectLossOutstandingIR, 
	DirectALAEOutstandingER, 
	DirectALAEOutstandingIR, 
	DirectOtherRecoveryOutstanding, 
	DirectOtherRecoveryLossOutstanding, 
	DirectOtherRecoveryALAEOutstanding, 
	DirectSubroOutstanding, 
	DirectSalvageOutstanding, 
	DirectLossIncurredER, 
	DirectLossIncurredIR, 
	DirectALAEIncurredER, 
	DirectALAEIncurredIR, 
	OrderInd
	FROM EXP_ByProduct1
	ORDER BY clndr_date ASC, ProductDescription ASC, OrderInd ASC
),
AGG_ByProduct11 AS (
	SELECT
	table_name,
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	-- *INF*: sum(outstanding_amt)
	sum(outstanding_amt) AS o_outstanding_amt,
	paid_loss_amt,
	-- *INF*: sum(paid_loss_amt)
	sum(paid_loss_amt) AS o_paid_loss_amt,
	paid_exp_amt,
	-- *INF*: sum(paid_exp_amt)
	sum(paid_exp_amt) AS o_paid_exp_amt,
	ChangeInOutstandingAmount,
	-- *INF*: sum(ChangeInOutstandingAmount)
	sum(ChangeInOutstandingAmount) AS o_ChangeInOutstandingAmount,
	DirectLossPaidER,
	-- *INF*: sum(DirectLossPaidER)
	sum(DirectLossPaidER) AS o_DirectLossPaidER,
	DirectLossPaidIR,
	-- *INF*: sum(DirectLossPaidIR)
	sum(DirectLossPaidIR) AS o_DirectLossPaidIR,
	DirectALAEPaidER,
	-- *INF*: sum(DirectALAEPaidER)
	sum(DirectALAEPaidER) AS o_DirectALAEPaidER,
	DirectALAEPaidIR,
	-- *INF*: sum(DirectALAEPaidIR)
	sum(DirectALAEPaidIR) AS o_DirectALAEPaidIR,
	DirectSalvagePaid,
	-- *INF*: sum(DirectSalvagePaid)
	sum(DirectSalvagePaid) AS o_DirectSalvagePaid,
	DirectSubrogationPaid,
	-- *INF*: sum(DirectSubrogationPaid)
	sum(DirectSubrogationPaid) AS o_DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	-- *INF*: sum(DirectOtherRecoveryPaid)
	sum(DirectOtherRecoveryPaid) AS o_DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	-- *INF*: sum(DirectOtherRecoveryLossPaid)
	sum(DirectOtherRecoveryLossPaid) AS o_DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	-- *INF*: sum(DirectOtherRecoveryALAEPaid)
	sum(DirectOtherRecoveryALAEPaid) AS o_DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	-- *INF*: sum(DirectLossOutstandingER)
	sum(DirectLossOutstandingER) AS o_DirectLossOutstandingER,
	DirectLossOutstandingIR,
	-- *INF*: sum(DirectLossOutstandingIR)
	sum(DirectLossOutstandingIR) AS o_DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	-- *INF*: sum(DirectALAEOutstandingER)
	sum(DirectALAEOutstandingER) AS o_DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	-- *INF*: sum(DirectALAEOutstandingIR)
	sum(DirectALAEOutstandingIR) AS o_DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	-- *INF*: sum(DirectOtherRecoveryOutstanding)
	sum(DirectOtherRecoveryOutstanding) AS o_DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	-- *INF*: sum(DirectOtherRecoveryLossOutstanding)
	sum(DirectOtherRecoveryLossOutstanding) AS o_DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	-- *INF*: sum(DirectOtherRecoveryALAEOutstanding)
	sum(DirectOtherRecoveryALAEOutstanding) AS o_DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	-- *INF*: sum(DirectSubroOutstanding)
	sum(DirectSubroOutstanding) AS o_DirectSubroOutstanding,
	DirectSalvageOutstanding,
	-- *INF*: sum(DirectSalvageOutstanding)
	sum(DirectSalvageOutstanding) AS o_DirectSalvageOutstanding,
	DirectLossIncurredER,
	-- *INF*: sum(DirectLossIncurredER)
	sum(DirectLossIncurredER) AS o_DirectLossIncurredER,
	DirectLossIncurredIR,
	-- *INF*: sum(DirectLossIncurredIR)
	sum(DirectLossIncurredIR) AS o_DirectLossIncurredIR,
	DirectALAEIncurredER,
	-- *INF*: sum(DirectALAEIncurredER)
	sum(DirectALAEIncurredER) AS o_DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: sum(DirectALAEIncurredIR)
	sum(DirectALAEIncurredIR) AS o_DirectALAEIncurredIR
	FROM Union311
	GROUP BY table_name, clndr_date, ProductDescription
),
EXP_ByProduct11 AS (
	SELECT
	'Balance vwLossMasterFact to claim_loss_transaction_fact' AS BalancingDescription,
	table_name,
	clndr_date,
	ProductDescription,
	o_outstanding_amt AS outstanding_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(outstanding_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(outstanding_amt)) AS o_outstanding_amt,
	o_paid_loss_amt AS paid_loss_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_loss_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_loss_amt)) AS o_paid_loss_amt,
	o_paid_exp_amt AS paid_exp_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_exp_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_exp_amt)) AS o_paid_exp_amt,
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(ChangeInOutstandingAmount))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(ChangeInOutstandingAmount)) AS o_ChangeInOutstandingAmount,
	o_DirectLossPaidER AS DirectLossPaidER,
	o_DirectLossPaidIR AS DirectLossPaidIR,
	o_DirectALAEPaidER AS DirectALAEPaidER,
	o_DirectALAEPaidIR AS DirectALAEPaidIR,
	o_DirectSalvagePaid AS DirectSalvagePaid,
	o_DirectSubrogationPaid AS DirectSubrogationPaid,
	o_DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid,
	o_DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid,
	o_DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid,
	o_DirectLossOutstandingER AS DirectLossOutstandingER,
	o_DirectLossOutstandingIR AS DirectLossOutstandingIR,
	o_DirectALAEOutstandingER AS DirectALAEOutstandingER,
	o_DirectALAEOutstandingIR AS DirectALAEOutstandingIR,
	o_DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding,
	o_DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding,
	o_DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding,
	o_DirectSubroOutstanding AS DirectSubroOutstanding,
	o_DirectSalvageOutstanding AS DirectSalvageOutstanding,
	o_DirectLossIncurredER AS DirectLossIncurredER,
	o_DirectLossIncurredIR AS DirectLossIncurredIR,
	o_DirectALAEIncurredER AS DirectALAEIncurredER,
	o_DirectALAEIncurredIR AS DirectALAEIncurredIR,
	-- *INF*: DECODE(TRUE,
	-- table_name='vwLossMasterFact',1,
	-- table_name='claim_loss_transaction_fact',2,
	-- table_name='Difference',3
	-- )
	DECODE(TRUE,
		table_name = 'vwLossMasterFact', 1,
		table_name = 'claim_loss_transaction_fact', 2,
		table_name = 'Difference', 3) AS OrderInd
	FROM AGG_ByProduct11
),
SRT_ByProduct11 AS (
	SELECT
	BalancingDescription, 
	table_name, 
	clndr_date, 
	ProductDescription, 
	o_outstanding_amt AS outstanding_amt, 
	o_paid_loss_amt AS paid_loss_amt, 
	o_paid_exp_amt AS paid_exp_amt, 
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount, 
	DirectLossPaidER, 
	DirectLossPaidIR, 
	DirectALAEPaidER, 
	DirectALAEPaidIR, 
	DirectSalvagePaid, 
	DirectSubrogationPaid, 
	DirectOtherRecoveryPaid, 
	DirectOtherRecoveryLossPaid, 
	DirectOtherRecoveryALAEPaid, 
	DirectLossOutstandingER, 
	DirectLossOutstandingIR, 
	DirectALAEOutstandingER, 
	DirectALAEOutstandingIR, 
	DirectOtherRecoveryOutstanding, 
	DirectOtherRecoveryLossOutstanding, 
	DirectOtherRecoveryALAEOutstanding, 
	DirectSubroOutstanding, 
	DirectSalvageOutstanding, 
	DirectLossIncurredER, 
	DirectLossIncurredIR, 
	DirectALAEIncurredER, 
	DirectALAEIncurredIR, 
	OrderInd
	FROM EXP_ByProduct11
	ORDER BY clndr_date ASC, ProductDescription ASC, OrderInd ASC
),
Union_ByProduct AS (
	SELECT BalancingDescription, table_name, clndr_date, ProductDescription, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR
	FROM SRT_ByProduct
	UNION
	SELECT BalancingDescription, table_name, clndr_date, ProductDescription, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR
	FROM SRT_ByProduct1
	UNION
	SELECT BalancingDescription, table_name, clndr_date, ProductDescription, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR
	FROM SRT_ByProduct11
),
ClaimFacts_Balancing_Product AS (
	INSERT INTO ClaimFacts_Balancing_Product
	(BalancingDescription, TableName, clndr_date, ProductDescription, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR)
	SELECT 
	BALANCINGDESCRIPTION, 
	table_name AS TABLENAME, 
	CLNDR_DATE, 
	PRODUCTDESCRIPTION, 
	OUTSTANDING_AMT, 
	PAID_LOSS_AMT, 
	PAID_EXP_AMT, 
	CHANGEINOUTSTANDINGAMOUNT, 
	DIRECTLOSSPAIDER, 
	DIRECTLOSSPAIDIR, 
	DIRECTALAEPAIDER, 
	DIRECTALAEPAIDIR, 
	DIRECTSALVAGEPAID, 
	DIRECTSUBROGATIONPAID, 
	DIRECTOTHERRECOVERYPAID, 
	DIRECTOTHERRECOVERYLOSSPAID, 
	DIRECTOTHERRECOVERYALAEPAID, 
	DIRECTLOSSOUTSTANDINGER, 
	DIRECTLOSSOUTSTANDINGIR, 
	DIRECTALAEOUTSTANDINGER, 
	DIRECTALAEOUTSTANDINGIR, 
	DIRECTOTHERRECOVERYOUTSTANDING, 
	DIRECTOTHERRECOVERYLOSSOUTSTANDING, 
	DIRECTOTHERRECOVERYALAEOUTSTANDING, 
	DIRECTSUBROOUTSTANDING, 
	DIRECTSALVAGEOUTSTANDING, 
	DIRECTLOSSINCURREDER, 
	DIRECTLOSSINCURREDIR, 
	DIRECTALAEINCURREDER, 
	DIRECTALAEINCURREDIR
	FROM Union_ByProduct
),
AGG_ByLineOfBusiness AS (
	SELECT
	table_name,
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	-- *INF*: sum(outstanding_amt)
	sum(outstanding_amt) AS o_outstanding_amt,
	paid_loss_amt,
	-- *INF*: sum(paid_loss_amt)
	sum(paid_loss_amt) AS o_paid_loss_amt,
	paid_exp_amt,
	-- *INF*: sum(paid_exp_amt)
	sum(paid_exp_amt) AS o_paid_exp_amt,
	ChangeInOutstandingAmount,
	-- *INF*: sum(ChangeInOutstandingAmount)
	sum(ChangeInOutstandingAmount) AS o_ChangeInOutstandingAmount,
	DirectLossPaidER,
	-- *INF*: sum(DirectLossPaidER)
	sum(DirectLossPaidER) AS o_DirectLossPaidER,
	DirectLossPaidIR,
	-- *INF*: sum(DirectLossPaidIR)
	sum(DirectLossPaidIR) AS o_DirectLossPaidIR,
	DirectALAEPaidER,
	-- *INF*: sum(DirectALAEPaidER)
	sum(DirectALAEPaidER) AS o_DirectALAEPaidER,
	DirectALAEPaidIR,
	-- *INF*: sum(DirectALAEPaidIR)
	sum(DirectALAEPaidIR) AS o_DirectALAEPaidIR,
	DirectSalvagePaid,
	-- *INF*: sum(DirectSalvagePaid)
	sum(DirectSalvagePaid) AS o_DirectSalvagePaid,
	DirectSubrogationPaid,
	-- *INF*: sum(DirectSubrogationPaid)
	sum(DirectSubrogationPaid) AS o_DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	-- *INF*: sum(DirectOtherRecoveryPaid)
	sum(DirectOtherRecoveryPaid) AS o_DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	-- *INF*: sum(DirectOtherRecoveryLossPaid)
	sum(DirectOtherRecoveryLossPaid) AS o_DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	-- *INF*: sum(DirectOtherRecoveryALAEPaid)
	sum(DirectOtherRecoveryALAEPaid) AS o_DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	-- *INF*: sum(DirectLossOutstandingER)
	sum(DirectLossOutstandingER) AS o_DirectLossOutstandingER,
	DirectLossOutstandingIR,
	-- *INF*: sum(DirectLossOutstandingIR)
	sum(DirectLossOutstandingIR) AS o_DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	-- *INF*: sum(DirectALAEOutstandingER)
	sum(DirectALAEOutstandingER) AS o_DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	-- *INF*: sum(DirectALAEOutstandingIR)
	sum(DirectALAEOutstandingIR) AS o_DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	-- *INF*: sum(DirectOtherRecoveryOutstanding)
	sum(DirectOtherRecoveryOutstanding) AS o_DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	-- *INF*: sum(DirectOtherRecoveryLossOutstanding)
	sum(DirectOtherRecoveryLossOutstanding) AS o_DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	-- *INF*: sum(DirectOtherRecoveryALAEOutstanding)
	sum(DirectOtherRecoveryALAEOutstanding) AS o_DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	-- *INF*: sum(DirectSubroOutstanding)
	sum(DirectSubroOutstanding) AS o_DirectSubroOutstanding,
	DirectSalvageOutstanding,
	-- *INF*: sum(DirectSalvageOutstanding)
	sum(DirectSalvageOutstanding) AS o_DirectSalvageOutstanding,
	DirectLossIncurredER,
	-- *INF*: sum(DirectLossIncurredER)
	sum(DirectLossIncurredER) AS o_DirectLossIncurredER,
	DirectLossIncurredIR,
	-- *INF*: sum(DirectLossIncurredIR)
	sum(DirectLossIncurredIR) AS o_DirectLossIncurredIR,
	DirectALAEIncurredER,
	-- *INF*: sum(DirectALAEIncurredER)
	sum(DirectALAEIncurredER) AS o_DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: sum(DirectALAEIncurredIR)
	sum(DirectALAEIncurredIR) AS o_DirectALAEIncurredIR
	FROM Union3
	GROUP BY table_name, clndr_date, InsuranceReferenceLineOfBusinessDescription
),
EXP_ByLineOfBusiness AS (
	SELECT
	'Balance loss_master_fact to vwLossMasterFact' AS BalancingDescription,
	table_name,
	clndr_date,
	InsuranceReferenceLineOfBusinessDescription,
	o_outstanding_amt AS outstanding_amt,
	o_paid_loss_amt AS paid_loss_amt,
	o_paid_exp_amt AS paid_exp_amt,
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount,
	o_DirectLossPaidER AS DirectLossPaidER,
	o_DirectLossPaidIR AS DirectLossPaidIR,
	o_DirectALAEPaidER AS DirectALAEPaidER,
	o_DirectALAEPaidIR AS DirectALAEPaidIR,
	o_DirectSalvagePaid AS DirectSalvagePaid,
	o_DirectSubrogationPaid AS DirectSubrogationPaid,
	o_DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid,
	o_DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid,
	o_DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid,
	o_DirectLossOutstandingER AS DirectLossOutstandingER,
	o_DirectLossOutstandingIR AS DirectLossOutstandingIR,
	o_DirectALAEOutstandingER AS DirectALAEOutstandingER,
	o_DirectALAEOutstandingIR AS DirectALAEOutstandingIR,
	o_DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding,
	o_DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding,
	o_DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding,
	o_DirectSubroOutstanding AS DirectSubroOutstanding,
	o_DirectSalvageOutstanding AS DirectSalvageOutstanding,
	o_DirectLossIncurredER AS DirectLossIncurredER,
	o_DirectLossIncurredIR AS DirectLossIncurredIR,
	o_DirectALAEIncurredER AS DirectALAEIncurredER,
	o_DirectALAEIncurredIR AS DirectALAEIncurredIR,
	-- *INF*: DECODE(TRUE,
	-- table_name='loss_master_fact',1,
	-- table_name='vwLossMasterFact',2,
	-- table_name='Difference',3
	-- )
	DECODE(TRUE,
		table_name = 'loss_master_fact', 1,
		table_name = 'vwLossMasterFact', 2,
		table_name = 'Difference', 3) AS OrderInd
	FROM AGG_ByLineOfBusiness
),
SRT_ByLineOfBusiness AS (
	SELECT
	BalancingDescription, 
	table_name, 
	clndr_date, 
	InsuranceReferenceLineOfBusinessDescription, 
	outstanding_amt, 
	paid_loss_amt, 
	paid_exp_amt, 
	ChangeInOutstandingAmount, 
	DirectLossPaidER, 
	DirectLossPaidIR, 
	DirectALAEPaidER, 
	DirectALAEPaidIR, 
	DirectSalvagePaid, 
	DirectSubrogationPaid, 
	DirectOtherRecoveryPaid, 
	DirectOtherRecoveryLossPaid, 
	DirectOtherRecoveryALAEPaid, 
	DirectLossOutstandingER, 
	DirectLossOutstandingIR, 
	DirectALAEOutstandingER, 
	DirectALAEOutstandingIR, 
	DirectOtherRecoveryOutstanding, 
	DirectOtherRecoveryLossOutstanding, 
	DirectOtherRecoveryALAEOutstanding, 
	DirectSubroOutstanding, 
	DirectSalvageOutstanding, 
	DirectLossIncurredER, 
	DirectLossIncurredIR, 
	DirectALAEIncurredER, 
	DirectALAEIncurredIR, 
	OrderInd
	FROM EXP_ByLineOfBusiness
	ORDER BY clndr_date ASC, InsuranceReferenceLineOfBusinessDescription ASC, OrderInd ASC
),
AGG_ByLineOfBusiness1 AS (
	SELECT
	table_name,
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	-- *INF*: sum(outstanding_amt)
	sum(outstanding_amt) AS o_outstanding_amt,
	paid_loss_amt,
	-- *INF*: sum(paid_loss_amt)
	sum(paid_loss_amt) AS o_paid_loss_amt,
	paid_exp_amt,
	-- *INF*: sum(paid_exp_amt)
	sum(paid_exp_amt) AS o_paid_exp_amt,
	ChangeInOutstandingAmount,
	-- *INF*: sum(ChangeInOutstandingAmount)
	sum(ChangeInOutstandingAmount) AS o_ChangeInOutstandingAmount,
	DirectLossPaidER,
	-- *INF*: sum(DirectLossPaidER)
	sum(DirectLossPaidER) AS o_DirectLossPaidER,
	DirectLossPaidIR,
	-- *INF*: sum(DirectLossPaidIR)
	sum(DirectLossPaidIR) AS o_DirectLossPaidIR,
	DirectALAEPaidER,
	-- *INF*: sum(DirectALAEPaidER)
	sum(DirectALAEPaidER) AS o_DirectALAEPaidER,
	DirectALAEPaidIR,
	-- *INF*: sum(DirectALAEPaidIR)
	sum(DirectALAEPaidIR) AS o_DirectALAEPaidIR,
	DirectSalvagePaid,
	-- *INF*: sum(DirectSalvagePaid)
	sum(DirectSalvagePaid) AS o_DirectSalvagePaid,
	DirectSubrogationPaid,
	-- *INF*: sum(DirectSubrogationPaid)
	sum(DirectSubrogationPaid) AS o_DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	-- *INF*: sum(DirectOtherRecoveryPaid)
	sum(DirectOtherRecoveryPaid) AS o_DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	-- *INF*: sum(DirectOtherRecoveryLossPaid)
	sum(DirectOtherRecoveryLossPaid) AS o_DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	-- *INF*: sum(DirectOtherRecoveryALAEPaid)
	sum(DirectOtherRecoveryALAEPaid) AS o_DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	-- *INF*: sum(DirectLossOutstandingER)
	sum(DirectLossOutstandingER) AS o_DirectLossOutstandingER,
	DirectLossOutstandingIR,
	-- *INF*: sum(DirectLossOutstandingIR)
	sum(DirectLossOutstandingIR) AS o_DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	-- *INF*: sum(DirectALAEOutstandingER)
	sum(DirectALAEOutstandingER) AS o_DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	-- *INF*: sum(DirectALAEOutstandingIR)
	sum(DirectALAEOutstandingIR) AS o_DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	-- *INF*: sum(DirectOtherRecoveryOutstanding)
	sum(DirectOtherRecoveryOutstanding) AS o_DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	-- *INF*: sum(DirectOtherRecoveryLossOutstanding)
	sum(DirectOtherRecoveryLossOutstanding) AS o_DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	-- *INF*: sum(DirectOtherRecoveryALAEOutstanding)
	sum(DirectOtherRecoveryALAEOutstanding) AS o_DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	-- *INF*: sum(DirectSubroOutstanding)
	sum(DirectSubroOutstanding) AS o_DirectSubroOutstanding,
	DirectSalvageOutstanding,
	-- *INF*: sum(DirectSalvageOutstanding)
	sum(DirectSalvageOutstanding) AS o_DirectSalvageOutstanding,
	DirectLossIncurredER,
	-- *INF*: sum(DirectLossIncurredER)
	sum(DirectLossIncurredER) AS o_DirectLossIncurredER,
	DirectLossIncurredIR,
	-- *INF*: sum(DirectLossIncurredIR)
	sum(DirectLossIncurredIR) AS o_DirectLossIncurredIR,
	DirectALAEIncurredER,
	-- *INF*: sum(DirectALAEIncurredER)
	sum(DirectALAEIncurredER) AS o_DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: sum(DirectALAEIncurredIR)
	sum(DirectALAEIncurredIR) AS o_DirectALAEIncurredIR
	FROM Union31
	GROUP BY table_name, clndr_date, InsuranceReferenceLineOfBusinessDescription
),
EXP_ByLineOfBusiness1 AS (
	SELECT
	'Balance loss_master_fact to claim_loss_transaction_fact' AS BalancingDescription,
	table_name,
	clndr_date,
	InsuranceReferenceLineOfBusinessDescription,
	o_outstanding_amt AS outstanding_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(outstanding_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(outstanding_amt)) AS o_outstanding_amt,
	o_paid_loss_amt AS paid_loss_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_loss_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_loss_amt)) AS o_paid_loss_amt,
	o_paid_exp_amt AS paid_exp_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_exp_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_exp_amt)) AS o_paid_exp_amt,
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(ChangeInOutstandingAmount))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(ChangeInOutstandingAmount)) AS o_ChangeInOutstandingAmount,
	o_DirectLossPaidER AS DirectLossPaidER,
	o_DirectLossPaidIR AS DirectLossPaidIR,
	o_DirectALAEPaidER AS DirectALAEPaidER,
	o_DirectALAEPaidIR AS DirectALAEPaidIR,
	o_DirectSalvagePaid AS DirectSalvagePaid,
	o_DirectSubrogationPaid AS DirectSubrogationPaid,
	o_DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid,
	o_DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid,
	o_DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid,
	o_DirectLossOutstandingER AS DirectLossOutstandingER,
	o_DirectLossOutstandingIR AS DirectLossOutstandingIR,
	o_DirectALAEOutstandingER AS DirectALAEOutstandingER,
	o_DirectALAEOutstandingIR AS DirectALAEOutstandingIR,
	o_DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding,
	o_DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding,
	o_DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding,
	o_DirectSubroOutstanding AS DirectSubroOutstanding,
	o_DirectSalvageOutstanding AS DirectSalvageOutstanding,
	o_DirectLossIncurredER AS DirectLossIncurredER,
	o_DirectLossIncurredIR AS DirectLossIncurredIR,
	o_DirectALAEIncurredER AS DirectALAEIncurredER,
	o_DirectALAEIncurredIR AS DirectALAEIncurredIR,
	-- *INF*: DECODE(TRUE,
	-- table_name='loss_master_fact',1,
	-- table_name='claim_loss_transaction_fact',2,
	-- table_name='Difference',3
	-- )
	DECODE(TRUE,
		table_name = 'loss_master_fact', 1,
		table_name = 'claim_loss_transaction_fact', 2,
		table_name = 'Difference', 3) AS OrderInd
	FROM AGG_ByLineOfBusiness1
),
SRT_ByLineOfBusiness1 AS (
	SELECT
	BalancingDescription, 
	table_name, 
	clndr_date, 
	InsuranceReferenceLineOfBusinessDescription, 
	o_outstanding_amt AS outstanding_amt, 
	o_paid_loss_amt AS paid_loss_amt, 
	o_paid_exp_amt AS paid_exp_amt, 
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount, 
	DirectLossPaidER, 
	DirectLossPaidIR, 
	DirectALAEPaidER, 
	DirectALAEPaidIR, 
	DirectSalvagePaid, 
	DirectSubrogationPaid, 
	DirectOtherRecoveryPaid, 
	DirectOtherRecoveryLossPaid, 
	DirectOtherRecoveryALAEPaid, 
	DirectLossOutstandingER, 
	DirectLossOutstandingIR, 
	DirectALAEOutstandingER, 
	DirectALAEOutstandingIR, 
	DirectOtherRecoveryOutstanding, 
	DirectOtherRecoveryLossOutstanding, 
	DirectOtherRecoveryALAEOutstanding, 
	DirectSubroOutstanding, 
	DirectSalvageOutstanding, 
	DirectLossIncurredER, 
	DirectLossIncurredIR, 
	DirectALAEIncurredER, 
	DirectALAEIncurredIR, 
	OrderInd
	FROM EXP_ByLineOfBusiness1
	ORDER BY clndr_date ASC, InsuranceReferenceLineOfBusinessDescription ASC, OrderInd ASC
),
AGG_ByLineOfBusiness11 AS (
	SELECT
	table_name,
	clndr_date,
	EnterpriseGroupDescription,
	StrategicProfitCenterDescription,
	InsuranceReferenceLegalEntityDescription,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	outstanding_amt,
	-- *INF*: sum(outstanding_amt)
	sum(outstanding_amt) AS o_outstanding_amt,
	paid_loss_amt,
	-- *INF*: sum(paid_loss_amt)
	sum(paid_loss_amt) AS o_paid_loss_amt,
	paid_exp_amt,
	-- *INF*: sum(paid_exp_amt)
	sum(paid_exp_amt) AS o_paid_exp_amt,
	ChangeInOutstandingAmount,
	-- *INF*: sum(ChangeInOutstandingAmount)
	sum(ChangeInOutstandingAmount) AS o_ChangeInOutstandingAmount,
	DirectLossPaidER,
	-- *INF*: sum(DirectLossPaidER)
	sum(DirectLossPaidER) AS o_DirectLossPaidER,
	DirectLossPaidIR,
	-- *INF*: sum(DirectLossPaidIR)
	sum(DirectLossPaidIR) AS o_DirectLossPaidIR,
	DirectALAEPaidER,
	-- *INF*: sum(DirectALAEPaidER)
	sum(DirectALAEPaidER) AS o_DirectALAEPaidER,
	DirectALAEPaidIR,
	-- *INF*: sum(DirectALAEPaidIR)
	sum(DirectALAEPaidIR) AS o_DirectALAEPaidIR,
	DirectSalvagePaid,
	-- *INF*: sum(DirectSalvagePaid)
	sum(DirectSalvagePaid) AS o_DirectSalvagePaid,
	DirectSubrogationPaid,
	-- *INF*: sum(DirectSubrogationPaid)
	sum(DirectSubrogationPaid) AS o_DirectSubrogationPaid,
	DirectOtherRecoveryPaid,
	-- *INF*: sum(DirectOtherRecoveryPaid)
	sum(DirectOtherRecoveryPaid) AS o_DirectOtherRecoveryPaid,
	DirectOtherRecoveryLossPaid,
	-- *INF*: sum(DirectOtherRecoveryLossPaid)
	sum(DirectOtherRecoveryLossPaid) AS o_DirectOtherRecoveryLossPaid,
	DirectOtherRecoveryALAEPaid,
	-- *INF*: sum(DirectOtherRecoveryALAEPaid)
	sum(DirectOtherRecoveryALAEPaid) AS o_DirectOtherRecoveryALAEPaid,
	DirectLossOutstandingER,
	-- *INF*: sum(DirectLossOutstandingER)
	sum(DirectLossOutstandingER) AS o_DirectLossOutstandingER,
	DirectLossOutstandingIR,
	-- *INF*: sum(DirectLossOutstandingIR)
	sum(DirectLossOutstandingIR) AS o_DirectLossOutstandingIR,
	DirectALAEOutstandingER,
	-- *INF*: sum(DirectALAEOutstandingER)
	sum(DirectALAEOutstandingER) AS o_DirectALAEOutstandingER,
	DirectALAEOutstandingIR,
	-- *INF*: sum(DirectALAEOutstandingIR)
	sum(DirectALAEOutstandingIR) AS o_DirectALAEOutstandingIR,
	DirectOtherRecoveryOutstanding,
	-- *INF*: sum(DirectOtherRecoveryOutstanding)
	sum(DirectOtherRecoveryOutstanding) AS o_DirectOtherRecoveryOutstanding,
	DirectOtherRecoveryLossOutstanding,
	-- *INF*: sum(DirectOtherRecoveryLossOutstanding)
	sum(DirectOtherRecoveryLossOutstanding) AS o_DirectOtherRecoveryLossOutstanding,
	DirectOtherRecoveryALAEOutstanding,
	-- *INF*: sum(DirectOtherRecoveryALAEOutstanding)
	sum(DirectOtherRecoveryALAEOutstanding) AS o_DirectOtherRecoveryALAEOutstanding,
	DirectSubroOutstanding,
	-- *INF*: sum(DirectSubroOutstanding)
	sum(DirectSubroOutstanding) AS o_DirectSubroOutstanding,
	DirectSalvageOutstanding,
	-- *INF*: sum(DirectSalvageOutstanding)
	sum(DirectSalvageOutstanding) AS o_DirectSalvageOutstanding,
	DirectLossIncurredER,
	-- *INF*: sum(DirectLossIncurredER)
	sum(DirectLossIncurredER) AS o_DirectLossIncurredER,
	DirectLossIncurredIR,
	-- *INF*: sum(DirectLossIncurredIR)
	sum(DirectLossIncurredIR) AS o_DirectLossIncurredIR,
	DirectALAEIncurredER,
	-- *INF*: sum(DirectALAEIncurredER)
	sum(DirectALAEIncurredER) AS o_DirectALAEIncurredER,
	DirectALAEIncurredIR,
	-- *INF*: sum(DirectALAEIncurredIR)
	sum(DirectALAEIncurredIR) AS o_DirectALAEIncurredIR
	FROM Union311
	GROUP BY table_name, clndr_date, InsuranceReferenceLineOfBusinessDescription
),
EXP_ByLineOfBusiness11 AS (
	SELECT
	'Balance vwLossMasterFact to claim_loss_transaction_fact' AS BalancingDescription,
	table_name,
	clndr_date,
	InsuranceReferenceLineOfBusinessDescription,
	o_outstanding_amt AS outstanding_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(outstanding_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(outstanding_amt)) AS o_outstanding_amt,
	o_paid_loss_amt AS paid_loss_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_loss_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_loss_amt)) AS o_paid_loss_amt,
	o_paid_exp_amt AS paid_exp_amt,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(paid_exp_amt))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(paid_exp_amt)) AS o_paid_exp_amt,
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount,
	-- *INF*: IIF(IN(table_name,'claim_loss_transaction_fact','Difference'),'Not Available',TO_CHAR(ChangeInOutstandingAmount))
	IFF(IN(table_name, 'claim_loss_transaction_fact', 'Difference'), 'Not Available', TO_CHAR(ChangeInOutstandingAmount)) AS o_ChangeInOutstandingAmount,
	o_DirectLossPaidER AS DirectLossPaidER,
	o_DirectLossPaidIR AS DirectLossPaidIR,
	o_DirectALAEPaidER AS DirectALAEPaidER,
	o_DirectALAEPaidIR AS DirectALAEPaidIR,
	o_DirectSalvagePaid AS DirectSalvagePaid,
	o_DirectSubrogationPaid AS DirectSubrogationPaid,
	o_DirectOtherRecoveryPaid AS DirectOtherRecoveryPaid,
	o_DirectOtherRecoveryLossPaid AS DirectOtherRecoveryLossPaid,
	o_DirectOtherRecoveryALAEPaid AS DirectOtherRecoveryALAEPaid,
	o_DirectLossOutstandingER AS DirectLossOutstandingER,
	o_DirectLossOutstandingIR AS DirectLossOutstandingIR,
	o_DirectALAEOutstandingER AS DirectALAEOutstandingER,
	o_DirectALAEOutstandingIR AS DirectALAEOutstandingIR,
	o_DirectOtherRecoveryOutstanding AS DirectOtherRecoveryOutstanding,
	o_DirectOtherRecoveryLossOutstanding AS DirectOtherRecoveryLossOutstanding,
	o_DirectOtherRecoveryALAEOutstanding AS DirectOtherRecoveryALAEOutstanding,
	o_DirectSubroOutstanding AS DirectSubroOutstanding,
	o_DirectSalvageOutstanding AS DirectSalvageOutstanding,
	o_DirectLossIncurredER AS DirectLossIncurredER,
	o_DirectLossIncurredIR AS DirectLossIncurredIR,
	o_DirectALAEIncurredER AS DirectALAEIncurredER,
	o_DirectALAEIncurredIR AS DirectALAEIncurredIR,
	-- *INF*: DECODE(TRUE,
	-- table_name='vwLossMasterFact',1,
	-- table_name='claim_loss_transaction_fact',2,
	-- table_name='Difference',3
	-- )
	DECODE(TRUE,
		table_name = 'vwLossMasterFact', 1,
		table_name = 'claim_loss_transaction_fact', 2,
		table_name = 'Difference', 3) AS OrderInd
	FROM AGG_ByLineOfBusiness11
),
SRT_ByLineOfBusiness11 AS (
	SELECT
	BalancingDescription, 
	table_name, 
	clndr_date, 
	InsuranceReferenceLineOfBusinessDescription, 
	o_outstanding_amt AS outstanding_amt, 
	o_paid_loss_amt AS paid_loss_amt, 
	o_paid_exp_amt AS paid_exp_amt, 
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount, 
	DirectLossPaidER, 
	DirectLossPaidIR, 
	DirectALAEPaidER, 
	DirectALAEPaidIR, 
	DirectSalvagePaid, 
	DirectSubrogationPaid, 
	DirectOtherRecoveryPaid, 
	DirectOtherRecoveryLossPaid, 
	DirectOtherRecoveryALAEPaid, 
	DirectLossOutstandingER, 
	DirectLossOutstandingIR, 
	DirectALAEOutstandingER, 
	DirectALAEOutstandingIR, 
	DirectOtherRecoveryOutstanding, 
	DirectOtherRecoveryLossOutstanding, 
	DirectOtherRecoveryALAEOutstanding, 
	DirectSubroOutstanding, 
	DirectSalvageOutstanding, 
	DirectLossIncurredER, 
	DirectLossIncurredIR, 
	DirectALAEIncurredER, 
	DirectALAEIncurredIR, 
	OrderInd
	FROM EXP_ByLineOfBusiness11
	ORDER BY clndr_date ASC, InsuranceReferenceLineOfBusinessDescription ASC, OrderInd ASC
),
Union_ByLineOfBusiness AS (
	SELECT BalancingDescription, table_name, clndr_date, InsuranceReferenceLineOfBusinessDescription, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR
	FROM SRT_ByLineOfBusiness
	UNION
	SELECT BalancingDescription, table_name, clndr_date, InsuranceReferenceLineOfBusinessDescription, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR
	FROM SRT_ByLineOfBusiness1
	UNION
	SELECT BalancingDescription, table_name, clndr_date, InsuranceReferenceLineOfBusinessDescription, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR
	FROM SRT_ByLineOfBusiness11
),
ClaimFacts_Balancing_LineOfBusiness AS (
	INSERT INTO ClaimFacts_Balancing_LineOfBusiness
	(BalancingDescription, TableName, clndr_date, InsuranceReferenceLineOfBusinessDescription, outstanding_amt, paid_loss_amt, paid_exp_amt, ChangeInOutstandingAmount, DirectLossPaidER, DirectLossPaidIR, DirectALAEPaidER, DirectALAEPaidIR, DirectSalvagePaid, DirectSubrogationPaid, DirectOtherRecoveryPaid, DirectOtherRecoveryLossPaid, DirectOtherRecoveryALAEPaid, DirectLossOutstandingER, DirectLossOutstandingIR, DirectALAEOutstandingER, DirectALAEOutstandingIR, DirectOtherRecoveryOutstanding, DirectOtherRecoveryLossOutstanding, DirectOtherRecoveryALAEOutstanding, DirectSubroOutstanding, DirectSalvageOutstanding, DirectLossIncurredER, DirectLossIncurredIR, DirectALAEIncurredER, DirectALAEIncurredIR)
	SELECT 
	BALANCINGDESCRIPTION, 
	table_name AS TABLENAME, 
	CLNDR_DATE, 
	INSURANCEREFERENCELINEOFBUSINESSDESCRIPTION, 
	OUTSTANDING_AMT, 
	PAID_LOSS_AMT, 
	PAID_EXP_AMT, 
	CHANGEINOUTSTANDINGAMOUNT, 
	DIRECTLOSSPAIDER, 
	DIRECTLOSSPAIDIR, 
	DIRECTALAEPAIDER, 
	DIRECTALAEPAIDIR, 
	DIRECTSALVAGEPAID, 
	DIRECTSUBROGATIONPAID, 
	DIRECTOTHERRECOVERYPAID, 
	DIRECTOTHERRECOVERYLOSSPAID, 
	DIRECTOTHERRECOVERYALAEPAID, 
	DIRECTLOSSOUTSTANDINGER, 
	DIRECTLOSSOUTSTANDINGIR, 
	DIRECTALAEOUTSTANDINGER, 
	DIRECTALAEOUTSTANDINGIR, 
	DIRECTOTHERRECOVERYOUTSTANDING, 
	DIRECTOTHERRECOVERYLOSSOUTSTANDING, 
	DIRECTOTHERRECOVERYALAEOUTSTANDING, 
	DIRECTSUBROOUTSTANDING, 
	DIRECTSALVAGEOUTSTANDING, 
	DIRECTLOSSINCURREDER, 
	DIRECTLOSSINCURREDIR, 
	DIRECTALAEINCURREDER, 
	DIRECTALAEINCURREDIR
	FROM Union_ByLineOfBusiness
),
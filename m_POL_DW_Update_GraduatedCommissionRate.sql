WITH
LKP_GraduatedCommission AS (
	SELECT
	AuthorizedAmount,
	PolicyReference,
	AuthorizationDate
	FROM (
		SELECT 
			AuthorizedAmount,
			PolicyReference,
			AuthorizationDate
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}..WorkDCBILCommissionUpdate
		WHERE UpdateType='Graduate'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyReference,AuthorizationDate ORDER BY AuthorizedAmount) = 1
),
LKP_ItemAmount AS (
	SELECT
	ItemAmount,
	PolicyReference,
	ItemEffectiveDate
	FROM (
		select PT.PolicyReference as PolicyReference,BI.ItemEffectiveDate as ItemEffectiveDate, SUM(ItemAmount-TransferredAmount) as ItemAmount, , BI.TransactionDate as TransactionDate
		from  @{pipeline().parameters.SOURCE_DATABASE_NAME}..DCBILBillItemStage BI join  @{pipeline().parameters.SOURCE_DATABASE_NAME}..DCBILPolicyTermStage PT on BI.PolicyTermId=PT.PolicyTermId
		where BI.ReceivableTypeCode='Prem' 
		group by PT.PolicyReference,BI.ItemEffectiveDate,  BI.TransactionDate 
		ORDER BY PolicyReference,ItemEffectiveDate,  BI.TransactionDate ,ItemAmount--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyReference,ItemEffectiveDate ORDER BY ItemAmount DESC) = 1
),
LKP_SplitPolicy AS (
	SELECT
	PolicyNumber
	FROM (
		select distinct DP.PolicyNumber as PolicyNumber  from @{pipeline().parameters.SOURCE_DATABASE_NAME}..DCCoverageStaging DC
		join @{pipeline().parameters.SOURCE_DATABASE_NAME}..DCPolicyStaging DP on DC.SessionId=DP.SessionId
		join @{pipeline().parameters.SOURCE_DATABASE_NAME}..DCWCStateTermStaging DW on DC.ObjectId=DW.WC_StateTermId
		where DC.ObjectName='DC_WC_StateTerm'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber ORDER BY PolicyNumber) = 1
),
LKP_TransactionEffectiveDate_Split AS (
	SELECT
	TransactionEffectiveDate,
	PremiumTransactionAKId
	FROM (
		select PT.PremiumTransactionAKId as PremiumTransactionAKId , WP.TransactionEffectiveDate as TransactionEffectiveDate  
		from WorkPremiumTransaction PT
		join @{pipeline().parameters.SOURCE_DATABASE_NAME}..WorkDCTTransactionInsuranceLineLocationBridge WB on PT.PremiumTransactionStageId=WB.CoverageId
		join @{pipeline().parameters.SOURCE_DATABASE_NAME}..WorkDCTInsuranceLine WI on WB.LineId=WI.LineId
		join @{pipeline().parameters.SOURCE_DATABASE_NAME}..WorkDCTPolicy WP on WP.PolicyId = WI.PolicyId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKId ORDER BY TransactionEffectiveDate) = 1
),
SQ_PremiumTransaction AS (
	With STG as 
	(select distinct P.PolicyNumber 
	from @{pipeline().parameters.SOURCE_DATABASE_NAME}..DCPolicyStaging P
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}..DCLineStaging L on P.SessionId = L.SessionId
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}..WBLineStaging WL on WL.LineId = L.LineId and L.SessionId = WL.SessionId
	where WL.IsGraduated=1)
	select  pt.PremiumTransactionID, pt. PremiumTransactionAKID,pol.pol_num,
	 pt.PremiumTransactionEffectiveDate,pt.CreatedDate as  premiumTranactionCreatedDate,
	sum(pt.PremiumTransactionAmount) over (partition by pol.pol_num, pt.PremiumTransactionEffectiveDate) as PremiumTransactionAmount,
	pt.AgencyActualCommissionRate
	from PremiumTransaction pt	
	inner join RatingCoverage rc	
	on rc.RatingCoverageAKID = pt.RatingCoverageAKId	
	and rc.EffectiveDate = pt.EffectiveDate	
	inner join PolicyCoverage pc	
	on pc.PolicyCoverageAKID = rc.PolicyCoverageAKID	
	and pc.CurrentSnapshotFlag = 1	
	inner join RiskLocation rl	
	on pc.RiskLocationAKID = rl.RiskLocationAKID	
	and rl.CurrentSnapshotFlag = 1	
	inner join @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol	
	on pol.pol_ak_id = pc.PolicyAKID	
	and pol.crrnt_snpsht_flag = 1
	inner join STG on STG.PolicyNumber=pol.pol_num
	where 
	PT.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	and PC.InsuranceLine='WorkersCompensation'	
	and PT.ReasonAmendedCode not in ('CWO','Claw Back')
	order by pt.PremiumTransactionID, pt. PremiumTransactionAKID, pol.pol_num, pt.PremiumTransactionEffectiveDate,pt.CreatedDate
),
EXP_Calc AS (
	SELECT
	PremiumTransactionID,
	PremiumTransactionAKID,
	pol_num,
	PremiumTransactionEffectiveDate,
	PremiumTransactionAmount,
	AgencyActualCommissionRate,
	-- *INF*: :LKP.LKP_GRADUATEDCOMMISSION(pol_num,PremiumTransactionEffectiveDate)
	LKP_GRADUATEDCOMMISSION_pol_num_PremiumTransactionEffectiveDate.AuthorizedAmount AS lkp_AuthorizedAmount,
	-- *INF*: :LKP.LKP_ITEMAMOUNT(pol_num,PremiumTransactionEffectiveDate)
	LKP_ITEMAMOUNT_pol_num_PremiumTransactionEffectiveDate.ItemAmount AS lkp_ItemAmount,
	-- *INF*: IIF(ISNULL(:LKP.LKP_SPLITPOLICY(pol_num)),'0', :LKP.LKP_SPLITPOLICY(pol_num))
	IFF(LKP_SPLITPOLICY_pol_num.PolicyNumber IS NULL, '0', LKP_SPLITPOLICY_pol_num.PolicyNumber) AS v_IsSplit,
	-- *INF*: DECODE(TRUE, 
	-- lkp_ItemAmount=0,0,
	-- ISNULL(lkp_AuthorizedAmount) or ISNULL(lkp_ItemAmount),AgencyActualCommissionRate,lkp_AuthorizedAmount/lkp_ItemAmount)
	DECODE(TRUE,
	lkp_ItemAmount = 0, 0,
	lkp_AuthorizedAmount IS NULL OR lkp_ItemAmount IS NULL, AgencyActualCommissionRate,
	lkp_AuthorizedAmount / lkp_ItemAmount) AS v_CommissionRate,
	-- *INF*: :LKP.LKP_TRANSACTIONEFFECTIVEDATE_SPLIT(PremiumTransactionAKID)
	LKP_TRANSACTIONEFFECTIVEDATE_SPLIT_PremiumTransactionAKID.TransactionEffectiveDate AS v_EffectiveDate_Split,
	-- *INF*: IIF(v_IsSplit<> '0',
	-- :LKP.LKP_GRADUATEDCOMMISSION(pol_num,v_EffectiveDate_Split),0
	-- )
	IFF(v_IsSplit <> '0', LKP_GRADUATEDCOMMISSION_pol_num_v_EffectiveDate_Split.AuthorizedAmount, 0) AS v_AuthorizedAmount_Split,
	-- *INF*: IIF(v_IsSplit<>'0',
	-- :LKP.LKP_ITEMAMOUNT(pol_num,v_EffectiveDate_Split),0)
	IFF(v_IsSplit <> '0', LKP_ITEMAMOUNT_pol_num_v_EffectiveDate_Split.ItemAmount, 0) AS v_ItemAmount_Split,
	-- *INF*: DECODE(TRUE,
	-- v_IsSplit<>'0' and (v_AuthorizedAmount_Split =0 or v_ItemAmount_Split=0),0, 
	-- v_IsSplit<>'0' and v_AuthorizedAmount_Split <>0 and v_ItemAmount_Split<>0 and NOT ISNULL(v_AuthorizedAmount_Split) and NOT ISNULL(v_ItemAmount_Split), v_AuthorizedAmount_Split/v_ItemAmount_Split, v_CommissionRate)
	DECODE(TRUE,
	v_IsSplit <> '0' AND ( v_AuthorizedAmount_Split = 0 OR v_ItemAmount_Split = 0 ), 0,
	v_IsSplit <> '0' AND v_AuthorizedAmount_Split <> 0 AND v_ItemAmount_Split <> 0 AND NOT v_AuthorizedAmount_Split IS NULL AND NOT v_ItemAmount_Split IS NULL, v_AuthorizedAmount_Split / v_ItemAmount_Split,
	v_CommissionRate) AS v_CommissionRate_Split,
	-- *INF*: IIF(v_IsSplit<>'0', v_CommissionRate_Split,v_CommissionRate)
	IFF(v_IsSplit <> '0', v_CommissionRate_Split, v_CommissionRate) AS v_CommissionRate1,
	-- *INF*: IIF(v_CommissionRate1<-1  or v_CommissionRate1>1,0.05111,v_CommissionRate1)
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	IFF(v_CommissionRate1 < - 1 OR v_CommissionRate1 > 1, 0.05111, v_CommissionRate1) AS o_AgencyCommissionRate,
	PremiumTransactionCreatedDate AS i_PremiumTransactionCreatedDate,
	-- *INF*: TRUNC(i_PremiumTransactionCreatedDate)
	TRUNC(i_PremiumTransactionCreatedDate) AS o_PremiumTransactionCreatedDate
	FROM SQ_PremiumTransaction
	LEFT JOIN LKP_GRADUATEDCOMMISSION LKP_GRADUATEDCOMMISSION_pol_num_PremiumTransactionEffectiveDate
	ON LKP_GRADUATEDCOMMISSION_pol_num_PremiumTransactionEffectiveDate.PolicyReference = pol_num
	AND LKP_GRADUATEDCOMMISSION_pol_num_PremiumTransactionEffectiveDate.AuthorizationDate = PremiumTransactionEffectiveDate

	LEFT JOIN LKP_ITEMAMOUNT LKP_ITEMAMOUNT_pol_num_PremiumTransactionEffectiveDate
	ON LKP_ITEMAMOUNT_pol_num_PremiumTransactionEffectiveDate.PolicyReference = pol_num
	AND LKP_ITEMAMOUNT_pol_num_PremiumTransactionEffectiveDate.ItemEffectiveDate = PremiumTransactionEffectiveDate

	LEFT JOIN LKP_SPLITPOLICY LKP_SPLITPOLICY_pol_num
	ON LKP_SPLITPOLICY_pol_num.PolicyNumber = pol_num

	LEFT JOIN LKP_TRANSACTIONEFFECTIVEDATE_SPLIT LKP_TRANSACTIONEFFECTIVEDATE_SPLIT_PremiumTransactionAKID
	ON LKP_TRANSACTIONEFFECTIVEDATE_SPLIT_PremiumTransactionAKID.PremiumTransactionAKId = PremiumTransactionAKID

	LEFT JOIN LKP_GRADUATEDCOMMISSION LKP_GRADUATEDCOMMISSION_pol_num_v_EffectiveDate_Split
	ON LKP_GRADUATEDCOMMISSION_pol_num_v_EffectiveDate_Split.PolicyReference = pol_num
	AND LKP_GRADUATEDCOMMISSION_pol_num_v_EffectiveDate_Split.AuthorizationDate = v_EffectiveDate_Split

	LEFT JOIN LKP_ITEMAMOUNT LKP_ITEMAMOUNT_pol_num_v_EffectiveDate_Split
	ON LKP_ITEMAMOUNT_pol_num_v_EffectiveDate_Split.PolicyReference = pol_num
	AND LKP_ITEMAMOUNT_pol_num_v_EffectiveDate_Split.ItemEffectiveDate = v_EffectiveDate_Split

),
FIL_SysDate AS (
	SELECT
	PremiumTransactionID, 
	o_AgencyCommissionRate AS CommissionRate, 
	o_PremiumTransactionCreatedDate AS PremiumTransactionCreatedDate
	FROM EXP_Calc
	WHERE trunc(PremiumTransactionCreatedDate)= 
trunc(SYSDATE)
),
UPD_Update AS (
	SELECT
	PremiumTransactionID, 
	CommissionRate
	FROM FIL_SysDate
),
PremiumTransaction AS (
	MERGE INTO PremiumTransaction AS T
	USING UPD_Update AS S
	ON T.PremiumTransactionID = S.PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AgencyActualCommissionRate = S.CommissionRate
),
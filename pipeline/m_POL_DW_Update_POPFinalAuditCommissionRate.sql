WITH
SQ_PremiumTransaction AS (
	select pt.PremiumTransactionID,pt.PremiumTransactionCode,
	SUM(case when pt.PremiumTransactionCode in ('FinalAudit','RevisedFinalAudit') then pt.PremiumTransactionAmount else 0 end) over (partition by pol.pol_num) as EDWPremium,
	SUM(case when pt.PremiumTransactionCode in ('FinalAudit','RevisedFinalAudit') then 0 else PremiumTransactionAmount*AgencyActualCommissionRate end) over (partition by pol.pol_num) as EDWCommission,
	DCBIL.AuthorizedAmount as BillingCommission
	from PremiumTransaction pt	
	inner join RatingCoverage rc on rc.RatingCoverageAKID = pt.RatingCoverageAKId	and rc.EffectiveDate = pt.EffectiveDate	
	inner join PolicyCoverage pc	on pc.PolicyCoverageAKID = rc.PolicyCoverageAKID	and pc.CurrentSnapshotFlag = 1	
	inner join RiskLocation rl	on pc.RiskLocationAKID = rl.RiskLocationAKID	and rl.CurrentSnapshotFlag = 1	
	inner join @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol	on pol.pol_ak_id = pc.PolicyAKID	and pol.crrnt_snpsht_flag = 1	
	inner join (select PolicyReference, SUM(AuthorizedAmount) as AuthorizedAmount from @{pipeline().parameters.SOURCE_DATABASE_NAME}..WorkDCBILCommissionUpdate
	where updatetype='FinalAudit' group by PolicyReference) DCBIL
	on DCBIL.PolicyReference=pol.pol_num
	where pt.ReasonAmendedCode not in ('CWO','Claw Back')
),
EXP_CalcRate AS (
	SELECT
	PremiumTransactionID,
	PremiumTransactionCode,
	EDWPremium,
	EDWCommission,
	BillingCommission,
	BillingCommission-EDWCommission AS v_CommissionGap,
	-- *INF*: IIF(v_CommissionGap=0 or EDWPremium=0, 0, v_CommissionGap/EDWPremium)
	IFF(v_CommissionGap = 0 or EDWPremium = 0, 0, v_CommissionGap / EDWPremium) AS v_CommissionRate,
	-- *INF*: IIF(v_CommissionRate < -1 OR v_CommissionRate > 1,0.024,v_CommissionRate)
	IFF(v_CommissionRate < - 1 OR v_CommissionRate > 1, 0.024, v_CommissionRate) AS o_CommissionRate
	FROM SQ_PremiumTransaction
),
FIL_FinalAuditOnly AS (
	SELECT
	PremiumTransactionID, 
	PremiumTransactionCode, 
	o_CommissionRate
	FROM EXP_CalcRate
	WHERE PremiumTransactionCode ='FinalAudit' OR  PremiumTransactionCode = 'RevisedFinalAudit'
),
UPD_Update AS (
	SELECT
	PremiumTransactionID, 
	o_CommissionRate AS CommissionRate
	FROM FIL_FinalAuditOnly
),
PremiumTransaction AS (
	MERGE INTO PremiumTransaction AS T
	USING UPD_Update AS S
	ON T.PremiumTransactionID = S.PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AgencyActualCommissionRate = S.CommissionRate
),
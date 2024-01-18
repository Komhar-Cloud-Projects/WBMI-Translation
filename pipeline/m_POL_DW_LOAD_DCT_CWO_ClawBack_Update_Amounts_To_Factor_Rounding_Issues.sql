WITH
SQ_PremiumTransaction AS (
	select pol.pol_num, pt.PremiumTransactionEffectiveDate, pt.PremiumTransactionID, pt.PremiumTransactionAmount,pt.AgencyActualCommissionRate,pt.ReasonAmendedCode, 
	ROW_NUMBER() over (partition by pol.pol_num, PremiumTransactionEffectiveDate, pt.AgencyActualCommissionRate, pt.ReasonAmendedCode order by pol.pol_num,PremiumTransactionEffectiveDate, pt.ReasonAmendedCode, abs(pt.PremiumTransactionAmount) desc, pt.PremiumTransactionID desc) Num,
	ROW_NUMBER() over (partition by pol.pol_num, PremiumTransactionEffectiveDate, pt.ReasonAmendedCode order by pol.pol_num,PremiumTransactionEffectiveDate, pt.ReasonAmendedCode, abs(pt.PremiumTransactionAmount) desc, pt.PremiumTransactionID desc) NumPolicyLevel,
	SUM(case when pt.ReasonAmendedCode='CWO' then PremiumTransactionAmount else 0 end) over (partition by pol.pol_num, PremiumTransactionEffectiveDate, pt.AgencyActualCommissionRate) as TotalCWOPT,
	SUM(case when pt.ReasonAmendedCode='CWO' then PremiumTransactionAmount else 0 end) over (partition by pol.pol_num,PremiumTransactionEffectiveDate ) as TotalCWOPTPolicyLevel,
	SUM(case when pt.ReasonAmendedCode='Claw Back' then PremiumTransactionAmount else 0 end) over (partition by pol.pol_num,PremiumTransactionEffectiveDate,  pt.AgencyActualCommissionRate) as TotalClawBackPT,
	SUM(case when pt.ReasonAmendedCode='Claw Back' then PremiumTransactionAmount else 0 end) over (partition by pol.pol_num,PremiumTransactionEffectiveDate) as TotalClawBackPTPolicyLevel,
	pol.pol_eff_date, pol.pol_exp_date
	from PremiumTransaction pt
	inner join RatingCoverage rc on rc.RatingCoverageAKID = pt.RatingCoverageAKId and rc.EffectiveDate = pt.EffectiveDate
	inner join PolicyCoverage pc on pc.PolicyCoverageAKID = rc.PolicyCoverageAKID and pc.CurrentSnapshotFlag = 1
	inner join @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol on pol.pol_ak_id = pc.PolicyAKID and pol.crrnt_snpsht_flag = 1
	where pt.ReasonAmendedCode in ('CWO', 'Claw Back')
	and pt.AuditID=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
),
LKP_CWOCLAW AS (
	SELECT
	CWOAmount,
	AuthorizedAmount,
	i_pol_eff_date,
	i_pol_exp_date,
	PolicyReference,
	CommissionPercent,
	AuthorizationDate,
	PolicyTermEffectiveDate,
	PolicyTermExpirationDate
	FROM (
		SELECT 
			CWOAmount,
			AuthorizedAmount,
			i_pol_eff_date,
			i_pol_exp_date,
			PolicyReference,
			CommissionPercent,
			AuthorizationDate,
			PolicyTermEffectiveDate,
			PolicyTermExpirationDate
		FROM WorkDCBILCommissionCWOClawBack
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyReference,CommissionPercent,AuthorizationDate,PolicyTermEffectiveDate,PolicyTermExpirationDate ORDER BY CWOAmount) = 1
),
LKP_CWOCLAW_PolicyLevel AS (
	SELECT
	CWOAmount,
	AuthorizedAmount,
	PolicyReference,
	PolicyTermEffectiveDate,
	PolicyTermExpirationDate,
	AuthorizationDate
	FROM (
		SELECT SUM(WorkDCBILCommissionCWOClawBack.CWOAmount) as CWOAmount, SUM(WorkDCBILCommissionCWOClawBack.AuthorizedAmount) as AuthorizedAmount, WorkDCBILCommissionCWOClawBack.PolicyReference as PolicyReference, WorkDCBILCommissionCWOClawBack.AuthorizationDate as AuthorizationDate,
		PolicyTermEffectiveDate as PolicyTermEffectiveDate  , PolicyTermExpirationDate as PolicyTermExpirationDate 
		FROM WorkDCBILCommissionCWOClawBack
		group by PolicyReference , AuthorizationDate,PolicyTermEffectiveDate ,PolicyTermExpirationDate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyReference,PolicyTermEffectiveDate,PolicyTermExpirationDate,AuthorizationDate ORDER BY CWOAmount) = 1
),
LKP_IsPolicy AS (
	SELECT
	pol_num,
	pol_eff_date,
	pol_exp_date
	FROM (
		select distinct pol.pol_num as pol_num , DCBIL2.PolicyTermEffectiveDate as pol_eff_date, DCBIL2.PolicyTermExpirationDate as pol_exp_date
		from PremiumTransaction pt inner join RatingCoverage rc on rc.RatingCoverageAKID = pt.RatingCoverageAKId and rc.EffectiveDate = pt.EffectiveDate
		inner join PolicyCoverage pc on pc.PolicyCoverageAKID = rc.PolicyCoverageAKID and pc.CurrentSnapshotFlag = 1
		inner join @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol on pol.pol_ak_id = pc.PolicyAKID and pol.crrnt_snpsht_flag = 1
		left join @{pipeline().parameters.SOURCE_DATABASE_NAME}..WorkDCBILCommissionCWOClawBack DCBIL on DCBIL.PolicyReference=pol.pol_num and DCBIL.CommissionPercent=PT.AgencyActualCommissionRate
		and DCBIL.PolicyTermEffectiveDate = pol.pol_eff_date and DCBIL.PolicyTermExpirationDate=pol.pol_exp_date
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}..WorkDCBILCommissionCWOClawBack DCBIL2 
		on DCBIL2.PolicyReference=pol.pol_num and DCBIL2.PolicyTermEffectiveDate = pol.pol_eff_date and DCBIL2.PolicyTermExpirationDate=pol.pol_exp_date
		where pt.ReasonAmendedCode not in ('CWO','Claw Back')
		and pt.SourceSystemID = 'DCT'
		and DCBIL.WorkDCBILCommissionCWOClawBackId is null
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_num,pol_eff_date,pol_exp_date ORDER BY pol_num) = 1
),
EXP_IsPolicyLevel AS (
	SELECT
	SQ_PremiumTransaction.pol_num,
	SQ_PremiumTransaction.PremiumTransactionEffectiveDate,
	SQ_PremiumTransaction.PremiumTransactionID,
	SQ_PremiumTransaction.PremiumTransactionAmount,
	SQ_PremiumTransaction.AgencyActualCommissionRate,
	SQ_PremiumTransaction.ReasonAmendedCode,
	SQ_PremiumTransaction.Num,
	SQ_PremiumTransaction.NumPolicyLevel,
	SQ_PremiumTransaction.TotalCWOPT,
	SQ_PremiumTransaction.TotalCWOPTPolicyLevel,
	SQ_PremiumTransaction.TotalClawBackPT,
	SQ_PremiumTransaction.TotalClawBackPTPolicyLevel,
	LKP_IsPolicy.pol_num AS lkp_pol_num_IsPolicy,
	LKP_CWOCLAW.CWOAmount AS lkp_CWOAmount,
	LKP_CWOCLAW.AuthorizedAmount AS lkp_AuthorizedAmount,
	LKP_CWOCLAW_PolicyLevel.CWOAmount AS lkp_CWOAmount_Policy,
	LKP_CWOCLAW_PolicyLevel.AuthorizedAmount AS lkp_AuthorizedAmount_Policy,
	-- *INF*: IIF(ISNULL(lkp_pol_num_IsPolicy),0,1)
	IFF(lkp_pol_num_IsPolicy IS NULL, 0, 1) AS IsPolicy,
	-- *INF*: IIF(ISNULL(lkp_CWOAmount) AND ISNULL(lkp_CWOAmount_Policy), 1, 0)
	IFF(lkp_CWOAmount IS NULL AND lkp_CWOAmount_Policy IS NULL, 1, 0) AS NoCWO
	FROM SQ_PremiumTransaction
	LEFT JOIN LKP_CWOCLAW
	ON LKP_CWOCLAW.PolicyReference = SQ_PremiumTransaction.pol_num AND LKP_CWOCLAW.CommissionPercent = SQ_PremiumTransaction.AgencyActualCommissionRate AND LKP_CWOCLAW.AuthorizationDate = SQ_PremiumTransaction.PremiumTransactionEffectiveDate AND LKP_CWOCLAW.PolicyTermEffectiveDate = SQ_PremiumTransaction.pol_eff_date AND LKP_CWOCLAW.PolicyTermExpirationDate = SQ_PremiumTransaction.pol_exp_date
	LEFT JOIN LKP_CWOCLAW_PolicyLevel
	ON LKP_CWOCLAW_PolicyLevel.PolicyReference = SQ_PremiumTransaction.pol_num AND LKP_CWOCLAW_PolicyLevel.PolicyTermEffectiveDate = SQ_PremiumTransaction.pol_eff_date AND LKP_CWOCLAW_PolicyLevel.PolicyTermExpirationDate = SQ_PremiumTransaction.pol_exp_date AND LKP_CWOCLAW_PolicyLevel.AuthorizationDate = SQ_PremiumTransaction.PremiumTransactionEffectiveDate
	LEFT JOIN LKP_IsPolicy
	ON LKP_IsPolicy.pol_num = SQ_PremiumTransaction.pol_num AND LKP_IsPolicy.pol_eff_date = SQ_PremiumTransaction.pol_eff_date AND LKP_IsPolicy.pol_exp_date = SQ_PremiumTransaction.pol_exp_date
),
FIL_MaxPremiumTransactionAmount AS (
	SELECT
	pol_num, 
	PremiumTransactionEffectiveDate, 
	PremiumTransactionID, 
	PremiumTransactionAmount, 
	AgencyActualCommissionRate, 
	ReasonAmendedCode, 
	Num, 
	NumPolicyLevel, 
	TotalCWOPT, 
	TotalCWOPTPolicyLevel, 
	TotalClawBackPT, 
	TotalClawBackPTPolicyLevel, 
	lkp_pol_num_IsPolicy, 
	lkp_CWOAmount, 
	lkp_AuthorizedAmount, 
	lkp_CWOAmount_Policy, 
	lkp_AuthorizedAmount_Policy, 
	IsPolicy, 
	NoCWO
	FROM EXP_IsPolicyLevel
	WHERE ((IsPolicy=1 AND NumPolicyLevel=1 AND NOT ISNULL(lkp_CWOAmount_Policy)) OR(IsPolicy=0 AND Num=1AND NOT ISNULL(lkp_CWOAmount))) AND NoCWO=0
),
EXP_CWOAmountCal AS (
	SELECT
	pol_num AS i_pol_num,
	PremiumTransactionEffectiveDate AS i_PremiumTransactionEffectiveDate,
	PremiumTransactionID AS i_PremiumTransactionID,
	PremiumTransactionAmount AS i_PremiumTransactionAmount,
	AgencyActualCommissionRate AS i_AgencyActualCommissionRate,
	ReasonAmendedCode AS i_ReasonAmendedCode,
	Num AS i_Num,
	NumPolicyLevel AS i_NumPolicyLevel,
	TotalCWOPT AS i_TotalCWOPT,
	TotalCWOPTPolicyLevel AS i_TotalCWOPTPolicyLevel,
	TotalClawBackPT AS i_TotalClawBackPT,
	TotalClawBackPTPolicyLevel AS i_TotalClawBackPTPolicyLevel,
	lkp_CWOAmount AS i_CWOAmount,
	lkp_AuthorizedAmount AS i_AuthorizedAmount,
	lkp_CWOAmount_Policy AS i_CWOAmount_Policy,
	lkp_AuthorizedAmount_Policy AS i_AuthorizedAmount_Policy,
	IsPolicy AS i_IsPolicy,
	-- *INF*: IIF(i_IsPolicy=1, i_CWOAmount_Policy-i_TotalCWOPTPolicyLevel,i_CWOAmount-i_TotalCWOPT)
	IFF(
	    i_IsPolicy = 1, i_CWOAmount_Policy - i_TotalCWOPTPolicyLevel, i_CWOAmount - i_TotalCWOPT
	) AS v_CWOAmountGap,
	-- *INF*: IIF(i_IsPolicy=1,i_AuthorizedAmount_Policy-i_TotalClawBackPTPolicyLevel,i_AuthorizedAmount-i_TotalClawBackPT)
	IFF(
	    i_IsPolicy = 1, i_AuthorizedAmount_Policy - i_TotalClawBackPTPolicyLevel,
	    i_AuthorizedAmount - i_TotalClawBackPT
	) AS v_ClawBackAmountGap,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	-- *INF*: IIF(i_ReasonAmendedCode='CWO',i_PremiumTransactionAmount+v_CWOAmountGap,i_PremiumTransactionAmount+v_ClawBackAmountGap)
	IFF(
	    i_ReasonAmendedCode = 'CWO', i_PremiumTransactionAmount + v_CWOAmountGap,
	    i_PremiumTransactionAmount + v_ClawBackAmountGap
	) AS o_PremiumTransactionAmount
	FROM FIL_MaxPremiumTransactionAmount
),
UPD_Update AS (
	SELECT
	o_PremiumTransactionID, 
	o_PremiumTransactionAmount
	FROM EXP_CWOAmountCal
),
PremiumTransaction_Update AS (
	MERGE INTO PremiumTransaction AS T
	USING UPD_Update AS S
	ON T.PremiumTransactionID = S.o_PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.PremiumTransactionAmount = S.o_PremiumTransactionAmount

	------------ POST SQL ----------
	Update PremiumTransaction set AgencyActualCommissionRate=0 where auditID=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} and ReasonAmendedCode = 'CWO'
	-------------------------------


),
WITH
SQ_PassThroughChargeTransaction AS (
	With SRC as
	(select pol.pol_num, pt.PassThroughChargeTransactionID, pt.PassThroughChargeTransactionAmount,pt.PassThroughChargeTransactionEffectiveDate,
	ROW_NUMBER() over (partition by pol.pol_num,pt.PassThroughChargeTransactionEffectiveDate order by pol.pol_num,pt.PassThroughChargeTransactionEffectiveDate, abs(pt.PassThroughChargeTransactionAmount) desc, pt.PassThroughChargeTransactionID desc) Num,
	SUM(PassThroughChargeTransactionAmount) over (partition by pol.pol_num, pt.PassThroughChargeTransactionEffectiveDate) as TotalCWOPT,
	pol.pol_eff_date,pol.pol_exp_date
	from PassThroughChargeTransaction pt
	inner join @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol
	on pol.pol_ak_id = pt.PolicyAKID
	and pol.crrnt_snpsht_flag = 1
	where pt.ReasonAmendedCode='CWO'
	and pt.AuditID=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID})
	
	select SRC.PassThroughChargeTransactionID, SRC.PassThroughChargeTransactionAmount, SRC.TotalCWOPT, DCBIL.WrittenOffAmount
	from SRC   
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}..WorkDCBILCommissionCWOTax DCBIL
	on DCBIL.PolicyReference=SRC.pol_num and DCBIL.InstallmentDate=SRC.PassThroughChargeTransactionEffectiveDate
	and DCBIL.PolicyTermEffectiveDate=SRC.pol_eff_date and DCBIL.PolicyTermExpirationDate=SRC.pol_exp_date
	and SRC.Num=1
),
EXP_AmountCalc AS (
	SELECT
	PassThroughChargeTransactionID AS i_PassThroughChargeTransactionID,
	PassThroughChargeTransactionAmount AS i_PassThroughChargeTransactionAmount,
	TotalCWOPT AS i_TotalCWOPT,
	WrittenOffAmount AS i_WrittenOffAmount,
	i_WrittenOffAmount-i_TotalCWOPT AS v_AmountGap,
	i_PassThroughChargeTransactionID AS o_PassThroughChargeTransactionID,
	i_PassThroughChargeTransactionAmount + v_AmountGap AS o_PassThroughChargeTransactionAmount,
	v_AmountGap AS o_AmountGap
	FROM SQ_PassThroughChargeTransaction
),
FIL_HasGap AS (
	SELECT
	o_PassThroughChargeTransactionID, 
	o_PassThroughChargeTransactionAmount, 
	o_AmountGap
	FROM EXP_AmountCalc
	WHERE o_AmountGap<>0
),
UPD_UPDATE AS (
	SELECT
	o_PassThroughChargeTransactionID AS PassThroughChargeTransactionID, 
	o_PassThroughChargeTransactionAmount AS PassThroughChargeTransactionAmount
	FROM FIL_HasGap
),
PassThroughChargeTransaction AS (
	MERGE INTO PassThroughChargeTransaction AS T
	USING UPD_UPDATE AS S
	ON T.PassThroughChargeTransactionID = S.PassThroughChargeTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.PassThroughChargeTransactionAmount = S.PassThroughChargeTransactionAmount
),
WITH
SQ_PassThroughChargeTransaction AS (
	select 
	pt.EffectiveDate,pt.LogicalIndicator,pt.LogicalDeleteFlag,pt.DuplicateSequence, pt.PassThroughChargeTransactionHashKey,
	pt.StatisticalCoverageAKID,
	max(pt.PassThroughChargeTransactionExpirationDate) over (partition by pol.pol_key) as PassThroughChargeTransactionExpirationDate,
	pt.PassThroughChargeTransactionAmount, pt.TaxPercentageRate,pt.PassThroughChargeTransactionCodeId, pt.RisklocationAKID,
	pt.PolicyAKID,pt.SupLGTLineOfInsuranceID,pt.SupSurchargeExemptID, pt.SupPassThroughChargeTypeID,pt.PolicyCoverageAKId,pt.DCTTaxCode,
	pt.OffsetOnsetCode,DCBIL.WrittenOffAmount, DCBIL.InstallmentDate, DCBIL.PolicyReference,
	pol.pol_eff_date, pol.pol_exp_date
	from PassThroughChargeTransaction pt
	inner join @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol
	on pol.pol_ak_id = pt.PolicyAKID
	and pol.crrnt_snpsht_flag = 1
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}..WorkDCBILCommissionCWOTax DCBIL
	on DCBIL.PolicyReference=pol.pol_num and DCBIL.PolicyTermEffectiveDate=pol.pol_eff_date and DCBIL.PolicyTermExpirationDate=pol.pol_exp_date
	where pt.PassThroughChargeTransactionAmount <> 0
	and DCBIL.WrittenOffAmount<>0
	And pt.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	and pt.ReasonAmendedCode!='CWO'
	order by pol.pol_key, pt.PassThroughChargeTransactionID
),
LKP_Exist AS (
	SELECT
	PolicyNumber,
	PassThroughChargeTransactionEffectiveDate,
	TotalCWO,
	pol_eff_date,
	pol_exp_date
	FROM (
		select pol.pol_num as PolicyNumber, pt.PassThroughChargeTransactionEffectiveDate as PassThroughChargeTransactionEffectiveDate, sum(pt.PassThroughChargeTransactionAmount) as TotalCWO,
		pol.pol_eff_date as pol_eff_date, pol.pol_exp_date as pol_exp_date
		from PassThroughChargeTransaction pt	
		inner join @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol	on pol.pol_ak_id = pt.PolicyAKID	and pol.crrnt_snpsht_flag = 1	
		where PT.ReasonAmendedCode='CWO'
		group by pol.pol_num, pt.PassThroughChargeTransactionEffectiveDate, pol.pol_eff_date, pol.pol_exp_date
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber,PassThroughChargeTransactionEffectiveDate,TotalCWO,pol_eff_date,pol_exp_date ORDER BY PolicyNumber) = 1
),
LKP_TotalPassThroughAmount AS (
	SELECT
	TotalPassThroughAmount,
	pol_num,
	pol_eff_date,
	pol_exp_date
	FROM (
		select pol.pol_num as pol_num, sum(pt.PassThroughChargeTransactionAmount) as TotalPassThroughAmount,
		pol.pol_eff_date as pol_eff_date, pol.pol_exp_date as pol_exp_date
		from PassThroughChargeTransaction pt
		inner join @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol
		on pol.pol_ak_id = pt.PolicyAKID
		and pol.crrnt_snpsht_flag = 1
		where pt.PassThroughChargeTransactionAmount <> 0
		And pt.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and pt.ReasonAmendedCode!='CWO'
		and POL.pol_num in (select distinct PolicyReference from @{pipeline().parameters.SOURCE_DATABASE_NAME}..WorkDCBILCommissionCWOTax DCBIL)
		group by pol.pol_num,pol.pol_eff_date, pol.pol_exp_date
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_num,pol_eff_date,pol_exp_date ORDER BY TotalPassThroughAmount) = 1
),
FIL_Exist AS (
	SELECT
	LKP_Exist.PolicyNumber AS lkp_PolicyNumber, 
	SQ_PassThroughChargeTransaction.EffectiveDate, 
	SQ_PassThroughChargeTransaction.LogicalIndicator, 
	SQ_PassThroughChargeTransaction.LogicalDeleteFlag, 
	SQ_PassThroughChargeTransaction.DuplicateSequence, 
	SQ_PassThroughChargeTransaction.PassThroughChargeTransactionHashKey, 
	SQ_PassThroughChargeTransaction.StatisticalCoverageAKID, 
	SQ_PassThroughChargeTransaction.PassThroughChargeTransactionExpirationDate, 
	SQ_PassThroughChargeTransaction.PassThroughChargeTransactionAmount, 
	SQ_PassThroughChargeTransaction.TaxPercentageRate, 
	SQ_PassThroughChargeTransaction.PassThroughChargeTransactionCodeId, 
	SQ_PassThroughChargeTransaction.RisklocationAKID, 
	SQ_PassThroughChargeTransaction.PolicyAKID, 
	SQ_PassThroughChargeTransaction.SupLGTLineOfInsuranceID, 
	SQ_PassThroughChargeTransaction.SupSurchargeExemptID, 
	SQ_PassThroughChargeTransaction.SupPassThroughChargeTypeID, 
	SQ_PassThroughChargeTransaction.PolicyCoverageAKId, 
	SQ_PassThroughChargeTransaction.DCTTaxCode, 
	SQ_PassThroughChargeTransaction.OffsetOnsetCode, 
	LKP_TotalPassThroughAmount.TotalPassThroughAmount, 
	SQ_PassThroughChargeTransaction.WrittenOffAmount, 
	SQ_PassThroughChargeTransaction.InstallmentDate
	FROM SQ_PassThroughChargeTransaction
	LEFT JOIN LKP_Exist
	ON LKP_Exist.PolicyNumber = SQ_PassThroughChargeTransaction.PolicyReference AND LKP_Exist.PassThroughChargeTransactionEffectiveDate = SQ_PassThroughChargeTransaction.InstallmentDate AND LKP_Exist.TotalCWO = SQ_PassThroughChargeTransaction.WrittenOffAmount AND LKP_Exist.pol_eff_date = SQ_PassThroughChargeTransaction.pol_eff_date AND LKP_Exist.pol_exp_date = SQ_PassThroughChargeTransaction.pol_exp_date
	LEFT JOIN LKP_TotalPassThroughAmount
	ON LKP_TotalPassThroughAmount.pol_num = SQ_PassThroughChargeTransaction.PolicyReference AND LKP_TotalPassThroughAmount.pol_eff_date = SQ_PassThroughChargeTransaction.pol_eff_date AND LKP_TotalPassThroughAmount.pol_exp_date = SQ_PassThroughChargeTransaction.pol_exp_date
	WHERE ISNULL(lkp_PolicyNumber)
),
SEQ_PassThroughChargeTransactionAKID AS (
	CREATE SEQUENCE SEQ_PassThroughChargeTransactionAKID
	START = 0
	INCREMENT = 1;
),
EXP_AmountCalc AS (
	SELECT
	EffectiveDate AS i_EffectiveDate,
	LogicalIndicator AS i_LogicalIndicator,
	LogicalDeleteFlag AS i_LogicalDeleteFlag,
	DuplicateSequence AS i_DuplicateSequence,
	PassThroughChargeTransactionHashKey AS i_PassThroughChargeTransactionHashKey,
	StatisticalCoverageAKID AS i_StatisticalCoverageAKID,
	PassThroughChargeTransactionExpirationDate AS i_PassThroughChargeTransactionExpirationDate,
	PassThroughChargeTransactionAmount AS i_PassThroughChargeTransactionAmount,
	TaxPercentageRate AS i_TaxPercentageRate,
	PassThroughChargeTransactionCodeId AS i_PassThroughChargeTransactionCodeId,
	RisklocationAKID AS i_RisklocationAKID,
	PolicyAKID AS i_PolicyAKID,
	SupLGTLineOfInsuranceID AS i_SupLGTLineOfInsuranceID,
	SupSurchargeExemptID AS i_SupSurchargeExemptID,
	SupPassThroughChargeTypeID AS i_SupPassThroughChargeTypeID,
	PolicyCoverageAKId AS i_PolicyCoverageAKId,
	DCTTaxCode AS i_DCTTaxCode,
	OffsetOnsetCode AS i_OffsetOnsetCode,
	TotalPassThroughAmount AS i_TotalPassThroughAmount,
	WrittenOffAmount AS i_WrittenOffAmount,
	InstallmentDate AS i_InstallmentDate,
	SEQ_PassThroughChargeTransactionAKID.NEXTVAL AS i_NEXTVAL,
	-- *INF*: IIF(i_TotalPassThroughAmount=0,0,i_PassThroughChargeTransactionAmount/i_TotalPassThroughAmount)
	IFF(i_TotalPassThroughAmount = 0,
		0,
		i_PassThroughChargeTransactionAmount / i_TotalPassThroughAmount
	) AS v_AllocationFactor,
	v_AllocationFactor*i_WrittenOffAmount AS v_CWOAmount,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	i_EffectiveDate AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_LogicalIndicator AS o_LogicalIndicator,
	-- *INF*: DECODE(TRUE,i_LogicalDeleteFlag='T','1',i_LogicalDeleteFlag='F','0','0')
	DECODE(TRUE,
		i_LogicalDeleteFlag = 'T', '1',
		i_LogicalDeleteFlag = 'F', '0',
		'0'
	) AS o_LogicalDeleteFlag,
	i_DuplicateSequence AS o_DuplicateSequence,
	i_PassThroughChargeTransactionHashKey AS o_PassThroughChargeTransactionHashKey,
	i_NEXTVAL AS o_PassThroughChargeTransactionAKID,
	i_StatisticalCoverageAKID AS o_StatisticalCoverageAKID,
	'Endorse' AS o_PassThroughChargeTransactionCode,
	i_InstallmentDate AS o_PassThroughChargeTransactionEnteredDate,
	i_InstallmentDate AS o_PassThroughChargeTransactionEffectiveDate,
	i_PassThroughChargeTransactionExpirationDate AS o_PassThroughChargeTransactionExpirationDate,
	i_InstallmentDate AS o_PassThroughChargeTransactionBookedDate,
	v_CWOAmount AS o_PassThroughChargeTransactionAmount,
	0.00 AS o_FullTermPremium,
	0.00 AS o_FullTaxAmount,
	i_TaxPercentageRate AS o_TaxPercentageRate,
	'CWO' AS o_ReasonAmendedCode,
	i_PassThroughChargeTransactionCodeId AS o_PassThroughChargeTransactionCodeId,
	i_RisklocationAKID AS o_RisklocationAKID,
	i_PolicyAKID AS o_PolicyAKID,
	i_SupLGTLineOfInsuranceID AS o_SupLGTLineOfInsuranceID,
	i_SupSurchargeExemptID AS o_SupSurchargeExemptID,
	i_SupPassThroughChargeTypeID AS o_SupPassThroughChargeTypeID,
	0.00 AS o_TotalAnnualPremiumSubjectToTax,
	i_PolicyCoverageAKId AS o_PolicyCoverageAKId,
	i_DCTTaxCode AS o_DCTTaxCode,
	i_OffsetOnsetCode AS o_OffsetOnsetCode,
	1 AS LoadSequence,
	'N/A' AS NegateRestateCode
	FROM FIL_Exist
),
PassThroughChargeTransaction AS (
	INSERT INTO PassThroughChargeTransaction
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, LogicalIndicator, LogicalDeleteFlag, DuplicateSequence, PassThroughChargeTransactionHashKey, PassThroughChargeTransactionAKID, StatisticalCoverageAKID, PassThroughChargeTransactionCode, PassThroughChargeTransactionEnteredDate, PassThroughChargeTransactionEffectiveDate, PassThroughChargeTransactionExpirationDate, PassThroughChargeTransactionBookedDate, PassThroughChargeTransactionAmount, FullTermPremium, FullTaxAmount, TaxPercentageRate, ReasonAmendedCode, PassThroughChargeTransactionCodeId, RiskLocationAKID, PolicyAKID, SupLGTLineOfInsuranceID, PolicyCoverageAKID, SupSurchargeExemptID, SupPassThroughChargeTypeID, TotalAnnualPremiumSubjectToTax, DCTTaxCode, OffsetOnsetCode, LoadSequence, NegateRestateCode)
	SELECT 
	o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	o_AuditID AS AUDITID, 
	o_EffectiveDate AS EFFECTIVEDATE, 
	o_ExpirationDate AS EXPIRATIONDATE, 
	o_SourceSystemID AS SOURCESYSTEMID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_LogicalIndicator AS LOGICALINDICATOR, 
	o_LogicalDeleteFlag AS LOGICALDELETEFLAG, 
	o_DuplicateSequence AS DUPLICATESEQUENCE, 
	o_PassThroughChargeTransactionHashKey AS PASSTHROUGHCHARGETRANSACTIONHASHKEY, 
	o_PassThroughChargeTransactionAKID AS PASSTHROUGHCHARGETRANSACTIONAKID, 
	o_StatisticalCoverageAKID AS STATISTICALCOVERAGEAKID, 
	o_PassThroughChargeTransactionCode AS PASSTHROUGHCHARGETRANSACTIONCODE, 
	o_PassThroughChargeTransactionEnteredDate AS PASSTHROUGHCHARGETRANSACTIONENTEREDDATE, 
	o_PassThroughChargeTransactionEffectiveDate AS PASSTHROUGHCHARGETRANSACTIONEFFECTIVEDATE, 
	o_PassThroughChargeTransactionExpirationDate AS PASSTHROUGHCHARGETRANSACTIONEXPIRATIONDATE, 
	o_PassThroughChargeTransactionBookedDate AS PASSTHROUGHCHARGETRANSACTIONBOOKEDDATE, 
	o_PassThroughChargeTransactionAmount AS PASSTHROUGHCHARGETRANSACTIONAMOUNT, 
	o_FullTermPremium AS FULLTERMPREMIUM, 
	o_FullTaxAmount AS FULLTAXAMOUNT, 
	o_TaxPercentageRate AS TAXPERCENTAGERATE, 
	o_ReasonAmendedCode AS REASONAMENDEDCODE, 
	o_PassThroughChargeTransactionCodeId AS PASSTHROUGHCHARGETRANSACTIONCODEID, 
	o_RisklocationAKID AS RISKLOCATIONAKID, 
	o_PolicyAKID AS POLICYAKID, 
	o_SupLGTLineOfInsuranceID AS SUPLGTLINEOFINSURANCEID, 
	o_PolicyCoverageAKId AS POLICYCOVERAGEAKID, 
	o_SupSurchargeExemptID AS SUPSURCHARGEEXEMPTID, 
	o_SupPassThroughChargeTypeID AS SUPPASSTHROUGHCHARGETYPEID, 
	o_TotalAnnualPremiumSubjectToTax AS TOTALANNUALPREMIUMSUBJECTTOTAX, 
	o_DCTTaxCode AS DCTTAXCODE, 
	o_OffsetOnsetCode AS OFFSETONSETCODE, 
	LOADSEQUENCE, 
	NEGATERESTATECODE
	FROM EXP_AmountCalc
),
WITH
SQ_DCBILCommissionAuthorizationStage AS (
	select pt.PolicyReference, pt.PolicyTermEffectiveDate,pt.PolicyTermExpirationDate, ca.AuthorizationDate,
	ca.CommissionPercent, SUM(ca.TierAmount) as TierAmount,
		SUM(ca.AuthorizedAmount) as AuthorizedAmount
	from DCBILPolicyTermStage pt    
	inner join DCBILCommissionAuthorizationStage ca      
	on pt.PolicyTermId = ca.PolicyTermId     
	where 
	ca.Activity in ('WriteOff','WriteOffReversal') 
	--and exists(select 1 from WorkControlKey where pt.PolicyReference = WorkControlKey.ControlKeyValue)
	and ca.AuthorizationDate>=SUBSTRING('@{pipeline().parameters.SELECTION_START_TS}',1,10)
	group by  pt.PolicyReference, pt.PolicyTermEffectiveDate,pt.PolicyTermExpirationDate, ca.AuthorizationDate, ca.CommissionPercent
),
EXP_DEFAULT AS (
	SELECT
	PolicyReference AS i_PolicyReference,
	AuthorizationDate AS i_AuthorizationDate,
	TierAmount AS i_CWOAmount,
	CommissionPercent AS i_CommissionPercent,
	AuthorizedAmount AS i_AuthorizedAmount,
	-- *INF*: TRUNC(@{pipeline().parameters.SELECTION_START_TS})
	TRUNC(@{pipeline().parameters.SELECTION_START_TS}) AS ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemId,
	PolicyTermEffectiveDate,
	PolicyTermExpirationDate
	FROM SQ_DCBILCommissionAuthorizationStage
),
WorkDCBILCommissionCWOClawBack AS (
	TRUNCATE TABLE WorkDCBILCommissionCWOClawBack;
	INSERT INTO WorkDCBILCommissionCWOClawBack
	(ExtractDate, SourceSystemId, AuthorizationDate, CWOAmount, CommissionPercent, AuthorizedAmount, PolicyReference, PolicyTermEffectiveDate, PolicyTermExpirationDate)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	i_AuthorizationDate AS AUTHORIZATIONDATE, 
	i_CWOAmount AS CWOAMOUNT, 
	i_CommissionPercent AS COMMISSIONPERCENT, 
	i_AuthorizedAmount AS AUTHORIZEDAMOUNT, 
	i_PolicyReference AS POLICYREFERENCE, 
	POLICYTERMEFFECTIVEDATE, 
	POLICYTERMEXPIRATIONDATE
	FROM EXP_DEFAULT
),
SQ_DCBILGeneralJounalStage AS (
	select PT.PolicyReference, pt.PolicyTermEffectiveDate,pt.PolicyTermExpirationDate, GJ.ActivityEffectiveDate, 
	sum(TransactionGrossAmount) as WriteOffAmount
	from DCBILGeneralJounalStage GJ join DCBILPolicyTermStage PT on PT.PolicyTermId=GJ.PolicyTermId
	where GJ.ActivityTypeCode in ('WO', 'RCWR') and AccountingClassCode in ('AR0','AR1') and GJ.JournalTypeCode='PREM'
	and GJ.ActivityEffectiveDate>=SUBSTRING('@{pipeline().parameters.SELECTION_START_TS}',1,10)
	and GJ.TransactionTypeCode in ('COLL', 'CWCP', 'UND')
	group by PT.PolicyReference, pt.PolicyTermEffectiveDate,pt.PolicyTermExpirationDate, GJ.ActivityEffectiveDate
),
EXP_AdditionalRule AS (
	SELECT
	PolicyReference,
	ActivityEffectiveDate AS WriteOffRequestDate,
	WriteOffAmount,
	0 AS CommissionPercent,
	PolicyTermEffectiveDate,
	PolicyTermExpirationDate
	FROM SQ_DCBILGeneralJounalStage
),
LKP_Exist AS (
	SELECT
	WorkDCBILCommissionCWOClawBackId,
	PolicyReference
	FROM (
		SELECT 
			WorkDCBILCommissionCWOClawBackId,
			PolicyReference
		FROM WorkDCBILCommissionCWOClawBack
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyReference ORDER BY WorkDCBILCommissionCWOClawBackId) = 1
),
EXP_FilterFlag AS (
	SELECT
	LKP_Exist.WorkDCBILCommissionCWOClawBackId AS lkp_WorkDCBILCommissionCWOClawBackId,
	EXP_AdditionalRule.PolicyReference,
	EXP_AdditionalRule.WriteOffRequestDate,
	EXP_AdditionalRule.WriteOffAmount,
	EXP_AdditionalRule.CommissionPercent,
	-- *INF*: IIF(ISNULL(lkp_WorkDCBILCommissionCWOClawBackId),1,0)
	IFF(lkp_WorkDCBILCommissionCWOClawBackId IS NULL, 1, 0) AS Filter_Flag,
	0 AS AuthorizedAmount,
	-- *INF*: trunc(@{pipeline().parameters.SELECTION_START_TS})
	TRUNC(@{pipeline().parameters.SELECTION_START_TS}) AS ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	EXP_AdditionalRule.PolicyTermEffectiveDate,
	EXP_AdditionalRule.PolicyTermExpirationDate
	FROM EXP_AdditionalRule
	LEFT JOIN LKP_Exist
	ON LKP_Exist.PolicyReference = EXP_AdditionalRule.PolicyReference
),
FIL_Additional AS (
	SELECT
	PolicyReference, 
	WriteOffRequestDate, 
	WriteOffAmount, 
	CommissionPercent, 
	Filter_Flag, 
	AuthorizedAmount, 
	ExtractDate, 
	SourceSystemID AS SourceSytemID, 
	PolicyTermEffectiveDate, 
	PolicyTermExpirationDate
	FROM EXP_FilterFlag
	WHERE Filter_Flag=1
),
WorkDCBILCommissionCWOClawBack_Graduated AS (
	INSERT INTO WorkDCBILCommissionCWOClawBack
	(ExtractDate, SourceSystemId, AuthorizationDate, CWOAmount, CommissionPercent, AuthorizedAmount, PolicyReference, PolicyTermEffectiveDate, PolicyTermExpirationDate)
	SELECT 
	EXTRACTDATE, 
	SourceSytemID AS SOURCESYSTEMID, 
	WriteOffRequestDate AS AUTHORIZATIONDATE, 
	WriteOffAmount AS CWOAMOUNT, 
	COMMISSIONPERCENT, 
	AUTHORIZEDAMOUNT, 
	POLICYREFERENCE, 
	POLICYTERMEFFECTIVEDATE, 
	POLICYTERMEXPIRATIONDATE
	FROM FIL_Additional
),
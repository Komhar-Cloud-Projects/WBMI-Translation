WITH
SQ_DCBILCommissionAuthorization AS (
	Set QUOTED_IDENTIFIER on
	
	With SRC as(
	select pt.PolicyTermId,pt.PolicyReference ,ca.LastUpdatedTimestamp,  ca.AuthorizedAmount,ca.TierAmount,ca.AuthorizationDate,ca.PlanId,ca.AccountId,ca.AuthorizationDateTime,
	ROW_NUMBER() over(partition by pt.PolicyReference order by ca.PolicyTermId, ca.PlanId, ca.AccountId, ca.CommissionAuthorizationId) Rownum
	from DCBILPolicyTermStage pt 
	inner join DCBILCommissionAuthorizationStage ca on pt.PolicyTermId = ca.PolicyTermId 
	inner join DCPLTPlanStage P on P.PlanId = ca.PlanId
	where AuthorizationTypeCode='POT'
	)
	select S.PolicyReference, BI.ItemEffectiveDate, S.AuthorizedAmount, S.TierAmount,  'Graduate' as UpdateType
	from 
	(select S1.PolicyTermId, S1.PolicyReference, S1.LastUpdatedTimestamp as CurrLastTS, ISNULL(S2.LastUpdatedTimestamp,'1990-01-01 00:00:00.000') as PrevLastTS,
	S1.AuthorizedAmount-ISNULL(S2.AuthorizedAmount,0) as AuthorizedAmount, 
	S1.TierAmount, S1.AuthorizationDate, S1.PlanId, S1.AccountId, S1.AuthorizationDateTime as CurrAuthTS, 
	ISNULL(S2.AuthorizationDateTime,'1990-01-01 00:00:00.000') as PrevAuthTS
	from SRC S1
	left join SRC S2 on S2.RowNum = S1.RowNum-1 and S1.PolicyTermid=S2.PolicyTermid and S1.PlanId=S2.PlanId and (S1.AccountId=S2.AccountId OR S2.AccountId is null)
	) S
	inner join  DCBILBillItemStage BI 
	on BI.PolicyTermId=S.PolicyTermId and BI.CommissionPlanId=S.PlanId and 
	(BI.AccountId=S.AccountId OR S.AccountID  is NULL)
	and BI.ReceivableTypeCode='Prem' and 
	((BI.PostedTimestamp<=S.CurrAuthTS and BI.PostedTimestamp> S.PrevAuthTS) or 
	(BI.TransferredAmount <> 0 and BI.LastUpdatedTimestamp<=S.CurrLastTS and BI.LastUpdatedTimestamp> S.PrevLastTS))
	and ItemID in 
	(select max(Bi.ItemId) from DCBILBillItemStage BI 
	where BI.PolicyTermId=S.PolicyTermId and BI.CommissionPlanId=S.PlanId and 
	(BI.AccountId=S.AccountId OR S.AccountID  is NULL)
	and BI.ReceivableTypeCode='Prem' and 
	((BI.PostedTimestamp<=S.CurrAuthTS and BI.PostedTimestamp> S.PrevAuthTS) or 
	(BI.TransferredAmount <> 0 and BI.LastUpdatedTimestamp<=S.CurrLastTS and BI.LastUpdatedTimestamp> S.PrevLastTS)))
	where S.AuthorizedAmount<>0 and BI.ItemId is not null
	and S.CurrLastTS>=SUBSTRING('@{pipeline().parameters.SELECTION_START_TS}',1,10)
	--FinalAudit
	 UNION ALL
	select pt.PolicyReference ,BI.ItemEffectiveDate, ca.AuthorizedAmount, ca.TierAmount,'FinalAudit' as UpdateType
	from DCBILPolicyTermStage pt 
	inner join DCBILCommissionAuthorizationStage ca 
	on pt.PolicyTermId = ca.PolicyTermId and pt.PolicyTermConfigCommissionScheme ='POP1' and AuthorizationTypeCode='AUTO'
	inner join DCPLTPlanStage P on P.PlanId = ca.PlanId
	inner join  DCBILBillItemStage BI 
	on BI.PolicyTermId=PT.PolicyTermId and BI.CommissionPlanId=P.PlanId and 
	(BI.AccountId=ca.AccountId OR ca.AccountID  is NULL)
	and BI.ReceivableTypeCode='Prem' and 
	((BI.PostedTimestamp<=ca.AuthorizationDateTime) or (BI.TransferredAmount <> 0 and BI.LastUpdatedTimestamp<=ca.LastUpdatedTimestamp))
	and ItemID in 
	(select max(Bi.ItemId) from DCBILBillItemStage BI 
	where BI.PolicyTermId=PT.PolicyTermId and BI.CommissionPlanId=P.PlanId and 
	(BI.AccountId=ca.AccountId OR ca.AccountID  is NULL)
	and BI.ReceivableTypeCode='Prem' and 
	((BI.PostedTimestamp<=ca.AuthorizationDateTime) or (BI.TransferredAmount <> 0 and BI.LastUpdatedTimestamp<=ca.LastUpdatedTimestamp)))
	where pt.PolicyReference in 
	(select distinct pt.PolicyReference
	from DCBILBillItemStage BI join DCBILPolicyTermStage PT on BI.PolicyTermId = PT.PolicyTermId
	where BI.ReceivableTypeCode='PREM'
	and BI.TransactionTypeCode in ('FAUD','RAUD')
	and BI.LastUpdatedTimestamp >= SUBSTRING('@{pipeline().parameters.SELECTION_START_TS}',1,10))
	--where ca.LastUpdatedTimestamp>=SUBSTRING('@{pipeline().parameters.SELECTION_START_TS}',1,10)
),
AGG_PolicyAndDate AS (
	SELECT
	PolicyReference,
	ItemEffectiveDate,
	AuthorizedAmount,
	TierAmount,
	UpdateType,
	-- *INF*: SUM(AuthorizedAmount)
	SUM(AuthorizedAmount) AS o_AuthorizedAmount,
	-- *INF*: SUM(TierAmount)
	SUM(TierAmount) AS o_TierAmount
	FROM SQ_DCBILCommissionAuthorization
	GROUP BY PolicyReference, ItemEffectiveDate, UpdateType
),
EXP_Metadata AS (
	SELECT
	PolicyReference,
	ItemEffectiveDate AS AuthorizationDate,
	o_AuthorizedAmount AS AuthorizedAmount,
	o_TierAmount AS TierAmount,
	-- *INF*: IIF(ISNULL(AuthorizedAmount),0,AuthorizedAmount)
	IFF(AuthorizedAmount IS NULL, 0, AuthorizedAmount) AS o_AuthorizedAmount,
	-- *INF*: IIF(ISNULL(TierAmount),0,TierAmount)
	IFF(TierAmount IS NULL, 0, TierAmount) AS o_TierAmount,
	UpdateType,
	-- *INF*: TRUNC(@{pipeline().parameters.SELECTION_START_TS})
	TRUNC(@{pipeline().parameters.SELECTION_START_TS}) AS ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemId
	FROM AGG_PolicyAndDate
),
WorkDCBILCommissionUpdate AS (
	TRUNCATE TABLE WorkDCBILCommissionUpdate;
	INSERT INTO WorkDCBILCommissionUpdate
	(ExtractDate, SourceSystemId, PolicyReference, AuthorizationDate, AuthorizedAmount, TierAmount, UpdateType)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	POLICYREFERENCE, 
	AUTHORIZATIONDATE, 
	o_AuthorizedAmount AS AUTHORIZEDAMOUNT, 
	o_TierAmount AS TIERAMOUNT, 
	UPDATETYPE
	FROM EXP_Metadata
),
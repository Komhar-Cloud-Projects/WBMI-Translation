WITH
LKP_TotalPremium_PerPolicy AS (
	SELECT
	TotalEDWPremium,
	PolicyNumber
	FROM (
		select pol.pol_num as PolicyNumber, sum(PremiumTransactionAmount) as TotalEDWPremium
		from PremiumTransaction pt	
		inner join RatingCoverage rc on rc.RatingCoverageAKID = pt.RatingCoverageAKId	and rc.EffectiveDate = pt.EffectiveDate	
		inner join PolicyCoverage pc	on pc.PolicyCoverageAKID = rc.PolicyCoverageAKID	and pc.CurrentSnapshotFlag = 1	
		inner join RiskLocation rl	on pc.RiskLocationAKID = rl.RiskLocationAKID	and rl.CurrentSnapshotFlag = 1	
		inner join @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol	on pol.pol_ak_id = pc.PolicyAKID	and pol.crrnt_snpsht_flag = 1	
		where pol.pol_num in (select distinct PolicyReference from @{pipeline().parameters.SOURCE_DATABASE_NAME}..WorkDCBILCommissionCWOClawBack)
		and pt.ReasonAmendedCode not in ('CWO','Claw Back')
		group by pol.pol_num
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber ORDER BY TotalEDWPremium) = 1
),
LKP_LastUpdateDate_PerPolicyAndRate AS (
	SELECT
	LastUpdatedTimestamp,
	PolicyReference,
	AuthorizationDate,
	CommissionPercent
	FROM (
		SELECT PolicyReference as PolicyReference, AuthorizationDate as AuthorizationDate, CommissionPercent  as CommissionPercent , LastUpdatedTimestamp as LastUpdatedTimestamp 
		FROM  (
		SELECT Pt.PolicyReference as PolicyReference, convert(date , ca.AuthorizationDateTime) as AuthorizationDate, ca.CommissionPercent as CommissionPercent, min(ca.LastUpdatedTimestamp) as LastUpdatedTimestamp
		 from DCBILCommissionAuthorizationStage ca
		join DCBILPolicyTermStage PT on CA.PolicyTermId=PT.PolicyTermId
		where ca.Activity in ('WriteOff', 'WriteOffReversal')
		group by Pt.PolicyReference, convert(date , ca.AuthorizationDateTime), ca.CommissionPercent
		
		union all
		
		SELECT PT.PolicyReference as PolicyReference, GJ.ActivityEffectiveDate as AuthorizationDate, 0 as CommissionPercent, min(GJ.LastUpdatedTimestamp) as LastUpdatedTimestamp
		from DCBILGeneralJounalStage GJ join DCBILPolicyTermStage PT on PT.PolicyTermId=GJ.PolicyTermId
		where GJ.ActivityTypeCode in ('WO', 'RCWR') and AccountingClassCode in ('AR0','AR1') and GJ.JournalTypeCode='PREM'
		group by PT.PolicyReference,GJ.ActivityEffectiveDate
		) S
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyReference,AuthorizationDate,CommissionPercent ORDER BY LastUpdatedTimestamp) = 1
),
LKP_LastUpdateDate_PerPolicy AS (
	SELECT
	LastUpdatedTimestamp,
	PolicyReference,
	AuthorizationDate
	FROM (
		SELECT PolicyReference as PolicyReference, AuthorizationDate as AuthorizationDate , LastUpdatedTimestamp as LastUpdatedTimestamp
		FROM  (
		SELECT Pt.PolicyReference as PolicyReference, convert(date , ca.AuthorizationDateTime) as AuthorizationDate, min(ca.LastUpdatedTimestamp) as LastUpdatedTimestamp
		 FROM DCBILCommissionAuthorizationStage ca
		INNER JOIN DCBILPolicyTermStage PT on CA.PolicyTermId=PT.PolicyTermId
		WHERE ca.Activity in ('WriteOff', 'WriteOffReversal')
		GROUP BY Pt.PolicyReference, convert(date , ca.AuthorizationDateTime)
		
		UNION ALL
		
		SELECT PT.PolicyReference as PolicyReference, GJ.ActivityEffectiveDate as AuthorizationDate, min(GJ.LastUpdatedTimestamp) as LastUpdatedTimestamp
		FROM DCBILGeneralJounalStage GJ INNER JOIN DCBILPolicyTermStage PT on PT.PolicyTermId=GJ.PolicyTermId
		WHERE GJ.ActivityTypeCode in ('WO', 'RCWR') and AccountingClassCode in ('AR0','AR1') and GJ.JournalTypeCode='PREM'
		GROUP BY PT.PolicyReference,GJ.ActivityEffectiveDate) S
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyReference,AuthorizationDate ORDER BY LastUpdatedTimestamp) = 1
),
LKP_TotalPremium_PerPolicyAndRate AS (
	SELECT
	TotalEDWPremium,
	PolicyNumber,
	AgencyActualCommissionRate
	FROM (
		select pol.pol_num as PolicyNumber, pt.AgencyActualCommissionRate as AgencyActualCommissionRate, sum(PremiumTransactionAmount) as TotalEDWPremium
		from PremiumTransaction pt	
		inner join RatingCoverage rc on rc.RatingCoverageAKID = pt.RatingCoverageAKId	and rc.EffectiveDate = pt.EffectiveDate	
		inner join PolicyCoverage pc	on pc.PolicyCoverageAKID = rc.PolicyCoverageAKID	and pc.CurrentSnapshotFlag = 1	
		inner join RiskLocation rl	on pc.RiskLocationAKID = rl.RiskLocationAKID	and rl.CurrentSnapshotFlag = 1	
		inner join @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol	on pol.pol_ak_id = pc.PolicyAKID	and pol.crrnt_snpsht_flag = 1	
		where pol.pol_num in (select distinct PolicyReference from @{pipeline().parameters.SOURCE_DATABASE_NAME}..WorkDCBILCommissionCWOClawBack)
		and pt.ReasonAmendedCode not in ('CWO','Claw Back')
		group by pol.pol_num, pt.AgencyActualCommissionRate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber,AgencyActualCommissionRate ORDER BY TotalEDWPremium) = 1
),
SQ_PremiumTransaction_DCT_CWO_ClawBack AS (
	WITH OnlyPolicyMatch as
	(select distinct pol.pol_num, DCBIL2.PolicyTermEffectiveDate, DCBIL2.PolicyTermExpirationDate
	from PremiumTransaction pt inner join RatingCoverage rc on rc.RatingCoverageAKID = pt.RatingCoverageAKId and rc.EffectiveDate = pt.EffectiveDate
	inner join PolicyCoverage pc on pc.PolicyCoverageAKID = rc.PolicyCoverageAKID and pc.CurrentSnapshotFlag = 1
	inner join @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol on pol.pol_ak_id = pc.PolicyAKID and pol.crrnt_snpsht_flag = 1
	left join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCBILCommissionCWOClawBack DCBIL on DCBIL.PolicyReference=pol.pol_num and DCBIL.CommissionPercent=PT.AgencyActualCommissionRate
	and DCBIL.PolicyTermEffectiveDate = pol.pol_eff_date and DCBIL.PolicyTermExpirationDate=pol.pol_exp_date
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCBILCommissionCWOClawBack  DCBIL2 
	on DCBIL2.PolicyReference=pol.pol_num and DCBIL2.PolicyTermEffectiveDate = pol.pol_eff_date and DCBIL2.PolicyTermExpirationDate=pol.pol_exp_date
	where pt.ReasonAmendedCode not in ('CWO','Claw Back')
	and pt.SourceSystemID = 'DCT'
	and DCBIL.WorkDCBILCommissionCWOClawBackId is null)
	select distinct pol.pol_num,
	pt.EffectiveDate,
	pt.LogicalIndicator,pt.LogicalDeleteFlag,pt.PremiumTransactionHashKey,pt.PremiumLoadSequence,
	pt.DuplicateSequence,pt.ReinsuranceCoverageAKID,pt.StatisticalCoverageAKID,pt.PremiumTransactionKey, pt.PMSFunctionCode,
	pt.PremiumTransactionExpirationDate,
	pt.PremiumTransactionAmount,
	pt.PremiumType,pt.RatingCoverageAkID, pt.DeductibleAmount,pt.ExperienceModificationFactor,pt.ExperienceModificationEffectiveDate,
	pt.PackageModificationAdjustmentFactor, pt.PackageModificationAdjustmentGroupCode,pt.IncreasedLimitFactor,pt.IncreasedLimitGroupCode,
	pt.YearBuilt,pt.AgencyActualCommissionRate,pt.BaseRate,pt.ConstructionCode,pt.StateRatingEffectiveDate,pt.IndividualRiskPremiumModification,
	pt.WindCoverageFlag,pt.DeductibleBasis,pt.ExposureBasis,
	case when DCBIL2.PolicyReference is null then DCBIL.AuthorizationDate else DCBIL2.AuthorizationDate end as AuthorizationDate,
	case when DCBIL2.PolicyReference is null then DCBIL.CWOAmount else DCBIL2.CWOAmount end as CWOAmount,
	case when DCBIL2.PolicyReference is null then 'BothMatch' else 'OnlyPolicyMatch' end as CWOType,
	case when DCBIL2.PolicyReference is null then DCBIL.AuthorizedAmount else DCBIL2.AuthorizedAmount end as AuthorizedAmount ,
	case when DCBIL2.PolicyReference is null then 'BothMatch' else 'OnlyPolicyMatch' end as CWBType,
	pol.pol_eff_date, pol.pol_exp_date, PT.TransactionCreatedUserId, PT.ServiceCentreName,
	PT.NumberOfEmployee,
	ISNULL(case when DCBIL2.PolicyReference is null then DCBIL.CWOAmount else DCBIL2.CWOAmount end,0) as CWOAmounts
	FROM PremiumTransaction pt
	inner join RatingCoverage rc on rc.RatingCoverageAKID = pt.RatingCoverageAKId and rc.EffectiveDate = pt.EffectiveDate
	inner join PolicyCoverage pc on pc.PolicyCoverageAKID = rc.PolicyCoverageAKID and pc.CurrentSnapshotFlag = 1
	inner join RiskLocation rl on pc.RiskLocationAKID = rl.RiskLocationAKID and rl.CurrentSnapshotFlag = 1
	inner join @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol on pol.pol_ak_id = pc.PolicyAKID and pol.crrnt_snpsht_flag = 1
	inner join (select distinct PolicyReference,PolicyTermEffectiveDate,PolicyTermExpirationDate from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCBILCommissionCWOClawBack ) a 
	ON a.Policyreference = pol.pol_num and a.PolicyTermEffectiveDate = pol.pol_eff_date and a.PolicyTermExpirationDate = pol.pol_exp_date
	left join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCBILCommissionCWOClawBack DCBIL on DCBIL.PolicyReference=pol.pol_num and DCBIL.CommissionPercent=PT.AgencyActualCommissionRate
	and DCBIL.PolicyTermEffectiveDate = pol.pol_eff_date and DCBIL.PolicyTermExpirationDate=pol.pol_exp_date
	left join (select PolicyReference, PolicyTermEffectiveDate, PolicyTermExpirationDate, AuthorizationDate, sum(CWOAmount) as CWOAmount, sum(AuthorizedAmount ) as AuthorizedAmount from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCBILCommissionCWOClawBack 
	group by PolicyReference, PolicyTermEffectiveDate, PolicyTermExpirationDate, AuthorizationDate ) DCBIL2
	on exists(select 1 from OnlyPolicyMatch where OnlyPolicyMatch.pol_num=DCBIL2.PolicyReference and OnlyPolicyMatch.PolicyTermEffectiveDate=DCBIL2.PolicyTermEffectiveDate and OnlyPolicyMatch.PolicyTermExpirationDate=DCBIL2.PolicyTermExpirationDate)
	and DCBIL2.PolicyReference=pol.pol_num and DCBIL2.PolicyTermEffectiveDate=pol.pol_eff_date and DCBIL2.PolicyTermExpirationDate=pol.pol_exp_date
	where pt.ReasonAmendedCode not in ('CWO','Claw Back')
	and pt.SourceSystemID = 'DCT'
	and pt.PremiumTransactionAmount !=0
),
RTR_Split_CWO_and_ClawBack AS (
	SELECT
	pol_num,
	EffectiveDate,
	LogicalIndicator,
	LogicalDeleteFlag,
	PremiumTransactionHashKey,
	PremiumLoadSequence,
	DuplicateSequence,
	ReinsuranceCoverageAKID,
	StatisticalCoverageAKID,
	PremiumTransactionKey,
	PMSFunctionCode,
	PremiumTransactionExpirationDate,
	PremiumTransactionAmount,
	PremiumType,
	RatingCoverageAKId,
	DeductibleAmount,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	PackageModificationAdjustmentFactor,
	PackageModificationAdjustmentGroupCode,
	IncreasedLimitFactor,
	IncreasedLimitGroupCode,
	YearBuilt,
	AgencyActualCommissionRate,
	BaseRate,
	ConstructionCode,
	StateRatingEffectiveDate,
	IndividualRiskPremiumModification,
	WindCoverageFlag,
	DeductibleBasis,
	ExposureBasis,
	AuthorizationDate,
	CWOAmount,
	CWOType,
	AuthorizedAmount,
	CWBType,
	pol_eff_date,
	pol_exp_date,
	TransactionCreatedUserId,
	ServiceCentreName,
	NumberOfEmployee,
	CWOAmounts
	FROM SQ_PremiumTransaction_DCT_CWO_ClawBack
),
RTR_Split_CWO_and_ClawBack_CWO AS (SELECT * FROM RTR_Split_CWO_and_ClawBack WHERE CWOAmounts!=0),
RTR_Split_CWO_and_ClawBack_CLAWBACK AS (SELECT * FROM RTR_Split_CWO_and_ClawBack WHERE CWOAmounts!=0),
EXP_CWOAmountCal AS (
	SELECT
	pol_num AS i_pol_num,
	EffectiveDate AS i_EffectiveDate,
	LogicalIndicator AS i_LogicalIndicator,
	LogicalDeleteFlag AS i_LogicalDeleteFlag,
	PremiumTransactionHashKey AS i_PremiumTransactionHashKey,
	PremiumLoadSequence AS i_PremiumLoadSequence,
	DuplicateSequence AS i_DuplicateSequence,
	ReinsuranceCoverageAKID AS i_ReinsuranceCoverageAKID,
	StatisticalCoverageAKID AS i_StatisticalCoverageAKID,
	PremiumTransactionKey AS i_PremiumTransactionKey,
	PMSFunctionCode AS i_PMSFunctionCode,
	PremiumTransactionExpirationDate AS i_PremiumTransactionExpirationDate,
	PremiumTransactionAmount AS i_PremiumTransactionAmount,
	PremiumType AS i_PremiumType,
	RatingCoverageAKId AS i_RatingCoverageAKId,
	DeductibleAmount AS i_DeductibleAmount,
	ExperienceModificationFactor AS i_ExperienceModificationFactor,
	ExperienceModificationEffectiveDate AS i_ExperienceModificationEffectiveDate,
	PackageModificationAdjustmentFactor AS i_PackageModificationAdjustmentFactor,
	PackageModificationAdjustmentGroupCode AS i_PackageModificationAdjustmentGroupCode,
	IncreasedLimitFactor AS i_IncreasedLimitFactor,
	IncreasedLimitGroupCode AS i_IncreasedLimitGroupCode,
	YearBuilt AS i_YearBuilt,
	AgencyActualCommissionRate AS i_AgencyActualCommissionRate,
	BaseRate AS i_BaseRate,
	ConstructionCode AS i_ConstructionCode,
	StateRatingEffectiveDate AS i_StateRatingEffectiveDate,
	IndividualRiskPremiumModification AS i_IndividualRiskPremiumModification,
	WindCoverageFlag AS i_WindCoverageFlag,
	DeductibleBasis AS i_DeductibleBasis,
	ExposureBasis AS i_ExposureBasis,
	AuthorizationDate AS i_AuthorizationDate,
	CWOAmount AS i_CWOAmount,
	CWOType AS i_CWOType,
	pol_eff_date AS i_pol_eff_date,
	pol_exp_date AS i_pol_exp_date,
	-- *INF*: DECODE(TRUE,
	-- i_CWOType='BothMatch', :LKP.LKP_TOTALPREMIUM_PERPOLICYANDRATE(i_pol_num,i_AgencyActualCommissionRate),
	-- i_CWOType='OnlyPolicyMatch',
	-- :LKP.LKP_TOTALPREMIUM_PERPOLICY(i_pol_num),
	-- 0
	-- )
	DECODE(TRUE,
		i_CWOType = 'BothMatch', LKP_TOTALPREMIUM_PERPOLICYANDRATE_i_pol_num_i_AgencyActualCommissionRate.TotalEDWPremium,
		i_CWOType = 'OnlyPolicyMatch', LKP_TOTALPREMIUM_PERPOLICY_i_pol_num.TotalEDWPremium,
		0) AS v_TotalPremium,
	-- *INF*: IIF(v_TotalPremium=0 or ISNULL(v_TotalPremium), 0, i_PremiumTransactionAmount/v_TotalPremium)
	IFF(v_TotalPremium = 0 OR v_TotalPremium IS NULL, 0, i_PremiumTransactionAmount / v_TotalPremium) AS v_AllocationFactor,
	i_CWOAmount*v_AllocationFactor AS v_CWOAmount,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: i_EffectiveDate
	-- --i_AuthorizationDate
	i_EffectiveDate AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_LogicalIndicator AS o_LogicalIndicator,
	-- *INF*: DECODE(TRUE,i_LogicalDeleteFlag='T','1',i_LogicalDeleteFlag='F','0','0')
	DECODE(TRUE,
		i_LogicalDeleteFlag = 'T', '1',
		i_LogicalDeleteFlag = 'F', '0',
		'0') AS o_LogicalDeleteFlag,
	i_PremiumTransactionHashKey AS o_PremiumTransactionHashKey,
	i_PremiumLoadSequence AS o_PremiumLoadSequence,
	i_DuplicateSequence AS o_DuplicateSequence,
	i_ReinsuranceCoverageAKID AS o_ReinsuranceCoverageAKID,
	i_StatisticalCoverageAKID AS o_StatisticalCoverageAKID,
	i_PremiumTransactionKey AS o_PremiumTransactionKey,
	i_PMSFunctionCode AS o_PMSFunctionCode,
	'Endorse' AS o_PremiumTransactionCode,
	-- *INF*: DECODE(TRUE,
	-- i_CWOType='BothMatch', :LKP.LKP_LASTUPDATEDATE_PERPOLICYANDRATE(i_pol_num,i_AuthorizationDate,i_AgencyActualCommissionRate),
	-- i_CWOType='OnlyPolicyMatch',
	-- :LKP.LKP_LASTUPDATEDATE_PERPOLICY(i_pol_num,i_AuthorizationDate),
	-- TO_DATE('18000101','YYYYMMDD')
	-- )
	DECODE(TRUE,
		i_CWOType = 'BothMatch', LKP_LASTUPDATEDATE_PERPOLICYANDRATE_i_pol_num_i_AuthorizationDate_i_AgencyActualCommissionRate.LastUpdatedTimestamp,
		i_CWOType = 'OnlyPolicyMatch', LKP_LASTUPDATEDATE_PERPOLICY_i_pol_num_i_AuthorizationDate.LastUpdatedTimestamp,
		TO_DATE('18000101', 'YYYYMMDD')) AS o_PremiumTransactionEnteredDate,
	i_AuthorizationDate AS o_PremiumTransactionEffectiveDate,
	-- *INF*: ADD_TO_DATE(i_AuthorizationDate,'DD',1)
	ADD_TO_DATE(i_AuthorizationDate, 'DD', 1) AS o_PremiumTransactionExpirationDate,
	-- *INF*: TRUNC(i_AuthorizationDate,'MM')
	TRUNC(i_AuthorizationDate, 'MM') AS o_PremiumTransactionBookedDate,
	v_CWOAmount AS o_PremiumTransactionAmount,
	0.00 AS o_FullTermPremium,
	i_PremiumType AS o_PremiumType,
	'CWO' AS o_ReasonAmendedCode,
	-- *INF*: 'N/A'
	-- --'Offset'
	'N/A' AS o_OffsetOnsetCode,
	28 AS o_sup_premium_transaction_id,
	i_RatingCoverageAKId AS o_RatingCoverageAKId,
	i_DeductibleAmount AS o_DeductibleAmount,
	i_ExperienceModificationFactor AS o_ExperienceModificationFactor,
	i_ExperienceModificationEffectiveDate AS o_ExperienceModificationEffectiveDate,
	i_PackageModificationAdjustmentFactor AS o_PackageModificationAdjustmentFactor,
	i_PackageModificationAdjustmentGroupCode AS o_PackageModificationAdjustmentGroupCode,
	i_IncreasedLimitFactor AS o_IncreasedLimitFactor,
	i_IncreasedLimitGroupCode AS o_IncreasedLimitGroupCode,
	i_YearBuilt AS o_YearBuilt,
	i_AgencyActualCommissionRate AS o_AgencyActualCommissionRate,
	i_BaseRate AS o_BaseRate,
	i_ConstructionCode AS o_ConstructionCode,
	i_StateRatingEffectiveDate AS o_StateRatingEffectiveDate,
	i_IndividualRiskPremiumModification AS o_IndividualRiskPremiumModification,
	-- *INF*: DECODE(TRUE, i_WindCoverageFlag='T','1',
	-- i_WindCoverageFlag='F','0',
	-- '0')
	DECODE(TRUE,
		i_WindCoverageFlag = 'T', '1',
		i_WindCoverageFlag = 'F', '0',
		'0') AS o_WindCoverageFlag,
	i_DeductibleBasis AS o_DeductibleBasis,
	i_ExposureBasis AS o_ExposureBasis,
	i_pol_eff_date AS o_pol_eff_date,
	i_pol_exp_date AS o_pol_exp_date,
	TransactionCreatedUserId,
	ServiceCentreName,
	NumberOfEmployee AS NumberOfEmployee1
	FROM RTR_Split_CWO_and_ClawBack_CWO
	LEFT JOIN LKP_TOTALPREMIUM_PERPOLICYANDRATE LKP_TOTALPREMIUM_PERPOLICYANDRATE_i_pol_num_i_AgencyActualCommissionRate
	ON LKP_TOTALPREMIUM_PERPOLICYANDRATE_i_pol_num_i_AgencyActualCommissionRate.PolicyNumber = i_pol_num
	AND LKP_TOTALPREMIUM_PERPOLICYANDRATE_i_pol_num_i_AgencyActualCommissionRate.AgencyActualCommissionRate = i_AgencyActualCommissionRate

	LEFT JOIN LKP_TOTALPREMIUM_PERPOLICY LKP_TOTALPREMIUM_PERPOLICY_i_pol_num
	ON LKP_TOTALPREMIUM_PERPOLICY_i_pol_num.PolicyNumber = i_pol_num

	LEFT JOIN LKP_LASTUPDATEDATE_PERPOLICYANDRATE LKP_LASTUPDATEDATE_PERPOLICYANDRATE_i_pol_num_i_AuthorizationDate_i_AgencyActualCommissionRate
	ON LKP_LASTUPDATEDATE_PERPOLICYANDRATE_i_pol_num_i_AuthorizationDate_i_AgencyActualCommissionRate.PolicyReference = i_pol_num
	AND LKP_LASTUPDATEDATE_PERPOLICYANDRATE_i_pol_num_i_AuthorizationDate_i_AgencyActualCommissionRate.AuthorizationDate = i_AuthorizationDate
	AND LKP_LASTUPDATEDATE_PERPOLICYANDRATE_i_pol_num_i_AuthorizationDate_i_AgencyActualCommissionRate.CommissionPercent = i_AgencyActualCommissionRate

	LEFT JOIN LKP_LASTUPDATEDATE_PERPOLICY LKP_LASTUPDATEDATE_PERPOLICY_i_pol_num_i_AuthorizationDate
	ON LKP_LASTUPDATEDATE_PERPOLICY_i_pol_num_i_AuthorizationDate.PolicyReference = i_pol_num
	AND LKP_LASTUPDATEDATE_PERPOLICY_i_pol_num_i_AuthorizationDate.AuthorizationDate = i_AuthorizationDate

),
EXP_ClawBackAmountCal AS (
	SELECT
	pol_num AS i_pol_num,
	EffectiveDate AS i_EffectiveDate,
	LogicalIndicator AS i_LogicalIndicator,
	LogicalDeleteFlag AS i_LogicalDeleteFlag,
	PremiumTransactionHashKey AS i_PremiumTransactionHashKey,
	PremiumLoadSequence AS i_PremiumLoadSequence,
	DuplicateSequence AS i_DuplicateSequence,
	ReinsuranceCoverageAKID AS i_ReinsuranceCoverageAKID,
	StatisticalCoverageAKID AS i_StatisticalCoverageAKID,
	PremiumTransactionKey AS i_PremiumTransactionKey,
	PMSFunctionCode AS i_PMSFunctionCode,
	PremiumTransactionExpirationDate AS i_PremiumTransactionExpirationDate,
	PremiumTransactionAmount AS i_PremiumTransactionAmount,
	PremiumType AS i_PremiumType,
	RatingCoverageAKId AS i_RatingCoverageAKId,
	DeductibleAmount AS i_DeductibleAmount,
	ExperienceModificationFactor AS i_ExperienceModificationFactor,
	ExperienceModificationEffectiveDate AS i_ExperienceModificationEffectiveDate,
	PackageModificationAdjustmentFactor AS i_PackageModificationAdjustmentFactor,
	PackageModificationAdjustmentGroupCode AS i_PackageModificationAdjustmentGroupCode,
	IncreasedLimitFactor AS i_IncreasedLimitFactor,
	IncreasedLimitGroupCode AS i_IncreasedLimitGroupCode,
	YearBuilt AS i_YearBuilt,
	AgencyActualCommissionRate AS i_AgencyActualCommissionRate,
	BaseRate AS i_BaseRate,
	ConstructionCode AS i_ConstructionCode,
	StateRatingEffectiveDate AS i_StateRatingEffectiveDate,
	IndividualRiskPremiumModification AS i_IndividualRiskPremiumModification,
	WindCoverageFlag AS i_WindCoverageFlag,
	DeductibleBasis AS i_DeductibleBasis,
	ExposureBasis AS i_ExposureBasis,
	AuthorizationDate AS i_AuthorizationDate,
	AuthorizedAmount AS i_AuthorizedAmount,
	CWBType AS i_CWBType,
	pol_eff_date AS i_pol_eff_date,
	pol_exp_date AS i_pol_exp_date,
	-- *INF*: DECODE(TRUE,
	-- i_CWBType='BothMatch', :LKP.LKP_TOTALPREMIUM_PERPOLICYANDRATE(i_pol_num,i_AgencyActualCommissionRate),
	-- i_CWBType='OnlyPolicyMatch',
	-- :LKP.LKP_TOTALPREMIUM_PERPOLICY(i_pol_num),
	-- 0
	-- )
	DECODE(TRUE,
		i_CWBType = 'BothMatch', LKP_TOTALPREMIUM_PERPOLICYANDRATE_i_pol_num_i_AgencyActualCommissionRate.TotalEDWPremium,
		i_CWBType = 'OnlyPolicyMatch', LKP_TOTALPREMIUM_PERPOLICY_i_pol_num.TotalEDWPremium,
		0) AS v_TotalPremium,
	-- *INF*: IIF(v_TotalPremium=0 or ISNULL(v_TotalPremium), 0, i_PremiumTransactionAmount/v_TotalPremium)
	IFF(v_TotalPremium = 0 OR v_TotalPremium IS NULL, 0, i_PremiumTransactionAmount / v_TotalPremium) AS v_AllocationFactor,
	i_AuthorizedAmount*v_AllocationFactor AS v_ClawBackAmount,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: i_EffectiveDate
	-- --i_AuthorizationDate
	i_EffectiveDate AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_LogicalIndicator AS o_LogicalIndicator,
	-- *INF*: DECODE(TRUE,i_LogicalDeleteFlag='T','1',i_LogicalDeleteFlag='F','0','0')
	DECODE(TRUE,
		i_LogicalDeleteFlag = 'T', '1',
		i_LogicalDeleteFlag = 'F', '0',
		'0') AS o_LogicalDeleteFlag,
	i_PremiumTransactionHashKey AS o_PremiumTransactionHashKey,
	i_PremiumLoadSequence AS o_PremiumLoadSequence,
	i_DuplicateSequence AS o_DuplicateSequence,
	i_ReinsuranceCoverageAKID AS o_ReinsuranceCoverageAKID,
	i_StatisticalCoverageAKID AS o_StatisticalCoverageAKID,
	i_PremiumTransactionKey AS o_PremiumTransactionKey,
	i_PMSFunctionCode AS o_PMSFunctionCode,
	'Endorse' AS o_PremiumTransactionCode,
	-- *INF*: DECODE(TRUE,
	-- i_CWBType='BothMatch', :LKP.LKP_LASTUPDATEDATE_PERPOLICYANDRATE(i_pol_num,i_AuthorizationDate,i_AgencyActualCommissionRate),
	-- i_CWBType='OnlyPolicyMatch',
	-- :LKP.LKP_LASTUPDATEDATE_PERPOLICY(i_pol_num,i_AuthorizationDate),
	-- TO_DATE('18000101','YYYYMMDD')
	-- )
	DECODE(TRUE,
		i_CWBType = 'BothMatch', LKP_LASTUPDATEDATE_PERPOLICYANDRATE_i_pol_num_i_AuthorizationDate_i_AgencyActualCommissionRate.LastUpdatedTimestamp,
		i_CWBType = 'OnlyPolicyMatch', LKP_LASTUPDATEDATE_PERPOLICY_i_pol_num_i_AuthorizationDate.LastUpdatedTimestamp,
		TO_DATE('18000101', 'YYYYMMDD')) AS o_PremiumTransactionEnteredDate,
	i_AuthorizationDate AS o_PremiumTransactionEffectiveDate,
	-- *INF*: ADD_TO_DATE(i_AuthorizationDate,'DD',1)
	ADD_TO_DATE(i_AuthorizationDate, 'DD', 1) AS o_PremiumTransactionExpirationDate,
	-- *INF*: TRUNC(i_AuthorizationDate,'MM')
	TRUNC(i_AuthorizationDate, 'MM') AS o_PremiumTransactionBookedDate,
	v_ClawBackAmount AS o_PremiumTransactionAmount,
	0.00 AS o_FullTermPremium,
	i_PremiumType AS o_PremiumType,
	'Claw Back' AS o_ReasonAmendedCode,
	-- *INF*: 'N/A'
	-- --'Offset'
	'N/A' AS o_OffsetOnsetCode,
	28 AS o_sup_premium_transaction_id,
	i_RatingCoverageAKId AS o_RatingCoverageAKId,
	i_DeductibleAmount AS o_DeductibleAmount,
	i_ExperienceModificationFactor AS o_ExperienceModificationFactor,
	i_ExperienceModificationEffectiveDate AS o_ExperienceModificationEffectiveDate,
	i_PackageModificationAdjustmentFactor AS o_PackageModificationAdjustmentFactor,
	i_PackageModificationAdjustmentGroupCode AS o_PackageModificationAdjustmentGroupCode,
	i_IncreasedLimitFactor AS o_IncreasedLimitFactor,
	i_IncreasedLimitGroupCode AS o_IncreasedLimitGroupCode,
	i_YearBuilt AS o_YearBuilt,
	i_AgencyActualCommissionRate AS o_AgencyActualCommissionRate,
	i_BaseRate AS o_BaseRate,
	i_ConstructionCode AS o_ConstructionCode,
	i_StateRatingEffectiveDate AS o_StateRatingEffectiveDate,
	i_IndividualRiskPremiumModification AS o_IndividualRiskPremiumModification,
	-- *INF*: DECODE(TRUE, i_WindCoverageFlag='T','1',
	-- i_WindCoverageFlag='F','0',
	-- '0')
	DECODE(TRUE,
		i_WindCoverageFlag = 'T', '1',
		i_WindCoverageFlag = 'F', '0',
		'0') AS o_WindCoverageFlag,
	i_DeductibleBasis AS o_DeductibleBasis,
	i_ExposureBasis AS o_ExposureBasis,
	i_pol_eff_date AS o_pol_eff_date,
	i_pol_exp_date AS o_pol_exp_date,
	TransactionCreatedUserId,
	ServiceCentreName,
	NumberOfEmployee AS NumberOfEmployee3
	FROM RTR_Split_CWO_and_ClawBack_CLAWBACK
	LEFT JOIN LKP_TOTALPREMIUM_PERPOLICYANDRATE LKP_TOTALPREMIUM_PERPOLICYANDRATE_i_pol_num_i_AgencyActualCommissionRate
	ON LKP_TOTALPREMIUM_PERPOLICYANDRATE_i_pol_num_i_AgencyActualCommissionRate.PolicyNumber = i_pol_num
	AND LKP_TOTALPREMIUM_PERPOLICYANDRATE_i_pol_num_i_AgencyActualCommissionRate.AgencyActualCommissionRate = i_AgencyActualCommissionRate

	LEFT JOIN LKP_TOTALPREMIUM_PERPOLICY LKP_TOTALPREMIUM_PERPOLICY_i_pol_num
	ON LKP_TOTALPREMIUM_PERPOLICY_i_pol_num.PolicyNumber = i_pol_num

	LEFT JOIN LKP_LASTUPDATEDATE_PERPOLICYANDRATE LKP_LASTUPDATEDATE_PERPOLICYANDRATE_i_pol_num_i_AuthorizationDate_i_AgencyActualCommissionRate
	ON LKP_LASTUPDATEDATE_PERPOLICYANDRATE_i_pol_num_i_AuthorizationDate_i_AgencyActualCommissionRate.PolicyReference = i_pol_num
	AND LKP_LASTUPDATEDATE_PERPOLICYANDRATE_i_pol_num_i_AuthorizationDate_i_AgencyActualCommissionRate.AuthorizationDate = i_AuthorizationDate
	AND LKP_LASTUPDATEDATE_PERPOLICYANDRATE_i_pol_num_i_AuthorizationDate_i_AgencyActualCommissionRate.CommissionPercent = i_AgencyActualCommissionRate

	LEFT JOIN LKP_LASTUPDATEDATE_PERPOLICY LKP_LASTUPDATEDATE_PERPOLICY_i_pol_num_i_AuthorizationDate
	ON LKP_LASTUPDATEDATE_PERPOLICY_i_pol_num_i_AuthorizationDate.PolicyReference = i_pol_num
	AND LKP_LASTUPDATEDATE_PERPOLICY_i_pol_num_i_AuthorizationDate.AuthorizationDate = i_AuthorizationDate

),
LKP_ClawBack_Policy AS (
	SELECT
	AuthorizedAmount,
	i_pol_num,
	i_AuthorizationDate,
	PolicyReference,
	AuthorizationDate
	FROM (
		select PolicyReference as PolicyReference,
		AuthorizationDate as AuthorizationDate,
		SUM(AuthorizedAmount) as AuthorizedAmount
		from WorkDCBILCommissionCWOClawBack
		group by PolicyReference ,AuthorizationDate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyReference,AuthorizationDate ORDER BY AuthorizedAmount) = 1
),
LKP_CWBExist AS (
	SELECT
	pol_num,
	i_AuthorizationDate,
	i_AuthorizedAmount,
	i_pol_eff_date,
	i_pol_exp_date,
	PremiumTransactionEffectiveDate,
	TotalCWBAmount,
	pol_eff_date,
	pol_exp_date
	FROM (
		select pol.pol_num as pol_num, pt.PremiumTransactionEffectiveDate as PremiumTransactionEffectiveDate, sum(PremiumTransactionAmount) as TotalCWBAmount, pol.pol_eff_date as pol_eff_date, pol.pol_exp_date as pol_exp_date
		from PremiumTransaction pt	
		inner join RatingCoverage rc on rc.RatingCoverageAKID = pt.RatingCoverageAKId	and rc.EffectiveDate = pt.EffectiveDate	
		inner join PolicyCoverage pc	on pc.PolicyCoverageAKID = rc.PolicyCoverageAKID	and pc.CurrentSnapshotFlag = 1	
		inner join RiskLocation rl	on pc.RiskLocationAKID = rl.RiskLocationAKID	and rl.CurrentSnapshotFlag = 1	
		inner join @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol	on pol.pol_ak_id = pc.PolicyAKID	and pol.crrnt_snpsht_flag = 1
		where pt.ReasonAmendedCode='Claw Back'
		group by pol.pol_num, pt.PremiumTransactionEffectiveDate, pol.pol_eff_date ,pol.pol_exp_date
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_num,PremiumTransactionEffectiveDate,TotalCWBAmount,pol_eff_date,pol_exp_date ORDER BY pol_num) = 1
),
LKP_CWO_Policy AS (
	SELECT
	CWOAmount,
	i_pol_num,
	i_AuthorizationDate,
	PolicyReference,
	AuthorizationDate
	FROM (
		select PolicyReference as PolicyReference,
		AuthorizationDate as AuthorizationDate,
		SUM(CWOAmount) as CWOAmount
		from WorkDCBILCommissionCWOClawBack
		group by PolicyReference, AuthorizationDate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyReference,AuthorizationDate ORDER BY CWOAmount) = 1
),
LKP_CWOExist AS (
	SELECT
	pol_num,
	PremiumTransactionEffectiveDate,
	TotalCWOAmount,
	pol_eff_date,
	pol_exp_date
	FROM (
		select pol.pol_num as pol_num , pt.PremiumTransactionEffectiveDate as PremiumTransactionEffectiveDate, sum(PremiumTransactionAmount) as TotalCWOAmount, pol.pol_eff_date as pol_eff_date, pol.pol_exp_date as pol_exp_date
		from PremiumTransaction pt	
		inner join RatingCoverage rc on rc.RatingCoverageAKID = pt.RatingCoverageAKId	and rc.EffectiveDate = pt.EffectiveDate	
		inner join PolicyCoverage pc	on pc.PolicyCoverageAKID = rc.PolicyCoverageAKID	and pc.CurrentSnapshotFlag = 1	
		inner join RiskLocation rl	on pc.RiskLocationAKID = rl.RiskLocationAKID	and rl.CurrentSnapshotFlag = 1	
		inner join @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol	on pol.pol_ak_id = pc.PolicyAKID	and pol.crrnt_snpsht_flag = 1
		where pt.ReasonAmendedCode='CWO'
		group by pol.pol_num, pt.PremiumTransactionEffectiveDate,pol.pol_eff_date, pol.pol_exp_date
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_num,PremiumTransactionEffectiveDate,TotalCWOAmount,pol_eff_date,pol_exp_date ORDER BY pol_num) = 1
),
Union AS (
	SELECT pol_num AS lkp_PolicyNumber, o_CurrentSnapshotFlag AS CurrentSnapshotFlag, o_AuditID AS AuditID, o_EffectiveDate AS EffectiveDate, o_ExpirationDate AS ExpirationDate, o_SourceSystemID AS SourceSystemID, o_CreatedDate AS CreatedDate, o_ModifiedDate AS ModifiedDate, o_LogicalIndicator AS LogicalIndicator, o_LogicalDeleteFlag AS LogicalDeleteFlag, o_PremiumTransactionHashKey AS PremiumTransactionHashKey, o_PremiumLoadSequence AS PremiumLoadSequence, o_DuplicateSequence AS DuplicateSequence, o_ReinsuranceCoverageAKID AS ReinsuranceCoverageAKID, o_StatisticalCoverageAKID AS StatisticalCoverageAKID, o_PremiumTransactionKey AS PremiumTransactionKey, o_PMSFunctionCode AS PMSFunctionCode, o_PremiumTransactionCode AS PremiumTransactionCode, o_PremiumTransactionEnteredDate AS PremiumTransactionEnteredDate, o_PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate, o_PremiumTransactionExpirationDate AS PremiumTransactionExpirationDate, o_PremiumTransactionBookedDate AS PremiumTransactionBookedDate, o_PremiumTransactionAmount AS PremiumTransactionAmount, o_FullTermPremium AS FullTermPremium, o_PremiumType AS PremiumType, o_ReasonAmendedCode AS ReasonAmendedCode, o_OffsetOnsetCode AS OffsetOnsetCode, o_sup_premium_transaction_id AS sup_premium_transaction_id, o_RatingCoverageAKId AS RatingCoverageAKId, o_DeductibleAmount AS DeductibleAmount, o_ExperienceModificationFactor AS ExperienceModificationFactor, o_ExperienceModificationEffectiveDate AS ExperienceModificationEffectiveDate, o_PackageModificationAdjustmentFactor AS PackageModificationAdjustmentFactor, o_PackageModificationAdjustmentGroupCode AS PackageModificationAdjustmentGroupCode, o_IncreasedLimitFactor AS IncreasedLimitFactor, o_IncreasedLimitGroupCode AS IncreasedLimitGroupCode, o_YearBuilt AS YearBuilt, o_AgencyActualCommissionRate AS AgencyActualCommissionRate, o_BaseRate AS BaseRate, o_ConstructionCode AS ConstructionCode, o_StateRatingEffectiveDate AS StateRatingEffectiveDate, o_IndividualRiskPremiumModification AS IndividualRiskPremiumModification, o_WindCoverageFlag AS WindCoverageFlag, o_DeductibleBasis AS DeductibleBasis, o_ExposureBasis AS ExposureBasis, TransactionCreatedUserId, ServiceCentreName, NumberOfEmployee1 AS NumberOfEmployee3
	FROM EXP_CWOAmountCal
	-- Manually join with LKP_CWOExist
	UNION
	SELECT pol_num AS lkp_PolicyNumber, o_CurrentSnapshotFlag AS CurrentSnapshotFlag, o_AuditID AS AuditID, o_EffectiveDate AS EffectiveDate, o_ExpirationDate AS ExpirationDate, o_SourceSystemID AS SourceSystemID, o_CreatedDate AS CreatedDate, o_ModifiedDate AS ModifiedDate, o_LogicalIndicator AS LogicalIndicator, o_LogicalDeleteFlag AS LogicalDeleteFlag, o_PremiumTransactionHashKey AS PremiumTransactionHashKey, o_PremiumLoadSequence AS PremiumLoadSequence, o_DuplicateSequence AS DuplicateSequence, o_ReinsuranceCoverageAKID AS ReinsuranceCoverageAKID, o_StatisticalCoverageAKID AS StatisticalCoverageAKID, o_PremiumTransactionKey AS PremiumTransactionKey, o_PMSFunctionCode AS PMSFunctionCode, o_PremiumTransactionCode AS PremiumTransactionCode, o_PremiumTransactionEnteredDate AS PremiumTransactionEnteredDate, o_PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate, o_PremiumTransactionExpirationDate AS PremiumTransactionExpirationDate, o_PremiumTransactionBookedDate AS PremiumTransactionBookedDate, o_PremiumTransactionAmount AS PremiumTransactionAmount, o_FullTermPremium AS FullTermPremium, o_PremiumType AS PremiumType, o_ReasonAmendedCode AS ReasonAmendedCode, o_OffsetOnsetCode AS OffsetOnsetCode, o_sup_premium_transaction_id AS sup_premium_transaction_id, o_RatingCoverageAKId AS RatingCoverageAKId, o_DeductibleAmount AS DeductibleAmount, o_ExperienceModificationFactor AS ExperienceModificationFactor, o_ExperienceModificationEffectiveDate AS ExperienceModificationEffectiveDate, o_PackageModificationAdjustmentFactor AS PackageModificationAdjustmentFactor, o_PackageModificationAdjustmentGroupCode AS PackageModificationAdjustmentGroupCode, o_IncreasedLimitFactor AS IncreasedLimitFactor, o_IncreasedLimitGroupCode AS IncreasedLimitGroupCode, o_YearBuilt AS YearBuilt, o_AgencyActualCommissionRate AS AgencyActualCommissionRate, o_BaseRate AS BaseRate, o_ConstructionCode AS ConstructionCode, o_StateRatingEffectiveDate AS StateRatingEffectiveDate, o_IndividualRiskPremiumModification AS IndividualRiskPremiumModification, o_WindCoverageFlag AS WindCoverageFlag, o_DeductibleBasis AS DeductibleBasis, o_ExposureBasis AS ExposureBasis, TransactionCreatedUserId, ServiceCentreName, NumberOfEmployee3
	FROM EXP_ClawBackAmountCal
	-- Manually join with LKP_CWBExist
),
FIL_Exist AS (
	SELECT
	lkp_PolicyNumber, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemID, 
	CreatedDate, 
	ModifiedDate, 
	LogicalIndicator, 
	LogicalDeleteFlag, 
	PremiumTransactionHashKey, 
	PremiumLoadSequence, 
	DuplicateSequence, 
	ReinsuranceCoverageAKID, 
	StatisticalCoverageAKID, 
	PremiumTransactionKey, 
	PMSFunctionCode, 
	PremiumTransactionCode, 
	PremiumTransactionEnteredDate, 
	PremiumTransactionEffectiveDate, 
	PremiumTransactionExpirationDate, 
	PremiumTransactionBookedDate, 
	PremiumTransactionAmount, 
	FullTermPremium, 
	PremiumType, 
	ReasonAmendedCode, 
	OffsetOnsetCode, 
	sup_premium_transaction_id, 
	RatingCoverageAKId, 
	DeductibleAmount, 
	ExperienceModificationFactor, 
	ExperienceModificationEffectiveDate, 
	PackageModificationAdjustmentFactor, 
	PackageModificationAdjustmentGroupCode, 
	IncreasedLimitFactor, 
	IncreasedLimitGroupCode, 
	YearBuilt, 
	AgencyActualCommissionRate, 
	BaseRate, 
	ConstructionCode, 
	StateRatingEffectiveDate, 
	IndividualRiskPremiumModification, 
	WindCoverageFlag, 
	DeductibleBasis, 
	ExposureBasis, 
	TransactionCreatedUserId, 
	ServiceCentreName, 
	NumberOfEmployee3
	FROM Union
	WHERE ISNULL(lkp_PolicyNumber)
),
SEQ_PremiumTransactionAKID AS (
	CREATE SEQUENCE SEQ_PremiumTransactionAKID
	START = 0
	INCREMENT = 1;
),
EXP_AKID AS (
	SELECT
	SEQ_PremiumTransactionAKID.NEXTVAL,
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	LogicalIndicator,
	LogicalDeleteFlag,
	PremiumTransactionHashKey,
	PremiumLoadSequence,
	DuplicateSequence,
	ReinsuranceCoverageAKID,
	StatisticalCoverageAKID,
	PremiumTransactionKey,
	PMSFunctionCode,
	PremiumTransactionCode,
	PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	PremiumTransactionExpirationDate,
	PremiumTransactionBookedDate,
	PremiumTransactionAmount,
	FullTermPremium,
	PremiumType,
	ReasonAmendedCode,
	OffsetOnsetCode,
	sup_premium_transaction_id,
	RatingCoverageAKId,
	DeductibleAmount,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	PackageModificationAdjustmentFactor,
	PackageModificationAdjustmentGroupCode,
	IncreasedLimitFactor,
	IncreasedLimitGroupCode,
	YearBuilt,
	AgencyActualCommissionRate,
	BaseRate,
	ConstructionCode,
	StateRatingEffectiveDate,
	IndividualRiskPremiumModification,
	WindCoverageFlag,
	DeductibleBasis,
	ExposureBasis,
	TransactionCreatedUserId,
	ServiceCentreName,
	0.00 AS Exposure,
	NumberOfEmployee3,
	'N/A' AS NegateRestateCode,
	0 AS DeclaredEventFlag
	FROM FIL_Exist
),
PremiumTransaction_Insert AS (
	INSERT INTO PremiumTransaction
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, LogicalIndicator, LogicalDeleteFlag, PremiumTransactionHashKey, PremiumLoadSequence, DuplicateSequence, PremiumTransactionAKID, ReinsuranceCoverageAKID, StatisticalCoverageAKID, PremiumTransactionKey, PMSFunctionCode, PremiumTransactionCode, PremiumTransactionEnteredDate, PremiumTransactionEffectiveDate, PremiumTransactionExpirationDate, PremiumTransactionBookedDate, PremiumTransactionAmount, FullTermPremium, PremiumType, ReasonAmendedCode, OffsetOnsetCode, SupPremiumTransactionCodeId, RatingCoverageAKId, DeductibleAmount, ExperienceModificationFactor, ExperienceModificationEffectiveDate, PackageModificationAdjustmentFactor, PackageModificationAdjustmentGroupCode, IncreasedLimitFactor, IncreasedLimitGroupCode, YearBuilt, AgencyActualCommissionRate, BaseRate, ConstructionCode, StateRatingEffectiveDate, IndividualRiskPremiumModification, WindCoverageFlag, DeductibleBasis, ExposureBasis, TransactionCreatedUserId, ServiceCentreName, Exposure, NumberOfEmployee, NegateRestateCode, WrittenExposure, DeclaredEventFlag)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	LOGICALINDICATOR, 
	LOGICALDELETEFLAG, 
	PREMIUMTRANSACTIONHASHKEY, 
	PREMIUMLOADSEQUENCE, 
	DUPLICATESEQUENCE, 
	NEXTVAL AS PREMIUMTRANSACTIONAKID, 
	REINSURANCECOVERAGEAKID, 
	STATISTICALCOVERAGEAKID, 
	PREMIUMTRANSACTIONKEY, 
	PMSFUNCTIONCODE, 
	PREMIUMTRANSACTIONCODE, 
	PREMIUMTRANSACTIONENTEREDDATE, 
	PREMIUMTRANSACTIONEFFECTIVEDATE, 
	PREMIUMTRANSACTIONEXPIRATIONDATE, 
	PREMIUMTRANSACTIONBOOKEDDATE, 
	PREMIUMTRANSACTIONAMOUNT, 
	FULLTERMPREMIUM, 
	PREMIUMTYPE, 
	REASONAMENDEDCODE, 
	OFFSETONSETCODE, 
	sup_premium_transaction_id AS SUPPREMIUMTRANSACTIONCODEID, 
	RATINGCOVERAGEAKID, 
	DEDUCTIBLEAMOUNT, 
	EXPERIENCEMODIFICATIONFACTOR, 
	EXPERIENCEMODIFICATIONEFFECTIVEDATE, 
	PACKAGEMODIFICATIONADJUSTMENTFACTOR, 
	PACKAGEMODIFICATIONADJUSTMENTGROUPCODE, 
	INCREASEDLIMITFACTOR, 
	INCREASEDLIMITGROUPCODE, 
	YEARBUILT, 
	AGENCYACTUALCOMMISSIONRATE, 
	BASERATE, 
	CONSTRUCTIONCODE, 
	STATERATINGEFFECTIVEDATE, 
	INDIVIDUALRISKPREMIUMMODIFICATION, 
	WINDCOVERAGEFLAG, 
	DEDUCTIBLEBASIS, 
	EXPOSUREBASIS, 
	TRANSACTIONCREATEDUSERID, 
	SERVICECENTRENAME, 
	EXPOSURE, 
	NumberOfEmployee3 AS NUMBEROFEMPLOYEE, 
	NEGATERESTATECODE, 
	Exposure AS WRITTENEXPOSURE, 
	DECLAREDEVENTFLAG
	FROM EXP_AKID
),
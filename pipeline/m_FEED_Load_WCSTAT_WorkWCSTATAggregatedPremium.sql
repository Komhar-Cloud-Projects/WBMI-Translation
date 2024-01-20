WITH
LKP_MultiStatePolicy AS (
	SELECT
	PolicyAKId
	FROM (
		select PolicyAKId as PolicyAKId
		from (
		SELECT distinct RL.PolicyAKId, RL.StateProvinceCode 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on RL.RiskLocationAKID = PC.RiskLocationAKID and RL.CurrentSnapshotFlag = 1
		and PC.CurrentSnapshotFlag  = 1
		)src
		group by PolicyAKId having count(*)>1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKId ORDER BY PolicyAKId) = 1
),
LKP_SupClassificationWorkersCompensation AS (
	SELECT
	SubjecttoExperienceModificationClassIndicator,
	ClassCode,
	RatingStateCode,
	EffectiveDate,
	ExpirationDate
	FROM (
		SELECT concat(s.SubjecttoExperienceModificationClassIndicator ,s.ExperienceModificationClassIndicator) as SubjecttoExperienceModificationClassIndicator, 
		 s.ClassCode as ClassCode,s.RatingStateCode as RatingStateCode, 
		 s.EffectiveDate as EffectiveDate,s.ExpirationDate as ExpirationDate 
		  FROM [RPT_EDM].[dbo].SupClassificationWorkersCompensation as s
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClassCode,RatingStateCode,EffectiveDate,ExpirationDate ORDER BY SubjecttoExperienceModificationClassIndicator) = 1
),
LKP_AuditSchedule AS (
	SELECT
	AuditStatus,
	PolicyKey,
	InsuranceLine,
	AuditEffectiveDate,
	AuditExpirationDate
	FROM (
		SELECT 
			AuditStatus,
			PolicyKey,
			InsuranceLine,
			AuditEffectiveDate,
			AuditExpirationDate
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.AuditSchedule
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,InsuranceLine,AuditEffectiveDate,AuditExpirationDate ORDER BY AuditStatus) = 1
),
LKP_ExpModTotalPremiumAmount AS (
	SELECT
	TotalPremiumTransactionAmount,
	PolicyKey
	FROM (
		select sum(pt.PremiumTransactionAmount) as TotalPremiumTransactionAmount, POL.pol_key as PolicyKey
		 
		FROM PremiumTransaction PT
		 
		inner join RatingCoverage RC
		on PT.RatingCoverageAKID=RC.RatingCoverageAKID
		and RC.EffectiveDate=PT.EffectiveDate
		 
		inner join SupClassificationWorkersCompensation SCWC
		on SCWC.ClassCode = RC.ClassCode
		AND SCWC.ExperienceModificationClassIndicator = 'Y'
		AND SCWC.CurrentSnapshotFlag = 1
		 
		inner join PolicyCoverage PC
		on PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
		and PC.CurrentSnapshotFlag=1
		and PC.TypeBureauCode in ('WC','WP','WorkersCompensation')
		 
		inner join v2.policy POL
		on POL.pol_ak_id=PC.PolicyAKID
		and POL.crrnt_snpsht_flag=1
		and PT.CurrentSnapshotFlag=1
		 
		group by POL.pol_key
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY TotalPremiumTransactionAmount) = 1
),
SEQTRANS AS (
	CREATE SEQUENCE SEQTRANS
	START = 0
	INCREMENT = 1;
),
LKP_sup_state AS (
	SELECT
	state_abbrev,
	state_code
	FROM (
		SELECT LTRIM(RTRIM(state_code)) as state_code,
		LTRIM(RTRIM(state_abbrev)) as state_abbrev 
		FROM sup_state
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code ORDER BY state_abbrev) = 1
),
lkp_ClaimNumber AS (
	SELECT
	TypeOfRecoveryCode,
	Claim_Number,
	claim_party_occurrence_ak_id
	FROM (
		select distinct RTRIM(Claim_Number) as Claim_Number, ,claim_party_occurrence_ak_id as claim_party_occurrence_ak_id,
		TypeOfRecoveryCode as TypeOfRecoveryCode 
		from (
		SELECT (CASE WHEN rtrim(ltrim(co.s3p_claim_num)) ='N/A' THEN (RIGHT('000'+CAST((DATEDIFF(d, DATEADD(yy, DATEDIFF(yy,0,co.claim_loss_date),0),co.claim_loss_date)+1) AS VARCHAR),3)+co.claim_occurrence_num+cpo.claimant_num) ELSE co.s3p_claim_num END) AS Claim_Number,
		MAX(case when (cttd.pms_trans_code in ('81','82','83','84','85','86','87','88','89')) AND cltf.direct_subrogation_paid <> 0  then '03' else '01' end) AS TypeOfRecoveryCode
		,cpo.claim_party_occurrence_ak_id
		FROM claim_loss_transaction_fact cltf
		inner join claimant_coverage_dim  ccdim on ccdim.claimant_cov_dim_id = cltf.claimant_cov_dim_id
		INNER JOIN loss_master_fact LMF ON LMF.claimant_cov_dim_id=ccdim.claimant_cov_dim_id
		inner join claim_transaction_type_dim  cttd on cltf.claim_trans_type_dim_id =cttd.claim_trans_type_dim_id and trans_kind_code = 'D' 
		inner join calendar_dim c on cltf.claim_trans_date_id=c.clndr_id
		inner join  @{pipeline().parameters.SOURCE_DATABASE_NAME_IL}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail  ccd
		on ccdim.edw_claimant_cov_det_ak_id =ccd.claimant_cov_det_ak_id  and ccd.crrnt_snpsht_flag=1
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME_IL}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence   cpo
		ON ccd.claim_party_occurrence_ak_id=cpo.claim_party_occurrence_ak_id 
		AND cpo.crrnt_snpsht_flag=1 AND ccd.crrnt_snpsht_flag = 1
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME_IL}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence co
		ON cpo.claim_occurrence_ak_id=co.claim_occurrence_ak_id 
		AND co.crrnt_snpsht_flag=1 
		WHERE c.clndr_date <DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}+1, 0)
		GROUP BY (CASE WHEN rtrim(ltrim(co.s3p_claim_num)) ='N/A' THEN (RIGHT('000'+CAST((DATEDIFF(d, DATEADD(yy, DATEDIFF(yy,0,co.claim_loss_date),0),co.claim_loss_date)+1) AS VARCHAR),3)+co.claim_occurrence_num+cpo.claimant_num) ELSE co.s3p_claim_num END)
		,cpo.claim_party_occurrence_ak_id
		HAVING MAX(case when (cttd.pms_trans_code in ('81','82','83','84','85','86','87','88','89')) AND cltf.direct_subrogation_paid <> 0  then '03' else '01' end) ='03'
		) t
		order by Claim_Number, claim_party_occurrence_ak_id,TypeOfRecoveryCode
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Claim_Number,claim_party_occurrence_ak_id ORDER BY TypeOfRecoveryCode) = 1
),
LKP_PremiumTransaction_pol_ak_id AS (
	SELECT
	pol_ak_id
	FROM (
		SELECT DISTINCT P.pol_ak_id AS pol_ak_id
		   from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
		on SC.StatisticalCoverageAKID=PT.StatisticalCoverageAKID
		and SC.CurrentSnapshotFlag=1 and  PT.CurrentSnapshotFlag=1
		
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on PC.PolicyCoverageAKID=SC.PolicyCoverageAKID
		and PC.CurrentSnapshotFlag=1
		
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
		on PC.RiskLocationAKID=RL.RiskLocationAKID
		and RL.CurrentSnapshotFlag=1
		
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P
		on P.pol_ak_id=RL.PolicyAKID 
		and P.crrnt_snpsht_flag=1 
		
		where SC.SourceSystemID='PMS'  
		and PC.TypeBureauCode in ('WC','WP','WorkersCompensation')  and  PT.PremiumType='D'   and PT.ReasonAmendedCode != 'CWO'
		and DATEDIFF(MM,P.pol_eff_date, DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}, 0) )  > 18
		and PT.PremiumTransactionBookedDate>=DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}, 0)
		and PT.PremiumTransactionBookedDate<DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}+1, 0)
		
		UNION ALL
		
		SELECT DISTINCT  P.pol_ak_id AS pol_ak_id
		   from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
		on RC.RatingCoverageAKID=PT.RatingCoverageAKId
		and RC.EffectiveDate=PT.EffectiveDate and PT.CurrentSnapshotFlag=1
		
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
		and PC.CurrentSnapshotFlag=1
		
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
		on PC.RiskLocationAKID=RL.RiskLocationAKID
		and RL.CurrentSnapshotFlag=1
		
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P
		on P.pol_ak_id=RL.PolicyAKID 
		and P.crrnt_snpsht_flag=1 
		
		where RC.SourceSystemID='DCT' and PC.TypeBureauCode in ('WC','WP','WorkersCompensation')  and PT.PremiumType='D'
		and PT.ReasonAmendedCode NOT IN ('CWO','Claw Back')
		and DATEDIFF(MM,P.pol_eff_date, DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}, 0) )  > 18
		and PT.PremiumTransactionBookedDate>=DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}, 0)
		and PT.PremiumTransactionBookedDate<DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}+1, 0)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id ORDER BY pol_ak_id) = 1
),
SQ_WorkWCSTATPremium AS (
	SELECT
		WorkWCSTATPremiumId,
		AuditId,
		CreatedDate,
		ModifiedDate,
		SourceSystemId,
		PremiumMasterCalculationID,
		PremiumMasterRunDate,
		StateRatingEffectiveDate,
		WCRetrospectiveRatingIndicator,
		ExperienceModificationFactor,
		ExperienceModificationEffectiveDate,
		PremiumMasterPremium,
		BaseRate,
		TypeBureauCode,
		PolicySymbol,
		PolicyNumber,
		PolicyModulus,
		StateProvinceCode,
		PolicyEffectiveDate,
		PolicyExpiryDate,
		PolicyCancellationDate,
		PolicyCancellationIndicator,
		InterstateRiskId,
		FederalTaxId,
		PolicyTerm,
		PolicyAKId,
		InsuranceSegmentAbbreviation,
		CustomerRole,
		Name,
		AddressLine1,
		CityName,
		StateProvCodeContractCustomerAddress,
		ZipPostalCode,
		ClassCode,
		Exposure,
		DeductibleAmount,
		InsuranceLine,
		BalanceMinimumPremiumTotal,
		RateEffectiveDate,
		ReasonAmendedCode,
		StrategicProfitCenterAbbreviation,
		PolicyStatusCode,
		PremiumTransactionEnteredDate,
		OffsetOnsetCode,
		PolicyKey,
		CoverageType,
		PremiumTransactionCode,
		CoverageGUID,
		RatingCoverageAKID,
		RunMonthAuditTransactionFlag,
		AgeOfPolicy,
		TermType,
		PeriodStartDate,
		PeriodEndDate,
		AnyARDIndicator,
		ExperienceRated,
		PremiumTransactionEffectiveDate,
		LossesSubjectToDeductibleCode,
		BasisOfDeductibleCalculationCode
	FROM WorkWCSTATPremium
	WHERE AuditId=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}  
	 @{pipeline().parameters.WHERE_CLAUSE}
),
EXP_workWcStatPremiumSource AS (
	SELECT
	WorkWCSTATPremiumId AS workWcStatPremiumSourceID,
	PremiumMasterCalculationID,
	PremiumMasterRunDate,
	StateRatingEffectiveDate,
	WCRetrospectiveRatingIndicator,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	PremiumMasterPremium,
	BaseRate,
	TypeBureauCode,
	PolicySymbol AS pol_sym,
	PolicyNumber AS pol_num,
	PolicyModulus AS pol_mod,
	StateProvinceCode,
	PolicyEffectiveDate AS pol_eff_date,
	PolicyExpiryDate AS pol_exp_date,
	PolicyCancellationDate AS pol_cancellation_date,
	PolicyCancellationIndicator AS pol_cancellation_ind,
	InterstateRiskId,
	FederalTaxId AS fed_tax_id,
	PolicyTerm AS pol_term,
	PolicyAKId AS pol_ak_id,
	InsuranceSegmentAbbreviation,
	CustomerRole AS cust_role,
	Name AS name,
	AddressLine1 AS addr_line_1,
	CityName AS city_name,
	StateProvCodeContractCustomerAddress AS state_prov_code,
	ZipPostalCode AS zip_postal_code,
	ClassCode,
	Exposure,
	DeductibleAmount,
	InsuranceLine,
	BalanceMinimumPremiumTotal AS BalMinPremiumTotal,
	RateEffectiveDate,
	SourceSystemId AS SourceSystem,
	ReasonAmendedCode,
	StrategicProfitCenterAbbreviation,
	PolicyStatusCode AS pol_status_code,
	PremiumTransactionEnteredDate,
	OffsetOnsetCode,
	PolicyKey AS Pol_key,
	CoverageType,
	PremiumTransactionCode,
	CoverageGUID,
	RatingCoverageAKID,
	RunMonthAuditTransactionFlag AS RunMonthAuditTransFlag,
	AgeOfPolicy,
	TermType,
	PeriodStartDate AS Period_start_date,
	PeriodEndDate AS Period_end_date,
	AnyARDIndicator,
	ExperienceRated,
	PremiumTransactionEffectiveDate,
	LossesSubjectToDeductibleCode,
	BasisOfDeductibleCalculationCode
	FROM SQ_WorkWCSTATPremium
),
RTR_Premium AS (
	SELECT
	workWcStatPremiumSourceID,
	PremiumMasterCalculationID,
	PremiumMasterRunDate,
	StateRatingEffectiveDate,
	WCRetrospectiveRatingIndicator,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	PremiumMasterPremium,
	BaseRate,
	TypeBureauCode,
	pol_sym,
	pol_num,
	pol_mod,
	StateProvinceCode,
	pol_eff_date,
	pol_exp_date,
	pol_cancellation_date,
	pol_cancellation_ind,
	InterstateRiskId,
	pol_id,
	fed_tax_id,
	pol_term,
	pol_ak_id,
	InsuranceSegmentAbbreviation,
	cust_role,
	name,
	addr_line_1,
	city_name,
	state_prov_code,
	zip_postal_code,
	ClassCode,
	Exposure,
	DeductibleAmount,
	InsuranceLine,
	BalMinPremiumTotal,
	RateEffectiveDate,
	SourceSystem,
	ReasonAmendedCode,
	StrategicProfitCenterAbbreviation,
	pol_status_code,
	PremiumTransactionEnteredDate,
	OffsetOnsetCode,
	Pol_key,
	CoverageType,
	PremiumTransactionCode,
	CoverageGUID,
	RatingCoverageAKID,
	RunMonthAuditTransFlag,
	AgeOfPolicy,
	TermType,
	Period_start_date,
	Period_end_date,
	AnyARDIndicator,
	ExperienceRated,
	PremiumTransactionEffectiveDate,
	LossesSubjectToDeductibleCode,
	BasisOfDeductibleCalculationCode
	FROM EXP_workWcStatPremiumSource
),
RTR_Premium_PMS AS (SELECT * FROM RTR_Premium WHERE SourceSystem='PMS'),
RTR_Premium_DCT AS (SELECT * FROM RTR_Premium WHERE SourceSystem='DCT'),
RTR_Premium_DCT_MN AS (SELECT * FROM RTR_Premium WHERE SourceSystem='DCT PT'),
EXP_dct AS (
	SELECT
	SourceSystem,
	PremiumMasterCalculationID,
	PremiumMasterRunDate,
	StateRatingEffectiveDate,
	WCRetrospectiveRatingIndicator,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	PremiumMasterPremium,
	BaseRate,
	TypeBureauCode,
	pol_sym,
	pol_num,
	pol_mod,
	Pol_key AS pol_key,
	CoverageType,
	PremiumTransactionCode,
	CoverageGUID,
	RatingCoverageAKID,
	Period_start_date,
	Period_end_date,
	AnyARDIndicator,
	ExperienceRated,
	TermType,
	StateProvinceCode,
	pol_eff_date,
	pol_exp_date,
	pol_cancellation_date,
	pol_cancellation_ind,
	InterstateRiskId,
	pol_id,
	fed_tax_id,
	pol_term,
	pol_ak_id,
	InsuranceSegmentAbbreviation,
	cust_role,
	name,
	addr_line_1,
	city_name,
	state_prov_code,
	zip_postal_code,
	ClassCode,
	Exposure AS WrittenExposure,
	DeductibleAmount,
	InsuranceLine,
	BalMinPremiumTotal,
	ReasonAmendedCode,
	StrategicProfitCenterAbbreviation,
	pol_status_code,
	PremiumTransactionEnteredDate,
	OffsetOnsetCode,
	RunMonthAuditTransFlag AS RunMonthAuditTransFlag3,
	AgeOfPolicy AS AgeOfPolicy3,
	RateEffectiveDate,
	PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate3,
	LossesSubjectToDeductibleCode,
	BasisOfDeductibleCalculationCode
	FROM RTR_Premium_DCT
),
SRT_Premium_DCT AS (
	SELECT
	SourceSystem, 
	PremiumMasterCalculationID, 
	PremiumMasterRunDate, 
	StateRatingEffectiveDate, 
	WCRetrospectiveRatingIndicator, 
	ExperienceModificationFactor, 
	ExperienceModificationEffectiveDate, 
	PremiumMasterPremium, 
	BaseRate, 
	TypeBureauCode, 
	pol_sym, 
	pol_num, 
	pol_mod, 
	pol_key, 
	CoverageType, 
	PremiumTransactionCode, 
	CoverageGUID, 
	RatingCoverageAKID, 
	Period_start_date, 
	Period_end_date, 
	AnyARDIndicator, 
	ExperienceRated, 
	TermType, 
	StateProvinceCode, 
	pol_eff_date, 
	pol_exp_date, 
	pol_cancellation_date, 
	pol_cancellation_ind, 
	InterstateRiskId, 
	pol_id, 
	fed_tax_id, 
	pol_term, 
	pol_ak_id, 
	InsuranceSegmentAbbreviation, 
	cust_role, 
	name, 
	addr_line_1, 
	city_name, 
	state_prov_code, 
	zip_postal_code, 
	ClassCode, 
	WrittenExposure, 
	DeductibleAmount, 
	InsuranceLine, 
	BalMinPremiumTotal, 
	ReasonAmendedCode, 
	StrategicProfitCenterAbbreviation, 
	pol_status_code, 
	PremiumTransactionEnteredDate, 
	OffsetOnsetCode, 
	RunMonthAuditTransFlag3, 
	AgeOfPolicy3, 
	RateEffectiveDate, 
	PremiumTransactionEffectiveDate3, 
	LossesSubjectToDeductibleCode, 
	BasisOfDeductibleCalculationCode
	FROM EXP_dct
	ORDER BY pol_key ASC
),
AGG_Premium_DCT_PremiumTotal AS (
	SELECT
	pol_key,
	PremiumMasterPremium,
	-- *INF*: ROUND(sum(PremiumMasterPremium))
	ROUND(sum(PremiumMasterPremium)) AS o_PolicyPremiumTotal
	FROM SRT_Premium_DCT
	GROUP BY pol_key
),
JNR_Premium_DCT_PremiumTotal AS (SELECT
	SRT_Premium_DCT.SourceSystem, 
	SRT_Premium_DCT.PremiumMasterCalculationID, 
	SRT_Premium_DCT.PremiumMasterRunDate, 
	SRT_Premium_DCT.StateRatingEffectiveDate, 
	SRT_Premium_DCT.WCRetrospectiveRatingIndicator, 
	SRT_Premium_DCT.ExperienceModificationFactor, 
	SRT_Premium_DCT.ExperienceModificationEffectiveDate, 
	SRT_Premium_DCT.PremiumMasterPremium, 
	SRT_Premium_DCT.BaseRate, 
	SRT_Premium_DCT.TypeBureauCode, 
	SRT_Premium_DCT.pol_sym, 
	SRT_Premium_DCT.pol_num, 
	SRT_Premium_DCT.pol_mod, 
	SRT_Premium_DCT.pol_key, 
	SRT_Premium_DCT.CoverageType, 
	SRT_Premium_DCT.PremiumTransactionCode, 
	SRT_Premium_DCT.CoverageGUID, 
	SRT_Premium_DCT.RatingCoverageAKID, 
	SRT_Premium_DCT.Period_start_date, 
	SRT_Premium_DCT.Period_end_date, 
	SRT_Premium_DCT.AnyARDIndicator, 
	SRT_Premium_DCT.ExperienceRated, 
	SRT_Premium_DCT.TermType, 
	SRT_Premium_DCT.StateProvinceCode, 
	SRT_Premium_DCT.pol_eff_date, 
	SRT_Premium_DCT.pol_exp_date, 
	SRT_Premium_DCT.pol_cancellation_date, 
	SRT_Premium_DCT.pol_cancellation_ind, 
	SRT_Premium_DCT.InterstateRiskId, 
	SRT_Premium_DCT.pol_id, 
	SRT_Premium_DCT.fed_tax_id, 
	SRT_Premium_DCT.pol_term, 
	SRT_Premium_DCT.pol_ak_id, 
	SRT_Premium_DCT.InsuranceSegmentAbbreviation, 
	SRT_Premium_DCT.cust_role, 
	SRT_Premium_DCT.name, 
	SRT_Premium_DCT.addr_line_1, 
	SRT_Premium_DCT.city_name, 
	SRT_Premium_DCT.state_prov_code, 
	SRT_Premium_DCT.zip_postal_code, 
	SRT_Premium_DCT.ClassCode, 
	SRT_Premium_DCT.WrittenExposure, 
	SRT_Premium_DCT.DeductibleAmount, 
	SRT_Premium_DCT.InsuranceLine, 
	SRT_Premium_DCT.BalMinPremiumTotal, 
	SRT_Premium_DCT.ReasonAmendedCode, 
	SRT_Premium_DCT.StrategicProfitCenterAbbreviation, 
	SRT_Premium_DCT.pol_status_code, 
	SRT_Premium_DCT.PremiumTransactionEnteredDate, 
	SRT_Premium_DCT.OffsetOnsetCode, 
	AGG_Premium_DCT_PremiumTotal.pol_key AS pol_key1, 
	AGG_Premium_DCT_PremiumTotal.o_PolicyPremiumTotal AS o_PremiumMasterPremium, 
	SRT_Premium_DCT.RunMonthAuditTransFlag3, 
	SRT_Premium_DCT.AgeOfPolicy3, 
	SRT_Premium_DCT.RateEffectiveDate, 
	SRT_Premium_DCT.PremiumTransactionEffectiveDate3, 
	SRT_Premium_DCT.LossesSubjectToDeductibleCode, 
	SRT_Premium_DCT.BasisOfDeductibleCalculationCode
	FROM SRT_Premium_DCT
	LEFT OUTER JOIN AGG_Premium_DCT_PremiumTotal
	ON AGG_Premium_DCT_PremiumTotal.pol_key = SRT_Premium_DCT.pol_key
),
SRT_Premium_DCT_ManualPremium AS (
	SELECT
	SourceSystem, 
	PremiumMasterCalculationID, 
	PremiumMasterRunDate, 
	StateRatingEffectiveDate, 
	WCRetrospectiveRatingIndicator, 
	ExperienceModificationFactor, 
	ExperienceModificationEffectiveDate, 
	PremiumMasterPremium, 
	BaseRate, 
	TypeBureauCode, 
	pol_sym, 
	pol_num, 
	pol_mod, 
	pol_key, 
	CoverageType, 
	PremiumTransactionCode, 
	CoverageGUID, 
	RatingCoverageAKID, 
	Period_start_date, 
	Period_end_date, 
	AnyARDIndicator, 
	ExperienceRated, 
	TermType, 
	StateProvinceCode, 
	pol_eff_date, 
	pol_exp_date, 
	pol_cancellation_date, 
	pol_cancellation_ind, 
	InterstateRiskId, 
	pol_id, 
	fed_tax_id, 
	pol_term, 
	pol_ak_id, 
	InsuranceSegmentAbbreviation, 
	cust_role, 
	name, 
	addr_line_1, 
	city_name, 
	state_prov_code, 
	zip_postal_code, 
	ClassCode, 
	WrittenExposure, 
	DeductibleAmount, 
	InsuranceLine, 
	BalMinPremiumTotal, 
	ReasonAmendedCode, 
	StrategicProfitCenterAbbreviation, 
	pol_status_code, 
	PremiumTransactionEnteredDate, 
	OffsetOnsetCode, 
	o_PremiumMasterPremium AS o_PolicyPremiumTotal, 
	RunMonthAuditTransFlag3, 
	AgeOfPolicy3, 
	RateEffectiveDate, 
	PremiumTransactionEffectiveDate3, 
	LossesSubjectToDeductibleCode, 
	BasisOfDeductibleCalculationCode
	FROM JNR_Premium_DCT_PremiumTotal
	ORDER BY pol_key ASC, StateProvinceCode ASC
),
AGG_Premium_DCT_ManualPremium AS (
	SELECT
	pol_key,
	StateProvinceCode,
	PremiumMasterPremium,
	CoverageType,
	-- *INF*: sum(iif(CoverageType='ManualPremium', 1, 0))
	sum(
	    IFF(
	        CoverageType = 'ManualPremium', 1, 0
	    )) AS o_ManualPremiumInd,
	-- *INF*: round(sum(iif(CoverageType='ManualPremium', PremiumMasterPremium, 0)))
	round(sum(
	        IFF(
	            CoverageType = 'ManualPremium', PremiumMasterPremium, 0
	        ))) AS o_PolicyStateManualPremiumTotal
	FROM SRT_Premium_DCT_ManualPremium
	GROUP BY pol_key, StateProvinceCode
),
JNR_Premium_DCT_ManualPremium AS (SELECT
	SRT_Premium_DCT_ManualPremium.SourceSystem, 
	SRT_Premium_DCT_ManualPremium.PremiumMasterCalculationID, 
	SRT_Premium_DCT_ManualPremium.PremiumMasterRunDate, 
	SRT_Premium_DCT_ManualPremium.StateRatingEffectiveDate, 
	SRT_Premium_DCT_ManualPremium.WCRetrospectiveRatingIndicator, 
	SRT_Premium_DCT_ManualPremium.ExperienceModificationFactor, 
	SRT_Premium_DCT_ManualPremium.ExperienceModificationEffectiveDate, 
	SRT_Premium_DCT_ManualPremium.PremiumMasterPremium, 
	SRT_Premium_DCT_ManualPremium.BaseRate, 
	SRT_Premium_DCT_ManualPremium.TypeBureauCode, 
	SRT_Premium_DCT_ManualPremium.pol_sym, 
	SRT_Premium_DCT_ManualPremium.pol_num, 
	SRT_Premium_DCT_ManualPremium.pol_mod, 
	SRT_Premium_DCT_ManualPremium.pol_key, 
	SRT_Premium_DCT_ManualPremium.CoverageType, 
	SRT_Premium_DCT_ManualPremium.PremiumTransactionCode, 
	SRT_Premium_DCT_ManualPremium.CoverageGUID, 
	SRT_Premium_DCT_ManualPremium.RatingCoverageAKID, 
	SRT_Premium_DCT_ManualPremium.Period_start_date, 
	SRT_Premium_DCT_ManualPremium.Period_end_date, 
	SRT_Premium_DCT_ManualPremium.AnyARDIndicator, 
	SRT_Premium_DCT_ManualPremium.ExperienceRated, 
	SRT_Premium_DCT_ManualPremium.TermType, 
	SRT_Premium_DCT_ManualPremium.StateProvinceCode, 
	SRT_Premium_DCT_ManualPremium.pol_eff_date, 
	SRT_Premium_DCT_ManualPremium.pol_exp_date, 
	SRT_Premium_DCT_ManualPremium.pol_cancellation_date, 
	SRT_Premium_DCT_ManualPremium.pol_cancellation_ind, 
	SRT_Premium_DCT_ManualPremium.InterstateRiskId, 
	SRT_Premium_DCT_ManualPremium.pol_id, 
	SRT_Premium_DCT_ManualPremium.fed_tax_id, 
	SRT_Premium_DCT_ManualPremium.pol_term, 
	SRT_Premium_DCT_ManualPremium.pol_ak_id, 
	SRT_Premium_DCT_ManualPremium.InsuranceSegmentAbbreviation, 
	SRT_Premium_DCT_ManualPremium.cust_role, 
	SRT_Premium_DCT_ManualPremium.name, 
	SRT_Premium_DCT_ManualPremium.addr_line_1, 
	SRT_Premium_DCT_ManualPremium.city_name, 
	SRT_Premium_DCT_ManualPremium.state_prov_code, 
	SRT_Premium_DCT_ManualPremium.zip_postal_code, 
	SRT_Premium_DCT_ManualPremium.ClassCode, 
	SRT_Premium_DCT_ManualPremium.WrittenExposure, 
	SRT_Premium_DCT_ManualPremium.DeductibleAmount, 
	SRT_Premium_DCT_ManualPremium.InsuranceLine, 
	SRT_Premium_DCT_ManualPremium.o_PolicyPremiumTotal, 
	AGG_Premium_DCT_ManualPremium.o_ManualPremiumInd, 
	AGG_Premium_DCT_ManualPremium.o_PolicyStateManualPremiumTotal, 
	SRT_Premium_DCT_ManualPremium.BalMinPremiumTotal, 
	SRT_Premium_DCT_ManualPremium.ReasonAmendedCode, 
	SRT_Premium_DCT_ManualPremium.StrategicProfitCenterAbbreviation, 
	SRT_Premium_DCT_ManualPremium.pol_status_code, 
	SRT_Premium_DCT_ManualPremium.PremiumTransactionEnteredDate, 
	SRT_Premium_DCT_ManualPremium.OffsetOnsetCode, 
	AGG_Premium_DCT_ManualPremium.StateProvinceCode AS StateProvinceCode1, 
	AGG_Premium_DCT_ManualPremium.pol_key AS pol_key2, 
	SRT_Premium_DCT_ManualPremium.RunMonthAuditTransFlag3, 
	SRT_Premium_DCT_ManualPremium.AgeOfPolicy3, 
	SRT_Premium_DCT_ManualPremium.RateEffectiveDate, 
	SRT_Premium_DCT_ManualPremium.PremiumTransactionEffectiveDate3, 
	SRT_Premium_DCT_ManualPremium.LossesSubjectToDeductibleCode, 
	SRT_Premium_DCT_ManualPremium.BasisOfDeductibleCalculationCode
	FROM SRT_Premium_DCT_ManualPremium
	LEFT OUTER JOIN AGG_Premium_DCT_ManualPremium
	ON AGG_Premium_DCT_ManualPremium.pol_key = SRT_Premium_DCT_ManualPremium.pol_key AND AGG_Premium_DCT_ManualPremium.StateProvinceCode = SRT_Premium_DCT_ManualPremium.StateProvinceCode
),
EXP_Premium_DCT AS (
	SELECT
	SourceSystem,
	PremiumMasterCalculationID,
	PremiumMasterRunDate,
	StateRatingEffectiveDate,
	WCRetrospectiveRatingIndicator,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	PremiumMasterPremium,
	BaseRate,
	TypeBureauCode,
	pol_sym,
	pol_num,
	pol_mod,
	pol_key,
	CoverageType,
	PremiumTransactionCode,
	CoverageGUID,
	RatingCoverageAKID,
	Period_start_date,
	Period_end_date,
	AnyARDIndicator,
	ExperienceRated,
	TermType,
	StateProvinceCode,
	pol_eff_date,
	pol_exp_date,
	pol_cancellation_date,
	pol_cancellation_ind,
	InterstateRiskId,
	pol_id,
	fed_tax_id,
	pol_term,
	pol_ak_id,
	InsuranceSegmentAbbreviation,
	cust_role,
	name,
	addr_line_1,
	city_name,
	state_prov_code,
	zip_postal_code,
	ClassCode,
	WrittenExposure,
	DeductibleAmount,
	InsuranceLine,
	o_PolicyPremiumTotal AS PolicyPremiumTotal,
	o_ManualPremiumInd AS ManualPremiumInd,
	o_PolicyStateManualPremiumTotal AS PolicyStateManualPremiumTotal,
	BalMinPremiumTotal,
	ReasonAmendedCode,
	StrategicProfitCenterAbbreviation,
	pol_status_code,
	PremiumTransactionEnteredDate,
	OffsetOnsetCode,
	RunMonthAuditTransFlag3,
	AgeOfPolicy3,
	RateEffectiveDate,
	PremiumTransactionEffectiveDate3,
	LossesSubjectToDeductibleCode,
	BasisOfDeductibleCalculationCode
	FROM JNR_Premium_DCT_ManualPremium
),
EXP_PMS AS (
	SELECT
	SourceSystem,
	PremiumMasterCalculationID,
	PremiumMasterRunDate,
	StateRatingEffectiveDate,
	WCRetrospectiveRatingIndicator,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	PremiumMasterPremium,
	BaseRate,
	TypeBureauCode,
	pol_sym,
	pol_num,
	pol_mod,
	Pol_key AS pol_key,
	CoverageType,
	PremiumTransactionCode,
	CoverageGUID,
	RatingCoverageAKID,
	Period_start_date,
	Period_end_date,
	AnyARDIndicator,
	ExperienceRated,
	TermType,
	StateProvinceCode,
	pol_eff_date,
	pol_exp_date,
	pol_cancellation_date,
	pol_cancellation_ind,
	InterstateRiskId,
	pol_id,
	fed_tax_id,
	pol_term,
	pol_ak_id,
	InsuranceSegmentAbbreviation,
	cust_role,
	name,
	addr_line_ AS addr_line_1,
	city_name,
	state_prov_code,
	zip_postal_code,
	ClassCode,
	Exposure AS WrittenExposure,
	DeductibleAmount,
	InsuranceLine,
	Pol_key AS policy_pol_key,
	ReasonAmendedCode,
	StrategicProfitCenterAbbreviation,
	pol_status_code,
	PremiumTransactionEnteredDate,
	OffsetOnsetCode,
	RunMonthAuditTransFlag AS RunMonthAuditTransFlag1,
	AgeOfPolicy AS AgeOfPolicy1,
	RateEffectiveDate,
	PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate1,
	LossesSubjectToDeductibleCode,
	BasisOfDeductibleCalculationCode
	FROM RTR_Premium_PMS
),
SRT_Premium_PMS AS (
	SELECT
	SourceSystem, 
	PremiumMasterCalculationID, 
	PremiumMasterRunDate, 
	StateRatingEffectiveDate, 
	WCRetrospectiveRatingIndicator, 
	ExperienceModificationFactor, 
	ExperienceModificationEffectiveDate, 
	PremiumMasterPremium, 
	BaseRate, 
	TypeBureauCode, 
	pol_sym, 
	pol_num, 
	pol_mod, 
	pol_key, 
	CoverageType, 
	PremiumTransactionCode, 
	CoverageGUID, 
	RatingCoverageAKID, 
	Period_start_date, 
	Period_end_date, 
	AnyARDIndicator, 
	ExperienceRated, 
	TermType, 
	StateProvinceCode, 
	pol_eff_date, 
	pol_exp_date, 
	pol_cancellation_date, 
	pol_cancellation_ind, 
	InterstateRiskId, 
	pol_id, 
	fed_tax_id, 
	pol_term, 
	pol_ak_id, 
	InsuranceSegmentAbbreviation, 
	cust_role, 
	name, 
	addr_line_1, 
	city_name, 
	state_prov_code, 
	zip_postal_code, 
	ClassCode, 
	WrittenExposure, 
	DeductibleAmount, 
	InsuranceLine, 
	policy_pol_key, 
	ReasonAmendedCode, 
	StrategicProfitCenterAbbreviation, 
	pol_status_code, 
	PremiumTransactionEnteredDate, 
	OffsetOnsetCode, 
	RunMonthAuditTransFlag1, 
	AgeOfPolicy1, 
	RateEffectiveDate, 
	PremiumTransactionEffectiveDate1, 
	LossesSubjectToDeductibleCode, 
	BasisOfDeductibleCalculationCode
	FROM EXP_PMS
	ORDER BY pol_key ASC
),
AGG_Premium_PMS_PremiumTotal AS (
	SELECT
	pol_key AS policy_pol_key,
	PremiumMasterPremium,
	-- *INF*: round(sum(PremiumMasterPremium))
	round(sum(PremiumMasterPremium)) AS o_PolicyPremiumTotal
	FROM SRT_Premium_PMS
	GROUP BY policy_pol_key
),
JNR_Premium_PMS_PremiumTotal AS (SELECT
	SRT_Premium_PMS.SourceSystem, 
	SRT_Premium_PMS.PremiumMasterCalculationID, 
	SRT_Premium_PMS.PremiumMasterRunDate, 
	SRT_Premium_PMS.StateRatingEffectiveDate, 
	SRT_Premium_PMS.WCRetrospectiveRatingIndicator, 
	SRT_Premium_PMS.ExperienceModificationFactor, 
	SRT_Premium_PMS.ExperienceModificationEffectiveDate, 
	SRT_Premium_PMS.PremiumMasterPremium, 
	SRT_Premium_PMS.BaseRate, 
	SRT_Premium_PMS.TypeBureauCode, 
	SRT_Premium_PMS.pol_sym, 
	SRT_Premium_PMS.pol_num, 
	SRT_Premium_PMS.pol_mod, 
	SRT_Premium_PMS.pol_key, 
	SRT_Premium_PMS.CoverageType, 
	SRT_Premium_PMS.PremiumTransactionCode, 
	SRT_Premium_PMS.CoverageGUID, 
	SRT_Premium_PMS.RatingCoverageAKID, 
	SRT_Premium_PMS.Period_start_date, 
	SRT_Premium_PMS.Period_end_date, 
	SRT_Premium_PMS.AnyARDIndicator, 
	SRT_Premium_PMS.ExperienceRated, 
	SRT_Premium_PMS.TermType, 
	SRT_Premium_PMS.StateProvinceCode, 
	SRT_Premium_PMS.pol_eff_date, 
	SRT_Premium_PMS.pol_exp_date, 
	SRT_Premium_PMS.pol_cancellation_date, 
	SRT_Premium_PMS.pol_cancellation_ind, 
	SRT_Premium_PMS.InterstateRiskId, 
	SRT_Premium_PMS.pol_id, 
	SRT_Premium_PMS.fed_tax_id, 
	SRT_Premium_PMS.pol_term, 
	SRT_Premium_PMS.pol_ak_id, 
	SRT_Premium_PMS.InsuranceSegmentAbbreviation, 
	SRT_Premium_PMS.cust_role, 
	SRT_Premium_PMS.name, 
	SRT_Premium_PMS.addr_line_1, 
	SRT_Premium_PMS.city_name, 
	SRT_Premium_PMS.state_prov_code, 
	SRT_Premium_PMS.zip_postal_code, 
	SRT_Premium_PMS.ClassCode, 
	SRT_Premium_PMS.WrittenExposure, 
	SRT_Premium_PMS.DeductibleAmount, 
	SRT_Premium_PMS.InsuranceLine, 
	SRT_Premium_PMS.policy_pol_key, 
	SRT_Premium_PMS.ReasonAmendedCode, 
	SRT_Premium_PMS.StrategicProfitCenterAbbreviation, 
	SRT_Premium_PMS.pol_status_code, 
	SRT_Premium_PMS.PremiumTransactionEnteredDate, 
	SRT_Premium_PMS.OffsetOnsetCode, 
	AGG_Premium_PMS_PremiumTotal.policy_pol_key AS agg_policy_pol_key, 
	AGG_Premium_PMS_PremiumTotal.o_PolicyPremiumTotal AS PolicyPremiumTotal, 
	SRT_Premium_PMS.RunMonthAuditTransFlag1, 
	SRT_Premium_PMS.AgeOfPolicy1, 
	SRT_Premium_PMS.RateEffectiveDate, 
	SRT_Premium_PMS.PremiumTransactionEffectiveDate1, 
	SRT_Premium_PMS.LossesSubjectToDeductibleCode, 
	SRT_Premium_PMS.BasisOfDeductibleCalculationCode
	FROM SRT_Premium_PMS
	INNER JOIN AGG_Premium_PMS_PremiumTotal
	ON AGG_Premium_PMS_PremiumTotal.policy_pol_key = SRT_Premium_PMS.pol_key
),
SRT_Premium_PMS_ManualPremium AS (
	SELECT
	SourceSystem, 
	PremiumMasterCalculationID, 
	PremiumMasterRunDate, 
	StateRatingEffectiveDate, 
	WCRetrospectiveRatingIndicator, 
	ExperienceModificationFactor, 
	ExperienceModificationEffectiveDate, 
	PremiumMasterPremium, 
	BaseRate, 
	TypeBureauCode, 
	pol_sym, 
	pol_num, 
	pol_mod, 
	pol_key, 
	CoverageType, 
	PremiumTransactionCode, 
	CoverageGUID, 
	RatingCoverageAKID, 
	Period_start_date, 
	Period_end_date, 
	AnyARDIndicator, 
	ExperienceRated, 
	TermType, 
	policy_pol_key, 
	StateProvinceCode, 
	pol_eff_date, 
	pol_exp_date, 
	pol_cancellation_date, 
	pol_cancellation_ind, 
	InterstateRiskId, 
	pol_id, 
	fed_tax_id, 
	pol_term, 
	pol_ak_id, 
	InsuranceSegmentAbbreviation, 
	cust_role, 
	name, 
	addr_line_1, 
	city_name, 
	state_prov_code, 
	zip_postal_code, 
	ClassCode, 
	WrittenExposure, 
	DeductibleAmount, 
	InsuranceLine, 
	ReasonAmendedCode, 
	StrategicProfitCenterAbbreviation, 
	pol_status_code, 
	PremiumTransactionEnteredDate, 
	OffsetOnsetCode, 
	PolicyPremiumTotal, 
	RunMonthAuditTransFlag1, 
	AgeOfPolicy1, 
	RateEffectiveDate, 
	PremiumTransactionEffectiveDate1, 
	LossesSubjectToDeductibleCode, 
	BasisOfDeductibleCalculationCode
	FROM JNR_Premium_PMS_PremiumTotal
	ORDER BY pol_key ASC, StateProvinceCode ASC
),
AGG_Premium_PMS_ManualPremium AS (
	SELECT
	pol_key AS policy_pol_key,
	StateProvinceCode,
	PremiumMasterPremium,
	ClassCode,
	-- *INF*: SUM(IIF(LENGTH(ClassCode)=6, 1, 0))
	SUM(
	    IFF(
	        LENGTH(ClassCode) = 6, 1, 0
	    )) AS o_ManualPremiumInd,
	-- *INF*: SUM(IIF(LENGTH(ClassCode)=6, PremiumMasterPremium, 0))
	SUM(
	    IFF(
	        LENGTH(ClassCode) = 6, PremiumMasterPremium, 0
	    )) AS o_PolicyStateManualPremiumTotal,
	-- *INF*: round(SUM(IIF(ClassCode='0990', PremiumMasterPremium, 0)))
	round(SUM(
	        IFF(
	            ClassCode = '0990', PremiumMasterPremium, 0
	        ))) AS o_BalMinPremiumTotal
	FROM SRT_Premium_PMS_ManualPremium
	GROUP BY policy_pol_key, StateProvinceCode
),
JNR_Premium_PMS_ManualPremium AS (SELECT
	SRT_Premium_PMS_ManualPremium.SourceSystem, 
	SRT_Premium_PMS_ManualPremium.PremiumMasterCalculationID, 
	SRT_Premium_PMS_ManualPremium.PremiumMasterRunDate, 
	SRT_Premium_PMS_ManualPremium.StateRatingEffectiveDate, 
	SRT_Premium_PMS_ManualPremium.WCRetrospectiveRatingIndicator, 
	SRT_Premium_PMS_ManualPremium.ExperienceModificationFactor, 
	SRT_Premium_PMS_ManualPremium.ExperienceModificationEffectiveDate, 
	SRT_Premium_PMS_ManualPremium.PremiumMasterPremium, 
	SRT_Premium_PMS_ManualPremium.BaseRate, 
	SRT_Premium_PMS_ManualPremium.TypeBureauCode, 
	SRT_Premium_PMS_ManualPremium.pol_sym, 
	SRT_Premium_PMS_ManualPremium.pol_num, 
	SRT_Premium_PMS_ManualPremium.pol_mod, 
	SRT_Premium_PMS_ManualPremium.pol_key, 
	SRT_Premium_PMS_ManualPremium.CoverageType, 
	SRT_Premium_PMS_ManualPremium.PremiumTransactionCode, 
	SRT_Premium_PMS_ManualPremium.CoverageGUID, 
	SRT_Premium_PMS_ManualPremium.RatingCoverageAKID, 
	SRT_Premium_PMS_ManualPremium.Period_start_date, 
	SRT_Premium_PMS_ManualPremium.Period_end_date, 
	SRT_Premium_PMS_ManualPremium.AnyARDIndicator, 
	SRT_Premium_PMS_ManualPremium.ExperienceRated, 
	SRT_Premium_PMS_ManualPremium.TermType, 
	SRT_Premium_PMS_ManualPremium.StateProvinceCode, 
	SRT_Premium_PMS_ManualPremium.pol_eff_date, 
	SRT_Premium_PMS_ManualPremium.pol_exp_date, 
	SRT_Premium_PMS_ManualPremium.pol_cancellation_date, 
	SRT_Premium_PMS_ManualPremium.pol_cancellation_ind, 
	SRT_Premium_PMS_ManualPremium.InterstateRiskId, 
	SRT_Premium_PMS_ManualPremium.pol_id, 
	SRT_Premium_PMS_ManualPremium.fed_tax_id, 
	SRT_Premium_PMS_ManualPremium.pol_term, 
	SRT_Premium_PMS_ManualPremium.pol_ak_id, 
	SRT_Premium_PMS_ManualPremium.InsuranceSegmentAbbreviation, 
	SRT_Premium_PMS_ManualPremium.cust_role, 
	SRT_Premium_PMS_ManualPremium.name, 
	SRT_Premium_PMS_ManualPremium.addr_line_1, 
	SRT_Premium_PMS_ManualPremium.city_name, 
	SRT_Premium_PMS_ManualPremium.state_prov_code, 
	SRT_Premium_PMS_ManualPremium.zip_postal_code, 
	SRT_Premium_PMS_ManualPremium.ClassCode, 
	SRT_Premium_PMS_ManualPremium.WrittenExposure, 
	SRT_Premium_PMS_ManualPremium.DeductibleAmount, 
	SRT_Premium_PMS_ManualPremium.InsuranceLine, 
	SRT_Premium_PMS_ManualPremium.PolicyPremiumTotal, 
	AGG_Premium_PMS_ManualPremium.o_ManualPremiumInd, 
	AGG_Premium_PMS_ManualPremium.o_PolicyStateManualPremiumTotal, 
	AGG_Premium_PMS_ManualPremium.o_BalMinPremiumTotal, 
	SRT_Premium_PMS_ManualPremium.policy_pol_key, 
	SRT_Premium_PMS_ManualPremium.ReasonAmendedCode, 
	SRT_Premium_PMS_ManualPremium.StrategicProfitCenterAbbreviation, 
	SRT_Premium_PMS_ManualPremium.pol_status_code, 
	SRT_Premium_PMS_ManualPremium.PremiumTransactionEnteredDate, 
	SRT_Premium_PMS_ManualPremium.OffsetOnsetCode, 
	AGG_Premium_PMS_ManualPremium.policy_pol_key AS policy_pol_key1, 
	AGG_Premium_PMS_ManualPremium.StateProvinceCode AS StateProvinceCode1, 
	SRT_Premium_PMS_ManualPremium.RunMonthAuditTransFlag1, 
	SRT_Premium_PMS_ManualPremium.AgeOfPolicy1, 
	SRT_Premium_PMS_ManualPremium.RateEffectiveDate, 
	SRT_Premium_PMS_ManualPremium.PremiumTransactionEffectiveDate1, 
	SRT_Premium_PMS_ManualPremium.LossesSubjectToDeductibleCode, 
	SRT_Premium_PMS_ManualPremium.BasisOfDeductibleCalculationCode
	FROM SRT_Premium_PMS_ManualPremium
	LEFT OUTER JOIN AGG_Premium_PMS_ManualPremium
	ON AGG_Premium_PMS_ManualPremium.policy_pol_key = SRT_Premium_PMS_ManualPremium.pol_key AND AGG_Premium_PMS_ManualPremium.StateProvinceCode = SRT_Premium_PMS_ManualPremium.StateProvinceCode
),
EXP_Premium_PMS AS (
	SELECT
	SourceSystem,
	PremiumMasterCalculationID,
	PremiumMasterRunDate,
	StateRatingEffectiveDate,
	WCRetrospectiveRatingIndicator,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	PremiumMasterPremium,
	BaseRate,
	TypeBureauCode,
	pol_sym,
	pol_num,
	pol_mod,
	pol_key,
	CoverageType,
	PremiumTransactionCode,
	CoverageGUID,
	RatingCoverageAKID,
	Period_start_date,
	Period_end_date,
	AnyARDIndicator,
	ExperienceRated,
	TermType,
	StateProvinceCode,
	pol_eff_date,
	pol_exp_date,
	pol_cancellation_date,
	pol_cancellation_ind,
	InterstateRiskId,
	pol_id,
	fed_tax_id,
	pol_term,
	pol_ak_id,
	InsuranceSegmentAbbreviation,
	cust_role,
	name,
	addr_line_1,
	city_name,
	state_prov_code,
	zip_postal_code,
	ClassCode,
	WrittenExposure,
	DeductibleAmount,
	InsuranceLine,
	PolicyPremiumTotal,
	o_ManualPremiumInd AS ManualPremiumInd,
	o_PolicyStateManualPremiumTotal AS PolicyStateManualPremiumTotal,
	o_BalMinPremiumTotal AS BalMinPremiumTotal,
	ReasonAmendedCode,
	StrategicProfitCenterAbbreviation,
	pol_status_code,
	PremiumTransactionEnteredDate,
	OffsetOnsetCode,
	RunMonthAuditTransFlag1,
	AgeOfPolicy1,
	RateEffectiveDate,
	PremiumTransactionEffectiveDate1,
	LossesSubjectToDeductibleCode,
	BasisOfDeductibleCalculationCode
	FROM JNR_Premium_PMS_ManualPremium
),
SRT_Premium_mnsc AS (
	SELECT
	SourceSystem, 
	PremiumMasterCalculationID, 
	PremiumMasterRunDate, 
	StateRatingEffectiveDate, 
	WCRetrospectiveRatingIndicator, 
	ExperienceModificationFactor, 
	ExperienceModificationEffectiveDate, 
	PremiumMasterPremium, 
	BaseRate, 
	TypeBureauCode, 
	pol_sym, 
	pol_num, 
	pol_mod, 
	Pol_key AS pol_key, 
	CoverageType, 
	PremiumTransactionCode, 
	CoverageGUID, 
	RatingCoverageAKID, 
	Period_start_date, 
	Period_end_date, 
	AnyARDIndicator, 
	ExperienceRated, 
	TermType, 
	StateProvinceCode, 
	pol_eff_date, 
	pol_exp_date, 
	pol_cancellation_date, 
	pol_cancellation_ind, 
	InterstateRiskId, 
	pol_id, 
	fed_tax_id, 
	pol_term, 
	pol_ak_id, 
	InsuranceSegmentAbbreviation, 
	cust_role, 
	name, 
	addr_line_1, 
	city_name, 
	state_prov_code, 
	zip_postal_code, 
	ClassCode, 
	Exposure AS WrittenExposure, 
	DeductibleAmount, 
	InsuranceLine, 
	BalMinPremiumTotal, 
	ReasonAmendedCode, 
	StrategicProfitCenterAbbreviation, 
	pol_status_code, 
	PremiumTransactionEnteredDate, 
	OffsetOnsetCode, 
	RunMonthAuditTransFlag AS RunMonthAuditTransFlag4, 
	AgeOfPolicy AS AgeOfPolicy4, 
	RateEffectiveDate, 
	PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate4, 
	LossesSubjectToDeductibleCode, 
	BasisOfDeductibleCalculationCode
	FROM RTR_Premium_DCT_MN
	ORDER BY pol_key ASC
),
AGG_Premium_mnsc AS (
	SELECT
	pol_key,
	PremiumMasterPremium,
	-- *INF*: sum(PremiumMasterPremium)
	sum(PremiumMasterPremium) AS o_PolicyPremiumTotal
	FROM SRT_Premium_mnsc
	GROUP BY pol_key
),
JNR_Premium_mnsc AS (SELECT
	SRT_Premium_mnsc.SourceSystem, 
	SRT_Premium_mnsc.PremiumMasterCalculationID, 
	SRT_Premium_mnsc.PremiumMasterRunDate, 
	SRT_Premium_mnsc.StateRatingEffectiveDate, 
	SRT_Premium_mnsc.WCRetrospectiveRatingIndicator, 
	SRT_Premium_mnsc.ExperienceModificationFactor, 
	SRT_Premium_mnsc.ExperienceModificationEffectiveDate, 
	SRT_Premium_mnsc.PremiumMasterPremium, 
	SRT_Premium_mnsc.BaseRate, 
	SRT_Premium_mnsc.TypeBureauCode, 
	SRT_Premium_mnsc.pol_sym, 
	SRT_Premium_mnsc.pol_num, 
	SRT_Premium_mnsc.pol_mod, 
	SRT_Premium_mnsc.pol_key, 
	SRT_Premium_mnsc.CoverageType, 
	SRT_Premium_mnsc.PremiumTransactionCode, 
	SRT_Premium_mnsc.CoverageGUID, 
	SRT_Premium_mnsc.RatingCoverageAKID, 
	SRT_Premium_mnsc.Period_start_date, 
	SRT_Premium_mnsc.Period_end_date, 
	SRT_Premium_mnsc.AnyARDIndicator, 
	SRT_Premium_mnsc.ExperienceRated, 
	SRT_Premium_mnsc.TermType, 
	SRT_Premium_mnsc.StateProvinceCode, 
	SRT_Premium_mnsc.pol_eff_date, 
	SRT_Premium_mnsc.pol_exp_date, 
	SRT_Premium_mnsc.pol_cancellation_date, 
	SRT_Premium_mnsc.pol_cancellation_ind, 
	SRT_Premium_mnsc.InterstateRiskId, 
	SRT_Premium_mnsc.pol_id, 
	SRT_Premium_mnsc.fed_tax_id, 
	SRT_Premium_mnsc.pol_term, 
	SRT_Premium_mnsc.pol_ak_id, 
	SRT_Premium_mnsc.InsuranceSegmentAbbreviation, 
	SRT_Premium_mnsc.cust_role, 
	SRT_Premium_mnsc.name, 
	SRT_Premium_mnsc.addr_line_1, 
	SRT_Premium_mnsc.city_name, 
	SRT_Premium_mnsc.state_prov_code, 
	SRT_Premium_mnsc.zip_postal_code, 
	SRT_Premium_mnsc.ClassCode, 
	SRT_Premium_mnsc.WrittenExposure, 
	SRT_Premium_mnsc.DeductibleAmount, 
	SRT_Premium_mnsc.InsuranceLine, 
	SRT_Premium_mnsc.ManualPremiumInd, 
	SRT_Premium_mnsc.PolicyStateManualPremiumTotal, 
	SRT_Premium_mnsc.BalMinPremiumTotal, 
	SRT_Premium_mnsc.ReasonAmendedCode, 
	SRT_Premium_mnsc.StrategicProfitCenterAbbreviation, 
	SRT_Premium_mnsc.pol_status_code, 
	SRT_Premium_mnsc.PremiumTransactionEnteredDate, 
	SRT_Premium_mnsc.OffsetOnsetCode, 
	AGG_Premium_mnsc.pol_key AS pol_key1, 
	AGG_Premium_mnsc.o_PolicyPremiumTotal, 
	SRT_Premium_mnsc.RunMonthAuditTransFlag4, 
	SRT_Premium_mnsc.AgeOfPolicy4, 
	SRT_Premium_mnsc.RateEffectiveDate, 
	SRT_Premium_mnsc.PremiumTransactionEffectiveDate4, 
	SRT_Premium_mnsc.LossesSubjectToDeductibleCode, 
	SRT_Premium_mnsc.BasisOfDeductibleCalculationCode
	FROM SRT_Premium_mnsc
	LEFT OUTER JOIN AGG_Premium_mnsc
	ON AGG_Premium_mnsc.pol_key = SRT_Premium_mnsc.pol_key
),
EXP_Premium_mnsc AS (
	SELECT
	SourceSystem,
	'DCT' AS o_SourceSystem,
	PremiumMasterCalculationID,
	PremiumMasterRunDate,
	StateRatingEffectiveDate,
	WCRetrospectiveRatingIndicator,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	PremiumMasterPremium,
	BaseRate,
	TypeBureauCode,
	pol_sym,
	pol_num,
	pol_mod,
	pol_key,
	CoverageType,
	PremiumTransactionCode,
	CoverageGUID,
	RatingCoverageAKID,
	Period_start_date,
	Period_end_date,
	AnyARDIndicator,
	ExperienceRated,
	TermType,
	StateProvinceCode,
	pol_eff_date,
	pol_exp_date,
	pol_cancellation_date,
	pol_cancellation_ind,
	InterstateRiskId,
	pol_id,
	fed_tax_id,
	pol_term,
	pol_ak_id,
	InsuranceSegmentAbbreviation,
	cust_role,
	name,
	addr_line_1,
	city_name,
	state_prov_code,
	zip_postal_code,
	ClassCode,
	WrittenExposure,
	DeductibleAmount,
	InsuranceLine,
	o_PolicyPremiumTotal AS PolicyPremiumTotal,
	0 AS ManualPremiumInd,
	0 AS PolicyStateManualPremiumTotal,
	BalMinPremiumTotal,
	ReasonAmendedCode,
	StrategicProfitCenterAbbreviation,
	pol_status_code,
	PremiumTransactionEnteredDate,
	OffsetOnsetCode,
	RunMonthAuditTransFlag4,
	AgeOfPolicy4,
	RateEffectiveDate,
	PremiumTransactionEffectiveDate4,
	LossesSubjectToDeductibleCode,
	BasisOfDeductibleCalculationCode
	FROM JNR_Premium_mnsc
),
Union_ AS (
	SELECT o_SourceSystem AS SourceSystem, PremiumMasterCalculationID, PremiumMasterRunDate, StateRatingEffectiveDate, WCRetrospectiveRatingIndicator, ExperienceModificationFactor, ExperienceModificationEffectiveDate, PremiumMasterPremium, BaseRate, TypeBureauCode, pol_sym, pol_num, pol_mod, pol_key, CoverageType, PremiumTransactionCode, CoverageGUID, RatingCoverageAKID, Period_start_date, Period_end_date, AnyARDIndicator, ExperienceRated, TermType, StateProvinceCode, pol_eff_date, pol_exp_date, pol_cancellation_date, pol_cancellation_ind, InterstateRiskId, pol_id, fed_tax_id, pol_term, pol_ak_id, InsuranceSegmentAbbreviation, cust_role, name, addr_line_1, city_name, state_prov_code, zip_postal_code, ClassCode, WrittenExposure, DeductibleAmount, InsuranceLine, PolicyPremiumTotal, ManualPremiumInd, PolicyStateManualPremiumTotal, BalMinPremiumTotal, ReasonAmendedCode, StrategicProfitCenterAbbreviation, pol_status_code, PremiumTransactionEnteredDate, OffsetOnsetCode, RunMonthAuditTransFlag4 AS RunMonthAuditTransFlag1, AgeOfPolicy4 AS AgeOfPolicy1, RateEffectiveDate, PremiumTransactionEffectiveDate4 AS PremiumTransactionEffectiveDate1, LossesSubjectToDeductibleCode, BasisOfDeductibleCalculationCode
	FROM EXP_Premium_mnsc
	UNION
	SELECT SourceSystem, PremiumMasterCalculationID, PremiumMasterRunDate, StateRatingEffectiveDate, WCRetrospectiveRatingIndicator, ExperienceModificationFactor, ExperienceModificationEffectiveDate, PremiumMasterPremium, BaseRate, TypeBureauCode, pol_sym, pol_num, pol_mod, pol_key, CoverageType, PremiumTransactionCode, CoverageGUID, RatingCoverageAKID, Period_start_date, Period_end_date, AnyARDIndicator, ExperienceRated, TermType, StateProvinceCode, pol_eff_date, pol_exp_date, pol_cancellation_date, pol_cancellation_ind, InterstateRiskId, pol_id, fed_tax_id, pol_term, pol_ak_id, InsuranceSegmentAbbreviation, cust_role, name, addr_line_1, city_name, state_prov_code, zip_postal_code, ClassCode, WrittenExposure, DeductibleAmount, InsuranceLine, PolicyPremiumTotal, ManualPremiumInd, PolicyStateManualPremiumTotal, BalMinPremiumTotal, ReasonAmendedCode, StrategicProfitCenterAbbreviation, pol_status_code, PremiumTransactionEnteredDate, OffsetOnsetCode, RunMonthAuditTransFlag3 AS RunMonthAuditTransFlag1, AgeOfPolicy3 AS AgeOfPolicy1, RateEffectiveDate, PremiumTransactionEffectiveDate3 AS PremiumTransactionEffectiveDate1, LossesSubjectToDeductibleCode, BasisOfDeductibleCalculationCode
	FROM EXP_Premium_DCT
	UNION
	SELECT SourceSystem, PremiumMasterCalculationID, PremiumMasterRunDate, StateRatingEffectiveDate, WCRetrospectiveRatingIndicator, ExperienceModificationFactor, ExperienceModificationEffectiveDate, PremiumMasterPremium, BaseRate, TypeBureauCode, pol_sym, pol_num, pol_mod, pol_key, CoverageType, PremiumTransactionCode, CoverageGUID, RatingCoverageAKID, Period_start_date, Period_end_date, AnyARDIndicator, ExperienceRated, TermType, StateProvinceCode, pol_eff_date, pol_exp_date, pol_cancellation_date, pol_cancellation_ind, InterstateRiskId, pol_id, fed_tax_id, pol_term, pol_ak_id, InsuranceSegmentAbbreviation, cust_role, name, addr_line_1, city_name, state_prov_code, zip_postal_code, ClassCode, WrittenExposure, DeductibleAmount, InsuranceLine, PolicyPremiumTotal, ManualPremiumInd, PolicyStateManualPremiumTotal, BalMinPremiumTotal, ReasonAmendedCode, StrategicProfitCenterAbbreviation, pol_status_code, PremiumTransactionEnteredDate, OffsetOnsetCode, RunMonthAuditTransFlag1, AgeOfPolicy1, RateEffectiveDate, PremiumTransactionEffectiveDate1, LossesSubjectToDeductibleCode, BasisOfDeductibleCalculationCode
	FROM EXP_Premium_PMS
),
EXP_State AS (
	SELECT
	SourceSystem,
	PremiumMasterCalculationID,
	PremiumMasterRunDate,
	StateRatingEffectiveDate,
	WCRetrospectiveRatingIndicator,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	PremiumMasterPremium,
	BaseRate,
	TypeBureauCode,
	pol_sym,
	pol_num,
	pol_mod,
	pol_key,
	CoverageType,
	PremiumTransactionCode,
	CoverageGUID,
	RatingCoverageAKID,
	Period_start_date,
	Period_end_date,
	AnyARDIndicator,
	ExperienceRated,
	TermType,
	StateProvinceCode,
	pol_eff_date,
	pol_exp_date,
	pol_cancellation_date,
	pol_cancellation_ind,
	InterstateRiskId,
	pol_id,
	fed_tax_id,
	pol_term,
	pol_ak_id,
	InsuranceSegmentAbbreviation,
	cust_role,
	name,
	addr_line_1,
	city_name,
	state_prov_code,
	zip_postal_code,
	ClassCode,
	WrittenExposure,
	DeductibleAmount,
	InsuranceLine,
	PolicyPremiumTotal,
	ManualPremiumInd,
	PolicyStateManualPremiumTotal,
	BalMinPremiumTotal,
	ReasonAmendedCode,
	StrategicProfitCenterAbbreviation,
	pol_status_code,
	PremiumTransactionEnteredDate,
	OffsetOnsetCode,
	RunMonthAuditTransFlag1,
	AgeOfPolicy1,
	RateEffectiveDate,
	PremiumTransactionEffectiveDate1,
	LossesSubjectToDeductibleCode,
	BasisOfDeductibleCalculationCode
	FROM Union_
),
SRT_State_Detail AS (
	SELECT
	SourceSystem, 
	PremiumMasterCalculationID, 
	PremiumMasterRunDate, 
	StateRatingEffectiveDate, 
	WCRetrospectiveRatingIndicator, 
	ExperienceModificationFactor, 
	ExperienceModificationEffectiveDate, 
	PremiumMasterPremium, 
	BaseRate, 
	TypeBureauCode, 
	pol_sym, 
	pol_num, 
	pol_mod, 
	pol_key, 
	CoverageType, 
	PremiumTransactionCode, 
	CoverageGUID, 
	RatingCoverageAKID, 
	Period_start_date, 
	Period_end_date, 
	AnyARDIndicator, 
	ExperienceRated, 
	TermType, 
	StateProvinceCode, 
	pol_eff_date, 
	pol_exp_date, 
	pol_cancellation_date, 
	pol_cancellation_ind, 
	InterstateRiskId, 
	pol_id, 
	fed_tax_id, 
	pol_term, 
	pol_ak_id, 
	InsuranceSegmentAbbreviation, 
	cust_role, 
	name, 
	addr_line_1, 
	city_name, 
	state_prov_code, 
	zip_postal_code, 
	ClassCode, 
	WrittenExposure, 
	DeductibleAmount, 
	InsuranceLine, 
	PolicyPremiumTotal, 
	ManualPremiumInd, 
	PolicyStateManualPremiumTotal, 
	BalMinPremiumTotal, 
	ReasonAmendedCode, 
	StrategicProfitCenterAbbreviation, 
	pol_status_code, 
	PremiumTransactionEnteredDate, 
	OffsetOnsetCode, 
	RunMonthAuditTransFlag1, 
	AgeOfPolicy1, 
	RateEffectiveDate, 
	PremiumTransactionEffectiveDate1, 
	LossesSubjectToDeductibleCode, 
	BasisOfDeductibleCalculationCode
	FROM EXP_State
	ORDER BY pol_key ASC, StateProvinceCode ASC
),
FIL_State AS (
	SELECT
	SourceSystem, 
	pol_key, 
	PremiumTransactionCode, 
	StateProvinceCode, 
	pol_eff_date, 
	ClassCode, 
	PremiumTransactionEffectiveDate1
	FROM EXP_State
	WHERE ClassCode<>'0174' and SourceSystem='DCT'
),
SRT_State AS (
	SELECT
	SourceSystem, 
	pol_key, 
	StateProvinceCode, 
	PremiumTransactionCode, 
	pol_eff_date, 
	ClassCode, 
	PremiumTransactionEffectiveDate1
	FROM FIL_State
	ORDER BY pol_key DESC, StateProvinceCode DESC, pol_eff_date DESC, PremiumTransactionEffectiveDate1 DESC
),
AGG_State AS (
	SELECT
	SourceSystem,
	pol_key,
	StateProvinceCode,
	PremiumTransactionCode,
	-- *INF*: LAST(PremiumTransactionCode)
	LAST(PremiumTransactionCode) AS o_PremiumTransactionCode,
	pol_eff_date,
	ClassCode,
	PremiumTransactionEffectiveDate1,
	-- *INF*: MIN(PremiumTransactionEffectiveDate1)
	MIN(PremiumTransactionEffectiveDate1) AS o_PremiumTransactionEffectiveDate
	FROM SRT_State
	GROUP BY pol_key, StateProvinceCode, pol_eff_date
),
FIL_Transactions AS (
	SELECT
	StateProvinceCode, 
	pol_key, 
	PremiumTransactionCode, 
	o_PremiumTransactionCode, 
	pol_eff_date, 
	ClassCode, 
	o_PremiumTransactionEffectiveDate
	FROM AGG_State
	WHERE pol_eff_date<>o_PremiumTransactionEffectiveDate and o_PremiumTransactionCode='Endorse'
),
SRT_State_master AS (
	SELECT
	pol_key, 
	StateProvinceCode, 
	PremiumTransactionCode, 
	o_PremiumTransactionCode, 
	pol_eff_date, 
	ClassCode, 
	o_PremiumTransactionEffectiveDate
	FROM FIL_Transactions
	ORDER BY pol_key ASC, StateProvinceCode ASC
),
JNR_State AS (SELECT
	SRT_State_Detail.SourceSystem, 
	SRT_State_Detail.PremiumMasterCalculationID, 
	SRT_State_Detail.PremiumMasterRunDate, 
	SRT_State_Detail.StateRatingEffectiveDate, 
	SRT_State_Detail.WCRetrospectiveRatingIndicator, 
	SRT_State_Detail.ExperienceModificationFactor, 
	SRT_State_Detail.ExperienceModificationEffectiveDate, 
	SRT_State_Detail.PremiumMasterPremium, 
	SRT_State_Detail.BaseRate, 
	SRT_State_Detail.TypeBureauCode, 
	SRT_State_Detail.pol_sym, 
	SRT_State_Detail.pol_num, 
	SRT_State_Detail.pol_mod, 
	SRT_State_Detail.pol_key, 
	SRT_State_Detail.CoverageType, 
	SRT_State_Detail.PremiumTransactionCode, 
	SRT_State_Detail.CoverageGUID, 
	SRT_State_Detail.RatingCoverageAKID, 
	SRT_State_Detail.Period_start_date, 
	SRT_State_Detail.Period_end_date, 
	SRT_State_Detail.AnyARDIndicator, 
	SRT_State_Detail.ExperienceRated, 
	SRT_State_Detail.TermType, 
	SRT_State_Detail.StateProvinceCode, 
	SRT_State_Detail.pol_eff_date, 
	SRT_State_Detail.pol_exp_date, 
	SRT_State_Detail.pol_cancellation_date, 
	SRT_State_Detail.pol_cancellation_ind, 
	SRT_State_Detail.InterstateRiskId, 
	SRT_State_Detail.pol_id, 
	SRT_State_Detail.fed_tax_id, 
	SRT_State_Detail.pol_term, 
	SRT_State_Detail.pol_ak_id, 
	SRT_State_Detail.InsuranceSegmentAbbreviation, 
	SRT_State_Detail.cust_role, 
	SRT_State_Detail.name, 
	SRT_State_Detail.addr_line_1, 
	SRT_State_Detail.city_name, 
	SRT_State_Detail.state_prov_code, 
	SRT_State_Detail.zip_postal_code, 
	SRT_State_Detail.ClassCode, 
	SRT_State_Detail.WrittenExposure, 
	SRT_State_Detail.DeductibleAmount, 
	SRT_State_Detail.InsuranceLine, 
	SRT_State_Detail.PolicyPremiumTotal, 
	SRT_State_Detail.ManualPremiumInd, 
	SRT_State_Detail.PolicyStateManualPremiumTotal, 
	SRT_State_Detail.BalMinPremiumTotal, 
	SRT_State_Detail.ReasonAmendedCode, 
	SRT_State_Detail.StrategicProfitCenterAbbreviation, 
	SRT_State_Detail.pol_status_code, 
	SRT_State_Detail.PremiumTransactionEnteredDate, 
	SRT_State_Detail.OffsetOnsetCode, 
	SRT_State_Detail.RunMonthAuditTransFlag1, 
	SRT_State_Detail.AgeOfPolicy1, 
	SRT_State_Detail.RateEffectiveDate, 
	SRT_State_Detail.PremiumTransactionEffectiveDate1, 
	SRT_State_master.pol_key AS pol_key1, 
	SRT_State_master.StateProvinceCode AS StateProvinceCode1, 
	SRT_State_master.pol_eff_date AS pol_eff_date1, 
	SRT_State_master.o_PremiumTransactionEffectiveDate, 
	SRT_State_Detail.LossesSubjectToDeductibleCode, 
	SRT_State_Detail.BasisOfDeductibleCalculationCode
	FROM SRT_State_Detail
	LEFT OUTER JOIN SRT_State_master
	ON SRT_State_master.pol_key = SRT_State_Detail.pol_key AND SRT_State_master.StateProvinceCode = SRT_State_Detail.StateProvinceCode
),
EXPTRANS AS (
	SELECT
	SourceSystem,
	PremiumMasterCalculationID,
	PremiumMasterRunDate,
	o_PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate1,
	StateRatingEffectiveDate AS i_StateRatingEffectiveDate,
	-- *INF*: IIF(ISNULL(PremiumTransactionEffectiveDate1),i_StateRatingEffectiveDate,PremiumTransactionEffectiveDate1)
	IFF(
	    PremiumTransactionEffectiveDate1 IS NULL, i_StateRatingEffectiveDate,
	    PremiumTransactionEffectiveDate1
	) AS o_StateRatingEffectiveDate,
	WCRetrospectiveRatingIndicator,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	PremiumMasterPremium,
	BaseRate AS i_BaseRate,
	-- *INF*: DECODE(TRUE,
	-- SourceSystem='DCT' AND StateProvinceCode='48' AND ClassCode='9046',1-i_BaseRate,
	-- SourceSystem='DCT' AND StateProvinceCode<>'48' AND ClassCode='9046',0,i_BaseRate)
	DECODE(
	    TRUE,
	    SourceSystem = 'DCT' AND StateProvinceCode = '48' AND ClassCode = '9046', 1 - i_BaseRate,
	    SourceSystem = 'DCT' AND StateProvinceCode <> '48' AND ClassCode = '9046', 0,
	    i_BaseRate
	) AS v_BaseRate,
	-- *INF*: DECODE(TRUE, 
	-- SourceSystem='DCT' AND ClassCode='9046',ROUND(v_BaseRate,3),
	-- SUBSTR(ClassCode,1,4)='7709', 0,v_BaseRate)
	DECODE(
	    TRUE,
	    SourceSystem = 'DCT' AND ClassCode = '9046', ROUND(v_BaseRate, 3),
	    SUBSTR(ClassCode, 1, 4) = '7709', 0,
	    v_BaseRate
	) AS o_BaseRate,
	TypeBureauCode,
	pol_sym,
	pol_num,
	pol_mod,
	pol_key,
	CoverageType,
	PremiumTransactionCode,
	CoverageGUID,
	RatingCoverageAKID,
	Period_start_date,
	Period_end_date,
	AnyARDIndicator,
	ExperienceRated,
	TermType,
	StateProvinceCode,
	pol_eff_date,
	pol_exp_date,
	pol_cancellation_date,
	pol_cancellation_ind,
	InterstateRiskId,
	pol_id,
	fed_tax_id,
	pol_term,
	pol_ak_id,
	InsuranceSegmentAbbreviation,
	cust_role,
	name,
	addr_line_1,
	city_name,
	state_prov_code,
	zip_postal_code,
	ClassCode,
	WrittenExposure,
	-- *INF*: IIF(SUBSTR(ClassCode,1,4)='7709',0,WrittenExposure)
	IFF(SUBSTR(ClassCode, 1, 4) = '7709', 0, WrittenExposure) AS o_Exposure,
	DeductibleAmount,
	InsuranceLine,
	PolicyPremiumTotal,
	ManualPremiumInd,
	PolicyStateManualPremiumTotal,
	BalMinPremiumTotal,
	ReasonAmendedCode,
	StrategicProfitCenterAbbreviation,
	pol_status_code,
	PremiumTransactionEnteredDate,
	OffsetOnsetCode,
	RunMonthAuditTransFlag1,
	AgeOfPolicy1,
	RateEffectiveDate,
	LossesSubjectToDeductibleCode,
	BasisOfDeductibleCalculationCode
	FROM JNR_State
),
sort_PTID AS (
	SELECT
	pol_key AS Pol_key, 
	Period_start_date, 
	ExperienceModificationEffectiveDate, 
	PremiumMasterCalculationID, 
	AnyARDIndicator, 
	ExperienceRated, 
	TermType
	FROM EXPTRANS
	ORDER BY Pol_key ASC, Period_start_date ASC, ExperienceModificationEffectiveDate ASC, PremiumMasterCalculationID ASC
),
agg_latest_indicators AS (
	SELECT
	Pol_key,
	PremiumMasterCalculationID,
	AnyARDIndicator,
	ExperienceRated,
	TermType
	FROM sort_PTID
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Pol_key ORDER BY NULL) = 1
),
sort_pol_key AS (
	SELECT
	Pol_key, 
	AnyARDIndicator, 
	ExperienceRated, 
	TermType
	FROM agg_latest_indicators
	ORDER BY Pol_key ASC
),
sort_pol_key_to_join AS (
	SELECT
	workWcStatPremiumSourceID, 
	PremiumMasterCalculationID, 
	PremiumMasterRunDate, 
	o_StateRatingEffectiveDate AS StateRatingEffectiveDate, 
	WCRetrospectiveRatingIndicator, 
	ExperienceModificationFactor, 
	ExperienceModificationEffectiveDate, 
	PremiumMasterPremium, 
	o_BaseRate AS BaseRate, 
	TypeBureauCode, 
	pol_sym, 
	pol_num, 
	pol_mod, 
	StateProvinceCode, 
	pol_eff_date, 
	pol_exp_date, 
	pol_cancellation_date, 
	pol_cancellation_ind, 
	InterstateRiskId, 
	pol_id, 
	fed_tax_id, 
	pol_term, 
	pol_ak_id, 
	InsuranceSegmentAbbreviation, 
	cust_role, 
	name, 
	addr_line_1, 
	city_name, 
	state_prov_code, 
	zip_postal_code, 
	ClassCode, 
	o_Exposure AS Exposure, 
	DeductibleAmount, 
	InsuranceLine, 
	PolicyPremiumTotal, 
	ManualPremiumInd, 
	PolicyStateManualPremiumTotal, 
	BalMinPremiumTotal, 
	RateEffectiveDate, 
	SourceSystem, 
	ReasonAmendedCode, 
	StrategicProfitCenterAbbreviation, 
	pol_status_code, 
	PremiumTransactionEnteredDate, 
	OffsetOnsetCode, 
	pol_key AS Pol_key, 
	CoverageType, 
	PremiumTransactionCode, 
	CoverageGUID, 
	RatingCoverageAKID, 
	RunMonthAuditTransFlag1 AS RunMonthAuditTransFlag, 
	AgeOfPolicy1 AS AgeOfPolicy, 
	TermType, 
	Period_start_date, 
	Period_end_date, 
	AnyARDIndicator, 
	ExperienceRated, 
	LossesSubjectToDeductibleCode, 
	BasisOfDeductibleCalculationCode
	FROM EXPTRANS
	ORDER BY Pol_key ASC
),
join_indicators AS (SELECT
	sort_pol_key_to_join.PremiumMasterCalculationID, 
	sort_pol_key_to_join.PremiumMasterRunDate, 
	sort_pol_key_to_join.StateRatingEffectiveDate, 
	sort_pol_key_to_join.WCRetrospectiveRatingIndicator, 
	sort_pol_key_to_join.ExperienceModificationFactor, 
	sort_pol_key_to_join.ExperienceModificationEffectiveDate, 
	sort_pol_key_to_join.PremiumMasterPremium, 
	sort_pol_key_to_join.BaseRate, 
	sort_pol_key_to_join.TypeBureauCode, 
	sort_pol_key_to_join.pol_sym, 
	sort_pol_key_to_join.pol_num, 
	sort_pol_key_to_join.pol_mod, 
	sort_pol_key_to_join.StateProvinceCode, 
	sort_pol_key_to_join.pol_eff_date, 
	sort_pol_key_to_join.pol_exp_date, 
	sort_pol_key_to_join.pol_cancellation_date, 
	sort_pol_key_to_join.pol_cancellation_ind, 
	sort_pol_key_to_join.InterstateRiskId, 
	sort_pol_key_to_join.pol_id, 
	sort_pol_key_to_join.fed_tax_id, 
	sort_pol_key_to_join.pol_term, 
	sort_pol_key_to_join.pol_ak_id, 
	sort_pol_key_to_join.InsuranceSegmentAbbreviation, 
	sort_pol_key_to_join.cust_role, 
	sort_pol_key_to_join.name, 
	sort_pol_key_to_join.addr_line_1, 
	sort_pol_key_to_join.city_name, 
	sort_pol_key_to_join.state_prov_code, 
	sort_pol_key_to_join.zip_postal_code, 
	sort_pol_key_to_join.ClassCode, 
	sort_pol_key_to_join.Exposure, 
	sort_pol_key_to_join.DeductibleAmount, 
	sort_pol_key_to_join.InsuranceLine, 
	sort_pol_key_to_join.SourceSystem, 
	sort_pol_key_to_join.ReasonAmendedCode, 
	sort_pol_key_to_join.StrategicProfitCenterAbbreviation, 
	sort_pol_key_to_join.pol_status_code, 
	sort_pol_key_to_join.RunMonthAuditTransFlag, 
	sort_pol_key_to_join.AgeOfPolicy, 
	sort_pol_key_to_join.PolicyPremiumTotal, 
	sort_pol_key_to_join.ManualPremiumInd, 
	sort_pol_key_to_join.PolicyStateManualPremiumTotal, 
	sort_pol_key_to_join.BalMinPremiumTotal, 
	sort_pol_key_to_join.RateEffectiveDate, 
	sort_pol_key_to_join.PremiumTransactionEnteredDate, 
	sort_pol_key_to_join.OffsetOnsetCode, 
	sort_pol_key_to_join.Pol_key, 
	sort_pol_key_to_join.CoverageType, 
	sort_pol_key_to_join.PremiumTransactionCode, 
	sort_pol_key_to_join.CoverageGUID, 
	sort_pol_key_to_join.RatingCoverageAKID, 
	sort_pol_key.TermType, 
	sort_pol_key_to_join.Period_start_date, 
	sort_pol_key_to_join.Period_end_date, 
	sort_pol_key.Pol_key AS Pol_key1, 
	sort_pol_key.AnyARDIndicator, 
	sort_pol_key.ExperienceRated, 
	sort_pol_key_to_join.LossesSubjectToDeductibleCode, 
	sort_pol_key_to_join.BasisOfDeductibleCalculationCode
	FROM sort_pol_key_to_join
	INNER JOIN sort_pol_key
	ON sort_pol_key.Pol_key = sort_pol_key_to_join.Pol_key
),
sort_CovGUID AS (
	SELECT
	PremiumMasterCalculationID, 
	StateRatingEffectiveDate, 
	WCRetrospectiveRatingIndicator, 
	ExperienceModificationFactor, 
	Pol_key, 
	ClassCode, 
	ExperienceModificationEffectiveDate, 
	PremiumMasterPremium, 
	BaseRate, 
	TypeBureauCode, 
	pol_sym, 
	pol_num, 
	pol_mod, 
	StateProvinceCode, 
	CoverageGUID, 
	pol_eff_date, 
	pol_exp_date, 
	pol_cancellation_date, 
	pol_cancellation_ind, 
	PremiumMasterRunDate, 
	InterstateRiskId, 
	pol_id, 
	fed_tax_id, 
	pol_term, 
	pol_ak_id, 
	InsuranceSegmentAbbreviation, 
	cust_role, 
	name, 
	addr_line_1, 
	city_name, 
	state_prov_code, 
	zip_postal_code, 
	Exposure, 
	DeductibleAmount, 
	InsuranceLine, 
	SourceSystem, 
	ReasonAmendedCode, 
	StrategicProfitCenterAbbreviation, 
	pol_status_code, 
	RunMonthAuditTransFlag, 
	AgeOfPolicy, 
	PolicyPremiumTotal, 
	ManualPremiumInd, 
	PolicyStateManualPremiumTotal, 
	BalMinPremiumTotal, 
	RateEffectiveDate, 
	PremiumTransactionEnteredDate, 
	OffsetOnsetCode, 
	CoverageType, 
	PremiumTransactionCode, 
	RatingCoverageAKID, 
	TermType, 
	Period_start_date, 
	Period_end_date, 
	AnyARDIndicator, 
	ExperienceRated, 
	LossesSubjectToDeductibleCode, 
	BasisOfDeductibleCalculationCode
	FROM join_indicators
	ORDER BY Pol_key ASC, ClassCode ASC, TypeBureauCode ASC, StateProvinceCode ASC, CoverageGUID ASC, pol_eff_date ASC, pol_exp_date ASC, PremiumMasterRunDate ASC, PremiumTransactionEnteredDate ASC, OffsetOnsetCode ASC
),
Filter_NonDCTandOffsets AS (
	SELECT
	Pol_key, 
	ClassCode, 
	TypeBureauCode, 
	StateProvinceCode, 
	CoverageGUID, 
	pol_eff_date, 
	pol_exp_date, 
	PremiumMasterRunDate, 
	ExperienceModificationFactor, 
	ExperienceModificationEffectiveDate, 
	BaseRate, 
	PremiumTransactionEnteredDate, 
	OffsetOnsetCode, 
	SourceSystem
	FROM sort_CovGUID
	WHERE (OffsetOnsetCode!='Offset' OR ISNULL(OffsetOnsetCode)) and SourceSystem='DCT'
),
Agg_Exp_mod AS (
	SELECT
	Pol_key,
	ClassCode,
	TypeBureauCode,
	StateProvinceCode,
	CoverageGUID,
	pol_eff_date,
	pol_exp_date,
	PremiumMasterRunDate,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	BaseRate,
	PremiumTransactionEnteredDate
	FROM Filter_NonDCTandOffsets
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Pol_key, ClassCode, TypeBureauCode, StateProvinceCode, CoverageGUID, pol_eff_date, pol_exp_date ORDER BY NULL) = 1
),
Join_Org_Exp_Mod AS (SELECT
	sort_CovGUID.PremiumMasterCalculationID, 
	sort_CovGUID.PremiumMasterRunDate, 
	sort_CovGUID.StateRatingEffectiveDate, 
	sort_CovGUID.WCRetrospectiveRatingIndicator, 
	Agg_Exp_mod.ExperienceModificationFactor AS ExperienceModificationFactor_DCT, 
	Agg_Exp_mod.ExperienceModificationEffectiveDate AS ExperienceModificationEffectiveDate_DCT, 
	sort_CovGUID.PremiumMasterPremium, 
	sort_CovGUID.TypeBureauCode, 
	sort_CovGUID.pol_sym, 
	sort_CovGUID.pol_num, 
	sort_CovGUID.pol_mod, 
	sort_CovGUID.StateProvinceCode, 
	sort_CovGUID.pol_eff_date, 
	sort_CovGUID.pol_exp_date, 
	sort_CovGUID.pol_cancellation_date, 
	sort_CovGUID.pol_cancellation_ind, 
	sort_CovGUID.InterstateRiskId, 
	sort_CovGUID.pol_id, 
	sort_CovGUID.fed_tax_id, 
	sort_CovGUID.pol_term, 
	sort_CovGUID.pol_ak_id, 
	sort_CovGUID.InsuranceSegmentAbbreviation, 
	sort_CovGUID.cust_role, 
	sort_CovGUID.name, 
	sort_CovGUID.addr_line_1, 
	sort_CovGUID.city_name, 
	sort_CovGUID.state_prov_code, 
	sort_CovGUID.zip_postal_code, 
	sort_CovGUID.ClassCode, 
	sort_CovGUID.Exposure, 
	sort_CovGUID.DeductibleAmount, 
	sort_CovGUID.InsuranceLine, 
	sort_CovGUID.SourceSystem, 
	sort_CovGUID.ReasonAmendedCode, 
	sort_CovGUID.StrategicProfitCenterAbbreviation, 
	sort_CovGUID.pol_status_code, 
	sort_CovGUID.RunMonthAuditTransFlag, 
	sort_CovGUID.AgeOfPolicy, 
	sort_CovGUID.PolicyPremiumTotal, 
	sort_CovGUID.ManualPremiumInd, 
	sort_CovGUID.PolicyStateManualPremiumTotal, 
	sort_CovGUID.BalMinPremiumTotal, 
	sort_CovGUID.RateEffectiveDate, 
	sort_CovGUID.PremiumTransactionEnteredDate, 
	sort_CovGUID.OffsetOnsetCode, 
	sort_CovGUID.Pol_key, 
	sort_CovGUID.CoverageType, 
	sort_CovGUID.PremiumTransactionCode, 
	sort_CovGUID.CoverageGUID, 
	sort_CovGUID.RatingCoverageAKID, 
	sort_CovGUID.TermType, 
	sort_CovGUID.Period_start_date, 
	sort_CovGUID.Period_end_date, 
	sort_CovGUID.AnyARDIndicator, 
	sort_CovGUID.ExperienceRated, 
	Agg_Exp_mod.Pol_key AS Pol_key1, 
	Agg_Exp_mod.ClassCode AS ClassCode1, 
	Agg_Exp_mod.CoverageGUID AS CoverageGUID1, 
	Agg_Exp_mod.BaseRate AS BaseRate_DCT, 
	Agg_Exp_mod.TypeBureauCode AS TypeBureauCode1, 
	Agg_Exp_mod.StateProvinceCode AS StateProvinceCode1, 
	sort_CovGUID.ExperienceModificationFactor AS ExperienceModificationFactor_PMS, 
	sort_CovGUID.ExperienceModificationEffectiveDate AS ExperienceModificationEffectiveDate_PMS, 
	sort_CovGUID.BaseRate AS BaseRate_PMS, 
	sort_CovGUID.LossesSubjectToDeductibleCode, 
	sort_CovGUID.BasisOfDeductibleCalculationCode
	FROM sort_CovGUID
	LEFT OUTER JOIN Agg_Exp_mod
	ON Agg_Exp_mod.Pol_key = sort_CovGUID.Pol_key AND Agg_Exp_mod.ClassCode = sort_CovGUID.ClassCode AND Agg_Exp_mod.TypeBureauCode = sort_CovGUID.TypeBureauCode AND Agg_Exp_mod.StateProvinceCode = sort_CovGUID.StateProvinceCode AND Agg_Exp_mod.CoverageGUID = sort_CovGUID.CoverageGUID
),
Exp_BaseRate_ExpMod AS (
	SELECT
	PremiumMasterCalculationID,
	PremiumMasterRunDate,
	StateRatingEffectiveDate,
	WCRetrospectiveRatingIndicator,
	ExperienceModificationEffectiveDate_DCT,
	PremiumMasterPremium,
	BaseRate_DCT,
	pol_sym,
	Pol_key,
	TypeBureauCode,
	StateProvinceCode,
	pol_eff_date,
	pol_exp_date,
	ClassCode,
	ExperienceModificationFactor_DCT,
	pol_num,
	pol_mod,
	pol_cancellation_date,
	pol_cancellation_ind,
	InterstateRiskId,
	pol_id,
	fed_tax_id,
	pol_term,
	pol_ak_id,
	InsuranceSegmentAbbreviation,
	cust_role,
	name,
	addr_line_1,
	city_name,
	state_prov_code,
	zip_postal_code,
	Exposure,
	DeductibleAmount,
	InsuranceLine,
	SourceSystem,
	ReasonAmendedCode,
	StrategicProfitCenterAbbreviation,
	pol_status_code,
	RunMonthAuditTransFlag,
	AgeOfPolicy,
	PolicyPremiumTotal,
	ManualPremiumInd,
	PolicyStateManualPremiumTotal,
	BalMinPremiumTotal,
	RateEffectiveDate,
	PremiumTransactionEnteredDate,
	OffsetOnsetCode,
	CoverageType,
	PremiumTransactionCode,
	CoverageGUID,
	RatingCoverageAKID,
	TermType,
	Period_start_date,
	Period_end_date,
	AnyARDIndicator,
	ExperienceRated,
	ExperienceModificationFactor_PMS,
	ExperienceModificationEffectiveDate_PMS,
	BaseRate_PMS,
	-- *INF*: iif(SourceSystem='DCT',ExperienceModificationFactor_DCT,ExperienceModificationFactor_PMS)
	IFF(
	    SourceSystem = 'DCT', ExperienceModificationFactor_DCT, ExperienceModificationFactor_PMS
	) AS O_ExperienceModificationFactor,
	-- *INF*: iif(SourceSystem='DCT',ExperienceModificationEffectiveDate_DCT,ExperienceModificationEffectiveDate_PMS)
	IFF(
	    SourceSystem = 'DCT', ExperienceModificationEffectiveDate_DCT,
	    ExperienceModificationEffectiveDate_PMS
	) AS O_ExperienceModificationEffectiveDate,
	-- *INF*: iif(SourceSystem='DCT',BaseRate_DCT,BaseRate_PMS)
	IFF(SourceSystem = 'DCT', BaseRate_DCT, BaseRate_PMS) AS O_BaseRate,
	LossesSubjectToDeductibleCode,
	BasisOfDeductibleCalculationCode
	FROM Join_Org_Exp_Mod
),
sort_Class_Exp_Mod AS (
	SELECT
	PremiumMasterCalculationID, 
	PremiumMasterRunDate, 
	StateRatingEffectiveDate, 
	WCRetrospectiveRatingIndicator, 
	O_ExperienceModificationEffectiveDate AS ExperienceModificationEffectiveDate, 
	PremiumMasterPremium, 
	O_BaseRate AS BaseRate, 
	pol_sym, 
	Pol_key, 
	TypeBureauCode, 
	StateProvinceCode, 
	pol_eff_date, 
	pol_exp_date, 
	ClassCode, 
	O_ExperienceModificationFactor AS ExperienceModificationFactor, 
	pol_num, 
	pol_mod, 
	pol_cancellation_date, 
	pol_cancellation_ind, 
	InterstateRiskId, 
	pol_id, 
	fed_tax_id, 
	pol_term, 
	pol_ak_id, 
	InsuranceSegmentAbbreviation, 
	cust_role, 
	name, 
	addr_line_1, 
	city_name, 
	state_prov_code, 
	zip_postal_code, 
	Exposure, 
	DeductibleAmount, 
	InsuranceLine, 
	SourceSystem, 
	ReasonAmendedCode, 
	StrategicProfitCenterAbbreviation, 
	pol_status_code, 
	RunMonthAuditTransFlag, 
	AgeOfPolicy, 
	PolicyPremiumTotal, 
	ManualPremiumInd, 
	PolicyStateManualPremiumTotal, 
	BalMinPremiumTotal, 
	RateEffectiveDate, 
	PremiumTransactionEnteredDate, 
	OffsetOnsetCode, 
	CoverageType, 
	PremiumTransactionCode, 
	CoverageGUID, 
	RatingCoverageAKID, 
	TermType, 
	Period_start_date, 
	Period_end_date, 
	AnyARDIndicator, 
	ExperienceRated, 
	LossesSubjectToDeductibleCode, 
	BasisOfDeductibleCalculationCode
	FROM Exp_BaseRate_ExpMod
	ORDER BY Pol_key ASC, TypeBureauCode ASC, StateProvinceCode ASC, pol_eff_date ASC, pol_exp_date ASC, ClassCode ASC, ExperienceModificationFactor ASC
),
Fil_DCT_data AS (
	SELECT
	Pol_key, 
	TypeBureauCode, 
	StateProvinceCode, 
	pol_eff_date, 
	pol_exp_date, 
	ClassCode, 
	ExperienceModificationFactor, 
	ExperienceModificationEffectiveDate, 
	SourceSystem
	FROM sort_Class_Exp_Mod
	WHERE SourceSystem='DCT'
),
agg_exp_mod_eff_date AS (
	SELECT
	Pol_key AS Pol_key1,
	TypeBureauCode,
	StateProvinceCode,
	pol_eff_date,
	pol_exp_date,
	ClassCode,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	-- *INF*: min(ExperienceModificationEffectiveDate)
	min(ExperienceModificationEffectiveDate) AS ExperienceModificationEffectiveDate_out
	FROM Fil_DCT_data
	GROUP BY Pol_key1, TypeBureauCode, StateProvinceCode, pol_eff_date, pol_exp_date, ClassCode, ExperienceModificationFactor
),
join_exp_Mod_eff_date AS (SELECT
	sort_Class_Exp_Mod.PremiumMasterCalculationID, 
	sort_Class_Exp_Mod.PremiumMasterRunDate, 
	sort_Class_Exp_Mod.StateRatingEffectiveDate, 
	sort_Class_Exp_Mod.WCRetrospectiveRatingIndicator, 
	sort_Class_Exp_Mod.ExperienceModificationFactor, 
	agg_exp_mod_eff_date.ExperienceModificationEffectiveDate_out AS ExperienceModificationEffectiveDate_DCT, 
	sort_Class_Exp_Mod.PremiumMasterPremium, 
	sort_Class_Exp_Mod.BaseRate, 
	sort_Class_Exp_Mod.TypeBureauCode, 
	sort_Class_Exp_Mod.pol_sym, 
	sort_Class_Exp_Mod.pol_num, 
	sort_Class_Exp_Mod.pol_mod, 
	sort_Class_Exp_Mod.StateProvinceCode, 
	sort_Class_Exp_Mod.pol_eff_date, 
	sort_Class_Exp_Mod.pol_exp_date, 
	sort_Class_Exp_Mod.pol_cancellation_date, 
	sort_Class_Exp_Mod.pol_cancellation_ind, 
	sort_Class_Exp_Mod.InterstateRiskId, 
	sort_Class_Exp_Mod.pol_id, 
	sort_Class_Exp_Mod.fed_tax_id, 
	sort_Class_Exp_Mod.pol_term, 
	sort_Class_Exp_Mod.pol_ak_id, 
	sort_Class_Exp_Mod.InsuranceSegmentAbbreviation, 
	sort_Class_Exp_Mod.cust_role, 
	sort_Class_Exp_Mod.name, 
	sort_Class_Exp_Mod.addr_line_1, 
	sort_Class_Exp_Mod.city_name, 
	sort_Class_Exp_Mod.state_prov_code, 
	sort_Class_Exp_Mod.zip_postal_code, 
	sort_Class_Exp_Mod.ClassCode, 
	sort_Class_Exp_Mod.Exposure, 
	sort_Class_Exp_Mod.DeductibleAmount, 
	sort_Class_Exp_Mod.InsuranceLine, 
	sort_Class_Exp_Mod.SourceSystem, 
	sort_Class_Exp_Mod.ReasonAmendedCode, 
	sort_Class_Exp_Mod.StrategicProfitCenterAbbreviation, 
	sort_Class_Exp_Mod.pol_status_code, 
	sort_Class_Exp_Mod.RunMonthAuditTransFlag, 
	sort_Class_Exp_Mod.AgeOfPolicy, 
	sort_Class_Exp_Mod.PolicyPremiumTotal, 
	sort_Class_Exp_Mod.ManualPremiumInd, 
	sort_Class_Exp_Mod.PolicyStateManualPremiumTotal, 
	sort_Class_Exp_Mod.BalMinPremiumTotal, 
	sort_Class_Exp_Mod.RateEffectiveDate, 
	sort_Class_Exp_Mod.PremiumTransactionEnteredDate, 
	sort_Class_Exp_Mod.OffsetOnsetCode, 
	sort_Class_Exp_Mod.Pol_key, 
	sort_Class_Exp_Mod.CoverageType, 
	sort_Class_Exp_Mod.PremiumTransactionCode, 
	sort_Class_Exp_Mod.CoverageGUID, 
	sort_Class_Exp_Mod.RatingCoverageAKID, 
	sort_Class_Exp_Mod.TermType, 
	sort_Class_Exp_Mod.Period_start_date, 
	sort_Class_Exp_Mod.Period_end_date, 
	sort_Class_Exp_Mod.AnyARDIndicator, 
	sort_Class_Exp_Mod.ExperienceRated, 
	agg_exp_mod_eff_date.Pol_key1, 
	agg_exp_mod_eff_date.TypeBureauCode AS TypeBureauCode1, 
	agg_exp_mod_eff_date.StateProvinceCode AS StateProvinceCode1, 
	agg_exp_mod_eff_date.pol_eff_date AS pol_eff_date1, 
	agg_exp_mod_eff_date.pol_exp_date AS pol_exp_date1, 
	agg_exp_mod_eff_date.ClassCode AS ClassCode1, 
	agg_exp_mod_eff_date.ExperienceModificationFactor AS ExperienceModificationFactor1, 
	sort_Class_Exp_Mod.ExperienceModificationEffectiveDate AS ExperienceModificationEffectiveDate_PMS, 
	sort_Class_Exp_Mod.LossesSubjectToDeductibleCode, 
	sort_Class_Exp_Mod.BasisOfDeductibleCalculationCode
	FROM sort_Class_Exp_Mod
	LEFT OUTER JOIN agg_exp_mod_eff_date
	ON agg_exp_mod_eff_date.Pol_key1 = sort_Class_Exp_Mod.Pol_key AND agg_exp_mod_eff_date.TypeBureauCode = sort_Class_Exp_Mod.TypeBureauCode AND agg_exp_mod_eff_date.StateProvinceCode = sort_Class_Exp_Mod.StateProvinceCode AND agg_exp_mod_eff_date.pol_eff_date = sort_Class_Exp_Mod.pol_eff_date AND agg_exp_mod_eff_date.pol_exp_date = sort_Class_Exp_Mod.pol_exp_date AND agg_exp_mod_eff_date.ClassCode = sort_Class_Exp_Mod.ClassCode AND agg_exp_mod_eff_date.ExperienceModificationFactor = sort_Class_Exp_Mod.ExperienceModificationFactor
),
LKP_PolicyAudit AS (
	SELECT
	PolicyAKId,
	NoncomplianceofWCPoolAudit,
	i_PolicyAKid
	FROM (
		SELECT 
			PolicyAKId,
			NoncomplianceofWCPoolAudit,
			i_PolicyAKid
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyAudit
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKId ORDER BY PolicyAKId) = 1
),
EXP_Premium AS (
	SELECT
	join_exp_Mod_eff_date.PremiumMasterCalculationID AS i_PremiumMasterCalculationID,
	join_exp_Mod_eff_date.PremiumMasterRunDate AS i_PremiumMasterRunDate,
	join_exp_Mod_eff_date.StateRatingEffectiveDate AS i_StateRatingEffectiveDate,
	join_exp_Mod_eff_date.WCRetrospectiveRatingIndicator AS i_WCRetrospectiveRatingIndicator,
	join_exp_Mod_eff_date.ExperienceModificationFactor AS i_WorkersCompensationExperienceModificationFactor,
	join_exp_Mod_eff_date.ExperienceModificationEffectiveDate_DCT AS i_WCExperienceModificationEffectiveDate,
	join_exp_Mod_eff_date.PremiumMasterPremium AS i_PremiumMasterPremium,
	join_exp_Mod_eff_date.BaseRate AS i_WCBaseRate,
	join_exp_Mod_eff_date.TypeBureauCode AS i_TypeBureauCode,
	join_exp_Mod_eff_date.pol_sym AS i_pol_sym,
	join_exp_Mod_eff_date.pol_num AS i_pol_num,
	join_exp_Mod_eff_date.pol_mod AS i_pol_mod,
	join_exp_Mod_eff_date.StateProvinceCode AS i_StateProvinceCode,
	join_exp_Mod_eff_date.pol_eff_date AS i_pol_eff_date,
	join_exp_Mod_eff_date.pol_exp_date AS i_pol_exp_date,
	join_exp_Mod_eff_date.pol_cancellation_date AS i_pol_cancellation_date,
	join_exp_Mod_eff_date.pol_cancellation_ind AS i_pol_cancellation_ind,
	join_exp_Mod_eff_date.InterstateRiskId AS i_WCInterStateRiskId,
	join_exp_Mod_eff_date.pol_id AS i_pol_id,
	join_exp_Mod_eff_date.fed_tax_id AS i_fed_tax_id,
	join_exp_Mod_eff_date.pol_term AS i_pol_term,
	join_exp_Mod_eff_date.pol_ak_id AS i_pol_ak_id,
	join_exp_Mod_eff_date.InsuranceSegmentAbbreviation AS i_InsuranceSegmentAbbreviation,
	join_exp_Mod_eff_date.cust_role AS i_cust_role,
	join_exp_Mod_eff_date.name AS i_name,
	join_exp_Mod_eff_date.addr_line_1 AS i_addr_line_1,
	join_exp_Mod_eff_date.city_name AS i_city_name,
	join_exp_Mod_eff_date.state_prov_code AS i_state_prov_code,
	join_exp_Mod_eff_date.zip_postal_code AS i_zip_postal_code,
	join_exp_Mod_eff_date.ClassCode AS i_ClassCode,
	join_exp_Mod_eff_date.Exposure AS i_Exposure,
	join_exp_Mod_eff_date.DeductibleAmount AS i_DeductibleAmount,
	join_exp_Mod_eff_date.InsuranceLine AS i_InsuranceLine,
	join_exp_Mod_eff_date.SourceSystem AS i_SourceSystem,
	join_exp_Mod_eff_date.ReasonAmendedCode AS i_ReasonAmendedCode,
	join_exp_Mod_eff_date.StrategicProfitCenterAbbreviation AS i_StrategicProfitCenterAbbreviation,
	join_exp_Mod_eff_date.pol_status_code AS i_pol_status_code,
	LKP_PolicyAudit.NoncomplianceofWCPoolAudit AS i_NoncomplianceofWCPoolAudit,
	-- *INF*: RTRIM(LTRIM(i_DeductibleAmount))
	RTRIM(LTRIM(i_DeductibleAmount)) AS v_DeductibleAmount,
	-- *INF*: IIF(LTRIM(RTRIM(i_addr_line_1))='N/A', '', LTRIM(RTRIM(i_addr_line_1)))
	IFF(LTRIM(RTRIM(i_addr_line_1)) = 'N/A', '', LTRIM(RTRIM(i_addr_line_1))) AS v_addr_line_1,
	-- *INF*: IIF(LTRIM(RTRIM(i_city_name))='N/A', '', LTRIM(RTRIM(i_city_name)))
	IFF(LTRIM(RTRIM(i_city_name)) = 'N/A', '', LTRIM(RTRIM(i_city_name))) AS v_city_name,
	-- *INF*: IIF(LTRIM(RTRIM(i_state_prov_code))='N/A', '', LTRIM(RTRIM(i_state_prov_code)))
	IFF(LTRIM(RTRIM(i_state_prov_code)) = 'N/A', '', LTRIM(RTRIM(i_state_prov_code))) AS v_state_prov_code,
	i_StateProvinceCode AS v_StateProvinceCode,
	-- *INF*: IIF(LTRIM(RTRIM(i_zip_postal_code))='N/A', '', LTRIM(RTRIM(i_zip_postal_code)))
	IFF(LTRIM(RTRIM(i_zip_postal_code)) = 'N/A', '', LTRIM(RTRIM(i_zip_postal_code))) AS v_zip_postal_code,
	i_pol_sym || i_pol_num || i_pol_mod AS v_PolicyKey,
	-- *INF*: :LKP.LKP_AUDITSCHEDULE(v_PolicyKey, i_InsuranceLine, i_pol_eff_date)
	LKP_AUDITSCHEDULE_v_PolicyKey_i_InsuranceLine_i_pol_eff_date.AuditStatus AS v_AuditStatus,
	-- *INF*: IIF(i_NoncomplianceofWCPoolAudit = 'T','U',DECODE(TRUE,
	--   UPPER(i_ReasonAmendedCode) = 'ESTIMATED',
	--     DECODE(TRUE,
	--       (IN(i_StateProvinceCode, '21', '48') OR UPPER(i_InsuranceSegmentAbbreviation)='POOL'),
	--         'U',
	--         'Y'
	--       ),
	--   IN(UPPER(i_ReasonAmendedCode),'CANCELLATION','SEEDETAIL'),
	--     'N',
	--   (UPPER(i_StrategicProfitCenterAbbreviation)='ARGENT' AND ISNULL(i_ReasonAmendedCode) AND UPPER(i_InsuranceSegmentAbbreviation)='CL'),
	--     'Y',
	--   (UPPER(i_StrategicProfitCenterAbbreviation)='WB - CL' AND ISNULL(i_ReasonAmendedCode) AND UPPER(i_InsuranceSegmentAbbreviation)='POOL' AND IN(i_pol_status_code,'N','C')),
	--     'Y',
	--   (UPPER(i_StrategicProfitCenterAbbreviation)='WB - CL' AND ISNULL(i_ReasonAmendedCode) AND UPPER(i_InsuranceSegmentAbbreviation)='CL' AND i_pol_status_code ='N'),
	--     'Y',
	--     'N'
	-- ))
	IFF(
	    i_NoncomplianceofWCPoolAudit = 'T', 'U',
	    DECODE(
	        TRUE,
	        UPPER(i_ReasonAmendedCode) = 'ESTIMATED', DECODE(
	            TRUE,
	            (i_StateProvinceCode IN ('21','48')
	    or UPPER(i_InsuranceSegmentAbbreviation) = 'POOL'), 'U',
	            'Y'
	        ),
	        UPPER(i_ReasonAmendedCode) IN ('CANCELLATION','SEEDETAIL'), 'N',
	        (UPPER(i_StrategicProfitCenterAbbreviation) = 'ARGENT'
	    and i_ReasonAmendedCode IS NULL
	    and UPPER(i_InsuranceSegmentAbbreviation) = 'CL'), 'Y',
	        (UPPER(i_StrategicProfitCenterAbbreviation) = 'WB - CL'
	    and i_ReasonAmendedCode IS NULL
	    and UPPER(i_InsuranceSegmentAbbreviation) = 'POOL'
	    and i_pol_status_code IN ('N','C')), 'Y',
	        (UPPER(i_StrategicProfitCenterAbbreviation) = 'WB - CL'
	    and i_ReasonAmendedCode IS NULL
	    and UPPER(i_InsuranceSegmentAbbreviation) = 'CL'
	    and i_pol_status_code = 'N'), 'Y',
	        'N'
	    )
	) AS v_DCT_EstimatedAuditCode,
	-- *INF*: IIF(ISNULL(:LKP.LKP_MULTISTATEPOLICY(i_pol_ak_id)), '0', '1')
	IFF(LKP_MULTISTATEPOLICY_i_pol_ak_id.PolicyAKId IS NULL, '0', '1') AS v_MultistatePolicyIndicator,
	-- *INF*: TRUNC(i_pol_eff_date, 'DD')
	CAST(TRUNC(i_pol_eff_date, 'DAY') AS TIMESTAMP_NTZ(0)) AS v_PolicyEffectiveDate,
	-- *INF*: :LKP.LKP_SUPClassificationWorkersCompensation(i_ClassCode, v_StateProvinceCode,v_PolicyEffectiveDate)
	LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_i_ClassCode_v_StateProvinceCode_v_PolicyEffectiveDate.SubjecttoExperienceModificationClassIndicator AS v_ExpModZeroByState,
	-- *INF*: iif ( not isnull(v_ExpModZeroByState) , substr(v_ExpModZeroByState,1,1) , null)
	IFF(v_ExpModZeroByState IS NOT NULL, substr(v_ExpModZeroByState, 1, 1), null) AS v_SubjecttoExperienceModificationClassIndicator_State,
	-- *INF*: iif ( not isnull(v_ExpModZeroByState) , substr(v_ExpModZeroByState,2,1) , null)
	IFF(v_ExpModZeroByState IS NOT NULL, substr(v_ExpModZeroByState, 2, 1), null) AS v_ExperienceModificationClassIndicator_State,
	-- *INF*: Decode( true, (not isnull(v_ExpModZeroByState)  and
	-- ((v_ExperienceModificationClassIndicator_State = 'N' ) AND (v_SubjecttoExperienceModificationClassIndicator_State =  'N'))) ,'0',
	-- (not isnull(v_ExpModZeroByState)  and
	-- ((v_ExperienceModificationClassIndicator_State = 'Y' ) OR  (v_SubjecttoExperienceModificationClassIndicator_State =  'Y'))) ,'1', '2')
	-- 
	-- -- 0 exp mod not appliable 
	-- --1 exp mod appliable 
	-- --2 no match found for classcode & statecode combination 
	-- 
	-- 
	Decode(
	    true,
	    (v_ExpModZeroByState IS NULL and ((v_ExperienceModificationClassIndicator_State = 'N') AND (v_SubjecttoExperienceModificationClassIndicator_State = 'NOT N'))), '0',
	    (v_ExpModZeroByState IS NULL and ((v_ExperienceModificationClassIndicator_State = 'Y') OR (v_SubjecttoExperienceModificationClassIndicator_State = 'NOT Y'))), '1',
	    '2'
	) AS v_ExpModZeroByStateInd,
	-- *INF*: :LKP.LKP_SUPClassificationWorkersCompensation(i_ClassCode, '99',v_PolicyEffectiveDate)
	-- -- second lookup on SupWorkersCompensationPremiumModifierClass table using classcode and default state of '99' for across states
	-- 
	LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_i_ClassCode_99_v_PolicyEffectiveDate.SubjecttoExperienceModificationClassIndicator AS v_ExpModZeroNonState,
	-- *INF*: iif ( not isnull(v_ExpModZeroNonState) , substr(v_ExpModZeroNonState,1,1) , null)
	IFF(v_ExpModZeroNonState IS NOT NULL, substr(v_ExpModZeroNonState, 1, 1), null) AS v_SubjecttoExperienceModificationClassIndicator_NonState,
	-- *INF*: iif (not isnull(v_ExpModZeroNonState) , substr(v_ExpModZeroNonState,2,1) , null)
	IFF(v_ExpModZeroNonState IS NOT NULL, substr(v_ExpModZeroNonState, 2, 1), null) AS v_ExperienceModificationClassIndicator_NonState,
	-- *INF*: Decode( true,
	--  (not isnull(v_ExpModZeroNonState)  and
	-- ((v_ExperienceModificationClassIndicator_NonState = 'N' ) AND (v_SubjecttoExperienceModificationClassIndicator_NonState =  'N'))) ,'0',
	-- (not isnull(v_ExpModZeroNonState)  and
	-- ((v_ExperienceModificationClassIndicator_NonState = 'Y' ) OR  (v_SubjecttoExperienceModificationClassIndicator_NonState =  'Y'))) ,'1',
	--  '2')
	-- 
	-- 
	-- -- 0 exp mod not appliable 
	-- --1 exp mod appliable 
	-- --2 no match found for classcode & statecode combination 
	-- 
	Decode(
	    true,
	    (v_ExpModZeroNonState IS NULL and ((v_ExperienceModificationClassIndicator_NonState = 'N') AND (v_SubjecttoExperienceModificationClassIndicator_NonState = 'NOT N'))), '0',
	    (v_ExpModZeroNonState IS NULL and ((v_ExperienceModificationClassIndicator_NonState = 'Y') OR (v_SubjecttoExperienceModificationClassIndicator_NonState = 'NOT Y'))), '1',
	    '2'
	) AS v_ExpModZeroNonStateInd,
	-- *INF*: --existing
	-- --DECODE(TRUE,(v_ExpModZeroByStateInd = '1'  OR v_ExpModZeroNonStateInd = '1'), '1', '0')
	-- 
	-- 
	-- DECODE(TRUE,v_ExpModZeroByStateInd = '1' , '1',  --found & exp mod apply 
	--  (v_ExpModZeroByStateInd = '2'  and  v_ExpModZeroNonStateInd = '1') , '1', --not found in lookup for state & but found in lookup for default state,
	-- '0')
	-- 
	-- 
	-- 
	-- -- 0 exp mod not appliable 
	-- --1 exp mod appliable 
	-- --2 no match found for classcode & statecode combination 
	-- 
	DECODE(
	    TRUE,
	    v_ExpModZeroByStateInd = '1', '1',
	    (v_ExpModZeroByStateInd = '2' and v_ExpModZeroNonStateInd = '1'), '1',
	    '0'
	) AS v_ExpModZeroPerClassInd,
	i_SourceSystem AS o_SourceSystemId,
	i_PremiumMasterCalculationID AS o_PremiumMasterCalculationID,
	-- *INF*: RTRIM(LTRIM(i_TypeBureauCode))
	RTRIM(LTRIM(i_TypeBureauCode)) AS o_TypeBureauCode,
	-- *INF*: TRUNC(i_PremiumMasterRunDate, 'DD')
	CAST(TRUNC(i_PremiumMasterRunDate, 'DAY') AS TIMESTAMP_NTZ(0)) AS o_PremiumMasterRunDate,
	-- *INF*: TO_DATE('1800-01-01', 'YYYY-MM-DD')
	TO_TIMESTAMP('1800-01-01', 'YYYY-MM-DD') AS o_LossMasterRunDate,
	'17124' AS o_BureauCompanyCode,
	-- *INF*: RTRIM(LTRIM(i_pol_sym || i_pol_num || i_pol_mod))
	RTRIM(LTRIM(i_pol_sym || i_pol_num || i_pol_mod)) AS o_PolicyKey,
	-- *INF*: RTRIM(LTRIM(i_StateProvinceCode))
	RTRIM(LTRIM(i_StateProvinceCode)) AS o_StateProvinceCode,
	v_PolicyEffectiveDate AS o_PolicyEffectiveDate,
	-- *INF*: IIF(i_pol_cancellation_ind='Y' AND i_pol_cancellation_date  != TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'), TRUNC(i_pol_cancellation_date, 'DD'), TRUNC(i_pol_exp_date,'DD'))
	IFF(
	    i_pol_cancellation_ind = 'Y'
	    and i_pol_cancellation_date != TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
	    CAST(TRUNC(i_pol_cancellation_date, 'DAY') AS TIMESTAMP_NTZ(0)),
	    CAST(TRUNC(i_pol_exp_date, 'DAY') AS TIMESTAMP_NTZ(0))
	) AS o_PolicyEndDate,
	-- *INF*: IIF(ISNULL(i_WCInterStateRiskId),'N/A', RTRIM(LTRIM(TO_CHAR(i_WCInterStateRiskId))))
	-- --IIF(ISNULL(i_WCInterStateRiskId), LPAD('', 9, '0'), RTRIM(LTRIM(TO_CHAR(i_WCInterStateRiskId))))
	IFF(i_WCInterStateRiskId IS NULL, 'N/A', RTRIM(LTRIM(TO_CHAR(i_WCInterStateRiskId)))) AS o_InterstateRiskId,
	-- *INF*: --Removed lookup as part of PROD-6820 and PROD-13914/12901/13877
	-- -- As per historic data, it was never used as this form was on none of the policies
	-- --IIF( NOT ISNULL(:LKP.LKP_POLICYFORM_FORM(i_pol_id, 'WC000316')), 'E', '')
	-- ''
	'' AS o_EmployeeLeasingCode,
	-- *INF*: IIF(ISNULL(i_StateRatingEffectiveDate), TO_DATE('1800-01-01', 'YYYY-MM-DD'), TRUNC(i_StateRatingEffectiveDate, 'DD'))
	IFF(
	    i_StateRatingEffectiveDate IS NULL, TO_TIMESTAMP('1800-01-01', 'YYYY-MM-DD'),
	    CAST(TRUNC(i_StateRatingEffectiveDate, 'DAY') AS TIMESTAMP_NTZ(0))
	) AS o_StateRatingEffectiveDate,
	-- *INF*: IIF(i_fed_tax_id='N/A', LPAD('',10, ' '),LPAD(SUBSTR(i_fed_tax_id, 1, 10), 10, '0'))
	IFF(i_fed_tax_id = 'N/A', LPAD('', 10, ' '), LPAD(SUBSTR(i_fed_tax_id, 1, 10), 10, '0')) AS o_FederalTaxId,
	-- *INF*: IIF(LTRIM(i_pol_term,'0')='36', '1', '0')
	IFF(LTRIM(i_pol_term, '0') = '36', '1', '0') AS o_ThreeYearFixedRatePolicyIndicator,
	v_MultistatePolicyIndicator AS o_MultistatePolicyIndicator,
	-- *INF*: IIF(i_StateProvinceCode='21', 0,
	--      IIF(LENGTH(i_WCInterStateRiskId)=9,1,0))
	-- 
	-- --IIF(ISNULL(i_WCInterStateRiskId) OR i_WCInterStateRiskId='-1' OR i_WCInterStateRiskId='N/A' , '0', '1')
	IFF(
	    i_StateProvinceCode = '21', 0,
	    IFF(
	        LENGTH(i_WCInterStateRiskId) = 9, 1, 0
	    )
	) AS o_InterstateRatedPolicyIndicator,
	-- *INF*: IIF(i_WCRetrospectiveRatingIndicator='1','1','0')
	IFF(i_WCRetrospectiveRatingIndicator = '1', '1', '0') AS o_RetrospectiveRatedPolicyIndicator,
	-- *INF*: IIF(i_pol_cancellation_date   >=   i_pol_eff_date  AND  i_pol_cancellation_date  <=  i_pol_exp_date, '1', '0')
	IFF(
	    i_pol_cancellation_date >= i_pol_eff_date AND i_pol_cancellation_date <= i_pol_exp_date, '1',
	    '0'
	) AS o_CancelledMidTermPolicyIndicator,
	-- *INF*: IIF(IN(i_StateProvinceCode, '22', '21'), '0', '1')
	IFF(i_StateProvinceCode IN ('22','21'), '0', '1') AS o_ManagedCareOrganizationPolicyIndicator,
	-- *INF*: IIF(i_WCRetrospectiveRatingIndicator='1' and i_StateProvinceCode='48' ,'05','01')
	-- 
	-- 
	-- --IIF(i_WCRetrospectiveRatingIndicator='1','05','01')
	IFF(i_WCRetrospectiveRatingIndicator = '1' and i_StateProvinceCode = '48', '05', '01') AS o_TypeOfCoverageIdCode,
	-- *INF*: IIF(i_InsuranceSegmentAbbreviation='Pool', '02', '01')
	IFF(i_InsuranceSegmentAbbreviation = 'Pool', '02', '01') AS o_TypeOfPlan,
	-- *INF*: IIF(ISNULL(v_DeductibleAmount) OR v_DeductibleAmount='N/A','0',v_DeductibleAmount)
	IFF(v_DeductibleAmount IS NULL OR v_DeductibleAmount = 'N/A', '0', v_DeductibleAmount) AS o_DeductibleAmountPerClaimAccident,
	-- *INF*: IIF(UPPER(i_cust_role)='INSURED', RTRIM(LTRIM(i_name)), 'N/A')
	IFF(UPPER(i_cust_role) = 'INSURED', RTRIM(LTRIM(i_name)), 'N/A') AS o_InsuredName,
	-- *INF*: substr(v_addr_line_1 || ' ' || v_city_name || ' ,' || v_state_prov_code || ' ' || v_zip_postal_code,1,500)
	substr(v_addr_line_1 || ' ' || v_city_name || ' ,' || v_state_prov_code || ' ' || v_zip_postal_code, 1, 500) AS o_WCSTATAddress,
	-- *INF*: IIF(IN(LTRIM(RTRIM(i_ClassCode)),'','N/A'), '',SUBSTR(LTRIM(RTRIM(i_ClassCode)),1,6))
	IFF(LTRIM(RTRIM(i_ClassCode)) IN ('','N/A'), '', SUBSTR(LTRIM(RTRIM(i_ClassCode)), 1, 6)) AS o_PremiumMasterClassCode,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_WorkersCompensationExperienceModificationFactor) or :LKP.LKP_EXPMODTOTALPREMIUMAMOUNT(v_PolicyKey)=0,0,
	-- v_ExpModZeroPerClassInd = '1',i_WorkersCompensationExperienceModificationFactor,
	-- 0)
	-- -- default to zero for nulls or if disqualified by virtue of lookup on SupWorkersCompensationPremiumModifierClass
	-- 
	-- 
	DECODE(
	    TRUE,
	    i_WorkersCompensationExperienceModificationFactor IS NULL or LKP_EXPMODTOTALPREMIUMAMOUNT_v_PolicyKey.TotalPremiumTransactionAmount = 0, 0,
	    v_ExpModZeroPerClassInd = '1', i_WorkersCompensationExperienceModificationFactor,
	    0
	) AS v_ExperienceModificationFactor,
	-- *INF*: RTRIM(LTRIM(i_pol_sym || i_pol_num || i_pol_mod))
	RTRIM(LTRIM(i_pol_sym || i_pol_num || i_pol_mod)) AS v_ExpModPolKey,
	-- *INF*: :LKP.LKP_EXPMODTOTALPREMIUMAMOUNT(v_ExpModPolKey)
	LKP_EXPMODTOTALPREMIUMAMOUNT_v_ExpModPolKey.TotalPremiumTransactionAmount AS v_ExpModTotalPremiumAmt,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_WorkersCompensationExperienceModificationFactor) or v_ExpModTotalPremiumAmt = 0,0,
	-- v_ExpModZeroPerClassInd = '1',i_WorkersCompensationExperienceModificationFactor,
	-- 0)
	DECODE(
	    TRUE,
	    i_WorkersCompensationExperienceModificationFactor IS NULL or v_ExpModTotalPremiumAmt = 0, 0,
	    v_ExpModZeroPerClassInd = '1', i_WorkersCompensationExperienceModificationFactor,
	    0
	) AS o_ExperienceModificationFactor,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystem = 'DCT PT',i_WCExperienceModificationEffectiveDate,
	-- ISNULL(V_ExperienceModificationEffectiveDate), TO_DATE('2100-12-31', 'YYYY-MM-DD'), 
	-- v_ExperienceModificationFactor = 0,TO_DATE('2100-12-31', 'YYYY-MM-DD'), 
	-- v_ExpModTotalPremiumAmt = 0,TO_DATE('2100-12-31', 'YYYY-MM-DD'), 
	-- TRUNC(V_ExperienceModificationEffectiveDate, 'DD'))
	-- -- default to '01-01-1800 for null dates but override to 12-31-2100 for exp mod factor value of zero if determined or naturally present
	DECODE(
	    TRUE,
	    i_SourceSystem = 'DCT PT', i_WCExperienceModificationEffectiveDate,
	    V_ExperienceModificationEffectiveDate IS NULL, TO_TIMESTAMP('2100-12-31', 'YYYY-MM-DD'),
	    v_ExperienceModificationFactor = 0, TO_TIMESTAMP('2100-12-31', 'YYYY-MM-DD'),
	    v_ExpModTotalPremiumAmt = 0, TO_TIMESTAMP('2100-12-31', 'YYYY-MM-DD'),
	    CAST(TRUNC(V_ExperienceModificationEffectiveDate, 'DAY') AS TIMESTAMP_NTZ(0))
	) AS o_ExperienceModificationEffectiveDate,
	i_Exposure AS o_Exposure,
	i_PremiumMasterPremium AS o_PremiumMasterDirectWrittenPremiumAmount,
	-- *INF*: IIF(ISNULL(i_WCBaseRate), 0, i_WCBaseRate)
	IFF(i_WCBaseRate IS NULL, 0, i_WCBaseRate) AS o_ManualChargedRate,
	v_AuditStatus AS o_AuditStatus,
	-- *INF*: DECODE(TRUE,
	--  IN(UPPER(v_AuditStatus), 'BYPASSED', 'REVERSED', 'OVERDUE'),
	-- 'Y',
	-- IN(i_StateProvinceCode, '21', '48') AND UPPER(v_AuditStatus)='ESTIMATED',
	-- 'U',
	-- 'N'
	--  )
	DECODE(
	    TRUE,
	    UPPER(v_AuditStatus) IN ('BYPASSED','REVERSED','OVERDUE'), 'Y',
	    i_StateProvinceCode IN ('21','48') AND UPPER(v_AuditStatus) = 'ESTIMATED', 'U',
	    'N'
	) AS v_PMS_EstimatedAuditCode,
	-- *INF*: IIF(i_SourceSystem='DCT',v_DCT_EstimatedAuditCode, v_PMS_EstimatedAuditCode)
	IFF(i_SourceSystem = 'DCT', v_DCT_EstimatedAuditCode, v_PMS_EstimatedAuditCode) AS o_EstimatedAuditCode,
	join_exp_Mod_eff_date.RunMonthAuditTransFlag AS o_RunMonthAuditTransFlag,
	join_exp_Mod_eff_date.AgeOfPolicy AS o_AgeOfPolicy,
	join_exp_Mod_eff_date.PolicyPremiumTotal,
	join_exp_Mod_eff_date.ManualPremiumInd,
	join_exp_Mod_eff_date.PolicyStateManualPremiumTotal,
	join_exp_Mod_eff_date.BalMinPremiumTotal,
	join_exp_Mod_eff_date.RateEffectiveDate,
	join_exp_Mod_eff_date.PremiumTransactionEnteredDate,
	join_exp_Mod_eff_date.OffsetOnsetCode,
	join_exp_Mod_eff_date.Pol_key,
	join_exp_Mod_eff_date.CoverageType,
	join_exp_Mod_eff_date.PremiumTransactionCode,
	join_exp_Mod_eff_date.CoverageGUID,
	join_exp_Mod_eff_date.RatingCoverageAKID,
	join_exp_Mod_eff_date.TermType,
	join_exp_Mod_eff_date.Period_start_date,
	join_exp_Mod_eff_date.Period_end_date,
	join_exp_Mod_eff_date.AnyARDIndicator,
	join_exp_Mod_eff_date.ExperienceRated,
	join_exp_Mod_eff_date.ExperienceModificationEffectiveDate_PMS,
	-- *INF*: iif(i_SourceSystem='DCT',i_WCExperienceModificationEffectiveDate,ExperienceModificationEffectiveDate_PMS)
	IFF(
	    i_SourceSystem = 'DCT', i_WCExperienceModificationEffectiveDate,
	    ExperienceModificationEffectiveDate_PMS
	) AS V_ExperienceModificationEffectiveDate,
	join_exp_Mod_eff_date.LossesSubjectToDeductibleCode,
	join_exp_Mod_eff_date.BasisOfDeductibleCalculationCode
	FROM join_exp_Mod_eff_date
	LEFT JOIN LKP_PolicyAudit
	ON LKP_PolicyAudit.PolicyAKId = join_exp_Mod_eff_date.pol_ak_id
	LEFT JOIN LKP_AUDITSCHEDULE LKP_AUDITSCHEDULE_v_PolicyKey_i_InsuranceLine_i_pol_eff_date
	ON LKP_AUDITSCHEDULE_v_PolicyKey_i_InsuranceLine_i_pol_eff_date.PolicyKey = v_PolicyKey
	AND LKP_AUDITSCHEDULE_v_PolicyKey_i_InsuranceLine_i_pol_eff_date.InsuranceLine = i_InsuranceLine
	AND LKP_AUDITSCHEDULE_v_PolicyKey_i_InsuranceLine_i_pol_eff_date.AuditEffectiveDate = i_pol_eff_date

	LEFT JOIN LKP_MULTISTATEPOLICY LKP_MULTISTATEPOLICY_i_pol_ak_id
	ON LKP_MULTISTATEPOLICY_i_pol_ak_id.PolicyAKId = i_pol_ak_id

	LEFT JOIN LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_i_ClassCode_v_StateProvinceCode_v_PolicyEffectiveDate
	ON LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_i_ClassCode_v_StateProvinceCode_v_PolicyEffectiveDate.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_i_ClassCode_v_StateProvinceCode_v_PolicyEffectiveDate.RatingStateCode = v_StateProvinceCode
	AND LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_i_ClassCode_v_StateProvinceCode_v_PolicyEffectiveDate.EffectiveDate = v_PolicyEffectiveDate

	LEFT JOIN LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_i_ClassCode_99_v_PolicyEffectiveDate
	ON LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_i_ClassCode_99_v_PolicyEffectiveDate.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_i_ClassCode_99_v_PolicyEffectiveDate.RatingStateCode = '99'
	AND LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_i_ClassCode_99_v_PolicyEffectiveDate.EffectiveDate = v_PolicyEffectiveDate

	LEFT JOIN LKP_EXPMODTOTALPREMIUMAMOUNT LKP_EXPMODTOTALPREMIUMAMOUNT_v_PolicyKey
	ON LKP_EXPMODTOTALPREMIUMAMOUNT_v_PolicyKey.PolicyKey = v_PolicyKey

	LEFT JOIN LKP_EXPMODTOTALPREMIUMAMOUNT LKP_EXPMODTOTALPREMIUMAMOUNT_v_ExpModPolKey
	ON LKP_EXPMODTOTALPREMIUMAMOUNT_v_ExpModPolKey.PolicyKey = v_ExpModPolKey

),
LKP_WorkWCSTATExtract_Premium AS (
	SELECT
	Exposure,
	CorrectionSeqNumber,
	PolicyKey,
	StateProvinceCode,
	PremiumMasterClassCode
	FROM (
		SELECT A.Exposure as Exposure, 
		A.CorrectionSeqNumber as CorrectionSeqNumber,
		A.PolicyKey as PolicyKey, 
		A.StateProvinceCode as StateProvinceCode, 
		A.PremiumMasterClassCode as PremiumMasterClassCode 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkWCSTATExtract A
		join (
		SELECT max(PremiumMasterRunDate) as PremiumMasterRunDate, 
		max(CorrectionSeqNumber) as CorrectionSeqNumber,
		PolicyKey as PolicyKey, 
		StateProvinceCode as StateProvinceCode, 
		PremiumMasterClassCode as PremiumMasterClassCode 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkWCSTATExtract
		where EDWPremiumMasterCalculationPKId<>-1
		group by PolicyKey, 
		StateProvinceCode, 
		PremiumMasterClassCode
		) B
		on A.PolicyKey=B.PolicyKey and A.StateProvinceCode=B.StateProvinceCode and A.PremiumMasterClassCode=B.PremiumMasterClassCode
		and A.PremiumMasterRunDate=B.PremiumMasterRunDate and A.CorrectionSeqNumber=B.CorrectionSeqNumber
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,StateProvinceCode,PremiumMasterClassCode ORDER BY Exposure) = 1
),
EXP_GetValueForPremium AS (
	SELECT
	EXP_Premium.o_SourceSystemId,
	LKP_WorkWCSTATExtract_Premium.Exposure AS i_lkp_Exposure,
	LKP_WorkWCSTATExtract_Premium.CorrectionSeqNumber AS i_lkp_CorrectionSeqNumber,
	EXP_Premium.o_PremiumMasterCalculationID AS EDWPremiumMasterCalculationPKId,
	-1 AS EDWLossMasterCalculationPKId,
	EXP_Premium.o_TypeBureauCode AS TypeBureauCode,
	EXP_Premium.o_PremiumMasterRunDate AS PremiumMasterRunDate,
	EXP_Premium.o_LossMasterRunDate AS LossMasterRunDate,
	EXP_Premium.o_BureauCompanyCode AS BureauCompanyCode,
	EXP_Premium.o_PolicyKey AS PolicyKey,
	EXP_Premium.o_StateProvinceCode AS StateProvinceCode,
	EXP_Premium.o_PolicyEffectiveDate AS PolicyEffectiveDate,
	EXP_Premium.o_PolicyEndDate AS PolicyEndDate,
	EXP_Premium.o_InterstateRiskId AS InterstateRiskId,
	EXP_Premium.o_EmployeeLeasingCode AS EmployeeLeasingCode,
	EXP_Premium.o_StateRatingEffectiveDate AS StateRatingEffectiveDate,
	EXP_Premium.o_FederalTaxId AS FederalTaxId,
	EXP_Premium.o_ThreeYearFixedRatePolicyIndicator AS ThreeYearFixedRatePolicyIndicator,
	EXP_Premium.o_MultistatePolicyIndicator AS MultistatePolicyIndicator,
	EXP_Premium.o_InterstateRatedPolicyIndicator AS InterstateRatedPolicyIndicator,
	EXP_Premium.o_RetrospectiveRatedPolicyIndicator AS RetrospectiveRatedPolicyIndicator,
	EXP_Premium.o_CancelledMidTermPolicyIndicator AS CancelledMidTermPolicyIndicator,
	EXP_Premium.o_ManagedCareOrganizationPolicyIndicator AS ManagedCareOrganizationPolicyIndicator,
	EXP_Premium.o_TypeOfCoverageIdCode AS TypeOfCoverageIdCode,
	EXP_Premium.o_TypeOfPlan AS TypeOfPlan,
	EXP_Premium.o_DeductibleAmountPerClaimAccident AS DeductibleAmountPerClaimAccident,
	EXP_Premium.o_InsuredName AS InsuredName,
	EXP_Premium.o_WCSTATAddress AS WCSTATAddress,
	EXP_Premium.o_PremiumMasterClassCode AS PremiumMasterClassCode,
	EXP_Premium.o_ExperienceModificationFactor AS ExperienceModificationFactor,
	EXP_Premium.o_ExperienceModificationEffectiveDate AS ExperienceModificationEffectiveDate,
	-- *INF*: IIF(TermType='ILF' and ExperienceModificationFactor!=0,PolicyEffectiveDate,ExperienceModificationEffectiveDate)
	IFF(
	    TermType = 'ILF' and ExperienceModificationFactor != 0, PolicyEffectiveDate,
	    ExperienceModificationEffectiveDate
	) AS ExperienceModificationEffectiveDate_out,
	EXP_Premium.o_Exposure AS Exposure,
	EXP_Premium.o_PremiumMasterDirectWrittenPremiumAmount AS PremiumMasterDirectWrittenPremiumAmount,
	EXP_Premium.o_ManualChargedRate AS ManualChargedRate,
	EXP_Premium.o_AuditStatus AS AuditStatus,
	'N/A' AS o_LossMasterClassCode,
	-- *INF*:  TO_DATE('1800-01-01', 'YYYY-MM-DD')
	TO_TIMESTAMP('1800-01-01', 'YYYY-MM-DD') AS o_ClaimLossDate,
	'N/A' AS o_ClaimNumber,
	'N/A' AS o_ClaimOccurrenceStatusCode,
	'' AS o_InjuryTypeCode,
	'N/A' AS o_CatastropheCode,
	0 AS o_IncurredIndemnityAmount,
	0 AS o_IncurredMedicalAmount,
	'N/A' AS o_CauseOfLoss,
	'N/A' AS o_TypeOfRecoveryCode,
	'N/A' AS o_JurisdictionStateCode,
	'N/A' AS o_BodyPartCode,
	'N/A' AS o_NatureOfInjuryCode,
	'N/A' AS o_CauseOfInjuryCode,
	0 AS o_PaidIndemnityAmount,
	0 AS o_PaidMedicalAmount,
	0 AS o_DeductibleReimbursementAmount,
	0 AS o_PaidAllocatedLossAdjustmentExpenseAmount,
	0 AS o_IncurredAllocatedLossAdjustmentExpenseAmount,
	'00' AS o_loss_condition,
	'00' AS o_TypeOfSettlement,
	'N/A' AS o_ManagedCareOrganizationType,
	'N' AS o_LumpSumIndicator,
	EXP_Premium.o_EstimatedAuditCode AS EstimatedAuditCode,
	-- *INF*: IIF(IS_NUMBER(i_lkp_CorrectionSeqNumber), TO_INTEGER(i_lkp_CorrectionSeqNumber), 0)
	IFF(
	    REGEXP_LIKE(i_lkp_CorrectionSeqNumber, '^[0-9]+$'),
	    CAST(i_lkp_CorrectionSeqNumber AS INTEGER),
	    0
	) AS v_lkp_CorrectionSeqNumber,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_lkp_Exposure),
	-- '',
	-- i_lkp_Exposure != Exposure AND IN(UPPER(AuditStatus), 'COMPLETED', 'ESTIMATED', 'AMENDED', 'CANCELLED'),
	-- TO_CHAR(v_lkp_CorrectionSeqNumber+1),
	-- ''
	-- )
	DECODE(
	    TRUE,
	    i_lkp_Exposure IS NULL, '',
	    i_lkp_Exposure != Exposure AND UPPER(AuditStatus) IN ('COMPLETED','ESTIMATED','AMENDED','CANCELLED'), TO_CHAR(v_lkp_CorrectionSeqNumber + 1),
	    ''
	) AS o_CorrectionSeqNumber,
	EXP_Premium.o_RunMonthAuditTransFlag,
	EXP_Premium.o_AgeOfPolicy,
	EXP_Premium.PolicyPremiumTotal,
	EXP_Premium.ManualPremiumInd,
	EXP_Premium.PolicyStateManualPremiumTotal,
	EXP_Premium.BalMinPremiumTotal,
	EXP_Premium.RateEffectiveDate,
	-- *INF*: IIF(TermType='ARD' OR TermType='RED' OR TermType='EMF',RateEffectiveDate,PolicyEffectiveDate)
	IFF(
	    TermType = 'ARD' OR TermType = 'RED' OR TermType = 'EMF', RateEffectiveDate,
	    PolicyEffectiveDate
	) AS RateEffectiveDate_out,
	EXP_Premium.PremiumTransactionEnteredDate,
	EXP_Premium.OffsetOnsetCode,
	EXP_Premium.Pol_key,
	EXP_Premium.CoverageType,
	EXP_Premium.PremiumTransactionCode,
	EXP_Premium.CoverageGUID,
	EXP_Premium.RatingCoverageAKID,
	EXP_Premium.TermType,
	EXP_Premium.Period_start_date,
	EXP_Premium.Period_end_date,
	EXP_Premium.AnyARDIndicator,
	EXP_Premium.ExperienceRated,
	EXP_Premium.LossesSubjectToDeductibleCode,
	EXP_Premium.BasisOfDeductibleCalculationCode
	FROM EXP_Premium
	LEFT JOIN LKP_WorkWCSTATExtract_Premium
	ON LKP_WorkWCSTATExtract_Premium.PolicyKey = EXP_Premium.o_PolicyKey AND LKP_WorkWCSTATExtract_Premium.StateProvinceCode = EXP_Premium.o_StateProvinceCode AND LKP_WorkWCSTATExtract_Premium.PremiumMasterClassCode = EXP_Premium.o_PremiumMasterClassCode
),
SRT_SortData AS (
	SELECT
	o_SourceSystemId, 
	TypeBureauCode, 
	BureauCompanyCode, 
	PolicyKey, 
	StateProvinceCode, 
	PolicyEffectiveDate, 
	PolicyEndDate, 
	PremiumMasterClassCode, 
	ExperienceModificationFactor, 
	ExperienceModificationEffectiveDate_out AS ExperienceModificationEffectiveDate, 
	ManualChargedRate, 
	PremiumMasterRunDate, 
	PremiumTransactionEnteredDate, 
	OffsetOnsetCode, 
	EDWPremiumMasterCalculationPKId, 
	EDWLossMasterCalculationPKId, 
	LossMasterRunDate, 
	InterstateRiskId, 
	EmployeeLeasingCode, 
	StateRatingEffectiveDate, 
	FederalTaxId, 
	ThreeYearFixedRatePolicyIndicator, 
	MultistatePolicyIndicator, 
	InterstateRatedPolicyIndicator, 
	RetrospectiveRatedPolicyIndicator, 
	CancelledMidTermPolicyIndicator, 
	ManagedCareOrganizationPolicyIndicator, 
	TypeOfCoverageIdCode, 
	TypeOfPlan, 
	DeductibleAmountPerClaimAccident, 
	InsuredName, 
	WCSTATAddress, 
	Exposure, 
	PremiumMasterDirectWrittenPremiumAmount, 
	AuditStatus, 
	o_LossMasterClassCode, 
	o_ClaimLossDate, 
	o_ClaimNumber, 
	o_ClaimOccurrenceStatusCode, 
	o_InjuryTypeCode, 
	o_CatastropheCode, 
	o_IncurredIndemnityAmount, 
	o_IncurredMedicalAmount, 
	o_CauseOfLoss, 
	o_TypeOfRecoveryCode, 
	o_JurisdictionStateCode, 
	o_BodyPartCode, 
	o_NatureOfInjuryCode, 
	o_CauseOfInjuryCode, 
	o_PaidIndemnityAmount, 
	o_PaidMedicalAmount, 
	o_DeductibleReimbursementAmount, 
	o_PaidAllocatedLossAdjustmentExpenseAmount, 
	o_IncurredAllocatedLossAdjustmentExpenseAmount, 
	o_loss_condition, 
	o_TypeOfSettlement, 
	o_ManagedCareOrganizationType, 
	o_LumpSumIndicator, 
	EstimatedAuditCode, 
	o_CorrectionSeqNumber, 
	o_RunMonthAuditTransFlag, 
	o_AgeOfPolicy, 
	PolicyPremiumTotal, 
	ManualPremiumInd, 
	PolicyStateManualPremiumTotal, 
	BalMinPremiumTotal, 
	RateEffectiveDate_out AS RateEffectiveDate, 
	Pol_key, 
	CoverageType, 
	PremiumTransactionCode, 
	CoverageGUID, 
	RatingCoverageAKID, 
	RunMonthAuditTransFlag, 
	AgeOfPolicy, 
	TermType, 
	Period_start_date, 
	Period_end_date, 
	AnyARDIndicator, 
	ExperienceRated, 
	LossesSubjectToDeductibleCode, 
	BasisOfDeductibleCalculationCode
	FROM EXP_GetValueForPremium
	ORDER BY TypeBureauCode ASC, BureauCompanyCode ASC, PolicyKey ASC, StateProvinceCode ASC, PolicyEffectiveDate ASC, PolicyEndDate ASC, PremiumMasterClassCode ASC, ExperienceModificationFactor ASC, ExperienceModificationEffectiveDate ASC, ManualChargedRate ASC, PremiumMasterRunDate ASC, PremiumTransactionEnteredDate ASC, OffsetOnsetCode ASC
),
agg_SummarizePremiums AS (
	SELECT
	o_SourceSystemId AS i_SourceSystemId,
	TypeBureauCode,
	BureauCompanyCode,
	PolicyKey,
	StateProvinceCode,
	PolicyEffectiveDate,
	PolicyEndDate,
	PremiumMasterClassCode,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	ManualChargedRate,
	PremiumMasterRunDate,
	EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId,
	LossMasterRunDate,
	InterstateRiskId,
	EmployeeLeasingCode,
	StateRatingEffectiveDate,
	FederalTaxId,
	ThreeYearFixedRatePolicyIndicator,
	MultistatePolicyIndicator,
	InterstateRatedPolicyIndicator,
	RetrospectiveRatedPolicyIndicator,
	CancelledMidTermPolicyIndicator,
	ManagedCareOrganizationPolicyIndicator,
	TypeOfCoverageIdCode,
	TypeOfPlan,
	DeductibleAmountPerClaimAccident,
	InsuredName,
	WCSTATAddress,
	Exposure,
	PremiumMasterDirectWrittenPremiumAmount,
	AuditStatus,
	o_LossMasterClassCode AS LossMasterClassCode,
	o_ClaimLossDate AS ClaimLossDate,
	o_ClaimNumber AS ClaimNumber,
	o_ClaimOccurrenceStatusCode AS ClaimOccurrenceStatusCode,
	o_InjuryTypeCode AS InjuryTypeCode,
	o_CatastropheCode AS CatastropheCode,
	o_IncurredIndemnityAmount AS IncurredIndemnityAmount,
	o_IncurredMedicalAmount AS IncurredMedicalAmount,
	o_CauseOfLoss AS CauseOfLoss,
	o_TypeOfRecoveryCode AS TypeOfRecoveryCode,
	o_JurisdictionStateCode AS JurisdictionStateCode,
	o_BodyPartCode AS BodyPartCode,
	o_NatureOfInjuryCode AS NatureOfInjuryCode,
	o_CauseOfInjuryCode AS CauseOfInjuryCode,
	o_PaidIndemnityAmount AS PaidIndemnityAmount,
	o_PaidMedicalAmount AS PaidMedicalAmount,
	o_DeductibleReimbursementAmount AS DeductibleReimbursementAmount,
	o_PaidAllocatedLossAdjustmentExpenseAmount AS PaidAllocatedLossAdjustmentExpenseAmount,
	o_IncurredAllocatedLossAdjustmentExpenseAmount AS IncurredAllocatedLossAdjustmentExpenseAmount,
	o_loss_condition AS loss_condition,
	o_TypeOfSettlement AS TypeOfSettlement,
	o_ManagedCareOrganizationType AS ManagedCareOrganizationType,
	o_LumpSumIndicator AS LumpSumIndicator,
	EstimatedAuditCode,
	o_CorrectionSeqNumber AS CorrectionSeqNumber,
	-- *INF*: Last(i_SourceSystemId)
	Last(i_SourceSystemId) AS o_SourceSystemId,
	-- *INF*: LAST(EDWPremiumMasterCalculationPKId)
	LAST(EDWPremiumMasterCalculationPKId) AS o_EDWPremiumMasterCalculationPKId2,
	-- *INF*: LAST(EDWLossMasterCalculationPKId)
	LAST(EDWLossMasterCalculationPKId) AS o_EDWLossMasterCalculationPKId2,
	TypeBureauCode AS o_TypeBureauCode1,
	-- *INF*: LAST(PremiumMasterRunDate)
	LAST(PremiumMasterRunDate) AS o_PremiumMasterRunDate1,
	-- *INF*: LAST(LossMasterRunDate)
	LAST(LossMasterRunDate) AS o_LossMasterRunDate1,
	BureauCompanyCode AS o_BureauCompanyCode1,
	PolicyKey AS o_PolicyKey1,
	StateProvinceCode AS o_StateProvinceCode1,
	PolicyEffectiveDate AS o_PolicyEffectiveDate1,
	PolicyEndDate AS o_PolicyEndDate1,
	-- *INF*: LAST(InterstateRiskId)
	LAST(InterstateRiskId) AS o_InterstateRiskId1,
	-- *INF*: LAST(EmployeeLeasingCode)
	LAST(EmployeeLeasingCode) AS o_EmployeeLeasingCode1,
	-- *INF*: LAST(StateRatingEffectiveDate)
	LAST(StateRatingEffectiveDate) AS o_StateRatingEffectiveDate1,
	-- *INF*: LAST(FederalTaxId)
	LAST(FederalTaxId) AS o_FederalTaxId1,
	-- *INF*: LAST(ThreeYearFixedRatePolicyIndicator)
	LAST(ThreeYearFixedRatePolicyIndicator) AS o_ThreeYearFixedRatePolicyIndicator1,
	-- *INF*: LAST(MultistatePolicyIndicator)
	LAST(MultistatePolicyIndicator) AS o_MultistatePolicyIndicator1,
	-- *INF*: LAST(InterstateRatedPolicyIndicator)
	LAST(InterstateRatedPolicyIndicator) AS o_InterstateRatedPolicyIndicator1,
	-- *INF*: LAST(RetrospectiveRatedPolicyIndicator)
	LAST(RetrospectiveRatedPolicyIndicator) AS o_RetrospectiveratedPolicyIndicator1,
	-- *INF*: LAST(CancelledMidTermPolicyIndicator)
	LAST(CancelledMidTermPolicyIndicator) AS o_CancelledMidTermPolicyIndicator1,
	-- *INF*: LAST(ManagedCareOrganizationPolicyIndicator)
	LAST(ManagedCareOrganizationPolicyIndicator) AS o_ManagedCareOrganizationPolicyIndicator1,
	-- *INF*: LAST(TypeOfCoverageIdCode)
	LAST(TypeOfCoverageIdCode) AS o_TypeOfCoverageIdCode1,
	-- *INF*: LAST(TypeOfPlan)
	LAST(TypeOfPlan) AS o_TypeOfPlan1,
	-- *INF*: LAST(DeductibleAmountPerClaimAccident)
	LAST(DeductibleAmountPerClaimAccident) AS o_DeductibleAmountPerClaimAccident1,
	-- *INF*: LAST(InsuredName)
	LAST(InsuredName) AS o_InsuredName1,
	-- *INF*: LAST(WCSTATAddress)
	LAST(WCSTATAddress) AS o_WCSTATAddress1,
	PremiumMasterClassCode AS o_PremiumMasterClassCode1,
	ExperienceModificationFactor AS o_ExperienceModificationFactor1,
	ExperienceModificationEffectiveDate AS o_ExperienceModificationEffectiveDate1,
	-- *INF*: SUM(Exposure)
	-- -- Compute net values of Written Exposure for later rounding
	SUM(Exposure) AS o_Exposure1,
	-- *INF*: SUM(PremiumMasterDirectWrittenPremiumAmount)
	SUM(PremiumMasterDirectWrittenPremiumAmount) AS o_PremiumMasterDirectWrittenPremiumAmount1,
	ManualChargedRate AS o_ManualChargedRate1,
	-- *INF*: LAST(LossMasterClassCode)
	LAST(LossMasterClassCode) AS o_LossMasterClassCode1,
	-- *INF*: LAST(ClaimLossDate)
	LAST(ClaimLossDate) AS o_ClaimLossDate1,
	-- *INF*: LAST(ClaimOccurrenceStatusCode)
	LAST(ClaimOccurrenceStatusCode) AS o_ClaimOccurrenceStatusCode1,
	-- *INF*: LAST(InjuryTypeCode)
	LAST(InjuryTypeCode) AS o_InjuryTypeCode1,
	-- *INF*: LAST(CatastropheCode)
	LAST(CatastropheCode) AS o_CatastropheCode1,
	-- *INF*: LAST(IncurredIndemnityAmount)
	LAST(IncurredIndemnityAmount) AS o_IncurredIndemnityAmount1,
	-- *INF*: LAST(IncurredMedicalAmount)
	LAST(IncurredMedicalAmount) AS o_IncurredMedicalAmount1,
	-- *INF*: LAST(CauseOfLoss)
	LAST(CauseOfLoss) AS o_CauseOfLoss1,
	-- *INF*: LAST(TypeOfRecoveryCode)
	LAST(TypeOfRecoveryCode) AS o_TypeOfRecoveryCode1,
	-- *INF*: LAST(JurisdictionStateCode)
	LAST(JurisdictionStateCode) AS o_JurisdictionStateCode1,
	-- *INF*: LAST(BodyPartCode)
	LAST(BodyPartCode) AS o_BodyPartCode1,
	-- *INF*: LAST(NatureOfInjuryCode)
	LAST(NatureOfInjuryCode) AS o_NatureOfInjuryCode1,
	-- *INF*: LAST(CauseOfInjuryCode)
	LAST(CauseOfInjuryCode) AS o_CauseOfInjuryCode1,
	-- *INF*: LAST(PaidIndemnityAmount)
	LAST(PaidIndemnityAmount) AS o_PaidIndemnityAmount1,
	-- *INF*: LAST(PaidMedicalAmount)
	LAST(PaidMedicalAmount) AS o_PaidMedicalAmount1,
	-- *INF*: LAST(DeductibleReimbursementAmount)
	LAST(DeductibleReimbursementAmount) AS o_DeductibleReimbursementAmount1,
	-- *INF*: LAST(PaidAllocatedLossAdjustmentExpenseAmount)
	LAST(PaidAllocatedLossAdjustmentExpenseAmount) AS o_PaidAllocatedLossAdjustmentExpenseAmount1,
	-- *INF*: LAST(IncurredAllocatedLossAdjustmentExpenseAmount)
	LAST(IncurredAllocatedLossAdjustmentExpenseAmount) AS o_IncurredAllocatedLossAdjustmentExpenseAmount1,
	-- *INF*: LAST(loss_condition)
	LAST(loss_condition) AS o_loss_condition2,
	-- *INF*: LAST(TypeOfSettlement)
	LAST(TypeOfSettlement) AS o_TypeOfSettlement2,
	-- *INF*: LAST(ManagedCareOrganizationType)
	LAST(ManagedCareOrganizationType) AS o_ManagedCareOrganizationType2,
	-- *INF*: LAST(LumpSumIndicator)
	LAST(LumpSumIndicator) AS o_LumpSumIndicator2,
	-- *INF*: LAST(EstimatedAuditCode)
	LAST(EstimatedAuditCode) AS o_EstimatedAuditCode2,
	-- *INF*: LAST(CorrectionSeqNumber)
	LAST(CorrectionSeqNumber) AS o_CorrectionSeqNumber2,
	-- *INF*: LAST(ClaimNumber)
	LAST(ClaimNumber) AS o_ClaimNumber1,
	o_RunMonthAuditTransFlag AS i_RunMonthAuditTransFlag,
	-- *INF*: SUM(
	-- IIF(i_RunMonthAuditTransFlag=1,
	-- PremiumMasterDirectWrittenPremiumAmount,0)
	-- )
	SUM(
	    IFF(
	        i_RunMonthAuditTransFlag = 1, PremiumMasterDirectWrittenPremiumAmount, 0
	    )) AS o_RunMonthAuditPremium,
	o_AgeOfPolicy AS i_AgeOfPolicy,
	-- *INF*: LAST(i_AgeOfPolicy)
	LAST(i_AgeOfPolicy) AS o_AgeOfPolicy,
	PolicyPremiumTotal,
	ManualPremiumInd,
	PolicyStateManualPremiumTotal,
	BalMinPremiumTotal,
	RateEffectiveDate,
	-- *INF*: min(RateEffectiveDate)
	min(RateEffectiveDate) AS O_RateEffectiveDate,
	Pol_key,
	CoverageType,
	PremiumTransactionCode,
	CoverageGUID,
	RatingCoverageAKID,
	RunMonthAuditTransFlag,
	AgeOfPolicy,
	TermType,
	Period_start_date,
	Period_end_date,
	AnyARDIndicator,
	ExperienceRated,
	0 AS ARDPassFlag,
	LossesSubjectToDeductibleCode,
	-- *INF*: LAST(LossesSubjectToDeductibleCode)
	LAST(LossesSubjectToDeductibleCode) AS o_LossesSubjectToDeductibleCode,
	BasisOfDeductibleCalculationCode,
	-- *INF*: LAST(BasisOfDeductibleCalculationCode)
	LAST(BasisOfDeductibleCalculationCode) AS o_BasisOfDeductibleCalculationCode
	FROM SRT_SortData
	GROUP BY TypeBureauCode, BureauCompanyCode, PolicyKey, StateProvinceCode, PolicyEffectiveDate, PolicyEndDate, PremiumMasterClassCode, ExperienceModificationFactor, ExperienceModificationEffectiveDate, ManualChargedRate
),
filter_EMF_ARD AS (
	SELECT
	o_TypeBureauCode1, 
	o_BureauCompanyCode1, 
	o_PolicyKey1, 
	o_StateProvinceCode1, 
	o_PolicyEffectiveDate1, 
	o_PolicyEndDate1, 
	o_PremiumMasterClassCode1, 
	o_ExperienceModificationFactor1, 
	o_ExperienceModificationEffectiveDate1, 
	o_ManualChargedRate1, 
	o_PremiumMasterDirectWrittenPremiumAmount1, 
	TermType
	FROM agg_SummarizePremiums
	WHERE TermType='EMF' OR TermType='ARD' OR  TermType='RED'
),
agg_split_class_code_premiums AS (
	SELECT
	o_TypeBureauCode1,
	o_BureauCompanyCode1,
	o_PolicyKey1,
	o_StateProvinceCode1,
	o_PolicyEffectiveDate1,
	o_PolicyEndDate1,
	o_PremiumMasterClassCode1,
	o_PremiumMasterDirectWrittenPremiumAmount1 AS i_PremiumMasterDirectWrittenPremiumAmount1,
	-- *INF*: sum(i_PremiumMasterDirectWrittenPremiumAmount1)
	sum(i_PremiumMasterDirectWrittenPremiumAmount1) AS o_PremiumMasterDirectWrittenPremiumAmount1
	FROM filter_EMF_ARD
	GROUP BY o_TypeBureauCode1, o_BureauCompanyCode1, o_PolicyKey1, o_StateProvinceCode1, o_PolicyEffectiveDate1, o_PolicyEndDate1, o_PremiumMasterClassCode1
),
sort_Pol_ClassCode AS (
	SELECT
	o_TypeBureauCode1, 
	o_BureauCompanyCode1, 
	o_PolicyKey1, 
	o_StateProvinceCode1, 
	o_PolicyEffectiveDate1, 
	o_PolicyEndDate1, 
	o_PremiumMasterClassCode1, 
	o_PremiumMasterDirectWrittenPremiumAmount1
	FROM agg_split_class_code_premiums
	ORDER BY o_TypeBureauCode1 ASC, o_BureauCompanyCode1 ASC, o_PolicyKey1 ASC, o_StateProvinceCode1 ASC, o_PolicyEffectiveDate1 ASC, o_PolicyEndDate1 ASC, o_PremiumMasterClassCode1 ASC
),
sort_pol_ClassCode_join AS (
	SELECT
	o_SourceSystemId, 
	o_EDWPremiumMasterCalculationPKId2, 
	o_EDWLossMasterCalculationPKId2, 
	o_TypeBureauCode1, 
	o_PremiumMasterRunDate1, 
	o_LossMasterRunDate1, 
	o_BureauCompanyCode1, 
	o_PolicyKey1, 
	o_StateProvinceCode1, 
	o_PolicyEffectiveDate1, 
	o_PolicyEndDate1, 
	o_InterstateRiskId1, 
	o_EmployeeLeasingCode1, 
	o_StateRatingEffectiveDate1, 
	o_FederalTaxId1, 
	o_ThreeYearFixedRatePolicyIndicator1, 
	o_MultistatePolicyIndicator1, 
	o_InterstateRatedPolicyIndicator1, 
	o_RetrospectiveratedPolicyIndicator1, 
	o_CancelledMidTermPolicyIndicator1, 
	o_ManagedCareOrganizationPolicyIndicator1, 
	o_TypeOfCoverageIdCode1, 
	o_TypeOfPlan1, 
	o_DeductibleAmountPerClaimAccident1, 
	o_InsuredName1, 
	o_WCSTATAddress1, 
	o_PremiumMasterClassCode1, 
	o_ExperienceModificationFactor1, 
	o_ExperienceModificationEffectiveDate1, 
	o_Exposure1, 
	o_PremiumMasterDirectWrittenPremiumAmount1, 
	o_ManualChargedRate1, 
	o_LossMasterClassCode1, 
	o_ClaimLossDate1, 
	o_ClaimOccurrenceStatusCode1, 
	o_InjuryTypeCode1, 
	o_CatastropheCode1, 
	o_IncurredIndemnityAmount1, 
	o_IncurredMedicalAmount1, 
	o_CauseOfLoss1, 
	o_TypeOfRecoveryCode1, 
	o_JurisdictionStateCode1, 
	o_BodyPartCode1, 
	o_NatureOfInjuryCode1, 
	o_CauseOfInjuryCode1, 
	o_PaidIndemnityAmount1, 
	o_PaidMedicalAmount1, 
	o_DeductibleReimbursementAmount1, 
	o_PaidAllocatedLossAdjustmentExpenseAmount1, 
	o_IncurredAllocatedLossAdjustmentExpenseAmount1, 
	o_loss_condition2, 
	o_TypeOfSettlement2, 
	o_ManagedCareOrganizationType2, 
	o_LumpSumIndicator2, 
	o_EstimatedAuditCode2, 
	o_CorrectionSeqNumber2, 
	o_ClaimNumber1, 
	o_RunMonthAuditPremium, 
	o_AgeOfPolicy, 
	PolicyPremiumTotal, 
	ManualPremiumInd, 
	PolicyStateManualPremiumTotal, 
	BalMinPremiumTotal, 
	O_RateEffectiveDate AS RateEffectiveDate, 
	TermType, 
	AnyARDIndicator, 
	ExperienceRated, 
	ARDPassFlag, 
	o_LossesSubjectToDeductibleCode, 
	o_BasisOfDeductibleCalculationCode
	FROM agg_SummarizePremiums
	ORDER BY o_TypeBureauCode1 ASC, o_BureauCompanyCode1 ASC, o_PolicyKey1 ASC, o_StateProvinceCode1 ASC, o_PolicyEffectiveDate1 ASC, o_PolicyEndDate1 ASC, o_PremiumMasterClassCode1 ASC
),
join_Split_period_ClassCode_premiums AS (SELECT
	sort_pol_ClassCode_join.o_SourceSystemId AS o_SourceSystemID, 
	sort_pol_ClassCode_join.o_EDWPremiumMasterCalculationPKId2, 
	sort_pol_ClassCode_join.o_EDWLossMasterCalculationPKId2, 
	sort_pol_ClassCode_join.o_TypeBureauCode1, 
	sort_pol_ClassCode_join.o_PremiumMasterRunDate1, 
	sort_pol_ClassCode_join.o_LossMasterRunDate1, 
	sort_pol_ClassCode_join.o_BureauCompanyCode1, 
	sort_pol_ClassCode_join.o_PolicyKey1, 
	sort_pol_ClassCode_join.o_StateProvinceCode1, 
	sort_pol_ClassCode_join.o_PolicyEffectiveDate1, 
	sort_pol_ClassCode_join.o_PolicyEndDate1, 
	sort_pol_ClassCode_join.o_InterstateRiskId1, 
	sort_pol_ClassCode_join.o_EmployeeLeasingCode1, 
	sort_pol_ClassCode_join.o_StateRatingEffectiveDate1, 
	sort_pol_ClassCode_join.o_FederalTaxId1, 
	sort_pol_ClassCode_join.o_ThreeYearFixedRatePolicyIndicator1, 
	sort_pol_ClassCode_join.o_MultistatePolicyIndicator1, 
	sort_pol_ClassCode_join.o_InterstateRatedPolicyIndicator1, 
	sort_pol_ClassCode_join.o_RetrospectiveratedPolicyIndicator1, 
	sort_pol_ClassCode_join.o_CancelledMidTermPolicyIndicator1, 
	sort_pol_ClassCode_join.o_ManagedCareOrganizationPolicyIndicator1, 
	sort_pol_ClassCode_join.o_TypeOfCoverageIdCode1, 
	sort_pol_ClassCode_join.o_TypeOfPlan1, 
	sort_pol_ClassCode_join.o_DeductibleAmountPerClaimAccident1, 
	sort_pol_ClassCode_join.o_InsuredName1, 
	sort_pol_ClassCode_join.o_WCSTATAddress1, 
	sort_pol_ClassCode_join.o_PremiumMasterClassCode1, 
	sort_pol_ClassCode_join.o_ExperienceModificationFactor1, 
	sort_pol_ClassCode_join.o_ExperienceModificationEffectiveDate1, 
	sort_pol_ClassCode_join.o_Exposure1, 
	sort_pol_ClassCode_join.o_PremiumMasterDirectWrittenPremiumAmount1, 
	sort_pol_ClassCode_join.o_ManualChargedRate1, 
	sort_pol_ClassCode_join.o_LossMasterClassCode1, 
	sort_pol_ClassCode_join.o_ClaimLossDate1, 
	sort_pol_ClassCode_join.o_ClaimOccurrenceStatusCode1, 
	sort_pol_ClassCode_join.o_InjuryTypeCode1, 
	sort_pol_ClassCode_join.o_CatastropheCode1, 
	sort_pol_ClassCode_join.o_IncurredIndemnityAmount1, 
	sort_pol_ClassCode_join.o_IncurredMedicalAmount1, 
	sort_pol_ClassCode_join.o_CauseOfLoss1, 
	sort_pol_ClassCode_join.o_TypeOfRecoveryCode1, 
	sort_pol_ClassCode_join.o_JurisdictionStateCode1, 
	sort_pol_ClassCode_join.o_BodyPartCode1, 
	sort_pol_ClassCode_join.o_NatureOfInjuryCode1, 
	sort_pol_ClassCode_join.o_CauseOfInjuryCode1, 
	sort_pol_ClassCode_join.o_PaidIndemnityAmount1, 
	sort_pol_ClassCode_join.o_PaidMedicalAmount1, 
	sort_pol_ClassCode_join.o_DeductibleReimbursementAmount1, 
	sort_pol_ClassCode_join.o_PaidAllocatedLossAdjustmentExpenseAmount1, 
	sort_pol_ClassCode_join.o_IncurredAllocatedLossAdjustmentExpenseAmount1, 
	sort_pol_ClassCode_join.o_loss_condition2, 
	sort_pol_ClassCode_join.o_TypeOfSettlement2, 
	sort_pol_ClassCode_join.o_ManagedCareOrganizationType2, 
	sort_pol_ClassCode_join.o_LumpSumIndicator2, 
	sort_pol_ClassCode_join.o_EstimatedAuditCode2, 
	sort_pol_ClassCode_join.o_CorrectionSeqNumber2, 
	sort_pol_ClassCode_join.o_ClaimNumber1, 
	sort_pol_ClassCode_join.o_RunMonthAuditPremium, 
	sort_pol_ClassCode_join.o_AgeOfPolicy, 
	sort_pol_ClassCode_join.PolicyPremiumTotal, 
	sort_pol_ClassCode_join.ManualPremiumInd, 
	sort_pol_ClassCode_join.PolicyStateManualPremiumTotal, 
	sort_pol_ClassCode_join.BalMinPremiumTotal, 
	sort_pol_ClassCode_join.RateEffectiveDate, 
	sort_pol_ClassCode_join.TermType, 
	sort_pol_ClassCode_join.AnyARDIndicator, 
	sort_pol_ClassCode_join.ExperienceRated, 
	sort_pol_ClassCode_join.ARDPassFlag, 
	sort_Pol_ClassCode.o_TypeBureauCode1 AS o_TypeBureauCode11, 
	sort_Pol_ClassCode.o_BureauCompanyCode1 AS o_BureauCompanyCode11, 
	sort_Pol_ClassCode.o_PolicyKey1 AS o_PolicyKey11, 
	sort_Pol_ClassCode.o_StateProvinceCode1 AS o_StateProvinceCode11, 
	sort_Pol_ClassCode.o_PolicyEffectiveDate1 AS o_PolicyEffectiveDate11, 
	sort_Pol_ClassCode.o_PolicyEndDate1 AS o_PolicyEndDate11, 
	sort_Pol_ClassCode.o_PremiumMasterClassCode1 AS o_PremiumMasterClassCode11, 
	sort_Pol_ClassCode.o_PremiumMasterDirectWrittenPremiumAmount1 AS o_PremiumMasterDirectWrittenPremiumAmount11, 
	sort_pol_ClassCode_join.o_LossesSubjectToDeductibleCode, 
	sort_pol_ClassCode_join.o_BasisOfDeductibleCalculationCode
	FROM sort_pol_ClassCode_join
	LEFT OUTER JOIN sort_Pol_ClassCode
	ON sort_Pol_ClassCode.o_TypeBureauCode1 = sort_pol_ClassCode_join.o_TypeBureauCode1 AND sort_Pol_ClassCode.o_BureauCompanyCode1 = sort_pol_ClassCode_join.o_BureauCompanyCode1 AND sort_Pol_ClassCode.o_PolicyKey1 = sort_pol_ClassCode_join.o_PolicyKey1 AND sort_Pol_ClassCode.o_StateProvinceCode1 = sort_pol_ClassCode_join.o_StateProvinceCode1 AND sort_Pol_ClassCode.o_PolicyEffectiveDate1 = sort_pol_ClassCode_join.o_PolicyEffectiveDate1 AND sort_Pol_ClassCode.o_PolicyEndDate1 = sort_pol_ClassCode_join.o_PolicyEndDate1 AND sort_Pol_ClassCode.o_PremiumMasterClassCode1 = sort_pol_ClassCode_join.o_PremiumMasterClassCode1
),
EXP_Target_Data AS (
	SELECT
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	o_SourceSystemID,
	o_EDWPremiumMasterCalculationPKId2 AS EDWPremiumMasterCalculationPKId,
	o_EDWLossMasterCalculationPKId2 AS EDWLossMasterCalculationPKId,
	o_TypeBureauCode1 AS TypeBureauCode,
	o_PremiumMasterRunDate1 AS PremiumMasterRunDate,
	o_LossMasterRunDate1 AS LossMasterRunDate,
	o_BureauCompanyCode1 AS BureauCompanyCode,
	o_PolicyKey1 AS PolicyKey,
	o_StateProvinceCode1 AS StateProvinceCode,
	o_PolicyEffectiveDate1 AS PolicyEffectiveDate,
	o_PolicyEndDate1 AS PolicyEndDate,
	o_InterstateRiskId1 AS InterstateRiskId,
	o_EmployeeLeasingCode1 AS EmployeeLeasingCode,
	o_StateRatingEffectiveDate1 AS StateRatingEffectiveDate,
	o_FederalTaxId1 AS FederalTaxId,
	o_ThreeYearFixedRatePolicyIndicator1 AS ThreeYearFixedRatePolicyIndicator,
	o_MultistatePolicyIndicator1 AS MultistatePolicyIndicator,
	o_InterstateRatedPolicyIndicator1 AS InterstateRatedPolicyIndicator,
	o_RetrospectiveratedPolicyIndicator1 AS RetrospectiveratedPolicyIndicator,
	o_CancelledMidTermPolicyIndicator1 AS CancelledMidTermPolicyIndicator,
	o_ManagedCareOrganizationPolicyIndicator1 AS ManagedCareOrganizationPolicyIndicator,
	o_TypeOfCoverageIdCode1 AS TypeOfCoverageIdCode,
	o_TypeOfPlan1 AS TypeOfPlan,
	o_DeductibleAmountPerClaimAccident1 AS DeductibleAmountPerClaimAccident,
	o_InsuredName1 AS InsuredName,
	o_WCSTATAddress1 AS WCSTATAddress,
	o_PremiumMasterClassCode1 AS PremiumMasterClassCode,
	o_ExperienceModificationFactor1 AS ExperienceModificationFactor,
	o_ExperienceModificationEffectiveDate1 AS ExperienceModificationEffectiveDate,
	o_Exposure1 AS Exposure,
	o_PremiumMasterDirectWrittenPremiumAmount1 AS PremiumMasterDirectWrittenPremiumAmount,
	o_ManualChargedRate1 AS ManualChargedRate,
	o_LossMasterClassCode1 AS LossMasterClassCode,
	o_ClaimLossDate1 AS ClaimLossDate,
	o_ClaimOccurrenceStatusCode1 AS ClaimOccurrenceStatusCode,
	o_InjuryTypeCode1 AS InjuryTypeCode,
	o_CatastropheCode1 AS CatastropheCode,
	o_IncurredIndemnityAmount1 AS IncurredIndemnityAmount,
	o_IncurredMedicalAmount1 AS IncurredMedicalAmount,
	o_CauseOfLoss1 AS CauseOfLoss,
	o_TypeOfRecoveryCode1 AS TypeOfRecoveryCode,
	o_JurisdictionStateCode1 AS JurisdictionStateCode,
	o_BodyPartCode1 AS BodyPartCode,
	o_NatureOfInjuryCode1 AS NatureOfInjuryCode,
	o_CauseOfInjuryCode1 AS CauseOfInjuryCode,
	o_PaidIndemnityAmount1 AS PaidIndemnityAmount,
	o_PaidMedicalAmount1 AS PaidMedicalAmount,
	o_DeductibleReimbursementAmount1 AS DeductibleReimbursementAmount,
	o_PaidAllocatedLossAdjustmentExpenseAmount1 AS PaidAllocatedLossAdjustmentExpenseAmount,
	o_IncurredAllocatedLossAdjustmentExpenseAmount1 AS IncurredAllocatedLossAdjustmentExpenseAmount,
	o_loss_condition2 AS loss_condition,
	o_TypeOfSettlement2 AS TypeOfSettlement,
	o_ManagedCareOrganizationType2 AS ManagedCareOrganizationType,
	o_LumpSumIndicator2 AS LumpSumIndicator,
	o_EstimatedAuditCode2 AS EstimatedAuditCode,
	o_CorrectionSeqNumber2 AS CorrectionSeqNumber,
	o_ClaimNumber1 AS ClaimNumber,
	o_RunMonthAuditPremium AS RunMonthAuditPremium,
	o_AgeOfPolicy AS AgeOfPolicy,
	PolicyPremiumTotal,
	ManualPremiumInd,
	PolicyStateManualPremiumTotal,
	BalMinPremiumTotal,
	RateEffectiveDate,
	AnyARDIndicator,
	ExperienceRated,
	TermType,
	o_PremiumMasterDirectWrittenPremiumAmount11 AS i_PremiumMasterDirectWrittenPremiumAmount11,
	-- *INF*: iif(ISNULL(i_PremiumMasterDirectWrittenPremiumAmount11),0,i_PremiumMasterDirectWrittenPremiumAmount11)
	IFF(
	    i_PremiumMasterDirectWrittenPremiumAmount11 IS NULL, 0,
	    i_PremiumMasterDirectWrittenPremiumAmount11
	) AS v_PremiumMasterDirectWrittenPremiumAmount,
	-- *INF*: iif(round(v_PremiumMasterDirectWrittenPremiumAmount)!=0,1,0)
	IFF(round(v_PremiumMasterDirectWrittenPremiumAmount) != 0, 1, 0) AS ARD_PASS_FLAG,
	'N/A' AS Splitperiodcode,
	o_LossesSubjectToDeductibleCode AS LossesSubjectToDeductibleCode,
	o_BasisOfDeductibleCalculationCode AS BasisOfDeductibleCalculationCode,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID
	FROM join_Split_period_ClassCode_premiums
),
WorkWCSTATAggregatedPremium AS (
	TRUNCATE TABLE WorkWCSTATAggregatedPremium;
	INSERT INTO WorkWCSTATAggregatedPremium
	(AuditId, CreatedDate, ModifiedDate, SourceSystemId, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, PremiumMasterRunDate, LossMasterRunDate, BureauCompanyCode, PolicyKey, StateProvinceCode, PolicyEffectiveDate, PolicyEndDate, InterstateRiskId, EmployeeLeasingCode, StateRatingEffectiveDate, FederalTaxId, ThreeYearFixedRatePolicyIndicator, MultistatePolicyIndicator, InterstateRatedPolicyIndicator, RetrospectiveratedPolicyIndicator, CancelledMidTermPolicyIndicator, ManagedCareOrganizationPolicyIndicator, TypeOfCoverageIdCode, TypeOfPlan, DeductibleAmountPerClaimAccident, InsuredName, WCSTATAddress, PremiumMasterClassCode, ExperienceModificationFactor, ExperienceModificationEffectiveDate, Exposure, PremiumMasterDirectWrittenPremiumAmount, ManualChargedRate, LossMasterClassCode, ClaimLossDate, ClaimOccurrenceStatusCode, InjuryTypeCode, CatastropheCode, IncurredIndemnityAmount, IncurredMedicalAmount, CauseOfLoss, TypeOfRecoveryCode, JurisdictionStateCode, BodyPartCode, NatureOfInjuryCode, CauseOfInjuryCode, PaidIndemnityAmount, PaidMedicalAmount, DeductibleReimbursementAmount, PaidAllocatedLossAdjustmentExpenseAmount, IncurredAllocatedLossAdjustmentExpenseAmount, LossCondition, TypeOfSettlement, ManagedCareOrganizationType, LumpSumIndicator, EstimatedAuditCode, CorrectionSeqNumber, ClaimNumber, RunMonthAuditPremium, AgeOfPolicy, PolicyPremiumTotal, ManualPremiumIndicator, PolicyStateManualPremiumTotal, BalMinPremiumTotal, RateEffectiveDate, AnyARDIndicator, ExperienceRated, TermType, ARDPassFlag, SplitPeriodCode, LossesSubjectToDeductibleCode, BasisOfDeductibleCalculationCode)
	SELECT 
	AuditID AS AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	o_SourceSystemID AS SOURCESYSTEMID, 
	EDWPREMIUMMASTERCALCULATIONPKID, 
	EDWLOSSMASTERCALCULATIONPKID, 
	TYPEBUREAUCODE, 
	PREMIUMMASTERRUNDATE, 
	LOSSMASTERRUNDATE, 
	BUREAUCOMPANYCODE, 
	POLICYKEY, 
	STATEPROVINCECODE, 
	POLICYEFFECTIVEDATE, 
	POLICYENDDATE, 
	INTERSTATERISKID, 
	EMPLOYEELEASINGCODE, 
	STATERATINGEFFECTIVEDATE, 
	FEDERALTAXID, 
	THREEYEARFIXEDRATEPOLICYINDICATOR, 
	MULTISTATEPOLICYINDICATOR, 
	INTERSTATERATEDPOLICYINDICATOR, 
	RETROSPECTIVERATEDPOLICYINDICATOR, 
	CANCELLEDMIDTERMPOLICYINDICATOR, 
	MANAGEDCAREORGANIZATIONPOLICYINDICATOR, 
	TYPEOFCOVERAGEIDCODE, 
	TYPEOFPLAN, 
	DEDUCTIBLEAMOUNTPERCLAIMACCIDENT, 
	INSUREDNAME, 
	WCSTATADDRESS, 
	PREMIUMMASTERCLASSCODE, 
	EXPERIENCEMODIFICATIONFACTOR, 
	EXPERIENCEMODIFICATIONEFFECTIVEDATE, 
	EXPOSURE, 
	PREMIUMMASTERDIRECTWRITTENPREMIUMAMOUNT, 
	MANUALCHARGEDRATE, 
	LOSSMASTERCLASSCODE, 
	CLAIMLOSSDATE, 
	CLAIMOCCURRENCESTATUSCODE, 
	INJURYTYPECODE, 
	CATASTROPHECODE, 
	INCURREDINDEMNITYAMOUNT, 
	INCURREDMEDICALAMOUNT, 
	CAUSEOFLOSS, 
	TYPEOFRECOVERYCODE, 
	JURISDICTIONSTATECODE, 
	BODYPARTCODE, 
	NATUREOFINJURYCODE, 
	CAUSEOFINJURYCODE, 
	PAIDINDEMNITYAMOUNT, 
	PAIDMEDICALAMOUNT, 
	DEDUCTIBLEREIMBURSEMENTAMOUNT, 
	PAIDALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	INCURREDALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	loss_condition AS LOSSCONDITION, 
	TYPEOFSETTLEMENT, 
	MANAGEDCAREORGANIZATIONTYPE, 
	LUMPSUMINDICATOR, 
	ESTIMATEDAUDITCODE, 
	CORRECTIONSEQNUMBER, 
	CLAIMNUMBER, 
	RUNMONTHAUDITPREMIUM, 
	AGEOFPOLICY, 
	POLICYPREMIUMTOTAL, 
	ManualPremiumInd AS MANUALPREMIUMINDICATOR, 
	POLICYSTATEMANUALPREMIUMTOTAL, 
	BALMINPREMIUMTOTAL, 
	RATEEFFECTIVEDATE, 
	ANYARDINDICATOR, 
	EXPERIENCERATED, 
	TERMTYPE, 
	ARD_PASS_FLAG AS ARDPASSFLAG, 
	Splitperiodcode AS SPLITPERIODCODE, 
	LOSSESSUBJECTTODEDUCTIBLECODE, 
	BASISOFDEDUCTIBLECALCULATIONCODE
	FROM EXP_Target_Data
),
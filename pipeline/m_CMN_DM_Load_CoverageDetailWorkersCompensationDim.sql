WITH
LKP_SupClassificationWorkersCompensation AS (
	SELECT
	RatableClassIndicator,
	RatingStateCode,
	ClassCode
	FROM (
		SELECT 
		SupClassificationWorkersCompensation.RatingStateCode as RatingStateCode, 
		SupClassificationWorkersCompensation.ClassCode as ClassCode,
		Case when SupClassificationWorkersCompensation.RatableClassIndicator ='N/A' or  SupClassificationWorkersCompensation.RatableClassIndicator is null 
				  then 'X'
				  Else 	SupClassificationWorkersCompensation.RatableClassIndicator
		End + '-'+
		Case when SupClassificationWorkersCompensation.SurchargeClassIndicator='N/A' or SupClassificationWorkersCompensation.SurchargeClassIndicator is null
				   then 'X'
				  Else  SupClassificationWorkersCompensation.SurchargeClassIndicator
			 End + '-'+
		case when SupClassificationWorkersCompensation.HazardGroupCode='N/A' or SupClassificationWorkersCompensation.HazardGroupCode is null
					then 'X'
					Else SupClassificationWorkersCompensation.HazardGroupCode
			End as RatableClassIndicator 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupClassificationWorkersCompensation
		where SupClassificationWorkersCompensation.CurrentSnapshotFlag=1
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingStateCode,ClassCode ORDER BY RatableClassIndicator) = 1
),
SQ_CoverageDetailWorkersCompensationDim AS (
	select distinct
	(case when SC.ClassCodeOrganizationCode='ISS' then 'ALL' else SS.state_code end)  as state_code,
	SIL.StandardInsuranceLineCode AS StandardInsuranceLineCode,
	PT.ExperienceModificationFactor AS ExperienceModificationFactor,
	PT.ExperienceModificationEffectiveDate AS ExperienceModificationEffectiveDate,
	SC.ClassCode AS ClassCode,
	SC.ClassCodeOrganizationCode AS ClassCodeOrganizationCode,
	SC.StatisticalCoverageEffectiveDate AS CoverageEffectiveDate,
	CDD.CoverageDetailDimID AS CoverageDetailDimID,
	CDD.CoverageGuid AS CoverageGuid,
	CDD.EffectiveDate AS EffectiveDate,
	CDD.ExpirationDate AS ExpirationDate,
	PLT.PolicyPerAccidentLimit AS PolicyPerAccidentLimit,
	PLT.PolicyPerDiseaseLimit AS PolicyPerDiseaseLimit,
	PLT.PolicyAggregateLimit AS PolicyAggregateLimit,
	CDWC.ConsentToRateFlag AS ConsentToRateFlag,
	CDWC.RateOverride AS RateOverride,
	CDWC.AdmiraltyActFlag As AdmiraltyActFlag,
	CDWC.FederalEmployersLiabilityActFlag As FederalEmployersLiabilityActFlag,
	CDWC.USLongShoreAndHarborWorkersCompensationActFlag As USLongShoreAndHarborWorkersCompensationActFlag
	from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	on CDD.EDWPremiumTransactionPKID = PT.PremiumTransactionID
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
	on PT.StatisticalCoverageAKId=SC.StatisticalCoverageAKId
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	on SC.PolicyCoverageAKId=PC.PolicyCoverageAKId
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	on PC.RiskLocationAKID=RL.RiskLocationAKID
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyLimit PLT
	on PC.PolicyLimitAKID=PLT.PolicyLimitAKID 
	and PLT.CurrentSnapshotFlag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailWorkersCompensation CDWC
	on CDWC.PremiumTransactionID=CDD.EDWPremiumTransactionPKID
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL
	on SIL.ins_line_code=PC.InsuranceLine and SIL.crrnt_snpsht_flag=1
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_state SS
	on (case when len(SS.state_abbrev)=1 then '0'+SS.state_abbrev else SS.state_abbrev end)=(case when len(RL.StateProvinceCode)=1 then '0'+RL.StateProvinceCode else RL.StateProvinceCode end) and SS.crrnt_snpsht_flag=1
	where PT.SourceSystemID='PMS' 
	and PT.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}' 
	--and PC.InsuranceLine='WC' 
	and PC.TypeBureauCode in ('WC','WP')
	@{pipeline().parameters.WHERE_CLAUSE_PMS}
	
	union all
	
	select distinct
	(case when RC.ClassCodeOrganizationCode='ISS' then 'ALL' else SS.state_code end)  as state_code,
	SIL.StandardInsuranceLineCode AS StandardInsuranceLineCode,
	PT.ExperienceModificationFactor AS ExperienceModificationFactor,
	PT.ExperienceModificationEffectiveDate AS ExperienceModificationEffectiveDate,
	RC.ClassCode AS ClassCode,
	RC.ClassCodeOrganizationCode AS ClassCodeOrganizationCode,
	RC.RatingCoverageEffectiveDate AS CoverageEffectiveDate,
	CDD.CoverageDetailDimID AS CoverageDetailDimID,
	CDD.CoverageGuid AS CoverageGuid,
	CDD.EffectiveDate AS EffectiveDate,
	CDD.ExpirationDate AS ExpirationDate,
	PLT.PolicyPerAccidentLimit AS PolicyPerAccidentLimit,
	PLT.PolicyPerDiseaseLimit AS PolicyPerDiseaseLimit,
	PLT.PolicyAggregateLimit AS PolicyAggregateLimit,
	CDWC.ConsentToRateFlag AS ConsentToRateFlag,
	CDWC.RateOVerride AS RateOVerride,
	CDWC.AdmiraltyActFlag As AdmiraltyActFlag,
	CDWC.FederalEmployersLiabilityActFlag As FederalEmployersLiabilityActFlag,
	CDWC.USLongShoreAndHarborWorkersCompensationActFlag As USLongShoreAndHarborWorkersCompensationActFlag
	from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	on CDD.EDWPremiumTransactionPKID = PT.PremiumTransactionID
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	on PT.RatingCoverageAKId=RC.RatingCoverageAKID and RC.EffectiveDate=PT.EffectiveDate
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	on RC.PolicyCoverageAKId=PC.PolicyCoverageAKId AND PC.CurrentSnapshotFlag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	on PC.RiskLocationAKID=RL.RiskLocationAKID
	and RL.CurrentSnapshotFlag=1 
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyLimit PLT
	on PC.PolicyLimitAKID=PLT.PolicyLimitAKID 
	and PLT.EffectiveDate <= PT.PremiumTransactionEnteredDate 
	and PLT.ExpirationDate > PT.PremiumTransactionEnteredDate
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailWorkersCompensation CDWC
	on CDWC.PremiumTransactionID=CDD.EDWPremiumTransactionPKID
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL
	on SIL.ins_line_code=PC.InsuranceLine and SIL.crrnt_snpsht_flag=1
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_state SS
	on (case when len(SS.state_abbrev)=1 then '0'+SS.state_abbrev else SS.state_abbrev end)=(case when len(RL.StateProvinceCode)=1 then '0'+RL.StateProvinceCode else RL.StateProvinceCode end) and SS.crrnt_snpsht_flag=1
	where PT.SourceSystemID='DCT' 
	and PT.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}' 
	and PC.InsuranceLine='WorkersCompensation' 
	@{pipeline().parameters.WHERE_CLAUSE_DCT}
),
SRTTRANS AS (
	SELECT
	state_code, 
	StandardInsuranceLineCode, 
	ExperienceModificationFactor, 
	ExperienceModificationEffectiveDate, 
	ClassCode, 
	ClassCodeOrganizationCode, 
	CoverageEffectiveDate, 
	CoverageDetailDimId, 
	CoverageGuid, 
	EffectiveDate, 
	ExpirationDate, 
	PolicyPerAccidentLimit, 
	PolicyPerDiseaseLimit, 
	PolicyAggregateLimit, 
	ConsentToRateFlag, 
	RateOverride, 
	AdmiraltyActFlag, 
	FederalEmployersLiabilityActFlag, 
	USLongShoreAndHarborWorkersCompensationActFlag
	FROM SQ_CoverageDetailWorkersCompensationDim
	ORDER BY EffectiveDate ASC
),
AGGTRANS AS (
	SELECT
	state_code,
	StandardInsuranceLineCode,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	ClassCode,
	ClassCodeOrganizationCode,
	CoverageEffectiveDate,
	CoverageDetailDimId,
	CoverageGuid,
	EffectiveDate,
	ExpirationDate,
	PolicyPerAccidentLimit,
	PolicyPerDiseaseLimit,
	PolicyAggregateLimit,
	ConsentToRateFlag,
	RateOverride,
	AdmiraltyActFlag,
	FederalEmployersLiabilityActFlag,
	USLongShoreAndHarborWorkersCompensationActFlag
	FROM SRTTRANS
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageDetailDimId ORDER BY NULL) = 1
),
LKP_ClassificationReference AS (
	SELECT
	ClassDescription,
	InsuranceLineCode,
	StateCode,
	OriginatingOrganizationCode,
	ClassCode,
	ClassCodeEffectiveDate,
	ClassCodeExpirationDate
	FROM (
		select ClassDescription as ClassDescription, 
		InsuranceLineCode as InsuranceLineCode, 
		StateCode as StateCode, 
		OriginatingOrganizationCode as OriginatingOrganizationCode, 
		ClassCode as ClassCode, 
		ClassCodeEffectiveDate as ClassCodeEffectiveDate, 
		ClassCodeExpirationDate as ClassCodeExpirationDate
		from (
		SELECT ltrim(rtrim(ClassDescription)) as ClassDescription, 
		ltrim(rtrim(InsuranceLineCode)) as InsuranceLineCode, 
		ltrim(rtrim(StateCode)) as StateCode, 
		ltrim(rtrim(OriginatingOrganizationCode)) as OriginatingOrganizationCode, 
		ltrim(rtrim(ClassCode)) as ClassCode, 
		ClassCodeEffectiveDate as ClassCodeEffectiveDate, 
		ClassCodeExpirationDate as ClassCodeExpirationDate,
		row_number() over (partition by InsuranceLineCode, StateCode, OriginatingOrganizationCode,ClassCode, 
		ClassCodeEffectiveDate,
		ClassCodeExpirationDate order by ClassificationReferenceId desc) rn
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.ClassificationReference
		where ltrim(rtrim(OriginatingOrganizationCode))='NCCI'
		)Src
		where rn=1
		order by ClassCodeEffectiveDate --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceLineCode,StateCode,OriginatingOrganizationCode,ClassCode,ClassCodeEffectiveDate,ClassCodeExpirationDate ORDER BY ClassDescription DESC) = 1
),
LKP_CoverageDetailWorkersCompensationDim AS (
	SELECT
	CoverageDetailDimId,
	EffectiveDate,
	ExpirationDate,
	CoverageGuid,
	NcciClassCode,
	NcciClassDescription,
	PolicyPerAccidentLimit,
	PolicyPerDiseaseLimit,
	PolicyAggregateLimit,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	ConsentToRateFlag,
	RateOverride,
	RatableClassIndicator,
	SurchargeClassIndicator,
	HazardGroupCode,
	AdmiraltyActFlag,
	FederalEmployersLiabilityActFlag,
	USLongShoreAndHarborWorkersCompensationActFlag
	FROM (
		SELECT 
			CoverageDetailDimId,
			EffectiveDate,
			ExpirationDate,
			CoverageGuid,
			NcciClassCode,
			NcciClassDescription,
			PolicyPerAccidentLimit,
			PolicyPerDiseaseLimit,
			PolicyAggregateLimit,
			ExperienceModificationFactor,
			ExperienceModificationEffectiveDate,
			ConsentToRateFlag,
			RateOverride,
			RatableClassIndicator,
			SurchargeClassIndicator,
			HazardGroupCode,
			AdmiraltyActFlag,
			FederalEmployersLiabilityActFlag,
			USLongShoreAndHarborWorkersCompensationActFlag
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailWorkersCompensationDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageDetailDimId ORDER BY CoverageDetailDimId) = 1
),
LKP_sup_state AS (
	SELECT
	state_abbrev,
	state_code
	FROM (
		SELECT 
			state_abbrev,
			state_code
		FROM sup_state
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code ORDER BY state_abbrev) = 1
),
EXP_AddMetaData AS (
	SELECT
	LKP_CoverageDetailWorkersCompensationDim.CoverageDetailDimId AS lkp_CoverageDetailDimId,
	LKP_CoverageDetailWorkersCompensationDim.EffectiveDate AS lkp_EffectiveDate,
	LKP_CoverageDetailWorkersCompensationDim.ExpirationDate AS lkp_ExpirationDate,
	LKP_CoverageDetailWorkersCompensationDim.CoverageGuid AS lkp_CoverageGuid,
	LKP_CoverageDetailWorkersCompensationDim.NcciClassCode AS lkp_NcciClassCode,
	LKP_CoverageDetailWorkersCompensationDim.NcciClassDescription AS lkp_NcciClassDescription,
	LKP_CoverageDetailWorkersCompensationDim.PolicyPerAccidentLimit AS lkp_PolicyPerAccidentLimit,
	LKP_CoverageDetailWorkersCompensationDim.PolicyPerDiseaseLimit AS lkp_PolicyPerDiseaseLimit,
	LKP_CoverageDetailWorkersCompensationDim.PolicyAggregateLimit AS lkp_PolicyAggregateLimit,
	LKP_CoverageDetailWorkersCompensationDim.ExperienceModificationFactor AS lkp_ExperienceModificationFactor,
	LKP_CoverageDetailWorkersCompensationDim.ExperienceModificationEffectiveDate AS lkp_ExperienceModificationEffectiveDate,
	LKP_CoverageDetailWorkersCompensationDim.ConsentToRateFlag AS lkp_ConsentToRateFlag,
	LKP_CoverageDetailWorkersCompensationDim.RateOverride AS lkp_RateOverride,
	LKP_CoverageDetailWorkersCompensationDim.RatableClassIndicator AS lkp_RatableClassIndicator,
	LKP_CoverageDetailWorkersCompensationDim.SurchargeClassIndicator AS lkp_SurchargeClassIndicator,
	LKP_CoverageDetailWorkersCompensationDim.HazardGroupCode AS lkp_HazardGroupCode,
	LKP_CoverageDetailWorkersCompensationDim.AdmiraltyActFlag AS lkp_AdmiraltyActFlag,
	LKP_CoverageDetailWorkersCompensationDim.FederalEmployersLiabilityActFlag AS lkp_FederalEmployersLiabilityActFlag,
	LKP_CoverageDetailWorkersCompensationDim.USLongShoreAndHarborWorkersCompensationActFlag AS lkp_USLongShoreAndHarborWorkersCompensationActFlag,
	AGGTRANS.ExperienceModificationFactor AS i_ExperienceModificationFactor,
	AGGTRANS.ExperienceModificationEffectiveDate AS i_ExperienceModificationEffectiveDate,
	AGGTRANS.ClassCode AS i_ClassCode,
	AGGTRANS.ClassCodeOrganizationCode AS i_ClassCodeOrganizationCode,
	LKP_ClassificationReference.ClassDescription AS i_NcciClassDescription,
	AGGTRANS.CoverageDetailDimId AS i_CoverageDetailDimId,
	AGGTRANS.CoverageGuid AS i_CoverageGuid,
	AGGTRANS.EffectiveDate AS i_EffectiveDate,
	AGGTRANS.ExpirationDate AS i_ExpirationDate,
	AGGTRANS.PolicyPerAccidentLimit AS i_PolicyPerAccidentLimit,
	AGGTRANS.PolicyPerDiseaseLimit AS i_PolicyPerDiseaseLimit,
	AGGTRANS.PolicyAggregateLimit AS i_PolicyAggregateLimit,
	AGGTRANS.ConsentToRateFlag AS i_ConsentToRateFlag,
	AGGTRANS.RateOverride AS i_RateOverride,
	LKP_sup_state.state_abbrev AS i_StateCode,
	AGGTRANS.AdmiraltyActFlag AS i_AdmiraltyActFlag,
	AGGTRANS.FederalEmployersLiabilityActFlag AS i_FederalEmployersLiabilityActFlag,
	AGGTRANS.USLongShoreAndHarborWorkersCompensationActFlag AS i_USLongShoreAndHarborWorkersCompensationActFlag,
	-- *INF*: IIF(ISNULL(i_EffectiveDate), TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'), i_EffectiveDate)
	IFF(i_EffectiveDate IS NULL,
		TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'
		),
		i_EffectiveDate
	) AS v_EffectiveDate,
	-- *INF*: IIF(ISNULL(i_ExpirationDate), TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'), i_ExpirationDate)
	IFF(i_ExpirationDate IS NULL,
		TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'
		),
		i_ExpirationDate
	) AS v_ExpirationDate,
	-- *INF*: IIF(ISNULL(i_CoverageGuid), 'N/A', i_CoverageGuid)
	IFF(i_CoverageGuid IS NULL,
		'N/A',
		i_CoverageGuid
	) AS v_CoverageGuid,
	-- *INF*: IIF(i_ClassCodeOrganizationCode='NCCI',i_ClassCode, 'N/A')
	IFF(i_ClassCodeOrganizationCode = 'NCCI',
		i_ClassCode,
		'N/A'
	) AS v_NcciClassCode,
	-- *INF*: IIF(ISNULL(i_NcciClassDescription), 'N/A', i_NcciClassDescription)
	IFF(i_NcciClassDescription IS NULL,
		'N/A',
		i_NcciClassDescription
	) AS v_NcciClassDescription,
	-- *INF*: IIF(ISNULL(i_PolicyPerAccidentLimit), 'N/A', i_PolicyPerAccidentLimit)
	IFF(i_PolicyPerAccidentLimit IS NULL,
		'N/A',
		i_PolicyPerAccidentLimit
	) AS v_PolicyPerAccidentLimit,
	-- *INF*: IIF(ISNULL(i_PolicyPerDiseaseLimit), 'N/A', i_PolicyPerDiseaseLimit)
	IFF(i_PolicyPerDiseaseLimit IS NULL,
		'N/A',
		i_PolicyPerDiseaseLimit
	) AS v_PolicyPerDiseaseLimit,
	-- *INF*: IIF(ISNULL(i_PolicyAggregateLimit), 'N/A', i_PolicyAggregateLimit)
	IFF(i_PolicyAggregateLimit IS NULL,
		'N/A',
		i_PolicyAggregateLimit
	) AS v_PolicyAggregateLimit,
	-- *INF*: IIF(ISNULL(i_ExperienceModificationFactor), 0, i_ExperienceModificationFactor)
	IFF(i_ExperienceModificationFactor IS NULL,
		0,
		i_ExperienceModificationFactor
	) AS v_ExperienceModificationFactor,
	-- *INF*: IIF(ISNULL(i_ExperienceModificationEffectiveDate),TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),i_ExperienceModificationEffectiveDate)
	IFF(i_ExperienceModificationEffectiveDate IS NULL,
		TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'
		),
		i_ExperienceModificationEffectiveDate
	) AS v_ExperienceModificationEffectiveDate,
	-- *INF*: DECODE(i_ConsentToRateFlag,'T','1','F','0','0')
	DECODE(i_ConsentToRateFlag,
		'T', '1',
		'F', '0',
		'0'
	) AS v_ConsentToRateFlag,
	-- *INF*: IIF(ISNULL(i_RateOverride), 0, i_RateOverride)
	IFF(i_RateOverride IS NULL,
		0,
		i_RateOverride
	) AS v_RateOverride,
	-- *INF*: SUBSTR(i_ClassCode,0,4)
	SUBSTR(i_ClassCode, 0, 4
	) AS v_ClassCode,
	-- *INF*: IIF	(ISNULL(:LKP.LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION(i_StateCode,v_ClassCode)),:LKP.LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION('99',v_ClassCode),:LKP.LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION(i_StateCode,v_ClassCode))
	IFF(LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_i_StateCode_v_ClassCode.RatableClassIndicator IS NULL,
		LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION__99_v_ClassCode.RatableClassIndicator,
		LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_i_StateCode_v_ClassCode.RatableClassIndicator
	) AS v_RatableSurchargeCombined,
	-- *INF*: Decode(SUBSTR(v_RatableSurchargeCombined,1,1), 'X', 'N/A' , NULL , 'N/A',SUBSTR(v_RatableSurchargeCombined,1,1))
	Decode(SUBSTR(v_RatableSurchargeCombined, 1, 1
		),
		'X', 'N/A',
		NULL, 'N/A',
		SUBSTR(v_RatableSurchargeCombined, 1, 1
		)
	) AS v_RatableClassIndicator,
	-- *INF*: Decode(SUBSTR(v_RatableSurchargeCombined,3,1),'X','N/A',NULL,'N/A',SUBSTR(v_RatableSurchargeCombined,3,1))
	Decode(SUBSTR(v_RatableSurchargeCombined, 3, 1
		),
		'X', 'N/A',
		NULL, 'N/A',
		SUBSTR(v_RatableSurchargeCombined, 3, 1
		)
	) AS v_SurchargeClassIndicator,
	-- *INF*: Decode(SUBSTR(v_RatableSurchargeCombined,5,2),'X','N/A',NULL,'N/A',SUBSTR(v_RatableSurchargeCombined,5,2))
	Decode(SUBSTR(v_RatableSurchargeCombined, 5, 2
		),
		'X', 'N/A',
		NULL, 'N/A',
		SUBSTR(v_RatableSurchargeCombined, 5, 2
		)
	) AS v_HazardGroupCode,
	-- *INF*: DECODE(i_AdmiraltyActFlag,'T','1','F','0','0')
	DECODE(i_AdmiraltyActFlag,
		'T', '1',
		'F', '0',
		'0'
	) AS v_AdmiraltyActFlag,
	-- *INF*: DECODE(i_FederalEmployersLiabilityActFlag,'T','1','F','0','0')
	DECODE(i_FederalEmployersLiabilityActFlag,
		'T', '1',
		'F', '0',
		'0'
	) AS v_FederalEmployersLiabilityActFlag,
	-- *INF*: DECODE(i_USLongShoreAndHarborWorkersCompensationActFlag,'T','1','F','0','0')
	DECODE(i_USLongShoreAndHarborWorkersCompensationActFlag,
		'T', '1',
		'F', '0',
		'0'
	) AS v_USLongShoreAndHarborWorkersCompensationActFlag,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_CoverageDetailDimId), 'NEW',
	-- lkp_EffectiveDate<>v_EffectiveDate
	-- OR lkp_ExpirationDate<>v_ExpirationDate
	-- OR lkp_CoverageGuid<>v_CoverageGuid
	-- OR LTRIM(RTRIM(lkp_NcciClassCode))<>v_NcciClassCode
	-- OR lkp_NcciClassDescription<>v_NcciClassDescription
	-- OR lkp_PolicyPerAccidentLimit<>v_PolicyPerAccidentLimit
	-- OR lkp_PolicyPerDiseaseLimit<>v_PolicyPerDiseaseLimit
	-- OR lkp_PolicyAggregateLimit<>v_PolicyAggregateLimit
	-- OR lkp_ExperienceModificationFactor<>v_ExperienceModificationFactor
	-- OR lkp_ExperienceModificationEffectiveDate<>v_ExperienceModificationEffectiveDate
	-- OR DECODE(lkp_ConsentToRateFlag,'T','1','F','0','0')<>v_ConsentToRateFlag
	-- OR lkp_RateOverride<>v_RateOverride
	-- OR  lkp_RatableClassIndicator <> v_RatableClassIndicator
	-- OR lkp_SurchargeClassIndicator <> v_SurchargeClassIndicator
	-- OR lkp_HazardGroupCode <> v_HazardGroupCode
	-- OR DECODE(lkp_AdmiraltyActFlag,'T','1','F','0','0')<>v_AdmiraltyActFlag
	-- OR DECODE(lkp_FederalEmployersLiabilityActFlag,'T','1','F','0','0')<>v_FederalEmployersLiabilityActFlag
	-- OR DECODE(lkp_USLongShoreAndHarborWorkersCompensationActFlag,'T','1','F','0','0')<>v_USLongShoreAndHarborWorkersCompensationActFlag
	-- , 'UPDATE', 'NOCHANGE')
	DECODE(TRUE,
		lkp_CoverageDetailDimId IS NULL, 'NEW',
		lkp_EffectiveDate <> v_EffectiveDate 
		OR lkp_ExpirationDate <> v_ExpirationDate 
		OR lkp_CoverageGuid <> v_CoverageGuid 
		OR LTRIM(RTRIM(lkp_NcciClassCode
			)
		) <> v_NcciClassCode 
		OR lkp_NcciClassDescription <> v_NcciClassDescription 
		OR lkp_PolicyPerAccidentLimit <> v_PolicyPerAccidentLimit 
		OR lkp_PolicyPerDiseaseLimit <> v_PolicyPerDiseaseLimit 
		OR lkp_PolicyAggregateLimit <> v_PolicyAggregateLimit 
		OR lkp_ExperienceModificationFactor <> v_ExperienceModificationFactor 
		OR lkp_ExperienceModificationEffectiveDate <> v_ExperienceModificationEffectiveDate 
		OR DECODE(lkp_ConsentToRateFlag,
		'T', '1',
		'F', '0',
		'0'
		) <> v_ConsentToRateFlag 
		OR lkp_RateOverride <> v_RateOverride 
		OR lkp_RatableClassIndicator <> v_RatableClassIndicator 
		OR lkp_SurchargeClassIndicator <> v_SurchargeClassIndicator 
		OR lkp_HazardGroupCode <> v_HazardGroupCode 
		OR DECODE(lkp_AdmiraltyActFlag,
		'T', '1',
		'F', '0',
		'0'
		) <> v_AdmiraltyActFlag 
		OR DECODE(lkp_FederalEmployersLiabilityActFlag,
		'T', '1',
		'F', '0',
		'0'
		) <> v_FederalEmployersLiabilityActFlag 
		OR DECODE(lkp_USLongShoreAndHarborWorkersCompensationActFlag,
		'T', '1',
		'F', '0',
		'0'
		) <> v_USLongShoreAndHarborWorkersCompensationActFlag, 'UPDATE',
		'NOCHANGE'
	) AS o_ChangeFlag,
	i_CoverageDetailDimId AS o_CoverageDetailDimId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	v_EffectiveDate AS o_EffectiveDate,
	v_ExpirationDate AS o_ExpirationDate,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	v_CoverageGuid AS o_CoverageGuid,
	v_NcciClassCode AS o_NcciClassCode,
	v_NcciClassDescription AS o_NcciClassDescription,
	v_PolicyPerAccidentLimit AS o_PolicyPerAccidentLimit,
	v_PolicyPerDiseaseLimit AS o_PolicyPerDiseaseLimit,
	v_PolicyAggregateLimit AS o_PolicyAggregateLimit,
	v_ExperienceModificationFactor AS o_ExperienceModificationFactor,
	v_ExperienceModificationEffectiveDate AS o_ExperienceModificationEffectiveDate,
	v_ConsentToRateFlag AS o_ConsentToRateFlag,
	v_RateOverride AS o_RateOverride,
	v_RatableClassIndicator AS o_RatableClassIndicator,
	v_SurchargeClassIndicator AS o_SurchargeClassIndicator,
	v_HazardGroupCode AS o_HazardGroupCode,
	v_AdmiraltyActFlag AS o_AdmiraltyActFlag,
	v_FederalEmployersLiabilityActFlag AS o_FederalEmployersLiabilityActFlag,
	v_USLongShoreAndHarborWorkersCompensationActFlag AS o_USLongShoreAndHarborWorkersCompensationActFlag
	FROM AGGTRANS
	LEFT JOIN LKP_ClassificationReference
	ON LKP_ClassificationReference.InsuranceLineCode = AGGTRANS.StandardInsuranceLineCode AND LKP_ClassificationReference.StateCode = AGGTRANS.state_code AND LKP_ClassificationReference.OriginatingOrganizationCode = AGGTRANS.ClassCodeOrganizationCode AND LKP_ClassificationReference.ClassCode = AGGTRANS.ClassCode AND LKP_ClassificationReference.ClassCodeEffectiveDate <= AGGTRANS.CoverageEffectiveDate AND LKP_ClassificationReference.ClassCodeExpirationDate >= AGGTRANS.CoverageEffectiveDate
	LEFT JOIN LKP_CoverageDetailWorkersCompensationDim
	ON LKP_CoverageDetailWorkersCompensationDim.CoverageDetailDimId = AGGTRANS.CoverageDetailDimId
	LEFT JOIN LKP_sup_state
	ON LKP_sup_state.state_code = AGGTRANS.state_code
	LEFT JOIN LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_i_StateCode_v_ClassCode
	ON LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_i_StateCode_v_ClassCode.RatingStateCode = i_StateCode
	AND LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_i_StateCode_v_ClassCode.ClassCode = v_ClassCode

	LEFT JOIN LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION__99_v_ClassCode
	ON LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION__99_v_ClassCode.RatingStateCode = '99'
	AND LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION__99_v_ClassCode.ClassCode = v_ClassCode

),
RTR_CoverageDetailWorkersCompensationDim AS (
	SELECT
	o_ChangeFlag AS ChangeFlag,
	o_CoverageDetailDimId AS CoverageDetailDimId,
	o_AuditID AS AuditID,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_CoverageGuid AS CoverageGuid,
	o_NcciClassCode AS NcciClassCode,
	o_NcciClassDescription AS NcciClassDescription,
	o_PolicyPerAccidentLimit AS PolicyPerAccidentLimit,
	o_PolicyPerDiseaseLimit AS PolicyPerDiseaseLimit,
	o_PolicyAggregateLimit AS PolicyAggregateLimit,
	o_ExperienceModificationFactor AS ExperienceModificationFactor,
	o_ExperienceModificationEffectiveDate AS ExperienceModificationEffectiveDate,
	o_ConsentToRateFlag,
	o_RateOverride,
	o_RatableClassIndicator,
	o_SurchargeClassIndicator,
	o_HazardGroupCode,
	o_AdmiraltyActFlag,
	o_FederalEmployersLiabilityActFlag,
	o_USLongShoreAndHarborWorkersCompensationActFlag
	FROM EXP_AddMetaData
),
RTR_CoverageDetailWorkersCompensationDim_Insert AS (SELECT * FROM RTR_CoverageDetailWorkersCompensationDim WHERE ChangeFlag='NEW'),
RTR_CoverageDetailWorkersCompensationDim_Update AS (SELECT * FROM RTR_CoverageDetailWorkersCompensationDim WHERE ChangeFlag='UPDATE'),
UPD_Existing AS (
	SELECT
	CoverageDetailDimId, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	ModifiedDate, 
	CoverageGuid, 
	NcciClassCode, 
	NcciClassDescription, 
	PolicyPerAccidentLimit, 
	PolicyPerDiseaseLimit, 
	PolicyAggregateLimit, 
	ExperienceModificationFactor, 
	ExperienceModificationEffectiveDate, 
	o_ConsentToRateFlag AS o_ConsentToRateFlag3, 
	o_RateOverride AS o_RateOverride3, 
	o_RatableClassIndicator AS o_RatableClassIndicator3, 
	o_SurchargeClassIndicator AS o_SurchargeClassIndicator3, 
	o_HazardGroupCode AS o_HazardGroupCode3, 
	o_AdmiraltyActFlag AS o_AdmiraltyActFlag3, 
	o_FederalEmployersLiabilityActFlag AS o_FederalEmployersLiabilityActFlag3, 
	o_USLongShoreAndHarborWorkersCompensationActFlag AS o_USLongShoreAndHarborWorkersCompensationActFlag3
	FROM RTR_CoverageDetailWorkersCompensationDim_Update
),
TGT_CoverageDetailWorkersCompensationDim_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailWorkersCompensationDim AS T
	USING UPD_Existing AS S
	ON 
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CoverageDetailDimId = S.CoverageDetailDimId, T.AuditId = S.AuditID, T.EffectiveDate = S.EffectiveDate, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate, T.CoverageGuid = S.CoverageGuid, T.NcciClassCode = S.NcciClassCode, T.NcciClassDescription = S.NcciClassDescription, T.PolicyPerAccidentLimit = S.PolicyPerAccidentLimit, T.PolicyPerDiseaseLimit = S.PolicyPerDiseaseLimit, T.PolicyAggregateLimit = S.PolicyAggregateLimit, T.ExperienceModificationFactor = S.ExperienceModificationFactor, T.ExperienceModificationEffectiveDate = S.ExperienceModificationEffectiveDate, T.ConsentToRateFlag = S.o_ConsentToRateFlag3, T.RateOverride = S.o_RateOverride3, T.RatableClassIndicator = S.o_RatableClassIndicator3, T.SurchargeClassIndicator = S.o_SurchargeClassIndicator3, T.HazardGroupCode = S.o_HazardGroupCode3, T.AdmiraltyActFlag = S.o_AdmiraltyActFlag3, T.FederalEmployersLiabilityActFlag = S.o_FederalEmployersLiabilityActFlag3, T.USLongShoreAndHarborWorkersCompensationActFlag = S.o_USLongShoreAndHarborWorkersCompensationActFlag3
),
TGT_CoverageDetailWorkersCompensationDim_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailWorkersCompensationDim
	(CoverageDetailDimId, AuditId, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, CoverageGuid, NcciClassCode, NcciClassDescription, PolicyPerAccidentLimit, PolicyPerDiseaseLimit, PolicyAggregateLimit, ExperienceModificationFactor, ExperienceModificationEffectiveDate, ConsentToRateFlag, RateOverride, RatableClassIndicator, SurchargeClassIndicator, HazardGroupCode, AdmiraltyActFlag, FederalEmployersLiabilityActFlag, USLongShoreAndHarborWorkersCompensationActFlag)
	SELECT 
	COVERAGEDETAILDIMID, 
	AuditID AS AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	COVERAGEGUID, 
	NCCICLASSCODE, 
	NCCICLASSDESCRIPTION, 
	POLICYPERACCIDENTLIMIT, 
	POLICYPERDISEASELIMIT, 
	POLICYAGGREGATELIMIT, 
	EXPERIENCEMODIFICATIONFACTOR, 
	EXPERIENCEMODIFICATIONEFFECTIVEDATE, 
	o_ConsentToRateFlag AS CONSENTTORATEFLAG, 
	o_RateOverride AS RATEOVERRIDE, 
	o_RatableClassIndicator AS RATABLECLASSINDICATOR, 
	o_SurchargeClassIndicator AS SURCHARGECLASSINDICATOR, 
	o_HazardGroupCode AS HAZARDGROUPCODE, 
	o_AdmiraltyActFlag AS ADMIRALTYACTFLAG, 
	o_FederalEmployersLiabilityActFlag AS FEDERALEMPLOYERSLIABILITYACTFLAG, 
	o_USLongShoreAndHarborWorkersCompensationActFlag AS USLONGSHOREANDHARBORWORKERSCOMPENSATIONACTFLAG
	FROM RTR_CoverageDetailWorkersCompensationDim_Insert
),
WITH
LKP_RatingPlanForSmallDeductible AS (
	SELECT
	PolicyNumber,
	PolicyVersion
	FROM (
		select distinct P.PolicyNumber as PolicyNumber,
		ISNULL(RIGHT('00'+CONVERT(VARCHAR(3),P.PolicyVersion),2),'00') as PolicyVersion
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTTransactionInsuranceLineLocationBridge B
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTCoverageTransaction C
		on C.CoverageId=B.CoverageId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTInsuranceLine L
		on L.LineId=B.LineId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy P
		on L.PolicyId=P.PolicyId
		and
		P.PolicyStatus<>'Quote'
		and
		P.TransactionState='committed'
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCClassCodeStaging CC
		on CC.ObjectId=C.ParentCoverageObjectId and CC.ObjectName='DC_WC_Risk'
		and L.LineType='WorkersCompensation' and LTRIM(RTRIM(C.ParentCoverageObjectName))='DC_WC_Risk' 
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCStatCodeStaging STC2
		on STC2.ObjectId=C.CoverageId and STC2.ObjectName='DC_Coverage' and STC2.Type in ('AdmiraltyBalanceToMinimumCharge',
		'AircraftSeatSurcharge',
		'AlcoholDrugFreeWorkplace',
		'AlcoholOrDrugFreeWorkplaceCoalMinePremiumCredit',
		'AlcoholOrDrugFreeWorkplacePremiumCredit',
		'AlcoholOrDrugFreeWorkplaceSubjectPremiumCredit',
		'AtomicEnergyRadiationExposureCharge',
		'BalancetoMinimum',
		'CertifiedRiskManagement',
		'Class',
		'CompulsoryWorkplace',
		'ContractorsCredit',
		'DTEC',
		'EmployeeLeasingStatCode',
		'EmployersLiabilityIncreasedBalanceToMinimumCharge',
		'EmployersLiabilityIncreasedLimits',
		'ExpenseConstant',
		'ExperienceModification',
		'FELAIncreasedLimitsPremium',
		'ForeignVoluntaryCompensationFlatFeeStatCode',
		'ManagedCareFactor',
		'MeritRatingCreditPremium',
		'MeritRatingDebitPremium',
		'MeritRatingStatCode',
		'PremiumDiscount',
		'PremiumDiscountTypeA',
		'PremiumIncentiveForSmallEmployees',
		'PreviouslyInjuredEmployee',
		'Repatriation',
		'SafetyCertificationCreditProgram',
		'ScheduledCreditModification',
		'ScheduledModification',
		'SmallDeductibleCredit',
		'StrikeDutySurcharge',
		'TabularAdjustmentCreditProgram',
		'TabularAdjustmentDebitProgram',
		'TerrorismRiskInsuranceProgramReauthorizationAct',
		'TRIA',
		'VoluntaryCompensationFlatFee',
		'VoluntaryCompensationFlatFeeStatCode',
		'VolunteerAmbulance',
		'VolunteerFirefighter',
		'WaiverOfSubrogation',
		'WorkplaceSafetyIncentiveProgramSubject')
		and L.LineType='WorkersCompensation'
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCStatCodeStaging STC
		on STC.ObjectId=C.CoverageId and STC.ObjectName='DC_Coverage' and STC.Type in ('Class') 
		where B.RatingPlan='Guaranteed' 
		and  case when L.LineType='WorkersCompensation' and LTRIM(RTRIM(C.ParentCoverageObjectName))='DC_WC_Risk'  then substring(ltrim(rtrim(CC.Value)), 1, 4) 
		when L.LineType='WorkersCompensation' then substring(ltrim(rtrim(STC2.Value)), 1, 4) else substring(ltrim(rtrim(STC.Value)), 1, 4) end  in ('9657','9663','9664','9667','9668','9669','9670','9671','9672','9673','9674','9675','9676','9677','9678','9679','9758','9759','9760','9761','9762','9763','9764','9770','9771','9772','9773','9780','9781','9784','9785','9786','9787','9788','9789','9790','9791','9792','9793','9794','9795','9796','9797','9798','9799','9801','9870','9871','9872','9878','9881','9882','9888','9895','9900','9901','9902','9903','9904','9905','9906','9907','9908','9909','9910','9911','9912','9913','9914','9915','9916','9917','9918','9919','9920','9924','9925','9926','9927','9928','9929','9930','9931','9932','9933','9934','9935','9936','9937','9938','9939','9940','9941','9942','9943','9944','9945','9946','9947','9948','9949','9950','9951','9952','9953','9954','9955','9970','9971','9972','9973','9974','9975','9981','9982','9983','9986','9987','9991','9992')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber,PolicyVersion ORDER BY PolicyNumber) = 1
),
LKP_RatingPlanForSmallDeductible2 AS (
	SELECT
	RatingPlanAKId,
	PolicyNumber,
	PolicyVersion
	FROM (
		SELECT PC.RatingPlanAKId as RatingPlanAKId, 
		P.pol_num as PolicyNumber, 
		P.pol_mod as PolicyVersion 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC
		join @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Policy P
		on PC.PolicyAKID=P.pol_ak_id and P.crrnt_snpsht_flag=1
		join @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingPlan RP
		on PC.RatingPlanAKID=RP.RatingPlanAKID and RP.CurrentSnapshotFlag=1
		where PC.SourceSystemId='DCT' 
		and RP.RatingPlanDescription='Small Deductible'
		and exists (select 1 
		from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WP
		where P.pol_num=WP.PolicyNumber and P.pol_mod=ISNULL(RIGHT('00'+CONVERT(VARCHAR(3),WP.PolicyVersion),2),'00') )
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber,PolicyVersion ORDER BY RatingPlanAKId) = 1
),
LKP_RatingCoverage_Risk AS (
	SELECT
	RiskLocationAKID,
	PolicyAKID,
	CoverageGUID
	FROM (
		SELECT R.RiskLocationAKID as RiskLocationAKID, R.PolicyAKID as PolicyAKID, RC.CoverageGUID as CoverageGUID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC
		INNER HASH JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC ON RC.PolicyCoverageAKID = PC.PolicyCoverageAKID
		and RC.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation R ON R.RiskLocationAKID = PC.RiskLocationAKID
			AND PC.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
			AND PC.CurrentSnapshotFlag = 1
		AND R.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
			AND R.CurrentSnapshotFlag = 1
			and  exists( select 1 from @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT 
		on WCT.PolicyNumber=pol.pol_num and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol.pol_mod  
		and pol.crrnt_snpsht_flag=1 and R.PolicyAKId=pol.pol_ak_id) 
		ORDER BY RC.RatingCoverageAKID
			,rc.EffectiveDate --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID,CoverageGUID ORDER BY RiskLocationAKID DESC) = 1
),
SQ_PolicyCoverage_SRC AS (
	SELECT distinct WorkDCTPolicy.SessionId, WorkDCTPolicy.PartyId, WorkDCTPolicy.PolicyGUId, WorkDCTPolicy.PolicyEffectiveDate,
	 WorkDCTPolicy.PolicyExpirationDate, WorkDCTPolicy.PolicyVersion, WorkDCTInsuranceLine.LineType, WorkDCTPolicy.CustomerNum,
	 WorkDCTInsuranceLine.RiskGrade, WorkDCTInsuranceLine.IsAuditable, WorkDCTInsuranceLine.PriorCarrierName, WorkDCTPolicy.LineOfBusiness, 
	 WorkDCTPolicy.PolicyNumber, WorkDCTInsuranceLine.PriorPolicyNumber, WorkDCTLocation.Territory, WorkDCTLocation.LocationNumber, 
	 WorkDCTLocation.LocationXmlId, WorkDCTLocation.StateProvince,WorkDCTInsuranceLine.CommissionCustomerCareAmount,Bridge.RatingPlan,
	 WorkDCTCoverageTransaction.ParentCoverageObjectName,WorkDCTCoverageTransaction.CoverageGUID
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy
	inner hash join
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTInsuranceLine
	on 
	WorkDCTPolicy.PolicyId=WorkDCTInsuranceLine.PolicyId
	and
	WorkDCTPolicy.PolicyStatus<>'Quote'
	and
	WorkDCTPolicy.TransactionState='committed'
	inner hash join
	(select distinct LineId,LocationAssociationId, RatingPlan,CoverageId from
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTTransactionInsuranceLineLocationBridge) Bridge
	on
	Bridge.LineId=WorkDCTInsuranceLine.LineId
	inner hash join
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTLocation
	on
	WorkDCTLocation.LocationAssociationId=Bridge.LocationAssociationId
	inner Hash Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTCoverageTransaction
	on WorkDCTCoverageTransaction.CoverageId=Bridge.CoverageId
	WHERE
	WorkDCTPolicy.TransactionType NOT IN ('RescindNonRenew','Reporting','VoidReporting','Information','Dividend','RevisedDividend',
	'VoidDividend','NonRenew','RescindCancelPending','CancelPending')
	@{pipeline().parameters.EXCLUDE_TTYPE}
	order by WorkDCTPolicy.SessionId
),
EXP_Pol_AK_ID AS (
	SELECT
	PolicyNumber AS i_PolicyNumber,
	PolicyVersion AS i_PolicyVersion,
	Territory AS i_Territory,
	LocationNumber AS i_LocationNumber,
	LocationXmlId AS i_LocationXmlId,
	-- *INF*: rtrim(ltrim(:UDF.DEFAULT_VALUE_FOR_STRINGS(i_PolicyNumber)))
	rtrim(ltrim(:UDF.DEFAULT_VALUE_FOR_STRINGS(i_PolicyNumber))) AS v_PolicyNumber,
	-- *INF*: rtrim(ltrim(IIF(ISNULL(i_PolicyVersion),'00',LPAD(TO_CHAR(i_PolicyVersion),2,'0'))))
	rtrim(ltrim(IFF(i_PolicyVersion IS NULL, '00', LPAD(TO_CHAR(i_PolicyVersion), 2, '0')))) AS v_PolicyVersion,
	-- *INF*: IIF(ISNULL(i_LocationNumber) or IS_SPACES(i_LocationNumber) or LENGTH(i_LocationNumber)=0,'0000',LPAD(LTRIM(RTRIM(i_LocationNumber)), 4, '0'))
	IFF(i_LocationNumber IS NULL OR IS_SPACES(i_LocationNumber) OR LENGTH(i_LocationNumber) = 0, '0000', LPAD(LTRIM(RTRIM(i_LocationNumber)), 4, '0')) AS v_LocationNumber,
	v_LocationNumber AS o_LocationNumber,
	-- *INF*: IIF(ISNULL(i_Territory) or IS_SPACES(i_Territory) or LENGTH(i_Territory)=0,'N/A',LTRIM(RTRIM(i_Territory)))
	IFF(i_Territory IS NULL OR IS_SPACES(i_Territory) OR LENGTH(i_Territory) = 0, 'N/A', LTRIM(RTRIM(i_Territory))) AS v_Territory,
	v_Territory AS o_Territory,
	-- *INF*: IIF(ISNULL(i_LocationXmlId) or IS_SPACES(i_LocationXmlId) or LENGTH(i_LocationXmlId)=0,'N/A',LTRIM(RTRIM(i_LocationXmlId)))
	IFF(i_LocationXmlId IS NULL OR IS_SPACES(i_LocationXmlId) OR LENGTH(i_LocationXmlId) = 0, 'N/A', LTRIM(RTRIM(i_LocationXmlId))) AS v_LocationXmlId,
	v_PolicyNumber||v_PolicyVersion AS o_PolicyKey,
	-- *INF*: v_LocationXmlId
	-- --Change for UID new approch
	-- --v_LocationNumber||'~'||v_Territory||'~'||v_LocationXmlId
	v_LocationXmlId AS o_RiskLocationKey,
	CoverageGUID
	FROM SQ_PolicyCoverage_SRC
),
LKP_Policy AS (
	SELECT
	pol_ak_id,
	pol_key
	FROM (
		SELECT 
			pol_ak_id,
			pol_key
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy
		WHERE crrnt_snpsht_flag=1 AND source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and exists (
		select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT
		where WCT.PolicyNumber=pol_num
		and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol_mod)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_ak_id) = 1
),
EXP_RiskLocation_key AS (
	SELECT
	LKP_Policy.pol_ak_id AS Pol_ak_id,
	EXP_Pol_AK_ID.o_RiskLocationKey AS RiskLocationKey,
	EXP_Pol_AK_ID.o_LocationNumber AS LocationNumber,
	EXP_Pol_AK_ID.o_Territory AS Territory,
	SQ_PolicyCoverage_SRC.LineType AS i_LineType,
	SQ_PolicyCoverage_SRC.ParentCoverageObjectName AS i_ParentCoverageObjectName,
	SQ_PolicyCoverage_SRC.StateProvince AS i_StateProvince,
	-- *INF*: IIF(isnull(Pol_ak_id),-1,Pol_ak_id)
	IFF(Pol_ak_id IS NULL, - 1, Pol_ak_id) AS v_PolicyAKID,
	-- *INF*: IIF(ISNULL(i_LineType) or IS_SPACES(i_LineType) or LENGTH(i_LineType)=0,'N/A',LTRIM(RTRIM(i_LineType)))
	IFF(i_LineType IS NULL OR IS_SPACES(i_LineType) OR LENGTH(i_LineType) = 0, 'N/A', LTRIM(RTRIM(i_LineType))) AS v_LineType,
	EXP_Pol_AK_ID.CoverageGUID,
	-- *INF*: :LKP.LKP_RatingCoverage_Risk(v_PolicyAKID,CoverageGUID)
	-- -- Condition is to pull RiskLocationAKID for Policy Their Coverage is already present
	LKP_RATINGCOVERAGE_RISK_v_PolicyAKID_CoverageGUID.RiskLocationAKID AS v_RiskLocationAKID,
	-- *INF*: DECODE (TRUE,
	-- in (i_ParentCoverageObjectName,  'DC_BP_Location','DC_BP_Risk','DC_CA_Risk','DC_CF_Risk','DC_GL_Risk','DC_IM_Risk','DC_WC_Risk','DCBPLocation','WB_GOC_Risk','WB_HIO_Risk', 'WB_EC_Risk')=1,
	-- v_PolicyAKID||'~'||RiskLocationKey,
	-- IN(i_ParentCoverageObjectName,'DC_CA_State','DC_WC_StateTerm','WB_GOC_State','WB_HIO_State','WB_IM_State','WB_EC_State','DC_WC_State')=1,
	-- v_PolicyAKID||'~'||'PrimaryLocation'||'~'||i_StateProvince||'~'||v_LineType ,
	-- IN(i_ParentCoverageObjectName,'DC_CR_Endorsement','DC_CR_Risk','DC_CR_RiskCrime','DC_Line','DC_CU_UmbrellaEmployersLiability','DC_IM_CoverageForm','WB_CU_PremiumDetail','DC_CA_BusinessInterruptionOption')=1,
	-- v_PolicyAKID||'~'||'PrimaryLocation'||'~'||v_LineType
	-- )
	DECODE(TRUE,
		in(i_ParentCoverageObjectName, 'DC_BP_Location', 'DC_BP_Risk', 'DC_CA_Risk', 'DC_CF_Risk', 'DC_GL_Risk', 'DC_IM_Risk', 'DC_WC_Risk', 'DCBPLocation', 'WB_GOC_Risk', 'WB_HIO_Risk', 'WB_EC_Risk') = 1, v_PolicyAKID || '~' || RiskLocationKey,
		IN(i_ParentCoverageObjectName, 'DC_CA_State', 'DC_WC_StateTerm', 'WB_GOC_State', 'WB_HIO_State', 'WB_IM_State', 'WB_EC_State', 'DC_WC_State') = 1, v_PolicyAKID || '~' || 'PrimaryLocation' || '~' || i_StateProvince || '~' || v_LineType,
		IN(i_ParentCoverageObjectName, 'DC_CR_Endorsement', 'DC_CR_Risk', 'DC_CR_RiskCrime', 'DC_Line', 'DC_CU_UmbrellaEmployersLiability', 'DC_IM_CoverageForm', 'WB_CU_PremiumDetail', 'DC_CA_BusinessInterruptionOption') = 1, v_PolicyAKID || '~' || 'PrimaryLocation' || '~' || v_LineType) AS v_RiskLocation_Key,
	v_RiskLocation_Key AS o_RiskLocationKey,
	v_RiskLocationAKID AS o_RatingCoverage_RiskLocationAKID
	FROM EXP_Pol_AK_ID
	 -- Manually join with SQ_PolicyCoverage_SRC
	LEFT JOIN LKP_Policy
	ON LKP_Policy.pol_key = EXP_Pol_AK_ID.o_PolicyKey
	LEFT JOIN LKP_RATINGCOVERAGE_RISK LKP_RATINGCOVERAGE_RISK_v_PolicyAKID_CoverageGUID
	ON LKP_RATINGCOVERAGE_RISK_v_PolicyAKID_CoverageGUID.PolicyAKID = v_PolicyAKID
	AND LKP_RATINGCOVERAGE_RISK_v_PolicyAKID_CoverageGUID.CoverageGUID = CoverageGUID

),
Mplt_RiskLocationAKID_Population AS (WITH
	LKP_RiskLocation_RiskLocationKey AS (
		SELECT
		RiskLocationAKID,
		RiskLocationKey
		FROM (
			SELECT 
				RiskLocationAKID,
				RiskLocationKey
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation
			WHERE CurrentSnapshotFlag = 1 and SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and 
			exists
			 ( select 1 from @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol inner join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT 
			 on WCT.PolicyNumber=pol.pol_num and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol.pol_mod and pol.crrnt_snpsht_flag=1 and PolicyAKID=pol.Pol_AK_ID ) Order By EffectiveDate DESC,CreatedDate DESC,RiskLocationAKID desc--
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationKey ORDER BY RiskLocationAKID) = 1
	),
	LKP_RiskLocation_RiskLocationKey_LocNum_Territory AS (
		SELECT
		RiskLocationAKID,
		RiskLocationKey,
		LocationUnitNumber,
		RiskTerritory
		FROM (
			SELECT 
				RiskLocationAKID,
				RiskLocationKey,
				LocationUnitNumber,
				RiskTerritory
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation
			WHERE CurrentSnapshotFlag = 1 and SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and 
			exists
			 ( select 1 from @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol inner join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT 
			 on WCT.PolicyNumber=pol.pol_num and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol.pol_mod and pol.crrnt_snpsht_flag=1 and PolicyAKID=pol.Pol_AK_ID ) Order By EffectiveDate DESC,CreatedDate DESC,RiskLocationAKID desc--
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationKey,LocationUnitNumber,RiskTerritory ORDER BY RiskLocationAKID) = 1
	),
	LKP_RiskLocation_RiskLocationKey_LocNum AS (
		SELECT
		RiskLocationAKID,
		RiskLocationKey,
		LocationUnitNumber
		FROM (
			SELECT 
				RiskLocationAKID,
				RiskLocationKey,
				LocationUnitNumber
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation
			WHERE CurrentSnapshotFlag = 1 and SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and 
			exists
			 ( select 1 from @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol inner join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT 
			 on WCT.PolicyNumber=pol.pol_num and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol.pol_mod and pol.crrnt_snpsht_flag=1 and PolicyAKID=pol.Pol_AK_ID ) Order By EffectiveDate DESC,CreatedDate DESC,RiskLocationAKID desc--
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationKey,LocationUnitNumber ORDER BY RiskLocationAKID) = 1
	),
	Source_Input AS (
		
	),
	Exp_RiskLocationAKID_population AS (
		SELECT
		RatingCoverage_RiskLocationAKID AS i_RatingCoverage_RiskLocationAKID,
		RiskLocationKey AS i_RiskLocationKey,
		LocationNumber AS i_LocationNumber,
		Territory AS i_Territory,
		-- *INF*: IIF(ISNULL(i_RatingCoverage_RiskLocationAKID),:LKP.LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY(i_RiskLocationKey,i_LocationNumber,i_Territory),i_RatingCoverage_RiskLocationAKID)
		IFF(i_RatingCoverage_RiskLocationAKID IS NULL, LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY_i_RiskLocationKey_i_LocationNumber_i_Territory.RiskLocationAKID, i_RatingCoverage_RiskLocationAKID) AS v_RiskLocationAKID_RiskKey_Location_Territory,
		-- *INF*: IIF(ISNULL(v_RiskLocationAKID_RiskKey_Location_Territory),:LKP.LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM(i_RiskLocationKey,i_LocationNumber),v_RiskLocationAKID_RiskKey_Location_Territory)
		IFF(v_RiskLocationAKID_RiskKey_Location_Territory IS NULL, LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_i_RiskLocationKey_i_LocationNumber.RiskLocationAKID, v_RiskLocationAKID_RiskKey_Location_Territory) AS v_RiskLocationAKID_RiskKey_Location,
		-- *INF*: IIF(ISNULL(v_RiskLocationAKID_RiskKey_Location),:LKP.LKP_RISKLOCATION_RISKLOCATIONKEY(i_RiskLocationKey),v_RiskLocationAKID_RiskKey_Location)
		IFF(v_RiskLocationAKID_RiskKey_Location IS NULL, LKP_RISKLOCATION_RISKLOCATIONKEY_i_RiskLocationKey.RiskLocationAKID, v_RiskLocationAKID_RiskKey_Location) AS v_RiskLocationAKID_RiskKey,
		-- *INF*: iif(isnull(v_RiskLocationAKID_RiskKey),-1,v_RiskLocationAKID_RiskKey)
		IFF(v_RiskLocationAKID_RiskKey IS NULL, - 1, v_RiskLocationAKID_RiskKey) AS o_RiskLocationAKID
		FROM Source_Input
		LEFT JOIN LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY_i_RiskLocationKey_i_LocationNumber_i_Territory
		ON LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY_i_RiskLocationKey_i_LocationNumber_i_Territory.RiskLocationKey = i_RiskLocationKey
		AND LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY_i_RiskLocationKey_i_LocationNumber_i_Territory.LocationUnitNumber = i_LocationNumber
		AND LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY_i_RiskLocationKey_i_LocationNumber_i_Territory.RiskTerritory = i_Territory
	
		LEFT JOIN LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_i_RiskLocationKey_i_LocationNumber
		ON LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_i_RiskLocationKey_i_LocationNumber.RiskLocationKey = i_RiskLocationKey
		AND LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_i_RiskLocationKey_i_LocationNumber.LocationUnitNumber = i_LocationNumber
	
		LEFT JOIN LKP_RISKLOCATION_RISKLOCATIONKEY LKP_RISKLOCATION_RISKLOCATIONKEY_i_RiskLocationKey
		ON LKP_RISKLOCATION_RISKLOCATIONKEY_i_RiskLocationKey.RiskLocationKey = i_RiskLocationKey
	
	),
	RiskLocationAKID AS (
		SELECT
		o_RiskLocationAKID
		FROM Exp_RiskLocationAKID_population
	),
),
AGG_RemoveDuplicates AS (
	SELECT
	SQ_PolicyCoverage_SRC.PolicyGUId AS i_Id,
	SQ_PolicyCoverage_SRC.PolicyEffectiveDate AS i_EffectiveDate,
	SQ_PolicyCoverage_SRC.PolicyExpirationDate AS i_ExpirationDate,
	SQ_PolicyCoverage_SRC.PolicyVersion AS i_PolicyVersion,
	SQ_PolicyCoverage_SRC.LineType AS i_Type,
	SQ_PolicyCoverage_SRC.CustomerNum AS i_CustomerNumber,
	SQ_PolicyCoverage_SRC.RiskGrade AS i_RiskGrade,
	SQ_PolicyCoverage_SRC.IsAuditable AS i_IsAuditable,
	SQ_PolicyCoverage_SRC.PriorCarrierName,
	SQ_PolicyCoverage_SRC.PolicyNumber AS i_PolicyNumber,
	SQ_PolicyCoverage_SRC.LineOfBusiness AS i_LineOfBusiness,
	SQ_PolicyCoverage_SRC.Territory AS i_Territory,
	SQ_PolicyCoverage_SRC.LocationNumber AS i_LocationNumber,
	SQ_PolicyCoverage_SRC.CommissionCustomerCareAmount AS i_CommissionCustomerCareAmount,
	SQ_PolicyCoverage_SRC.LocationXmlId AS i_LocationXmlId,
	SQ_PolicyCoverage_SRC.StateProvince AS StateProv,
	SQ_PolicyCoverage_SRC.SessionId,
	SQ_PolicyCoverage_SRC.PartyId,
	SQ_PolicyCoverage_SRC.PriorPolicyNumber AS PriorInsurancePolicyNumber,
	SQ_PolicyCoverage_SRC.RatingPlan AS i_RatingPlan,
	LKP_Policy.pol_ak_id,
	Mplt_RiskLocationAKID_Population.o_RiskLocationAKID AS RiskLocationAKID,
	-- *INF*: IIF(ISNULL(i_CustomerNumber) or IS_SPACES(i_CustomerNumber) or LENGTH(i_CustomerNumber)=0,'N/A',LTRIM(RTRIM(i_CustomerNumber)))
	IFF(i_CustomerNumber IS NULL OR IS_SPACES(i_CustomerNumber) OR LENGTH(i_CustomerNumber) = 0, 'N/A', LTRIM(RTRIM(i_CustomerNumber))) AS o_CustomerNumber,
	-- *INF*: IIF(ISNULL(i_Id) or IS_SPACES(i_Id) or LENGTH(i_Id)=0,'N/A',LTRIM(RTRIM(i_Id)))
	IFF(i_Id IS NULL OR IS_SPACES(i_Id) OR LENGTH(i_Id) = 0, 'N/A', LTRIM(RTRIM(i_Id))) AS o_Id,
	-- *INF*: IIF(ISNULL(i_PolicyVersion),'00',LPAD(TO_CHAR(i_PolicyVersion),2,'0'))
	IFF(i_PolicyVersion IS NULL, '00', LPAD(TO_CHAR(i_PolicyVersion), 2, '0')) AS o_PolicyVersion,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_PolicyNumber)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_PolicyNumber) AS o_PolicyNumber,
	-- *INF*: IIF(ISNULL(i_LocationNumber) or IS_SPACES(i_LocationNumber) or LENGTH(i_LocationNumber)=0,'0000',LPAD(LTRIM(RTRIM(i_LocationNumber)), 4, '0'))
	IFF(i_LocationNumber IS NULL OR IS_SPACES(i_LocationNumber) OR LENGTH(i_LocationNumber) = 0, '0000', LPAD(LTRIM(RTRIM(i_LocationNumber)), 4, '0')) AS o_LocationNumber,
	-- *INF*: IIF(ISNULL(i_LocationXmlId) or IS_SPACES(i_LocationXmlId) or LENGTH(i_LocationXmlId)=0,'N/A',LTRIM(RTRIM(i_LocationXmlId)))
	IFF(i_LocationXmlId IS NULL OR IS_SPACES(i_LocationXmlId) OR LENGTH(i_LocationXmlId) = 0, 'N/A', LTRIM(RTRIM(i_LocationXmlId))) AS o_LocationXmlId,
	-- *INF*: IIF(ISNULL(i_Territory) or IS_SPACES(i_Territory) or LENGTH(i_Territory)=0,'N/A',LTRIM(RTRIM(i_Territory)))
	IFF(i_Territory IS NULL OR IS_SPACES(i_Territory) OR LENGTH(i_Territory) = 0, 'N/A', LTRIM(RTRIM(i_Territory))) AS o_Territory,
	-- *INF*: IIF(ISNULL(i_Type) or IS_SPACES(i_Type) or LENGTH(i_Type)=0,'N/A',LTRIM(RTRIM(i_Type)))
	IFF(i_Type IS NULL OR IS_SPACES(i_Type) OR LENGTH(i_Type) = 0, 'N/A', LTRIM(RTRIM(i_Type))) AS o_Type,
	-- *INF*: IIF(ISNULL(i_EffectiveDate),TO_DATE('21001231235959' , 'YYYYMMDDHH24MISS'),i_EffectiveDate)
	IFF(i_EffectiveDate IS NULL, TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'), i_EffectiveDate) AS o_EffectiveDate,
	i_ExpirationDate AS o_ExpirationDate,
	-- *INF*: IIF(i_IsAuditable='T','1','0')
	IFF(i_IsAuditable = 'T', '1', '0') AS o_AuditableIndicator,
	-- *INF*: IIF(ISNULL(i_RiskGrade) OR IS_SPACES(i_RiskGrade) OR LENGTH(i_RiskGrade)=0, 'N/A', LTRIM(RTRIM(i_RiskGrade)))
	-- 
	-- --IIF(ISNULL(i_RiskGrade) OR IS_SPACES(i_RiskGrade) OR LENGTH(i_RiskGrade)=0 OR NOT IS_NUMBER(LTRIM(RTRIM(i_RiskGrade))), 'N/A', LTRIM(RTRIM(i_RiskGrade)))
	IFF(i_RiskGrade IS NULL OR IS_SPACES(i_RiskGrade) OR LENGTH(i_RiskGrade) = 0, 'N/A', LTRIM(RTRIM(i_RiskGrade))) AS o_RiskGrade,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(PriorCarrierName)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(PriorCarrierName) AS o_CarrierName,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_LineOfBusiness)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_LineOfBusiness) AS o_LineOfBusiness,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(PriorInsurancePolicyNumber)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(PriorInsurancePolicyNumber) AS o_PriorInsurancePolicyKey,
	-- *INF*: IIF(ISNULL(i_CommissionCustomerCareAmount) or i_CommissionCustomerCareAmount =0.000,0,ABS(i_CommissionCustomerCareAmount/100))
	IFF(i_CommissionCustomerCareAmount IS NULL OR i_CommissionCustomerCareAmount = 0.000, 0, ABS(i_CommissionCustomerCareAmount / 100)) AS o_CommissionCustomerCareAmount,
	-- *INF*: LTRIM(RTRIM(i_RatingPlan))
	LTRIM(RTRIM(i_RatingPlan)) AS o_RatingPlan
	FROM Mplt_RiskLocationAKID_Population
	 -- Manually join with SQ_PolicyCoverage_SRC
	LEFT JOIN LKP_Policy
	ON LKP_Policy.pol_key = EXP_Pol_AK_ID.o_PolicyKey
	GROUP BY pol_ak_id, RiskLocationAKID, o_PolicyVersion, o_PolicyNumber, o_Type, o_EffectiveDate
),
EXP_Values AS (
	SELECT
	StateProv AS i_StateProv,
	o_CustomerNumber AS i_CustomerNumber,
	o_Id AS i_Id,
	o_PolicyVersion AS i_PolicyVersion,
	o_LocationNumber AS i_LocationNumber,
	o_LocationXmlId AS i_LocationXmlId,
	o_Territory AS i_Territory,
	o_Type AS i_Type,
	o_EffectiveDate AS i_EffectiveDate,
	o_ExpirationDate AS i_ExpirationDate,
	o_AuditableIndicator AS i_AuditableIndicator,
	o_RiskGrade AS i_RiskGrade,
	o_LineOfBusiness AS i_LineOfBusiness,
	o_CarrierName AS CarrierName,
	o_PolicyNumber AS PolicyNumber,
	o_PriorInsurancePolicyKey AS PriorInsurancePolicyKey,
	SessionId,
	PartyId,
	o_CommissionCustomerCareAmount AS CommissionCustomerCareAmount,
	o_RatingPlan AS i_RatingPlan,
	pol_ak_id AS i_pol_ak_id,
	RiskLocationAKID AS i_RiskLocationAKID,
	-- *INF*: rtrim(ltrim(PolicyNumber))||rtrim(ltrim(i_PolicyVersion))
	-- 
	-- 
	-- 
	-- --i_CustomerNumber||i_PolicyNumber||i_PolicyVersion
	rtrim(ltrim(PolicyNumber)) || rtrim(ltrim(i_PolicyVersion)) AS v_PolicyKey,
	i_Id||i_PolicyVersion AS v_PolicyIDKey,
	-- *INF*: :LKP.LKP_RATINGPLANFORSMALLDEDUCTIBLE(PolicyNumber, i_PolicyVersion)
	LKP_RATINGPLANFORSMALLDEDUCTIBLE_PolicyNumber_i_PolicyVersion.PolicyNumber AS v_RatingPlanForSmallDeductible,
	-- *INF*: :LKP.LKP_RATINGPLANFORSMALLDEDUCTIBLE2(PolicyNumber, i_PolicyVersion)
	LKP_RATINGPLANFORSMALLDEDUCTIBLE2_PolicyNumber_i_PolicyVersion.RatingPlanAKId AS v_RatingPlanForSmallDeductible2,
	-- *INF*: LTRIM(RTRIM(i_StateProv))
	LTRIM(RTRIM(i_StateProv)) AS o_StateProv,
	i_pol_ak_id AS o_Pol_AK_ID,
	-- *INF*: i_RiskLocationAKID
	-- --i_pol_ak_id||i_LocationNumber||i_Territory||i_LocationXmlId
	-- 
	-- --i_Id||i_PolicyVersion||i_LocationNumber||i_Territory||i_LocationXmlId
	i_RiskLocationAKID AS o_RiskLocationAKID,
	i_Type AS o_Type,
	i_EffectiveDate AS o_EffectiveDate,
	-- *INF*: IIF(ISNULL(i_ExpirationDate),TO_DATE('21001231235959' , 'YYYYMMDDHH24MISS'),i_ExpirationDate)
	IFF(i_ExpirationDate IS NULL, TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'), i_ExpirationDate) AS o_ExpirationDate,
	i_AuditableIndicator AS o_AuditableIndicator,
	i_RiskGrade AS o_RiskGradeCode,
	i_LineOfBusiness AS o_PriorInsuranceLine,
	-- *INF*: DECODE(TRUE,
	--  NOT ISNULL(v_RatingPlanForSmallDeductible) OR  NOT ISNULL(v_RatingPlanForSmallDeductible2),
	-- 'Small Deductible',
	-- UPPER(i_RatingPlan)='RETROSPECTIVE', 
	-- 'Large Risk Alternative Rating Option(LRARO)', 
	-- 'Guaranteed Cost'
	-- )
	DECODE(TRUE,
		NOT v_RatingPlanForSmallDeductible IS NULL OR NOT v_RatingPlanForSmallDeductible2 IS NULL, 'Small Deductible',
		UPPER(i_RatingPlan) = 'RETROSPECTIVE', 'Large Risk Alternative Rating Option(LRARO)',
		'Guaranteed Cost') AS o_RatingPlanDescription
	FROM AGG_RemoveDuplicates
	LEFT JOIN LKP_RATINGPLANFORSMALLDEDUCTIBLE LKP_RATINGPLANFORSMALLDEDUCTIBLE_PolicyNumber_i_PolicyVersion
	ON LKP_RATINGPLANFORSMALLDEDUCTIBLE_PolicyNumber_i_PolicyVersion.PolicyNumber = PolicyNumber
	AND LKP_RATINGPLANFORSMALLDEDUCTIBLE_PolicyNumber_i_PolicyVersion.PolicyVersion = i_PolicyVersion

	LEFT JOIN LKP_RATINGPLANFORSMALLDEDUCTIBLE2 LKP_RATINGPLANFORSMALLDEDUCTIBLE2_PolicyNumber_i_PolicyVersion
	ON LKP_RATINGPLANFORSMALLDEDUCTIBLE2_PolicyNumber_i_PolicyVersion.PolicyNumber = PolicyNumber
	AND LKP_RATINGPLANFORSMALLDEDUCTIBLE2_PolicyNumber_i_PolicyVersion.PolicyVersion = i_PolicyVersion

),
LKP_PolicyLimit AS (
	SELECT
	PolicyLimitAKId,
	PolicyAKId,
	InsuranceLine
	FROM (
		SELECT 
			PolicyLimitAKId,
			PolicyAKId,
			InsuranceLine
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyLimit
		WHERE CurrentSnapshotFlag=1 AND SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKId,InsuranceLine ORDER BY PolicyLimitAKId) = 1
),
LKP_PriorCoverage AS (
	SELECT
	PriorCoverageId,
	PriorCarrierName,
	PriorPolicyKey,
	PriorInsuranceLine
	FROM (
		SELECT 
			PriorCoverageId,
			PriorCarrierName,
			PriorPolicyKey,
			PriorInsuranceLine
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PriorCoverage
		WHERE SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PriorCarrierName,PriorPolicyKey,PriorInsuranceLine ORDER BY PriorCoverageId) = 1
),
LKP_RatingPlan AS (
	SELECT
	RatingPlanAKId,
	RatingPlanDescription
	FROM (
		SELECT 
			RatingPlanAKId,
			RatingPlanDescription
		FROM RatingPlan
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingPlanDescription ORDER BY RatingPlanAKId) = 1
),
LKP_WBWCLineStage AS (
	SELECT
	InterstateRiskID,
	SessionId,
	PartyId,
	State
	FROM (
		select distinct
		ptya.SessionId as SessionId,
		ptya.PartyId as PartyId,
		LTRIM(RTRIM(st.state)) as State,
		ls.InterstateRiskId as InterstateRiskId
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBWCLineStage ls
		,@{pipeline().parameters.SOURCE_TABLE_OWNER}.DCWCLineStaging wlin
		,@{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLineStaging lin
		,@{pipeline().parameters.SOURCE_TABLE_OWNER}.DCPartyAssociationStaging ptya
		,@{pipeline().parameters.SOURCE_TABLE_OWNER}.DCWCStateStaging st
		where ls.SessionId = wlin.SessionId
		and ls.WCLineId = wlin.WC_LineId
		and wlin.SessionId = lin.SessionId
		and wlin.LineId = lin.LineId
		and ptya.SessionId = lin.SessionId
		and st.SessionId = lin.SessionId
		and st.LineId = lin.LineId
		and ptya.PartyAssociationType = 'Account'
		and lin.Type = 'WorkersCompensation'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SessionId,PartyId,State ORDER BY InterstateRiskID) = 1
),
LKP_sup_insurance_line AS (
	SELECT
	sup_ins_line_id,
	ins_line_descript
	FROM (
		SELECT 
			sup_ins_line_id,
			ins_line_descript
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_insurance_line
		WHERE crrnt_snpsht_flag='1' and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ins_line_descript ORDER BY sup_ins_line_id) = 1
),
LKP_sup_type_bureau_code AS (
	SELECT
	sup_type_bureau_code_id,
	type_bureau_code
	FROM (
		SELECT 
			sup_type_bureau_code_id,
			type_bureau_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_type_bureau_code
		WHERE crrnt_snpsht_flag='1' and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY type_bureau_code ORDER BY sup_type_bureau_code_id) = 1
),
EXP_MD5 AS (
	SELECT
	EXP_Values.o_RiskLocationAKID AS i_RiskLocationAKID,
	EXP_Values.o_Pol_AK_ID AS i_pol_ak_id,
	LKP_sup_insurance_line.sup_ins_line_id AS i_sup_ins_line_id,
	LKP_sup_type_bureau_code.sup_type_bureau_code_id AS i_sup_type_bureau_code_id,
	EXP_Values.o_Type AS i_Type,
	EXP_Values.o_EffectiveDate AS i_EffectiveDate,
	EXP_Values.o_ExpirationDate AS i_ExpirationDate,
	LKP_WBWCLineStage.InterstateRiskID AS i_InterstateRiskID,
	EXP_Values.o_AuditableIndicator AS AuditableIndicator,
	EXP_Values.o_RiskGradeCode AS RiskGradeCode,
	-- *INF*: IIF(ISNULL(i_pol_ak_id),ERROR('Pol_ak_id can not be blank!'),i_pol_ak_id)
	IFF(i_pol_ak_id IS NULL, ERROR('Pol_ak_id can not be blank!'), i_pol_ak_id) AS v_pol_ak_id,
	-- *INF*: IIF(ISNULL(i_RiskLocationAKID),ERROR('RiskLocationAKID can not be blank!'),i_RiskLocationAKID)
	IFF(i_RiskLocationAKID IS NULL, ERROR('RiskLocationAKID can not be blank!'), i_RiskLocationAKID) AS v_RiskLocationAKID,
	-- *INF*: TO_CHAR(i_pol_ak_id)||'~'||TO_CHAR(i_RiskLocationAKID)
	-- 
	-- --- Change ID and version with Pol_ak_id for UID Project
	TO_CHAR(i_pol_ak_id) || '~' || TO_CHAR(i_RiskLocationAKID) AS v_PolicyCoverageKey,
	-- *INF*: MD5(TO_CHAR(v_pol_ak_id)||TO_CHAR(v_RiskLocationAKID)||i_Type||TO_CHAR(i_EffectiveDate))
	MD5(TO_CHAR(v_pol_ak_id) || TO_CHAR(v_RiskLocationAKID) || i_Type || TO_CHAR(i_EffectiveDate)) AS o_PolicyCoverageHashKey,
	v_pol_ak_id AS o_PolicyAKID,
	v_RiskLocationAKID AS o_RiskLocationAKID,
	-- *INF*: IIF(ISNULL(v_PolicyCoverageKey),'N/A',v_PolicyCoverageKey)
	IFF(v_PolicyCoverageKey IS NULL, 'N/A', v_PolicyCoverageKey) AS o_PolicyCoverageKey,
	i_Type AS o_InsuranceLine,
	i_Type AS o_TypeBureauCode,
	i_EffectiveDate AS o_PolicyCoverageEffectiveDate,
	i_ExpirationDate AS o_PolicyCoverageExpirationDate,
	-- *INF*: IIF(ISNULL(i_sup_ins_line_id),-1,i_sup_ins_line_id)
	IFF(i_sup_ins_line_id IS NULL, - 1, i_sup_ins_line_id) AS o_sup_ins_line_id,
	-- *INF*: IIF(ISNULL(i_sup_type_bureau_code_id),-1,i_sup_type_bureau_code_id)
	IFF(i_sup_type_bureau_code_id IS NULL, - 1, i_sup_type_bureau_code_id) AS o_sup_type_bureau_code_id,
	-- *INF*: IIF(ISNULL(i_InterstateRiskID),'N/A',TO_CHAR(i_InterstateRiskID))
	IFF(i_InterstateRiskID IS NULL, 'N/A', TO_CHAR(i_InterstateRiskID)) AS o_InterstateRiskId,
	LKP_PolicyLimit.PolicyLimitAKId,
	LKP_PriorCoverage.PriorCoverageId,
	EXP_Values.CommissionCustomerCareAmount,
	LKP_RatingPlan.RatingPlanAKId
	FROM EXP_Values
	LEFT JOIN LKP_PolicyLimit
	ON LKP_PolicyLimit.PolicyAKId = EXP_Values.o_Pol_AK_ID AND LKP_PolicyLimit.InsuranceLine = EXP_Values.o_Type
	LEFT JOIN LKP_PriorCoverage
	ON LKP_PriorCoverage.PriorCarrierName = EXP_Values.CarrierName AND LKP_PriorCoverage.PriorPolicyKey = EXP_Values.PriorInsurancePolicyKey AND LKP_PriorCoverage.PriorInsuranceLine = EXP_Values.o_PriorInsuranceLine
	LEFT JOIN LKP_RatingPlan
	ON LKP_RatingPlan.RatingPlanDescription = EXP_Values.o_RatingPlanDescription
	LEFT JOIN LKP_WBWCLineStage
	ON LKP_WBWCLineStage.SessionId = EXP_Values.SessionId AND LKP_WBWCLineStage.PartyId = EXP_Values.PartyId AND LKP_WBWCLineStage.State = EXP_Values.o_StateProv
	LEFT JOIN LKP_sup_insurance_line
	ON LKP_sup_insurance_line.ins_line_descript = EXP_Values.o_Type
	LEFT JOIN LKP_sup_type_bureau_code
	ON LKP_sup_type_bureau_code.type_bureau_code = EXP_Values.o_Type
),
LKP_PolicyCoverage AS (
	SELECT
	PolicyCoverageAKID,
	PolicyCoverageExpirationDate,
	AuditableIndicator,
	RiskGradeCode,
	InterstateRiskId,
	PolicyLimitAKId,
	PriorCoverageId,
	CustomerCareCommissionRate,
	RatingPlanAkId,
	PolicyCoverageHashKey
	FROM (
		SELECT 
			PolicyCoverageAKID,
			PolicyCoverageExpirationDate,
			AuditableIndicator,
			RiskGradeCode,
			InterstateRiskId,
			PolicyLimitAKId,
			PriorCoverageId,
			CustomerCareCommissionRate,
			RatingPlanAkId,
			PolicyCoverageHashKey
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage
		WHERE CurrentSnapshotFlag='1' and SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and
		PolicyAKId in (
		select pol_ak_id from @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol
		where exists (
		select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT
		where WCT.PolicyNumber=pol.pol_num
		and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol.pol_mod)
		and pol.crrnt_snpsht_flag=1)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyCoverageHashKey ORDER BY PolicyCoverageAKID) = 1
),
EXP_DetectChange AS (
	SELECT
	LKP_PolicyCoverage.PolicyCoverageAKID AS i_PolicyCoverageAKID,
	LKP_PolicyCoverage.PolicyCoverageExpirationDate AS i_PolicyCoverageExpirationDate,
	LKP_PolicyCoverage.AuditableIndicator AS i_AuditableIndicator,
	LKP_PolicyCoverage.RiskGradeCode AS i_RiskGradeCode,
	LKP_PolicyCoverage.InterstateRiskId AS i_InterstateRiskId,
	LKP_PolicyCoverage.PolicyLimitAKId AS i_PolicyLimitAKId,
	LKP_PolicyCoverage.PriorCoverageId AS i_PriorCoverageId,
	LKP_PolicyCoverage.CustomerCareCommissionRate AS i_CustomerCareCommissionRate,
	LKP_PolicyCoverage.RatingPlanAkId AS i_RatingPlanAkId,
	-- *INF*: DECODE(i_AuditableIndicator,'T', '1', 'F','0', NULL)
	DECODE(i_AuditableIndicator,
		'T', '1',
		'F', '0',
		NULL) AS v_LKP_AuditableIndicator,
	EXP_MD5.AuditableIndicator,
	EXP_MD5.RiskGradeCode,
	EXP_MD5.o_PolicyCoverageHashKey AS PolicyCoverageHashKey,
	EXP_MD5.o_PolicyAKID AS PolicyAKID,
	EXP_MD5.o_RiskLocationAKID AS RiskLocationAKID,
	EXP_MD5.o_PolicyCoverageKey AS PolicyCoverageKey,
	EXP_MD5.o_InsuranceLine AS InsuranceLine,
	EXP_MD5.o_TypeBureauCode AS TypeBureauCode,
	EXP_MD5.o_PolicyCoverageEffectiveDate AS PolicyCoverageEffectiveDate,
	EXP_MD5.o_PolicyCoverageExpirationDate AS PolicyCoverageExpirationDate,
	EXP_MD5.o_sup_ins_line_id AS sup_ins_line_id,
	EXP_MD5.o_sup_type_bureau_code_id AS sup_type_bureau_code_id,
	EXP_MD5.CommissionCustomerCareAmount,
	EXP_MD5.o_InterstateRiskId AS InterstateRiskId,
	EXP_MD5.PolicyLimitAKId,
	EXP_MD5.PriorCoverageId,
	EXP_MD5.RatingPlanAKId,
	-- *INF*: IIF(ISNULL(i_PolicyCoverageAKID), 'NEW', IIF(i_PolicyCoverageExpirationDate<>PolicyCoverageExpirationDate OR v_LKP_AuditableIndicator<>AuditableIndicator OR i_RiskGradeCode<>RiskGradeCode OR i_InterstateRiskId<>InterstateRiskId OR i_PolicyLimitAKId<>PolicyLimitAKId OR i_PriorCoverageId<>PriorCoverageId OR ISNULL(i_CustomerCareCommissionRate) OR i_CustomerCareCommissionRate<>CommissionCustomerCareAmount OR i_RatingPlanAkId <> RatingPlanAKId,
	-- 'UPDATE', 'NOCHANGE'))
	IFF(i_PolicyCoverageAKID IS NULL, 'NEW', IFF(i_PolicyCoverageExpirationDate <> PolicyCoverageExpirationDate OR v_LKP_AuditableIndicator <> AuditableIndicator OR i_RiskGradeCode <> RiskGradeCode OR i_InterstateRiskId <> InterstateRiskId OR i_PolicyLimitAKId <> PolicyLimitAKId OR i_PriorCoverageId <> PriorCoverageId OR i_CustomerCareCommissionRate IS NULL OR i_CustomerCareCommissionRate <> CommissionCustomerCareAmount OR i_RatingPlanAkId <> RatingPlanAKId, 'UPDATE', 'NOCHANGE')) AS o_ChangeFlag
	FROM EXP_MD5
	LEFT JOIN LKP_PolicyCoverage
	ON LKP_PolicyCoverage.PolicyCoverageHashKey = EXP_MD5.o_PolicyCoverageHashKey
),
FIL_InsertNewRows AS (
	SELECT
	i_PolicyCoverageAKID, 
	AuditableIndicator, 
	RiskGradeCode, 
	PolicyCoverageHashKey, 
	PolicyAKID, 
	RiskLocationAKID, 
	PolicyCoverageKey, 
	InsuranceLine, 
	TypeBureauCode, 
	PolicyCoverageEffectiveDate, 
	PolicyCoverageExpirationDate, 
	sup_ins_line_id, 
	sup_type_bureau_code_id, 
	o_ChangeFlag AS ChangeFlag, 
	InterstateRiskId, 
	PolicyLimitAKId, 
	PriorCoverageId, 
	CommissionCustomerCareAmount, 
	RatingPlanAKId
	FROM EXP_DetectChange
	WHERE ChangeFlag='NEW'  OR ChangeFlag='UPDATE'
),
SEQ_PolicyCoverageAKID AS (
	CREATE SEQUENCE SEQ_PolicyCoverageAKID
	START = 0
	INCREMENT = 1;
),
EXP_AKandMetaData AS (
	SELECT
	i_PolicyCoverageAKID,
	AuditableIndicator AS i_AuditableIndicator,
	RiskGradeCode AS i_RiskGradeCode,
	PolicyCoverageHashKey AS i_PolicyCoverageHashKey,
	PolicyAKID AS i_PolicyAKID,
	RiskLocationAKID AS i_RiskLocationAKID,
	PolicyCoverageKey AS i_PolicyCoverageKey,
	InsuranceLine AS i_InsuranceLine,
	TypeBureauCode AS i_TypeBureauCode,
	PolicyCoverageEffectiveDate AS i_PolicyCoverageEffectiveDate,
	PolicyCoverageExpirationDate AS i_PolicyCoverageExpirationDate,
	sup_ins_line_id AS i_sup_ins_line_id,
	sup_type_bureau_code_id AS i_sup_type_bureau_code_id,
	ChangeFlag AS i_ChangeFlag,
	SEQ_PolicyCoverageAKID.NEXTVAL,
	1 AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: IIF(i_ChangeFlag='NEW', TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE)
	IFF(i_ChangeFlag = 'NEW', TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59' , 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreateDate,
	SYSDATE AS o_ModifiedDate,
	0 AS o_LogicalIndicator,
	i_PolicyCoverageHashKey AS o_PolicyCoverageHashKey,
	-- *INF*: IIF(ISNULL(i_PolicyCoverageAKID),NEXTVAL, i_PolicyCoverageAKID)
	IFF(i_PolicyCoverageAKID IS NULL, NEXTVAL, i_PolicyCoverageAKID) AS o_PolicyCoverageAKID,
	i_PolicyAKID AS o_PolicyAKID,
	i_RiskLocationAKID AS o_RiskLocationAKID,
	i_PolicyCoverageKey AS o_PolicyCoverageKey,
	i_InsuranceLine AS o_InsuranceLine,
	i_TypeBureauCode AS o_TypeBureauCode,
	i_PolicyCoverageEffectiveDate AS o_PolicyCoverageEffectiveDate,
	i_PolicyCoverageExpirationDate AS o_PolicyCoverageExpirationDate,
	i_sup_ins_line_id AS o_sup_ins_line_id,
	i_sup_type_bureau_code_id AS o_sup_type_bureau_code_id,
	i_AuditableIndicator AS o_AuditableIndicator,
	i_RiskGradeCode AS o_RiskGradeCode,
	InterstateRiskId,
	PolicyLimitAKId,
	PriorCoverageId,
	CommissionCustomerCareAmount,
	RatingPlanAKId
	FROM FIL_InsertNewRows
),
TGT_PolicyCoverage_Insert AS (

	------------ PRE SQL ----------
	exec [spSetIndexStatus] @Enable = 0, @Schema = 'dbo', @TableName = 'PolicyCoverage', @IndexWildcard = 'Ak1PolicyCoverage'
	-------------------------------


	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, LogicalIndicator, PolicyCoverageHashKey, PolicyCoverageAKID, PolicyAKID, RiskLocationAKID, PolicyCoverageKey, InsuranceLine, TypeBureauCode, PolicyCoverageEffectiveDate, PolicyCoverageExpirationDate, SupInsuranceLineId, SupTypeBureauCodeId, RatingPlanAKId, AuditableIndicator, RiskGradeCode, InterstateRiskId, PolicyLimitAKId, PriorCoverageId, CustomerCareCommissionRate)
	SELECT 
	o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	o_AuditID AS AUDITID, 
	o_EffectiveDate AS EFFECTIVEDATE, 
	o_ExpirationDate AS EXPIRATIONDATE, 
	o_SourceSystemID AS SOURCESYSTEMID, 
	o_CreateDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_LogicalIndicator AS LOGICALINDICATOR, 
	o_PolicyCoverageHashKey AS POLICYCOVERAGEHASHKEY, 
	o_PolicyCoverageAKID AS POLICYCOVERAGEAKID, 
	o_PolicyAKID AS POLICYAKID, 
	o_RiskLocationAKID AS RISKLOCATIONAKID, 
	o_PolicyCoverageKey AS POLICYCOVERAGEKEY, 
	o_InsuranceLine AS INSURANCELINE, 
	o_TypeBureauCode AS TYPEBUREAUCODE, 
	o_PolicyCoverageEffectiveDate AS POLICYCOVERAGEEFFECTIVEDATE, 
	o_PolicyCoverageExpirationDate AS POLICYCOVERAGEEXPIRATIONDATE, 
	o_sup_ins_line_id AS SUPINSURANCELINEID, 
	o_sup_type_bureau_code_id AS SUPTYPEBUREAUCODEID, 
	RATINGPLANAKID, 
	o_AuditableIndicator AS AUDITABLEINDICATOR, 
	o_RiskGradeCode AS RISKGRADECODE, 
	INTERSTATERISKID, 
	POLICYLIMITAKID, 
	PRIORCOVERAGEID, 
	CommissionCustomerCareAmount AS CUSTOMERCARECOMMISSIONRATE
	FROM EXP_AKandMetaData
),
SQ_PolicyCoverage AS (
	SELECT 
		PolicyCoverageID,
		EffectiveDate,
		ExpirationDate,
		PolicyCoverageAKID 
	FROM
		@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage a
	WHERE  exists 
		   (SELECT 1 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage b
	           WHERE CurrentSnapshotFlag = 1 AND SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND a.PolicyCoverageAKID=b.PolicyCoverageAKID GROUP BY PolicyCoverageAKID  HAVING count(*) > 1)
	AND SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}' and CurrentSnapshotFlag=1
	ORDER BY PolicyCoverageAKID , EffectiveDate  DESC
	
	--IN Subquery exists to pick AK ID column values that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag 
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
),
EXP_Lag_eff_from_date AS (
	SELECT
	EffectiveDate AS i_eff_from_date,
	PolicyCoverageAKID AS i_PolicyCoverageAKID,
	ExpirationDate AS orig_eff_to_date,
	PolicyCoverageID,
	-- *INF*: DECODE(TRUE,
	-- i_PolicyCoverageAKID = v_prev_cust_ak_id  ,
	-- ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),orig_eff_to_date)
	DECODE(TRUE,
		i_PolicyCoverageAKID = v_prev_cust_ak_id, ADD_TO_DATE(v_prev_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	i_PolicyCoverageAKID AS v_prev_cust_ak_id,
	i_eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	v_eff_to_date AS eff_to_date,
	SYSDATE AS modified_date
	FROM SQ_PolicyCoverage
),
FIL_FirstRowInAKGroup AS (
	SELECT
	orig_eff_to_date AS i_orig_eff_to_date, 
	PolicyCoverageID, 
	crrnt_snpsht_flag, 
	eff_to_date, 
	modified_date
	FROM EXP_Lag_eff_from_date
	WHERE i_orig_eff_to_date != eff_to_date
),
UPD_PolicyCoverage AS (
	SELECT
	PolicyCoverageID, 
	crrnt_snpsht_flag AS CurrentSnapshotFlag, 
	eff_to_date AS ExpirationDate, 
	modified_date AS ModifiedDate
	FROM FIL_FirstRowInAKGroup
),
TGT_PolicyCoverage_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage AS T
	USING UPD_PolicyCoverage AS S
	ON T.PolicyCoverageID = S.PolicyCoverageID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate

	------------ POST SQL ----------
	exec [spSetIndexStatus] @Enable = 1, @Schema = 'dbo', @TableName = 'PolicyCoverage', @IndexWildcard = 'Ak1PolicyCoverage'
	-------------------------------


),
WITH
SQ_WC_DirectPremium AS (
	DECLARE @SELECTION_START_TS AS DATETIME,
	@MIN_START_CLNDR_ID AS INT
	
	SELECT @MIN_START_CLNDR_ID = MIN(clndr_id) FROM calendar_dim WHERE clndr_date >= '@{pipeline().parameters.SELECTION_START_TS}'
	
	
	SELECT a.AnnualStatementLineDimID, 
	a.AnnualStatementLineProductCodeDimID,
	a.AgencyDimID,
	a.PolicyDimID,
	a.ContractCustomerDimID,
	a.RiskLocationDimID,
	a.PremiumTransactionTypeDimID, 
	a.PremiumTransactionEnteredDateID, 
	a.PremiumTransactionEffectiveDateID, 
	a.PremiumTransactionExpirationDateID, 
	a.PremiumTransactionBookedDateID, 
	a.PremiumTransactionRunDateID, 
	a.MonthlyChangeinDirectEarnedPremium,
	a.InsuranceReferenceDimId,
	a.EDWPremiumMasterCalculationPKId, 
	a.CoverageDetailDimId, 
	a.InsuranceReferenceCoverageDimId, 
	a.CoverageCancellationDateId,
	'0' as GeneratedRecordIndicator,
	'Direct' as ModifiedPremiumType,
	a.EDWEarnedPremiumMonthlyCalculationPKID
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.EarnedPremiumTransactionMonthlyFact a
	INNER JOIN  @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransactionTypeDim pttd
	on a.PremiumTransactionTypeDimID=pttd.PremiumTransactionTypeDimID and pttd.PremiumTypeCode='D'
	@{pipeline().parameters.WHERE_EPTMF_SINCE_LAST_LOAD} 
	INNER JOIN  @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceDim i 
	on a.InsuranceReferenceDimId =i.InsuranceReferenceDimId  and InsuranceReferenceLineOfBusinessAbbreviation ='WC'
	INNER JOIN  @{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim pd
	on a.PolicyDimID = pd.pol_dim_id 
	AND pd.pol_sym = '000'
	@{pipeline().parameters.JOIN_POLICY_LIST_IL}
),
EXP_DirectPremium AS (
	SELECT
	EDWPremiumMasterCalculationPKId,
	PremiumTransactionEnteredDateID,
	PremiumTransactionEffectiveDateID,
	PremiumTransactionRunDateID,
	AnnualStatementLineDimID,
	AnnualStatementLineProductCodeDimID,
	AgencyDimID,
	PolicyDimID,
	ContractCustomerDimID,
	RiskLocationDimID,
	PremiumTransactionTypeDimID,
	InsuranceReferenceDimId,
	InsuranceReferenceCoverageDimId,
	CoverageDetailDimId,
	PremiumTransactionBookedDateID,
	PremiumTransactionExpirationDateID,
	CoverageCancellationDateId,
	GeneratedRecordIndicator,
	MonthlyChangeinDirectEarnedPremium,
	ModifiedPremiumType,
	EDWEarnedPremiumMonthlyCalculationPKID
	FROM SQ_WC_DirectPremium
),
SQ_WC_ModifiedPremium AS (
	--SQ_WC_ModifiedPremium 
	SELECT WEMP.SourceSystemID, 
	WEMP.PolicyAKID, 
	WEMP.StatisticalCoverageAKID, 
	WEMP.PremiumTransactionAKID, 
	WNMP.PremiumMasterCalculationId, 
	WEMP.StatisticalCoverageCancellationDate, 
	WEMP.PremiumTransactionEnteredDate, 
	WEMP.PremiumTransactionEffectiveDate, 
	WEMP.PremiumTransactionExpirationDate, 
	WEMP.PremiumTransactionBookedDate, 
	WEMP.PremiumTransactionCode, 
	WEMP.PremiumType, 
	WEMP.ReasonAmendedCode, 
	WEMP.EarnedPremium, 
	WEMP.ChangeInEarnedPremium, 
	WEMP.UnearnedPremium, 
	WEMP.ChangeInUnearnedPremium, 
	isnull(PRODUCT.ProductCode, '000') as ProductCode, 
	WEMP.AnnualStatementLineCode, 
	WEMP.SubAnnualStatementLineCode, 
	WEMP.NonSubAnnualStatementLineCode, 
	WEMP.AnnualStatementLineProductCode, 
	Isnull(InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessCode, '000') as LineOfBusinessCode, 
	Isnull(PolicyOffering.PolicyOfferingCode, '000') as PolicyOfferingCode, 
	WEMP.RunDate, 
	isnull(SPC.StrategicProfitCenterCode, '6') as StrategicProfitCenterCode, 
	isnull(EGP.EnterpriseGroupCode, '1') as EnterpriseGroupCode, 
	isnull(IRLE.InsuranceReferenceLegalEntityCode, '1') as InsuranceReferenceLegalEntityCode, 
	isnull(ISS.InsuranceSegmentCode, 'N/A') as InsuranceSegmentCode, 
	'N/A' as RiskUnitGroup, 
	'N/A' as RiskUnit, 
	'N/A' as RiskUnitSequenceNumber, 
	'N/A' as MajorPerilCode, 
	'N/A' as ClassCode, 
	RC.RiskType , 
	RC.CoverageType, 
	SIL.StandardInsuranceLineCode, 
	RL.RiskLocationHashKey, 
	PC.TypeBureauCode,
	RC.PerilGroup,
	RC.CoverageForm,
	RC.SubCoverageTypeCode,
	RC.CoverageVersion,
	PC.CustomerCareCommissionRate,
	RPDT.RatingPlanCode,
	WNMP.GeneratedRecordFlag,
	WNMP.ModifiedPremiumType
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkEarnedModifiedPremium WEMP
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkNormalizedModifiedPremium WNMP
	ON WEMP.PremiumMasterCalculationPKID=WNMP.WorkNormalizedModifiedPremiumId
	AND WEMP.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation PMC on PMC.PremiumMasterCalculationID=WNMP.PremiumMasterCalculationId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC ON PMC.RatingCoverageAKID=RC.RatingCoverageAKID and PMC.RatingCoverageEffectiveDate = RC.EffectiveDate
	@{pipeline().parameters.WHERE_WEMP_SINCE_LAST_LOAD}
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	             ON WEMP.PolicyCoverageAKID=PC.PolicyCoverageAKID  and PC.CurrentSnapshotFlag=1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	             ON WEMP.RiskLocationAKID=RL.RiskLocationAKID and RL.CurrentSnapshotFlag=1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Policy P
			ON WEMP.PolicyAKID=P.pol_ak_id
			AND P.crrnt_snpsht_flag=1
			@{pipeline().parameters.JOIN_POLICY_LIST_IL}		
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product 
	on RC.ProductAKID=Product.ProductAKID and Product.CurrentSnapshotFlag=1
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLineOfBusiness 
	On RC.InsuranceReferenceLineOfBusinessAKID=InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessAKId 
	and InsuranceReferenceLineOfBusiness.CurrentSnapshotFlag=1
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering
	On PolicyOffering.PolicyOfferingAKID=P.PolicyOfferingAKID and PolicyOffering.CurrentSnapshotFlag=1			
			
	LEFT OUTER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISS
			ON P.InsuranceSegmentAKId=ISS.InsuranceSegmentAKId
			AND ISS.CurrentSnapshotFlag=1
	LEFT OUTER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter SPC
	            ON P.StrategicProfitCenterAKId=SPC.StrategicProfitCenterAKId
	            AND SPC.CurrentSnapshotFlag=1
	LEFT OUTER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.EnterpriseGroup EGP
	           ON SPC.EnterpriseGroupId=EGP.EnterpriseGroupId
	           AND EGP.CurrentSnapshotFlag=1
	LEFT OUTER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLegalEntity IRLE
	          ON SPC.InsuranceReferenceLegalEntityId=IRLE.InsuranceReferenceLegalEntityId
	          AND IRLE.CurrentSnapshotFlag=1
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL
	          ON SIL.ins_line_code=PC.InsuranceLine
	          AND SIL.crrnt_snpsht_flag=1 
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingPlan RPDT
	           ON PC.RatingPlanAKId=RPDT.RatingPlanAKId and RPDT.CurrentSnapshotFlag=1
	WHERE WEMP.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'  
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_EarnedPremiumCalculation_IN AS (
	SELECT
	SourceSystemID,
	PolicyAKID,
	StatisticalCoverageAKID,
	PremiumTransactionAKID AS PremiumTransactionAKId,
	PremiumMasterCalculationPKID,
	StatisticalCoverageCancellationDate,
	PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	PremiumTransactionExpirationDate,
	PremiumTransactionBookedDate,
	PremiumTransactionCode,
	PremiumType,
	ReasonAmendedCode,
	EarnedPremium,
	ChangeInEarnedPremium,
	UnearnedPremium,
	ChangeInUnearnedPremium,
	ProductCode,
	AnnualStatementLineCode,
	SubAnnualStatementLineCode,
	NonSubAnnualStatementLineCode AS i_NonSubAnnualStatementLineCode,
	AnnualStatementLineProductCode,
	LineOfBusinessCode,
	PolicyOfferingCode,
	RunDate,
	StrategicProfitCenterCode,
	EnterpriseGroupCode,
	InsuranceReferenceLegalEntityCode,
	InsuranceSegmentCode,
	RiskUnitGroup AS i_RiskUnitGroup,
	RiskUnit AS i_RiskUnit,
	RiskUnitSequenceNumber AS i_RiskUnitSequenceNumber,
	MajorPerilCode AS i_MajorPerilCode,
	ClassCode AS i_ClassCode,
	CoverageForm AS i_CoverageForm,
	RiskType AS i_RiskType,
	CoverageType AS i_CoverageType,
	StandardInsuranceLineCode AS i_StandardInsuranceLineCode,
	RiskLocationHashKey,
	TypeBureauCode AS i_TypeBureauCode,
	PerilGroup AS i_PerilGroup,
	SubCoverageTypeCode AS i_SubCoverageTypeCode,
	CoverageVersion AS i_CoverageVersion,
	CustomerCareCommissionRate,
	RatingPlanCode AS i_RatingPlanCode,
	GeneratedRecordIndicator AS i_GeneratedRecordIndicator,
	ModifiedPremiumType
	FROM SQ_WC_ModifiedPremium
),
SQ_OtherLines_NaturalTransaction AS (
	DECLARE @SELECTION_START_TS AS DATETIME,
	@MIN_START_CLNDR_ID AS INT
	
	SELECT @MIN_START_CLNDR_ID = MIN(clndr_id) FROM calendar_dim WHERE clndr_date >= '@{pipeline().parameters.SELECTION_START_TS}'
	
	--SQ_OtherLines_NaturalTransaction
	
	SELECT ISNULL(rmNonWCCalc.DirectWrittenPremium,1.0 ),
	ISNULL(rmNonWCCalc.OtherModifiedPremium,1.0 ),
	ISNULL(rmNonWCCalc.ScheduleModifiedPremium,1.0 ),
	ISNULL(rmNonWCCalc.ExperienceModifiedPremium,1.0 ),
	ISNULL(rmNonWCCalc.SubjectWrittenPremium,1.0 ),
	a.AnnualStatementLineDimID, 
	a.AnnualStatementLineProductCodeDimID,
	a.AgencyDimID,
	a.PolicyDimID,
	a.ContractCustomerDimID,
	a.RiskLocationDimID,
	a.PremiumTransactionTypeDimID, 
	a.PremiumTransactionEnteredDateID, 
	a.PremiumTransactionEffectiveDateID, 
	a.PremiumTransactionExpirationDateID, 
	a.PremiumTransactionBookedDateID, 
	a.PremiumTransactionRunDateID, 
	a.MonthlyChangeinDirectEarnedPremium,
	a.InsuranceReferenceDimId,
	a.EDWPremiumMasterCalculationPKId, 
	a.CoverageDetailDimId, 
	a.InsuranceReferenceCoverageDimId, 
	a.CoverageCancellationDateId,
	irc.InsuranceLineCode,
	a.EDWEarnedPremiumMonthlyCalculationPKID
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.EarnedPremiumTransactionMonthlyFact a
	@{pipeline().parameters.JOIN_PREMIUMTRANSACTION_LIST} 
	INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransactionTypeDim ptdim
	on a.PremiumTransactionTypeDimID=ptdim.PremiumTransactionTypeDimID and ptdim.PremiumTypeCode='D'
	@{pipeline().parameters.WHERE_EPTMF_SINCE_LAST_LOAD}
	INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceDim i 
	on a.InsuranceReferenceDimId =i.InsuranceReferenceDimId  and InsuranceReferenceLineOfBusinessAbbreviation <>'WC'
	INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceCoverageDim irc  
	on irc.InsuranceReferenceCoverageDimId = a.InsuranceReferenceCoverageDimId
	INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim pd
	on a.PolicyDimID = pd.pol_dim_id  AND pd.pol_sym = '000'
	@{pipeline().parameters.JOIN_POLICY_LIST_IL}
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.ModifiedPremiumNonWorkersCompensationCalculation rmNonWCCalc
	on a.EDWPremiumMasterCalculationPKId = rmNonWCCalc.PremiumMasterCalculationId
	and rmNonWCCalc.GeneratedRecordIndicator=0 
	and rmNonWCCalc.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
),
EXP_OtherLinesEarnedPremium AS (
	SELECT
	DirectWrittenPremium,
	OtherModifiedPremium,
	ScheduleModifiedPremium,
	ExperienceModifiedPremium,
	SubjectWrittenPremium,
	AnnualStatementLineDimID,
	AnnualStatementLineProductCodeDimID,
	AgencyDimID,
	PolicyDimID,
	ContractCustomerDimID,
	RiskLocationDimID,
	PremiumTransactionTypeDimID,
	PremiumTransactionEnteredDateID,
	PremiumTransactionEffectiveDateID,
	PremiumTransactionExpirationDateID,
	PremiumTransactionBookedDateID,
	PremiumTransactionRunDateID,
	MonthlyChangeinDirectEarnedPremium,
	InsuranceReferenceDimId,
	EDWPremiumMasterCalculationPKId,
	CoverageDetailDimId,
	InsuranceReferenceCoverageDimId,
	CoverageCancellationDateId,
	InsuranceLineCode,
	EDWEarnedPremiumMonthlyCalculationPKID
	FROM SQ_OtherLines_NaturalTransaction
),
mplt_PolicyDimID_PremiumMaster AS (WITH
	Input AS (
		
	),
	EXP_Default AS (
		SELECT
		IN_PolicyAKID AS PolicyAKID,
		IN_Trans_Date
		FROM Input
	),
	LKP_V2_Policy AS (
		SELECT
		contract_cust_ak_id,
		agencyakid,
		pol_status_code,
		strtgc_bus_dvsn_ak_id,
		IN_Trans_Date,
		pol_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT policy.contract_cust_ak_id as contract_cust_ak_id, policy.agencyakid as agencyakid, policy.pol_status_code as pol_status_code, policy.strtgc_bus_dvsn_ak_id as strtgc_bus_dvsn_ak_id, policy.pol_ak_id as pol_ak_id, policy.eff_from_date as eff_from_date, policy.eff_to_date as eff_to_date FROM 
			V2.policy
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id,eff_from_date,eff_to_date ORDER BY contract_cust_ak_id DESC) = 1
	),
	LKP_PolicyDimID AS (
		SELECT
		pol_dim_id,
		pol_key,
		pol_eff_date,
		pol_exp_date,
		pms_pol_lob_code,
		ClassOfBusinessCode,
		IN_Trans_Date,
		edw_pol_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				pol_dim_id,
				pol_key,
				pol_eff_date,
				pol_exp_date,
				pms_pol_lob_code,
				ClassOfBusinessCode,
				IN_Trans_Date,
				edw_pol_ak_id,
				eff_from_date,
				eff_to_date
			FROM policy_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_pol_ak_id,eff_from_date,eff_to_date ORDER BY pol_dim_id DESC) = 1
	),
	LKP_V3_AgencyDimID AS (
		SELECT
		agency_dim_id,
		edw_agency_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT AgencyDim.AgencyDimID as agency_dim_id, AgencyDim.EDWAgencyAKID as edw_agency_ak_id, AgencyDim.EffectiveDate as eff_from_date, AgencyDim.ExpirationDate as eff_to_date
			 FROM V3.AgencyDim as AgencyDim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_agency_ak_id,eff_from_date,eff_to_date ORDER BY agency_dim_id DESC) = 1
	),
	LKP_ContractCustomerDim AS (
		SELECT
		contract_cust_dim_id,
		IN_Trans_Date,
		edw_contract_cust_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				contract_cust_dim_id,
				IN_Trans_Date,
				edw_contract_cust_ak_id,
				eff_from_date,
				eff_to_date
			FROM contract_customer_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_contract_cust_ak_id,eff_from_date,eff_to_date ORDER BY contract_cust_dim_id DESC) = 1
	),
	lkp_StrategicBusinessDivisionDIM AS (
		SELECT
		strtgc_bus_dvsn_dim_id,
		edw_strtgc_bus_dvsn_ak_id
		FROM (
			SELECT strategic_business_division_dim.strtgc_bus_dvsn_dim_id as strtgc_bus_dvsn_dim_id, strategic_business_division_dim.edw_strtgc_bus_dvsn_ak_id as edw_strtgc_bus_dvsn_ak_id 
			FROM strategic_business_division_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_strtgc_bus_dvsn_ak_id ORDER BY strtgc_bus_dvsn_dim_id DESC) = 1
	),
	EXP_Values AS (
		SELECT
		LKP_V3_AgencyDimID.agency_dim_id,
		LKP_ContractCustomerDim.contract_cust_dim_id,
		LKP_PolicyDimID.pol_dim_id,
		LKP_V2_Policy.pol_status_code,
		LKP_PolicyDimID.pol_eff_date,
		LKP_PolicyDimID.pol_exp_date,
		lkp_StrategicBusinessDivisionDIM.strtgc_bus_dvsn_dim_id,
		LKP_PolicyDimID.pol_key,
		LKP_PolicyDimID.pms_pol_lob_code,
		LKP_PolicyDimID.ClassOfBusinessCode
		FROM 
		LEFT JOIN LKP_ContractCustomerDim
		ON LKP_ContractCustomerDim.edw_contract_cust_ak_id = LKP_V2_Policy.contract_cust_ak_id AND LKP_ContractCustomerDim.eff_from_date <= EXP_Default.IN_Trans_Date AND LKP_ContractCustomerDim.eff_to_date >= EXP_Default.IN_Trans_Date
		LEFT JOIN LKP_PolicyDimID
		ON LKP_PolicyDimID.edw_pol_ak_id = EXP_Default.PolicyAKID AND LKP_PolicyDimID.eff_from_date <= EXP_Default.IN_Trans_Date AND LKP_PolicyDimID.eff_to_date >= EXP_Default.IN_Trans_Date
		LEFT JOIN LKP_V2_Policy
		ON LKP_V2_Policy.pol_ak_id = EXP_Default.PolicyAKID AND LKP_V2_Policy.eff_from_date <= EXP_Default.IN_Trans_Date AND LKP_V2_Policy.eff_to_date >= EXP_Default.IN_Trans_Date
		LEFT JOIN LKP_V3_AgencyDimID
		ON LKP_V3_AgencyDimID.edw_agency_ak_id = LKP_V2_Policy.agencyakid AND LKP_V3_AgencyDimID.eff_from_date <= EXP_Default.IN_Trans_Date AND LKP_V3_AgencyDimID.eff_to_date >= EXP_Default.IN_Trans_Date
		LEFT JOIN lkp_StrategicBusinessDivisionDIM
		ON lkp_StrategicBusinessDivisionDIM.edw_strtgc_bus_dvsn_ak_id = LKP_V2_Policy.strtgc_bus_dvsn_ak_id
	),
	Output AS (
		SELECT
		agency_dim_id, 
		contract_cust_dim_id, 
		pol_dim_id, 
		pol_status_code, 
		pol_eff_date, 
		pol_exp_date, 
		strtgc_bus_dvsn_dim_id, 
		pol_key, 
		pms_pol_lob_code, 
		ClassOfBusinessCode
		FROM EXP_Values
	),
),
mplt_EarnedPremium AS (WITH
	LKP_CoverageDetailDim AS (
		SELECT
		CoverageDetailDimId,
		PremiumTransactionAKId
		FROM (
			select CDD.CoverageDetailDimId as CoverageDetailDimId, 
			PT.PremiumTransactionAKId as PremiumTransactionAKId
			from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
			join @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
			on  PT.PremiumTransactionID=CDD.EDWPremiumTransactionPKID
			where exists (
			select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkEarnedModifiedPremium EPMC
			where EPMC.PremiumTransactionAKId=PT.PremiumTransactionAKId
			and EPMC.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}')
			or '@{pipeline().parameters.SELECTION_START_TS}'<='01/01/1800 00:00:00'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKId ORDER BY CoverageDetailDimId) = 1
	),
	LKP_InsuranceReferenceCoverageDim_DCT AS (
		SELECT
		InsuranceReferenceCoverageDimId,
		DctRiskTypeCode,
		DctCoverageTypeCode,
		InsuranceLineCode,
		DctPerilGroup,
		DctSubCoverageTypeCode,
		DctCoverageVersion
		FROM (
			SELECT 
				InsuranceReferenceCoverageDimId,
				DctRiskTypeCode,
				DctCoverageTypeCode,
				InsuranceLineCode,
				DctPerilGroup,
				DctSubCoverageTypeCode,
				DctCoverageVersion
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceCoverageDim
			WHERE NOT (DctRiskTypeCode='N/A' AND DctCoverageTypeCode='N/A' AND DctPerilGroup='N/A' AND DctSubCoverageTypeCode='N/A' AND DctCoverageVersion='N/A')
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY DctRiskTypeCode,DctCoverageTypeCode,InsuranceLineCode,DctPerilGroup,DctSubCoverageTypeCode,DctCoverageVersion ORDER BY InsuranceReferenceCoverageDimId) = 1
	),
	LKP_InsuranceReferenceCoverageDim_PMS AS (
		SELECT
		InsuranceReferenceCoverageDimId,
		InsuranceLineCode,
		PmsRiskUnitGroupCode,
		PmsRiskUnitCode,
		PmsMajorPerilCode,
		PmsProductTypeCode
		FROM (
			SELECT 
				InsuranceReferenceCoverageDimId,
				InsuranceLineCode,
				PmsRiskUnitGroupCode,
				PmsRiskUnitCode,
				PmsMajorPerilCode,
				PmsProductTypeCode
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceCoverageDim
			WHERE DctRiskTypeCode='N/A' AND DctCoverageTypeCode='N/A' AND DctPerilGroup='N/A' AND DctSubCoverageTypeCode='N/A' AND DctCoverageVersion='N/A'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceLineCode,PmsRiskUnitGroupCode,PmsRiskUnitCode,PmsMajorPerilCode,PmsProductTypeCode ORDER BY InsuranceReferenceCoverageDimId) = 1
	),
	LKP_CoverageDetailDim_Hist AS (
		SELECT
		CoverageDetailDimId,
		StatisticalCoverageAKID
		FROM (
			select CDD.CoverageDetailDimId as CoverageDetailDimId, 
			SC.StatisticalCoverageAKID as StatisticalCoverageAKID
			from  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkEarnedModifiedPremium EPMC
			join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
			on EPMC.StatisticalCoverageAKId=SC.StatisticalCoverageAKId
			join @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
			on  SC.CoverageGUID=CDD.CoverageGUID and EPMC.PremiumTransactionEffectiveDate between CDD.EffectiveDate and CDD.ExpirationDate
			where EPMC.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}' and EPMC.PremiumTransactionAKID=-1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY StatisticalCoverageAKID ORDER BY CoverageDetailDimId) = 1
	),
	LKP_calender_dim AS (
		SELECT
		clndr_id,
		clndr_date
		FROM (
			SELECT 
				clndr_id,
				clndr_date
			FROM calendar_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY clndr_date ORDER BY clndr_id) = 1
	),
	INPUT_OtherLinesEarnedPremium AS (
		
	),
	INPUT_DirectPremium AS (
		
	),
	INPUT_OtherLinesGeneratedmodifiedPremium AS (
		
	),
	EXP_EarnedPremiumCalculation_IN AS (
		SELECT
		SourceSystemID,
		StatisticalCoverageAKID,
		PremiumTransactionAKId,
		PremiumMasterCalculationPKID,
		StatisticalCoverageCancellationDate,
		PremiumTransactionEnteredDate,
		PremiumTransactionEffectiveDate,
		PremiumTransactionExpirationDate,
		PremiumTransactionBookedDate,
		PremiumTransactionCode,
		PremiumType,
		ReasonAmendedCode,
		EarnedPremium,
		ChangeInEarnedPremium,
		UnearnedPremium,
		ChangeInUnearnedPremium,
		ProductCode,
		AnnualStatementLineCode,
		SubAnnualStatementLineCode,
		i_NonSubAnnualStatementLineCode,
		-- *INF*: i_NonSubAnnualStatementLineCode
		-- 
		-- --IIF(SourceSystemID='DCT','N/A',i_NonSubAnnualStatementLineCode)
		i_NonSubAnnualStatementLineCode AS o_NonSubAnnualStatementLineCode,
		AnnualStatementLineProductCode,
		LineOfBusinessCode,
		PolicyOfferingCode,
		RunDate,
		StrategicProfitCenterCode,
		EnterpriseGroupCode,
		InsuranceReferenceLegalEntityCode,
		InsuranceSegmentCode,
		i_RiskUnitGroup,
		i_RiskUnit,
		i_RiskUnitSequenceNumber,
		i_MajorPerilCode,
		i_ClassCode,
		i_CoverageForm,
		i_RiskType,
		i_CoverageType,
		i_StandardInsuranceLineCode,
		RiskLocationHashKey,
		i_TypeBureauCode,
		i_PerilGroup,
		i_SubCoverageTypeCode,
		i_CoverageVersion,
		-- *INF*: i_RiskType
		-- 
		-- --IIF(LTRIM(RTRIM(i_CoverageForm))='BusinessAuto','N/A',i_RiskType)
		i_RiskType AS v_RiskType,
		-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(SUBSTR(i_RiskUnitSequenceNumber,2,1))
		:UDF.DEFAULT_VALUE_FOR_STRINGS(SUBSTR(i_RiskUnitSequenceNumber, 2, 1)) AS v_ProductTypeCode,
		-- *INF*: IIF(REG_MATCH(i_StandardInsuranceLineCode,'[^0-9a-zA-Z]'),'N/A',i_StandardInsuranceLineCode)
		IFF(REG_MATCH(i_StandardInsuranceLineCode, '[^0-9a-zA-Z]'), 'N/A', i_StandardInsuranceLineCode) AS v_Reg_StandardInsuranceLineCode,
		-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_MajorPerilCode)
		:UDF.DEFAULT_VALUE_FOR_STRINGS(i_MajorPerilCode) AS v_MajorPerilCode,
		-- *INF*: IIF(LTRIM(v_MajorPerilCode,'0')='','N/A',v_MajorPerilCode)
		IFF(LTRIM(v_MajorPerilCode, '0') = '', 'N/A', v_MajorPerilCode) AS v_Zero_MajorPerilCode,
		-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ClassCode)
		:UDF.DEFAULT_VALUE_FOR_STRINGS(i_ClassCode) AS v_ClassCode,
		-- *INF*: IIF(v_Reg_StandardInsuranceLineCode='N/A' AND (IN(i_TypeBureauCode,'AL','AN','AP') OR IN(v_Zero_MajorPerilCode,'930','931')),'CA',v_Reg_StandardInsuranceLineCode)
		IFF(v_Reg_StandardInsuranceLineCode = 'N/A' AND ( IN(i_TypeBureauCode, 'AL', 'AN', 'AP') OR IN(v_Zero_MajorPerilCode, '930', '931') ), 'CA', v_Reg_StandardInsuranceLineCode) AS v_StandardInsuranceLineCode,
		-- *INF*: IIF(v_StandardInsuranceLineCode='N/A' AND IN(i_TypeBureauCode,'CF','B2','BB','BE','BF','BM','BT','FT','GL','GS','IM','MS','PF','PH','PI','PL','PQ','WC','WP','NB','RL','RN','RP','BC','N/A'),1,0)
		IFF(v_StandardInsuranceLineCode = 'N/A' AND IN(i_TypeBureauCode, 'CF', 'B2', 'BB', 'BE', 'BF', 'BM', 'BT', 'FT', 'GL', 'GS', 'IM', 'MS', 'PF', 'PH', 'PI', 'PL', 'PQ', 'WC', 'WP', 'NB', 'RL', 'RN', 'RP', 'BC', 'N/A'), 1, 0) AS v_flag,
		-- *INF*: IIF(IN(v_StandardInsuranceLineCode,'CR') OR v_flag=1,'N/A',:UDF.DEFAULT_VALUE_FOR_STRINGS(i_RiskUnitGroup))
		IFF(IN(v_StandardInsuranceLineCode, 'CR') OR v_flag = 1, 'N/A', :UDF.DEFAULT_VALUE_FOR_STRINGS(i_RiskUnitGroup)) AS v_RiskUnitGroup,
		-- *INF*: IIF(LTRIM(v_RiskUnitGroup,'0')='','N/A',v_RiskUnitGroup)
		IFF(LTRIM(v_RiskUnitGroup, '0') = '', 'N/A', v_RiskUnitGroup) AS v_Zero_RiskUnitGroup,
		-- *INF*: IIF(
		--   v_flag=1
		-- OR   (v_StandardInsuranceLineCode='GL' AND (NOT IN(v_MajorPerilCode,'540','599','919')
		--   OR NOT IN(v_ClassCode,'11111','22222','22250','92100','17000','17001','17002','80051','80052','80053','80054','80055','80056','80057','80058')))
		--   OR IN(v_StandardInsuranceLineCode,'WC','IM','CG','CA')=1,
		--  'N/A',:UDF.DEFAULT_VALUE_FOR_STRINGS(i_RiskUnit))
		IFF(v_flag = 1 OR ( v_StandardInsuranceLineCode = 'GL' AND ( NOT IN(v_MajorPerilCode, '540', '599', '919') OR NOT IN(v_ClassCode, '11111', '22222', '22250', '92100', '17000', '17001', '17002', '80051', '80052', '80053', '80054', '80055', '80056', '80057', '80058') ) ) OR IN(v_StandardInsuranceLineCode, 'WC', 'IM', 'CG', 'CA') = 1, 'N/A', :UDF.DEFAULT_VALUE_FOR_STRINGS(i_RiskUnit)) AS v_RiskUnit,
		-- *INF*: IIF(LTRIM(v_RiskUnit,'0')='','N/A',v_RiskUnit)
		IFF(LTRIM(v_RiskUnit, '0') = '', 'N/A', v_RiskUnit) AS v_Zero_RiskUnit,
		-- *INF*: IIF(REG_MATCH(v_Zero_RiskUnitGroup,'[^0-9a-zA-Z]'),'N/A',v_Zero_RiskUnitGroup)
		IFF(REG_MATCH(v_Zero_RiskUnitGroup, '[^0-9a-zA-Z]'), 'N/A', v_Zero_RiskUnitGroup) AS v_PmsRiskUnitGroupCode,
		-- *INF*: IIF(REG_MATCH(v_Zero_RiskUnit,'[^0-9a-zA-Z]'),'N/A',v_Zero_RiskUnit)
		IFF(REG_MATCH(v_Zero_RiskUnit, '[^0-9a-zA-Z]'), 'N/A', v_Zero_RiskUnit) AS v_PmsRiskUnitCode,
		-- *INF*: SUBSTR(v_PmsRiskUnitCode, 1, 3)
		SUBSTR(v_PmsRiskUnitCode, 1, 3) AS v_PmsRiskUnitCode_1_3,
		-- *INF*: IIF(SourceSystemID='PMS',v_StandardInsuranceLineCode,i_StandardInsuranceLineCode)
		IFF(SourceSystemID = 'PMS', v_StandardInsuranceLineCode, i_StandardInsuranceLineCode) AS v_InsuranceLineCode,
		-- *INF*: IIF(REG_MATCH(v_Zero_MajorPerilCode,'[^0-9a-zA-Z]'),'N/A',v_Zero_MajorPerilCode)
		IFF(REG_MATCH(v_Zero_MajorPerilCode, '[^0-9a-zA-Z]'), 'N/A', v_Zero_MajorPerilCode) AS v_PmsMajorPerilCode,
		-- *INF*: IIF(
		--   REG_MATCH(v_ProductTypeCode,'[^0-9a-zA-Z]') OR v_Reg_StandardInsuranceLineCode<>'GL' OR v_ProductTypeCode='0' OR LENGTH(v_ProductTypeCode)=0,
		--   'N/A',v_ProductTypeCode
		-- )
		IFF(REG_MATCH(v_ProductTypeCode, '[^0-9a-zA-Z]') OR v_Reg_StandardInsuranceLineCode <> 'GL' OR v_ProductTypeCode = '0' OR LENGTH(v_ProductTypeCode) = 0, 'N/A', v_ProductTypeCode) AS v_PmsProductTypeCode,
		-- *INF*: :LKP.LKP_INSURANCEREFERENCECOVERAGEDIM_PMS(v_PmsRiskUnitGroupCode, v_PmsRiskUnitCode, v_PmsMajorPerilCode, v_InsuranceLineCode, v_PmsProductTypeCode)
		LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_v_PmsRiskUnitGroupCode_v_PmsRiskUnitCode_v_PmsMajorPerilCode_v_InsuranceLineCode_v_PmsProductTypeCode.InsuranceReferenceCoverageDimId AS v_InsuranceReferenceCoverageDimId_PMS_1,
		-- *INF*: v_InsuranceReferenceCoverageDimId_PMS_1
		-- 
		-- --IIF(ISNULL(v_InsuranceReferenceCoverageDimId_PMS_1), :LKP.LKP_INSURANCEREFERENCECOVERAGEDIM_PMS(v_PmsRiskUnitGroupCode, v_PmsRiskUnitCode_1_3, v_PmsMajorPerilCode, v_InsuranceLineCode, v_PmsProductTypeCode), v_InsuranceReferenceCoverageDimId_PMS_1)
		v_InsuranceReferenceCoverageDimId_PMS_1 AS v_InsuranceReferenceCoverageDimId_PMS_2,
		-- *INF*: :LKP.LKP_INSURANCEREFERENCECOVERAGEDIM_DCT(v_RiskType, i_CoverageType, v_InsuranceLineCode, i_PerilGroup,i_SubCoverageTypeCode,i_CoverageVersion)
		LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_v_RiskType_i_CoverageType_v_InsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.InsuranceReferenceCoverageDimId AS v_InsuranceReferenceCoverageDimId_DCT,
		-- *INF*: IIF(PremiumTransactionAKId=-1,
		-- --Historical records
		-- :LKP.LKP_COVERAGEDETAILDIM_HIST(StatisticalCoverageAKID),
		-- --Incremental records
		-- :LKP.LKP_CoverageDetailDim(PremiumTransactionAKId)
		-- )
		IFF(PremiumTransactionAKId = - 1, LKP_COVERAGEDETAILDIM_HIST_StatisticalCoverageAKID.CoverageDetailDimId, LKP_COVERAGEDETAILDIM_PremiumTransactionAKId.CoverageDetailDimId) AS v_CoverageDetailDimId,
		-- *INF*: IIF(ISNULL(v_InsuranceReferenceCoverageDimId_PMS_2), -1, v_InsuranceReferenceCoverageDimId_PMS_2)
		IFF(v_InsuranceReferenceCoverageDimId_PMS_2 IS NULL, - 1, v_InsuranceReferenceCoverageDimId_PMS_2) AS o_InsuranceReferenceCoverageDimId_PMS,
		-- *INF*: IIF(ISNULL(v_InsuranceReferenceCoverageDimId_DCT), -1, v_InsuranceReferenceCoverageDimId_DCT)
		IFF(v_InsuranceReferenceCoverageDimId_DCT IS NULL, - 1, v_InsuranceReferenceCoverageDimId_DCT) AS o_InsuranceReferenceCoverageDimId_DCT,
		-- *INF*: IIF(ISNULL(v_CoverageDetailDimId), -1, v_CoverageDetailDimId)
		IFF(v_CoverageDetailDimId IS NULL, - 1, v_CoverageDetailDimId) AS o_CoverageDetailDimId_lkp,
		CustomerCareCommissionRate,
		i_RatingPlanCode,
		-- *INF*: IIF(ISNULL(i_RatingPlanCode), '1', i_RatingPlanCode)
		IFF(i_RatingPlanCode IS NULL, '1', i_RatingPlanCode) AS o_RatingPlanCode,
		i_GeneratedRecordIndicator,
		-- *INF*: IIF(i_GeneratedRecordIndicator='T' or i_GeneratedRecordIndicator='1','1','0')
		IFF(i_GeneratedRecordIndicator = 'T' OR i_GeneratedRecordIndicator = '1', '1', '0') AS o_GeneratedRecordIndicator,
		ModifiedPremiumType,
		AgencyDimID,
		PolicyDimID,
		ContractCustomerDimID
		FROM INPUT_OtherLinesGeneratedmodifiedPremium
		LEFT JOIN LKP_INSURANCEREFERENCECOVERAGEDIM_PMS LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_v_PmsRiskUnitGroupCode_v_PmsRiskUnitCode_v_PmsMajorPerilCode_v_InsuranceLineCode_v_PmsProductTypeCode
		ON LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_v_PmsRiskUnitGroupCode_v_PmsRiskUnitCode_v_PmsMajorPerilCode_v_InsuranceLineCode_v_PmsProductTypeCode.InsuranceLineCode = v_PmsRiskUnitGroupCode
		AND LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_v_PmsRiskUnitGroupCode_v_PmsRiskUnitCode_v_PmsMajorPerilCode_v_InsuranceLineCode_v_PmsProductTypeCode.PmsRiskUnitGroupCode = v_PmsRiskUnitCode
		AND LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_v_PmsRiskUnitGroupCode_v_PmsRiskUnitCode_v_PmsMajorPerilCode_v_InsuranceLineCode_v_PmsProductTypeCode.PmsRiskUnitCode = v_PmsMajorPerilCode
		AND LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_v_PmsRiskUnitGroupCode_v_PmsRiskUnitCode_v_PmsMajorPerilCode_v_InsuranceLineCode_v_PmsProductTypeCode.PmsMajorPerilCode = v_InsuranceLineCode
		AND LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_v_PmsRiskUnitGroupCode_v_PmsRiskUnitCode_v_PmsMajorPerilCode_v_InsuranceLineCode_v_PmsProductTypeCode.PmsProductTypeCode = v_PmsProductTypeCode
	
		LEFT JOIN LKP_INSURANCEREFERENCECOVERAGEDIM_DCT LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_v_RiskType_i_CoverageType_v_InsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion
		ON LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_v_RiskType_i_CoverageType_v_InsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.DctRiskTypeCode = v_RiskType
		AND LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_v_RiskType_i_CoverageType_v_InsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.DctCoverageTypeCode = i_CoverageType
		AND LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_v_RiskType_i_CoverageType_v_InsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.InsuranceLineCode = v_InsuranceLineCode
		AND LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_v_RiskType_i_CoverageType_v_InsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.DctPerilGroup = i_PerilGroup
		AND LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_v_RiskType_i_CoverageType_v_InsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.DctSubCoverageTypeCode = i_SubCoverageTypeCode
		AND LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_v_RiskType_i_CoverageType_v_InsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.DctCoverageVersion = i_CoverageVersion
	
		LEFT JOIN LKP_COVERAGEDETAILDIM_HIST LKP_COVERAGEDETAILDIM_HIST_StatisticalCoverageAKID
		ON LKP_COVERAGEDETAILDIM_HIST_StatisticalCoverageAKID.StatisticalCoverageAKID = StatisticalCoverageAKID
	
		LEFT JOIN LKP_COVERAGEDETAILDIM LKP_COVERAGEDETAILDIM_PremiumTransactionAKId
		ON LKP_COVERAGEDETAILDIM_PremiumTransactionAKId.PremiumTransactionAKId = PremiumTransactionAKId
	
	),
	EXP_OtherLinesEarnedPremium AS (
		SELECT
		DirectWrittenPremium,
		OtherModifiedPremium,
		ScheduleModifiedPremium,
		ExperienceModifiedPremium,
		SubjectWrittenPremium,
		AnnualStatementLineDimID,
		AnnualStatementLineProductCodeDimID,
		AgencyDimID,
		PolicyDimID,
		ContractCustomerDimID,
		RiskLocationDimID,
		PremiumTransactionTypeDimID,
		PremiumTransactionEnteredDateID,
		PremiumTransactionEffectiveDateID,
		PremiumTransactionExpirationDateID,
		PremiumTransactionBookedDateID,
		PremiumTransactionRunDateID,
		MonthlyChangeinDirectEarnedPremium,
		InsuranceReferenceDimId,
		EDWPremiumMasterCalculationPKId,
		CoverageDetailDimId,
		InsuranceReferenceCoverageDimId,
		CoverageCancellationDateId,
		InsuranceLineCode,
		'0' AS GeneratedRecordFlag,
		MonthlyChangeinDirectEarnedPremium AS EarnedDirectWrittenPremium,
		-- *INF*: 0
		-- 
		-- --IIF(InsuranceLineCode='WC',MonthlyChangeinDirectEarnedPremium,0)
		-- 
		0 AS EarnedClassifiedPremium,
		-- *INF*: 0
		-- 
		-- --IIF(InsuranceLineCode='WC',MonthlyChangeinDirectEarnedPremium,0)
		-- 
		0 AS EarnedRatablePremium,
		-- *INF*: TO_DECIMAL(OtherModifiedPremium/DirectWrittenPremium*MonthlyChangeinDirectEarnedPremium,8)
		TO_DECIMAL(OtherModifiedPremium / DirectWrittenPremium * MonthlyChangeinDirectEarnedPremium, 8) AS v_EarnedOtherModifiedPremium,
		-- *INF*: TO_DECIMAL(ScheduleModifiedPremium/DirectWrittenPremium*MonthlyChangeinDirectEarnedPremium,8)
		TO_DECIMAL(ScheduleModifiedPremium / DirectWrittenPremium * MonthlyChangeinDirectEarnedPremium, 8) AS v_EarnedScheduleModifiedPremium,
		-- *INF*: TO_DECIMAL(ExperienceModifiedPremium/DirectWrittenPremium*MonthlyChangeinDirectEarnedPremium,8)
		TO_DECIMAL(ExperienceModifiedPremium / DirectWrittenPremium * MonthlyChangeinDirectEarnedPremium, 8) AS v_EarnedExperienceModifiedPremium,
		-- *INF*: TO_DECIMAL(SubjectWrittenPremium/DirectWrittenPremium*MonthlyChangeinDirectEarnedPremium,8)
		TO_DECIMAL(SubjectWrittenPremium / DirectWrittenPremium * MonthlyChangeinDirectEarnedPremium, 8) AS v_EarnedSubjectWrittenPremium,
		-- *INF*: ROUND(v_EarnedOtherModifiedPremium,4)
		ROUND(v_EarnedOtherModifiedPremium, 4) AS o_EarnedOtherModifiedPremium,
		-- *INF*: ROUND(v_EarnedScheduleModifiedPremium,4)
		ROUND(v_EarnedScheduleModifiedPremium, 4) AS o_EarnedScheduleModifiedPremium,
		-- *INF*: ROUND(v_EarnedExperienceModifiedPremium,4)
		ROUND(v_EarnedExperienceModifiedPremium, 4) AS o_EarnedExperienceModifiedPremium,
		-- *INF*: ROUND(v_EarnedSubjectWrittenPremium,4)
		ROUND(v_EarnedSubjectWrittenPremium, 4) AS o_EarnedSubjectWrittenPremium,
		EDWEarnedPremiumMonthlyCalculationPKID
		FROM INPUT_OtherLinesEarnedPremium
	),
	EXP_DirectPremium AS (
		SELECT
		EDWPremiumMasterCalculationPKId,
		PremiumTransactionEnteredDateID,
		PremiumTransactionEffectiveDateID,
		PremiumTransactionRunDateID,
		AnnualStatementLineDimID,
		AnnualStatementLineProductCodeDimID,
		AgencyDimID,
		PolicyDimID,
		ContractCustomerDimID,
		RiskLocationDimID,
		PremiumTransactionTypeDimID,
		InsuranceReferenceDimId,
		InsuranceReferenceCoverageDimId,
		CoverageDetailDimId,
		PremiumTransactionBookedDateID,
		PremiumTransactionExpirationDateID,
		CoverageCancellationDateId,
		GeneratedRecordIndicator,
		MonthlyChangeinDirectEarnedPremium,
		ModifiedPremiumType,
		EDWEarnedPremiumMonthlyCalculationPKID
		FROM INPUT_DirectPremium
	),
	lkp_PremiumTransactionTypeDim AS (
		SELECT
		PremiumTransactionTypeDimID,
		PremiumTransactionCode,
		ReasonAmendedCode,
		PremiumTypeCode
		FROM (
			SELECT 
			PTTD.PremiumTransactionTypeDimID as PremiumTransactionTypeDimID,
			LTRIM(RTRIM(PTTD.PremiumTransactionCode)) as PremiumTransactionCode, 
			LTRIM(RTRIM(PTTD.ReasonAmendedCode)) as ReasonAmendedCode, 
			LTRIM(RTRIM(PTTD.PremiumTypeCode)) as PremiumTypeCode 
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransactionTypeDim PTTD
			where PTTD.CurrentSnapShotFlag=1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionCode,ReasonAmendedCode,PremiumTypeCode ORDER BY PremiumTransactionTypeDimID DESC) = 1
	),
	LKP_asl_dim AS (
		SELECT
		asl_dim_id,
		asl_code,
		sub_asl_code,
		sub_non_asl_code
		FROM (
			SELECT 
				asl_dim_id,
				asl_code,
				sub_asl_code,
				sub_non_asl_code
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.asl_dim
			WHERE crrnt_snpsht_flag=1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY asl_code,sub_asl_code,sub_non_asl_code ORDER BY asl_dim_id DESC) = 1
	),
	LKP_asl_product_code AS (
		SELECT
		asl_prdct_code_dim_id,
		asl_prdct_code
		FROM (
			SELECT 
				asl_prdct_code_dim_id,
				asl_prdct_code
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.asl_product_code_dim
			WHERE crrnt_snpsht_flag=1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY asl_prdct_code ORDER BY asl_prdct_code_dim_id DESC) = 1
	),
	LKP_RiskLocationDim AS (
		SELECT
		RiskLocationDimID,
		RiskLocationHashKey
		FROM (
			SELECT 
				RiskLocationDimID,
				RiskLocationHashKey
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocationDim
			WHERE CurrentSnapshotFlag=1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationHashKey ORDER BY RiskLocationDimID) = 1
	),
	LKP_InsuranceReferenceDimId AS (
		SELECT
		InsuranceReferenceDimId,
		EnterpriseGroupCode,
		InsuranceReferenceLegalEntityCode,
		StrategicProfitCenterCode,
		InsuranceSegmentCode,
		PolicyOfferingCode,
		ProductCode,
		InsuranceReferenceLineOfBusinessCode,
		RatingPlanCode
		FROM (
			SELECT 
				InsuranceReferenceDimId,
				EnterpriseGroupCode,
				InsuranceReferenceLegalEntityCode,
				StrategicProfitCenterCode,
				InsuranceSegmentCode,
				PolicyOfferingCode,
				ProductCode,
				InsuranceReferenceLineOfBusinessCode,
				RatingPlanCode
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceDim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY EnterpriseGroupCode,InsuranceReferenceLegalEntityCode,StrategicProfitCenterCode,InsuranceSegmentCode,PolicyOfferingCode,ProductCode,InsuranceReferenceLineOfBusinessCode,RatingPlanCode ORDER BY InsuranceReferenceDimId) = 1
	),
	EXP_Consolidate_Data_from_Lookups AS (
		SELECT
		EXP_EarnedPremiumCalculation_IN.AgencyDimID,
		-- *INF*: IIF(NOT ISNULL(AgencyDimID),AgencyDimID,-1)
		IFF(NOT AgencyDimID IS NULL, AgencyDimID, - 1) AS AgencyDimID_out,
		EXP_EarnedPremiumCalculation_IN.PolicyDimID,
		-- *INF*: IIF(NOT ISNULL(PolicyDimID),PolicyDimID,-1)
		IFF(NOT PolicyDimID IS NULL, PolicyDimID, - 1) AS PolicyDimID_out,
		EXP_EarnedPremiumCalculation_IN.ContractCustomerDimID,
		-- *INF*: IIF(NOT ISNULL(ContractCustomerDimID),ContractCustomerDimID,-1)
		IFF(NOT ContractCustomerDimID IS NULL, ContractCustomerDimID, - 1) AS ContractCustomerDimID_out,
		LKP_RiskLocationDim.RiskLocationDimID,
		-- *INF*: IIF(NOT ISNULL(RiskLocationDimID),RiskLocationDimID,-1)
		IFF(NOT RiskLocationDimID IS NULL, RiskLocationDimID, - 1) AS RiskLocationDimID_out,
		lkp_PremiumTransactionTypeDim.PremiumTransactionTypeDimID,
		-- *INF*: IIF(NOT ISNULL(PremiumTransactionTypeDimID),PremiumTransactionTypeDimID,-1)
		IFF(NOT PremiumTransactionTypeDimID IS NULL, PremiumTransactionTypeDimID, - 1) AS PremiumTransactionTypeDimID_out,
		EXP_EarnedPremiumCalculation_IN.EarnedPremium,
		EXP_EarnedPremiumCalculation_IN.ChangeInEarnedPremium,
		EXP_EarnedPremiumCalculation_IN.UnearnedPremium,
		EXP_EarnedPremiumCalculation_IN.ChangeInUnearnedPremium,
		EXP_EarnedPremiumCalculation_IN.PremiumTransactionBookedDate,
		-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(PremiumTransactionBookedDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
		LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionBookedDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PremiumTransactionBookedDateID,
		-- *INF*: IIF(NOT ISNULL(v_PremiumTransactionBookedDateID),v_PremiumTransactionBookedDateID,-1)
		IFF(NOT v_PremiumTransactionBookedDateID IS NULL, v_PremiumTransactionBookedDateID, - 1) AS PremiumTransactionBookedDateID_out,
		EXP_EarnedPremiumCalculation_IN.RunDate AS PremiumTransactionRunDate,
		-- *INF*: IIF((PremiumTransactionBookedDate<=LAST_DAY(PremiumTransactionRunDate)) AND (PremiumTransactionBookedDate>=
		-- SET_DATE_PART(SET_DATE_PART(SET_DATE_PART(SET_DATE_PART( PremiumTransactionRunDate, 'DD', 1 ),'HH',0),'MI',0),'SS',0)),'Y','N')
		IFF(( PremiumTransactionBookedDate <= LAST_DAY(PremiumTransactionRunDate) ) AND ( PremiumTransactionBookedDate >= SET_DATE_PART(SET_DATE_PART(SET_DATE_PART(SET_DATE_PART(PremiumTransactionRunDate, 'DD', 1), 'HH', 0), 'MI', 0), 'SS', 0) ), 'Y', 'N') AS DateFlagForWrittenPremium,
		-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(PremiumTransactionRunDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
		LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionRunDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PremiumTransactionRunDateID,
		-- *INF*: IIF(NOT ISNULL(v_PremiumTransactionRunDateID),v_PremiumTransactionRunDateID,-1)
		IFF(NOT v_PremiumTransactionRunDateID IS NULL, v_PremiumTransactionRunDateID, - 1) AS PremiumTransactionRunDateID_out,
		EXP_EarnedPremiumCalculation_IN.PremiumTransactionEnteredDate,
		-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(PremiumTransactionEnteredDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
		LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionEnteredDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PremiumTransactionEnteredDateID,
		-- *INF*: IIF(NOT ISNULL(v_PremiumTransactionEnteredDateID),v_PremiumTransactionEnteredDateID,-1)
		IFF(NOT v_PremiumTransactionEnteredDateID IS NULL, v_PremiumTransactionEnteredDateID, - 1) AS PremiumTransactionEnteredDateID_out,
		EXP_EarnedPremiumCalculation_IN.PremiumTransactionEffectiveDate,
		-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(PremiumTransactionEffectiveDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
		LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PremiumTransactionEffectiveDateID,
		-- *INF*: IIF(NOT ISNULL(v_PremiumTransactionEffectiveDateID),v_PremiumTransactionEffectiveDateID,-1)
		IFF(NOT v_PremiumTransactionEffectiveDateID IS NULL, v_PremiumTransactionEffectiveDateID, - 1) AS PremiumTransactionEffectiveDateID_out,
		EXP_EarnedPremiumCalculation_IN.PremiumTransactionExpirationDate,
		-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(PremiumTransactionExpirationDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
		LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionExpirationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PremiumTransactionExpirationDateID,
		-- *INF*: IIF(NOT ISNULL(v_PremiumTransactionExpirationDateID),v_PremiumTransactionExpirationDateID,-1)
		IFF(NOT v_PremiumTransactionExpirationDateID IS NULL, v_PremiumTransactionExpirationDateID, - 1) AS PremiumTransactionExpirationDateID_out,
		EXP_EarnedPremiumCalculation_IN.PremiumType,
		LKP_asl_dim.asl_dim_id AS ASLdimID,
		-- *INF*: IIF(NOT ISNULL(ASLdimID),ASLdimID,-1)
		IFF(NOT ASLdimID IS NULL, ASLdimID, - 1) AS ASLdimID_out,
		LKP_asl_product_code.asl_prdct_code_dim_id AS ASLproductcodedimID,
		-- *INF*: IIF(NOT ISNULL(ASLproductcodedimID),ASLproductcodedimID,-1)
		IFF(NOT ASLproductcodedimID IS NULL, ASLproductcodedimID, - 1) AS ASLproductcodedimID_out,
		EXP_EarnedPremiumCalculation_IN.PremiumMasterCalculationPKID,
		LKP_InsuranceReferenceDimId.InsuranceReferenceDimId,
		-- *INF*: IIF(ISNULL(InsuranceReferenceDimId), -1, InsuranceReferenceDimId)
		IFF(InsuranceReferenceDimId IS NULL, - 1, InsuranceReferenceDimId) AS o_InsuranceReferenceDimId,
		EXP_EarnedPremiumCalculation_IN.SourceSystemID,
		EXP_EarnedPremiumCalculation_IN.o_InsuranceReferenceCoverageDimId_PMS AS InsuranceReferenceCoverageDimId_PMS,
		EXP_EarnedPremiumCalculation_IN.o_InsuranceReferenceCoverageDimId_DCT AS InsuranceReferenceCoverageDimId_DCT,
		-- *INF*: DECODE(SourceSystemID,'DCT',InsuranceReferenceCoverageDimId_DCT,'PMS',InsuranceReferenceCoverageDimId_PMS)
		DECODE(SourceSystemID,
		'DCT', InsuranceReferenceCoverageDimId_DCT,
		'PMS', InsuranceReferenceCoverageDimId_PMS) AS o_InsuranceReferenceCoverageDimId,
		EXP_EarnedPremiumCalculation_IN.o_CoverageDetailDimId_lkp AS CoverageDetailDimId,
		EXP_EarnedPremiumCalculation_IN.StatisticalCoverageCancellationDate AS i_StatisticalCoverageCancellationDate,
		-- *INF*: :LKP.LKP_CALENDER_DIM(TO_DATE(TO_CHAR(i_StatisticalCoverageCancellationDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
		LKP_CALENDER_DIM_TO_DATE_TO_CHAR_i_StatisticalCoverageCancellationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_StatisticalCoverageCancellationDateId,
		-- *INF*: IIF(NOT ISNULL(v_StatisticalCoverageCancellationDateId),v_StatisticalCoverageCancellationDateId,-1)
		IFF(NOT v_StatisticalCoverageCancellationDateId IS NULL, v_StatisticalCoverageCancellationDateId, - 1) AS o_StatisticalCoverageCancellationDateId,
		EXP_EarnedPremiumCalculation_IN.o_GeneratedRecordIndicator AS GeneratedRecordIndicator,
		EXP_EarnedPremiumCalculation_IN.ModifiedPremiumType
		FROM EXP_EarnedPremiumCalculation_IN
		LEFT JOIN LKP_InsuranceReferenceDimId
		ON LKP_InsuranceReferenceDimId.EnterpriseGroupCode = EXP_EarnedPremiumCalculation_IN.EnterpriseGroupCode AND LKP_InsuranceReferenceDimId.InsuranceReferenceLegalEntityCode = EXP_EarnedPremiumCalculation_IN.InsuranceReferenceLegalEntityCode AND LKP_InsuranceReferenceDimId.StrategicProfitCenterCode = EXP_EarnedPremiumCalculation_IN.StrategicProfitCenterCode AND LKP_InsuranceReferenceDimId.InsuranceSegmentCode = EXP_EarnedPremiumCalculation_IN.InsuranceSegmentCode AND LKP_InsuranceReferenceDimId.PolicyOfferingCode = EXP_EarnedPremiumCalculation_IN.PolicyOfferingCode AND LKP_InsuranceReferenceDimId.ProductCode = EXP_EarnedPremiumCalculation_IN.ProductCode AND LKP_InsuranceReferenceDimId.InsuranceReferenceLineOfBusinessCode = EXP_EarnedPremiumCalculation_IN.LineOfBusinessCode AND LKP_InsuranceReferenceDimId.RatingPlanCode = EXP_EarnedPremiumCalculation_IN.o_RatingPlanCode
		LEFT JOIN LKP_RiskLocationDim
		ON LKP_RiskLocationDim.RiskLocationHashKey = EXP_EarnedPremiumCalculation_IN.RiskLocationHashKey
		LEFT JOIN LKP_asl_dim
		ON LKP_asl_dim.asl_code = EXP_EarnedPremiumCalculation_IN.AnnualStatementLineCode AND LKP_asl_dim.sub_asl_code = EXP_EarnedPremiumCalculation_IN.SubAnnualStatementLineCode AND LKP_asl_dim.sub_non_asl_code = EXP_EarnedPremiumCalculation_IN.o_NonSubAnnualStatementLineCode
		LEFT JOIN LKP_asl_product_code
		ON LKP_asl_product_code.asl_prdct_code = EXP_EarnedPremiumCalculation_IN.AnnualStatementLineProductCode
		LEFT JOIN lkp_PremiumTransactionTypeDim
		ON lkp_PremiumTransactionTypeDim.PremiumTransactionCode = EXP_EarnedPremiumCalculation_IN.PremiumTransactionCode AND lkp_PremiumTransactionTypeDim.ReasonAmendedCode = EXP_EarnedPremiumCalculation_IN.ReasonAmendedCode AND lkp_PremiumTransactionTypeDim.PremiumTypeCode = EXP_EarnedPremiumCalculation_IN.PremiumType
		LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionBookedDate_MM_DD_YYYY_MM_DD_YYYY
		ON LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionBookedDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(PremiumTransactionBookedDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')
	
		LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionRunDate_MM_DD_YYYY_MM_DD_YYYY
		ON LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionRunDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(PremiumTransactionRunDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')
	
		LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionEnteredDate_MM_DD_YYYY_MM_DD_YYYY
		ON LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionEnteredDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(PremiumTransactionEnteredDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')
	
		LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionEffectiveDate_MM_DD_YYYY_MM_DD_YYYY
		ON LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(PremiumTransactionEffectiveDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')
	
		LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionExpirationDate_MM_DD_YYYY_MM_DD_YYYY
		ON LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionExpirationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(PremiumTransactionExpirationDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')
	
		LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_TO_DATE_TO_CHAR_i_StatisticalCoverageCancellationDate_MM_DD_YYYY_MM_DD_YYYY
		ON LKP_CALENDER_DIM_TO_DATE_TO_CHAR_i_StatisticalCoverageCancellationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_DATE(TO_CHAR(i_StatisticalCoverageCancellationDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')
	
	),
	EXP_Evaluate_Fields AS (
		SELECT
		ASLdimID_out AS AnnualStatementLineDimID,
		ASLproductcodedimID_out AS AnnualStatementLineProductCodeDimID,
		AgencyDimID_out AS AgencyDimID,
		PolicyDimID_out AS PolicyDimID,
		ContractCustomerDimID_out AS ContractCustomerDimID,
		RiskLocationDimID_out AS RiskLocationDimID,
		PremiumTransactionTypeDimID_out AS PremiumTransactionTypeDimID,
		PremiumTransactionEnteredDateID_out AS PremiumTransactionEnteredDateID,
		PremiumTransactionEffectiveDateID_out AS PremiumTransactionEffectiveDateID,
		PremiumTransactionExpirationDateID_out AS PremiumTransactionExpirationDateID,
		PremiumTransactionBookedDateID_out AS PremiumTransactionBookedDateID,
		-1 AS EDWEarnedPremiumMonthlyCalculationPKID,
		PremiumTransactionRunDateID_out AS PremiumTransactionRunDateID,
		DateFlagForWrittenPremium,
		PremiumType,
		ChangeInEarnedPremium,
		-- *INF*: IIF(rtrim(PremiumType)='D',ChangeInEarnedPremium,0)
		IFF(rtrim(PremiumType) = 'D', ChangeInEarnedPremium, 0) AS v_ChangeinDirectEarnedPremium,
		v_ChangeinDirectEarnedPremium AS ChangeinDirectEarnedPremium,
		o_InsuranceReferenceDimId AS InsuranceReferenceDimId,
		PremiumMasterCalculationPKID AS EDWPremiumMasterCalculationId,
		o_InsuranceReferenceCoverageDimId AS InsuranceReferenceCoverageDimId,
		CoverageDetailDimId,
		o_StatisticalCoverageCancellationDateId AS StatisticalCoverageCancellationDateId,
		GeneratedRecordIndicator,
		ModifiedPremiumType
		FROM EXP_Consolidate_Data_from_Lookups
	),
	Union_WC_OtherLinesGenerated AS (
		SELECT EDWPremiumMasterCalculationId, PremiumTransactionEnteredDateID AS PremiumTransactionEnteredDateId, PremiumTransactionEffectiveDateID AS PremiumTransactionEffectiveDateId, PremiumTransactionRunDateID AS EarnedPremiumRunDateId, AnnualStatementLineDimID, AnnualStatementLineProductCodeDimID AS AnnualStatementLineProductCodeDimId, AgencyDimID AS AgencyDimId, PolicyDimID AS PolicyDimId, ContractCustomerDimID AS ContractCustomerDimId, RiskLocationDimID AS RiskLocationDimId, PremiumTransactionTypeDimID AS PremiumTransactionTypeDimId, InsuranceReferenceDimId, InsuranceReferenceCoverageDimId, CoverageDetailDimId, PremiumTransactionBookedDateID AS PremiumTransactionBookedDateId, PremiumTransactionExpirationDateID AS PremiumTransactionExpirationDateId, StatisticalCoverageCancellationDateId AS CoverageCancellationDateId, GeneratedRecordIndicator AS GeneratedRecordFlag, ChangeinDirectEarnedPremium AS ChangeInEarnedPremium, ModifiedPremiumType, EDWEarnedPremiumMonthlyCalculationPKID
		FROM EXP_Evaluate_Fields
		UNION
		SELECT EDWPremiumMasterCalculationPKId AS EDWPremiumMasterCalculationId, PremiumTransactionEnteredDateID AS PremiumTransactionEnteredDateId, PremiumTransactionEffectiveDateID AS PremiumTransactionEffectiveDateId, PremiumTransactionRunDateID AS EarnedPremiumRunDateId, AnnualStatementLineDimID, AnnualStatementLineProductCodeDimID AS AnnualStatementLineProductCodeDimId, AgencyDimID AS AgencyDimId, PolicyDimID AS PolicyDimId, ContractCustomerDimID AS ContractCustomerDimId, RiskLocationDimID AS RiskLocationDimId, PremiumTransactionTypeDimID AS PremiumTransactionTypeDimId, InsuranceReferenceDimId, InsuranceReferenceCoverageDimId, CoverageDetailDimId, PremiumTransactionBookedDateID AS PremiumTransactionBookedDateId, PremiumTransactionExpirationDateID AS PremiumTransactionExpirationDateId, CoverageCancellationDateId, GeneratedRecordIndicator AS GeneratedRecordFlag, MonthlyChangeinDirectEarnedPremium AS ChangeInEarnedPremium, ModifiedPremiumType, EDWEarnedPremiumMonthlyCalculationPKID
		FROM EXP_DirectPremium
	),
	AGG_Premium AS (
		SELECT
		EDWPremiumMasterCalculationId, 
		PremiumTransactionEnteredDateId, 
		PremiumTransactionEffectiveDateId, 
		EarnedPremiumRunDateId, 
		AnnualStatementLineDimID, 
		AnnualStatementLineProductCodeDimId, 
		AgencyDimId, 
		PolicyDimId, 
		ContractCustomerDimId, 
		RiskLocationDimId, 
		PremiumTransactionTypeDimId, 
		InsuranceReferenceDimId, 
		InsuranceReferenceCoverageDimId, 
		CoverageDetailDimId, 
		PremiumTransactionBookedDateId, 
		PremiumTransactionExpirationDateId, 
		CoverageCancellationDateId, 
		GeneratedRecordFlag, 
		EDWEarnedPremiumMonthlyCalculationPKID, 
		ChangeInEarnedPremium, 
		ModifiedPremiumType, 
		SUM(ChangeInEarnedPremium, ModifiedPremiumType = 'Direct') AS EarnedDirectWrittenPremium, 
		SUM(ChangeInEarnedPremium, ModifiedPremiumType = 'Classified') AS EarnedClassifiedPremium, 
		SUM(ChangeInEarnedPremium, ModifiedPremiumType = 'Ratable') AS EarnedRatablePremium, 
		SUM(ChangeInEarnedPremium, ModifiedPremiumType = 'Other') AS EarnedOtherModifiedPremium, 
		SUM(ChangeInEarnedPremium, ModifiedPremiumType = 'Schedule') AS EarnedScheduleModifiedPremium, 
		SUM(ChangeInEarnedPremium, ModifiedPremiumType = 'Experience') AS EarnedExperienceModifiedPremium, 
		SUM(ChangeInEarnedPremium, ModifiedPremiumType = 'Subject') AS EarnedSubjectWrittenPremium
		FROM Union_WC_OtherLinesGenerated
		GROUP BY EDWPremiumMasterCalculationId, PremiumTransactionEnteredDateId, EarnedPremiumRunDateId, AnnualStatementLineDimID, AnnualStatementLineProductCodeDimId, GeneratedRecordFlag, EDWEarnedPremiumMonthlyCalculationPKID
	),
	EXP_DefaultPremium AS (
		SELECT
		EDWPremiumMasterCalculationId,
		PremiumTransactionEnteredDateId,
		PremiumTransactionEffectiveDateId,
		EarnedPremiumRunDateId,
		AnnualStatementLineDimID,
		AnnualStatementLineProductCodeDimId,
		AgencyDimId,
		PolicyDimId,
		ContractCustomerDimId,
		RiskLocationDimId,
		PremiumTransactionTypeDimId,
		InsuranceReferenceDimId,
		InsuranceReferenceCoverageDimId,
		CoverageDetailDimId,
		PremiumTransactionBookedDateId,
		PremiumTransactionExpirationDateId,
		CoverageCancellationDateId,
		GeneratedRecordFlag,
		EarnedDirectWrittenPremium AS i_EarnedDirectWrittenPremium,
		EarnedClassifiedPremium AS i_EarnedClassifiedPremium,
		EarnedRatablePremium AS i_EarnedRatablePremium,
		EarnedOtherModifiedPremium AS i_EarnedOtherModifiedPremium,
		EarnedScheduleModifiedPremium AS i_EarnedScheduleModifiedPremium,
		EarnedExperienceModifiedPremium AS i_EarnedExperienceModifiedPremium,
		EarnedSubjectWrittenPremium AS i_EarnedSubjectWrittenPremium,
		-- *INF*: IIF(NOT ISNULL(i_EarnedDirectWrittenPremium),i_EarnedDirectWrittenPremium,0)
		IFF(NOT i_EarnedDirectWrittenPremium IS NULL, i_EarnedDirectWrittenPremium, 0) AS EarnedDirectWrittenPremium,
		-- *INF*: IIF(NOT ISNULL(i_EarnedClassifiedPremium),i_EarnedClassifiedPremium,0)
		IFF(NOT i_EarnedClassifiedPremium IS NULL, i_EarnedClassifiedPremium, 0) AS EarnedClassifiedPremium,
		-- *INF*: IIF(NOT ISNULL(i_EarnedRatablePremium),i_EarnedRatablePremium,0)
		IFF(NOT i_EarnedRatablePremium IS NULL, i_EarnedRatablePremium, 0) AS EarnedRatablePremium,
		-- *INF*: IIF(NOT ISNULL(i_EarnedOtherModifiedPremium),i_EarnedOtherModifiedPremium,0)
		IFF(NOT i_EarnedOtherModifiedPremium IS NULL, i_EarnedOtherModifiedPremium, 0) AS EarnedOtherModifiedPremium,
		-- *INF*: IIF(NOT ISNULL(i_EarnedScheduleModifiedPremium),i_EarnedScheduleModifiedPremium,0)
		IFF(NOT i_EarnedScheduleModifiedPremium IS NULL, i_EarnedScheduleModifiedPremium, 0) AS EarnedScheduleModifiedPremium,
		-- *INF*: IIF(NOT ISNULL(i_EarnedExperienceModifiedPremium),i_EarnedExperienceModifiedPremium,0)
		IFF(NOT i_EarnedExperienceModifiedPremium IS NULL, i_EarnedExperienceModifiedPremium, 0) AS EarnedExperienceModifiedPremium,
		-- *INF*: IIF(NOT ISNULL(i_EarnedSubjectWrittenPremium),i_EarnedSubjectWrittenPremium,0)
		IFF(NOT i_EarnedSubjectWrittenPremium IS NULL, i_EarnedSubjectWrittenPremium, 0) AS EarnedSubjectWrittenPremium,
		EDWEarnedPremiumMonthlyCalculationPKID
		FROM AGG_Premium
	),
	Union_WC_OtherLinesGenerated_OtherLinesNatural AS (
		SELECT EDWPremiumMasterCalculationPKId AS EDWPremiumMasterCalculationId, PremiumTransactionEnteredDateID AS PremiumTransactionEnteredDateId, PremiumTransactionEffectiveDateID AS PremiumTransactionEffectiveDateId, PremiumTransactionRunDateID AS EarnedPremiumRunDateId, AnnualStatementLineDimID, AnnualStatementLineProductCodeDimID AS AnnualStatementLineProductCodeDimId, AgencyDimID AS AgencyDimId, PolicyDimID AS PolicyDimId, ContractCustomerDimID AS ContractCustomerDimId, RiskLocationDimID AS RiskLocationDimId, PremiumTransactionTypeDimID AS PremiumTransactionTypeDimId, InsuranceReferenceDimId, InsuranceReferenceCoverageDimId, CoverageDetailDimId, PremiumTransactionBookedDateID AS PremiumTransactionBookedDateId, PremiumTransactionExpirationDateID AS PremiumTransactionExpirationDateId, CoverageCancellationDateId, GeneratedRecordFlag, EarnedDirectWrittenPremium, EarnedClassifiedPremium, EarnedRatablePremium, o_EarnedOtherModifiedPremium AS EarnedOtherModifiedPremium, o_EarnedScheduleModifiedPremium AS EarnedScheduleModifiedPremium, o_EarnedExperienceModifiedPremium AS EarnedExperienceModifiedPremium, o_EarnedSubjectWrittenPremium AS EarnedSubjectWrittenPremium, EDWEarnedPremiumMonthlyCalculationPKID
		FROM EXP_OtherLinesEarnedPremium
		UNION
		SELECT EDWPremiumMasterCalculationId, PremiumTransactionEnteredDateId, PremiumTransactionEffectiveDateId, EarnedPremiumRunDateId, AnnualStatementLineDimID, AnnualStatementLineProductCodeDimId, AgencyDimId, PolicyDimId, ContractCustomerDimId, RiskLocationDimId, PremiumTransactionTypeDimId, InsuranceReferenceDimId, InsuranceReferenceCoverageDimId, CoverageDetailDimId, PremiumTransactionBookedDateId, PremiumTransactionExpirationDateId, CoverageCancellationDateId, GeneratedRecordFlag, EarnedDirectWrittenPremium, EarnedClassifiedPremium, EarnedRatablePremium, EarnedOtherModifiedPremium, EarnedScheduleModifiedPremium, EarnedExperienceModifiedPremium, EarnedSubjectWrittenPremium, EDWEarnedPremiumMonthlyCalculationPKID
		FROM EXP_DefaultPremium
	),
	OUTPUT AS (
		SELECT
		EDWPremiumMasterCalculationId, 
		PremiumTransactionEnteredDateId, 
		PremiumTransactionEffectiveDateId, 
		EarnedPremiumRunDateId, 
		AnnualStatementLineDimID, 
		AnnualStatementLineProductCodeDimId, 
		AgencyDimId, 
		PolicyDimId, 
		ContractCustomerDimId, 
		RiskLocationDimId, 
		PremiumTransactionTypeDimId, 
		InsuranceReferenceDimId, 
		InsuranceReferenceCoverageDimId, 
		CoverageDetailDimId, 
		PremiumTransactionBookedDateId, 
		PremiumTransactionExpirationDateId, 
		CoverageCancellationDateId, 
		GeneratedRecordFlag, 
		EarnedDirectWrittenPremium, 
		EarnedClassifiedPremium, 
		EarnedRatablePremium, 
		EarnedOtherModifiedPremium, 
		EarnedScheduleModifiedPremium, 
		EarnedExperienceModifiedPremium, 
		EarnedSubjectWrittenPremium, 
		EDWEarnedPremiumMonthlyCalculationPKID
		FROM Union_WC_OtherLinesGenerated_OtherLinesNatural
	),
),
EXP_Audit AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	EDWPremiumMasterCalculationId,
	PremiumTransactionEnteredDateId2 AS PremiumTransactionEnteredDateId,
	PremiumTransactionEffectiveDateId2 AS PremiumTransactionEffectiveDateId,
	'0' AS DirectOrEarnedPremiumFlag,
	EarnedPremiumRunDateId,
	AnnualStatementLineDimID2 AS AnnualStatementLineDimID,
	AnnualStatementLineProductCodeDimId2 AS AnnualStatementLineProductCodeDimId,
	AgencyDimId3 AS AgencyDimId,
	PolicyDimId3 AS PolicyDimId,
	ContractCustomerDimId3 AS ContractCustomerDimId,
	RiskLocationDimId2 AS RiskLocationDimId,
	PremiumTransactionTypeDimId2 AS PremiumTransactionTypeDimId,
	InsuranceReferenceDimId2 AS InsuranceReferenceDimId,
	InsuranceReferenceCoverageDimId2 AS InsuranceReferenceCoverageDimId,
	CoverageDetailDimId2 AS CoverageDetailDimId,
	PremiumTransactionBookedDateId2 AS PremiumTransactionBookedDateId,
	PremiumTransactionExpirationDateId2 AS PremiumTransactionExpirationDateId,
	CoverageCancellationDateId2 AS CoverageCancellationDateId,
	GeneratedRecordFlag,
	0 AS DirectWrittenPremium,
	0 AS RatablePremium,
	0 AS ClassifiedPremium,
	0 AS OtherModifiedPremium,
	0 AS ScheduleModifiedPremium,
	0 AS ExperienceModifiedPremium,
	0 AS SubjectWrittenPremium,
	EarnedDirectWrittenPremium,
	EarnedClassifiedPremium,
	EarnedRatablePremium,
	EarnedOtherModifiedPremium,
	EarnedScheduleModifiedPremium,
	EarnedExperienceModifiedPremium,
	EarnedSubjectWrittenPremium,
	EDWEarnedPremiumMonthlyCalculationPKID2 AS EDWEarnedPremiumMonthlyCalculationPKID
	FROM mplt_EarnedPremium
),
LKP_ModifiedEarnedPremiumTransactionMonthlyFact AS (
	SELECT
	ModifiedEarnedPremiumTransactionMonthlyFactId,
	EDWPremiumMasterCalculationPKId,
	RunDateId,
	PremiumTransactionEnteredDateId,
	AnnualStatementLineDimID,
	AnnualStatementLineProductCodeDimId
	FROM (
		SELECT ModifiedEarnedPremiumTransactionMonthlyFactId as ModifiedEarnedPremiumTransactionMonthlyFactId,
		EDWPremiumMasterCalculationPKId as EDWPremiumMasterCalculationPKId,
		RunDateId as RunDateId,
		PremiumTransactionEnteredDateId as PremiumTransactionEnteredDateId,
		AnnualStatementLineDimID as AnnualStatementLineDimID,
		AnnualStatementLineProductCodeDimId as AnnualStatementLineProductCodeDimId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.@{pipeline().parameters.TARGET_TABLE_NAME} F 
		@{pipeline().parameters.TARGET_LOOKUP_SOURCE_FILTER}
		ORDER BY EDWPremiumMasterCalculationPKId,RunDateId,PremiumTransactionEnteredDateId,AnnualStatementLineDimID,AnnualStatementLineProductCodeDimId --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWPremiumMasterCalculationPKId,RunDateId,PremiumTransactionEnteredDateId,AnnualStatementLineDimID,AnnualStatementLineProductCodeDimId ORDER BY ModifiedEarnedPremiumTransactionMonthlyFactId) = 1
),
RTR_INSERT_UPDATE AS (
	SELECT
	LKP_ModifiedEarnedPremiumTransactionMonthlyFact.ModifiedEarnedPremiumTransactionMonthlyFactId AS ModifiedPremiumTransactionMonthlyFactId,
	EXP_Audit.AuditId,
	EXP_Audit.EDWPremiumMasterCalculationId,
	EXP_Audit.PremiumTransactionEnteredDateId,
	EXP_Audit.PremiumTransactionEffectiveDateId,
	EXP_Audit.EarnedPremiumRunDateId,
	EXP_Audit.DirectOrEarnedPremiumFlag,
	EXP_Audit.AnnualStatementLineDimID,
	EXP_Audit.AnnualStatementLineProductCodeDimId,
	EXP_Audit.AgencyDimId,
	EXP_Audit.PolicyDimId,
	EXP_Audit.ContractCustomerDimId,
	EXP_Audit.RiskLocationDimId,
	EXP_Audit.PremiumTransactionTypeDimId,
	EXP_Audit.InsuranceReferenceDimId,
	EXP_Audit.InsuranceReferenceCoverageDimId,
	EXP_Audit.CoverageDetailDimId,
	EXP_Audit.PremiumTransactionBookedDateId,
	EXP_Audit.PremiumTransactionExpirationDateId,
	EXP_Audit.CoverageCancellationDateId,
	EXP_Audit.GeneratedRecordFlag,
	EXP_Audit.DirectWrittenPremium,
	EXP_Audit.RatablePremium,
	EXP_Audit.ClassifiedPremium,
	EXP_Audit.OtherModifiedPremium,
	EXP_Audit.ScheduleModifiedPremium,
	EXP_Audit.ExperienceModifiedPremium,
	EXP_Audit.SubjectWrittenPremium,
	EXP_Audit.EarnedDirectWrittenPremium,
	EXP_Audit.EarnedClassifiedPremium,
	EXP_Audit.EarnedRatablePremium,
	EXP_Audit.EarnedOtherModifiedPremium,
	EXP_Audit.EarnedScheduleModifiedPremium,
	EXP_Audit.EarnedExperienceModifiedPremium,
	EXP_Audit.EarnedSubjectWrittenPremium,
	EXP_Audit.EDWEarnedPremiumMonthlyCalculationPKID
	FROM EXP_Audit
	LEFT JOIN LKP_ModifiedEarnedPremiumTransactionMonthlyFact
	ON LKP_ModifiedEarnedPremiumTransactionMonthlyFact.EDWPremiumMasterCalculationPKId = EXP_Audit.EDWPremiumMasterCalculationId AND LKP_ModifiedEarnedPremiumTransactionMonthlyFact.RunDateId = EXP_Audit.EarnedPremiumRunDateId AND LKP_ModifiedEarnedPremiumTransactionMonthlyFact.PremiumTransactionEnteredDateId = EXP_Audit.PremiumTransactionEnteredDateId AND LKP_ModifiedEarnedPremiumTransactionMonthlyFact.AnnualStatementLineDimID = EXP_Audit.AnnualStatementLineDimID AND LKP_ModifiedEarnedPremiumTransactionMonthlyFact.AnnualStatementLineProductCodeDimId = EXP_Audit.AnnualStatementLineProductCodeDimId
),
RTR_INSERT_UPDATE_INSERT AS (SELECT * FROM RTR_INSERT_UPDATE WHERE ISNULL(ModifiedPremiumTransactionMonthlyFactId)),
ModifiedEarnedPremiumTransactionMonthlyFact AS (

	------------ PRE SQL ----------
	@{pipeline().parameters.PRE_SQL}
	-------------------------------


	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.@{pipeline().parameters.TARGET_TABLE_NAME}
	(AuditId, AnnualStatementLineDimID, AnnualStatementLineProductCodeDimId, AgencyDimId, PolicyDimId, ContractCustomerDimId, RiskLocationDimId, InsuranceReferenceDimId, InsuranceReferenceCoverageDimId, CoverageDetailDimId, PremiumTransactionTypeDimId, EDWPremiumMasterCalculationPKId, RunDateId, PremiumTransactionEnteredDateId, PremiumTransactionEffectiveDateId, PremiumTransactionBookedDateId, PremiumTransactionExpirationDateId, CoverageCancellationDateId, GeneratedRecordFlag, EarnedDirectWrittenPremium, EarnedClassifiedPremium, EarnedRatablePremium, EarnedOtherModifiedPremium, EarnedScheduleModifiedPremium, EarnedExperienceModifiedPremium, EarnedSubjectWrittenPremium, EDWEarnedPremiumMonthlyCalculationPKID)
	SELECT 
	AUDITID, 
	ANNUALSTATEMENTLINEDIMID, 
	ANNUALSTATEMENTLINEPRODUCTCODEDIMID, 
	AGENCYDIMID, 
	POLICYDIMID, 
	CONTRACTCUSTOMERDIMID, 
	RISKLOCATIONDIMID, 
	INSURANCEREFERENCEDIMID, 
	INSURANCEREFERENCECOVERAGEDIMID, 
	COVERAGEDETAILDIMID, 
	PREMIUMTRANSACTIONTYPEDIMID, 
	EDWPremiumMasterCalculationId AS EDWPREMIUMMASTERCALCULATIONPKID, 
	EarnedPremiumRunDateId AS RUNDATEID, 
	PREMIUMTRANSACTIONENTEREDDATEID, 
	PREMIUMTRANSACTIONEFFECTIVEDATEID, 
	PREMIUMTRANSACTIONBOOKEDDATEID, 
	PREMIUMTRANSACTIONEXPIRATIONDATEID, 
	COVERAGECANCELLATIONDATEID, 
	GENERATEDRECORDFLAG, 
	EARNEDDIRECTWRITTENPREMIUM, 
	EARNEDCLASSIFIEDPREMIUM, 
	EARNEDRATABLEPREMIUM, 
	EARNEDOTHERMODIFIEDPREMIUM, 
	EARNEDSCHEDULEMODIFIEDPREMIUM, 
	EARNEDEXPERIENCEMODIFIEDPREMIUM, 
	EARNEDSUBJECTWRITTENPREMIUM, 
	EDWEARNEDPREMIUMMONTHLYCALCULATIONPKID
	FROM RTR_INSERT_UPDATE_INSERT
),
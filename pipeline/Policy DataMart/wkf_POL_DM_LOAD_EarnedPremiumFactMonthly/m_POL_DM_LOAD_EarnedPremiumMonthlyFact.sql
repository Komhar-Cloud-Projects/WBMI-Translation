WITH
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
		select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.EarnedPremiumMonthlyCalculation EPMC
		where EPMC.PremiumTransactionAKId=PT.PremiumTransactionAKId
		and EPMC.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}')
		or '@{pipeline().parameters.SELECTION_START_TS}'<='01/01/1800 00:00:00'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKId ORDER BY CoverageDetailDimId) = 1
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
lkp_sup_reason_amended_code AS (
	SELECT
	StandardReasonAmendedCode,
	rsn_amended_code
	FROM (
		SELECT StandardReasonAmendedCode as StandardReasonAmendedCode,
		LOWER(rsn_amended_code) as rsn_amended_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_reason_amended_code
		WHERE crrnt_snpsht_flag=1 and source_sys_id='DCT'
		ORDER BY LOWER(rsn_amended_code)
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY rsn_amended_code ORDER BY StandardReasonAmendedCode) = 1
),
LKP_sup_premium_transaction_code AS (
	SELECT
	StandardPremiumTransactionCode,
	source_sys_id,
	prem_trans_code
	FROM (
		SELECT 
			StandardPremiumTransactionCode,
			source_sys_id,
			prem_trans_code
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_premium_transaction_code
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY source_sys_id,prem_trans_code ORDER BY StandardPremiumTransactionCode) = 1
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
LKP_CoverageDetailDim_Hist AS (
	SELECT
	CoverageDetailDimId,
	StatisticalCoverageAKID
	FROM (
		select CDD.CoverageDetailDimId as CoverageDetailDimId, 
		EPMC.StatisticalCoverageAKID as StatisticalCoverageAKID
		from  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.EarnedPremiumMonthlyCalculation EPMC
		join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
		on EPMC.StatisticalCoverageAKId=SC.StatisticalCoverageAKId
		join @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
		on  SC.CoverageGUID=CDD.CoverageGUID and EPMC.PremiumTransactionEffectiveDate between CDD.EffectiveDate and CDD.ExpirationDate
		where EPMC.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}' and EPMC.PremiumTransactionAKID=-1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StatisticalCoverageAKID ORDER BY CoverageDetailDimId) = 1
),
SQ_EarnedPremiumMonthlyCalculation AS (
	SELECT EPMC.EarnedPremiumMonthlyCalculationID, 
	EPMC.CurrentSnapshotFlag, 
	EPMC.AuditID, 
	EPMC.EffectiveDate, 
	EPMC.ExpirationDate, 
	EPMC.SourceSystemID, 
	EPMC.CreatedDate, 
	EPMC.ModifiedDate, 
	EPMC.PolicyKey, 
	EPMC.AgencyAKID, 
	EPMC.ContractCustomerAKID, 
	EPMC.PolicyAKID, 
	EPMC.PolicyCoverageAKID, 
	EPMC.StatisticalCoverageAKID, 
	EPMC.ReinsuranceCoverageAKID, 
	EPMC.PremiumTransactionAKID, 
	EPMC.BureauStatisticalCodeAKID, 
	EPMC.PremiumMasterCalculationPKID, 
	EPMC.PolicyEffectiveDate, 
	EPMC.PolicyExpirationDate, 
	EPMC.StatisticalCoverageCancellationDate, 
	EPMC.PremiumTransactionEnteredDate, 
	EPMC.PremiumTransactionEffectiveDate, 
	EPMC.PremiumTransactionExpirationDate, 
	EPMC.PremiumTransactionBookedDate, 
	EPMC.PremiumTransactionCode, 
	EPMC.PremiumTransactionAmount, 
	EPMC.FullTermPremium, 
	EPMC.PremiumType, 
	EPMC.ReasonAmendedCode, 
	EPMC.EarnedPremium, 
	EPMC.ChangeInEarnedPremium, 
	EPMC.UnearnedPremium, 
	EPMC.ChangeInUnearnedPremium, 
	isnull(PRODUCT.ProductCode, '000') as ProductCode, 
	EPMC.AnnualStatementLineCode, 
	EPMC.SubAnnualStatementLineCode, 
	EPMC.NonSubAnnualStatementLineCode, 
	EPMC.AnnualStatementLineProductCode, 
	Isnull(InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessCode, '000') as LineOfBusinessCode, 
	Isnull(PolicyOffering.PolicyOfferingCode, '000') as PolicyOfferingCode, 
	EPMC.RunDate, 
	EPMC.RatingCoverageAKId, 
	EPMC.RatingCoverageEffectiveDate, 
	EPMC.RatingCoverageExpirationDate, 
	EPMC.EarnedExposure, 
	EPMC.ChangeInEarnedExposure, 
	isnull(SPC.StrategicProfitCenterCode, '6') as StrategicProfitCenterCode, 
	isnull(EGP.EnterpriseGroupCode, '1') as EnterpriseGroupCode, 
	isnull(IRLE.InsuranceReferenceLegalEntityCode, '1') as InsuranceReferenceLegalEntityCode, 
	isnull(ISS.InsuranceSegmentCode, 'N/A') as InsuranceSegmentCode, 
	SC.RiskUnitGroup, 
	SC.RiskUnit, 
	SC.RiskUnitSequenceNumber, 
	SC.MajorPerilCode, 
	SC.ClassCode, 
	'N/A' as RiskType, 
	'N/A' as CoverageType, 
	SIL.StandardInsuranceLineCode, 
	RL.RiskLocationHashKey, 
	PC.TypeBureauCode,
	'N/A' as PerilGroup,
	'N/A' as CoverageForm,
	PC.PolicyCoverageEffectiveDate,
	PC.PolicyCoverageExpirationDate,
	'N/A' as SubCoverageTypeCode,
	'N/A' as CoverageVersion,
	PC.CustomerCareCommissionRate,
	RPDT.RatingPlanCode
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.EarnedPremiumMonthlyCalculation EPMC
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
	             ON EPMC.StatisticalCoverageAKID=SC.StatisticalCoverageAKID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	             ON EPMC.PolicyCoverageAKID=PC.PolicyCoverageAKID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	             ON EPMC.RiskLocationAKID=RL.RiskLocationAKID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Policy P
			ON EPMC.PolicyAKID=P.pol_ak_id
			AND P.crrnt_snpsht_flag=1
			
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product 
	on SC.ProductAKID=Product.ProductAKID and Product.CurrentSnapshotFlag=1
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLineOfBusiness 
	On SC.InsuranceReferenceLineOfBusinessAKID=InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessAKId and InsuranceReferenceLineOfBusiness.CurrentSnapshotFlag=1
	
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
	WHERE EPMC.CreatedDate  >= '@{pipeline().parameters.SELECTION_START_TS}' and EPMC.SourceSystemID='PMS'
	@{pipeline().parameters.WHERE_CLAUSE} 
	
	union all
	SELECT EPMC.EarnedPremiumMonthlyCalculationID, 
	EPMC.CurrentSnapshotFlag, 
	EPMC.AuditID, 
	EPMC.EffectiveDate, 
	EPMC.ExpirationDate, 
	EPMC.SourceSystemID, 
	EPMC.CreatedDate, 
	EPMC.ModifiedDate, 
	EPMC.PolicyKey, 
	EPMC.AgencyAKID, 
	EPMC.ContractCustomerAKID, 
	EPMC.PolicyAKID, 
	EPMC.PolicyCoverageAKID, 
	EPMC.StatisticalCoverageAKID, 
	EPMC.ReinsuranceCoverageAKID, 
	EPMC.PremiumTransactionAKID, 
	EPMC.BureauStatisticalCodeAKID, 
	EPMC.PremiumMasterCalculationPKID, 
	EPMC.PolicyEffectiveDate, 
	EPMC.PolicyExpirationDate, 
	EPMC.StatisticalCoverageCancellationDate, 
	EPMC.PremiumTransactionEnteredDate, 
	EPMC.PremiumTransactionEffectiveDate, 
	EPMC.PremiumTransactionExpirationDate, 
	EPMC.PremiumTransactionBookedDate, 
	EPMC.PremiumTransactionCode, 
	EPMC.PremiumTransactionAmount, 
	EPMC.FullTermPremium, 
	EPMC.PremiumType, 
	EPMC.ReasonAmendedCode, 
	EPMC.EarnedPremium, 
	EPMC.ChangeInEarnedPremium, 
	EPMC.UnearnedPremium, 
	EPMC.ChangeInUnearnedPremium, 
	isnull(PRODUCT.ProductCode, '000') as ProductCode, 
	EPMC.AnnualStatementLineCode, 
	EPMC.SubAnnualStatementLineCode, 
	EPMC.NonSubAnnualStatementLineCode, 
	EPMC.AnnualStatementLineProductCode, 
	Isnull(InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessCode, '000') as LineOfBusinessCode, 
	Isnull(PolicyOffering.PolicyOfferingCode, '000') as PolicyOfferingCode, 
	EPMC.RunDate, 
	EPMC.RatingCoverageAKId, 
	EPMC.RatingCoverageEffectiveDate, 
	EPMC.RatingCoverageExpirationDate, 
	EPMC.EarnedExposure, 
	EPMC.ChangeInEarnedExposure, 
	isnull(SPC.StrategicProfitCenterCode, '6') as StrategicProfitCenterCode, 
	isnull(EGP.EnterpriseGroupCode, '1') as EnterpriseGroupCode, 
	isnull(IRLE.InsuranceReferenceLegalEntityCode, '1') as InsuranceReferenceLegalEntityCode, 
	isnull(ISS.InsuranceSegmentCode, 'N/A') as InsuranceSegmentCode, 
	'N/A' as RiskUnitGroup, 
	'N/A' as RiskUnit, 
	'N/A' as RiskUnitSequenceNumber, 
	'N/A' as MajorPerilCode, 
	RC.ClassCode, 
	RC.RiskType, 
	RC.CoverageType, 
	ISNULL(SIL.StandardInsuranceLineCode,'N/A') StandardInsuranceLineCode, 
	RL.RiskLocationHashKey, 
	PC.TypeBureauCode,
	RC.PerilGroup,
	RC.CoverageForm,
	PC.PolicyCoverageEffectiveDate,
	PC.PolicyCoverageExpirationDate,
	RC.SubCoverageTypeCode as SubCoverageTypeCode,
	RC.CoverageVersion as CoverageVersion,
	PC.CustomerCareCommissionRate,
	RPDT.RatingPlanCode
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.EarnedPremiumMonthlyCalculation EPMC
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	             ON EPMC.RatingCoverageAKID=RC.RatingCoverageAKId 
	             AND RC.EffectiveDate=EPMC.RatingCoverageEffectiveDate
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	             ON EPMC.PolicyCoverageAKID=PC.PolicyCoverageAKID 
	             AND PC.CurrentSnapshotFlag=1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	             ON EPMC.RiskLocationAKID=RL.RiskLocationAKID 
	             AND RL.CurrentSnapshotFlag=1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Policy P
			ON EPMC.PolicyAKID=P.pol_ak_id
			AND P.crrnt_snpsht_flag=1
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product 
	on RC.ProductAKID=Product.ProductAKID and Product.CurrentSnapshotFlag=1
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLineOfBusiness 
	On RC.InsuranceReferenceLineOfBusinessAKID=InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessAKId and InsuranceReferenceLineOfBusiness.CurrentSnapshotFlag=1
	
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
	WHERE EPMC.CreatedDate  >= '@{pipeline().parameters.SELECTION_START_TS}' and EPMC.SourceSystemID='DCT'
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_EarnedPremiumCalculation_IN AS (
	SELECT
	EarnedPremiumMonthlyCalculationID AS EarnedPremiumCalculationID,
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	PolicyKey,
	AgencyAKID,
	ContractCustomerAKID,
	PolicyAKID,
	PolicyCoverageAKID,
	StatisticalCoverageAKID,
	ReinsuranceCoverageAKID,
	PremiumTransactionAKID,
	BureauStatisticalCodeAKID,
	PremiumMasterCalculationPKID,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	StatisticalCoverageCancellationDate,
	PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	PremiumTransactionExpirationDate,
	PremiumTransactionBookedDate,
	PremiumTransactionCode,
	-- *INF*: :LKP.LKP_SUP_PREMIUM_TRANSACTION_CODE(SourceSystemID,PremiumTransactionCode)
	LKP_SUP_PREMIUM_TRANSACTION_CODE_SourceSystemID_PremiumTransactionCode.StandardPremiumTransactionCode AS v_PremiumTransactionCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_PremiumTransactionCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_PremiumTransactionCode) AS PremiumTransactionCode_lkp,
	PremiumTransactionAmount,
	FullTermPremium,
	PremiumType,
	ReasonAmendedCode,
	-- *INF*: IIF(
	-- SourceSystemID='DCT',
	-- :LKP.LKP_SUP_REASON_AMENDED_CODE(LOWER(ReasonAmendedCode)),
	-- ReasonAmendedCode
	-- )
	IFF(SourceSystemID = 'DCT', LKP_SUP_REASON_AMENDED_CODE_LOWER_ReasonAmendedCode.StandardReasonAmendedCode, ReasonAmendedCode) AS v_ReasonAmendedCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_ReasonAmendedCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_ReasonAmendedCode) AS ReasonAmendedCode_lkp,
	EarnedPremium,
	ChangeInEarnedPremium,
	UnearnedPremium,
	ChangeInUnearnedPremium,
	ProductCode,
	AnnualStatementLineCode,
	SubAnnualStatementLineCode,
	NonSubAnnualStatementLineCode AS i_NonSubAnnualStatementLineCode,
	-- *INF*: i_NonSubAnnualStatementLineCode
	-- 
	-- --IIF(SourceSystemID='DCT','N/A',i_NonSubAnnualStatementLineCode)
	i_NonSubAnnualStatementLineCode AS o_NonSubAnnualStatementLineCode,
	AnnualStatementLineProductCode,
	LineOfBusinessCode,
	PolicyOfferingCode,
	RunDate,
	RatingCoverageAKId,
	RatingCoverageEffectiveDate,
	RatingCoverageExpirationDate,
	EarnedExposure,
	ChangeInEarnedExposure,
	StrategicProfitCenterCode,
	EnterpriseGroupCode,
	InsuranceReferenceLegalEntityCode,
	InsuranceSegmentCode,
	PremiumTransactionAKID AS PremiumTransactionID,
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
	-- *INF*: IIF(PremiumTransactionAKID=-1,
	-- --Historical records
	-- :LKP.LKP_COVERAGEDETAILDIM_HIST(StatisticalCoverageAKID),
	-- --Incremental records
	-- :LKP.LKP_CoverageDetailDim(PremiumTransactionID)
	-- )
	IFF(PremiumTransactionAKID = - 1, LKP_COVERAGEDETAILDIM_HIST_StatisticalCoverageAKID.CoverageDetailDimId, LKP_COVERAGEDETAILDIM_PremiumTransactionID.CoverageDetailDimId) AS v_CoverageDetailDimId,
	-- *INF*: IIF(i_StandardInsuranceLineCode='WC','Payroll',NULL)
	IFF(i_StandardInsuranceLineCode = 'WC', 'Payroll', NULL) AS o_ExposureBasisName,
	-- *INF*: IIF(ISNULL(v_InsuranceReferenceCoverageDimId_PMS_2), -1, v_InsuranceReferenceCoverageDimId_PMS_2)
	IFF(v_InsuranceReferenceCoverageDimId_PMS_2 IS NULL, - 1, v_InsuranceReferenceCoverageDimId_PMS_2) AS o_InsuranceReferenceCoverageDimId_PMS,
	-- *INF*: IIF(ISNULL(v_InsuranceReferenceCoverageDimId_DCT), -1, v_InsuranceReferenceCoverageDimId_DCT)
	IFF(v_InsuranceReferenceCoverageDimId_DCT IS NULL, - 1, v_InsuranceReferenceCoverageDimId_DCT) AS o_InsuranceReferenceCoverageDimId_DCT,
	-- *INF*: IIF(ISNULL(v_CoverageDetailDimId), -1, v_CoverageDetailDimId)
	IFF(v_CoverageDetailDimId IS NULL, - 1, v_CoverageDetailDimId) AS o_CoverageDetailDimId_lkp,
	PolicyCoverageEffectiveDate,
	PolicyCoverageExpirationDate,
	CustomerCareCommissionRate,
	RatingPlanCode AS i_RatingPlanCode,
	-- *INF*: IIF(ISNULL(i_RatingPlanCode), '1', i_RatingPlanCode)
	IFF(i_RatingPlanCode IS NULL, '1', i_RatingPlanCode) AS o_RatingPlanCode
	FROM SQ_EarnedPremiumMonthlyCalculation
	LEFT JOIN LKP_SUP_PREMIUM_TRANSACTION_CODE LKP_SUP_PREMIUM_TRANSACTION_CODE_SourceSystemID_PremiumTransactionCode
	ON LKP_SUP_PREMIUM_TRANSACTION_CODE_SourceSystemID_PremiumTransactionCode.source_sys_id = SourceSystemID
	AND LKP_SUP_PREMIUM_TRANSACTION_CODE_SourceSystemID_PremiumTransactionCode.prem_trans_code = PremiumTransactionCode

	LEFT JOIN LKP_SUP_REASON_AMENDED_CODE LKP_SUP_REASON_AMENDED_CODE_LOWER_ReasonAmendedCode
	ON LKP_SUP_REASON_AMENDED_CODE_LOWER_ReasonAmendedCode.rsn_amended_code = LOWER(ReasonAmendedCode)

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

	LEFT JOIN LKP_COVERAGEDETAILDIM LKP_COVERAGEDETAILDIM_PremiumTransactionID
	ON LKP_COVERAGEDETAILDIM_PremiumTransactionID.PremiumTransactionAKId = PremiumTransactionID

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
LKP_SalesDivisionDim AS (
	SELECT
	SalesDivisionDimID,
	PolicyAkId
	FROM (
		select 
						dim.SalesDivisionDimId AS SalesDivisionDimId,
			 			VPOL.pol_ak_id as PolicyAkId
		from  @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Policy VPOL
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Agency Agency
					on VPOL.AgencyAKID=Agency.AgencyAKID  
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RegionalSalesManager RSM
		      on RSM.RegionalSalesManagerAKID = Agency.RegionalSalesManagerAKID
		inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SalesDivisionDim dim
			 on dim.EDWSalesTerritoryAKID = Agency.SalesTerritoryAKID 
			 and Agency.RegionalSalesManagerAKID = dim.EDWRegionalSalesManagerAKID
			 and RSM.SalesDirectorAKID = dim.EDWSalesDirectorAKID
		where VPOL.crrnt_snpsht_flag=1 and Agency.CurrentSnapshotFlag =1 and dim.CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAkId ORDER BY SalesDivisionDimID) = 1
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
LKP_reinsurance_coverage_dim AS (
	SELECT
	reins_cov_dim_id,
	edw_reins_cov_ak_id,
	eff_from_date,
	eff_to_date
	FROM (
		SELECT 
			reins_cov_dim_id,
			edw_reins_cov_ak_id,
			eff_from_date,
			eff_to_date
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.reinsurance_coverage_dim
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_reins_cov_ak_id,eff_from_date,eff_to_date ORDER BY reins_cov_dim_id DESC) = 1
),
lkp_PremiumTransactionTypeDim AS (
	SELECT
	PremiumTransactionTypeDimID,
	in_CustomerCareCommissionRate,
	PremiumTransactionCode,
	ReasonAmendedCode,
	PremiumTypeCode,
	CustomerCareCommissionRate
	FROM (
		SELECT 
		PTTD.PremiumTransactionTypeDimID as PremiumTransactionTypeDimID,
		LTRIM(RTRIM(PTTD.PremiumTransactionCode)) as PremiumTransactionCode, 
		LTRIM(RTRIM(PTTD.ReasonAmendedCode)) as ReasonAmendedCode, 
		LTRIM(RTRIM(PTTD.PremiumTypeCode)) as PremiumTypeCode,
		PTTD.CustomerCareCommissionRate as CustomerCareCommissionRate
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransactionTypeDim PTTD
		where PTTD.CurrentSnapShotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionCode,ReasonAmendedCode,PremiumTypeCode,CustomerCareCommissionRate ORDER BY PremiumTransactionTypeDimID DESC) = 1
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
EXP_Consolidate_Data_from_Lookups AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	mplt_PolicyDimID_PremiumMaster.agency_dim_id AS AgencyDimID,
	-- *INF*: IIF(NOT ISNULL(AgencyDimID),AgencyDimID,-1)
	IFF(NOT AgencyDimID IS NULL, AgencyDimID, - 1) AS AgencyDimID_out,
	mplt_PolicyDimID_PremiumMaster.pol_dim_id AS PolicyDimID,
	-- *INF*: IIF(NOT ISNULL(PolicyDimID),PolicyDimID,-1)
	IFF(NOT PolicyDimID IS NULL, PolicyDimID, - 1) AS PolicyDimID_out,
	mplt_PolicyDimID_PremiumMaster.pol_status_code,
	mplt_PolicyDimID_PremiumMaster.contract_cust_dim_id AS ContractCustomerDimID,
	-- *INF*: IIF(NOT ISNULL(ContractCustomerDimID),ContractCustomerDimID,-1)
	IFF(NOT ContractCustomerDimID IS NULL, ContractCustomerDimID, - 1) AS ContractCustomerDimID_out,
	LKP_RiskLocationDim.RiskLocationDimID,
	-- *INF*: IIF(NOT ISNULL(RiskLocationDimID),RiskLocationDimID,-1)
	IFF(NOT RiskLocationDimID IS NULL, RiskLocationDimID, - 1) AS RiskLocationDimID_out,
	LKP_reinsurance_coverage_dim.reins_cov_dim_id AS ReinsuranceCoverageDimID,
	-- *INF*: IIF(NOT ISNULL(ReinsuranceCoverageDimID),ReinsuranceCoverageDimID,-1)
	IFF(NOT ReinsuranceCoverageDimID IS NULL, ReinsuranceCoverageDimID, - 1) AS ReinsuranceCoverageDimID_out,
	lkp_PremiumTransactionTypeDim.PremiumTransactionTypeDimID,
	-- *INF*: IIF(NOT ISNULL(PremiumTransactionTypeDimID),PremiumTransactionTypeDimID,-1)
	IFF(NOT PremiumTransactionTypeDimID IS NULL, PremiumTransactionTypeDimID, - 1) AS PremiumTransactionTypeDimID_out,
	EXP_EarnedPremiumCalculation_IN.PremiumTransactionID AS EDWPremiumTransactionPKID,
	-- *INF*: IIF(NOT ISNULL(EDWPremiumTransactionPKID),EDWPremiumTransactionPKID,-1)
	IFF(NOT EDWPremiumTransactionPKID IS NULL, EDWPremiumTransactionPKID, - 1) AS EDWPremiumTransactionPKID_out,
	EXP_EarnedPremiumCalculation_IN.PremiumTransactionAmount,
	EXP_EarnedPremiumCalculation_IN.FullTermPremium,
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
	EXP_EarnedPremiumCalculation_IN.PolicyCoverageEffectiveDate,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(PolicyCoverageEffectiveDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_PolicyCoverageEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PolicyCoverageEffectiveDateID,
	-- *INF*: IIF(NOT ISNULL(v_PolicyCoverageEffectiveDateID),v_PolicyCoverageEffectiveDateID,-1)
	IFF(NOT v_PolicyCoverageEffectiveDateID IS NULL, v_PolicyCoverageEffectiveDateID, - 1) AS PolicyCoverageEffectiveDateID_out,
	EXP_EarnedPremiumCalculation_IN.PolicyCoverageExpirationDate,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(PolicyCoverageExpirationDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_PolicyCoverageExpirationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PolicyCoverageExpirationDateID,
	-- *INF*: IIF(NOT ISNULL(v_PolicyCoverageExpirationDateID),v_PolicyCoverageExpirationDateID,-1)
	IFF(NOT v_PolicyCoverageExpirationDateID IS NULL, v_PolicyCoverageExpirationDateID, - 1) AS PolicyCoverageExpirationDateID_out,
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
	EXP_EarnedPremiumCalculation_IN.PolicyEffectiveDate,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(PolicyEffectiveDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_PolicyEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PolicyEffectiveDateID,
	v_PolicyEffectiveDateID AS PolicyEffectiveDateID_out,
	EXP_EarnedPremiumCalculation_IN.PolicyExpirationDate,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(PolicyExpirationDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_PolicyExpirationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PolicyExpirationDateID,
	-- *INF*: IIF(ISNULL(v_PolicyExpirationDateID),-1,v_PolicyExpirationDateID)
	IFF(v_PolicyExpirationDateID IS NULL, - 1, v_PolicyExpirationDateID) AS PolicyExpirationDateID_out,
	EXP_EarnedPremiumCalculation_IN.EarnedPremiumCalculationID AS EDWEarnedPremiumMonthlyCalculationPKID,
	EXP_EarnedPremiumCalculation_IN.RatingCoverageEffectiveDate,
	-- *INF*: :LKP.LKP_CALENDER_DIM(TO_DATE(TO_CHAR(RatingCoverageEffectiveDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_TO_DATE_TO_CHAR_RatingCoverageEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS RatingCoverageEffectiveDateId,
	EXP_EarnedPremiumCalculation_IN.RatingCoverageExpirationDate,
	-- *INF*: :LKP.LKP_CALENDER_DIM(TO_DATE(TO_CHAR(RatingCoverageExpirationDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_TO_DATE_TO_CHAR_RatingCoverageExpirationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS RatingCoverageExpirationDateId,
	LKP_InsuranceReferenceDimId.InsuranceReferenceDimId,
	-- *INF*: IIF(ISNULL(InsuranceReferenceDimId), -1, InsuranceReferenceDimId)
	IFF(InsuranceReferenceDimId IS NULL, - 1, InsuranceReferenceDimId) AS o_InsuranceReferenceDimId,
	EXP_EarnedPremiumCalculation_IN.LineOfBusinessCode AS InsuranceReferenceLineOfBusinessCode,
	EXP_EarnedPremiumCalculation_IN.PolicyKey,
	EXP_EarnedPremiumCalculation_IN.SourceSystemID,
	EXP_EarnedPremiumCalculation_IN.o_InsuranceReferenceCoverageDimId_PMS AS InsuranceReferenceCoverageDimId_PMS,
	EXP_EarnedPremiumCalculation_IN.o_InsuranceReferenceCoverageDimId_DCT AS InsuranceReferenceCoverageDimId_DCT,
	-- *INF*: DECODE(SourceSystemID,'DCT',InsuranceReferenceCoverageDimId_DCT,'PMS',InsuranceReferenceCoverageDimId_PMS)
	DECODE(SourceSystemID,
	'DCT', InsuranceReferenceCoverageDimId_DCT,
	'PMS', InsuranceReferenceCoverageDimId_PMS) AS o_InsuranceReferenceCoverageDimId,
	EXP_EarnedPremiumCalculation_IN.o_CoverageDetailDimId_lkp AS CoverageDetailDimId,
	EXP_EarnedPremiumCalculation_IN.EarnedExposure,
	EXP_EarnedPremiumCalculation_IN.ChangeInEarnedExposure,
	LKP_SalesDivisionDim.SalesDivisionDimID,
	-- *INF*: IIF(ISNULL(SalesDivisionDimID), -1, SalesDivisionDimID)
	IFF(SalesDivisionDimID IS NULL, - 1, SalesDivisionDimID) AS o_SalesDivisionDimID,
	EXP_EarnedPremiumCalculation_IN.StatisticalCoverageCancellationDate AS i_StatisticalCoverageCancellationDate,
	-- *INF*: :LKP.LKP_CALENDER_DIM(TO_DATE(TO_CHAR(i_StatisticalCoverageCancellationDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_TO_DATE_TO_CHAR_i_StatisticalCoverageCancellationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_StatisticalCoverageCancellationDateId,
	-- *INF*: IIF(NOT ISNULL(v_StatisticalCoverageCancellationDateId),v_StatisticalCoverageCancellationDateId,-1)
	IFF(NOT v_StatisticalCoverageCancellationDateId IS NULL, v_StatisticalCoverageCancellationDateId, - 1) AS o_StatisticalCoverageCancellationDateId
	FROM EXP_EarnedPremiumCalculation_IN
	 -- Manually join with mplt_PolicyDimID_PremiumMaster
	LEFT JOIN LKP_InsuranceReferenceDimId
	ON LKP_InsuranceReferenceDimId.EnterpriseGroupCode = EXP_EarnedPremiumCalculation_IN.EnterpriseGroupCode AND LKP_InsuranceReferenceDimId.InsuranceReferenceLegalEntityCode = EXP_EarnedPremiumCalculation_IN.InsuranceReferenceLegalEntityCode AND LKP_InsuranceReferenceDimId.StrategicProfitCenterCode = EXP_EarnedPremiumCalculation_IN.StrategicProfitCenterCode AND LKP_InsuranceReferenceDimId.InsuranceSegmentCode = EXP_EarnedPremiumCalculation_IN.InsuranceSegmentCode AND LKP_InsuranceReferenceDimId.PolicyOfferingCode = EXP_EarnedPremiumCalculation_IN.PolicyOfferingCode AND LKP_InsuranceReferenceDimId.ProductCode = EXP_EarnedPremiumCalculation_IN.ProductCode AND LKP_InsuranceReferenceDimId.InsuranceReferenceLineOfBusinessCode = EXP_EarnedPremiumCalculation_IN.LineOfBusinessCode AND LKP_InsuranceReferenceDimId.RatingPlanCode = EXP_EarnedPremiumCalculation_IN.o_RatingPlanCode
	LEFT JOIN LKP_RiskLocationDim
	ON LKP_RiskLocationDim.RiskLocationHashKey = EXP_EarnedPremiumCalculation_IN.RiskLocationHashKey
	LEFT JOIN LKP_SalesDivisionDim
	ON LKP_SalesDivisionDim.PolicyAkId = EXP_EarnedPremiumCalculation_IN.PolicyAKID
	LEFT JOIN LKP_asl_dim
	ON LKP_asl_dim.asl_code = EXP_EarnedPremiumCalculation_IN.AnnualStatementLineCode AND LKP_asl_dim.sub_asl_code = EXP_EarnedPremiumCalculation_IN.SubAnnualStatementLineCode AND LKP_asl_dim.sub_non_asl_code = EXP_EarnedPremiumCalculation_IN.o_NonSubAnnualStatementLineCode
	LEFT JOIN LKP_asl_product_code
	ON LKP_asl_product_code.asl_prdct_code = EXP_EarnedPremiumCalculation_IN.AnnualStatementLineProductCode
	LEFT JOIN LKP_reinsurance_coverage_dim
	ON LKP_reinsurance_coverage_dim.edw_reins_cov_ak_id = EXP_EarnedPremiumCalculation_IN.ReinsuranceCoverageAKID AND LKP_reinsurance_coverage_dim.eff_from_date <= EXP_EarnedPremiumCalculation_IN.RunDate AND LKP_reinsurance_coverage_dim.eff_to_date >= EXP_EarnedPremiumCalculation_IN.RunDate
	LEFT JOIN lkp_PremiumTransactionTypeDim
	ON lkp_PremiumTransactionTypeDim.PremiumTransactionCode = EXP_EarnedPremiumCalculation_IN.PremiumTransactionCode AND lkp_PremiumTransactionTypeDim.ReasonAmendedCode = EXP_EarnedPremiumCalculation_IN.ReasonAmendedCode AND lkp_PremiumTransactionTypeDim.PremiumTypeCode = EXP_EarnedPremiumCalculation_IN.PremiumType AND lkp_PremiumTransactionTypeDim.CustomerCareCommissionRate = EXP_EarnedPremiumCalculation_IN.CustomerCareCommissionRate
	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionBookedDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionBookedDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(PremiumTransactionBookedDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionRunDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionRunDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(PremiumTransactionRunDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_PolicyCoverageEffectiveDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_PolicyCoverageEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(PolicyCoverageEffectiveDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_PolicyCoverageExpirationDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_PolicyCoverageExpirationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(PolicyCoverageExpirationDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionEnteredDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionEnteredDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(PremiumTransactionEnteredDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionEffectiveDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(PremiumTransactionEffectiveDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionExpirationDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionExpirationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(PremiumTransactionExpirationDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_PolicyEffectiveDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_PolicyEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(PolicyEffectiveDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_PolicyExpirationDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_PolicyExpirationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(PolicyExpirationDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_TO_DATE_TO_CHAR_RatingCoverageEffectiveDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_TO_DATE_TO_CHAR_RatingCoverageEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_DATE(TO_CHAR(RatingCoverageEffectiveDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_TO_DATE_TO_CHAR_RatingCoverageExpirationDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_TO_DATE_TO_CHAR_RatingCoverageExpirationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_DATE(TO_CHAR(RatingCoverageExpirationDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_TO_DATE_TO_CHAR_i_StatisticalCoverageCancellationDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_TO_DATE_TO_CHAR_i_StatisticalCoverageCancellationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_DATE(TO_CHAR(i_StatisticalCoverageCancellationDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

),
EXP_Evaluate_Fields AS (
	SELECT
	AuditID,
	EDWPremiumTransactionPKID_out AS EDWPremiumTransactionPKID,
	ASLdimID_out AS AnnualStatementLineDimID,
	ASLproductcodedimID_out AS AnnualStatementLineProductCodeDimID,
	AgencyDimID_out AS AgencyDimID,
	PolicyDimID_out AS PolicyDimID,
	pol_status_code,
	ContractCustomerDimID_out AS ContractCustomerDimID,
	RiskLocationDimID_out AS RiskLocationDimID,
	ReinsuranceCoverageDimID_out AS ReinsuranceCoverageDimID,
	PremiumTransactionTypeDimID_out AS PremiumTransactionTypeDimID,
	PolicyEffectiveDateID_out AS PolicyEffectiveDateID,
	PolicyExpirationDateID_out AS PolicyExpirationDateID,
	PolicyCoverageEffectiveDateID_out AS PolicyCoverageEffectiveDateID,
	PolicyCoverageExpirationDateID_out AS PolicyCoverageExpirationDateID,
	PremiumTransactionEnteredDateID_out AS PremiumTransactionEnteredDateID,
	PremiumTransactionEffectiveDateID_out AS PremiumTransactionEffectiveDateID,
	PremiumTransactionExpirationDateID_out AS PremiumTransactionExpirationDateID,
	PremiumTransactionBookedDateID_out AS PremiumTransactionBookedDateID,
	PremiumTransactionRunDateID_out AS PremiumTransactionRunDateID,
	DateFlagForWrittenPremium,
	PremiumType,
	PremiumTransactionAmount,
	FullTermPremium,
	-- *INF*: IIF(rtrim(PremiumType)='D' AND DateFlagForWrittenPremium='Y',PremiumTransactionAmount,0)
	IFF(rtrim(PremiumType) = 'D' AND DateFlagForWrittenPremium = 'Y', PremiumTransactionAmount, 0) AS v_TotalDirectWrittenPremium,
	v_TotalDirectWrittenPremium AS TotalDirectWrittenPremium,
	-- *INF*: IIF(rtrim(PremiumType)='C' AND DateFlagForWrittenPremium='Y',PremiumTransactionAmount,0)
	IFF(rtrim(PremiumType) = 'C' AND DateFlagForWrittenPremium = 'Y', PremiumTransactionAmount, 0) AS v_TotalCededWrittenPremium,
	v_TotalCededWrittenPremium AS TotalCededWrittenPremium,
	v_TotalDirectWrittenPremium - v_TotalCededWrittenPremium AS TotalNetWrittenPremium,
	EarnedPremium,
	-- *INF*: IIF(rtrim(PremiumType)='D',EarnedPremium,0)
	IFF(rtrim(PremiumType) = 'D', EarnedPremium, 0) AS v_DirectEarnedPremium,
	v_DirectEarnedPremium AS DirectEarnedPremium,
	-- *INF*: IIF(rtrim(PremiumType)='C',EarnedPremium,0)
	IFF(rtrim(PremiumType) = 'C', EarnedPremium, 0) AS v_CededEarnedPremium,
	v_CededEarnedPremium AS CededEarnedPremium,
	v_DirectEarnedPremium - v_CededEarnedPremium AS NetEarnedPremium,
	ChangeInEarnedPremium,
	-- *INF*: IIF(rtrim(PremiumType)='D',ChangeInEarnedPremium,0)
	IFF(rtrim(PremiumType) = 'D', ChangeInEarnedPremium, 0) AS v_ChangeinDirectEarnedPremium,
	v_ChangeinDirectEarnedPremium AS ChangeinDirectEarnedPremium,
	-- *INF*: IIF(rtrim(PremiumType)='C',ChangeInEarnedPremium,0)
	IFF(rtrim(PremiumType) = 'C', ChangeInEarnedPremium, 0) AS v_ChangeinCededEarnedPremium,
	v_ChangeinCededEarnedPremium AS ChangeinCededEarnedPremium,
	v_ChangeinDirectEarnedPremium - v_ChangeinCededEarnedPremium AS NetChangeinEarnedPremium,
	UnearnedPremium AS DirectUnearnedPremium,
	-- *INF*: IIF(rtrim(PremiumType)='D',DirectUnearnedPremium,0)
	IFF(rtrim(PremiumType) = 'D', DirectUnearnedPremium, 0) AS v_Monthly_DirectUnearnedPremium,
	v_Monthly_DirectUnearnedPremium AS Monthly_DirectUnearnedPremium,
	-- *INF*: IIF(rtrim(PremiumType)='C',DirectUnearnedPremium,0)
	-- 
	-- 
	-- --IIF(rtrim(PremiumType)='C',v_TotalCededWrittenPremium - v_CededEarnedPremium,0)
	IFF(rtrim(PremiumType) = 'C', DirectUnearnedPremium, 0) AS v_CededUnearnedPremium,
	v_CededUnearnedPremium AS CededUnearnedPremium,
	v_Monthly_DirectUnearnedPremium - v_CededUnearnedPremium AS NetUnearnedPremium,
	ChangeInUnearnedPremium AS MonthlyChangeInDirectUnearnedPremium,
	-- *INF*: IIF(rtrim(PremiumType)='D',MonthlyChangeInDirectUnearnedPremium,0)
	IFF(rtrim(PremiumType) = 'D', MonthlyChangeInDirectUnearnedPremium, 0) AS ChangeInDirectUnearnedPremium,
	-- *INF*: IIF(rtrim(PremiumType)='C',MonthlyChangeInDirectUnearnedPremium,0)
	-- 
	-- --IIF(rtrim(PremiumType)='C',v_ChangeinCededEarnedPremium*(-1),0)
	IFF(rtrim(PremiumType) = 'C', MonthlyChangeInDirectUnearnedPremium, 0) AS MonthlyChangeInCededUnearnedPremium,
	-- *INF*: IIF(pol_status_code='I' AND PremiumType='D', PremiumTransactionAmount, 0)
	IFF(pol_status_code = 'I' AND PremiumType = 'D', PremiumTransactionAmount, 0) AS v_DirectInforcePremium,
	v_DirectInforcePremium AS DirectInforcePremium,
	-- *INF*: IIF(pol_status_code='I' AND PremiumType='C', PremiumTransactionAmount, 0)
	IFF(pol_status_code = 'I' AND PremiumType = 'C', PremiumTransactionAmount, 0) AS v_CededInforcePremium,
	v_CededInforcePremium AS CededInforcePremium,
	v_DirectInforcePremium - v_CededInforcePremium AS v_NetInforcePremium,
	v_NetInforcePremium AS NetInforcePremium,
	EDWEarnedPremiumMonthlyCalculationPKID,
	RatingCoverageEffectiveDateId,
	RatingCoverageExpirationDateId,
	o_InsuranceReferenceDimId AS InsuranceReferenceDimId,
	PremiumMasterCalculationPKID AS EDWPremiumMasterCalculationPKId,
	InsuranceReferenceLineOfBusinessCode,
	PolicyKey,
	o_InsuranceReferenceCoverageDimId AS InsuranceReferenceCoverageDimId,
	CoverageDetailDimId,
	ExposureBasisDimId,
	0.00 AS EarnedExposure,
	ChangeInEarnedExposure,
	o_SalesDivisionDimID AS SalesDivisionDimID,
	o_StatisticalCoverageCancellationDateId AS StatisticalCoverageCancellationDateId
	FROM EXP_Consolidate_Data_from_Lookups
),
LKP_EarnedPremiumMonthlyFact AS (
	SELECT
	EarnedPremiumTransactionMonthlyFactID,
	EDWEarnedPremiumMonthlyCalculationPKID
	FROM (
		SELECT 
			EarnedPremiumTransactionMonthlyFactID,
			EDWEarnedPremiumMonthlyCalculationPKID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.@{pipeline().parameters.TARGET_TABLE_NAME}
		WHERE @{pipeline().parameters.TARGET_TABLE_NAME}.AUDITID=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
		@{pipeline().parameters.LOOKUP_CLAUSE}
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWEarnedPremiumMonthlyCalculationPKID ORDER BY EarnedPremiumTransactionMonthlyFactID DESC) = 1
),
RTR_INSERT_UPDATE AS (
	SELECT
	LKP_EarnedPremiumMonthlyFact.EarnedPremiumTransactionMonthlyFactID,
	EXP_Evaluate_Fields.AuditID,
	EXP_Evaluate_Fields.EDWEarnedPremiumMonthlyCalculationPKID,
	EXP_Evaluate_Fields.EDWPremiumTransactionPKID,
	EXP_Evaluate_Fields.AnnualStatementLineDimID,
	EXP_Evaluate_Fields.AnnualStatementLineProductCodeDimID,
	EXP_Evaluate_Fields.AgencyDimID,
	EXP_Evaluate_Fields.PolicyDimID,
	EXP_Evaluate_Fields.ContractCustomerDimID,
	EXP_Evaluate_Fields.RiskLocationDimID,
	EXP_Evaluate_Fields.ReinsuranceCoverageDimID,
	EXP_Evaluate_Fields.PremiumTransactionTypeDimID,
	EXP_Evaluate_Fields.PolicyEffectiveDateID,
	EXP_Evaluate_Fields.PolicyExpirationDateID,
	EXP_Evaluate_Fields.PolicyCoverageEffectiveDateID,
	EXP_Evaluate_Fields.PolicyCoverageExpirationDateID,
	EXP_Evaluate_Fields.PremiumTransactionEnteredDateID,
	EXP_Evaluate_Fields.PremiumTransactionEffectiveDateID,
	EXP_Evaluate_Fields.PremiumTransactionExpirationDateID,
	EXP_Evaluate_Fields.PremiumTransactionBookedDateID,
	EXP_Evaluate_Fields.PremiumTransactionRunDateID,
	EXP_Evaluate_Fields.TotalDirectWrittenPremium,
	EXP_Evaluate_Fields.TotalCededWrittenPremium,
	EXP_Evaluate_Fields.TotalNetWrittenPremium,
	EXP_Evaluate_Fields.DirectEarnedPremium,
	EXP_Evaluate_Fields.CededEarnedPremium,
	EXP_Evaluate_Fields.NetEarnedPremium,
	EXP_Evaluate_Fields.ChangeinDirectEarnedPremium,
	EXP_Evaluate_Fields.ChangeinCededEarnedPremium,
	EXP_Evaluate_Fields.NetChangeinEarnedPremium AS NetChangeinCededEarnedPremium,
	EXP_Evaluate_Fields.Monthly_DirectUnearnedPremium AS DirectUnearnedPremium,
	EXP_Evaluate_Fields.CededUnearnedPremium,
	EXP_Evaluate_Fields.NetUnearnedPremium,
	EXP_Evaluate_Fields.ChangeInDirectUnearnedPremium AS MonthlyChangeInDirectUnearnedPremium,
	EXP_Evaluate_Fields.MonthlyChangeInCededUnearnedPremium,
	EXP_Evaluate_Fields.DirectInforcePremium,
	EXP_Evaluate_Fields.CededInforcePremium,
	EXP_Evaluate_Fields.NetInforcePremium,
	EXP_Evaluate_Fields.RatingCoverageEffectiveDateId,
	EXP_Evaluate_Fields.RatingCoverageExpirationDateId,
	EXP_Evaluate_Fields.InsuranceReferenceDimId,
	EXP_Evaluate_Fields.EDWPremiumMasterCalculationPKId,
	EXP_Evaluate_Fields.EarnedExposure,
	EXP_Evaluate_Fields.CoverageDetailDimId,
	EXP_Evaluate_Fields.InsuranceReferenceCoverageDimId,
	EXP_Evaluate_Fields.ExposureBasisDimId,
	EXP_Evaluate_Fields.ChangeInEarnedExposure,
	EXP_Evaluate_Fields.SalesDivisionDimID,
	EXP_Evaluate_Fields.StatisticalCoverageCancellationDateId
	FROM EXP_Evaluate_Fields
	LEFT JOIN LKP_EarnedPremiumMonthlyFact
	ON LKP_EarnedPremiumMonthlyFact.EDWEarnedPremiumMonthlyCalculationPKID = EXP_Evaluate_Fields.EDWEarnedPremiumMonthlyCalculationPKID
),
RTR_INSERT_UPDATE_INSERT AS (SELECT * FROM RTR_INSERT_UPDATE WHERE isnull(EarnedPremiumTransactionMonthlyFactID)),
RTR_INSERT_UPDATE_DEFAULT1 AS (SELECT * FROM RTR_INSERT_UPDATE WHERE NOT ( (isnull(EarnedPremiumTransactionMonthlyFactID)) )),
TGT_EarnedPremiumTransactionMonthlyFact_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.@{pipeline().parameters.TARGET_TABLE_NAME}
	(AuditID, EDWEarnedPremiumMonthlyCalculationPKID, AnnualStatementLineDimID, AnnualStatementLineProductCodeDimID, AgencyDimID, PolicyDimID, ContractCustomerDimID, RiskLocationDimID, ReinsuranceCoverageDimID, PremiumTransactionTypeDimID, PolicyEffectiveDateID, PolicyExpirationDateID, PolicyCoverageEffectiveDateID, PolicyCoverageExpirationDateID, PremiumTransactionEnteredDateID, PremiumTransactionEffectiveDateID, PremiumTransactionExpirationDateID, PremiumTransactionBookedDateID, PremiumTransactionRunDateID, MonthlyTotalDirectWrittenPremium, MonthlyTotalCededWrittenPremium, MonthlyTotalNetWrittenPremium, MonthlyDirectEarnedPremium, MonthlyCededEarnedPremium, MonthlyNetEarnedPremium, MonthlyChangeinDirectEarnedPremium, MonthlyChangeinCededEarnedPremium, MonthlyNetChangeinCededEarnedPremium, MonthlyDirectUnearnedPremium, MonthlyCededUnearnedPremium, MonthlyNetUnearnedPremium, MonthlyChangeInDirectUnearnedPremium, MonthlyChangeInCededUnearnedPremium, MonthlyDirectInforcePremium, MonthlyCededInforcePremium, MonthlyNetInforcePremium, EDWPremiumMasterCalculationPKId, InsuranceReferenceDimId, EarnedExposureAmount, CoverageDetailDimId, InsuranceReferenceCoverageDimId, MonthlyChangeInEarnedExposureAmount, CoverageCancellationDateId)
	SELECT 
	AUDITID, 
	EDWEARNEDPREMIUMMONTHLYCALCULATIONPKID, 
	ANNUALSTATEMENTLINEDIMID, 
	ANNUALSTATEMENTLINEPRODUCTCODEDIMID, 
	AGENCYDIMID, 
	POLICYDIMID, 
	CONTRACTCUSTOMERDIMID, 
	RISKLOCATIONDIMID, 
	REINSURANCECOVERAGEDIMID, 
	PREMIUMTRANSACTIONTYPEDIMID, 
	POLICYEFFECTIVEDATEID, 
	POLICYEXPIRATIONDATEID, 
	POLICYCOVERAGEEFFECTIVEDATEID, 
	POLICYCOVERAGEEXPIRATIONDATEID, 
	PREMIUMTRANSACTIONENTEREDDATEID, 
	PREMIUMTRANSACTIONEFFECTIVEDATEID, 
	PREMIUMTRANSACTIONEXPIRATIONDATEID, 
	PREMIUMTRANSACTIONBOOKEDDATEID, 
	PREMIUMTRANSACTIONRUNDATEID, 
	TotalDirectWrittenPremium AS MONTHLYTOTALDIRECTWRITTENPREMIUM, 
	TotalCededWrittenPremium AS MONTHLYTOTALCEDEDWRITTENPREMIUM, 
	TotalNetWrittenPremium AS MONTHLYTOTALNETWRITTENPREMIUM, 
	DirectEarnedPremium AS MONTHLYDIRECTEARNEDPREMIUM, 
	CededEarnedPremium AS MONTHLYCEDEDEARNEDPREMIUM, 
	NetEarnedPremium AS MONTHLYNETEARNEDPREMIUM, 
	ChangeinDirectEarnedPremium AS MONTHLYCHANGEINDIRECTEARNEDPREMIUM, 
	ChangeinCededEarnedPremium AS MONTHLYCHANGEINCEDEDEARNEDPREMIUM, 
	NetChangeinCededEarnedPremium AS MONTHLYNETCHANGEINCEDEDEARNEDPREMIUM, 
	DirectUnearnedPremium AS MONTHLYDIRECTUNEARNEDPREMIUM, 
	CededUnearnedPremium AS MONTHLYCEDEDUNEARNEDPREMIUM, 
	NetUnearnedPremium AS MONTHLYNETUNEARNEDPREMIUM, 
	MONTHLYCHANGEINDIRECTUNEARNEDPREMIUM, 
	MONTHLYCHANGEINCEDEDUNEARNEDPREMIUM, 
	DirectInforcePremium AS MONTHLYDIRECTINFORCEPREMIUM, 
	CededInforcePremium AS MONTHLYCEDEDINFORCEPREMIUM, 
	NetInforcePremium AS MONTHLYNETINFORCEPREMIUM, 
	EDWPREMIUMMASTERCALCULATIONPKID, 
	INSURANCEREFERENCEDIMID, 
	EarnedExposure AS EARNEDEXPOSUREAMOUNT, 
	COVERAGEDETAILDIMID, 
	INSURANCEREFERENCECOVERAGEDIMID, 
	ChangeInEarnedExposure AS MONTHLYCHANGEINEARNEDEXPOSUREAMOUNT, 
	StatisticalCoverageCancellationDateId AS COVERAGECANCELLATIONDATEID
	FROM RTR_INSERT_UPDATE_INSERT
),
UPD_Update AS (
	SELECT
	EarnedPremiumTransactionMonthlyFactID AS EarnedPremiumTransactionMonthlyFactID1, 
	AuditID, 
	EDWEarnedPremiumMonthlyCalculationPKID AS EDWEarnedPremiumMonthlyCalculationPKID1, 
	EDWPremiumTransactionPKID, 
	AnnualStatementLineDimID, 
	AnnualStatementLineProductCodeDimID, 
	AgencyDimID, 
	PolicyDimID, 
	ContractCustomerDimID, 
	RiskLocationDimID, 
	ReinsuranceCoverageDimID, 
	PremiumTransactionTypeDimID, 
	PolicyEffectiveDateID, 
	PolicyExpirationDateID, 
	PolicyCoverageEffectiveDateID, 
	PolicyCoverageExpirationDateID, 
	PremiumTransactionEnteredDateID, 
	PremiumTransactionEffectiveDateID, 
	PremiumTransactionExpirationDateID, 
	PremiumTransactionBookedDateID, 
	PremiumTransactionRunDateID, 
	TotalDirectWrittenPremium, 
	TotalCededWrittenPremium, 
	TotalNetWrittenPremium, 
	DirectEarnedPremium, 
	CededEarnedPremium, 
	NetEarnedPremium, 
	ChangeinDirectEarnedPremium, 
	ChangeinCededEarnedPremium, 
	NetChangeinCededEarnedPremium, 
	DirectUnearnedPremium, 
	CededUnearnedPremium, 
	NetUnearnedPremium, 
	MonthlyChangeInDirectUnearnedPremium, 
	MonthlyChangeInCededUnearnedPremium, 
	DirectInforcePremium, 
	CededInforcePremium, 
	NetInforcePremium, 
	RatingCoverageEffectiveDateId, 
	RatingCoverageExpirationDateId, 
	InsuranceReferenceDimId AS InsuranceReferenceDimId2, 
	EDWPremiumMasterCalculationPKId AS EDWPremiumMasterCalculationPKId2, 
	EarnedExposure AS EarnedExposure2, 
	CoverageDetailDimId AS CoverageDetailDimId2, 
	InsuranceReferenceCoverageDimId AS InsuranceReferenceCoverageDimId2, 
	ExposureBasisDimId AS ExposureBasisDimId2, 
	ChangeInEarnedExposure AS ChangeInEarnedExposure2, 
	SalesDivisionDimID AS SalesDivisionDimID2, 
	StatisticalCoverageCancellationDateId AS StatisticalCoverageCancellationDateId2
	FROM RTR_INSERT_UPDATE_DEFAULT1
),
TGT_EarnedPremiumTransactionMonthlyFact_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.@{pipeline().parameters.TARGET_TABLE_NAME} AS T
	USING UPD_Update AS S
	ON T.EarnedPremiumTransactionMonthlyFactID = S.EarnedPremiumTransactionMonthlyFactID1
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditID = S.AuditID, T.EDWEarnedPremiumMonthlyCalculationPKID = S.EDWEarnedPremiumMonthlyCalculationPKID1, T.AnnualStatementLineDimID = S.AnnualStatementLineDimID, T.AnnualStatementLineProductCodeDimID = S.AnnualStatementLineProductCodeDimID, T.AgencyDimID = S.AgencyDimID, T.PolicyDimID = S.PolicyDimID, T.ContractCustomerDimID = S.ContractCustomerDimID, T.RiskLocationDimID = S.RiskLocationDimID, T.ReinsuranceCoverageDimID = S.ReinsuranceCoverageDimID, T.PremiumTransactionTypeDimID = S.PremiumTransactionTypeDimID, T.PolicyEffectiveDateID = S.PolicyEffectiveDateID, T.PolicyExpirationDateID = S.PolicyExpirationDateID, T.PolicyCoverageEffectiveDateID = S.PolicyCoverageEffectiveDateID, T.PolicyCoverageExpirationDateID = S.PolicyCoverageExpirationDateID, T.PremiumTransactionEnteredDateID = S.PremiumTransactionEnteredDateID, T.PremiumTransactionEffectiveDateID = S.PremiumTransactionEffectiveDateID, T.PremiumTransactionExpirationDateID = S.PremiumTransactionExpirationDateID, T.PremiumTransactionBookedDateID = S.PremiumTransactionBookedDateID, T.PremiumTransactionRunDateID = S.PremiumTransactionRunDateID, T.MonthlyTotalDirectWrittenPremium = S.TotalDirectWrittenPremium, T.MonthlyTotalCededWrittenPremium = S.TotalCededWrittenPremium, T.MonthlyTotalNetWrittenPremium = S.TotalNetWrittenPremium, T.MonthlyDirectEarnedPremium = S.DirectEarnedPremium, T.MonthlyCededEarnedPremium = S.CededEarnedPremium, T.MonthlyNetEarnedPremium = S.NetEarnedPremium, T.MonthlyChangeinDirectEarnedPremium = S.ChangeinDirectEarnedPremium, T.MonthlyChangeinCededEarnedPremium = S.ChangeinCededEarnedPremium, T.MonthlyNetChangeinCededEarnedPremium = S.NetChangeinCededEarnedPremium, T.MonthlyDirectUnearnedPremium = S.DirectUnearnedPremium, T.MonthlyCededUnearnedPremium = S.CededUnearnedPremium, T.MonthlyNetUnearnedPremium = S.NetUnearnedPremium, T.MonthlyChangeInDirectUnearnedPremium = S.MonthlyChangeInDirectUnearnedPremium, T.MonthlyChangeInCededUnearnedPremium = S.MonthlyChangeInCededUnearnedPremium, T.MonthlyDirectInforcePremium = S.DirectInforcePremium, T.MonthlyCededInforcePremium = S.CededInforcePremium, T.MonthlyNetInforcePremium = S.NetInforcePremium, T.EDWPremiumMasterCalculationPKId = S.EDWPremiumMasterCalculationPKId2, T.InsuranceReferenceDimId = S.InsuranceReferenceDimId2, T.EarnedExposureAmount = S.EarnedExposure2, T.CoverageDetailDimId = S.CoverageDetailDimId2, T.InsuranceReferenceCoverageDimId = S.InsuranceReferenceCoverageDimId2, T.MonthlyChangeInEarnedExposureAmount = S.ChangeInEarnedExposure2, T.CoverageCancellationDateId = S.StatisticalCoverageCancellationDateId2
),
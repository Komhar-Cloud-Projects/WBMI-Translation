WITH
LKP_CoverageDetailDim AS (
	SELECT
	CoverageDetailDimId,
	PremiumTransactionAKId
	FROM (
		declare @Date1 as datetime
		
		set @Date1=dateadd(dd,-@{pipeline().parameters.NO_OF_DAYS},cast(getdate() as date))
		
		select CDD.CoverageDetailDimId as CoverageDetailDimId, 
		PT.PremiumTransactionAKId as PremiumTransactionAKId
		from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		join @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
		on  PT.PremiumTransactionID=CDD.EDWPremiumTransactionPKID
		where exists (
		select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.EarnedPremiumDailyCalculation EPDC
		where EPDC.PremiumTransactionAKId=PT.PremiumTransactionAKId
		and EPDC.RunDate>=@Date1  and EPDC.RunDate<=getdate() )
		or '@{pipeline().parameters.SELECTION_START_TS}'<='01/01/1800 00:00:00'
		order by PT.PremiumTransactionAKId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKId ORDER BY CoverageDetailDimId) = 1
),
LKP_CoverageDetailDim_Hist AS (
	SELECT
	CoverageDetailDimId,
	StatisticalCoverageAKID
	FROM (
		declare @Date1 as datetime
		
		set @Date1=dateadd(dd,-@{pipeline().parameters.NO_OF_DAYS},cast(getdate() as date))
		
		select CDD.CoverageDetailDimId as CoverageDetailDimId, 
		EPDC.StatisticalCoverageAKID as StatisticalCoverageAKID
		from  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.EarnedPremiumDailyCalculation EPDC
		join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
		on EPDC.StatisticalCoverageAKId=SC.StatisticalCoverageAKId
		join @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
		on  SC.CoverageGUID=CDD.CoverageGUID and EPDC.PremiumTransactionEffectiveDate between CDD.EffectiveDate and CDD.ExpirationDate
		where EPDC.RunDate>=@Date1
		and EPDC.RunDate<=getdate()  
		and EPDC.PremiumTransactionAKID=-1
		order by EPDC.StatisticalCoverageAKID
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StatisticalCoverageAKID ORDER BY CoverageDetailDimId) = 1
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
LKP_EarnedPremiumDailyFact AS (
	SELECT
	EarnedPremiumTransactionDailyFactID,
	EDWEarnedPremiumDailyCalculationPKID
	FROM (
		SELECT 
			EarnedPremiumTransactionDailyFactID,
			EDWEarnedPremiumDailyCalculationPKID
		FROM EarnedPremiumTransactionDailyFact
		WHERE AuditID=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWEarnedPremiumDailyCalculationPKID ORDER BY EarnedPremiumTransactionDailyFactID) = 1
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
SQ_EarnedPremiumDailyCalculation AS (
	Declare @Date1 as date
	
	set @Date1=dateadd(dd,-@{pipeline().parameters.NO_OF_DAYS},cast(getdate() as date))
	
	
	SELECT EarnedPremiumDailyCalculation.EarnedPremiumDailyCalculationID, 
	EarnedPremiumDailyCalculation.SourceSystemID,
	EarnedPremiumDailyCalculation.PolicyAKID, 
	EarnedPremiumDailyCalculation.PolicyCoverageAKID,
	EarnedPremiumDailyCalculation.StatisticalCoverageAKID,
	EarnedPremiumDailyCalculation.ReinsuranceCoverageAKID,
	EarnedPremiumDailyCalculation.PremiumTransactionAKID,
	EarnedPremiumDailyCalculation.BureauStatisticalCodeAKID, 
	EarnedPremiumDailyCalculation.PolicyEffectiveDate, 
	EarnedPremiumDailyCalculation.PolicyExpirationDate, 
	EarnedPremiumDailyCalculation.StatisticalCoverageCancellationDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionEnteredDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionEffectiveDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionExpirationDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionBookedDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionCode, 
	EarnedPremiumDailyCalculation.PremiumTransactionAmount, 
	EarnedPremiumDailyCalculation.FullTermPremium, 
	EarnedPremiumDailyCalculation.PremiumType, 
	EarnedPremiumDailyCalculation.ReasonAmendedCode, 
	EarnedPremiumDailyCalculation.EarnedPremium, 
	EarnedPremiumDailyCalculation.ChangeInEarnedPremium, 
	EarnedPremiumDailyCalculation.UnearnedPremium, 
	EarnedPremiumDailyCalculation.ChangeInUnearnedPremium,
	isnull(PRODUCT.ProductCode, '000') as ProductCode, 
	EarnedPremiumDailyCalculation.AnnualStatementLineCode,
	EarnedPremiumDailyCalculation.SubAnnualStatementLineCode,
	EarnedPremiumDailyCalculation.NonSubAnnualStatementLineCode,
	EarnedPremiumDailyCalculation.AnnualStatementLineProductCode,
	Isnull(InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessCode, '000') as LineOfBusinessCode, 
	Isnull(PolicyOffering.PolicyOfferingCode, '000') as PolicyOfferingCode, 
	EarnedPremiumDailyCalculation.RunDate, 
	EarnedPremiumDailyCalculation.RatingCoverageAKId, 
	EarnedPremiumDailyCalculation.RatingCoverageEffectiveDate,
	EarnedPremiumDailyCalculation.RatingCoverageExpirationDate,
	RiskLocation.RiskLocationHashKey, 
	EnterpriseGroup.EnterpriseGroupCode, 
	InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityCode,
	StrategicProfitCenter.StrategicProfitCenterCode, 
	InsuranceSegment.InsuranceSegmentCode,
	'N/A' as RiskUnitGroup, 
	'N/A' as RiskUnit, 
	'N/A' as RiskUnitSequenceNumber, 
	'N/A' as MajorPerilCode, 
	RC.ClassCode, 
	RC.RiskType, 
	RC.CoverageType, 
	SIL.StandardInsuranceLineCode, 
	PC.TypeBureauCode,
	RC.PerilGroup,
	RC.SubCoverageTypeCode,
	RC.CoverageVersion,
	PC.CustomerCareCommissionRate,
	RPDT.RatingPlanCode,
	PC.PolicyCoverageEffectiveDate,
	PC.PolicyCoverageExpirationDate
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.EarnedPremiumDailyCalculation
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation
	ON EarnedPremiumDailyCalculation.RiskLocationAKID=RiskLocation.RiskLocationAKID
	and EarnedPremiumDailyCalculation.SourceSystemID='DCT' 
	and RiskLocation.SourceSystemID='DCT'
	AND RiskLocation.CurrentSnapshotFlag=1
	
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy
	ON EarnedPremiumDailyCalculation.PolicyAKID=policy.pol_ak_id
	and policy.source_sys_id='DCT'
	AND policy.crrnt_snpsht_flag=1
	
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	             ON EarnedPremiumDailyCalculation.RatingCoverageAKID=RC.RatingCoverageAKId 
			AND EarnedPremiumDailyCalculation.PremiumTransactionEnteredDate between RC.EffectiveDate and RC.ExpirationDate
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	             ON EarnedPremiumDailyCalculation.PolicyCoverageAKID=PC.PolicyCoverageAKID 
				 and PC.SourceSystemID='DCT' 
	             AND PC.CurrentSnapshotFlag=1
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL
	          ON SIL.ins_line_code=PC.InsuranceLine
	          AND SIL.crrnt_snpsht_flag=1
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product 
	on EarnedPremiumDailyCalculation.SourceSystemID='DCT' and RC.ProductAKID=Product.ProductAKID and Product.CurrentSnapshotFlag=1
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLineOfBusiness 
	On EarnedPremiumDailyCalculation.SourceSystemID='DCT' and RC.InsuranceReferenceLineOfBusinessAKID=InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessAKId and InsuranceReferenceLineOfBusiness.CurrentSnapshotFlag=1
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering
	On PolicyOffering.PolicyOfferingAKID=policy.PolicyOfferingAKID and PolicyOffering.CurrentSnapshotFlag=1
	
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter
	ON policy.StrategicProfitCenterAKId=StrategicProfitCenter.StrategicProfitCenterAKId
	AND StrategicProfitCenter.CurrentSnapshotFlag=1
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment
	ON InsuranceSegment.InsuranceSegmentAKId=policy.InsuranceSegmentAKId
	AND InsuranceSegment.CurrentSnapshotFlag=1
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.EnterpriseGroup
	ON EnterpriseGroup.EnterpriseGroupId=StrategicProfitCenter.EnterpriseGroupId
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLegalEntity
	ON InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityId=StrategicProfitCenter.InsuranceReferenceLegalEntityId 
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingPlan RPDT
	           ON PC.RatingPlanAKId=RPDT.RatingPlanAKId and RPDT.CurrentSnapshotFlag=1
	WHERE EarnedPremiumDailyCalculation.SourceSystemId='DCT'
	AND EarnedPremiumDailyCalculation.RunDate>=@Date1 and EarnedPremiumDailyCalculation.RunDate<=getdate()
	AND EarnedPremiumDailyCalculation.EarnedPremiumDailyCalculationID%@{pipeline().parameters.NO_OF_PARTITIONS}=0
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	
	-- PMS
	
	SELECT EarnedPremiumDailyCalculation.EarnedPremiumDailyCalculationID, 
	EarnedPremiumDailyCalculation.SourceSystemID,
	EarnedPremiumDailyCalculation.PolicyAKID, 
	EarnedPremiumDailyCalculation.PolicyCoverageAKID,
	EarnedPremiumDailyCalculation.StatisticalCoverageAKID,
	EarnedPremiumDailyCalculation.ReinsuranceCoverageAKID,
	EarnedPremiumDailyCalculation.PremiumTransactionAKID,
	EarnedPremiumDailyCalculation.BureauStatisticalCodeAKID, 
	EarnedPremiumDailyCalculation.PolicyEffectiveDate, 
	EarnedPremiumDailyCalculation.PolicyExpirationDate, 
	EarnedPremiumDailyCalculation.StatisticalCoverageCancellationDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionEnteredDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionEffectiveDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionExpirationDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionBookedDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionCode, 
	EarnedPremiumDailyCalculation.PremiumTransactionAmount, 
	EarnedPremiumDailyCalculation.FullTermPremium, 
	EarnedPremiumDailyCalculation.PremiumType, 
	EarnedPremiumDailyCalculation.ReasonAmendedCode, 
	EarnedPremiumDailyCalculation.EarnedPremium, 
	EarnedPremiumDailyCalculation.ChangeInEarnedPremium, 
	EarnedPremiumDailyCalculation.UnearnedPremium, 
	EarnedPremiumDailyCalculation.ChangeInUnearnedPremium,
	isnull(PRODUCT.ProductCode, '000') as ProductCode, 
	EarnedPremiumDailyCalculation.AnnualStatementLineCode,
	EarnedPremiumDailyCalculation.SubAnnualStatementLineCode,
	EarnedPremiumDailyCalculation.NonSubAnnualStatementLineCode,
	EarnedPremiumDailyCalculation.AnnualStatementLineProductCode,
	Isnull(InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessCode, '000') as LineOfBusinessCode, 
	Isnull(PolicyOffering.PolicyOfferingCode, '000') as PolicyOfferingCode, 
	EarnedPremiumDailyCalculation.RunDate, 
	EarnedPremiumDailyCalculation.RatingCoverageAKId, 
	EarnedPremiumDailyCalculation.RatingCoverageEffectiveDate,
	EarnedPremiumDailyCalculation.RatingCoverageExpirationDate,
	RiskLocation.RiskLocationHashKey, 
	EnterpriseGroup.EnterpriseGroupCode, 
	InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityCode,
	StrategicProfitCenter.StrategicProfitCenterCode, 
	InsuranceSegment.InsuranceSegmentCode,
	SC.RiskUnitGroup, 
	SC.RiskUnit, 
	SC.RiskUnitSequenceNumber, 
	SC.MajorPerilCode, 
	SC.ClassCode, 
	'N/A' as RiskType, 
	'N/A' as CoverageType, 
	SIL.StandardInsuranceLineCode, 
	PC.TypeBureauCode,
	'N/A' as PerilGroup,
	'N/A' as SubCoverageTypeCode,
	'N/A' as CoverageVersion,
	PC.CustomerCareCommissionRate,
	RPDT.RatingPlanCode,
	PC.PolicyCoverageEffectiveDate,
	PC.PolicyCoverageExpirationDate
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.EarnedPremiumDailyCalculation
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation
	ON EarnedPremiumDailyCalculation.RiskLocationAKID=RiskLocation.RiskLocationAKID
	and EarnedPremiumDailyCalculation.SourceSystemID='PMS' 
	and RiskLocation.SourceSystemID='PMS'
	AND RiskLocation.CurrentSnapshotFlag=1
	
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy
	ON EarnedPremiumDailyCalculation.PolicyAKID=policy.pol_ak_id
	and policy.source_sys_id='PMS'
	AND policy.crrnt_snpsht_flag=1
	
	INNER JOIN StatisticalCoverage SC
	             ON EarnedPremiumDailyCalculation.StatisticalCoverageAKID=SC.StatisticalCoverageAKID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	             ON EarnedPremiumDailyCalculation.PolicyCoverageAKID=PC.PolicyCoverageAKID 
				 and PC.SourceSystemID='PMS'
	             AND PC.CurrentSnapshotFlag=1
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL
	          ON SIL.ins_line_code=PC.InsuranceLine
	          AND SIL.crrnt_snpsht_flag=1
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product 
	on EarnedPremiumDailyCalculation.SourceSystemID='PMS' and SC.ProductAKID=Product.ProductAKID and Product.CurrentSnapshotFlag=1 
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLineOfBusiness 
	On EarnedPremiumDailyCalculation.SourceSystemID='PMS' and SC.InsuranceReferenceLineOfBusinessAKID=InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessAKId and InsuranceReferenceLineOfBusiness.CurrentSnapshotFlag=1
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering
	On PolicyOffering.PolicyOfferingAKID=policy.PolicyOfferingAKID and PolicyOffering.CurrentSnapshotFlag=1
	
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter
	ON policy.StrategicProfitCenterAKId=StrategicProfitCenter.StrategicProfitCenterAKId
	AND StrategicProfitCenter.CurrentSnapshotFlag=1
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment
	ON InsuranceSegment.InsuranceSegmentAKId=policy.InsuranceSegmentAKId
	AND InsuranceSegment.CurrentSnapshotFlag=1
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.EnterpriseGroup
	ON EnterpriseGroup.EnterpriseGroupId=StrategicProfitCenter.EnterpriseGroupId
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLegalEntity
	ON InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityId=StrategicProfitCenter.InsuranceReferenceLegalEntityId 
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingPlan RPDT
	           ON PC.RatingPlanAKId=RPDT.RatingPlanAKId and RPDT.CurrentSnapshotFlag=1
	WHERE EarnedPremiumDailyCalculation.SourceSystemId='PMS'
	AND EarnedPremiumDailyCalculation.RunDate>=@Date1 and EarnedPremiumDailyCalculation.RunDate<=getdate()
	AND EarnedPremiumDailyCalculation.EarnedPremiumDailyCalculationID%@{pipeline().parameters.NO_OF_PARTITIONS}=0
	@{pipeline().parameters.WHERE_CLAUSE}
	
	
	-- DATEADD(MS, -DATEPART(MS,EarnedPremiumDailyCalculation.RunDate), EarnedPremiumDailyCalculation.RunDate)=DATEADD(SS,-1,CAST(FLOOR(CAST--(DATEADD(dd,-@{pipeline().parameters.NO_OF_DAYS}+1,GETDATE()) as float)) as datetime))
	
	UNION ALL
	Declare @Date1 as date
	
	set @Date1=dateadd(dd,-@{pipeline().parameters.NO_OF_DAYS},cast(getdate() as date))
	
	
	SELECT EarnedPremiumDailyCalculation.EarnedPremiumDailyCalculationID, 
	EarnedPremiumDailyCalculation.SourceSystemID,
	EarnedPremiumDailyCalculation.PolicyAKID, 
	EarnedPremiumDailyCalculation.PolicyCoverageAKID,
	EarnedPremiumDailyCalculation.StatisticalCoverageAKID,
	EarnedPremiumDailyCalculation.ReinsuranceCoverageAKID,
	EarnedPremiumDailyCalculation.PremiumTransactionAKID,
	EarnedPremiumDailyCalculation.BureauStatisticalCodeAKID, 
	EarnedPremiumDailyCalculation.PolicyEffectiveDate, 
	EarnedPremiumDailyCalculation.PolicyExpirationDate, 
	EarnedPremiumDailyCalculation.StatisticalCoverageCancellationDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionEnteredDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionEffectiveDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionExpirationDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionBookedDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionCode, 
	EarnedPremiumDailyCalculation.PremiumTransactionAmount, 
	EarnedPremiumDailyCalculation.FullTermPremium, 
	EarnedPremiumDailyCalculation.PremiumType, 
	EarnedPremiumDailyCalculation.ReasonAmendedCode, 
	EarnedPremiumDailyCalculation.EarnedPremium, 
	EarnedPremiumDailyCalculation.ChangeInEarnedPremium, 
	EarnedPremiumDailyCalculation.UnearnedPremium, 
	EarnedPremiumDailyCalculation.ChangeInUnearnedPremium,
	isnull(PRODUCT.ProductCode, '000') as ProductCode, 
	EarnedPremiumDailyCalculation.AnnualStatementLineCode,
	EarnedPremiumDailyCalculation.SubAnnualStatementLineCode,
	EarnedPremiumDailyCalculation.NonSubAnnualStatementLineCode,
	EarnedPremiumDailyCalculation.AnnualStatementLineProductCode,
	Isnull(InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessCode, '000') as LineOfBusinessCode, 
	Isnull(PolicyOffering.PolicyOfferingCode, '000') as PolicyOfferingCode, 
	EarnedPremiumDailyCalculation.RunDate, 
	EarnedPremiumDailyCalculation.RatingCoverageAKId, 
	EarnedPremiumDailyCalculation.RatingCoverageEffectiveDate,
	EarnedPremiumDailyCalculation.RatingCoverageExpirationDate,
	RiskLocation.RiskLocationHashKey, 
	EnterpriseGroup.EnterpriseGroupCode, 
	InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityCode,
	StrategicProfitCenter.StrategicProfitCenterCode, 
	InsuranceSegment.InsuranceSegmentCode,
	'N/A' as RiskUnitGroup, 
	'N/A' as RiskUnit, 
	'N/A' as RiskUnitSequenceNumber, 
	'N/A' as MajorPerilCode, 
	RC.ClassCode, 
	RC.RiskType, 
	RC.CoverageType, 
	SIL.StandardInsuranceLineCode, 
	PC.TypeBureauCode,
	RC.PerilGroup,
	RC.SubCoverageTypeCode,
	RC.CoverageVersion,
	PC.CustomerCareCommissionRate,
	RPDT.RatingPlanCode,
	PC.PolicyCoverageEffectiveDate,
	PC.PolicyCoverageExpirationDate
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.EarnedPremiumDailyCalculation
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation
	ON EarnedPremiumDailyCalculation.RiskLocationAKID=RiskLocation.RiskLocationAKID
	and EarnedPremiumDailyCalculation.SourceSystemID='DCT' 
	and RiskLocation.SourceSystemID='DCT'
	AND RiskLocation.CurrentSnapshotFlag=1
	
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy
	ON EarnedPremiumDailyCalculation.PolicyAKID=policy.pol_ak_id
	and policy.source_sys_id='DCT'
	AND policy.crrnt_snpsht_flag=1
	
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	             ON EarnedPremiumDailyCalculation.RatingCoverageAKID=RC.RatingCoverageAKId 
			AND EarnedPremiumDailyCalculation.PremiumTransactionEnteredDate between RC.EffectiveDate and RC.ExpirationDate
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	             ON EarnedPremiumDailyCalculation.PolicyCoverageAKID=PC.PolicyCoverageAKID 
				 and PC.SourceSystemID='DCT' 
	             AND PC.CurrentSnapshotFlag=1
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL
	          ON SIL.ins_line_code=PC.InsuranceLine
	          AND SIL.crrnt_snpsht_flag=1
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product 
	on EarnedPremiumDailyCalculation.SourceSystemID='DCT' and RC.ProductAKID=Product.ProductAKID and Product.CurrentSnapshotFlag=1
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLineOfBusiness 
	On EarnedPremiumDailyCalculation.SourceSystemID='DCT' and RC.InsuranceReferenceLineOfBusinessAKID=InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessAKId and InsuranceReferenceLineOfBusiness.CurrentSnapshotFlag=1
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering
	On PolicyOffering.PolicyOfferingAKID=policy.PolicyOfferingAKID and PolicyOffering.CurrentSnapshotFlag=1
	
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter
	ON policy.StrategicProfitCenterAKId=StrategicProfitCenter.StrategicProfitCenterAKId
	AND StrategicProfitCenter.CurrentSnapshotFlag=1
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment
	ON InsuranceSegment.InsuranceSegmentAKId=policy.InsuranceSegmentAKId
	AND InsuranceSegment.CurrentSnapshotFlag=1
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.EnterpriseGroup
	ON EnterpriseGroup.EnterpriseGroupId=StrategicProfitCenter.EnterpriseGroupId
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLegalEntity
	ON InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityId=StrategicProfitCenter.InsuranceReferenceLegalEntityId 
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingPlan RPDT
	           ON PC.RatingPlanAKId=RPDT.RatingPlanAKId and RPDT.CurrentSnapshotFlag=1
	WHERE EarnedPremiumDailyCalculation.SourceSystemId='DCT'
	AND EarnedPremiumDailyCalculation.RunDate>=@Date1 and EarnedPremiumDailyCalculation.RunDate<=getdate()
	AND EarnedPremiumDailyCalculation.EarnedPremiumDailyCalculationID%@{pipeline().parameters.NO_OF_PARTITIONS}=1
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	
	-- PMS
	
	SELECT EarnedPremiumDailyCalculation.EarnedPremiumDailyCalculationID, 
	EarnedPremiumDailyCalculation.SourceSystemID,
	EarnedPremiumDailyCalculation.PolicyAKID, 
	EarnedPremiumDailyCalculation.PolicyCoverageAKID,
	EarnedPremiumDailyCalculation.StatisticalCoverageAKID,
	EarnedPremiumDailyCalculation.ReinsuranceCoverageAKID,
	EarnedPremiumDailyCalculation.PremiumTransactionAKID,
	EarnedPremiumDailyCalculation.BureauStatisticalCodeAKID, 
	EarnedPremiumDailyCalculation.PolicyEffectiveDate, 
	EarnedPremiumDailyCalculation.PolicyExpirationDate, 
	EarnedPremiumDailyCalculation.StatisticalCoverageCancellationDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionEnteredDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionEffectiveDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionExpirationDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionBookedDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionCode, 
	EarnedPremiumDailyCalculation.PremiumTransactionAmount, 
	EarnedPremiumDailyCalculation.FullTermPremium, 
	EarnedPremiumDailyCalculation.PremiumType, 
	EarnedPremiumDailyCalculation.ReasonAmendedCode, 
	EarnedPremiumDailyCalculation.EarnedPremium, 
	EarnedPremiumDailyCalculation.ChangeInEarnedPremium, 
	EarnedPremiumDailyCalculation.UnearnedPremium, 
	EarnedPremiumDailyCalculation.ChangeInUnearnedPremium,
	isnull(PRODUCT.ProductCode, '000') as ProductCode, 
	EarnedPremiumDailyCalculation.AnnualStatementLineCode,
	EarnedPremiumDailyCalculation.SubAnnualStatementLineCode,
	EarnedPremiumDailyCalculation.NonSubAnnualStatementLineCode,
	EarnedPremiumDailyCalculation.AnnualStatementLineProductCode,
	Isnull(InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessCode, '000') as LineOfBusinessCode, 
	Isnull(PolicyOffering.PolicyOfferingCode, '000') as PolicyOfferingCode, 
	EarnedPremiumDailyCalculation.RunDate, 
	EarnedPremiumDailyCalculation.RatingCoverageAKId, 
	EarnedPremiumDailyCalculation.RatingCoverageEffectiveDate,
	EarnedPremiumDailyCalculation.RatingCoverageExpirationDate,
	RiskLocation.RiskLocationHashKey, 
	EnterpriseGroup.EnterpriseGroupCode, 
	InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityCode,
	StrategicProfitCenter.StrategicProfitCenterCode, 
	InsuranceSegment.InsuranceSegmentCode,
	SC.RiskUnitGroup, 
	SC.RiskUnit, 
	SC.RiskUnitSequenceNumber, 
	SC.MajorPerilCode, 
	SC.ClassCode, 
	'N/A' as RiskType, 
	'N/A' as CoverageType, 
	SIL.StandardInsuranceLineCode, 
	PC.TypeBureauCode,
	'N/A' as PerilGroup,
	'N/A' as SubCoverageTypeCode,
	'N/A' as CoverageVersion,
	PC.CustomerCareCommissionRate,
	RPDT.RatingPlanCode,
	PC.PolicyCoverageEffectiveDate,
	PC.PolicyCoverageExpirationDate
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.EarnedPremiumDailyCalculation
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation
	ON EarnedPremiumDailyCalculation.RiskLocationAKID=RiskLocation.RiskLocationAKID
	and EarnedPremiumDailyCalculation.SourceSystemID='PMS' 
	and RiskLocation.SourceSystemID='PMS'
	AND RiskLocation.CurrentSnapshotFlag=1
	
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy
	ON EarnedPremiumDailyCalculation.PolicyAKID=policy.pol_ak_id
	and policy.source_sys_id='PMS'
	AND policy.crrnt_snpsht_flag=1
	
	INNER JOIN StatisticalCoverage SC
	             ON EarnedPremiumDailyCalculation.StatisticalCoverageAKID=SC.StatisticalCoverageAKID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	             ON EarnedPremiumDailyCalculation.PolicyCoverageAKID=PC.PolicyCoverageAKID 
				 and PC.SourceSystemID='PMS'
	             AND PC.CurrentSnapshotFlag=1
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL
	          ON SIL.ins_line_code=PC.InsuranceLine
	          AND SIL.crrnt_snpsht_flag=1
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product 
	on EarnedPremiumDailyCalculation.SourceSystemID='PMS' and SC.ProductAKID=Product.ProductAKID and Product.CurrentSnapshotFlag=1 
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLineOfBusiness 
	On EarnedPremiumDailyCalculation.SourceSystemID='PMS' and SC.InsuranceReferenceLineOfBusinessAKID=InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessAKId and InsuranceReferenceLineOfBusiness.CurrentSnapshotFlag=1
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering
	On PolicyOffering.PolicyOfferingAKID=policy.PolicyOfferingAKID and PolicyOffering.CurrentSnapshotFlag=1
	
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter
	ON policy.StrategicProfitCenterAKId=StrategicProfitCenter.StrategicProfitCenterAKId
	AND StrategicProfitCenter.CurrentSnapshotFlag=1
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment
	ON InsuranceSegment.InsuranceSegmentAKId=policy.InsuranceSegmentAKId
	AND InsuranceSegment.CurrentSnapshotFlag=1
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.EnterpriseGroup
	ON EnterpriseGroup.EnterpriseGroupId=StrategicProfitCenter.EnterpriseGroupId
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLegalEntity
	ON InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityId=StrategicProfitCenter.InsuranceReferenceLegalEntityId 
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingPlan RPDT
	           ON PC.RatingPlanAKId=RPDT.RatingPlanAKId and RPDT.CurrentSnapshotFlag=1
	WHERE EarnedPremiumDailyCalculation.SourceSystemId='PMS'
	AND EarnedPremiumDailyCalculation.RunDate>=@Date1 and EarnedPremiumDailyCalculation.RunDate<=getdate()
	AND EarnedPremiumDailyCalculation.EarnedPremiumDailyCalculationID%@{pipeline().parameters.NO_OF_PARTITIONS}=1
	@{pipeline().parameters.WHERE_CLAUSE}
	
	
	-- DATEADD(MS, -DATEPART(MS,EarnedPremiumDailyCalculation.RunDate), EarnedPremiumDailyCalculation.RunDate)=DATEADD(SS,-1,CAST(FLOOR(CAST--(DATEADD(dd,-@{pipeline().parameters.NO_OF_DAYS}+1,GETDATE()) as float)) as datetime))
	
	UNION ALL
	Declare @Date1 as date
	
	set @Date1=dateadd(dd,-@{pipeline().parameters.NO_OF_DAYS},cast(getdate() as date))
	
	
	SELECT EarnedPremiumDailyCalculation.EarnedPremiumDailyCalculationID, 
	EarnedPremiumDailyCalculation.SourceSystemID,
	EarnedPremiumDailyCalculation.PolicyAKID, 
	EarnedPremiumDailyCalculation.PolicyCoverageAKID,
	EarnedPremiumDailyCalculation.StatisticalCoverageAKID,
	EarnedPremiumDailyCalculation.ReinsuranceCoverageAKID,
	EarnedPremiumDailyCalculation.PremiumTransactionAKID,
	EarnedPremiumDailyCalculation.BureauStatisticalCodeAKID, 
	EarnedPremiumDailyCalculation.PolicyEffectiveDate, 
	EarnedPremiumDailyCalculation.PolicyExpirationDate, 
	EarnedPremiumDailyCalculation.StatisticalCoverageCancellationDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionEnteredDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionEffectiveDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionExpirationDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionBookedDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionCode, 
	EarnedPremiumDailyCalculation.PremiumTransactionAmount, 
	EarnedPremiumDailyCalculation.FullTermPremium, 
	EarnedPremiumDailyCalculation.PremiumType, 
	EarnedPremiumDailyCalculation.ReasonAmendedCode, 
	EarnedPremiumDailyCalculation.EarnedPremium, 
	EarnedPremiumDailyCalculation.ChangeInEarnedPremium, 
	EarnedPremiumDailyCalculation.UnearnedPremium, 
	EarnedPremiumDailyCalculation.ChangeInUnearnedPremium,
	isnull(PRODUCT.ProductCode, '000') as ProductCode, 
	EarnedPremiumDailyCalculation.AnnualStatementLineCode,
	EarnedPremiumDailyCalculation.SubAnnualStatementLineCode,
	EarnedPremiumDailyCalculation.NonSubAnnualStatementLineCode,
	EarnedPremiumDailyCalculation.AnnualStatementLineProductCode,
	Isnull(InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessCode, '000') as LineOfBusinessCode, 
	Isnull(PolicyOffering.PolicyOfferingCode, '000') as PolicyOfferingCode, 
	EarnedPremiumDailyCalculation.RunDate, 
	EarnedPremiumDailyCalculation.RatingCoverageAKId, 
	EarnedPremiumDailyCalculation.RatingCoverageEffectiveDate,
	EarnedPremiumDailyCalculation.RatingCoverageExpirationDate,
	RiskLocation.RiskLocationHashKey, 
	EnterpriseGroup.EnterpriseGroupCode, 
	InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityCode,
	StrategicProfitCenter.StrategicProfitCenterCode, 
	InsuranceSegment.InsuranceSegmentCode,
	'N/A' as RiskUnitGroup, 
	'N/A' as RiskUnit, 
	'N/A' as RiskUnitSequenceNumber, 
	'N/A' as MajorPerilCode, 
	RC.ClassCode, 
	RC.RiskType, 
	RC.CoverageType, 
	SIL.StandardInsuranceLineCode, 
	PC.TypeBureauCode,
	RC.PerilGroup,
	RC.SubCoverageTypeCode,
	RC.CoverageVersion,
	PC.CustomerCareCommissionRate,
	RPDT.RatingPlanCode,
	PC.PolicyCoverageEffectiveDate,
	PC.PolicyCoverageExpirationDate
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.EarnedPremiumDailyCalculation
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation
	ON EarnedPremiumDailyCalculation.RiskLocationAKID=RiskLocation.RiskLocationAKID
	and EarnedPremiumDailyCalculation.SourceSystemID='DCT' 
	and RiskLocation.SourceSystemID='DCT'
	AND RiskLocation.CurrentSnapshotFlag=1
	
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy
	ON EarnedPremiumDailyCalculation.PolicyAKID=policy.pol_ak_id
	and policy.source_sys_id='DCT'
	AND policy.crrnt_snpsht_flag=1
	
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	             ON EarnedPremiumDailyCalculation.RatingCoverageAKID=RC.RatingCoverageAKId 
			AND EarnedPremiumDailyCalculation.PremiumTransactionEnteredDate between RC.EffectiveDate and RC.ExpirationDate
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	             ON EarnedPremiumDailyCalculation.PolicyCoverageAKID=PC.PolicyCoverageAKID 
				 and PC.SourceSystemID='DCT' 
	             AND PC.CurrentSnapshotFlag=1
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL
	          ON SIL.ins_line_code=PC.InsuranceLine
	          AND SIL.crrnt_snpsht_flag=1
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product 
	on EarnedPremiumDailyCalculation.SourceSystemID='DCT' and RC.ProductAKID=Product.ProductAKID and Product.CurrentSnapshotFlag=1
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLineOfBusiness 
	On EarnedPremiumDailyCalculation.SourceSystemID='DCT' and RC.InsuranceReferenceLineOfBusinessAKID=InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessAKId and InsuranceReferenceLineOfBusiness.CurrentSnapshotFlag=1
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering
	On PolicyOffering.PolicyOfferingAKID=policy.PolicyOfferingAKID and PolicyOffering.CurrentSnapshotFlag=1
	
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter
	ON policy.StrategicProfitCenterAKId=StrategicProfitCenter.StrategicProfitCenterAKId
	AND StrategicProfitCenter.CurrentSnapshotFlag=1
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment
	ON InsuranceSegment.InsuranceSegmentAKId=policy.InsuranceSegmentAKId
	AND InsuranceSegment.CurrentSnapshotFlag=1
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.EnterpriseGroup
	ON EnterpriseGroup.EnterpriseGroupId=StrategicProfitCenter.EnterpriseGroupId
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLegalEntity
	ON InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityId=StrategicProfitCenter.InsuranceReferenceLegalEntityId 
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingPlan RPDT
	           ON PC.RatingPlanAKId=RPDT.RatingPlanAKId and RPDT.CurrentSnapshotFlag=1
	WHERE EarnedPremiumDailyCalculation.SourceSystemId='DCT'
	AND EarnedPremiumDailyCalculation.RunDate>=@Date1 and EarnedPremiumDailyCalculation.RunDate<=getdate()
	AND EarnedPremiumDailyCalculation.EarnedPremiumDailyCalculationID%@{pipeline().parameters.NO_OF_PARTITIONS}=2
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	
	-- PMS
	
	SELECT EarnedPremiumDailyCalculation.EarnedPremiumDailyCalculationID, 
	EarnedPremiumDailyCalculation.SourceSystemID,
	EarnedPremiumDailyCalculation.PolicyAKID, 
	EarnedPremiumDailyCalculation.PolicyCoverageAKID,
	EarnedPremiumDailyCalculation.StatisticalCoverageAKID,
	EarnedPremiumDailyCalculation.ReinsuranceCoverageAKID,
	EarnedPremiumDailyCalculation.PremiumTransactionAKID,
	EarnedPremiumDailyCalculation.BureauStatisticalCodeAKID, 
	EarnedPremiumDailyCalculation.PolicyEffectiveDate, 
	EarnedPremiumDailyCalculation.PolicyExpirationDate, 
	EarnedPremiumDailyCalculation.StatisticalCoverageCancellationDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionEnteredDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionEffectiveDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionExpirationDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionBookedDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionCode, 
	EarnedPremiumDailyCalculation.PremiumTransactionAmount, 
	EarnedPremiumDailyCalculation.FullTermPremium, 
	EarnedPremiumDailyCalculation.PremiumType, 
	EarnedPremiumDailyCalculation.ReasonAmendedCode, 
	EarnedPremiumDailyCalculation.EarnedPremium, 
	EarnedPremiumDailyCalculation.ChangeInEarnedPremium, 
	EarnedPremiumDailyCalculation.UnearnedPremium, 
	EarnedPremiumDailyCalculation.ChangeInUnearnedPremium,
	isnull(PRODUCT.ProductCode, '000') as ProductCode, 
	EarnedPremiumDailyCalculation.AnnualStatementLineCode,
	EarnedPremiumDailyCalculation.SubAnnualStatementLineCode,
	EarnedPremiumDailyCalculation.NonSubAnnualStatementLineCode,
	EarnedPremiumDailyCalculation.AnnualStatementLineProductCode,
	Isnull(InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessCode, '000') as LineOfBusinessCode, 
	Isnull(PolicyOffering.PolicyOfferingCode, '000') as PolicyOfferingCode, 
	EarnedPremiumDailyCalculation.RunDate, 
	EarnedPremiumDailyCalculation.RatingCoverageAKId, 
	EarnedPremiumDailyCalculation.RatingCoverageEffectiveDate,
	EarnedPremiumDailyCalculation.RatingCoverageExpirationDate,
	RiskLocation.RiskLocationHashKey, 
	EnterpriseGroup.EnterpriseGroupCode, 
	InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityCode,
	StrategicProfitCenter.StrategicProfitCenterCode, 
	InsuranceSegment.InsuranceSegmentCode,
	SC.RiskUnitGroup, 
	SC.RiskUnit, 
	SC.RiskUnitSequenceNumber, 
	SC.MajorPerilCode, 
	SC.ClassCode, 
	'N/A' as RiskType, 
	'N/A' as CoverageType, 
	SIL.StandardInsuranceLineCode, 
	PC.TypeBureauCode,
	'N/A' as PerilGroup,
	'N/A' as SubCoverageTypeCode,
	'N/A' as CoverageVersion,
	PC.CustomerCareCommissionRate,
	RPDT.RatingPlanCode,
	PC.PolicyCoverageEffectiveDate,
	PC.PolicyCoverageExpirationDate
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.EarnedPremiumDailyCalculation
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation
	ON EarnedPremiumDailyCalculation.RiskLocationAKID=RiskLocation.RiskLocationAKID
	and EarnedPremiumDailyCalculation.SourceSystemID='PMS' 
	and RiskLocation.SourceSystemID='PMS'
	AND RiskLocation.CurrentSnapshotFlag=1
	
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy
	ON EarnedPremiumDailyCalculation.PolicyAKID=policy.pol_ak_id
	and policy.source_sys_id='PMS'
	AND policy.crrnt_snpsht_flag=1
	
	INNER JOIN StatisticalCoverage SC
	             ON EarnedPremiumDailyCalculation.StatisticalCoverageAKID=SC.StatisticalCoverageAKID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	             ON EarnedPremiumDailyCalculation.PolicyCoverageAKID=PC.PolicyCoverageAKID 
				 and PC.SourceSystemID='PMS'
	             AND PC.CurrentSnapshotFlag=1
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL
	          ON SIL.ins_line_code=PC.InsuranceLine
	          AND SIL.crrnt_snpsht_flag=1
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product 
	on EarnedPremiumDailyCalculation.SourceSystemID='PMS' and SC.ProductAKID=Product.ProductAKID and Product.CurrentSnapshotFlag=1 
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLineOfBusiness 
	On EarnedPremiumDailyCalculation.SourceSystemID='PMS' and SC.InsuranceReferenceLineOfBusinessAKID=InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessAKId and InsuranceReferenceLineOfBusiness.CurrentSnapshotFlag=1
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering
	On PolicyOffering.PolicyOfferingAKID=policy.PolicyOfferingAKID and PolicyOffering.CurrentSnapshotFlag=1
	
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter
	ON policy.StrategicProfitCenterAKId=StrategicProfitCenter.StrategicProfitCenterAKId
	AND StrategicProfitCenter.CurrentSnapshotFlag=1
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment
	ON InsuranceSegment.InsuranceSegmentAKId=policy.InsuranceSegmentAKId
	AND InsuranceSegment.CurrentSnapshotFlag=1
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.EnterpriseGroup
	ON EnterpriseGroup.EnterpriseGroupId=StrategicProfitCenter.EnterpriseGroupId
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLegalEntity
	ON InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityId=StrategicProfitCenter.InsuranceReferenceLegalEntityId 
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingPlan RPDT
	           ON PC.RatingPlanAKId=RPDT.RatingPlanAKId and RPDT.CurrentSnapshotFlag=1
	WHERE EarnedPremiumDailyCalculation.SourceSystemId='PMS'
	AND EarnedPremiumDailyCalculation.RunDate>=@Date1 and EarnedPremiumDailyCalculation.RunDate<=getdate()
	AND EarnedPremiumDailyCalculation.EarnedPremiumDailyCalculationID%@{pipeline().parameters.NO_OF_PARTITIONS}=2
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	Declare @Date1 as date
	
	set @Date1=dateadd(dd,-@{pipeline().parameters.NO_OF_DAYS},cast(getdate() as date))
	
	
	SELECT EarnedPremiumDailyCalculation.EarnedPremiumDailyCalculationID, 
	EarnedPremiumDailyCalculation.SourceSystemID,
	EarnedPremiumDailyCalculation.PolicyAKID, 
	EarnedPremiumDailyCalculation.PolicyCoverageAKID,
	EarnedPremiumDailyCalculation.StatisticalCoverageAKID,
	EarnedPremiumDailyCalculation.ReinsuranceCoverageAKID,
	EarnedPremiumDailyCalculation.PremiumTransactionAKID,
	EarnedPremiumDailyCalculation.BureauStatisticalCodeAKID, 
	EarnedPremiumDailyCalculation.PolicyEffectiveDate, 
	EarnedPremiumDailyCalculation.PolicyExpirationDate, 
	EarnedPremiumDailyCalculation.StatisticalCoverageCancellationDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionEnteredDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionEffectiveDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionExpirationDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionBookedDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionCode, 
	EarnedPremiumDailyCalculation.PremiumTransactionAmount, 
	EarnedPremiumDailyCalculation.FullTermPremium, 
	EarnedPremiumDailyCalculation.PremiumType, 
	EarnedPremiumDailyCalculation.ReasonAmendedCode, 
	EarnedPremiumDailyCalculation.EarnedPremium, 
	EarnedPremiumDailyCalculation.ChangeInEarnedPremium, 
	EarnedPremiumDailyCalculation.UnearnedPremium, 
	EarnedPremiumDailyCalculation.ChangeInUnearnedPremium,
	isnull(PRODUCT.ProductCode, '000') as ProductCode, 
	EarnedPremiumDailyCalculation.AnnualStatementLineCode,
	EarnedPremiumDailyCalculation.SubAnnualStatementLineCode,
	EarnedPremiumDailyCalculation.NonSubAnnualStatementLineCode,
	EarnedPremiumDailyCalculation.AnnualStatementLineProductCode,
	Isnull(InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessCode, '000') as LineOfBusinessCode, 
	Isnull(PolicyOffering.PolicyOfferingCode, '000') as PolicyOfferingCode, 
	EarnedPremiumDailyCalculation.RunDate, 
	EarnedPremiumDailyCalculation.RatingCoverageAKId, 
	EarnedPremiumDailyCalculation.RatingCoverageEffectiveDate,
	EarnedPremiumDailyCalculation.RatingCoverageExpirationDate,
	RiskLocation.RiskLocationHashKey, 
	EnterpriseGroup.EnterpriseGroupCode, 
	InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityCode,
	StrategicProfitCenter.StrategicProfitCenterCode, 
	InsuranceSegment.InsuranceSegmentCode,
	'N/A' as RiskUnitGroup, 
	'N/A' as RiskUnit, 
	'N/A' as RiskUnitSequenceNumber, 
	'N/A' as MajorPerilCode, 
	RC.ClassCode, 
	RC.RiskType, 
	RC.CoverageType, 
	SIL.StandardInsuranceLineCode, 
	PC.TypeBureauCode,
	RC.PerilGroup,
	RC.SubCoverageTypeCode,
	RC.CoverageVersion,
	PC.CustomerCareCommissionRate,
	RPDT.RatingPlanCode,
	PC.PolicyCoverageEffectiveDate,
	PC.PolicyCoverageExpirationDate
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.EarnedPremiumDailyCalculation
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation
	ON EarnedPremiumDailyCalculation.RiskLocationAKID=RiskLocation.RiskLocationAKID
	and EarnedPremiumDailyCalculation.SourceSystemID='DCT' 
	and RiskLocation.SourceSystemID='DCT'
	AND RiskLocation.CurrentSnapshotFlag=1
	
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy
	ON EarnedPremiumDailyCalculation.PolicyAKID=policy.pol_ak_id
	and policy.source_sys_id='DCT'
	AND policy.crrnt_snpsht_flag=1
	
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	             ON EarnedPremiumDailyCalculation.RatingCoverageAKID=RC.RatingCoverageAKId 
			AND EarnedPremiumDailyCalculation.PremiumTransactionEnteredDate between RC.EffectiveDate and RC.ExpirationDate
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	             ON EarnedPremiumDailyCalculation.PolicyCoverageAKID=PC.PolicyCoverageAKID 
				 and PC.SourceSystemID='DCT' 
	             AND PC.CurrentSnapshotFlag=1
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL
	          ON SIL.ins_line_code=PC.InsuranceLine
	          AND SIL.crrnt_snpsht_flag=1
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product 
	on EarnedPremiumDailyCalculation.SourceSystemID='DCT' and RC.ProductAKID=Product.ProductAKID and Product.CurrentSnapshotFlag=1
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLineOfBusiness 
	On EarnedPremiumDailyCalculation.SourceSystemID='DCT' and RC.InsuranceReferenceLineOfBusinessAKID=InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessAKId and InsuranceReferenceLineOfBusiness.CurrentSnapshotFlag=1
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering
	On PolicyOffering.PolicyOfferingAKID=policy.PolicyOfferingAKID and PolicyOffering.CurrentSnapshotFlag=1
	
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter
	ON policy.StrategicProfitCenterAKId=StrategicProfitCenter.StrategicProfitCenterAKId
	AND StrategicProfitCenter.CurrentSnapshotFlag=1
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment
	ON InsuranceSegment.InsuranceSegmentAKId=policy.InsuranceSegmentAKId
	AND InsuranceSegment.CurrentSnapshotFlag=1
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.EnterpriseGroup
	ON EnterpriseGroup.EnterpriseGroupId=StrategicProfitCenter.EnterpriseGroupId
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLegalEntity
	ON InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityId=StrategicProfitCenter.InsuranceReferenceLegalEntityId 
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingPlan RPDT
	           ON PC.RatingPlanAKId=RPDT.RatingPlanAKId and RPDT.CurrentSnapshotFlag=1
	WHERE EarnedPremiumDailyCalculation.SourceSystemId='DCT'
	AND EarnedPremiumDailyCalculation.RunDate>=@Date1 and EarnedPremiumDailyCalculation.RunDate<=getdate()
	AND EarnedPremiumDailyCalculation.EarnedPremiumDailyCalculationID%@{pipeline().parameters.NO_OF_PARTITIONS}=3
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	
	-- PMS
	
	SELECT EarnedPremiumDailyCalculation.EarnedPremiumDailyCalculationID, 
	EarnedPremiumDailyCalculation.SourceSystemID,
	EarnedPremiumDailyCalculation.PolicyAKID, 
	EarnedPremiumDailyCalculation.PolicyCoverageAKID,
	EarnedPremiumDailyCalculation.StatisticalCoverageAKID,
	EarnedPremiumDailyCalculation.ReinsuranceCoverageAKID,
	EarnedPremiumDailyCalculation.PremiumTransactionAKID,
	EarnedPremiumDailyCalculation.BureauStatisticalCodeAKID, 
	EarnedPremiumDailyCalculation.PolicyEffectiveDate, 
	EarnedPremiumDailyCalculation.PolicyExpirationDate, 
	EarnedPremiumDailyCalculation.StatisticalCoverageCancellationDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionEnteredDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionEffectiveDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionExpirationDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionBookedDate, 
	EarnedPremiumDailyCalculation.PremiumTransactionCode, 
	EarnedPremiumDailyCalculation.PremiumTransactionAmount, 
	EarnedPremiumDailyCalculation.FullTermPremium, 
	EarnedPremiumDailyCalculation.PremiumType, 
	EarnedPremiumDailyCalculation.ReasonAmendedCode, 
	EarnedPremiumDailyCalculation.EarnedPremium, 
	EarnedPremiumDailyCalculation.ChangeInEarnedPremium, 
	EarnedPremiumDailyCalculation.UnearnedPremium, 
	EarnedPremiumDailyCalculation.ChangeInUnearnedPremium,
	isnull(PRODUCT.ProductCode, '000') as ProductCode, 
	EarnedPremiumDailyCalculation.AnnualStatementLineCode,
	EarnedPremiumDailyCalculation.SubAnnualStatementLineCode,
	EarnedPremiumDailyCalculation.NonSubAnnualStatementLineCode,
	EarnedPremiumDailyCalculation.AnnualStatementLineProductCode,
	Isnull(InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessCode, '000') as LineOfBusinessCode, 
	Isnull(PolicyOffering.PolicyOfferingCode, '000') as PolicyOfferingCode, 
	EarnedPremiumDailyCalculation.RunDate, 
	EarnedPremiumDailyCalculation.RatingCoverageAKId, 
	EarnedPremiumDailyCalculation.RatingCoverageEffectiveDate,
	EarnedPremiumDailyCalculation.RatingCoverageExpirationDate,
	RiskLocation.RiskLocationHashKey, 
	EnterpriseGroup.EnterpriseGroupCode, 
	InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityCode,
	StrategicProfitCenter.StrategicProfitCenterCode, 
	InsuranceSegment.InsuranceSegmentCode,
	SC.RiskUnitGroup, 
	SC.RiskUnit, 
	SC.RiskUnitSequenceNumber, 
	SC.MajorPerilCode, 
	SC.ClassCode, 
	'N/A' as RiskType, 
	'N/A' as CoverageType, 
	SIL.StandardInsuranceLineCode, 
	PC.TypeBureauCode,
	'N/A' as PerilGroup,
	'N/A' as SubCoverageTypeCode,
	'N/A' as CoverageVersion,
	PC.CustomerCareCommissionRate,
	RPDT.RatingPlanCode,
	PC.PolicyCoverageEffectiveDate,
	PC.PolicyCoverageExpirationDate
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.EarnedPremiumDailyCalculation
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation
	ON EarnedPremiumDailyCalculation.RiskLocationAKID=RiskLocation.RiskLocationAKID
	and EarnedPremiumDailyCalculation.SourceSystemID='PMS' 
	and RiskLocation.SourceSystemID='PMS'
	AND RiskLocation.CurrentSnapshotFlag=1
	
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy
	ON EarnedPremiumDailyCalculation.PolicyAKID=policy.pol_ak_id
	and policy.source_sys_id='PMS'
	AND policy.crrnt_snpsht_flag=1
	
	INNER JOIN StatisticalCoverage SC
	             ON EarnedPremiumDailyCalculation.StatisticalCoverageAKID=SC.StatisticalCoverageAKID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	             ON EarnedPremiumDailyCalculation.PolicyCoverageAKID=PC.PolicyCoverageAKID 
				 and PC.SourceSystemID='PMS'
	             AND PC.CurrentSnapshotFlag=1
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL
	          ON SIL.ins_line_code=PC.InsuranceLine
	          AND SIL.crrnt_snpsht_flag=1
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product 
	on EarnedPremiumDailyCalculation.SourceSystemID='PMS' and SC.ProductAKID=Product.ProductAKID and Product.CurrentSnapshotFlag=1 
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLineOfBusiness 
	On EarnedPremiumDailyCalculation.SourceSystemID='PMS' and SC.InsuranceReferenceLineOfBusinessAKID=InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessAKId and InsuranceReferenceLineOfBusiness.CurrentSnapshotFlag=1
	
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering
	On PolicyOffering.PolicyOfferingAKID=policy.PolicyOfferingAKID and PolicyOffering.CurrentSnapshotFlag=1
	
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter
	ON policy.StrategicProfitCenterAKId=StrategicProfitCenter.StrategicProfitCenterAKId
	AND StrategicProfitCenter.CurrentSnapshotFlag=1
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment
	ON InsuranceSegment.InsuranceSegmentAKId=policy.InsuranceSegmentAKId
	AND InsuranceSegment.CurrentSnapshotFlag=1
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.EnterpriseGroup
	ON EnterpriseGroup.EnterpriseGroupId=StrategicProfitCenter.EnterpriseGroupId
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLegalEntity
	ON InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityId=StrategicProfitCenter.InsuranceReferenceLegalEntityId 
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingPlan RPDT
	           ON PC.RatingPlanAKId=RPDT.RatingPlanAKId and RPDT.CurrentSnapshotFlag=1
	WHERE EarnedPremiumDailyCalculation.SourceSystemId='PMS'
	AND EarnedPremiumDailyCalculation.RunDate>=@Date1 and EarnedPremiumDailyCalculation.RunDate<=getdate()
	AND EarnedPremiumDailyCalculation.EarnedPremiumDailyCalculationID%@{pipeline().parameters.NO_OF_PARTITIONS}=3
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_EarnedPremiumCalculation_IN AS (
	SELECT
	EarnedPremiumDailyCalculationID AS EarnedPremiumCalculationID,
	SourceSystemID,
	PolicyAKID,
	PolicyCoverageAKID,
	StatisticalCoverageAKID,
	ReinsuranceCoverageAKID,
	BureauStatisticalCodeAKID,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	StatisticalCoverageCancellationDate,
	PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	PremiumTransactionExpirationDate,
	PremiumTransactionBookedDate,
	PremiumTransactionCode,
	PremiumTransactionAmount,
	FullTermPremium,
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
	-- *INF*: IIF(SourceSystemID='DCT','N/A',i_NonSubAnnualStatementLineCode)
	IFF(SourceSystemID = 'DCT',
		'N/A',
		i_NonSubAnnualStatementLineCode
	) AS o_NonSubAnnualStatementLineCode,
	AnnualStatementLineProductCode,
	LineOfBusinessCode,
	PolicyOfferingCode,
	RunDate,
	RatingCoverageAKId,
	RatingCoverageEffectiveDate,
	RatingCoverageExpirationDate,
	-1 AS PremiumTransactionID,
	PremiumTransactionAKID AS PremiumTransactionAKId,
	-- *INF*: IIF(PremiumTransactionAKId=-1,
	-- --Historical records
	-- :LKP.LKP_COVERAGEDETAILDIM_HIST(StatisticalCoverageAKID),
	-- --Incremental records
	-- :LKP.LKP_CoverageDetailDim(PremiumTransactionAKId)
	-- )
	IFF(PremiumTransactionAKId = - 1,
		LKP_COVERAGEDETAILDIM_HIST_StatisticalCoverageAKID.CoverageDetailDimId,
		LKP_COVERAGEDETAILDIM_PremiumTransactionAKId.CoverageDetailDimId
	) AS v_CoverageDetailDimID,
	-- *INF*: IIF(ISNULL(v_CoverageDetailDimID), -1, v_CoverageDetailDimID)
	IFF(v_CoverageDetailDimID IS NULL,
		- 1,
		v_CoverageDetailDimID
	) AS CoverageDetailDimID_lkp,
	RiskLocationHashKey,
	EnterpriseGroupCode AS i_EnterpriseGroupCode,
	InsuranceReferenceLegalEntityCode AS i_InsuranceReferenceLegalEntityCode,
	StrategicProfitCenterCode AS i_StrategicProfitCenterCode,
	InsuranceSegmentCode AS i_InsuranceSegmentCode,
	-- *INF*: IIF(NOT ISNULL(i_EnterpriseGroupCode), i_EnterpriseGroupCode, '1')
	IFF(i_EnterpriseGroupCode IS NOT NULL,
		i_EnterpriseGroupCode,
		'1'
	) AS EnterpriseGroupCode,
	-- *INF*: IIF(NOT ISNULL(i_InsuranceReferenceLegalEntityCode), i_InsuranceReferenceLegalEntityCode, '1')
	IFF(i_InsuranceReferenceLegalEntityCode IS NOT NULL,
		i_InsuranceReferenceLegalEntityCode,
		'1'
	) AS InsuranceReferenceLegalEntityCode,
	-- *INF*: IIF(NOT ISNULL(i_StrategicProfitCenterCode), i_StrategicProfitCenterCode, '6')
	IFF(i_StrategicProfitCenterCode IS NOT NULL,
		i_StrategicProfitCenterCode,
		'6'
	) AS StrategicProfitCenterCode,
	-- *INF*: IIF(NOT ISNULL(i_InsuranceSegmentCode), i_InsuranceSegmentCode, 'N/A')
	IFF(i_InsuranceSegmentCode IS NOT NULL,
		i_InsuranceSegmentCode,
		'N/A'
	) AS InsuranceSegmentCode,
	RiskUnitGroup AS i_RiskUnitGroup,
	RiskUnit AS i_RiskUnit,
	RiskUnitSequenceNumber AS i_RiskUnitSequenceNumber,
	MajorPerilCode AS i_MajorPerilCode,
	ClassCode AS i_ClassCode,
	RiskType AS i_RiskType,
	CoverageType AS i_CoverageType,
	StandardInsuranceLineCode AS i_StandardInsuranceLineCode,
	TypeBureauCode AS i_TypeBureauCode,
	PerilGroup AS i_PerilGroup,
	SubCoverageTypeCode AS i_SubCoverageTypeCode,
	CoverageVersion AS i_CoverageVersion,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(SUBSTR(i_RiskUnitSequenceNumber,2,1))
	:UDF.DEFAULT_VALUE_FOR_STRINGS(SUBSTR(i_RiskUnitSequenceNumber, 2, 1
		)
	) AS v_ProductTypeCode,
	-- *INF*: IIF(REG_MATCH(i_StandardInsuranceLineCode,'[^0-9a-zA-Z]'),'N/A',i_StandardInsuranceLineCode)
	IFF(REGEXP_LIKE(i_StandardInsuranceLineCode, '[^0-9a-zA-Z]'
		),
		'N/A',
		i_StandardInsuranceLineCode
	) AS v_Reg_StandardInsuranceLineCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_MajorPerilCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_MajorPerilCode
	) AS v_MajorPerilCode,
	-- *INF*: IIF(LTRIM(v_MajorPerilCode,'0')='','N/A',v_MajorPerilCode)
	IFF(LTRIM(v_MajorPerilCode, '0'
		) = '',
		'N/A',
		v_MajorPerilCode
	) AS v_Zero_MajorPerilCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ClassCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_ClassCode
	) AS v_ClassCode,
	-- *INF*: IIF(v_Reg_StandardInsuranceLineCode='N/A' AND (IN(i_TypeBureauCode,'AL','AN','AP') OR IN(v_Zero_MajorPerilCode,'930','931')),'CA',v_Reg_StandardInsuranceLineCode)
	IFF(v_Reg_StandardInsuranceLineCode = 'N/A' 
		AND ( i_TypeBureauCode IN ('AL','AN','AP') 
			OR v_Zero_MajorPerilCode IN ('930','931') 
		),
		'CA',
		v_Reg_StandardInsuranceLineCode
	) AS v_StandardInsuranceLineCode,
	-- *INF*: IIF(v_StandardInsuranceLineCode='N/A' AND IN(i_TypeBureauCode,'CF','B2','BB','BE','BF','BM','BT','FT','GL','GS','IM','MS','PF','PH','PI','PL','PQ','WC','WP','NB','RL','RN','RP','BC','N/A'),1,0)
	IFF(v_StandardInsuranceLineCode = 'N/A' 
		AND i_TypeBureauCode IN ('CF','B2','BB','BE','BF','BM','BT','FT','GL','GS','IM','MS','PF','PH','PI','PL','PQ','WC','WP','NB','RL','RN','RP','BC','N/A'),
		1,
		0
	) AS v_flag,
	-- *INF*: IIF(IN(v_StandardInsuranceLineCode,'CR') OR v_flag=1,'N/A',:UDF.DEFAULT_VALUE_FOR_STRINGS(i_RiskUnitGroup))
	IFF(v_StandardInsuranceLineCode IN ('CR') 
		OR v_flag = 1,
		'N/A',
		:UDF.DEFAULT_VALUE_FOR_STRINGS(i_RiskUnitGroup
		)
	) AS v_RiskUnitGroup,
	-- *INF*: IIF(LTRIM(v_RiskUnitGroup,'0')='','N/A',v_RiskUnitGroup)
	IFF(LTRIM(v_RiskUnitGroup, '0'
		) = '',
		'N/A',
		v_RiskUnitGroup
	) AS v_Zero_RiskUnitGroup,
	-- *INF*: IIF( v_flag=1 OR   (v_StandardInsuranceLineCode='GL' AND (NOT IN(v_MajorPerilCode,'540','599','919')    OR NOT IN(v_ClassCode,'11111','22222','22250','92100','17000','17001','17002','80051','80052','80053','80054','80055','80056','80057','80058')))   OR IN(v_StandardInsuranceLineCode,'WC','IM','CG','CA')=1,  'N/A',:UDF.DEFAULT_VALUE_FOR_STRINGS(i_RiskUnit))
	IFF(v_flag = 1 
		OR ( v_StandardInsuranceLineCode = 'GL' 
			AND ( NOT v_MajorPerilCode IN ('540','599','919') 
				OR NOT v_ClassCode IN ('11111','22222','22250','92100','17000','17001','17002','80051','80052','80053','80054','80055','80056','80057','80058') 
			) 
		) 
		OR v_StandardInsuranceLineCode IN ('WC','IM','CG','CA') = 1,
		'N/A',
		:UDF.DEFAULT_VALUE_FOR_STRINGS(i_RiskUnit
		)
	) AS v_RiskUnit,
	-- *INF*: IIF(LTRIM(v_RiskUnit,'0')='','N/A',v_RiskUnit)
	IFF(LTRIM(v_RiskUnit, '0'
		) = '',
		'N/A',
		v_RiskUnit
	) AS v_Zero_RiskUnit,
	-- *INF*: IIF(REG_MATCH(v_Zero_RiskUnitGroup,'[^0-9a-zA-Z]'),'N/A',v_Zero_RiskUnitGroup)
	IFF(REGEXP_LIKE(v_Zero_RiskUnitGroup, '[^0-9a-zA-Z]'
		),
		'N/A',
		v_Zero_RiskUnitGroup
	) AS v_PmsRiskUnitGroupCode,
	-- *INF*: IIF(REG_MATCH(v_Zero_RiskUnit,'[^0-9a-zA-Z]'),'N/A',v_Zero_RiskUnit)
	IFF(REGEXP_LIKE(v_Zero_RiskUnit, '[^0-9a-zA-Z]'
		),
		'N/A',
		v_Zero_RiskUnit
	) AS v_PmsRiskUnitCode,
	-- *INF*: SUBSTR(v_PmsRiskUnitCode, 1, 3)
	SUBSTR(v_PmsRiskUnitCode, 1, 3
	) AS v_PmsRiskUnitCode_1_3,
	-- *INF*: IIF(SourceSystemID='PMS',v_StandardInsuranceLineCode,i_StandardInsuranceLineCode)
	IFF(SourceSystemID = 'PMS',
		v_StandardInsuranceLineCode,
		i_StandardInsuranceLineCode
	) AS v_InsuranceLineCode,
	-- *INF*: IIF(REG_MATCH(v_Zero_MajorPerilCode,'[^0-9a-zA-Z]'),'N/A',v_Zero_MajorPerilCode)
	IFF(REGEXP_LIKE(v_Zero_MajorPerilCode, '[^0-9a-zA-Z]'
		),
		'N/A',
		v_Zero_MajorPerilCode
	) AS v_PmsMajorPerilCode,
	-- *INF*: IIF(   REG_MATCH(v_ProductTypeCode,'[^0-9a-zA-Z]') OR v_Reg_StandardInsuranceLineCode<>'GL' OR v_ProductTypeCode='0' OR LENGTH(v_ProductTypeCode)=0,   'N/A',v_ProductTypeCode )
	IFF(REGEXP_LIKE(v_ProductTypeCode, '[^0-9a-zA-Z]'
		) 
		OR v_Reg_StandardInsuranceLineCode <> 'GL' 
		OR v_ProductTypeCode = '0' 
		OR LENGTH(v_ProductTypeCode
		) = 0,
		'N/A',
		v_ProductTypeCode
	) AS v_PmsProductTypeCode,
	-- *INF*: :LKP.LKP_INSURANCEREFERENCECOVERAGEDIM_PMS(v_PmsRiskUnitGroupCode, v_PmsRiskUnitCode, v_PmsMajorPerilCode, v_InsuranceLineCode, v_PmsProductTypeCode)
	LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_v_PmsRiskUnitGroupCode_v_PmsRiskUnitCode_v_PmsMajorPerilCode_v_InsuranceLineCode_v_PmsProductTypeCode.InsuranceReferenceCoverageDimId AS v_InsuranceReferenceCoverageDimId_PMS_1,
	-- *INF*: v_InsuranceReferenceCoverageDimId_PMS_1
	-- 
	-- --IIF(ISNULL(v_InsuranceReferenceCoverageDimId_PMS_1), :LKP.LKP_INSURANCEREFERENCECOVERAGEDIM_PMS(v_PmsRiskUnitGroupCode, v_PmsRiskUnitCode_1_3, v_PmsMajorPerilCode, v_InsuranceLineCode, v_PmsProductTypeCode), v_InsuranceReferenceCoverageDimId_PMS_1)
	v_InsuranceReferenceCoverageDimId_PMS_1 AS v_InsuranceReferenceCoverageDimId_PMS_2,
	-- *INF*: :LKP.LKP_INSURANCEREFERENCECOVERAGEDIM_DCT(i_RiskType, i_CoverageType, v_InsuranceLineCode, i_PerilGroup,i_SubCoverageTypeCode,i_CoverageVersion)
	LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_i_RiskType_i_CoverageType_v_InsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.InsuranceReferenceCoverageDimId AS v_InsuranceReferenceCoverageDimId_DCT,
	-- *INF*: IIF(ISNULL(v_InsuranceReferenceCoverageDimId_PMS_2), -1, v_InsuranceReferenceCoverageDimId_PMS_2)
	IFF(v_InsuranceReferenceCoverageDimId_PMS_2 IS NULL,
		- 1,
		v_InsuranceReferenceCoverageDimId_PMS_2
	) AS o_InsuranceReferenceCoverageDimId_PMS,
	-- *INF*: IIF(ISNULL(v_InsuranceReferenceCoverageDimId_DCT), -1, v_InsuranceReferenceCoverageDimId_DCT)
	IFF(v_InsuranceReferenceCoverageDimId_DCT IS NULL,
		- 1,
		v_InsuranceReferenceCoverageDimId_DCT
	) AS o_InsuranceReferenceCoverageDimId_DCT,
	CustomerCareCommissionRate,
	RatingPlanCode AS i_RatingPlanCode,
	-- *INF*: IIF(ISNULL(i_RatingPlanCode), '1', i_RatingPlanCode)
	IFF(i_RatingPlanCode IS NULL,
		'1',
		i_RatingPlanCode
	) AS o_RatingPlanCode,
	PolicyCoverageEffectiveDate,
	PolicyCoverageExpirationDate
	FROM SQ_EarnedPremiumDailyCalculation
	LEFT JOIN LKP_COVERAGEDETAILDIM_HIST LKP_COVERAGEDETAILDIM_HIST_StatisticalCoverageAKID
	ON LKP_COVERAGEDETAILDIM_HIST_StatisticalCoverageAKID.StatisticalCoverageAKID = StatisticalCoverageAKID

	LEFT JOIN LKP_COVERAGEDETAILDIM LKP_COVERAGEDETAILDIM_PremiumTransactionAKId
	ON LKP_COVERAGEDETAILDIM_PremiumTransactionAKId.PremiumTransactionAKId = PremiumTransactionAKId

	LEFT JOIN LKP_INSURANCEREFERENCECOVERAGEDIM_PMS LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_v_PmsRiskUnitGroupCode_v_PmsRiskUnitCode_v_PmsMajorPerilCode_v_InsuranceLineCode_v_PmsProductTypeCode
	ON LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_v_PmsRiskUnitGroupCode_v_PmsRiskUnitCode_v_PmsMajorPerilCode_v_InsuranceLineCode_v_PmsProductTypeCode.InsuranceLineCode = v_PmsRiskUnitGroupCode
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_v_PmsRiskUnitGroupCode_v_PmsRiskUnitCode_v_PmsMajorPerilCode_v_InsuranceLineCode_v_PmsProductTypeCode.PmsRiskUnitGroupCode = v_PmsRiskUnitCode
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_v_PmsRiskUnitGroupCode_v_PmsRiskUnitCode_v_PmsMajorPerilCode_v_InsuranceLineCode_v_PmsProductTypeCode.PmsRiskUnitCode = v_PmsMajorPerilCode
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_v_PmsRiskUnitGroupCode_v_PmsRiskUnitCode_v_PmsMajorPerilCode_v_InsuranceLineCode_v_PmsProductTypeCode.PmsMajorPerilCode = v_InsuranceLineCode
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_v_PmsRiskUnitGroupCode_v_PmsRiskUnitCode_v_PmsMajorPerilCode_v_InsuranceLineCode_v_PmsProductTypeCode.PmsProductTypeCode = v_PmsProductTypeCode

	LEFT JOIN LKP_INSURANCEREFERENCECOVERAGEDIM_DCT LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_i_RiskType_i_CoverageType_v_InsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion
	ON LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_i_RiskType_i_CoverageType_v_InsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.DctRiskTypeCode = i_RiskType
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_i_RiskType_i_CoverageType_v_InsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.DctCoverageTypeCode = i_CoverageType
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_i_RiskType_i_CoverageType_v_InsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.InsuranceLineCode = v_InsuranceLineCode
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_i_RiskType_i_CoverageType_v_InsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.DctPerilGroup = i_PerilGroup
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_i_RiskType_i_CoverageType_v_InsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.DctSubCoverageTypeCode = i_SubCoverageTypeCode
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_i_RiskType_i_CoverageType_v_InsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.DctCoverageVersion = i_CoverageVersion

),
LKP_InsuranceReferenceDim AS (
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
		WHERE crrnt_snpsht_flag='1'
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
		WHERE crrnt_snpsht_flag='1'
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
		WHERE crrnt_snpsht_flag='1'
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
	IFF(AgencyDimID IS NOT NULL,
		AgencyDimID,
		- 1
	) AS AgencyDimID_out,
	mplt_PolicyDimID_PremiumMaster.pol_dim_id AS PolicyDimID,
	mplt_PolicyDimID_PremiumMaster.pol_status_code,
	-- *INF*: IIF(NOT ISNULL(PolicyDimID),PolicyDimID,-1)
	IFF(PolicyDimID IS NOT NULL,
		PolicyDimID,
		- 1
	) AS PolicyDimID_out,
	mplt_PolicyDimID_PremiumMaster.contract_cust_dim_id AS ContractCustomerDimID,
	-- *INF*: IIF(NOT ISNULL(ContractCustomerDimID),ContractCustomerDimID,-1)
	IFF(ContractCustomerDimID IS NOT NULL,
		ContractCustomerDimID,
		- 1
	) AS ContractCustomerDimID_out,
	LKP_RiskLocationDim.RiskLocationDimID,
	-- *INF*: IIF(NOT ISNULL(RiskLocationDimID),RiskLocationDimID,-1)
	IFF(RiskLocationDimID IS NOT NULL,
		RiskLocationDimID,
		- 1
	) AS RiskLocationDimID_out,
	LKP_reinsurance_coverage_dim.reins_cov_dim_id AS ReinsuranceCoverageDimID,
	-- *INF*: IIF(NOT ISNULL(ReinsuranceCoverageDimID),ReinsuranceCoverageDimID,-1)
	IFF(ReinsuranceCoverageDimID IS NOT NULL,
		ReinsuranceCoverageDimID,
		- 1
	) AS ReinsuranceCoverageDimID_out,
	lkp_PremiumTransactionTypeDim.PremiumTransactionTypeDimID,
	-- *INF*: IIF(NOT ISNULL(PremiumTransactionTypeDimID),PremiumTransactionTypeDimID,-1)
	IFF(PremiumTransactionTypeDimID IS NOT NULL,
		PremiumTransactionTypeDimID,
		- 1
	) AS PremiumTransactionTypeDimID_out,
	EXP_EarnedPremiumCalculation_IN.PremiumTransactionID AS EDWPremiumTransactionPKID,
	-- *INF*: IIF(NOT ISNULL(EDWPremiumTransactionPKID),EDWPremiumTransactionPKID,-1)
	IFF(EDWPremiumTransactionPKID IS NOT NULL,
		EDWPremiumTransactionPKID,
		- 1
	) AS EDWPremiumTransactionPKID_out,
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
	IFF(v_PremiumTransactionBookedDateID IS NOT NULL,
		v_PremiumTransactionBookedDateID,
		- 1
	) AS PremiumTransactionBookedDateID_out,
	EXP_EarnedPremiumCalculation_IN.RunDate AS PremiumTransactionRunDate,
	-- *INF*: IIF((PremiumTransactionBookedDate<=LAST_DAY(PremiumTransactionRunDate)) AND (PremiumTransactionBookedDate>=
	-- SET_DATE_PART(SET_DATE_PART(SET_DATE_PART(SET_DATE_PART( PremiumTransactionRunDate, 'DD', 1 ),'HH',0),'MI',0),'SS',0)),'Y','N')
	IFF(( PremiumTransactionBookedDate <= LAST_DAY(PremiumTransactionRunDate
			) 
		) 
		AND ( PremiumTransactionBookedDate >= DATEADD(SECOND,0-DATE_PART(SECOND,DATEADD(MINUTE,0-DATE_PART(MINUTE,DATEADD(HOUR,0-DATE_PART(HOUR,DATEADD(DAY,1-DATE_PART(DAY,PremiumTransactionRunDate),PremiumTransactionRunDate)),DATEADD(DAY,1-DATE_PART(DAY,PremiumTransactionRunDate),PremiumTransactionRunDate))),DATEADD(HOUR,0-DATE_PART(HOUR,DATEADD(DAY,1-DATE_PART(DAY,PremiumTransactionRunDate),PremiumTransactionRunDate)),DATEADD(DAY,1-DATE_PART(DAY,PremiumTransactionRunDate),PremiumTransactionRunDate)))),DATEADD(MINUTE,0-DATE_PART(MINUTE,DATEADD(HOUR,0-DATE_PART(HOUR,DATEADD(DAY,1-DATE_PART(DAY,PremiumTransactionRunDate),PremiumTransactionRunDate)),DATEADD(DAY,1-DATE_PART(DAY,PremiumTransactionRunDate),PremiumTransactionRunDate))),DATEADD(HOUR,0-DATE_PART(HOUR,DATEADD(DAY,1-DATE_PART(DAY,PremiumTransactionRunDate),PremiumTransactionRunDate)),DATEADD(DAY,1-DATE_PART(DAY,PremiumTransactionRunDate),PremiumTransactionRunDate)))) 
		),
		'Y',
		'N'
	) AS DateFlagForWittenPremium,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(PremiumTransactionRunDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionRunDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PremiumTransactionRunDateID,
	-- *INF*: IIF(NOT ISNULL(v_PremiumTransactionRunDateID),v_PremiumTransactionRunDateID,-1)
	IFF(v_PremiumTransactionRunDateID IS NOT NULL,
		v_PremiumTransactionRunDateID,
		- 1
	) AS PremiumTransactionRunDateID_out,
	EXP_EarnedPremiumCalculation_IN.PolicyCoverageEffectiveDate,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(PolicyCoverageEffectiveDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_PolicyCoverageEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PolicyCoverageEffectiveDateID,
	-- *INF*: IIF(NOT ISNULL(v_PolicyCoverageEffectiveDateID),v_PolicyCoverageEffectiveDateID,-1)
	IFF(v_PolicyCoverageEffectiveDateID IS NOT NULL,
		v_PolicyCoverageEffectiveDateID,
		- 1
	) AS PolicyCoverageEffectiveDateID_out,
	EXP_EarnedPremiumCalculation_IN.PolicyCoverageExpirationDate,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(PolicyCoverageExpirationDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_PolicyCoverageExpirationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PolicyCoverageExpirationDateID,
	-- *INF*: IIF(NOT ISNULL(v_PolicyCoverageExpirationDateID),v_PolicyCoverageExpirationDateID,-1)
	IFF(v_PolicyCoverageExpirationDateID IS NOT NULL,
		v_PolicyCoverageExpirationDateID,
		- 1
	) AS PolicyCoverageExpirationDateID_out,
	EXP_EarnedPremiumCalculation_IN.PremiumTransactionEnteredDate,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(PremiumTransactionEnteredDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionEnteredDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PremiumTransactionEnteredDateID,
	-- *INF*: IIF(NOT ISNULL(v_PremiumTransactionEnteredDateID),v_PremiumTransactionEnteredDateID,-1)
	IFF(v_PremiumTransactionEnteredDateID IS NOT NULL,
		v_PremiumTransactionEnteredDateID,
		- 1
	) AS PremiumTransactionEnteredDateID_out,
	EXP_EarnedPremiumCalculation_IN.PremiumTransactionEffectiveDate,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(PremiumTransactionEffectiveDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PremiumTransactionEffectiveDateID,
	-- *INF*: IIF(NOT ISNULL(v_PremiumTransactionEffectiveDateID),v_PremiumTransactionEffectiveDateID,-1)
	IFF(v_PremiumTransactionEffectiveDateID IS NOT NULL,
		v_PremiumTransactionEffectiveDateID,
		- 1
	) AS PremiumTransactionEffectiveDateID_out,
	EXP_EarnedPremiumCalculation_IN.PremiumTransactionExpirationDate,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(PremiumTransactionExpirationDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionExpirationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PremiumTransactionExpirationDateID,
	-- *INF*: IIF(NOT ISNULL(v_PremiumTransactionExpirationDateID),v_PremiumTransactionExpirationDateID,-1)
	IFF(v_PremiumTransactionExpirationDateID IS NOT NULL,
		v_PremiumTransactionExpirationDateID,
		- 1
	) AS PremiumTransactionExpirationDateID_out,
	EXP_EarnedPremiumCalculation_IN.PremiumType,
	LKP_asl_dim.asl_dim_id AS ASLdimID,
	-- *INF*: IIF(NOT ISNULL(ASLdimID),ASLdimID,-1)
	IFF(ASLdimID IS NOT NULL,
		ASLdimID,
		- 1
	) AS ASLdimID_out,
	LKP_asl_product_code.asl_prdct_code_dim_id AS ASLproductcodedimID,
	-- *INF*: IIF(NOT ISNULL(ASLproductcodedimID),ASLproductcodedimID,-1)
	IFF(ASLproductcodedimID IS NOT NULL,
		ASLproductcodedimID,
		- 1
	) AS ASLproductcodedimID_out,
	EXP_EarnedPremiumCalculation_IN.PolicyEffectiveDate,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(PolicyEffectiveDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_PolicyEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PolicyEffectiveDateID,
	v_PolicyEffectiveDateID AS PolicyEffectiveDateID_out,
	EXP_EarnedPremiumCalculation_IN.PolicyExpirationDate,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(PolicyExpirationDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_PolicyExpirationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PolicyExpirationDateID,
	-- *INF*: IIF(ISNULL(v_PolicyExpirationDateID),-1,v_PolicyExpirationDateID)
	IFF(v_PolicyExpirationDateID IS NULL,
		- 1,
		v_PolicyExpirationDateID
	) AS PolicyExpirationDateID_out,
	EXP_EarnedPremiumCalculation_IN.EarnedPremiumCalculationID AS EDWEarnedPremiumDailyCalculationPKID,
	-- *INF*: :LKP.LKP_CALENDER_DIM(TO_DATE(TO_CHAR(RatingCoverageEffectiveDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_TO_DATE_TO_CHAR_RatingCoverageEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS RatingCoverageEffectiveDateId,
	-- *INF*: :LKP.LKP_CALENDER_DIM(TO_DATE(TO_CHAR(RatingCoverageExpirationDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_TO_DATE_TO_CHAR_RatingCoverageExpirationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS RatingCoverageExpirationDateId,
	-- *INF*: --IIF(ISNULL(RatingCoverageDimId), -1, RatingCoverageDimId)
	'' AS RatingCoverageDimId_out,
	LKP_InsuranceReferenceDim.InsuranceReferenceDimId,
	-- *INF*: IIF(ISNULL(InsuranceReferenceDimId), -1, InsuranceReferenceDimId)
	IFF(InsuranceReferenceDimId IS NULL,
		- 1,
		InsuranceReferenceDimId
	) AS o_InsuranceReferenceDimId,
	-- *INF*: --IIF(ISNULL(SalesDivisionDimID), -1, SalesDivisionDimID)
	'' AS o_SalesDivisionDimID,
	EXP_EarnedPremiumCalculation_IN.CoverageDetailDimID_lkp AS CoverageDetailDimID,
	EXP_EarnedPremiumCalculation_IN.StatisticalCoverageCancellationDate AS i_StatisticalCoverageCancellationDate,
	-- *INF*: :LKP.LKP_CALENDER_DIM(TO_DATE(TO_CHAR(i_StatisticalCoverageCancellationDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_TO_DATE_TO_CHAR_i_StatisticalCoverageCancellationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_StatisticalCoverageCancellationDate,
	-- *INF*: IIF(NOT ISNULL(v_StatisticalCoverageCancellationDate),v_StatisticalCoverageCancellationDate,-1)
	IFF(v_StatisticalCoverageCancellationDate IS NOT NULL,
		v_StatisticalCoverageCancellationDate,
		- 1
	) AS o_StatisticalCoverageCancellationDate,
	EXP_EarnedPremiumCalculation_IN.SourceSystemID,
	EXP_EarnedPremiumCalculation_IN.o_InsuranceReferenceCoverageDimId_PMS AS InsuranceReferenceCoverageDimId_PMS,
	EXP_EarnedPremiumCalculation_IN.o_InsuranceReferenceCoverageDimId_DCT AS InsuranceReferenceCoverageDimId_DCT,
	-- *INF*: DECODE(SourceSystemID,'DCT',InsuranceReferenceCoverageDimId_DCT,'PMS',InsuranceReferenceCoverageDimId_PMS)
	DECODE(SourceSystemID,
		'DCT', InsuranceReferenceCoverageDimId_DCT,
		'PMS', InsuranceReferenceCoverageDimId_PMS
	) AS o_InsuranceReferenceCoverageDimId
	FROM EXP_EarnedPremiumCalculation_IN
	 -- Manually join with mplt_PolicyDimID_PremiumMaster
	LEFT JOIN LKP_InsuranceReferenceDim
	ON LKP_InsuranceReferenceDim.EnterpriseGroupCode = EXP_EarnedPremiumCalculation_IN.EnterpriseGroupCode AND LKP_InsuranceReferenceDim.InsuranceReferenceLegalEntityCode = EXP_EarnedPremiumCalculation_IN.InsuranceReferenceLegalEntityCode AND LKP_InsuranceReferenceDim.StrategicProfitCenterCode = EXP_EarnedPremiumCalculation_IN.StrategicProfitCenterCode AND LKP_InsuranceReferenceDim.InsuranceSegmentCode = EXP_EarnedPremiumCalculation_IN.InsuranceSegmentCode AND LKP_InsuranceReferenceDim.PolicyOfferingCode = EXP_EarnedPremiumCalculation_IN.PolicyOfferingCode AND LKP_InsuranceReferenceDim.ProductCode = EXP_EarnedPremiumCalculation_IN.ProductCode AND LKP_InsuranceReferenceDim.InsuranceReferenceLineOfBusinessCode = EXP_EarnedPremiumCalculation_IN.LineOfBusinessCode AND LKP_InsuranceReferenceDim.RatingPlanCode = EXP_EarnedPremiumCalculation_IN.o_RatingPlanCode
	LEFT JOIN LKP_RiskLocationDim
	ON LKP_RiskLocationDim.RiskLocationHashKey = EXP_EarnedPremiumCalculation_IN.RiskLocationHashKey
	LEFT JOIN LKP_asl_dim
	ON LKP_asl_dim.asl_code = EXP_EarnedPremiumCalculation_IN.AnnualStatementLineCode AND LKP_asl_dim.sub_asl_code = EXP_EarnedPremiumCalculation_IN.SubAnnualStatementLineCode AND LKP_asl_dim.sub_non_asl_code = EXP_EarnedPremiumCalculation_IN.o_NonSubAnnualStatementLineCode
	LEFT JOIN LKP_asl_product_code
	ON LKP_asl_product_code.asl_prdct_code = EXP_EarnedPremiumCalculation_IN.AnnualStatementLineProductCode
	LEFT JOIN LKP_reinsurance_coverage_dim
	ON LKP_reinsurance_coverage_dim.edw_reins_cov_ak_id = EXP_EarnedPremiumCalculation_IN.ReinsuranceCoverageAKID AND LKP_reinsurance_coverage_dim.eff_from_date <= EXP_EarnedPremiumCalculation_IN.PremiumTransactionEffectiveDate AND LKP_reinsurance_coverage_dim.eff_to_date >= EXP_EarnedPremiumCalculation_IN.PremiumTransactionEffectiveDate
	LEFT JOIN lkp_PremiumTransactionTypeDim
	ON lkp_PremiumTransactionTypeDim.PremiumTransactionCode = EXP_EarnedPremiumCalculation_IN.PremiumTransactionCode AND lkp_PremiumTransactionTypeDim.ReasonAmendedCode = EXP_EarnedPremiumCalculation_IN.ReasonAmendedCode AND lkp_PremiumTransactionTypeDim.PremiumTypeCode = EXP_EarnedPremiumCalculation_IN.PremiumType AND lkp_PremiumTransactionTypeDim.CustomerCareCommissionRate = EXP_EarnedPremiumCalculation_IN.CustomerCareCommissionRate
	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionBookedDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionBookedDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(PremiumTransactionBookedDate, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionRunDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionRunDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(PremiumTransactionRunDate, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_PolicyCoverageEffectiveDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_PolicyCoverageEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(PolicyCoverageEffectiveDate, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_PolicyCoverageExpirationDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_PolicyCoverageExpirationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(PolicyCoverageExpirationDate, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionEnteredDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionEnteredDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(PremiumTransactionEnteredDate, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionEffectiveDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(PremiumTransactionEffectiveDate, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionExpirationDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_PremiumTransactionExpirationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(PremiumTransactionExpirationDate, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_PolicyEffectiveDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_PolicyEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(PolicyEffectiveDate, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_PolicyExpirationDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_PolicyExpirationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(PolicyExpirationDate, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_TO_DATE_TO_CHAR_RatingCoverageEffectiveDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_TO_DATE_TO_CHAR_RatingCoverageEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_DATE(TO_CHAR(RatingCoverageEffectiveDate, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_TO_DATE_TO_CHAR_RatingCoverageExpirationDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_TO_DATE_TO_CHAR_RatingCoverageExpirationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_DATE(TO_CHAR(RatingCoverageExpirationDate, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_TO_DATE_TO_CHAR_i_StatisticalCoverageCancellationDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_TO_DATE_TO_CHAR_i_StatisticalCoverageCancellationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_DATE(TO_CHAR(i_StatisticalCoverageCancellationDate, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

),
EXP_Evaluate_Fields AS (
	SELECT
	AuditID,
	EDWEarnedPremiumDailyCalculationPKID,
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
	PremiumType,
	PremiumTransactionAmount,
	FullTermPremium,
	DateFlagForWittenPremium,
	-- *INF*: IIF(rtrim(PremiumType)='D' AND DateFlagForWittenPremium='Y',PremiumTransactionAmount,0)
	IFF(rtrim(PremiumType
		) = 'D' 
		AND DateFlagForWittenPremium = 'Y',
		PremiumTransactionAmount,
		0
	) AS v_TotalDirectWrittenPremium,
	v_TotalDirectWrittenPremium AS TotalDirectWrittenPremium,
	-- *INF*: IIF(rtrim(PremiumType)='C' AND DateFlagForWittenPremium='Y',PremiumTransactionAmount,0)
	IFF(rtrim(PremiumType
		) = 'C' 
		AND DateFlagForWittenPremium = 'Y',
		PremiumTransactionAmount,
		0
	) AS v_TotalCededWrittenPremium,
	v_TotalCededWrittenPremium AS TotalCededWrittenPremium,
	v_TotalDirectWrittenPremium - v_TotalCededWrittenPremium AS TotalNetWrittenPremium,
	EarnedPremium,
	-- *INF*: IIF(rtrim(PremiumType)='D',EarnedPremium,0)
	IFF(rtrim(PremiumType
		) = 'D',
		EarnedPremium,
		0
	) AS v_DirectEarnedPremium,
	v_DirectEarnedPremium AS DirectEarnedPremium,
	-- *INF*: IIF(rtrim(PremiumType)='C',EarnedPremium,0)
	IFF(rtrim(PremiumType
		) = 'C',
		EarnedPremium,
		0
	) AS v_CededEarnedPremium,
	v_CededEarnedPremium AS CededEarnedPremium,
	v_DirectEarnedPremium - v_CededEarnedPremium AS NetEarnedPremium,
	ChangeInEarnedPremium,
	-- *INF*: IIF(rtrim(PremiumType)='D',ChangeInEarnedPremium,0)
	IFF(rtrim(PremiumType
		) = 'D',
		ChangeInEarnedPremium,
		0
	) AS v_ChangeinDirectEarnedPremium,
	v_ChangeinDirectEarnedPremium AS ChangeinDirectEarnedPremium,
	-- *INF*: IIF(rtrim(PremiumType)='C',ChangeInEarnedPremium,0)
	IFF(rtrim(PremiumType
		) = 'C',
		ChangeInEarnedPremium,
		0
	) AS v_ChangeinCededEarnedPremium,
	v_ChangeinCededEarnedPremium AS ChangeinCededEarnedPremium,
	v_ChangeinDirectEarnedPremium - v_ChangeinCededEarnedPremium AS NetChangeinEarnedPremium,
	UnearnedPremium AS Direct_UnearnedPremium,
	-- *INF*: IIF(rtrim(PremiumType)='D',Direct_UnearnedPremium,0)
	IFF(rtrim(PremiumType
		) = 'D',
		Direct_UnearnedPremium,
		0
	) AS v_Direct_UnearnedPremium,
	v_Direct_UnearnedPremium AS O_Direct_UnearnedPremium,
	ChangeInUnearnedPremium AS DirectChangeInUnearnedPremium,
	-- *INF*: IIF(rtrim(PremiumType)='D',DirectChangeInUnearnedPremium,0)
	IFF(rtrim(PremiumType
		) = 'D',
		DirectChangeInUnearnedPremium,
		0
	) AS DailyDirectChangeInUnearnedPremium,
	-- *INF*: IIF(rtrim(PremiumType)='C',v_TotalCededWrittenPremium - v_CededEarnedPremium,0)
	IFF(rtrim(PremiumType
		) = 'C',
		v_TotalCededWrittenPremium - v_CededEarnedPremium,
		0
	) AS v_CededUnearnedPremium,
	v_CededUnearnedPremium AS CededUnearnedPremium,
	v_Direct_UnearnedPremium - v_CededUnearnedPremium AS NetUnearnedPremium,
	-- *INF*: IIF(pol_status_code='I' AND PremiumType='D', PremiumTransactionAmount, 0)
	IFF(pol_status_code = 'I' 
		AND PremiumType = 'D',
		PremiumTransactionAmount,
		0
	) AS v_DirectInforcePremium,
	v_DirectInforcePremium AS DirectInforcePremium,
	-- *INF*: IIF(pol_status_code='I' AND PremiumType='C', PremiumTransactionAmount, 0)
	IFF(pol_status_code = 'I' 
		AND PremiumType = 'C',
		PremiumTransactionAmount,
		0
	) AS v_CededInforcePremium,
	v_CededInforcePremium AS CededInforcePremium,
	v_DirectInforcePremium - v_CededInforcePremium AS v_NetInforcePremium,
	v_NetInforcePremium AS NetInforcePremium,
	-- *INF*: IIF(rtrim(PremiumType)='C',v_ChangeinCededEarnedPremium*(-1),0)
	IFF(rtrim(PremiumType
		) = 'C',
		v_ChangeinCededEarnedPremium * ( - 1 
		),
		0
	) AS DailyChangeInCededUnearnedPremium,
	RatingCoverageEffectiveDateId,
	RatingCoverageExpirationDateId,
	RatingCoverageDimId_out,
	o_InsuranceReferenceDimId AS InsuranceReferenceDimId,
	o_SalesDivisionDimID AS SalesDivisionDimID,
	CoverageDetailDimID,
	o_StatisticalCoverageCancellationDate AS StatisticalCoverageCancellationDate,
	o_InsuranceReferenceCoverageDimId AS InsuranceReferenceCoverageDimId,
	-- *INF*: :LKP.LKP_EARNEDPREMIUMDAILYFACT(EDWEarnedPremiumDailyCalculationPKID)
	LKP_EARNEDPREMIUMDAILYFACT_EDWEarnedPremiumDailyCalculationPKID.EarnedPremiumTransactionDailyFactID AS LKP_EDWEarnedPremiumDailyCalcID
	FROM EXP_Consolidate_Data_from_Lookups
	LEFT JOIN LKP_EARNEDPREMIUMDAILYFACT LKP_EARNEDPREMIUMDAILYFACT_EDWEarnedPremiumDailyCalculationPKID
	ON LKP_EARNEDPREMIUMDAILYFACT_EDWEarnedPremiumDailyCalculationPKID.EDWEarnedPremiumDailyCalculationPKID = EDWEarnedPremiumDailyCalculationPKID

),
FIL_Duplicate_EDWEarnedPremiumCalc_Daily AS (
	SELECT
	AuditID, 
	EDWEarnedPremiumDailyCalculationPKID, 
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
	NetChangeinEarnedPremium, 
	O_Direct_UnearnedPremium, 
	DailyDirectChangeInUnearnedPremium, 
	CededUnearnedPremium, 
	NetUnearnedPremium, 
	DirectInforcePremium, 
	CededInforcePremium, 
	NetInforcePremium, 
	DailyChangeInCededUnearnedPremium, 
	RatingCoverageEffectiveDateId, 
	RatingCoverageExpirationDateId, 
	RatingCoverageDimId_out, 
	InsuranceReferenceDimId, 
	SalesDivisionDimID, 
	CoverageDetailDimID, 
	StatisticalCoverageCancellationDate, 
	InsuranceReferenceCoverageDimId, 
	LKP_EDWEarnedPremiumDailyCalcID
	FROM EXP_Evaluate_Fields
	WHERE ISNULL(LKP_EDWEarnedPremiumDailyCalcID)
),
EXP_Pre_Insert AS (
	SELECT
	AuditID,
	EDWEarnedPremiumDailyCalculationPKID,
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
	NetChangeinEarnedPremium AS NetChangeinCededEarnedPremium,
	O_Direct_UnearnedPremium AS DirectUnearnedPremium,
	DailyDirectChangeInUnearnedPremium AS DirectChangeInUnearnedPremium,
	CededUnearnedPremium,
	NetUnearnedPremium,
	DirectInforcePremium,
	CededInforcePremium,
	NetInforcePremium,
	DailyChangeInCededUnearnedPremium,
	RatingCoverageEffectiveDateId,
	RatingCoverageExpirationDateId,
	RatingCoverageDimId_out,
	InsuranceReferenceDimId,
	SalesDivisionDimID,
	CoverageDetailDimID,
	StatisticalCoverageCancellationDate,
	InsuranceReferenceCoverageDimId
	FROM FIL_Duplicate_EDWEarnedPremiumCalc_Daily
),
TGT_EarnedPremiumTransactionDailyFact_INSERT AS (

	------------ PRE SQL ----------
	if exists(select * from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}. EarnedPremiumTransactionDailyFact where substring(CONVERT(varchar(8),DATEADD(d, -@{pipeline().parameters.NO_OF_DAYS}+1, getdate()),112),7,2)='02')
	begin
	            exec ('truncate table @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}. EarnedPremiumTransactionDailyFact')
	end
	-------------------------------


	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.EarnedPremiumTransactionDailyFact
	(AuditID, EDWEarnedPremiumDailyCalculationPKID, AnnualStatementLineDimID, AnnualStatementLineProductCodeDimID, AgencyDimID, PolicyDimID, ContractCustomerDimID, RiskLocationDimID, ReinsuranceCoverageDimID, PremiumTransactionTypeDimID, PolicyEffectiveDateID, PolicyExpirationDateID, PolicyCoverageEffectiveDateID, PolicyCoverageExpirationDateID, PremiumTransactionEnteredDateID, PremiumTransactionEffectiveDateID, PremiumTransactionExpirationDateID, PremiumTransactionBookedDateID, PremiumTransactionRunDateID, DailyTotalDirectWrittenPremium, DailyTotalCededWrittenPremium, DailyTotalNetWrittenPremium, DailyDirectEarnedPremium, DailyCededEarnedPremium, DailyNetEarnedPremium, DailyChangeinDirectEarnedPremium, DailyChangeinCededEarnedPremium, DailyNetChangeinCededEarnedPremium, DailyDirectUnearnedPremium, DailyCededUnearnedPremium, DailyNetUnearnedPremium, DailyChangeInDirectUnearnedPremium, DailyChangeInCededUnearnedPremium, DailyDirectInforcePremium, DailyCededInforcePremium, DailyNetInforcePremium, InsuranceReferenceDimId, CoverageDetailDimId, InsuranceReferenceCoverageDimId, CoverageCancellationDateId)
	SELECT 
	AUDITID, 
	EDWEARNEDPREMIUMDAILYCALCULATIONPKID, 
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
	TotalDirectWrittenPremium AS DAILYTOTALDIRECTWRITTENPREMIUM, 
	TotalCededWrittenPremium AS DAILYTOTALCEDEDWRITTENPREMIUM, 
	TotalNetWrittenPremium AS DAILYTOTALNETWRITTENPREMIUM, 
	DirectEarnedPremium AS DAILYDIRECTEARNEDPREMIUM, 
	CededEarnedPremium AS DAILYCEDEDEARNEDPREMIUM, 
	NetEarnedPremium AS DAILYNETEARNEDPREMIUM, 
	ChangeinDirectEarnedPremium AS DAILYCHANGEINDIRECTEARNEDPREMIUM, 
	ChangeinCededEarnedPremium AS DAILYCHANGEINCEDEDEARNEDPREMIUM, 
	NetChangeinCededEarnedPremium AS DAILYNETCHANGEINCEDEDEARNEDPREMIUM, 
	DirectUnearnedPremium AS DAILYDIRECTUNEARNEDPREMIUM, 
	CededUnearnedPremium AS DAILYCEDEDUNEARNEDPREMIUM, 
	NetUnearnedPremium AS DAILYNETUNEARNEDPREMIUM, 
	DirectChangeInUnearnedPremium AS DAILYCHANGEINDIRECTUNEARNEDPREMIUM, 
	DAILYCHANGEINCEDEDUNEARNEDPREMIUM, 
	DirectInforcePremium AS DAILYDIRECTINFORCEPREMIUM, 
	CededInforcePremium AS DAILYCEDEDINFORCEPREMIUM, 
	NetInforcePremium AS DAILYNETINFORCEPREMIUM, 
	INSURANCEREFERENCEDIMID, 
	CoverageDetailDimID AS COVERAGEDETAILDIMID, 
	INSURANCEREFERENCECOVERAGEDIMID, 
	StatisticalCoverageCancellationDate AS COVERAGECANCELLATIONDATEID
	FROM EXP_Pre_Insert
),
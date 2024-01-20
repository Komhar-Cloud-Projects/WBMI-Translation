WITH
SQ_InsuranceReferenceCoverageDim AS (
	select distinct DctRiskTypeCode,
	DctCoverageTypeCode,
	DctPerilGroup,
	DctSubCoverageTypeCode,
	InsuranceLineCode,
	DctCoverageVersion,
	CoverageDescription,
	CoverageGroupDescription 
	from @{pipeline().parameters.DATAMART_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim
),
EXP_Src_Data AS (
	SELECT
	DctRiskTypeCode,
	DctCoverageTypeCode,
	-- *INF*: Upper(DctCoverageTypeCode)
	Upper(DctCoverageTypeCode) AS Upper_DctCoverageTypeCode,
	DctPerilGroup,
	DctSubCoverageTypeCode,
	InsuranceLineCode,
	DctCoverageVersion,
	CoverageDescription,
	CoverageGroupDescription
	FROM SQ_InsuranceReferenceCoverageDim
),
SQ_PremiumTransaction AS (
	declare @ProcessDate as datetime
	
	set @ProcessDate=@{pipeline().parameters.PROCESS_DATE};
	
	SELECT PT.SourceSystemID,
		PT.PremiumTransactionAKID,
		POL.pol_key,
		POL.Pol_ak_id,
		RL.LocationUnitNumber,
		RC.SubLocationUnitNumber,
		PT.WindCoverageFlag,
		PT.PremiumTransactionAmount,
		RC.RatingCoverageAKID,
		PC.PolicyCoverageAKID,
		RL.RiskLocationAKID,
		CDCA.VehicleNumber,
		RC.RiskType,
		RC.CoverageType,
		RC.PerilGroup,
		RC.SubCoverageTypeCode,
		SIL.StandardInsuranceLineCode,
		RC.CoverageVersion,
	      POL.Terrorism_risk_ind,
	      POL.StrategicProfitCenterAKId,
	      POL.InsuranceSegmentAKId,
	      POL.PolicyOfferingAKId,
		RC.ProductAKId,
		RC.InsuranceReferenceLineOfBusinessAKId
	FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	INNER JOIN @{pipeline().parameters.DATAMART_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailDim CDD ON PT.PremiumTransactionID = CDD.EDWPremiumTransactionPKId
		AND CDD.CoverageEffectiveDate <= @ProcessDate
		AND CDD.CoverageExpirationDate > @ProcessDate
	      AND PT.SourceSystemID='DCT'
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC ON PT.RatingCoverageAKID = RC.RatingCoverageAKID
		AND RC.EffectiveDate = PT.EffectiveDate
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC ON PC.PolicyCoverageAKID = RC.PolicyCoverageAKID
		AND PC.CurrentSnapshotFlag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL ON PC.SupInsuranceLineId = SIL.sup_ins_line_id
		AND SIL.crrnt_snpsht_flag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL ON PC.RiskLocationAKID = RL.RiskLocationAKID
		AND RL.CurrentSnapshotFlag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL ON POL.pol_ak_id = RL.PolicyAKID
		AND POL.crrnt_snpsht_flag = 1
		AND POL.pol_eff_date <= @ProcessDate
		AND POL.pol_exp_date > @ProcessDate 
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.Product PRD ON PRD.ProductAKId = RC.ProductAKId
		AND PRD.CurrentSnapshotFlag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLineOfBusiness IRLOB ON IRLOB.InsuranceReferenceLineOfBusinessAKId = RC.InsuranceReferenceLineOfBusinessAKId
		AND IRLOB.CurrentSnapshotFlag = 1
	LEFT JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto CDCA ON CDCA.PremiumTransactionId = PT.PremiumTransactionId
	WHERE PT.CurrentSnapshotFlag = 1
	AND PT.ReasonAmendedCode not in ('CWO','Claw Back')	
	AND PT.PremiumTransactionBookedDate<=@ProcessDate
	AND PT.PremiumTransactionEnteredDate<=@ProcessDate
	AND PT.PremiumTransactionEffectiveDate<=@ProcessDate
	
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_SRCData AS (
	SELECT
	SourceSystemID,
	PremiumTransactionAKID,
	pol_key,
	pol_ak_id,
	LocationUnitNumber,
	SubLocationUnitNumber,
	WindCoverageFlag,
	PremiumTransactionAmount,
	RatingCoverageAKID,
	PolicyCoverageAKID,
	RiskLocationAKID,
	VehicleNumber,
	RiskType,
	CoverageType,
	PerilGroup,
	SubCoverageTypeCode,
	StandardInsuranceLineCode,
	CoverageVersion,
	Terrorism_risk_ind,
	StrategicProfitCenterAKId,
	InsuranceSegmentAKId,
	PolicyOfferingAKId,
	ProductAKId,
	InsuranceReferenceLineOfBusinessAKId
	FROM SQ_PremiumTransaction
),
LKP_CancelledCVG AS (
	SELECT
	RatingCoverageAKID,
	PolicyCoverageAKID
	FROM (
		declare @ProcessDate as datetime
		
		set @ProcessDate=DATEADD(s,-1,DATEADD(mm, DATEDIFF(m,-1,GETDATE()) + @{pipeline().parameters.NO_OF_MONTH},0))
		
		select distinct RatingCoverageAKID as RatingCoverageAKID,PolicyCoverageAKID as PolicyCoverageAKID from (
		select A.PolicyCoverageAKID,A.RatingCoverageAKID,First_Value(RatingCoverageCancellationDate) over(partition by PolicyCoverageAKID,A.RatingCoverageAKID order by A.EffectiveDate desc) RatingCoverageCancellationDate
		from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage A 
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction B
		on A.RatingCoverageAKID=B.RatingCoverageAKId
		and A.EffectiveDate=B.EffectiveDate
		where B.ReasonAmendedCode not in ('CWO','Claw Back')	
		AND B.PremiumTransactionBookedDate<=@ProcessDate
		AND B.PremiumTransactionEnteredDate<=@ProcessDate
		AND B.PremiumTransactionEffectiveDate<=@ProcessDate
		and B.SourceSystemID='DCT') A
		where RatingCoverageCancellationDate<=@ProcessDate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingCoverageAKID,PolicyCoverageAKID ORDER BY RatingCoverageAKID) = 1
),
LKP_CancelledPolicy AS (
	SELECT
	EDWPolicyAKId,
	pol_ak_id
	FROM (
		declare @ProcessDate as datetime
		
		set @ProcessDate=@{pipeline().parameters.PROCESS_DATE};
		
		select EDWPolicyAKId as EDWPolicyAKId from (
		select PCSD.EDWPolicyAKId, Rundate,max(Rundate) over(partition by PCSD.EDWPolicyAKId) MaxRundate,PolicyCancellationDate
		from @{pipeline().parameters.DATAMART_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCurrentStatusDim PCSD
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy p 
		on PCSD.EDWPolicyAKId = p.pol_ak_id 
		and p.source_sys_id = 'DCT' 
		and p.crrnt_snpsht_flag = 1
		where PCSD.RunDate<=@ProcessDate) A
		where RunDate=MaxRundate
		and PolicyCancellationDate > @ProcessDate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWPolicyAKId ORDER BY EDWPolicyAKId) = 1
),
LKP_InsuranceReferenceLineOfBusiness AS (
	SELECT
	InsuranceReferenceLineOfBusinessDescription,
	IN_InsuranceReferenceLineOfBusinessAKId,
	InsuranceReferenceLineOfBusinessAKId
	FROM (
		SELECT 
			InsuranceReferenceLineOfBusinessDescription,
			IN_InsuranceReferenceLineOfBusinessAKId,
			InsuranceReferenceLineOfBusinessAKId
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLineOfBusiness
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceReferenceLineOfBusinessAKId ORDER BY InsuranceReferenceLineOfBusinessDescription) = 1
),
LKP_PolicyOffering AS (
	SELECT
	PolicyOfferingDescription,
	InsuranceSegmentAKId,
	StrategicProfitCenterAKId,
	PolicyOfferingAKId
	FROM (
		select distinct PO.PolicyOfferingDescription as PolicyOfferingDescription,
		ISG.InsuranceSegmentAKId as InsuranceSegmentAKId,
		SPC.StrategicProfitCenterAKId as StrategicProfitCenterAKId,
		PO.PolicyOfferingAKId as PolicyOfferingAKId
		
		from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL 
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISG 
			on POL.InsuranceSegmentAKId = ISG.InsuranceSegmentAKId
			AND ISG.CurrentSnapshotFlag = 1
			AND ISG.InsuranceSegmentAbbreviation = 'CL'
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter SPC 
		       ON POL.StrategicProfitCenterAKId = SPC.StrategicProfitCenterAKId
			AND SPC.CurrentSnapshotFlag = 1
			AND SPC.StrategicProfitCenterDescription IN ('West Bend Commercial Lines', 'NSI')
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering PO 
		       ON POL.PolicyOfferingAKId = PO.PolicyOfferingAKId
			AND PO.CurrentSnapshotFlag = 1
		where POL.source_sys_id='DCT'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceSegmentAKId,StrategicProfitCenterAKId,PolicyOfferingAKId ORDER BY PolicyOfferingDescription) = 1
),
LKP_Product AS (
	SELECT
	ProductDescription,
	IN_ProductAKId,
	ProductAKId
	FROM (
		SELECT 
			ProductDescription,
			IN_ProductAKId,
			ProductAKId
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.Product
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProductAKId ORDER BY ProductDescription) = 1
),
EXP_Flag_Cancelled_Policy_Coverages AS (
	SELECT
	EXP_SRCData.SourceSystemID,
	EXP_SRCData.PremiumTransactionAKID,
	EXP_SRCData.pol_key,
	EXP_SRCData.LocationUnitNumber,
	EXP_SRCData.SubLocationUnitNumber,
	EXP_SRCData.PremiumTransactionAmount,
	EXP_SRCData.RatingCoverageAKID,
	EXP_SRCData.PolicyCoverageAKID,
	EXP_SRCData.RiskLocationAKID,
	EXP_SRCData.VehicleNumber,
	LKP_Product.ProductDescription AS LKP_ProductDescription,
	LKP_InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessDescription AS LKP_InsuranceReferenceLineOfBusinessDescription,
	LKP_PolicyOffering.PolicyOfferingDescription,
	EXP_SRCData.RiskType,
	EXP_SRCData.CoverageType,
	-- *INF*: Upper(CoverageType)
	Upper(CoverageType) AS Upper_CoverageType,
	EXP_SRCData.PerilGroup,
	EXP_SRCData.SubCoverageTypeCode,
	EXP_SRCData.StandardInsuranceLineCode,
	EXP_SRCData.CoverageVersion,
	EXP_SRCData.Terrorism_risk_ind,
	LKP_CancelledCVG.RatingCoverageAKID AS LKP_RatingCoverageAKID,
	LKP_CancelledPolicy.EDWPolicyAKId AS LKP_EDWPolicyAKId,
	-- *INF*: IIF((NOT ISNULL(LKP_RatingCoverageAKID)) OR ISNULL(LKP_EDWPolicyAKId) OR ISNULL(PolicyOfferingDescription) OR ISNULL(LKP_ProductDescription) OR ISNULL(LKP_InsuranceReferenceLineOfBusinessDescription),'1','0')
	IFF(
	    (LKP_RatingCoverageAKID IS NOT NULL)
	    or LKP_EDWPolicyAKId IS NULL
	    or PolicyOfferingDescription IS NULL
	    or LKP_ProductDescription IS NULL
	    or LKP_InsuranceReferenceLineOfBusinessDescription IS NULL,
	    '1',
	    '0'
	) AS FilterFlag,
	EXP_SRCData.StrategicProfitCenterAKId,
	EXP_SRCData.InsuranceSegmentAKId,
	EXP_SRCData.PolicyOfferingAKId,
	EXP_SRCData.ProductAKId,
	EXP_SRCData.InsuranceReferenceLineOfBusinessAKId,
	EXP_SRCData.WindCoverageFlag
	FROM EXP_SRCData
	LEFT JOIN LKP_CancelledCVG
	ON LKP_CancelledCVG.RatingCoverageAKID = EXP_SRCData.RatingCoverageAKID AND LKP_CancelledCVG.PolicyCoverageAKID = EXP_SRCData.PolicyCoverageAKID
	LEFT JOIN LKP_CancelledPolicy
	ON LKP_CancelledPolicy.EDWPolicyAKId = EXP_SRCData.pol_ak_id
	LEFT JOIN LKP_InsuranceReferenceLineOfBusiness
	ON LKP_InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessAKId = EXP_SRCData.InsuranceReferenceLineOfBusinessAKId
	LEFT JOIN LKP_PolicyOffering
	ON LKP_PolicyOffering.InsuranceSegmentAKId = EXP_SRCData.InsuranceSegmentAKId AND LKP_PolicyOffering.StrategicProfitCenterAKId = EXP_SRCData.StrategicProfitCenterAKId AND LKP_PolicyOffering.PolicyOfferingAKId = EXP_SRCData.PolicyOfferingAKId
	LEFT JOIN LKP_Product
	ON LKP_Product.ProductAKId = EXP_SRCData.ProductAKId
),
FIL_Cancelled_Policies_Coverages AS (
	SELECT
	SourceSystemID, 
	PremiumTransactionAKID, 
	pol_key, 
	LocationUnitNumber, 
	SubLocationUnitNumber, 
	WindCoverageFlag, 
	PremiumTransactionAmount, 
	RatingCoverageAKID, 
	PolicyCoverageAKID, 
	RiskLocationAKID, 
	VehicleNumber, 
	LKP_ProductDescription AS ProductDescription, 
	LKP_InsuranceReferenceLineOfBusinessDescription AS InsuranceReferenceLineOfBusinessDescription, 
	PolicyOfferingDescription, 
	RiskType, 
	CoverageType, 
	Upper_CoverageType, 
	PerilGroup, 
	SubCoverageTypeCode, 
	StandardInsuranceLineCode, 
	CoverageVersion, 
	Terrorism_risk_ind, 
	FilterFlag, 
	LKP_RatingCoverageAKID, 
	LKP_EDWPolicyAKId
	FROM EXP_Flag_Cancelled_Policy_Coverages
	WHERE FilterFlag='0'
),
JNR_Get_CVG_Info AS (SELECT
	FIL_Cancelled_Policies_Coverages.SourceSystemID, 
	FIL_Cancelled_Policies_Coverages.PremiumTransactionAKID, 
	FIL_Cancelled_Policies_Coverages.pol_key, 
	FIL_Cancelled_Policies_Coverages.LocationUnitNumber, 
	FIL_Cancelled_Policies_Coverages.SubLocationUnitNumber, 
	FIL_Cancelled_Policies_Coverages.WindCoverageFlag, 
	FIL_Cancelled_Policies_Coverages.PremiumTransactionAmount, 
	FIL_Cancelled_Policies_Coverages.RatingCoverageAKID, 
	FIL_Cancelled_Policies_Coverages.PolicyCoverageAKID, 
	FIL_Cancelled_Policies_Coverages.RiskLocationAKID, 
	FIL_Cancelled_Policies_Coverages.VehicleNumber, 
	FIL_Cancelled_Policies_Coverages.ProductDescription, 
	FIL_Cancelled_Policies_Coverages.InsuranceReferenceLineOfBusinessDescription, 
	FIL_Cancelled_Policies_Coverages.PolicyOfferingDescription, 
	FIL_Cancelled_Policies_Coverages.RiskType, 
	FIL_Cancelled_Policies_Coverages.CoverageType, 
	FIL_Cancelled_Policies_Coverages.Upper_CoverageType, 
	FIL_Cancelled_Policies_Coverages.PerilGroup, 
	FIL_Cancelled_Policies_Coverages.SubCoverageTypeCode, 
	FIL_Cancelled_Policies_Coverages.StandardInsuranceLineCode, 
	FIL_Cancelled_Policies_Coverages.CoverageVersion, 
	FIL_Cancelled_Policies_Coverages.Terrorism_risk_ind, 
	EXP_Src_Data.DctRiskTypeCode, 
	EXP_Src_Data.DctCoverageTypeCode, 
	EXP_Src_Data.DctPerilGroup, 
	EXP_Src_Data.DctSubCoverageTypeCode, 
	EXP_Src_Data.Upper_DctCoverageTypeCode, 
	EXP_Src_Data.InsuranceLineCode, 
	EXP_Src_Data.DctCoverageVersion, 
	EXP_Src_Data.CoverageDescription, 
	EXP_Src_Data.CoverageGroupDescription
	FROM FIL_Cancelled_Policies_Coverages
	INNER JOIN EXP_Src_Data
	ON EXP_Src_Data.DctRiskTypeCode = FIL_Cancelled_Policies_Coverages.RiskType AND EXP_Src_Data.Upper_DctCoverageTypeCode = FIL_Cancelled_Policies_Coverages.Upper_CoverageType AND EXP_Src_Data.DctPerilGroup = FIL_Cancelled_Policies_Coverages.PerilGroup AND EXP_Src_Data.DctSubCoverageTypeCode = FIL_Cancelled_Policies_Coverages.SubCoverageTypeCode AND EXP_Src_Data.InsuranceLineCode = FIL_Cancelled_Policies_Coverages.StandardInsuranceLineCode AND EXP_Src_Data.DctCoverageVersion = FIL_Cancelled_Policies_Coverages.CoverageVersion
),
LKP_SupCatastropheExposureBusinessType_BusinessType AS (
	SELECT
	BusinessType,
	AddToLocationPremiumFlag,
	SourceSystemId,
	PolicyOfferingDescription,
	ProductDescription,
	InsuranceReferenceLineOfBusinessDescription,
	CoverageGroupDescription,
	CoverageDescription,
	DctCoverageTypeCode
	FROM (
		SELECT 
			BusinessType,
			AddToLocationPremiumFlag,
			SourceSystemId,
			PolicyOfferingDescription,
			ProductDescription,
			InsuranceReferenceLineOfBusinessDescription,
			CoverageGroupDescription,
			CoverageDescription,
			DctCoverageTypeCode
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SupCatastropheExposureBusinessType
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SourceSystemId,PolicyOfferingDescription,ProductDescription,InsuranceReferenceLineOfBusinessDescription,CoverageGroupDescription,CoverageDescription,DctCoverageTypeCode ORDER BY BusinessType) = 1
),
EXP_Cal AS (
	SELECT
	JNR_Get_CVG_Info.SourceSystemID AS i_SourceSystemID,
	JNR_Get_CVG_Info.PremiumTransactionAKID AS i_PremiumTransactionAKID,
	JNR_Get_CVG_Info.pol_key AS i_pol_key,
	JNR_Get_CVG_Info.LocationUnitNumber AS i_LocationUnitNumber,
	JNR_Get_CVG_Info.SubLocationUnitNumber AS i_SubLocationUnitNumber,
	JNR_Get_CVG_Info.WindCoverageFlag AS i_WindCoverageFlag,
	JNR_Get_CVG_Info.PremiumTransactionAmount AS i_PremiumTransactionAmount,
	JNR_Get_CVG_Info.RatingCoverageAKID AS i_RatingCoverageAKID,
	JNR_Get_CVG_Info.PolicyCoverageAKID AS i_PolicyCoverageAKID,
	JNR_Get_CVG_Info.RiskLocationAKID AS i_RiskLocationAKID,
	JNR_Get_CVG_Info.VehicleNumber AS i_VehicleNumber,
	JNR_Get_CVG_Info.ProductDescription AS i_ProductDescription,
	i_ProductDescription AS o_ProductDescription,
	JNR_Get_CVG_Info.CoverageGroupDescription AS i_CoverageGroupDescription,
	JNR_Get_CVG_Info.InsuranceReferenceLineOfBusinessDescription AS i_InsuranceReferenceLineOfBusinessDescription,
	JNR_Get_CVG_Info.CoverageDescription AS i_CoverageDescription,
	JNR_Get_CVG_Info.DctCoverageTypeCode AS i_DctCoverageTypeCode,
	JNR_Get_CVG_Info.PolicyOfferingDescription AS i_PolicyOfferingDescription,
	LKP_SupCatastropheExposureBusinessType_BusinessType.BusinessType AS i_BusinessType,
	LKP_SupCatastropheExposureBusinessType_BusinessType.AddToLocationPremiumFlag AS i_AddToLocationPremiumFlag,
	JNR_Get_CVG_Info.Terrorism_risk_ind AS i_Terrorism_risk_ind,
	-- *INF*: --IIF( i_BusinessType <> '', i_BusinessType, 
	-- DECODE(TRUE, 
	-- NOT IsNull(i_BusinessType),i_BusinessType,
	-- i_ProductDescription='Commercial Auto' AND in(i_CoverageGroupDescription,'Comprehensive'), 'Commercial Auto',
	-- --i_ProductDescription = 'Commercial Property', 'Commercial Property',
	-- --i_ProductDescription = 'SBOP' AND i_InsuranceReferenceLineOfBusinessDescription <> 'Commercial Inland Marine', 'SBOP',
	-- --i_ProductDescription = 'SMARTbusiness' AND i_InsuranceReferenceLineOfBusinessDescription <> 'Commercial Inland Marine', 'SMARTbusiness',
	-- --i_InsuranceReferenceLineOfBusinessDescription = 'Commercial Inland Marine', 'Commercial Inland Marine',
	-- INSTR(i_CoverageGroupDescription,'Dealers Physical Damage')  > 0 AND INSTR(i_CoverageDescription,'Comprehensive') > 0 , 'Dealers Physical Damage',
	--  i_CoverageGroupDescription = 'Garagekeepers' AND INSTR(i_CoverageDescription,'Comprehensive') > 0 , 'Garagekeepers Liability',
	-- NULL)
	-- --)
	DECODE(
	    TRUE,
	    i_BusinessType IS NOT NULL, i_BusinessType,
	    i_ProductDescription = 'Commercial Auto' AND i_CoverageGroupDescription IN ('Comprehensive'), 'Commercial Auto',
	    REGEXP_INSTR(i_CoverageGroupDescription, 'Dealers Physical Damage') > 0 AND REGEXP_INSTR(i_CoverageDescription, 'Comprehensive') > 0, 'Dealers Physical Damage',
	    i_CoverageGroupDescription = 'Garagekeepers' AND REGEXP_INSTR(i_CoverageDescription, 'Comprehensive') > 0, 'Garagekeepers Liability',
	    NULL
	) AS v_BusinessType,
	-- *INF*: IIF(ISNULL(i_VehicleNumber),-1,i_VehicleNumber)
	IFF(i_VehicleNumber IS NULL, - 1, i_VehicleNumber) AS v_VehicleNumber,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	-- *INF*: IIF(ISNULL(i_SourceSystemID),'N/A',i_SourceSystemID)
	IFF(i_SourceSystemID IS NULL, 'N/A', i_SourceSystemID) AS o_SourceSystemID,
	-- *INF*: IIF(ISNULL(i_PremiumTransactionAKID),-1,i_PremiumTransactionAKID)
	IFF(i_PremiumTransactionAKID IS NULL, - 1, i_PremiumTransactionAKID) AS o_PremiumTransactionAKID,
	-- *INF*: IIF(ISNULL(i_pol_key),'N/A',i_pol_key)
	IFF(i_pol_key IS NULL, 'N/A', i_pol_key) AS o_PolicyKey,
	-- *INF*: IIF(ISNULL(i_LocationUnitNumber),'N/A',i_LocationUnitNumber)
	IFF(i_LocationUnitNumber IS NULL, 'N/A', i_LocationUnitNumber) AS o_LocationNumber,
	-- *INF*: IIF(ISNULL(i_SubLocationUnitNumber),'N/A',i_SubLocationUnitNumber)
	IFF(i_SubLocationUnitNumber IS NULL, 'N/A', i_SubLocationUnitNumber) AS o_BuildingNumber,
	-- *INF*: IIF(v_BusinessType<>'Commercial Auto', NULL, v_VehicleNumber)
	IFF(v_BusinessType <> 'Commercial Auto', NULL, v_VehicleNumber) AS o_VehicleNumber,
	v_BusinessType AS o_BusinessType,
	-- *INF*: IIF(ISNULL(i_CoverageGroupDescription),'N/A',i_CoverageGroupDescription)
	IFF(i_CoverageGroupDescription IS NULL, 'N/A', i_CoverageGroupDescription) AS o_CoverageGroupDescription,
	-- *INF*: IIF(ISNULL(i_CoverageDescription),'N/A',i_CoverageDescription)
	IFF(i_CoverageDescription IS NULL, 'N/A', i_CoverageDescription) AS o_CoverageDescription,
	-- *INF*: IIF(ISNULL(i_DctCoverageTypeCode),'N/A',i_DctCoverageTypeCode)
	IFF(i_DctCoverageTypeCode IS NULL, 'N/A', i_DctCoverageTypeCode) AS o_DCTCoverageTypeCode,
	-- *INF*: IIF(ISNULL(i_PolicyOfferingDescription),'N/A',i_PolicyOfferingDescription)
	IFF(i_PolicyOfferingDescription IS NULL, 'N/A', i_PolicyOfferingDescription) AS o_PolicyOfferingDescription,
	-- *INF*: DECODE(i_WindCoverageFlag, 'T', 1, 'F', 0, NULL)
	DECODE(
	    i_WindCoverageFlag,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_WindCoverageFlag,
	i_PremiumTransactionAmount AS o_PremiumTransactionAmount,
	-- *INF*: i_RatingCoverageAKID
	-- 
	-- 
	-- 
	-- 
	-- 
	-- ---IIF(IN(v_BusinessType, 'CIM','DPD','GKD'), NULL, v_RatingCoverageAKID)
	i_RatingCoverageAKID AS o_RatingCoverageAKID,
	-- *INF*: IIF(ISNULL(i_PolicyCoverageAKID),-1,i_PolicyCoverageAKID)
	IFF(i_PolicyCoverageAKID IS NULL, - 1, i_PolicyCoverageAKID) AS o_PolicyCoverageAKID,
	-- *INF*: IIF(ISNULL(i_RiskLocationAKID),-1,i_RiskLocationAKID)
	IFF(i_RiskLocationAKID IS NULL, - 1, i_RiskLocationAKID) AS o_RiskLocationAKID,
	-- *INF*: DECODE(i_AddToLocationPremiumFlag,
	-- 'T',1,
	-- 'F',0,
	-- IIF(i_CoverageDescription <> 'Plus Pak'  
	-- AND  i_CoverageGroupDescription <>'Terrorism'
	-- AND  i_CoverageGroupDescription <>'Equipment Breakdown' 
	-- AND  i_CoverageGroupDescription <>'Data Compromise',
	-- 1,
	-- 0))
	DECODE(
	    i_AddToLocationPremiumFlag,
	    'T', 1,
	    'F', 0,
	    IFF(
	        i_CoverageDescription <> 'Plus Pak'
	        and i_CoverageGroupDescription <> 'Terrorism'
	        and i_CoverageGroupDescription <> 'Equipment Breakdown'
	        and i_CoverageGroupDescription <> 'Data Compromise',
	        1,
	        0
	    )
	) AS o_AddToLocationPremiumFlag,
	-- *INF*: DECODE (i_Terrorism_risk_ind, '1', 1, 'Y', 1,'0',0,'N',0, NULL)
	-- 
	-- --DECODE(i_WindCoverageFlag, 'T', 1, 'F', 0, NULL)
	DECODE(
	    i_Terrorism_risk_ind,
	    '1', 1,
	    'Y', 1,
	    '0', 0,
	    'N', 0,
	    NULL
	) AS o_TerrorismRiskIndicator
	FROM JNR_Get_CVG_Info
	LEFT JOIN LKP_SupCatastropheExposureBusinessType_BusinessType
	ON LKP_SupCatastropheExposureBusinessType_BusinessType.SourceSystemId = JNR_Get_CVG_Info.SourceSystemID AND LKP_SupCatastropheExposureBusinessType_BusinessType.PolicyOfferingDescription = JNR_Get_CVG_Info.PolicyOfferingDescription AND LKP_SupCatastropheExposureBusinessType_BusinessType.ProductDescription = JNR_Get_CVG_Info.ProductDescription AND LKP_SupCatastropheExposureBusinessType_BusinessType.InsuranceReferenceLineOfBusinessDescription = JNR_Get_CVG_Info.InsuranceReferenceLineOfBusinessDescription AND LKP_SupCatastropheExposureBusinessType_BusinessType.CoverageGroupDescription = JNR_Get_CVG_Info.CoverageGroupDescription AND LKP_SupCatastropheExposureBusinessType_BusinessType.CoverageDescription = JNR_Get_CVG_Info.CoverageDescription AND LKP_SupCatastropheExposureBusinessType_BusinessType.DctCoverageTypeCode = JNR_Get_CVG_Info.DctCoverageTypeCode
),
FIL_NULL AS (
	SELECT
	o_AuditId AS AuditId, 
	o_CreatedDate AS CreatedDate, 
	o_ModifiedDate AS ModifiedDate, 
	o_SourceSystemID AS SourceSystemID, 
	o_PremiumTransactionAKID AS PremiumTransactionAKID, 
	o_PolicyKey AS PolicyKey, 
	o_LocationNumber AS LocationNumber, 
	o_BuildingNumber AS BuildingNumber, 
	o_VehicleNumber AS VehicleNumber, 
	o_BusinessType AS BusinessType, 
	o_CoverageGroupDescription AS CoverageGroupDescription, 
	o_CoverageDescription AS CoverageDescription, 
	o_DCTCoverageTypeCode AS DCTCoverageTypeCode, 
	o_PolicyOfferingDescription AS PolicyOfferingDescription, 
	o_WindCoverageFlag AS WindCoverageFlag, 
	o_PremiumTransactionAmount AS PremiumTransactionAmount, 
	o_RatingCoverageAKID AS RatingCoverageAKID, 
	o_PolicyCoverageAKID AS PolicyCoverageAKID, 
	o_RiskLocationAKID AS RiskLocationAKID, 
	o_AddToLocationPremiumFlag, 
	o_TerrorismRiskIndicator, 
	o_ProductDescription AS ProductDescription
	FROM EXP_Cal
	WHERE NOT ISNULL(BusinessType)
),
WorkCatastropheExposureTransaction AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction
	(AuditId, CreatedDate, ModifiedDate, SourceSystemId, PremiumTransactionAKID, PolicyKey, LocationNumber, BuildingNumber, VehicleNumber, BusinessType, CoverageGroupDescription, CoverageDescription, DCTCoverageTypeCode, PolicyOfferingDescription, WindCoverageFlag, PremiumTransactionAmount, RatingCoverageAKID, PolicyCoverageAKID, RiskLocationAKID, AddToLocationPremiumFlag, TerrorismRiskIndicator, ProductDescription)
	SELECT 
	AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	SourceSystemID AS SOURCESYSTEMID, 
	PREMIUMTRANSACTIONAKID, 
	POLICYKEY, 
	LOCATIONNUMBER, 
	BUILDINGNUMBER, 
	VEHICLENUMBER, 
	BUSINESSTYPE, 
	COVERAGEGROUPDESCRIPTION, 
	COVERAGEDESCRIPTION, 
	DCTCOVERAGETYPECODE, 
	POLICYOFFERINGDESCRIPTION, 
	WINDCOVERAGEFLAG, 
	PREMIUMTRANSACTIONAMOUNT, 
	RATINGCOVERAGEAKID, 
	POLICYCOVERAGEAKID, 
	RISKLOCATIONAKID, 
	o_AddToLocationPremiumFlag AS ADDTOLOCATIONPREMIUMFLAG, 
	o_TerrorismRiskIndicator AS TERRORISMRISKINDICATOR, 
	PRODUCTDESCRIPTION
	FROM FIL_NULL
),
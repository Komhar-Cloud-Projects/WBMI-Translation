WITH
LKP_WBCUPremiumDetailStage AS (
	SELECT
	Override,
	WBCUPremiumDetailId
	FROM (
		select WBCUPremiumDetailId as WBCUPremiumDetailId,
		CONVERT(varchar,Override) as Override
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCUPremiumDetailStage
		order by WBCUPremiumDetailId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WBCUPremiumDetailId ORDER BY Override) = 1
),
LKP_WorkDCTPolicy_EnteredDate AS (
	SELECT
	TransactionCreatedDate,
	PolicyNumber,
	PolicyVersionFormatted,
	TransactionEffectiveDate
	FROM (
		SELECT T.Policynumber AS Policynumber, 
		ISNULL(RIGHT('00'+convert(varchar(3),T.PolicyVersion),2),'00') AS PolicyVersionFormatted, 
		T.TransactionCreatedDate AS TransactionCreatedDate, 
		T.TransactionEffectiveDate AS TransactionEffectiveDate
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy T
		where T.TransactionState='committed'
		and T.PolicyStatus<>'Quote'
		and T.TransactionPurpose<>'Offset'
		ORDER BY T.Policynumber,ISNULL(RIGHT('00'+convert(varchar(3),T.PolicyVersion),2),'00'),T.TransactionCreatedDate,T.TransactionEffectiveDate
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber,PolicyVersionFormatted,TransactionCreatedDate,TransactionEffectiveDate ORDER BY TransactionCreatedDate) = 1
),
LKP_WorkDCTPolicy AS (
	SELECT
	BookedDate,
	PolicyNumber,
	PolicyVersionFormatted,
	TransactionCreatedDate,
	TransactionEffectiveDate
	FROM (
		SELECT T.Policynumber AS Policynumber, 
		ISNULL(RIGHT('00'+convert(varchar(3),T.PolicyVersion),2),'00') AS PolicyVersionFormatted, 
		T.TransactionCreatedDate AS TransactionCreatedDate, 
		T.TransactionEffectiveDate AS TransactionEffectiveDate, 
		CASE WHEN T.TransactionCreatedDate>T.TransactionEffectiveDate THEN T.TransactionCreatedDate ELSE T.TransactionEffectiveDate END AS BookedDate
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy T
		where T.TransactionState='committed'
		and T.PolicyStatus<>'Quote'
		and T.TransactionPurpose<>'Offset'
		ORDER BY T.Policynumber,ISNULL(RIGHT('00'+convert(varchar(3),T.PolicyVersion),2),'00'),T.TransactionCreatedDate,T.TransactionEffectiveDate
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber,PolicyVersionFormatted,TransactionCreatedDate,TransactionEffectiveDate ORDER BY BookedDate) = 1
),
LKP_ArchWorkPremiumTransaction AS (
	SELECT
	PremiumTransactionStageId,
	PremiumTransactionAKId
	FROM (
		SELECT 
			PremiumTransactionStageId,
			PremiumTransactionAKId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchWorkPremiumTransaction
		WHERE SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKId ORDER BY PremiumTransactionStageId) = 1
),
LKP_WorkDCTPolicy_Initial AS (
	SELECT
	BookedDate,
	PolicyNumber,
	PolicyVersionFormatted,
	TransactionCreatedDate,
	TransactionEffectiveDate
	FROM (
		SELECT T.Policynumber AS Policynumber, 
		ISNULL(RIGHT('00'+convert(varchar(3),T.PolicyVersion),2),'00') AS PolicyVersionFormatted, 
		T.TransactionCreatedDate AS TransactionCreatedDate, 
		T.TransactionEffectiveDate AS TransactionEffectiveDate, 
		CASE WHEN T.TransactionCreatedDate>T.TransactionEffectiveDate THEN T.TransactionCreatedDate ELSE T.TransactionEffectiveDate END AS BookedDate
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy T
		where T.TransactionState='committed'
		and T.PolicyStatus<>'Quote'
		and T.TransactionPurpose<>'Offset'
		ORDER BY T.Policynumber,ISNULL(RIGHT('00'+convert(varchar(3),T.PolicyVersion),2),'00'),T.TransactionCreatedDate,T.TransactionEffectiveDate
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber,PolicyVersionFormatted,TransactionCreatedDate,TransactionEffectiveDate ORDER BY BookedDate) = 1
),
LKP_DCModifierStaging_DCLine AS (
	SELECT
	Value,
	ObjectId
	FROM (
		SELECT 
			Value,
			ObjectId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCModifierStaging
		WHERE ObjectName='DC_Line' and Type='IRPM'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ObjectId ORDER BY Value) = 1
),
LKP_WorkDCTPolicy_EnteredDate_Initial AS (
	SELECT
	TransactionCreatedDate,
	PolicyNumber,
	PolicyVersionFormatted,
	TransactionEffectiveDate
	FROM (
		SELECT T.Policynumber AS Policynumber,  
		ISNULL(RIGHT('00'+convert(varchar(3),T.PolicyVersion),2),'00') AS PolicyVersionFormatted, 
		T.TransactionCreatedDate AS TransactionCreatedDate, 
		T.TransactionEffectiveDate AS TransactionEffectiveDate
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy T
		where T.TransactionState='committed'
		and T.PolicyStatus<>'Quote'
		and T.TransactionPurpose<>'Offset'
		ORDER BY T.Policynumber,ISNULL(RIGHT('00'+convert(varchar(3),T.PolicyVersion),2),'00'),T.TransactionCreatedDate,T.TransactionEffectiveDate
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber,PolicyVersionFormatted,TransactionCreatedDate,TransactionEffectiveDate ORDER BY TransactionCreatedDate) = 1
),
LKP_DCDeductibleStaging_Value AS (
	SELECT
	Value,
	CoverageId
	FROM (
		SELECT
		  CoverageId AS CoverageId,
		  MAX(Value) AS Value
		FROM (
		--BusinessOwners
		SELECT
		  b.CoverageId,
		  a.Type,
		  convert(money, a.Value) as Value
		FROM DCDeductibleStaging a
		INNER JOIN DCCoverageStaging b
		  ON a.ObjectId = b.ObjectId
		  AND a.SessionId = b.SessionId
		  AND a.ObjectName = 'DC_BP_Risk'
		  AND a.ObjectName = b.ObjectName
		  AND
		     CASE
		       WHEN ISNUMERIC(a.Value) = 1 THEN convert(money, a.Value)
		       ELSE 0
		     END > 0
		--CommercialCrime
		UNION ALL
		SELECT
		  c.CoverageId,
		  a.Type,
		  convert(money, a.Value) as Value
		FROM DCDeductibleStaging a
		INNER JOIN DCCRBuildingStaging b
		  ON a.SessionId = b.SessionId
		  AND a.ObjectId = b.CR_BuildingId
		  AND a.ObjectName = 'DC_CR_Building'
		INNER JOIN DCCRBuildingCoverageStaging c
		  ON c.SessionId = b.SessionId
		  AND c.CR_BuildingId = b.CR_BuildingId
		  AND
		     CASE
		       WHEN ISNUMERIC(a.Value) = 1 THEN convert(money, a.Value)
		       ELSE 0
		     END > 0
		UNION ALL
		SELECT
		  b.CoverageId,
		  a.Type,
		  convert(money, a.Value) as Value
		FROM DCDeductibleStaging a
		INNER JOIN DCCoverageStaging b
		  ON a.SessionId = b.SessionId
		  AND b.ObjectId = a.ObjectId
		  AND b.ObjectName = a.ObjectName
		  AND b.ObjectName = 'DC_CR_RiskCrime'
		  AND
		     CASE
		       WHEN ISNUMERIC(a.Value) = 1 THEN convert(money, a.Value)
		       ELSE 0
		     END > 0
		UNION ALL
		SELECT
		  b.CoverageId,
		  a.Type,
		  convert(money, a.Value) as Value
		FROM DCDeductibleStaging a
		INNER JOIN DCCoverageStaging b
		  ON a.SessionId = b.SessionId
		  AND b.ObjectId = a.ObjectId
		  AND b.ObjectName = a.ObjectName
		  AND b.ObjectName = 'DC_GL_Risk'
		  AND
		     CASE
		       WHEN ISNUMERIC(a.Value) = 1 THEN convert(money, a.Value)
		       ELSE 0
		     END > 0
		--Commercial Property
		UNION ALL
		SELECT
		  c.CoverageId,
		  a.Type,
		  convert(money, a.Value) as Value
		FROM DCDeductibleStaging a
		INNER JOIN DCCFBuilderStaging b
		  ON a.ObjectId = b.CF_BuilderId
		  AND a.SessionId = b.SessionId
		  AND a.ObjectName = 'DC_CF_Builder'
		INNER JOIN DCCoverageStaging c
		  ON c.SessionId = b.SessionId
		  AND b.CF_RiskId = c.ObjectId
		  AND c.ObjectName = 'DC_CF_Risk'
		  AND
		     CASE
		       WHEN ISNUMERIC(a.Value) = 1 THEN convert(money, a.Value)
		       ELSE 0
		     END > 0
		UNION ALL
		SELECT
		  b.CoverageId,
		  a.Type,
		  convert(money, a.Value) as Value
		FROM DCDeductibleStaging a
		INNER JOIN DCCoverageStaging b
		  ON a.SessionId = b.SessionId
		  AND b.ObjectId = a.ObjectId
		  AND b.ObjectName = a.ObjectName
		  AND b.ObjectName = 'DC_CF_Building'
		  AND
		     CASE
		       WHEN ISNUMERIC(a.Value) = 1 THEN convert(money, a.Value)
		       ELSE 0
		     END > 0
		UNION ALL
		SELECT
		  d.CoverageId,
		  a.Type,
		  convert(money, a.Value) as Value
		FROM DCDeductibleStaging a
		INNER JOIN DCCFBuildingStage b
		  ON a.ObjectId = b.CFBuildingId
		  AND a.SessionId = b.SessionId
		  AND a.ObjectName = 'DC_CF_Building'
		INNER JOIN DCCFRiskStaging c
		  ON c.SessionId = b.SessionId
		  AND c.CF_BuildingId = b.CFBuildingId
		INNER JOIN DCCoverageStaging d
		  ON c.SessionId = d.SessionId
		  AND c.CF_RiskId = d.ObjectId
		  AND d.ObjectName = 'DC_CF_Risk'
		  AND
		     CASE
		       WHEN ISNUMERIC(a.Value) = 1 THEN convert(money, a.Value)
		       ELSE 0
		     END > 0
		UNION ALL
		SELECT
		  c.CoverageId,
		  a.Type,
		  convert(money, a.Value) as Value
		FROM DCDeductibleStaging a
		INNER JOIN DCCFPropertyStaging b
		  ON a.ObjectId = b.CF_PropertyId
		  AND a.SessionId = b.SessionId
		  AND a.ObjectName = 'DC_CF_Property'
		INNER JOIN DCCoverageStaging c
		  ON c.SessionId = b.SessionId
		  AND b.CF_RiskId = c.ObjectId
		  AND c.ObjectName = 'DC_CF_Risk'
		  AND
		     CASE
		       WHEN ISNUMERIC(a.Value) = 1 THEN convert(money, a.Value)
		       ELSE 0
		     END > 0
		UNION ALL
		SELECT
		  c.CoverageId,
		  a.Type,
		  convert(money, a.Value) as Value
		FROM DCDeductibleStaging a
		INNER JOIN DCCFRatingGroupStaging b
		  ON a.ObjectId = b.CF_RatingGroupId
		  AND a.SessionId = b.SessionId
		  AND a.ObjectName = 'DC_CF_RatingGroup'
		INNER JOIN DCCoverageStaging c
		  ON c.SessionId = b.SessionId
		  AND b.CF_RiskId = c.ObjectId
		  AND c.ObjectName = 'DC_CF_Risk'
		  AND
		     CASE
		       WHEN ISNUMERIC(a.Value) = 1 THEN convert(money, a.Value)
		       ELSE 0
		     END > 0
		UNION ALL
		SELECT
		  b.CoverageId,
		  a.Type,
		  convert(money, a.Value) as Value
		FROM DCDeductibleStaging a
		INNER JOIN DCCoverageStaging b
		  ON a.SessionId = b.SessionId
		  AND b.ObjectId = a.ObjectId
		  AND b.ObjectName = a.ObjectName
		  AND b.ObjectName = 'DC_WC_StateTerm'
		  AND
		     CASE
		       WHEN ISNUMERIC(a.Value) = 1 THEN convert(money, a.Value)
		       ELSE 0
		     END > 0) temp
		GROUP BY CoverageId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY Value) = 1
),
SQ_DCStaging_Tables AS (
	SELECT
		WorkDCTCoverageTransaction.CoverageGUID,
		WorkDCTPolicy.TransactionCreatedDate,
		WorkDCTPolicy.TransactionPurpose,
		WorkDCTTransactionInsuranceLineLocationBridge.SessionId,
		WorkDCTTransactionInsuranceLineLocationBridge.CoverageId,
		WorkDCTTransactionInsuranceLineLocationBridge.LineId,
		WorkDCTTransactionInsuranceLineLocationBridge.IndividualRiskPremiumModification,
		WorkDCTPolicy.TransactionType,
		WorkDCTCoverageTransaction.CoverageType,
		WorkDCTTransactionInsuranceLineLocationBridge.CoverageForm,
		WorkDCTTransactionInsuranceLineLocationBridge.RiskType,
		WorkDCTTransactionInsuranceLineLocationBridge.Exposure,
		WorkDCTTransactionInsuranceLineLocationBridge.CommissionPercentage,
		WorkDCTInsuranceLine.LineType,
		WorkDCTPolicy.PolicyGUId,
		WorkDCTPolicy.PolicyEffectiveDate,
		WorkDCTPolicy.PolicyStatus,
		WorkDCTPolicy.TransactionEffectiveDate,
		WorkDCTPolicy.TransactionExpirationDate,
		WorkDCTPolicy.TransactionCancellationDate,
		WorkDCTLocation.LocationNumber,
		WorkDCTPolicy.PolicyVersion,
		WorkDCTPolicy.PolicyExpirationDate,
		WorkDCTCoverageTransaction.Premium,
		WorkDCTCoverageTransaction.Change,
		WorkDCTCoverageTransaction.Written,
		WorkDCTCoverageTransaction.Prior,
		WorkDCTPolicy.ReasonCode,
		WorkDCTPolicy.ReasonCodeCaption,
		WorkDCTTransactionInsuranceLineLocationBridge.ILFTableAssignmentCode,
		WorkDCTTransactionInsuranceLineLocationBridge.OccupancyType,
		WorkDCTCoverageTransaction.BaseRate,
		WorkDCTTransactionInsuranceLineLocationBridge.RetroactiveDate,
		WorkDCTInsuranceLine.ExperienceModifier,
		WorkDCTTransactionInsuranceLineLocationBridge.OrginalPackageModifier AS OriginalPackageModifier,
		WorkDCTCoverageTransaction.IncreasedLimitFactor,
		WorkDCTTransactionInsuranceLineLocationBridge.YearBuilt,
		WorkDCTTransactionInsuranceLineLocationBridge.ExperienceModEffectiveDate,
		WorkDCTInsuranceLine.FinalCommission,
		WorkDCTTransactionInsuranceLineLocationBridge.ConstructionCode,
		WorkDCTTransactionInsuranceLineLocationBridge.RateEffectiveDate,
		WorkDCTTransactionInsuranceLineLocationBridge.WindCoverageIndicator,
		WorkDCTCoverageTransaction.CoverageDeleteFlag,
		WorkDCTCoverageTransaction.ParentCoverageObjectId,
		WorkDCTCoverageTransaction.ParentCoverageObjectName,
		WorkDCTTransactionInsuranceLineLocationBridge.ExposureBasis,
		WorkDCTTransactionInsuranceLineLocationBridge.FullCoverageGlass,
		WorkDCTPolicy.TransactionCreatedUserId AS TransactionCreatedUserID,
		WorkDCTPolicy.EndorsedProcessedBy,
		WorkDCTPolicy.DeclaredEvent,
		WorkDCTPolicy.PolicyNumber,
		WorkDCTLocation.Territory,
		WorkDCTLocation.LocationXmlId,
		WorkDCTLocation.StateProvince
	FROM WorkDCTPolicy
	INNER JOIN WorkDCTLocation
	INNER JOIN WorkDCTInsuranceLine
	INNER JOIN WorkDCTTransactionInsuranceLineLocationBridge
	INNER JOIN WorkDCTCoverageTransaction
	ON WorkDCTCoverageTransaction.CoverageId=WorkDCTTransactionInsuranceLineLocationBridge.CoverageId
	and
	WorkDCTTransactionInsuranceLineLocationBridge.LocationAssociationId=WorkDCTLocation.LocationAssociationId
	and
	WorkDCTInsuranceLine.LineId=WorkDCTTransactionInsuranceLineLocationBridge.LineId
	and
	WorkDCTInsuranceLine.PolicyId=WorkDCTPolicy.PolicyId
	and
	WorkDCTPolicy.PolicyStatus<>'Quote'
	and
	WorkDCTPolicy.TransactionState='committed'
	--For line level Umbrella coverage, only load 'PolicyMinimum'
	and not (WorkDCTInsuranceLine.LineType='CommercialUmbrella'
	and
	WorkDCTCoverageTransaction.ParentCoverageObjectName='DC_Line'
	and WorkDCTCoverageTransaction.CoverageType<>'PolicyMinimum'
	)
	and
	WorkDCTPolicy.TransactionType @{pipeline().parameters.EXCLUDE_TTYPE}
),
LKP_DCWCStateTermStaging AS (
	SELECT
	PeriodStartDate,
	PeriodEndDate,
	WC_StateTermId,
	ObjectName
	FROM (
		SELECT PeriodStartDate as PeriodStartDate, 
		PeriodEndDate as PeriodEndDate, 
		WC_StateTermId as WC_StateTermId,
		'DC_WC_StateTerm' as ObjectName
		FROM DCWCStateTermStaging
		order by WC_StateTermId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WC_StateTermId,ObjectName ORDER BY PeriodStartDate) = 1
),
LKP_WBWCCoverageTermStage AS (
	SELECT
	PeriodStartDate,
	PeriodEndDate,
	CoverageId
	FROM (
		SELECT CT.PeriodStartDate as PeriodStartDate, 
		CT.PeriodEndDate as PeriodEndDate, 
		WBC.CoverageId as CoverageId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBWCCoverageTermStage CT
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCoverageStage WBC
		ON CT.WB_CoverageId=WBC.WBCoverageId
		ORDER BY WBC.CoverageId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY PeriodStartDate) = 1
),
LKP_WorkDCTInBalancePolicy AS (
	SELECT
	AccountingDate,
	ManualOverrideAccountingDate,
	SessionID,
	PolicyNumber
	FROM (
		SELECT 
			AccountingDate,
			ManualOverrideAccountingDate,
			SessionID,
			PolicyNumber
		FROM WorkDCTInBalancePolicy
		WHERE HistoryId=(select max(HistoryId) from WorkDCTInBalancePolicy B where B.SessionId=WorkDCTInBalancePolicy.SessionId)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SessionID,PolicyNumber ORDER BY AccountingDate) = 1
),
EXP_Default AS (
	SELECT
	LKP_WBWCCoverageTermStage.PeriodStartDate AS lkp_PeriodStartDate,
	LKP_WBWCCoverageTermStage.PeriodEndDate AS lkp_PeriodEndDate,
	LKP_DCWCStateTermStaging.PeriodStartDate AS i_PeriodStartDate,
	LKP_DCWCStateTermStaging.PeriodEndDate AS i_PeriodEndDate,
	SQ_DCStaging_Tables.SessionId AS i_SessionId,
	SQ_DCStaging_Tables.CoverageId AS i_CoverageId,
	SQ_DCStaging_Tables.TransactionType AS i_Type,
	SQ_DCStaging_Tables.CoverageGUID AS i_CoverageGUID,
	SQ_DCStaging_Tables.CoverageType AS i_CoverageType,
	SQ_DCStaging_Tables.CoverageForm AS i_CoverageForm,
	SQ_DCStaging_Tables.RiskType AS i_RiskType,
	SQ_DCStaging_Tables.Exposure AS i_Exposure,
	SQ_DCStaging_Tables.CommissionPercentage AS i_CommissionPercentage,
	SQ_DCStaging_Tables.LineType AS i_LineType,
	SQ_DCStaging_Tables.PolicyGUId AS i_Id,
	SQ_DCStaging_Tables.PolicyEffectiveDate AS i_PolicyEffectiveDate,
	SQ_DCStaging_Tables.PolicyStatus,
	SQ_DCStaging_Tables.TransactionEffectiveDate AS i_TransactionEffectiveDate,
	SQ_DCStaging_Tables.TransactionExpirationDate AS i_TransactionExpirationDate,
	SQ_DCStaging_Tables.TransactionCreatedDate AS i_TransactionCreatedDate,
	SQ_DCStaging_Tables.TransactionCancellationDate AS i_TransactionCancellationDate,
	SQ_DCStaging_Tables.LocationNumber AS i_LocationNumber,
	SQ_DCStaging_Tables.PolicyVersion AS i_PolicyVersion,
	SQ_DCStaging_Tables.PolicyExpirationDate AS i_PolicyExpirationDate,
	SQ_DCStaging_Tables.Premium AS i_Premium,
	SQ_DCStaging_Tables.Change AS i_Change,
	SQ_DCStaging_Tables.Written AS i_Written,
	SQ_DCStaging_Tables.Prior AS i_Prior,
	SQ_DCStaging_Tables.ReasonCode AS i_Code,
	SQ_DCStaging_Tables.ReasonCodeCaption AS i_CodeCaption,
	SQ_DCStaging_Tables.ILFTableAssignmentCode AS i_ILFTableAssignmentCode,
	SQ_DCStaging_Tables.OccupancyType AS i_OccupancyType,
	SQ_DCStaging_Tables.BaseRate AS i_BaseRate,
	SQ_DCStaging_Tables.RetroactiveDate AS i_RetroactiveDate,
	SQ_DCStaging_Tables.ExperienceModifier AS i_ExperienceMod_DCModifier,
	SQ_DCStaging_Tables.OriginalPackageModifier AS i_OriginalPackageModifier,
	SQ_DCStaging_Tables.IncreasedLimitFactor AS i_IncreasedLimitFactor,
	SQ_DCStaging_Tables.YearBuilt AS i_YearBuilt,
	SQ_DCStaging_Tables.ExperienceModEffectiveDate AS i_ExperienceModEffectiveDate,
	SQ_DCStaging_Tables.FinalCommission AS i_FinalCommission,
	-- *INF*: IIF(ISNULL(i_FinalCommission),0,i_FinalCommission)
	IFF(i_FinalCommission IS NULL, 0, i_FinalCommission) AS v_FinalCommission,
	SQ_DCStaging_Tables.ConstructionCode AS i_ConstructionCode,
	SQ_DCStaging_Tables.RateEffectiveDate AS i_RateEffectiveDate,
	SQ_DCStaging_Tables.LineId AS i_LineId,
	SQ_DCStaging_Tables.IndividualRiskPremiumModification AS i_Value,
	SQ_DCStaging_Tables.WindCoverageIndicator AS i_WindCoverageFlag,
	SQ_DCStaging_Tables.CoverageDeleteFlag AS i_CoverageDeleteFlag,
	SQ_DCStaging_Tables.TransactionPurpose AS i_TransactionPurpose,
	SQ_DCStaging_Tables.ParentCoverageObjectId AS i_ParentCoverageObjectId,
	SQ_DCStaging_Tables.ParentCoverageObjectName AS i_ParentCoverageObjectName,
	SQ_DCStaging_Tables.ExposureBasis AS i_ExposureBasis,
	SQ_DCStaging_Tables.FullCoverageGlass AS i_FullCoverageGlass,
	LKP_WorkDCTInBalancePolicy.AccountingDate AS i_AccountingDate,
	SQ_DCStaging_Tables.PolicyNumber AS i_PolicyNumber,
	SQ_DCStaging_Tables.Territory AS i_Territory,
	SQ_DCStaging_Tables.LocationXmlId AS i_LocationXmlId,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_PeriodStartDate) AND ISNULL(lkp_PeriodStartDate),i_TransactionEffectiveDate,
	-- NOT ISNULL(lkp_PeriodStartDate),GREATEST(lkp_PeriodStartDate,i_TransactionEffectiveDate),
	-- GREATEST(i_PeriodStartDate,i_TransactionEffectiveDate))
	DECODE(
	    TRUE,
	    i_PeriodStartDate IS NULL AND lkp_PeriodStartDate IS NULL, i_TransactionEffectiveDate,
	    lkp_PeriodStartDate IS NOT NULL, GREATEST(lkp_PeriodStartDate, i_TransactionEffectiveDate),
	    GREATEST(i_PeriodStartDate, i_TransactionEffectiveDate)
	) AS v_EffectiveDate,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_PeriodEndDate) AND ISNULL(lkp_PeriodEndDate),i_TransactionExpirationDate,
	-- NOT ISNULL(lkp_PeriodEndDate),LEAST(lkp_PeriodEndDate,i_TransactionExpirationDate),
	-- LEAST(i_PeriodEndDate,i_TransactionExpirationDate))
	DECODE(
	    TRUE,
	    i_PeriodEndDate IS NULL AND lkp_PeriodEndDate IS NULL, i_TransactionExpirationDate,
	    lkp_PeriodEndDate IS NOT NULL, LEAST(lkp_PeriodEndDate, i_TransactionExpirationDate),
	    LEAST(i_PeriodEndDate, i_TransactionExpirationDate)
	) AS v_ExpirationDate,
	-- *INF*: IIF(
	--   ISNULL(i_CommissionPercentage),
	--   0,
	--   i_CommissionPercentage
	-- )
	IFF(i_CommissionPercentage IS NULL, 0, i_CommissionPercentage) AS v_CommissionPercentage,
	-- *INF*: IIF(ISNULL(i_Exposure),0,i_Exposure)
	IFF(i_Exposure IS NULL, 0, i_Exposure) AS v_Exposure,
	-- *INF*: IIF(
	--   ISNULL(i_CoverageForm) OR LENGTH(LTRIM(RTRIM(i_CoverageForm)))=0,
	--   'N/A',
	--   LTRIM(RTRIM(i_CoverageForm))
	-- )
	IFF(
	    i_CoverageForm IS NULL OR LENGTH(LTRIM(RTRIM(i_CoverageForm))) = 0, 'N/A',
	    LTRIM(RTRIM(i_CoverageForm))
	) AS v_CoverageForm,
	-- *INF*: IIF(ISNULL(i_Id) or IS_SPACES(i_Id) or LENGTH(i_Id)=0,'N/A',LTRIM(RTRIM(i_Id)))
	IFF(
	    i_Id IS NULL or LENGTH(i_Id)>0 AND TRIM(i_Id)='' or LENGTH(i_Id) = 0, 'N/A',
	    LTRIM(RTRIM(i_Id))
	) AS v_Id,
	-- *INF*: IIF(ISNULL(i_PolicyVersion),'00',LPAD(TO_CHAR(i_PolicyVersion),2,'0'))
	IFF(i_PolicyVersion IS NULL, '00', LPAD(TO_CHAR(i_PolicyVersion), 2, '0')) AS v_PolicyVersion,
	-- *INF*: IIF(ISNULL(i_PolicyNumber) or IS_SPACES(i_PolicyNumber) or LENGTH(i_PolicyNumber)=0, 'N/A', LTRIM(RTRIM(i_PolicyNumber)))
	IFF(
	    i_PolicyNumber IS NULL
	    or LENGTH(i_PolicyNumber)>0
	    and TRIM(i_PolicyNumber)=''
	    or LENGTH(i_PolicyNumber) = 0,
	    'N/A',
	    LTRIM(RTRIM(i_PolicyNumber))
	) AS v_PolicyNumber,
	-- *INF*: IIF(ISNULL(i_Territory) OR IS_SPACES(i_Territory) OR LENGTH(i_Territory)=0,'N/A',LTRIM(RTRIM(i_Territory)))
	IFF(
	    i_Territory IS NULL
	    or LENGTH(i_Territory)>0
	    and TRIM(i_Territory)=''
	    or LENGTH(i_Territory) = 0,
	    'N/A',
	    LTRIM(RTRIM(i_Territory))
	) AS v_Territory,
	-- *INF*: IIF(ISNULL(i_LocationXmlId) OR IS_SPACES(i_LocationXmlId) OR LENGTH(i_LocationXmlId)=0,'N/A',LTRIM(RTRIM(i_LocationXmlId)))
	IFF(
	    i_LocationXmlId IS NULL
	    or LENGTH(i_LocationXmlId)>0
	    and TRIM(i_LocationXmlId)=''
	    or LENGTH(i_LocationXmlId) = 0,
	    'N/A',
	    LTRIM(RTRIM(i_LocationXmlId))
	) AS v_LocationXmlID,
	-- *INF*: IIF(ISNULL(i_LocationNumber) or IS_SPACES(i_LocationNumber) or LENGTH(i_LocationNumber)=0,'0000', LPAD(LTRIM(RTRIM (i_LocationNumber)), 4, '0')) 
	IFF(
	    i_LocationNumber IS NULL
	    or LENGTH(i_LocationNumber)>0
	    and TRIM(i_LocationNumber)=''
	    or LENGTH(i_LocationNumber) = 0,
	    '0000',
	    LPAD(LTRIM(RTRIM(i_LocationNumber)), 4, '0')
	) AS v_LocationNumber,
	-- *INF*: IIF(
	--   ISNULL(i_LineType) OR LENGTH(LTRIM(RTRIM(i_LineType)))=0,
	--   'N/A',
	--   LTRIM(RTRIM(i_LineType))
	-- )
	IFF(
	    i_LineType IS NULL OR LENGTH(LTRIM(RTRIM(i_LineType))) = 0, 'N/A', LTRIM(RTRIM(i_LineType))
	) AS v_LineType,
	-- *INF*: IIF(ISNULL(i_PolicyEffectiveDate),TO_DATE('2100-12-31 23:59:59.000','YYYY-MM-DD HH24:MI:SS.MS'),i_PolicyEffectiveDate)
	IFF(
	    i_PolicyEffectiveDate IS NULL,
	    TO_TIMESTAMP('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.MS'),
	    i_PolicyEffectiveDate
	) AS v_PolicyEffectiveDate,
	-- *INF*: IIF(
	--   ISNULL(i_RiskType) OR LENGTH(LTRIM(RTRIM(i_RiskType)))=0,
	--   'N/A',
	--   LTRIM(RTRIM(i_RiskType))
	-- )
	IFF(
	    i_RiskType IS NULL OR LENGTH(LTRIM(RTRIM(i_RiskType))) = 0, 'N/A', LTRIM(RTRIM(i_RiskType))
	) AS v_RiskType,
	-- *INF*: IIF(ISNULL(i_CoverageType) OR IS_SPACES(i_CoverageType) OR LENGTH(i_CoverageType)=0,'N/A',LTRIM(RTRIM(i_CoverageType)))
	IFF(
	    i_CoverageType IS NULL
	    or LENGTH(i_CoverageType)>0
	    and TRIM(i_CoverageType)=''
	    or LENGTH(i_CoverageType) = 0,
	    'N/A',
	    LTRIM(RTRIM(i_CoverageType))
	) AS v_CoverageType,
	-- *INF*: IIF(ISNULL(i_ILFTableAssignmentCode) OR IS_SPACES(i_ILFTableAssignmentCode) OR LENGTH(i_ILFTableAssignmentCode)=0, 'N/A', i_ILFTableAssignmentCode)
	IFF(
	    i_ILFTableAssignmentCode IS NULL
	    or LENGTH(i_ILFTableAssignmentCode)>0
	    and TRIM(i_ILFTableAssignmentCode)=''
	    or LENGTH(i_ILFTableAssignmentCode) = 0,
	    'N/A',
	    i_ILFTableAssignmentCode
	) AS v_ILFTableAssignmentCode,
	-- *INF*: IIF(ISNULL(i_Value) OR i_Value='N/A',:LKP.LKP_DCMODIFIERSTAGING_DCLINE(i_LineId),i_Value)
	IFF(
	    i_Value IS NULL OR i_Value = 'N/A', LKP_DCMODIFIERSTAGING_DCLINE_i_LineId.Value, i_Value
	) AS v_IndividualRiskPremiumModification,
	-- *INF*: IIF(ISNULL(i_TransactionPurpose) or IS_SPACES(i_TransactionPurpose) or LENGTH(i_TransactionPurpose)=0,'N/A', LTRIM(RTRIM (i_TransactionPurpose))) 
	IFF(
	    i_TransactionPurpose IS NULL
	    or LENGTH(i_TransactionPurpose)>0
	    and TRIM(i_TransactionPurpose)=''
	    or LENGTH(i_TransactionPurpose) = 0,
	    'N/A',
	    LTRIM(RTRIM(i_TransactionPurpose))
	) AS v_TransactionPurpose,
	-- *INF*: IIF(v_TransactionPurpose!='Offset',i_TransactionCreatedDate,:LKP.LKP_WORKDCTPOLICY_ENTEREDDATE_INITIAL(v_PolicyNumber,v_PolicyVersion,i_TransactionCreatedDate,i_TransactionEffectiveDate))
	IFF(
	    v_TransactionPurpose != 'Offset', i_TransactionCreatedDate,
	    LKP_WORKDCTPOLICY_ENTEREDDATE_INITIAL_v_PolicyNumber_v_PolicyVersion_i_TransactionCreatedDate_i_TransactionEffectiveDate.TransactionCreatedDate
	) AS v_PremiumTransactionEnteredDate_Initial,
	-- *INF*: IIF(NOT ISNULL(v_PremiumTransactionEnteredDate_Initial),v_PremiumTransactionEnteredDate_Initial,:LKP.LKP_WORKDCTPOLICY_ENTEREDDATE(v_PolicyNumber,v_PolicyVersion,i_TransactionCreatedDate,i_TransactionEffectiveDate))
	IFF(
	    v_PremiumTransactionEnteredDate_Initial IS NOT NULL, v_PremiumTransactionEnteredDate_Initial,
	    LKP_WORKDCTPOLICY_ENTEREDDATE_v_PolicyNumber_v_PolicyVersion_i_TransactionCreatedDate_i_TransactionEffectiveDate.TransactionCreatedDate
	) AS v_PremiumTransactionEnteredDate,
	-- *INF*: IIF(ISNULL(i_AccountingDate), TO_DATE('1800-01-01', 'YYYY-MM-DD'), TRUNC(i_AccountingDate, 'MM'))
	-- 
	-- --The lookup to get the AccountingDate may fail, we have to do regular null check and assign the default date '1800-01-01' to pass the job. Then support team need to investigate why and correct the data.
	-- 
	-- --IIF(
	-- --  i_EffectiveDate<i_CreatedDate,
	-- --  TRUNC(i_CreatedDate,'MM'),
	-- --  TRUNC(i_EffectiveDate,'MM')
	-- --)
	IFF(
	    i_AccountingDate IS NULL, TO_TIMESTAMP('1800-01-01', 'YYYY-MM-DD'),
	    CAST(TRUNC(i_AccountingDate, 'MONTH') AS TIMESTAMP_NTZ(0))
	) AS v_BookedDate,
	-- *INF*: IIF(v_TransactionPurpose!='Offset',v_BookedDate,GREATEST(TRUNC(:LKP.LKP_WORKDCTPOLICY_INITIAL(v_PolicyNumber,v_PolicyVersion,i_TransactionCreatedDate,i_TransactionEffectiveDate),'MM'),v_BookedDate))
	IFF(
	    v_TransactionPurpose != 'Offset', v_BookedDate,
	    GREATEST(CAST(TRUNC(LKP_WORKDCTPOLICY_INITIAL_v_PolicyNumber_v_PolicyVersion_i_TransactionCreatedDate_i_TransactionEffectiveDate.BookedDate, 'MONTH') AS TIMESTAMP_NTZ(0)), v_BookedDate)
	) AS v_PremiumTransactionBookedDate_Initial,
	-- *INF*: IIF(NOT ISNULL(v_PremiumTransactionBookedDate_Initial),v_PremiumTransactionBookedDate_Initial,GREATEST(TRUNC(:LKP.LKP_WORKDCTPOLICY(v_PolicyNumber,v_PolicyVersion,i_TransactionCreatedDate,i_TransactionEffectiveDate),'MM'),v_BookedDate))
	IFF(
	    v_PremiumTransactionBookedDate_Initial IS NOT NULL, v_PremiumTransactionBookedDate_Initial,
	    GREATEST(CAST(TRUNC(LKP_WORKDCTPOLICY_v_PolicyNumber_v_PolicyVersion_i_TransactionCreatedDate_i_TransactionEffectiveDate.BookedDate, 'MONTH') AS TIMESTAMP_NTZ(0)), v_BookedDate)
	) AS v_PremiumTransactionBookedDate,
	-- *INF*: IIF(ISNULL(i_ExposureBasis) OR IS_SPACES(i_ExposureBasis) OR LENGTH(i_ExposureBasis)=0,'N/A',LTRIM(RTRIM(i_ExposureBasis)))
	IFF(
	    i_ExposureBasis IS NULL
	    or LENGTH(i_ExposureBasis)>0
	    and TRIM(i_ExposureBasis)=''
	    or LENGTH(i_ExposureBasis) = 0,
	    'N/A',
	    LTRIM(RTRIM(i_ExposureBasis))
	) AS v_ExposureBasis,
	v_PolicyVersion AS o_PolicyVersion,
	-- *INF*: IIF(ISNULL(i_Written),0,i_Written)
	IFF(i_Written IS NULL, 0, i_Written) AS o_Written,
	-- *INF*: LTRIM(RTRIM(i_Type))
	LTRIM(RTRIM(i_Type)) AS o_Type,
	-- *INF*: IIF(ISNULL(i_TransactionCreatedDate),TO_DATE('2100-12-31 23:59:59.000','YYYY-MM-DD HH24:MI:SS.MS'),i_TransactionCreatedDate)
	IFF(
	    i_TransactionCreatedDate IS NULL,
	    TO_TIMESTAMP('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.MS'),
	    i_TransactionCreatedDate
	) AS o_CreatedDate,
	-- *INF*: IIF(ISNULL(v_EffectiveDate),TO_DATE('21001231235959' , 'YYYYMMDDHH24MISS'),v_EffectiveDate)
	IFF(
	    v_EffectiveDate IS NULL, TO_TIMESTAMP('21001231235959', 'YYYYMMDDHH24MISS'), v_EffectiveDate
	) AS o_TransactionEffectiveDate,
	-- *INF*: IIF(ISNULL(v_ExpirationDate),TO_DATE('21001231235959' , 'YYYYMMDDHH24MISS'),v_ExpirationDate) 
	IFF(
	    v_ExpirationDate IS NULL, TO_TIMESTAMP('21001231235959', 'YYYYMMDDHH24MISS'),
	    v_ExpirationDate
	) AS o_TransactionExpirationDate,
	-- *INF*: IIF(ISNULL(i_TransactionCancellationDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_TransactionCancellationDate)
	IFF(
	    i_TransactionCancellationDate IS NULL, TO_TIMESTAMP('21001231235959', 'YYYYMMDDHH24MISS'),
	    i_TransactionCancellationDate
	) AS o_TransactionCancellationDate,
	-- *INF*: IIF(NOT ISNULL(v_PremiumTransactionEnteredDate),v_PremiumTransactionEnteredDate,i_TransactionCreatedDate)
	IFF(
	    v_PremiumTransactionEnteredDate IS NOT NULL, v_PremiumTransactionEnteredDate,
	    i_TransactionCreatedDate
	) AS o_PremiumTransactionEnteredDate,
	-- *INF*: IIF(NOT ISNULL(v_PremiumTransactionBookedDate),v_PremiumTransactionBookedDate,v_BookedDate)
	IFF(
	    v_PremiumTransactionBookedDate IS NOT NULL, v_PremiumTransactionBookedDate, v_BookedDate
	) AS o_PremiumTransactionBookedDate,
	v_PolicyEffectiveDate AS o_PolicyEffectiveDate,
	-- *INF*: IIF(ISNULL(i_PolicyExpirationDate),TO_DATE('2100-12-31 23:59:59.000','YYYY-MM-DD HH24:MI:SS.MS'),i_PolicyExpirationDate)
	IFF(
	    i_PolicyExpirationDate IS NULL,
	    TO_TIMESTAMP('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.MS'),
	    i_PolicyExpirationDate
	) AS o_PolicyExpirationDate,
	-- *INF*: IIF(ISNULL(i_Premium),0,i_Premium)
	IFF(i_Premium IS NULL, 0, i_Premium) AS o_Premium,
	-- *INF*: IIF(ISNULL(i_Exposure),0,i_Exposure)
	IFF(i_Exposure IS NULL, 0, i_Exposure) AS o_Exposure,
	-- *INF*: IIF(ISNULL(i_Change),0,i_Change)
	IFF(i_Change IS NULL, 0, i_Change) AS o_Change,
	-- *INF*: IIF(ISNULL(i_Prior),0,i_Prior)
	IFF(i_Prior IS NULL, 0, i_Prior) AS o_Prior,
	'D' AS o_PremiumType,
	-- *INF*: IIF(ISNULL(i_Code) or IS_SPACES(i_Code) or LENGTH(i_Code)=0,'N/A', LTRIM(RTRIM (i_Code))) 
	IFF(
	    i_Code IS NULL or LENGTH(i_Code)>0 AND TRIM(i_Code)='' or LENGTH(i_Code) = 0, 'N/A',
	    LTRIM(RTRIM(i_Code))
	) AS v_ReasonAmendedCode,
	-- *INF*: --updated as per PROD-15109, removed translation for Audit transaction types
	-- 
	-- DECODE(TRUE,
	-- IN(LTRIM(RTRIM(i_Type)),'FinalReporting','VoidFinalReporting'),'OX1', 
	-- :UDF.DEFAULT_VALUE_FOR_STRINGS(i_Code)
	-- )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(i_Type)) IN ('FinalReporting','VoidFinalReporting'), 'OX1',
	    UDF_DEFAULT_VALUE_FOR_STRINGS(i_Code)
	) AS o_ReasonAmendedCode,
	-- *INF*: IIF(ISNULL(i_CodeCaption) or IS_SPACES(i_CodeCaption) or LENGTH(i_CodeCaption)=0,'N/A', LTRIM(RTRIM (i_CodeCaption))) 
	IFF(
	    i_CodeCaption IS NULL
	    or LENGTH(i_CodeCaption)>0
	    and TRIM(i_CodeCaption)=''
	    or LENGTH(i_CodeCaption) = 0,
	    'N/A',
	    LTRIM(RTRIM(i_CodeCaption))
	) AS o_CodeCaption,
	-- *INF*: IIF(ISNULL(i_CoverageGUID) OR IS_SPACES(i_CoverageGUID) OR LENGTH(i_CoverageGUID)=0, 'N/A', LTRIM(RTRIM(i_CoverageGUID)))
	IFF(
	    i_CoverageGUID IS NULL
	    or LENGTH(i_CoverageGUID)>0
	    and TRIM(i_CoverageGUID)=''
	    or LENGTH(i_CoverageGUID) = 0,
	    'N/A',
	    LTRIM(RTRIM(i_CoverageGUID))
	) AS o_CoverageGUID,
	-- *INF*: IIF(ISNULL(:LKP.LKP_DCDEDUCTIBLESTAGING_VALUE(i_CoverageId)),'0',:LKP.LKP_DCDEDUCTIBLESTAGING_VALUE(i_CoverageId))
	IFF(
	    LKP_DCDEDUCTIBLESTAGING_VALUE_i_CoverageId.Value IS NULL, '0',
	    LKP_DCDEDUCTIBLESTAGING_VALUE_i_CoverageId.Value
	) AS o_DeductibleAmount,
	-- *INF*: IIF(ISNULL(i_RetroactiveDate), TO_DATE('2100-12-31', 'YYYY-MM-DD'), TRUNC(i_RetroactiveDate, 'DD'))
	IFF(
	    i_RetroactiveDate IS NULL, TO_TIMESTAMP('2100-12-31', 'YYYY-MM-DD'),
	    CAST(TRUNC(i_RetroactiveDate, 'DAY') AS TIMESTAMP_NTZ(0))
	) AS o_RetroactiveDate,
	-- *INF*: 0
	-- --we set Experience Modification Factor to zero for all transactions. For workers Comp policies, it will be correctly determined from the base rate of the ExperienceModification coverage transactions and suitably applied to all applicable underlying transactions
	-- 
	-- --IIF(ISNULL(i_ExperienceMod_DCModifier) OR IS_SPACES(i_ExperienceMod_DCModifier) OR LENGTH(i_ExperienceMod_DCModifier)=0 OR IS_NUMBER(LTRIM(RTRIM(i_ExperienceMod_DCModifier)))=0,  0,  TO_DECIMAL(LTRIM(RTRIM(i_ExperienceMod_DCModifier))))
	-- 
	-- --DECODE(TRUE, ISNULL(i_ExperienceMod_DCModifier) OR IS_SPACES(i_ExperienceMod_DCModifier) OR LENGTH(i_ExperienceMod_DCModifier)=0,  0,  LOWER(i_LineType)='workerscompensation' AND IS_NUMBER(LTRIM(RTRIM(i_ExperienceMod_DCModifier))), TO_DECIMAL(LTRIM(RTRIM(i_ExperienceMod_DCModifier))), 0)
	0 AS o_ExperienceModificationFactor,
	-- *INF*: IIF(i_CoverageType = 'ExperienceModification', i_ExperienceModEffectiveDate,TO_DATE('2100-12-31 00:00:00', 'YYYY-MM-DD HH24:MI:SS') ) 
	-- -- we retain the correctly derived Experience Modification Effective date from DC_WC_StateTerm for ExperienceModification. All other applicable transactions will receive the date in a later mapping, while we default them and all other transactions for now
	IFF(
	    i_CoverageType = 'ExperienceModification', i_ExperienceModEffectiveDate,
	    TO_TIMESTAMP('2100-12-31 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
	) AS o_ExperienceModificationEffectiveDate,
	-- *INF*: IIF(ISNULL(i_OriginalPackageModifier), 0, i_OriginalPackageModifier)
	IFF(i_OriginalPackageModifier IS NULL, 0, i_OriginalPackageModifier) AS o_PackageModificationAdjustmentFactor,
	-- *INF*: IIF(ISNULL(i_OccupancyType) OR IS_SPACES(i_OccupancyType) OR LENGTH(i_OccupancyType)=0,'N/A',LTRIM(RTRIM(i_OccupancyType)))
	IFF(
	    i_OccupancyType IS NULL
	    or LENGTH(i_OccupancyType)>0
	    and TRIM(i_OccupancyType)=''
	    or LENGTH(i_OccupancyType) = 0,
	    'N/A',
	    LTRIM(RTRIM(i_OccupancyType))
	) AS o_PackageModificationAdjustmentGroupCode,
	-- *INF*: IIF(ISNULL(i_IncreasedLimitFactor) OR IS_SPACES(i_IncreasedLimitFactor) OR LENGTH(i_IncreasedLimitFactor)=0 OR IS_NUMBER(LTRIM(RTRIM(i_IncreasedLimitFactor)))=0, 0, TO_DECIMAL(LTRIM(RTRIM(i_IncreasedLimitFactor))))
	IFF(
	    i_IncreasedLimitFactor IS NULL
	    or LENGTH(i_IncreasedLimitFactor)>0
	    and TRIM(i_IncreasedLimitFactor)=''
	    or LENGTH(i_IncreasedLimitFactor) = 0
	    or REGEXP_LIKE(LTRIM(RTRIM(i_IncreasedLimitFactor)), '^[0-9]+$') = 0,
	    0,
	    CAST(LTRIM(RTRIM(i_IncreasedLimitFactor)) AS FLOAT)
	) AS o_IncreasedLimitFactor,
	-- *INF*: DECODE(TRUE, v_ILFTableAssignmentCode='N/A', 'N/A', v_CoverageType='PremisesOperations', SUBSTR(v_ILFTableAssignmentCode,1,1), v_CoverageType='ProductsCompletedOps' and SUBSTR(v_ILFTableAssignmentCode,2,1) = '-', 'I', SUBSTR(v_ILFTableAssignmentCode,2,1))
	DECODE(
	    TRUE,
	    v_ILFTableAssignmentCode = 'N/A', 'N/A',
	    v_CoverageType = 'PremisesOperations', SUBSTR(v_ILFTableAssignmentCode, 1, 1),
	    v_CoverageType = 'ProductsCompletedOps' and SUBSTR(v_ILFTableAssignmentCode, 2, 1) = '-', 'I',
	    SUBSTR(v_ILFTableAssignmentCode, 2, 1)
	) AS o_IncreasedLimitGroupCode,
	-- *INF*: IIF(ISNULL(i_YearBuilt), '0000', TO_CHAR(i_YearBuilt))
	IFF(i_YearBuilt IS NULL, '0000', TO_CHAR(i_YearBuilt)) AS o_YearBuilt,
	-- *INF*: IIF(ISNULL(i_CommissionPercentage) or i_CommissionPercentage=-1,v_FinalCommission, v_CommissionPercentage)
	-- --IIF(ISNULL(i_FinalCommission), 0 , i_FinalCommission)
	IFF(
	    i_CommissionPercentage IS NULL or i_CommissionPercentage = - 1, v_FinalCommission,
	    v_CommissionPercentage
	) AS o_AgencyActualCommissionRate,
	-- *INF*: ROUND(IIF(NOT ISNULL(i_BaseRate), i_BaseRate, 0),4)
	ROUND(
	    IFF(
	        i_BaseRate IS NOT NULL, i_BaseRate, 0
	    ), 4) AS o_BaseRate,
	-- *INF*: IIF(ISNULL(i_ConstructionCode) OR IS_SPACES(i_ConstructionCode) OR LENGTH(i_ConstructionCode)=0, 'N/A', LTRIM(RTRIM(i_ConstructionCode)))
	IFF(
	    i_ConstructionCode IS NULL
	    or LENGTH(i_ConstructionCode)>0
	    and TRIM(i_ConstructionCode)=''
	    or LENGTH(i_ConstructionCode) = 0,
	    'N/A',
	    LTRIM(RTRIM(i_ConstructionCode))
	) AS o_ConstructionCode,
	-- *INF*: IIF(NOT ISNULL(i_RateEffectiveDate),i_RateEffectiveDate,TO_DATE('18000101','YYYYMMDD'))
	IFF(
	    i_RateEffectiveDate IS NOT NULL, i_RateEffectiveDate, TO_TIMESTAMP('18000101', 'YYYYMMDD')
	) AS o_StateRatingEffectiveDate,
	i_CoverageId AS o_CoverageId,
	-- *INF*: IIF(IS_NUMBER(v_IndividualRiskPremiumModification),TO_DECIMAL(v_IndividualRiskPremiumModification,4),0)
	IFF(
	    REGEXP_LIKE(v_IndividualRiskPremiumModification, '^[0-9]+$'),
	    CAST(v_IndividualRiskPremiumModification AS FLOAT),
	    0
	) AS o_IndividualRiskPremiumModification,
	-- *INF*: DECODE(i_WindCoverageFlag,'T','1','0')
	DECODE(
	    i_WindCoverageFlag,
	    'T', '1',
	    '0'
	) AS o_WindCoverageFlag,
	-- *INF*: IIF(NOT ISNULL(i_CoverageDeleteFlag),i_CoverageDeleteFlag,'0')
	IFF(i_CoverageDeleteFlag IS NOT NULL, i_CoverageDeleteFlag, '0') AS o_CoverageDeleteFlag,
	v_TransactionPurpose AS o_TransactionPurpose,
	i_ParentCoverageObjectId AS o_ParentCoverageObjectId,
	-- *INF*: LTRIM(RTRIM(i_ParentCoverageObjectName))
	LTRIM(RTRIM(i_ParentCoverageObjectName)) AS o_ParentCoverageObjectName,
	-- *INF*: LTRIM(RTRIM(i_CoverageType))
	LTRIM(RTRIM(i_CoverageType)) AS o_CoverageType,
	v_ExposureBasis AS o_ExposureBasis,
	-- *INF*: IIF(i_FullCoverageGlass='T','F','D')
	IFF(i_FullCoverageGlass = 'T', 'F', 'D') AS o_DeductibleBasis,
	SQ_DCStaging_Tables.TransactionCreatedUserID,
	-- *INF*: IIF(ISNULL(TransactionCreatedUserID),'N/A',TransactionCreatedUserID)
	IFF(TransactionCreatedUserID IS NULL, 'N/A', TransactionCreatedUserID) AS o_TransactionCreatedUserId,
	SQ_DCStaging_Tables.EndorsedProcessedBy,
	SQ_DCStaging_Tables.DeclaredEvent,
	-- *INF*: IIF(ISNULL(EndorsedProcessedBy),'N/A',EndorsedProcessedBy)
	IFF(EndorsedProcessedBy IS NULL, 'N/A', EndorsedProcessedBy) AS o_ServiceCentreName,
	v_PolicyNumber||v_PolicyVersion AS o_Policy_Key,
	v_LineType AS o_LineType,
	v_LocationNumber AS o_LocationNumber,
	v_Territory AS o_Territory,
	SQ_DCStaging_Tables.StateProvince,
	v_LocationXmlID AS o_LocationXmlId
	FROM SQ_DCStaging_Tables
	LEFT JOIN LKP_DCWCStateTermStaging
	ON LKP_DCWCStateTermStaging.WC_StateTermId = SQ_DCStaging_Tables.ParentCoverageObjectId AND LKP_DCWCStateTermStaging.ObjectName = SQ_DCStaging_Tables.ParentCoverageObjectName
	LEFT JOIN LKP_WBWCCoverageTermStage
	ON LKP_WBWCCoverageTermStage.CoverageId = SQ_DCStaging_Tables.CoverageId
	LEFT JOIN LKP_WorkDCTInBalancePolicy
	ON LKP_WorkDCTInBalancePolicy.SessionID = SQ_DCStaging_Tables.SessionId AND LKP_WorkDCTInBalancePolicy.PolicyNumber = SQ_DCStaging_Tables.PolicyNumber
	LEFT JOIN LKP_DCMODIFIERSTAGING_DCLINE LKP_DCMODIFIERSTAGING_DCLINE_i_LineId
	ON LKP_DCMODIFIERSTAGING_DCLINE_i_LineId.ObjectId = i_LineId

	LEFT JOIN LKP_WORKDCTPOLICY_ENTEREDDATE_INITIAL LKP_WORKDCTPOLICY_ENTEREDDATE_INITIAL_v_PolicyNumber_v_PolicyVersion_i_TransactionCreatedDate_i_TransactionEffectiveDate
	ON LKP_WORKDCTPOLICY_ENTEREDDATE_INITIAL_v_PolicyNumber_v_PolicyVersion_i_TransactionCreatedDate_i_TransactionEffectiveDate.PolicyNumber = v_PolicyNumber
	AND LKP_WORKDCTPOLICY_ENTEREDDATE_INITIAL_v_PolicyNumber_v_PolicyVersion_i_TransactionCreatedDate_i_TransactionEffectiveDate.PolicyVersionFormatted = v_PolicyVersion
	AND LKP_WORKDCTPOLICY_ENTEREDDATE_INITIAL_v_PolicyNumber_v_PolicyVersion_i_TransactionCreatedDate_i_TransactionEffectiveDate.TransactionCreatedDate = i_TransactionCreatedDate
	AND LKP_WORKDCTPOLICY_ENTEREDDATE_INITIAL_v_PolicyNumber_v_PolicyVersion_i_TransactionCreatedDate_i_TransactionEffectiveDate.TransactionEffectiveDate = i_TransactionEffectiveDate

	LEFT JOIN LKP_WORKDCTPOLICY_ENTEREDDATE LKP_WORKDCTPOLICY_ENTEREDDATE_v_PolicyNumber_v_PolicyVersion_i_TransactionCreatedDate_i_TransactionEffectiveDate
	ON LKP_WORKDCTPOLICY_ENTEREDDATE_v_PolicyNumber_v_PolicyVersion_i_TransactionCreatedDate_i_TransactionEffectiveDate.PolicyNumber = v_PolicyNumber
	AND LKP_WORKDCTPOLICY_ENTEREDDATE_v_PolicyNumber_v_PolicyVersion_i_TransactionCreatedDate_i_TransactionEffectiveDate.PolicyVersionFormatted = v_PolicyVersion
	AND LKP_WORKDCTPOLICY_ENTEREDDATE_v_PolicyNumber_v_PolicyVersion_i_TransactionCreatedDate_i_TransactionEffectiveDate.TransactionCreatedDate = i_TransactionCreatedDate
	AND LKP_WORKDCTPOLICY_ENTEREDDATE_v_PolicyNumber_v_PolicyVersion_i_TransactionCreatedDate_i_TransactionEffectiveDate.TransactionEffectiveDate = i_TransactionEffectiveDate

	LEFT JOIN LKP_WORKDCTPOLICY_INITIAL LKP_WORKDCTPOLICY_INITIAL_v_PolicyNumber_v_PolicyVersion_i_TransactionCreatedDate_i_TransactionEffectiveDate
	ON LKP_WORKDCTPOLICY_INITIAL_v_PolicyNumber_v_PolicyVersion_i_TransactionCreatedDate_i_TransactionEffectiveDate.PolicyNumber = v_PolicyNumber
	AND LKP_WORKDCTPOLICY_INITIAL_v_PolicyNumber_v_PolicyVersion_i_TransactionCreatedDate_i_TransactionEffectiveDate.PolicyVersionFormatted = v_PolicyVersion
	AND LKP_WORKDCTPOLICY_INITIAL_v_PolicyNumber_v_PolicyVersion_i_TransactionCreatedDate_i_TransactionEffectiveDate.TransactionCreatedDate = i_TransactionCreatedDate
	AND LKP_WORKDCTPOLICY_INITIAL_v_PolicyNumber_v_PolicyVersion_i_TransactionCreatedDate_i_TransactionEffectiveDate.TransactionEffectiveDate = i_TransactionEffectiveDate

	LEFT JOIN LKP_WORKDCTPOLICY LKP_WORKDCTPOLICY_v_PolicyNumber_v_PolicyVersion_i_TransactionCreatedDate_i_TransactionEffectiveDate
	ON LKP_WORKDCTPOLICY_v_PolicyNumber_v_PolicyVersion_i_TransactionCreatedDate_i_TransactionEffectiveDate.PolicyNumber = v_PolicyNumber
	AND LKP_WORKDCTPOLICY_v_PolicyNumber_v_PolicyVersion_i_TransactionCreatedDate_i_TransactionEffectiveDate.PolicyVersionFormatted = v_PolicyVersion
	AND LKP_WORKDCTPOLICY_v_PolicyNumber_v_PolicyVersion_i_TransactionCreatedDate_i_TransactionEffectiveDate.TransactionCreatedDate = i_TransactionCreatedDate
	AND LKP_WORKDCTPOLICY_v_PolicyNumber_v_PolicyVersion_i_TransactionCreatedDate_i_TransactionEffectiveDate.TransactionEffectiveDate = i_TransactionEffectiveDate

	LEFT JOIN LKP_DCDEDUCTIBLESTAGING_VALUE LKP_DCDEDUCTIBLESTAGING_VALUE_i_CoverageId
	ON LKP_DCDEDUCTIBLESTAGING_VALUE_i_CoverageId.CoverageId = i_CoverageId

),
LKP_PolicyAKID AS (
	SELECT
	pol_ak_id,
	i_Policy_Key,
	pol_key
	FROM (
		SELECT 
			pol_ak_id,
			i_Policy_Key,
			pol_key
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Policy
		WHERE crrnt_snpsht_flag='1' and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}' and exists ( select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT where WCT.PolicyNumber=pol_num and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol_mod)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_ak_id) = 1
),
AGG_Remove_Duplicate AS (
	SELECT
	EXP_Default.PolicyStatus,
	EXP_Default.o_PolicyVersion AS PolicyVersion,
	EXP_Default.o_Written AS Written,
	EXP_Default.o_Type AS Type,
	EXP_Default.o_CoverageGUID AS CoverageGUID,
	EXP_Default.o_CreatedDate AS CreatedDate,
	EXP_Default.o_TransactionPurpose AS TransactionPurpose,
	EXP_Default.o_TransactionEffectiveDate AS TransactionEffectiveDate,
	EXP_Default.o_TransactionExpirationDate AS TransactionExpirationDate,
	EXP_Default.o_TransactionCancellationDate AS TransactionCancellationDate,
	EXP_Default.o_PremiumTransactionEnteredDate AS PremiumTransactionEnteredDate,
	EXP_Default.o_PremiumTransactionBookedDate AS PremiumTransactionBookedDate,
	EXP_Default.o_PolicyEffectiveDate AS PolicyEffectiveDate,
	EXP_Default.o_PolicyExpirationDate AS PolicyExpirationDate,
	EXP_Default.o_Premium AS Premium,
	EXP_Default.o_Exposure AS Exposure,
	EXP_Default.o_Change AS Change,
	EXP_Default.o_Prior AS Prior,
	EXP_Default.o_PremiumType AS PremiumType,
	EXP_Default.o_ReasonAmendedCode AS ReasonAmendedCode,
	EXP_Default.o_CodeCaption AS CodeCaption,
	EXP_Default.o_DeductibleAmount AS DeductibleAmount,
	EXP_Default.o_RetroactiveDate AS RetroactiveDate,
	EXP_Default.o_ExperienceModificationFactor AS ExperienceModificationFactor,
	EXP_Default.o_ExperienceModificationEffectiveDate AS ExperienceModificationEffectiveDate,
	EXP_Default.o_PackageModificationAdjustmentFactor AS PackageModificationAdjustmentFactor,
	EXP_Default.o_PackageModificationAdjustmentGroupCode AS PackageModificationAdjustmentGroupCode,
	EXP_Default.o_IncreasedLimitFactor AS IncreasedLimitFactor,
	EXP_Default.o_IncreasedLimitGroupCode AS IncreasedLimitGroupCode,
	EXP_Default.o_YearBuilt AS YearBuilt,
	EXP_Default.o_AgencyActualCommissionRate AS AgencyActualCommissionRate,
	EXP_Default.o_BaseRate AS BaseRate,
	EXP_Default.o_ConstructionCode AS ConstructionCode,
	EXP_Default.o_StateRatingEffectiveDate AS StateRatingEffectiveDate,
	EXP_Default.o_CoverageId AS CoverageId,
	EXP_Default.o_IndividualRiskPremiumModification AS IndividualRiskPremiumModification,
	EXP_Default.o_WindCoverageFlag AS WindCoverageFlag,
	EXP_Default.o_CoverageDeleteFlag AS CoverageDeleteFlag,
	EXP_Default.o_ParentCoverageObjectId AS ParentCoverageObjectId,
	EXP_Default.o_ParentCoverageObjectName AS ParentCoverageObjectName,
	EXP_Default.o_CoverageType AS CoverageType,
	EXP_Default.o_ExposureBasis AS ExposureBasis,
	EXP_Default.o_DeductibleBasis AS DeductibleBasis,
	EXP_Default.o_TransactionCreatedUserId AS TransactionCreatedUserId,
	EXP_Default.o_ServiceCentreName AS ServiceCentreName,
	LKP_PolicyAKID.pol_ak_id AS i_pol_ak_id,
	-- *INF*: iif(isnull(i_pol_ak_id),-1,i_pol_ak_id)
	IFF(i_pol_ak_id IS NULL, - 1, i_pol_ak_id) AS o_pol_ak_id,
	EXP_Default.o_Policy_Key AS Policy_Key,
	EXP_Default.DeclaredEvent
	FROM EXP_Default
	LEFT JOIN LKP_PolicyAKID
	ON LKP_PolicyAKID.pol_key = EXP_Default.o_Policy_Key
	GROUP BY CoverageGUID, CreatedDate, TransactionPurpose, o_pol_ak_id
),
LKP_DCCoverageStaging AS (
	SELECT
	CoverageDeleteFlag,
	Type,
	i_PolicyKey,
	Pol_Key,
	CoverageGUID,
	EffectiveDate,
	CreatedDate,
	OffsetCreatedDate
	FROM (
		SELECT T.PolicyNumber+case when len(T.policyversion)=1 then '0'+CONVERT(varchar(1) ,T.policyversion) else CONVERT(varchar(2),T.policyversion) end as Pol_Key, A.CoverageGUID AS CoverageGUID, 
		CASE WHEN CT.PeriodStartDate>T.TransactionEffectiveDate THEN CT.PeriodStartDate WHEN ST.PeriodStartDate>T.TransactionEffectiveDate THEN ST.PeriodStartDate ELSE T.TransactionEffectiveDate END AS EffectiveDate, 
		T.TransactionCreatedDate AS CreatedDate, 
		ISNULL(F.OffsetCreatedDate, '2100-12-31 23:59:59') AS OffsetCreatedDate,
		A.CoverageDeleteFlag AS CoverageDeleteFlag,
		T.TransactionType AS Type,
		A.CoverageId AS CoverageId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTCoverageTransaction A
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy T
		ON A.SessionId=T.SessionId
		AND T.TransactionState='committed'
		AND T.PolicyStatus<>'Quote'
		AND T.TransactionPurpose<>'Offset'
		AND T.TransactionType @{pipeline().parameters.EXCLUDE_TTYPE}
		LEFT JOIN 
		(SELECT F.PolicyNumber,F.PolicyVersion,F.TransactionCreatedDate,
		MIN(ISNULL(O.TransactionCreatedDate,O1.TransactionCreatedDate)) OffsetCreatedDate
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy F
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy O
		ON O.PolicyNumber=F.PolicyNumber
		AND ISNULL(O.PolicyVersion,0)=ISNULL(F.PolicyVersion,0)
		AND O.TransactionCreatedDate>F.TransactionCreatedDate
		AND O.TransactionEffectiveDate<F.TransactionEffectiveDate
		AND O.TransactionState='committed'
		AND O.PolicyStatus<>'Quote'
		AND O.TransactionPurpose<>'Offset'
		LEFT  JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy O1
		ON O1.PolicyNumber=F.PolicyNumber
		AND ISNULL(O1.PolicyVersion,0)=ISNULL(F.PolicyVersion,0)
		AND O1.TransactionCreatedDate>F.TransactionCreatedDate
		AND O1.TransactionEffectiveDate<=F.TransactionEffectiveDate
		AND O1.TransactionState='committed'
		AND O1.PolicyStatus<>'Quote'
		AND O1.TransactionPurpose<>'Offset'
		WHERE F.TransactionState='committed'
		AND F.PolicyStatus<>'Quote'
		AND F.TransactionPurpose='Offset'
		GROUP BY F.PolicyNumber,F.PolicyVersion,F.TransactionCreatedDate) F
		ON T.PolicyNumber=F.PolicyNumber
		AND ISNULL(T.PolicyVersion,0)=ISNULL(F.PolicyVersion,0)
		AND F.TransactionCreatedDate=T.TransactionCreatedDate
		LEFT  JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCoverageStage WBC
		ON WBC.CoverageId=A.CoverageId
		LEFT  JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBWCCoverageTermStage CT
		ON CT.WB_CoverageId=WBC.WBCoverageId
		LEFT  JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCWCStateTermStaging ST
		ON ST.WC_StateTermId=A.ParentCoverageObjectId
		AND A.ParentCoverageObjectName='DC_WC_StateTerm'
		ORDER BY T.PolicyNumber+case when len(T.policyversion)=1 then '0'+CONVERT(varchar(1) ,T.policyversion) else CONVERT(varchar(2),T.policyversion) end ,A.CoverageGUID,CASE WHEN CT.PeriodStartDate>T.TransactionEffectiveDate THEN CT.PeriodStartDate WHEN ST.PeriodStartDate>T.TransactionEffectiveDate THEN ST.PeriodStartDate ELSE T.TransactionEffectiveDate END, T.TransactionCreatedDate, ISNULL(F.OffsetCreatedDate, '2100-12-31 23:59:59'), A.CoverageId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Pol_Key,CoverageGUID,EffectiveDate,CreatedDate,OffsetCreatedDate ORDER BY CoverageDeleteFlag DESC) = 1
),
LKP_RatingCoverage AS (
	SELECT
	RatingCoverageCancellationDate,
	PremiumTransactionCode,
	Exposure,
	CoverageGUID,
	PolicyAKID,
	TEffectiveDate,
	TCreatedDate,
	OffsetCreatedDate
	FROM (
		SELECT DISTINCT a.RatingCoverageCancellationDate as RatingCoverageCancellationDate,
		a.CoverageGUID as CoverageGUID,
		b.PremiumTransactionCode as PremiumTransactionCode,
		b.PremiumTransactionEffectiveDate as TEffectiveDate,
		b.PremiumTransactionEnteredDate as TCreatedDate,
		ISNULL(c.PremiumTransactionEnteredDate,'2100-12-31 23:59:59') as OffsetCreatedDate,
		a.Exposure as Exposure,
		pc.PolicyAKID as PolicyAKID
		FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage pc 
		INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage a on PC.PolicyCoverageAKID=a.PolicyCoverageAKID AND pc.SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		AND pc.CurrentSnapshotFlag=1
		INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction b on a.RatingCoverageAKId=b.RatingCoverageAKid
		AND b.EffectiveDate=a.EffectiveDate AND b.SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND not b.OffsetOnsetCode in ('Offset','Deprecated')
		LEFT JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction c on c.RatingCoverageAKId=b.RatingCoverageAKid
		AND c.EffectiveDate=a.EffectiveDate AND c.SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND c.OffsetOnsetCode='Deprecated'
		INNER  JOIN (
		select DISTINCT WCT.CoverageGUId from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTCoverageTransaction WCT) WCT
		on WCT.CoverageGUID=a.CoverageGUID
		order by pc.PolicyAKID,a.CoverageGUID,b.PremiumTransactionEffectiveDate,b.PremiumTransactionEnteredDate,ISNULL(c.PremiumTransactionEnteredDate,'2100-12-31 23:59:59')
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageGUID,PolicyAKID,TEffectiveDate,TCreatedDate,OffsetCreatedDate ORDER BY RatingCoverageCancellationDate DESC) = 1
),
mplt_get_RiskLocation_PolicyCoverage_RatingCoverage_Akids_Hierarchy AS (WITH
	Input_Policy AS (
		
	),
	EXP_Get_Value AS (
		SELECT
		PolicyAKID,
		CoverageGuid,
		TransactionCreatedDate,
		-- *INF*: IIF(ISNULL(PolicyAKID),-1,PolicyAKID)
		IFF(PolicyAKID IS NULL, - 1, PolicyAKID) AS o_PolicyAKID
		FROM Input_Policy
	),
	LKP_Policy_Heirarchy_With_Date AS (
		SELECT
		RiskLocationAKID,
		PolicyCoverageAKID,
		PolicyAKID,
		CoverageGUID,
		EffectiveDate,
		LocationUnitNumber,
		RatingCoverageCancellationDate,
		RatingCoverageAKID,
		RatingCoverageKey,
		RatingCoverageHashKey,
		RatingCoverageId,
		RatingCoverageEffectivedate,
		RatingCoverageExpirationdate,
		ClassCode,
		CoverageType,
		ProductAbbreviation
		FROM (
			SELECT R.RiskLocationAKID AS RiskLocationAKID,
				PC.PolicyCoverageAKID AS PolicyCoverageAKID,
				R.PolicyAKID AS PolicyAKID,
				RC.CoverageGUID AS CoverageGUID,
				RC.EffectiveDate AS EffectiveDate,
				R.LocationUnitNumber AS LocationUnitNumber,
				RC.RatingCoverageCancellationDate AS RatingCoverageCancellationDate,
				RC.RatingCoverageAKID AS RatingCoverageAKID,
				RC.RatingCoverageKey AS RatingCoverageKey,
				RC.RatingCoverageHashKey AS RatingCoverageHashKey,
				RC.RatingCoverageid AS RatingCoverageid,
				RC.RatingCoverageEffectivedate AS RatingCoverageEffectivedate,
				RC.RatingCoverageExpirationdate AS RatingCoverageExpirationdate ,
				RC.ClassCode AS ClassCode,
				RC.coveragetype as coveragetype,
				PR.ProductAbbreviation as ProductAbbreviation
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC
			INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC
				ON RC.PolicyCoverageAKID = PC.PolicyCoverageAKID
					AND RC.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
			INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation R
				ON R.RiskLocationAKID = PC.RiskLocationAKID
						AND PC.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
					AND PC.CurrentSnapshotFlag = 1
					AND R.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
					AND R.CurrentSnapshotFlag = 1
			 LEFT JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}. product PR on
				PR.productakid=RC.productakid and PR.CurrentSnapshotFlag=1
					where EXISTS (
						SELECT 1
						FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol
						INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT
							ON WCT.PolicyNumber = pol.pol_num
								AND ISNULL(RIGHT('00' + convert(VARCHAR(3), WCT.PolicyVersion), 2), '00') = pol.pol_mod
								AND pol.crrnt_snpsht_flag = 1
								AND R.PolicyAKId = pol.pol_ak_id
						)
			ORDER BY PC.Policyakid,RC.Coverageguid,RC.Createddate,RC.effectivedate--
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID,CoverageGUID,EffectiveDate ORDER BY RiskLocationAKID DESC) = 1
	),
	LKP_Policy_Heirarchy_Without_Date AS (
		SELECT
		RiskLocationAKID,
		PolicyCoverageAKID,
		PolicyAKID,
		CoverageGUID,
		LocationUnitNumber,
		RatingCoverageCancellationDate,
		RatingCoverageAKID,
		RatingCoverageKey,
		RatingCoverageHashKey,
		RatingCoverageId,
		RatingCoverageEffectivedate,
		RatingCoverageExpirationdate,
		CoverageType,
		ProductAbbreviation
		FROM (
			SELECT R.RiskLocationAKID AS RiskLocationAKID,
				PC.PolicyCoverageAKID AS PolicyCoverageAKID,
				R.PolicyAKID AS PolicyAKID,
				RC.CoverageGUID AS CoverageGUID,
				RC.EffectiveDate AS EffectiveDate,
				R.LocationUnitNumber AS LocationUnitNumber,
				RC.RatingCoverageCancellationDate AS RatingCoverageCancellationDate,
				RC.RatingCoverageAKID AS RatingCoverageAKID,
				RC.RatingCoverageKey AS RatingCoverageKey,
				RC.RatingCoverageHashKey AS RatingCoverageHashKey,
				RC.RatingCoverageid AS RatingCoverageid,
				RC.RatingCoverageEffectivedate AS RatingCoverageEffectivedate,
				RC.RatingCoverageExpirationdate AS RatingCoverageExpirationdate ,
					RC.coveragetype as coveragetype,
				PR.ProductAbbreviation as ProductAbbreviation
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC
			INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC
				ON RC.PolicyCoverageAKID = PC.PolicyCoverageAKID
					AND RC.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
			INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation R
				ON R.RiskLocationAKID = PC.RiskLocationAKID
						AND PC.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
					AND PC.CurrentSnapshotFlag = 1
					AND R.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
					AND R.CurrentSnapshotFlag = 1
			LEFT JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}. product PR on
				PR.productakid=RC.productakid and PR.CurrentSnapshotFlag=1
					where EXISTS (
						SELECT 1
						FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol
						INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT
							ON WCT.PolicyNumber = pol.pol_num
								AND ISNULL(RIGHT('00' + convert(VARCHAR(3), WCT.PolicyVersion), 2), '00') = pol.pol_mod
								AND pol.crrnt_snpsht_flag = 1
								AND R.PolicyAKId = pol.pol_ak_id
						)
			ORDER BY PC.Policyakid,RC.Coverageguid,RC.Createddate,RC.effectivedate--
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID,CoverageGUID ORDER BY RiskLocationAKID DESC) = 1
	),
	EXP_Calculate_PremiumtransactionKey AS (
		SELECT
		LKP_Policy_Heirarchy_With_Date.RatingCoverageAKID AS RatingCoverageAKID_WithDate,
		-- *INF*: IIF(isnull(RatingCoverageAKID_WithDate),0,1)
		IFF(RatingCoverageAKID_WithDate IS NULL, 0, 1) AS Flag,
		LKP_Policy_Heirarchy_With_Date.RiskLocationAKID AS RiskLocationAKID_Date,
		LKP_Policy_Heirarchy_With_Date.PolicyCoverageAKID AS PolicyCoverageAKID_Date,
		LKP_Policy_Heirarchy_With_Date.PolicyAKID AS PolicyAKID_Date,
		LKP_Policy_Heirarchy_With_Date.CoverageGUID AS CoverageGUID_Date,
		LKP_Policy_Heirarchy_With_Date.LocationUnitNumber AS LocationUnitNumber_Date,
		LKP_Policy_Heirarchy_With_Date.RatingCoverageCancellationDate AS RatingCoverageCancellationDate_Date,
		LKP_Policy_Heirarchy_With_Date.RatingCoverageKey AS RatingCoverageKey_Date,
		LKP_Policy_Heirarchy_With_Date.RatingCoverageHashKey AS RatingCoverageHashKey_Date,
		LKP_Policy_Heirarchy_With_Date.RatingCoverageId AS RatingCoverageId_Date,
		LKP_Policy_Heirarchy_With_Date.RatingCoverageEffectivedate AS RatingCoverageEffectivedate_Date,
		LKP_Policy_Heirarchy_With_Date.RatingCoverageExpirationdate AS RatingCoverageExpirationdate_Date,
		LKP_Policy_Heirarchy_Without_Date.PolicyAKID,
		LKP_Policy_Heirarchy_Without_Date.RiskLocationAKID,
		LKP_Policy_Heirarchy_Without_Date.PolicyCoverageAKID,
		LKP_Policy_Heirarchy_Without_Date.CoverageGUID,
		LKP_Policy_Heirarchy_Without_Date.LocationUnitNumber,
		LKP_Policy_Heirarchy_Without_Date.RatingCoverageCancellationDate,
		LKP_Policy_Heirarchy_Without_Date.RatingCoverageAKID,
		LKP_Policy_Heirarchy_Without_Date.RatingCoverageKey,
		LKP_Policy_Heirarchy_Without_Date.RatingCoverageHashKey,
		LKP_Policy_Heirarchy_Without_Date.RatingCoverageId,
		LKP_Policy_Heirarchy_Without_Date.RatingCoverageEffectivedate,
		LKP_Policy_Heirarchy_Without_Date.RatingCoverageExpirationdate,
		-- *INF*: iif(Flag=1,PolicyAKID_Date,PolicyAKID)
		IFF(Flag = 1, PolicyAKID_Date, PolicyAKID) AS v_PolicyAKID,
		-- *INF*: IIF(Flag=1,RiskLocationAKID_Date,RiskLocationAKID)
		IFF(Flag = 1, RiskLocationAKID_Date, RiskLocationAKID) AS v_RiskLocationAKID,
		-- *INF*: iif(Flag=1,PolicyCoverageAKID_Date,PolicyCoverageAKID)
		IFF(Flag = 1, PolicyCoverageAKID_Date, PolicyCoverageAKID) AS v_PolicyCoverageAKID,
		-- *INF*: iif(Flag=1,CoverageGUID_Date,CoverageGUID)
		IFF(Flag = 1, CoverageGUID_Date, CoverageGUID) AS v_CoverageGUID,
		v_CoverageGUID AS o_CoverageGUID,
		-- *INF*: iif(Flag=1,LocationUnitNumber_Date,LocationUnitNumber)
		IFF(Flag = 1, LocationUnitNumber_Date, LocationUnitNumber) AS v_LocationUnitNumber,
		v_RiskLocationAKID AS o_RiskLocationAKID,
		-- *INF*: iif(@{pipeline().parameters.PMSESSIONNAME}='s_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',NULL,RatingCoverageCancellationDate_Date)
		IFF(
		    @{pipeline().parameters.PMSESSIONNAME} = 's_m_POL_DW_LOAD_RatingCoverage_Restate_DCT', NULL,
		    RatingCoverageCancellationDate_Date
		) AS o_RatingCoverageCancellationDate,
		-- *INF*: iif(@{pipeline().parameters.PMSESSIONNAME}='s_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',NULL,iif(Flag=1,RatingCoverageAKID_WithDate,RatingCoverageAKID))
		IFF(
		    @{pipeline().parameters.PMSESSIONNAME} = 's_m_POL_DW_LOAD_RatingCoverage_Restate_DCT', NULL,
		    IFF(
		        Flag = 1, RatingCoverageAKID_WithDate, RatingCoverageAKID
		    )
		) AS o_RatingCoverageAKID,
		v_PolicyCoverageAKID AS o_PolicyCoverageAKID,
		-- *INF*: TO_CHAR(v_PolicyAKID) || '~'  || TO_CHAR(v_RiskLocationAKID)  || '~' || TO_CHAR( v_PolicyCoverageAKID)  || '~' || v_CoverageGUID  || '~'  || v_LocationUnitNumber
		TO_CHAR(v_PolicyAKID) || '~' || TO_CHAR(v_RiskLocationAKID) || '~' || TO_CHAR(v_PolicyCoverageAKID) || '~' || v_CoverageGUID || '~' || v_LocationUnitNumber AS o_PremiumTransactionKey,
		-- *INF*: iif(@{pipeline().parameters.PMSESSIONNAME}='s_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',NULL,RatingCoverageKey_Date)
		IFF(
		    @{pipeline().parameters.PMSESSIONNAME} = 's_m_POL_DW_LOAD_RatingCoverage_Restate_DCT', NULL, RatingCoverageKey_Date
		) AS o_RatingCoverageKey,
		-- *INF*: iif(@{pipeline().parameters.PMSESSIONNAME}='s_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',NULL,RatingCoverageHashKey_Date)
		IFF(
		    @{pipeline().parameters.PMSESSIONNAME} = 's_m_POL_DW_LOAD_RatingCoverage_Restate_DCT', NULL,
		    RatingCoverageHashKey_Date
		) AS o_RatingCoverageHashKey,
		-- *INF*: iif(@{pipeline().parameters.PMSESSIONNAME}='s_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',NULL,RatingCoverageId_Date)
		-- 
		-- --IIF(Flag=1,RatingCoverageId_Date,RatingCoverageId)
		-- 
		-- 
		-- 
		IFF(
		    @{pipeline().parameters.PMSESSIONNAME} = 's_m_POL_DW_LOAD_RatingCoverage_Restate_DCT', NULL, RatingCoverageId_Date
		) AS o_RatingCoverageId,
		LKP_Policy_Heirarchy_With_Date.ClassCode,
		LKP_Policy_Heirarchy_With_Date.CoverageType AS i_CoverageType_Date,
		LKP_Policy_Heirarchy_With_Date.ProductAbbreviation AS i_ProductAbbreviation_Date,
		LKP_Policy_Heirarchy_Without_Date.CoverageType AS i_CoverageType,
		LKP_Policy_Heirarchy_Without_Date.ProductAbbreviation AS i_ProductAbbreviation,
		-- *INF*: IIF(@{pipeline().parameters.PMSESSIONNAME}='s_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',NULL,
		-- IIF(Flag=1,RatingCoverageEffectivedate_Date,RatingCoverageEffectivedate))
		-- 
		-- ---IIF(Flag=1,RatingCoverageEffectivedate_Date,RatingCoverageEffectivedate)
		IFF(
		    @{pipeline().parameters.PMSESSIONNAME} = 's_m_POL_DW_LOAD_RatingCoverage_Restate_DCT', NULL,
		    IFF(
		        Flag = 1, RatingCoverageEffectivedate_Date, RatingCoverageEffectivedate
		    )
		) AS o_RatingCoverageEffectivedate,
		-- *INF*: IIF(@{pipeline().parameters.PMSESSIONNAME}='s_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',NULL,
		-- IIF(Flag=1,RatingCoverageExpirationdate_Date,RatingCoverageExpirationdate))
		-- 
		-- 
		-- --IIF(Flag=1,RatingCoverageExpirationdate_Date,RatingCoverageExpirationdate)
		IFF(
		    @{pipeline().parameters.PMSESSIONNAME} = 's_m_POL_DW_LOAD_RatingCoverage_Restate_DCT', NULL,
		    IFF(
		        Flag = 1, RatingCoverageExpirationdate_Date, RatingCoverageExpirationdate
		    )
		) AS o_RatingCoverageExpirationdate,
		-- *INF*: IIF(@{pipeline().parameters.PMSESSIONNAME}='s_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',NULL,
		-- IIF(Flag=1,i_CoverageType_Date,i_CoverageType))
		IFF(
		    @{pipeline().parameters.PMSESSIONNAME} = 's_m_POL_DW_LOAD_RatingCoverage_Restate_DCT', NULL,
		    IFF(
		        Flag = 1, i_CoverageType_Date, i_CoverageType
		    )
		) AS o_CoverageType,
		-- *INF*: IIF(@{pipeline().parameters.PMSESSIONNAME}='s_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',NULL,
		-- IIF(Flag=1,i_ProductAbbreviation_Date,i_ProductAbbreviation))
		IFF(
		    @{pipeline().parameters.PMSESSIONNAME} = 's_m_POL_DW_LOAD_RatingCoverage_Restate_DCT', NULL,
		    IFF(
		        Flag = 1, i_ProductAbbreviation_Date, i_ProductAbbreviation
		    )
		) AS o_ProductAbbreviation
		FROM 
		LEFT JOIN LKP_Policy_Heirarchy_With_Date
		ON LKP_Policy_Heirarchy_With_Date.PolicyAKID = EXP_Get_Value.o_PolicyAKID AND LKP_Policy_Heirarchy_With_Date.CoverageGUID = EXP_Get_Value.CoverageGuid AND LKP_Policy_Heirarchy_With_Date.EffectiveDate = EXP_Get_Value.TransactionCreatedDate
		LEFT JOIN LKP_Policy_Heirarchy_Without_Date
		ON LKP_Policy_Heirarchy_Without_Date.PolicyAKID = EXP_Get_Value.o_PolicyAKID AND LKP_Policy_Heirarchy_Without_Date.CoverageGUID = EXP_Get_Value.CoverageGuid
	),
	Output_Policy AS (
		SELECT
		o_CoverageGUID AS CoverageGUID, 
		o_RiskLocationAKID AS RiskLocationAKID, 
		o_RatingCoverageCancellationDate AS RatingCoverageCancellationDate, 
		o_RatingCoverageAKID AS RatingCoverageAKID, 
		o_PremiumTransactionKey AS PremiumTransactionKey, 
		o_RatingCoverageKey AS RatingCoverageKey, 
		o_RatingCoverageHashKey AS RatingCoverageHashKey, 
		o_RatingCoverageId AS RatingCoverageId, 
		o_PolicyCoverageAKID AS PolicyCoverageAKID, 
		o_RatingCoverageEffectivedate AS RatingCoverageEffectivedate, 
		o_RatingCoverageExpirationdate AS RatingCoverageExpirationdate, 
		ClassCode, 
		o_CoverageType AS CoverageType, 
		o_ProductAbbreviation AS ProductAbbreviation
		FROM EXP_Calculate_PremiumtransactionKey
	),
),
EXP_CoverageStatus AS (
	SELECT
	LKP_DCCoverageStaging.CoverageDeleteFlag AS lkp_StageCoverageDeleteFlag,
	LKP_DCCoverageStaging.Type AS lkp_StageTransactionType,
	AGG_Remove_Duplicate.TransactionCancellationDate AS i_TCancellationDate,
	AGG_Remove_Duplicate.PolicyVersion AS i_PolicyVersion,
	AGG_Remove_Duplicate.ParentCoverageObjectId AS i_ParentCoverageObjectId,
	AGG_Remove_Duplicate.ParentCoverageObjectName AS i_ParentCoverageObjectName,
	AGG_Remove_Duplicate.CoverageType,
	mplt_get_RiskLocation_PolicyCoverage_RatingCoverage_Akids_Hierarchy.RatingCoverageAKID,
	mplt_get_RiskLocation_PolicyCoverage_RatingCoverage_Akids_Hierarchy.RatingCoverageCancellationDate,
	AGG_Remove_Duplicate.Exposure,
	AGG_Remove_Duplicate.PolicyStatus,
	AGG_Remove_Duplicate.Written,
	mplt_get_RiskLocation_PolicyCoverage_RatingCoverage_Akids_Hierarchy.PremiumTransactionKey,
	AGG_Remove_Duplicate.Type AS TType,
	AGG_Remove_Duplicate.CreatedDate,
	AGG_Remove_Duplicate.TransactionEffectiveDate,
	AGG_Remove_Duplicate.TransactionExpirationDate,
	AGG_Remove_Duplicate.PremiumTransactionEnteredDate,
	AGG_Remove_Duplicate.PremiumTransactionBookedDate,
	AGG_Remove_Duplicate.PolicyEffectiveDate,
	AGG_Remove_Duplicate.PolicyExpirationDate,
	AGG_Remove_Duplicate.Premium,
	AGG_Remove_Duplicate.Change,
	AGG_Remove_Duplicate.Prior,
	AGG_Remove_Duplicate.PremiumType,
	'N/A' AS o_New_OffsetOnsetCode,
	'Onset' AS o_Endorse_OnsetCode,
	'Offset' AS o_Endorse_OffsetCode,
	LKP_RatingCoverage.RatingCoverageCancellationDate AS lkp_RatingCoverageCancellationDate,
	LKP_RatingCoverage.PremiumTransactionCode AS lkp_PremiumTransactionCode,
	AGG_Remove_Duplicate.ReasonAmendedCode,
	AGG_Remove_Duplicate.CodeCaption,
	AGG_Remove_Duplicate.CoverageGUID,
	AGG_Remove_Duplicate.DeductibleAmount,
	AGG_Remove_Duplicate.RetroactiveDate,
	AGG_Remove_Duplicate.ExperienceModificationFactor,
	AGG_Remove_Duplicate.ExperienceModificationEffectiveDate,
	AGG_Remove_Duplicate.PackageModificationAdjustmentFactor,
	AGG_Remove_Duplicate.PackageModificationAdjustmentGroupCode,
	AGG_Remove_Duplicate.IncreasedLimitFactor,
	AGG_Remove_Duplicate.IncreasedLimitGroupCode,
	AGG_Remove_Duplicate.YearBuilt,
	AGG_Remove_Duplicate.AgencyActualCommissionRate,
	AGG_Remove_Duplicate.BaseRate,
	AGG_Remove_Duplicate.ConstructionCode,
	AGG_Remove_Duplicate.StateRatingEffectiveDate,
	AGG_Remove_Duplicate.CoverageId,
	AGG_Remove_Duplicate.IndividualRiskPremiumModification,
	AGG_Remove_Duplicate.WindCoverageFlag AS i_WindCoverageFlag,
	AGG_Remove_Duplicate.CoverageDeleteFlag,
	-- *INF*: DECODE(TRUE,
	-- CoverageDeleteFlag='1',1,
	-- PolicyStatus='Cancelled',1,
	-- 0)
	DECODE(
	    TRUE,
	    CoverageDeleteFlag = '1', 1,
	    PolicyStatus = 'Cancelled', 1,
	    0
	) AS v_RatingCoverageCancellationFlag,
	-- *INF*: DECODE(TRUE,
	-- TType='New' OR TType='Renew', 'New',
	-- TType='Endorse', 'Endorse',
	-- 'Other')
	DECODE(
	    TRUE,
	    TType = 'New' OR TType = 'Renew', 'New',
	    TType = 'Endorse', 'Endorse',
	    'Other'
	) AS o_EndorsementFlag,
	-- *INF*: IIF(i_ParentCoverageObjectName!='WB_CU_PremiumDetail','0',:LKP.LKP_WBCUPREMIUMDETAILSTAGE(i_ParentCoverageObjectId))
	IFF(
	    i_ParentCoverageObjectName != 'WB_CU_PremiumDetail', '0',
	    LKP_WBCUPREMIUMDETAILSTAGE_i_ParentCoverageObjectId.Override
	) AS o_Override,
	-- *INF*: DECODE(TRUE,
	-- PremiumTransactionBookedDate=TO_DATE('1800-01-01', 'YYYY-MM-DD'),0,
	-- CoverageDeleteFlag='0',1,
	-- CoverageDeleteFlag='1' AND lkp_StageCoverageDeleteFlag='0',1,
	-- CoverageDeleteFlag='1' AND lkp_RatingCoverageCancellationDate>=TO_DATE('21001231','YYYYMMDD'),1,
	-- Change<>0,1,
	-- 0)
	DECODE(
	    TRUE,
	    PremiumTransactionBookedDate = TO_TIMESTAMP('1800-01-01', 'YYYY-MM-DD'), 0,
	    CoverageDeleteFlag = '0', 1,
	    CoverageDeleteFlag = '1' AND lkp_StageCoverageDeleteFlag = '0', 1,
	    CoverageDeleteFlag = '1' AND lkp_RatingCoverageCancellationDate >= TO_TIMESTAMP('21001231', 'YYYYMMDD'), 1,
	    Change <> 0, 1,
	    0
	) AS o_FilterFlag,
	-- *INF*: DECODE(TRUE,
	-- TType='New','10',
	-- TType='Renew','11',
	-- TType='Endorse' AND CoverageDeleteFlag='0' AND Change>=0, '12',
	-- TType='Endorse' AND CoverageDeleteFlag='0' AND Change<0, '22',
	-- TType='Endorse' AND CoverageDeleteFlag='1', '28',
	-- TType='Cancel' AND i_TCancellationDate=PolicyEffectiveDate AND i_PolicyVersion='00','20',
	-- TType='Cancel' AND i_TCancellationDate>PolicyEffectiveDate AND i_PolicyVersion='00','23',
	-- TType='Cancel' AND i_TCancellationDate=PolicyEffectiveDate AND i_PolicyVersion<>'00','21',
	-- TType='Cancel' AND i_TCancellationDate>PolicyEffectiveDate AND i_PolicyVersion<>'00','25',
	-- TType='Reinstate','15',
	-- TType='Reissue','30',
	-- TType='Rewrite','31',
	-- TType='Rescind' AND Change>=0, '12',
	-- TType='Rescind' AND Change<0, '22',
	-- IN(TType,'FinalAudit','VoidFinalAudit','RevisedFinalAudit') AND Change>=0,'14',
	-- IN(TType,'FinalAudit','VoidFinalAudit','RevisedFinalAudit') AND Change<0,'24',
	-- IN(TType,'RetroCalculation','RevisedRetroCalculation','RetrospectiveCalculation') AND Change>=0,'57',
	-- IN(TType,'RetroCalculation','RevisedRetroCalculation','RetrospectiveCalculation') AND Change<0,'67',
	-- IN(TType,'FinalReporting','VoidFinalReporting') AND Change>=0,'12',
	-- IN(TType,'FinalReporting','VoidFinalReporting') AND Change<0,'22'
	-- )	
	DECODE(
	    TRUE,
	    TType = 'New', '10',
	    TType = 'Renew', '11',
	    TType = 'Endorse' AND CoverageDeleteFlag = '0' AND Change >= 0, '12',
	    TType = 'Endorse' AND CoverageDeleteFlag = '0' AND Change < 0, '22',
	    TType = 'Endorse' AND CoverageDeleteFlag = '1', '28',
	    TType = 'Cancel' AND i_TCancellationDate = PolicyEffectiveDate AND i_PolicyVersion = '00', '20',
	    TType = 'Cancel' AND i_TCancellationDate > PolicyEffectiveDate AND i_PolicyVersion = '00', '23',
	    TType = 'Cancel' AND i_TCancellationDate = PolicyEffectiveDate AND i_PolicyVersion <> '00', '21',
	    TType = 'Cancel' AND i_TCancellationDate > PolicyEffectiveDate AND i_PolicyVersion <> '00', '25',
	    TType = 'Reinstate', '15',
	    TType = 'Reissue', '30',
	    TType = 'Rewrite', '31',
	    TType = 'Rescind' AND Change >= 0, '12',
	    TType = 'Rescind' AND Change < 0, '22',
	    TType IN ('FinalAudit','VoidFinalAudit','RevisedFinalAudit') AND Change >= 0, '14',
	    TType IN ('FinalAudit','VoidFinalAudit','RevisedFinalAudit') AND Change < 0, '24',
	    TType IN ('RetroCalculation','RevisedRetroCalculation','RetrospectiveCalculation') AND Change >= 0, '57',
	    TType IN ('RetroCalculation','RevisedRetroCalculation','RetrospectiveCalculation') AND Change < 0, '67',
	    TType IN ('FinalReporting','VoidFinalReporting') AND Change >= 0, '12',
	    TType IN ('FinalReporting','VoidFinalReporting') AND Change < 0, '22'
	) AS o_StandardTransactionCode,
	AGG_Remove_Duplicate.TransactionPurpose,
	AGG_Remove_Duplicate.ExposureBasis,
	AGG_Remove_Duplicate.DeductibleBasis,
	AGG_Remove_Duplicate.TransactionCreatedUserId,
	AGG_Remove_Duplicate.ServiceCentreName,
	mplt_get_RiskLocation_PolicyCoverage_RatingCoverage_Akids_Hierarchy.ClassCode,
	mplt_get_RiskLocation_PolicyCoverage_RatingCoverage_Akids_Hierarchy.CoverageType AS i_CoverageType,
	mplt_get_RiskLocation_PolicyCoverage_RatingCoverage_Akids_Hierarchy.ProductAbbreviation AS i_ProductAbbreviation,
	-- *INF*: IIF
	-- ((i_CoverageType='Building' and i_ProductAbbreviation='SBOP') 
	-- or (i_CoverageType='BLDG' and i_ProductAbbreviation='SBOP') 
	-- or (IN(i_CoverageType,'Building', 'FunctionalBuildingValuation')  and i_ProductAbbreviation='SMART') 
	-- or (i_CoverageType='OTC' and i_ProductAbbreviation='Garage Liab'),'1',i_WindCoverageFlag)
	-- 
	-- 
	-- --IIF((i_CoverageType='Building' and i_ProductAbbreviation='SBOP') or (i_CoverageType='BLDG' and i_ProductAbbreviation='SBOP')  or (i_CoverageType='Building' and i_ProductAbbreviation='SMART') or (i_CoverageType='OTC' and i_ProductAbbreviation='Garage Liab'),'1',i_WindCoverageFlag) 
	-- 
	-- 
	-- 
	IFF(
	    (i_CoverageType = 'Building'
	    and i_ProductAbbreviation = 'SBOP')
	    or (i_CoverageType = 'BLDG'
	    and i_ProductAbbreviation = 'SBOP')
	    or (i_CoverageType IN ('Building','FunctionalBuildingValuation')
	    and i_ProductAbbreviation = 'SMART')
	    or (i_CoverageType = 'OTC'
	    and i_ProductAbbreviation = 'Garage Liab'),
	    '1',
	    i_WindCoverageFlag
	) AS v_WindCoverageFlag,
	v_WindCoverageFlag AS o_WindCoverageFlag,
	AGG_Remove_Duplicate.DeclaredEvent,
	-- *INF*: DECODE(TRUE,
	-- DeclaredEvent = 'T',1,
	-- DeclaredEvent ='F',0,
	-- ISNULL(DeclaredEvent),0
	-- )
	DECODE(
	    TRUE,
	    DeclaredEvent = 'T', 1,
	    DeclaredEvent = 'F', 0,
	    DeclaredEvent IS NULL, 0
	) AS O_DeclaredEvent
	FROM AGG_Remove_Duplicate
	 -- Manually join with mplt_get_RiskLocation_PolicyCoverage_RatingCoverage_Akids_Hierarchy
	LEFT JOIN LKP_DCCoverageStaging
	ON LKP_DCCoverageStaging.Pol_Key = AGG_Remove_Duplicate.Policy_Key AND LKP_DCCoverageStaging.CoverageGUID = AGG_Remove_Duplicate.CoverageGUID AND LKP_DCCoverageStaging.EffectiveDate <= AGG_Remove_Duplicate.TransactionEffectiveDate AND LKP_DCCoverageStaging.CreatedDate < AGG_Remove_Duplicate.CreatedDate AND LKP_DCCoverageStaging.OffsetCreatedDate > AGG_Remove_Duplicate.CreatedDate
	LEFT JOIN LKP_RatingCoverage
	ON LKP_RatingCoverage.CoverageGUID = AGG_Remove_Duplicate.CoverageGUID AND LKP_RatingCoverage.PolicyAKID = AGG_Remove_Duplicate.o_pol_ak_id AND LKP_RatingCoverage.TEffectiveDate <= AGG_Remove_Duplicate.TransactionEffectiveDate AND LKP_RatingCoverage.TCreatedDate < AGG_Remove_Duplicate.CreatedDate AND LKP_RatingCoverage.OffsetCreatedDate > AGG_Remove_Duplicate.CreatedDate
	LEFT JOIN LKP_WBCUPREMIUMDETAILSTAGE LKP_WBCUPREMIUMDETAILSTAGE_i_ParentCoverageObjectId
	ON LKP_WBCUPREMIUMDETAILSTAGE_i_ParentCoverageObjectId.WBCUPremiumDetailId = i_ParentCoverageObjectId

),
FIL_DefaultCoverages AS (
	SELECT
	RatingCoverageAKID, 
	RatingCoverageCancellationDate, 
	Written, 
	PremiumTransactionKey, 
	TType AS Type, 
	CreatedDate, 
	TransactionEffectiveDate, 
	TransactionExpirationDate, 
	PremiumTransactionEnteredDate, 
	PremiumTransactionBookedDate, 
	PolicyEffectiveDate, 
	PolicyExpirationDate, 
	Premium, 
	Change, 
	Prior, 
	PremiumType, 
	o_New_OffsetOnsetCode AS New_OffsetOnsetCode, 
	o_Endorse_OnsetCode AS Endorse_OnsetCode, 
	o_Endorse_OffsetCode AS Endorse_OffsetCode, 
	ReasonAmendedCode, 
	CodeCaption, 
	CoverageGUID, 
	DeductibleAmount, 
	RetroactiveDate, 
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
	CoverageId, 
	IndividualRiskPremiumModification, 
	o_WindCoverageFlag AS WindCoverageFlag, 
	o_EndorsementFlag AS EndorsementFlag, 
	o_FilterFlag AS FilterFlag, 
	o_StandardTransactionCode AS StandardTransactionCode, 
	TransactionPurpose, 
	o_Override AS Override, 
	CoverageType, 
	ExposureBasis, 
	DeductibleBasis, 
	Exposure, 
	TransactionCreatedUserId, 
	ServiceCentreName, 
	ClassCode, 
	O_DeclaredEvent AS DeclaredEvent
	FROM EXP_CoverageStatus
	WHERE FilterFlag=1
),
RTR_Classify_New_Endorse_GRP AS (
	SELECT
	RatingCoverageAKID,
	RatingCoverageCancellationDate,
	Written,
	PremiumTransactionKey,
	Type,
	CreatedDate,
	TransactionEffectiveDate,
	TransactionExpirationDate,
	PremiumTransactionBookedDate,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	Premium,
	Change,
	Prior,
	PremiumType,
	New_OffsetOnsetCode,
	Endorse_OnsetCode,
	Endorse_OffsetCode,
	ReasonAmendedCode,
	CodeCaption,
	CoverageGUID,
	DeductibleAmount,
	RetroactiveDate,
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
	CoverageId,
	IndividualRiskPremiumModification,
	WindCoverageFlag,
	EndorsementFlag,
	StandardTransactionCode,
	TransactionPurpose,
	Override,
	CoverageType,
	PremiumTransactionEnteredDate,
	ExposureBasis,
	DeductibleBasis,
	Exposure,
	TransactionCreatedUserId,
	ServiceCentreName,
	ClassCode,
	DeclaredEvent
	FROM FIL_DefaultCoverages
),
RTR_Classify_New_Endorse_GRP_GRP_NEW AS (SELECT * FROM RTR_Classify_New_Endorse_GRP WHERE EndorsementFlag='New'),
RTR_Classify_New_Endorse_GRP_GRP_ENDORSE AS (SELECT * FROM RTR_Classify_New_Endorse_GRP WHERE EndorsementFlag='Endorse'),
RTR_Classify_New_Endorse_GRP_GRP_OTHER AS (SELECT * FROM RTR_Classify_New_Endorse_GRP WHERE EndorsementFlag='Other'),
EXPTRANS AS (
	SELECT
	RatingCoverageAKID AS RatingCoverageAKID4,
	RatingCoverageCancellationDate AS i_RatingCoverageCancellationDate4,
	Written AS Written4,
	PremiumTransactionKey AS PremiumTransactionKey4,
	Type AS Type4,
	CreatedDate AS CreatedDate4,
	TransactionEffectiveDate,
	TransactionExpirationDate,
	PremiumTransactionBookedDate AS PremiumTransactionBookedDate4,
	Premium AS i_Premium,
	-- *INF*: DECODE(TRUE,
	-- i_RatingCoverageCancellationDate4<TO_DATE('21001231','YYYYMMDD'),-1*i_Premium,
	-- Change4=0,0,
	-- i_Premium)
	DECODE(
	    TRUE,
	    i_RatingCoverageCancellationDate4 < TO_TIMESTAMP('21001231', 'YYYYMMDD'), - 1 * i_Premium,
	    Change4 = 0, 0,
	    i_Premium
	) AS o_Premium,
	Change AS Change4,
	PremiumType AS PremiumType4,
	New_OffsetOnsetCode AS New_OffsetOnsetCode4,
	ReasonAmendedCode AS ReasonAmendedCode4,
	CodeCaption AS CodeCaption4,
	CoverageGUID AS CoverageGUID4,
	DeductibleAmount AS DeductibleAmount4,
	ExperienceModificationFactor AS ExperienceModificationFactor4,
	ExperienceModificationEffectiveDate AS ExperienceModificationEffectiveDate4,
	PackageModificationAdjustmentFactor AS PackageModificationAdjustmentFactor4,
	PackageModificationAdjustmentGroupCode AS PackageModificationAdjustmentGroupCode4,
	IncreasedLimitFactor AS IncreasedLimitFactor4,
	IncreasedLimitGroupCode AS IncreasedLimitGroupCode4,
	YearBuilt AS YearBuilt4,
	AgencyActualCommissionRate AS AgencyActualCommissionRate4,
	BaseRate AS BaseRate4,
	ConstructionCode AS ConstructionCode4,
	StateRatingEffectiveDate AS StateRatingEffectiveDate4,
	CoverageId AS CoverageId4,
	IndividualRiskPremiumModification AS IndividualRiskPremiumModification4,
	WindCoverageFlag AS WindCoverageFlag4,
	StandardTransactionCode AS StandardTransactionCode4,
	TransactionPurpose AS TransactionPurpose4,
	Override AS Override4,
	CoverageType AS CoverageType4,
	PremiumTransactionEnteredDate AS PremiumTransactionEnteredDate4,
	ExposureBasis AS ExposureBasis4,
	DeductibleBasis AS DeductibleBasis4,
	Exposure AS Exposure4,
	ClassCode AS ClassCode4,
	DeclaredEvent AS DeclaredEvent4
	FROM RTR_Classify_New_Endorse_GRP_GRP_OTHER
),
EXP_Calculate_Endorse_Onset_Offset AS (
	SELECT
	TransactionEffectiveDate,
	TransactionExpirationDate,
	PolicyEffectiveDate AS i_PolicyEffectiveDate,
	PolicyExpirationDate AS i_PolicyExpirationDate,
	Premium AS i_Premium,
	Change AS i_Change,
	Prior AS i_Prior,
	-- *INF*: DATE_DIFF(TransactionExpirationDate,TransactionEffectiveDate,'D')
	DATEDIFF(DAY,TransactionExpirationDate,TransactionEffectiveDate) AS v_TransactionPeriod,
	-- *INF*: DATE_DIFF(i_PolicyExpirationDate,i_PolicyEffectiveDate,'D')
	DATEDIFF(DAY,i_PolicyExpirationDate,i_PolicyEffectiveDate) AS v_PolicyPeriod,
	-- *INF*: -1*IIF(
	--   v_TransactionPeriod<=0 OR v_PolicyPeriod<=0,
	--   i_Prior,
	--   i_Prior*v_TransactionPeriod/v_PolicyPeriod
	-- )
	- 1 * IFF(
	    v_TransactionPeriod <= 0 OR v_PolicyPeriod <= 0, i_Prior,
	    i_Prior * v_TransactionPeriod / v_PolicyPeriod
	) AS o_PremiumTransactionAmount_Offset,
	-1*i_Prior AS o_FullTermPremium_Offset,
	-- *INF*: IIF(
	--   v_TransactionPeriod<=0 OR v_PolicyPeriod<=0,
	--   i_Change+i_Prior,
	-- i_Change+i_Prior*v_TransactionPeriod/v_PolicyPeriod
	-- )
	IFF(
	    v_TransactionPeriod <= 0 OR v_PolicyPeriod <= 0, i_Change + i_Prior,
	    i_Change + i_Prior * v_TransactionPeriod / v_PolicyPeriod
	) AS o_PremiumTransactionAmount_Onset,
	i_Premium AS o_FullTermPremium_Onset
	FROM RTR_Classify_New_Endorse_GRP_GRP_ENDORSE
),
Un_New_Endorse_GRP AS (
	SELECT RatingCoverageAKID, Written, PremiumTransactionKey, Type AS PremiumTransactionCode, CreatedDate, TransactionEffectiveDate AS PremiumTransactionEffectiveDate, TransactionExpirationDate AS PremiumTransactionExpirationDate, PremiumTransactionBookedDate, Written AS PremiumTransactionAmount, Premium AS FullTermPremium, PremiumType, New_OffsetOnsetCode AS OffsetOnsetIndicator, CodeCaption, CoverageGUID, ReasonAmendedCode, DeductibleAmount, ExperienceModificationFactor, ExperienceModificationEffectiveDate, PackageModificationAdjustmentFactor, PackageModificationAdjustmentGroupCode, IncreasedLimitFactor, IncreasedLimitGroupCode, YearBuilt, AgencyActualCommissionRate, BaseRate, ConstructionCode, StateRatingEffectiveDate, CoverageId, IndividualRiskPremiumModification, WindCoverageFlag, StandardTransactionCode, TransactionPurpose, Override, CoverageType, PremiumTransactionEnteredDate, ExposureBasis, DeductibleBasis, Exposure, TransactionCreatedUserId, ServiceCentreName, ClassCode, DeclaredEvent AS DeclaredEventFlag
	FROM RTR_Classify_New_Endorse_GRP_GRP_NEW
	UNION
	SELECT RatingCoverageAKID, Written, PremiumTransactionKey, Type AS PremiumTransactionCode, CreatedDate, TransactionEffectiveDate AS PremiumTransactionEffectiveDate, TransactionExpirationDate AS PremiumTransactionExpirationDate, PremiumTransactionBookedDate, o_PremiumTransactionAmount_Onset AS PremiumTransactionAmount, o_FullTermPremium_Onset AS FullTermPremium, PremiumType, Endorse_OnsetCode AS OffsetOnsetIndicator, CodeCaption, CoverageGUID, ReasonAmendedCode, DeductibleAmount, ExperienceModificationFactor, ExperienceModificationEffectiveDate, PackageModificationAdjustmentFactor, PackageModificationAdjustmentGroupCode, IncreasedLimitFactor, IncreasedLimitGroupCode, YearBuilt, AgencyActualCommissionRate, BaseRate, ConstructionCode, StateRatingEffectiveDate, CoverageId, IndividualRiskPremiumModification, WindCoverageFlag, StandardTransactionCode, TransactionPurpose, Override, CoverageType, PremiumTransactionEnteredDate, ExposureBasis, DeductibleBasis, Exposure, TransactionCreatedUserId, ServiceCentreName, ClassCode, DeclaredEvent AS DeclaredEventFlag
	FROM EXP_Calculate_Endorse_Onset_Offset
	-- Manually join with RTR_Classify_New_Endorse_GRP_GRP_ENDORSE
	UNION
	SELECT RatingCoverageAKID, Written, PremiumTransactionKey, Type AS PremiumTransactionCode, CreatedDate, TransactionEffectiveDate AS PremiumTransactionEffectiveDate, TransactionExpirationDate AS PremiumTransactionExpirationDate, PremiumTransactionBookedDate, o_PremiumTransactionAmount_Offset AS PremiumTransactionAmount, o_FullTermPremium_Offset AS FullTermPremium, PremiumType, Endorse_OffsetCode AS OffsetOnsetIndicator, CodeCaption, CoverageGUID, ReasonAmendedCode, DeductibleAmount, ExperienceModificationFactor, ExperienceModificationEffectiveDate, PackageModificationAdjustmentFactor, PackageModificationAdjustmentGroupCode, IncreasedLimitFactor, IncreasedLimitGroupCode, YearBuilt, AgencyActualCommissionRate, BaseRate, ConstructionCode, StateRatingEffectiveDate, CoverageId, IndividualRiskPremiumModification, WindCoverageFlag, StandardTransactionCode, TransactionPurpose, Override, CoverageType, PremiumTransactionEnteredDate, ExposureBasis, DeductibleBasis, Exposure, TransactionCreatedUserId, ServiceCentreName, ClassCode, DeclaredEvent AS DeclaredEventFlag
	FROM EXP_Calculate_Endorse_Onset_Offset
	-- Manually join with RTR_Classify_New_Endorse_GRP_GRP_ENDORSE
	UNION
	SELECT RatingCoverageAKID4 AS RatingCoverageAKID, Written4 AS Written, PremiumTransactionKey4 AS PremiumTransactionKey, Type4 AS PremiumTransactionCode, CreatedDate4 AS CreatedDate, TransactionEffectiveDate AS PremiumTransactionEffectiveDate, TransactionExpirationDate AS PremiumTransactionExpirationDate, PremiumTransactionBookedDate4 AS PremiumTransactionBookedDate, Change4 AS PremiumTransactionAmount, o_Premium AS FullTermPremium, PremiumType4 AS PremiumType, New_OffsetOnsetCode4 AS OffsetOnsetIndicator, CodeCaption4 AS CodeCaption, CoverageGUID4 AS CoverageGUID, ReasonAmendedCode4 AS ReasonAmendedCode, DeductibleAmount4 AS DeductibleAmount, ExperienceModificationFactor4 AS ExperienceModificationFactor, ExperienceModificationEffectiveDate4 AS ExperienceModificationEffectiveDate, PackageModificationAdjustmentFactor4 AS PackageModificationAdjustmentFactor, PackageModificationAdjustmentGroupCode4 AS PackageModificationAdjustmentGroupCode, IncreasedLimitFactor4 AS IncreasedLimitFactor, IncreasedLimitGroupCode4 AS IncreasedLimitGroupCode, YearBuilt4 AS YearBuilt, AgencyActualCommissionRate4 AS AgencyActualCommissionRate, BaseRate4 AS BaseRate, ConstructionCode4 AS ConstructionCode, StateRatingEffectiveDate4 AS StateRatingEffectiveDate, CoverageId4 AS CoverageId, IndividualRiskPremiumModification4 AS IndividualRiskPremiumModification, WindCoverageFlag4 AS WindCoverageFlag, StandardTransactionCode4 AS StandardTransactionCode, TransactionPurpose4 AS TransactionPurpose, Override4 AS Override, CoverageType4 AS CoverageType, PremiumTransactionEnteredDate4 AS PremiumTransactionEnteredDate, ExposureBasis4 AS ExposureBasis, DeductibleBasis4 AS DeductibleBasis, Exposure4 AS Exposure, TransactionCreatedUserId, ServiceCentreName, ClassCode4 AS ClassCode, DeclaredEvent4 AS DeclaredEventFlag
	FROM EXPTRANS
	-- Manually join with RTR_Classify_New_Endorse_GRP_GRP_OTHER
),
EXP_Calculate_PremiumTransactionHashKey AS (
	SELECT
	RatingCoverageAKID AS i_RatingCoverageAKID,
	Written AS i_Written,
	PremiumTransactionCode AS i_PremiumTransactionCode,
	Override AS i_Override,
	CoverageType AS i_CoverageType,
	PremiumTransactionAmount AS i_PremiumTransactionAmount,
	FullTermPremium AS i_FullTermPremium,
	TransactionPurpose AS i_TransactionPurpose,
	OffsetOnsetIndicator AS i_OffsetOnsetCode,
	PremiumTransactionKey,
	CreatedDate,
	PremiumTransactionEffectiveDate,
	PremiumTransactionExpirationDate,
	PremiumTransactionBookedDate,
	PremiumType,
	CodeCaption,
	CoverageGUID,
	ReasonAmendedCode,
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
	CoverageId,
	IndividualRiskPremiumModification,
	WindCoverageFlag,
	StandardTransactionCode,
	PremiumTransactionEnteredDate,
	DeductibleBasis,
	ExposureBasis,
	Exposure,
	-- *INF*: IIF(i_TransactionPurpose!='Offset',i_PremiumTransactionAmount,-1*i_PremiumTransactionAmount)
	IFF(
	    i_TransactionPurpose != 'Offset', i_PremiumTransactionAmount,
	    - 1 * i_PremiumTransactionAmount
	) AS v_PremiumTransactionAmount,
	-- *INF*: IIF(i_TransactionPurpose!='Offset',i_FullTermPremium,-1*i_FullTermPremium)
	IFF(i_TransactionPurpose != 'Offset', i_FullTermPremium, - 1 * i_FullTermPremium) AS v_FullTermPremium,
	-- *INF*: IIF(NOT ISNULL(i_RatingCoverageAKID), i_RatingCoverageAKID,-1)
	IFF(i_RatingCoverageAKID IS NOT NULL, i_RatingCoverageAKID, - 1) AS v_RatingCoverageAKId,
	-- *INF*: IIF(i_TransactionPurpose!='Offset',i_OffsetOnsetCode,'Deprecated')
	IFF(i_TransactionPurpose != 'Offset', i_OffsetOnsetCode, 'Deprecated') AS o_OffsetOnsetCode,
	-- *INF*: LTRIM(RTRIM(i_PremiumTransactionCode))
	LTRIM(RTRIM(i_PremiumTransactionCode)) AS o_PremiumTransactionCode,
	-- *INF*: DECODE(TRUE,
	-- i_Override='0',v_PremiumTransactionAmount,
	-- i_Override='1',v_PremiumTransactionAmount,
	-- i_CoverageType='Revised',v_PremiumTransactionAmount,
	-- 0
	-- )
	-- 
	-- -- Added condition for i_Override='1' as part of PROD-9733
	DECODE(
	    TRUE,
	    i_Override = '0', v_PremiumTransactionAmount,
	    i_Override = '1', v_PremiumTransactionAmount,
	    i_CoverageType = 'Revised', v_PremiumTransactionAmount,
	    0
	) AS o_PremiumTransactionAmount,
	-- *INF*: DECODE(TRUE,
	-- i_Override='0',v_FullTermPremium,
	-- i_CoverageType='Revised',v_FullTermPremium,
	-- 0
	-- )
	DECODE(
	    TRUE,
	    i_Override = '0', v_FullTermPremium,
	    i_CoverageType = 'Revised', v_FullTermPremium,
	    0
	) AS o_FullTermPremium,
	v_RatingCoverageAKId AS o_RatingCoverageAKId,
	-- *INF*: MD5(v_RatingCoverageAKId|| CoverageGUID||TO_CHAR(CreatedDate)||  i_OffsetOnsetCode || i_TransactionPurpose)
	-- 
	-- -- Above changes for UID project
	-- -- MD5(CoverageGUID|| TO_CHAR(CreatedDate)||  i_OffsetOnsetCode || i_TransactionPurpose
	--  
	-- 
	MD5(v_RatingCoverageAKId || CoverageGUID || TO_CHAR(CreatedDate) || i_OffsetOnsetCode || i_TransactionPurpose) AS o_PremiumTransactionHashKey,
	TransactionCreatedUserId AS TransactionCreatedUserId5,
	ServiceCentreName AS ServiceCentreName5,
	'N/A' AS NegateRestateCode,
	ClassCode,
	DeclaredEventFlag
	FROM Un_New_Endorse_GRP
),
LKP_NumberofEmployees AS (
	SELECT
	NumberOfEmployees,
	CoverageId
	FROM (
		select TotalNumberOfEmployees as NumberOfEmployees,
		CoverageId as CoverageId
		from WBEPLCoverageEmploymentPracticesLiabilityStage
		where ISNULL(TotalNumberOfEmployees,0) <> 0
		
		union all
		
		select NumberOfEmployees as NumberOfEmployees,
		CoverageId as CoverageId
		from WBGLCoverageWB516GLStage
		where ISNULL(NumberOfEmployees,0) <> 0
		
		union all
		
		select NumberOfEmployees as NumberOfEmployees,
		CoverageId as CoverageId
		from WBBPCoverageEmploymentPracticesLiabilityStage
		where ISNULL(NumberOfEmployees,0) <> 0
		
		union all
		
		select a.NumberEmployees as NumberOfEmployees,
		b.CoverageId as CoverageId
		from WBCAEndorsementWB516Stage a
		join WBCoverageStage b
		on a.WB_CoverageId=b.WBCoverageId
		where ISNULL(a.NumberEmployees,0) <> 0
		
		---- This is added as part of PROD-14189 for SBOP EPLI Policies
		union all
		
		select WGL.EmploymentPracticesNumberOfEmployees as NumberOfEmployees,
		WBC.CoverageId as CoverageId
		from WBGLLineStage WGL
		inner join WBCoverageStage WBC
		on WGL.SessionId=WBC.SessionId
		where ISNULL(WGL.EmploymentPracticesNumberOfEmployees,0) <> 0
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY NumberOfEmployees) = 1
),
LKP_PremiumTransaction AS (
	SELECT
	PremiumTransactionAKID,
	PremiumTransactionID,
	PremiumTransactionHashKey
	FROM (
		SELECT PT.PremiumTransactionAKID as PremiumTransactionAKID, 
		PT.PremiumTransactionID as PremiumTransactionID, 
		PT.PremiumTransactionHashKey as PremiumTransactionHashKey,
		PT.NegateRestateCode as NegateRestateCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT
		INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC ON RC.RatingCoverageAKId=PT.RatingCoverageAKId AND RC.EffectiveDate=PT.EffectiveDate
		INNER JOIN (
		select DISTINCT WCT.CoverageGUId from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTCoverageTransaction WCT) WCT
		on WCT.CoverageGUID=RC.CoverageGUID
		WHERE PT.CurrentSnapshotFlag='1' AND PT.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		And PT.ReasonAmendedCode NOT IN ('Claw Back','CWO')
		And PT.NegateRestateCode not in ('PremiumChange') -- To make sure that we load True Offset records created in IDO due to deprecation (PROD-14950)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionHashKey ORDER BY PremiumTransactionAKID DESC) = 1
),
LKP_sup_premium_transaction_code AS (
	SELECT
	sup_prem_trans_code_id,
	prem_trans_code,
	StandardPremiumTransactionCode
	FROM (
		SELECT 
			sup_prem_trans_code_id,
			prem_trans_code,
			StandardPremiumTransactionCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_premium_transaction_code
		WHERE crrnt_snpsht_flag='1' AND source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY prem_trans_code,StandardPremiumTransactionCode ORDER BY sup_prem_trans_code_id) = 1
),
mplt_Evaluate_WrittenExposure_DCT1 AS (WITH
	INPUT AS (
		
	),
	EXP_Calculate_WrittenExposure AS (
		SELECT
		ClassCode,
		CoverageType,
		-- *INF*: DECODE  (TRUE,
		-- INSTR(CoverageType,'ManualPremium') > 0,
		-- 'ManualPremium',
		-- 'N/A')
		-- --Change coverage type coming in from staging as necessary to include subtypes like USL&H
		DECODE(
		    TRUE,
		    REGEXP_INSTR(CoverageType, 'ManualPremium') > 0, 'ManualPremium',
		    'N/A'
		) AS v_CoverageType,
		v_CoverageType AS o_CoverageType,
		BaseRate,
		PremiumTransactionAmount,
		PremiumTransactionCode,
		ReasonAmendedCode,
		-- *INF*: ROUND(PremiumTransactionAmount,4)
		ROUND(PremiumTransactionAmount, 4) AS v_PremiumTransactionAmount,
		-- *INF*: DECODE(TRUE,
		-- BaseRate = 0,0,
		-- BaseRate <> 0, (v_PremiumTransactionAmount  / BaseRate)
		-- )
		DECODE(
		    TRUE,
		    BaseRate = 0, 0,
		    BaseRate <> 0, (v_PremiumTransactionAmount / BaseRate)
		) AS v_CalculatedExposure,
		-- *INF*: DECODE(TRUE,
		-- v_CoverageType = 'ManualPremium'  AND ClassCode = '0908',1,
		-- v_CoverageType = 'ManualPremium' AND ClassCode = '0913',1,
		-- v_CoverageType = 'ManualPremium' AND ClassCode = '7709',0,
		-- v_CoverageType = 'ManualPremium',100,
		-- 0)
		-- --Flag eligible class codes of Exposure Basis 'Unit' to compensate for historical issue where ExampleData didn't pass along correct exposure basis to us. Determine the multipler for DomesticWorkers  class codes as 1, VolunteerFirefighters as 0 and all other Manual Premium typically Payroll as 100. the default value is zero for all coverage types that are not ManualPremium
		DECODE(
		    TRUE,
		    v_CoverageType = 'ManualPremium' AND ClassCode = '0908', 1,
		    v_CoverageType = 'ManualPremium' AND ClassCode = '0913', 1,
		    v_CoverageType = 'ManualPremium' AND ClassCode = '7709', 0,
		    v_CoverageType = 'ManualPremium', 100,
		    0
		) AS v_UnitMultiplier,
		-- *INF*: DECODE(TRUE,
		-- INSTR(PremiumTransactionCode,'Audit') = 0  AND v_CoverageType = 'ManualPremium', v_CalculatedExposure * v_UnitMultiplier,
		-- INSTR(PremiumTransactionCode,'Audit') > 0  AND  v_CoverageType = 'ManualPremium',0,
		-- 0)
		-- -- For eligible manual premium coverage records with non audit transaction codes, we back into Exposure as an inverse function of premium and apply the determined multipler of 1,100 or 0 for DomesticWorkers, Payroll or VolunteerFirefighters
		-- -- For eligible manual premium coverage records with audit type transactions, we zero out the written exposure for subsequent true up downstream
		-- --For all non manual premium coverages with audit or non audit transactions, we default to zero for written exposure
		DECODE(
		    TRUE,
		    REGEXP_INSTR(PremiumTransactionCode, 'Audit') = 0 AND v_CoverageType = 'ManualPremium', v_CalculatedExposure * v_UnitMultiplier,
		    REGEXP_INSTR(PremiumTransactionCode, 'Audit') > 0 AND v_CoverageType = 'ManualPremium', 0,
		    0
		) AS v_WrittenExposure,
		-- *INF*: DECODE(TRUE,
		-- ISNULL(v_WrittenExposure),0,
		-- IN(ReasonAmendedCode,'CWO','Claw Back'), 0,
		-- ROUND(v_WrittenExposure,4))
		-- -- If CWO or ClawBack premium, then we zero out written exposure else we round up calculated written exposure value to 4 decimal places
		DECODE(
		    TRUE,
		    v_WrittenExposure IS NULL, 0,
		    ReasonAmendedCode IN ('CWO','Claw Back'), 0,
		    ROUND(v_WrittenExposure, 4)
		) AS WrittenExposure
		FROM INPUT
	),
	OUTPUT AS (
		SELECT
		ClassCode, 
		o_CoverageType AS CoverageType, 
		BaseRate, 
		PremiumTransactionAmount, 
		PremiumTransactionCode, 
		WrittenExposure
		FROM EXP_Calculate_WrittenExposure
	),
),
EXP_Format_PremiumTransaction AS (
	SELECT
	LKP_sup_premium_transaction_code.sup_prem_trans_code_id AS i_sup_premium_transaction_id,
	EXP_Calculate_PremiumTransactionHashKey.PremiumTransactionKey AS i_PremiumTransactionKey,
	EXP_Calculate_PremiumTransactionHashKey.o_PremiumTransactionCode AS i_PremiumTransactionCode,
	EXP_Calculate_PremiumTransactionHashKey.PremiumTransactionEnteredDate AS i_PremiumTransactionEnteredDate,
	EXP_Calculate_PremiumTransactionHashKey.PremiumTransactionEffectiveDate AS i_PremiumTransactionEffectiveDate,
	EXP_Calculate_PremiumTransactionHashKey.PremiumTransactionExpirationDate AS i_PremiumTransactionExpirationDate,
	EXP_Calculate_PremiumTransactionHashKey.PremiumTransactionBookedDate AS i_PremiumTransactionBookedDate,
	EXP_Calculate_PremiumTransactionHashKey.o_PremiumTransactionAmount AS i_PremiumTransactionAmount,
	EXP_Calculate_PremiumTransactionHashKey.o_FullTermPremium AS i_FullTermPremium,
	EXP_Calculate_PremiumTransactionHashKey.PremiumType AS i_PremiumType,
	EXP_Calculate_PremiumTransactionHashKey.o_OffsetOnsetCode AS i_OffsetOnsetIndicator,
	EXP_Calculate_PremiumTransactionHashKey.CoverageGUID AS i_CoverageGUID,
	EXP_Calculate_PremiumTransactionHashKey.ReasonAmendedCode AS i_ReasonAmendedCode,
	EXP_Calculate_PremiumTransactionHashKey.o_PremiumTransactionHashKey AS i_PremiumTransactionHashKey,
	EXP_Calculate_PremiumTransactionHashKey.o_RatingCoverageAKId AS i_RatingCoverageAKId,
	EXP_Calculate_PremiumTransactionHashKey.CoverageId AS i_CoverageId,
	LKP_PremiumTransaction.PremiumTransactionAKID,
	LKP_PremiumTransaction.PremiumTransactionID,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	i_PremiumTransactionEnteredDate AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	0 AS o_LogicalIndicator,
	'1' AS o_LogicalDeleteFlag,
	i_PremiumTransactionHashKey AS o_PremiumTransactionHashKey,
	1 AS o_PremiumLoadSequence,
	1 AS o_DuplicateSequence,
	-1 AS o_ReinsuranceCoverageAKID,
	-1 AS o_StatisticalCoverageAKID,
	i_PremiumTransactionKey AS o_PremiumTransactionKey,
	'N/A' AS o_PMSFunctionCode,
	i_PremiumTransactionCode AS o_PremiumTransactionCode,
	i_PremiumTransactionEnteredDate AS o_PremiumTransactionEnteredDate,
	i_PremiumTransactionEffectiveDate AS o_PremiumTransactionEffectiveDate,
	i_PremiumTransactionExpirationDate AS o_PremiumTransactionExpirationDate,
	i_PremiumTransactionBookedDate AS o_PremiumTransactionBookedDate,
	i_PremiumTransactionAmount AS o_PremiumTransactionAmount,
	i_FullTermPremium AS o_FullTermPremium,
	i_PremiumType AS o_PremiumType,
	-- *INF*: i_ReasonAmendedCode
	-- 
	-- --'TBD'
	i_ReasonAmendedCode AS o_ReasonAmendedCode,
	i_OffsetOnsetIndicator AS o_OffsetOnsetCode,
	i_sup_premium_transaction_id AS o_sup_premium_transaction_id,
	i_RatingCoverageAKId AS o_RatingCoverageAKId,
	EXP_Calculate_PremiumTransactionHashKey.DeductibleAmount,
	EXP_Calculate_PremiumTransactionHashKey.ExperienceModificationFactor,
	EXP_Calculate_PremiumTransactionHashKey.ExperienceModificationEffectiveDate,
	EXP_Calculate_PremiumTransactionHashKey.PackageModificationAdjustmentFactor,
	EXP_Calculate_PremiumTransactionHashKey.PackageModificationAdjustmentGroupCode,
	EXP_Calculate_PremiumTransactionHashKey.IncreasedLimitFactor,
	EXP_Calculate_PremiumTransactionHashKey.IncreasedLimitGroupCode,
	EXP_Calculate_PremiumTransactionHashKey.YearBuilt,
	EXP_Calculate_PremiumTransactionHashKey.AgencyActualCommissionRate AS i_AgencyActualCommissionRate,
	i_AgencyActualCommissionRate*0.01 AS o_AgencyActualCommissionRate,
	EXP_Calculate_PremiumTransactionHashKey.BaseRate,
	EXP_Calculate_PremiumTransactionHashKey.ConstructionCode,
	EXP_Calculate_PremiumTransactionHashKey.StateRatingEffectiveDate,
	i_CoverageId AS o_PremiumTransactionStageId,
	EXP_Calculate_PremiumTransactionHashKey.IndividualRiskPremiumModification,
	EXP_Calculate_PremiumTransactionHashKey.WindCoverageFlag,
	EXP_Calculate_PremiumTransactionHashKey.CreatedDate,
	EXP_Calculate_PremiumTransactionHashKey.DeductibleBasis,
	EXP_Calculate_PremiumTransactionHashKey.ExposureBasis,
	EXP_Calculate_PremiumTransactionHashKey.Exposure,
	EXP_Calculate_PremiumTransactionHashKey.TransactionCreatedUserId5,
	EXP_Calculate_PremiumTransactionHashKey.ServiceCentreName5,
	LKP_NumberofEmployees.NumberOfEmployees AS lkp_NumberOfEmployees,
	-- *INF*: IIF(ISNULL(lkp_NumberOfEmployees),0,lkp_NumberOfEmployees)
	IFF(lkp_NumberOfEmployees IS NULL, 0, lkp_NumberOfEmployees) AS o_NumberOfEmployees,
	EXP_Calculate_PremiumTransactionHashKey.NegateRestateCode,
	mplt_Evaluate_WrittenExposure_DCT1.WrittenExposure,
	-- *INF*: substr(i_PremiumTransactionKey,1,instr(i_PremiumTransactionKey,'~',1,1))
	substr(i_PremiumTransactionKey, 1, REGEXP_INSTR(i_PremiumTransactionKey, '~', 1, 1)) AS o_Policyakid,
	EXP_Calculate_PremiumTransactionHashKey.DeclaredEventFlag
	FROM EXP_Calculate_PremiumTransactionHashKey
	 -- Manually join with mplt_Evaluate_WrittenExposure_DCT1
	LEFT JOIN LKP_NumberofEmployees
	ON LKP_NumberofEmployees.CoverageId = EXP_Calculate_PremiumTransactionHashKey.CoverageId
	LEFT JOIN LKP_PremiumTransaction
	ON LKP_PremiumTransaction.PremiumTransactionHashKey = EXP_Calculate_PremiumTransactionHashKey.o_PremiumTransactionHashKey
	LEFT JOIN LKP_sup_premium_transaction_code
	ON LKP_sup_premium_transaction_code.prem_trans_code = EXP_Calculate_PremiumTransactionHashKey.o_PremiumTransactionCode AND LKP_sup_premium_transaction_code.StandardPremiumTransactionCode = EXP_Calculate_PremiumTransactionHashKey.StandardTransactionCode
),
RTR_Insert_Update AS (
	SELECT
	PremiumTransactionAKID AS lkp_PremiumTransactionAKID,
	PremiumTransactionID AS lkp_PremiumTransactionID,
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	o_AuditID AS AuditID,
	CreatedDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_SourceSystemID AS SourceSystemID,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_LogicalIndicator AS LogicalIndicator,
	o_LogicalDeleteFlag AS LogicalDeleteFlag,
	o_PremiumTransactionHashKey AS PremiumTransactionHashKey,
	o_PremiumLoadSequence AS PremiumLoadSequence,
	o_DuplicateSequence AS DuplicateSequence,
	o_ReinsuranceCoverageAKID AS ReinsuranceCoverageAKID,
	o_StatisticalCoverageAKID AS StatisticalCoverageAKID,
	o_PremiumTransactionKey AS PremiumTransactionKey,
	o_PMSFunctionCode AS PMSFunctionCode,
	o_PremiumTransactionCode AS PremiumTransactionCode,
	o_PremiumTransactionEnteredDate AS PremiumTransactionEnteredDate,
	o_PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate,
	o_PremiumTransactionExpirationDate AS PremiumTransactionExpirationDate,
	o_PremiumTransactionBookedDate AS PremiumTransactionBookedDate,
	o_PremiumTransactionAmount AS PremiumTransactionAmount,
	o_FullTermPremium AS FullTermPremium,
	o_PremiumType AS PremiumType,
	o_ReasonAmendedCode AS ReasonAmendedCode,
	o_OffsetOnsetCode AS OffsetOnsetCode,
	o_sup_premium_transaction_id AS SupPremiumTransactionCodeId,
	o_RatingCoverageAKId AS RatingCoverageAKId,
	DeductibleAmount,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	PackageModificationAdjustmentFactor,
	PackageModificationAdjustmentGroupCode,
	IncreasedLimitFactor,
	IncreasedLimitGroupCode,
	YearBuilt,
	o_AgencyActualCommissionRate AS AgencyActualCommissionRate,
	BaseRate,
	ConstructionCode,
	StateRatingEffectiveDate,
	IndividualRiskPremiumModification,
	WindCoverageFlag,
	DeductibleBasis,
	ExposureBasis,
	o_PremiumTransactionStageId AS PremiumTransactionStageId,
	Exposure,
	TransactionCreatedUserId5 AS TransactionCreatedUserId,
	ServiceCentreName5 AS ServiceCentreName,
	o_NumberOfEmployees AS NumberOfEmployee,
	NegateRestateCode,
	WrittenExposure,
	o_Policyakid,
	DeclaredEventFlag
	FROM EXP_Format_PremiumTransaction
),
RTR_Insert_Update_INSERT_PREMIUM AS (SELECT * FROM RTR_Insert_Update WHERE ISNULL(lkp_PremiumTransactionAKID) AND NOT (IN(OffsetOnsetCode,'Offset','Deprecated')=1 AND PremiumTransactionAmount=0)),
RTR_Insert_Update_UPDATE_PREMIUM AS (SELECT * FROM RTR_Insert_Update WHERE (NOT ISNULL(lkp_PremiumTransactionAKID)) AND (NOT (IN(OffsetOnsetCode,'Offset','Deprecated')=1 AND PremiumTransactionAmount=0))),
SEQ_PremiumTransactionAKID AS (
	CREATE SEQUENCE SEQ_PremiumTransactionAKID
	START = 0
	INCREMENT = 1;
),
EXP_Set_AKID AS (
	SELECT
	SEQ_PremiumTransactionAKID.NEXTVAL AS i_NEXTVAL,
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
	i_NEXTVAL AS PremiumTransactionAKID,
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
	SupPremiumTransactionCodeId,
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
	PremiumTransactionStageId,
	Exposure,
	TransactionCreatedUserId AS TransactionCreatedUserId1,
	ServiceCentreName AS ServiceCentreName1,
	NumberOfEmployee AS NumberOfEmployee1,
	NegateRestateCode,
	WrittenExposure AS WrittenExposure1,
	DeclaredEventFlag AS DeclaredEventFlag1
	FROM RTR_Insert_Update_INSERT_PREMIUM
),
TGT_PremiumTransaction_Insert_Incremental AS (

	------------ PRE SQL ----------
	exec [spSetIndexStatus] @Enable = 0, @Schema = 'dbo', @TableName = 'PREMIUMTRANSACTION', @IndexWildcard = 'AK1PremiumTransaction'
	-------------------------------


	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction
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
	PREMIUMTRANSACTIONAKID, 
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
	SUPPREMIUMTRANSACTIONCODEID, 
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
	TransactionCreatedUserId1 AS TRANSACTIONCREATEDUSERID, 
	ServiceCentreName1 AS SERVICECENTRENAME, 
	EXPOSURE, 
	NumberOfEmployee1 AS NUMBEROFEMPLOYEE, 
	NEGATERESTATECODE, 
	WrittenExposure1 AS WRITTENEXPOSURE, 
	DeclaredEventFlag1 AS DECLAREDEVENTFLAG
	FROM EXP_Set_AKID

	------------ POST SQL ----------
	exec [spSetIndexStatus] @Enable = 1, @Schema = 'dbo', @TableName = 'PREMIUMTRANSACTION', @IndexWildcard = 'AK1PremiumTransaction'
	-------------------------------


),
WorkPremiumTransaction_Incremental_Insert AS (

	------------ PRE SQL ----------
	if not exists (
	select 1 from @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction
	where AuditID=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}')
	truncate table @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransaction
	-------------------------------


	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransaction
	(AuditID, SourceSystemID, CreatedDate, PremiumTransactionAKId, PremiumTransactionStageId)
	SELECT 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	PremiumTransactionAKID AS PREMIUMTRANSACTIONAKID, 
	PREMIUMTRANSACTIONSTAGEID
	FROM EXP_Set_AKID
),
AGGR_Premiumtransactionstageid AS (
	SELECT
	lkp_PremiumTransactionID AS PremiumTransactionID,
	PremiumTransactionStageId AS PremiumTransactionStageId3,
	-- *INF*: max(PremiumTransactionStageId3)
	max(PremiumTransactionStageId3) AS o_PremiumTransactionID,
	ReasonAmendedCode AS ReasonAmendedCode3,
	RatingCoverageAKId,
	-- *INF*: Count(RatingCoverageAKId)
	Count(RatingCoverageAKId) AS Count_RatingCoverageAKId,
	o_Policyakid AS Policyakid3,
	-- *INF*: Count(Policyakid3)
	Count(Policyakid3) AS Count_Policyakid
	FROM RTR_Insert_Update_UPDATE_PREMIUM
	GROUP BY PremiumTransactionID
),
FLT_Workdctpremiumtransctiontracking AS (
	SELECT
	ReasonAmendedCode3 AS ReasonAmendedCode, 
	PremiumTransactionID, 
	o_PremiumTransactionID AS PremiumTransactionStageId3, 
	Count_RatingCoverageAKId, 
	Count_Policyakid
	FROM AGGR_Premiumtransactionstageid
	WHERE Count_RatingCoverageAKId=1 and Count_Policyakid=1 and ReasonAmendedCode!= 'CWO' and ReasonAmendedCode!='Claw Back'
),
LKP_CoverageID AS (
	SELECT
	DCTTransactionSequence,
	DCTTransactionIndex,
	DCTTransactionGuid,
	CoverageId
	FROM (
		SELECT 
		re.[Sequence] as DCTTransactionSequence,
		 re.[Index] as DCTTransactionIndex, 
		  c.CoverageId as CoverageId,
		 t.Id as DCTTransactionGuid
		from dbo.DCCoverageStaging c with (nolock)
		join dbo.DCTransactionStaging t with (nolock) on c.SessionId = t.SessionId 
		left outer join dbo.DCTransactionReportEntryStaging re with (nolock) on c.SessionId = re.SessionId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY DCTTransactionSequence) = 1
),
EXP_Workdctpremiumtransctiontracking AS (
	SELECT
	FLT_Workdctpremiumtransctiontracking.ReasonAmendedCode,
	FLT_Workdctpremiumtransctiontracking.PremiumTransactionID,
	FLT_Workdctpremiumtransctiontracking.PremiumTransactionStageId3,
	LKP_CoverageID.DCTTransactionGuid,
	LKP_CoverageID.DCTTransactionSequence,
	LKP_CoverageID.DCTTransactionIndex,
	sysdate AS o_ModifiedDate
	FROM FLT_Workdctpremiumtransctiontracking
	LEFT JOIN LKP_CoverageID
	ON LKP_CoverageID.CoverageId = FLT_Workdctpremiumtransctiontracking.PremiumTransactionStageId3
),
UPD_WorkDCTPremiumTransactionTracking AS (
	SELECT
	PremiumTransactionID, 
	DCTTransactionGuid, 
	DCTTransactionSequence, 
	DCTTransactionIndex, 
	o_ModifiedDate, 
	PremiumTransactionStageId3
	FROM EXP_Workdctpremiumtransctiontracking
),
WorkDCTPremiumTransactionTracking_Update AS (
	MERGE INTO WorkDCTPremiumTransactionTracking AS T
	USING UPD_WorkDCTPremiumTransactionTracking AS S
	ON T.PremiumTransactionID = S.PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.o_ModifiedDate, T.DCTTransactionGuid = S.DCTTransactionGuid, T.DCTTransactionSequence = S.DCTTransactionSequence, T.DCTTransactionIndex = S.DCTTransactionIndex, T.DCTCoverageId = S.PremiumTransactionStageId3
),
LKP_WorkPremiumTransaction AS (
	SELECT
	WorkPremiumTransactionId,
	PremiumTransactionAKId,
	AuditID,
	SourceSystemID,
	PremiumTransactionStageId
	FROM (
		SELECT 
			WorkPremiumTransactionId,
			PremiumTransactionAKId,
			AuditID,
			SourceSystemID,
			PremiumTransactionStageId
		FROM WorkPremiumTransaction
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKId,AuditID,SourceSystemID,PremiumTransactionStageId ORDER BY WorkPremiumTransactionId) = 1
),
RTR_WorkPremiumTransaction_Insert_Update AS (
	SELECT
	LKP_WorkPremiumTransaction.WorkPremiumTransactionId AS lkp_WorkPremiumTransactionId,
	RTR_Insert_Update_UPDATE_PREMIUM.AuditID AS AuditID3,
	RTR_Insert_Update_UPDATE_PREMIUM.SourceSystemID AS SourceSystemID3,
	RTR_Insert_Update_UPDATE_PREMIUM.CreatedDate AS CreatedDate3,
	RTR_Insert_Update_UPDATE_PREMIUM.lkp_PremiumTransactionAKID AS lkp_PremiumTransactionAKID3,
	RTR_Insert_Update_UPDATE_PREMIUM.PremiumTransactionStageId AS PremiumTransactionStageId3
	FROM RTR_Insert_Update_UPDATE_PREMIUM
	LEFT JOIN LKP_WorkPremiumTransaction
	ON LKP_WorkPremiumTransaction.PremiumTransactionAKId = RTR_Insert_Update.lkp_PremiumTransactionAKID3 AND LKP_WorkPremiumTransaction.AuditID = RTR_Insert_Update.AuditID3 AND LKP_WorkPremiumTransaction.SourceSystemID = RTR_Insert_Update.SourceSystemID3 AND LKP_WorkPremiumTransaction.PremiumTransactionStageId = RTR_Insert_Update.PremiumTransactionStageId3
),
RTR_WorkPremiumTransaction_Insert_Update_Insert AS (SELECT * FROM RTR_WorkPremiumTransaction_Insert_Update WHERE ISNULL(lkp_WorkPremiumTransactionId)),
RTR_WorkPremiumTransaction_Insert_Update_Update AS (SELECT * FROM RTR_WorkPremiumTransaction_Insert_Update WHERE NOT ISNULL(lkp_WorkPremiumTransactionId)),
WorkPremiumTransaction_insert AS (

	------------ PRE SQL ----------
	if not exists (
	select 1 from @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction
	where AuditID=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}')
	truncate table @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransaction
	-------------------------------


	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransaction
	(AuditID, SourceSystemID, CreatedDate, PremiumTransactionAKId, PremiumTransactionStageId)
	SELECT 
	AuditID3 AS AUDITID, 
	SourceSystemID3 AS SOURCESYSTEMID, 
	CreatedDate3 AS CREATEDDATE, 
	lkp_PremiumTransactionAKID3 AS PREMIUMTRANSACTIONAKID, 
	PremiumTransactionStageId3 AS PREMIUMTRANSACTIONSTAGEID
	FROM RTR_WorkPremiumTransaction_Insert_Update_Insert
),
UPD_WorkPremiumTransaction AS (
	SELECT
	lkp_WorkPremiumTransactionId AS WorkPremiumTransactionId, 
	AuditID AS AuditID3, 
	SourceSystemID AS SourceSystemID3, 
	CreatedDate AS CreatedDate3, 
	lkp_PremiumTransactionAKID AS lkp_PremiumTransactionAKID3, 
	PremiumTransactionStageId AS PremiumTransactionStageId3
	FROM RTR_WorkPremiumTransaction_Insert_Update_Update
),
WorkPremiumTransaction_update AS (
	MERGE INTO WorkPremiumTransaction AS T
	USING UPD_WorkPremiumTransaction AS S
	ON T.WorkPremiumTransactionId = S.WorkPremiumTransactionId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditID = S.AuditID3, T.SourceSystemID = S.SourceSystemID3, T.CreatedDate = S.CreatedDate3, T.PremiumTransactionAKId = S.lkp_PremiumTransactionAKID3, T.PremiumTransactionStageId = S.PremiumTransactionStageId3
),
UPD_PremiumTransaction AS (
	SELECT
	lkp_PremiumTransactionID AS PremiumTransactionID, 
	PremiumTransactionCode, 
	PremiumTransactionEnteredDate, 
	PremiumTransactionEffectiveDate, 
	PremiumTransactionBookedDate, 
	PremiumType, 
	OffsetOnsetCode, 
	SupPremiumTransactionCodeId, 
	RatingCoverageAKId, 
	AgencyActualCommissionRate, 
	ExposureBasis, 
	Exposure, 
	TransactionCreatedUserId AS TransactionCreatedUserId3, 
	WrittenExposure AS WrittenExposure3, 
	DeclaredEventFlag AS DeclaredEventFlag3
	FROM RTR_Insert_Update_UPDATE_PREMIUM
),
TGT_PremiumTransaction_Update_Incremental AS (

	------------ PRE SQL ----------
	exec [spSetIndexStatus] @Enable = 0, @Schema = 'dbo', @TableName = 'PREMIUMTRANSACTION', @IndexWildcard = 'AK1PremiumTransaction'
	-------------------------------


	MERGE INTO PremiumTransaction AS T
	USING UPD_PremiumTransaction AS S
	ON T.PremiumTransactionID = S.PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.PremiumTransactionCode = S.PremiumTransactionCode, T.PremiumTransactionEnteredDate = S.PremiumTransactionEnteredDate, T.PremiumTransactionEffectiveDate = S.PremiumTransactionEffectiveDate, T.PremiumType = S.PremiumType, T.OffsetOnsetCode = S.OffsetOnsetCode, T.SupPremiumTransactionCodeId = S.SupPremiumTransactionCodeId, T.RatingCoverageAKId = S.RatingCoverageAKId, T.AgencyActualCommissionRate = S.AgencyActualCommissionRate, T.ExposureBasis = S.ExposureBasis, T.TransactionCreatedUserId = S.TransactionCreatedUserId3, T.Exposure = S.Exposure, T.WrittenExposure = S.WrittenExposure3, T.DeclaredEventFlag = S.DeclaredEventFlag3

	------------ POST SQL ----------
	exec [spSetIndexStatus] @Enable = 1, @Schema = 'dbo', @TableName = 'PREMIUMTRANSACTION', @IndexWildcard = 'AK1PremiumTransaction'
	-------------------------------


),
WITH
SQ_PremiumTransaction_Offset AS (
	SELECT ptToUpdate.PremiumTransactionID,
		ptToUpdate.EffectiveDate,
		ptToUpdate.ReasonAmendedCode,
		ptToUpdate.PremiumTransactionExpirationDate,
		ptToUpdate.DeductibleAmount,
		ptToUpdate.ExperienceModificationFactor,
		ptToUpdate.ExperienceModificationEffectiveDate,
		ptToUpdate.PackageModificationAdjustmentFactor,
		ptToUpdate.PackageModificationAdjustmentGroupCode,
		ptToUpdate.IncreasedLimitFactor,
		ptToUpdate.IncreasedLimitGroupCode,
		ptToUpdate.YearBuilt,
		ptToUpdate.BaseRate,
		ptToUpdate.ConstructionCode,
		ptToUpdate.IndividualRiskPremiumModification,
		ptToUpdate.StateRatingEffectiveDate,
		ptToUpdate.WindCoverageFlag,
		ptToUpdate.DeductibleBasis,
		ptToUpdate.ExposureBasis,
		ptToUpdate.ServiceCentreName,
		ptToUpdate.NumberOfEmployee,
		ptAttrValues.EffectiveDate,
		ptAttrValues.ReasonAmendedCode,
		ptAttrValues.PremiumTransactionExpirationDate,
		ptAttrValues.DeductibleAmount,
		ptAttrValues.ExperienceModificationFactor,
		ptAttrValues.ExperienceModificationEffectiveDate,
		ptAttrValues.PackageModificationAdjustmentFactor,
		ptAttrValues.PackageModificationAdjustmentGroupCode,
		ptAttrValues.IncreasedLimitFactor,
		ptAttrValues.IncreasedLimitGroupCode,
		ptAttrValues.YearBuilt,
		ptAttrValues.BaseRate,
		ptAttrValues.ConstructionCode,
		ptAttrValues.IndividualRiskPremiumModification,
		ptAttrValues.StateRatingEffectiveDate,
		ptAttrValues.WindCoverageFlag,
		ptAttrValues.DeductibleBasis,
		ptAttrValues.ExposureBasis,
		ptAttrValues.ServiceCentreName,
		ptAttrValues.NumberOfEmployee
	FROM WorkPremiumTransactionOffsetLineage wptol 
	INNER JOIN PremiumTransaction ptToUpdate ON wptol.PremiumTransactionID = ptToUpdate.PremiumTransactionID
	INNER JOIN PremiumTransaction ptAttrValues ON wptol.PreviousPremiumTransactionID = ptAttrValues.PremiumTransactionID
	WHERE wptol.UpdateAttributeFlag = 1 
	AND ptToUpdate.OffsetOnsetCode = 'Offset' and ptToUpdate.AuditId=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} 
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_offset_PTUpdate_attributes AS (
	SELECT
	ptToUpdate_PremiumTransactionID AS PremiumTransactionID,
	ptToUpdate_EffectiveDate,
	ptToUpdate_ReasonAmendedCode,
	ptToUpdate_PremiumTransactionExpirationDate,
	ptToUpdate_DeductibleAmount,
	ptToUpdate_ExperienceModificationFactor,
	ptToUpdate_ExperienceModificationEffectiveDate,
	ptToUpdate_PackageModificationAdjustmentFactor,
	ptToUpdate_PackageModificationAdjustmentGroupCode,
	ptToUpdate_IncreasedLimitFactor,
	ptToUpdate_IncreasedLimitGroupCode,
	ptToUpdate_YearBuilt,
	ptToUpdate_BaseRate,
	ptToUpdate_ConstructionCode,
	ptToUpdate_IndividualRiskPremiumModification,
	ptToUpdate_StateRatingEffectiveDate,
	ptToUpdate_WindCoverageFlag,
	ptToUpdate_DeductibleBasis,
	ptToUpdate_ExposureBasis,
	ptToUpdate_ServiceCentreName,
	ptToUpdate_NumberOfEmployee,
	ptAttrValues_EffectiveDate,
	ptAttrValues_PremiumTransactionExpirationDate,
	ptAttrValues_ReasonAmendedCode,
	ptAttrValues_DeductibleAmount,
	ptAttrValues_ExperienceModificationFactor,
	ptAttrValues_ExperienceModificationEffectiveDate,
	ptAttrValues_PackageModificationAdjustmentFactor,
	ptAttrValues_PackageModificationAdjustmentGroupCode,
	ptAttrValues_IncreasedLimitFactor,
	ptAttrValues_IncreasedLimitGroupCode,
	ptAttrValues_YearBuilt,
	ptAttrValues_BaseRate,
	ptAttrValues_ConstructionCode,
	ptAttrValues_IndividualRiskPremiumModification,
	ptAttrValues_StateRatingEffectiveDate,
	ptAttrValues_WindCoverageFlag,
	ptAttrValues_DeductibleBasis,
	ptAttrValues_ExposureBasis,
	ptAttrValues_ServiceCentreName,
	ptAttrValues_NumberOfEmployee,
	-- *INF*: DECODE(TRUE,
	-- 	(ptToUpdate_EffectiveDate != ptAttrValues_EffectiveDate or
	-- 	ptToUpdate_ReasonAmendedCode != ptAttrValues_ReasonAmendedCode or
	-- 	ptToUpdate_PremiumTransactionExpirationDate != ptAttrValues_PremiumTransactionExpirationDate or
	-- 	ptToUpdate_DeductibleAmount != ptAttrValues_DeductibleAmount or
	-- 	ptToUpdate_ExperienceModificationFactor != ptAttrValues_ExperienceModificationFactor or
	-- 	ptToUpdate_ExperienceModificationEffectiveDate != ptAttrValues_ExperienceModificationEffectiveDate or
	-- 	ptToUpdate_PackageModificationAdjustmentFactor != ptAttrValues_PackageModificationAdjustmentFactor or
	-- 	ptToUpdate_PackageModificationAdjustmentGroupCode != ptAttrValues_PackageModificationAdjustmentGroupCode or
	-- 	ptToUpdate_IncreasedLimitFactor != ptAttrValues_IncreasedLimitFactor or
	-- 	ptToUpdate_IncreasedLimitGroupCode != ptAttrValues_IncreasedLimitGroupCode or
	-- 	ptToUpdate_YearBuilt != ptAttrValues_YearBuilt  or
	-- 	ptToUpdate_BaseRate != ptAttrValues_BaseRate or
	-- 	ptToUpdate_ConstructionCode != ptAttrValues_ConstructionCode or
	-- 	ptToUpdate_IndividualRiskPremiumModification != ptAttrValues_IndividualRiskPremiumModification or 
	-- 	ptToUpdate_StateRatingEffectiveDate != ptAttrValues_StateRatingEffectiveDate or
	-- 	ptToUpdate_WindCoverageFlag != ptAttrValues_WindCoverageFlag or
	-- 	ptToUpdate_DeductibleBasis != ptAttrValues_DeductibleBasis or
	-- 	ptToUpdate_ExposureBasis != ptAttrValues_ExposureBasis or
	-- 	ptToUpdate_ServiceCentreName != ptAttrValues_ServiceCentreName or
	-- 	ptToUpdate_NumberOfEmployee != ptAttrValues_NumberOfEmployee),
	-- 		'UPD',
	-- 	'NOCHANGE')
	DECODE(TRUE,
		( ptToUpdate_EffectiveDate != ptAttrValues_EffectiveDate OR ptToUpdate_ReasonAmendedCode != ptAttrValues_ReasonAmendedCode OR ptToUpdate_PremiumTransactionExpirationDate != ptAttrValues_PremiumTransactionExpirationDate OR ptToUpdate_DeductibleAmount != ptAttrValues_DeductibleAmount OR ptToUpdate_ExperienceModificationFactor != ptAttrValues_ExperienceModificationFactor OR ptToUpdate_ExperienceModificationEffectiveDate != ptAttrValues_ExperienceModificationEffectiveDate OR ptToUpdate_PackageModificationAdjustmentFactor != ptAttrValues_PackageModificationAdjustmentFactor OR ptToUpdate_PackageModificationAdjustmentGroupCode != ptAttrValues_PackageModificationAdjustmentGroupCode OR ptToUpdate_IncreasedLimitFactor != ptAttrValues_IncreasedLimitFactor OR ptToUpdate_IncreasedLimitGroupCode != ptAttrValues_IncreasedLimitGroupCode OR ptToUpdate_YearBuilt != ptAttrValues_YearBuilt OR ptToUpdate_BaseRate != ptAttrValues_BaseRate OR ptToUpdate_ConstructionCode != ptAttrValues_ConstructionCode OR ptToUpdate_IndividualRiskPremiumModification != ptAttrValues_IndividualRiskPremiumModification OR ptToUpdate_StateRatingEffectiveDate != ptAttrValues_StateRatingEffectiveDate OR ptToUpdate_WindCoverageFlag != ptAttrValues_WindCoverageFlag OR ptToUpdate_DeductibleBasis != ptAttrValues_DeductibleBasis OR ptToUpdate_ExposureBasis != ptAttrValues_ExposureBasis OR ptToUpdate_ServiceCentreName != ptAttrValues_ServiceCentreName OR ptToUpdate_NumberOfEmployee != ptAttrValues_NumberOfEmployee ), 'UPD',
		'NOCHANGE') AS v_PTUpdateFlag,
	v_PTUpdateFlag AS PTUpdateFlag
	FROM SQ_PremiumTransaction_Offset
),
FLT_UpdateRecords AS (
	SELECT
	PremiumTransactionID, 
	ptAttrValues_EffectiveDate AS EffectiveDate, 
	ptAttrValues_PremiumTransactionExpirationDate AS PremiumTransactionExpirationDate, 
	ptAttrValues_ReasonAmendedCode AS ReasonAmendedCode, 
	ptAttrValues_DeductibleAmount AS DeductibleAmount, 
	ptAttrValues_ExperienceModificationFactor AS ExperienceModificationFactor, 
	ptAttrValues_ExperienceModificationEffectiveDate AS ExperienceModificationEffectiveDate, 
	ptAttrValues_PackageModificationAdjustmentFactor AS PackageModificationAdjustmentFactor, 
	ptAttrValues_PackageModificationAdjustmentGroupCode AS PackageModificationAdjustmentGroupCode, 
	ptAttrValues_IncreasedLimitFactor AS IncreasedLimitFactor, 
	ptAttrValues_IncreasedLimitGroupCode AS IncreasedLimitGroupCode, 
	ptAttrValues_YearBuilt AS YearBuilt, 
	ptAttrValues_BaseRate AS BaseRate, 
	ptAttrValues_ConstructionCode AS ConstructionCode, 
	ptAttrValues_IndividualRiskPremiumModification AS IndividualRiskPremiumModification, 
	ptAttrValues_StateRatingEffectiveDate AS StateRatingEffectiveDate, 
	ptAttrValues_WindCoverageFlag AS WindCoverageFlag, 
	ptAttrValues_DeductibleBasis AS DeductibleBasis, 
	ptAttrValues_ExposureBasis AS ExposureBasis, 
	ptAttrValues_ServiceCentreName AS ServiceCentreName, 
	ptAttrValues_NumberOfEmployee AS NumberOfEmployee, 
	PTUpdateFlag
	FROM EXP_offset_PTUpdate_attributes
	WHERE PTUpdateFlag='UPD'
),
UPD_PremiumTransaction AS (
	SELECT
	PremiumTransactionID, 
	EffectiveDate, 
	PremiumTransactionExpirationDate, 
	ReasonAmendedCode, 
	DeductibleAmount, 
	ExperienceModificationFactor, 
	ExperienceModificationEffectiveDate, 
	PackageModificationAdjustmentFactor, 
	PackageModificationAdjustmentGroupCode, 
	IncreasedLimitFactor, 
	IncreasedLimitGroupCode, 
	YearBuilt, 
	BaseRate, 
	ConstructionCode, 
	IndividualRiskPremiumModification, 
	StateRatingEffectiveDate, 
	WindCoverageFlag, 
	DeductibleBasis, 
	ExposureBasis, 
	ServiceCentreName, 
	NumberOfEmployee
	FROM FLT_UpdateRecords
),
PremiumTransaction_upd AS (
	MERGE INTO PremiumTransaction AS T
	USING UPD_PremiumTransaction AS S
	ON T.PremiumTransactionID = S.PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.EffectiveDate = S.EffectiveDate, T.PremiumTransactionExpirationDate = S.PremiumTransactionExpirationDate, T.ReasonAmendedCode = S.ReasonAmendedCode, T.DeductibleAmount = S.DeductibleAmount, T.ExperienceModificationFactor = S.ExperienceModificationFactor, T.ExperienceModificationEffectiveDate = S.ExperienceModificationEffectiveDate, T.PackageModificationAdjustmentFactor = S.PackageModificationAdjustmentFactor, T.PackageModificationAdjustmentGroupCode = S.PackageModificationAdjustmentGroupCode, T.IncreasedLimitFactor = S.IncreasedLimitFactor, T.IncreasedLimitGroupCode = S.IncreasedLimitGroupCode, T.YearBuilt = S.YearBuilt, T.BaseRate = S.BaseRate, T.ConstructionCode = S.ConstructionCode, T.StateRatingEffectiveDate = S.StateRatingEffectiveDate, T.IndividualRiskPremiumModification = S.IndividualRiskPremiumModification, T.WindCoverageFlag = S.WindCoverageFlag, T.DeductibleBasis = S.DeductibleBasis, T.ExposureBasis = S.ExposureBasis, T.ServiceCentreName = S.ServiceCentreName, T.NumberOfEmployee = S.NumberOfEmployee
),
SQ_PremiumTransaction_Deprecated AS (
	SELECT ptToUpdate.PremiumTransactionID,
		ptToUpdate.EffectiveDate,
		ptToUpdate.ReasonAmendedCode,
		ptToUpdate.PremiumTransactionExpirationDate,
		ptToUpdate.DeductibleAmount,
		ptToUpdate.ExperienceModificationFactor,
		ptToUpdate.ExperienceModificationEffectiveDate,
		ptToUpdate.PackageModificationAdjustmentFactor,
		ptToUpdate.PackageModificationAdjustmentGroupCode,
		ptToUpdate.IncreasedLimitFactor,
		ptToUpdate.IncreasedLimitGroupCode,
		ptToUpdate.YearBuilt,
		ptToUpdate.BaseRate,
		ptToUpdate.ConstructionCode,
		ptToUpdate.IndividualRiskPremiumModification,
		ptToUpdate.StateRatingEffectiveDate,
		ptToUpdate.WindCoverageFlag,
		ptToUpdate.DeductibleBasis,
		ptToUpdate.ExposureBasis,
		ptToUpdate.ServiceCentreName,
		ptToUpdate.NumberOfEmployee,
		ptAttrValues.EffectiveDate,
		ptAttrValues.ReasonAmendedCode,
		ptAttrValues.PremiumTransactionExpirationDate,
		ptAttrValues.DeductibleAmount,
		ptAttrValues.ExperienceModificationFactor,
		ptAttrValues.ExperienceModificationEffectiveDate,
		ptAttrValues.PackageModificationAdjustmentFactor,
		ptAttrValues.PackageModificationAdjustmentGroupCode,
		ptAttrValues.IncreasedLimitFactor,
		ptAttrValues.IncreasedLimitGroupCode,
		ptAttrValues.YearBuilt,
		ptAttrValues.BaseRate,
		ptAttrValues.ConstructionCode,
		ptAttrValues.IndividualRiskPremiumModification,
		ptAttrValues.StateRatingEffectiveDate,
		ptAttrValues.WindCoverageFlag,
		ptAttrValues.DeductibleBasis,
		ptAttrValues.ExposureBasis,
		ptAttrValues.ServiceCentreName,
		ptAttrValues.NumberOfEmployee
	FROM WorkPremiumTransactionOffsetLineage wptol 
	INNER JOIN PremiumTransaction ptToUpdate ON wptol.PremiumTransactionID = ptToUpdate.PremiumTransactionID
	INNER JOIN PremiumTransaction ptAttrValues ON wptol.PreviousPremiumTransactionID = ptAttrValues.PremiumTransactionID
	WHERE wptol.UpdateAttributeFlag = 1 
	AND ptToUpdate.OffsetOnsetCode = 'Deprecated' and ptToUpdate.AuditId=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} 
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Deprecated_PTUpdate_attributes AS (
	SELECT
	ptToUpdate_PremiumTransactionID AS PremiumTransactionID,
	ptToUpdate_EffectiveDate,
	ptToUpdate_ReasonAmendedCode,
	ptToUpdate_PremiumTransactionExpirationDate,
	ptToUpdate_DeductibleAmount,
	ptToUpdate_ExperienceModificationFactor,
	ptToUpdate_ExperienceModificationEffectiveDate,
	ptToUpdate_PackageModificationAdjustmentFactor,
	ptToUpdate_PackageModificationAdjustmentGroupCode,
	ptToUpdate_IncreasedLimitFactor,
	ptToUpdate_IncreasedLimitGroupCode,
	ptToUpdate_YearBuilt,
	ptToUpdate_BaseRate,
	ptToUpdate_ConstructionCode,
	ptToUpdate_IndividualRiskPremiumModification,
	ptToUpdate_StateRatingEffectiveDate,
	ptToUpdate_WindCoverageFlag,
	ptToUpdate_DeductibleBasis,
	ptToUpdate_ExposureBasis,
	ptToUpdate_ServiceCentreName,
	ptToUpdate_NumberOfEmployee,
	ptAttrValues_EffectiveDate,
	ptAttrValues_PremiumTransactionExpirationDate,
	ptAttrValues_ReasonAmendedCode,
	ptAttrValues_DeductibleAmount,
	ptAttrValues_ExperienceModificationFactor,
	ptAttrValues_ExperienceModificationEffectiveDate,
	ptAttrValues_PackageModificationAdjustmentFactor,
	ptAttrValues_PackageModificationAdjustmentGroupCode,
	ptAttrValues_IncreasedLimitFactor,
	ptAttrValues_IncreasedLimitGroupCode,
	ptAttrValues_YearBuilt,
	ptAttrValues_BaseRate,
	ptAttrValues_ConstructionCode,
	ptAttrValues_IndividualRiskPremiumModification,
	ptAttrValues_StateRatingEffectiveDate,
	ptAttrValues_WindCoverageFlag,
	ptAttrValues_DeductibleBasis,
	ptAttrValues_ExposureBasis,
	ptAttrValues_ServiceCentreName,
	ptAttrValues_NumberOfEmployee,
	-- *INF*: DECODE(TRUE,
	-- 	(ptToUpdate_EffectiveDate != ptAttrValues_EffectiveDate or
	-- 	ptToUpdate_ReasonAmendedCode != ptAttrValues_ReasonAmendedCode or
	-- 	ptToUpdate_PremiumTransactionExpirationDate != ptAttrValues_PremiumTransactionExpirationDate or
	-- 	ptToUpdate_DeductibleAmount != ptAttrValues_DeductibleAmount or
	-- 	ptToUpdate_ExperienceModificationFactor != ptAttrValues_ExperienceModificationFactor or
	-- 	ptToUpdate_ExperienceModificationEffectiveDate != ptAttrValues_ExperienceModificationEffectiveDate or
	-- 	ptToUpdate_PackageModificationAdjustmentFactor != ptAttrValues_PackageModificationAdjustmentFactor or
	-- 	ptToUpdate_PackageModificationAdjustmentGroupCode != ptAttrValues_PackageModificationAdjustmentGroupCode or
	-- 	ptToUpdate_IncreasedLimitFactor != ptAttrValues_IncreasedLimitFactor or
	-- 	ptToUpdate_IncreasedLimitGroupCode != ptAttrValues_IncreasedLimitGroupCode or
	-- 	ptToUpdate_YearBuilt != ptAttrValues_YearBuilt  or
	-- 	ptToUpdate_BaseRate != ptAttrValues_BaseRate or
	-- 	ptToUpdate_ConstructionCode != ptAttrValues_ConstructionCode or
	-- 	ptToUpdate_IndividualRiskPremiumModification != ptAttrValues_IndividualRiskPremiumModification or 
	-- 	ptToUpdate_StateRatingEffectiveDate != ptAttrValues_StateRatingEffectiveDate or
	-- 	ptToUpdate_WindCoverageFlag != ptAttrValues_WindCoverageFlag or
	-- 	ptToUpdate_DeductibleBasis != ptAttrValues_DeductibleBasis or
	-- 	ptToUpdate_ExposureBasis != ptAttrValues_ExposureBasis or
	-- 	ptToUpdate_ServiceCentreName != ptAttrValues_ServiceCentreName or
	-- 	ptToUpdate_NumberOfEmployee != ptAttrValues_NumberOfEmployee),
	-- 		'UPD',
	-- 	'NOCHANGE')
	DECODE(TRUE,
		( ptToUpdate_EffectiveDate != ptAttrValues_EffectiveDate OR ptToUpdate_ReasonAmendedCode != ptAttrValues_ReasonAmendedCode OR ptToUpdate_PremiumTransactionExpirationDate != ptAttrValues_PremiumTransactionExpirationDate OR ptToUpdate_DeductibleAmount != ptAttrValues_DeductibleAmount OR ptToUpdate_ExperienceModificationFactor != ptAttrValues_ExperienceModificationFactor OR ptToUpdate_ExperienceModificationEffectiveDate != ptAttrValues_ExperienceModificationEffectiveDate OR ptToUpdate_PackageModificationAdjustmentFactor != ptAttrValues_PackageModificationAdjustmentFactor OR ptToUpdate_PackageModificationAdjustmentGroupCode != ptAttrValues_PackageModificationAdjustmentGroupCode OR ptToUpdate_IncreasedLimitFactor != ptAttrValues_IncreasedLimitFactor OR ptToUpdate_IncreasedLimitGroupCode != ptAttrValues_IncreasedLimitGroupCode OR ptToUpdate_YearBuilt != ptAttrValues_YearBuilt OR ptToUpdate_BaseRate != ptAttrValues_BaseRate OR ptToUpdate_ConstructionCode != ptAttrValues_ConstructionCode OR ptToUpdate_IndividualRiskPremiumModification != ptAttrValues_IndividualRiskPremiumModification OR ptToUpdate_StateRatingEffectiveDate != ptAttrValues_StateRatingEffectiveDate OR ptToUpdate_WindCoverageFlag != ptAttrValues_WindCoverageFlag OR ptToUpdate_DeductibleBasis != ptAttrValues_DeductibleBasis OR ptToUpdate_ExposureBasis != ptAttrValues_ExposureBasis OR ptToUpdate_ServiceCentreName != ptAttrValues_ServiceCentreName OR ptToUpdate_NumberOfEmployee != ptAttrValues_NumberOfEmployee ), 'UPD',
		'NOCHANGE') AS v_PTUpdateFlag,
	v_PTUpdateFlag AS PTUpdateFlag
	FROM SQ_PremiumTransaction_Deprecated
),
FLT_UpdateRecords_Deprecated AS (
	SELECT
	PremiumTransactionID, 
	ptAttrValues_EffectiveDate AS EffectiveDate, 
	ptAttrValues_PremiumTransactionExpirationDate AS PremiumTransactionExpirationDate, 
	ptAttrValues_ReasonAmendedCode AS ReasonAmendedCode, 
	ptAttrValues_DeductibleAmount AS DeductibleAmount, 
	ptAttrValues_ExperienceModificationFactor AS ExperienceModificationFactor, 
	ptAttrValues_ExperienceModificationEffectiveDate AS ExperienceModificationEffectiveDate, 
	ptAttrValues_PackageModificationAdjustmentFactor AS PackageModificationAdjustmentFactor, 
	ptAttrValues_PackageModificationAdjustmentGroupCode AS PackageModificationAdjustmentGroupCode, 
	ptAttrValues_IncreasedLimitFactor AS IncreasedLimitFactor, 
	ptAttrValues_IncreasedLimitGroupCode AS IncreasedLimitGroupCode, 
	ptAttrValues_YearBuilt AS YearBuilt, 
	ptAttrValues_BaseRate AS BaseRate, 
	ptAttrValues_ConstructionCode AS ConstructionCode, 
	ptAttrValues_IndividualRiskPremiumModification AS IndividualRiskPremiumModification, 
	ptAttrValues_StateRatingEffectiveDate AS StateRatingEffectiveDate, 
	ptAttrValues_WindCoverageFlag AS WindCoverageFlag, 
	ptAttrValues_DeductibleBasis AS DeductibleBasis, 
	ptAttrValues_ExposureBasis AS ExposureBasis, 
	ptAttrValues_ServiceCentreName AS ServiceCentreName, 
	ptAttrValues_NumberOfEmployee AS NumberOfEmployee, 
	PTUpdateFlag
	FROM EXP_Deprecated_PTUpdate_attributes
	WHERE PTUpdateFlag='UPD'
),
UPD_PremiumTransaction_Deprecated AS (
	SELECT
	PremiumTransactionID, 
	EffectiveDate, 
	PremiumTransactionExpirationDate, 
	ReasonAmendedCode, 
	DeductibleAmount, 
	ExperienceModificationFactor, 
	ExperienceModificationEffectiveDate, 
	PackageModificationAdjustmentFactor, 
	PackageModificationAdjustmentGroupCode, 
	IncreasedLimitFactor, 
	IncreasedLimitGroupCode, 
	YearBuilt, 
	BaseRate, 
	ConstructionCode, 
	IndividualRiskPremiumModification, 
	StateRatingEffectiveDate, 
	WindCoverageFlag, 
	DeductibleBasis, 
	ExposureBasis, 
	ServiceCentreName, 
	NumberOfEmployee
	FROM FLT_UpdateRecords_Deprecated
),
PremiumTransaction_upd_Deprecated AS (
	MERGE INTO PremiumTransaction AS T
	USING UPD_PremiumTransaction_Deprecated AS S
	ON T.PremiumTransactionID = S.PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.EffectiveDate = S.EffectiveDate, T.PremiumTransactionExpirationDate = S.PremiumTransactionExpirationDate, T.ReasonAmendedCode = S.ReasonAmendedCode, T.DeductibleAmount = S.DeductibleAmount, T.ExperienceModificationFactor = S.ExperienceModificationFactor, T.ExperienceModificationEffectiveDate = S.ExperienceModificationEffectiveDate, T.PackageModificationAdjustmentFactor = S.PackageModificationAdjustmentFactor, T.PackageModificationAdjustmentGroupCode = S.PackageModificationAdjustmentGroupCode, T.IncreasedLimitFactor = S.IncreasedLimitFactor, T.IncreasedLimitGroupCode = S.IncreasedLimitGroupCode, T.YearBuilt = S.YearBuilt, T.BaseRate = S.BaseRate, T.ConstructionCode = S.ConstructionCode, T.StateRatingEffectiveDate = S.StateRatingEffectiveDate, T.IndividualRiskPremiumModification = S.IndividualRiskPremiumModification, T.WindCoverageFlag = S.WindCoverageFlag, T.DeductibleBasis = S.DeductibleBasis, T.ExposureBasis = S.ExposureBasis, T.ServiceCentreName = S.ServiceCentreName, T.NumberOfEmployee = S.NumberOfEmployee
),
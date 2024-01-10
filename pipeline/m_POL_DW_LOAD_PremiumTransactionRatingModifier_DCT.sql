WITH
SQ_PremiumTransaction AS (
	SELECT PremiumTransaction.PremiumTransactionID
		,PremiumTransaction.PremiumTransactionAKID
		,WorkPremiumTransaction.PremiumTransactionStageId
		,RatingCoverage.CoverageGUID
		,RatingCoverage.CoverageType
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransaction ON PremiumTransaction.PremiumTransactionAKID = WorkPremiumTransaction.PremiumTransactionAKID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage ON PremiumTransaction.RatingcoverageAKID = RatingCoverage.RatingcoverageAKID
		AND PremiumTransaction.EffectiveDate = RatingCoverage.EffectiveDate
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product  ON RatingCoverage.ProductAKId = Product.ProductAKId
		AND Product.CurrentSnapshotFlag = 1
		AND Product.ProductAbbreviation <> 'WC'
	WHERE PremiumTransaction.PremiumTransactionID NOT IN (
			SELECT WPTOL.PremiumTransactionID
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransactionOffsetLineage WPTOL
			with (nolock) WHERE WPTOL.UpdateAttributeFlag = 1) 
		AND PremiumTransaction.ReasonAmendedCode NOT IN (
			'CWO'
			,'Claw Back'
			) and PremiumTransaction.PremiumType='D'
),
EXP_Extract_IL AS (
	SELECT
	PremiumTransactionID,
	PremiumTransactionAKID,
	PremiumTransactionStageId,
	CoverageGUID,
	CoverageType
	FROM SQ_PremiumTransaction
),
Filter_CoverageType_BuiltUp AS (
	SELECT
	PremiumTransactionID, 
	PremiumTransactionAKID, 
	CoverageGUID, 
	PremiumTransactionStageId, 
	CoverageType
	FROM EXP_Extract_IL
	WHERE CoverageType = 'BuiltUp'
),
EXP_BuitUp AS (
	SELECT
	PremiumTransactionID,
	PremiumTransactionAKID,
	CoverageGUID,
	PremiumTransactionStageId
	FROM Filter_CoverageType_BuiltUp
),
SQ_WBCUPremiumDetailStage_Umbrella AS (
	select 
	Cov.CoverageId,
	Cov.Id as CoverageGUID,
	Cov.SessionId
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCoverageStaging Cov
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCUPremiumDetailStage CU on CU.SessionId=Cov.SessionId
	and CU.WBCUPremiumDetailId=Cov.ObjectId
	and CU.Type='Treaty'
	where
	Cov.Type = 'BuiltUp'
	and Cov.ObjectName='WB_CU_PremiumDetail'
),
lkp_DCTUmbrella AS (
	SELECT
	Modifier,
	SessionId
	FROM (
		select SessionId as SessionId,
		       case when sum(FirstMillionModifiedPremium)<>0 and sum(FirstMillionBasePremium)<>0
		       then sum(FirstMillionModifiedPremium)/sum(FirstMillionBasePremium) else 1 end as Modifier
		       from (
		              select SessionId,FirstMillionBasePremium,FirstMillionModifiedPremium from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCUUmbrellaCommercialAutoStaging
					  union all
		              select SessionId,FirstMillionBasePremium,FirstMillionModifiedPremium from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCUUmbrellaGeneralLiabilityStaging
		              union all
		              select SessionId,FirstMillionBasePremium,FirstMillionModifiedPremium from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCUUmbrellaSMARTBusinessStage
					  union all
		              select SessionId,FirstMillionBasePremium,FirstMillionModifiedPremium from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCUUmbrellaSBOPStage
					  union all
		              select SessionId,FirstMillionBasePremium,FirstMillionModifiedPremium from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCUUmbrellaBusinessOwnersStaging
					  ) a
		group by SessionId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SessionId ORDER BY Modifier) = 1
),
EXP_DCTUmbrella AS (
	SELECT
	SQ_WBCUPremiumDetailStage_Umbrella.CoverageId,
	SQ_WBCUPremiumDetailStage_Umbrella.CoverageGUID,
	'DCTUmbrella' AS Type,
	lkp_DCTUmbrella.Modifier AS Value,
	'DCTUmbrellaScheduled' AS Scope
	FROM SQ_WBCUPremiumDetailStage_Umbrella
	LEFT JOIN lkp_DCTUmbrella
	ON lkp_DCTUmbrella.SessionId = SQ_WBCUPremiumDetailStage_Umbrella.SessionId
),
JNR_BuiltUP_DCTUmbrella AS (SELECT
	EXP_BuitUp.PremiumTransactionID, 
	EXP_BuitUp.PremiumTransactionAKID, 
	EXP_BuitUp.CoverageGUID, 
	EXP_BuitUp.PremiumTransactionStageId, 
	EXP_DCTUmbrella.CoverageId, 
	EXP_DCTUmbrella.CoverageGUID AS CoverageGUID_Stage, 
	EXP_DCTUmbrella.Type, 
	EXP_DCTUmbrella.Value, 
	EXP_DCTUmbrella.Scope
	FROM EXP_BuitUp
	INNER JOIN EXP_DCTUmbrella
	ON EXP_DCTUmbrella.CoverageId = EXP_BuitUp.PremiumTransactionStageId AND EXP_DCTUmbrella.CoverageGUID = EXP_BuitUp.CoverageGUID
),
EXP_BuitUp_DCTUmbrella AS (
	SELECT
	PremiumTransactionID,
	PremiumTransactionAKID,
	Type,
	Value,
	Scope,
	'CommercialUmbrella' AS InsuranceLine_stg
	FROM JNR_BuiltUP_DCTUmbrella
),
SQ_DCCoverageStaging_NonUmbrella AS (
	WITH PCoverage								
	AS (								
		SELECT parent.SessionId, 							
			parent.ObjectId AS ObjectId, 						
			parent.ObjectName AS ObjectName, 						
			parent.CoverageId, 						
			parent.Id AS CoverageGUID, 						
			parent.Type AS CoverageType, 						
			CASE substring(parent.ObjectName, 4, 3) 						
				WHEN 'CF_' THEN 'Property' 					
				WHEN 'GL_' THEN 'GeneralLiability' 					
				WHEN 'WC_' THEN 'WorkersCompensation' 					
				WHEN 'BP_' THEN 'BusinessOwners' 					
				WHEN 'CR_' THEN 'Crime' 					
				WHEN 'IM_' THEN 'InlandMarine' 					
				WHEN 'EXL' THEN 'ExcessLiability' 					
				WHEN 'CU_' THEN 'CommercialUmbrella' 					
				WHEN 'CA_' THEN 'CommercialAuto' 					
				WHEN 'CDO' THEN 'DirectorsAndOffsCondos' 					
				WHEN 'EPL' THEN 'EmploymentPracticesLiab' 					
				WHEN 'HIO' THEN 'HoleInOne' 					
				WHEN 'GOC' THEN 'GamesOfChance' 					
				ELSE 'N/A' 					
			END InsuranceLine						
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCoverageStaging parent							
		WHERE parent.ObjectName <> 'DC_Coverage'							
									
		UNION ALL							
									
		SELECT parent.SessionId, 							
			parent.ObjectId AS ObjectId, 						
			parent.ObjectName AS ObjectName, 						
			child.CoverageId, 						
			child.Id AS CoverageGUID, 						
			child.Type AS CoverageType, 						
			CASE substring(parent.ObjectName, 4, 3) 						
				WHEN 'CF_' THEN 'Property' 					
				WHEN 'GL_' THEN 'GeneralLiability' 					
				WHEN 'WC_' THEN 'WorkersCompensation' 					
				WHEN 'BP_' THEN 'BusinessOwners' 					
				WHEN 'CR_' THEN 'Crime' 					
				WHEN 'IM_' THEN 'InlandMarine' 					
				WHEN 'EXL' THEN 'ExcessLiability' 					
				WHEN 'CU_' THEN 'CommercialUmbrella' 					
				WHEN 'CA_' THEN 'CommercialAuto' 					
				WHEN 'CDO' THEN 'DirectorsAndOffsCondos' 					
				WHEN 'EPL' THEN 'EmploymentPracticesLiab' 					
				WHEN 'HIO' THEN 'HoleInOne' 					
				WHEN 'GOC' THEN 'GamesOfChance' 					
				ELSE 'N/A' 					
			END InsuranceLine						
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCoverageStaging child							
		JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCoverageStaging parent ON child.SessionId = parent.SessionId							
			AND child.ObjectId = parent.CoverageId						
			AND parent.ObjectName <> 'DC_Coverage'						
		WHERE child.ObjectName = 'DC_Coverage'							
		)
	select a.CoverageGuid,a.CoverageId,a.ModifierType,a.ModifierValue,a.ModifierScope,a.InsuranceLine from 
	(SELECT C.CoverageGuid,								
		C.CoverageId,							
		m.Type AS ModifierType,							
		m.Value AS ModifierValue,							
		m.Scope as ModifierScope,							
		isnull(L.Type, C.InsuranceLine) AS InsuranceLine							
	FROM PCoverage C 								
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCModifierStaging m ON m.ObjectId = C.CoverageId AND m.SessionId = C.SessionId								
		AND m.ObjectName = 'DC_Coverage'							
		AND m.Type IS NOT NULL							
		AND m.Value IS NOT NULL							
		AND m.Value <> '0'		
	     				
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLineStaging L ON L.SessionId = C.SessionId								
		AND (							
				(L.LineId = C.ObjectId AND C.ObjectName = 'DC_Line')					
				OR L.Type = C.InsuranceLine					
			)) a
	where a.InsuranceLine <> 'CommercialUmbrella'
),
EXP_Stage AS (
	SELECT
	Id AS CoverageGUID,
	CoverageId,
	Type,
	Value,
	Scope,
	ObjectName AS InsuranceLine_stg
	FROM SQ_DCCoverageStaging_NonUmbrella
),
JNR_IL_Stage AS (SELECT
	EXP_Extract_IL.PremiumTransactionID, 
	EXP_Extract_IL.PremiumTransactionAKID, 
	EXP_Extract_IL.PremiumTransactionStageId, 
	EXP_Extract_IL.CoverageGUID, 
	EXP_Stage.CoverageGUID AS CoverageGUID_Stage, 
	EXP_Stage.CoverageId, 
	EXP_Stage.Type, 
	EXP_Stage.Value, 
	EXP_Stage.Scope, 
	EXP_Stage.InsuranceLine_stg
	FROM EXP_Extract_IL
	INNER JOIN EXP_Stage
	ON EXP_Stage.CoverageGUID = EXP_Extract_IL.CoverageGUID AND EXP_Stage.CoverageId = EXP_Extract_IL.PremiumTransactionStageId
),
EXP_IL_Stage AS (
	SELECT
	PremiumTransactionID,
	PremiumTransactionAKID,
	Type,
	Value,
	Scope,
	InsuranceLine_stg
	FROM JNR_IL_Stage
),
Union_All_lines AS (
	SELECT PremiumTransactionID, PremiumTransactionAKID, Type, Value, Scope, InsuranceLine_stg
	FROM EXP_BuitUp_DCTUmbrella
	UNION
	SELECT PremiumTransactionID, PremiumTransactionAKID, Type, Value, Scope, InsuranceLine_stg
	FROM EXP_IL_Stage
),
mplt_PremiumTransactionRatingModifier AS (WITH
	Source_RatingModifier AS (
		
	),
	Exp_GetValues AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Type,
		Value AS i_Value,
		Scope,
		InsuranceLine_stg AS InsuranceLine,
		-- *INF*: TO_DECIMAL(i_Value,8)
		CAST(i_Value AS FLOAT) AS v_Value,
		-- *INF*: ROUND(v_Value,4)
		ROUND(v_Value, 4
		) AS o_Value
		FROM Source_RatingModifier
	),
	RTR_InsuranceLine AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Type,
		o_Value AS Value,
		Scope,
		InsuranceLine
		FROM Exp_GetValues
	),
	RTR_InsuranceLine_Property AS (SELECT * FROM RTR_InsuranceLine WHERE InsuranceLine='Property'),
	RTR_InsuranceLine_GeneralLiability AS (SELECT * FROM RTR_InsuranceLine WHERE InsuranceLine='GeneralLiability'),
	RTR_InsuranceLine_BusinessOwners AS (SELECT * FROM RTR_InsuranceLine WHERE InsuranceLine='BusinessOwners'),
	RTR_InsuranceLine_CommercialAuto AS (SELECT * FROM RTR_InsuranceLine WHERE InsuranceLine='CommercialAuto'),
	RTR_InsuranceLine_Crime AS (SELECT * FROM RTR_InsuranceLine WHERE InsuranceLine='Crime'),
	RTR_InsuranceLine_EmploymentPracticesLiab AS (SELECT * FROM RTR_InsuranceLine WHERE InsuranceLine='EmploymentPracticesLiab'),
	RTR_InsuranceLine_InlandMarine AS (SELECT * FROM RTR_InsuranceLine WHERE InsuranceLine='InlandMarine'),
	RTR_InsuranceLine_SBOPGeneralLiability AS (SELECT * FROM RTR_InsuranceLine WHERE InsuranceLine='SBOPGeneralLiability'),
	RTR_InsuranceLine_SBOPProperty AS (SELECT * FROM RTR_InsuranceLine WHERE InsuranceLine='SBOPProperty'),
	RTR_InsuranceLine_CommercialUmbrella AS (SELECT * FROM RTR_InsuranceLine WHERE InsuranceLine='CommercialUmbrella'),
	RTR_InsuranceLine_DEFAULT1 AS (SELECT * FROM RTR_InsuranceLine WHERE NOT ( (InsuranceLine='Property') OR (InsuranceLine='GeneralLiability') OR (InsuranceLine='BusinessOwners') OR (InsuranceLine='CommercialAuto') OR (InsuranceLine='Crime') OR (InsuranceLine='EmploymentPracticesLiab') OR (InsuranceLine='InlandMarine') OR (InsuranceLine='SBOPGeneralLiability') OR (InsuranceLine='SBOPProperty') OR (InsuranceLine='CommercialUmbrella') )),
	EXP_InalandMarine AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Type,
		Value,
		Scope,
		-- *INF*: IIF(Type='IRPM' AND Scope='IRPMFactor',Value,0)
		IFF(Type = 'IRPM' 
			AND Scope = 'IRPMFactor',
			Value,
			0
		) AS IRPM_Irpmfactor_IM
		FROM RTR_InsuranceLine_InlandMarine
	),
	EXP_Other_lines AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID
		FROM RTR_InsuranceLine_DEFAULT1
	),
	EXP_EmploymentPracticesLiab AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Type,
		Value,
		Scope,
		-- *INF*: IIF(Type='ScheduledMod' AND Scope='Liability',Value,0)
		IFF(Type = 'ScheduledMod' 
			AND Scope = 'Liability',
			Value,
			0
		) AS Scheduledmod_Liability_EMP
		FROM RTR_InsuranceLine_EmploymentPracticesLiab
	),
	EXP_SBOPGeneralLiability AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Type,
		Value,
		Scope
		FROM RTR_InsuranceLine_SBOPGeneralLiability
	),
	EXP_Property AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Type,
		Value,
		Scope
		FROM RTR_InsuranceLine_Property
	),
	EXP_CommercialAuto AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Type,
		Value,
		Scope
		FROM RTR_InsuranceLine_CommercialAuto
	),
	EXP_Crime AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Type,
		Value,
		Scope,
		-- *INF*: IIF(Type='ScheduledMod' AND Scope='Liability',Value,0)
		IFF(Type = 'ScheduledMod' 
			AND Scope = 'Liability',
			Value,
			0
		) AS Scheduledmod_Liability_Crime
		FROM RTR_InsuranceLine_Crime
	),
	EXP_CommercialUmbrella AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Type,
		Value,
		Scope,
		-- *INF*: IIF(Type='DCTUmbrella' AND Scope='DCTUmbrellaScheduled',Value,0)
		IFF(Type = 'DCTUmbrella' 
			AND Scope = 'DCTUmbrellaScheduled',
			Value,
			0
		) AS Scheduledmod_DCTUmbrella
		FROM RTR_InsuranceLine_CommercialUmbrella
	),
	EXP_BusinessOwners AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Type,
		Value,
		Scope,
		-- *INF*: IIF(Type='IRPM' AND Scope='IRPMFactor',Value,0)
		IFF(Type = 'IRPM' 
			AND Scope = 'IRPMFactor',
			Value,
			0
		) AS o_ScheduledMod_IRPM_BO,
		-- *INF*: IIF(Type = 'TransitionMod' AND ISNULL(Scope),Value,0)
		IFF(Type = 'TransitionMod' 
			AND Scope IS NULL,
			Value,
			0
		) AS o_TransitionMod_BO_7x,
		-- *INF*: IIF(Type = 'TransitionMod' AND Scope='IRPM',Value,0)
		IFF(Type = 'TransitionMod' 
			AND Scope = 'IRPM',
			Value,
			0
		) AS o_TransitionMod_BO_9x
		FROM RTR_InsuranceLine_BusinessOwners
	),
	EXP_SBOPProperty AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Type,
		Value,
		Scope
		FROM RTR_InsuranceLine_SBOPProperty
	),
	EXP_GeneralLiability AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Type,
		Value,
		Scope
		FROM RTR_InsuranceLine_GeneralLiability
	),
	AGG_RowsToColumns_EmploymentPracticesLiab AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Scheduledmod_Liability_EMP AS i_Scheduledmod_Liability_EMP,
		-- *INF*: Max(i_Scheduledmod_Liability_EMP)
		Max(i_Scheduledmod_Liability_EMP
		) AS o_Scheduledmod_Liability_EMP
		FROM EXP_EmploymentPracticesLiab
		GROUP BY PremiumTransactionID
	),
	AGG_RowsToColuns_BusinessOwners AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_ScheduledMod_IRPM_BO AS i_ScheduledMod_IRPM_BO,
		o_TransitionMod_BO_7x AS i_TransitionMod_BO_7x,
		o_TransitionMod_BO_9x AS i_TransitionMod_BO_9x,
		-- *INF*: Max(i_ScheduledMod_IRPM_BO)
		Max(i_ScheduledMod_IRPM_BO
		) AS o_ScheduledMod_IRPM_BO,
		-- *INF*: Max(i_TransitionMod_BO_7x)
		Max(i_TransitionMod_BO_7x
		) AS o_TransitionMod_BO_7x,
		-- *INF*: Max(i_TransitionMod_BO_9x)
		Max(i_TransitionMod_BO_9x
		) AS o_TransitionMod_BO_9x
		FROM EXP_BusinessOwners
		GROUP BY PremiumTransactionID
	),
	AGG_RowsToColumns_Crime AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Scheduledmod_Liability_Crime AS i_Scheduledmod_Liability_Crime,
		-- *INF*: Max(i_Scheduledmod_Liability_Crime)
		Max(i_Scheduledmod_Liability_Crime
		) AS o_Scheduledmod_Liability_Crime
		FROM EXP_Crime
		GROUP BY PremiumTransactionID
	),
	AGG_RowsTocolumns_Otherlines AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID
		FROM EXP_Other_lines
		QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY NULL) = 1
	),
	RTE_CommercialAuto AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Type,
		Value,
		Scope
		FROM EXP_CommercialAuto
	),
	RTE_CommercialAuto_ScheduledModifier_PhysicalDamage AS (SELECT * FROM RTE_CommercialAuto WHERE Type='ScheduledMod' AND Scope='PhysicalDamage'),
	RTE_CommercialAuto_ScheduledModifier_Liability AS (SELECT * FROM RTE_CommercialAuto WHERE Type='ScheduledMod' AND Scope='Liability'),
	RTE_CommercialAuto_ExperienceModifier_PhysicalDamage AS (SELECT * FROM RTE_CommercialAuto WHERE Type='ExperienceMod' AND Scope='PhysicalDamage'),
	RTE_CommercialAuto_ExperienceModifier_Liability AS (SELECT * FROM RTE_CommercialAuto WHERE Type='ExperienceMod' AND Scope='Liability'),
	RTE_Property AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Type,
		Value,
		Scope
		FROM EXP_Property
	),
	RTE_Property_OtherMod_PreferredPropertyCreditFactor AS (SELECT * FROM RTE_Property WHERE Type='Other' AND Scope='PreferredProperty'),
	RTE_Property_OtherMod_MultiLocationCreditFactor AS (SELECT * FROM RTE_Property WHERE Type='Other' AND Scope='MultiLocation'),
	RTE_Property_Scheduled_Modifier AS (SELECT * FROM RTE_Property WHERE Type='IRPM' AND Scope='IRPMFactor'),
	RTE_Property_Transition_Modifier AS (SELECT * FROM RTE_Property WHERE Type='TransitionMod' AND (ISNULL(Scope) OR Scope = 'IRPM')),
	RTE_Property_OtherMod_MLPDC AS (SELECT * FROM RTE_Property WHERE Type='Other' AND Scope='MLPDC'),
	RTE_Property_OtherMod_StateOwnedPropertyCredit AS (SELECT * FROM RTE_Property WHERE Type='Other' AND Scope='StateOwned'),
	RTE_SBOPProperty AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Type,
		Value,
		Scope
		FROM EXP_SBOPProperty
	),
	RTE_SBOPProperty_OtherMod_PreferredProperty AS (SELECT * FROM RTE_SBOPProperty WHERE Type='Other' AND Scope='PreferredProperty'),
	RTE_SBOPProperty_OtherMod_Multilocation AS (SELECT * FROM RTE_SBOPProperty WHERE Type='Other' AND Scope='MultiLocation'),
	RTE_SBOPProperty_ScheduledMod AS (SELECT * FROM RTE_SBOPProperty WHERE Type = 'IRPM' AND  Scope = 'IRPMFactor'),
	RTE_SBOPProperty_TransitionMod AS (SELECT * FROM RTE_SBOPProperty WHERE Type='TransitionMod' AND ISNULL(Scope)),
	AGG_RowsToColumns_InlandMarine AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		IRPM_Irpmfactor_IM,
		-- *INF*: Max(IRPM_Irpmfactor_IM)
		Max(IRPM_Irpmfactor_IM
		) AS o_IRPM_Irpmfactor_IM
		FROM EXP_InalandMarine
		GROUP BY PremiumTransactionID
	),
	RTE_GeneralLiabilty AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Type,
		Value,
		Scope
		FROM EXP_GeneralLiability
	),
	RTE_GeneralLiabilty_ScheduledModifier AS (SELECT * FROM RTE_GeneralLiabilty WHERE Type='ScheduledMod' AND Scope='Liability'),
	RTE_GeneralLiabilty_ExperienceModifier AS (SELECT * FROM RTE_GeneralLiabilty WHERE Type='ExperienceMod' AND Scope='Liability'),
	RTE_GeneralLiabilty_TransitionModifier AS (SELECT * FROM RTE_GeneralLiabilty WHERE Type='TransitionMod' AND Scope='IRPM'),
	RTE_SBOPGeneralLiabilty AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Type,
		Value,
		Scope
		FROM EXP_SBOPGeneralLiability
	),
	RTE_SBOPGeneralLiabilty_ScheduledModifier AS (SELECT * FROM RTE_SBOPGeneralLiabilty WHERE Type='ScheduledMod' AND Scope='Liability'),
	RTE_SBOPGeneralLiabilty_ExperienceModifier AS (SELECT * FROM RTE_SBOPGeneralLiabilty WHERE Type='ExperienceMod' AND Scope='Liability'),
	AGG_RowsToColumns_CommercialUmbrella AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Scheduledmod_DCTUmbrella AS i_Scheduledmod_DCTUmbrella,
		-- *INF*: max(i_Scheduledmod_DCTUmbrella)
		max(i_Scheduledmod_DCTUmbrella
		) AS o_Scheduledmod_DCTUmbrella
		FROM EXP_CommercialUmbrella
		GROUP BY PremiumTransactionID
	),
	EXP_StandardizeModifier_EmploymentPracticesLiab AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_Scheduledmod_Liability_EMP AS i_Scheduledmod_Liability_EMP,
		1.0 AS OtherModifiedFactor_EMP,
		i_Scheduledmod_Liability_EMP AS ScheduleModifiedFactor_EMP,
		1.0 AS ExperienceModifiedFactor_EMP,
		1.0 AS TransitionFactor_EMP
		FROM AGG_RowsToColumns_EmploymentPracticesLiab
	),
	EXP_Standardizemodifier_Otherlines AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		1.0 AS OtherModifiedFactor,
		1.0 AS ScheduleModifiedFactor,
		1.0 AS ExperienceModifiedFactor,
		1.0 AS TransitionFactor
		FROM AGG_RowsTocolumns_Otherlines
	),
	EXP_StandardizeModifier_Crime AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_Scheduledmod_Liability_Crime AS i_Scheduledmod_Liability_Crime,
		1.0 AS OtherModifiedFactor_crime,
		i_Scheduledmod_Liability_Crime AS ScheduleModifiedFactor_crime,
		1.0 AS ExperienceModifiedFactor_crime,
		1.0 AS TransitionFactor_crime
		FROM AGG_RowsToColumns_Crime
	),
	EXP_TransitionMod_Property AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Value AS i_Value,
		-- *INF*: IIF(ISNULL(i_Value),0,i_Value)
		IFF(i_Value IS NULL,
			0,
			i_Value
		) AS o_TransitionMod
		FROM RTE_Property_Transition_Modifier
	),
	EXP_SBOP_Property_TransitionMod AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Value AS i_Value,
		-- *INF*: IIF(ISNULL(i_Value),0,i_Value)
		IFF(i_Value IS NULL,
			0,
			i_Value
		) AS o_TransitionMod
		FROM RTE_SBOPProperty_TransitionMod
	),
	EXP_OtherMod_MLPDC AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Value AS i_Value,
		-- *INF*: IIF(ISNULL(i_Value),0,i_Value)
		IFF(i_Value IS NULL,
			0,
			i_Value
		) AS o_OtherMod_MLPDC
		FROM RTE_Property_OtherMod_MLPDC
	),
	EXP_OtherMod_StateOwnedPropertyCredit AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Value AS i_Value,
		-- *INF*: IIF(ISNULL(i_Value),0,i_Value)
		IFF(i_Value IS NULL,
			0,
			i_Value
		) AS o_OtherMod_StateOwnedPropertyCredit
		FROM RTE_Property_OtherMod_StateOwnedPropertyCredit
	),
	EXP_SBOPGL_ScheduledModifier AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Value AS i_Value,
		-- *INF*: IIF(ISNULL(i_Value),0,i_Value)
		IFF(i_Value IS NULL,
			0,
			i_Value
		) AS o_ScheduledModifier
		FROM RTE_SBOPGeneralLiabilty_ScheduledModifier
	),
	EXP_SBOPGL_ExperienceModifier AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Value AS i_Value,
		-- *INF*: IIF(ISNULL(i_Value),0,i_Value)
		IFF(i_Value IS NULL,
			0,
			i_Value
		) AS o_ExperienceModifier
		FROM RTE_SBOPGeneralLiabilty_ExperienceModifier
	),
	EXP_CA_SchMod_PhysicalDamage AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Value AS i_Value,
		-- *INF*: IIF(ISNULL(i_Value),0,i_Value)
		IFF(i_Value IS NULL,
			0,
			i_Value
		) AS o_Scheduledmod_PhyicalDamage
		FROM RTE_CommercialAuto_ScheduledModifier_PhysicalDamage
	),
	EXP_StandardizeModifier_CommercialUmbrella AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_Scheduledmod_DCTUmbrella AS i_Scheduledmod_DCTUmbrella,
		1.0 AS OtherModifiedFactor_CU,
		i_Scheduledmod_DCTUmbrella AS ScheduleModifiedFactor_CU,
		1.0 AS ExperienceModifiedFactor_CU,
		1.0 AS TransitionFactor_CU
		FROM AGG_RowsToColumns_CommercialUmbrella
	),
	EXP_GL_ScheduleModifer AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Value AS i_Value,
		-- *INF*: iif(isnull(i_Value),0,i_Value)
		IFF(i_Value IS NULL,
			0,
			i_Value
		) AS o_ScheduledModifer
		FROM RTE_GeneralLiabilty_ScheduledModifier
	),
	EXP_GL_ExperienceModifier AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Value AS i_Value,
		-- *INF*: IIF(ISNULL(i_Value),0,i_Value)
		IFF(i_Value IS NULL,
			0,
			i_Value
		) AS o_ExperienceModifier
		FROM RTE_GeneralLiabilty_ExperienceModifier
	),
	EXP_CA_SchMod_Liability AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Value AS i_Value,
		-- *INF*: IIF(ISNULL(i_Value),0,i_Value)
		IFF(i_Value IS NULL,
			0,
			i_Value
		) AS o_Scheduledmod_Liability
		FROM RTE_CommercialAuto_ScheduledModifier_Liability
	),
	EXP_StandardizeModifier_BusinessOwners AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_ScheduledMod_IRPM_BO AS i_ScheduledMod_IRPM_BO,
		o_TransitionMod_BO_7x AS i_TransitionMod_BO_7x,
		o_TransitionMod_BO_9x AS i_TransitionMod_BO_9x,
		1.0 AS OtherModifiedFactor_BO,
		i_ScheduledMod_IRPM_BO AS ScheduleModifiedFactor_BO,
		1.0 AS ExperienceModifiedFactor_BO,
		-- *INF*: IIF(i_TransitionMod_BO_9x=0 OR ISNULL(i_TransitionMod_BO_9x) OR i_TransitionMod_BO_9x=1,i_TransitionMod_BO_7x, i_TransitionMod_BO_9x
		-- )
		-- 
		-- --Pick the 9x Template Transition factor if available else grab the 7.x. If either are not present then it is defaulted to 1.0 in EXP_Modifier_Null_Handle
		IFF(i_TransitionMod_BO_9x = 0 
			OR i_TransitionMod_BO_9x IS NULL 
			OR i_TransitionMod_BO_9x = 1,
			i_TransitionMod_BO_7x,
			i_TransitionMod_BO_9x
		) AS TransitionFactor_BO
		FROM AGG_RowsToColuns_BusinessOwners
	),
	EXP_StandardizeModifier_InlandMrine AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_IRPM_Irpmfactor_IM AS i_IRPM_Irpmfactor_IM,
		1.0 AS OtherModifiedFactor_IM,
		i_IRPM_Irpmfactor_IM AS ScheduleModifiedFactor_IM,
		1.0 AS ExperienceModifiedFactor_IM,
		1.0 AS TransitionFactor_IM
		FROM AGG_RowsToColumns_InlandMarine
	),
	EXP_CA_ExpMod_PhysicalDamage AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Value AS i_Value,
		-- *INF*: IIF(ISNULL(i_Value),0,i_Value)
		IFF(i_Value IS NULL,
			0,
			i_Value
		) AS o_Experiencemod_PhysicalDamage
		FROM RTE_CommercialAuto_ExperienceModifier_PhysicalDamage
	),
	EXP_CA_ExpMod_Liability AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Value AS i_Value,
		-- *INF*: IIF(ISNULL(i_Value),0,i_Value)
		IFF(i_Value IS NULL,
			0,
			i_Value
		) AS o_ExperienceModifier_Liability
		FROM RTE_CommercialAuto_ExperienceModifier_Liability
	),
	EXP_OtherMod_PreferredPropertyCreditFactor AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Value AS i_Value,
		-- *INF*: IIF(ISNULL(i_Value),0,i_Value)
		IFF(i_Value IS NULL,
			0,
			i_Value
		) AS o_OtherMod_PreferredPropertyCreditFactor
		FROM RTE_Property_OtherMod_PreferredPropertyCreditFactor
	),
	EXP_OtherMod_MultiLocationCreditFactor AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Value AS i_Value,
		-- *INF*: IIF(ISNULL(i_Value),0,i_Value)
		IFF(i_Value IS NULL,
			0,
			i_Value
		) AS o_OtherMod_MultiLocationCreditFactor
		FROM RTE_Property_OtherMod_MultiLocationCreditFactor
	),
	EXP_GL_TransitionModifier AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Value AS i_Value,
		-- *INF*: IIF(ISNULL(i_Value),0,i_Value)
		IFF(i_Value IS NULL,
			0,
			i_Value
		) AS o_ExperienceModifier
		FROM RTE_GeneralLiabilty_TransitionModifier
	),
	EXP_ScheduledMod AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Value AS i_Value,
		-- *INF*: IIF(ISNULL(i_Value),0,i_Value)
		IFF(i_Value IS NULL,
			0,
			i_Value
		) AS o_Scheduledmod
		FROM RTE_Property_Scheduled_Modifier
	),
	EXP_OtherMod_PreferredProperty AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Value AS i_Value,
		-- *INF*: IIF(ISNULL(i_Value),i_Value)
		IFF(i_Value IS NULL,
			i_Value
		) AS o_OtherMod_PreferredProperty
		FROM RTE_SBOPProperty_OtherMod_PreferredProperty
	),
	EXP_OtherMod_Multilocation AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Value AS i_Value,
		-- *INF*: IIF(ISNULL(i_Value),0,i_Value)
		IFF(i_Value IS NULL,
			0,
			i_Value
		) AS o_OtherMod_Multilocation
		FROM RTE_SBOPProperty_OtherMod_Multilocation
	),
	EXP_SBOP_Property_ScheduledMod AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		Value AS i_Value,
		-- *INF*: IIF(ISNULL(i_Value),0,i_Value)
		IFF(i_Value IS NULL,
			0,
			i_Value
		) AS o_ScheduledMod
		FROM RTE_SBOPProperty_ScheduledMod
	),
	AGG_RowsToColumns_TransitionMod AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_TransitionMod AS i_TransitionMod,
		-- *INF*: Max(i_TransitionMod)
		Max(i_TransitionMod
		) AS o_TransitionMod,
		0 AS o_ScheduledMod,
		0 AS o_OtherModPreferredPropertyCreditFactor,
		0 AS o_OtherModMultiLocationCredit,
		0 AS o_OtherModMLPDC,
		0 AS o_OtherModStateOwnedPropertyCredit
		FROM EXP_TransitionMod_Property
		GROUP BY PremiumTransactionID
	),
	AGG_RowsToColumns_TransitionMod_SBOPProperty AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_TransitionMod AS i_TransitionMod,
		-- *INF*: Max(i_TransitionMod)
		Max(i_TransitionMod
		) AS o_TransitionMod,
		0 AS o_ScheduledMod,
		0 AS o_OtherMod_PreferredProperty,
		0 AS o_OtherMod_Multilocation
		FROM EXP_SBOP_Property_TransitionMod
		GROUP BY PremiumTransactionID
	),
	AGG_RowsToColumns_OtherMod_MLPDC AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_OtherMod_MLPDC AS i_OtherMod_MLPDC,
		-- *INF*: Max(i_OtherMod_MLPDC)
		Max(i_OtherMod_MLPDC
		) AS o_OtherMod_MLPDC,
		0 AS o_OtherModPreferredPropertyCreditFactor,
		0 AS o_OtherMod_MultiLocationCreditFactor,
		0 AS o_ScheduledMod,
		0 AS o_TransitionMod,
		0 AS o_OtherModStateOwnedPropertyCredit
		FROM EXP_OtherMod_MLPDC
		GROUP BY PremiumTransactionID
	),
	AGG_RowsToColumns_SBOPGL_ScheduleModifier AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_ScheduledModifier AS i_ScheduledModifier,
		-- *INF*: MAX(i_ScheduledModifier)
		MAX(i_ScheduledModifier
		) AS o_ScheduledModifier,
		0 AS o_ExperienceModifier
		FROM EXP_SBOPGL_ScheduledModifier
		GROUP BY PremiumTransactionID
	),
	AGG_RowsToColumns_SBOPGL_ExperienceModifier AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_ExperienceModifier AS i_ExperienceModifier,
		-- *INF*: Max(i_ExperienceModifier)
		Max(i_ExperienceModifier
		) AS o_ExperienceModifier,
		0 AS o_ScheduledModifier
		FROM EXP_SBOPGL_ExperienceModifier
		GROUP BY PremiumTransactionID
	),
	AGG_RowsToColuns_CA_SchMod_PhysicalDamage AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_Scheduledmod_PhyicalDamage AS i_Scheduledmod_PhyicalDamage,
		-- *INF*: MAX(i_Scheduledmod_PhyicalDamage)
		MAX(i_Scheduledmod_PhyicalDamage
		) AS o_Scheduledmod_PhyicalDamage,
		0 AS o_Scheduledmod_Liability,
		0 AS o_Experiencemod_PhysicalDamage,
		0 AS o_Experiencemod_Liability
		FROM EXP_CA_SchMod_PhysicalDamage
		GROUP BY PremiumTransactionID
	),
	AGG_RowsToColuns_CA_SchMod_Liability AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_Scheduledmod_Liability AS i_Scheduledmod_Liability,
		-- *INF*: Max(i_Scheduledmod_Liability)
		Max(i_Scheduledmod_Liability
		) AS o_Scheduledmod_Liability,
		0 AS o_Scheduledmod_PhysicalDamage,
		0 AS o_Experiencemod_PhysicalDamage,
		0 AS o_Experiencemod_Liability
		FROM EXP_CA_SchMod_Liability
		GROUP BY PremiumTransactionID
	),
	AGG_RowsToColuns_CA_ExpMod_PhysicalDamage AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_Experiencemod_PhysicalDamage AS i_Experiencemod_PhysicalDamage,
		-- *INF*: Max(i_Experiencemod_PhysicalDamage)
		Max(i_Experiencemod_PhysicalDamage
		) AS o_Experiencemod_PhysicalDamage,
		0 AS o_Scheduledmod_PhysicalDamage,
		0 AS o_Scheduledmod_Liability,
		0 AS o_Experiencemod_Liability
		FROM EXP_CA_ExpMod_PhysicalDamage
		GROUP BY PremiumTransactionID
	),
	AGG_RowsToColuns_CA_ExpMod_Liability AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_ExperienceModifier_Liability AS i_ExperienceModifier_Liability,
		-- *INF*: Max(i_ExperienceModifier_Liability)
		Max(i_ExperienceModifier_Liability
		) AS o_ExperienceModifier_Liability,
		0 AS o_Scheduledmod_PhysicalDamage,
		0 AS o_Scheduledmod_Liability,
		0 AS o_Experiencemod_PhysicalDamage
		FROM EXP_CA_ExpMod_Liability
		GROUP BY PremiumTransactionID
	),
	AGG_RowsToColumns_OtherMod_PreferredProperty AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_OtherMod_PreferredPropertyCreditFactor AS i_OtherMod_PreferredPropertyCreditFactor,
		-- *INF*: Max(i_OtherMod_PreferredPropertyCreditFactor)
		Max(i_OtherMod_PreferredPropertyCreditFactor
		) AS o_OtherMod_PreferredPropertyCreditFactor,
		0 AS o_OtherModMultiLocationCredit,
		0 AS o_OtherModMLPDC,
		0 AS o_ScheduledMod,
		0 AS o_TransitionMod,
		0 AS o_OtherModStateOwnedPropertyCredit
		FROM EXP_OtherMod_PreferredPropertyCreditFactor
		GROUP BY PremiumTransactionID
	),
	AGG_RowsToColumns_OtherMod_MultiLocationCredit AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_OtherMod_MultiLocationCreditFactor AS i_OtherMod_MultiLocationCreditFactor,
		-- *INF*: Max(i_OtherMod_MultiLocationCreditFactor)
		Max(i_OtherMod_MultiLocationCreditFactor
		) AS o_OtherMod_MultiLocationCreditFactor,
		0 AS o_OtherModPreferredPropertyCreditFactor,
		0 AS o_OtherModMLPDC,
		0 AS o_ScheduledMod,
		0 AS o_TransitionMod,
		0 AS o_OtherModStateOwnedPropertyCredit
		FROM EXP_OtherMod_MultiLocationCreditFactor
		GROUP BY PremiumTransactionID
	),
	AGG_RowsToColumns_ScheduledMod AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_Scheduledmod AS i_ScheduledMod,
		-- *INF*: Max(i_ScheduledMod)
		Max(i_ScheduledMod
		) AS o_ScheduledMod,
		0 AS o_OtherModPreferredPropertyCreditFactor,
		0 AS o_OtherModMultiLocationCredit,
		0 AS o_OtherModMLPDC,
		0 AS o_TransitionMod,
		0 AS o_OtherModStateOwnedPropertyCredit
		FROM EXP_ScheduledMod
		GROUP BY PremiumTransactionID
	),
	AGG_RowsToColumns_OtherMod_StateOwnedPropertyCredit AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_OtherMod_StateOwnedPropertyCredit AS i_OtherMod_StateOwnedPropertyCredit,
		-- *INF*: Max(i_OtherMod_StateOwnedPropertyCredit)
		Max(i_OtherMod_StateOwnedPropertyCredit
		) AS o_OtherMod_StateOwnedPropertyCredit,
		0 AS o_OtherModPreferredPropertCredit,
		0 AS o_OtherModMultiLocationCredit,
		0 AS o_OtherModMLPDC,
		0 AS o_ScheduledMod,
		0 AS o_TransitionMod
		FROM EXP_OtherMod_StateOwnedPropertyCredit
		GROUP BY PremiumTransactionID
	),
	AGG_RowsToColumns_OtherMod_PreferredProperty_SBOPProperty AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_OtherMod_PreferredProperty AS i_OtherMod_PreferredProperty,
		-- *INF*: Max(i_OtherMod_PreferredProperty)
		Max(i_OtherMod_PreferredProperty
		) AS o_OtherMod_PreferredProperty,
		0 AS o_OtherMod_Multilocation,
		0 AS o_ScheduledMod,
		0 AS o_TransitionMod
		FROM EXP_OtherMod_PreferredProperty
		GROUP BY PremiumTransactionID
	),
	AGG_RowsToColumns_OtherMod_Multilocation_SBOPProperty AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_OtherMod_Multilocation AS i_OtherMod_Multilocation,
		-- *INF*: Max(i_OtherMod_Multilocation)
		Max(i_OtherMod_Multilocation
		) AS o_OtherMod_Multilocation,
		0 AS o_OtherMod_PreferredProperty,
		0 AS o_ScheduledMod,
		0 AS o_TransitionMod
		FROM EXP_OtherMod_Multilocation
		GROUP BY PremiumTransactionID
	),
	AGG_RowsToColumns_ScheduledMod_SBOPProperty AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_ScheduledMod AS i_ScheduledMod,
		-- *INF*: Max(i_ScheduledMod)
		Max(i_ScheduledMod
		) AS o_ScheduledMod,
		0 AS o_OtherMod_PreferredProperty,
		0 AS o_OtherMod_Multilocation,
		0 AS o_TransitionMod
		FROM EXP_SBOP_Property_ScheduledMod
		GROUP BY PremiumTransactionID
	),
	AGG_RowsToColumns_GL_TransitionModifier AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_ExperienceModifier AS i_TransitionModifier,
		-- *INF*: Max(i_TransitionModifier)
		Max(i_TransitionModifier
		) AS o_TransitionModifier,
		0 AS o_ExperienceModifier,
		0 AS o_ScheduledModifier
		FROM EXP_GL_TransitionModifier
		GROUP BY PremiumTransactionID
	),
	AGG_RowsToColumns_GL_ScheduleModifier AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_ScheduledModifer AS i_Scheduledmod_Liability_GL,
		0 AS o_TransitionModifier,
		0 AS o_ExperienceModifier,
		-- *INF*: max(i_Scheduledmod_Liability_GL)
		max(i_Scheduledmod_Liability_GL
		) AS o_Scheduledmod_Liability_GL
		FROM EXP_GL_ScheduleModifer
		GROUP BY PremiumTransactionID
	),
	AGG_RowsToColumns_GL_ExperienceModifier AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_ExperienceModifier AS i_ExperienceModifier,
		0 AS o_TransitionModifier,
		-- *INF*: Max(i_ExperienceModifier)
		Max(i_ExperienceModifier
		) AS o_ExperienceModifier,
		0 AS o_ScheduledModifier
		FROM EXP_GL_ExperienceModifier
		GROUP BY PremiumTransactionID
	),
	UN_GL_Modifier AS (
		SELECT PremiumTransactionID, PremiumTransactionAKID, o_Scheduledmod_Liability_GL AS ScheduledModifier, o_ExperienceModifier AS ExperienceModifier, o_TransitionModifier AS TransitionModifier
		FROM AGG_RowsToColumns_GL_ScheduleModifier
		UNION
		SELECT PremiumTransactionID, PremiumTransactionAKID, o_ScheduledModifier AS ScheduledModifier, o_ExperienceModifier AS ExperienceModifier, o_TransitionModifier AS TransitionModifier
		FROM AGG_RowsToColumns_GL_ExperienceModifier
		UNION
		SELECT PremiumTransactionID, PremiumTransactionAKID, o_ScheduledModifier AS ScheduledModifier, o_ExperienceModifier AS ExperienceModifier, o_TransitionModifier AS TransitionModifier
		FROM AGG_RowsToColumns_GL_TransitionModifier
	),
	UN_SBOPGL_Modifier AS (
		SELECT PremiumTransactionID, PremiumTransactionAKID, o_ScheduledModifier AS ScheduledModifier, o_ExperienceModifier AS ExperienceModifier
		FROM AGG_RowsToColumns_SBOPGL_ScheduleModifier
		UNION
		SELECT PremiumTransactionID, PremiumTransactionAKID, o_ScheduledModifier AS ScheduledModifier, o_ExperienceModifier AS ExperienceModifier
		FROM AGG_RowsToColumns_SBOPGL_ExperienceModifier
	),
	UN_CommercialAuto AS (
		SELECT PremiumTransactionID AS PremiumTransactionId, PremiumTransactionAKID AS PremiumTransactionAKId, o_Scheduledmod_PhyicalDamage AS Scheduledmod_PhysicalDamage, o_Scheduledmod_Liability AS Scheduledmod_Liability, o_Experiencemod_PhysicalDamage AS Experiencemod_PhysicalDamage, o_Experiencemod_Liability AS Experiencemod_Liability
		FROM AGG_RowsToColuns_CA_SchMod_PhysicalDamage
		UNION
		SELECT PremiumTransactionID AS PremiumTransactionId, PremiumTransactionAKID AS PremiumTransactionAKId, o_Scheduledmod_PhysicalDamage AS Scheduledmod_PhysicalDamage, o_Scheduledmod_Liability AS Scheduledmod_Liability, o_Experiencemod_PhysicalDamage AS Experiencemod_PhysicalDamage, o_Experiencemod_Liability AS Experiencemod_Liability
		FROM AGG_RowsToColuns_CA_SchMod_Liability
		UNION
		SELECT PremiumTransactionID AS PremiumTransactionId, PremiumTransactionAKID AS PremiumTransactionAKId, o_Scheduledmod_PhysicalDamage AS Scheduledmod_PhysicalDamage, o_Scheduledmod_Liability AS Scheduledmod_Liability, o_Experiencemod_PhysicalDamage AS Experiencemod_PhysicalDamage, o_Experiencemod_Liability AS Experiencemod_Liability
		FROM AGG_RowsToColuns_CA_ExpMod_PhysicalDamage
		UNION
		SELECT PremiumTransactionID AS PremiumTransactionId, PremiumTransactionAKID AS PremiumTransactionAKId, o_Scheduledmod_PhysicalDamage AS Scheduledmod_PhysicalDamage, o_Scheduledmod_Liability AS Scheduledmod_Liability, o_Experiencemod_PhysicalDamage AS Experiencemod_PhysicalDamage, o_ExperienceModifier_Liability AS Experiencemod_Liability
		FROM AGG_RowsToColuns_CA_ExpMod_Liability
	),
	UN_SBOPProperty AS (
		SELECT PremiumTransactionID AS PremiumTransactionId, PremiumTransactionAKID AS PremiumTransactionAKId, o_OtherMod_PreferredProperty AS OtherMod_PreferredProperty, o_OtherMod_Multilocation AS OtherMod_Multilocation, o_ScheduledMod AS ScheduledMod, o_TransitionMod AS TransitionMod
		FROM AGG_RowsToColumns_OtherMod_PreferredProperty_SBOPProperty
		UNION
		SELECT PremiumTransactionID AS PremiumTransactionId, PremiumTransactionAKID AS PremiumTransactionAKId, o_OtherMod_PreferredProperty AS OtherMod_PreferredProperty, o_OtherMod_Multilocation AS OtherMod_Multilocation, o_ScheduledMod AS ScheduledMod, o_TransitionMod AS TransitionMod
		FROM AGG_RowsToColumns_OtherMod_Multilocation_SBOPProperty
		UNION
		SELECT PremiumTransactionID AS PremiumTransactionId, PremiumTransactionAKID AS PremiumTransactionAKId, o_OtherMod_PreferredProperty AS OtherMod_PreferredProperty, o_OtherMod_Multilocation AS OtherMod_Multilocation, o_ScheduledMod AS ScheduledMod, o_TransitionMod AS TransitionMod
		FROM AGG_RowsToColumns_ScheduledMod_SBOPProperty
		UNION
		SELECT PremiumTransactionID AS PremiumTransactionId, PremiumTransactionAKID AS PremiumTransactionAKId, o_OtherMod_PreferredProperty AS OtherMod_PreferredProperty, o_OtherMod_Multilocation AS OtherMod_Multilocation, o_ScheduledMod AS ScheduledMod, o_TransitionMod AS TransitionMod
		FROM AGG_RowsToColumns_TransitionMod_SBOPProperty
	),
	Union_Property AS (
		SELECT PremiumTransactionID, PremiumTransactionAKID, o_OtherMod_PreferredPropertyCreditFactor AS OtherMod_PreferredProperyCredit, o_OtherModMultiLocationCredit AS OtherMod_MultiLocationCredit, o_OtherModMLPDC AS OtherMod_MLPDC, o_ScheduledMod AS ScheduledMod, o_TransitionMod AS TransitionMod, o_OtherModStateOwnedPropertyCredit AS OtherMod_StateOwnedPropertyCredit
		FROM AGG_RowsToColumns_OtherMod_PreferredProperty
		UNION
		SELECT PremiumTransactionID, PremiumTransactionAKID, o_OtherModPreferredPropertyCreditFactor AS OtherMod_PreferredProperyCredit, o_OtherMod_MultiLocationCreditFactor AS OtherMod_MultiLocationCredit, o_OtherModMLPDC AS OtherMod_MLPDC, o_ScheduledMod AS ScheduledMod, o_TransitionMod AS TransitionMod, o_OtherModStateOwnedPropertyCredit AS OtherMod_StateOwnedPropertyCredit
		FROM AGG_RowsToColumns_OtherMod_MultiLocationCredit
		UNION
		SELECT PremiumTransactionID, PremiumTransactionAKID, o_OtherModPreferredPropertyCreditFactor AS OtherMod_PreferredProperyCredit, o_OtherMod_MultiLocationCreditFactor AS OtherMod_MultiLocationCredit, o_OtherMod_MLPDC AS OtherMod_MLPDC, o_ScheduledMod AS ScheduledMod, o_TransitionMod AS TransitionMod, o_OtherModStateOwnedPropertyCredit AS OtherMod_StateOwnedPropertyCredit
		FROM AGG_RowsToColumns_OtherMod_MLPDC
		UNION
		SELECT PremiumTransactionID, PremiumTransactionAKID, o_OtherModPreferredPropertyCreditFactor AS OtherMod_PreferredProperyCredit, o_OtherModMultiLocationCredit AS OtherMod_MultiLocationCredit, o_OtherModMLPDC AS OtherMod_MLPDC, o_ScheduledMod AS ScheduledMod, o_TransitionMod AS TransitionMod, o_OtherModStateOwnedPropertyCredit AS OtherMod_StateOwnedPropertyCredit
		FROM AGG_RowsToColumns_ScheduledMod
		UNION
		SELECT PremiumTransactionID, PremiumTransactionAKID, o_OtherModPreferredPropertyCreditFactor AS OtherMod_PreferredProperyCredit, o_OtherModMultiLocationCredit AS OtherMod_MultiLocationCredit, o_OtherModMLPDC AS OtherMod_MLPDC, o_ScheduledMod AS ScheduledMod, o_TransitionMod AS TransitionMod, o_OtherModStateOwnedPropertyCredit AS OtherMod_StateOwnedPropertyCredit
		FROM AGG_RowsToColumns_TransitionMod
		UNION
		SELECT PremiumTransactionID, PremiumTransactionAKID, o_OtherModPreferredPropertCredit AS OtherMod_PreferredProperyCredit, o_OtherModMultiLocationCredit AS OtherMod_MultiLocationCredit, o_OtherModMLPDC AS OtherMod_MLPDC, o_ScheduledMod AS ScheduledMod, o_TransitionMod AS TransitionMod, o_OtherMod_StateOwnedPropertyCredit AS OtherMod_StateOwnedPropertyCredit
		FROM AGG_RowsToColumns_OtherMod_StateOwnedPropertyCredit
	),
	Agg_RowToColumn_CommercialAuto AS (
		SELECT
		PremiumTransactionId,
		PremiumTransactionAKId,
		Scheduledmod_PhysicalDamage AS i_Scheduledmod_PhysicalDamage,
		-- *INF*: Max(i_Scheduledmod_PhysicalDamage)
		Max(i_Scheduledmod_PhysicalDamage
		) AS o_Scheduledmod_PhysicalDamage,
		Scheduledmod_Liability AS i_Scheduledmod_Liability,
		-- *INF*: Max(i_Scheduledmod_Liability)
		Max(i_Scheduledmod_Liability
		) AS o_Scheduledmod_Liability,
		Experiencemod_PhysicalDamage AS i_Experiencemod_PhysicalDamage,
		-- *INF*: Max(i_Experiencemod_PhysicalDamage)
		Max(i_Experiencemod_PhysicalDamage
		) AS o_Experiencemod_PhysicalDamage,
		Experiencemod_Liability AS i_Experiencemod_Liability,
		-- *INF*: Max(i_Experiencemod_Liability)
		Max(i_Experiencemod_Liability
		) AS o_Experiencemod_Liability
		FROM UN_CommercialAuto
		GROUP BY PremiumTransactionId
	),
	Agg_RowtoColumn_Property AS (
		SELECT
		PremiumTransactionID AS PremiumTransactionId,
		PremiumTransactionAKID AS PremiumTransactionAKId,
		OtherMod_PreferredProperyCredit AS i_OtherModPreferredPropertyCreditFactor,
		-- *INF*: Max(i_OtherModPreferredPropertyCreditFactor)
		Max(i_OtherModPreferredPropertyCreditFactor
		) AS o_OtherModPreferredPropertyCreditFactor,
		OtherMod_MultiLocationCredit AS i_OtherModMultiLocationCredit,
		-- *INF*: Max(i_OtherModMultiLocationCredit)
		Max(i_OtherModMultiLocationCredit
		) AS o_OtherModMultiLocationCredit,
		OtherMod_MLPDC AS i_OtherMod_MLPDC,
		-- *INF*: Max(i_OtherMod_MLPDC)
		Max(i_OtherMod_MLPDC
		) AS o_OtherMod_MLPDC,
		ScheduledMod AS i_ScheduledMod,
		-- *INF*: Max(i_ScheduledMod)
		Max(i_ScheduledMod
		) AS o_ScheduledMod,
		TransitionMod AS i_TransitionMod,
		-- *INF*: Max(i_TransitionMod)
		Max(i_TransitionMod
		) AS o_TransitionMod,
		OtherMod_StateOwnedPropertyCredit AS i_OtherMod_StateOwnedPropertyCredit,
		-- *INF*: Max(i_OtherMod_StateOwnedPropertyCredit)
		Max(i_OtherMod_StateOwnedPropertyCredit
		) AS o_OtherModStateOwnedPropertyCredit
		FROM Union_Property
		GROUP BY PremiumTransactionId
	),
	Agg_RowToColumn_GeneralLiability AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		ScheduledModifier AS i_ScheduledModifier,
		-- *INF*: Max(i_ScheduledModifier)
		Max(i_ScheduledModifier
		) AS o_ScheduledModifier,
		ExperienceModifier AS i_ExperienceModifier,
		-- *INF*: Max(i_ExperienceModifier)
		Max(i_ExperienceModifier
		) AS o_ExperienceModifier,
		TransitionModifier AS i_TransitionModifier,
		-- *INF*: Max(i_TransitionModifier)
		Max(i_TransitionModifier
		) AS o_TransitionModifier
		FROM UN_GL_Modifier
		GROUP BY PremiumTransactionID
	),
	Agg_RowToColumn_SBOPGeneralLiability AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		ScheduledModifier AS i_ScheduledModifier,
		-- *INF*: Max(i_ScheduledModifier)
		Max(i_ScheduledModifier
		) AS o_ScheduledModifier,
		ExperienceModifier AS i_ExperienceModifier,
		-- *INF*: Max(i_ExperienceModifier)
		Max(i_ExperienceModifier
		) AS o_ExperienceModifier
		FROM UN_SBOPGL_Modifier
		GROUP BY PremiumTransactionID
	),
	Agg_RowToColumn_SBOPProperty AS (
		SELECT
		PremiumTransactionId,
		PremiumTransactionAKId,
		OtherMod_PreferredProperty AS i_OtherMod_PreferredProperty,
		-- *INF*: Max(i_OtherMod_PreferredProperty)
		Max(i_OtherMod_PreferredProperty
		) AS o_OtherMod_PreferredProperty,
		OtherMod_Multilocation AS i_OtherMod_Multilocation,
		-- *INF*: Max(i_OtherMod_Multilocation)
		Max(i_OtherMod_Multilocation
		) AS o_OtherMod_Multilocation,
		ScheduledMod AS i_ScheduledMod,
		-- *INF*: Max(i_ScheduledMod)
		Max(i_ScheduledMod
		) AS o_ScheduledMod,
		TransitionMod AS i_TransitionMod,
		-- *INF*: Max(i_TransitionMod)
		Max(i_TransitionMod
		) AS o_TransitionMod
		FROM UN_SBOPProperty
		GROUP BY PremiumTransactionId
	),
	EXP_StandardizeModifier_SBOPProperty AS (
		SELECT
		PremiumTransactionId AS PremiumTransactionID,
		PremiumTransactionAKId AS PremiumTransactionAKID,
		o_OtherMod_PreferredProperty AS i_PreferredPropertyCreditFactor,
		o_OtherMod_Multilocation AS i_MultiLocationCreditFactor,
		o_ScheduledMod AS i_ScheduledMod,
		o_TransitionMod AS i_TransitionMod,
		i_PreferredPropertyCreditFactor AS v_PreferredPropertyCreditFactor,
		i_MultiLocationCreditFactor AS v_MultiLocationCreditFactor,
		-- *INF*: DECODE(TRUE,ISNULL(v_PreferredPropertyCreditFactor) OR v_PreferredPropertyCreditFactor=0.0,1.0,
		--                                     ISNULL(v_MultiLocationCreditFactor) OR v_MultiLocationCreditFactor=0.0,1.0,
		--                                      1 - (1 - v_PreferredPropertyCreditFactor) - (1 - v_MultiLocationCreditFactor))
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		DECODE(TRUE,
			v_PreferredPropertyCreditFactor IS NULL 
			OR v_PreferredPropertyCreditFactor = 0.0, 1.0,
			v_MultiLocationCreditFactor IS NULL 
			OR v_MultiLocationCreditFactor = 0.0, 1.0,
			1 - ( 1 - v_PreferredPropertyCreditFactor 
			) - ( 1 - v_MultiLocationCreditFactor 
			)
		) AS OtherModifiedFactor,
		i_ScheduledMod AS ScheduleModifiedFactor,
		1.0 AS ExperienceModifiedFactor,
		i_TransitionMod AS TransitionFactor
		FROM Agg_RowToColumn_SBOPProperty
	),
	EXP_StandardizeModifier_SBOPGeneralLiability AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_ScheduledModifier AS i_Scheduledmod_Liability_SBOPGL,
		o_ExperienceModifier AS i_Experiencemod_Liability_SBOPGL,
		1.0 AS OtherModifiedFactor_SGL,
		i_Scheduledmod_Liability_SBOPGL AS ScheduleModifiedFactor_SGL,
		i_Experiencemod_Liability_SBOPGL AS ExperienceModifiedFactor_SGL,
		1.0 AS TransitionFactor_SGL
		FROM Agg_RowToColumn_SBOPGeneralLiability
	),
	EXP_StandardizeModifier_GeneralLiability AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		o_ScheduledModifier AS i_Scheduledmod_Liability_GL,
		o_ExperienceModifier AS i_Experiencemod_Liability_GL,
		o_TransitionModifier AS i_TransitionMod_Liability_GL,
		1.0 AS OtherModifiedFactor_GL,
		i_Scheduledmod_Liability_GL AS ScheduleModifiedFactor_GL,
		i_Experiencemod_Liability_GL AS ExperienceModifiedFactor_GL,
		i_TransitionMod_Liability_GL AS TransitionModifierFactor_GL
		FROM Agg_RowToColumn_GeneralLiability
	),
	EXP_StandardizeModifier_CommercialAuto AS (
		SELECT
		PremiumTransactionId AS PremiumTransactionID,
		PremiumTransactionAKId AS PremiumTransactionAKID,
		o_Scheduledmod_PhysicalDamage AS i_Scheduledmod_PhysicalDamage,
		o_Scheduledmod_Liability AS i_Scheduledmod_Liability,
		o_Experiencemod_PhysicalDamage AS i_Experiencemod_PhysicalDamage,
		o_Experiencemod_Liability AS i_Experiencemod_Liability,
		1.0 AS o_OtherModifiedFactor,
		-- *INF*: IIF(ISNULL(i_Scheduledmod_PhysicalDamage)  or  i_Scheduledmod_PhysicalDamage=0,(IIF(ISNULL(i_Scheduledmod_Liability) or
		-- i_Scheduledmod_Liability=0 , 1.0,i_Scheduledmod_Liability)), i_Scheduledmod_PhysicalDamage)
		-- 
		-- 
		-- 
		-- 
		IFF(i_Scheduledmod_PhysicalDamage IS NULL 
			OR i_Scheduledmod_PhysicalDamage = 0,
			( IFF(i_Scheduledmod_Liability IS NULL 
					OR i_Scheduledmod_Liability = 0,
					1.0,
					i_Scheduledmod_Liability
				) 
			),
			i_Scheduledmod_PhysicalDamage
		) AS o_ScheduleModifiedFactor,
		-- *INF*: IIF(ISNULL(i_Experiencemod_PhysicalDamage)  or  i_Experiencemod_PhysicalDamage=0,(IIF(ISNULL(i_Experiencemod_Liability) or
		-- i_Experiencemod_Liability=0 , 1.0,i_Experiencemod_Liability)), i_Experiencemod_PhysicalDamage)
		IFF(i_Experiencemod_PhysicalDamage IS NULL 
			OR i_Experiencemod_PhysicalDamage = 0,
			( IFF(i_Experiencemod_Liability IS NULL 
					OR i_Experiencemod_Liability = 0,
					1.0,
					i_Experiencemod_Liability
				) 
			),
			i_Experiencemod_PhysicalDamage
		) AS o_ExperienceModifiedFactor,
		1.0 AS o_TransitionFactor
		FROM Agg_RowToColumn_CommercialAuto
	),
	EXP_StandardizeModifier_Property AS (
		SELECT
		PremiumTransactionId AS PremiumTransactionID,
		PremiumTransactionAKId AS PremiumTransactionAKID,
		o_OtherModPreferredPropertyCreditFactor AS i_OtherMod_PreferredPropertyCreditFactor,
		o_OtherModMultiLocationCredit AS i_OtherModMultiLocationCreditFactor,
		o_OtherMod_MLPDC AS i_OtherMod_MLPDC,
		o_OtherModStateOwnedPropertyCredit AS i_OtherModStateOwnedPropertyCredit,
		o_ScheduledMod AS i_ScheduledMod,
		o_TransitionMod AS i_TransitionMod,
		i_OtherMod_PreferredPropertyCreditFactor AS v_PreferredPropertyCreditFactor,
		i_OtherModMultiLocationCreditFactor AS v_MultiLocationCreditFactor,
		i_OtherMod_MLPDC AS v_MLPDC,
		-- *INF*: DECODE(TRUE,
		-- ISNULL(i_OtherModStateOwnedPropertyCredit),1.0,
		-- i_OtherModStateOwnedPropertyCredit = 0.0, 1.0,
		-- i_OtherModStateOwnedPropertyCredit)
		-- --default value to 1.0 when null or absent to ensure that OtherModifier doesn't evaluate to 1 and miss other contributors
		DECODE(TRUE,
			i_OtherModStateOwnedPropertyCredit IS NULL, 1.0,
			i_OtherModStateOwnedPropertyCredit = 0.0, 1.0,
			i_OtherModStateOwnedPropertyCredit
		) AS v_OtherModStateOwnedPropertyCredit,
		-- *INF*: DECODE(TRUE, ISNULL(v_PreferredPropertyCreditFactor) OR   v_PreferredPropertyCreditFactor=0.0 ,1.0,                                                                                                       
		--                                       ISNULL(v_MultiLocationCreditFactor) OR  v_MultiLocationCreditFactor=0.0,1.0,
		-- 	                                 ISNULL(v_MLPDC) OR  v_MLPDC=0.0,1.0,
		--                                        1 - ( 1 - v_PreferredPropertyCreditFactor) -  (1 - v_MultiLocationCreditFactor) - (1-v_MLPDC)- (1-v_OtherModStateOwnedPropertyCredit))       
		-- -- we do not default null SPOC to 1 because it is not present except on BLDG1 leading to other modifier not being calculated for the presence of other modifiers                               
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		DECODE(TRUE,
			v_PreferredPropertyCreditFactor IS NULL 
			OR v_PreferredPropertyCreditFactor = 0.0, 1.0,
			v_MultiLocationCreditFactor IS NULL 
			OR v_MultiLocationCreditFactor = 0.0, 1.0,
			v_MLPDC IS NULL 
			OR v_MLPDC = 0.0, 1.0,
			1 - ( 1 - v_PreferredPropertyCreditFactor 
			) - ( 1 - v_MultiLocationCreditFactor 
			) - ( 1 - v_MLPDC 
			) - ( 1 - v_OtherModStateOwnedPropertyCredit 
			)
		) AS OtherModifiedFactor,
		i_ScheduledMod AS ScheduleModifiedFactor,
		1.0 AS ExperienceModifiedFactor,
		i_TransitionMod AS TransitionFactor
		FROM Agg_RowtoColumn_Property
	),
	UN_all_InsuranceLines_new AS (
		SELECT PremiumTransactionID, PremiumTransactionAKID, OtherModifiedFactor_BO AS OtherModifiedFactor, ScheduleModifiedFactor_BO AS ScheduleModifiedFactor, ExperienceModifiedFactor_BO AS ExperienceModifiedFactor, TransitionFactor_BO AS TransitionFactor
		FROM EXP_StandardizeModifier_BusinessOwners
		UNION
		SELECT PremiumTransactionID, PremiumTransactionAKID, o_OtherModifiedFactor AS OtherModifiedFactor, o_ScheduleModifiedFactor AS ScheduleModifiedFactor, o_ExperienceModifiedFactor AS ExperienceModifiedFactor, o_TransitionFactor AS TransitionFactor
		FROM EXP_StandardizeModifier_CommercialAuto
		UNION
		SELECT PremiumTransactionID, PremiumTransactionAKID, OtherModifiedFactor_crime AS OtherModifiedFactor, ScheduleModifiedFactor_crime AS ScheduleModifiedFactor, ExperienceModifiedFactor_crime AS ExperienceModifiedFactor, TransitionFactor_crime AS TransitionFactor
		FROM EXP_StandardizeModifier_Crime
		UNION
		SELECT PremiumTransactionID, PremiumTransactionAKID, OtherModifiedFactor, ScheduleModifiedFactor, ExperienceModifiedFactor, TransitionFactor
		FROM EXP_StandardizeModifier_Property
		UNION
		SELECT PremiumTransactionID, PremiumTransactionAKID, OtherModifiedFactor_GL AS OtherModifiedFactor, ScheduleModifiedFactor_GL AS ScheduleModifiedFactor, ExperienceModifiedFactor_GL AS ExperienceModifiedFactor, TransitionModifierFactor_GL AS TransitionFactor
		FROM EXP_StandardizeModifier_GeneralLiability
		UNION
		SELECT PremiumTransactionID, PremiumTransactionAKID, OtherModifiedFactor_EMP AS OtherModifiedFactor, ScheduleModifiedFactor_EMP AS ScheduleModifiedFactor, ExperienceModifiedFactor_EMP AS ExperienceModifiedFactor, TransitionFactor_EMP AS TransitionFactor
		FROM EXP_StandardizeModifier_EmploymentPracticesLiab
		UNION
		SELECT PremiumTransactionID, PremiumTransactionAKID, OtherModifiedFactor_IM AS OtherModifiedFactor, ScheduleModifiedFactor_IM AS ScheduleModifiedFactor, ExperienceModifiedFactor_IM AS ExperienceModifiedFactor, TransitionFactor_IM AS TransitionFactor
		FROM EXP_StandardizeModifier_InlandMrine
		UNION
		SELECT PremiumTransactionID, PremiumTransactionAKID, OtherModifiedFactor_SGL AS OtherModifiedFactor, ScheduleModifiedFactor_SGL AS ScheduleModifiedFactor, ExperienceModifiedFactor_SGL AS ExperienceModifiedFactor, TransitionFactor_SGL AS TransitionFactor
		FROM EXP_StandardizeModifier_SBOPGeneralLiability
		UNION
		SELECT PremiumTransactionID, PremiumTransactionAKID, OtherModifiedFactor, ScheduleModifiedFactor, ExperienceModifiedFactor, TransitionFactor
		FROM EXP_StandardizeModifier_SBOPProperty
		UNION
		SELECT PremiumTransactionID, PremiumTransactionAKID, OtherModifiedFactor, ScheduleModifiedFactor, ExperienceModifiedFactor, TransitionFactor
		FROM EXP_Standardizemodifier_Otherlines
		UNION
		SELECT PremiumTransactionID, PremiumTransactionAKID, OtherModifiedFactor_CU AS OtherModifiedFactor, ScheduleModifiedFactor_CU AS ScheduleModifiedFactor, ExperienceModifiedFactor_CU AS ExperienceModifiedFactor, TransitionFactor_CU AS TransitionFactor
		FROM EXP_StandardizeModifier_CommercialUmbrella
	),
	EXP_Modifier_Null_Handle AS (
		SELECT
		PremiumTransactionID,
		PremiumTransactionAKID,
		OtherModifiedFactor AS i_OtherModifiedFactor,
		-- *INF*: iif(isnull(i_OtherModifiedFactor) or i_OtherModifiedFactor=0.0,1.0,i_OtherModifiedFactor)
		IFF(i_OtherModifiedFactor IS NULL 
			OR i_OtherModifiedFactor = 0.0,
			1.0,
			i_OtherModifiedFactor
		) AS o_OtherModifiedFactor,
		ScheduleModifiedFactor AS i_ScheduleModifiedFactor,
		-- *INF*: iif(isnull(i_ScheduleModifiedFactor) or i_ScheduleModifiedFactor=0.0,1.0,i_ScheduleModifiedFactor)
		IFF(i_ScheduleModifiedFactor IS NULL 
			OR i_ScheduleModifiedFactor = 0.0,
			1.0,
			i_ScheduleModifiedFactor
		) AS o_ScheduleModifiedFactor,
		ExperienceModifiedFactor AS i_ExperienceModifiedFactor,
		-- *INF*: iif(isnull(i_ExperienceModifiedFactor) or i_ExperienceModifiedFactor=0.0,1.0,i_ExperienceModifiedFactor)
		IFF(i_ExperienceModifiedFactor IS NULL 
			OR i_ExperienceModifiedFactor = 0.0,
			1.0,
			i_ExperienceModifiedFactor
		) AS o_ExperienceModifiedFactor,
		TransitionFactor AS i_TransitionFactor,
		-- *INF*: iif(isnull(i_TransitionFactor) or i_TransitionFactor=0.0,1.0,i_TransitionFactor)
		IFF(i_TransitionFactor IS NULL 
			OR i_TransitionFactor = 0.0,
			1.0,
			i_TransitionFactor
		) AS o_TransitionFactor
		FROM UN_all_InsuranceLines_new
	),
	Mplt_RatingModifer_Target AS (
		SELECT
		PremiumTransactionID, 
		PremiumTransactionAKID, 
		o_OtherModifiedFactor AS OtherModifiedFactor, 
		o_ScheduleModifiedFactor AS ScheduleModifiedFactor, 
		o_ExperienceModifiedFactor AS ExperienceModifiedFactor, 
		o_TransitionFactor AS TransitionFactor
		FROM EXP_Modifier_Null_Handle
	),
),
EXP_all_Insurancelines AS (
	SELECT
	PremiumTransactionID1 AS PremiumTransactionID,
	PremiumTransactionAKID1 AS PremiumTransactionAKID,
	OtherModifiedFactor,
	ScheduleModifiedFactor,
	ExperienceModifiedFactor,
	TransitionFactor
	FROM mplt_PremiumTransactionRatingModifier
),
LKP_PremiumTransactionRatingModifier AS (
	SELECT
	PremiumTransactionID
	FROM (
		SELECT PremiumTransactionRatingModifier.PremiumTransactionID as PremiumTransactionID FROM @{pipeline().parameters.SOURCE_TABLE_OWNER} .PremiumTransactionRatingModifier
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY PremiumTransactionID) = 1
),
Filter_insert_new_records AS (
	SELECT
	LKP_PremiumTransactionRatingModifier.PremiumTransactionID AS lkp_PremiumTransactionID, 
	EXP_all_Insurancelines.PremiumTransactionID, 
	EXP_all_Insurancelines.PremiumTransactionAKID, 
	EXP_all_Insurancelines.OtherModifiedFactor, 
	EXP_all_Insurancelines.ScheduleModifiedFactor, 
	EXP_all_Insurancelines.ExperienceModifiedFactor, 
	EXP_all_Insurancelines.TransitionFactor
	FROM EXP_all_Insurancelines
	LEFT JOIN LKP_PremiumTransactionRatingModifier
	ON LKP_PremiumTransactionRatingModifier.PremiumTransactionID = EXP_all_Insurancelines.PremiumTransactionID
	WHERE ISNULL(lkp_PremiumTransactionID)
),
EXP_Target_Columns AS (
	SELECT
	PremiumTransactionID,
	PremiumTransactionAKID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	OtherModifiedFactor,
	ScheduleModifiedFactor,
	ExperienceModifiedFactor,
	TransitionFactor
	FROM Filter_insert_new_records
),
PremiumTransactionRatingModifier_insert AS (
	INSERT INTO PremiumTransactionRatingModifier
	(PremiumTransactionID, PremiumTransactionAKID, AuditID, SourceSystemID, CreatedDate, ModifiedDate, OtherModifiedFactor, ScheduleModifiedFactor, ExperienceModifiedFactor, TransitionFactor)
	SELECT 
	PREMIUMTRANSACTIONID, 
	PREMIUMTRANSACTIONAKID, 
	o_AuditID AS AUDITID, 
	o_SourceSystemID AS SOURCESYSTEMID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	OTHERMODIFIEDFACTOR, 
	SCHEDULEMODIFIEDFACTOR, 
	EXPERIENCEMODIFIEDFACTOR, 
	TRANSITIONFACTOR
	FROM EXP_Target_Columns
),
SQ_PremiumTransactionRatingModifier_offset_records AS (
	SELECT DISTINCT WorkPremiumTransactionOffsetLineage.PremiumTransactionID
		,WorkPremiumTransactionOffsetLineage.PremiumTransactionAKID
		,PremiumTransactionRatingModifier.OtherModifiedFactor
		,PremiumTransactionRatingModifier.ScheduleModifiedFactor
		,PremiumTransactionRatingModifier.ExperienceModifiedFactor
		,PremiumTransactionRatingModifier.TransitionFactor
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransactionOffsetLineage
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransactionRatingModifier
	 ON PremiumTransactionRatingModifier.PremiumTransactionID = WorkPremiumTransactionOffsetLineage.PreviousPremiumTransactionID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction 
	ON WorkPremiumTransactionOffsetLineage.PremiumTransactionID = PremiumTransaction.PremiumTransactionID and PremiumTransaction.OffsetOnsetCode='Offset' 
		AND PremiumTransaction.ReasonAmendedCode NOT IN (
			'CWO'
			,'Claw Back'
			)
		AND PremiumTransaction.PremiumType = 'D'
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage ON RatingCoverage.RatingCoverageAKID = PremiumTransaction.RatingCoverageAKId
		AND RatingCoverage.EffectiveDate = PremiumTransaction.EffectiveDate
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product ON RatingCoverage.ProductAKId = Product.ProductAKId
		AND Product.CurrentSnapshotFlag = 1
		AND Product.ProductAbbreviation <> 'WC'
		where WorkPremiumTransactionOffsetLineage. UpdateAttributeFlag = 1
),
EXP_Extract_offset AS (
	SELECT
	PremiumTransactionID,
	PremiumTransactionAKID,
	OtherModifiedFactor,
	ScheduleModifiedFactor,
	ExperienceModifiedFactor,
	TransitionFactor,
	-- *INF*: IIF(ISNULL(OtherModifiedFactor),1.0,OtherModifiedFactor)
	IFF(OtherModifiedFactor IS NULL,
		1.0,
		OtherModifiedFactor
	) AS o_OtherModifiedFactor,
	-- *INF*: IIF(ISNULL(ScheduleModifiedFactor),1.0,ScheduleModifiedFactor)
	IFF(ScheduleModifiedFactor IS NULL,
		1.0,
		ScheduleModifiedFactor
	) AS o_ScheduleModifiedFactor,
	-- *INF*: IIF(ISNULL(ExperienceModifiedFactor),1.0,ExperienceModifiedFactor)
	IFF(ExperienceModifiedFactor IS NULL,
		1.0,
		ExperienceModifiedFactor
	) AS o_ExperienceModifiedFactor,
	-- *INF*: IIF(ISNULL(TransitionFactor),1.0,TransitionFactor)
	IFF(TransitionFactor IS NULL,
		1.0,
		TransitionFactor
	) AS o_TransitionFactor
	FROM SQ_PremiumTransactionRatingModifier_offset_records
),
LKP_PremiumtransactionRatingmodifier_offset AS (
	SELECT
	PremiumTransactionID
	FROM (
		SELECT PremiumTransactionRatingModifier.PremiumTransactionID as PremiumTransactionID FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransactionRatingModifier
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY PremiumTransactionID) = 1
),
Filter_offset_records AS (
	SELECT
	LKP_PremiumtransactionRatingmodifier_offset.PremiumTransactionID AS lkp_PremiumTransactionID, 
	EXP_Extract_offset.PremiumTransactionID, 
	EXP_Extract_offset.PremiumTransactionAKID, 
	EXP_Extract_offset.o_OtherModifiedFactor AS OtherModifiedFactor, 
	EXP_Extract_offset.o_ScheduleModifiedFactor AS ScheduleModifiedFactor, 
	EXP_Extract_offset.o_ExperienceModifiedFactor AS ExperienceModifiedFactor, 
	EXP_Extract_offset.o_TransitionFactor AS TransitionFactor
	FROM EXP_Extract_offset
	LEFT JOIN LKP_PremiumtransactionRatingmodifier_offset
	ON LKP_PremiumtransactionRatingmodifier_offset.PremiumTransactionID = EXP_Extract_offset.PremiumTransactionID
	WHERE ISNULL(lkp_PremiumTransactionID)
),
EXP_Target_columns_offset AS (
	SELECT
	PremiumTransactionID,
	PremiumTransactionAKID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	OtherModifiedFactor,
	ScheduleModifiedFactor,
	ExperienceModifiedFactor,
	TransitionFactor
	FROM Filter_offset_records
),
PremiumTransactionRatingModifier_offset_insert AS (
	INSERT INTO PremiumTransactionRatingModifier
	(PremiumTransactionID, PremiumTransactionAKID, AuditID, SourceSystemID, CreatedDate, ModifiedDate, OtherModifiedFactor, ScheduleModifiedFactor, ExperienceModifiedFactor, TransitionFactor)
	SELECT 
	PREMIUMTRANSACTIONID, 
	PREMIUMTRANSACTIONAKID, 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	OTHERMODIFIEDFACTOR, 
	SCHEDULEMODIFIEDFACTOR, 
	EXPERIENCEMODIFIEDFACTOR, 
	TRANSITIONFACTOR
	FROM EXP_Target_columns_offset
),
SQ_PremiumTransactionRatingModifier_Deprecated_records AS (
	SELECT DISTINCT WorkPremiumTransactionOffsetLineage.PremiumTransactionID
		,WorkPremiumTransactionOffsetLineage.PremiumTransactionAKID
		,PremiumTransactionRatingModifier.OtherModifiedFactor
		,PremiumTransactionRatingModifier.ScheduleModifiedFactor
		,PremiumTransactionRatingModifier.ExperienceModifiedFactor
		,PremiumTransactionRatingModifier.TransitionFactor
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransactionOffsetLineage
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransactionRatingModifier
	 ON PremiumTransactionRatingModifier.PremiumTransactionID = WorkPremiumTransactionOffsetLineage.PreviousPremiumTransactionID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction 
	ON WorkPremiumTransactionOffsetLineage.PremiumTransactionID = PremiumTransaction.PremiumTransactionID and PremiumTransaction.OffsetOnsetCode='Deprecated' 
		AND PremiumTransaction.ReasonAmendedCode NOT IN (
			'CWO'
			,'Claw Back'
			)
		AND PremiumTransaction.PremiumType = 'D'
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage ON RatingCoverage.RatingCoverageAKID = PremiumTransaction.RatingCoverageAKId
		AND RatingCoverage.EffectiveDate = PremiumTransaction.EffectiveDate
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product ON RatingCoverage.ProductAKId = Product.ProductAKId
		AND Product.CurrentSnapshotFlag = 1
		AND Product.ProductAbbreviation <> 'WC'
		where WorkPremiumTransactionOffsetLineage. UpdateAttributeFlag = 1
),
EXP_Extract_Deprecated AS (
	SELECT
	PremiumTransactionID,
	PremiumTransactionAKID,
	OtherModifiedFactor,
	ScheduleModifiedFactor,
	ExperienceModifiedFactor,
	TransitionFactor,
	-- *INF*: IIF(ISNULL(OtherModifiedFactor),1.0,OtherModifiedFactor)
	IFF(OtherModifiedFactor IS NULL,
		1.0,
		OtherModifiedFactor
	) AS o_OtherModifiedFactor,
	-- *INF*: IIF(ISNULL(ScheduleModifiedFactor),1.0,ScheduleModifiedFactor)
	IFF(ScheduleModifiedFactor IS NULL,
		1.0,
		ScheduleModifiedFactor
	) AS o_ScheduleModifiedFactor,
	-- *INF*: IIF(ISNULL(ExperienceModifiedFactor),1.0,ExperienceModifiedFactor)
	IFF(ExperienceModifiedFactor IS NULL,
		1.0,
		ExperienceModifiedFactor
	) AS o_ExperienceModifiedFactor,
	-- *INF*: IIF(ISNULL(TransitionFactor),1.0,TransitionFactor)
	IFF(TransitionFactor IS NULL,
		1.0,
		TransitionFactor
	) AS o_TransitionFactor
	FROM SQ_PremiumTransactionRatingModifier_Deprecated_records
),
LKP_PremiumtransactionRatingmodifier_Deprecated AS (
	SELECT
	PremiumTransactionID
	FROM (
		SELECT PremiumTransactionRatingModifier.PremiumTransactionID as PremiumTransactionID FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransactionRatingModifier
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY PremiumTransactionID) = 1
),
Filter_offset_Deprecated AS (
	SELECT
	LKP_PremiumtransactionRatingmodifier_Deprecated.PremiumTransactionID AS lkp_PremiumTransactionID, 
	EXP_Extract_Deprecated.PremiumTransactionID, 
	EXP_Extract_Deprecated.PremiumTransactionAKID, 
	EXP_Extract_Deprecated.o_OtherModifiedFactor AS OtherModifiedFactor, 
	EXP_Extract_Deprecated.o_ScheduleModifiedFactor AS ScheduleModifiedFactor, 
	EXP_Extract_Deprecated.o_ExperienceModifiedFactor AS ExperienceModifiedFactor, 
	EXP_Extract_Deprecated.o_TransitionFactor AS TransitionFactor
	FROM EXP_Extract_Deprecated
	LEFT JOIN LKP_PremiumtransactionRatingmodifier_Deprecated
	ON LKP_PremiumtransactionRatingmodifier_Deprecated.PremiumTransactionID = EXP_Extract_Deprecated.PremiumTransactionID
	WHERE ISNULL(lkp_PremiumTransactionID)
),
EXP_Target_columns_Deprecated AS (
	SELECT
	PremiumTransactionID,
	PremiumTransactionAKID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	OtherModifiedFactor,
	ScheduleModifiedFactor,
	ExperienceModifiedFactor,
	TransitionFactor
	FROM Filter_offset_Deprecated
),
PremiumTransactionRatingModifier_Deprecated_insert AS (
	INSERT INTO PremiumTransactionRatingModifier
	(PremiumTransactionID, PremiumTransactionAKID, AuditID, SourceSystemID, CreatedDate, ModifiedDate, OtherModifiedFactor, ScheduleModifiedFactor, ExperienceModifiedFactor, TransitionFactor)
	SELECT 
	PREMIUMTRANSACTIONID, 
	PREMIUMTRANSACTIONAKID, 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	OTHERMODIFIEDFACTOR, 
	SCHEDULEMODIFIEDFACTOR, 
	EXPERIENCEMODIFIEDFACTOR, 
	TRANSITIONFACTOR
	FROM EXP_Target_columns_Deprecated
),
SQ_PremiumTransactionRatingModifier_backfill AS (
	SELECT DISTINCT PT.PremiumTransactionID AS PremiumTransactionRatingModifierId
		,PT.PremiumTransactionID
		,WPT.PremiumTransactionAKID
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransaction WPT
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT ON WPT.PremiumTransactionAKId = PT.PremiumTransactionAKId
	 and PT.ReasonAmendedCode not in ('CWO','Claw Back') and PT.PremiumType='D'
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC ON PT.RatingCoverageAKId = RC.RatingCoverageAKID
		AND PT.EffectiveDate = RC.EffectiveDate
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product PRD ON RC.ProductAKId = PRD.ProductAKId
		AND PRD.CurrentSnapshotFlag = 1
		AND PRD.ProductAbbreviation <> 'WC'
	WHERE NOT EXISTS (
			SELECT 1
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransactionRatingModifier PTRM WITH (NOLOCK)
			WHERE WPT.PremiumTransactionAKId = PTRM.PremiumTransactionAKID
			)
	and WPT.SourceSystemID = 'DCT'
),
EXP_Extract_Othetransactions AS (
	SELECT
	PremiumTransactionRatingModifierId,
	PremiumTransactionID,
	PremiumTransactionAKID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	1.0 AS OtherModifiedFactor,
	1.0 AS ScheduleModifiedFactor,
	1.0 AS ExperienceModifiedFactor,
	1.0 AS TransitionFactor
	FROM SQ_PremiumTransactionRatingModifier_backfill
),
PremiumTransactionRatingModifier_insert_othertrnsactions AS (
	INSERT INTO PremiumTransactionRatingModifier
	(PremiumTransactionID, PremiumTransactionAKID, AuditID, SourceSystemID, CreatedDate, ModifiedDate, OtherModifiedFactor, ScheduleModifiedFactor, ExperienceModifiedFactor, TransitionFactor)
	SELECT 
	PREMIUMTRANSACTIONID, 
	PREMIUMTRANSACTIONAKID, 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	OTHERMODIFIEDFACTOR, 
	SCHEDULEMODIFIEDFACTOR, 
	EXPERIENCEMODIFIEDFACTOR, 
	TRANSITIONFACTOR
	FROM EXP_Extract_Othetransactions
),
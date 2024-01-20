WITH
SQ_ISSCommercialPropertyExtract_Loss_Unique AS (
	SELECT ISCP.ISSCommercialPropertyExtractId, ISCP.AuditId, 
	ISCP.CreatedDate, ISCP.EDWPremiumMasterCalculationPKId, 
	ISCP.EDWLossMasterCalculationPKId, ISCP.TypeBureauCode, 
	ISCP.BureauLineOfInsurance, ISCP.BureauCompanyNumber,
	 ISCP.StateProvinceCode, ISCP.PremiumMasterRunDate, 
	 ISCP.LossMasterRunDate, ISCP.PolicyKey,
	  ISCP.PremiumMasterClassCode, ISCP.LossMasterClassCode, 
	  ISCP.ClaimNumber, ISCP.ClaimantNumber, 
	  ISCP.RiskTerritoryCode, ISCP.PolicyEffectiveDate, 
	  ISCP.CauseOfLoss, ISCP.DeductibleAmount, 
	  ISCP.CoverageCode, ISCP.ConstructionCode, 
	  ISCP.ISOFireProtectionCode, ISCP.SublineCode, 
	  ISCP.PackageModificationAdjustmentGroupDescription, ISCP.PolicyForm,
	   ISCP.PremiumMasterDirectWrittenPremiumAmount, ISCP.PaidLossAmount,
	    ISCP.OutstandingLossAmount, ISCP.PolicyExpirationDate, 
		ISCP.InceptionToDatePaidLossAmount, ISCP.ClaimantCoverageDetailId, 
		ISCP.AnnualStatementLineNumber, ISCP.TransactionEffectiveDate, 
		ISCP.LocationNumber, ISCP.BuildingNumber 
	FROM
	 ISSCommercialPropertyExtract ISCP
	 where ISCP.EDWLossMasterCalculationPKId<>-1 and 
	ISCP.LossMasterRunDate
	between 
	 DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0)--first day of last Quarter   
	 AND
	 DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0)) -- Last day of last Quarter
),
EXP_Loss_Unique AS (
	SELECT
	ISSCommercialPropertyExtractId,
	AuditId,
	CreatedDate,
	EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId,
	TypeBureauCode,
	BureauLineOfInsurance,
	BureauCompanyNumber,
	StateProvinceCode,
	PremiumMasterRunDate,
	LossMasterRunDate,
	PolicyKey,
	PremiumMasterClassCode,
	LossMasterClassCode,
	ClaimNumber,
	ClaimantNumber,
	RiskTerritoryCode,
	PolicyEffectiveDate,
	CauseOfLoss,
	DeductibleAmount,
	CoverageCode,
	ConstructionCode,
	ISOFireProtectionCode,
	SublineCode,
	PackageModificationAdjustmentGroupDescription,
	PolicyForm,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	PolicyExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId,
	AnnualStatementLineNumber,
	TransactionEffectiveDate,
	LocationNumber,
	BuildingNumber
	FROM SQ_ISSCommercialPropertyExtract_Loss_Unique
),
SQ_ISSCommercialPropertyExtract_PremiumDup AS (
	WITH ROLLUP_TABLE_TEMP
	AS
	(SELECT
			CONCAT(TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode,
			LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, DeductibleAmount, CoverageCode, ConstructionCode, ISOFireProtectionCode,
			SublineCode, PackageModificationAdjustmentGroupDescription, PolicyForm, ABS(PaidLossAmount), ABS(OutstandingLossAmount),
			PolicyExpirationDate,
			ABS(InceptionToDatePaidLossAmount), ClaimantCoverageDetailId, AnnualStatementLineNumber, TransactionEffectiveDate, LocationNumber,
			BuildingNumber) AS CONCAT_KEY
			,SUM(PremiumMasterDirectWrittenPremiumAmount) ROLL_UP_DWP_AMT
			,MAX(ISSCommercialPropertyExtractId) MAX_ISS_KEY
			,COUNT(*) AS count
		FROM ISSCommercialPropertyExtract
		WHERE EDWPremiumMasterCalculationPKId <> -1
		AND (ISSCommercialPropertyExtract.PremiumMasterRunDate
		BETWEEN
		DATEADD(qq, DATEDIFF(qq, 0, GETDATE()) + @{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER}, 0)--first day of last Quarter   
		AND
		DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE()) + @{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0)) -- Last day of last Quarter
		)
		GROUP BY CONCAT(TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode,
		LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, DeductibleAmount, CoverageCode, ConstructionCode, ISOFireProtectionCode,
		SublineCode, PackageModificationAdjustmentGroupDescription, PolicyForm, ABS(PaidLossAmount), ABS(OutstandingLossAmount),
		PolicyExpirationDate,
		ABS(InceptionToDatePaidLossAmount), ClaimantCoverageDetailId, AnnualStatementLineNumber, TransactionEffectiveDate, LocationNumber,
		BuildingNumber)
		HAVING COUNT(*) > 1)
	
	SELECT
		ISCP.ISSCommercialPropertyExtractId
		,ISCP.AuditId
		,ISCP.CreatedDate
		,ISCP.EDWPremiumMasterCalculationPKId
		,ISCP.EDWLossMasterCalculationPKId
		,ISCP.TypeBureauCode
		,ISCP.BureauLineOfInsurance
		,ISCP.BureauCompanyNumber
		,ISCP.StateProvinceCode
		,ISCP.PremiumMasterRunDate
		,ISCP.LossMasterRunDate
		,ISCP.PolicyKey
		,ISCP.PremiumMasterClassCode
		,ISCP.LossMasterClassCode
		,ISCP.ClaimNumber
		,ISCP.ClaimantNumber
		,ISCP.RiskTerritoryCode
		,ISCP.PolicyEffectiveDate
		,ISCP.CauseOfLoss
		,ISCP.DeductibleAmount
		,ISCP.CoverageCode
		,ISCP.ConstructionCode
		,ISCP.ISOFireProtectionCode
		,ISCP.SublineCode
		,ISCP.PackageModificationAdjustmentGroupDescription
		,ISCP.PolicyForm
		,ROLLUP_TABLE_TEMP.ROLL_UP_DWP_AMT AS PremiumMasterDirectWrittenPremiumAmount
		,ISCP.PaidLossAmount
		,ISCP.OutstandingLossAmount
		,ISCP.PolicyExpirationDate
		,ISCP.InceptionToDatePaidLossAmount
		,ISCP.ClaimantCoverageDetailId
		,ISCP.AnnualStatementLineNumber
		,ISCP.TransactionEffectiveDate
		,ISCP.LocationNumber
		,ISCP.BuildingNumber
	FROM ROLLUP_TABLE_TEMP
	INNER JOIN ISSCommercialPropertyExtract ISCP
		ON ISCP.ISSCommercialPropertyExtractId = ROLLUP_TABLE_TEMP.MAX_ISS_KEY
			AND ISCP.EDWPremiumMasterCalculationPKId <> -1
			AND (ISCP.PremiumMasterRunDate
			BETWEEN
			DATEADD(qq, DATEDIFF(qq, 0, GETDATE()) + @{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER}, 0)--first day of last Quarter   
			AND
			DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE()) + @{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0)) -- Last day of last Quarter
			)
	WHERE ROLLUP_TABLE_TEMP.ROLL_UP_DWP_AMT <> 0
),
EXP_Premium_Dup AS (
	SELECT
	ISSCommercialPropertyExtractId,
	AuditId,
	CreatedDate,
	EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId,
	TypeBureauCode,
	BureauLineOfInsurance,
	BureauCompanyNumber,
	StateProvinceCode,
	PremiumMasterRunDate,
	LossMasterRunDate,
	PolicyKey,
	PremiumMasterClassCode,
	LossMasterClassCode,
	ClaimNumber,
	ClaimantNumber,
	RiskTerritoryCode,
	PolicyEffectiveDate,
	CauseOfLoss,
	DeductibleAmount,
	CoverageCode,
	ConstructionCode,
	ISOFireProtectionCode,
	SublineCode,
	PackageModificationAdjustmentGroupDescription,
	PolicyForm,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	PolicyExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId,
	AnnualStatementLineNumber,
	TransactionEffectiveDate,
	LocationNumber,
	BuildingNumber
	FROM SQ_ISSCommercialPropertyExtract_PremiumDup
),
SQ_ISSCommercialPropertyExtract_Premium_unique AS (
	with Premium_unique as
	(
	select concat(TypeBureauCode,BureauLineOfInsurance,BureauCompanyNumber,StateProvinceCode,PremiumMasterRunDate,LossMasterRunDate,PolicyKey,PremiumMasterClassCode,
	LossMasterClassCode,ClaimNumber,ClaimantNumber,RiskTerritoryCode,PolicyEffectiveDate,CauseOfLoss,DeductibleAmount,CoverageCode,ConstructionCode,ISOFireProtectionCode,
	SublineCode,PackageModificationAdjustmentGroupDescription,PolicyForm,abs(PaidLossAmount),abs(OutstandingLossAmount),
	PolicyExpirationDate,
	abs(InceptionToDatePaidLossAmount),ClaimantCoverageDetailId,AnnualStatementLineNumber,TransactionEffectiveDate,LocationNumber,
	BuildingNumber) as concat,
	count(*) as count
	from
	ISSCommercialPropertyExtract where EDWPremiumMasterCalculationPKId<>-1 and ( ISSCommercialPropertyExtract.PremiumMasterRunDate
	 between 
	 DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0)--first day of last Quarter   
	 AND
	 DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0)) -- Last day of last Quarter
	) OR
	(ISSCommercialPropertyExtract.LossMasterRunDate
	between 
	 DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0)--first day of last Quarter   
	 AND
	 DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0)) -- Last day of last Quarter
	)  group by
	concat(TypeBureauCode,BureauLineOfInsurance,BureauCompanyNumber,StateProvinceCode,PremiumMasterRunDate,LossMasterRunDate,PolicyKey,PremiumMasterClassCode,
	LossMasterClassCode,ClaimNumber,ClaimantNumber,RiskTerritoryCode,PolicyEffectiveDate,CauseOfLoss,DeductibleAmount,CoverageCode,ConstructionCode,ISOFireProtectionCode,
	SublineCode,PackageModificationAdjustmentGroupDescription,PolicyForm,
	abs(PaidLossAmount),abs(OutstandingLossAmount),
	PolicyExpirationDate,
	abs(InceptionToDatePaidLossAmount),ClaimantCoverageDetailId,AnnualStatementLineNumber,TransactionEffectiveDate,
	LocationNumber,BuildingNumber) 
	 having count(*)=1  
	 )
	 SELECT ISCP.ISSCommercialPropertyExtractId, ISCP.AuditId,
	 ISCP.CreatedDate, ISCP.EDWPremiumMasterCalculationPKId,
	  ISCP.EDWLossMasterCalculationPKId, ISCP.TypeBureauCode, 
	  ISCP.BureauLineOfInsurance, ISCP.BureauCompanyNumber, 
	  ISCP.StateProvinceCode, ISCP.PremiumMasterRunDate,
	   ISCP.LossMasterRunDate, ISCP.PolicyKey, 
	   ISCP.PremiumMasterClassCode, ISCP.LossMasterClassCode,
	    ISCP.ClaimNumber, ISCP.ClaimantNumber, 
		ISCP.RiskTerritoryCode, ISCP.PolicyEffectiveDate, 
		ISCP.CauseOfLoss, ISCP.DeductibleAmount, 
		ISCP.CoverageCode, ISCP.ConstructionCode,
		 ISCP.ISOFireProtectionCode, ISCP.SublineCode, 
		 ISCP.PackageModificationAdjustmentGroupDescription, 
		 ISCP.PolicyForm, ISCP.PremiumMasterDirectWrittenPremiumAmount, 
		 ISCP.PaidLossAmount, ISCP.OutstandingLossAmount,
		  ISCP.PolicyExpirationDate, ISCP.InceptionToDatePaidLossAmount,
		   ISCP.ClaimantCoverageDetailId, ISCP.AnnualStatementLineNumber,
		    ISCP.TransactionEffectiveDate, ISCP.LocationNumber, 
			ISCP.BuildingNumber 
	FROM
	 ISSCommercialPropertyExtract  ISCP inner join Premium_unique b on
	 concat(TypeBureauCode,BureauLineOfInsurance,BureauCompanyNumber,StateProvinceCode,PremiumMasterRunDate,LossMasterRunDate,PolicyKey,PremiumMasterClassCode,
	LossMasterClassCode,ClaimNumber,ClaimantNumber,RiskTerritoryCode,PolicyEffectiveDate,CauseOfLoss,DeductibleAmount,CoverageCode,ConstructionCode,ISOFireProtectionCode,
	SublineCode,PackageModificationAdjustmentGroupDescription,PolicyForm,abs(PaidLossAmount),abs(OutstandingLossAmount),
	PolicyExpirationDate,
	abs(InceptionToDatePaidLossAmount),ClaimantCoverageDetailId,AnnualStatementLineNumber,TransactionEffectiveDate,LocationNumber,
	BuildingNumber) =b.concat 
	
	WHERE
	( ISCP.PremiumMasterRunDate
	 between 
	 DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0)--first day of last Quarter   
	 AND
	 DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0)) -- Last day of last Quarter
	)
	and ISCP.EDWPremiumMasterCalculationPKId<>-1
),
EXP_Premium_unique AS (
	SELECT
	ISSCommercialPropertyExtractId,
	AuditId,
	CreatedDate,
	EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId,
	TypeBureauCode,
	BureauLineOfInsurance,
	BureauCompanyNumber,
	StateProvinceCode,
	PremiumMasterRunDate,
	LossMasterRunDate,
	PolicyKey,
	PremiumMasterClassCode,
	LossMasterClassCode,
	ClaimNumber,
	ClaimantNumber,
	RiskTerritoryCode,
	PolicyEffectiveDate,
	CauseOfLoss,
	DeductibleAmount,
	CoverageCode,
	ConstructionCode,
	ISOFireProtectionCode,
	SublineCode,
	PackageModificationAdjustmentGroupDescription,
	PolicyForm,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	PolicyExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId,
	AnnualStatementLineNumber,
	TransactionEffectiveDate,
	LocationNumber,
	BuildingNumber
	FROM SQ_ISSCommercialPropertyExtract_Premium_unique
),
Unoin_All AS (
	SELECT ISSCommercialPropertyExtractId, AuditId AS Auditid, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, DeductibleAmount, CoverageCode, ConstructionCode, ISOFireProtectionCode, SublineCode, PackageModificationAdjustmentGroupDescription, PolicyForm, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, AnnualStatementLineNumber, TransactionEffectiveDate, LocationNumber, BuildingNumber
	FROM EXP_Premium_unique
	UNION
	SELECT ISSCommercialPropertyExtractId, AuditId AS Auditid, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, DeductibleAmount, CoverageCode, ConstructionCode, ISOFireProtectionCode, SublineCode, PackageModificationAdjustmentGroupDescription, PolicyForm, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, AnnualStatementLineNumber, TransactionEffectiveDate, LocationNumber, BuildingNumber
	FROM EXP_Loss_Unique
	UNION
	SELECT ISSCommercialPropertyExtractId, AuditId AS Auditid, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, DeductibleAmount, CoverageCode, ConstructionCode, ISOFireProtectionCode, SublineCode, PackageModificationAdjustmentGroupDescription, PolicyForm, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, AnnualStatementLineNumber, TransactionEffectiveDate, LocationNumber, BuildingNumber
	FROM EXP_Premium_Dup
),
EXP_get_all_data AS (
	SELECT
	ISSCommercialPropertyExtractId,
	Auditid AS i_AuditId,
	CreatedDate AS i_CreatedDate,
	EDWPremiumMasterCalculationPKId AS i_EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId AS i_EDWLossMasterCalculationPKId,
	TypeBureauCode AS i_TypeBureauCode,
	BureauLineOfInsurance AS i_BureauLineOfInsurance,
	BureauCompanyNumber AS i_BureauCompanyNumber,
	StateProvinceCode AS i_StateProvinceCode,
	PremiumMasterRunDate AS i_PremiumMasterRunDate,
	LossMasterRunDate AS i_LossMasterRunDate,
	PolicyKey AS i_PolicyKey,
	PremiumMasterClassCode AS i_PremiumMasterClassCode,
	LossMasterClassCode AS i_LossMasterClassCode,
	ClaimNumber AS i_ClaimNumber,
	ClaimantNumber AS i_ClaimantNumber,
	RiskTerritoryCode AS i_RiskTerritoryCode,
	PolicyEffectiveDate AS i_PolicyEffectiveDate,
	CauseOfLoss AS i_CauseOfLoss,
	DeductibleAmount AS i_DeductibleAmount,
	CoverageCode AS i_CoverageCode,
	ConstructionCode AS i_ConstructionCode,
	ISOFireProtectionCode AS i_ISOFireProtectionCode,
	SublineCode AS i_SublineCode,
	PackageModificationAdjustmentGroupDescription AS i_PackageModificationAdjustmentGroupDescription,
	PolicyForm AS i_PolicyForm,
	PremiumMasterDirectWrittenPremiumAmount AS i_PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount AS i_PaidLossAmount,
	OutstandingLossAmount AS i_OutstandingLossAmount,
	PolicyExpirationDate AS i_PolicyExpirationDate,
	InceptionToDatePaidLossAmount AS i_InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId AS i_ClaimantCoverageDetailId,
	AnnualStatementLineNumber AS i_AnnualStatementLineNumber,
	TransactionEffectiveDate AS i_TransactionEffectiveDate,
	LocationNumber AS i_LocationNumber,
	BuildingNumber AS i_BuildingNumber,
	ISSCommercialPropertyExtractId AS o_WorkISSExtractId,
	i_AuditId AS o_AuditId,
	i_CreatedDate AS o_CreatedDate,
	i_EDWPremiumMasterCalculationPKId AS o_EDWPremiumMasterCalculationPKId,
	i_EDWLossMasterCalculationPKId AS o_EDWLossMasterCalculationPKId,
	-- *INF*: LTRIM(RTRIM(i_TypeBureauCode))
	LTRIM(RTRIM(i_TypeBureauCode)) AS o_TypeBureauCode,
	-- *INF*: LTRIM(RTRIM(i_BureauLineOfInsurance))
	LTRIM(RTRIM(i_BureauLineOfInsurance)) AS o_BureauLineOfInsurance,
	-- *INF*: LTRIM(RTRIM(i_BureauCompanyNumber))
	LTRIM(RTRIM(i_BureauCompanyNumber)) AS o_BureauCompanyNumber,
	-- *INF*: LTRIM(RTRIM(i_StateProvinceCode))
	LTRIM(RTRIM(i_StateProvinceCode)) AS o_StateProvinceCode,
	i_PremiumMasterRunDate AS o_PremiumMasterRunDate,
	i_LossMasterRunDate AS o_LossMasterRunDate,
	-- *INF*: LTRIM(RTRIM(i_PolicyKey))
	LTRIM(RTRIM(i_PolicyKey)) AS o_PolicyKey,
	-- *INF*: LTRIM(RTRIM(i_PremiumMasterClassCode))
	LTRIM(RTRIM(i_PremiumMasterClassCode)) AS o_PremiumMasterClassCode,
	-- *INF*: LTRIM(RTRIM(i_LossMasterClassCode))
	LTRIM(RTRIM(i_LossMasterClassCode)) AS o_LossMasterClassCode,
	-- *INF*: LTRIM(RTRIM(i_ClaimNumber))
	LTRIM(RTRIM(i_ClaimNumber)) AS o_ClaimNumber,
	-- *INF*: LTRIM(RTRIM(i_ClaimantNumber))
	LTRIM(RTRIM(i_ClaimantNumber)) AS o_ClaimantNumber,
	-- *INF*: LTRIM(RTRIM(i_RiskTerritoryCode))
	LTRIM(RTRIM(i_RiskTerritoryCode)) AS o_RiskTerritoryCode,
	i_PolicyEffectiveDate AS o_PolicyEffectiveDate,
	-- *INF*: LTRIM(RTRIM(i_CauseOfLoss))
	LTRIM(RTRIM(i_CauseOfLoss)) AS o_CauseOfLoss,
	-- *INF*: LTRIM(RTRIM(i_DeductibleAmount))
	LTRIM(RTRIM(i_DeductibleAmount)) AS o_DeductibleAmount,
	-- *INF*: LTRIM(RTRIM(i_CoverageCode))
	LTRIM(RTRIM(i_CoverageCode)) AS o_CoverageCode,
	-- *INF*: LTRIM(RTRIM(i_ConstructionCode))
	LTRIM(RTRIM(i_ConstructionCode)) AS o_ConstructionCode,
	-- *INF*: LTRIM(RTRIM(i_ISOFireProtectionCode))
	LTRIM(RTRIM(i_ISOFireProtectionCode)) AS o_ISOFireProtectionCode,
	-- *INF*: LTRIM(RTRIM(i_SublineCode))
	LTRIM(RTRIM(i_SublineCode)) AS o_SublineCode,
	-- *INF*: LTRIM(RTRIM(i_PackageModificationAdjustmentGroupDescription))
	LTRIM(RTRIM(i_PackageModificationAdjustmentGroupDescription)) AS o_PackageModificationAdjustmentGroupDescription,
	-- *INF*: LTRIM(RTRIM(i_PolicyForm))
	LTRIM(RTRIM(i_PolicyForm)) AS o_PolicyForm,
	i_PremiumMasterDirectWrittenPremiumAmount AS o_PremiumMasterDirectWrittenPremiumAmount,
	i_PaidLossAmount AS o_PaidLossAmount,
	i_OutstandingLossAmount AS o_OutstandingLossAmount,
	i_PolicyExpirationDate AS o_PolicyExpirationDate,
	i_InceptionToDatePaidLossAmount AS o_InceptionToDatePaidLossAmount,
	i_ClaimantCoverageDetailId AS o_ClaimantCoverageDetailID,
	i_AnnualStatementLineNumber AS o_AnnualStatementLineNumber,
	i_TransactionEffectiveDate AS o_TransactionEffectiveDate,
	i_LocationNumber AS o_LocationNumber,
	i_BuildingNumber AS o_BuildingNumber
	FROM Unoin_All
),
RTR_CR_CF AS (
	SELECT
	o_WorkISSExtractId AS WorkISSExtractId,
	o_AuditId AS AuditId,
	o_CreatedDate AS CreatedDate,
	o_EDWPremiumMasterCalculationPKId AS EDWPremiumMasterCalculationPKId,
	o_EDWLossMasterCalculationPKId AS EDWLossMasterCalculationPKId,
	o_TypeBureauCode AS TypeBureauCode,
	o_BureauLineOfInsurance AS BureauLineOfInsurance,
	o_BureauCompanyNumber AS BureauCompanyNumber,
	o_StateProvinceCode AS StateProvinceCode,
	o_PremiumMasterRunDate AS PremiumMasterRunDate,
	o_LossMasterRunDate AS LossMasterRunDate,
	o_PolicyKey AS PolicyKey,
	o_PremiumMasterClassCode AS PremiumMasterClassCode,
	o_LossMasterClassCode AS LossMasterClassCode,
	o_ClaimNumber AS ClaimNumber,
	o_ClaimantNumber AS ClaimantNumber,
	o_RiskTerritoryCode AS RiskTerritoryCode,
	o_PolicyEffectiveDate AS PolicyEffectiveDate,
	o_CauseOfLoss AS CauseOfLoss,
	o_DeductibleAmount AS DeductibleAmount,
	o_CoverageCode AS CoverageCode,
	o_ConstructionCode AS ConstructionCode,
	o_ISOFireProtectionCode AS ISOFireProtectionCode,
	o_SublineCode AS SublineCode,
	o_PackageModificationAdjustmentGroupDescription AS PackageModificationAdjustmentGroupDescription,
	o_PolicyForm AS PolicyForm,
	o_PremiumMasterDirectWrittenPremiumAmount AS PremiumMasterDirectWrittenPremiumAmount,
	o_PaidLossAmount AS PaidLossAmount,
	o_OutstandingLossAmount AS OutstandingLossAmount,
	o_PolicyExpirationDate AS PolicyExpirationDate,
	o_InceptionToDatePaidLossAmount AS InceptionToDatePaidLossAmount,
	o_ClaimantCoverageDetailID AS ClaimantCoverageDetailID,
	o_AnnualStatementLineNumber AS AnnualStatementLineNumber,
	o_TransactionEffectiveDate AS TransactionEffectiveDate,
	o_LocationNumber AS LocationNumber,
	o_BuildingNumber AS BuildingNumber
	FROM EXP_get_all_data
),
RTR_CR_CF_CommercialProperty AS (SELECT * FROM RTR_CR_CF WHERE IN(LTRIM(RTRIM(TypeBureauCode)),'CF','Property','SBOPProperty') AND NOT IN(SublineCode,'940','960','965')),
RTR_CR_CF_DEFAULT1 AS (SELECT * FROM RTR_CR_CF WHERE NOT ( (IN(LTRIM(RTRIM(TypeBureauCode)),'CF','Property','SBOPProperty') AND NOT IN(SublineCode,'940','960','965')) )),
EXP_TargetCommercialProperty AS (
	SELECT
	-- *INF*: TRUNC(sysdate,'DD')
	CAST(TRUNC(CURRENT_TIMESTAMP, 'DAY') AS TIMESTAMP_NTZ(0)) AS v_RunDate,
	-- *INF*: 'ISS_CommercialProperty_'||TO_CHAR(v_RunDate,'YYYYMMDD')||'.CSV'
	'ISS_CommercialProperty_' || TO_CHAR(v_RunDate, 'YYYYMMDD') || '.CSV' AS FileName,
	WorkISSExtractId,
	AuditId,
	CreatedDate,
	EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId,
	TypeBureauCode,
	BureauLineOfInsurance,
	BureauCompanyNumber,
	StateProvinceCode,
	PremiumMasterRunDate,
	LossMasterRunDate,
	PolicyKey,
	PremiumMasterClassCode,
	LossMasterClassCode,
	ClaimNumber,
	ClaimantNumber,
	RiskTerritoryCode,
	PolicyEffectiveDate,
	CauseOfLoss,
	DeductibleAmount,
	CoverageCode,
	ConstructionCode,
	ISOFireProtectionCode,
	SublineCode,
	PackageModificationAdjustmentGroupDescription,
	PolicyForm,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	PolicyExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailID,
	AnnualStatementLineNumber,
	TransactionEffectiveDate,
	LocationNumber,
	BuildingNumber
	FROM RTR_CR_CF_CommercialProperty
),
SRT_ISS_FlatFile_CF AS (
	SELECT
	FileName, 
	WorkISSExtractId, 
	AuditId, 
	CreatedDate, 
	EDWPremiumMasterCalculationPKId, 
	EDWLossMasterCalculationPKId, 
	TypeBureauCode, 
	BureauLineOfInsurance, 
	BureauCompanyNumber, 
	StateProvinceCode, 
	PremiumMasterRunDate, 
	LossMasterRunDate, 
	PolicyKey, 
	PremiumMasterClassCode, 
	LossMasterClassCode, 
	ClaimNumber, 
	ClaimantNumber, 
	RiskTerritoryCode, 
	PolicyEffectiveDate, 
	CauseOfLoss, 
	DeductibleAmount, 
	CoverageCode, 
	ConstructionCode, 
	ISOFireProtectionCode, 
	SublineCode, 
	PackageModificationAdjustmentGroupDescription, 
	PolicyForm, 
	PremiumMasterDirectWrittenPremiumAmount, 
	PaidLossAmount, 
	OutstandingLossAmount, 
	PolicyExpirationDate, 
	InceptionToDatePaidLossAmount, 
	ClaimantCoverageDetailID, 
	AnnualStatementLineNumber, 
	TransactionEffectiveDate, 
	LocationNumber, 
	BuildingNumber
	FROM EXP_TargetCommercialProperty
	ORDER BY PolicyKey ASC, PremiumMasterClassCode ASC, CoverageCode ASC, AnnualStatementLineNumber ASC
),
ISSFlatFile_CF AS (
	INSERT INTO ISSFlatFile
	(FileName, WorkISSExtractId, AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, DeductibleAmount, CoverageCode, ConstructionCode, ISOFireProtectionCode, SublineCode, PackageModificationAdjustmentGroupDescription, PolicyForm, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailID, AnnualStatementLineNumber, TransactionEffectiveDate, LocationNumber, BuildingNumber)
	SELECT 
	FILENAME, 
	WORKISSEXTRACTID, 
	AUDITID, 
	CREATEDDATE, 
	EDWPREMIUMMASTERCALCULATIONPKID, 
	EDWLOSSMASTERCALCULATIONPKID, 
	TYPEBUREAUCODE, 
	BUREAULINEOFINSURANCE, 
	BUREAUCOMPANYNUMBER, 
	STATEPROVINCECODE, 
	PREMIUMMASTERRUNDATE, 
	LOSSMASTERRUNDATE, 
	POLICYKEY, 
	PREMIUMMASTERCLASSCODE, 
	LOSSMASTERCLASSCODE, 
	CLAIMNUMBER, 
	CLAIMANTNUMBER, 
	RISKTERRITORYCODE, 
	POLICYEFFECTIVEDATE, 
	CAUSEOFLOSS, 
	DEDUCTIBLEAMOUNT, 
	COVERAGECODE, 
	CONSTRUCTIONCODE, 
	ISOFIREPROTECTIONCODE, 
	SUBLINECODE, 
	PACKAGEMODIFICATIONADJUSTMENTGROUPDESCRIPTION, 
	POLICYFORM, 
	PREMIUMMASTERDIRECTWRITTENPREMIUMAMOUNT, 
	PAIDLOSSAMOUNT, 
	OUTSTANDINGLOSSAMOUNT, 
	POLICYEXPIRATIONDATE, 
	INCEPTIONTODATEPAIDLOSSAMOUNT, 
	CLAIMANTCOVERAGEDETAILID, 
	ANNUALSTATEMENTLINENUMBER, 
	TRANSACTIONEFFECTIVEDATE, 
	LOCATIONNUMBER, 
	BUILDINGNUMBER
	FROM SRT_ISS_FlatFile_CF
),
EXP_TargetCrime AS (
	SELECT
	-- *INF*: TRUNC(sysdate,'DD')
	CAST(TRUNC(CURRENT_TIMESTAMP, 'DAY') AS TIMESTAMP_NTZ(0)) AS v_RunDate,
	-- *INF*: 'ISS_Crime_'||TO_CHAR(v_RunDate,'YYYYMMDD')||'.CSV'
	'ISS_Crime_' || TO_CHAR(v_RunDate, 'YYYYMMDD') || '.CSV' AS FileName,
	WorkISSExtractId,
	AuditId,
	CreatedDate,
	EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId,
	TypeBureauCode,
	BureauLineOfInsurance,
	BureauCompanyNumber,
	StateProvinceCode,
	PremiumMasterRunDate,
	LossMasterRunDate,
	PolicyKey,
	PremiumMasterClassCode,
	LossMasterClassCode,
	ClaimNumber,
	ClaimantNumber,
	RiskTerritoryCode,
	PolicyEffectiveDate,
	CauseOfLoss,
	DeductibleAmount,
	CoverageCode,
	ConstructionCode,
	ISOFireProtectionCode,
	SublineCode,
	PackageModificationAdjustmentGroupDescription,
	PolicyForm,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	PolicyExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailID,
	AnnualStatementLineNumber,
	TransactionEffectiveDate,
	LocationNumber,
	BuildingNumber,
	'00' AS DefaultString,
	03 AS o_BureauLineOfInsurance,
	'N/A' AS o_PremiumMasterClassCode,
	'N/A' AS o_LossMasterClassCode,
	'N/A' AS o_CauseOfLoss,
	'N/A' AS o_DeductibleAmount,
	'N/A' AS o_CoverageCode
	FROM RTR_CR_CF_DEFAULT1
),
SRT_ISS_FlatFile_CR AS (
	SELECT
	FileName, 
	WorkISSExtractId, 
	AuditId, 
	CreatedDate, 
	EDWPremiumMasterCalculationPKId, 
	EDWLossMasterCalculationPKId, 
	TypeBureauCode, 
	o_BureauLineOfInsurance AS BureauLineOfInsurance, 
	BureauCompanyNumber, 
	StateProvinceCode, 
	PremiumMasterRunDate, 
	LossMasterRunDate, 
	PolicyKey, 
	o_PremiumMasterClassCode AS PremiumMasterClassCode, 
	o_LossMasterClassCode AS LossMasterClassCode, 
	ClaimNumber, 
	ClaimantNumber, 
	RiskTerritoryCode, 
	PolicyEffectiveDate, 
	o_CauseOfLoss AS CauseOfLoss, 
	o_DeductibleAmount AS DeductibleAmount, 
	o_CoverageCode AS CoverageCode, 
	DefaultString AS ConstructionCode, 
	DefaultString AS ISOFireProtectionCode, 
	SublineCode, 
	PackageModificationAdjustmentGroupDescription, 
	PolicyForm, 
	PremiumMasterDirectWrittenPremiumAmount, 
	PaidLossAmount, 
	OutstandingLossAmount, 
	PolicyExpirationDate, 
	InceptionToDatePaidLossAmount, 
	ClaimantCoverageDetailID, 
	AnnualStatementLineNumber, 
	TransactionEffectiveDate, 
	LocationNumber, 
	BuildingNumber
	FROM EXP_TargetCrime
	ORDER BY PolicyKey ASC, PremiumMasterClassCode ASC, CoverageCode ASC, AnnualStatementLineNumber ASC
),
ISSFlatFile_CR AS (
	INSERT INTO ISSFlatFile
	(FileName, WorkISSExtractId, AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, DeductibleAmount, CoverageCode, ConstructionCode, ISOFireProtectionCode, SublineCode, PackageModificationAdjustmentGroupDescription, PolicyForm, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailID, AnnualStatementLineNumber, TransactionEffectiveDate, LocationNumber, BuildingNumber)
	SELECT 
	FILENAME, 
	WORKISSEXTRACTID, 
	AUDITID, 
	CREATEDDATE, 
	EDWPREMIUMMASTERCALCULATIONPKID, 
	EDWLOSSMASTERCALCULATIONPKID, 
	TYPEBUREAUCODE, 
	BUREAULINEOFINSURANCE, 
	BUREAUCOMPANYNUMBER, 
	STATEPROVINCECODE, 
	PREMIUMMASTERRUNDATE, 
	LOSSMASTERRUNDATE, 
	POLICYKEY, 
	PREMIUMMASTERCLASSCODE, 
	LOSSMASTERCLASSCODE, 
	CLAIMNUMBER, 
	CLAIMANTNUMBER, 
	RISKTERRITORYCODE, 
	POLICYEFFECTIVEDATE, 
	CAUSEOFLOSS, 
	DEDUCTIBLEAMOUNT, 
	COVERAGECODE, 
	CONSTRUCTIONCODE, 
	ISOFIREPROTECTIONCODE, 
	SUBLINECODE, 
	PACKAGEMODIFICATIONADJUSTMENTGROUPDESCRIPTION, 
	POLICYFORM, 
	PREMIUMMASTERDIRECTWRITTENPREMIUMAMOUNT, 
	PAIDLOSSAMOUNT, 
	OUTSTANDINGLOSSAMOUNT, 
	POLICYEXPIRATIONDATE, 
	INCEPTIONTODATEPAIDLOSSAMOUNT, 
	CLAIMANTCOVERAGEDETAILID, 
	ANNUALSTATEMENTLINENUMBER, 
	TRANSACTIONEFFECTIVEDATE, 
	LOCATIONNUMBER, 
	BUILDINGNUMBER
	FROM SRT_ISS_FlatFile_CR
),
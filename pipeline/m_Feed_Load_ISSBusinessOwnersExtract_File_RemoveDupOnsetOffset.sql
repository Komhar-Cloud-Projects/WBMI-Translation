WITH
SQ_ISSBusinessOwnersExtract_Loss_Unique AS (
	SELECT ISBO.ISSBusinessOwnersExtractId, 
	ISBO.AuditId, 
	ISBO.CreatedDate,
	 ISBO.EDWPremiumMasterCalculationPKId, 
	 ISBO.EDWLossMasterCalculationPKId,
	  ISBO.TypeBureauCode, 
	  ISBO.BureauLineOfInsurance,
	   ISBO.BureauCompanyNumber,
	    ISBO.StateProvinceCode,
		 ISBO.PremiumMasterRunDate,
		  ISBO.LossMasterRunDate,
		   ISBO.PolicyKey, 
		   ISBO.PremiumMasterClassCode,
		    ISBO.LossMasterClassCode, 
			ISBO.ClaimNumber, 
			ISBO.ClaimantNumber, 
			ISBO.RiskTerritoryCode,
			 ISBO.PolicyEffectiveDate,
			  ISBO.CauseOfLoss, 
			  ISBO.CoverageCode,
			   ISBO.ISOFireProtectionCode, 
			   ISBO.TypeOfPolicyForm,
			  ISBO.PremiumMasterDirectWrittenPremiumAmount,
			   ISBO.PaidLossAmount,
			    ISBO.OutstandingLossAmount,
				 ISBO.PolicyExpirationDate, 
				 ISBO.InceptionToDatePaidLossAmount,
				  ISBO.ClaimantCoverageDetailId,
				   ISBO.AnnualStatementLineNumber,
				    ISBO.PolicyLimit, 
					ISBO.ExposureBasis, ISBO.ConstructionCode,
					 ISBO.SprinklerFlag, ISBO.LimitOfInsurance, 
					 ISBO.WrittenExposure, ISBO.PaidAllocatedLossAdjustmentExpenseAmount,
					  ISBO.OutstandingAllocatedLossAdjustmentExpenseAmount, 
					  ISBO.ClaimLossDate, ISBO.ZipPostalCode,
					   ISBO.TransactionEffectiveDate, ISBO.BusinessClassificationCode ,
	                            ISBO.LocationNumber,
	                            ISBO.BuildingNumber
	FROM
	 ISSBusinessOwnersExtract ISBO 
	WHERE ISBO.EDWLossMasterCalculationPKId<>-1  and
	ISBO.LossMasterRunDate
	between 
	 DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0)--first day of last Quarter   
	 AND
	 DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0)) -- Last day of last Quarter
),
EXP_Loss_Uinique AS (
	SELECT
	ISSBusinessOwnersExtractId,
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
	CoverageCode,
	ISOFireProtectionCode,
	TypeOfPolicyForm,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	PolicyExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId,
	AnnualStatementLineNumber,
	PolicyLimit,
	ExposureBasis,
	ConstructionCode,
	SprinklerFlag,
	LimitOfInsurance,
	WrittenExposure,
	PaidAllocatedLossAdjustmentExpenseAmount,
	OutstandingAllocatedLossAdjustmentExpenseAmount,
	ClaimLossDate,
	ZipPostalCode,
	TransactionEffectiveDate,
	BusinessClassificationCode,
	LocationNumber,
	BuildingNumber
	FROM SQ_ISSBusinessOwnersExtract_Loss_Unique
),
SQ_ISSBusinessOwnersExtract_PremiumDup AS (
	WITH ROLLUP_TABLE_TEMP
	AS
	(SELECT
			SUM(PremiumMasterDirectWrittenPremiumAmount) ROLL_UP_DWP_AMT
			,MAX(ISSBusinessOwnersExtractId) MAX_ISS_KEY
		FROM ISSBusinessOwnersExtract
		WHERE EDWPremiumMasterCalculationPKId <> -1
		AND (ISSBusinessOwnersExtract.PremiumMasterRunDate
		BETWEEN
		DATEADD(qq, DATEDIFF(qq, 0, GETDATE()) + @{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER}, 0)--first day of last Quarter   
		AND
		DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE()) + @{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0)) -- Last day of last Quarter
		)
		GROUP BY CONCAT(TypeBureauCode, BureauLineOfInsurance,
		BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode,
		LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, CoverageCode,
		ISOFireProtectionCode, TypeOfPolicyForm, ABS(PaidLossAmount), ABS(OutstandingLossAmount),
		PolicyExpirationDate, ABS(InceptionToDatePaidLossAmount), ClaimantCoverageDetailId, AnnualStatementLineNumber, PolicyLimit,
		ExposureBasis, ConstructionCode, SprinklerFlag, LimitOfInsurance, ABS(WrittenExposure), ABS(PaidAllocatedLossAdjustmentExpenseAmount),
		ABS(OutstandingAllocatedLossAdjustmentExpenseAmount), ClaimLossDate, ZipPostalCode, TransactionEffectiveDate,
		BusinessClassificationCode, LocationNumber, BuildingNumber)
		HAVING COUNT(1) > 1)
	
	SELECT
		ISBO.ISSBusinessOwnersExtractId
		,ISBO.AuditId
		,ISBO.CreatedDate
		,ISBO.EDWPremiumMasterCalculationPKId
		,ISBO.EDWLossMasterCalculationPKId
		,ISBO.TypeBureauCode
		,ISBO.BureauLineOfInsurance
		,ISBO.BureauCompanyNumber
		,ISBO.StateProvinceCode
		,ISBO.PremiumMasterRunDate
		,ISBO.LossMasterRunDate
		,ISBO.PolicyKey
		,ISBO.PremiumMasterClassCode
		,ISBO.LossMasterClassCode
		,ISBO.ClaimNumber
		,ISBO.ClaimantNumber
		,ISBO.RiskTerritoryCode
		,ISBO.PolicyEffectiveDate
		,ISBO.CauseOfLoss
		,ISBO.CoverageCode
		,ISBO.ISOFireProtectionCode
		,ISBO.TypeOfPolicyForm
		,ROLLUP_TABLE_TEMP.ROLL_UP_DWP_AMT AS PremiumMasterDirectWrittenPremiumAmount
		,ISBO.PaidLossAmount
		,ISBO.OutstandingLossAmount
		,ISBO.PolicyExpirationDate
		,ISBO.InceptionToDatePaidLossAmount
		,ISBO.ClaimantCoverageDetailId
		,ISBO.AnnualStatementLineNumber
		,ISBO.PolicyLimit
		,ISBO.ExposureBasis
		,ISBO.ConstructionCode
		,ISBO.SprinklerFlag
		,ISBO.LimitOfInsurance
		,CASE
			WHEN ROLLUP_TABLE_TEMP.ROLL_UP_DWP_AMT > 0 THEN ABS(ISBO.WrittenExposure)
			ELSE (-1 * ABS(ISBO.WrittenExposure))
		END AS WrittenExposure
		,ISBO.PaidAllocatedLossAdjustmentExpenseAmount
		,ISBO.OutstandingAllocatedLossAdjustmentExpenseAmount
		,ISBO.ClaimLossDate
		,ISBO.ZipPostalCode
		,ISBO.TransactionEffectiveDate
		,ISBO.BusinessClassificationCode
		,ISBO.LocationNumber
		,ISBO.BuildingNumber
	FROM ROLLUP_TABLE_TEMP
	INNER JOIN ISSBusinessOwnersExtract ISBO
		ON ISBO.ISSBusinessOwnersExtractid = ROLLUP_TABLE_TEMP.MAX_ISS_KEY
			AND ISBO.EDWPremiumMasterCalculationPKId <> -1
			AND (ISBO.PremiumMasterRunDate
			BETWEEN
			DATEADD(qq, DATEDIFF(qq, 0, GETDATE()) + @{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER}, 0)--first day of last Quarter   
			AND
			DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE()) + @{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0)) -- Last day of last Quarter
			)
	WHERE ROLLUP_TABLE_TEMP.ROLL_UP_DWP_AMT <> 0
),
EXP_Premium_Dup AS (
	SELECT
	ISSBusinessOwnersExtractId,
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
	CoverageCode,
	ISOFireProtectionCode,
	TypeOfPolicyForm,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	PolicyExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId,
	AnnualStatementLineNumber,
	PolicyLimit,
	ExposureBasis,
	ConstructionCode,
	SprinklerFlag,
	LimitOfInsurance,
	WrittenExposure,
	PaidAllocatedLossAdjustmentExpenseAmount,
	OutstandingAllocatedLossAdjustmentExpenseAmount,
	ClaimLossDate,
	ZipPostalCode,
	TransactionEffectiveDate,
	BusinessClassificationCode,
	LocationNumber,
	BuildingNumber
	FROM SQ_ISSBusinessOwnersExtract_PremiumDup
),
SQ_ISSBusinessOwnersExtract_Premium_Unique AS (
	with premium_unique as
	(
	SELECT   
	   concat( TypeBureauCode, BureauLineOfInsurance,
	     BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate,LossMasterRunDate, PolicyKey, PremiumMasterClassCode, 
		  LossMasterClassCode,ClaimNumber,ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate,CauseOfLoss,CoverageCode,
		  ISOFireProtectionCode,TypeOfPolicyForm,abs(PaidLossAmount),abs(OutstandingLossAmount),
		  PolicyExpirationDate,abs(InceptionToDatePaidLossAmount),ClaimantCoverageDetailId,AnnualStatementLineNumber,PolicyLimit, 
		  ExposureBasis,ConstructionCode,SprinklerFlag,LimitOfInsurance,abs(WrittenExposure),abs(PaidAllocatedLossAdjustmentExpenseAmount), 
		  abs(OutstandingAllocatedLossAdjustmentExpenseAmount),ClaimLossDate,ZipPostalCode,TransactionEffectiveDate,
		  BusinessClassificationCode,LocationNumber,BuildingNumber) as concat, count(*) as count 
	FROM
	 ISSBusinessOwnersExtract  where EDWPremiumMasterCalculationPKId<>-1  and 
	ISSBusinessOwnersExtract .PremiumMasterRunDate
	 between 
	 DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0)--first day of last Quarter   
	 AND
	 DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0)) -- Last day of last Quarter
	 group by concat( TypeBureauCode, BureauLineOfInsurance,
	     BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate,LossMasterRunDate, PolicyKey, PremiumMasterClassCode, 
		  LossMasterClassCode,ClaimNumber,ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate,CauseOfLoss,CoverageCode,
		  ISOFireProtectionCode,TypeOfPolicyForm,abs(PaidLossAmount),abs(OutstandingLossAmount),
		  PolicyExpirationDate,abs(InceptionToDatePaidLossAmount),ClaimantCoverageDetailId,AnnualStatementLineNumber,PolicyLimit, 
		  ExposureBasis,ConstructionCode,SprinklerFlag,LimitOfInsurance,abs(WrittenExposure),abs(PaidAllocatedLossAdjustmentExpenseAmount), 
		  abs(OutstandingAllocatedLossAdjustmentExpenseAmount),ClaimLossDate,ZipPostalCode,TransactionEffectiveDate,
		  BusinessClassificationCode,LocationNumber,BuildingNumber)  having  count(*)=1
	 )
	 SELECT   ISSBusinessOwnersExtractId,
	    AuditId,   CreatedDate, 
	    EDWPremiumMasterCalculationPKId,   EDWLossMasterCalculationPKId, 
	    TypeBureauCode,   BureauLineOfInsurance,   BureauCompanyNumber, 
	    StateProvinceCode,   PremiumMasterRunDate,   LossMasterRunDate, 
	    PolicyKey,   PremiumMasterClassCode,   LossMasterClassCode,   ClaimNumber, 
	    ClaimantNumber,   RiskTerritoryCode,   PolicyEffectiveDate,   CauseOfLoss, 
	    CoverageCode,   ISOFireProtectionCode,   TypeOfPolicyForm,   PremiumMasterDirectWrittenPremiumAmount, 
	    PaidLossAmount,   OutstandingLossAmount,   PolicyExpirationDate,   InceptionToDatePaidLossAmount, 
	    ClaimantCoverageDetailId,   AnnualStatementLineNumber,   PolicyLimit,   ExposureBasis,   ConstructionCode, 
	    SprinklerFlag,   LimitOfInsurance,   WrittenExposure,   PaidAllocatedLossAdjustmentExpenseAmount,
	     OutstandingAllocatedLossAdjustmentExpenseAmount,   ClaimLossDate, 
	     ZipPostalCode,   TransactionEffectiveDate,   BusinessClassificationCode,
	      LocationNumber,   BuildingNumber 
	FROM
	 ISSBusinessOwnersExtract   inner join   premium_unique b on
	 concat( TypeBureauCode, BureauLineOfInsurance,
	     BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate,LossMasterRunDate, PolicyKey, PremiumMasterClassCode, 
		  LossMasterClassCode,ClaimNumber,ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate,CauseOfLoss,CoverageCode,
		  ISOFireProtectionCode,TypeOfPolicyForm,abs(PaidLossAmount),abs(OutstandingLossAmount),
		  PolicyExpirationDate,abs(InceptionToDatePaidLossAmount),ClaimantCoverageDetailId,AnnualStatementLineNumber,PolicyLimit, 
		  ExposureBasis,ConstructionCode,SprinklerFlag,LimitOfInsurance,abs(WrittenExposure),abs(PaidAllocatedLossAdjustmentExpenseAmount), 
		  abs(OutstandingAllocatedLossAdjustmentExpenseAmount),ClaimLossDate,ZipPostalCode,TransactionEffectiveDate,
		  BusinessClassificationCode,LocationNumber,BuildingNumber)= b.concat
	WHERE EDWPremiumMasterCalculationPKId<>-1 and
	ISSBusinessOwnersExtract .PremiumMasterRunDate
	 between 
	 DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0)--first day of last Quarter   
	 AND
	 DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0)) -- Last day of last Quarter
),
EXP_Premium_Unique AS (
	SELECT
	ISSBusinessOwnersExtractId,
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
	CoverageCode,
	ISOFireProtectionCode,
	TypeOfPolicyForm,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	PolicyExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId,
	AnnualStatementLineNumber,
	PolicyLimit,
	ExposureBasis,
	ConstructionCode,
	SprinklerFlag,
	LimitOfInsurance,
	WrittenExposure,
	PaidAllocatedLossAdjustmentExpenseAmount,
	OutstandingAllocatedLossAdjustmentExpenseAmount,
	ClaimLossDate,
	ZipPostalCode,
	TransactionEffectiveDate,
	BusinessClassificationCode,
	LocationNumber,
	BuildingNumber
	FROM SQ_ISSBusinessOwnersExtract_Premium_Unique
),
UN_Union_all AS (
	SELECT ISSBusinessOwnersExtractId, AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, CoverageCode, ISOFireProtectionCode, TypeOfPolicyForm, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, AnnualStatementLineNumber, PolicyLimit, ExposureBasis, ConstructionCode, SprinklerFlag, LimitOfInsurance, WrittenExposure, PaidAllocatedLossAdjustmentExpenseAmount, OutstandingAllocatedLossAdjustmentExpenseAmount, ClaimLossDate, ZipPostalCode, TransactionEffectiveDate, BusinessClassificationCode, LocationNumber, BuildingNumber
	FROM EXP_Loss_Uinique
	UNION
	SELECT ISSBusinessOwnersExtractId, AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, CoverageCode, ISOFireProtectionCode, TypeOfPolicyForm, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, AnnualStatementLineNumber, PolicyLimit, ExposureBasis, ConstructionCode, SprinklerFlag, LimitOfInsurance, WrittenExposure, PaidAllocatedLossAdjustmentExpenseAmount, OutstandingAllocatedLossAdjustmentExpenseAmount, ClaimLossDate, ZipPostalCode, TransactionEffectiveDate, BusinessClassificationCode, LocationNumber, BuildingNumber
	FROM EXP_Premium_Dup
	UNION
	SELECT ISSBusinessOwnersExtractId, AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, CoverageCode, ISOFireProtectionCode, TypeOfPolicyForm, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, AnnualStatementLineNumber, PolicyLimit, ExposureBasis, ConstructionCode, SprinklerFlag, LimitOfInsurance, WrittenExposure, PaidAllocatedLossAdjustmentExpenseAmount, OutstandingAllocatedLossAdjustmentExpenseAmount, ClaimLossDate, ZipPostalCode, TransactionEffectiveDate, BusinessClassificationCode, LocationNumber, BuildingNumber
	FROM EXP_Premium_Unique
),
EXP_Set_FileName AS (
	SELECT
	-- *INF*: TRUNC(SYSDATE,'DD')
	CAST(TRUNC(CURRENT_TIMESTAMP, 'DAY') AS TIMESTAMP_NTZ(0)) AS v_RunDate,
	-- *INF*: 'ISS_BO_CL_'||TO_CHAR(v_RunDate,'YYYYMMDD')||'.CSV'
	'ISS_BO_CL_' || TO_CHAR(v_RunDate, 'YYYYMMDD') || '.CSV' AS FileName,
	ISSBusinessOwnersExtractId,
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
	CoverageCode,
	ISOFireProtectionCode,
	TypeOfPolicyForm,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	PolicyExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId,
	AnnualStatementLineNumber,
	PolicyLimit,
	ExposureBasis,
	ConstructionCode,
	SprinklerFlag,
	LimitOfInsurance,
	WrittenExposure,
	PaidAllocatedLossAdjustmentExpenseAmount,
	OutstandingAllocatedLossAdjustmentExpenseAmount,
	ClaimLossDate,
	ZipPostalCode,
	TransactionEffectiveDate,
	BusinessClassificationCode,
	LocationNumber,
	BuildingNumber
	FROM UN_Union_all
),
LKP_ISSBusinessOwnersExtract AS (
	SELECT
	lkp_PolicyKey,
	PolicyKey,
	CoverageCode,
	ISOFireProtectionCode,
	LocationNumber,
	lkp_LocationNumber
	FROM (
		SELECT ISBO.CoverageCode as CoverageCode, ISBO.ISOFireProtectionCode as ISOFireProtectionCode, ISBO.PolicyKey as lkp_PolicyKey, ISBO.LocationNumber as lkp_LocationNumber FROM ISSBusinessOwnersExtract ISBO
		where ISBO.IsoFireProtectionCode is not null 
		and ISBO.IsoFireProtectionCode <> '00' and ISBO.IsoFireProtectionCode <> 'N/A' 
		and ISBO.CoverageCode <> 'BLKBC'
		and
		((ISBO.PremiumMasterRunDate
		 between 
		 DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0)--first day of last Quarter   
		 AND
		 DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0)) -- Last day of last Quarter
		)
		OR
		(ISBO.LossMasterRunDate
		between 
		 DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0)--first day of last Quarter   
		 AND
		 DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0)) -- Last day of last Quarter
		)
		)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY lkp_PolicyKey,lkp_LocationNumber ORDER BY lkp_PolicyKey) = 1
),
EXP_ISOProtectionCode AS (
	SELECT
	LKP_ISSBusinessOwnersExtract.ISOFireProtectionCode AS lkp_ISOFireProtectionCode,
	EXP_Set_FileName.ISOFireProtectionCode,
	LKP_ISSBusinessOwnersExtract.CoverageCode AS lkp_CoverageCode,
	EXP_Set_FileName.CoverageCode,
	-- *INF*: ISOFireProtectionCode
	-- --iif( 
	-- --CoverageCode  <>  'BLKBC',ISOFireProtectionCode,
	-- --iif(isnull(lkp_ISOFireProtectionCode),'N/A',lkp_ISOFireProtectionCode))
	-- 
	-- 
	ISOFireProtectionCode AS o_ISOFireProtectionCode
	FROM EXP_Set_FileName
	LEFT JOIN LKP_ISSBusinessOwnersExtract
	ON LKP_ISSBusinessOwnersExtract.lkp_PolicyKey = EXP_Set_FileName.PolicyKey AND LKP_ISSBusinessOwnersExtract.lkp_LocationNumber = EXP_Set_FileName.LocationNumber
),
SRT_ISS_FlatFile_BO AS (
	SELECT
	EXP_Set_FileName.FileName, 
	EXP_Set_FileName.ISSBusinessOwnersExtractId AS WorkISSExtractId, 
	EXP_Set_FileName.AuditId, 
	EXP_Set_FileName.CreatedDate, 
	EXP_Set_FileName.EDWPremiumMasterCalculationPKId, 
	EXP_Set_FileName.EDWLossMasterCalculationPKId, 
	EXP_Set_FileName.TypeBureauCode, 
	EXP_Set_FileName.BureauLineOfInsurance, 
	EXP_Set_FileName.BureauCompanyNumber, 
	EXP_Set_FileName.StateProvinceCode, 
	EXP_Set_FileName.PremiumMasterRunDate, 
	EXP_Set_FileName.LossMasterRunDate, 
	EXP_Set_FileName.PolicyKey, 
	EXP_Set_FileName.PremiumMasterClassCode, 
	EXP_Set_FileName.LossMasterClassCode, 
	EXP_Set_FileName.ClaimNumber, 
	EXP_Set_FileName.ClaimantNumber, 
	EXP_Set_FileName.RiskTerritoryCode, 
	EXP_Set_FileName.PolicyEffectiveDate, 
	EXP_Set_FileName.CauseOfLoss, 
	EXP_Set_FileName.CoverageCode, 
	EXP_ISOProtectionCode.o_ISOFireProtectionCode AS ISOFireProtectionCode, 
	EXP_Set_FileName.TypeOfPolicyForm, 
	EXP_Set_FileName.PremiumMasterDirectWrittenPremiumAmount, 
	EXP_Set_FileName.PaidLossAmount, 
	EXP_Set_FileName.OutstandingLossAmount, 
	EXP_Set_FileName.PolicyExpirationDate, 
	EXP_Set_FileName.InceptionToDatePaidLossAmount, 
	EXP_Set_FileName.ClaimantCoverageDetailId, 
	EXP_Set_FileName.AnnualStatementLineNumber, 
	EXP_Set_FileName.PolicyLimit, 
	EXP_Set_FileName.ExposureBasis, 
	EXP_Set_FileName.ConstructionCode, 
	EXP_Set_FileName.SprinklerFlag, 
	EXP_Set_FileName.LimitOfInsurance, 
	EXP_Set_FileName.WrittenExposure, 
	EXP_Set_FileName.PaidAllocatedLossAdjustmentExpenseAmount, 
	EXP_Set_FileName.OutstandingAllocatedLossAdjustmentExpenseAmount, 
	EXP_Set_FileName.ClaimLossDate, 
	EXP_Set_FileName.ZipPostalCode, 
	EXP_Set_FileName.TransactionEffectiveDate, 
	EXP_Set_FileName.BusinessClassificationCode, 
	EXP_Set_FileName.LocationNumber, 
	EXP_Set_FileName.BuildingNumber
	FROM EXP_Set_FileName
	ORDER BY PolicyKey ASC, PremiumMasterClassCode ASC, CoverageCode ASC, AnnualStatementLineNumber ASC, ZipPostalCode ASC
),
ISSFlatFile_BO AS (
	INSERT INTO ISSFlatFile_BO
	(FileName, WorkISSExtractId, AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, CoverageCode, ISOFireProtectionCode, TypeOfPolicyForm, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, AnnualStatementLineNumber, PolicyLimit, ExposureBasis, ConstructionCode, SprinklerFlag, LimitOfInsurance, WrittenExposure, PaidAllocatedLossAdjustmentExpenseAmount, OutstandingAllocatedLossAdjustmentExpenseAmount, ClaimLossDate, ZipPostalCode, TransactionEffectiveDate, BusinessClassificationCode, LocationNumber, BuildingNumber)
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
	COVERAGECODE, 
	ISOFIREPROTECTIONCODE, 
	TYPEOFPOLICYFORM, 
	PREMIUMMASTERDIRECTWRITTENPREMIUMAMOUNT, 
	PAIDLOSSAMOUNT, 
	OUTSTANDINGLOSSAMOUNT, 
	POLICYEXPIRATIONDATE, 
	INCEPTIONTODATEPAIDLOSSAMOUNT, 
	CLAIMANTCOVERAGEDETAILID, 
	ANNUALSTATEMENTLINENUMBER, 
	POLICYLIMIT, 
	EXPOSUREBASIS, 
	CONSTRUCTIONCODE, 
	SPRINKLERFLAG, 
	LIMITOFINSURANCE, 
	WRITTENEXPOSURE, 
	PAIDALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	OUTSTANDINGALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	CLAIMLOSSDATE, 
	ZIPPOSTALCODE, 
	TRANSACTIONEFFECTIVEDATE, 
	BUSINESSCLASSIFICATIONCODE, 
	LOCATIONNUMBER, 
	BUILDINGNUMBER
	FROM SRT_ISS_FlatFile_BO
),
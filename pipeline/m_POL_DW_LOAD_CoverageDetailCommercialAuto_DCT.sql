WITH
LKP_5NewColumns AS (
	SELECT
	lkp_result,
	ClassCode,
	RatingStateCode
	FROM (
		SELECT ClassCode as ClassCode,
		RatingStateCode as RatingStateCode,
		VehicleTypeSize+'@1'
		       +BusinessUseClass+'@2'
			   +SecondaryClass+'@3'
			   +FleetType+'@4'
			   +SecondaryClassGroup+'@5'
		         +RadiusOfOperation+'@6'
		      as lkp_result
		  FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationCommercialAuto
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClassCode,RatingStateCode ORDER BY lkp_result) = 1
),
LKP_CoverageLimit AS (
	SELECT
	CoverageLimitType,
	PremiumTransactionAKID
	FROM (
		SELECT
		CoverageLimit.CoverageLimitType AS CoverageLimitType,
		CLB.PremiumTransactionAKID AS PremiumTransactionAKID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageLimit
		INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageLimitBridge CLB
		ON CoverageLimit.CoverageLimitID=CLB.CoverageLimitID
		INNER JOIN  dbo.PremiumTransaction PT ON CLB.PremiumTransactionAKID = PT.PremiumTransactionAKID
		AND PT.CreatedDate >= '@{pipeline().parameters.SELECTION_START_TS}'
		WHERE CoverageLimit.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		ORDER BY PremiumTransactionAKID--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKID ORDER BY CoverageLimitType) = 1
),
SQ_DCT_StageTables AS (
	SELECT DISTINCT WorkDCTCoverageTransaction.CoverageId,
	  WorkDCTCoverageTransaction.CoverageGUID id,
	  DCCARS.Type,
	  DCCATS.GCW,
	  DCCATS.GVW,
	  DCCAVS.RadiusOfOperation,
	  DCCATS.SecondaryClassCategory,
	  DCCATS.UsedInDumping,
	  DCCAVS.Year,
	  CASE WHEN WorkDCTCoverageTransaction.CoverageType like 'HiredAndBorrowed%' THEN HB.StatedAmountEstimate
	  ELSE DCCAVS.StatedAmount END AS StatedAmount,
	  (CASE WHEN DCCARS.Deleted='1' THEN WorkDCTPolicy.TransactionEffectiveDate ELSE '2100-12-31 23:59:59' END) AS VehicleDeleteDate,
	  DCCAVS.VIN,
	  DCCAVS.Make,
	  DCCAVS.Model,
	  DCCARS.VehicleNumber,
	 DCCALine.CompositeRating,
	 DCCAVS.ZoneTerminal,
	 DCCARS.RegistrationState,
	 DCCASS.SubjectToNoFault,
	 DCCACPS.GuestPIP,
	 WBCASS.PipWorkComp,
	 DCCACPS.CoordinationOfBenefits,
	 WBCARS.TotalVehicleCost,
	 WorkDCTTransactionInsuranceLineLocationBridge.RetroactiveDate,
	 DCCACPS.ExclustionOfWorkLoss,
	 DCCACPS.AdditionalLimit,
	 UMBI.IncludeUIM,
	  DCCACPS.WorkComp,
	WBCAPS.MedicalExpenses,
	WBCASS.AdditionalLimitKS,
	WBCASS.AdditionalLimitKY,
	WBCASS.AdditionalLimitMN,
	DCCAVS.ZoneGaraging,
	CASE
		WHEN WorkDCTCoverageTransaction.SubCoverageType = 'OTCCoverage' THEN WBCACOS.ReplacementCost
		WHEN WorkDCTCoverageTransaction.SubCoverageType = 'CollisionCoverage' THEN WBCACCS.ReplacementCost
		END AS ReplacementCost,
	CASE
		WHEN WorkDCTCoverageTransaction.SubCoverageType = 'OTCCoverage' THEN WBCACOS.FullSafetyGlassCoverage
		WHEN WorkDCTCoverageTransaction.CoverageType = 'DriveOtherCarOTC' THEN DCCACDOCOS.FullGlassIndicator
		END AS FullGlassIndicator,
	DCCARS.HistoricVehicle
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTTransactionInsuranceLineLocationBridge
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTCoverageTransaction
	ON
	WorkDCTCoverageTransaction.CoverageId=WorkDCTTransactionInsuranceLineLocationBridge.CoverageId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTInsuranceLine
	ON
	WorkDCTInsuranceLine.LineId=WorkDCTTransactionInsuranceLineLocationBridge.LineId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy
	ON
	WorkDCTInsuranceLine.PolicyId=WorkDCTPolicy.PolicyId AND WorkDCTInsuranceLine.LineType='CommercialAuto'
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCALineStaging DCCALine
	ON 
	WorkDCTTransactionInsuranceLineLocationBridge.LineId=DCCALine.LineId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCARiskStaging DCCARS
	ON 
	WorkDCTCoverageTransaction.ParentCoverageObjectId = DCCARS.CA_RiskId
	--AND WorkDCTCoverageTransaction.ParentCoverageObjectName='DC_CA_Risk'
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCARiskStaging WBCARS
	ON 
	DCCARS.CA_RiskId = WBCARS.CA_RiskId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCAVehicleStaging DCCAVS
	ON 
	DCCARS.CA_RiskId = DCCAVS.CA_RiskId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCATruckStaging DCCATS
	ON 
	DCCAVS.CA_VehicleId = DCCATS.CA_VehicleId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCAStateStaging DCCASS
	ON DCCASS.CA_StateId=DCCARS.CA_StateId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCAStateStaging DCCASS_State
	ON WorkDCTCoverageTransaction.ParentCoverageObjectId = DCCASS_State.CA_StateId
	AND WorkDCTCoverageTransaction.ParentCoverageObjectName='DC_CA_State'
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCACoveragePIPStage DCCACPS
	ON DCCACPS.CoverageId=WorkDCTTransactionInsuranceLineLocationBridge.CoverageId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCAStateStaging WBCASS
	ON WBCASS.CA_StateId=DCCARS.CA_StateId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCAHiredAndBorrowStage HB
	ON WorkDCTCoverageTransaction.ParentCoverageObjectId=HB.CA_StateId
	and WorkDCTCoverageTransaction.ParentCoverageObjectName='DC_CA_State' and HB.Type='riskHiredAndBorrowedPhysicalDamage'
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCACoverageUMBIStaging UMBI
	on UMBI.SessionId=WorkDCTCoverageTransaction.SessionId
	and UMBI.CoverageId=WorkDCTCoverageTransaction.CoverageId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCACoveragePIPStage WBCAPS
	ON WBCAPS.CACoveragePIPId =  DCCACPS.CA_CoveragePIPId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCoverageStaging DCCOTC
	ON DCCOTC.ObjectId = DCCARS.CA_RiskId and DCCOTC.ObjectName = 'DC_CA_Risk' AND DCCOTC.[Type] = 'OTC'
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCoverageStaging DCCCollision
	ON DCCCollision.ObjectId = DCCARS.CA_RiskId and DCCCollision.ObjectName = 'DC_CA_Risk' AND DCCCollision.[Type] = 'Collision'
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCoverageStaging DCCDriveOtherCar
	ON DCCDriveOtherCar.ObjectId = DCCASS_State.CA_StateId and DCCDriveOtherCar.ObjectName = 'DC_CA_State' AND DCCDriveOtherCar.[Type] = 'DriveOtherCarOTC'
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCACoverageOTCStage DCCACOS
	ON DCCACOS.CoverageId = DCCOTC.CoverageId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCACoverageOTCStage WBCACOS
	ON WBCACOS.CA_CoverageOTCId = DCCACOS.CA_CoverageOTCId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCACoverageCollisionStaging DCCACCS
	ON DCCACCS.CoverageId = DCCCollision.CoverageId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCACoverageCollisionStage WBCACCS
	ON WBCACCS.CA_CoverageCollisionId = DCCACCS.CA_CoverageCollisionId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCACoverageDriveOtherCarOTCStage DCCACDOCOS
	ON DCCACDOCOS.CoverageId = DCCDriveOtherCar.CoverageId
	--INNER JOIN (select * from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransaction where SourceSystemID ='DCT'  )  WPT
	--ON WPT. PremiumTransactionStageId= WorkDCTCoverageTransaction.CoverageId
	@{pipeline().parameters.WHERE_CLAUSE_STAGE}
),
EXP_Default AS (
	SELECT
	CoverageId,
	Id,
	Type,
	GCW,
	GVW,
	RadiusOfOperation,
	SecondaryClassCategory,
	UsedInDumping,
	Year,
	StatedAmount,
	VehicleDeleteDate,
	VIN,
	Make,
	Model,
	VehicleNumber,
	CompositeRating,
	ZoneTerminal,
	RegistrationState,
	SubjectToNoFault,
	GuestPIP,
	PipWorkComp,
	CoordinationOfBenefits,
	TotalVehicleCost,
	RetroactiveDate,
	ExclustionOfWorkLoss,
	AdditionalLimit,
	IncludeUIM,
	WorkComp,
	MedicalExpenses,
	-- *INF*: IIF(ISNULL(CoordinationOfBenefits),'N/A',CoordinationOfBenefits)
	IFF(CoordinationOfBenefits IS NULL, 'N/A', CoordinationOfBenefits) AS o_CoordinationOfBenefits,
	-- *INF*: IIF(ISNULL(MedicalExpenses),'N/A',MedicalExpenses)
	IFF(MedicalExpenses IS NULL, 'N/A', MedicalExpenses) AS o_MedicalExpenses,
	-- *INF*: IIF(ISNULL(WorkComp),0,IIF(WorkComp = 'T',1,0))
	IFF(WorkComp IS NULL, 0, IFF(WorkComp = 'T', 1, 0)) AS o_WorkComp,
	AdditionalLimitKS,
	AdditionalLimitKY,
	AdditionalLimitMN,
	ZoneGaraging,
	ReplacementCost,
	-- *INF*: IIF(ISNULL(ReplacementCost),0,IIF(ReplacementCost = 'T',1,0))
	IFF(ReplacementCost IS NULL, 0, IFF(ReplacementCost = 'T', 1, 0)) AS o_ReplacementCost,
	FullGlassIndicator,
	-- *INF*: IIF(ISNULL(FullGlassIndicator),0,IIF(FullGlassIndicator = 'T',1,0))
	IFF(FullGlassIndicator IS NULL, 0, IFF(FullGlassIndicator = 'T', 1, 0)) AS o_FullGlassIndicator,
	HistoricVehicleIndicator,
	-- *INF*: IIF(ISNULL(HistoricVehicleIndicator),0,IIF(HistoricVehicleIndicator= 'T',1,0))
	IFF(HistoricVehicleIndicator IS NULL, 0, IFF(HistoricVehicleIndicator = 'T', 1, 0)) AS o_HistoricVehicleIndicator
	FROM SQ_DCT_StageTables
),
SQ_DCT_ILTables AS (
	SELECT PT.PremiumTransactionID, PT.PremiumTransactionAKID, RC.ClassCode, PT.PremiumTransactionEffectiveDate, 
	RL.StateProvinceCode, WPT.PremiumTransactionStageId 
	FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransaction WPT 
	       INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Premiumtransaction PT ON PT.PremiumTransactionAKID = WPT.PremiumTransactionAKID 
	       INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Ratingcoverage RC ON RC.RatingCoverageAKID = PT.RatingCoverageAKID AND RC.Effectivedate = PT.Effectivedate 
	       INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Policycoverage PC ON RC.PolicyCoverageAKID = PC.PolicyCoverageAKID AND PC.Currentsnapshotflag =1 
	       INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL ON RL.RiskLocationAKID = PC.RiskLocationAKID AND RL.Currentsnapshotflag =1 
	WHERE  PT.Sourcesystemid = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND WPT.Sourcesystemid = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	AND PC.InsuranceLine = 'CommercialAuto'
	@{pipeline().parameters.WHERE_CLAUSE_IL}
),
JNR_IL_Stage AS (SELECT
	EXP_Default.CoverageId, 
	EXP_Default.Id, 
	EXP_Default.Type, 
	EXP_Default.GCW, 
	EXP_Default.GVW, 
	EXP_Default.RadiusOfOperation, 
	EXP_Default.SecondaryClassCategory, 
	EXP_Default.UsedInDumping, 
	EXP_Default.Year, 
	EXP_Default.StatedAmount, 
	EXP_Default.VehicleDeleteDate, 
	EXP_Default.VIN, 
	EXP_Default.Make, 
	EXP_Default.Model, 
	EXP_Default.VehicleNumber, 
	EXP_Default.CompositeRating, 
	EXP_Default.ZoneTerminal, 
	EXP_Default.RegistrationState, 
	EXP_Default.SubjectToNoFault, 
	EXP_Default.GuestPIP, 
	EXP_Default.PipWorkComp, 
	EXP_Default.o_CoordinationOfBenefits AS CoordinationOfBenefits, 
	EXP_Default.TotalVehicleCost, 
	EXP_Default.RetroactiveDate, 
	EXP_Default.ExclustionOfWorkLoss, 
	EXP_Default.AdditionalLimit, 
	EXP_Default.IncludeUIM, 
	SQ_DCT_ILTables.PremiumTransactionID, 
	SQ_DCT_ILTables.PremiumTransactionAKID, 
	SQ_DCT_ILTables.ClassCode, 
	SQ_DCT_ILTables.PremiumTransactionEffectiveDate, 
	SQ_DCT_ILTables.StateProvinceCode, 
	SQ_DCT_ILTables.PremiumTransactionStageId, 
	EXP_Default.o_MedicalExpenses AS MedicalExpenses, 
	EXP_Default.o_WorkComp AS WorkComp, 
	EXP_Default.AdditionalLimitKS, 
	EXP_Default.AdditionalLimitKY, 
	EXP_Default.AdditionalLimitMN, 
	EXP_Default.ZoneGaraging, 
	EXP_Default.o_ReplacementCost AS ReplacementCost, 
	EXP_Default.o_FullGlassIndicator AS FullGlassIndicator, 
	EXP_Default.o_HistoricVehicleIndicator AS HistoricVehicleIndicator
	FROM EXP_Default
	INNER JOIN SQ_DCT_ILTables
	ON SQ_DCT_ILTables.PremiumTransactionStageId = EXP_Default.CoverageId
),
EXP_MetaData AS (
	SELECT
	PremiumTransactionID AS i_PremiumTransactionID,
	PremiumTransactionAKID AS i_PremiumTransactionAKID,
	Type AS i_Type,
	GCW AS i_GCW,
	GVW AS i_GVW,
	RadiusOfOperation AS i_RadiusOfOperation,
	SecondaryClassCategory AS i_SecondaryClassCategory,
	UsedInDumping AS i_UsedInDumping,
	Id AS i_Id,
	Year AS i_Year,
	StatedAmount AS i_StatedAmount,
	VehicleDeleteDate,
	VIN AS i_VIN,
	Make AS i_Make,
	Model AS i_Model,
	VehicleNumber AS i_VehicleNumber,
	CompositeRating AS i_CompositeRating,
	ZoneTerminal AS i_ZoneTerminal,
	RegistrationState AS i_RegistrationState,
	SubjectToNoFault AS i_SubjectToNoFault,
	GuestPIP AS i_GuestPIP,
	PipWorkComp AS i_PipWorkComp,
	CoordinationOfBenefits AS i_CoordinationOfBenefits,
	TotalVehicleCost AS i_TotalVehicleCost,
	AdditionalLimitKS AS i_AdditionalLimitKS,
	AdditionalLimitKY AS i_AdditionalLimitKY,
	AdditionalLimitMN AS i_AdditionalLimitMN,
	ZoneGaraging AS i_ZoneGaraging,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_RegistrationState)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_RegistrationState) AS v_RegistrationState,
	-- *INF*: DECODE(i_SubjectToNoFault,'T','1','F','0',NULL)
	DECODE(i_SubjectToNoFault,
		'T', '1',
		'F', '0',
		NULL) AS v_SubjectToNoFault,
	-- *INF*: DECODE(i_GuestPIP,'T','1','F','0',NULL)
	DECODE(i_GuestPIP,
		'T', '1',
		'F', '0',
		NULL) AS v_GuestPIP,
	-- *INF*: DECODE(i_PipWorkComp,'T','1','F','0',NULL)
	DECODE(i_PipWorkComp,
		'T', '1',
		'F', '0',
		NULL) AS v_PipWorkComp,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_CoordinationOfBenefits)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_CoordinationOfBenefits) AS v_CoordinationOfBenefits,
	-- *INF*: :LKP.LKP_COVERAGELIMIT(i_PremiumTransactionAKID)
	LKP_COVERAGELIMIT_i_PremiumTransactionAKID.CoverageLimitType AS v_CoverageLimitType,
	ClassCode AS i_ClassCode,
	PremiumTransactionEffectiveDate AS i_PTExpDate,
	StateProvinceCode AS i_RSC,
	RetroactiveDate AS i_RetroactiveDate,
	ExclustionOfWorkLoss AS i_ExclustionOfWorkLoss,
	-- *INF*: IIF(ISNULL(i_ExclustionOfWorkLoss) OR i_ExclustionOfWorkLoss='-1' OR i_ExclustionOfWorkLoss='0' OR i_ExclustionOfWorkLoss='n/a','N/A', ltrim(rtrim(i_ExclustionOfWorkLoss)))
	-- 
	-- --IIF(ISNULL(i_ExclustionOfWorkLoss) OR i_ExclustionOfWorkLoss='-1','N/A', ltrim(rtrim(i_ExclustionOfWorkLoss)))
	IFF(i_ExclustionOfWorkLoss IS NULL OR i_ExclustionOfWorkLoss = '-1' OR i_ExclustionOfWorkLoss = '0' OR i_ExclustionOfWorkLoss = 'n/a', 'N/A', ltrim(rtrim(i_ExclustionOfWorkLoss))) AS v_ExclustionOfWorkLoss,
	AdditionalLimit AS i_AdditionalLimit,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_AdditionalLimit),'0',
	-- i_AdditionalLimit='F','0',
	-- i_AdditionalLimit='N','0',
	-- i_AdditionalLimit='0','0',
	-- '1')
	-- 
	-- 
	-- -- think of all the goofy ways Informatica handles bit types and try to translate them.
	DECODE(TRUE,
		i_AdditionalLimit IS NULL, '0',
		i_AdditionalLimit = 'F', '0',
		i_AdditionalLimit = 'N', '0',
		i_AdditionalLimit = '0', '0',
		'1') AS v_AdditionalLimit,
	IncludeUIM AS i_IncludeUIM,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_IncludeUIM),'N/A',
	-- i_IncludeUIM='F','0',
	-- i_IncludeUIM='N','0',
	-- i_IncludeUIM='0','0',
	-- '1')
	DECODE(TRUE,
		i_IncludeUIM IS NULL, 'N/A',
		i_IncludeUIM = 'F', '0',
		i_IncludeUIM = 'N', '0',
		i_IncludeUIM = '0', '0',
		'1') AS v_IncludeUIM,
	-- *INF*: DECODE(true,
	-- NOT ISNULL(:LKP.LKP_5NewColumns(i_ClassCode,i_RSC)),:LKP.LKP_5NewColumns(i_ClassCode,i_RSC),
	-- NOT ISNULL(:LKP.LKP_5NewColumns(i_ClassCode,'99')),:LKP.LKP_5NewColumns(i_ClassCode,'99'),
	-- 'N/A')
	DECODE(true,
		NOT LKP_5NEWCOLUMNS_i_ClassCode_i_RSC.lkp_result IS NULL, LKP_5NEWCOLUMNS_i_ClassCode_i_RSC.lkp_result,
		NOT LKP_5NEWCOLUMNS_i_ClassCode_99.lkp_result IS NULL, LKP_5NEWCOLUMNS_i_ClassCode_99.lkp_result,
		'N/A') AS v_lkp_result,
	-- *INF*: DECODE(TRUE,
	-- in(v_CoverageLimitType,'PersonalInjuryProtectionBasicLimit','PersonalInjuryProtectionExcessLimit') and v_AdditionalLimit = '0','PersonalInjuryProtectionBasicLimit',
	-- 
	-- in(v_CoverageLimitType,'PersonalInjuryProtectionBasicLimit','PersonalInjuryProtectionExcessLimit') and v_AdditionalLimit = '1','PersonalInjuryProtectionExcessLimit',
	-- 
	-- v_CoverageLimitType)
	-- 
	-- 
	-- -- Because CoverageLimitType of basic and excess are almost always created even if they aren't actually selected use the AdditionalLimit indicator as the rule whether excess or basic was selected  and only for cases where Basic or Excess are returned
	DECODE(TRUE,
		in(v_CoverageLimitType, 'PersonalInjuryProtectionBasicLimit', 'PersonalInjuryProtectionExcessLimit') AND v_AdditionalLimit = '0', 'PersonalInjuryProtectionBasicLimit',
		in(v_CoverageLimitType, 'PersonalInjuryProtectionBasicLimit', 'PersonalInjuryProtectionExcessLimit') AND v_AdditionalLimit = '1', 'PersonalInjuryProtectionExcessLimit',
		v_CoverageLimitType) AS v_CoverageLimitType_Alt,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	-- *INF*: RTRIM(LTRIM(i_Type))
	RTRIM(LTRIM(i_Type)) AS o_Type,
	-- *INF*: IIF(ISNULL(i_GCW),0,i_GCW)
	IFF(i_GCW IS NULL, 0, i_GCW) AS o_GCW,
	-- *INF*: IIF(ISNULL(i_GVW),0,i_GVW)
	IFF(i_GVW IS NULL, 0, i_GVW) AS o_GVW,
	-- *INF*: SUBSTR(v_lkp_result,instr(v_lkp_result,'@5')+2,instr(v_lkp_result,'@6')-instr(v_lkp_result,'@5')-2)
	SUBSTR(v_lkp_result, instr(v_lkp_result, '@5') + 2, instr(v_lkp_result, '@6') - instr(v_lkp_result, '@5') - 2) AS o_RadiusOfOperation,
	-- *INF*: IIF(NOT ISNULL(i_SecondaryClassCategory), RTRIM(LTRIM(i_SecondaryClassCategory)), 'N/A')
	IFF(NOT i_SecondaryClassCategory IS NULL, RTRIM(LTRIM(i_SecondaryClassCategory)), 'N/A') AS o_SecondaryClassCategory,
	-- *INF*: DECODE(i_UsedInDumping,'T',1,0)
	DECODE(i_UsedInDumping,
		'T', 1,
		0) AS o_UsedInDumping,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_Id)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_Id) AS o_CoverageGuid,
	-- *INF*: IIF(ISNULL(i_VIN), 'N/A', i_VIN)
	IFF(i_VIN IS NULL, 'N/A', i_VIN) AS o_VIN,
	-- *INF*: IIF(ISNULL(i_Make), 'N/A', i_Make)
	IFF(i_Make IS NULL, 'N/A', i_Make) AS o_Make,
	-- *INF*: IIF(ISNULL(i_Model), 'N/A', i_Model)
	IFF(i_Model IS NULL, 'N/A', i_Model) AS o_Model,
	-- *INF*: IIF(ISNULL(i_VehicleNumber), 0, i_VehicleNumber)
	IFF(i_VehicleNumber IS NULL, 0, i_VehicleNumber) AS o_VehicleNumber,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_CompositeRating),0,
	-- i_CompositeRating='T',1,
	-- i_CompositeRating='F',0,
	-- 0
	-- )
	DECODE(TRUE,
		i_CompositeRating IS NULL, 0,
		i_CompositeRating = 'T', 1,
		i_CompositeRating = 'F', 0,
		0) AS o_CompositeRating,
	-- *INF*: IIF(ISNULL(i_ZoneTerminal),'N/A',TO_CHAR(i_ZoneTerminal))
	IFF(i_ZoneTerminal IS NULL, 'N/A', TO_CHAR(i_ZoneTerminal)) AS o_ZoneTerminal,
	-- *INF*: DECODE(TRUE,
	-- -- rules fields
	--  -- v_RegistrationState v_SubjectToNoFault v_CoverageLimitType  v_CoordinationOfBenefits <>  v_ExclustionOfWorkLoss
	-- 
	-- -- KS----------------------
	-- v_RegistrationState = 'KS' AND v_CoverageLimitType_Alt = 'PersonalInjuryProtectionBasicLimit' ,'681',
	-- 
	-- v_RegistrationState = 'KS' AND v_CoverageLimitType_Alt = 'PersonalInjuryProtectionExcessLimit' ,'682',
	-- 
	-- --  KY-----------------------
	-- v_RegistrationState='KY' AND v_CoverageLimitType_Alt='PersonalInjuryProtectionBasicLimit' AND v_PipWorkComp='1','671',
	-- 
	-- v_RegistrationState='KY' AND v_CoverageLimitType_Alt='PersonalInjuryProtectionExcessLimit' AND v_PipWorkComp='1','672',
	-- 
	-- v_RegistrationState='KY' AND v_CoverageLimitType_Alt='PersonalInjuryProtectionBasicLimit' AND v_PipWorkComp='0','681',
	-- 
	-- v_RegistrationState='KY' AND v_CoverageLimitType_Alt='PersonalInjuryProtectionExcessLimit' AND v_PipWorkComp='0','682',
	-- 
	-- v_RegistrationState='KY' AND v_SubjectToNoFault='0' AND v_GuestPIP='1' AND v_PipWorkComp='1','675',
	-- 
	-- v_RegistrationState='KY' AND v_SubjectToNoFault='0' AND v_GuestPIP='1' AND (v_PipWorkComp='0' or ISNULL(i_PipWorkComp)),'685',
	-- 
	-- -- MI-------------------------
	-- v_RegistrationState='MI'  AND v_PipWorkComp='1','671',
	-- 
	-- v_RegistrationState='MI' AND IN(v_CoordinationOfBenefits,'None','N/A','0') AND v_PipWorkComp='0','681',
	-- 
	-- v_RegistrationState='MI' AND IN(v_CoordinationOfBenefits,'MedicalExpenses') AND v_PipWorkComp='0','691',
	-- 
	-- v_RegistrationState='MI' AND IN(v_CoordinationOfBenefits,'WorkLoss') AND v_PipWorkComp='0','692',
	-- 
	-- v_RegistrationState='MI' AND IN(v_CoordinationOfBenefits,'MedicalWorkLoss') AND v_PipWorkComp='0','693',
	-- 
	-- -- MN -----------------------------
	-- v_RegistrationState='MN' AND IN(v_CoverageLimitType_Alt,'PersonalInjuryProtectionBasicLimit','PersonalInjuryProtectionExcessLimit') AND v_PipWorkComp='1' AND v_ExclustionOfWorkLoss='N/A','671',
	-- 
	-- v_RegistrationState='MN' AND  IN(v_CoverageLimitType_Alt,'PersonalInjuryProtectionBasicLimit','PersonalInjuryProtectionExcessLimit') AND v_PipWorkComp='1' 
	--  AND v_ExclustionOfWorkLoss <> 'N/A', '675',
	-- --AND IN(v_ExclustionOfWorkLoss,'NamedInsuredOnly','NamedInsuredAndRelative'),'675',
	-- 
	-- v_RegistrationState='MN' AND v_GuestPIP='0' AND IN(v_CoverageLimitType_Alt,'PersonalInjuryProtectionBasicLimit','PersonalInjuryProtectionExcessLimit') AND v_PipWorkComp='0' AND v_ExclustionOfWorkLoss='N/A','681',
	-- 
	-- v_RegistrationState='MN' AND v_GuestPIP='0' AND IN(v_CoverageLimitType_Alt,'PersonalInjuryProtectionBasicLimit','PersonalInjuryProtectionExcessLimit') AND v_PipWorkComp='0' 
	-- AND v_ExclustionOfWorkLoss <> 'N/A', '685',
	-- --AND IN(v_ExclustionOfWorkLoss,'NamedInsuredOnly','NamedInsuredAndRelative'),'685',
	-- 'N/A')
	DECODE(TRUE,
		v_RegistrationState = 'KS' AND v_CoverageLimitType_Alt = 'PersonalInjuryProtectionBasicLimit', '681',
		v_RegistrationState = 'KS' AND v_CoverageLimitType_Alt = 'PersonalInjuryProtectionExcessLimit', '682',
		v_RegistrationState = 'KY' AND v_CoverageLimitType_Alt = 'PersonalInjuryProtectionBasicLimit' AND v_PipWorkComp = '1', '671',
		v_RegistrationState = 'KY' AND v_CoverageLimitType_Alt = 'PersonalInjuryProtectionExcessLimit' AND v_PipWorkComp = '1', '672',
		v_RegistrationState = 'KY' AND v_CoverageLimitType_Alt = 'PersonalInjuryProtectionBasicLimit' AND v_PipWorkComp = '0', '681',
		v_RegistrationState = 'KY' AND v_CoverageLimitType_Alt = 'PersonalInjuryProtectionExcessLimit' AND v_PipWorkComp = '0', '682',
		v_RegistrationState = 'KY' AND v_SubjectToNoFault = '0' AND v_GuestPIP = '1' AND v_PipWorkComp = '1', '675',
		v_RegistrationState = 'KY' AND v_SubjectToNoFault = '0' AND v_GuestPIP = '1' AND ( v_PipWorkComp = '0' OR i_PipWorkComp IS NULL ), '685',
		v_RegistrationState = 'MI' AND v_PipWorkComp = '1', '671',
		v_RegistrationState = 'MI' AND IN(v_CoordinationOfBenefits, 'None', 'N/A', '0') AND v_PipWorkComp = '0', '681',
		v_RegistrationState = 'MI' AND IN(v_CoordinationOfBenefits, 'MedicalExpenses') AND v_PipWorkComp = '0', '691',
		v_RegistrationState = 'MI' AND IN(v_CoordinationOfBenefits, 'WorkLoss') AND v_PipWorkComp = '0', '692',
		v_RegistrationState = 'MI' AND IN(v_CoordinationOfBenefits, 'MedicalWorkLoss') AND v_PipWorkComp = '0', '693',
		v_RegistrationState = 'MN' AND IN(v_CoverageLimitType_Alt, 'PersonalInjuryProtectionBasicLimit', 'PersonalInjuryProtectionExcessLimit') AND v_PipWorkComp = '1' AND v_ExclustionOfWorkLoss = 'N/A', '671',
		v_RegistrationState = 'MN' AND IN(v_CoverageLimitType_Alt, 'PersonalInjuryProtectionBasicLimit', 'PersonalInjuryProtectionExcessLimit') AND v_PipWorkComp = '1' AND v_ExclustionOfWorkLoss <> 'N/A', '675',
		v_RegistrationState = 'MN' AND v_GuestPIP = '0' AND IN(v_CoverageLimitType_Alt, 'PersonalInjuryProtectionBasicLimit', 'PersonalInjuryProtectionExcessLimit') AND v_PipWorkComp = '0' AND v_ExclustionOfWorkLoss = 'N/A', '681',
		v_RegistrationState = 'MN' AND v_GuestPIP = '0' AND IN(v_CoverageLimitType_Alt, 'PersonalInjuryProtectionBasicLimit', 'PersonalInjuryProtectionExcessLimit') AND v_PipWorkComp = '0' AND v_ExclustionOfWorkLoss <> 'N/A', '685',
		'N/A') AS o_PIPBureaucoverageCode,
	-- *INF*: IIF(ISNULL(i_TotalVehicleCost),0, i_TotalVehicleCost)
	IFF(i_TotalVehicleCost IS NULL, 0, i_TotalVehicleCost) AS o_TotalVehicleCost,
	-- *INF*: SUBSTR(v_lkp_result,1,instr(v_lkp_result,'@1')-1)
	SUBSTR(v_lkp_result, 1, instr(v_lkp_result, '@1') - 1) AS o_CommercialAutoVehicleType,
	-- *INF*: SUBSTR(v_lkp_result,instr(v_lkp_result,'@1')+2,instr(v_lkp_result,'@2')-instr(v_lkp_result,'@1')-2)
	SUBSTR(v_lkp_result, instr(v_lkp_result, '@1') + 2, instr(v_lkp_result, '@2') - instr(v_lkp_result, '@1') - 2) AS o_CommercialAutoBusinessUseClass,
	-- *INF*: SUBSTR(v_lkp_result,instr(v_lkp_result,'@2')+2,instr(v_lkp_result,'@3')-instr(v_lkp_result,'@2')-2)
	SUBSTR(v_lkp_result, instr(v_lkp_result, '@2') + 2, instr(v_lkp_result, '@3') - instr(v_lkp_result, '@2') - 2) AS o_SecondaryClass,
	-- *INF*: SUBSTR(v_lkp_result,instr(v_lkp_result,'@3')+2,instr(v_lkp_result,'@4')-instr(v_lkp_result,'@3')-2)
	SUBSTR(v_lkp_result, instr(v_lkp_result, '@3') + 2, instr(v_lkp_result, '@4') - instr(v_lkp_result, '@3') - 2) AS o_FleetType,
	-- *INF*: SUBSTR(v_lkp_result,instr(v_lkp_result,'@4')+2,instr(v_lkp_result,'@5')-instr(v_lkp_result,'@4')-2)
	SUBSTR(v_lkp_result, instr(v_lkp_result, '@4') + 2, instr(v_lkp_result, '@5') - instr(v_lkp_result, '@4') - 2) AS o_SecondaryClassGroup,
	-- *INF*: IIF(ISNULL(i_RetroactiveDate),
	-- TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),
	-- i_RetroactiveDate
	-- )
	IFF(i_RetroactiveDate IS NULL, TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), i_RetroactiveDate) AS o_RetroactiveDate,
	v_IncludeUIM AS o_IncludeUIM,
	i_CoordinationOfBenefits AS o_CoordinationOfBenefits,
	MedicalExpenses,
	WorkComp,
	-- *INF*: DECODE(TRUE,
	-- i_SubjectToNoFault='T','Yes',
	-- i_SubjectToNoFault='F','No', 'N/A')
	DECODE(TRUE,
		i_SubjectToNoFault = 'T', 'Yes',
		i_SubjectToNoFault = 'F', 'No',
		'N/A') AS o_SubjectToNoFault,
	-- *INF*: IIF(ISNULL(i_AdditionalLimitKS), -1, i_AdditionalLimitKS)
	IFF(i_AdditionalLimitKS IS NULL, - 1, i_AdditionalLimitKS) AS o_AdditionalLimitKS,
	-- *INF*: IIF(ISNULL(i_AdditionalLimitKY), -1, i_AdditionalLimitKY)
	IFF(i_AdditionalLimitKY IS NULL, - 1, i_AdditionalLimitKY) AS o_AdditionalLimitKY,
	-- *INF*: IIF(ISNULL(i_AdditionalLimitMN), -1, i_AdditionalLimitMN)
	IFF(i_AdditionalLimitMN IS NULL, - 1, i_AdditionalLimitMN) AS o_AdditionalLimitMN,
	-- *INF*: IIF(ISNULL(i_ZoneGaraging),'N/A',TO_CHAR(i_ZoneGaraging))
	IFF(i_ZoneGaraging IS NULL, 'N/A', TO_CHAR(i_ZoneGaraging)) AS o_ZoneGaraging,
	ReplacementCost,
	FullGlassIndicator,
	HistoricVehicleIndicator
	FROM JNR_IL_Stage
	LEFT JOIN LKP_COVERAGELIMIT LKP_COVERAGELIMIT_i_PremiumTransactionAKID
	ON LKP_COVERAGELIMIT_i_PremiumTransactionAKID.PremiumTransactionAKID = i_PremiumTransactionAKID

	LEFT JOIN LKP_5NEWCOLUMNS LKP_5NEWCOLUMNS_i_ClassCode_i_RSC
	ON LKP_5NEWCOLUMNS_i_ClassCode_i_RSC.ClassCode = i_ClassCode
	AND LKP_5NEWCOLUMNS_i_ClassCode_i_RSC.RatingStateCode = i_RSC

	LEFT JOIN LKP_5NEWCOLUMNS LKP_5NEWCOLUMNS_i_ClassCode_99
	ON LKP_5NEWCOLUMNS_i_ClassCode_99.ClassCode = i_ClassCode
	AND LKP_5NEWCOLUMNS_i_ClassCode_99.RatingStateCode = '99'

),
EXP_CoverageDetailCommercialAuto AS (
	SELECT
	o_PremiumTransactionID AS i_PremiumTransactionID,
	o_CoverageGuid AS i_CoverageGuid,
	o_Type AS i_Type,
	o_GCW AS i_GCW,
	o_GVW AS i_GVW,
	o_RadiusOfOperation AS i_RadiusOfOperation,
	o_SecondaryClassCategory AS i_SecondaryClassCategory,
	o_UsedInDumping AS i_UsedInDumping,
	o_VIN AS i_VIN,
	o_Make AS i_Make,
	o_Model AS i_Model,
	o_ZoneTerminal AS i_ZoneTerminal,
	o_CommercialAutoVehicleType AS i_CommercialAutoVehicleType,
	o_CommercialAutoBusinessUseClass AS i_CommercialAutoBusinessUseClass,
	o_SecondaryClass AS i_SecondaryClass,
	o_FleetType AS i_FleetType,
	o_SecondaryClassGroup AS i_SecondaryClassGroup,
	i_Year AS Year,
	i_StatedAmount AS StatedAmount,
	VehicleDeleteDate,
	o_PIPBureaucoverageCode AS PIPBureaucoverageCode,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	-- *INF*: IIF(ISNULL(i_CoverageGuid),'N/A',i_CoverageGuid)
	IFF(i_CoverageGuid IS NULL, 'N/A', i_CoverageGuid) AS o_CoverageGUID,
	-- *INF*: DECODE(TRUE,
	-- i_Type='Truck' ,DECODE(TRUE,i_GVW <= 10000,'Light Trucks',i_GVW  >= 10001 AND i_GVW <= 20000,'Medium Trucks',i_GVW  >= 20001 AND i_GVW <=  45000,'Heavy Trucks',i_GVW > 45000,'Extra Heavy Trucks'),
	-- i_Type='TruckTractor',DECODE(TRUE,i_GCW <= 45000,'Heavy Truck Tractors',i_GCW > 45000,'Extra Heavy Truck Tractors'),
	-- i_Type='Semitrailer','Semitrailers',
	-- i_Type='Trailer','Trailers',
	-- i_Type='ServiceUtilityTrailer','ServiceUtilityTrailers',
	-- IN(i_Type,'PrivatePassenger','FuneralDirectors'), 'PrivatePassenger',
	-- IN(i_Type,'Ambulance','RegistrationPlates','Motorcycle','PublicVehicle','RepossessedAutos','MobileHome','MobileHomeContents','GolfMobile','AntiqueAuto','SpecialOrMobileEquipment', 'Snowmobile', 'AllTerrainUtilityTaskVehicle') ,'Special Class',
	-- i_Type='Garage','Garage',
	-- 'N/A'
	-- )
	DECODE(TRUE,
		i_Type = 'Truck', DECODE(TRUE,
		i_GVW <= 10000, 'Light Trucks',
		i_GVW >= 10001 AND i_GVW <= 20000, 'Medium Trucks',
		i_GVW >= 20001 AND i_GVW <= 45000, 'Heavy Trucks',
		i_GVW > 45000, 'Extra Heavy Trucks'),
		i_Type = 'TruckTractor', DECODE(TRUE,
		i_GCW <= 45000, 'Heavy Truck Tractors',
		i_GCW > 45000, 'Extra Heavy Truck Tractors'),
		i_Type = 'Semitrailer', 'Semitrailers',
		i_Type = 'Trailer', 'Trailers',
		i_Type = 'ServiceUtilityTrailer', 'ServiceUtilityTrailers',
		IN(i_Type, 'PrivatePassenger', 'FuneralDirectors'), 'PrivatePassenger',
		IN(i_Type, 'Ambulance', 'RegistrationPlates', 'Motorcycle', 'PublicVehicle', 'RepossessedAutos', 'MobileHome', 'MobileHomeContents', 'GolfMobile', 'AntiqueAuto', 'SpecialOrMobileEquipment', 'Snowmobile', 'AllTerrainUtilityTaskVehicle'), 'Special Class',
		i_Type = 'Garage', 'Garage',
		'N/A') AS o_VehicleGroupCode,
	-- *INF*: IIF(length(i_RadiusOfOperation)=0,'N/A',i_RadiusOfOperation)
	-- 
	IFF(length(i_RadiusOfOperation) = 0, 'N/A', i_RadiusOfOperation) AS o_RadiusOfOperation,
	-- *INF*: IIF(NOT ISNULL(i_SecondaryClassCategory),i_SecondaryClassCategory,'N/A')
	IFF(NOT i_SecondaryClassCategory IS NULL, i_SecondaryClassCategory, 'N/A') AS o_SecondaryVehicleType,
	i_UsedInDumping AS o_UsedInDumpingIndicator,
	-- *INF*: IIF(NOT ISNULL(i_VIN),i_VIN,'N/A')
	IFF(NOT i_VIN IS NULL, i_VIN, 'N/A') AS o_VIN,
	-- *INF*: IIF(NOT ISNULL(i_Make),i_Make,'N/A')
	IFF(NOT i_Make IS NULL, i_Make, 'N/A') AS o_Make,
	-- *INF*: IIF(NOT ISNULL(i_Model),i_Model,'N/A')
	IFF(NOT i_Model IS NULL, i_Model, 'N/A') AS o_Model,
	o_VehicleNumber,
	o_CompositeRating AS CompositeRating,
	-- *INF*: IIF(NOT ISNULL(i_ZoneTerminal),i_ZoneTerminal,'N/A')
	IFF(NOT i_ZoneTerminal IS NULL, i_ZoneTerminal, 'N/A') AS o_ZoneTerminal,
	-- *INF*: IIF(NOT ISNULL(i_Type),i_Type,'N/A')
	IFF(NOT i_Type IS NULL, i_Type, 'N/A') AS o_VehicleType,
	o_TotalVehicleCost,
	-- *INF*: IIF(length(i_CommercialAutoVehicleType)=0,'N/A',i_CommercialAutoVehicleType)
	IFF(length(i_CommercialAutoVehicleType) = 0, 'N/A', i_CommercialAutoVehicleType) AS o_CommercialAutoVehicleType,
	-- *INF*: IIF(length(i_CommercialAutoBusinessUseClass)=0,'N/A',i_CommercialAutoBusinessUseClass)
	IFF(length(i_CommercialAutoBusinessUseClass) = 0, 'N/A', i_CommercialAutoBusinessUseClass) AS o_CommercialAutoBusinessUseClass,
	-- *INF*: IIF(length(i_SecondaryClass)=0,'N/A',i_SecondaryClass)
	-- 
	-- 
	IFF(length(i_SecondaryClass) = 0, 'N/A', i_SecondaryClass) AS o_SecondaryClass,
	-- *INF*: IIF(length(i_FleetType)=0,'N/A',i_FleetType)
	-- 
	IFF(length(i_FleetType) = 0, 'N/A', i_FleetType) AS o_FleetType,
	-- *INF*: IIF(length(i_SecondaryClassGroup)=0,'N/A',i_SecondaryClassGroup)
	-- 
	-- 
	IFF(length(i_SecondaryClassGroup) = 0, 'N/A', i_SecondaryClassGroup) AS o_SecondaryClassGroup,
	o_RetroactiveDate AS RetroactiveDate,
	o_IncludeUIM AS IncludeUIM,
	o_CoordinationOfBenefits,
	MedicalExpenses,
	WorkComp,
	o_SubjectToNoFault,
	o_AdditionalLimitKS AS AdditionalLimitKS,
	o_AdditionalLimitKY AS AdditionalLimitKY,
	o_AdditionalLimitMN AS AdditionalLimitMN,
	o_ZoneGaraging AS ZoneGaraging,
	ReplacementCost,
	FullGlassIndicator,
	HistoricVehicleIndicator
	FROM EXP_MetaData
),
LKP_CoverageDetailCommercialAuto AS (
	SELECT
	PremiumTransactionID,
	VehicleGroupCode,
	RadiusOfOperation,
	SecondaryVehicleType,
	UsedInDumpingIndicator,
	VehicleYear,
	StatedAmount,
	CostNew,
	VehicleDeleteDate,
	VIN,
	VehicleMake,
	VehicleModel,
	VehicleNumber,
	CompositeRatedFlag,
	TerminalZoneCode,
	VehicleType,
	PIPBureaucoverageCode,
	VehicleTypeSize,
	BusinessUseClass,
	SecondaryClass,
	FleetType,
	SecondaryClassGroup,
	IncludeUIM,
	RatingZoneCode,
	ReplacementCost,
	FullGlassIndicator,
	HistoricVehicleIndicator
	FROM (
		SELECT 
			PremiumTransactionID,
			VehicleGroupCode,
			RadiusOfOperation,
			SecondaryVehicleType,
			UsedInDumpingIndicator,
			VehicleYear,
			StatedAmount,
			CostNew,
			VehicleDeleteDate,
			VIN,
			VehicleMake,
			VehicleModel,
			VehicleNumber,
			CompositeRatedFlag,
			TerminalZoneCode,
			VehicleType,
			PIPBureaucoverageCode,
			VehicleTypeSize,
			BusinessUseClass,
			SecondaryClass,
			FleetType,
			SecondaryClassGroup,
			IncludeUIM,
			RatingZoneCode,
			ReplacementCost,
			FullGlassIndicator,
			HistoricVehicleIndicator
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialAuto
		WHERE SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and PremiumTransactionID  in (select pt.PremiumTransactionID from
		PremiumTransaction pt
		inner join WorkPremiumTransaction wpt
		on pt.PremiumTransactionAKID=wpt.PremiumTransactionAKId)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY PremiumTransactionID) = 1
),
EXP_DetectChanges AS (
	SELECT
	LKP_CoverageDetailCommercialAuto.PremiumTransactionID AS lkp_PremiumTransactionID,
	LKP_CoverageDetailCommercialAuto.VehicleGroupCode AS lkp_VehicleGroupCode,
	LKP_CoverageDetailCommercialAuto.RadiusOfOperation AS lkp_RadiusOfOperation,
	LKP_CoverageDetailCommercialAuto.SecondaryVehicleType AS lkp_SecondaryVehicleType,
	LKP_CoverageDetailCommercialAuto.UsedInDumpingIndicator AS lkp_UsedInDumpingIndicator,
	-- *INF*: IIF(lkp_UsedInDumpingIndicator='T',1,0)
	IFF(lkp_UsedInDumpingIndicator = 'T', 1, 0) AS v_UsedInDumpingIndicator,
	LKP_CoverageDetailCommercialAuto.VehicleYear AS lkp_VehicleYear,
	LKP_CoverageDetailCommercialAuto.StatedAmount AS lkp_StatedAmount,
	LKP_CoverageDetailCommercialAuto.CostNew AS lkp_CostNew,
	LKP_CoverageDetailCommercialAuto.VehicleDeleteDate AS lkp_VehicleDeleteDate,
	LKP_CoverageDetailCommercialAuto.VIN AS lkp_VIN,
	LKP_CoverageDetailCommercialAuto.VehicleMake AS lkp_VehicleMake,
	LKP_CoverageDetailCommercialAuto.VehicleModel AS lkp_VehicleModel,
	LKP_CoverageDetailCommercialAuto.VehicleNumber AS lkp_VehicleNumber,
	LKP_CoverageDetailCommercialAuto.CompositeRatedFlag AS lkp_CompositeRatedFlag,
	-- *INF*: IIF(lkp_CompositeRatedFlag='T',1,0)
	IFF(lkp_CompositeRatedFlag = 'T', 1, 0) AS v_CompositeRatedFlag,
	LKP_CoverageDetailCommercialAuto.TerminalZoneCode AS lkp_TerminalZoneCode,
	LKP_CoverageDetailCommercialAuto.VehicleType AS lkp_VehicleType,
	LKP_CoverageDetailCommercialAuto.PIPBureaucoverageCode AS lkp_PIPBureaucoverageCode,
	LKP_CoverageDetailCommercialAuto.VehicleTypeSize AS lkp_VehicleTypeSize,
	LKP_CoverageDetailCommercialAuto.BusinessUseClass AS lkp_BusinessUseClass,
	LKP_CoverageDetailCommercialAuto.SecondaryClass AS lkp_SecondaryClass,
	LKP_CoverageDetailCommercialAuto.FleetType AS lkp_FleetType,
	LKP_CoverageDetailCommercialAuto.SecondaryClassGroup AS lkp_SecondaryClassGroup,
	LKP_CoverageDetailCommercialAuto.IncludeUIM AS lkp_IncludeUIM,
	LKP_CoverageDetailCommercialAuto.RatingZoneCode AS lkp_RatingZoneCode,
	LKP_CoverageDetailCommercialAuto.ReplacementCost AS lkp_ReplacementCost,
	LKP_CoverageDetailCommercialAuto.FullGlassIndicator AS lkp_FullGlassIndicator,
	LKP_CoverageDetailCommercialAuto.HistoricVehicleIndicator AS lkp_HistoricVehicleIndicator,
	EXP_CoverageDetailCommercialAuto.o_PremiumTransactionID AS i_PremiumTransactionID,
	EXP_CoverageDetailCommercialAuto.o_CoverageGUID AS i_CoverageGUID,
	EXP_CoverageDetailCommercialAuto.o_VehicleGroupCode AS i_VehicleGroupCode,
	EXP_CoverageDetailCommercialAuto.o_RadiusOfOperation AS i_RadiusOfOperation,
	EXP_CoverageDetailCommercialAuto.o_SecondaryVehicleType AS i_SecondaryVehicleType,
	EXP_CoverageDetailCommercialAuto.o_UsedInDumpingIndicator AS i_UsedInDumpingIndicator,
	EXP_CoverageDetailCommercialAuto.Year AS i_Year,
	EXP_CoverageDetailCommercialAuto.StatedAmount AS i_StatedAmount,
	EXP_CoverageDetailCommercialAuto.VehicleDeleteDate AS i_VehicleDeleteDate,
	EXP_CoverageDetailCommercialAuto.PIPBureaucoverageCode AS i_PIPBureaucoverageCode,
	-- *INF*: SUBSTR('0000' || IIF(ISNULL(i_Year), '0000', TO_CHAR(i_Year)), -4, 4)
	SUBSTR('0000' || IFF(i_Year IS NULL, '0000', TO_CHAR(i_Year)), - 4, 4) AS v_Year,
	-- *INF*: TO_CHAR(IIF(ISNULL(i_StatedAmount), 0, i_StatedAmount))
	TO_CHAR(IFF(i_StatedAmount IS NULL, 0, i_StatedAmount)) AS v_StatedAmount,
	EXP_CoverageDetailCommercialAuto.o_VIN,
	EXP_CoverageDetailCommercialAuto.o_Make,
	EXP_CoverageDetailCommercialAuto.o_Model,
	EXP_CoverageDetailCommercialAuto.o_VehicleNumber,
	EXP_CoverageDetailCommercialAuto.CompositeRating,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	'1' AS o_CurrentSnapshotFlag,
	EXP_CoverageDetailCommercialAuto.o_ZoneTerminal AS o_TerminalZoneCode,
	EXP_CoverageDetailCommercialAuto.o_VehicleType AS VehicleType,
	EXP_CoverageDetailCommercialAuto.o_TotalVehicleCost AS i_TotalVehicleCost,
	i_TotalVehicleCost AS v_CostNew,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('1800-01-01 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.US')
	TO_DATE('1800-01-01 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.US') AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.US')
	TO_DATE('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.US') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_CoverageGUID AS o_CoverageGUID,
	i_VehicleGroupCode AS o_VehicleGroupCode,
	i_RadiusOfOperation AS o_RadiusOfOperation,
	i_SecondaryVehicleType AS o_SecondaryVehicleType,
	i_UsedInDumpingIndicator AS o_UsedInDumpingIndicator,
	v_Year AS o_Year,
	v_StatedAmount AS o_StatedAmount,
	v_CostNew AS o_CostNew,
	i_VehicleDeleteDate AS o_VehicleDeleteDate,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_PremiumTransactionID),
	-- 'NEW',
	-- lkp_VehicleGroupCode = i_VehicleGroupCode AND lkp_RadiusOfOperation =i_RadiusOfOperation AND lkp_SecondaryVehicleType = i_SecondaryVehicleType AND v_UsedInDumpingIndicator = i_UsedInDumpingIndicator AND v_Year = lkp_VehicleYear AND v_StatedAmount  = lkp_StatedAmount
	-- AND lkp_CostNew=v_CostNew
	-- AND lkp_VehicleDeleteDate=i_VehicleDeleteDate
	-- AND lkp_VIN = o_VIN
	-- AND lkp_VehicleMake = o_Make
	-- AND lkp_VehicleModel = o_Model
	-- AND lkp_VehicleNumber = o_VehicleNumber
	-- AND v_CompositeRatedFlag = CompositeRating
	-- AND lkp_TerminalZoneCode=o_TerminalZoneCode
	-- AND lkp_VehicleType=VehicleType
	-- AND lkp_VehicleTypeSize=o_CommercialAutoVehicleType
	-- AND lkp_BusinessUseClass=o_CommercialAutoBusinessUseClass
	-- AND lkp_SecondaryClass=o_SecondaryClass
	-- AND lkp_FleetType=o_FleetType
	-- AND lkp_SecondaryClassGroup=o_SecondaryClassGroup
	-- AND lkp_PIPBureaucoverageCode=i_PIPBureaucoverageCode
	-- AND lkp_IncludeUIM=IncludeUIM
	-- AND lkp_RatingZoneCode=RatingZoneCode
	-- AND lkp_ReplacementCost=ReplacementCost
	-- AND lkp_FullGlassIndicator=FullGlassIndicator
	-- AND lkp_HistoricVehicleIndicator=HistoricVehicleIndicator,
	-- 'NOCHANGE',
	-- 'UPDATE')
	DECODE(TRUE,
		lkp_PremiumTransactionID IS NULL, 'NEW',
		lkp_VehicleGroupCode = i_VehicleGroupCode AND lkp_RadiusOfOperation = i_RadiusOfOperation AND lkp_SecondaryVehicleType = i_SecondaryVehicleType AND v_UsedInDumpingIndicator = i_UsedInDumpingIndicator AND v_Year = lkp_VehicleYear AND v_StatedAmount = lkp_StatedAmount AND lkp_CostNew = v_CostNew AND lkp_VehicleDeleteDate = i_VehicleDeleteDate AND lkp_VIN = o_VIN AND lkp_VehicleMake = o_Make AND lkp_VehicleModel = o_Model AND lkp_VehicleNumber = o_VehicleNumber AND v_CompositeRatedFlag = CompositeRating AND lkp_TerminalZoneCode = o_TerminalZoneCode AND lkp_VehicleType = VehicleType AND lkp_VehicleTypeSize = o_CommercialAutoVehicleType AND lkp_BusinessUseClass = o_CommercialAutoBusinessUseClass AND lkp_SecondaryClass = o_SecondaryClass AND lkp_FleetType = o_FleetType AND lkp_SecondaryClassGroup = o_SecondaryClassGroup AND lkp_PIPBureaucoverageCode = i_PIPBureaucoverageCode AND lkp_IncludeUIM = IncludeUIM AND lkp_RatingZoneCode = RatingZoneCode AND lkp_ReplacementCost = ReplacementCost AND lkp_FullGlassIndicator = FullGlassIndicator AND lkp_HistoricVehicleIndicator = HistoricVehicleIndicator, 'NOCHANGE',
		'UPDATE') AS o_ChangeFlag,
	i_PIPBureaucoverageCode AS o_PIPBureaucoverageCode,
	EXP_CoverageDetailCommercialAuto.o_CommercialAutoVehicleType,
	EXP_CoverageDetailCommercialAuto.o_CommercialAutoBusinessUseClass,
	EXP_CoverageDetailCommercialAuto.o_SecondaryClass,
	EXP_CoverageDetailCommercialAuto.o_FleetType,
	EXP_CoverageDetailCommercialAuto.o_SecondaryClassGroup,
	EXP_CoverageDetailCommercialAuto.RetroactiveDate,
	EXP_CoverageDetailCommercialAuto.IncludeUIM,
	EXP_CoverageDetailCommercialAuto.o_CoordinationOfBenefits,
	EXP_CoverageDetailCommercialAuto.MedicalExpenses,
	EXP_CoverageDetailCommercialAuto.WorkComp,
	EXP_CoverageDetailCommercialAuto.o_SubjectToNoFault,
	EXP_CoverageDetailCommercialAuto.AdditionalLimitKS,
	EXP_CoverageDetailCommercialAuto.AdditionalLimitKY,
	EXP_CoverageDetailCommercialAuto.AdditionalLimitMN,
	EXP_CoverageDetailCommercialAuto.ZoneGaraging AS RatingZoneCode,
	EXP_CoverageDetailCommercialAuto.ReplacementCost,
	EXP_CoverageDetailCommercialAuto.FullGlassIndicator,
	EXP_CoverageDetailCommercialAuto.HistoricVehicleIndicator
	FROM EXP_CoverageDetailCommercialAuto
	LEFT JOIN LKP_CoverageDetailCommercialAuto
	ON LKP_CoverageDetailCommercialAuto.PremiumTransactionID = EXP_CoverageDetailCommercialAuto.o_PremiumTransactionID
),
FIL_Insert AS (
	SELECT
	o_VIN, 
	o_Make, 
	o_Model, 
	o_VehicleNumber, 
	CompositeRating, 
	o_PremiumTransactionID, 
	o_CurrentSnapshotFlag, 
	o_TerminalZoneCode, 
	VehicleType, 
	o_AuditID, 
	o_EffectiveDate, 
	o_ExpirationDate, 
	o_SourceSystemID, 
	o_CreatedDate, 
	o_ModifiedDate, 
	o_CoverageGUID, 
	o_VehicleGroupCode, 
	o_RadiusOfOperation, 
	o_SecondaryVehicleType, 
	o_UsedInDumpingIndicator, 
	o_Year, 
	o_StatedAmount, 
	o_CostNew, 
	o_VehicleDeleteDate, 
	o_ChangeFlag, 
	o_PIPBureaucoverageCode, 
	o_CommercialAutoVehicleType, 
	o_CommercialAutoBusinessUseClass, 
	o_SecondaryClass, 
	o_FleetType, 
	o_SecondaryClassGroup, 
	RetroactiveDate, 
	IncludeUIM, 
	o_CoordinationOfBenefits, 
	MedicalExpenses, 
	WorkComp, 
	o_SubjectToNoFault, 
	AdditionalLimitKS, 
	AdditionalLimitKY, 
	AdditionalLimitMN, 
	RatingZoneCode, 
	ReplacementCost, 
	FullGlassIndicator, 
	HistoricVehicleIndicator
	FROM EXP_DetectChanges
	WHERE o_ChangeFlag = 'NEW'
),
CoverageDetailCommercialAuto_INSERT AS (
	INSERT INTO CoverageDetailCommercialAuto
	(PremiumTransactionID, CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, CoverageGuid, VehicleGroupCode, RadiusOfOperation, SecondaryVehicleType, UsedInDumpingIndicator, VehicleYear, StatedAmount, CostNew, VehicleDeleteDate, VIN, VehicleMake, VehicleModel, VehicleNumber, CompositeRatedFlag, TerminalZoneCode, VehicleType, PIPBureauCoverageCode, RetroactiveDate, VehicleTypeSize, BusinessUseClass, SecondaryClass, FleetType, SecondaryClassGroup, IncludeUIM, CoordinationOfBenefits, MedicalExpensesOption, CoveredByWorkersCompensationFlag, SubjectToNoFault, AdditionalLimitKS, AdditionalLimitKY, AdditionalLimitMN, RatingZoneCode, ReplacementCost, FullGlassIndicator, HistoricVehicleIndicator)
	SELECT 
	o_PremiumTransactionID AS PREMIUMTRANSACTIONID, 
	o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	o_AuditID AS AUDITID, 
	o_EffectiveDate AS EFFECTIVEDATE, 
	o_ExpirationDate AS EXPIRATIONDATE, 
	o_SourceSystemID AS SOURCESYSTEMID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_CoverageGUID AS COVERAGEGUID, 
	o_VehicleGroupCode AS VEHICLEGROUPCODE, 
	o_RadiusOfOperation AS RADIUSOFOPERATION, 
	o_SecondaryVehicleType AS SECONDARYVEHICLETYPE, 
	o_UsedInDumpingIndicator AS USEDINDUMPINGINDICATOR, 
	o_Year AS VEHICLEYEAR, 
	o_StatedAmount AS STATEDAMOUNT, 
	o_CostNew AS COSTNEW, 
	o_VehicleDeleteDate AS VEHICLEDELETEDATE, 
	o_VIN AS VIN, 
	o_Make AS VEHICLEMAKE, 
	o_Model AS VEHICLEMODEL, 
	o_VehicleNumber AS VEHICLENUMBER, 
	CompositeRating AS COMPOSITERATEDFLAG, 
	o_TerminalZoneCode AS TERMINALZONECODE, 
	VEHICLETYPE, 
	o_PIPBureaucoverageCode AS PIPBUREAUCOVERAGECODE, 
	RETROACTIVEDATE, 
	o_CommercialAutoVehicleType AS VEHICLETYPESIZE, 
	o_CommercialAutoBusinessUseClass AS BUSINESSUSECLASS, 
	o_SecondaryClass AS SECONDARYCLASS, 
	o_FleetType AS FLEETTYPE, 
	o_SecondaryClassGroup AS SECONDARYCLASSGROUP, 
	INCLUDEUIM, 
	o_CoordinationOfBenefits AS COORDINATIONOFBENEFITS, 
	MedicalExpenses AS MEDICALEXPENSESOPTION, 
	WorkComp AS COVEREDBYWORKERSCOMPENSATIONFLAG, 
	o_SubjectToNoFault AS SUBJECTTONOFAULT, 
	ADDITIONALLIMITKS, 
	ADDITIONALLIMITKY, 
	ADDITIONALLIMITMN, 
	RATINGZONECODE, 
	REPLACEMENTCOST, 
	FULLGLASSINDICATOR, 
	HISTORICVEHICLEINDICATOR
	FROM FIL_Insert
),
SQ_CoverageDetailCommercialAuto AS (
	SELECT 
	CDCAPrevious.VehicleGroupCode, 
	CDCAPrevious.RadiusOfOperation, 
	CDCAPrevious.SecondaryVehicleType, 
	CDCAPrevious.UsedInDumpingIndicator, 
	CDCAPrevious.VehicleYear, 
	CDCAPrevious.StatedAmount, 
	CDCAPrevious.CostNew, 
	CDCAPrevious.VehicleDeleteDate, 
	CDCAPrevious.VIN, 
	CDCAPrevious.VehicleMake, 
	CDCAPrevious.VehicleModel, 
	CDCAPrevious.VehicleNumber, 
	CDCAPrevious.CompositeRatedFlag, 
	CDCAPrevious.TerminalZoneCode, 
	CDCAPrevious.VehicleType, 
	CDCAPrevious.PIPBureauCoverageCode, 
	CDCAPrevious.VehicleTypeSize, 
	CDCAPrevious.BusinessUseClass, 
	CDCAPrevious.SecondaryClass, 
	CDCAPrevious.FleetType, 
	CDCAPrevious.SecondaryClassGroup,
	CDCAPrevious.RetroactiveDate,
	CDCAPrevious.IncludeUIM,
	CDCAPrevious.CoordinationOfBenefits,
	CDCAPrevious.MedicalExpensesOption,
	CDCAPrevious.CoveredByWorkersCompensationFlag,
	CDCAPrevious.SubjectToNoFault,
	CDCAPrevious.AdditionalLimitKS,
	CDCAPrevious.AdditionalLimitKY,
	CDCAPrevious.AdditionalLimitMN,
	CDCAPrevious.RatingZoneCode,
	CDCAPrevious.ReplacementCost,
	CDCAPrevious.FullGlassIndicator,
	CDCAPrevious.HistoricVehicleIndicator,
	WPTOL.PremiumTransactionID AS Wrk_PremiumTransactionID
	FROM
	WorkPremiumTransactionOffsetLineage WPTOL inner join CoverageDetailCommercialAuto CDCAPrevious
	on ( CDCAPrevious.PremiumTransactionID = WPTOL.PreviousPremiumTransactionID)
	INNER JOIN CoverageDetailCommercialAuto CDCAToUpdate
	on ( CDCAToUpdate.PremiumTransactionID = WPTOL.PremiumTransactionid)
	INNER JOIN premiumtransaction pt WITH (NOLOCK) on
		WPTOL .premiumtransactionid=pt.premiumtransactionid and PT.OffsetOnsetCode='Offset'
	WHERE
	WPTOL.UpdateAttributeFlag = 1 
	AND (
	  CDCAPrevious.VehicleGroupCode <> CDCAToUpdate.VehicleGroupCode
	  OR CDCAPrevious.RadiusOfOperation <> CDCAToUpdate.RadiusOfOperation
	  OR CDCAPrevious.SecondaryVehicleType <> CDCAToUpdate.SecondaryVehicleType
	  OR CDCAPrevious.UsedInDumpingIndicator <> CDCAToUpdate.UsedInDumpingIndicator
	  OR CDCAPrevious.VehicleYear <> CDCAToUpdate.VehicleYear
	  OR CDCAPrevious.StatedAmount <> CDCAToUpdate.StatedAmount
	  OR CDCAPrevious.CostNew <> CDCAToUpdate.CostNew
	  OR CDCAPrevious.VehicleDeleteDate <> CDCAToUpdate.VehicleDeleteDate
	  OR CDCAPrevious.VIN <> CDCAToUpdate.VIN
	  OR CDCAPrevious.VehicleMake <> CDCAToUpdate.VehicleMake
	  OR CDCAPrevious.VehicleModel <> CDCAToUpdate.VehicleModel
	  OR CDCAPrevious.VehicleNumber <> CDCAToUpdate.VehicleNumber
	  OR CDCAPrevious.CompositeRatedFlag <> CDCAToUpdate.CompositeRatedFlag
	  OR CDCAPrevious.TerminalZoneCode <> CDCAToUpdate.TerminalZoneCode
	  OR CDCAPrevious.VehicleType <> CDCAToUpdate.VehicleType
	  OR CDCAPrevious.PIPBureauCoverageCode <> CDCAToUpdate.PIPBureauCoverageCode
	  OR CDCAPrevious.VehicleTypeSize <> CDCAToUpdate.VehicleTypeSize
	  OR CDCAPrevious.BusinessUseClass <> CDCAToUpdate.BusinessUseClass
	  OR CDCAPrevious.SecondaryClass <> CDCAToUpdate.SecondaryClass
	  OR CDCAPrevious.FleetType <> CDCAToUpdate.FleetType
	  OR CDCAPrevious.SecondaryClassGroup <> CDCAToUpdate.SecondaryClassGroup
	  OR CDCAPrevious.RetroactiveDate <> CDCAToUpdate.RetroactiveDate
	  OR CDCAPrevious.IncludeUIM <> CDCAToUpdate.IncludeUIM
	  OR CDCAPrevious.CoordinationOfBenefits <> CDCAToUpdate.CoordinationOfBenefits
	  OR CDCAPrevious.MedicalExpensesOption <>  CDCAToUpdate.MedicalExpensesOption 
	  OR CDCAPrevious.CoveredByWorkersCompensationFlag <> CDCAToUpdate.CoveredByWorkersCompensationFlag
	  OR CDCAPrevious.SubjectToNoFault <> CDCAToUpdate.SubjectToNoFault
	  OR CDCAPrevious.AdditionalLimitKS <> CDCAToUpdate.AdditionalLimitKS
	  OR CDCAPrevious.AdditionalLimitKY <> CDCAToUpdate.AdditionalLimitKY
	  OR CDCAPrevious.AdditionalLimitMN <> CDCAToUpdate.AdditionalLimitMN
	  --added null check to make sure null attributes are being offset with the correct value
	  OR ISNULL(CDCAPrevious.RatingZoneCode, 'N/A') <> CDCAToUpdate.RatingZoneCode
	  OR ISNULL(CDCAPrevious.ReplacementCost, 0) <> CDCAToUpdate.ReplacementCost
	  OR ISNULL(CDCAPrevious.FullGlassIndicator, 0) <> CDCAToUpdate.FullGlassIndicator
	  OR ISNULL(CDCAPrevious.HistoricVehicleIndicator, 0) <> CDCAToUpdate.HistoricVehicleIndicator
	)
),
EXP_Coveragedetailauto AS (
	SELECT
	Wrk_PremiumTransactionID,
	SYSDATE AS o_ModifiedDate,
	VehicleGroupCode,
	RadiusOfOperation,
	SecondaryVehicleType,
	UsedInDumpingIndicator,
	VehicleYear,
	StatedAmount,
	CostNew,
	VehicleDeleteDate,
	VIN,
	VehicleMake,
	VehicleModel,
	VehicleNumber,
	CompositeRatedFlag,
	TerminalZoneCode,
	VehicleType,
	PIPBureaucoverageCode,
	VehicleTypeSize,
	BusinessUseClass,
	SecondaryClass,
	FleetType,
	SecondaryClassGroup,
	RetroactiveDate,
	IncludeUIM,
	CoordinationOfBenefits,
	MedicalExpensesOption,
	CoveredByWorkersCompensationFlag AS CoveredByWorkComp,
	-- *INF*: IIF(ISNULL(CoordinationOfBenefits),'N/A',CoordinationOfBenefits)
	IFF(CoordinationOfBenefits IS NULL, 'N/A', CoordinationOfBenefits) AS o_CoordinationOfBenefits,
	-- *INF*: IIF(ISNULL(MedicalExpensesOption),'N/A',MedicalExpensesOption)
	IFF(MedicalExpensesOption IS NULL, 'N/A', MedicalExpensesOption) AS o_MedicalExpensesOption,
	-- *INF*: IIF(ISNULL(CoveredByWorkComp),0,IIF(CoveredByWorkComp='T',1,0))
	IFF(CoveredByWorkComp IS NULL, 0, IFF(CoveredByWorkComp = 'T', 1, 0)) AS o_CoveredByWorkComp,
	SubjectToNoFault,
	AdditionalLimitKS,
	AdditionalLimitKY,
	AdditionalLimitMN,
	RatingZoneCode,
	-- *INF*: IIF(ISNULL(RatingZoneCode),'N/A',TO_CHAR(RatingZoneCode))
	IFF(RatingZoneCode IS NULL, 'N/A', TO_CHAR(RatingZoneCode)) AS o_RatingZoneCode,
	ReplacementCost,
	-- *INF*: IIF(ISNULL(ReplacementCost),0,IIF(ReplacementCost = 'T',1,0))
	IFF(ReplacementCost IS NULL, 0, IFF(ReplacementCost = 'T', 1, 0)) AS o_ReplacementCost,
	FullGlassIndicator,
	-- *INF*: IIF(ISNULL(FullGlassIndicator),0,IIF(FullGlassIndicator = 'T',1,0))
	IFF(FullGlassIndicator IS NULL, 0, IFF(FullGlassIndicator = 'T', 1, 0)) AS o_FullGlassIndicator,
	HistoricVehicleIndicator,
	-- *INF*: IIF(ISNULL(HistoricVehicleIndicator),0,IIF(HistoricVehicleIndicator= 'T',1,0))
	IFF(HistoricVehicleIndicator IS NULL, 0, IFF(HistoricVehicleIndicator = 'T', 1, 0)) AS o_HistoricVehicleIndicator
	FROM SQ_CoverageDetailCommercialAuto
),
UPD_Coveragedetailcommercialauto AS (
	SELECT
	Wrk_PremiumTransactionID, 
	o_ModifiedDate AS ModifiedDate, 
	VehicleGroupCode, 
	RadiusOfOperation, 
	SecondaryVehicleType, 
	UsedInDumpingIndicator, 
	VehicleYear, 
	StatedAmount, 
	CostNew, 
	VehicleDeleteDate, 
	VIN, 
	VehicleMake, 
	VehicleModel, 
	VehicleNumber, 
	CompositeRatedFlag, 
	TerminalZoneCode, 
	VehicleType, 
	PIPBureaucoverageCode, 
	VehicleTypeSize, 
	BusinessUseClass, 
	SecondaryClass, 
	FleetType, 
	SecondaryClassGroup, 
	RetroactiveDate, 
	IncludeUIM, 
	o_CoordinationOfBenefits AS CoordinationOfBenefits, 
	o_MedicalExpensesOption AS MedicalExpensesOption, 
	o_CoveredByWorkComp AS CoveredByWorkComp, 
	SubjectToNoFault, 
	AdditionalLimitKS, 
	AdditionalLimitKY, 
	AdditionalLimitMN, 
	o_RatingZoneCode AS RatingZoneCode, 
	o_ReplacementCost AS ReplacementCost, 
	o_FullGlassIndicator AS FullGlassIndicator, 
	o_HistoricVehicleIndicator AS HistoricVehicleIndicator
	FROM EXP_Coveragedetailauto
),
TGT_CoverageDetailCommercialAuto_Offset AS (
	MERGE INTO CoverageDetailCommercialAuto AS T
	USING UPD_Coveragedetailcommercialauto AS S
	ON T.PremiumTransactionID = S.Wrk_PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.VehicleGroupCode = S.VehicleGroupCode, T.RadiusOfOperation = S.RadiusOfOperation, T.SecondaryVehicleType = S.SecondaryVehicleType, T.UsedInDumpingIndicator = S.UsedInDumpingIndicator, T.VehicleYear = S.VehicleYear, T.StatedAmount = S.StatedAmount, T.CostNew = S.CostNew, T.VehicleDeleteDate = S.VehicleDeleteDate, T.VIN = S.VIN, T.VehicleMake = S.VehicleMake, T.VehicleModel = S.VehicleModel, T.VehicleNumber = S.VehicleNumber, T.CompositeRatedFlag = S.CompositeRatedFlag, T.TerminalZoneCode = S.TerminalZoneCode, T.VehicleType = S.VehicleType, T.PIPBureauCoverageCode = S.PIPBureaucoverageCode, T.RetroactiveDate = S.RetroactiveDate, T.VehicleTypeSize = S.VehicleTypeSize, T.BusinessUseClass = S.BusinessUseClass, T.SecondaryClass = S.SecondaryClass, T.FleetType = S.FleetType, T.SecondaryClassGroup = S.SecondaryClassGroup, T.IncludeUIM = S.IncludeUIM, T.CoordinationOfBenefits = S.CoordinationOfBenefits, T.MedicalExpensesOption = S.MedicalExpensesOption, T.CoveredByWorkersCompensationFlag = S.CoveredByWorkComp, T.SubjectToNoFault = S.SubjectToNoFault, T.AdditionalLimitKS = S.AdditionalLimitKS, T.AdditionalLimitKY = S.AdditionalLimitKY, T.AdditionalLimitMN = S.AdditionalLimitMN, T.RatingZoneCode = S.RatingZoneCode, T.ReplacementCost = S.ReplacementCost, T.FullGlassIndicator = S.FullGlassIndicator, T.HistoricVehicleIndicator = S.HistoricVehicleIndicator
),
SQ_CoverageDetailCommercialAuto_Deprecated AS (
	SELECT 
	CDCAPrevious.VehicleGroupCode, 
	CDCAPrevious.RadiusOfOperation, 
	CDCAPrevious.SecondaryVehicleType, 
	CDCAPrevious.UsedInDumpingIndicator, 
	CDCAPrevious.VehicleYear, 
	CDCAPrevious.StatedAmount, 
	CDCAPrevious.CostNew, 
	CDCAPrevious.VehicleDeleteDate, 
	CDCAPrevious.VIN, 
	CDCAPrevious.VehicleMake, 
	CDCAPrevious.VehicleModel, 
	CDCAPrevious.VehicleNumber, 
	CDCAPrevious.CompositeRatedFlag, 
	CDCAPrevious.TerminalZoneCode, 
	CDCAPrevious.VehicleType, 
	CDCAPrevious.PIPBureauCoverageCode, 
	CDCAPrevious.VehicleTypeSize, 
	CDCAPrevious.BusinessUseClass, 
	CDCAPrevious.SecondaryClass, 
	CDCAPrevious.FleetType, 
	CDCAPrevious.SecondaryClassGroup,
	CDCAPrevious.RetroactiveDate,
	CDCAPrevious.IncludeUIM,
	CDCAPrevious.CoordinationOfBenefits, 
	CDCAPrevious.MedicalExpensesOption, 
	CDCAPrevious.CoveredByWorkersCompensationFlag, 
	CDCAPrevious.SubjectToNoFault,
	CDCAPrevious.AdditionalLimitKS,
	CDCAPrevious.AdditionalLimitKY,
	CDCAPrevious.AdditionalLimitMN,
	CDCAPrevious.RatingZoneCode,
	CDCAPrevious.ReplacementCost,
	CDCAPrevious.FullGlassIndicator,
	CDCAPrevious.HistoricVehicleIndicator,
	WPTOL.PremiumTransactionID AS Wrk_PremiumTransactionID
	FROM
	WorkPremiumTransactionOffsetLineage WPTOL inner join CoverageDetailCommercialAuto CDCAPrevious
	on ( CDCAPrevious.PremiumTransactionID = WPTOL.PreviousPremiumTransactionID)
	inner join CoverageDetailCommercialAuto CDCAToUpdate
	on ( CDCAToUpdate.PremiumTransactionID = WPTOL.PremiumTransactionid)
	INNER JOIN premiumtransaction pt WITH (NOLOCK) on
		WPTOL .premiumtransactionid=pt.premiumtransactionid and PT.OffsetOnsetCode='Deprecated'
	WHERE
	WPTOL.UpdateAttributeFlag = 1 
	AND (
	  CDCAPrevious.VehicleGroupCode <> CDCAToUpdate.VehicleGroupCode
	  OR CDCAPrevious.RadiusOfOperation <> CDCAToUpdate.RadiusOfOperation
	  OR CDCAPrevious.SecondaryVehicleType <> CDCAToUpdate.SecondaryVehicleType
	  OR CDCAPrevious.UsedInDumpingIndicator <> CDCAToUpdate.UsedInDumpingIndicator
	  OR CDCAPrevious.VehicleYear <> CDCAToUpdate.VehicleYear
	  OR CDCAPrevious.StatedAmount <> CDCAToUpdate.StatedAmount
	  OR CDCAPrevious.CostNew <> CDCAToUpdate.CostNew
	  OR CDCAPrevious.VehicleDeleteDate <> CDCAToUpdate.VehicleDeleteDate
	  OR CDCAPrevious.VIN <> CDCAToUpdate.VIN
	  OR CDCAPrevious.VehicleMake <> CDCAToUpdate.VehicleMake
	  OR CDCAPrevious.VehicleModel <> CDCAToUpdate.VehicleModel
	  OR CDCAPrevious.VehicleNumber <> CDCAToUpdate.VehicleNumber
	  OR CDCAPrevious.CompositeRatedFlag <> CDCAToUpdate.CompositeRatedFlag
	  OR CDCAPrevious.TerminalZoneCode <> CDCAToUpdate.TerminalZoneCode
	  OR CDCAPrevious.VehicleType <> CDCAToUpdate.VehicleType
	  OR CDCAPrevious.PIPBureauCoverageCode <> CDCAToUpdate.PIPBureauCoverageCode
	  OR CDCAPrevious.VehicleTypeSize <> CDCAToUpdate.VehicleTypeSize
	  OR CDCAPrevious.BusinessUseClass <> CDCAToUpdate.BusinessUseClass
	  OR CDCAPrevious.SecondaryClass <> CDCAToUpdate.SecondaryClass
	  OR CDCAPrevious.FleetType <> CDCAToUpdate.FleetType
	  OR CDCAPrevious.SecondaryClassGroup <> CDCAToUpdate.SecondaryClassGroup
	  OR CDCAPrevious.RetroactiveDate <> CDCAToUpdate.RetroactiveDate
	  OR CDCAPrevious.IncludeUIM <> CDCAToUpdate.IncludeUIM
	  OR CDCAPrevious.CoordinationOfBenefits <> CDCAToUpdate.CoordinationOfBenefits
	  OR CDCAPrevious.MedicalExpensesOption <>  CDCAToUpdate.MedicalExpensesOption 
	  OR CDCAPrevious.CoveredByWorkersCompensationFlag <> CDCAToUpdate.CoveredByWorkersCompensationFlag
	  OR CDCAPrevious.SubjectToNoFault <> CDCAToUpdate.SubjectToNoFault
	  OR CDCAPrevious.AdditionalLimitKS <> CDCAToUpdate.AdditionalLimitKS
	  OR CDCAPrevious.AdditionalLimitKY <> CDCAToUpdate.AdditionalLimitKY
	  OR CDCAPrevious.AdditionalLimitMN <> CDCAToUpdate.AdditionalLimitMN
	  --added null check to make sure attribute is being offset with the correct value
	  OR ISNULL(CDCAPrevious.RatingZoneCode, 'N/A') <> CDCAToUpdate.RatingZoneCode
	  OR ISNULL(CDCAPrevious.ReplacementCost, 0) <> CDCAToUpdate.ReplacementCost
	  OR ISNULL(CDCAPrevious.FullGlassIndicator, 0) <> CDCAToUpdate.FullGlassIndicator
	  OR ISNULL(CDCAPrevious.HistoricVehicleIndicator, 0) <> CDCAToUpdate.HistoricVehicleIndicator
	  )
),
EXP_Coveragedetailauto_Deprecated AS (
	SELECT
	Wrk_PremiumTransactionID,
	SYSDATE AS o_ModifiedDate,
	VehicleGroupCode,
	RadiusOfOperation,
	SecondaryVehicleType,
	UsedInDumpingIndicator,
	VehicleYear,
	StatedAmount,
	CostNew,
	VehicleDeleteDate,
	VIN,
	VehicleMake,
	VehicleModel,
	VehicleNumber,
	CompositeRatedFlag,
	TerminalZoneCode,
	VehicleType,
	PIPBureaucoverageCode,
	VehicleTypeSize,
	BusinessUseClass,
	SecondaryClass,
	FleetType,
	SecondaryClassGroup,
	RetroactiveDate,
	IncludeUIM,
	CoordinationOfBenefits,
	MedicalExpensesOption,
	CoveredByWorkersCompensationFlag,
	-- *INF*: IIF(ISNULL(CoordinationOfBenefits),'N/A',CoordinationOfBenefits)
	IFF(CoordinationOfBenefits IS NULL, 'N/A', CoordinationOfBenefits) AS o_CoordinationOfBenefits,
	-- *INF*: IIF(ISNULL(MedicalExpensesOption),'N/A',MedicalExpensesOption)
	IFF(MedicalExpensesOption IS NULL, 'N/A', MedicalExpensesOption) AS o_MedicalExpensesOption,
	-- *INF*: IIF(ISNULL(CoveredByWorkersCompensationFlag),0,IIF(CoveredByWorkersCompensationFlag='T',1,0))
	IFF(CoveredByWorkersCompensationFlag IS NULL, 0, IFF(CoveredByWorkersCompensationFlag = 'T', 1, 0)) AS o_CoveredByWorkersCompensationFlag,
	SubjectToNoFault,
	AdditionalLimitKS,
	AdditionalLimitKY,
	AdditionalLimitMN,
	RatingZoneCode,
	-- *INF*: IIF(ISNULL(RatingZoneCode),'N/A',TO_CHAR(RatingZoneCode))
	IFF(RatingZoneCode IS NULL, 'N/A', TO_CHAR(RatingZoneCode)) AS o_RatingZoneCode,
	ReplacementCost,
	-- *INF*: IIF(ISNULL(ReplacementCost),0,IIF(ReplacementCost = 'T',1,0))
	IFF(ReplacementCost IS NULL, 0, IFF(ReplacementCost = 'T', 1, 0)) AS o_ReplacementCost,
	FullGlassIndicator,
	-- *INF*: IIF(ISNULL(FullGlassIndicator),0,IIF(FullGlassIndicator = 'T',1,0))
	IFF(FullGlassIndicator IS NULL, 0, IFF(FullGlassIndicator = 'T', 1, 0)) AS o_FullGlassIndicator,
	HistoricVehicleIndicator,
	-- *INF*: IIF(ISNULL(HistoricVehicleIndicator),0,IIF(HistoricVehicleIndicator= 'T',1,0))
	IFF(HistoricVehicleIndicator IS NULL, 0, IFF(HistoricVehicleIndicator = 'T', 1, 0)) AS o_HistoricVehicleIndicator
	FROM SQ_CoverageDetailCommercialAuto_Deprecated
),
UPD_Coveragedetailcommercialauto_Deprecated AS (
	SELECT
	Wrk_PremiumTransactionID, 
	o_ModifiedDate AS ModifiedDate, 
	VehicleGroupCode, 
	RadiusOfOperation, 
	SecondaryVehicleType, 
	UsedInDumpingIndicator, 
	VehicleYear, 
	StatedAmount, 
	CostNew, 
	VehicleDeleteDate, 
	VIN, 
	VehicleMake, 
	VehicleModel, 
	VehicleNumber, 
	CompositeRatedFlag, 
	TerminalZoneCode, 
	VehicleType, 
	PIPBureaucoverageCode, 
	VehicleTypeSize, 
	BusinessUseClass, 
	SecondaryClass, 
	FleetType, 
	SecondaryClassGroup, 
	RetroactiveDate, 
	IncludeUIM, 
	o_CoordinationOfBenefits AS CoordinationOfBenefits, 
	o_MedicalExpensesOption AS MedicalExpensesOption, 
	o_CoveredByWorkersCompensationFlag AS CoveredByWorkersCompensationFlag, 
	SubjectToNoFault, 
	AdditionalLimitKS, 
	AdditionalLimitKY, 
	AdditionalLimitMN, 
	o_RatingZoneCode AS RatingZoneCode, 
	o_ReplacementCost AS ReplacementCost, 
	o_FullGlassIndicator AS FullGlassIndicator, 
	o_HistoricVehicleIndicator AS HistoricVehicleIndicator
	FROM EXP_Coveragedetailauto_Deprecated
),
TGT_CoverageDetailCommercialAuto_Deprecated AS (
	MERGE INTO CoverageDetailCommercialAuto AS T
	USING UPD_Coveragedetailcommercialauto_Deprecated AS S
	ON T.PremiumTransactionID = S.Wrk_PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.VehicleGroupCode = S.VehicleGroupCode, T.RadiusOfOperation = S.RadiusOfOperation, T.SecondaryVehicleType = S.SecondaryVehicleType, T.UsedInDumpingIndicator = S.UsedInDumpingIndicator, T.VehicleYear = S.VehicleYear, T.StatedAmount = S.StatedAmount, T.CostNew = S.CostNew, T.VehicleDeleteDate = S.VehicleDeleteDate, T.VIN = S.VIN, T.VehicleMake = S.VehicleMake, T.VehicleModel = S.VehicleModel, T.VehicleNumber = S.VehicleNumber, T.CompositeRatedFlag = S.CompositeRatedFlag, T.TerminalZoneCode = S.TerminalZoneCode, T.VehicleType = S.VehicleType, T.PIPBureauCoverageCode = S.PIPBureaucoverageCode, T.RetroactiveDate = S.RetroactiveDate, T.VehicleTypeSize = S.VehicleTypeSize, T.BusinessUseClass = S.BusinessUseClass, T.SecondaryClass = S.SecondaryClass, T.FleetType = S.FleetType, T.SecondaryClassGroup = S.SecondaryClassGroup, T.IncludeUIM = S.IncludeUIM, T.CoordinationOfBenefits = S.CoordinationOfBenefits, T.MedicalExpensesOption = S.MedicalExpensesOption, T.CoveredByWorkersCompensationFlag = S.CoveredByWorkersCompensationFlag, T.SubjectToNoFault = S.SubjectToNoFault, T.AdditionalLimitKS = S.AdditionalLimitKS, T.AdditionalLimitKY = S.AdditionalLimitKY, T.AdditionalLimitMN = S.AdditionalLimitMN, T.RatingZoneCode = S.RatingZoneCode, T.ReplacementCost = S.ReplacementCost, T.FullGlassIndicator = S.FullGlassIndicator, T.HistoricVehicleIndicator = S.HistoricVehicleIndicator
),
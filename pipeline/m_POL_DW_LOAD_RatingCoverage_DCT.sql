WITH
LKP_DCClassCodeStaging_WC AS (
	SELECT
	Value,
	ObjectId,
	SessionId
	FROM (
		SELECT 
			Value,
			ObjectId,
			SessionId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCClassCodeStaging
		WHERE ObjectName='DC_WC_Risk'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ObjectId,SessionId ORDER BY Value) = 1
),
LKP_DCStatCodeStaging_CA AS (
	SELECT
	Value,
	ObjectId,
	SessionId
	FROM (
		SELECT 
			Value,
			ObjectId,
			SessionId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCStatCodeStaging
		WHERE ObjectName='DC_Coverage' and Type='Class' and Value <> '9999'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ObjectId,SessionId ORDER BY Value) = 1
),
LKP_DCClassCodeStaging_CA_Risk AS (
	SELECT
	Value,
	ObjectId,
	SessionId
	FROM (
		select CLS.ObjectId as ObjectId, 
		CLS.SessionId as SessionId, 
		CLS.Value as Value
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCClassCodeStaging CLS
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCARiskStaging RSK
		on RSK.CA_RiskId = CLS.ObjectId
		and CLS.ObjectName = 'DC_CA_Risk'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ObjectId,SessionId ORDER BY Value) = 1
),
LKP_DCClassCodeStaging_CA_HiredAndBorrow AS (
	SELECT
	Value,
	ObjectId,
	SessionId
	FROM (
		select ST.CA_StateId as ObjectId,
		CLS.SessionId as SessionId,
		CLS.Value as Value
		from  @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCClassCodeStaging CLS
		inner join  @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCAHiredAndBorrowStage HB
		on CLS.ObjectId = HB.CA_HiredAndBorrowId 
		and CLS.ObjectName = 'DC_CA_HiredAndBorrow'
		inner join  @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCAStateStaging ST
		on ST.CA_StateId = HB.CA_StateId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ObjectId,SessionId ORDER BY Value) = 1
),
LKP_DCClassCodeStaging_CA_DriveOtherCar AS (
	SELECT
	Value,
	ObjectId,
	SessionId
	FROM (
		select ST.CA_StateId as ObjectId,
		CLS.SessionId as SessionId,
		CLS.Value as Value
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCClassCodeStaging CLS
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCADriveOtherCarStage DOC
		on CLS.ObjectId = DOC.CA_DriveOtherCarId 
		and CLS.ObjectName = 'DC_CA_DriveOtherCar'
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCAStateStaging ST
		on ST.CA_StateId = DOC.CA_StateId
		and ST.DriveOtherCarCoverage = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ObjectId,SessionId ORDER BY Value) = 1
),
LKP_DCClassCodeStaging_CA_NonOwned AS (
	SELECT
	Value,
	ObjectId,
	SessionId
	FROM (
		select L.LineId as ObjectId,
		CLS.SessionId as SessionId,
		CLS.Value as Value
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCClassCodeStaging CLS
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCANonOwnedStage CNO
		on CLS.ObjectId = CNO.CA_NonOwnedId 
		and CLS.ObjectName = 'DC_CA_NonOwned'
		and CLS.Type='Risk'
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLineStaging L
		on L.LineId = CNO.LineId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ObjectId,SessionId ORDER BY Value) = 1
),
LKP_DCStatCodeStaging AS (
	SELECT
	Value,
	ObjectId,
	SessionId,
	Type
	FROM (
		SELECT 
			Value,
			ObjectId,
			SessionId,
			Type
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCStatCodeStaging
		WHERE ObjectName='DC_Coverage' and Type in ('Class', 'Subline','AnnualStatementLOBCode')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ObjectId,SessionId,Type ORDER BY Value) = 1
),
LKP_DCClassCodeStaging_CA_MotorJunkLicense AS (
	SELECT
	Value,
	ObjectId,
	SessionId
	FROM (
		select ST.CA_StateId as ObjectId,
		CLS.SessionId as SessionId, 
		CLS.Value as Value
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCClassCodeStaging CLS
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCAMotorJunkLicenseStage MJL
		on CLS.ObjectId = MJL.CA_MotorJunkLicenseId
		and CLS.ObjectName = 'DC_CA_MotorJunkLicense'
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCAStateStaging ST
		on ST.CA_StateId = MJL.CA_StateId
		and ST.DriveOtherCarCoverage = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ObjectId,SessionId ORDER BY Value) = 1
),
LKP_DCStatCodeStaging_WC AS (
	SELECT
	Value,
	ObjectId,
	SessionId
	FROM (
		SELECT 
			Value,
			ObjectId,
			SessionId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCStatCodeStaging
		WHERE ObjectName='DC_Coverage' and Type in ('AdmiraltyBalanceToMinimumCharge',
		'AircraftSeatSurcharge',
		'AlcoholDrugFreeWorkplace',
		'AlcoholOrDrugFreeWorkplaceCoalMinePremiumCredit',
		'AlcoholOrDrugFreeWorkplacePremiumCredit',
		'AlcoholOrDrugFreeWorkplaceSubjectPremiumCredit',
		'AtomicEnergyRadiationExposureCharge',
		'BalancetoMinimum',
		'CertifiedRiskManagement',
		'Class',
		'CompulsoryWorkplace',
		'ContractorsCredit',
		'DTEC',
		'EmployeeLeasingStatCode',
		'EmployersLiabilityIncreasedBalanceToMinimumCharge',
		'EmployersLiabilityIncreasedLimits',
		'ExpenseConstant',
		'ExperienceModification',
		'FELAIncreasedLimitsPremium',
		'ForeignVoluntaryCompensationFlatFeeStatCode',
		'ManagedCareFactor',
		'MeritRatingCreditPremium',
		'MeritRatingDebitPremium',
		'MeritRatingStatCode',
		'PremiumDiscount',
		'PremiumDiscountTypeA',
		'PremiumIncentiveForSmallEmployees',
		'PreviouslyInjuredEmployee',
		'Repatriation',
		'SafetyCertificationCreditProgram',
		'ScheduledCreditModification',
		'ScheduledModification',
		'SmallDeductibleCredit',
		'StrikeDutySurcharge',
		'TabularAdjustmentCreditProgram',
		'TabularAdjustmentDebitProgram',
		'TerrorismRiskInsuranceProgramReauthorizationAct',
		'TRIA',
		'VoluntaryCompensationFlatFee',
		'VoluntaryCompensationFlatFeeStatCode',
		'VolunteerAmbulance',
		'VolunteerFirefighter',
		'WaiverOfSubrogation',
		'WorkplaceSafetyIncentiveProgramSubject',
		'AdmiraltyIncreasedLimits',
		'FELAIncreasedLimits',
		'StudentWorkStudy',
		'StudentWorkStudy9447',
		'Aircraft',
		'AtomicEnergyRadiation',
		'EmployersLiabilityIncreasedLimitsBalanceToMinimum',
		'FlexibleRatingAdjustment',
		'MeritRating',
		'NonRatableElements',
		'BalanceToMinimum',
		'NE',
		'AuditNoncomplianceCharge')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ObjectId,SessionId ORDER BY Value) = 1
),
LKP_DCStatCode_SupDCTStatCode AS (
	SELECT
	Value,
	ObjectId,
	LineOfBusiness,
	ObjectKey,
	ObjectValue,
	SessionId,
	ObjectName
	FROM (
		SELECT 
		B.Value as Value,
		B.ObjectId as objectId,
		A.LineOfBusiness as LineOfBusiness,
		A.ObjectKey as ObjectKey,
		A.ObjectValue as ObjectValue,
		B.SessionId as SessionId,
		B.ObjectName as ObjectName
		FROM 
		SupDCTStatCode A 
		INNER JOIN 
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.DCStatCodeStaging B on A.ObjectValue = B.[Type]
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY LineOfBusiness,SessionId,ObjectId,ObjectName,ObjectKey ORDER BY Value) = 1
),
SEQ_RatingCoverageAKID AS (
	CREATE SEQUENCE SEQ_RatingCoverageAKID
	START = 1
	INCREMENT = 1;
),
SQ_RatingCoverage_SRC AS (
	SELECT
		WorkDCTCoverageTransaction.CoverageGUID,
		WorkDCTPolicy.TransactionCreatedDate,
		WorkDCTInsuranceLine.SessionId,
		WorkDCTCoverageTransaction.CoverageId,
		WorkDCTInsuranceLine.LineId,
		WorkDCTCoverageTransaction.CoverageType,
		WorkDCTCoverageTransaction.Premium,
		WorkDCTTransactionInsuranceLineLocationBridge.CoverageRiskType,
		WorkDCTCoverageTransaction.SubCoverageType,
		WorkDCTCoverageTransaction.Change,
		WorkDCTTransactionInsuranceLineLocationBridge.CoverageForm,
		WorkDCTTransactionInsuranceLineLocationBridge.RiskId,
		WorkDCTTransactionInsuranceLineLocationBridge.RiskType,
		WorkDCTTransactionInsuranceLineLocationBridge.Exposure,
		WorkDCTTransactionInsuranceLineLocationBridge.CommissionPercentage,
		WorkDCTInsuranceLine.LineType,
		WorkDCTPolicy.PolicyGUId,
		WorkDCTPolicy.PolicyEffectiveDate,
		WorkDCTPolicy.PolicyExpirationDate,
		WorkDCTPolicy.LineOfBusiness,
		WorkDCTPolicy.TransactionType,
		WorkDCTPolicy.PolicyStatus,
		WorkDCTPolicy.TransactionEffectiveDate,
		WorkDCTPolicy.TransactionExpirationDate,
		WorkDCTPolicy.TransactionCancellationDate,
		WorkDCTLocation.LocationNumber,
		WorkDCTLocation.LocationXmlId,
		WorkDCTLocation.Territory,
		WorkDCTPolicy.PolicyVersion,
		WorkDCTTransactionInsuranceLineLocationBridge.CoverageVersion,
		WorkDCTPolicy.WBProduct,
		WorkDCTPolicy.WBProductType,
		WorkDCTPolicy.Division,
		WorkDCTTransactionInsuranceLineLocationBridge.SpecialClassLevel1,
		WorkDCTTransactionInsuranceLineLocationBridge.BuildingNumber,
		WorkDCTTransactionInsuranceLineLocationBridge.PolicyCoverage,
		WorkDCTTransactionInsuranceLineLocationBridge.PerilGroup,
		WorkDCTCoverageTransaction.CoverageDeleteFlag,
		WorkDCTCoverageTransaction.ParentCoverageObjectId,
		WorkDCTCoverageTransaction.ParentCoverageObjectName,
		WorkDCTTransactionInsuranceLineLocationBridge.OccupancyClassDescription,
		WorkDCTTransactionInsuranceLineLocationBridge.ActiveBuildingFlag,
		WorkDCTPolicy.PolicyNumber,
		WorkDCTLocation.StateProvince
	FROM WorkDCTInsuranceLine
	INNER JOIN WorkDCTLocation
	INNER JOIN WorkDCTPolicy
	INNER JOIN WorkDCTCoverageTransaction
	INNER JOIN WorkDCTTransactionInsuranceLineLocationBridge
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
	and
	WorkDCTPolicy.TransactionType @{pipeline().parameters.EXCLUDE_TTYPE}
),
EXP_PolicyKey AS (
	SELECT
	PolicyNumber AS i_PolicyNumber,
	PolicyVersion AS i_PolicyVersion,
	LocationNumber AS i_LocationNumber,
	Territory AS i_Territory,
	LocationXmlId AS i_LocationXmlId,
	LineType AS i_LineType,
	PolicyEffectiveDate AS i_PolicyEffectiveDate,
	-- *INF*: IIF(ISNULL(i_PolicyNumber) or IS_SPACES(i_PolicyNumber) or LENGTH(i_PolicyNumber)=0, 'N/A', LTRIM(RTRIM(i_PolicyNumber)))
	IFF(i_PolicyNumber IS NULL 
		OR LENGTH(i_PolicyNumber)>0 AND TRIM(i_PolicyNumber)='' 
		OR LENGTH(i_PolicyNumber
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_PolicyNumber
			)
		)
	) AS v_PolicyNumber,
	-- *INF*: IIF(ISNULL(i_PolicyVersion),'00',LPAD(TO_CHAR(i_PolicyVersion),2,'0'))
	IFF(i_PolicyVersion IS NULL,
		'00',
		LPAD(TO_CHAR(i_PolicyVersion
			), 2, '0'
		)
	) AS v_PolicyVersion,
	-- *INF*: IIF(ISNULL(i_LocationNumber) OR IS_SPACES(i_LocationNumber) OR LENGTH(i_LocationNumber)=0,'0000',LPAD(LTRIM(RTRIM(i_LocationNumber)),4,'0'))
	IFF(i_LocationNumber IS NULL 
		OR LENGTH(i_LocationNumber)>0 AND TRIM(i_LocationNumber)='' 
		OR LENGTH(i_LocationNumber
		) = 0,
		'0000',
		LPAD(LTRIM(RTRIM(i_LocationNumber
				)
			), 4, '0'
		)
	) AS v_LocationNumber,
	-- *INF*: IIF(ISNULL(i_Territory) OR IS_SPACES(i_Territory) OR LENGTH(i_Territory)=0,'N/A',LTRIM(RTRIM(i_Territory)))
	IFF(i_Territory IS NULL 
		OR LENGTH(i_Territory)>0 AND TRIM(i_Territory)='' 
		OR LENGTH(i_Territory
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_Territory
			)
		)
	) AS v_Territory,
	-- *INF*: IIF(ISNULL(i_LocationXmlId) OR IS_SPACES(i_LocationXmlId) OR LENGTH(i_LocationXmlId)=0,'N/A',LTRIM(RTRIM(i_LocationXmlId)))
	IFF(i_LocationXmlId IS NULL 
		OR LENGTH(i_LocationXmlId)>0 AND TRIM(i_LocationXmlId)='' 
		OR LENGTH(i_LocationXmlId
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_LocationXmlId
			)
		)
	) AS v_LocationXmlId,
	-- *INF*: rtrim(ltrim(v_PolicyNumber))||rtrim(ltrim(v_PolicyVersion))
	rtrim(ltrim(v_PolicyNumber
		)
	) || rtrim(ltrim(v_PolicyVersion
		)
	) AS o_PolicyKey,
	v_LocationNumber AS o_LocationNumber,
	v_Territory AS o_Territory,
	v_LocationXmlId AS o_LocationXmlId,
	-- *INF*: IIF(ISNULL(i_LineType) OR IS_SPACES(i_LineType) OR LENGTH(i_LineType)=0,'N/A',LTRIM(RTRIM(i_LineType)))
	IFF(i_LineType IS NULL 
		OR LENGTH(i_LineType)>0 AND TRIM(i_LineType)='' 
		OR LENGTH(i_LineType
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_LineType
			)
		)
	) AS o_LineType,
	-- *INF*: IIF(ISNULL(i_PolicyEffectiveDate),TO_DATE('21001231235959' , 'YYYYMMDDHH24MISS'),i_PolicyEffectiveDate)
	IFF(i_PolicyEffectiveDate IS NULL,
		TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'
		),
		i_PolicyEffectiveDate
	) AS o_PEffectiveDate,
	CoverageGUID,
	TransactionCreatedDate
	FROM SQ_RatingCoverage_SRC
),
LKP_Pol_AK_Id AS (
	SELECT
	pol_ak_id,
	Pol_Key
	FROM (
		SELECT 
			pol_ak_id,
			Pol_Key
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy
		WHERE crrnt_snpsht_flag='1' and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}' and exists ( select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT where WCT.PolicyNumber=pol_num and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol_mod)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Pol_Key ORDER BY pol_ak_id) = 1
),
mplt_get_RiskLocation_PolicyCoverage_RatingCoverage_Akids_Hierarchy1 AS (WITH
	Input_Policy AS (
		
	),
	EXP_Get_Value AS (
		SELECT
		PolicyAKID,
		CoverageGuid,
		TransactionCreatedDate,
		-- *INF*: IIF(ISNULL(PolicyAKID),-1,PolicyAKID)
		IFF(PolicyAKID IS NULL,
			- 1,
			PolicyAKID
		) AS o_PolicyAKID
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
		IFF(RatingCoverageAKID_WithDate IS NULL,
			0,
			1
		) AS Flag,
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
		IFF(Flag = 1,
			PolicyAKID_Date,
			PolicyAKID
		) AS v_PolicyAKID,
		-- *INF*: IIF(Flag=1,RiskLocationAKID_Date,RiskLocationAKID)
		IFF(Flag = 1,
			RiskLocationAKID_Date,
			RiskLocationAKID
		) AS v_RiskLocationAKID,
		-- *INF*: iif(Flag=1,PolicyCoverageAKID_Date,PolicyCoverageAKID)
		IFF(Flag = 1,
			PolicyCoverageAKID_Date,
			PolicyCoverageAKID
		) AS v_PolicyCoverageAKID,
		-- *INF*: iif(Flag=1,CoverageGUID_Date,CoverageGUID)
		IFF(Flag = 1,
			CoverageGUID_Date,
			CoverageGUID
		) AS v_CoverageGUID,
		v_CoverageGUID AS o_CoverageGUID,
		-- *INF*: iif(Flag=1,LocationUnitNumber_Date,LocationUnitNumber)
		IFF(Flag = 1,
			LocationUnitNumber_Date,
			LocationUnitNumber
		) AS v_LocationUnitNumber,
		v_RiskLocationAKID AS o_RiskLocationAKID,
		-- *INF*: iif(@{pipeline().parameters.PMSESSIONNAME}='s_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',NULL,RatingCoverageCancellationDate_Date)
		IFF(@{pipeline().parameters.PMSESSIONNAME} = 's_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',
			NULL,
			RatingCoverageCancellationDate_Date
		) AS o_RatingCoverageCancellationDate,
		-- *INF*: iif(@{pipeline().parameters.PMSESSIONNAME}='s_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',NULL,iif(Flag=1,RatingCoverageAKID_WithDate,RatingCoverageAKID))
		IFF(@{pipeline().parameters.PMSESSIONNAME} = 's_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',
			NULL,
			IFF(Flag = 1,
				RatingCoverageAKID_WithDate,
				RatingCoverageAKID
			)
		) AS o_RatingCoverageAKID,
		v_PolicyCoverageAKID AS o_PolicyCoverageAKID,
		-- *INF*: TO_CHAR(v_PolicyAKID) || '~'  || TO_CHAR(v_RiskLocationAKID)  || '~' || TO_CHAR( v_PolicyCoverageAKID)  || '~' || v_CoverageGUID  || '~'  || v_LocationUnitNumber
		TO_CHAR(v_PolicyAKID
		) || '~' || TO_CHAR(v_RiskLocationAKID
		) || '~' || TO_CHAR(v_PolicyCoverageAKID
		) || '~' || v_CoverageGUID || '~' || v_LocationUnitNumber AS o_PremiumTransactionKey,
		-- *INF*: iif(@{pipeline().parameters.PMSESSIONNAME}='s_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',NULL,RatingCoverageKey_Date)
		IFF(@{pipeline().parameters.PMSESSIONNAME} = 's_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',
			NULL,
			RatingCoverageKey_Date
		) AS o_RatingCoverageKey,
		-- *INF*: iif(@{pipeline().parameters.PMSESSIONNAME}='s_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',NULL,RatingCoverageHashKey_Date)
		IFF(@{pipeline().parameters.PMSESSIONNAME} = 's_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',
			NULL,
			RatingCoverageHashKey_Date
		) AS o_RatingCoverageHashKey,
		-- *INF*: iif(@{pipeline().parameters.PMSESSIONNAME}='s_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',NULL,RatingCoverageId_Date)
		-- 
		-- --IIF(Flag=1,RatingCoverageId_Date,RatingCoverageId)
		-- 
		-- 
		-- 
		IFF(@{pipeline().parameters.PMSESSIONNAME} = 's_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',
			NULL,
			RatingCoverageId_Date
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
		IFF(@{pipeline().parameters.PMSESSIONNAME} = 's_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',
			NULL,
			IFF(Flag = 1,
				RatingCoverageEffectivedate_Date,
				RatingCoverageEffectivedate
			)
		) AS o_RatingCoverageEffectivedate,
		-- *INF*: IIF(@{pipeline().parameters.PMSESSIONNAME}='s_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',NULL,
		-- IIF(Flag=1,RatingCoverageExpirationdate_Date,RatingCoverageExpirationdate))
		-- 
		-- 
		-- --IIF(Flag=1,RatingCoverageExpirationdate_Date,RatingCoverageExpirationdate)
		IFF(@{pipeline().parameters.PMSESSIONNAME} = 's_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',
			NULL,
			IFF(Flag = 1,
				RatingCoverageExpirationdate_Date,
				RatingCoverageExpirationdate
			)
		) AS o_RatingCoverageExpirationdate,
		-- *INF*: IIF(@{pipeline().parameters.PMSESSIONNAME}='s_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',NULL,
		-- IIF(Flag=1,i_CoverageType_Date,i_CoverageType))
		IFF(@{pipeline().parameters.PMSESSIONNAME} = 's_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',
			NULL,
			IFF(Flag = 1,
				i_CoverageType_Date,
				i_CoverageType
			)
		) AS o_CoverageType,
		-- *INF*: IIF(@{pipeline().parameters.PMSESSIONNAME}='s_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',NULL,
		-- IIF(Flag=1,i_ProductAbbreviation_Date,i_ProductAbbreviation))
		IFF(@{pipeline().parameters.PMSESSIONNAME} = 's_m_POL_DW_LOAD_RatingCoverage_Restate_DCT',
			NULL,
			IFF(Flag = 1,
				i_ProductAbbreviation_Date,
				i_ProductAbbreviation
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
EXP_RiskLocationKey AS (
	SELECT
	LKP_Pol_AK_Id.pol_ak_id AS i_pol_ak_id,
	EXP_PolicyKey.o_LocationNumber AS LocationNumber,
	EXP_PolicyKey.o_Territory AS Territory,
	EXP_PolicyKey.o_LocationXmlId AS i_LocationXmlId,
	EXP_PolicyKey.o_LineType AS i_LineType,
	SQ_RatingCoverage_SRC.StateProvince AS i_StateProvince,
	SQ_RatingCoverage_SRC.ParentCoverageObjectName AS i_ParentCoverageObjectName,
	SQ_RatingCoverage_SRC.TransactionCreatedDate AS i_TransactionCreatedDate,
	EXP_PolicyKey.CoverageGUID,
	-- *INF*: IIF(ISNULL(i_LineType) or IS_SPACES(i_LineType) or LENGTH(i_LineType)=0,'N/A',LTRIM(RTRIM(i_LineType)))
	IFF(i_LineType IS NULL 
		OR LENGTH(i_LineType)>0 AND TRIM(i_LineType)='' 
		OR LENGTH(i_LineType
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_LineType
			)
		)
	) AS v_LineType,
	-- *INF*: IIF(ISNULL(i_pol_ak_id),-1,i_pol_ak_id)
	IFF(i_pol_ak_id IS NULL,
		- 1,
		i_pol_ak_id
	) AS v_Pol_AK_ID,
	v_Pol_AK_ID AS o_Pol_AK_ID,
	-- *INF*: DECODE (TRUE,
	-- in (i_ParentCoverageObjectName,  'DC_BP_Location','DC_BP_Risk','DC_CA_Risk','DC_CF_Risk','DC_GL_Risk','DC_IM_Risk','DC_WC_Risk','DCBPLocation','WB_GOC_Risk','WB_HIO_Risk','WB_EC_Risk')=1,
	-- v_Pol_AK_ID||'~'||i_LocationXmlId,
	-- IN(i_ParentCoverageObjectName,'DC_CA_State','DC_WC_StateTerm','WB_GOC_State','WB_HIO_State','WB_IM_State','WB_EC_State','DC_WC_State')=1,
	-- v_Pol_AK_ID||'~'||'PrimaryLocation'||'~'||i_StateProvince||'~'||v_LineType ,
	-- IN(i_ParentCoverageObjectName,'DC_CR_Endorsement','DC_CR_Risk','DC_CR_RiskCrime','DC_Line','DC_CU_UmbrellaEmployersLiability','DC_IM_CoverageForm','WB_CU_PremiumDetail','DC_CA_BusinessInterruptionOption')=1,
	-- v_Pol_AK_ID||'~'||'PrimaryLocation'||'~'||v_LineType
	-- )
	DECODE(TRUE,
		i_ParentCoverageObjectName IN ('DC_BP_Location','DC_BP_Risk','DC_CA_Risk','DC_CF_Risk','DC_GL_Risk','DC_IM_Risk','DC_WC_Risk','DCBPLocation','WB_GOC_Risk','WB_HIO_Risk','WB_EC_Risk') = 1, v_Pol_AK_ID || '~' || i_LocationXmlId,
		i_ParentCoverageObjectName IN ('DC_CA_State','DC_WC_StateTerm','WB_GOC_State','WB_HIO_State','WB_IM_State','WB_EC_State','DC_WC_State') = 1, v_Pol_AK_ID || '~' || 'PrimaryLocation' || '~' || i_StateProvince || '~' || v_LineType,
		i_ParentCoverageObjectName IN ('DC_CR_Endorsement','DC_CR_Risk','DC_CR_RiskCrime','DC_Line','DC_CU_UmbrellaEmployersLiability','DC_IM_CoverageForm','WB_CU_PremiumDetail','DC_CA_BusinessInterruptionOption') = 1, v_Pol_AK_ID || '~' || 'PrimaryLocation' || '~' || v_LineType
	) AS o_RiskLocationKey,
	RiskLocationAKID AS o_RatingCoverage_RiskLocationAKID,
	mplt_get_RiskLocation_PolicyCoverage_RatingCoverage_Akids_Hierarchy1.RiskLocationAKID,
	mplt_get_RiskLocation_PolicyCoverage_RatingCoverage_Akids_Hierarchy1.RatingCoverageAKID,
	mplt_get_RiskLocation_PolicyCoverage_RatingCoverage_Akids_Hierarchy1.RatingCoverageHashKey,
	mplt_get_RiskLocation_PolicyCoverage_RatingCoverage_Akids_Hierarchy1.RatingCoverageId,
	mplt_get_RiskLocation_PolicyCoverage_RatingCoverage_Akids_Hierarchy1.PolicyCoverageAKID,
	mplt_get_RiskLocation_PolicyCoverage_RatingCoverage_Akids_Hierarchy1.RatingCoverageEffectivedate,
	mplt_get_RiskLocation_PolicyCoverage_RatingCoverage_Akids_Hierarchy1.RatingCoverageExpirationdate
	FROM EXP_PolicyKey
	 -- Manually join with SQ_RatingCoverage_SRC
	 -- Manually join with mplt_get_RiskLocation_PolicyCoverage_RatingCoverage_Akids_Hierarchy1
	LEFT JOIN LKP_Pol_AK_Id
	ON LKP_Pol_AK_Id.Pol_Key = EXP_PolicyKey.o_PolicyKey
),
Mplt_RiskLocationAKID_Population AS (WITH
	LKP_RiskLocation_RiskLocationKey AS (
		SELECT
		RiskLocationAKID,
		RiskLocationKey
		FROM (
			SELECT 
				RiskLocationAKID,
				RiskLocationKey
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation
			WHERE CurrentSnapshotFlag = 1 and SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and 
			exists
			 ( select 1 from @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol inner join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT 
			 on WCT.PolicyNumber=pol.pol_num and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol.pol_mod and pol.crrnt_snpsht_flag=1 and PolicyAKID=pol.Pol_AK_ID ) Order By EffectiveDate DESC,CreatedDate DESC,RiskLocationAKID desc--
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationKey ORDER BY RiskLocationAKID) = 1
	),
	LKP_RiskLocation_RiskLocationKey_LocNum_Territory AS (
		SELECT
		RiskLocationAKID,
		RiskLocationKey,
		LocationUnitNumber,
		RiskTerritory
		FROM (
			SELECT 
				RiskLocationAKID,
				RiskLocationKey,
				LocationUnitNumber,
				RiskTerritory
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation
			WHERE CurrentSnapshotFlag = 1 and SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and 
			exists
			 ( select 1 from @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol inner join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT 
			 on WCT.PolicyNumber=pol.pol_num and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol.pol_mod and pol.crrnt_snpsht_flag=1 and PolicyAKID=pol.Pol_AK_ID ) Order By EffectiveDate DESC,CreatedDate DESC,RiskLocationAKID desc--
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationKey,LocationUnitNumber,RiskTerritory ORDER BY RiskLocationAKID) = 1
	),
	LKP_RiskLocation_RiskLocationKey_LocNum AS (
		SELECT
		RiskLocationAKID,
		RiskLocationKey,
		LocationUnitNumber
		FROM (
			SELECT 
				RiskLocationAKID,
				RiskLocationKey,
				LocationUnitNumber
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation
			WHERE CurrentSnapshotFlag = 1 and SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and 
			exists
			 ( select 1 from @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol inner join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT 
			 on WCT.PolicyNumber=pol.pol_num and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol.pol_mod and pol.crrnt_snpsht_flag=1 and PolicyAKID=pol.Pol_AK_ID ) Order By EffectiveDate DESC,CreatedDate DESC,RiskLocationAKID desc--
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationKey,LocationUnitNumber ORDER BY RiskLocationAKID) = 1
	),
	Source_Input AS (
		
	),
	Exp_RiskLocationAKID_population AS (
		SELECT
		RatingCoverage_RiskLocationAKID AS i_RatingCoverage_RiskLocationAKID,
		RiskLocationKey AS i_RiskLocationKey,
		LocationNumber AS i_LocationNumber,
		Territory AS i_Territory,
		-- *INF*: IIF(ISNULL(i_RatingCoverage_RiskLocationAKID),:LKP.LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY(i_RiskLocationKey,i_LocationNumber,i_Territory),i_RatingCoverage_RiskLocationAKID)
		IFF(i_RatingCoverage_RiskLocationAKID IS NULL,
			LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY_i_RiskLocationKey_i_LocationNumber_i_Territory.RiskLocationAKID,
			i_RatingCoverage_RiskLocationAKID
		) AS v_RiskLocationAKID_RiskKey_Location_Territory,
		-- *INF*: IIF(ISNULL(v_RiskLocationAKID_RiskKey_Location_Territory),:LKP.LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM(i_RiskLocationKey,i_LocationNumber),v_RiskLocationAKID_RiskKey_Location_Territory)
		IFF(v_RiskLocationAKID_RiskKey_Location_Territory IS NULL,
			LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_i_RiskLocationKey_i_LocationNumber.RiskLocationAKID,
			v_RiskLocationAKID_RiskKey_Location_Territory
		) AS v_RiskLocationAKID_RiskKey_Location,
		-- *INF*: IIF(ISNULL(v_RiskLocationAKID_RiskKey_Location),:LKP.LKP_RISKLOCATION_RISKLOCATIONKEY(i_RiskLocationKey),v_RiskLocationAKID_RiskKey_Location)
		IFF(v_RiskLocationAKID_RiskKey_Location IS NULL,
			LKP_RISKLOCATION_RISKLOCATIONKEY_i_RiskLocationKey.RiskLocationAKID,
			v_RiskLocationAKID_RiskKey_Location
		) AS v_RiskLocationAKID_RiskKey,
		-- *INF*: iif(isnull(v_RiskLocationAKID_RiskKey),-1,v_RiskLocationAKID_RiskKey)
		IFF(v_RiskLocationAKID_RiskKey IS NULL,
			- 1,
			v_RiskLocationAKID_RiskKey
		) AS o_RiskLocationAKID
		FROM Source_Input
		LEFT JOIN LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY_i_RiskLocationKey_i_LocationNumber_i_Territory
		ON LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY_i_RiskLocationKey_i_LocationNumber_i_Territory.RiskLocationKey = i_RiskLocationKey
		AND LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY_i_RiskLocationKey_i_LocationNumber_i_Territory.LocationUnitNumber = i_LocationNumber
		AND LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY_i_RiskLocationKey_i_LocationNumber_i_Territory.RiskTerritory = i_Territory
	
		LEFT JOIN LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_i_RiskLocationKey_i_LocationNumber
		ON LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_i_RiskLocationKey_i_LocationNumber.RiskLocationKey = i_RiskLocationKey
		AND LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_i_RiskLocationKey_i_LocationNumber.LocationUnitNumber = i_LocationNumber
	
		LEFT JOIN LKP_RISKLOCATION_RISKLOCATIONKEY LKP_RISKLOCATION_RISKLOCATIONKEY_i_RiskLocationKey
		ON LKP_RISKLOCATION_RISKLOCATIONKEY_i_RiskLocationKey.RiskLocationKey = i_RiskLocationKey
	
	),
	RiskLocationAKID AS (
		SELECT
		o_RiskLocationAKID
		FROM Exp_RiskLocationAKID_population
	),
),
EXP_PolicyCoverageAKID AS (
	SELECT
	EXP_PolicyKey.o_LineType AS i_LineType,
	EXP_PolicyKey.o_PEffectiveDate AS i_PEffectiveDate,
	EXP_RiskLocationKey.o_Pol_AK_ID AS pol_ak_id,
	Mplt_RiskLocationAKID_Population.o_RiskLocationAKID AS RiskLocationAKID,
	-- *INF*: IIF(ISNULL(RiskLocationAKID),-1,RiskLocationAKID)
	IFF(RiskLocationAKID IS NULL,
		- 1,
		RiskLocationAKID
	) AS v_RiskLocationAKID,
	-- *INF*: MD5(TO_CHAR(pol_ak_id) || 
	-- TO_CHAR(v_RiskLocationAKID) || 
	-- i_LineType || TO_CHAR(
	-- i_PEffectiveDate))
	MD5(TO_CHAR(pol_ak_id
		) || TO_CHAR(v_RiskLocationAKID
		) || i_LineType || TO_CHAR(i_PEffectiveDate
		)
	) AS o_PolicyCoverageHashKey
	FROM EXP_PolicyKey
	 -- Manually join with EXP_RiskLocationKey
	 -- Manually join with Mplt_RiskLocationAKID_Population
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
LKP_PolicyCoverageAKID AS (
	SELECT
	PolicyCoverageAKID,
	PolicyCoverageHashKey
	FROM (
		SELECT a.PolicyCoverageAKID as PolicyCoverageAKID, a.PolicyCoverageHashKey as PolicyCoverageHashKey FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage a
		INNER JOIN 
		@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy b
		on b.pol_ak_id=a.PolicyAKId
		and b.crrnt_snpsht_flag=1
		INNER JOIN 
		(select distinct WCT.PolicyNumber,ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00') as PolicyVersionFormatted from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT) WCT
		on WCT.PolicyNumber=b.pol_num
		and PolicyVersionFormatted=b.pol_mod
		where a.CurrentSnapshotFlag=1 and a.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		order by a.PolicyCoverageHashKey
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyCoverageHashKey ORDER BY PolicyCoverageAKID) = 1
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
		INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCoverageStage WBC
		ON CT.WB_CoverageId=WBC.WBCoverageId
		ORDER BY WBC.CoverageId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY PeriodStartDate) = 1
),
EXP_GetValues AS (
	SELECT
	LKP_WBWCCoverageTermStage.PeriodStartDate AS lkp_PeriodStartDate,
	LKP_WBWCCoverageTermStage.PeriodEndDate AS lkp_PeriodEndDate,
	LKP_DCWCStateTermStaging.PeriodStartDate AS i_PeriodStartDate,
	LKP_DCWCStateTermStaging.PeriodEndDate AS i_PeriodEndDate,
	SQ_RatingCoverage_SRC.SessionId AS i_SessionId,
	SQ_RatingCoverage_SRC.CoverageId AS i_CoverageId,
	SQ_RatingCoverage_SRC.LineId AS i_LineId,
	SQ_RatingCoverage_SRC.CoverageGUID AS i_CoverageGUID,
	SQ_RatingCoverage_SRC.CoverageType AS i_CoverageType,
	SQ_RatingCoverage_SRC.Premium AS i_Premium,
	SQ_RatingCoverage_SRC.CoverageRiskType AS i_ParentCoverageType,
	SQ_RatingCoverage_SRC.SubCoverageType AS i_SubCoverageType,
	SQ_RatingCoverage_SRC.Change AS i_Change,
	SQ_RatingCoverage_SRC.CoverageForm AS i_CoverageForm,
	SQ_RatingCoverage_SRC.RiskType AS i_RiskType,
	SQ_RatingCoverage_SRC.Exposure AS i_Exposure,
	SQ_RatingCoverage_SRC.CommissionPercentage AS i_CommissionPercentage,
	SQ_RatingCoverage_SRC.LineType AS i_LineType,
	SQ_RatingCoverage_SRC.PolicyGUId AS i_Id,
	SQ_RatingCoverage_SRC.PolicyEffectiveDate AS i_PEffectiveDate,
	SQ_RatingCoverage_SRC.PolicyExpirationDate AS i_PExpirationDate,
	SQ_RatingCoverage_SRC.LineOfBusiness AS i_LineOfBusiness,
	SQ_RatingCoverage_SRC.TransactionType AS i_TType,
	SQ_RatingCoverage_SRC.PolicyStatus,
	SQ_RatingCoverage_SRC.TransactionCreatedDate AS i_TCreatedDate,
	SQ_RatingCoverage_SRC.TransactionEffectiveDate AS i_TEffectiveDate,
	SQ_RatingCoverage_SRC.TransactionExpirationDate AS i_TExpirationDate,
	SQ_RatingCoverage_SRC.TransactionCancellationDate AS i_TCancellationDate,
	SQ_RatingCoverage_SRC.LocationNumber AS i_LocationNumber,
	SQ_RatingCoverage_SRC.CoverageVersion AS i_CoverageVersion,
	SQ_RatingCoverage_SRC.WBProduct AS i_WBProduct,
	SQ_RatingCoverage_SRC.WBProductType AS i_WBProductType,
	SQ_RatingCoverage_SRC.Division AS i_Division,
	SQ_RatingCoverage_SRC.SpecialClassLevel1 AS i_SpecialClassLevel1,
	SQ_RatingCoverage_SRC.BuildingNumber AS i_BuildingNumber,
	SQ_RatingCoverage_SRC.PolicyCoverage AS i_PolicyCoverage,
	SQ_RatingCoverage_SRC.PerilGroup AS i_PerilGroup,
	SQ_RatingCoverage_SRC.RiskId AS i_RiskId,
	SQ_RatingCoverage_SRC.Territory AS i_Territory,
	SQ_RatingCoverage_SRC.LocationXmlId AS i_LocationXmlId,
	SQ_RatingCoverage_SRC.CoverageDeleteFlag AS i_CoverageDeleteFlag,
	SQ_RatingCoverage_SRC.ParentCoverageObjectId AS i_ParentCoverageObjectId,
	SQ_RatingCoverage_SRC.ParentCoverageObjectName AS i_ParentCoverageObjectName,
	SQ_RatingCoverage_SRC.OccupancyClassDescription AS i_OccupancyClassDescription,
	LKP_PolicyCoverageAKID.PolicyCoverageAKID AS i_PolicyCoverageAKID,
	SQ_RatingCoverage_SRC.ActiveBuildingFlag AS i_ActiveBuildingFlag,
	-- *INF*: IIF(ISNULL(i_CoverageType) OR IS_SPACES(i_CoverageType) OR LENGTH(i_CoverageType)=0,'N/A',LTRIM(RTRIM(i_CoverageType)))
	IFF(i_CoverageType IS NULL 
		OR LENGTH(i_CoverageType)>0 AND TRIM(i_CoverageType)='' 
		OR LENGTH(i_CoverageType
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_CoverageType
			)
		)
	) AS v_CoverageType,
	-- *INF*: IIF(ISNULL(i_PolicyCoverageAKID), -1, i_PolicyCoverageAKID)
	IFF(i_PolicyCoverageAKID IS NULL,
		- 1,
		i_PolicyCoverageAKID
	) AS v_PolicyCoverageAKID,
	-- *INF*: IIF(ISNULL(i_Id) or IS_SPACES(i_Id) or LENGTH(i_Id)=0,'N/A',LTRIM(RTRIM(i_Id)))
	IFF(i_Id IS NULL 
		OR LENGTH(i_Id)>0 AND TRIM(i_Id)='' 
		OR LENGTH(i_Id
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_Id
			)
		)
	) AS v_Id,
	-- *INF*: IIF(ISNULL(i_PEffectiveDate),TO_DATE('21001231235959' , 'YYYYMMDDHH24MISS'),i_PEffectiveDate)
	IFF(i_PEffectiveDate IS NULL,
		TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'
		),
		i_PEffectiveDate
	) AS v_PEffectiveDate,
	-- *INF*: IIF(ISNULL(i_PExpirationDate),TO_DATE('21001231235959' , 'YYYYMMDDHH24MISS'),i_PExpirationDate)
	IFF(i_PExpirationDate IS NULL,
		TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'
		),
		i_PExpirationDate
	) AS v_PExpirationDate,
	-- *INF*: IIF(ISNULL(i_LineOfBusiness) OR IS_SPACES(i_LineOfBusiness) OR LENGTH(i_LineOfBusiness)=0,'N/A',LTRIM(RTRIM(i_LineOfBusiness)))
	IFF(i_LineOfBusiness IS NULL 
		OR LENGTH(i_LineOfBusiness)>0 AND TRIM(i_LineOfBusiness)='' 
		OR LENGTH(i_LineOfBusiness
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_LineOfBusiness
			)
		)
	) AS v_LineOfBusiness,
	-- *INF*: IIF(ISNULL(i_LineType) OR IS_SPACES(i_LineType) OR LENGTH(i_LineType)=0,'N/A',LTRIM(RTRIM(i_LineType)))
	IFF(i_LineType IS NULL 
		OR LENGTH(i_LineType)>0 AND TRIM(i_LineType)='' 
		OR LENGTH(i_LineType
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_LineType
			)
		)
	) AS v_LineType,
	-- *INF*: IIF(ISNULL(i_LocationNumber) OR IS_SPACES(i_LocationNumber) OR LENGTH(i_LocationNumber)=0,'0000',LPAD(LTRIM(RTRIM(i_LocationNumber)),4,'0'))
	IFF(i_LocationNumber IS NULL 
		OR LENGTH(i_LocationNumber)>0 AND TRIM(i_LocationNumber)='' 
		OR LENGTH(i_LocationNumber
		) = 0,
		'0000',
		LPAD(LTRIM(RTRIM(i_LocationNumber
				)
			), 4, '0'
		)
	) AS v_LocationNumber,
	-- *INF*: IIF(ISNULL(i_LocationXmlId) OR IS_SPACES(i_LocationXmlId) OR LENGTH(i_LocationXmlId)=0,'N/A',LTRIM(RTRIM(i_LocationXmlId)))
	IFF(i_LocationXmlId IS NULL 
		OR LENGTH(i_LocationXmlId)>0 AND TRIM(i_LocationXmlId)='' 
		OR LENGTH(i_LocationXmlId
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_LocationXmlId
			)
		)
	) AS v_LocationXmlId,
	-- *INF*: IIF(ISNULL(i_TType) OR IS_SPACES(i_TType) OR LENGTH(i_TType)=0,'N/A',LTRIM(RTRIM(i_TType)))
	IFF(i_TType IS NULL 
		OR LENGTH(i_TType)>0 AND TRIM(i_TType)='' 
		OR LENGTH(i_TType
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_TType
			)
		)
	) AS v_TType,
	-- *INF*: IIF(ISNULL(i_TEffectiveDate),TO_DATE('21001231235959' , 'YYYYMMDDHH24MISS'),i_TEffectiveDate)
	IFF(i_TEffectiveDate IS NULL,
		TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'
		),
		i_TEffectiveDate
	) AS v_TEffectiveDate,
	-- *INF*: IIF(ISNULL(i_TCreatedDate),TO_DATE('21001231235959' , 'YYYYMMDDHH24MISS'),i_TCreatedDate)
	IFF(i_TCreatedDate IS NULL,
		TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'
		),
		i_TCreatedDate
	) AS v_TCreatedDate,
	-- *INF*: IIF(ISNULL(i_TExpirationDate),TO_DATE('21001231235959' , 'YYYYMMDDHH24MISS'),i_TExpirationDate)
	-- 
	IFF(i_TExpirationDate IS NULL,
		TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'
		),
		i_TExpirationDate
	) AS v_TExpirationDate,
	-- *INF*: IIF(ISNULL(i_TCancellationDate),TO_DATE('21001231235959' , 'YYYYMMDDHH24MISS'),i_TCancellationDate)
	IFF(i_TCancellationDate IS NULL,
		TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'
		),
		i_TCancellationDate
	) AS v_TCancellationDate,
	-- *INF*: IIF(ISNULL(i_CommissionPercentage),-1,i_CommissionPercentage)
	IFF(i_CommissionPercentage IS NULL,
		- 1,
		i_CommissionPercentage
	) AS v_CommissionPercentage,
	-- *INF*: IIF(ISNULL(i_CoverageForm) or IS_SPACES(i_CoverageForm) or LENGTH(i_CoverageForm)=0,'N/A',i_CoverageForm)
	IFF(i_CoverageForm IS NULL 
		OR LENGTH(i_CoverageForm)>0 AND TRIM(i_CoverageForm)='' 
		OR LENGTH(i_CoverageForm
		) = 0,
		'N/A',
		i_CoverageForm
	) AS v_CoverageForm,
	-- *INF*: IIF(ISNULL(i_RiskType) OR IS_SPACES(i_RiskType) OR LENGTH(i_RiskType)=0,'N/A',LTRIM(RTRIM(i_RiskType)))
	IFF(i_RiskType IS NULL 
		OR LENGTH(i_RiskType)>0 AND TRIM(i_RiskType)='' 
		OR LENGTH(i_RiskType
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_RiskType
			)
		)
	) AS v_RiskType,
	-- *INF*: IIF(ISNULL(i_Exposure),0,i_Exposure)
	IFF(i_Exposure IS NULL,
		0,
		i_Exposure
	) AS v_Exposure,
	-- *INF*: DECODE(TRUE,
	-- IN(i_LineType,'GeneralLiability','SBOPGeneralLiability'),SUBSTR(:LKP.LKP_DCSTATCODESTAGING(i_CoverageId,i_SessionId,'Class'),1,5),
	-- 
	-- i_LineType='WorkersCompensation' and LTRIM(RTRIM(i_ParentCoverageObjectName))='DC_WC_Risk',:LKP.LKP_DCCLASSCODESTAGING_WC(i_ParentCoverageObjectId,i_SessionId),
	-- 
	-- i_LineType='WorkersCompensation',:LKP.LKP_DCSTATCODE_SUPDCTSTATCODE('WorkersCompensation','Type',i_CoverageId,'DC_Coverage',i_SessionId),
	-- 
	-- --i_LineType='WorkersCompensation',:LKP.LKP_DCSTATCODESTAGING_WC(i_CoverageId,i_SessionId),
	-- 
	-- --------------------------------------------------------------------
	-- i_LineType='CommercialAuto' and LTRIM(RTRIM(i_ParentCoverageObjectName))='DC_CA_Risk', :LKP.LKP_DCCLASSCODESTAGING_CA_Risk(i_ParentCoverageObjectId,i_SessionId),
	-- i_LineType='CommercialAuto' and LTRIM(RTRIM(i_ParentCoverageObjectName))='DC_CA_State' and Instr(i_CoverageType,'HiredAndBorrowed',1,1)>0, '6625',
	-- 
	-- --:LKP.LKP_DCCLASSCODESTAGING_CA_HiredAndBorrow(i_ParentCoverageObjectId,i_SessionId,'Primary'),
	-- 
	-- i_LineType='CommercialAuto' and LTRIM(RTRIM(i_ParentCoverageObjectName))='DC_CA_State' and Instr(i_CoverageType,'DriveOtherCar',1,1)>0, :LKP.LKP_DCCLASSCODESTAGING_CA_DriveOtherCar(i_ParentCoverageObjectId,i_SessionId),
	-- i_LineType='CommercialAuto' and LTRIM(RTRIM(i_ParentCoverageObjectName))='DC_Line' and Instr(i_CoverageType,'NonOwned',1,1)>0, :LKP.LKP_DCCLASSCODESTAGING_CA_NonOwned(i_ParentCoverageObjectId,i_SessionId),
	-- i_LineType='CommercialAuto' and LTRIM(RTRIM(i_ParentCoverageObjectName))='DC_Line', :LKP.LKP_DCSTATCODESTAGING_CA(i_CoverageId,i_SessionId),
	-- i_LineType='CommercialAuto' and LTRIM(RTRIM(i_ParentCoverageObjectName))='DC_CA_State' and Instr(i_CoverageType,'MotorJunkLicense',1,1)>0, :LKP.LKP_DCCLASSCODESTAGING_CA_MotorJunkLicense(i_ParentCoverageObjectId,i_SessionId),
	-- --------------------------------------------------------------------
	-- :LKP.LKP_DCSTATCODESTAGING(i_CoverageId,i_SessionId,'Class'))
	DECODE(TRUE,
		i_LineType IN ('GeneralLiability','SBOPGeneralLiability'), SUBSTR(LKP_DCSTATCODESTAGING_i_CoverageId_i_SessionId_Class.Value, 1, 5
		),
		i_LineType = 'WorkersCompensation' 
		AND LTRIM(RTRIM(i_ParentCoverageObjectName
			)
		) = 'DC_WC_Risk', LKP_DCCLASSCODESTAGING_WC_i_ParentCoverageObjectId_i_SessionId.Value,
		i_LineType = 'WorkersCompensation', LKP_DCSTATCODE_SUPDCTSTATCODE__WorkersCompensation_Type_i_CoverageId_DC_Coverage_i_SessionId.Value,
		i_LineType = 'CommercialAuto' 
		AND LTRIM(RTRIM(i_ParentCoverageObjectName
			)
		) = 'DC_CA_Risk', LKP_DCCLASSCODESTAGING_CA_RISK_i_ParentCoverageObjectId_i_SessionId.Value,
		i_LineType = 'CommercialAuto' 
		AND LTRIM(RTRIM(i_ParentCoverageObjectName
			)
		) = 'DC_CA_State' 
		AND REGEXP_INSTR(i_CoverageType, 'HiredAndBorrowed', 1, 1
		) > 0, '6625',
		i_LineType = 'CommercialAuto' 
		AND LTRIM(RTRIM(i_ParentCoverageObjectName
			)
		) = 'DC_CA_State' 
		AND REGEXP_INSTR(i_CoverageType, 'DriveOtherCar', 1, 1
		) > 0, LKP_DCCLASSCODESTAGING_CA_DRIVEOTHERCAR_i_ParentCoverageObjectId_i_SessionId.Value,
		i_LineType = 'CommercialAuto' 
		AND LTRIM(RTRIM(i_ParentCoverageObjectName
			)
		) = 'DC_Line' 
		AND REGEXP_INSTR(i_CoverageType, 'NonOwned', 1, 1
		) > 0, LKP_DCCLASSCODESTAGING_CA_NONOWNED_i_ParentCoverageObjectId_i_SessionId.Value,
		i_LineType = 'CommercialAuto' 
		AND LTRIM(RTRIM(i_ParentCoverageObjectName
			)
		) = 'DC_Line', LKP_DCSTATCODESTAGING_CA_i_CoverageId_i_SessionId.Value,
		i_LineType = 'CommercialAuto' 
		AND LTRIM(RTRIM(i_ParentCoverageObjectName
			)
		) = 'DC_CA_State' 
		AND REGEXP_INSTR(i_CoverageType, 'MotorJunkLicense', 1, 1
		) > 0, LKP_DCCLASSCODESTAGING_CA_MOTORJUNKLICENSE_i_ParentCoverageObjectId_i_SessionId.Value,
		LKP_DCSTATCODESTAGING_i_CoverageId_i_SessionId_Class.Value
	) AS v_ClassCode_lkp,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(v_ClassCode_lkp),'N/A',
	-- IS_SPACES(v_ClassCode_lkp),'N/A',
	-- LENGTH(v_ClassCode_lkp)=0,'N/A',
	-- INSTR(i_CoverageType, 'NonOwned',1,1) > 0 AND INSTR(v_ClassCode_lkp,',',1,1)>0,
	-- SUBSTR(v_ClassCode_lkp,INSTR(v_ClassCode_lkp,',',1,1)+1),
	-- NOT INSTR(i_CoverageType,'NonOwned',1,1) > 0 AND INSTR(v_ClassCode_lkp,',',1,1)>0, 
	-- SUBSTR(v_ClassCode_lkp,1,INSTR(v_ClassCode_lkp,',',1,1)-1),
	-- v_ClassCode_lkp)
	-- 
	-- 
	-- 
	-- -- 4/15/2015 SM#548627, PROD-9266 : Code change to get only one class code value when we get multiple classcode from Source system.
	DECODE(TRUE,
		v_ClassCode_lkp IS NULL, 'N/A',
		LENGTH(v_ClassCode_lkp)>0 AND TRIM(v_ClassCode_lkp)='', 'N/A',
		LENGTH(v_ClassCode_lkp
		) = 0, 'N/A',
		REGEXP_INSTR(i_CoverageType, 'NonOwned', 1, 1
		) > 0 
		AND REGEXP_INSTR(v_ClassCode_lkp, ',', 1, 1
		) > 0, SUBSTR(v_ClassCode_lkp, REGEXP_INSTR(v_ClassCode_lkp, ',', 1, 1
			) + 1
		),
		NOT REGEXP_INSTR(i_CoverageType, 'NonOwned', 1, 1
		) > 0 
		AND REGEXP_INSTR(v_ClassCode_lkp, ',', 1, 1
		) > 0, SUBSTR(v_ClassCode_lkp, 1, REGEXP_INSTR(v_ClassCode_lkp, ',', 1, 1
			) - 1
		),
		v_ClassCode_lkp
	) AS v_ClassCode,
	-- *INF*: :LKP.LKP_DCSTATCODESTAGING(i_CoverageId,i_SessionId,'Subline')
	LKP_DCSTATCODESTAGING_i_CoverageId_i_SessionId_Subline.Value AS v_SubLineCode_lkp,
	-- *INF*: IIF(ISNULL(v_SubLineCode_lkp) OR IS_SPACES(v_SubLineCode_lkp) OR LENGTH(v_SubLineCode_lkp)=0,'N/A',LTRIM(RTRIM(v_SubLineCode_lkp)))
	IFF(v_SubLineCode_lkp IS NULL 
		OR LENGTH(v_SubLineCode_lkp)>0 AND TRIM(v_SubLineCode_lkp)='' 
		OR LENGTH(v_SubLineCode_lkp
		) = 0,
		'N/A',
		LTRIM(RTRIM(v_SubLineCode_lkp
			)
		)
	) AS v_SubLineCode,
	-- *INF*: IIF(ISNULL(i_Territory) OR IS_SPACES(i_Territory) OR LENGTH(i_Territory)=0,'N/A',LTRIM(RTRIM(i_Territory)))
	IFF(i_Territory IS NULL 
		OR LENGTH(i_Territory)>0 AND TRIM(i_Territory)='' 
		OR LENGTH(i_Territory
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_Territory
			)
		)
	) AS v_Territory,
	-- *INF*: IIF(ISNULL(i_Premium),0,i_Premium)
	IFF(i_Premium IS NULL,
		0,
		i_Premium
	) AS v_Premium,
	-- *INF*: IIF(ISNULL(i_ParentCoverageType) OR LENGTH(LTRIM(RTRIM(i_ParentCoverageType)))=0,'N/A',LTRIM(RTRIM(i_ParentCoverageType)))
	IFF(i_ParentCoverageType IS NULL 
		OR LENGTH(LTRIM(RTRIM(i_ParentCoverageType
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_ParentCoverageType
			)
		)
	) AS v_PType,
	-- *INF*: IIF(ISNULL(i_Change),0,i_Change)
	IFF(i_Change IS NULL,
		0,
		i_Change
	) AS v_Change,
	-- *INF*: IIF(
	--   v_Change=0,
	--   0,
	--   1
	-- )
	IFF(v_Change = 0,
		0,
		1
	) AS v_PremiumBearingIndicator,
	EXP_PolicyCoverageAKID.pol_ak_id,
	EXP_PolicyCoverageAKID.RiskLocationAKID,
	v_LineOfBusiness AS o_LineOfBusiness,
	v_CommissionPercentage AS o_CommissionPercentage,
	v_LineType AS o_LineType,
	v_PEffectiveDate AS o_PEffectiveDate,
	v_PExpirationDate AS o_PExpirationDate,
	v_Change AS o_Change,
	i_TType AS o_TType,
	-- *INF*: IIF(ISNULL(i_CoverageGUID) OR IS_SPACES(i_CoverageGUID) OR LENGTH(i_CoverageGUID)=0, 'N/A', LTRIM(RTRIM(i_CoverageGUID)))
	IFF(i_CoverageGUID IS NULL 
		OR LENGTH(i_CoverageGUID)>0 AND TRIM(i_CoverageGUID)='' 
		OR LENGTH(i_CoverageGUID
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_CoverageGUID
			)
		)
	) AS o_CoverageGUID,
	i_LineId AS o_LineId,
	-- *INF*: pol_ak_id||'~'||RiskLocationAKID||'~'||v_PolicyCoverageAKID||'~'||TO_CHAR(v_TCreatedDate)||'~'||i_CoverageGUID
	pol_ak_id || '~' || RiskLocationAKID || '~' || v_PolicyCoverageAKID || '~' || TO_CHAR(v_TCreatedDate
	) || '~' || i_CoverageGUID AS o_RatingCoverageKey,
	v_CoverageForm AS o_CoverageForm,
	v_RiskType AS o_RiskType,
	v_CoverageType AS o_CoverageType,
	-- *INF*: IIF(i_LineType='WorkersCompensation' AND ltrim(rtrim(v_ClassCode)) !='N/A',LPAD(LTRIM(RTRIM(v_ClassCode)),4,'0'),LTRIM(RTRIM(v_ClassCode)))
	-- 
	IFF(i_LineType = 'WorkersCompensation' 
		AND ltrim(rtrim(v_ClassCode
			)
		) != 'N/A',
		LPAD(LTRIM(RTRIM(v_ClassCode
				)
			), 4, '0'
		),
		LTRIM(RTRIM(v_ClassCode
			)
		)
	) AS o_ClassCode,
	v_Exposure AS o_Exposure,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_PeriodStartDate) AND ISNULL(lkp_PeriodStartDate),v_TEffectiveDate,
	-- NOT ISNULL(lkp_PeriodStartDate),GREATEST(lkp_PeriodStartDate,v_TEffectiveDate),
	-- GREATEST(i_PeriodStartDate,v_TEffectiveDate))
	DECODE(TRUE,
		i_PeriodStartDate IS NULL 
		AND lkp_PeriodStartDate IS NULL, v_TEffectiveDate,
		lkp_PeriodStartDate IS NOT NULL, GREATEST(lkp_PeriodStartDate, v_TEffectiveDate
		),
		GREATEST(i_PeriodStartDate, v_TEffectiveDate
		)
	) AS o_TEffectiveDate,
	v_TCreatedDate AS o_TCreatedDate,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_PeriodEndDate) AND ISNULL(lkp_PeriodEndDate),v_TExpirationDate,
	-- NOT ISNULL(lkp_PeriodEndDate),LEAST(lkp_PeriodEndDate,v_TExpirationDate),
	-- LEAST(i_PeriodEndDate,v_TExpirationDate))
	DECODE(TRUE,
		i_PeriodEndDate IS NULL 
		AND lkp_PeriodEndDate IS NULL, v_TExpirationDate,
		lkp_PeriodEndDate IS NOT NULL, LEAST(lkp_PeriodEndDate, v_TExpirationDate
		),
		LEAST(i_PeriodEndDate, v_TExpirationDate
		)
	) AS o_TExpirationDate,
	-- *INF*: IIF(v_TType='Cancel', v_TCancellationDate, TO_DATE('21001231235959','YYYYMMDDHH24MISS'))
	IFF(v_TType = 'Cancel',
		v_TCancellationDate,
		TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'
		)
	) AS o_TCancellationDate,
	-- *INF*: IIF(ISNULL(i_CoverageVersion) OR IS_SPACES(i_CoverageVersion) OR LENGTH(i_CoverageVersion)=0, 'N/A', i_CoverageVersion)
	IFF(i_CoverageVersion IS NULL 
		OR LENGTH(i_CoverageVersion)>0 AND TRIM(i_CoverageVersion)='' 
		OR LENGTH(i_CoverageVersion
		) = 0,
		'N/A',
		i_CoverageVersion
	) AS o_CoverageVersion,
	v_SubLineCode AS o_SubLineCode,
	v_PremiumBearingIndicator AS o_PremiumBearingIndicator,
	-- *INF*: IIF(ISNULL(i_WBProduct) OR IS_SPACES(i_WBProduct) OR LENGTH(i_WBProduct)=0, 'N/A', i_WBProduct)
	IFF(i_WBProduct IS NULL 
		OR LENGTH(i_WBProduct)>0 AND TRIM(i_WBProduct)='' 
		OR LENGTH(i_WBProduct
		) = 0,
		'N/A',
		i_WBProduct
	) AS o_WBProduct,
	-- *INF*: IIF(ISNULL(i_WBProductType) OR IS_SPACES(i_WBProductType) OR LENGTH(i_WBProductType)=0, 'N/A', i_WBProductType)
	IFF(i_WBProductType IS NULL 
		OR LENGTH(i_WBProductType)>0 AND TRIM(i_WBProductType)='' 
		OR LENGTH(i_WBProductType
		) = 0,
		'N/A',
		i_WBProductType
	) AS o_WBProductType,
	-- *INF*: IIF(ISNULL(i_Division) OR IS_SPACES(i_Division) OR LENGTH(i_Division)=0, 'N/A', i_Division)
	IFF(i_Division IS NULL 
		OR LENGTH(i_Division)>0 AND TRIM(i_Division)='' 
		OR LENGTH(i_Division
		) = 0,
		'N/A',
		i_Division
	) AS o_Division,
	-- *INF*: IIF(ISNULL(i_SpecialClassLevel1) OR IS_SPACES(i_SpecialClassLevel1) OR LENGTH(i_SpecialClassLevel1)=0,'N/A',LTRIM(RTRIM(i_SpecialClassLevel1)))
	IFF(i_SpecialClassLevel1 IS NULL 
		OR LENGTH(i_SpecialClassLevel1)>0 AND TRIM(i_SpecialClassLevel1)='' 
		OR LENGTH(i_SpecialClassLevel1
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_SpecialClassLevel1
			)
		)
	) AS o_SpecialClassLevel1,
	-- *INF*: IIF( (IN(LOWER(i_LineType),'property','sbopproperty')  OR  LTRIM(RTRIM(i_WBProduct))='SMARTbusiness') AND  NOT ISNULL(i_BuildingNumber), LPAD(i_BuildingNumber, 3, '0'), '000')
	IFF(( LOWER(i_LineType
			) IN ('property','sbopproperty') 
			OR LTRIM(RTRIM(i_WBProduct
				)
			) = 'SMARTbusiness' 
		) 
		AND i_BuildingNumber IS NOT NULL,
		LPAD(i_BuildingNumber, 3, '0'
		),
		'000'
	) AS o_SubLocationUnitNumber,
	-- *INF*: IIF(ISNULL(i_PolicyCoverage) OR IS_SPACES(i_PolicyCoverage) OR LENGTH(i_PolicyCoverage)=0,'N/A',LTRIM(RTRIM(i_PolicyCoverage)))
	IFF(i_PolicyCoverage IS NULL 
		OR LENGTH(i_PolicyCoverage)>0 AND TRIM(i_PolicyCoverage)='' 
		OR LENGTH(i_PolicyCoverage
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_PolicyCoverage
			)
		)
	) AS o_PolicyCoverage,
	-- *INF*: IIF(ISNULL(i_PerilGroup) OR IS_SPACES(i_PerilGroup) OR LENGTH(i_PerilGroup)=0, 'N/A', LTRIM(RTRIM(i_PerilGroup)))
	IFF(i_PerilGroup IS NULL 
		OR LENGTH(i_PerilGroup)>0 AND TRIM(i_PerilGroup)='' 
		OR LENGTH(i_PerilGroup
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_PerilGroup
			)
		)
	) AS o_PerilGroup,
	-- *INF*: DECODE(TRUE,
	-- v_LineType = 'BusinessOwners' and LTRIM(RTRIM(i_WBProduct))='SMARTbusiness' and LTRIM(RTRIM(i_Division))='CommercialLines' and INSTR(v_PType,'Spoilage')>0,'Spoilage',
	-- (i_WBProduct = 'SMARTbusiness' and v_LineType = 'BusinessOwners') Or (IN (v_PType,'EquipmentBreakdown','MineSubsidence','DataCompromise','CyberComputerAttack','CyberExtendedReportingPeriod','CyberExtortionExpenses','CyberNetworkSecurity', 'CyberSuite','EmploymentPracticesLiability', 'WB516', 'WB516CA','WB516CANC', 'WB516GL', 'NS0279', 'NS0313','ActsErrorsOmissionsLiabilityCoverages', 'WB1429', 'WB2086', 'WB1144', 'WB1430', 'WB1146', 'NS0320', 'WB2525','WB2525NC', 'WB2499', 'WB2216', 'WB1372','NS0453','NS0453NC', 'NS0321', 'CA2562', 'CA2564', 'CA2565')),v_PType,
	--  'N/A')
	DECODE(TRUE,
		v_LineType = 'BusinessOwners' 
		AND LTRIM(RTRIM(i_WBProduct
			)
		) = 'SMARTbusiness' 
		AND LTRIM(RTRIM(i_Division
			)
		) = 'CommercialLines' 
		AND REGEXP_INSTR(v_PType, 'Spoilage'
		) > 0, 'Spoilage',
		( i_WBProduct = 'SMARTbusiness' 
			AND v_LineType = 'BusinessOwners' 
		) 
		OR ( v_PType IN ('EquipmentBreakdown','MineSubsidence','DataCompromise','CyberComputerAttack','CyberExtendedReportingPeriod','CyberExtortionExpenses','CyberNetworkSecurity','CyberSuite','EmploymentPracticesLiability','WB516','WB516CA','WB516CANC','WB516GL','NS0279','NS0313','ActsErrorsOmissionsLiabilityCoverages','WB1429','WB2086','WB1144','WB1430','WB1146','NS0320','WB2525','WB2525NC','WB2499','WB2216','WB1372','NS0453','NS0453NC','NS0321','CA2562','CA2564','CA2565') 
		), v_PType,
		'N/A'
	) AS lkp_InsRefParentCoverageType,
	v_PType AS o_ParentCoverageType,
	-- *INF*: IIF(NOT ISNULL(i_SubCoverageType) AND NOT LENGTH(i_SubCoverageType)=0 AND NOT IS_SPACES(i_SubCoverageType),i_SubCoverageType,'N/A')
	IFF(i_SubCoverageType IS NULL 
		AND NOT LENGTH(i_SubCoverageType
		) = 0 
		AND NOT LENGTH(i_SubCoverageType)>0 AND TRIM(i_SubCoverageTypeNOT )='',
		i_SubCoverageType,
		'N/A'
	) AS o_SubCoverageType,
	-- *INF*: IIF(NOT ISNULL(i_CoverageDeleteFlag),i_CoverageDeleteFlag,'0')
	IFF(i_CoverageDeleteFlag IS NOT NULL,
		i_CoverageDeleteFlag,
		'0'
	) AS o_CoverageDeleteFlag,
	-- *INF*: IIF(ISNULL(i_OccupancyClassDescription) OR IS_SPACES(i_OccupancyClassDescription) OR LENGTH(i_OccupancyClassDescription)=0,'N/A',LTRIM(RTRIM(i_OccupancyClassDescription)))
	IFF(i_OccupancyClassDescription IS NULL 
		OR LENGTH(i_OccupancyClassDescription)>0 AND TRIM(i_OccupancyClassDescription)='' 
		OR LENGTH(i_OccupancyClassDescription
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_OccupancyClassDescription
			)
		)
	) AS OccupancyClassDescription,
	-- *INF*: DECODE(i_ActiveBuildingFlag,'T','1','F','0','1')
	DECODE(i_ActiveBuildingFlag,
		'T', '1',
		'F', '0',
		'1'
	) AS ActiveBuildingFlag,
	EXP_PolicyKey.o_PolicyKey,
	EXP_RiskLocationKey.RatingCoverageAKID,
	EXP_RiskLocationKey.RatingCoverageHashKey,
	EXP_RiskLocationKey.RatingCoverageId,
	EXP_RiskLocationKey.PolicyCoverageAKID,
	EXP_RiskLocationKey.RatingCoverageEffectivedate,
	EXP_RiskLocationKey.RatingCoverageExpirationdate
	FROM EXP_PolicyCoverageAKID
	 -- Manually join with EXP_PolicyKey
	 -- Manually join with EXP_RiskLocationKey
	 -- Manually join with SQ_RatingCoverage_SRC
	LEFT JOIN LKP_DCWCStateTermStaging
	ON LKP_DCWCStateTermStaging.WC_StateTermId = SQ_RatingCoverage_SRC.ParentCoverageObjectId AND LKP_DCWCStateTermStaging.ObjectName = SQ_RatingCoverage_SRC.ParentCoverageObjectName
	LEFT JOIN LKP_PolicyCoverageAKID
	ON LKP_PolicyCoverageAKID.PolicyCoverageHashKey = EXP_PolicyCoverageAKID.o_PolicyCoverageHashKey
	LEFT JOIN LKP_WBWCCoverageTermStage
	ON LKP_WBWCCoverageTermStage.CoverageId = SQ_RatingCoverage_SRC.CoverageId
	LEFT JOIN LKP_DCSTATCODESTAGING LKP_DCSTATCODESTAGING_i_CoverageId_i_SessionId_Class
	ON LKP_DCSTATCODESTAGING_i_CoverageId_i_SessionId_Class.ObjectId = i_CoverageId
	AND LKP_DCSTATCODESTAGING_i_CoverageId_i_SessionId_Class.SessionId = i_SessionId
	AND LKP_DCSTATCODESTAGING_i_CoverageId_i_SessionId_Class.Type = 'Class'

	LEFT JOIN LKP_DCCLASSCODESTAGING_WC LKP_DCCLASSCODESTAGING_WC_i_ParentCoverageObjectId_i_SessionId
	ON LKP_DCCLASSCODESTAGING_WC_i_ParentCoverageObjectId_i_SessionId.ObjectId = i_ParentCoverageObjectId
	AND LKP_DCCLASSCODESTAGING_WC_i_ParentCoverageObjectId_i_SessionId.SessionId = i_SessionId

	LEFT JOIN LKP_DCSTATCODE_SUPDCTSTATCODE LKP_DCSTATCODE_SUPDCTSTATCODE__WorkersCompensation_Type_i_CoverageId_DC_Coverage_i_SessionId
	ON LKP_DCSTATCODE_SUPDCTSTATCODE__WorkersCompensation_Type_i_CoverageId_DC_Coverage_i_SessionId.LineOfBusiness = 'WorkersCompensation'
	AND LKP_DCSTATCODE_SUPDCTSTATCODE__WorkersCompensation_Type_i_CoverageId_DC_Coverage_i_SessionId.SessionId = 'Type'
	AND LKP_DCSTATCODE_SUPDCTSTATCODE__WorkersCompensation_Type_i_CoverageId_DC_Coverage_i_SessionId.ObjectId = i_CoverageId
	AND LKP_DCSTATCODE_SUPDCTSTATCODE__WorkersCompensation_Type_i_CoverageId_DC_Coverage_i_SessionId.ObjectName = 'DC_Coverage'
	AND LKP_DCSTATCODE_SUPDCTSTATCODE__WorkersCompensation_Type_i_CoverageId_DC_Coverage_i_SessionId.ObjectKey = i_SessionId

	LEFT JOIN LKP_DCCLASSCODESTAGING_CA_RISK LKP_DCCLASSCODESTAGING_CA_RISK_i_ParentCoverageObjectId_i_SessionId
	ON LKP_DCCLASSCODESTAGING_CA_RISK_i_ParentCoverageObjectId_i_SessionId.ObjectId = i_ParentCoverageObjectId
	AND LKP_DCCLASSCODESTAGING_CA_RISK_i_ParentCoverageObjectId_i_SessionId.SessionId = i_SessionId

	LEFT JOIN LKP_DCCLASSCODESTAGING_CA_DRIVEOTHERCAR LKP_DCCLASSCODESTAGING_CA_DRIVEOTHERCAR_i_ParentCoverageObjectId_i_SessionId
	ON LKP_DCCLASSCODESTAGING_CA_DRIVEOTHERCAR_i_ParentCoverageObjectId_i_SessionId.ObjectId = i_ParentCoverageObjectId
	AND LKP_DCCLASSCODESTAGING_CA_DRIVEOTHERCAR_i_ParentCoverageObjectId_i_SessionId.SessionId = i_SessionId

	LEFT JOIN LKP_DCCLASSCODESTAGING_CA_NONOWNED LKP_DCCLASSCODESTAGING_CA_NONOWNED_i_ParentCoverageObjectId_i_SessionId
	ON LKP_DCCLASSCODESTAGING_CA_NONOWNED_i_ParentCoverageObjectId_i_SessionId.ObjectId = i_ParentCoverageObjectId
	AND LKP_DCCLASSCODESTAGING_CA_NONOWNED_i_ParentCoverageObjectId_i_SessionId.SessionId = i_SessionId

	LEFT JOIN LKP_DCSTATCODESTAGING_CA LKP_DCSTATCODESTAGING_CA_i_CoverageId_i_SessionId
	ON LKP_DCSTATCODESTAGING_CA_i_CoverageId_i_SessionId.ObjectId = i_CoverageId
	AND LKP_DCSTATCODESTAGING_CA_i_CoverageId_i_SessionId.SessionId = i_SessionId

	LEFT JOIN LKP_DCCLASSCODESTAGING_CA_MOTORJUNKLICENSE LKP_DCCLASSCODESTAGING_CA_MOTORJUNKLICENSE_i_ParentCoverageObjectId_i_SessionId
	ON LKP_DCCLASSCODESTAGING_CA_MOTORJUNKLICENSE_i_ParentCoverageObjectId_i_SessionId.ObjectId = i_ParentCoverageObjectId
	AND LKP_DCCLASSCODESTAGING_CA_MOTORJUNKLICENSE_i_ParentCoverageObjectId_i_SessionId.SessionId = i_SessionId

	LEFT JOIN LKP_DCSTATCODESTAGING LKP_DCSTATCODESTAGING_i_CoverageId_i_SessionId_Subline
	ON LKP_DCSTATCODESTAGING_i_CoverageId_i_SessionId_Subline.ObjectId = i_CoverageId
	AND LKP_DCSTATCODESTAGING_i_CoverageId_i_SessionId_Subline.SessionId = i_SessionId
	AND LKP_DCSTATCODESTAGING_i_CoverageId_i_SessionId_Subline.Type = 'Subline'

),
LKP_RiskLocation_GetStateProvinceCode AS (
	SELECT
	state_code,
	RiskLocationAKID
	FROM (
		SELECT 
		RiskLocation.CurrentSnapshotFlag as CurrentSnapshotFlag, 
		st.state_code as state_code, 
		RiskLocation.RiskLocationAKID as RiskLocationAKID 
		FROM 
		RiskLocation RiskLocation
		inner join
		sup_state St on ST.state_abbrev=RiskLocation.StateProvinceCode
		Inner Join
		V2.policy b
		on b.pol_ak_id=RiskLocation.PolicyAKID
		and b.crrnt_snpsht_flag=1
		INNER JOIN 
		(select distinct WCT.PolicyNumber,ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00') as PolicyVersionFormatted from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT) WCT
		on WCT.PolicyNumber=b.pol_num
		and PolicyVersionFormatted=b.pol_mod
		WHERE 
		RiskLocation.CurrentSnapshotFlag=1 and 
		RiskLocation.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationAKID ORDER BY state_code) = 1
),
EXP_ApplyTemplateChangeRules AS (
	SELECT
	EXP_GetValues.o_CoverageType AS i_CoverageType,
	EXP_GetValues.o_ClassCode AS i_ClassCode,
	LKP_RiskLocation_GetStateProvinceCode.state_code AS i_state_code,
	EXP_GetValues.o_LineType AS i_LineType,
	-- *INF*: DECODE(TRUE,
	-- i_LineType != 'WorkersCompensation', i_ClassCode,
	-- i_state_code='MN' and i_CoverageType='SecondInjuryFund', '0174',
	-- i_state_code='IN' and i_CoverageType='SecondInjuryFund', '0935',
	-- i_state_code='MN' and i_CoverageType='OtherTaxesAndAssessments1', '0988',
	-- i_state_code='MT' and i_CoverageType='SecondInjuryFund', '0935',
	-- i_state_code='MT' and i_CoverageType='AdministrationFund', '0939',
	-- i_state_code='MT' and i_CoverageType='SafetyEducationAndTrainingFund', '9616',
	-- i_state_code='MT' and i_CoverageType='OtherTaxesAndAssessments1', '0934',
	-- i_state_code='NJ' and i_CoverageType='SecondInjuryFund', '0935',
	-- i_state_code='NJ' and i_CoverageType='UninsuredEmployersFund', '0936',
	-- i_state_code='NY' and i_CoverageType='StateAssessment', '0932',
	-- i_state_code='NY' and i_CoverageType='SecurityFundCharge', '9749',
	-- i_state_code='PA' and i_CoverageType='EmployerAssessment', '0938',
	-- 
	-- i_ClassCode
	-- )
	DECODE(TRUE,
		i_LineType != 'WorkersCompensation', i_ClassCode,
		i_state_code = 'MN' 
		AND i_CoverageType = 'SecondInjuryFund', '0174',
		i_state_code = 'IN' 
		AND i_CoverageType = 'SecondInjuryFund', '0935',
		i_state_code = 'MN' 
		AND i_CoverageType = 'OtherTaxesAndAssessments1', '0988',
		i_state_code = 'MT' 
		AND i_CoverageType = 'SecondInjuryFund', '0935',
		i_state_code = 'MT' 
		AND i_CoverageType = 'AdministrationFund', '0939',
		i_state_code = 'MT' 
		AND i_CoverageType = 'SafetyEducationAndTrainingFund', '9616',
		i_state_code = 'MT' 
		AND i_CoverageType = 'OtherTaxesAndAssessments1', '0934',
		i_state_code = 'NJ' 
		AND i_CoverageType = 'SecondInjuryFund', '0935',
		i_state_code = 'NJ' 
		AND i_CoverageType = 'UninsuredEmployersFund', '0936',
		i_state_code = 'NY' 
		AND i_CoverageType = 'StateAssessment', '0932',
		i_state_code = 'NY' 
		AND i_CoverageType = 'SecurityFundCharge', '9749',
		i_state_code = 'PA' 
		AND i_CoverageType = 'EmployerAssessment', '0938',
		i_ClassCode
	) AS v_MN_IN_ClassCode_Override,
	v_MN_IN_ClassCode_Override AS o_ClassCode
	FROM EXP_GetValues
	LEFT JOIN LKP_RiskLocation_GetStateProvinceCode
	ON LKP_RiskLocation_GetStateProvinceCode.RiskLocationAKID = Mplt_RiskLocationAKID_Population.o_RiskLocationAKID
),
AGG_RemoveDuplicates AS (
	SELECT
	EXP_GetValues.PolicyStatus,
	EXP_GetValues.o_LineOfBusiness AS i_LineOfBusiness,
	EXP_GetValues.o_CommissionPercentage AS i_CommissionPercentage,
	EXP_GetValues.o_LineType AS LineType,
	EXP_GetValues.o_PEffectiveDate AS PEffectiveDate,
	EXP_GetValues.o_PExpirationDate AS PExpirationDate,
	EXP_GetValues.o_Change AS Change,
	EXP_GetValues.o_TType AS TType,
	EXP_GetValues.o_LineId AS LineId,
	EXP_GetValues.o_RatingCoverageKey AS RatingCoverageKey,
	EXP_GetValues.o_CoverageForm AS CoverageForm,
	EXP_GetValues.o_RiskType AS RiskType,
	EXP_GetValues.o_CoverageType AS CoverageType,
	EXP_ApplyTemplateChangeRules.o_ClassCode AS ClassCode,
	EXP_GetValues.o_Exposure AS Exposure,
	EXP_GetValues.o_TEffectiveDate AS TEffectiveDate,
	EXP_GetValues.pol_ak_id,
	EXP_GetValues.o_CoverageGUID AS CoverageGUID,
	EXP_GetValues.o_TCreatedDate AS TCreatedDate,
	EXP_GetValues.i_PolicyCoverageAKID AS PolicyCoverageAKID,
	EXP_GetValues.o_TExpirationDate AS TExpirationDate,
	EXP_GetValues.o_TCancellationDate AS TCancellationDate,
	EXP_GetValues.o_CoverageVersion AS CoverageVersion,
	EXP_GetValues.o_SubLineCode AS SubLineCode,
	EXP_GetValues.o_PremiumBearingIndicator AS PremiumBearingIndicator,
	EXP_GetValues.o_WBProduct AS WBProduct,
	EXP_GetValues.o_WBProductType AS WBProductType,
	EXP_GetValues.o_Division AS Division,
	EXP_GetValues.o_SpecialClassLevel1 AS SpecialClassLevel1,
	EXP_GetValues.o_SubLocationUnitNumber AS SubLocationUnitNumber,
	EXP_GetValues.o_PolicyCoverage AS PolicyCoverage,
	EXP_GetValues.o_PerilGroup AS PerilGroup,
	EXP_GetValues.lkp_InsRefParentCoverageType,
	EXP_GetValues.o_ParentCoverageType AS ParentCoverageType,
	EXP_GetValues.o_SubCoverageType AS SubCoverageType,
	EXP_GetValues.o_CoverageDeleteFlag AS CoverageDeleteFlag,
	EXP_GetValues.OccupancyClassDescription,
	EXP_GetValues.ActiveBuildingFlag,
	EXP_GetValues.RiskLocationAKID,
	EXP_GetValues.o_PolicyKey AS PolicyKey,
	EXP_GetValues.RatingCoverageAKID,
	EXP_GetValues.RatingCoverageHashKey,
	EXP_GetValues.RatingCoverageId,
	EXP_GetValues.PolicyCoverageAKID AS RatingCoverage_PolicyCoverageAKID,
	EXP_GetValues.RatingCoverageEffectivedate,
	EXP_GetValues.RatingCoverageExpirationdate
	FROM EXP_ApplyTemplateChangeRules
	 -- Manually join with EXP_GetValues
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id, CoverageGUID, TCreatedDate ORDER BY NULL) = 1
),
LKP_SupDCTPolicyOfferingLineOfBusinessProductRules AS (
	SELECT
	InsuranceReferenceLineOfBusinessCode,
	ProductCode,
	DCTProductCode,
	DCTProductType,
	DCTPolicyDivision,
	DCTLineOfBusinessCode,
	DCTCoverageType
	FROM (
		SELECT 
		InsuranceReferenceLineOfBusinessCode as InsuranceReferenceLineOfBusinessCode, 
		ProductCode as ProductCode,
		DCTProductCode as DCTProductCode, 
		DCTProductType as DCTProductType, 
		DCTPolicyDivision as DCTPolicyDivision, 
		REPLACE(DCTLineOfBusinessCode,' ','') as DCTLineOfBusinessCode,
		DCTCoverageType as DCTCoverageType 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupDCTPolicyOfferingLineOfBusinessProductRules
		where getdate() between effectivedate and expirationdate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY DCTProductCode,DCTProductType,DCTPolicyDivision,DCTLineOfBusinessCode,DCTCoverageType ORDER BY InsuranceReferenceLineOfBusinessCode) = 1
),
LKP_SupSpecialClassGroup AS (
	SELECT
	StandardSpecialClassGroupCode,
	SpecialClassGroupCode
	FROM (
		SELECT 
			StandardSpecialClassGroupCode,
			SpecialClassGroupCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupSpecialClassGroup
		WHERE CurrentSnapshotFlag=1 AND SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SpecialClassGroupCode ORDER BY StandardSpecialClassGroupCode) = 1
),
LKP_sup_insurance_line AS (
	SELECT
	StandardInsuranceLineCode,
	ins_line_code
	FROM (
		SELECT 
			StandardInsuranceLineCode,
			ins_line_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_insurance_line
		WHERE crrnt_snpsht_flag=1 and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ins_line_code ORDER BY StandardInsuranceLineCode) = 1
),
EXP_CalPolicyCoverageHashKey AS (
	SELECT
	AGG_RemoveDuplicates.pol_ak_id AS i_pol_ak_id,
	AGG_RemoveDuplicates.RiskLocationAKID AS i_RiskLocationAKID,
	AGG_RemoveDuplicates.PolicyCoverageAKID AS i_PolicyCoverageAKID,
	AGG_RemoveDuplicates.LineType AS i_LineType,
	AGG_RemoveDuplicates.PEffectiveDate,
	AGG_RemoveDuplicates.PExpirationDate,
	AGG_RemoveDuplicates.PolicyCoverage AS i_PolicyCoverage,
	LKP_SupSpecialClassGroup.StandardSpecialClassGroupCode AS i_StandardSpecialClassGroupCode,
	LKP_sup_insurance_line.StandardInsuranceLineCode,
	AGG_RemoveDuplicates.PolicyStatus,
	AGG_RemoveDuplicates.Change,
	AGG_RemoveDuplicates.TType,
	AGG_RemoveDuplicates.CoverageGUID,
	AGG_RemoveDuplicates.LineId,
	AGG_RemoveDuplicates.RatingCoverageKey,
	AGG_RemoveDuplicates.CoverageForm,
	AGG_RemoveDuplicates.RiskType,
	AGG_RemoveDuplicates.CoverageType,
	AGG_RemoveDuplicates.ClassCode,
	AGG_RemoveDuplicates.Exposure,
	AGG_RemoveDuplicates.TEffectiveDate,
	AGG_RemoveDuplicates.TCreatedDate,
	AGG_RemoveDuplicates.TExpirationDate,
	AGG_RemoveDuplicates.TCancellationDate,
	AGG_RemoveDuplicates.CoverageVersion,
	AGG_RemoveDuplicates.SubLineCode,
	AGG_RemoveDuplicates.PremiumBearingIndicator,
	AGG_RemoveDuplicates.SubLocationUnitNumber,
	AGG_RemoveDuplicates.PerilGroup,
	AGG_RemoveDuplicates.ParentCoverageType,
	AGG_RemoveDuplicates.SubCoverageType,
	AGG_RemoveDuplicates.CoverageDeleteFlag,
	AGG_RemoveDuplicates.OccupancyClassDescription,
	AGG_RemoveDuplicates.ActiveBuildingFlag,
	LKP_SupDCTPolicyOfferingLineOfBusinessProductRules.InsuranceReferenceLineOfBusinessCode,
	LKP_SupDCTPolicyOfferingLineOfBusinessProductRules.ProductCode,
	-- *INF*: IIF(ISNULL(i_StandardSpecialClassGroupCode), 'N/A', LTRIM(RTRIM(i_StandardSpecialClassGroupCode)))
	IFF(i_StandardSpecialClassGroupCode IS NULL,
		'N/A',
		LTRIM(RTRIM(i_StandardSpecialClassGroupCode
			)
		)
	) AS o_SpecialClassGroupCode,
	i_PolicyCoverageAKID AS o_PolicyCoverageAKID,
	AGG_RemoveDuplicates.PolicyKey,
	i_pol_ak_id AS o_PolicyAKID,
	AGG_RemoveDuplicates.RatingCoverageAKID,
	AGG_RemoveDuplicates.RatingCoverageHashKey,
	AGG_RemoveDuplicates.RatingCoverageId,
	AGG_RemoveDuplicates.RatingCoverage_PolicyCoverageAKID,
	AGG_RemoveDuplicates.RatingCoverageEffectivedate,
	AGG_RemoveDuplicates.RatingCoverageExpirationdate
	FROM AGG_RemoveDuplicates
	LEFT JOIN LKP_SupDCTPolicyOfferingLineOfBusinessProductRules
	ON LKP_SupDCTPolicyOfferingLineOfBusinessProductRules.DCTProductCode = AGG_RemoveDuplicates.WBProduct AND LKP_SupDCTPolicyOfferingLineOfBusinessProductRules.DCTProductType = AGG_RemoveDuplicates.WBProductType AND LKP_SupDCTPolicyOfferingLineOfBusinessProductRules.DCTPolicyDivision = AGG_RemoveDuplicates.Division AND LKP_SupDCTPolicyOfferingLineOfBusinessProductRules.DCTLineOfBusinessCode = AGG_RemoveDuplicates.LineType AND LKP_SupDCTPolicyOfferingLineOfBusinessProductRules.DCTCoverageType = AGG_RemoveDuplicates.lkp_InsRefParentCoverageType
	LEFT JOIN LKP_SupSpecialClassGroup
	ON LKP_SupSpecialClassGroup.SpecialClassGroupCode = AGG_RemoveDuplicates.SpecialClassLevel1
	LEFT JOIN LKP_sup_insurance_line
	ON LKP_sup_insurance_line.ins_line_code = AGG_RemoveDuplicates.LineType
),
LKP_ASL AS (
	SELECT
	AnnualStatementLineId,
	SchedulePNumber,
	AnnualStatementLineNumber,
	AnnualStatementLineCode,
	SubAnnualStatementLineNumber,
	SubAnnualStatementLineCode,
	SubNonAnnualStatementLineCode,
	InsuranceLineCode,
	DctRiskTypeCode,
	DctCoverageTypeCode,
	DctPerilGroup,
	DctSubCoverageTypeCode,
	DctCoverageVersion
	FROM (
		SELECT DISTINCT ASLRule.AnnualStatementLineId as AnnualStatementLineId, 
		ASL.SchedulePNumber as SchedulePNumber, 
		ASL.AnnualStatementLineNumber as AnnualStatementLineNumber, 
		ASL.AnnualStatementLineCode as AnnualStatementLineCode, 
		ASL.SubAnnualStatementLineNumber as SubAnnualStatementLineNumber, 
		ASL.SubAnnualStatementLineCode as SubAnnualStatementLineCode, 
		ASL.SubNonAnnualStatementLineCode as SubNonAnnualStatementLineCode, 
		SC.InsuranceLineCode as InsuranceLineCode, 
		SC.DctRiskTypeCode as DctRiskTypeCode, 
		SC.DctCoverageTypeCode as DctCoverageTypeCode, 
		SC.DctPerilGroup as DctPerilGroup, 
		SC.DctSubCoverageTypeCode as DctSubCoverageTypeCode, 
		SC.DctCoverageVersion as DctCoverageVersion 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupDCTAnnualStatementLineRule ASLRule
		join @{pipeline().parameters.TARGET_TABLE_OWNER}.AnnualStatementLine ASL
		on ASLRule.AnnualStatementLineId=ASL.AnnualStatementLineId
		join @{pipeline().parameters.TARGET_TABLE_OWNER}.SystemCoverage SC
		on ASLRule.SystemCoverageId=SC.SystemCoverageId
		WHERE SC.SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceLineCode,DctRiskTypeCode,DctCoverageTypeCode,DctPerilGroup,DctSubCoverageTypeCode,DctCoverageVersion ORDER BY AnnualStatementLineId) = 1
),
LKP_ClassificationReference AS (
	SELECT
	OriginatingOrganizationCode,
	InsuranceLineCode,
	ClassCode
	FROM (
		SELECT DISTINCT ltrim(rtrim(OriginatingOrganizationCode)) as OriginatingOrganizationCode, 
		ltrim(rtrim(InsuranceLineCode)) as InsuranceLineCode, 
		ltrim(rtrim(ClassCode)) as ClassCode 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.ClassificationReference
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceLineCode,ClassCode ORDER BY OriginatingOrganizationCode) = 1
),
LKP_ConformedCoverage AS (
	SELECT
	CoverageSummaryDescription,
	DctCoverageTypeCode,
	DctCoverageVersion,
	DctPerilGroup,
	DctRiskTypeCode,
	DctSubCoverageTypeCode,
	InsuranceLineCode
	FROM (
		SELECT DISTINCT
		CS.CoverageSummaryDescription as CoverageSummaryDescription,
		LTRIM(RTRIM(SC.DctCoverageTypeCode)) as DctCoverageTypeCode,
		LTRIM(RTRIM(SC.DctCoverageVersion)) as DctCoverageVersion,
		LTRIM(RTRIM(SC.DctPerilGroup)) as DctPerilGroup,
		LTRIM(RTRIM(SC.DctRiskTypeCode)) as DctRiskTypeCode,
		LTRIM(RTRIM(SC.DctSubCoverageTypeCode)) as DctSubCoverageTypeCode,
		LTRIM(RTRIM(SC.InsuranceLineCode)) as InsuranceLineCode
		FROM SystemCoverage SC 
		INNER JOIN ConformedCoverage CC ON SC.ConformedCoverageId=CC.ConformedCoverageId
		INNER JOIN CoverageGroup CG ON CC.CoverageGroupId=CG.CoverageGroupId
		INNER JOIN CoverageSummary CS ON CG.CoverageSummaryId=CS.CoverageSummaryId
		WHERE LTRIM(RTRIM(CS.CoverageSummaryDescription)) in 
		('Garage Liability','Garage Physical Damage','Garagekeepers - Dealers','Garage Other')
		AND SC.SourceSystemId='DCT'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY DctCoverageTypeCode,DctCoverageVersion,DctPerilGroup,DctRiskTypeCode,DctSubCoverageTypeCode,InsuranceLineCode ORDER BY CoverageSummaryDescription) = 1
),
EXP_RatingCoverageHashKey AS (
	SELECT
	EXP_CalPolicyCoverageHashKey.o_PolicyCoverageAKID AS i_PolicyCoverageAKID,
	LKP_ClassificationReference.OriginatingOrganizationCode AS i_OriginatingOrganizationCode,
	EXP_CalPolicyCoverageHashKey.StandardInsuranceLineCode,
	EXP_CalPolicyCoverageHashKey.PolicyStatus,
	EXP_CalPolicyCoverageHashKey.Change,
	EXP_CalPolicyCoverageHashKey.TType,
	EXP_CalPolicyCoverageHashKey.CoverageGUID,
	EXP_CalPolicyCoverageHashKey.LineId,
	EXP_CalPolicyCoverageHashKey.RatingCoverageKey,
	EXP_CalPolicyCoverageHashKey.CoverageForm,
	EXP_CalPolicyCoverageHashKey.RiskType,
	EXP_CalPolicyCoverageHashKey.CoverageType,
	EXP_CalPolicyCoverageHashKey.ClassCode,
	EXP_CalPolicyCoverageHashKey.Exposure,
	EXP_CalPolicyCoverageHashKey.TEffectiveDate,
	EXP_CalPolicyCoverageHashKey.TCreatedDate,
	EXP_CalPolicyCoverageHashKey.TExpirationDate,
	EXP_CalPolicyCoverageHashKey.TCancellationDate,
	EXP_CalPolicyCoverageHashKey.CoverageVersion,
	EXP_CalPolicyCoverageHashKey.SubLineCode,
	LKP_ASL.AnnualStatementLineNumber,
	EXP_CalPolicyCoverageHashKey.PremiumBearingIndicator,
	EXP_CalPolicyCoverageHashKey.SubLocationUnitNumber,
	EXP_CalPolicyCoverageHashKey.o_SpecialClassGroupCode AS SpecialClassGroupCode,
	EXP_CalPolicyCoverageHashKey.PerilGroup,
	-- *INF*: IIF(StandardInsuranceLineCode='WC', 'NCCI','ISO')
	-- 
	-- ---IIF(ISNULL(i_OriginatingOrganizationCode), 'N/A', i_OriginatingOrganizationCode)
	IFF(StandardInsuranceLineCode = 'WC',
		'NCCI',
		'ISO'
	) AS o_ClassCodeOrganizationCode,
	-- *INF*: iif(isnull(i_PolicyCoverageAKID),-1,i_PolicyCoverageAKID)
	IFF(i_PolicyCoverageAKID IS NULL,
		- 1,
		i_PolicyCoverageAKID
	) AS o_PolicyCoverageAKID,
	-1 AS o_StatisticalCoverageAKID,
	LKP_ASL.AnnualStatementLineId,
	LKP_ASL.SchedulePNumber,
	LKP_ASL.AnnualStatementLineCode,
	LKP_ASL.SubAnnualStatementLineNumber,
	LKP_ASL.SubAnnualStatementLineCode,
	LKP_ASL.SubNonAnnualStatementLineCode,
	EXP_CalPolicyCoverageHashKey.ParentCoverageType,
	EXP_CalPolicyCoverageHashKey.SubCoverageType,
	EXP_CalPolicyCoverageHashKey.CoverageDeleteFlag,
	EXP_CalPolicyCoverageHashKey.OccupancyClassDescription,
	EXP_CalPolicyCoverageHashKey.ActiveBuildingFlag,
	EXP_CalPolicyCoverageHashKey.PolicyKey,
	LKP_ConformedCoverage.CoverageSummaryDescription AS i_CoverageSummaryDescription,
	EXP_CalPolicyCoverageHashKey.InsuranceReferenceLineOfBusinessCode AS i_InsuranceReferenceLineOfBusinessCode,
	-- *INF*: --IIF(i_InsuranceReferenceLineOfBusinessCode='200' AND i_ProductCode='200' AND NOT ISNULL(i_CoverageSummaryDescription), '340',i_InsuranceReferenceLineOfBusinessCode)
	-- 
	-- DECODE(TRUE,
	-- CoverageForm = 'AutoDealers' AND i_InsuranceReferenceLineOfBusinessCode='330' AND i_ProductCode='200',  '330',
	-- CoverageForm = 'AutoDealers' AND i_InsuranceReferenceLineOfBusinessCode='360' AND i_ProductCode='200',  '360',
	-- CoverageForm = 'AutoDealers', '341',
	-- i_InsuranceReferenceLineOfBusinessCode='200' AND i_ProductCode='200' AND NOT ISNULL(i_CoverageSummaryDescription), '340',
	-- i_InsuranceReferenceLineOfBusinessCode)
	DECODE(TRUE,
		CoverageForm = 'AutoDealers' 
		AND i_InsuranceReferenceLineOfBusinessCode = '330' 
		AND i_ProductCode = '200', '330',
		CoverageForm = 'AutoDealers' 
		AND i_InsuranceReferenceLineOfBusinessCode = '360' 
		AND i_ProductCode = '200', '360',
		CoverageForm = 'AutoDealers', '341',
		i_InsuranceReferenceLineOfBusinessCode = '200' 
		AND i_ProductCode = '200' 
		AND i_CoverageSummaryDescription IS NOT NULL, '340',
		i_InsuranceReferenceLineOfBusinessCode
	) AS o_InsuranceReferenceLineOfBusinessCode,
	EXP_CalPolicyCoverageHashKey.ProductCode AS i_ProductCode,
	-- *INF*: --IIF(i_ProductCode='200' AND NOT ISNULL(i_CoverageSummaryDescription), '340', i_ProductCode)
	-- 
	-- DECODE(TRUE,
	-- CoverageForm = 'AutoDealers', '341',
	-- i_ProductCode='200' AND NOT ISNULL(i_CoverageSummaryDescription), '340',
	--  i_ProductCode)
	DECODE(TRUE,
		CoverageForm = 'AutoDealers', '341',
		i_ProductCode = '200' 
		AND i_CoverageSummaryDescription IS NOT NULL, '340',
		i_ProductCode
	) AS o_ProductCode,
	EXP_CalPolicyCoverageHashKey.o_PolicyAKID,
	EXP_CalPolicyCoverageHashKey.RatingCoverageAKID,
	EXP_CalPolicyCoverageHashKey.RatingCoverageHashKey,
	EXP_CalPolicyCoverageHashKey.RatingCoverageId,
	EXP_CalPolicyCoverageHashKey.RatingCoverage_PolicyCoverageAKID,
	EXP_CalPolicyCoverageHashKey.RatingCoverageEffectivedate,
	EXP_CalPolicyCoverageHashKey.RatingCoverageExpirationdate,
	EXP_CalPolicyCoverageHashKey.PEffectiveDate,
	EXP_CalPolicyCoverageHashKey.PExpirationDate
	FROM EXP_CalPolicyCoverageHashKey
	LEFT JOIN LKP_ASL
	ON LKP_ASL.InsuranceLineCode = EXP_CalPolicyCoverageHashKey.StandardInsuranceLineCode AND LKP_ASL.DctRiskTypeCode = EXP_CalPolicyCoverageHashKey.RiskType AND LKP_ASL.DctCoverageTypeCode = EXP_CalPolicyCoverageHashKey.ParentCoverageType AND LKP_ASL.DctPerilGroup = EXP_CalPolicyCoverageHashKey.PerilGroup AND LKP_ASL.DctSubCoverageTypeCode = EXP_CalPolicyCoverageHashKey.SubCoverageType AND LKP_ASL.DctCoverageVersion = EXP_CalPolicyCoverageHashKey.CoverageVersion
	LEFT JOIN LKP_ClassificationReference
	ON LKP_ClassificationReference.InsuranceLineCode = EXP_CalPolicyCoverageHashKey.StandardInsuranceLineCode AND LKP_ClassificationReference.ClassCode = EXP_CalPolicyCoverageHashKey.ClassCode
	LEFT JOIN LKP_ConformedCoverage
	ON LKP_ConformedCoverage.DctCoverageTypeCode = EXP_CalPolicyCoverageHashKey.ParentCoverageType AND LKP_ConformedCoverage.DctCoverageVersion = EXP_CalPolicyCoverageHashKey.CoverageVersion AND LKP_ConformedCoverage.DctPerilGroup = EXP_CalPolicyCoverageHashKey.PerilGroup AND LKP_ConformedCoverage.DctRiskTypeCode = EXP_CalPolicyCoverageHashKey.RiskType AND LKP_ConformedCoverage.DctSubCoverageTypeCode = EXP_CalPolicyCoverageHashKey.SubCoverageType AND LKP_ConformedCoverage.InsuranceLineCode = EXP_CalPolicyCoverageHashKey.StandardInsuranceLineCode
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
		SELECT
		 T.PolicyNumber+case when len(T.policyversion)=1 then '0'+CONVERT(varchar(1),T.policyversion) 
		 else CONVERT(varchar(2),T.policyversion) end  as Pol_Key,A.CoverageGUID AS CoverageGUID, 
		CASE WHEN CT.PeriodStartDate>T.TransactionEffectiveDate THEN CT.PeriodStartDate
		 WHEN ST.PeriodStartDate>T.TransactionEffectiveDate THEN ST.PeriodStartDate
		 ELSE T.TransactionEffectiveDate END AS EffectiveDate, 
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
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy O1
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
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCoverageStage WBC
		ON WBC.CoverageId=A.CoverageId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBWCCoverageTermStage CT
		ON CT.WB_CoverageId=WBC.WBCoverageId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCWCStateTermStaging ST
		ON ST.WC_StateTermId=A.ParentCoverageObjectId
		AND A.ParentCoverageObjectName='DC_WC_StateTerm'
		ORDER BY T.PolicyNumber+case when len(T.policyversion)=1 then '0'+convert(varchar(1),T.policyversion) else convert(varchar(2),T.policyversion) end 
		,A.CoverageGUID,CASE WHEN CT.PeriodStartDate>T.TransactionEffectiveDate THEN CT.PeriodStartDate WHEN ST.PeriodStartDate>T.TransactionEffectiveDate THEN ST.PeriodStartDate ELSE T.TransactionEffectiveDate END, T.TransactionCreatedDate, ISNULL(F.OffsetCreatedDate, '2100-12-31 23:59:59'), A.CoverageId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Pol_Key,CoverageGUID,EffectiveDate,CreatedDate,OffsetCreatedDate ORDER BY CoverageDeleteFlag DESC) = 1
),
LKP_InsuranceReferenceLineOfBusiness AS (
	SELECT
	InsuranceReferenceLineOfBusinessAKId,
	InsuranceReferenceLineOfBusinessCode
	FROM (
		SELECT 
			InsuranceReferenceLineOfBusinessAKId,
			InsuranceReferenceLineOfBusinessCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceLineOfBusiness
		WHERE CurrentSnapshotFlag='1'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceReferenceLineOfBusinessCode ORDER BY InsuranceReferenceLineOfBusinessAKId) = 1
),
LKP_Product AS (
	SELECT
	ProductAKId,
	ProductCode
	FROM (
		SELECT 
			ProductAKId,
			ProductCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.Product
		WHERE CurrentSnapshotFlag='1'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProductCode ORDER BY ProductAKId) = 1
),
LKP_RatingCoverage AS (
	SELECT
	RatingCoverageCancellationDate,
	PremiumTransactionCode,
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
		PC.PolicyAKID as PolicyAKID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC
		INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage a on PC.PolicyCoverageAKID=a.PolicyCoverageAKID
		AND PC.SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND pc.CurrentSnapshotFlag=1
		INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction b on a.RatingCoverageAKId=b.RatingCoverageAKid
		AND b.EffectiveDate=a.EffectiveDate AND b.SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND not b.OffsetOnsetCode in ('Offset','Deprecated')
		LEFT JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction c on c.RatingCoverageAKId=b.RatingCoverageAKid AND c.EffectiveDate=a.EffectiveDate
		AND c.SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND c.OffsetOnsetCode='Deprecated'
		INNER JOIN (
		select DISTINCT WCT.CoverageGUId from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTCoverageTransaction WCT) WCT
		on WCT.CoverageGUID=a.CoverageGUID
		order by PC.PolicyAKID,a.CoverageGUID,b.PremiumTransactionEffectiveDate,
		b.PremiumTransactionEnteredDate,ISNULL(c.PremiumTransactionEnteredDate,'2100-12-31 23:59:59')
		
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageGUID,PolicyAKID,TEffectiveDate,TCreatedDate,OffsetCreatedDate ORDER BY RatingCoverageCancellationDate DESC) = 1
),
EXP_CoverageCancellationDate AS (
	SELECT
	LKP_DCCoverageStaging.CoverageDeleteFlag AS lkp_StageCoverageDeleteFlag,
	LKP_DCCoverageStaging.Type AS lkp_StageTransactionType,
	LKP_RatingCoverage.RatingCoverageCancellationDate AS lkp_RatingCoverageCancellationDate,
	LKP_RatingCoverage.PremiumTransactionCode AS lkp_PremiumTransactionCode,
	EXP_RatingCoverageHashKey.PolicyStatus,
	EXP_RatingCoverageHashKey.Change,
	EXP_RatingCoverageHashKey.TType,
	EXP_RatingCoverageHashKey.CoverageGUID,
	EXP_RatingCoverageHashKey.LineId,
	EXP_RatingCoverageHashKey.RatingCoverageKey,
	EXP_RatingCoverageHashKey.CoverageForm,
	EXP_RatingCoverageHashKey.RiskType,
	EXP_RatingCoverageHashKey.CoverageType,
	EXP_RatingCoverageHashKey.ClassCode,
	EXP_RatingCoverageHashKey.Exposure,
	EXP_RatingCoverageHashKey.TEffectiveDate,
	EXP_RatingCoverageHashKey.TCreatedDate,
	EXP_RatingCoverageHashKey.TExpirationDate,
	EXP_RatingCoverageHashKey.TCancellationDate AS i_TCancellationDate,
	EXP_RatingCoverageHashKey.CoverageVersion,
	EXP_RatingCoverageHashKey.SubLineCode,
	EXP_RatingCoverageHashKey.AnnualStatementLineNumber,
	EXP_RatingCoverageHashKey.PremiumBearingIndicator,
	LKP_Product.ProductAKId,
	LKP_InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessAKId,
	EXP_RatingCoverageHashKey.SubLocationUnitNumber,
	EXP_RatingCoverageHashKey.SpecialClassGroupCode,
	EXP_RatingCoverageHashKey.PerilGroup,
	EXP_RatingCoverageHashKey.o_ClassCodeOrganizationCode,
	EXP_RatingCoverageHashKey.o_PolicyCoverageAKID,
	EXP_RatingCoverageHashKey.o_StatisticalCoverageAKID,
	EXP_RatingCoverageHashKey.AnnualStatementLineId,
	EXP_RatingCoverageHashKey.SchedulePNumber,
	EXP_RatingCoverageHashKey.AnnualStatementLineCode,
	EXP_RatingCoverageHashKey.SubAnnualStatementLineNumber,
	EXP_RatingCoverageHashKey.SubAnnualStatementLineCode,
	EXP_RatingCoverageHashKey.SubNonAnnualStatementLineCode,
	EXP_RatingCoverageHashKey.ParentCoverageType,
	EXP_RatingCoverageHashKey.SubCoverageType,
	EXP_RatingCoverageHashKey.CoverageDeleteFlag,
	EXP_RatingCoverageHashKey.OccupancyClassDescription,
	EXP_RatingCoverageHashKey.ActiveBuildingFlag,
	-- *INF*: DECODE(TRUE,
	-- CoverageDeleteFlag='1',1,
	-- PolicyStatus='Cancelled',1,
	-- 0)
	DECODE(TRUE,
		CoverageDeleteFlag = '1', 1,
		PolicyStatus = 'Cancelled', 1,
		0
	) AS v_RatingCoverageCancellationFlag,
	-- *INF*: IIF(v_RatingCoverageCancellationFlag=1,LEAST(TEffectiveDate,i_TCancellationDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'))
	IFF(v_RatingCoverageCancellationFlag = 1,
		LEAST(TEffectiveDate, i_TCancellationDate
		),
		TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'
		)
	) AS o_RatingCoverageCancellationDate,
	-- *INF*: DECODE(TRUE,
	-- CoverageDeleteFlag='0',1,
	-- CoverageDeleteFlag='1' AND lkp_StageCoverageDeleteFlag='0',1,
	-- CoverageDeleteFlag='1' AND lkp_RatingCoverageCancellationDate>=TO_DATE('21001231','YYYYMMDD'),1,
	-- Change<>0,1,
	-- 0)
	DECODE(TRUE,
		CoverageDeleteFlag = '0', 1,
		CoverageDeleteFlag = '1' 
		AND lkp_StageCoverageDeleteFlag = '0', 1,
		CoverageDeleteFlag = '1' 
		AND lkp_RatingCoverageCancellationDate >= TO_DATE('21001231', 'YYYYMMDD'
		), 1,
		Change <> 0, 1,
		0
	) AS o_FilterFlag,
	EXP_RatingCoverageHashKey.o_PolicyAKID,
	EXP_RatingCoverageHashKey.RatingCoverageAKID,
	EXP_RatingCoverageHashKey.RatingCoverageHashKey,
	EXP_RatingCoverageHashKey.RatingCoverageId,
	EXP_RatingCoverageHashKey.RatingCoverage_PolicyCoverageAKID,
	EXP_RatingCoverageHashKey.RatingCoverageEffectivedate,
	EXP_RatingCoverageHashKey.RatingCoverageExpirationdate,
	EXP_RatingCoverageHashKey.PEffectiveDate,
	EXP_RatingCoverageHashKey.PExpirationDate
	FROM EXP_RatingCoverageHashKey
	LEFT JOIN LKP_DCCoverageStaging
	ON LKP_DCCoverageStaging.Pol_Key = EXP_RatingCoverageHashKey.PolicyKey AND LKP_DCCoverageStaging.CoverageGUID = EXP_RatingCoverageHashKey.CoverageGUID AND LKP_DCCoverageStaging.EffectiveDate <= EXP_RatingCoverageHashKey.TEffectiveDate AND LKP_DCCoverageStaging.CreatedDate < EXP_RatingCoverageHashKey.TCreatedDate AND LKP_DCCoverageStaging.OffsetCreatedDate > EXP_RatingCoverageHashKey.TCreatedDate
	LEFT JOIN LKP_InsuranceReferenceLineOfBusiness
	ON LKP_InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessCode = EXP_RatingCoverageHashKey.o_InsuranceReferenceLineOfBusinessCode
	LEFT JOIN LKP_Product
	ON LKP_Product.ProductCode = EXP_RatingCoverageHashKey.o_ProductCode
	LEFT JOIN LKP_RatingCoverage
	ON LKP_RatingCoverage.CoverageGUID = EXP_RatingCoverageHashKey.CoverageGUID AND LKP_RatingCoverage.PolicyAKID = EXP_RatingCoverageHashKey.o_PolicyAKID AND LKP_RatingCoverage.TEffectiveDate <= EXP_RatingCoverageHashKey.TEffectiveDate AND LKP_RatingCoverage.TCreatedDate < EXP_RatingCoverageHashKey.TCreatedDate AND LKP_RatingCoverage.OffsetCreatedDate > EXP_RatingCoverageHashKey.TCreatedDate
),
FIL_DefaultCoveragesAndPastTransactions AS (
	SELECT
	TType, 
	CoverageGUID, 
	LineId, 
	RatingCoverageKey, 
	CoverageForm, 
	RiskType, 
	CoverageType, 
	ClassCode, 
	Exposure, 
	TEffectiveDate, 
	TCreatedDate, 
	TExpirationDate, 
	CoverageVersion, 
	SubLineCode, 
	AnnualStatementLineNumber, 
	PremiumBearingIndicator, 
	ProductAKId, 
	InsuranceReferenceLineOfBusinessAKId, 
	SubLocationUnitNumber, 
	SpecialClassGroupCode, 
	PerilGroup, 
	o_ClassCodeOrganizationCode, 
	o_PolicyCoverageAKID, 
	o_StatisticalCoverageAKID, 
	AnnualStatementLineId, 
	SchedulePNumber, 
	AnnualStatementLineCode, 
	SubAnnualStatementLineNumber, 
	SubAnnualStatementLineCode, 
	SubNonAnnualStatementLineCode, 
	ParentCoverageType, 
	SubCoverageType, 
	CoverageDeleteFlag, 
	OccupancyClassDescription, 
	ActiveBuildingFlag, 
	o_RatingCoverageCancellationDate AS RatingCoverageCancellationDate, 
	o_FilterFlag AS FilterFlag, 
	o_PolicyAKID, 
	RatingCoverageAKID, 
	RatingCoverageHashKey, 
	RatingCoverageId, 
	RatingCoverage_PolicyCoverageAKID, 
	RatingCoverageEffectivedate, 
	RatingCoverageExpirationdate, 
	PEffectiveDate, 
	PExpirationDate
	FROM EXP_CoverageCancellationDate
	WHERE FilterFlag=1
),
SRT_CoverageGUID_SessionID AS (
	SELECT
	TType, 
	LineId, 
	RatingCoverageKey, 
	CoverageForm, 
	RiskType, 
	CoverageType, 
	ClassCode, 
	Exposure, 
	TEffectiveDate, 
	o_PolicyAKID AS PolicyAKID, 
	CoverageGUID, 
	TCreatedDate, 
	o_PolicyCoverageAKID AS PolicyCoverageAKID, 
	TExpirationDate, 
	CoverageVersion, 
	SubLineCode, 
	AnnualStatementLineNumber, 
	PremiumBearingIndicator, 
	ProductAKId, 
	InsuranceReferenceLineOfBusinessAKId, 
	SubLocationUnitNumber, 
	SpecialClassGroupCode, 
	o_ClassCodeOrganizationCode AS ClassCodeOrganizationCode, 
	o_StatisticalCoverageAKID AS StatisticalCoverageAKID, 
	AnnualStatementLineId, 
	PerilGroup, 
	SchedulePNumber, 
	AnnualStatementLineCode, 
	SubAnnualStatementLineNumber, 
	SubAnnualStatementLineCode, 
	SubNonAnnualStatementLineCode, 
	ParentCoverageType, 
	SubCoverageType, 
	CoverageDeleteFlag, 
	OccupancyClassDescription, 
	ActiveBuildingFlag, 
	RatingCoverageCancellationDate, 
	RatingCoverageAKID, 
	RatingCoverageHashKey, 
	RatingCoverageId, 
	RatingCoverage_PolicyCoverageAKID, 
	RatingCoverageEffectivedate, 
	RatingCoverageExpirationdate, 
	PEffectiveDate, 
	PExpirationDate
	FROM FIL_DefaultCoveragesAndPastTransactions
	ORDER BY PolicyAKID ASC, CoverageGUID ASC, TCreatedDate ASC
),
EXP_CalValues AS (
	SELECT
	RatingCoverage_PolicyCoverageAKID AS lkp_PolicyCoverageAKID,
	SEQ_RatingCoverageAKID.NEXTVAL AS i_NEXTVAL,
	RatingCoverageId AS lkp_RatingCoverageId,
	RatingCoverageAKID AS lkp_RatingCoverageAKID,
	RatingCoverageHashKey AS lkp_RatingCoverageHashKey,
	TType AS i_TType,
	CoverageGUID AS i_CoverageGUID,
	TEffectiveDate AS i_TEffectiveDate,
	LineId AS i_LineId,
	RatingCoverageKey AS i_RatingCoverageKey,
	CoverageForm AS i_CoverageForm,
	RiskType AS i_RiskType,
	CoverageType AS i_CoverageType,
	ClassCode AS i_ClassCode,
	Exposure AS i_Exposure,
	TCreatedDate AS i_TCreatedDate,
	TExpirationDate AS i_TExpirationDate,
	CoverageVersion AS i_CoverageVersion,
	SubLineCode AS i_SubLineCode,
	AnnualStatementLineNumber AS i_AnnualStatementLineNumber,
	PremiumBearingIndicator AS i_PremiumBearingIndicator,
	ProductAKId AS i_ProductAKId,
	InsuranceReferenceLineOfBusinessAKId AS i_InsuranceReferenceLineOfBusinessAKId,
	SubLocationUnitNumber AS i_SubLocationUnitNumber,
	SpecialClassGroupCode AS i_SpecialClassGroupCode,
	ClassCodeOrganizationCode AS i_ClassCodeOrganizationCode,
	PolicyCoverageAKID AS i_PolicyCoverageAKID,
	StatisticalCoverageAKID AS i_StatisticalCoverageAKID,
	AnnualStatementLineId AS i_AnnualStatementLineId,
	PerilGroup AS i_PerilGroup,
	PolicyAKID,
	-- *INF*: MD5(i_ClassCode
	-- ||i_RiskType
	-- ||TO_CHAR(i_Exposure)
	-- ||TO_CHAR(RatingCoverageCancellationDate)
	-- ||i_SubLineCode
	-- ||i_AnnualStatementLineNumber
	-- ||TO_CHAR(i_PremiumBearingIndicator)
	-- --||TO_CHAR(i_AnnualStatementLineId)
	-- ||i_SubLocationUnitNumber
	-- ||i_SpecialClassGroupCode
	-- ||i_ClassCodeOrganizationCode
	-- ||i_PerilGroup
	-- ||OccupancyClassDescription
	-- ||ActiveBuildingFlag)
	MD5(i_ClassCode || i_RiskType || TO_CHAR(i_Exposure
		) || TO_CHAR(RatingCoverageCancellationDate
		) || i_SubLineCode || i_AnnualStatementLineNumber || TO_CHAR(i_PremiumBearingIndicator
		) || i_SubLocationUnitNumber || i_SpecialClassGroupCode || i_ClassCodeOrganizationCode || i_PerilGroup || OccupancyClassDescription || ActiveBuildingFlag
	) AS v_RatingCoverageHashKey,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_RatingCoverageId) ,'New', 
	-- lkp_RatingCoverageHashKey!=v_RatingCoverageHashKey,'Change',
	-- 'NoChange')
	DECODE(TRUE,
		lkp_RatingCoverageId IS NULL, 'New',
		lkp_RatingCoverageHashKey != v_RatingCoverageHashKey, 'Change',
		'NoChange'
	) AS v_ChangeFlag,
	-- *INF*: IIF(PolicyAKID=v_prev_PolicyAKID and i_CoverageGUID=v_prev_CoverageGUID, v_prev_NEXTVAL, i_NEXTVAL)
	IFF(PolicyAKID = v_prev_PolicyAKID 
		AND i_CoverageGUID = v_prev_CoverageGUID,
		v_prev_NEXTVAL,
		i_NEXTVAL
	) AS v_NEXTVAL,
	-- *INF*: IIF(PolicyAKID=v_prev_PolicyAKID and i_CoverageGUID=v_prev_CoverageGUID, v_Seq+1, 1)
	IFF(PolicyAKID = v_prev_PolicyAKID 
		AND i_CoverageGUID = v_prev_CoverageGUID,
		v_Seq + 1,
		1
	) AS v_Seq,
	v_NEXTVAL AS v_prev_NEXTVAL,
	i_CoverageGUID AS v_prev_CoverageGUID,
	i_PolicyCoverageAKID AS v_prev_PolicyCoverageAKID,
	PolicyAKID AS v_prev_PolicyAKID,
	v_ChangeFlag AS o_ChangeFlag,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	i_TCreatedDate AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	0 AS o_LogicalIndicator,
	v_RatingCoverageHashKey AS o_RatingCoverageHashKey,
	-- *INF*: IIF(v_ChangeFlag='New' AND ISNULL(lkp_RatingCoverageAKID),v_NEXTVAL,lkp_RatingCoverageAKID)
	IFF(v_ChangeFlag = 'New' 
		AND lkp_RatingCoverageAKID IS NULL,
		v_NEXTVAL,
		lkp_RatingCoverageAKID
	) AS o_RatingCoverageAKID,
	i_StatisticalCoverageAKID AS o_StatisticalCoverageAKID,
	-- *INF*: iif(isnull(lkp_PolicyCoverageAKID),i_PolicyCoverageAKID,lkp_PolicyCoverageAKID)
	IFF(lkp_PolicyCoverageAKID IS NULL,
		i_PolicyCoverageAKID,
		lkp_PolicyCoverageAKID
	) AS o_PolicyCoverageAKID,
	i_RatingCoverageKey AS o_RatingCoverageKey,
	i_CoverageForm AS o_CoverageForm,
	i_ClassCode AS o_ClassCode,
	i_RiskType AS o_RiskType,
	ParentCoverageType,
	i_Exposure AS o_Exposure,
	i_CoverageVersion AS o_CoverageVersion,
	i_CoverageGUID AS o_CoverageGUID,
	RatingCoverageCancellationDate,
	i_SubLineCode AS o_SubLineCode,
	i_AnnualStatementLineNumber AS o_AnnualStatementLineNumber,
	i_PremiumBearingIndicator AS o_PremiumBearingIndicator,
	i_ProductAKId AS o_ProductAKId,
	i_InsuranceReferenceLineOfBusinessAKId AS o_InsuranceReferenceLineOfBusinessAKId,
	i_SubLocationUnitNumber AS o_SubLocationUnitNumber,
	i_SpecialClassGroupCode AS o_SpecialClassGroupCode,
	i_AnnualStatementLineId AS o_AnnualStatementLineId,
	i_ClassCodeOrganizationCode AS o_ClassCodeOrganizationCode,
	i_PerilGroup AS o_PerilGroup,
	SchedulePNumber,
	AnnualStatementLineCode,
	SubAnnualStatementLineNumber,
	SubAnnualStatementLineCode,
	SubNonAnnualStatementLineCode,
	SubCoverageType AS SubCoverageTypeCode,
	OccupancyClassDescription,
	ActiveBuildingFlag,
	PEffectiveDate AS i_PEffectiveDate,
	PExpirationDate AS i_PExpirationDate,
	RatingCoverageEffectivedate AS lkp_RatingCoverageEffectivedate,
	RatingCoverageExpirationdate AS lkp_RatingCoverageExpirationdate,
	-- *INF*: DECODE(TRUE,v_Seq=1 AND not isnull(lkp_RatingCoverageAKID),lkp_RatingCoverageEffectivedate,
	--                        v_Seq=1 AND  isnull(lkp_RatingCoverageAKID),i_TEffectiveDate,v_RatingCoverageEffectiveDate)
	-- --TO_DATE('18000101','YYYYMMDD')
	DECODE(TRUE,
		v_Seq = 1 
		AND lkp_RatingCoverageAKID IS NOT NULL, lkp_RatingCoverageEffectivedate,
		v_Seq = 1 
		AND lkp_RatingCoverageAKID IS NULL, i_TEffectiveDate,
		v_RatingCoverageEffectiveDate
	) AS v_RatingCoverageEffectiveDate,
	-- *INF*: DECODE(TRUE,v_Seq=1 AND not isnull(lkp_RatingCoverageAKID),lkp_RatingCoverageExpirationdate,
	--                        v_Seq=1 AND  isnull(lkp_RatingCoverageAKID),i_TExpirationDate,v_RatingCoverageExpirationDate)
	-- 
	-- --TO_DATE('21001231235959','YYYYMMDDHH24MISS')
	DECODE(TRUE,
		v_Seq = 1 
		AND lkp_RatingCoverageAKID IS NOT NULL, lkp_RatingCoverageExpirationdate,
		v_Seq = 1 
		AND lkp_RatingCoverageAKID IS NULL, i_TExpirationDate,
		v_RatingCoverageExpirationDate
	) AS v_RatingCoverageExpirationDate,
	-- *INF*: IIF(ISNULL(v_RatingCoverageEffectiveDate),i_PEffectiveDate,v_RatingCoverageEffectiveDate)
	-- --IIF(ISNULL(v_RatingCoverageEffectiveDate),TO_DATE('18000101','YYYYMMDD'),v_RatingCoverageEffectiveDate)
	IFF(v_RatingCoverageEffectiveDate IS NULL,
		i_PEffectiveDate,
		v_RatingCoverageEffectiveDate
	) AS o_RatingCoverageEffectiveDate,
	-- *INF*: IIF(ISNULL(v_RatingCoverageExpirationDate), i_PExpirationDate,v_RatingCoverageExpirationDate)
	-- --IIF(ISNULL(v_RatingCoverageExpirationDate), TO_DATE('21001231235959','YYYYMMDDHH24MISS'),v_RatingCoverageExpirationDate)
	-- 
	IFF(v_RatingCoverageExpirationDate IS NULL,
		i_PExpirationDate,
		v_RatingCoverageExpirationDate
	) AS o_RatingCoverageExpirationDate
	FROM SRT_CoverageGUID_SessionID
),
RTR_INSERT_UPDATE AS (
	SELECT
	lkp_RatingCoverageId,
	o_ChangeFlag AS i_ChangeFlag,
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	o_AuditID AS AuditID,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_SourceSystemID AS SourceSystemID,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_LogicalIndicator AS LogicalIndicator,
	o_RatingCoverageHashKey AS RatingCoverageHashKey,
	o_RatingCoverageAKID AS RatingCoverageAKID,
	o_StatisticalCoverageAKID AS StatisticalCoverageAKID,
	o_PolicyCoverageAKID AS PolicyCoverageAKID,
	o_RatingCoverageKey AS RatingCoverageKey,
	o_CoverageForm AS CoverageForm,
	o_ClassCode AS ClassCode,
	o_RiskType AS RiskType,
	ParentCoverageType AS CoverageType,
	o_Exposure AS Exposure,
	o_RatingCoverageEffectiveDate AS RatingCoverageEffectiveDate,
	o_RatingCoverageExpirationDate AS RatingCoverageExpirationDate,
	o_CoverageVersion AS CoverageVersion,
	o_CoverageGUID AS CoverageGUID,
	RatingCoverageCancellationDate,
	o_SubLineCode AS SubLineCode,
	o_AnnualStatementLineNumber AS ASLNum,
	o_PremiumBearingIndicator AS PremiumBearingIndicator,
	o_ProductAKId AS ProductAKId,
	o_InsuranceReferenceLineOfBusinessAKId AS InsuranceReferenceLineOfBusinessAKId,
	o_SubLocationUnitNumber AS SubLocationUnitNumber,
	o_SpecialClassGroupCode AS SpecialClassGroupCode,
	o_AnnualStatementLineId AS AnnualStatementLineId,
	o_ClassCodeOrganizationCode AS ClassCodeOrganizationCode,
	o_PerilGroup AS PerilGroup,
	SchedulePNumber,
	AnnualStatementLineCode,
	SubAnnualStatementLineNumber,
	SubAnnualStatementLineCode,
	SubNonAnnualStatementLineCode,
	SubCoverageTypeCode,
	OccupancyClassDescription,
	ActiveBuildingFlag
	FROM EXP_CalValues
),
RTR_INSERT_UPDATE_INSERT AS (SELECT * FROM RTR_INSERT_UPDATE WHERE i_ChangeFlag='New'),
RTR_INSERT_UPDATE_UPDATE AS (SELECT * FROM RTR_INSERT_UPDATE WHERE i_ChangeFlag='Change'),
UPD_DueToCodeChange AS (
	SELECT
	lkp_RatingCoverageId, 
	ModifiedDate, 
	LogicalIndicator, 
	RatingCoverageHashKey, 
	CoverageForm, 
	ClassCode, 
	RiskType, 
	CoverageType, 
	Exposure, 
	CoverageVersion, 
	RatingCoverageCancellationDate AS RatingCoverageCancellationDate3, 
	SubLineCode, 
	ASLNum, 
	PremiumBearingIndicator, 
	ProductAKId, 
	InsuranceReferenceLineOfBusinessAKId, 
	SubLocationUnitNumber, 
	SpecialClassGroupCode, 
	AnnualStatementLineId, 
	ClassCodeOrganizationCode, 
	PerilGroup, 
	SchedulePNumber, 
	AnnualStatementLineCode, 
	SubAnnualStatementLineNumber, 
	SubAnnualStatementLineCode, 
	SubNonAnnualStatementLineCode, 
	SubCoverageTypeCode, 
	OccupancyClassDescription, 
	ActiveBuildingFlag
	FROM RTR_INSERT_UPDATE_UPDATE
),
TGT_RatingCoverage_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage AS T
	USING UPD_DueToCodeChange AS S
	ON T.RatingCoverageId = S.lkp_RatingCoverageId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.LogicalIndicator = S.LogicalIndicator, T.RatingCoverageHashKey = S.RatingCoverageHashKey, T.CoverageForm = S.CoverageForm, T.ClassCode = S.ClassCode, T.RiskType = S.RiskType, T.CoverageType = S.CoverageType, T.Exposure = S.Exposure, T.CoverageVersion = S.CoverageVersion, T.SublineCode = S.SubLineCode, T.AnnualStatementLineNumber = S.ASLNum, T.PremiumBearingIndicator = S.PremiumBearingIndicator, T.ProductAKId = S.ProductAKId, T.InsuranceReferenceLineOfBusinessAKId = S.InsuranceReferenceLineOfBusinessAKId, T.SubLocationUnitNumber = S.SubLocationUnitNumber, T.SpecialClassGroupCode = S.SpecialClassGroupCode, T.AnnualStatementLineId = S.AnnualStatementLineId, T.ClassCodeOrganizationCode = S.ClassCodeOrganizationCode, T.PerilGroup = S.PerilGroup, T.SchedulePNumber = S.SchedulePNumber, T.AnnualStatementLineCode = S.AnnualStatementLineCode, T.SubAnnualStatementLineNumber = S.SubAnnualStatementLineNumber, T.SubAnnualStatementLineCode = S.SubAnnualStatementLineCode, T.SubNonAnnualStatementLineCode = S.SubNonAnnualStatementLineCode, T.SubCoverageTypeCode = S.SubCoverageTypeCode, T.OccupancyClassDescription = S.OccupancyClassDescription, T.ActiveBuildingFlag = S.ActiveBuildingFlag
),
TGT_RatingCoverage_Insert AS (

	------------ PRE SQL ----------
	update RC
	set RC.AnnualStatementLineId=ASL.AnnualStatementLineId,
	RC.AnnualStatementLineNumber=ASL.AnnualStatementLineNumber,
	RC.AnnualStatementLineCode=ASL.AnnualStatementLineCode,
	RC.SubAnnualStatementLineCode=ASL.SubAnnualStatementLineCode,
	RC.SubAnnualStatementLineNumber=ASL.SubAnnualStatementLineNumber,
	RC.SubNonAnnualStatementLineCode=ASL.SubNonAnnualStatementLineCode,
	RC.SchedulePNumber=ASL.SchedulePNumber,
	RC.RatingCoverageHashKey=convert(varchar(max),hashbytes('MD5',convert(varchar(max),RC.ClassCode+RC.RiskType+convert(varchar(max),RC.Exposure)+
	Convert(CHAR(10),RC.RatingCoverageCancellationDate,101) + ' ' + Convert(CHAR(8),RC.RatingCoverageCancellationDate,108)
	+RC.SubLineCode+ASL.AnnualStatementLineNumber+CONVERT(varchar(max),RC.PremiumBearingIndicator)+RC.SubLocationUnitNumber
	+RC.SpecialClassGroupCode+RC.ClassCodeOrganizationCode+RC.PerilGroup+RC.OccupancyClassDescription+convert(varchar(1),RC.ActiveBuildingFlag))),2)
	from @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC
	join @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC
	on RC.PolicyCoverageAKID=PC.PolicyCoverageAKID and PC.CurrentSnapshotFlag=1
	left join @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_insurance_line SIL
	on PC.InsuranceLine=SIL.ins_line_code and SIL.crrnt_snpsht_flag=1
	join @{pipeline().parameters.TARGET_TABLE_OWNER}.SystemCoverage SC
	on RC.CoverageType=SC.DctCoverageTypeCode
	and RC.RiskType=SC.DctRiskTypeCode
	and RC.CoverageVersion=SC.DctCoverageVersion
	and RC.PerilGroup=SC.DctPerilGroup
	and RC.SubCoverageTypeCode=SC.DctSubCoverageTypeCode
	and isnull(SIL.StandardInsuranceLineCode, 'N/A')=SC.InsuranceLineCode
	join @{pipeline().parameters.TARGET_TABLE_OWNER}.SupDCTAnnualStatementLineRule R
	on SC.SystemCoverageId=R.SystemCoverageId
	join @{pipeline().parameters.TARGET_TABLE_OWNER}.AnnualStatementLine ASL
	on R.AnnualStatementLineId=ASL.AnnualStatementLineId
	where RC.AnnualStatementLineId=-1 and ASL.AnnualStatementLineId is not null
	-------------------------------


	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, LogicalIndicator, RatingCoverageHashKey, RatingCoverageAKID, StatisticalCoverageAKID, PolicyCoverageAKID, RatingCoverageKey, CoverageForm, ClassCode, RiskType, CoverageType, Exposure, RatingCoverageEffectiveDate, RatingCoverageExpirationDate, CoverageVersion, CoverageGUID, RatingCoverageCancellationDate, SublineCode, AnnualStatementLineNumber, PremiumBearingIndicator, ProductAKId, InsuranceReferenceLineOfBusinessAKId, SubLocationUnitNumber, SpecialClassGroupCode, AnnualStatementLineId, ClassCodeOrganizationCode, PerilGroup, SchedulePNumber, AnnualStatementLineCode, SubAnnualStatementLineNumber, SubAnnualStatementLineCode, SubNonAnnualStatementLineCode, SubCoverageTypeCode, OccupancyClassDescription, ActiveBuildingFlag)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	LOGICALINDICATOR, 
	RATINGCOVERAGEHASHKEY, 
	RATINGCOVERAGEAKID, 
	STATISTICALCOVERAGEAKID, 
	POLICYCOVERAGEAKID, 
	RATINGCOVERAGEKEY, 
	COVERAGEFORM, 
	CLASSCODE, 
	RISKTYPE, 
	COVERAGETYPE, 
	EXPOSURE, 
	RATINGCOVERAGEEFFECTIVEDATE, 
	RATINGCOVERAGEEXPIRATIONDATE, 
	COVERAGEVERSION, 
	COVERAGEGUID, 
	RATINGCOVERAGECANCELLATIONDATE, 
	SubLineCode AS SUBLINECODE, 
	ASLNum AS ANNUALSTATEMENTLINENUMBER, 
	PREMIUMBEARINGINDICATOR, 
	PRODUCTAKID, 
	INSURANCEREFERENCELINEOFBUSINESSAKID, 
	SUBLOCATIONUNITNUMBER, 
	SPECIALCLASSGROUPCODE, 
	ANNUALSTATEMENTLINEID, 
	CLASSCODEORGANIZATIONCODE, 
	PERILGROUP, 
	SCHEDULEPNUMBER, 
	ANNUALSTATEMENTLINECODE, 
	SUBANNUALSTATEMENTLINENUMBER, 
	SUBANNUALSTATEMENTLINECODE, 
	SUBNONANNUALSTATEMENTLINECODE, 
	SUBCOVERAGETYPECODE, 
	OCCUPANCYCLASSDESCRIPTION, 
	ACTIVEBUILDINGFLAG
	FROM RTR_INSERT_UPDATE_INSERT
),
SQ_RatingCoverage_UPDATE AS (
	SELECT RC.RatingCoverageId, RC.EffectiveDate, RC.ExpirationDate, RC.RatingCoverageAKID,PC.Policyakid 
	FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC
	 inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC
	on PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
	and PC.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	and PC.CurrentSnapshotFlag=1
	WHERE EXISTS (SELECT RC1.RatingCoverageAKID
	FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC1 
	inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC1
	on PC1.PolicyCoverageAKID=RC1.PolicyCoverageAKID
	and PC1.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	and PC1.CurrentSnapshotFlag=1
	WHERE RC1.CurrentSnapshotFlag = 1 AND RC1.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND RC1.RatingCoverageAKID=RC.RatingCoverageAKID and PC1.PolicyAKID=PC.PolicyAKID
	GROUP BY PC1.PolicyAKID,RC1.RatingCoverageAKID HAVING COUNT(*)>1)
	ORDER BY PC.PolicyAKID,RatingCoverageAKID, EffectiveDate DESC
),
EXP_GetDates AS (
	SELECT
	RatingCoverageId AS i_RatingCoverageId,
	EffectiveDate AS i_EffectiveDate,
	ExpirationDate AS i_ExpirationDate,
	RatingCoverageAKID AS i_RatingCoverageAKID,
	PolicyAKID AS i_PolicyAKID,
	-- *INF*: IIF(i_PolicyAKID=v_Prev_PolicyAKID and i_RatingCoverageAKID = v_Prev_RatingCoverageAKID,ADD_TO_DATE(v_Prev_EffectiveDate,'SS',-1),i_ExpirationDate)
	IFF(i_PolicyAKID = v_Prev_PolicyAKID 
		AND i_RatingCoverageAKID = v_Prev_RatingCoverageAKID,
		DATEADD(SECOND,- 1,v_Prev_EffectiveDate),
		i_ExpirationDate
	) AS v_ExpirationDate,
	i_PolicyAKID AS v_Prev_PolicyAKID,
	i_RatingCoverageAKID AS v_Prev_RatingCoverageAKID,
	i_EffectiveDate AS v_Prev_EffectiveDate,
	i_ExpirationDate AS o_Orig_ExpirationDate,
	i_RatingCoverageId AS o_RatingCoverageId,
	'0' AS o_CurrentSnapshotFlag,
	v_ExpirationDate AS o_ExpirationDate,
	SYSDATE AS o_ModifiedDate
	FROM SQ_RatingCoverage_UPDATE
),
FIL_FirstRowInAKIDGroup AS (
	SELECT
	o_Orig_ExpirationDate AS i_Orig_ExpirationDate, 
	o_RatingCoverageId AS RatingCoverageId, 
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag, 
	o_ExpirationDate AS ExpirationDate, 
	o_ModifiedDate AS ModifiedDate
	FROM EXP_GetDates
	WHERE i_Orig_ExpirationDate  !=  ExpirationDate
),
UPD_ExpiratedRecords AS (
	SELECT
	RatingCoverageId, 
	CurrentSnapshotFlag, 
	ExpirationDate, 
	ModifiedDate
	FROM FIL_FirstRowInAKIDGroup
),
TGT_RatingCoverage_Expire AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage AS T
	USING UPD_ExpiratedRecords AS S
	ON T.RatingCoverageId = S.RatingCoverageId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),
SQ_RatingCoverage_MissingCoverageAttributesPipeline AS (
	with _CTE as (
	select RatingCoverageId, RatingCoverageAKID from RatingCoverage 
	where CoverageType='N/A' and RiskType='N/A' and PerilGroup='N/A' and SubCoverageTypeCode='N/A' and CoverageVersion='N/A'
	)
	
	select A.CTE_RatingCoverageId, A.RC_RatingCoverageId, A.RatingCoverageAKID, A.CoverageType from (
	select 
	_CTE.RatingCoverageId as CTE_RatingCoverageId,
	RC.RatingCoverageId as RC_RatingCoverageId, 
	_CTE.RatingCoverageAKID as RatingCoverageAKID, 
	RC.CoverageType, 
	ROW_NUMBER() over (partition by RC.RatingCoverageAKID order by RC.ratingcoverageId desc) as rn 
	from RatingCoverage RC 
	inner join _CTE on _CTE.RatingCoverageAKID=RC.RatingCoverageAKID and _CTE.RatingCoverageId != RC.RatingCoverageId
	where RC.CoverageType !='N/A'
	) A 
	where rn=1
	order by 1
),
EXP_Input_MissingCoverageAttributesPipeline AS (
	SELECT
	CTE_RatingCoverageId,
	RC_RatingCoverageId,
	RatingCoverageAKID,
	CoverageType
	FROM SQ_RatingCoverage_MissingCoverageAttributesPipeline
),
UPD_MissingCoverageAttributePipeline AS (
	SELECT
	CTE_RatingCoverageId, 
	CoverageType
	FROM EXP_Input_MissingCoverageAttributesPipeline
),
TGT_RatingCoverage_MissingCoverageAttributesPipeline AS (
	MERGE INTO RatingCoverage AS T
	USING UPD_MissingCoverageAttributePipeline AS S
	ON T.RatingCoverageId = S.CTE_RatingCoverageId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CoverageType = S.CoverageType
),
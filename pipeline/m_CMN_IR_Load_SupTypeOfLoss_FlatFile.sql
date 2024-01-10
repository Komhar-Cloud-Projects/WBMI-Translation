WITH
SQ_TypeOfLossMapping AS (

-- TODO Manual --

),
EXP_Default AS (
	SELECT
	EffectiveDate,
	ExpirationDate,
	Insurance_Segment_Code AS i_Insurance_Segment_Code,
	MajorPeril AS i_MajorPeril,
	CauseOfLoss AS i_CauseOfLoss,
	CauseOfLossName AS i_CauseOfLossName,
	Type_of_Loss AS i_Type_of_Loss,
	Claim_Type_Category AS i_Claim_Type_Category,
	Claim_Type_Group AS i_Claim_Type_Group,
	Subrogation_Eligible_Indicator AS i_Subrogation_Eligible_Indicator,
	-- *INF*: Ltrim(Rtrim(i_Insurance_Segment_Code))
	Ltrim(Rtrim(i_Insurance_Segment_Code
		)
	) AS o_Insurance_Segment_Code,
	-- *INF*: Ltrim(Rtrim(i_MajorPeril))
	Ltrim(Rtrim(i_MajorPeril
		)
	) AS o_MajorPeril,
	-- *INF*: Ltrim(Rtrim(i_CauseOfLoss))
	Ltrim(Rtrim(i_CauseOfLoss
		)
	) AS o_CauseOfLoss,
	-- *INF*: Ltrim(Rtrim(i_CauseOfLossName))
	Ltrim(Rtrim(i_CauseOfLossName
		)
	) AS o_CauseOfLossName,
	-- *INF*: Ltrim(Rtrim(i_Type_of_Loss))
	Ltrim(Rtrim(i_Type_of_Loss
		)
	) AS o_Type_of_Loss,
	-- *INF*: Ltrim(Rtrim(i_Claim_Type_Category))
	Ltrim(Rtrim(i_Claim_Type_Category
		)
	) AS o_Claim_Type_Category,
	-- *INF*: Ltrim(Rtrim(i_Claim_Type_Group))
	Ltrim(Rtrim(i_Claim_Type_Group
		)
	) AS o_Claim_Type_Group,
	-- *INF*: Ltrim(Rtrim(i_Subrogation_Eligible_Indicator))
	Ltrim(Rtrim(i_Subrogation_Eligible_Indicator
		)
	) AS o_Subrogation_Eligible_Indicator
	FROM SQ_TypeOfLossMapping
),
Exp_Detect_Changes AS (
	SELECT
	'InformS' AS o_ModifyUserID,
	sysdate AS o_ModifiedDate,
	-- *INF*: TO_DATE(i_EffectiveDate, 'YYYY-MM-DD HH24:MI:SS')
	TO_DATE(i_EffectiveDate, 'YYYY-MM-DD HH24:MI:SS'
	) AS o_EffectiveDate,
	-- *INF*: TO_DATE(i_ExpirationDate  ,'YYYY-MM-DD HH24:MI:SS')
	TO_DATE(i_ExpirationDate, 'YYYY-MM-DD HH24:MI:SS'
	) AS o_ExpirationDate,
	-- *INF*: IIF(ISNULL( i_Insurance_Segment_Code    ),'N/A',   i_Insurance_Segment_Code)
	IFF(i_Insurance_Segment_Code IS NULL,
		'N/A',
		i_Insurance_Segment_Code
	) AS o_InsuranceSegCode,
	-- *INF*: IIF(ISNULL( i_MajorPeril ),'N/A',   i_MajorPeril  )
	IFF(i_MajorPeril IS NULL,
		'N/A',
		i_MajorPeril
	) AS o_MajorPeril,
	-- *INF*: IIF(ISNULL(i_CauseOfLoss),'N/A',i_CauseOfLoss)
	IFF(i_CauseOfLoss IS NULL,
		'N/A',
		i_CauseOfLoss
	) AS o_CauseOfLoss,
	-- *INF*: IIF(ISNULL(i_CauseOfLossName),'N/A',i_CauseOfLossName)
	IFF(i_CauseOfLossName IS NULL,
		'N/A',
		i_CauseOfLossName
	) AS o_CauseOfLossName,
	-- *INF*: IIF(ISNULL(i_Type_of_Loss   ),'N/A',i_Type_of_Loss  )
	IFF(i_Type_of_Loss IS NULL,
		'N/A',
		i_Type_of_Loss
	) AS o_TypeOfLoss,
	-- *INF*: IIF(ISNULL(i_Claim_Type_Category),'N/A',i_Claim_Type_Category )
	IFF(i_Claim_Type_Category IS NULL,
		'N/A',
		i_Claim_Type_Category
	) AS o_ClaimTypeCategory,
	-- *INF*: IIF(ISNULL(i_Claim_Type_Group),'N/A',i_Claim_Type_Group )
	IFF(i_Claim_Type_Group IS NULL,
		'N/A',
		i_Claim_Type_Group
	) AS o_Claim_Type_Group,
	-- *INF*: IIF(ISNULL(i_Subrogation_Eligible_Indicator),'N/A',i_Subrogation_Eligible_Indicator )
	IFF(i_Subrogation_Eligible_Indicator IS NULL,
		'N/A',
		i_Subrogation_Eligible_Indicator
	) AS o_Subrogation_Eligible_Indicator,
	o_Insurance_Segment_Code AS i_Insurance_Segment_Code,
	o_MajorPeril AS i_MajorPeril,
	o_CauseOfLoss AS i_CauseOfLoss,
	o_CauseOfLossName AS i_CauseOfLossName,
	o_Type_of_Loss AS i_Type_of_Loss,
	o_Claim_Type_Category AS i_Claim_Type_Category,
	o_Claim_Type_Group AS i_Claim_Type_Group,
	o_Subrogation_Eligible_Indicator AS i_Subrogation_Eligible_Indicator,
	EffectiveDate AS i_EffectiveDate,
	ExpirationDate AS i_ExpirationDate
	FROM EXP_Default
),
FIL_InValidData AS (
	SELECT
	o_ModifyUserID, 
	o_ModifiedDate, 
	o_EffectiveDate, 
	o_ExpirationDate, 
	o_InsuranceSegCode, 
	o_MajorPeril, 
	o_CauseOfLoss, 
	o_CauseOfLossName, 
	o_TypeOfLoss, 
	o_ClaimTypeCategory, 
	o_Claim_Type_Group, 
	o_Subrogation_Eligible_Indicator
	FROM Exp_Detect_Changes
	WHERE o_InsuranceSegCode != 'N/A' OR o_MajorPeril != 'N/A' OR
o_CauseOfLoss != 'N/A' OR o_TypeOfLoss != 'N/A'
),
SupTypeOfLossRules_IR AS (
	TRUNCATE TABLE SupTypeOfLossRules;
	INSERT INTO SupTypeOfLossRules
	(ModifiedUserId, ModifiedDate, EffectiveDate, ExpirationDate, InsuranceSegmentCode, MajorPerilCode, CauseOfLoss, CauseOfLossName, TypeOfLoss, ClaimTypeCategory, ClaimTypeGroup, SubrogationEligibleIndicator)
	SELECT 
	o_ModifyUserID AS MODIFIEDUSERID, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_EffectiveDate AS EFFECTIVEDATE, 
	o_ExpirationDate AS EXPIRATIONDATE, 
	o_InsuranceSegCode AS INSURANCESEGMENTCODE, 
	o_MajorPeril AS MAJORPERILCODE, 
	o_CauseOfLoss AS CAUSEOFLOSS, 
	o_CauseOfLossName AS CAUSEOFLOSSNAME, 
	o_TypeOfLoss AS TYPEOFLOSS, 
	o_ClaimTypeCategory AS CLAIMTYPECATEGORY, 
	o_Claim_Type_Group AS CLAIMTYPEGROUP, 
	o_Subrogation_Eligible_Indicator AS SUBROGATIONELIGIBLEINDICATOR
	FROM FIL_InValidData
),
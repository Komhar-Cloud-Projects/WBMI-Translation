WITH
SQ_SupTypeOfLossRules AS (
	SELECT 
	         a.EffectiveDate
	        ,a.ExpirationDate
		  ,a.InsuranceSegmentCode
		  ,a.MajorPerilCode
		  ,a.CauseOfLoss
	        ,a.CauseOfLossName
		  ,a.TypeOfLoss
		  ,a.ClaimTypeCategory
		  ,a.ClaimTypeGroup
		,a.SubrogationEligibleIndicator
	FROM
	(
	SELECT [SupTypeOfLossRulesId]
	      ,[ModifiedUserId]
	      ,[ModifiedDate]
	      ,[EffectiveDate]
	      ,[ExpirationDate]
	      ,[InsuranceSegmentCode]
	      ,[MajorPerilCode]
	      ,[CauseOfLoss]
		,CauseOfLossName
	      ,[TypeOfLoss]
		,[ClaimTypeCategory]
		,[ClaimTypeGroup]
		,[SubrogationEligibleIndicator]
		  ,ROW_NUMBER() OVER (PARTITION BY [InsuranceSegmentCode],[MajorPerilCode], [CauseOfLoss], [EffectiveDate]  ORDER BY [TypeOfLoss]) AS RN
	  FROM [dbo].[SupTypeOfLossRules]
	) a
	WHERE  a.RN =1
),
EXP_Trim_Values AS (
	SELECT
	EffectiveDate,
	ExpirationDate,
	InsuranceSegmentCode AS i_InsuranceSegmentCode,
	MajorPerilCode AS i_MajorPerilCode,
	CauseOfLoss AS i_CauseOfLoss,
	CauseOfLossName AS i_CauseOfLossName,
	TypeOfLoss AS i_TypeOfLoss,
	ClaimTypeCategory AS i_ClaimTypeCategory,
	ClaimTypeGroup AS i_ClaimTypeGroup,
	SubrogationEligibleIndicator AS i_SubrogationEligibleIndicator,
	-- *INF*: LTRIM(RTRIM(i_InsuranceSegmentCode))
	LTRIM(RTRIM(i_InsuranceSegmentCode
		)
	) AS o_InsuranceSegmentCode,
	-- *INF*: LTRIM(RTRIM(i_MajorPerilCode))
	LTRIM(RTRIM(i_MajorPerilCode
		)
	) AS o_MajorPerilCode,
	-- *INF*: LTRIM(RTRIM(i_CauseOfLoss))
	LTRIM(RTRIM(i_CauseOfLoss
		)
	) AS o_CauseOfLoss,
	-- *INF*: Ltrim(Rtrim(i_CauseOfLossName))
	Ltrim(Rtrim(i_CauseOfLossName
		)
	) AS o_CauseOfLossName,
	-- *INF*: LTRIM(RTRIM(i_TypeOfLoss))
	LTRIM(RTRIM(i_TypeOfLoss
		)
	) AS o_TypeOfLoss,
	-- *INF*: LTRIM(RTRIM(i_ClaimTypeCategory))
	LTRIM(RTRIM(i_ClaimTypeCategory
		)
	) AS o_ClaimTypeCategory,
	-- *INF*: LTRIM(RTRIM(i_ClaimTypeGroup))
	LTRIM(RTRIM(i_ClaimTypeGroup
		)
	) AS o_ClaimTypeGroup,
	-- *INF*: LTRIM(RTRIM(i_SubrogationEligibleIndicator))
	LTRIM(RTRIM(i_SubrogationEligibleIndicator
		)
	) AS o_SubrogationEligibleIndicator,
	sysdate AS ModifiedDate
	FROM SQ_SupTypeOfLossRules
),
Lkp_SupTypeOfLossRules AS (
	SELECT
	SupTypeOfLossRulesId,
	TypeOfLoss,
	ClaimTypeCategory,
	ClaimTypeGroup,
	SubrogationEligibleIndicator,
	InsuranceSegmentCode,
	MajorPerilCode,
	CauseOfLoss
	FROM (
		SELECT 
			SupTypeOfLossRulesId,
			TypeOfLoss,
			ClaimTypeCategory,
			ClaimTypeGroup,
			SubrogationEligibleIndicator,
			InsuranceSegmentCode,
			MajorPerilCode,
			CauseOfLoss
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupTypeOfLossRules
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceSegmentCode,MajorPerilCode,CauseOfLoss ORDER BY SupTypeOfLossRulesId) = 1
),
EXP_Detect_Changes AS (
	SELECT
	Lkp_SupTypeOfLossRules.TypeOfLoss AS lkp_TypeOfLoss,
	Lkp_SupTypeOfLossRules.ClaimTypeCategory AS lkp_ClaimTypeCategory,
	Lkp_SupTypeOfLossRules.ClaimTypeGroup AS lkp_ClaimTypeGroup,
	Lkp_SupTypeOfLossRules.SubrogationEligibleIndicator AS lkp_SubrogationEligibleIndicator,
	Lkp_SupTypeOfLossRules.SupTypeOfLossRulesId,
	EXP_Trim_Values.EffectiveDate AS i_EffectiveDate,
	EXP_Trim_Values.ExpirationDate AS i_ExpirationDate,
	EXP_Trim_Values.o_InsuranceSegmentCode AS i_InsuranceSegmentCode,
	EXP_Trim_Values.o_MajorPerilCode AS i_MajorPerilCode,
	EXP_Trim_Values.o_CauseOfLoss AS i_CauseOfLoss,
	EXP_Trim_Values.o_CauseOfLossName AS i_CauseOfLossName,
	EXP_Trim_Values.o_TypeOfLoss AS i_TypeOfLoss,
	EXP_Trim_Values.o_ClaimTypeCategory AS i_ClaimTypeCategory,
	EXP_Trim_Values.o_ClaimTypeGroup AS i_ClaimTypeGroup,
	EXP_Trim_Values.o_SubrogationEligibleIndicator AS i_SubrogationEligibleIndicator,
	EXP_Trim_Values.ModifiedDate AS i_ModifiedDate,
	-- *INF*: IIF(ISNULL(i_InsuranceSegmentCode),'N/A',i_InsuranceSegmentCode)
	IFF(i_InsuranceSegmentCode IS NULL,
		'N/A',
		i_InsuranceSegmentCode
	) AS o_InsuranceSegmentCode,
	-- *INF*: IIF(ISNULL(i_MajorPerilCode),'N/A',i_MajorPerilCode)
	IFF(i_MajorPerilCode IS NULL,
		'N/A',
		i_MajorPerilCode
	) AS o_MajorPerilCode,
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
	-- *INF*: IIF(ISNULL(i_TypeOfLoss),'N/A',i_TypeOfLoss)
	IFF(i_TypeOfLoss IS NULL,
		'N/A',
		i_TypeOfLoss
	) AS o_TypeOfLoss,
	-- *INF*: IIF(ISNULL(i_ClaimTypeCategory),'N/A',i_ClaimTypeCategory)
	IFF(i_ClaimTypeCategory IS NULL,
		'N/A',
		i_ClaimTypeCategory
	) AS o_ClaimTypeCategory,
	-- *INF*: IIF(ISNULL(i_ClaimTypeGroup),'N/A',i_ClaimTypeGroup)
	IFF(i_ClaimTypeGroup IS NULL,
		'N/A',
		i_ClaimTypeGroup
	) AS o_ClaimTypeGroup,
	-- *INF*: IIF(ISNULL(i_SubrogationEligibleIndicator),'N/A',i_SubrogationEligibleIndicator)
	IFF(i_SubrogationEligibleIndicator IS NULL,
		'N/A',
		i_SubrogationEligibleIndicator
	) AS o_SubrogationEligibleIndicator,
	v_change_flag AS o_change_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	sysdate AS CreatedDate,
	i_ModifiedDate AS o_ModifiedDate,
	-- *INF*: IIF( v_change_flag=1,i_EffectiveDate , sysdate   )
	IFF(v_change_flag = 1,
		i_EffectiveDate,
		sysdate
	) AS o_EffectiveDate,
	i_ExpirationDate AS o_ExpirationDate,
	'InsRef' AS SourceSystemId,
	1 AS changedflag_insert,
	0 AS changedflag_update,
	-- *INF*: DECODE(TRUE,ISNULL(SupTypeOfLossRulesId),1,
	-- (  ISNULL(SupTypeOfLossRulesId) = 0  AND ((lkp_TypeOfLoss<>i_TypeOfLoss) OR (lkp_ClaimTypeCategory<>i_ClaimTypeCategory) OR (lkp_ClaimTypeGroup<>i_ClaimTypeGroup) OR (lkp_SubrogationEligibleIndicator<>i_SubrogationEligibleIndicator) )  )
	-- ,0,
	-- 2)
	DECODE(TRUE,
		SupTypeOfLossRulesId IS NULL, 1,
		( SupTypeOfLossRulesId IS NULL = 0 
			AND ( ( lkp_TypeOfLoss <> i_TypeOfLoss 
				) 
				OR ( lkp_ClaimTypeCategory <> i_ClaimTypeCategory 
				) 
				OR ( lkp_ClaimTypeGroup <> i_ClaimTypeGroup 
				) 
				OR ( lkp_SubrogationEligibleIndicator <> i_SubrogationEligibleIndicator 
				) 
			) 
		), 0,
		2
	) AS v_change_flag,
	-- *INF*: ADD_TO_DATE(sysdate,'D',-1)
	DATEADD(DAY,- 1,sysdate) AS o_ExpirationDate_update
	FROM EXP_Trim_Values
	LEFT JOIN Lkp_SupTypeOfLossRules
	ON Lkp_SupTypeOfLossRules.InsuranceSegmentCode = EXP_Trim_Values.o_InsuranceSegmentCode AND Lkp_SupTypeOfLossRules.MajorPerilCode = EXP_Trim_Values.o_MajorPerilCode AND Lkp_SupTypeOfLossRules.CauseOfLoss = EXP_Trim_Values.o_CauseOfLoss
),
RTR_SupTypeOfLossRules AS (
	SELECT
	SupTypeOfLossRulesId,
	o_InsuranceSegmentCode AS InsuranceSegDescpt,
	o_MajorPerilCode AS MajorPerilCode,
	o_CauseOfLoss AS CauseOfLoss,
	o_CauseOfLossName AS CauseOfLossName,
	o_TypeOfLoss AS TypeOfLoss,
	o_ClaimTypeCategory AS ClaimTypeCategory,
	o_ClaimTypeGroup AS ClaimTypeGroup,
	o_SubrogationEligibleIndicator AS SubrogationEligibleIndicator,
	o_change_flag AS change_flag,
	AuditId,
	o_ModifiedDate AS ModifiedDate,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	CreatedDate,
	changedflag_insert,
	changedflag_update,
	SourceSystemId,
	o_ExpirationDate_update AS ExpirationDate_UPD
	FROM EXP_Detect_Changes
),
RTR_SupTypeOfLossRules_Insert AS (SELECT * FROM RTR_SupTypeOfLossRules WHERE change_flag=1 OR  change_flag=0),
RTR_SupTypeOfLossRules_Update AS (SELECT * FROM RTR_SupTypeOfLossRules WHERE change_flag=0),
SupTypeOfLossRules_Insert AS (
	INSERT INTO SupTypeOfLossRules
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, InsuranceSegmentCode, MajorPerilCode, CauseOfLoss, CauseOfLossName, TypeOfLoss, ClaimTypeCategory, ClaimTypeGroup, SubrogationEligibleIndicator)
	SELECT 
	changedflag_insert AS CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	InsuranceSegDescpt AS INSURANCESEGMENTCODE, 
	MAJORPERILCODE, 
	CAUSEOFLOSS, 
	CAUSEOFLOSSNAME, 
	TYPEOFLOSS, 
	CLAIMTYPECATEGORY, 
	CLAIMTYPEGROUP, 
	SUBROGATIONELIGIBLEINDICATOR
	FROM RTR_SupTypeOfLossRules_Insert
),
Upd_sup_TypeOfLoss_Update AS (
	SELECT
	SupTypeOfLossRulesId, 
	InsuranceSegDescpt, 
	MajorPerilCode, 
	CauseOfLoss, 
	TypeOfLoss, 
	change_flag, 
	AuditId, 
	ModifiedDate, 
	EffectiveDate, 
	ExpirationDate, 
	CreatedDate, 
	changedflag_insert, 
	changedflag_update, 
	SourceSystemId, 
	ExpirationDate_UPD AS Expiration_Date_Update, 
	CauseOfLossName
	FROM RTR_SupTypeOfLossRules_Update
),
SupTypeOfLossRules_Update AS (
	MERGE INTO SupTypeOfLossRules AS T
	USING Upd_sup_TypeOfLoss_Update AS S
	ON T.SupTypeOfLossRulesId = S.SupTypeOfLossRulesId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.changedflag_update, T.ExpirationDate = S.Expiration_Date_Update, T.ModifiedDate = S.ModifiedDate
),
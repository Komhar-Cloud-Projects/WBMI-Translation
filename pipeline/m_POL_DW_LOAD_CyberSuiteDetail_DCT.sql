WITH
SQ_PremiumTransaction_WorkPremiumTransaction AS (
	Select 
	DISTINCT 
	PT.PremiumTransactionID,
	PT.PremiumTransactionAKID,
	WPT.PremiumTransactionStageId
	from @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT
	inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransaction WPT on WPT.PremiumTransactionAKID=PT.PremiumTransactionAKID
	inner join RatingCoverage RC on PT.RatingCoverageAKId=RC.RatingCoverageAKID and PT.EffectiveDate=RC.EffectiveDate
	and RC.CoverageType in (
	'CyberSuite',
	'CyberSuiteExtendedReporting')
	where  PT.SourceSystemId='DCT'
	and PT.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}'
	@{pipeline().parameters.WHERE_CLAUSE_PT}
	Order by 
	WPT.PremiumTransactionStageId
),
EXP_InputFromPT AS (
	SELECT
	PremiumTransactionID,
	PremiumTransactionAKID,
	PremiumTransactionStageId
	FROM SQ_PremiumTransaction_WorkPremiumTransaction
),
SQ_WBCLLineStage AS (
	select distinct  
	C.CyberOneIncreasedLimitQuestionOne as CyberOneIncreasedLimitQuestionOne, 
	C.CyberOneIncreasedLimitQuestionTwo as CyberOneIncreasedLimitQuestionTwo, 
	C.CyberOneIncreasedLimitQuestionThree as CyberOneIncreasedLimitQuestionThree,
	C.CyberOneIncreasedLimitQuestionFour as CyberOneIncreasedLimitQuestionFour, 
	C.CyberOneIncreasedLimitQuestionFive as CyberOneIncreasedLimitQuestionFive, 
	C.CyberOneIncreasedLimitQuestionSix as CyberOneIncreasedLimitQuestionSix,
	C.RatingTier as RatingTier,
	D.CoverageId as CoverageId
	FROM
	DCLineStaging A
	INNER JOIN WBLineStaging B on A.LineId=B.LineId and A.SessionId=B.SessionId
	INNER JOIN WBCLLineStage C on C.SessionId=A.SessionId and C.WBLineId=B.WB_LineId
	INNER JOIN WorkDCTTransactionInsuranceLineLocationBridge D on D.SessionId=A.SessionId and D.LineId=A.LineId
	and D.CoverageRiskType in (
	'CyberSuite',
	'CyberSuiteExtendedReporting')
	INNER JOIN DCCoverageStaging Cov on Cov.ObjectId=A.LineId and Cov.ObjectName='dc_line' and Cov.Type in (
	'CyberSuite',
	'CyberSuiteExtendedReporting')
	@{pipeline().parameters.WHERE_CLAUSE}
	ORDER BY
	D.CoverageId
),
EXP_InputFromStage AS (
	SELECT
	CyberOneIncreasedLimitQuestionOne,
	CyberOneIncreasedLimitQuestionTwo,
	CyberOneIncreasedLimitQuestionThree,
	CyberOneIncreasedLimitQuestionFour,
	CyberOneIncreasedLimitQuestionFive,
	CyberOneIncreasedLimitQuestionSix,
	RatingTier,
	CoverageId
	FROM SQ_WBCLLineStage
),
JNR_StageAndPT AS (SELECT
	EXP_InputFromPT.PremiumTransactionID, 
	EXP_InputFromPT.PremiumTransactionAKID, 
	EXP_InputFromPT.PremiumTransactionStageId, 
	EXP_InputFromStage.CyberOneIncreasedLimitQuestionOne, 
	EXP_InputFromStage.CyberOneIncreasedLimitQuestionTwo, 
	EXP_InputFromStage.CyberOneIncreasedLimitQuestionThree, 
	EXP_InputFromStage.CyberOneIncreasedLimitQuestionFour, 
	EXP_InputFromStage.CyberOneIncreasedLimitQuestionFive, 
	EXP_InputFromStage.CyberOneIncreasedLimitQuestionSix, 
	EXP_InputFromStage.RatingTier, 
	EXP_InputFromStage.CoverageId
	FROM EXP_InputFromPT
	INNER JOIN EXP_InputFromStage
	ON EXP_InputFromStage.CoverageId = EXP_InputFromPT.PremiumTransactionStageId
),
EXP_JoinerOutput AS (
	SELECT
	PremiumTransactionID,
	PremiumTransactionAKID,
	PremiumTransactionStageId,
	CyberOneIncreasedLimitQuestionOne,
	CyberOneIncreasedLimitQuestionTwo,
	CyberOneIncreasedLimitQuestionThree,
	CyberOneIncreasedLimitQuestionFour,
	CyberOneIncreasedLimitQuestionFive,
	CyberOneIncreasedLimitQuestionSix,
	RatingTier,
	CoverageId
	FROM JNR_StageAndPT
),
LKP_CyberSuiteDetail AS (
	SELECT
	CyberSuiteDetailId,
	PremiumTransactionID,
	i_PremiumTransactionID
	FROM (
		SELECT 
			CyberSuiteDetailId,
			PremiumTransactionID,
			i_PremiumTransactionID
		FROM CyberSuiteDetail
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY CyberSuiteDetailId) = 1
),
FIL_InsertsOnly AS (
	SELECT
	LKP_CyberSuiteDetail.CyberSuiteDetailId AS lkp_CyberSuiteDetailId, 
	EXP_JoinerOutput.PremiumTransactionID, 
	EXP_JoinerOutput.PremiumTransactionAKID, 
	EXP_JoinerOutput.PremiumTransactionStageId, 
	EXP_JoinerOutput.CyberOneIncreasedLimitQuestionOne, 
	EXP_JoinerOutput.CyberOneIncreasedLimitQuestionTwo, 
	EXP_JoinerOutput.CyberOneIncreasedLimitQuestionThree, 
	EXP_JoinerOutput.CyberOneIncreasedLimitQuestionFour, 
	EXP_JoinerOutput.CyberOneIncreasedLimitQuestionFive, 
	EXP_JoinerOutput.CyberOneIncreasedLimitQuestionSix, 
	EXP_JoinerOutput.RatingTier
	FROM EXP_JoinerOutput
	LEFT JOIN LKP_CyberSuiteDetail
	ON LKP_CyberSuiteDetail.PremiumTransactionID = EXP_JoinerOutput.PremiumTransactionID
	WHERE ISNULL(lkp_CyberSuiteDetailId)
),
EXP_Output AS (
	SELECT
	PremiumTransactionID,
	PremiumTransactionAKID,
	PremiumTransactionStageId,
	CyberOneIncreasedLimitQuestionOne AS i_CyberOneIncreasedLimitQuestionOne,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_CyberOneIncreasedLimitQuestionOne),'N/A',
	-- IN(i_CyberOneIncreasedLimitQuestionOne,'T','1'),'1',
	-- IN(i_CyberOneIncreasedLimitQuestionOne,'F','0'),'0',
	-- 'N/A')
	DECODE(TRUE,
		i_CyberOneIncreasedLimitQuestionOne IS NULL, 'N/A',
		IN(i_CyberOneIncreasedLimitQuestionOne, 'T', '1'), '1',
		IN(i_CyberOneIncreasedLimitQuestionOne, 'F', '0'), '0',
		'N/A') AS o_CyberOneIncreasedLimitQuestionOne,
	CyberOneIncreasedLimitQuestionTwo AS i_CyberOneIncreasedLimitQuestionTwo,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_CyberOneIncreasedLimitQuestionTwo),'N/A',
	-- IN(i_CyberOneIncreasedLimitQuestionTwo,'T','1'),'1',
	-- IN(i_CyberOneIncreasedLimitQuestionTwo,'F','0'),'0',
	-- 'N/A')
	DECODE(TRUE,
		i_CyberOneIncreasedLimitQuestionTwo IS NULL, 'N/A',
		IN(i_CyberOneIncreasedLimitQuestionTwo, 'T', '1'), '1',
		IN(i_CyberOneIncreasedLimitQuestionTwo, 'F', '0'), '0',
		'N/A') AS o_CyberOneIncreasedLimitQuestionTwo,
	CyberOneIncreasedLimitQuestionThree AS i_CyberOneIncreasedLimitQuestionThree,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_CyberOneIncreasedLimitQuestionThree),'N/A',
	-- IN(i_CyberOneIncreasedLimitQuestionThree,'T','1'),'1',
	-- IN(i_CyberOneIncreasedLimitQuestionThree,'F','0'),'0',
	-- 'N/A')
	DECODE(TRUE,
		i_CyberOneIncreasedLimitQuestionThree IS NULL, 'N/A',
		IN(i_CyberOneIncreasedLimitQuestionThree, 'T', '1'), '1',
		IN(i_CyberOneIncreasedLimitQuestionThree, 'F', '0'), '0',
		'N/A') AS o_CyberOneIncreasedLimitQuestionThree,
	CyberOneIncreasedLimitQuestionFour AS i_CyberOneIncreasedLimitQuestionFour,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_CyberOneIncreasedLimitQuestionFour),'N/A',
	-- IN(i_CyberOneIncreasedLimitQuestionFour,'T','1'),'1',
	-- IN(i_CyberOneIncreasedLimitQuestionFour,'F','0'),'0',
	-- 'N/A')
	DECODE(TRUE,
		i_CyberOneIncreasedLimitQuestionFour IS NULL, 'N/A',
		IN(i_CyberOneIncreasedLimitQuestionFour, 'T', '1'), '1',
		IN(i_CyberOneIncreasedLimitQuestionFour, 'F', '0'), '0',
		'N/A') AS o_CyberOneIncreasedLimitQuestionFour,
	CyberOneIncreasedLimitQuestionFive AS i_CyberOneIncreasedLimitQuestionFive,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_CyberOneIncreasedLimitQuestionFive),'N/A',
	-- IN(i_CyberOneIncreasedLimitQuestionFive,'T','1'),'1',
	-- IN(i_CyberOneIncreasedLimitQuestionFive,'F','0'),'0',
	-- 'N/A')
	DECODE(TRUE,
		i_CyberOneIncreasedLimitQuestionFive IS NULL, 'N/A',
		IN(i_CyberOneIncreasedLimitQuestionFive, 'T', '1'), '1',
		IN(i_CyberOneIncreasedLimitQuestionFive, 'F', '0'), '0',
		'N/A') AS o_CyberOneIncreasedLimitQuestionFive,
	CyberOneIncreasedLimitQuestionSix AS i_CyberOneIncreasedLimitQuestionSix,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_CyberOneIncreasedLimitQuestionSix),'N/A',
	-- IN(i_CyberOneIncreasedLimitQuestionSix,'T','1'),'1',
	-- IN(i_CyberOneIncreasedLimitQuestionSix,'F','0'),'0',
	-- 'N/A')
	DECODE(TRUE,
		i_CyberOneIncreasedLimitQuestionSix IS NULL, 'N/A',
		IN(i_CyberOneIncreasedLimitQuestionSix, 'T', '1'), '1',
		IN(i_CyberOneIncreasedLimitQuestionSix, 'F', '0'), '0',
		'N/A') AS o_CyberOneIncreasedLimitQuestionSix,
	RatingTier AS i_RatingTier,
	-- *INF*: IIF(ISNULL(i_RatingTier),-1,i_RatingTier)
	-- 
	-- -- this should never be null, but it turns out sometimes DCT sends us nulls
	IFF(i_RatingTier IS NULL, - 1, i_RatingTier) AS o_RatingTier,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemId,
	SYSDATE AS DefaultDate
	FROM FIL_InsertsOnly
),
CyberSuiteDetail AS (
	INSERT INTO CyberSuiteDetail
	(PremiumTransactionID, PremiumTransactionAKID, AuditID, SourceSystemID, CreatedDate, ModifiedDate, CyberSuiteEligibilityQuestionOne, CyberSuiteEligibilityQuestionTwo, CyberSuiteEligibilityQuestionThree, CyberSuiteEligibilityQuestionFour, CyberSuiteEligibilityQuestionFive, CyberSuiteEligibilityQuestionSix, RatingTier)
	SELECT 
	PREMIUMTRANSACTIONID, 
	PREMIUMTRANSACTIONAKID, 
	AuditId AS AUDITID, 
	SourceSystemId AS SOURCESYSTEMID, 
	DefaultDate AS CREATEDDATE, 
	DefaultDate AS MODIFIEDDATE, 
	o_CyberOneIncreasedLimitQuestionOne AS CYBERSUITEELIGIBILITYQUESTIONONE, 
	o_CyberOneIncreasedLimitQuestionTwo AS CYBERSUITEELIGIBILITYQUESTIONTWO, 
	o_CyberOneIncreasedLimitQuestionThree AS CYBERSUITEELIGIBILITYQUESTIONTHREE, 
	o_CyberOneIncreasedLimitQuestionFour AS CYBERSUITEELIGIBILITYQUESTIONFOUR, 
	o_CyberOneIncreasedLimitQuestionFive AS CYBERSUITEELIGIBILITYQUESTIONFIVE, 
	o_CyberOneIncreasedLimitQuestionSix AS CYBERSUITEELIGIBILITYQUESTIONSIX, 
	o_RatingTier AS RATINGTIER
	FROM EXP_Output
),
SQ_CyberSuiteDetail_Offset_and_Deprecated AS (
	SELECT 
	CSDToUpdate.CyberSuiteDetailId,
	CSDPrevious.CyberSuiteEligibilityQuestionOne,
	CSDPrevious.CyberSuiteEligibilityQuestionTwo,
	CSDPrevious.CyberSuiteEligibilityQuestionThree,
	CSDPrevious.CyberSuiteEligibilityQuestionFour,
	CSDPrevious.CyberSuiteEligibilityQuestionFive,
	CSDPrevious.CyberSuiteEligibilityQuestionSix,
	CSDPrevious.RatingTier
	FROM
	WorkPremiumTransactionOffsetLineage WPTOL
	inner join CyberSuiteDetail CSDPrevious
	on ( CSDPrevious.PremiumTransactionID= WPTOL.previouspremiumtransactionid)
	inner join CyberSuiteDetail CSDToUpdate
	on ( CSDToUpdate.PremiumTransactionID= WPTOL.PremiumTransactionid)
	INNER JOIN premiumtransaction pt WITH (NOLOCK) on
	WPTOL.premiumtransactionID=pt.premiumtransactionID and PT.OffsetOnsetCode in ('Offset','Deprecated')
	WHERE
	WPTOL.UpdateAttributeFlag = 1 
	AND (
	CSDPrevious.CyberSuiteEligibilityQuestionOne <> CSDToUpdate.CyberSuiteEligibilityQuestionOne 
	OR CSDPrevious.CyberSuiteEligibilityQuestionTwo <> CSDToUpdate.CyberSuiteEligibilityQuestionTwo 
	OR CSDPrevious.CyberSuiteEligibilityQuestionThree <> CSDToUpdate.CyberSuiteEligibilityQuestionThree 
	OR CSDPrevious.CyberSuiteEligibilityQuestionFour <> CSDToUpdate.CyberSuiteEligibilityQuestionFour 
	OR CSDPrevious.CyberSuiteEligibilityQuestionFive <> CSDToUpdate.CyberSuiteEligibilityQuestionFive 
	OR CSDPrevious.CyberSuiteEligibilityQuestionSix <> CSDToUpdate.CyberSuiteEligibilityQuestionSix 
	OR CSDPrevious.RatingTier <> CSDToUpdate.RatingTier 
	)
),
EXP_Input_Offset AS (
	SELECT
	CyberSuiteDetailId,
	CyberSuiteEligibilityQuestionOne,
	CyberSuiteEligibilityQuestionTwo,
	CyberSuiteEligibilityQuestionThree,
	CyberSuiteEligibilityQuestionFour,
	CyberSuiteEligibilityQuestionFive,
	CyberSuiteEligibilityQuestionSix,
	RatingTier,
	SYSDATE AS ModifiedDate
	FROM SQ_CyberSuiteDetail_Offset_and_Deprecated
),
UPD_Update_Offset AS (
	SELECT
	CyberSuiteDetailId, 
	CyberSuiteEligibilityQuestionOne, 
	CyberSuiteEligibilityQuestionTwo, 
	CyberSuiteEligibilityQuestionThree, 
	CyberSuiteEligibilityQuestionFour, 
	CyberSuiteEligibilityQuestionFive, 
	CyberSuiteEligibilityQuestionSix, 
	RatingTier, 
	ModifiedDate
	FROM EXP_Input_Offset
),
CyberSuiteDetail_Offset AS (
	MERGE INTO CyberSuiteDetail AS T
	USING UPD_Update_Offset AS S
	ON T.CyberSuiteDetailId = S.CyberSuiteDetailId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.CyberSuiteEligibilityQuestionOne = S.CyberSuiteEligibilityQuestionOne, T.CyberSuiteEligibilityQuestionTwo = S.CyberSuiteEligibilityQuestionTwo, T.CyberSuiteEligibilityQuestionThree = S.CyberSuiteEligibilityQuestionThree, T.CyberSuiteEligibilityQuestionFour = S.CyberSuiteEligibilityQuestionFour, T.CyberSuiteEligibilityQuestionFive = S.CyberSuiteEligibilityQuestionFive, T.CyberSuiteEligibilityQuestionSix = S.CyberSuiteEligibilityQuestionSix, T.RatingTier = S.RatingTier
),
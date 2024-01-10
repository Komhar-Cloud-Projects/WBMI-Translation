WITH
SQ_CyberSuiteDetail_Negate AS (
	SELECT
	PT.PremiumTransactionID as NewNegatePremiumTransactionID,
	PT.PremiumTransactionAKID as NewNegatePremiumTransactionAKID,
	CSD.PremiumTransactionID,
	CSD.CyberSuiteEligibilityQuestionOne,
	CSD.CyberSuiteEligibilityQuestionTwo,
	CSD.CyberSuiteEligibilityQuestionThree,
	CSD.CyberSuiteEligibilityQuestionFour,
	CSD.CyberSuiteEligibilityQuestionFive,
	CSD.CyberSuiteEligibilityQuestionSix,
	CSD.RatingTier
	FROM
	CyberSuiteDetail CSD WITH (NOLOCK)
	INNER JOIN WorkPremiumTransactionDataRepairNegate WPTDRN  WITH (NOLOCK)
	    ON CSD.PremiumTransactionID = WPTDRN.OriginalPremiumTransactionID
	INNER JOIN premiumtransaction PT WITH (NOLOCK) 
	    ON PT.PremiumTransactionAKID = WPTDRN.NewNegatePremiumTransactionAKID
	    AND PT.SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Input AS (
	SELECT
	NewNegatePremiumTransactionID,
	NewNegatePremiumTransactionAKID,
	PremiumTransactionID,
	CyberSuiteEligibilityQuestionOne,
	CyberSuiteEligibilityQuestionTwo,
	CyberSuiteEligibilityQuestionThree,
	CyberSuiteEligibilityQuestionFour,
	CyberSuiteEligibilityQuestionFive,
	CyberSuiteEligibilityQuestionSix,
	RatingTier,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate
	FROM SQ_CyberSuiteDetail_Negate
),
LKP_CyberSuiteDetail AS (
	SELECT
	CyberSuiteDetailId,
	PremiumTransactionID,
	PremiumTransactionAKID,
	AuditID,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	CyberSuiteEligibilityQuestionOne,
	CyberSuiteEligibilityQuestionTwo,
	CyberSuiteEligibilityQuestionThree,
	CyberSuiteEligibilityQuestionFour,
	CyberSuiteEligibilityQuestionFive,
	CyberSuiteEligibilityQuestionSix,
	RatingTier,
	in_PremiumTransactionID
	FROM (
		SELECT 
			CyberSuiteDetailId,
			PremiumTransactionID,
			PremiumTransactionAKID,
			AuditID,
			SourceSystemID,
			CreatedDate,
			ModifiedDate,
			CyberSuiteEligibilityQuestionOne,
			CyberSuiteEligibilityQuestionTwo,
			CyberSuiteEligibilityQuestionThree,
			CyberSuiteEligibilityQuestionFour,
			CyberSuiteEligibilityQuestionFive,
			CyberSuiteEligibilityQuestionSix,
			RatingTier,
			in_PremiumTransactionID
		FROM CyberSuiteDetail
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY CyberSuiteDetailId) = 1
),
FIL_Inserts AS (
	SELECT
	LKP_CyberSuiteDetail.PremiumTransactionID AS lkp_PremiumTransactionID, 
	EXP_Input.NewNegatePremiumTransactionID, 
	EXP_Input.NewNegatePremiumTransactionAKID, 
	EXP_Input.CyberSuiteEligibilityQuestionOne, 
	EXP_Input.CyberSuiteEligibilityQuestionTwo, 
	EXP_Input.CyberSuiteEligibilityQuestionThree, 
	EXP_Input.CyberSuiteEligibilityQuestionFour, 
	EXP_Input.CyberSuiteEligibilityQuestionFive, 
	EXP_Input.CyberSuiteEligibilityQuestionSix, 
	EXP_Input.RatingTier, 
	EXP_Input.AuditID, 
	EXP_Input.SourceSystemID, 
	EXP_Input.CreatedDate, 
	EXP_Input.ModifiedDate
	FROM EXP_Input
	LEFT JOIN LKP_CyberSuiteDetail
	ON LKP_CyberSuiteDetail.PremiumTransactionID = EXP_Input.NewNegatePremiumTransactionID
	WHERE ISNULL(lkp_PremiumTransactionID)
),
CyberSuiteDetail AS (
	INSERT INTO CyberSuiteDetail
	(PremiumTransactionID, PremiumTransactionAKID, AuditID, SourceSystemID, CreatedDate, ModifiedDate, CyberSuiteEligibilityQuestionOne, CyberSuiteEligibilityQuestionTwo, CyberSuiteEligibilityQuestionThree, CyberSuiteEligibilityQuestionFour, CyberSuiteEligibilityQuestionFive, CyberSuiteEligibilityQuestionSix, RatingTier)
	SELECT 
	NewNegatePremiumTransactionID AS PREMIUMTRANSACTIONID, 
	NewNegatePremiumTransactionAKID AS PREMIUMTRANSACTIONAKID, 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	CYBERSUITEELIGIBILITYQUESTIONONE, 
	CYBERSUITEELIGIBILITYQUESTIONTWO, 
	CYBERSUITEELIGIBILITYQUESTIONTHREE, 
	CYBERSUITEELIGIBILITYQUESTIONFOUR, 
	CYBERSUITEELIGIBILITYQUESTIONFIVE, 
	CYBERSUITEELIGIBILITYQUESTIONSIX, 
	RATINGTIER
	FROM FIL_Inserts
),
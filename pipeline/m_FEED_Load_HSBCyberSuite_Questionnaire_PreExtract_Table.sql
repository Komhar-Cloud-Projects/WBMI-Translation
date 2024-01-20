WITH
SQ_Select_EligiblePolicies AS (
	SELECT distinct
	PolicyKey, 
	CyberSuiteEligibilityQuestionOne, 
	CyberSuiteEligibilityQuestionTwo, 
	CyberSuiteEligibilityQuestionThree, 
	CyberSuiteEligibilityQuestionFour, 
	CyberSuiteEligibilityQuestionFive, 
	CyberSuiteEligibilityQuestionSix, 
	PremiumTransactionEnteredDate 
	FROM
	WorkHSBCyberSuite
	where CoverageType = 'CyberSuite' AND
	OffsetOnsetCode in ('N/A','Onset') AND
	AuditId=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	ORDER BY PolicyKey
),
FIL_EligiblePolicies AS (
	SELECT
	PolicyKey, 
	CyberSuiteEligibilityQuestionOne, 
	CyberSuiteEligibilityQuestionTwo, 
	CyberSuiteEligibilityQuestionThree, 
	CyberSuiteEligibilityQuestionFour, 
	CyberSuiteEligibilityQuestionFive, 
	CyberSuiteEligibilityQuestionSix, 
	PremiumTransactionEnteredDate
	FROM SQ_Select_EligiblePolicies
	WHERE CyberSuiteEligibilityQuestionOne  !=  '' AND CyberSuiteEligibilityQuestionTwo  !=  '' AND CyberSuiteEligibilityQuestionThree  !=  '' AND CyberSuiteEligibilityQuestionFour  !=  '' AND CyberSuiteEligibilityQuestionFive  !=  '' AND CyberSuiteEligibilityQuestionSix  !=  ''
),
AGG_EligiblePolicies AS (
	SELECT
	PolicyKey
	FROM FIL_EligiblePolicies
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY NULL) = 1
),
SQ_WorkHSBCyberSuite AS (
	SELECT 
	AuditId, 
	CreatedDate, 
	ModifiedDate, 
	ContractCustomerId, 
	AgencyId, 
	PolicyKey, 
	RunDate, 
	PolicyEffectiveDate, 
	InsuredName, 
	Limit, 
	OccupancyCode, 
	AgencyCode, 
	CyberSuiteEligibilityQuestionOne, 
	CyberSuiteEligibilityQuestionTwo, 
	CyberSuiteEligibilityQuestionThree, 
	CyberSuiteEligibilityQuestionFour, 
	CyberSuiteEligibilityQuestionFive, 
	CyberSuiteEligibilityQuestionSix, 
	PremiumTransactionEnteredDate 
	FROM
	WorkHSBCyberSuite
	where CoverageType = 'CyberSuite' AND
	OffsetOnsetCode in ('N/A','Onset') AND
	AuditId=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	ORDER BY PolicyKey, PremiumTransactionEnteredDate
),
EXP_Passthrough AS (
	SELECT
	AuditId,
	CreatedDate,
	ModifiedDate,
	ContractCustomerId,
	AgencyId,
	PolicyKey,
	RunDate,
	PolicyEffectiveDate,
	InsuredName,
	Limit,
	OccupancyCode,
	AgencyCode,
	CyberSuiteEligibilityQuestionOne,
	CyberSuiteEligibilityQuestionTwo,
	CyberSuiteEligibilityQuestionThree,
	CyberSuiteEligibilityQuestionFour,
	CyberSuiteEligibilityQuestionFive,
	CyberSuiteEligibilityQuestionSix,
	PremiumTransactionEnteredDate
	FROM SQ_WorkHSBCyberSuite
),
LKP_Agency_Name AS (
	SELECT
	DoingBusinessAsName,
	AgencyID
	FROM (
		SELECT 
			DoingBusinessAsName,
			AgencyID
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Agency
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyID ORDER BY DoingBusinessAsName DESC) = 1
),
LKP_cust_number AS (
	SELECT
	cust_num,
	contract_cust_id
	FROM (
		SELECT 
			cust_num,
			contract_cust_id
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY contract_cust_id ORDER BY cust_num DESC) = 1
),
EXP_PreTarget AS (
	SELECT
	EXP_Passthrough.AuditId,
	EXP_Passthrough.CreatedDate,
	EXP_Passthrough.ModifiedDate,
	EXP_Passthrough.RunDate,
	EXP_Passthrough.RunDate AS CreationDate,
	EXP_Passthrough.PremiumTransactionEnteredDate AS PolicyRequestDate,
	-- *INF*: SET_DATE_PART(
	--          SET_DATE_PART(
	--                      SET_DATE_PART((add_to_date(PolicyRequestDate,'MM',0)), 'HH', 23) 
	--                                           ,'MI',59)
	--                                ,'SS',59)
	DATEADD(SECOND,59-DATE_PART(SECOND,DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,(DATEADD(MONTH,0,PolicyRequestDate))),(DATEADD(MONTH,0,PolicyRequestDate)))),DATEADD(HOUR,23-DATE_PART(HOUR,(DATEADD(MONTH,0,PolicyRequestDate))),(DATEADD(MONTH,0,PolicyRequestDate))))),DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,(DATEADD(MONTH,0,PolicyRequestDate))),(DATEADD(MONTH,0,PolicyRequestDate)))),DATEADD(HOUR,23-DATE_PART(HOUR,(DATEADD(MONTH,0,PolicyRequestDate))),(DATEADD(MONTH,0,PolicyRequestDate))))) AS o_PolicyRequestDate,
	EXP_Passthrough.InsuredName AS NameOfInsured,
	LKP_cust_number.cust_num AS CustomerNumber,
	EXP_Passthrough.PolicyKey AS PolicyNumber,
	LKP_Agency_Name.DoingBusinessAsName,
	-- *INF*: LTRIM(RTRIM(DoingBusinessAsName))
	LTRIM(RTRIM(DoingBusinessAsName)) AS AgencyName,
	EXP_Passthrough.AgencyCode AS AgentCode,
	EXP_Passthrough.Limit AS LimitAmount,
	EXP_Passthrough.OccupancyCode,
	EXP_Passthrough.CyberSuiteEligibilityQuestionOne AS Question1,
	-- *INF*: DECODE(TRUE,
	-- Question1 = '','N/A',
	-- Question1)
	DECODE(
	    TRUE,
	    Question1 = '', 'N/A',
	    Question1
	) AS o_Question1,
	EXP_Passthrough.CyberSuiteEligibilityQuestionTwo AS Question2,
	-- *INF*: DECODE(TRUE,
	-- Question2 = '','N/A',
	-- Question2)
	DECODE(
	    TRUE,
	    Question2 = '', 'N/A',
	    Question2
	) AS o_Question2,
	EXP_Passthrough.CyberSuiteEligibilityQuestionThree AS Question3,
	-- *INF*: DECODE(TRUE,
	-- Question3 = '','N/A',
	-- Question3)
	DECODE(
	    TRUE,
	    Question3 = '', 'N/A',
	    Question3
	) AS o_Question3,
	EXP_Passthrough.CyberSuiteEligibilityQuestionFour AS Question4,
	-- *INF*: DECODE(TRUE,
	-- Question4 = '','N/A',
	-- Question4)
	DECODE(
	    TRUE,
	    Question4 = '', 'N/A',
	    Question4
	) AS o_Question4,
	EXP_Passthrough.CyberSuiteEligibilityQuestionFive AS Question5,
	-- *INF*: DECODE(TRUE,
	-- Question5 = '','N/A',
	-- Question5)
	DECODE(
	    TRUE,
	    Question5 = '', 'N/A',
	    Question5
	) AS o_Question5,
	EXP_Passthrough.CyberSuiteEligibilityQuestionSix AS Question6,
	-- *INF*: DECODE(TRUE,
	-- Question6 = '','N/A',
	-- Question6)
	DECODE(
	    TRUE,
	    Question6 = '', 'N/A',
	    Question6
	) AS o_Question6
	FROM EXP_Passthrough
	LEFT JOIN LKP_Agency_Name
	ON LKP_Agency_Name.AgencyID = EXP_Passthrough.AgencyId
	LEFT JOIN LKP_cust_number
	ON LKP_cust_number.contract_cust_id = EXP_Passthrough.ContractCustomerId
),
JNR_CandidatePolicies AS (SELECT
	EXP_PreTarget.AuditId, 
	EXP_PreTarget.CreatedDate, 
	EXP_PreTarget.ModifiedDate, 
	EXP_PreTarget.RunDate, 
	EXP_PreTarget.NameOfInsured, 
	EXP_PreTarget.CustomerNumber, 
	EXP_PreTarget.PolicyNumber, 
	EXP_PreTarget.AgencyName, 
	EXP_PreTarget.AgentCode, 
	EXP_PreTarget.LimitAmount, 
	EXP_PreTarget.OccupancyCode, 
	EXP_PreTarget.o_Question1 AS Question1, 
	EXP_PreTarget.o_Question2 AS Question2, 
	EXP_PreTarget.o_Question3 AS Question3, 
	EXP_PreTarget.o_Question4 AS Question4, 
	EXP_PreTarget.o_Question5 AS Question5, 
	EXP_PreTarget.o_Question6 AS Question6, 
	EXP_PreTarget.o_PolicyRequestDate AS PolicyRequestDate, 
	AGG_EligiblePolicies.PolicyKey
	FROM EXP_PreTarget
	INNER JOIN AGG_EligiblePolicies
	ON AGG_EligiblePolicies.PolicyKey = EXP_PreTarget.PolicyNumber
),
SRT_PTEnteredDate AS (
	SELECT
	AuditId, 
	CreatedDate, 
	ModifiedDate, 
	RunDate, 
	RunDate AS CreationDate, 
	NameOfInsured, 
	CustomerNumber, 
	PolicyKey AS PolicyNumber, 
	AgencyName, 
	AgentCode, 
	LimitAmount, 
	OccupancyCode, 
	Question1, 
	Question2, 
	Question3, 
	Question4, 
	Question5, 
	Question6, 
	PolicyRequestDate
	FROM JNR_CandidatePolicies
	ORDER BY PolicyRequestDate ASC
),
AGG_PolicyRecord AS (
	SELECT
	AuditId,
	CreatedDate,
	ModifiedDate,
	RunDate,
	CreationDate,
	PolicyRequestDate,
	NameOfInsured,
	CustomerNumber,
	PolicyNumber,
	AgencyName,
	AgentCode,
	LimitAmount,
	-- *INF*: LAST(LimitAmount)
	LAST(LimitAmount) AS out_LimitAmount,
	OccupancyCode,
	Question1,
	Question2,
	Question3,
	Question4,
	Question5,
	Question6
	FROM SRT_PTEnteredDate
	GROUP BY PolicyRequestDate, PolicyNumber, Question1, Question2, Question3, Question4, Question5, Question6
),
HSBCyberSuiteReferralExtract AS (
	TRUNCATE TABLE HSBCyberSuiteReferralExtract;
	INSERT INTO HSBCyberSuiteReferralExtract
	(AuditId, CreatedDate, ModifiedDate, RunDate, CreationDate, PolicyRequestDate, NameOfInsured, CustomerNumber, PolicyNumber, AgencyName, AgentCode, LimitAmount, OccupancyCode, Question1, Question2, Question3, Question4, Question5, Question6)
	SELECT 
	AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	RUNDATE, 
	CREATIONDATE, 
	POLICYREQUESTDATE, 
	NAMEOFINSURED, 
	CUSTOMERNUMBER, 
	POLICYNUMBER, 
	AGENCYNAME, 
	AGENTCODE, 
	out_LimitAmount AS LIMITAMOUNT, 
	OCCUPANCYCODE, 
	QUESTION1, 
	QUESTION2, 
	QUESTION3, 
	QUESTION4, 
	QUESTION5, 
	QUESTION6
	FROM AGG_PolicyRecord
),
WITH
SQ_WB_Policy AS (
	WITH cte_WBPolicy(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.PolicyId, 
	X.WB_PolicyId, 
	X.SessionId, 
	X.CustomerCare, 
	X.Division, 
	X.Terrorism, 
	X.Program, 
	X.Association, 
	X.ReceivedAMPM, 
	X.ReceivedDate, 
	X.ReceivedTimeHour, 
	X.ReceivedTimeMinute, 
	X.PolicyVersion, 
	X.PolicyVersionFormatted, 
	X.IsPreliminaryAuditRequired, 
	X.IsMidTermAuditRequired, 
	X.WBProduct, 
	X.WBProductType, 
	X.RiskGrade, 
	X.BCCCode, 
	X.PlusPak, 
	X.PolicyNumber, 
	X.IsRollover, 
	X.PriorCarrierNameOther, 
	X.PremiumMining, 
	X.QuoteType, 
	X.DescriptionOfChildCarePremises, 
	X.AssociationMessages, 
	X.AssociationMessagesMCRA, 
	X.AssociationDiscount, 
	X.AssociationDiscountFactor, 
	X.ProgramFactor, 
	X.ClearedIdentification, 
	X.ClearedIdentificationDateTimeStamp, 
	X.LegalNoticeRequired, 
	X.AssignedUnderwriterFirstName, 
	X.AssignedUnderwriterLastName, 
	X.StatusCode, 
	X.Code, 
	X.BCCCodeDesc, 
	X.ConsentToRate, 
	X.MultipleLocationCredit, 
	X.Comments, 
	X.Decision, 
	X.Message, 
	X.AutomaticRenewalIndicator, 
	X.PolicyCoverage, 
	X.IsBindableFlag, 
	X.PurePremium, 
	X.RuleType, 
	X.PolicyProgram, 
	X.ReinsuranceIndicator, 
	X.RequestingEntity, 
	X.OriginalBillingAccountNumber, 
	X.OriginalPayPlan, 
	X.OriginalTargetDueDate, 
	X.PreviousPolicyVersion, 
	X.EndorseProcessedBy, 
	X.PriorCarrierName,
	X.PolicyIssueCodeDesc,
	X.PolicyIssueCodeOverride,
	X.PenguinTechGenerated,
	X.TotalFloodLimit,
	X.TotalFloodDeductible,
	X.PoolCode,
	X.IssuedUWID,
	X.IssuedUnderwriter,
	X.ExternalQuoteSource,
	X.TurnstileGenerated,
	X.LCSurveyOrderedIndicator,
	X.LCSurveyOrderedDate
	FROM
	WB_Policy X
	inner join
	cte_WBPolicy Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	PolicyId,
	WB_PolicyId,
	SessionId,
	CustomerCare,
	Division,
	Terrorism,
	Program,
	Association,
	ReceivedAMPM,
	ReceivedDate,
	ReceivedTimeHour,
	ReceivedTimeMinute,
	PolicyVersion,
	PolicyVersionFormatted,
	IsPreliminaryAuditRequired AS i_IsPreliminaryAuditRequired,
	-- *INF*: DECODE(i_IsPreliminaryAuditRequired, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_IsPreliminaryAuditRequired,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_IsPreliminaryAuditRequired,
	IsMidTermAuditRequired AS i_IsMidTermAuditRequired,
	-- *INF*: DECODE(i_IsMidTermAuditRequired, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_IsMidTermAuditRequired,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_IsMidTermAuditRequired,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	'N/A' AS o_UserName,
	WBProduct,
	WBProductType,
	RiskGrade,
	BCCCode,
	PlusPak AS i_PlusPak,
	-- *INF*: DECODE(i_PlusPak, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_PlusPak,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_PlusPak,
	PolicyNumber,
	IsRollover AS i_IsRollover,
	-- *INF*: DECODE(i_IsRollover, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_IsRollover,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_IsRollover,
	PriorCarrierNameOther,
	PremiumMining,
	QuoteType,
	DescriptionOfChildCarePremises,
	AssociationMessages,
	AssociationMessagesMCRA,
	AssociationDiscount,
	AssociationDiscountFactor,
	ProgramFactor,
	ClearedIdentification AS i_ClearedIdentification,
	-- *INF*: DECODE(i_ClearedIdentification, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_ClearedIdentification,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ClearedIdentification,
	ClearedIdentificationDateTimeStamp,
	LegalNoticeRequired,
	AssignedUnderwriterFirstName,
	AssignedUnderwriterLastName,
	StatusCode,
	Code,
	BCCCodeDesc,
	ConsentToRate AS i_ConsentToRate,
	-- *INF*: DECODE(i_ConsentToRate, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_ConsentToRate,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ConsentToRate,
	MultipleLocationCredit AS i_MultipleLocationCredit,
	-- *INF*: DECODE(i_MultipleLocationCredit, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_MultipleLocationCredit,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_MultipleLocationCredit,
	Comments,
	Decision,
	Message,
	AutomaticRenewalIndicator AS i_AutomaticRenewalIndicator,
	-- *INF*: DECODE(i_AutomaticRenewalIndicator, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_AutomaticRenewalIndicator,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_AutomaticRenewalIndicator,
	PolicyCoverage,
	IsBindableFlag AS i_IsBindableFlag,
	-- *INF*: DECODE(i_IsBindableFlag, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_IsBindableFlag,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_IsBindableFlag,
	PurePremium,
	RuleType,
	PolicyProgram,
	ReinsuranceIndicator AS i_ReinsuranceIndicator,
	-- *INF*: DECODE(i_ReinsuranceIndicator,'T','1','F','0')
	DECODE(
	    i_ReinsuranceIndicator,
	    'T', '1',
	    'F', '0'
	) AS o_ReinsuranceIndicator,
	RequestingEntity,
	OriginalBillingAccountNumber,
	OriginalPayPlan,
	OriginalTargetDueDate,
	PreviousPolicyVersion,
	EndorseProcessedBy,
	-- *INF*: IIF(ISNULL(EndorseProcessedBy),'N/A',EndorseProcessedBy)
	IFF(EndorseProcessedBy IS NULL, 'N/A', EndorseProcessedBy) AS o_EndorseProcessedBy,
	PriorCarrierName,
	PolicyIssueCodeDesc,
	PolicyIssueCodeOverride AS i_PolicyIssueCodeOverride,
	-- *INF*: DECODE(i_PolicyIssueCodeOverride, 'T', 1, 'F', 0)
	DECODE(
	    i_PolicyIssueCodeOverride,
	    'T', 1,
	    'F', 0
	) AS o_PolicyIssueCodeOverride,
	PenguinTechGenerated AS i_PenguinTechGenerated,
	-- *INF*: DECODE(i_PenguinTechGenerated, 'T', 1, 'F', 0,0)
	DECODE(
	    i_PenguinTechGenerated,
	    'T', 1,
	    'F', 0,
	    0
	) AS o_PenguinTechGenerated,
	TotalFloodLimit,
	TotalFloodDeductible,
	PoolCode,
	IssuedUWID,
	IssuedUnderwriter,
	ExternalQuoteSource,
	TurnstileGenerated AS i_TurnstileGenerated,
	-- *INF*: DECODE(i_TurnstileGenerated, 'T', 1, 'F', 0,0)
	-- 
	-- 
	DECODE(
	    i_TurnstileGenerated,
	    'T', 1,
	    'F', 0,
	    0
	) AS o_TurnstileGenerated,
	LCSurveyOrderedIndicator,
	-- *INF*: DECODE(LCSurveyOrderedIndicator, 'T', '1', 'F', '0',null)
	-- 
	DECODE(
	    LCSurveyOrderedIndicator,
	    'T', '1',
	    'F', '0',
	    null
	) AS o_LCSurveyOrderedIndicator,
	LCSurveyOrderedDate
	FROM SQ_WB_Policy
),
WBPolicyStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBPolicyStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBPolicyStaging
	(ExtractDate, SourceSystemId, PolicyId, WB_PolicyId, SessionId, PolicyCoverage, PurePremium, ReinsuranceIndicator, CustomerCare, Division, Terrorism, IsPreliminaryAuditRequired, IsMidTermAuditRequired, PolicyProgram, Association, ReceivedAMPM, ReceivedDate, ReceivedTimeHour, ReceivedTimeMinute, BCCCode, PlusPak, PolicyNumber, IsRollover, PriorCarrierNameOther, PremiumMining, QuoteType, BCCCodeDesc, DescriptionOfChildCarePremises, ConsentToRate, MultipleLocationCredit, WBProduct, WBProductType, RequestingEntity, OriginalBillingAccountNumber, OriginalPayPlan, OriginalTargetDueDate, PolicyVersion, PolicyVersionFormatted, AssociationDiscount, AssociationDiscountFactor, AssociationMessages, AssociationMessagesMCRA, RiskGrade, ProgramFactor, ClearedIdentification, ClearedIdentificationDateTimeStamp, LegalNoticeRequired, AutomaticRenewalIndicator, IsBindableFlag, AssignedUnderwriterFirstName, AssignedUnderwriterLastName, PreviousPolicyVersion, Code, Comments, Decision, Message, RuleType, Program, StatusCode, EndorseProcessedBy, PriorCarrierName, PolicyIssueCodeDesc, PolicyIssueCodeOverride, PenguinTechGenerated, TotalFloodLimit, TotalFloodDeductible, PoolCode, IssuedUWID, IssuedUnderwriter, ExternalQuoteSource, TurnstileGenerated, LCSurveyOrderedIndicator, LCSurveyOrderedDate)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	POLICYID, 
	WB_POLICYID, 
	SESSIONID, 
	POLICYCOVERAGE, 
	PUREPREMIUM, 
	o_ReinsuranceIndicator AS REINSURANCEINDICATOR, 
	CUSTOMERCARE, 
	DIVISION, 
	TERRORISM, 
	o_IsPreliminaryAuditRequired AS ISPRELIMINARYAUDITREQUIRED, 
	o_IsMidTermAuditRequired AS ISMIDTERMAUDITREQUIRED, 
	POLICYPROGRAM, 
	ASSOCIATION, 
	RECEIVEDAMPM, 
	RECEIVEDDATE, 
	RECEIVEDTIMEHOUR, 
	RECEIVEDTIMEMINUTE, 
	BCCCODE, 
	o_PlusPak AS PLUSPAK, 
	POLICYNUMBER, 
	o_IsRollover AS ISROLLOVER, 
	PRIORCARRIERNAMEOTHER, 
	PREMIUMMINING, 
	QUOTETYPE, 
	BCCCODEDESC, 
	DESCRIPTIONOFCHILDCAREPREMISES, 
	o_ConsentToRate AS CONSENTTORATE, 
	o_MultipleLocationCredit AS MULTIPLELOCATIONCREDIT, 
	WBPRODUCT, 
	WBPRODUCTTYPE, 
	REQUESTINGENTITY, 
	ORIGINALBILLINGACCOUNTNUMBER, 
	ORIGINALPAYPLAN, 
	ORIGINALTARGETDUEDATE, 
	POLICYVERSION, 
	POLICYVERSIONFORMATTED, 
	ASSOCIATIONDISCOUNT, 
	ASSOCIATIONDISCOUNTFACTOR, 
	ASSOCIATIONMESSAGES, 
	ASSOCIATIONMESSAGESMCRA, 
	RISKGRADE, 
	PROGRAMFACTOR, 
	o_ClearedIdentification AS CLEAREDIDENTIFICATION, 
	CLEAREDIDENTIFICATIONDATETIMESTAMP, 
	LEGALNOTICEREQUIRED, 
	o_AutomaticRenewalIndicator AS AUTOMATICRENEWALINDICATOR, 
	o_IsBindableFlag AS ISBINDABLEFLAG, 
	ASSIGNEDUNDERWRITERFIRSTNAME, 
	ASSIGNEDUNDERWRITERLASTNAME, 
	PREVIOUSPOLICYVERSION, 
	CODE, 
	COMMENTS, 
	DECISION, 
	MESSAGE, 
	RULETYPE, 
	PROGRAM, 
	STATUSCODE, 
	o_EndorseProcessedBy AS ENDORSEPROCESSEDBY, 
	PRIORCARRIERNAME, 
	POLICYISSUECODEDESC, 
	o_PolicyIssueCodeOverride AS POLICYISSUECODEOVERRIDE, 
	o_PenguinTechGenerated AS PENGUINTECHGENERATED, 
	TOTALFLOODLIMIT, 
	TOTALFLOODDEDUCTIBLE, 
	POOLCODE, 
	ISSUEDUWID, 
	ISSUEDUNDERWRITER, 
	EXTERNALQUOTESOURCE, 
	o_TurnstileGenerated AS TURNSTILEGENERATED, 
	o_LCSurveyOrderedIndicator AS LCSURVEYORDEREDINDICATOR, 
	LCSURVEYORDEREDDATE
	FROM EXP_Metadata
),
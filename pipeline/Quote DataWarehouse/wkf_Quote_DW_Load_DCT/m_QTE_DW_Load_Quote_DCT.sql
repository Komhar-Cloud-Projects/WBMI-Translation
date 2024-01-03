WITH
LKP_V2_Agency AS (
	SELECT
	AgencyAKID,
	AgencyCode
	FROM (
		SELECT 
			AgencyAKID,
			AgencyCode
		FROM V2.Agency
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyCode ORDER BY AgencyAKID) = 1
),
LKP_AgencyEmployee AS (
	SELECT
	AgencyEmployeeAKID,
	UserID,
	AgencyAKID
	FROM (
		SELECT 
		AgencyEmployeeAKID as AgencyEmployeeAKID, 
		UPPER(LTRIM(RTRIM(UserID))) as UserID, 
		AgencyAKID as AgencyAKID 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyEmployee
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY UserID,AgencyAKID ORDER BY AgencyEmployeeAKID) = 1
),
SQ_Quote AS (
	SELECT
		PolicyGUId,
		QuoteActionTimeStamp,
		SessionId,
		QuoteActionStatus,
		RejectedReason,
		RejectedReasonDetails,
		PolicyEffectiveDate,
		PolicyExpirationDate,
		QuoteActionUserClassification,
		QuoteActionUserName,
		PrimaryRatingState,
		PolicyNumber,
		Division,
		PolicyProgram,
		PolicyVersion,
		WBProduct,
		WBProductType,
		LineType,
		BCCCode,
		AgencyCode,
		EstimatedQuotePremium,
		PolicyIssueCodeDesc,
		PolicyIssueCodeOverride,
		TransactionDate,
		ProducerName,
		IssuedUWID,
		IssuedUnderwriter,
		ExternalQuoteSource,
		TurnstileGenerated,
		PenguinTechGenerated,
		LCSurveyOrderedIndicator,
		LCSurveyOrderedDate,
		ExpiredReason,
		ExpiredReasonDetails,
		CustomerCare AS ServCenterSupportCode,
		IsRollover AS RolloverPolicyIndicator,
		PriorCarrierName AS RolloverPriorCarrier,
		PirorCarrierNameOther
	FROM WorkDCTPolicy
	ON WorkDCTPolicy.QuoteActionTimeStamp is not null
	@{pipeline().parameters.WHERE_CLAUSE}
),
AGG_RemoveDuplicates AS (
	SELECT
	PolicyGUId AS QuoteKey, 
	QuoteActionTimeStamp, 
	SessionId AS i_SessionId, 
	QuoteActionStatus, 
	RejectedReason, 
	RejectedReasonDetails, 
	PolicyEffectiveDate, 
	PolicyExpirationDate, 
	QuoteActionUserClassification, 
	QuoteActionUserName, 
	PrimaryRatingState, 
	PolicyNumber, 
	Division, 
	PolicyProgram, 
	PolicyVersion, 
	WBProduct, 
	WBProductType, 
	LineType, 
	BCCCode, 
	AgencyCode, 
	EstimatedQuotePremium, 
	PolicyIssueCodeDesc, 
	PolicyIssueCodeOverride, 
	TransactionDate, 
	ProducerName, 
	IssuedUWID, 
	IssuedUnderwriter, 
	ExternalQuoteSource, 
	TurnstileGenerated, 
	PenguinTechGenerated, 
	LCSurveyOrderedIndicator, 
	LCSurveyOrderedDate, 
	ExpiredReason, 
	ExpiredReasonDetails, 
	ServCenterSupportCode, 
	RolloverPolicyIndicator, 
	RolloverPriorCarrier, 
	PirorCarrierNameOther
	FROM SQ_Quote
	QUALIFY ROW_NUMBER() OVER (PARTITION BY QuoteKey, QuoteActionTimeStamp ORDER BY NULL) = 1
),
EXP_GetValues AS (
	SELECT
	QuoteKey AS i_QuoteKey,
	QuoteActionTimeStamp AS i_QuoteActionTimeStamp,
	QuoteActionStatus AS i_QuoteActionStatus,
	RejectedReason AS i_RejectedReason,
	RejectedReasonDetails AS i_RejectedReasonDetails,
	PolicyEffectiveDate AS i_PolicyEffectiveDate,
	PolicyExpirationDate AS i_PolicyExpirationDate,
	QuoteActionUserClassification AS i_QuoteActionUserClassification,
	QuoteActionUserName AS i_QuoteActionUserName,
	PrimaryRatingState AS i_PrimaryRatingState,
	PolicyNumber AS i_PolicyNumber,
	Division AS i_Division,
	PolicyProgram AS i_PolicyProgram,
	PolicyVersion AS i_PolicyVersion,
	WBProduct AS i_WBProduct,
	WBProductType AS i_WBProductType,
	LineType AS i_LineType,
	BCCCode AS i_BCCCode,
	AgencyCode AS i_AgencyCode,
	EstimatedQuotePremium AS i_EstimatedQuotePremium,
	PolicyIssueCodeDesc AS i_PolicyIssueCodeDesc,
	-- *INF*: IIF (ISNULL(i_PolicyIssueCodeDesc),'N',SUBSTR(i_PolicyIssueCodeDesc,1,1))
	IFF(i_PolicyIssueCodeDesc IS NULL, 'N', SUBSTR(i_PolicyIssueCodeDesc, 1, 1)) AS o_QuoteIssueCode,
	PolicyIssueCodeOverride,
	-- *INF*: IIF(NOT ISNULL(i_AgencyCode),LTRIM(RTRIM(i_AgencyCode)),'N/A')
	IFF(NOT i_AgencyCode IS NULL, LTRIM(RTRIM(i_AgencyCode)), 'N/A') AS v_AgencyCode,
	-- *INF*: :LKP.LKP_V2_AGENCY(v_AgencyCode)
	LKP_V2_AGENCY_v_AgencyCode.AgencyAKID AS v_AgencyAKId,
	-- *INF*: substr(in_ProducerName,11)
	-- 
	-- --REPLACESTR(0,UPPER(LTRIM(RTRIM(i_QuoteActionUserName))),'WBMI\','WBCONNECT\','WBENP\','')
	substr(in_ProducerName, 11) AS v_ProducerName,
	-- *INF*: :LKP.LKP_AGENCYEMPLOYEE(v_ProducerName, v_AgencyAKId)
	LKP_AGENCYEMPLOYEE_v_ProducerName_v_AgencyAKId.AgencyEmployeeAKID AS v_AgencyEmployeeAKId,
	-- *INF*: :LKP.LKP_V2_AGENCY('99999')
	LKP_V2_AGENCY__99999.AgencyAKID AS v_AgencyAKID_99999,
	-- *INF*: :LKP.LKP_AGENCYEMPLOYEE(v_ProducerName, v_AgencyAKID_99999)
	LKP_AGENCYEMPLOYEE_v_ProducerName_v_AgencyAKID_99999.AgencyEmployeeAKID AS v_AgencyEmployeeAKId_99999,
	i_QuoteActionStatus AS QuoteStatusCode,
	-1 AS UnderwritingAssociateAKId,
	-- *INF*: DECODE(TRUE,
	--  NOT ISNULL(v_AgencyEmployeeAKId),
	-- v_AgencyEmployeeAKId,
	--  NOT ISNULL(v_AgencyEmployeeAKId_99999),
	-- v_AgencyEmployeeAKId_99999,
	-- -1)
	DECODE(TRUE,
	NOT v_AgencyEmployeeAKId IS NULL, v_AgencyEmployeeAKId,
	NOT v_AgencyEmployeeAKId_99999 IS NULL, v_AgencyEmployeeAKId_99999,
	- 1) AS AgencyEmployeeAKId,
	i_QuoteActionTimeStamp AS StatusDate,
	-- *INF*: IIF(ISNULL(i_RejectedReason) OR IS_SPACES(i_RejectedReason) OR LENGTH(i_RejectedReason)=0,'-1',LTRIM(RTRIM(i_RejectedReason)))
	IFF(i_RejectedReason IS NULL OR IS_SPACES(i_RejectedReason) OR LENGTH(i_RejectedReason) = 0, '-1', LTRIM(RTRIM(i_RejectedReason))) AS ReasonCode,
	-- *INF*: IIF(ISNULL(i_RejectedReasonDetails) OR IS_SPACES(i_RejectedReasonDetails) OR LENGTH(i_RejectedReasonDetails)=0,'N/A',LTRIM(RTRIM(i_RejectedReasonDetails)))
	IFF(i_RejectedReasonDetails IS NULL OR IS_SPACES(i_RejectedReasonDetails) OR LENGTH(i_RejectedReasonDetails) = 0, 'N/A', LTRIM(RTRIM(i_RejectedReasonDetails))) AS o_OtherReasonComment,
	-- *INF*: IIF(NOT ISNULL(i_PolicyEffectiveDate),i_PolicyEffectiveDate,TO_DATE('18000101','YYYYMMDD'))
	IFF(NOT i_PolicyEffectiveDate IS NULL, i_PolicyEffectiveDate, TO_DATE('18000101', 'YYYYMMDD')) AS QuoteEffectiveDate,
	-- *INF*: IIF(NOT ISNULL(i_PolicyExpirationDate),i_PolicyExpirationDate,TO_DATE('18000101','YYYYMMDD'))
	IFF(NOT i_PolicyExpirationDate IS NULL, i_PolicyExpirationDate, TO_DATE('18000101', 'YYYYMMDD')) AS QuoteExpirationDate,
	-- *INF*: IIF(ISNULL(i_QuoteKey) OR IS_SPACES(i_QuoteKey) OR LENGTH(i_QuoteKey)=0,'N/A',LTRIM(RTRIM(i_QuoteKey)))
	IFF(i_QuoteKey IS NULL OR IS_SPACES(i_QuoteKey) OR LENGTH(i_QuoteKey) = 0, 'N/A', LTRIM(RTRIM(i_QuoteKey))) AS QuoteKey,
	-- *INF*: IIF(ISNULL(i_PolicyNumber) OR IS_SPACES(i_PolicyNumber) OR LENGTH(i_PolicyNumber)=0,'N/A',LTRIM(RTRIM(i_PolicyNumber)))
	IFF(i_PolicyNumber IS NULL OR IS_SPACES(i_PolicyNumber) OR LENGTH(i_PolicyNumber) = 0, 'N/A', LTRIM(RTRIM(i_PolicyNumber))) AS QuoteNumber,
	-- *INF*: IIF(ISNULL(i_PolicyVersion),'00',LPAD(TO_CHAR(i_PolicyVersion),2,'0'))
	IFF(i_PolicyVersion IS NULL, '00', LPAD(TO_CHAR(i_PolicyVersion), 2, '0')) AS QuoteVersion,
	-- *INF*: IIF(ISNULL(i_PolicyProgram) OR IS_SPACES(i_PolicyProgram) OR LENGTH(i_PolicyProgram)=0,'N/A',LTRIM(RTRIM(i_PolicyProgram)))
	IFF(i_PolicyProgram IS NULL OR IS_SPACES(i_PolicyProgram) OR LENGTH(i_PolicyProgram) = 0, 'N/A', LTRIM(RTRIM(i_PolicyProgram))) AS o_ProgramCode,
	-- *INF*: IIF(ISNULL(i_BCCCode) OR IS_SPACES(i_BCCCode) OR LENGTH(i_BCCCode)=0,'N/A',IIF(LENGTH(i_BCCCode)<5,LPAD(LTRIM(RTRIM(i_BCCCode)),5,'0'),LTRIM(RTRIM(i_BCCCode))))
	IFF(i_BCCCode IS NULL OR IS_SPACES(i_BCCCode) OR LENGTH(i_BCCCode) = 0, 'N/A', IFF(LENGTH(i_BCCCode) < 5, LPAD(LTRIM(RTRIM(i_BCCCode)), 5, '0'), LTRIM(RTRIM(i_BCCCode)))) AS BusinessClassCode,
	-- *INF*: IIF(NOT ISNULL(i_QuoteActionUserClassification),i_QuoteActionUserClassification,'N/A')
	IFF(NOT i_QuoteActionUserClassification IS NULL, i_QuoteActionUserClassification, 'N/A') AS InternalExternalIndicator,
	-- *INF*: IIF(ISNULL(i_PrimaryRatingState) OR IS_SPACES(i_PrimaryRatingState) OR LENGTH(i_PrimaryRatingState)=0,'N/A',LTRIM(RTRIM(i_PrimaryRatingState)))
	IFF(i_PrimaryRatingState IS NULL OR IS_SPACES(i_PrimaryRatingState) OR LENGTH(i_PrimaryRatingState) = 0, 'N/A', LTRIM(RTRIM(i_PrimaryRatingState))) AS RiskState,
	-- *INF*: IIF(ISNULL(i_Division) OR IS_SPACES(i_Division) OR LENGTH(i_Division)=0, 'N/A', LTRIM(RTRIM(i_Division)))
	IFF(i_Division IS NULL OR IS_SPACES(i_Division) OR LENGTH(i_Division) = 0, 'N/A', LTRIM(RTRIM(i_Division))) AS o_Division,
	-- *INF*: IIF(ISNULL(i_WBProduct) OR IS_SPACES(i_WBProduct) OR LENGTH(i_WBProduct)=0, 'N/A', LTRIM(RTRIM(i_WBProduct)))
	IFF(i_WBProduct IS NULL OR IS_SPACES(i_WBProduct) OR LENGTH(i_WBProduct) = 0, 'N/A', LTRIM(RTRIM(i_WBProduct))) AS o_WBProduct,
	-- *INF*: IIF(ISNULL(i_WBProductType) OR IS_SPACES(i_WBProductType) OR LENGTH(i_WBProductType)=0, 'N/A', LTRIM(RTRIM(i_WBProductType)))
	IFF(i_WBProductType IS NULL OR IS_SPACES(i_WBProductType) OR LENGTH(i_WBProductType) = 0, 'N/A', LTRIM(RTRIM(i_WBProductType))) AS o_WBProductType,
	-- *INF*: IIF(ISNULL(i_LineType) OR IS_SPACES(i_LineType) OR LENGTH(i_LineType)=0, 'N/A', LTRIM(RTRIM(i_LineType)))
	IFF(i_LineType IS NULL OR IS_SPACES(i_LineType) OR LENGTH(i_LineType) = 0, 'N/A', LTRIM(RTRIM(i_LineType))) AS o_LineType,
	-- *INF*: IIF(NOT ISNULL(v_AgencyAKId),v_AgencyAKId,v_AgencyAKID_99999)
	IFF(NOT v_AgencyAKId IS NULL, v_AgencyAKId, v_AgencyAKID_99999) AS AgencyAKId,
	-- *INF*: IIF (ISNULL(i_EstimatedQuotePremium),0,i_EstimatedQuotePremium)
	IFF(i_EstimatedQuotePremium IS NULL, 0, i_EstimatedQuotePremium) AS EstimatedQuotePremium,
	TransactionDate,
	-- *INF*: IIF (ISNULL(TransactionDate),i_QuoteActionTimeStamp,TransactionDate)
	IFF(TransactionDate IS NULL, i_QuoteActionTimeStamp, TransactionDate) AS v_TransactionDate,
	-- *INF*: IIF (ISNULL(i_PolicyIssueCodeDesc),NULL,v_TransactionDate)
	IFF(i_PolicyIssueCodeDesc IS NULL, NULL, v_TransactionDate) AS o_TransactionDate,
	ProducerName AS in_ProducerName,
	IssuedUWID,
	-- *INF*: IIF(ISNULL(IssuedUWID) OR IssuedUWID= '0','N/A',IssuedUWID)
	IFF(IssuedUWID IS NULL OR IssuedUWID = '0', 'N/A', IssuedUWID) AS o_IssuedUWID,
	IssuedUnderwriter,
	-- *INF*: IIF(ISNULL(IssuedUnderwriter),'N/A',IssuedUnderwriter)
	IFF(IssuedUnderwriter IS NULL, 'N/A', IssuedUnderwriter) AS o_IssuedUnderwriter,
	ExternalQuoteSource AS i_ExternalQuoteSource,
	TurnstileGenerated AS i_TurnstileGenerated,
	PenguinTechGenerated AS i_PenguinTechGenerated,
	-- *INF*: IIF(ISNULL(i_TurnstileGenerated),'F',i_TurnstileGenerated)
	IFF(i_TurnstileGenerated IS NULL, 'F', i_TurnstileGenerated) AS v_TurnstileGenerated,
	-- *INF*: IIF(ISNULL(i_PenguinTechGenerated),'F',i_PenguinTechGenerated)
	IFF(i_PenguinTechGenerated IS NULL, 'F', i_PenguinTechGenerated) AS v_PenguinTechGenerated,
	-- *INF*: DECODE(TRUE, 
	-- ISNULL(i_ExternalQuoteSource) AND v_TurnstileGenerated='F' AND v_PenguinTechGenerated='F', 'DCT',
	-- ISNULL(i_ExternalQuoteSource) AND v_TurnstileGenerated='T' AND v_PenguinTechGenerated='F', 'Turnstile',
	-- ISNULL(i_ExternalQuoteSource) AND v_TurnstileGenerated='F' AND v_PenguinTechGenerated='T', 'AgentPortal',
	-- i_ExternalQuoteSource)
	-- 
	-- --REG_REPLACE( i_ExternalQuoteSource, '\s+', ''))
	DECODE(TRUE,
	i_ExternalQuoteSource IS NULL AND v_TurnstileGenerated = 'F' AND v_PenguinTechGenerated = 'F', 'DCT',
	i_ExternalQuoteSource IS NULL AND v_TurnstileGenerated = 'T' AND v_PenguinTechGenerated = 'F', 'Turnstile',
	i_ExternalQuoteSource IS NULL AND v_TurnstileGenerated = 'F' AND v_PenguinTechGenerated = 'T', 'AgentPortal',
	i_ExternalQuoteSource) AS v_QuoteChannel,
	v_QuoteChannel AS o_QuoteChannel,
	LCSurveyOrderedIndicator,
	LCSurveyOrderedDate,
	-- *INF*: Decode( true,
	-- v_QuoteChannel ='DCT', 'Internal', 
	-- v_QuoteChannel ='Turnstile', 'Internal', 
	-- v_QuoteChannel = 'UploadAgentPortal','Internal', 
	-- v_QuoteChannel ='UploadUWPortal','Internal',
	-- --v_QuoteChannel='3rd Party Rater  Tarmika','External', 
	--  --v_QuoteChannel='Dais', 'External',  
	--  v_QuoteChannel='DAIS', 'External',  
	-- SUBSTR(REPLACESTR(1,v_QuoteChannel,chr(32),chr(45),''), 0, 11)='AgentPortal','External', 
	-- SUBSTR(REPLACESTR(1,v_QuoteChannel,chr(32),chr(45),''), 0, 13)='3rdPartyRater','External', 
	-- 'N/A')
	-- 
	-- 
	-- 
	Decode(true,
	v_QuoteChannel = 'DCT', 'Internal',
	v_QuoteChannel = 'Turnstile', 'Internal',
	v_QuoteChannel = 'UploadAgentPortal', 'Internal',
	v_QuoteChannel = 'UploadUWPortal', 'Internal',
	v_QuoteChannel = 'DAIS', 'External',
	SUBSTR(REPLACESTR(1, v_QuoteChannel, chr(32), chr(45), ''), 0, 11) = 'AgentPortal', 'External',
	SUBSTR(REPLACESTR(1, v_QuoteChannel, chr(32), chr(45), ''), 0, 13) = '3rdPartyRater', 'External',
	'N/A') AS v_QuoteChannelOrigin,
	v_QuoteChannelOrigin AS o_QuoteChannelOrigin,
	ExpiredReason AS i_ExpiredReason,
	-- *INF*: IIF(ISNULL(i_ExpiredReason) OR IS_SPACES(i_ExpiredReason) OR LENGTH(i_ExpiredReason)=0,'-1',LTRIM(RTRIM(i_ExpiredReason)))
	IFF(i_ExpiredReason IS NULL OR IS_SPACES(i_ExpiredReason) OR LENGTH(i_ExpiredReason) = 0, '-1', LTRIM(RTRIM(i_ExpiredReason))) AS QuoteReasonClosedCode,
	ExpiredReasonDetails AS i_ExpiredReasonDetails,
	-- *INF*: IIF(ISNULL(i_ExpiredReasonDetails) OR IS_SPACES(i_ExpiredReasonDetails) OR LENGTH(i_ExpiredReasonDetails)=0,'N/A',LTRIM(RTRIM(i_ExpiredReasonDetails)))
	IFF(i_ExpiredReasonDetails IS NULL OR IS_SPACES(i_ExpiredReasonDetails) OR LENGTH(i_ExpiredReasonDetails) = 0, 'N/A', LTRIM(RTRIM(i_ExpiredReasonDetails))) AS QuoteReasonClosedComments,
	ServCenterSupportCode AS i_ServCenterSupportCode,
	RolloverPolicyIndicator AS i_RolloverPolicyIndicator,
	RolloverPriorCarrier AS i_RolloverPriorCarrier,
	PirorCarrierNameOther AS i_PriorCarrierNameOther,
	-- *INF*: Decode(True,
	--       In(ltrim(rtrim(i_ServCenterSupportCode)),'0'),'N',
	--    In(ltrim(rtrim(i_ServCenterSupportCode)),'1'),'Y',
	--       'N/A')
	-- 
	Decode(True,
	In(ltrim(rtrim(i_ServCenterSupportCode)), '0'), 'N',
	In(ltrim(rtrim(i_ServCenterSupportCode)), '1'), 'Y',
	'N/A') AS o_ServCenterSupportCode,
	-- *INF*: IIF(ISNULL(i_RolloverPriorCarrier),'N/A', SUBSTR(LTRIM(RTRIM(i_RolloverPriorCarrier)),1,50))
	IFF(i_RolloverPriorCarrier IS NULL, 'N/A', SUBSTR(LTRIM(RTRIM(i_RolloverPriorCarrier)), 1, 50)) AS v_RolloverPriorCarrier,
	-- *INF*: IIF(ISNULL(i_PriorCarrierNameOther),'N/A',SUBSTR(LTRIM(RTRIM(i_PriorCarrierNameOther)),1,50))
	IFF(i_PriorCarrierNameOther IS NULL, 'N/A', SUBSTR(LTRIM(RTRIM(i_PriorCarrierNameOther)), 1, 50)) AS v_PriorCarrierNameOther,
	-- *INF*: DECODE(i_RolloverPolicyIndicator, 'T',1,'F',0,0)
	DECODE(i_RolloverPolicyIndicator,
	'T', 1,
	'F', 0,
	0) AS o_RolloverPolicyIndicator,
	-- *INF*: IIF(i_RolloverPolicyIndicator ='T',
	-- IIF(v_RolloverPriorCarrier='Other', v_PriorCarrierNameOther,v_RolloverPriorCarrier),'N/A')
	IFF(i_RolloverPolicyIndicator = 'T', IFF(v_RolloverPriorCarrier = 'Other', v_PriorCarrierNameOther, v_RolloverPriorCarrier), 'N/A') AS o_RolloverPriorCarrier
	FROM AGG_RemoveDuplicates
	LEFT JOIN LKP_V2_AGENCY LKP_V2_AGENCY_v_AgencyCode
	ON LKP_V2_AGENCY_v_AgencyCode.AgencyCode = v_AgencyCode

	LEFT JOIN LKP_AGENCYEMPLOYEE LKP_AGENCYEMPLOYEE_v_ProducerName_v_AgencyAKId
	ON LKP_AGENCYEMPLOYEE_v_ProducerName_v_AgencyAKId.UserID = v_ProducerName
	AND LKP_AGENCYEMPLOYEE_v_ProducerName_v_AgencyAKId.AgencyAKID = v_AgencyAKId

	LEFT JOIN LKP_V2_AGENCY LKP_V2_AGENCY__99999
	ON LKP_V2_AGENCY__99999.AgencyCode = '99999'

	LEFT JOIN LKP_AGENCYEMPLOYEE LKP_AGENCYEMPLOYEE_v_ProducerName_v_AgencyAKID_99999
	ON LKP_AGENCYEMPLOYEE_v_ProducerName_v_AgencyAKID_99999.UserID = v_ProducerName
	AND LKP_AGENCYEMPLOYEE_v_ProducerName_v_AgencyAKID_99999.AgencyAKID = v_AgencyAKID_99999

),
LKP_SupStrategicProfitCenterInsuranceSegment AS (
	SELECT
	StrategicProfitCenterCode,
	InsuranceSegmentCode,
	Division
	FROM (
		SELECT 
			StrategicProfitCenterCode,
			InsuranceSegmentCode,
			Division
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupStrategicProfitCenterInsuranceSegment
		WHERE CurrentSnapshotFlag=1 AND SourceCode='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Division ORDER BY StrategicProfitCenterCode) = 1
),
LKP_InsuranceSegment AS (
	SELECT
	InsuranceSegmentAKId,
	InsuranceSegmentCode
	FROM (
		SELECT 
			InsuranceSegmentAKId,
			InsuranceSegmentCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceSegment
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceSegmentCode ORDER BY InsuranceSegmentAKId) = 1
),
LKP_SupDCTPolicyOfferingLineOfBusinessProductRules AS (
	SELECT
	PolicyOfferingCode,
	DCTPolicyDivision,
	DCTProductCode,
	DCTProductType,
	EffectiveDate,
	ExpirationDate
	FROM (
		SELECT SupDCTPolicyOfferingLineOfBusinessProductRules.PolicyOfferingCode as PolicyOfferingCode, SupDCTPolicyOfferingLineOfBusinessProductRules.DCTPolicyDivision as DCTPolicyDivision, SupDCTPolicyOfferingLineOfBusinessProductRules.DCTProductCode as DCTProductCode, SupDCTPolicyOfferingLineOfBusinessProductRules.DCTProductType as DCTProductType, SupDCTPolicyOfferingLineOfBusinessProductRules.EffectiveDate as EffectiveDate, SupDCTPolicyOfferingLineOfBusinessProductRules.ExpirationDate as ExpirationDate 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupDCTPolicyOfferingLineOfBusinessProductRules
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY DCTPolicyDivision,DCTProductCode,DCTProductType,EffectiveDate,ExpirationDate ORDER BY PolicyOfferingCode) = 1
),
LKP_PolicyOffering AS (
	SELECT
	PolicyOfferingAKId,
	PolicyOfferingCode
	FROM (
		SELECT 
			PolicyOfferingAKId,
			PolicyOfferingCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyOffering
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyOfferingCode ORDER BY PolicyOfferingAKId) = 1
),
LKP_Program AS (
	SELECT
	ProgramAKId,
	ProgramCode
	FROM (
		SELECT 
			ProgramAKId,
			ProgramCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.Program
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProgramCode ORDER BY ProgramAKId DESC) = 1
),
LKP_Quote AS (
	SELECT
	QuoteId,
	StatusDate,
	QuoteKey
	FROM (
		Select 
		a.QuoteId as QuoteId,
		a.QuoteKey as QuoteKey,
		a.StatusDate as StatusDate
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.Quote a
		where exists (select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy w
		where w.PolicyGUId=a.QuoteKey)
		and a.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		order by a.StatusDate,a.QuoteKey
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StatusDate,QuoteKey ORDER BY QuoteId) = 1
),
LKP_StrategicProfitCenter AS (
	SELECT
	StrategicProfitCenterAKId,
	StrategicProfitCenterCode
	FROM (
		SELECT 
			StrategicProfitCenterAKId,
			StrategicProfitCenterCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.StrategicProfitCenter
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StrategicProfitCenterCode ORDER BY StrategicProfitCenterAKId) = 1
),
EXP_InsertFlag AS (
	SELECT
	LKP_Quote.QuoteId,
	LKP_Program.ProgramAKId AS i_ProgramAKId,
	LKP_StrategicProfitCenter.StrategicProfitCenterAKId AS i_StrategicProfitCenterAKId,
	LKP_InsuranceSegment.InsuranceSegmentAKId AS i_InsuranceSegmentAKId,
	LKP_PolicyOffering.PolicyOfferingAKId AS i_PolicyOfferingAKId,
	EXP_GetValues.QuoteStatusCode,
	EXP_GetValues.UnderwritingAssociateAKId,
	EXP_GetValues.AgencyEmployeeAKId,
	EXP_GetValues.StatusDate,
	EXP_GetValues.ReasonCode,
	EXP_GetValues.o_OtherReasonComment AS OtherReasonComment,
	EXP_GetValues.QuoteEffectiveDate,
	EXP_GetValues.QuoteExpirationDate,
	EXP_GetValues.QuoteKey,
	EXP_GetValues.QuoteNumber,
	EXP_GetValues.QuoteVersion,
	EXP_GetValues.BusinessClassCode,
	EXP_GetValues.InternalExternalIndicator,
	EXP_GetValues.RiskState,
	EXP_GetValues.AgencyAKId AS AgencyAKID,
	-- *INF*: IIF(ISNULL(i_StrategicProfitCenterAKId),-1,i_StrategicProfitCenterAKId)
	IFF(i_StrategicProfitCenterAKId IS NULL, - 1, i_StrategicProfitCenterAKId) AS StrategicProfitCenterAKId,
	-- *INF*: IIF(ISNULL(i_InsuranceSegmentAKId),-1,i_InsuranceSegmentAKId)
	IFF(i_InsuranceSegmentAKId IS NULL, - 1, i_InsuranceSegmentAKId) AS InsuranceSegmentAKId,
	-- *INF*: IIF(ISNULL(i_PolicyOfferingAKId), -1, i_PolicyOfferingAKId)
	IFF(i_PolicyOfferingAKId IS NULL, - 1, i_PolicyOfferingAKId) AS PolicyOfferingAKId,
	-- *INF*: IIF(ISNULL(i_ProgramAKId),-1,i_ProgramAKId)
	IFF(i_ProgramAKId IS NULL, - 1, i_ProgramAKId) AS ProgramAKId,
	EXP_GetValues.EstimatedQuotePremium,
	EXP_GetValues.o_QuoteIssueCode AS QuoteIssueCode,
	EXP_GetValues.PolicyIssueCodeOverride,
	EXP_GetValues.o_TransactionDate AS TransactionDate,
	EXP_GetValues.o_IssuedUWID AS IssuedUWID,
	EXP_GetValues.o_IssuedUnderwriter AS IssuedUnderwriter,
	EXP_GetValues.o_QuoteChannel AS QuoteChannel,
	EXP_GetValues.LCSurveyOrderedIndicator,
	EXP_GetValues.LCSurveyOrderedDate,
	EXP_GetValues.o_QuoteChannelOrigin AS QuoteChannelOrigin,
	EXP_GetValues.QuoteReasonClosedCode,
	EXP_GetValues.QuoteReasonClosedComments,
	EXP_GetValues.o_ServCenterSupportCode AS ServCenterSupportCode,
	EXP_GetValues.o_RolloverPolicyIndicator AS RolloverPolicyIndicator,
	EXP_GetValues.o_RolloverPriorCarrier AS RolloverPriorCarrier
	FROM EXP_GetValues
	LEFT JOIN LKP_InsuranceSegment
	ON LKP_InsuranceSegment.InsuranceSegmentCode = LKP_SupStrategicProfitCenterInsuranceSegment.InsuranceSegmentCode
	LEFT JOIN LKP_PolicyOffering
	ON LKP_PolicyOffering.PolicyOfferingCode = LKP_SupDCTPolicyOfferingLineOfBusinessProductRules.PolicyOfferingCode
	LEFT JOIN LKP_Program
	ON LKP_Program.ProgramCode = EXP_GetValues.o_ProgramCode
	LEFT JOIN LKP_Quote
	ON LKP_Quote.StatusDate = EXP_GetValues.StatusDate AND LKP_Quote.QuoteKey = EXP_GetValues.QuoteKey
	LEFT JOIN LKP_StrategicProfitCenter
	ON LKP_StrategicProfitCenter.StrategicProfitCenterCode = LKP_SupStrategicProfitCenterInsuranceSegment.StrategicProfitCenterCode
),
FIL_UnchangedRecords AS (
	SELECT
	QuoteId, 
	QuoteStatusCode, 
	UnderwritingAssociateAKId, 
	AgencyEmployeeAKId, 
	StatusDate, 
	ReasonCode, 
	OtherReasonComment, 
	QuoteEffectiveDate, 
	QuoteExpirationDate, 
	QuoteKey, 
	QuoteNumber, 
	QuoteVersion, 
	BusinessClassCode, 
	InternalExternalIndicator, 
	RiskState, 
	AgencyAKID, 
	StrategicProfitCenterAKId, 
	InsuranceSegmentAKId, 
	PolicyOfferingAKId, 
	ProgramAKId, 
	EstimatedQuotePremium, 
	QuoteIssueCode, 
	PolicyIssueCodeOverride, 
	TransactionDate, 
	IssuedUWID, 
	IssuedUnderwriter, 
	QuoteChannel, 
	LCSurveyOrderedIndicator, 
	LCSurveyOrderedDate, 
	QuoteChannelOrigin, 
	QuoteReasonClosedCode, 
	QuoteReasonClosedComments, 
	ServCenterSupportCode, 
	RolloverPolicyIndicator, 
	RolloverPriorCarrier
	FROM EXP_InsertFlag
	WHERE ISNULL(QuoteId)
),
LKP_QuoteAKId AS (
	SELECT
	QuoteAKId,
	LkpQuoteIssueCodeChangeDate,
	LkpQuoteIssueCode,
	QuoteKey
	FROM (
		Select 
		a.QuoteAKId as QuoteAKId,
		a.QuoteKey as QuoteKey,
		a.QuoteIssueCode as LkpQuoteIssueCode,
		a.QuoteIssueCodeChangeDate as LkpQuoteIssueCodeChangeDate
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.Quote a
		where exists (select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy w
		where w.PolicyGUId=a.QuoteKey)
		and a.CurrentSnapshotFlag = 1
		and a.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY QuoteKey ORDER BY QuoteAKId) = 1
),
LKP_Quotecreateddate AS (
	SELECT
	QuoteActionTimeStamp,
	PolicyGUId
	FROM (
		Select 
		min (w.QuoteActionTimeStamp) as QuoteActionTimeStamp,
		a.PolicyGUId as PolicyGUId
		from  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy a
		inner join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.archWorkDCTPolicy w
		on w.PolicyGUId=a.PolicyGUId
		group by a.PolicyGUId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyGUId ORDER BY QuoteActionTimeStamp) = 1
),
LKP_WBPolicyStaging AS (
	SELECT
	PenguinTechGenerated,
	PolicyNumber,
	PolicyVersionFormatted
	FROM (
		SELECT 
			PenguinTechGenerated,
			PolicyNumber,
			PolicyVersionFormatted
		FROM WBPolicyStaging
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber,PolicyVersionFormatted ORDER BY PenguinTechGenerated) = 1
),
SEQ_QuoteAKId AS (
	CREATE SEQUENCE SEQ_QuoteAKId
	START = 1
	INCREMENT = 1;
),
EXP_GetSupportIds AS (
	SELECT
	SEQ_QuoteAKId.NEXTVAL AS i_NEXTVAL,
	LKP_Quotecreateddate.QuoteActionTimeStamp AS i_QuoteCreatedDate,
	LKP_QuoteAKId.QuoteAKId AS i_QuoteAKId,
	FIL_UnchangedRecords.QuoteStatusCode,
	FIL_UnchangedRecords.UnderwritingAssociateAKId,
	FIL_UnchangedRecords.AgencyEmployeeAKId,
	FIL_UnchangedRecords.StatusDate,
	FIL_UnchangedRecords.ReasonCode,
	FIL_UnchangedRecords.OtherReasonComment AS i_OtherReasonComment,
	FIL_UnchangedRecords.QuoteEffectiveDate,
	FIL_UnchangedRecords.QuoteExpirationDate,
	FIL_UnchangedRecords.QuoteKey,
	FIL_UnchangedRecords.QuoteNumber,
	FIL_UnchangedRecords.QuoteVersion,
	FIL_UnchangedRecords.BusinessClassCode,
	LKP_WBPolicyStaging.PenguinTechGenerated,
	FIL_UnchangedRecords.InternalExternalIndicator AS i_InternalExternalIndicator,
	-- *INF*: IIF(PenguinTechGenerated='T', 'External', i_InternalExternalIndicator)
	IFF(PenguinTechGenerated = 'T', 'External', i_InternalExternalIndicator) AS v_InternalExternalIndicator,
	v_InternalExternalIndicator AS o_InternalExternalIndicator,
	FIL_UnchangedRecords.RiskState,
	FIL_UnchangedRecords.AgencyAKID,
	FIL_UnchangedRecords.StrategicProfitCenterAKId,
	FIL_UnchangedRecords.InsuranceSegmentAKId,
	FIL_UnchangedRecords.PolicyOfferingAKId,
	FIL_UnchangedRecords.ProgramAKId,
	-- *INF*: IIF(QuoteKey=v_prev_QuoteKey,v_NEXTVAL, i_NEXTVAL)
	IFF(QuoteKey = v_prev_QuoteKey, v_NEXTVAL, i_NEXTVAL) AS v_NEXTVAL,
	-- *INF*: DECODE(TRUE,
	-- --QuoteKey=v_prev_QuoteKey,v_QuoteCreatedDate,
	-- ISNULL(i_QuoteCreatedDate),StatusDate,
	-- i_QuoteCreatedDate)
	DECODE(TRUE,
	i_QuoteCreatedDate IS NULL, StatusDate,
	i_QuoteCreatedDate) AS v_QuoteCreatedDate,
	QuoteKey AS v_prev_QuoteKey,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	-- *INF*: IIF(ISNULL(i_QuoteAKId),v_NEXTVAL,i_QuoteAKId)
	IFF(i_QuoteAKId IS NULL, v_NEXTVAL, i_QuoteAKId) AS QuoteAKId,
	-1 AS PolicyAKId,
	i_OtherReasonComment AS OtherReasonComment,
	v_QuoteCreatedDate AS QuoteCreatedDate,
	FIL_UnchangedRecords.EstimatedQuotePremium,
	FIL_UnchangedRecords.PolicyIssueCodeOverride AS i_PolicyIssueCodeOverride,
	-- *INF*: DECODE(i_PolicyIssueCodeOverride, 'T', 1, 'F', 0, NULL)  
	DECODE(i_PolicyIssueCodeOverride,
	'T', 1,
	'F', 0,
	NULL) AS o_PolicyIssueCodeOverride,
	FIL_UnchangedRecords.QuoteIssueCode,
	FIL_UnchangedRecords.TransactionDate,
	LKP_QuoteAKId.LkpQuoteIssueCode,
	LKP_QuoteAKId.LkpQuoteIssueCodeChangeDate,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(LkpQuoteIssueCodeChangeDate),TransactionDate,
	-- ISNULL(LkpQuoteIssueCode),TransactionDate,
	-- QuoteIssueCode = LkpQuoteIssueCode, LkpQuoteIssueCodeChangeDate,
	-- TransactionDate)
	DECODE(TRUE,
	LkpQuoteIssueCodeChangeDate IS NULL, TransactionDate,
	LkpQuoteIssueCode IS NULL, TransactionDate,
	QuoteIssueCode = LkpQuoteIssueCode, LkpQuoteIssueCodeChangeDate,
	TransactionDate) AS o_QuoteIssueCodeChangeDate,
	FIL_UnchangedRecords.IssuedUWID,
	FIL_UnchangedRecords.IssuedUnderwriter,
	FIL_UnchangedRecords.QuoteChannel,
	FIL_UnchangedRecords.LCSurveyOrderedIndicator,
	-- *INF*: DECODE(LCSurveyOrderedIndicator, 'T', '1', 'F','0', NULL)  
	DECODE(LCSurveyOrderedIndicator,
	'T', '1',
	'F', '0',
	NULL) AS o_LCSurveyOrderedIndicator,
	FIL_UnchangedRecords.LCSurveyOrderedDate,
	FIL_UnchangedRecords.QuoteChannelOrigin,
	FIL_UnchangedRecords.QuoteReasonClosedCode,
	FIL_UnchangedRecords.QuoteReasonClosedComments,
	FIL_UnchangedRecords.ServCenterSupportCode,
	FIL_UnchangedRecords.RolloverPolicyIndicator,
	FIL_UnchangedRecords.RolloverPriorCarrier
	FROM FIL_UnchangedRecords
	LEFT JOIN LKP_QuoteAKId
	ON LKP_QuoteAKId.QuoteKey = FIL_UnchangedRecords.QuoteKey
	LEFT JOIN LKP_Quotecreateddate
	ON LKP_Quotecreateddate.PolicyGUId = FIL_UnchangedRecords.QuoteKey
	LEFT JOIN LKP_WBPolicyStaging
	ON LKP_WBPolicyStaging.PolicyNumber = FIL_UnchangedRecords.QuoteNumber AND LKP_WBPolicyStaging.PolicyVersionFormatted = FIL_UnchangedRecords.QuoteVersion
),
Quote AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.Quote
	(CurrentSnapshotFlag, AuditID, SourceSystemID, CreatedDate, ModifiedDate, QuoteAKId, QuoteKey, PolicyAKId, AgencyAKId, QuoteNumber, QuoteVersion, BusinessClassCode, InternalExternalIndicator, RiskState, StrategicProfitCenterAKId, InsuranceSegmentAKId, PolicyOfferingAKId, ProgramAKId, QuoteStatusCode, UnderwritingAssociateAKId, AgencyEmployeeAKId, StatusDate, QuoteCreatedDate, ReasonCode, OtherReasonComment, QuoteEffectiveDate, QuoteExpirationDate, EstimatedQuotePremium, PolicyIssueCodeOverride, QuoteIssueCode, QuoteIssueCodeChangeDate, IssuedUWID, IssuedUnderwriter, QuoteChannel, LCSurveyOrderedIndicator, LCSurveyOrderedDate, QuoteChannelOrigin, QuoteReasonClosedCode, QuoteReasonClosedComments, RolloverPolicyIndicator, RolloverPriorCarrier, ServCenterSupportCode)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	QUOTEAKID, 
	QUOTEKEY, 
	POLICYAKID, 
	AgencyAKID AS AGENCYAKID, 
	QUOTENUMBER, 
	QUOTEVERSION, 
	BUSINESSCLASSCODE, 
	o_InternalExternalIndicator AS INTERNALEXTERNALINDICATOR, 
	RISKSTATE, 
	STRATEGICPROFITCENTERAKID, 
	INSURANCESEGMENTAKID, 
	POLICYOFFERINGAKID, 
	PROGRAMAKID, 
	QUOTESTATUSCODE, 
	UNDERWRITINGASSOCIATEAKID, 
	AGENCYEMPLOYEEAKID, 
	STATUSDATE, 
	QUOTECREATEDDATE, 
	REASONCODE, 
	OTHERREASONCOMMENT, 
	QUOTEEFFECTIVEDATE, 
	QUOTEEXPIRATIONDATE, 
	ESTIMATEDQUOTEPREMIUM, 
	o_PolicyIssueCodeOverride AS POLICYISSUECODEOVERRIDE, 
	QUOTEISSUECODE, 
	o_QuoteIssueCodeChangeDate AS QUOTEISSUECODECHANGEDATE, 
	ISSUEDUWID, 
	ISSUEDUNDERWRITER, 
	QUOTECHANNEL, 
	o_LCSurveyOrderedIndicator AS LCSURVEYORDEREDINDICATOR, 
	LCSURVEYORDEREDDATE, 
	QUOTECHANNELORIGIN, 
	QUOTEREASONCLOSEDCODE, 
	QUOTEREASONCLOSEDCOMMENTS, 
	ROLLOVERPOLICYINDICATOR, 
	ROLLOVERPRIORCARRIER, 
	SERVCENTERSUPPORTCODE
	FROM EXP_GetSupportIds
),
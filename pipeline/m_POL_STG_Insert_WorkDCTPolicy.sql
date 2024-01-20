WITH
LKP_CheckPolicyForReissueORRewrite AS (
	SELECT
	PolicyNumber,
	PolicyVersion
	FROM (
		select A.PolicyNumber AS PolicyNumber,A.PolicyVersion AS PolicyVersion 
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchWBPolicyStaging A
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchDCTransactionStaging B
		on A.SessionId=B.SessionId
		where B.Type in ('Reissue','Rewrite')
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber,PolicyVersion ORDER BY PolicyNumber) = 1
),
SQ_WorkDCTPolicy AS (
	SELECT  DCPolicyStaging.SessionId,
			DCPolicyStaging.PolicyId,
			DCPartyStaging.PartyId,
			DCPartyStaging.NAME,																			
			DCPartyStaging.FirstName,
			DCPartyStaging.LastName,
			DCPartyStaging.MiddleName,
			DCPartyAssociationStaging.EntityType,
			DCPartyAssociationStaging.FederalEmployeeIDNumber,
			DCPolicyStaging.SICCode,
			DCPolicyStaging.NAICSCode,
			WBPolicyStaging.Program,
			DCPolicyStaging.Id PolicyGUId,
			WBPartyStaging.CustomerNum,
			DCPolicyStaging.PolicyNumber,
			WBPolicyStaging.PolicyVersion,
			WBPolicyStaging.PolicyVersionFormatted,
			DCPolicyStaging.EffectiveDate PolicyEffectiveDate,
			DCPolicyStaging.ExpirationDate PolicyExpirationDate,
			DCPolicyStaging.LineOfBusiness,
			DCPolicyStaging.Term,
			DCPolicyStaging.PrimaryRatingState,
			DCPolicyStaging.Product,
			DCPolicyStaging.AuditPeriod,
			DCPolicyStaging.CancellationDate,
			DCPolicyStaging.TransactionDate,
			DCPolicyStaging.InceptionDate,
			DCTransactionStaging.Type TransactionType,
			WBPolicyStaging.Division,
			WBPolicyStaging.Terrorism,
			WBPolicyStaging.WBProduct,
			WBPolicyStaging.WBProductType,
			'N/A' AS RiskGrade,																		
			WBPolicyStaging.BCCCode,
			WBPolicyStaging.WB_PolicyId,
			WBPolicyStaging.AutomaticRenewalIndicator,
			WBPolicyStaging.Association,
			WBPolicyStaging.AssociationDiscountFactor,
			'N/A' AS LineType,
			WBPolicyStaging.PolicyProgram,
			DCPolicyStaging.STATUS PolicyStatus,
			ISNULL(DCTransactionStaging.TransactionDate, DCTransactionStaging.CreatedDate) TransactionCreatedDate,
			DCTransactionStaging.EffectiveDate TransactionEffectiveDate,
			DCTransactionStaging.ExpirationDate TransactionExpirationDate,
			DCTransactionStaging.CancellationDate TransactionCancellationDate,
			WBReasonStaging.Code AS ReasonCode,
			WBReasonStaging.CodeCaption  AS ReasonCodeCaption,
			DCTransactionStaging.STATE AS TransactionState,
			DCSessionStaging.Purpose AS TransactionPurpose,
			DCPolicyStaging.SICCodeDesc,
			DCPolicyStaging.NAICSCodeDesc,
			DCPartyStaging.Title,
			WBPartyStaging.DoingBusinessAs,																				
			WBTransactionStage.QuoteActionTimeStamp,
			WBTransactionStage.QuoteActionStatus,
			WBTransactionStage.QuoteActionUserClassification,
			WBTransactionStage.QuoteActionUserName,
			WBAgencyStaging.Reference AS AgencyCode,
			WBCLPolicyStage.IsApplicant,
			WBCLPolicyStage.RejectedReason,
			WBCLPolicyStage.RejectedReasonDetails,
			WBProducerStage.NAME AS ProducerName,
			DCContactStaging.PhoneNumber,
			WBPolicyStaging.customercare,
			WBBPPartyStage.BOP_New_BusinessSegment AS BusinessSegmentCode,
			DCTransactionStaging.IssuedUserName AS TransactionCreatedUserId,	
			WBPolicyStaging.EndorseProcessedBy AS EndorsedProcessedBy,
			WBCLPolicyStage.EstimatedQuotePremium,
			WBPolicyStaging.IsRollover,
			CASE WHEN WBPolicyStaging.IsRollover = 1 AND LTRIM(RTRIM(WBPolicyStaging.PriorCarrierName)) = 'Other'
				THEN WBPolicyStaging.PriorCarrierNameOther 
			ELSE PriorInsurance.CarrierNameOther 
			end as PriorCarrierNameOther,	
			WBCLPolicyStage.MailPolicyToInsured,
			WBTransactionStage.DataFix,
			WBTransactionStage.DataFixDate,
			WBTransactionStage.DataFixType,
			CASE 
				WHEN WBPolicyStaging.IsRollover = 1 
					THEN WBPolicyStaging.PriorCarrierName
				WHEN DCPolicyStaging.PreviousPolicyNumber IS NOT NULL 																
					THEN COALESCE(WBPolicyStaging.PriorCarrierName, PriorInsurance.CarrierName, 'WestBend')
				ELSE COALESCE(PriorInsurance.CarrierName, DCTPriorLookup.CarrierName)
			END AS PriorCarrierName,
	
			CASE
				WHEN (
						DCPolicyStaging.PreviousPolicyNumber IS NULL
						OR WBPolicyStaging.PreviousPolicyVersion IS NULL
						)
					AND (
						(
							PriorInsurance.CarrierName = 'WestBend'
							OR PriorInsurance.CarrierName IS NULL
							)
						AND (
							PriorInsurance.PolicyNumber IS NULL
							OR PriorInsurance.PolicyMod IS NULL
							)
						)
					THEN DCTPriorLookup.PolicySymbol
				ELSE CASE 
						WHEN PriorInsurance.CarrierName = 'WestBend'
							OR DCPolicyStaging.PreviousPolicyNumber IS NOT NULL
							THEN ISNULL(PriorInsurance.PolicySymbol, '000')					
						END
				END AS PriorPolicySymbol,
	
				CASE
				WHEN (
						DCPolicyStaging.PreviousPolicyNumber IS NULL
						OR WBPolicyStaging.PreviousPolicyVersion IS NULL
						)
					AND (
						(
							PriorInsurance.CarrierName = 'WestBend'
							OR PriorInsurance.CarrierName IS NULL
							)
						AND (
							PriorInsurance.PolicyNumber IS NULL
							OR PriorInsurance.PolicyMod IS NULL
							)
						)
					THEN DCTPriorLookup.PolicyNumber
				ELSE COALESCE(DCPolicyStaging.PreviousPolicyNumber, PriorInsurance.PolicyNumber)
				END AS PriorPolicyNumber,
	
				CASE
				WHEN (
						DCPolicyStaging.PreviousPolicyNumber IS NULL
						OR WBPolicyStaging.PreviousPolicyVersion IS NULL
						)
					AND (
						(
							PriorInsurance.CarrierName = 'WestBend'
							OR PriorInsurance.CarrierName IS NULL
							)
						AND (
							PriorInsurance.PolicyNumber IS NULL
							OR PriorInsurance.PolicyMod IS NULL
							)
						)
					THEN DCTPriorLookup.PolicyVersion	
					ELSE COALESCE(CASE 
							WHEN ISNUMERIC(WBPolicyStaging.PreviousPolicyVersion) = 1
								THEN RIGHT('00' + CAST(WBPolicyStaging.PreviousPolicyVersion AS VARCHAR(2)), 2)
							ELSE NULL
							END,PriorInsurance.PolicyMod)
				END AS PriorPolicyMod,
	
			CASE
				WHEN PriorInsurance.CarrierName = 'WestBend' THEN PriorInsurance.PolicyNumber
				WHEN DCPolicyStaging.PreviousPolicyNumber IS NULL AND
					WBPolicyStaging.PreviousPolicyVersion > 0 THEN DCPolicyStaging.PolicyNumber 				
				ELSE COALESCE(DCPolicyStaging.PreviousPolicyNumber,null)
			END AS 	PreviousPolicyNumber,
		WBPolicyStaging.PolicyIssueCodeDesc,
		WBPolicyStaging.PolicyIssueCodeOverride,
		WBTransactionStage.DeclaredEvent,
		WBPolicyStaging.IssuedUWID,
		WBPolicyStaging.IssuedUnderwriter,
		WBPolicyStaging.ExternalQuoteSource,
		WBPolicyStaging.TurnstileGenerated,
		WBPolicyStaging.PenguinTechGenerated,
		WBPolicyStaging.LCSurveyOrderedIndicator,
		WBPolicyStaging.LCSurveyOrderedDate,
		WBCLPolicyStage.ExpiredReason,
		WBCLPolicyStage.ExpiredReasonDetails
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCPolicyStaging DCPolicyStaging
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCSessionStaging DCSessionStaging
			ON DCSessionStaging.SessionId = DCPolicyStaging.SessionId
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBPolicyStaging WBPolicyStaging
			ON WBPolicyStaging.SessionId = DCPolicyStaging.SessionId
				AND WBPolicyStaging.PolicyId = DCPolicyStaging.PolicyId
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCTransactionStaging DCTransactionStaging
			ON DCTransactionStaging.SessionId = DCPolicyStaging.SessionId
				-- This accomodates obscure situations where for a given sessionID I get multiple HistoryIDs.  This situation
				-- should not occur, but has in the past so this logic deals with that anomaly by retrieving only the maximum
				-- (i.e. lastest) history ID for a given SessionID
				AND DCTransactionStaging.HistoryId IN (
					SELECT MAX(a.HistoryId)
					FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCTransactionStaging a
					WHERE a.SessionId = DCTransactionStaging.SessionId
					)
		Left Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBReasonStaging WBReasonStaging
		on WBReasonStaging.TransactionId=DCTransactionStaging.TransactionId
		and WBReasonStaging.SessionId=DCTransactionStaging.SessionId
		and not exists (
		select 1 from WBReasonStaging WBR
		where WBR.TransactionId=WBReasonStaging.TransactionId
		and WBR.Sessionid=WBReasonStaging.Sessionid
		and WBR.WB_ReasonId>WBReasonStaging.WB_ReasonId)
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCPartyAssociationStaging DCPartyAssociationStaging
			ON DCPartyAssociationStaging.SessionId = DCPolicyStaging.SessionId
				AND DCPartyAssociationStaging.PartyAssociationType = 'Account'
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCPartyStaging DCPartyStaging
			ON DCPartyStaging.SessionId = DCPartyAssociationStaging.SessionId
				AND DCPartyStaging.PartyId = DCPartyAssociationStaging.PartyId
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBPartyStaging WBPartyStaging
			ON WBPartyStaging.SessionId = DCPartyAssociationStaging.SessionId
				AND WBPartyStaging.PartyId = DCPartyAssociationStaging.PartyId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBTransactionStage WBTransactionStage
			ON WBTransactionStage.SessionId = DCPolicyStaging.SessionId
				AND WBTransactionStage.TransactionId = DCTransactionStaging.TransactionId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBAgencyStaging WBAgencyStaging
			ON WBAgencyStaging.SessionId = DCPolicyStaging.SessionId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCLPolicyStage WBCLPolicyStage
			ON WBCLPolicyStage.SessionId = DCPolicyStaging.SessionId
				AND WBCLPolicyStage.WB_PolicyId = WBPolicyStaging.WB_PolicyId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBProducerStage WBProducerStage
			ON WBProducerStage.SessionId = DCPolicyStaging.SessionId
				AND WBProducerStage.PolicyId = DCPolicyStaging.PolicyId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCContactStaging DCContactStaging
			ON DCContactStaging.SessionId = DCPolicyStaging.SessionId
				AND DCContactStaging.Type = 'Primary'
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCLPartyStage WBCLPartyStage
			ON WBCLPartyStage.SessionId = DCPolicyStaging.SessionId
				AND WBCLPartyStage.WB_PartyId = WBPartyStaging.WB_PartyId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBBPPartyStage WBBPPartyStage
			ON WBBPPartyStage.SessionId = DCPolicyStaging.SessionId
				AND WBBPPartyStage.WB_CL_PartyId = WBCLPartyStage.WB_CL_PartyId
		OUTER APPLY (
			 --Get the policy key information from the Prior Insurance tables for one and only one policy 
			SELECT TOP 1 dpi.CarrierName,
				wpi.CarrierNameOther,
				wcpi.PolicySymbol,
				dpi.PolicyNumber,
				CASE 
					WHEN ISNUMERIC(wcpi.PolicyMod) = 1
						THEN RIGHT('00' + CAST(wcpi.PolicyMod AS VARCHAR(2)), 2)
					ELSE NULL
					END AS PolicyMod
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCPriorInsuranceStaging dpi
			INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBPriorInsuranceStage wpi
				ON dpi.PriorInsuranceId = wpi.PriorInsuranceId
					AND dpi.SessionId = wpi.SessionId
			INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCLPriorInsuranceStage wcpi
				ON wpi.WBPriorInsuranceId = wcpi.WBPriorInsuranceId
					AND wpi.SessionId = wcpi.SessionId
			WHERE dpi.PolicyId = DCPolicyStaging.PolicyId
				AND dpi.SessionId = DCPolicyStaging.SessionId
			ORDER BY wcpi.PolicySymbol,
				dpi.PolicyNumber,
				wcpi.PolicyMod
			) PriorInsurance
		OUTER APPLY (
			-- Get the policy key information with same policy number that has the highest version that is less than the current version
			SELECT TOP 1 'WestBend' AS CarrierName,
				'000' AS PolicySymbol,
				a.PolicyNumber,
				CASE 
					WHEN ISNUMERIC(b.PolicyVersion) = 1
						THEN RIGHT('00' + CAST(b.PolicyVersion AS VARCHAR(2)), 2)
					ELSE NULL
					END AS PolicyVersion
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCPolicyStaging a
			INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBPolicyStaging b
				ON a.policyid = b.policyid
					AND a.SessionId = b.SessionId
			WHERE a.policynumber = DCPolicyStaging.PolicyNumber
				AND b.PolicyVersion < WBPolicyStaging.PolicyVersion
			ORDER BY b.PolicyVersion DESC
			) DCTPriorLookup
),
EXP_Default AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SessionId,
	PolicyId,
	PartyId,
	Name,
	FirstName,
	LastName,
	MiddleName,
	EntityType,
	FederalEmployeeIDNumber,
	SICCode,
	NAICSCode,
	Program,
	PolicyGUId,
	CustomerNum,
	PolicyNumber,
	PolicyVersion,
	PolicyVersionFormatted,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	LineOfBusiness,
	Term,
	PrimaryRatingState,
	Product,
	AuditPeriod,
	CancellationDate,
	TransactionDate,
	PriorPolicySymbol,
	-- *INF*: IIF(ISNULL(PriorPolicySymbol),'',ltrim(rtrim(PriorPolicySymbol)))
	IFF(PriorPolicySymbol IS NULL, '', ltrim(rtrim(PriorPolicySymbol))) AS v_PriorPolicySymbol,
	PriorPolicyNumber,
	-- *INF*: IIF(ISNULL(PriorPolicyNumber),'',ltrim(rtrim(PriorPolicyNumber)))
	IFF(PriorPolicyNumber IS NULL, '', ltrim(rtrim(PriorPolicyNumber))) AS v_PriorPolicyNumber,
	PriorPolicyMod,
	-- *INF*: IIF(ISNULL(PriorPolicyMod),'',ltrim(rtrim(PriorPolicyMod)))
	IFF(PriorPolicyMod IS NULL, '', ltrim(rtrim(PriorPolicyMod))) AS v_PriorPolicyMod,
	-- *INF*: IIF(v_PriorPolicySymbol='000',v_PriorPolicyNumber||v_PriorPolicyMod,v_PriorPolicySymbol||v_PriorPolicyNumber||v_PriorPolicyMod)
	-- 
	-- --IIF(ISNULL(v_PriorPolicySymbol||v_PreviousPolicyNumber||v_PriorPolicyMod),'',v_PriorPolicySymbol||v_PreviousPolicyNumber||v_PriorPolicyMod)
	IFF(
	    v_PriorPolicySymbol = '000', v_PriorPolicyNumber || v_PriorPolicyMod,
	    v_PriorPolicySymbol || v_PriorPolicyNumber || v_PriorPolicyMod
	) AS v_PriorPolicyKey_Check,
	-- *INF*: IIF(PriorCarrierName='WestBend' or ISNULL(PriorCarrierName),v_PriorPolicyKey_Check,'')
	IFF(PriorCarrierName = 'WestBend' or PriorCarrierName IS NULL, v_PriorPolicyKey_Check, '') AS v_PriorPolicyKey_Check2,
	-- *INF*: IIF(ISNULL(v_PriorPolicyKey_Check2),'', v_PriorPolicyKey_Check2)
	-- 
	-- --IIF(ISNULL(v_PriorPolicySymbol||v_PreviousPolicyNumber||v_PriorPolicyMod),'',v_PriorPolicySymbol||v_PreviousPolicyNumber||v_PriorPolicyMod)
	IFF(v_PriorPolicyKey_Check2 IS NULL, '', v_PriorPolicyKey_Check2) AS v_PriorPolicyKey,
	InceptionDate,
	TransactionType,
	Division,
	Terrorism,
	WBProduct,
	WBProductType,
	RiskGrade,
	BCCCode,
	WBPolicyId,
	AutomaticRenewalIndicator AS i_AutomaticRenewalIndicator,
	-- *INF*: DECODE(i_AutomaticRenewalIndicator,'T','1','F','0',NULL)
	DECODE(
	    i_AutomaticRenewalIndicator,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_AutomaticRenewalIndicator,
	Association,
	AssociationDiscountFactor,
	LineType,
	PolicyProgram,
	v_PriorPolicyKey AS PriorPolicyKey,
	-- *INF*: IIF(PriorCarrierName='WestBend',IIF(ISNULL(:LKP.LKP_CHECKPOLICYFORREISSUEORREWRITE(PolicyNumber,PolicyVersion))=1,'1','0'),'0')
	IFF(
	    PriorCarrierName = 'WestBend',
	    IFF(
	        LKP_CHECKPOLICYFORREISSUEORREWRITE_PolicyNumber_PolicyVersion.PolicyNumber IS NULL = 1,
	        '1',
	        '0'
	    ),
	    '0'
	) AS v_RenewalPolicyFlag,
	-- *INF*: IIF(v_RenewalPolicyFlag='1',v_PriorPolicySymbol,NULL)
	IFF(v_RenewalPolicyFlag = '1', v_PriorPolicySymbol, NULL) AS RenewalPolicySymbol,
	-- *INF*: IIF(v_RenewalPolicyFlag='1',v_PriorPolicyNumber,NULL)
	IFF(v_RenewalPolicyFlag = '1', v_PriorPolicyNumber, NULL) AS RenewalPolicyNumber,
	-- *INF*: IIF(v_RenewalPolicyFlag='1',v_PriorPolicyMod,NULL)
	IFF(v_RenewalPolicyFlag = '1', v_PriorPolicyMod, NULL) AS RenewalPolicyMod,
	PolicyStatus,
	TransactionCreatedDate,
	TransactionEffectiveDate,
	TransactionExpirationDate,
	TransactionCancellationDate,
	ReasonCode,
	ReasonCodeCaption,
	TransactionState,
	TransactionPurpose,
	SICCodeDesc,
	NAICSCodeDesc,
	Title,
	DoingBusinessAs,
	QuoteActionTimeStamp,
	QuoteActionStatus,
	QuoteActionUserClassification,
	QuoteActionUserName,
	AgencyCode,
	IsApplicant,
	RejectedReason,
	RejectedReasonDetails,
	ProducerName,
	PhoneNumber,
	CustomerCare,
	BusinessSegmentCode,
	TransactionCreatedUserId,
	-- *INF*: IIF (isnull(TransactionCreatedUserId),'N/A',
	-- SUBSTR(TransactionCreatedUserId,
	--                   INSTR(TransactionCreatedUserId,'\')+1,LENGTH(TransactionCreatedUserId)))
	IFF(
	    TransactionCreatedUserId IS NULL, 'N/A',
	    SUBSTR(TransactionCreatedUserId, REGEXP_INSTR(TransactionCreatedUserId, '\') + 1, LENGTH(TransactionCreatedUserId))
	) AS o_TransactionCreateduserId,
	EndorsedProcessedby AS EndorseProcessedby,
	-- *INF*: IIF(ISNULL(EndorseProcessedby),'N/A',EndorseProcessedby)
	IFF(EndorseProcessedby IS NULL, 'N/A', EndorseProcessedby) AS o_EndorseProcessedby,
	EstimatedQuotePremium,
	IsRollover AS i_IsRollover,
	-- *INF*: IIF(i_IsRollover='T','1','0')
	IFF(i_IsRollover = 'T', '1', '0') AS o_IsRollover,
	PriorCarrierName,
	PirorCarrierNameOther,
	MailPolicyToInsured AS i_MailPolicyToInsured,
	-- *INF*: IIF(i_MailPolicyToInsured='T','1','0')
	IFF(i_MailPolicyToInsured = 'T', '1', '0') AS o_MailPolicyToInsured,
	DataFix,
	DataFixDate,
	DataFixType,
	PreviousPolicyNumber,
	PolicyIssueCodeDesc,
	PolicyIssueCodeOverride AS i_PolicyIssueCodeOverride,
	-- *INF*: DECODE(i_PolicyIssueCodeOverride, 'T', 1, 'F', 0)
	DECODE(
	    i_PolicyIssueCodeOverride,
	    'T', 1,
	    'F', 0
	) AS o_PolicyIssueCodeOverride,
	DeclaredEvent AS i_DeclaredEvent,
	-- *INF*: DECODE(TRUE,i_DeclaredEvent ='T',1,
	-- i_DeclaredEvent = 'F',0,
	-- ISNULL(i_DeclaredEvent),0
	-- )
	-- 
	-- 
	DECODE(
	    TRUE,
	    i_DeclaredEvent = 'T', 1,
	    i_DeclaredEvent = 'F', 0,
	    i_DeclaredEvent IS NULL, 0
	) AS o_DeclaredEvent,
	IssuedUWID,
	IssuedUnderwriter,
	ExternalQuoteSource,
	TurnstileGenerated AS i_TurnstileGenerated,
	-- *INF*: DECODE(i_TurnstileGenerated, 'T', 1, 'F', 0)
	DECODE(
	    i_TurnstileGenerated,
	    'T', 1,
	    'F', 0
	) AS o_TurnstileGenerated,
	PenguinTechGenerated AS i_PenguinTechGenerated,
	-- *INF*: DECODE(i_PenguinTechGenerated, 'T', 1, 'F', 0)
	DECODE(
	    i_PenguinTechGenerated,
	    'T', 1,
	    'F', 0
	) AS o_PenguinTechGenerated,
	LCSurveyOrderedIndicator,
	-- *INF*: DECODE(LCSurveyOrderedIndicator, 'T', '1', 'F','0',null)
	DECODE(
	    LCSurveyOrderedIndicator,
	    'T', '1',
	    'F', '0',
	    null
	) AS o_LCSurveyOrderedIndicator,
	LCSurveyOrderedDate,
	ExpiredReason AS i_ExpiredReason,
	-- *INF*: IIF(QuoteActionUserClassification = 'System' AND QuoteActionUserName = 'admin' AND QuoteActionStatus = 'Closed', '11', i_ExpiredReason)
	IFF(
	    QuoteActionUserClassification = 'System'
	    and QuoteActionUserName = 'admin'
	    and QuoteActionStatus = 'Closed',
	    '11',
	    i_ExpiredReason
	) AS v_ExpiredReason,
	v_ExpiredReason AS o_ExpiredReason,
	ExpiredReasonDetails
	FROM SQ_WorkDCTPolicy
	LEFT JOIN LKP_CHECKPOLICYFORREISSUEORREWRITE LKP_CHECKPOLICYFORREISSUEORREWRITE_PolicyNumber_PolicyVersion
	ON LKP_CHECKPOLICYFORREISSUEORREWRITE_PolicyNumber_PolicyVersion.PolicyNumber = PolicyNumber
	AND LKP_CHECKPOLICYFORREISSUEORREWRITE_PolicyNumber_PolicyVersion.PolicyVersion = PolicyVersion

),
WorkDCTPolicy AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkDCTPolicy;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkDCTPolicy
	(ExtractDate, SourceSystemId, SessionId, PolicyId, PartyId, Name, FirstName, LastName, MiddleName, EntityType, FederalEmployeeIDNumber, SICCode, NAICSCode, Program, PolicyGUId, CustomerNum, PolicyNumber, PolicyVersion, PolicyVersionFormatted, PolicyEffectiveDate, PolicyExpirationDate, LineOfBusiness, Term, PrimaryRatingState, Product, AuditPeriod, CancellationDate, TransactionDate, PreviousPolicyNumber, InceptionDate, TransactionType, Division, Terrorism, WBProduct, WBProductType, RiskGrade, BCCCode, WBPolicyId, AutomaticRenewalIndicator, Association, AssociationDiscountFactor, LineType, PolicyProgram, PriorPolicyKey, RenewalPolicySymbol, RenewalPolicyNumber, RenewalPolicyMod, PolicyStatus, TransactionCreatedDate, TransactionEffectiveDate, TransactionExpirationDate, TransactionCancellationDate, ReasonCode, ReasonCodeCaption, TransactionState, TransactionPurpose, SICCodeDesc, NAICSCodeDesc, Title, DoingBusinessAs, QuoteActionTimeStamp, QuoteActionStatus, QuoteActionUserClassification, QuoteActionUserName, AgencyCode, IsApplicant, RejectedReason, RejectedReasonDetails, ProducerName, PhoneNumber, CustomerCare, BusinessSegmentCode, TransactionCreatedUserId, EndorsedProcessedBy, EstimatedQuotePremium, IsRollover, PriorCarrierName, PirorCarrierNameOther, MailPolicyToInsured, DataFix, DataFixDate, DataFixType, PolicyIssueCodeDesc, PolicyIssueCodeOverride, DeclaredEvent, IssuedUWID, IssuedUnderwriter, ExternalQuoteSource, TurnstileGenerated, PenguinTechGenerated, LCSurveyOrderedIndicator, LCSurveyOrderedDate, ExpiredReason, ExpiredReasonDetails)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	SESSIONID, 
	POLICYID, 
	PARTYID, 
	NAME, 
	FIRSTNAME, 
	LASTNAME, 
	MIDDLENAME, 
	ENTITYTYPE, 
	FEDERALEMPLOYEEIDNUMBER, 
	SICCODE, 
	NAICSCODE, 
	PROGRAM, 
	POLICYGUID, 
	CUSTOMERNUM, 
	POLICYNUMBER, 
	POLICYVERSION, 
	POLICYVERSIONFORMATTED, 
	POLICYEFFECTIVEDATE, 
	POLICYEXPIRATIONDATE, 
	LINEOFBUSINESS, 
	TERM, 
	PRIMARYRATINGSTATE, 
	PRODUCT, 
	AUDITPERIOD, 
	CANCELLATIONDATE, 
	TRANSACTIONDATE, 
	PREVIOUSPOLICYNUMBER, 
	INCEPTIONDATE, 
	TRANSACTIONTYPE, 
	DIVISION, 
	TERRORISM, 
	WBPRODUCT, 
	WBPRODUCTTYPE, 
	RISKGRADE, 
	BCCCODE, 
	WBPOLICYID, 
	o_AutomaticRenewalIndicator AS AUTOMATICRENEWALINDICATOR, 
	ASSOCIATION, 
	ASSOCIATIONDISCOUNTFACTOR, 
	LINETYPE, 
	POLICYPROGRAM, 
	PRIORPOLICYKEY, 
	RENEWALPOLICYSYMBOL, 
	RENEWALPOLICYNUMBER, 
	RENEWALPOLICYMOD, 
	POLICYSTATUS, 
	TRANSACTIONCREATEDDATE, 
	TRANSACTIONEFFECTIVEDATE, 
	TRANSACTIONEXPIRATIONDATE, 
	TRANSACTIONCANCELLATIONDATE, 
	REASONCODE, 
	REASONCODECAPTION, 
	TRANSACTIONSTATE, 
	TRANSACTIONPURPOSE, 
	SICCODEDESC, 
	NAICSCODEDESC, 
	TITLE, 
	DOINGBUSINESSAS, 
	QUOTEACTIONTIMESTAMP, 
	QUOTEACTIONSTATUS, 
	QUOTEACTIONUSERCLASSIFICATION, 
	QUOTEACTIONUSERNAME, 
	AGENCYCODE, 
	ISAPPLICANT, 
	REJECTEDREASON, 
	REJECTEDREASONDETAILS, 
	PRODUCERNAME, 
	PHONENUMBER, 
	CUSTOMERCARE, 
	BUSINESSSEGMENTCODE, 
	o_TransactionCreateduserId AS TRANSACTIONCREATEDUSERID, 
	o_EndorseProcessedby AS ENDORSEDPROCESSEDBY, 
	ESTIMATEDQUOTEPREMIUM, 
	o_IsRollover AS ISROLLOVER, 
	PRIORCARRIERNAME, 
	PIRORCARRIERNAMEOTHER, 
	o_MailPolicyToInsured AS MAILPOLICYTOINSURED, 
	DATAFIX, 
	DATAFIXDATE, 
	DATAFIXTYPE, 
	POLICYISSUECODEDESC, 
	o_PolicyIssueCodeOverride AS POLICYISSUECODEOVERRIDE, 
	o_DeclaredEvent AS DECLAREDEVENT, 
	ISSUEDUWID, 
	ISSUEDUNDERWRITER, 
	EXTERNALQUOTESOURCE, 
	o_TurnstileGenerated AS TURNSTILEGENERATED, 
	o_PenguinTechGenerated AS PENGUINTECHGENERATED, 
	o_LCSurveyOrderedIndicator AS LCSURVEYORDEREDINDICATOR, 
	LCSURVEYORDEREDDATE, 
	o_ExpiredReason AS EXPIREDREASON, 
	EXPIREDREASONDETAILS
	FROM EXP_Default
),
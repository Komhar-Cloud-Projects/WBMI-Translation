WITH
LKP_SupWCPOLS AS (
	SELECT
	WCPOLSCode,
	SourcesystemID,
	SourceCode,
	TableName,
	ProcessName,
	i_SourcesystemID,
	i_SourceCode,
	i_TableName,
	i_ProcessName
	FROM (
		SELECT
		     WCPOLSCode as WCPOLSCode
			,SourcesystemID as SourcesystemID
			,SourceCode as SourceCode
			,TableName as TableName
			,ProcessName as ProcessName
		FROM SupWCPOLS
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SourcesystemID,SourceCode,TableName,ProcessName ORDER BY WCPOLSCode) = 1
),
LKP_WorkWCLimit AS (
	SELECT
	LimitValue,
	i_WCTrackHistoryID,
	i_LimitType,
	WCTrackHistoryID,
	LimitType
	FROM (
		SELECT WCTrackHistoryID as WCTrackHistoryID
		      ,LimitType as LimitType
		      ,LimitValue as LimitValue
		  FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCLimit
		WHERE Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WCTrackHistoryID,LimitType ORDER BY LimitValue) = 1
),
SQ_WCPols00Record AS (
	SELECT
		WCTrackHistoryID,
		LinkData,
	     AuditId
	FROM dbo.WCPols00Record
	WHERE 1=1
	AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE_01}
),
EXP_PrepLKP_01 AS (
	SELECT
	WCTrackHistoryID,
	LinkData,
	AuditId,
	'DCT' AS o_SourceSystemID,
	'As needed for this lookup type' AS o_ProcessName
	FROM SQ_WCPols00Record
),
LKP_Forms AS (
	SELECT
	WCTrackHistoryID,
	FormName,
	in_WCTrackHistoryID
	FROM (
		Select 
		WCTrackHistoryID as WCTrackHistoryID,
		FormName as FormName
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCForms
		WHERE 1=1
		AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
		ORDER BY WCTrackHistoryID -----
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WCTrackHistoryID ORDER BY WCTrackHistoryID) = 1
),
LKP_Line AS (
	SELECT
	AuditPeriod,
	RatingPlan,
	PrimaryLocationState,
	InterstateRiskID,
	MinimumPremiumMaximum,
	MinimumPremiumMaximumState,
	InstallmentType,
	DepositPremium,
	WCTrackHistoryID
	FROM (
		SELECT 
			AuditPeriod,
			RatingPlan,
			PrimaryLocationState,
			InterstateRiskID,
			MinimumPremiumMaximum,
			MinimumPremiumMaximumState,
			InstallmentType,
			DepositPremium,
			WCTrackHistoryID
		FROM WorkWCLine
		WHERE Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WCTrackHistoryID ORDER BY AuditPeriod) = 1
),
LKP_Party_Account AS (
	SELECT
	WCTrackHistoryID,
	EntityType,
	EntityOtherType,
	in_WCTrackHistoryID
	FROM (
		SELECT WCTrackHistoryID as WCTrackHistoryID
		      ,EntityType as EntityType
		      ,EntityOtherType as EntityOtherType
		  FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCParty
		  WHERE Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
		AND PartyAssociationType = 'Account'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WCTrackHistoryID ORDER BY WCTrackHistoryID) = 1
),
LKP_Party_Agent AS (
	SELECT
	Name,
	WCTrackHistoryID
	FROM (
		SELECT WCTrackHistoryID as WCTrackHistoryID
		      ,Name as Name
		  FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCParty
		  WHERE Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
		AND PartyAssociationType = 'Agency'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WCTrackHistoryID ORDER BY Name) = 1
),
LKP_Policy AS (
	SELECT
	PolicyTerm,
	PolicyExpirationDate_YYMMDD,
	TransactionEffectiveDate_YYMMDD,
	TransactionExpirationDate_YYMMDD,
	PreviousPolicyKey,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	WCTrackHistoryID
	FROM (
		SELECT WCTrackHistoryID as WCTrackHistoryID
		      ,PolicyTerm as PolicyTerm
		      ,CONVERT(VARCHAR(6), PolicyExpirationDate, 12) as PolicyExpirationDate_YYMMDD
		      ,CONVERT(VARCHAR(6), TransactionEffectiveDate, 12) as TransactionEffectiveDate_YYMMDD
			  ,CONVERT(VARCHAR(6), TransactionExpirationDate, 12) as TransactionExpirationDate_YYMMDD
		      ,TransactionType as TransactionType
		      ,PreviousPolicyKey as PreviousPolicyKey
		      ,PolicyEffectiveDate as PolicyEffectiveDate
		      ,PolicyExpirationDate as PolicyExpirationDate
		  FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCPolicy
		WHERE Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WCTrackHistoryID ORDER BY PolicyTerm) = 1
),
LKP_StateTerm AS (
	SELECT
	EmployeeLeasing,
	EmployeeLeasingRatingOption,
	IntrastateRiskid,
	TotalStandardPremium,
	WCTrackHistoryID
	FROM (
		SELECT A.EmployeeLeasing as EmployeeLeasing, 
		max(A.EmployeeLeasingRatingOption) as EmployeeLeasingRatingOption, 
		Max(A.IntrastateRiskid) as IntrastateRiskid, 
		Sum(A.TotalStandardPremium) as TotalStandardPremium, 
		A.WCTrackHistoryID as WCTrackHistoryID 
		FROM WorkWCStateTerm A
		where Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
		group by A.WCTrackHistoryID,A.EmployeeLeasing
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WCTrackHistoryID ORDER BY EmployeeLeasing) = 1
),
EXP_Format_01_Output AS (
	SELECT
	CURRENT_TIMESTAMP AS ExtractDate,
	EXP_PrepLKP_01.WCTrackHistoryID,
	EXP_PrepLKP_01.LinkData,
	LKP_Line.InterstateRiskID AS InterstateRiskIDNumber,
	LKP_Policy.PolicyTerm,
	LKP_Policy.PolicyExpirationDate_YYMMDD,
	LKP_Policy.PreviousPolicyKey AS PriorPolicyNumberIdentifier,
	LKP_StateTerm.IntrastateRiskid,
	LKP_StateTerm.EmployeeLeasing,
	LKP_StateTerm.EmployeeLeasingRatingOption,
	LKP_Line.RatingPlan,
	LKP_Line.MinimumPremiumMaximum AS PolicyMinimumPremiumAmount,
	LKP_StateTerm.TotalStandardPremium AS PolicyEstimatedStandardPremiumTotal,
	-- *INF*: TO_CHAR(ROUND(PolicyEstimatedStandardPremiumTotal))
	TO_CHAR(ROUND(PolicyEstimatedStandardPremiumTotal)) AS o_PolicyEstimatedStandardPremiumTotal,
	LKP_Line.AuditPeriod,
	LKP_Party_Agent.Name AS NameOfProducer,
	LKP_Policy.TransactionEffectiveDate_YYMMDD AS PolicyChangeEffectiveDate,
	LKP_Policy.TransactionExpirationDate_YYMMDD AS PolicyChangeExpirationDate,
	-- *INF*: :LKP.LKP_WorkWCLimit(WCTrackHistoryID,'EachAccident')
	LKP_WORKWCLIMIT_WCTrackHistoryID_EachAccident.LimitValue AS v_Limit_EachAccident,
	-- *INF*: :LKP.LKP_WorkWCLimit(WCTrackHistoryID,'Policy')
	LKP_WORKWCLIMIT_WCTrackHistoryID_Policy.LimitValue AS v_Limit_Policy,
	-- *INF*: :LKP.LKP_WorkWCLimit(WCTrackHistoryID,'EachEmployeeDisease')
	LKP_WORKWCLIMIT_WCTrackHistoryID_EachEmployeeDisease.LimitValue AS v_Limit_EachEmployeeDisease,
	LKP_Party_Account.EntityType,
	LKP_Party_Account.EntityOtherType,
	LKP_Line.PrimaryLocationState,
	LKP_Line.MinimumPremiumMaximumState,
	LKP_Line.InstallmentType,
	LKP_Line.DepositPremium,
	-- *INF*: TO_CHAR(ROUND(DepositPremium))
	TO_CHAR(ROUND(DepositPremium)) AS o_PolicyDepositPremiumAmount,
	-- *INF*: IIF ((IsNull(InterstateRiskIDNumber) or Is_Spaces(InterstateRiskIDNumber) or InterstateRiskIDNumber='0'),0,1)
	IFF(
	    (InterstateRiskIDNumber IS NULL
	    or LENGTH(InterstateRiskIDNumber)>0
	    and TRIM(InterstateRiskIDNumber)=''
	    or InterstateRiskIDNumber = '0'),
	    0,
	    1
	) AS v_INTERstateID,
	-- *INF*: IIF ((IsNull(IntrastateRiskid) or Is_Spaces(IntrastateRiskid) or IntrastateRiskid='0'),0,1)
	IFF(
	    (IntrastateRiskid IS NULL
	    or LENGTH(IntrastateRiskid)>0
	    and TRIM(IntrastateRiskid)=''
	    or IntrastateRiskid = '0'),
	    0,
	    1
	) AS v_INTRAstateID,
	-- *INF*: DECODE(TRUE,
	-- v_INTERstateID=1 AND v_INTRAstateID=0,1,
	-- v_INTERstateID=1 AND v_INTRAstateID=1,2,
	-- v_INTERstateID=0 AND v_INTRAstateID=1,3,
	-- 5)
	-- 
	-- 
	-- 
	-- 
	DECODE(
	    TRUE,
	    v_INTERstateID = 1 AND v_INTRAstateID = 0, 1,
	    v_INTERstateID = 1 AND v_INTRAstateID = 1, 2,
	    v_INTERstateID = 0 AND v_INTRAstateID = 1, 3,
	    5
	) AS v_ExperienceRatingCode,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	CURRENT_TIMESTAMP AS o_ExtractDate,
	'01' AS o_RecordTypeCode,
	v_ExperienceRatingCode AS o_ExperienceRatingCode,
	LKP_Forms.FormName,
	-- *INF*: IIF(FormName='WC000516','05','01')
	IFF(FormName = 'WC000516', '05', '01') AS o_TypeOfCoverageIDCode,
	-- *INF*: IIF (EmployeeLeasing='T',EmployeeLeasingRatingOption,1)
	IFF(EmployeeLeasing = 'T', EmployeeLeasingRatingOption, 1) AS o_EmployeeLeasingPolicyTypeCode,
	LKP_Policy.PolicyEffectiveDate,
	LKP_Policy.PolicyExpirationDate,
	-- *INF*: IIF(DATE_DIFF(PolicyExpirationDate,PolicyEffectiveDate,'DD')>=365,1,4)
	-- 
	-- 
	-- --IIF ((PolicyTerm >= 0 and PolicyTerm < 12),4,
	--    --IIF (PolicyTerm = 12,1,2))
	IFF(DATEDIFF(DAY,PolicyExpirationDate,PolicyEffectiveDate) >= 365, 1, 4) AS o_PolicyTermCode,
	-- *INF*: :LKP.LKP_SupWCPOLS('DCT',EntityType,'WCPOLS01Record','LegalNatureOfInsuredCode')
	LKP_SUPWCPOLS__DCT_EntityType_WCPOLS01Record_LegalNatureOfInsuredCode.WCPOLSCode AS v_LegalNatureOfInsuredCode,
	v_LegalNatureOfInsuredCode AS o_LegalNatureOfInsuredCode,
	-- *INF*: IIF (RatingPlan = 'WCPool',:LKP.LKP_SupWCPOLS('DCT','WCPool','WCPOLS01Record','TypeOfPlanIdCode'),'1')
	IFF(
	    RatingPlan = 'WCPool', LKP_SUPWCPOLS__DCT_WCPool_WCPOLS01Record_TypeOfPlanIdCode.WCPOLSCode,
	    '1'
	) AS o_TypeOfPlanIDCode,
	'2' AS o_WrapUpOwnerControlledInsuranceProgramCode,
	-- *INF*: IIF (TRUNC(PolicyMinimumPremiumAmount) = PolicyMinimumPremiumAmount, to_char(PolicyMinimumPremiumAmount), to_char(to_decimal(PolicyMinimumPremiumAmount,2)))
	IFF(
	    TRUNC(PolicyMinimumPremiumAmount) = PolicyMinimumPremiumAmount,
	    to_char(PolicyMinimumPremiumAmount),
	    to_char(CAST(PolicyMinimumPremiumAmount AS FLOAT))
	) AS o_PolicyMinimumPremiumAmount,
	-- *INF*: :LKP.LKP_SupWCPOLS('DCT',MinimumPremiumMaximumState,'WCPOLS01Record','StateCodeRecord01')
	LKP_SUPWCPOLS__DCT_MinimumPremiumMaximumState_WCPOLS01Record_StateCodeRecord01.WCPOLSCode AS o_PolicyMinimumPremiumStateCode,
	-- *INF*: IIF (IsNull(:LKP.LKP_SupWCPOLS('DCT',AuditPeriod,'WCPOLS01Record','AuditFrequencyCode')),'5',:LKP.LKP_SupWCPOLS('DCT',AuditPeriod,'WCPOLS01Record','AuditFrequencyCode'))
	-- 
	-- 
	IFF(
	    LKP_SUPWCPOLS__DCT_AuditPeriod_WCPOLS01Record_AuditFrequencyCode.WCPOLSCode IS NULL, '5',
	    LKP_SUPWCPOLS__DCT_AuditPeriod_WCPOLS01Record_AuditFrequencyCode.WCPOLSCode
	) AS o_AuditFrequencyCode,
	-- *INF*: :LKP.LKP_SupWCPOLS('DCT',InstallmentType,'WCPOLS01Record','BillingFrequencyCode')
	LKP_SUPWCPOLS__DCT_InstallmentType_WCPOLS01Record_BillingFrequencyCode.WCPOLSCode AS o_BillingFrequencyCode,
	-- *INF*: DECODE(TRUE,
	-- RatingPlan <> 'Retrospective','3',
	-- RatingPlan = 'Retrospective' AND IN(PrimaryLocationState,'MI','MN','NC','WI'),'1',
	-- '5')
	-- 
	DECODE(
	    TRUE,
	    RatingPlan <> 'Retrospective', '3',
	    RatingPlan = 'Retrospective' AND PrimaryLocationState IN ('MI','MN','NC','WI'), '1',
	    '5'
	) AS o_RetrospectiveRatingCode,
	-- *INF*: IIF (Is_Number(v_Limit_EachAccident), To_Bigint(v_Limit_EachAccident) * 1000, 0)
	IFF(
	    REGEXP_LIKE(v_Limit_EachAccident, '^[0-9]+$'), CAST(v_Limit_EachAccident AS BIGINT) * 1000,
	    0
	) AS o_EmployerLiabilityLimitAmountBodilyInjuryByAccidentEachAccidentAmount,
	-- *INF*: IIF (Is_Number(v_Limit_Policy), To_Bigint(v_Limit_Policy) * 1000, 0)
	IFF(REGEXP_LIKE(v_Limit_Policy, '^[0-9]+$'), CAST(v_Limit_Policy AS BIGINT) * 1000, 0) AS o_EmployerLiabilityLimitAmountBodilyInjuryByDiseasePolicyLimitAmount,
	-- *INF*: IIF (Is_Number(v_Limit_EachEmployeeDisease), To_Bigint(v_Limit_EachEmployeeDisease) * 1000, 0)
	IFF(
	    REGEXP_LIKE(v_Limit_EachEmployeeDisease, '^[0-9]+$'),
	    CAST(v_Limit_EachEmployeeDisease AS BIGINT) * 1000,
	    0
	) AS o_EmployerLiabilityLimitAmountBodilyInjuryByDiseaseEachEmployeeAmount,
	-- *INF*: DECODE(TRUE,
	-- v_LegalNatureOfInsuredCode='99' AND (ISNULL(EntityOtherType) OR LTRIM(RTRIM(EntityOtherType))=''),EntityType,
	-- v_LegalNatureOfInsuredCode='99',EntityOtherType,
	-- '')
	-- 
	DECODE(
	    TRUE,
	    v_LegalNatureOfInsuredCode = '99' AND (EntityOtherType IS NULL OR LTRIM(RTRIM(EntityOtherType)) = ''), EntityType,
	    v_LegalNatureOfInsuredCode = '99', EntityOtherType,
	    ''
	) AS o_OtherLegalNatureOfInsured
	FROM EXP_PrepLKP_01
	LEFT JOIN LKP_Forms
	ON LKP_Forms.WCTrackHistoryID = EXP_PrepLKP_01.WCTrackHistoryID
	LEFT JOIN LKP_Line
	ON LKP_Line.WCTrackHistoryID = EXP_PrepLKP_01.WCTrackHistoryID
	LEFT JOIN LKP_Party_Account
	ON LKP_Party_Account.WCTrackHistoryID = EXP_PrepLKP_01.WCTrackHistoryID
	LEFT JOIN LKP_Party_Agent
	ON LKP_Party_Agent.WCTrackHistoryID = EXP_PrepLKP_01.WCTrackHistoryID
	LEFT JOIN LKP_Policy
	ON LKP_Policy.WCTrackHistoryID = EXP_PrepLKP_01.WCTrackHistoryID
	LEFT JOIN LKP_StateTerm
	ON LKP_StateTerm.WCTrackHistoryID = EXP_PrepLKP_01.WCTrackHistoryID
	LEFT JOIN LKP_WORKWCLIMIT LKP_WORKWCLIMIT_WCTrackHistoryID_EachAccident
	ON LKP_WORKWCLIMIT_WCTrackHistoryID_EachAccident.WCTrackHistoryID = WCTrackHistoryID
	AND LKP_WORKWCLIMIT_WCTrackHistoryID_EachAccident.LimitType = 'EachAccident'

	LEFT JOIN LKP_WORKWCLIMIT LKP_WORKWCLIMIT_WCTrackHistoryID_Policy
	ON LKP_WORKWCLIMIT_WCTrackHistoryID_Policy.WCTrackHistoryID = WCTrackHistoryID
	AND LKP_WORKWCLIMIT_WCTrackHistoryID_Policy.LimitType = 'Policy'

	LEFT JOIN LKP_WORKWCLIMIT LKP_WORKWCLIMIT_WCTrackHistoryID_EachEmployeeDisease
	ON LKP_WORKWCLIMIT_WCTrackHistoryID_EachEmployeeDisease.WCTrackHistoryID = WCTrackHistoryID
	AND LKP_WORKWCLIMIT_WCTrackHistoryID_EachEmployeeDisease.LimitType = 'EachEmployeeDisease'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_EntityType_WCPOLS01Record_LegalNatureOfInsuredCode
	ON LKP_SUPWCPOLS__DCT_EntityType_WCPOLS01Record_LegalNatureOfInsuredCode.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_EntityType_WCPOLS01Record_LegalNatureOfInsuredCode.SourceCode = EntityType
	AND LKP_SUPWCPOLS__DCT_EntityType_WCPOLS01Record_LegalNatureOfInsuredCode.TableName = 'WCPOLS01Record'
	AND LKP_SUPWCPOLS__DCT_EntityType_WCPOLS01Record_LegalNatureOfInsuredCode.ProcessName = 'LegalNatureOfInsuredCode'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_WCPool_WCPOLS01Record_TypeOfPlanIdCode
	ON LKP_SUPWCPOLS__DCT_WCPool_WCPOLS01Record_TypeOfPlanIdCode.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_WCPool_WCPOLS01Record_TypeOfPlanIdCode.SourceCode = 'WCPool'
	AND LKP_SUPWCPOLS__DCT_WCPool_WCPOLS01Record_TypeOfPlanIdCode.TableName = 'WCPOLS01Record'
	AND LKP_SUPWCPOLS__DCT_WCPool_WCPOLS01Record_TypeOfPlanIdCode.ProcessName = 'TypeOfPlanIdCode'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_MinimumPremiumMaximumState_WCPOLS01Record_StateCodeRecord01
	ON LKP_SUPWCPOLS__DCT_MinimumPremiumMaximumState_WCPOLS01Record_StateCodeRecord01.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_MinimumPremiumMaximumState_WCPOLS01Record_StateCodeRecord01.SourceCode = MinimumPremiumMaximumState
	AND LKP_SUPWCPOLS__DCT_MinimumPremiumMaximumState_WCPOLS01Record_StateCodeRecord01.TableName = 'WCPOLS01Record'
	AND LKP_SUPWCPOLS__DCT_MinimumPremiumMaximumState_WCPOLS01Record_StateCodeRecord01.ProcessName = 'StateCodeRecord01'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_AuditPeriod_WCPOLS01Record_AuditFrequencyCode
	ON LKP_SUPWCPOLS__DCT_AuditPeriod_WCPOLS01Record_AuditFrequencyCode.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_AuditPeriod_WCPOLS01Record_AuditFrequencyCode.SourceCode = AuditPeriod
	AND LKP_SUPWCPOLS__DCT_AuditPeriod_WCPOLS01Record_AuditFrequencyCode.TableName = 'WCPOLS01Record'
	AND LKP_SUPWCPOLS__DCT_AuditPeriod_WCPOLS01Record_AuditFrequencyCode.ProcessName = 'AuditFrequencyCode'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_InstallmentType_WCPOLS01Record_BillingFrequencyCode
	ON LKP_SUPWCPOLS__DCT_InstallmentType_WCPOLS01Record_BillingFrequencyCode.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_InstallmentType_WCPOLS01Record_BillingFrequencyCode.SourceCode = InstallmentType
	AND LKP_SUPWCPOLS__DCT_InstallmentType_WCPOLS01Record_BillingFrequencyCode.TableName = 'WCPOLS01Record'
	AND LKP_SUPWCPOLS__DCT_InstallmentType_WCPOLS01Record_BillingFrequencyCode.ProcessName = 'BillingFrequencyCode'

),
WCPols01Record AS (

	------------ PRE SQL ----------
	DELETE
	  FROM dbo.WCPols01Record
	  WHERE 1=1
	  AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	-------------------------------


	INSERT INTO WCPols01Record
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, RecordTypeCode, ExperienceRatingCode, InterstateRiskIDNumber, PolicyExpirationDate, TypeOfCoverageIDCode, EmployeeLeasingPolicyTypeCode, PolicyTermCode, PriorPolicyNumberIdentifier, LegalNatureOfInsuredCode, TypeOfPlanIDCode, WrapUpOwnerControlledInsuranceProgramCode, PolicyMinimumPremiumAmount, PolicyMinimumPremiumStateCode, PolicyEstimatedStandardPremiumTotal, PolicyDepositPremiumAmount, AuditFrequencyCode, BillingFrequencyCode, RetrospectiveRatingCode, EmployerLiabilityLimitAmountBodilyInjuryByAccidentEachAccidentAmount, EmployerLiabilityLimitAmountBodilyInjuryByDiseasePolicyLimitAmount, EmployerLiabilityLimitAmountBodilyInjuryByDiseaseEachEmployeeAmount, NameOfProducer, TextForOtherLegalNatureOfInsured, PolicyChangeEffectiveDate, PolicyChangeExpirationDate)
	SELECT 
	EXTRACTDATE, 
	o_AuditId AS AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	o_RecordTypeCode AS RECORDTYPECODE, 
	o_ExperienceRatingCode AS EXPERIENCERATINGCODE, 
	INTERSTATERISKIDNUMBER, 
	PolicyExpirationDate_YYMMDD AS POLICYEXPIRATIONDATE, 
	o_TypeOfCoverageIDCode AS TYPEOFCOVERAGEIDCODE, 
	o_EmployeeLeasingPolicyTypeCode AS EMPLOYEELEASINGPOLICYTYPECODE, 
	o_PolicyTermCode AS POLICYTERMCODE, 
	PRIORPOLICYNUMBERIDENTIFIER, 
	o_LegalNatureOfInsuredCode AS LEGALNATUREOFINSUREDCODE, 
	o_TypeOfPlanIDCode AS TYPEOFPLANIDCODE, 
	o_WrapUpOwnerControlledInsuranceProgramCode AS WRAPUPOWNERCONTROLLEDINSURANCEPROGRAMCODE, 
	o_PolicyMinimumPremiumAmount AS POLICYMINIMUMPREMIUMAMOUNT, 
	o_PolicyMinimumPremiumStateCode AS POLICYMINIMUMPREMIUMSTATECODE, 
	o_PolicyEstimatedStandardPremiumTotal AS POLICYESTIMATEDSTANDARDPREMIUMTOTAL, 
	o_PolicyDepositPremiumAmount AS POLICYDEPOSITPREMIUMAMOUNT, 
	o_AuditFrequencyCode AS AUDITFREQUENCYCODE, 
	o_BillingFrequencyCode AS BILLINGFREQUENCYCODE, 
	o_RetrospectiveRatingCode AS RETROSPECTIVERATINGCODE, 
	o_EmployerLiabilityLimitAmountBodilyInjuryByAccidentEachAccidentAmount AS EMPLOYERLIABILITYLIMITAMOUNTBODILYINJURYBYACCIDENTEACHACCIDENTAMOUNT, 
	o_EmployerLiabilityLimitAmountBodilyInjuryByDiseasePolicyLimitAmount AS EMPLOYERLIABILITYLIMITAMOUNTBODILYINJURYBYDISEASEPOLICYLIMITAMOUNT, 
	o_EmployerLiabilityLimitAmountBodilyInjuryByDiseaseEachEmployeeAmount AS EMPLOYERLIABILITYLIMITAMOUNTBODILYINJURYBYDISEASEEACHEMPLOYEEAMOUNT, 
	NAMEOFPRODUCER, 
	o_OtherLegalNatureOfInsured AS TEXTFOROTHERLEGALNATUREOFINSURED, 
	POLICYCHANGEEFFECTIVEDATE, 
	POLICYCHANGEEXPIRATIONDATE
	FROM EXP_Format_01_Output
),
EXP_ExpRatingCode AS (
	SELECT
	o_AuditId AS AuditId,
	ExtractDate,
	o_ExperienceRatingCode AS ExperienceRatingCode,
	WCTrackHistoryID
	FROM EXP_Format_01_Output
),
LKP_WorkWCTrackHistory AS (
	SELECT
	WCTrackHistoryID,
	Auditid
	FROM (
		SELECT 
			WCTrackHistoryID,
			Auditid
		FROM WorkWCTrackHistory
		WHERE NCRequiredFlag = 1 AND AuditID = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Auditid,WCTrackHistoryID ORDER BY WCTrackHistoryID) = 1
),
FIL_ExpRatingCode AS (
	SELECT
	EXP_ExpRatingCode.AuditId, 
	EXP_ExpRatingCode.ExtractDate, 
	LKP_WorkWCTrackHistory.WCTrackHistoryID, 
	EXP_ExpRatingCode.ExperienceRatingCode
	FROM EXP_ExpRatingCode
	LEFT JOIN LKP_WorkWCTrackHistory
	ON LKP_WorkWCTrackHistory.Auditid = EXP_ExpRatingCode.AuditId AND LKP_WorkWCTrackHistory.WCTrackHistoryID = EXP_ExpRatingCode.WCTrackHistoryID
	WHERE ExperienceRatingCode = '2'AND NOT ISNULL(WCTrackHistoryID)
),
EXP_WorkWCProcessUpdateTable AS (
	SELECT
	AuditId,
	ExtractDate,
	WCTrackHistoryID,
	'ExpRatingCode-01' AS ProcessName,
	ExperienceRatingCode AS AttributeValue
	FROM FIL_ExpRatingCode
),
WorkWCProcessUpdateTable AS (
	INSERT INTO WorkWCProcessUpdateTable
	(Auditid, ExtractDate, WCTrackHistoryID, ProcessName, AttributeValue)
	SELECT 
	AuditId AS AUDITID, 
	EXTRACTDATE, 
	WCTRACKHISTORYID, 
	PROCESSNAME, 
	ATTRIBUTEVALUE
	FROM EXP_WorkWCProcessUpdateTable
),
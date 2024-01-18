WITH
SQ_WorkDCTPLPolicy AS (
	SELECT
		WorkDCTPLPolicyId,
		ExtractDate,
		SourceSystemId,
		PolicyId,
		PolicyLevelsCompoundId,
		PolicyTransactionId,
		TransactionTypeId,
		PolicyStatusId,
		AgencyPolicyId,
		PolicyKey,
		PolicyTransactionKey,
		PolicyNumber,
		PolicyVersion,
		PolicySymbol,
		TransactionTypeKey,
		TransactionTypeCode,
		PolicyStatusKey,
		PolicyStatusCode,
		TransactionCreatedDate,
		PolicyEffectiveDate,
		PolicyExpirationDate,
		AgencyCode,
		InceptionDate,
		BCCode,
		PolicyCancellationInd,
		PolicyCancellationDate,
		PolicyCancellationReasonCode,
		PolicyState,
		ServiceCenterSupportCode,
		PolicyTerm,
		TerrorismRiskInd,
		PriorPolicyNumber,
		PolicyIssueCode,
		PolicyAge,
		IndustryRiskGradeCode,
		BusinessSegmentCode,
		Userid,
		ClassOfBusiness,
		RenewalPolicySymbol,
		SupBusinessClassCode,
		AutomaticRenewalIndicator,
		AssociationCode,
		PolicyIssueCodeOverride,
		PolicyOfferingCode,
		StrategicProfitCenterCode,
		AutomatedUnderwritingIndicator,
		CustomerServicingCd,
		ProducerCode,
		LineageId,
		StartDate,
		TransactionNumber,
		DataFix,
		DataFixDate,
		DataFixUser,
		DataFixType
	FROM WorkDCTPLPolicy
),
EXP_SRC_DataCollect AS (
	SELECT
	WorkDCTPLPolicyId,
	ExtractDate,
	SourceSystemId,
	PolicyId,
	PolicyLevelsCompoundId,
	PolicyTransactionId,
	TransactionTypeId,
	PolicyStatusId,
	AgencyPolicyId,
	PolicyKey,
	PolicyTransactionKey,
	PolicyNumber,
	PolicyVersion,
	PolicySymbol,
	TransactionTypeKey,
	TransactionTypeCode,
	PolicyStatusKey,
	PolicyStatusCode,
	TransactionCreatedDate,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	AgencyCode,
	InceptionDate,
	BCCode,
	PolicyCancellationInd,
	PolicyCancellationDate,
	PolicyCancellationReasonCode,
	PolicyState,
	ServiceCenterSupportCode,
	PolicyTerm,
	TerrorismRiskInd,
	PriorPolicyNumber,
	PolicyIssueCode,
	PolicyAge,
	IndustryRiskGradeCode,
	BusinessSegmentCode,
	Userid,
	ClassOfBusiness,
	RenewalPolicySymbol,
	SupBusinessClassCode,
	AutomaticRenewalIndicator,
	AssociationCode,
	PolicyIssueCodeOverride,
	PolicyOfferingCode,
	StrategicProfitCenterCode,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_Auditid,
	AutomatedUnderwritingIndicator,
	CustomerServicingCd,
	ProducerCode,
	LineageId,
	StartDate,
	TransactionNumber,
	DataFix,
	DataFixDate,
	DataFixUser,
	DataFixType
	FROM SQ_WorkDCTPLPolicy
),
ArchWorkDCTPLPolicy AS (
	INSERT INTO ArchWorkDCTPLPolicy
	(Auditid, ExtractDate, SourceSystemId, WorkDCTPLPolicyId, PolicyId, LineageId, PolicyLevelsCompoundId, PolicyTransactionId, TransactionTypeId, PolicyStatusId, AgencyPolicyId, PolicyKey, PolicyTransactionKey, PolicyNumber, PolicyVersion, PolicySymbol, TransactionTypeKey, TransactionTypeCode, PolicyStatusKey, PolicyStatusCode, TransactionCreatedDate, PolicyEffectiveDate, PolicyExpirationDate, AgencyCode, InceptionDate, BCCode, PolicyCancellationInd, PolicyCancellationDate, PolicyCancellationReasonCode, PolicyState, ServiceCenterSupportCode, PolicyTerm, TerrorismRiskInd, PriorPolicyNumber, PolicyIssueCode, PolicyAge, IndustryRiskGradeCode, BusinessSegmentCode, Userid, ClassOfBusiness, RenewalPolicySymbol, SupBusinessClassCode, AutomaticRenewalIndicator, AssociationCode, PolicyIssueCodeOverride, PolicyOfferingCode, StrategicProfitCenterCode, AutomatedUnderwritingIndicator, CustomerServicingCd, ProducerCode, TransactionNumber, StartDate, DataFix, DataFixDate, DataFixUser, DataFixType)
	SELECT 
	o_Auditid AS AUDITID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	WORKDCTPLPOLICYID, 
	POLICYID, 
	LINEAGEID, 
	POLICYLEVELSCOMPOUNDID, 
	POLICYTRANSACTIONID, 
	TRANSACTIONTYPEID, 
	POLICYSTATUSID, 
	AGENCYPOLICYID, 
	POLICYKEY, 
	POLICYTRANSACTIONKEY, 
	POLICYNUMBER, 
	POLICYVERSION, 
	POLICYSYMBOL, 
	TRANSACTIONTYPEKEY, 
	TRANSACTIONTYPECODE, 
	POLICYSTATUSKEY, 
	POLICYSTATUSCODE, 
	TRANSACTIONCREATEDDATE, 
	POLICYEFFECTIVEDATE, 
	POLICYEXPIRATIONDATE, 
	AGENCYCODE, 
	INCEPTIONDATE, 
	BCCODE, 
	POLICYCANCELLATIONIND, 
	POLICYCANCELLATIONDATE, 
	POLICYCANCELLATIONREASONCODE, 
	POLICYSTATE, 
	SERVICECENTERSUPPORTCODE, 
	POLICYTERM, 
	TERRORISMRISKIND, 
	PRIORPOLICYNUMBER, 
	POLICYISSUECODE, 
	POLICYAGE, 
	INDUSTRYRISKGRADECODE, 
	BUSINESSSEGMENTCODE, 
	USERID, 
	CLASSOFBUSINESS, 
	RENEWALPOLICYSYMBOL, 
	SUPBUSINESSCLASSCODE, 
	AUTOMATICRENEWALINDICATOR, 
	ASSOCIATIONCODE, 
	POLICYISSUECODEOVERRIDE, 
	POLICYOFFERINGCODE, 
	STRATEGICPROFITCENTERCODE, 
	AUTOMATEDUNDERWRITINGINDICATOR, 
	CUSTOMERSERVICINGCD, 
	PRODUCERCODE, 
	TRANSACTIONNUMBER, 
	STARTDATE, 
	DATAFIX, 
	DATAFIXDATE, 
	DATAFIXUSER, 
	DATAFIXTYPE
	FROM EXP_SRC_DataCollect
),
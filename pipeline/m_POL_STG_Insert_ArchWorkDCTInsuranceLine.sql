WITH
SQ_WorkDCTInsuranceLine AS (
	SELECT
		WorkDCTInsuranceLineId,
		ExtractDate,
		SourceSystemId,
		SessionId,
		PolicyId,
		LineId,
		LineType,
		RiskGrade,
		IsAuditable,
		PriorCarrierName,
		PriorPolicyNumber,
		PriorLineOfBusiness,
		ExperienceModifier,
		FinalCommission,
		CommissionCustomerCareAmount
	FROM WorkDCTInsuranceLine
),
EXp_Default AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	WorkDCTInsuranceLineId,
	ExtractDate,
	SourceSystemId,
	SessionId,
	PolicyId,
	LineId,
	LineType,
	RiskGrade,
	IsAuditable,
	-- *INF*: DECODE(IsAuditable, 'T', 1, 'F', 0, NULL)
	DECODE(
	    IsAuditable,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IsAuditable,
	PriorCarrierName,
	PriorPolicyNumber,
	PriorLineOfBusiness,
	ExperienceModifier,
	FinalCommission,
	CommissionCustomerCareAmount
	FROM SQ_WorkDCTInsuranceLine
),
ArchWorkDCTInsuranceLine AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWorkDCTInsuranceLine
	(ExtractDate, SourceSystemId, AuditId, WorkDCTInsuranceLineId, SessionId, PolicyId, LineId, LineType, RiskGrade, IsAuditable, PriorCarrierName, PriorPolicyNumber, PriorLineOfBusiness, ExperienceModifier, FinalCommission, CommissionCustomerCareAmount)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	audit_id AS AUDITID, 
	WORKDCTINSURANCELINEID, 
	SESSIONID, 
	POLICYID, 
	LINEID, 
	LINETYPE, 
	RISKGRADE, 
	o_IsAuditable AS ISAUDITABLE, 
	PRIORCARRIERNAME, 
	PRIORPOLICYNUMBER, 
	PRIORLINEOFBUSINESS, 
	EXPERIENCEMODIFIER, 
	FINALCOMMISSION, 
	COMMISSIONCUSTOMERCAREAMOUNT
	FROM EXp_Default
),
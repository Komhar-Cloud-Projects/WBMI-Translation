WITH
SQ_WorkDCBILCommissionCWOClawBack AS (
	SELECT
		WorkDCBILCommissionCWOClawBackId,
		ExtractDate,
		SourceSystemId,
		AuthorizationDate,
		CWOAmount,
		CommissionPercent,
		AuthorizedAmount,
		PolicyReference,
		PolicyTermEffectiveDate,
		PolicyTermExpirationDate
	FROM WorkDCBILCommissionCWOClawBack
),
EXp_Default AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	WorkDCBILCommissionCWOClawBackId,
	ExtractDate,
	SourceSystemId,
	AuthorizationDate,
	CWOAmount,
	CommissionPercent,
	AuthorizedAmount,
	PolicyReference,
	PolicyTermEffectiveDate,
	PolicyTermExpirationDate
	FROM SQ_WorkDCBILCommissionCWOClawBack
),
ArchWorkDCBILCommissionCWOClawBack AS (
	INSERT INTO ArchWorkDCBILCommissionCWOClawBack
	(ExtractDate, SourceSystemId, AuditId, WorkDCBILCommissionCWOClawBackId, AuthorizationDate, CWOAmount, CommissionPercent, AuthorizedAmount, PolicyReference, PolicyTermEffectiveDate, PolicyTermExpirationDate)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	audit_id AS AUDITID, 
	WORKDCBILCOMMISSIONCWOCLAWBACKID, 
	AUTHORIZATIONDATE, 
	CWOAMOUNT, 
	COMMISSIONPERCENT, 
	AUTHORIZEDAMOUNT, 
	POLICYREFERENCE, 
	POLICYTERMEFFECTIVEDATE, 
	POLICYTERMEXPIRATIONDATE
	FROM EXp_Default
),
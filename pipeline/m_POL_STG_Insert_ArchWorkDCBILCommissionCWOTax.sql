WITH
SQ_WorkDCBILCommissionCWOTax AS (
	SELECT
		WorkDCBILCommissionCWOTaxId,
		ExtractDate,
		SourceSystemId,
		InstallmentDate,
		WrittenOffAmount,
		PolicyReference,
		PolicyTermEffectiveDate,
		PolicyTermExpirationDate
	FROM WorkDCBILCommissionCWOTax
),
EXp_Default AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	WorkDCBILCommissionCWOTaxId,
	ExtractDate,
	SourceSystemId,
	InstallmentDate,
	WrittenOffAmount,
	PolicyReference,
	PolicyTermEffectiveDate,
	PolicyTermExpirationDate
	FROM SQ_WorkDCBILCommissionCWOTax
),
ArchWorkDCBILCommissionCWOTax AS (
	INSERT INTO ArchWorkDCBILCommissionCWOTax
	(ExtractDate, SourceSystemId, AuditId, WorkDCBILCommissionCWOTaxId, InstallmentDate, WrittenOffAmount, PolicyReference, PolicyTermEffectiveDate, PolicyTermExpirationDate)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	audit_id AS AUDITID, 
	WORKDCBILCOMMISSIONCWOTAXID, 
	INSTALLMENTDATE, 
	WRITTENOFFAMOUNT, 
	POLICYREFERENCE, 
	POLICYTERMEFFECTIVEDATE, 
	POLICYTERMEXPIRATIONDATE
	FROM EXp_Default
),
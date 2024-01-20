WITH
SQ_DCBILGeneralJounalStage AS (
	select PT.PolicyReference,pt.PolicyTermEffectiveDate,pt.PolicyTermExpirationDate, GJ.ActivityEffectiveDate as InstallmentDate, SUM(GJ.TransactionGrossAmount) as WrittenOffAmount
	from DCBILGeneralJounalStage GJ join DCBILPolicyTermStage PT on PT.PolicyTermId=GJ.PolicyTermId
	where GJ.JournalTypeCode='Tax' and GJ.ActivityTypeCode in ('WO', 'RCWR') and GJ.AccountingClassCode in ('AR0','AR1')
	And GJ.TransactionGrossAmount !=0
	and GJ.ActivityEffectiveDate >=SUBSTRING('@{pipeline().parameters.SELECTION_START_TS}',1,10)
	 group by PT.PolicyReference,pt.PolicyTermEffectiveDate,pt.PolicyTermExpirationDate,  GJ.ActivityEffectiveDate
),
EXP_DEFAULT AS (
	SELECT
	PolicyReference,
	InstallmentDate,
	ItemWrittenOffAmount,
	-- *INF*: trunc(@{pipeline().parameters.SELECTION_START_TS})
	TRUNC(@{pipeline().parameters.SELECTION_START_TS}) AS ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	PolicyTermEffectiveDate,
	PolicyTermExpirationDate
	FROM SQ_DCBILGeneralJounalStage
),
WorkDCBILCommissionCWOTax AS (
	TRUNCATE TABLE WorkDCBILCommissionCWOTax;
	INSERT INTO WorkDCBILCommissionCWOTax
	(ExtractDate, SourceSystemId, InstallmentDate, WrittenOffAmount, PolicyReference, PolicyTermEffectiveDate, PolicyTermExpirationDate)
	SELECT 
	EXTRACTDATE, 
	SourceSystemID AS SOURCESYSTEMID, 
	INSTALLMENTDATE, 
	ItemWrittenOffAmount AS WRITTENOFFAMOUNT, 
	POLICYREFERENCE, 
	POLICYTERMEFFECTIVEDATE, 
	POLICYTERMEXPIRATIONDATE
	FROM EXP_DEFAULT
),
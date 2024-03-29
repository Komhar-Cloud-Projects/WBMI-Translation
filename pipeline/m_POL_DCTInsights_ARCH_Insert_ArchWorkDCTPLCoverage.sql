WITH
SQ_WorkDCTPLCoverage AS (
	SELECT
		WorkDCTPLCoverageId,
		ExtractDate,
		SourceSystemId,
		PolicyId,
		PolicyKey,
		AddressId,
		RiskAddressId,
		LineOfInsuranceCode,
		LineOfInsuranceDesc,
		TransactionDate,
		CoverageKey,
		CoverageCodeKey,
		CoverageCodeDesc,
		CoverageSubCd,
		PerilCode,
		PerilType,
		ExposureClassCode,
		ExposureAmount,
		ExposureAmount_OC,
		SublineCode,
		TransactionAmount,
		CoverageEffectiveDate,
		CoverageExpirationDate,
		TerminationDate,
		CoverageVersion,
		AnnualStatementLineNumber,
		SpecialClassGroupCode,
		AnnualStatementLineCode,
		SubAnnualStatementLineCode,
		SubNonAnnualStatementLineCode,
		MeasureName,
		MeasureDetailCode,
		TransactionEffectiveDate,
		TransactionIssueDate,
		TransactionReasonCode,
		DeductibleAmount,
		ProductCode,
		ProductDesc,
		CoverageId,
		FullTermPremium,
		LineageId,
		StartDate,
		RiskAddressKey
	FROM WorkDCTPLCoverage
),
EXP_SRC_DataCollect AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_Auditid,
	WorkDCTPLCoverageId,
	ExtractDate,
	SourceSystemId,
	PolicyId,
	PolicyKey,
	AddressId,
	RiskAddressId,
	LineOfInsuranceCode,
	LineOfInsuranceDesc,
	TransactionDate,
	CoverageKey,
	CoverageCodeKey,
	CoverageCodeDesc,
	CoverageSubCd,
	PerilCode,
	PerilType,
	ExposureClassCode,
	ExposureAmount,
	ExposureAmount_OC,
	SublineCode,
	TransactionAmount,
	CoverageEffectiveDate,
	CoverageExpirationDate,
	TerminationDate,
	CoverageVersion,
	AnnualStatementLineNumber,
	SpecialClassGroupCode,
	AnnualStatementLineCode,
	SubAnnualStatementLineCode,
	SubNonAnnualStatementLineCode,
	MeasureName,
	MeasureDetailCode,
	TransactionEffectiveDate,
	TransactionIssueDate,
	TransactionReasonCode,
	DeductibleAmount,
	ProductCode,
	ProductDesc,
	CoverageId,
	FullTermPremium,
	LineageId,
	StartDate,
	RiskAddressKey
	FROM SQ_WorkDCTPLCoverage
),
ArchWorkDCTPLCoverage AS (
	INSERT INTO ArchWorkDCTPLCoverage
	(Auditid, ExtractDate, SourceSystemId, WorkDCTPLCoverageId, PolicyId, PolicyKey, AddressId, RiskAddressId, CoverageId, LineOfInsuranceCode, LineOfInsuranceDesc, TransactionDate, CoverageKey, CoverageCodeKey, CoverageCodeDesc, CoverageSubCd, PerilCode, PerilType, ExposureClassCode, ExposureAmount, ExposureAmount_OC, SublineCode, TransactionAmount, FullTermPremium, CoverageEffectiveDate, CoverageExpirationDate, TerminationDate, CoverageVersion, AnnualStatementLineNumber, SpecialClassGroupCode, AnnualStatementLineCode, SubAnnualStatementLineCode, SubNonAnnualStatementLineCode, MeasureName, MeasureDetailCode, TransactionEffectiveDate, TransactionIssueDate, TransactionReasonCode, DeductibleAmount, ProductCode, ProductDesc, LineageId, StartDate, RiskAddressKey)
	SELECT 
	o_Auditid AS AUDITID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	WORKDCTPLCOVERAGEID, 
	POLICYID, 
	POLICYKEY, 
	ADDRESSID, 
	RISKADDRESSID, 
	COVERAGEID, 
	LINEOFINSURANCECODE, 
	LINEOFINSURANCEDESC, 
	TRANSACTIONDATE, 
	COVERAGEKEY, 
	COVERAGECODEKEY, 
	COVERAGECODEDESC, 
	COVERAGESUBCD, 
	PERILCODE, 
	PERILTYPE, 
	EXPOSURECLASSCODE, 
	EXPOSUREAMOUNT, 
	EXPOSUREAMOUNT_OC, 
	SUBLINECODE, 
	TRANSACTIONAMOUNT, 
	FULLTERMPREMIUM, 
	COVERAGEEFFECTIVEDATE, 
	COVERAGEEXPIRATIONDATE, 
	TERMINATIONDATE, 
	COVERAGEVERSION, 
	ANNUALSTATEMENTLINENUMBER, 
	SPECIALCLASSGROUPCODE, 
	ANNUALSTATEMENTLINECODE, 
	SUBANNUALSTATEMENTLINECODE, 
	SUBNONANNUALSTATEMENTLINECODE, 
	MEASURENAME, 
	MEASUREDETAILCODE, 
	TRANSACTIONEFFECTIVEDATE, 
	TRANSACTIONISSUEDATE, 
	TRANSACTIONREASONCODE, 
	DEDUCTIBLEAMOUNT, 
	PRODUCTCODE, 
	PRODUCTDESC, 
	LINEAGEID, 
	STARTDATE, 
	RISKADDRESSKEY
	FROM EXP_SRC_DataCollect
),
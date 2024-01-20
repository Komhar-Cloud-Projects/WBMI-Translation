WITH
SQ_DCGLLineStaging AS (
	SELECT
		LineId,
		GL_LineId,
		SessionId,
		Id,
		ClaimsMadeMultiplierOverride,
		ClaimsMadeYear,
		CommissionPercentage,
		CoverageForm,
		DeductibleScope,
		DeductibleType,
		Description,
		EPANumber,
		IncludeLiquorLiability,
		LegalDefenseLimit,
		PolicyType,
		PollutionType,
		RetroactiveDate,
		SeparateProductsDeductible,
		SeparateProductsPDDeductible,
		UseRetroDate,
		ExtractDate,
		SourceSystemId,
		CompositeRating,
		CompositeEligibility
	FROM DCGLLineStaging
),
EXP_Metadata AS (
	SELECT
	LineId,
	GL_LineId,
	SessionId,
	Id,
	ClaimsMadeMultiplierOverride,
	ClaimsMadeYear,
	CommissionPercentage,
	CoverageForm,
	DeductibleScope,
	DeductibleType,
	Description,
	EPANumber,
	IncludeLiquorLiability,
	LegalDefenseLimit,
	PolicyType,
	PollutionType,
	RetroactiveDate,
	SeparateProductsDeductible,
	SeparateProductsPDDeductible,
	UseRetroDate,
	ExtractDate,
	SourceSystemId,
	-- *INF*: DECODE(IncludeLiquorLiability,'T',1,'F',0,NULL)
	DECODE(
	    IncludeLiquorLiability,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IncludeLiquorLiability,
	-- *INF*: DECODE(SeparateProductsDeductible,'T',1,'F',0,NULL)
	DECODE(
	    SeparateProductsDeductible,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_SeparateProductsDeductible,
	-- *INF*: DECODE(SeparateProductsPDDeductible,'T',1,'F',0,NULL)
	DECODE(
	    SeparateProductsPDDeductible,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_SeparateProductsPDDeductible,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	CompositeRating,
	-- *INF*: LTRIM(RTRIM(CompositeRating))
	LTRIM(RTRIM(CompositeRating)) AS o_CompositeRating,
	CompositeEligibility,
	-- *INF*: LTRIM(RTRIM(CompositeEligibility))
	LTRIM(RTRIM(CompositeEligibility)) AS o_CompositeEligibility
	FROM SQ_DCGLLineStaging
),
archDCGLLineStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCGLLineStaging
	(LineId, GL_LineId, SessionId, Id, ClaimsMadeMultiplierOverride, ClaimsMadeYear, CommissionPercentage, CoverageForm, DeductibleScope, DeductibleType, Description, EPANumber, IncludeLiquorLiability, LegalDefenseLimit, PolicyType, PollutionType, RetroactiveDate, SeparateProductsDeductible, SeparateProductsPDDeductible, UseRetroDate, ExtractDate, SourceSystemId, AuditId, CompositeRating, CompositeEligibility)
	SELECT 
	LINEID, 
	GL_LINEID, 
	SESSIONID, 
	ID, 
	CLAIMSMADEMULTIPLIEROVERRIDE, 
	CLAIMSMADEYEAR, 
	COMMISSIONPERCENTAGE, 
	COVERAGEFORM, 
	DEDUCTIBLESCOPE, 
	DEDUCTIBLETYPE, 
	DESCRIPTION, 
	EPANUMBER, 
	o_IncludeLiquorLiability AS INCLUDELIQUORLIABILITY, 
	LEGALDEFENSELIMIT, 
	POLICYTYPE, 
	POLLUTIONTYPE, 
	RETROACTIVEDATE, 
	o_SeparateProductsDeductible AS SEPARATEPRODUCTSDEDUCTIBLE, 
	o_SeparateProductsPDDeductible AS SEPARATEPRODUCTSPDDEDUCTIBLE, 
	USERETRODATE, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	o_CompositeRating AS COMPOSITERATING, 
	o_CompositeEligibility AS COMPOSITEELIGIBILITY
	FROM EXP_Metadata
),
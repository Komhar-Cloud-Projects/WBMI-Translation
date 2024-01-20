WITH
DCCACoverageUMBIStaging AS (
	SELECT
		DCCACoverageUMBIStagingId,
		ExtractDate,
		SourceSystemId,
		SessionId,
		CoverageId,
		CA_CoverageUMBIId,
		AcceptUMCoverage,
		CovUMBIPrivateIsFirstWAUMBI,
		DesignatedPerson,
		EconomicLossCoverage,
		IncludeUIM,
		StatutoryCoverage,
		TXAutoDescription,
		TXAutoOtherDescription,
		UMType,
		UnderwriterOverride,
		WAAutoDescription
	FROM DCCACoverageUMBIStaging
	INNER JOIN DCCACoverageUMBIStaging
),
EXPTRANS AS (
	SELECT
	DCCACoverageUMBIStagingId,
	ExtractDate,
	SourceSystemId,
	SessionId,
	CoverageId,
	CA_CoverageUMBIId,
	AcceptUMCoverage,
	CovUMBIPrivateIsFirstWAUMBI,
	DesignatedPerson,
	EconomicLossCoverage,
	IncludeUIM,
	StatutoryCoverage,
	TXAutoDescription,
	TXAutoOtherDescription,
	UMType,
	UnderwriterOverride,
	WAAutoDescription,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM DCCACoverageUMBIStaging
),
ArchDCCACoverageUMBIStaging AS (
	INSERT INTO Shortcut_to_ArchDCCACoverageUMBIStaging
	(ExtractDate, SourceSystemId, AuditId, DCCACoverageUMBIStagingId, SessionId, CoverageId, CA_CoverageUMBIId, AcceptUMCoverage, CovUMBIPrivateIsFirstWAUMBI, DesignatedPerson, EconomicLossCoverage, IncludeUIM, StatutoryCoverage, TXAutoDescription, TXAutoOtherDescription, UMType, UnderwriterOverride, WAAutoDescription)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCCACOVERAGEUMBISTAGINGID, 
	SESSIONID, 
	COVERAGEID, 
	CA_COVERAGEUMBIID, 
	ACCEPTUMCOVERAGE, 
	COVUMBIPRIVATEISFIRSTWAUMBI, 
	DESIGNATEDPERSON, 
	ECONOMICLOSSCOVERAGE, 
	INCLUDEUIM, 
	STATUTORYCOVERAGE, 
	TXAUTODESCRIPTION, 
	TXAUTOOTHERDESCRIPTION, 
	UMTYPE, 
	UNDERWRITEROVERRIDE, 
	WAAUTODESCRIPTION
	FROM EXPTRANS
),
WITH
SQ_DC_CA_CoverageUMBI AS (
	WITH cte_DC_CA_CoverageUMBI(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
		X.CoverageId,
		X.CA_CoverageUMBIId, 
		X.SessionId, 
		X.AcceptUMCoverage, 
		X.CovUMBIPrivateIsFirstWAUMBI, 
		X.DesignatedPerson, 
		X.EconomicLossCoverage, 
		X.IncludeUIM, 
		X.StatutoryCoverage, 
		X.TXAutoDescription, 
		X.TXAutoOtherDescription, 
		X.UMType, 
		X.UnderwriterOverride, 
		X.WAAutoDescription
	FROM
	DC_CA_CoverageUMBI X
	inner join
	cte_DC_CA_CoverageUMBI Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXPTRANS AS (
	SELECT
	CoverageId,
	CA_CoverageUMBIId,
	SessionId,
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
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CA_CoverageUMBI
),
DCCACoverageUMBIStaging AS (
	TRUNCATE TABLE Shortcut_to_DCCACoverageUMBIStaging;
	INSERT INTO Shortcut_to_DCCACoverageUMBIStaging
	(ExtractDate, SourceSystemId, SessionId, CoverageId, CA_CoverageUMBIId, AcceptUMCoverage, CovUMBIPrivateIsFirstWAUMBI, DesignatedPerson, EconomicLossCoverage, IncludeUIM, StatutoryCoverage, TXAutoDescription, TXAutoOtherDescription, UMType, UnderwriterOverride, WAAutoDescription)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
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
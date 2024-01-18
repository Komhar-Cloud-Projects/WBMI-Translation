WITH
SQ_DC_GL_Line AS (
	WITH cte_DCGLLine(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.GL_LineId, 
	X.SessionId, 
	X.Id, 
	X.ClaimsMadeMultiplierOverride, 
	X.ClaimsMadeYear, 
	X.CommissionPercentage, 
	X.CoverageForm, 
	X.DeductibleScope, 
	X.DeductibleType, 
	X.Description, 
	X.EPANumber, 
	X.IncludeLiquorLiability, 
	X.LegalDefenseLimit, 
	X.PolicyType, 
	X.PollutionType, 
	X.RetroactiveDate, 
	X.SeparateProductsDeductible, 
	X.SeparateProductsPDDeductible, 
	X.UseRetroDate,
	X.CompositeRating,
	X.CompositeEligibility  
	FROM
	DC_GL_Line X
	inner join
	cte_DCGLLine Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
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
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CompositeRating,
	-- *INF*: LTRIM(RTRIM(CompositeRating))
	LTRIM(RTRIM(CompositeRating)) AS o_CompositeRating,
	CompositeEligibility,
	-- *INF*: LTRIM(RTRIM(CompositeEligibility))
	LTRIM(RTRIM(CompositeEligibility)) AS o_CompositeEligibility
	FROM SQ_DC_GL_Line
),
DCGLLineStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCGLLineStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCGLLineStaging
	(LineId, GL_LineId, SessionId, Id, ClaimsMadeMultiplierOverride, ClaimsMadeYear, CommissionPercentage, CoverageForm, DeductibleScope, DeductibleType, Description, EPANumber, IncludeLiquorLiability, LegalDefenseLimit, PolicyType, PollutionType, RetroactiveDate, SeparateProductsDeductible, SeparateProductsPDDeductible, UseRetroDate, ExtractDate, SourceSystemId, CompositeRating, CompositeEligibility)
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
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_CompositeRating AS COMPOSITERATING, 
	o_CompositeEligibility AS COMPOSITEELIGIBILITY
	FROM EXP_Metadata
),
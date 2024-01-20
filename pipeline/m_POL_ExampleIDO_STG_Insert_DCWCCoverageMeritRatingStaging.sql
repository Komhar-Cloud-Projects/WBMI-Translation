WITH
SQ_DC_WC_CoverageMeritRating AS (
	WITH cte_DCWCCoverageMeritRating(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.WC_CoverageMeritRatingId, 
	X.SessionId, 
	X.ALMeritRatingSelections, 
	X.ARMeritRatingSelections, 
	X.DEMeritRatingSelections, 
	X.GAMeritRatingSelections, 
	X.HIMeritRatingSelections, 
	X.MAMeritRatingSelections, 
	X.MEMeritRatingSelections, 
	X.MIMeritRatingSelections, 
	X.NYMeritRatingSelections, 
	X.OKMeritRatingSelections, 
	X.ORMeritRatingSelections, 
	X.PAMeritRatingSelections, 
	X.SDMeritRatingSelections 
	FROM
	DC_WC_CoverageMeritRating X
	inner join
	cte_DCWCCoverageMeritRating Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CoverageId,
	WC_CoverageMeritRatingId,
	SessionId,
	ALMeritRatingSelections,
	ARMeritRatingSelections,
	DEMeritRatingSelections,
	GAMeritRatingSelections,
	HIMeritRatingSelections,
	MAMeritRatingSelections,
	MEMeritRatingSelections,
	MIMeritRatingSelections,
	NYMeritRatingSelections,
	OKMeritRatingSelections,
	ORMeritRatingSelections,
	PAMeritRatingSelections,
	SDMeritRatingSelections,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_WC_CoverageMeritRating
),
DCWCCoverageMeritRatingStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCWCCoverageMeritRatingStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCWCCoverageMeritRatingStaging
	(CoverageId, WC_CoverageMeritRatingId, SessionId, ALMeritRatingSelections, ARMeritRatingSelections, DEMeritRatingSelections, GAMeritRatingSelections, HIMeritRatingSelections, MAMeritRatingSelections, MEMeritRatingSelections, MIMeritRatingSelections, NYMeritRatingSelections, OKMeritRatingSelections, ORMeritRatingSelections, PAMeritRatingSelections, SDMeritRatingSelections, ExtractDate, SourceSystemId)
	SELECT 
	COVERAGEID, 
	WC_COVERAGEMERITRATINGID, 
	SESSIONID, 
	ALMERITRATINGSELECTIONS, 
	ARMERITRATINGSELECTIONS, 
	DEMERITRATINGSELECTIONS, 
	GAMERITRATINGSELECTIONS, 
	HIMERITRATINGSELECTIONS, 
	MAMERITRATINGSELECTIONS, 
	MEMERITRATINGSELECTIONS, 
	MIMERITRATINGSELECTIONS, 
	NYMERITRATINGSELECTIONS, 
	OKMERITRATINGSELECTIONS, 
	ORMERITRATINGSELECTIONS, 
	PAMERITRATINGSELECTIONS, 
	SDMERITRATINGSELECTIONS, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),
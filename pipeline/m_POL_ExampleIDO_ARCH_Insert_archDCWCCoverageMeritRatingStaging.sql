WITH
SQ_DCWCCoverageMeritRatingStaging AS (
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
		ExtractDate,
		SourceSystemId
	FROM DCWCCoverageMeritRatingStaging
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
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCWCCoverageMeritRatingStaging
),
archDCWCCoverageMeritRatingStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCWCCoverageMeritRatingStaging
	(CoverageId, WC_CoverageMeritRatingId, SessionId, ALMeritRatingSelections, ARMeritRatingSelections, DEMeritRatingSelections, GAMeritRatingSelections, HIMeritRatingSelections, MAMeritRatingSelections, MEMeritRatingSelections, MIMeritRatingSelections, NYMeritRatingSelections, OKMeritRatingSelections, ORMeritRatingSelections, PAMeritRatingSelections, SDMeritRatingSelections, ExtractDate, SourceSystemId, AuditId)
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
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),
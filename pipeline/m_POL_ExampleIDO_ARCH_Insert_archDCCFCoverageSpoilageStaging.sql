WITH
SQ_DCCFCoverageSpoilageStaging AS (
	SELECT
		CoverageId,
		CF_CoverageSpoilageId,
		SessionId,
		ARate,
		BreakdownContamination,
		PowerOutage,
		Class,
		RefrigerationMaintenanceAgreement,
		CatastropheArea,
		SellingPrice,
		ExtractDate,
		SourceSystemId
	FROM DCCFCoverageSpoilageStaging
),
EXP_Metadata AS (
	SELECT
	CoverageId,
	CF_CoverageSpoilageId,
	SessionId,
	ARate,
	BreakdownContamination AS i_BreakdownContamination,
	-- *INF*: DECODE(i_BreakdownContamination, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_BreakdownContamination,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_BreakdownContamination,
	PowerOutage AS i_PowerOutage,
	-- *INF*: DECODE(i_PowerOutage, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_PowerOutage,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_PowerOutage,
	Class,
	RefrigerationMaintenanceAgreement AS i_RefrigerationMaintenanceAgreement,
	-- *INF*: DECODE(i_RefrigerationMaintenanceAgreement, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_RefrigerationMaintenanceAgreement,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_RefrigerationMaintenanceAgreement,
	CatastropheArea AS i_CatastropheArea,
	-- *INF*: DECODE(i_CatastropheArea, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_CatastropheArea,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_CatastropheArea,
	SellingPrice AS i_SellingPrice,
	-- *INF*: DECODE(i_SellingPrice, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_SellingPrice,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_SellingPrice,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCFCoverageSpoilageStaging
),
archDCCFCoverageSpoilageStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCCFCoverageSpoilageStaging
	(CoverageId, CF_CoverageSpoilageId, SessionId, ARate, BreakdownContamination, PowerOutage, Class, RefrigerationMaintenanceAgreement, CatastropheArea, SellingPrice, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	COVERAGEID, 
	CF_COVERAGESPOILAGEID, 
	SESSIONID, 
	ARATE, 
	o_BreakdownContamination AS BREAKDOWNCONTAMINATION, 
	o_PowerOutage AS POWEROUTAGE, 
	CLASS, 
	o_RefrigerationMaintenanceAgreement AS REFRIGERATIONMAINTENANCEAGREEMENT, 
	o_CatastropheArea AS CATASTROPHEAREA, 
	o_SellingPrice AS SELLINGPRICE, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),
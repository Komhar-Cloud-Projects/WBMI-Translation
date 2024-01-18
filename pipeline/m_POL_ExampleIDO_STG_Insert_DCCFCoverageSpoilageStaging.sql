WITH
SQ_DC_CF_CoverageSpoilage AS (
	WITH cte_DCCFCoverageSpoilage(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.CF_CoverageSpoilageId, 
	X.SessionId, 
	X.ARate, 
	X.BreakdownContamination, 
	X.PowerOutage, 
	X.Class, 
	X.RefrigerationMaintenanceAgreement, 
	X.CatastropheArea, 
	X.SellingPrice 
	FROM
	DC_CF_CoverageSpoilage X
	inner join
	cte_DCCFCoverageSpoilage Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CoverageId,
	CF_CoverageSpoilageId,
	SessionId,
	ARate,
	BreakdownContamination AS i_BreakdownContamination,
	-- *INF*: DECODE(i_BreakdownContamination,'T',1,'F',0,NULL)
	DECODE(
	    i_BreakdownContamination,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_BreakdownContamination,
	PowerOutage AS i_PowerOutage,
	-- *INF*: DECODE(i_PowerOutage,'T',1,'F',0,NULL)
	DECODE(
	    i_PowerOutage,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PowerOutage,
	Class,
	RefrigerationMaintenanceAgreement AS i_RefrigerationMaintenanceAgreement,
	-- *INF*: DECODE(i_RefrigerationMaintenanceAgreement,'T',1,'F',0,NULL)
	DECODE(
	    i_RefrigerationMaintenanceAgreement,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_RefrigerationMaintenanceAgreement,
	CatastropheArea AS i_CatastropheArea,
	-- *INF*: DECODE(i_CatastropheArea,'T',1,'F',0,NULL)
	DECODE(
	    i_CatastropheArea,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_CatastropheArea,
	SellingPrice AS i_SellingPrice,
	-- *INF*: DECODE(i_SellingPrice,'T',1,'F',0,NULL)
	DECODE(
	    i_SellingPrice,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_SellingPrice,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CF_CoverageSpoilage
),
DCCFCoverageSpoilageStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFCoverageSpoilageStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFCoverageSpoilageStaging
	(CoverageId, CF_CoverageSpoilageId, SessionId, ARate, BreakdownContamination, PowerOutage, Class, RefrigerationMaintenanceAgreement, CatastropheArea, SellingPrice, ExtractDate, SourceSystemId)
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
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),
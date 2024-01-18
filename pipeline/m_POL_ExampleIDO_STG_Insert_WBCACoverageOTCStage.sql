WITH
SQ_WB_CA_CoverageOTC AS (
	WITH cte_WBCACoverageOTCStage(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CA_CoverageOTCId, 
	X.WB_CA_CoverageOTCId, 
	X.SessionId, 
	X.AntiTheftDeviceDiscountKY,
	X.AntiTheftDeviceDiscountMN,
	X.AcceptOTCCoverageSoftMsg,
	X.ReplacementCost,
	X.FullSafetyGlassCoverage,
	X.DeductibleType
	FROM
	WB_CA_CoverageOTC X
	inner join
	cte_WBCACoverageOTCStage Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CA_CoverageOTCId,
	WB_CA_CoverageOTCId,
	SessionId,
	AntiTheftDeviceDiscountKY,
	AntiTheftDeviceDiscountMN,
	AcceptOTCCoverageSoftMsg,
	ReplacementCost AS i_ReplacementCost,
	-- *INF*: DECODE(i_ReplacementCost,'T',1,'F',0,NULL)
	DECODE(
	    i_ReplacementCost,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ReplacementCost,
	FullSafetyGlassCoverage AS i_FullSafetyGlassCoverage,
	-- *INF*: DECODE(i_FullSafetyGlassCoverage,'T',1,'F',0,NULL)
	DECODE(
	    i_FullSafetyGlassCoverage,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_FullSafetyGlassCoverage,
	DeductibleType
	FROM SQ_WB_CA_CoverageOTC
),
WBCACoverageOTCStage AS (
	TRUNCATE TABLE WBCACoverageOTCStage;
	INSERT INTO WBCACoverageOTCStage
	(ExtractDate, SourceSystemId, CA_CoverageOTCId, WB_CA_CoverageOTCId, SessionId, AntiTheftDeviceDiscountKY, AntiTheftDeviceDiscountMN, AcceptOTCCoverageSoftMsg, ReplacementCost, FullSafetyGlassCoverage, DeductibleType)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CA_COVERAGEOTCID, 
	WB_CA_COVERAGEOTCID, 
	SESSIONID, 
	ANTITHEFTDEVICEDISCOUNTKY, 
	ANTITHEFTDEVICEDISCOUNTMN, 
	ACCEPTOTCCOVERAGESOFTMSG, 
	o_ReplacementCost AS REPLACEMENTCOST, 
	o_FullSafetyGlassCoverage AS FULLSAFETYGLASSCOVERAGE, 
	DEDUCTIBLETYPE
	FROM EXP_Metadata
),
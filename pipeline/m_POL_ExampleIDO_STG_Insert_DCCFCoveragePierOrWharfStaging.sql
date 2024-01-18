WITH
SQ_DC_CF_CoveragePierOrWharf AS (
	WITH cte_DCCFCoveragePierOrWharf(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.CF_CoveragePierOrWharfId, 
	X.SessionId, 
	X.PierOrWharfCauseOfLoss, 
	X.PremiumBLDG, 
	X.PremiumPP, 
	X.PremiumPO, 
	X.PremiumTIME, 
	X.PremiumEE 
	FROM
	DC_CF_CoveragePierOrWharf X
	inner join
	cte_DCCFCoveragePierOrWharf Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CoverageId,
	CF_CoveragePierOrWharfId,
	SessionId,
	PierOrWharfCauseOfLoss,
	PremiumBLDG,
	PremiumPP,
	PremiumPO,
	PremiumTIME,
	PremiumEE,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CF_CoveragePierOrWharf
),
DCCFCoveragePierOrWharfStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFCoveragePierOrWharfStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFCoveragePierOrWharfStaging
	(CoverageId, CF_CoveragePierOrWharfId, SessionId, PierOrWharfCauseOfLoss, PremiumBLDG, PremiumPP, PremiumPO, PremiumTIME, PremiumEE, ExtractDate, SourceSystemId)
	SELECT 
	COVERAGEID, 
	CF_COVERAGEPIERORWHARFID, 
	SESSIONID, 
	PIERORWHARFCAUSEOFLOSS, 
	PREMIUMBLDG, 
	PREMIUMPP, 
	PREMIUMPO, 
	PREMIUMTIME, 
	PREMIUMEE, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),
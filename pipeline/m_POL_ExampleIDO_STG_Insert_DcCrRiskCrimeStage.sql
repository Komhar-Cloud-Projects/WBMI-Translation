WITH
SQ_CR_Risk_Crime AS (
	WITH cte_DCCRRiskCrime(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CR_RiskId, 
	X.CR_RiskCrimeId, 
	X.SessionId, 
	X.Id, 
	X.Type 
	FROM
	DC_CR_RiskCrime X
	inner join
	cte_DCCRRiskCrime Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CR_RiskId,
	CR_RiskCrimeId,
	SessionId,
	Id,
	Type,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_CR_Risk_Crime
),
DcCrRiskCrimeStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DcCrRiskCrimeStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DcCrRiskCrimeStage
	(CrRiskId, CrRiskCrimeId, SessionId, Id, Type, ExtractDate, SourceSystemId)
	SELECT 
	CR_RiskId AS CRRISKID, 
	CR_RiskCrimeId AS CRRISKCRIMEID, 
	SESSIONID, 
	ID, 
	TYPE, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),
WITH
SQ_WB_CA_CoveragePIP AS (
	WITH cte_WBCACoveragePIP(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CA_CoveragePIPId
	, X.WB_CA_CoveragePIPId
	, X.SessionId
	, X.RideSharingArrangement
	, X.RideSharingUsage
	, X.MedicalExpenses 
	
	FROM
	WB_CA_CoveragePIP X
	inner join
	cte_WBCACoveragePIP Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXPTRANS AS (
	SELECT
	CA_CoveragePIPId,
	WB_CA_CoveragePIPId,
	SessionId,
	RideSharingArrangement,
	RideSharingUsage,
	MedicalExpenses,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_CA_CoveragePIP
),
WBCACoveragePIPStage AS (
	TRUNCATE TABLE WBCACoveragePIPStage;
	INSERT INTO WBCACoveragePIPStage
	(ExtractDate, SourceSystemId, CACoveragePIPId, WBCACoveragePIPId, SessionId, RideSharingArrangement, RideSharingUsage, MedicalExpenses)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CA_CoveragePIPId AS CACOVERAGEPIPID, 
	WB_CA_CoveragePIPId AS WBCACOVERAGEPIPID, 
	SESSIONID, 
	RIDESHARINGARRANGEMENT, 
	RIDESHARINGUSAGE, 
	MEDICALEXPENSES
	FROM EXPTRANS
),
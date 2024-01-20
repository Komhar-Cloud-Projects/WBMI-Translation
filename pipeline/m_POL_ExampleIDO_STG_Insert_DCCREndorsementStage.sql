WITH
SQ_DC_CR_Endorsement AS (
	WITH cte_DCCREndorsement(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CR_RiskId, 
	X.CR_EndorsementId, 
	X.SessionId, 
	X.Id, 
	X.Type, 
	X.FaithfulPerformanceCoverageWritten 
	FROM
	DC_CR_Endorsement X
	inner join
	cte_DCCREndorsement Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXPTRANS AS (
	SELECT
	CR_RiskId,
	CR_EndorsementId,
	SessionId,
	Id,
	Type,
	FaithfulPerformanceCoverageWritten,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CR_Endorsement
),
DCCREndorsementStage AS (
	TRUNCATE TABLE DCCREndorsementStage;
	INSERT INTO DCCREndorsementStage
	(ExtractDate, SourceSystemId, CRRiskId, CREndorsementId, SessionId, Id, Type, FaithfulPerformanceCoverageWritten)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CR_RiskId AS CRRISKID, 
	CR_EndorsementId AS CRENDORSEMENTID, 
	SESSIONID, 
	ID, 
	TYPE, 
	FAITHFULPERFORMANCECOVERAGEWRITTEN
	FROM EXPTRANS
),
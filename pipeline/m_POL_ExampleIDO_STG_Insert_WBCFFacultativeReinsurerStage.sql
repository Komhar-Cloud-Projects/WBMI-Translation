WITH
SQ_WB_CF_FacultativeReinsurer AS (
	WITH cte_WBCFFacultativeReinsurer(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.WB_CF_ReinsuranceId, 
	X.WB_CF_FacultativeReinsurerId, 
	X.SessionId, 
	X.CertificateReceived, 
	X.ReinsurerName, 
	X.Type, 
	X.AmountCeded, 
	X.ReinsurerPremium 
	FROM
	WB_CF_FacultativeReinsurer X
	inner join
	cte_WBCFFacultativeReinsurer Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	WB_CF_ReinsuranceId,
	WB_CF_FacultativeReinsurerId,
	SessionId,
	CertificateReceived,
	ReinsurerName,
	Type,
	AmountCeded,
	ReinsurerPremium,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_CF_FacultativeReinsurer
),
WBCFFacultativeReinsurerStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCFFacultativeReinsurerStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCFFacultativeReinsurerStage
	(ExtractDate, SourceSystemId, WBCFReinsuranceId, WBCFFacultativeReinsurerId, SessionId, CertificateReceived, ReinsurerName, Type, AmountCeded, ReinsurerPremium)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	WB_CF_ReinsuranceId AS WBCFREINSURANCEID, 
	WB_CF_FacultativeReinsurerId AS WBCFFACULTATIVEREINSURERID, 
	SESSIONID, 
	CERTIFICATERECEIVED, 
	REINSURERNAME, 
	TYPE, 
	AMOUNTCEDED, 
	REINSURERPREMIUM
	FROM EXP_Metadata
),
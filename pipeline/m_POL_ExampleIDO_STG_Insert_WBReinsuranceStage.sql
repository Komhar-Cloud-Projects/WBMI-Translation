WITH
SQ_WB_Reinsurance AS (
	WITH cte_WBReinsurance(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.ReinsuranceId, 
	X.WB_ReinsuranceId, 
	X.SessionId, 
	X.CertificateReceived, 
	X.GrossReinsurancePremium, 
	X.NetReinsurancePremium 
	FROM
	WB_Reinsurance X
	inner join
	cte_WBReinsurance Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	ReinsuranceId,
	WB_ReinsuranceId,
	SessionId,
	CertificateReceived,
	GrossReinsurancePremium,
	NetReinsurancePremium,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_Reinsurance
),
WBReinsuranceStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBReinsuranceStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBReinsuranceStage
	(ExtractDate, SourceSystemId, ReinsuranceId, WBReinsuranceId, SessionId, CertificateReceived, GrossReinsurancePremium, NetReinsurancePremium)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	REINSURANCEID, 
	WB_ReinsuranceId AS WBREINSURANCEID, 
	SESSIONID, 
	CERTIFICATERECEIVED, 
	GROSSREINSURANCEPREMIUM, 
	NETREINSURANCEPREMIUM
	FROM EXP_Metadata
),
WITH
SQ_WB_WC_Reinsurance AS (
	WITH cte_WBWCReinsurance(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.WB_CL_ReinsuranceId, 
	X.WB_WC_ReinsuranceId, 
	X.SessionId, 
	X.CertificateReceived, 
	X.Premium, 
	X.PolicyTerms, 
	X.RetentionLimit, 
	X.CededLimit 
	FROM  
	WB_WC_Reinsurance X
	inner join
	cte_WBWCReinsurance Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	WB_CL_ReinsuranceId,
	WB_WC_ReinsuranceId,
	SessionId,
	CertificateReceived,
	Premium,
	PolicyTerms,
	RetentionLimit,
	CededLimit,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_WC_Reinsurance
),
WBWCReinsuranceStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBWCReinsuranceStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBWCReinsuranceStage
	(ExtractDate, SourceSystemId, WBCLReinsuranceId, WBWCReinsuranceId, SessionId, CertificateReceived, Premium, PolicyTerms, RetentionLimit, CededLimit)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	WB_CL_ReinsuranceId AS WBCLREINSURANCEID, 
	WB_WC_ReinsuranceId AS WBWCREINSURANCEID, 
	SESSIONID, 
	CERTIFICATERECEIVED, 
	PREMIUM, 
	POLICYTERMS, 
	RETENTIONLIMIT, 
	CEDEDLIMIT
	FROM EXP_Metadata
),
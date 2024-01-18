WITH
SQ_DC_Reinsurance AS (
	WITH cte_DCReinsurance(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.PolicyId, 
	X.ReinsuranceId, 
	X.SessionId, 
	X.Id, 
	X.Type, 
	X.AggregateLimit, 
	X.CertificatePolicyNumber, 
	X.CommissionRate, 
	X.OccurrenceLimit, 
	X.PercentCeded, 
	X.PercentLoss, 
	X.Company, 
	X.CompanyNumber, 
	X.EffectiveDate, 
	X.ExpirationDate 
	FROM
	DC_Reinsurance X
	inner join
	cte_DCReinsurance Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	PolicyId,
	ReinsuranceId,
	SessionId,
	Id,
	Type,
	AggregateLimit,
	CertificatePolicyNumber,
	CommissionRate,
	OccurrenceLimit,
	PercentCeded,
	PercentLoss,
	Company,
	CompanyNumber,
	EffectiveDate,
	ExpirationDate,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_Reinsurance
),
DCReinsuranceStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCReinsuranceStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCReinsuranceStaging
	(PolicyId, ReinsuranceId, SessionId, Id, Type, AggregateLimit, CertificatePolicyNumber, CommissionRate, OccurrenceLimit, PercentCeded, PercentLoss, Company, CompanyNumber, EffectiveDate, ExpirationDate, ExtractDate, SourceSystemId)
	SELECT 
	POLICYID, 
	REINSURANCEID, 
	SESSIONID, 
	ID, 
	TYPE, 
	AGGREGATELIMIT, 
	CERTIFICATEPOLICYNUMBER, 
	COMMISSIONRATE, 
	OCCURRENCELIMIT, 
	PERCENTCEDED, 
	PERCENTLOSS, 
	COMPANY, 
	COMPANYNUMBER, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),
WITH
SQ_DC_PriorInsurance AS (
	WITH cte_DCPriorInsurance(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.PolicyId, 
	X.PriorInsuranceId, 
	X.SessionId, 
	X.Id, 
	X.CarrierName, 
	X.EffectiveDate, 
	X.ExpirationDate, 
	X.PolicyNumber, 
	X.PolicyType, 
	X.ModificationFactor, 
	X.TotalPremium 
	FROM
	DC_PriorInsurance X
	inner join
	cte_DCPriorInsurance Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	PolicyId,
	PriorInsuranceId,
	SessionId,
	Id,
	CarrierName,
	EffectiveDate,
	ExpirationDate,
	PolicyNumber,
	PolicyType,
	ModificationFactor,
	TotalPremium,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_PriorInsurance
),
DCPriorInsuranceStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCPriorInsuranceStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCPriorInsuranceStaging
	(PolicyId, PriorInsuranceId, SessionId, Id, CarrierName, EffectiveDate, ExpirationDate, PolicyNumber, PolicyType, ModificationFactor, TotalPremium, ExtractDate, SourceSystemId)
	SELECT 
	POLICYID, 
	PRIORINSURANCEID, 
	SESSIONID, 
	ID, 
	CARRIERNAME, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	POLICYNUMBER, 
	POLICYTYPE, 
	MODIFICATIONFACTOR, 
	TOTALPREMIUM, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),
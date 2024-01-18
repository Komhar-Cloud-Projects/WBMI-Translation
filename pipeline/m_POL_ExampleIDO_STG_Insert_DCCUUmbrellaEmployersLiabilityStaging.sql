WITH
SQ_DC_CU_UmbrellaEmployersLiability AS (
	WITH cte_DCCUUmbrellaEmployersLiability(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.CU_UmbrellaEmployersLiabilityId, 
	X.SessionId, 
	X.Id, 
	X.CarrierName, 
	X.Description, 
	X.EffectiveDate, 
	X.ExpirationDate, 
	X.PolicyNumber 
	FROM
	DC_CU_UmbrellaEmployersLiability X
	inner join
	cte_DCCUUmbrellaEmployersLiability Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	LineId,
	CU_UmbrellaEmployersLiabilityId,
	SessionId,
	Id,
	CarrierName,
	Description,
	EffectiveDate,
	ExpirationDate,
	PolicyNumber,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CU_UmbrellaEmployersLiability
),
DCCUUmbrellaEmployersLiabilityStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCUUmbrellaEmployersLiabilityStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCUUmbrellaEmployersLiabilityStaging
	(ExtractDate, SourceSystemId, LineId, CU_UmbrellaEmployersLiabilityId, SessionId, Id, CarrierName, Description, EffectiveDate, ExpirationDate, PolicyNumber)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	LINEID, 
	CU_UMBRELLAEMPLOYERSLIABILITYID, 
	SESSIONID, 
	ID, 
	CARRIERNAME, 
	DESCRIPTION, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	POLICYNUMBER
	FROM EXP_Metadata
),
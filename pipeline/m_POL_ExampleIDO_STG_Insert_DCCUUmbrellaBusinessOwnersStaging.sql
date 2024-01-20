WITH
SQ_DC_CU_UmbrellaBusinessOwners AS (
	WITH cte_DCCUUmbrellaBusinessOwners(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.CU_UmbrellaBusinessOwnersId, 
	X.SessionId, 
	X.Id, 
	X.Description, 
	X.EffectiveDate, 
	X.ExpirationDate, 
	X.PersonalLiability, 
	X.PolicyNumber 
	FROM
	DC_CU_UmbrellaBusinessOwners X
	inner join
	cte_DCCUUmbrellaBusinessOwners Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	LineId,
	CU_UmbrellaBusinessOwnersId,
	SessionId,
	Id,
	Description,
	EffectiveDate,
	ExpirationDate,
	PersonalLiability,
	PolicyNumber,
	-- *INF*: DECODE(PersonalLiability, 'T', 1, 'F', 0, NULL)
	DECODE(
	    PersonalLiability,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PersonalLiability,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CU_UmbrellaBusinessOwners
),
DCCUUmbrellaBusinessOwnersStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCUUmbrellaBusinessOwnersStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCUUmbrellaBusinessOwnersStaging
	(ExtractDate, SourceSystemId, LineId, CU_UmbrellaBusinessOwnersId, SessionId, Id, Description, EffectiveDate, ExpirationDate, PersonalLiability, PolicyNumber)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	LINEID, 
	CU_UMBRELLABUSINESSOWNERSID, 
	SESSIONID, 
	ID, 
	DESCRIPTION, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	o_PersonalLiability AS PERSONALLIABILITY, 
	POLICYNUMBER
	FROM EXP_Metadata
),
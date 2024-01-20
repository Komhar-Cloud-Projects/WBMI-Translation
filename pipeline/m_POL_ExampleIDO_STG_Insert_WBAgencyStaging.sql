WITH
SQ_WBAgencyStaging AS (
	WITH cte_WBAgency(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.PartyId, 
	X.WB_AgencyId, 
	X.SessionId, 
	X.Reference 
	FROM
	WB_Agency X
	inner join
	cte_WBAgency Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	PartyId,
	WB_AgencyId,
	SessionId,
	Reference,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WBAgencyStaging
),
WBAgencyStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBAgencyStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBAgencyStaging
	(PartyId, WB_AgencyId, SessionId, Reference, ExtractDate, SourceSystemId)
	SELECT 
	PARTYID, 
	WB_AGENCYID, 
	SESSIONID, 
	REFERENCE, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),
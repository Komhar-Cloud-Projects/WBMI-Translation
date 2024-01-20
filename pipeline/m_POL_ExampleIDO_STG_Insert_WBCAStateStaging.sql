WITH
SQ_WB_CA_State AS (
	WITH cte_WBCAState(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CA_StateId, 
	X.WB_CA_StateId, 
	X.SessionId, 
	X.ReinsuranceApplies, 
	X.PageHasBeenVisited, 
	X.RejectionDate, 
	X.AdditionalLimitKS, 
	X.AdditionalLimitKY, 
	X.AdditionalLimitMN, 
	X.PipWorkComp, 
	X.AdditionalLimitIndicator, 
	X.LocationSelectForTerritory 
	FROM
	WB_CA_State X
	inner join
	cte_WBCAState Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CA_StateId,
	WB_CA_StateId,
	SessionId,
	ReinsuranceApplies,
	-- *INF*: DECODE(ReinsuranceApplies,'T',1,'F',0,NULL)
	DECODE(
	    ReinsuranceApplies,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS ReinsuranceApplies_out,
	PageHasBeenVisited,
	-- *INF*: DECODE(PageHasBeenVisited,'T',1,'F',0,NULL)
	DECODE(
	    PageHasBeenVisited,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS PageHasBeenVisited_out,
	RejectionDate,
	AdditionalLimitKS,
	AdditionalLimitKY,
	AdditionalLimitMN,
	PipWorkComp,
	-- *INF*: DECODE(PipWorkComp,'T',1,'F',0,NULL)
	DECODE(
	    PipWorkComp,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS PipWorkComp_out,
	AdditionalLimitIndicator,
	-- *INF*: DECODE(AdditionalLimitIndicator,'T',1,'F',0,NULL)
	DECODE(
	    AdditionalLimitIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS AdditionalLimitIndicator_out,
	LocationSelectForTerritory
	FROM SQ_WB_CA_State
),
WBCAStateStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCAStateStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCAStateStaging
	(ExtractDate, SourceSystemId, CA_StateId, WB_CA_StateId, SessionId, ReinsuranceApplies, PageHasBeenVisited, RejectionDate, AdditionalLimitKS, AdditionalLimitKY, AdditionalLimitMN, PipWorkComp, AdditionalLimitIndicator, LocationSelectForTerritory)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CA_STATEID, 
	WB_CA_STATEID, 
	SESSIONID, 
	ReinsuranceApplies_out AS REINSURANCEAPPLIES, 
	PageHasBeenVisited_out AS PAGEHASBEENVISITED, 
	REJECTIONDATE, 
	ADDITIONALLIMITKS, 
	ADDITIONALLIMITKY, 
	ADDITIONALLIMITMN, 
	PipWorkComp_out AS PIPWORKCOMP, 
	AdditionalLimitIndicator_out AS ADDITIONALLIMITINDICATOR, 
	LOCATIONSELECTFORTERRITORY
	FROM EXP_Metadata
),
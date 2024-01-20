WITH
SQ_WBCAStateStaging AS (
	SELECT
		WBCAStateStagingId,
		ExtractDate,
		SourceSystemId,
		CA_StateId,
		WB_CA_StateId,
		SessionId,
		ReinsuranceApplies,
		PageHasBeenVisited,
		RejectionDate,
		AdditionalLimitKS,
		AdditionalLimitKY,
		AdditionalLimitMN,
		PipWorkComp,
		AdditionalLimitIndicator,
		LocationSelectForTerritory
	FROM WBCAStateStaging
),
EXP_Metadata AS (
	SELECT
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
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
	FROM SQ_WBCAStateStaging
),
ArchWBCAStateStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBCAStateStaging
	(ExtractDate, SourceSystemId, AuditId, CA_StateId, WB_CA_StateId, SessionId, ReinsuranceApplies, PageHasBeenVisited, RejectionDate, AdditionalLimitKS, AdditionalLimitKY, AdditionalLimitMN, PipWorkComp, AdditionalLimitIndicator, LocationSelectForTerritory)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
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
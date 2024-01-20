WITH
SQ_to_DC_CU_UmbrellaForm AS (
	WITH cte_DCCUUmbrellaForm(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CU_LineId, 
	X.CU_UmbrellaFormId, 
	X.SessionId, 
	X.Id, 
	X.BICoverageProvided, 
	X.GarageBIAndPDNotApplicable, 
	X.PDCoverageProvided, 
	X.PersonalAdvertisingInjuryCoverageProvided 
	FROM
	DC_CU_UmbrellaForm X
	inner join
	cte_DCCUUmbrellaForm Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_handle AS (
	SELECT
	CU_LineId,
	CU_UmbrellaFormId,
	SessionId,
	Id,
	BICoverageProvided,
	GarageBIAndPDNotApplicable,
	PDCoverageProvided,
	PersonalAdvertisingInjuryCoverageProvided,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemid,
	-- *INF*: decode(BICoverageProvided,'T',1,'F',0,NULL)
	decode(
	    BICoverageProvided,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_BICoverageProvided,
	-- *INF*: DECODE(GarageBIAndPDNotApplicable,'T',1,'F',0,NULL)
	DECODE(
	    GarageBIAndPDNotApplicable,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_GarageBIAndPDNotApplicable,
	-- *INF*: decode(PDCoverageProvided,'T',1,'F',0,NULL)
	decode(
	    PDCoverageProvided,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PDCoverageProvided,
	-- *INF*: decode(PersonalAdvertisingInjuryCoverageProvided,'T',1,'F',0,NULL)
	decode(
	    PersonalAdvertisingInjuryCoverageProvided,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PersonalAdvertisingInjuryCoverageProvided
	FROM SQ_to_DC_CU_UmbrellaForm
),
DCCUUmbrellaFormStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCUUmbrellaFormStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCUUmbrellaFormStaging
	(ExtractDate, SourceSystemId, CU_LineId, CU_UmbrellaFormId, SessionId, Id, BICoverageProvided, GarageBIAndPDNotApplicable, PDCoverageProvided, PersonalAdvertisingInjuryCoverageProvided)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemid AS SOURCESYSTEMID, 
	CU_LINEID, 
	CU_UMBRELLAFORMID, 
	SESSIONID, 
	ID, 
	o_BICoverageProvided AS BICOVERAGEPROVIDED, 
	o_GarageBIAndPDNotApplicable AS GARAGEBIANDPDNOTAPPLICABLE, 
	o_PDCoverageProvided AS PDCOVERAGEPROVIDED, 
	o_PersonalAdvertisingInjuryCoverageProvided AS PERSONALADVERTISINGINJURYCOVERAGEPROVIDED
	FROM EXP_handle
),
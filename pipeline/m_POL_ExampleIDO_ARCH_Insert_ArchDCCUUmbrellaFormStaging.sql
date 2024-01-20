WITH
SQ_DCCUUmbrellaFormStaging AS (
	SELECT
		ExtractDate,
		SourceSystemId,
		CU_LineId,
		CU_UmbrellaFormId,
		SessionId,
		Id,
		BICoverageProvided,
		GarageBIAndPDNotApplicable,
		PDCoverageProvided,
		PersonalAdvertisingInjuryCoverageProvided
	FROM DCCUUmbrellaFormStaging1
),
EXP_Metadata AS (
	SELECT
	ExtractDate,
	SourceSystemId,
	CU_LineId,
	CU_UmbrellaFormId,
	SessionId,
	Id,
	BICoverageProvided,
	GarageBIAndPDNotApplicable,
	PDCoverageProvided,
	PersonalAdvertisingInjuryCoverageProvided,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	-- *INF*: decode(BICoverageProvided,'T',1,'F',0,NULL)
	decode(
	    BICoverageProvided,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_BICoverageProvided,
	-- *INF*: decode(GarageBIAndPDNotApplicable,'T',1,'F',0,NULL)
	decode(
	    GarageBIAndPDNotApplicable,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_GarageBIAndPDNotApplicable,
	-- *INF*: decode(PDCoverageProvided,'T',1,'F',NULL)
	decode(
	    PDCoverageProvided,
	    'T', 1,
	    'F', NULL
	) AS o_PDCoverageProvided,
	-- *INF*: decode(PersonalAdvertisingInjuryCoverageProvided,'T',1,'F',NULL)
	decode(
	    PersonalAdvertisingInjuryCoverageProvided,
	    'T', 1,
	    'F', NULL
	) AS o_PersonalAdvertisingInjuryCoverageProvided
	FROM SQ_DCCUUmbrellaFormStaging
),
ArchDCCUUmbrellaFormStaging1 AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCUUmbrellaFormStaging
	(ExtractDate, SourceSystemId, AuditId, CU_LineId, CU_UmbrellaFormId, SessionId, Id, BICoverageProvided, GarageBIAndPDNotApplicable, PDCoverageProvided, PersonalAdvertisingInjuryCoverageProvided)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	CU_LINEID, 
	CU_UMBRELLAFORMID, 
	SESSIONID, 
	ID, 
	o_BICoverageProvided AS BICOVERAGEPROVIDED, 
	o_GarageBIAndPDNotApplicable AS GARAGEBIANDPDNOTAPPLICABLE, 
	o_PDCoverageProvided AS PDCOVERAGEPROVIDED, 
	o_PersonalAdvertisingInjuryCoverageProvided AS PERSONALADVERTISINGINJURYCOVERAGEPROVIDED
	FROM EXP_Metadata
),
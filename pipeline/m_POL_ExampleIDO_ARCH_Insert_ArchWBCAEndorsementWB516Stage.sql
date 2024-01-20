WITH
SQ_WBCAEndorsementWB516Stage AS (
	SELECT
		WBCAEndorsementWB516StageId,
		ExtractDate,
		SourceSystemid,
		WB_CoverageId,
		WB_CA_EndorsementWB516Id,
		SessionId,
		RetroactiveDate,
		NumberEmployees
	FROM WBCAEndorsementWB516Stage
),
EXP_Metadata AS (
	SELECT
	WBCAEndorsementWB516StageId,
	ExtractDate,
	SourceSystemid,
	WB_CoverageId,
	WB_CA_EndorsementWB516Id,
	SessionId,
	RetroactiveDate,
	NumberEmployees,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBCAEndorsementWB516Stage
),
ArchWBCAEndorsementWB516Stage AS (
	INSERT INTO ArchWBCAEndorsementWB516Stage
	(ExtractDate, SourceSystemId, AuditId, WBCAEndorsementWB516StageId, WB_CoverageId, WB_CA_EndorsementWB516Id, SessionId, RetroactiveDate, NumberEmployees)
	SELECT 
	EXTRACTDATE, 
	SourceSystemid AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBCAENDORSEMENTWB516STAGEID, 
	WB_COVERAGEID, 
	WB_CA_ENDORSEMENTWB516ID, 
	SESSIONID, 
	RETROACTIVEDATE, 
	NUMBEREMPLOYEES
	FROM EXP_Metadata
),
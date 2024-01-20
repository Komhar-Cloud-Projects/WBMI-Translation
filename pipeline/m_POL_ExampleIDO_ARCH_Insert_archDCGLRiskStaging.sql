WITH
SQ_DCGLRiskStaging AS (
	SELECT
		LineId,
		GL_LocationId,
		GL_RiskId,
		SessionId,
		Id,
		Auditable,
		CompositeRating,
		CompositeType,
		IfAnyExposure,
		RiskExcludeDeductible,
		SpecialCombinedMinimum,
		Type,
		UnderwriterOverride,
		UnitsDivider,
		WithdrawalType,
		Exposure,
		ExposureEstimated,
		ExposureAudited,
		GL_LocationXmlId,
		ExtractDate,
		SourceSystemId,
		CompositeRatingID
	FROM DCGLRiskStaging
),
EXP_Metadata AS (
	SELECT
	LineId,
	GL_LocationId,
	GL_RiskId,
	SessionId,
	Id,
	Auditable,
	CompositeRating,
	CompositeType,
	IfAnyExposure,
	RiskExcludeDeductible,
	SpecialCombinedMinimum,
	Type,
	UnderwriterOverride,
	UnitsDivider,
	WithdrawalType,
	Exposure,
	ExposureEstimated,
	ExposureAudited,
	GL_LocationXmlId,
	ExtractDate,
	SourceSystemId,
	-- *INF*: DECODE(Auditable,'T',1,'F',0,NULL)
	DECODE(
	    Auditable,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Auditable,
	-- *INF*: DECODE(CompositeRating,'T',1,'F',0,NULL)
	DECODE(
	    CompositeRating,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_CompositeRating,
	-- *INF*: DECODE(IfAnyExposure,'T',1,'F',0,NULL)
	DECODE(
	    IfAnyExposure,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IfAnyExposure,
	-- *INF*: DECODE(RiskExcludeDeductible,'T',1,'F',0,NULL)
	DECODE(
	    RiskExcludeDeductible,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_RiskExcludeDeductible,
	-- *INF*: DECODE(SpecialCombinedMinimum,'T',1,'F',0,NULL)
	DECODE(
	    SpecialCombinedMinimum,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_SpecialCombinedMinimum,
	-- *INF*: DECODE(UnderwriterOverride,'T',1,'F',0,NULL)
	DECODE(
	    UnderwriterOverride,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_UnderwriterOverride,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	CompositeRatingID
	FROM SQ_DCGLRiskStaging
),
archDCGLRiskStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCGLRiskStaging
	(LineId, GL_LocationId, GL_RiskId, SessionId, Id, Auditable, CompositeRating, CompositeType, IfAnyExposure, RiskExcludeDeductible, SpecialCombinedMinimum, Type, UnderwriterOverride, UnitsDivider, WithdrawalType, Exposure, ExposureEstimated, ExposureAudited, GL_LocationXmlId, ExtractDate, SourceSystemId, AuditId, CompositeRatingID)
	SELECT 
	LINEID, 
	GL_LOCATIONID, 
	GL_RISKID, 
	SESSIONID, 
	ID, 
	o_Auditable AS AUDITABLE, 
	o_CompositeRating AS COMPOSITERATING, 
	COMPOSITETYPE, 
	o_IfAnyExposure AS IFANYEXPOSURE, 
	o_RiskExcludeDeductible AS RISKEXCLUDEDEDUCTIBLE, 
	o_SpecialCombinedMinimum AS SPECIALCOMBINEDMINIMUM, 
	TYPE, 
	o_UnderwriterOverride AS UNDERWRITEROVERRIDE, 
	UNITSDIVIDER, 
	WITHDRAWALTYPE, 
	EXPOSURE, 
	EXPOSUREESTIMATED, 
	EXPOSUREAUDITED, 
	GL_LOCATIONXMLID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	COMPOSITERATINGID
	FROM EXP_Metadata
),
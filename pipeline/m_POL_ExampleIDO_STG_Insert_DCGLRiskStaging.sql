WITH
SQ_DC_GL_Risk AS (
	WITH cte_DCGLRisk(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.GL_LocationId, 
	X.GL_RiskId, 
	X.SessionId, 
	X.Id, 
	X.Auditable, 
	X.CompositeRating, 
	X.CompositeType, 
	X.IfAnyExposure, 
	X.RiskExcludeDeductible, 
	X.SpecialCombinedMinimum, 
	X.Type, 
	X.UnderwriterOverride, 
	X.UnitsDivider, 
	X.WithdrawalType, 
	X.Exposure, 
	X.ExposureEstimated, 
	X.ExposureAudited,
	X.CompositeRatingID, 
	X.GL_LocationXmlId 
	FROM
	DC_GL_Risk X
	inner join
	cte_DCGLRisk Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	LineId AS i_LineId,
	GL_LocationId AS i_GL_LocationId,
	GL_RiskId AS i_GL_RiskId,
	SessionId AS i_SessionId,
	Id AS i_Id,
	Auditable AS i_Auditable,
	CompositeRating AS i_CompositeRating,
	CompositeType AS i_CompositeType,
	IfAnyExposure AS i_IfAnyExposure,
	RiskExcludeDeductible AS i_RiskExcludeDeductible,
	SpecialCombinedMinimum AS i_SpecialCombinedMinimum,
	Type AS i_Type,
	UnderwriterOverride AS i_UnderwriterOverride,
	UnitsDivider AS i_UnitsDivider,
	WithdrawalType AS i_WithdrawalType,
	Exposure AS i_Exposure,
	ExposureEstimated AS i_ExposureEstimated,
	ExposureAudited AS i_ExposureAudited,
	CompositeRatingID,
	GL_LocationXmlId AS i_GL_LocationXmlId,
	i_LineId AS o_LineId,
	i_GL_LocationId AS o_GL_LocationId,
	i_GL_RiskId AS o_GL_RiskId,
	i_SessionId AS o_SessionId,
	i_Id AS o_Id,
	-- *INF*: DECODE(i_Auditable,'T',1,'F',0,NULL)
	DECODE(
	    i_Auditable,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Auditable,
	-- *INF*: DECODE(i_CompositeRating,'T',1,'F',0,NULL)
	DECODE(
	    i_CompositeRating,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_CompositeRating,
	i_CompositeType AS o_CompositeType,
	-- *INF*: DECODE(i_IfAnyExposure,'T',1,'F',0,NULL)
	DECODE(
	    i_IfAnyExposure,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IfAnyExposure,
	-- *INF*: DECODE(i_RiskExcludeDeductible,'T',1,'F',0,NULL)
	DECODE(
	    i_RiskExcludeDeductible,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_RiskExcludeDeductible,
	-- *INF*: DECODE(i_SpecialCombinedMinimum,'T',1,'F',0,NULL)
	DECODE(
	    i_SpecialCombinedMinimum,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_SpecialCombinedMinimum,
	i_Type AS o_Type,
	-- *INF*: DECODE(i_UnderwriterOverride,'T',1,'F',0,NULL)
	DECODE(
	    i_UnderwriterOverride,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_UnderwriterOverride,
	i_UnitsDivider AS o_UnitsDivider,
	i_WithdrawalType AS o_WithdrawalType,
	i_Exposure AS o_Exposure,
	i_ExposureEstimated AS o_ExposureEstimated,
	i_ExposureAudited AS o_ExposureAudited,
	i_GL_LocationXmlId AS o_GL_LocationXmlId,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_GL_Risk
),
DCGLRiskStaging AS (
	TRUNCATE TABLE DCGLRiskStaging;
	INSERT INTO DCGLRiskStaging
	(LineId, GL_LocationId, GL_RiskId, SessionId, Id, Auditable, CompositeRating, CompositeType, IfAnyExposure, RiskExcludeDeductible, SpecialCombinedMinimum, Type, UnderwriterOverride, UnitsDivider, WithdrawalType, Exposure, ExposureEstimated, ExposureAudited, GL_LocationXmlId, ExtractDate, SourceSystemId, CompositeRatingID)
	SELECT 
	o_LineId AS LINEID, 
	o_GL_LocationId AS GL_LOCATIONID, 
	o_GL_RiskId AS GL_RISKID, 
	o_SessionId AS SESSIONID, 
	o_Id AS ID, 
	o_Auditable AS AUDITABLE, 
	o_CompositeRating AS COMPOSITERATING, 
	o_CompositeType AS COMPOSITETYPE, 
	o_IfAnyExposure AS IFANYEXPOSURE, 
	o_RiskExcludeDeductible AS RISKEXCLUDEDEDUCTIBLE, 
	o_SpecialCombinedMinimum AS SPECIALCOMBINEDMINIMUM, 
	o_Type AS TYPE, 
	o_UnderwriterOverride AS UNDERWRITEROVERRIDE, 
	o_UnitsDivider AS UNITSDIVIDER, 
	o_WithdrawalType AS WITHDRAWALTYPE, 
	o_Exposure AS EXPOSURE, 
	o_ExposureEstimated AS EXPOSUREESTIMATED, 
	o_ExposureAudited AS EXPOSUREAUDITED, 
	o_GL_LocationXmlId AS GL_LOCATIONXMLID, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	COMPOSITERATINGID
	FROM EXP_Metadata
),
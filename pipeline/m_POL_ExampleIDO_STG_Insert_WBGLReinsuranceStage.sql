WITH
SQ_WB_GL_Reinsurance AS (
	WITH cte_WBGLReinsurance(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.WB_CL_ReinsuranceId, 
	X.WB_GL_ReinsuranceId, 
	X.SessionId, 
	X.SpecialConditionsIncluded, 
	X.SpecialConditionsExcluded, 
	X.SpecialConditionsAnnotation, 
	X.GrossReinsurancePremiumOCPGLSBOP, 
	X.GrossReinsurancePremiumRR 
	FROM
	WB_GL_Reinsurance X
	inner join
	cte_WBGLReinsurance Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	WB_CL_ReinsuranceId,
	WB_GL_ReinsuranceId,
	SessionId,
	SpecialConditionsIncluded,
	SpecialConditionsExcluded,
	SpecialConditionsAnnotation,
	GrossReinsurancePremiumOCPGLSBOP,
	GrossReinsurancePremiumRR,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_GL_Reinsurance
),
WBGLReinsuranceStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBGLReinsuranceStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBGLReinsuranceStage
	(ExtractDate, SourceSystemId, WBCLReinsuranceId, WBGLReinsuranceId, SessionId, SpecialConditionsIncluded, SpecialConditionsExcluded, SpecialConditionsAnnotation, GrossReinsurancePremiumOCPGLSBOP, GrossReinsurancePremiumRR)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	WB_CL_ReinsuranceId AS WBCLREINSURANCEID, 
	WB_GL_ReinsuranceId AS WBGLREINSURANCEID, 
	SESSIONID, 
	SPECIALCONDITIONSINCLUDED, 
	SPECIALCONDITIONSEXCLUDED, 
	SPECIALCONDITIONSANNOTATION, 
	GROSSREINSURANCEPREMIUMOCPGLSBOP, 
	GROSSREINSURANCEPREMIUMRR
	FROM EXP_Metadata
),
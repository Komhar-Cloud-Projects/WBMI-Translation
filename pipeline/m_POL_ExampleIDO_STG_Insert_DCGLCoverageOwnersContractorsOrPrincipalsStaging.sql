WITH
SQ_DC_GL_CoverageOwnersContractorsOrPrincipals AS (
	WITH cte_DCGLCoverageOwnersContractorsOrPrincipals(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.GL_CoverageOwnersContractorsOrPrincipalsId, 
	X.SessionId, 
	X.ApplyARate, 
	X.Auditable, 
	X.DesignatedArchitect, 
	X.DesignatedConstructionManager, 
	X.DesignatedConstructionProject, 
	X.DesignatedOwner, 
	X.TypeOfBusiness, 
	X.UnderwriterOverride, 
	X.Exposure, 
	X.ExposureAudited, 
	X.ExposureEstimated 
	FROM
	DC_GL_CoverageOwnersContractorsOrPrincipals X
	inner join
	cte_DCGLCoverageOwnersContractorsOrPrincipals Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CoverageId,
	GL_CoverageOwnersContractorsOrPrincipalsId,
	SessionId,
	ApplyARate AS i_ApplyARate,
	-- *INF*: IIF(i_ApplyARate='T','1','0')
	IFF(i_ApplyARate = 'T', '1', '0') AS o_ApplyARate,
	Auditable AS i_Auditable,
	-- *INF*: IIF(i_Auditable='T','1','0')
	IFF(i_Auditable = 'T', '1', '0') AS o_Auditable,
	DesignatedArchitect,
	DesignatedConstructionManager,
	DesignatedConstructionProject,
	DesignatedOwner,
	TypeOfBusiness,
	UnderwriterOverride AS i_UnderwriterOverride,
	-- *INF*: IIF(i_UnderwriterOverride='T','1','0')
	IFF(i_UnderwriterOverride = 'T', '1', '0') AS o_UnderwriterOverride,
	Exposure,
	ExposureAudited,
	ExposureEstimated,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_GL_CoverageOwnersContractorsOrPrincipals
),
DCGLCoverageOwnersContractorsOrPrincipalsStaging AS (
	TRUNCATE TABLE DCGLCoverageOwnersContractorsOrPrincipalsStaging;
	INSERT INTO DCGLCoverageOwnersContractorsOrPrincipalsStaging
	(CoverageId, GL_CoverageOwnersContractorsOrPrincipalsId, SessionId, ApplyARate, Auditable, DesignatedArchitect, DesignatedConstructionManager, DesignatedConstructionProject, DesignatedOwner, TypeOfBusiness, UnderwriterOverride, Exposure, ExposureAudited, ExposureEstimated, ExtractDate, SourceSystemId)
	SELECT 
	COVERAGEID, 
	GL_COVERAGEOWNERSCONTRACTORSORPRINCIPALSID, 
	SESSIONID, 
	o_ApplyARate AS APPLYARATE, 
	o_Auditable AS AUDITABLE, 
	DESIGNATEDARCHITECT, 
	DESIGNATEDCONSTRUCTIONMANAGER, 
	DESIGNATEDCONSTRUCTIONPROJECT, 
	DESIGNATEDOWNER, 
	TYPEOFBUSINESS, 
	o_UnderwriterOverride AS UNDERWRITEROVERRIDE, 
	EXPOSURE, 
	EXPOSUREAUDITED, 
	EXPOSUREESTIMATED, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),
WITH
SQ_DCGLCoverageOwnersContractorsOrPrincipalsStaging AS (
	SELECT
		CoverageId,
		GL_CoverageOwnersContractorsOrPrincipalsId,
		SessionId,
		ApplyARate,
		Auditable,
		DesignatedArchitect,
		DesignatedConstructionManager,
		DesignatedConstructionProject,
		DesignatedOwner,
		TypeOfBusiness,
		UnderwriterOverride,
		Exposure,
		ExposureAudited,
		ExposureEstimated,
		ExtractDate,
		SourceSystemId
	FROM DCGLCoverageOwnersContractorsOrPrincipalsStaging
),
EXPTRANS AS (
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
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_DCGLCoverageOwnersContractorsOrPrincipalsStaging
),
archDCGLCoverageOwnersContractorsOrPrincipalsStaging AS (
	INSERT INTO archDCGLCoverageOwnersContractorsOrPrincipalsStaging
	(CoverageId, GL_CoverageOwnersContractorsOrPrincipalsId, SessionId, ApplyARate, Auditable, DesignatedArchitect, DesignatedConstructionManager, DesignatedConstructionProject, DesignatedOwner, TypeOfBusiness, UnderwriterOverride, Exposure, ExposureAudited, ExposureEstimated, ExtractDate, SourceSystemId, AuditId)
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
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID
	FROM EXPTRANS
),
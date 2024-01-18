WITH
SQ_DCCUUmbrellaGeneralLiabilityStaging AS (
	SELECT
		ExtractDate,
		SourceSystemId,
		LineId,
		CU_UmbrellaGeneralLiabilityId,
		SessionId,
		Id,
		CGL,
		Description,
		EffectiveDate,
		EmployeeBenefitLiability,
		ExpirationDate,
		LiquorLiability,
		OCP,
		PolicyNumber,
		PredominantClassTable,
		Rejected,
		TerrorismGL
	FROM DCCUUmbrellaGeneralLiabilityStaging4
),
EXP_Metadata AS (
	SELECT
	ExtractDate AS i_ExtractDate,
	SourceSystemId AS i_SourceSystemId,
	LineId AS i_LineId,
	CU_UmbrellaGeneralLiabilityId AS i_CU_UmbrellaGeneralLiabilityId,
	SessionId AS i_SessionId,
	Id AS i_Id,
	CGL AS i_CGL,
	Description AS i_Description,
	EffectiveDate AS i_EffectiveDate,
	EmployeeBenefitLiability AS i_EmployeeBenefitLiability,
	ExpirationDate AS i_ExpirationDate,
	LiquorLiability AS i_LiquorLiability,
	OCP AS i_OCP,
	PolicyNumber AS i_PolicyNumber,
	PredominantClassTable AS i_PredominantClassTable,
	Rejected AS i_Rejected,
	TerrorismGL AS i_TerrorismGL,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	i_ExtractDate AS o_ExtractDate,
	i_SourceSystemId AS o_SourceSystemId,
	i_LineId AS o_LineId,
	i_CU_UmbrellaGeneralLiabilityId AS o_CU_UmbrellaGeneralLiabilityId,
	i_SessionId AS o_SessionId,
	i_Id AS o_Id,
	-- *INF*: decode(i_CGL,'T',1,'F',0,NULL)
	decode(
	    i_CGL,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_CGL,
	i_Description AS o_Description,
	i_EffectiveDate AS o_EffectiveDate,
	-- *INF*: decode(i_EmployeeBenefitLiability,'T',1,'F',0,NULL)
	decode(
	    i_EmployeeBenefitLiability,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_EmployeeBenefitLiability,
	i_ExpirationDate AS o_ExpirationDate,
	-- *INF*: decode(i_LiquorLiability,'T',1,'F',0,NULL)
	decode(
	    i_LiquorLiability,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_LiquorLiability,
	-- *INF*: decode(i_OCP,'T',1,'F',0,NULL)
	decode(
	    i_OCP,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_OCP,
	i_PolicyNumber AS o_PolicyNumber,
	i_PredominantClassTable AS o_PredominantClassTable,
	-- *INF*: decode(i_Rejected,'T',1,'F',0,NULL)
	decode(
	    i_Rejected,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Rejected,
	-- *INF*: decode(i_TerrorismGL,'T',1,'F',0,NULL)
	decode(
	    i_TerrorismGL,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TerrorismGL
	FROM SQ_DCCUUmbrellaGeneralLiabilityStaging
),
ArchDCCUUmbrellaGeneralLiabilityStaging4 AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCUUmbrellaGeneralLiabilityStaging
	(ExtractDate, SourceSystemId, AuditId, LineId, CU_UmbrellaGeneralLiabilityId, SessionId, Id, CGL, Description, EffectiveDate, EmployeeBenefitLiability, ExpirationDate, LiquorLiability, OCP, PolicyNumber, PredominantClassTable, Rejected, TerrorismGL)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	o_LineId AS LINEID, 
	o_CU_UmbrellaGeneralLiabilityId AS CU_UMBRELLAGENERALLIABILITYID, 
	o_SessionId AS SESSIONID, 
	o_Id AS ID, 
	o_CGL AS CGL, 
	o_Description AS DESCRIPTION, 
	o_EffectiveDate AS EFFECTIVEDATE, 
	o_EmployeeBenefitLiability AS EMPLOYEEBENEFITLIABILITY, 
	o_ExpirationDate AS EXPIRATIONDATE, 
	o_LiquorLiability AS LIQUORLIABILITY, 
	o_OCP AS OCP, 
	o_PolicyNumber AS POLICYNUMBER, 
	o_PredominantClassTable AS PREDOMINANTCLASSTABLE, 
	o_Rejected AS REJECTED, 
	o_TerrorismGL AS TERRORISMGL
	FROM EXP_Metadata
),
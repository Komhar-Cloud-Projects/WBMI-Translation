WITH
SQ_DC_CU_UmbrellaGeneralLiability AS (
	WITH cte_DCCUUmbrellaGeneralLiability(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.CU_UmbrellaGeneralLiabilityId, 
	X.SessionId, 
	X.Id, 
	X.CGL, 
	X.Description, 
	X.EffectiveDate, 
	X.EmployeeBenefitLiability, 
	X.ExpirationDate, 
	X.LiquorLiability, 
	X.OCP, 
	X.PolicyNumber, 
	X.PredominantClassTable, 
	X.Rejected, 
	X.TerrorismGL 
	FROM
	DC_CU_UmbrellaGeneralLiability X
	inner join
	cte_DCCUUmbrellaGeneralLiability Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_handle AS (
	SELECT
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
	sysdate AS o_ExtracteDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemid,
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
	-- 
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
	-- *INF*: DECODE(i_TerrorismGL,'T',1,'F',0,NULL)
	DECODE(
	    i_TerrorismGL,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TerrorismGL
	FROM SQ_DC_CU_UmbrellaGeneralLiability
),
DCCUUmbrellaGeneralLiabilityStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCUUmbrellaGeneralLiabilityStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCUUmbrellaGeneralLiabilityStaging
	(ExtractDate, SourceSystemId, LineId, CU_UmbrellaGeneralLiabilityId, SessionId, Id, CGL, Description, EffectiveDate, EmployeeBenefitLiability, ExpirationDate, LiquorLiability, OCP, PolicyNumber, PredominantClassTable, Rejected, TerrorismGL)
	SELECT 
	o_ExtracteDate AS EXTRACTDATE, 
	o_SourceSystemid AS SOURCESYSTEMID, 
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
	FROM EXP_handle
),
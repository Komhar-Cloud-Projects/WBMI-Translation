WITH
SQ_to_WB_CA_Policy AS (
	WITH cte_WBCAPolicy(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.WB_CL_PolicyId, 
	X.WB_CA_PolicyId, 
	X.SessionId, 
	X.TaskFlagCAFormSelectedWB1409, 
	X.TaskFlagHistoricVehicleRegistration, 
	X.TaskFlagCAFormSelectedWB1525, 
	X.TaskFlagCAFormSelectedWB1396, 
	X.TaskFlagCAOTCCoverageOnAntiqueAuto, 
	X.TaskFlagCADriverFinancialResponsibility, 
	X.TaskFlagCADriverLicenseNumber, 
	X.PlusPakAuto, 
	X.PlusPakGarage, 
	X.ReinsuranceLiabilityLimit, 
	X.ReinsuranceLiabilityPremium, 
	X.ReinsurancePremiumMessage, 
	X.ReinsuranceIndicatorMessage 
	FROM
	WB_CA_Policy X
	inner join
	cte_WBCAPolicy Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_handle AS (
	SELECT
	WB_CL_PolicyId AS i_WB_CL_PolicyId,
	WB_CA_PolicyId AS i_WB_CA_PolicyId,
	SessionId AS i_SessionId,
	TaskFlagCAFormSelectedWB1409 AS i_TaskFlagCAFormSelectedWB1409,
	TaskFlagHistoricVehicleRegistration AS i_TaskFlagHistoricVehicleRegistration,
	TaskFlagCAFormSelectedWB1525 AS i_TaskFlagCAFormSelectedWB1525,
	TaskFlagCAFormSelectedWB1396 AS i_TaskFlagCAFormSelectedWB1396,
	TaskFlagCAOTCCoverageOnAntiqueAuto AS i_TaskFlagCAOTCCoverageOnAntiqueAuto,
	TaskFlagCADriverFinancialResponsibility AS i_TaskFlagCADriverFinancialResponsibility,
	TaskFlagCADriverLicenseNumber AS i_TaskFlagCADriverLicenseNumber,
	PlusPakAuto AS i_PlusPakAuto,
	PlusPakGarage AS i_PlusPakGarage,
	ReinsuranceLiabilityLimit AS i_ReinsuranceLiabilityLimit,
	ReinsuranceLiabilityPremium AS i_ReinsuranceLiabilityPremium,
	ReinsurancePremiumMessage AS i_ReinsurancePremiumMessage,
	ReinsuranceIndicatorMessage AS i_ReinsuranceIndicatorMessage,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemid,
	i_WB_CL_PolicyId AS o_WB_CL_PolicyId,
	i_WB_CA_PolicyId AS o_WB_CA_PolicyId,
	i_SessionId AS o_SessionId,
	-- *INF*: DECODE(i_PlusPakAuto,'T',1,'F',0,NULL)
	DECODE(
	    i_PlusPakAuto,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PlusPakAuto,
	-- *INF*: decode(i_PlusPakGarage,'T',1,'F',0,NULL)
	decode(
	    i_PlusPakGarage,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PlusPakGarage,
	i_ReinsuranceLiabilityLimit AS o_ReinsuranceLiabilityLimit,
	i_ReinsuranceLiabilityPremium AS o_ReinsuranceLiabilityPremium,
	i_ReinsurancePremiumMessage AS o_ReinsurancePremiumMessage,
	i_ReinsuranceIndicatorMessage AS o_ReinsuranceIndicatorMessage,
	-- *INF*: decode(i_TaskFlagCAFormSelectedWB1409,'T',1,'F',0,NULL)
	decode(
	    i_TaskFlagCAFormSelectedWB1409,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TaskFlagCAFormSelectedWB1409,
	-- *INF*: decode(i_TaskFlagHistoricVehicleRegistration,'T',1,'F',0,NULL)
	decode(
	    i_TaskFlagHistoricVehicleRegistration,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TaskFlagHistoricVehicleRegistration,
	-- *INF*: decode(i_TaskFlagCAFormSelectedWB1525,'T',1,'F',0,NULL)
	decode(
	    i_TaskFlagCAFormSelectedWB1525,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TaskFlagCAFormSelectedWB1525,
	-- *INF*: decode(i_TaskFlagCAFormSelectedWB1396,'T',1,'F',0,NULL)
	decode(
	    i_TaskFlagCAFormSelectedWB1396,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TaskFlagCAFormSelectedWB1396,
	-- *INF*: decode(i_TaskFlagCAOTCCoverageOnAntiqueAuto,'T',1,'F',NULL)
	decode(
	    i_TaskFlagCAOTCCoverageOnAntiqueAuto,
	    'T', 1,
	    'F', NULL
	) AS o_TaskFlagCAOTCCoverageOnAntiqueAuto,
	-- *INF*: decode(i_TaskFlagCADriverFinancialResponsibility,'T',1,'F',0,NULL)
	decode(
	    i_TaskFlagCADriverFinancialResponsibility,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TaskFlagCADriverFinancialResponsibility,
	-- *INF*: decode(i_TaskFlagCADriverLicenseNumber,'T',1,'F',0,NULL)
	decode(
	    i_TaskFlagCADriverLicenseNumber,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TaskFlagCADriverLicenseNumber
	FROM SQ_to_WB_CA_Policy
),
WBCAPolicyStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCAPolicyStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCAPolicyStaging
	(ExtractDate, SourceSystemId, WB_CL_PolicyId, WB_CA_PolicyId, SessionId, TaskFlagCAFormSelectedWB1409, TaskFlagHistoricVehicleRegistration, TaskFlagCAFormSelectedWB1525, TaskFlagCAFormSelectedWB1396, TaskFlagCAOTCCoverageOnAntiqueAuto, TaskFlagCADriverFinancialResponsibility, TaskFlagCADriverLicenseNumber, PlusPakAuto, PlusPakGarage, ReinsuranceLiabilityLimit, ReinsuranceLiabilityPremium, ReinsurancePremiumMessage, ReinsuranceIndicatorMessage)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemid AS SOURCESYSTEMID, 
	o_WB_CL_PolicyId AS WB_CL_POLICYID, 
	o_WB_CA_PolicyId AS WB_CA_POLICYID, 
	o_SessionId AS SESSIONID, 
	o_TaskFlagCAFormSelectedWB1409 AS TASKFLAGCAFORMSELECTEDWB1409, 
	o_TaskFlagHistoricVehicleRegistration AS TASKFLAGHISTORICVEHICLEREGISTRATION, 
	o_TaskFlagCAFormSelectedWB1525 AS TASKFLAGCAFORMSELECTEDWB1525, 
	o_TaskFlagCAFormSelectedWB1396 AS TASKFLAGCAFORMSELECTEDWB1396, 
	o_TaskFlagCAOTCCoverageOnAntiqueAuto AS TASKFLAGCAOTCCOVERAGEONANTIQUEAUTO, 
	o_TaskFlagCADriverFinancialResponsibility AS TASKFLAGCADRIVERFINANCIALRESPONSIBILITY, 
	o_TaskFlagCADriverLicenseNumber AS TASKFLAGCADRIVERLICENSENUMBER, 
	o_PlusPakAuto AS PLUSPAKAUTO, 
	o_PlusPakGarage AS PLUSPAKGARAGE, 
	o_ReinsuranceLiabilityLimit AS REINSURANCELIABILITYLIMIT, 
	o_ReinsuranceLiabilityPremium AS REINSURANCELIABILITYPREMIUM, 
	o_ReinsurancePremiumMessage AS REINSURANCEPREMIUMMESSAGE, 
	o_ReinsuranceIndicatorMessage AS REINSURANCEINDICATORMESSAGE
	FROM EXP_handle
),
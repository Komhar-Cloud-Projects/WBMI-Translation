WITH
SQ_WBCAPolicyStaging AS (
	SELECT
		ExtractDate,
		SourceSystemId,
		WB_CL_PolicyId,
		WB_CA_PolicyId,
		SessionId,
		PlusPakAuto,
		PlusPakGarage,
		ReinsuranceLiabilityLimit,
		ReinsuranceLiabilityPremium,
		ReinsurancePremiumMessage,
		ReinsuranceIndicatorMessage,
		TaskFlagCAFormSelectedWB1409,
		TaskFlagHistoricVehicleRegistration,
		TaskFlagCAFormSelectedWB1525,
		TaskFlagCAFormSelectedWB1396,
		TaskFlagCAOTCCoverageOnAntiqueAuto,
		TaskFlagCADriverFinancialResponsibility,
		TaskFlagCADriverLicenseNumber
	FROM WBCAPolicyStaging3
),
EXP_Metadata AS (
	SELECT
	ExtractDate AS i_ExtractDate,
	SourceSystemId AS i_SourceSystemId,
	WB_CL_PolicyId AS i_WB_CL_PolicyId,
	WB_CA_PolicyId AS i_WB_CA_PolicyId,
	SessionId AS i_SessionId,
	PlusPakAuto AS i_PlusPakAuto,
	PlusPakGarage AS i_PlusPakGarage,
	ReinsuranceLiabilityLimit AS i_ReinsuranceLiabilityLimit,
	ReinsuranceLiabilityPremium AS i_ReinsuranceLiabilityPremium,
	ReinsurancePremiumMessage AS i_ReinsurancePremiumMessage,
	ReinsuranceIndicatorMessage AS i_ReinsuranceIndicatorMessage,
	TaskFlagCAFormSelectedWB1409 AS i_TaskFlagCAFormSelectedWB1409,
	TaskFlagHistoricVehicleRegistration AS i_TaskFlagHistoricVehicleRegistration,
	TaskFlagCAFormSelectedWB1525 AS i_TaskFlagCAFormSelectedWB1525,
	TaskFlagCAFormSelectedWB1396 AS i_TaskFlagCAFormSelectedWB1396,
	TaskFlagCAOTCCoverageOnAntiqueAuto AS i_TaskFlagCAOTCCoverageOnAntiqueAuto,
	TaskFlagCADriverFinancialResponsibility AS i_TaskFlagCADriverFinancialResponsibility,
	TaskFlagCADriverLicenseNumber AS i_TaskFlagCADriverLicenseNumber,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	i_ExtractDate AS o_ExtractDate,
	i_SourceSystemId AS o_SourceSystemId,
	i_WB_CL_PolicyId AS o_WB_CL_PolicyId,
	i_WB_CA_PolicyId AS o_WB_CA_PolicyId,
	i_SessionId AS o_SessionId,
	-- *INF*: decode(i_PlusPakAuto,'T',1,'F',0,NULL)
	decode(
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
	i_PolicyId AS o_PolicyId,
	-- *INF*: decode(i_TaskFlagCAFormSelectedWB1409,'T',1,'F',0,NULL)
	decode(
	    i_TaskFlagCAFormSelectedWB1409,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TaskFlagCAFormSelectedWB1409,
	-- *INF*: decode(i_IsSymbol10Selected,'T',1,'F',0,NULL)
	decode(
	    i_IsSymbol10Selected,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IsSymbol10Selected,
	-- *INF*: decode(i_TaskFlagHistoricVehicleRegistration,'T',1,'F',0,NULL)
	decode(
	    i_TaskFlagHistoricVehicleRegistration,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TaskFlagHistoricVehicleRegistration,
	-- *INF*: DECODE(i_TaskFlagCAFormSelectedWB1525,'T',1,'F',0,NULL)
	DECODE(
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
	-- *INF*: decode(i_TaskFlagCAOTCCoverageOnAntiqueAuto,'T',1,'F',0,NULL)
	decode(
	    i_TaskFlagCAOTCCoverageOnAntiqueAuto,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TaskFlagCAOTCCoverageOnAntiqueAuto,
	-- *INF*: decode(i_TaskFlagCADriverFinancialResponsibility,'T',1,'F',0,NULL)
	decode(
	    i_TaskFlagCADriverFinancialResponsibility,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TaskFlagCADriverFinancialResponsibility,
	-- *INF*: decode(i_TaskFlagCADriverLicenseNumber,'T',1,'F',0,null)
	decode(
	    i_TaskFlagCADriverLicenseNumber,
	    'T', 1,
	    'F', 0,
	    null
	) AS o_TaskFlagCADriverLicenseNumber
	FROM SQ_WBCAPolicyStaging
),
SEQTRANS AS (
	CREATE SEQUENCE SEQTRANS
	START = 0
	INCREMENT = 1;
),
ArchWBCAPolicyStaging3 AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBCAPolicyStaging
	(ArchWBCAPolicyStagingId, ExtractDate, SourceSystemId, AuditId, WB_CL_PolicyId, WB_CA_PolicyId, SessionId, TaskFlagCAFormSelectedWB1409, TaskFlagHistoricVehicleRegistration, TaskFlagCAFormSelectedWB1525, TaskFlagCAFormSelectedWB1396, TaskFlagCAOTCCoverageOnAntiqueAuto, TaskFlagCADriverFinancialResponsibility, TaskFlagCADriverLicenseNumber, PlusPakAuto, PlusPakGarage, ReinsuranceLiabilityLimit, ReinsuranceLiabilityPremium, ReinsurancePremiumMessage, ReinsuranceIndicatorMessage)
	SELECT 
	SEQTRANS.NEXTVAL AS ARCHWBCAPOLICYSTAGINGID, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
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
	FROM EXP_Metadata
),
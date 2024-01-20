WITH
SQ_WBCARiskStaging AS (
	SELECT
		WBCARiskStagingId,
		ExtractDate,
		SourceSystemId,
		CA_RiskId,
		WB_CA_RiskId,
		SessionId,
		PurePremium,
		ApplyChargeTransportClients,
		ExtendedEmployeeCovMessage,
		SecondLevelCoverage,
		HaulingOperations,
		CustomEquipment,
		CustomEquipmentCost,
		TotalVehicleCost,
		CoverageType,
		RentalReimbursementSoftMessage,
		LoanLeaseSotMsg,
		SubjectToNoFaultMessage,
		GVWUC,
		DriverTrainingIndicator,
		LayUpCredit,
		LayUpCreditDays,
		NewVehicleIndicator,
		VINHasChangedIndicator
	FROM WBCARiskStaging
),
EXP_handle AS (
	SELECT
	WBCARiskStagingId,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	CA_RiskId,
	WB_CA_RiskId,
	SessionId,
	PurePremium,
	-- *INF*: DECODE(LimitedLiability,'T',1,'F',0,NULL)
	DECODE(
	    LimitedLiability,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS LimitedLiability_out,
	-- *INF*: DECODE(LossPayee,'T',1,'F',0,NULL)
	DECODE(
	    LossPayee,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS LossPayee_out,
	-- *INF*: DECODE(ActualLossSustained,'T',1,'F',0,NULL)
	DECODE(
	    ActualLossSustained,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS ActualLossSustained_out,
	ApplyChargeTransportClients,
	-- *INF*: DECODE(ApplyChargeTransportClients,'T',1,'F',0,NULL)
	DECODE(
	    ApplyChargeTransportClients,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS ApplyChargeTransportClients_out,
	ExtendedEmployeeCovMessage,
	SecondLevelCoverage,
	-- *INF*: DECODE(SecondLevelCoverage,'T',1,'F',0,NULL)
	DECODE(
	    SecondLevelCoverage,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS SecondLevelCoverage_out,
	HaulingOperations,
	CustomEquipment,
	CustomEquipmentCost,
	TotalVehicleCost,
	CoverageType,
	RentalReimbursementSoftMessage,
	LoanLeaseSotMsg,
	SubjectToNoFaultMessage,
	GVWUC,
	DriverTrainingIndicator,
	-- *INF*: DECODE(DriverTrainingIndicator,'T',1,'F',0,NULL)
	DECODE(
	    DriverTrainingIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS DriverTrainingIndicator_out,
	LayUpCredit,
	-- *INF*: DECODE(LayUpCredit,'T',1,'F',0,NULL)
	DECODE(
	    LayUpCredit,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS LayUpCredit_out,
	LayUpCreditDays,
	NewVehicleIndicator,
	-- *INF*: DECODE(NewVehicleIndicator,'T',1,'F',0,NULL)
	DECODE(
	    NewVehicleIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS NewVehicleIndicator_out,
	VINHasChangedIndicator,
	-- *INF*: DECODE(VINHasChangedIndicator,'T',1,'F',0,NULL)
	DECODE(
	    VINHasChangedIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS VINHasChangedIndicator_out
	FROM SQ_WBCARiskStaging
),
archWBCARiskStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.archWBCARiskStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archWBCARiskStaging
	(ExtractDate, SourceSystemId, AuditId, WBCARiskStagingId, CA_RiskId, WB_CA_RiskId, SessionId, PurePremium, ApplyChargeTransportClients, ExtendedEmployeeCovMessage, SecondLevelCoverage, HaulingOperations, CustomEquipment, CustomEquipmentCost, TotalVehicleCost, CoverageType, RentalReimbursementSoftMessage, LoanLeaseSotMsg, SubjectToNoFaultMessage, GVWUC, DriverTrainingIndicator, LayUpCredit, LayUpCreditDays, NewVehicleIndicator, VINHasChangedIndicator)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID, 
	WBCARISKSTAGINGID, 
	CA_RISKID, 
	WB_CA_RISKID, 
	SESSIONID, 
	PUREPREMIUM, 
	ApplyChargeTransportClients_out AS APPLYCHARGETRANSPORTCLIENTS, 
	EXTENDEDEMPLOYEECOVMESSAGE, 
	SecondLevelCoverage_out AS SECONDLEVELCOVERAGE, 
	HAULINGOPERATIONS, 
	CUSTOMEQUIPMENT, 
	CUSTOMEQUIPMENTCOST, 
	TOTALVEHICLECOST, 
	COVERAGETYPE, 
	RENTALREIMBURSEMENTSOFTMESSAGE, 
	LOANLEASESOTMSG, 
	SUBJECTTONOFAULTMESSAGE, 
	GVWUC, 
	DriverTrainingIndicator_out AS DRIVERTRAININGINDICATOR, 
	LayUpCredit_out AS LAYUPCREDIT, 
	LAYUPCREDITDAYS, 
	NewVehicleIndicator_out AS NEWVEHICLEINDICATOR, 
	VINHasChangedIndicator_out AS VINHASCHANGEDINDICATOR
	FROM EXP_handle
),
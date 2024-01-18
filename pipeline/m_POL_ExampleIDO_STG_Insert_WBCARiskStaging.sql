WITH
SQ_WB_CA_Risk AS (
	WITH cte_WBCARisk(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CA_RiskId, 
	X.WB_CA_RiskId, 
	X.SessionId, 
	X.PurePremium, 
	X.ApplyChargeTransportClients, 
	X.ExtendedEmployeeCovMessage, 
	X.SecondLevelCoverage, 
	X.HaulingOperations, 
	X.CustomEquipment, 
	X.CustomEquipmentCost, 
	X.TotalVehicleCost, 
	X.CoverageType, 
	X.RentalReimbursementSoftMessage, 
	X.LoanLeaseSotMsg, 
	X.SubjectToNoFaultMessage, 
	X.GVWUC, 
	X.DriverTrainingIndicator, 
	X.LayUpCredit, 
	X.LayUpCreditDays, 
	X.NewVehicleIndicator, 
	X.VINHasChangedIndicator 
	FROM
	WB_CA_Risk X
	inner join
	cte_WBCARisk Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CA_RiskId,
	WB_CA_RiskId,
	SessionId,
	PurePremium,
	-- *INF*: decode(LimitedLiability,'T',1,'F',0,NULL)
	decode(
	    LimitedLiability,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS LimitedLiability_out,
	-- *INF*: decode(LossPayee,'T',1,'F',0,NULL)
	decode(
	    LossPayee,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS LossPayee_out,
	-- *INF*: decode(ActualLossSustained,'T',1,'F',0,NULL)
	decode(
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
	-- *INF*: decode(DriverTrainingIndicator,'T',1,'F',0,NULL)
	decode(
	    DriverTrainingIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS DriverTrainingIndicator_out,
	LayUpCredit,
	-- *INF*: decode(LayUpCredit,'T',1,'F',0,NULL)
	decode(
	    LayUpCredit,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS LayUpCredit_out,
	LayUpCreditDays,
	NewVehicleIndicator,
	-- *INF*: decode(NewVehicleIndicator,'T',1,'F',0,NULL)
	decode(
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
	FROM SQ_WB_CA_Risk
),
WBCARiskStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCARiskStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCARiskStaging
	(ExtractDate, SourceSystemId, CA_RiskId, WB_CA_RiskId, SessionId, PurePremium, ApplyChargeTransportClients, ExtendedEmployeeCovMessage, SecondLevelCoverage, HaulingOperations, CustomEquipment, CustomEquipmentCost, TotalVehicleCost, CoverageType, RentalReimbursementSoftMessage, LoanLeaseSotMsg, SubjectToNoFaultMessage, GVWUC, DriverTrainingIndicator, LayUpCredit, LayUpCreditDays, NewVehicleIndicator, VINHasChangedIndicator)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
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
	FROM EXP_Metadata
),
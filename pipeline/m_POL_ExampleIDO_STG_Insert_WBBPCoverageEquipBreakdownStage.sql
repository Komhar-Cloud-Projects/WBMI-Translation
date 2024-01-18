WITH
SQ_WB_BP_CoverageEquipBreakdown AS (
	WITH cte_WBBPCoverageEquipBreakdown(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.WB_BP_CoverageEquipBreakdownId, 
	X.SessionId, 
	X.CovEquipmentBreakdownIndicator, 
	X.PrintingPresses, 
	X.SheetFedFiveColorAgeOfOldestPress, 
	X.SheetFedFiveColorCaption, 
	X.SheetFedFiveColorNumberOfPresses, 
	X.SheetFedFourColorAgeOfOldestPress, 
	X.SheetFedFourColorCaption, 
	X.SheetFedFourColorNumberOfPresses, 
	X.SheetFedMoreThanFiveColorAgeOfOldestPress, 
	X.SheetFedMoreThanFiveColorCaption, 
	X.SheetFedMoreThanFiveColorNumberOfPresses, 
	X.SheetFedThreeColorAgeOfOldestPress, 
	X.SheetFedThreeColorCaption, 
	X.SheetFedThreeColorNumberOfPresses, 
	X.SheetFedTwoColorAgeOfOldestPress, 
	X.SheetFedTwoColorCaption, 
	X.SheetFedTwoColorNumberOfPresses, 
	X.SheetFedOneColorAgeOfOldestPress, 
	X.SheetFedOneColorCaption, 
	X.SheetFedOneColorNumberOfPresses, 
	X.WebFedAgeOfOldestPress, 
	X.WebFedCaption, 
	X.WebFedNumberOfPresses, 
	X.TypeOfPress, 
	X.NumberOfPresses, 
	X.AgeOfOldestPressInYears, 
	X.MedicalEquipment, 
	X.MinimumDeductibleDirectCoveragesMedical, 
	X.MinimumDeductibleIndirectCoveragesMedical, 
	X.MedicalEquipmentGreaterThanOneMillion, 
	X.EquipBreakdownSetPrintingProcessValue, 
	X.BillingLOB, 
	X.CommissionPlanID, 
	X.IsBillingSubline, 
	X.ParentBillingLOB, 
	X.PurePremium, 
	X.TransactionCommissionType, 
	X.TransactionCommissionValue, 
	X.TransactionFinalCommissionValue 
	FROM
	WB_BP_CoverageEquipBreakdown X
	inner join
	cte_WBBPCoverageEquipBreakdown Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CoverageId,
	WB_BP_CoverageEquipBreakdownId,
	SessionId,
	CovEquipmentBreakdownIndicator,
	-- *INF*: DECODE(CovEquipmentBreakdownIndicator,'T',1,'F',0,NULL)
	DECODE(
	    CovEquipmentBreakdownIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_CovEquipmentBreakdownIndicator,
	PrintingPresses,
	-- *INF*: DECODE(PrintingPresses,'T',1,'F',0,NULL)
	DECODE(
	    PrintingPresses,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PrintingPresses,
	SheetFedFiveColorAgeOfOldestPress,
	SheetFedFiveColorCaption,
	SheetFedFiveColorNumberOfPresses,
	SheetFedFourColorAgeOfOldestPress,
	SheetFedFourColorCaption,
	SheetFedFourColorNumberOfPresses,
	SheetFedMoreThanFiveColorAgeOfOldestPress,
	SheetFedMoreThanFiveColorCaption,
	SheetFedMoreThanFiveColorNumberOfPresses,
	SheetFedThreeColorAgeOfOldestPress,
	SheetFedThreeColorCaption,
	SheetFedThreeColorNumberOfPresses,
	SheetFedTwoColorAgeOfOldestPress,
	SheetFedTwoColorCaption,
	SheetFedTwoColorNumberOfPresses,
	SheetFedOneColorAgeOfOldestPress,
	SheetFedOneColorCaption,
	SheetFedOneColorNumberOfPresses,
	WebFedAgeOfOldestPress,
	WebFedCaption,
	WebFedNumberOfPresses,
	TypeOfPress,
	NumberOfPresses,
	AgeOfOldestPressInYears,
	MedicalEquipment,
	-- *INF*: DECODE(MedicalEquipment,'T',1,'F',0,NULL)
	DECODE(
	    MedicalEquipment,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_MedicalEquipment,
	MinimumDeductibleDirectCoveragesMedical,
	MinimumDeductibleIndirectCoveragesMedical,
	MedicalEquipmentGreaterThanOneMillion,
	-- *INF*: DECODE(MedicalEquipmentGreaterThanOneMillion,'T',1,'F',0,NULL)
	DECODE(
	    MedicalEquipmentGreaterThanOneMillion,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_MedicalEquipmentGreaterThanOneMillion,
	EquipBreakdownSetPrintingProcessValue,
	-- *INF*: DECODE(EquipBreakdownSetPrintingProcessValue,'T',1,'F',0,NULL)
	DECODE(
	    EquipBreakdownSetPrintingProcessValue,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_EquipBreakdownSetPrintingProcessValue,
	BillingLOB,
	CommissionPlanID,
	IsBillingSubline,
	-- *INF*: DECODE(IsBillingSubline,'T',1,'F',0,NULL)
	DECODE(
	    IsBillingSubline,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IsBillingSubline,
	ParentBillingLOB,
	PurePremium,
	TransactionCommissionType,
	TransactionCommissionValue,
	TransactionFinalCommissionValue
	FROM SQ_WB_BP_CoverageEquipBreakdown
),
WBBPCoverageEquipBreakdownStage AS (
	TRUNCATE TABLE WBBPCoverageEquipBreakdownStage;
	INSERT INTO WBBPCoverageEquipBreakdownStage
	(ExtractDate, SourceSystemId, CoverageId, WB_BP_CoverageEquipBreakdownId, SessionId, CovEquipmentBreakdownIndicator, PrintingPresses, SheetFedFiveColorAgeOfOldestPress, SheetFedFiveColorCaption, SheetFedFiveColorNumberOfPresses, SheetFedFourColorAgeOfOldestPress, SheetFedFourColorCaption, SheetFedFourColorNumberOfPresses, SheetFedMoreThanFiveColorAgeOfOldestPress, SheetFedMoreThanFiveColorCaption, SheetFedMoreThanFiveColorNumberOfPresses, SheetFedThreeColorAgeOfOldestPress, SheetFedThreeColorCaption, SheetFedThreeColorNumberOfPresses, SheetFedTwoColorAgeOfOldestPress, SheetFedTwoColorCaption, SheetFedTwoColorNumberOfPresses, SheetFedOneColorAgeOfOldestPress, SheetFedOneColorCaption, SheetFedOneColorNumberOfPresses, WebFedAgeOfOldestPress, WebFedCaption, WebFedNumberOfPresses, TypeOfPress, NumberOfPresses, AgeOfOldestPressInYears, MedicalEquipment, MinimumDeductibleDirectCoveragesMedical, MinimumDeductibleIndirectCoveragesMedical, MedicalEquipmentGreaterThanOneMillion, EquipBreakdownSetPrintingProcessValue, BillingLOB, CommissionPlanID, IsBillingSubline, ParentBillingLOB, PurePremium, TransactionCommissionType, TransactionCommissionValue, TransactionFinalCommissionValue)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	COVERAGEID, 
	WB_BP_COVERAGEEQUIPBREAKDOWNID, 
	SESSIONID, 
	o_CovEquipmentBreakdownIndicator AS COVEQUIPMENTBREAKDOWNINDICATOR, 
	o_PrintingPresses AS PRINTINGPRESSES, 
	SHEETFEDFIVECOLORAGEOFOLDESTPRESS, 
	SHEETFEDFIVECOLORCAPTION, 
	SHEETFEDFIVECOLORNUMBEROFPRESSES, 
	SHEETFEDFOURCOLORAGEOFOLDESTPRESS, 
	SHEETFEDFOURCOLORCAPTION, 
	SHEETFEDFOURCOLORNUMBEROFPRESSES, 
	SHEETFEDMORETHANFIVECOLORAGEOFOLDESTPRESS, 
	SHEETFEDMORETHANFIVECOLORCAPTION, 
	SHEETFEDMORETHANFIVECOLORNUMBEROFPRESSES, 
	SHEETFEDTHREECOLORAGEOFOLDESTPRESS, 
	SHEETFEDTHREECOLORCAPTION, 
	SHEETFEDTHREECOLORNUMBEROFPRESSES, 
	SHEETFEDTWOCOLORAGEOFOLDESTPRESS, 
	SHEETFEDTWOCOLORCAPTION, 
	SHEETFEDTWOCOLORNUMBEROFPRESSES, 
	SHEETFEDONECOLORAGEOFOLDESTPRESS, 
	SHEETFEDONECOLORCAPTION, 
	SHEETFEDONECOLORNUMBEROFPRESSES, 
	WEBFEDAGEOFOLDESTPRESS, 
	WEBFEDCAPTION, 
	WEBFEDNUMBEROFPRESSES, 
	TYPEOFPRESS, 
	NUMBEROFPRESSES, 
	AGEOFOLDESTPRESSINYEARS, 
	o_MedicalEquipment AS MEDICALEQUIPMENT, 
	MINIMUMDEDUCTIBLEDIRECTCOVERAGESMEDICAL, 
	MINIMUMDEDUCTIBLEINDIRECTCOVERAGESMEDICAL, 
	o_MedicalEquipmentGreaterThanOneMillion AS MEDICALEQUIPMENTGREATERTHANONEMILLION, 
	o_EquipBreakdownSetPrintingProcessValue AS EQUIPBREAKDOWNSETPRINTINGPROCESSVALUE, 
	BILLINGLOB, 
	COMMISSIONPLANID, 
	o_IsBillingSubline AS ISBILLINGSUBLINE, 
	PARENTBILLINGLOB, 
	PUREPREMIUM, 
	TRANSACTIONCOMMISSIONTYPE, 
	TRANSACTIONCOMMISSIONVALUE, 
	TRANSACTIONFINALCOMMISSIONVALUE
	FROM EXP_Metadata
),
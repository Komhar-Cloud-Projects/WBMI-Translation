WITH
SQ_DC_CA_Risk AS (
	WITH cte_DCCARisk(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CA_StateId, 
	X.CA_LocationId, 
	X.CA_RiskId, 
	X.SessionId, 
	X.Id, 
	X.Deleted, 
	X.Type, 
	X.RiskType, 
	X.Description, 
	X.AssignedDriverAge, 
	X.Auditable, 
	X.AutoInterests_Coll_Consignment, 
	X.AutoInterests_Coll_CrNameLossPayee, 
	X.AutoInterests_Coll_FinancedAutos, 
	X.AutoInterests_Coll_NewAutos, 
	X.AutoInterests_Coll_OwnAutos, 
	X.AutoInterests_Coll_UsedAutos, 
	X.AutoInterests_OTC_Consignment, 
	X.AutoInterests_OTC_CrNameLossPayee, 
	X.AutoInterests_OTC_FinancedAutos, 
	X.AutoInterests_OTC_NewAutos, 
	X.AutoInterests_OTC_OwnAutos, 
	X.AutoInterests_OTC_UsedAutos, 
	X.AuxiliaryLightingSystem, 
	X.AuxiliaryRunningLampsDiscount, 
	X.CA0198, 
	X.CA0199, 
	X.CA04180604, 
	X.CA2090, 
	X.CA2352, 
	X.CA2355, 
	X.DayCareCenterVehicle, 
	X.Emergency, 
	X.EmployeeTransport, 
	X.EmployerFurnished, 
	X.ExperienceRatingBasicLimitPremiumPhysicalDamageGarageSum, 
	X.ExperienceRatingBasicLimitPremiumPhysicalDamageSum, 
	X.FareCharged, 
	X.FireDepartments, 
	X.FleetIndicator, 
	X.FleetIndicatorPrivatePassenger, 
	X.FullCoverageGlass, 
	X.FullCoverageGlassColl, 
	X.GolfCartType, 
	X.GolfMobileCommercialUse, 
	X.HistoricVehicle, 
	X.IndividuallyOwned, 
	X.IndividualOrMarried, 
	X.InterpolicyStacking, 
	X.IsFareCharged, 
	X.LawEnforcement, 
	X.LoanLease, 
	X.MCCASurcharge, 
	X.Mileage, 
	X.MileageEstimatePublic, 
	X.MotorscooterIndicator, 
	X.MSSeatingCapacity, 
	X.NumberOfDaysLeased, 
	X.NumberOfPlates, 
	X.NumberOfRegPlates, 
	X.NYMotorVehLawEnforcementFee, 
	X.OneRoundTripPerDay, 
	X.PassiveRestraintDiscount, 
	X.PropertyProtectionUnits, 
	X.RatingBasis, 
	X.RegistrationPlates, 
	X.RegistrationPlatesDriveAwayContractors, 
	X.RegistrationState, 
	X.RentalReimbursement, 
	X.SchoolTermBegins, 
	X.SchoolTermEnds, 
	X.SoundReceivingCoverageType, 
	X.SpecialEquipmentTerritory, 
	X.Stacked, 
	X.StackedUIM, 
	X.SubjectToCompulsoryLaw, 
	X.SubjectToNoFault, 
	X.TotalAnnualPayroll, 
	X.TotalAnnualPayrollAnnual, 
	X.TotalAnnualPayrollEstimate, 
	X.TrailerCollision, 
	X.TrailerComprehensive, 
	X.TrailerPhysicalDamageExclusion, 
	X.TrailerSpecifiedCauses, 
	X.TransOfEmp, 
	X.TruckingUse, 
	X.UsedAsShowroom, 
	X.UsedWithLightTrucks, 
	X.VehicleNumber, 
	X.WAAutoLoanCoverage, 
	X.CA_LocationXmlId, 
	X.CA_RegistrantXmlId 
	FROM
	DC_CA_Risk X
	inner join
	cte_DCCARisk Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CA_StateId,
	CA_LocationId,
	CA_RiskId,
	SessionId,
	Id,
	Deleted,
	Type,
	RiskType,
	Description,
	AssignedDriverAge,
	Auditable,
	AutoInterests_Coll_Consignment,
	AutoInterests_Coll_CrNameLossPayee,
	AutoInterests_Coll_FinancedAutos,
	AutoInterests_Coll_NewAutos,
	AutoInterests_Coll_OwnAutos,
	AutoInterests_Coll_UsedAutos,
	AutoInterests_OTC_Consignment,
	AutoInterests_OTC_CrNameLossPayee,
	AutoInterests_OTC_FinancedAutos,
	AutoInterests_OTC_NewAutos,
	AutoInterests_OTC_OwnAutos,
	AutoInterests_OTC_UsedAutos,
	AuxiliaryLightingSystem,
	AuxiliaryRunningLampsDiscount,
	CA0198,
	CA0199,
	CA04180604,
	CA2090,
	CA2352,
	CA2355,
	DayCareCenterVehicle,
	Emergency,
	EmployeeTransport,
	EmployerFurnished,
	ExperienceRatingBasicLimitPremiumPhysicalDamageGarageSum,
	ExperienceRatingBasicLimitPremiumPhysicalDamageSum,
	FareCharged,
	FireDepartments,
	FleetIndicator,
	FleetIndicatorPrivatePassenger,
	FullCoverageGlass,
	FullCoverageGlassColl,
	GolfCartType,
	GolfMobileCommercialUse,
	HistoricVehicle,
	IndividuallyOwned,
	IndividualOrMarried,
	InterpolicyStacking,
	IsFareCharged,
	LawEnforcement,
	LoanLease,
	MCCASurcharge,
	Mileage,
	MileageEstimatePublic,
	MotorscooterIndicator,
	MSSeatingCapacity,
	NumberOfDaysLeased,
	NumberOfPlates,
	NumberOfRegPlates,
	NYMotorVehLawEnforcementFee,
	OneRoundTripPerDay,
	PassiveRestraintDiscount,
	PropertyProtectionUnits,
	RatingBasis,
	RegistrationPlates,
	RegistrationPlatesDriveAwayContractors,
	RegistrationState,
	RentalReimbursement,
	SchoolTermBegins,
	SchoolTermEnds,
	SoundReceivingCoverageType,
	SpecialEquipmentTerritory,
	Stacked,
	StackedUIM,
	SubjectToCompulsoryLaw,
	SubjectToNoFault,
	TotalAnnualPayroll,
	TotalAnnualPayrollAnnual,
	TotalAnnualPayrollEstimate,
	TrailerCollision,
	TrailerComprehensive,
	TrailerPhysicalDamageExclusion,
	TrailerSpecifiedCauses,
	TransOfEmp,
	TruckingUse,
	UsedAsShowroom,
	UsedWithLightTrucks,
	VehicleNumber,
	WAAutoLoanCoverage,
	CA_LocationXmlId,
	CA_RegistrantXmlId,
	-- *INF*: DECODE(Deleted, 'T', 1, 'F', 0, NULL)
	DECODE(
	    Deleted,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Deleted,
	-- *INF*: DECODE(AutoInterests_Coll_Consignment, 'T', 1, 'F', 0, NULL)
	DECODE(
	    AutoInterests_Coll_Consignment,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AutoInterests_Coll_Consignment,
	-- *INF*: DECODE(AutoInterests_Coll_CrNameLossPayee, 'T', 1, 'F', 0, NULL)
	DECODE(
	    AutoInterests_Coll_CrNameLossPayee,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AutoInterests_Coll_CrNameLossPayee,
	-- *INF*: DECODE(AutoInterests_Coll_FinancedAutos, 'T', 1, 'F', 0, NULL)
	DECODE(
	    AutoInterests_Coll_FinancedAutos,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AutoInterests_Coll_FinancedAutos,
	-- *INF*: DECODE(AutoInterests_Coll_NewAutos, 'T', 1, 'F', 0, NULL)
	DECODE(
	    AutoInterests_Coll_NewAutos,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AutoInterests_Coll_NewAutos,
	-- *INF*: DECODE(AutoInterests_Coll_OwnAutos, 'T', 1, 'F', 0, NULL)
	DECODE(
	    AutoInterests_Coll_OwnAutos,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AutoInterests_Coll_OwnAutos,
	-- *INF*: DECODE(AutoInterests_Coll_UsedAutos, 'T', 1, 'F', 0, NULL)
	DECODE(
	    AutoInterests_Coll_UsedAutos,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AutoInterests_Coll_UsedAutos,
	-- *INF*: DECODE(AutoInterests_OTC_Consignment, 'T', 1, 'F', 0, NULL)
	DECODE(
	    AutoInterests_OTC_Consignment,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AutoInterests_OTC_Consignment,
	-- *INF*: DECODE(AutoInterests_OTC_CrNameLossPayee, 'T', 1, 'F', 0, NULL)
	DECODE(
	    AutoInterests_OTC_CrNameLossPayee,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AutoInterests_OTC_CrNameLossPayee,
	-- *INF*: DECODE(AutoInterests_OTC_FinancedAutos, 'T', 1, 'F', 0, NULL)
	DECODE(
	    AutoInterests_OTC_FinancedAutos,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AutoInterests_OTC_FinancedAutos,
	-- *INF*: DECODE(AutoInterests_OTC_NewAutos, 'T', 1, 'F', 0, NULL)
	DECODE(
	    AutoInterests_OTC_NewAutos,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AutoInterests_OTC_NewAutos,
	-- *INF*: DECODE(AutoInterests_OTC_OwnAutos, 'T', 1, 'F', 0, NULL)
	DECODE(
	    AutoInterests_OTC_OwnAutos,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AutoInterests_OTC_OwnAutos,
	-- *INF*: DECODE(AutoInterests_OTC_UsedAutos, 'T', 1, 'F', 0, NULL)
	DECODE(
	    AutoInterests_OTC_UsedAutos,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AutoInterests_OTC_UsedAutos,
	-- *INF*: DECODE(AuxiliaryLightingSystem, 'T', 1, 'F', 0, NULL)
	DECODE(
	    AuxiliaryLightingSystem,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AuxiliaryLightingSystem,
	-- *INF*: DECODE(AuxiliaryRunningLampsDiscount, 'T', 1, 'F', 0, NULL)
	DECODE(
	    AuxiliaryRunningLampsDiscount,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AuxiliaryRunningLampsDiscount,
	-- *INF*: DECODE(CA0198, 'T', 1, 'F', 0, NULL)
	DECODE(
	    CA0198,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_CA0198,
	-- *INF*: DECODE(CA0199, 'T', 1, 'F', 0, NULL)
	DECODE(
	    CA0199,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_CA0199,
	-- *INF*: DECODE(CA04180604, 'T', 1, 'F', 0, NULL)
	DECODE(
	    CA04180604,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_CA04180604,
	-- *INF*: DECODE(CA2090, 'T', 1, 'F', 0, NULL)
	DECODE(
	    CA2090,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_CA2090,
	-- *INF*: DECODE(CA2352, 'T', 1, 'F', 0, NULL)
	DECODE(
	    CA2352,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_CA2352,
	-- *INF*: DECODE(CA2355, 'T', 1, 'F', 0, NULL)
	DECODE(
	    CA2355,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_CA2355,
	-- *INF*: DECODE(DayCareCenterVehicle, 'T', 1, 'F', 0, NULL)
	DECODE(
	    DayCareCenterVehicle,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_DayCareCenterVehicle,
	-- *INF*: DECODE(Emergency, 'T', 1, 'F', 0, NULL)
	DECODE(
	    Emergency,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Emergency,
	-- *INF*: DECODE(EmployeeTransport, 'T', 1, 'F', 0, NULL)
	DECODE(
	    EmployeeTransport,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_EmployeeTransport,
	-- *INF*: DECODE(EmployerFurnished, 'T', 1, 'F', 0, NULL)
	DECODE(
	    EmployerFurnished,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_EmployerFurnished,
	-- *INF*: DECODE(FareCharged, 'T', 1, 'F', 0, NULL)
	DECODE(
	    FareCharged,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_FareCharged,
	-- *INF*: DECODE(FireDepartments, 'T', 1, 'F', 0, NULL)
	DECODE(
	    FireDepartments,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_FireDepartments,
	-- *INF*: DECODE(FleetIndicator, 'T', 1, 'F', 0, NULL)
	DECODE(
	    FleetIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_FleetIndicator,
	-- *INF*: DECODE(FleetIndicatorPrivatePassenger, 'T', 1, 'F', 0, NULL)
	DECODE(
	    FleetIndicatorPrivatePassenger,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_FleetIndicatorPrivatePassenger,
	-- *INF*: DECODE(FullCoverageGlass, 'T', 1, 'F', 0, NULL)
	DECODE(
	    FullCoverageGlass,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_FullCoverageGlass,
	-- *INF*: DECODE(FullCoverageGlassColl, 'T', 1, 'F', 0, NULL)
	DECODE(
	    FullCoverageGlassColl,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_FullCoverageGlassColl,
	-- *INF*: DECODE(GolfMobileCommercialUse, 'T', 1, 'F', 0, NULL)
	DECODE(
	    GolfMobileCommercialUse,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_GolfMobileCommercialUse,
	-- *INF*: DECODE(HistoricVehicle, 'T', 1, 'F', 0, NULL)
	DECODE(
	    HistoricVehicle,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_HistoricVehicle,
	-- *INF*: DECODE(IndividuallyOwned, 'T', 1, 'F', 0, NULL)
	DECODE(
	    IndividuallyOwned,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IndividuallyOwned,
	-- *INF*: DECODE(IndividualOrMarried, 'T', 1, 'F', 0, NULL)
	DECODE(
	    IndividualOrMarried,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IndividualOrMarried,
	-- *INF*: DECODE(IsFareCharged, 'T', 1, 'F', 0, NULL)
	DECODE(
	    IsFareCharged,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IsFareCharged,
	-- *INF*: DECODE(LawEnforcement, 'T', 1, 'F', 0, NULL)
	DECODE(
	    LawEnforcement,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_LawEnforcement,
	-- *INF*: DECODE(LoanLease, 'T', 1, 'F', 0, NULL)
	DECODE(
	    LoanLease,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_LoanLease,
	-- *INF*: DECODE(MotorscooterIndicator, 'T', 1, 'F', 0, NULL)
	DECODE(
	    MotorscooterIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_MotorscooterIndicator,
	-- *INF*: DECODE(OneRoundTripPerDay, 'T', 1, 'F', 0, NULL)
	DECODE(
	    OneRoundTripPerDay,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_OneRoundTripPerDay,
	-- *INF*: DECODE(RentalReimbursement, 'T', 1, 'F', 0, NULL)
	DECODE(
	    RentalReimbursement,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_RentalReimbursement,
	-- *INF*: DECODE(Stacked, 'T', 1, 'F', 0, NULL)
	DECODE(
	    Stacked,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Stacked,
	-- *INF*: DECODE(StackedUIM, 'T', 1, 'F', 0, NULL)
	DECODE(
	    StackedUIM,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_StackedUIM,
	-- *INF*: DECODE(SubjectToCompulsoryLaw, 'T', 1, 'F', 0, NULL)
	DECODE(
	    SubjectToCompulsoryLaw,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_SubjectToCompulsoryLaw,
	-- *INF*: DECODE(SubjectToNoFault, 'T', 1, 'F', 0, NULL)
	DECODE(
	    SubjectToNoFault,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_SubjectToNoFault,
	-- *INF*: DECODE(TrailerCollision, 'T', 1, 'F', 0, NULL)
	DECODE(
	    TrailerCollision,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TrailerCollision,
	-- *INF*: DECODE(TrailerComprehensive, 'T', 1, 'F', 0, NULL)
	DECODE(
	    TrailerComprehensive,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TrailerComprehensive,
	-- *INF*: DECODE(TrailerSpecifiedCauses, 'T', 1, 'F', 0, NULL)
	DECODE(
	    TrailerSpecifiedCauses,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TrailerSpecifiedCauses,
	-- *INF*: DECODE(TransOfEmp, 'T', 1, 'F', 0, NULL)
	DECODE(
	    TransOfEmp,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TransOfEmp,
	-- *INF*: DECODE(TruckingUse, 'T', 1, 'F', 0, NULL)
	DECODE(
	    TruckingUse,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TruckingUse,
	-- *INF*: DECODE(UsedAsShowroom, 'T', 1, 'F', 0, NULL)
	DECODE(
	    UsedAsShowroom,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_UsedAsShowroom,
	-- *INF*: DECODE(UsedWithLightTrucks, 'T', 1, 'F', 0, NULL)
	DECODE(
	    UsedWithLightTrucks,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_UsedWithLightTrucks,
	-- *INF*: DECODE(WAAutoLoanCoverage, 'T', 1, 'F', 0, NULL)
	DECODE(
	    WAAutoLoanCoverage,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_WAAutoLoanCoverage,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CA_Risk
),
DCCARiskStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCARiskStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCARiskStaging
	(ExtractDate, SourceSystemId, CA_StateId, CA_LocationId, CA_RiskId, SessionId, Id, Type, RiskType, Description, AssignedDriverAge, Auditable, AutoInterests_Coll_Consignment, AutoInterests_Coll_CrNameLossPayee, AutoInterests_Coll_FinancedAutos, AutoInterests_Coll_NewAutos, AutoInterests_Coll_OwnAutos, AutoInterests_Coll_UsedAutos, AutoInterests_OTC_Consignment, AutoInterests_OTC_CrNameLossPayee, AutoInterests_OTC_FinancedAutos, AutoInterests_OTC_NewAutos, AutoInterests_OTC_OwnAutos, AutoInterests_OTC_UsedAutos, AuxiliaryLightingSystem, AuxiliaryRunningLampsDiscount, CA0198, CA0199, CA04180604, CA2090, CA2352, CA2355, DayCareCenterVehicle, Emergency, EmployeeTransport, EmployerFurnished, ExperienceRatingBasicLimitPremiumPhysicalDamageGarageSum, ExperienceRatingBasicLimitPremiumPhysicalDamageSum, FareCharged, FireDepartments, FleetIndicator, FleetIndicatorPrivatePassenger, FullCoverageGlass, FullCoverageGlassColl, GolfCartType, GolfMobileCommercialUse, HistoricVehicle, IndividuallyOwned, IndividualOrMarried, InterpolicyStacking, IsFareCharged, LawEnforcement, LoanLease, MCCASurcharge, Mileage, MileageEstimatePublic, MotorscooterIndicator, MSSeatingCapacity, NumberOfDaysLeased, NumberOfPlates, NumberOfRegPlates, NYMotorVehLawEnforcementFee, OneRoundTripPerDay, PassiveRestraintDiscount, PropertyProtectionUnits, RatingBasis, RegistrationPlates, RegistrationPlatesDriveAwayContractors, RegistrationState, RentalReimbursement, SchoolTermBegins, SchoolTermEnds, SoundReceivingCoverageType, SpecialEquipmentTerritory, Stacked, StackedUIM, SubjectToCompulsoryLaw, SubjectToNoFault, TotalAnnualPayroll, TotalAnnualPayrollAnnual, TotalAnnualPayrollEstimate, TrailerCollision, TrailerComprehensive, TrailerPhysicalDamageExclusion, TrailerSpecifiedCauses, TransOfEmp, TruckingUse, UsedAsShowroom, UsedWithLightTrucks, VehicleNumber, WAAutoLoanCoverage, CA_LocationXmlId, CA_RegistrantXmlId, Deleted)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CA_STATEID, 
	CA_LOCATIONID, 
	CA_RISKID, 
	SESSIONID, 
	ID, 
	TYPE, 
	RISKTYPE, 
	DESCRIPTION, 
	ASSIGNEDDRIVERAGE, 
	AUDITABLE, 
	o_AutoInterests_Coll_Consignment AS AUTOINTERESTS_COLL_CONSIGNMENT, 
	o_AutoInterests_Coll_CrNameLossPayee AS AUTOINTERESTS_COLL_CRNAMELOSSPAYEE, 
	o_AutoInterests_Coll_FinancedAutos AS AUTOINTERESTS_COLL_FINANCEDAUTOS, 
	o_AutoInterests_Coll_NewAutos AS AUTOINTERESTS_COLL_NEWAUTOS, 
	o_AutoInterests_Coll_OwnAutos AS AUTOINTERESTS_COLL_OWNAUTOS, 
	o_AutoInterests_Coll_UsedAutos AS AUTOINTERESTS_COLL_USEDAUTOS, 
	o_AutoInterests_OTC_Consignment AS AUTOINTERESTS_OTC_CONSIGNMENT, 
	o_AutoInterests_OTC_CrNameLossPayee AS AUTOINTERESTS_OTC_CRNAMELOSSPAYEE, 
	o_AutoInterests_OTC_FinancedAutos AS AUTOINTERESTS_OTC_FINANCEDAUTOS, 
	o_AutoInterests_OTC_NewAutos AS AUTOINTERESTS_OTC_NEWAUTOS, 
	o_AutoInterests_OTC_OwnAutos AS AUTOINTERESTS_OTC_OWNAUTOS, 
	o_AutoInterests_OTC_UsedAutos AS AUTOINTERESTS_OTC_USEDAUTOS, 
	o_AuxiliaryLightingSystem AS AUXILIARYLIGHTINGSYSTEM, 
	o_AuxiliaryRunningLampsDiscount AS AUXILIARYRUNNINGLAMPSDISCOUNT, 
	o_CA0198 AS CA0198, 
	o_CA0199 AS CA0199, 
	o_CA04180604 AS CA04180604, 
	o_CA2090 AS CA2090, 
	o_CA2352 AS CA2352, 
	o_CA2355 AS CA2355, 
	o_DayCareCenterVehicle AS DAYCARECENTERVEHICLE, 
	o_Emergency AS EMERGENCY, 
	o_EmployeeTransport AS EMPLOYEETRANSPORT, 
	o_EmployerFurnished AS EMPLOYERFURNISHED, 
	EXPERIENCERATINGBASICLIMITPREMIUMPHYSICALDAMAGEGARAGESUM, 
	EXPERIENCERATINGBASICLIMITPREMIUMPHYSICALDAMAGESUM, 
	o_FareCharged AS FARECHARGED, 
	o_FireDepartments AS FIREDEPARTMENTS, 
	o_FleetIndicator AS FLEETINDICATOR, 
	o_FleetIndicatorPrivatePassenger AS FLEETINDICATORPRIVATEPASSENGER, 
	o_FullCoverageGlass AS FULLCOVERAGEGLASS, 
	o_FullCoverageGlassColl AS FULLCOVERAGEGLASSCOLL, 
	GOLFCARTTYPE, 
	o_GolfMobileCommercialUse AS GOLFMOBILECOMMERCIALUSE, 
	o_HistoricVehicle AS HISTORICVEHICLE, 
	o_IndividuallyOwned AS INDIVIDUALLYOWNED, 
	o_IndividualOrMarried AS INDIVIDUALORMARRIED, 
	INTERPOLICYSTACKING, 
	o_IsFareCharged AS ISFARECHARGED, 
	o_LawEnforcement AS LAWENFORCEMENT, 
	o_LoanLease AS LOANLEASE, 
	MCCASURCHARGE, 
	MILEAGE, 
	MILEAGEESTIMATEPUBLIC, 
	o_MotorscooterIndicator AS MOTORSCOOTERINDICATOR, 
	MSSEATINGCAPACITY, 
	NUMBEROFDAYSLEASED, 
	NUMBEROFPLATES, 
	NUMBEROFREGPLATES, 
	NYMOTORVEHLAWENFORCEMENTFEE, 
	o_OneRoundTripPerDay AS ONEROUNDTRIPPERDAY, 
	PASSIVERESTRAINTDISCOUNT, 
	PROPERTYPROTECTIONUNITS, 
	RATINGBASIS, 
	REGISTRATIONPLATES, 
	REGISTRATIONPLATESDRIVEAWAYCONTRACTORS, 
	REGISTRATIONSTATE, 
	o_RentalReimbursement AS RENTALREIMBURSEMENT, 
	SCHOOLTERMBEGINS, 
	SCHOOLTERMENDS, 
	SOUNDRECEIVINGCOVERAGETYPE, 
	SPECIALEQUIPMENTTERRITORY, 
	o_Stacked AS STACKED, 
	o_StackedUIM AS STACKEDUIM, 
	o_SubjectToCompulsoryLaw AS SUBJECTTOCOMPULSORYLAW, 
	o_SubjectToNoFault AS SUBJECTTONOFAULT, 
	TOTALANNUALPAYROLL, 
	TOTALANNUALPAYROLLANNUAL, 
	TOTALANNUALPAYROLLESTIMATE, 
	o_TrailerCollision AS TRAILERCOLLISION, 
	o_TrailerComprehensive AS TRAILERCOMPREHENSIVE, 
	TRAILERPHYSICALDAMAGEEXCLUSION, 
	o_TrailerSpecifiedCauses AS TRAILERSPECIFIEDCAUSES, 
	o_TransOfEmp AS TRANSOFEMP, 
	o_TruckingUse AS TRUCKINGUSE, 
	o_UsedAsShowroom AS USEDASSHOWROOM, 
	o_UsedWithLightTrucks AS USEDWITHLIGHTTRUCKS, 
	VEHICLENUMBER, 
	o_WAAutoLoanCoverage AS WAAUTOLOANCOVERAGE, 
	CA_LOCATIONXMLID, 
	CA_REGISTRANTXMLID, 
	o_Deleted AS DELETED
	FROM EXP_Metadata
),
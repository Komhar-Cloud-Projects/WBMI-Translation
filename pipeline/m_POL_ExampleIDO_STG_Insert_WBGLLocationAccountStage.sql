WITH
SQ_WB_GL_LocationAccount AS (
	WITH cte_WBGLLocationAccount(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.WB_CL_LocationAccountId, 
	X.WB_GL_LocationAccountId, 
	X.SessionId, 
	X.LiabScheduleModCooperationMedicalSetLiabilityValue, 
	X.LiabScheduleModCooperationMedicalSetCommentValue, 
	X.SetCommentValue, 
	X.LiabScheduleModClassificationSetLiabilityValue, 
	X.LiabScheduleModCooperationSafetySetLiabilityValue, 
	X.LiabScheduleModCooperationSafetySetCommentValue, 
	X.LiabScheduleModEmployeesSetCommentValue, 
	X.LiabScheduleModEmployeesSetLiabilityValue, 
	X.LiabScheduleModEquipmentSetCommentValue, 
	X.LiabScheduleModEquipmentSetLiabilityValue, 
	X.LiabScheduleModLocationInsideSetCommentValue, 
	X.LiabScheduleModLocationInsideSetLiabilityValue, 
	X.LiabScheduleModLocationOutsideSetCommentValue, 
	X.LiabScheduleModLocationOutsideSetLiabilityValue, 
	X.LiabScheduleModPremisesSetCommentValue, 
	X.LiabScheduleModPremisesSetLiabilityValue, 
	X.LiquorPermitNumber, 
	X.OutdoorServiceArea, 
	X.MunicipalAutoRate, 
	X.MunicipalLiabilityRate, 
	X.MunicipalMarineRate, 
	X.MunicipalMinimumRate, 
	X.MunicipalOtherRate, 
	X.MunicipalPropertyRate, 
	X.CountyAutoRate, 
	X.CountyLiabilityRate, 
	X.CountyMarineRate, 
	X.CountyMinimumRate, 
	X.CountyOtherRate, 
	X.CountyPropertyRate, 
	X.GeoTaxConfidence, 
	X.CertifiedOperationsFactor, 
	X.NighttimeOperationsFactor, 
	X.ProgramFactor, 
	X.AssociationFactor, 
	X.MaximumNumberOfChildren, 
	X.HoursOpen, 
	X.OnlyBeforeAndAfterSchoolCare, 
	X.DogCatCoverage, 
	X.DogBreedsPresent, 
	X.OwnAnyBuildings, 
	X.LeaseAnyBuildingsToOthers, 
	X.TotalSquareFeetLeasedToOthers, 
	X.TotalNumberApartments, 
	X.IndependentContractors, 
	X.NumberFullTimeBeauticians, 
	X.NumberPartTimeBeauticians, 
	X.NumberFullTimeElectrologists, 
	X.NumberPartTimeElectrologists, 
	X.NumberFullTimeMassageTherapists, 
	X.NumberPartTimeMassageTherapists, 
	X.NumberBeautyInstructors, 
	X.NumberAquaMassageBeds, 
	X.NumberSuntanBeds, 
	X.NumberAirBrushOrSprayOnBooths, 
	X.NumberHotTubs, 
	X.NumberCircuitWorkout 
	FROM
	WB_GL_LocationAccount X
	inner join
	cte_WBGLLocationAccount Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	WB_CL_LocationAccountId,
	WB_GL_LocationAccountId,
	SessionId,
	LiabScheduleModCooperationMedicalSetLiabilityValue,
	LiabScheduleModCooperationMedicalSetCommentValue,
	SetCommentValue,
	LiabScheduleModClassificationSetLiabilityValue,
	LiabScheduleModCooperationSafetySetLiabilityValue,
	LiabScheduleModCooperationSafetySetCommentValue,
	LiabScheduleModEmployeesSetCommentValue,
	LiabScheduleModEmployeesSetLiabilityValue,
	LiabScheduleModEquipmentSetCommentValue,
	LiabScheduleModEquipmentSetLiabilityValue,
	LiabScheduleModLocationInsideSetCommentValue,
	LiabScheduleModLocationInsideSetLiabilityValue,
	LiabScheduleModLocationOutsideSetCommentValue,
	LiabScheduleModLocationOutsideSetLiabilityValue,
	LiabScheduleModPremisesSetCommentValue,
	LiabScheduleModPremisesSetLiabilityValue,
	LiquorPermitNumber,
	OutdoorServiceArea,
	MunicipalAutoRate,
	MunicipalLiabilityRate,
	MunicipalMarineRate,
	MunicipalMinimumRate,
	MunicipalOtherRate,
	MunicipalPropertyRate,
	CountyAutoRate,
	CountyLiabilityRate,
	CountyMarineRate,
	CountyMinimumRate,
	CountyOtherRate,
	CountyPropertyRate,
	GeoTaxConfidence,
	CertifiedOperationsFactor,
	NighttimeOperationsFactor,
	ProgramFactor,
	AssociationFactor,
	MaximumNumberOfChildren,
	HoursOpen,
	OnlyBeforeAndAfterSchoolCare AS i_OnlyBeforeAndAfterSchoolCare,
	-- *INF*: IIF(i_OnlyBeforeAndAfterSchoolCare='T','1','0')
	IFF(i_OnlyBeforeAndAfterSchoolCare = 'T', '1', '0') AS o_OnlyBeforeAndAfterSchoolCare,
	DogCatCoverage,
	DogBreedsPresent AS i_DogBreedsPresent,
	-- *INF*: IIF(i_DogBreedsPresent='T','1','0')
	IFF(i_DogBreedsPresent = 'T', '1', '0') AS o_DogBreedsPresent,
	OwnAnyBuildings AS i_OwnAnyBuildings,
	-- *INF*: IIF(i_OwnAnyBuildings='T','1','0')
	IFF(i_OwnAnyBuildings = 'T', '1', '0') AS o_OwnAnyBuildings,
	LeaseAnyBuildingsToOthers AS i_LeaseAnyBuildingsToOthers,
	-- *INF*: IIF(i_LeaseAnyBuildingsToOthers='T','1','0')
	IFF(i_LeaseAnyBuildingsToOthers = 'T', '1', '0') AS o_LeaseAnyBuildingsToOthers,
	TotalSquareFeetLeasedToOthers,
	TotalNumberApartments,
	IndependentContractors AS i_IndependentContractors,
	-- *INF*: IIF(i_IndependentContractors='T','1','0')
	IFF(i_IndependentContractors = 'T', '1', '0') AS o_IndependentContractors,
	NumberFullTimeBeauticians,
	NumberPartTimeBeauticians,
	NumberFullTimeElectrologists,
	NumberPartTimeElectrologists,
	NumberFullTimeMassageTherapists,
	NumberPartTimeMassageTherapists,
	NumberBeautyInstructors,
	NumberAquaMassageBeds,
	NumberSuntanBeds,
	NumberAirBrushOrSprayOnBooths,
	NumberHotTubs,
	NumberCircuitWorkout
	FROM SQ_WB_GL_LocationAccount
),
WBGLLocationAccountStage AS (
	TRUNCATE TABLE WBGLLocationAccountStage;
	INSERT INTO WBGLLocationAccountStage
	(ExtractDate, SourceSystemId, WB_CL_LocationAccountId, WB_GL_LocationAccountId, SessionId, LiabScheduleModCooperationMedicalSetLiabilityValue, LiabScheduleModCooperationMedicalSetCommentValue, SetCommentValue, LiabScheduleModClassificationSetLiabilityValue, LiabScheduleModCooperationSafetySetLiabilityValue, LiabScheduleModCooperationSafetySetCommentValue, LiabScheduleModEmployeesSetCommentValue, LiabScheduleModEmployeesSetLiabilityValue, LiabScheduleModEquipmentSetCommentValue, LiabScheduleModEquipmentSetLiabilityValue, LiabScheduleModLocationInsideSetCommentValue, LiabScheduleModLocationInsideSetLiabilityValue, LiabScheduleModLocationOutsideSetCommentValue, LiabScheduleModLocationOutsideSetLiabilityValue, LiabScheduleModPremisesSetCommentValue, LiabScheduleModPremisesSetLiabilityValue, LiquorPermitNumber, MunicipalAutoRate, MunicipalLiabilityRate, MunicipalMarineRate, MunicipalMinimumRate, MunicipalOtherRate, MunicipalPropertyRate, CountyAutoRate, CountyLiabilityRate, CountyMarineRate, CountyMinimumRate, CountyOtherRate, CountyPropertyRate, GeoTaxConfidence, CertifiedOperationsFactor, NighttimeOperationsFactor, ProgramFactor, AssociationFactor, MaximumNumberOfChildren, HoursOpen, OnlyBeforeAndAfterSchoolCare, DogCatCoverage, DogBreedsPresent, OwnAnyBuildings, LeaseAnyBuildingsToOthers, TotalSquareFeetLeasedToOthers, TotalNumberApartments, IndependentContractors, NumberFullTimeBeauticians, NumberPartTimeBeauticians, NumberFullTimeElectrologists, NumberPartTimeElectrologists, NumberFullTimeMassageTherapists, NumberPartTimeMassageTherapists, NumberBeautyInstructors, NumberAquaMassageBeds, NumberSuntanBeds, NumberAirBrushOrSprayOnBooths, NumberHotTubs, NumberCircuitWorkout, OutdoorServiceArea)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	WB_CL_LOCATIONACCOUNTID, 
	WB_GL_LOCATIONACCOUNTID, 
	SESSIONID, 
	LIABSCHEDULEMODCOOPERATIONMEDICALSETLIABILITYVALUE, 
	LIABSCHEDULEMODCOOPERATIONMEDICALSETCOMMENTVALUE, 
	SETCOMMENTVALUE, 
	LIABSCHEDULEMODCLASSIFICATIONSETLIABILITYVALUE, 
	LIABSCHEDULEMODCOOPERATIONSAFETYSETLIABILITYVALUE, 
	LIABSCHEDULEMODCOOPERATIONSAFETYSETCOMMENTVALUE, 
	LIABSCHEDULEMODEMPLOYEESSETCOMMENTVALUE, 
	LIABSCHEDULEMODEMPLOYEESSETLIABILITYVALUE, 
	LIABSCHEDULEMODEQUIPMENTSETCOMMENTVALUE, 
	LIABSCHEDULEMODEQUIPMENTSETLIABILITYVALUE, 
	LIABSCHEDULEMODLOCATIONINSIDESETCOMMENTVALUE, 
	LIABSCHEDULEMODLOCATIONINSIDESETLIABILITYVALUE, 
	LIABSCHEDULEMODLOCATIONOUTSIDESETCOMMENTVALUE, 
	LIABSCHEDULEMODLOCATIONOUTSIDESETLIABILITYVALUE, 
	LIABSCHEDULEMODPREMISESSETCOMMENTVALUE, 
	LIABSCHEDULEMODPREMISESSETLIABILITYVALUE, 
	LIQUORPERMITNUMBER, 
	MUNICIPALAUTORATE, 
	MUNICIPALLIABILITYRATE, 
	MUNICIPALMARINERATE, 
	MUNICIPALMINIMUMRATE, 
	MUNICIPALOTHERRATE, 
	MUNICIPALPROPERTYRATE, 
	COUNTYAUTORATE, 
	COUNTYLIABILITYRATE, 
	COUNTYMARINERATE, 
	COUNTYMINIMUMRATE, 
	COUNTYOTHERRATE, 
	COUNTYPROPERTYRATE, 
	GEOTAXCONFIDENCE, 
	CERTIFIEDOPERATIONSFACTOR, 
	NIGHTTIMEOPERATIONSFACTOR, 
	PROGRAMFACTOR, 
	ASSOCIATIONFACTOR, 
	MAXIMUMNUMBEROFCHILDREN, 
	HOURSOPEN, 
	o_OnlyBeforeAndAfterSchoolCare AS ONLYBEFOREANDAFTERSCHOOLCARE, 
	DOGCATCOVERAGE, 
	o_DogBreedsPresent AS DOGBREEDSPRESENT, 
	o_OwnAnyBuildings AS OWNANYBUILDINGS, 
	o_LeaseAnyBuildingsToOthers AS LEASEANYBUILDINGSTOOTHERS, 
	TOTALSQUAREFEETLEASEDTOOTHERS, 
	TOTALNUMBERAPARTMENTS, 
	o_IndependentContractors AS INDEPENDENTCONTRACTORS, 
	NUMBERFULLTIMEBEAUTICIANS, 
	NUMBERPARTTIMEBEAUTICIANS, 
	NUMBERFULLTIMEELECTROLOGISTS, 
	NUMBERPARTTIMEELECTROLOGISTS, 
	NUMBERFULLTIMEMASSAGETHERAPISTS, 
	NUMBERPARTTIMEMASSAGETHERAPISTS, 
	NUMBERBEAUTYINSTRUCTORS, 
	NUMBERAQUAMASSAGEBEDS, 
	NUMBERSUNTANBEDS, 
	NUMBERAIRBRUSHORSPRAYONBOOTHS, 
	NUMBERHOTTUBS, 
	NUMBERCIRCUITWORKOUT, 
	OUTDOORSERVICEAREA
	FROM EXP_Metadata
),
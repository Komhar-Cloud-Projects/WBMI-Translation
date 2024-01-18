WITH
SQ_WB_CL_Line AS (
	WITH cte_WBCLLine(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.WB_CL_LineId, 
	X.SessionId, 
	X.WB_LineId, 
	X.IsAuditable, 
	X.PreliminaryAudit, 
	X.AuditPeriod, 
	X.OverrideAuditReason, 
	X.OverrideAudit, 
	X.MidTermAudit, 
	X.AuditablePremium, 
	X.TaxAppliedToKYPremiumWritten, 
	X.TaxAppliedToKYPremium, 
	X.TaxAppliedToKYPremiumChange, 
	X.RiskGradePremises, 
	X.RiskGradeProduct, 
	X.RiskGradeSMARTOther, 
	X.DescriptionForAuto, 
	X.ArcheryNS0018, 
	X.AerobicsNS0018, 
	X.BaseballOrSoftballNS0018, 
	X.BasketballNS0018, 
	X.BicyclingNS0018, 
	X.BowlingNS0018, 
	X.CanoeingNS0018, 
	X.CheerleadingNS0018, 
	X.CurlingNS0018, 
	X.CrossCountrySkiingNS0018, 
	X.FencingNS0018, 
	X.FieldHockeyNS0018, 
	X.FlagFootballNS0018, 
	X.FootballTouchNS0018, 
	X.GolfNS0018, 
	X.IceHockeyYouthNS0018, 
	X.MartialArtsNS0018, 
	X.OtherNS0018, 
	X.OtherRow1NS0018, 
	X.OtherRow2NS0018, 
	X.RunningNS0018, 
	X.SnowshoeingNS0018, 
	X.SoccerNS0018, 
	X.SpeedSkatingNS0018, 
	X.SwimmingNS0018, 
	X.TennisNS0018, 
	X.VolleyballNS0018, 
	X.WrestlingYouthNS0018, 
	X.DateDescriptionOfEventsActivitiesOperationsLocationNS0034GLIL, 
	X.DescriptionOfEventsNS0038, 
	X.DateDescriptionOfEventsActivitiesOperationsLocationNS0034GL, 
	X.DescriptionOfDesignatedOperationsNS0044GL, 
	X.DescriptionOfOperationsSpecifiedLocationNS0044GL, 
	X.DescriptionOfMobilEquipmentNS0065GL, 
	X.DescriptionOfSpecificCircumstancesNS0108, 
	X.NamesOfMunicipalitiesWB1882,
	X.CyberOneIndicator,
	X.CyberOnePremium,
	X.CyberOneEligibilityQuestion,
	X.CyberOneIncreasedLimitQuestionOne,
	X.CyberOneIncreasedLimitQuestionTwo,
	X.CyberOneIncreasedLimitQuestionThree,
	X.CyberOneIncreasedLimitQuestionFour,
	X.CyberOneIncreasedLimitQuestionFive,
	X.CyberOneIncreasedLimitQuestionSix,
	X.CyberOneIncreasedLimitQuestionSeven,
	X.CyberOneIncreasedLimitQuestionEight,
	X.CyberOneIncreasedLimitQuestionNine,
	X.CyberOneIncreasedLimitQuestionTen,
	X.CyberOneIncreasedLimitQuestionEleven,
	X.CyberOneIncreasedLimitQuestionTwelve,
	X.RatingTier
	FROM
	WB_CL_Line X
	inner join
	cte_WBCLLine Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	WB_CL_LineId,
	SessionId,
	WB_LineId,
	IsAuditable,
	-- *INF*: DECODE(IsAuditable,'T','1','F','0')
	DECODE(
	    IsAuditable,
	    'T', '1',
	    'F', '0'
	) AS o_IsAuditable,
	PreliminaryAudit,
	-- *INF*: DECODE(PreliminaryAudit,'T','1','F','0')
	DECODE(
	    PreliminaryAudit,
	    'T', '1',
	    'F', '0'
	) AS o_PreliminaryAudit,
	AuditPeriod,
	OverrideAuditReason,
	OverrideAudit,
	MidTermAudit,
	-- *INF*: DECODE(MidTermAudit,'T','1','F','0')
	DECODE(
	    MidTermAudit,
	    'T', '1',
	    'F', '0'
	) AS o_MidTermAudit,
	-- *INF*: DECODE(FederalOperatingAuthority,'T','1','F','0')
	DECODE(
	    FederalOperatingAuthority,
	    'T', '1',
	    'F', '0'
	) AS o_FederalOperatingAuthority,
	-- *INF*: DECODE(MC90Only,'T','1','F','0')
	DECODE(
	    MC90Only,
	    'T', '1',
	    'F', '0'
	) AS o_MC90Only,
	-- *INF*: DECODE(InterstateBMC91X,'T','1','F','0')
	DECODE(
	    InterstateBMC91X,
	    'T', '1',
	    'F', '0'
	) AS o_InterstateBMC91X,
	-- *INF*: DECODE(IntrastateFormEEX,'T','1','F','0')
	DECODE(
	    IntrastateFormEEX,
	    'T', '1',
	    'F', '0'
	) AS o_IntrastateFormEEX,
	-- *INF*: DECODE(WIHumanServices,'T','1','F','0')
	DECODE(
	    WIHumanServices,
	    'T', '1',
	    'F', '0'
	) AS o_WIHumanServices,
	-- *INF*: DECODE(WISchoolBuss,'T','1','F','0')
	DECODE(
	    WISchoolBuss,
	    'T', '1',
	    'F', '0'
	) AS o_WISchoolBuss,
	-- *INF*: DECODE(OHHaulingPermit,'T','1','F','0')
	DECODE(
	    OHHaulingPermit,
	    'T', '1',
	    'F', '0'
	) AS o_OHHaulingPermit,
	-- *INF*: DECODE(InterstateBMC34,'T','1','F','0')
	DECODE(
	    InterstateBMC34,
	    'T', '1',
	    'F', '0'
	) AS o_InterstateBMC34,
	-- *INF*: DECODE(IntrastateFormH,'T','1','F','0')
	DECODE(
	    IntrastateFormH,
	    'T', '1',
	    'F', '0'
	) AS o_IntrastateFormH,
	-- *INF*: DECODE(Deleted,'T','1','F','0')
	DECODE(
	    Deleted,
	    'T', '1',
	    'F', '0'
	) AS o_Deleted,
	-- *INF*: DECODE(IntrastateFormEEXHaulingStates,'T','1','F','0')
	DECODE(
	    IntrastateFormEEXHaulingStates,
	    'T', '1',
	    'F', '0'
	) AS o_IntrastateFormEEXHaulingStates,
	AuditablePremium,
	TaxAppliedToKYPremiumWritten,
	TaxAppliedToKYPremium,
	TaxAppliedToKYPremiumChange,
	RiskGradePremises,
	RiskGradeProduct,
	RiskGradeSMARTOther,
	DescriptionForAuto,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	ArcheryNS0018,
	-- *INF*: DECODE(ArcheryNS0018,'T','1','F','0')
	DECODE(
	    ArcheryNS0018,
	    'T', '1',
	    'F', '0'
	) AS ArcheryNS0018_out,
	AerobicsNS0018,
	-- *INF*: DECODE(AerobicsNS0018,'T','1','F','0')
	DECODE(
	    AerobicsNS0018,
	    'T', '1',
	    'F', '0'
	) AS AerobicsNS0018_out,
	BaseballOrSoftballNS0018,
	-- *INF*: DECODE(BaseballOrSoftballNS0018,'T','1','F','0')
	DECODE(
	    BaseballOrSoftballNS0018,
	    'T', '1',
	    'F', '0'
	) AS BaseballOrSoftballNS0018_out,
	BasketballNS0018,
	-- *INF*: DECODE(BasketballNS0018,'T','1','F','0')
	DECODE(
	    BasketballNS0018,
	    'T', '1',
	    'F', '0'
	) AS BasketballNS0018_out,
	BicyclingNS0018,
	-- *INF*: DECODE(BicyclingNS0018,'T','1','F','0')
	DECODE(
	    BicyclingNS0018,
	    'T', '1',
	    'F', '0'
	) AS BicyclingNS0018_out,
	BowlingNS0018,
	-- *INF*: DECODE(BowlingNS0018,'T','1','F','0')
	DECODE(
	    BowlingNS0018,
	    'T', '1',
	    'F', '0'
	) AS BowlingNS0018_out,
	CanoeingNS0018,
	-- *INF*: DECODE(CanoeingNS0018,'T','1','F','0')
	DECODE(
	    CanoeingNS0018,
	    'T', '1',
	    'F', '0'
	) AS CanoeingNS0018_out,
	CheerleadingNS0018,
	-- *INF*: DECODE(CheerleadingNS0018,'T','1','F','0')
	DECODE(
	    CheerleadingNS0018,
	    'T', '1',
	    'F', '0'
	) AS CheerleadingNS0018_out,
	CurlingNS0018,
	-- *INF*: DECODE(CurlingNS0018,'T','1','F','0')
	DECODE(
	    CurlingNS0018,
	    'T', '1',
	    'F', '0'
	) AS CurlingNS0018_out,
	CrossCountrySkiingNS0018,
	-- *INF*: DECODE(CrossCountrySkiingNS0018,'T','1','F','0')
	DECODE(
	    CrossCountrySkiingNS0018,
	    'T', '1',
	    'F', '0'
	) AS CrossCountrySkiingNS0018_out,
	FencingNS0018,
	-- *INF*: DECODE(FencingNS0018,'T','1','F','0')
	DECODE(
	    FencingNS0018,
	    'T', '1',
	    'F', '0'
	) AS FencingNS0018_out,
	FieldHockeyNS0018,
	-- *INF*: DECODE(FieldHockeyNS0018,'T','1','F','0')
	DECODE(
	    FieldHockeyNS0018,
	    'T', '1',
	    'F', '0'
	) AS FieldHockeyNS0018_out,
	FlagFootballNS0018,
	-- *INF*: DECODE(FlagFootballNS0018,'T','1','F','0')
	DECODE(
	    FlagFootballNS0018,
	    'T', '1',
	    'F', '0'
	) AS FlagFootballNS0018_out,
	FootballTouchNS0018,
	-- *INF*: DECODE(FootballTouchNS0018,'T','1','F','0')
	DECODE(
	    FootballTouchNS0018,
	    'T', '1',
	    'F', '0'
	) AS FootballTouchNS0018_out,
	GolfNS0018,
	-- *INF*: DECODE(GolfNS0018,'T','1','F','0')
	DECODE(
	    GolfNS0018,
	    'T', '1',
	    'F', '0'
	) AS GolfNS0018_out,
	IceHockeyYouthNS0018,
	-- *INF*: DECODE(IceHockeyYouthNS0018,'T','1','F','0')
	DECODE(
	    IceHockeyYouthNS0018,
	    'T', '1',
	    'F', '0'
	) AS IceHockeyYouthNS0018_out,
	MartialArtsNS0018,
	-- *INF*: DECODE(MartialArtsNS0018,'T','1','F','0')
	DECODE(
	    MartialArtsNS0018,
	    'T', '1',
	    'F', '0'
	) AS MartialArtsNS0018_out,
	OtherNS0018,
	-- *INF*: DECODE(OtherNS0018,'T','1','F','0')
	DECODE(
	    OtherNS0018,
	    'T', '1',
	    'F', '0'
	) AS OtherNS0018_out,
	OtherRow1NS0018,
	OtherRow2NS0018,
	RunningNS0018,
	-- *INF*: DECODE(RunningNS0018,'T','1','F','0')
	DECODE(
	    RunningNS0018,
	    'T', '1',
	    'F', '0'
	) AS RunningNS0018_out,
	SnowshoeingNS0018,
	-- *INF*: DECODE(SnowshoeingNS0018,'T','1','F','0')
	DECODE(
	    SnowshoeingNS0018,
	    'T', '1',
	    'F', '0'
	) AS SnowshoeingNS0018_out,
	SoccerNS0018,
	-- *INF*: DECODE(SoccerNS0018,'T','1','F','0')
	DECODE(
	    SoccerNS0018,
	    'T', '1',
	    'F', '0'
	) AS SoccerNS0018_out,
	SpeedSkatingNS0018,
	-- *INF*: DECODE(SpeedSkatingNS0018,'T','1','F','0')
	DECODE(
	    SpeedSkatingNS0018,
	    'T', '1',
	    'F', '0'
	) AS SpeedSkatingNS0018_out,
	SwimmingNS0018,
	-- *INF*: DECODE(SwimmingNS0018,'T','1','F','0')
	DECODE(
	    SwimmingNS0018,
	    'T', '1',
	    'F', '0'
	) AS SwimmingNS0018_out,
	TennisNS0018,
	-- *INF*: DECODE(TennisNS0018,'T','1','F','0')
	DECODE(
	    TennisNS0018,
	    'T', '1',
	    'F', '0'
	) AS TennisNS0018_out,
	VolleyballNS0018,
	-- *INF*: DECODE(VolleyballNS0018,'T','1','F','0')
	DECODE(
	    VolleyballNS0018,
	    'T', '1',
	    'F', '0'
	) AS VolleyballNS0018_out,
	WrestlingYouthNS0018,
	-- *INF*: DECODE(WrestlingYouthNS0018,'T','1','F','0')
	DECODE(
	    WrestlingYouthNS0018,
	    'T', '1',
	    'F', '0'
	) AS WrestlingYouthNS0018_out,
	DateDescriptionOfEventsActivitiesOperationsLocationNS0034GLIL,
	DescriptionOfEventsNS0038,
	DateDescriptionOfEventsActivitiesOperationsLocationNS0034GL,
	DescriptionOfDesignatedOperationsNS0044GL,
	DescriptionOfOperationsSpecifiedLocationNS0044GL,
	DescriptionOfMobilEquipmentNS0065GL,
	DescriptionOfSpecificCircumstancesNS0108,
	NamesOfMunicipalitiesWB1882,
	CyberOneIndicator,
	-- *INF*: DECODE(CyberOneIndicator,'T','1','F','0')
	DECODE(
	    CyberOneIndicator,
	    'T', '1',
	    'F', '0'
	) AS CyberOneIndicator_out,
	CyberOnePremium,
	CyberOneEligibilityQuestion,
	-- *INF*: DECODE(CyberOneEligibilityQuestion,'T','1','F','0')
	DECODE(
	    CyberOneEligibilityQuestion,
	    'T', '1',
	    'F', '0'
	) AS CyberOneEligibilityQuestion_out,
	CyberOneIncreasedLimitQuestionOne,
	-- *INF*: DECODE(CyberOneIncreasedLimitQuestionOne,'T','1','F','0')
	DECODE(
	    CyberOneIncreasedLimitQuestionOne,
	    'T', '1',
	    'F', '0'
	) AS CyberOneIncreasedLimitQuestionOne_out,
	CyberOneIncreasedLimitQuestionTwo,
	-- *INF*: DECODE(CyberOneIncreasedLimitQuestionTwo,'T','1','F','0')
	DECODE(
	    CyberOneIncreasedLimitQuestionTwo,
	    'T', '1',
	    'F', '0'
	) AS CyberOneIncreasedLimitQuestionTwo_out,
	CyberOneIncreasedLimitQuestionThree,
	-- *INF*: DECODE(CyberOneIncreasedLimitQuestionThree,'T','1','F','0')
	DECODE(
	    CyberOneIncreasedLimitQuestionThree,
	    'T', '1',
	    'F', '0'
	) AS CyberOneIncreasedLimitQuestionThree_out,
	CyberOneIncreasedLimitQuestionFour,
	-- *INF*: DECODE(CyberOneIncreasedLimitQuestionFour,'T','1','F','0')
	DECODE(
	    CyberOneIncreasedLimitQuestionFour,
	    'T', '1',
	    'F', '0'
	) AS CyberOneIncreasedLimitQuestionFour_out,
	CyberOneIncreasedLimitQuestionFive,
	-- *INF*: DECODE(CyberOneIncreasedLimitQuestionFive,'T','1','F','0')
	DECODE(
	    CyberOneIncreasedLimitQuestionFive,
	    'T', '1',
	    'F', '0'
	) AS CyberOneIncreasedLimitQuestionFive_out,
	CyberOneIncreasedLimitQuestionSix,
	-- *INF*: DECODE(CyberOneIncreasedLimitQuestionSix,'T','1','F','0')
	DECODE(
	    CyberOneIncreasedLimitQuestionSix,
	    'T', '1',
	    'F', '0'
	) AS CyberOneIncreasedLimitQuestionSix_out,
	CyberOneIncreasedLimitQuestionSeven,
	-- *INF*: DECODE(CyberOneIncreasedLimitQuestionSeven,'T','1','F','0')
	DECODE(
	    CyberOneIncreasedLimitQuestionSeven,
	    'T', '1',
	    'F', '0'
	) AS CyberOneIncreasedLimitQuestionSeven_out,
	CyberOneIncreasedLimitQuestionEight,
	-- *INF*: DECODE(CyberOneIncreasedLimitQuestionEight,'T','1','F','0')
	DECODE(
	    CyberOneIncreasedLimitQuestionEight,
	    'T', '1',
	    'F', '0'
	) AS CyberOneIncreasedLimitQuestionEight_out,
	CyberOneIncreasedLimitQuestionNine,
	-- *INF*: DECODE(CyberOneIncreasedLimitQuestionNine,'T','1','F','0')
	DECODE(
	    CyberOneIncreasedLimitQuestionNine,
	    'T', '1',
	    'F', '0'
	) AS CyberOneIncreasedLimitQuestionNine_out,
	CyberOneIncreasedLimitQuestionTen,
	-- *INF*: DECODE(CyberOneIncreasedLimitQuestionTen,'T','1','F','0')
	DECODE(
	    CyberOneIncreasedLimitQuestionTen,
	    'T', '1',
	    'F', '0'
	) AS CyberOneIncreasedLimitQuestionTen_out,
	CyberOneIncreasedLimitQuestionEleven,
	-- *INF*: DECODE(CyberOneIncreasedLimitQuestionEleven,'T','1','F','0')
	DECODE(
	    CyberOneIncreasedLimitQuestionEleven,
	    'T', '1',
	    'F', '0'
	) AS CyberOneIncreasedLimitQuestionEleven_out,
	CyberOneIncreasedLimitQuestionTwelve,
	-- *INF*: DECODE(CyberOneIncreasedLimitQuestionTwelve,'T','1','F','0')
	DECODE(
	    CyberOneIncreasedLimitQuestionTwelve,
	    'T', '1',
	    'F', '0'
	) AS CyberOneIncreasedLimitQuestionTwelve_out,
	RatingTier
	FROM SQ_WB_CL_Line
),
WBCLLineStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCLLineStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCLLineStage
	(ExtractDate, SourceSystemId, WBLineId, WBCLLineId, SessionId, AuditablePremium, RiskGradePremises, RiskGradeProduct, RiskGradeSMARTOther, DescriptionForAuto, TaxAppliedToKYPremiumWritten, TaxAppliedToKYPremium, TaxAppliedToKYPremiumChange, IsAuditable, PreliminaryAudit, AuditPeriod, OverrideAuditReason, OverrideAudit, MidTermAudit, ArcheryNS0018, AerobicsNS0018, BaseballOrSoftballNS0018, BasketballNS0018, BicyclingNS0018, BowlingNS0018, CanoeingNS0018, CheerleadingNS0018, CurlingNS0018, CrossCountrySkiingNS0018, FencingNS0018, FieldHockeyNS0018, FlagFootballNS0018, FootballTouchNS0018, GolfNS0018, IceHockeyYouthNS0018, MartialArtsNS0018, OtherNS0018, OtherRow1NS0018, OtherRow2NS0018, RunningNS0018, SnowshoeingNS0018, SoccerNS0018, SpeedSkatingNS0018, SwimmingNS0018, TennisNS0018, VolleyballNS0018, WrestlingYouthNS0018, DateDescriptionOfEventsActivitiesOperationsLocationNS0034GLIL, DescriptionOfEventsNS0038, DateDescriptionOfEventsActivitiesOperationsLocationNS0034GL, DescriptionOfDesignatedOperationsNS0044GL, DescriptionOfOperationsSpecifiedLocationNS0044GL, DescriptionOfMobilEquipmentNS0065GL, DescriptionOfSpecificCircumstancesNS0108, NamesOfMunicipalitiesWB1882, CyberOneIndicator, CyberOnePremium, CyberOneEligibilityQuestion, CyberOneIncreasedLimitQuestionOne, CyberOneIncreasedLimitQuestionTwo, CyberOneIncreasedLimitQuestionThree, CyberOneIncreasedLimitQuestionFour, CyberOneIncreasedLimitQuestionFive, CyberOneIncreasedLimitQuestionSix, CyberOneIncreasedLimitQuestionSeven, CyberOneIncreasedLimitQuestionEight, CyberOneIncreasedLimitQuestionNine, CyberOneIncreasedLimitQuestionTen, CyberOneIncreasedLimitQuestionEleven, CyberOneIncreasedLimitQuestionTwelve, RatingTier)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	WB_LineId AS WBLINEID, 
	WB_CL_LineId AS WBCLLINEID, 
	SESSIONID, 
	AUDITABLEPREMIUM, 
	RISKGRADEPREMISES, 
	RISKGRADEPRODUCT, 
	RISKGRADESMARTOTHER, 
	DESCRIPTIONFORAUTO, 
	TAXAPPLIEDTOKYPREMIUMWRITTEN, 
	TAXAPPLIEDTOKYPREMIUM, 
	TAXAPPLIEDTOKYPREMIUMCHANGE, 
	o_IsAuditable AS ISAUDITABLE, 
	o_PreliminaryAudit AS PRELIMINARYAUDIT, 
	AUDITPERIOD, 
	OVERRIDEAUDITREASON, 
	OVERRIDEAUDIT, 
	o_MidTermAudit AS MIDTERMAUDIT, 
	ArcheryNS0018_out AS ARCHERYNS0018, 
	AerobicsNS0018_out AS AEROBICSNS0018, 
	BaseballOrSoftballNS0018_out AS BASEBALLORSOFTBALLNS0018, 
	BasketballNS0018_out AS BASKETBALLNS0018, 
	BicyclingNS0018_out AS BICYCLINGNS0018, 
	BowlingNS0018_out AS BOWLINGNS0018, 
	CanoeingNS0018_out AS CANOEINGNS0018, 
	CheerleadingNS0018_out AS CHEERLEADINGNS0018, 
	CurlingNS0018_out AS CURLINGNS0018, 
	CrossCountrySkiingNS0018_out AS CROSSCOUNTRYSKIINGNS0018, 
	FencingNS0018_out AS FENCINGNS0018, 
	FieldHockeyNS0018_out AS FIELDHOCKEYNS0018, 
	FlagFootballNS0018_out AS FLAGFOOTBALLNS0018, 
	FootballTouchNS0018_out AS FOOTBALLTOUCHNS0018, 
	GolfNS0018_out AS GOLFNS0018, 
	IceHockeyYouthNS0018_out AS ICEHOCKEYYOUTHNS0018, 
	MartialArtsNS0018_out AS MARTIALARTSNS0018, 
	OtherNS0018_out AS OTHERNS0018, 
	OTHERROW1NS0018, 
	OTHERROW2NS0018, 
	RunningNS0018_out AS RUNNINGNS0018, 
	SnowshoeingNS0018_out AS SNOWSHOEINGNS0018, 
	SoccerNS0018_out AS SOCCERNS0018, 
	SpeedSkatingNS0018_out AS SPEEDSKATINGNS0018, 
	SwimmingNS0018_out AS SWIMMINGNS0018, 
	TennisNS0018_out AS TENNISNS0018, 
	VolleyballNS0018_out AS VOLLEYBALLNS0018, 
	WrestlingYouthNS0018_out AS WRESTLINGYOUTHNS0018, 
	DATEDESCRIPTIONOFEVENTSACTIVITIESOPERATIONSLOCATIONNS0034GLIL, 
	DESCRIPTIONOFEVENTSNS0038, 
	DATEDESCRIPTIONOFEVENTSACTIVITIESOPERATIONSLOCATIONNS0034GL, 
	DESCRIPTIONOFDESIGNATEDOPERATIONSNS0044GL, 
	DESCRIPTIONOFOPERATIONSSPECIFIEDLOCATIONNS0044GL, 
	DESCRIPTIONOFMOBILEQUIPMENTNS0065GL, 
	DESCRIPTIONOFSPECIFICCIRCUMSTANCESNS0108, 
	NAMESOFMUNICIPALITIESWB1882, 
	CyberOneIndicator_out AS CYBERONEINDICATOR, 
	CYBERONEPREMIUM, 
	CyberOneEligibilityQuestion_out AS CYBERONEELIGIBILITYQUESTION, 
	CyberOneIncreasedLimitQuestionOne_out AS CYBERONEINCREASEDLIMITQUESTIONONE, 
	CyberOneIncreasedLimitQuestionTwo_out AS CYBERONEINCREASEDLIMITQUESTIONTWO, 
	CyberOneIncreasedLimitQuestionThree_out AS CYBERONEINCREASEDLIMITQUESTIONTHREE, 
	CyberOneIncreasedLimitQuestionFour_out AS CYBERONEINCREASEDLIMITQUESTIONFOUR, 
	CyberOneIncreasedLimitQuestionFive_out AS CYBERONEINCREASEDLIMITQUESTIONFIVE, 
	CyberOneIncreasedLimitQuestionSix_out AS CYBERONEINCREASEDLIMITQUESTIONSIX, 
	CyberOneIncreasedLimitQuestionSeven_out AS CYBERONEINCREASEDLIMITQUESTIONSEVEN, 
	CyberOneIncreasedLimitQuestionEight_out AS CYBERONEINCREASEDLIMITQUESTIONEIGHT, 
	CyberOneIncreasedLimitQuestionNine_out AS CYBERONEINCREASEDLIMITQUESTIONNINE, 
	CyberOneIncreasedLimitQuestionTen_out AS CYBERONEINCREASEDLIMITQUESTIONTEN, 
	CyberOneIncreasedLimitQuestionEleven_out AS CYBERONEINCREASEDLIMITQUESTIONELEVEN, 
	CyberOneIncreasedLimitQuestionTwelve_out AS CYBERONEINCREASEDLIMITQUESTIONTWELVE, 
	RATINGTIER
	FROM EXP_Metadata
),
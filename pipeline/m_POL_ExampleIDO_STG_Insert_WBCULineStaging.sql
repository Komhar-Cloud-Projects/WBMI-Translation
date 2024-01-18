WITH
SQ_WB_CU_Line AS (
	WITH cte_WBCULine(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CU_LineId, 
	X.WB_CU_LineId, 
	X.SessionId, 
	X.TreatyOrFacultative, 
	X.Terrorism, 
	X.AnnualPayroll, 
	X.AnnualGrossSales, 
	X.NumberOfEmployees, 
	X.Over4Vehicles, 
	X.Over5MillionSales, 
	X.Over2MillionSales, 
	X.ResidentialCondosApartmentsOver150Units, 
	X.MotelOver100Units, 
	X.LiquorLiabilityCoveredUnderWBM, 
	X.LineOfBusiness, 
	X.HigherLimitRequired, 
	X.HigherLimit, 
	X.PersonalAndAdvertisingInjuryCoverage, 
	X.PremiumDetailTotalBalanceToMeetMinimum, 
	X.PremiumDetailTotalBuiltUpPremium, 
	X.PremiumDetailTotalEndorsementPremium, 
	X.PremiumDetailTotalFinalPremium, 
	X.PremiumDetailTotalFinalPremiumWritten, 
	X.PremiumDetailTotalFinalPremiumChange 
	FROM
	WB_CU_Line X
	inner join
	cte_WBCULine Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CU_LineId,
	WB_CU_LineId,
	SessionId,
	TreatyOrFacultative,
	Terrorism,
	AnnualPayroll,
	AnnualGrossSales,
	NumberOfEmployees,
	Over4Vehicles,
	-- *INF*: DECODE(Over4Vehicles,'T',1,'F',0,NULL)
	DECODE(
	    Over4Vehicles,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS Over4Vehicles_out,
	Over5MillionSales,
	-- *INF*: DECODE(Over5MillionSales,'T',1,'F',0,NULL)
	DECODE(
	    Over5MillionSales,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS Over5MillionSales_out,
	Over2MillionSales,
	-- *INF*: DECODE(Over2MillionSales,'T',1,'F',0,NULL)
	DECODE(
	    Over2MillionSales,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS Over2MillionSales_out,
	ResidentialCondosApartmentsOver150Units,
	-- *INF*: DECODE(ResidentialCondosApartmentsOver150Units,'T',1,'F',0,NULL)
	DECODE(
	    ResidentialCondosApartmentsOver150Units,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS ResidentialCondosApartmentsOver150Units_out,
	MotelOver100Units,
	-- *INF*: DECODE(MotelOver100Units,'T',1,'F',0,NULL)
	DECODE(
	    MotelOver100Units,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS MotelOver100Units_out,
	LiquorLiabilityCoveredUnderWBM,
	-- *INF*: DECODE(LiquorLiabilityCoveredUnderWBM,'T',1,'F',0,NULL)
	DECODE(
	    LiquorLiabilityCoveredUnderWBM,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS LiquorLiabilityCoveredUnderWBM_out,
	LineOfBusiness,
	HigherLimitRequired,
	-- *INF*: DECODE(HigherLimitRequired,'T',1,'F',0,NULL)
	DECODE(
	    HigherLimitRequired,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS HigherLimitRequired_out,
	HigherLimit,
	PersonalAndAdvertisingInjuryCoverage,
	PremiumDetailTotalBalanceToMeetMinimum,
	PremiumDetailTotalBuiltUpPremium,
	PremiumDetailTotalEndorsementPremium,
	PremiumDetailTotalFinalPremium,
	PremiumDetailTotalFinalPremiumWritten,
	PremiumDetailTotalFinalPremiumChange
	FROM SQ_WB_CU_Line
),
WBCULineStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCULineStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCULineStaging
	(ExtractDate, SourceSystemId, CU_LineId, WB_CU_LineId, SessionId, TreatyOrFacultative, Terrorism, AnnualPayroll, AnnualGrossSales, NumberOfEmployees, Over4Vehicles, Over5MillionSales, Over2MillionSales, ResidentialCondosApartmentsOver150Units, MotelOver100Units, LiquorLiabilityCoveredUnderWBM, LineOfBusiness, HigherLimitRequired, HigherLimit, PersonalAndAdvertisingInjuryCoverage, PremiumDetailTotalBalanceToMeetMinimum, PremiumDetailTotalBuiltUpPremium, PremiumDetailTotalEndorsementPremium, PremiumDetailTotalFinalPremium, PremiumDetailTotalFinalPremiumWritten, PremiumDetailTotalFinalPremiumChange)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CU_LINEID, 
	WB_CU_LINEID, 
	SESSIONID, 
	TREATYORFACULTATIVE, 
	TERRORISM, 
	ANNUALPAYROLL, 
	ANNUALGROSSSALES, 
	NUMBEROFEMPLOYEES, 
	Over4Vehicles_out AS OVER4VEHICLES, 
	Over5MillionSales_out AS OVER5MILLIONSALES, 
	Over2MillionSales_out AS OVER2MILLIONSALES, 
	ResidentialCondosApartmentsOver150Units_out AS RESIDENTIALCONDOSAPARTMENTSOVER150UNITS, 
	MotelOver100Units_out AS MOTELOVER100UNITS, 
	LiquorLiabilityCoveredUnderWBM_out AS LIQUORLIABILITYCOVEREDUNDERWBM, 
	LINEOFBUSINESS, 
	HigherLimitRequired_out AS HIGHERLIMITREQUIRED, 
	HIGHERLIMIT, 
	PERSONALANDADVERTISINGINJURYCOVERAGE, 
	PREMIUMDETAILTOTALBALANCETOMEETMINIMUM, 
	PREMIUMDETAILTOTALBUILTUPPREMIUM, 
	PREMIUMDETAILTOTALENDORSEMENTPREMIUM, 
	PREMIUMDETAILTOTALFINALPREMIUM, 
	PREMIUMDETAILTOTALFINALPREMIUMWRITTEN, 
	PREMIUMDETAILTOTALFINALPREMIUMCHANGE
	FROM EXP_Metadata
),
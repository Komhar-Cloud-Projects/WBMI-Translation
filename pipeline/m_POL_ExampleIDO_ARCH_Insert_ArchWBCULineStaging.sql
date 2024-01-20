WITH
SQ_WBCULineStaging AS (
	SELECT
		WBCULineStagingId,
		ExtractDate,
		SourceSystemId,
		CU_LineId,
		WB_CU_LineId,
		SessionId,
		TreatyOrFacultative,
		Terrorism,
		AnnualPayroll,
		AnnualGrossSales,
		NumberOfEmployees,
		Over4Vehicles,
		Over5MillionSales,
		Over2MillionSales,
		ResidentialCondosApartmentsOver150Units,
		MotelOver100Units,
		LiquorLiabilityCoveredUnderWBM,
		LineOfBusiness,
		HigherLimitRequired,
		HigherLimit,
		PersonalAndAdvertisingInjuryCoverage,
		PremiumDetailTotalBalanceToMeetMinimum,
		PremiumDetailTotalBuiltUpPremium,
		PremiumDetailTotalEndorsementPremium,
		PremiumDetailTotalFinalPremium,
		PremiumDetailTotalFinalPremiumWritten,
		PremiumDetailTotalFinalPremiumChange
	FROM WBCULineStaging
),
EXP_Metadata AS (
	SELECT
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
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
	FROM SQ_WBCULineStaging
),
archWBCULineStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archWBCULineStaging
	(ExtractDate, SourceSystemId, AuditId, CU_LineId, WB_CU_LineId, SessionId, TreatyOrFacultative, Terrorism, AnnualPayroll, AnnualGrossSales, NumberOfEmployees, Over4Vehicles, Over5MillionSales, Over2MillionSales, ResidentialCondosApartmentsOver150Units, MotelOver100Units, LiquorLiabilityCoveredUnderWBM, LineOfBusiness, HigherLimitRequired, HigherLimit, PersonalAndAdvertisingInjuryCoverage, PremiumDetailTotalBalanceToMeetMinimum, PremiumDetailTotalBuiltUpPremium, PremiumDetailTotalEndorsementPremium, PremiumDetailTotalFinalPremium, PremiumDetailTotalFinalPremiumWritten, PremiumDetailTotalFinalPremiumChange)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID, 
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
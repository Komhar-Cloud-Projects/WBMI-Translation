WITH
SQ_Shortcut_to_WorkClaimsCentersOfExpertiseExtract AS (
	SELECT WorkClaimsCentersOfExpertiseExtract.ClaimNumber,
		WorkClaimsCentersOfExpertiseExtract.SubClaimId,
		WorkClaimsCentersOfExpertiseExtract.Claimtype,
		WorkClaimsCentersOfExpertiseExtract.CoverageType,
		WorkClaimsCentersOfExpertiseExtract.STATUS,
		WorkClaimsCentersOfExpertiseExtract.PolicyNumber,
		WorkClaimsCentersOfExpertiseExtract.LossTotal,
		WorkClaimsCentersOfExpertiseExtract.ExpenseTotal,
		WorkClaimsCentersOfExpertiseExtract.SalvageTotal,
		WorkClaimsCentersOfExpertiseExtract.SubrogationTotal,
		WorkClaimsCentersOfExpertiseExtract.LossDate,
		WorkClaimsCentersOfExpertiseExtract.OpenedDate,
		WorkClaimsCentersOfExpertiseExtract.ClosedDate,
		WorkClaimsCentersOfExpertiseExtract.AdjusterName,
		WorkClaimsCentersOfExpertiseExtract.AdjusterCode,
		WorkClaimsCentersOfExpertiseExtract.SupervisorCode,
		WorkClaimsCentersOfExpertiseExtract.SupervisorName,
		WorkClaimsCentersOfExpertiseExtract.OfficeCode,
		WorkClaimsCentersOfExpertiseExtract.OfficeName,
		WorkClaimsCentersOfExpertiseExtract.UnitName,
		WorkClaimsCentersOfExpertiseExtract.UnitCode,
		WorkClaimsCentersOfExpertiseExtract.ClaimantName,
		WorkClaimsCentersOfExpertiseExtract.LossIndemnity,
		WorkClaimsCentersOfExpertiseExtract.LossMedical,
		WorkClaimsCentersOfExpertiseExtract.LegalIndicator,
		WorkClaimsCentersOfExpertiseExtract.LossState,
		WorkClaimsCentersOfExpertiseExtract.PolicyType,
		WorkClaimsCentersOfExpertiseExtract.LossType,
		WorkClaimsCentersOfExpertiseExtract.CompanyName,
		WorkClaimsCentersOfExpertiseExtract.Jurisdiction,
		WorkClaimsCentersOfExpertiseExtract.LossMiscellaneous1,
		WorkClaimsCentersOfExpertiseExtract.LossReserveTotal,
		WorkClaimsCentersOfExpertiseExtract.LossReserveMiscellaneous1,
		WorkClaimsCentersOfExpertiseExtract.ExpenseMiscellaneous1,
		WorkClaimsCentersOfExpertiseExtract.ExpenseReserveTotal,
		WorkClaimsCentersOfExpertiseExtract.ExpenseReserveMiscellaneous1,
		WorkClaimsCentersOfExpertiseExtract.AdjusterEmail,
		WorkClaimsCentersOfExpertiseExtract.SupervisorEmail,
		WorkClaimsCentersOfExpertiseExtract.LossPostal,
		WorkClaimsCentersOfExpertiseExtract.CategoryIndicator,
		WorkClaimsCentersOfExpertiseExtract.InsuredName,
		WorkClaimsCentersOfExpertiseExtract.SalvageIndicator,
		WorkClaimsCentersOfExpertiseExtract.SubrogationIndicator,
		WorkClaimsCentersOfExpertiseExtract.ExAdjusterIndicator,
		WorkClaimsCentersOfExpertiseExtract.GeneralMiscellaneous1,
		WorkClaimsCentersOfExpertiseExtract.GeneralMiscellaneous2,
		WorkClaimsCentersOfExpertiseExtract.GeneralMiscellaneous3,
		WorkClaimsCentersOfExpertiseExtract.GeneralMiscellaneous4,
		WorkClaimsCentersOfExpertiseExtract.GeneralMiscellaneous5,
		WorkClaimsCentersOfExpertiseExtract.GeneralMiscellaneous6,
		WorkClaimsCentersOfExpertiseExtract.GeneralMiscellaneous7,
		WorkClaimsCentersOfExpertiseExtract.GeneralMiscellaneous8,
		WorkClaimsCentersOfExpertiseExtract.GeneralMiscellaneous9,
		WorkClaimsCentersOfExpertiseExtract.GeneralMiscellaneous10,
		WorkClaimsCentersOfExpertiseExtract.AgencyCode,
		WorkClaimsCentersOfExpertiseExtract.ClaimFeatureRepresentativeCode,
		WorkClaimsCentersOfExpertiseExtract.ClaimFeatureRepresentativeName
		, WorkClaimsCentersOfExpertiseExtract.LossDescription
		, WorkClaimsCentersOfExpertiseExtract.DateNotification
		, WorkClaimsCentersOfExpertiseExtract.PolicyEffectiveDate
		, WorkClaimsCentersOfExpertiseExtract.PolicyTerminationDate
	      ,WorkClaimsCentersOfExpertiseExtract.SalvageReserveAmount
	      ,WorkClaimsCentersOfExpertiseExtract.SubrogationReserveAmount
	      ,WorkClaimsCentersOfExpertiseExtract.ExpenseIncurred
	      ,WorkClaimsCentersOfExpertiseExtract.LossIncurred
	     ,WorkClaimsCentersOfExpertiseExtract.SalvageIncurred
	     ,WorkClaimsCentersOfExpertiseExtract.SubrogationIncurred
	FROM WorkClaimsCentersOfExpertiseExtract
	WHERE AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
),
EXP_Derivefields AS (
	SELECT
	ClaimNumber,
	-- *INF*: IIF(LTRIM(RTRIM(ClaimNumber))= 'N/A', '',LTRIM(RTRIM(ClaimNumber)))
	IFF(LTRIM(RTRIM(ClaimNumber)) = 'N/A', '', LTRIM(RTRIM(ClaimNumber))) AS ClaimNumber_o,
	SubClaimId,
	-- *INF*: IIF(LTRIM(RTRIM(SubClaimId))= 'N/A', '',LTRIM(RTRIM(SubClaimId)))
	IFF(LTRIM(RTRIM(SubClaimId)) = 'N/A', '', LTRIM(RTRIM(SubClaimId))) AS SubClaimId_o,
	Claimtype,
	-- *INF*: IIF(LTRIM(RTRIM(Claimtype))= 'N/A', '',LTRIM(RTRIM(Claimtype)))
	IFF(LTRIM(RTRIM(Claimtype)) = 'N/A', '', LTRIM(RTRIM(Claimtype))) AS Claimtype_o,
	CoverageType,
	-- *INF*: IIF(LTRIM(RTRIM(CoverageType))= 'N/A', '',LTRIM(RTRIM(CoverageType)))
	IFF(LTRIM(RTRIM(CoverageType)) = 'N/A', '', LTRIM(RTRIM(CoverageType))) AS CoverageType_o,
	Status,
	-- *INF*: IIF(LTRIM(RTRIM(Status))= 'N/A', '',LTRIM(RTRIM(Status)))
	IFF(LTRIM(RTRIM(Status)) = 'N/A', '', LTRIM(RTRIM(Status))) AS Status_o,
	PolicyNumber,
	-- *INF*: IIF(LTRIM(RTRIM(PolicyNumber))= 'N/A', '',LTRIM(RTRIM(PolicyNumber)))
	IFF(LTRIM(RTRIM(PolicyNumber)) = 'N/A', '', LTRIM(RTRIM(PolicyNumber))) AS PolicyNumber_o,
	LossTotal,
	-- *INF*: IIF(LTRIM(RTRIM(v_LossTotal))= 'N/A', '',LTRIM(RTRIM(v_LossTotal)))
	IFF(LTRIM(RTRIM(v_LossTotal)) = 'N/A', '', LTRIM(RTRIM(v_LossTotal))) AS LossTotal_o,
	-- *INF*: IIF(LossTotal<0,'('||REPLACECHR(0,TO_CHAR(LossTotal),'-','')||')',TO_CHAR(LossTotal)
	-- )
	IFF(
	    LossTotal < 0, '(' || REGEXP_REPLACE(TO_CHAR(LossTotal),'-','','i') || ')',
	    TO_CHAR(LossTotal)
	) AS v_LossTotal,
	ExpenseTotal,
	-- *INF*: IIF(LTRIM(RTRIM(v_ExpenseTotal))= 'N/A', '',LTRIM(RTRIM(v_ExpenseTotal)))
	IFF(LTRIM(RTRIM(v_ExpenseTotal)) = 'N/A', '', LTRIM(RTRIM(v_ExpenseTotal))) AS ExpenseTotal_o,
	-- *INF*: IIF(ExpenseTotal<0,'('||REPLACECHR(0,TO_CHAR(ExpenseTotal),'-','')||')',TO_CHAR(ExpenseTotal)
	-- )
	IFF(
	    ExpenseTotal < 0, '(' || REGEXP_REPLACE(TO_CHAR(ExpenseTotal),'-','','i') || ')',
	    TO_CHAR(ExpenseTotal)
	) AS v_ExpenseTotal,
	SalvageTotal,
	-- *INF*: IIF(LTRIM(RTRIM(v_salvagetotal))= 'N/A', '',LTRIM(RTRIM(v_salvagetotal)))
	IFF(LTRIM(RTRIM(v_salvagetotal)) = 'N/A', '', LTRIM(RTRIM(v_salvagetotal))) AS SalvageTotal_o,
	-- *INF*: IIF(SalvageTotal<0,'('||REPLACECHR(0,TO_CHAR(SalvageTotal),'-','')||')',TO_CHAR(SalvageTotal)
	-- )
	IFF(
	    SalvageTotal < 0, '(' || REGEXP_REPLACE(TO_CHAR(SalvageTotal),'-','','i') || ')',
	    TO_CHAR(SalvageTotal)
	) AS v_salvagetotal,
	SubrogationTotal,
	-- *INF*: IIF(LTRIM(RTRIM(v_SubrogationTotal))= 'N/A', '',LTRIM(RTRIM(v_SubrogationTotal)))
	IFF(LTRIM(RTRIM(v_SubrogationTotal)) = 'N/A', '', LTRIM(RTRIM(v_SubrogationTotal))) AS SubrogationTotal_o,
	-- *INF*: IIF(SubrogationTotal<0,'('||REPLACECHR(0,TO_CHAR(SubrogationTotal),'-','')||')',TO_CHAR(SubrogationTotal)
	-- )
	IFF(
	    SubrogationTotal < 0, '(' || REGEXP_REPLACE(TO_CHAR(SubrogationTotal),'-','','i') || ')',
	    TO_CHAR(SubrogationTotal)
	) AS v_SubrogationTotal,
	LossDate,
	-- *INF*: TO_CHAR(LossDate,'MM/DD/YYYY')
	TO_CHAR(LossDate, 'MM/DD/YYYY') AS LossDate_o,
	OpenedDate,
	-- *INF*: TO_CHAR(OpenedDate,'MM/DD/YYYY')
	TO_CHAR(OpenedDate, 'MM/DD/YYYY') AS OpendDate_o,
	ClosedDate,
	-- *INF*: TO_CHAR(ClosedDate,'MM/DD/YYYY')
	TO_CHAR(ClosedDate, 'MM/DD/YYYY') AS CloseDate_o,
	AdjusterName,
	-- *INF*: IIF(LTRIM(RTRIM(AdjusterName))= 'N/A', '',LTRIM(RTRIM(AdjusterName)))
	IFF(LTRIM(RTRIM(AdjusterName)) = 'N/A', '', LTRIM(RTRIM(AdjusterName))) AS AdjusterName_o,
	AdjusterCode,
	-- *INF*: IIF(LTRIM(RTRIM(AdjusterCode))= 'N/A', '',LTRIM(RTRIM(AdjusterCode)))
	IFF(LTRIM(RTRIM(AdjusterCode)) = 'N/A', '', LTRIM(RTRIM(AdjusterCode))) AS AdjusterCode_o,
	SupervisorCode,
	-- *INF*: IIF(LTRIM(RTRIM(SupervisorCode))= 'N/A', '',LTRIM(RTRIM(SupervisorCode)))
	IFF(LTRIM(RTRIM(SupervisorCode)) = 'N/A', '', LTRIM(RTRIM(SupervisorCode))) AS SupervisorCode_o,
	SupervisorName,
	-- *INF*: IIF(LTRIM(RTRIM(SupervisorName))= 'N/A', '',LTRIM(RTRIM(SupervisorName)))
	IFF(LTRIM(RTRIM(SupervisorName)) = 'N/A', '', LTRIM(RTRIM(SupervisorName))) AS SupervisorName_o,
	OfficeCode,
	-- *INF*: IIF(LTRIM(RTRIM(OfficeCode))= 'N/A', '',LTRIM(RTRIM(OfficeCode)))
	IFF(LTRIM(RTRIM(OfficeCode)) = 'N/A', '', LTRIM(RTRIM(OfficeCode))) AS OfficeCode_o,
	OfficeName,
	-- *INF*: IIF(LTRIM(RTRIM(OfficeName))= 'N/A', '',LTRIM(RTRIM(OfficeName)))
	IFF(LTRIM(RTRIM(OfficeName)) = 'N/A', '', LTRIM(RTRIM(OfficeName))) AS OfficeName_o,
	UnitName,
	-- *INF*: IIF(LTRIM(RTRIM(UnitName))= 'N/A', '',LTRIM(RTRIM(UnitName)))
	IFF(LTRIM(RTRIM(UnitName)) = 'N/A', '', LTRIM(RTRIM(UnitName))) AS UnitName_o,
	UnitCode,
	-- *INF*: IIF(LTRIM(RTRIM(UnitCode))= 'N/A', '',LTRIM(RTRIM(UnitCode)))
	IFF(LTRIM(RTRIM(UnitCode)) = 'N/A', '', LTRIM(RTRIM(UnitCode))) AS UnitCode_o,
	ClaimantName,
	-- *INF*: IIF(LTRIM(RTRIM(ClaimantName))= 'N/A', '',LTRIM(RTRIM(ClaimantName)))
	IFF(LTRIM(RTRIM(ClaimantName)) = 'N/A', '', LTRIM(RTRIM(ClaimantName))) AS ClaimantName_o,
	LossIndemnity,
	-- *INF*: IIF(LTRIM(RTRIM(v_LossIndemnity))= 'N/A', '',LTRIM(RTRIM(v_LossIndemnity)))
	IFF(LTRIM(RTRIM(v_LossIndemnity)) = 'N/A', '', LTRIM(RTRIM(v_LossIndemnity))) AS LossIndemnity_o,
	-- *INF*: IIF(LossIndemnity<0,'('||REPLACECHR(0,TO_CHAR(LossIndemnity),'-','')||')',TO_CHAR(LossIndemnity)
	-- )
	IFF(
	    LossIndemnity < 0, '(' || REGEXP_REPLACE(TO_CHAR(LossIndemnity),'-','','i') || ')',
	    TO_CHAR(LossIndemnity)
	) AS v_LossIndemnity,
	LossMedical,
	-- *INF*: IIF(LTRIM(RTRIM(V_LossMedical))= 'N/A', '',LTRIM(RTRIM(V_LossMedical)))
	IFF(LTRIM(RTRIM(V_LossMedical)) = 'N/A', '', LTRIM(RTRIM(V_LossMedical))) AS LossMedical_o,
	-- *INF*: IIF(LossMedical<0,'('||REPLACECHR(0,TO_CHAR(LossMedical),'-','')||')',TO_CHAR(LossMedical)
	-- )
	IFF(
	    LossMedical < 0, '(' || REGEXP_REPLACE(TO_CHAR(LossMedical),'-','','i') || ')',
	    TO_CHAR(LossMedical)
	) AS V_LossMedical,
	LegalIndicator,
	-- *INF*: IIF(LTRIM(RTRIM(LegalIndicator))= 'N/A', '',LTRIM(RTRIM(LegalIndicator)))
	IFF(LTRIM(RTRIM(LegalIndicator)) = 'N/A', '', LTRIM(RTRIM(LegalIndicator))) AS LegalIndicator_o,
	LossState,
	-- *INF*: IIF(LTRIM(RTRIM(LossState))= 'N/A', '',LTRIM(RTRIM(LossState)))
	IFF(LTRIM(RTRIM(LossState)) = 'N/A', '', LTRIM(RTRIM(LossState))) AS LossState_o,
	PolicyType,
	-- *INF*: IIF(LTRIM(RTRIM(PolicyType))= 'N/A', '',LTRIM(RTRIM(PolicyType)))
	IFF(LTRIM(RTRIM(PolicyType)) = 'N/A', '', LTRIM(RTRIM(PolicyType))) AS PolicyType_o,
	LossType,
	-- *INF*: IIF(LTRIM(RTRIM(LossType))= 'N/A', '',LTRIM(RTRIM(LossType)))
	IFF(LTRIM(RTRIM(LossType)) = 'N/A', '', LTRIM(RTRIM(LossType))) AS LossType_o,
	CompanyName,
	-- *INF*: IIF(LTRIM(RTRIM(CompanyName))= 'N/A', '',LTRIM(RTRIM(CompanyName)))
	IFF(LTRIM(RTRIM(CompanyName)) = 'N/A', '', LTRIM(RTRIM(CompanyName))) AS CompanyName_o,
	Jurisdiction,
	-- *INF*: IIF(LTRIM(RTRIM(Jurisdiction))= 'N/A', '',LTRIM(RTRIM(Jurisdiction)))
	IFF(LTRIM(RTRIM(Jurisdiction)) = 'N/A', '', LTRIM(RTRIM(Jurisdiction))) AS Jurisdiction_o,
	LossMiscellaneous1,
	-- *INF*: IIF(LTRIM(RTRIM(v_LossMiscellaneous1))= 'N/A', '',LTRIM(RTRIM(v_LossMiscellaneous1)))
	IFF(LTRIM(RTRIM(v_LossMiscellaneous1)) = 'N/A', '', LTRIM(RTRIM(v_LossMiscellaneous1))) AS LossMiscellaneous1_o,
	-- *INF*: IIF(LossMiscellaneous1<0,'('||REPLACECHR(0,TO_CHAR(LossMiscellaneous1),'-','')||')',TO_CHAR(LossMiscellaneous1)
	-- )
	IFF(
	    LossMiscellaneous1 < 0, '(' || REGEXP_REPLACE(TO_CHAR(LossMiscellaneous1),'-','','i') || ')',
	    TO_CHAR(LossMiscellaneous1)
	) AS v_LossMiscellaneous1,
	LossReserveTotal,
	-- *INF*: IIF(LTRIM(RTRIM(v_LossReserveTotal))= 'N/A', '',LTRIM(RTRIM(v_LossReserveTotal)))
	IFF(LTRIM(RTRIM(v_LossReserveTotal)) = 'N/A', '', LTRIM(RTRIM(v_LossReserveTotal))) AS LossReserveTotal_o,
	-- *INF*: IIF(LossReserveTotal<0,'('||REPLACECHR(0,TO_CHAR(LossReserveTotal),'-','')||')',TO_CHAR(LossReserveTotal)
	-- )
	IFF(
	    LossReserveTotal < 0, '(' || REGEXP_REPLACE(TO_CHAR(LossReserveTotal),'-','','i') || ')',
	    TO_CHAR(LossReserveTotal)
	) AS v_LossReserveTotal,
	LossReserveMiscellaneous1,
	-- *INF*: IIF(LTRIM(RTRIM(v_LossReserveMiscellaneous1))= 'N/A', '',LTRIM(RTRIM(v_LossReserveMiscellaneous1)))
	IFF(
	    LTRIM(RTRIM(v_LossReserveMiscellaneous1)) = 'N/A', '',
	    LTRIM(RTRIM(v_LossReserveMiscellaneous1))
	) AS LossReserveMiscellaneous1_o,
	-- *INF*: IIF(LossReserveMiscellaneous1<0,'('||REPLACECHR(0,TO_CHAR(LossReserveMiscellaneous1),'-','')||')',TO_CHAR(LossReserveMiscellaneous1)
	-- )
	IFF(
	    LossReserveMiscellaneous1 < 0,
	    '(' || REGEXP_REPLACE(TO_CHAR(LossReserveMiscellaneous1),'-','','i') || ')',
	    TO_CHAR(LossReserveMiscellaneous1)
	) AS v_LossReserveMiscellaneous1,
	ExpenseMiscellaneous1,
	-- *INF*: IIF(LTRIM(RTRIM(v_ExpenseMiscellaneous1))= 'N/A', '',LTRIM(RTRIM(v_ExpenseMiscellaneous1)))
	IFF(
	    LTRIM(RTRIM(v_ExpenseMiscellaneous1)) = 'N/A', '', LTRIM(RTRIM(v_ExpenseMiscellaneous1))
	) AS ExpenseMiscellaneous1_o,
	-- *INF*: IIF(ExpenseMiscellaneous1<0,'('||REPLACECHR(0,TO_CHAR(ExpenseMiscellaneous1),'-','')||')',TO_CHAR(ExpenseMiscellaneous1))
	IFF(
	    ExpenseMiscellaneous1 < 0,
	    '(' || REGEXP_REPLACE(TO_CHAR(ExpenseMiscellaneous1),'-','','i') || ')',
	    TO_CHAR(ExpenseMiscellaneous1)
	) AS v_ExpenseMiscellaneous1,
	ExpenseReserveTotal,
	-- *INF*: IIF(LTRIM(RTRIM(v_ExpenseReserveTotal))= 'N/A', '',LTRIM(RTRIM(v_ExpenseReserveTotal)))
	IFF(LTRIM(RTRIM(v_ExpenseReserveTotal)) = 'N/A', '', LTRIM(RTRIM(v_ExpenseReserveTotal))) AS ExpenseReserveTotal_o,
	-- *INF*: IIF(ExpenseReserveTotal<0,'('||REPLACECHR(0,TO_CHAR(ExpenseReserveTotal),'-','')||')',TO_CHAR(ExpenseReserveTotal)
	-- )
	IFF(
	    ExpenseReserveTotal < 0,
	    '(' || REGEXP_REPLACE(TO_CHAR(ExpenseReserveTotal),'-','','i') || ')',
	    TO_CHAR(ExpenseReserveTotal)
	) AS v_ExpenseReserveTotal,
	ExpenseReserveMiscellaneous1,
	-- *INF*: IIF(LTRIM(RTRIM(v_ExpenseReserveMiscellaneous1))= 'N/A', '',LTRIM(RTRIM(v_ExpenseReserveMiscellaneous1)))
	IFF(
	    LTRIM(RTRIM(v_ExpenseReserveMiscellaneous1)) = 'N/A', '',
	    LTRIM(RTRIM(v_ExpenseReserveMiscellaneous1))
	) AS ExpenseReserveMiscellaneous1_o,
	-- *INF*: IIF(ExpenseReserveMiscellaneous1<0,'('||REPLACECHR(0,TO_CHAR(ExpenseReserveMiscellaneous1),'-','')||')',TO_CHAR(ExpenseReserveMiscellaneous1)
	-- )
	IFF(
	    ExpenseReserveMiscellaneous1 < 0,
	    '(' || REGEXP_REPLACE(TO_CHAR(ExpenseReserveMiscellaneous1),'-','','i') || ')',
	    TO_CHAR(ExpenseReserveMiscellaneous1)
	) AS v_ExpenseReserveMiscellaneous1,
	AdjusterEmail,
	-- *INF*: IIF(LTRIM(RTRIM(AdjusterEmail))= 'N/A', '',LTRIM(RTRIM(AdjusterEmail)))
	IFF(LTRIM(RTRIM(AdjusterEmail)) = 'N/A', '', LTRIM(RTRIM(AdjusterEmail))) AS AdjusterEmail_o,
	SupervisorEmail,
	-- *INF*: IIF(LTRIM(RTRIM(SupervisorEmail))= 'N/A', '',LTRIM(RTRIM(SupervisorEmail)))
	IFF(LTRIM(RTRIM(SupervisorEmail)) = 'N/A', '', LTRIM(RTRIM(SupervisorEmail))) AS SupervisorEmail_o,
	LossPostal,
	-- *INF*: IIF(LTRIM(RTRIM(LossPostal))= 'N/A', '',LTRIM(RTRIM(LossPostal)))
	IFF(LTRIM(RTRIM(LossPostal)) = 'N/A', '', LTRIM(RTRIM(LossPostal))) AS LossPostal_o,
	CategoryIndicator,
	-- *INF*: IIF(LTRIM(RTRIM(CategoryIndicator))= 'N/A', '',LTRIM(RTRIM(CategoryIndicator)))
	IFF(LTRIM(RTRIM(CategoryIndicator)) = 'N/A', '', LTRIM(RTRIM(CategoryIndicator))) AS CategoryIndicator_o,
	InsuredName,
	-- *INF*: IIF(LTRIM(RTRIM(InsuredName))= 'N/A', '',LTRIM(RTRIM(InsuredName)))
	IFF(LTRIM(RTRIM(InsuredName)) = 'N/A', '', LTRIM(RTRIM(InsuredName))) AS InsuredName_o,
	SalvageIndicator,
	-- *INF*: IIF(LTRIM(RTRIM(SalvageIndicator))= 'N/A', '',LTRIM(RTRIM(SalvageIndicator)))
	IFF(LTRIM(RTRIM(SalvageIndicator)) = 'N/A', '', LTRIM(RTRIM(SalvageIndicator))) AS SalvageIndicator_o,
	SubrogationIndicator,
	-- *INF*: IIF(LTRIM(RTRIM(SubrogationIndicator))= 'N/A', '',LTRIM(RTRIM(SubrogationIndicator)))
	IFF(LTRIM(RTRIM(SubrogationIndicator)) = 'N/A', '', LTRIM(RTRIM(SubrogationIndicator))) AS SubrogationIndicator_o,
	ExAdjusterIndicator,
	-- *INF*: IIF(LTRIM(RTRIM(ExAdjusterIndicator))= 'N/A', '',LTRIM(RTRIM(ExAdjusterIndicator)))
	IFF(LTRIM(RTRIM(ExAdjusterIndicator)) = 'N/A', '', LTRIM(RTRIM(ExAdjusterIndicator))) AS ExAdjusterIndicator_o,
	GeneralMiscellaneous1,
	-- *INF*: IIF(LTRIM(RTRIM(GeneralMiscellaneous1))= 'N/A', '',LTRIM(RTRIM(GeneralMiscellaneous1)))
	IFF(LTRIM(RTRIM(GeneralMiscellaneous1)) = 'N/A', '', LTRIM(RTRIM(GeneralMiscellaneous1))) AS GeneralMiscellaneous1_o,
	GeneralMiscellaneous2,
	-- *INF*: IIF(LTRIM(RTRIM(GeneralMiscellaneous2))= 'N/A', '',LTRIM(RTRIM(GeneralMiscellaneous2)))
	IFF(LTRIM(RTRIM(GeneralMiscellaneous2)) = 'N/A', '', LTRIM(RTRIM(GeneralMiscellaneous2))) AS GeneralMiscellaneous2_o,
	GeneralMiscellaneous3,
	-- *INF*: IIF(LTRIM(RTRIM(GeneralMiscellaneous3))= 'N/A', '',LTRIM(RTRIM(GeneralMiscellaneous3)))
	IFF(LTRIM(RTRIM(GeneralMiscellaneous3)) = 'N/A', '', LTRIM(RTRIM(GeneralMiscellaneous3))) AS GeneralMiscellaneous3_o,
	GeneralMiscellaneous4,
	-- *INF*: IIF(LTRIM(RTRIM(GeneralMiscellaneous4))= 'N/A', '',LTRIM(RTRIM(GeneralMiscellaneous4)))
	IFF(LTRIM(RTRIM(GeneralMiscellaneous4)) = 'N/A', '', LTRIM(RTRIM(GeneralMiscellaneous4))) AS GeneralMiscellaneous4_o,
	GeneralMiscellaneous5,
	-- *INF*: IIF(LTRIM(RTRIM(GeneralMiscellaneous5))= 'N/A', '',LTRIM(RTRIM(GeneralMiscellaneous5)))
	IFF(LTRIM(RTRIM(GeneralMiscellaneous5)) = 'N/A', '', LTRIM(RTRIM(GeneralMiscellaneous5))) AS GeneralMiscellaneous5_o,
	GeneralMiscellaneous6,
	-- *INF*: IIF(LTRIM(RTRIM(GeneralMiscellaneous6))= 'N/A', '',LTRIM(RTRIM(GeneralMiscellaneous6)))
	IFF(LTRIM(RTRIM(GeneralMiscellaneous6)) = 'N/A', '', LTRIM(RTRIM(GeneralMiscellaneous6))) AS GeneralMiscellaneous6_o,
	GeneralMiscellaneous7,
	-- *INF*: IIF(LTRIM(RTRIM(GeneralMiscellaneous7))= 'N/A', '',LTRIM(RTRIM(GeneralMiscellaneous7)))
	IFF(LTRIM(RTRIM(GeneralMiscellaneous7)) = 'N/A', '', LTRIM(RTRIM(GeneralMiscellaneous7))) AS GeneralMiscellaneous7_o,
	GeneralMiscellaneous8,
	-- *INF*: IIF(LTRIM(RTRIM(GeneralMiscellaneous8))= 'N/A', '',LTRIM(RTRIM(GeneralMiscellaneous8)))
	IFF(LTRIM(RTRIM(GeneralMiscellaneous8)) = 'N/A', '', LTRIM(RTRIM(GeneralMiscellaneous8))) AS GeneralMiscellaneous8_o,
	GeneralMiscellaneous9,
	-- *INF*: IIF(LTRIM(RTRIM(GeneralMiscellaneous9))= 'N/A', '',LTRIM(RTRIM(GeneralMiscellaneous9)))
	IFF(LTRIM(RTRIM(GeneralMiscellaneous9)) = 'N/A', '', LTRIM(RTRIM(GeneralMiscellaneous9))) AS GeneralMiscellaneous9_o,
	GeneralMiscellaneous10,
	-- *INF*: IIF(LTRIM(RTRIM(GeneralMiscellaneous10))= 'N/A', '',LTRIM(RTRIM(GeneralMiscellaneous10)))
	IFF(LTRIM(RTRIM(GeneralMiscellaneous10)) = 'N/A', '', LTRIM(RTRIM(GeneralMiscellaneous10))) AS GeneralMiscellaneous10_o,
	AgencyCode,
	-- *INF*: IIF(LTRIM(RTRIM(AgencyCode))= 'N/A', '',LTRIM(RTRIM(AgencyCode)))
	IFF(LTRIM(RTRIM(AgencyCode)) = 'N/A', '', LTRIM(RTRIM(AgencyCode))) AS o_AgencyCode,
	ClaimFeatureRepresentativeCode,
	-- *INF*: IIF(LTRIM(RTRIM(ClaimFeatureRepresentativeCode))= 'N/A', '',LTRIM(RTRIM(ClaimFeatureRepresentativeCode)))
	-- 
	IFF(
	    LTRIM(RTRIM(ClaimFeatureRepresentativeCode)) = 'N/A', '',
	    LTRIM(RTRIM(ClaimFeatureRepresentativeCode))
	) AS o_ClaimFeatureRepresentativeCode,
	ClaimFeatureRepresentativeName,
	-- *INF*: IIF(LTRIM(RTRIM(ClaimFeatureRepresentativeName))= 'N/A', '',LTRIM(RTRIM(ClaimFeatureRepresentativeName)))
	IFF(
	    LTRIM(RTRIM(ClaimFeatureRepresentativeName)) = 'N/A', '',
	    LTRIM(RTRIM(ClaimFeatureRepresentativeName))
	) AS o_ClaimFeatureRepresentativeName,
	LossDescription,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(LossDescription)
	UDF_DEFAULT_VALUE_FOR_STRINGS(LossDescription) AS o_LossDescription,
	DateNotification,
	-- *INF*: TO_CHAR(DateNotification,'MM/DD/YYYY')
	TO_CHAR(DateNotification, 'MM/DD/YYYY') AS o_DateNotification,
	PolicyEffectiveDate,
	-- *INF*: TO_CHAR(PolicyEffectiveDate,'MM/DD/YYYY')
	TO_CHAR(PolicyEffectiveDate, 'MM/DD/YYYY') AS o_PolicyEffectiveDate,
	PolicyTerminationDate,
	-- *INF*: TO_CHAR(PolicyTerminationDate,'MM/DD/YYYY')
	TO_CHAR(PolicyTerminationDate, 'MM/DD/YYYY') AS o_PolicyTerminationDate,
	SalvageReserveAmount,
	-- *INF*: IIF(LTRIM(RTRIM(v_SalvageReserveAmount))= 'N/A', '',LTRIM(RTRIM(v_SalvageReserveAmount)))
	IFF(LTRIM(RTRIM(v_SalvageReserveAmount)) = 'N/A', '', LTRIM(RTRIM(v_SalvageReserveAmount))) AS SalvageReserveAmount_o,
	-- *INF*: IIF(SalvageReserveAmount<0,'('||REPLACECHR(0,TO_CHAR(SalvageReserveAmount),'-','')||')',TO_CHAR(SalvageReserveAmount))
	IFF(
	    SalvageReserveAmount < 0,
	    '(' || REGEXP_REPLACE(TO_CHAR(SalvageReserveAmount),'-','','i') || ')',
	    TO_CHAR(SalvageReserveAmount)
	) AS v_SalvageReserveAmount,
	SubrogationReserveAmount,
	-- *INF*: IIF(LTRIM(RTRIM(v_SubrogationReserveAmount))= 'N/A', '',LTRIM(RTRIM(v_SubrogationReserveAmount)))
	IFF(
	    LTRIM(RTRIM(v_SubrogationReserveAmount)) = 'N/A', '',
	    LTRIM(RTRIM(v_SubrogationReserveAmount))
	) AS SubrogationReserveAmount_o,
	-- *INF*: IIF(SubrogationReserveAmount<0,'('||REPLACECHR(0,TO_CHAR(SubrogationReserveAmount),'-','')||')',TO_CHAR(SubrogationReserveAmount))
	IFF(
	    SubrogationReserveAmount < 0,
	    '(' || REGEXP_REPLACE(TO_CHAR(SubrogationReserveAmount),'-','','i') || ')',
	    TO_CHAR(SubrogationReserveAmount)
	) AS v_SubrogationReserveAmount,
	ExpenseIncurred,
	-- *INF*: IIF(LTRIM(RTRIM(v_ExpenseIncurred))= 'N/A', '',LTRIM(RTRIM(v_ExpenseIncurred)))
	IFF(LTRIM(RTRIM(v_ExpenseIncurred)) = 'N/A', '', LTRIM(RTRIM(v_ExpenseIncurred))) AS ExpenseIncurred_o,
	-- *INF*: IIF(ExpenseIncurred<0,'('||REPLACECHR(0,TO_CHAR(ExpenseIncurred),'-','')||')',TO_CHAR(ExpenseIncurred))
	IFF(
	    ExpenseIncurred < 0, '(' || REGEXP_REPLACE(TO_CHAR(ExpenseIncurred),'-','','i') || ')',
	    TO_CHAR(ExpenseIncurred)
	) AS v_ExpenseIncurred,
	LossIncurred,
	-- *INF*: IIF(LTRIM(RTRIM(v_LossIncurred))= 'N/A', '',LTRIM(RTRIM(v_LossIncurred)))
	IFF(LTRIM(RTRIM(v_LossIncurred)) = 'N/A', '', LTRIM(RTRIM(v_LossIncurred))) AS LossIncurred_o,
	-- *INF*: IIF(LossIncurred<0,'('||REPLACECHR(0,TO_CHAR(LossIncurred),'-','')||')',TO_CHAR(LossIncurred))
	IFF(
	    LossIncurred < 0, '(' || REGEXP_REPLACE(TO_CHAR(LossIncurred),'-','','i') || ')',
	    TO_CHAR(LossIncurred)
	) AS v_LossIncurred,
	SalvageIncurred,
	-- *INF*: IIF(LTRIM(RTRIM(v_SalvageIncurred))= 'N/A', '',LTRIM(RTRIM(v_SalvageIncurred)))
	IFF(LTRIM(RTRIM(v_SalvageIncurred)) = 'N/A', '', LTRIM(RTRIM(v_SalvageIncurred))) AS SalvageIncurred_o,
	-- *INF*: IIF(SalvageIncurred<0,'('||REPLACECHR(0,TO_CHAR(SalvageIncurred),'-','')||')',TO_CHAR(SalvageIncurred))
	IFF(
	    SalvageIncurred < 0, '(' || REGEXP_REPLACE(TO_CHAR(SalvageIncurred),'-','','i') || ')',
	    TO_CHAR(SalvageIncurred)
	) AS v_SalvageIncurred,
	SubrogationIncurred,
	-- *INF*: IIF(LTRIM(RTRIM(v_SubrogationIncurred))= 'N/A', '',LTRIM(RTRIM(v_SubrogationIncurred)))
	IFF(LTRIM(RTRIM(v_SubrogationIncurred)) = 'N/A', '', LTRIM(RTRIM(v_SubrogationIncurred))) AS SubrogationIncurred_o,
	-- *INF*: IIF(SubrogationIncurred<0,'('||REPLACECHR(0,TO_CHAR(SubrogationIncurred),'-','')||')',TO_CHAR(SubrogationIncurred))
	IFF(
	    SubrogationIncurred < 0,
	    '(' || REGEXP_REPLACE(TO_CHAR(SubrogationIncurred),'-','','i') || ')',
	    TO_CHAR(SubrogationIncurred)
	) AS v_SubrogationIncurred
	FROM SQ_Shortcut_to_WorkClaimsCentersOfExpertiseExtract
),
EXP_ClosedDate AS (
	SELECT
	InsuredName_o,
	-- *INF*: REPLACECHR(0,InsuredName_o,'"','')
	REGEXP_REPLACE(InsuredName_o,'"','','i') AS InsuredName_Output
	FROM EXP_Derivefields
),
EXP_Insuredname AS (
	SELECT
	CloseDate_o,
	-- *INF*: IIF(CloseDate_o='01/01/1800','',CloseDate_o)
	IFF(CloseDate_o = '01/01/1800', '', CloseDate_o) AS CloseDate_out
	FROM EXP_Derivefields
),
WorkClaimsCentersOfExpertise AS (
	INSERT INTO WorkClaimsCentersOfExpertise
	(ClaimNumber, SubClaimId, Claimtype, CoverageType, Status, PolicyNumber, LossTotal, ExpenseTotal, SalvageTotal, SubrogationTotal, LossDate, OpenedDate, ClosedDate, AdjusterName, AdjusterCode, SupervisorCode, SupervisorName, OfficeCode, OfficeName, UnitName, UnitCode, ClaimantName, LossIndemnity, LossMedical, LegalIndicator, LossState, PolicyType, LossType, CompanyName, Jurisdiction, LossMiscellaneous1, LossReserveTotal, LossReserveMiscellaneous1, ExpenseMiscellaneous1, ExpenseReserveTotal, ExpenseReserveMiscellaneous1, AdjusterEmail, SupervisorEmail, LossPostal, CategoryIndicator, InsuredName, SalvageIndicator, SubrogationIndicator, ExAdjusterIndicator, GeneralMiscellaneous1, GeneralMiscellaneous2, GeneralMiscellaneous3, GeneralMiscellaneous4, GeneralMiscellaneous5, GeneralMiscellaneous6, GeneralMiscellaneous7, GeneralMiscellaneous8, GeneralMiscellaneous9, GeneralMiscellaneous10, AgencyCode, ClaimFeatureRepresentativeCode, ClaimFeatureRepresentativeName, LossDescription, DateNotification, PolicyEffectiveDate, PolicyTerminationDate, SalvageReserve, SubrogationReserve, ExpenseIncurred, LossIncurred, SalvageIncurred, SubrogationIncurred)
	SELECT 
	EXP_Derivefields.ClaimNumber_o AS CLAIMNUMBER, 
	EXP_Derivefields.SubClaimId_o AS SUBCLAIMID, 
	EXP_Derivefields.Claimtype_o AS CLAIMTYPE, 
	EXP_Derivefields.CoverageType_o AS COVERAGETYPE, 
	EXP_Derivefields.Status_o AS STATUS, 
	EXP_Derivefields.PolicyNumber_o AS POLICYNUMBER, 
	EXP_Derivefields.LossTotal_o AS LOSSTOTAL, 
	EXP_Derivefields.ExpenseTotal_o AS EXPENSETOTAL, 
	EXP_Derivefields.SalvageTotal_o AS SALVAGETOTAL, 
	EXP_Derivefields.SubrogationTotal_o AS SUBROGATIONTOTAL, 
	EXP_Derivefields.LossDate_o AS LOSSDATE, 
	EXP_Derivefields.OpendDate_o AS OPENEDDATE, 
	EXP_Insuredname.CloseDate_out AS CLOSEDDATE, 
	EXP_Derivefields.AdjusterName_o AS ADJUSTERNAME, 
	EXP_Derivefields.AdjusterCode_o AS ADJUSTERCODE, 
	EXP_Derivefields.SupervisorCode_o AS SUPERVISORCODE, 
	EXP_Derivefields.SupervisorName_o AS SUPERVISORNAME, 
	EXP_Derivefields.OfficeCode_o AS OFFICECODE, 
	EXP_Derivefields.OfficeName_o AS OFFICENAME, 
	EXP_Derivefields.UnitName_o AS UNITNAME, 
	EXP_Derivefields.UnitCode_o AS UNITCODE, 
	EXP_Derivefields.ClaimantName_o AS CLAIMANTNAME, 
	EXP_Derivefields.LossIndemnity_o AS LOSSINDEMNITY, 
	EXP_Derivefields.LossMedical_o AS LOSSMEDICAL, 
	EXP_Derivefields.LegalIndicator_o AS LEGALINDICATOR, 
	EXP_Derivefields.LossState_o AS LOSSSTATE, 
	EXP_Derivefields.PolicyType_o AS POLICYTYPE, 
	EXP_Derivefields.LossType_o AS LOSSTYPE, 
	EXP_Derivefields.CompanyName_o AS COMPANYNAME, 
	EXP_Derivefields.Jurisdiction_o AS JURISDICTION, 
	EXP_Derivefields.LossMiscellaneous1_o AS LOSSMISCELLANEOUS1, 
	EXP_Derivefields.LossReserveTotal_o AS LOSSRESERVETOTAL, 
	EXP_Derivefields.LossReserveMiscellaneous1_o AS LOSSRESERVEMISCELLANEOUS1, 
	EXP_Derivefields.ExpenseMiscellaneous1_o AS EXPENSEMISCELLANEOUS1, 
	EXP_Derivefields.ExpenseReserveTotal_o AS EXPENSERESERVETOTAL, 
	EXP_Derivefields.ExpenseReserveMiscellaneous1_o AS EXPENSERESERVEMISCELLANEOUS1, 
	EXP_Derivefields.AdjusterEmail_o AS ADJUSTEREMAIL, 
	EXP_Derivefields.SupervisorEmail_o AS SUPERVISOREMAIL, 
	EXP_Derivefields.LossPostal_o AS LOSSPOSTAL, 
	EXP_Derivefields.CategoryIndicator_o AS CATEGORYINDICATOR, 
	EXP_ClosedDate.InsuredName_Output AS INSUREDNAME, 
	EXP_Derivefields.SalvageIndicator_o AS SALVAGEINDICATOR, 
	EXP_Derivefields.SubrogationIndicator_o AS SUBROGATIONINDICATOR, 
	EXP_Derivefields.ExAdjusterIndicator_o AS EXADJUSTERINDICATOR, 
	EXP_Derivefields.GeneralMiscellaneous1_o AS GENERALMISCELLANEOUS1, 
	EXP_Derivefields.GeneralMiscellaneous2_o AS GENERALMISCELLANEOUS2, 
	EXP_Derivefields.GeneralMiscellaneous3_o AS GENERALMISCELLANEOUS3, 
	EXP_Derivefields.GeneralMiscellaneous4_o AS GENERALMISCELLANEOUS4, 
	EXP_Derivefields.GeneralMiscellaneous5_o AS GENERALMISCELLANEOUS5, 
	EXP_Derivefields.GeneralMiscellaneous6_o AS GENERALMISCELLANEOUS6, 
	EXP_Derivefields.GeneralMiscellaneous7_o AS GENERALMISCELLANEOUS7, 
	EXP_Derivefields.GeneralMiscellaneous8_o AS GENERALMISCELLANEOUS8, 
	EXP_Derivefields.GeneralMiscellaneous9_o AS GENERALMISCELLANEOUS9, 
	EXP_Derivefields.GeneralMiscellaneous10_o AS GENERALMISCELLANEOUS10, 
	EXP_Derivefields.o_AgencyCode AS AGENCYCODE, 
	EXP_Derivefields.o_ClaimFeatureRepresentativeCode AS CLAIMFEATUREREPRESENTATIVECODE, 
	EXP_Derivefields.o_ClaimFeatureRepresentativeName AS CLAIMFEATUREREPRESENTATIVENAME, 
	EXP_Derivefields.o_LossDescription AS LOSSDESCRIPTION, 
	EXP_Derivefields.o_DateNotification AS DATENOTIFICATION, 
	EXP_Derivefields.o_PolicyEffectiveDate AS POLICYEFFECTIVEDATE, 
	EXP_Derivefields.o_PolicyTerminationDate AS POLICYTERMINATIONDATE, 
	EXP_Derivefields.SalvageReserveAmount_o AS SALVAGERESERVE, 
	EXP_Derivefields.SubrogationReserveAmount_o AS SUBROGATIONRESERVE, 
	EXP_Derivefields.ExpenseIncurred_o AS EXPENSEINCURRED, 
	EXP_Derivefields.LossIncurred_o AS LOSSINCURRED, 
	EXP_Derivefields.SalvageIncurred_o AS SALVAGEINCURRED, 
	EXP_Derivefields.SubrogationIncurred_o AS SUBROGATIONINCURRED
	FROM EXP_Derivefields
),
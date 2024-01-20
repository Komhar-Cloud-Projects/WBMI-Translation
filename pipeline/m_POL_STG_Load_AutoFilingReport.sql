WITH
SQ_AutoFiling AS (
	WITH AutoFilingsList (HistoryID, PolicyNumber, PolicyVersionFormatted)
	AS
	(SELECT MAX(trans.HistoryID), pol.PolicyNumber, wpol.PolicyVersionFormatted
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction trans WITH(NOLOCK)
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy pol WITH(NOLOCK) on trans.sessionid = pol.sessionid
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy wpol WITH(NOLOCK) on wpol.policyid = pol.PolicyId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.wb_cl_policy wcpol WITH(NOLOCK) on wcpol.wb_policyid = wpol.WB_PolicyId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_CL_Filing wcfiling WITH(NOLOCK) on wcfiling.WB_CL_PolicyId = wcpol.WB_CL_PolicyId
	WHERE trans.TransactionDate between @{pipeline().parameters.START_DATE} and @{pipeline().parameters.END_DATE}
	and trans.State = 'Committed' 
	and trans.type not in ('VoidDividend','RevisedDividend','RetrospectiveCalculation', 'FinalAudit', 'Dividend', 'Reporting', 'VoidFinalAudit', 'RevisedFinalAudit')
	and wcfiling.FilingType is not null
	GROUP BY pol.policynumber, wpol.PolicyVersionFormatted
	)
	
	SELECT wpty.CustomerNum
	,pol.PolicyNumber
	,wpol.PolicyVersionFormatted as Mod
	,wpol.Division
	,pol.PrimaryRatingState
	,pol.EffectiveDate
	,pol.ExpirationDate
	,agn.Reference as AgencyNumber
	,pty.Name as AgencyName
	,pty1.Name as InsuredName
	,cast(case when pol.expirationdate < @{pipeline().parameters.POL_EXP} and pol.Status not in ('Cancelled','NonRenewed') then 'Expired' else pol.Status end as varchar) as PolicyStatus
	,wcfiling.USDOTNumber
	,wcfiling.FilingName
	,wcfiling.FilingType
	,cast(case when wcfiling.MC90Only = '1' then 'Y' else 'N' end as varchar) as MC90Only
	,cast(case when wcfiling.FormType is not null then wcfiling.FormType else 'N/A' end as varchar) as FormType
	,cast (case when wcfiling.IntrastateFormEEX = '1' then 'Y' else 'N' end as varchar) as IntrastateFormEEX
	,cast (case when wcfiling.IntrastateFormH = '1' then 'Y' else 'N' end as varchar) as IntrastateFormH
	,cast (case when wcfiling.IntrastateFormHHaulingStates is null or wcfiling.IntrastateFormHHaulingStates = '0'  then 'N/A' else wcfiling.IntrastateFormHHaulingStates end as varchar) as HaulingStates
	,cast (case when wcfiling.InterstateBMC91X = '1' then 'Y' else 'N' end as varchar) as InterstateBMC91X
	,cast (case when wcfiling.InterstateBMC34 = '1' then 'Y' else 'N' end as varchar) as InterstateBMC34
	,cast (case when wcfiling.WIHumanServices = '1' then 'Y' else 'N' end as varchar) as HumanServices
	,cast (case when wcfiling.WISchoolBuss = '1' then 'Y' else 'N' end as varchar) as SchoolBus
	,cast (case when wcfiling.OHHaulingPermit = '1' then 'Y' else 'N' end as varchar) as OHHaulingPermit
	,wcfiling.status as FilingStatus
	,wcfiling.effective as FilingEffectiveDate
	,cast (case when wcfiling.Deleted = '1' then 'Y' else 'N' end as varchar) as DeletedFilingPage
	FROM AutoFilingsList AFL
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction trn WITH(NOLOCK) on trn.historyid = AFL.HistoryID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy pol WITH(NOLOCK) on trn.sessionid = pol.sessionid
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy wpol WITH(NOLOCK) on wpol.sessionid = trn.sessionid
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_CL_Filing wcfiling WITH(NOLOCK) on wcfiling.sessionid = trn.sessionid
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Agency agn WITH(NOLOCK) on agn.sessionid = trn.sessionid
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Party pty WITH(NOLOCK) on pty.sessionid = trn.sessionid and pty.partyid = agn.partyid
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Party pty1 WITH(NOLOCK) on pty1.sessionid = trn.sessionid
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Party wpty WITH(NOLOCK) on wpty.sessionid = pol.sessionid
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_PartyAssociation ptya WITH(NOLOCK) on ptya.partyid = pty1.partyid and ptya.PartyAssociationType = 'Account'
	WHERE trn.State = 'Committed'
	and trn.type not in ('VoidDividend','RevisedDividend','RetrospectiveCalculation', 'FinalAudit', 'Dividend', 'Reporting', 'VoidFinalAudit', 'RevisedFinalAudit')
	and trn.TransactionDate between @{pipeline().parameters.START_DATE} and @{pipeline().parameters.END_DATE}
	and wcfiling.FilingType is not null
	and wpty.customernum is not null
	@{pipeline().parameters.WHERE_CLAUSE}
	GROUP BY wpty.CustomerNum
	,pol.PolicyNumber
	,wpol.PolicyVersionFormatted
	,wpol.Division
	,pol.primaryratingstate
	,pol.effectivedate
	,pol.expirationdate
	,agn.Reference
	,pty.Name
	,pty1.Name
	,pol.Status
	,wcfiling.USDOTNumber
	,wcfiling.FilingName
	,wcfiling.FilingType
	,wcfiling.MC90Only
	,wcfiling.FormType
	,wcfiling.IntrastateFormEEX
	,wcfiling.IntrastateFormH
	,wcfiling.IntrastateFormHHaulingStates
	,wcfiling.InterstateBMC91X
	,wcfiling.InterstateBMC34
	,wcfiling.WIHumanServices
	,wcfiling.WISchoolBuss
	,wcfiling.OHHaulingPermit
	,wcfiling.Status
	,wcfiling.Effective
	,wcfiling.Deleted
	ORDER BY 1, 2, 3, 4
),
EXP_AutoFiling AS (
	SELECT
	CustomerNum,
	PolicyNumber,
	PolicyVersionFormatted AS Mod,
	Division,
	PrimaryRatingState,
	EffectiveDate,
	ExpirationDate,
	Reference AS AgencyNumber,
	Name AS i_AgencyName,
	-- *INF*: chr(34)  || i_AgencyName || chr(34)
	chr(34) || i_AgencyName || chr(34) AS o_AgencyName,
	InsuredName AS i_InsuredName,
	-- *INF*: chr(34)  || i_InsuredName || chr(34)
	chr(34) || i_InsuredName || chr(34) AS o_InsuredName,
	PolicyStatus,
	USDOTNumber,
	FilingName AS i_FilingName,
	-- *INF*: chr(34) || i_FilingName || chr(34)
	chr(34) || i_FilingName || chr(34) AS o_FilingName,
	FilingType,
	MC90Only,
	FormType,
	IntrastateFormEEX,
	IntrastateFormH,
	IntrastateFormHHaulingStates AS i_HaulingStates,
	'"' || i_HaulingStates || '"' AS o_HaulingStates,
	InterstateBMC91X,
	InterstateBMC34,
	WIHumanServices AS HumanServices,
	WISchoolBuss AS SchoolBus,
	OHHaulingPermit,
	Status AS FilingStatus,
	Effective AS FilingEffectiveDate,
	Deleted AS DeletedFilingPage
	FROM SQ_AutoFiling
),
AutoFilingReport AS (
	INSERT INTO AutoFilingReport
	(CustomerNum, PolicyNumber, Mod, Division, PrimaryRatingState, EffectiveDate, ExpirationDate, AgencyNumber, AgencyName, InsuredName, PolicyStatus, USDOTNumber, FilingName, FilingType, MC90Only, FormType, IntrastateFormEEX, IntrastateFormH, HaulingStates, InterstateBMC91X, InterstateBMC34, HumanServices, SchoolBus, OHHaulingPermit, FilingStatus, FilingEffectiveDate, DeletedFilingPage)
	SELECT 
	CUSTOMERNUM, 
	POLICYNUMBER, 
	MOD, 
	DIVISION, 
	PRIMARYRATINGSTATE, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	AGENCYNUMBER, 
	o_AgencyName AS AGENCYNAME, 
	o_InsuredName AS INSUREDNAME, 
	POLICYSTATUS, 
	USDOTNUMBER, 
	o_FilingName AS FILINGNAME, 
	FILINGTYPE, 
	MC90ONLY, 
	FORMTYPE, 
	INTRASTATEFORMEEX, 
	INTRASTATEFORMH, 
	o_HaulingStates AS HAULINGSTATES, 
	INTERSTATEBMC91X, 
	INTERSTATEBMC34, 
	HUMANSERVICES, 
	SCHOOLBUS, 
	OHHAULINGPERMIT, 
	FILINGSTATUS, 
	FILINGEFFECTIVEDATE, 
	DELETEDFILINGPAGE
	FROM EXP_AutoFiling
),
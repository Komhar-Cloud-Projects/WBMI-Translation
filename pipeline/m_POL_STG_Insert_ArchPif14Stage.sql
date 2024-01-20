WITH
SQ_Pif14Stage AS (
	SELECT
		Pif14StageId,
		ExtractDate,
		SourceSystemId,
		AuditId,
		FrrPifsymbol,
		FrrPifpolicynumber,
		FrrPifmodule,
		FrrRecordid,
		FrrUnit,
		FrrAmendment,
		FrrLastchange,
		FrrLocationstate,
		FrrTerritory,
		FrrForm,
		FrrNumberoffampos,
		FrrOccupancy,
		FrrConstruction,
		FrrProtectionclass,
		FrrDeductibletype,
		FrrDeductible,
		FrrHydrant,
		FrrFirestation,
		FrrInsidecity,
		FrrBuildingamount,
		FrrBuildingpremium,
		FrrContentsAmount,
		FrrContentsPremium,
		FrrECAECcoverage,
		FrrVandalismMaliciousmis,
		FrrStoriesorWoodstove,
		FrrInflationguard,
		FrrValueup,
		FrrMoneyendorsement,
		FrrYearofconstruction,
		FrrAgentscommission,
		FrrZipcode,
		FrrOriginalratebook,
		FrrTaxlocation,
		FrrCurrReplacementcost,
		FrrCurrReplaceCostdate,
		FrrReplacementCost,
		FrrInspectionYear,
		FrrLastValueChange,
		FrrInspectionReport,
		FrrReplaceCostmethod,
		FrrHistoryOption,
		FrrNewPremium,
		FrrOldPremium,
		FrrWindstormAssumption,
		FrrCommercialIndicator,
		FrrRatebook,
		FrrLastchangefactor,
		Pif14MarketValuepos1,
		Pif14Filler1,
		FrrSpecialuse2,
		FrrECConstruction,
		FrrWndstmconstruction,
		FrrCoinsurancecode,
		FrrApprovedroofind,
		FrrLiability,
		FrrMedpay,
		FrrLiabilitypremium,
		FrrRatinggrouplit,
		FrrFoundationtype,
		FrrFinishbaseper,
		Pif14Filler2,
		FrrExistinscreditdate,
		FrrCondo,
		FrrZipind,
		FrrBuildLimchangeind,
		FrrDedLimchangeind,
		FrrReplCostchangeind,
		FrrCustFutureuse,
		FrrYr2000Custuse,
		FrrDupKeySeqnum
	FROM Pif14Stage
),
EXP_MetaData AS (
	SELECT
	Pif14StageId,
	ExtractDate,
	SourceSystemId,
	AuditId,
	FrrPifsymbol,
	FrrPifpolicynumber,
	FrrPifmodule,
	FrrRecordid,
	FrrUnit,
	FrrAmendment,
	FrrLastchange,
	FrrLocationstate,
	FrrTerritory,
	FrrForm,
	FrrNumberoffampos,
	FrrOccupancy,
	FrrConstruction,
	FrrProtectionclass,
	FrrDeductibletype,
	FrrDeductible,
	FrrHydrant,
	FrrFirestation,
	FrrInsidecity,
	FrrBuildingamount,
	FrrBuildingpremium,
	FrrContentsAmount,
	FrrContentsPremium,
	FrrECAECcoverage,
	FrrVandalismMaliciousmis,
	FrrStoriesorWoodstove,
	FrrInflationguard,
	FrrValueup,
	FrrMoneyendorsement,
	FrrYearofconstruction,
	FrrAgentscommission,
	FrrZipcode,
	FrrOriginalratebook,
	FrrTaxlocation,
	FrrCurrReplacementcost,
	FrrCurrReplaceCostdate,
	FrrReplacementCost,
	FrrInspectionYear,
	FrrLastValueChange,
	FrrInspectionReport,
	FrrReplaceCostmethod,
	FrrHistoryOption,
	FrrNewPremium,
	FrrOldPremium,
	FrrWindstormAssumption,
	FrrCommercialIndicator,
	FrrRatebook,
	FrrLastchangefactor,
	Pif14MarketValuepos1,
	Pif14Filler1,
	FrrSpecialuse2,
	FrrECConstruction,
	FrrWndstmconstruction,
	FrrCoinsurancecode,
	FrrApprovedroofind,
	FrrLiability,
	FrrMedpay,
	FrrLiabilitypremium,
	FrrRatinggrouplit,
	FrrFoundationtype,
	FrrFinishbaseper,
	Pif14Filler2,
	FrrExistinscreditdate,
	FrrCondo,
	FrrZipind,
	FrrBuildLimchangeind,
	FrrDedLimchangeind,
	FrrReplCostchangeind,
	FrrCustFutureuse,
	FrrYr2000Custuse,
	FrrDupKeySeqnum
	FROM SQ_Pif14Stage
),
ArchPif14Stage AS (
	INSERT INTO ArchPif14Stage
	(Pif14StageId, ExtractDate, SourceSystemId, AuditId, FrrPifsymbol, FrrPifpolicynumber, FrrPifmodule, FrrRecordid, FrrUnit, FrrAmendment, FrrLastchange, FrrLocationstate, FrrTerritory, FrrForm, FrrNumberoffampos, FrrOccupancy, FrrConstruction, FrrProtectionclass, FrrDeductibletype, FrrDeductible, FrrHydrant, FrrFirestation, FrrInsidecity, FrrBuildingamount, FrrBuildingpremium, FrrContentsAmount, FrrContentsPremium, FrrECAECcoverage, FrrVandalismMaliciousmis, FrrStoriesorWoodstove, FrrInflationguard, FrrValueup, FrrMoneyendorsement, FrrYearofconstruction, FrrAgentscommission, FrrZipcode, FrrOriginalratebook, FrrTaxlocation, FrrCurrReplacementcost, FrrCurrReplaceCostdate, FrrReplacementCost, FrrInspectionYear, FrrLastValueChange, FrrInspectionReport, FrrReplaceCostmethod, FrrHistoryOption, FrrNewPremium, FrrOldPremium, FrrWindstormAssumption, FrrCommercialIndicator, FrrRatebook, FrrLastchangefactor, ArchPif14MarketValuepos1, Pif14Filler1, FrrSpecialuse2, FrrECConstruction, FrrWndstmconstruction, FrrCoinsurancecode, FrrApprovedroofind, FrrLiability, FrrMedpay, FrrLiabilitypremium, FrrRatinggrouplit, FrrFoundationtype, FrrFinishbaseper, Pif14Filler2, FrrExistinscreditdate, FrrCondo, FrrZipind, FrrBuildLimchangeind, FrrDedLimchangeind, FrrReplCostchangeind, FrrCustFutureuse, FrrYr2000Custuse, FrrDupKeySeqnum)
	SELECT 
	PIF14STAGEID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID, 
	FRRPIFSYMBOL, 
	FRRPIFPOLICYNUMBER, 
	FRRPIFMODULE, 
	FRRRECORDID, 
	FRRUNIT, 
	FRRAMENDMENT, 
	FRRLASTCHANGE, 
	FRRLOCATIONSTATE, 
	FRRTERRITORY, 
	FRRFORM, 
	FRRNUMBEROFFAMPOS, 
	FRROCCUPANCY, 
	FRRCONSTRUCTION, 
	FRRPROTECTIONCLASS, 
	FRRDEDUCTIBLETYPE, 
	FRRDEDUCTIBLE, 
	FRRHYDRANT, 
	FRRFIRESTATION, 
	FRRINSIDECITY, 
	FRRBUILDINGAMOUNT, 
	FRRBUILDINGPREMIUM, 
	FRRCONTENTSAMOUNT, 
	FRRCONTENTSPREMIUM, 
	FRRECAECCOVERAGE, 
	FRRVANDALISMMALICIOUSMIS, 
	FRRSTORIESORWOODSTOVE, 
	FRRINFLATIONGUARD, 
	FRRVALUEUP, 
	FRRMONEYENDORSEMENT, 
	FRRYEAROFCONSTRUCTION, 
	FRRAGENTSCOMMISSION, 
	FRRZIPCODE, 
	FRRORIGINALRATEBOOK, 
	FRRTAXLOCATION, 
	FRRCURRREPLACEMENTCOST, 
	FRRCURRREPLACECOSTDATE, 
	FRRREPLACEMENTCOST, 
	FRRINSPECTIONYEAR, 
	FRRLASTVALUECHANGE, 
	FRRINSPECTIONREPORT, 
	FRRREPLACECOSTMETHOD, 
	FRRHISTORYOPTION, 
	FRRNEWPREMIUM, 
	FRROLDPREMIUM, 
	FRRWINDSTORMASSUMPTION, 
	FRRCOMMERCIALINDICATOR, 
	FRRRATEBOOK, 
	FRRLASTCHANGEFACTOR, 
	Pif14MarketValuepos1 AS ARCHPIF14MARKETVALUEPOS1, 
	PIF14FILLER1, 
	FRRSPECIALUSE2, 
	FRRECCONSTRUCTION, 
	FRRWNDSTMCONSTRUCTION, 
	FRRCOINSURANCECODE, 
	FRRAPPROVEDROOFIND, 
	FRRLIABILITY, 
	FRRMEDPAY, 
	FRRLIABILITYPREMIUM, 
	FRRRATINGGROUPLIT, 
	FRRFOUNDATIONTYPE, 
	FRRFINISHBASEPER, 
	PIF14FILLER2, 
	FRREXISTINSCREDITDATE, 
	FRRCONDO, 
	FRRZIPIND, 
	FRRBUILDLIMCHANGEIND, 
	FRRDEDLIMCHANGEIND, 
	FRRREPLCOSTCHANGEIND, 
	FRRCUSTFUTUREUSE, 
	FRRYR2000CUSTUSE, 
	FRRDUPKEYSEQNUM
	FROM EXP_MetaData
),
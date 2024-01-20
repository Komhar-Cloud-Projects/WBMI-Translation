WITH
SQ_PIF43UYCRStage AS (
	SELECT
		PIF43UYCRstageId,
		ExtractDate,
		SourceSystemId,
		PifSymbol,
		PifPolicyNumber,
		PifPolicyModule,
		PMDUYC1RecLength,
		PMDUYC1ActionCode,
		PMDUYC1FileID,
		PMDUYC1SegmentID,
		PMDUYC1SegmentStatus,
		PMDUYC1YearTransaction,
		PMDUYC1MonthTransaction,
		PMDUYC1DayTransaction,
		PMDUYC1SegmentLevelCode,
		PMDUYC1SegmentPartCode,
		PMDUYC1SubPartCode,
		PMDUYC1InsuranceLine,
		PMDUYC1LocationNumber,
		PMDUYC1SubLocationNumber,
		PMDUYC1RiskUnitGroupKey,
		PMDUYC1Coverage,
		PMDUYC1CoverageAmendment,
		PMDUYC1Messenger,
		PMDUYC1YearItemEffective,
		PMDUYC1MonthItemEffective,
		PMDUYC1DayItemEffective,
		PMDUYC1VariableKey,
		PMDUYC1YearProcess,
		PMDUYC1MonthProcess,
		PMDUYC1DayProcess,
		PMDUYC1SegmentMgInd,
		PMDUYC1Premium,
		PMDUYC1PremMgInd,
		PMDUYC1DeductibleMultip,
		PMDUYC1DedMgInd,
		PMDUYC1Rdf,
		PMDUYC1RdfMgInd,
		PMDUYC1CommissionRate,
		PMDUYC1CommMgInd,
		PMDUYC1PackageMod,
		PMDUYC1PackageMgInd,
		PMDUYC1CompanyDeviation,
		PMDUYC1InitPremMessChr,
		PMDUYC1RateBookIdD,
		PMDUYC1Rmf,
		PMDUYC1TotalDiscountRate,
		PMDUYC1DiscMgInd,
		PMDUYC1AmendmentRate,
		PMDUYC1AmendmentMgInd,
		PMDUYC1CalcPremFctr,
		PMDUYC1FormCode,
		PMDUYC1ProtectiveDevice,
		PMDUYC1TerritoryMultiplier,
		PMDUYC1TerrMgInd,
		PMDUYC1DedCredit,
		PMDUYC1NumMessengers,
		PMDUYC1StBaseRate,
		PMDUYC11stRateMgInd,
		PMDUYC12ndBaseRate,
		PMDUYC12ndRateMgInd,
		PMDUYC13rdBaseRate,
		PMDUYC13rdRateMgInd,
		PMDUYC14thBaseRate,
		PMDUYC14thRateMgInd,
		PMDUYC15thBaseRate,
		PMDUYC15thRateMgInd,
		PMDUYC1ClassOfInsured,
		PMDUYC1Rate1,
		PMDUYC1Rate2,
		PMDUYC1NumberOfEmployee1,
		PMDUYC1NumberOfEmployee2,
		PMDUYC1TotalEmployees,
		PMDUYC1NumberEmployeeCode,
		PMDUYC1DecChangeFlag,
		PMDUYC11st25LocFctr,
		PMDUYC12nd25LocFctr,
		PMDUYC1Ovr50LocFctr,
		PMDUYC1TotAddLocChg,
		PMDUYC1TermFactor,
		PMDUYC1AddlLocChg,
		PMDUYC1PersAcctsFactor,
		PMDUYC1CovMinPremInd,
		PMDUYC1FormFactor,
		PMDUYC1RmfMgInd,
		PMDUYC1Rate1MgInd,
		PMDUYC1Rate2MgInd,
		PMDUYC1BaseLossCostFactor,
		PMDUYC1FormCodeThree,
		PMDUYC1ModBaseLossFact,
		PMDUYC1NewLossCostMult,
		PMDUYC1PmsFutureUSE,
		PMDUYC11st25Units,
		PMDUYC1Over25Units,
		PMDUYC1CustomerUse,
		PMDUYC1SmartFactor,
		PMDUYC1AssocFactor,
		PMDUYC1ProgFactor,
		PMDUYC1SapFactor,
		PMDUYC1EmplPremOver5,
		PMDUYC1EmplPrem,
		PMDUYC1AddlPrem,
		PMDUYC1CffPremBefAdj,
		PMDUYC1IntPrem5,
		PMDUYC1IntPrem6,
		PMDUYC1RelativityFac,
		PMDUYC1EmplDedCredit,
		PMDUYC1YR2000CustUse
	FROM PIF43UYCRStage
),
EXP_Values AS (
	SELECT
	PIF43UYCRstageId,
	ExtractDate,
	SourceSystemId,
	PifSymbol,
	PifPolicyNumber,
	PifPolicyModule,
	PMDUYC1RecLength,
	PMDUYC1ActionCode,
	PMDUYC1FileID,
	PMDUYC1SegmentID,
	PMDUYC1SegmentStatus,
	PMDUYC1YearTransaction,
	PMDUYC1MonthTransaction,
	PMDUYC1DayTransaction,
	PMDUYC1SegmentLevelCode,
	PMDUYC1SegmentPartCode,
	PMDUYC1SubPartCode,
	PMDUYC1InsuranceLine,
	PMDUYC1LocationNumber,
	PMDUYC1SubLocationNumber,
	PMDUYC1RiskUnitGroupKey,
	PMDUYC1Coverage,
	PMDUYC1CoverageAmendment,
	PMDUYC1Messenger,
	PMDUYC1YearItemEffective,
	PMDUYC1MonthItemEffective,
	PMDUYC1DayItemEffective,
	PMDUYC1VariableKey,
	PMDUYC1YearProcess,
	PMDUYC1MonthProcess,
	PMDUYC1DayProcess,
	PMDUYC1SegmentMgInd,
	PMDUYC1Premium,
	PMDUYC1PremMgInd,
	PMDUYC1DeductibleMultip,
	PMDUYC1DedMgInd,
	PMDUYC1Rdf,
	PMDUYC1RdfMgInd,
	PMDUYC1CommissionRate,
	PMDUYC1CommMgInd,
	PMDUYC1PackageMod,
	PMDUYC1PackageMgInd,
	PMDUYC1CompanyDeviation,
	PMDUYC1InitPremMessChr,
	PMDUYC1RateBookIdD,
	PMDUYC1Rmf,
	PMDUYC1TotalDiscountRate,
	PMDUYC1DiscMgInd,
	PMDUYC1AmendmentRate,
	PMDUYC1AmendmentMgInd,
	PMDUYC1CalcPremFctr,
	PMDUYC1FormCode,
	PMDUYC1ProtectiveDevice,
	PMDUYC1TerritoryMultiplier,
	PMDUYC1TerrMgInd,
	PMDUYC1DedCredit,
	PMDUYC1NumMessengers,
	PMDUYC1StBaseRate,
	PMDUYC11stRateMgInd,
	PMDUYC12ndBaseRate,
	PMDUYC12ndRateMgInd,
	PMDUYC13rdBaseRate,
	PMDUYC13rdRateMgInd,
	PMDUYC14thBaseRate,
	PMDUYC14thRateMgInd,
	PMDUYC15thBaseRate,
	PMDUYC15thRateMgInd,
	PMDUYC1ClassOfInsured,
	PMDUYC1Rate1,
	PMDUYC1Rate2,
	PMDUYC1NumberOfEmployee1,
	PMDUYC1NumberOfEmployee2,
	PMDUYC1TotalEmployees,
	PMDUYC1NumberEmployeeCode,
	PMDUYC1DecChangeFlag,
	PMDUYC11st25LocFctr,
	PMDUYC12nd25LocFctr,
	PMDUYC1Ovr50LocFctr,
	PMDUYC1TotAddLocChg,
	PMDUYC1TermFactor,
	PMDUYC1AddlLocChg,
	PMDUYC1PersAcctsFactor,
	PMDUYC1CovMinPremInd,
	PMDUYC1FormFactor,
	PMDUYC1RmfMgInd,
	PMDUYC1Rate1MgInd,
	PMDUYC1Rate2MgInd,
	PMDUYC1BaseLossCostFactor,
	PMDUYC1FormCodeThree,
	PMDUYC1ModBaseLossFact,
	PMDUYC1NewLossCostMult,
	PMDUYC1PmsFutureUSE,
	PMDUYC11st25Units,
	PMDUYC1Over25Units,
	PMDUYC1CustomerUse,
	PMDUYC1SmartFactor,
	PMDUYC1AssocFactor,
	PMDUYC1ProgFactor,
	PMDUYC1SapFactor,
	PMDUYC1EmplPremOver5,
	PMDUYC1EmplPrem,
	PMDUYC1AddlPrem,
	PMDUYC1CffPremBefAdj,
	PMDUYC1IntPrem5,
	PMDUYC1IntPrem6,
	PMDUYC1RelativityFac,
	PMDUYC1EmplDedCredit,
	PMDUYC1YR2000CustUse,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_PIF43UYCRStage
),
ArchPIF43UYCRStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchPIF43UYCRStage
	(PIF43UYCRStageId, ExtractDate, SourceSystemId, AuditId, PifSymbol, PifPolicyNumber, PifPolicyModule, PMDUYC1RecLength, PMDUYC1ActionCode, PMDUYC1FileID, PMDUYC1SegmentID, PMDUYC1SegmentStatus, PMDUYC1YearTransaction, PMDUYC1MonthTransaction, PMDUYC1DayTransaction, PMDUYC1SegmentLevelCode, PMDUYC1SegmentPartCode, PMDUYC1SubPartCode, PMDUYC1InsuranceLine, PMDUYC1LocationNumber, PMDUYC1SubLocationNumber, PMDUYC1RiskUnitGroupKey, PMDUYC1Coverage, PMDUYC1CoverageAmendment, PMDUYC1Messenger, PMDUYC1YearItemEffective, PMDUYC1MonthItemEffective, PMDUYC1DayItemEffective, PMDUYC1VariableKey, PMDUYC1YearProcess, PMDUYC1MonthProcess, PMDUYC1DayProcess, PMDUYC1SegmentMgInd, PMDUYC1Premium, PMDUYC1PremMgInd, PMDUYC1DeductibleMultip, PMDUYC1DedMgInd, PMDUYC1Rdf, PMDUYC1RdfMgInd, PMDUYC1CommissionRate, PMDUYC1CommMgInd, PMDUYC1PackageMod, PMDUYC1PackageMgInd, PMDUYC1CompanyDeviation, PMDUYC1InitPremMessChr, PMDUYC1RateBookIdD, PMDUYC1Rmf, PMDUYC1TotalDiscountRate, PMDUYC1DiscMgInd, PMDUYC1AmendmentRate, PMDUYC1AmendmentMgInd, PMDUYC1CalcPremFctr, PMDUYC1FormCode, PMDUYC1ProtectiveDevice, PMDUYC1TerritoryMultiplier, PMDUYC1TerrMgInd, PMDUYC1DedCredit, PMDUYC1NumMessengers, PMDUYC1StBaseRate, PMDUYC11stRateMgInd, PMDUYC12ndBaseRate, PMDUYC12ndRateMgInd, PMDUYC13rdBaseRate, PMDUYC13rdRateMgInd, PMDUYC14thBaseRate, PMDUYC14thRateMgInd, PMDUYC15thBaseRate, PMDUYC15thRateMgInd, PMDUYC1ClassOfInsured, PMDUYC1Rate1, PMDUYC1Rate2, PMDUYC1NumberOfEmployee1, PMDUYC1NumberOfEmployee2, PMDUYC1TotalEmployees, PMDUYC1NumberEmployeeCode, PMDUYC1DecChangeFlag, PMDUYC11st25LocFctr, PMDUYC12nd25LocFctr, PMDUYC1Ovr50LocFctr, PMDUYC1TotAddLocChg, PMDUYC1TermFactor, PMDUYC1AddlLocChg, PMDUYC1PersAcctsFactor, PMDUYC1CovMinPremInd, PMDUYC1FormFactor, PMDUYC1RmfMgInd, PMDUYC1Rate1MgInd, PMDUYC1Rate2MgInd, PMDUYC1BaseLossCostFactor, PMDUYC1FormCodeThree, PMDUYC1ModBaseLossFact, PMDUYC1NewLossCostMult, PMDUYC1PmsFutureUSE, PMDUYC11st25Units, PMDUYC1Over25Units, PMDUYC1CustomerUse, PMDUYC1SmartFactor, PMDUYC1AssocFactor, PMDUYC1ProgFactor, PMDUYC1SapFactor, PMDUYC1EmplPremOver5, PMDUYC1EmplPrem, PMDUYC1AddlPrem, PMDUYC1CffPremBefAdj, PMDUYC1IntPrem5, PMDUYC1IntPrem6, PMDUYC1RelativityFac, PMDUYC1EmplDedCredit, PMDUYC1YR2000CustUse)
	SELECT 
	PIF43UYCRstageId AS PIF43UYCRSTAGEID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	PIFSYMBOL, 
	PIFPOLICYNUMBER, 
	PIFPOLICYMODULE, 
	PMDUYC1RECLENGTH, 
	PMDUYC1ACTIONCODE, 
	PMDUYC1FILEID, 
	PMDUYC1SEGMENTID, 
	PMDUYC1SEGMENTSTATUS, 
	PMDUYC1YEARTRANSACTION, 
	PMDUYC1MONTHTRANSACTION, 
	PMDUYC1DAYTRANSACTION, 
	PMDUYC1SEGMENTLEVELCODE, 
	PMDUYC1SEGMENTPARTCODE, 
	PMDUYC1SUBPARTCODE, 
	PMDUYC1INSURANCELINE, 
	PMDUYC1LOCATIONNUMBER, 
	PMDUYC1SUBLOCATIONNUMBER, 
	PMDUYC1RISKUNITGROUPKEY, 
	PMDUYC1COVERAGE, 
	PMDUYC1COVERAGEAMENDMENT, 
	PMDUYC1MESSENGER, 
	PMDUYC1YEARITEMEFFECTIVE, 
	PMDUYC1MONTHITEMEFFECTIVE, 
	PMDUYC1DAYITEMEFFECTIVE, 
	PMDUYC1VARIABLEKEY, 
	PMDUYC1YEARPROCESS, 
	PMDUYC1MONTHPROCESS, 
	PMDUYC1DAYPROCESS, 
	PMDUYC1SEGMENTMGIND, 
	PMDUYC1PREMIUM, 
	PMDUYC1PREMMGIND, 
	PMDUYC1DEDUCTIBLEMULTIP, 
	PMDUYC1DEDMGIND, 
	PMDUYC1RDF, 
	PMDUYC1RDFMGIND, 
	PMDUYC1COMMISSIONRATE, 
	PMDUYC1COMMMGIND, 
	PMDUYC1PACKAGEMOD, 
	PMDUYC1PACKAGEMGIND, 
	PMDUYC1COMPANYDEVIATION, 
	PMDUYC1INITPREMMESSCHR, 
	PMDUYC1RATEBOOKIDD, 
	PMDUYC1RMF, 
	PMDUYC1TOTALDISCOUNTRATE, 
	PMDUYC1DISCMGIND, 
	PMDUYC1AMENDMENTRATE, 
	PMDUYC1AMENDMENTMGIND, 
	PMDUYC1CALCPREMFCTR, 
	PMDUYC1FORMCODE, 
	PMDUYC1PROTECTIVEDEVICE, 
	PMDUYC1TERRITORYMULTIPLIER, 
	PMDUYC1TERRMGIND, 
	PMDUYC1DEDCREDIT, 
	PMDUYC1NUMMESSENGERS, 
	PMDUYC1STBASERATE, 
	PMDUYC11STRATEMGIND, 
	PMDUYC12NDBASERATE, 
	PMDUYC12NDRATEMGIND, 
	PMDUYC13RDBASERATE, 
	PMDUYC13RDRATEMGIND, 
	PMDUYC14THBASERATE, 
	PMDUYC14THRATEMGIND, 
	PMDUYC15THBASERATE, 
	PMDUYC15THRATEMGIND, 
	PMDUYC1CLASSOFINSURED, 
	PMDUYC1RATE1, 
	PMDUYC1RATE2, 
	PMDUYC1NUMBEROFEMPLOYEE1, 
	PMDUYC1NUMBEROFEMPLOYEE2, 
	PMDUYC1TOTALEMPLOYEES, 
	PMDUYC1NUMBEREMPLOYEECODE, 
	PMDUYC1DECCHANGEFLAG, 
	PMDUYC11ST25LOCFCTR, 
	PMDUYC12ND25LOCFCTR, 
	PMDUYC1OVR50LOCFCTR, 
	PMDUYC1TOTADDLOCCHG, 
	PMDUYC1TERMFACTOR, 
	PMDUYC1ADDLLOCCHG, 
	PMDUYC1PERSACCTSFACTOR, 
	PMDUYC1COVMINPREMIND, 
	PMDUYC1FORMFACTOR, 
	PMDUYC1RMFMGIND, 
	PMDUYC1RATE1MGIND, 
	PMDUYC1RATE2MGIND, 
	PMDUYC1BASELOSSCOSTFACTOR, 
	PMDUYC1FORMCODETHREE, 
	PMDUYC1MODBASELOSSFACT, 
	PMDUYC1NEWLOSSCOSTMULT, 
	PMDUYC1PMSFUTUREUSE, 
	PMDUYC11ST25UNITS, 
	PMDUYC1OVER25UNITS, 
	PMDUYC1CUSTOMERUSE, 
	PMDUYC1SMARTFACTOR, 
	PMDUYC1ASSOCFACTOR, 
	PMDUYC1PROGFACTOR, 
	PMDUYC1SAPFACTOR, 
	PMDUYC1EMPLPREMOVER5, 
	PMDUYC1EMPLPREM, 
	PMDUYC1ADDLPREM, 
	PMDUYC1CFFPREMBEFADJ, 
	PMDUYC1INTPREM5, 
	PMDUYC1INTPREM6, 
	PMDUYC1RELATIVITYFAC, 
	PMDUYC1EMPLDEDCREDIT, 
	PMDUYC1YR2000CUSTUSE
	FROM EXP_Values
),
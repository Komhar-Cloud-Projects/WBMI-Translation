WITH
SQ_Pif43LXZWCStage AS (
	SELECT
		Pif43LXZWCStageId,
		PifSymbol,
		PifPolicyNumber,
		PifModule,
		Pmdl4w1RecLength,
		Pmdl4w1ActionCode,
		Pmdl4w1FileId,
		Pmdl4w1SegmentId,
		Pmdl4w1SegmentStatus,
		Pmdl4w1YearTransaction,
		Pmdl4w1MonthTransaction,
		Pmdl4w1DayTransaction,
		Pmdl4w1SegmentLevelCode,
		Pmdl4w1SegmentPartCode,
		Pmdl4w1SubPartCode,
		Pmdl4w1InsuranceLine,
		Pmdl4w1LocationNumber,
		Pmdl4w1SplitRateSeq,
		Pmdl4w1YearItemEffective,
		Pmdl4w1MonthItemEffective,
		Pmdl4w1DayItemEffective,
		Pmdl4w1AuditNumber,
		Pmdl4w1AuditNumSeq,
		Pmdl4w1YearProcess,
		Pmdl4w1MonthProcess,
		Pmdl4w1DayProcess,
		Pmdl4w1YearItemExpire,
		Pmdl4w1MonthItemExpire,
		Pmdl4w1DayItemExpire,
		Pmdl4w1RatingProgramType,
		Pmdl4w1PolicyType,
		Pmdl4w1FedEmpIdNumber,
		Pmdl4w1CommissionType,
		Pmdl4w1CommissionSchedule,
		Pmdl4w1CommSchedMgInd,
		Pmdl4w1CommissionRate,
		Pmdl4w1CommRateMgInd,
		Pmdl4w1InterstRiskIdNo2,
		Pmdl4w1InterstateExpMod1,
		Pmdl4w1InterMod1Type,
		Pmdl4w1InterstateExpMod2,
		Pmdl4w1InterMod2Type,
		Pmdl4w1AnnRateDteMm,
		Pmdl4w1AnnRateDteDd,
		Pmdl4w1SplitRateInd,
		Pmdl4w1PrevCancReason,
		Pmdl4w1PrevCancDate,
		Pmdl4w1CovIiLmtsStdEach,
		Pmdl4w1CovIiLmtsStdPol,
		Pmdl4w1CovIiLmtsVcEach,
		Pmdl4w1CovIiLmtsVcPol,
		Pmdl4w1CovIiLmtsFedEach,
		Pmdl4w1FedProgramType,
		Pmdl4w1UslhCoverageFlag,
		Pmdl4w1VolCompCoverageFlag,
		Pmdl4w1RetroOptionId,
		Pmdl4w1OtherStCoverageFlag,
		Pmdl4w1CoveredState1,
		Pmdl4w1CoveredState2,
		Pmdl4w1CoveredState3,
		Pmdl4w1CoveredState4,
		Pmdl4w1CoveredState5,
		Pmdl4w1CoveredState6,
		Pmdl4w1CoveredState7,
		Pmdl4w1CoveredState8,
		Pmdl4w1CoveredState9,
		Pmdl4w1CoveredState10,
		Pmdl4w1CoveredState11,
		Pmdl4w1CoveredState12,
		Pmdl4w1CoveredState13,
		Pmdl4w1CoveredState14,
		Pmdl4w1CoveredState15,
		Pmdl4w1CoveredState16,
		Pmdl4w1CoveredState17,
		Pmdl4w1CoveredState18,
		Pmdl4w1CoveredState19,
		Pmdl4w1CoveredState20,
		Pmdl4w1CoveredState21,
		Pmdl4w1CoveredState22,
		Pmdl4w1CoveredState23,
		Pmdl4w1CoveredState24,
		Pmdl4w1StandardPremium,
		Pmdl4w1PremiumDiscountAmt,
		Pmdl4w1ExpenseConstantState,
		Pmdl4w1ExpenseConstantAmt,
		Pmdl4w1PolicyMinPremState,
		Pmdl4w1PolicyMinPremium,
		Pmdl4w1PolicyTermPremium,
		Pmdl4w1TaxAssessCharge,
		Pmdl4w1DepositPremiumPct,
		Pmdl4w1DepPremPctMgInd,
		Pmdl4w1DepositPremium,
		Pmdl4w1DepPremMgInd,
		Pmdl4w1FederalCoverageCode,
		Pmdl4w1PolJulianEffDteYy,
		Pmdl4w1PolJulianEffDteDdd,
		Pmdl4w1PrevCancelEntryDate,
		Pmdl4w1InterstArapRate1,
		Pmdl4w1InterstArapRate2,
		Pmdl4w1CurrCoviiStdId,
		Pmdl4w1CurrCoviiVolId,
		Pmdl4w1CurrCoviiAdmId,
		Pmdl4w1PolicyFullMinPrem,
		Pmdl4w1PmsFutureUse,
		Pmdl4w1MmsChangeByte,
		Pmdl4w1CustomerFutureUse,
		Pmdl4w1MnSpecFund,
		Pmdl4w1TerrorismFund,
		Pmdl4w1DtecFund,
		Pmdl4w1MnTotalModPrem,
		Pmdl4w1Yr2000CustUse,
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemId,
		Pmdl4w1InterstRiskFiller
	FROM Pif43LXZWCStage
),
EXP_VALUE AS (
	SELECT
	Pif43LXZWCStageId,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	Pmdl4w1RecLength,
	Pmdl4w1ActionCode,
	Pmdl4w1FileId,
	Pmdl4w1SegmentId,
	Pmdl4w1SegmentStatus,
	Pmdl4w1YearTransaction,
	Pmdl4w1MonthTransaction,
	Pmdl4w1DayTransaction,
	Pmdl4w1SegmentLevelCode,
	Pmdl4w1SegmentPartCode,
	Pmdl4w1SubPartCode,
	Pmdl4w1InsuranceLine,
	Pmdl4w1LocationNumber,
	Pmdl4w1SplitRateSeq,
	Pmdl4w1YearItemEffective,
	Pmdl4w1MonthItemEffective,
	Pmdl4w1DayItemEffective,
	Pmdl4w1AuditNumber,
	Pmdl4w1AuditNumSeq,
	Pmdl4w1YearProcess,
	Pmdl4w1MonthProcess,
	Pmdl4w1DayProcess,
	Pmdl4w1YearItemExpire,
	Pmdl4w1MonthItemExpire,
	Pmdl4w1DayItemExpire,
	Pmdl4w1RatingProgramType,
	Pmdl4w1PolicyType,
	Pmdl4w1FedEmpIdNumber,
	Pmdl4w1CommissionType,
	Pmdl4w1CommissionSchedule,
	Pmdl4w1CommSchedMgInd,
	Pmdl4w1CommissionRate,
	Pmdl4w1CommRateMgInd,
	Pmdl4w1InterstRiskIdNo2,
	Pmdl4w1InterstateExpMod1,
	Pmdl4w1InterMod1Type,
	Pmdl4w1InterstateExpMod2,
	Pmdl4w1InterMod2Type,
	Pmdl4w1AnnRateDteMm,
	Pmdl4w1AnnRateDteDd,
	Pmdl4w1SplitRateInd,
	Pmdl4w1PrevCancReason,
	Pmdl4w1PrevCancDate,
	Pmdl4w1CovIiLmtsStdEach,
	Pmdl4w1CovIiLmtsStdPol,
	Pmdl4w1CovIiLmtsVcEach,
	Pmdl4w1CovIiLmtsVcPol,
	Pmdl4w1CovIiLmtsFedEach,
	Pmdl4w1FedProgramType,
	Pmdl4w1UslhCoverageFlag,
	Pmdl4w1VolCompCoverageFlag,
	Pmdl4w1RetroOptionId,
	Pmdl4w1OtherStCoverageFlag,
	Pmdl4w1CoveredState1,
	Pmdl4w1CoveredState2,
	Pmdl4w1CoveredState3,
	Pmdl4w1CoveredState4,
	Pmdl4w1CoveredState5,
	Pmdl4w1CoveredState6,
	Pmdl4w1CoveredState7,
	Pmdl4w1CoveredState8,
	Pmdl4w1CoveredState9,
	Pmdl4w1CoveredState10,
	Pmdl4w1CoveredState11,
	Pmdl4w1CoveredState12,
	Pmdl4w1CoveredState13,
	Pmdl4w1CoveredState14,
	Pmdl4w1CoveredState15,
	Pmdl4w1CoveredState16,
	Pmdl4w1CoveredState17,
	Pmdl4w1CoveredState18,
	Pmdl4w1CoveredState19,
	Pmdl4w1CoveredState20,
	Pmdl4w1CoveredState21,
	Pmdl4w1CoveredState22,
	Pmdl4w1CoveredState23,
	Pmdl4w1CoveredState24,
	Pmdl4w1StandardPremium,
	Pmdl4w1PremiumDiscountAmt,
	Pmdl4w1ExpenseConstantState,
	Pmdl4w1ExpenseConstantAmt,
	Pmdl4w1PolicyMinPremState,
	Pmdl4w1PolicyMinPremium,
	Pmdl4w1PolicyTermPremium,
	Pmdl4w1TaxAssessCharge,
	Pmdl4w1DepositPremiumPct,
	Pmdl4w1DepPremPctMgInd,
	Pmdl4w1DepositPremium,
	Pmdl4w1DepPremMgInd,
	Pmdl4w1FederalCoverageCode,
	Pmdl4w1PolJulianEffDteYy,
	Pmdl4w1PolJulianEffDteDdd,
	Pmdl4w1PrevCancelEntryDate,
	Pmdl4w1InterstArapRate1,
	Pmdl4w1InterstArapRate2,
	Pmdl4w1CurrCoviiStdId,
	Pmdl4w1CurrCoviiVolId,
	Pmdl4w1CurrCoviiAdmId,
	Pmdl4w1PolicyFullMinPrem,
	Pmdl4w1PmsFutureUse,
	Pmdl4w1MmsChangeByte,
	Pmdl4w1CustomerFutureUse,
	Pmdl4w1MnSpecFund,
	Pmdl4w1TerrorismFund,
	Pmdl4w1DtecFund,
	Pmdl4w1MnTotalModPrem,
	Pmdl4w1Yr2000CustUse,
	ExtractDate,
	AsOfDate,
	RecordCount,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	Pmdl4w1InterstRiskFiller
	FROM SQ_Pif43LXZWCStage
),
ArchPif43LXZWCStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchPif43LXZWCStage
	(Pif43LXZWCStageId, PifSymbol, PifPolicyNumber, PifModule, Pmdl4w1RecLength, Pmdl4w1ActionCode, Pmdl4w1FileId, Pmdl4w1SegmentId, Pmdl4w1SegmentStatus, Pmdl4w1YearTransaction, Pmdl4w1MonthTransaction, Pmdl4w1DayTransaction, Pmdl4w1SegmentLevelCode, Pmdl4w1SegmentPartCode, Pmdl4w1SubPartCode, Pmdl4w1InsuranceLine, Pmdl4w1LocationNumber, Pmdl4w1SplitRateSeq, Pmdl4w1YearItemEffective, Pmdl4w1MonthItemEffective, Pmdl4w1DayItemEffective, Pmdl4w1AuditNumber, Pmdl4w1AuditNumSeq, Pmdl4w1YearProcess, Pmdl4w1MonthProcess, Pmdl4w1DayProcess, Pmdl4w1YearItemExpire, Pmdl4w1MonthItemExpire, Pmdl4w1DayItemExpire, Pmdl4w1RatingProgramType, Pmdl4w1PolicyType, Pmdl4w1FedEmpIdNumber, Pmdl4w1CommissionType, Pmdl4w1CommissionSchedule, Pmdl4w1CommSchedMgInd, Pmdl4w1CommissionRate, Pmdl4w1CommRateMgInd, Pmdl4w1InterstRiskIdNo2, Pmdl4w1InterstateExpMod1, Pmdl4w1InterMod1Type, Pmdl4w1InterstateExpMod2, Pmdl4w1InterMod2Type, Pmdl4w1AnnRateDteMm, Pmdl4w1AnnRateDteDd, Pmdl4w1SplitRateInd, Pmdl4w1PrevCancReason, Pmdl4w1PrevCancDate, Pmdl4w1CovIiLmtsStdEach, Pmdl4w1CovIiLmtsStdPol, Pmdl4w1CovIiLmtsVcEach, Pmdl4w1CovIiLmtsVcPol, Pmdl4w1CovIiLmtsFedEach, Pmdl4w1FedProgramType, Pmdl4w1UslhCoverageFlag, Pmdl4w1VolCompCoverageFlag, Pmdl4w1RetroOptionId, Pmdl4w1OtherStCoverageFlag, Pmdl4w1CoveredState1, Pmdl4w1CoveredState2, Pmdl4w1CoveredState3, Pmdl4w1CoveredState4, Pmdl4w1CoveredState5, Pmdl4w1CoveredState6, Pmdl4w1CoveredState7, Pmdl4w1CoveredState8, Pmdl4w1CoveredState9, Pmdl4w1CoveredState10, Pmdl4w1CoveredState11, Pmdl4w1CoveredState12, Pmdl4w1CoveredState13, Pmdl4w1CoveredState14, Pmdl4w1CoveredState15, Pmdl4w1CoveredState16, Pmdl4w1CoveredState17, Pmdl4w1CoveredState18, Pmdl4w1CoveredState19, Pmdl4w1CoveredState20, Pmdl4w1CoveredState21, Pmdl4w1CoveredState22, Pmdl4w1CoveredState23, Pmdl4w1CoveredState24, Pmdl4w1StandardPremium, Pmdl4w1PremiumDiscountAmt, Pmdl4w1ExpenseConstantState, Pmdl4w1ExpenseConstantAmt, Pmdl4w1PolicyMinPremState, Pmdl4w1PolicyMinPremium, Pmdl4w1PolicyTermPremium, Pmdl4w1TaxAssessCharge, Pmdl4w1DepositPremiumPct, Pmdl4w1DepPremPctMgInd, Pmdl4w1DepositPremium, Pmdl4w1DepPremMgInd, Pmdl4w1FederalCoverageCode, Pmdl4w1PolJulianEffDteYy, Pmdl4w1PolJulianEffDteDdd, Pmdl4w1PrevCancelEntryDate, Pmdl4w1InterstArapRate1, Pmdl4w1InterstArapRate2, Pmdl4w1CurrCoviiStdId, Pmdl4w1CurrCoviiVolId, Pmdl4w1CurrCoviiAdmId, Pmdl4w1PolicyFullMinPrem, Pmdl4w1PmsFutureUse, Pmdl4w1MmsChangeByte, Pmdl4w1CustomerFutureUse, Pmdl4w1MnSpecFund, Pmdl4w1TerrorismFund, Pmdl4w1DtecFund, Pmdl4w1MnTotalModPrem, Pmdl4w1Yr2000CustUse, ExtractDate, AsOfDate, RecordCount, SourceSystemId, AuditId, Pmdl4w1InterstRiskFiller)
	SELECT 
	PIF43LXZWCSTAGEID, 
	PIFSYMBOL, 
	PIFPOLICYNUMBER, 
	PIFMODULE, 
	PMDL4W1RECLENGTH, 
	PMDL4W1ACTIONCODE, 
	PMDL4W1FILEID, 
	PMDL4W1SEGMENTID, 
	PMDL4W1SEGMENTSTATUS, 
	PMDL4W1YEARTRANSACTION, 
	PMDL4W1MONTHTRANSACTION, 
	PMDL4W1DAYTRANSACTION, 
	PMDL4W1SEGMENTLEVELCODE, 
	PMDL4W1SEGMENTPARTCODE, 
	PMDL4W1SUBPARTCODE, 
	PMDL4W1INSURANCELINE, 
	PMDL4W1LOCATIONNUMBER, 
	PMDL4W1SPLITRATESEQ, 
	PMDL4W1YEARITEMEFFECTIVE, 
	PMDL4W1MONTHITEMEFFECTIVE, 
	PMDL4W1DAYITEMEFFECTIVE, 
	PMDL4W1AUDITNUMBER, 
	PMDL4W1AUDITNUMSEQ, 
	PMDL4W1YEARPROCESS, 
	PMDL4W1MONTHPROCESS, 
	PMDL4W1DAYPROCESS, 
	PMDL4W1YEARITEMEXPIRE, 
	PMDL4W1MONTHITEMEXPIRE, 
	PMDL4W1DAYITEMEXPIRE, 
	PMDL4W1RATINGPROGRAMTYPE, 
	PMDL4W1POLICYTYPE, 
	PMDL4W1FEDEMPIDNUMBER, 
	PMDL4W1COMMISSIONTYPE, 
	PMDL4W1COMMISSIONSCHEDULE, 
	PMDL4W1COMMSCHEDMGIND, 
	PMDL4W1COMMISSIONRATE, 
	PMDL4W1COMMRATEMGIND, 
	PMDL4W1INTERSTRISKIDNO2, 
	PMDL4W1INTERSTATEEXPMOD1, 
	PMDL4W1INTERMOD1TYPE, 
	PMDL4W1INTERSTATEEXPMOD2, 
	PMDL4W1INTERMOD2TYPE, 
	PMDL4W1ANNRATEDTEMM, 
	PMDL4W1ANNRATEDTEDD, 
	PMDL4W1SPLITRATEIND, 
	PMDL4W1PREVCANCREASON, 
	PMDL4W1PREVCANCDATE, 
	PMDL4W1COVIILMTSSTDEACH, 
	PMDL4W1COVIILMTSSTDPOL, 
	PMDL4W1COVIILMTSVCEACH, 
	PMDL4W1COVIILMTSVCPOL, 
	PMDL4W1COVIILMTSFEDEACH, 
	PMDL4W1FEDPROGRAMTYPE, 
	PMDL4W1USLHCOVERAGEFLAG, 
	PMDL4W1VOLCOMPCOVERAGEFLAG, 
	PMDL4W1RETROOPTIONID, 
	PMDL4W1OTHERSTCOVERAGEFLAG, 
	PMDL4W1COVEREDSTATE1, 
	PMDL4W1COVEREDSTATE2, 
	PMDL4W1COVEREDSTATE3, 
	PMDL4W1COVEREDSTATE4, 
	PMDL4W1COVEREDSTATE5, 
	PMDL4W1COVEREDSTATE6, 
	PMDL4W1COVEREDSTATE7, 
	PMDL4W1COVEREDSTATE8, 
	PMDL4W1COVEREDSTATE9, 
	PMDL4W1COVEREDSTATE10, 
	PMDL4W1COVEREDSTATE11, 
	PMDL4W1COVEREDSTATE12, 
	PMDL4W1COVEREDSTATE13, 
	PMDL4W1COVEREDSTATE14, 
	PMDL4W1COVEREDSTATE15, 
	PMDL4W1COVEREDSTATE16, 
	PMDL4W1COVEREDSTATE17, 
	PMDL4W1COVEREDSTATE18, 
	PMDL4W1COVEREDSTATE19, 
	PMDL4W1COVEREDSTATE20, 
	PMDL4W1COVEREDSTATE21, 
	PMDL4W1COVEREDSTATE22, 
	PMDL4W1COVEREDSTATE23, 
	PMDL4W1COVEREDSTATE24, 
	PMDL4W1STANDARDPREMIUM, 
	PMDL4W1PREMIUMDISCOUNTAMT, 
	PMDL4W1EXPENSECONSTANTSTATE, 
	PMDL4W1EXPENSECONSTANTAMT, 
	PMDL4W1POLICYMINPREMSTATE, 
	PMDL4W1POLICYMINPREMIUM, 
	PMDL4W1POLICYTERMPREMIUM, 
	PMDL4W1TAXASSESSCHARGE, 
	PMDL4W1DEPOSITPREMIUMPCT, 
	PMDL4W1DEPPREMPCTMGIND, 
	PMDL4W1DEPOSITPREMIUM, 
	PMDL4W1DEPPREMMGIND, 
	PMDL4W1FEDERALCOVERAGECODE, 
	PMDL4W1POLJULIANEFFDTEYY, 
	PMDL4W1POLJULIANEFFDTEDDD, 
	PMDL4W1PREVCANCELENTRYDATE, 
	PMDL4W1INTERSTARAPRATE1, 
	PMDL4W1INTERSTARAPRATE2, 
	PMDL4W1CURRCOVIISTDID, 
	PMDL4W1CURRCOVIIVOLID, 
	PMDL4W1CURRCOVIIADMID, 
	PMDL4W1POLICYFULLMINPREM, 
	PMDL4W1PMSFUTUREUSE, 
	PMDL4W1MMSCHANGEBYTE, 
	PMDL4W1CUSTOMERFUTUREUSE, 
	PMDL4W1MNSPECFUND, 
	PMDL4W1TERRORISMFUND, 
	PMDL4W1DTECFUND, 
	PMDL4W1MNTOTALMODPREM, 
	PMDL4W1YR2000CUSTUSE, 
	EXTRACTDATE, 
	ASOFDATE, 
	RECORDCOUNT, 
	SOURCESYSTEMID, 
	AUDITID, 
	PMDL4W1INTERSTRISKFILLER
	FROM EXP_VALUE
),
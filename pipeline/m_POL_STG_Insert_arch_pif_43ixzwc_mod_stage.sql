WITH
SQ_Pif43IXZWCModStage AS (
	SELECT
		Pif43IXZWCModStageId,
		PifSymbol,
		PifPolicyNumber,
		PifModule,
		Pmdi4w1RecLength,
		Pmdi4w1ActionCode,
		Pmdi4w1FileId,
		Pmdi4w1SegmentId,
		Pmdi4w1SegmentStatus,
		Pmdi4w1YearTransaction,
		Pmdi4w1MonthTransaction,
		Pmdi4w1DayTransaction,
		Pmdi4w1SegmentLevelCode,
		Pmdi4w1SegmentPartCode,
		Pmdi4w1SubPartCode,
		Pmdi4w1InsuranceLine,
		Pmdi4w1WcRatingState,
		Pmdi4w1LocationNumber,
		Pmdi4w1ClassOrderCode,
		Pmdi4w1ClassOrderSeq,
		Pmdi4w1ReportingClassCode,
		Pmdi4w1ReportingClassSeq,
		Pmdi4w1SplitRateSeq,
		Pmdi4w1YearItemEffective,
		Pmdi4w1MonthItemEffective,
		Pmdi4w1DayItemEffective,
		Pmdi4w1AuditNumber,
		Pmdi4w1AuditNumSeq,
		Pmdi4w1YearProcess,
		Pmdi4w1MonthProcess,
		Pmdi4w1DayProcess,
		Pmdi4w1YearItemExpire,
		Pmdi4w1MonthItemExpire,
		Pmdi4w1DayItemExpire,
		Pmdi4w1GeneratedSegInd,
		Pmdi4w1ModifierDesc,
		Pmdi4w1ModifierRate,
		Pmdi4w1ModFactorMdInd,
		Pmdi4w1ModifierPremium,
		Pmdi4w1ModPremMgInd,
		Pmdi4w1ModifierType1,
		Pmdi4w1ModifierType2,
		Pmdi4w1ModifierPremBasis,
		Pmdi4w1ModBasisMgInd,
		Pmdi4w1ModifierMinPrem,
		Pmdi4w1ModMinPremMgInd,
		Pmdi4w1DepositPremium,
		Pmdi4w1ModifierUslhFac,
		Pmdi4w1AuditSegBuiltInd,
		Pmdi4w1YearRatingExpire,
		Pmdi4w1MonthRatingExpire,
		Pmdi4w1DayRatingExpire,
		Pmdi4w1CoviiLimitId,
		Pmdi4w1YearModAnnivRate,
		Pmdi4w1MonthModAnnivRate,
		Pmdi4w1DayModAnnivRate,
		Pmdi4w1VarDeductibleAmt,
		Pmdi4w1VarAggrDeductAmt,
		Pmdi4w1CaEmodAtIncept,
		Pmdi4w1PmsFutureUse,
		Pmdi4w1CustomerFutureUse,
		Pmdi4w1Yr2000CustUse,
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemId
	FROM Pif43IXZWCModStage
),
EXP_Pif43IXZWCModStage AS (
	SELECT
	Pif43IXZWCModStageId,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	Pmdi4w1RecLength,
	Pmdi4w1ActionCode,
	Pmdi4w1FileId,
	Pmdi4w1SegmentId,
	Pmdi4w1SegmentStatus,
	Pmdi4w1YearTransaction,
	Pmdi4w1MonthTransaction,
	Pmdi4w1DayTransaction,
	Pmdi4w1SegmentLevelCode,
	Pmdi4w1SegmentPartCode,
	Pmdi4w1SubPartCode,
	Pmdi4w1InsuranceLine,
	Pmdi4w1WcRatingState,
	Pmdi4w1LocationNumber,
	Pmdi4w1ClassOrderCode,
	Pmdi4w1ClassOrderSeq,
	Pmdi4w1ReportingClassCode,
	Pmdi4w1ReportingClassSeq,
	Pmdi4w1SplitRateSeq,
	Pmdi4w1YearItemEffective,
	Pmdi4w1MonthItemEffective,
	Pmdi4w1DayItemEffective,
	Pmdi4w1AuditNumber,
	Pmdi4w1AuditNumSeq,
	Pmdi4w1YearProcess,
	Pmdi4w1MonthProcess,
	Pmdi4w1DayProcess,
	Pmdi4w1YearItemExpire,
	Pmdi4w1MonthItemExpire,
	Pmdi4w1DayItemExpire,
	Pmdi4w1GeneratedSegInd,
	Pmdi4w1ModifierDesc,
	Pmdi4w1ModifierRate,
	Pmdi4w1ModFactorMdInd,
	Pmdi4w1ModifierPremium,
	Pmdi4w1ModPremMgInd,
	Pmdi4w1ModifierType1,
	Pmdi4w1ModifierType2,
	Pmdi4w1ModifierPremBasis,
	Pmdi4w1ModBasisMgInd,
	Pmdi4w1ModifierMinPrem,
	Pmdi4w1ModMinPremMgInd,
	Pmdi4w1DepositPremium,
	Pmdi4w1ModifierUslhFac,
	Pmdi4w1AuditSegBuiltInd,
	Pmdi4w1YearRatingExpire,
	Pmdi4w1MonthRatingExpire,
	Pmdi4w1DayRatingExpire,
	Pmdi4w1CoviiLimitId,
	Pmdi4w1YearModAnnivRate,
	Pmdi4w1MonthModAnnivRate,
	Pmdi4w1DayModAnnivRate,
	Pmdi4w1VarDeductibleAmt,
	Pmdi4w1VarAggrDeductAmt,
	Pmdi4w1CaEmodAtIncept,
	Pmdi4w1PmsFutureUse,
	Pmdi4w1CustomerFutureUse,
	Pmdi4w1Yr2000CustUse,
	ExtractDate,
	AsOfDate,
	RecordCount,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_Pif43IXZWCModStage
),
ArchPif43IXZWCModStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchPif43IXZWCModStage
	(Pif43IXZWCModStageId, PifSymbol, PifPolicyNumber, PifModule, Pmdi4w1RecLength, Pmdi4w1ActionCode, Pmdi4w1FileId, Pmdi4w1SegmentId, Pmdi4w1SegmentStatus, Pmdi4w1YearTransaction, Pmdi4w1MonthTransaction, Pmdi4w1DayTransaction, Pmdi4w1SegmentLevelCode, Pmdi4w1SegmentPartCode, Pmdi4w1SubPartCode, Pmdi4w1InsuranceLine, Pmdi4w1WcRatingState, Pmdi4w1LocationNumber, Pmdi4w1ClassOrderCode, Pmdi4w1ClassOrderSeq, Pmdi4w1ReportingClassCode, Pmdi4w1ReportingClassSeq, Pmdi4w1SplitRateSeq, Pmdi4w1YearItemEffective, Pmdi4w1MonthItemEffective, Pmdi4w1DayItemEffective, Pmdi4w1AuditNumber, Pmdi4w1AuditNumSeq, Pmdi4w1YearProcess, Pmdi4w1MonthProcess, Pmdi4w1DayProcess, Pmdi4w1YearItemExpire, Pmdi4w1MonthItemExpire, Pmdi4w1DayItemExpire, Pmdi4w1GeneratedSegInd, Pmdi4w1ModifierDesc, Pmdi4w1ModifierRate, Pmdi4w1ModFactorMdInd, Pmdi4w1ModifierPremium, Pmdi4w1ModPremMgInd, Pmdi4w1ModifierType1, Pmdi4w1ModifierType2, Pmdi4w1ModifierPremBasis, Pmdi4w1ModBasisMgInd, Pmdi4w1ModifierMinPrem, Pmdi4w1ModMinPremMgInd, Pmdi4w1DepositPremium, Pmdi4w1ModifierUslhFac, Pmdi4w1AuditSegBuiltInd, Pmdi4w1YearRatingExpire, Pmdi4w1MonthRatingExpire, Pmdi4w1DayRatingExpire, Pmdi4w1CoviiLimitId, Pmdi4w1YearModAnnivRate, Pmdi4w1MonthModAnnivRate, Pmdi4w1DayModAnnivRate, Pmdi4w1VarDeductibleAmt, Pmdi4w1VarAggrDeductAmt, Pmdi4w1CaEmodAtIncept, Pmdi4w1PmsFutureUse, Pmdi4w1CustomerFutureUse, Pmdi4w1Yr2000CustUse, ExtractDate, AsOfDate, RecordCount, SourceSystemId, AuditId)
	SELECT 
	PIF43IXZWCMODSTAGEID, 
	PIFSYMBOL, 
	PIFPOLICYNUMBER, 
	PIFMODULE, 
	PMDI4W1RECLENGTH, 
	PMDI4W1ACTIONCODE, 
	PMDI4W1FILEID, 
	PMDI4W1SEGMENTID, 
	PMDI4W1SEGMENTSTATUS, 
	PMDI4W1YEARTRANSACTION, 
	PMDI4W1MONTHTRANSACTION, 
	PMDI4W1DAYTRANSACTION, 
	PMDI4W1SEGMENTLEVELCODE, 
	PMDI4W1SEGMENTPARTCODE, 
	PMDI4W1SUBPARTCODE, 
	PMDI4W1INSURANCELINE, 
	PMDI4W1WCRATINGSTATE, 
	PMDI4W1LOCATIONNUMBER, 
	PMDI4W1CLASSORDERCODE, 
	PMDI4W1CLASSORDERSEQ, 
	PMDI4W1REPORTINGCLASSCODE, 
	PMDI4W1REPORTINGCLASSSEQ, 
	PMDI4W1SPLITRATESEQ, 
	PMDI4W1YEARITEMEFFECTIVE, 
	PMDI4W1MONTHITEMEFFECTIVE, 
	PMDI4W1DAYITEMEFFECTIVE, 
	PMDI4W1AUDITNUMBER, 
	PMDI4W1AUDITNUMSEQ, 
	PMDI4W1YEARPROCESS, 
	PMDI4W1MONTHPROCESS, 
	PMDI4W1DAYPROCESS, 
	PMDI4W1YEARITEMEXPIRE, 
	PMDI4W1MONTHITEMEXPIRE, 
	PMDI4W1DAYITEMEXPIRE, 
	PMDI4W1GENERATEDSEGIND, 
	PMDI4W1MODIFIERDESC, 
	PMDI4W1MODIFIERRATE, 
	PMDI4W1MODFACTORMDIND, 
	PMDI4W1MODIFIERPREMIUM, 
	PMDI4W1MODPREMMGIND, 
	PMDI4W1MODIFIERTYPE1, 
	PMDI4W1MODIFIERTYPE2, 
	PMDI4W1MODIFIERPREMBASIS, 
	PMDI4W1MODBASISMGIND, 
	PMDI4W1MODIFIERMINPREM, 
	PMDI4W1MODMINPREMMGIND, 
	PMDI4W1DEPOSITPREMIUM, 
	PMDI4W1MODIFIERUSLHFAC, 
	PMDI4W1AUDITSEGBUILTIND, 
	PMDI4W1YEARRATINGEXPIRE, 
	PMDI4W1MONTHRATINGEXPIRE, 
	PMDI4W1DAYRATINGEXPIRE, 
	PMDI4W1COVIILIMITID, 
	PMDI4W1YEARMODANNIVRATE, 
	PMDI4W1MONTHMODANNIVRATE, 
	PMDI4W1DAYMODANNIVRATE, 
	PMDI4W1VARDEDUCTIBLEAMT, 
	PMDI4W1VARAGGRDEDUCTAMT, 
	PMDI4W1CAEMODATINCEPT, 
	PMDI4W1PMSFUTUREUSE, 
	PMDI4W1CUSTOMERFUTUREUSE, 
	PMDI4W1YR2000CUSTUSE, 
	EXTRACTDATE, 
	ASOFDATE, 
	RECORDCOUNT, 
	SOURCESYSTEMID, 
	AUDITID
	FROM EXP_Pif43IXZWCModStage
),
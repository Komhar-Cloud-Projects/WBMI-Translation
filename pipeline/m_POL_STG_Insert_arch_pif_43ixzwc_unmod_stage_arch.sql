WITH
SQ_Pif43IXZWCUnmodStage AS (
	SELECT
		Pif43IXZWCUnmodStageId,
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
		Pmdi4w1IntrastateIdNum,
		Pmdi4w1UnmodStatePrem,
		Pmdi4w1StateTermPrem,
		Pmdi4w1DividendPlanInd,
		Pmdi4w1AircrftSeatMaxPrm,
		Pmdi4w1UslhPercent,
		Pmdi4w1CoverageFlag,
		Pmdi4w1DepositPremPercent,
		Pmdi4w1ClassCoverageInd,
		Pmdi4w1VolCompCovFlag,
		Pmdi4w1FederalCoverageCode,
		Pmdi4w1HighestMinPrem,
		Pmdi4w1NonPremInfo,
		Pmdi4w1AArFileNumber,
		Pmdi4w1BArArNumberOfEmpl,
		Pmdi4w1ADcNumberOfEmpl,
		Pmdi4w1AHiUnemplmtNum,
		Pmdi4w1AKyNumberOfEmpl,
		Pmdi4w1BKyEmplIdNumber,
		Pmdi4w1AMeUnemplmtNumber,
		Pmdi4w1BMdDcBenefitsInd,
		Pmdi4w1AMiNumberOfEmpl,
		Pmdi4w1AMnAssocNumber,
		Pmdi4w1BMnBureauFileNo,
		Pmdi4w1ANhNumberOfEmpl,
		Pmdi4w1ANmUnempltNumber,
		Pmdi4w1BNmSafetyDevInd,
		Pmdi4w1NmSftyDevFactor,
		Pmdi4w1ANcEmplCodeNum,
		Pmdi4w1AOrWcdContrNum,
		Pmdi4w1ABureauFileNum,
		Pmdi4w1BVaDcBenefitsInd,
		Pmdi4w1VaDcBenefitsFctr,
		Pmdi4w1NsAuditSegBuiltInd,
		Pmdi4w1YearNonStatRateExp,
		Pmdi4w1MthNonStatRateExp,
		Pmdi4w1DayNonStatRateExp,
		Pmdi4w1YearAnnRateDate,
		Pmdi4w1MonthAnnRateDate,
		Pmdi4w1DayAnnRateDate,
		Pmdi4w1CoviiPiecesErrorSw,
		Pmdi4w1CoviiStdMinimum,
		Pmdi4w1CoviiAdmMinimum,
		Pmdi4w1PmsFutureUse,
		Pmdi4w1WbmDividendPaid,
		Pmdi4w1AcctFormA,
		Pmdi4w1AcctFormB,
		Pmdi4w1AcctFormC,
		Pmdi4w1AcctFormPrint,
		Pmdi4w1WbmPrevDivPaid,
		Pmdi4w1AcctFormD,
		Pmdi4w1HoldClaimAmount,
		Pmdi4w1DividendPercent,
		Pmdi4w1TerrorismPrem,
		Pmdi4w1DtecPrem,
		Pmdi4w1Yr2000CustUse,
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemId
	FROM SRC_Pif43IXZWCUnmodStage
),
EXP_VALUE AS (
	SELECT
	Pif43IXZWCUnmodStageId,
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
	Pmdi4w1IntrastateIdNum,
	Pmdi4w1UnmodStatePrem,
	Pmdi4w1StateTermPrem,
	Pmdi4w1DividendPlanInd,
	Pmdi4w1AircrftSeatMaxPrm,
	Pmdi4w1UslhPercent,
	Pmdi4w1CoverageFlag,
	Pmdi4w1DepositPremPercent,
	Pmdi4w1ClassCoverageInd,
	Pmdi4w1VolCompCovFlag,
	Pmdi4w1FederalCoverageCode,
	Pmdi4w1HighestMinPrem,
	Pmdi4w1NonPremInfo,
	Pmdi4w1AArFileNumber,
	Pmdi4w1BArArNumberOfEmpl,
	Pmdi4w1ADcNumberOfEmpl,
	Pmdi4w1AHiUnemplmtNum,
	Pmdi4w1AKyNumberOfEmpl,
	Pmdi4w1BKyEmplIdNumber,
	Pmdi4w1AMeUnemplmtNumber,
	Pmdi4w1BMdDcBenefitsInd,
	Pmdi4w1AMiNumberOfEmpl,
	Pmdi4w1AMnAssocNumber,
	Pmdi4w1BMnBureauFileNo,
	Pmdi4w1ANhNumberOfEmpl,
	Pmdi4w1ANmUnempltNumber,
	Pmdi4w1BNmSafetyDevInd,
	Pmdi4w1NmSftyDevFactor,
	Pmdi4w1ANcEmplCodeNum,
	Pmdi4w1AOrWcdContrNum,
	Pmdi4w1ABureauFileNum,
	Pmdi4w1BVaDcBenefitsInd,
	Pmdi4w1VaDcBenefitsFctr,
	Pmdi4w1NsAuditSegBuiltInd,
	Pmdi4w1YearNonStatRateExp,
	Pmdi4w1MthNonStatRateExp,
	Pmdi4w1DayNonStatRateExp,
	Pmdi4w1YearAnnRateDate,
	Pmdi4w1MonthAnnRateDate,
	Pmdi4w1DayAnnRateDate,
	Pmdi4w1CoviiPiecesErrorSw,
	Pmdi4w1CoviiStdMinimum,
	Pmdi4w1CoviiAdmMinimum,
	Pmdi4w1PmsFutureUse,
	Pmdi4w1WbmDividendPaid,
	Pmdi4w1AcctFormA,
	Pmdi4w1AcctFormB,
	Pmdi4w1AcctFormC,
	Pmdi4w1AcctFormPrint,
	Pmdi4w1WbmPrevDivPaid,
	Pmdi4w1AcctFormD,
	Pmdi4w1HoldClaimAmount,
	Pmdi4w1DividendPercent,
	Pmdi4w1TerrorismPrem,
	Pmdi4w1DtecPrem,
	Pmdi4w1Yr2000CustUse,
	ExtractDate,
	AsOfDate,
	RecordCount,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_Pif43IXZWCUnmodStage
),
TGT_ArchPif43IXZWCUnmodStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchPif43IXZWCUnmodStage
	(Pif43IXZWCUnmodStageId, PifSymbol, PifPolicyNumber, PifModule, Pmdi4w1RecLength, Pmdi4w1ActionCode, Pmdi4w1FileId, Pmdi4w1SegmentId, Pmdi4w1SegmentStatus, Pmdi4w1YearTransaction, Pmdi4w1MonthTransaction, Pmdi4w1DayTransaction, Pmdi4w1SegmentLevelCode, Pmdi4w1SegmentPartCode, Pmdi4w1SubPartCode, Pmdi4w1InsuranceLine, Pmdi4w1WcRatingState, Pmdi4w1LocationNumber, Pmdi4w1ClassOrderCode, Pmdi4w1ClassOrderSeq, Pmdi4w1ReportingClassCode, Pmdi4w1ReportingClassSeq, Pmdi4w1SplitRateSeq, Pmdi4w1YearItemEffective, Pmdi4w1MonthItemEffective, Pmdi4w1DayItemEffective, Pmdi4w1AuditNumber, Pmdi4w1AuditNumSeq, Pmdi4w1YearProcess, Pmdi4w1MonthProcess, Pmdi4w1DayProcess, Pmdi4w1YearItemExpire, Pmdi4w1MonthItemExpire, Pmdi4w1DayItemExpire, Pmdi4w1IntrastateIdNum, Pmdi4w1UnmodStatePrem, Pmdi4w1StateTermPrem, Pmdi4w1DividendPlanInd, Pmdi4w1AircrftSeatMaxPrm, Pmdi4w1UslhPercent, Pmdi4w1CoverageFlag, Pmdi4w1DepositPremPercent, Pmdi4w1ClassCoverageInd, Pmdi4w1VolCompCovFlag, Pmdi4w1FederalCoverageCode, Pmdi4w1HighestMinPrem, Pmdi4w1NonPremInfo, Pmdi4w1AArFileNumber, Pmdi4w1BArArNumberOfEmpl, Pmdi4w1ADcNumberOfEmpl, Pmdi4w1AHiUnemplmtNum, Pmdi4w1AKyNumberOfEmpl, Pmdi4w1BKyEmplIdNumber, Pmdi4w1AMeUnemplmtNumber, Pmdi4w1BMdDcBenefitsInd, Pmdi4w1AMiNumberOfEmpl, Pmdi4w1AMnAssocNumber, Pmdi4w1BMnBureauFileNo, Pmdi4w1ANhNumberOfEmpl, Pmdi4w1ANmUnempltNumber, Pmdi4w1BNmSafetyDevInd, Pmdi4w1NmSftyDevFactor, Pmdi4w1ANcEmplCodeNum, Pmdi4w1AOrWcdContrNum, Pmdi4w1ABureauFileNum, Pmdi4w1BVaDcBenefitsInd, Pmdi4w1VaDcBenefitsFctr, Pmdi4w1NsAuditSegBuiltInd, Pmdi4w1YearNonStatRateExp, Pmdi4w1MthNonStatRateExp, Pmdi4w1DayNonStatRateExp, Pmdi4w1YearAnnRateDate, Pmdi4w1MonthAnnRateDate, Pmdi4w1DayAnnRateDate, Pmdi4w1CoviiPiecesErrorSw, Pmdi4w1CoviiStdMinimum, Pmdi4w1CoviiAdmMinimum, Pmdi4w1PmsFutureUse, Pmdi4w1WbmDividendPaid, Pmdi4w1AcctFormA, Pmdi4w1AcctFormB, Pmdi4w1AcctFormC, Pmdi4w1AcctFormPrint, Pmdi4w1WbmPrevDivPaid, Pmdi4w1AcctFormD, Pmdi4w1HoldClaimAmount, Pmdi4w1DividendPercent, Pmdi4w1TerrorismPrem, Pmdi4w1DtecPrem, Pmdi4w1Yr2000CustUse, ExtractDate, AsOfDate, RecordCount, SourceSystemId, AuditId)
	SELECT 
	PIF43IXZWCUNMODSTAGEID, 
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
	PMDI4W1INTRASTATEIDNUM, 
	PMDI4W1UNMODSTATEPREM, 
	PMDI4W1STATETERMPREM, 
	PMDI4W1DIVIDENDPLANIND, 
	PMDI4W1AIRCRFTSEATMAXPRM, 
	PMDI4W1USLHPERCENT, 
	PMDI4W1COVERAGEFLAG, 
	PMDI4W1DEPOSITPREMPERCENT, 
	PMDI4W1CLASSCOVERAGEIND, 
	PMDI4W1VOLCOMPCOVFLAG, 
	PMDI4W1FEDERALCOVERAGECODE, 
	PMDI4W1HIGHESTMINPREM, 
	PMDI4W1NONPREMINFO, 
	PMDI4W1AARFILENUMBER, 
	PMDI4W1BARARNUMBEROFEMPL, 
	PMDI4W1ADCNUMBEROFEMPL, 
	PMDI4W1AHIUNEMPLMTNUM, 
	PMDI4W1AKYNUMBEROFEMPL, 
	PMDI4W1BKYEMPLIDNUMBER, 
	PMDI4W1AMEUNEMPLMTNUMBER, 
	PMDI4W1BMDDCBENEFITSIND, 
	PMDI4W1AMINUMBEROFEMPL, 
	PMDI4W1AMNASSOCNUMBER, 
	PMDI4W1BMNBUREAUFILENO, 
	PMDI4W1ANHNUMBEROFEMPL, 
	PMDI4W1ANMUNEMPLTNUMBER, 
	PMDI4W1BNMSAFETYDEVIND, 
	PMDI4W1NMSFTYDEVFACTOR, 
	PMDI4W1ANCEMPLCODENUM, 
	PMDI4W1AORWCDCONTRNUM, 
	PMDI4W1ABUREAUFILENUM, 
	PMDI4W1BVADCBENEFITSIND, 
	PMDI4W1VADCBENEFITSFCTR, 
	PMDI4W1NSAUDITSEGBUILTIND, 
	PMDI4W1YEARNONSTATRATEEXP, 
	PMDI4W1MTHNONSTATRATEEXP, 
	PMDI4W1DAYNONSTATRATEEXP, 
	PMDI4W1YEARANNRATEDATE, 
	PMDI4W1MONTHANNRATEDATE, 
	PMDI4W1DAYANNRATEDATE, 
	PMDI4W1COVIIPIECESERRORSW, 
	PMDI4W1COVIISTDMINIMUM, 
	PMDI4W1COVIIADMMINIMUM, 
	PMDI4W1PMSFUTUREUSE, 
	PMDI4W1WBMDIVIDENDPAID, 
	PMDI4W1ACCTFORMA, 
	PMDI4W1ACCTFORMB, 
	PMDI4W1ACCTFORMC, 
	PMDI4W1ACCTFORMPRINT, 
	PMDI4W1WBMPREVDIVPAID, 
	PMDI4W1ACCTFORMD, 
	PMDI4W1HOLDCLAIMAMOUNT, 
	PMDI4W1DIVIDENDPERCENT, 
	PMDI4W1TERRORISMPREM, 
	PMDI4W1DTECPREM, 
	PMDI4W1YR2000CUSTUSE, 
	EXTRACTDATE, 
	ASOFDATE, 
	RECORDCOUNT, 
	SOURCESYSTEMID, 
	AUDITID
	FROM EXP_VALUE
),
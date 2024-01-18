WITH
SQ_Pif43RXGLStage AS (
	SELECT
		Pif43RXGLStageId,
		PifSymbol,
		PifPolicyNumber,
		PifModule,
		Pmdrxg1RecLength,
		Pmdrxg1ActionCode,
		Pmdrxg1FileId,
		Pmdrxg1SegmentId,
		Pmdrxg1SegmentStatus,
		Pmdrxg1YearTransaction,
		Pmdrxg1MonthTransaction,
		Pmdrxg1DayTransaction,
		Pmdrxg1SegmentLevelCode,
		Pmdrxg1SegmentPartCode,
		Pmdrxg1SubPartCode,
		Pmdrxg1InsuranceLine,
		Pmdrxg1LocationNumber,
		Pmdrxg1PmsDefGlSubline,
		Pmdrxg1RiskTypeInd,
		Pmdrxg1YearItemEffective,
		Pmdrxg1MonthItemEffective,
		Pmdrxg1DayItemEffective,
		Pmdrxg1YearProcess,
		Pmdrxg1MonthProcess,
		Pmdrxg1DayProcess,
		Pmdrxg1LimitSubline,
		Pmdrxg1LimAggrPd,
		Pmdrxg1LimOcc,
		Pmdrxg1LimOccPd,
		Pmdrxg1FringeCode1,
		Pmdrxg1FringeRate1,
		Pmdrxg1FringeRateMgInd1,
		Pmdrxg1FringeLimit1,
		Pmdrxg1FringeCode2,
		Pmdrxg1FringeRate2,
		Pmdrxg1FringeRateMgInd2,
		Pmdrxg1FringeLimit2,
		Pmdrxg1FringeCode3,
		Pmdrxg1FringeRate3,
		Pmdrxg1FringeRateMgInd3,
		Pmdrxg1FringeLimit3,
		Pmdrxg1AgentsCommRate,
		Pmdrxg1ExperienceMod,
		Pmdrxg1ScheduleMod,
		Pmdrxg1CommRedMod,
		Pmdrxg1ExpenseMod,
		Pmdrxg1OtherMod,
		Pmdrxg1LimitPerPerson,
		Pmdrxg1DecChangeFlag,
		Pmdrxg1AggByJobInd,
		Pmdrxg1AggByLocation,
		Pmdrxg1PmsFutureUse,
		Pmdrxg1VolPdOcc,
		Pmdrxg1VolPdAgg,
		Pmdrxg1CustSplUse,
		Pmdrxg1VolPdDed,
		Pmdrxg1SaleDisposalOcc,
		Pmdrxg1SaleDisposalAgg,
		Pmdrxg1SdDedPerClm,
		Pmdrxg1LimitedPwIncident,
		Pmdrxg1LimitedPwAgg,
		Pmdrxg1LimitedPwDed,
		Pmdrxg1LimitedPwCutoff,
		Pmdrxg1PrinterEoLs,
		Pmdrxg1PrinterEoAgg,
		Pmdrxg1PrinterEoDed,
		Pmdrxg1EmpBlEmp,
		Pmdrxg1EmpBlAgg,
		Pmdrxg1EmpBlDed,
		Pmdrxg1EmpBlRetro,
		Pmdrxg1EdllLs,
		Pmdrxg1EdllAgg,
		Pmdrxg1EdlbIncident,
		Pmdrxg1EdlbAgg,
		Pmdrxg1EdlbRetro,
		Pmdrxg1Yr2000CustUse,
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemId
	FROM Pif43RXGLStage
),
EXP_Values AS (
	SELECT
	Pif43RXGLStageId AS Pif43RXStageId,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	Pmdrxg1RecLength AS Pmdlxg1RecLength,
	Pmdrxg1ActionCode AS Pmdlxg1ActionCode,
	Pmdrxg1FileId AS Pmdlxg1FileId,
	Pmdrxg1SegmentId AS Pmdlxg1SegmentId,
	Pmdrxg1SegmentStatus AS Pmdlxg1SegmentStatus,
	Pmdrxg1YearTransaction AS Pmdlxg1YearTransaction,
	Pmdrxg1MonthTransaction AS Pmdlxg1MonthTransaction,
	Pmdrxg1DayTransaction AS Pmdlxg1DayTransaction,
	Pmdrxg1SegmentLevelCode,
	Pmdrxg1SegmentPartCode,
	Pmdrxg1SubPartCode,
	Pmdrxg1InsuranceLine,
	Pmdrxg1LocationNumber,
	Pmdrxg1PmsDefGlSubline,
	Pmdrxg1RiskTypeInd,
	Pmdrxg1YearItemEffective,
	Pmdrxg1MonthItemEffective,
	Pmdrxg1DayItemEffective,
	Pmdrxg1YearProcess,
	Pmdrxg1MonthProcess,
	Pmdrxg1DayProcess,
	Pmdrxg1LimitSubline,
	Pmdrxg1LimAggrPd,
	Pmdrxg1LimOcc,
	Pmdrxg1LimOccPd,
	Pmdrxg1FringeCode1,
	Pmdrxg1FringeRate1,
	Pmdrxg1FringeRateMgInd1,
	Pmdrxg1FringeLimit1,
	Pmdrxg1FringeCode2,
	Pmdrxg1FringeRate2,
	Pmdrxg1FringeRateMgInd2,
	Pmdrxg1FringeLimit2,
	Pmdrxg1FringeCode3,
	Pmdrxg1FringeRate3,
	Pmdrxg1FringeRateMgInd3,
	Pmdrxg1FringeLimit3,
	Pmdrxg1AgentsCommRate,
	Pmdrxg1ExperienceMod,
	Pmdrxg1ScheduleMod,
	Pmdrxg1CommRedMod,
	Pmdrxg1ExpenseMod,
	Pmdrxg1OtherMod,
	Pmdrxg1LimitPerPerson,
	Pmdrxg1DecChangeFlag,
	Pmdrxg1AggByJobInd,
	Pmdrxg1AggByLocation,
	Pmdrxg1PmsFutureUse,
	Pmdrxg1VolPdOcc,
	Pmdrxg1VolPdAgg,
	Pmdrxg1CustSplUse,
	Pmdrxg1VolPdDed,
	Pmdrxg1SaleDisposalOcc,
	Pmdrxg1SaleDisposalAgg,
	Pmdrxg1SdDedPerClm,
	Pmdrxg1LimitedPwIncident,
	Pmdrxg1LimitedPwAgg,
	Pmdrxg1LimitedPwDed,
	Pmdrxg1LimitedPwCutoff,
	Pmdrxg1PrinterEoLs,
	Pmdrxg1PrinterEoAgg,
	Pmdrxg1PrinterEoDed,
	Pmdrxg1EmpBlEmp,
	Pmdrxg1EmpBlAgg,
	Pmdrxg1EmpBlDed,
	Pmdrxg1EmpBlRetro,
	Pmdrxg1EdllLs,
	Pmdrxg1EdllAgg,
	Pmdrxg1EdlbIncident,
	Pmdrxg1EdlbAgg,
	Pmdrxg1EdlbRetro,
	Pmdrxg1Yr2000CustUse,
	ExtractDate,
	AsOfDate,
	RecordCount,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_Pif43RXGLStage
),
ArchPif43RXGLStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchPif43RXGLStage
	(Pif43RXGLStageId, PifSymbol, PifPolicyNumber, PifModule, Pmdrxg1RecLength, Pmdrxg1ActionCode, Pmdrxg1FileId, Pmdrxg1SegmentId, Pmdrxg1SegmentStatus, Pmdrxg1YearTransaction, Pmdrxg1MonthTransaction, Pmdrxg1DayTransaction, Pmdrxg1SegmentLevelCode, Pmdrxg1SegmentPartCode, Pmdrxg1SubPartCode, Pmdrxg1InsuranceLine, Pmdrxg1LocationNumber, Pmdrxg1PmsDefGlSubline, Pmdrxg1RiskTypeInd, Pmdrxg1YearItemEffective, Pmdrxg1MonthItemEffective, Pmdrxg1DayItemEffective, Pmdrxg1YearProcess, Pmdrxg1MonthProcess, Pmdrxg1DayProcess, Pmdrxg1LimitSubline, Pmdrxg1LimAggrPd, Pmdrxg1LimOcc, Pmdrxg1LimOccPd, Pmdrxg1FringeCode1, Pmdrxg1FringeRate1, Pmdrxg1FringeRateMgInd1, Pmdrxg1FringeLimit1, Pmdrxg1FringeCode2, Pmdrxg1FringeRate2, Pmdrxg1FringeRateMgInd2, Pmdrxg1FringeLimit2, Pmdrxg1FringeCode3, Pmdrxg1FringeRate3, Pmdrxg1FringeRateMgInd3, Pmdrxg1FringeLimit3, Pmdrxg1AgentsCommRate, Pmdrxg1ExperienceMod, Pmdrxg1ScheduleMod, Pmdrxg1CommRedMod, Pmdrxg1ExpenseMod, Pmdrxg1OtherMod, Pmdrxg1LimitPerPerson, Pmdrxg1DecChangeFlag, Pmdrxg1AggByJobInd, Pmdrxg1AggByLocation, Pmdrxg1PmsFutureUse, Pmdrxg1VolPdOcc, Pmdrxg1VolPdAgg, Pmdrxg1CustSplUse, Pmdrxg1VolPdDed, Pmdrxg1SaleDisposalOcc, Pmdrxg1SaleDisposalAgg, Pmdrxg1SdDedPerClm, Pmdrxg1LimitedPwIncident, Pmdrxg1LimitedPwAgg, Pmdrxg1LimitedPwDed, Pmdrxg1LimitedPwCutoff, Pmdrxg1PrinterEoLs, Pmdrxg1PrinterEoAgg, Pmdrxg1PrinterEoDed, Pmdrxg1EmpBlEmp, Pmdrxg1EmpBlAgg, Pmdrxg1EmpBlDed, Pmdrxg1EmpBlRetro, Pmdrxg1EdllLs, Pmdrxg1EdllAgg, Pmdrxg1EdlbIncident, Pmdrxg1EdlbAgg, Pmdrxg1EdlbRetro, Pmdrxg1Yr2000CustUse, ExtractDate, AsOfDate, RecordCount, SourceSystemId, AuditId)
	SELECT 
	Pif43RXStageId AS PIF43RXGLSTAGEID, 
	PIFSYMBOL, 
	PIFPOLICYNUMBER, 
	PIFMODULE, 
	Pmdlxg1RecLength AS PMDRXG1RECLENGTH, 
	Pmdlxg1ActionCode AS PMDRXG1ACTIONCODE, 
	Pmdlxg1FileId AS PMDRXG1FILEID, 
	Pmdlxg1SegmentId AS PMDRXG1SEGMENTID, 
	Pmdlxg1SegmentStatus AS PMDRXG1SEGMENTSTATUS, 
	Pmdlxg1YearTransaction AS PMDRXG1YEARTRANSACTION, 
	Pmdlxg1MonthTransaction AS PMDRXG1MONTHTRANSACTION, 
	Pmdlxg1DayTransaction AS PMDRXG1DAYTRANSACTION, 
	PMDRXG1SEGMENTLEVELCODE, 
	PMDRXG1SEGMENTPARTCODE, 
	PMDRXG1SUBPARTCODE, 
	PMDRXG1INSURANCELINE, 
	PMDRXG1LOCATIONNUMBER, 
	PMDRXG1PMSDEFGLSUBLINE, 
	PMDRXG1RISKTYPEIND, 
	PMDRXG1YEARITEMEFFECTIVE, 
	PMDRXG1MONTHITEMEFFECTIVE, 
	PMDRXG1DAYITEMEFFECTIVE, 
	PMDRXG1YEARPROCESS, 
	PMDRXG1MONTHPROCESS, 
	PMDRXG1DAYPROCESS, 
	PMDRXG1LIMITSUBLINE, 
	PMDRXG1LIMAGGRPD, 
	PMDRXG1LIMOCC, 
	PMDRXG1LIMOCCPD, 
	PMDRXG1FRINGECODE1, 
	PMDRXG1FRINGERATE1, 
	PMDRXG1FRINGERATEMGIND1, 
	PMDRXG1FRINGELIMIT1, 
	PMDRXG1FRINGECODE2, 
	PMDRXG1FRINGERATE2, 
	PMDRXG1FRINGERATEMGIND2, 
	PMDRXG1FRINGELIMIT2, 
	PMDRXG1FRINGECODE3, 
	PMDRXG1FRINGERATE3, 
	PMDRXG1FRINGERATEMGIND3, 
	PMDRXG1FRINGELIMIT3, 
	PMDRXG1AGENTSCOMMRATE, 
	PMDRXG1EXPERIENCEMOD, 
	PMDRXG1SCHEDULEMOD, 
	PMDRXG1COMMREDMOD, 
	PMDRXG1EXPENSEMOD, 
	PMDRXG1OTHERMOD, 
	PMDRXG1LIMITPERPERSON, 
	PMDRXG1DECCHANGEFLAG, 
	PMDRXG1AGGBYJOBIND, 
	PMDRXG1AGGBYLOCATION, 
	PMDRXG1PMSFUTUREUSE, 
	PMDRXG1VOLPDOCC, 
	PMDRXG1VOLPDAGG, 
	PMDRXG1CUSTSPLUSE, 
	PMDRXG1VOLPDDED, 
	PMDRXG1SALEDISPOSALOCC, 
	PMDRXG1SALEDISPOSALAGG, 
	PMDRXG1SDDEDPERCLM, 
	PMDRXG1LIMITEDPWINCIDENT, 
	PMDRXG1LIMITEDPWAGG, 
	PMDRXG1LIMITEDPWDED, 
	PMDRXG1LIMITEDPWCUTOFF, 
	PMDRXG1PRINTEREOLS, 
	PMDRXG1PRINTEREOAGG, 
	PMDRXG1PRINTEREODED, 
	PMDRXG1EMPBLEMP, 
	PMDRXG1EMPBLAGG, 
	PMDRXG1EMPBLDED, 
	PMDRXG1EMPBLRETRO, 
	PMDRXG1EDLLLS, 
	PMDRXG1EDLLAGG, 
	PMDRXG1EDLBINCIDENT, 
	PMDRXG1EDLBAGG, 
	PMDRXG1EDLBRETRO, 
	PMDRXG1YR2000CUSTUSE, 
	EXTRACTDATE, 
	ASOFDATE, 
	RECORDCOUNT, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Values
),
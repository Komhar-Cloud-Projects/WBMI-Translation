WITH
SQ_Pif43LXGAStage AS (
	SELECT
		Pif43LXGAStageId,
		ExtractDate,
		SourceSystemid,
		PifSymbol,
		PifPolicyNumber,
		PifModule,
		PMDLXA1SegmentId,
		PMDLXA1SegmentStatus,
		PMDLXA1YearTransaction,
		PMDLXA1MonthTransaction,
		PMDLXA1DayTransaction,
		PMDLXA1SegmentLevelCode,
		PMDLXA1SegmentPartCode,
		PMDLXA1SubPartCode,
		PMDLXA1InsuranceLine,
		PMDLXA1LocationNumber,
		PMDLXA1YearItemEffective,
		PMDLXA1MonthItemEffective,
		PMDLXA1DayItemEffective,
		PMDLXA1YearProcess,
		PMDLXA1MonthProcess,
		PMDLXA1DayProcess,
		PMDLXA1YearOrigProcess,
		PMDLXA1MonthOrigProcess,
		PMDLXA1DayOrigProcess,
		PMDLXA1PolicyCompany,
		PMDLXA1LiabilityAudit,
		PMDLXA1PhysicalDamage,
		PMDLXA1BroadenedCoverage,
		PMDLXA1Pilg1,
		PMDLXA1Pilg2,
		PMDLXA1Pilg3,
		PMDLXA1DelExcC,
		PMDLXA1BroadFormProducts,
		PMDLXA1Delete100Comp,
		PMDLXA1CompanyDeviation,
		PMDLXA1BlkColAdjFac,
		PMDLXA1TermFactor,
		PMDLXA1BlkColValuFac,
		PMDLXA1SaveLiabUxAc,
		PMDLXA1LiabUxTransDate,
		PMDLXA1SaveLiabRxAc,
		PMDLXA1LiabRxTransDate,
		PMDLXA1AgentsCommRate,
		PMDLXA1FireLegalLiab,
		PMDLXA1PmaCode,
		PMDLXA1RateLevelDate,
		PMDLXA1WorkSheetInd,
		PMDLXA1PmsFutureUse,
		PMDLXA1CustomerUse,
		PMDLXA1Yr2000CustUse
	FROM Pif43LXGAStage
),
EXP_Pif43LXGAStage AS (
	SELECT
	Pif43LXGAStageId,
	ExtractDate,
	SourceSystemid,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	PMDLXA1SegmentId,
	PMDLXA1SegmentStatus,
	PMDLXA1YearTransaction,
	PMDLXA1MonthTransaction,
	PMDLXA1DayTransaction,
	PMDLXA1SegmentLevelCode,
	PMDLXA1SegmentPartCode,
	PMDLXA1SubPartCode,
	PMDLXA1InsuranceLine,
	PMDLXA1LocationNumber,
	PMDLXA1YearItemEffective,
	PMDLXA1MonthItemEffective,
	PMDLXA1DayItemEffective,
	PMDLXA1YearProcess,
	PMDLXA1MonthProcess,
	PMDLXA1DayProcess,
	PMDLXA1YearOrigProcess,
	PMDLXA1MonthOrigProcess,
	PMDLXA1DayOrigProcess,
	PMDLXA1PolicyCompany,
	PMDLXA1LiabilityAudit,
	PMDLXA1PhysicalDamage,
	PMDLXA1BroadenedCoverage,
	PMDLXA1Pilg1,
	PMDLXA1Pilg2,
	PMDLXA1Pilg3,
	PMDLXA1DelExcC,
	PMDLXA1BroadFormProducts,
	PMDLXA1Delete100Comp,
	PMDLXA1CompanyDeviation,
	PMDLXA1BlkColAdjFac,
	PMDLXA1TermFactor,
	PMDLXA1BlkColValuFac,
	PMDLXA1SaveLiabUxAc,
	PMDLXA1LiabUxTransDate,
	PMDLXA1SaveLiabRxAc,
	PMDLXA1LiabRxTransDate,
	PMDLXA1AgentsCommRate,
	PMDLXA1FireLegalLiab,
	PMDLXA1PmaCode,
	PMDLXA1RateLevelDate,
	PMDLXA1WorkSheetInd,
	PMDLXA1PmsFutureUse,
	PMDLXA1CustomerUse,
	PMDLXA1Yr2000CustUse,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_audit_id
	FROM SQ_Pif43LXGAStage
),
ArchPif43LXGAStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchPif43LXGAStage
	(ExtractDate, SourceSystemid, AuditId, PifSymbol, PifPolicyNumber, PifModule, PMDLXA1SegmentId, PMDLXA1SegmentStatus, PMDLXA1YearTransaction, PMDLXA1MonthTransaction, PMDLXA1DayTransaction, PMDLXA1SegmentLevelCode, PMDLXA1SegmentPartCode, PMDLXA1SubPartCode, PMDLXA1InsuranceLine, PMDLXA1LocationNumber, PMDLXA1YearItemEffective, PMDLXA1MonthItemEffective, PMDLXA1DayItemEffective, PMDLXA1YearProcess, PMDLXA1MonthProcess, PMDLXA1DayProcess, PMDLXA1YearOrigProcess, PMDLXA1MonthOrigProcess, PMDLXA1DayOrigProcess, PMDLXA1PolicyCompany, PMDLXA1LiabilityAudit, PMDLXA1PhysicalDamage, PMDLXA1BroadenedCoverage, PMDLXA1Pilg1, PMDLXA1Pilg2, PMDLXA1Pilg3, PMDLXA1DelExcC, PMDLXA1BroadFormProducts, PMDLXA1Delete100Comp, PMDLXA1CompanyDeviation, PMDLXA1BlkColAdjFac, PMDLXA1TermFactor, PMDLXA1BlkColValuFac, PMDLXA1SaveLiabUxAc, PMDLXA1LiabUxTransDate, PMDLXA1SaveLiabRxAc, PMDLXA1LiabRxTransDate, PMDLXA1AgentsCommRate, PMDLXA1FireLegalLiab, PMDLXA1PmaCode, PMDLXA1RateLevelDate, PMDLXA1WorkSheetInd, PMDLXA1PmsFutureUse, PMDLXA1CustomerUse, PMDLXA1Yr2000CustUse)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_audit_id AS AUDITID, 
	PIFSYMBOL, 
	PIFPOLICYNUMBER, 
	PIFMODULE, 
	PMDLXA1SEGMENTID, 
	PMDLXA1SEGMENTSTATUS, 
	PMDLXA1YEARTRANSACTION, 
	PMDLXA1MONTHTRANSACTION, 
	PMDLXA1DAYTRANSACTION, 
	PMDLXA1SEGMENTLEVELCODE, 
	PMDLXA1SEGMENTPARTCODE, 
	PMDLXA1SUBPARTCODE, 
	PMDLXA1INSURANCELINE, 
	PMDLXA1LOCATIONNUMBER, 
	PMDLXA1YEARITEMEFFECTIVE, 
	PMDLXA1MONTHITEMEFFECTIVE, 
	PMDLXA1DAYITEMEFFECTIVE, 
	PMDLXA1YEARPROCESS, 
	PMDLXA1MONTHPROCESS, 
	PMDLXA1DAYPROCESS, 
	PMDLXA1YEARORIGPROCESS, 
	PMDLXA1MONTHORIGPROCESS, 
	PMDLXA1DAYORIGPROCESS, 
	PMDLXA1POLICYCOMPANY, 
	PMDLXA1LIABILITYAUDIT, 
	PMDLXA1PHYSICALDAMAGE, 
	PMDLXA1BROADENEDCOVERAGE, 
	PMDLXA1PILG1, 
	PMDLXA1PILG2, 
	PMDLXA1PILG3, 
	PMDLXA1DELEXCC, 
	PMDLXA1BROADFORMPRODUCTS, 
	PMDLXA1DELETE100COMP, 
	PMDLXA1COMPANYDEVIATION, 
	PMDLXA1BLKCOLADJFAC, 
	PMDLXA1TERMFACTOR, 
	PMDLXA1BLKCOLVALUFAC, 
	PMDLXA1SAVELIABUXAC, 
	PMDLXA1LIABUXTRANSDATE, 
	PMDLXA1SAVELIABRXAC, 
	PMDLXA1LIABRXTRANSDATE, 
	PMDLXA1AGENTSCOMMRATE, 
	PMDLXA1FIRELEGALLIAB, 
	PMDLXA1PMACODE, 
	PMDLXA1RATELEVELDATE, 
	PMDLXA1WORKSHEETIND, 
	PMDLXA1PMSFUTUREUSE, 
	PMDLXA1CUSTOMERUSE, 
	PMDLXA1YR2000CUSTUSE
	FROM EXP_Pif43LXGAStage
),
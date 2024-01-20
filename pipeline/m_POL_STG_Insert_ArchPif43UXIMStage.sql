WITH
SQ_Pif43UXIMStage AS (
	SELECT
		Pif43UXIMStageId,
		ExtractDate,
		SourceSystemid,
		PifSymbol,
		PifPolicyNumber,
		PifModule,
		PMDUXI1SegmentId,
		PMDUXI1SegmentStatus,
		PMDUXI1YearTransaction,
		PMDUXI1MonthTransaction,
		PMDUXI1DayTransaction,
		PMDUXI1SegmentLevelCode,
		PMDUXI1SegmentPartCode,
		PMDUXI1SubPartCode,
		PMDUXI1InsuranceLine,
		PMDUXI1LocationNumber,
		PMDUXI1SubLocationNumber,
		PMDUXI1RiskUnitGroup,
		PMDUXI1RiskUnitGroupSeq,
		PMDUXI1ItemNumber,
		PMDUXI1RiskSequence,
		PMDUXI1RiskTypeInd,
		PMDUXI1YearItemEffective,
		PMDUXI1MonthItemEffective,
		PMDUXI1DayItemEffective,
		PMDUXI1VariableKey,
		PMDUXI1YearProcess,
		PMDUXI1MonthProcess,
		PMDUXI1DayProcess,
		PMDUXI1RatingState,
		PMDUXI1ItemDescLine1,
		PMDUXI1LimitN,
		PMDUXI1SignsPosition,
		PMDUXI1DecChangeFlag,
		PMDUXI1ExtensionInd,
		PMDUXI1ItemDescLine3,
		PMDUXI1ItemDescLine4,
		PMDUXI1ItemDescLine5,
		PMDUXI1ItemDescLine6,
		PMDUXI1ItemDescLine7,
		PMDUXI1ItemDescLine8,
		PMDUXI1Limit2N,
		PMDUXI1PmscFutureUse
	FROM Pif43UXIMStage
),
EXP_Pif43UXIMStage AS (
	SELECT
	Pif43UXIMStageId,
	ExtractDate,
	SourceSystemid,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	PMDUXI1SegmentId,
	PMDUXI1SegmentStatus,
	PMDUXI1YearTransaction,
	PMDUXI1MonthTransaction,
	PMDUXI1DayTransaction,
	PMDUXI1SegmentLevelCode,
	PMDUXI1SegmentPartCode,
	PMDUXI1SubPartCode,
	PMDUXI1InsuranceLine,
	PMDUXI1LocationNumber,
	PMDUXI1SubLocationNumber,
	PMDUXI1RiskUnitGroup,
	PMDUXI1RiskUnitGroupSeq,
	PMDUXI1ItemNumber,
	PMDUXI1RiskSequence,
	PMDUXI1RiskTypeInd,
	PMDUXI1YearItemEffective,
	PMDUXI1MonthItemEffective,
	PMDUXI1DayItemEffective,
	PMDUXI1VariableKey,
	PMDUXI1YearProcess,
	PMDUXI1MonthProcess,
	PMDUXI1DayProcess,
	PMDUXI1RatingState,
	PMDUXI1ItemDescLine1,
	PMDUXI1LimitN,
	PMDUXI1SignsPosition,
	PMDUXI1DecChangeFlag,
	PMDUXI1ExtensionInd,
	PMDUXI1ItemDescLine3,
	PMDUXI1ItemDescLine4,
	PMDUXI1ItemDescLine5,
	PMDUXI1ItemDescLine6,
	PMDUXI1ItemDescLine7,
	PMDUXI1ItemDescLine8,
	PMDUXI1Limit2N,
	PMDUXI1PmscFutureUse,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_audit_id
	FROM SQ_Pif43UXIMStage
),
ArchPif43UXIMStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchPif43UXIMStage
	(ExtractDate, SourceSystemid, AuditId, PifSymbol, PifPolicyNumber, PifModule, PMDUXI1SegmentId, PMDUXI1SegmentStatus, PMDUXI1YearTransaction, PMDUXI1MonthTransaction, PMDUXI1DayTransaction, PMDUXI1SegmentLevelCode, PMDUXI1SegmentPartCode, PMDUXI1SubPartCode, PMDUXI1InsuranceLine, PMDUXI1LocationNumber, PMDUXI1SubLocationNumber, PMDUXI1RiskUnitGroup, PMDUXI1RiskUnitGroupSeq, PMDUXI1ItemNumber, PMDUXI1RiskSequence, PMDUXI1RiskTypeInd, PMDUXI1YearItemEffective, PMDUXI1MonthItemEffective, PMDUXI1DayItemEffective, PMDUXI1VariableKey, PMDUXI1YearProcess, PMDUXI1MonthProcess, PMDUXI1DayProcess, PMDUXI1RatingState, PMDUXI1ItemDescLine1, PMDUXI1LimitN, PMDUXI1SignsPosition, PMDUXI1DecChangeFlag, PMDUXI1ExtensionInd, PMDUXI1ItemDescLine3, PMDUXI1ItemDescLine4, PMDUXI1ItemDescLine5, PMDUXI1ItemDescLine6, PMDUXI1ItemDescLine7, PMDUXI1ItemDescLine8, PMDUXI1Limit2N, PMDUXI1PmscFutureUse)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_audit_id AS AUDITID, 
	PIFSYMBOL, 
	PIFPOLICYNUMBER, 
	PIFMODULE, 
	PMDUXI1SEGMENTID, 
	PMDUXI1SEGMENTSTATUS, 
	PMDUXI1YEARTRANSACTION, 
	PMDUXI1MONTHTRANSACTION, 
	PMDUXI1DAYTRANSACTION, 
	PMDUXI1SEGMENTLEVELCODE, 
	PMDUXI1SEGMENTPARTCODE, 
	PMDUXI1SUBPARTCODE, 
	PMDUXI1INSURANCELINE, 
	PMDUXI1LOCATIONNUMBER, 
	PMDUXI1SUBLOCATIONNUMBER, 
	PMDUXI1RISKUNITGROUP, 
	PMDUXI1RISKUNITGROUPSEQ, 
	PMDUXI1ITEMNUMBER, 
	PMDUXI1RISKSEQUENCE, 
	PMDUXI1RISKTYPEIND, 
	PMDUXI1YEARITEMEFFECTIVE, 
	PMDUXI1MONTHITEMEFFECTIVE, 
	PMDUXI1DAYITEMEFFECTIVE, 
	PMDUXI1VARIABLEKEY, 
	PMDUXI1YEARPROCESS, 
	PMDUXI1MONTHPROCESS, 
	PMDUXI1DAYPROCESS, 
	PMDUXI1RATINGSTATE, 
	PMDUXI1ITEMDESCLINE1, 
	PMDUXI1LIMITN, 
	PMDUXI1SIGNSPOSITION, 
	PMDUXI1DECCHANGEFLAG, 
	PMDUXI1EXTENSIONIND, 
	PMDUXI1ITEMDESCLINE3, 
	PMDUXI1ITEMDESCLINE4, 
	PMDUXI1ITEMDESCLINE5, 
	PMDUXI1ITEMDESCLINE6, 
	PMDUXI1ITEMDESCLINE7, 
	PMDUXI1ITEMDESCLINE8, 
	PMDUXI1LIMIT2N, 
	PMDUXI1PMSCFUTUREUSE
	FROM EXP_Pif43UXIMStage
),
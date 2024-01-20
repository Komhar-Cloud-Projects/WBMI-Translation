WITH
SQ_WB_WC_Dividend AS (
	WITH cte_WBWCDividend(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.WC_LineId, 
	X.WB_WC_DividendId, 
	X.SessionId, 
	X.DividendType, 
	X.ManualIncurredLossRatio, 
	X.ManualDividendPercentagePaid, 
	X.ManualGrossVariableDividendPercentage, 
	X.ManualTotalEarnedPremium, 
	X.ManualPreviouslyPaidDividentPercentage, 
	X.ManualNetVariableDividendPercentage, 
	X.ManualNetDividendAmount, 
	X.ManualPreviouslyNetVariableDividendPercentage, 
	X.ManualPreviouslyPaidNetVariableDividend, 
	X.State, 
	X.ProjectedDividendDate, 
	X.DividendPaid, 
	X.DividendPaidDate, 
	X.InDividendSelectEditMode, 
	X.DividendSoftMessage, 
	X.DividendMultiStateSoftMessage, 
	X.ArgentCappedFlatDividendPremium, 
	X.ArgentFlatDividendPremium, 
	X.ArgentFlatVariableDividendPremium, 
	X.ArgentVariableDividendPremium, 
	X.CLNSICappedFlatDividendPremium, 
	X.CLNSIFlatDividendPremium, 
	X.CLNSIFlatVariableDividendPremium, 
	X.CLNSIVariableDividendPremium, 
	X.DividendIncurredLossRatio, 
	X.Premium, 
	X.Percentage, 
	X.ManualDividendCalculation, 
	X.DividendOptions 
	FROM  
	WB_WC_Dividend X
	inner join
	cte_WBWCDividend Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Values AS (
	SELECT
	InDividendSelectEditMode AS i_InDividendSelectEditMode,
	WC_LineId,
	WB_WC_DividendId,
	SessionId,
	DividendType,
	ManualIncurredLossRatio,
	ManualDividendPercentagePaid,
	ManualGrossVariableDividendPercentage,
	ManualTotalEarnedPremium,
	ManualPreviouslyPaidDividentPercentage,
	ManualNetVariableDividendPercentage,
	ManualNetDividendAmount,
	ManualPreviouslyNetVariableDividendPercentage,
	ManualPreviouslyPaidNetVariableDividend,
	State,
	ProjectedDividendDate,
	DividendPaid,
	DividendPaidDate,
	DividendSoftMessage,
	DividendMultiStateSoftMessage,
	ArgentCappedFlatDividendPremium,
	ArgentFlatDividendPremium,
	ArgentFlatVariableDividendPremium,
	ArgentVariableDividendPremium,
	CLNSICappedFlatDividendPremium,
	CLNSIFlatDividendPremium,
	CLNSIFlatVariableDividendPremium,
	CLNSIVariableDividendPremium,
	DividendIncurredLossRatio,
	Premium,
	Percentage,
	ManualDividendCalculation,
	-- *INF*: DECODE(ManualDividendCalculation,'T','1','F','0')
	DECODE(
	    ManualDividendCalculation,
	    'T', '1',
	    'F', '0'
	) AS o_ManualDividendCalculation,
	DividendOptions,
	-- *INF*: DECODE(i_InDividendSelectEditMode,'T','1','F','0')
	DECODE(
	    i_InDividendSelectEditMode,
	    'T', '1',
	    'F', '0'
	) AS o_InDividendSelectEditMode,
	SYSDATE AS o_ExtractDate,
	SYSDATE AS o_AsOfDate,
	NULL AS o_RecordCount,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_WC_Dividend
),
WBWCDividendStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBWCDividendStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBWCDividendStage
	(ExtractDate, SourceSystemId, WBWCDividendId, SessionId, DividendType, ManualDividendCalculation, DividendOptions, ManualIncurredLossRatio, ManualDividendPercentagePaid, ManualGrossVariableDividendPercentage, ManualTotalEarnedPremium, ManualPreviouslyPaidDividentPercentage, ManualNetVariableDividendPercentage, ManualNetDividendAmount, ManualPreviouslyNetVariableDividendPercentage, ManualPreviouslyPaidNetVariableDividend, State, ProjectedDividendDate, DividendPaid, DividendPaidDate, InDividendSelectEditMode, DividendSoftMessage, DividendMultiStateSoftMessage, ArgentCappedFlatDividendPremium, ArgentFlatDividendPremium, ArgentFlatVariableDividendPremium, ArgentVariableDividendPremium, CLNSICappedFlatDividendPremium, CLNSIFlatDividendPremium, CLNSIFlatVariableDividendPremium, CLNSIVariableDividendPremium, DividendIncurredLossRatio, Premium, Percentage, WCLineId)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	WB_WC_DividendId AS WBWCDIVIDENDID, 
	SESSIONID, 
	DIVIDENDTYPE, 
	o_ManualDividendCalculation AS MANUALDIVIDENDCALCULATION, 
	DIVIDENDOPTIONS, 
	MANUALINCURREDLOSSRATIO, 
	MANUALDIVIDENDPERCENTAGEPAID, 
	MANUALGROSSVARIABLEDIVIDENDPERCENTAGE, 
	MANUALTOTALEARNEDPREMIUM, 
	MANUALPREVIOUSLYPAIDDIVIDENTPERCENTAGE, 
	MANUALNETVARIABLEDIVIDENDPERCENTAGE, 
	MANUALNETDIVIDENDAMOUNT, 
	MANUALPREVIOUSLYNETVARIABLEDIVIDENDPERCENTAGE, 
	MANUALPREVIOUSLYPAIDNETVARIABLEDIVIDEND, 
	STATE, 
	PROJECTEDDIVIDENDDATE, 
	DIVIDENDPAID, 
	DIVIDENDPAIDDATE, 
	o_InDividendSelectEditMode AS INDIVIDENDSELECTEDITMODE, 
	DIVIDENDSOFTMESSAGE, 
	DIVIDENDMULTISTATESOFTMESSAGE, 
	ARGENTCAPPEDFLATDIVIDENDPREMIUM, 
	ARGENTFLATDIVIDENDPREMIUM, 
	ARGENTFLATVARIABLEDIVIDENDPREMIUM, 
	ARGENTVARIABLEDIVIDENDPREMIUM, 
	CLNSICAPPEDFLATDIVIDENDPREMIUM, 
	CLNSIFLATDIVIDENDPREMIUM, 
	CLNSIFLATVARIABLEDIVIDENDPREMIUM, 
	CLNSIVARIABLEDIVIDENDPREMIUM, 
	DIVIDENDINCURREDLOSSRATIO, 
	PREMIUM, 
	PERCENTAGE, 
	WC_LineId AS WCLINEID
	FROM EXP_Values
),
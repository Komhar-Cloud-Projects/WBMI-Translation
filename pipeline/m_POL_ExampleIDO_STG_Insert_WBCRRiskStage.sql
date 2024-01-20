WITH
SQ_WB_CR_Risk AS (
	WITH cte_WBCRRisk(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CR_RiskId, 
	X.WB_CR_RiskId, 
	X.SessionId, 
	X.Indicator28, 
	X.Indicator85, 
	X.CoverageWritten, 
	X.Indicator, 
	X.EndorsementSafeDepositBoxTransfer, 
	X.EndorsementIncludeMoneyForInsurance, 
	X.EndorsementIncludeDesignatedAgents, 
	X.EndorsementAddFaithfulPerformance, 
	X.EndorsementIncludeSecurities, 
	X.EndorsementIncludeBulky, 
	X.EndorsemenFaithfulPerformanceEmployeeorPosition, 
	X.EndorsementAddBlanketExcessLimit, 
	X.EndorsementEmployeeTheftExcessOver, 
	X.EndorsementAddScheduleExcessLimit, 
	X.EndorsementAddScheduleExcessLimitFor35, 
	X.EndorsementAddTradingCoverage, 
	X.EndorsmentFaithfulDutyCoverage, 
	X.EndorsementIncludeDesignatedAgents_PerEmployee, 
	X.EndorsementIncludeDesignatedAgentsEmpTheftETF, 
	X.EndorsementAddTradingCoverage_PerEmployee, 
	X.EndorsementCreditCardOrChargeCard, 
	X.EndorsementAddFaithfulPerformance_PerEmployee, 
	X.EndorsementAddBlanketExcessLimit_PerEmployee, 
	X.EndorsementAddScheduleExcessLimit_PerEmployee, 
	X.EndorsementAddScheduleExcessLimitFor35_PerEmployee, 
	X.EndorsementIncludeComputerSoftwareContractors_PerEmployee, 
	X.EndorsementRequireRecordOfChecksForGovernmentTheftMoney, 
	X.EndorsementIncreaseLimitForSpecifiedPeriodsForGovernmentTheftMoney, 
	X.EndorsementIncreaseLimitForSpecifiedPeriodsGovernment, 
	X.EndorsementDecreaseLimitForGovernmentTheftMoney, 
	X.EndorsementDecreaseLimitGovernment, 
	X.EndorsementReducedLimitForDesignatedForGovernmentTheftMoney, 
	X.EndorsementReducedLimitForDesignatedGovenment, 
	X.EndorsementRequireRecordOfChecksForGovernmentOutsidePremises, 
	X.EndorsementIncreaseLimitForSpecifiedPeriodsForGovernmentOutsidePremises, 
	X.EndorsementLimitedToRobberyForGovernmentOutsidePremises, 
	X.EndorsementIncreaseLimitForSpecifiedPeriodsGovernmentRobberyOther, 
	X.EndorsementReducedLimitForDesignatedGovenmentRobberyOther, 
	X.EndorsementRequireRecordOfChecksForGovernmentInsideRobberySecurities, 
	X.EndorsementReducedLimitForDesignatedForGovernmentInsideRobberySecurities, 
	X.EndorsementDecreaseLimitForGovernmentInsideRobberySecurities, 
	X.EndorsementIncreaseLimitForSpecifiedPeriodsForGovernmentInsideRobberySecurities, 
	X.EndorsementIncludeDesignatedAgentsPerEmployeeGETF, 
	X.EndorsementAddTradingCoveragePerEmployeeGETF, 
	X.EndorsementAddFaithfulPerformancePerEmployeeGETF, 
	X.EndorsementAddBlanketExcessLimitPerEmployeeGETF, 
	X.EndorsementAddScheduleExcessLimitPerEmployeeGETF, 
	X.EndorsementIncludeComputerSoftwareContractorsPerEmployeeGETF, 
	X.EndorsementAddScheduleExcessLimitFor35PerEmployeeGETF, 
	X.EndorsementIncludeComputerSoftwareContractorsEmpTheftETF, 
	X.EndorsementAddScheduleExcessLimitEmpTheftETF, 
	X.EndorsementEmployeeTheftExcessLimitETF, 
	X.EndorsementIncludePartnersEmpTheftETF, 
	X.EndorsementAddTradingCoverageEmpTheftETF, 
	X.EndorsementAddFaithfulPerformanceEmpTheftETF, 
	X.EndorsementRuralUtilitiesCollectionAgentsEmpTheftETF, 
	X.EndorsementAddBlanketExcessLimitEmpTheftETF, 
	X.EndorsementWarehouseReceiptsEmpTheftETF, 
	X.EndorsementCreditCardETF, 
	X.EndorsementSpecialLimitForGovernment, 
	X.EndorsementIncludePersonalAccountsETF, 
	X.EndorsementWarehouseReceipts28ETF, 
	X.EndorsementAddFaithfulPerformanceNamePositionETF, 
	X.EndorsementIncludeDesignatedAgentsPerLossGETF, 
	X.EndorsementIncludeDesignatedAgentsPerEmployee, 
	X.EndorsementAddTradingPerLossGETFCoverage, 
	X.EndorsementAddTradingCoveragePerEmployee, 
	X.EndorsementAddFaithfulPerformancePerLossGETF, 
	X.EndorsementAddFaithfulPerformancePerEmployee, 
	X.EndorsementAddBlanketExcessLimitPerLossGETF, 
	X.EndorsementAddBlanketExcessLimitPerEmployee, 
	X.EndorsementAddScheduleExcessLimitPerLossGETF, 
	X.EndorsementAddScheduleExcessLimitPerEmployee, 
	X.EndorsementIncludeComputerSoftwareContractorsPerLossGETF, 
	X.EndorsementIncludeComputerSoftwareContractorsPerEmployee, 
	X.EndorsementAddScheduleExcessLimitFor35PerLossGETF, 
	X.EndorsementAddScheduleExcessLimitFor35PerEmployee, 
	X.EndorsementEmployeeTheftExcessOverGETF, 
	X.EndorsementCreditCardGETF, 
	X.EndorsemenFaithfulPerformanceEmployeeorPositionGETF, 
	X.InsuringAgreement, 
	X.PurePremium 
	FROM
	WB_CR_Risk X
	inner join
	cte_WBCRRisk Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CR_RiskId,
	WB_CR_RiskId,
	SessionId,
	Indicator28 AS i_Indicator28,
	Indicator85 AS i_Indicator85,
	-- *INF*: DECODE(i_Indicator28, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_Indicator28,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_Indicator28,
	-- *INF*: DECODE(i_Indicator85, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_Indicator85,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_Indicator85,
	CoverageWritten,
	Indicator AS i_Indicator,
	EndorsementSafeDepositBoxTransfer AS i_EndorsementSafeDepositBoxTransfer,
	EndorsementIncludeMoneyForInsurance AS i_EndorsementIncludeMoneyForInsurance,
	EndorsementIncludeDesignatedAgents AS i_EndorsementIncludeDesignatedAgents,
	EndorsementAddFaithfulPerformance AS i_EndorsementAddFaithfulPerformance,
	EndorsementIncludeSecurities AS i_EndorsementIncludeSecurities,
	EndorsementIncludeBulky AS i_EndorsementIncludeBulky,
	EndorsemenFaithfulPerformanceEmployeeorPosition AS i_EndorsemenFaithfulPerformanceEmployeeorPosition,
	EndorsementAddBlanketExcessLimit AS i_EndorsementAddBlanketExcessLimit,
	EndorsementEmployeeTheftExcessOver AS i_EndorsementEmployeeTheftExcessOver,
	EndorsementAddScheduleExcessLimit AS i_EndorsementAddScheduleExcessLimit,
	EndorsementAddScheduleExcessLimitFor35 AS i_EndorsementAddScheduleExcessLimitFor35,
	EndorsementAddTradingCoverage AS i_EndorsementAddTradingCoverage,
	EndorsmentFaithfulDutyCoverage AS i_EndorsmentFaithfulDutyCoverage,
	EndorsementIncludeDesignatedAgents_PerEmployee AS i_EndorsementIncludeDesignatedAgents_PerEmployee,
	EndorsementIncludeDesignatedAgentsEmpTheftETF AS i_EndorsementIncludeDesignatedAgentsEmpTheftETF,
	EndorsementAddTradingCoverage_PerEmployee AS i_EndorsementAddTradingCoverage_PerEmployee,
	EndorsementCreditCardOrChargeCard AS i_EndorsementCreditCardOrChargeCard,
	EndorsementAddFaithfulPerformance_PerEmployee AS i_EndorsementAddFaithfulPerformance_PerEmployee,
	EndorsementAddBlanketExcessLimit_PerEmployee AS i_EndorsementAddBlanketExcessLimit_PerEmployee,
	EndorsementAddScheduleExcessLimit_PerEmployee AS i_EndorsementAddScheduleExcessLimit_PerEmployee,
	EndorsementAddScheduleExcessLimitFor35_PerEmployee AS i_EndorsementAddScheduleExcessLimitFor35_PerEmployee,
	EndorsementIncludeComputerSoftwareContractors_PerEmployee AS i_EndorsementIncludeComputerSoftwareContractors_PerEmployee,
	EndorsementRequireRecordOfChecksForGovernmentTheftMoney AS i_EndorsementRequireRecordOfChecksForGovernmentTheftMoney,
	EndorsementIncreaseLimitForSpecifiedPeriodsForGovernmentTheftMoney AS i_EndorsementIncreaseLimitForSpecifiedPeriodsForGovernmentTheftMoney,
	EndorsementIncreaseLimitForSpecifiedPeriodsGovernment AS i_EndorsementIncreaseLimitForSpecifiedPeriodsGovernment,
	EndorsementDecreaseLimitForGovernmentTheftMoney AS i_EndorsementDecreaseLimitForGovernmentTheftMoney,
	EndorsementDecreaseLimitGovernment AS i_EndorsementDecreaseLimitGovernment,
	EndorsementReducedLimitForDesignatedForGovernmentTheftMoney AS i_EndorsementReducedLimitForDesignatedForGovernmentTheftMoney,
	EndorsementReducedLimitForDesignatedGovenment AS i_EndorsementReducedLimitForDesignatedGovenment,
	EndorsementRequireRecordOfChecksForGovernmentOutsidePremises AS i_EndorsementRequireRecordOfChecksForGovernmentOutsidePremises,
	EndorsementIncreaseLimitForSpecifiedPeriodsForGovernmentOutsidePremises AS i_EndorsementIncreaseLimitForSpecifiedPeriodsForGovernmentOutsidePremises,
	EndorsementLimitedToRobberyForGovernmentOutsidePremises AS i_EndorsementLimitedToRobberyForGovernmentOutsidePremises,
	EndorsementIncreaseLimitForSpecifiedPeriodsGovernmentRobberyOther AS i_EndorsementIncreaseLimitForSpecifiedPeriodsGovernmentRobberyOther,
	EndorsementReducedLimitForDesignatedGovenmentRobberyOther AS i_EndorsementReducedLimitForDesignatedGovenmentRobberyOther,
	EndorsementRequireRecordOfChecksForGovernmentInsideRobberySecurities AS i_EndorsementRequireRecordOfChecksForGovernmentInsideRobberySecurities,
	EndorsementReducedLimitForDesignatedForGovernmentInsideRobberySecurities AS i_EndorsementReducedLimitForDesignatedForGovernmentInsideRobberySecurities,
	EndorsementDecreaseLimitForGovernmentInsideRobberySecurities AS i_EndorsementDecreaseLimitForGovernmentInsideRobberySecurities,
	EndorsementIncreaseLimitForSpecifiedPeriodsForGovernmentInsideRobberySecurities,
	EndorsementIncludeDesignatedAgentsPerEmployeeGETF AS i_EndorsementIncludeDesignatedAgentsPerEmployeeGETF,
	EndorsementAddTradingCoveragePerEmployeeGETF AS i_EndorsementAddTradingCoveragePerEmployeeGETF,
	EndorsementAddFaithfulPerformancePerEmployeeGETF AS i_EndorsementAddFaithfulPerformancePerEmployeeGETF,
	EndorsementAddBlanketExcessLimitPerEmployeeGETF AS i_EndorsementAddBlanketExcessLimitPerEmployeeGETF,
	EndorsementAddScheduleExcessLimitPerEmployeeGETF AS i_EndorsementAddScheduleExcessLimitPerEmployeeGETF,
	EndorsementIncludeComputerSoftwareContractorsPerEmployeeGETF AS i_EndorsementIncludeComputerSoftwareContractorsPerEmployeeGETF,
	EndorsementAddScheduleExcessLimitFor35PerEmployeeGETF AS i_EndorsementAddScheduleExcessLimitFor35PerEmployeeGETF,
	EndorsementIncludeComputerSoftwareContractorsEmpTheftETF AS i_EndorsementIncludeComputerSoftwareContractorsEmpTheftETF,
	EndorsementAddScheduleExcessLimitEmpTheftETF AS i_EndorsementAddScheduleExcessLimitEmpTheftETF,
	EndorsementEmployeeTheftExcessLimitETF AS i_EndorsementEmployeeTheftExcessLimitETF,
	EndorsementIncludePartnersEmpTheftETF AS i_EndorsementIncludePartnersEmpTheftETF,
	EndorsementAddTradingCoverageEmpTheftETF AS i_EndorsementAddTradingCoverageEmpTheftETF,
	EndorsementAddFaithfulPerformanceEmpTheftETF AS i_EndorsementAddFaithfulPerformanceEmpTheftETF,
	EndorsementRuralUtilitiesCollectionAgentsEmpTheftETF AS i_EndorsementRuralUtilitiesCollectionAgentsEmpTheftETF,
	EndorsementAddBlanketExcessLimitEmpTheftETF AS i_EndorsementAddBlanketExcessLimitEmpTheftETF,
	EndorsementWarehouseReceiptsEmpTheftETF AS i_EndorsementWarehouseReceiptsEmpTheftETF,
	EndorsementCreditCardETF AS i_EndorsementCreditCardETF,
	EndorsementSpecialLimitForGovernment AS i_EndorsementSpecialLimitForGovernment,
	EndorsementIncludePersonalAccountsETF AS i_EndorsementIncludePersonalAccountsETF,
	EndorsementWarehouseReceipts28ETF AS i_EndorsementWarehouseReceipts28ETF,
	EndorsementAddFaithfulPerformanceNamePositionETF AS i_EndorsementAddFaithfulPerformanceNamePositionETF,
	EndorsementIncludeDesignatedAgentsPerLossGETF AS i_EndorsementIncludeDesignatedAgentsPerLossGETF,
	EndorsementIncludeDesignatedAgentsPerEmployee AS i_EndorsementIncludeDesignatedAgentsPerEmployee,
	EndorsementAddTradingPerLossGETFCoverage AS i_EndorsementAddTradingPerLossGETFCoverage,
	EndorsementAddTradingCoveragePerEmployee AS i_EndorsementAddTradingCoveragePerEmployee,
	EndorsementAddFaithfulPerformancePerLossGETF AS i_EndorsementAddFaithfulPerformancePerLossGETF,
	EndorsementAddFaithfulPerformancePerEmployee AS i_EndorsementAddFaithfulPerformancePerEmployee,
	EndorsementAddBlanketExcessLimitPerLossGETF AS i_EndorsementAddBlanketExcessLimitPerLossGETF,
	EndorsementAddBlanketExcessLimitPerEmployee AS i_EndorsementAddBlanketExcessLimitPerEmployee,
	EndorsementAddScheduleExcessLimitPerLossGETF AS i_EndorsementAddScheduleExcessLimitPerLossGETF,
	EndorsementAddScheduleExcessLimitPerEmployee AS i_EndorsementAddScheduleExcessLimitPerEmployee,
	EndorsementIncludeComputerSoftwareContractorsPerLossGETF AS i_EndorsementIncludeComputerSoftwareContractorsPerLossGETF,
	EndorsementIncludeComputerSoftwareContractorsPerEmployee AS i_EndorsementIncludeComputerSoftwareContractorsPerEmployee,
	EndorsementAddScheduleExcessLimitFor35PerLossGETF AS i_EndorsementAddScheduleExcessLimitFor35PerLossGETF,
	EndorsementAddScheduleExcessLimitFor35PerEmployee AS i_EndorsementAddScheduleExcessLimitFor35PerEmployee,
	EndorsementEmployeeTheftExcessOverGETF AS i_EndorsementEmployeeTheftExcessOverGETF,
	EndorsementCreditCardGETF AS i_EndorsementCreditCardGETF,
	EndorsemenFaithfulPerformanceEmployeeorPositionGETF AS i_EndorsemenFaithfulPerformanceEmployeeorPositionGETF,
	-- *INF*: DECODE(i_Indicator, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_Indicator,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_Indicator,
	-- *INF*: DECODE(i_EndorsementSafeDepositBoxTransfer, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementSafeDepositBoxTransfer,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementSafeDepositBoxTransfer,
	-- *INF*: DECODE(i_EndorsementIncludeMoneyForInsurance, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementIncludeMoneyForInsurance,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementIncludeMoneyForInsurance,
	-- *INF*: DECODE(i_EndorsementIncludeDesignatedAgents, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementIncludeDesignatedAgents,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementIncludeDesignatedAgents,
	-- *INF*: DECODE(i_EndorsementAddFaithfulPerformance, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddFaithfulPerformance,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddFaithfulPerformance,
	-- *INF*: DECODE(i_EndorsementIncludeSecurities, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementIncludeSecurities,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementIncludeSecurities,
	-- *INF*: DECODE(i_EndorsementIncludeBulky, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementIncludeBulky,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementIncludeBulky,
	-- *INF*: DECODE(i_EndorsemenFaithfulPerformanceEmployeeorPosition, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsemenFaithfulPerformanceEmployeeorPosition,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsemenFaithfulPerformanceEmployeeorPosition,
	-- *INF*: DECODE(i_EndorsementAddBlanketExcessLimit, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddBlanketExcessLimit,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddBlanketExcessLimit,
	-- *INF*: DECODE(i_EndorsementEmployeeTheftExcessOver, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementEmployeeTheftExcessOver,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementEmployeeTheftExcessOver,
	-- *INF*: DECODE(i_EndorsementAddScheduleExcessLimit, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddScheduleExcessLimit,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddScheduleExcessLimit,
	-- *INF*: DECODE(i_EndorsementAddScheduleExcessLimitFor35, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddScheduleExcessLimitFor35,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddScheduleExcessLimitFor35,
	-- *INF*: DECODE(i_EndorsementAddTradingCoverage, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddTradingCoverage,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddTradingCoverage,
	-- *INF*: DECODE(i_EndorsmentFaithfulDutyCoverage, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsmentFaithfulDutyCoverage,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsmentFaithfulDutyCoverage,
	-- *INF*: DECODE(i_EndorsementIncludeDesignatedAgents_PerEmployee, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementIncludeDesignatedAgents_PerEmployee,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementIncludeDesignatedAgents_PerEmployee,
	-- *INF*: DECODE(i_EndorsementIncludeDesignatedAgentsEmpTheftETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementIncludeDesignatedAgentsEmpTheftETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementIncludeDesignatedAgentsEmpTheftETF,
	-- *INF*: DECODE(i_EndorsementAddTradingCoverage_PerEmployee, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddTradingCoverage_PerEmployee,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddTradingCoverage_PerEmployee,
	-- *INF*: DECODE(i_EndorsementCreditCardOrChargeCard, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementCreditCardOrChargeCard,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementCreditCardOrChargeCard,
	-- *INF*: DECODE(i_EndorsementAddFaithfulPerformance_PerEmployee, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddFaithfulPerformance_PerEmployee,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddFaithfulPerformance_PerEmployee,
	-- *INF*: DECODE(i_EndorsementAddBlanketExcessLimit_PerEmployee, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddBlanketExcessLimit_PerEmployee,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddBlanketExcessLimit_PerEmployee,
	-- *INF*: DECODE(i_EndorsementAddScheduleExcessLimit_PerEmployee, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddScheduleExcessLimit_PerEmployee,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddScheduleExcessLimit_PerEmployee,
	-- *INF*: DECODE(i_EndorsementAddScheduleExcessLimitFor35_PerEmployee, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddScheduleExcessLimitFor35_PerEmployee,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddScheduleExcessLimitFor35_PerEmployee,
	-- *INF*: DECODE(i_EndorsementIncludeComputerSoftwareContractors_PerEmployee, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementIncludeComputerSoftwareContractors_PerEmployee,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementIncludeComputerSoftwareContractors_PerEmployee,
	-- *INF*: DECODE(i_EndorsementRequireRecordOfChecksForGovernmentTheftMoney, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementRequireRecordOfChecksForGovernmentTheftMoney,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementRequireRecordOfChecksForGovernmentTheftMoney,
	-- *INF*: DECODE(i_EndorsementIncreaseLimitForSpecifiedPeriodsForGovernmentTheftMoney, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementIncreaseLimitForSpecifiedPeriodsForGovernmentTheftMoney,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementIncreaseLimitForSpecifiedPeriodsForGovernmentTheftMoney,
	-- *INF*: DECODE(i_EndorsementIncreaseLimitForSpecifiedPeriodsGovernment, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementIncreaseLimitForSpecifiedPeriodsGovernment,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementIncreaseLimitForSpecifiedPeriodsGovernment,
	-- *INF*: DECODE(i_EndorsementDecreaseLimitForGovernmentTheftMoney, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementDecreaseLimitForGovernmentTheftMoney,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementDecreaseLimitForGovernmentTheftMoney,
	-- *INF*: DECODE(i_EndorsementDecreaseLimitGovernment, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementDecreaseLimitGovernment,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementDecreaseLimitGovernment,
	-- *INF*: DECODE(i_EndorsementReducedLimitForDesignatedForGovernmentTheftMoney, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementReducedLimitForDesignatedForGovernmentTheftMoney,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementReducedLimitForDesignatedForGovernmentTheftMoney,
	-- *INF*: DECODE(i_EndorsementReducedLimitForDesignatedGovenment, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementReducedLimitForDesignatedGovenment,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementReducedLimitForDesignatedGovenment,
	-- *INF*: DECODE(i_EndorsementRequireRecordOfChecksForGovernmentOutsidePremises, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementRequireRecordOfChecksForGovernmentOutsidePremises,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementRequireRecordOfChecksForGovernmentOutsidePremises,
	-- *INF*: DECODE(i_EndorsementIncreaseLimitForSpecifiedPeriodsForGovernmentOutsidePremises, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementIncreaseLimitForSpecifiedPeriodsForGovernmentOutsidePremises,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementIncreaseLimitForSpecifiedPeriodsForGovernmentOutsidePremises,
	-- *INF*: DECODE(i_EndorsementLimitedToRobberyForGovernmentOutsidePremises, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementLimitedToRobberyForGovernmentOutsidePremises,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementLimitedToRobberyForGovernmentOutsidePremises,
	-- *INF*: DECODE(i_EndorsementIncreaseLimitForSpecifiedPeriodsGovernmentRobberyOther, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementIncreaseLimitForSpecifiedPeriodsGovernmentRobberyOther,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementIncreaseLimitForSpecifiedPeriodsGovernmentRobberyOther,
	-- *INF*: DECODE(i_EndorsementReducedLimitForDesignatedGovenmentRobberyOther, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementReducedLimitForDesignatedGovenmentRobberyOther,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementReducedLimitForDesignatedGovenmentRobberyOther,
	-- *INF*: DECODE(i_EndorsementRequireRecordOfChecksForGovernmentInsideRobberySecurities, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementRequireRecordOfChecksForGovernmentInsideRobberySecurities,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementRequireRecordOfChecksForGovernmentInsideRobberySecurities,
	-- *INF*: DECODE(i_EndorsementReducedLimitForDesignatedForGovernmentInsideRobberySecurities, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementReducedLimitForDesignatedForGovernmentInsideRobberySecurities,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementReducedLimitForDesignatedForGovernmentInsideRobberySecurities,
	-- *INF*: DECODE(i_EndorsementDecreaseLimitForGovernmentInsideRobberySecurities, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementDecreaseLimitForGovernmentInsideRobberySecurities,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementDecreaseLimitForGovernmentInsideRobberySecurities,
	-- *INF*: DECODE(EndorsementIncreaseLimitForSpecifiedPeriodsForGovernmentInsideRobberySecurities, 'T', '1', 'F', '0', NULL)
	DECODE(
	    EndorsementIncreaseLimitForSpecifiedPeriodsForGovernmentInsideRobberySecurities,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ENIncreaseLimitForSpecifiedPeriodsForGovernmentInsideRobberySecurities,
	-- *INF*: DECODE(i_EndorsementIncludeDesignatedAgentsPerEmployeeGETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementIncludeDesignatedAgentsPerEmployeeGETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementIncludeDesignatedAgentsPerEmployeeGETF,
	-- *INF*: DECODE(i_EndorsementAddTradingCoveragePerEmployeeGETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddTradingCoveragePerEmployeeGETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddTradingCoveragePerEmployeeGETF,
	-- *INF*: DECODE(i_EndorsementAddFaithfulPerformancePerEmployeeGETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddFaithfulPerformancePerEmployeeGETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddFaithfulPerformancePerEmployeeGETF,
	-- *INF*: DECODE(i_EndorsementAddBlanketExcessLimitPerEmployeeGETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddBlanketExcessLimitPerEmployeeGETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddBlanketExcessLimitPerEmployeeGETF,
	-- *INF*: DECODE(i_EndorsementAddScheduleExcessLimitPerEmployeeGETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddScheduleExcessLimitPerEmployeeGETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddScheduleExcessLimitPerEmployeeGETF,
	-- *INF*: DECODE(i_EndorsementIncludeComputerSoftwareContractorsPerEmployeeGETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementIncludeComputerSoftwareContractorsPerEmployeeGETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementIncludeComputerSoftwareContractorsPerEmployeeGETF,
	-- *INF*: DECODE(i_EndorsementAddScheduleExcessLimitFor35PerEmployeeGETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddScheduleExcessLimitFor35PerEmployeeGETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddScheduleExcessLimitFor35PerEmployeeGETF,
	-- *INF*: DECODE(i_EndorsementIncludeComputerSoftwareContractorsEmpTheftETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementIncludeComputerSoftwareContractorsEmpTheftETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementIncludeComputerSoftwareContractorsEmpTheftETF,
	-- *INF*: DECODE(i_EndorsementAddScheduleExcessLimitEmpTheftETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddScheduleExcessLimitEmpTheftETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddScheduleExcessLimitEmpTheftETF,
	-- *INF*: DECODE(i_EndorsementEmployeeTheftExcessLimitETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementEmployeeTheftExcessLimitETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementEmployeeTheftExcessLimitETF,
	-- *INF*: DECODE(i_EndorsementIncludePartnersEmpTheftETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementIncludePartnersEmpTheftETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementIncludePartnersEmpTheftETF,
	-- *INF*: DECODE(i_EndorsementAddTradingCoverageEmpTheftETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddTradingCoverageEmpTheftETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddTradingCoverageEmpTheftETF,
	-- *INF*: DECODE(i_EndorsementAddFaithfulPerformanceEmpTheftETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddFaithfulPerformanceEmpTheftETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddFaithfulPerformanceEmpTheftETF,
	-- *INF*: DECODE(i_EndorsementRuralUtilitiesCollectionAgentsEmpTheftETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementRuralUtilitiesCollectionAgentsEmpTheftETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementRuralUtilitiesCollectionAgentsEmpTheftETF,
	-- *INF*: DECODE(i_EndorsementAddBlanketExcessLimitEmpTheftETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddBlanketExcessLimitEmpTheftETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddBlanketExcessLimitEmpTheftETF,
	-- *INF*: DECODE(i_EndorsementWarehouseReceiptsEmpTheftETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementWarehouseReceiptsEmpTheftETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementWarehouseReceiptsEmpTheftETF,
	-- *INF*: DECODE(i_EndorsementCreditCardETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementCreditCardETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementCreditCardETF,
	-- *INF*: DECODE(i_EndorsementSpecialLimitForGovernment, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementSpecialLimitForGovernment,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementSpecialLimitForGovernment,
	-- *INF*: DECODE(i_EndorsementIncludePersonalAccountsETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementIncludePersonalAccountsETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementIncludePersonalAccountsETF,
	-- *INF*: DECODE(i_EndorsementWarehouseReceipts28ETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementWarehouseReceipts28ETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementWarehouseReceipts28ETF,
	-- *INF*: DECODE(i_EndorsementAddFaithfulPerformanceNamePositionETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddFaithfulPerformanceNamePositionETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddFaithfulPerformanceNamePositionETF,
	-- *INF*: DECODE(i_EndorsementIncludeDesignatedAgentsPerLossGETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementIncludeDesignatedAgentsPerLossGETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementIncludeDesignatedAgentsPerLossGETF,
	-- *INF*: DECODE(i_EndorsementIncludeDesignatedAgentsPerEmployee, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementIncludeDesignatedAgentsPerEmployee,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementIncludeDesignatedAgentsPerEmployee,
	-- *INF*: DECODE(i_EndorsementAddTradingPerLossGETFCoverage, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddTradingPerLossGETFCoverage,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddTradingPerLossGETFCoverage,
	-- *INF*: DECODE(i_EndorsementAddTradingCoveragePerEmployee, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddTradingCoveragePerEmployee,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddTradingCoveragePerEmployee,
	-- *INF*: DECODE(i_EndorsementAddFaithfulPerformancePerLossGETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddFaithfulPerformancePerLossGETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddFaithfulPerformancePerLossGETF,
	-- *INF*: DECODE(i_EndorsementAddFaithfulPerformancePerEmployee, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddFaithfulPerformancePerEmployee,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddFaithfulPerformancePerEmployee,
	-- *INF*: DECODE(i_EndorsementAddBlanketExcessLimitPerLossGETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddBlanketExcessLimitPerLossGETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddBlanketExcessLimitPerLossGETF,
	-- *INF*: DECODE(i_EndorsementAddBlanketExcessLimitPerEmployee, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddBlanketExcessLimitPerEmployee,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddBlanketExcessLimitPerEmployee,
	-- *INF*: DECODE(i_EndorsementAddScheduleExcessLimitPerLossGETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddScheduleExcessLimitPerLossGETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddScheduleExcessLimitPerLossGETF,
	-- *INF*: DECODE(i_EndorsementAddScheduleExcessLimitPerEmployee, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddScheduleExcessLimitPerEmployee,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddScheduleExcessLimitPerEmployee,
	-- *INF*: DECODE(i_EndorsementIncludeComputerSoftwareContractorsPerLossGETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementIncludeComputerSoftwareContractorsPerLossGETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementIncludeComputerSoftwareContractorsPerLossGETF,
	-- *INF*: DECODE(i_EndorsementIncludeComputerSoftwareContractorsPerEmployee, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementIncludeComputerSoftwareContractorsPerEmployee,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementIncludeComputerSoftwareContractorsPerEmployee,
	-- *INF*: DECODE(i_EndorsementAddScheduleExcessLimitFor35PerLossGETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddScheduleExcessLimitFor35PerLossGETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddScheduleExcessLimitFor35PerLossGETF,
	-- *INF*: DECODE(i_EndorsementAddScheduleExcessLimitFor35PerEmployee, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementAddScheduleExcessLimitFor35PerEmployee,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementAddScheduleExcessLimitFor35PerEmployee,
	-- *INF*: DECODE(i_EndorsementEmployeeTheftExcessOverGETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementEmployeeTheftExcessOverGETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementEmployeeTheftExcessOverGETF,
	-- *INF*: DECODE(i_EndorsementCreditCardGETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsementCreditCardGETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsementCreditCardGETF,
	-- *INF*: DECODE(i_EndorsemenFaithfulPerformanceEmployeeorPositionGETF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_EndorsemenFaithfulPerformanceEmployeeorPositionGETF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_EndorsemenFaithfulPerformanceEmployeeorPositionGETF,
	InsuringAgreement,
	PurePremium,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_CR_Risk
),
WBCRRiskStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCRRiskStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCRRiskStage
	(ExtractDate, SourceSystemId, CRRiskId, WBCRRiskId, SessionId, PurePremium, Indicator28, Indicator85, CoverageWritten, Indicator, EndorsementSafeDepositBoxTransfer, EndorsementIncludeMoneyForInsurance, EndorsementIncludeDesignatedAgents, EndorsementAddFaithfulPerformance, EndorsementIncludeSecurities, EndorsementIncludeBulky, EndorsemenFaithfulPerformanceEmployeeorPosition, EndorsementAddBlanketExcessLimit, EndorsementEmployeeTheftExcessOver, EndorsementAddScheduleExcessLimit, EndorsementAddScheduleExcessLimitFor35, EndorsementAddTradingCoverage, EndorsmentFaithfulDutyCoverage, EndorsementIncludeDesignatedAgents_PerEmployee, EndorsementAddTradingCoverage_PerEmployee, EndorsementCreditCardOrChargeCard, EndorsementAddFaithfulPerformance_PerEmployee, EndorsementAddBlanketExcessLimit_PerEmployee, EndorsementAddScheduleExcessLimit_PerEmployee, EndorsementAddScheduleExcessLimitFor35_PerEmployee, EndorsementIncludeComputerSoftwareContractors_PerEmployee, EndorsementRequireRecordOfChecksForGovernmentTheftMoney, EndorsementIncreaseLimitForSpecifiedPeriodsForGovernmentTheftMoney, EndorsementIncreaseLimitForSpecifiedPeriodsGovernment, EndorsementDecreaseLimitForGovernmentTheftMoney, EndorsementDecreaseLimitGovernment, EndorsementReducedLimitForDesignatedForGovernmentTheftMoney, EndorsementReducedLimitForDesignatedGovenment, EndorsementRequireRecordOfChecksForGovernmentOutsidePremises, EndorsementIncreaseLimitForSpecifiedPeriodsForGovernmentOutsidePremises, EndorsementLimitedToRobberyForGovernmentOutsidePremises, EndorsementIncreaseLimitForSpecifiedPeriodsGovernmentRobberyOther, EndorsementReducedLimitForDesignatedGovenmentRobberyOther, EndorsementRequireRecordOfChecksForGovernmentInsideRobberySecurities, EndorsementReducedLimitForDesignatedForGovernmentInsideRobberySecurities, EndorsementDecreaseLimitForGovernmentInsideRobberySecurities, EndorsementIncreaseLimitForSpecifiedPeriodsForGovernmentInsideRobberySecurities, EndorsementIncludeDesignatedAgentsPerEmployeeGETF, EndorsementAddTradingCoveragePerEmployeeGETF, EndorsementAddFaithfulPerformancePerEmployeeGETF, EndorsementAddBlanketExcessLimitPerEmployeeGETF, EndorsementAddScheduleExcessLimitPerEmployeeGETF, EndorsementIncludeComputerSoftwareContractorsPerEmployeeGETF, EndorsementAddScheduleExcessLimitFor35PerEmployeeGETF, EndorsementIncludeDesignatedAgentsEmpTheftETF, EndorsementIncludeComputerSoftwareContractorsEmpTheftETF, EndorsementAddScheduleExcessLimitEmpTheftETF, EndorsementEmployeeTheftExcessLimitETF, EndorsementIncludePartnersEmpTheftETF, EndorsementAddTradingCoverageEmpTheftETF, EndorsementAddFaithfulPerformanceEmpTheftETF, EndorsementRuralUtilitiesCollectionAgentsEmpTheftETF, EndorsementAddBlanketExcessLimitEmpTheftETF, EndorsementWarehouseReceiptsEmpTheftETF, EndorsementCreditCardETF, EndorsementSpecialLimitForGovernment, EndorsementIncludePersonalAccountsETF, EndorsementWarehouseReceipts28ETF, EndorsementAddFaithfulPerformanceNamePositionETF, EndorsementIncludeDesignatedAgentsPerLossGETF, EndorsementIncludeDesignatedAgentsPerEmployee, EndorsementAddTradingPerLossGETFCoverage, EndorsementAddTradingCoveragePerEmployee, EndorsementAddFaithfulPerformancePerLossGETF, EndorsementAddFaithfulPerformancePerEmployee, EndorsementAddBlanketExcessLimitPerLossGETF, EndorsementAddBlanketExcessLimitPerEmployee, EndorsementAddScheduleExcessLimitPerLossGETF, EndorsementAddScheduleExcessLimitPerEmployee, EndorsementIncludeComputerSoftwareContractorsPerLossGETF, EndorsementIncludeComputerSoftwareContractorsPerEmployee, EndorsementAddScheduleExcessLimitFor35PerLossGETF, EndorsementAddScheduleExcessLimitFor35PerEmployee, EndorsementEmployeeTheftExcessOverGETF, EndorsementCreditCardGETF, EndorsemenFaithfulPerformanceEmployeeorPositionGETF, InsuringAgreement)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CR_RiskId AS CRRISKID, 
	WB_CR_RiskId AS WBCRRISKID, 
	SESSIONID, 
	PUREPREMIUM, 
	o_Indicator28 AS INDICATOR28, 
	o_Indicator85 AS INDICATOR85, 
	COVERAGEWRITTEN, 
	o_Indicator AS INDICATOR, 
	o_EndorsementSafeDepositBoxTransfer AS ENDORSEMENTSAFEDEPOSITBOXTRANSFER, 
	o_EndorsementIncludeMoneyForInsurance AS ENDORSEMENTINCLUDEMONEYFORINSURANCE, 
	o_EndorsementIncludeDesignatedAgents AS ENDORSEMENTINCLUDEDESIGNATEDAGENTS, 
	o_EndorsementAddFaithfulPerformance AS ENDORSEMENTADDFAITHFULPERFORMANCE, 
	o_EndorsementIncludeSecurities AS ENDORSEMENTINCLUDESECURITIES, 
	o_EndorsementIncludeBulky AS ENDORSEMENTINCLUDEBULKY, 
	o_EndorsemenFaithfulPerformanceEmployeeorPosition AS ENDORSEMENFAITHFULPERFORMANCEEMPLOYEEORPOSITION, 
	o_EndorsementAddBlanketExcessLimit AS ENDORSEMENTADDBLANKETEXCESSLIMIT, 
	o_EndorsementEmployeeTheftExcessOver AS ENDORSEMENTEMPLOYEETHEFTEXCESSOVER, 
	o_EndorsementAddScheduleExcessLimit AS ENDORSEMENTADDSCHEDULEEXCESSLIMIT, 
	o_EndorsementAddScheduleExcessLimitFor35 AS ENDORSEMENTADDSCHEDULEEXCESSLIMITFOR35, 
	o_EndorsementAddTradingCoverage AS ENDORSEMENTADDTRADINGCOVERAGE, 
	o_EndorsmentFaithfulDutyCoverage AS ENDORSMENTFAITHFULDUTYCOVERAGE, 
	o_EndorsementIncludeDesignatedAgents_PerEmployee AS ENDORSEMENTINCLUDEDESIGNATEDAGENTS_PEREMPLOYEE, 
	o_EndorsementAddTradingCoverage_PerEmployee AS ENDORSEMENTADDTRADINGCOVERAGE_PEREMPLOYEE, 
	o_EndorsementCreditCardOrChargeCard AS ENDORSEMENTCREDITCARDORCHARGECARD, 
	o_EndorsementAddFaithfulPerformance_PerEmployee AS ENDORSEMENTADDFAITHFULPERFORMANCE_PEREMPLOYEE, 
	o_EndorsementAddBlanketExcessLimit_PerEmployee AS ENDORSEMENTADDBLANKETEXCESSLIMIT_PEREMPLOYEE, 
	o_EndorsementAddScheduleExcessLimit_PerEmployee AS ENDORSEMENTADDSCHEDULEEXCESSLIMIT_PEREMPLOYEE, 
	o_EndorsementAddScheduleExcessLimitFor35_PerEmployee AS ENDORSEMENTADDSCHEDULEEXCESSLIMITFOR35_PEREMPLOYEE, 
	o_EndorsementIncludeComputerSoftwareContractors_PerEmployee AS ENDORSEMENTINCLUDECOMPUTERSOFTWARECONTRACTORS_PEREMPLOYEE, 
	o_EndorsementRequireRecordOfChecksForGovernmentTheftMoney AS ENDORSEMENTREQUIRERECORDOFCHECKSFORGOVERNMENTTHEFTMONEY, 
	o_EndorsementIncreaseLimitForSpecifiedPeriodsForGovernmentTheftMoney AS ENDORSEMENTINCREASELIMITFORSPECIFIEDPERIODSFORGOVERNMENTTHEFTMONEY, 
	o_EndorsementIncreaseLimitForSpecifiedPeriodsGovernment AS ENDORSEMENTINCREASELIMITFORSPECIFIEDPERIODSGOVERNMENT, 
	o_EndorsementDecreaseLimitForGovernmentTheftMoney AS ENDORSEMENTDECREASELIMITFORGOVERNMENTTHEFTMONEY, 
	o_EndorsementDecreaseLimitGovernment AS ENDORSEMENTDECREASELIMITGOVERNMENT, 
	o_EndorsementReducedLimitForDesignatedForGovernmentTheftMoney AS ENDORSEMENTREDUCEDLIMITFORDESIGNATEDFORGOVERNMENTTHEFTMONEY, 
	o_EndorsementReducedLimitForDesignatedGovenment AS ENDORSEMENTREDUCEDLIMITFORDESIGNATEDGOVENMENT, 
	o_EndorsementRequireRecordOfChecksForGovernmentOutsidePremises AS ENDORSEMENTREQUIRERECORDOFCHECKSFORGOVERNMENTOUTSIDEPREMISES, 
	o_EndorsementIncreaseLimitForSpecifiedPeriodsForGovernmentOutsidePremises AS ENDORSEMENTINCREASELIMITFORSPECIFIEDPERIODSFORGOVERNMENTOUTSIDEPREMISES, 
	o_EndorsementLimitedToRobberyForGovernmentOutsidePremises AS ENDORSEMENTLIMITEDTOROBBERYFORGOVERNMENTOUTSIDEPREMISES, 
	o_EndorsementIncreaseLimitForSpecifiedPeriodsGovernmentRobberyOther AS ENDORSEMENTINCREASELIMITFORSPECIFIEDPERIODSGOVERNMENTROBBERYOTHER, 
	o_EndorsementReducedLimitForDesignatedGovenmentRobberyOther AS ENDORSEMENTREDUCEDLIMITFORDESIGNATEDGOVENMENTROBBERYOTHER, 
	o_EndorsementRequireRecordOfChecksForGovernmentInsideRobberySecurities AS ENDORSEMENTREQUIRERECORDOFCHECKSFORGOVERNMENTINSIDEROBBERYSECURITIES, 
	o_EndorsementReducedLimitForDesignatedForGovernmentInsideRobberySecurities AS ENDORSEMENTREDUCEDLIMITFORDESIGNATEDFORGOVERNMENTINSIDEROBBERYSECURITIES, 
	o_EndorsementDecreaseLimitForGovernmentInsideRobberySecurities AS ENDORSEMENTDECREASELIMITFORGOVERNMENTINSIDEROBBERYSECURITIES, 
	o_ENIncreaseLimitForSpecifiedPeriodsForGovernmentInsideRobberySecurities AS ENDORSEMENTINCREASELIMITFORSPECIFIEDPERIODSFORGOVERNMENTINSIDEROBBERYSECURITIES, 
	o_EndorsementIncludeDesignatedAgentsPerEmployeeGETF AS ENDORSEMENTINCLUDEDESIGNATEDAGENTSPEREMPLOYEEGETF, 
	o_EndorsementAddTradingCoveragePerEmployeeGETF AS ENDORSEMENTADDTRADINGCOVERAGEPEREMPLOYEEGETF, 
	o_EndorsementAddFaithfulPerformancePerEmployeeGETF AS ENDORSEMENTADDFAITHFULPERFORMANCEPEREMPLOYEEGETF, 
	o_EndorsementAddBlanketExcessLimitPerEmployeeGETF AS ENDORSEMENTADDBLANKETEXCESSLIMITPEREMPLOYEEGETF, 
	o_EndorsementAddScheduleExcessLimitPerEmployeeGETF AS ENDORSEMENTADDSCHEDULEEXCESSLIMITPEREMPLOYEEGETF, 
	o_EndorsementIncludeComputerSoftwareContractorsPerEmployeeGETF AS ENDORSEMENTINCLUDECOMPUTERSOFTWARECONTRACTORSPEREMPLOYEEGETF, 
	o_EndorsementAddScheduleExcessLimitFor35PerEmployeeGETF AS ENDORSEMENTADDSCHEDULEEXCESSLIMITFOR35PEREMPLOYEEGETF, 
	o_EndorsementIncludeDesignatedAgentsEmpTheftETF AS ENDORSEMENTINCLUDEDESIGNATEDAGENTSEMPTHEFTETF, 
	o_EndorsementIncludeComputerSoftwareContractorsEmpTheftETF AS ENDORSEMENTINCLUDECOMPUTERSOFTWARECONTRACTORSEMPTHEFTETF, 
	o_EndorsementAddScheduleExcessLimitEmpTheftETF AS ENDORSEMENTADDSCHEDULEEXCESSLIMITEMPTHEFTETF, 
	o_EndorsementEmployeeTheftExcessLimitETF AS ENDORSEMENTEMPLOYEETHEFTEXCESSLIMITETF, 
	o_EndorsementIncludePartnersEmpTheftETF AS ENDORSEMENTINCLUDEPARTNERSEMPTHEFTETF, 
	o_EndorsementAddTradingCoverageEmpTheftETF AS ENDORSEMENTADDTRADINGCOVERAGEEMPTHEFTETF, 
	o_EndorsementAddFaithfulPerformanceEmpTheftETF AS ENDORSEMENTADDFAITHFULPERFORMANCEEMPTHEFTETF, 
	o_EndorsementRuralUtilitiesCollectionAgentsEmpTheftETF AS ENDORSEMENTRURALUTILITIESCOLLECTIONAGENTSEMPTHEFTETF, 
	o_EndorsementAddBlanketExcessLimitEmpTheftETF AS ENDORSEMENTADDBLANKETEXCESSLIMITEMPTHEFTETF, 
	o_EndorsementWarehouseReceiptsEmpTheftETF AS ENDORSEMENTWAREHOUSERECEIPTSEMPTHEFTETF, 
	o_EndorsementCreditCardETF AS ENDORSEMENTCREDITCARDETF, 
	o_EndorsementSpecialLimitForGovernment AS ENDORSEMENTSPECIALLIMITFORGOVERNMENT, 
	o_EndorsementIncludePersonalAccountsETF AS ENDORSEMENTINCLUDEPERSONALACCOUNTSETF, 
	o_EndorsementWarehouseReceipts28ETF AS ENDORSEMENTWAREHOUSERECEIPTS28ETF, 
	o_EndorsementAddFaithfulPerformanceNamePositionETF AS ENDORSEMENTADDFAITHFULPERFORMANCENAMEPOSITIONETF, 
	o_EndorsementIncludeDesignatedAgentsPerLossGETF AS ENDORSEMENTINCLUDEDESIGNATEDAGENTSPERLOSSGETF, 
	o_EndorsementIncludeDesignatedAgentsPerEmployee AS ENDORSEMENTINCLUDEDESIGNATEDAGENTSPEREMPLOYEE, 
	o_EndorsementAddTradingPerLossGETFCoverage AS ENDORSEMENTADDTRADINGPERLOSSGETFCOVERAGE, 
	o_EndorsementAddTradingCoveragePerEmployee AS ENDORSEMENTADDTRADINGCOVERAGEPEREMPLOYEE, 
	o_EndorsementAddFaithfulPerformancePerLossGETF AS ENDORSEMENTADDFAITHFULPERFORMANCEPERLOSSGETF, 
	o_EndorsementAddFaithfulPerformancePerEmployee AS ENDORSEMENTADDFAITHFULPERFORMANCEPEREMPLOYEE, 
	o_EndorsementAddBlanketExcessLimitPerLossGETF AS ENDORSEMENTADDBLANKETEXCESSLIMITPERLOSSGETF, 
	o_EndorsementAddBlanketExcessLimitPerEmployee AS ENDORSEMENTADDBLANKETEXCESSLIMITPEREMPLOYEE, 
	o_EndorsementAddScheduleExcessLimitPerLossGETF AS ENDORSEMENTADDSCHEDULEEXCESSLIMITPERLOSSGETF, 
	o_EndorsementAddScheduleExcessLimitPerEmployee AS ENDORSEMENTADDSCHEDULEEXCESSLIMITPEREMPLOYEE, 
	o_EndorsementIncludeComputerSoftwareContractorsPerLossGETF AS ENDORSEMENTINCLUDECOMPUTERSOFTWARECONTRACTORSPERLOSSGETF, 
	o_EndorsementIncludeComputerSoftwareContractorsPerEmployee AS ENDORSEMENTINCLUDECOMPUTERSOFTWARECONTRACTORSPEREMPLOYEE, 
	o_EndorsementAddScheduleExcessLimitFor35PerLossGETF AS ENDORSEMENTADDSCHEDULEEXCESSLIMITFOR35PERLOSSGETF, 
	o_EndorsementAddScheduleExcessLimitFor35PerEmployee AS ENDORSEMENTADDSCHEDULEEXCESSLIMITFOR35PEREMPLOYEE, 
	o_EndorsementEmployeeTheftExcessOverGETF AS ENDORSEMENTEMPLOYEETHEFTEXCESSOVERGETF, 
	o_EndorsementCreditCardGETF AS ENDORSEMENTCREDITCARDGETF, 
	o_EndorsemenFaithfulPerformanceEmployeeorPositionGETF AS ENDORSEMENFAITHFULPERFORMANCEEMPLOYEEORPOSITIONGETF, 
	INSURINGAGREEMENT
	FROM EXP_Metadata
),
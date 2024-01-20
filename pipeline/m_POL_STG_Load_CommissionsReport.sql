WITH
SQ_CommissionsReport AS (
	select
	wbp.Division
	,dcp.PolicyNumber
	,wbpartyCust.CustomerNum
	,wbag.Reference AS Agency_Number
	,dcp.PrimaryRatingState
	,dcp.Status
	,dcp.EffectiveDate
	,wbl.CommissionCustomerCareAmount
	,wbl.IsContribution as Contribution
	,wbl.Contribution as ContributionAmount
	,wbl.CommissionProgramAmount as ProgramAdjustment
	,dcl.Type as LOB_Type 
	,wbl.IsOverride as Override
	,wbl.IsGraduated as Graduated
	,wbl.FinalCommission
	,wbl.CommissionAmount
	,wbl.FinalCommissionGraduated
	,wbl.CommissionAmountGraduated
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy dcp with(nolock)
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy wbp with(nolock) 
	on wbp.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Party wbpartyCust with(nolock) 
	on wbpartyCust.SessionId = wbp.SessionId 
	and wbpartyCust.CustomerNum is not null
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Agency wbag with(nolock) 
	on wbag.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Line wbl with(nolock) 
	on wbl.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line dcl with(nolock) 
	on dcl.LineId = wbl.lineid
	where
	dcp.Status in ('Inforce', 'Cancel-Pending', 'CancelPending')
	and DATEPART(quarter,dcp.EffectiveDate) = @{pipeline().parameters.PREV_QUARTER}
	and YEAR(dcp.EffectiveDate) = @{pipeline().parameters.PREV_QUARTER_YEAR}
	and dcp.sessionID in 
	(
	    select max(sessionID)
	    from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy dcp with(nolock)
	    group by PolicyNumber, dcp.EffectiveDate
	)
	and
	(
		(wbl.IsGraduated = 0 and wbl.FinalCommission != wbl.CommissionAmount)
		or (wbl.IsGraduated = 1 and wbl.FinalCommission = wbl.CommissionAmount and wbl.CommissionAmountGraduated != wbl.FinalCommissionGraduated)
		or (wbl.IsGraduated = 1 and wbl.FinalCommission != wbl.CommissionAmount and wbl.CommissionAmountGraduated = wbl.FinalCommissionGraduated)
		or (wbl.IsGraduated = 1 and wbl.FinalCommission != wbl.CommissionAmount and wbl.CommissionAmountGraduated != wbl.FinalCommissionGraduated)
		or (wbl.IsContribution = 1 and wbl.FinalCommission = wbl.CommissionAmount and Contribution > 0)
	)
	@{pipeline().parameters.WHERE_CLAUSE}
),
SRTTRANS AS (
	SELECT
	Division, 
	PolicyNumber, 
	CustomerNum, 
	Agency_Number, 
	PrimaryRatingState, 
	Status, 
	EffectiveDate, 
	CommissionCustomerCareAmount, 
	Contribution, 
	ContributionAmount, 
	ProgramAdjustment, 
	LOB_Type, 
	Override, 
	Graduated, 
	FinalCommission, 
	CommissionAmount, 
	FinalCommissionGraduated, 
	CommissionAmountGraduated
	FROM SQ_CommissionsReport
	ORDER BY Division ASC, PolicyNumber ASC, EffectiveDate ASC
),
EXP_FileName_TransFlag AS (
	SELECT
	PolicyNumber AS i_PolicyNumber,
	-- *INF*: CHR(39) || i_PolicyNumber || CHR(39)
	CHR(39) || i_PolicyNumber || CHR(39) AS o_PolicyNumber,
	Division,
	CustomerNum AS i_CustomerNum,
	-- *INF*: CHR(39) || i_CustomerNum || CHR(39)
	CHR(39) || i_CustomerNum || CHR(39) AS o_CustomerNum,
	Agency_Number,
	PrimaryRatingState,
	Status,
	EffectiveDate,
	CommissionCustomerCareAmount,
	Contribution AS i_Contribution,
	-- *INF*: DECODE(i_Contribution, 'T', 'Y', 'F', 'N')
	DECODE(
	    i_Contribution,
	    'T', 'Y',
	    'F', 'N'
	) AS v_Contribution,
	v_Contribution AS o_Contribution,
	ContributionAmount,
	ProgramAdjustment,
	LOB_Type,
	Override AS i_Override,
	-- *INF*: DECODE(i_Override, 'T', 'Y', 'F', 'N')
	DECODE(
	    i_Override,
	    'T', 'Y',
	    'F', 'N'
	) AS v_Override,
	v_Override AS o_Override,
	Graduated AS i_Graduated,
	-- *INF*: DECODE(i_Graduated, 'T', 'Y', 'F', 'N')
	DECODE(
	    i_Graduated,
	    'T', 'Y',
	    'F', 'N'
	) AS v_Graduated,
	v_Graduated AS o_Graduated,
	FinalCommission,
	CommissionAmount,
	FinalCommissionGraduated,
	CommissionAmountGraduated,
	-- *INF*: IIF(ISNULL(v_LastDivision) OR v_LastDivision  !=  Division, 'Y', 'N')
	IFF(v_LastDivision IS NULL OR v_LastDivision != Division, 'Y', 'N') AS v_NewTransFlag,
	Division AS v_LastDivision,
	-- *INF*: Division || '_' || TO_CHAR(EffectiveDate, 'YYYY') || '_Q' || TO_CHAR(EffectiveDate, 'Q') || '.csv'
	Division || '_' || TO_CHAR(EffectiveDate, 'YYYY') || '_Q' || TO_CHAR(EffectiveDate, 'Q') || '.csv' AS v_FileName,
	v_FileName AS o_FileName,
	v_NewTransFlag AS o_NewTransFlag
	FROM SRTTRANS
),
TC_Trans_PerDivision AS (
),
CommissionsReport_FF AS (
	INSERT INTO CommissionsReport_FF
	(PolicyNumber, Division, CustomerNum, Agency_Number, PrimaryRatingState, Status, EffectiveDate, CommissionCustomerCareAmount, Contribution, ContributionAmount, ProgramAdjustment, LOB_Type, Override, Graduated, FinalCommission, CommissionAmount, FinalCommissionGraduated, CommissionAmountGraduated, FileName)
	SELECT 
	POLICYNUMBER, 
	DIVISION, 
	CUSTOMERNUM, 
	AGENCY_NUMBER, 
	PRIMARYRATINGSTATE, 
	STATUS, 
	EFFECTIVEDATE, 
	COMMISSIONCUSTOMERCAREAMOUNT, 
	CONTRIBUTION, 
	CONTRIBUTIONAMOUNT, 
	PROGRAMADJUSTMENT, 
	LOB_TYPE, 
	OVERRIDE, 
	GRADUATED, 
	FINALCOMMISSION, 
	COMMISSIONAMOUNT, 
	FINALCOMMISSIONGRADUATED, 
	COMMISSIONAMOUNTGRADUATED, 
	FILENAME
	FROM TC_Trans_PerDivision
),
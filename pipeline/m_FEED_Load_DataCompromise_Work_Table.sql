WITH
LKP_DefenseAndLiability AS (
	SELECT
	CoverageDeductibleValue,
	PremiumTransactionAKID
	FROM (
		select b.PremiumTransactionAKID as PremiumTransactionAKID,
		 a.CoverageDeductibleValue  as CoverageDeductibleValue  
		from CoverageDeductible a join CoverageDeductibleBridge b on a.CoverageDeductibleId=b.CoverageDeductibleId and a.CoverageDeductibleType='DataCompromiseDefenseAndLiability' and a.CoverageDeductibleValue<>'0'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKID ORDER BY CoverageDeductibleValue DESC) = 1
),
LKP_ProgramCodeForNSI AS (
	SELECT
	ProgramDescription,
	ProgramAKId
	FROM (
		SELECT 
			ProgramDescription,
			ProgramAKId
		FROM Program
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProgramAKId ORDER BY ProgramDescription) = 1
),
SQ_DataCompromise AS (
	Declare @STARTDATE as datetime,
	        @ENDDATE as datetime
	set @STARTDATE=@{pipeline().parameters.STARTDATE}
	set @ENDDATE=@{pipeline().parameters.ENDDATE}
	SELECT 
	PremiumMasterCalculation.SourceSystemID, policy.pol_id, RiskLocation.RiskLocationID, PolicyCoverage.PolicyCoverageID, RatingCoverage.RatingCoverageId, 
	PremiumTransaction.PremiumTransactionID, PremiumMasterCalculation.PremiumMasterCalculationID, contract_customer.contract_cust_id, contract_customer_address.contract_cust_addr_id, Agency.AgencyID, policy.pol_sym, policy.pol_num, policy.pol_mod, policy.PolicyOfferingAKId, policy.StrategicProfitCenterAKId, policy.pol_eff_date, policy.pol_exp_date, contract_customer.name, contract_customer_address.addr_line_1, contract_customer_address.addr_line_2, contract_customer_address.addr_line_3, contract_customer_address.city_name, contract_customer_address.state_prov_code, contract_customer_address.zip_postal_code, 
	---------------------------------------------------------------------------------------------------------------
	ResponseExpenses.CoverageLimitValue as FirstPartyLimit,
	ResponseExpensesDeductible.CoverageDeductibleValue as DeductibleAmount,
	DefenseandLiability .CoverageLimitValue as ThirdPartyIndicator,
	PremiumTransaction.PremiumTransactionCode,
	----------------------------------------------------------------------------------------------------------------
	policy.prim_bus_class_code,RatingCoverage.ClassCode,
	 policy.prior_pol_key, Agency.AgencyCode, PremiumMasterCalculation.PremiumMasterTransactionCode, PremiumMasterCalculation.PremiumMasterCoverageEffectiveDate, 
	---RatingCoverage.RatingCoverageEffectiveDate,
	 --RatingCoverage.RatingCoverageExpirationDate, 
	PremiumMasterCalculation.PremiumMasterCoverageExpirationDate, 
	PremiumMasterCalculation.PremiumMasterPremium ,
	PremiumTransaction.PremiumTransactionAKID,
	PremiumMasterCalculation.premiummasterfulltermpremium,
	PremiumMasterCalculation.PremiumMasterRunDate,
	PolicyCoverage.PolicyAKID,
	PolicyCoverage.PolicyCoverageEffectiveDate,
	RatingCoverage.RatingCoverageCancellationDate,
	policy.ProgramAKId
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy 
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage on PolicyCoverage.PolicyAKID=policy.pol_ak_id and PolicyCoverage.CurrentSnapshotFlag=1 and policy.crrnt_snpsht_flag=1
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer on policy.contract_cust_ak_id=contract_customer.contract_cust_ak_id and contract_customer.crrnt_snpsht_flag=1
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_address on contract_customer_address.contract_cust_ak_id=contract_customer.contract_cust_ak_id and contract_customer_address.crrnt_snpsht_flag=1
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Agency on policy.AgencyAKId=Agency.AgencyAKID and Agency.CurrentSnapshotFlag=1
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation on PolicyCoverage.RiskLocationAKID= RiskLocation.RiskLocationAKID and RiskLocation.CurrentSnapshotFlag=1
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage on RatingCoverage.PolicyCoverageAKID=PolicyCoverage.PolicyCoverageAKID  and PolicyCoverage.CurrentSnapshotFlag=1
	left  join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction on PremiumTransaction.RatingCoverageAKId=RatingCoverage.RatingCoverageAKID and PremiumTransaction.EffectiveDate=RatingCoverage.EffectiveDate and PremiumTransaction.CurrentSnapshotFlag=1
	left  join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation on  PremiumMasterCalculation.PremiumTransactionAKID=PremiumTransaction.PremiumTransactionAKID and  PremiumMasterCalculation.CurrentSnapshotFlag =1
	--------------------------------
	left join (select CoverageLimitBridge.PremiumTransactionAKID , CoverageLimit.CoverageLimitType,  CoverageLimit.CoverageLimitValue
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit on CoverageLimit.CoverageLimitId=CoverageLimitBridge.CoverageLimitId  ) as  ResponseExpenses
	 on ResponseExpenses.PremiumTransactionAKID= PremiumTransaction.PremiumTransactionAKID 
	and ResponseExpenses.CoverageLimitType='ResponseExpense' and ResponseExpenses.CoverageLimitValue<>0
	--------------------------------
	left join (select CoveragedeductibleBridge.PremiumTransactionAKID , CoverageDeductible.CoverageDeductibleType,  CoverageDeductible.CoverageDeductibleValue
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoveragedeductibleBridge inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductible on CoverageDeductible.CoverageDeductibleId=CoverageDeductibleBridge.CoverageDeductibleId  ) as  ResponseExpensesDeductible
	 on ResponseExpensesDeductible.PremiumTransactionAKID= PremiumTransaction.PremiumTransactionAKID 
	and ResponseExpensesDeductible.CoverageDeductibleType='DataCompromiseResponseExpense'
	 and ResponseExpensesDeductible.CoverageDeductibleValue<>0
	--------------------------------
	left join (select CoverageLimitBridge.PremiumTransactionAKID , CoverageLimit.CoverageLimitType,  CoverageLimit.CoverageLimitValue
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit on CoverageLimit.CoverageLimitId=CoverageLimitBridge.CoverageLimitId  ) as  DefenseandLiability
	 on DefenseandLiability.PremiumTransactionAKID= PremiumTransaction.PremiumTransactionAKID 
	and DefenseandLiability.CoverageLimitType='DefenseandLiability' and DefenseandLiability.CoverageLimitValue<>0
	
	WHERE
	
	PremiumMasterCalculation.PremiumMasterPremiumType='D'
	and 
	PremiumMasterCalculation.PremiumMasterReasonAmendedCode not in ('COL','CWO')
	and
	((PremiumMasterCalculation.PremiumMasterRunDate between @STARTDATE and @ENDDATE)
	 OR
	(PremiumMasterCoverageEffectiveDate < @ENDDATE AND PremiumMasterCoverageExpirationDate >@ENDDATE))
	
	and
	
	-------------------------------------------------------------------
	--PremiumMasterCalculation.PremiumMasterRunDate  between ----PremiumTransaction.PremiumTransactionEffectiveDate and --PremiumTransaction.PremiumTransactionExpirationDate
	------------------------------------------------------------------
	--start fix for Defect 3350
	--exists (
	--select 1 from PremiumTransaction PT
	--join RatingCoverage a
	--on PT.RatingCoverageAKId=a.RatingCoverageAKId
	--and PT.EffectiveDate=a.EffectiveDate
	--where a.RatingCoverageAKID=RatingCoverage.RatingCoverageAKID
	--and PremiumMasterCalculation.PremiumMasterRunDate>=PremiumTransactionEffectiveDate
	--and PremiumMasterCalculation.PremiumMasterRunDate<PremiumTransactionExpirationDate
	--and a.CoverageType='DataCompromise')
	--end of fix 3350
	--and 
	PremiumMasterCalculation.PremiumMasterRunDate  between policy.pol_eff_date and policy.pol_exp_date
	and PolicyCoverage.SourceSystemID='DCT' and policy.source_sys_id='DCT'
	and contract_customer.source_sys_id='DCT'
	and contract_customer_address.source_sys_id='DCT'
	and RiskLocation.SourceSystemID='DCT'
	and RatingCoverage.SourceSystemID='DCT'
	and 
	------------------------------------------------
	RatingCoverage.CoverageType='DataCompromise'
	@{pipeline().parameters.WHERE}
),
LKP_PolicyCurrentStatusDim AS (
	SELECT
	PolicyCancellationDate,
	EDWPolicyAKId
	FROM (
		SELECT 
			PolicyCancellationDate,
			EDWPolicyAKId
		FROM @{pipeline().parameters.DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCurrentStatusDim
		WHERE RunDate between @{pipeline().parameters.STARTDATE} and @{pipeline().parameters.ENDDATE}
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWPolicyAKId ORDER BY PolicyCancellationDate) = 1
),
LKP_PolicyOffering AS (
	SELECT
	CurrentSnapshotFlag,
	PolicyOfferingAKId,
	PolicyOfferingCode,
	in_PolicyOfferingAKId
	FROM (
		SELECT 
			CurrentSnapshotFlag,
			PolicyOfferingAKId,
			PolicyOfferingCode,
			in_PolicyOfferingAKId
		FROM PolicyOffering
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyOfferingAKId ORDER BY CurrentSnapshotFlag) = 1
),
LKP_PremiumTransaction AS (
	SELECT
	PremiumTransactionEffectiveDate,
	PolicyAKID,
	in_PolicyAKID
	FROM (
		Declare @defaultcanceldate as datetime
		        
		set @defaultcanceldate = '2100-12-31'
		
		select  pt.PremiumTransactionEffectiveDate as PremiumTransactionEffectiveDate,pc.PolicyAKID as PolicyAKID
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction pt join 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage rc 
		on rc.RatingCoverageAKID = pt.RatingCoverageAKId and rc.EffectiveDate = pt.EffectiveDate
		and rc.RatingCoverageCancellationDate > @defaultcanceldate
		and rc.CoverageType = 'DataCompromise'
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.policycoverage pc
		on pc.PolicyCoverageAKID = rc.PolicyCoverageAKID and pc.CurrentSnapshotFlag = 1
		where not exists 
		(select * from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction pt1 
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage rc1 
		on rc1.RatingCoverageAKID = pt1.RatingCoverageAKId and rc1.EffectiveDate = pt1.EffectiveDate
		and rc1.RatingCoverageCancellationDate > @defaultcanceldate
		and rc1.CoverageType = 'DataCompromise'
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.policycoverage pc1
		on pc1.PolicyCoverageAKID = rc1.PolicyCoverageAKID and pc1.CurrentSnapshotFlag = 1
		where pc1.PolicyAKID = pc.PolicyAKID
		and pt1.PremiumTransactionEnteredDate < pt.PremiumTransactionEnteredDate )
		and not exists 
		(select * from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction pt1
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage rc1 
		on rc1.RatingCoverageAKID = pt1.RatingCoverageAKId and rc1.EffectiveDate = pt1.EffectiveDate
		and rc1.RatingCoverageCancellationDate < @defaultcanceldate
		and rc1.CoverageType = 'DataCompromise'
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.policycoverage pc1
		on pc1.PolicyCoverageAKID = rc1.PolicyCoverageAKID and pc1.CurrentSnapshotFlag = 1
		where pc1.PolicyAKID = pc.PolicyAKID
		and pt1.PremiumTransactionEnteredDate > pt.PremiumTransactionEnteredDate)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID ORDER BY PremiumTransactionEffectiveDate) = 1
),
LKP_StrategicProfitCenter AS (
	SELECT
	CurrentSnapshotFlag,
	StrategicProfitCenterAKId,
	StrategicProfitCenterCode,
	StrategicProfitCenterAbbreviation,
	StrategicProfitCenterDescription,
	in_StrategicProfitCenterAKId
	FROM (
		SELECT 
			CurrentSnapshotFlag,
			StrategicProfitCenterAKId,
			StrategicProfitCenterCode,
			StrategicProfitCenterAbbreviation,
			StrategicProfitCenterDescription,
			in_StrategicProfitCenterAKId
		FROM StrategicProfitCenter
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StrategicProfitCenterAKId ORDER BY CurrentSnapshotFlag) = 1
),
LKP_WorkDatacompromise AS (
	SELECT
	WorkDataCompromiseId,
	PremiumMasterCalculationID,
	in_PremiumMasterCalculationID
	FROM (
		SELECT 
			WorkDataCompromiseId,
			PremiumMasterCalculationID,
			in_PremiumMasterCalculationID
		FROM WorkDataCompromise
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumMasterCalculationID ORDER BY WorkDataCompromiseId DESC) = 1
),
EXP_GetValues AS (
	SELECT
	LKP_WorkDatacompromise.WorkDataCompromiseId AS lkp_WorkDataCompromiseId,
	LKP_PremiumTransaction.PremiumTransactionEffectiveDate AS lkp_PremiumTransactionEffectiveDate,
	SQ_DataCompromise.SourceSystemID AS in_SourceSystemID,
	SQ_DataCompromise.pol_id AS in_pol_id,
	SQ_DataCompromise.RiskLocationID AS in_RiskLocationID,
	SQ_DataCompromise.PolicyCoverageID AS in_PolicyCoverageID,
	SQ_DataCompromise.RatingCoverageId AS in_RatingCoverageId,
	SQ_DataCompromise.PremiumTransactionID AS in_PremiumTransactionID,
	SQ_DataCompromise.PremiumMasterCalculationID AS in_PremiumMasterCalculationID,
	SQ_DataCompromise.contract_cust_id AS in_contract_cust_id,
	SQ_DataCompromise.contract_cust_addr_id AS in_contract_cust_addr_id,
	SQ_DataCompromise.AgencyID AS in_AgencyID,
	SQ_DataCompromise.pol_sym AS in_pol_sym,
	SQ_DataCompromise.pol_num AS in_pol_num,
	SQ_DataCompromise.pol_mod AS in_pol_mod,
	SQ_DataCompromise.PolicyOfferingAKId AS in_PolicyOfferingAKId,
	SQ_DataCompromise.StrategicProfitCenterAKId AS in_StrategicProfitCenterAKId,
	SQ_DataCompromise.pol_eff_date AS in_pol_eff_date,
	SQ_DataCompromise.pol_exp_date AS in_pol_exp_date,
	SQ_DataCompromise.name AS in_name,
	SQ_DataCompromise.addr_line_1 AS in_addr_line_1,
	SQ_DataCompromise.addr_line_2 AS in_addr_line_2,
	SQ_DataCompromise.addr_line_3 AS in_addr_line_3,
	SQ_DataCompromise.city_name AS in_city_name,
	SQ_DataCompromise.state_prov_code AS in_state_prov_code,
	SQ_DataCompromise.zip_postal_code AS in_zip_postal_code,
	SQ_DataCompromise.FirstPartyLimit AS in_FirstPartyLimit,
	SQ_DataCompromise.DeductibleAmount AS in_DeductibleAmount,
	SQ_DataCompromise.ThirdPartyIndicator AS in_ThirdPartyIndicator,
	SQ_DataCompromise.TransactionCode AS in_TransactionCode,
	SQ_DataCompromise.prim_bus_class_code AS in_prim_bus_class_code,
	SQ_DataCompromise.ClassCode AS in_ClassCode,
	SQ_DataCompromise.prior_pol_key AS in_prior_pol_key,
	SQ_DataCompromise.AgencyCode AS in_AgencyCode,
	SQ_DataCompromise.PremiumMasterTransactionCode AS in_PremiumMasterTransactionCode,
	SQ_DataCompromise.PremiumMasterCoverageEffectiveDate AS in_PremiumMasterCoverageEffectiveDate,
	SQ_DataCompromise.PremiumMasterCoverageEffectiveDate AS in_PremiumMastrCoverageEffectiveDate,
	SQ_DataCompromise.PremiumMasterCoverageExpirationDate AS in_PremiumMasterCoverageExpirationDate,
	SQ_DataCompromise.PremiumMasterPremium AS in_PremiumMasterPremium,
	LKP_PolicyOffering.PolicyOfferingCode AS in_PolicyOfferingCode,
	LKP_StrategicProfitCenter.StrategicProfitCenterCode AS in_StrategicProfitCenterCode,
	LKP_StrategicProfitCenter.StrategicProfitCenterAbbreviation AS in_StrategicProfitCenterAbbreviation,
	LKP_StrategicProfitCenter.StrategicProfitCenterDescription AS in_StrategicProfitCenterDescription,
	SQ_DataCompromise.PremiumTransactionAKID AS in_PremiumTransactionAKID,
	SQ_DataCompromise.premiummasterfulltermpremium AS in_premiummasterfulltermpremium,
	SQ_DataCompromise.PremiumMasterRunDate AS in_PremiumMasterRunDate,
	SQ_DataCompromise.PolicyCoverageEffectiveDate AS in_PolicyCoverageEffectiveDate,
	SQ_DataCompromise.RatingCoverageCancellationDate AS in_RatingCoverageCancellationDate,
	SQ_DataCompromise.ProgramAKId AS in_ProgramAKId,
	Sysdate AS out_CreatedDate,
	Sysdate AS out_ModifiedDate,
	in_pol_id AS out_pol_id,
	in_RiskLocationID AS out_RiskLocationID,
	in_PolicyCoverageID AS out_PolicyCoverageID,
	in_RatingCoverageId AS out_RatingCoverageID,
	in_PremiumTransactionID AS out_PremiumTransactionID,
	in_PremiumMasterCalculationID AS out_PremiumMasterCalculationID,
	in_PremiumMasterRunDate AS out_RunDate,
	in_contract_cust_id AS out_contract_cust_id,
	in_contract_cust_addr_id AS out_contract_cust_addr_id,
	in_AgencyID AS out_AgencyID,
	in_pol_sym||in_pol_num||in_pol_mod AS out_PolKey,
	'2633' AS out_Company,
	'DCC' AS out_Productcode,
	-- *INF*: DECODE(in_PolicyOfferingCode, 
	-- '500', DECODE(in_StrategicProfitCenterDescription,'NSI', '1002721', '1002719'),
	-- '400', DECODE(in_StrategicProfitCenterDescription,'NSI', '1002720', '1002718'),
	-- '410', DECODE(in_StrategicProfitCenterDescription,'NSI', '1002720', '1002718'),
	-- '420', DECODE(in_StrategicProfitCenterDescription,'NSI', '1002720', '1002718'),
	-- '430', DECODE(in_StrategicProfitCenterDescription,'NSI', '1002720', '1002718'),
	-- '450', DECODE(in_StrategicProfitCenterDescription,'NSI', '1002720', '1002718'),
	-- ''
	-- )
	-- 
	-- 
	DECODE(
	    in_PolicyOfferingCode,
	    '500', DECODE(
	        in_StrategicProfitCenterDescription,
	        'NSI', '1002721',
	        '1002719'
	    ),
	    '400', DECODE(
	        in_StrategicProfitCenterDescription,
	        'NSI', '1002720',
	        '1002718'
	    ),
	    '410', DECODE(
	        in_StrategicProfitCenterDescription,
	        'NSI', '1002720',
	        '1002718'
	    ),
	    '420', DECODE(
	        in_StrategicProfitCenterDescription,
	        'NSI', '1002720',
	        '1002718'
	    ),
	    '430', DECODE(
	        in_StrategicProfitCenterDescription,
	        'NSI', '1002720',
	        '1002718'
	    ),
	    '450', DECODE(
	        in_StrategicProfitCenterDescription,
	        'NSI', '1002720',
	        '1002718'
	    ),
	    ''
	) AS out_ContractNumber,
	in_pol_eff_date AS out_PolicyEffectiveDate,
	in_pol_exp_date AS out_PolicyExpirationDate,
	-- *INF*: IIF(in_name='N/A', '', SUBSTR(in_name,1,55))
	IFF(in_name = 'N/A', '', SUBSTR(in_name, 1, 55)) AS out_nameOfInsured,
	-- *INF*: CONCAT(
	--                      CONCAT(
	--                                            IIF(RTRIM(LTRIM(in_addr_line_1))='N/A', '', RTRIM(LTRIM(in_addr_line_1))),
	--                                            IIF(RTRIM(LTRIM(in_addr_line_2))='N/A', '', RTRIM(LTRIM(in_addr_line_2)))),
	--                      IIF(RTRIM(LTRIM(in_addr_line_3))='N/A', '', RTRIM(LTRIM(in_addr_line_3))))
	CONCAT(CONCAT(
	        IFF(
	            RTRIM(LTRIM(in_addr_line_1)) = 'N/A', '', RTRIM(LTRIM(in_addr_line_1))
	        ), 
	        IFF(
	            RTRIM(LTRIM(in_addr_line_2)) = 'N/A', '', RTRIM(LTRIM(in_addr_line_2))
	        )), 
	    IFF(
	        RTRIM(LTRIM(in_addr_line_3)) = 'N/A', '', RTRIM(LTRIM(in_addr_line_3))
	    )) AS out_MailingAddressStreetName,
	-- *INF*: IIF(RTRIM(LTRIM(in_city_name))='N/A', '', RTRIM(LTRIM(in_city_name)))
	IFF(RTRIM(LTRIM(in_city_name)) = 'N/A', '', RTRIM(LTRIM(in_city_name))) AS out_MailingAddressCity,
	-- *INF*: IIF(RTRIM(LTRIM(in_state_prov_code))='N/A', '', RTRIM(LTRIM(in_state_prov_code)))
	IFF(RTRIM(LTRIM(in_state_prov_code)) = 'N/A', '', RTRIM(LTRIM(in_state_prov_code))) AS out_MailingAddressState,
	-- *INF*: IIF(RTRIM(LTRIM(in_zip_postal_code))='N/A', '', RTRIM(LTRIM(in_zip_postal_code)))
	-- 
	IFF(RTRIM(LTRIM(in_zip_postal_code)) = 'N/A', '', RTRIM(LTRIM(in_zip_postal_code))) AS out_MailingAddressZipCode,
	0 AS out_TotalPackageGrossPremium,
	0 AS out_TotalPropertyGrossPremium,
	-- *INF*: IIF(ISNULL(in_FirstPartyLimit) , '0', in_FirstPartyLimit)
	IFF(in_FirstPartyLimit IS NULL, '0', in_FirstPartyLimit) AS out_FirstPartyLimit,
	-- *INF*: IIF(ISNULL(in_DeductibleAmount) , :LKP.LKP_DefenseAndLiability(in_PremiumTransactionAKID), in_DeductibleAmount)
	IFF(
	    in_DeductibleAmount IS NULL,
	    LKP_DEFENSEANDLIABILITY_in_PremiumTransactionAKID.CoverageDeductibleValue,
	    in_DeductibleAmount
	) AS v_DeductibleAmount,
	-- *INF*: IIF(ISNULL(v_DeductibleAmount) , '0', v_DeductibleAmount)
	IFF(v_DeductibleAmount IS NULL, '0', v_DeductibleAmount) AS out_DeductibleAmount,
	-- *INF*: IIF(RTRIM(LTRIM(in_prim_bus_class_code))='N/A' ,
	-- RTRIM(LTRIM(in_ClassCode)),
	--  RTRIM(LTRIM(in_prim_bus_class_code)))
	IFF(
	    RTRIM(LTRIM(in_prim_bus_class_code)) = 'N/A', RTRIM(LTRIM(in_ClassCode)),
	    RTRIM(LTRIM(in_prim_bus_class_code))
	) AS out_OccupancyCode,
	0 AS out_PolicyTotalInsuredValue,
	-- *INF*: IIF(RTRIM(LTRIM(in_prior_pol_key))='N/A', '', RTRIM(LTRIM(in_prior_pol_key)))
	IFF(RTRIM(LTRIM(in_prior_pol_key)) = 'N/A', '', RTRIM(LTRIM(in_prior_pol_key))) AS out_PreviousPolicyNumber,
	-- *INF*: IIF(ISNULL(in_AgencyCode) OR in_AgencyCode='N/A', '', in_AgencyCode)
	IFF(in_AgencyCode IS NULL OR in_AgencyCode = 'N/A', '', in_AgencyCode) AS out_AgencyCode,
	-- *INF*: DECODE (TRUE,
	-- ISNULL(in_StrategicProfitCenterAbbreviation), 'Other',
	-- LTRIM(RTRIM(in_StrategicProfitCenterAbbreviation)) = 'NSI',:LKP.LKP_PROGRAMCODEFORNSI(in_ProgramAKId),
	-- in_StrategicProfitCenterAbbreviation)
	-- 
	-- -- below was the exsisting code and the chnage is made for WREQ-11766
	-- 
	-- --IIF(ISNULL(in_StrategicProfitCenterAbbreviation), 'Other', in_StrategicProfitCenterAbbreviation)
	-- 
	-- 
	DECODE(
	    TRUE,
	    in_StrategicProfitCenterAbbreviation IS NULL, 'Other',
	    LTRIM(RTRIM(in_StrategicProfitCenterAbbreviation)) = 'NSI', LKP_PROGRAMCODEFORNSI_in_ProgramAKId.ProgramDescription,
	    in_StrategicProfitCenterAbbreviation
	) AS out_BranchCode,
	-- *INF*: IIF(ISNULL(in_ThirdPartyIndicator), 'N', 'Y')
	IFF(in_ThirdPartyIndicator IS NULL, 'N', 'Y') AS out_ThirdPartyIndicator,
	-- *INF*: --Fix EDWP-3822 to remove the '?' from Endorse
	-- DECODE(TRUE,
	-- LTRIM(RTRIM(in_TransactionCode))= 'New','01',
	-- LTRIM(RTRIM(in_TransactionCode))='Cancel','03',
	-- LTRIM(RTRIM(in_TransactionCode))='Rewrite','04',
	-- LTRIM(RTRIM(in_TransactionCode))='Renew','07',
	-- LTRIM(RTRIM(in_TransactionCode))='Reinstate','10',
	-- LTRIM(RTRIM(in_TransactionCode))='Endorse' AND LTRIM(RTRIM(TO_CHAR(in_RatingCoverageCancellationDate,'MM/DD/YYYY'))) ='12/31/2100','30',
	-- LTRIM(RTRIM(in_TransactionCode))='Endorse' 
	-- AND LTRIM(RTRIM(TO_CHAR(in_RatingCoverageCancellationDate,'MM/DD/YYYY'))) <> '12/31/2100','03',
	-- LTRIM(RTRIM(in_TransactionCode))='Reissue','04',
	-- '')
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(in_TransactionCode)) = 'New', '01',
	    LTRIM(RTRIM(in_TransactionCode)) = 'Cancel', '03',
	    LTRIM(RTRIM(in_TransactionCode)) = 'Rewrite', '04',
	    LTRIM(RTRIM(in_TransactionCode)) = 'Renew', '07',
	    LTRIM(RTRIM(in_TransactionCode)) = 'Reinstate', '10',
	    LTRIM(RTRIM(in_TransactionCode)) = 'Endorse' AND LTRIM(RTRIM(TO_CHAR(in_RatingCoverageCancellationDate, 'MM/DD/YYYY'))) = '12/31/2100', '30',
	    LTRIM(RTRIM(in_TransactionCode)) = 'Endorse' AND LTRIM(RTRIM(TO_CHAR(in_RatingCoverageCancellationDate, 'MM/DD/YYYY'))) <> '12/31/2100', '03',
	    LTRIM(RTRIM(in_TransactionCode)) = 'Reissue', '04',
	    ''
	) AS out_TransactionCode,
	-- *INF*: IIF(ISNULL(in_PremiumMasterCoverageEffectiveDate), TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'), in_PremiumMasterCoverageEffectiveDate)
	IFF(
	    in_PremiumMasterCoverageEffectiveDate IS NULL,
	    TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
	    in_PremiumMasterCoverageEffectiveDate
	) AS out_TransactionEffectiveDate,
	-- *INF*: --edwp4376 PolicyCoverageEffectiveDt when the Transaction Code is cancel, otherwise follow old logic
	-- iif(in_TransactionCode='Cancel',in_PolicyCoverageEffectiveDate,
	-- 	iif (isnull(lkp_PremiumTransactionEffectiveDate),
	-- 	in_PremiumMastrCoverageEffectiveDate,lkp_PremiumTransactionEffectiveDate))
	IFF(
	    in_TransactionCode = 'Cancel', in_PolicyCoverageEffectiveDate,
	    IFF(
	        lkp_PremiumTransactionEffectiveDate IS NULL, in_PremiumMastrCoverageEffectiveDate,
	        lkp_PremiumTransactionEffectiveDate
	    )
	) AS out_CoverageEffectiveDate,
	in_PremiumMasterCoverageExpirationDate AS out_CoverageExpirationDate,
	-- *INF*: IIF(ISNULL(in_premiummasterfulltermpremium),0,in_premiummasterfulltermpremium)
	IFF(in_premiummasterfulltermpremium IS NULL, 0, in_premiummasterfulltermpremium) AS out_CoverageGrossPremium,
	-- *INF*: ROUND(IIF(ISNULL(in_PremiumMasterPremium),0,in_PremiumMasterPremium) ,2)
	ROUND(
	    IFF(
	        in_PremiumMasterPremium IS NULL, 0, in_PremiumMasterPremium
	    ), 2) AS out_CoverageNetPremium,
	-- *INF*: DECODE(in_PolicyOfferingCode, 
	-- '500', '016',
	-- '400', '011',
	-- '410', '011',
	-- '420', '011',
	-- '430', '011',
	-- '450', '011',
	-- ''
	-- )
	-- 
	DECODE(
	    in_PolicyOfferingCode,
	    '500', '016',
	    '400', '011',
	    '410', '011',
	    '420', '011',
	    '430', '011',
	    '450', '011',
	    ''
	) AS out_ProgramID,
	-- *INF*: TRUNC(@{pipeline().parameters.EXTRACTDATE},'D')
	CAST(TRUNC(@{pipeline().parameters.EXTRACTDATE}, 'DAY') AS TIMESTAMP_NTZ(0)) AS out_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS out_SourceSystemID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS out_AuditID,
	LKP_PolicyCurrentStatusDim.PolicyCancellationDate
	FROM SQ_DataCompromise
	LEFT JOIN LKP_PolicyCurrentStatusDim
	ON LKP_PolicyCurrentStatusDim.EDWPolicyAKId = SQ_DataCompromise.PolicyAKID
	LEFT JOIN LKP_PolicyOffering
	ON LKP_PolicyOffering.PolicyOfferingAKId = SQ_DataCompromise.PolicyOfferingAKId
	LEFT JOIN LKP_PremiumTransaction
	ON LKP_PremiumTransaction.PolicyAKID = SQ_DataCompromise.PolicyAKID
	LEFT JOIN LKP_StrategicProfitCenter
	ON LKP_StrategicProfitCenter.StrategicProfitCenterAKId = SQ_DataCompromise.StrategicProfitCenterAKId
	LEFT JOIN LKP_WorkDatacompromise
	ON LKP_WorkDatacompromise.PremiumMasterCalculationID = SQ_DataCompromise.PremiumMasterCalculationID
	LEFT JOIN LKP_DEFENSEANDLIABILITY LKP_DEFENSEANDLIABILITY_in_PremiumTransactionAKID
	ON LKP_DEFENSEANDLIABILITY_in_PremiumTransactionAKID.PremiumTransactionAKID = in_PremiumTransactionAKID

	LEFT JOIN LKP_PROGRAMCODEFORNSI LKP_PROGRAMCODEFORNSI_in_ProgramAKId
	ON LKP_PROGRAMCODEFORNSI_in_ProgramAKId.ProgramAKId = in_ProgramAKId

),
FIL_WorkDataCompromise AS (
	SELECT
	out_CreatedDate AS CreatedDate, 
	out_ModifiedDate AS ModifiedDate, 
	out_pol_id AS PolicyID, 
	out_RiskLocationID AS RiskLocationID, 
	out_PolicyCoverageID AS PolicyCoverageID, 
	out_RatingCoverageID AS RatingCoverageID, 
	out_PremiumTransactionID AS PremiumTransactionID, 
	out_PremiumMasterCalculationID AS PremiumMasterCalculationID, 
	out_RunDate AS RunDate, 
	out_contract_cust_id AS ContractCustID, 
	out_contract_cust_addr_id AS ContractCustAddrID, 
	out_AgencyID AS AgencyID, 
	out_PolKey AS PolKey, 
	out_Company AS Company, 
	out_Productcode AS ProductCode, 
	out_ContractNumber AS ContractNumber, 
	out_PolicyEffectiveDate AS PolicyEffectiveDate, 
	out_PolicyExpirationDate AS PolicyExpirationDate, 
	out_nameOfInsured AS NameOfInsured, 
	out_MailingAddressStreetName AS MailingAddressStreetName, 
	out_MailingAddressCity AS MailingAddressCity, 
	out_MailingAddressState AS MailingAddressState, 
	out_MailingAddressZipCode AS MailingAddressZipCode, 
	out_TotalPackageGrossPremium AS TotalPackageGrossPremium, 
	out_TotalPropertyGrossPremium AS TotalPropertyGrossPremium, 
	out_FirstPartyLimit AS FirstPartyLimit, 
	out_DeductibleAmount AS DeductibleAmount, 
	out_OccupancyCode AS OccupancyCode, 
	out_PolicyTotalInsuredValue AS PolicyTotalInsuredValue, 
	out_PreviousPolicyNumber AS PreviousPolicyNumber, 
	out_AgencyCode AS AgencyCode, 
	out_BranchCode AS BranchCode, 
	out_ThirdPartyIndicator AS ThirdPartyIndicator, 
	out_TransactionCode AS TransactionCode, 
	out_TransactionEffectiveDate AS TransactionEffectiveDate, 
	out_CoverageEffectiveDate AS CoverageEffectiveDate, 
	out_CoverageExpirationDate AS CoverageExpirationDate, 
	out_CoverageGrossPremium AS CoverageGrossPremium, 
	out_CoverageNetPremium AS CoverageNetPremium, 
	out_ProgramID AS ProgramID, 
	out_AuditID AS AuditID, 
	out_SourceSystemID AS SourceSystemID, 
	lkp_WorkDataCompromiseId, 
	PolicyCancellationDate
	FROM EXP_GetValues
	WHERE ContractNumber != '' and ProgramID  != '' and isnull(lkp_WorkDataCompromiseId)
--fix for Defect 3350 adding filter condition to limit only valid and active coverages
--and isnull(StatisticalCoverageCancellationDate)
),
WorkDataCompromise AS (
	INSERT INTO WorkDataCompromise
	(AuditID, SourceSystemID, CreatedDate, ModifiedDate, PolicyID, RiskLocationID, PolicyCoverageID, RatingCoverageID, PremiumTransactionID, PremiumMasterCalculationID, RunDate, ContractCustID, ContractCustAddrID, AgencyID, PolKey, Company, ProductCode, ContractNumber, PolicyEffectiveDate, PolicyExpirationDate, NameOfInsured, MailingAddressStreetName, MailingAddressCity, MailingAddressState, MailingAddressZipCode, TotalPackageGrossPremium, TotalPropertyGrossPremium, FirstPartyLimit, DeductibleAmount, OccupancyCode, PolicyTotalInsuredValue, PreviousPolicyNumber, AgencyCode, BranchCode, ThirdPartyIndicator, TransactionCode, TransactionEffectiveDate, CoverageEffectiveDate, CoverageExpirationDate, CoverageGrossPremium, CoverageNetPremium, ProgramID, PolicyCancellationDate)
	SELECT 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	POLICYID, 
	RISKLOCATIONID, 
	POLICYCOVERAGEID, 
	RATINGCOVERAGEID, 
	PREMIUMTRANSACTIONID, 
	PREMIUMMASTERCALCULATIONID, 
	RUNDATE, 
	CONTRACTCUSTID, 
	CONTRACTCUSTADDRID, 
	AGENCYID, 
	POLKEY, 
	COMPANY, 
	PRODUCTCODE, 
	CONTRACTNUMBER, 
	POLICYEFFECTIVEDATE, 
	POLICYEXPIRATIONDATE, 
	NAMEOFINSURED, 
	MAILINGADDRESSSTREETNAME, 
	MAILINGADDRESSCITY, 
	MAILINGADDRESSSTATE, 
	MAILINGADDRESSZIPCODE, 
	TOTALPACKAGEGROSSPREMIUM, 
	TOTALPROPERTYGROSSPREMIUM, 
	FIRSTPARTYLIMIT, 
	DEDUCTIBLEAMOUNT, 
	OCCUPANCYCODE, 
	POLICYTOTALINSUREDVALUE, 
	PREVIOUSPOLICYNUMBER, 
	AGENCYCODE, 
	BRANCHCODE, 
	THIRDPARTYINDICATOR, 
	TRANSACTIONCODE, 
	TRANSACTIONEFFECTIVEDATE, 
	COVERAGEEFFECTIVEDATE, 
	COVERAGEEXPIRATIONDATE, 
	COVERAGEGROSSPREMIUM, 
	COVERAGENETPREMIUM, 
	PROGRAMID, 
	POLICYCANCELLATIONDATE
	FROM FIL_WorkDataCompromise
),
SQ_WorkDataCompromise_RatingCoverage AS (
	;With _CTE as (
	select distinct substring(polkey,4,9) as polkey, RatingCoverageID, WorkDataCompromiseId
	from 
	-- gathering only records from within the past year, we do not want to keep reprocessing old history
	DataFeedMart.dbo.WorkDataCompromise where Convert(Date,RunDate) >= CONVERT(DATE,DATEADD(Year,-1,@{pipeline().parameters.RUNDATE}))
	and OccupancyCode='0'
	)
	
	Select B.polkey,B.WorkDataCompromiseId, 
	Case When B.AK_ClassCode ='N/A' then B.EB_ClassCode Else B.AK_ClassCode End as OccupancyCode
	
	From(
	select distinct  
	polkey, 
	RC.CoverageType,
	isnull(RCAK.ClassCode,'N/A') as AK_ClassCode, 
	B.WorkDataCompromiseId, 
	isnull(A.ClassCode,'N/A') as EB_ClassCode, 
	isnull(A.CoverageType,'N/A') as EB_CoverageType, 
	isnull(A.RatingcoverageId, 0) as EB_RatingCoverageId, 
	case 
	when A.RatingcoverageId is null then 1 else
	ROW_NUMBER() Over (partition by WorkDataCompromiseId order by A.RatingcoverageId desc) 
	End as rn
	from 
	RatingCoverage RC with (nolock)
	inner join _CTE B on RC.RatingCoverageId=B.RatingCoverageID
	inner join PolicyCoverage PC with (nolock) on RC.PolicyCoverageAKID=PC.PolicyCoverageAKID and PC.CurrentSnapshotFlag=1
	inner join v2.policy P with (nolock) on PC.PolicyAKID=P.pol_ak_id and P.crrnt_snpsht_flag=1 
	left join RatingCoverage RCAK with (nolock) on RC.RatingCoverageAKID=RCAK.RatingCoverageAKID and RCAK.ClassCode !='0'
	outer apply
	(select distinct RC_EB.ClassCode, RC_EB.CoverageType, max(RC_EB.ratingcoverageid) as RatingcoverageId
	from 
	v2.policy P2 with (nolock)
	inner join PolicyCoverage PC2 with (nolock) on P2.pol_ak_id=PC2.PolicyAKID and PC2.currentsnapshotflag=1
	inner join RatingCoverage RC_EB with (nolock) on RC_EB.PolicyCoverageAKID=PC2.PolicyCoverageAKID 
	and RC_EB.CoverageType='EquipmentBreakdown' and RC_EB.ClassCode not in ('0','N/A') and RCAK.ClassCode is null
	where P2.pol_key=P.pol_key
	group by RC_EB.ClassCode, RC_EB.CoverageType
	) A
	) B where rn=1 
	order by 2
),
EXP_Update_OccupancyCode_Input AS (
	SELECT
	PolKey,
	WorkDataCompromiseId,
	OccupancyCode
	FROM SQ_WorkDataCompromise_RatingCoverage
),
UPD_Update_OccupancyCode AS (
	SELECT
	WorkDataCompromiseId, 
	OccupancyCode
	FROM EXP_Update_OccupancyCode_Input
),
WorkDataCompromise_Update AS (
	MERGE INTO WorkDataCompromise AS T
	USING UPD_Update_OccupancyCode AS S
	ON T.WorkDataCompromiseId = S.WorkDataCompromiseId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.OccupancyCode = S.OccupancyCode
),
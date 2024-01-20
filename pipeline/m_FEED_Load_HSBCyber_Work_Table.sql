WITH
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
SQ_WorkHSBCyber AS (
	Declare @STARTDATE as datetime,
	@ENDDATE as datetime
	set @STARTDATE=@{pipeline().parameters.STARTDATE}
	set @ENDDATE=@{pipeline().parameters.ENDDATE}
	SELECT 
	PremiumMasterCalculation.SourceSystemID,
	policy.pol_id, RiskLocation.RiskLocationID, 
	PolicyCoverage.PolicyCoverageID, 
	RatingCoverage.RatingCoverageId, 
	PremiumTransaction.PremiumTransactionID, 
	PremiumMasterCalculation.PremiumMasterCalculationID, 
	contract_customer.contract_cust_id, 
	contract_customer_address.contract_cust_addr_id, 
	Agency.AgencyID, policy.pol_sym, policy.pol_num, 
	policy.pol_mod, policy.PolicyOfferingAKId, 
	policy.StrategicProfitCenterAKId, 
	policy.pol_eff_date, policy.pol_exp_date, 
	contract_customer.name, 
	contract_customer_address.addr_line_1, 
	contract_customer_address.addr_line_2, 
	contract_customer_address.addr_line_3, 
	contract_customer_address.city_name, 
	contract_customer_address.state_prov_code, 
	contract_customer_address.zip_postal_code, 
	---------------------------------------------------------------------------------------------------------------
	ComputerAttackLimit.CoverageLimitValue as CyberOneFirstPartyLimit,
	ComputerAttackExtortionDeductible.CoverageDeductibleValue as CyberOneFirstPartyDeductible,
	NetworkSecurityLimit.CoverageLimitValue as CyberOneThirdPartyLimit,
	NetworkSecurityDeductible.CoverageDeductibleValue as CyberOneThirdPartyDeductible,
	ExtortionLimit.CoverageLimitValue as ExtortionLimit,
	PremiumTransaction.PremiumTransactionCode,
	----------------------------------------------------------------------------------------------------------------
	policy.prim_bus_class_code,
	RatingCoverage.ClassCode,
	policy.prior_pol_key, 
	Agency.AgencyCode, 
	PremiumMasterCalculation.
	PremiumMasterTransactionCode, 
	PremiumMasterCalculation.PremiumMasterCoverageEffectiveDate, 
	--RatingCoverage.RatingCoverageEffectiveDate,
	--RatingCoverage.RatingCoverageExpirationDate, 
	PremiumMasterCalculation.PremiumMasterCoverageExpirationDate, 
	PremiumMasterCalculation.PremiumMasterPremium ,
	PremiumTransaction.PremiumTransactionAKID,
	PremiumMasterCalculation.premiummasterfulltermpremium,
	PremiumMasterCalculation.PremiumMasterRunDate,
	PolicyCoverage.PolicyAKID,
	PolicyCoverage.PolicyCoverageEffectiveDate,
	RatingCoverage.RatingCoverageCancellationDate,
	policy.ProgramAKId,
	RatingCoverage.CoverageType,PremiumTransaction.PremiumTransactionEnteredDate
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
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit on CoverageLimit.CoverageLimitId=CoverageLimitBridge.CoverageLimitId  ) as  ComputerAttackLimit
	 on ComputerAttackLimit.PremiumTransactionAKID= PremiumTransaction.PremiumTransactionAKID 
	and ComputerAttackLimit.CoverageLimitType = 'Computer Attack Limit'
	 and ComputerAttackLimit.CoverageLimitValue<>0
	--------------------------------
	left join (select CoveragedeductibleBridge.PremiumTransactionAKID , CoverageDeductible.CoverageDeductibleType,  CoverageDeductible.CoverageDeductibleValue
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoveragedeductibleBridge inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductible on CoverageDeductible.CoverageDeductibleId=CoverageDeductibleBridge.CoverageDeductibleId  ) as  ComputerAttackExtortionDeductible
	 on ComputerAttackExtortionDeductible.PremiumTransactionAKID= PremiumTransaction.PremiumTransactionAKID 
	and ComputerAttackExtortionDeductible.CoverageDeductibleType ='Computer Attack & Cyber Extortion Deductible'
	 and ComputerAttackExtortionDeductible.CoverageDeductibleValue<>0
	--------------------------------
	left join (select CoverageLimitBridge.PremiumTransactionAKID , CoverageLimit.CoverageLimitType,  CoverageLimit.CoverageLimitValue
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit on 
	CoverageLimit.CoverageLimitId=CoverageLimitBridge.CoverageLimitId  ) as  NetworkSecurityLimit
	on NetworkSecurityLimit.PremiumTransactionAKID= PremiumTransaction.PremiumTransactionAKID 
	and NetworkSecurityLimit.CoverageLimitType='Network Security Liability Limit'
	and NetworkSecurityLimit.CoverageLimitValue<>0
	-------------------------
	left join (select CoveragedeductibleBridge.PremiumTransactionAKID , CoverageDeductible.CoverageDeductibleType,  CoverageDeductible.CoverageDeductibleValue
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoveragedeductibleBridge inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductible on CoverageDeductible.CoverageDeductibleId=CoverageDeductibleBridge.CoverageDeductibleId  ) as  NetworkSecurityDeductible
	 on NetworkSecurityDeductible.PremiumTransactionAKID= PremiumTransaction.PremiumTransactionAKID 
	and NetworkSecurityDeductible.CoverageDeductibleType = 'Network Security Liability Deductible' and NetworkSecurityDeductible.CoverageDeductibleValue<>0
	
	left join (select CoverageLimitBridge.PremiumTransactionAKID , CoverageLimit.CoverageLimitType,  CoverageLimit.CoverageLimitValue
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit on 
	CoverageLimit.CoverageLimitId=CoverageLimitBridge.CoverageLimitId  ) as  ExtortionLimit
	on ExtortionLimit.PremiumTransactionAKID= PremiumTransaction.PremiumTransactionAKID 
	and ExtortionLimit.CoverageLimitType='Cyber Extortion Sublimit'
	and ExtortionLimit.CoverageLimitValue<>0  
	
	WHERE
	PremiumMasterCalculation.PremiumMasterPremiumType='D'  
	and 
	PremiumMasterCalculation.PremiumMasterReasonAmendedCode not in ('COL','CWO')
	and 
	((PremiumMasterCalculation.PremiumMasterRunDate between @STARTDATE and @ENDDATE)
	OR 
	(PremiumMasterCoverageEffectiveDate <  @ENDDATE AND PremiumMasterCoverageExpirationDate > @ENDDATE))
	and PremiumMasterCalculation.PremiumMasterRunDate  between policy.pol_eff_date and policy.pol_exp_date
	and PolicyCoverage.SourceSystemID='DCT' 
	and policy.source_sys_id='DCT'
	and contract_customer.source_sys_id='DCT'
	and contract_customer_address.source_sys_id='DCT'
	and RiskLocation.SourceSystemID='DCT'
	and RatingCoverage.SourceSystemID='DCT'
	and RatingCoverage.CoverageType in ('CyberComputerAttack','CyberNetworkSecurity','CyberExtortionExpenses')
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
		select pc.PolicyAKID as PolicyAKID, pt.PremiumTransactionEffectiveDate as PremiumTransactionEffectiveDate
		 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction pt join 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage rc 
		on rc.RatingCoverageAKID = pt.RatingCoverageAKId
		and rc.EffectiveDate = pt.EffectiveDate
		and rc.RatingCoverageCancellationDate >'2100-12-31'
		and rc.CoverageType  in ('CyberComputerAttack','CyberExtendedReportingPeriod','CyberNetworkSecurity','CyberExtortion')
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.policycoverage pc
		on (pc.PolicyCoverageAKID = rc.PolicyCoverageAKID
		and pc.CurrentSnapshotFlag = 1)
		where not exists (select * from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction pt1 
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage rc1 
		on rc1.RatingCoverageAKID = pt1.RatingCoverageAKId
		and rc1.EffectiveDate = pt1.EffectiveDate
		and rc1.RatingCoverageCancellationDate > '2100-12-31'
		and rc1.CoverageType  in ('CyberComputerAttack','CyberNetworkSecurity','CyberExtortion')
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.policycoverage pc1
		on (pc1.PolicyCoverageAKID = rc1.PolicyCoverageAKID
		and pc1.CurrentSnapshotFlag = 1)
		where pc1.PolicyAKID = pc.PolicyAKID
		and pt1.PremiumTransactionEnteredDate < pt.PremiumTransactionEnteredDate)
		and not exists (select * from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction pt1
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage rc1 
		on rc1.RatingCoverageAKID = pt1.RatingCoverageAKId
		and rc1.EffectiveDate = pt1.EffectiveDate
		and rc1.RatingCoverageCancellationDate < '2100-12-31'
		and rc1.CoverageType  in ('CyberComputerAttack','CyberNetworkSecurity','CyberExtortion')
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.policycoverage pc1
		on (pc1.PolicyCoverageAKID = rc1.PolicyCoverageAKID
		and pc1.CurrentSnapshotFlag = 1)
		where pc1.PolicyAKID = pc.PolicyAKID
		and pt1.PremiumTransactionEnteredDate>pt.PremiumTransactionEnteredDate)
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
LKP_WorkHSBCyber AS (
	SELECT
	WorkHSBCyberId,
	PremiumMasterCalculationId,
	in_PremiumMasterCalculationId
	FROM (
		SELECT 
			WorkHSBCyberId,
			PremiumMasterCalculationId,
			in_PremiumMasterCalculationId
		FROM WorkHSBCyber
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumMasterCalculationId ORDER BY WorkHSBCyberId) = 1
),
EXP_GetValues AS (
	SELECT
	LKP_WorkHSBCyber.WorkHSBCyberId AS lkp_WorkHSBCyberID,
	LKP_PremiumTransaction.PremiumTransactionEffectiveDate AS lkp_PremiumTransactionEffectiveDate,
	SQ_WorkHSBCyber.SourceSystemID AS in_SourceSystemID,
	SQ_WorkHSBCyber.pol_id AS in_pol_id,
	SQ_WorkHSBCyber.RiskLocationID AS in_RiskLocationID,
	SQ_WorkHSBCyber.PolicyCoverageID AS in_PolicyCoverageID,
	SQ_WorkHSBCyber.RatingCoverageId AS in_RatingCoverageId,
	SQ_WorkHSBCyber.PremiumTransactionID AS in_PremiumTransactionID,
	SQ_WorkHSBCyber.PremiumMasterCalculationID AS in_PremiumMasterCalculationID,
	SQ_WorkHSBCyber.contract_cust_id AS in_contract_cust_id,
	SQ_WorkHSBCyber.contract_cust_addr_id AS in_contract_cust_addr_id,
	SQ_WorkHSBCyber.AgencyID AS in_AgencyID,
	SQ_WorkHSBCyber.pol_sym AS in_pol_sym,
	SQ_WorkHSBCyber.pol_num AS in_pol_num,
	SQ_WorkHSBCyber.pol_mod AS in_pol_mod,
	SQ_WorkHSBCyber.PolicyOfferingAKId AS in_PolicyOfferingAKId,
	SQ_WorkHSBCyber.StrategicProfitCenterAKId AS in_StrategicProfitCenterAKId,
	SQ_WorkHSBCyber.pol_eff_date AS in_pol_eff_date,
	SQ_WorkHSBCyber.pol_exp_date AS in_pol_exp_date,
	SQ_WorkHSBCyber.name AS in_name,
	SQ_WorkHSBCyber.addr_line_1 AS in_addr_line_1,
	SQ_WorkHSBCyber.addr_line_2 AS in_addr_line_2,
	SQ_WorkHSBCyber.addr_line_3 AS in_addr_line_3,
	SQ_WorkHSBCyber.city_name AS in_city_name,
	SQ_WorkHSBCyber.state_prov_code AS in_state_prov_code,
	SQ_WorkHSBCyber.zip_postal_code AS in_zip_postal_code,
	SQ_WorkHSBCyber.FirstPartyLimit AS in_FirstPartyLimit,
	SQ_WorkHSBCyber.FirstPartyDeductible AS in_FirstPartyDeductible,
	SQ_WorkHSBCyber.TransactionCode AS in_TransactionCode,
	SQ_WorkHSBCyber.prim_bus_class_code AS in_prim_bus_class_code,
	SQ_WorkHSBCyber.ClassCode AS in_ClassCode,
	SQ_WorkHSBCyber.prior_pol_key AS in_prior_pol_key,
	SQ_WorkHSBCyber.AgencyCode AS in_AgencyCode,
	SQ_WorkHSBCyber.PremiumMasterTransactionCode AS in_PremiumMasterTransactionCode,
	SQ_WorkHSBCyber.PremiumMasterCoverageEffectiveDate AS in_PremiumMasterCoverageEffectiveDate,
	SQ_WorkHSBCyber.PremiumMasterCoverageExpirationDate AS in_PremiumMasterCoverageExpirationDate,
	SQ_WorkHSBCyber.PremiumMasterPremium AS in_PremiumMasterPremium,
	LKP_PolicyOffering.PolicyOfferingCode AS in_PolicyOfferingCode,
	LKP_StrategicProfitCenter.StrategicProfitCenterCode AS in_StrategicProfitCenterCode,
	LKP_StrategicProfitCenter.StrategicProfitCenterAbbreviation AS in_StrategicProfitCenterAbbreviation,
	LKP_StrategicProfitCenter.StrategicProfitCenterDescription AS in_StrategicProfitCenterDescription,
	SQ_WorkHSBCyber.PremiumTransactionAKID AS in_PremiumTransactionAKID,
	SQ_WorkHSBCyber.premiummasterfulltermpremium AS in_premiummasterfulltermpremium,
	SQ_WorkHSBCyber.PremiumMasterRunDate AS in_PremiumMasterRunDate,
	SQ_WorkHSBCyber.PolicyCoverageEffectiveDate AS in_PolicyCoverageEffectiveDate,
	SQ_WorkHSBCyber.RatingCoverageCancellationDate AS in_RatingCoverageCancellationDate,
	SQ_WorkHSBCyber.ProgramAKId AS in_ProgramAKId,
	SQ_WorkHSBCyber.ThirdPartyDeductible AS in_ThirdPartyDeductible,
	SQ_WorkHSBCyber.ThirdPartyLimit AS in_ThirdPartyLimit,
	SQ_WorkHSBCyber.ExtortionLimit,
	Sysdate AS out_CreatedDate,
	Sysdate AS out_ModifiedDate,
	in_pol_id AS out_pol_id,
	in_RiskLocationID AS out_RiskLocationID,
	in_PolicyCoverageID AS out_PolicyCoverageID,
	in_RatingCoverageId AS out_RatingCoverageID,
	-- *INF*: IIF(ISNULL(in_PremiumTransactionID),-1,in_PremiumTransactionID)
	IFF(in_PremiumTransactionID IS NULL, - 1, in_PremiumTransactionID) AS out_PremiumTransactionID,
	-- *INF*: IIF(ISNULL(in_PremiumMasterCalculationID),-1,in_PremiumMasterCalculationID)
	IFF(in_PremiumMasterCalculationID IS NULL, - 1, in_PremiumMasterCalculationID) AS out_PremiumMasterCalculationID,
	in_PremiumMasterRunDate AS out_RunDate,
	in_contract_cust_id AS out_contract_cust_id,
	in_contract_cust_addr_id AS out_contract_cust_addr_id,
	in_AgencyID AS out_AgencyID,
	-- *INF*: LTRIM(RTRIM(in_pol_sym))||LTRIM(RTRIM(in_pol_num))
	LTRIM(RTRIM(in_pol_sym)) || LTRIM(RTRIM(in_pol_num)) AS out_PolKey,
	'3851' AS out_Company,
	'CY1' AS out_Productcode,
	-- *INF*: IIF(IN(CoverageType,'CyberComputerAttack','CyberExtortionExpenses') ,DECODE(in_PolicyOfferingCode, 
	-- '500', DECODE(in_StrategicProfitCenterDescription,'NSI', '1003700', '1003698'),
	-- '400', DECODE(in_StrategicProfitCenterDescription,'NSI', '1003702', '1003696'),
	-- '410', DECODE(in_StrategicProfitCenterDescription,'NSI', '1003702', '1003696'),
	-- '420', DECODE(in_StrategicProfitCenterDescription,'NSI', '1003702', '1003696'),
	-- '430', DECODE(in_StrategicProfitCenterDescription,'NSI', '1003702', '1003696'),
	-- '450', DECODE(in_StrategicProfitCenterDescription,'NSI', '1003702', '1003696'),
	-- ''
	-- ),IIF(IN(CoverageType,'CyberNetworkSecurity'),DECODE(in_PolicyOfferingCode, 
	-- '500', DECODE(in_StrategicProfitCenterDescription,'NSI', '1003701', '1003699'),
	-- '400', DECODE(in_StrategicProfitCenterDescription,'NSI', '1003703', '1003697'),
	-- '410', DECODE(in_StrategicProfitCenterDescription,'NSI', '1003703', '1003697'),
	-- '420', DECODE(in_StrategicProfitCenterDescription,'NSI', '1003703', '1003697'),
	-- '430', DECODE(in_StrategicProfitCenterDescription,'NSI', '1003703', '1003697'),
	-- '450', DECODE(in_StrategicProfitCenterDescription,'NSI', '1003703', '1003697'),
	-- ''
	-- ),DECODE(in_PolicyOfferingCode, 
	-- '500', DECODE(in_StrategicProfitCenterDescription,'NSI', '1003657', '1003655'),
	-- '400', DECODE(in_StrategicProfitCenterDescription,'NSI', '1003656', '1003654'),
	-- '410', DECODE(in_StrategicProfitCenterDescription,'NSI', '1003656', '1003654'),
	-- '420', DECODE(in_StrategicProfitCenterDescription,'NSI', '1003656', '1003654'),
	-- '430', DECODE(in_StrategicProfitCenterDescription,'NSI', '1003656', '1003654'),
	-- '450', DECODE(in_StrategicProfitCenterDescription,'NSI', '1003656', '1003654'),
	-- ''
	-- )
	-- 
	-- )
	-- )
	IFF(
	    CoverageType IN ('CyberComputerAttack','CyberExtortionExpenses'),
	    DECODE(
	        in_PolicyOfferingCode,
	        '500', DECODE(
	            in_StrategicProfitCenterDescription,
	            'NSI', '1003700',
	            '1003698'
	        ),
	        '400', DECODE(
	            in_StrategicProfitCenterDescription,
	            'NSI', '1003702',
	            '1003696'
	        ),
	        '410', DECODE(
	            in_StrategicProfitCenterDescription,
	            'NSI', '1003702',
	            '1003696'
	        ),
	        '420', DECODE(
	            in_StrategicProfitCenterDescription,
	            'NSI', '1003702',
	            '1003696'
	        ),
	        '430', DECODE(
	            in_StrategicProfitCenterDescription,
	            'NSI', '1003702',
	            '1003696'
	        ),
	        '450', DECODE(
	            in_StrategicProfitCenterDescription,
	            'NSI', '1003702',
	            '1003696'
	        ),
	        ''
	    ),
	    IFF(
	        CoverageType IN ('CyberNetworkSecurity'),
	        DECODE(
	            in_PolicyOfferingCode,
	            '500', DECODE(
	                in_StrategicProfitCenterDescription,
	                'NSI', '1003701',
	                '1003699'
	            ),
	            '400', DECODE(
	                in_StrategicProfitCenterDescription,
	                'NSI', '1003703',
	                '1003697'
	            ),
	            '410', DECODE(
	                in_StrategicProfitCenterDescription,
	                'NSI', '1003703',
	                '1003697'
	            ),
	            '420', DECODE(
	                in_StrategicProfitCenterDescription,
	                'NSI', '1003703',
	                '1003697'
	            ),
	            '430', DECODE(
	                in_StrategicProfitCenterDescription,
	                'NSI', '1003703',
	                '1003697'
	            ),
	            '450', DECODE(
	                in_StrategicProfitCenterDescription,
	                'NSI', '1003703',
	                '1003697'
	            ),
	            ''
	        ),
	        DECODE(
	            in_PolicyOfferingCode,
	            '500', DECODE(
	                in_StrategicProfitCenterDescription,
	                'NSI', '1003657',
	                '1003655'
	            ),
	            '400', DECODE(
	                in_StrategicProfitCenterDescription,
	                'NSI', '1003656',
	                '1003654'
	            ),
	            '410', DECODE(
	                in_StrategicProfitCenterDescription,
	                'NSI', '1003656',
	                '1003654'
	            ),
	            '420', DECODE(
	                in_StrategicProfitCenterDescription,
	                'NSI', '1003656',
	                '1003654'
	            ),
	            '430', DECODE(
	                in_StrategicProfitCenterDescription,
	                'NSI', '1003656',
	                '1003654'
	            ),
	            '450', DECODE(
	                in_StrategicProfitCenterDescription,
	                'NSI', '1003656',
	                '1003654'
	            ),
	            ''
	        )
	    )
	) AS out_ContractNumber,
	in_pol_eff_date AS out_PolicyEffectiveDate,
	in_pol_exp_date AS out_PolicyExpirationDate,
	-- *INF*: IIF(ltrim(rtrim(in_name))='N/A', '', SUBSTR(ltrim(rtrim(in_name)),1,55))
	IFF(ltrim(rtrim(in_name)) = 'N/A', '', SUBSTR(ltrim(rtrim(in_name)), 1, 55)) AS out_nameOfInsured,
	-- *INF*: substr(CONCAT(
	--                      CONCAT(
	--                                            IIF(RTRIM(LTRIM(in_addr_line_1))='N/A', '', RTRIM(LTRIM(in_addr_line_1))),
	--                                            IIF(RTRIM(LTRIM(in_addr_line_2))='N/A', '', RTRIM(LTRIM(in_addr_line_2)))),
	--                      IIF(RTRIM(LTRIM(in_addr_line_3))='N/A', '', RTRIM(LTRIM(in_addr_line_3))))
	-- ,1,55)
	substr(CONCAT(CONCAT(
	            IFF(
	                RTRIM(LTRIM(in_addr_line_1)) = 'N/A', '',
	                RTRIM(LTRIM(in_addr_line_1))
	            ), 
	            IFF(
	                RTRIM(LTRIM(in_addr_line_2)) = 'N/A', '',
	                RTRIM(LTRIM(in_addr_line_2))
	            )), 
	        IFF(
	            RTRIM(LTRIM(in_addr_line_3)) = 'N/A', '', RTRIM(LTRIM(in_addr_line_3))
	        )), 1, 55) AS out_MailingAddressStreetName,
	-- *INF*: IIF(RTRIM(LTRIM(in_city_name))='N/A', '', RTRIM(LTRIM(in_city_name)))
	IFF(RTRIM(LTRIM(in_city_name)) = 'N/A', '', RTRIM(LTRIM(in_city_name))) AS out_MailingAddressCity,
	-- *INF*: IIF(RTRIM(LTRIM(in_state_prov_code))='N/A', '', RTRIM(LTRIM(in_state_prov_code)))
	IFF(RTRIM(LTRIM(in_state_prov_code)) = 'N/A', '', RTRIM(LTRIM(in_state_prov_code))) AS out_MailingAddressState,
	-- *INF*: IIF(RTRIM(LTRIM(in_zip_postal_code))='N/A', '', RTRIM(LTRIM(in_zip_postal_code)))
	-- 
	IFF(RTRIM(LTRIM(in_zip_postal_code)) = 'N/A', '', RTRIM(LTRIM(in_zip_postal_code))) AS out_MailingAddressZipCode,
	0 AS out_TotalPackageGrossPremium,
	0 AS out_TotalPropertyGrossPremium,
	in_FirstPartyLimit AS out_FirstPartyLimit,
	in_FirstPartyDeductible AS out_FirstPartyDeductibleAmount,
	in_ThirdPartyDeductible AS out_ThirdPartyDeductibleAmount,
	-- *INF*: IIF(RTRIM(LTRIM(in_prim_bus_class_code))='N/A' ,
	-- RTRIM(LTRIM(in_ClassCode)),
	--  RTRIM(LTRIM(in_prim_bus_class_code)))
	IFF(
	    RTRIM(LTRIM(in_prim_bus_class_code)) = 'N/A', RTRIM(LTRIM(in_ClassCode)),
	    RTRIM(LTRIM(in_prim_bus_class_code))
	) AS out_OccupancyCode,
	-- *INF*: IIF(RTRIM(LTRIM(in_prior_pol_key))='N/A', '', RTRIM(LTRIM(in_prior_pol_key)))
	IFF(RTRIM(LTRIM(in_prior_pol_key)) = 'N/A', '', RTRIM(LTRIM(in_prior_pol_key))) AS out_PreviousPolicyNumber,
	-- *INF*: IIF(ISNULL(in_AgencyCode) OR in_AgencyCode='N/A', '', in_AgencyCode)
	IFF(in_AgencyCode IS NULL OR in_AgencyCode = 'N/A', '', in_AgencyCode) AS out_AgencyCode,
	-- *INF*: DECODE (TRUE,
	-- ISNULL(in_StrategicProfitCenterAbbreviation), 'Other',
	-- LTRIM(RTRIM(in_StrategicProfitCenterAbbreviation)) = 'NSI',:LKP.LKP_PROGRAMCODEFORNSI(in_ProgramAKId),
	-- in_StrategicProfitCenterAbbreviation)
	-- 
	DECODE(
	    TRUE,
	    in_StrategicProfitCenterAbbreviation IS NULL, 'Other',
	    LTRIM(RTRIM(in_StrategicProfitCenterAbbreviation)) = 'NSI', LKP_PROGRAMCODEFORNSI_in_ProgramAKId.ProgramDescription,
	    in_StrategicProfitCenterAbbreviation
	) AS out_BranchCode,
	-- *INF*: DECODE(TRUE,
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
	-- *INF*: iif(in_TransactionCode='Cancel',in_PolicyCoverageEffectiveDate,
	-- 	iif (isnull(lkp_PremiumTransactionEffectiveDate),
	-- 	in_PremiumMasterCoverageEffectiveDate,lkp_PremiumTransactionEffectiveDate))
	IFF(
	    in_TransactionCode = 'Cancel', in_PolicyCoverageEffectiveDate,
	    IFF(
	        lkp_PremiumTransactionEffectiveDate IS NULL, in_PremiumMasterCoverageEffectiveDate,
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
	-- '500', '035',
	-- '400', '009',
	-- '410', '009',
	-- '420', '009',
	-- '430', '009',
	-- '450', '009',
	-- ''
	-- )
	-- 
	DECODE(
	    in_PolicyOfferingCode,
	    '500', '035',
	    '400', '009',
	    '410', '009',
	    '420', '009',
	    '430', '009',
	    '450', '009',
	    ''
	) AS out_ProgramID,
	-- *INF*: TRUNC(@{pipeline().parameters.EXTRACTDATE},'D')
	CAST(TRUNC(@{pipeline().parameters.EXTRACTDATE}, 'DAY') AS TIMESTAMP_NTZ(0)) AS out_ExtractDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS out_AuditID,
	-- *INF*: DECODE(TRUE , 
	--  LTRIM(RTRIM(in_FirstPartyLimit))='50000','L',
	--  LTRIM(RTRIM(in_FirstPartyLimit))='100000','F',
	--  LTRIM(RTRIM(in_FirstPartyLimit))='250000','F',
	-- LTRIM(RTRIM(in_FirstPartyLimit))='500000','F',
	-- LTRIM(RTRIM(in_FirstPartyLimit))='1000000','F',
	-- 'N'
	-- )
	-- 
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(in_FirstPartyLimit)) = '50000', 'L',
	    LTRIM(RTRIM(in_FirstPartyLimit)) = '100000', 'F',
	    LTRIM(RTRIM(in_FirstPartyLimit)) = '250000', 'F',
	    LTRIM(RTRIM(in_FirstPartyLimit)) = '500000', 'F',
	    LTRIM(RTRIM(in_FirstPartyLimit)) = '1000000', 'F',
	    'N'
	) AS FirstPartyCoverage,
	-- *INF*: DECODE(TRUE , 
	-- LTRIM(RTRIM(in_ThirdPartyLimit))='50000','L',
	-- LTRIM(RTRIM(in_ThirdPartyLimit))='10000','F',
	-- LTRIM(RTRIM(in_ThirdPartyLimit))='100000','F',
	-- LTRIM(RTRIM(in_ThirdPartyLimit))='250000','F',
	-- LTRIM(RTRIM(in_ThirdPartyLimit))='500000','F',
	-- LTRIM(RTRIM(in_ThirdPartyLimit))='1000000','F',
	-- 'N'
	-- )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(in_ThirdPartyLimit)) = '50000', 'L',
	    LTRIM(RTRIM(in_ThirdPartyLimit)) = '10000', 'F',
	    LTRIM(RTRIM(in_ThirdPartyLimit)) = '100000', 'F',
	    LTRIM(RTRIM(in_ThirdPartyLimit)) = '250000', 'F',
	    LTRIM(RTRIM(in_ThirdPartyLimit)) = '500000', 'F',
	    LTRIM(RTRIM(in_ThirdPartyLimit)) = '1000000', 'F',
	    'N'
	) AS ThirdPartyCoverage,
	LKP_PolicyCurrentStatusDim.PolicyCancellationDate,
	SQ_WorkHSBCyber.CoverageType,
	SQ_WorkHSBCyber.PremiumTransactionEnteredDate
	FROM SQ_WorkHSBCyber
	LEFT JOIN LKP_PolicyCurrentStatusDim
	ON LKP_PolicyCurrentStatusDim.EDWPolicyAKId = SQ_WorkHSBCyber.PolicyAKID
	LEFT JOIN LKP_PolicyOffering
	ON LKP_PolicyOffering.PolicyOfferingAKId = SQ_WorkHSBCyber.PolicyOfferingAKId
	LEFT JOIN LKP_PremiumTransaction
	ON LKP_PremiumTransaction.PolicyAKID = SQ_WorkHSBCyber.PolicyAKID
	LEFT JOIN LKP_StrategicProfitCenter
	ON LKP_StrategicProfitCenter.StrategicProfitCenterAKId = SQ_WorkHSBCyber.StrategicProfitCenterAKId
	LEFT JOIN LKP_WorkHSBCyber
	ON LKP_WorkHSBCyber.PremiumMasterCalculationId = SQ_WorkHSBCyber.PremiumMasterCalculationID
	LEFT JOIN LKP_PROGRAMCODEFORNSI LKP_PROGRAMCODEFORNSI_in_ProgramAKId
	ON LKP_PROGRAMCODEFORNSI_in_ProgramAKId.ProgramAKId = in_ProgramAKId

),
FIL_WorkHSBCyber AS (
	SELECT
	lkp_WorkHSBCyberID AS WorkHSBCyberID, 
	out_CreatedDate AS CreatedDate, 
	out_ModifiedDate AS ModifiedDate, 
	out_pol_id AS PolicyID, 
	out_RiskLocationID AS RiskLocationID, 
	out_PolicyCoverageID AS PolicyCoverageID, 
	out_RatingCoverageID AS RatingCoverageID, 
	out_PremiumTransactionID AS PremiumTransactionID, 
	out_PremiumMasterCalculationID AS PremiumMasterCalculationID, 
	out_RunDate AS RunDate, 
	out_contract_cust_id AS ContractCustomerID, 
	out_contract_cust_addr_id AS ContractCustomerAddressID, 
	out_AgencyID AS AgencyID, 
	out_PolKey AS PolicyKey, 
	out_Company AS Company, 
	out_Productcode AS ProductCode, 
	out_ContractNumber AS ContractNumber, 
	out_PolicyEffectiveDate AS PolicyEffectiveDate, 
	out_PolicyExpirationDate AS PolicyExpirationDate, 
	out_nameOfInsured AS InsuredName, 
	out_MailingAddressStreetName AS MailingAddressStreetName, 
	out_MailingAddressCity AS MailingAddressCityName, 
	out_MailingAddressState AS MailingAddressStateAbbreviation, 
	out_MailingAddressZipCode AS MailingAddressZipCode, 
	out_TotalPackageGrossPremium AS TotalPackageGrossPremium, 
	out_TotalPropertyGrossPremium AS TotalPropertyGrossPremium, 
	out_FirstPartyLimit AS FirstPartyLimit, 
	out_FirstPartyDeductibleAmount AS FirstPartyDeductible, 
	out_OccupancyCode AS OccupancyCode, 
	out_PreviousPolicyNumber AS PreviousPolicyNumber, 
	out_AgencyCode AS AgencyCode, 
	out_BranchCode AS BranchCode, 
	CoverageType, 
	out_TransactionCode AS PremiumTransactionCode, 
	out_TransactionEffectiveDate AS PremiumTransactionEffectiveDate, 
	out_CoverageEffectiveDate AS CoverageEffectiveDate, 
	out_CoverageExpirationDate AS CoverageExpirationDate, 
	out_CoverageGrossPremium AS CyberCoverageGrossPremium, 
	out_CoverageNetPremium AS CyberCoverageNetPremium, 
	out_ProgramID AS ProgramCode, 
	out_AuditID AS AuditID, 
	PolicyCancellationDate, 
	in_ThirdPartyLimit AS ThirdPartyLimit, 
	out_ThirdPartyDeductibleAmount AS ThirdPartyDeductible, 
	ExtortionLimit AS ExtortionSublimit, 
	FirstPartyCoverage, 
	ThirdPartyCoverage, 
	PremiumTransactionEnteredDate
	FROM EXP_GetValues
	WHERE ContractNumber != '' and ProgramCode  != ''  and isnull(WorkHSBCyberID)
),
WorkHSBCyber AS (
	INSERT INTO WorkHSBCyber
	(AuditId, CreatedDate, ModifiedDate, PolicyId, RiskLocationId, PolicyCoverageId, RatingCoverageId, PremiumTransactionId, PremiumMasterCalculationId, ContractCustomerId, ContractCustomerAddressId, AgencyId, PolicyKey, RunDate, Company, ProductCode, ContractNumber, PolicyEffectiveDate, PolicyExpirationDate, InsuredName, MailingAddressStreetName, MailingAddressCityName, MailingAddressStateAbbreviation, MailingAddressZipCode, TotalPackageGrossPremium, TotalPropertyGrossPremium, FirstPartyLimit, FirstPartyDeductible, OccupancyCode, PreviousPolicyNumber, AgencyCode, BranchCode, CoverageType, PremiumTransactionCode, PremiumTransactionEffectiveDate, CoverageEffectiveDate, CoverageExpirationDate, CyberCoverageGrossPremium, CyberCoverageNetPremium, ProgramCode, PolicyCancellationDate, FirstPartyCoverage, ThirdPartyLimit, ThirdPartyDeductible, ThirdPartyCoverage, ExtortionSublimit, PremiumTransactionEnteredDate)
	SELECT 
	AuditID AS AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	PolicyID AS POLICYID, 
	RiskLocationID AS RISKLOCATIONID, 
	PolicyCoverageID AS POLICYCOVERAGEID, 
	RatingCoverageID AS RATINGCOVERAGEID, 
	PremiumTransactionID AS PREMIUMTRANSACTIONID, 
	PremiumMasterCalculationID AS PREMIUMMASTERCALCULATIONID, 
	ContractCustomerID AS CONTRACTCUSTOMERID, 
	ContractCustomerAddressID AS CONTRACTCUSTOMERADDRESSID, 
	AgencyID AS AGENCYID, 
	POLICYKEY, 
	RUNDATE, 
	COMPANY, 
	PRODUCTCODE, 
	CONTRACTNUMBER, 
	POLICYEFFECTIVEDATE, 
	POLICYEXPIRATIONDATE, 
	INSUREDNAME, 
	MAILINGADDRESSSTREETNAME, 
	MAILINGADDRESSCITYNAME, 
	MAILINGADDRESSSTATEABBREVIATION, 
	MAILINGADDRESSZIPCODE, 
	TOTALPACKAGEGROSSPREMIUM, 
	TOTALPROPERTYGROSSPREMIUM, 
	FIRSTPARTYLIMIT, 
	FIRSTPARTYDEDUCTIBLE, 
	OCCUPANCYCODE, 
	PREVIOUSPOLICYNUMBER, 
	AGENCYCODE, 
	BRANCHCODE, 
	COVERAGETYPE, 
	PREMIUMTRANSACTIONCODE, 
	PREMIUMTRANSACTIONEFFECTIVEDATE, 
	COVERAGEEFFECTIVEDATE, 
	COVERAGEEXPIRATIONDATE, 
	CYBERCOVERAGEGROSSPREMIUM, 
	CYBERCOVERAGENETPREMIUM, 
	PROGRAMCODE, 
	POLICYCANCELLATIONDATE, 
	FIRSTPARTYCOVERAGE, 
	THIRDPARTYLIMIT, 
	THIRDPARTYDEDUCTIBLE, 
	THIRDPARTYCOVERAGE, 
	EXTORTIONSUBLIMIT, 
	PREMIUMTRANSACTIONENTEREDDATE
	FROM FIL_WorkHSBCyber
),
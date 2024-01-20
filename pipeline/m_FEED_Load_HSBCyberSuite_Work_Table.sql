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
SQ_WorkHSBCyberSuite AS (
	Declare @STARTDATE as datetime,
	@ENDDATE as datetime
	set @STARTDATE=@{pipeline().parameters.STARTDATE}
	set @ENDDATE=@{pipeline().parameters.ENDDATE}
	SELECT 
	PremiumTransaction.SourceSystemID,
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
	CyberSuiteLimit.CoverageLimitValue as CyberSuiteFirstPartyLimit,
	CyberSuiteDeductible.CoverageDeductibleValue as CyberSuiteFirstPartyDeductible,
	PremiumTransaction.PremiumTransactionCode,
	PremiumTransaction.OffsetOnsetCode,
	policy.prim_bus_class_code,
	RatingCoverage.ClassCode,
	policy.prior_pol_key, 
	Agency.AgencyCode, 
	PremiumMasterCalculation.PremiumMasterCoverageEffectiveDate, 
	PremiumMasterCalculation.PremiumMasterCoverageExpirationDate,
	PremiumMasterCalculation.PremiumMasterPremium ,
	PremiumTransaction.PremiumTransactionAKID,
	PremiumMasterCalculation.premiummasterfulltermpremium,
	PremiumMasterCalculation.PremiumMasterRunDate,
	PolicyCoverage.PolicyAKID,
	PolicyCoverage.PolicyCoverageEffectiveDate,
	RatingCoverage.RatingCoverageCancellationDate,
	policy.ProgramAKId,
	RatingCoverage.CoverageType,
	PremiumTransaction.PremiumTransactionEnteredDate,
	csd.RatingTier,
	csd.CyberSuiteEligibilityQuestionOne,
	csd.CyberSuiteEligibilityQuestionTwo,
	csd.CyberSuiteEligibilityQuestionThree,
	csd.CyberSuiteEligibilityQuestionFour,
	csd.CyberSuiteEligibilityQuestionFive,
	csd.CyberSuiteEligibilityQuestionSix
	FROM
	V2.policy 
	inner join dbo.PolicyCoverage on PolicyCoverage.PolicyAKID=policy.pol_ak_id and PolicyCoverage.CurrentSnapshotFlag=1 and policy.crrnt_snpsht_flag=1
	inner join dbo.contract_customer on policy.contract_cust_ak_id=contract_customer.contract_cust_ak_id and contract_customer.crrnt_snpsht_flag=1
	inner join dbo.contract_customer_address on contract_customer_address.contract_cust_ak_id=contract_customer.contract_cust_ak_id and contract_customer_address.crrnt_snpsht_flag=1
	inner join V2.Agency on policy.AgencyAKId=Agency.AgencyAKID and Agency.CurrentSnapshotFlag=1
	inner join dbo.RiskLocation on PolicyCoverage.RiskLocationAKID= RiskLocation.RiskLocationAKID and RiskLocation.CurrentSnapshotFlag=1
	inner join dbo.RatingCoverage on RatingCoverage.PolicyCoverageAKID=PolicyCoverage.PolicyCoverageAKID  and PolicyCoverage.CurrentSnapshotFlag=1
	left  join dbo.PremiumTransaction on PremiumTransaction.RatingCoverageAKId=RatingCoverage.RatingCoverageAKID and PremiumTransaction.EffectiveDate=RatingCoverage.EffectiveDate and PremiumTransaction.CurrentSnapshotFlag=1
	left  join dbo.PremiumMasterCalculation on  PremiumMasterCalculation.PremiumTransactionAKID=PremiumTransaction.PremiumTransactionAKID and  PremiumMasterCalculation.CurrentSnapshotFlag =1
	--------------------------------
	left join (select CoverageLimitBridge.PremiumTransactionAKID , CoverageLimit.CoverageLimitType,  CoverageLimit.CoverageLimitValue
	from dbo.CoverageLimitBridge inner join dbo.CoverageLimit on CoverageLimit.CoverageLimitId=CoverageLimitBridge.CoverageLimitId  ) as  CyberSuiteLimit
	 on CyberSuiteLimit.PremiumTransactionAKID= PremiumTransaction.PremiumTransactionAKID 
	and CyberSuiteLimit.CoverageLimitType = 'FirstPartyAnnualAggregate'
	 and CyberSuiteLimit.CoverageLimitValue<>0
	--------------------------------
	left join (select CoveragedeductibleBridge.PremiumTransactionAKID , CoverageDeductible.CoverageDeductibleType,  CoverageDeductible.CoverageDeductibleValue
	from dbo.CoveragedeductibleBridge inner join dbo.CoverageDeductible on CoverageDeductible.CoverageDeductibleId=CoverageDeductibleBridge.CoverageDeductibleId  ) as  CyberSuiteDeductible
	 on CyberSuiteDeductible.PremiumTransactionAKID= PremiumTransaction.PremiumTransactionAKID 
	and CyberSuiteDeductible.CoverageDeductibleType ='FirstPartyAnnualAggregate'
	 and CyberSuiteDeductible.CoverageDeductibleValue<>0
	 left join CyberSuiteDetail csd
	 on csd.PremiumTransactionID = PremiumTransaction.PremiumTransactionID
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
	and RatingCoverage.CoverageType in ('CyberSuite')
	order by pol_num,pol_mod
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
		and rc.CoverageType  in  ('CyberSuite','CyberSuiteExtendedReporting')
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.policycoverage pc
		on (pc.PolicyCoverageAKID = rc.PolicyCoverageAKID
		and pc.CurrentSnapshotFlag = 1)
		where not exists (select * from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction pt1 
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage rc1 
		on rc1.RatingCoverageAKID = pt1.RatingCoverageAKId
		and rc1.EffectiveDate = pt1.EffectiveDate
		and rc1.RatingCoverageCancellationDate > '2100-12-31'
		and rc1.CoverageType  in  ('CyberSuite','CyberSuiteExtendedReporting')
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
		and rc1.CoverageType  in  ('CyberSuite','CyberSuiteExtendedReporting')
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
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StrategicProfitCenterAKId ORDER BY CurrentSnapshotFlag DESC) = 1
),
LKP_WorkHSBCyberSuite AS (
	SELECT
	WorkHSBCyberSuiteId,
	PremiumMasterCalculationId,
	in_PremiumMasterCalculationId
	FROM (
		SELECT 
			WorkHSBCyberSuiteId,
			PremiumMasterCalculationId,
			in_PremiumMasterCalculationId
		FROM WorkHSBCyberSuite
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumMasterCalculationId ORDER BY WorkHSBCyberSuiteId DESC) = 1
),
EXP_GetValues AS (
	SELECT
	LKP_WorkHSBCyberSuite.WorkHSBCyberSuiteId AS lkp_WorkHSBCyberSuiteID,
	LKP_PremiumTransaction.PremiumTransactionEffectiveDate AS lkp_PremiumTransactionEffectiveDate,
	SQ_WorkHSBCyberSuite.SourceSystemID AS in_SourceSystemID,
	SQ_WorkHSBCyberSuite.pol_id AS in_pol_id,
	SQ_WorkHSBCyberSuite.RiskLocationID AS in_RiskLocationID,
	SQ_WorkHSBCyberSuite.PolicyCoverageID AS in_PolicyCoverageID,
	SQ_WorkHSBCyberSuite.RatingCoverageId AS in_RatingCoverageId,
	SQ_WorkHSBCyberSuite.PremiumTransactionID AS in_PremiumTransactionID,
	SQ_WorkHSBCyberSuite.PremiumMasterCalculationID AS in_PremiumMasterCalculationID,
	SQ_WorkHSBCyberSuite.contract_cust_id AS in_contract_cust_id,
	SQ_WorkHSBCyberSuite.contract_cust_addr_id AS in_contract_cust_addr_id,
	SQ_WorkHSBCyberSuite.AgencyID AS in_AgencyID,
	SQ_WorkHSBCyberSuite.pol_sym AS in_pol_sym,
	SQ_WorkHSBCyberSuite.pol_num AS in_pol_num,
	SQ_WorkHSBCyberSuite.pol_mod AS in_pol_mod,
	SQ_WorkHSBCyberSuite.PolicyOfferingAKId AS in_PolicyOfferingAKId,
	SQ_WorkHSBCyberSuite.StrategicProfitCenterAKId AS in_StrategicProfitCenterAKId,
	SQ_WorkHSBCyberSuite.pol_eff_date AS in_pol_eff_date,
	SQ_WorkHSBCyberSuite.pol_exp_date AS in_pol_exp_date,
	SQ_WorkHSBCyberSuite.name AS in_name,
	SQ_WorkHSBCyberSuite.addr_line_1 AS in_addr_line_1,
	SQ_WorkHSBCyberSuite.addr_line_2 AS in_addr_line_2,
	SQ_WorkHSBCyberSuite.addr_line_3 AS in_addr_line_3,
	SQ_WorkHSBCyberSuite.city_name AS in_city_name,
	SQ_WorkHSBCyberSuite.state_prov_code AS in_state_prov_code,
	SQ_WorkHSBCyberSuite.zip_postal_code AS in_zip_postal_code,
	SQ_WorkHSBCyberSuite.FirstPartyLimit AS in_Limit,
	-- *INF*: TO_INTEGER(in_Limit)
	CAST(in_Limit AS INTEGER) AS v_LimitAmount,
	SQ_WorkHSBCyberSuite.FirstPartyDeductible AS in_Deductible,
	SQ_WorkHSBCyberSuite.TransactionCode AS in_TransactionCode,
	SQ_WorkHSBCyberSuite.prim_bus_class_code AS in_prim_bus_class_code,
	SQ_WorkHSBCyberSuite.ClassCode AS in_ClassCode,
	SQ_WorkHSBCyberSuite.prior_pol_key AS in_prior_pol_key,
	SQ_WorkHSBCyberSuite.AgencyCode AS in_AgencyCode,
	SQ_WorkHSBCyberSuite.PremiumMasterCoverageEffectiveDate AS in_PremiumMasterCoverageEffectiveDate,
	SQ_WorkHSBCyberSuite.PremiumMasterCoverageExpirationDate AS in_PremiumMasterCoverageExpirationDate,
	SQ_WorkHSBCyberSuite.PremiumMasterPremium AS in_PremiumMasterPremium,
	LKP_PolicyOffering.PolicyOfferingCode AS in_PolicyOfferingCode,
	LKP_StrategicProfitCenter.StrategicProfitCenterCode AS in_StrategicProfitCenterCode,
	LKP_StrategicProfitCenter.StrategicProfitCenterAbbreviation AS in_StrategicProfitCenterAbbreviation,
	LKP_StrategicProfitCenter.StrategicProfitCenterDescription AS in_StrategicProfitCenterDescription,
	SQ_WorkHSBCyberSuite.PremiumTransactionAKID AS in_PremiumTransactionAKID,
	SQ_WorkHSBCyberSuite.premiummasterfulltermpremium AS in_premiummasterfulltermpremium,
	SQ_WorkHSBCyberSuite.PremiumMasterRunDate AS in_PremiumMasterRunDate,
	SQ_WorkHSBCyberSuite.PolicyCoverageEffectiveDate AS in_PolicyCoverageEffectiveDate,
	SQ_WorkHSBCyberSuite.RatingCoverageCancellationDate AS in_RatingCoverageCancellationDate,
	SQ_WorkHSBCyberSuite.ProgramAKId AS in_ProgramAKId,
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
	'4744' AS out_Company,
	'CBS' AS out_Productcode,
	-- *INF*: IIF(
	-- v_LimitAmount  >  1000000,
	-- DECODE(
	-- in_PolicyOfferingCode, 
	-- '500', DECODE(in_StrategicProfitCenterDescription,'NSI', '1004577', '1004575'),
	-- '400', DECODE(in_StrategicProfitCenterDescription,'NSI', '1004578', '1004576'),
	-- '410', DECODE(in_StrategicProfitCenterDescription,'NSI', '1004578', '1004576'),
	-- '420', DECODE(in_StrategicProfitCenterDescription,'NSI', '1004578', '1004576'),
	-- '430', DECODE(in_StrategicProfitCenterDescription,'NSI', '1004578', '1004576'),
	-- '450', DECODE(in_StrategicProfitCenterDescription,'NSI', '1004578', '1004576')
	-- ),
	-- DECODE(
	-- in_PolicyOfferingCode, 
	-- '500', DECODE(in_StrategicProfitCenterDescription,'NSI', '1004482', '1004477'),
	-- '400', DECODE(in_StrategicProfitCenterDescription,'NSI', '1004547', '1004548'),
	-- '410', DECODE(in_StrategicProfitCenterDescription,'NSI', '1004547', '1004548'),
	-- '420', DECODE(in_StrategicProfitCenterDescription,'NSI', '1004547', '1004548'),
	-- '430', DECODE(in_StrategicProfitCenterDescription,'NSI', '1004547', '1004548'),
	-- '450', DECODE(in_StrategicProfitCenterDescription,'NSI', '1004547', '1004548')
	-- )
	-- )
	-- --contract numbers for NSI/WB for BOP/CPP determined by referral amount (limit)
	IFF(
	    v_LimitAmount > 1000000,
	    DECODE(
	        in_PolicyOfferingCode,
	        '500', DECODE(
	            in_StrategicProfitCenterDescription,
	            'NSI', '1004577',
	            '1004575'
	        ),
	        '400', DECODE(
	            in_StrategicProfitCenterDescription,
	            'NSI', '1004578',
	            '1004576'
	        ),
	        '410', DECODE(
	            in_StrategicProfitCenterDescription,
	            'NSI', '1004578',
	            '1004576'
	        ),
	        '420', DECODE(
	            in_StrategicProfitCenterDescription,
	            'NSI', '1004578',
	            '1004576'
	        ),
	        '430', DECODE(
	            in_StrategicProfitCenterDescription,
	            'NSI', '1004578',
	            '1004576'
	        ),
	        '450', DECODE(
	            in_StrategicProfitCenterDescription,
	            'NSI', '1004578',
	            '1004576'
	        )
	    ),
	    DECODE(
	        in_PolicyOfferingCode,
	        '500', DECODE(
	            in_StrategicProfitCenterDescription,
	            'NSI', '1004482',
	            '1004477'
	        ),
	        '400', DECODE(
	            in_StrategicProfitCenterDescription,
	            'NSI', '1004547',
	            '1004548'
	        ),
	        '410', DECODE(
	            in_StrategicProfitCenterDescription,
	            'NSI', '1004547',
	            '1004548'
	        ),
	        '420', DECODE(
	            in_StrategicProfitCenterDescription,
	            'NSI', '1004547',
	            '1004548'
	        ),
	        '430', DECODE(
	            in_StrategicProfitCenterDescription,
	            'NSI', '1004547',
	            '1004548'
	        ),
	        '450', DECODE(
	            in_StrategicProfitCenterDescription,
	            'NSI', '1004547',
	            '1004548'
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
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL(in_Limit), in_Limit,
	-- '0')
	DECODE(
	    TRUE,
	    in_Limit IS NOT NULL, in_Limit,
	    '0'
	) AS out_Limit,
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL(in_Deductible), in_Deductible,
	-- '0')
	DECODE(
	    TRUE,
	    in_Deductible IS NOT NULL, in_Deductible,
	    '0'
	) AS out_DeductibleAmount,
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
	SQ_WorkHSBCyberSuite.OffsetOnsetCode,
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
	-- '500', '040',
	-- '400', '039',
	-- '410', '039',
	-- '420', '039',
	-- '430', '039',
	-- '450', '039',
	-- ''
	-- )
	-- --BOP is 039, CPP is 040
	-- 
	DECODE(
	    in_PolicyOfferingCode,
	    '500', '040',
	    '400', '039',
	    '410', '039',
	    '420', '039',
	    '430', '039',
	    '450', '039',
	    ''
	) AS out_ProgramID,
	-- *INF*: TRUNC(@{pipeline().parameters.EXTRACTDATE},'D')
	CAST(TRUNC(@{pipeline().parameters.EXTRACTDATE}, 'DAY') AS TIMESTAMP_NTZ(0)) AS out_ExtractDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS out_AuditID,
	LKP_PolicyCurrentStatusDim.PolicyCancellationDate,
	SQ_WorkHSBCyberSuite.CoverageType,
	SQ_WorkHSBCyberSuite.PremiumTransactionEnteredDate,
	'C' AS LimitType,
	SQ_WorkHSBCyberSuite.PricingTier,
	SQ_WorkHSBCyberSuite.CyberSuiteEligibilityQuestionOne,
	-- *INF*: DECODE(TRUE,
	-- CyberSuiteEligibilityQuestionOne = '1','Y',
	-- CyberSuiteEligibilityQuestionOne = '0','N',
	-- '')
	DECODE(
	    TRUE,
	    CyberSuiteEligibilityQuestionOne = '1', 'Y',
	    CyberSuiteEligibilityQuestionOne = '0', 'N',
	    ''
	) AS o_CyberSuiteEligibilityQuestionOne,
	SQ_WorkHSBCyberSuite.CyberSuiteEligibilityQuestionTwo,
	-- *INF*: DECODE(TRUE,
	-- CyberSuiteEligibilityQuestionTwo = '1','Y',
	-- CyberSuiteEligibilityQuestionTwo = '0','N',
	-- '')
	DECODE(
	    TRUE,
	    CyberSuiteEligibilityQuestionTwo = '1', 'Y',
	    CyberSuiteEligibilityQuestionTwo = '0', 'N',
	    ''
	) AS o_CyberSuiteEligibilityQuestionTwo,
	SQ_WorkHSBCyberSuite.CyberSuiteEligibilityQuestionThree,
	-- *INF*: DECODE(TRUE,
	-- CyberSuiteEligibilityQuestionThree = '1','Y',
	-- CyberSuiteEligibilityQuestionThree = '0','N',
	-- '')
	DECODE(
	    TRUE,
	    CyberSuiteEligibilityQuestionThree = '1', 'Y',
	    CyberSuiteEligibilityQuestionThree = '0', 'N',
	    ''
	) AS o_CyberSuiteEligibilityQuestionThree,
	SQ_WorkHSBCyberSuite.CyberSuiteEligibilityQuestionFour,
	-- *INF*: DECODE(TRUE,
	-- CyberSuiteEligibilityQuestionFour = '1','Y',
	-- CyberSuiteEligibilityQuestionFour = '0','N',
	-- '')
	DECODE(
	    TRUE,
	    CyberSuiteEligibilityQuestionFour = '1', 'Y',
	    CyberSuiteEligibilityQuestionFour = '0', 'N',
	    ''
	) AS o_CyberSuiteEligibilityQuestionFour,
	SQ_WorkHSBCyberSuite.CyberSuiteEligibilityQuestionFive,
	-- *INF*: DECODE(TRUE,
	-- CyberSuiteEligibilityQuestionFive = '1','Y',
	-- CyberSuiteEligibilityQuestionFive = '0','N',
	-- '')
	DECODE(
	    TRUE,
	    CyberSuiteEligibilityQuestionFive = '1', 'Y',
	    CyberSuiteEligibilityQuestionFive = '0', 'N',
	    ''
	) AS o_CyberSuiteEligibilityQuestionFive,
	SQ_WorkHSBCyberSuite.CyberSuiteEligibilityQuestionSix,
	-- *INF*: DECODE(TRUE,
	-- CyberSuiteEligibilityQuestionSix = '1','Y',
	-- CyberSuiteEligibilityQuestionSix = '0','N',
	-- '')
	DECODE(
	    TRUE,
	    CyberSuiteEligibilityQuestionSix = '1', 'Y',
	    CyberSuiteEligibilityQuestionSix = '0', 'N',
	    ''
	) AS o_CyberSuiteEligibilityQuestionSix
	FROM SQ_WorkHSBCyberSuite
	LEFT JOIN LKP_PolicyCurrentStatusDim
	ON LKP_PolicyCurrentStatusDim.EDWPolicyAKId = SQ_WorkHSBCyberSuite.PolicyAKID
	LEFT JOIN LKP_PolicyOffering
	ON LKP_PolicyOffering.PolicyOfferingAKId = SQ_WorkHSBCyberSuite.PolicyOfferingAKId
	LEFT JOIN LKP_PremiumTransaction
	ON LKP_PremiumTransaction.PolicyAKID = SQ_WorkHSBCyberSuite.PolicyAKID
	LEFT JOIN LKP_StrategicProfitCenter
	ON LKP_StrategicProfitCenter.StrategicProfitCenterAKId = SQ_WorkHSBCyberSuite.StrategicProfitCenterAKId
	LEFT JOIN LKP_WorkHSBCyberSuite
	ON LKP_WorkHSBCyberSuite.PremiumMasterCalculationId = SQ_WorkHSBCyberSuite.PremiumMasterCalculationID
	LEFT JOIN LKP_PROGRAMCODEFORNSI LKP_PROGRAMCODEFORNSI_in_ProgramAKId
	ON LKP_PROGRAMCODEFORNSI_in_ProgramAKId.ProgramAKId = in_ProgramAKId

),
FIL_WorkHSBCyberSuite AS (
	SELECT
	lkp_WorkHSBCyberSuiteID AS WorkHSBCyberID, 
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
	out_Limit AS FirstPartyLimit, 
	out_DeductibleAmount AS FirstPartyDeductible, 
	out_OccupancyCode AS OccupancyCode, 
	out_PreviousPolicyNumber AS PreviousPolicyNumber, 
	out_AgencyCode AS AgencyCode, 
	out_BranchCode AS BranchCode, 
	CoverageType, 
	out_TransactionCode AS PremiumTransactionCode, 
	OffsetOnsetCode, 
	out_TransactionEffectiveDate AS PremiumTransactionEffectiveDate, 
	out_CoverageEffectiveDate AS CoverageEffectiveDate, 
	out_CoverageExpirationDate AS CoverageExpirationDate, 
	out_CoverageGrossPremium AS CyberCoverageGrossPremium, 
	out_CoverageNetPremium AS CyberCoverageNetPremium, 
	out_ProgramID AS ProgramCode, 
	out_AuditID AS AuditID, 
	PolicyCancellationDate, 
	LimitType, 
	PricingTier, 
	o_CyberSuiteEligibilityQuestionOne AS CyberSuiteEligibilityQuestionOne, 
	o_CyberSuiteEligibilityQuestionTwo AS CyberSuiteEligibilityQuestionTwo, 
	o_CyberSuiteEligibilityQuestionThree AS CyberSuiteEligibilityQuestionThree, 
	o_CyberSuiteEligibilityQuestionFour AS CyberSuiteEligibilityQuestionFour, 
	o_CyberSuiteEligibilityQuestionFive AS CyberSuiteEligibilityQuestionFive, 
	o_CyberSuiteEligibilityQuestionSix AS CyberSuiteEligibilityQuestionSix, 
	PremiumTransactionEnteredDate
	FROM EXP_GetValues
	WHERE ContractNumber != '' and ProgramCode  != ''  and isnull(WorkHSBCyberID)
),
WorkHSBCyberSuite AS (
	INSERT INTO WorkHSBCyberSuite
	(AuditId, CreatedDate, ModifiedDate, PolicyId, RiskLocationId, PolicyCoverageId, RatingCoverageId, PremiumTransactionId, PremiumMasterCalculationId, ContractCustomerId, ContractCustomerAddressId, AgencyId, PolicyKey, RunDate, Company, ProductCode, ContractNumber, PolicyEffectiveDate, PolicyExpirationDate, InsuredName, MailingAddressStreetName, MailingAddressCityName, MailingAddressStateAbbreviation, MailingAddressZipCode, TotalPackageGrossPremium, TotalPropertyGrossPremium, Limit, Deductible, OccupancyCode, PreviousPolicyNumber, AgencyCode, BranchCode, CoverageType, PremiumTransactionCode, OffsetOnsetCode, PremiumTransactionEffectiveDate, CoverageEffectiveDate, CoverageExpirationDate, CyberSuiteCoverageGrossPremium, CyberSuiteCoverageNetPremium, ProgramCode, PolicyCancellationDate, LimitType, PricingTier, CyberSuiteEligibilityQuestionOne, CyberSuiteEligibilityQuestionTwo, CyberSuiteEligibilityQuestionThree, CyberSuiteEligibilityQuestionFour, CyberSuiteEligibilityQuestionFive, CyberSuiteEligibilityQuestionSix, PremiumTransactionEnteredDate)
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
	FirstPartyLimit AS LIMIT, 
	FirstPartyDeductible AS DEDUCTIBLE, 
	OCCUPANCYCODE, 
	PREVIOUSPOLICYNUMBER, 
	AGENCYCODE, 
	BRANCHCODE, 
	COVERAGETYPE, 
	PREMIUMTRANSACTIONCODE, 
	OFFSETONSETCODE, 
	PREMIUMTRANSACTIONEFFECTIVEDATE, 
	COVERAGEEFFECTIVEDATE, 
	COVERAGEEXPIRATIONDATE, 
	CyberCoverageGrossPremium AS CYBERSUITECOVERAGEGROSSPREMIUM, 
	CyberCoverageNetPremium AS CYBERSUITECOVERAGENETPREMIUM, 
	PROGRAMCODE, 
	POLICYCANCELLATIONDATE, 
	LIMITTYPE, 
	PRICINGTIER, 
	CYBERSUITEELIGIBILITYQUESTIONONE, 
	CYBERSUITEELIGIBILITYQUESTIONTWO, 
	CYBERSUITEELIGIBILITYQUESTIONTHREE, 
	CYBERSUITEELIGIBILITYQUESTIONFOUR, 
	CYBERSUITEELIGIBILITYQUESTIONFIVE, 
	CYBERSUITEELIGIBILITYQUESTIONSIX, 
	PREMIUMTRANSACTIONENTEREDDATE
	FROM FIL_WorkHSBCyberSuite
),
WITH
LKP_CoverageLimitNonBlanket AS (
	SELECT
	NonBlanketLimit,
	PremiumTransactionAKID
	FROM (
		SELECT PMC.PremiumTransactionAKID as PremiumTransactionAKID 
			,CLB.CreatedDate as CreatedDate
			,CL.CoverageLimitValue AS NonBlanketLimit
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation PMC WITH (NOLOCK)
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC WITH (NOLOCK) ON RC.RatingCoverageAKID = PMC.RatingCoverageAKID
			AND RC.EffectiveDate = PMC.RatingCoverageEffectiveDate
			AND PMC.CurrentSnapshotFlag = 1
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC WITH (NOLOCK) ON PC.PolicyCoverageAKID = RC.PolicyCoverageAKID
			AND PC.CurrentSnapshotFlag = 1
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge CLB WITH (NOLOCK) ON CLB.PremiumTransactionAKId = PMC.PremiumTransactionAKID
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit CL WITH (NOLOCK) ON CL.CoverageLimitID = CLB.CoverageLimitID
		WHERE PC.InsuranceLine = 'BusinessOwners'
			AND RC.CoverageType <> 'Blanket'
			AND DATEADD(QUARTER, 1 + DATEDIFF(QUARTER, 0, PMC.PremiumMasterRunDate), - 1) = DATEADD(QUARTER, 1 + DATEDIFF(QUARTER, 0, GETDATE()) + @{pipeline().parameters.NO_OF_QUARTERS}, - 1)
		ORDER BY PMC.PremiumTransactionAKID
			,CLB.CreatedDate DESC --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKID ORDER BY NonBlanketLimit) = 1
),
LKP_BLANKETCOVERAGELIMIT AS (
	SELECT
	BlanketLimit,
	PremiumTransactionAKID
	FROM (
		SELECT PMC.PremiumTransactionAKID as PremiumTransactionAKID,CONVERT(VARCHAR,SUM(CONVERT(NUMERIC,CL.CoverageLimitValue))) as BlanketLimit 
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation PMC WITH (NOLOCK)
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC WITH (NOLOCK) ON RC.RatingCoverageAKID = PMC.RatingCoverageAKID AND RC.EffectiveDate = PMC.RatingCoverageEffectiveDate
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC WITH (NOLOCK) ON PC.PolicyCoverageAKID = RC.PolicyCoverageAKID AND PC.CurrentSnapshotFlag = 1
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge CLB WITH (NOLOCK) ON CLB.PremiumTransactionAKId = PMC.PremiumTransactionAKID
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit CL WITH (NOLOCK) ON CL.CoverageLimitID = CLB.CoverageLimitID
		WHERE PC.InsuranceLine = 'BusinessOwners' AND RC.CoverageType = 'Blanket' AND ISNumeric(CL.CoverageLimitValue) = 1
		 AND DATEADD(QUARTER, 1 + DATEDIFF(QUARTER, 0, PMC.PremiumMasterRunDate), - 1) = DATEADD(QUARTER, 1 + DATEDIFF(QUARTER, 0, GETDATE()) + @{pipeline().parameters.NO_OF_QUARTERS}, - 1)
		GROUP BY PMC.PremiumTransactionAKID
		ORDER BY PMC.PremiumTransactionAKID--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKID ORDER BY BlanketLimit) = 1
),
LKP_Update_ISOFireProtectionCode AS (
	SELECT
	IsoFireProtectionCode,
	PolicyKey
	FROM (
		SELECT PolicyKey AS PolicyKey, 
		ISOFireProtectionCode AS ISOFireProtectionCode
		FROM (
			SELECT DISTINCT POL.pol_key AS PolicyKey, 
			RL.LocationUnitNumber AS LocationNumber, 
			RC.SubLocationUnitNumber AS BuildingNumber, 
			CDCP.ISOFireProtectionCode AS ISOFireProtectionCode,
			(CASE 
				WHEN RC.SubLocationUnitNumber <> '000'
				THEN 
				ROW_NUMBER() OVER (PARTITION BY POL.pol_key
				ORDER BY RL.LocationUnitNumber, RC.SubLocationUnitNumber)
				ELSE 0
			END) 
			AS RowNumber
			FROM PremiumTransaction AS PT WITH (NOLOCK)
			INNER JOIN  RatingCoverage AS RC WITH (NOLOCK)
			ON PT.RatingCoverageAKID = RC.RatingCoverageAKID
			AND RC.EffectiveDate = PT.EffectiveDate 
			INNER JOIN PolicyCoverage AS PC WITH (NOLOCK)
			ON PC.PolicyCoverageAKID = RC.PolicyCoverageAKID
			AND PC.CurrentSnapshotFlag=1
			INNER JOIN RiskLocation AS RL WITH (NOLOCK)
			ON PC.RiskLocationAKID = RL.RiskLocationAKID
			AND RL.CurrentSnapshotFlag = 1
			INNER JOIN V2.policy AS POL WITH (NOLOCK)
			ON POL.pol_ak_id = PC.PolicyAKID
			AND POL.crrnt_snpsht_flag = 1
			INNER JOIN PremiumMasterCalculation AS PMC WITH (NOLOCK)
			ON PMC.PremiumTransactionAKID = PT.PremiumTransactionAKID
			AND PMC.CurrentSnapshotFlag = 1
			INNER JOIN CoverageDetailCommercialProperty AS CDCP WITH (NOLOCK)
			ON CDCP.PremiumTransactionID = PT.PremiumTransactionID
			AND CDCP.CurrentSnapshotFlag = 1
			WHERE RC.SubLocationUnitNumber <> '000'
			AND LEN(POL.pol_key) <> 12
			AND CDCP.ISOFireProtectionCode NOT IN ('N/A', '00')
		) AS CorrectISOFireProtectionCode
		WHERE RowNumber = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY IsoFireProtectionCode) = 1
),
LKP_Update_ConstructionCode AS (
	SELECT
	ConstructionCode,
	PolicyKey
	FROM (
		SELECT PolicyKey AS PolicyKey, 
		ConstructionCode AS ConstructionCode
		FROM (
			SELECT DISTINCT POL.pol_key AS PolicyKey, 
			RL.LocationUnitNumber AS LocationNumber, 
			RC.SubLocationUnitNumber AS BuildingNumber, 
			SCC.ConstructionCode AS ConstructionCode,
			(CASE 
				WHEN RC.SubLocationUnitNumber <> '000'
				THEN 
				ROW_NUMBER() OVER (PARTITION BY POL.pol_key
				ORDER BY RL.LocationUnitNumber, RC.SubLocationUnitNumber)
				ELSE 0
			END) 
			AS RowNumber
			FROM PremiumTransaction AS PT WITH (NOLOCK)
			INNER JOIN  RatingCoverage AS RC WITH (NOLOCK)
			ON PT.RatingCoverageAKID = RC.RatingCoverageAKID
			AND RC.EffectiveDate = PT.EffectiveDate 
			INNER JOIN PolicyCoverage AS PC WITH (NOLOCK)
			ON PC.PolicyCoverageAKID = RC.PolicyCoverageAKID
			AND PC.CurrentSnapshotFlag=1
			INNER JOIN RiskLocation AS RL WITH (NOLOCK)
			ON PC.RiskLocationAKID = RL.RiskLocationAKID
			AND RL.CurrentSnapshotFlag = 1
			INNER JOIN V2.policy AS POL WITH (NOLOCK)
			ON POL.pol_ak_id = PC.PolicyAKID
			AND POL.crrnt_snpsht_flag = 1
			INNER JOIN PremiumMasterCalculation AS PMC WITH (NOLOCK)
			ON PMC.PremiumTransactionAKID = PT.PremiumTransactionAKID
			AND PMC.CurrentSnapshotFlag = 1
			INNER JOIN SupConstructionCode AS SCC WITH (NOLOCK)
			ON PT.ConstructionCode = SCC.ConstructionCode
			AND  SCC.CurrentSnapshotFlag = 1
			WHERE RC.SubLocationUnitNumber <> '000'
			AND LEN(POL.pol_key) <> 12
			AND SCC.ConstructionCode <> 'N/A'
		) AS CorrectConstructionCode
		WHERE RowNumber = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY ConstructionCode) = 1
),
SQ_Loss AS (
	--DCT
	declare @QuarterDate as datetime
	
	set @QuarterDate=cast(DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1) as date);
	
	SELECT distinct
	LMC.loss_master_calculation_id,
	LMC.loss_master_run_date,
	POL.prim_bus_class_code,
	RL.StateProvinceCode,
	OCC.claim_loss_date,
	LMC.sub_line_code,
	LMC.class_code,
	CT.cause_of_loss,
	PTRR.RatingTerritoryCode as RiskTerritory,
	POL.pol_eff_date,
	POL.pol_key,
	OCC.claim_occurrence_num,
	CPO.claimant_num,
	(case when LMC.trans_kind_code = 'D' then  LMC.paid_loss_amt else 0 end) as paid_loss_amt,
	(Case when LMC.financialtypecode  = 'D' and LMC.trans_kind_code = 'D' Then LMC.outstanding_amt Else 0 End) as outstanding_amt,
	'BE' pms_type_bureau_code,
	'N/A' AS RiskUnitGroup,
	CCD.PolicySourceID,
	LTRIM(RTRIM(RC.RiskType)) AS RiskType,
	-1 as StatisticalCoverageAKID,
	RC.RatingCoverageAKID,
	POL.pol_exp_date,
	OCC.s3p_claim_num,
	CT.claim_trans_id,
	CCD.claimant_cov_det_ak_id,
	(case when ASL.asl_code='80' then '5.1'
	when ASL.asl_code='100' then '5.2'
	else ASL.asl_num end) asl_num,
	PC.InsuranceLine,
	POL.pol_num,
	LMC.statistical_code1,
	ISG.InsuranceSegmentCode,
	LMC.exposure,
	LTRIM(RTRIM(IRC.CoverageCode)),
	(case when LMC.financialtypecode = 'E' and LMC.trans_kind_code = 'D' then LMF.eom_unpaid_loss_adjust_exp else 0 End) as DirectALAEOutstandingER,
	(case when LMC.trans_kind_code = 'D' then  LMF.paid_exp_amt else 0 end) as DirectALAEPaidIR,
	PL.PolicyPerOccurenceLimit,
	CCD.major_peril_code MajorPerilCode,
	RL.ZipPostalCode,
	POL.pms_pol_lob_code,
	CASE WHEN CT.trans_date<DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},0) THEN LMC.loss_master_run_date ELSE CT.trans_date END AS trans_date
	FROM @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_fact LMF
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_dim ASL
	on LMF.asl_dim_id=ASL.asl_dim_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_calculation LMC
	on LMC.loss_master_calculation_id=LMF.edw_loss_master_calculation_pk_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction CT
	on LMC.claim_trans_ak_id=CT.claim_trans_ak_id
	and LMC.crrnt_snpsht_flag=1
	and CT.crrnt_snpsht_flag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD
	ON CT.claimant_cov_det_ak_id= CCD.claimant_cov_det_ak_id
	AND CCD.crrnt_snpsht_flag = 1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence CPO
	ON CPO.claim_party_occurrence_ak_id=CCD.claim_party_occurrence_ak_id
	AND CPO.Crrnt_snpsht_flag = 1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence OCC
	ON CPO.claim_occurrence_ak_id= OCC.claim_occurrence_ak_id
	AND  OCC.crrnt_snpsht_flag = 1
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim PD with(nolock)
	on LMF.pol_dim_id=PD.pol_dim_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL with (nolock)
	on POL.pol_id=PD.edw_pol_pk_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISG
	on POL.InsuranceSegmentAKId=ISG.InsuranceSegmentAKId
	and ISG.CurrentSnapshotFlag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	on CCD.RatingCoverageAKID=RC.RatingCoverageAKID
	and (case when LMC.trans_offset_onset_ind='O' and LMC.pms_acct_entered_date != '1800-01-01 01:00:00.000'
	then LMC.pms_acct_entered_date
	else DATEADD(D,1,LMC.loss_master_run_date)  end) between RC.EffectiveDate and RC.ExpirationDate 
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	on PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
	and PC.CurrentSnapshotFlag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	on PC.RiskLocationAKID=RL.RiskLocationAKID
	and RL.CurrentSnapshotFlag=1 
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim IRC
	on IRC.InsuranceReferenceCoverageDimId=LMF.InsuranceReferenceCoverageDimId
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.policyLimit PL
	on PC.PolicyAKID = Pl.PolicyAKId and PC.InsuranceLine =PL.InsuranceLine and LMC.loss_master_run_date between PL.EffectiveDate and PL.ExpirationDate
	left join PremiumTransactionRatingRisk PTRR with (nolock) 
	on PTRR.PremiumTransactionAKID=LMC.PremiumTransactionAKID
	where PC.InsuranceLine = 'BusinessOwners'
	AND LMC.trans_kind_code='D'
	AND (DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,LMC.loss_master_run_date),-1) = @QuarterDate)
	AND (LMC.paid_loss_amt<>0 or LMC.outstanding_amt<>0 or LMF.eom_unpaid_loss_adjust_exp <>0 or LMF.paid_exp_amt<>0) 
	AND LMF.audit_id<>-9
	@{pipeline().parameters.WHERE_CLAUSE_2}
),
AGG_RemoveDuplicate AS (
	SELECT
	loss_master_calculation_id,
	loss_master_run_date,
	prim_bus_class_code,
	StateProvinceCode,
	claim_loss_date,
	sub_line_code,
	class_code,
	cause_of_loss,
	RiskTerritory,
	pol_eff_date,
	pol_key,
	claim_occurrence_num,
	claimant_num,
	paid_loss_amt,
	outstanding_amt,
	pms_type_bureau_code,
	RiskUnitGroup,
	PolicySourceID,
	RiskType,
	StatisticalCoverageAKID,
	RatingCoverageAKID,
	pol_exp_date,
	s3p_claim_num,
	claim_trans_id,
	claim_coverage_detail_ak_id,
	asl_num,
	InsuranceLine,
	pol_num,
	statistical_code1,
	InsuranceSegmentCode,
	exposure,
	CoverageCode,
	DirectALAEOutstandingER,
	DirectALAEPaidIR,
	PolicyPerOccurenceLimit,
	MajorPerilCode,
	ZipPostalCode,
	pms_pol_lob_code,
	trans_date
	FROM SQ_Loss
	QUALIFY ROW_NUMBER() OVER (PARTITION BY loss_master_calculation_id ORDER BY NULL) = 1
),
LKP_ISSWorkTable_Loss AS (
	SELECT
	EDWLossMasterCalculationPKId
	FROM (
		SELECT 
			EDWLossMasterCalculationPKId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.ISSBusinessOwnersExtract
		WHERE (DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,LossMasterRunDate),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1))
		and EDWLossMasterCalculationPKId<>-1
		
		--YEAR(LossMasterRunDate)=YEAR(dateadd(year,@{pipeline().parameters.NO_OF_YEARS},GETDATE())) and EDWLossMasterCalculationPKId<>-1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWLossMasterCalculationPKId ORDER BY EDWLossMasterCalculationPKId) = 1
),
EXP_Calculate_ClaimNumber AS (
	SELECT
	AGG_RemoveDuplicate.loss_master_calculation_id,
	AGG_RemoveDuplicate.loss_master_run_date,
	-- *INF*: TO_CHAR(loss_master_run_date, 'YYYYMMDD')
	TO_CHAR(loss_master_run_date, 'YYYYMMDD') AS loss_master_run_datekey,
	AGG_RemoveDuplicate.prim_bus_class_code,
	AGG_RemoveDuplicate.StateProvinceCode,
	AGG_RemoveDuplicate.claim_loss_date,
	AGG_RemoveDuplicate.sub_line_code,
	AGG_RemoveDuplicate.class_code,
	AGG_RemoveDuplicate.cause_of_loss,
	AGG_RemoveDuplicate.RiskTerritory,
	AGG_RemoveDuplicate.pol_eff_date,
	AGG_RemoveDuplicate.pol_key,
	AGG_RemoveDuplicate.claim_occurrence_num,
	AGG_RemoveDuplicate.claimant_num,
	AGG_RemoveDuplicate.paid_loss_amt,
	AGG_RemoveDuplicate.outstanding_amt,
	AGG_RemoveDuplicate.pms_type_bureau_code,
	AGG_RemoveDuplicate.RiskUnitGroup,
	AGG_RemoveDuplicate.PolicySourceID,
	AGG_RemoveDuplicate.RiskType,
	AGG_RemoveDuplicate.StatisticalCoverageAKID AS i_StatisticalCoverageAKID,
	AGG_RemoveDuplicate.RatingCoverageAKID AS i_RatingCoverageAKID,
	-- *INF*: IIF(
	-- i_StatisticalCoverageAKID=-1, i_RatingCoverageAKID, i_StatisticalCoverageAKID
	-- )
	IFF(i_StatisticalCoverageAKID = - 1, i_RatingCoverageAKID, i_StatisticalCoverageAKID) AS o_CoverageAKID,
	-- *INF*: IIF(i_StatisticalCoverageAKID=-1,'DCT','PMS')
	IFF(i_StatisticalCoverageAKID = - 1, 'DCT', 'PMS') AS o_SourceSystem,
	AGG_RemoveDuplicate.pol_exp_date,
	AGG_RemoveDuplicate.s3p_claim_num,
	AGG_RemoveDuplicate.claim_trans_id,
	LKP_ISSWorkTable_Loss.EDWLossMasterCalculationPKId,
	AGG_RemoveDuplicate.claim_coverage_detail_ak_id,
	AGG_RemoveDuplicate.asl_num,
	AGG_RemoveDuplicate.InsuranceLine,
	AGG_RemoveDuplicate.pol_num,
	AGG_RemoveDuplicate.statistical_code1,
	AGG_RemoveDuplicate.InsuranceSegmentCode,
	AGG_RemoveDuplicate.exposure,
	AGG_RemoveDuplicate.CoverageCode,
	AGG_RemoveDuplicate.DirectALAEOutstandingER,
	AGG_RemoveDuplicate.DirectALAEPaidIR,
	AGG_RemoveDuplicate.PolicyPerOccurenceLimit,
	AGG_RemoveDuplicate.MajorPerilCode,
	AGG_RemoveDuplicate.ZipPostalCode,
	-- *INF*: IIF(ISNULL(claim_occurrence_num) OR IS_SPACES(claim_occurrence_num) OR LENGTH(claim_occurrence_num)=0 OR claim_occurrence_num='N/A' OR IS_NUMBER(claim_occurrence_num)=0, 0, TO_INTEGER(claim_occurrence_num))
	IFF(
	    claim_occurrence_num IS NULL
	    or LENGTH(claim_occurrence_num)>0
	    and TRIM(claim_occurrence_num)=''
	    or LENGTH(claim_occurrence_num) = 0
	    or claim_occurrence_num = 'N/A'
	    or REGEXP_LIKE(claim_occurrence_num, '^[0-9]+$') = 0,
	    0,
	    CAST(claim_occurrence_num AS INTEGER)
	) AS v_claim_occurrence_num,
	-- *INF*: LTRIM(RTRIM(pol_num))||TO_CHAR(claim_loss_date,'YYMMDD') ||SUBSTR(claim_occurrence_num,2,2)
	LTRIM(RTRIM(pol_num)) || TO_CHAR(claim_loss_date, 'YYMMDD') || SUBSTR(claim_occurrence_num, 2, 2) AS o_ClaimNum,
	AGG_RemoveDuplicate.pms_pol_lob_code,
	AGG_RemoveDuplicate.trans_date
	FROM AGG_RemoveDuplicate
	LEFT JOIN LKP_ISSWorkTable_Loss
	ON LKP_ISSWorkTable_Loss.EDWLossMasterCalculationPKId = AGG_RemoveDuplicate.loss_master_calculation_id
),
SQ_Coverage AS (
	declare @QuarterDate as datetime
	
	set @QuarterDate=cast(DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1) as date);
	
	select loss_master_calculation_id as loss_master_calculation_id,
	CoverageAKID as CoverageAKID,
	ConstructionCode AS ConstructionCode, 
	IsoFireProtectionCode as IsoFireProtectionCode, 
	SprinklerFlag as SprinklerFlag,
	BureauCode1 as BureauCode1,
	BureauCode2 as BureauCode2,
	BureauCode4 as BureauCode4,
	CoverageLimitValue as CoverageLimitValue,
	PremiumTransactionAKID as PremiumTransactionAKID,
	PremiumTransactionEffectiveDate as PremiumTransactionEffectiveDate
	from 
	--------------------------------------------------------------------------------------------
	(
		select distinct LMC.loss_master_calculation_id,
		PT.RatingCoverageAKID as CoverageAKID,
		SCC.StandardConstructionCodeDescription AS ConstructionCode,
		CDCP.IsoFireProtectionCode as IsoFireProtectionCode, 
		CDCP.SprinklerFlag as SprinklerFlag,
		null as BureauCode1,
		null as BureauCode2,
		null as BureauCode4,
		CL.CoverageLimitValue as CoverageLimitValue,
		PT.PremiumTransactionAKID as PremiumTransactionAKID ,
		PT.PremiumTransactionEffectiveDate as PremiumTransactionEffectiveDate,
		row_number() over (partition by LMC.loss_master_calculation_id
		order by PT.PremiumTransactionEffectiveDate desc, PT.EffectiveDate desc, PT.OffsetOnsetCode desc) RowNumber
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_calculation LMC with (nolock)
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_fact LMF with (nolock)
		on LMC.loss_master_calculation_id=LMF.edw_loss_master_calculation_pk_id
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction CT with (nolock)
		on LMC.claim_trans_ak_id=CT.claim_trans_ak_id
		and LMC.crrnt_snpsht_flag=1
		and CT.crrnt_snpsht_flag=1
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD with (nolock)
		on CT.claimant_cov_det_ak_id= CCD.claimant_cov_det_ak_id
		and CCD.crrnt_snpsht_flag = 1
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence CPO with (nolock)
		on CPO.claim_party_occurrence_ak_id=CCD.claim_party_occurrence_ak_id
		and CPO.Crrnt_snpsht_flag = 1
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence OCC with (nolock)
		on CPO.claim_occurrence_ak_id= OCC.claim_occurrence_ak_id
		and  OCC.crrnt_snpsht_flag = 1
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC with (nolock)
		on LMC.RatingCoverageAKID = RC.RatingCoverageAKID
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC with (nolock)
		on PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
		and PC.CurrentSnapshotFlag=1
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT with (nolock)
		on RC.RatingCoverageAKID = PT.RatingCoverageAKID 
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialProperty CDCP with (nolock)
		on CDCP.PremiumTransactionID = PT.PremiumTransactionID
		and CDCP.CurrentSnapshotFlag=1
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupConstructionCode SCC
		on PT.ConstructionCode = SCC.ConstructionCode
		and SCC.CurrentSnapshotFlag=1
		left join (@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit CL
			inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge CLB
			on CL.CoverageLimitID=CLB.CoverageLimitID)
		on CLB.PremiumTransactionAKID = PT.PremiumTransactionAKID
		where PT.SourceSystemID='DCT'
		and CL.coveragelimitvalue is not null
		and PT.OffsetOnsetCode in ('Onset', 'N/A')
		and PT.PremiumTransactionEffectiveDate < OCC.claim_loss_date
		and PC.InsuranceLine = 'BusinessOwners'
		and LMC.trans_kind_code='D'
		and (DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,LMC.loss_master_run_date),-1) = @QuarterDate)
		and (LMC.paid_loss_amt<>0 or LMC.outstanding_amt<>0 or LMF.eom_unpaid_loss_adjust_exp <>0 or LMF.paid_exp_amt<>0) 
		and LMF.audit_id<>-9
	)  as lookuptable 
	where RowNumber = 1
	@{pipeline().parameters.WHERE_CLAUSE_3}
),
JNR_Coverage AS (SELECT
	EXP_Calculate_ClaimNumber.loss_master_calculation_id, 
	EXP_Calculate_ClaimNumber.loss_master_run_date, 
	EXP_Calculate_ClaimNumber.loss_master_run_datekey, 
	EXP_Calculate_ClaimNumber.prim_bus_class_code, 
	EXP_Calculate_ClaimNumber.StateProvinceCode, 
	EXP_Calculate_ClaimNumber.claim_loss_date, 
	EXP_Calculate_ClaimNumber.sub_line_code, 
	EXP_Calculate_ClaimNumber.class_code, 
	EXP_Calculate_ClaimNumber.cause_of_loss, 
	EXP_Calculate_ClaimNumber.RiskTerritory, 
	EXP_Calculate_ClaimNumber.pol_eff_date, 
	EXP_Calculate_ClaimNumber.pol_key, 
	EXP_Calculate_ClaimNumber.claim_occurrence_num, 
	EXP_Calculate_ClaimNumber.claimant_num, 
	EXP_Calculate_ClaimNumber.paid_loss_amt, 
	EXP_Calculate_ClaimNumber.outstanding_amt, 
	EXP_Calculate_ClaimNumber.pms_type_bureau_code, 
	EXP_Calculate_ClaimNumber.RiskUnitGroup, 
	EXP_Calculate_ClaimNumber.PolicySourceID, 
	EXP_Calculate_ClaimNumber.RiskType, 
	EXP_Calculate_ClaimNumber.o_CoverageAKID, 
	EXP_Calculate_ClaimNumber.o_SourceSystem AS SourceSystem, 
	EXP_Calculate_ClaimNumber.pol_exp_date, 
	EXP_Calculate_ClaimNumber.s3p_claim_num, 
	EXP_Calculate_ClaimNumber.claim_trans_id, 
	EXP_Calculate_ClaimNumber.EDWLossMasterCalculationPKId, 
	EXP_Calculate_ClaimNumber.claim_coverage_detail_ak_id, 
	EXP_Calculate_ClaimNumber.asl_num, 
	EXP_Calculate_ClaimNumber.InsuranceLine, 
	EXP_Calculate_ClaimNumber.pol_num, 
	EXP_Calculate_ClaimNumber.statistical_code1, 
	EXP_Calculate_ClaimNumber.InsuranceSegmentCode, 
	EXP_Calculate_ClaimNumber.exposure, 
	EXP_Calculate_ClaimNumber.CoverageCode, 
	EXP_Calculate_ClaimNumber.DirectALAEOutstandingER, 
	EXP_Calculate_ClaimNumber.DirectALAEPaidIR, 
	EXP_Calculate_ClaimNumber.PolicyPerOccurenceLimit, 
	EXP_Calculate_ClaimNumber.MajorPerilCode, 
	EXP_Calculate_ClaimNumber.ZipPostalCode, 
	EXP_Calculate_ClaimNumber.o_ClaimNum AS ClaimNum, 
	EXP_Calculate_ClaimNumber.pms_pol_lob_code, 
	EXP_Calculate_ClaimNumber.trans_date, 
	SQ_Coverage.loss_master_calculation_id AS loss_master_calculation_id1, 
	SQ_Coverage.CoverageAKID, 
	SQ_Coverage.ConstructionCode, 
	SQ_Coverage.IsoFireProtectionCode, 
	SQ_Coverage.SprinklerFlag, 
	SQ_Coverage.BureauCode1, 
	SQ_Coverage.BureauCode2, 
	SQ_Coverage.BureauCode4, 
	SQ_Coverage.CoverageLimitValue, 
	SQ_Coverage.PremiumTransactionAKID, 
	SQ_Coverage.PremiumTransactionEffectiveDate
	FROM SQ_Coverage
	RIGHT OUTER JOIN EXP_Calculate_ClaimNumber
	ON EXP_Calculate_ClaimNumber.loss_master_calculation_id = SQ_Coverage.loss_master_calculation_id AND EXP_Calculate_ClaimNumber.o_CoverageAKID = SQ_Coverage.CoverageAKID
),
SRT_Sort_data AS (
	SELECT
	pol_key, 
	ClaimNum, 
	loss_master_run_date, 
	loss_master_run_datekey, 
	claim_coverage_detail_ak_id, 
	loss_master_calculation_id, 
	prim_bus_class_code, 
	StateProvinceCode, 
	claim_loss_date, 
	sub_line_code, 
	class_code, 
	cause_of_loss, 
	RiskTerritory, 
	pol_eff_date, 
	claim_occurrence_num, 
	claimant_num, 
	paid_loss_amt, 
	outstanding_amt, 
	pms_type_bureau_code, 
	RiskUnitGroup, 
	PolicySourceID, 
	RiskType, 
	CoverageAKID, 
	SourceSystem, 
	pol_exp_date, 
	s3p_claim_num, 
	claim_trans_id, 
	EDWLossMasterCalculationPKId, 
	asl_num, 
	InsuranceLine, 
	pol_num, 
	statistical_code1, 
	InsuranceSegmentCode, 
	exposure, 
	CoverageCode, 
	DirectALAEOutstandingER, 
	DirectALAEPaidIR, 
	PolicyPerOccurenceLimit, 
	MajorPerilCode, 
	ZipPostalCode, 
	pms_pol_lob_code, 
	trans_date, 
	ConstructionCode, 
	IsoFireProtectionCode, 
	SprinklerFlag, 
	BureauCode1, 
	BureauCode2, 
	BureauCode4, 
	CoverageLimitValue, 
	PremiumTransactionAKID, 
	PremiumTransactionEffectiveDate
	FROM JNR_Coverage
	ORDER BY pol_key ASC, ClaimNum ASC, loss_master_run_date ASC, claim_coverage_detail_ak_id ASC
),
LKP_InceptionToDatePaidLossAmount AS (
	SELECT
	InceptionToDatePaidLossAmount,
	i_loss_master_calculation_id,
	pol_key,
	edw_claimant_cov_det_ak_id,
	trans_date,
	loss_master_calculation_id
	FROM (
		SELECT DISTINCT
		--Old logic for OutstandingAmount records
		--CASE WHEN InceptionToDatePaidLossAmount=0 and PaidLossAmount=0 THEN LAST_VALUE(InceptionToDatePaidLossAmount) OVER (partition by pol_key,edw_claimant_cov_det_ak_id,year(trans_date), month(trans_date) order by trans_date rows between unbounded preceding and unbounded following ) 
		CASE WHEN InceptionToDatePaidLossAmount=0 and PaidLossAmount=0 THEN SUM(PaidLossAmount) OVER (partition by pol_key,edw_claimant_cov_det_ak_id,claim_num order by loss_master_run_date)
		ELSE InceptionToDatePaidLossAmount END AS InceptionToDatePaidLossAmount,
		pol_key AS pol_key,
		edw_claimant_cov_det_ak_id AS edw_claimant_cov_det_ak_id,
		trans_date AS trans_date
		,loss_master_calculation_id AS loss_master_calculation_id
		FROM
		(
		SELECT distinct SUM(InceptionToDatePaidLossAmount) over (partition by pol_key,edw_claimant_cov_det_ak_id,claim_num order by edw_claimant_cov_det_ak_id,trans_date,claim_trans_pk_id) AS InceptionToDatePaidLossAmount,
		edw_claimant_cov_det_ak_id AS edw_claimant_cov_det_ak_id,
		trans_date AS trans_date,
		InceptionToDatePaidLossAmount AS PaidLossAmount,
		pol_key
		,loss_master_calculation_id
		,loss_master_run_date
		,claim_num
		FROM (
		SELECT  f.direct_loss_paid_including_recoveries AS InceptionToDatePaidLossAmount,  
		--f.direct_loss_outstanding_excluding_recoveries AS OutstandingAmount, --US-403702 Commenting out since we don't need it
		edw_claimant_cov_det_ak_id AS edw_claimant_cov_det_ak_id,
		ct.trans_date,
		p.pol_key AS pol_key,
		o.claim_num AS claim_num,
		lmc.loss_master_calculation_id,
		lmc.loss_master_run_date,
		lmc.claim_trans_pk_id
		from @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_loss_transaction_fact f
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_dim d
		on f.claimant_cov_dim_id = d.claimant_cov_dim_id
		inner join vw_claim_transaction ct
		on f.edw_claim_trans_pk_id=ct.claim_trans_id
		and ct.trans_date<'2001-01-01'
		join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim p
		on f.pol_dim_id=p.pol_dim_id
		join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_dim O 
		on F.claim_occurrence_dim_id=o.claim_occurrence_dim_id
		--Join added for US-403702
		left join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.vwLossMasterFact lmf
		on lmf.claimant_cov_dim_id = d.claimant_cov_dim_id
		join loss_master_calculation lmc
		on lmc.loss_master_calculation_id = lmf.edw_loss_master_calculation_pk_id
		UNION ALL
		SELECT f.DirectLossPaidIR AS InceptionToDatePaidLossAmount,  
		--f.DirectLossOutstandingER AS OutstandingAmount, --US-403702 Commenting out since we don't need it
		edw_claimant_cov_det_ak_id AS edw_claimant_cov_det_ak_id,
		ct.trans_date,
		p.pol_key AS pol_key,
		o.claim_num AS claim_num,
		lmc.loss_master_calculation_id,
		lmc.loss_master_run_date,
		lmc.claim_trans_pk_id
		from @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.vwLossMasterFact f
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_dim d
		on f.claimant_cov_dim_id = d.claimant_cov_dim_id
		inner join vw_claim_transaction ct
		on f.edw_claim_trans_pk_id=ct.claim_trans_id
		and ct.trans_date>='2001-01-01'
		join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim p
		on f.pol_dim_id=p.pol_dim_id
		join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_dim O 
		on F.claim_occurrence_dim_id=o.claim_occurrence_dim_id
		--Join added for US-403702
		join loss_master_calculation lmc
		on lmc.loss_master_calculation_id = f.edw_loss_master_calculation_pk_id
		) T
		) T
		WHERE cast(trans_date as date)<=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS} ,-1) 
		ORDER BY pol_key,edw_claimant_cov_det_ak_id,trans_date
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key,edw_claimant_cov_det_ak_id,trans_date,loss_master_calculation_id ORDER BY InceptionToDatePaidLossAmount DESC) = 1
),
FIL_Exists_Loss AS (
	SELECT
	SRT_Sort_data.EDWLossMasterCalculationPKId AS LKP_LossMasterCalculationId, 
	SRT_Sort_data.pol_key, 
	SRT_Sort_data.ClaimNum, 
	SRT_Sort_data.loss_master_run_date, 
	SRT_Sort_data.loss_master_calculation_id, 
	SRT_Sort_data.prim_bus_class_code, 
	SRT_Sort_data.StateProvinceCode, 
	SRT_Sort_data.claim_loss_date, 
	SRT_Sort_data.sub_line_code, 
	SRT_Sort_data.class_code AS ClassCode, 
	SRT_Sort_data.cause_of_loss, 
	SRT_Sort_data.RiskTerritory, 
	SRT_Sort_data.pol_eff_date, 
	SRT_Sort_data.claim_occurrence_num, 
	SRT_Sort_data.claimant_num, 
	SRT_Sort_data.paid_loss_amt, 
	SRT_Sort_data.outstanding_amt, 
	SRT_Sort_data.pms_type_bureau_code AS TypeBureauCode, 
	SRT_Sort_data.RiskUnitGroup, 
	SRT_Sort_data.PolicySourceID, 
	SRT_Sort_data.RiskType, 
	SRT_Sort_data.CoverageAKID, 
	SRT_Sort_data.SourceSystem, 
	SRT_Sort_data.pol_exp_date, 
	SRT_Sort_data.s3p_claim_num, 
	SRT_Sort_data.claim_trans_id, 
	SRT_Sort_data.claim_coverage_detail_ak_id, 
	LKP_InceptionToDatePaidLossAmount.InceptionToDatePaidLossAmount, 
	SRT_Sort_data.asl_num, 
	SRT_Sort_data.InsuranceLine, 
	SRT_Sort_data.pol_num, 
	SRT_Sort_data.statistical_code1, 
	SRT_Sort_data.InsuranceSegmentCode, 
	SRT_Sort_data.exposure, 
	SRT_Sort_data.CoverageCode, 
	SRT_Sort_data.DirectALAEOutstandingER, 
	SRT_Sort_data.DirectALAEPaidIR, 
	SRT_Sort_data.PolicyPerOccurenceLimit, 
	SRT_Sort_data.MajorPerilCode, 
	SRT_Sort_data.ZipPostalCode, 
	SRT_Sort_data.pms_pol_lob_code, 
	SRT_Sort_data.ConstructionCode, 
	SRT_Sort_data.IsoFireProtectionCode, 
	SRT_Sort_data.SprinklerFlag, 
	SRT_Sort_data.BureauCode1, 
	SRT_Sort_data.BureauCode2, 
	SRT_Sort_data.BureauCode4, 
	SRT_Sort_data.CoverageLimitValue, 
	SRT_Sort_data.PremiumTransactionAKID, 
	SRT_Sort_data.PremiumTransactionEffectiveDate
	FROM SRT_Sort_data
	LEFT JOIN LKP_InceptionToDatePaidLossAmount
	ON LKP_InceptionToDatePaidLossAmount.pol_key = SRT_Sort_data.pol_key AND LKP_InceptionToDatePaidLossAmount.edw_claimant_cov_det_ak_id = SRT_Sort_data.claim_coverage_detail_ak_id AND LKP_InceptionToDatePaidLossAmount.trans_date <= SRT_Sort_data.trans_date AND LKP_InceptionToDatePaidLossAmount.loss_master_calculation_id = SRT_Sort_data.loss_master_calculation_id
	WHERE ISNULL(LKP_LossMasterCalculationId) AND  
(paid_loss_amt != 0 or outstanding_amt!=0 or DirectALAEPaidIR!=0  or DirectALAEOutstandingER !=0)
and TO_CHAR(loss_master_run_date, 'YYYY') ||TO_CHAR(loss_master_run_date, 'QQ')=
TO_CHAR( ADD_TO_DATE(sysdate, 'MM', 3*@{pipeline().parameters.NO_OF_QUARTERS}), 'YYYY') ||TO_CHAR( ADD_TO_DATE(sysdate, 'MM', 3*@{pipeline().parameters.NO_OF_QUARTERS}), 'QQ')
and  NOT  IN(LTRIM(RTRIM(CoverageCode)),'MINE','COMPATT','CYBEXTEXP','NETSECLIAB','EXTREP')


---CoverageCode != 'MINE'

--Excluding coverage code COMPATT,CYBEXTEXP,NETSECLIAB,EXTREP as part of Cyber One project Requirement
),
EXP_Cleansing_Loss AS (
	SELECT
	pol_key AS i_pol_key,
	ClaimNum AS i_ClaimNum,
	loss_master_run_date AS i_loss_master_run_date,
	loss_master_calculation_id AS i_loss_master_calculation_id,
	prim_bus_class_code AS i_prim_bus_class_code,
	StateProvinceCode AS i_StateProvinceCode,
	claim_loss_date AS i_claim_loss_date,
	sub_line_code AS i_sub_line_code,
	ClassCode AS i_ClassCode,
	cause_of_loss AS i_cause_of_loss,
	RiskTerritory AS i_RiskTerritory,
	pol_eff_date AS i_pol_eff_date,
	claim_occurrence_num AS i_claim_occurrence_num,
	claimant_num AS i_claimant_num,
	paid_loss_amt AS i_paid_loss_amt,
	outstanding_amt AS i_outstanding_amt,
	TypeBureauCode AS i_TypeBureauCode,
	RiskUnitGroup AS i_RiskUnitGroup,
	PolicySourceID AS i_PolicySourceID,
	RiskType AS i_RiskType,
	CoverageAKID,
	SourceSystem,
	pol_exp_date AS i_pol_exp_date,
	s3p_claim_num,
	InceptionToDatePaidLossAmount AS i_InceptionToDatePaidLossAmount,
	asl_num AS i_asl_num,
	claim_coverage_detail_ak_id,
	i_loss_master_calculation_id AS o_loss_master_calculation_id,
	i_loss_master_run_date AS o_loss_master_run_date,
	-- *INF*: RTRIM(LTRIM(i_pol_key))
	RTRIM(LTRIM(i_pol_key)) AS o_pol_key,
	-- *INF*: RTRIM(LTRIM(i_prim_bus_class_code))
	RTRIM(LTRIM(i_prim_bus_class_code)) AS o_prim_bus_class_code,
	-- *INF*: RTRIM(LTRIM(i_StateProvinceCode))
	RTRIM(LTRIM(i_StateProvinceCode)) AS o_StateProvinceCode,
	i_claim_loss_date AS o_claim_loss_date,
	-- *INF*: RTRIM(LTRIM(i_sub_line_code))
	RTRIM(LTRIM(i_sub_line_code)) AS o_sub_line_code,
	-- *INF*: RTRIM(LTRIM(i_ClassCode))
	RTRIM(LTRIM(i_ClassCode)) AS o_ClassCode,
	-- *INF*: RTRIM(LTRIM(i_cause_of_loss))
	RTRIM(LTRIM(i_cause_of_loss)) AS o_cause_of_loss,
	-- *INF*: RTRIM(LTRIM(i_RiskTerritory))
	RTRIM(LTRIM(i_RiskTerritory)) AS o_RiskTerritory,
	i_pol_eff_date AS o_pol_eff_date,
	-- *INF*: RTRIM(LTRIM(i_claim_occurrence_num))
	RTRIM(LTRIM(i_claim_occurrence_num)) AS o_claim_occurrence_num,
	-- *INF*: RTRIM(LTRIM(i_claimant_num))
	RTRIM(LTRIM(i_claimant_num)) AS o_claimant_num,
	-- *INF*: IIF(ISNULL(i_paid_loss_amt), 0, i_paid_loss_amt)
	-- 
	-- ---i_paid_loss_amt
	IFF(i_paid_loss_amt IS NULL, 0, i_paid_loss_amt) AS o_PaidLossAmount,
	-- *INF*: IIF(ISNULL(i_outstanding_amt), 0, i_outstanding_amt)
	-- 
	-- --i_outstanding_amt
	IFF(i_outstanding_amt IS NULL, 0, i_outstanding_amt) AS o_OutstandingLossAmount,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_TypeBureauCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_TypeBureauCode) AS o_TypeBureauCode,
	-- *INF*: RTRIM(LTRIM(i_RiskUnitGroup))
	RTRIM(LTRIM(i_RiskUnitGroup)) AS o_RiskUnitGroup,
	-- *INF*: LTRIM(RTRIM(i_PolicySourceID))
	LTRIM(RTRIM(i_PolicySourceID)) AS o_PolicySourceID,
	-- *INF*: LTRIM(RTRIM(i_RiskType))
	LTRIM(RTRIM(i_RiskType)) AS o_RiskType,
	i_pol_exp_date AS o_pol_exp_date,
	-- *INF*: RTRIM(LTRIM(s3p_claim_num))
	RTRIM(LTRIM(s3p_claim_num)) AS o_s3p_claim_num,
	-- *INF*: DECODE(True,
	-- direct_alae_paid_including_recoveries<>0, 0,
	-- direct_loss_outstanding_excluding_recoveries<>0,0,
	-- i_InceptionToDatePaidLossAmount
	-- )
	DECODE(
	    True,
	    direct_alae_paid_including_recoveries <> 0, 0,
	    direct_loss_outstanding_excluding_recoveries <> 0, 0,
	    i_InceptionToDatePaidLossAmount
	) AS o_InceptionToDatePaidLossAmount,
	i_asl_num AS o_AnnualStatementLineNumber,
	InsuranceLine AS i_InsuranceLine,
	-- *INF*: LTRIM(RTRIM(i_InsuranceLine))
	LTRIM(RTRIM(i_InsuranceLine)) AS o_InsuranceLine,
	pol_num,
	i_ClaimNum AS o_ClaimNum,
	statistical_code1 AS i_statistical_code1,
	-- *INF*: DECODE(TRUE,
	-- isnull(i_statistical_code1),'N/A',
	-- ltrim(rtrim(i_statistical_code1))='','N/A',
	-- length(rtrim(ltrim(i_statistical_code1))) < 5,'N/A',
	-- i_statistical_code1)
	DECODE(
	    TRUE,
	    i_statistical_code1 IS NULL, 'N/A',
	    ltrim(rtrim(i_statistical_code1)) = '', 'N/A',
	    length(rtrim(ltrim(i_statistical_code1))) < 5, 'N/A',
	    i_statistical_code1
	) AS v_statistical_code1,
	-- *INF*: IIF(v_statistical_code1 != 'N/A',substr(v_statistical_code1,3,1),'N/A')
	IFF(v_statistical_code1 != 'N/A', substr(v_statistical_code1, 3, 1), 'N/A') AS o_pms_const_code,
	-- *INF*: IIF(v_statistical_code1 != 'N/A',substr(v_statistical_code1,4,2),'N/A')
	IFF(v_statistical_code1 != 'N/A', substr(v_statistical_code1, 4, 2), 'N/A') AS o_pms_iso_ppc_code,
	InsuranceSegmentCode,
	exposure AS i_exposure,
	-- *INF*: IIF(ISNULL(i_exposure),0,i_exposure)
	IFF(i_exposure IS NULL, 0, i_exposure) AS o_exposure,
	CoverageCode,
	DirectALAEPaidIR AS direct_alae_paid_including_recoveries,
	DirectALAEOutstandingER AS direct_loss_outstanding_excluding_recoveries,
	PolicyPerOccurenceLimit,
	ZipPostalCode AS i_ZipPostalCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ZipPostalCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_ZipPostalCode) AS o_ZipPostalCode,
	MajorPerilCode,
	pms_pol_lob_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(pms_pol_lob_code)
	UDF_DEFAULT_VALUE_FOR_STRINGS(pms_pol_lob_code) AS v_pms_pol_lob_code,
	-- *INF*: IIF(v_pms_pol_lob_code  != 'N/A' , v_pms_pol_lob_code  , 
	-- decode(i_InsuranceLine, 'BusinessOwners' , 'BOP',
	-- 'CommercialAuto' , 'CPP',
	-- 'CommercialUmbrella' , 'CPP',
	-- 'Crime' , 'CPP',
	-- 'DirectorsAndOfficersNFP' , 'CPP',
	-- 'DirectorsAndOffsCondos' , 'CPP',
	-- 'EmploymentPracticesLiab' , 'CPP',
	-- 'ExcessLiability' , 'CPP',
	-- 'GamesOfChance' , 'CPP',
	-- 'GeneralLiability' , 'CPP',
	-- 'HoleInOne' , 'CPP',
	-- 'InlandMarine' , 'CPP',
	-- 'Property' , 'CPP',
	-- 'SBOPGeneralLiability' , 'CPP',
	-- 'SBOPProperty' , 'CPP',
	-- i_InsuranceLine))
	IFF(
	    v_pms_pol_lob_code != 'N/A', v_pms_pol_lob_code,
	    decode(
	        i_InsuranceLine,
	        'BusinessOwners', 'BOP',
	        'CommercialAuto', 'CPP',
	        'CommercialUmbrella', 'CPP',
	        'Crime', 'CPP',
	        'DirectorsAndOfficersNFP', 'CPP',
	        'DirectorsAndOffsCondos', 'CPP',
	        'EmploymentPracticesLiab', 'CPP',
	        'ExcessLiability', 'CPP',
	        'GamesOfChance', 'CPP',
	        'GeneralLiability', 'CPP',
	        'HoleInOne', 'CPP',
	        'InlandMarine', 'CPP',
	        'Property', 'CPP',
	        'SBOPGeneralLiability', 'CPP',
	        'SBOPProperty', 'CPP',
	        i_InsuranceLine
	    )
	) AS o_pms_pol_lob_code,
	ConstructionCode,
	IsoFireProtectionCode,
	SprinklerFlag,
	BureauCode1,
	BureauCode2,
	BureauCode4,
	CoverageLimitValue,
	PremiumTransactionAKID,
	PremiumTransactionEffectiveDate
	FROM FIL_Exists_Loss
),
EXP_Reset_Pms_ConstCode_IsoPPC AS (
	SELECT
	o_TypeBureauCode AS i_TypeBureauCode,
	o_pms_const_code AS i_pms_const_code,
	o_pms_iso_ppc_code AS i_pms_iso_ppc_code,
	SourceSystem AS i_SourceSystem,
	ConstructionCode AS lkp_ConstructionCode,
	IsoFireProtectionCode AS lkp_IsoFireProtectionCode,
	SprinklerFlag AS lkp_SprinklerFlag,
	-- *INF*: lkp_ConstructionCode
	-- 
	-- --IIF(i_SourceSystem='PMS' and i_pms_const_code != 'N/A' and ltrim(rtrim(i_TypeBureauCode))--='CF',i_pms_const_code,lkp_ConstructionCode)
	lkp_ConstructionCode AS v_const_code,
	-- *INF*: lkp_IsoFireProtectionCode
	-- 
	-- --IIF(i_SourceSystem='PMS' and i_pms_iso_ppc_code != 'N/A' and ltrim(rtrim(i_TypeBureauCode))--='CF',i_pms_iso_ppc_code,lkp_IsoFireProtectionCode)
	lkp_IsoFireProtectionCode AS v_iso_code,
	v_const_code AS o_ConsturctionCode,
	v_iso_code AS o_IsoFireProtectionCode,
	-- *INF*: DECODE(lkp_SprinklerFlag,'T','1','F','0',NULL)
	DECODE(
	    lkp_SprinklerFlag,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_SprinklerFlag
	FROM EXP_Cleansing_Loss
),
LKP_CauseOfLoss AS (
	SELECT
	BureauCauseOfLoss,
	CauseOfLoss,
	LineOfBusiness,
	MajorPeril
	FROM (
		SELECT 
			BureauCauseOfLoss,
			CauseOfLoss,
			LineOfBusiness,
			MajorPeril
		FROM sup_CauseOfLoss
		WHERE CurrentSnapshotFlag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CauseOfLoss,LineOfBusiness,MajorPeril ORDER BY BureauCauseOfLoss) = 1
),
EXP_Logic_Loss AS (
	SELECT
	EXP_Cleansing_Loss.o_loss_master_calculation_id AS loss_master_calculation_id,
	EXP_Cleansing_Loss.claim_coverage_detail_ak_id,
	-1 AS PremiumMasterCalculationID,
	-- *INF*: TO_DATE('1800-01-01','YYYY/MM/DD')
	TO_TIMESTAMP('1800-01-01', 'YYYY/MM/DD') AS PremiumMasterRunDate,
	EXP_Cleansing_Loss.o_loss_master_run_date AS loss_master_run_date,
	EXP_Cleansing_Loss.o_pol_key AS pol_key,
	EXP_Cleansing_Loss.o_prim_bus_class_code AS prim_bus_class_code,
	EXP_Cleansing_Loss.o_StateProvinceCode AS StateProvinceCode,
	EXP_Cleansing_Loss.o_claim_loss_date AS claim_loss_date,
	EXP_Cleansing_Loss.o_sub_line_code AS sub_line_code,
	-- *INF*: DECODE(TRUE,sub_line_code='025','027',sub_line_code)
	DECODE(
	    TRUE,
	    sub_line_code = '025', '027',
	    sub_line_code
	) AS sub_line_code_out,
	EXP_Cleansing_Loss.o_ClassCode AS ClassCode,
	'N/A' AS PremiumMasterClassCode_out,
	-- *INF*: IIF(ISNULL(ClassCode) OR IS_SPACES(ClassCode) OR LENGTH(ClassCode)=0
	-- OR IN(ClassCode, 'N/A','TBD'),
	-- '00000',
	-- LPAD(ClassCode, 5, '0')
	-- )
	-- 
	-- --ClassCode
	IFF(
	    ClassCode IS NULL
	    or LENGTH(ClassCode)>0
	    and TRIM(ClassCode)=''
	    or LENGTH(ClassCode) = 0
	    or ClassCode IN ('N/A','TBD'),
	    '00000',
	    LPAD(ClassCode, 5, '0')
	) AS LossMasterClassCode_out,
	LKP_CauseOfLoss.BureauCauseOfLoss AS cause_of_loss,
	-- *INF*: IIF(ISNULL(cause_of_loss) OR LENGTH(cause_of_loss)=0,'N/A',cause_of_loss)
	IFF(cause_of_loss IS NULL OR LENGTH(cause_of_loss) = 0, 'N/A', cause_of_loss) AS cause_of_loss_out,
	EXP_Cleansing_Loss.o_RiskTerritory AS RiskTerritory,
	-- *INF*: RiskTerritory
	-- 
	-- --SUBSTR(RiskTerritory,2,2)
	RiskTerritory AS TerritoryCode_out,
	EXP_Cleansing_Loss.o_pol_eff_date AS pol_eff_date,
	EXP_Cleansing_Loss.o_claim_occurrence_num AS i_claim_occurrence_num,
	EXP_Cleansing_Loss.o_s3p_claim_num AS i_s3p_claim_num,
	EXP_Cleansing_Loss.o_InsuranceLine AS i_InsuranceLine,
	EXP_Cleansing_Loss.pol_num AS i_pol_num,
	-- *INF*: IIF(ISNULL(i_claim_occurrence_num) OR IS_SPACES(i_claim_occurrence_num) OR LENGTH(i_claim_occurrence_num)=0 OR i_claim_occurrence_num='N/A' OR IS_NUMBER(i_claim_occurrence_num)=0, 0, TO_INTEGER(i_claim_occurrence_num))
	IFF(
	    i_claim_occurrence_num IS NULL
	    or LENGTH(i_claim_occurrence_num)>0
	    and TRIM(i_claim_occurrence_num)=''
	    or LENGTH(i_claim_occurrence_num) = 0
	    or i_claim_occurrence_num = 'N/A'
	    or REGEXP_LIKE(i_claim_occurrence_num, '^[0-9]+$') = 0,
	    0,
	    CAST(i_claim_occurrence_num AS INTEGER)
	) AS v_claim_occurrence_num,
	-- *INF*: DECODE(TRUE,
	-- i_InsuranceLine='CF',
	-- IIF(TRUNC(pol_eff_date, 'MM')  <= TO_DATE('2003-10', 'YYYY-MM'), TO_CHAR(ADD_TO_DATE(claim_loss_date, 'DD', v_claim_occurrence_num), 'YYYYMMDD'), i_s3p_claim_num),
	-- i_InsuranceLine='CR',
	-- i_pol_num || TO_CHAR(claim_loss_date,'YYMMDD') || SUBSTR(i_claim_occurrence_num,2,2),
	-- --i_InsuranceLine='GL',
	-- i_s3p_claim_num
	-- )
	DECODE(
	    TRUE,
	    i_InsuranceLine = 'CF', IFF(
	        CAST(TRUNC(pol_eff_date, 'MONTH') AS TIMESTAMP_NTZ(0)) <= TO_TIMESTAMP('2003-10', 'YYYY-MM'),
	        TO_CHAR(DATEADD(DAY,v_claim_occurrence_num,claim_loss_date), 'YYYYMMDD'),
	        i_s3p_claim_num
	    ),
	    i_InsuranceLine = 'CR', i_pol_num || TO_CHAR(claim_loss_date, 'YYMMDD') || SUBSTR(i_claim_occurrence_num, 2, 2),
	    i_s3p_claim_num
	) AS v_ClaimNumber,
	EXP_Cleansing_Loss.o_ClaimNum AS i_ClaimNum,
	i_ClaimNum AS ClaimNum,
	EXP_Cleansing_Loss.o_claimant_num AS claimant_num,
	0.00 AS PremiumMasterPremium,
	EXP_Cleansing_Loss.o_PaidLossAmount AS PaidLossAmount,
	EXP_Cleansing_Loss.o_OutstandingLossAmount AS OutstandingLossAmount,
	EXP_Cleansing_Loss.o_TypeBureauCode AS TypeBureauCode,
	EXP_Cleansing_Loss.o_RiskUnitGroup AS RiskUnitGroup,
	EXP_Cleansing_Loss.o_PolicySourceID AS PolicySourceID,
	EXP_Cleansing_Loss.o_RiskType AS RiskType,
	EXP_Cleansing_Loss.BureauCode1,
	EXP_Cleansing_Loss.BureauCode2,
	EXP_Cleansing_Loss.BureauCode4,
	EXP_Reset_Pms_ConstCode_IsoPPC.o_ConsturctionCode AS i_ConstructionCode,
	EXP_Reset_Pms_ConstCode_IsoPPC.o_IsoFireProtectionCode AS i_IsoFireProtectionCode,
	EXP_Cleansing_Loss.CoverageCode AS i_CoverageCode,
	i_CoverageCode AS CoverageCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ConstructionCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_ConstructionCode) AS ConstructionCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_IsoFireProtectionCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_IsoFireProtectionCode) AS IsoFireProtectionCode,
	EXP_Cleansing_Loss.o_pol_exp_date AS pol_exp_date,
	EXP_Cleansing_Loss.o_InceptionToDatePaidLossAmount AS InceptionToDatePaidLossAmount,
	EXP_Cleansing_Loss.o_AnnualStatementLineNumber AS AnnualStatementLineNumber,
	EXP_Cleansing_Loss.InsuranceSegmentCode,
	0 AS exposure,
	EXP_Cleansing_Loss.direct_alae_paid_including_recoveries,
	EXP_Cleansing_Loss.direct_loss_outstanding_excluding_recoveries,
	EXP_Cleansing_Loss.CoverageLimitValue AS InsuranceLineLimit,
	EXP_Cleansing_Loss.PolicyPerOccurenceLimit AS PolicyLimit,
	EXP_Cleansing_Loss.o_ZipPostalCode AS ZipPostalCode,
	'N/A' AS o_ExposureBasis,
	EXP_Reset_Pms_ConstCode_IsoPPC.o_SprinklerFlag AS SprinklerFlag,
	'N/A' AS LocationNumber,
	'N/A' AS BuildingNumber,
	EXP_Cleansing_Loss.PremiumTransactionEffectiveDate AS PT_EffectiveDate,
	EXP_Cleansing_Loss.PremiumTransactionAKID
	FROM EXP_Cleansing_Loss
	 -- Manually join with EXP_Reset_Pms_ConstCode_IsoPPC
	LEFT JOIN LKP_CauseOfLoss
	ON LKP_CauseOfLoss.CauseOfLoss = EXP_Cleansing_Loss.o_cause_of_loss AND LKP_CauseOfLoss.LineOfBusiness = EXP_Cleansing_Loss.o_pms_pol_lob_code AND LKP_CauseOfLoss.MajorPeril = EXP_Cleansing_Loss.MajorPerilCode
),
SQ_Premium AS (
	declare @QuarterDate as datetime
	
	set @QuarterDate=cast(DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1) as date);
	
	SELECT distinct
	PMC.PremiumMasterCalculationID,
	PMC.PremiumMasterRunDate,
	POL.pol_key,
	POL.prim_bus_class_code,
	RL.StateProvinceCode,
	PT.PremiumTransactionBookedDate,
	PMC.PremiumMasterSubLine,
	RC.ClassCode,
	PTRR.RatingTerritoryCode as RiskTerritory,
	POL.pol_eff_date,
	PMC.PremiumMasterPremium,
	'BE' TypeBureauCode,
	'N/A' AS RiskUnitGroup,
	PT.SourceSystemID,
	PMC.PremiumMasterTransactionCode,
	PMC.PremiumMasterReasonAmendedCode,
	RC.RiskType,
	SCC.StandardConstructionCodeDescription AS ConstructionCode,
	CDCP. IsoFireProtectionCode,
	'N/A' AS BureauCode1,
	'N/A' AS BureauCode2,
	'N/A' AS BureauCode4,
	POL.pol_exp_date,
	(case when ASL.asl_code='80' then '5.1'
	when ASL.asl_code='100' then '5.2'
	else
	ASL.asl_num
	end) asl_num,
	ISG.InsuranceSegmentCode,
	PMC.PremiumMasterExposure,
	LTRIM(RTRIM(IRC.CoverageCode)),
	PL.PolicyPerOccurenceLimit,
	RL.ZipPostalCode,
	CDCP.SprinklerFlag,
	PT.ExposureBasis,
	PT.PremiumTransactionEffectiveDate,
	--added LocationNumber as part of 12020
	RL.Locationunitnumber,
	-- added BuildingNumber as part of 12020
	RC.SubLocationUnitNumber,
	PMC.PremiumTransactionAKID
	from @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterFact PMF
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_dim ASL
	on PMF.AnnualStatementLineDimId=ASL.asl_dim_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation PMC
	on PMF.EDWPremiumMasterCalculationPKID=PMC.PremiumMasterCalculationId
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	on PT.PremiumTransactionAKID=PMC.PremiumTransactionAKID
	and PMC.CurrentSnapshotFlag=1
	and PT.CurrentSnapshotFlag=1
	join  @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	on PT.RatingCoverageAKID=RC.RatingCoverageAKID
	and RC.EffectiveDate=PT.EffectiveDate 
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	on PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
	and PC.CurrentSnapshotFlag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	on PC.RiskLocationAKID=RL.RiskLocationAKID
	and RL.CurrentSnapshotFlag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL
	on POL.pol_ak_id=RL.PolicyAKID
	and POL.crrnt_snpsht_flag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISG
	on POL.InsuranceSegmentAKId=ISG.InsuranceSegmentAKId
	and ISG.CurrentSnapshotFlag=1 
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim IRC
	on IRC.InsuranceReferenceCoverageDimId=PMF.InsuranceReferenceCoverageDimId
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.policyLimit PL
	on PC.PolicyAKID = Pl.PolicyAKId and PC.InsuranceLine =PL.InsuranceLine and PMC.PremiumMasterRunDate between PL.EffectiveDate and PL.ExpirationDate
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialProperty CDCP
	ON CDCP.PremiumTransactionID=PT.PremiumTransactionID
	AND CDCP.CurrentSnapshotFlag=1
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupConstructionCode SCC
	ON PT.ConstructionCode =SCC.ConstructionCode
	AND  SCC.CurrentSnapshotFlag=1
	left join PremiumTransactionRatingRisk PTRR  with (nolock) 
	on PTRR.PremiumTransactionAKID=PT.PremiumTransactionAKID
	WHERE
	(DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,PMC.PremiumMasterRunDate),-1) = @QuarterDate)
	and PC.InsuranceLine = 'BusinessOwners'
	AND PT.SourceSystemID='DCT'
	--AND RL.StateProvinceCode IN ('12','13','14','15','16','21','22','24','34','48')
	AND PMC.PremiumMasterPremium <>0
	AND PMC.PremiumMasterPremiumType='D'
	AND PMC.PremiumMasterTransactionCode IN ('10','11','12','13','14','15','18','19','20','21','22','23','24','25','28','29','30','31','57','67') 
	AND PMC.PremiumMasterReasonAmendedCode NOT IN ('CWO', 'CWB')
	@{pipeline().parameters.WHERE_CLAUSE_1}
	
	/*Removed the sql and union using WorkBlanketPremiumBreakOut table for PROD-20016*/
),
LKP_ISSWorkTable_Premium AS (
	SELECT
	EDWPremiumMasterCalculationPKId,
	PremiumMasterDirectWrittenPremiumAmount
	FROM (
		SELECT 
			EDWPremiumMasterCalculationPKId,
			PremiumMasterDirectWrittenPremiumAmount
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.ISSBusinessOwnersExtract
		WHERE (DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,PremiumMasterRunDate),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1))
		and EDWPremiumMasterCalculationPKId<>-1
		
		
		
		--YEAR(PremiumMasterRunDate)=YEAR(dateadd(year,@{pipeline().parameters.NO_OF_YEARS},GETDATE())) and EDWPremiumMasterCalculationPKId<>-1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWPremiumMasterCalculationPKId,PremiumMasterDirectWrittenPremiumAmount ORDER BY EDWPremiumMasterCalculationPKId) = 1
),
FIL_Exists_Premium AS (
	SELECT
	LKP_ISSWorkTable_Premium.EDWPremiumMasterCalculationPKId AS LKP_PremiumMasterCalculationID, 
	SQ_Premium.PremiumMasterCalculationID, 
	SQ_Premium.PremiumMasterRunDate, 
	SQ_Premium.pol_key, 
	SQ_Premium.prim_bus_class_code, 
	SQ_Premium.StateProvinceCode, 
	SQ_Premium.PremiumTransactionBookedDate, 
	SQ_Premium.PremiumMasterSubLine, 
	SQ_Premium.ClassCode, 
	SQ_Premium.RiskTerritory, 
	SQ_Premium.pol_eff_date, 
	SQ_Premium.PremiumMasterPremium, 
	SQ_Premium.TypeBureauCode, 
	SQ_Premium.RiskUnitGroup, 
	SQ_Premium.SourceSystemID, 
	SQ_Premium.PremiumMasterTransactionCode, 
	SQ_Premium.PremiumMasterReasonAmendedCode, 
	SQ_Premium.RiskType, 
	SQ_Premium.ConstructionCode, 
	SQ_Premium.IsoFireProtectionCode, 
	SQ_Premium.BureauCode1, 
	SQ_Premium.BureauCode2, 
	SQ_Premium.BureauCode4, 
	SQ_Premium.pol_exp_date, 
	SQ_Premium.asl_num, 
	SQ_Premium.InsuranceSegmentCode, 
	SQ_Premium.PremiumMasterExposure, 
	SQ_Premium.CoverageCode, 
	SQ_Premium.PolicyPerOccurenceLimit, 
	SQ_Premium.ZipPostalCode, 
	SQ_Premium.SprinklerFlag, 
	SQ_Premium.ExposureBasis, 
	SQ_Premium.PremiumTransactionEffectiveDate, 
	SQ_Premium.LocationNumber, 
	SQ_Premium.BuildingNumber, 
	SQ_Premium.PremiumTransactionAKID
	FROM SQ_Premium
	LEFT JOIN LKP_ISSWorkTable_Premium
	ON LKP_ISSWorkTable_Premium.EDWPremiumMasterCalculationPKId = SQ_Premium.PremiumMasterCalculationID AND LKP_ISSWorkTable_Premium.PremiumMasterDirectWrittenPremiumAmount = SQ_Premium.PremiumMasterPremium
	WHERE ISNULL(LKP_PremiumMasterCalculationID)
-------------------Filter Premium is 0-------------------------
AND ROUND(PremiumMasterPremium,2)<>0
--------------------EDWP-4085---------------------------------
and  NOT  IN(LTRIM(RTRIM(CoverageCode)),'MINE','COMPATT','CYBEXTEXP','NETSECLIAB','EXTREP')

---CoverageCode != 'MINE'

---Excluding coverage code COMPATT,CYBEXTEXP,NETSECLIAB,EXTREP as part of Cyber One project Requirement
),
EXP_Cleansing_Premium AS (
	SELECT
	PremiumMasterCalculationID AS i_PremiumMasterCalculationID,
	PremiumMasterRunDate AS i_PremiumMasterRunDate,
	pol_key AS i_pol_key,
	prim_bus_class_code AS i_prim_bus_class_code,
	StateProvinceCode AS i_StateProvinceCode,
	PremiumTransactionBookedDate AS i_PremiumTransactionBookedDate,
	PremiumMasterSubLine AS i_PremiumMasterSubLine,
	ClassCode AS i_ClassCode,
	RiskTerritory AS i_RiskTerritory,
	pol_eff_date AS i_pol_eff_date,
	PremiumMasterPremium AS i_PremiumMasterPremium,
	TypeBureauCode AS i_TypeBureauCode,
	RiskUnitGroup AS i_RiskUnitGroup,
	SourceSystemID AS i_SourceSystemID,
	PremiumMasterTransactionCode AS i_PremiumMasterTransactionCode,
	PremiumMasterReasonAmendedCode AS i_PremiumMasterReasonAmendedCode,
	RiskType AS i_RiskType,
	ConstructionCode AS i_ConstructionCode,
	IsoFireProtectionCode AS i_IsoFireProtectionCode,
	BureauCode1 AS i_BureauCode1,
	pol_exp_date AS i_pol_exp_date,
	asl_num AS i_asl_num,
	ExposureBasis AS i_ExposureBasis,
	-- *INF*: IIF(i_PremiumMasterCalculationID<0,-1,i_PremiumMasterCalculationID)
	IFF(i_PremiumMasterCalculationID < 0, - 1, i_PremiumMasterCalculationID) AS o_PremiumMasterCalculationID,
	i_PremiumMasterRunDate AS o_PremiumMasterRunDate,
	-- *INF*: RTRIM(LTRIM(i_pol_key))
	RTRIM(LTRIM(i_pol_key)) AS o_pol_key,
	-- *INF*: RTRIM(LTRIM(i_prim_bus_class_code))
	RTRIM(LTRIM(i_prim_bus_class_code)) AS o_prim_bus_class_code,
	-- *INF*: RTRIM(LTRIM(i_StateProvinceCode))
	RTRIM(LTRIM(i_StateProvinceCode)) AS o_StateProvinceCode,
	i_PremiumTransactionBookedDate AS o_PremiumTransactionBookedDate,
	-- *INF*: RTRIM(LTRIM(i_PremiumMasterSubLine))
	RTRIM(LTRIM(i_PremiumMasterSubLine)) AS o_PremiumMasterSubLine,
	-- *INF*: RTRIM(LTRIM(i_ClassCode))
	RTRIM(LTRIM(i_ClassCode)) AS o_ClassCode,
	-- *INF*: RTRIM(LTRIM(i_RiskTerritory))
	RTRIM(LTRIM(i_RiskTerritory)) AS o_RiskTerritory,
	i_pol_eff_date AS o_pol_eff_date,
	-- *INF*: IIF( IN(i_PremiumMasterTransactionCode, '10','11','12','13','14','15','18','19','20','21','22','23','24','25','28','29','30','31','57','67') AND  NOT IN(i_PremiumMasterReasonAmendedCode, 'COL' , 'CWO'), i_PremiumMasterPremium, 0)
	IFF(
	    i_PremiumMasterTransactionCode IN ('10','11','12','13','14','15','18','19','20','21','22','23','24','25','28','29','30','31','57','67')
	    and NOT i_PremiumMasterReasonAmendedCode IN ('COL','CWO'),
	    i_PremiumMasterPremium,
	    0
	) AS o_PremiumMasterPremium,
	-- *INF*: RTRIM(LTRIM(i_TypeBureauCode))
	RTRIM(LTRIM(i_TypeBureauCode)) AS o_TypeBureauCode,
	-- *INF*: RTRIM(LTRIM(i_RiskUnitGroup))
	RTRIM(LTRIM(i_RiskUnitGroup)) AS o_RiskUnitGroup,
	-- *INF*: LTRIM(RTRIM(i_SourceSystemID))
	LTRIM(RTRIM(i_SourceSystemID)) AS o_SourceSystemID,
	-- *INF*: LTRIM(RTRIM(i_RiskType))
	LTRIM(RTRIM(i_RiskType)) AS o_RiskType,
	-- *INF*: RTRIM(LTRIM(i_ConstructionCode))
	RTRIM(LTRIM(i_ConstructionCode)) AS o_ConstructionCode,
	-- *INF*: RTRIM(LTRIM(i_IsoFireProtectionCode))
	RTRIM(LTRIM(i_IsoFireProtectionCode)) AS o_IsoFireProtectionCode,
	-- *INF*: RTRIM(LTRIM(i_BureauCode1))
	RTRIM(LTRIM(i_BureauCode1)) AS o_BureauCode1,
	BureauCode2,
	BureauCode4,
	i_pol_exp_date AS o_pol_exp_date,
	i_asl_num AS o_AnnualStatementLineNumber,
	InsuranceSegmentCode,
	PremiumMasterExposure,
	CoverageCode,
	PolicyPerOccurenceLimit,
	ZipPostalCode AS i_ZipPostalCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ZipPostalCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_ZipPostalCode) AS o_ZipPostalCode,
	SprinklerFlag,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ExposureBasis)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_ExposureBasis) AS o_ExposureBasis,
	PremiumTransactionEffectiveDate,
	LocationNumber,
	BuildingNumber,
	PremiumTransactionAKID
	FROM FIL_Exists_Premium
),
EXP_Logic_Premium AS (
	SELECT
	-1 AS LossMasterCalculationId,
	o_PremiumMasterCalculationID AS PremiumMasterCalculationID,
	o_PremiumMasterRunDate AS PremiumMasterRunDate,
	-- *INF*: TO_DATE('1800-01-01','YYYY/MM/DD')
	TO_TIMESTAMP('1800-01-01', 'YYYY/MM/DD') AS loss_master_run_date,
	o_pol_key AS pol_key,
	o_prim_bus_class_code AS prim_bus_class_code,
	o_StateProvinceCode AS StateProvinceCode,
	o_PremiumTransactionBookedDate AS PremiumTransactionBookedDate,
	o_PremiumMasterSubLine AS PremiumMasterSubLine,
	-- *INF*: DECODE(TRUE,PremiumMasterSubLine='025','027',PremiumMasterSubLine)
	DECODE(
	    TRUE,
	    PremiumMasterSubLine = '025', '027',
	    PremiumMasterSubLine
	) AS sub_line_code_out,
	o_ClassCode AS ClassCode,
	-- *INF*: IIF(ISNULL(ClassCode) OR IS_SPACES(ClassCode) OR LENGTH(ClassCode)=0 OR IN(ClassCode, 'N/A','TBD'),
	-- '00000',
	-- LPAD(ClassCode, 5, '0')
	-- )
	-- 
	-- 
	-- --IIF(ISNULL(ClassCode) OR LENGTH(ClassCode)=0,'N/A',ClassCode)
	IFF(
	    ClassCode IS NULL
	    or LENGTH(ClassCode)>0
	    and TRIM(ClassCode)=''
	    or LENGTH(ClassCode) = 0
	    or ClassCode IN ('N/A','TBD'),
	    '00000',
	    LPAD(ClassCode, 5, '0')
	) AS PremiumMasterClassCode,
	'N/A' AS LossMasterClassCode,
	'N/A' AS Cause_of_Loss,
	o_RiskTerritory AS RiskTerritory,
	-- *INF*: RiskTerritory
	-- 
	-- --SUBSTR(RiskTerritory,2,2)
	RiskTerritory AS TerritoryCode,
	o_pol_eff_date AS pol_eff_date,
	'N/A' AS ClaimNum,
	'N/A' AS ClaimantNum,
	o_PremiumMasterPremium AS PremiumMasterPremium,
	-- *INF*: ROUND(PremiumMasterPremium,2)
	ROUND(PremiumMasterPremium, 2) AS PremiumMasterPremium_out,
	0.00 AS PaidLossAmt,
	0.00 AS OutstandingAmt,
	o_TypeBureauCode AS TypeBureauCode,
	o_RiskUnitGroup AS RiskUnitGroup,
	o_SourceSystemID AS SourceSystemID,
	o_RiskType AS RiskType,
	o_BureauCode1 AS BureauCode1,
	BureauCode2,
	BureauCode4,
	CoverageCode,
	o_ConstructionCode AS ConstructionCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(ConstructionCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(ConstructionCode) AS ConstructionCode_out,
	o_IsoFireProtectionCode AS IsoFireProtectionCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(IsoFireProtectionCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(IsoFireProtectionCode) AS IsoFireProtectionCode_out,
	o_pol_exp_date AS pol_exp_date,
	o_AnnualStatementLineNumber AS AnnualStatementLineNumber,
	InsuranceSegmentCode,
	PremiumMasterExposure,
	-- *INF*: '0'
	-- --This is hardcoded to 0 zero as the coverage limit has been removed from Src and then made it as lookup
	'0' AS InsuranceLineLimit,
	PolicyPerOccurenceLimit,
	o_ZipPostalCode AS ZipPostalCode,
	SprinklerFlag,
	o_ExposureBasis AS ExposureBasis,
	PremiumTransactionEffectiveDate,
	LocationNumber,
	BuildingNumber,
	PremiumTransactionAKID
	FROM EXP_Cleansing_Premium
),
Union AS (
	SELECT LossMasterCalculationId, PremiumMasterCalculationID, PremiumMasterRunDate, loss_master_run_date, pol_key, prim_bus_class_code, StateProvinceCode, sub_line_code_out AS sub_line_code, PremiumMasterClassCode, LossMasterClassCode, Cause_of_Loss, TerritoryCode, pol_eff_date, ClaimNum, ClaimantNum, PremiumMasterPremium_out AS PremiumMasterPremium, PaidLossAmt, OutstandingAmt, TypeBureauCode, RiskUnitGroup, SourceSystemID, RiskType, CoverageCode, ConstructionCode_out AS ConstructionCode, IsoFireProtectionCode_out AS IsoFireProtectionCode, pol_exp_date AS PolicyExpirationDate, AnnualStatementLineNumber, BureauCode1, BureauCode2, BureauCode4, InsuranceSegmentCode, PremiumMasterExposure, PolicyPerOccurenceLimit AS PolicyLimit, ZipPostalCode, SprinklerFlag, ExposureBasis, InsuranceLineLimit, PremiumTransactionEffectiveDate, LocationNumber, BuildingNumber, PremiumTransactionAKID
	FROM EXP_Logic_Premium
	UNION
	SELECT loss_master_calculation_id AS LossMasterCalculationId, PremiumMasterCalculationID, PremiumMasterRunDate, loss_master_run_date, pol_key, prim_bus_class_code, StateProvinceCode, sub_line_code_out AS sub_line_code, PremiumMasterClassCode_out AS PremiumMasterClassCode, LossMasterClassCode_out AS LossMasterClassCode, cause_of_loss_out AS Cause_of_Loss, TerritoryCode_out AS TerritoryCode, pol_eff_date, ClaimNum, claimant_num AS ClaimantNum, PremiumMasterPremium, PaidLossAmount AS PaidLossAmt, OutstandingLossAmount AS OutstandingAmt, TypeBureauCode, RiskUnitGroup, PolicySourceID AS SourceSystemID, RiskType, CoverageCode, ConstructionCode, IsoFireProtectionCode, pol_exp_date AS PolicyExpirationDate, InceptionToDatePaidLossAmount, claim_coverage_detail_ak_id AS ClaimCoverageID, AnnualStatementLineNumber, BureauCode1, BureauCode2, BureauCode4, InsuranceSegmentCode, exposure AS PremiumMasterExposure, claim_loss_date, direct_alae_paid_including_recoveries, direct_loss_outstanding_excluding_recoveries, PolicyLimit, ZipPostalCode, SprinklerFlag, o_ExposureBasis AS ExposureBasis, InsuranceLineLimit, LocationNumber, BuildingNumber, PremiumTransactionAKID
	FROM EXP_Logic_Loss
),
EXP_ConstCode_IsoPC_Rules AS (
	SELECT
	sub_line_code AS i_sub_line_code,
	ConstructionCode AS i_ConstructionCode,
	CoverageCode AS i_CoverageCode,
	IsoFireProtectionCode AS i_IsoFireProtectionCode,
	-- *INF*: DECODE
	-- (TRUE,
	-- in(i_ConstructionCode,'N/A',null),'N/A',
	-- i_ConstructionCode
	-- )
	DECODE(
	    TRUE,
	    i_ConstructionCode IN ('N/A',null), 'N/A',
	    i_ConstructionCode
	) AS v_ConstructionCode,
	-- *INF*: i_IsoFireProtectionCode
	-- 
	-- --DECODE(TRUE,
	-- --i_IsoFireProtectionCode='N/A' and in--(i_sub_line_code,'010','015','016','017','018'),'10',
	-- --i_IsoFireProtectionCode='N/A', '00',
	-- --i_IsoFireProtectionCode='1', '01',
	-- --in (i_IsoFireProtectionCode,'2','20'),'02',
	-- --in (i_IsoFireProtectionCode,'3','13','30'),'03',
	-- --i_IsoFireProtectionCode='4','04',
	-- --i_IsoFireProtectionCode='5','05',
	-- --i_IsoFireProtectionCode='6','06',
	-- --i_IsoFireProtectionCode='7','07',
	-- --i_IsoFireProtectionCode='8','08',
	-- --in(i_IsoFireProtectionCode,'9','92','97'),'09',
	-- --i_IsoFireProtectionCode='12','10',
	-- --in(i_IsoFireProtectionCode,'OR','O4'),'04',
	-- --i_IsoFireProtectionCode='8B','19',
	-- --i_IsoFireProtectionCode='96','06',
	-- --i_IsoFireProtectionCode
	-- --)
	-- 
	i_IsoFireProtectionCode AS v_IsoFireProtectionCode,
	-- *INF*: iif(in(i_CoverageCode,'BLDG','BLDGFUNCVAL','BLDRSK','BLKBC',
	-- 'BLKBLDG','BLKCON','BUSPTY','FUNCBPP','HYDROWC','IMPROV','IMPROVMNT',
	-- 'LLBBLDG','LLBPPTY','MISCPROP','MISCREALPROP','PERPTY','PPTYO'),i_IsoFireProtectionCode,'00')
	IFF(
	    i_CoverageCode IN ('BLDG','BLDGFUNCVAL','BLDRSK','BLKBC','BLKBLDG','BLKCON','BUSPTY','FUNCBPP','HYDROWC','IMPROV','IMPROVMNT','LLBBLDG','LLBPPTY','MISCPROP','MISCREALPROP','PERPTY','PPTYO'),
	    i_IsoFireProtectionCode,
	    '00'
	) AS v_IsoFireProtectionCode_BO,
	v_ConstructionCode AS o_ConstructionCode,
	v_IsoFireProtectionCode_BO AS o_IsoFireProtectionCode
	FROM Union
),
EXP_GetLimits AS (
	SELECT
	PremiumTransactionAKID AS i_PremiumTransactionAKID,
	CoverageCode AS i_CoverageCode,
	-- *INF*: DECODE(TRUE, i_LossMasterCalculationId = -1 AND UPPER(i_CoverageCode)='BLKBC',:LKP.LKP_BLANKETCOVERAGELIMIT(i_PremiumTransactionAKID),
	-- i_LossMasterCalculationId = -1 AND  IN ( UPPER(i_CoverageCode),'BLDG','BLDGFUNCVAL','BLDRSK','BLKBLDG','BLKCON','BUSPTY','FUNCBPP','HYDROWC','IMPROV','IMPROVMNT','LLBBLDG','LLBPPTY','MISCPROP','MISCREALPROP','PERPTY','PPTYO')=1, :LKP.LKP_COVERAGELIMITNONBLANKET(i_PremiumTransactionAKID),
	-- i_LossMasterCalculationId != -1, i_InsuranceLineLimit,
	-- '0')
	-- 
	-- 
	DECODE(
	    TRUE,
	    i_LossMasterCalculationId = - 1 AND UPPER(i_CoverageCode) = 'BLKBC', LKP_BLANKETCOVERAGELIMIT_i_PremiumTransactionAKID.BlanketLimit,
	    i_LossMasterCalculationId = - 1 AND UPPER(i_CoverageCode) IN ('BLDG','BLDGFUNCVAL','BLDRSK','BLKBLDG','BLKCON','BUSPTY','FUNCBPP','HYDROWC','IMPROV','IMPROVMNT','LLBBLDG','LLBPPTY','MISCPROP','MISCREALPROP','PERPTY','PPTYO') = 1, LKP_COVERAGELIMITNONBLANKET_i_PremiumTransactionAKID.NonBlanketLimit,
	    i_LossMasterCalculationId != - 1, i_InsuranceLineLimit,
	    '0'
	) AS v_LookupLimt,
	-- *INF*: IIF(ISNULL(v_LookupLimt),'0',v_LookupLimt)
	IFF(v_LookupLimt IS NULL, '0', v_LookupLimt) AS o_InsuranceLineLimit,
	LossMasterCalculationId AS i_LossMasterCalculationId,
	InsuranceLineLimit AS i_InsuranceLineLimit
	FROM Union
	LEFT JOIN LKP_BLANKETCOVERAGELIMIT LKP_BLANKETCOVERAGELIMIT_i_PremiumTransactionAKID
	ON LKP_BLANKETCOVERAGELIMIT_i_PremiumTransactionAKID.PremiumTransactionAKID = i_PremiumTransactionAKID

	LEFT JOIN LKP_COVERAGELIMITNONBLANKET LKP_COVERAGELIMITNONBLANKET_i_PremiumTransactionAKID
	ON LKP_COVERAGELIMITNONBLANKET_i_PremiumTransactionAKID.PremiumTransactionAKID = i_PremiumTransactionAKID

),
EXP_Values AS (
	SELECT
	Union.LossMasterCalculationId AS i_LossMasterCalculationId,
	Union.PremiumMasterCalculationID AS i_PremiumMasterCalculationID,
	Union.PremiumMasterRunDate AS i_PremiumMasterRunDate,
	Union.loss_master_run_date AS i_LossMasterRunDate,
	Union.pol_key AS i_pol_key,
	Union.prim_bus_class_code AS i_prim_bus_class_code,
	Union.StateProvinceCode AS i_StateProvinceCode,
	Union.sub_line_code AS i_sub_line_code,
	Union.PremiumMasterClassCode AS i_PremiumMasterClassCode,
	Union.LossMasterClassCode AS i_LossMasterClassCode,
	Union.Cause_of_Loss AS i_Cause_of_Loss,
	Union.TerritoryCode AS i_TerritoryCode,
	Union.pol_eff_date AS i_pol_eff_date,
	Union.ClaimNum AS i_ClaimNum,
	Union.ClaimantNum AS i_ClaimantNum,
	Union.PremiumMasterPremium AS i_PremiumMasterPremium,
	Union.PaidLossAmt AS i_PaidLossAmt,
	Union.OutstandingAmt AS i_OutstandingAmt,
	Union.TypeBureauCode AS i_TypeBureauCode,
	Union.SourceSystemID AS i_SourceSystemID,
	Union.RiskType AS i_RiskType,
	EXP_ConstCode_IsoPC_Rules.i_CoverageCode,
	EXP_ConstCode_IsoPC_Rules.o_ConstructionCode AS i_ConstructionCode,
	EXP_ConstCode_IsoPC_Rules.o_IsoFireProtectionCode AS i_IsoFireProtectionCode,
	Union.PolicyExpirationDate AS i_PolicyExpirationDate,
	Union.InceptionToDatePaidLossAmount AS i_InceptionToDatePaidLossAmount,
	Union.ClaimCoverageID AS i_ClaimCoverageID,
	Union.AnnualStatementLineNumber AS i_AnnualStatementLineNumber,
	Union.BureauCode1 AS i_BureauCode1,
	Union.BureauCode2 AS i_BureauCode2,
	Union.BureauCode4 AS i_BureauCode4,
	Union.InsuranceSegmentCode AS i_InsuranceSegmentCode,
	Union.PremiumMasterExposure AS i_PremiumMasterExposure,
	Union.claim_loss_date AS i_claim_loss_date,
	Union.direct_alae_paid_including_recoveries AS i_direct_alae_paid_including_recoveries,
	Union.direct_loss_outstanding_excluding_recoveries AS i_direct_loss_outstanding_excluding_recoveries,
	Union.PolicyLimit AS i_PolicyLimit,
	Union.ZipPostalCode AS i_ZipPostalCode,
	Union.SprinklerFlag AS i_SprinklerFlag,
	Union.ExposureBasis AS i_ExposureBasis,
	EXP_GetLimits.o_InsuranceLineLimit AS i_InsuranceLineLimit,
	Union.PremiumTransactionEffectiveDate AS i_PremiumTransactionEffectiveDate,
	Union.LocationNumber AS i_LocationNUmber,
	Union.BuildingNumber AS i_BuildingNumber,
	-- *INF*: DECODE(TRUE,
	-- i_TypeBureauCode='Property',
	-- DECODE(TRUE,
	-- i_RiskType='BLDG','01',
	-- i_RiskType='PP','02',
	-- 'N/A'
	-- ),
	-- 
	-- i_TypeBureauCode='Crime',
	-- DECODE(TRUE,
	-- i_RiskType='ClientsProperty','200',
	-- i_RiskType='ClientsProperty_ETF','400',
	-- i_RiskType='ComputerFraud','200',
	-- i_RiskType='ComputerFraud_G','300',
	-- i_RiskType='EmployeeTheft','200',
	-- i_RiskType='EmployeeTheft_ETF','400',
	-- i_RiskType='EmployeeTheftNameOrPosition','200',
	-- i_RiskType='EmployeeTheftNameOrPosition_ETF','400',
	-- i_RiskType='EmployeeTheftNameOrPosition_G','300',
	-- i_RiskType='EmployeeTheftNameOrPosition_GETF','400',
	-- i_RiskType='EmployeeTheftPerEmployee','300',
	-- i_RiskType='EmployeeTheftPerEmployee_GETF','400',
	-- i_RiskType='EmployeeTheftPerLoss','300',
	-- i_RiskType='EmployeeTheftPerLoss_GETF','400',
	-- i_RiskType='ForgeryAndAlteration','200',
	-- i_RiskType='ForgeryAndAlteration_ETF','400',
	-- i_RiskType='ForgeryAndAlteration_G','300',
	-- i_RiskType='ForgeryAndAlteration_GETF','400',
	-- i_RiskType='FundsTransfer','200',
	-- i_RiskType='FundsTransfer_G','300',
	-- i_RiskType='GuestPropertyInsidePremises','200',
	-- i_RiskType='GuestPropertySafeDeposit','200',
	-- i_RiskType='InsideRobbery','200',
	-- i_RiskType='InsideRobbery_G','300',
	-- i_RiskType='InsideRobberyOther','200',
	-- i_RiskType='InsideRobberyOther_G','300',
	-- i_RiskType='InsideRobberySecurities','200',
	-- i_RiskType='InsideRobberySecurities_G','300',
	-- i_RiskType='InsideTheftMoney','200',
	-- i_RiskType='InsideTheftMoney_G','300',
	-- i_RiskType='InsideTheftProperty','200',
	-- i_RiskType='InsideTheftProperty_G','300',
	-- i_RiskType='MoneyOrders','200',
	-- i_RiskType='OutsidePremises','200',
	-- i_RiskType='OutsidePremises_G','300',
	-- 'N/A'
	-- ),
	-- 'N/A')
	DECODE(
	    TRUE,
	    i_TypeBureauCode = 'Property', DECODE(
	        TRUE,
	        i_RiskType = 'BLDG', '01',
	        i_RiskType = 'PP', '02',
	        'N/A'
	    ),
	    i_TypeBureauCode = 'Crime', DECODE(
	        TRUE,
	        i_RiskType = 'ClientsProperty', '200',
	        i_RiskType = 'ClientsProperty_ETF', '400',
	        i_RiskType = 'ComputerFraud', '200',
	        i_RiskType = 'ComputerFraud_G', '300',
	        i_RiskType = 'EmployeeTheft', '200',
	        i_RiskType = 'EmployeeTheft_ETF', '400',
	        i_RiskType = 'EmployeeTheftNameOrPosition', '200',
	        i_RiskType = 'EmployeeTheftNameOrPosition_ETF', '400',
	        i_RiskType = 'EmployeeTheftNameOrPosition_G', '300',
	        i_RiskType = 'EmployeeTheftNameOrPosition_GETF', '400',
	        i_RiskType = 'EmployeeTheftPerEmployee', '300',
	        i_RiskType = 'EmployeeTheftPerEmployee_GETF', '400',
	        i_RiskType = 'EmployeeTheftPerLoss', '300',
	        i_RiskType = 'EmployeeTheftPerLoss_GETF', '400',
	        i_RiskType = 'ForgeryAndAlteration', '200',
	        i_RiskType = 'ForgeryAndAlteration_ETF', '400',
	        i_RiskType = 'ForgeryAndAlteration_G', '300',
	        i_RiskType = 'ForgeryAndAlteration_GETF', '400',
	        i_RiskType = 'FundsTransfer', '200',
	        i_RiskType = 'FundsTransfer_G', '300',
	        i_RiskType = 'GuestPropertyInsidePremises', '200',
	        i_RiskType = 'GuestPropertySafeDeposit', '200',
	        i_RiskType = 'InsideRobbery', '200',
	        i_RiskType = 'InsideRobbery_G', '300',
	        i_RiskType = 'InsideRobberyOther', '200',
	        i_RiskType = 'InsideRobberyOther_G', '300',
	        i_RiskType = 'InsideRobberySecurities', '200',
	        i_RiskType = 'InsideRobberySecurities_G', '300',
	        i_RiskType = 'InsideTheftMoney', '200',
	        i_RiskType = 'InsideTheftMoney_G', '300',
	        i_RiskType = 'InsideTheftProperty', '200',
	        i_RiskType = 'InsideTheftProperty_G', '300',
	        i_RiskType = 'MoneyOrders', '200',
	        i_RiskType = 'OutsidePremises', '200',
	        i_RiskType = 'OutsidePremises_G', '300',
	        'N/A'
	    ),
	    'N/A'
	) AS v_PolicyForm_DCT,
	-- *INF*: DECODE(TRUE,
	-- i_TypeBureauCode = 'CR',
	-- DECODE(TRUE,
	-- IN(i_BureauCode4,'01','02','03'),'200',
	-- IN(i_BureauCode4,'11','12','13'),'300',
	-- IN(i_BureauCode4,'21','22','23','27','28'),'400',
	-- '200'),
	-- 
	-- i_TypeBureauCode = 'BT',
	-- DECODE(TRUE,
	-- IN(i_BureauCode1||i_BureauCode2,'01','02','03','04','05','06','09'),'120',
	-- IN(i_BureauCode1||i_BureauCode2,'07','08'),'190',
	-- IN(i_BureauCode1||i_BureauCode2,'10','20','26'),'170',
	-- IN(i_BureauCode1||i_BureauCode2,'11','12','13','14','15','16','17','18','19','21','22','23','24','25','29','50'),'110',
	-- IN(i_BureauCode1||i_BureauCode2,'31','32','39'),'130',
	-- IN(i_BureauCode1||i_BureauCode2,'42','43','44','45'),'140',
	-- IN(i_BureauCode1||i_BureauCode2,'45'),'111',
	-- IN(i_BureauCode1||i_BureauCode2,'47'),'121',
	-- IN(i_BureauCode1||i_BureauCode2,'48'),'131',
	-- IN(i_BureauCode1||i_BureauCode2,'49','41','42','43','44'),'140',
	-- IN(i_BureauCode1||i_BureauCode2,'51','59'),'150',
	-- IN(i_BureauCode1||i_BureauCode2,'52','53'),'141',
	-- IN(i_BureauCode1||i_BureauCode2,'54','55'),'151',
	-- IN(i_BureauCode1||i_BureauCode2,'56'),'161',
	-- IN(i_BureauCode1||i_BureauCode2,'57'),'171',
	-- IN(i_BureauCode1||i_BureauCode2,'58'),'181',
	-- IN(i_BureauCode1||i_BureauCode2,'60'),'191',
	-- IN(i_BureauCode1||i_BureauCode2,'61'),'160',
	-- IN(i_BureauCode1||i_BureauCode2,'62','63'),'192',
	-- IN(i_BureauCode1||i_BureauCode2,'64'),'112',
	-- IN(i_BureauCode1||i_BureauCode2,'67','68'),'142',
	-- IN(i_BureauCode1||i_BureauCode2,'69'),'152',
	-- IN(i_BureauCode1||i_BureauCode2,'70','71','72'),'180',
	-- IN(i_BureauCode1||i_BureauCode2,'73'),'122',
	-- '199'
	-- ),
	-- i_TypeBureauCode = 'FT','199',
	-- 'N/A')
	DECODE(
	    TRUE,
	    i_TypeBureauCode = 'CR', DECODE(
	        TRUE,
	        i_BureauCode4 IN ('01','02','03'), '200',
	        i_BureauCode4 IN ('11','12','13'), '300',
	        i_BureauCode4 IN ('21','22','23','27','28'), '400',
	        '200'
	    ),
	    i_TypeBureauCode = 'BT', DECODE(
	        TRUE,
	        i_BureauCode1 || i_BureauCode2 IN ('01','02','03','04','05','06','09'), '120',
	        i_BureauCode1 || i_BureauCode2 IN ('07','08'), '190',
	        i_BureauCode1 || i_BureauCode2 IN ('10','20','26'), '170',
	        i_BureauCode1 || i_BureauCode2 IN ('11','12','13','14','15','16','17','18','19','21','22','23','24','25','29','50'), '110',
	        i_BureauCode1 || i_BureauCode2 IN ('31','32','39'), '130',
	        i_BureauCode1 || i_BureauCode2 IN ('42','43','44','45'), '140',
	        i_BureauCode1 || i_BureauCode2 IN ('45'), '111',
	        i_BureauCode1 || i_BureauCode2 IN ('47'), '121',
	        i_BureauCode1 || i_BureauCode2 IN ('48'), '131',
	        i_BureauCode1 || i_BureauCode2 IN ('49','41','42','43','44'), '140',
	        i_BureauCode1 || i_BureauCode2 IN ('51','59'), '150',
	        i_BureauCode1 || i_BureauCode2 IN ('52','53'), '141',
	        i_BureauCode1 || i_BureauCode2 IN ('54','55'), '151',
	        i_BureauCode1 || i_BureauCode2 IN ('56'), '161',
	        i_BureauCode1 || i_BureauCode2 IN ('57'), '171',
	        i_BureauCode1 || i_BureauCode2 IN ('58'), '181',
	        i_BureauCode1 || i_BureauCode2 IN ('60'), '191',
	        i_BureauCode1 || i_BureauCode2 IN ('61'), '160',
	        i_BureauCode1 || i_BureauCode2 IN ('62','63'), '192',
	        i_BureauCode1 || i_BureauCode2 IN ('64'), '112',
	        i_BureauCode1 || i_BureauCode2 IN ('67','68'), '142',
	        i_BureauCode1 || i_BureauCode2 IN ('69'), '152',
	        i_BureauCode1 || i_BureauCode2 IN ('70','71','72'), '180',
	        i_BureauCode1 || i_BureauCode2 IN ('73'), '122',
	        '199'
	    ),
	    i_TypeBureauCode = 'FT', '199',
	    'N/A'
	) AS v_PolicyForm_PMS,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS',v_PolicyForm_PMS,
	-- IN(i_SourceSystemID,'DCT','DUC'),v_PolicyForm_DCT
	-- )
	DECODE(
	    TRUE,
	    i_SourceSystemID = 'PMS', v_PolicyForm_PMS,
	    i_SourceSystemID IN ('DCT','DUC'), v_PolicyForm_DCT
	) AS v_PolicyForm,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	SYSDATE AS o_CreatedDate,
	i_PremiumMasterCalculationID AS o_PremiumMasterCalculationID,
	i_LossMasterCalculationId AS o_LossMasterCalculationId,
	i_TypeBureauCode AS o_TypeBureauCode,
	'09' AS o_BureauLineOfInsurance,
	-- *INF*: --Fix for EDWP-3967
	-- '0731'
	-- --'0761'
	'0731' AS o_BureauCompanyNumber,
	i_StateProvinceCode AS o_StateProvinceCode,
	i_PremiumMasterRunDate AS o_PremiumMasterRunDate,
	i_LossMasterRunDate AS o_LossMasterRunDate,
	i_pol_key AS o_pol_key,
	i_PremiumMasterClassCode AS o_PremiumMasterClassCode,
	i_LossMasterClassCode AS o_LossMasterClassCode,
	i_ClaimNum AS o_ClaimNum,
	i_ClaimantNum AS o_ClaimantNum,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_TerritoryCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_TerritoryCode) AS o_RiskTerritoryCode,
	i_pol_eff_date AS o_PolicyEffectiveDate,
	-- *INF*: i_Cause_of_Loss
	-- --DECODE(TRUE,IN(i_Cause_of_Loss,'11','21','31','41','51','61','71','81','91'),'01',
	-- --IN(i_Cause_of_Loss,'12','22','32','42','52','62','72','82','92','97'),'02',
	-- --IN(i_Cause_of_Loss,'05','15','25','35','45','55','65','75','85','95'),'03',
	-- --IN(i_Cause_of_Loss,'14','24','34','44','54','64','74','84','94'),'04',
	-- --IN(i_Cause_of_Loss,'08','18','28','38','48','58','68','88','98'),'05',
	-- --IN(i_Cause_of_Loss,'16','26','36','46','56','66','76','86','96'),'06',
	-- --IN(i_Cause_of_Loss,'17','27','37','47','57','67','87'),'07',
	-- --IN(i_Cause_of_Loss,'03','13','23','33','43','53','63','73','83','93'),'08',
	-- --IN(i_Cause_of_Loss,'19','29','39','49','59','69','77','79','89','99'),'09',i_Cause_of_Loss)
	i_Cause_of_Loss AS o_CauseOfLoss,
	i_CoverageCode AS o_CoverageCode,
	i_IsoFireProtectionCode AS o_ISOFireProtectionCode,
	'2' AS o_TypeOfPolicyForm,
	i_PremiumMasterPremium AS o_PremiumMasterDirectWrittenPremiumAmount,
	i_PaidLossAmt AS o_PaidLossAmount,
	i_OutstandingAmt AS o_OutstandingLossAmount,
	i_PolicyExpirationDate AS o_PolicyExpirationDate,
	-- *INF*: IIF(ISNULL(i_InceptionToDatePaidLossAmount), 0, i_InceptionToDatePaidLossAmount)
	IFF(i_InceptionToDatePaidLossAmount IS NULL, 0, i_InceptionToDatePaidLossAmount) AS o_InceptionToDatePaidLossAmount,
	-- *INF*: IIF(ISNULL(i_ClaimCoverageID), -1, i_ClaimCoverageID)
	IFF(i_ClaimCoverageID IS NULL, - 1, i_ClaimCoverageID) AS o_ClaimCoverageID,
	i_AnnualStatementLineNumber AS o_AnnualStatementLineNumber,
	-- *INF*: Decode(True,  isnull(i_PolicyLimit), 'N/A', Ltrim(rtrim(i_PolicyLimit)))
	Decode(
	    True,
	    i_PolicyLimit IS NULL, 'N/A',
	    Ltrim(rtrim(i_PolicyLimit))
	) AS o_PolicyLimit,
	-- *INF*: IIF( i_CoverageCode='RSKLIAB', i_ExposureBasis, 'N/A')
	-- 
	-- --'N/A'
	IFF(i_CoverageCode = 'RSKLIAB', i_ExposureBasis, 'N/A') AS o_ExposureBasis,
	-- *INF*: DECODE(TRUE, 
	-- NOT IN(i_CoverageCode, 'BLDG', 'BLDGFUNCVAL', 'BLDRSK', 'BLKBC', 'BLKBLDG', 'BLKCON', 'BUSPTY', 'FUNCBPP', 'HYDROWC', 'IMPROV', 'IMPROVMNT', 'LLBBLDG', 'LLBPPTY', 'MISCPROP', 'MISCREALPROP', 'PERPTY', 'PPTYO'), 'N/A',
	-- i_ConstructionCode)
	DECODE(
	    TRUE,
	    NOT i_CoverageCode IN ('BLDG','BLDGFUNCVAL','BLDRSK','BLKBC','BLKBLDG','BLKCON','BUSPTY','FUNCBPP','HYDROWC','IMPROV','IMPROVMNT','LLBBLDG','LLBPPTY','MISCPROP','MISCREALPROP','PERPTY','PPTYO'), 'N/A',
	    i_ConstructionCode
	) AS o_ConstructionCode,
	-- *INF*: DECODE(i_SprinklerFlag, 'T','YES',
	-- '1','YES',
	-- 'F','NO',
	-- '0','NO',
	-- 'N/A')
	DECODE(
	    i_SprinklerFlag,
	    'T', 'YES',
	    '1', 'YES',
	    'F', 'NO',
	    '0', 'NO',
	    'N/A'
	) AS v_SprinklerFlag,
	-- *INF*: DECODE(TRUE, 
	-- NOT IN(i_CoverageCode, 'BLDG', 'BLDGFUNCVAL', 'BLDRSK', 'BLKBC', 'BLKBLDG', 'BLKCON', 'BUSPTY', 'FUNCBPP', 'HYDROWC', 'IMPROV', 'IMPROVMNT', 'LLBBLDG', 'LLBPPTY', 'MISCPROP', 'MISCREALPROP', 'PERPTY', 'PPTYO'), 'N/A',
	-- v_SprinklerFlag)
	DECODE(
	    TRUE,
	    NOT i_CoverageCode IN ('BLDG','BLDGFUNCVAL','BLDRSK','BLKBC','BLKBLDG','BLKCON','BUSPTY','FUNCBPP','HYDROWC','IMPROV','IMPROVMNT','LLBBLDG','LLBPPTY','MISCPROP','MISCREALPROP','PERPTY','PPTYO'), 'N/A',
	    v_SprinklerFlag
	) AS o_SprinklerFlag,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_InsuranceLineLimit)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_InsuranceLineLimit) AS o_LimitOfInsurance,
	-- *INF*: IIF(ISNULL(i_PremiumMasterExposure), 0,i_PremiumMasterExposure )
	-- 
	IFF(i_PremiumMasterExposure IS NULL, 0, i_PremiumMasterExposure) AS v_PremiumMasterExposure_1,
	-- *INF*: IIF(i_CoverageCode = 'RSKLIAB',v_PremiumMasterExposure_1,0)
	IFF(i_CoverageCode = 'RSKLIAB', v_PremiumMasterExposure_1, 0) AS v_PremiumMasterExposure_2,
	-- *INF*: IIF(v_PremiumMasterExposure_2 > 0 AND i_PremiumMasterPremium < 0, (v_PremiumMasterExposure_2 * -1), v_PremiumMasterExposure_2)
	IFF(
	    v_PremiumMasterExposure_2 > 0 AND i_PremiumMasterPremium < 0,
	    (v_PremiumMasterExposure_2 * - 1),
	    v_PremiumMasterExposure_2
	) AS o_PremiumMasterExposure,
	-- *INF*: IIF(ISNULL(i_direct_alae_paid_including_recoveries),0,i_direct_alae_paid_including_recoveries)
	IFF(
	    i_direct_alae_paid_including_recoveries IS NULL, 0, i_direct_alae_paid_including_recoveries
	) AS o_PaidAllocatedLossAdjustmentExpenseAmount,
	-- *INF*: IIF(ISNULL(i_direct_loss_outstanding_excluding_recoveries),0,i_direct_loss_outstanding_excluding_recoveries)
	IFF(
	    i_direct_loss_outstanding_excluding_recoveries IS NULL, 0,
	    i_direct_loss_outstanding_excluding_recoveries
	) AS o_OutstandingAllocatedLossAdjustmentExpenseAmount,
	-- *INF*: IIF(ISNULL(i_claim_loss_date),TO_DATE('18000101','YYYYMMDD'),i_claim_loss_date)
	IFF(i_claim_loss_date IS NULL, TO_TIMESTAMP('18000101', 'YYYYMMDD'), i_claim_loss_date) AS o_ClaimLossDate,
	i_ZipPostalCode AS o_ZipPostalCode,
	-- *INF*: IIF(ISNULL(i_PremiumTransactionEffectiveDate),TO_DATE('18000101','YYYYMMDD'),i_PremiumTransactionEffectiveDate)
	IFF(
	    i_PremiumTransactionEffectiveDate IS NULL, TO_TIMESTAMP('18000101', 'YYYYMMDD'),
	    i_PremiumTransactionEffectiveDate
	) AS o_PremiumTransactionEffectiveDate1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_prim_bus_class_code)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_prim_bus_class_code) AS o_prim_bus_class_code,
	i_LocationNUmber AS o_LocationNumber,
	i_BuildingNumber AS o_BuildingNumber
	FROM EXP_ConstCode_IsoPC_Rules
	 -- Manually join with EXP_GetLimits
	 -- Manually join with Union
),
FIL_ASL AS (
	SELECT
	o_AuditID AS AuditID, 
	o_CreatedDate AS CreatedDate, 
	o_PremiumMasterCalculationID AS PremiumMasterCalculationID, 
	o_LossMasterCalculationId AS LossMasterCalculationId, 
	o_TypeBureauCode AS TypeBureauCode, 
	o_BureauLineOfInsurance AS BureauLineOfInsurance, 
	o_BureauCompanyNumber AS BureauCompanyNumber, 
	o_StateProvinceCode AS StateProvinceCode, 
	o_PremiumMasterRunDate AS PremiumMasterRunDate, 
	o_LossMasterRunDate AS LossMasterRunDate, 
	o_pol_key AS pol_key, 
	o_PremiumMasterClassCode AS PremiumMasterClassCode, 
	o_LossMasterClassCode AS LossMasterClassCode, 
	o_ClaimNum AS ClaimNum, 
	o_ClaimantNum AS ClaimantNum, 
	o_RiskTerritoryCode AS RiskTerritoryCode, 
	o_PolicyEffectiveDate AS PolicyEffectiveDate, 
	o_CauseOfLoss AS CauseOfLoss, 
	o_CoverageCode AS CoverageCode, 
	o_ISOFireProtectionCode AS ISOFireProtectionCode, 
	o_TypeOfPolicyForm AS TypeOfPolicyForm, 
	o_PremiumMasterDirectWrittenPremiumAmount AS PremiumMasterDirectWrittenPremiumAmount, 
	o_PaidLossAmount AS PaidLossAmount, 
	o_OutstandingLossAmount AS OutstandingLossAmount, 
	o_PolicyExpirationDate AS PolicyExpirationDate, 
	o_InceptionToDatePaidLossAmount AS InceptionToDatePaidLossAmount, 
	o_ClaimCoverageID AS ClaimCoverageID, 
	o_AnnualStatementLineNumber AS AnnualStatementLineNumber, 
	o_PolicyLimit AS PolicyLimit, 
	o_ExposureBasis AS ExposureBasis, 
	o_ConstructionCode AS ConstructionCode, 
	o_SprinklerFlag AS SprinklerFlag, 
	o_LimitOfInsurance AS LimitOfInsurance, 
	o_PremiumMasterExposure AS PremiumMasterExposure, 
	o_PaidAllocatedLossAdjustmentExpenseAmount AS PaidAllocatedLossAdjustmentExpenseAmount, 
	o_OutstandingAllocatedLossAdjustmentExpenseAmount AS OutstandingAllocatedLossAdjustmentExpenseAmount, 
	o_ClaimLossDate AS ClaimLossDate, 
	o_ZipPostalCode AS ZipPostalCode, 
	o_PremiumTransactionEffectiveDate1 AS PremiumTransactionEffectiveDate1, 
	o_prim_bus_class_code AS prim_bus_class_code, 
	o_LocationNumber AS LocationNumber, 
	o_BuildingNumber AS BuildingNumber
	FROM EXP_Values
	WHERE (NOT  in (AnnualStatementLineNumber,'9','17.2','27')) AND (NOT IN (CoverageCode,'DATACOMP','BOILER - BRK'))
),
ISSBusinessOwnersExtract AS (

	------------ PRE SQL ----------
	@{pipeline().parameters.DELETE_PRESQL}
	-------------------------------


	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ISSBusinessOwnersExtract
	(AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, CoverageCode, ISOFireProtectionCode, TypeOfPolicyForm, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, AnnualStatementLineNumber, PolicyLimit, ExposureBasis, ConstructionCode, SprinklerFlag, LimitOfInsurance, WrittenExposure, PaidAllocatedLossAdjustmentExpenseAmount, OutstandingAllocatedLossAdjustmentExpenseAmount, ClaimLossDate, ZipPostalCode, TransactionEffectiveDate, BusinessClassificationCode, LocationNumber, BuildingNumber)
	SELECT 
	AuditID AS AUDITID, 
	CREATEDDATE, 
	PremiumMasterCalculationID AS EDWPREMIUMMASTERCALCULATIONPKID, 
	LossMasterCalculationId AS EDWLOSSMASTERCALCULATIONPKID, 
	TYPEBUREAUCODE, 
	BUREAULINEOFINSURANCE, 
	BUREAUCOMPANYNUMBER, 
	STATEPROVINCECODE, 
	PREMIUMMASTERRUNDATE, 
	LOSSMASTERRUNDATE, 
	pol_key AS POLICYKEY, 
	PREMIUMMASTERCLASSCODE, 
	LOSSMASTERCLASSCODE, 
	ClaimNum AS CLAIMNUMBER, 
	ClaimantNum AS CLAIMANTNUMBER, 
	RISKTERRITORYCODE, 
	POLICYEFFECTIVEDATE, 
	CAUSEOFLOSS, 
	COVERAGECODE, 
	ISOFIREPROTECTIONCODE, 
	TYPEOFPOLICYFORM, 
	PREMIUMMASTERDIRECTWRITTENPREMIUMAMOUNT, 
	PAIDLOSSAMOUNT, 
	OUTSTANDINGLOSSAMOUNT, 
	POLICYEXPIRATIONDATE, 
	INCEPTIONTODATEPAIDLOSSAMOUNT, 
	ClaimCoverageID AS CLAIMANTCOVERAGEDETAILID, 
	ANNUALSTATEMENTLINENUMBER, 
	POLICYLIMIT, 
	EXPOSUREBASIS, 
	CONSTRUCTIONCODE, 
	SPRINKLERFLAG, 
	LIMITOFINSURANCE, 
	PremiumMasterExposure AS WRITTENEXPOSURE, 
	PAIDALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	OUTSTANDINGALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	CLAIMLOSSDATE, 
	ZIPPOSTALCODE, 
	PremiumTransactionEffectiveDate1 AS TRANSACTIONEFFECTIVEDATE, 
	prim_bus_class_code AS BUSINESSCLASSIFICATIONCODE, 
	LOCATIONNUMBER, 
	BUILDINGNUMBER
	FROM FIL_ASL
),
SQ_ISSBusinessOwnersExtract_Update_00000ClassCode AS (
	DECLARE @StartDate AS DATETIME = (SELECT DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},0));	--First day of last Quarter 
	DECLARE @EndDate AS DATETIME = (SELECT DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS}+1, 0));	--First day of current Quarter
	
	--INCORRECT DATA
	WITH IncorrectCodes
	AS
	(
		SELECT DISTINCT ISS.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
		ISS.PolicyKey AS PolicyKey,
		ISS.PremiumMasterClassCode AS CurrentClassCode,
		ISS.CoverageCode, ISS.LocationNumber, ISS.BuildingNumber
		FROM ISSBusinessOwnersExtract AS ISS WITH (NOLOCK)
		WHERE (ISS.CoverageCode NOT IN ('ADLINS', 'SELFSTRGSD', 'SELFSTRGCUST')
			AND ISS.PremiumMasterClassCode = '00000')
		AND (ISS.PremiumMasterRunDate BETWEEN @StartDate AND @EndDate) 	--Returns previous quarter timeframe
	)
	
	
	
	SELECT DISTINCT IncorrectCodes.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
	IncorrectCodes.PolicyKey AS PolicyKey,
	(CASE
		WHEN IncorrectCodes.CurrentClassCode <> '00000'
		THEN IncorrectCodes.CurrentClassCode
		ELSE ISNULL(CorrectClassCode.ClassCode, 0)
	END) AS PremiumMasterClassCode
	FROM IncorrectCodes AS IncorrectCodes
	LEFT JOIN(
		SELECT DISTINCT ISS.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
		ISS.PolicyKey AS PolicyKey, 
		ISS.LocationNumber AS LocationNumber, 
		ISS.BuildingNumber AS BuildingNumber,
		ISS.PremiumMasterClassCode AS ClassCode,
		(CASE 
			WHEN ISS.BuildingNumber <> '000'
			THEN 
			ROW_NUMBER() OVER (PARTITION BY ISS.PolicyKey
			ORDER BY ISS.LocationNumber, ISS.BuildingNumber)
			ELSE 0
		END) 
		AS RowNumber  
		FROM ISSBusinessOwnersExtract AS ISS WITH (NOLOCK)
		INNER JOIN IncorrectCodes AS IncorrectCodes ON IncorrectCodes.PolicyKey = ISS.PolicyKey
		WHERE ISS.BuildingNumber <> '000'
			AND ISS.PremiumMasterClassCode <> '00000'	--Not replacing ClassCode w/ 00000
			AND ISS.CoverageCode NOT IN ('ADLINS', 'SELFSTRGSD', 'SELFSTRGCUST')	--Not using ClassCodes where CoverageCode IN ADLINS, SELFSTRGSD, SELFSTRGCUST
	) AS CorrectClassCode
	ON IncorrectCodes.PolicyKey = CorrectClassCode.PolicyKey
	AND CorrectClassCode.RowNumber = 1
	ORDER BY ISSBusinessOwnersExtractId
),
FIL_Update_00000ClassCode AS (
	SELECT
	ISSBusinessOwnersExtractId, 
	PolicyKey, 
	PremiumMasterClassCode
	FROM SQ_ISSBusinessOwnersExtract_Update_00000ClassCode
	WHERE PremiumMasterClassCode <> '0'
),
UPD_Update_00000ClassCode AS (
	SELECT
	ISSBusinessOwnersExtractId, 
	PremiumMasterClassCode
	FROM FIL_Update_00000ClassCode
),
ISSBusinessOwnersExtract_Update_00000ClassCode AS (
	MERGE INTO ISSBusinessOwnersExtract AS T
	USING UPD_Update_00000ClassCode AS S
	ON T.ISSBusinessOwnersExtractId = S.ISSBusinessOwnersExtractId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.PremiumMasterClassCode = S.PremiumMasterClassCode
),
SQ_ISSBusinessOwnersExtract_Update_00798ClassCode AS (
	DECLARE @StartDate AS DATETIME = (SELECT DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},0));	--First day of last Quarter 
	DECLARE @EndDate AS DATETIME = (SELECT DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS}+1, 0));	--First day of current Quarter
	
	--INCORRECT DATA
	WITH #IncorrectCodes
	AS
	(
		SELECT DISTINCT ISS.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
		ISS.PolicyKey AS PolicyKey,
		ISS.PremiumMasterClassCode AS CurrentClassCode,
		ISS.LocationNumber AS LocationNumber,
		ISS.BuildingNumber AS BuildingNumber
		FROM ISSBusinessOwnersExtract AS ISS WITH (NOLOCK)
		WHERE ISS.PremiumMasterClassCode = '00798'
		AND (ISS.PremiumMasterRunDate BETWEEN @StartDate AND @EndDate) 	--Returns previous quarter timeframe
	),
	
	
	
	#ClassCodeCase1 AS (
		SELECT DISTINCT	IncorrectCodes.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
		IncorrectCodes.PolicyKey AS PolicyKey,
		ISNULL(CorrectClassCode.ClassCode, 0) AS CorrectClassCode
		FROM #IncorrectCodes AS IncorrectCodes
		LEFT JOIN(
			SELECT DISTINCT ISS.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
			ISS.PolicyKey AS PolicyKey, 
			ISS.LocationNumber AS LocationNumber, 
			ISS.BuildingNumber AS BuildingNumber,
			ISS.PremiumMasterClassCode AS ClassCode,
			(CASE 
				WHEN ISS.BuildingNumber <> '000'
				THEN 
				ROW_NUMBER() OVER (PARTITION BY ISS.PolicyKey, ISS.LocationNumber
				ORDER BY ISS.LocationNumber, ISS.BuildingNumber)
				ELSE 0
			END)
			AS SameLocationFirstBuilding  
			FROM ISSBusinessOwnersExtract AS ISS WITH (NOLOCK)
			INNER JOIN #IncorrectCodes AS IncorrectCodes ON IncorrectCodes.PolicyKey = ISS.PolicyKey
			WHERE ISS.EDWPremiumMasterCalculationPKId <> -1
			AND ISS.BuildingNumber <> '000'
			AND ISS.PremiumMasterClassCode NOT IN ('00000', '00798')	--Not replacing ClassCode w/ 00000 or 00798
			AND ISS.LocationNumber = IncorrectCodes.LocationNumber
		) AS CorrectClassCode
		ON IncorrectCodes.PolicyKey = CorrectClassCode.PolicyKey
		AND CorrectClassCode.SameLocationFirstBuilding = 1
		AND CorrectClassCode.LocationNumber = IncorrectCodes.LocationNumber
	),
	
	
	
	
	#ClassCodeCase2 AS (
		SELECT DISTINCT	IncorrectCodes.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
		IncorrectCodes.PolicyKey AS PolicyKey,
		ISNULL(CorrectClassCode.ClassCode, 0) AS CorrectClassCode
		FROM #IncorrectCodes AS IncorrectCodes
		LEFT JOIN(
			SELECT DISTINCT ISS.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
			ISS.PolicyKey AS PolicyKey, 
			ISS.LocationNumber AS LocationNumber, 
			ISS.BuildingNumber AS BuildingNumber,
			ISS.PremiumMasterClassCode AS ClassCode,
			(ROW_NUMBER() OVER (PARTITION BY ISS.PolicyKey, ISS.LocationNumber
			ORDER BY ISS.LocationNumber, ISS.BuildingNumber))
			AS SameLocationAnyBuilding  
			FROM ISSBusinessOwnersExtract AS ISS WITH (NOLOCK)
			INNER JOIN #IncorrectCodes AS IncorrectCodes ON IncorrectCodes.PolicyKey = ISS.PolicyKey
			WHERE ISS.EDWPremiumMasterCalculationPKId <> -1
			AND ISS.PremiumMasterClassCode NOT IN ('00000', '00798')	--Not replacing ClassCode w/ 00000 or 00798
			AND ISS.LocationNumber = IncorrectCodes.LocationNumber
		) AS CorrectClassCode
		ON IncorrectCodes.PolicyKey = CorrectClassCode.PolicyKey
		AND CorrectClassCode.SameLocationAnyBuilding = 1
		AND CorrectClassCode.LocationNumber = IncorrectCodes.LocationNumber
	),
	
	
	
	
	#ClassCodeCase3 AS (
		SELECT DISTINCT	IncorrectCodes.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
		IncorrectCodes.PolicyKey AS PolicyKey,
		ISNULL(CorrectClassCode.ClassCode, 0) AS CorrectClassCode
		FROM #IncorrectCodes AS IncorrectCodes
		LEFT JOIN(
			SELECT DISTINCT ISS.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
			ISS.PolicyKey AS PolicyKey, 
			ISS.LocationNumber AS LocationNumber, 
			ISS.BuildingNumber AS BuildingNumber,
			ISS.PremiumMasterClassCode AS ClassCode,
			(CASE 
				WHEN ISS.BuildingNumber <> '000'
				THEN 
				ROW_NUMBER() OVER (PARTITION BY ISS.PolicyKey
				ORDER BY ISS.LocationNumber, ISS.BuildingNumber)
				ELSE 0
			END) 
			AS RowNumber  
			FROM ISSBusinessOwnersExtract AS ISS WITH (NOLOCK)
			INNER JOIN #IncorrectCodes AS IncorrectCodes ON IncorrectCodes.PolicyKey = ISS.PolicyKey
			WHERE ISS.BuildingNumber <> '000'
			AND ISS.PremiumMasterClassCode NOT IN ('00000', '00798')	--Not replacing ClassCode w/ 00000 or 00798
		) AS CorrectClassCode
		ON IncorrectCodes.PolicyKey = CorrectClassCode.PolicyKey
		AND CorrectClassCode.RowNumber = 1
	)
	
	
	
	SELECT ClassCodeCase1.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
	ClassCodeCase1.PolicyKey AS PolicyKey,
	(CASE
		WHEN ClassCodeCase1.CorrectClassCode = '0'
		THEN (CASE
				WHEN ClassCodeCase2.CorrectClassCode = '0'
				THEN ClassCodeCase3.CorrectClassCode
				ELSE ClassCodeCase2.CorrectClassCode
			END)
		ELSE ClassCodeCase1.CorrectClassCode
	END) AS PremiumMasterClassCode
	FROM #ClassCodeCase1 AS ClassCodeCase1
	INNER JOIN #ClassCodeCase2 AS ClassCodeCase2
	ON ClassCodeCase2.ISSBusinessOwnersExtractId = ClassCodeCase1.ISSBusinessOwnersExtractId
	INNER JOIN #ClassCodeCase3 AS ClassCodeCase3
	ON ClassCodeCase3.ISSBusinessOwnersExtractId = ClassCodeCase1.ISSBusinessOwnersExtractId
	ORDER BY ISSBusinessOwnersExtractId
),
FIL_Update_00798ClassCode AS (
	SELECT
	ISSBusinessOwnersExtractId, 
	PolicyKey, 
	PremiumMasterClassCode
	FROM SQ_ISSBusinessOwnersExtract_Update_00798ClassCode
	WHERE PremiumMasterClassCode <> '0'
),
UPD_Update_00798ClassCode AS (
	SELECT
	ISSBusinessOwnersExtractId, 
	PremiumMasterClassCode
	FROM FIL_Update_00798ClassCode
),
ISSBusinessOwnersExtract_Update_00798ClassCode AS (
	MERGE INTO ISSBusinessOwnersExtract AS T
	USING UPD_Update_00798ClassCode AS S
	ON T.ISSBusinessOwnersExtractId = S.ISSBusinessOwnersExtractId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.PremiumMasterClassCode = S.PremiumMasterClassCode
),
SQ_ISSBusinessOwnersExtract_Update_SprinklerFlag AS (
	DECLARE @StartDate AS DATETIME = (SELECT DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},0));	--First day of last Quarter 
	DECLARE @EndDate AS DATETIME = (SELECT DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS}+1, 0));	--First day of current Quarter
	
	--INCORRECT DATA
	WITH #IncorrectCodes
	AS
	(
		SELECT DISTINCT ISS.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId,
		ISS.PolicyKey AS PolicyKey,
		ISS.SprinklerFlag AS CurrentSprinklerFlag,
		ISS.CoverageCode AS CoverageCode,
		ISS.LocationNumber AS LocationNumber,
		ISS.BuildingNumber AS BuildingNumber
		FROM ISSBusinessOwnersExtract AS ISS WITH (NOLOCK)
		WHERE (ISS.CoverageCode IN ('BLDG', 'BLDGFUNCVAL', 'BLDRSK', 'BLKBC', 'BLKBLDG', 'BLKCON', 'BUSPTY', 'FUNCBPP', 'HYDROWC', 'IMPROV', 'IMPROVMNT', 'LLBBLDG', 'LLBPPTY', 'MISCPROP', 'MISCREALPROP', 'PERPTY', 'PPTYO')
				AND ISS.SprinklerFlag = 'N/A')
		AND ISS.EDWPremiumMasterCalculationPKId <> -1
		AND (ISS.PremiumMasterRunDate BETWEEN @StartDate AND @EndDate) 	--Returns previous quarter timeframe
	),
	
	
	
	#SprinklerCase1 AS (
		SELECT DISTINCT	IncorrectCodes.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId,
		IncorrectCodes.PolicyKey AS PolicyKey,
		(CASE
			WHEN IncorrectCodes.CurrentSprinklerFlag <> 'N/A'
			THEN IncorrectCodes.CurrentSprinklerFlag
			ELSE ISNULL(CorrectSprinklerFlag.SprinklerFlag, 0)
		END) AS CorrectSprinklerFlag
		FROM #IncorrectCodes AS IncorrectCodes
		LEFT JOIN(
			SELECT DISTINCT ISS.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId,
			ISS.PolicyKey AS PolicyKey, 
			ISS.LocationNumber AS LocationNumber, 
			ISS.BuildingNumber AS BuildingNumber,
			ISS.SprinklerFlag AS SprinklerFlag,
			(CASE 
				WHEN ISS.BuildingNumber <> '000'
				THEN 
				ROW_NUMBER() OVER (PARTITION BY ISS.PolicyKey, ISS.LocationNumber
				ORDER BY ISS.LocationNumber, ISS.BuildingNumber)
				ELSE 0
			END)
			AS SameLocationFirstBuilding  
			FROM ISSBusinessOwnersExtract AS ISS WITH (NOLOCK)
			INNER JOIN #IncorrectCodes AS IncorrectCodes ON IncorrectCodes.PolicyKey = ISS.PolicyKey
			WHERE ISS.EDWPremiumMasterCalculationPKId <> -1
			AND ISS.BuildingNumber <> '000'
			AND ISS.SprinklerFlag <> 'N/A'	--Not replacing SprinklerFlag w/ N/A
			AND ISS.LocationNumber = IncorrectCodes.LocationNumber
		) AS CorrectSprinklerFlag
		ON IncorrectCodes.PolicyKey = CorrectSprinklerFlag.PolicyKey
		AND CorrectSprinklerFlag.SameLocationFirstBuilding = 1
		AND CorrectSprinklerFlag.LocationNumber = IncorrectCodes.LocationNumber
	),
	
	
	
	#SprinklerCase2 AS (
		SELECT DISTINCT	IncorrectCodes.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
		IncorrectCodes.PolicyKey AS PolicyKey,
		(CASE
			WHEN IncorrectCodes.CurrentSprinklerFlag <> 'N/A'
			THEN IncorrectCodes.CurrentSprinklerFlag
			ELSE ISNULL(CorrectSprinklerFlag.SprinklerFlag, 0)
		END) AS CorrectSprinklerFlag
		FROM #IncorrectCodes AS IncorrectCodes
		LEFT JOIN(
			SELECT DISTINCT ISS.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
			ISS.PolicyKey AS PolicyKey, 
			ISS.LocationNumber AS LocationNumber, 
			ISS.BuildingNumber AS BuildingNumber,
			ISS.SprinklerFlag AS SprinklerFlag,
			(ROW_NUMBER() OVER (PARTITION BY ISS.PolicyKey, ISS.LocationNumber
			ORDER BY ISS.LocationNumber, ISS.BuildingNumber))
			AS SameLocationAnyBuilding  
			FROM ISSBusinessOwnersExtract AS ISS WITH (NOLOCK)
			INNER JOIN #IncorrectCodes AS IncorrectCodes ON IncorrectCodes.PolicyKey = ISS.PolicyKey
			WHERE ISS.EDWPremiumMasterCalculationPKId <> -1
			AND ISS.SprinklerFlag <> 'N/A'	--Not replacing SprinklerFlag w/ N/A
			AND ISS.LocationNumber = IncorrectCodes.LocationNumber
		) AS CorrectSprinklerFlag
		ON IncorrectCodes.PolicyKey = CorrectSprinklerFlag.PolicyKey
		AND CorrectSprinklerFlag.SameLocationAnyBuilding = 1
		AND CorrectSprinklerFlag.LocationNumber = IncorrectCodes.LocationNumber
	),
	
	
	
	#SprinklerCase3 AS (
		SELECT DISTINCT	IncorrectCodes.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
		IncorrectCodes.PolicyKey AS PolicyKey,
		(CASE
			WHEN IncorrectCodes.CurrentSprinklerFlag <> 'N/A'
			THEN IncorrectCodes.CurrentSprinklerFlag
			ELSE ISNULL(CorrectSprinklerFlag.SprinklerFlag, 0)
		END) AS CorrectSprinklerFlag
		FROM #IncorrectCodes AS IncorrectCodes
		LEFT JOIN(
			SELECT DISTINCT ISS.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
			ISS.PolicyKey AS PolicyKey, 
			ISS.LocationNumber AS LocationNumber, 
			ISS.BuildingNumber AS BuildingNumber,
			ISS.SprinklerFlag AS SprinklerFlag,
			(CASE 
				WHEN ISS.BuildingNumber <> '000'
				THEN 
				ROW_NUMBER() OVER (PARTITION BY ISS.PolicyKey
				ORDER BY ISS.LocationNumber, ISS.BuildingNumber)
				ELSE 0
			END) 
			AS RowNumber  
			FROM ISSBusinessOwnersExtract AS ISS WITH (NOLOCK)
			INNER JOIN #IncorrectCodes AS IncorrectCodes ON IncorrectCodes.PolicyKey = ISS.PolicyKey
			WHERE ISS.EDWPremiumMasterCalculationPKId <> -1
			AND ISS.BuildingNumber <> '000'
			AND ISS.SprinklerFlag <> 'N/A'	--Not replacing SprinklerFlag w/ N/A
		) AS CorrectSprinklerFlag
		ON IncorrectCodes.PolicyKey = CorrectSprinklerFlag.PolicyKey
		AND CorrectSprinklerFlag.RowNumber = 1
	)
	
	
	
	SELECT SprinklerCase1.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
	SprinklerCase1.PolicyKey AS PolicyKey,
	(CASE
		WHEN SprinklerCase1.CorrectSprinklerFlag = '0'
		THEN (CASE
				WHEN SprinklerCase2.CorrectSprinklerFlag = '0'
				THEN SprinklerCase3.CorrectSprinklerFlag
				ELSE SprinklerCase2.CorrectSprinklerFlag
			END)
		ELSE SprinklerCase1.CorrectSprinklerFlag
	END) AS SprinklerFlag
	FROM #SprinklerCase1 AS SprinklerCase1
	INNER JOIN #SprinklerCase2 AS SprinklerCase2
	ON SprinklerCase2.ISSBusinessOwnersExtractId = SprinklerCase1.ISSBusinessOwnersExtractId
	INNER JOIN #SprinklerCase3 AS SprinklerCase3
	ON SprinklerCase3.ISSBusinessOwnersExtractId = SprinklerCase1.ISSBusinessOwnersExtractId
	ORDER BY ISSBusinessOwnersExtractId
),
FIL_Update_SprinklerFlag AS (
	SELECT
	ISSBusinessOwnersExtractId, 
	PolicyKey, 
	SprinklerFlag
	FROM SQ_ISSBusinessOwnersExtract_Update_SprinklerFlag
	WHERE SprinklerFlag <> '0'
),
UPD_Update_SprinklerFlag AS (
	SELECT
	ISSBusinessOwnersExtractId, 
	SprinklerFlag
	FROM FIL_Update_SprinklerFlag
),
ISSBusinessOwnersExtract_Update_SprinklerFlag AS (
	MERGE INTO ISSBusinessOwnersExtract AS T
	USING UPD_Update_SprinklerFlag AS S
	ON T.ISSBusinessOwnersExtractId = S.ISSBusinessOwnersExtractId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.SprinklerFlag = S.SprinklerFlag
),
SQ_ISSBusinessOwnersExtract_Update_ConstructionCode AS (
	DECLARE @StartDate AS DATETIME = (SELECT DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},0));	--First day of last Quarter 
	DECLARE @EndDate AS DATETIME = (SELECT DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS}+1, 0));	--First day of current Quarter
	
	--INCORRECT DATA
	WITH #IncorrectCodes
	AS
	(
		SELECT DISTINCT ISS.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId,
		ISS.PolicyKey AS PolicyKey,
		ISS.ConstructionCode AS CurrentConstructionCode,
		ISS.CoverageCode AS CoverageCode,
		ISS.LocationNumber AS LocationNumber,
		ISS.BuildingNumber AS BuildingNumber
		FROM ISSBusinessOwnersExtract AS ISS WITH (NOLOCK)
		WHERE (ISS.CoverageCode IN ('BLDG', 'BLDGFUNCVAL', 'BLDRSK', 'BLKBC', 'BLKBLDG', 'BLKCON', 'BUSPTY', 'FUNCBPP', 'HYDROWC', 'IMPROV', 'IMPROVMNT', 'LLBBLDG', 'LLBPPTY', 'MISCPROP', 'MISCREALPROP', 'PERPTY', 'PPTYO')
				AND ISS.ConstructionCode = 'N/A')
		AND ISS.EDWPremiumMasterCalculationPKId <> -1
		AND (ISS.PremiumMasterRunDate BETWEEN @StartDate AND @EndDate) 	--Returns previous quarter timeframe
	),
	
	
	
	#ConstructionCase1 AS (
		SELECT DISTINCT	IncorrectCodes.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId,
		IncorrectCodes.PolicyKey AS PolicyKey,
		(CASE
			WHEN IncorrectCodes.CurrentConstructionCode <> 'N/A'
			THEN IncorrectCodes.CurrentConstructionCode
			ELSE ISNULL(CorrectConstructionCode.ConstructionCode, 0)
		END) AS CorrectConstructionCode
		FROM #IncorrectCodes AS IncorrectCodes
		LEFT JOIN(
			SELECT DISTINCT ISS.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId,
			ISS.PolicyKey AS PolicyKey, 
			ISS.LocationNumber AS LocationNumber, 
			ISS.BuildingNumber AS BuildingNumber,
			ISS.ConstructionCode AS ConstructionCode,
			(CASE 
				WHEN ISS.BuildingNumber <> '000'
				THEN 
				ROW_NUMBER() OVER (PARTITION BY ISS.PolicyKey, ISS.LocationNumber
				ORDER BY ISS.LocationNumber, ISS.BuildingNumber)
				ELSE 0
			END)
			AS SameLocationFirstBuilding
			FROM ISSBusinessOwnersExtract AS ISS WITH (NOLOCK)
			INNER JOIN #IncorrectCodes AS IncorrectCodes ON IncorrectCodes.PolicyKey = ISS.PolicyKey
			WHERE ISS.EDWPremiumMasterCalculationPKId <> -1
			AND ISS.BuildingNumber <> '000'
			AND ISS.ConstructionCode <> 'N/A'	--Not replacing ConstructionCode w/ N/A
			AND ISS.LocationNumber = IncorrectCodes.LocationNumber
		) AS CorrectConstructionCode
		ON IncorrectCodes.PolicyKey = CorrectConstructionCode.PolicyKey
		AND CorrectConstructionCode.SameLocationFirstBuilding = 1
		AND CorrectConstructionCode.LocationNumber = IncorrectCodes.LocationNumber
	),
	
	
	
	#ConstructionCase2 AS (
		SELECT DISTINCT	IncorrectCodes.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
		IncorrectCodes.PolicyKey AS PolicyKey,
		(CASE
			WHEN IncorrectCodes.CurrentConstructionCode <> 'N/A'
			THEN IncorrectCodes.CurrentConstructionCode
			ELSE ISNULL(CorrectConstructionCode.ConstructionCode, 0)
		END) AS CorrectConstructionCode
		FROM #IncorrectCodes AS IncorrectCodes
		LEFT JOIN(
			SELECT DISTINCT ISS.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
			ISS.PolicyKey AS PolicyKey, 
			ISS.LocationNumber AS LocationNumber, 
			ISS.BuildingNumber AS BuildingNumber,
			ISS.ConstructionCode AS ConstructionCode,
			(ROW_NUMBER() OVER (PARTITION BY ISS.PolicyKey, ISS.LocationNumber
			ORDER BY ISS.LocationNumber, ISS.BuildingNumber))
			AS SameLocationAnyBuilding  
			FROM ISSBusinessOwnersExtract AS ISS WITH (NOLOCK)
			INNER JOIN #IncorrectCodes AS IncorrectCodes ON IncorrectCodes.PolicyKey = ISS.PolicyKey
			WHERE ISS.EDWPremiumMasterCalculationPKId <> -1
			AND ISS.ConstructionCode <> 'N/A'	--Not replacing ConstructionCode w/ N/A
			AND ISS.LocationNumber = IncorrectCodes.LocationNumber
		) AS CorrectConstructionCode
		ON IncorrectCodes.PolicyKey = CorrectConstructionCode.PolicyKey
		AND CorrectConstructionCode.SameLocationAnyBuilding = 1
		AND CorrectConstructionCode.LocationNumber = IncorrectCodes.LocationNumber
	),
	
	
	
	#ConstructionCase3 AS (
		SELECT DISTINCT	IncorrectCodes.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
		IncorrectCodes.PolicyKey AS PolicyKey,
		(CASE
			WHEN IncorrectCodes.CurrentConstructionCode <> 'N/A'
			THEN IncorrectCodes.CurrentConstructionCode
			ELSE ISNULL(CorrectConstructionCode.ConstructionCode, 0)
		END) AS CorrectConstructionCode
		FROM #IncorrectCodes AS IncorrectCodes
		LEFT JOIN(
			SELECT DISTINCT ISS.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
			ISS.PolicyKey AS PolicyKey, 
			ISS.LocationNumber AS LocationNumber, 
			ISS.BuildingNumber AS BuildingNumber,
			ISS.ConstructionCode AS ConstructionCode,
			(CASE 
				WHEN ISS.BuildingNumber <> '000'
				THEN 
				ROW_NUMBER() OVER (PARTITION BY ISS.PolicyKey
				ORDER BY ISS.LocationNumber, ISS.BuildingNumber)
				ELSE 0
			END) 
			AS RowNumber  
			FROM ISSBusinessOwnersExtract AS ISS WITH (NOLOCK)
			INNER JOIN #IncorrectCodes AS IncorrectCodes ON IncorrectCodes.PolicyKey = ISS.PolicyKey
			WHERE ISS.EDWPremiumMasterCalculationPKId <> -1
			AND ISS.BuildingNumber <> '000'
			AND ISS.ConstructionCode <> 'N/A'	--Not replacing ConstructionCode w/ N/A
		) AS CorrectConstructionCode
		ON IncorrectCodes.PolicyKey = CorrectConstructionCode.PolicyKey
		AND CorrectConstructionCode.RowNumber = 1
	)
	
	
	
	SELECT ConstructionCase1.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
	ConstructionCase1.PolicyKey AS PolicyKey,
	(CASE
		WHEN ConstructionCase1.CorrectConstructionCode = '0'
		THEN (CASE
				WHEN ConstructionCase2.CorrectConstructionCode = '0'
				THEN ConstructionCase3.CorrectConstructionCode
				ELSE ConstructionCase2.CorrectConstructionCode
			END)
		ELSE ConstructionCase1.CorrectConstructionCode
	END) AS ConstructionCode
	FROM #ConstructionCase1 AS ConstructionCase1
	INNER JOIN #ConstructionCase2 AS ConstructionCase2
	ON ConstructionCase2.ISSBusinessOwnersExtractId = ConstructionCase1.ISSBusinessOwnersExtractId
	INNER JOIN #ConstructionCase3 AS ConstructionCase3
	ON ConstructionCase3.ISSBusinessOwnersExtractId = ConstructionCase1.ISSBusinessOwnersExtractId
	ORDER BY ISSBusinessOwnersExtractId
),
EXP_Update_ConstructionCode AS (
	SELECT
	ISSBusinessOwnersExtractId,
	PolicyKey,
	ConstructionCode AS i_ConstructionCode,
	-- *INF*: IIF(i_ConstructionCode = '0', :LKP.LKP_Update_ConstructionCode(PolicyKey),
	-- i_ConstructionCode)
	IFF(
	    i_ConstructionCode = '0', LKP_UPDATE_CONSTRUCTIONCODE_PolicyKey.ConstructionCode,
	    i_ConstructionCode
	) AS v_ConstructionCode,
	-- *INF*: IIF(NOT ISNULL(v_ConstructionCode),
	-- v_ConstructionCode, '0')
	IFF(v_ConstructionCode IS NOT NULL, v_ConstructionCode, '0') AS o_ConstructionCode
	FROM SQ_ISSBusinessOwnersExtract_Update_ConstructionCode
	LEFT JOIN LKP_UPDATE_CONSTRUCTIONCODE LKP_UPDATE_CONSTRUCTIONCODE_PolicyKey
	ON LKP_UPDATE_CONSTRUCTIONCODE_PolicyKey.PolicyKey = PolicyKey

),
FIL_Update_ConstructionCode AS (
	SELECT
	ISSBusinessOwnersExtractId, 
	PolicyKey, 
	o_ConstructionCode AS ConstructionCode
	FROM EXP_Update_ConstructionCode
	WHERE ConstructionCode <> '0'
),
UPD_Update_ConstructionCode AS (
	SELECT
	ISSBusinessOwnersExtractId, 
	ConstructionCode
	FROM FIL_Update_ConstructionCode
),
ISSBusinessOwnersExtract_Update_ConstructionCode AS (
	MERGE INTO ISSBusinessOwnersExtract AS T
	USING UPD_Update_ConstructionCode AS S
	ON T.ISSBusinessOwnersExtractId = S.ISSBusinessOwnersExtractId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ConstructionCode = S.ConstructionCode
),
SQ_ISSBusinessOwnersExtract_Update_ISOFireProtectionCode AS (
	DECLARE @StartDate AS DATETIME = (SELECT DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},0));	--First day of last Quarter 
	DECLARE @EndDate AS DATETIME = (SELECT DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS}+1, 0));	--First day of current Quarter
	
	--INCORRECT DATA
	WITH #IncorrectCodes
	AS
	(
		SELECT DISTINCT ISS.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId,
		ISS.PolicyKey AS PolicyKey,
		ISS.ISOFireProtectionCode AS CurrentISOFireProtectionCode,
		ISS.CoverageCode AS CoverageCode,
		ISS.LocationNumber AS LocationNumber,
		ISS.BuildingNumber AS BuildingNumber
		FROM ISSBusinessOwnersExtract AS ISS WITH (NOLOCK)
		WHERE (ISS.CoverageCode IN ('BLDG', 'BLDGFUNCVAL', 'BLDRSK', 'BLKBC', 'BLKBLDG', 'BLKCON', 'BUSPTY', 'FUNCBPP', 'HYDROWC', 'IMPROV', 'IMPROVMNT', 'LLBBLDG', 'LLBPPTY', 'MISCPROP', 'MISCREALPROP', 'PERPTY', 'PPTYO')
			AND ISS.ISOFireProtectionCode IN ('N/A', '00'))
		AND ISS.EDWPremiumMasterCalculationPKId <> -1
		AND (ISS.PremiumMasterRunDate BETWEEN @StartDate AND @EndDate) 	--Returns previous quarter timeframe
	),
	
	
	
	#ISOCase1 AS (
		SELECT DISTINCT	IncorrectCodes.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId,
		IncorrectCodes.PolicyKey AS PolicyKey,
		(CASE
			WHEN IncorrectCodes.CurrentISOFireProtectionCode <> 'N/A'
			THEN IncorrectCodes.CurrentISOFireProtectionCode
			ELSE ISNULL(CorrectISOFireProtectionCode.ISOFireProtectionCode, 0)
		END) AS CorrectISOFireProtectionCode
		FROM #IncorrectCodes AS IncorrectCodes
		LEFT JOIN(
			SELECT DISTINCT ISS.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId,
			ISS.PolicyKey AS PolicyKey, 
			ISS.LocationNumber AS LocationNumber, 
			ISS.BuildingNumber AS BuildingNumber,
			ISS.ISOFireProtectionCode AS ISOFireProtectionCode,
			(CASE 
				WHEN ISS.BuildingNumber <> '000'
				THEN 
				ROW_NUMBER() OVER (PARTITION BY ISS.PolicyKey, ISS.LocationNumber
				ORDER BY ISS.LocationNumber, ISS.BuildingNumber)
				ELSE 0
			END)
			AS SameLocationFirstBuilding
			FROM ISSBusinessOwnersExtract AS ISS WITH (NOLOCK)
			INNER JOIN #IncorrectCodes AS IncorrectCodes ON IncorrectCodes.PolicyKey = ISS.PolicyKey
			WHERE ISS.EDWPremiumMasterCalculationPKId <> -1
			AND ISS.BuildingNumber <> '000'
			AND ISS.ISOFireProtectionCode NOT IN ('N/A', '00')	--Not replacing ISOFireProtectionCode w/ N/A
			AND ISS.LocationNumber = IncorrectCodes.LocationNumber
		) AS CorrectISOFireProtectionCode
		ON IncorrectCodes.PolicyKey = CorrectISOFireProtectionCode.PolicyKey
		AND CorrectISOFireProtectionCode.SameLocationFirstBuilding = 1
		AND CorrectISOFireProtectionCode.LocationNumber = IncorrectCodes.LocationNumber
	),
	
	
	
	
	#ISOCase2 AS (
		SELECT DISTINCT	IncorrectCodes.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
		IncorrectCodes.PolicyKey AS PolicyKey,
		(CASE
			WHEN IncorrectCodes.CurrentISOFireProtectionCode <> 'N/A'
			THEN IncorrectCodes.CurrentISOFireProtectionCode
			ELSE ISNULL(CorrectISOFireProtectionCode.ISOFireProtectionCode, 0)
		END) AS CorrectISOFireProtectionCode
		FROM #IncorrectCodes AS IncorrectCodes
		LEFT JOIN(
			SELECT DISTINCT ISS.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
			ISS.PolicyKey AS PolicyKey, 
			ISS.LocationNumber AS LocationNumber, 
			ISS.BuildingNumber AS BuildingNumber,
			ISS.ISOFireProtectionCode AS ISOFireProtectionCode,
			(ROW_NUMBER() OVER (PARTITION BY ISS.PolicyKey, ISS.LocationNumber
			ORDER BY ISS.LocationNumber, ISS.BuildingNumber))
			AS SameLocationAnyBuilding   
			FROM ISSBusinessOwnersExtract AS ISS WITH (NOLOCK)
			INNER JOIN #IncorrectCodes AS IncorrectCodes ON IncorrectCodes.PolicyKey = ISS.PolicyKey
			WHERE ISS.EDWPremiumMasterCalculationPKId <> -1
			AND ISS.ISOFireProtectionCode NOT IN ('N/A', '00')	--Not replacing ISOFireProtectionCode w/ N/A
			AND ISS.LocationNumber = IncorrectCodes.LocationNumber
		) AS CorrectISOFireProtectionCode
		ON IncorrectCodes.PolicyKey = CorrectISOFireProtectionCode.PolicyKey
		AND CorrectISOFireProtectionCode.SameLocationAnyBuilding = 1
		AND CorrectISOFireProtectionCode.LocationNumber = IncorrectCodes.LocationNumber
	),
	
	
	
	#ISOCase3 AS (
		SELECT DISTINCT	IncorrectCodes.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
		IncorrectCodes.PolicyKey AS PolicyKey,
		(CASE
			WHEN IncorrectCodes.CurrentISOFireProtectionCode <> 'N/A'
			THEN IncorrectCodes.CurrentISOFireProtectionCode
			ELSE ISNULL(CorrectISOFireProtectionCode.ISOFireProtectionCode, 0)
		END) AS CorrectISOFireProtectionCode
		FROM #IncorrectCodes AS IncorrectCodes
		LEFT JOIN(
			SELECT DISTINCT ISS.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
			ISS.PolicyKey AS PolicyKey, 
			ISS.LocationNumber AS LocationNumber, 
			ISS.BuildingNumber AS BuildingNumber,
			ISS.ISOFireProtectionCode AS ISOFireProtectionCode,
			(CASE 
				WHEN ISS.BuildingNumber <> '000'
				THEN 
				ROW_NUMBER() OVER (PARTITION BY ISS.PolicyKey
				ORDER BY ISS.LocationNumber, ISS.BuildingNumber)
				ELSE 0
			END) 
			AS RowNumber  
			FROM ISSBusinessOwnersExtract AS ISS WITH (NOLOCK)
			INNER JOIN #IncorrectCodes AS IncorrectCodes ON IncorrectCodes.PolicyKey = ISS.PolicyKey
			WHERE ISS.EDWPremiumMasterCalculationPKId <> -1
			AND ISS.BuildingNumber <> '000'
			AND ISS.ISOFireProtectionCode NOT IN ('N/A', '00')	--Not replacing ISOFireProtectionCode w/ N/A
		) AS CorrectISOFireProtectionCode
		ON IncorrectCodes.PolicyKey = CorrectISOFireProtectionCode.PolicyKey
		AND CorrectISOFireProtectionCode.RowNumber = 1
	)
	
	
	
	SELECT ISOCase1.ISSBusinessOwnersExtractId AS ISSBusinessOwnersExtractId, 
	ISOCase1.PolicyKey AS PolicyKey,
	(CASE
		WHEN ISOCase1.CorrectISOFireProtectionCode = '0'
		THEN (CASE
				WHEN ISOCase2.CorrectISOFireProtectionCode = '0'
				THEN ISOCase3.CorrectISOFireProtectionCode
				ELSE ISOCase2.CorrectISOFireProtectionCode
			END)
		ELSE ISOCase1.CorrectISOFireProtectionCode
	END) AS ISOFireProtectionCode
	FROM #ISOCase1 AS ISOCase1
	INNER JOIN #ISOCase2 AS ISOCase2
	ON ISOCase2.ISSBusinessOwnersExtractId = ISOCase1.ISSBusinessOwnersExtractId
	INNER JOIN #ISOCase3 AS ISOCase3
	ON ISOCase3.ISSBusinessOwnersExtractId = ISOCase1.ISSBusinessOwnersExtractId
	ORDER BY ISSBusinessOwnersExtractId
),
EXP_Update_ISOFireProtectionCode AS (
	SELECT
	ISSBusinessOwnersExtractId,
	PolicyKey,
	ISOFireProtectionCode AS i_ISOFireProtectionCode,
	-- *INF*: IIF(i_ISOFireProtectionCode = '0', :LKP.LKP_Update_ISOFireProtectionCode(PolicyKey),
	-- i_ISOFireProtectionCode)
	IFF(
	    i_ISOFireProtectionCode = '0',
	    LKP_UPDATE_ISOFIREPROTECTIONCODE_PolicyKey.IsoFireProtectionCode,
	    i_ISOFireProtectionCode
	) AS v_ISOFireProtectionCode,
	-- *INF*: IIF(NOT ISNULL(v_ISOFireProtectionCode),
	-- v_ISOFireProtectionCode, '0')
	IFF(v_ISOFireProtectionCode IS NOT NULL, v_ISOFireProtectionCode, '0') AS o_ISOFireProtectionCode
	FROM SQ_ISSBusinessOwnersExtract_Update_ISOFireProtectionCode
	LEFT JOIN LKP_UPDATE_ISOFIREPROTECTIONCODE LKP_UPDATE_ISOFIREPROTECTIONCODE_PolicyKey
	ON LKP_UPDATE_ISOFIREPROTECTIONCODE_PolicyKey.PolicyKey = PolicyKey

),
FIL_Update_ISOFireProtectionCode AS (
	SELECT
	ISSBusinessOwnersExtractId, 
	PolicyKey, 
	o_ISOFireProtectionCode AS ISOFireProtectionCode
	FROM EXP_Update_ISOFireProtectionCode
	WHERE ISOFireProtectionCode <> '0'
),
UPD_Update_ISOFireProtectionCode AS (
	SELECT
	ISSBusinessOwnersExtractId, 
	ISOFireProtectionCode
	FROM FIL_Update_ISOFireProtectionCode
),
ISSBusinessOwnersExtract_Update_ISOFireProtectionCode AS (
	MERGE INTO ISSBusinessOwnersExtract AS T
	USING UPD_Update_ISOFireProtectionCode AS S
	ON T.ISSBusinessOwnersExtractId = S.ISSBusinessOwnersExtractId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ISOFireProtectionCode = S.ISOFireProtectionCode
),
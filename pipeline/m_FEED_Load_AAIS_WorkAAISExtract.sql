WITH
LKP_StatisticalCoverage_SupConstructionCode_StandardConstructionCodeDescription AS (
	SELECT
	StandardConstructionCodeDescription,
	ConstructionCode
	FROM (
		select distinct scc.ConstructionCode AS ConstructionCode,scc.StandardConstructionCodeDescription AS StandardConstructionCodeDescription
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction pt
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage sc
		on pt.StatisticalCoverageAKID = sc.StatisticalCoverageAKID
		and sc.ClassCode in ('079','081','084','121','149', '235','236','237', '800') and pt.CurrentSnapshotFlag = 1
		and sc.CurrentSnapshotFlag = 1
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupConstructionCode scc
		on scc.ConstructionCode = pt.ConstructionCode and scc.CurrentSnapshotFlag = 1
		where pt.SourceSystemID = 'PMS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ConstructionCode ORDER BY StandardConstructionCodeDescription) = 1
),
LKP_RatingCoverage_SupConstructionCode_StandardConstructionCodeDescription AS (
	SELECT
	StandardConstructionCodeDescription,
	ConstructionCode
	FROM (
		select distinct scc.ConstructionCode AS ConstructionCode,scc.StandardConstructionCodeDescription AS StandardConstructionCodeDescription
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction pt
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage rc
		on pt.RatingCoverageAKId = rc.RatingCoverageAKID
		and rc.ClassCode in ('079','081','084','121','149', '235','236','237', '800') and pt.CurrentSnapshotFlag = 1
		and rc.EffectiveDate=pt.EffectiveDate
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupConstructionCode scc
		on scc.ConstructionCode = pt.ConstructionCode and scc.CurrentSnapshotFlag = 1
		where pt.SourceSystemID <>'PMS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ConstructionCode ORDER BY StandardConstructionCodeDescription) = 1
),
LKP_sup_CauseOfLoss_CauseOfLossName AS (
	SELECT
	CauseOfLossName,
	CauseOfLossId
	FROM (
		select distinct scl.CauseOfLossId AS CauseOfLossId,col.CauseOfLossName AS CauseOfLossName
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_calculation lmc
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail ccd
		on lmc.new_claim_count = '1' 
		and ccd.pms_type_bureau_code in ('IM','PI','InlandMarine','PersonalInlandMarine')
		and lmc.claimant_cov_det_ak_id = ccd.claimant_cov_det_ak_id
		and lmc.crrnt_snpsht_flag = 1 and ccd.crrnt_snpsht_flag = 1
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_CauseOfLoss scl
		on scl.LineOfBusiness in ('CPP','HAP')
		and scl.CauseOfLossId = ccd.CauseOfLossID
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_CauseOfLoss col
		on scl.CauseOfLossAKID = col.CauseOfLossAKID
		AND col.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CauseOfLossId ORDER BY CauseOfLossName) = 1
),
LKP_Exposure_Previous_Onset_Transaction_PMS AS (
	SELECT
	PremiumMasterExposure,
	PolicyAKID,
	PMSFunctionCode,
	InsuranceLine,
	LocationUnitNumber,
	SubLocationUnitNumber,
	RiskUnitGroup,
	RiskUnitGroupSequenceNumber,
	RiskUnit,
	RiskUnitSequenceNumber,
	PMSTypeExposure,
	MajorPerilCode,
	MajorPerilSequenceNumber,
	PremiumTransactionEffectiveDate,
	PremiumTransactionEnteredDate
	FROM (
		Declare @StartTime datetime
		Declare @EndTime datetime
		
		set @StartTime = @{pipeline().parameters.FIRSTQMONTH}
		set @EndTime = @{pipeline().parameters.LASTQMONTH}
		
		SELECT 
		PMC.PremiumMasterExposure as PremiumMasterExposure,
		PMC.PolicyAKID as PolicyAKID,
		PT.PMSFunctionCode as PMSFunctionCode,
		PC.InsuranceLine as InsuranceLine,
		RL.LocationUnitNumber as LocationUnitNumber,
		SC.SubLocationUnitNumber as SubLocationUnitNumber,
		SC.RiskUnitGroup as RiskUnitGroup,
		SC.RiskUnitGroupSequenceNumber as RiskUnitGroupSequenceNumber,
		SC.RiskUnit as RiskUnit,
		SC.RiskUnitSequenceNumber as RiskUnitSequenceNumber,
		SC.PMSTypeExposure as PMSTypeExposure,
		SC.MajorPerilCode as MajorPerilCode,
		SC.MajorPerilSequenceNumber as MajorPerilSequenceNumber,
		PT.PremiumTransactionEffectiveDate as PremiumTransactionEffectiveDate,
		PMC.PremiumTransactionEnteredDate as PremiumTransactionEnteredDate
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation PMC
		
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on PMC.PolicyCoverageAKID=PC.PolicyCoverageAKID
		and PMC.PremiumMasterRunDate between PC.EffectiveDate and PC.ExpirationDate
		and PMC.RiskLocationAKID=PC.RiskLocationAKID
		
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
		on PMC.StatisticalCoverageAKID=SC.StatisticalCoverageAKID
		
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		on PMC.PremiumTransactionAKID=PT.PremiumTransactionAKID
		
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
		on PMC.RiskLocationAKId=RL.RiskLocationAKId
		and PMC.PremiumMasterRunDate between RL.EffectiveDate and RL.ExpirationDate
		and PC.RiskLocationAKID=RL.RiskLocationAKID
		
		where 
		PT.SourceSystemId='PMS'
		and PC.TypeBureauCode in ('IM','PI')
		and PMC.PremiumMasterPremium <>0
		and PMC.PremiumMasterPremiumType='D'
		and PMC.PremiumMasterRecordType<>'OFFSET'
		and PMC.PremiumMasterRunDate<=@EndTime
		and exists 
		(select 1 
		from 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation PMC2 
		
		join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P2
		on PMC2.PolicyAKID=P2.pol_ak_id and P2.crrnt_snpsht_flag=1
		
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC2
		on PMC2.PolicyCoverageAKID=PC2.PolicyCoverageAKID
		and PMC2.PremiumMasterRunDate between PC2.EffectiveDate and PC2.ExpirationDate
		and PMC2.RiskLocationAKID=PC2.RiskLocationAKID
		
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISG2
		on P2.InsuranceSegmentAKId=ISG2.InsuranceSegmentAKId and ISG2.CurrentSnapshotFlag=1
		
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC2
		on PMC2.StatisticalCoverageAKID=SC2.StatisticalCoverageAKID
		
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT2
		on PMC2.PremiumTransactionAKID=PT2.PremiumTransactionAKID
		
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL2
		on PMC2.RiskLocationAKId=RL2.RiskLocationAKId
		and PC2.RiskLocationAKID=RL2.RiskLocationAKID
		and PMC2.PremiumMasterRunDate between RL2.EffectiveDate and RL2.ExpirationDate
		
		where 
		PC2.TypeBureauCode in ('IM','PI') and ISG2.InsuranceSegmentCode IN ('1','2')
		and PMC2.PremiumMasterPremium <>0 and PMC2.PremiumMasterPremiumType='D'
		and PMC2.PremiumMasterRecordType='OFFSET'
		and PMC2.PremiumMasterRunDate>=@StartTime and PMC2.PremiumMasterRunDate<=@EndTime
		and PMC2.PolicyAKID=PMC.PolicyAKID and PT2.PMSFunctionCode=PT.PMSFunctionCode
		and PC2.InsuranceLine=PC.InsuranceLine and RL2.LocationUnitNumber=RL.LocationUnitNumber
		and SC2.SubLocationUnitNumber=SC.SubLocationUnitNumber and SC2.RiskUnitGroup=SC.RiskUnitGroup
		and SC2.RiskUnitGroupSequenceNumber=SC.RiskUnitGroupSequenceNumber and SC2.RiskUnit=SC.RiskUnit
		and SC2.RiskUnitSequenceNumber=SC.RiskUnitSequenceNumber and SC2.PMSTypeExposure=SC.PMSTypeExposure
		and SC2.MajorPerilCode=SC.MajorPerilCode and SC2.MajorPerilSequenceNumber=SC.MajorPerilSequenceNumber
		)
		
		ORDER  BY PMC.PolicyAKID,PT.PMSFunctionCode,PC.InsuranceLine,RL.LocationUnitNumber,         SC.SubLocationUnitNumber,SC.RiskUnitGroup,SC.RiskUnitGroupSequenceNumber,SC.RiskUnit,         SC.RiskUnitSequenceNumber,SC.PMSTypeExposure,SC.MajorPerilCode,SC.MajorPerilSequenceNumber,          PT.PremiumTransactionEffectiveDate,PMC.PremiumTransactionEnteredDate,PT.PremiumLoadSequence   --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID,PMSFunctionCode,InsuranceLine,LocationUnitNumber,SubLocationUnitNumber,RiskUnitGroup,RiskUnitGroupSequenceNumber,RiskUnit,RiskUnitSequenceNumber,PMSTypeExposure,MajorPerilCode,MajorPerilSequenceNumber,PremiumTransactionEffectiveDate,PremiumTransactionEnteredDate ORDER BY PremiumMasterExposure DESC) = 1
),
LKP_Exposure_Previous_Onset_Transaction_DCT AS (
	SELECT
	Exposure,
	RatingCoverageAKId,
	PremiumTransactionEffectiveDate,
	PremiumTransactionEnteredDate
	FROM (
		Declare @StartTime datetime
		Declare @EndTime datetime
		
		set @StartTime = @{pipeline().parameters.FIRSTQMONTH}
		set @EndTime = @{pipeline().parameters.LASTQMONTH}
		
		select PT.Exposure as Exposure,
		PT.RatingCoverageAKId as RatingCoverageAKId,
		PT.PremiumTransactionEffectiveDate as PremiumTransactionEffectiveDate,
		PT.PremiumTransactionEnteredDate as PremiumTransactionEnteredDate
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		join (
		SELECT distinct PT.RatingCoverageAKId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
		on PT.RatingCoverageAKId=RC.RatingCoverageAKId and PT.EffectiveDate=RC.EffectiveDate
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on RC.PolicyCoverageAKId=PC.PolicyCoverageAKId and PC.CurrentSnapshotFlag=1
		join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Policy P
		on PC.PolicyAKId=P.pol_ak_id and P.crrnt_snpsht_flag=1
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISG
		on P.InsuranceSegmentAKID=ISG.InsuranceSegmentAKID and ISG.CurrentSnapshotFlag=1
		where PT.SourceSystemId='DCT' and  PC.TypeBureauCode in ('InlandMarine','GamesOfChance','HoleInOne')
		and ISG.InsuranceSegmentCode IN ('1','2')
		and PT.PremiumTransactionAmount<>0
		and PT.PremiumType='D'
		and PT.OffsetOnsetCode='Offset'
		and PT.PremiumTransactionBookedDate between @StartTime and @EndTime
		) S
		on PT.RatingCoverageAKID=S.RatingCoverageAKID
		where PT.SourceSystemId='DCT'
		and PT.OffsetOnsetCode not in ('Offset', 'Deprecated') 
		and PT.PremiumTransactionAmount<>0
		and PT.PremiumType='D'
		and PT.PremiumTransactionBookedDate<= @EndTime
		order by PT.RatingCoverageAKId, PT.PremiumTransactionEffectiveDate,PT.PremiumTransactionEnteredDate, PT.PremiumLoadSequence--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingCoverageAKId,PremiumTransactionEffectiveDate,PremiumTransactionEnteredDate ORDER BY Exposure) = 1
),
LKP_CoverageDeductible_Deductibleamount_Prem AS (
	SELECT
	DeductibleAmount,
	PolicyKey,
	CoverageType
	FROM (
		SELECT 
		MAX(Convert(Integer,D.CoverageDeductibleValue)) as DeductibleAmount,
		Pol.pol_key as PolicyKey, 
		RC.CoverageType as CoverageType
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransactiON pt 
		INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage rc      
		ON rc.RatingCoverageAKID = pt.RatingCoverageAKId      
		AND rc.EffectiveDate = pt.EffectiveDate 
		INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage pc      
		ON pc.PolicyCoverageAKID = rc.PolicyCoverageAKID      
		AND pc.CurrentSnapshotFlag = 1   
		INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocatiON rl 
		ON pc.RiskLocatiONAKID = rl.RiskLocatiONAKID   
		AND rl.CurrentSnapshotFlag = 1   
		INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy pol    
		ON pol.pol_ak_id = pc.PolicyAKID 
		AND pol.crrnt_snpsht_flag = 1 
		INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductibleBridge B ON B.PremiumTransactiONAKId = pt.PremiumTransactiONAKID 
		INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductible D ON D.CoverageDeductibleId = B.CoverageDeductibleId 
		and ISNUMERIC(D.CoverageDeductibleValue)=1
		WHERE  
		pt.SourceSystemID='DCT' AND 
		CoverageDeductibleType IN  (
		'ContractorsEquipmentStandard',
		'LineContractorsEquipmentScheduleStandard',
		'IMLAT_Deductible',
		'IMLAT_OtherDeductible',
		'BaileesCustomersStandard',
		'Bailee Customers - Other',
		'BoatDealersBaileeStandard',
		'BoatDealersPhysicalDamageStandard',
		'BoatDealersPhysicalDamageOther',
		'BoatownersStandard',
		'BoatownersOther',
		'BuildersRiskStandard',
		'BuildersRiskOther',
		'CommercialFineArtsStandard',
		'IMLCFA_FormDisplayDeductible',
		'ElectronicDataProcessingStandard',
		'LCS_OtherDeductible',
		'DealersStandard',
		'Dealers - Other',
		'EquipmentDealersStandard',
		'ExhibitionStandard',
		'InstallationStandard',
		'InstallationOther',
		'MiscellaneousArticlesStandard',
		'MiscellaneousArticlesOther',
		'MiscellaneousBaileesProcessorsStandard',
		'IMLMBP_FormDisplayDeductible',
		'LineMiscellaneousBaileeProcessor',
		'MotorTruckCargoStandard',
		'MotorTruckCargoOther',
		'MusicalInstrumentsStandard',
		'PatternsAndDiesStandard',
		'PatternsAndDiesOther',
		'PhysiciansSurgeonsStandard',
		'RadioAndTelevisionTowersAndEquipmentStandard',
		'RadioAndTelevisionTowersAndEquipmentOther',
		'RiggersLiabilityStandard',
		'RiggersLiabilityOther',
		'SalespersonsSamplesStandard',
		'SalespersonsSamplesOther',
		'SignsStandard',
		'TripTransitStandard',
		'Trip Transit - Other',
		'ValuablePapersStandard',
		'Photographic Equip',
		'Warehouse Operators Legal Liab',
		'Musical Instruments',
		'Photographic Equip',
		'Scheduled Property',
		'Transportation',
		'ElectronicDataProcessingOther',
		'CameraMusicalStandard',
		'ComputerSystemsPersonalPortableComputers',
		
		'IMLI_FormDisplayDeductible',
		'IMLRTE_FormDisplayDeductible',
		
		'BPEDP_Standard',
		'BPFineArts_Standard',
		
		'BPVoluntaryPropertyDamage_Standard',
		'Computer Attack & Cyber Extortion Deductible',
		'Network Security Liability Deductible',
		'DataCompromiseResponseExpense',
		'DataCompromiseDefenseAndLiability',
		'EarthquakeStandard'
		)
		GROUP BY Pol.pol_key, RC.CoverageType
		ORDER BY Pol.pol_key --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,CoverageType ORDER BY DeductibleAmount) = 1
),
LKP_CoverageDeductible_Deductibleamount_Loss AS (
	SELECT
	DeductibleAmount,
	PolicyKey,
	CoverageType,
	EffectiveDate,
	ExpirationDate
	FROM (
		select 
		MAX(Convert(Integer,A.CoverageDeductibleValue)) as DeductibleAmount 
		,A.PolicyKey as PolicyKey
		,A.CoverageType as CoverageType
		,A.EffectiveDate as EffectiveDate
		,A.ExpirationDate as ExpirationDate
		FROM
		(
		SELECT 
		D.CoverageDeductibleValue,
		Pol.pol_key as PolicyKey, 
		RC.CoverageType as CoverageType,
		-- use lower of date, based loosely on how coveragedetaildim takes the lower value
		CASE 
		WHEN rc.RatingCoverageEffectiveDate < pt.PremiumTransactionEffectiveDate THEN rc.RatingCoverageEffectiveDate
		ELSE pt.PremiumTransactionEffectiveDate 
		END as  EffectiveDate, 
		CASE
		WHEN rc.RatingCoverageExpirationDate < pt.PremiumTransactionExpirationDate THEN rc.RatingCoverageExpirationDate
		ELSE pt.PremiumTransactionExpirationDate 
		END as ExpirationDate
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction pt 
		INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage rc      
		ON rc.RatingCoverageAKID = pt.RatingCoverageAKId      
		AND rc.EffectiveDate = pt.EffectiveDate 
		INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage pc      
		ON pc.PolicyCoverageAKID = rc.PolicyCoverageAKID      
		AND pc.CurrentSnapshotFlag = 1   
		INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocatiON rl 
		ON pc.RiskLocatiONAKID = rl.RiskLocatiONAKID   
		AND rl.CurrentSnapshotFlag = 1   
		INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy pol    
		ON pol.pol_ak_id = pc.PolicyAKID 
		AND pol.crrnt_snpsht_flag = 1 
		INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductibleBridge B ON B.PremiumTransactiONAKId = pt.PremiumTransactiONAKID 
		INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductible D ON D.CoverageDeductibleId = B.CoverageDeductibleId
		and ISNUMERIC(D.CoverageDeductibleValue)=1
		WHERE  
		pt.SourceSystemID='DCT' 
		AND CoverageDeductibleType IN  (
		'ContractorsEquipmentStandard',
		'LineContractorsEquipmentScheduleStandard',
		'IMLAT_Deductible',
		'IMLAT_OtherDeductible',
		'BaileesCustomersStandard',
		'Bailee Customers - Other',
		'BoatDealersBaileeStandard',
		'BoatDealersPhysicalDamageStandard',
		'BoatDealersPhysicalDamageOther',
		'BoatownersStandard',
		'BoatownersOther',
		'BuildersRiskStandard',
		'BuildersRiskOther',
		'CommercialFineArtsStandard',
		'IMLCFA_FormDisplayDeductible',
		'ElectronicDataProcessingStandard',
		'LCS_OtherDeductible',
		'DealersStandard',
		'Dealers - Other',
		'EquipmentDealersStandard',
		'ExhibitionStandard',
		'InstallationStandard',
		'InstallationOther',
		'MiscellaneousArticlesStandard',
		'MiscellaneousArticlesOther',
		'MiscellaneousBaileesProcessorsStandard',
		'IMLMBP_FormDisplayDeductible',
		'LineMiscellaneousBaileeProcessor',
		'MotorTruckCargoStandard',
		'MotorTruckCargoOther',
		'MusicalInstrumentsStandard',
		'PatternsAndDiesStandard',
		'PatternsAndDiesOther',
		'PhysiciansSurgeonsStandard',
		'RadioAndTelevisionTowersAndEquipmentStandard',
		'RadioAndTelevisionTowersAndEquipmentOther',
		'RiggersLiabilityStandard',
		'RiggersLiabilityOther',
		'SalespersonsSamplesStandard',
		'SalespersonsSamplesOther',
		'SignsStandard',
		'TripTransitStandard',
		'Trip Transit - Other',
		'ValuablePapersStandard',
		'Photographic Equip',
		'Warehouse Operators Legal Liab',
		'Musical Instruments',
		'Photographic Equip',
		'Scheduled Property',
		'Transportation',
		'ElectronicDataProcessingOther',
		'CameraMusicalStandard',
		'ComputerSystemsPersonalPortableComputers',
		
		'IMLI_FormDisplayDeductible',
		'IMLRTE_FormDisplayDeductible',
		
		'BPEDP_Standard',
		'BPFineArts_Standard',
		
		'BPVoluntaryPropertyDamage_Standard',
		'Computer Attack & Cyber Extortion Deductible',
		'Network Security Liability Deductible',
		'DataCompromiseResponseExpense',
		'DataCompromiseDefenseAndLiability',
		'EarthquakeStandard'
		)
		) A
		GROUP BY A.PolicyKey, A.CoverageType,EffectiveDate, ExpirationDate
		ORDER BY A.PolicyKey, EffectiveDate desc --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,CoverageType,EffectiveDate,ExpirationDate ORDER BY DeductibleAmount) = 1
),
SQ_Loss AS (
	DECLARE @ST_DT datetime;
	DECLARE @ED_DT datetime;
	
	SET @ST_DT = DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0) 
	SET @ED_DT = DATEADD(s, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0))
	
	SELECT 
	SQ_Loss.loss_master_calculation_id,
	SQ_Loss.TypeBureauCode,
	SQ_Loss.loss_master_run_date,
	SQ_Loss.pol_key,
	SQ_Loss.StateProvinceCode,
	SQ_Loss.RatingCounty,
	SQ_Loss.paid_loss_amt,
	SQ_Loss.outstanding_amt,
	SQ_Loss.new_claim_count,
	SQ_Loss.class_code,
	SQ_Loss.terrorism_risk_ind,
	SQ_Loss.InsuranceSegmentDescription,
	SQ_Loss.PolicyOfferingDescription,
	SQ_Loss.pol_term,
	SQ_Loss.CauseOfLossID,
	SQ_Loss.claim_loss_date,
	SQ_Loss.ZipPostalCode,
	SQ_Loss.claimant_cov_det_id,
	SQ_Loss.s3p_claim_num,
	SQ_Loss.claim_occurrence_num,
	SQ_Loss.pol_eff_date,
	CASE WHEN SQ_Loss.PolicySourceID = 'DUC' THEN 'DCT' ELSE 'PMS' END AS source_sys_id,
	SQ_Loss.claimant_cov_det_ak_id,
	SQ_Loss.TerrorismInd,
	SQ_Loss.trans_date,
	SQ_Loss.CoverageType,
	SQ_Loss.CoverageAKID,
	SQ_Loss.DirectALAEOutstandingER,
	SQ_Loss.DirectALAEPaidIR,
	SQ_Loss.FinancialTypeCode
	FROM
	(
	SELECT 
	LMC.loss_master_calculation_id,
	CT.claim_trans_id,
	PC.TypeBureauCode,
	LMC.loss_master_run_date,
	OCC.pol_key,
	RL.StateProvinceCode,
	RL.RatingCounty,
	(case when LMC.trans_kind_code = 'D' then  LMC.paid_loss_amt else 0 end) as paid_loss_amt,
	(Case when LMC.financialtypecode = 'D' and LMC.trans_kind_code = 'D' Then LMC.outstanding_amt Else 0 End ) as outstanding_amt,
	LMC.new_claim_count,
	LMC.class_code,
	POL.terrorism_risk_ind,
	ISG.InsuranceSegmentDescription,
	POF.PolicyOfferingDescription,
	POL.pol_term,
	CCD.CauseOfLossID,
	OCC.claim_loss_date,
	RL.ZipPostalCode,
	LMC.trans_kind_code,
	ISG.InsuranceSegmentCode,
	CCD.claimant_cov_det_id,
	OCC.s3p_claim_num,
	OCC.claim_occurrence_num,
	POL.pol_eff_date,
	CCD.PolicySourceID,
	CCD.claimant_cov_det_ak_id,
	SC.StatisticalCoverageAKID as CoverageAKID,
	CASE WHEN LTRIM(RTRIM(MajorPerilCode))='919' THEN '1' ELSE 'N/A' END AS TerrorismInd,
	CASE WHEN CT.trans_date<@{pipeline().parameters.FIRSTQMONTH} THEN LMC.loss_master_run_date ELSE CT.trans_date END AS trans_date,
	'N/A' as CoverageType,
	(case when LMC.financialtypecode = 'E' and LMC.trans_kind_code = 'D' then LMF.eom_unpaid_loss_adjust_exp else 0 End) as DirectALAEOutstandingER,
	(case when LMC.trans_kind_code = 'D' then  LMF.paid_exp_amt else 0 end) as DirectALAEPaidIR,
	LMC.financialtypecode
	
	FROM 
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_calculation LMC
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction CT
	on LMC.claim_trans_ak_id=CT.claim_trans_ak_id and LMC.crrnt_snpsht_flag=1 and CT.crrnt_snpsht_flag=1
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.vwLossMasterFact LMF
	on LMC.loss_master_calculation_id=LMF.edw_loss_master_calculation_pk_id
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim IRCD
	on LMF.InsuranceReferenceCoverageDimId=IRCD.InsuranceReferenceCoverageDimId
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD
	ON CT.claimant_cov_det_ak_id= CCD.claimant_cov_det_ak_id AND CCD.crrnt_snpsht_flag = 1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence CPO
	ON CPO.claim_party_occurrence_ak_id=CCD.claim_party_occurrence_ak_id AND CPO.Crrnt_snpsht_flag = 1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence OCC
	ON CPO.claim_occurrence_ak_id= OCC.claim_occurrence_ak_id AND  OCC.crrnt_snpsht_flag = 1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL with (nolock)
	on POL.pol_ak_id=OCC.pol_key_ak_id  AND  POL.crrnt_snpsht_flag = 1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISG
	on POL.InsuranceSegmentAKId=ISG.InsuranceSegmentAKId
	and ISG.CurrentSnapshotFlag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering POF
	on POL.PolicyOfferingAKId = POF.PolicyOfferingAKId and POF.CurrentSnapshotFlag = 1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
	on SC.StatisticalCoverageAKID=CCD.StatisticalCoverageAKID
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	on PC.PolicyCoverageAKID=SC.PolicyCoverageAKID
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	on PC.RiskLocationAKID=RL.RiskLocationAKID
	WHERE ( PC.TypeBureauCode in('IM','PI') OR 
			     ( LTRIM(RTRIM(IRCD.CoverageSummaryDescription))='SMART Inland Marine' AND IRCD.CoverageCode NOT IN 
				('VALPAP','ACCREC') )  )
	AND LMF.audit_id <> -9
	@{pipeline().parameters.WHERE_CLAUSE_2}
	
	UNION ALL
	
	SELECT 
	LMC.loss_master_calculation_id,
	CT.claim_trans_id,
	PC.TypeBureauCode,
	LMC.loss_master_run_date,
	OCC.pol_key,
	RL.StateProvinceCode,
	RL.RatingCounty,
	(case when LMC.trans_kind_code = 'D' then  LMC.paid_loss_amt else 0 end) as paid_loss_amt,
	(Case when LMC.financialtypecode = 'D' and LMC.trans_kind_code = 'D' Then LMC.outstanding_amt Else 0 End ) as outstanding_amt,
	LMC.new_claim_count,
	LMC.class_code,
	POL.terrorism_risk_ind,
	ISG.InsuranceSegmentDescription,
	POF.PolicyOfferingDescription,
	POL.pol_term,
	CCD.CauseOfLossID,
	OCC.claim_loss_date,
	RL.ZipPostalCode,
	LMC.trans_kind_code,
	ISG.InsuranceSegmentCode,
	CCD.claimant_cov_det_id,
	OCC.s3p_claim_num,
	OCC.claim_occurrence_num,
	POL.pol_eff_date,
	CCD.PolicySourceID,
	CCD.claimant_cov_det_ak_id,
	RC.RatingCoverageAKID as CoverageAKID,
	CASE WHEN LTRIM(RTRIM(RC.CoverageType)) in ('Tria','Terrorism') THEN '1' ELSE 'N/A' END AS TerrorismInd,
	CASE WHEN CT.trans_date<@{pipeline().parameters.FIRSTQMONTH} THEN LMC.loss_master_run_date ELSE CT.trans_date END AS trans_date,
	RC.CoverageType as CoverageType,
	(case when LMC.financialtypecode = 'E' and LMC.trans_kind_code = 'D' then LMF.eom_unpaid_loss_adjust_exp else 0 End) as DirectALAEOutstandingER,
	(case when LMC.trans_kind_code = 'D' then  LMF.paid_exp_amt else 0 end) as DirectALAEPaidIR,
	LMC.financialtypecode
	FROM 
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_calculation LMC
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction CT
	on LMC.claim_trans_ak_id=CT.claim_trans_ak_id
	and LMC.crrnt_snpsht_flag=1
	and CT.crrnt_snpsht_flag=1
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.vwLossMasterFact LMF
	on LMC.loss_master_calculation_id=LMF.edw_loss_master_calculation_pk_id
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim IRCD
	on LMF.InsuranceReferenceCoverageDimId=IRCD.InsuranceReferenceCoverageDimId
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD
	ON CT.claimant_cov_det_ak_id= CCD.claimant_cov_det_ak_id
	AND CCD.crrnt_snpsht_flag = 1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence CPO
	ON CPO.claim_party_occurrence_ak_id=CCD.claim_party_occurrence_ak_id AND CPO.Crrnt_snpsht_flag = 1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence OCC
	ON CPO.claim_occurrence_ak_id= OCC.claim_occurrence_ak_id AND  OCC.crrnt_snpsht_flag = 1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL with (nolock)
	on POL.pol_ak_id=OCC.pol_key_ak_id AND POL.crrnt_snpsht_flag = 1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISG
	on POL.InsuranceSegmentAKId=ISG.InsuranceSegmentAKId
	and ISG.CurrentSnapshotFlag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering POF
	on POL.PolicyOfferingAKId = POF.PolicyOfferingAKId
	and POF.CurrentSnapshotFlag = 1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	on CCD.RatingCoverageAKID=RC.RatingCoverageAKID
	and (case when LMC.trans_offset_onset_ind='O'  and LMC.pms_acct_entered_date != '1800-01-01 01:00:00.000'
	then LMC.pms_acct_entered_date
	else DATEADD(D,1,LMC.loss_master_run_date)  end) between RC.EffectiveDate and RC.ExpirationDate
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	on PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
	and PC.CurrentSnapshotFlag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	on PC.RiskLocationAKID=RL.RiskLocationAKID
	and RL.CurrentSnapshotFlag=1
	WHERE ( PC.TypeBureauCode in ('InlandMarine','GamesOfChance','HoleInOne','EventCancellation') OR 
			  	(LTRIM(RTRIM(IRCD.CoverageSummaryDescription))='SMART Inland Marine' AND IRCD.CoverageCode NOT IN 
				('VALPAP','ACCREC') )  )
	AND LMF.audit_id <> -9
	@{pipeline().parameters.WHERE_CLAUSE_2}
	)SQ_Loss
	where SQ_Loss.trans_kind_code='D'
	AND SQ_Loss.InsuranceSegmentCode IN ('1','2')
	AND SQ_Loss.loss_master_run_date BETWEEN @ST_DT AND @ED_DT
	AND (SQ_Loss.paid_loss_amt<>0 or SQ_Loss.outstanding_amt<>0 OR SQ_Loss.DirectALAEOutstandingER <> 0 OR SQ_Loss.DirectALAEPaidIR <> 0)
	ORDER BY SQ_Loss.loss_master_calculation_id
),
AGG_Remove_Duplicate AS (
	SELECT
	loss_master_calculation_id,
	TypeBureauCode,
	loss_master_run_date,
	pol_key,
	StateProvinceCode,
	RatingCounty,
	paid_loss_amt,
	outstanding_amt,
	new_claim_count,
	class_code,
	terrorism_risk_ind,
	InsuranceSegmentDescription,
	PolicyOfferingDescription,
	pol_term,
	CauseOfLossID,
	claim_loss_date,
	ZipPostalCode,
	claimant_cov_det_id,
	s3p_claim_num,
	claim_occurrence_num,
	pol_eff_date,
	source_sys_id,
	claimant_cov_det_ak_id,
	TerrorismInd,
	trans_date,
	CoverageType,
	CoverageAKID,
	DirectALAEOutstandingER,
	DirectALAEPaidIR,
	FinancialTypeCode
	FROM SQ_Loss
	QUALIFY ROW_NUMBER() OVER (PARTITION BY loss_master_calculation_id ORDER BY NULL) = 1
),
LKP_PremiumTransactionAttributes AS (
	SELECT
	PremiumTransactionAKID,
	Exposure,
	CoverageEffectiveDate,
	CoverageExpirationDate,
	DeductibleAmount,
	CoverageAKId
	FROM (
		SELECT 
		PT.PremiumTransactionAKID AS PremiumTransactionAKID,
		PT.Exposure as Exposure,
		PT.PremiumTransactionEffectiveDate as CoverageEffectiveDate,
		PT.PremiumTransactionExpirationDate as CoverageExpirationDate,
		PT.DeductibleAmount AS DeductibleAmount,
		PT.StatisticalCoverageAKID AS CoverageAKID 
		FROM PremiumTransaction PT
		inner join dbo.StatisticalCoverage SC
		on SC.StatisticalCoverageAKID=PT.StatisticalCoverageAKID AND SC.CurrentSnapshotFlag = 1
		inner join dbo.PolicyCoverage PC
		on PC.PolicyCoverageAKID=SC.PolicyCoverageAKID AND PC.CurrentSnapshotFlag = 1
		join dbo.RiskLocation RL
		on PC.RiskLocationAKID=RL.RiskLocationAKID and RL.CurrentSnapshotFlag=1
		inner join V2.policy P ON RL.PolicyAKID = P.pol_ak_id AND P.crrnt_snpsht_flag=1
		WHERE (PC.TypeBureauCode in ('IM','PI') OR PC.InsuranceLine = 'BP')
		AND p.pol_ak_id IN  (SELECT distinct LMC.pol_ak_id from DBO.loss_master_calculation LMC WHERE 
		(DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,LMC.loss_master_run_date),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},-1)))
		
		UNION ALL 
		
		SELECT 
		PT.PremiumTransactionAKID AS PremiumTransactionAKID,
		RC.Exposure as Exposure,
		PT.PremiumTransactionEffectiveDate as CoverageEffectiveDate,
		PT.PremiumTransactionExpirationDate as CoverageExpirationDate,
		PT.DeductibleAmount AS DeductibleAmount,
		PT.RatingCoverageAKID as CoverageAKID
		from 
		DBO.PremiumTransaction PT
		INNER JOIN dbo.RatingCoverage RC ON PT.RatingCoverageAKID = RC.RatingCoverageAKID AND RC.EffectiveDate=PT.EffectiveDate
		INNER JOIN dbo.PolicyCoverage PC ON PC.PolicyCoverageAKID=RC.PolicyCoverageAKID and PC.CurrentSnapshotFlag=1
		INNER JOIN dbo.RiskLocation RL ON PC.RiskLocationAKID=RL.RiskLocationAKID and RL.CurrentSnapshotFlag=1 
		INNER JOIN V2.policy P on P.pol_ak_id = RL.Policyakid and P.crrnt_snpsht_flag = 1
		WHERE PT.sourcesystemid = 'DCT' AND RC.sourcesystemid = 'DCT' 
		AND ( PC.TypeBureauCode in ('InlandMarine','GamesOfChance','HoleInOne') OR PC.InsuranceLine = 'BP') 
		AND P.pol_ak_id IN  (SELECT distinct LMC.pol_ak_id from DBO.loss_master_calculation LMC WHERE 
		(DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,LMC.loss_master_run_date),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},-1)))
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageAKId ORDER BY PremiumTransactionAKID) = 1
),
LKP_WorkAAISExtract_Loss AS (
	SELECT
	EDWLossMasterCalculationPKId
	FROM (
		SELECT 
			EDWLossMasterCalculationPKId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkAAISExtract
		WHERE LossMasterRunDate BETWEEN @{pipeline().parameters.FIRSTQMONTH} AND @{pipeline().parameters.LASTQMONTH} and EDWLossMasterCalculationPKId<>-1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWLossMasterCalculationPKId ORDER BY EDWLossMasterCalculationPKId) = 1
),
EXP_Default AS (
	SELECT
	AGG_Remove_Duplicate.loss_master_calculation_id,
	AGG_Remove_Duplicate.TypeBureauCode,
	AGG_Remove_Duplicate.loss_master_run_date,
	AGG_Remove_Duplicate.pol_key,
	AGG_Remove_Duplicate.StateProvinceCode,
	AGG_Remove_Duplicate.RatingCounty,
	AGG_Remove_Duplicate.paid_loss_amt,
	AGG_Remove_Duplicate.outstanding_amt,
	AGG_Remove_Duplicate.new_claim_count,
	AGG_Remove_Duplicate.class_code,
	AGG_Remove_Duplicate.terrorism_risk_ind,
	AGG_Remove_Duplicate.InsuranceSegmentDescription,
	AGG_Remove_Duplicate.PolicyOfferingDescription,
	AGG_Remove_Duplicate.pol_term,
	AGG_Remove_Duplicate.CauseOfLossID,
	AGG_Remove_Duplicate.claim_loss_date,
	AGG_Remove_Duplicate.ZipPostalCode,
	AGG_Remove_Duplicate.claimant_cov_det_id,
	AGG_Remove_Duplicate.s3p_claim_num,
	AGG_Remove_Duplicate.claim_occurrence_num,
	AGG_Remove_Duplicate.pol_eff_date,
	AGG_Remove_Duplicate.source_sys_id,
	AGG_Remove_Duplicate.claimant_cov_det_ak_id,
	AGG_Remove_Duplicate.TerrorismInd,
	AGG_Remove_Duplicate.trans_date,
	AGG_Remove_Duplicate.CoverageType,
	AGG_Remove_Duplicate.CoverageAKID,
	LKP_WorkAAISExtract_Loss.EDWLossMasterCalculationPKId,
	LKP_PremiumTransactionAttributes.PremiumTransactionAKID,
	LKP_PremiumTransactionAttributes.Exposure,
	LKP_PremiumTransactionAttributes.CoverageEffectiveDate,
	LKP_PremiumTransactionAttributes.CoverageExpirationDate,
	LKP_PremiumTransactionAttributes.DeductibleAmount,
	AGG_Remove_Duplicate.DirectALAEOutstandingER,
	AGG_Remove_Duplicate.DirectALAEPaidIR,
	AGG_Remove_Duplicate.FinancialTypeCode
	FROM AGG_Remove_Duplicate
	LEFT JOIN LKP_PremiumTransactionAttributes
	ON LKP_PremiumTransactionAttributes.CoverageAKId = AGG_Remove_Duplicate.CoverageAKID
	LEFT JOIN LKP_WorkAAISExtract_Loss
	ON LKP_WorkAAISExtract_Loss.EDWLossMasterCalculationPKId = AGG_Remove_Duplicate.loss_master_calculation_id
),
SRT_Data AS (
	SELECT
	claimant_cov_det_ak_id, 
	loss_master_run_date, 
	loss_master_calculation_id, 
	TypeBureauCode, 
	pol_key, 
	StateProvinceCode, 
	RatingCounty, 
	paid_loss_amt, 
	outstanding_amt, 
	new_claim_count, 
	class_code, 
	terrorism_risk_ind, 
	Exposure, 
	InsuranceSegmentDescription, 
	PolicyOfferingDescription, 
	pol_term, 
	CauseOfLossID, 
	claim_loss_date, 
	ZipPostalCode, 
	CoverageEffectiveDate, 
	CoverageExpirationDate, 
	claimant_cov_det_id, 
	s3p_claim_num, 
	claim_occurrence_num, 
	pol_eff_date, 
	source_sys_id, 
	EDWLossMasterCalculationPKId, 
	TerrorismInd, 
	DeductibleAmount, 
	trans_date, 
	CoverageType, 
	DirectALAEOutstandingER, 
	DirectALAEPaidIR, 
	FinancialTypeCode
	FROM EXP_Default
	ORDER BY claimant_cov_det_ak_id ASC, loss_master_run_date ASC
),
LKP_InceptionToDatePaidLossAmount AS (
	SELECT
	InceptionToDatePaidLossAmount,
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
		--f.direct_loss_outstanding_excluding_recoveries AS OutstandingAmount, --US-403724 Commenting out since we don't need it
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
		--Join added for US-403724
		left join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.vwLossMasterFact lmf
		on lmf.claimant_cov_dim_id = d.claimant_cov_dim_id
		join loss_master_calculation lmc
		on lmc.loss_master_calculation_id = lmf.edw_loss_master_calculation_pk_id
		UNION ALL
		SELECT f.DirectLossPaidIR AS InceptionToDatePaidLossAmount,  
		--f.DirectLossOutstandingER AS OutstandingAmount, --US-403724 Commenting out since we don't need it
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
		--Join added for US-403724
		join loss_master_calculation lmc
		on lmc.loss_master_calculation_id = f.edw_loss_master_calculation_pk_id
		) T
		) T
		WHERE cast(trans_date as date)<=@{pipeline().parameters.LASTQMONTH} 
		ORDER BY pol_key,edw_claimant_cov_det_ak_id,trans_date
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key,edw_claimant_cov_det_ak_id,trans_date,loss_master_calculation_id ORDER BY InceptionToDatePaidLossAmount DESC) = 1
),
EXP_Calculate_Cummulative_Loss_Paid AS (
	SELECT
	LKP_InceptionToDatePaidLossAmount.InceptionToDatePaidLossAmount,
	-- *INF*: IIF(ISNULL(InceptionToDatePaidLossAmount),0,InceptionToDatePaidLossAmount)
	IFF(InceptionToDatePaidLossAmount IS NULL, 0, InceptionToDatePaidLossAmount) AS var_InceptionToDatePaidAmount,
	var_InceptionToDatePaidAmount AS o_InceptionToDatePaidAmount,
	SRT_Data.paid_loss_amt,
	-- *INF*: IIF(ISNULL(paid_loss_amt),0,paid_loss_amt)
	IFF(paid_loss_amt IS NULL, 0, paid_loss_amt) AS o_paid_loss_amt,
	SRT_Data.outstanding_amt AS OutstandingAmount,
	-- *INF*: IIF(ISNULL(OutstandingAmount),0,OutstandingAmount)
	IFF(OutstandingAmount IS NULL, 0, OutstandingAmount) AS o_OutstandingAmount
	FROM SRT_Data
	LEFT JOIN LKP_InceptionToDatePaidLossAmount
	ON LKP_InceptionToDatePaidLossAmount.pol_key = SRT_Data.pol_key AND LKP_InceptionToDatePaidLossAmount.edw_claimant_cov_det_ak_id = SRT_Data.claimant_cov_det_ak_id AND LKP_InceptionToDatePaidLossAmount.trans_date <= SRT_Data.trans_date AND LKP_InceptionToDatePaidLossAmount.loss_master_calculation_id = SRT_Data.loss_master_calculation_id
),
FIL_Exists_Loss AS (
	SELECT
	SRT_Data.EDWLossMasterCalculationPKId AS LKP_LossMasterCalculationId, 
	SRT_Data.loss_master_calculation_id, 
	SRT_Data.TypeBureauCode, 
	SRT_Data.loss_master_run_date, 
	SRT_Data.pol_key, 
	SRT_Data.StateProvinceCode, 
	SRT_Data.RatingCounty, 
	EXP_Calculate_Cummulative_Loss_Paid.o_paid_loss_amt AS paid_loss_amt, 
	EXP_Calculate_Cummulative_Loss_Paid.o_OutstandingAmount AS outstanding_amt, 
	SRT_Data.new_claim_count, 
	SRT_Data.class_code AS ClassCode, 
	SRT_Data.terrorism_risk_ind, 
	SRT_Data.Exposure, 
	SRT_Data.InsuranceSegmentDescription, 
	SRT_Data.PolicyOfferingDescription, 
	SRT_Data.pol_term, 
	SRT_Data.CauseOfLossID, 
	SRT_Data.claim_loss_date, 
	SRT_Data.ZipPostalCode, 
	SRT_Data.CoverageEffectiveDate, 
	SRT_Data.CoverageExpirationDate, 
	SRT_Data.claimant_cov_det_id, 
	SRT_Data.s3p_claim_num, 
	EXP_Calculate_Cummulative_Loss_Paid.o_InceptionToDatePaidAmount AS direct_loss_paid_excluding_recoveries, 
	SRT_Data.claim_occurrence_num, 
	SRT_Data.pol_eff_date, 
	SRT_Data.source_sys_id, 
	SRT_Data.TerrorismInd, 
	SRT_Data.DeductibleAmount, 
	SRT_Data.CoverageType, 
	SRT_Data.DirectALAEOutstandingER, 
	SRT_Data.DirectALAEPaidIR, 
	SRT_Data.FinancialTypeCode
	FROM EXP_Calculate_Cummulative_Loss_Paid
	 -- Manually join with SRT_Data
	WHERE ISNULL(LKP_LossMasterCalculationId)
),
EXP_Cleansing_Loss AS (
	SELECT
	loss_master_calculation_id AS i_loss_master_calculation_id,
	TypeBureauCode AS i_TypeBureauCode,
	loss_master_run_date AS i_loss_master_run_date,
	pol_key AS i_pol_key,
	StateProvinceCode AS i_StateProvinceCode,
	RatingCounty AS i_RatingCounty,
	paid_loss_amt AS i_paid_loss_amt,
	outstanding_amt AS i_outstanding_amt,
	new_claim_count AS i_new_claim_count,
	ClassCode AS i_ClassCode,
	terrorism_risk_ind AS i_terrorism_risk_ind,
	Exposure AS i_Exposure,
	InsuranceSegmentDescription AS i_InsuranceSegmentDescription,
	PolicyOfferingDescription AS i_PolicyOfferingDescription,
	pol_term AS i_pol_term,
	CauseOfLossID AS i_CauseOfLossID,
	claim_loss_date AS i_claim_loss_date,
	ZipPostalCode AS i_ZipPostalCode,
	CoverageEffectiveDate AS i_CoverageEffectiveDate,
	CoverageExpirationDate AS i_CoverageExpirationDate,
	claimant_cov_det_id AS i_claimant_cov_det_id,
	s3p_claim_num AS i_s3p_claim_num,
	direct_loss_paid_excluding_recoveries AS i_direct_loss_paid_including_recoveries,
	claim_occurrence_num AS i_claim_occurrence_num,
	pol_eff_date AS i_pol_eff_date,
	TerrorismInd AS i_TerrorismInd,
	FinancialTypeCode AS i_FinancialTypeCode,
	i_loss_master_calculation_id AS o_loss_master_calculation_id,
	-- *INF*: RTRIM(LTRIM(i_TypeBureauCode))
	RTRIM(LTRIM(i_TypeBureauCode)) AS o_TypeBureauCode,
	-- *INF*: TRUNC(i_loss_master_run_date,'DD')
	CAST(TRUNC(i_loss_master_run_date, 'DAY') AS TIMESTAMP_NTZ(0)) AS o_loss_master_run_date,
	-- *INF*: RTRIM(LTRIM(i_pol_key))
	RTRIM(LTRIM(i_pol_key)) AS o_pol_key,
	-- *INF*: RTRIM(LTRIM(i_StateProvinceCode))
	RTRIM(LTRIM(i_StateProvinceCode)) AS o_StateProvinceCode,
	-- *INF*: rtrim(ltrim(i_RatingCounty))
	rtrim(ltrim(i_RatingCounty)) AS o_RatingCounty,
	i_paid_loss_amt AS o_paid_loss_amt,
	i_outstanding_amt AS o_outstanding_amt,
	i_new_claim_count AS o_new_claim_count,
	-- *INF*: RTRIM(LTRIM(i_ClassCode))
	RTRIM(LTRIM(i_ClassCode)) AS o_ClassCode,
	-- *INF*: rtrim(ltrim(i_TerrorismInd))
	rtrim(ltrim(i_TerrorismInd)) AS o_terrorism_risk_ind,
	-- *INF*: IIF(ISNULL(i_Exposure) OR LENGTH(i_Exposure)=0 , 0 ,i_Exposure)
	IFF(i_Exposure IS NULL OR LENGTH(i_Exposure) = 0, 0, i_Exposure) AS o_Exposure,
	-- *INF*: RTRIM(LTRIM(i_InsuranceSegmentDescription))
	RTRIM(LTRIM(i_InsuranceSegmentDescription)) AS o_InsuranceSegmentDescription,
	-- *INF*: RTRIM(LTRIM(i_PolicyOfferingDescription))
	RTRIM(LTRIM(i_PolicyOfferingDescription)) AS o_PolicyOfferingDescription,
	-- *INF*: RTRIM(LTRIM(i_pol_term))
	RTRIM(LTRIM(i_pol_term)) AS o_pol_term,
	i_CauseOfLossID AS o_CauseOfLossID,
	-- *INF*: TRUNC(i_claim_loss_date,'DD')
	CAST(TRUNC(i_claim_loss_date, 'DAY') AS TIMESTAMP_NTZ(0)) AS o_claim_loss_date,
	-- *INF*: RTRIM(LTRIM(i_ZipPostalCode))
	RTRIM(LTRIM(i_ZipPostalCode)) AS o_ZipPostalCode,
	-- *INF*: IIF(ISNULL(i_CoverageEffectiveDate), TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),i_CoverageEffectiveDate)
	IFF(
	    i_CoverageEffectiveDate IS NULL,
	    TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    i_CoverageEffectiveDate
	) AS o_CoverageEffectiveDate,
	-- *INF*: IIF(ISNULL(i_CoverageExpirationDate), TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),i_CoverageExpirationDate)
	-- 
	-- 
	IFF(
	    i_CoverageExpirationDate IS NULL,
	    TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    i_CoverageExpirationDate
	) AS o_CoverageExpirationDate,
	i_claimant_cov_det_id AS o_ClaimantDetailId,
	-- *INF*: IIF(TO_INTEGER(GET_DATE_PART(i_pol_eff_date,'YYYY')) <= 2003  AND TO_INTEGER(GET_DATE_PART(i_pol_eff_date,'MM'))<10,SUBSTR(TO_CHAR(i_claim_loss_date),1,16) ||' '|| i_claim_occurrence_num,i_s3p_claim_num)
	IFF(
	    CAST(DATE_PART(i_pol_eff_date, 'YYYY') AS INTEGER) <= 2003
	    and CAST(DATE_PART(i_pol_eff_date, 'MM') AS INTEGER) < 10,
	    SUBSTR(TO_CHAR(i_claim_loss_date), 1, 16) || ' ' || i_claim_occurrence_num,
	    i_s3p_claim_num
	) AS o_ClaimNumber,
	-- *INF*: IIF(i_FinancialTypeCode='E',0, IIF(IsNull(i_direct_loss_paid_including_recoveries),0, i_direct_loss_paid_including_recoveries))
	IFF(
	    i_FinancialTypeCode = 'E', 0,
	    IFF(
	        i_direct_loss_paid_including_recoveries IS NULL, 0,
	        i_direct_loss_paid_including_recoveries
	    )
	) AS o_direct_loss_paid_including_recoveries,
	source_sys_id,
	DeductibleAmount,
	CoverageType,
	DirectALAEOutstandingER,
	DirectALAEPaidIR
	FROM FIL_Exists_Loss
),
LKP_BureauStatisticalCode_Loss AS (
	SELECT
	BureauCode3,
	BureauCode4,
	o_loss_master_calculation_id,
	loss_master_calculation_id
	FROM (
		SELECT lmc.loss_master_calculation_id as loss_master_calculation_id,b.BureauCode3 as BureauCode3, b.BureauCode4 as BureauCode4
		  FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.BureauStatisticalCode b
		  inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_calculation  lmc on 
		          lmc.BureauStatisticalCodeAKID = b.BureauStatisticalCodeAKID
		  inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage s on
		         lmc.StatisticalCoverageAKID = s.StatisticalCoverageAKID
		  inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage p on 
		          p.PolicyCoverageAKID = s.PolicyCoverageAKID
		  where p.TypeBureauCode in ('PI')
		 and
		 S.ClassCode IN ('335','336','337','338','800') 
		 and lmc.source_sys_id='PMS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY loss_master_calculation_id ORDER BY BureauCode3) = 1
),
EXP_Logic_Loss AS (
	SELECT
	-1 AS PremiumMasterCalculationID,
	EXP_Cleansing_Loss.o_loss_master_calculation_id AS loss_master_calculation_id,
	EXP_Cleansing_Loss.o_TypeBureauCode AS TypeBureauCode,
	-- *INF*: TO_DATE('1800-01-01','YYYY-MM-DD')
	TO_TIMESTAMP('1800-01-01', 'YYYY-MM-DD') AS PremiumMasterRunDate,
	EXP_Cleansing_Loss.o_loss_master_run_date AS loss_master_run_date,
	EXP_Cleansing_Loss.o_pol_key AS pol_key,
	EXP_Cleansing_Loss.o_StateProvinceCode AS StateProvinceCode,
	EXP_Cleansing_Loss.o_RatingCounty AS RatingCounty,
	0.00 AS PremiumMasterPremium,
	EXP_Cleansing_Loss.o_paid_loss_amt AS paid_loss_amt,
	EXP_Cleansing_Loss.o_outstanding_amt AS outstanding_amt,
	EXP_Cleansing_Loss.o_new_claim_count AS new_claim_count,
	EXP_Cleansing_Loss.o_ClassCode AS ClassCode,
	'N/A' AS PremiumMasterClassCode_out,
	ClassCode AS LossMasterClassCode_out,
	EXP_Cleansing_Loss.DeductibleAmount,
	EXP_Cleansing_Loss.o_terrorism_risk_ind AS i_terrorism_risk_ind,
	-- *INF*: i_terrorism_risk_ind
	-- 
	-- --DECODE(i_terrorism_risk_ind,'0','0','1','1','N','0','Y','1','N/A')
	i_terrorism_risk_ind AS o_terrorism_risk_ind,
	EXP_Cleansing_Loss.o_Exposure AS i_Exposure,
	-- *INF*: to_char(i_Exposure)
	to_char(i_Exposure) AS o_Exposure,
	EXP_Cleansing_Loss.source_sys_id AS Source_Sys_Id,
	LKP_BureauStatisticalCode_Loss.BureauCode3 AS lkp_ConstructionCode,
	-- *INF*: IIF(Source_Sys_Id='PMS',IIF(ISNULL(lkp_ConstructionCode) or LENGTH(lkp_ConstructionCode)=0, '00', lkp_ConstructionCode),'N/A')
	IFF(
	    Source_Sys_Id = 'PMS',
	    IFF(
	        lkp_ConstructionCode IS NULL
	    or LENGTH(lkp_ConstructionCode) = 0, '00',
	        lkp_ConstructionCode
	    ),
	    'N/A'
	) AS ConstructionCode,
	LKP_BureauStatisticalCode_Loss.BureauCode4 AS lkp_IsoFireProtectionCode,
	-- *INF*: IIF(Source_Sys_Id='PMS',IIF(ISNULL(lkp_IsoFireProtectionCode) or LENGTH(lkp_IsoFireProtectionCode)=0, '00', lkp_IsoFireProtectionCode),'N/A')
	IFF(
	    Source_Sys_Id = 'PMS',
	    IFF(
	        lkp_IsoFireProtectionCode IS NULL
	    or LENGTH(lkp_IsoFireProtectionCode) = 0, '00',
	        lkp_IsoFireProtectionCode
	    ),
	    'N/A'
	) AS IsoFireProtectionCode,
	EXP_Cleansing_Loss.o_InsuranceSegmentDescription AS InsuranceSegmentDescription,
	EXP_Cleansing_Loss.o_PolicyOfferingDescription AS i_PolicyOfferingDescription,
	-- *INF*: IIF(IN(i_PolicyOfferingDescription,'CPP','SBOP','Home & Highway', 'CBOP', 'SMARTbusiness' ),i_PolicyOfferingDescription,'Unassigned')
	IFF(
	    i_PolicyOfferingDescription IN ('CPP','SBOP','Home & Highway','CBOP','SMARTbusiness'),
	    i_PolicyOfferingDescription,
	    'Unassigned'
	) AS o_PolicyOfferingDescription,
	EXP_Cleansing_Loss.o_pol_term AS i_pol_term,
	-- *INF*: IIF(ISNULL(i_pol_term),'N/A',i_pol_term)
	IFF(i_pol_term IS NULL, 'N/A', i_pol_term) AS o_pol_term,
	EXP_Cleansing_Loss.o_CauseOfLossID AS i_CauseOfLossID,
	-- *INF*: :LKP.LKP_SUP_CAUSEOFLOSS_CAUSEOFLOSSNAME(i_CauseOfLossID)
	LKP_SUP_CAUSEOFLOSS_CAUSEOFLOSSNAME_i_CauseOfLossID.CauseOfLossName AS v_CauseOfLossName,
	-- *INF*: IIF(ISNULL(v_CauseOfLossName),'N/A',v_CauseOfLossName)
	IFF(v_CauseOfLossName IS NULL, 'N/A', v_CauseOfLossName) AS o_CauseOfLossName,
	EXP_Cleansing_Loss.o_claim_loss_date AS claim_loss_date,
	EXP_Cleansing_Loss.o_ZipPostalCode AS i_ZipPostalCode,
	-- *INF*: i_ZipPostalCode
	-- 
	-- --DECODE(LENGTH(i_ZipPostalCode),
	-- --6,DECODE(TRUE,SUBSTR(i_ZipPostalCode,1,1)='0',SUBSTR(i_ZipPostalCode,2),'00000'),
	-- --5,i_ZipPostalCode,
	-- --10,DECODE(TRUE,INSTR(i_ZipPostalCode,'-',1,1)=6,SUBSTR(i_ZipPostalCode,7),'0000'),
	-- --'00000'
	-- --)
	i_ZipPostalCode AS o_ZipPostalCode,
	EXP_Cleansing_Loss.o_CoverageEffectiveDate AS CoverageEffectiveDate,
	EXP_Cleansing_Loss.o_CoverageExpirationDate AS CoverageExpirationDate,
	EXP_Cleansing_Loss.o_ClaimantDetailId AS ClaimantDetailId,
	EXP_Cleansing_Loss.o_ClaimNumber AS ClaimNumber,
	EXP_Cleansing_Loss.o_direct_loss_paid_including_recoveries AS DirectLossPaidIncludingRecoveries,
	EXP_Cleansing_Loss.CoverageType,
	EXP_Cleansing_Loss.DirectALAEOutstandingER AS OutstandingAllocatedLossAdjustmentExpenseAmount,
	EXP_Cleansing_Loss.DirectALAEPaidIR AS PaidAllocatedlossAdjustmentExpenseAmount
	FROM EXP_Cleansing_Loss
	LEFT JOIN LKP_BureauStatisticalCode_Loss
	ON LKP_BureauStatisticalCode_Loss.loss_master_calculation_id = EXP_Cleansing_Loss.o_loss_master_calculation_id
	LEFT JOIN LKP_SUP_CAUSEOFLOSS_CAUSEOFLOSSNAME LKP_SUP_CAUSEOFLOSS_CAUSEOFLOSSNAME_i_CauseOfLossID
	ON LKP_SUP_CAUSEOFLOSS_CAUSEOFLOSSNAME_i_CauseOfLossID.CauseOfLossId = i_CauseOfLossID

),
SQ_Premium AS (
	Declare @StartTime datetime
	Declare @EndTime datetime
	
	SET @StartTime = DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0) 
	SET @EndTime = DATEADD(s, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0))
	
	SELECT 
	PMC.PremiumMasterCalculationID,
	PC.TypeBureauCode,
	PMC.PremiumMasterRunDate,
	PMC.PolicyAKID,
	PMC.PolicyKey,
	RL.StateProvinceCode,
	RL.RatingCounty,
	PMC.PremiumMasterPremium,
	SC.ClassCode,
	PT.DeductibleAmount,
	POL.terrorism_risk_ind,
	PMC.PremiumMasterExposure,
	PT.ConstructionCode,
	PT.SourceSystemID,
	ISNULL(CDIM.IsoFireProtectionCode, CDCP.IsoFireProtectionCode) IsoFireProtectionCode,
	ISG.InsuranceSegmentDescription,
	POF.PolicyOfferingDescription,
	POL.pol_term,
	RL.ZipPostalCode,
	PT.PremiumTransactionEffectiveDate CoverageEffectiveDate,
	PT.PremiumTransactionExpirationDate CoverageExpirationDate,
	CASE WHEN LTRIM(RTRIM(MajorPerilCode))='919' THEN '1' ELSE 'N/A' END AS TerrorismInd,
	PT.PMSFunctionCode,
	PC.InsuranceLine,
	RL.LocationUnitNumber,
	SC.SubLocationUnitNumber,
	SC.RiskUnitGroup,
	SC.RiskUnitGroupSequenceNumber,
	SC.RiskUnit,
	SC.RiskUnitSequenceNumber,
	SC.PMSTypeExposure,
	SC.MajorPerilCode,
	SC.MajorPerilSequenceNumber,
	PMC.PremiumTransactionEnteredDate,
	PMC.PremiumMasterRecordType,
	PMC.RatingCoverageAKId,
	'N/A' as CoverageType
	from @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterFact PMF with(nolock)
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim IRCD
	on PMF.InsuranceReferenceCoverageDimId=IRCD.InsuranceReferenceCoverageDimId
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation PMC 
	on PMF.EDWPremiumMasterCalculationPKId=PMC.PremiumMasterCalculationId
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	on PT.PremiumTransactionAKID=PMC.PremiumTransactionAKID
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
	on SC.StatisticalCoverageAKID=PT.StatisticalCoverageAKID
	and SC.SourceSystemID='PMS'
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	on PC.PolicyCoverageAKID=SC.PolicyCoverageAKID
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	on PC.RiskLocationAKID=RL.RiskLocationAKID
	join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL
	on POL.pol_ak_id=RL.PolicyAKID
	and POL.crrnt_snpsht_flag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISG
	on POL.InsuranceSegmentAKId=ISG.InsuranceSegmentAKId
	and ISG.CurrentSnapshotFlag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering POF
	on POL.PolicyOfferingAKId = POF.PolicyOfferingAKId
	and POF.CurrentSnapshotFlag = 1
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialProperty CDCP
	on CDCP.PremiumTransactionId=PT.PremiumTransactionId
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailInlandMarine CDIM
	on CDIM.PremiumTransactionId=PT.PremiumTransactionId
	where PMC.PremiumMasterRunDate BETWEEN @StartTime AND @EndTime
	AND PT.SourceSystemId='PMS' 
	AND (PC.TypeBureauCode in ('IM','PI') OR  
			(LTRIM(RTRIM(IRCD.CoverageSummaryDescription))='SMART Inland Marine' AND IRCD.CoverageCode NOT IN  ('VALPAP','ACCREC') )  )
	AND ISG.InsuranceSegmentCode IN ('1','2')
	--AND RL.StateProvinceCode IN ('12','13','14','15','16','21','22','24','34','48')
	AND PMC.PremiumMasterPremium <>0
	AND PMC.PremiumMasterPremiumType='D'
	AND PMC.PremiumMasterReasonAmendedCode NOT IN ( 'COL' , 'CWO')
	@{pipeline().parameters.WHERE_CLAUSE_1}
	
	UNION ALL
	
	SELECT 
	PMC.PremiumMasterCalculationID,
	PC.TypeBureauCode,
	PMC.PremiumMasterRunDate,
	PMC.PolicyAKID,
	PMC.PolicyKey,
	RL.StateProvinceCode,
	RL.RatingCounty,
	PMC.PremiumMasterPremium,
	RC.ClassCode,
	PT.DeductibleAmount,
	POL.terrorism_risk_ind,
	PMC.PremiumMasterExposure,
	PT.ConstructionCode,
	PT.SourceSystemID,
	ISNULL(CDIM.IsoFireProtectionCode, CDCP.IsoFireProtectionCode) IsoFireProtectionCode,
	ISG.InsuranceSegmentDescription,
	POF.PolicyOfferingDescription,
	POL.pol_term,
	RL.ZipPostalCode,
	PT.PremiumTransactionEffectiveDate CoverageEffectiveDate,
	PT.PremiumTransactionExpirationDate CoverageExpirationDate,
	CASE WHEN LTRIM(RTRIM(RC.CoverageType)) in ('Tria','Terrorism') THEN '1' ELSE 'N/A' END AS TerrorismInd,
	PT.PMSFunctionCode,
	PC.InsuranceLine,
	RL.LocationUnitNumber,
	Null as SubLocationUnitNumber,
	Null as RiskUnitGroup,
	Null as RiskUnitGroupSequenceNumber,
	Null as RiskUnit,
	Null as RiskUnitSequenceNumber,
	Null as PMSTypeExposure,
	Null as MajorPerilCode,
	Null as MajorPerilSequenceNumber,
	PMC.PremiumTransactionEnteredDate,
	PMC.PremiumMasterRecordType,
	PMC.RatingCoverageAKId,
	RC.CoverageType
	from @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterFact PMF with(nolock)
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim IRCD
	on PMF.InsuranceReferenceCoverageDimId=IRCD.InsuranceReferenceCoverageDimId
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation PMC 
	on PMF.EDWPremiumMasterCalculationPKId=PMC.PremiumMasterCalculationId
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	on PT.PremiumTransactionAKID=PMC.PremiumTransactionAKID
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	on PMC.RatingCoverageAKID=RC.RatingCoverageAKID
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
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering POF
	on POL.PolicyOfferingAKId=POF.PolicyOfferingAKId
	and POF.CurrentSnapshotFlag=1
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialProperty CDCP
	on CDCP.PremiumTransactionId=PT.PremiumTransactionId
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailInlandMarine CDIM
	on CDIM.PremiumTransactionId=PT.PremiumTransactionId
	where PMC.PremiumMasterRunDate BETWEEN @StartTime AND @EndTime
	and PT.SourceSystemId='DCT'
	AND (PC.TypeBureauCode in ('InlandMarine','GamesOfChance','HoleInOne','EventCancellation')  OR (LTRIM(RTRIM(IRCD.CoverageSummaryDescription))='SMART Inland Marine' AND IRCD.CoverageCode NOT IN 
				('VALPAP','ACCREC') )  )
	AND ISG.InsuranceSegmentCode IN ('1','2')
	--AND RL.StateProvinceCode IN ('12','13','14','15','16','21','22','24','34','48')
	AND PMC.PremiumMasterPremium <>0
	AND PMC.PremiumMasterPremiumType='D'
	AND PMC.PremiumMasterReasonAmendedCode NOT IN ( 'CWO','CWB')
	@{pipeline().parameters.WHERE_CLAUSE_1}
),
LKP_WorkAAISExtract_Premium AS (
	SELECT
	EDWPremiumMasterCalculationPKId
	FROM (
		SELECT 
			EDWPremiumMasterCalculationPKId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkAAISExtract
		WHERE PremiumMasterRunDate BETWEEN @{pipeline().parameters.FIRSTQMONTH} AND @{pipeline().parameters.LASTQMONTH} and EDWPremiumMasterCalculationPKId<>-1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWPremiumMasterCalculationPKId ORDER BY EDWPremiumMasterCalculationPKId) = 1
),
FIL_Exists_Premium AS (
	SELECT
	LKP_WorkAAISExtract_Premium.EDWPremiumMasterCalculationPKId AS LKP_PremiumMasterCalculationID, 
	SQ_Premium.PremiumMasterCalculationID, 
	SQ_Premium.TypeBureauCode, 
	SQ_Premium.PremiumMasterRunDate, 
	SQ_Premium.PolicyAKID, 
	SQ_Premium.PolicyKey AS pol_key, 
	SQ_Premium.StateProvinceCode, 
	SQ_Premium.RatingCounty, 
	SQ_Premium.PremiumMasterPremium, 
	SQ_Premium.ClassCode, 
	SQ_Premium.DeductibleAmount, 
	SQ_Premium.terrorism_risk_ind, 
	SQ_Premium.PremiumMasterExposure, 
	SQ_Premium.ConstructionCode, 
	SQ_Premium.SourceSystemID, 
	SQ_Premium.IsoFireProtectionCode, 
	SQ_Premium.InsuranceSegmentDescription, 
	SQ_Premium.PolicyOfferingDescription, 
	SQ_Premium.pol_term, 
	SQ_Premium.ZipPostalCode, 
	SQ_Premium.CoverageEffectiveDate, 
	SQ_Premium.CoverageExpirationDate, 
	SQ_Premium.TerrorismInd, 
	SQ_Premium.PMSFunctionCode, 
	SQ_Premium.InsuranceLine, 
	SQ_Premium.LocationUnitNumber, 
	SQ_Premium.SubLocationUnitNumber, 
	SQ_Premium.RiskUnitGroup, 
	SQ_Premium.RiskUnitGroupSequenceNumber, 
	SQ_Premium.RiskUnit, 
	SQ_Premium.RiskUnitSequenceNumber, 
	SQ_Premium.PMSTypeExposure, 
	SQ_Premium.MajorPerilCode, 
	SQ_Premium.MajorPerilSequenceNumber, 
	SQ_Premium.PremiumTransactionEnteredDate, 
	SQ_Premium.PremiumMasterRecordType, 
	SQ_Premium.RatingCoverageAKId, 
	SQ_Premium.CoverageType
	FROM SQ_Premium
	LEFT JOIN LKP_WorkAAISExtract_Premium
	ON LKP_WorkAAISExtract_Premium.EDWPremiumMasterCalculationPKId = SQ_Premium.PremiumMasterCalculationID
	WHERE ISNULL(LKP_PremiumMasterCalculationID)
),
EXP_Cleansing_Premium AS (
	SELECT
	PremiumMasterCalculationID AS i_PremiumMasterCalculationID,
	TypeBureauCode AS i_TypeBureauCode,
	PremiumMasterRunDate AS i_PremiumMasterRunDate,
	PolicyAKID AS i_PolicyAKID,
	pol_key AS i_pol_key,
	StateProvinceCode AS i_StateProvinceCode,
	RatingCounty AS i_RatingCounty,
	PremiumMasterPremium AS i_PremiumMasterPremium,
	ClassCode AS i_ClassCode,
	DeductibleAmount AS i_DeductibleAmount,
	terrorism_risk_ind AS i_terrorism_risk_ind,
	PremiumMasterExposure AS i_PremiumMasterExposure,
	ConstructionCode AS i_ConstructionCode,
	SourceSystemID AS i_SourceSystemID,
	IsoFireProtectionCode AS i_IsoFireProtectionCode,
	InsuranceSegmentDescription AS i_InsuranceSegmentDescription,
	PolicyOfferingDescription AS i_PolicyOfferingDescription,
	pol_term AS i_pol_term,
	ZipPostalCode AS i_ZipPostalCode,
	CoverageEffectiveDate AS i_CoverageEffectiveDate,
	CoverageExpirationDate AS i_CoverageExpirationDate,
	TerrorismInd AS i_TerrorismInd,
	PMSFunctionCode AS i_PMSFunctionCode,
	InsuranceLine AS i_InsuranceLine,
	LocationUnitNumber AS i_LocationUnitNumber,
	SubLocationUnitNumber AS i_SubLocationUnitNumber,
	RiskUnitGroup AS i_RiskUnitGroup,
	RiskUnitGroupSequenceNumber AS i_RiskUnitGroupSequenceNumber,
	RiskUnit AS i_RiskUnit,
	RiskUnitSequenceNumber AS i_RiskUnitSequenceNumber,
	PMSTypeExposure AS i_PMSTypeExposure,
	MajorPerilCode AS i_MajorPerilCode,
	MajorPerilSequenceNumber AS i_MajorPerilSequenceNumber,
	PremiumTransactionEnteredDate AS i_PremiumTransactionEnteredDate,
	PremiumMasterRecordType AS i_PremiumMasterRecordType,
	RatingCoverageAKId AS i_RatingCoverageAKId,
	CoverageType AS i_CoverageType,
	-- *INF*: IIF(i_PremiumMasterRecordType='OFFSET' AND i_SourceSystemID='PMS', :LKP.LKP_EXPOSURE_PREVIOUS_ONSET_TRANSACTION_PMS(i_PolicyAKID, i_PMSFunctionCode, i_InsuranceLine, i_LocationUnitNumber, i_SubLocationUnitNumber, i_RiskUnitGroup, i_RiskUnitGroupSequenceNumber, i_RiskUnit, i_RiskUnitSequenceNumber, i_PMSTypeExposure, i_MajorPerilCode, i_MajorPerilSequenceNumber, i_CoverageEffectiveDate, i_PremiumTransactionEnteredDate))
	IFF(
	    i_PremiumMasterRecordType = 'OFFSET' AND i_SourceSystemID = 'PMS',
	    LKP_EXPOSURE_PREVIOUS_ONSET_TRANSACTION_PMS_i_PolicyAKID_i_PMSFunctionCode_i_InsuranceLine_i_LocationUnitNumber_i_SubLocationUnitNumber_i_RiskUnitGroup_i_RiskUnitGroupSequenceNumber_i_RiskUnit_i_RiskUnitSequenceNumber_i_PMSTypeExposure_i_MajorPerilCode_i_MajorPerilSequenceNumber_i_CoverageEffectiveDate_i_PremiumTransactionEnteredDate.PremiumMasterExposure
	) AS v_Exposure_Previous_Onset_Transaction_PMS,
	-- *INF*: :LKP.LKP_EXPOSURE_PREVIOUS_ONSET_TRANSACTION_DCT(i_RatingCoverageAKId, i_CoverageEffectiveDate, i_PremiumTransactionEnteredDate)
	LKP_EXPOSURE_PREVIOUS_ONSET_TRANSACTION_DCT_i_RatingCoverageAKId_i_CoverageEffectiveDate_i_PremiumTransactionEnteredDate.Exposure AS v_Exposure_Previous_Onset_Transaction_DCT,
	i_PremiumMasterCalculationID AS o_PremiumMasterCalculationID,
	-- *INF*: RTRIM(LTRIM(i_TypeBureauCode))
	RTRIM(LTRIM(i_TypeBureauCode)) AS o_TypeBureauCode,
	-- *INF*: TRUNC(i_PremiumMasterRunDate,'DD')
	CAST(TRUNC(i_PremiumMasterRunDate, 'DAY') AS TIMESTAMP_NTZ(0)) AS o_PremiumMasterRunDate,
	-- *INF*: RTRIM(LTRIM(i_pol_key))
	RTRIM(LTRIM(i_pol_key)) AS o_pol_key,
	-- *INF*: RTRIM(LTRIM(i_StateProvinceCode))
	RTRIM(LTRIM(i_StateProvinceCode)) AS o_StateProvinceCode,
	-- *INF*: rtrim(ltrim(i_RatingCounty))
	rtrim(ltrim(i_RatingCounty)) AS o_RatingCounty,
	i_PremiumMasterPremium AS o_PremiumMasterPremium,
	-- *INF*: RTRIM(LTRIM(i_ClassCode))
	RTRIM(LTRIM(i_ClassCode)) AS o_ClassCode,
	-- *INF*: LTRIM(RTRIM(i_DeductibleAmount))
	LTRIM(RTRIM(i_DeductibleAmount)) AS o_DeductibleAmount,
	-- *INF*: RTRIM(LTRIM(i_TerrorismInd))
	RTRIM(LTRIM(i_TerrorismInd)) AS o_terrorism_risk_ind,
	-- *INF*: DECODE(TRUE,
	-- i_PremiumMasterRecordType='OFFSET' AND i_SourceSystemID='PMS' AND  NOT ISNULL(v_Exposure_Previous_Onset_Transaction_PMS), 
	-- -1 * v_Exposure_Previous_Onset_Transaction_PMS, 
	-- i_PremiumMasterRecordType='OFFSET' AND i_SourceSystemID='DCT' AND  NOT ISNULL(v_Exposure_Previous_Onset_Transaction_DCT),
	-- -1 * v_Exposure_Previous_Onset_Transaction_DCT,
	-- i_PremiumMasterExposure
	-- )
	DECODE(
	    TRUE,
	    i_PremiumMasterRecordType = 'OFFSET' AND i_SourceSystemID = 'PMS' AND v_Exposure_Previous_Onset_Transaction_PMS IS NOT NULL, - 1 * v_Exposure_Previous_Onset_Transaction_PMS,
	    i_PremiumMasterRecordType = 'OFFSET' AND i_SourceSystemID = 'DCT' AND v_Exposure_Previous_Onset_Transaction_DCT IS NOT NULL, - 1 * v_Exposure_Previous_Onset_Transaction_DCT,
	    i_PremiumMasterExposure
	) AS o_Exposure,
	-- *INF*: RTRIM(LTRIM(i_ConstructionCode))
	RTRIM(LTRIM(i_ConstructionCode)) AS o_ConstructionCode,
	-- *INF*: LTRIM(RTRIM(i_SourceSystemID))
	LTRIM(RTRIM(i_SourceSystemID)) AS o_SourceSystemID,
	-- *INF*: RTRIM(LTRIM(i_IsoFireProtectionCode))
	RTRIM(LTRIM(i_IsoFireProtectionCode)) AS o_IsoFireProtectionCode,
	-- *INF*: RTRIM(LTRIM(i_InsuranceSegmentDescription))
	RTRIM(LTRIM(i_InsuranceSegmentDescription)) AS o_InsuranceSegmentDescription,
	-- *INF*: RTRIM(LTRIM(i_PolicyOfferingDescription))
	RTRIM(LTRIM(i_PolicyOfferingDescription)) AS o_PolicyOfferingDescription,
	-- *INF*: RTRIM(LTRIM(i_pol_term))
	RTRIM(LTRIM(i_pol_term)) AS o_pol_term,
	-- *INF*: RTRIM(LTRIM(i_ZipPostalCode))
	RTRIM(LTRIM(i_ZipPostalCode)) AS o_ZipPostalCode,
	i_CoverageEffectiveDate AS o_CoverageEffectiveDate,
	i_CoverageExpirationDate AS o_CoverageExpirationDate,
	i_CoverageType AS o_CoverageType
	FROM FIL_Exists_Premium
	LEFT JOIN LKP_EXPOSURE_PREVIOUS_ONSET_TRANSACTION_PMS LKP_EXPOSURE_PREVIOUS_ONSET_TRANSACTION_PMS_i_PolicyAKID_i_PMSFunctionCode_i_InsuranceLine_i_LocationUnitNumber_i_SubLocationUnitNumber_i_RiskUnitGroup_i_RiskUnitGroupSequenceNumber_i_RiskUnit_i_RiskUnitSequenceNumber_i_PMSTypeExposure_i_MajorPerilCode_i_MajorPerilSequenceNumber_i_CoverageEffectiveDate_i_PremiumTransactionEnteredDate
	ON LKP_EXPOSURE_PREVIOUS_ONSET_TRANSACTION_PMS_i_PolicyAKID_i_PMSFunctionCode_i_InsuranceLine_i_LocationUnitNumber_i_SubLocationUnitNumber_i_RiskUnitGroup_i_RiskUnitGroupSequenceNumber_i_RiskUnit_i_RiskUnitSequenceNumber_i_PMSTypeExposure_i_MajorPerilCode_i_MajorPerilSequenceNumber_i_CoverageEffectiveDate_i_PremiumTransactionEnteredDate.PolicyAKID = i_PolicyAKID
	AND LKP_EXPOSURE_PREVIOUS_ONSET_TRANSACTION_PMS_i_PolicyAKID_i_PMSFunctionCode_i_InsuranceLine_i_LocationUnitNumber_i_SubLocationUnitNumber_i_RiskUnitGroup_i_RiskUnitGroupSequenceNumber_i_RiskUnit_i_RiskUnitSequenceNumber_i_PMSTypeExposure_i_MajorPerilCode_i_MajorPerilSequenceNumber_i_CoverageEffectiveDate_i_PremiumTransactionEnteredDate.PMSFunctionCode = i_PMSFunctionCode
	AND LKP_EXPOSURE_PREVIOUS_ONSET_TRANSACTION_PMS_i_PolicyAKID_i_PMSFunctionCode_i_InsuranceLine_i_LocationUnitNumber_i_SubLocationUnitNumber_i_RiskUnitGroup_i_RiskUnitGroupSequenceNumber_i_RiskUnit_i_RiskUnitSequenceNumber_i_PMSTypeExposure_i_MajorPerilCode_i_MajorPerilSequenceNumber_i_CoverageEffectiveDate_i_PremiumTransactionEnteredDate.InsuranceLine = i_InsuranceLine
	AND LKP_EXPOSURE_PREVIOUS_ONSET_TRANSACTION_PMS_i_PolicyAKID_i_PMSFunctionCode_i_InsuranceLine_i_LocationUnitNumber_i_SubLocationUnitNumber_i_RiskUnitGroup_i_RiskUnitGroupSequenceNumber_i_RiskUnit_i_RiskUnitSequenceNumber_i_PMSTypeExposure_i_MajorPerilCode_i_MajorPerilSequenceNumber_i_CoverageEffectiveDate_i_PremiumTransactionEnteredDate.LocationUnitNumber = i_LocationUnitNumber
	AND LKP_EXPOSURE_PREVIOUS_ONSET_TRANSACTION_PMS_i_PolicyAKID_i_PMSFunctionCode_i_InsuranceLine_i_LocationUnitNumber_i_SubLocationUnitNumber_i_RiskUnitGroup_i_RiskUnitGroupSequenceNumber_i_RiskUnit_i_RiskUnitSequenceNumber_i_PMSTypeExposure_i_MajorPerilCode_i_MajorPerilSequenceNumber_i_CoverageEffectiveDate_i_PremiumTransactionEnteredDate.SubLocationUnitNumber = i_SubLocationUnitNumber
	AND LKP_EXPOSURE_PREVIOUS_ONSET_TRANSACTION_PMS_i_PolicyAKID_i_PMSFunctionCode_i_InsuranceLine_i_LocationUnitNumber_i_SubLocationUnitNumber_i_RiskUnitGroup_i_RiskUnitGroupSequenceNumber_i_RiskUnit_i_RiskUnitSequenceNumber_i_PMSTypeExposure_i_MajorPerilCode_i_MajorPerilSequenceNumber_i_CoverageEffectiveDate_i_PremiumTransactionEnteredDate.RiskUnitGroup = i_RiskUnitGroup
	AND LKP_EXPOSURE_PREVIOUS_ONSET_TRANSACTION_PMS_i_PolicyAKID_i_PMSFunctionCode_i_InsuranceLine_i_LocationUnitNumber_i_SubLocationUnitNumber_i_RiskUnitGroup_i_RiskUnitGroupSequenceNumber_i_RiskUnit_i_RiskUnitSequenceNumber_i_PMSTypeExposure_i_MajorPerilCode_i_MajorPerilSequenceNumber_i_CoverageEffectiveDate_i_PremiumTransactionEnteredDate.RiskUnitGroupSequenceNumber = i_RiskUnitGroupSequenceNumber
	AND LKP_EXPOSURE_PREVIOUS_ONSET_TRANSACTION_PMS_i_PolicyAKID_i_PMSFunctionCode_i_InsuranceLine_i_LocationUnitNumber_i_SubLocationUnitNumber_i_RiskUnitGroup_i_RiskUnitGroupSequenceNumber_i_RiskUnit_i_RiskUnitSequenceNumber_i_PMSTypeExposure_i_MajorPerilCode_i_MajorPerilSequenceNumber_i_CoverageEffectiveDate_i_PremiumTransactionEnteredDate.RiskUnit = i_RiskUnit
	AND LKP_EXPOSURE_PREVIOUS_ONSET_TRANSACTION_PMS_i_PolicyAKID_i_PMSFunctionCode_i_InsuranceLine_i_LocationUnitNumber_i_SubLocationUnitNumber_i_RiskUnitGroup_i_RiskUnitGroupSequenceNumber_i_RiskUnit_i_RiskUnitSequenceNumber_i_PMSTypeExposure_i_MajorPerilCode_i_MajorPerilSequenceNumber_i_CoverageEffectiveDate_i_PremiumTransactionEnteredDate.RiskUnitSequenceNumber = i_RiskUnitSequenceNumber
	AND LKP_EXPOSURE_PREVIOUS_ONSET_TRANSACTION_PMS_i_PolicyAKID_i_PMSFunctionCode_i_InsuranceLine_i_LocationUnitNumber_i_SubLocationUnitNumber_i_RiskUnitGroup_i_RiskUnitGroupSequenceNumber_i_RiskUnit_i_RiskUnitSequenceNumber_i_PMSTypeExposure_i_MajorPerilCode_i_MajorPerilSequenceNumber_i_CoverageEffectiveDate_i_PremiumTransactionEnteredDate.PMSTypeExposure = i_PMSTypeExposure
	AND LKP_EXPOSURE_PREVIOUS_ONSET_TRANSACTION_PMS_i_PolicyAKID_i_PMSFunctionCode_i_InsuranceLine_i_LocationUnitNumber_i_SubLocationUnitNumber_i_RiskUnitGroup_i_RiskUnitGroupSequenceNumber_i_RiskUnit_i_RiskUnitSequenceNumber_i_PMSTypeExposure_i_MajorPerilCode_i_MajorPerilSequenceNumber_i_CoverageEffectiveDate_i_PremiumTransactionEnteredDate.MajorPerilCode = i_MajorPerilCode
	AND LKP_EXPOSURE_PREVIOUS_ONSET_TRANSACTION_PMS_i_PolicyAKID_i_PMSFunctionCode_i_InsuranceLine_i_LocationUnitNumber_i_SubLocationUnitNumber_i_RiskUnitGroup_i_RiskUnitGroupSequenceNumber_i_RiskUnit_i_RiskUnitSequenceNumber_i_PMSTypeExposure_i_MajorPerilCode_i_MajorPerilSequenceNumber_i_CoverageEffectiveDate_i_PremiumTransactionEnteredDate.MajorPerilSequenceNumber = i_MajorPerilSequenceNumber
	AND LKP_EXPOSURE_PREVIOUS_ONSET_TRANSACTION_PMS_i_PolicyAKID_i_PMSFunctionCode_i_InsuranceLine_i_LocationUnitNumber_i_SubLocationUnitNumber_i_RiskUnitGroup_i_RiskUnitGroupSequenceNumber_i_RiskUnit_i_RiskUnitSequenceNumber_i_PMSTypeExposure_i_MajorPerilCode_i_MajorPerilSequenceNumber_i_CoverageEffectiveDate_i_PremiumTransactionEnteredDate.PremiumTransactionEffectiveDate = i_CoverageEffectiveDate
	AND LKP_EXPOSURE_PREVIOUS_ONSET_TRANSACTION_PMS_i_PolicyAKID_i_PMSFunctionCode_i_InsuranceLine_i_LocationUnitNumber_i_SubLocationUnitNumber_i_RiskUnitGroup_i_RiskUnitGroupSequenceNumber_i_RiskUnit_i_RiskUnitSequenceNumber_i_PMSTypeExposure_i_MajorPerilCode_i_MajorPerilSequenceNumber_i_CoverageEffectiveDate_i_PremiumTransactionEnteredDate.PremiumTransactionEnteredDate = i_PremiumTransactionEnteredDate

	LEFT JOIN LKP_EXPOSURE_PREVIOUS_ONSET_TRANSACTION_DCT LKP_EXPOSURE_PREVIOUS_ONSET_TRANSACTION_DCT_i_RatingCoverageAKId_i_CoverageEffectiveDate_i_PremiumTransactionEnteredDate
	ON LKP_EXPOSURE_PREVIOUS_ONSET_TRANSACTION_DCT_i_RatingCoverageAKId_i_CoverageEffectiveDate_i_PremiumTransactionEnteredDate.RatingCoverageAKId = i_RatingCoverageAKId
	AND LKP_EXPOSURE_PREVIOUS_ONSET_TRANSACTION_DCT_i_RatingCoverageAKId_i_CoverageEffectiveDate_i_PremiumTransactionEnteredDate.PremiumTransactionEffectiveDate = i_CoverageEffectiveDate
	AND LKP_EXPOSURE_PREVIOUS_ONSET_TRANSACTION_DCT_i_RatingCoverageAKId_i_CoverageEffectiveDate_i_PremiumTransactionEnteredDate.PremiumTransactionEnteredDate = i_PremiumTransactionEnteredDate

),
LKP_BureauStatisticalCode_Premium AS (
	SELECT
	BureauCode3,
	BureauCode4,
	o_PremiumMasterCalculationID,
	PremiumMasterCalculationID
	FROM (
		SELECT pm.PremiumMasterCalculationID as PremiumMasterCalculationID ,b.BureauCode3 as BureauCode3 , b.BureauCode4 as BureauCode4 
		  FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.BureauStatisticalCode b
		  inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation  pm on 
		          pm.BureauStatisticalCodeAKID = b.BureauStatisticalCodeAKID
		  inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage s on
		         pm.StatisticalCoverageAKID = s.StatisticalCoverageAKID
		  inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage p on 
		          p.PolicyCoverageAKID = s.PolicyCoverageAKID
		  where p.TypeBureauCode in ('PI')
		 and S.ClassCode IN ('335','336','337','338','800') 
		 and pm.SourceSystemID='PMS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumMasterCalculationID ORDER BY BureauCode3) = 1
),
EXP_Logic_Premium AS (
	SELECT
	EXP_Cleansing_Premium.o_PremiumMasterCalculationID AS PremiumMasterCalculationID,
	-1 AS LossMasterCalculationId,
	EXP_Cleansing_Premium.o_TypeBureauCode AS TypeBureauCode,
	EXP_Cleansing_Premium.o_PremiumMasterRunDate AS PremiumMasterRunDate,
	-- *INF*: TO_DATE('1800-01-01','YYYY-MM-DD')
	TO_TIMESTAMP('1800-01-01', 'YYYY-MM-DD') AS loss_master_run_date,
	EXP_Cleansing_Premium.o_pol_key AS pol_key,
	EXP_Cleansing_Premium.o_StateProvinceCode AS StateProvinceCode,
	EXP_Cleansing_Premium.o_RatingCounty AS RatingCounty,
	'TBD' AS AAISTransactionCode,
	EXP_Cleansing_Premium.o_PremiumMasterPremium AS PremiumMasterPremium,
	-- *INF*: ROUND(PremiumMasterPremium,2)
	ROUND(PremiumMasterPremium, 2) AS PremiumMasterPremium_out,
	0.00 AS PaidLossAmt,
	0.00 AS OutstandingAmt,
	0 AS new_claim_count,
	EXP_Cleansing_Premium.o_ClassCode AS i_ClassCode,
	i_ClassCode AS PremiumMasterClassCode,
	'N/A' AS LossMasterClassCode,
	EXP_Cleansing_Premium.o_DeductibleAmount AS DeductibleAmount,
	-- *INF*: IIF(LENGTH(DeductibleAmount)=0 OR ISNULL(DeductibleAmount),'0',DeductibleAmount)
	IFF(LENGTH(DeductibleAmount) = 0 OR DeductibleAmount IS NULL, '0', DeductibleAmount) AS DeductibleAmount_out,
	EXP_Cleansing_Premium.o_terrorism_risk_ind AS i_terrorism_risk_ind,
	-- *INF*: i_terrorism_risk_ind
	-- 
	-- --DECODE(i_terrorism_risk_ind,'0','0','1','1','N','0','Y','1','N/A')
	i_terrorism_risk_ind AS o_terrorism_risk_ind,
	EXP_Cleansing_Premium.o_Exposure AS i_Exposure,
	-- *INF*: to_char(i_Exposure)
	to_char(i_Exposure) AS o_Exposure,
	EXP_Cleansing_Premium.o_ConstructionCode AS i_ConstructionCode,
	EXP_Cleansing_Premium.o_SourceSystemID AS i_SourceSystemID,
	LKP_BureauStatisticalCode_Premium.BureauCode3 AS lkp_ConstructionCode,
	-- *INF*: IIF(i_SourceSystemID='PMS',IIF(ISNULL(lkp_ConstructionCode) or LENGTH(lkp_ConstructionCode)=0,'00',lkp_ConstructionCode),:LKP.LKP_RATINGCOVERAGE_SUPCONSTRUCTIONCODE_STANDARDCONSTRUCTIONCODEDESCRIPTION(i_ConstructionCode))
	IFF(
	    i_SourceSystemID = 'PMS',
	    IFF(
	        lkp_ConstructionCode IS NULL
	    or LENGTH(lkp_ConstructionCode) = 0, '00',
	        lkp_ConstructionCode
	    ),
	    LKP_RATINGCOVERAGE_SUPCONSTRUCTIONCODE_STANDARDCONSTRUCTIONCODEDESCRIPTION_i_ConstructionCode.StandardConstructionCodeDescription
	) AS v_ConstructionCode,
	-- *INF*: iif(isnull(v_ConstructionCode),'N/A',v_ConstructionCode)
	IFF(v_ConstructionCode IS NULL, 'N/A', v_ConstructionCode) AS o_ConstructionCode,
	EXP_Cleansing_Premium.o_IsoFireProtectionCode AS IsoFireProtectionCode,
	LKP_BureauStatisticalCode_Premium.BureauCode4 AS lkp_IsoFireProtectionCode,
	-- *INF*: IIF(i_SourceSystemID='PMS',IIF(ISNULL(lkp_IsoFireProtectionCode) or LENGTH(lkp_IsoFireProtectionCode)=0,'00',lkp_IsoFireProtectionCode),IIF(ISNULL(IsoFireProtectionCode) OR LENGTH(IsoFireProtectionCode)=0,'N/A',IsoFireProtectionCode))
	IFF(
	    i_SourceSystemID = 'PMS',
	    IFF(
	        lkp_IsoFireProtectionCode IS NULL
	    or LENGTH(lkp_IsoFireProtectionCode) = 0, '00',
	        lkp_IsoFireProtectionCode
	    ),
	    IFF(
	        IsoFireProtectionCode IS NULL
	    or LENGTH(IsoFireProtectionCode) = 0, 'N/A',
	        IsoFireProtectionCode
	    )
	) AS IsoFireProtectionCode_out,
	EXP_Cleansing_Premium.o_InsuranceSegmentDescription AS InsuranceSegmentDescription,
	EXP_Cleansing_Premium.o_PolicyOfferingDescription AS i_PolicyOfferingDescription,
	-- *INF*: IIF(IN(i_PolicyOfferingDescription,'CPP','SBOP','Home & Highway', 'CBOP', 'SMARTbusiness' ),i_PolicyOfferingDescription,'Unassigned')
	IFF(
	    i_PolicyOfferingDescription IN ('CPP','SBOP','Home & Highway','CBOP','SMARTbusiness'),
	    i_PolicyOfferingDescription,
	    'Unassigned'
	) AS o_PolicyOfferingDescription,
	EXP_Cleansing_Premium.o_pol_term AS i_pol_term,
	-- *INF*: IIF(ISNULL(i_pol_term),'N/A',i_pol_term)
	IFF(i_pol_term IS NULL, 'N/A', i_pol_term) AS o_pol_term,
	'N/A' AS o_CauseOfLossName,
	-- *INF*: TO_DATE('1800-01-01','YYYY-MM-DD')
	TO_TIMESTAMP('1800-01-01', 'YYYY-MM-DD') AS o_ClaimLossDate,
	EXP_Cleansing_Premium.o_ZipPostalCode AS i_ZipPostalCode,
	-- *INF*: i_ZipPostalCode
	-- 
	-- --DECODE(LENGTH(i_ZipPostalCode),
	-- --6,DECODE(TRUE,SUBSTR(i_ZipPostalCode,1,1)='0',SUBSTR(i_ZipPostalCode,2),'00000'),
	-- --5,i_ZipPostalCode,
	-- --10,DECODE(TRUE,INSTR(i_ZipPostalCode,'-',1,1)=6,SUBSTR(i_ZipPostalCode,7),'0000'),
	-- --'00000'
	-- --)
	i_ZipPostalCode AS o_ZipPostalCode,
	EXP_Cleansing_Premium.o_CoverageEffectiveDate AS CoverageEffectiveDate,
	EXP_Cleansing_Premium.o_CoverageExpirationDate AS CoverageExpirationDate,
	-1 AS o_ClaimantDetailId,
	'N/A' AS o_ClaimNumber,
	0.00 AS o_DirectLossPaidIncludingRecoveries,
	EXP_Cleansing_Premium.o_CoverageType,
	0 AS OutstandingAllocatedLossAdjustmentExpenseAmount,
	0 AS PaidAllocatedlossAdjustmentExpenseAmount
	FROM EXP_Cleansing_Premium
	LEFT JOIN LKP_BureauStatisticalCode_Premium
	ON LKP_BureauStatisticalCode_Premium.PremiumMasterCalculationID = EXP_Cleansing_Premium.o_PremiumMasterCalculationID
	LEFT JOIN LKP_RATINGCOVERAGE_SUPCONSTRUCTIONCODE_STANDARDCONSTRUCTIONCODEDESCRIPTION LKP_RATINGCOVERAGE_SUPCONSTRUCTIONCODE_STANDARDCONSTRUCTIONCODEDESCRIPTION_i_ConstructionCode
	ON LKP_RATINGCOVERAGE_SUPCONSTRUCTIONCODE_STANDARDCONSTRUCTIONCODEDESCRIPTION_i_ConstructionCode.ConstructionCode = i_ConstructionCode

),
UN_ALL_Premium_Loss AS (
	SELECT PremiumMasterCalculationID, LossMasterCalculationId, TypeBureauCode, PremiumMasterRunDate, loss_master_run_date, pol_key, StateProvinceCode, RatingCounty, AAISTransactionCode, PremiumMasterPremium_out AS PremiumMasterDirectWrittenPremiumAmount, PaidLossAmt, OutstandingAmt, new_claim_count AS LossMasterNewClaimCount, PremiumMasterClassCode, LossMasterClassCode, DeductibleAmount_out AS DeductibleAmount, o_terrorism_risk_ind AS TerrorismRiskIndicator, o_Exposure AS InlandMarinePropertyAmountOfInsurance, o_ConstructionCode AS ConstructionCode, IsoFireProtectionCode_out AS IsoFireProtectionCode, InsuranceSegmentDescription, o_PolicyOfferingDescription AS PolicyOfferingDescription, o_pol_term AS PolicyTerm, o_CauseOfLossName AS CauseOfLossName, o_ClaimLossDate AS ClaimLossDate, o_ZipPostalCode AS ZipPostalCode, CoverageEffectiveDate, CoverageExpirationDate, o_ClaimantDetailId AS ClaimantDetailId, o_ClaimNumber AS ClaimNumber, o_DirectLossPaidIncludingRecoveries AS DirectLossPaidIncludingRecoveries, i_SourceSystemID AS Source_System_ID, o_CoverageType AS CoverageType, OutstandingAllocatedLossAdjustmentExpenseAmount AS OutstandingAllocatedLossAdjustmentExpenseAmount1, PaidAllocatedlossAdjustmentExpenseAmount
	FROM EXP_Logic_Premium
	UNION
	SELECT PremiumMasterCalculationID, loss_master_calculation_id AS LossMasterCalculationId, TypeBureauCode, PremiumMasterRunDate, loss_master_run_date, pol_key, StateProvinceCode, RatingCounty, PremiumMasterPremium AS PremiumMasterDirectWrittenPremiumAmount, paid_loss_amt AS PaidLossAmt, outstanding_amt AS OutstandingAmt, new_claim_count AS LossMasterNewClaimCount, PremiumMasterClassCode_out AS PremiumMasterClassCode, LossMasterClassCode_out AS LossMasterClassCode, DeductibleAmount, o_terrorism_risk_ind AS TerrorismRiskIndicator, o_Exposure AS InlandMarinePropertyAmountOfInsurance, ConstructionCode, IsoFireProtectionCode, InsuranceSegmentDescription, o_PolicyOfferingDescription AS PolicyOfferingDescription, o_pol_term AS PolicyTerm, o_CauseOfLossName AS CauseOfLossName, claim_loss_date AS ClaimLossDate, o_ZipPostalCode AS ZipPostalCode, CoverageEffectiveDate, CoverageExpirationDate, ClaimantDetailId, ClaimNumber, DirectLossPaidIncludingRecoveries, Source_Sys_Id AS Source_System_ID, CoverageType, OutstandingAllocatedLossAdjustmentExpenseAmount AS OutstandingAllocatedLossAdjustmentExpenseAmount1, PaidAllocatedlossAdjustmentExpenseAmount
	FROM EXP_Logic_Loss
),
EXP_Values AS (
	SELECT
	PremiumMasterCalculationID AS i_PremiumMasterCalculationID,
	LossMasterCalculationId AS i_LossMasterCalculationId,
	TypeBureauCode AS i_TypeBureauCode,
	PremiumMasterRunDate AS i_PremiumMasterRunDate,
	loss_master_run_date AS i_LossMasterRunDate,
	pol_key AS i_pol_key,
	StateProvinceCode AS i_StateProvinceCode,
	RatingCounty AS i_RatingCounty,
	AAISTransactionCode AS i_AAISTransactionCode,
	PremiumMasterDirectWrittenPremiumAmount AS i_PremiumMasterPremium,
	PaidLossAmt AS i_PaidLossAmt,
	OutstandingAmt AS i_OutstandingAmt,
	LossMasterNewClaimCount AS i_LossMasterNewClaimCount,
	PremiumMasterClassCode AS i_PremiumMasterClassCode,
	LossMasterClassCode AS i_LossMasterClassCode,
	DeductibleAmount AS i_DeductibleAmount,
	TerrorismRiskIndicator AS i_TerrorismRiskIndicator,
	InlandMarinePropertyAmountOfInsurance AS i_InlandMarinePropertyAmountOfInsurance,
	ConstructionCode AS i_ConstructionCode,
	IsoFireProtectionCode AS i_IsoFireProtectionCode,
	InsuranceSegmentDescription AS i_InsuranceSegmentDescription,
	PolicyOfferingDescription AS i_PolicyOfferingDescription,
	PolicyTerm AS i_PolicyTerm,
	CauseOfLossName AS i_CauseOfLossName,
	ClaimLossDate AS i_ClaimLossDate,
	ZipPostalCode AS i_ZipPostalCode,
	CoverageEffectiveDate AS i_CoverageEffectiveDate,
	CoverageExpirationDate AS i_CoverageExpirationDate,
	ClaimantDetailId AS i_ClaimantDetailId,
	ClaimNumber AS i_ClaimNumber,
	DirectLossPaidIncludingRecoveries AS i_DirectLossPaidIncludingRecoveries,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TRUNC(@{pipeline().parameters.EXTRACTDATE},'D')
	CAST(TRUNC(@{pipeline().parameters.EXTRACTDATE}, 'DAY') AS TIMESTAMP_NTZ(0)) AS o_CreatedDate,
	i_PremiumMasterCalculationID AS o_PremiumMasterCalculationID,
	i_LossMasterCalculationId AS o_LossMasterCalculationId,
	i_TypeBureauCode AS o_TypeBureauCode,
	'31' AS o_BureauLineOfInsurance,
	i_PremiumMasterRunDate AS o_PremiumMasterRunDate,
	i_LossMasterRunDate AS o_LossMasterRunDate,
	'6115' AS o_BureauCompanyNumber,
	i_pol_key AS o_pol_key,
	i_StateProvinceCode AS o_StateProvinceCode,
	i_RatingCounty AS o_RatingCounty,
	i_PremiumMasterPremium AS o_PremiumMasterDirectWrittenPremiumAmount,
	i_PaidLossAmt AS o_PaidLossAmount,
	i_OutstandingAmt AS o_OutstandingLossAmount,
	i_LossMasterNewClaimCount AS o_LossMasterNewClaimCount,
	i_PremiumMasterClassCode AS o_PremiumMasterClassCode,
	i_LossMasterClassCode AS o_LossMasterClassCode,
	'090' AS o_BureauAnnualStatementLineCode,
	'1' AS o_BureauOrganizationCode,
	i_TerrorismRiskIndicator AS o_TerrorismRiskIndicator,
	i_InlandMarinePropertyAmountOfInsurance AS o_InlandMarinePropertyAmountOfInsurance,
	i_ConstructionCode AS o_ConstructionCode,
	i_IsoFireProtectionCode AS o_ISOFireProtectionCode,
	i_InsuranceSegmentDescription AS o_InsuranceSegmentDescription,
	i_PolicyOfferingDescription AS o_PolicyOfferingDescription,
	i_PolicyTerm AS o_PolicyTerm,
	i_CauseOfLossName AS o_CauseOfLossName,
	i_ClaimLossDate AS o_ClaimLossDate,
	i_ZipPostalCode AS o_ZipPostalCode,
	i_CoverageEffectiveDate AS o_CoverageEffectiveDate,
	i_CoverageExpirationDate AS o_CoverageExpirationDate,
	i_ClaimantDetailId AS o_ClaimantDetailId,
	-- *INF*: IIF(i_ClaimLossDate=TO_DATE('1800-01-01','YYYY-MM-DD'),'N/A',SUBSTR(LTRIM(RTRIM(i_pol_key)),1,20))
	-- 
	-- --substr needs to be removed once policykey for duckcreek changed to policynumber+policyversion
	IFF(
	    i_ClaimLossDate = TO_TIMESTAMP('1800-01-01', 'YYYY-MM-DD'), 'N/A',
	    SUBSTR(LTRIM(RTRIM(i_pol_key)), 1, 20)
	) AS o_ClaimNumber,
	i_DirectLossPaidIncludingRecoveries AS o_DirectLossPaidIncludingRecoveries,
	-- *INF*: GET_DATE_PART(i_CoverageExpirationDate,'yyyy')-GET_DATE_PART(i_CoverageEffectiveDate,'yyyy')
	DATE_PART(i_CoverageExpirationDate, 'yyyy') - DATE_PART(i_CoverageEffectiveDate, 'yyyy') AS o_flag,
	Source_System_ID AS Source_Sys_ID,
	CoverageType,
	-- *INF*: IIF(LENGTH(i_DeductibleAmount)=0 OR ISNULL(i_DeductibleAmount),'0',i_DeductibleAmount)
	IFF(LENGTH(i_DeductibleAmount) = 0 OR i_DeductibleAmount IS NULL, '0', i_DeductibleAmount) AS v_DeductibleAmount_1,
	-- *INF*: DECODE(TRUE,
	-- Source_Sys_ID='PMS',v_DeductibleAmount_1,
	-- i_ClaimLossDate >TO_DATE('1800','YYYY'), :LKP.LKP_COVERAGEDEDUCTIBLE_DEDUCTIBLEAMOUNT_LOSS(i_pol_key,CoverageType,i_ClaimLossDate),
	-- :LKP.LKP_COVERAGEDEDUCTIBLE_DEDUCTIBLEAMOUNT_PREM(i_pol_key,CoverageType)
	-- )
	DECODE(
	    TRUE,
	    Source_Sys_ID = 'PMS', v_DeductibleAmount_1,
	    i_ClaimLossDate > TO_TIMESTAMP('1800', 'YYYY'), LKP_COVERAGEDEDUCTIBLE_DEDUCTIBLEAMOUNT_LOSS_i_pol_key_CoverageType_i_ClaimLossDate.DeductibleAmount,
	    LKP_COVERAGEDEDUCTIBLE_DEDUCTIBLEAMOUNT_PREM_i_pol_key_CoverageType.DeductibleAmount
	) AS v_DeductibleAmount_2,
	-- *INF*: IIF(LENGTH(v_DeductibleAmount_2)=0 OR ISNULL(v_DeductibleAmount_2),'0',v_DeductibleAmount_2)
	IFF(
	    LENGTH(v_DeductibleAmount_2) = 0 OR v_DeductibleAmount_2 IS NULL, '0', v_DeductibleAmount_2
	) AS o_DeductibleAmount,
	OutstandingAllocatedLossAdjustmentExpenseAmount1 AS i_OutstandingAllocatedLossAdjustmentExpenseAmount,
	PaidAllocatedlossAdjustmentExpenseAmount AS i_PaidAllocatedlossAdjustmentExpenseAmount,
	-- *INF*: IIF(ISNULL(i_OutstandingAllocatedLossAdjustmentExpenseAmount), 0, i_OutstandingAllocatedLossAdjustmentExpenseAmount)
	IFF(
	    i_OutstandingAllocatedLossAdjustmentExpenseAmount IS NULL, 0,
	    i_OutstandingAllocatedLossAdjustmentExpenseAmount
	) AS o_OutstandingAllocatedLossAdjustmentExpenseAmount,
	-- *INF*: IIF(ISNULL(i_PaidAllocatedlossAdjustmentExpenseAmount), 0, i_PaidAllocatedlossAdjustmentExpenseAmount)
	IFF(
	    i_PaidAllocatedlossAdjustmentExpenseAmount IS NULL, 0,
	    i_PaidAllocatedlossAdjustmentExpenseAmount
	) AS o_PaidAllocatedlossAdjustmentExpenseAmount
	FROM UN_ALL_Premium_Loss
	LEFT JOIN LKP_COVERAGEDEDUCTIBLE_DEDUCTIBLEAMOUNT_LOSS LKP_COVERAGEDEDUCTIBLE_DEDUCTIBLEAMOUNT_LOSS_i_pol_key_CoverageType_i_ClaimLossDate
	ON LKP_COVERAGEDEDUCTIBLE_DEDUCTIBLEAMOUNT_LOSS_i_pol_key_CoverageType_i_ClaimLossDate.PolicyKey = i_pol_key
	AND LKP_COVERAGEDEDUCTIBLE_DEDUCTIBLEAMOUNT_LOSS_i_pol_key_CoverageType_i_ClaimLossDate.CoverageType = CoverageType
	AND LKP_COVERAGEDEDUCTIBLE_DEDUCTIBLEAMOUNT_LOSS_i_pol_key_CoverageType_i_ClaimLossDate.EffectiveDate = i_ClaimLossDate

	LEFT JOIN LKP_COVERAGEDEDUCTIBLE_DEDUCTIBLEAMOUNT_PREM LKP_COVERAGEDEDUCTIBLE_DEDUCTIBLEAMOUNT_PREM_i_pol_key_CoverageType
	ON LKP_COVERAGEDEDUCTIBLE_DEDUCTIBLEAMOUNT_PREM_i_pol_key_CoverageType.PolicyKey = i_pol_key
	AND LKP_COVERAGEDEDUCTIBLE_DEDUCTIBLEAMOUNT_PREM_i_pol_key_CoverageType.CoverageType = CoverageType

),
WorkAAISExtract AS (

	------------ PRE SQL ----------
	@{pipeline().parameters.DELETE_PRESQL}
	-------------------------------


	INSERT INTO WorkAAISExtract
	(AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, PremiumMasterRunDate, LossMasterRunDate, BureauCompanyNumber, PolicyKey, StateProvinceCode, RatingCounty, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, LossMasterNewClaimCount, PremiumMasterClassCode, LossMasterClassCode, DeductibleAmount, BureauAnnualStatementLineCode, BureauOrganizationCode, TerrorismRiskIndicator, InlandMarinePropertyAmountOfInsurance, ConstructionCode, ISOFireProtectionCode, InsuranceSegmentDescription, PolicyOfferingDescription, PolicyTerm, CauseOfLossName, ClaimLossDate, ZipPostalCode, CoverageEffectiveDate, CoverageExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, ClaimNumber, PaidAllocatedLossAdjustmentExpenseAmount, OutstandingAllocatedLossAdjustmentExpenseAmount)
	SELECT 
	o_AuditID AS AUDITID, 
	o_CreatedDate AS CREATEDDATE, 
	o_PremiumMasterCalculationID AS EDWPREMIUMMASTERCALCULATIONPKID, 
	o_LossMasterCalculationId AS EDWLOSSMASTERCALCULATIONPKID, 
	o_TypeBureauCode AS TYPEBUREAUCODE, 
	o_BureauLineOfInsurance AS BUREAULINEOFINSURANCE, 
	o_PremiumMasterRunDate AS PREMIUMMASTERRUNDATE, 
	o_LossMasterRunDate AS LOSSMASTERRUNDATE, 
	o_BureauCompanyNumber AS BUREAUCOMPANYNUMBER, 
	o_pol_key AS POLICYKEY, 
	o_StateProvinceCode AS STATEPROVINCECODE, 
	o_RatingCounty AS RATINGCOUNTY, 
	o_PremiumMasterDirectWrittenPremiumAmount AS PREMIUMMASTERDIRECTWRITTENPREMIUMAMOUNT, 
	o_PaidLossAmount AS PAIDLOSSAMOUNT, 
	o_OutstandingLossAmount AS OUTSTANDINGLOSSAMOUNT, 
	o_LossMasterNewClaimCount AS LOSSMASTERNEWCLAIMCOUNT, 
	o_PremiumMasterClassCode AS PREMIUMMASTERCLASSCODE, 
	o_LossMasterClassCode AS LOSSMASTERCLASSCODE, 
	o_DeductibleAmount AS DEDUCTIBLEAMOUNT, 
	o_BureauAnnualStatementLineCode AS BUREAUANNUALSTATEMENTLINECODE, 
	o_BureauOrganizationCode AS BUREAUORGANIZATIONCODE, 
	o_TerrorismRiskIndicator AS TERRORISMRISKINDICATOR, 
	o_InlandMarinePropertyAmountOfInsurance AS INLANDMARINEPROPERTYAMOUNTOFINSURANCE, 
	o_ConstructionCode AS CONSTRUCTIONCODE, 
	o_ISOFireProtectionCode AS ISOFIREPROTECTIONCODE, 
	o_InsuranceSegmentDescription AS INSURANCESEGMENTDESCRIPTION, 
	o_PolicyOfferingDescription AS POLICYOFFERINGDESCRIPTION, 
	o_PolicyTerm AS POLICYTERM, 
	o_CauseOfLossName AS CAUSEOFLOSSNAME, 
	o_ClaimLossDate AS CLAIMLOSSDATE, 
	o_ZipPostalCode AS ZIPPOSTALCODE, 
	o_CoverageEffectiveDate AS COVERAGEEFFECTIVEDATE, 
	o_CoverageExpirationDate AS COVERAGEEXPIRATIONDATE, 
	o_DirectLossPaidIncludingRecoveries AS INCEPTIONTODATEPAIDLOSSAMOUNT, 
	o_ClaimantDetailId AS CLAIMANTCOVERAGEDETAILID, 
	o_ClaimNumber AS CLAIMNUMBER, 
	o_PaidAllocatedlossAdjustmentExpenseAmount AS PAIDALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	o_OutstandingAllocatedLossAdjustmentExpenseAmount AS OUTSTANDINGALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT
	FROM EXP_Values
),
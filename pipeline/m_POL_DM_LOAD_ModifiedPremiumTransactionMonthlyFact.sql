WITH
LKP_calender_dim AS (
	SELECT
	clndr_id,
	clndr_date
	FROM (
		SELECT 
			clndr_id,
			clndr_date
		FROM calendar_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY clndr_date ORDER BY clndr_id) = 1
),
LKP_CoverageDetailDim_Historic_PMS AS (
	SELECT
	CoverageDetailDimId,
	StatisticalCoverageAKID,
	EffectiveDate,
	ExpirationDate
	FROM (
		SELECT C.CoverageDetailDimId AS CoverageDetailDimId,
			SC.StatisticalCoverageAKID AS StatisticalCoverageAKID,
			C.EffectiveDate AS EffectiveDate,
			C.ExpirationDate AS ExpirationDate
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim C
		@{pipeline().parameters.HISTORICCLAUSE}
		JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC ON SC.CoverageGuid = C.CoverageGuid 
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StatisticalCoverageAKID,EffectiveDate,ExpirationDate ORDER BY CoverageDetailDimId) = 1
),
SQ_PremiumMasterCalculation_PMS_NO_IN_SCOPE AS (
	----PMS other lines not in scope 
	SELECT PC.PremiumMasterCalculationID, 
	PC.SourceSystemID,
	PC.AgencyAKID, 
	PC.PolicyAKID, 
	PC.ContractCustomerAKID, 
	PC.PolicyCoverageAKID, 
	PC.StatisticalCoverageAKID, 
	PC.PremiumTransactionAKID, 
	PC.BureauStatisticalCodeAKID, 
	PC.PremiumMasterTransactionCode, 
	PC.PremiumMasterRunDate, 
	PC.PremiumTransactionEnteredDate, 
	PC.PremiumMasterPolicyExpirationYear, 
	PC.PremiumMasterCoverageEffectiveDate, 
	PC.PremiumMasterCoverageExpirationDate, 
	PC.PremiumMasterPolicyTerm, 
	PC.PremiumMasterBureauPolicyType, 
	PC.PremiumMasterAuditCode, 
	PC.PremiumMasterPremiumType, 
	PC.PremiumMasterTypeBureauCode, 
	PC.PremiumMasterBureauStatisticalLine, 
	PC.PremiumMasterProductLine, 
	PC.PremiumMasterAgencyCommissionRate, 
	PC.PremiumMasterClassCode, 
	PC.PremiumMasterExposure, 
	PC.PremiumMasterSubLine, 
	PC.PremiumMasterStatisticalCode1, 
	PC.PremiumMasterStatisticalCode2, 
	PC.PremiumMasterStatisticalCode3, 
	PC.PremiumMasterRateModifier, 
	PC.PremiumMasterRateDeparture, 
	PC.PremiumMasterBureauInceptionDate, 
	PC.PremiumMasterCountersignAgencyType, 
	PC.PremiumMasterCountersignAgencyCode, 
	PC.PremiumMasterCountersignAgencyState, 
	PC.PremiumMasterCountersignAgencyRate, 
	PC.PremiumMasterPremium, 
	PC.PremiumMasterFullTermPremium, 
	PC.PremiumMasterRenewalIndicator, 
	PC.PremiumMasterRecordType, 
	PC.RatingCoverageEffectiveDate,
	PC.RatingCoverageExpirationDate,
	PC.RatingCoverageAKId,
	POL.pol_sym, 
	POL.pol_key, 
	POL.pms_pol_lob_code, 
	POL.ClassOfBusiness, 
	POLCOV.InsuranceLine, 
	STATCOV.RiskUnitGroup, 
	STATCOV.RiskUnit, 
	STATCOV.RiskUnitSequenceNumber,
	STATCOV.MajorPerilCode,
	 -1 AS PremiumTransactionID ,
	 PC.PremiumMasterReasonAmendedCode ,
	ISNULL(INRB.InsuranceReferenceLineOfBusinessCode,'000') as InsuranceReferenceLineOfBusinessCode,
	ISNULL(PROD.ProductCode,'000') as ProductCode,
	ISNULL(POFF.PolicyOfferingCode,'000') as PolicyOfferingCode,
	ISNULL(INSG.InsuranceSegmentCode,'N/A') as InsuranceSegmentCode,
	ISNULL(STPC.StrategicProfitCenterCode,'6') as StrategicProfitCenterCode,
	ISNULL(ENGP.EnterpriseGroupCode,'1') as EnterpriseGroupCode,
	ISNULL(NRLE.InsuranceReferenceLegalEntityCode,'1') as InsuranceReferenceLegalEntityCode,
	'N/A' as RiskType,
	'N/A' as CoverageType,
	ISNULL(SIL.StandardInsuranceLineCode,'N/A') as StandardInsuranceLineCode,
	RL.RiskLocationHashKey,
	RL.StateProvinceCode,
	'N/A' as PerilGroup,
	'N/A' as SubCoverageTypeCode,
	'N/A' as CoverageVersion,
	'N/A' as CoverageForm,
	'N/A' as AnnualStatementLineCode,
	'N/A' as SubAnnualStatementLineCode,
	'-1' as PolicyAuditAKId,
	'1800-01-01' as PolicyAuditEffectiveDate,
	'N/A' as SubNonAnnualStatementLineCode,
	'N/A' CoverageCode,
	PC.PremiumMasterRunDate as WAMP_RUNDATE,
	PC.PremiumMasterCustomerCareCommissionRate,
	RPDT.RatingPlanCode,
	0 AS GeneratedRecordIndicator,
	PC.PremiumMasterPremium AS DirectWrittenPremium,
	0 AS RatablePremium,
	0 AS ClassifiedPremium,
	PC.PremiumMasterPremium AS OtherModifiedPremium,
	PC.PremiumMasterPremium AS ScheduleModifiedPremium,
	PC.PremiumMasterPremium AS ExperienceModifiedPremium,
	PC.PremiumMasterPremium AS SubjectWrittenPremium
	FROM
	PremiumMasterCalculation PC with (nolock)
	Join StatisticalCoverage STATCOV  with (nolock)
	on STATCOV.StatisticalCoverageAKID = PC.StatisticalCoverageAKID
	Join PolicyCoverage POLCOV
	on 
	POLCOV.PolicyCoverageAKID = PC.PolicyCoverageAKID
	Join RiskLocation RL
	on 
	RL.RiskLocationAKID=POLCOV.RiskLocationAKID
	Join V2.Policy POL with (nolock)
	on PC.PolicyAKID = POL.pol_ak_id AND POL.crrnt_snpsht_flag = 1
	@{pipeline().parameters.JOIN_POLICY_LIST}
	Left Join InsuranceReferenceLineOfBusiness INRB
	on STATCOV.InsuranceReferenceLineOfBusinessAKId=INRB.InsuranceReferenceLineOfBusinessAKId and INRB.CurrentSnapshotFlag=1
	Left Join Product PROD
	on STATCOV.ProductAKId=PROD.ProductAKId and PROD.CurrentSnapshotFlag=1
	Left Join PolicyOffering POFF
	on POL.PolicyOfferingAKId=POFF.PolicyOfferingAKId and POFF.CurrentSnapshotFlag=1
	Left Join InsuranceSegment INSG
	on POL.InsuranceSegmentAKId=INSG.InsuranceSegmentAKId and INSG.CurrentSnapshotFlag=1
	Left Join StrategicProfitCenter STPC
	on POL.StrategicProfitCenterAKId=STPC.StrategicProfitCenterAKId and STPC.CurrentSnapshotFlag=1
	Left Join EnterpriseGroup ENGP
	on STPC.EnterpriseGroupId=ENGP.EnterpriseGroupId and ENGP.CurrentSnapshotFlag=1
	Left Join InsuranceReferenceLegalEntity NRLE
	on STPC.InsuranceReferenceLegalEntityId=NRLE.InsuranceReferenceLegalEntityId and NRLE.CurrentSnapshotFlag=1
	Left Join sup_insurance_line SIL
	ON SIL.ins_line_code = POLCOV.InsuranceLine
	AND SIL.crrnt_snpsht_flag = 1
	Left Join RatingPlan RPDT
	on POLCOV.RatingPlanAKId=RPDT.RatingPlanAKId and RPDT.CurrentSnapshotFlag=1
	Left Join ModifiedPremiumNonWorkersCompensationCalculation wdp  with (nolock) on wdp.PremiumMasterCalculationId=PC.PremiumMasterCalculationId and wdp.SourceSystemID='PMS' 
	WHERE 
	PC.SourceSystemID='PMS' 
	and wdp.PremiumMasterCalculationId is null 
	and pc.PremiumMasterReasonAmendedCode not in ('COL','CWO','Claw Back')
	and pc.CurrentSnapshotFlag=1
	and pc.PremiumMasterPremiumType='D'
	--AND NOT EXISTS (SELECT 1 FROM ModifiedPremiumNonWorkersCompensationCalculation wdp WHERE wdp.SourceSystemID='PMS' AND wdp.PremiumMasterCalculationId=PC.PremiumMasterCalculationId)
	AND INRB.InsuranceReferenceLineOfBusinessAbbreviation NOT IN ('WC') 
	AND @{pipeline().parameters.WHERE_PMC_CURRENT_MONTH}
	@{pipeline().parameters.WHERE_CLAUSE}
),
SQ_PremiumMasterCalculation_PMS_OTHERLINES AS (
	--PMS other lines
	SELECT PC.PremiumMasterCalculationID, 
	PC.SourceSystemID,
	PC.AgencyAKID, 
	PC.PolicyAKID, 
	PC.ContractCustomerAKID, 
	PC.PolicyCoverageAKID, 
	PC.StatisticalCoverageAKID, 
	PC.PremiumTransactionAKID, 
	PC.BureauStatisticalCodeAKID, 
	PC.PremiumMasterTransactionCode, 
	PC.PremiumMasterRunDate, 
	PC.PremiumTransactionEnteredDate, 
	PC.PremiumMasterPolicyExpirationYear, 
	PC.PremiumMasterCoverageEffectiveDate, 
	PC.PremiumMasterCoverageExpirationDate, 
	PC.PremiumMasterPolicyTerm, 
	PC.PremiumMasterBureauPolicyType, 
	PC.PremiumMasterAuditCode, 
	PC.PremiumMasterPremiumType, 
	PC.PremiumMasterTypeBureauCode, 
	PC.PremiumMasterBureauStatisticalLine, 
	PC.PremiumMasterProductLine, 
	PC.PremiumMasterAgencyCommissionRate, 
	PC.PremiumMasterClassCode, 
	PC.PremiumMasterExposure, 
	PC.PremiumMasterSubLine, 
	PC.PremiumMasterStatisticalCode1, 
	PC.PremiumMasterStatisticalCode2, 
	PC.PremiumMasterStatisticalCode3, 
	PC.PremiumMasterRateModifier, 
	PC.PremiumMasterRateDeparture, 
	PC.PremiumMasterBureauInceptionDate, 
	PC.PremiumMasterCountersignAgencyType, 
	PC.PremiumMasterCountersignAgencyCode, 
	PC.PremiumMasterCountersignAgencyState, 
	PC.PremiumMasterCountersignAgencyRate, 
	PC.PremiumMasterPremium, 
	PC.PremiumMasterFullTermPremium, 
	PC.PremiumMasterRenewalIndicator, 
	PC.PremiumMasterRecordType, 
	PC.RatingCoverageEffectiveDate,
	PC.RatingCoverageExpirationDate,
	PC.RatingCoverageAKId,
	POL.pol_sym, 
	POL.pol_key, 
	POL.pms_pol_lob_code, 
	POL.ClassOfBusiness, 
	POLCOV.InsuranceLine, 
	STATCOV.RiskUnitGroup, 
	STATCOV.RiskUnit, 
	STATCOV.RiskUnitSequenceNumber,
	STATCOV.MajorPerilCode,
	 -1 AS PremiumTransactionID ,
	 PC.PremiumMasterReasonAmendedCode ,
	ISNULL(INRB.InsuranceReferenceLineOfBusinessCode,'000') as InsuranceReferenceLineOfBusinessCode,
	ISNULL(PROD.ProductCode,'000') as ProductCode,
	ISNULL(POFF.PolicyOfferingCode,'000') as PolicyOfferingCode,
	ISNULL(INSG.InsuranceSegmentCode,'N/A') as InsuranceSegmentCode,
	ISNULL(STPC.StrategicProfitCenterCode,'6') as StrategicProfitCenterCode,
	ISNULL(ENGP.EnterpriseGroupCode,'1') as EnterpriseGroupCode,
	ISNULL(NRLE.InsuranceReferenceLegalEntityCode,'1') as InsuranceReferenceLegalEntityCode,
	'N/A' as RiskType,
	'N/A' as CoverageType,
	ISNULL(SIL.StandardInsuranceLineCode,'N/A') as StandardInsuranceLineCode,
	RL.RiskLocationHashKey,
	RL.StateProvinceCode,
	'N/A' as PerilGroup,
	'N/A' as SubCoverageTypeCode,
	'N/A' as CoverageVersion,
	'N/A' as CoverageForm,
	'N/A' as AnnualStatementLineCode,
	'N/A' as SubAnnualStatementLineCode,
	'-1' as PolicyAuditAKId,
	'1800-01-01' as PolicyAuditEffectiveDate,
	'N/A' as SubNonAnnualStatementLineCode,
	'N/A' CoverageCode,
	WAMP.RunDate as WAMP_RUNDATE,
	PC.PremiumMasterCustomerCareCommissionRate,
	RPDT.RatingPlanCode,
	WAMP.GeneratedRecordIndicator,
	WAMP.DirectWrittenPremium,
	0 AS RatablePremium,
	0 AS ClassifiedPremium,
	WAMP.OtherModifiedPremium,
	WAMP.ScheduleModifiedPremium,
	WAMP.ExperienceModifiedPremium,
	WAMP.SubjectWrittenPremium AS SubjectWrittenPremium
	FROM
	ModifiedPremiumNonWorkersCompensationCalculation WAMP  with (nolock)
	inner hash join PremiumMasterCalculation PC with (nolock)
	on PC.PremiumMasterCalculationId = WAMP.PremiumMasterCalculationId
	Join StatisticalCoverage STATCOV  with (nolock)
	on STATCOV.StatisticalCoverageAKID = PC.StatisticalCoverageAKID
	Join PolicyCoverage POLCOV
	on 
	POLCOV.PolicyCoverageAKID = PC.PolicyCoverageAKID
	Join RiskLocation RL
	on 
	RL.RiskLocationAKID=POLCOV.RiskLocationAKID
	Join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Policy POL with (nolock)
	on PC.PolicyAKID = POL.pol_ak_id AND POL.crrnt_snpsht_flag = 1
	@{pipeline().parameters.JOIN_POLICY_LIST}
	Left Join InsuranceReferenceLineOfBusiness INRB
	on STATCOV.InsuranceReferenceLineOfBusinessAKId=INRB.InsuranceReferenceLineOfBusinessAKId and INRB.CurrentSnapshotFlag=1
	Left Join Product PROD
	on STATCOV.ProductAKId=PROD.ProductAKId and PROD.CurrentSnapshotFlag=1
	Left Join PolicyOffering POFF
	on POL.PolicyOfferingAKId=POFF.PolicyOfferingAKId and POFF.CurrentSnapshotFlag=1
	Left Join InsuranceSegment INSG
	on POL.InsuranceSegmentAKId=INSG.InsuranceSegmentAKId and INSG.CurrentSnapshotFlag=1
	Left Join StrategicProfitCenter STPC
	on POL.StrategicProfitCenterAKId=STPC.StrategicProfitCenterAKId and STPC.CurrentSnapshotFlag=1
	Left Join EnterpriseGroup ENGP
	on STPC.EnterpriseGroupId=ENGP.EnterpriseGroupId and ENGP.CurrentSnapshotFlag=1
	Left Join InsuranceReferenceLegalEntity NRLE
	on STPC.InsuranceReferenceLegalEntityId=NRLE.InsuranceReferenceLegalEntityId and NRLE.CurrentSnapshotFlag=1
	Left Join sup_insurance_line SIL
	ON SIL.ins_line_code = POLCOV.InsuranceLine
	AND SIL.crrnt_snpsht_flag = 1
	Left Join RatingPlan RPDT
	on POLCOV.RatingPlanAKId=RPDT.RatingPlanAKId and RPDT.CurrentSnapshotFlag=1
	WHERE 
	PC.SourceSystemID='PMS' 
	and pc.PremiumMasterReasonAmendedCode not in ('COL','CWO','Claw Back')
	and pc.CurrentSnapshotFlag=1
	and pc.PremiumMasterPremiumType='D'  and @{pipeline().parameters.WHERE_MPWC_CURRENT_MONTH} 
	@{pipeline().parameters.WHERE_CLAUSE}
),
SQ_PremiumMasterCalculation_PMS_WC AS (
	--PMS WorkersCompensation
	SELECT 
	PC.PremiumMasterCalculationID, 
	PC.SourceSystemID,
	PC.AgencyAKID, 
	PC.PolicyAKID, 
	PC.ContractCustomerAKID, 
	PC.PolicyCoverageAKID, 
	PC.StatisticalCoverageAKID, 
	PC.PremiumTransactionAKID, 
	PC.BureauStatisticalCodeAKID, 
	PC.PremiumMasterTransactionCode, 
	PC.PremiumMasterRunDate, 
	PC.PremiumTransactionEnteredDate, 
	PC.PremiumMasterPolicyExpirationYear, 
	PC.PremiumMasterCoverageEffectiveDate, 
	PC.PremiumMasterCoverageExpirationDate, 
	PC.PremiumMasterPolicyTerm, 
	PC.PremiumMasterBureauPolicyType, 
	PC.PremiumMasterAuditCode, 
	PC.PremiumMasterPremiumType, 
	PC.PremiumMasterTypeBureauCode, 
	PC.PremiumMasterBureauStatisticalLine, 
	PC.PremiumMasterProductLine, 
	PC.PremiumMasterAgencyCommissionRate, 
	PC.PremiumMasterClassCode, 
	PC.PremiumMasterExposure, 
	PC.PremiumMasterSubLine, 
	PC.PremiumMasterStatisticalCode1, 
	PC.PremiumMasterStatisticalCode2, 
	PC.PremiumMasterStatisticalCode3, 
	PC.PremiumMasterRateModifier, 
	PC.PremiumMasterRateDeparture, 
	PC.PremiumMasterBureauInceptionDate, 
	PC.PremiumMasterCountersignAgencyType, 
	PC.PremiumMasterCountersignAgencyCode, 
	PC.PremiumMasterCountersignAgencyState, 
	PC.PremiumMasterCountersignAgencyRate, 
	PC.PremiumMasterPremium, 
	PC.PremiumMasterFullTermPremium, 
	PC.PremiumMasterRenewalIndicator, 
	PC.PremiumMasterRecordType, 
	PC.RatingCoverageEffectiveDate,
	PC.RatingCoverageExpirationDate,
	PC.RatingCoverageAKId,
	POL.pol_sym, 
	POL.pol_key, 
	POL.pms_pol_lob_code, 
	POL.ClassOfBusiness, 
	POLCOV.InsuranceLine, 
	ISNULL(STATCOV.RiskUnitGroup, 'N/A' )as RiskUnitGroup,
	ISNULL(STATCOV.RiskUnit, 'N/A' ) as RiskUnit,
	ISNULL(STATCOV.RiskUnitSequenceNumber, 'N/A' )  as RiskUnitSequenceNumber,
	ISNULL(STATCOV.MajorPerilCode, 'N/A' ) as MajorPerilCode,
	-1 AS PremiumTransactionID ,
	PC.PremiumMasterReasonAmendedCode ,
	ISNULL(INRB.InsuranceReferenceLineOfBusinessCode,'000') as InsuranceReferenceLineOfBusinessCode,
	ISNULL(PROD.ProductCode,'000') as ProductCode,
	ISNULL(POFF.PolicyOfferingCode,'000') as PolicyOfferingCode,
	ISNULL(INSG.InsuranceSegmentCode,'N/A') as InsuranceSegmentCode,
	ISNULL(STPC.StrategicProfitCenterCode,'6') as StrategicProfitCenterCode,
	ISNULL(ENGP.EnterpriseGroupCode,'1') as EnterpriseGroupCode,
	ISNULL(NRLE.InsuranceReferenceLegalEntityCode,'1') as InsuranceReferenceLegalEntityCode,
	'N/A' as RiskType,
	'N/A' as CoverageType,
	ISNULL(SIL.StandardInsuranceLineCode,'N/A') as StandardInsuranceLineCode,
	RL.RiskLocationHashKey,
	RL.StateProvinceCode,
	'N/A' as PerilGroup,
	'N/A' as SubCoverageTypeCode,
	'N/A' as CoverageVersion,
	'N/A' as CoverageForm,
	'N/A' as AnnualStatementLineCode,
	'N/A' as SubAnnualStatementLineCode,
	'-1' as PolicyAuditAKId,
	'1800-01-01' as PolicyAuditEffectiveDate,
	'N/A' as SubNonAnnualStatementLineCode,
	'N/A' CoverageCode,
	CASE WHEN WAMP.PremiumMasterCalculationId IS NULL THEN PC.PremiumMasterRunDate ELSE WAMP.RunDate END as WAMP_RUNDATE,
	PC.PremiumMasterCustomerCareCommissionRate,
	RPDT.RatingPlanCode,
	CASE WHEN WAMP.PremiumMasterCalculationId IS NULL THEN 0 ELSE WAMP.GeneratedRecordIndicator END as GeneratedRecordIndicator,
	CASE WHEN WAMP.PremiumMasterCalculationId IS NULL THEN PC.PremiumMasterPremium ELSE WAMP.DirectWrittenPremium END AS DirectWrittenPremium,
	CASE WHEN WAMP.PremiumMasterCalculationId IS NULL THEN 0 ELSE WAMP.RatableDirectWrittenPremium END AS RatablePremium,
	CASE WHEN WAMP.PremiumMasterCalculationId IS NULL THEN 0 ELSE WAMP.ClassifiedDirectWrittenPremium END AS ClassifiedPremium,
	CASE WHEN WAMP.PremiumMasterCalculationId IS NULL THEN 0 ELSE WAMP.OtherModifiedDirectWrittenPremium END AS OtherModifiedPremium,
	CASE WHEN WAMP.PremiumMasterCalculationId IS NULL THEN 0 ELSE WAMP.ScheduleModifiedDirectWrittenPremium END AS ScheduleModifiedPremium,
	CASE WHEN WAMP.PremiumMasterCalculationId IS NULL THEN 0 ELSE WAMP.ExperienceModifiedDirectWrittenPremium END AS ExperienceModifiedPremium,
	CASE WHEN WAMP.PremiumMasterCalculationId IS NULL THEN 0 ELSE WAMP.SubjectDirectWrittenPremium END AS SubjectWrittenPremium
	
	FROM
	PremiumMasterCalculation PC with (nolock)
	Join StatisticalCoverage STATCOV  with (nolock)
	on STATCOV.StatisticalCoverageAKID = PC.StatisticalCoverageAKID 
	Join PolicyCoverage POLCOV
	on 
	POLCOV.PolicyCoverageAKID = PC.PolicyCoverageAKID  
	Join RiskLocation RL
	on RL.RiskLocationAKID=POLCOV.RiskLocationAKID and RL.SourceSystemID='PMS'
	Join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Policy POL with (nolock)
	on PC.PolicyAKID = POL.pol_ak_id AND POL.crrnt_snpsht_flag = 1and POL.source_sys_id='PMS'
	@{pipeline().parameters.JOIN_POLICY_LIST}
	Left Join InsuranceReferenceLineOfBusiness INRB
	on STATCOV.InsuranceReferenceLineOfBusinessAKId=INRB.InsuranceReferenceLineOfBusinessAKId and INRB.CurrentSnapshotFlag=1
	Left Join Product PROD
	on STATCOV.ProductAKId=PROD.ProductAKId and PROD.CurrentSnapshotFlag=1
	Left Join PolicyOffering POFF
	on POL.PolicyOfferingAKId=POFF.PolicyOfferingAKId and POFF.CurrentSnapshotFlag=1
	Left Join InsuranceSegment INSG
	on POL.InsuranceSegmentAKId=INSG.InsuranceSegmentAKId and INSG.CurrentSnapshotFlag=1
	Left Join StrategicProfitCenter STPC
	on POL.StrategicProfitCenterAKId=STPC.StrategicProfitCenterAKId and STPC.CurrentSnapshotFlag=1
	Left Join EnterpriseGroup ENGP
	on STPC.EnterpriseGroupId=ENGP.EnterpriseGroupId and ENGP.CurrentSnapshotFlag=1
	Left Join InsuranceReferenceLegalEntity NRLE
	on STPC.InsuranceReferenceLegalEntityId=NRLE.InsuranceReferenceLegalEntityId and NRLE.CurrentSnapshotFlag=1
	Left Join sup_insurance_line SIL
	ON SIL.ins_line_code = POLCOV.InsuranceLine
	AND SIL.crrnt_snpsht_flag = 1
	Left Join RatingPlan RPDT
	on POLCOV.RatingPlanAKId=RPDT.RatingPlanAKId and RPDT.CurrentSnapshotFlag=1
	left join ModifiedPremiumWorkersCompensationCalculation WAMP  with (nolock) 
	on  PC.PremiumMasterCalculationId = WAMP.PremiumMasterCalculationId 
	where PROD.ProductAbbreviation ='WC' 
	and @{pipeline().parameters.WHERE_PMC_CURRENT_MONTH} 
	and PC.PremiumMasterReasonAmendedCode not in ('COL','CWO','Claw Back','CWB')
	--and PC.PremiumMasterPremium<>0  
	and pc.PremiumMasterPremiumType = 'D' 
	and PC.SourceSystemID='PMS'
	@{pipeline().parameters.WHERE_CLAUSE}
),
Union AS (
	SELECT PremiumMasterCalculationID, SourceSystemID, AgencyAKID, PolicyAKID, ContractCustomerAKID, PolicyCoverageAKID, StatisticalCoverageAKID, PremiumTransactionAKID, BureauStatisticalCodeAKID, PremiumMasterTransactionCode, PremiumMasterRunDate, PremiumTransactionEnteredDate, PremiumMasterPolicyExpirationYear, PremiumMasterCoverageEffectiveDate, PremiumMasterCoverageExpirationDate, PremiumMasterPolicyTerm, PremiumMasterBureauPolicyType, PremiumMasterAuditCode, PremiumMasterPremiumType, PremiumMasterTypeBureauCode, PremiumMasterBureauStatisticalLine, PremiumMasterProductLine, PremiumMasterAgencyCommissionRate, PremiumMasterClassCode, PremiumMasterExposure, PremiumMasterSubLine, PremiumMasterStatisticalCode1, PremiumMasterStatisticalCode2, PremiumMasterStatisticalCode3, PremiumMasterRateModifier, PremiumMasterRateDeparture, PremiumMasterBureauInceptionDate, PremiumMasterCountersignAgencyType, PremiumMasterCountersignAgencyCode, PremiumMasterCountersignAgencyState, PremiumMasterCountersignAgencyRate, PremiumMasterPremium, PremiumMasterFullTermPremium, PremiumMasterRenewalIndicator, PremiumMasterRecordType, RatingCoverageEffectiveDate, RatingCoverageExpirationDate, RatingCoverageAKId, pol_sym, pol_key, pms_pol_lob_code, ClassOfBusiness, InsuranceLine, RiskUnitGroup, RiskUnit, RiskUnitSequenceNumber, MajorPerilCode, PremiumTransactionID, PremiumMasterReasonAmendedCode, InsuranceReferenceLineOfBusinessCode, ProductCode, PolicyOfferingCode, InsuranceSegmentCode, StrategicProfitCenterCode, EnterpriseGroupCode, InsuranceReferenceLegalEntityCode, RiskType, CoverageType, StandardInsuranceLineCode, RiskLocationHashKey, StateProvinceCode, PerilGroup, SubCoverageTypeCode, CoverageVersion, CoverageForm, AnnualStatementLineCode, SubAnnualStatementLineCode, PolicyAuditAKId, PolicyAuditEffectiveDate, SubNonAnnualStatementLineCode, CoverageCode, WAMP_RUNDATE, PremiumMasterCustomerCareCommissionRate, RatingPlanCode, GeneratedRecordIndicator, DirectWrittenPremium, RatablePremium, ClassifiedPremium, OtherModifiedPremium, ScheduleModifiedPremium, ExperienceModifiedPremium, SubjectWrittenPremium
	FROM SQ_PremiumMasterCalculation_PMS_OTHERLINES
	UNION
	SELECT PremiumMasterCalculationID, SourceSystemID, AgencyAKID, PolicyAKID, ContractCustomerAKID, PolicyCoverageAKID, StatisticalCoverageAKID, PremiumTransactionAKID, BureauStatisticalCodeAKID, PremiumMasterTransactionCode, PremiumMasterRunDate, PremiumTransactionEnteredDate, PremiumMasterPolicyExpirationYear, PremiumMasterCoverageEffectiveDate, PremiumMasterCoverageExpirationDate, PremiumMasterPolicyTerm, PremiumMasterBureauPolicyType, PremiumMasterAuditCode, PremiumMasterPremiumType, PremiumMasterTypeBureauCode, PremiumMasterBureauStatisticalLine, PremiumMasterProductLine, PremiumMasterAgencyCommissionRate, PremiumMasterClassCode, PremiumMasterExposure, PremiumMasterSubLine, PremiumMasterStatisticalCode1, PremiumMasterStatisticalCode2, PremiumMasterStatisticalCode3, PremiumMasterRateModifier, PremiumMasterRateDeparture, PremiumMasterBureauInceptionDate, PremiumMasterCountersignAgencyType, PremiumMasterCountersignAgencyCode, PremiumMasterCountersignAgencyState, PremiumMasterCountersignAgencyRate, PremiumMasterPremium, PremiumMasterFullTermPremium, PremiumMasterRenewalIndicator, PremiumMasterRecordType, RatingCoverageEffectiveDate, RatingCoverageExpirationDate, RatingCoverageAKId, pol_sym, pol_key, pms_pol_lob_code, ClassOfBusiness, InsuranceLine, RiskUnitGroup, RiskUnit, RiskUnitSequenceNumber, MajorPerilCode, PremiumTransactionID, PremiumMasterReasonAmendedCode, InsuranceReferenceLineOfBusinessCode, ProductCode, PolicyOfferingCode, InsuranceSegmentCode, StrategicProfitCenterCode, EnterpriseGroupCode, InsuranceReferenceLegalEntityCode, RiskType, CoverageType, StandardInsuranceLineCode, RiskLocationHashKey, StateProvinceCode, PerilGroup, SubCoverageTypeCode, CoverageVersion, CoverageForm, AnnualStatementLineCode, SubAnnualStatementLineCode, PolicyAuditAKId, PolicyAuditEffectiveDate, SubNonAnnualStatementLineCode, CoverageCode, WAMP_RUNDATE, PremiumMasterCustomerCareCommissionRate, RatingPlanCode, GeneratedRecordIndicator, DirectWrittenPremium, RatablePremium, ClassifiedPremium, OtherModifiedPremium, ScheduleModifiedPremium, ExperienceModifiedPremium, SubjectWrittenPremium
	FROM SQ_PremiumMasterCalculation_PMS_NO_IN_SCOPE
	UNION
	SELECT PremiumMasterCalculationID, SourceSystemID, AgencyAKID, PolicyAKID, ContractCustomerAKID, PolicyCoverageAKID, StatisticalCoverageAKID, PremiumTransactionAKID, BureauStatisticalCodeAKID, PremiumMasterTransactionCode, PremiumMasterRunDate, PremiumTransactionEnteredDate, PremiumMasterPolicyExpirationYear, PremiumMasterCoverageEffectiveDate, PremiumMasterCoverageExpirationDate, PremiumMasterPolicyTerm, PremiumMasterBureauPolicyType, PremiumMasterAuditCode, PremiumMasterPremiumType, PremiumMasterTypeBureauCode, PremiumMasterBureauStatisticalLine, PremiumMasterProductLine, PremiumMasterAgencyCommissionRate, PremiumMasterClassCode, PremiumMasterExposure, PremiumMasterSubLine, PremiumMasterStatisticalCode1, PremiumMasterStatisticalCode2, PremiumMasterStatisticalCode3, PremiumMasterRateModifier, PremiumMasterRateDeparture, PremiumMasterBureauInceptionDate, PremiumMasterCountersignAgencyType, PremiumMasterCountersignAgencyCode, PremiumMasterCountersignAgencyState, PremiumMasterCountersignAgencyRate, PremiumMasterPremium, PremiumMasterFullTermPremium, PremiumMasterRenewalIndicator, PremiumMasterRecordType, RatingCoverageEffectiveDate, RatingCoverageExpirationDate, RatingCoverageAKId, pol_sym, pol_key, pms_pol_lob_code, ClassOfBusiness, InsuranceLine, RiskUnitGroup, RiskUnit, RiskUnitSequenceNumber, MajorPerilCode, PremiumTransactionID, PremiumMasterReasonAmendedCode, InsuranceReferenceLineOfBusinessCode, ProductCode, PolicyOfferingCode, InsuranceSegmentCode, StrategicProfitCenterCode, EnterpriseGroupCode, InsuranceReferenceLegalEntityCode, RiskType, CoverageType, StandardInsuranceLineCode, RiskLocationHashKey, StateProvinceCode, PerilGroup, SubCoverageTypeCode, CoverageVersion, CoverageForm, AnnualStatementLineCode, SubAnnualStatementLineCode, PolicyAuditAKId, PolicyAuditEffectiveDate, SubNonAnnualStatementLineCode, CoverageCode, WAMP_RUNDATE, PremiumMasterCustomerCareCommissionRate, RatingPlanCode, GeneratedRecordIndicator, DirectWrittenPremium, RatablePremium, ClassifiedPremium, OtherModifiedPremium, ScheduleModifiedPremium, ExperienceModifiedPremium, SubjectWrittenPremium
	FROM SQ_PremiumMasterCalculation_PMS_WC
	UNION
	SELEC
	FROM 
),
EXP_Post_Source AS (
	SELECT
	PremiumMasterPremiumType AS i_PremiumType,
	PremiumMasterReasonAmendedCode,
	pol_sym AS i_PolicySymbol,
	pms_pol_lob_code AS i_Line_of_Business,
	InsuranceLine AS i_Insurance_Line,
	PremiumMasterTypeBureauCode AS i_TypeBureauCode,
	RiskUnitGroup AS i_RiskUnitGroup,
	MajorPerilCode AS i_MajorPerilCode,
	PremiumMasterSubLine AS i_SubLineCode,
	ClassOfBusiness AS i_class_of_business,
	pol_key AS PolicyKey,
	StatisticalCoverageAKID,
	PremiumMasterTransactionCode AS PremiumTransactionCode,
	PremiumMasterCoverageEffectiveDate AS PremiumTransactionEffectiveDate,
	PremiumMasterCoverageExpirationDate AS PremiumTransactionExpirationDate,
	RiskUnit,
	RiskUnitSequenceNumber,
	PremiumMasterClassCode AS ClassCode,
	PremiumMasterPremium AS PremiumAmount,
	PremiumMasterFullTermPremium AS FullTermPremiumAmount,
	PremiumTransactionEnteredDate,
	PremiumMasterCalculationID,
	AgencyAKID,
	PolicyAKID,
	ContractCustomerAKID,
	PolicyCoverageAKID,
	PremiumTransactionAKID,
	BureauStatisticalCodeAKID,
	PremiumMasterPolicyExpirationYear,
	PremiumMasterPolicyTerm,
	PremiumMasterBureauPolicyType,
	PremiumMasterAuditCode,
	PremiumMasterBureauStatisticalLine,
	PremiumMasterProductLine,
	PremiumMasterAgencyCommissionRate,
	PremiumMasterExposure,
	PremiumMasterStatisticalCode1,
	PremiumMasterStatisticalCode2,
	PremiumMasterStatisticalCode3,
	PremiumMasterRateModifier,
	PremiumMasterRateDeparture,
	PremiumMasterBureauInceptionDate,
	PremiumMasterCountersignAgencyType,
	PremiumMasterCountersignAgencyCode,
	PremiumMasterCountersignAgencyState,
	PremiumMasterCountersignAgencyRate,
	PremiumMasterRenewalIndicator,
	PremiumMasterRecordType,
	RatingCoverageEffectiveDate,
	RatingCoverageExpirationDate,
	RatingCoverageAKId,
	PremiumTransactionID,
	InsuranceReferenceLineOfBusinessCode,
	ProductCode,
	PolicyOfferingCode,
	InsuranceSegmentCode,
	StrategicProfitCenterCode,
	EnterpriseGroupCode,
	InsuranceReferenceLegalEntityCode,
	SourceSystemID,
	RiskType,
	CoverageType,
	StandardInsuranceLineCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_PremiumType)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_PremiumType) AS o_PremiumType_out,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(PremiumMasterReasonAmendedCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(PremiumMasterReasonAmendedCode) AS o_ReasonAmendedCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_PolicySymbol)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_PolicySymbol) AS o_PolicySymbol,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_Line_of_Business)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_Line_of_Business) AS o_Line_of_Business,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_Insurance_Line)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_Insurance_Line) AS o_Insurance_Line,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_TypeBureauCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_TypeBureauCode) AS o_TypeBureauCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_RiskUnitGroup)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_RiskUnitGroup) AS o_RiskUnitGroup,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_MajorPerilCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_MajorPerilCode) AS o_MajorPerilCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_SubLineCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_SubLineCode) AS o_SubLineCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_class_of_business)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_class_of_business) AS o_class_of_business,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	RiskLocationHashKey,
	StateProvinceCode,
	PerilGroup,
	CoverageForm,
	AnnualStatementLineCode AS AnnualStatementLineCode_DCT,
	SubAnnualStatementLineCode AS SubAnnualStatementLineCode_DCT,
	PolicyAuditAKId,
	PolicyAuditEffectiveDate,
	SubCoverageTypeCode,
	CoverageVersion,
	SubNonAnnualStatementLineCode,
	CoverageCode,
	-- *INF*: DECODE(TRUE, 
	-- i_GeneratedRecordIndicator='T' or i_GeneratedRecordIndicator='1' , 
	-- TRUNC(i_WAMP_RUNDATE,'DD'),
	-- PremiumTransactionEnteredDate)
	-- 
	-- 
	-- 
	DECODE(
	    TRUE,
	    i_GeneratedRecordIndicator = 'T' or i_GeneratedRecordIndicator = '1', CAST(TRUNC(i_WAMP_RUNDATE, 'DAY') AS TIMESTAMP_NTZ(0)),
	    PremiumTransactionEnteredDate
	) AS o_PremiumTransactionEnteredDate,
	PremiumMasterCustomerCareCommissionRate,
	RatingPlanCode AS i_RatingPlanCode,
	-- *INF*: IIF(ISNULL(i_RatingPlanCode), '1', i_RatingPlanCode)
	IFF(i_RatingPlanCode IS NULL, '1', i_RatingPlanCode) AS o_RatingPlanCode,
	GeneratedRecordIndicator AS i_GeneratedRecordIndicator,
	-- *INF*: IIF(i_GeneratedRecordIndicator='T'  OR i_GeneratedRecordIndicator='1', '1' ,'0' ) 
	IFF(i_GeneratedRecordIndicator = 'T' OR i_GeneratedRecordIndicator = '1', '1', '0') AS o_GeneratedRecordIndicator,
	DirectWrittenPremium,
	RatablePremium,
	ClassifiedPremium,
	OtherModifiedPremium,
	ScheduleModifiedPremium,
	ExperienceModifiedPremium,
	SubjectWrittenPremium,
	WAMP_RUNDATE AS i_WAMP_RUNDATE,
	PremiumMasterRunDate AS i_PremiumMasterRunDate,
	-- *INF*: DECODE(TRUE,
	-- i_GeneratedRecordIndicator='T' or i_GeneratedRecordIndicator='1' ,
	-- LAST_DAY(TRUNC(i_WAMP_RUNDATE,'MM')) ,
	-- i_PremiumMasterRunDate)
	-- 
	-- 
	-- 
	-- --i_PremiumMasterRunDate
	DECODE(
	    TRUE,
	    i_GeneratedRecordIndicator = 'T' or i_GeneratedRecordIndicator = '1', LAST_DAY(CAST(TRUNC(i_WAMP_RUNDATE, 'MONTH') AS TIMESTAMP_NTZ(0))),
	    i_PremiumMasterRunDate
	) AS o_RunDate,
	-- *INF*: DECODE(TRUE,
	-- i_GeneratedRecordIndicator='T' or i_GeneratedRecordIndicator='1' ,
	-- LAST_DAY(TRUNC(i_WAMP_RUNDATE,'MM')) ,
	-- i_PremiumMasterRunDate)
	-- 
	DECODE(
	    TRUE,
	    i_GeneratedRecordIndicator = 'T' or i_GeneratedRecordIndicator = '1', LAST_DAY(CAST(TRUNC(i_WAMP_RUNDATE, 'MONTH') AS TIMESTAMP_NTZ(0))),
	    i_PremiumMasterRunDate
	) AS o_PremiumTransactionBookedDate
	FROM Union
),
mplt_Premium_ASL_Insurance_Hierarchy AS (WITH
	LKP_asl_product_code AS (
		SELECT
		asl_prdct_code_dim_id,
		asl_prdct_code
		FROM (
			SELECT 
				asl_prdct_code_dim_id,
				asl_prdct_code
			FROM asl_product_code_dim
			WHERE crrnt_snpsht_flag=1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY asl_prdct_code ORDER BY asl_prdct_code_dim_id DESC) = 1
	),
	LKP_product_code_dim AS (
		SELECT
		prdct_code_dim_id,
		prdct_code
		FROM (
			SELECT product_code_dim.prdct_code_dim_id as prdct_code_dim_id, product_code_dim.prdct_code as prdct_code FROM product_code_dim
			where crrnt_snpsht_flag=1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY prdct_code ORDER BY prdct_code_dim_id DESC) = 1
	),
	LKP_strategic_business_division_dim AS (
		SELECT
		strtgc_bus_dvsn_dim_id,
		crrnt_snpsht_flag,
		audit_id,
		edw_strtgc_bus_dvsn_ak_id
		FROM (
			SELECT 
				strtgc_bus_dvsn_dim_id,
				crrnt_snpsht_flag,
				audit_id,
				edw_strtgc_bus_dvsn_ak_id
			FROM strategic_business_division_dim
			WHERE crrnt_snpsht_flag=1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_strtgc_bus_dvsn_ak_id ORDER BY strtgc_bus_dvsn_dim_id DESC) = 1
	),
	LKP_asl_dim AS (
		SELECT
		asl_dim_id,
		asl_code,
		sub_asl_code,
		sub_non_asl_code
		FROM (
			SELECT 
				asl_dim_id,
				asl_code,
				sub_asl_code,
				sub_non_asl_code
			FROM asl_dim
			WHERE crrnt_snpsht_flag=1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY asl_code,sub_asl_code,sub_non_asl_code ORDER BY asl_dim_id DESC) = 1
	),
	INPUT AS (
		
	),
	EXP_accept_inputs AS (
		SELECT
		PolicyKey,
		PolicyEffectiveDate,
		PolicyExpirationDate,
		PremiumTransactionID,
		ReinsuranceCoverageAKID,
		StatisticalCoverageAKID,
		PremiumTransactionCode,
		-- *INF*: LTRIM(RTRIM(PremiumTransactionCode))
		LTRIM(RTRIM(PremiumTransactionCode)) AS PremiumTransactionCode_out,
		PremiumTransactionEnteredDate,
		PremiumTransactionEffectiveDate,
		PremiumTransactionExpirationDate,
		PremiumTransactionBookedDate,
		PremiumType,
		-- *INF*: LTRIM(RTRIM(PremiumType))
		LTRIM(RTRIM(PremiumType)) AS PremiumType_out,
		ReasonAmendedCode,
		-- *INF*: LTRIM(RTRIM(ReasonAmendedCode))
		LTRIM(RTRIM(ReasonAmendedCode)) AS ReasonAmendedCode_out,
		PolicySymbol,
		-- *INF*: LTRIM(RTRIM(PolicySymbol))
		LTRIM(RTRIM(PolicySymbol)) AS PolicySymbol_out,
		Line_of_Business,
		-- *INF*: LTRIM(RTRIM(Line_of_Business))
		LTRIM(RTRIM(Line_of_Business)) AS Line_of_Business_out,
		Insurance_Line,
		-- *INF*: LTRIM(RTRIM(Insurance_Line))
		LTRIM(RTRIM(Insurance_Line)) AS Insurance_Line_out,
		TypeBureauCode,
		-- *INF*: LTRIM(RTRIM(TypeBureauCode))
		LTRIM(RTRIM(TypeBureauCode)) AS TypeBureauCode_out,
		RiskUnitGroup,
		-- *INF*: LTRIM(RTRIM(RiskUnitGroup))
		LTRIM(RTRIM(RiskUnitGroup)) AS RiskUnitGroup_out,
		RiskUnit,
		RiskUnitSequenceNumber,
		MajorPerilCode,
		-- *INF*: LTRIM(RTRIM(MajorPerilCode))
		LTRIM(RTRIM(MajorPerilCode)) AS MajorPerilCode_out,
		SubLineCode,
		-- *INF*: LTRIM(RTRIM(SubLineCode))
		LTRIM(RTRIM(SubLineCode)) AS SubLineCode_out,
		ClassCode,
		-- *INF*: LTRIM(RTRIM(ClassCode))
		LTRIM(RTRIM(ClassCode)) AS ClassCode_out,
		class_of_business,
		-- *INF*: LTRIM(RTRIM(class_of_business))
		LTRIM(RTRIM(class_of_business)) AS class_of_business_out,
		nsi_indicator,
		PremiumAmount,
		FullTermPremiumAmount,
		EarnedPremiumAmount,
		ChangeInEarnedPremium,
		StatisticalCoverageEffectiveDate,
		StatisticalCoverageExpirationDate,
		StatisticalCoverageCancellationDate,
		RunDate,
		PremiumMasterCalculationID,
		AgencyAKID,
		PolicyAKID,
		strtgc_bus_dvsn_ak_id,
		ContractCustomerAKID,
		RiskLocationAKID,
		PolicyCoverageAKID,
		PremiumTransactionAKID,
		BureauStatisticalCodeAKID,
		PremiumMasterPolicyExpirationYear,
		PremiumMasterPolicyTerm,
		PremiumMasterBureauPolicyType,
		PremiumMasterAuditCode,
		PremiumMasterBureauStatisticalLine,
		PremiumMasterProductLine,
		PremiumMasterAgencyCommissionRate,
		PremiumMasterExposure,
		PremiumMasterStatisticalCode1,
		PremiumMasterStatisticalCode2,
		PremiumMasterStatisticalCode3,
		PremiumMasterRateModifier,
		PremiumMasterRateDeparture,
		PremiumMasterBureauInceptionDate,
		PremiumMasterCountersignAgencyType,
		PremiumMasterCountersignAgencyCode,
		PremiumMasterCountersignAgencyState,
		PremiumMasterCountersignAgencyRate,
		PremiumMasterRenewalIndicator,
		PremiumMasterRecordType,
		premium_master_stage_id,
		pm_policy_number,
		pm_module,
		pm_account_date,
		pm_sar_location_number,
		pm_unit_number,
		pm_risk_state,
		pm_risk_zone_territory,
		pm_tax_location,
		pm_risk_zip_code_postal_zone,
		pm_sar_insurance_line,
		pm_sar_sub_location_number,
		pm_sar_risk_unit_group,
		pm_sar_class_code_group,
		pm_sar_class_code_member,
		pm_sar_sequence_risk_unit_n,
		pm_sar_sequence_risk_unit_a,
		pm_sar_type_exposure,
		pm_sar_mp_seq_no,
		pm_csp_inception_date,
		pm_coverage_effective_date,
		pm_coverage_expiration_date,
		pm_reinsurance_ceded_premium,
		pm_reins_ceded_orig_premium,
		pm_reinsurance_type_code,
		pm_reinsurance_company_number,
		pm_reinsurance_ratio,
		AuditID,
		ProductCode,
		RatingCoverageEffectiveDate,
		RatingCoverageExpirationDate,
		RatingCoverageCancellationDate,
		RatingCoverageAKID,
		PolicyOfferingCode,
		PolicyCoverageEffectiveDate,
		PolicyCoverageExpirationDate,
		AgencyActualCommissionRate,
		InsuranceReferenceLineOfBusinessCode,
		EnterpriseGroupCode,
		InsuranceReferenceLegalEntityCode,
		StrategicProfitCenterCode,
		InsuranceSegmentCode,
		StandardInsuranceLineCode,
		RatingCoverage,
		RiskType,
		CoverageType,
		StandardSpecialClassGroupCode,
		StandardIncreasedLimitGroupCode,
		StandardPackageModifcationAdjustmentGroupCode,
		SourceSystemID,
		EarnedExposure,
		ChangeInEarnedExposure,
		RiskLocationHashKey,
		PerilGroup,
		CoverageForm,
		AnnualStatementLineCode_DCT,
		SubAnnualStatementLineCode_DCT,
		PolicyAuditAKID,
		PolicyAuditEffectiveDate,
		SubCoverageTypeCode,
		CoverageVersion,
		SubNonAnnualStatementLineCode_DCT,
		CoverageCode,
		CustomerCareCommissionRate,
		RatingPlanCode,
		CoverageCancellationDate,
		GeneratedRecordIndicator,
		DirectWrittenPremium,
		RatablePremium,
		ClassifiedPremium,
		OtherModifiedPremium,
		ScheduleModifiedPremium,
		ExperienceModifiedPremium,
		SubjectWrittenPremium,
		EarnedDirectWrittenPremium,
		EarnedClassifiedPremium,
		EarnedRatablePremium,
		EarnedOtherModifiedPremium,
		EarnedScheduleModifiedPremium,
		EarnedExperienceModifiedPremium,
		EarnedSubjectWrittenPremium,
		EarnedPremiumRunDate,
		PremiumMasterWrittenExposure,
		DeclaredEventFlag
		FROM INPUT
	),
	EXP_Evaluate AS (
		SELECT
		PolicyKey,
		PolicyEffectiveDate,
		PolicyExpirationDate,
		PremiumTransactionID,
		ReinsuranceCoverageAKID,
		StatisticalCoverageAKID,
		PremiumTransactionCode_out AS PremiumTransactionCode,
		PremiumTransactionEnteredDate,
		PremiumTransactionEffectiveDate,
		PremiumTransactionExpirationDate,
		PremiumTransactionBookedDate,
		PremiumType_out AS PremiumType,
		ReasonAmendedCode_out AS ReasonAmendedCode,
		PolicySymbol_out AS PolicySymbol,
		Line_of_Business_out AS Line_of_Business,
		Insurance_Line_out AS Insurance_Line,
		TypeBureauCode_out AS Type_Bureau,
		RiskUnitGroup_out AS Risk_Unit_Group,
		RiskUnit,
		RiskUnitSequenceNumber,
		MajorPerilCode_out AS Major_Peril,
		SubLineCode_out AS SubLine,
		ClassCode_out AS Class_Code,
		class_of_business_out AS class_of_business,
		nsi_indicator,
		-- *INF*: SUBSTR(PolicySymbol,1,2)
		SUBSTR(PolicySymbol, 1, 2) AS v_symbol_pos_1_2,
		PremiumAmount,
		FullTermPremiumAmount,
		EarnedPremiumAmount,
		ChangeInEarnedPremium,
		v_symbol_pos_1_2 AS symbol_pos_1_2_out,
		-- *INF*: DECODE(TRUE,
		-- IN (v_symbol_pos_1_2,'CP','BC','BD','NA','NB','NS','BO') AND type_bureau = 'CF' AND IN(risk_unit_group,'917','918','967','974') , '140',
		-- IN (v_symbol_pos_1_2,'HH', 'FP', 'FL') AND IN (major_peril,'210','211','249','250','081','280') AND type_bureau = 'PF', '20',
		-- IN (v_symbol_pos_1_2,'CP', 'BC', 'BD', 'BO', 'BG', 'BH', 'NS', 'NA', 'NB','CM')  AND IN (major_peril,'415', '463', '490', '496', '498','599','919') AND type_bureau = 'CF', '20',
		-- IN (v_symbol_pos_1_2,'HH', 'FP', 'FL') AND IN (major_peril,@{pipeline().parameters.MP_220_230}) AND type_bureau = 'PF', '40',
		-- IN (v_symbol_pos_1_2,'BA','BB','BC','BD','BG', 'BH', 'CP','FL','FP','NA','NB','NS','BO') AND major_peril = '050' AND IN (type_bureau,'MS','NB') , '40',
		-- IN (v_symbol_pos_1_2,'CP','BC','BD','BO','BG','BH','NS','NA','NB','CM')  AND IN (major_peril,'425','426','435','455', '480','599') AND IN(type_bureau,'CF','GS'), '40',
		-- IN (v_symbol_pos_1_2,'HH', 'HB', 'HA','HX','PX','XX') AND IN (major_peril,'002', '097', '911','050','914')  AND IN (type_bureau,'PH','MS') , '60',
		-- IN (v_symbol_pos_1_2,'BG','BH') AND IN (major_peril,'901','902') AND IN(type_bureau,'CF','BC'), '80',
		-- IN (v_symbol_pos_1_2,'BA','BB') AND major_peril ='919' AND type_bureau = 'BC', '80',
		-- IN (v_symbol_pos_1_2,'BA','BB','BG','BH') AND IN (major_peril,'903','904','905','908') AND IN(type_bureau,'BB','BE','BC'), '80',
		-- IN (v_symbol_pos_1_2,'BA','BB','XX') AND IN (major_peril,'901','902','599') AND IN(type_bureau,'BB','BE','BC'), '80',
		-- IN (v_symbol_pos_1_2,'BG','BH') AND IN (major_peril,'901','902') AND IN(type_bureau,'CF','BC'), '100',
		-- IN (v_symbol_pos_1_2,'BG','BH','BA','BB') AND major_peril ='907' AND type_bureau = 'BE', '100',
		-- IN (v_symbol_pos_1_2,'BA','BB') AND major_peril ='919' AND type_bureau = 'BE', '100',
		-- IN (v_symbol_pos_1_2,'BA','BB','XX') AND IN (major_peril,'901','902','599') AND IN(type_bureau,'BB','BE','BC'), '100',
		-- IN (v_symbol_pos_1_2,'HH', 'HB', 'HA', 'IP', 'IB','CP', 'BC', 'BD', 'BO', 'BG', 'BH', 'NS', 'NA', 'NB','PX') AND IN (major_peril,'062','200','201', '042','044','206','551','599','909',
		-- '919') AND IN (type_bureau,'PI','IM') , '120',
		-- IN (v_symbol_pos_1_2,'HH', 'HB', 'HA','FP', 'FL') AND IN (major_peril, @{pipeline().parameters.MP_260_261}) AND type_bureau = 'PQ', '140',
		-- IN(type_bureau,'WP','WC'), '160',
		-- IN (v_symbol_pos_1_2,'HH', 'HB', 'HA','IB') AND type_bureau = 'PL', '200',
		-- IN (v_symbol_pos_1_2,'CP','BO','NS','BG','BH') AND IN(major_peril,'530','550','599') AND type_bureau = 'GL' AND IN(subline,'336','365') , '240',
		-- IN (v_symbol_pos_1_2,'CM','NE','NS') AND IN(major_peril,'540') AND type_bureau = 'GL' AND subline = '336', '250',
		-- IN (v_symbol_pos_1_2,'HH', 'UP','XX') AND major_peril ='017' AND type_bureau = 'GL', '220',
		-- IN (v_symbol_pos_1_2,'UC','CP','NU','CU') AND major_peril ='517' AND type_bureau = 'GL', '220',
		-- IN (v_symbol_pos_1_2,'CP','BC','BD','BO','BG','BH','NS','NA','NB','XX') AND IN(major_peril,'530', '599','919','067','084','085') AND type_bureau = 'GL' AND 
		-- IN(subline,@{pipeline().parameters.SUB_325_335},@{pipeline().parameters.SUB_342_350},'324'), '220',
		-- IN (v_symbol_pos_1_2,'BA','BB') AND major_peril = '540' AND type_bureau = 'BE' AND IN(risk_unit_group,'366','367') ,'230',
		-- IN (v_symbol_pos_1_2,'BC','BD','BG','BH','CP','BO','NS','NA','NB') AND IN(major_peril,'540','541') AND type_bureau = 'GL' AND  subline='334' AND
		-- IN (class_code,'22222', '22250'), '230',
		-- IN (v_symbol_pos_1_2,'BC','BD','BG','BH','CP','BO','NS','NA','NB')  AND  type_bureau = 'GL' AND  IN(risk_unit_group,'366','367') ,'230',
		-- IN (v_symbol_pos_1_2,'BG','BH','CP','NS') AND major_peril = '540' AND type_bureau = 'AL' AND IN(risk_unit_group,'417','418') ,'230',
		-- v_symbol_pos_1_2 = 'NS' AND major_peril = '540' AND type_bureau = 'GL' AND IN(risk_unit_group,'340') , '230',
		-- v_symbol_pos_1_2 = 'CP' AND major_peril = '540'  AND type_bureau = 'GL'  AND subline = '345' , '230',
		-- IN (v_symbol_pos_1_2,'NN','NK','NE','CD','CM') ,'230',
		-- IN (v_symbol_pos_1_2,'HH','PP','PA','PM','PS','PT','HA','XX') AND IN(major_peril,@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},'150') AND IN(type_bureau,'RL','RN'),'260',
		-- IN (v_symbol_pos_1_2,'CP','BC','BD','BG','BH','GG','NS','NA','NB','XX') AND IN(major_peril,@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},'150','599',@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_930_931}) AND IN(type_bureau,'AN','AL','NB') ,'340',
		-- IN (v_symbol_pos_1_2,'HH','PP','PA','PM','PS','PT','HA','XA','XX') AND IN(major_peril, @{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163} ,'168','169',@{pipeline().parameters.MP_170_178},'912') AND  type_bureau = 'RP','440',
		-- IN (v_symbol_pos_1_2,'CP','BC','BD','BG','BH','GG','NS','NA','NB') AND IN(major_peril,'132',@{pipeline().parameters.MP_145_160},'177','178',@{pipeline().parameters.MP_163_166}, @{pipeline().parameters.MP_170_173}, @{pipeline().parameters.MP_269_270}) AND type_bureau = 'AP','500',
		-- IN (v_symbol_pos_1_2,'CP','BC','BD','BO','BG','BH','NS','NA','NB','FF') AND IN(major_peril,'566','016') AND IN(type_bureau,'FT','CR'),'600',
		-- IN (v_symbol_pos_1_2,'NF') AND IN(major_peril,'566','599'),'600', 
		-- IN (v_symbol_pos_1_2,'NC','NJ','NL','NO','NM'), '620',
		-- v_symbol_pos_1_2 = 'NF' AND major_peril = '565', '640',
		-- IN (v_symbol_pos_1_2,'CP','BC','BD','BO','BG','BH','NS','NA','NB') AND IN(major_peril,'565','599') AND IN(type_bureau,'BT','CR','FT'), '640',
		-- IN (v_symbol_pos_1_2,'CP','BA','BB','BC','BD','BO','BG','BH','NS','NA','NB','CM') AND IN(major_peril,'570','906') AND IN(type_bureau,'CF','BE','BM'),'660',
		-- '999')
		-- 
		-- 
		-- 
		DECODE(
		    TRUE,
		    v_symbol_pos_1_2 IN ('CP','BC','BD','NA','NB','NS','BO') AND type_bureau = 'CF' AND risk_unit_group IN ('917','918','967','974'), '140',
		    v_symbol_pos_1_2 IN ('HH','FP','FL') AND major_peril IN ('210','211','249','250','081','280') AND type_bureau = 'PF', '20',
		    v_symbol_pos_1_2 IN ('CP','BC','BD','BO','BG','BH','NS','NA','NB','CM') AND major_peril IN ('415','463','490','496','498','599','919') AND type_bureau = 'CF', '20',
		    v_symbol_pos_1_2 IN ('HH','FP','FL') AND major_peril IN (@{pipeline().parameters.MP_220_230}) AND type_bureau = 'PF', '40',
		    v_symbol_pos_1_2 IN ('BA','BB','BC','BD','BG','BH','CP','FL','FP','NA','NB','NS','BO') AND major_peril = '050' AND type_bureau IN ('MS','NB'), '40',
		    v_symbol_pos_1_2 IN ('CP','BC','BD','BO','BG','BH','NS','NA','NB','CM') AND major_peril IN ('425','426','435','455','480','599') AND type_bureau IN ('CF','GS'), '40',
		    v_symbol_pos_1_2 IN ('HH','HB','HA','HX','PX','XX') AND major_peril IN ('002','097','911','050','914') AND type_bureau IN ('PH','MS'), '60',
		    v_symbol_pos_1_2 IN ('BG','BH') AND major_peril IN ('901','902') AND type_bureau IN ('CF','BC'), '80',
		    v_symbol_pos_1_2 IN ('BA','BB') AND major_peril = '919' AND type_bureau = 'BC', '80',
		    v_symbol_pos_1_2 IN ('BA','BB','BG','BH') AND major_peril IN ('903','904','905','908') AND type_bureau IN ('BB','BE','BC'), '80',
		    v_symbol_pos_1_2 IN ('BA','BB','XX') AND major_peril IN ('901','902','599') AND type_bureau IN ('BB','BE','BC'), '80',
		    v_symbol_pos_1_2 IN ('BG','BH') AND major_peril IN ('901','902') AND type_bureau IN ('CF','BC'), '100',
		    v_symbol_pos_1_2 IN ('BG','BH','BA','BB') AND major_peril = '907' AND type_bureau = 'BE', '100',
		    v_symbol_pos_1_2 IN ('BA','BB') AND major_peril = '919' AND type_bureau = 'BE', '100',
		    v_symbol_pos_1_2 IN ('BA','BB','XX') AND major_peril IN ('901','902','599') AND type_bureau IN ('BB','BE','BC'), '100',
		    v_symbol_pos_1_2 IN ('HH','HB','HA','IP','IB','CP','BC','BD','BO','BG','BH','NS','NA','NB','PX') AND major_peril IN ('062','200','201','042','044','206','551','599','909','919') AND type_bureau IN ('PI','IM'), '120',
		    v_symbol_pos_1_2 IN ('HH','HB','HA','FP','FL') AND major_peril IN (@{pipeline().parameters.MP_260_261}) AND type_bureau = 'PQ', '140',
		    type_bureau IN ('WP','WC'), '160',
		    v_symbol_pos_1_2 IN ('HH','HB','HA','IB') AND type_bureau = 'PL', '200',
		    v_symbol_pos_1_2 IN ('CP','BO','NS','BG','BH') AND major_peril IN ('530','550','599') AND type_bureau = 'GL' AND subline IN ('336','365'), '240',
		    v_symbol_pos_1_2 IN ('CM','NE','NS') AND major_peril IN ('540') AND type_bureau = 'GL' AND subline = '336', '250',
		    v_symbol_pos_1_2 IN ('HH','UP','XX') AND major_peril = '017' AND type_bureau = 'GL', '220',
		    v_symbol_pos_1_2 IN ('UC','CP','NU','CU') AND major_peril = '517' AND type_bureau = 'GL', '220',
		    v_symbol_pos_1_2 IN ('CP','BC','BD','BO','BG','BH','NS','NA','NB','XX') AND major_peril IN ('530','599','919','067','084','085') AND type_bureau = 'GL' AND subline IN (@{pipeline().parameters.SUB_325_335},@{pipeline().parameters.SUB_342_350},'324'), '220',
		    v_symbol_pos_1_2 IN ('BA','BB') AND major_peril = '540' AND type_bureau = 'BE' AND risk_unit_group IN ('366','367'), '230',
		    v_symbol_pos_1_2 IN ('BC','BD','BG','BH','CP','BO','NS','NA','NB') AND major_peril IN ('540','541') AND type_bureau = 'GL' AND subline = '334' AND class_code IN ('22222','22250'), '230',
		    v_symbol_pos_1_2 IN ('BC','BD','BG','BH','CP','BO','NS','NA','NB') AND type_bureau = 'GL' AND risk_unit_group IN ('366','367'), '230',
		    v_symbol_pos_1_2 IN ('BG','BH','CP','NS') AND major_peril = '540' AND type_bureau = 'AL' AND risk_unit_group IN ('417','418'), '230',
		    v_symbol_pos_1_2 = 'NS' AND major_peril = '540' AND type_bureau = 'GL' AND risk_unit_group IN ('340'), '230',
		    v_symbol_pos_1_2 = 'CP' AND major_peril = '540' AND type_bureau = 'GL' AND subline = '345', '230',
		    v_symbol_pos_1_2 IN ('NN','NK','NE','CD','CM'), '230',
		    v_symbol_pos_1_2 IN ('HH','PP','PA','PM','PS','PT','HA','XX') AND major_peril IN (@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},'150') AND type_bureau IN ('RL','RN'), '260',
		    v_symbol_pos_1_2 IN ('CP','BC','BD','BG','BH','GG','NS','NA','NB','XX') AND major_peril IN (@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},'150','599',@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_930_931}) AND type_bureau IN ('AN','AL','NB'), '340',
		    v_symbol_pos_1_2 IN ('HH','PP','PA','PM','PS','PT','HA','XA','XX') AND major_peril IN (@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},'168','169',@{pipeline().parameters.MP_170_178},'912') AND type_bureau = 'RP', '440',
		    v_symbol_pos_1_2 IN ('CP','BC','BD','BG','BH','GG','NS','NA','NB') AND major_peril IN ('132',@{pipeline().parameters.MP_145_160},'177','178',@{pipeline().parameters.MP_163_166},@{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270}) AND type_bureau = 'AP', '500',
		    v_symbol_pos_1_2 IN ('CP','BC','BD','BO','BG','BH','NS','NA','NB','FF') AND major_peril IN ('566','016') AND type_bureau IN ('FT','CR'), '600',
		    v_symbol_pos_1_2 IN ('NF') AND major_peril IN ('566','599'), '600',
		    v_symbol_pos_1_2 IN ('NC','NJ','NL','NO','NM'), '620',
		    v_symbol_pos_1_2 = 'NF' AND major_peril = '565', '640',
		    v_symbol_pos_1_2 IN ('CP','BC','BD','BO','BG','BH','NS','NA','NB') AND major_peril IN ('565','599') AND type_bureau IN ('BT','CR','FT'), '640',
		    v_symbol_pos_1_2 IN ('CP','BA','BB','BC','BD','BO','BG','BH','NS','NA','NB','CM') AND major_peril IN ('570','906') AND type_bureau IN ('CF','BE','BM'), '660',
		    '999'
		) AS v_Coverage_Code_1_or_ASL_Code,
		v_Coverage_Code_1_or_ASL_Code AS aslcode,
		-- *INF*: DECODE(TRUE,
		-- IN (v_symbol_pos_1_2,'HH', 'FP', 'FL') AND IN (major_peril,@{pipeline().parameters.MP_220_230}) AND type_bureau = 'PF', '421',
		-- IN (v_symbol_pos_1_2,'BA','BB','BC','BD','BG', 'BH', 'CP','FL','FP','NA','NB','NS','BO') AND major_peril = '050' AND IN (type_bureau,'MS','NB') , '421',
		-- IN (v_symbol_pos_1_2,'CP','BC','BD','BO','BG','BH','NS','NA','NB','CM')  AND IN (major_peril,'425','426','435','455', '480','599') AND IN(type_bureau,'CF','GS'), '421',
		-- IN (v_symbol_pos_1_2,'HH','PP','PA','PM','PS','PT','HA','XX') AND IN(major_peril,'130') AND type_bureau = 'RN', '270',
		-- IN (v_symbol_pos_1_2,'HH','PP','PA','PM','PS','PT','HA','XX') AND IN(major_peril,@{pipeline().parameters.MP_100_121},@{pipeline().parameters.MP_140_143},'150') AND type_bureau = 'RL','280',
		-- IN (v_symbol_pos_1_2,'CP','BC','BD','BG','BH','GG','NS','NA','NB','XX') AND IN(major_peril,'130',@{pipeline().parameters.MP_930_931}) AND IN(type_bureau,'AN','NB'), '360',
		-- IN (v_symbol_pos_1_2,'CP','BC','BD','BG','BH','GG','NS','NA','NB','XX') AND IN(major_peril,@{pipeline().parameters.MP_100_125},@{pipeline().parameters.MP_140_143},'150',@{pipeline().parameters.MP_271_274},'599') 
		-- AND IN(type_bureau,'AL') ,'380',
		-- IN (v_symbol_pos_1_2,'HH','PP','PA','PM','PS','PT','HA','XX') AND IN(major_peril, @{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_155} ,'168','169',@{pipeline().parameters.MP_157_163},'174','912') AND  type_bureau = 'RP','460',
		-- IN (v_symbol_pos_1_2,'HH','PP','PA','PM','PS','PT','XA','XX') AND IN(major_peril, @{pipeline().parameters.MP_170_173},'178','156') AND  type_bureau = 'RP','480',
		-- IN (v_symbol_pos_1_2,'CP','BC','BD','BG','BH','GG','NS','NA','NB') AND IN(major_peril,'132','147','177','270','145','146',@{pipeline().parameters.MP_148_155},@{pipeline().parameters.MP_157_160},@{pipeline().parameters.MP_163_166}) AND type_bureau = 'AP','520',
		-- IN (v_symbol_pos_1_2,'CP','BC','BD','BG','BH','GG','NS','NA','NB') AND IN(major_peril,'156','178','269',@{pipeline().parameters.MP_170_173}) AND type_bureau = 'AP','540',
		-- 'N/A')
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		DECODE(
		    TRUE,
		    v_symbol_pos_1_2 IN ('HH','FP','FL') AND major_peril IN (@{pipeline().parameters.MP_220_230}) AND type_bureau = 'PF', '421',
		    v_symbol_pos_1_2 IN ('BA','BB','BC','BD','BG','BH','CP','FL','FP','NA','NB','NS','BO') AND major_peril = '050' AND type_bureau IN ('MS','NB'), '421',
		    v_symbol_pos_1_2 IN ('CP','BC','BD','BO','BG','BH','NS','NA','NB','CM') AND major_peril IN ('425','426','435','455','480','599') AND type_bureau IN ('CF','GS'), '421',
		    v_symbol_pos_1_2 IN ('HH','PP','PA','PM','PS','PT','HA','XX') AND major_peril IN ('130') AND type_bureau = 'RN', '270',
		    v_symbol_pos_1_2 IN ('HH','PP','PA','PM','PS','PT','HA','XX') AND major_peril IN (@{pipeline().parameters.MP_100_121},@{pipeline().parameters.MP_140_143},'150') AND type_bureau = 'RL', '280',
		    v_symbol_pos_1_2 IN ('CP','BC','BD','BG','BH','GG','NS','NA','NB','XX') AND major_peril IN ('130',@{pipeline().parameters.MP_930_931}) AND type_bureau IN ('AN','NB'), '360',
		    v_symbol_pos_1_2 IN ('CP','BC','BD','BG','BH','GG','NS','NA','NB','XX') AND major_peril IN (@{pipeline().parameters.MP_100_125},@{pipeline().parameters.MP_140_143},'150',@{pipeline().parameters.MP_271_274},'599') AND type_bureau IN ('AL'), '380',
		    v_symbol_pos_1_2 IN ('HH','PP','PA','PM','PS','PT','HA','XX') AND major_peril IN (@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_155},'168','169',@{pipeline().parameters.MP_157_163},'174','912') AND type_bureau = 'RP', '460',
		    v_symbol_pos_1_2 IN ('HH','PP','PA','PM','PS','PT','XA','XX') AND major_peril IN (@{pipeline().parameters.MP_170_173},'178','156') AND type_bureau = 'RP', '480',
		    v_symbol_pos_1_2 IN ('CP','BC','BD','BG','BH','GG','NS','NA','NB') AND major_peril IN ('132','147','177','270','145','146',@{pipeline().parameters.MP_148_155},@{pipeline().parameters.MP_157_160},@{pipeline().parameters.MP_163_166}) AND type_bureau = 'AP', '520',
		    v_symbol_pos_1_2 IN ('CP','BC','BD','BG','BH','GG','NS','NA','NB') AND major_peril IN ('156','178','269',@{pipeline().parameters.MP_170_173}) AND type_bureau = 'AP', '540',
		    'N/A'
		) AS v_Coverage_Code_2_or_SubASLCode,
		v_Coverage_Code_2_or_SubASLCode AS subaslcode,
		-- *INF*: DECODE(TRUE,
		-- IN (v_symbol_pos_1_2,'HH', 'FP', 'FL') AND IN (major_peril,@{pipeline().parameters.MP_220_230}) AND type_bureau = 'PF', '421',
		-- IN (v_symbol_pos_1_2,'BA','BB','BC','BD','BG', 'BH', 'CP','FL','FP','NA','NB','NS','BO') AND major_peril = '050' AND IN (type_bureau,'MS','NB') , '421',
		-- IN (v_symbol_pos_1_2,'CP','BC','BD','BO','BG','BH','NS','NA','NB','CM')  AND IN (major_peril,'425','426','435','455', '480','599') AND IN(type_bureau,'CF','GS'), '421',
		-- IN (v_symbol_pos_1_2,'HH','PP','PA','PM','PS','PT','HA','XX') AND IN(major_peril,@{pipeline().parameters.MP_101_103},@{pipeline().parameters.MP_114_119},'130',@{pipeline().parameters.MP_140_143},'100') AND IN(type_bureau,'RL','RN'),'300',
		-- IN (v_symbol_pos_1_2,'HH','PP','PA','PM','PS','PT','HA','XX') AND IN(major_peril,@{pipeline().parameters.MP_110_112},@{pipeline().parameters.MP_120_121},'100') AND type_bureau = 'RL','320',
		-- IN (v_symbol_pos_1_2,'CP','BC','BD','BG','BH','GG','NS','NA','NB','XX') AND IN(major_peril,@{pipeline().parameters.MP_101_103},@{pipeline().parameters.MP_114_119},'130',@{pipeline().parameters.MP_140_143},'150',@{pipeline().parameters.MP_271_274},'100','599',@{pipeline().parameters.MP_930_931}) AND IN(type_bureau,'AN','AL','NB'), '400',
		-- IN (v_symbol_pos_1_2,'CP','BC','BD','BG','BH','GG','NS','NA','NB','XX') AND IN(major_peril,@{pipeline().parameters.MP_110_112},@{pipeline().parameters.MP_120_125},'100',@{pipeline().parameters.MP_271_274},'599') 
		-- AND IN(type_bureau,'AL') ,'420',
		-- 'N/A')
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		DECODE(
		    TRUE,
		    v_symbol_pos_1_2 IN ('HH','FP','FL') AND major_peril IN (@{pipeline().parameters.MP_220_230}) AND type_bureau = 'PF', '421',
		    v_symbol_pos_1_2 IN ('BA','BB','BC','BD','BG','BH','CP','FL','FP','NA','NB','NS','BO') AND major_peril = '050' AND type_bureau IN ('MS','NB'), '421',
		    v_symbol_pos_1_2 IN ('CP','BC','BD','BO','BG','BH','NS','NA','NB','CM') AND major_peril IN ('425','426','435','455','480','599') AND type_bureau IN ('CF','GS'), '421',
		    v_symbol_pos_1_2 IN ('HH','PP','PA','PM','PS','PT','HA','XX') AND major_peril IN (@{pipeline().parameters.MP_101_103},@{pipeline().parameters.MP_114_119},'130',@{pipeline().parameters.MP_140_143},'100') AND type_bureau IN ('RL','RN'), '300',
		    v_symbol_pos_1_2 IN ('HH','PP','PA','PM','PS','PT','HA','XX') AND major_peril IN (@{pipeline().parameters.MP_110_112},@{pipeline().parameters.MP_120_121},'100') AND type_bureau = 'RL', '320',
		    v_symbol_pos_1_2 IN ('CP','BC','BD','BG','BH','GG','NS','NA','NB','XX') AND major_peril IN (@{pipeline().parameters.MP_101_103},@{pipeline().parameters.MP_114_119},'130',@{pipeline().parameters.MP_140_143},'150',@{pipeline().parameters.MP_271_274},'100','599',@{pipeline().parameters.MP_930_931}) AND type_bureau IN ('AN','AL','NB'), '400',
		    v_symbol_pos_1_2 IN ('CP','BC','BD','BG','BH','GG','NS','NA','NB','XX') AND major_peril IN (@{pipeline().parameters.MP_110_112},@{pipeline().parameters.MP_120_125},'100',@{pipeline().parameters.MP_271_274},'599') AND type_bureau IN ('AL'), '420',
		    'N/A'
		) AS v_Coverage_Code_3_or_NonsSubASLcode,
		v_Coverage_Code_3_or_NonsSubASLcode AS Nonsubaslcode,
		-- *INF*: DECODE(TRUE,
		-- IN(v_symbol_pos_1_2,'HH','HX','PX','XA','XX') AND IN(major_peril,'081','280',@{pipeline().parameters.MP_210_211},@{pipeline().parameters.MP_249_250},@{pipeline().parameters.MP_220_230},'002','097','911','914','042','062','200','201','206',@{pipeline().parameters.MP_260_261},'017','150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},'168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178},'044','010') AND  IN(type_bureau,'PF','PH','PI','PQ','PL','GL','RL','RP','RN'), '20',
		-- v_symbol_pos_1_2 = 'PP' AND IN(major_peril,'150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},'168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178}) AND IN(type_bureau,'RL','RP','RN'), '40',
		-- v_symbol_pos_1_2 = 'PA' AND IN(major_peril,'150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},'168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178}) AND IN(type_bureau,'RL','RP','RN'), '60',
		-- IN(v_symbol_pos_1_2,'HB','HX') AND IN(major_peril,@{pipeline().parameters.MP_220_230},'002','042','044','062','200','201','206',@{pipeline().parameters.MP_260_261}) AND IN(type_bureau,'NB','PH','PI','PQ','PL'), '80',
		-- v_symbol_pos_1_2 = 'HA' AND IN(major_peril,@{pipeline().parameters.MP_220_230},'002','042','044','062','200','201','206',@{pipeline().parameters.MP_260_261}) AND IN(type_bureau,'NB','PH','PI','PQ','PL'), '100',
		-- IN (v_symbol_pos_1_2,'FP','FL') AND IN (major_peril,@{pipeline().parameters.MP_210_211},'081',@{pipeline().parameters.MP_249_250},@{pipeline().parameters.MP_220_230},@{pipeline().parameters.MP_260_261}) AND IN(type_bureau,'NB','PF','PQ'), '120',
		-- IN (v_symbol_pos_1_2,'IP') AND IN(type_bureau,'PI','PL'),'140',
		-- IN (v_symbol_pos_1_2,'PM') AND IN (major_peril,'150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},'168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178})AND IN(type_bureau,'RL','RP','RN'),'160',
		-- IN (v_symbol_pos_1_2,'IB') AND IN(type_bureau,'PI','PL'),'180',
		-- IN (v_symbol_pos_1_2,'PS') AND IN (major_peril,'150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},'168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178})AND IN(type_bureau,'RL','RP','RN'),'200',
		-- IN (v_symbol_pos_1_2,'PT') AND IN (major_peril,'150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},'168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178})AND IN(type_bureau,'RL','RP','RN'),'220',
		-- IN (v_symbol_pos_1_2,'BC','BD','CP','BG','BH','GG','XX') AND IN (major_peril,'150','599',@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},@{pipeline().parameters.MP_930_931},'132','147','177','178',@{pipeline().parameters.MP_145_146},@{pipeline().parameters.MP_148_160},@{pipeline().parameters.MP_163_166},@{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270}) AND IN(type_bureau,'AN','AL','NB','AP')AND NOT IN(SubLine,'641','643','645','648'),'240',
		-- IN (v_symbol_pos_1_2,'CP') AND IN (major_peril,'599',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_930_931},'132','177','178',@{pipeline().parameters.MP_145_159},@{pipeline().parameters.MP_165_166},@{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270})AND IN(type_bureau,'AN','AL','NB','AP') AND IN(SubLine,'641','643','645','648'),'260',
		-- (IN (SUBSTR(v_symbol_pos_1_2,1,1),'V','W','Y') OR v_symbol_pos_1_2='XX' ) AND  IN(type_bureau,'WC','WP'),'280',
		-- IN (v_symbol_pos_1_2,'CP') AND NOT IN(class_of_business,'I','O') AND IN(major_peril,'415','463','490','496','498','599','919','425','426','435','455','480') AND IN(type_bureau,'CF','NB','GS'),'300',
		-- IN(v_symbol_pos_1_2,'CP')AND class_of_business = 'I'AND major_peril='599' AND type_bureau='GL' AND SubLine='336' AND Class_Code='22222','320',
		-- IN (v_symbol_pos_1_2,'CP','FF') AND class_of_business = 'I' AND IN(major_peril,'415','463','490','496','498','599','919','425','426','435','455','480','551','530','540','550','566','016','565') 
		-- AND IN(type_bureau,'GL') AND IN(SubLine,@{pipeline().parameters.SUB_325_335},@{pipeline().parameters.SUB_342_350},'336','365') AND NOT IN(Class_Code,'99999','22222','22250'),'320',
		-- IN (v_symbol_pos_1_2,'CP','FF') AND class_of_business = 'I' AND IN(major_peril,'415','463','490','496','498','599','919','425','426','435','455','480','551','530','540','550','566','016','565') 
		-- AND IN(type_bureau,'CF','NB','GS','IM','CM','FT','CR','BT'),'320',
		-- IN (v_symbol_pos_1_2,'CP','FF') AND class_of_business = 'O' AND IN(major_peril,'415','463','490','496','498','599','919','425','426','435','455','480','551','530','540','550','566','016','565') 
		-- AND IN(type_bureau,'GL') AND IN(SubLine,@{pipeline().parameters.SUB_325_335},@{pipeline().parameters.SUB_342_350},'336','365') AND NOT IN(Class_Code,'99999','22222','22250'),'340',
		-- IN (v_symbol_pos_1_2,'CP','FF') AND class_of_business = 'O' AND IN(major_peril,'415','463','490','496','498','599','919','425','426','435','455','480','551','530','540','550','566','016','565') 
		-- AND IN(type_bureau,'CF','NB','GS','IM','CM','FT','CR','BT'),'340',
		-- IN (v_symbol_pos_1_2,'CP') AND NOT IN(class_of_business,'I','O') AND IN(major_peril,'530','599','919','550','540') AND type_bureau = 'GL' AND IN(SubLine,@{pipeline().parameters.SUB_325_335},@{pipeline().parameters.SUB_342_350},'336','365') AND NOT IN(Class_Code,'22222','22250'),'360',
		-- IN (v_symbol_pos_1_2,'CP') AND NOT IN(class_of_business,'I','O') AND major_peril = '599' AND type_bureau = 'GL' AND IN(Class_Code,'22222','22250'),'360',
		-- v_symbol_pos_1_2 = 'XX' AND IN(major_peril,'084','085') AND type_bureau = 'GL', '360',
		-- IN (v_symbol_pos_1_2,'CP','FF') AND NOT IN(class_of_business,'I','O') AND IN(major_peril,'566','016','565','599') AND IN(type_bureau,'FT','BT','CR'),'380',
		-- IN (v_symbol_pos_1_2,'CP') AND NOT IN(class_of_business,'I','O')  AND IN(major_peril,'551','599','919') AND type_bureau = 'IM', '400',
		-- IN (v_symbol_pos_1_2,'BA','BB','XX') AND IN(major_peril,@{pipeline().parameters.MP_901_904},'905','908','919','599','907','919') AND IN(type_bureau,'BB','BC','BE','NB'), '420',
		-- IN (v_symbol_pos_1_2,'BC','BD') AND IN(major_peril,'415','463','490','496','498','599','919','425','426','435','455','480','551','530','566','565') AND IN(type_bureau,'CF','GS','IM','GL','FT','BT'), '440',
		-- IN (v_symbol_pos_1_2,'BO') AND IN(major_peril,'016','336','365','415','463','490','496','498','599','919','425','426','435','455','480','550','551','530','566','565','540') 
		-- AND IN(type_bureau,'GL') AND IN(SubLine,'334','336'),'450',
		-- IN (v_symbol_pos_1_2,'BO') AND IN(major_peril,'016','336','365','415','463','490','496','498','599','919','425','426','435','455','480','550','551','530','566','565','540') 
		-- AND IN(type_bureau,'CR','CF','IM','FT','BT'),'450',
		-- IN (v_symbol_pos_1_2,'BG','BH') AND  IN(major_peril,'415','463','490','496','498','599','919','425','426','435','455','480','551','530','566','565','907','269',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_930_931},@{pipeline().parameters.MP_901_904},@{pipeline().parameters.MP_145_160},@{pipeline().parameters.MP_165_166},@{pipeline().parameters.MP_170_173}) AND IN(type_bureau,'CF','GS','IM','GL','AN','AL','NB','BE','AP','FT','BT','BC'),'460',
		-- v_symbol_pos_1_2 = 'UP' AND Major_Peril = '017' AND Type_Bureau='GL', '480',
		-- IN (v_symbol_pos_1_2,'CP','UC','CU') AND  Major_Peril = '517' AND Type_Bureau='GL', '500',
		-- IN (v_symbol_pos_1_2,'BG','BH','CP') AND  IN(major_peril,'540') AND Type_Bureau='AL' AND IN(Risk_Unit_Group,'417','418'),'520',
		-- IN(major_peril,'540') AND Type_Bureau='BE' AND IN(Risk_Unit_Group,'366','367'),'520',
		-- IN (v_symbol_pos_1_2,'BC','BD','BG','BH','CP') AND  IN(major_peril,'540') AND Type_Bureau='GL' AND IN(Class_Code,'22222','22250') AND IN(Risk_Unit_Group,'366','367','340'),'520',
		-- IN (v_symbol_pos_1_2,'CD','CM') AND  IN(major_peril,'540','599','919') AND Type_Bureau='GL'  AND IN(SubLine,'345','334'), '530',
		-- IN (v_symbol_pos_1_2,'BA','BB','BC','BD','BG','BH','BO','CP') AND  IN(major_peril,'570','906') AND IN(Type_Bureau,'CF','BE','BM') ,'540',
		-- IN (v_symbol_pos_1_2,'HA','HB','HH','CP','BA','BB','BC','BD','BG','BH','BO','FL','FP') AND major_peril = '050' AND IN(Type_Bureau,'MS','NB'),'560',
		-- PolicySymbol ='ZZZ','580',
		-- IN (v_symbol_pos_1_2,'NA','NB','NS') AND IN(major_peril,'150','599',@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},@{pipeline().parameters.MP_930_931},'132','147','177','178',@{pipeline().parameters.MP_145_146},@{pipeline().parameters.MP_148_160},@{pipeline().parameters.MP_163_166},@{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270}) AND IN(Type_Bureau,'AN','AL','NB','AP') AND NOT IN (SubLine,'641','643','645','648'),'600',
		-- IN (v_symbol_pos_1_2,'NS') AND IN(major_peril,'599',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_930_931},'132','177','178',@{pipeline().parameters.MP_145_159},@{pipeline().parameters.MP_165_166},@{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270})
		-- AND  IN(Type_Bureau,'AN','AL','NB','AP') AND IN(SubLine,'641','643','645','648'), '620',
		-- IN (SUBSTR(v_symbol_pos_1_2,1,1),'R','S','T') AND  IN(type_bureau,'WC','WP'),'640',
		-- IN (v_symbol_pos_1_2,'NS') AND IN(major_peril,'415','463','490','496','498','599','919','425','426','435','455','480') AND IN(Type_Bureau,'CF','NB','GS'), '660',
		-- IN (v_symbol_pos_1_2,'NS','NE') AND NOT IN(class_of_business,'I','O')  AND IN(major_peril,'530','919','540','599') AND IN(type_bureau,'GL') AND IN(SubLine,@{pipeline().parameters.SUB_325_335},@{pipeline().parameters.SUB_342_350},'336'), '680',
		-- IN (v_symbol_pos_1_2,'NS') AND IN(major_peril,'566','016','565','599') AND IN(Type_Bureau,'FT','BT','CR'), '700',
		-- IN (v_symbol_pos_1_2,'NS') AND IN(major_peril,'551','919','599') AND IN(Type_Bureau,'IM'), '720',
		-- IN (v_symbol_pos_1_2,'NA','NB') AND IN(major_peril,'415','463','490','496','498','599','919','425','426','435','455','480','551','530','566','565') AND IN(Type_Bureau,'GS','IM','GL','FT','BT','CF'), '740',
		-- v_symbol_pos_1_2 = 'NU' AND  major_peril = '517' AND Type_Bureau = 'GL', '760',
		-- v_symbol_pos_1_2 = 'NF' AND  IN(major_peril,'566','599','565'), '780',
		-- IN(v_symbol_pos_1_2,'NC','NJ','NL','NO','NM') , '800',
		-- v_symbol_pos_1_2 = 'NE' AND SubLine = '360', '820',
		-- IN (v_symbol_pos_1_2,'NA','NB','NS') AND Major_Peril ='540' AND Type_Bureau = 'GL' AND IN(Class_Code,'22222','22250') AND IN(Risk_Unit_Group,'366','367','340'),'820',
		-- IN(v_symbol_pos_1_2,'NK','NN'), '840',
		-- IN (v_symbol_pos_1_2,'NA','NB','NS') AND IN(Major_Peril,'570','906') AND IN(Type_Bureau,'CF','BE','BM'), '860',
		-- IN (v_symbol_pos_1_2,'NA','NB','NS') AND Major_Peril = '050' AND IN(Type_Bureau,'MS','NB'), '880',
		-- IN (SUBSTR(v_symbol_pos_1_2,1,1),'A','J','L') AND IN(type_bureau,'WC','WP'),'950',
		-- '999')
		-- 
		-- 
		DECODE(
		    TRUE,
		    v_symbol_pos_1_2 IN ('HH','HX','PX','XA','XX') AND major_peril IN ('081','280',@{pipeline().parameters.MP_210_211},@{pipeline().parameters.MP_249_250},@{pipeline().parameters.MP_220_230},'002','097','911','914','042','062','200','201','206',@{pipeline().parameters.MP_260_261},'017','150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},'168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178},'044','010') AND type_bureau IN ('PF','PH','PI','PQ','PL','GL','RL','RP','RN'), '20',
		    v_symbol_pos_1_2 = 'PP' AND major_peril IN ('150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},'168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178}) AND type_bureau IN ('RL','RP','RN'), '40',
		    v_symbol_pos_1_2 = 'PA' AND major_peril IN ('150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},'168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178}) AND type_bureau IN ('RL','RP','RN'), '60',
		    v_symbol_pos_1_2 IN ('HB','HX') AND major_peril IN (@{pipeline().parameters.MP_220_230},'002','042','044','062','200','201','206',@{pipeline().parameters.MP_260_261}) AND type_bureau IN ('NB','PH','PI','PQ','PL'), '80',
		    v_symbol_pos_1_2 = 'HA' AND major_peril IN (@{pipeline().parameters.MP_220_230},'002','042','044','062','200','201','206',@{pipeline().parameters.MP_260_261}) AND type_bureau IN ('NB','PH','PI','PQ','PL'), '100',
		    v_symbol_pos_1_2 IN ('FP','FL') AND major_peril IN (@{pipeline().parameters.MP_210_211},'081',@{pipeline().parameters.MP_249_250},@{pipeline().parameters.MP_220_230},@{pipeline().parameters.MP_260_261}) AND type_bureau IN ('NB','PF','PQ'), '120',
		    v_symbol_pos_1_2 IN ('IP') AND type_bureau IN ('PI','PL'), '140',
		    v_symbol_pos_1_2 IN ('PM') AND major_peril IN ('150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},'168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178}) AND type_bureau IN ('RL','RP','RN'), '160',
		    v_symbol_pos_1_2 IN ('IB') AND type_bureau IN ('PI','PL'), '180',
		    v_symbol_pos_1_2 IN ('PS') AND major_peril IN ('150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},'168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178}) AND type_bureau IN ('RL','RP','RN'), '200',
		    v_symbol_pos_1_2 IN ('PT') AND major_peril IN ('150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},'168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178}) AND type_bureau IN ('RL','RP','RN'), '220',
		    v_symbol_pos_1_2 IN ('BC','BD','CP','BG','BH','GG','XX') AND major_peril IN ('150','599',@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},@{pipeline().parameters.MP_930_931},'132','147','177','178',@{pipeline().parameters.MP_145_146},@{pipeline().parameters.MP_148_160},@{pipeline().parameters.MP_163_166},@{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270}) AND type_bureau IN ('AN','AL','NB','AP') AND NOT SubLine IN ('641','643','645','648'), '240',
		    v_symbol_pos_1_2 IN ('CP') AND major_peril IN ('599',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_930_931},'132','177','178',@{pipeline().parameters.MP_145_159},@{pipeline().parameters.MP_165_166},@{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270}) AND type_bureau IN ('AN','AL','NB','AP') AND SubLine IN ('641','643','645','648'), '260',
		    (SUBSTR(v_symbol_pos_1_2, 1, 1) IN ('V','W','Y') OR v_symbol_pos_1_2 = 'XX') AND type_bureau IN ('WC','WP'), '280',
		    v_symbol_pos_1_2 IN ('CP') AND NOT class_of_business IN ('I','O') AND major_peril IN ('415','463','490','496','498','599','919','425','426','435','455','480') AND type_bureau IN ('CF','NB','GS'), '300',
		    v_symbol_pos_1_2 IN ('CP') AND class_of_business = 'I' AND major_peril = '599' AND type_bureau = 'GL' AND SubLine = '336' AND Class_Code = '22222', '320',
		    v_symbol_pos_1_2 IN ('CP','FF') AND class_of_business = 'I' AND major_peril IN ('415','463','490','496','498','599','919','425','426','435','455','480','551','530','540','550','566','016','565') AND type_bureau IN ('GL') AND SubLine IN (@{pipeline().parameters.SUB_325_335},@{pipeline().parameters.SUB_342_350},'336','365') AND NOT Class_Code IN ('99999','22222','22250'), '320',
		    v_symbol_pos_1_2 IN ('CP','FF') AND class_of_business = 'I' AND major_peril IN ('415','463','490','496','498','599','919','425','426','435','455','480','551','530','540','550','566','016','565') AND type_bureau IN ('CF','NB','GS','IM','CM','FT','CR','BT'), '320',
		    v_symbol_pos_1_2 IN ('CP','FF') AND class_of_business = 'O' AND major_peril IN ('415','463','490','496','498','599','919','425','426','435','455','480','551','530','540','550','566','016','565') AND type_bureau IN ('GL') AND SubLine IN (@{pipeline().parameters.SUB_325_335},@{pipeline().parameters.SUB_342_350},'336','365') AND NOT Class_Code IN ('99999','22222','22250'), '340',
		    v_symbol_pos_1_2 IN ('CP','FF') AND class_of_business = 'O' AND major_peril IN ('415','463','490','496','498','599','919','425','426','435','455','480','551','530','540','550','566','016','565') AND type_bureau IN ('CF','NB','GS','IM','CM','FT','CR','BT'), '340',
		    v_symbol_pos_1_2 IN ('CP') AND NOT class_of_business IN ('I','O') AND major_peril IN ('530','599','919','550','540') AND type_bureau = 'GL' AND SubLine IN (@{pipeline().parameters.SUB_325_335},@{pipeline().parameters.SUB_342_350},'336','365') AND NOT Class_Code IN ('22222','22250'), '360',
		    v_symbol_pos_1_2 IN ('CP') AND NOT class_of_business IN ('I','O') AND major_peril = '599' AND type_bureau = 'GL' AND Class_Code IN ('22222','22250'), '360',
		    v_symbol_pos_1_2 = 'XX' AND major_peril IN ('084','085') AND type_bureau = 'GL', '360',
		    v_symbol_pos_1_2 IN ('CP','FF') AND NOT class_of_business IN ('I','O') AND major_peril IN ('566','016','565','599') AND type_bureau IN ('FT','BT','CR'), '380',
		    v_symbol_pos_1_2 IN ('CP') AND NOT class_of_business IN ('I','O') AND major_peril IN ('551','599','919') AND type_bureau = 'IM', '400',
		    v_symbol_pos_1_2 IN ('BA','BB','XX') AND major_peril IN (@{pipeline().parameters.MP_901_904},'905','908','919','599','907','919') AND type_bureau IN ('BB','BC','BE','NB'), '420',
		    v_symbol_pos_1_2 IN ('BC','BD') AND major_peril IN ('415','463','490','496','498','599','919','425','426','435','455','480','551','530','566','565') AND type_bureau IN ('CF','GS','IM','GL','FT','BT'), '440',
		    v_symbol_pos_1_2 IN ('BO') AND major_peril IN ('016','336','365','415','463','490','496','498','599','919','425','426','435','455','480','550','551','530','566','565','540') AND type_bureau IN ('GL') AND SubLine IN ('334','336'), '450',
		    v_symbol_pos_1_2 IN ('BO') AND major_peril IN ('016','336','365','415','463','490','496','498','599','919','425','426','435','455','480','550','551','530','566','565','540') AND type_bureau IN ('CR','CF','IM','FT','BT'), '450',
		    v_symbol_pos_1_2 IN ('BG','BH') AND major_peril IN ('415','463','490','496','498','599','919','425','426','435','455','480','551','530','566','565','907','269',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_930_931},@{pipeline().parameters.MP_901_904},@{pipeline().parameters.MP_145_160},@{pipeline().parameters.MP_165_166},@{pipeline().parameters.MP_170_173}) AND type_bureau IN ('CF','GS','IM','GL','AN','AL','NB','BE','AP','FT','BT','BC'), '460',
		    v_symbol_pos_1_2 = 'UP' AND Major_Peril = '017' AND Type_Bureau = 'GL', '480',
		    v_symbol_pos_1_2 IN ('CP','UC','CU') AND Major_Peril = '517' AND Type_Bureau = 'GL', '500',
		    v_symbol_pos_1_2 IN ('BG','BH','CP') AND major_peril IN ('540') AND Type_Bureau = 'AL' AND Risk_Unit_Group IN ('417','418'), '520',
		    major_peril IN ('540') AND Type_Bureau = 'BE' AND Risk_Unit_Group IN ('366','367'), '520',
		    v_symbol_pos_1_2 IN ('BC','BD','BG','BH','CP') AND major_peril IN ('540') AND Type_Bureau = 'GL' AND Class_Code IN ('22222','22250') AND Risk_Unit_Group IN ('366','367','340'), '520',
		    v_symbol_pos_1_2 IN ('CD','CM') AND major_peril IN ('540','599','919') AND Type_Bureau = 'GL' AND SubLine IN ('345','334'), '530',
		    v_symbol_pos_1_2 IN ('BA','BB','BC','BD','BG','BH','BO','CP') AND major_peril IN ('570','906') AND Type_Bureau IN ('CF','BE','BM'), '540',
		    v_symbol_pos_1_2 IN ('HA','HB','HH','CP','BA','BB','BC','BD','BG','BH','BO','FL','FP') AND major_peril = '050' AND Type_Bureau IN ('MS','NB'), '560',
		    PolicySymbol = 'ZZZ', '580',
		    v_symbol_pos_1_2 IN ('NA','NB','NS') AND major_peril IN ('150','599',@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},@{pipeline().parameters.MP_930_931},'132','147','177','178',@{pipeline().parameters.MP_145_146},@{pipeline().parameters.MP_148_160},@{pipeline().parameters.MP_163_166},@{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270}) AND Type_Bureau IN ('AN','AL','NB','AP') AND NOT SubLine IN ('641','643','645','648'), '600',
		    v_symbol_pos_1_2 IN ('NS') AND major_peril IN ('599',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_930_931},'132','177','178',@{pipeline().parameters.MP_145_159},@{pipeline().parameters.MP_165_166},@{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270}) AND Type_Bureau IN ('AN','AL','NB','AP') AND SubLine IN ('641','643','645','648'), '620',
		    SUBSTR(v_symbol_pos_1_2, 1, 1) IN ('R','S','T') AND type_bureau IN ('WC','WP'), '640',
		    v_symbol_pos_1_2 IN ('NS') AND major_peril IN ('415','463','490','496','498','599','919','425','426','435','455','480') AND Type_Bureau IN ('CF','NB','GS'), '660',
		    v_symbol_pos_1_2 IN ('NS','NE') AND NOT class_of_business IN ('I','O') AND major_peril IN ('530','919','540','599') AND type_bureau IN ('GL') AND SubLine IN (@{pipeline().parameters.SUB_325_335},@{pipeline().parameters.SUB_342_350},'336'), '680',
		    v_symbol_pos_1_2 IN ('NS') AND major_peril IN ('566','016','565','599') AND Type_Bureau IN ('FT','BT','CR'), '700',
		    v_symbol_pos_1_2 IN ('NS') AND major_peril IN ('551','919','599') AND Type_Bureau IN ('IM'), '720',
		    v_symbol_pos_1_2 IN ('NA','NB') AND major_peril IN ('415','463','490','496','498','599','919','425','426','435','455','480','551','530','566','565') AND Type_Bureau IN ('GS','IM','GL','FT','BT','CF'), '740',
		    v_symbol_pos_1_2 = 'NU' AND major_peril = '517' AND Type_Bureau = 'GL', '760',
		    v_symbol_pos_1_2 = 'NF' AND major_peril IN ('566','599','565'), '780',
		    v_symbol_pos_1_2 IN ('NC','NJ','NL','NO','NM'), '800',
		    v_symbol_pos_1_2 = 'NE' AND SubLine = '360', '820',
		    v_symbol_pos_1_2 IN ('NA','NB','NS') AND Major_Peril = '540' AND Type_Bureau = 'GL' AND Class_Code IN ('22222','22250') AND Risk_Unit_Group IN ('366','367','340'), '820',
		    v_symbol_pos_1_2 IN ('NK','NN'), '840',
		    v_symbol_pos_1_2 IN ('NA','NB','NS') AND Major_Peril IN ('570','906') AND Type_Bureau IN ('CF','BE','BM'), '860',
		    v_symbol_pos_1_2 IN ('NA','NB','NS') AND Major_Peril = '050' AND Type_Bureau IN ('MS','NB'), '880',
		    SUBSTR(v_symbol_pos_1_2, 1, 1) IN ('A','J','L') AND type_bureau IN ('WC','WP'), '950',
		    '999'
		) AS v_ASLProduct_Code,
		v_ASLProduct_Code AS ASLProduct_Code,
		-- *INF*: DECODE(TRUE,
		-- IN(v_symbol_pos_1_2,'CP','NS') AND Insurance_Line='GL' AND IN(Major_Peril,'530','599') AND RTRIM(Class_Code)='99999' AND IN(SubLine,'334','336'),'320',
		-- 
		-- IN(v_symbol_pos_1_2,'CP','NS') AND Line_of_Business = 'CPP' AND Type_Bureau='CR','520',
		-- 
		-- IN(v_symbol_pos_1_2,'CP','NS') AND Type_Bureau='IM','550',
		-- 
		-- v_symbol_pos_1_2='CP' AND Insurance_Line='GL' AND SubLine='365','380',
		-- 
		-- IN(v_symbol_pos_1_2, 'CP','NS') AND Insurance_Line='GL' AND IN(Major_Peril,'599','919') AND IN(Risk_Unit_Group,'345','367'),'300',
		-- 
		-- IN(v_symbol_pos_1_2, 'CP','NS') AND Insurance_Line='GL' AND IN(Major_Peril,'530','540','919','599') AND RTRIM(Class_Code) <>'99999' AND NOT IN(Risk_Unit_Group,'345','346','355','900','901','367','286','365'),'300',
		-- 
		-- IN(v_symbol_pos_1_2,'CF','CP','NS') AND IN(Insurance_Line,'BM','CF','CG','CR','GS','N/A') AND NOT IN(Type_Bureau,'AL','AP','AN','GL','IM'),'500',
		-- 
		-- IN(v_symbol_pos_1_2,'BC','BD','BG','BH','CA','CP','NB','NS','NA','XX') AND IN(Insurance_Line,'N/A','CA')  AND IN(Type_Bureau,'AL','AP','AN'),'200',
		-- 
		-- IN(v_symbol_pos_1_2,'CP','NS') AND Insurance_Line='GL' AND Risk_Unit_Group='355','370',
		-- 
		-- IN(v_symbol_pos_1_2,'BA','BB','XX') AND IN(Line_of_Business,'BOP','BO') AND NOT IN(Insurance_Line,'CA'),'400',
		-- 
		-- v_symbol_pos_1_2='CM' AND Insurance_Line='GL' AND IN(Risk_Unit_Group,'901','902','903'),'360',
		-- 
		-- IN(v_symbol_pos_1_2,'CP','NS') AND Insurance_Line='GL'  AND Risk_Unit_Group='345','365',
		-- 
		-- IN(v_symbol_pos_1_2,'CU','NU','CP','UC') AND Type_Bureau='GL' AND IN(Major_Peril,'517'),'900',
		-- 
		-- IN(v_symbol_pos_1_2,'BC','BD') AND IN(Insurance_Line,'CF','GL','CR','IM','CG','N/A'),'410',
		-- 
		-- v_symbol_pos_1_2='CP' AND Insurance_Line='GL'  AND Risk_Unit_Group='346','321',
		-- 
		-- IN(v_symbol_pos_1_2,'NA','NB') AND IN(Insurance_Line,'CF','GL','CR','IM','CG'),'430',
		-- 
		-- IN(v_symbol_pos_1_2,'BG','BH','GG') AND IN(Insurance_Line,'CF','GL','CR','IM','GA','CG','N/A'),'420',
		-- 
		-- v_symbol_pos_1_2='NF' AND IN(class_of_business,'XN','XO','XP','XQ','9'),'620',
		-- 
		-- IN(v_symbol_pos_1_2,'CD','CM') AND IN(Risk_Unit_Group,'367','900'),'350',
		-- 
		-- IN(v_symbol_pos_1_2,'BA','BB') AND Insurance_Line='GL' AND IN(Risk_Unit_Group,'110','111'),'200',
		-- 
		-- IN(v_symbol_pos_1_2,'CP','NS') AND Insurance_Line='GA','340',
		-- 
		-- IN(v_symbol_pos_1_2,'HH','HA','HB','HX','IB','IP','PA','PX','XX') AND IN(Type_Bureau,'PH','PI','PL','PQ','MS'),'800',
		-- 
		-- ----v_symbol_pos_1_2='NF' AND class_of_business = '9','510',
		-- 
		-- ----IN(Line_of_Business,'APV','ASV','FP','HP','IMP'),'810',  'Personal Lines Monoline',
		-- 
		-- v_symbol_pos_1_2='BO','450',
		-- 
		-- IN(v_symbol_pos_1_2,'GL','XX') AND IN(Major_Peril,'084','085'),'300',
		-- 
		-- v_symbol_pos_1_2='NN','310',
		-- 
		-- v_symbol_pos_1_2='NK','311',
		-- 
		-- v_symbol_pos_1_2='NE','330',
		-- 
		-- Major_Peril='032','100',
		-- 
		-- v_symbol_pos_1_2='NC','610',
		-- 
		-- v_symbol_pos_1_2='NJ','630',
		-- 
		-- v_symbol_pos_1_2='NL','640',
		-- 
		-- v_symbol_pos_1_2='NM','650',
		-- 
		-- v_symbol_pos_1_2='NO','660',
		-- 
		-- v_symbol_pos_1_2='FF','510',
		-- 
		-- IN(v_symbol_pos_1_2,'FL','FP') AND IN(Type_Bureau,'PF','PQ','MS'),'820',
		-- 
		-- v_symbol_pos_1_2='HH' AND Type_Bureau='PF','820',
		-- 
		-- IN(v_symbol_pos_1_2,'HH','PA','PM','PP','PS','PT','HA','XX','XA') AND IN(Type_Bureau,'RL','RP','RN'),'850',
		-- 
		-- IN(v_symbol_pos_1_2,'HH','UP','HX','XX') AND Type_Bureau ='GL' AND Major_Peril='017','890',
		-- 
		-- '000')
		DECODE(
		    TRUE,
		    v_symbol_pos_1_2 IN ('CP','NS') AND Insurance_Line = 'GL' AND Major_Peril IN ('530','599') AND RTRIM(Class_Code) = '99999' AND SubLine IN ('334','336'), '320',
		    v_symbol_pos_1_2 IN ('CP','NS') AND Line_of_Business = 'CPP' AND Type_Bureau = 'CR', '520',
		    v_symbol_pos_1_2 IN ('CP','NS') AND Type_Bureau = 'IM', '550',
		    v_symbol_pos_1_2 = 'CP' AND Insurance_Line = 'GL' AND SubLine = '365', '380',
		    v_symbol_pos_1_2 IN ('CP','NS') AND Insurance_Line = 'GL' AND Major_Peril IN ('599','919') AND Risk_Unit_Group IN ('345','367'), '300',
		    v_symbol_pos_1_2 IN ('CP','NS') AND Insurance_Line = 'GL' AND Major_Peril IN ('530','540','919','599') AND RTRIM(Class_Code) <> '99999' AND NOT Risk_Unit_Group IN ('345','346','355','900','901','367','286','365'), '300',
		    v_symbol_pos_1_2 IN ('CF','CP','NS') AND Insurance_Line IN ('BM','CF','CG','CR','GS','N/A') AND NOT Type_Bureau IN ('AL','AP','AN','GL','IM'), '500',
		    v_symbol_pos_1_2 IN ('BC','BD','BG','BH','CA','CP','NB','NS','NA','XX') AND Insurance_Line IN ('N/A','CA') AND Type_Bureau IN ('AL','AP','AN'), '200',
		    v_symbol_pos_1_2 IN ('CP','NS') AND Insurance_Line = 'GL' AND Risk_Unit_Group = '355', '370',
		    v_symbol_pos_1_2 IN ('BA','BB','XX') AND Line_of_Business IN ('BOP','BO') AND NOT Insurance_Line IN ('CA'), '400',
		    v_symbol_pos_1_2 = 'CM' AND Insurance_Line = 'GL' AND Risk_Unit_Group IN ('901','902','903'), '360',
		    v_symbol_pos_1_2 IN ('CP','NS') AND Insurance_Line = 'GL' AND Risk_Unit_Group = '345', '365',
		    v_symbol_pos_1_2 IN ('CU','NU','CP','UC') AND Type_Bureau = 'GL' AND Major_Peril IN ('517'), '900',
		    v_symbol_pos_1_2 IN ('BC','BD') AND Insurance_Line IN ('CF','GL','CR','IM','CG','N/A'), '410',
		    v_symbol_pos_1_2 = 'CP' AND Insurance_Line = 'GL' AND Risk_Unit_Group = '346', '321',
		    v_symbol_pos_1_2 IN ('NA','NB') AND Insurance_Line IN ('CF','GL','CR','IM','CG'), '430',
		    v_symbol_pos_1_2 IN ('BG','BH','GG') AND Insurance_Line IN ('CF','GL','CR','IM','GA','CG','N/A'), '420',
		    v_symbol_pos_1_2 = 'NF' AND class_of_business IN ('XN','XO','XP','XQ','9'), '620',
		    v_symbol_pos_1_2 IN ('CD','CM') AND Risk_Unit_Group IN ('367','900'), '350',
		    v_symbol_pos_1_2 IN ('BA','BB') AND Insurance_Line = 'GL' AND Risk_Unit_Group IN ('110','111'), '200',
		    v_symbol_pos_1_2 IN ('CP','NS') AND Insurance_Line = 'GA', '340',
		    v_symbol_pos_1_2 IN ('HH','HA','HB','HX','IB','IP','PA','PX','XX') AND Type_Bureau IN ('PH','PI','PL','PQ','MS'), '800',
		    v_symbol_pos_1_2 = 'BO', '450',
		    v_symbol_pos_1_2 IN ('GL','XX') AND Major_Peril IN ('084','085'), '300',
		    v_symbol_pos_1_2 = 'NN', '310',
		    v_symbol_pos_1_2 = 'NK', '311',
		    v_symbol_pos_1_2 = 'NE', '330',
		    Major_Peril = '032', '100',
		    v_symbol_pos_1_2 = 'NC', '610',
		    v_symbol_pos_1_2 = 'NJ', '630',
		    v_symbol_pos_1_2 = 'NL', '640',
		    v_symbol_pos_1_2 = 'NM', '650',
		    v_symbol_pos_1_2 = 'NO', '660',
		    v_symbol_pos_1_2 = 'FF', '510',
		    v_symbol_pos_1_2 IN ('FL','FP') AND Type_Bureau IN ('PF','PQ','MS'), '820',
		    v_symbol_pos_1_2 = 'HH' AND Type_Bureau = 'PF', '820',
		    v_symbol_pos_1_2 IN ('HH','PA','PM','PP','PS','PT','HA','XX','XA') AND Type_Bureau IN ('RL','RP','RN'), '850',
		    v_symbol_pos_1_2 IN ('HH','UP','HX','XX') AND Type_Bureau = 'GL' AND Major_Peril = '017', '890',
		    '000'
		) AS v_Hierarchy_Product_Code,
		v_Hierarchy_Product_Code AS Hierarchy_Product_Code,
		-- *INF*: DECODE(TRUE,
		-- IN(v_symbol_pos_1_2,'BA','BB') and Type_Bureau = 'BE' and Major_Peril = '540' and  IN(Risk_Unit_Group,'366','367'),'330',
		-- IN(v_symbol_pos_1_2, 'BC','BD','BO','CP','NA','NB','NS') and Insurance_Line='GL' and Major_Peril <>'517' and NOT IN(RTRIM(Class_Code),'22222', '22250'),'300',
		-- IN(v_symbol_pos_1_2, 'BC','BD','BO','CP','NA','NB','NS') and Type_Bureau='GL' and Major_Peril <>'517' and IN(Class_Code, '22222', '22250'),'330',
		-- IN(v_symbol_pos_1_2,'BC','BD','BO','CP','NA','NB','NS') and IN(Type_Bureau,'CF','GS') and IN(Major_Peril,'415','463','490','496','498','599','919','425','426','435','455','480'),'500',
		-- v_symbol_pos_1_2='HH' and Type_Bureau='PL' and NOT IN(RTRIM(Special_Use),'H164','H828','H075','HOBM','HBBM','HOMT','HOPE','HOTR'),'830',
		-- IN(v_symbol_pos_1_2,'CU','NU','CP') and Type_Bureau='GL' and Major_Peril = '517','900',
		-- v_symbol_pos_1_2='HH'and IN(Type_Bureau,'RL','RP','RN') and RTRIM(Class_Code) <>'9','850',
		-- IN(v_symbol_pos_1_2,'BA','BB','BC','BD','BG','BH','BO','CP','NA','NB','NS') and Type_Bureau='NB' and Major_Peril = '050','590',
		-- v_symbol_pos_1_2='CM' and Type_Bureau='GL' and Risk_Unit_Group='900','310',
		-- IN(v_symbol_pos_1_2,'CP','NS') and Insurance_Line='GA' and  IN(Risk_Unit_Group,'417','418'),'330',
		-- v_symbol_pos_1_2='HH' and Type_Bureau='PI' and Major_Peril = '201','830',
		-- v_symbol_pos_1_2='HH' and Type_Bureau='GL' and Major_Peril = '017','890',
		-- v_symbol_pos_1_2='HH' and Type_Bureau='PQ' and IN(Major_Peril,'260','261'),'811',
		-- v_symbol_pos_1_2='HH' and Type_Bureau='MS' and Major_Peril = '050','812',
		-- IN(v_symbol_pos_1_2,'BC','BD','BG','BH','CA','CP','NB','NS','NA') and IN(Insurance_Line,'N/A','CA') and IN(Type_Bureau,'AL','AP','AN'),'200',
		-- IN(v_symbol_pos_1_2,'BA','BB') and Insurance_Line='GL' and IN(Risk_Unit_Group,'110','111'),'200',
		-- v_symbol_pos_1_2='CM' and Insurance_Line='GL' and IN(Risk_Unit_Group,'901','902','903'),'360',
		-- v_symbol_pos_1_2='HH' and SUBSTR(RiskUnit,1,1) = '1' and sar_code_2 = '3','803',
		-- v_symbol_pos_1_2='HH' and SUBSTR(RiskUnit,1,1) = '1' and sar_code_2 = '4','804',
		-- v_symbol_pos_1_2='HH' and SUBSTR(RiskUnit,1,1) = '1' and sar_code_2 = '6','806',
		-- IN(v_symbol_pos_1_2,'BA','BB') and IN(Major_Peril,'901','902','903','904'),'500',
		-- IN(v_symbol_pos_1_2,'BA','BB') and IN(Major_Peril,'901','902','903','904'),'300',
		-- IN(v_symbol_pos_1_2,'BC','BD','BG','BH','BO','CP','NA','NB','NS') and IN(Type_Bureau,'BT','CR','FT'),'520',
		-- IN(v_symbol_pos_1_2,'CP','NS') and Insurance_Line='GA','340',
		-- IN(v_symbol_pos_1_2,'BA','BB','BG') and Major_Peril = '908','520',
		-- v_symbol_pos_1_2='HH' and IN(RTRIM(Special_Use),'H164','H828'),'880',
		-- v_symbol_pos_1_2='HH' and IN(RTRIM(Special_Use),'H075','HOBM','HBBM','HOMT','HOPE','HOTR'),'870',
		-- v_symbol_pos_1_2='HH' and IN(RTRIM(Class_Code),'9221','9222','9223','9224','9225','9226','9231','9232','9233','9234','9235','9236','9520'),'860',
		-- v_symbol_pos_1_2='HH' and IN(RTRIM(Class_Code),'9620','9900'),'852',
		-- v_symbol_pos_1_2='HH' and IN(RTRIM(Class_Code),'9410','9442'),'856',
		-- v_symbol_pos_1_2='HH' and RTRIM(Class_Code)='9437','854',
		-- v_symbol_pos_1_2='HH' and Major_Peril ='097','813',
		-- v_symbol_pos_1_2='HH' and Type_Bureau ='PF','820',
		-- IN(Type_Bureau,'CF','BE','BM') and IN(Major_Peril,'570','906'),'530',
		-- v_symbol_pos_1_2='NF' and IN(class_of_business,'XN','XO','XP','XQ'),'640',
		-- v_symbol_pos_1_2='NF' and class_of_business = '9','520',
		-- v_symbol_pos_1_2='NK' and Type_Bureau = 'GL','310',
		-- v_symbol_pos_1_2='CD' and Type_Bureau = 'GL','310',
		-- v_symbol_pos_1_2='NK' and Type_Bureau = 'GL','330',
		-- IN(v_symbol_pos_1_2,'NC','NJ','NL','NO','NM'),'600',
		-- v_symbol_pos_1_2='NE','330',
		-- Type_Bureau='IM','550',
		-- Major_Peril='032','100')
		DECODE(
		    TRUE,
		    v_symbol_pos_1_2 IN ('BA','BB') and Type_Bureau = 'BE' and Major_Peril = '540' and Risk_Unit_Group IN ('366','367'), '330',
		    v_symbol_pos_1_2 IN ('BC','BD','BO','CP','NA','NB','NS') and Insurance_Line = 'GL' and Major_Peril <> '517' and NOT RTRIM(Class_Code) IN ('22222','22250'), '300',
		    v_symbol_pos_1_2 IN ('BC','BD','BO','CP','NA','NB','NS') and Type_Bureau = 'GL' and Major_Peril <> '517' and Class_Code IN ('22222','22250'), '330',
		    v_symbol_pos_1_2 IN ('BC','BD','BO','CP','NA','NB','NS') and Type_Bureau IN ('CF','GS') and Major_Peril IN ('415','463','490','496','498','599','919','425','426','435','455','480'), '500',
		    v_symbol_pos_1_2 = 'HH' and Type_Bureau = 'PL' and NOT RTRIM(Special_Use) IN ('H164','H828','H075','HOBM','HBBM','HOMT','HOPE','HOTR'), '830',
		    v_symbol_pos_1_2 IN ('CU','NU','CP') and Type_Bureau = 'GL' and Major_Peril = '517', '900',
		    v_symbol_pos_1_2 = 'HH' and Type_Bureau IN ('RL','RP','RN') and RTRIM(Class_Code) <> '9', '850',
		    v_symbol_pos_1_2 IN ('BA','BB','BC','BD','BG','BH','BO','CP','NA','NB','NS') and Type_Bureau = 'NB' and Major_Peril = '050', '590',
		    v_symbol_pos_1_2 = 'CM' and Type_Bureau = 'GL' and Risk_Unit_Group = '900', '310',
		    v_symbol_pos_1_2 IN ('CP','NS') and Insurance_Line = 'GA' and Risk_Unit_Group IN ('417','418'), '330',
		    v_symbol_pos_1_2 = 'HH' and Type_Bureau = 'PI' and Major_Peril = '201', '830',
		    v_symbol_pos_1_2 = 'HH' and Type_Bureau = 'GL' and Major_Peril = '017', '890',
		    v_symbol_pos_1_2 = 'HH' and Type_Bureau = 'PQ' and Major_Peril IN ('260','261'), '811',
		    v_symbol_pos_1_2 = 'HH' and Type_Bureau = 'MS' and Major_Peril = '050', '812',
		    v_symbol_pos_1_2 IN ('BC','BD','BG','BH','CA','CP','NB','NS','NA') and Insurance_Line IN ('N/A','CA') and Type_Bureau IN ('AL','AP','AN'), '200',
		    v_symbol_pos_1_2 IN ('BA','BB') and Insurance_Line = 'GL' and Risk_Unit_Group IN ('110','111'), '200',
		    v_symbol_pos_1_2 = 'CM' and Insurance_Line = 'GL' and Risk_Unit_Group IN ('901','902','903'), '360',
		    v_symbol_pos_1_2 = 'HH' and SUBSTR(RiskUnit, 1, 1) = '1' and sar_code_2 = '3', '803',
		    v_symbol_pos_1_2 = 'HH' and SUBSTR(RiskUnit, 1, 1) = '1' and sar_code_2 = '4', '804',
		    v_symbol_pos_1_2 = 'HH' and SUBSTR(RiskUnit, 1, 1) = '1' and sar_code_2 = '6', '806',
		    v_symbol_pos_1_2 IN ('BA','BB') and Major_Peril IN ('901','902','903','904'), '500',
		    v_symbol_pos_1_2 IN ('BA','BB') and Major_Peril IN ('901','902','903','904'), '300',
		    v_symbol_pos_1_2 IN ('BC','BD','BG','BH','BO','CP','NA','NB','NS') and Type_Bureau IN ('BT','CR','FT'), '520',
		    v_symbol_pos_1_2 IN ('CP','NS') and Insurance_Line = 'GA', '340',
		    v_symbol_pos_1_2 IN ('BA','BB','BG') and Major_Peril = '908', '520',
		    v_symbol_pos_1_2 = 'HH' and RTRIM(Special_Use) IN ('H164','H828'), '880',
		    v_symbol_pos_1_2 = 'HH' and RTRIM(Special_Use) IN ('H075','HOBM','HBBM','HOMT','HOPE','HOTR'), '870',
		    v_symbol_pos_1_2 = 'HH' and RTRIM(Class_Code) IN ('9221','9222','9223','9224','9225','9226','9231','9232','9233','9234','9235','9236','9520'), '860',
		    v_symbol_pos_1_2 = 'HH' and RTRIM(Class_Code) IN ('9620','9900'), '852',
		    v_symbol_pos_1_2 = 'HH' and RTRIM(Class_Code) IN ('9410','9442'), '856',
		    v_symbol_pos_1_2 = 'HH' and RTRIM(Class_Code) = '9437', '854',
		    v_symbol_pos_1_2 = 'HH' and Major_Peril = '097', '813',
		    v_symbol_pos_1_2 = 'HH' and Type_Bureau = 'PF', '820',
		    Type_Bureau IN ('CF','BE','BM') and Major_Peril IN ('570','906'), '530',
		    v_symbol_pos_1_2 = 'NF' and class_of_business IN ('XN','XO','XP','XQ'), '640',
		    v_symbol_pos_1_2 = 'NF' and class_of_business = '9', '520',
		    v_symbol_pos_1_2 = 'NK' and Type_Bureau = 'GL', '310',
		    v_symbol_pos_1_2 = 'CD' and Type_Bureau = 'GL', '310',
		    v_symbol_pos_1_2 = 'NK' and Type_Bureau = 'GL', '330',
		    v_symbol_pos_1_2 IN ('NC','NJ','NL','NO','NM'), '600',
		    v_symbol_pos_1_2 = 'NE', '330',
		    Type_Bureau = 'IM', '550',
		    Major_Peril = '032', '100'
		) AS v_Line_Of_Business_Code,
		v_Line_Of_Business_Code AS Line_Of_Business_Code,
		StatisticalCoverageEffectiveDate,
		StatisticalCoverageExpirationDate,
		StatisticalCoverageCancellationDate,
		RunDate,
		PremiumMasterCalculationID,
		AgencyAKID,
		PolicyAKID,
		strtgc_bus_dvsn_ak_id,
		ContractCustomerAKID,
		RiskLocationAKID,
		PolicyCoverageAKID,
		PremiumTransactionAKID,
		BureauStatisticalCodeAKID,
		PremiumMasterPolicyExpirationYear,
		PremiumMasterPolicyTerm,
		PremiumMasterBureauPolicyType,
		PremiumMasterAuditCode,
		PremiumMasterBureauStatisticalLine,
		PremiumMasterProductLine,
		PremiumMasterAgencyCommissionRate,
		PremiumMasterExposure,
		PremiumMasterStatisticalCode1,
		PremiumMasterStatisticalCode2,
		PremiumMasterStatisticalCode3,
		PremiumMasterRateModifier,
		PremiumMasterRateDeparture,
		PremiumMasterBureauInceptionDate,
		PremiumMasterCountersignAgencyType,
		PremiumMasterCountersignAgencyCode,
		PremiumMasterCountersignAgencyState,
		PremiumMasterCountersignAgencyRate,
		PremiumMasterRenewalIndicator,
		PremiumMasterRecordType,
		premium_master_stage_id,
		pm_policy_number,
		pm_module,
		pm_account_date,
		pm_sar_location_number,
		pm_unit_number,
		pm_risk_state,
		pm_risk_zone_territory,
		pm_tax_location,
		pm_risk_zip_code_postal_zone,
		pm_sar_insurance_line,
		pm_sar_sub_location_number,
		pm_sar_risk_unit_group,
		pm_sar_class_code_group,
		pm_sar_class_code_member,
		pm_sar_sequence_risk_unit_n,
		pm_sar_sequence_risk_unit_a,
		pm_sar_type_exposure,
		pm_sar_mp_seq_no,
		pm_csp_inception_date,
		pm_coverage_effective_date,
		pm_coverage_expiration_date,
		pm_reinsurance_ceded_premium,
		pm_reins_ceded_orig_premium,
		pm_reinsurance_type_code,
		pm_reinsurance_company_number,
		pm_reinsurance_ratio,
		AuditID,
		ProductCode,
		RatingCoverageEffectiveDate,
		RatingCoverageExpirationDate,
		RatingCoverageCancellationDate,
		RatingCoverageAKID,
		PolicyOfferingCode,
		PolicyCoverageEffectiveDate,
		PolicyCoverageExpirationDate,
		AgencyActualCommissionRate,
		InsuranceReferenceLineOfBusinessCode,
		EnterpriseGroupCode,
		InsuranceReferenceLegalEntityCode,
		StrategicProfitCenterCode,
		InsuranceSegmentCode,
		StandardInsuranceLineCode,
		RatingCoverage,
		RiskType,
		CoverageType,
		StandardSpecialClassGroupCode,
		StandardIncreasedLimitGroupCode,
		StandardPackageModifcationAdjustmentGroupCode,
		SourceSystemID,
		EarnedExposure,
		ChangeInEarnedExposure,
		RiskLocationHashKey,
		PerilGroup,
		CoverageForm,
		AnnualStatementLineCode_DCT,
		SubAnnualStatementLineCode_DCT,
		PolicyAuditAKID,
		PolicyAuditEffectiveDate,
		SubCoverageTypeCode,
		CoverageVersion,
		SubNonAnnualStatementLineCode_DCT,
		CoverageCode,
		CustomerCareCommissionRate,
		RatingPlanCode,
		CoverageCancellationDate,
		GeneratedRecordIndicator,
		DirectWrittenPremium,
		RatablePremium,
		ClassifiedPremium,
		OtherModifiedPremium,
		ScheduleModifiedPremium,
		ExperienceModifiedPremium,
		SubjectWrittenPremium,
		EarnedDirectWrittenPremium,
		EarnedClassifiedPremium,
		EarnedRatablePremium,
		EarnedOtherModifiedPremium,
		EarnedScheduleModifiedPremium,
		EarnedExperienceModifiedPremium,
		EarnedSubjectWrittenPremium,
		EarnedPremiumRunDate,
		PremiumMasterWrittenExposure,
		DeclaredEventFlag
		FROM EXP_accept_inputs
	),
	RTR_Split_Transactions AS (
		SELECT
		PolicyKey,
		PolicyEffectiveDate,
		PolicyExpirationDate,
		PremiumTransactionID,
		ReinsuranceCoverageAKID,
		StatisticalCoverageAKID,
		PremiumTransactionCode,
		PremiumTransactionEnteredDate,
		PremiumTransactionEffectiveDate,
		PremiumTransactionExpirationDate,
		PremiumTransactionBookedDate,
		PremiumType,
		ReasonAmendedCode,
		PolicySymbol,
		Type_Bureau AS TypeBureauCode,
		Major_Peril AS MajorPerilCode,
		RiskUnit,
		RiskUnitSequenceNumber,
		nsi_indicator,
		PremiumAmount,
		FullTermPremiumAmount,
		EarnedPremiumAmount,
		ChangeInEarnedPremium,
		symbol_pos_1_2_out AS symbol_pos_1_2,
		aslcode,
		subaslcode,
		Nonsubaslcode,
		ASLProduct_Code,
		Hierarchy_Product_Code,
		StatisticalCoverageEffectiveDate,
		StatisticalCoverageExpirationDate,
		StatisticalCoverageCancellationDate,
		RunDate,
		PremiumMasterCalculationID,
		AgencyAKID,
		PolicyAKID,
		strtgc_bus_dvsn_ak_id,
		ContractCustomerAKID,
		RiskLocationAKID,
		PolicyCoverageAKID,
		PremiumTransactionAKID,
		BureauStatisticalCodeAKID,
		PremiumMasterPolicyExpirationYear,
		PremiumMasterPolicyTerm,
		PremiumMasterBureauPolicyType,
		PremiumMasterAuditCode,
		PremiumMasterBureauStatisticalLine,
		PremiumMasterProductLine,
		PremiumMasterAgencyCommissionRate,
		PremiumMasterExposure,
		PremiumMasterStatisticalCode1,
		PremiumMasterStatisticalCode2,
		PremiumMasterStatisticalCode3,
		PremiumMasterRateModifier,
		PremiumMasterRateDeparture,
		PremiumMasterBureauInceptionDate,
		PremiumMasterCountersignAgencyType,
		PremiumMasterCountersignAgencyCode,
		PremiumMasterCountersignAgencyState,
		PremiumMasterCountersignAgencyRate,
		PremiumMasterRenewalIndicator,
		PremiumMasterRecordType,
		Class_Code AS ClassCode,
		SubLine,
		premium_master_stage_id,
		pm_policy_number,
		pm_module,
		pm_account_date,
		pm_sar_location_number,
		pm_unit_number,
		pm_risk_state,
		pm_risk_zone_territory,
		pm_tax_location,
		pm_risk_zip_code_postal_zone,
		pm_sar_insurance_line,
		pm_sar_sub_location_number,
		pm_sar_risk_unit_group,
		pm_sar_class_code_group,
		pm_sar_class_code_member,
		pm_sar_sequence_risk_unit_n,
		pm_sar_sequence_risk_unit_a,
		pm_sar_type_exposure,
		pm_sar_mp_seq_no,
		pm_csp_inception_date,
		pm_coverage_effective_date,
		pm_coverage_expiration_date,
		pm_reinsurance_ceded_premium AS pm_reins_ceded_premium,
		pm_reins_ceded_orig_premium AS pm_reins_ceded_original_premium,
		pm_reinsurance_type_code,
		pm_reinsurance_company_number,
		pm_reinsurance_ratio,
		AuditID,
		ProductCode,
		RatingCoverageEffectiveDate,
		RatingCoverageExpirationDate,
		RatingCoverageCancellationDate,
		RatingCoverageAKID,
		PolicyOfferingCode,
		PolicyCoverageEffectiveDate,
		PolicyCoverageExpirationDate,
		AgencyActualCommissionRate,
		InsuranceReferenceLineOfBusinessCode,
		EnterpriseGroupCode,
		InsuranceReferenceLegalEntityCode,
		StrategicProfitCenterCode,
		InsuranceSegmentCode,
		Risk_Unit_Group,
		StandardInsuranceLineCode,
		RatingCoverage,
		RiskType,
		CoverageType,
		StandardSpecialClassGroupCode,
		StandardIncreasedLimitGroupCode,
		StandardPackageModifcationAdjustmentGroupCode,
		SourceSystemID,
		EarnedExposure,
		ChangeInEarnedExposure,
		RiskLocationHashKey,
		PerilGroup,
		CoverageForm,
		AnnualStatementLineCode_DCT,
		SubAnnualStatementLineCode_DCT,
		PolicyAuditAKID,
		PolicyAuditEffectiveDate,
		PolicyAuditAKID AS PolicyAuditAKID11,
		PolicyAuditEffectiveDate AS PolicyAuditEffectiveDate11,
		SubCoverageTypeCode,
		CoverageVersion,
		SubNonAnnualStatementLineCode_DCT,
		CoverageCode,
		CustomerCareCommissionRate,
		RatingPlanCode,
		CoverageCancellationDate,
		GeneratedRecordIndicator,
		DirectWrittenPremium,
		RatablePremium,
		ClassifiedPremium,
		OtherModifiedPremium,
		ScheduleModifiedPremium,
		ExperienceModifiedPremium,
		SubjectWrittenPremium,
		EarnedDirectWrittenPremium,
		EarnedClassifiedPremium,
		EarnedRatablePremium,
		EarnedOtherModifiedPremium,
		EarnedScheduleModifiedPremium,
		EarnedExperienceModifiedPremium,
		EarnedSubjectWrittenPremium,
		EarnedPremiumRunDate,
		PremiumMasterWrittenExposure,
		DeclaredEventFlag
		FROM EXP_Evaluate
	),
	RTR_Split_Transactions_asl_Level AS (SELECT * FROM RTR_Split_Transactions WHERE SourceSystemID='PMS'),
	RTR_Split_Transactions_Mine_Subsidence AS (SELECT * FROM RTR_Split_Transactions WHERE SourceSystemID='PMS' AND MajorPerilCode = '050' AND PremiumType =  'D'
	
	--DECODE(TRUE,
	--IN(symbol_pos_1_2,'HA','HB','HH') AND MajorPerilCode = '050' AND TypeBureauCode = 'MS',
	--SUBSTR(symbol_pos_1_2,1,1) = 'N' AND IN(TypeBureauCode,'MS','NB') AND MajorPerilCode = '050',
	--SUBSTR(symbol_pos_1_2,1,1) = 'N' AND IN(TypeBureauCode,'MS','NB') AND MajorPerilCode = '050')),
	RTR_Split_Transactions_asl_20 AS (SELECT * FROM RTR_Split_Transactions WHERE SourceSystemID='PMS' AND aslcode='20' AND MajorPerilCode = '599'),
	RTR_Split_Transactions_asl_80 AS (SELECT * FROM RTR_Split_Transactions WHERE SourceSystemID='PMS' AND aslcode='80' and IN(MajorPerilCode,'901','902','599')),
	RTR_Split_Transactions_subasl_level_rows AS (SELECT * FROM RTR_Split_Transactions WHERE SourceSystemID='PMS' AND IN(subaslcode,'460','480','520','540')),
	RTR_Split_Transactions_NonSubasl_level_rows AS (SELECT * FROM RTR_Split_Transactions WHERE SourceSystemID='PMS' AND IN(aslcode,'260','340')),
	RTR_Split_Transactions_NonSubASL_Level_Row_320 AS (SELECT * FROM RTR_Split_Transactions WHERE (SourceSystemID='PMS' AND IN(Nonsubaslcode,'300') AND MajorPerilCode = '100')),
	RTR_Split_Transactions_NonSubASL_Level_Row_420 AS (SELECT * FROM RTR_Split_Transactions WHERE (
	SourceSystemID='PMS' AND IN(Nonsubaslcode,'400') AND IN(MajorPerilCode,@{pipeline().parameters.MP_271_274}, '100','599')
	)
	 OR 
	(
	SourceSystemID='DCT' AND IN(SubNonAnnualStatementLineCode_DCT,'400') AND StandardInsuranceLineCode='CA' AND 
	   ( IN(CoverageCode, 'ADLINS', 'AGTEO', 'BIPDEX', 'BIPD', 'BRDCOVGA', 'BRDFRMPRDCOMOP', 'BRDFRMPRD', 'COMPMISC', 'COMRLIABUIM', 'COMRLIABUM', 'COMRLIAB', 'CAFEMPCOV', 'EMPLESSOR', 'EMPLBEN', 'FELEMPL', 'INJLEASEWRKS', 'LSECONCRN', 'LIMMEXCOV', 'LEMONLAW', 'MINPREM', 'MNRENTVHCL', 'NFRNCHSAD',   'MANU', 'MNRENTVEH', 'PLSPAK - BRD', 'RAILOPTS', 'RACEXCL','REINSPREM', 'RNTTEMPVHCL', 'TLEASE', 'TLENDG','WATRCRFTEXT','UMBIPD','COMRLIABUMBIPD','EXCDWYP','EXCDP','PRDAMEO',
	'LGLDEFCST','EXCPWYP','EXCDRENTP','EXCNAFAD','LIMCTLIABPAA','CADLGLAL',
	'LIMPRODW','EMPLBENERPE','FACTESTHAZ','BIPDBUYBK'
	
	) 
	      OR 
	      IN(CoverageCode, 'UIM', 'UM') AND IN(CoverageType, 'UIM', 'UMBIPD', 'DriveOtherCarUIM', 'NonOwnedAutoUIM', 'NonOwnedAutoUM','NonOwnedAutoStateUIM')
	      OR 
		  CoverageCode = 'SR22' AND IN(CoverageType,'FinancialResponsibilityLiability','FinancialResponsibilityLawsLiability')
	     ) 
	)),
	RTR_Split_Transactions_asl_DCT AS (SELECT * FROM RTR_Split_Transactions WHERE SourceSystemID='DCT'),
	EXP2_ASL_100_Level_Row AS (
		SELECT
		PolicyKey AS PolicyKey5,
		PolicyEffectiveDate AS PolicyEffectiveDate5,
		PolicyExpirationDate AS PolicyExpirationDate5,
		PremiumTransactionID AS PremiumTransactionID6,
		ReinsuranceCoverageAKID AS ReinsuranceCoverageAKID6,
		StatisticalCoverageAKID AS StatisticalCoverageAKID6,
		PremiumTransactionCode AS PremiumTransactionCode6,
		PremiumTransactionEnteredDate AS PremiumTransactionEnteredDate6,
		PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate6,
		PremiumTransactionExpirationDate AS PremiumTransactionExpirationDate6,
		PremiumTransactionBookedDate AS PremiumTransactionBookedDate6,
		PremiumType AS PremiumType6,
		ReasonAmendedCode AS ReasonAmendedCode6,
		PolicySymbol,
		TypeBureauCode,
		MajorPerilCode,
		RiskUnit,
		RiskUnitSequenceNumber AS RiskUnitSequenceNumber5,
		nsi_indicator,
		symbol_pos_1_2,
		PremiumAmount,
		-- *INF*: DECODE(TRUE,
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (.35) * PremiumAmount,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (.35) * PremiumAmount,
		-- PremiumAmount)
		DECODE(
		    TRUE,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (.35) * PremiumAmount,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (.35) * PremiumAmount,
		    PremiumAmount
		) AS PremiumAmount_Out,
		FullTermPremiumAmount,
		-- *INF*: DECODE(TRUE,
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (.35) * FullTermPremiumAmount,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (.35) * FullTermPremiumAmount,
		-- FullTermPremiumAmount)
		DECODE(
		    TRUE,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (.35) * FullTermPremiumAmount,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (.35) * FullTermPremiumAmount,
		    FullTermPremiumAmount
		) AS FullTermPremiumAmount_Out,
		EarnedPremiumAmount,
		-- *INF*: DECODE(TRUE,
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (.35) * EarnedPremiumAmount,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (.35) * EarnedPremiumAmount,
		-- EarnedPremiumAmount)
		DECODE(
		    TRUE,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (.35) * EarnedPremiumAmount,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (.35) * EarnedPremiumAmount,
		    EarnedPremiumAmount
		) AS EarnedPremiumAmount_Out,
		ChangeInEarnedPremium,
		-- *INF*: DECODE(TRUE,
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (.35) * ChangeInEarnedPremium,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (.35) * ChangeInEarnedPremium,
		-- ChangeInEarnedPremium)
		DECODE(
		    TRUE,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (.35) * ChangeInEarnedPremium,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (.35) * ChangeInEarnedPremium,
		    ChangeInEarnedPremium
		) AS ChangeInEarnedPremium_out,
		'100' AS aslcode,
		'N/A' AS subaslcode,
		'N/A' AS Nonsubaslcode,
		ASLProduct_Code AS ASLProduct_Code5,
		Hierarchy_Product_Code AS Hierarchy_Product_Code5,
		StatisticalCoverageEffectiveDate AS StatisticalCoverageEffectiveDate5,
		StatisticalCoverageExpirationDate,
		StatisticalCoverageCancellationDate,
		RunDate AS RunDate5,
		PremiumMasterCalculationID AS PremiumMasterCalculationID5,
		AgencyAKID AS AgencyAKID5,
		PolicyAKID AS PolicyAKID5,
		strtgc_bus_dvsn_ak_id AS strtgc_bus_dvsn_ak_id5,
		ContractCustomerAKID AS ContractCustomerAKID5,
		RiskLocationAKID,
		PolicyCoverageAKID AS PolicyCoverageAKID5,
		PremiumTransactionAKID AS PremiumTransactionAKID5,
		BureauStatisticalCodeAKID AS BureauStatisticalCodeAKID5,
		PremiumMasterPolicyExpirationYear AS PremiumMasterPolicyExpirationYear5,
		PremiumMasterPolicyTerm AS PremiumMasterPolicyTerm5,
		PremiumMasterBureauPolicyType AS PremiumMasterBureauPolicyType5,
		PremiumMasterAuditCode AS PremiumMasterAuditCode5,
		PremiumMasterBureauStatisticalLine AS PremiumMasterBureauStatisticalLine5,
		PremiumMasterProductLine AS PremiumMasterProductLine5,
		PremiumMasterAgencyCommissionRate AS PremiumMasterAgencyCommissionRate5,
		PremiumMasterExposure AS PremiumMasterExposure5,
		PremiumMasterStatisticalCode1 AS PremiumMasterStatisticalCode15,
		PremiumMasterStatisticalCode2 AS PremiumMasterStatisticalCode25,
		PremiumMasterStatisticalCode3 AS PremiumMasterStatisticalCode35,
		PremiumMasterRateModifier AS PremiumMasterRateModifier5,
		PremiumMasterRateDeparture AS PremiumMasterRateDeparture5,
		PremiumMasterBureauInceptionDate AS PremiumMasterBureauInceptionDate5,
		PremiumMasterCountersignAgencyType AS PremiumMasterCountersignAgencyType5,
		PremiumMasterCountersignAgencyCode AS PremiumMasterCountersignAgencyCode5,
		PremiumMasterCountersignAgencyState AS PremiumMasterCountersignAgencyState5,
		PremiumMasterCountersignAgencyRate AS PremiumMasterCountersignAgencyRate5,
		PremiumMasterRenewalIndicator AS PremiumMasterRenewalIndicator5,
		PremiumMasterRecordType AS PremiumMasterRecordType5,
		ClassCode AS ClassCode5,
		SubLine AS SubLine5,
		premium_master_stage_id AS premium_master_stage_id5,
		pm_policy_number AS pm_policy_number5,
		pm_module AS pm_module5,
		pm_account_date AS pm_account_date5,
		pm_sar_location_number AS pm_sar_location_number5,
		pm_unit_number AS pm_unit_number5,
		pm_risk_state AS pm_risk_state5,
		pm_risk_zone_territory AS pm_risk_zone_territory5,
		pm_tax_location AS pm_tax_location5,
		pm_risk_zip_code_postal_zone AS pm_risk_zip_code_postal_zone5,
		pm_sar_insurance_line AS pm_sar_insurance_line5,
		pm_sar_sub_location_number AS pm_sar_sub_location_number5,
		pm_sar_risk_unit_group AS pm_sar_risk_unit_group5,
		pm_sar_class_code_group AS pm_sar_class_code_group5,
		pm_sar_class_code_member AS pm_sar_class_code_member5,
		pm_sar_sequence_risk_unit_n AS pm_sar_sequence_risk_unit_n5,
		pm_sar_sequence_risk_unit_a AS pm_sar_sequence_risk_unit_a5,
		pm_sar_type_exposure AS pm_sar_type_exposure5,
		pm_sar_mp_seq_no AS pm_sar_mp_seq_no5,
		pm_csp_inception_date AS pm_csp_inception_date5,
		pm_coverage_effective_date AS pm_coverage_effective_date5,
		pm_coverage_expiration_date AS pm_coverage_expiration_date5,
		pm_reins_ceded_premium AS pm_reins_ceded_premium5,
		-- *INF*: DECODE(TRUE,
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (.35) * pm_reins_ceded_premium5,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (.35) * pm_reins_ceded_premium5, pm_reins_ceded_premium5)
		DECODE(
		    TRUE,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (.35) * pm_reins_ceded_premium5,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (.35) * pm_reins_ceded_premium5,
		    pm_reins_ceded_premium5
		) AS out_pm_reins_ceded_premium,
		pm_reins_ceded_original_premium AS pm_reins_ceded_original_premium5,
		-- *INF*: DECODE(TRUE,
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (.35) * pm_reins_ceded_original_premium5,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (.35) * pm_reins_ceded_original_premium5, pm_reins_ceded_original_premium5)
		DECODE(
		    TRUE,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (.35) * pm_reins_ceded_original_premium5,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (.35) * pm_reins_ceded_original_premium5,
		    pm_reins_ceded_original_premium5
		) AS out_pm_reins_ceded_original_premium,
		pm_reinsurance_type_code AS pm_reinsurance_type_code5,
		pm_reinsurance_company_number AS pm_reinsurance_company_number5,
		pm_reinsurance_ratio AS pm_reinsurance_ratio5,
		AuditID AS AuditID5,
		ProductCode AS ProductCode5,
		RatingCoverageEffectiveDate AS RatingCoverageEffectiveDate5,
		RatingCoverageExpirationDate AS RatingCoverageExpirationDate5,
		RatingCoverageCancellationDate AS RatingCoverageCancellationDate5,
		RatingCoverageAKID AS RatingCoverageAKID5,
		PolicyOfferingCode AS PolicyOfferingCode5,
		PolicyCoverageEffectiveDate AS PolicyCoverageEffectiveDate5,
		PolicyCoverageExpirationDate AS PolicyCoverageExpirationDate5,
		AgencyActualCommissionRate AS AgencyActualCommissionRate5,
		InsuranceReferenceLineOfBusinessCode AS InsuranceReferenceLineOfBusinessCode5,
		-- *INF*: IIF(IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,@{pipeline().parameters.MP_901_904},'599') AND IN(TypeBureauCode,'BB','BE','BC'),'300',InsuranceReferenceLineOfBusinessCode5)
		-- 
		-- ---- InsuraceReferenceLineofBusinessCode for Symbol - BA,BA  need to be changed to 300 when the % Split is 35%, other wise it is original value of 500 from StatisticalCoverage.
		IFF(
		    symbol_pos_1_2 IN ('BA','BB')
		    and MajorPerilCode IN (@{pipeline().parameters.MP_901_904},'599')
		    and TypeBureauCode IN ('BB','BE','BC'),
		    '300',
		    InsuranceReferenceLineOfBusinessCode5
		) AS InsuranceReferenceLineOfBusinessCode,
		EnterpriseGroupCode AS EnterpriseGroupCode5,
		InsuranceReferenceLegalEntityCode AS InsuranceReferenceLegalEntityCode5,
		StrategicProfitCenterCode AS StrategicProfitCenterCode5,
		InsuranceSegmentCode AS InsuranceSegmentCode5,
		Risk_Unit_Group AS Risk_Unit_Group5,
		StandardInsuranceLineCode AS StandardInsuranceLineCode5,
		RatingCoverage AS RatingCoverage5,
		RiskType AS RiskType5,
		CoverageType AS CoverageType5,
		StandardSpecialClassGroupCode AS StandardSpecialClassGroupCode5,
		StandardIncreasedLimitGroupCode AS StandardIncreasedLimitGroupCode5,
		StandardPackageModifcationAdjustmentGroupCode AS StandardPackageModifcationAdjustmentGroupCode5,
		SourceSystemID AS SourceSystemID5,
		EarnedExposure AS EarnedExposure5,
		ChangeInEarnedExposure AS ChangeInEarnedExposure5,
		RiskLocationHashKey AS RiskLocationHashKey5,
		PerilGroup,
		CoverageForm AS CoverageForm5,
		PolicyAuditAKID11 AS PolicyAuditAKID115,
		PolicyAuditEffectiveDate11 AS PolicyAuditEffectiveDate115,
		SubCoverageTypeCode AS SubCoverageTypeCode5,
		CoverageVersion AS CoverageVersion5,
		CustomerCareCommissionRate AS CustomerCareCommissionRate5,
		RatingPlanCode AS RatingPlanCode5,
		CoverageCancellationDate AS CoverageCancellationDate5,
		GeneratedRecordIndicator AS GeneratedRecordIndicator5,
		DirectWrittenPremium AS i_DirectWrittenPremium5,
		RatablePremium AS i_RatablePremium5,
		ClassifiedPremium AS i_ClassifiedPremium5,
		OtherModifiedPremium AS i_OtherModifiedPremium5,
		ScheduleModifiedPremium AS i_ScheduleModifiedPremium5,
		ExperienceModifiedPremium AS i_ExperienceModifiedPremium5,
		SubjectWrittenPremium AS i_SubjectWrittenPremium5,
		-- *INF*: DECODE(TRUE,
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (.35) * i_DirectWrittenPremium5,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (.35) * i_DirectWrittenPremium5,
		-- i_DirectWrittenPremium5)
		DECODE(
		    TRUE,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (.35) * i_DirectWrittenPremium5,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (.35) * i_DirectWrittenPremium5,
		    i_DirectWrittenPremium5
		) AS o_DirectWrittenPremium5,
		-- *INF*: DECODE(TRUE,
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (.35) * i_RatablePremium5,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (.35) * i_RatablePremium5,
		-- i_RatablePremium5)
		DECODE(
		    TRUE,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (.35) * i_RatablePremium5,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (.35) * i_RatablePremium5,
		    i_RatablePremium5
		) AS o_RatablePremium5,
		-- *INF*: DECODE(TRUE,
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (.35) * i_ClassifiedPremium5,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (.35) * i_ClassifiedPremium5,
		-- i_ClassifiedPremium5)
		DECODE(
		    TRUE,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (.35) * i_ClassifiedPremium5,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (.35) * i_ClassifiedPremium5,
		    i_ClassifiedPremium5
		) AS o_ClassifiedPremium5,
		-- *INF*: DECODE(TRUE,
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (.35) * i_OtherModifiedPremium5,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (.35) * i_OtherModifiedPremium5,
		-- i_OtherModifiedPremium5)
		DECODE(
		    TRUE,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (.35) * i_OtherModifiedPremium5,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (.35) * i_OtherModifiedPremium5,
		    i_OtherModifiedPremium5
		) AS o_OtherModifiedPremium5,
		-- *INF*: DECODE(TRUE,
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (.35) * i_ScheduleModifiedPremium5,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (.35) * i_ScheduleModifiedPremium5,
		-- i_ScheduleModifiedPremium5)
		DECODE(
		    TRUE,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (.35) * i_ScheduleModifiedPremium5,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (.35) * i_ScheduleModifiedPremium5,
		    i_ScheduleModifiedPremium5
		) AS o_ScheduleModifiedPremium5,
		-- *INF*: DECODE(TRUE,
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (.35) * i_ExperienceModifiedPremium5,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (.35) * i_ExperienceModifiedPremium5,
		-- i_ExperienceModifiedPremium5)
		DECODE(
		    TRUE,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (.35) * i_ExperienceModifiedPremium5,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (.35) * i_ExperienceModifiedPremium5,
		    i_ExperienceModifiedPremium5
		) AS o_ExperienceModifiedPremium5,
		-- *INF*: DECODE(TRUE,
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (.35) * i_SubjectWrittenPremium5,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (.35) * i_SubjectWrittenPremium5,
		-- i_SubjectWrittenPremium5)
		DECODE(
		    TRUE,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (.35) * i_SubjectWrittenPremium5,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (.35) * i_SubjectWrittenPremium5,
		    i_SubjectWrittenPremium5
		) AS o_SubjectWrittenPremium5,
		EarnedDirectWrittenPremium AS EarnedDirectWrittenPremium5,
		EarnedClassifiedPremium AS EarnedClassifiedPremium5,
		EarnedRatablePremium AS EarnedRatablePremium5,
		EarnedOtherModifiedPremium AS EarnedOtherModifiedPremium5,
		EarnedScheduleModifiedPremium AS EarnedScheduleModifiedPremium5,
		EarnedExperienceModifiedPremium AS EarnedExperienceModifiedPremium5,
		EarnedSubjectWrittenPremium AS EarnedSubjectWrittenPremium5,
		EarnedPremiumRunDate AS EarnedPremiumRunDate5,
		PremiumMasterWrittenExposure AS PremiumMasterWrittenExposure5,
		DeclaredEventFlag AS DeclaredEventFlag5
		FROM RTR_Split_Transactions_asl_80
	),
	EXP_NonSubASL_Level_Row AS (
		SELECT
		PolicyKey AS PolicyKey7,
		PolicyEffectiveDate AS PolicyEffectiveDate7,
		PolicyExpirationDate AS PolicyExpirationDate7,
		PremiumTransactionID,
		ReinsuranceCoverageAKID,
		StatisticalCoverageAKID,
		PremiumTransactionCode,
		PremiumTransactionEnteredDate,
		PremiumTransactionEffectiveDate,
		PremiumTransactionExpirationDate,
		PremiumTransactionBookedDate,
		PremiumType,
		ReasonAmendedCode,
		PolicySymbol,
		TypeBureauCode,
		MajorPerilCode,
		RiskUnit,
		RiskUnitSequenceNumber AS RiskUnitSequenceNumber7,
		nsi_indicator,
		symbol_pos_1_2,
		PremiumAmount,
		-- *INF*: DECODE(TRUE,
		-- aslcode = '260' AND MajorPerilCode ='100' , (0.68) * PremiumAmount, 
		-- aslcode = '340' AND IN(MajorPerilCode,@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * PremiumAmount,
		-- PremiumAmount)
		-- 
		DECODE(
		    TRUE,
		    aslcode = '260' AND MajorPerilCode = '100', (0.68) * PremiumAmount,
		    aslcode = '340' AND MajorPerilCode IN (@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * PremiumAmount,
		    PremiumAmount
		) AS PremiumAmount_Out,
		FullTermPremiumAmount,
		-- *INF*: DECODE(TRUE,
		-- aslcode = '260' AND MajorPerilCode ='100' , (0.68) * FullTermPremiumAmount, 
		-- aslcode = '340' AND IN(MajorPerilCode,@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * FullTermPremiumAmount,
		-- FullTermPremiumAmount)
		DECODE(
		    TRUE,
		    aslcode = '260' AND MajorPerilCode = '100', (0.68) * FullTermPremiumAmount,
		    aslcode = '340' AND MajorPerilCode IN (@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * FullTermPremiumAmount,
		    FullTermPremiumAmount
		) AS FullTermPremiumAmount_Out,
		EarnedPremiumAmount,
		-- *INF*: DECODE(TRUE,
		-- aslcode = '260' AND MajorPerilCode ='100' , (0.68) * EarnedPremiumAmount, 
		-- aslcode = '340' AND IN(MajorPerilCode,@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * EarnedPremiumAmount,
		-- EarnedPremiumAmount)
		DECODE(
		    TRUE,
		    aslcode = '260' AND MajorPerilCode = '100', (0.68) * EarnedPremiumAmount,
		    aslcode = '340' AND MajorPerilCode IN (@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * EarnedPremiumAmount,
		    EarnedPremiumAmount
		) AS EarnedPremiumAmount_Out,
		ChangeInEarnedPremium,
		-- *INF*: DECODE(TRUE,
		-- aslcode = '260' AND MajorPerilCode ='100' , (0.68) * ChangeInEarnedPremium, 
		-- aslcode = '340' AND IN(MajorPerilCode,@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * ChangeInEarnedPremium,
		-- ChangeInEarnedPremium)
		DECODE(
		    TRUE,
		    aslcode = '260' AND MajorPerilCode = '100', (0.68) * ChangeInEarnedPremium,
		    aslcode = '340' AND MajorPerilCode IN (@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * ChangeInEarnedPremium,
		    ChangeInEarnedPremium
		) AS ChangeInEarnedPremium_Out,
		aslcode,
		subaslcode,
		Nonsubaslcode,
		ASLProduct_Code AS ASLProduct_Code7,
		Hierarchy_Product_Code AS Hierarchy_Product_Code7,
		StatisticalCoverageEffectiveDate AS StatisticalCoverageEffectiveDate7,
		StatisticalCoverageExpirationDate AS StatisticalCoverageExpirationDate7,
		StatisticalCoverageCancellationDate AS StatisticalCoverageCancellationDate7,
		RunDate AS RunDate7,
		PremiumMasterCalculationID AS PremiumMasterCalculationID7,
		AgencyAKID AS AgencyAKID7,
		PolicyAKID AS PolicyAKID7,
		strtgc_bus_dvsn_ak_id AS strtgc_bus_dvsn_ak_id7,
		ContractCustomerAKID AS ContractCustomerAKID7,
		RiskLocationAKID,
		PolicyCoverageAKID AS PolicyCoverageAKID7,
		PremiumTransactionAKID AS PremiumTransactionAKID7,
		BureauStatisticalCodeAKID AS BureauStatisticalCodeAKID7,
		PremiumMasterPolicyExpirationYear AS PremiumMasterPolicyExpirationYear7,
		PremiumMasterPolicyTerm AS PremiumMasterPolicyTerm7,
		PremiumMasterBureauPolicyType AS PremiumMasterBureauPolicyType7,
		PremiumMasterAuditCode AS PremiumMasterAuditCode7,
		PremiumMasterBureauStatisticalLine AS PremiumMasterBureauStatisticalLine7,
		PremiumMasterProductLine AS PremiumMasterProductLine7,
		PremiumMasterAgencyCommissionRate AS PremiumMasterAgencyCommissionRate7,
		PremiumMasterExposure AS PremiumMasterExposure7,
		PremiumMasterStatisticalCode1 AS PremiumMasterStatisticalCode17,
		PremiumMasterStatisticalCode2 AS PremiumMasterStatisticalCode27,
		PremiumMasterStatisticalCode3 AS PremiumMasterStatisticalCode37,
		PremiumMasterRateModifier AS PremiumMasterRateModifier7,
		PremiumMasterRateDeparture AS PremiumMasterRateDeparture7,
		PremiumMasterBureauInceptionDate AS PremiumMasterBureauInceptionDate7,
		PremiumMasterCountersignAgencyType AS PremiumMasterCountersignAgencyType7,
		PremiumMasterCountersignAgencyCode AS PremiumMasterCountersignAgencyCode7,
		PremiumMasterCountersignAgencyState AS PremiumMasterCountersignAgencyState7,
		PremiumMasterCountersignAgencyRate AS PremiumMasterCountersignAgencyRate7,
		PremiumMasterRenewalIndicator AS PremiumMasterRenewalIndicator7,
		PremiumMasterRecordType AS PremiumMasterRecordType7,
		ClassCode AS ClassCode7,
		SubLine AS SubLine7,
		premium_master_stage_id AS premium_master_stage_id7,
		pm_policy_number AS pm_policy_number7,
		pm_module AS pm_module7,
		pm_account_date AS pm_account_date7,
		pm_sar_location_number AS pm_sar_location_number7,
		pm_unit_number AS pm_unit_number7,
		pm_risk_state AS pm_risk_state7,
		pm_risk_zone_territory AS pm_risk_zone_territory7,
		pm_tax_location AS pm_tax_location7,
		pm_risk_zip_code_postal_zone AS pm_risk_zip_code_postal_zone7,
		pm_sar_insurance_line AS pm_sar_insurance_line7,
		pm_sar_sub_location_number AS pm_sar_sub_location_number7,
		pm_sar_risk_unit_group AS pm_sar_risk_unit_group7,
		pm_sar_class_code_group AS pm_sar_class_code_group7,
		pm_sar_class_code_member AS pm_sar_class_code_member7,
		pm_sar_sequence_risk_unit_n AS pm_sar_sequence_risk_unit_n7,
		pm_sar_sequence_risk_unit_a AS pm_sar_sequence_risk_unit_a7,
		pm_sar_type_exposure AS pm_sar_type_exposure7,
		pm_sar_mp_seq_no AS pm_sar_mp_seq_no7,
		pm_csp_inception_date AS pm_csp_inception_date7,
		pm_coverage_effective_date AS pm_coverage_effective_date7,
		pm_coverage_expiration_date AS pm_coverage_expiration_date7,
		pm_reins_ceded_premium AS pm_reins_ceded_premium7,
		-- *INF*: DECODE(TRUE,
		-- aslcode = '260' AND MajorPerilCode ='100' , (0.68) * pm_reins_ceded_premium7, 
		-- aslcode = '340' AND IN(MajorPerilCode,@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * pm_reins_ceded_premium7,
		-- pm_reins_ceded_premium7)
		DECODE(
		    TRUE,
		    aslcode = '260' AND MajorPerilCode = '100', (0.68) * pm_reins_ceded_premium7,
		    aslcode = '340' AND MajorPerilCode IN (@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * pm_reins_ceded_premium7,
		    pm_reins_ceded_premium7
		) AS out_pm_reins_ceded_premium,
		pm_reins_ceded_original_premium AS pm_reins_ceded_original_premium7,
		-- *INF*: DECODE(TRUE,
		-- aslcode = '260' AND MajorPerilCode ='100' , (0.68) * pm_reins_ceded_original_premium7, 
		-- aslcode = '340' AND IN(MajorPerilCode,@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * pm_reins_ceded_original_premium7,
		-- pm_reins_ceded_original_premium7)
		DECODE(
		    TRUE,
		    aslcode = '260' AND MajorPerilCode = '100', (0.68) * pm_reins_ceded_original_premium7,
		    aslcode = '340' AND MajorPerilCode IN (@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * pm_reins_ceded_original_premium7,
		    pm_reins_ceded_original_premium7
		) AS out_pm_reins_ceded_original_premium,
		pm_reinsurance_type_code AS pm_reinsurance_type_code7,
		pm_reinsurance_company_number AS pm_reinsurance_company_number7,
		pm_reinsurance_ratio AS pm_reinsurance_ratio7,
		AuditID AS AuditID7,
		ProductCode AS ProductCode7,
		RatingCoverageEffectiveDate AS RatingCoverageEffectiveDate7,
		RatingCoverageExpirationDate AS RatingCoverageExpirationDate7,
		RatingCoverageCancellationDate AS RatingCoverageCancellationDate7,
		RatingCoverageAKID AS RatingCoverageAKID7,
		PolicyOfferingCode AS PolicyOfferingCode7,
		PolicyCoverageEffectiveDate AS PolicyCoverageEffectiveDate7,
		PolicyCoverageExpirationDate AS PolicyCoverageExpirationDate7,
		AgencyActualCommissionRate AS AgencyActualCommissionRate7,
		InsuranceReferenceLineOfBusinessCode AS InsuranceReferenceLineOfBusinessCode7,
		EnterpriseGroupCode AS EnterpriseGroupCode7,
		InsuranceReferenceLegalEntityCode AS InsuranceReferenceLegalEntityCode7,
		StrategicProfitCenterCode AS StrategicProfitCenterCode7,
		InsuranceSegmentCode AS InsuranceSegmentCode7,
		Risk_Unit_Group AS Risk_Unit_Group7,
		StandardInsuranceLineCode AS StandardInsuranceLineCode7,
		RatingCoverage AS RatingCoverage7,
		RiskType AS RiskType7,
		CoverageType AS CoverageType7,
		StandardSpecialClassGroupCode AS StandardSpecialClassGroupCode7,
		StandardIncreasedLimitGroupCode AS StandardIncreasedLimitGroupCode7,
		StandardPackageModifcationAdjustmentGroupCode AS StandardPackageModifcationAdjustmentGroupCode7,
		SourceSystemID AS SourceSystemID7,
		EarnedExposure AS EarnedExposure7,
		ChangeInEarnedExposure AS ChangeInEarnedExposure7,
		RiskLocationHashKey AS RiskLocationHashKey7,
		PerilGroup,
		CoverageForm AS CoverageForm7,
		PolicyAuditAKID11 AS PolicyAuditAKID117,
		PolicyAuditEffectiveDate11 AS PolicyAuditEffectiveDate117,
		SubCoverageTypeCode AS SubCoverageTypeCode7,
		CoverageVersion AS CoverageVersion7,
		CustomerCareCommissionRate AS CustomerCareCommissionRate7,
		RatingPlanCode AS RatingPlanCode7,
		CoverageCancellationDate AS CoverageCancellationDate7,
		GeneratedRecordIndicator AS GeneratedRecordIndicator7,
		DirectWrittenPremium AS i_DirectWrittenPremium7,
		RatablePremium AS i_RatablePremium7,
		ClassifiedPremium AS i_ClassifiedPremium7,
		OtherModifiedPremium AS i_OtherModifiedPremium7,
		ScheduleModifiedPremium AS i_ScheduleModifiedPremium7,
		ExperienceModifiedPremium AS i_ExperienceModifiedPremium7,
		SubjectWrittenPremium AS i_SubjectWrittenPremium7,
		-- *INF*: DECODE(TRUE,
		-- aslcode = '260' AND MajorPerilCode ='100' , (0.68) * i_DirectWrittenPremium7, 
		-- aslcode = '340' AND IN(MajorPerilCode,@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * i_DirectWrittenPremium7,
		-- i_DirectWrittenPremium7)
		DECODE(
		    TRUE,
		    aslcode = '260' AND MajorPerilCode = '100', (0.68) * i_DirectWrittenPremium7,
		    aslcode = '340' AND MajorPerilCode IN (@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * i_DirectWrittenPremium7,
		    i_DirectWrittenPremium7
		) AS o_DirectWrittenPremium7,
		-- *INF*: DECODE(TRUE,
		-- aslcode = '260' AND MajorPerilCode ='100' , (0.68) * i_RatablePremium7, 
		-- aslcode = '340' AND IN(MajorPerilCode,@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * i_RatablePremium7,
		-- i_RatablePremium7)
		DECODE(
		    TRUE,
		    aslcode = '260' AND MajorPerilCode = '100', (0.68) * i_RatablePremium7,
		    aslcode = '340' AND MajorPerilCode IN (@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * i_RatablePremium7,
		    i_RatablePremium7
		) AS o_RatablePremium7,
		-- *INF*: DECODE(TRUE,
		-- aslcode = '260' AND MajorPerilCode ='100' , (0.68) * i_ClassifiedPremium7, 
		-- aslcode = '340' AND IN(MajorPerilCode,@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * i_ClassifiedPremium7,
		-- i_ClassifiedPremium7)
		DECODE(
		    TRUE,
		    aslcode = '260' AND MajorPerilCode = '100', (0.68) * i_ClassifiedPremium7,
		    aslcode = '340' AND MajorPerilCode IN (@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * i_ClassifiedPremium7,
		    i_ClassifiedPremium7
		) AS o_ClassifiedPremium7,
		-- *INF*: DECODE(TRUE,
		-- aslcode = '260' AND MajorPerilCode ='100' , (0.68) * i_ScheduleModifiedPremium7, 
		-- aslcode = '340' AND IN(MajorPerilCode,@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * i_ScheduleModifiedPremium7,
		-- i_ScheduleModifiedPremium7)
		-- 
		DECODE(
		    TRUE,
		    aslcode = '260' AND MajorPerilCode = '100', (0.68) * i_ScheduleModifiedPremium7,
		    aslcode = '340' AND MajorPerilCode IN (@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * i_ScheduleModifiedPremium7,
		    i_ScheduleModifiedPremium7
		) AS o_ScheduleModifiedPremium7,
		-- *INF*: DECODE(TRUE,
		-- aslcode = '260' AND MajorPerilCode ='100' , (0.68) * i_OtherModifiedPremium7, 
		-- aslcode = '340' AND IN(MajorPerilCode,@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * i_OtherModifiedPremium7,
		-- i_OtherModifiedPremium7)
		-- 
		DECODE(
		    TRUE,
		    aslcode = '260' AND MajorPerilCode = '100', (0.68) * i_OtherModifiedPremium7,
		    aslcode = '340' AND MajorPerilCode IN (@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * i_OtherModifiedPremium7,
		    i_OtherModifiedPremium7
		) AS o_OtherModifiedPremium7,
		-- *INF*: DECODE(TRUE,
		-- aslcode = '260' AND MajorPerilCode ='100' , (0.68) * i_ExperienceModifiedPremium7, 
		-- aslcode = '340' AND IN(MajorPerilCode,@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * i_ExperienceModifiedPremium7,
		-- i_ExperienceModifiedPremium7)
		DECODE(
		    TRUE,
		    aslcode = '260' AND MajorPerilCode = '100', (0.68) * i_ExperienceModifiedPremium7,
		    aslcode = '340' AND MajorPerilCode IN (@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * i_ExperienceModifiedPremium7,
		    i_ExperienceModifiedPremium7
		) AS o_ExperienceModifiedPremium7,
		-- *INF*: DECODE(TRUE,
		-- aslcode = '260' AND MajorPerilCode ='100' , (0.68) * i_SubjectWrittenPremium7, 
		-- aslcode = '340' AND IN(MajorPerilCode,@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * i_SubjectWrittenPremium7,
		-- i_SubjectWrittenPremium7)
		DECODE(
		    TRUE,
		    aslcode = '260' AND MajorPerilCode = '100', (0.68) * i_SubjectWrittenPremium7,
		    aslcode = '340' AND MajorPerilCode IN (@{pipeline().parameters.MP_271_274},'100','599'), (0.68) * i_SubjectWrittenPremium7,
		    i_SubjectWrittenPremium7
		) AS o_SubjectWrittenPremium7,
		EarnedDirectWrittenPremium AS EarnedDirectWrittenPremium7,
		EarnedClassifiedPremium AS EarnedClassifiedPremium7,
		EarnedRatablePremium AS EarnedRatablePremium7,
		EarnedOtherModifiedPremium AS EarnedOtherModifiedPremium7,
		EarnedScheduleModifiedPremium AS EarnedScheduleModifiedPremium7,
		EarnedExperienceModifiedPremium AS EarnedExperienceModifiedPremium7,
		EarnedSubjectWrittenPremium AS EarnedSubjectWrittenPremium7,
		EarnedPremiumRunDate AS EarnedPremiumRunDate7,
		PremiumMasterWrittenExposure AS PremiumMasterWrittenExposure7,
		DeclaredEventFlag AS DeclaredEventFlag7
		FROM RTR_Split_Transactions_NonSubasl_level_rows
	),
	EXP2_ASL_40_Level_Row AS (
		SELECT
		PolicyKey AS PolicyKey4,
		PolicyEffectiveDate AS PolicyEffectiveDate4,
		PolicyExpirationDate AS PolicyExpirationDate4,
		PremiumTransactionID AS PremiumTransactionID5,
		ReinsuranceCoverageAKID AS ReinsuranceCoverageAKID5,
		StatisticalCoverageAKID AS StatisticalCoverageAKID5,
		PremiumTransactionCode AS PremiumTransactionCode5,
		PremiumTransactionEnteredDate AS PremiumTransactionEnteredDate5,
		PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate5,
		PremiumTransactionExpirationDate AS PremiumTransactionExpirationDate5,
		PremiumTransactionBookedDate AS PremiumTransactionBookedDate5,
		PremiumType AS PremiumType5,
		ReasonAmendedCode AS ReasonAmendedCode5,
		PolicySymbol,
		TypeBureauCode,
		MajorPerilCode,
		RiskUnit,
		RiskUnitSequenceNumber AS RiskUnitSequenceNumber4,
		nsi_indicator,
		PremiumAmount,
		-- *INF*: IIF(MajorPerilCode = '599', 0.5 * PremiumAmount, PremiumAmount)
		IFF(MajorPerilCode = '599', 0.5 * PremiumAmount, PremiumAmount) AS PremiumAmount_Out,
		FullTermPremiumAmount,
		-- *INF*: IIF(MajorPerilCode = '599', 0.5 * FullTermPremiumAmount, FullTermPremiumAmount)
		IFF(MajorPerilCode = '599', 0.5 * FullTermPremiumAmount, FullTermPremiumAmount) AS FullTermPremiumAmount_Out,
		EarnedPremiumAmount,
		-- *INF*: IIF(MajorPerilCode = '599', 0.5 * EarnedPremiumAmount, EarnedPremiumAmount)
		-- 
		IFF(MajorPerilCode = '599', 0.5 * EarnedPremiumAmount, EarnedPremiumAmount) AS EarnedPremiumAmount_Out,
		ChangeInEarnedPremium AS ChangeInEarnedPremium4,
		-- *INF*: IIF(MajorPerilCode = '599', 0.5 * ChangeInEarnedPremium4, ChangeInEarnedPremium4)
		-- 
		IFF(MajorPerilCode = '599', 0.5 * ChangeInEarnedPremium4, ChangeInEarnedPremium4) AS ChangeInEarnedPremium_Out,
		symbol_pos_1_2 AS symbol_pos_1_2_out,
		'40' AS aslcode,
		'N/A' AS subaslcode,
		'N/A' AS Nonsubaslcode,
		ASLProduct_Code AS ASLProduct_Code4,
		Hierarchy_Product_Code AS Hierarchy_Product_Code4,
		StatisticalCoverageEffectiveDate AS StatisticalCoverageEffectiveDate4,
		StatisticalCoverageExpirationDate AS StatisticalCoverageExpirationDate4,
		StatisticalCoverageCancellationDate AS StatisticalCoverageCancellationDate4,
		RunDate AS RunDate4,
		PremiumMasterCalculationID AS PremiumMasterCalculationID4,
		AgencyAKID AS AgencyAKID4,
		PolicyAKID AS PolicyAKID4,
		strtgc_bus_dvsn_ak_id AS strtgc_bus_dvsn_ak_id4,
		ContractCustomerAKID AS ContractCustomerAKID4,
		RiskLocationAKID,
		PolicyCoverageAKID AS PolicyCoverageAKID4,
		PremiumTransactionAKID AS PremiumTransactionAKID4,
		BureauStatisticalCodeAKID AS BureauStatisticalCodeAKID4,
		PremiumMasterPolicyExpirationYear AS PremiumMasterPolicyExpirationYear4,
		PremiumMasterPolicyTerm AS PremiumMasterPolicyTerm4,
		PremiumMasterBureauPolicyType AS PremiumMasterBureauPolicyType4,
		PremiumMasterAuditCode AS PremiumMasterAuditCode4,
		PremiumMasterBureauStatisticalLine AS PremiumMasterBureauStatisticalLine4,
		PremiumMasterProductLine AS PremiumMasterProductLine4,
		PremiumMasterAgencyCommissionRate AS PremiumMasterAgencyCommissionRate4,
		PremiumMasterExposure AS PremiumMasterExposure4,
		PremiumMasterStatisticalCode1 AS PremiumMasterStatisticalCode14,
		PremiumMasterStatisticalCode2 AS PremiumMasterStatisticalCode24,
		PremiumMasterStatisticalCode3 AS PremiumMasterStatisticalCode34,
		PremiumMasterRateModifier AS PremiumMasterRateModifier4,
		PremiumMasterRateDeparture AS PremiumMasterRateDeparture4,
		PremiumMasterBureauInceptionDate AS PremiumMasterBureauInceptionDate4,
		PremiumMasterCountersignAgencyType AS PremiumMasterCountersignAgencyType4,
		PremiumMasterCountersignAgencyCode AS PremiumMasterCountersignAgencyCode4,
		PremiumMasterCountersignAgencyState AS PremiumMasterCountersignAgencyState4,
		PremiumMasterCountersignAgencyRate AS PremiumMasterCountersignAgencyRate4,
		PremiumMasterRenewalIndicator AS PremiumMasterRenewalIndicator4,
		PremiumMasterRecordType AS PremiumMasterRecordType4,
		ClassCode AS ClassCode4,
		SubLine AS SubLine4,
		premium_master_stage_id AS premium_master_stage_id4,
		pm_policy_number AS pm_policy_number4,
		pm_module AS pm_module4,
		pm_account_date AS pm_account_date4,
		pm_sar_location_number AS pm_sar_location_number4,
		pm_unit_number AS pm_unit_number4,
		pm_risk_state AS pm_risk_state4,
		pm_risk_zone_territory AS pm_risk_zone_territory4,
		pm_tax_location AS pm_tax_location4,
		pm_risk_zip_code_postal_zone AS pm_risk_zip_code_postal_zone4,
		pm_sar_insurance_line AS pm_sar_insurance_line4,
		pm_sar_sub_location_number AS pm_sar_sub_location_number4,
		pm_sar_risk_unit_group AS pm_sar_risk_unit_group4,
		pm_sar_class_code_group AS pm_sar_class_code_group4,
		pm_sar_class_code_member AS pm_sar_class_code_member4,
		pm_sar_sequence_risk_unit_n AS pm_sar_sequence_risk_unit_n4,
		pm_sar_sequence_risk_unit_a AS pm_sar_sequence_risk_unit_a4,
		pm_sar_type_exposure AS pm_sar_type_exposure4,
		pm_sar_mp_seq_no AS pm_sar_mp_seq_no4,
		pm_csp_inception_date AS pm_csp_inception_date4,
		pm_coverage_effective_date AS pm_coverage_effective_date4,
		pm_coverage_expiration_date AS pm_coverage_expiration_date4,
		pm_reins_ceded_premium AS pm_reins_ceded_premium4,
		-- *INF*: IIF(MajorPerilCode = '599', 0.5 * pm_reins_ceded_premium4, pm_reins_ceded_premium4)
		IFF(MajorPerilCode = '599', 0.5 * pm_reins_ceded_premium4, pm_reins_ceded_premium4) AS out_pm_reins_ceded_premium,
		pm_reins_ceded_original_premium AS pm_reins_ceded_original_premium4,
		-- *INF*: IIF(MajorPerilCode = '599', 0.5 * pm_reins_ceded_original_premium4, pm_reins_ceded_original_premium4)
		IFF(
		    MajorPerilCode = '599', 0.5 * pm_reins_ceded_original_premium4,
		    pm_reins_ceded_original_premium4
		) AS out_pm_reins_ceded_original_premium,
		pm_reinsurance_type_code AS pm_reinsurance_type_code4,
		pm_reinsurance_company_number AS pm_reinsurance_company_number4,
		pm_reinsurance_ratio AS pm_reinsurance_ratio4,
		AuditID AS AuditID4,
		ProductCode AS ProductCode4,
		RatingCoverageEffectiveDate AS RatingCoverageEffectiveDate4,
		RatingCoverageExpirationDate AS RatingCoverageExpirationDate4,
		RatingCoverageCancellationDate AS RatingCoverageCancellationDate4,
		RatingCoverageAKID AS RatingCoverageAKID4,
		PolicyOfferingCode AS PolicyOfferingCode4,
		PolicyCoverageEffectiveDate AS PolicyCoverageEffectiveDate9,
		PolicyCoverageExpirationDate AS PolicyCoverageExpirationDate9,
		AgencyActualCommissionRate AS AgencyActualCommissionRate4,
		InsuranceReferenceLineOfBusinessCode AS InsuranceReferenceLineOfBusinessCode4,
		EnterpriseGroupCode AS EnterpriseGroupCode4,
		InsuranceReferenceLegalEntityCode AS InsuranceReferenceLegalEntityCode4,
		StrategicProfitCenterCode AS StrategicProfitCenterCode4,
		InsuranceSegmentCode AS InsuranceSegmentCode4,
		Risk_Unit_Group AS Risk_Unit_Group4,
		StandardInsuranceLineCode AS StandardInsuranceLineCode4,
		RatingCoverage AS RatingCoverage4,
		RiskType AS RiskType4,
		CoverageType AS CoverageType4,
		StandardSpecialClassGroupCode AS StandardSpecialClassGroupCode4,
		StandardIncreasedLimitGroupCode AS StandardIncreasedLimitGroupCode4,
		StandardPackageModifcationAdjustmentGroupCode AS StandardPackageModifcationAdjustmentGroupCode4,
		SourceSystemID AS SourceSystemID4,
		EarnedExposure AS EarnedExposure4,
		ChangeInEarnedExposure AS ChangeInEarnedExposure4,
		RiskLocationHashKey AS RiskLocationHashKey4,
		PerilGroup,
		CoverageForm AS CoverageForm4,
		PolicyAuditAKID11 AS PolicyAuditAKID114,
		PolicyAuditEffectiveDate11 AS PolicyAuditEffectiveDate114,
		SubCoverageTypeCode AS SubCoverageTypeCode4,
		CoverageVersion AS CoverageVersion4,
		CustomerCareCommissionRate AS CustomerCareCommissionRate4,
		RatingPlanCode AS RatingPlanCode4,
		CoverageCancellationDate AS CoverageCancellationDate4,
		GeneratedRecordIndicator AS GeneratedRecordIndicator4,
		DirectWrittenPremium AS i_DirectWrittenPremium4,
		RatablePremium AS i_RatablePremium4,
		ClassifiedPremium AS i_ClassifiedPremium4,
		OtherModifiedPremium AS i_OtherModifiedPremium4,
		ScheduleModifiedPremium AS i_ScheduleModifiedPremium4,
		ExperienceModifiedPremium AS i_ExperienceModifiedPremium4,
		SubjectWrittenPremium AS i_SubjectWrittenPremium4,
		-- *INF*: IIF(MajorPerilCode = '599', 0.5 * i_DirectWrittenPremium4, i_DirectWrittenPremium4)
		IFF(MajorPerilCode = '599', 0.5 * i_DirectWrittenPremium4, i_DirectWrittenPremium4) AS o_DirectWrittenPremium4,
		-- *INF*: IIF(MajorPerilCode = '599', 0.5 * i_RatablePremium4, i_RatablePremium4)
		IFF(MajorPerilCode = '599', 0.5 * i_RatablePremium4, i_RatablePremium4) AS o_RatablePremium4,
		-- *INF*: IIF(MajorPerilCode = '599', 0.5 * i_ClassifiedPremium4, i_ClassifiedPremium4)
		IFF(MajorPerilCode = '599', 0.5 * i_ClassifiedPremium4, i_ClassifiedPremium4) AS o_ClassifiedPremium4,
		-- *INF*: IIF(MajorPerilCode = '599', 0.5 * i_OtherModifiedPremium4, i_OtherModifiedPremium4)
		-- 
		IFF(MajorPerilCode = '599', 0.5 * i_OtherModifiedPremium4, i_OtherModifiedPremium4) AS o_OtherModifiedPremium4,
		-- *INF*: IIF(MajorPerilCode = '599', 0.5 * i_ScheduleModifiedPremium4, i_ScheduleModifiedPremium4)
		IFF(MajorPerilCode = '599', 0.5 * i_ScheduleModifiedPremium4, i_ScheduleModifiedPremium4) AS o_ScheduleModifiedPremium4,
		-- *INF*: IIF(MajorPerilCode = '599', 0.5 * i_ExperienceModifiedPremium4, i_ExperienceModifiedPremium4)
		IFF(
		    MajorPerilCode = '599', 0.5 * i_ExperienceModifiedPremium4, i_ExperienceModifiedPremium4
		) AS o_ExperienceModifiedPremium4,
		-- *INF*: IIF(MajorPerilCode = '599', 0.5 * i_SubjectWrittenPremium4, i_SubjectWrittenPremium4)
		IFF(MajorPerilCode = '599', 0.5 * i_SubjectWrittenPremium4, i_SubjectWrittenPremium4) AS o_SubjectWrittenPremium4,
		EarnedDirectWrittenPremium AS EarnedDirectWrittenPremium4,
		EarnedClassifiedPremium AS EarnedClassifiedPremium4,
		EarnedRatablePremium AS EarnedRatablePremium4,
		EarnedOtherModifiedPremium AS EarnedOtherModifiedPremium4,
		EarnedScheduleModifiedPremium AS EarnedScheduleModifiedPremium4,
		EarnedExperienceModifiedPremium AS EarnedExperienceModifiedPremium4,
		EarnedSubjectWrittenPremium AS EarnedSubjectWrittenPremium4,
		EarnedPremiumRunDate AS EarnedPremiumRunDate4,
		PremiumMasterWrittenExposure AS PremiumMasterWrittenExposure4,
		DeclaredEventFlag AS DeclaredEventFlag4
		FROM RTR_Split_Transactions_asl_20
	),
	EXP1_ASL_Level_Row AS (
		SELECT
		PolicyKey AS PolicyKey1,
		PolicyEffectiveDate AS PolicyEffectiveDate1,
		PolicyExpirationDate AS PolicyExpirationDate1,
		PremiumTransactionID AS PremiumTransactionID1,
		ReinsuranceCoverageAKID AS ReinsuranceCoverageAKID1,
		StatisticalCoverageAKID AS StatisticalCoverageAKID1,
		PremiumTransactionCode AS PremiumTransactionCode1,
		PremiumTransactionEnteredDate AS PremiumTransactionEnteredDate1,
		PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate1,
		PremiumTransactionExpirationDate AS PremiumTransactionExpirationDate1,
		PremiumTransactionBookedDate AS PremiumTransactionBookedDate1,
		PremiumType AS PremiumType1,
		ReasonAmendedCode AS ReasonAmendedCode1,
		PolicySymbol,
		TypeBureauCode,
		MajorPerilCode,
		RiskUnit,
		RiskUnitSequenceNumber AS RiskUnitSequenceNumber1,
		nsi_indicator,
		symbol_pos_1_2,
		PremiumAmount,
		-- *INF*: DECODE(TRUE,
		-- aslcode= '20' AND MajorPerilCode = '599', 0.5 * PremiumAmount, 
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (0.65) * PremiumAmount,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (0.65) * PremiumAmount,
		-- PremiumAmount)
		DECODE(
		    TRUE,
		    aslcode = '20' AND MajorPerilCode = '599', 0.5 * PremiumAmount,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (0.65) * PremiumAmount,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (0.65) * PremiumAmount,
		    PremiumAmount
		) AS PremiumAmount_Out,
		FullTermPremiumAmount,
		-- *INF*: DECODE(TRUE,
		-- aslcode= '20' AND MajorPerilCode = '599', 0.5 * FullTermPremiumAmount, 
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (0.65) * FullTermPremiumAmount,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (0.65) * FullTermPremiumAmount,
		-- FullTermPremiumAmount)
		DECODE(
		    TRUE,
		    aslcode = '20' AND MajorPerilCode = '599', 0.5 * FullTermPremiumAmount,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (0.65) * FullTermPremiumAmount,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (0.65) * FullTermPremiumAmount,
		    FullTermPremiumAmount
		) AS FullTermPremiumAmount_Out,
		EarnedPremiumAmount,
		-- *INF*: DECODE(TRUE,
		-- aslcode= '20' AND MajorPerilCode = '599', 0.5 * EarnedPremiumAmount, 
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (0.65) * EarnedPremiumAmount,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (0.65) * EarnedPremiumAmount,
		-- EarnedPremiumAmount)
		DECODE(
		    TRUE,
		    aslcode = '20' AND MajorPerilCode = '599', 0.5 * EarnedPremiumAmount,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (0.65) * EarnedPremiumAmount,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (0.65) * EarnedPremiumAmount,
		    EarnedPremiumAmount
		) AS EarnedPremiumAmount_out,
		ChangeInEarnedPremium,
		-- *INF*: DECODE(TRUE,
		-- aslcode= '20' AND MajorPerilCode = '599', 0.5 * ChangeInEarnedPremium, 
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (0.65) * ChangeInEarnedPremium,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (0.65) * ChangeInEarnedPremium,
		-- ChangeInEarnedPremium)
		DECODE(
		    TRUE,
		    aslcode = '20' AND MajorPerilCode = '599', 0.5 * ChangeInEarnedPremium,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (0.65) * ChangeInEarnedPremium,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (0.65) * ChangeInEarnedPremium,
		    ChangeInEarnedPremium
		) AS ChangeInEarnedPremium_out,
		aslcode,
		subaslcode,
		-- *INF*: IIF(subaslcode='421',subaslcode ,'N/A')
		IFF(subaslcode = '421', subaslcode, 'N/A') AS subaslcode_out,
		Nonsubaslcode,
		-- *INF*: IIF(Nonsubaslcode='421',Nonsubaslcode,'N/A')
		IFF(Nonsubaslcode = '421', Nonsubaslcode, 'N/A') AS Nonsubaslcode_out,
		ASLProduct_Code AS ASLProduct_Code1,
		Hierarchy_Product_Code AS Hierarchy_Product_Code1,
		StatisticalCoverageEffectiveDate AS StatisticalCoverageEffectiveDate1,
		StatisticalCoverageExpirationDate,
		StatisticalCoverageCancellationDate,
		RunDate AS RunDate1,
		PremiumMasterCalculationID AS PremiumMasterCalculationID1,
		AgencyAKID AS AgencyAKID1,
		PolicyAKID AS PolicyAKID1,
		strtgc_bus_dvsn_ak_id AS strtgc_bus_dvsn_ak_id1,
		ContractCustomerAKID AS ContractCustomerAKID1,
		RiskLocationAKID,
		PolicyCoverageAKID AS PolicyCoverageAKID1,
		PremiumTransactionAKID AS PremiumTransactionAKID1,
		BureauStatisticalCodeAKID AS BureauStatisticalCodeAKID1,
		PremiumMasterPolicyExpirationYear AS PremiumMasterPolicyExpirationYear1,
		PremiumMasterPolicyTerm AS PremiumMasterPolicyTerm1,
		PremiumMasterBureauPolicyType AS PremiumMasterBureauPolicyType1,
		PremiumMasterAuditCode AS PremiumMasterAuditCode1,
		PremiumMasterBureauStatisticalLine AS PremiumMasterBureauStatisticalLine1,
		PremiumMasterProductLine AS PremiumMasterProductLine1,
		PremiumMasterAgencyCommissionRate AS PremiumMasterAgencyCommissionRate1,
		PremiumMasterExposure AS PremiumMasterExposure1,
		PremiumMasterStatisticalCode AS PremiumMasterStatisticalCode11,
		PremiumMasterStatisticalCode2 AS PremiumMasterStatisticalCode21,
		PremiumMasterStatisticalCode3 AS PremiumMasterStatisticalCode31,
		PremiumMasterRateModifier AS PremiumMasterRateModifier1,
		PremiumMasterRateDeparture AS PremiumMasterRateDeparture1,
		PremiumMasterBureauInceptionDate AS PremiumMasterBureauInceptionDate1,
		PremiumMasterCountersignAgencyType AS PremiumMasterCountersignAgencyType1,
		PremiumMasterCountersignAgencyCode AS PremiumMasterCountersignAgencyCode1,
		PremiumMasterCountersignAgencyState AS PremiumMasterCountersignAgencyState1,
		PremiumMasterCountersignAgencyRate AS PremiumMasterCountersignAgencyRate1,
		PremiumMasterRenewalIndicator AS PremiumMasterRenewalIndicator1,
		PremiumMasterRecordType AS PremiumMasterRecordType1,
		ClassCode AS ClassCode1,
		SubLine AS SubLine1,
		premium_master_stage_id AS premium_master_stage_id1,
		pm_policy_number AS pm_policy_number1,
		pm_module AS pm_module1,
		pm_account_date AS pm_account_date1,
		pm_sar_location_number AS pm_sar_location_number1,
		pm_unit_number AS pm_unit_number1,
		pm_risk_state AS pm_risk_state1,
		pm_risk_zone_territory AS pm_risk_zone_territory1,
		pm_tax_location AS pm_tax_location1,
		pm_risk_zip_code_postal_zone AS pm_risk_zip_code_postal_zone1,
		pm_sar_insurance_line AS pm_sar_insurance_line1,
		pm_sar_sub_location_number AS pm_sar_sub_location_number1,
		pm_sar_risk_unit_group AS pm_sar_risk_unit_group1,
		pm_sar_class_code_group AS pm_sar_class_code_group1,
		pm_sar_class_code_member AS pm_sar_class_code_member1,
		pm_sar_sequence_risk_unit_n AS pm_sar_sequence_risk_unit_n1,
		pm_sar_sequence_risk_unit_a AS pm_sar_sequence_risk_unit_a1,
		pm_sar_type_exposure AS pm_sar_type_exposure1,
		pm_sar_mp_seq_no AS pm_sar_mp_seq_no1,
		pm_csp_inception_date AS pm_csp_inception_date1,
		pm_coverage_effective_date AS pm_coverage_effective_date1,
		pm_coverage_expiration_date AS pm_coverage_expiration_date1,
		pm_reins_ceded_premium AS pm_reins_ceded_premium1,
		-- *INF*: DECODE(TRUE,
		-- aslcode= '20' AND MajorPerilCode = '599', 0.5 * pm_reins_ceded_premium1, 
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (0.65) * pm_reins_ceded_premium1,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (0.65) * pm_reins_ceded_premium1,pm_reins_ceded_premium1)
		DECODE(
		    TRUE,
		    aslcode = '20' AND MajorPerilCode = '599', 0.5 * pm_reins_ceded_premium1,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (0.65) * pm_reins_ceded_premium1,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (0.65) * pm_reins_ceded_premium1,
		    pm_reins_ceded_premium1
		) AS out_pm_reins_ceded_premium,
		pm_reins_ceded_original_premium AS pm_reins_ceded_original_premium1,
		-- *INF*: DECODE(TRUE,
		-- aslcode= '20' AND MajorPerilCode = '599', 0.5 * pm_reins_ceded_original_premium1, 
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (0.65) * pm_reins_ceded_original_premium1,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (0.65) * pm_reins_ceded_original_premium1, pm_reins_ceded_original_premium1)
		DECODE(
		    TRUE,
		    aslcode = '20' AND MajorPerilCode = '599', 0.5 * pm_reins_ceded_original_premium1,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (0.65) * pm_reins_ceded_original_premium1,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (0.65) * pm_reins_ceded_original_premium1,
		    pm_reins_ceded_original_premium1
		) AS out_pm_reins_ceded_original_premium,
		pm_reinsurance_type_code AS pm_reinsurance_type_code1,
		pm_reinsurance_company_number AS pm_reinsurance_company_number1,
		pm_reinsurance_ratio AS pm_reinsurance_ratio1,
		AuditID AS AuditID1,
		ProductCode AS ProductCode1,
		RatingCoverageEffectiveDate AS RatingCoverageEffectiveDate1,
		RatingCoverageExpirationDate AS RatingCoverageExpirationDate1,
		RatingCoverageCancellationDate AS RatingCoverageCancellationDate1,
		RatingCoverageAKID AS RatingCoverageAKID1,
		PolicyOfferingCode AS PolicyOfferingCode1,
		PolicyCoverageEffectiveDate AS PolicyCoverageEffectiveDate1,
		PolicyCoverageExpirationDate AS PolicyCoverageExpirationDate1,
		AgencyActualCommissionRate AS AgencyActualCommissionRate1,
		InsuranceReferenceLineOfBusinessCode AS InsuranceReferenceLineOfBusinessCode1,
		EnterpriseGroupCode AS EnterpriseGroupCode1,
		InsuranceReferenceLegalEntityCode AS InsuranceReferenceLegalEntityCode1,
		StrategicProfitCenterCode AS StrategicProfitCenterCode1,
		InsuranceSegmentCode AS InsuranceSegmentCode1,
		Risk_Unit_Group AS Risk_Unit_Group1,
		StandardInsuranceLineCode AS StandardInsuranceLineCode1,
		RatingCoverage AS RatingCoverage1,
		RiskType AS RiskType1,
		CoverageType AS CoverageType1,
		StandardSpecialClassGroupCode AS StandardSpecialClassGroupCode1,
		StandardIncreasedLimitGroupCode AS StandardIncreasedLimitGroupCode1,
		StandardPackageModifcationAdjustmentGroupCode AS StandardPackageModifcationAdjustmentGroupCode1,
		SourceSystemID AS SourceSystemID1,
		EarnedExposure AS EarnedExposure1,
		ChangeInEarnedExposure AS ChangeInEarnedExposure1,
		RiskLocationHashKey AS RiskLocationHashKey1,
		PerilGroup,
		CoverageForm AS CoverageForm1,
		PolicyAuditAKID AS PolicyAuditAKID111,
		PolicyAuditEffectiveDate AS PolicyAuditEffectiveDate111,
		SubCoverageTypeCode AS SubCoverageTypeCode1,
		CoverageVersion AS CoverageVersion1,
		CustomerCareCommissionRate AS CustomerCareCommissionRate1,
		RatingPlanCode AS RatingPlanCode1,
		CoverageCancellationDate AS CoverageCancellationDate1,
		GeneratedRecordIndicator AS GeneratedRecordIndicator1,
		DirectWrittenPremium AS i_DirectWrittenPremium1,
		RatablePremium AS i_RatablePremium1,
		ClassifiedPremium AS i_ClassifiedPremium1,
		OtherModifiedPremium AS i_OtherModifiedPremium1,
		ScheduleModifiedPremium AS i_ScheduleModifiedPremium1,
		ExperienceModifiedPremium AS i_ExperienceModifiedPremium1,
		SubjectWrittenPremium AS i_SubjectWrittenPremium1,
		-- *INF*: DECODE(TRUE,
		-- aslcode= '20' AND MajorPerilCode = '599', 0.5 * i_DirectWrittenPremium1, 
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (0.65) * i_DirectWrittenPremium1,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (0.65) * i_DirectWrittenPremium1,
		-- i_DirectWrittenPremium1)
		DECODE(
		    TRUE,
		    aslcode = '20' AND MajorPerilCode = '599', 0.5 * i_DirectWrittenPremium1,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (0.65) * i_DirectWrittenPremium1,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (0.65) * i_DirectWrittenPremium1,
		    i_DirectWrittenPremium1
		) AS o_DirectWrittenPremium1,
		-- *INF*: DECODE(TRUE,
		-- aslcode= '20' AND MajorPerilCode = '599', 0.5 * i_RatablePremium1, 
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (0.65) * i_RatablePremium1,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (0.65) * i_RatablePremium1,
		-- i_RatablePremium1)
		DECODE(
		    TRUE,
		    aslcode = '20' AND MajorPerilCode = '599', 0.5 * i_RatablePremium1,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (0.65) * i_RatablePremium1,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (0.65) * i_RatablePremium1,
		    i_RatablePremium1
		) AS o_RatablePremium1,
		-- *INF*: DECODE(TRUE,
		-- aslcode= '20' AND MajorPerilCode = '599', 0.5 * i_ClassifiedPremium1, 
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (0.65) * i_ClassifiedPremium1,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (0.65) * i_ClassifiedPremium1,
		-- i_ClassifiedPremium1)
		DECODE(
		    TRUE,
		    aslcode = '20' AND MajorPerilCode = '599', 0.5 * i_ClassifiedPremium1,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (0.65) * i_ClassifiedPremium1,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (0.65) * i_ClassifiedPremium1,
		    i_ClassifiedPremium1
		) AS o_ClassifiedPremium1,
		-- *INF*: DECODE(TRUE,
		-- aslcode= '20' AND MajorPerilCode = '599', 0.5 * i_OtherModifiedPremium1, 
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (0.65) * i_OtherModifiedPremium1,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (0.65) * i_OtherModifiedPremium1,
		-- i_OtherModifiedPremium1)
		DECODE(
		    TRUE,
		    aslcode = '20' AND MajorPerilCode = '599', 0.5 * i_OtherModifiedPremium1,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (0.65) * i_OtherModifiedPremium1,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (0.65) * i_OtherModifiedPremium1,
		    i_OtherModifiedPremium1
		) AS o_OtherModifiedPremium1,
		-- *INF*: DECODE(TRUE,
		-- aslcode= '20' AND MajorPerilCode = '599', 0.5 * i_ScheduleModifiedPremium1, 
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (0.65) * i_ScheduleModifiedPremium1,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (0.65) * i_ScheduleModifiedPremium1,
		-- i_ScheduleModifiedPremium1)
		DECODE(
		    TRUE,
		    aslcode = '20' AND MajorPerilCode = '599', 0.5 * i_ScheduleModifiedPremium1,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (0.65) * i_ScheduleModifiedPremium1,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (0.65) * i_ScheduleModifiedPremium1,
		    i_ScheduleModifiedPremium1
		) AS o_ScheduleModifiedPremium1,
		-- *INF*: DECODE(TRUE,
		-- aslcode= '20' AND MajorPerilCode = '599', 0.5 * i_ExperienceModifiedPremium1, 
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (0.65) * i_ExperienceModifiedPremium1,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (0.65) * i_ExperienceModifiedPremium1,
		-- i_ExperienceModifiedPremium1)
		DECODE(
		    TRUE,
		    aslcode = '20' AND MajorPerilCode = '599', 0.5 * i_ExperienceModifiedPremium1,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (0.65) * i_ExperienceModifiedPremium1,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (0.65) * i_ExperienceModifiedPremium1,
		    i_ExperienceModifiedPremium1
		) AS o_ExperienceModifiedPremium1,
		-- *INF*: DECODE(TRUE,
		-- aslcode= '20' AND MajorPerilCode = '599', 0.5 * i_SubjectWrittenPremium1, 
		-- IN (symbol_pos_1_2,'BG','BH') AND IN (MajorPerilCode,'901','902') AND TypeBureauCode = 'CF', (0.65) * i_SubjectWrittenPremium1,
		-- IN (symbol_pos_1_2,'BA','BB') AND IN (MajorPerilCode,'901','902','599') AND IN(TypeBureauCode,'BB','BE','BC'), (0.65) * i_SubjectWrittenPremium1,
		-- i_SubjectWrittenPremium1)
		DECODE(
		    TRUE,
		    aslcode = '20' AND MajorPerilCode = '599', 0.5 * i_SubjectWrittenPremium1,
		    symbol_pos_1_2 IN ('BG','BH') AND MajorPerilCode IN ('901','902') AND TypeBureauCode = 'CF', (0.65) * i_SubjectWrittenPremium1,
		    symbol_pos_1_2 IN ('BA','BB') AND MajorPerilCode IN ('901','902','599') AND TypeBureauCode IN ('BB','BE','BC'), (0.65) * i_SubjectWrittenPremium1,
		    i_SubjectWrittenPremium1
		) AS o_SubjectWrittenPremium1,
		EarnedDirectWrittenPremium AS EarnedDirectWrittenPremium1,
		EarnedClassifiedPremium AS EarnedClassifiedPremium1,
		EarnedRatablePremium AS EarnedRatablePremium1,
		EarnedOtherModifiedPremium AS EarnedOtherModifiedPremium1,
		EarnedScheduleModifiedPremium AS EarnedScheduleModifiedPremium1,
		EarnedExperienceModifiedPremium AS EarnedExperienceModifiedPremium1,
		EarnedSubjectWrittenPremium AS EarnedSubjectWrittenPremium1,
		EarnedPremiumRunDate AS EarnedPremiumRunDate1,
		PremiumMasterWrittenExposure AS PremiumMasterWrittenExposure1,
		DeclaredEventFlag AS DeclaredEventFlag1
		FROM RTR_Split_Transactions_asl_Level
	),
	EXP_NonSubASL_320_Level_Row AS (
		SELECT
		PolicyKey AS PolicyKey8,
		PolicyEffectiveDate AS PolicyEffectiveDate8,
		PolicyExpirationDate AS PolicyExpirationDate8,
		PremiumTransactionID AS PremiumTransactionID5,
		ReinsuranceCoverageAKID AS ReinsuranceCoverageAKID5,
		StatisticalCoverageAKID AS StatisticalCoverageAKID5,
		PremiumTransactionCode AS PremiumTransactionCode5,
		PremiumTransactionEnteredDate AS PremiumTransactionEnteredDate5,
		PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate5,
		PremiumTransactionExpirationDate AS PremiumTransactionExpirationDate5,
		PremiumTransactionBookedDate AS PremiumTransactionBookedDate5,
		PremiumType AS PremiumType5,
		ReasonAmendedCode AS ReasonAmendedCode5,
		PolicySymbol,
		TypeBureauCode,
		MajorPerilCode,
		RiskUnit,
		RiskUnitSequenceNumber AS RiskUnitSequenceNumber8,
		nsi_indicator,
		symbol_pos_1_2 AS symbol_pos_1_2_out,
		PremiumAmount,
		-- *INF*: IIF(MajorPerilCode = '100', (0.32) * PremiumAmount, PremiumAmount)
		IFF(MajorPerilCode = '100', (0.32) * PremiumAmount, PremiumAmount) AS PremiumAmount_Out,
		FullTermPremiumAmount,
		-- *INF*: IIF(MajorPerilCode = '100', (0.32) * FullTermPremiumAmount, FullTermPremiumAmount)
		IFF(MajorPerilCode = '100', (0.32) * FullTermPremiumAmount, FullTermPremiumAmount) AS FullTermPremiumAmount_Out,
		EarnedPremiumAmount,
		-- *INF*: IIF(MajorPerilCode = '100', (0.32) * EarnedPremiumAmount, EarnedPremiumAmount)
		IFF(MajorPerilCode = '100', (0.32) * EarnedPremiumAmount, EarnedPremiumAmount) AS EarnedPremiumAmount_Out,
		ChangeInEarnedPremium,
		-- *INF*: IIF(MajorPerilCode = '100', (0.32) * ChangeInEarnedPremium, ChangeInEarnedPremium)
		IFF(MajorPerilCode = '100', (0.32) * ChangeInEarnedPremium, ChangeInEarnedPremium) AS ChangeInEarnedPremium_Out,
		'260' AS aslcode,
		'280' AS subaslcode,
		'320' AS Nonsubaslcode,
		ASLProduct_Code AS ASLProduct_Code8,
		Hierarchy_Product_Code AS Hierarchy_Product_Code8,
		StatisticalCoverageEffectiveDate AS StatisticalCoverageEffectiveDate8,
		StatisticalCoverageExpirationDate AS StatisticalCoverageExpirationDate8,
		StatisticalCoverageCancellationDate AS StatisticalCoverageCancellationDate8,
		RunDate AS RunDate8,
		PremiumMasterCalculationID AS PremiumMasterCalculationID8,
		AgencyAKID AS AgencyAKID8,
		PolicyAKID AS PolicyAKID8,
		strtgc_bus_dvsn_ak_id AS strtgc_bus_dvsn_ak_id8,
		ContractCustomerAKID AS ContractCustomerAKID8,
		RiskLocationAKID,
		PolicyCoverageAKID AS PolicyCoverageAKID8,
		PremiumTransactionAKID AS PremiumTransactionAKID8,
		BureauStatisticalCodeAKID AS BureauStatisticalCodeAKID8,
		PremiumMasterPolicyExpirationYear AS PremiumMasterPolicyExpirationYear8,
		PremiumMasterPolicyTerm AS PremiumMasterPolicyTerm8,
		PremiumMasterBureauPolicyType AS PremiumMasterBureauPolicyType8,
		PremiumMasterAuditCode AS PremiumMasterAuditCode8,
		PremiumMasterBureauStatisticalLine AS PremiumMasterBureauStatisticalLine8,
		PremiumMasterProductLine AS PremiumMasterProductLine8,
		PremiumMasterAgencyCommissionRate AS PremiumMasterAgencyCommissionRate8,
		PremiumMasterExposure AS PremiumMasterExposure8,
		PremiumMasterStatisticalCode1 AS PremiumMasterStatisticalCode18,
		PremiumMasterStatisticalCode2 AS PremiumMasterStatisticalCode28,
		PremiumMasterStatisticalCode3 AS PremiumMasterStatisticalCode38,
		PremiumMasterRateModifier AS PremiumMasterRateModifier8,
		PremiumMasterRateDeparture AS PremiumMasterRateDeparture8,
		PremiumMasterBureauInceptionDate AS PremiumMasterBureauInceptionDate8,
		PremiumMasterCountersignAgencyType AS PremiumMasterCountersignAgencyType8,
		PremiumMasterCountersignAgencyCode AS PremiumMasterCountersignAgencyCode8,
		PremiumMasterCountersignAgencyState AS PremiumMasterCountersignAgencyState8,
		PremiumMasterCountersignAgencyRate AS PremiumMasterCountersignAgencyRate8,
		PremiumMasterRenewalIndicator AS PremiumMasterRenewalIndicator8,
		PremiumMasterRecordType AS PremiumMasterRecordType8,
		ClassCode AS ClassCode8,
		SubLine AS SubLine8,
		premium_master_stage_id AS premium_master_stage_id8,
		pm_policy_number AS pm_policy_number8,
		pm_module AS pm_module8,
		pm_account_date AS pm_account_date8,
		pm_sar_location_number AS pm_sar_location_number8,
		pm_unit_number AS pm_unit_number8,
		pm_risk_state AS pm_risk_state8,
		pm_risk_zone_territory AS pm_risk_zone_territory8,
		pm_tax_location AS pm_tax_location8,
		pm_risk_zip_code_postal_zone AS pm_risk_zip_code_postal_zone8,
		pm_sar_insurance_line AS pm_sar_insurance_line8,
		pm_sar_sub_location_number AS pm_sar_sub_location_number8,
		pm_sar_risk_unit_group AS pm_sar_risk_unit_group8,
		pm_sar_class_code_group AS pm_sar_class_code_group8,
		pm_sar_class_code_member AS pm_sar_class_code_member8,
		pm_sar_sequence_risk_unit_n AS pm_sar_sequence_risk_unit_n8,
		pm_sar_sequence_risk_unit_a AS pm_sar_sequence_risk_unit_a8,
		pm_sar_type_exposure AS pm_sar_type_exposure8,
		pm_sar_mp_seq_no AS pm_sar_mp_seq_no8,
		pm_csp_inception_date AS pm_csp_inception_date8,
		pm_coverage_effective_date AS pm_coverage_effective_date8,
		pm_coverage_expiration_date AS pm_coverage_expiration_date8,
		pm_reins_ceded_premium AS pm_reins_ceded_premium8,
		-- *INF*: IIF(MajorPerilCode = '100', (0.32) * pm_reins_ceded_premium8, pm_reins_ceded_premium8)
		IFF(MajorPerilCode = '100', (0.32) * pm_reins_ceded_premium8, pm_reins_ceded_premium8) AS out_pm_reins_ceded_premium,
		pm_reins_ceded_original_premium AS pm_reins_ceded_original_premium8,
		-- *INF*: IIF(MajorPerilCode = '100', (0.32) * pm_reins_ceded_original_premium8, pm_reins_ceded_original_premium8)
		IFF(
		    MajorPerilCode = '100', (0.32) * pm_reins_ceded_original_premium8,
		    pm_reins_ceded_original_premium8
		) AS out_pm_reins_ceded_original_premium,
		pm_reinsurance_type_code AS pm_reinsurance_type_code8,
		pm_reinsurance_company_number AS pm_reinsurance_company_number8,
		pm_reinsurance_ratio AS pm_reinsurance_ratio8,
		AuditID AS AuditID8,
		ProductCode AS ProductCode8,
		RatingCoverageEffectiveDate AS RatingCoverageEffectiveDate8,
		RatingCoverageExpirationDate AS RatingCoverageExpirationDate8,
		RatingCoverageCancellationDate AS RatingCoverageCancellationDate8,
		RatingCoverageAKID AS RatingCoverageAKID8,
		PolicyOfferingCode AS PolicyOfferingCode8,
		PolicyCoverageEffectiveDate AS PolicyCoverageEffectiveDate8,
		PolicyCoverageExpirationDate AS PolicyCoverageExpirationDate8,
		AgencyActualCommissionRate AS AgencyActualCommissionRate8,
		InsuranceReferenceLineOfBusinessCode AS InsuranceReferenceLineOfBusinessCode8,
		EnterpriseGroupCode AS EnterpriseGroupCode8,
		InsuranceReferenceLegalEntityCode AS InsuranceReferenceLegalEntityCode8,
		StrategicProfitCenterCode AS StrategicProfitCenterCode8,
		InsuranceSegmentCode AS InsuranceSegmentCode8,
		Risk_Unit_Group AS Risk_Unit_Group8,
		StandardInsuranceLineCode AS StandardInsuranceLineCode8,
		RatingCoverage AS RatingCoverage8,
		RiskType AS RiskType8,
		CoverageType AS CoverageType8,
		StandardSpecialClassGroupCode AS StandardSpecialClassGroupCode8,
		StandardIncreasedLimitGroupCode AS StandardIncreasedLimitGroupCode8,
		StandardPackageModifcationAdjustmentGroupCode AS StandardPackageModifcationAdjustmentGroupCode8,
		SourceSystemID AS SourceSystemID8,
		EarnedExposure AS EarnedExposure8,
		ChangeInEarnedExposure AS ChangeInEarnedExposure8,
		RiskLocationHashKey AS RiskLocationHashKey8,
		PerilGroup,
		CoverageForm AS CoverageForm8,
		PolicyAuditAKID11 AS PolicyAuditAKID118,
		PolicyAuditEffectiveDate11 AS PolicyAuditEffectiveDate118,
		SubCoverageTypeCode AS SubCoverageTypeCode8,
		CoverageVersion AS CoverageVersion8,
		CustomerCareCommissionRate AS CustomerCareCommissionRate8,
		RatingPlanCode AS RatingPlanCode8,
		CoverageCancellationDate AS CoverageCancellationDate8,
		GeneratedRecordIndicator AS GeneratedRecordIndicator8,
		DirectWrittenPremium AS i_DirectWrittenPremium8,
		RatablePremium AS i_RatablePremium8,
		ClassifiedPremium AS i_ClassifiedPremium8,
		OtherModifiedPremium AS i_OtherModifiedPremium8,
		ScheduleModifiedPremium AS i_ScheduleModifiedPremium8,
		ExperienceModifiedPremium AS i_ExperienceModifiedPremium8,
		SubjectWrittenPremium AS i_SubjectWrittenPremium8,
		-- *INF*: IIF(MajorPerilCode = '100', (0.32) * i_DirectWrittenPremium8, i_DirectWrittenPremium8)
		IFF(MajorPerilCode = '100', (0.32) * i_DirectWrittenPremium8, i_DirectWrittenPremium8) AS o_DirectWrittenPremium8,
		-- *INF*: IIF(MajorPerilCode = '100', (0.32) * i_RatablePremium8, i_RatablePremium8)
		IFF(MajorPerilCode = '100', (0.32) * i_RatablePremium8, i_RatablePremium8) AS o_RatablePremium8,
		-- *INF*: IIF(MajorPerilCode = '100', (0.32) * i_ClassifiedPremium8, i_ClassifiedPremium8)
		IFF(MajorPerilCode = '100', (0.32) * i_ClassifiedPremium8, i_ClassifiedPremium8) AS o_ClassifiedPremium8,
		-- *INF*: IIF(MajorPerilCode = '100', (0.32) * i_OtherModifiedPremium8, i_OtherModifiedPremium8)
		IFF(MajorPerilCode = '100', (0.32) * i_OtherModifiedPremium8, i_OtherModifiedPremium8) AS o_OtherModifiedPremium8,
		-- *INF*: IIF(MajorPerilCode = '100', (0.32) * i_ScheduleModifiedPremium8, i_ScheduleModifiedPremium8)
		IFF(MajorPerilCode = '100', (0.32) * i_ScheduleModifiedPremium8, i_ScheduleModifiedPremium8) AS o_ScheduleModifiedPremium8,
		-- *INF*: IIF(MajorPerilCode = '100', (0.32) * i_ExperienceModifiedPremium8, i_ExperienceModifiedPremium8)
		IFF(
		    MajorPerilCode = '100', (0.32) * i_ExperienceModifiedPremium8, i_ExperienceModifiedPremium8
		) AS o_ExperienceModifiedPremium8,
		-- *INF*: IIF(MajorPerilCode = '100', (0.32) * i_SubjectWrittenPremium8, i_SubjectWrittenPremium8)
		IFF(MajorPerilCode = '100', (0.32) * i_SubjectWrittenPremium8, i_SubjectWrittenPremium8) AS o_SubjectWrittenPremium8,
		EarnedDirectWrittenPremium AS EarnedDirectWrittenPremium8,
		EarnedClassifiedPremium AS EarnedClassifiedPremium8,
		EarnedRatablePremium AS EarnedRatablePremium8,
		EarnedOtherModifiedPremium AS EarnedOtherModifiedPremium8,
		EarnedScheduleModifiedPremium AS EarnedScheduleModifiedPremium8,
		EarnedExperienceModifiedPremium AS EarnedExperienceModifiedPremium8,
		EarnedSubjectWrittenPremium AS EarnedSubjectWrittenPremium8,
		EarnedPremiumRunDate AS EarnedPremiumRunDate8,
		PremiumMasterWrittenExposure AS PremiumMasterWrittenExposure8,
		DeclaredEventFlag AS DeclaredEventFlag8
		FROM RTR_Split_Transactions_NonSubASL_Level_Row_320
	),
	EXP_Mine_Subsidence_Row AS (
		SELECT
		PolicyKey AS PolicyKey3,
		PolicyEffectiveDate AS PolicyEffectiveDate3,
		PolicyExpirationDate AS PolicyExpirationDate3,
		PremiumTransactionID AS PremiumTransactionID3,
		ReinsuranceCoverageAKID AS ReinsuranceCoverageAKID3,
		StatisticalCoverageAKID AS StatisticalCoverageAKID3,
		PremiumTransactionCode AS PremiumTransactionCode3,
		PremiumTransactionEnteredDate AS PremiumTransactionEnteredDate3,
		PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate3,
		PremiumTransactionExpirationDate AS PremiumTransactionExpirationDate3,
		PremiumTransactionBookedDate AS PremiumTransactionBookedDate3,
		'C' AS PremiumType3,
		ReasonAmendedCode AS ReasonAmendedCode3,
		PolicySymbol,
		TypeBureauCode,
		MajorPerilCode,
		RiskUnit,
		RiskUnitSequenceNumber AS RiskUnitSequenceNumber3,
		nsi_indicator AS nsi_indicator5,
		symbol_pos_1_2 AS symbol_pos_1_2_out5,
		PremiumAmount AS PremiumAmount5,
		FullTermPremiumAmount AS FullTermPremiumAmount5,
		EarnedPremiumAmount,
		ChangeInEarnedPremium AS ChangeInEarnedPremium3,
		aslcode AS aslcode5,
		subaslcode AS subaslcode5,
		Nonsubaslcode AS Nonsubaslcode5,
		ASLProduct_Code AS ASLProduct_Code3,
		Hierarchy_Product_Code AS Hierarchy_Product_Code3,
		'C' AS Kind_Code_Mine_Sub,
		'N' AS Facultative_Ind,
		StatisticalCoverageEffectiveDate AS StatisticalCoverageEffectiveDate3,
		StatisticalCoverageExpirationDate AS StatisticalCoverageExpirationDate3,
		StatisticalCoverageCancellationDate AS StatisticalCoverageCancellationDate3,
		RunDate AS RunDate3,
		PremiumMasterCalculationID AS PremiumMasterCalculationID3,
		AgencyAKID AS AgencyAKID3,
		PolicyAKID AS PolicyAKID3,
		strtgc_bus_dvsn_ak_id AS strtgc_bus_dvsn_ak_id3,
		ContractCustomerAKID AS ContractCustomerAKID3,
		RiskLocationAKID,
		PolicyCoverageAKID AS PolicyCoverageAKID3,
		PremiumTransactionAKID AS PremiumTransactionAKID3,
		BureauStatisticalCodeAKID AS BureauStatisticalCodeAKID3,
		PremiumMasterPolicyExpirationYear AS PremiumMasterPolicyExpirationYear3,
		PremiumMasterPolicyTerm AS PremiumMasterPolicyTerm3,
		PremiumMasterBureauPolicyType AS PremiumMasterBureauPolicyType3,
		PremiumMasterAuditCode AS PremiumMasterAuditCode3,
		PremiumMasterBureauStatisticalLine AS PremiumMasterBureauStatisticalLine3,
		PremiumMasterProductLine AS PremiumMasterProductLine3,
		PremiumMasterAgencyCommissionRate AS PremiumMasterAgencyCommissionRate3,
		PremiumMasterExposure AS PremiumMasterExposure3,
		PremiumMasterStatisticalCode1 AS PremiumMasterStatisticalCode13,
		PremiumMasterStatisticalCode2 AS PremiumMasterStatisticalCode23,
		PremiumMasterStatisticalCode AS PremiumMasterStatisticalCode33,
		PremiumMasterRateModifier AS PremiumMasterRateModifier3,
		PremiumMasterRateDeparture AS PremiumMasterRateDeparture3,
		PremiumMasterBureauInceptionDate AS PremiumMasterBureauInceptionDate3,
		PremiumMasterCountersignAgencyType AS PremiumMasterCountersignAgencyType3,
		PremiumMasterCountersignAgencyCode AS PremiumMasterCountersignAgencyCode3,
		PremiumMasterCountersignAgencyState AS PremiumMasterCountersignAgencyState3,
		PremiumMasterCountersignAgencyRate AS PremiumMasterCountersignAgencyRate3,
		PremiumMasterRenewalIndicator AS PremiumMasterRenewalIndicator3,
		PremiumMasterRecordType AS PremiumMasterRecordType3,
		ClassCode AS ClassCode3,
		SubLine AS SubLine3,
		premium_master_stage_id AS premium_master_stage_id3,
		pm_policy_number AS pm_policy_number3,
		pm_module AS pm_module3,
		pm_account_date AS pm_account_date3,
		pm_sar_location_number AS pm_sar_location_number3,
		pm_unit_number AS pm_unit_number3,
		pm_risk_state AS pm_risk_state3,
		pm_risk_zone_territory AS pm_risk_zone_territory3,
		pm_tax_location AS pm_tax_location3,
		pm_risk_zip_code_postal_zone AS pm_risk_zip_code_postal_zone3,
		pm_sar_insurance_line AS pm_sar_insurance_line3,
		pm_sar_sub_location_number AS pm_sar_sub_location_number3,
		pm_sar_risk_unit_group AS pm_sar_risk_unit_group3,
		pm_sar_class_code_group AS pm_sar_class_code_group3,
		pm_sar_class_code_member AS pm_sar_class_code_member3,
		pm_sar_sequence_risk_unit_n AS pm_sar_sequence_risk_unit_n3,
		pm_sar_sequence_risk_unit_a AS pm_sar_sequence_risk_unit_a3,
		pm_sar_type_exposure AS pm_sar_type_exposure3,
		pm_sar_mp_seq_no AS pm_sar_mp_seq_no3,
		pm_csp_inception_date AS pm_csp_inception_date3,
		pm_coverage_effective_date AS pm_coverage_effective_date3,
		pm_coverage_expiration_date AS pm_coverage_expiration_date3,
		pm_reins_ceded_premium AS pm_reins_ceded_premium3,
		pm_reins_ceded_original_premium AS pm_reins_ceded_original_premium3,
		pm_reinsurance_type_code AS pm_reinsurance_type_code3,
		pm_reinsurance_company_number AS pm_reinsurance_company_number3,
		pm_reinsurance_ratio AS pm_reinsurance_ratio3,
		AuditID AS AuditID3,
		ProductCode AS ProductCode3,
		RatingCoverageEffectiveDate AS RatingCoverageEffectiveDate3,
		RatingCoverageExpirationDate AS RatingCoverageExpirationDate3,
		RatingCoverageCancellationDate AS RatingCoverageCancellationDate3,
		RatingCoverageAKID AS RatingCoverageAKID3,
		PolicyOfferingCode AS PolicyOfferingCode3,
		PolicyCoverageEffectiveDate AS PolicyCoverageEffectiveDate3,
		PolicyCoverageExpirationDate AS PolicyCoverageExpirationDate3,
		AgencyActualCommissionRate AS AgencyActualCommissionRate3,
		InsuranceReferenceLineOfBusinessCode AS InsuranceReferenceLineOfBusinessCode3,
		EnterpriseGroupCode AS EnterpriseGroupCode3,
		InsuranceReferenceLegalEntityCode AS InsuranceReferenceLegalEntityCode3,
		StrategicProfitCenterCode AS StrategicProfitCenterCode3,
		InsuranceSegmentCode AS InsuranceSegmentCode3,
		Risk_Unit_Group AS Risk_Unit_Group3,
		StandardInsuranceLineCode AS StandardInsuranceLineCode3,
		RatingCoverage AS RatingCoverage3,
		RiskType AS RiskType3,
		CoverageType AS CoverageType3,
		StandardSpecialClassGroupCode AS StandardSpecialClassGroupCode3,
		StandardIncreasedLimitGroupCode AS StandardIncreasedLimitGroupCode3,
		StandardPackageModifcationAdjustmentGroupCode AS StandardPackageModifcationAdjustmentGroupCode3,
		SourceSystemID AS SourceSystemID3,
		EarnedExposure AS EarnedExposure3,
		ChangeInEarnedExposure AS ChangeInEarnedExposure3,
		RiskLocationHashKey AS RiskLocationHashKey3,
		PerilGroup,
		CoverageForm AS CoverageForm3,
		PolicyAuditAKID11 AS PolicyAuditAKID113,
		PolicyAuditEffectiveDate11 AS PolicyAuditEffectiveDate113,
		SubCoverageTypeCode AS SubCoverageTypeCode3,
		CoverageVersion AS CoverageVersion3,
		CustomerCareCommissionRate AS CustomerCareCommissionRate3,
		RatingPlanCode AS RatingPlanCode3,
		CoverageCancellationDate AS CoverageCancellationDate3,
		GeneratedRecordIndicator AS GeneratedRecordIndicator3,
		DirectWrittenPremium AS DirectWrittenPremium3,
		RatablePremium AS RatablePremium3,
		ClassifiedPremium AS ClassifiedPremium3,
		OtherModifiedPremium AS OtherModifiedPremium3,
		ScheduleModifiedPremium AS ScheduleModifiedPremium3,
		ExperienceModifiedPremium AS ExperienceModifiedPremium3,
		SubjectWrittenPremium AS SubjectWrittenPremium3,
		EarnedDirectWrittenPremium AS EarnedDirectWrittenPremium3,
		EarnedClassifiedPremium AS EarnedClassifiedPremium3,
		EarnedRatablePremium AS EarnedRatablePremium3,
		EarnedOtherModifiedPremium AS EarnedOtherModifiedPremium3,
		EarnedScheduleModifiedPremium AS EarnedScheduleModifiedPremium3,
		EarnedExperienceModifiedPremium AS EarnedExperienceModifiedPremium3,
		EarnedSubjectWrittenPremium AS EarnedSubjectWrittenPremium3,
		EarnedPremiumRunDate AS EarnedPremiumRunDate3,
		PremiumMasterWrittenExposure AS PremiumMasterWrittenExposure3,
		DeclaredEventFlag AS DeclaredEventFlag3
		FROM RTR_Split_Transactions_Mine_Subsidence
	),
	EXP_SubASL_Level_Row AS (
		SELECT
		PolicyKey AS PolicyKey6,
		PolicyEffectiveDate AS PolicyEffectiveDate6,
		PolicyExpirationDate AS PolicyExpirationDate6,
		PremiumTransactionID AS PremiumTransactionID14,
		ReinsuranceCoverageAKID AS ReinsuranceCoverageAKID14,
		StatisticalCoverageAKID AS StatisticalCoverageAKID14,
		PremiumTransactionCode AS PremiumTransactionCode14,
		PremiumTransactionEnteredDate AS PremiumTransactionEnteredDate14,
		PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate14,
		PremiumTransactionExpirationDate AS PremiumTransactionExpirationDate14,
		PremiumTransactionBookedDate AS PremiumTransactionBookedDate14,
		PremiumType AS PremiumType14,
		ReasonAmendedCode AS ReasonAmendedCode14,
		PolicySymbol,
		TypeBureauCode,
		MajorPerilCode,
		RiskUnit,
		RiskUnitSequenceNumber AS RiskUnitSequenceNumber6,
		symbol_pos_1_2,
		nsi_indicator AS nsi_indicator14,
		PremiumAmount AS PremiumAmount14,
		FullTermPremiumAmount AS FullTermPremiumAmount14,
		EarnedPremiumAmount,
		ChangeInEarnedPremium AS ChangeInEarnedPremium6,
		aslcode AS aslcode14,
		subaslcode AS subaslcode14,
		Nonsubaslcode AS Nonsubaslcode14,
		ASLProduct_Code AS ASLProduct_Code6,
		Hierarchy_Product_Code AS Hierarchy_Product_Code6,
		StatisticalCoverageEffectiveDate AS StatisticalCoverageEffectiveDate6,
		StatisticalCoverageExpirationDate AS StatisticalCoverageExpirationDate6,
		StatisticalCoverageCancellationDate AS StatisticalCoverageCancellationDate6,
		RunDate AS RunDate6,
		PremiumMasterCalculationID AS PremiumMasterCalculationID6,
		AgencyAKID AS AgencyAKID6,
		PolicyAKID AS PolicyAKID6,
		strtgc_bus_dvsn_ak_id AS strtgc_bus_dvsn_ak_id6,
		ContractCustomerAKID AS ContractCustomerAKID6,
		RiskLocationAKID,
		PolicyCoverageAKID AS PolicyCoverageAKID6,
		PremiumTransactionAKID AS PremiumTransactionAKID6,
		BureauStatisticalCodeAKID AS BureauStatisticalCodeAKID6,
		PremiumMasterPolicyExpirationYear AS PremiumMasterPolicyExpirationYear6,
		PremiumMasterPolicyTerm AS PremiumMasterPolicyTerm6,
		PremiumMasterBureauPolicyType AS PremiumMasterBureauPolicyType6,
		PremiumMasterAuditCode AS PremiumMasterAuditCode6,
		PremiumMasterBureauStatisticalLine AS PremiumMasterBureauStatisticalLine6,
		PremiumMasterProductLine AS PremiumMasterProductLine6,
		PremiumMasterAgencyCommissionRate AS PremiumMasterAgencyCommissionRate6,
		PremiumMasterExposure AS PremiumMasterExposure6,
		PremiumMasterStatisticalCode1 AS PremiumMasterStatisticalCode16,
		PremiumMasterStatisticalCode2 AS PremiumMasterStatisticalCode26,
		PremiumMasterStatisticalCode3 AS PremiumMasterStatisticalCode36,
		PremiumMasterRateModifier AS PremiumMasterRateModifier6,
		PremiumMasterRateDeparture AS PremiumMasterRateDeparture6,
		PremiumMasterBureauInceptionDate AS PremiumMasterBureauInceptionDate6,
		PremiumMasterCountersignAgencyType AS PremiumMasterCountersignAgencyType6,
		PremiumMasterCountersignAgencyCode AS PremiumMasterCountersignAgencyCode6,
		PremiumMasterCountersignAgencyState AS PremiumMasterCountersignAgencyState6,
		PremiumMasterCountersignAgencyRate AS PremiumMasterCountersignAgencyRate6,
		PremiumMasterRenewalIndicator AS PremiumMasterRenewalIndicator6,
		PremiumMasterRecordType AS PremiumMasterRecordType6,
		ClassCode AS ClassCode6,
		SubLine AS SubLine6,
		premium_master_stage_id AS premium_master_stage_id6,
		pm_policy_number AS pm_policy_number6,
		pm_module AS pm_module6,
		pm_account_date AS pm_account_date6,
		pm_sar_location_number AS pm_sar_location_number6,
		pm_unit_number AS pm_unit_number6,
		pm_risk_state AS pm_risk_state6,
		pm_risk_zone_territory AS pm_risk_zone_territory6,
		pm_tax_location AS pm_tax_location6,
		pm_risk_zip_code_postal_zone AS pm_risk_zip_code_postal_zone6,
		pm_sar_insurance_line AS pm_sar_insurance_line6,
		pm_sar_sub_location_number AS pm_sar_sub_location_number6,
		pm_sar_risk_unit_group AS pm_sar_risk_unit_group6,
		pm_sar_class_code_group AS pm_sar_class_code_group6,
		pm_sar_class_code_member AS pm_sar_class_code_member6,
		pm_sar_sequence_risk_unit_n AS pm_sar_sequence_risk_unit_n6,
		pm_sar_sequence_risk_unit_a AS pm_sar_sequence_risk_unit_a6,
		pm_sar_type_exposure AS pm_sar_type_exposure6,
		pm_sar_mp_seq_no AS pm_sar_mp_seq_no6,
		pm_csp_inception_date AS pm_csp_inception_date6,
		pm_coverage_effective_date AS pm_coverage_effective_date6,
		pm_coverage_expiration_date AS pm_coverage_expiration_date6,
		pm_reins_ceded_premium AS pm_reins_ceded_premium6,
		pm_reins_ceded_original_premium AS pm_reins_ceded_original_premium6,
		pm_reinsurance_type_code AS pm_reinsurance_type_code6,
		pm_reinsurance_company_number AS pm_reinsurance_company_number6,
		pm_reinsurance_ratio AS pm_reinsurance_ratio6,
		AuditID AS AuditID6,
		ProductCode AS ProductCode6,
		RatingCoverageEffectiveDate AS RatingCoverageEffectiveDate6,
		RatingCoverageExpirationDate AS RatingCoverageExpirationDate6,
		RatingCoverageCancellationDate AS RatingCoverageCancellationDate6,
		RatingCoverageAKID AS RatingCoverageAKID6,
		PolicyOfferingCode AS PolicyOfferingCode6,
		PolicyCoverageEffectiveDate AS PolicyCoverageEffectiveDate6,
		PolicyCoverageExpirationDate AS PolicyCoverageExpirationDate6,
		AgencyActualCommissionRate AS AgencyActualCommissionRate6,
		InsuranceReferenceLineOfBusinessCode AS InsuranceReferenceLineOfBusinessCode6,
		EnterpriseGroupCode AS EnterpriseGroupCode6,
		InsuranceReferenceLegalEntityCode AS InsuranceReferenceLegalEntityCode6,
		StrategicProfitCenterCode AS StrategicProfitCenterCode6,
		InsuranceSegmentCode AS InsuranceSegmentCode6,
		Risk_Unit_Group AS Risk_Unit_Group6,
		StandardInsuranceLineCode AS StandardInsuranceLineCode6,
		RatingCoverage AS RatingCoverage6,
		RiskType AS RiskType6,
		CoverageType AS CoverageType6,
		StandardSpecialClassGroupCode AS StandardSpecialClassGroupCode6,
		StandardIncreasedLimitGroupCode AS StandardIncreasedLimitGroupCode6,
		StandardPackageModifcationAdjustmentGroupCode AS StandardPackageModifcationAdjustmentGroupCode6,
		SourceSystemID AS SourceSystemID6,
		EarnedExposure AS EarnedExposure6,
		ChangeInEarnedExposure AS ChangeInEarnedExposure6,
		RiskLocationHashKey AS RiskLocationHashKey6,
		PerilGroup,
		CoverageForm AS CoverageForm6,
		PolicyAuditAKID11 AS PolicyAuditAKID116,
		PolicyAuditEffectiveDate11 AS PolicyAuditEffectiveDate116,
		SubCoverageTypeCode AS SubCoverageTypeCode6,
		CoverageVersion AS CoverageVersion6,
		CustomerCareCommissionRate AS CustomerCareCommissionRate6,
		RatingPlanCode AS RatingPlanCode6,
		CoverageCancellationDate AS CoverageCancellationDate6,
		GeneratedRecordIndicator AS GeneratedRecordIndicator6,
		DirectWrittenPremium AS DirectWrittenPremium6,
		RatablePremium AS RatablePremium6,
		ClassifiedPremium AS ClassifiedPremium6,
		OtherModifiedPremium AS OtherModifiedPremium6,
		ScheduleModifiedPremium AS ScheduleModifiedPremium6,
		ExperienceModifiedPremium AS ExperienceModifiedPremium6,
		SubjectWrittenPremium AS SubjectWrittenPremium6,
		EarnedDirectWrittenPremium AS EarnedDirectWrittenPremium6,
		EarnedClassifiedPremium AS EarnedClassifiedPremium6,
		EarnedRatablePremium AS EarnedRatablePremium6,
		EarnedOtherModifiedPremium AS EarnedOtherModifiedPremium6,
		EarnedScheduleModifiedPremium AS EarnedScheduleModifiedPremium6,
		EarnedExperienceModifiedPremium AS EarnedExperienceModifiedPremium6,
		EarnedSubjectWrittenPremium AS EarnedSubjectWrittenPremium6,
		EarnedPremiumRunDate AS EarnedPremiumRunDate6,
		PremiumMasterWrittenExposure AS PremiumMasterWrittenExposure6,
		DeclaredEventFlag AS DeclaredEventFlag6
		FROM RTR_Split_Transactions_subasl_level_rows
	),
	EXP_NonSubASL_420_Level_Row AS (
		SELECT
		PolicyKey AS PolicyKey9,
		PolicyEffectiveDate AS PolicyEffectiveDate9,
		PolicyExpirationDate AS PolicyExpirationDate9,
		PremiumTransactionID AS PremiumTransactionID5,
		ReinsuranceCoverageAKID AS ReinsuranceCoverageAKID5,
		StatisticalCoverageAKID AS StatisticalCoverageAKID5,
		PremiumTransactionCode AS PremiumTransactionCode5,
		PremiumTransactionEnteredDate AS PremiumTransactionEnteredDate5,
		PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate5,
		PremiumTransactionExpirationDate AS PremiumTransactionExpirationDate5,
		PremiumTransactionBookedDate AS PremiumTransactionBookedDate5,
		PremiumType AS PremiumType5,
		ReasonAmendedCode AS ReasonAmendedCode5,
		PolicySymbol,
		TypeBureauCode,
		MajorPerilCode,
		RiskUnit,
		RiskUnitSequenceNumber AS RiskUnitSequenceNumber9,
		nsi_indicator,
		symbol_pos_1_2 AS symbol_pos_1_2_out,
		PremiumAmount,
		-- *INF*: (0.32) * PremiumAmount
		-- 
		-- --IIF(IN(MajorPerilCode, @{pipeline().parameters.MP_271_274}, '100','599'), (0.32) * PremiumAmount, PremiumAmount)
		(0.32) * PremiumAmount AS PremiumAmount_Out,
		FullTermPremiumAmount,
		-- *INF*: (0.32) * FullTermPremiumAmount
		-- 
		-- --IIF(IN(MajorPerilCode, @{pipeline().parameters.MP_271_274}, '100','599'), (0.32) * FullTermPremiumAmount, FullTermPremiumAmount)
		(0.32) * FullTermPremiumAmount AS FullTermPremiumAmount_Out,
		EarnedPremiumAmount,
		-- *INF*: (0.32) * EarnedPremiumAmount
		-- 
		-- --IIF(IN(MajorPerilCode, @{pipeline().parameters.MP_271_274}, '100','599'), (0.32) * EarnedPremiumAmount, EarnedPremiumAmount)
		(0.32) * EarnedPremiumAmount AS EarnedPremiumAmount_Out,
		ChangeInEarnedPremium,
		-- *INF*: (0.32) * ChangeInEarnedPremium
		-- 
		-- --IIF(IN(MajorPerilCode, @{pipeline().parameters.MP_271_274}, '100','599'), (0.32) * ChangeInEarnedPremium, ChangeInEarnedPremium)
		(0.32) * ChangeInEarnedPremium AS ChangeInEarnedPremium_Out,
		'340' AS aslcode,
		'380' AS subaslcode,
		'420' AS Nonsubaslcode,
		ASLProduct_Code AS ASLProduct_Code9,
		Hierarchy_Product_Code AS Hierarchy_Product_Code9,
		StatisticalCoverageEffectiveDate AS StatisticalCoverageEffectiveDate9,
		StatisticalCoverageExpirationDate AS StatisticalCoverageExpirationDate9,
		StatisticalCoverageCancellationDate AS StatisticalCoverageCancellationDate9,
		RunDate AS RunDate9,
		PremiumMasterCalculationID AS PremiumMasterCalculationID9,
		AgencyAKID AS AgencyAKID9,
		PolicyAKID AS PolicyAKID9,
		strtgc_bus_dvsn_ak_id AS strtgc_bus_dvsn_ak_id9,
		ContractCustomerAKID AS ContractCustomerAKID9,
		RiskLocationAKID,
		PolicyCoverageAKID AS PolicyCoverageAKID9,
		PremiumTransactionAKID AS PremiumTransactionAKID9,
		BureauStatisticalCodeAKID AS BureauStatisticalCodeAKID9,
		PremiumMasterPolicyExpirationYear AS PremiumMasterPolicyExpirationYear9,
		PremiumMasterPolicyTerm AS PremiumMasterPolicyTerm9,
		PremiumMasterBureauPolicyType AS PremiumMasterBureauPolicyType9,
		PremiumMasterAuditCode AS PremiumMasterAuditCode9,
		PremiumMasterBureauStatisticalLine AS PremiumMasterBureauStatisticalLine9,
		PremiumMasterProductLine AS PremiumMasterProductLine9,
		PremiumMasterAgencyCommissionRate AS PremiumMasterAgencyCommissionRate9,
		PremiumMasterExposure AS PremiumMasterExposure9,
		PremiumMasterStatisticalCode1 AS PremiumMasterStatisticalCode19,
		PremiumMasterStatisticalCode2 AS PremiumMasterStatisticalCode29,
		PremiumMasterStatisticalCode3 AS PremiumMasterStatisticalCode39,
		PremiumMasterRateModifier AS PremiumMasterRateModifier9,
		PremiumMasterRateDeparture AS PremiumMasterRateDeparture9,
		PremiumMasterBureauInceptionDate AS PremiumMasterBureauInceptionDate9,
		PremiumMasterCountersignAgencyType AS PremiumMasterCountersignAgencyType9,
		PremiumMasterCountersignAgencyCode AS PremiumMasterCountersignAgencyCode9,
		PremiumMasterCountersignAgencyState AS PremiumMasterCountersignAgencyState9,
		PremiumMasterCountersignAgencyRate AS PremiumMasterCountersignAgencyRate9,
		PremiumMasterRenewalIndicator AS PremiumMasterRenewalIndicator9,
		PremiumMasterRecordType AS PremiumMasterRecordType9,
		ClassCode AS ClassCode9,
		SubLine AS SubLine9,
		premium_master_stage_id AS premium_master_stage_id9,
		pm_policy_number AS pm_policy_number9,
		pm_module AS pm_module9,
		pm_account_date AS pm_account_date9,
		pm_sar_location_number AS pm_sar_location_number9,
		pm_unit_number AS pm_unit_number9,
		pm_risk_state AS pm_risk_state9,
		pm_risk_zone_territory AS pm_risk_zone_territory9,
		pm_tax_location AS pm_tax_location9,
		pm_risk_zip_code_postal_zone AS pm_risk_zip_code_postal_zone9,
		pm_sar_insurance_line AS pm_sar_insurance_line9,
		pm_sar_sub_location_number AS pm_sar_sub_location_number9,
		pm_sar_risk_unit_group AS pm_sar_risk_unit_group9,
		pm_sar_class_code_group AS pm_sar_class_code_group9,
		pm_sar_class_code_member AS pm_sar_class_code_member9,
		pm_sar_sequence_risk_unit_n AS pm_sar_sequence_risk_unit_n9,
		pm_sar_sequence_risk_unit_a AS pm_sar_sequence_risk_unit_a9,
		pm_sar_type_exposure AS pm_sar_type_exposure9,
		pm_sar_mp_seq_no AS pm_sar_mp_seq_no9,
		pm_csp_inception_date AS pm_csp_inception_date9,
		pm_coverage_effective_date AS pm_coverage_effective_date9,
		pm_coverage_expiration_date AS pm_coverage_expiration_date9,
		pm_reins_ceded_premium AS pm_reins_ceded_premium9,
		-- *INF*: (0.32) * pm_reins_ceded_premium9
		-- 
		-- --IIF(IN(MajorPerilCode, @{pipeline().parameters.MP_271_274}, '100','599'), (0.32) * pm_reins_ceded_premium9)
		(0.32) * pm_reins_ceded_premium9 AS out_pm_reins_ceded_premium,
		pm_reins_ceded_original_premium AS pm_reins_ceded_original_premium9,
		-- *INF*: (0.32) * pm_reins_ceded_original_premium9
		-- 
		-- --IIF(IN(MajorPerilCode, @{pipeline().parameters.MP_271_274}, '100','599'), (0.32) * pm_reins_ceded_original_premium9)
		(0.32) * pm_reins_ceded_original_premium9 AS out_pm_reins_ceded_original_premium,
		pm_reinsurance_type_code AS pm_reinsurance_type_code9,
		pm_reinsurance_company_number AS pm_reinsurance_company_number9,
		pm_reinsurance_ratio AS pm_reinsurance_ratio9,
		AuditID AS AuditID9,
		ProductCode AS ProductCode9,
		RatingCoverageEffectiveDate AS RatingCoverageEffectiveDate9,
		RatingCoverageExpirationDate AS RatingCoverageExpirationDate9,
		RatingCoverageCancellationDate AS RatingCoverageCancellationDate9,
		RatingCoverageAKID AS RatingCoverageAKID9,
		PolicyOfferingCode AS PolicyOfferingCode9,
		PolicyCoverageEffectiveDate AS PolicyCoverageEffectiveDate9,
		PolicyCoverageExpirationDate AS PolicyCoverageExpirationDate9,
		AgencyActualCommissionRate AS AgencyActualCommissionRate9,
		InsuranceReferenceLineOfBusinessCode AS InsuranceReferenceLineOfBusinessCode9,
		EnterpriseGroupCode AS EnterpriseGroupCode9,
		InsuranceReferenceLegalEntityCode AS InsuranceReferenceLegalEntityCode9,
		StrategicProfitCenterCode AS StrategicProfitCenterCode9,
		InsuranceSegmentCode AS InsuranceSegmentCode9,
		Risk_Unit_Group AS Risk_Unit_Group9,
		StandardInsuranceLineCode AS StandardInsuranceLineCode9,
		RatingCoverage AS RatingCoverage9,
		RiskType AS RiskType9,
		CoverageType AS CoverageType9,
		StandardSpecialClassGroupCode AS StandardSpecialClassGroupCode9,
		StandardIncreasedLimitGroupCode AS StandardIncreasedLimitGroupCode9,
		StandardPackageModifcationAdjustmentGroupCode AS StandardPackageModifcationAdjustmentGroupCode9,
		SourceSystemID AS SourceSystemID9,
		EarnedExposure AS EarnedExposure9,
		ChangeInEarnedExposure AS ChangeInEarnedExposure9,
		RiskLocationHashKey AS RiskLocationHashKey9,
		PerilGroup,
		CoverageForm AS CoverageForm9,
		PolicyAuditAKID11 AS PolicyAuditAKID119,
		PolicyAuditEffectiveDate11 AS PolicyAuditEffectiveDate119,
		SubCoverageTypeCode AS SubCoverageTypeCode9,
		CoverageVersion AS CoverageVersion9,
		'340' AS o_AnnualStatementLineCode_DCT,
		'380' AS o_SubAnnualStatementLineCode_DCT,
		'420' AS o_SubNonAnnualStatementLineCode_DCT,
		CustomerCareCommissionRate AS CustomerCareCommissionRate9,
		RatingPlanCode AS RatingPlanCode9,
		CoverageCancellationDate AS CoverageCancellationDate9,
		GeneratedRecordIndicator AS GeneratedRecordIndicator9,
		DirectWrittenPremium AS i_DirectWrittenPremium9,
		RatablePremium AS i_RatablePremium9,
		ClassifiedPremium AS i_ClassifiedPremium9,
		OtherModifiedPremium AS i_OtherModifiedPremium9,
		ScheduleModifiedPremium AS i_ScheduleModifiedPremium9,
		ExperienceModifiedPremium AS i_ExperienceModifiedPremium9,
		SubjectWrittenPremium AS i_SubjectWrittenPremium9,
		-- *INF*: (0.32) * i_DirectWrittenPremium9
		(0.32) * i_DirectWrittenPremium9 AS o_DirectWrittenPremium9,
		-- *INF*: (0.32) * i_RatablePremium9
		(0.32) * i_RatablePremium9 AS o_RatablePremium9,
		-- *INF*: (0.32) * i_ClassifiedPremium9
		(0.32) * i_ClassifiedPremium9 AS o_ClassifiedPremium9,
		-- *INF*: (0.32) * i_OtherModifiedPremium9
		(0.32) * i_OtherModifiedPremium9 AS o_OtherModifiedPremium9,
		-- *INF*: (0.32) * i_ScheduleModifiedPremium9
		(0.32) * i_ScheduleModifiedPremium9 AS o_ScheduleModifiedPremium9,
		-- *INF*: (0.32) * i_ExperienceModifiedPremium9
		(0.32) * i_ExperienceModifiedPremium9 AS o_ExperienceModifiedPremium9,
		-- *INF*: (0.32) * i_SubjectWrittenPremium9
		(0.32) * i_SubjectWrittenPremium9 AS o_SubjectWrittenPremium9,
		EarnedDirectWrittenPremium AS EarnedDirectWrittenPremium9,
		EarnedClassifiedPremium AS EarnedClassifiedPremium9,
		EarnedRatablePremium AS EarnedRatablePremium9,
		EarnedOtherModifiedPremium AS EarnedOtherModifiedPremium9,
		EarnedScheduleModifiedPremium AS EarnedScheduleModifiedPremium9,
		EarnedExperienceModifiedPremium AS EarnedExperienceModifiedPremium9,
		EarnedSubjectWrittenPremium AS EarnedSubjectWrittenPremium9,
		EarnedPremiumRunDate AS EarnedPremiumRunDate9,
		PremiumMasterWrittenExposure AS PremiumMasterWrittenExposure9,
		DeclaredEventFlag AS DeclaredEventFlag9
		FROM RTR_Split_Transactions_NonSubASL_Level_Row_420
	),
	EXP_ASL_DCT AS (
		SELECT
		PolicyKey,
		PolicyEffectiveDate,
		PolicyExpirationDate,
		PremiumTransactionID,
		ReinsuranceCoverageAKID,
		StatisticalCoverageAKID,
		PremiumTransactionCode,
		PremiumTransactionEnteredDate,
		PremiumTransactionEffectiveDate,
		PremiumTransactionExpirationDate,
		PremiumTransactionBookedDate,
		PremiumType,
		ReasonAmendedCode,
		PolicySymbol,
		TypeBureauCode,
		MajorPerilCode,
		RiskUnit,
		RiskUnitSequenceNumber,
		nsi_indicator,
		PremiumAmount AS i_PremiumAmount,
		FullTermPremiumAmount AS i_FullTermPremiumAmount,
		EarnedPremiumAmount AS i_EarnedPremiumAmount,
		ChangeInEarnedPremium AS i_ChangeInEarnedPremium,
		symbol_pos_1_2,
		aslcode,
		subaslcode,
		Nonsubaslcode,
		ASLProduct_Code,
		Hierarchy_Product_Code,
		StatisticalCoverageEffectiveDate,
		StatisticalCoverageExpirationDate,
		StatisticalCoverageCancellationDate,
		RunDate,
		PremiumMasterCalculationID,
		AgencyAKID,
		PolicyAKID,
		strtgc_bus_dvsn_ak_id,
		ContractCustomerAKID,
		RiskLocationAKID,
		PolicyCoverageAKID,
		PremiumTransactionAKID,
		BureauStatisticalCodeAKID,
		PremiumMasterPolicyExpirationYear,
		PremiumMasterPolicyTerm,
		PremiumMasterBureauPolicyType,
		PremiumMasterAuditCode,
		PremiumMasterBureauStatisticalLine,
		PremiumMasterProductLine,
		PremiumMasterAgencyCommissionRate,
		PremiumMasterExposure,
		PremiumMasterStatisticalCode1,
		PremiumMasterStatisticalCode2,
		PremiumMasterStatisticalCode3,
		PremiumMasterRateModifier,
		PremiumMasterRateDeparture,
		PremiumMasterBureauInceptionDate,
		PremiumMasterCountersignAgencyType,
		PremiumMasterCountersignAgencyCode,
		PremiumMasterCountersignAgencyState,
		PremiumMasterCountersignAgencyRate,
		PremiumMasterRenewalIndicator,
		PremiumMasterRecordType,
		ClassCode,
		SubLine,
		premium_master_stage_id,
		pm_policy_number,
		pm_module,
		pm_account_date,
		pm_sar_location_number,
		pm_unit_number,
		pm_risk_state,
		pm_risk_zone_territory,
		pm_tax_location,
		pm_risk_zip_code_postal_zone,
		pm_sar_insurance_line,
		pm_sar_sub_location_number,
		pm_sar_risk_unit_group,
		pm_sar_class_code_group,
		pm_sar_class_code_member,
		pm_sar_sequence_risk_unit_n,
		pm_sar_sequence_risk_unit_a,
		pm_sar_type_exposure,
		pm_sar_mp_seq_no,
		pm_csp_inception_date,
		pm_coverage_effective_date,
		pm_coverage_expiration_date,
		pm_reins_ceded_premium AS i_pm_reins_ceded_premium,
		pm_reins_ceded_original_premium AS i_pm_reins_ceded_original_premium,
		pm_reinsurance_type_code,
		pm_reinsurance_company_number,
		pm_reinsurance_ratio,
		AuditID,
		ProductCode,
		RatingCoverageEffectiveDate,
		RatingCoverageExpirationDate,
		RatingCoverageCancellationDate,
		RatingCoverageAKID,
		PolicyOfferingCode,
		PolicyCoverageEffectiveDate,
		PolicyCoverageExpirationDate,
		AgencyActualCommissionRate,
		InsuranceReferenceLineOfBusinessCode,
		EnterpriseGroupCode,
		InsuranceReferenceLegalEntityCode,
		StrategicProfitCenterCode,
		InsuranceSegmentCode,
		Risk_Unit_Group,
		StandardInsuranceLineCode,
		RatingCoverage,
		RiskType,
		CoverageType,
		StandardSpecialClassGroupCode,
		StandardIncreasedLimitGroupCode,
		StandardPackageModifcationAdjustmentGroupCode,
		SourceSystemID,
		EarnedExposure,
		ChangeInEarnedExposure,
		RiskLocationHashKey,
		PerilGroup,
		CoverageForm,
		AnnualStatementLineCode_DCT,
		SubAnnualStatementLineCode_DCT,
		PolicyAuditAKID11 AS PolicyAuditAKID,
		PolicyAuditEffectiveDate11 AS PolicyAuditEffectiveDate,
		SubCoverageTypeCode,
		CoverageVersion,
		SubNonAnnualStatementLineCode_DCT,
		CoverageCode,
		-- *INF*: IIF(IN(SubNonAnnualStatementLineCode_DCT,'400') AND StandardInsuranceLineCode='CA' AND 
		--     ( IN(CoverageCode, 'ADLINS', 'AGTEO', 'BIPDEX', 'BIPD', 'BRDCOVGA', 'BRDFRMPRDCOMOP', 'BRDFRMPRD', 'COMPMISC', 'COMRLIABUIM', 'COMRLIABUM', 'COMRLIAB', 
		-- 	    'CAFEMPCOV', 'EMPLESSOR', 'EMPLBEN', 'FELEMPL', 'INJLEASEWRKS', 'LSECONCRN', 'LIMMEXCOV', 'LEMONLAW', 'MINPREM', 'MNRENTVHCL', 'NFRNCHSAD', 'MANU', 'MNRENTVEH', 'PLSPAK - BRD', 'RAILOPTS', 'RACEXCL','REINSPREM', 'RNTTEMPVHCL', 'TLEASE', 'TLENDG','WATRCRFTEXT','UMBIPD','COMRLIABUMBIPD','EXCDWYP','EXCDP','PRDAMEO',
		-- 'LGLDEFCST','EXCPWYP','EXCDRENTP','EXCNAFAD','LIMCTLIABPAA','CADLGLAL',
		-- 'LIMPRODW','EMPLBENERPE','FACTESTHAZ','BIPDBUYBK'
		-- )  
		--       OR 
		--       IN(CoverageCode, 'UIM', 'UM') AND IN(CoverageType, 'UIM', 'UMBIPD', 'DriveOtherCarUIM', 'NonOwnedAutoUIM', 'NonOwnedAutoUM','NonOwnedAutoStateUIM')
		--       OR 
		-- 	  CoverageCode = 'SR22' AND IN(CoverageType,'FinancialResponsibilityLiability','FinancialResponsibilityLawsLiability')
		--      ) 
		-- ,1,0 
		--      )
		IFF(
		    SubNonAnnualStatementLineCode_DCT IN ('400')
		    and StandardInsuranceLineCode = 'CA'
		    and (CoverageCode IN ('ADLINS','AGTEO','BIPDEX','BIPD','BRDCOVGA','BRDFRMPRDCOMOP','BRDFRMPRD','COMPMISC','COMRLIABUIM','COMRLIABUM','COMRLIAB','CAFEMPCOV','EMPLESSOR','EMPLBEN','FELEMPL','INJLEASEWRKS','LSECONCRN','LIMMEXCOV','LEMONLAW','MINPREM','MNRENTVHCL','NFRNCHSAD','MANU','MNRENTVEH','PLSPAK - BRD','RAILOPTS','RACEXCL','REINSPREM','RNTTEMPVHCL','TLEASE','TLENDG','WATRCRFTEXT','UMBIPD','COMRLIABUMBIPD','EXCDWYP','EXCDP','PRDAMEO','LGLDEFCST','EXCPWYP','EXCDRENTP','EXCNAFAD','LIMCTLIABPAA','CADLGLAL','LIMPRODW','EMPLBENERPE','FACTESTHAZ','BIPDBUYBK')
		    or CoverageCode IN ('UIM','UM')
		    and CoverageType IN ('UIM','UMBIPD','DriveOtherCarUIM','NonOwnedAutoUIM','NonOwnedAutoUM','NonOwnedAutoStateUIM')
		    or CoverageCode = 'SR22'
		    and CoverageType IN ('FinancialResponsibilityLiability','FinancialResponsibilityLawsLiability')),
		    1,
		    0
		) AS v_68Flag,
		-- *INF*: IIF( v_68Flag=0, i_PremiumAmount,
		-- (0.68) * i_PremiumAmount)
		IFF(v_68Flag = 0, i_PremiumAmount, (0.68) * i_PremiumAmount) AS o_PremiumAmount,
		-- *INF*: IIF( v_68Flag=0,i_FullTermPremiumAmount,
		-- (0.68) * i_FullTermPremiumAmount)
		IFF(v_68Flag = 0, i_FullTermPremiumAmount, (0.68) * i_FullTermPremiumAmount) AS o_FullTermPremiumAmount,
		-- *INF*: IIF( v_68Flag=0, i_EarnedPremiumAmount,(0.68) * i_EarnedPremiumAmount)
		IFF(v_68Flag = 0, i_EarnedPremiumAmount, (0.68) * i_EarnedPremiumAmount) AS o_EarnedPremiumAmount,
		-- *INF*: IIF( v_68Flag=0, i_ChangeInEarnedPremium,
		-- (0.68) * i_ChangeInEarnedPremium)
		IFF(v_68Flag = 0, i_ChangeInEarnedPremium, (0.68) * i_ChangeInEarnedPremium) AS o_ChangeInEarnedPremium,
		-- *INF*: IIF( v_68Flag=0, i_pm_reins_ceded_premium,
		-- (0.68) * i_pm_reins_ceded_premium)
		IFF(v_68Flag = 0, i_pm_reins_ceded_premium, (0.68) * i_pm_reins_ceded_premium) AS o_pm_reins_ceded_premium,
		-- *INF*: IIF( v_68Flag=0, i_pm_reins_ceded_original_premium,
		-- (0.68) * i_pm_reins_ceded_original_premium)
		IFF(
		    v_68Flag = 0, i_pm_reins_ceded_original_premium, (0.68) * i_pm_reins_ceded_original_premium
		) AS o_pm_reins_ceded_original_premium,
		CustomerCareCommissionRate AS CustomerCareCommissionRate10,
		RatingPlanCode AS RatingPlanCode10,
		CoverageCancellationDate AS CoverageCancellationDate10,
		GeneratedRecordIndicator AS GeneratedRecordIndicator10,
		DirectWrittenPremium AS i_DirectWrittenPremium10,
		-- *INF*: IIF( v_68Flag=0, i_DirectWrittenPremium10,
		-- (0.68) * i_DirectWrittenPremium10)
		-- 
		IFF(v_68Flag = 0, i_DirectWrittenPremium10, (0.68) * i_DirectWrittenPremium10) AS o_DirectWrittenPremium10,
		RatablePremium AS i_RatablePremium10,
		-- *INF*: IIF( v_68Flag=0, i_RatablePremium10,
		-- (0.68) * i_RatablePremium10)
		-- 
		IFF(v_68Flag = 0, i_RatablePremium10, (0.68) * i_RatablePremium10) AS o_RatablePremium10,
		ClassifiedPremium AS i_ClassifiedPremium10,
		-- *INF*: IIF( v_68Flag=0, i_ClassifiedPremium10,
		-- (0.68) * i_ClassifiedPremium10)
		-- 
		IFF(v_68Flag = 0, i_ClassifiedPremium10, (0.68) * i_ClassifiedPremium10) AS o_ClassifiedPremium10,
		OtherModifiedPremium AS i_OtherModifiedPremium10,
		-- *INF*: IIF( v_68Flag=0, i_OtherModifiedPremium10,
		-- (0.68) * i_OtherModifiedPremium10)
		IFF(v_68Flag = 0, i_OtherModifiedPremium10, (0.68) * i_OtherModifiedPremium10) AS o_OtherModifiedPremium10,
		ScheduleModifiedPremium AS i_ScheduleModifiedPremium10,
		-- *INF*: IIF( v_68Flag=0, i_ScheduleModifiedPremium10,
		-- (0.68) * i_ScheduleModifiedPremium10) 
		IFF(v_68Flag = 0, i_ScheduleModifiedPremium10, (0.68) * i_ScheduleModifiedPremium10) AS o_ScheduleModifiedPremium10,
		ExperienceModifiedPremium AS i_ExperienceModifiedPremium10,
		-- *INF*: IIF( v_68Flag=0, i_ExperienceModifiedPremium10,
		-- (0.68) * i_ExperienceModifiedPremium10)
		-- 
		IFF(v_68Flag = 0, i_ExperienceModifiedPremium10, (0.68) * i_ExperienceModifiedPremium10) AS o_ExperienceModifiedPremium10,
		SubjectWrittenPremium AS i_SubjectWrittenPremium10,
		-- *INF*: IIF( v_68Flag=0, i_SubjectWrittenPremium10,
		-- (0.68) * i_SubjectWrittenPremium10)
		-- 
		IFF(v_68Flag = 0, i_SubjectWrittenPremium10, (0.68) * i_SubjectWrittenPremium10) AS o_i_SubjectWrittenPremium10,
		EarnedDirectWrittenPremium AS i_EarnedDirectWrittenPremium10,
		EarnedClassifiedPremium AS i_EarnedClassifiedPremium10,
		EarnedRatablePremium AS i_EarnedRatablePremium10,
		EarnedOtherModifiedPremium AS i_EarnedOtherModifiedPremium10,
		EarnedScheduleModifiedPremium AS i_EarnedScheduleModifiedPremium10,
		EarnedExperienceModifiedPremium AS i_EarnedExperienceModifiedPremium10,
		EarnedSubjectWrittenPremium AS i_EarnedSubjectWrittenPremium10,
		EarnedPremiumRunDate AS i_EarnedPremiumRunDate10,
		PremiumMasterWrittenExposure AS PremiumMasterWrittenExposure10,
		DeclaredEventFlag AS DeclaredEventFlag10
		FROM RTR_Split_Transactions_asl_DCT
	),
	FIL_ASLRows AS (
		SELECT
		PolicyKey1, 
		PolicyEffectiveDate1, 
		PolicyExpirationDate1, 
		PremiumTransactionID1, 
		ReinsuranceCoverageAKID1, 
		StatisticalCoverageAKID1, 
		PremiumTransactionCode1, 
		PremiumTransactionEnteredDate1, 
		PremiumTransactionEffectiveDate1, 
		PremiumTransactionExpirationDate1, 
		PremiumTransactionBookedDate1, 
		PremiumType1, 
		ReasonAmendedCode1, 
		PolicySymbol, 
		TypeBureauCode, 
		MajorPerilCode, 
		RiskUnit, 
		RiskUnitSequenceNumber1, 
		nsi_indicator, 
		symbol_pos_1_2, 
		PremiumAmount_Out, 
		FullTermPremiumAmount_Out AS FullTermPremiumAmount, 
		EarnedPremiumAmount_out, 
		ChangeInEarnedPremium_out, 
		aslcode, 
		subaslcode_out AS subaslcode, 
		Nonsubaslcode_out AS Nonsubaslcode, 
		ASLProduct_Code1 AS ASLProduct_Code, 
		Hierarchy_Product_Code1 AS Hierarchy_Product_Code, 
		StatisticalCoverageEffectiveDate1, 
		StatisticalCoverageExpirationDate, 
		StatisticalCoverageCancellationDate, 
		RunDate1, 
		PremiumMasterCalculationID1, 
		AgencyAKID1, 
		PolicyAKID1, 
		strtgc_bus_dvsn_ak_id1, 
		ContractCustomerAKID1, 
		RiskLocationAKID, 
		PolicyCoverageAKID1, 
		PremiumTransactionAKID1, 
		BureauStatisticalCodeAKID1, 
		PremiumMasterPolicyExpirationYear1, 
		PremiumMasterPolicyTerm1, 
		PremiumMasterBureauPolicyType1, 
		PremiumMasterAuditCode1, 
		PremiumMasterBureauStatisticalLine1, 
		PremiumMasterProductLine1, 
		PremiumMasterAgencyCommissionRate1, 
		PremiumMasterExposure1, 
		PremiumMasterStatisticalCode11, 
		PremiumMasterStatisticalCode21, 
		PremiumMasterStatisticalCode31, 
		PremiumMasterRateModifier1, 
		PremiumMasterRateDeparture1, 
		PremiumMasterBureauInceptionDate1, 
		PremiumMasterCountersignAgencyType1, 
		PremiumMasterCountersignAgencyCode1, 
		PremiumMasterCountersignAgencyState1, 
		PremiumMasterCountersignAgencyRate1, 
		PremiumMasterRenewalIndicator1, 
		PremiumMasterRecordType1, 
		ClassCode1, 
		SubLine1, 
		premium_master_stage_id1, 
		pm_policy_number1, 
		pm_module1, 
		pm_account_date1, 
		pm_sar_location_number1, 
		pm_unit_number1, 
		pm_risk_state1, 
		pm_risk_zone_territory1, 
		pm_tax_location1, 
		pm_risk_zip_code_postal_zone1, 
		pm_sar_insurance_line1, 
		pm_sar_sub_location_number1, 
		pm_sar_risk_unit_group1, 
		pm_sar_class_code_group1, 
		pm_sar_class_code_member1, 
		pm_sar_sequence_risk_unit_n1, 
		pm_sar_sequence_risk_unit_a1, 
		pm_sar_type_exposure1, 
		pm_sar_mp_seq_no1, 
		pm_csp_inception_date1, 
		pm_coverage_effective_date1, 
		pm_coverage_expiration_date1, 
		out_pm_reins_ceded_premium AS pm_reins_ceded_premium1, 
		out_pm_reins_ceded_original_premium AS pm_reins_ceded_original_premium1, 
		pm_reinsurance_type_code1, 
		pm_reinsurance_company_number1, 
		pm_reinsurance_ratio1, 
		AuditID1, 
		ProductCode1, 
		RatingCoverageEffectiveDate1, 
		RatingCoverageExpirationDate1, 
		RatingCoverageCancellationDate1, 
		RatingCoverageAKID1, 
		PolicyOfferingCode1, 
		PolicyCoverageEffectiveDate1, 
		PolicyCoverageExpirationDate1, 
		AgencyActualCommissionRate1, 
		InsuranceReferenceLineOfBusinessCode1, 
		EnterpriseGroupCode1, 
		InsuranceReferenceLegalEntityCode1, 
		StrategicProfitCenterCode1, 
		InsuranceSegmentCode1, 
		Risk_Unit_Group1, 
		StandardInsuranceLineCode1, 
		RatingCoverage1, 
		RiskType1, 
		CoverageType1, 
		StandardSpecialClassGroupCode1, 
		StandardIncreasedLimitGroupCode1, 
		StandardPackageModifcationAdjustmentGroupCode1, 
		SourceSystemID1, 
		EarnedExposure1, 
		ChangeInEarnedExposure1, 
		RiskLocationHashKey1, 
		PerilGroup, 
		CoverageForm1, 
		PolicyAuditAKID111 AS PolicyAuditAKID, 
		PolicyAuditEffectiveDate111 AS PolicyAuditEffectiveDate, 
		SubCoverageTypeCode1, 
		CoverageVersion1, 
		CustomerCareCommissionRate1, 
		RatingPlanCode1, 
		CoverageCancellationDate1, 
		GeneratedRecordIndicator1, 
		o_DirectWrittenPremium1 AS DirectWrittenPremium1, 
		o_RatablePremium1 AS RatablePremium1, 
		o_ClassifiedPremium1 AS ClassifiedPremium1, 
		o_OtherModifiedPremium1 AS OtherModifiedPremium1, 
		o_ScheduleModifiedPremium1 AS ScheduleModifiedPremium1, 
		o_ExperienceModifiedPremium1 AS ExperienceModifiedPremium1, 
		o_SubjectWrittenPremium1 AS SubjectWrittenPremium1, 
		EarnedDirectWrittenPremium1, 
		EarnedClassifiedPremium1, 
		EarnedRatablePremium1, 
		EarnedOtherModifiedPremium1, 
		EarnedScheduleModifiedPremium1, 
		EarnedExperienceModifiedPremium1, 
		EarnedSubjectWrittenPremium1, 
		EarnedPremiumRunDate1, 
		PremiumMasterWrittenExposure1, 
		DeclaredEventFlag1
		FROM EXP1_ASL_Level_Row
		WHERE IIF(IN(aslcode,'260','340','440','500'),FALSE,TRUE)
	),
	Union AS (
		SELECT PolicyKey1, PremiumTransactionID1, ReinsuranceCoverageAKID1, StatisticalCoverageAKID1, PremiumTransactionCode1, PremiumTransactionEnteredDate1, PremiumTransactionEffectiveDate1, PremiumTransactionExpirationDate1, PremiumTransactionBookedDate1, PremiumType1, ReasonAmendedCode1, PolicySymbol, TypeBureauCode, MajorPerilCode, RiskUnit, nsi_indicator, symbol_pos_1_2, PremiumAmount_Out, FullTermPremiumAmount, aslcode, subaslcode, Nonsubaslcode, ASLProduct_Code, Hierarchy_Product_Code, StatisticalCoverageEffectiveDate1 AS StatisticalCoverageEffectiveDate, RunDate1 AS RunDate4, PremiumMasterCalculationID1 AS PremiumMasterCalculationID, AgencyAKID1 AS AgencyAKID, PolicyAKID1 AS PolicyAKID, ContractCustomerAKID1 AS ContractCustomerAKID, RiskLocationAKID, PolicyCoverageAKID1 AS PolicyCoverageAKID, PremiumTransactionAKID1 AS PremiumTransactionAKID, BureauStatisticalCodeAKID1 AS BureauStatisticalCodeAKID, PremiumMasterPolicyExpirationYear1 AS PremiumMasterPolicyExpirationYear, PremiumMasterPolicyTerm1 AS PremiumMasterPolicyTerm, PremiumMasterBureauPolicyType1 AS PremiumMasterBureauPolicyType, PremiumMasterAuditCode1 AS PremiumMasterAuditCode, PremiumMasterBureauStatisticalLine1 AS PremiumMasterBureauStatisticalLine, PremiumMasterProductLine1 AS PremumMasterProductLine, PremiumMasterAgencyCommissionRate1 AS PremiumMasterAgencyCommissionRate, PremiumMasterExposure1 AS PremiumMasterExposure, PremiumMasterStatisticalCode11 AS PremiumMasterStatisticalCode1, PremiumMasterStatisticalCode21 AS PremiumMasterStatisticalCode2, PremiumMasterStatisticalCode31 AS PremiumMasterStatisticalCode3, PremiumMasterRateModifier1 AS PremiumMasterRateModifier, PremiumMasterRateDeparture1 AS PremiumMasterRateDeparture, PremiumMasterBureauInceptionDate1 AS PremiumMasterBureauInceptionDate, PremiumMasterCountersignAgencyType1 AS PremiumMasterCountersignAgencyType, PremiumMasterCountersignAgencyCode1 AS PremiumMasterCountersignAgencyCode, PremiumMasterCountersignAgencyState1 AS PremiumMasterCountersignAgencyState, PremiumMasterCountersignAgencyRate1 AS PremiumMasterCountersignAgencyRate, PremiumMasterRenewalIndicator1 AS PremiumMasterRenewalIndicator, PremiumMasterRecordType1 AS PremiumMasterRecordType, ClassCode1 AS ClassCode, SubLine1 AS SubLine, premium_master_stage_id1 AS premium_master_stage_id, pm_policy_number1 AS pm_policy_number, pm_module1 AS pm_module, pm_account_date1 AS pm_account_date, pm_sar_location_number1 AS pm_sar_location_number, pm_unit_number1 AS pm_unit_number, pm_risk_state1 AS pm_risk_state, pm_risk_zone_territory1 AS pm_risk_zone_territory, pm_tax_location1 AS pm_tax_location, pm_risk_zip_code_postal_zone1 AS pm_risk_zip_code_postal_zone, pm_sar_insurance_line1 AS pm_sar_insurance_line, pm_sar_sub_location_number1 AS pm_sar_sub_location_number, pm_sar_risk_unit_group1 AS pm_sar_risk_unit_group, pm_sar_class_code_group1 AS pm_sar_class_code_group, pm_sar_class_code_member1 AS pm_sar_class_Code_member, pm_sar_sequence_risk_unit_n1 AS pm_sar_sequence_risk_unit_n, pm_sar_sequence_risk_unit_a1 AS pm_sar_sequence_risk_unit_a, pm_sar_type_exposure1 AS pm_sar_type_exposure, pm_sar_mp_seq_no1 AS pm_sar_mp_seq_no, pm_csp_inception_date1 AS pm_csp_inception_date, pm_coverage_effective_date1 AS pm_coverage_effective_date, pm_coverage_expiration_date1 AS pm_coverage_expiration_date, pm_reins_ceded_premium1 AS pm_reins_ceded_premium, pm_reins_ceded_original_premium1 AS pm_reins_ceded_original_premium, pm_reinsurance_type_code1 AS pm_reinsurance_type_code, pm_reinsurance_company_number1 AS pm_reinsurance_company_number, pm_reinsurance_ratio1 AS pm_reinsurance_ratio, AuditID1 AS AuditID, ChangeInEarnedPremium_out AS ChangeInEarnedPremium, EarnedPremiumAmount_out AS EarnedPremiumAmount, PolicyEffectiveDate1 AS PolicyEffectiveDate, PolicyExpirationDate1 AS PolicyExpirationDate, StatisticalCoverageExpirationDate, StatisticalCoverageCancellationDate, ProductCode1 AS ProductCode, RatingCoverageEffectiveDate1 AS RatingCoverageEffectiveDate, RatingCoverageExpirationDate1 AS RatingCoverageExpirationDate, RatingCoverageCancellationDate1 AS RatingCoverageCancellationDate, RatingCoverageAKID1 AS RatingCoverageAKID, PolicyOfferingCode1 AS PolicyOfferingCode, strtgc_bus_dvsn_ak_id1 AS strtgc_bus_dvsn_ak_id, PolicyCoverageEffectiveDate1 AS PolicyCoverageEffectiveDate, PolicyCoverageExpirationDate1 AS PolicyCoverageExpirationDate, AgencyActualCommissionRate1 AS AgencyActualCommissionRate, InsuranceReferenceLineOfBusinessCode1 AS InsuranceReferenceLineOfBusinessCode, EnterpriseGroupCode1 AS EnterpriseGroupCode, InsuranceReferenceLegalEntityCode1 AS InsuranceReferenceLegalEntityCode, StrategicProfitCenterCode1 AS StrategicProfitCenterCode, InsuranceSegmentCode1 AS InsuranceSegmentCode, Risk_Unit_Group1 AS Risk_Unit_Group, StandardInsuranceLineCode1 AS StandardInsuranceLineCode, RatingCoverage1 AS RatingCoverage, RiskType1 AS RiskType, CoverageType1 AS CoverageType, StandardSpecialClassGroupCode1 AS StandardSpecialClassGroupCode, StandardIncreasedLimitGroupCode1 AS StandardIncreasedLimitGroupCode, StandardPackageModifcationAdjustmentGroupCode1 AS StandardPackageModifcationAdjustmentGroupCode, SourceSystemID1 AS SourceSystemID, EarnedExposure1, ChangeInEarnedExposure1, RiskLocationHashKey1, RiskUnitSequenceNumber1 AS RiskUnitSequenceNumber, PerilGroup, CoverageForm1 AS CoverageForm, PolicyAuditAKID, PolicyAuditEffectiveDate, SubCoverageTypeCode1 AS SubCoverageTypeCode, CoverageVersion1 AS CoverageVersion, CustomerCareCommissionRate1 AS CustomerCareCommissionRate, RatingPlanCode1 AS RatingPlanCode, CoverageCancellationDate1 AS CoverageCancellationDate, GeneratedRecordIndicator1 AS GeneratedRecordIndicator, DirectWrittenPremium1 AS DirectWrittenPremium, RatablePremium1 AS RatablePremium, ClassifiedPremium1 AS ClassifiedPremium, OtherModifiedPremium1 AS OtherModifiedPremium, ScheduleModifiedPremium1 AS ScheduleModifiedPremium, ExperienceModifiedPremium1 AS ExperienceModifiedPremium, SubjectWrittenPremium1 AS SubjectWrittenPremium, EarnedDirectWrittenPremium1 AS EarnedDirectWrittenPremium, EarnedClassifiedPremium1 AS EarnedClassifiedPremium, EarnedRatablePremium1 AS EarnedRatablePremium, EarnedOtherModifiedPremium1 AS EarnedOtherModifiedPremium, EarnedScheduleModifiedPremium1 AS EarnedScheduleModifiedPremium, EarnedExperienceModifiedPremium1 AS EarnedExperienceModifiedPremium, EarnedSubjectWrittenPremium1 AS EarnedSubjectWrittenPremium, EarnedPremiumRunDate1 AS EarnedPremiumRunDate, PremiumMasterWrittenExposure1 AS PremiumMasterWrittenExposure, DeclaredEventFlag1 AS DeclaredEventFlag
		FROM FIL_ASLRows
		UNION
		SELECT PolicyKey4 AS PolicyKey1, PremiumTransactionID5 AS PremiumTransactionID1, ReinsuranceCoverageAKID5 AS ReinsuranceCoverageAKID1, StatisticalCoverageAKID5 AS StatisticalCoverageAKID1, PremiumTransactionCode5 AS PremiumTransactionCode1, PremiumTransactionEnteredDate5 AS PremiumTransactionEnteredDate1, PremiumTransactionEffectiveDate5 AS PremiumTransactionEffectiveDate1, PremiumTransactionExpirationDate5 AS PremiumTransactionExpirationDate1, PremiumTransactionBookedDate5 AS PremiumTransactionBookedDate1, PremiumType5 AS PremiumType1, ReasonAmendedCode5 AS ReasonAmendedCode1, PolicySymbol, TypeBureauCode, MajorPerilCode, RiskUnit, nsi_indicator, symbol_pos_1_2_out AS symbol_pos_1_2, PremiumAmount_Out, FullTermPremiumAmount_Out AS FullTermPremiumAmount, aslcode, subaslcode, Nonsubaslcode, ASLProduct_Code4 AS ASLProduct_Code, Hierarchy_Product_Code4 AS Hierarchy_Product_Code, StatisticalCoverageEffectiveDate4 AS StatisticalCoverageEffectiveDate, RunDate4, PremiumMasterCalculationID4 AS PremiumMasterCalculationID, AgencyAKID4 AS AgencyAKID, PolicyAKID4 AS PolicyAKID, ContractCustomerAKID4 AS ContractCustomerAKID, RiskLocationAKID, PolicyCoverageAKID4 AS PolicyCoverageAKID, PremiumTransactionAKID4 AS PremiumTransactionAKID, BureauStatisticalCodeAKID4 AS BureauStatisticalCodeAKID, PremiumMasterPolicyExpirationYear4 AS PremiumMasterPolicyExpirationYear, PremiumMasterPolicyTerm4 AS PremiumMasterPolicyTerm, PremiumMasterBureauPolicyType4 AS PremiumMasterBureauPolicyType, PremiumMasterAuditCode4 AS PremiumMasterAuditCode, PremiumMasterBureauStatisticalLine4 AS PremiumMasterBureauStatisticalLine, PremiumMasterProductLine4 AS PremumMasterProductLine, PremiumMasterAgencyCommissionRate4 AS PremiumMasterAgencyCommissionRate, PremiumMasterExposure4 AS PremiumMasterExposure, PremiumMasterStatisticalCode14 AS PremiumMasterStatisticalCode1, PremiumMasterStatisticalCode24 AS PremiumMasterStatisticalCode2, PremiumMasterStatisticalCode34 AS PremiumMasterStatisticalCode3, PremiumMasterRateModifier4 AS PremiumMasterRateModifier, PremiumMasterRateDeparture4 AS PremiumMasterRateDeparture, PremiumMasterBureauInceptionDate4 AS PremiumMasterBureauInceptionDate, PremiumMasterCountersignAgencyType4 AS PremiumMasterCountersignAgencyType, PremiumMasterCountersignAgencyCode4 AS PremiumMasterCountersignAgencyCode, PremiumMasterCountersignAgencyState4 AS PremiumMasterCountersignAgencyState, PremiumMasterCountersignAgencyRate4 AS PremiumMasterCountersignAgencyRate, PremiumMasterRenewalIndicator4 AS PremiumMasterRenewalIndicator, PremiumMasterRecordType4 AS PremiumMasterRecordType, ClassCode4 AS ClassCode, SubLine4 AS SubLine, premium_master_stage_id4 AS premium_master_stage_id, pm_policy_number4 AS pm_policy_number, pm_module4 AS pm_module, pm_account_date4 AS pm_account_date, pm_sar_location_number4 AS pm_sar_location_number, pm_unit_number4 AS pm_unit_number, pm_risk_state4 AS pm_risk_state, pm_risk_zone_territory4 AS pm_risk_zone_territory, pm_tax_location4 AS pm_tax_location, pm_risk_zip_code_postal_zone4 AS pm_risk_zip_code_postal_zone, pm_sar_insurance_line4 AS pm_sar_insurance_line, pm_sar_sub_location_number4 AS pm_sar_sub_location_number, pm_sar_risk_unit_group4 AS pm_sar_risk_unit_group, pm_sar_class_code_group4 AS pm_sar_class_code_group, pm_sar_class_code_member4 AS pm_sar_class_Code_member, pm_sar_sequence_risk_unit_n4 AS pm_sar_sequence_risk_unit_n, pm_sar_sequence_risk_unit_a4 AS pm_sar_sequence_risk_unit_a, pm_sar_type_exposure4 AS pm_sar_type_exposure, pm_sar_mp_seq_no4 AS pm_sar_mp_seq_no, pm_csp_inception_date4 AS pm_csp_inception_date, pm_coverage_effective_date4 AS pm_coverage_effective_date, pm_coverage_expiration_date4 AS pm_coverage_expiration_date, out_pm_reins_ceded_premium AS pm_reins_ceded_premium, out_pm_reins_ceded_original_premium AS pm_reins_ceded_original_premium, pm_reinsurance_type_code4 AS pm_reinsurance_type_code, pm_reinsurance_company_number4 AS pm_reinsurance_company_number, pm_reinsurance_ratio4 AS pm_reinsurance_ratio, AuditID4 AS AuditID, ChangeInEarnedPremium_Out AS ChangeInEarnedPremium, EarnedPremiumAmount_Out AS EarnedPremiumAmount, PolicyEffectiveDate4 AS PolicyEffectiveDate, PolicyExpirationDate4 AS PolicyExpirationDate, StatisticalCoverageExpirationDate4 AS StatisticalCoverageExpirationDate, StatisticalCoverageCancellationDate4 AS StatisticalCoverageCancellationDate, ProductCode4 AS ProductCode, RatingCoverageEffectiveDate4 AS RatingCoverageEffectiveDate, RatingCoverageExpirationDate4 AS RatingCoverageExpirationDate, RatingCoverageCancellationDate4 AS RatingCoverageCancellationDate, RatingCoverageAKID4 AS RatingCoverageAKID, PolicyOfferingCode4 AS PolicyOfferingCode, strtgc_bus_dvsn_ak_id4 AS strtgc_bus_dvsn_ak_id, PolicyCoverageEffectiveDate9 AS PolicyCoverageEffectiveDate, PolicyCoverageExpirationDate9 AS PolicyCoverageExpirationDate, AgencyActualCommissionRate4 AS AgencyActualCommissionRate, InsuranceReferenceLineOfBusinessCode4 AS InsuranceReferenceLineOfBusinessCode, EnterpriseGroupCode4 AS EnterpriseGroupCode, InsuranceReferenceLegalEntityCode4 AS InsuranceReferenceLegalEntityCode, StrategicProfitCenterCode4 AS StrategicProfitCenterCode, InsuranceSegmentCode4 AS InsuranceSegmentCode, Risk_Unit_Group4 AS Risk_Unit_Group, StandardInsuranceLineCode4 AS StandardInsuranceLineCode, RatingCoverage4 AS RatingCoverage, RiskType4 AS RiskType, CoverageType4 AS CoverageType, StandardSpecialClassGroupCode4 AS StandardSpecialClassGroupCode, StandardIncreasedLimitGroupCode4 AS StandardIncreasedLimitGroupCode, StandardPackageModifcationAdjustmentGroupCode4 AS StandardPackageModifcationAdjustmentGroupCode, SourceSystemID4 AS SourceSystemID, EarnedExposure4 AS EarnedExposure1, ChangeInEarnedExposure4 AS ChangeInEarnedExposure1, RiskLocationHashKey4 AS RiskLocationHashKey1, RiskUnitSequenceNumber4 AS RiskUnitSequenceNumber, PerilGroup, CoverageForm4 AS CoverageForm, PolicyAuditAKID114 AS PolicyAuditAKID, PolicyAuditEffectiveDate114 AS PolicyAuditEffectiveDate, SubCoverageTypeCode4 AS SubCoverageTypeCode, CoverageVersion4 AS CoverageVersion, CustomerCareCommissionRate4 AS CustomerCareCommissionRate, RatingPlanCode4 AS RatingPlanCode, CoverageCancellationDate4 AS CoverageCancellationDate, GeneratedRecordIndicator4 AS GeneratedRecordIndicator, o_DirectWrittenPremium4 AS DirectWrittenPremium, o_RatablePremium4 AS RatablePremium, o_ClassifiedPremium4 AS ClassifiedPremium, o_OtherModifiedPremium4 AS OtherModifiedPremium, o_ScheduleModifiedPremium4 AS ScheduleModifiedPremium, o_ExperienceModifiedPremium4 AS ExperienceModifiedPremium, o_SubjectWrittenPremium4 AS SubjectWrittenPremium, EarnedDirectWrittenPremium4 AS EarnedDirectWrittenPremium, EarnedClassifiedPremium4 AS EarnedClassifiedPremium, EarnedRatablePremium4 AS EarnedRatablePremium, EarnedOtherModifiedPremium4 AS EarnedOtherModifiedPremium, EarnedScheduleModifiedPremium4 AS EarnedScheduleModifiedPremium, EarnedExperienceModifiedPremium4 AS EarnedExperienceModifiedPremium, EarnedSubjectWrittenPremium4 AS EarnedSubjectWrittenPremium, EarnedPremiumRunDate4 AS EarnedPremiumRunDate, PremiumMasterWrittenExposure4 AS PremiumMasterWrittenExposure, DeclaredEventFlag4 AS DeclaredEventFlag
		FROM EXP2_ASL_40_Level_Row
		UNION
		SELECT PolicyKey5 AS PolicyKey1, PremiumTransactionID6 AS PremiumTransactionID1, ReinsuranceCoverageAKID6 AS ReinsuranceCoverageAKID1, StatisticalCoverageAKID6 AS StatisticalCoverageAKID1, PremiumTransactionCode6 AS PremiumTransactionCode1, PremiumTransactionEnteredDate6 AS PremiumTransactionEnteredDate1, PremiumTransactionEffectiveDate6 AS PremiumTransactionEffectiveDate1, PremiumTransactionExpirationDate6 AS PremiumTransactionExpirationDate1, PremiumTransactionBookedDate6 AS PremiumTransactionBookedDate1, PremiumType6 AS PremiumType1, ReasonAmendedCode6 AS ReasonAmendedCode1, PolicySymbol, TypeBureauCode, MajorPerilCode, RiskUnit, nsi_indicator, symbol_pos_1_2, PremiumAmount_Out, FullTermPremiumAmount_Out AS FullTermPremiumAmount, aslcode, subaslcode, Nonsubaslcode, ASLProduct_Code5 AS ASLProduct_Code, Hierarchy_Product_Code5 AS Hierarchy_Product_Code, StatisticalCoverageEffectiveDate5 AS StatisticalCoverageEffectiveDate, RunDate5 AS RunDate4, PremiumMasterCalculationID5 AS PremiumMasterCalculationID, AgencyAKID5 AS AgencyAKID, PolicyAKID5 AS PolicyAKID, ContractCustomerAKID5 AS ContractCustomerAKID, RiskLocationAKID, PolicyCoverageAKID5 AS PolicyCoverageAKID, PremiumTransactionAKID5 AS PremiumTransactionAKID, BureauStatisticalCodeAKID5 AS BureauStatisticalCodeAKID, PremiumMasterPolicyExpirationYear5 AS PremiumMasterPolicyExpirationYear, PremiumMasterPolicyTerm5 AS PremiumMasterPolicyTerm, PremiumMasterBureauPolicyType5 AS PremiumMasterBureauPolicyType, PremiumMasterAuditCode5 AS PremiumMasterAuditCode, PremiumMasterBureauStatisticalLine5 AS PremiumMasterBureauStatisticalLine, PremiumMasterProductLine5 AS PremumMasterProductLine, PremiumMasterAgencyCommissionRate5 AS PremiumMasterAgencyCommissionRate, PremiumMasterExposure5 AS PremiumMasterExposure, PremiumMasterStatisticalCode15 AS PremiumMasterStatisticalCode1, PremiumMasterStatisticalCode25 AS PremiumMasterStatisticalCode2, PremiumMasterStatisticalCode35 AS PremiumMasterStatisticalCode3, PremiumMasterRateModifier5 AS PremiumMasterRateModifier, PremiumMasterRateDeparture5 AS PremiumMasterRateDeparture, PremiumMasterBureauInceptionDate5 AS PremiumMasterBureauInceptionDate, PremiumMasterCountersignAgencyType5 AS PremiumMasterCountersignAgencyType, PremiumMasterCountersignAgencyCode5 AS PremiumMasterCountersignAgencyCode, PremiumMasterCountersignAgencyState5 AS PremiumMasterCountersignAgencyState, PremiumMasterCountersignAgencyRate5 AS PremiumMasterCountersignAgencyRate, PremiumMasterRenewalIndicator5 AS PremiumMasterRenewalIndicator, PremiumMasterRecordType5 AS PremiumMasterRecordType, ClassCode5 AS ClassCode, SubLine5 AS SubLine, premium_master_stage_id5 AS premium_master_stage_id, pm_policy_number5 AS pm_policy_number, pm_module5 AS pm_module, pm_account_date5 AS pm_account_date, pm_sar_location_number5 AS pm_sar_location_number, pm_unit_number5 AS pm_unit_number, pm_risk_state5 AS pm_risk_state, pm_risk_zone_territory5 AS pm_risk_zone_territory, pm_tax_location5 AS pm_tax_location, pm_risk_zip_code_postal_zone5 AS pm_risk_zip_code_postal_zone, pm_sar_insurance_line5 AS pm_sar_insurance_line, pm_sar_sub_location_number5 AS pm_sar_sub_location_number, pm_sar_risk_unit_group5 AS pm_sar_risk_unit_group, pm_sar_class_code_group5 AS pm_sar_class_code_group, pm_sar_class_code_member5 AS pm_sar_class_Code_member, pm_sar_sequence_risk_unit_n5 AS pm_sar_sequence_risk_unit_n, pm_sar_sequence_risk_unit_a5 AS pm_sar_sequence_risk_unit_a, pm_sar_type_exposure5 AS pm_sar_type_exposure, pm_sar_mp_seq_no5 AS pm_sar_mp_seq_no, pm_csp_inception_date5 AS pm_csp_inception_date, pm_coverage_effective_date5 AS pm_coverage_effective_date, pm_coverage_expiration_date5 AS pm_coverage_expiration_date, out_pm_reins_ceded_premium AS pm_reins_ceded_premium, out_pm_reins_ceded_original_premium AS pm_reins_ceded_original_premium, pm_reinsurance_type_code5 AS pm_reinsurance_type_code, pm_reinsurance_company_number5 AS pm_reinsurance_company_number, pm_reinsurance_ratio5 AS pm_reinsurance_ratio, AuditID5 AS AuditID, ChangeInEarnedPremium_out AS ChangeInEarnedPremium, EarnedPremiumAmount_Out AS EarnedPremiumAmount, PolicyEffectiveDate5 AS PolicyEffectiveDate, PolicyExpirationDate5 AS PolicyExpirationDate, StatisticalCoverageExpirationDate, StatisticalCoverageCancellationDate, ProductCode5 AS ProductCode, RatingCoverageEffectiveDate5 AS RatingCoverageEffectiveDate, RatingCoverageExpirationDate5 AS RatingCoverageExpirationDate, RatingCoverageCancellationDate5 AS RatingCoverageCancellationDate, RatingCoverageAKID5 AS RatingCoverageAKID, PolicyOfferingCode5 AS PolicyOfferingCode, strtgc_bus_dvsn_ak_id5 AS strtgc_bus_dvsn_ak_id, PolicyCoverageEffectiveDate5 AS PolicyCoverageEffectiveDate, PolicyCoverageExpirationDate5 AS PolicyCoverageExpirationDate, AgencyActualCommissionRate5 AS AgencyActualCommissionRate, InsuranceReferenceLineOfBusinessCode, EnterpriseGroupCode5 AS EnterpriseGroupCode, InsuranceReferenceLegalEntityCode5 AS InsuranceReferenceLegalEntityCode, StrategicProfitCenterCode5 AS StrategicProfitCenterCode, InsuranceSegmentCode5 AS InsuranceSegmentCode, Risk_Unit_Group5 AS Risk_Unit_Group, StandardInsuranceLineCode5 AS StandardInsuranceLineCode, RatingCoverage5 AS RatingCoverage, RiskType5 AS RiskType, CoverageType5 AS CoverageType, StandardSpecialClassGroupCode5 AS StandardSpecialClassGroupCode, StandardIncreasedLimitGroupCode5 AS StandardIncreasedLimitGroupCode, StandardPackageModifcationAdjustmentGroupCode5 AS StandardPackageModifcationAdjustmentGroupCode, SourceSystemID5 AS SourceSystemID, EarnedExposure5 AS EarnedExposure1, ChangeInEarnedExposure5 AS ChangeInEarnedExposure1, RiskLocationHashKey5 AS RiskLocationHashKey1, RiskUnitSequenceNumber5 AS RiskUnitSequenceNumber, PerilGroup, CoverageForm5 AS CoverageForm, PolicyAuditAKID115 AS PolicyAuditAKID, PolicyAuditEffectiveDate115 AS PolicyAuditEffectiveDate, SubCoverageTypeCode5 AS SubCoverageTypeCode, CoverageVersion5 AS CoverageVersion, CustomerCareCommissionRate5 AS CustomerCareCommissionRate, RatingPlanCode5 AS RatingPlanCode, CoverageCancellationDate5 AS CoverageCancellationDate, GeneratedRecordIndicator5 AS GeneratedRecordIndicator, o_DirectWrittenPremium5 AS DirectWrittenPremium, o_RatablePremium5 AS RatablePremium, o_ClassifiedPremium5 AS ClassifiedPremium, o_OtherModifiedPremium5 AS OtherModifiedPremium, o_ScheduleModifiedPremium5 AS ScheduleModifiedPremium, o_ExperienceModifiedPremium5 AS ExperienceModifiedPremium, o_SubjectWrittenPremium5 AS SubjectWrittenPremium, EarnedDirectWrittenPremium5 AS EarnedDirectWrittenPremium, EarnedClassifiedPremium5 AS EarnedClassifiedPremium, EarnedRatablePremium5 AS EarnedRatablePremium, EarnedOtherModifiedPremium5 AS EarnedOtherModifiedPremium, EarnedScheduleModifiedPremium5 AS EarnedScheduleModifiedPremium, EarnedExperienceModifiedPremium5 AS EarnedExperienceModifiedPremium, EarnedSubjectWrittenPremium5 AS EarnedSubjectWrittenPremium, EarnedPremiumRunDate5 AS EarnedPremiumRunDate, PremiumMasterWrittenExposure5 AS PremiumMasterWrittenExposure, DeclaredEventFlag5 AS DeclaredEventFlag
		FROM EXP2_ASL_100_Level_Row
		UNION
		SELECT PolicyKey6 AS PolicyKey1, PremiumTransactionID14 AS PremiumTransactionID1, ReinsuranceCoverageAKID14 AS ReinsuranceCoverageAKID1, StatisticalCoverageAKID14 AS StatisticalCoverageAKID1, PremiumTransactionCode14 AS PremiumTransactionCode1, PremiumTransactionEnteredDate14 AS PremiumTransactionEnteredDate1, PremiumTransactionEffectiveDate14 AS PremiumTransactionEffectiveDate1, PremiumTransactionExpirationDate14 AS PremiumTransactionExpirationDate1, PremiumTransactionBookedDate14 AS PremiumTransactionBookedDate1, PremiumType14 AS PremiumType1, ReasonAmendedCode14 AS ReasonAmendedCode1, PolicySymbol, TypeBureauCode, MajorPerilCode, RiskUnit, symbol_pos_1_2 AS nsi_indicator, nsi_indicator14 AS symbol_pos_1_2, PremiumAmount14 AS PremiumAmount_Out, FullTermPremiumAmount14 AS FullTermPremiumAmount, aslcode14 AS aslcode, subaslcode14 AS subaslcode, Nonsubaslcode14 AS Nonsubaslcode, ASLProduct_Code6 AS ASLProduct_Code, Hierarchy_Product_Code6 AS Hierarchy_Product_Code, StatisticalCoverageEffectiveDate6 AS StatisticalCoverageEffectiveDate, RunDate6 AS RunDate4, PremiumMasterCalculationID6 AS PremiumMasterCalculationID, AgencyAKID6 AS AgencyAKID, PolicyAKID6 AS PolicyAKID, ContractCustomerAKID6 AS ContractCustomerAKID, RiskLocationAKID, PolicyCoverageAKID6 AS PolicyCoverageAKID, PremiumTransactionAKID6 AS PremiumTransactionAKID, BureauStatisticalCodeAKID6 AS BureauStatisticalCodeAKID, PremiumMasterPolicyExpirationYear6 AS PremiumMasterPolicyExpirationYear, PremiumMasterPolicyTerm6 AS PremiumMasterPolicyTerm, PremiumMasterBureauPolicyType6 AS PremiumMasterBureauPolicyType, PremiumMasterAuditCode6 AS PremiumMasterAuditCode, PremiumMasterBureauStatisticalLine6 AS PremiumMasterBureauStatisticalLine, PremiumMasterProductLine6 AS PremumMasterProductLine, PremiumMasterAgencyCommissionRate6 AS PremiumMasterAgencyCommissionRate, PremiumMasterExposure6 AS PremiumMasterExposure, PremiumMasterStatisticalCode16 AS PremiumMasterStatisticalCode1, PremiumMasterStatisticalCode26 AS PremiumMasterStatisticalCode2, PremiumMasterStatisticalCode36 AS PremiumMasterStatisticalCode3, PremiumMasterRateModifier6 AS PremiumMasterRateModifier, PremiumMasterRateDeparture6 AS PremiumMasterRateDeparture, PremiumMasterBureauInceptionDate6 AS PremiumMasterBureauInceptionDate, PremiumMasterCountersignAgencyType6 AS PremiumMasterCountersignAgencyType, PremiumMasterCountersignAgencyCode6 AS PremiumMasterCountersignAgencyCode, PremiumMasterCountersignAgencyState6 AS PremiumMasterCountersignAgencyState, PremiumMasterCountersignAgencyRate6 AS PremiumMasterCountersignAgencyRate, PremiumMasterRenewalIndicator6 AS PremiumMasterRenewalIndicator, PremiumMasterRecordType6 AS PremiumMasterRecordType, ClassCode6 AS ClassCode, SubLine6 AS SubLine, premium_master_stage_id6 AS premium_master_stage_id, pm_policy_number6 AS pm_policy_number, pm_module6 AS pm_module, pm_account_date6 AS pm_account_date, pm_sar_location_number6 AS pm_sar_location_number, pm_unit_number6 AS pm_unit_number, pm_risk_state6 AS pm_risk_state, pm_risk_zone_territory6 AS pm_risk_zone_territory, pm_tax_location6 AS pm_tax_location, pm_risk_zip_code_postal_zone6 AS pm_risk_zip_code_postal_zone, pm_sar_insurance_line6 AS pm_sar_insurance_line, pm_sar_sub_location_number6 AS pm_sar_sub_location_number, pm_sar_risk_unit_group6 AS pm_sar_risk_unit_group, pm_sar_class_code_group6 AS pm_sar_class_code_group, pm_sar_class_code_member6 AS pm_sar_class_Code_member, pm_sar_sequence_risk_unit_n6 AS pm_sar_sequence_risk_unit_n, pm_sar_sequence_risk_unit_a6 AS pm_sar_sequence_risk_unit_a, pm_sar_type_exposure6 AS pm_sar_type_exposure, pm_sar_mp_seq_no6 AS pm_sar_mp_seq_no, pm_csp_inception_date6 AS pm_csp_inception_date, pm_coverage_effective_date6 AS pm_coverage_effective_date, pm_coverage_expiration_date6 AS pm_coverage_expiration_date, pm_reins_ceded_premium6 AS pm_reins_ceded_premium, pm_reins_ceded_original_premium6 AS pm_reins_ceded_original_premium, pm_reinsurance_type_code6 AS pm_reinsurance_type_code, pm_reinsurance_company_number6 AS pm_reinsurance_company_number, pm_reinsurance_ratio6 AS pm_reinsurance_ratio, AuditID6 AS AuditID, ChangeInEarnedPremium6 AS ChangeInEarnedPremium, EarnedPremiumAmount, PolicyEffectiveDate6 AS PolicyEffectiveDate, PolicyExpirationDate6 AS PolicyExpirationDate, StatisticalCoverageExpirationDate6 AS StatisticalCoverageExpirationDate, StatisticalCoverageCancellationDate6 AS StatisticalCoverageCancellationDate, ProductCode6 AS ProductCode, RatingCoverageEffectiveDate6 AS RatingCoverageEffectiveDate, RatingCoverageExpirationDate6 AS RatingCoverageExpirationDate, RatingCoverageCancellationDate6 AS RatingCoverageCancellationDate, RatingCoverageAKID6 AS RatingCoverageAKID, PolicyOfferingCode6 AS PolicyOfferingCode, strtgc_bus_dvsn_ak_id6 AS strtgc_bus_dvsn_ak_id, PolicyCoverageEffectiveDate6 AS PolicyCoverageEffectiveDate, PolicyCoverageExpirationDate6 AS PolicyCoverageExpirationDate, AgencyActualCommissionRate6 AS AgencyActualCommissionRate, InsuranceReferenceLineOfBusinessCode6 AS InsuranceReferenceLineOfBusinessCode, EnterpriseGroupCode6 AS EnterpriseGroupCode, InsuranceReferenceLegalEntityCode6 AS InsuranceReferenceLegalEntityCode, StrategicProfitCenterCode6 AS StrategicProfitCenterCode, InsuranceSegmentCode6 AS InsuranceSegmentCode, Risk_Unit_Group6 AS Risk_Unit_Group, StandardInsuranceLineCode6 AS StandardInsuranceLineCode, RatingCoverage6 AS RatingCoverage, RiskType6 AS RiskType, CoverageType6 AS CoverageType, StandardSpecialClassGroupCode6 AS StandardSpecialClassGroupCode, StandardIncreasedLimitGroupCode6 AS StandardIncreasedLimitGroupCode, StandardPackageModifcationAdjustmentGroupCode6 AS StandardPackageModifcationAdjustmentGroupCode, SourceSystemID6 AS SourceSystemID, EarnedExposure6 AS EarnedExposure1, ChangeInEarnedExposure6 AS ChangeInEarnedExposure1, RiskLocationHashKey6 AS RiskLocationHashKey1, RiskUnitSequenceNumber6 AS RiskUnitSequenceNumber, PerilGroup, CoverageForm6 AS CoverageForm, PolicyAuditAKID116 AS PolicyAuditAKID, PolicyAuditEffectiveDate116 AS PolicyAuditEffectiveDate, SubCoverageTypeCode6 AS SubCoverageTypeCode, CoverageVersion6 AS CoverageVersion, CustomerCareCommissionRate6 AS CustomerCareCommissionRate, RatingPlanCode6 AS RatingPlanCode, CoverageCancellationDate6 AS CoverageCancellationDate, GeneratedRecordIndicator6 AS GeneratedRecordIndicator, DirectWrittenPremium6 AS DirectWrittenPremium, RatablePremium6 AS RatablePremium, ClassifiedPremium6 AS ClassifiedPremium, OtherModifiedPremium6 AS OtherModifiedPremium, ScheduleModifiedPremium6 AS ScheduleModifiedPremium, ExperienceModifiedPremium6 AS ExperienceModifiedPremium, SubjectWrittenPremium6 AS SubjectWrittenPremium, EarnedDirectWrittenPremium6 AS EarnedDirectWrittenPremium, EarnedClassifiedPremium6 AS EarnedClassifiedPremium, EarnedRatablePremium6 AS EarnedRatablePremium, EarnedOtherModifiedPremium6 AS EarnedOtherModifiedPremium, EarnedScheduleModifiedPremium6 AS EarnedScheduleModifiedPremium, EarnedExperienceModifiedPremium6 AS EarnedExperienceModifiedPremium, EarnedSubjectWrittenPremium6 AS EarnedSubjectWrittenPremium, EarnedPremiumRunDate6 AS EarnedPremiumRunDate, PremiumMasterWrittenExposure6 AS PremiumMasterWrittenExposure, DeclaredEventFlag6 AS DeclaredEventFlag
		FROM EXP_SubASL_Level_Row
		UNION
		SELECT PolicyKey7 AS PolicyKey1, PremiumTransactionID AS PremiumTransactionID1, ReinsuranceCoverageAKID AS ReinsuranceCoverageAKID1, StatisticalCoverageAKID AS StatisticalCoverageAKID1, PremiumTransactionCode AS PremiumTransactionCode1, PremiumTransactionEnteredDate AS PremiumTransactionEnteredDate1, PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate1, PremiumTransactionExpirationDate AS PremiumTransactionExpirationDate1, PremiumTransactionBookedDate AS PremiumTransactionBookedDate1, PremiumType AS PremiumType1, ReasonAmendedCode AS ReasonAmendedCode1, PolicySymbol, TypeBureauCode, MajorPerilCode, RiskUnit, nsi_indicator, symbol_pos_1_2, PremiumAmount_Out, FullTermPremiumAmount_Out AS FullTermPremiumAmount, aslcode, subaslcode, Nonsubaslcode, ASLProduct_Code7 AS ASLProduct_Code, Hierarchy_Product_Code7 AS Hierarchy_Product_Code, StatisticalCoverageEffectiveDate7 AS StatisticalCoverageEffectiveDate, RunDate7 AS RunDate4, PremiumMasterCalculationID7 AS PremiumMasterCalculationID, AgencyAKID7 AS AgencyAKID, PolicyAKID7 AS PolicyAKID, ContractCustomerAKID7 AS ContractCustomerAKID, RiskLocationAKID, PolicyCoverageAKID7 AS PolicyCoverageAKID, PremiumTransactionAKID7 AS PremiumTransactionAKID, BureauStatisticalCodeAKID7 AS BureauStatisticalCodeAKID, PremiumMasterPolicyExpirationYear7 AS PremiumMasterPolicyExpirationYear, PremiumMasterPolicyTerm7 AS PremiumMasterPolicyTerm, PremiumMasterBureauPolicyType7 AS PremiumMasterBureauPolicyType, PremiumMasterAuditCode7 AS PremiumMasterAuditCode, PremiumMasterBureauStatisticalLine7 AS PremiumMasterBureauStatisticalLine, PremiumMasterProductLine7 AS PremumMasterProductLine, PremiumMasterAgencyCommissionRate7 AS PremiumMasterAgencyCommissionRate, PremiumMasterExposure7 AS PremiumMasterExposure, PremiumMasterStatisticalCode17 AS PremiumMasterStatisticalCode1, PremiumMasterStatisticalCode27 AS PremiumMasterStatisticalCode2, PremiumMasterStatisticalCode37 AS PremiumMasterStatisticalCode3, PremiumMasterRateModifier7 AS PremiumMasterRateModifier, PremiumMasterRateDeparture7 AS PremiumMasterRateDeparture, PremiumMasterBureauInceptionDate7 AS PremiumMasterBureauInceptionDate, PremiumMasterCountersignAgencyType7 AS PremiumMasterCountersignAgencyType, PremiumMasterCountersignAgencyCode7 AS PremiumMasterCountersignAgencyCode, PremiumMasterCountersignAgencyState7 AS PremiumMasterCountersignAgencyState, PremiumMasterCountersignAgencyRate7 AS PremiumMasterCountersignAgencyRate, PremiumMasterRenewalIndicator7 AS PremiumMasterRenewalIndicator, PremiumMasterRecordType7 AS PremiumMasterRecordType, ClassCode7 AS ClassCode, SubLine7 AS SubLine, premium_master_stage_id7 AS premium_master_stage_id, pm_policy_number7 AS pm_policy_number, pm_module7 AS pm_module, pm_account_date7 AS pm_account_date, pm_sar_location_number7 AS pm_sar_location_number, pm_unit_number7 AS pm_unit_number, pm_risk_state7 AS pm_risk_state, pm_risk_zone_territory7 AS pm_risk_zone_territory, pm_tax_location7 AS pm_tax_location, pm_risk_zip_code_postal_zone7 AS pm_risk_zip_code_postal_zone, pm_sar_insurance_line7 AS pm_sar_insurance_line, pm_sar_sub_location_number7 AS pm_sar_sub_location_number, pm_sar_risk_unit_group7 AS pm_sar_risk_unit_group, pm_sar_class_code_group7 AS pm_sar_class_code_group, pm_sar_class_code_member7 AS pm_sar_class_Code_member, pm_sar_sequence_risk_unit_n7 AS pm_sar_sequence_risk_unit_n, pm_sar_sequence_risk_unit_a7 AS pm_sar_sequence_risk_unit_a, pm_sar_type_exposure7 AS pm_sar_type_exposure, pm_sar_mp_seq_no7 AS pm_sar_mp_seq_no, pm_csp_inception_date7 AS pm_csp_inception_date, pm_coverage_effective_date7 AS pm_coverage_effective_date, pm_coverage_expiration_date7 AS pm_coverage_expiration_date, out_pm_reins_ceded_premium AS pm_reins_ceded_premium, out_pm_reins_ceded_original_premium AS pm_reins_ceded_original_premium, pm_reinsurance_type_code7 AS pm_reinsurance_type_code, pm_reinsurance_company_number7 AS pm_reinsurance_company_number, pm_reinsurance_ratio7 AS pm_reinsurance_ratio, AuditID7 AS AuditID, ChangeInEarnedPremium_Out AS ChangeInEarnedPremium, EarnedPremiumAmount_Out AS EarnedPremiumAmount, PolicyEffectiveDate7 AS PolicyEffectiveDate, PolicyExpirationDate7 AS PolicyExpirationDate, StatisticalCoverageExpirationDate7 AS StatisticalCoverageExpirationDate, StatisticalCoverageCancellationDate7 AS StatisticalCoverageCancellationDate, ProductCode7 AS ProductCode, RatingCoverageEffectiveDate7 AS RatingCoverageEffectiveDate, RatingCoverageExpirationDate7 AS RatingCoverageExpirationDate, RatingCoverageCancellationDate7 AS RatingCoverageCancellationDate, RatingCoverageAKID7 AS RatingCoverageAKID, PolicyOfferingCode7 AS PolicyOfferingCode, strtgc_bus_dvsn_ak_id7 AS strtgc_bus_dvsn_ak_id, PolicyCoverageEffectiveDate7 AS PolicyCoverageEffectiveDate, PolicyCoverageExpirationDate7 AS PolicyCoverageExpirationDate, AgencyActualCommissionRate7 AS AgencyActualCommissionRate, InsuranceReferenceLineOfBusinessCode7 AS InsuranceReferenceLineOfBusinessCode, EnterpriseGroupCode7 AS EnterpriseGroupCode, InsuranceReferenceLegalEntityCode7 AS InsuranceReferenceLegalEntityCode, StrategicProfitCenterCode7 AS StrategicProfitCenterCode, InsuranceSegmentCode7 AS InsuranceSegmentCode, Risk_Unit_Group7 AS Risk_Unit_Group, StandardInsuranceLineCode7 AS StandardInsuranceLineCode, RatingCoverage7 AS RatingCoverage, RiskType7 AS RiskType, CoverageType7 AS CoverageType, StandardSpecialClassGroupCode7 AS StandardSpecialClassGroupCode, StandardIncreasedLimitGroupCode7 AS StandardIncreasedLimitGroupCode, StandardPackageModifcationAdjustmentGroupCode7 AS StandardPackageModifcationAdjustmentGroupCode, SourceSystemID7 AS SourceSystemID, EarnedExposure7 AS EarnedExposure1, ChangeInEarnedExposure7 AS ChangeInEarnedExposure1, RiskLocationHashKey7 AS RiskLocationHashKey1, RiskUnitSequenceNumber7 AS RiskUnitSequenceNumber, PerilGroup, CoverageForm7 AS CoverageForm, PolicyAuditAKID117 AS PolicyAuditAKID, PolicyAuditEffectiveDate117 AS PolicyAuditEffectiveDate, SubCoverageTypeCode7 AS SubCoverageTypeCode, CoverageVersion7 AS CoverageVersion, CustomerCareCommissionRate7 AS CustomerCareCommissionRate, RatingPlanCode7 AS RatingPlanCode, CoverageCancellationDate7 AS CoverageCancellationDate, GeneratedRecordIndicator7 AS GeneratedRecordIndicator, o_DirectWrittenPremium7 AS DirectWrittenPremium, o_RatablePremium7 AS RatablePremium, o_ClassifiedPremium7 AS ClassifiedPremium, o_OtherModifiedPremium7 AS OtherModifiedPremium, o_ScheduleModifiedPremium7 AS ScheduleModifiedPremium, o_ExperienceModifiedPremium7 AS ExperienceModifiedPremium, o_SubjectWrittenPremium7 AS SubjectWrittenPremium, EarnedDirectWrittenPremium7 AS EarnedDirectWrittenPremium, EarnedClassifiedPremium7 AS EarnedClassifiedPremium, EarnedRatablePremium7 AS EarnedRatablePremium, EarnedOtherModifiedPremium7 AS EarnedOtherModifiedPremium, EarnedScheduleModifiedPremium7 AS EarnedScheduleModifiedPremium, EarnedExperienceModifiedPremium7 AS EarnedExperienceModifiedPremium, EarnedSubjectWrittenPremium7 AS EarnedSubjectWrittenPremium, EarnedPremiumRunDate7 AS EarnedPremiumRunDate, PremiumMasterWrittenExposure7 AS PremiumMasterWrittenExposure, DeclaredEventFlag7 AS DeclaredEventFlag
		FROM EXP_NonSubASL_Level_Row
		UNION
		SELECT PolicyKey8 AS PolicyKey1, PremiumTransactionID5 AS PremiumTransactionID1, ReinsuranceCoverageAKID5 AS ReinsuranceCoverageAKID1, StatisticalCoverageAKID5 AS StatisticalCoverageAKID1, PremiumTransactionCode5 AS PremiumTransactionCode1, PremiumTransactionEnteredDate5 AS PremiumTransactionEnteredDate1, PremiumTransactionEffectiveDate5 AS PremiumTransactionEffectiveDate1, PremiumTransactionExpirationDate5 AS PremiumTransactionExpirationDate1, PremiumTransactionBookedDate5 AS PremiumTransactionBookedDate1, PremiumType5 AS PremiumType1, ReasonAmendedCode5 AS ReasonAmendedCode1, PolicySymbol, TypeBureauCode, MajorPerilCode, RiskUnit, nsi_indicator, symbol_pos_1_2_out AS symbol_pos_1_2, PremiumAmount_Out, FullTermPremiumAmount_Out AS FullTermPremiumAmount, aslcode, subaslcode, Nonsubaslcode, ASLProduct_Code8 AS ASLProduct_Code, Hierarchy_Product_Code8 AS Hierarchy_Product_Code, StatisticalCoverageEffectiveDate8 AS StatisticalCoverageEffectiveDate, RunDate8 AS RunDate4, PremiumMasterCalculationID8 AS PremiumMasterCalculationID, AgencyAKID8 AS AgencyAKID, PolicyAKID8 AS PolicyAKID, ContractCustomerAKID8 AS ContractCustomerAKID, RiskLocationAKID, PolicyCoverageAKID8 AS PolicyCoverageAKID, PremiumTransactionAKID8 AS PremiumTransactionAKID, BureauStatisticalCodeAKID8 AS BureauStatisticalCodeAKID, PremiumMasterPolicyExpirationYear8 AS PremiumMasterPolicyExpirationYear, PremiumMasterPolicyTerm8 AS PremiumMasterPolicyTerm, PremiumMasterBureauPolicyType8 AS PremiumMasterBureauPolicyType, PremiumMasterAuditCode8 AS PremiumMasterAuditCode, PremiumMasterBureauStatisticalLine8 AS PremiumMasterBureauStatisticalLine, PremiumMasterProductLine8 AS PremumMasterProductLine, PremiumMasterAgencyCommissionRate8 AS PremiumMasterAgencyCommissionRate, PremiumMasterExposure8 AS PremiumMasterExposure, PremiumMasterStatisticalCode18 AS PremiumMasterStatisticalCode1, PremiumMasterStatisticalCode28 AS PremiumMasterStatisticalCode2, PremiumMasterStatisticalCode38 AS PremiumMasterStatisticalCode3, PremiumMasterRateModifier8 AS PremiumMasterRateModifier, PremiumMasterRateDeparture8 AS PremiumMasterRateDeparture, PremiumMasterBureauInceptionDate8 AS PremiumMasterBureauInceptionDate, PremiumMasterCountersignAgencyType8 AS PremiumMasterCountersignAgencyType, PremiumMasterCountersignAgencyCode8 AS PremiumMasterCountersignAgencyCode, PremiumMasterCountersignAgencyState8 AS PremiumMasterCountersignAgencyState, PremiumMasterCountersignAgencyRate8 AS PremiumMasterCountersignAgencyRate, PremiumMasterRenewalIndicator8 AS PremiumMasterRenewalIndicator, PremiumMasterRecordType8 AS PremiumMasterRecordType, ClassCode8 AS ClassCode, SubLine8 AS SubLine, premium_master_stage_id8 AS premium_master_stage_id, pm_policy_number8 AS pm_policy_number, pm_module8 AS pm_module, pm_account_date8 AS pm_account_date, pm_sar_location_number8 AS pm_sar_location_number, pm_unit_number8 AS pm_unit_number, pm_risk_state8 AS pm_risk_state, pm_risk_zone_territory8 AS pm_risk_zone_territory, pm_tax_location8 AS pm_tax_location, pm_risk_zip_code_postal_zone8 AS pm_risk_zip_code_postal_zone, pm_sar_insurance_line8 AS pm_sar_insurance_line, pm_sar_sub_location_number8 AS pm_sar_sub_location_number, pm_sar_risk_unit_group8 AS pm_sar_risk_unit_group, pm_sar_class_code_group8 AS pm_sar_class_code_group, pm_sar_class_code_member8 AS pm_sar_class_Code_member, pm_sar_sequence_risk_unit_n8 AS pm_sar_sequence_risk_unit_n, pm_sar_sequence_risk_unit_a8 AS pm_sar_sequence_risk_unit_a, pm_sar_type_exposure8 AS pm_sar_type_exposure, pm_sar_mp_seq_no8 AS pm_sar_mp_seq_no, pm_csp_inception_date8 AS pm_csp_inception_date, pm_coverage_effective_date8 AS pm_coverage_effective_date, pm_coverage_expiration_date8 AS pm_coverage_expiration_date, out_pm_reins_ceded_premium AS pm_reins_ceded_premium, out_pm_reins_ceded_original_premium AS pm_reins_ceded_original_premium, pm_reinsurance_type_code8 AS pm_reinsurance_type_code, pm_reinsurance_company_number8 AS pm_reinsurance_company_number, pm_reinsurance_ratio8 AS pm_reinsurance_ratio, AuditID8 AS AuditID, ChangeInEarnedPremium_Out AS ChangeInEarnedPremium, EarnedPremiumAmount_Out AS EarnedPremiumAmount, PolicyEffectiveDate8 AS PolicyEffectiveDate, PolicyExpirationDate8 AS PolicyExpirationDate, StatisticalCoverageExpirationDate8 AS StatisticalCoverageExpirationDate, StatisticalCoverageCancellationDate8 AS StatisticalCoverageCancellationDate, ProductCode8 AS ProductCode, RatingCoverageEffectiveDate8 AS RatingCoverageEffectiveDate, RatingCoverageExpirationDate8 AS RatingCoverageExpirationDate, RatingCoverageCancellationDate8 AS RatingCoverageCancellationDate, RatingCoverageAKID8 AS RatingCoverageAKID, PolicyOfferingCode8 AS PolicyOfferingCode, strtgc_bus_dvsn_ak_id8 AS strtgc_bus_dvsn_ak_id, PolicyCoverageEffectiveDate8 AS PolicyCoverageEffectiveDate, PolicyCoverageExpirationDate8 AS PolicyCoverageExpirationDate, AgencyActualCommissionRate8 AS AgencyActualCommissionRate, InsuranceReferenceLineOfBusinessCode8 AS InsuranceReferenceLineOfBusinessCode, EnterpriseGroupCode8 AS EnterpriseGroupCode, InsuranceReferenceLegalEntityCode8 AS InsuranceReferenceLegalEntityCode, StrategicProfitCenterCode8 AS StrategicProfitCenterCode, InsuranceSegmentCode8 AS InsuranceSegmentCode, Risk_Unit_Group8 AS Risk_Unit_Group, StandardInsuranceLineCode8 AS StandardInsuranceLineCode, RatingCoverage8 AS RatingCoverage, RiskType8 AS RiskType, CoverageType8 AS CoverageType, StandardSpecialClassGroupCode8 AS StandardSpecialClassGroupCode, StandardIncreasedLimitGroupCode8 AS StandardIncreasedLimitGroupCode, StandardPackageModifcationAdjustmentGroupCode8 AS StandardPackageModifcationAdjustmentGroupCode, SourceSystemID8 AS SourceSystemID, EarnedExposure8 AS EarnedExposure1, ChangeInEarnedExposure8 AS ChangeInEarnedExposure1, RiskLocationHashKey8 AS RiskLocationHashKey1, RiskUnitSequenceNumber8 AS RiskUnitSequenceNumber, PerilGroup, CoverageForm8 AS CoverageForm, PolicyAuditAKID118 AS PolicyAuditAKID, PolicyAuditEffectiveDate118 AS PolicyAuditEffectiveDate, SubCoverageTypeCode8 AS SubCoverageTypeCode, CoverageVersion8 AS CoverageVersion, CustomerCareCommissionRate8 AS CustomerCareCommissionRate, RatingPlanCode8 AS RatingPlanCode, CoverageCancellationDate8 AS CoverageCancellationDate, GeneratedRecordIndicator8 AS GeneratedRecordIndicator, o_DirectWrittenPremium8 AS DirectWrittenPremium, o_RatablePremium8 AS RatablePremium, o_ClassifiedPremium8 AS ClassifiedPremium, o_OtherModifiedPremium8 AS OtherModifiedPremium, o_ScheduleModifiedPremium8 AS ScheduleModifiedPremium, o_ExperienceModifiedPremium8 AS ExperienceModifiedPremium, o_SubjectWrittenPremium8 AS SubjectWrittenPremium, EarnedDirectWrittenPremium8 AS EarnedDirectWrittenPremium, EarnedClassifiedPremium8 AS EarnedClassifiedPremium, EarnedRatablePremium8 AS EarnedRatablePremium, EarnedOtherModifiedPremium8 AS EarnedOtherModifiedPremium, EarnedScheduleModifiedPremium8 AS EarnedScheduleModifiedPremium, EarnedExperienceModifiedPremium8 AS EarnedExperienceModifiedPremium, EarnedSubjectWrittenPremium8 AS EarnedSubjectWrittenPremium, EarnedPremiumRunDate8 AS EarnedPremiumRunDate, PremiumMasterWrittenExposure8 AS PremiumMasterWrittenExposure, DeclaredEventFlag8 AS DeclaredEventFlag
		FROM EXP_NonSubASL_320_Level_Row
		UNION
		SELECT PolicyKey9 AS PolicyKey1, PremiumTransactionID5 AS PremiumTransactionID1, ReinsuranceCoverageAKID5 AS ReinsuranceCoverageAKID1, StatisticalCoverageAKID5 AS StatisticalCoverageAKID1, PremiumTransactionCode5 AS PremiumTransactionCode1, PremiumTransactionEnteredDate5 AS PremiumTransactionEnteredDate1, PremiumTransactionEffectiveDate5 AS PremiumTransactionEffectiveDate1, PremiumTransactionExpirationDate5 AS PremiumTransactionExpirationDate1, PremiumTransactionBookedDate5 AS PremiumTransactionBookedDate1, PremiumType5 AS PremiumType1, ReasonAmendedCode5 AS ReasonAmendedCode1, PolicySymbol, TypeBureauCode, MajorPerilCode, RiskUnit, nsi_indicator, symbol_pos_1_2_out AS symbol_pos_1_2, PremiumAmount_Out, FullTermPremiumAmount_Out AS FullTermPremiumAmount, aslcode, subaslcode, Nonsubaslcode, ASLProduct_Code9 AS ASLProduct_Code, Hierarchy_Product_Code9 AS Hierarchy_Product_Code, StatisticalCoverageEffectiveDate9 AS StatisticalCoverageEffectiveDate, RunDate9 AS RunDate4, PremiumMasterCalculationID9 AS PremiumMasterCalculationID, AgencyAKID9 AS AgencyAKID, PolicyAKID9 AS PolicyAKID, ContractCustomerAKID9 AS ContractCustomerAKID, RiskLocationAKID, PolicyCoverageAKID9 AS PolicyCoverageAKID, PremiumTransactionAKID9 AS PremiumTransactionAKID, BureauStatisticalCodeAKID9 AS BureauStatisticalCodeAKID, PremiumMasterPolicyExpirationYear9 AS PremiumMasterPolicyExpirationYear, PremiumMasterPolicyTerm9 AS PremiumMasterPolicyTerm, PremiumMasterBureauPolicyType9 AS PremiumMasterBureauPolicyType, PremiumMasterAuditCode9 AS PremiumMasterAuditCode, PremiumMasterBureauStatisticalLine9 AS PremiumMasterBureauStatisticalLine, PremiumMasterProductLine9 AS PremumMasterProductLine, PremiumMasterAgencyCommissionRate9 AS PremiumMasterAgencyCommissionRate, PremiumMasterExposure9 AS PremiumMasterExposure, PremiumMasterStatisticalCode19 AS PremiumMasterStatisticalCode1, PremiumMasterStatisticalCode29 AS PremiumMasterStatisticalCode2, PremiumMasterStatisticalCode39 AS PremiumMasterStatisticalCode3, PremiumMasterRateModifier9 AS PremiumMasterRateModifier, PremiumMasterRateDeparture9 AS PremiumMasterRateDeparture, PremiumMasterBureauInceptionDate9 AS PremiumMasterBureauInceptionDate, PremiumMasterCountersignAgencyType9 AS PremiumMasterCountersignAgencyType, PremiumMasterCountersignAgencyCode9 AS PremiumMasterCountersignAgencyCode, PremiumMasterCountersignAgencyState9 AS PremiumMasterCountersignAgencyState, PremiumMasterCountersignAgencyRate9 AS PremiumMasterCountersignAgencyRate, PremiumMasterRenewalIndicator9 AS PremiumMasterRenewalIndicator, PremiumMasterRecordType9 AS PremiumMasterRecordType, ClassCode9 AS ClassCode, SubLine9 AS SubLine, premium_master_stage_id9 AS premium_master_stage_id, pm_policy_number9 AS pm_policy_number, pm_module9 AS pm_module, pm_account_date9 AS pm_account_date, pm_sar_location_number9 AS pm_sar_location_number, pm_unit_number9 AS pm_unit_number, pm_risk_state9 AS pm_risk_state, pm_risk_zone_territory9 AS pm_risk_zone_territory, pm_tax_location9 AS pm_tax_location, pm_risk_zip_code_postal_zone9 AS pm_risk_zip_code_postal_zone, pm_sar_insurance_line9 AS pm_sar_insurance_line, pm_sar_sub_location_number9 AS pm_sar_sub_location_number, pm_sar_risk_unit_group9 AS pm_sar_risk_unit_group, pm_sar_class_code_group9 AS pm_sar_class_code_group, pm_sar_class_code_member9 AS pm_sar_class_Code_member, pm_sar_sequence_risk_unit_n9 AS pm_sar_sequence_risk_unit_n, pm_sar_sequence_risk_unit_a9 AS pm_sar_sequence_risk_unit_a, pm_sar_type_exposure9 AS pm_sar_type_exposure, pm_sar_mp_seq_no9 AS pm_sar_mp_seq_no, pm_csp_inception_date9 AS pm_csp_inception_date, pm_coverage_effective_date9 AS pm_coverage_effective_date, pm_coverage_expiration_date9 AS pm_coverage_expiration_date, out_pm_reins_ceded_premium AS pm_reins_ceded_premium, out_pm_reins_ceded_original_premium AS pm_reins_ceded_original_premium, pm_reinsurance_type_code9 AS pm_reinsurance_type_code, pm_reinsurance_company_number9 AS pm_reinsurance_company_number, pm_reinsurance_ratio9 AS pm_reinsurance_ratio, AuditID9 AS AuditID, ChangeInEarnedPremium_Out AS ChangeInEarnedPremium, EarnedPremiumAmount_Out AS EarnedPremiumAmount, PolicyEffectiveDate9 AS PolicyEffectiveDate, PolicyExpirationDate9 AS PolicyExpirationDate, StatisticalCoverageExpirationDate9 AS StatisticalCoverageExpirationDate, StatisticalCoverageCancellationDate9 AS StatisticalCoverageCancellationDate, ProductCode9 AS ProductCode, RatingCoverageEffectiveDate9 AS RatingCoverageEffectiveDate, RatingCoverageExpirationDate9 AS RatingCoverageExpirationDate, RatingCoverageCancellationDate9 AS RatingCoverageCancellationDate, RatingCoverageAKID9 AS RatingCoverageAKID, PolicyOfferingCode9 AS PolicyOfferingCode, strtgc_bus_dvsn_ak_id9 AS strtgc_bus_dvsn_ak_id, PolicyCoverageEffectiveDate9 AS PolicyCoverageEffectiveDate, PolicyCoverageExpirationDate9 AS PolicyCoverageExpirationDate, AgencyActualCommissionRate9 AS AgencyActualCommissionRate, InsuranceReferenceLineOfBusinessCode9 AS InsuranceReferenceLineOfBusinessCode, EnterpriseGroupCode9 AS EnterpriseGroupCode, InsuranceReferenceLegalEntityCode9 AS InsuranceReferenceLegalEntityCode, StrategicProfitCenterCode9 AS StrategicProfitCenterCode, InsuranceSegmentCode9 AS InsuranceSegmentCode, Risk_Unit_Group9 AS Risk_Unit_Group, StandardInsuranceLineCode9 AS StandardInsuranceLineCode, RatingCoverage9 AS RatingCoverage, RiskType9 AS RiskType, CoverageType9 AS CoverageType, StandardSpecialClassGroupCode9 AS StandardSpecialClassGroupCode, StandardIncreasedLimitGroupCode9 AS StandardIncreasedLimitGroupCode, StandardPackageModifcationAdjustmentGroupCode9 AS StandardPackageModifcationAdjustmentGroupCode, SourceSystemID9 AS SourceSystemID, EarnedExposure9 AS EarnedExposure1, ChangeInEarnedExposure9 AS ChangeInEarnedExposure1, RiskLocationHashKey9 AS RiskLocationHashKey1, RiskUnitSequenceNumber9 AS RiskUnitSequenceNumber, PerilGroup, CoverageForm9 AS CoverageForm, o_AnnualStatementLineCode_DCT AS AnnualStatementLineCode_DCT, o_SubAnnualStatementLineCode_DCT AS SubAnnualStatementLineCode_DCT, PolicyAuditAKID119 AS PolicyAuditAKID, PolicyAuditEffectiveDate119 AS PolicyAuditEffectiveDate, SubCoverageTypeCode9 AS SubCoverageTypeCode, CoverageVersion9 AS CoverageVersion, o_SubNonAnnualStatementLineCode_DCT AS SubNonAnnualStatementLineCode_DCT, CustomerCareCommissionRate9 AS CustomerCareCommissionRate, RatingPlanCode9 AS RatingPlanCode, CoverageCancellationDate9 AS CoverageCancellationDate, GeneratedRecordIndicator9 AS GeneratedRecordIndicator, o_DirectWrittenPremium9 AS DirectWrittenPremium, o_RatablePremium9 AS RatablePremium, o_ClassifiedPremium9 AS ClassifiedPremium, o_OtherModifiedPremium9 AS OtherModifiedPremium, o_ScheduleModifiedPremium9 AS ScheduleModifiedPremium, o_ExperienceModifiedPremium9 AS ExperienceModifiedPremium, o_SubjectWrittenPremium9 AS SubjectWrittenPremium, EarnedDirectWrittenPremium9 AS EarnedDirectWrittenPremium, EarnedClassifiedPremium9 AS EarnedClassifiedPremium, EarnedRatablePremium9 AS EarnedRatablePremium, EarnedOtherModifiedPremium9 AS EarnedOtherModifiedPremium, EarnedScheduleModifiedPremium9 AS EarnedScheduleModifiedPremium, EarnedExperienceModifiedPremium9 AS EarnedExperienceModifiedPremium, EarnedSubjectWrittenPremium9 AS EarnedSubjectWrittenPremium, EarnedPremiumRunDate9 AS EarnedPremiumRunDate, PremiumMasterWrittenExposure9 AS PremiumMasterWrittenExposure, DeclaredEventFlag9 AS DeclaredEventFlag
		FROM EXP_NonSubASL_420_Level_Row
		UNION
		SELECT PolicyKey3 AS PolicyKey1, PremiumTransactionID3 AS PremiumTransactionID1, ReinsuranceCoverageAKID3 AS ReinsuranceCoverageAKID1, StatisticalCoverageAKID3 AS StatisticalCoverageAKID1, PremiumTransactionCode3 AS PremiumTransactionCode1, PremiumTransactionEnteredDate3 AS PremiumTransactionEnteredDate1, PremiumTransactionEffectiveDate3 AS PremiumTransactionEffectiveDate1, PremiumTransactionExpirationDate3 AS PremiumTransactionExpirationDate1, PremiumTransactionBookedDate3 AS PremiumTransactionBookedDate1, PremiumType3 AS PremiumType1, ReasonAmendedCode3 AS ReasonAmendedCode1, PolicySymbol, TypeBureauCode, MajorPerilCode, RiskUnit, nsi_indicator5 AS nsi_indicator, symbol_pos_1_2_out5 AS symbol_pos_1_2, PremiumAmount5 AS PremiumAmount_Out, FullTermPremiumAmount5 AS FullTermPremiumAmount, aslcode5 AS aslcode, subaslcode5 AS subaslcode, Nonsubaslcode5 AS Nonsubaslcode, ASLProduct_Code3 AS ASLProduct_Code, Hierarchy_Product_Code3 AS Hierarchy_Product_Code, Kind_Code_Mine_Sub AS KindCode, Facultative_Ind, StatisticalCoverageEffectiveDate3 AS StatisticalCoverageEffectiveDate, RunDate3 AS RunDate4, PremiumMasterCalculationID3 AS PremiumMasterCalculationID, AgencyAKID3 AS AgencyAKID, PolicyAKID3 AS PolicyAKID, ContractCustomerAKID3 AS ContractCustomerAKID, RiskLocationAKID, PolicyCoverageAKID3 AS PolicyCoverageAKID, PremiumTransactionAKID3 AS PremiumTransactionAKID, BureauStatisticalCodeAKID3 AS BureauStatisticalCodeAKID, PremiumMasterPolicyExpirationYear3 AS PremiumMasterPolicyExpirationYear, PremiumMasterPolicyTerm3 AS PremiumMasterPolicyTerm, PremiumMasterBureauPolicyType3 AS PremiumMasterBureauPolicyType, PremiumMasterAuditCode3 AS PremiumMasterAuditCode, PremiumMasterBureauStatisticalLine3 AS PremiumMasterBureauStatisticalLine, PremiumMasterProductLine3 AS PremumMasterProductLine, PremiumMasterAgencyCommissionRate3 AS PremiumMasterAgencyCommissionRate, PremiumMasterExposure3 AS PremiumMasterExposure, PremiumMasterStatisticalCode13 AS PremiumMasterStatisticalCode1, PremiumMasterStatisticalCode23 AS PremiumMasterStatisticalCode2, PremiumMasterStatisticalCode33 AS PremiumMasterStatisticalCode3, PremiumMasterRateModifier3 AS PremiumMasterRateModifier, PremiumMasterRateDeparture3 AS PremiumMasterRateDeparture, PremiumMasterBureauInceptionDate3 AS PremiumMasterBureauInceptionDate, PremiumMasterCountersignAgencyType3 AS PremiumMasterCountersignAgencyType, PremiumMasterCountersignAgencyCode3 AS PremiumMasterCountersignAgencyCode, PremiumMasterCountersignAgencyState3 AS PremiumMasterCountersignAgencyState, PremiumMasterCountersignAgencyRate3 AS PremiumMasterCountersignAgencyRate, PremiumMasterRenewalIndicator3 AS PremiumMasterRenewalIndicator, PremiumMasterRecordType3 AS PremiumMasterRecordType, ClassCode3 AS ClassCode, SubLine3 AS SubLine, premium_master_stage_id3 AS premium_master_stage_id, pm_policy_number3 AS pm_policy_number, pm_module3 AS pm_module, pm_account_date3 AS pm_account_date, pm_sar_location_number3 AS pm_sar_location_number, pm_unit_number3 AS pm_unit_number, pm_risk_state3 AS pm_risk_state, pm_risk_zone_territory3 AS pm_risk_zone_territory, pm_tax_location3 AS pm_tax_location, pm_risk_zip_code_postal_zone3 AS pm_risk_zip_code_postal_zone, pm_sar_insurance_line3 AS pm_sar_insurance_line, pm_sar_sub_location_number3 AS pm_sar_sub_location_number, pm_sar_risk_unit_group3 AS pm_sar_risk_unit_group, pm_sar_class_code_group3 AS pm_sar_class_code_group, pm_sar_class_code_member3 AS pm_sar_class_Code_member, pm_sar_sequence_risk_unit_n3 AS pm_sar_sequence_risk_unit_n, pm_sar_sequence_risk_unit_a3 AS pm_sar_sequence_risk_unit_a, pm_sar_type_exposure3 AS pm_sar_type_exposure, pm_sar_mp_seq_no3 AS pm_sar_mp_seq_no, pm_csp_inception_date3 AS pm_csp_inception_date, pm_coverage_effective_date3 AS pm_coverage_effective_date, pm_coverage_expiration_date3 AS pm_coverage_expiration_date, pm_reins_ceded_premium3 AS pm_reins_ceded_premium, pm_reins_ceded_original_premium3 AS pm_reins_ceded_original_premium, pm_reinsurance_type_code3 AS pm_reinsurance_type_code, pm_reinsurance_company_number3 AS pm_reinsurance_company_number, pm_reinsurance_ratio3 AS pm_reinsurance_ratio, AuditID3 AS AuditID, ChangeInEarnedPremium3 AS ChangeInEarnedPremium, EarnedPremiumAmount, PolicyEffectiveDate3 AS PolicyEffectiveDate, PolicyExpirationDate3 AS PolicyExpirationDate, StatisticalCoverageExpirationDate3 AS StatisticalCoverageExpirationDate, StatisticalCoverageCancellationDate3 AS StatisticalCoverageCancellationDate, ProductCode3 AS ProductCode, RatingCoverageEffectiveDate3 AS RatingCoverageEffectiveDate, RatingCoverageExpirationDate3 AS RatingCoverageExpirationDate, RatingCoverageCancellationDate3 AS RatingCoverageCancellationDate, RatingCoverageAKID3 AS RatingCoverageAKID, PolicyOfferingCode3 AS PolicyOfferingCode, strtgc_bus_dvsn_ak_id3 AS strtgc_bus_dvsn_ak_id, PolicyCoverageEffectiveDate3 AS PolicyCoverageEffectiveDate, PolicyCoverageExpirationDate3 AS PolicyCoverageExpirationDate, AgencyActualCommissionRate3 AS AgencyActualCommissionRate, InsuranceReferenceLineOfBusinessCode3 AS InsuranceReferenceLineOfBusinessCode, EnterpriseGroupCode3 AS EnterpriseGroupCode, InsuranceReferenceLegalEntityCode3 AS InsuranceReferenceLegalEntityCode, StrategicProfitCenterCode3 AS StrategicProfitCenterCode, InsuranceSegmentCode3 AS InsuranceSegmentCode, Risk_Unit_Group3 AS Risk_Unit_Group, StandardInsuranceLineCode3 AS StandardInsuranceLineCode, RatingCoverage3 AS RatingCoverage, RiskType3 AS RiskType, CoverageType3 AS CoverageType, StandardSpecialClassGroupCode3 AS StandardSpecialClassGroupCode, StandardIncreasedLimitGroupCode3 AS StandardIncreasedLimitGroupCode, StandardPackageModifcationAdjustmentGroupCode3 AS StandardPackageModifcationAdjustmentGroupCode, SourceSystemID3 AS SourceSystemID, EarnedExposure3 AS EarnedExposure1, ChangeInEarnedExposure3 AS ChangeInEarnedExposure1, RiskLocationHashKey3 AS RiskLocationHashKey1, RiskUnitSequenceNumber3 AS RiskUnitSequenceNumber, PerilGroup, CoverageForm3 AS CoverageForm, PolicyAuditAKID113 AS PolicyAuditAKID, PolicyAuditEffectiveDate113 AS PolicyAuditEffectiveDate, SubCoverageTypeCode3 AS SubCoverageTypeCode, CoverageVersion3 AS CoverageVersion, CustomerCareCommissionRate3 AS CustomerCareCommissionRate, RatingPlanCode3 AS RatingPlanCode, CoverageCancellationDate3 AS CoverageCancellationDate, GeneratedRecordIndicator3 AS GeneratedRecordIndicator, DirectWrittenPremium3 AS DirectWrittenPremium, RatablePremium3 AS RatablePremium, ClassifiedPremium3 AS ClassifiedPremium, OtherModifiedPremium3 AS OtherModifiedPremium, ScheduleModifiedPremium3 AS ScheduleModifiedPremium, ExperienceModifiedPremium3 AS ExperienceModifiedPremium, SubjectWrittenPremium3 AS SubjectWrittenPremium, EarnedDirectWrittenPremium3 AS EarnedDirectWrittenPremium, EarnedClassifiedPremium3 AS EarnedClassifiedPremium, EarnedRatablePremium3 AS EarnedRatablePremium, EarnedOtherModifiedPremium3 AS EarnedOtherModifiedPremium, EarnedScheduleModifiedPremium3 AS EarnedScheduleModifiedPremium, EarnedExperienceModifiedPremium3 AS EarnedExperienceModifiedPremium, EarnedSubjectWrittenPremium3 AS EarnedSubjectWrittenPremium, EarnedPremiumRunDate3 AS EarnedPremiumRunDate, PremiumMasterWrittenExposure3 AS PremiumMasterWrittenExposure, DeclaredEventFlag3 AS DeclaredEventFlag
		FROM EXP_Mine_Subsidence_Row
		UNION
		SELECT PolicyKey AS PolicyKey1, PremiumTransactionID AS PremiumTransactionID1, ReinsuranceCoverageAKID AS ReinsuranceCoverageAKID1, StatisticalCoverageAKID AS StatisticalCoverageAKID1, PremiumTransactionCode AS PremiumTransactionCode1, PremiumTransactionEnteredDate AS PremiumTransactionEnteredDate1, PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate1, PremiumTransactionExpirationDate AS PremiumTransactionExpirationDate1, PremiumTransactionBookedDate AS PremiumTransactionBookedDate1, PremiumType AS PremiumType1, ReasonAmendedCode AS ReasonAmendedCode1, PolicySymbol, TypeBureauCode, MajorPerilCode, RiskUnit, nsi_indicator, symbol_pos_1_2, o_PremiumAmount AS PremiumAmount_Out, o_FullTermPremiumAmount AS FullTermPremiumAmount, aslcode, subaslcode, Nonsubaslcode, ASLProduct_Code, Hierarchy_Product_Code, StatisticalCoverageEffectiveDate, RunDate AS RunDate4, PremiumMasterCalculationID, AgencyAKID, PolicyAKID, ContractCustomerAKID, RiskLocationAKID, PolicyCoverageAKID, PremiumTransactionAKID, BureauStatisticalCodeAKID, PremiumMasterPolicyExpirationYear, PremiumMasterPolicyTerm, PremiumMasterBureauPolicyType, PremiumMasterAuditCode, PremiumMasterBureauStatisticalLine, PremiumMasterProductLine AS PremumMasterProductLine, PremiumMasterAgencyCommissionRate, PremiumMasterExposure, PremiumMasterStatisticalCode1, PremiumMasterStatisticalCode2, PremiumMasterStatisticalCode3, PremiumMasterRateModifier, PremiumMasterRateDeparture, PremiumMasterBureauInceptionDate, PremiumMasterCountersignAgencyType, PremiumMasterCountersignAgencyCode, PremiumMasterCountersignAgencyState, PremiumMasterCountersignAgencyRate, PremiumMasterRenewalIndicator, PremiumMasterRecordType, ClassCode, SubLine, premium_master_stage_id, pm_policy_number, pm_module, pm_account_date, pm_sar_location_number, pm_unit_number, pm_risk_state, pm_risk_zone_territory, pm_tax_location, pm_risk_zip_code_postal_zone, pm_sar_insurance_line, pm_sar_sub_location_number, pm_sar_risk_unit_group, pm_sar_class_code_group, pm_sar_class_code_member AS pm_sar_class_Code_member, pm_sar_sequence_risk_unit_n, pm_sar_sequence_risk_unit_a, pm_sar_type_exposure, pm_sar_mp_seq_no, pm_csp_inception_date, pm_coverage_effective_date, pm_coverage_expiration_date, o_pm_reins_ceded_premium AS pm_reins_ceded_premium, o_pm_reins_ceded_original_premium AS pm_reins_ceded_original_premium, pm_reinsurance_type_code, pm_reinsurance_company_number, pm_reinsurance_ratio, AuditID, o_ChangeInEarnedPremium AS ChangeInEarnedPremium, o_EarnedPremiumAmount AS EarnedPremiumAmount, PolicyEffectiveDate, PolicyExpirationDate, StatisticalCoverageExpirationDate, StatisticalCoverageCancellationDate, ProductCode, RatingCoverageEffectiveDate, RatingCoverageExpirationDate, RatingCoverageCancellationDate, RatingCoverageAKID, PolicyOfferingCode, strtgc_bus_dvsn_ak_id, PolicyCoverageEffectiveDate, PolicyCoverageExpirationDate, AgencyActualCommissionRate, InsuranceReferenceLineOfBusinessCode, EnterpriseGroupCode, InsuranceReferenceLegalEntityCode, StrategicProfitCenterCode, InsuranceSegmentCode, Risk_Unit_Group, StandardInsuranceLineCode, RatingCoverage, RiskType, CoverageType, StandardSpecialClassGroupCode, StandardIncreasedLimitGroupCode, StandardPackageModifcationAdjustmentGroupCode, SourceSystemID, EarnedExposure AS EarnedExposure1, ChangeInEarnedExposure AS ChangeInEarnedExposure1, RiskLocationHashKey AS RiskLocationHashKey1, RiskUnitSequenceNumber, PerilGroup, CoverageForm, AnnualStatementLineCode_DCT, SubAnnualStatementLineCode_DCT, PolicyAuditAKID, PolicyAuditEffectiveDate, SubCoverageTypeCode, CoverageVersion, SubNonAnnualStatementLineCode_DCT, CustomerCareCommissionRate10 AS CustomerCareCommissionRate, RatingPlanCode10 AS RatingPlanCode, CoverageCancellationDate10 AS CoverageCancellationDate, GeneratedRecordIndicator10 AS GeneratedRecordIndicator, o_DirectWrittenPremium10 AS DirectWrittenPremium, o_RatablePremium10 AS RatablePremium, o_ClassifiedPremium10 AS ClassifiedPremium, o_OtherModifiedPremium10 AS OtherModifiedPremium, o_ScheduleModifiedPremium10 AS ScheduleModifiedPremium, o_ExperienceModifiedPremium10 AS ExperienceModifiedPremium, o_i_SubjectWrittenPremium10 AS SubjectWrittenPremium, i_EarnedDirectWrittenPremium10 AS EarnedDirectWrittenPremium, i_EarnedClassifiedPremium10 AS EarnedClassifiedPremium, i_EarnedRatablePremium10 AS EarnedRatablePremium, i_EarnedOtherModifiedPremium10 AS EarnedOtherModifiedPremium, i_EarnedScheduleModifiedPremium10 AS EarnedScheduleModifiedPremium, i_EarnedExperienceModifiedPremium10 AS EarnedExperienceModifiedPremium, i_EarnedSubjectWrittenPremium10 AS EarnedSubjectWrittenPremium, i_EarnedPremiumRunDate10 AS EarnedPremiumRunDate, PremiumMasterWrittenExposure10 AS PremiumMasterWrittenExposure, DeclaredEventFlag10 AS DeclaredEventFlag
		FROM EXP_ASL_DCT
	),
	EXPTRANS AS (
		SELECT
		PolicyKey1,
		PremiumTransactionID1 AS PremiumTransactionID,
		ReinsuranceCoverageAKID1 AS ReinsuranceCoverageAKID,
		StatisticalCoverageAKID1 AS StatisticalCoverageAKID,
		PremiumTransactionCode1 AS PremiumTransactionCode,
		PremiumTransactionEnteredDate1 AS PremiumTransactionEnteredDate,
		PremiumTransactionEffectiveDate1 AS PremiumTransactionEffectiveDate,
		PremiumTransactionExpirationDate1 AS PremiumTransactionExpirationDate,
		PremiumTransactionBookedDate1 AS PremiumTransactionBookedDate,
		PremiumType1 AS PremiumType,
		ReasonAmendedCode1 AS ReasonAmendedCode,
		PolicySymbol,
		TypeBureauCode,
		MajorPerilCode,
		RiskUnit,
		RiskUnitSequenceNumber,
		nsi_indicator,
		symbol_pos_1_2,
		PremiumAmount_Out AS PremiumAmount,
		FullTermPremiumAmount,
		EarnedPremiumAmount,
		ChangeInEarnedPremium,
		aslcode,
		subaslcode,
		Nonsubaslcode,
		ASLProduct_Code AS ASLProductCode,
		Hierarchy_Product_Code AS HierarchyProductCode,
		KindCode AS Kind_Code_Mine_Sub,
		Facultative_Ind,
		StatisticalCoverageEffectiveDate AS StatisticalCoverageEffectiveDate4,
		RunDate4,
		strtgc_bus_dvsn_ak_id,
		AnnualStatementLineCode_DCT,
		SubAnnualStatementLineCode_DCT,
		SubNonAnnualStatementLineCode_DCT,
		-- *INF*: IIF(ISNULL(AnnualStatementLineCode_DCT),'N/A',AnnualStatementLineCode_DCT)
		IFF(AnnualStatementLineCode_DCT IS NULL, 'N/A', AnnualStatementLineCode_DCT) AS v_AnnualStatementLineCode_DCT,
		-- *INF*: IIF(ISNULL(SubAnnualStatementLineCode_DCT),'N/A',SubAnnualStatementLineCode_DCT)
		IFF(SubAnnualStatementLineCode_DCT IS NULL, 'N/A', SubAnnualStatementLineCode_DCT) AS v_SubAnnualStatementLineCode_DCT,
		-- *INF*: DECODE(True,
		-- SourceSystemID='PMS',:LKP.LKP_ASL_DIM(aslcode, subaslcode, Nonsubaslcode),
		-- SourceSystemID='DCT',:LKP.LKP_ASL_DIM(v_AnnualStatementLineCode_DCT,v_SubAnnualStatementLineCode_DCT, SubNonAnnualStatementLineCode_DCT),-1)
		DECODE(
		    True,
		    SourceSystemID = 'PMS', LKP_ASL_DIM_aslcode_subaslcode_Nonsubaslcode.asl_dim_id,
		    SourceSystemID = 'DCT', LKP_ASL_DIM_v_AnnualStatementLineCode_DCT_v_SubAnnualStatementLineCode_DCT_SubNonAnnualStatementLineCode_DCT.asl_dim_id,
		    - 1
		) AS v_asldimID,
		-- *INF*: :LKP.LKP_ASL_PRODUCT_CODE(ASLProductCode)
		LKP_ASL_PRODUCT_CODE_ASLProductCode.asl_prdct_code_dim_id AS v_aslproductcodedimID,
		-- *INF*: :LKP.LKP_PRODUCT_CODE_DIM(HierarchyProductCode)
		LKP_PRODUCT_CODE_DIM_HierarchyProductCode.prdct_code_dim_id AS v_productcodedimID,
		-- *INF*: :LKP.LKP_STRATEGIC_BUSINESS_DIVISION_DIM(strtgc_bus_dvsn_ak_id)
		LKP_STRATEGIC_BUSINESS_DIVISION_DIM_strtgc_bus_dvsn_ak_id.strtgc_bus_dvsn_dim_id AS v_strategicbusinessdivisiondimID,
		-- *INF*: IIF(ISNULL(v_asldimID),-1,v_asldimID)
		IFF(v_asldimID IS NULL, - 1, v_asldimID) AS o_asldimID,
		-- *INF*: IIF(ISNULL(v_aslproductcodedimID),-1,v_aslproductcodedimID)
		IFF(v_aslproductcodedimID IS NULL, - 1, v_aslproductcodedimID) AS o_aslproductcodedimID,
		-- *INF*: IIF(ISNULL(v_productcodedimID),-1,v_productcodedimID)
		IFF(v_productcodedimID IS NULL, - 1, v_productcodedimID) AS o_productcodedimID,
		-- *INF*: IIF(ISNULL(v_strategicbusinessdivisiondimID),-1,v_strategicbusinessdivisiondimID)
		IFF(v_strategicbusinessdivisiondimID IS NULL, - 1, v_strategicbusinessdivisiondimID) AS o_strategicbusinessdivisiondimID,
		PremiumMasterCalculationID,
		AgencyAKID,
		PolicyAKID,
		ContractCustomerAKID,
		RiskLocationAKID,
		PolicyCoverageAKID,
		PremiumTransactionAKID,
		BureauStatisticalCodeAKID,
		PremiumMasterPolicyExpirationYear,
		PremiumMasterPolicyTerm,
		PremiumMasterBureauPolicyType,
		PremiumMasterAuditCode,
		PremiumMasterBureauStatisticalLine,
		PremumMasterProductLine AS PremiumMasterProductLine,
		PremiumMasterAgencyCommissionRate,
		PremiumMasterExposure,
		PremiumMasterStatisticalCode1,
		PremiumMasterStatisticalCode2,
		PremiumMasterStatisticalCode3,
		PremiumMasterRateModifier,
		PremiumMasterRateDeparture,
		PremiumMasterBureauInceptionDate,
		PremiumMasterCountersignAgencyType,
		PremiumMasterCountersignAgencyCode,
		PremiumMasterCountersignAgencyState,
		PremiumMasterCountersignAgencyRate,
		PremiumMasterRenewalIndicator,
		PremiumMasterRecordType,
		ClassCode,
		SubLine,
		premium_master_stage_id,
		pm_policy_number,
		pm_module,
		pm_account_date,
		pm_sar_location_number,
		pm_unit_number,
		pm_risk_state,
		pm_risk_zone_territory,
		pm_tax_location,
		pm_risk_zip_code_postal_zone,
		pm_sar_insurance_line,
		pm_sar_sub_location_number,
		pm_sar_risk_unit_group,
		pm_sar_class_code_group,
		pm_sar_class_Code_member AS pm_sar_class_code_member,
		pm_unit_number AS pm_unit_number1,
		pm_sar_sequence_risk_unit_n,
		pm_sar_sequence_risk_unit_a,
		pm_sar_type_exposure,
		pm_sar_mp_seq_no,
		pm_csp_inception_date,
		pm_coverage_effective_date,
		pm_coverage_expiration_date,
		pm_reins_ceded_premium AS pm_reinsurance_ceded_premium,
		pm_reins_ceded_original_premium AS pm_reins_ceded_orig_premium,
		pm_reinsurance_type_code,
		pm_reinsurance_company_number,
		pm_reinsurance_ratio,
		AuditID,
		-- *INF*: IIF(PremiumType='C' AND MajorPerilCode='050',050,AuditID)
		IFF(PremiumType = 'C' AND MajorPerilCode = '050', 050, AuditID) AS o_AuditID,
		PolicyEffectiveDate,
		PolicyExpirationDate,
		StatisticalCoverageExpirationDate,
		StatisticalCoverageCancellationDate,
		ProductCode,
		RatingCoverageEffectiveDate,
		RatingCoverageExpirationDate,
		RatingCoverageCancellationDate,
		RatingCoverageAKID,
		PolicyOfferingCode,
		PolicyCoverageEffectiveDate,
		PolicyCoverageExpirationDate,
		AgencyActualCommissionRate,
		InsuranceReferenceLineOfBusinessCode,
		EnterpriseGroupCode,
		InsuranceReferenceLegalEntityCode,
		StrategicProfitCenterCode,
		InsuranceSegmentCode,
		Risk_Unit_Group,
		StandardInsuranceLineCode,
		RatingCoverage,
		RiskType,
		CoverageType,
		StandardSpecialClassGroupCode,
		StandardIncreasedLimitGroupCode,
		StandardPackageModifcationAdjustmentGroupCode,
		SourceSystemID,
		EarnedExposure1 AS EarnedExposure,
		ChangeInEarnedExposure1 AS ChangeInEarnedExposure,
		RiskLocationHashKey1 AS RiskLocationHashKey,
		PerilGroup,
		CoverageForm,
		PolicyAuditAKID,
		PolicyAuditEffectiveDate,
		SubCoverageTypeCode,
		CoverageVersion,
		CustomerCareCommissionRate,
		RatingPlanCode,
		CoverageCancellationDate AS CoverageCancellationDate1,
		GeneratedRecordIndicator AS GeneratedRecordIndicator1,
		DirectWrittenPremium AS DirectWrittenPremium1,
		RatablePremium AS RatablePremium1,
		ClassifiedPremium AS ClassifiedPremium1,
		OtherModifiedPremium AS OtherModifiedPremium1,
		ScheduleModifiedPremium AS ScheduleModifiedPremium1,
		ExperienceModifiedPremium AS ExperienceModifiedPremium1,
		SubjectWrittenPremium AS SubjectWrittenPremium1,
		EarnedDirectWrittenPremium AS EarnedDirectWrittenPremium1,
		EarnedClassifiedPremium AS EarnedClassifiedPremium1,
		EarnedRatablePremium AS EarnedRatablePremium1,
		EarnedOtherModifiedPremium AS EarnedOtherModifiedPremium1,
		EarnedScheduleModifiedPremium AS EarnedScheduleModifiedPremium1,
		EarnedExperienceModifiedPremium AS EarnedExperienceModifiedPremium1,
		EarnedSubjectWrittenPremium AS EarnedSubjectWrittenPremium1,
		EarnedPremiumRunDate AS EarnedPremiumRunDate1,
		PremiumMasterWrittenExposure,
		DeclaredEventFlag AS DeclaredEventFlag10
		FROM Union
		LEFT JOIN LKP_ASL_DIM LKP_ASL_DIM_aslcode_subaslcode_Nonsubaslcode
		ON LKP_ASL_DIM_aslcode_subaslcode_Nonsubaslcode.asl_code = aslcode
		AND LKP_ASL_DIM_aslcode_subaslcode_Nonsubaslcode.sub_asl_code = subaslcode
		AND LKP_ASL_DIM_aslcode_subaslcode_Nonsubaslcode.sub_non_asl_code = Nonsubaslcode
	
		LEFT JOIN LKP_ASL_DIM LKP_ASL_DIM_v_AnnualStatementLineCode_DCT_v_SubAnnualStatementLineCode_DCT_SubNonAnnualStatementLineCode_DCT
		ON LKP_ASL_DIM_v_AnnualStatementLineCode_DCT_v_SubAnnualStatementLineCode_DCT_SubNonAnnualStatementLineCode_DCT.asl_code = v_AnnualStatementLineCode_DCT
		AND LKP_ASL_DIM_v_AnnualStatementLineCode_DCT_v_SubAnnualStatementLineCode_DCT_SubNonAnnualStatementLineCode_DCT.sub_asl_code = v_SubAnnualStatementLineCode_DCT
		AND LKP_ASL_DIM_v_AnnualStatementLineCode_DCT_v_SubAnnualStatementLineCode_DCT_SubNonAnnualStatementLineCode_DCT.sub_non_asl_code = SubNonAnnualStatementLineCode_DCT
	
		LEFT JOIN LKP_ASL_PRODUCT_CODE LKP_ASL_PRODUCT_CODE_ASLProductCode
		ON LKP_ASL_PRODUCT_CODE_ASLProductCode.asl_prdct_code = ASLProductCode
	
		LEFT JOIN LKP_PRODUCT_CODE_DIM LKP_PRODUCT_CODE_DIM_HierarchyProductCode
		ON LKP_PRODUCT_CODE_DIM_HierarchyProductCode.prdct_code = HierarchyProductCode
	
		LEFT JOIN LKP_STRATEGIC_BUSINESS_DIVISION_DIM LKP_STRATEGIC_BUSINESS_DIVISION_DIM_strtgc_bus_dvsn_ak_id
		ON LKP_STRATEGIC_BUSINESS_DIVISION_DIM_strtgc_bus_dvsn_ak_id.edw_strtgc_bus_dvsn_ak_id = strtgc_bus_dvsn_ak_id
	
	),
	OUTPUT AS (
		SELECT
		PolicyKey1 AS PolicyKey, 
		PremiumTransactionID AS O_PremiumTransactionID, 
		ReinsuranceCoverageAKID AS O_ReinsuranceCoverageAKID, 
		StatisticalCoverageAKID AS O_StatisticalCoverageAKID, 
		PremiumTransactionCode AS O_PremiumTransactionCode, 
		PremiumTransactionEnteredDate AS O_PremiumTransactionEnteredDate, 
		PremiumTransactionEffectiveDate AS O_PremiumTransactionEffectiveDate, 
		PremiumTransactionExpirationDate AS O_PremiumTransactionExpirationDate, 
		PremiumTransactionBookedDate AS O_PremiumTransactionBookedDate, 
		PremiumType AS O_PremiumType, 
		ReasonAmendedCode AS O_ReasonAmendedCode, 
		PolicySymbol AS O_PolicySymbol, 
		TypeBureauCode AS o_TypeBureauCode, 
		MajorPerilCode AS o_MajorPerilCode, 
		RiskUnit AS o_RiskUnit, 
		RiskUnitSequenceNumber AS o_RiskUnitSequenceNumber, 
		nsi_indicator AS o_nsi_indicator, 
		symbol_pos_1_2 AS o_symbol_pos_1_2, 
		PremiumAmount AS o_PremiumAmount, 
		FullTermPremiumAmount AS o_FullTermPremiumAmount, 
		EarnedPremiumAmount AS o_EarnedPremiumAmount, 
		ChangeInEarnedPremium AS o_ChangeInEarnedPremium, 
		aslcode AS o_aslcode, 
		subaslcode AS o_subaslcode, 
		Nonsubaslcode AS o_Nonsubaslcode, 
		ASLProductCode AS o_ASLProductCode, 
		HierarchyProductCode AS o_HierarchyProductCode, 
		Kind_Code_Mine_Sub, 
		Facultative_Ind, 
		StatisticalCoverageEffectiveDate4 AS StatisticalCoverageEffectiveDate, 
		RunDate4 AS RunDate, 
		o_asldimID, 
		o_aslproductcodedimID, 
		o_productcodedimID, 
		o_strategicbusinessdivisiondimID, 
		PremiumMasterCalculationID, 
		AgencyAKID, 
		PolicyAKID, 
		ContractCustomerAKID, 
		RiskLocationAKID, 
		PolicyCoverageAKID, 
		PremiumTransactionAKID, 
		BureauStatisticalCodeAKID, 
		PremiumMasterPolicyExpirationYear, 
		PremiumMasterPolicyTerm, 
		PremiumMasterBureauPolicyType, 
		PremiumMasterAuditCode, 
		PremiumMasterBureauStatisticalLine, 
		PremiumMasterProductLine, 
		PremiumMasterAgencyCommissionRate, 
		PremiumMasterExposure, 
		PremiumMasterStatisticalCode1, 
		PremiumMasterStatisticalCode2, 
		PremiumMasterStatisticalCode3, 
		PremiumMasterRateModifier, 
		PremiumMasterRateDeparture, 
		PremiumMasterBureauInceptionDate, 
		PremiumMasterCountersignAgencyType, 
		PremiumMasterCountersignAgencyCode, 
		PremiumMasterCountersignAgencyState, 
		PremiumMasterCountersignAgencyRate, 
		PremiumMasterRenewalIndicator, 
		PremiumMasterRecordType, 
		ClassCode, 
		SubLine, 
		premium_master_stage_id, 
		pm_policy_number, 
		pm_module, 
		pm_account_date, 
		pm_sar_location_number, 
		pm_unit_number, 
		pm_risk_state, 
		pm_risk_zone_territory, 
		pm_tax_location, 
		pm_risk_zip_code_postal_zone, 
		pm_sar_insurance_line, 
		pm_sar_sub_location_number, 
		pm_sar_risk_unit_group, 
		pm_sar_class_code_group, 
		pm_sar_class_code_member, 
		pm_unit_number1, 
		pm_sar_sequence_risk_unit_n, 
		pm_sar_sequence_risk_unit_a, 
		pm_sar_type_exposure, 
		pm_sar_mp_seq_no, 
		pm_csp_inception_date, 
		pm_coverage_effective_date, 
		pm_coverage_expiration_date, 
		pm_reinsurance_ceded_premium, 
		pm_reins_ceded_orig_premium, 
		pm_reinsurance_type_code, 
		pm_reinsurance_company_number, 
		pm_reinsurance_ratio, 
		o_AuditID, 
		PolicyEffectiveDate AS o_PolicyEffectiveDate, 
		PolicyExpirationDate AS o_PolicyExpirationDate, 
		StatisticalCoverageExpirationDate AS o_StatisticalCoverageExpirationDate, 
		StatisticalCoverageCancellationDate AS o_StatisticalCoverageCancellationDate, 
		ProductCode, 
		RatingCoverageEffectiveDate, 
		RatingCoverageExpirationDate, 
		RatingCoverageCancellationDate, 
		RatingCoverageAKID, 
		PolicyOfferingCode, 
		PolicyCoverageEffectiveDate, 
		PolicyCoverageExpirationDate, 
		AgencyActualCommissionRate, 
		InsuranceReferenceLineOfBusinessCode, 
		EnterpriseGroupCode, 
		InsuranceReferenceLegalEntityCode, 
		StrategicProfitCenterCode, 
		InsuranceSegmentCode, 
		Risk_Unit_Group, 
		StandardInsuranceLineCode, 
		RatingCoverage, 
		RiskType, 
		CoverageType, 
		StandardSpecialClassGroupCode, 
		StandardIncreasedLimitGroupCode, 
		StandardPackageModifcationAdjustmentGroupCode, 
		SourceSystemID, 
		EarnedExposure, 
		ChangeInEarnedExposure, 
		RiskLocationHashKey, 
		PerilGroup, 
		CoverageForm, 
		PolicyAuditAKID, 
		PolicyAuditEffectiveDate, 
		SubCoverageTypeCode, 
		CoverageVersion, 
		AnnualStatementLineCode_DCT, 
		SubAnnualStatementLineCode_DCT, 
		SubNonAnnualStatementLineCode_DCT, 
		CustomerCareCommissionRate, 
		RatingPlanCode, 
		CoverageCancellationDate1, 
		GeneratedRecordIndicator1, 
		DirectWrittenPremium1, 
		RatablePremium1, 
		ClassifiedPremium1, 
		OtherModifiedPremium1, 
		ScheduleModifiedPremium1, 
		ExperienceModifiedPremium1, 
		SubjectWrittenPremium1, 
		EarnedDirectWrittenPremium1, 
		EarnedClassifiedPremium1, 
		EarnedRatablePremium1, 
		EarnedOtherModifiedPremium1, 
		EarnedScheduleModifiedPremium1, 
		EarnedExperienceModifiedPremium1, 
		EarnedSubjectWrittenPremium1, 
		EarnedPremiumRunDate1, 
		PremiumMasterWrittenExposure, 
		DeclaredEventFlag10 AS DeclaredEventFlag
		FROM EXPTRANS
	),
),
LKP_CoverageDetailDim AS (
	SELECT
	CoverageDetailDimId,
	ClassCode,
	PremiumTransactionAKId
	FROM (
		select CDD.CoverageDetailDimId as CoverageDetailDimId, 
		CDD.ClassCode as ClassCode,
		PT.PremiumTransactionAKId as PremiumTransactionAKId  
		from  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation PC 
		join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		on PC.PremiumTransactionAKId=PT.PremiumTransactionAKId
		join @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
		on PT.PremiumTransactionID=CDD.EDWPremiumTransactionPKID
		where @{pipeline().parameters.WHERE_PMC_CURRENT_MONTH}
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKId ORDER BY CoverageDetailDimId) = 1
),
FILTRANS AS (
	SELECT
	mplt_Premium_ASL_Insurance_Hierarchy.PremiumMasterBureauInceptionDate1, 
	mplt_Premium_ASL_Insurance_Hierarchy.O_PremiumTransactionCode, 
	mplt_Premium_ASL_Insurance_Hierarchy.O_ReasonAmendedCode, 
	mplt_Premium_ASL_Insurance_Hierarchy.O_PremiumTransactionID, 
	mplt_Premium_ASL_Insurance_Hierarchy.PremiumMasterCalculationID1, 
	mplt_Premium_ASL_Insurance_Hierarchy.PolicyAKID1, 
	mplt_Premium_ASL_Insurance_Hierarchy.O_PremiumTransactionBookedDate, 
	mplt_Premium_ASL_Insurance_Hierarchy.PremiumMasterPolicyExpirationYear1, 
	mplt_Premium_ASL_Insurance_Hierarchy.O_PremiumTransactionEffectiveDate, 
	mplt_Premium_ASL_Insurance_Hierarchy.O_PremiumTransactionExpirationDate, 
	mplt_Premium_ASL_Insurance_Hierarchy.PremiumMasterPolicyTerm1, 
	mplt_Premium_ASL_Insurance_Hierarchy.PremiumMasterBureauPolicyType1, 
	mplt_Premium_ASL_Insurance_Hierarchy.PremiumMasterAuditCode1, 
	mplt_Premium_ASL_Insurance_Hierarchy.O_PremiumType, 
	mplt_Premium_ASL_Insurance_Hierarchy.o_TypeBureauCode, 
	mplt_Premium_ASL_Insurance_Hierarchy.PremiumMasterBureauStatisticalLine1, 
	mplt_Premium_ASL_Insurance_Hierarchy.PremiumMasterProductLine1, 
	mplt_Premium_ASL_Insurance_Hierarchy.ClassCode1, 
	mplt_Premium_ASL_Insurance_Hierarchy.PremiumMasterExposure1, 
	mplt_Premium_ASL_Insurance_Hierarchy.SubLine, 
	mplt_Premium_ASL_Insurance_Hierarchy.PremiumMasterStatisticalCode11, 
	mplt_Premium_ASL_Insurance_Hierarchy.PremiumMasterStatisticalCode21, 
	mplt_Premium_ASL_Insurance_Hierarchy.PremiumMasterStatisticalCode31, 
	mplt_Premium_ASL_Insurance_Hierarchy.PremiumMasterRateModifier1, 
	mplt_Premium_ASL_Insurance_Hierarchy.PremiumMasterRateDeparture1, 
	mplt_Premium_ASL_Insurance_Hierarchy.PremiumMasterCountersignAgencyType1, 
	mplt_Premium_ASL_Insurance_Hierarchy.PremiumMasterCountersignAgencyCode1, 
	mplt_Premium_ASL_Insurance_Hierarchy.PremiumMasterCountersignAgencyState1, 
	mplt_Premium_ASL_Insurance_Hierarchy.PremiumMasterCountersignAgencyRate1, 
	mplt_Premium_ASL_Insurance_Hierarchy.o_PremiumAmount, 
	mplt_Premium_ASL_Insurance_Hierarchy.o_FullTermPremiumAmount, 
	mplt_Premium_ASL_Insurance_Hierarchy.PremiumMasterRenewalIndicator1, 
	mplt_Premium_ASL_Insurance_Hierarchy.o_asldimID, 
	mplt_Premium_ASL_Insurance_Hierarchy.o_aslproductcodedimID, 
	mplt_Premium_ASL_Insurance_Hierarchy.o_AuditID, 
	mplt_Premium_ASL_Insurance_Hierarchy.ProductCode1, 
	mplt_Premium_ASL_Insurance_Hierarchy.PolicyOfferingCode1, 
	mplt_Premium_ASL_Insurance_Hierarchy.InsuranceReferenceLineOfBusinessCode1, 
	mplt_Premium_ASL_Insurance_Hierarchy.EnterpriseGroupCode1, 
	mplt_Premium_ASL_Insurance_Hierarchy.InsuranceReferenceLegalEntityCode1, 
	mplt_Premium_ASL_Insurance_Hierarchy.StrategicProfitCenterCode1, 
	mplt_Premium_ASL_Insurance_Hierarchy.InsuranceSegmentCode1, 
	mplt_Premium_ASL_Insurance_Hierarchy.SourceSystemID1, 
	mplt_Premium_ASL_Insurance_Hierarchy.Risk_Unit_Group, 
	mplt_Premium_ASL_Insurance_Hierarchy.StandardInsuranceLineCode1, 
	mplt_Premium_ASL_Insurance_Hierarchy.RiskType1, 
	mplt_Premium_ASL_Insurance_Hierarchy.CoverageType1, 
	mplt_Premium_ASL_Insurance_Hierarchy.o_MajorPerilCode, 
	mplt_Premium_ASL_Insurance_Hierarchy.o_RiskUnit, 
	mplt_Premium_ASL_Insurance_Hierarchy.o_RiskUnitSequenceNumber, 
	mplt_Premium_ASL_Insurance_Hierarchy.RiskLocationHashKey1, 
	mplt_Premium_ASL_Insurance_Hierarchy.pm_risk_state1, 
	mplt_Premium_ASL_Insurance_Hierarchy.PerilGroup1, 
	mplt_Premium_ASL_Insurance_Hierarchy.PolicyAuditAKID1, 
	mplt_Premium_ASL_Insurance_Hierarchy.PolicyAuditEffectiveDate1, 
	mplt_Premium_ASL_Insurance_Hierarchy.SubCoverageTypeCode1, 
	mplt_Premium_ASL_Insurance_Hierarchy.CoverageVersion1, 
	mplt_Premium_ASL_Insurance_Hierarchy.O_PremiumTransactionEnteredDate, 
	mplt_Premium_ASL_Insurance_Hierarchy.CustomerCareCommissionRate1, 
	mplt_Premium_ASL_Insurance_Hierarchy.RatingPlanCode1, 
	mplt_Premium_ASL_Insurance_Hierarchy.GeneratedRecordIndicator1, 
	mplt_Premium_ASL_Insurance_Hierarchy.DirectWrittenPremium1, 
	mplt_Premium_ASL_Insurance_Hierarchy.RatablePremium1, 
	mplt_Premium_ASL_Insurance_Hierarchy.ClassifiedPremium1, 
	mplt_Premium_ASL_Insurance_Hierarchy.OtherModifiedPremium1, 
	mplt_Premium_ASL_Insurance_Hierarchy.ScheduleModifiedPremium1, 
	mplt_Premium_ASL_Insurance_Hierarchy.ExperienceModifiedPremium1, 
	mplt_Premium_ASL_Insurance_Hierarchy.SubjectWrittenPremium1, 
	mplt_Premium_ASL_Insurance_Hierarchy.O_StatisticalCoverageAKID, 
	mplt_Premium_ASL_Insurance_Hierarchy.PremiumTransactionAKID1, 
	LKP_CoverageDetailDim.CoverageDetailDimId, 
	LKP_CoverageDetailDim.ClassCode, 
	mplt_Premium_ASL_Insurance_Hierarchy.RunDate1 AS RunDate
	FROM mplt_Premium_ASL_Insurance_Hierarchy
	LEFT JOIN LKP_CoverageDetailDim
	ON LKP_CoverageDetailDim.PremiumTransactionAKId = mplt_Premium_ASL_Insurance_Hierarchy.PremiumTransactionAKID1
	WHERE O_PremiumType='D'
),
EXP_PremiumMasterCalculation_IN AS (
	SELECT
	CoverageDetailDimId AS i_CoverageDetailDimId,
	ClassCode AS i_ClassCode,
	PremiumMasterBureauInceptionDate1 AS i_PremiumMasterBureauInceptionDate,
	O_PremiumTransactionCode AS i_PremiumMasterTransactionCode,
	O_ReasonAmendedCode AS i_ReasonAmendedCode,
	O_PremiumTransactionID AS PremiumTransactionID,
	PremiumMasterCalculationID1 AS PremiumMasterCalculationID,
	PolicyAKID1 AS PolicyAKID,
	O_PremiumTransactionBookedDate AS PremiumMasterBookedDate,
	PremiumMasterPolicyExpirationYear1 AS PremiumMasterPolicyExpirationYear,
	O_PremiumTransactionEffectiveDate AS PremiumMasterCoverageEffectiveDate,
	O_PremiumTransactionExpirationDate AS PremiumMasterCoverageExpirationDate,
	PremiumMasterPolicyTerm1 AS PremiumMasterPolicyTerm,
	PremiumMasterBureauPolicyType1 AS PremiumMasterBureauPolicyType,
	PremiumMasterAuditCode1 AS PremiumMasterAuditCode,
	O_PremiumType AS PremiumMasterPremiumType,
	o_TypeBureauCode AS PremiumMasterTypeBureauCode,
	PremiumMasterBureauStatisticalLine1 AS PremiumMasterBureauStatisticalLine,
	PremiumMasterProductLine1 AS PremiumMasterProductLine,
	ClassCode1 AS PremiumMasterClassCode,
	PremiumMasterExposure1 AS PremiumMasterExposure,
	SubLine AS PremiumMasterSubLine,
	PremiumMasterStatisticalCode11 AS PremiumMasterStatisticalCode1,
	PremiumMasterStatisticalCode21 AS PremiumMasterStatisticalCode2,
	PremiumMasterStatisticalCode31 AS PremiumMasterStatisticalCode3,
	PremiumMasterRateModifier1 AS PremiumMasterRateModifier,
	PremiumMasterRateDeparture1 AS PremiumMasterRateDeparture,
	PremiumMasterCountersignAgencyType1 AS PremiumMasterCountersignAgencyType,
	PremiumMasterCountersignAgencyCode1 AS PremiumMasterCountersignAgencyCode,
	PremiumMasterCountersignAgencyState1 AS PremiumMasterCountersignAgencyState,
	PremiumMasterCountersignAgencyRate1 AS PremiumMasterCountersignAgencyRate,
	o_PremiumAmount AS PremiumMasterPremium,
	o_FullTermPremiumAmount AS PremiumMasterFullTermPremium,
	PremiumMasterRenewalIndicator1 AS PremiumMasterRenewalIndicator,
	o_asldimID AS asldimID,
	o_aslproductcodedimID AS aslproductcodedimID,
	o_AuditID AS AuditID,
	ProductCode1 AS ProductCode,
	PolicyOfferingCode1 AS PolicyOfferingCode,
	InsuranceReferenceLineOfBusinessCode1 AS InsuranceReferenceLineOfBusinessCode,
	EnterpriseGroupCode1 AS EnterpriseGroupCode,
	InsuranceReferenceLegalEntityCode1 AS InsuranceReferenceLegalEntityCode,
	StrategicProfitCenterCode1 AS StrategicProfitCenterCode,
	InsuranceSegmentCode1 AS InsuranceSegmentCode,
	SourceSystemID1 AS SourceSystemID,
	Risk_Unit_Group AS i_Risk_Unit_Group,
	StandardInsuranceLineCode1 AS i_StandardInsuranceLineCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_MajorPerilCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_MajorPerilCode) AS v_MajorPerilCode,
	-- *INF*: IIF(LTRIM(v_MajorPerilCode,'0')='','N/A',v_MajorPerilCode)
	IFF(LTRIM(v_MajorPerilCode, '0') = '', 'N/A', v_MajorPerilCode) AS v_Zero_MajorPerilCode,
	-- *INF*: IIF(REG_MATCH(i_StandardInsuranceLineCode,'[^0-9a-zA-Z]'),'N/A',i_StandardInsuranceLineCode)
	IFF(
	    REGEXP_LIKE(i_StandardInsuranceLineCode, '[^0-9a-zA-Z]'), 'N/A', i_StandardInsuranceLineCode
	) AS v_Reg_StandardInsuranceLineCode,
	-- *INF*: IIF(v_Reg_StandardInsuranceLineCode='N/A' AND (IN(PremiumMasterTypeBureauCode,'AL','AN','AP') OR IN(v_Zero_MajorPerilCode,'930','931')),'CA',v_Reg_StandardInsuranceLineCode)
	IFF(
	    v_Reg_StandardInsuranceLineCode = 'N/A'
	    and (PremiumMasterTypeBureauCode IN ('AL','AN','AP')
	    or v_Zero_MajorPerilCode IN ('930','931')),
	    'CA',
	    v_Reg_StandardInsuranceLineCode
	) AS v_StandardInsuranceLineCode,
	-- *INF*: IIF(v_StandardInsuranceLineCode='N/A' AND IN(PremiumMasterTypeBureauCode,'CF','B2','BB','BE','BF','BM','BT','FT','GL','GS','IM','MS','PF','PH','PI','PL','PQ','WC','WP','NB','RL','RN','RP','BC','N/A'),1,0)
	IFF(
	    v_StandardInsuranceLineCode = 'N/A'
	    and PremiumMasterTypeBureauCode IN ('CF','B2','BB','BE','BF','BM','BT','FT','GL','GS','IM','MS','PF','PH','PI','PL','PQ','WC','WP','NB','RL','RN','RP','BC','N/A'),
	    1,
	    0
	) AS v_flag,
	-- *INF*: IIF(IN(v_StandardInsuranceLineCode,'CR') OR v_flag=1,'N/A',:UDF.DEFAULT_VALUE_FOR_STRINGS(i_Risk_Unit_Group))
	IFF(
	    v_StandardInsuranceLineCode IN ('CR') OR v_flag = 1, 'N/A',
	    UDF_DEFAULT_VALUE_FOR_STRINGS(i_Risk_Unit_Group)
	) AS v_Risk_Unit_Group,
	-- *INF*: IIF(LTRIM(v_Risk_Unit_Group,'0')='','N/A',v_Risk_Unit_Group)
	IFF(LTRIM(v_Risk_Unit_Group, '0') = '', 'N/A', v_Risk_Unit_Group) AS v_Zero_Risk_Unit_Group,
	-- *INF*: IIF(REG_MATCH(v_Zero_Risk_Unit_Group,'[^0-9a-zA-Z]'),'N/A',v_Zero_Risk_Unit_Group)
	IFF(REGEXP_LIKE(v_Zero_Risk_Unit_Group, '[^0-9a-zA-Z]'), 'N/A', v_Zero_Risk_Unit_Group) AS o_Risk_Unit_Group,
	-- *INF*: IIF(SourceSystemID='PMS',v_StandardInsuranceLineCode,i_StandardInsuranceLineCode)
	IFF(SourceSystemID = 'PMS', v_StandardInsuranceLineCode, i_StandardInsuranceLineCode) AS o_StandardInsuranceLineCode,
	RiskType1 AS i_RiskType,
	-- *INF*: i_RiskType
	-- 
	-- --IIF(LTRIM(RTRIM(i_CoverageForm))='BusinessAuto','N/A',i_RiskType)
	i_RiskType AS o_RiskType,
	CoverageType1 AS CoverageType,
	o_MajorPerilCode AS i_MajorPerilCode,
	-- *INF*: IIF(REG_MATCH(v_Zero_MajorPerilCode,'[^0-9a-zA-Z]'),'N/A',v_Zero_MajorPerilCode)
	IFF(REGEXP_LIKE(v_Zero_MajorPerilCode, '[^0-9a-zA-Z]'), 'N/A', v_Zero_MajorPerilCode) AS o_MajorPerilCode,
	o_RiskUnit AS i_RiskUnit,
	o_RiskUnitSequenceNumber AS i_RiskUnitSequenceNumber,
	RiskLocationHashKey1 AS RiskLocationHashKey,
	-- *INF*: IIF(
	--   v_flag=1
	-- OR   (v_StandardInsuranceLineCode='GL' AND (NOT IN(v_MajorPerilCode,'540','599','919')
	--   OR NOT IN(PremiumMasterClassCode,'11111','22222','22250','92100','17000','17001','17002','80051','80052','80053','80054','80055','80056','80057','80058')))
	--   OR IN(v_StandardInsuranceLineCode,'WC','IM','CG','CA')=1,
	--  'N/A',:UDF.DEFAULT_VALUE_FOR_STRINGS(i_RiskUnit))
	IFF(
	    v_flag = 1
	    or (v_StandardInsuranceLineCode = 'GL'
	    and (NOT v_MajorPerilCode IN ('540','599','919')
	    or NOT PremiumMasterClassCode IN ('11111','22222','22250','92100','17000','17001','17002','80051','80052','80053','80054','80055','80056','80057','80058')))
	    or v_StandardInsuranceLineCode IN ('WC','IM','CG','CA') = 1,
	    'N/A',
	    UDF_DEFAULT_VALUE_FOR_STRINGS(i_RiskUnit)
	) AS v_RiskUnit,
	-- *INF*: IIF(LTRIM(v_RiskUnit,'0')='','N/A',v_RiskUnit)
	IFF(LTRIM(v_RiskUnit, '0') = '', 'N/A', v_RiskUnit) AS v_Zero_RiskUnit,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(SUBSTR(i_RiskUnitSequenceNumber,2,1))
	UDF_DEFAULT_VALUE_FOR_STRINGS(SUBSTR(i_RiskUnitSequenceNumber, 2, 1)) AS v_ProductTypeCode,
	-- *INF*: IIF(REG_MATCH(v_Zero_RiskUnit,'[^0-9a-zA-Z]'),'N/A',v_Zero_RiskUnit)
	IFF(REGEXP_LIKE(v_Zero_RiskUnit, '[^0-9a-zA-Z]'), 'N/A', v_Zero_RiskUnit) AS o_RiskUnit,
	-- *INF*: IIF(
	--   REG_MATCH(v_ProductTypeCode,'[^0-9a-zA-Z]') OR v_Reg_StandardInsuranceLineCode<>'GL' OR v_ProductTypeCode='0',
	--   'N/A',v_ProductTypeCode
	-- )
	IFF(
	    REGEXP_LIKE(v_ProductTypeCode, '[^0-9a-zA-Z]')
	    or v_Reg_StandardInsuranceLineCode <> 'GL'
	    or v_ProductTypeCode = '0',
	    'N/A',
	    v_ProductTypeCode
	) AS o_ProductTypeCode,
	-- *INF*: rtrim(i_PremiumMasterTransactionCode)
	rtrim(i_PremiumMasterTransactionCode) AS o_PremiumMasterTransactionCode,
	-- *INF*: rtrim(PremiumMasterPremiumType)
	rtrim(PremiumMasterPremiumType) AS o_PremiumMasterPremiumType,
	-- *INF*: TO_CHAR(i_PremiumMasterBureauInceptionDate,'YYYY')
	TO_CHAR(i_PremiumMasterBureauInceptionDate, 'YYYY') AS o_PremiumMasterBureauInceptionDate,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ReasonAmendedCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_ReasonAmendedCode) AS o_ReasonAmendedCode,
	-- *INF*: IIF(i_StandardInsuranceLineCode='WC','Payroll',NULL)
	IFF(i_StandardInsuranceLineCode = 'WC', 'Payroll', NULL) AS o_ExposureBasisName,
	PremiumTransactionAKID1,
	-- *INF*: IIF(PremiumTransactionAKID1 <> -1 , i_CoverageDetailDimId, 
	-- :LKP.LKP_COVERAGEDETAILDIM_HISTORIC_PMS(O_StatisticalCoverageAKID,PremiumMasterCoverageEffectiveDate)
	-- )
	-- 
	IFF(
	    PremiumTransactionAKID1 <> - 1, i_CoverageDetailDimId,
	    LKP_COVERAGEDETAILDIM_HISTORIC_PMS_O_StatisticalCoverageAKID_PremiumMasterCoverageEffectiveDate.CoverageDetailDimId
	) AS v_CoverageDetailDimId,
	-- *INF*: IIF(ISNULL(v_CoverageDetailDimId), -1, v_CoverageDetailDimId)
	IFF(v_CoverageDetailDimId IS NULL, - 1, v_CoverageDetailDimId) AS CoverageDetailDimId_lkp,
	pm_risk_state1 AS pm_risk_state,
	pm_risk_state AS StateProvinceCode,
	PerilGroup1 AS PerilGroup,
	PolicyAuditAKID1 AS PolicyAuditAKId,
	PolicyAuditEffectiveDate1 AS PolicyAuditEffectiveDate,
	SubCoverageTypeCode1 AS SubCoverageTypeCode,
	CoverageVersion1 AS CoverageVersion,
	O_PremiumTransactionEnteredDate,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ClassCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_ClassCode) AS o_ClassCode,
	CustomerCareCommissionRate1 AS CustomerCareCommissionRate,
	RatingPlanCode1 AS RatingPlanCode,
	GeneratedRecordIndicator1,
	DirectWrittenPremium1,
	RatablePremium1,
	ClassifiedPremium1,
	OtherModifiedPremium1,
	ScheduleModifiedPremium1,
	ExperienceModifiedPremium1,
	SubjectWrittenPremium1,
	EarnedPremiumRunDate1,
	O_StatisticalCoverageAKID,
	RunDate
	FROM FILTRANS
	LEFT JOIN LKP_COVERAGEDETAILDIM_HISTORIC_PMS LKP_COVERAGEDETAILDIM_HISTORIC_PMS_O_StatisticalCoverageAKID_PremiumMasterCoverageEffectiveDate
	ON LKP_COVERAGEDETAILDIM_HISTORIC_PMS_O_StatisticalCoverageAKID_PremiumMasterCoverageEffectiveDate.StatisticalCoverageAKID = O_StatisticalCoverageAKID
	AND LKP_COVERAGEDETAILDIM_HISTORIC_PMS_O_StatisticalCoverageAKID_PremiumMasterCoverageEffectiveDate.EffectiveDate = PremiumMasterCoverageEffectiveDate

),
LKP_InsuranceReferenceCoverageDim_DCT AS (
	SELECT
	InsuranceReferenceCoverageDimId,
	InsuranceLineCode,
	DctRiskTypeCode,
	DctCoverageTypeCode,
	DctPerilGroup,
	DctSubCoverageTypeCode,
	DctCoverageVersion
	FROM (
		SELECT 
			InsuranceReferenceCoverageDimId,
			InsuranceLineCode,
			DctRiskTypeCode,
			DctCoverageTypeCode,
			DctPerilGroup,
			DctSubCoverageTypeCode,
			DctCoverageVersion
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceCoverageDim
		WHERE NOT (DctRiskTypeCode='N/A' AND DctCoverageTypeCode='N/A' AND DctPerilGroup='N/A' AND DctSubCoverageTypeCode='N/A' AND DctCoverageVersion='N/A')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceLineCode,DctRiskTypeCode,DctCoverageTypeCode,DctPerilGroup,DctSubCoverageTypeCode,DctCoverageVersion ORDER BY InsuranceReferenceCoverageDimId) = 1
),
LKP_InsuranceReferenceCoverageDim_PMS AS (
	SELECT
	InsuranceReferenceCoverageDimId,
	IN_ProductTypeCode,
	InsuranceLineCode,
	PmsRiskUnitGroupCode,
	PmsRiskUnitCode,
	PmsMajorPerilCode,
	PmsProductTypeCode
	FROM (
		SELECT 
			InsuranceReferenceCoverageDimId,
			IN_ProductTypeCode,
			InsuranceLineCode,
			PmsRiskUnitGroupCode,
			PmsRiskUnitCode,
			PmsMajorPerilCode,
			PmsProductTypeCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceCoverageDim
		WHERE DctRiskTypeCode='N/A' AND DctCoverageTypeCode='N/A' AND DctPerilGroup='N/A' AND DctSubCoverageTypeCode='N/A' AND DctCoverageVersion='N/A'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceLineCode,PmsRiskUnitGroupCode,PmsRiskUnitCode,PmsMajorPerilCode,PmsProductTypeCode ORDER BY InsuranceReferenceCoverageDimId) = 1
),
LKP_InsuranceReferenceDimId AS (
	SELECT
	InsuranceReferenceDimId,
	EnterpriseGroupCode,
	InsuranceReferenceLegalEntityCode,
	StrategicProfitCenterCode,
	InsuranceSegmentCode,
	PolicyOfferingCode,
	ProductCode,
	InsuranceReferenceLineOfBusinessCode,
	RatingPlanCode
	FROM (
		SELECT 
			InsuranceReferenceDimId,
			EnterpriseGroupCode,
			InsuranceReferenceLegalEntityCode,
			StrategicProfitCenterCode,
			InsuranceSegmentCode,
			PolicyOfferingCode,
			ProductCode,
			InsuranceReferenceLineOfBusinessCode,
			RatingPlanCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EnterpriseGroupCode,InsuranceReferenceLegalEntityCode,StrategicProfitCenterCode,InsuranceSegmentCode,PolicyOfferingCode,ProductCode,InsuranceReferenceLineOfBusinessCode,RatingPlanCode ORDER BY InsuranceReferenceDimId) = 1
),
LKP_RiskLocationDim AS (
	SELECT
	RiskLocationDimID,
	RiskLocationHashKey
	FROM (
		SELECT 
			RiskLocationDimID,
			RiskLocationHashKey
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocationDim
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationHashKey ORDER BY RiskLocationDimID) = 1
),
lkp_PremiumTransactionTypeDim AS (
	SELECT
	PremiumTransactionTypeDimID,
	PremiumTransactionCode,
	ReasonAmendedCode,
	PremiumTypeCode
	FROM (
		SELECT 
		PTTD.PremiumTransactionTypeDimID as PremiumTransactionTypeDimID,
		LTRIM(RTRIM(PTTD.PremiumTransactionCode)) as PremiumTransactionCode, 
		LTRIM(RTRIM(PTTD.ReasonAmendedCode)) as ReasonAmendedCode, 
		LTRIM(RTRIM(PTTD.PremiumTypeCode)) as PremiumTypeCode 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransactionTypeDim PTTD
		where PTTD.CurrentSnapShotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionCode,ReasonAmendedCode,PremiumTypeCode ORDER BY PremiumTransactionTypeDimID DESC) = 1
),
mplt_PolicyDimID_PremiumMaster AS (WITH
	Input AS (
		
	),
	EXP_Default AS (
		SELECT
		IN_PolicyAKID AS PolicyAKID,
		IN_Trans_Date
		FROM Input
	),
	LKP_V2_Policy AS (
		SELECT
		contract_cust_ak_id,
		agencyakid,
		pol_status_code,
		strtgc_bus_dvsn_ak_id,
		IN_Trans_Date,
		pol_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT policy.contract_cust_ak_id as contract_cust_ak_id, policy.agencyakid as agencyakid, policy.pol_status_code as pol_status_code, policy.strtgc_bus_dvsn_ak_id as strtgc_bus_dvsn_ak_id, policy.pol_ak_id as pol_ak_id, policy.eff_from_date as eff_from_date, policy.eff_to_date as eff_to_date FROM 
			V2.policy
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id,eff_from_date,eff_to_date ORDER BY contract_cust_ak_id DESC) = 1
	),
	LKP_PolicyDimID AS (
		SELECT
		pol_dim_id,
		pol_key,
		pol_eff_date,
		pol_exp_date,
		pms_pol_lob_code,
		ClassOfBusinessCode,
		IN_Trans_Date,
		edw_pol_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				pol_dim_id,
				pol_key,
				pol_eff_date,
				pol_exp_date,
				pms_pol_lob_code,
				ClassOfBusinessCode,
				IN_Trans_Date,
				edw_pol_ak_id,
				eff_from_date,
				eff_to_date
			FROM policy_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_pol_ak_id,eff_from_date,eff_to_date ORDER BY pol_dim_id DESC) = 1
	),
	LKP_V3_AgencyDimID AS (
		SELECT
		agency_dim_id,
		edw_agency_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT AgencyDim.AgencyDimID as agency_dim_id, AgencyDim.EDWAgencyAKID as edw_agency_ak_id, AgencyDim.EffectiveDate as eff_from_date, AgencyDim.ExpirationDate as eff_to_date
			 FROM V3.AgencyDim as AgencyDim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_agency_ak_id,eff_from_date,eff_to_date ORDER BY agency_dim_id DESC) = 1
	),
	LKP_ContractCustomerDim AS (
		SELECT
		contract_cust_dim_id,
		IN_Trans_Date,
		edw_contract_cust_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				contract_cust_dim_id,
				IN_Trans_Date,
				edw_contract_cust_ak_id,
				eff_from_date,
				eff_to_date
			FROM contract_customer_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_contract_cust_ak_id,eff_from_date,eff_to_date ORDER BY contract_cust_dim_id DESC) = 1
	),
	lkp_StrategicBusinessDivisionDIM AS (
		SELECT
		strtgc_bus_dvsn_dim_id,
		edw_strtgc_bus_dvsn_ak_id
		FROM (
			SELECT strategic_business_division_dim.strtgc_bus_dvsn_dim_id as strtgc_bus_dvsn_dim_id, strategic_business_division_dim.edw_strtgc_bus_dvsn_ak_id as edw_strtgc_bus_dvsn_ak_id 
			FROM strategic_business_division_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_strtgc_bus_dvsn_ak_id ORDER BY strtgc_bus_dvsn_dim_id DESC) = 1
	),
	EXP_Values AS (
		SELECT
		LKP_V3_AgencyDimID.agency_dim_id,
		LKP_ContractCustomerDim.contract_cust_dim_id,
		LKP_PolicyDimID.pol_dim_id,
		LKP_V2_Policy.pol_status_code,
		LKP_PolicyDimID.pol_eff_date,
		LKP_PolicyDimID.pol_exp_date,
		lkp_StrategicBusinessDivisionDIM.strtgc_bus_dvsn_dim_id,
		LKP_PolicyDimID.pol_key,
		LKP_PolicyDimID.pms_pol_lob_code,
		LKP_PolicyDimID.ClassOfBusinessCode
		FROM 
		LEFT JOIN LKP_ContractCustomerDim
		ON LKP_ContractCustomerDim.edw_contract_cust_ak_id = LKP_V2_Policy.contract_cust_ak_id AND LKP_ContractCustomerDim.eff_from_date <= EXP_Default.IN_Trans_Date AND LKP_ContractCustomerDim.eff_to_date >= EXP_Default.IN_Trans_Date
		LEFT JOIN LKP_PolicyDimID
		ON LKP_PolicyDimID.edw_pol_ak_id = EXP_Default.PolicyAKID AND LKP_PolicyDimID.eff_from_date <= EXP_Default.IN_Trans_Date AND LKP_PolicyDimID.eff_to_date >= EXP_Default.IN_Trans_Date
		LEFT JOIN LKP_V2_Policy
		ON LKP_V2_Policy.pol_ak_id = EXP_Default.PolicyAKID AND LKP_V2_Policy.eff_from_date <= EXP_Default.IN_Trans_Date AND LKP_V2_Policy.eff_to_date >= EXP_Default.IN_Trans_Date
		LEFT JOIN LKP_V3_AgencyDimID
		ON LKP_V3_AgencyDimID.edw_agency_ak_id = LKP_V2_Policy.agencyakid AND LKP_V3_AgencyDimID.eff_from_date <= EXP_Default.IN_Trans_Date AND LKP_V3_AgencyDimID.eff_to_date >= EXP_Default.IN_Trans_Date
		LEFT JOIN lkp_StrategicBusinessDivisionDIM
		ON lkp_StrategicBusinessDivisionDIM.edw_strtgc_bus_dvsn_ak_id = LKP_V2_Policy.strtgc_bus_dvsn_ak_id
	),
	Output AS (
		SELECT
		agency_dim_id, 
		contract_cust_dim_id, 
		pol_dim_id, 
		pol_status_code, 
		pol_eff_date, 
		pol_exp_date, 
		strtgc_bus_dvsn_dim_id, 
		pol_key, 
		pms_pol_lob_code, 
		ClassOfBusinessCode
		FROM EXP_Values
	),
),
EXP_Consolidate_Data_from_Lookups AS (
	SELECT
	EXP_PremiumMasterCalculation_IN.o_ClassCode AS ClassCode,
	EXP_PremiumMasterCalculation_IN.o_StandardInsuranceLineCode AS StandardInsuranceLineCode,
	mplt_PolicyDimID_PremiumMaster.agency_dim_id AS i_AgencyDimID,
	mplt_PolicyDimID_PremiumMaster.pol_dim_id AS i_PolicyDimID,
	mplt_PolicyDimID_PremiumMaster.contract_cust_dim_id AS i_ContractCustomerDimID,
	LKP_RiskLocationDim.RiskLocationDimID AS i_RiskLocationDimID,
	lkp_PremiumTransactionTypeDim.PremiumTransactionTypeDimID AS i_PremiumTransactionTypeDimID,
	EXP_PremiumMasterCalculation_IN.asldimID AS i_ASLdimID,
	EXP_PremiumMasterCalculation_IN.aslproductcodedimID AS i_ASLproductcodedimID,
	EXP_PremiumMasterCalculation_IN.PremiumMasterBookedDate AS i_PremiumMasterBookedDate,
	EXP_PremiumMasterCalculation_IN.PremiumMasterCoverageEffectiveDate AS i_PremiumMasterCoverageEffectiveDate,
	EXP_PremiumMasterCalculation_IN.PremiumMasterCoverageExpirationDate AS i_PremiumMasterCoverageExpirationDate,
	mplt_PolicyDimID_PremiumMaster.pol_eff_date AS i_PolicyEffectiveDate,
	mplt_PolicyDimID_PremiumMaster.pol_exp_date AS i_PolicyExpirationDate,
	EXP_PremiumMasterCalculation_IN.AuditID,
	EXP_PremiumMasterCalculation_IN.PremiumTransactionID AS EDWPremiumTransactionPKID,
	EXP_PremiumMasterCalculation_IN.PremiumMasterCalculationID AS EDWPremiumMasterCalculationPKID,
	EXP_PremiumMasterCalculation_IN.PremiumMasterPremium,
	EXP_PremiumMasterCalculation_IN.PremiumMasterFullTermPremium,
	EXP_PremiumMasterCalculation_IN.o_PremiumMasterPremiumType AS PremiumMasterPremiumType,
	EXP_PremiumMasterCalculation_IN.o_PremiumMasterTransactionCode AS PremiumMasterTransactionCode,
	EXP_PremiumMasterCalculation_IN.o_ReasonAmendedCode AS ReasonAmendedCode,
	LKP_InsuranceReferenceDimId.InsuranceReferenceDimId AS i_InsuranceReferenceDimId,
	EXP_PremiumMasterCalculation_IN.SourceSystemID AS i_SourceSystemID,
	LKP_InsuranceReferenceCoverageDim_PMS.InsuranceReferenceCoverageDimId AS i_InsuranceReferenceCoverageDimId_PMS,
	LKP_InsuranceReferenceCoverageDim_DCT.InsuranceReferenceCoverageDimId AS i_InsuranceReferenceCoverageDimId_DCT,
	EXP_PremiumMasterCalculation_IN.CoverageDetailDimId_lkp AS i_CoverageDetailDimId,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(i_PremiumMasterBookedDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_i_PremiumMasterBookedDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PremiumMasterBookedDateID,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(i_PremiumMasterCoverageEffectiveDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_i_PremiumMasterCoverageEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PremiumMasterCoverageEffectiveDateID,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(i_PremiumMasterCoverageExpirationDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_i_PremiumMasterCoverageExpirationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PremiumMasterCoverageExpirationDateID,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(i_PolicyEffectiveDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_i_PolicyEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PremiumMasterPolicyEffectiveDateID,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(i_PolicyExpirationDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_i_PolicyExpirationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PremiumMasterPolicyExpirationDateID,
	-- *INF*: IIF(NOT ISNULL(i_AgencyDimID),i_AgencyDimID,-1)
	IFF(i_AgencyDimID IS NOT NULL, i_AgencyDimID, - 1) AS o_AgencyDimID,
	-- *INF*: IIF(NOT ISNULL(i_PolicyDimID),i_PolicyDimID,-1)
	IFF(i_PolicyDimID IS NOT NULL, i_PolicyDimID, - 1) AS o_PolicyDimID,
	-- *INF*: IIF(NOT ISNULL(i_ContractCustomerDimID),i_ContractCustomerDimID,-1)
	IFF(i_ContractCustomerDimID IS NOT NULL, i_ContractCustomerDimID, - 1) AS o_ContractCustomerDimID,
	-- *INF*: IIF(NOT ISNULL(i_RiskLocationDimID),i_RiskLocationDimID,-1)
	IFF(i_RiskLocationDimID IS NOT NULL, i_RiskLocationDimID, - 1) AS o_RiskLocationDimID,
	-- *INF*: IIF(NOT ISNULL(i_PremiumMasterDimID),i_PremiumMasterDimID,-1)
	IFF(i_PremiumMasterDimID IS NOT NULL, i_PremiumMasterDimID, - 1) AS o_PremiumMasterDimID,
	-- *INF*: IIF(NOT ISNULL(i_PremiumTransactionTypeDimID),i_PremiumTransactionTypeDimID,-1)
	IFF(i_PremiumTransactionTypeDimID IS NOT NULL, i_PremiumTransactionTypeDimID, - 1) AS o_PremiumTransactionTypeDimID,
	-- *INF*: IIF(NOT ISNULL(v_PremiumMasterBookedDateID),v_PremiumMasterBookedDateID,-1)
	IFF(v_PremiumMasterBookedDateID IS NOT NULL, v_PremiumMasterBookedDateID, - 1) AS o_PremiumMasterBookedDateID,
	-- *INF*: IIF(NOT ISNULL(v_PremiumMasterCoverageEffectiveDateID),v_PremiumMasterCoverageEffectiveDateID,-1)
	IFF(
	    v_PremiumMasterCoverageEffectiveDateID IS NOT NULL, v_PremiumMasterCoverageEffectiveDateID,
	    - 1
	) AS o_PremiumMasterCoverageEffectiveDateID,
	-- *INF*: IIF(NOT ISNULL(v_PremiumMasterCoverageExpirationDateID),v_PremiumMasterCoverageExpirationDateID,-1)
	IFF(
	    v_PremiumMasterCoverageExpirationDateID IS NOT NULL, v_PremiumMasterCoverageExpirationDateID,
	    - 1
	) AS o_PremiumMasterCoverageExpirationDateID,
	-- *INF*: IIF(NOT ISNULL(i_ASLdimID),i_ASLdimID,-1)
	IFF(i_ASLdimID IS NOT NULL, i_ASLdimID, - 1) AS o_ASLdimID,
	-- *INF*: IIF(NOT ISNULL(i_ASLproductcodedimID),i_ASLproductcodedimID,-1)
	IFF(i_ASLproductcodedimID IS NOT NULL, i_ASLproductcodedimID, - 1) AS o_ASLproductcodedimID,
	-- *INF*: IIF(ISNULL(v_PremiumMasterPolicyEffectiveDateID),:LKP.LKP_CALENDER_DIM(TO_DATE('21001231','YYYYMMDD')),v_PremiumMasterPolicyEffectiveDateID)
	IFF(
	    v_PremiumMasterPolicyEffectiveDateID IS NULL,
	    LKP_CALENDER_DIM_TO_DATE_21001231_YYYYMMDD.clndr_id,
	    v_PremiumMasterPolicyEffectiveDateID
	) AS o_PremiumMasterPolicyEffectiveDateID,
	-- *INF*: IIF(ISNULL(v_PremiumMasterPolicyExpirationDateID),:LKP.LKP_CALENDER_DIM(TO_DATE('21001231','YYYYMMDD')),v_PremiumMasterPolicyExpirationDateID)
	IFF(
	    v_PremiumMasterPolicyExpirationDateID IS NULL,
	    LKP_CALENDER_DIM_TO_DATE_21001231_YYYYMMDD.clndr_id,
	    v_PremiumMasterPolicyExpirationDateID
	) AS o_PremiumMasterPolicyExpirationDateID,
	-- *INF*: IIF(ISNULL(i_InsuranceReferenceDimId), -1, i_InsuranceReferenceDimId)
	IFF(i_InsuranceReferenceDimId IS NULL, - 1, i_InsuranceReferenceDimId) AS o_InsuranceReferenceDimId,
	-- *INF*: IIF(ISNULL(i_SalesDivisionDimId), -1, i_SalesDivisionDimId)
	IFF(i_SalesDivisionDimId IS NULL, - 1, i_SalesDivisionDimId) AS o_SalesDivisionDimId,
	-- *INF*: DECODE(i_SourceSystemID,'PMS',i_InsuranceReferenceCoverageDimId_PMS,'DCT',i_InsuranceReferenceCoverageDimId_DCT)
	DECODE(
	    i_SourceSystemID,
	    'PMS', i_InsuranceReferenceCoverageDimId_PMS,
	    'DCT', i_InsuranceReferenceCoverageDimId_DCT
	) AS o_InsuranceReferenceCoverageDimID,
	i_CoverageDetailDimId AS o_CoverageDetailDimId,
	EXP_PremiumMasterCalculation_IN.o_MajorPerilCode,
	EXP_PremiumMasterCalculation_IN.o_PremiumMasterPremiumType,
	EXP_PremiumMasterCalculation_IN.StateProvinceCode,
	-- *INF*: IIF(ISNULL(i_PolicyAuditDimId),-1,i_PolicyAuditDimId)
	IFF(i_PolicyAuditDimId IS NULL, - 1, i_PolicyAuditDimId) AS o_PolicyAuditDimId,
	EXP_PremiumMasterCalculation_IN.O_PremiumTransactionEnteredDate AS i_PremiumTransactionEnteredDate,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(i_PremiumTransactionEnteredDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_i_PremiumTransactionEnteredDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PremiumTransactionEnteredDateID,
	-- *INF*: IIF(NOT ISNULL(v_PremiumTransactionEnteredDateID),v_PremiumTransactionEnteredDateID,-1)
	IFF(v_PremiumTransactionEnteredDateID IS NOT NULL, v_PremiumTransactionEnteredDateID, - 1) AS o_PremiumTransactionEnteredDateID,
	EXP_PremiumMasterCalculation_IN.GeneratedRecordIndicator1,
	EXP_PremiumMasterCalculation_IN.DirectWrittenPremium1,
	EXP_PremiumMasterCalculation_IN.RatablePremium1,
	EXP_PremiumMasterCalculation_IN.ClassifiedPremium1,
	EXP_PremiumMasterCalculation_IN.OtherModifiedPremium1,
	EXP_PremiumMasterCalculation_IN.ScheduleModifiedPremium1,
	EXP_PremiumMasterCalculation_IN.ExperienceModifiedPremium1,
	EXP_PremiumMasterCalculation_IN.SubjectWrittenPremium1,
	EXP_PremiumMasterCalculation_IN.EarnedPremiumRunDate1 AS i_EarnedPremiumRunDate1,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(i_EarnedPremiumRunDate1, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_i_EarnedPremiumRunDate1_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_EarnedPremiumRunDate1,
	-- *INF*: IIF(NOT ISNULL(v_EarnedPremiumRunDate1),v_EarnedPremiumRunDate1,-1)
	IFF(v_EarnedPremiumRunDate1 IS NOT NULL, v_EarnedPremiumRunDate1, - 1) AS o_EarnedPremiumRunDate1,
	-1 AS CoverageCancellationDateID,
	EXP_PremiumMasterCalculation_IN.RunDate,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(RunDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_RunDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_RunDateId,
	-- *INF*: IIF(NOT ISNULL(v_RunDateId),v_RunDateId,-1)
	IFF(v_RunDateId IS NOT NULL, v_RunDateId, - 1) AS O_RunDateId
	FROM EXP_PremiumMasterCalculation_IN
	 -- Manually join with mplt_PolicyDimID_PremiumMaster
	LEFT JOIN LKP_InsuranceReferenceCoverageDim_DCT
	ON LKP_InsuranceReferenceCoverageDim_DCT.InsuranceLineCode = EXP_PremiumMasterCalculation_IN.o_StandardInsuranceLineCode AND LKP_InsuranceReferenceCoverageDim_DCT.DctRiskTypeCode = EXP_PremiumMasterCalculation_IN.o_RiskType AND LKP_InsuranceReferenceCoverageDim_DCT.DctCoverageTypeCode = EXP_PremiumMasterCalculation_IN.CoverageType AND LKP_InsuranceReferenceCoverageDim_DCT.DctPerilGroup = EXP_PremiumMasterCalculation_IN.PerilGroup AND LKP_InsuranceReferenceCoverageDim_DCT.DctSubCoverageTypeCode = EXP_PremiumMasterCalculation_IN.SubCoverageTypeCode AND LKP_InsuranceReferenceCoverageDim_DCT.DctCoverageVersion = EXP_PremiumMasterCalculation_IN.CoverageVersion
	LEFT JOIN LKP_InsuranceReferenceCoverageDim_PMS
	ON LKP_InsuranceReferenceCoverageDim_PMS.InsuranceLineCode = EXP_PremiumMasterCalculation_IN.o_StandardInsuranceLineCode AND LKP_InsuranceReferenceCoverageDim_PMS.PmsRiskUnitGroupCode = EXP_PremiumMasterCalculation_IN.o_Risk_Unit_Group AND LKP_InsuranceReferenceCoverageDim_PMS.PmsRiskUnitCode = EXP_PremiumMasterCalculation_IN.o_RiskUnit AND LKP_InsuranceReferenceCoverageDim_PMS.PmsMajorPerilCode = EXP_PremiumMasterCalculation_IN.o_MajorPerilCode AND LKP_InsuranceReferenceCoverageDim_PMS.PmsProductTypeCode = EXP_PremiumMasterCalculation_IN.o_ProductTypeCode
	LEFT JOIN LKP_InsuranceReferenceDimId
	ON LKP_InsuranceReferenceDimId.EnterpriseGroupCode = EXP_PremiumMasterCalculation_IN.EnterpriseGroupCode AND LKP_InsuranceReferenceDimId.InsuranceReferenceLegalEntityCode = EXP_PremiumMasterCalculation_IN.InsuranceReferenceLegalEntityCode AND LKP_InsuranceReferenceDimId.StrategicProfitCenterCode = EXP_PremiumMasterCalculation_IN.StrategicProfitCenterCode AND LKP_InsuranceReferenceDimId.InsuranceSegmentCode = EXP_PremiumMasterCalculation_IN.InsuranceSegmentCode AND LKP_InsuranceReferenceDimId.PolicyOfferingCode = EXP_PremiumMasterCalculation_IN.PolicyOfferingCode AND LKP_InsuranceReferenceDimId.ProductCode = EXP_PremiumMasterCalculation_IN.ProductCode AND LKP_InsuranceReferenceDimId.InsuranceReferenceLineOfBusinessCode = EXP_PremiumMasterCalculation_IN.InsuranceReferenceLineOfBusinessCode AND LKP_InsuranceReferenceDimId.RatingPlanCode = EXP_PremiumMasterCalculation_IN.RatingPlanCode
	LEFT JOIN LKP_RiskLocationDim
	ON LKP_RiskLocationDim.RiskLocationHashKey = EXP_PremiumMasterCalculation_IN.RiskLocationHashKey
	LEFT JOIN lkp_PremiumTransactionTypeDim
	ON lkp_PremiumTransactionTypeDim.PremiumTransactionCode = EXP_PremiumMasterCalculation_IN.o_PremiumMasterTransactionCode AND lkp_PremiumTransactionTypeDim.ReasonAmendedCode = EXP_PremiumMasterCalculation_IN.o_ReasonAmendedCode AND lkp_PremiumTransactionTypeDim.PremiumTypeCode = EXP_PremiumMasterCalculation_IN.o_PremiumMasterPremiumType
	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_i_PremiumMasterBookedDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_i_PremiumMasterBookedDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(i_PremiumMasterBookedDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_i_PremiumMasterCoverageEffectiveDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_i_PremiumMasterCoverageEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(i_PremiumMasterCoverageEffectiveDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_i_PremiumMasterCoverageExpirationDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_i_PremiumMasterCoverageExpirationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(i_PremiumMasterCoverageExpirationDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_i_PolicyEffectiveDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_i_PolicyEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(i_PolicyEffectiveDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_i_PolicyExpirationDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_i_PolicyExpirationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(i_PolicyExpirationDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_TO_DATE_21001231_YYYYMMDD
	ON LKP_CALENDER_DIM_TO_DATE_21001231_YYYYMMDD.clndr_date = TO_TIMESTAMP('21001231', 'YYYYMMDD')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_i_PremiumTransactionEnteredDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_i_PremiumTransactionEnteredDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(i_PremiumTransactionEnteredDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_i_EarnedPremiumRunDate1_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_i_EarnedPremiumRunDate1_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(i_EarnedPremiumRunDate1, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_RunDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_RunDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(RunDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

),
EXP_Evaluate_Fields AS (
	SELECT
	ClassCode AS i_ClassCode,
	StandardInsuranceLineCode AS i_StandardInsuranceLineCode,
	PremiumMasterPremiumType AS i_PremiumMasterPremiumType,
	AuditID,
	o_ASLdimID AS AnnualStatementLineDimID,
	o_ASLproductcodedimID AS AnnualStatementLineProductCodeDimID,
	o_AgencyDimID AS AgencyDimID,
	o_PolicyDimID AS PolicyDimID,
	o_ContractCustomerDimID AS ContractCustomerDimID,
	o_RiskLocationDimID AS RiskLocationDimID,
	o_PremiumMasterDimID AS PremiumMasterDimID,
	EDWPremiumMasterCalculationPKID,
	EDWPremiumTransactionPKID,
	o_PremiumTransactionTypeDimID AS PremiumTransactionTypeDimID,
	o_PremiumMasterBookedDateID AS PremiumMasterBookedDateID,
	o_PremiumMasterCoverageEffectiveDateID AS PremiumMasterCoverageEffectiveDateID,
	o_PremiumMasterCoverageExpirationDateID AS PremiumMasterCoverageExpirationDateID,
	PremiumMasterPremium,
	o_PremiumMasterPolicyEffectiveDateID AS PremiumMasterPolicyEffectiveDateID,
	o_PremiumMasterPolicyExpirationDateID AS PremiumMasterPolicyExpirationDateID,
	PremiumMasterTransactionCode,
	ReasonAmendedCode,
	PremiumMasterExposure1,
	o_InsuranceReferenceDimId AS InsuranceReferenceDimId,
	o_SalesDivisionDimId AS SalesDivisionDimId,
	i_PremiumMasterBookedDate,
	i_PolicyEffectiveDate,
	-- *INF*: to_date('07-01-2013 00:00:00','MM-DD-YYYY HH24:MI:SS')
	TO_TIMESTAMP('07-01-2013 00:00:00', 'MM-DD-YYYY HH24:MI:SS') AS v_MineSubPLDate,
	-- *INF*: to_date('07-01-2014 00:00:00','MM-DD-YYYY HH24:MI:SS')
	-- 
	TO_TIMESTAMP('07-01-2014 00:00:00', 'MM-DD-YYYY HH24:MI:SS') AS v_CLMineSubDate,
	o_InsuranceReferenceCoverageDimID,
	o_CoverageDetailDimId,
	-1 AS o_DEFAULT_ID,
	o_MajorPerilCode,
	o_PremiumMasterPremiumType,
	StateProvinceCode,
	-- *INF*: IIF(ReasonAmendedCode='CWB',0,PremiumMasterPremium)
	IFF(ReasonAmendedCode = 'CWB', 0, PremiumMasterPremium) AS o_PremiumMasterPremium,
	o_PolicyAuditDimId,
	o_PremiumTransactionEnteredDateID,
	GeneratedRecordIndicator1,
	DirectWrittenPremium1,
	RatablePremium1 AS i_RatablePremium1,
	-- *INF*: IIF(ISNULL(i_RatablePremium1),0 ,i_RatablePremium1) 
	IFF(i_RatablePremium1 IS NULL, 0, i_RatablePremium1) AS o_RatablePremium1,
	ClassifiedPremium1 AS i_ClassifiedPremium1,
	-- *INF*: IIF(ISNULL(i_ClassifiedPremium1),0 ,i_ClassifiedPremium1) 
	IFF(i_ClassifiedPremium1 IS NULL, 0, i_ClassifiedPremium1) AS o_ClassifiedPremium,
	OtherModifiedPremium1 AS i_OtherModifiedPremium1,
	-- *INF*: IIF(ISNULL(i_OtherModifiedPremium1),0 ,i_OtherModifiedPremium1) 
	IFF(i_OtherModifiedPremium1 IS NULL, 0, i_OtherModifiedPremium1) AS o_OtherModifiedPremium,
	ScheduleModifiedPremium1 AS i_ScheduleModifiedPremium1,
	-- *INF*: IIF(ISNULL(i_ScheduleModifiedPremium1),0 ,i_ScheduleModifiedPremium1) 
	IFF(i_ScheduleModifiedPremium1 IS NULL, 0, i_ScheduleModifiedPremium1) AS o_ScheduleModifiedPremium,
	ExperienceModifiedPremium1 AS i_ExperienceModifiedPremium1,
	-- *INF*: IIF(ISNULL(i_ExperienceModifiedPremium1),0 ,i_ExperienceModifiedPremium1) 
	IFF(i_ExperienceModifiedPremium1 IS NULL, 0, i_ExperienceModifiedPremium1) AS o_ExperienceModifiedPremium,
	SubjectWrittenPremium1 AS i_SubjectWrittenPremium1,
	-- *INF*: IIF(ISNULL(i_SubjectWrittenPremium1),0 ,i_SubjectWrittenPremium1) 
	IFF(i_SubjectWrittenPremium1 IS NULL, 0, i_SubjectWrittenPremium1) AS o_SubjectWrittenPremium,
	o_EarnedPremiumRunDate1 AS EarnedPremiumRunDate1,
	CoverageCancellationDateID,
	1 AS DirectOrEarnedPremiumFlag,
	0 AS EARNEDPREMIUM,
	O_RunDateId AS RunDateId
	FROM EXP_Consolidate_Data_from_Lookups
),
LKP_ModifiedPremiumTransactionMonthlyFact AS (
	SELECT
	ModifiedPremiumTransactionMonthlyFactId,
	EDWPremiumMasterCalculationPKId,
	RunDateId,
	PremiumTransactionEnteredDateID,
	AnnualStatementLineDimID,
	AnnualStatementLineProductCodeDimID
	FROM (
		select f.ModifiedPremiumTransactionMonthlyFactId as ModifiedPremiumTransactionMonthlyFactId, 
			f.AnnualStatementLineDimID as AnnualStatementLineDimID, 
			f.EDWPremiumMasterCalculationPKId as EDWPremiumMasterCalculationPKId, 
			f.RunDateId as RunDateId, 
			f.PremiumTransactionEnteredDateID as PremiumTransactionEnteredDateID, 
			f.AnnualStatementLineProductCodeDimID as AnnualStatementLineProductCodeDimID
		from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.@{pipeline().parameters.TARGET_TABLE_NAME} f with (nolock) 
		@{pipeline().parameters.WHERE_CLAUSE_TARGET_LOOKUP}
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWPremiumMasterCalculationPKId,RunDateId,PremiumTransactionEnteredDateID,AnnualStatementLineDimID,AnnualStatementLineProductCodeDimID ORDER BY ModifiedPremiumTransactionMonthlyFactId) = 1
),
RTR_INSERT_UPDATE AS (
	SELECT
	LKP_ModifiedPremiumTransactionMonthlyFact.ModifiedPremiumTransactionMonthlyFactId,
	EXP_Evaluate_Fields.AuditID,
	EXP_Evaluate_Fields.AnnualStatementLineDimID,
	EXP_Evaluate_Fields.AnnualStatementLineProductCodeDimID,
	EXP_Evaluate_Fields.AgencyDimID,
	EXP_Evaluate_Fields.PolicyDimID,
	EXP_Evaluate_Fields.ContractCustomerDimID,
	EXP_Evaluate_Fields.RiskLocationDimID,
	EXP_Evaluate_Fields.PremiumMasterDimID,
	EXP_Evaluate_Fields.EDWPremiumMasterCalculationPKID,
	EXP_Evaluate_Fields.EDWPremiumTransactionPKID,
	EXP_Evaluate_Fields.PremiumTransactionTypeDimID,
	EXP_Evaluate_Fields.o_DEFAULT_ID AS DEFAULT_ID,
	EXP_Evaluate_Fields.PremiumMasterBookedDateID,
	EXP_Evaluate_Fields.PremiumMasterCoverageEffectiveDateID,
	EXP_Evaluate_Fields.PremiumMasterCoverageExpirationDateID,
	EXP_Evaluate_Fields.o_PremiumMasterPremium AS PremiumMasterPremium,
	EXP_Evaluate_Fields.PremiumMasterPolicyEffectiveDateID,
	EXP_Evaluate_Fields.PremiumMasterPolicyExpirationDateID,
	EXP_Evaluate_Fields.InsuranceReferenceDimId,
	EXP_Evaluate_Fields.SalesDivisionDimId,
	EXP_Evaluate_Fields.o_InsuranceReferenceCoverageDimID AS InsuranceReferenceCoverageDimID,
	EXP_Evaluate_Fields.o_CoverageDetailDimId AS CoverageDetailDimId,
	EXP_Evaluate_Fields.PremiumMasterExposure1 AS PremiumMasterExposure,
	EXP_Evaluate_Fields.o_PolicyAuditDimId AS PolicyAuditDimId,
	EXP_Evaluate_Fields.o_PremiumTransactionEnteredDateID,
	EXP_Evaluate_Fields.GeneratedRecordIndicator1,
	EXP_Evaluate_Fields.DirectWrittenPremium1,
	EXP_Evaluate_Fields.o_RatablePremium1 AS RatablePremium1,
	EXP_Evaluate_Fields.o_ClassifiedPremium AS ClassifiedPremium1,
	EXP_Evaluate_Fields.o_OtherModifiedPremium AS OtherModifiedPremium1,
	EXP_Evaluate_Fields.o_ScheduleModifiedPremium AS ScheduleModifiedPremium1,
	EXP_Evaluate_Fields.o_ExperienceModifiedPremium AS ExperienceModifiedPremium1,
	EXP_Evaluate_Fields.o_SubjectWrittenPremium AS SubjectWrittenPremium1,
	EXP_Evaluate_Fields.EARNEDPREMIUM AS EarnedDirectWrittenPremium1,
	EXP_Evaluate_Fields.EARNEDPREMIUM AS EarnedClassifiedPremium1,
	EXP_Evaluate_Fields.EARNEDPREMIUM AS EarnedRatablePremium1,
	EXP_Evaluate_Fields.EARNEDPREMIUM AS EarnedOtherModifiedPremium1,
	EXP_Evaluate_Fields.EARNEDPREMIUM AS EarnedScheduleModifiedPremium1,
	EXP_Evaluate_Fields.EARNEDPREMIUM AS EarnedExperienceModifiedPremium1,
	EXP_Evaluate_Fields.EARNEDPREMIUM AS EarnedSubjectWrittenPremium1,
	EXP_Evaluate_Fields.EarnedPremiumRunDate1,
	EXP_Evaluate_Fields.CoverageCancellationDateID,
	EXP_Evaluate_Fields.DirectOrEarnedPremiumFlag,
	EXP_Evaluate_Fields.RunDateId
	FROM EXP_Evaluate_Fields
	LEFT JOIN LKP_ModifiedPremiumTransactionMonthlyFact
	ON LKP_ModifiedPremiumTransactionMonthlyFact.EDWPremiumMasterCalculationPKId = EXP_Evaluate_Fields.EDWPremiumMasterCalculationPKID AND LKP_ModifiedPremiumTransactionMonthlyFact.RunDateId = EXP_Evaluate_Fields.PremiumMasterBookedDateID AND LKP_ModifiedPremiumTransactionMonthlyFact.PremiumTransactionEnteredDateID = EXP_Evaluate_Fields.o_PremiumTransactionEnteredDateID AND LKP_ModifiedPremiumTransactionMonthlyFact.AnnualStatementLineDimID = EXP_Evaluate_Fields.AnnualStatementLineDimID AND LKP_ModifiedPremiumTransactionMonthlyFact.AnnualStatementLineProductCodeDimID = EXP_Evaluate_Fields.AnnualStatementLineProductCodeDimID
),
RTR_INSERT_UPDATE_INSERT AS (SELECT * FROM RTR_INSERT_UPDATE WHERE ISNULL(ModifiedPremiumTransactionMonthlyFactId)),
RTR_INSERT_UPDATE_DEFAULT1 AS (SELECT * FROM RTR_INSERT_UPDATE WHERE NOT ( (ISNULL(ModifiedPremiumTransactionMonthlyFactId)) )),
UPD_Update AS (
	SELECT
	ModifiedPremiumTransactionMonthlyFactId AS PremiumMasterFactID2, 
	AuditID AS AuditID2, 
	AnnualStatementLineDimID AS AnnualStatementLineDimID2, 
	AnnualStatementLineProductCodeDimID AS AnnualStatementLineProductCodeDimID2, 
	AgencyDimID AS AgencyDimID2, 
	PolicyDimID AS PolicyDimID2, 
	ContractCustomerDimID AS ContractCustomerDimID2, 
	RiskLocationDimID AS RiskLocationDimID2, 
	PremiumMasterDimID AS PremiumMasterDimID2, 
	PremiumTransactionTypeDimID AS PremiumTransactionTypeDimID2, 
	EDWPremiumMasterCalculationPKID AS EDWPremiumMasterCalculationPKID2, 
	EDWPremiumTransactionPKID AS EDWPremiumTransactionPKID2, 
	DEFAULT_ID AS DEFAULT_ID2, 
	PremiumMasterPolicyEffectiveDateID AS PremiumMasterPolicyEffectiveDateID2, 
	PremiumMasterPolicyExpirationDateID AS PremiumMasterPolicyExpirationDateID2, 
	PremiumMasterCoverageEffectiveDateID AS PremiumMasterCoverageEffectiveDateID2, 
	PremiumMasterCoverageExpirationDateID AS PremiumMasterCoverageExpirationDateID2, 
	PremiumMasterBookedDateID AS PremiumMasterRunDateID2, 
	PremiumMasterPremium AS PremiumMasterPremium2, 
	InsuranceReferenceDimId, 
	SalesDivisionDimId, 
	InsuranceReferenceCoverageDimID AS InsuranceReferenceCoverageDimID2, 
	CoverageDetailDimId AS CoverageDetailDimId2, 
	PremiumMasterExposure AS PremiumMasterExposure2, 
	PolicyAuditDimId AS PolicyAuditDimId2, 
	GeneratedRecordIndicator1 AS GeneratedRecordIndicator12, 
	DirectWrittenPremium1, 
	RatablePremium1, 
	ClassifiedPremium1, 
	OtherModifiedPremium1, 
	ScheduleModifiedPremium1, 
	ExperienceModifiedPremium1, 
	SubjectWrittenPremium1, 
	EarnedDirectWrittenPremium1, 
	EarnedClassifiedPremium1, 
	EarnedRatablePremium1, 
	EarnedOtherModifiedPremium1, 
	EarnedScheduleModifiedPremium1, 
	EarnedExperienceModifiedPremium1, 
	EarnedSubjectWrittenPremium1, 
	o_PremiumTransactionEnteredDateID AS o_PremiumTransactionEnteredDateID2, 
	EarnedPremiumRunDate1 AS EarnedPremiumRunDate12, 
	CoverageCancellationDateID AS CoverageCancellationDateID2, 
	DirectOrEarnedPremiumFlag AS DirectOrEarnedPremiumFlag2, 
	RunDateId
	FROM RTR_INSERT_UPDATE_DEFAULT1
),
ModifiedPremiumTransactionMonthlyFact_UPD AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.@{pipeline().parameters.TARGET_TABLE_NAME} AS T
	USING UPD_Update AS S
	ON T.ModifiedPremiumTransactionMonthlyFactId = S.PremiumMasterFactID2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AuditID2, T.AnnualStatementLineDimID = S.AnnualStatementLineDimID2, T.AnnualStatementLineProductCodeDimId = S.AnnualStatementLineProductCodeDimID2, T.AgencyDimId = S.AgencyDimID2, T.PolicyDimId = S.PolicyDimID2, T.ContractCustomerDimId = S.ContractCustomerDimID2, T.RiskLocationDimId = S.RiskLocationDimID2, T.InsuranceReferenceDimId = S.InsuranceReferenceDimId, T.InsuranceReferenceCoverageDimId = S.InsuranceReferenceCoverageDimID2, T.CoverageDetailDimId = S.CoverageDetailDimId2, T.PremiumTransactionTypeDimId = S.PremiumTransactionTypeDimID2, T.EDWPremiumMasterCalculationPKId = S.EDWPremiumMasterCalculationPKID2, T.RunDateId = S.RunDateId, T.PremiumTransactionEnteredDateId = S.o_PremiumTransactionEnteredDateID2, T.PremiumTransactionEffectiveDateId = S.PremiumMasterCoverageEffectiveDateID2, T.PremiumTransactionBookedDateId = S.PremiumMasterRunDateID2, T.PremiumTransactionExpirationDateId = S.PremiumMasterCoverageExpirationDateID2, T.GeneratedRecordFlag = S.GeneratedRecordIndicator12, T.DirectWrittenPremium = S.DirectWrittenPremium1, T.RatablePremium = S.RatablePremium1, T.ClassifiedPremium = S.ClassifiedPremium1, T.OtherModifiedPremium = S.OtherModifiedPremium1, T.ScheduleModifiedPremium = S.ScheduleModifiedPremium1, T.ExperienceModifiedPremium = S.ExperienceModifiedPremium1, T.SubjectWrittenPremium = S.SubjectWrittenPremium1
),
ModifiedPremiumTransactionMonthlyFact_INS AS (

	------------ PRE SQL ----------
	@{pipeline().parameters.PRE_SQL}
	-------------------------------


	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.@{pipeline().parameters.TARGET_TABLE_NAME}
	(AuditId, AnnualStatementLineDimID, AnnualStatementLineProductCodeDimId, AgencyDimId, PolicyDimId, ContractCustomerDimId, RiskLocationDimId, InsuranceReferenceDimId, InsuranceReferenceCoverageDimId, CoverageDetailDimId, PremiumTransactionTypeDimId, EDWPremiumMasterCalculationPKId, RunDateId, PremiumTransactionEnteredDateId, PremiumTransactionEffectiveDateId, PremiumTransactionBookedDateId, PremiumTransactionExpirationDateId, GeneratedRecordFlag, DirectWrittenPremium, RatablePremium, ClassifiedPremium, OtherModifiedPremium, ScheduleModifiedPremium, ExperienceModifiedPremium, SubjectWrittenPremium)
	SELECT 
	AuditID AS AUDITID, 
	ANNUALSTATEMENTLINEDIMID, 
	AnnualStatementLineProductCodeDimID AS ANNUALSTATEMENTLINEPRODUCTCODEDIMID, 
	AgencyDimID AS AGENCYDIMID, 
	PolicyDimID AS POLICYDIMID, 
	ContractCustomerDimID AS CONTRACTCUSTOMERDIMID, 
	RiskLocationDimID AS RISKLOCATIONDIMID, 
	INSURANCEREFERENCEDIMID, 
	InsuranceReferenceCoverageDimID AS INSURANCEREFERENCECOVERAGEDIMID, 
	COVERAGEDETAILDIMID, 
	PremiumTransactionTypeDimID AS PREMIUMTRANSACTIONTYPEDIMID, 
	EDWPremiumMasterCalculationPKID AS EDWPREMIUMMASTERCALCULATIONPKID, 
	RUNDATEID, 
	o_PremiumTransactionEnteredDateID AS PREMIUMTRANSACTIONENTEREDDATEID, 
	PremiumMasterCoverageEffectiveDateID AS PREMIUMTRANSACTIONEFFECTIVEDATEID, 
	PremiumMasterBookedDateID AS PREMIUMTRANSACTIONBOOKEDDATEID, 
	PremiumMasterCoverageExpirationDateID AS PREMIUMTRANSACTIONEXPIRATIONDATEID, 
	GeneratedRecordIndicator AS GENERATEDRECORDFLAG, 
	DIRECTWRITTENPREMIUM, 
	RATABLEPREMIUM, 
	CLASSIFIEDPREMIUM, 
	OTHERMODIFIEDPREMIUM, 
	SCHEDULEMODIFIEDPREMIUM, 
	EXPERIENCEMODIFIEDPREMIUM, 
	SUBJECTWRITTENPREMIUM
	FROM RTR_INSERT_UPDATE_INSERT
),
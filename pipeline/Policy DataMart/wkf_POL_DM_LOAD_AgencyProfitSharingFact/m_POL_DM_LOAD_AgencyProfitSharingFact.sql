WITH
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
LKP_SalesDivisionDim AS (
	SELECT
	SalesDivisionDimID,
	AgencyCode
	FROM (
		Select A.AgencyCode AS AgencyCode, 
		SDD.SalesDivisionDimID AS SalesDivisionDimID
		FROM 
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Agency A,
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RegionalSalesManager RSM,
		@{pipeline().parameters.TARGET_TABLE_OWNER}.SalesDivisionDim SDD
		WHERE A.CurrentSnapshotFlag =1
		AND RSM.RegionalSalesManagerAKID = A.RegionalSalesManagerAKID
		AND RSM.CurrentSnapshotFlag = 1
		AND RSM.SalesDirectorAKID = SDD.EDWSalesDirectorAKID
		AND A.SalesTerritoryAKID = SDD.EDWSalesTerritoryAKID
		AND RSM.RegionalSalesManagerAKID = SDD.EDWRegionalSalesManagerAKID
		AND SDD.CurrentSnapshotFlag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyCode ORDER BY SalesDivisionDimID DESC) = 1
),
LKP_V3_AgencyDimID_Group AS (
	SELECT
	agency_dim_id,
	AgencyCode,
	eff_from_date,
	eff_to_date
	FROM (
		SELECT AgencyDim.AgencyDimID as agency_dim_id, AgencyDim.AgencyCode as AgencyCode, AgencyDim.EffectiveDate as eff_from_date, AgencyDim.ExpirationDate as eff_to_date
		 FROM V3.AgencyDim as AgencyDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyCode,eff_from_date,eff_to_date ORDER BY agency_dim_id DESC) = 1
),
LKP_SupProftSharingBonus AS (
	SELECT
	ProfitSharingBonusRate,
	DirectWrittenPremiumLow,
	DirectWrittenPremiumHigh,
	LossRatioLow,
	LossRatioHigh
	FROM (
		SELECT 
			ProfitSharingBonusRate,
			DirectWrittenPremiumLow,
			DirectWrittenPremiumHigh,
			LossRatioLow,
			LossRatioHigh
		FROM @{pipeline().parameters.EDW_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.SupProftSharingBonus
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY DirectWrittenPremiumLow,DirectWrittenPremiumHigh,LossRatioLow,LossRatioHigh ORDER BY ProfitSharingBonusRate DESC) = 1
),
LKP_AgencyBonusRates AS (
),
SQ_EarnedPremiumTransactionMonthlyFact AS (
	DECLARE @DATEEPSTART as datetime,
		                @DATEEPEND as datetime
	
	SET @DATEEPSTART =dateadd(yy,DATEDIFF(MM,0,dateadd(SS,-1,dateadd(mm,DATEDIFF(MM,0,GETDATE())-@{pipeline().parameters.NO_OF_MONTHS},0)))/12-@{pipeline().parameters.NO_OF_YEARS},0)
	SET @DATEEPEND=dateadd(SS,-1,dateadd(mm,DATEDIFF(MM,0,GETDATE())-@{pipeline().parameters.NO_OF_MONTHS},0))
	
	SELECT
	A.EDWAgencyAKID,
	A.AgencyCode,
	max(CASE WHEN ard.LegalPrimaryAgencyCode is null THEN a.agencycode ELSE   ard.LegalPrimaryAgencyCode end) as LegalPrimaryAgencyCode,
	SUM(case when ird.PolicyOfferingCode='600' then MonthlyChangeinDirectEarnedPremium else 0 end)  as BondsDirectEarnedPremium,
	SUM(case when ird.StrategicProfitCenterCode='3' and ird.PolicyOfferingCode<>'600' then MonthlyChangeinDirectEarnedPremium else 0 end)  as NSIEarnedPremium,
	sum(case when PolicyOfferingCode in ('600') 
	OR ird.InsuranceSegmentCode = '3'
	OR (ircd.CoverageGroupCode in ('BOILER','CYBERSEC','DATACOMP','CYBERSUITE') 
	OR ircd.CoverageGroupCode like '%TRIA' 
	OR ircd.CoverageGroupDescription in ('MCCA Surcharge'))
	OR ird.InsuranceReferenceLineofBusinessCode in ('350','311','590','812','890','900')
	OR (ird.RatingPlanAbbreviation in ('LRARO','Retro'))
	OR apd.asl_prdct_code in ('200', '220')
	OR (ird.StrategicProfitCentercode <> '5' and cdd.ISOClassCode='0174')
	then 0 else MonthlyChangeinDirectEarnedPremium end)  as ProfitSharingEligibleDirectEarnedPremium
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.EarnedPremiumTransactionMonthlyFact mf
	LEFT OUTER join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim IRD on MF.InsuranceReferenceDimID = IRD.InsuranceReferenceDimID
	LEFT OUTER join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim ircd on MF.InsuranceReferenceCoverageDimID = IRCD.InsuranceReferenceCoverageDimID
	LEFT OUTER join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailDim cdd on  MF.CoverageDetailDimID = CDD.CoverageDetailDimID
	LEFT OUTER join @{pipeline().parameters.SOURCE_TABLE_OWNER}.ASL_Product_code_Dim apd on MF.AnnualStatementLineProductCodeDimID = APD.asl_prdct_code_dim_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim cld on PremiumTransactionRunDateID=cld.clndr_id
	Join @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim A ON MF.AgencyDimId=A.AgencyDimId 
	
	left join (select * from  @{pipeline().parameters.SOURCE_TABLE_OWNER}.Agencyrelationshipdim where currentsnapshotflag = 1) ard
	 on (A.edwagencyakid=ard.edwagencyakid and cld.clndr_date between ARD.agencyrelationshipeffectivedate and ARD.agencyrelationshipexpirationdate)
	
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim P
	on mf.PolicyDimID=p.pol_dim_id
	where cld.clndr_date between @DATEEPSTART and @DATEEPEND
	@{pipeline().parameters.WHERECLAUSEEARNEDPREMIUM}
	group by A.EDWAgencyAKID,
	A.AgencyCode
	--CASE WHEN ard.LegalPrimaryAgencyCode is null THEN a.agencycode ELSE   --ard.LegalPrimaryAgencyCode end
	
	
	/*Left join with "agencyrelationshipdim" in order to get legalprimaryagencycode values from "AgencyRelationshipDim" table*/
),
EXP_GetValues_EarnedPremium AS (
	SELECT
	EDWAgencyAKID,
	AgencyCode,
	LegalPrimaryAgencyCode,
	BondsDirectEarnedPremium,
	NSIEarnedPremium,
	ProfitSharingEligibleDirectEarnedPremium
	FROM SQ_EarnedPremiumTransactionMonthlyFact
),
SQ_loss_master_fact AS (
	DECLARE @DATELMSTART_ONE AS DATETIME
		,@DATELMSTART_TWO AS DATETIME
		,@DATELMEND AS DATETIME
	
	SET @DATELMSTART_ONE = dateadd(yy, DATEDIFF(MM, 0, dateadd(SS, - 1, dateadd(mm, DATEDIFF(MM, 0, GETDATE()) - @{pipeline().parameters.NO_OF_MONTHS}, 0))) / 12 - @{pipeline().parameters.NO_OF_YEARS}, 0)
	SET @DATELMSTART_TWO = dateadd(yy, DATEDIFF(MM, 0, dateadd(SS, - 1, dateadd(mm, DATEDIFF(MM, 0, GETDATE()) - @{pipeline().parameters.NO_OF_MONTHS}, 0))) / 12 - 1 - @{pipeline().parameters.NO_OF_YEARS}, 0)
	SET @DATELMEND = dateadd(SS, - 1, dateadd(mm, DATEDIFF(MM, 0, GETDATE()) - @{pipeline().parameters.NO_OF_MONTHS}, 0))
	
	SELECT EDWAgencyAKID
		,AgencyCode,
	max(LegalPrimaryAgencyCode)  LegalPrimaryAgencyCode
		,sum(CASE 
				WHEN PolicyOfferingCode = '600'
					THEN CASE 
							WHEN claim_rpted_date <= '2013-12-31 00:00:00'
								THEN (DirectLossPaidIR + DirectLossOutstandingIR + DirectALAEPaidIR)
							ELSE (DirectLossPaidIR + DirectLossOutstandingIR + DirectALAEPaidIR + DirectALAEOutstandingIR)
							END
				ELSE 0
				END) BondsDirectIncurredLoss
		,SUM(CASE 
				WHEN PolicyOfferingCode = '600' AND RunDate >= @DATELMSTART_ONE
					THEN CASE 
							WHEN claim_rpted_date <= '2013-12-31 00:00:00'
								THEN (DirectLossPaidIR + DirectLossOutstandingIR + DirectALAEPaidIR)
							ELSE (DirectLossPaidIR + DirectLossOutstandingIR + DirectALAEPaidIR + DirectALAEOutstandingIR)
							END
				ELSE 0
				END) BondsDirectIncurredLossSameYear
		,SUM(CASE 
				WHEN RunDate < @DATELMSTART_ONE
					OR PolicyOfferingCode in ('600')
	OR InsuranceSegmentCode = '3'
	OR (CoverageGroupCode in ('BOILER','CYBERSEC','DATACOMP','CYBERSUITE')
	OR CoverageGroupCode like '%TRIA'
	OR CoverageGroupDescription in ('MCCA Surcharge'))
	OR InsuranceReferenceLineofBusinessCode in ('350','311','590','812','890','900')
	OR (RatingPlanAbbreviation in ('LRARO','Retro'))
	OR asl_prdct_code in ('200', '220')
	OR (StrategicProfitCentercode <> '5' and ISOClassCode='0174')
	then 0			ELSE (
						CASE 
							WHEN claim_rpted_date <= '2013-12-31 00:00:00'
								THEN (DirectLossPaidIR + DirectLossOutstandingER + DirectALAEPaidIR)
							ELSE (DirectLossPaidIR + DirectLossOutstandingER + DirectALAEPaidIR + DirectALAEOutstandingER)
							END
						)
				END) ProfitSharingEligibleDirectIncurredLoss ----Added new column to the query as the Profit Sharing loss should not include recoveries for Reserves.
	
	FROM (
		SELECT A.EDWAgencyAKID
			,A.AgencyCode
			,(
			CASE 
				WHEN ard.LegalPrimaryAgencyCode IS NULL
					THEN a.agencycode
				ELSE ard.LegalPrimaryAgencyCode
				END
			) AS LegalPrimaryAgencyCode,
			A.CurrentSnapshotFlag
			,POL.edw_pol_ak_id
			,OCC.edw_claim_occurrence_ak_id
			,CLTF.direct_loss_paid_including_recoveries DirectLossPaidIR
			,CLTF.direct_alae_paid_including_recoveries DirectALAEPaidIR
			,0.0 AS DirectLossOutstandingIR
			,0.0 AS DirectALAEOutstandingIR
			,0.0 AS DirectLossOutstandingER --Added new column to the query as the Profit Sharing loss should not include recoveries for Reserves.
			,0.0 AS DirectALAEOutstandingER --Added new column to the query as the Profit Sharing loss should not include recoveries for Reserves.
			,OCC.claim_rpted_date
			,CD.clndr_date AS RunDate
			,IRD.PolicyOfferingCode
			,ird.StrategicProfitCentercode
			,ird.InsuranceSegmentCode
			,ircd.CoverageGroupCode
			,ircd.CoverageGroupDescription
			,ird.InsuranceReferenceLineofBusinessCode
			,cdd.ISOClassCode
			,ird.RatingPlanAbbreviation
			,apd.asl_prdct_code
			,ircd.PmsMajorPerilCode
			,ircd.PmsRiskUnitGroupDescription
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_loss_transaction_fact CLTF
		JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim CD
			ON CLTF.claim_trans_date_id = CD.clndr_id
		JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_dim OCC
			ON CLTF.claim_occurrence_dim_id = OCC.claim_occurrence_dim_id
		JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim A
			ON CLTF.AgencyDimId = A.AgencyDimID
			
			left join (select * from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Agencyrelationshipdim where currentsnapshotflag = 1) ard
	on (a.edwagencyakid=ard.edwagencyakid and CD.clndr_date between agencyrelationshipeffectivedate and agencyrelationshipexpirationdate)
	
			
		JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim POL
			ON CLTF.pol_dim_id = POL.pol_dim_id
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim ird
			ON CLTF.InsuranceReferenceDimId = ird.InsuranceReferenceDimId
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim ircd
			ON CLTF.InsuranceReferenceCoverageDimId = ircd.InsuranceReferenceCoverageDimId
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailDim cdd
			ON CLTF.CoverageDetailDimId = cdd.CoverageDetailDimId
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_product_code_dim apd
			ON CLTF.asl_prdct_code_dim_id = apd.asl_prdct_code_dim_id
		WHERE clndr_date < '01/01/2001 00:00:00'
		
		UNION ALL
		
		SELECT A.EDWAgencyAKID
			,A.AgencyCode
			,(
			CASE 
				WHEN ard.LegalPrimaryAgencyCode IS NULL
					THEN a.agencycode
				ELSE ard.LegalPrimaryAgencyCode
				END
			) AS LegalPrimaryAgencyCode
			,A.CurrentSnapshotFlag
			,POL.edw_pol_ak_id
			,OCC.edw_claim_occurrence_ak_id
			,VLMF.DirectLossPaidIR
			,VLMF.DirectALAEPaidIR
			,VLMF.DirectLossOutstandingIR
			,VLMF.DirectALAEOutstandingIR
			,VLMF.DirectLossOutstandingER --Added new column to the query as the Profit Sharing loss should not include recoveries for Reserves.
			,VLMF.DirectALAEOutstandingER --Added new column to the query as the Profit Sharing loss should not include recoveries for Reserves.
			,OCC.claim_rpted_date
			,CD.clndr_date AS RunDate
			,IRD.PolicyOfferingCode
			,ird.StrategicProfitCentercode
			,ird.InsuranceSegmentCode
			,ircd.CoverageGroupCode
			,ircd.CoverageGroupDescription
			,ird.InsuranceReferenceLineofBusinessCode
			,cdd.ISOClassCode
			,ird.RatingPlanAbbreviation
			,apd.asl_prdct_code
			,ircd.PmsMajorPerilCode
			,ircd.PmsRiskUnitGroupDescription
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.vwLossMasterFact VLMF
		JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim CD
			ON VLMF.loss_master_run_date_id = CD.clndr_id
		JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_dim OCC
			ON VLMF.claim_occurrence_dim_id = OCC.claim_occurrence_dim_id
		JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim A
			ON VLMF.AgencyDimId = A.AgencyDimID
			
			left join (select * from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Agencyrelationshipdim where currentsnapshotflag = 1) ard
	on (a.edwagencyakid=ard.edwagencyakid and CD.clndr_date between ard.agencyrelationshipeffectivedate and ard.agencyrelationshipexpirationdate)
			
			
		JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim POL
			ON VLMF.pol_dim_id = POL.pol_dim_id
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim ird
			ON VLMF.InsuranceReferenceDimId = ird.InsuranceReferenceDimId
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim ircd
			ON VLMF.InsuranceReferenceCoverageDimId = ircd.InsuranceReferenceCoverageDimId
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailDim cdd
			ON VLMF.CoverageDetailDimId = cdd.CoverageDetailDimId
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_product_code_dim apd
			ON VLMF.asl_prdct_code_dim_id = apd.asl_prdct_code_dim_id
		WHERE clndr_date >= '01/01/2001 00:00:00'
		) A
	WHERE RunDate BETWEEN @DATELMSTART_TWO
			AND @DATELMEND @{pipeline().parameters.WHERECLAUSELOSS}
	GROUP BY EDWAgencyAKID
		,AgencyCode
	--LegalPrimaryAgencyCode 
	
	
	/*Left join with "agencyrelationshipdim" in order to get legalprimaryagencycode values from "AgencyRelationshipDim" table*/
),
EXP_GetValues_Loss AS (
	SELECT
	EDWAgencyAKID,
	AgencyCode,
	LegalPrimaryAgencyCode,
	DirectLossPaidExcludingRecoveries AS BondsDirectIncurredLoss,
	BondsDirectIncurredLossSameYear,
	DirectLossOutstandingExcludingRecoveries AS ProfitSharingEligibleDirectIncurredLoss
	FROM SQ_loss_master_fact
),
JNR_Earned_Loss AS (SELECT
	EXP_GetValues_EarnedPremium.EDWAgencyAKID, 
	EXP_GetValues_EarnedPremium.AgencyCode, 
	EXP_GetValues_EarnedPremium.LegalPrimaryAgencyCode, 
	EXP_GetValues_EarnedPremium.BondsDirectEarnedPremium, 
	EXP_GetValues_EarnedPremium.NSIEarnedPremium, 
	EXP_GetValues_EarnedPremium.ProfitSharingEligibleDirectEarnedPremium, 
	EXP_GetValues_Loss.EDWAgencyAKID AS EDWAgencyAKID1, 
	EXP_GetValues_Loss.AgencyCode AS AgencyCode1, 
	EXP_GetValues_Loss.LegalPrimaryAgencyCode AS LegalPrimaryAgencyCode1, 
	EXP_GetValues_Loss.BondsDirectIncurredLoss, 
	EXP_GetValues_Loss.BondsDirectIncurredLossSameYear, 
	EXP_GetValues_Loss.ProfitSharingEligibleDirectIncurredLoss
	FROM EXP_GetValues_EarnedPremium
	FULL OUTER JOIN EXP_GetValues_Loss
	ON EXP_GetValues_Loss.EDWAgencyAKID = EXP_GetValues_EarnedPremium.EDWAgencyAKID
),
EXP_DefaultHandle AS (
	SELECT
	BondsDirectEarnedPremium,
	NSIEarnedPremium,
	ProfitSharingEligibleDirectEarnedPremium,
	BondsDirectIncurredLoss,
	BondsDirectIncurredLossSameYear,
	ProfitSharingEligibleDirectIncurredLoss,
	EDWAgencyAKID,
	AgencyCode,
	LegalPrimaryAgencyCode,
	EDWAgencyAKID1,
	AgencyCode1,
	LegalPrimaryAgencyCode1,
	-- *INF*: IIF(ISNULL(EDWAgencyAKID),EDWAgencyAKID1,EDWAgencyAKID)
	IFF(EDWAgencyAKID IS NULL, EDWAgencyAKID1, EDWAgencyAKID) AS o_EDWAgencyAKID,
	-- *INF*: IIF(ISNULL(AgencyCode),AgencyCode1,AgencyCode)
	IFF(AgencyCode IS NULL, AgencyCode1, AgencyCode) AS o_AgencyCode,
	-- *INF*: IIF(ISNULL(LegalPrimaryAgencyCode),LegalPrimaryAgencyCode1,LegalPrimaryAgencyCode)
	IFF(LegalPrimaryAgencyCode IS NULL, LegalPrimaryAgencyCode1, LegalPrimaryAgencyCode) AS o_LegalPrimaryAgencyCode
	FROM JNR_Earned_Loss
),
SQ_PremiumMasterFact AS (
	DECLARE @DATEPMSTART as datetime,
		                @DATEPMEND as datetime
	
	SET @DATEPMSTART =dateadd(yy,DATEDIFF(MM,0,dateadd(SS,-1,dateadd(mm,DATEDIFF(MM,0,GETDATE())-@{pipeline().parameters.NO_OF_MONTHS},0)))/12-@{pipeline().parameters.NO_OF_YEARS},0)
	SET @DATEPMEND =dateadd(SS,-1,dateadd(mm,DATEDIFF(MM,0,GETDATE())-@{pipeline().parameters.NO_OF_MONTHS},0))
	
	SELECT
	A.EDWAgencyAKID,
	A.AgencyCode,
	max(CASE WHEN ard.LegalPrimaryAgencyCode is null THEN a.agencycode ELSE   ard.LegalPrimaryAgencyCode end) as LegalPrimaryAgencyCode,
	SUM(case when ird.PolicyOfferingCode='600' then PremiumMasterDirectWrittenPremium else 0 end) as BondsDirectWrittenPremium,
	SUM(case when ird.StrategicProfitCenterCode='3' and ird.PolicyOfferingCode<>'600' then PremiumMasterDirectWrittenPremium else 0 end) as NSIDirectWrittenPremium,
	sum(case when PolicyOfferingCode in ('600') 
	OR ird.InsuranceSegmentCode = '3'
	OR (ircd.CoverageGroupCode in ('BOILER','CYBERSEC','DATACOMP','CYBERSUITE') 
	OR ircd.CoverageGroupCode like '%TRIA' 
	OR ircd.CoverageGroupDescription in ('MCCA Surcharge'))
	OR ird.InsuranceReferenceLineofBusinessCode in ('350','311','590','812','890','900')
	OR (ird.RatingPlanAbbreviation in ('LRARO','Retro'))
	OR apd.asl_prdct_code in ('200', '220')
	OR (ird.StrategicProfitCentercode <> '5' and cdd.ISOClassCode='0174')
	then 0
	ELSE case when PTT.ReasonAmendedCode @{pipeline().parameters.REASON_AMENDED_CODE} and PTT.PremiumTypeCode='D' 
	then pmf.PremiumMasterDirectWrittenPremium+pmf.PremiumMasterPremium else pmf.PremiumMasterDirectWrittenPremium end end)  as ProfitSharingEligibleDirectWrittenPremium,
	sum(case when PTT.ReasonAmendedCode @{pipeline().parameters.REASON_AMENDED_CODE} and PTT.PremiumTypeCode='D' and not(PolicyOfferingCode in ('600') or 
	 InsuranceReferenceLineOfBusinessCode='330'
	OR ird.InsuranceSegmentCode = '3'
	OR ird.StrategicProfitCentercode ='5'
	OR (ircd.CoverageGroupCode in ('BOILER','CYBERSEC','DATACOMP','CYBERSUITE') 
	OR ircd.CoverageGroupCode like '%TRIA' 
	OR ircd.CoverageGroupDescription in ('Earthquake','MCCA Surcharge'))
	OR ird.InsuranceReferenceLineofBusinessCode in ('811','350','310','311', '312','590','812','890','900')
	OR (ird.InsuranceReferenceLineOfBusinessCode='100' and cdd.ISOClassCode in ('9741', '9740'))
	OR (ird.RatingPlanAbbreviation in ('LRARO','Retro'))
	OR apd.asl_prdct_code in ('200', '220')
	OR cdd.ISOClassCode='0174') then pmf.PremiumMasterPremium else 0 end) as CollectionWriteOffPremium,
	sum(PremiumMasterAgencyDirectWrittenCommission) AS PremiumMasterAgencyDirectWrittenCommission
	 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterFact pmf
	 LEFT OUTER join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim IRD 
	on pMF.InsuranceReferenceDimID = IRD.InsuranceReferenceDimID
	 LEFT OUTER join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim IRCD 
	on 	PMF.InsuranceReferenceCoverageDimID = IRCD.InsuranceReferenceCoverageDimID
	 LEFT OUTER join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailDim cdd 
	on  PMF.CoverageDetailDimID = CDD.CoverageDetailDimID
	 LEFT OUTER join @{pipeline().parameters.SOURCE_TABLE_OWNER}.ASL_Product_code_Dim apd
	 on PMF.AnnualStatementLineProductCodeDimID = APD.asl_prdct_code_dim_id
	 join @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim cld 
	on pmf.PremiumMasterRunDateID=cld.clndr_id
	Join @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim A 
	on pmf.AgencyDimId=A.AgencyDimId 
	--and a.currentsnapshotflag = 1
	
	left join  (select * from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Agencyrelationshipdim where currentsnapshotflag = 1)   ard on (A.edwagencyakid=ard.edwagencyakid and cld.clndr_date between ard.agencyrelationshipeffectivedate and ard.agencyrelationshipexpirationdate)
	
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransactionTypeDim PTT
	on pmf.PremiumTransactionTypeDimID=PTT.PremiumTransactionTypeDimID
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim P
	on pmf.PolicyDimID=p.pol_dim_id
	 where cld.clndr_date between @DATEPMSTART and @DATEPMEND
	@{pipeline().parameters.WHERECLAUSEPREMIUM}
	group by A.EDWAgencyAKID,
	A.AgencyCode
	--CASE WHEN ard.LegalPrimaryAgencyCode is null THEN a.agencycode ELSE   --ard.LegalPrimaryAgencyCode end
	
	
	
	/*Left join with "agencyrelationshipdim" in order to get legalprimaryagencycode values from "AgencyRelationshipDim" table*/
),
EXP_GetValues_PremiumMaster AS (
	SELECT
	EDWAgencyAKID,
	AgencyCode,
	LegalPrimaryAgencyCode,
	BondsDirectWrittenPremium,
	NSIDirectWrittenPremium,
	ProfitSharingEligibleDirectWrittenPremium,
	CollectionWriteOffPremium,
	PremiumMasterAgencyDirectWrittenCommission AS RegularCommission
	FROM SQ_PremiumMasterFact
),
JNR_Earned_Loss_PremiumMaster AS (SELECT
	EXP_GetValues_PremiumMaster.EDWAgencyAKID, 
	EXP_GetValues_PremiumMaster.AgencyCode, 
	EXP_GetValues_PremiumMaster.LegalPrimaryAgencyCode, 
	EXP_GetValues_PremiumMaster.BondsDirectWrittenPremium, 
	EXP_GetValues_PremiumMaster.NSIDirectWrittenPremium, 
	EXP_GetValues_PremiumMaster.ProfitSharingEligibleDirectWrittenPremium, 
	EXP_GetValues_PremiumMaster.CollectionWriteOffPremium, 
	EXP_GetValues_PremiumMaster.RegularCommission, 
	EXP_DefaultHandle.BondsDirectEarnedPremium, 
	EXP_DefaultHandle.NSIEarnedPremium, 
	EXP_DefaultHandle.ProfitSharingEligibleDirectEarnedPremium, 
	EXP_DefaultHandle.BondsDirectIncurredLoss, 
	EXP_DefaultHandle.BondsDirectIncurredLossSameYear, 
	EXP_DefaultHandle.ProfitSharingEligibleDirectIncurredLoss, 
	EXP_DefaultHandle.o_EDWAgencyAKID AS EDWAgencyAKID1, 
	EXP_DefaultHandle.o_AgencyCode AS AgencyCode1, 
	EXP_DefaultHandle.o_LegalPrimaryAgencyCode AS LegalPrimaryAgencyCode1
	FROM EXP_GetValues_PremiumMaster
	FULL OUTER JOIN EXP_DefaultHandle
	ON EXP_DefaultHandle.o_EDWAgencyAKID = EXP_GetValues_PremiumMaster.EDWAgencyAKID
),
EXP_DefaultHandle1 AS (
	SELECT
	EDWAgencyAKID,
	AgencyCode,
	LegalPrimaryAgencyCode,
	BondsDirectWrittenPremium,
	NSIDirectWrittenPremium,
	ProfitSharingEligibleDirectWrittenPremium,
	CollectionWriteOffPremium,
	RegularCommission,
	BondsDirectEarnedPremium,
	NSIEarnedPremium,
	ProfitSharingEligibleDirectEarnedPremium,
	ProfitSharingEligibleDirectEarnedPremium+CollectionWriteOffPremium AS O_ProfitSharingEligibleDirectEarnedPremium,
	BondsDirectIncurredLoss,
	BondsDirectIncurredLossSameYear,
	ProfitSharingEligibleDirectIncurredLoss,
	EDWAgencyAKID1,
	AgencyCode1,
	LegalPrimaryAgencyCode1,
	-- *INF*: IIF(ISNULL(EDWAgencyAKID),EDWAgencyAKID1,EDWAgencyAKID)
	IFF(EDWAgencyAKID IS NULL, EDWAgencyAKID1, EDWAgencyAKID) AS o_EDWAgencyAKID,
	-- *INF*: IIF(ISNULL(AgencyCode),AgencyCode1,AgencyCode)
	IFF(AgencyCode IS NULL, AgencyCode1, AgencyCode) AS o_AgencyCode,
	-- *INF*: IIF(ISNULL(LegalPrimaryAgencyCode),LegalPrimaryAgencyCode1,LegalPrimaryAgencyCode)
	IFF(LegalPrimaryAgencyCode IS NULL, LegalPrimaryAgencyCode1, LegalPrimaryAgencyCode) AS o_LegalPrimaryAgencyCode
	FROM JNR_Earned_Loss_PremiumMaster
),
SQ_DCTDividendFact AS (
	DECLARE @DATEDVSTART as datetime,
		                @DATEDVEND as datetime
	
	SET @DATEDVSTART =dateadd(yy,DATEDIFF(MM,0,dateadd(SS,-1,dateadd(mm,DATEDIFF(MM,0,GETDATE())-@{pipeline().parameters.NO_OF_MONTHS},0)))/12-@{pipeline().parameters.NO_OF_YEARS},0)
	SET @DATEDVEND =dateadd(SS,-1,dateadd(mm,DATEDIFF(MM,0,GETDATE())-@{pipeline().parameters.NO_OF_MONTHS},0))
	
	SELECT
	A.EDWAgencyAKID,
	A.AgencyCode,
	max(CASE WHEN ard.LegalPrimaryAgencyCode is null THEN a.agencycode ELSE   ard.LegalPrimaryAgencyCode end) as  LegalPrimaryAgencyCode,
	SUM(DividendPaidAmount) AS DividendPaidAmount
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCTDividendFact df
	LEFT OUTER join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim ird on df.InsuranceReferenceDimId=ird.InsuranceReferenceDimId
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim cld on df.DividendRunDateId=cld.clndr_id
	Join @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim A on df.AgencyDimId=A.AgencyDimId
	Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol on pol.pol_dim_id=df.PolicyDimId and pol.pol_sym='000'
	left join 
	 (select * from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Agencyrelationshipdim where currentsnapshotflag = 1) ard
	 on (A.edwagencyakid=ard.edwagencyakid 
	 and cld.clndr_date between ard.agencyrelationshipeffectivedate and ard.agencyrelationshipexpirationdate)
	where cld.clndr_date between @DATEDVSTART and @DATEDVEND
	@{pipeline().parameters.WHERECLAUSEDIVIDEND}
	group by A.EDWAgencyAKID,
	A.AgencyCode
	--CASE WHEN ard.LegalPrimaryAgencyCode is null THEN a.agencycode ELSE   --ard.LegalPrimaryAgencyCode end
	
	
	/*Left join with "agencyrelationshipdim" in order to get legalprimaryagencycode values from "AgencyRelationshipDim" table*/
),
SQ_DividendFact AS (
	DECLARE @DATEDVSTART as datetime,
		                @DATEDVEND as datetime
	
	SET @DATEDVSTART =dateadd(yy,DATEDIFF(MM,0,dateadd(SS,-1,dateadd(mm,DATEDIFF(MM,0,GETDATE())-@{pipeline().parameters.NO_OF_MONTHS},0)))/12-@{pipeline().parameters.NO_OF_YEARS},0)
	SET @DATEDVEND =dateadd(SS,-1,dateadd(mm,DATEDIFF(MM,0,GETDATE())-@{pipeline().parameters.NO_OF_MONTHS},0))
	
	SELECT
	A.EDWAgencyAKID,
	A.AgencyCode,
	
	max(CASE WHEN ard.LegalPrimaryAgencyCode is null THEN a.agencycode ELSE   ard.LegalPrimaryAgencyCode end) as  LegalPrimaryAgencyCode,
	
	
	SUM(DividendPaidAmount) AS DividendPaidAmount
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DividendFact df
	LEFT OUTER join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim ird on df.StrategicProfitCenterDimId=ird.InsuranceReferenceDimId
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim cld on df.DividendRunDateId=cld.clndr_id
	Join @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim A on df.AgencyDimId=A.AgencyDimId
	Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol on pol.pol_dim_id=df.PolicyDimId and pol.pol_sym<>'000'
	left join 
	 (select * from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Agencyrelationshipdim where currentsnapshotflag = 1) ard
	 on (A.edwagencyakid=ard.edwagencyakid 
	 and cld.clndr_date between ard.agencyrelationshipeffectivedate and ard.agencyrelationshipexpirationdate)
	
	where cld.clndr_date between @DATEDVSTART and @DATEDVEND
	@{pipeline().parameters.WHERECLAUSEDIVIDEND}
	group by A.EDWAgencyAKID,
	A.AgencyCode
	--CASE WHEN ard.LegalPrimaryAgencyCode is null THEN a.agencycode ELSE   --ard.LegalPrimaryAgencyCode end
	
	
	/*Left join with "agencyrelationshipdim" in order to get legalprimaryagencycode values from "AgencyRelationshipDim" table*/
),
UN_DCT_and_NonDCT_DividendFact AS (
	SELECT EDWAgencyAKID, AgencyCode, LegalPrimaryAgencyCode, DividendPaidAmount
	FROM SQ_DCTDividendFact
	UNION
	SELECT EDWAgencyAKID, AgencyCode, LegalPrimaryAgencyCode, DividendPaidAmount
	FROM SQ_DividendFact
),
AGG_DividendPaidAmount AS (
	SELECT
	EDWAgencyAKID, 
	AgencyCode, 
	LegalPrimaryAgencyCode, 
	DividendPaidAmount, 
	SUM(DividendPaidAmount) AS o_DividendPaidAmount
	FROM UN_DCT_and_NonDCT_DividendFact
	GROUP BY EDWAgencyAKID
),
EXP_GetValues_Dividend AS (
	SELECT
	EDWAgencyAKID,
	AgencyCode,
	LegalPrimaryAgencyCode,
	o_DividendPaidAmount AS DividendPaidAmount
	FROM AGG_DividendPaidAmount
),
JNR_All AS (SELECT
	EXP_DefaultHandle1.BondsDirectWrittenPremium, 
	EXP_DefaultHandle1.NSIDirectWrittenPremium, 
	EXP_DefaultHandle1.ProfitSharingEligibleDirectWrittenPremium, 
	EXP_DefaultHandle1.RegularCommission, 
	EXP_DefaultHandle1.BondsDirectEarnedPremium, 
	EXP_DefaultHandle1.NSIEarnedPremium, 
	EXP_DefaultHandle1.O_ProfitSharingEligibleDirectEarnedPremium AS ProfitSharingEligibleDirectEarnedPremium, 
	EXP_DefaultHandle1.BondsDirectIncurredLoss, 
	EXP_DefaultHandle1.BondsDirectIncurredLossSameYear, 
	EXP_DefaultHandle1.ProfitSharingEligibleDirectIncurredLoss, 
	EXP_DefaultHandle1.o_EDWAgencyAKID AS EDWAgencyAKID, 
	EXP_DefaultHandle1.o_AgencyCode AS AgencyCode, 
	EXP_DefaultHandle1.o_LegalPrimaryAgencyCode AS LegalPrimaryAgencyCode, 
	EXP_GetValues_Dividend.EDWAgencyAKID AS EDWAgencyAKID1, 
	EXP_GetValues_Dividend.AgencyCode AS AgencyCode1, 
	EXP_GetValues_Dividend.LegalPrimaryAgencyCode AS LegalPrimaryAgencyCode1, 
	EXP_GetValues_Dividend.DividendPaidAmount
	FROM EXP_DefaultHandle1
	FULL OUTER JOIN EXP_GetValues_Dividend
	ON EXP_GetValues_Dividend.EDWAgencyAKID = EXP_DefaultHandle1.o_EDWAgencyAKID
),
EXP_DefaultHandle3 AS (
	SELECT
	BondsDirectWrittenPremium AS i_BondsDirectWrittenPremium,
	NSIDirectWrittenPremium AS i_NSIDirectWrittenPremium,
	ProfitSharingEligibleDirectWrittenPremium AS i_ProfitSharingEligibleDirectWrittenPremium,
	RegularCommission AS i_RegularCommission,
	BondsDirectEarnedPremium AS i_BondsDirectEarnedPremium,
	NSIEarnedPremium AS i_NSIEarnedPremium,
	ProfitSharingEligibleDirectEarnedPremium AS i_ProfitSharingEligibleDirectEarnedPremium,
	BondsDirectIncurredLoss AS i_BondsDirectIncurredLoss,
	BondsDirectIncurredLossSameYear AS i_BondsDirectIncurredLossSameYear,
	ProfitSharingEligibleDirectIncurredLoss AS i_ProfitSharingEligibleDirectIncurredLoss,
	EDWAgencyAKID,
	AgencyCode,
	LegalPrimaryAgencyCode,
	EDWAgencyAKID1,
	AgencyCode1,
	LegalPrimaryAgencyCode1,
	DividendPaidAmount,
	-- *INF*: IIF(ISNULL(EDWAgencyAKID),EDWAgencyAKID1,EDWAgencyAKID)
	IFF(EDWAgencyAKID IS NULL, EDWAgencyAKID1, EDWAgencyAKID) AS o_EDWAgencyAKID,
	-- *INF*: IIF(ISNULL(AgencyCode),AgencyCode1,AgencyCode)
	IFF(AgencyCode IS NULL, AgencyCode1, AgencyCode) AS o_AgencyCode,
	-- *INF*: IIF(ISNULL(LegalPrimaryAgencyCode),LegalPrimaryAgencyCode1,LegalPrimaryAgencyCode)
	IFF(LegalPrimaryAgencyCode IS NULL, LegalPrimaryAgencyCode1, LegalPrimaryAgencyCode) AS o_LegalPrimaryAgencyCode,
	-- *INF*: IIF(ISNULL(i_BondsDirectWrittenPremium),0,i_BondsDirectWrittenPremium)
	IFF(i_BondsDirectWrittenPremium IS NULL, 0, i_BondsDirectWrittenPremium) AS o_BondsDirectWrittenPremium,
	-- *INF*: IIF(ISNULL(i_NSIDirectWrittenPremium),0,i_NSIDirectWrittenPremium)
	IFF(i_NSIDirectWrittenPremium IS NULL, 0, i_NSIDirectWrittenPremium) AS o_NSIDirectWrittenPremium,
	-- *INF*: IIF(ISNULL(i_ProfitSharingEligibleDirectWrittenPremium),0,i_ProfitSharingEligibleDirectWrittenPremium)
	IFF(i_ProfitSharingEligibleDirectWrittenPremium IS NULL, 0, i_ProfitSharingEligibleDirectWrittenPremium) AS o_ProfitSharingEligibleDirectWrittenPremium,
	-- *INF*: IIF(ISNULL(i_RegularCommission),0,i_RegularCommission)
	IFF(i_RegularCommission IS NULL, 0, i_RegularCommission) AS o_RegularCommission,
	-- *INF*: IIF(ISNULL(i_BondsDirectEarnedPremium),0,i_BondsDirectEarnedPremium)
	IFF(i_BondsDirectEarnedPremium IS NULL, 0, i_BondsDirectEarnedPremium) AS o_BondsDirectEarnedPremium,
	-- *INF*: IIF(ISNULL(i_NSIEarnedPremium),0,i_NSIEarnedPremium)
	IFF(i_NSIEarnedPremium IS NULL, 0, i_NSIEarnedPremium) AS o_NSIEarnedPremium,
	-- *INF*: IIF(ISNULL(i_ProfitSharingEligibleDirectEarnedPremium),0,i_ProfitSharingEligibleDirectEarnedPremium)
	IFF(i_ProfitSharingEligibleDirectEarnedPremium IS NULL, 0, i_ProfitSharingEligibleDirectEarnedPremium) AS o_ProfitSharingEligibleDirectEarnedPremium,
	-- *INF*: IIF(ISNULL(i_BondsDirectIncurredLoss),0,i_BondsDirectIncurredLoss)
	IFF(i_BondsDirectIncurredLoss IS NULL, 0, i_BondsDirectIncurredLoss) AS o_BondsDirectIncurredLoss,
	-- *INF*: IIF(ISNULL(i_BondsDirectIncurredLossSameYear),0,i_BondsDirectIncurredLossSameYear)
	IFF(i_BondsDirectIncurredLossSameYear IS NULL, 0, i_BondsDirectIncurredLossSameYear) AS o_BondsDirectIncurredLossSameYear,
	-- *INF*: IIF(ISNULL(i_ProfitSharingEligibleDirectIncurredLoss),0,i_ProfitSharingEligibleDirectIncurredLoss)
	-- 
	-- 
	-- 
	-- 
	IFF(i_ProfitSharingEligibleDirectIncurredLoss IS NULL, 0, i_ProfitSharingEligibleDirectIncurredLoss) AS o_ProfitSharingEligibleDirectIncurredLoss,
	-- *INF*: IIF(ISNULL(DividendPaidAmount),0,DividendPaidAmount)
	IFF(DividendPaidAmount IS NULL, 0, DividendPaidAmount) AS o_DividendAmount,
	1 AS o_Dummy_Records
	FROM JNR_All
),
SQ_RunDate AS (
	SELECT
	       a.clndr_id AS RunDateID
	       ,a.clndr_date AS RunDate
	       ,c.clndr_id AS PreviousYearRunDateID
	       ,b.CalendarEndOfMonthDate AS PreviousYearRunDate
	       ,1 AS dummy
	FROM   calendar_dim a
	              ,calendar_dim b
	              ,calendar_dim c
	WHERE a.clndr_date = DATEADD(dd, -1, DATEADD(mm, DATEDIFF(MM, 0, GETDATE()) - @{pipeline().parameters.NO_OF_MONTHS}, 0))
	AND b.clndr_date = DATEADD(yy, -1, a.clndr_date)
	AND b.CalendarEndOfMonthDate = c.clndr_date
),
JNR_RunDate AS (SELECT
	SQ_RunDate.RunDateID, 
	SQ_RunDate.RunDate, 
	SQ_RunDate.PreviousYearRunDateID, 
	SQ_RunDate.PreviousYearRunDate, 
	SQ_RunDate.Dummy, 
	EXP_DefaultHandle3.o_EDWAgencyAKID AS EDWAgencyAKID, 
	EXP_DefaultHandle3.o_AgencyCode AS AgencyCode, 
	EXP_DefaultHandle3.o_LegalPrimaryAgencyCode AS LegalPrimaryAgencyCode, 
	EXP_DefaultHandle3.o_BondsDirectWrittenPremium AS BondsDirectWrittenPremium, 
	EXP_DefaultHandle3.o_NSIDirectWrittenPremium AS NSIDirectWrittenPremium, 
	EXP_DefaultHandle3.o_ProfitSharingEligibleDirectWrittenPremium AS ProfitSharingEligibleDirectWrittenPremium, 
	EXP_DefaultHandle3.o_RegularCommission AS RegularCommission, 
	EXP_DefaultHandle3.o_BondsDirectEarnedPremium AS BondsDirectEarnedPremium, 
	EXP_DefaultHandle3.o_NSIEarnedPremium AS NSIEarnedPremium, 
	EXP_DefaultHandle3.o_ProfitSharingEligibleDirectEarnedPremium AS ProfitSharingEligibleDirectEarnedPremium, 
	EXP_DefaultHandle3.o_BondsDirectIncurredLoss AS BondsDirectIncurredLoss, 
	EXP_DefaultHandle3.o_BondsDirectIncurredLossSameYear AS BondsDirectIncurredLossSameYear, 
	EXP_DefaultHandle3.o_ProfitSharingEligibleDirectIncurredLoss AS ProfitSharingEligibleDirectIncurredLoss, 
	EXP_DefaultHandle3.o_DividendAmount AS DividendAmount, 
	EXP_DefaultHandle3.o_Dummy_Records AS Dummy_Records
	FROM SQ_RunDate
	INNER JOIN EXP_DefaultHandle3
	ON EXP_DefaultHandle3.o_Dummy_Records = SQ_RunDate.Dummy
),
Exp_DataCollect AS (
	SELECT
	RunDateID,
	RunDate,
	PreviousYearRunDateID,
	PreviousYearRunDate,
	Dummy,
	EDWAgencyAKID,
	AgencyCode,
	LegalPrimaryAgencyCode,
	-- *INF*: :LKP.LKP_V3_AGENCYDIMID(EDWAgencyAKID,RunDate)
	LKP_V3_AGENCYDIMID_EDWAgencyAKID_RunDate.agency_dim_id AS AgencyDimId,
	-- *INF*: IIF(ISNULL(:LKP.LKP_SALESDIVISIONDIM(AgencyCode)),-1,:LKP.LKP_SALESDIVISIONDIM(AgencyCode))
	IFF(LKP_SALESDIVISIONDIM_AgencyCode.SalesDivisionDimID IS NULL, - 1, LKP_SALESDIVISIONDIM_AgencyCode.SalesDivisionDimID) AS SalesDivisionDimId,
	BondsDirectWrittenPremium,
	NSIDirectWrittenPremium,
	ProfitSharingEligibleDirectWrittenPremium,
	RegularCommission,
	BondsDirectEarnedPremium,
	NSIEarnedPremium,
	ProfitSharingEligibleDirectEarnedPremium,
	BondsDirectIncurredLoss,
	BondsDirectIncurredLossSameYear,
	ProfitSharingEligibleDirectIncurredLoss,
	DividendAmount,
	Dummy_Records
	FROM JNR_RunDate
	LEFT JOIN LKP_V3_AGENCYDIMID LKP_V3_AGENCYDIMID_EDWAgencyAKID_RunDate
	ON LKP_V3_AGENCYDIMID_EDWAgencyAKID_RunDate.edw_agency_ak_id = EDWAgencyAKID
	AND LKP_V3_AGENCYDIMID_EDWAgencyAKID_RunDate.eff_from_date = RunDate

	LEFT JOIN LKP_SALESDIVISIONDIM LKP_SALESDIVISIONDIM_AgencyCode
	ON LKP_SALESDIVISIONDIM_AgencyCode.AgencyCode = AgencyCode

),
LKP_Agency AS (
	SELECT
	ProfitSharingGuaranteeFlag,
	EDWAgencyAKID
	FROM (
		select 
		ad.EDWAgencyAKID as EDWAgencyAKID,  
		--ad.AgencyDimID as AgencyDimID,
		  case when a.ProfitSharingGuaranteeFlag=1 then '1' else '0' end as ProfitSharingGuaranteeFlag
		  from 
		  @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim ad, 
		  @{pipeline().parameters.EDW_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Agency a
		  where ad.EDWAgencyAKID=a.AgencyAKID
		 and a.CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWAgencyAKID ORDER BY ProfitSharingGuaranteeFlag DESC) = 1
),
LKP_LastYearAgencyProfitSharingYTDFact AS (
	SELECT
	BondsDirectWrittenPremium,
	EDWAgencyAKID,
	RunDateId
	FROM (
		Select A.EDWAgencyAKID AS EDWAgencyAKID ,
		RunDateId AS RunDateId,
		BondsDirectWrittenPremium  AS BondsDirectWrittenPremium
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyProfitSharingYTDFact af
		join @{pipeline().parameters.TARGET_TABLE_OWNER}.calendar_dim cd 
		on af.RunDateId=cd.clndr_id 
		Join @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim A
		on af.AgencyDimId=A.AgencyDimId
		and cd.clndr_yr=YEAR(dateadd(dd,-1,dateadd(mm,DATEDIFF(MM,0,GETDATE())-@{pipeline().parameters.NO_OF_MONTHS},0)))-1
		WHERE af.GroupExperienceIndicator = 'INDIVIDUAL'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWAgencyAKID,RunDateId ORDER BY BondsDirectWrittenPremium DESC) = 1
),
LKP_StopLossAdjustmentCatastropheFact AS (
	SELECT
	AgencyDimId_cat,
	StopLossAdjustmentAmount,
	RunDateId,
	LegalPrimaryAgencyCode
	FROM (
		select 
		(CASE WHEN ard.LegalPrimaryAgencyCode is null THEN agy.agencycode ELSE   ard.LegalPrimaryAgencyCode end) as  LegalPrimaryAgencyCode,
		RunDateId as RunDateId
		,agy.AgencyDimId as AgencyDimId_cat
		,SUM(ChangeInStopLossAdjustmentAmount) as StopLossAdjustmentAmount
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.StopLossAdjustmentCatastropheFact sacf
		join @{pipeline().parameters.TARGET_TABLE_OWNER}.calendar_dim cd
		on sacf.RunDateId=cd.clndr_id
		and cd.clndr_yr=YEAR(dateadd(dd,-1,dateadd(mm,DATEDIFF(MM,0,GETDATE())-@{pipeline().parameters.NO_OF_MONTHS},0)))
		join @{pipeline().parameters.TARGET_TABLE_OWNER_V3}.AgencyDim agy
		on agy.AgencyDimId=sacf.AgencyDimId
		
		left join 
		 (select * from Agencyrelationshipdim where currentsnapshotflag = 1) ard
		 on (Agy.edwagencyakid=ard.edwagencyakid 
		 and cd.clndr_date between ard.agencyrelationshipeffectivedate and ard.agencyrelationshipexpirationdate)
		
		
		group by 
		--agy.LegalPrimaryAgencyCode
		RunDateId
		,agy.AgencyDimId,
		CASE WHEN ard.LegalPrimaryAgencyCode is null THEN agy.agencycode ELSE   ard.LegalPrimaryAgencyCode end
		
		
		
		--Left join with "agencyrelationshipdim" in order to get legalprimaryagencycode values from --"AgencyRelationshipDim" table
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RunDateId,LegalPrimaryAgencyCode ORDER BY AgencyDimId_cat DESC) = 1
),
LKP_StopLossAdjustmentClaimOccurrenceFact AS (
	SELECT
	StopLossAdjustmentAmount,
	RunDateId,
	EDWAgencyAKID
	FROM (
		select 
		agy.EDWAgencyAKID as EDWAgencyAKID
		,RunDateId as RunDateId
		,SUM(ChangeInStopLossAdjustmentAmount) as StopLossAdjustmentAmount
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.StopLossAdjustmentClaimOccurrenceFact saof
		join @{pipeline().parameters.TARGET_TABLE_OWNER}.calendar_dim cd
		on saof.RunDateId=cd.clndr_id
		and cd.clndr_yr=YEAR(dateadd(dd,-1,dateadd(mm,DATEDIFF(MM,0,GETDATE())-@{pipeline().parameters.NO_OF_MONTHS},0)))
		join @{pipeline().parameters.TARGET_TABLE_OWNER_V3}.AgencyDim agy
		on agy.AgencyDimId=saof.AgencyDimId
		group by 
		agy.EDWAgencyAKID
		,RunDateId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RunDateId,EDWAgencyAKID ORDER BY StopLossAdjustmentAmount DESC) = 1
),
LKP_V3_AgencyDimID_PrimaryAgency AS (
	SELECT
	LegalPrimaryAgencyCode,
	AgencyCode,
	eff_from_date,
	eff_to_date
	FROM (
		SELECT 
		ARD.LegalPrimaryAgencyCode as LegalPrimaryAgencyCode, 
		AgencyDim.AgencyCode as AgencyCode, 
		ARD.AgencyRelationshipEffectivedate as eff_from_date, 
		ARD.AgencyRelationshipExpirationdate as eff_to_date
		 FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V3}.AgencyDim as AgencyDim
		left join 
		 (select * from @{pipeline().parameters.TARGET_TABLE_OWNER}.Agencyrelationshipdim where currentsnapshotflag = 1) ard
		 on (AgencyDim.edwagencyakid=ard.edwagencyakid )
		
		
		
		
		--Left join with "agencyrelationshipdim" in order to get legalprimaryagencycode values from --"AgencyRelationshipDim" table
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyCode,eff_from_date,eff_to_date ORDER BY LegalPrimaryAgencyCode DESC) = 1
),
EXP_DataCollect_Individual_Group AS (
	SELECT
	Exp_DataCollect.RunDateID,
	Exp_DataCollect.PreviousYearRunDateID AS LastYearRunDateID,
	Exp_DataCollect.AgencyDimId AS AgencyDimID,
	Exp_DataCollect.SalesDivisionDimId,
	Exp_DataCollect.BondsDirectWrittenPremium,
	Exp_DataCollect.NSIDirectWrittenPremium,
	Exp_DataCollect.ProfitSharingEligibleDirectWrittenPremium,
	Exp_DataCollect.RegularCommission,
	Exp_DataCollect.BondsDirectEarnedPremium,
	Exp_DataCollect.NSIEarnedPremium,
	Exp_DataCollect.ProfitSharingEligibleDirectEarnedPremium,
	Exp_DataCollect.BondsDirectIncurredLoss,
	Exp_DataCollect.BondsDirectIncurredLossSameYear,
	Exp_DataCollect.ProfitSharingEligibleDirectIncurredLoss,
	Exp_DataCollect.DividendAmount,
	LKP_LastYearAgencyProfitSharingYTDFact.BondsDirectWrittenPremium AS lkp_LastYearYTDBondsDirectWrittenPremium,
	LKP_StopLossAdjustmentClaimOccurrenceFact.StopLossAdjustmentAmount AS lkp_StopLossAdjustmentAmountOccurrence,
	LKP_StopLossAdjustmentCatastropheFact.StopLossAdjustmentAmount AS lkp_StopLossAdjustmentAmountCatastrophe,
	LKP_StopLossAdjustmentCatastropheFact.AgencyDimId_cat,
	LKP_Agency.ProfitSharingGuaranteeFlag AS lkp_ProfitSharingGuaranteeFlag,
	Exp_DataCollect.EDWAgencyAKID,
	Exp_DataCollect.AgencyCode,
	LKP_V3_AgencyDimID_PrimaryAgency.LegalPrimaryAgencyCode,
	-- *INF*: iif(isnull(LegalPrimaryAgencyCode),AgencyCode,LegalPrimaryAgencyCode)
	IFF(LegalPrimaryAgencyCode IS NULL, AgencyCode, LegalPrimaryAgencyCode) AS O_LegalPrimaryAgencyCode,
	Exp_DataCollect.RunDate
	FROM Exp_DataCollect
	LEFT JOIN LKP_Agency
	ON LKP_Agency.EDWAgencyAKID = Exp_DataCollect.EDWAgencyAKID
	LEFT JOIN LKP_LastYearAgencyProfitSharingYTDFact
	ON LKP_LastYearAgencyProfitSharingYTDFact.EDWAgencyAKID = JNR_RunDate.EDWAgencyAKID AND LKP_LastYearAgencyProfitSharingYTDFact.RunDateId = JNR_RunDate.PreviousYearRunDateID
	LEFT JOIN LKP_StopLossAdjustmentCatastropheFact
	ON LKP_StopLossAdjustmentCatastropheFact.RunDateId = Exp_DataCollect.RunDateID AND LKP_StopLossAdjustmentCatastropheFact.LegalPrimaryAgencyCode = Exp_DataCollect.LegalPrimaryAgencyCode
	LEFT JOIN LKP_StopLossAdjustmentClaimOccurrenceFact
	ON LKP_StopLossAdjustmentClaimOccurrenceFact.RunDateId = Exp_DataCollect.RunDateID AND LKP_StopLossAdjustmentClaimOccurrenceFact.EDWAgencyAKID = Exp_DataCollect.EDWAgencyAKID
	LEFT JOIN LKP_V3_AgencyDimID_PrimaryAgency
	ON LKP_V3_AgencyDimID_PrimaryAgency.AgencyCode = Exp_DataCollect.AgencyCode AND LKP_V3_AgencyDimID_PrimaryAgency.eff_from_date <= Exp_DataCollect.RunDate AND LKP_V3_AgencyDimID_PrimaryAgency.eff_to_date >= Exp_DataCollect.RunDate
),
AGG_Data_PrimaryAgency AS (
	SELECT
	O_LegalPrimaryAgencyCode AS LegalPrimaryAgencyCode, 
	RunDateID, 
	LastYearRunDateID, 
	BondsDirectWrittenPremium, 
	SUM(BondsDirectWrittenPremium) AS O_BondsDirectWrittenPremium, 
	NSIDirectWrittenPremium, 
	SUM(NSIDirectWrittenPremium) AS O_NSIDirectWrittenPremium, 
	ProfitSharingEligibleDirectWrittenPremium, 
	SUM(ProfitSharingEligibleDirectWrittenPremium) AS O_ProfitSharingEligibleDirectWrittenPremium, 
	RegularCommission, 
	SUM(RegularCommission) AS O_RegularCommission, 
	BondsDirectEarnedPremium, 
	SUM(BondsDirectEarnedPremium) AS O_BondsDirectEarnedPremium, 
	NSIEarnedPremium, 
	SUM(NSIEarnedPremium) AS O_NSIEarnedPremium, 
	ProfitSharingEligibleDirectEarnedPremium, 
	SUM(ProfitSharingEligibleDirectEarnedPremium) AS O_ProfitSharingEligibleDirectEarnedPremium, 
	BondsDirectIncurredLoss, 
	SUM(BondsDirectIncurredLoss) AS O_BondsDirectIncurredLoss, 
	BondsDirectIncurredLossSameYear, 
	SUM(BondsDirectIncurredLossSameYear) AS O_BondsDirectIncurredLossSameYear, 
	ProfitSharingEligibleDirectIncurredLoss, 
	SUM(ProfitSharingEligibleDirectIncurredLoss) AS O_ProfitSharingEligibleDirectIncurredLoss, 
	DividendAmount, 
	SUM(DividendAmount) AS O_DividendAmount, 
	lkp_LastYearYTDBondsDirectWrittenPremium AS LastYearYTDBondsDirectWrittenPremium, 
	SUM(LastYearYTDBondsDirectWrittenPremium) AS O_LastYearYTDBondsDirectWrittenPremium, 
	lkp_StopLossAdjustmentAmountOccurrence AS StopLossAdjustmentAmountOccurrence, 
	SUM(StopLossAdjustmentAmountOccurrence) AS O_StopLossAdjustmentAmountOccurrence, 
	lkp_StopLossAdjustmentAmountCatastrophe AS StopLossAdjustmentAmountCatastrophe, 
	StopLossAdjustmentAmountCatastrophe AS O_StopLossAdjustmentAmountCatastrophe, 
	lkp_ProfitSharingGuaranteeFlag AS ProfitSharingGuaranteeFlag, 
	RunDate
	FROM EXP_DataCollect_Individual_Group
	GROUP BY LegalPrimaryAgencyCode
),
EXP_Data_PrimaryAgency AS (
	SELECT
	LegalPrimaryAgencyCode,
	RunDate,
	-- *INF*: :LKP.LKP_V3_AGENCYDIMID_GROUP(LegalPrimaryAgencyCode,RunDate)
	LKP_V3_AGENCYDIMID_GROUP_LegalPrimaryAgencyCode_RunDate.agency_dim_id AS AgencyDimId,
	-- *INF*: IIF(ISNULL(:LKP.LKP_SALESDIVISIONDIM(LegalPrimaryAgencyCode)),-1,:LKP.LKP_SALESDIVISIONDIM(LegalPrimaryAgencyCode))
	IFF(LKP_SALESDIVISIONDIM_LegalPrimaryAgencyCode.SalesDivisionDimID IS NULL, - 1, LKP_SALESDIVISIONDIM_LegalPrimaryAgencyCode.SalesDivisionDimID) AS SalesDivisionDimId,
	RunDateID,
	LastYearRunDateID,
	O_BondsDirectWrittenPremium,
	O_NSIDirectWrittenPremium,
	O_ProfitSharingEligibleDirectWrittenPremium,
	O_RegularCommission,
	O_BondsDirectEarnedPremium,
	O_NSIEarnedPremium,
	O_ProfitSharingEligibleDirectEarnedPremium,
	O_BondsDirectIncurredLoss,
	O_BondsDirectIncurredLossSameYear,
	O_ProfitSharingEligibleDirectIncurredLoss,
	O_DividendAmount,
	O_LastYearYTDBondsDirectWrittenPremium,
	O_StopLossAdjustmentAmountOccurrence,
	O_StopLossAdjustmentAmountCatastrophe,
	ProfitSharingGuaranteeFlag
	FROM AGG_Data_PrimaryAgency
	LEFT JOIN LKP_V3_AGENCYDIMID_GROUP LKP_V3_AGENCYDIMID_GROUP_LegalPrimaryAgencyCode_RunDate
	ON LKP_V3_AGENCYDIMID_GROUP_LegalPrimaryAgencyCode_RunDate.AgencyCode = LegalPrimaryAgencyCode
	AND LKP_V3_AGENCYDIMID_GROUP_LegalPrimaryAgencyCode_RunDate.eff_from_date = RunDate

	LEFT JOIN LKP_SALESDIVISIONDIM LKP_SALESDIVISIONDIM_LegalPrimaryAgencyCode
	ON LKP_SALESDIVISIONDIM_LegalPrimaryAgencyCode.AgencyCode = LegalPrimaryAgencyCode

),
LKP_AgencyProfitSharing_GuaranteedCommission AS (
	SELECT
	NetProfitSharingAmount,
	GuaranteeFee,
	IN_LegalPrimaryAgencyCode,
	LegalPrimaryAgencyCode
	FROM (
		SELECT
		C.AgencyCode  as LegalPrimaryAgencyCode, 
		A.NetProfitSharingAmount AS NetProfitSharingAmount,
		A.GuaranteeFee as GuaranteeFee 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyProfitSharingYTDFact A 
		INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.calendar_dim B on (A.RunDateId=B.clndr_id)
		INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER_V3}.AgencyDim C on (A.AgencyDimId=C.AgencyDimID)
		WHERE A.GroupExperienceIndicator = 'GROUP' AND B.clndr_month=9 AND
		YEAR(DATEADD(dd,-1,DATEADD(mm,DATEDIFF(MM,0,GETDATE())-@{pipeline().parameters.NO_OF_MONTHS},0)))=B.clndr_yr
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY LegalPrimaryAgencyCode ORDER BY NetProfitSharingAmount DESC) = 1
),
LKP_Agency_Primary_Profit_SharingFlag AS (
	SELECT
	AgencyAppointedDate,
	ProfitSharingGuaranteeFlag,
	AgencyDimId
	FROM (
		select 
		ad.AgencyDimID as AgencyDimID,
		ad.AgencyAppointedDate as AgencyAppointedDate,
		  case when a.ProfitSharingGuaranteeFlag=1 then '1' else '0' end as ProfitSharingGuaranteeFlag
		  from 
		  @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim ad, 
		  @{pipeline().parameters.EDW_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Agency a
		  where ad.EDWAgencyAKID=a.AgencyAKID
		 and a.CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyDimId ORDER BY AgencyAppointedDate DESC) = 1
),
LKP_LastYearAgencyProfitSharingYTDFact_GROUP AS (
	SELECT
	BondsDirectWrittenPremium,
	AgencyCode,
	RunDateId
	FROM (
		SELECT
		      PRIM.AgencyCode AS AgencyCode
		      ,RunDateId AS RunDateId
		      ,SUM(BondsDirectWrittenPremium) AS BondsDirectWrittenPremium
		
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyProfitSharingYTDFact af
		
		INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.calendar_dim cd
		      ON af.RunDateId = cd.clndr_id
		
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim BRNCH
		      ON af.AgencyDimId = BRNCH.AgencyDimId
		
		INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyRelationshipCurrent ARC
		      ON (BRNCH.EDWAgencyAKID = ARC.EDWAgencyAKID
		                   AND DATEADD(dd, -1, DATEADD(mm, DATEDIFF(MM, 0, GETDATE()) - @{pipeline().parameters.NO_OF_MONTHS}, 0)) 
		                   BETWEEN ARC.AgencyRelationshipEffectiveDate
		                   AND ARC.AgencyRelationshipExpirationDate)
		
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim PRIM
		      ON (PRIM.EDWAgencyAKID = ARC.EDWLegalPrimaryAgencyAKId
		                   AND PRIM.CurrentSnapshotFlag = 1)
		
		WHERE af.GroupExperienceIndicator = 'GROUP'
		AND cd.clndr_date = DATEADD(dd, -1, DATEADD(mm, DATEDIFF(MM, 0, GETDATE()) - 12 - @{pipeline().parameters.NO_OF_MONTHS}, 0))
		
		GROUP BY
		                PRIM.AgencyCode,RunDateId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyCode,RunDateId ORDER BY BondsDirectWrittenPremium DESC) = 1
),
EXP_CalValues_PrimaryAgency AS (
	SELECT
	EXP_Data_PrimaryAgency.AgencyDimId AS i_AgencyDimId,
	EXP_Data_PrimaryAgency.RunDateID AS i_RunDateID,
	EXP_Data_PrimaryAgency.RunDate,
	-- *INF*: GET_DATE_PART(RunDate,'MM')
	GET_DATE_PART(RunDate, 'MM') AS v_RunMonth,
	EXP_Data_PrimaryAgency.SalesDivisionDimId AS i_SalesDivisionDimId,
	EXP_Data_PrimaryAgency.O_BondsDirectWrittenPremium AS i_BondsDirectWrittenPremium,
	EXP_Data_PrimaryAgency.O_NSIDirectWrittenPremium AS i_NSIDirectWrittenPremium,
	EXP_Data_PrimaryAgency.O_ProfitSharingEligibleDirectWrittenPremium AS i_ProfitSharingEligibleDirectWrittenPremium,
	EXP_Data_PrimaryAgency.O_RegularCommission AS i_RegularCommission,
	EXP_Data_PrimaryAgency.O_BondsDirectEarnedPremium AS i_BondsDirectEarnedPremium,
	EXP_Data_PrimaryAgency.O_NSIEarnedPremium AS i_NSIEarnedPremium,
	EXP_Data_PrimaryAgency.O_ProfitSharingEligibleDirectEarnedPremium AS i_ProfitSharingEligibleDirectEarnedPremium,
	EXP_Data_PrimaryAgency.O_BondsDirectIncurredLoss AS i_BondsDirectIncurredLoss,
	EXP_Data_PrimaryAgency.O_ProfitSharingEligibleDirectIncurredLoss AS i_ProfitSharingEligibleDirectIncurredLoss,
	EXP_Data_PrimaryAgency.O_DividendAmount AS i_DividendAmount,
	LKP_LastYearAgencyProfitSharingYTDFact_GROUP.BondsDirectWrittenPremium AS lkp_LastYearYTDBondsDirectWrittenPremium,
	EXP_Data_PrimaryAgency.O_StopLossAdjustmentAmountOccurrence AS lkp_StopLossAdjustmentAmountOccurrence,
	EXP_Data_PrimaryAgency.O_StopLossAdjustmentAmountCatastrophe AS lkp_StopLossAdjustmentAmountCatastrophe,
	LKP_Agency_Primary_Profit_SharingFlag.AgencyAppointedDate,
	-- *INF*: DATE_DIFF(RunDate,AgencyAppointedDate,'YY')
	DATE_DIFF(RunDate, AgencyAppointedDate, 'YY') AS v_WB_AgencyYears,
	LKP_Agency_Primary_Profit_SharingFlag.ProfitSharingGuaranteeFlag AS lkp_ProfitSharingGuaranteeFlag,
	LKP_AgencyProfitSharing_GuaranteedCommission.NetProfitSharingAmount AS i_GuaranteedCommission,
	LKP_AgencyProfitSharing_GuaranteedCommission.IN_LegalPrimaryAgencyCode AS in_LegalPrimaryAgencyCode,
	-- *INF*: SUBSTR(in_LegalPrimaryAgencyCode,1,2)
	SUBSTR(in_LegalPrimaryAgencyCode, 1, 2) AS v_NSI_Agencies,
	LKP_AgencyProfitSharing_GuaranteedCommission.GuaranteeFee AS i_GuaranteeFee_Sep,
	-- *INF*: ROUND(i_BondsDirectWrittenPremium-lkp_LastYearYTDBondsDirectWrittenPremium,4)
	ROUND(i_BondsDirectWrittenPremium - lkp_LastYearYTDBondsDirectWrittenPremium, 4) AS v_BondsGrowthAmount,
	-- *INF*: DECODE(TRUE,
	-- v_BondsGrowthAmount<25000.00,
	-- 0.00,
	-- v_BondsGrowthAmount>=25000.00 AND v_BondsGrowthAmount<50001.00,
	-- 1.0,
	-- v_BondsGrowthAmount>=50001.00 AND v_BondsGrowthAmount<100001.00,
	-- 2.0,
	-- v_BondsGrowthAmount>=100001.00 AND v_BondsGrowthAmount<150001.00,
	-- 3.0,
	-- v_BondsGrowthAmount>=150001.00,
	-- 4.0)
	-- 
	DECODE(TRUE,
	v_BondsGrowthAmount < 25000.00, 0.00,
	v_BondsGrowthAmount >= 25000.00 AND v_BondsGrowthAmount < 50001.00, 1.0,
	v_BondsGrowthAmount >= 50001.00 AND v_BondsGrowthAmount < 100001.00, 2.0,
	v_BondsGrowthAmount >= 100001.00 AND v_BondsGrowthAmount < 150001.00, 3.0,
	v_BondsGrowthAmount >= 150001.00, 4.0) AS v_BondsGrowthBonusRate,
	-- *INF*: lkp_ProfitSharingGuaranteeFlag
	-- 
	-- --DECODE(lkp_ProfitSharingGuaranteeFlag,'T','1','F','0','')
	lkp_ProfitSharingGuaranteeFlag AS v_ProfitSharingGuaranteeFlag,
	-- *INF*: DECODE(TRUE,i_BondsDirectEarnedPremium<=0,100,i_BondsDirectIncurredLoss <=0,0,ROUND(i_BondsDirectIncurredLoss/ i_BondsDirectEarnedPremium  * 100,4))
	DECODE(TRUE,
	i_BondsDirectEarnedPremium <= 0, 100,
	i_BondsDirectIncurredLoss <= 0, 0,
	ROUND(i_BondsDirectIncurredLoss / i_BondsDirectEarnedPremium * 100, 4)) AS v_BondsLossRatio,
	-- *INF*: DECODE(TRUE,
	-- v_BondsLossRatio<5.1,
	-- 4.0,
	-- v_BondsLossRatio>=5.1 AND v_BondsLossRatio<11.1,
	-- 3.0,
	-- v_BondsLossRatio>=11.1 AND v_BondsLossRatio<15.1,
	-- 2.0,
	-- v_BondsLossRatio>=15.1 AND v_BondsLossRatio<20.1,
	-- 1.0,
	-- 0)
	DECODE(TRUE,
	v_BondsLossRatio < 5.1, 4.0,
	v_BondsLossRatio >= 5.1 AND v_BondsLossRatio < 11.1, 3.0,
	v_BondsLossRatio >= 11.1 AND v_BondsLossRatio < 15.1, 2.0,
	v_BondsLossRatio >= 15.1 AND v_BondsLossRatio < 20.1, 1.0,
	0) AS v_BondsLossRatioBonusRate,
	-- *INF*: IIF(i_NSIDirectWrittenPremium<100000,ROUND(i_NSIDirectWrittenPremium*.1,4),0)
	IFF(i_NSIDirectWrittenPremium < 100000, ROUND(i_NSIDirectWrittenPremium * .1, 4), 0) AS v_NSIExpense,
	-- *INF*: ROUND(i_ProfitSharingEligibleDirectEarnedPremium-i_DividendAmount,4)
	ROUND(i_ProfitSharingEligibleDirectEarnedPremium - i_DividendAmount, 4) AS v_NetDirectEarnedPremium,
	-- *INF*: ROUND(i_ProfitSharingEligibleDirectIncurredLoss-lkp_StopLossAdjustmentAmountOccurrence-lkp_StopLossAdjustmentAmountCatastrophe+0.00,4)
	-- 
	-- --had v_NSIExpense value were 0.00 was present as it was removed for PROD-8913 issue 4
	ROUND(i_ProfitSharingEligibleDirectIncurredLoss - lkp_StopLossAdjustmentAmountOccurrence - lkp_StopLossAdjustmentAmountCatastrophe + 0.00, 4) AS v_NetDirectIncurredLoss,
	-- *INF*: DECODE(TRUE,v_NetDirectEarnedPremium<=0,100,v_NetDirectIncurredLoss<=0,0,ROUND(v_NetDirectIncurredLoss/v_NetDirectEarnedPremium*100,4))
	DECODE(TRUE,
	v_NetDirectEarnedPremium <= 0, 100,
	v_NetDirectIncurredLoss <= 0, 0,
	ROUND(v_NetDirectIncurredLoss / v_NetDirectEarnedPremium * 100, 4)) AS v_NetLossRatio,
	-- *INF*: :LKP.LKP_SUPPROFTSHARINGBONUS(TO_INTEGER(ROUND(i_ProfitSharingEligibleDirectWrittenPremium)),ROUND(v_NetLossRatio,1))
	LKP_SUPPROFTSHARINGBONUS_TO_INTEGER_ROUND_i_ProfitSharingEligibleDirectWrittenPremium_ROUND_v_NetLossRatio_1.ProfitSharingBonusRate AS v_lkpProfitSharingBonusRate,
	-- *INF*: :LKP.LKP_AGENCYBONUSRATES(in_LegalPrimaryAgencyCode)
	LKP_AGENCYBONUSRATES_in_LegalPrimaryAgencyCode.BonusRate AS v_lkpProfitSharingBonusRateOverride,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(v_lkpProfitSharingBonusRate),0,
	-- v_WB_AgencyYears<=2 and i_ProfitSharingEligibleDirectWrittenPremium>=100000,v_lkpProfitSharingBonusRate,
	-- v_WB_AgencyYears > 2 and i_ProfitSharingEligibleDirectWrittenPremium>=250000,v_lkpProfitSharingBonusRate,0)
	DECODE(TRUE,
	v_lkpProfitSharingBonusRate IS NULL, 0,
	v_WB_AgencyYears <= 2 AND i_ProfitSharingEligibleDirectWrittenPremium >= 100000, v_lkpProfitSharingBonusRate,
	v_WB_AgencyYears > 2 AND i_ProfitSharingEligibleDirectWrittenPremium >= 250000, v_lkpProfitSharingBonusRate,
	0) AS v_ProfitSharingBonusRate,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(v_lkpProfitSharingBonusRateOverride),
	-- v_ProfitSharingBonusRate,
	-- v_lkpProfitSharingBonusRateOverride)
	-- -- use Agency Bonus Rate override if it exists else default to calculated Bonus Rate
	DECODE(TRUE,
	v_lkpProfitSharingBonusRateOverride IS NULL, v_ProfitSharingBonusRate,
	v_lkpProfitSharingBonusRateOverride) AS v_ProfitSharingBonusRateFinal,
	-- *INF*: DECODE(TRUE,
	--  NOT ISNULL(v_lkpProfitSharingBonusRateOverride),'0',
	-- v_ProfitSharingGuaranteeFlag)
	-- --No Guarantee Fee applies for Agency Bonus Rate overrides
	DECODE(TRUE,
	NOT v_lkpProfitSharingBonusRateOverride IS NULL, '0',
	v_ProfitSharingGuaranteeFlag) AS v_ProfitSharingGuaranteeFlagFinal,
	-- *INF*: ROUND((v_NetDirectEarnedPremium * v_ProfitSharingBonusRateFinal)/100,4)
	-- 
	-- --v_ProfitSharingBonusRate)/100,4)
	-- -- Modified to use override rate if it exists else use computed rate
	-- -- Made change as part of fix for EDWP-1812, as BonusRate is %, we need divide by 100.
	ROUND(( v_NetDirectEarnedPremium * v_ProfitSharingBonusRateFinal ) / 100, 4) AS v_ProfitSharingCommission,
	-- *INF*: IIF(IN(v_RunMonth,'10','11','12'),i_GuaranteeFee_Sep,IIF(v_ProfitSharingGuaranteeFlagFinal='1',ROUND(v_ProfitSharingCommission * .15,4),0))
	-- 
	-- --modified to check for guarantee flag final to allow for bonus rate overrides 
	IFF(IN(v_RunMonth, '10', '11', '12'), i_GuaranteeFee_Sep, IFF(v_ProfitSharingGuaranteeFlagFinal = '1', ROUND(v_ProfitSharingCommission * .15, 4), 0)) AS v_GuaranteeFee,
	-- *INF*: ROUND(v_ProfitSharingCommission-v_GuaranteeFee,4)
	ROUND(v_ProfitSharingCommission - v_GuaranteeFee, 4) AS v_NetProfitSharingAmount,
	-- *INF*: IIF(IN(v_RunMonth,'10','11','12'),IIF(i_GuaranteedCommission>v_NetProfitSharingAmount,i_GuaranteedCommission,v_NetProfitSharingAmount),v_NetProfitSharingAmount)
	IFF(IN(v_RunMonth, '10', '11', '12'), IFF(i_GuaranteedCommission > v_NetProfitSharingAmount, i_GuaranteedCommission, v_NetProfitSharingAmount), v_NetProfitSharingAmount) AS v_ProfitSharingPaymentAmount,
	EXP_Data_PrimaryAgency.O_BondsDirectIncurredLossSameYear AS i_BondsDirectIncurredLossSameYear,
	-- *INF*: DECODE(TRUE,i_BondsDirectEarnedPremium<=0,100,i_BondsDirectIncurredLossSameYear <=0,0,ROUND(i_BondsDirectIncurredLossSameYear/ i_BondsDirectEarnedPremium  * 100,4))
	DECODE(TRUE,
	i_BondsDirectEarnedPremium <= 0, 100,
	i_BondsDirectIncurredLossSameYear <= 0, 0,
	ROUND(i_BondsDirectIncurredLossSameYear / i_BondsDirectEarnedPremium * 100, 4)) AS v_BondsGrowthLossRatio,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	i_SalesDivisionDimId AS o_SalesDivisionDimId,
	i_AgencyDimId AS o_AgencyDimID,
	i_RunDateID AS o_RunDateID,
	i_BondsDirectWrittenPremium AS o_BondsDirectWrittenPremium,
	i_NSIDirectWrittenPremium AS o_NSIDirectWrittenPremium,
	i_ProfitSharingEligibleDirectWrittenPremium AS o_ProfitSharingEligibleDirectWrittenPremium,
	i_BondsDirectEarnedPremium AS o_BondsDirectEarnedPremium,
	i_NSIEarnedPremium AS o_NSIEarnedPremium,
	i_ProfitSharingEligibleDirectEarnedPremium AS o_ProfitSharingEligibleDirectEarnedPremium,
	i_BondsDirectIncurredLoss AS o_BondsDirectIncurredLoss,
	i_ProfitSharingEligibleDirectIncurredLoss AS o_ProfitSharingEligibleDirectIncurredLoss,
	v_BondsGrowthAmount AS o_BondsGrowthAmount,
	v_BondsGrowthBonusRate AS o_BondsGrowthBonusRate,
	-- *INF*: ROUND(i_BondsDirectWrittenPremium*v_BondsGrowthBonusRate/100,4)
	ROUND(i_BondsDirectWrittenPremium * v_BondsGrowthBonusRate / 100, 4) AS o_BondsGrowthBonusAmount,
	v_BondsLossRatio AS o_BondsLossRatio,
	v_BondsLossRatioBonusRate AS o_BondsLossRatioBonusRate,
	-- *INF*: ROUND(v_BondsLossRatioBonusRate*i_BondsDirectEarnedPremium/100,4)
	ROUND(v_BondsLossRatioBonusRate * i_BondsDirectEarnedPremium / 100, 4) AS o_BondsLossRatioBonusAmount,
	i_RegularCommission AS o_RegularCommission,
	i_DividendAmount AS o_DividendAmount,
	-- *INF*: 0
	-- --v_NSIExpense removed this variable condition for PROD-8913 issue #4 and defaulting the value to 0
	0 AS o_NSIExpense,
	lkp_StopLossAdjustmentAmountOccurrence AS o_StopLossAdjustmentClaimOccurrenceAmount,
	lkp_StopLossAdjustmentAmountCatastrophe AS o_StopLossAdjustmentCatastropheAmount,
	v_NetDirectEarnedPremium AS o_NetDirectEarnedPremium,
	v_NetDirectIncurredLoss AS o_NetDirectIncurredLoss,
	v_NetLossRatio AS o_NetLossRatio,
	v_ProfitSharingBonusRateFinal AS o_ProfitSharingBonusRate,
	v_ProfitSharingCommission AS o_ProfitSharingCommission,
	-- *INF*: IIF(isnull(v_GuaranteeFee),0,v_GuaranteeFee)
	IFF(v_GuaranteeFee IS NULL, 0, v_GuaranteeFee) AS o_GuaranteeFee,
	-- *INF*: IIF(isnull(v_NetProfitSharingAmount),0,v_NetProfitSharingAmount)
	IFF(v_NetProfitSharingAmount IS NULL, 0, v_NetProfitSharingAmount) AS o_NetProfitSharingAmount,
	-- *INF*: IIF(isnull(v_ProfitSharingPaymentAmount),0,v_ProfitSharingPaymentAmount)
	IFF(v_ProfitSharingPaymentAmount IS NULL, 0, v_ProfitSharingPaymentAmount) AS o_ProfitSharingPaymentAmount,
	v_ProfitSharingGuaranteeFlagFinal AS o_ProfitSharingGuaranteeFlag,
	'GROUP' AS GroupExperienceIndicator,
	i_BondsDirectIncurredLossSameYear AS o_BondsDirectIncurredLossSameYear,
	v_BondsGrowthLossRatio AS o_BondsGrowthLossRatio
	FROM EXP_Data_PrimaryAgency
	LEFT JOIN LKP_AgencyProfitSharing_GuaranteedCommission
	ON LKP_AgencyProfitSharing_GuaranteedCommission.LegalPrimaryAgencyCode = EXP_Data_PrimaryAgency.LegalPrimaryAgencyCode
	LEFT JOIN LKP_Agency_Primary_Profit_SharingFlag
	ON LKP_Agency_Primary_Profit_SharingFlag.AgencyDimId = EXP_Data_PrimaryAgency.AgencyDimId
	LEFT JOIN LKP_LastYearAgencyProfitSharingYTDFact_GROUP
	ON LKP_LastYearAgencyProfitSharingYTDFact_GROUP.AgencyCode = EXP_Data_PrimaryAgency.LegalPrimaryAgencyCode AND LKP_LastYearAgencyProfitSharingYTDFact_GROUP.RunDateId = EXP_Data_PrimaryAgency.LastYearRunDateID
	LEFT JOIN LKP_SUPPROFTSHARINGBONUS LKP_SUPPROFTSHARINGBONUS_TO_INTEGER_ROUND_i_ProfitSharingEligibleDirectWrittenPremium_ROUND_v_NetLossRatio_1
	ON LKP_SUPPROFTSHARINGBONUS_TO_INTEGER_ROUND_i_ProfitSharingEligibleDirectWrittenPremium_ROUND_v_NetLossRatio_1.DirectWrittenPremiumLow = TO_INTEGER(ROUND(i_ProfitSharingEligibleDirectWrittenPremium))
	AND LKP_SUPPROFTSHARINGBONUS_TO_INTEGER_ROUND_i_ProfitSharingEligibleDirectWrittenPremium_ROUND_v_NetLossRatio_1.DirectWrittenPremiumHigh = ROUND(v_NetLossRatio, 1)

	LEFT JOIN LKP_AGENCYBONUSRATES LKP_AGENCYBONUSRATES_in_LegalPrimaryAgencyCode
	ON LKP_AGENCYBONUSRATES_in_LegalPrimaryAgencyCode.Agency = in_LegalPrimaryAgencyCode

),
LKP_AgencyProfitSharingYTDFact_PrimaryAgency AS (
	SELECT
	AgencyProfitSharingYTDFactId,
	SalesDivisionDimID,
	AgencyDimId,
	RunDateId
	FROM (
		select AgencyDimId AS AgencyDimId,
		SalesDivisionDimID AS SalesDivisionDimID,
		RunDateId AS RunDateId,
		AgencyProfitSharingYTDFactId  AS AgencyProfitSharingYTDFactId
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyProfitSharingYTDFact af
		join @{pipeline().parameters.TARGET_TABLE_OWNER}.calendar_dim cd 
		on af.RunDateId=cd.clndr_id 
		and cd.clndr_yr=YEAR(dateadd(dd,-1,dateadd(mm,DATEDIFF(MM,0,GETDATE())-@{pipeline().parameters.NO_OF_MONTHS},0)))
		and af.GroupExperienceIndicator='GROUP'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SalesDivisionDimID,AgencyDimId,RunDateId ORDER BY AgencyProfitSharingYTDFactId DESC) = 1
),
RTR_Insert_Update_PrimaryAgency AS (
	SELECT
	LKP_AgencyProfitSharingYTDFact_PrimaryAgency.AgencyProfitSharingYTDFactId,
	EXP_CalValues_PrimaryAgency.o_AuditId AS AuditId,
	EXP_CalValues_PrimaryAgency.o_SalesDivisionDimId AS SalesDivisionDimID,
	EXP_CalValues_PrimaryAgency.o_AgencyDimID AS AgencyDimId,
	EXP_CalValues_PrimaryAgency.o_RunDateID AS RunDateId,
	EXP_CalValues_PrimaryAgency.o_BondsDirectWrittenPremium AS BondsDirectWrittenPremium,
	EXP_CalValues_PrimaryAgency.o_NSIDirectWrittenPremium AS NSIDirectWrittenPremium,
	EXP_CalValues_PrimaryAgency.o_ProfitSharingEligibleDirectWrittenPremium AS ProfitSharingEligibleDirectWrittenPremium,
	EXP_CalValues_PrimaryAgency.o_BondsDirectEarnedPremium AS BondsDirectEarnedPremium,
	EXP_CalValues_PrimaryAgency.o_NSIEarnedPremium AS NSIEarnedPremium,
	EXP_CalValues_PrimaryAgency.o_ProfitSharingEligibleDirectEarnedPremium AS ProfitSharingEligibleDirectEarnedPremium,
	EXP_CalValues_PrimaryAgency.o_BondsDirectIncurredLoss AS BondsDirectIncurredLoss,
	EXP_CalValues_PrimaryAgency.o_ProfitSharingEligibleDirectIncurredLoss AS ProfitSharingEligibleDirectIncurredLoss,
	EXP_CalValues_PrimaryAgency.o_BondsGrowthAmount AS BondsGrowthAmount,
	EXP_CalValues_PrimaryAgency.o_BondsGrowthBonusRate AS BondsGrowthBonusRate,
	EXP_CalValues_PrimaryAgency.o_BondsGrowthBonusAmount AS BondsGrowthBonusAmount,
	EXP_CalValues_PrimaryAgency.o_BondsLossRatio AS BondsLossRatio,
	EXP_CalValues_PrimaryAgency.o_BondsLossRatioBonusRate AS BondsLossRatioBonusRate,
	EXP_CalValues_PrimaryAgency.o_BondsLossRatioBonusAmount AS BondsLossRatioBonusAmount,
	EXP_CalValues_PrimaryAgency.o_RegularCommission AS RegularCommission,
	EXP_CalValues_PrimaryAgency.o_DividendAmount AS DividendAmount,
	EXP_CalValues_PrimaryAgency.o_NSIExpense AS NSIExpense,
	EXP_CalValues_PrimaryAgency.o_StopLossAdjustmentClaimOccurrenceAmount AS StopLossAdjustmentClaimOccurrenceAmount,
	EXP_CalValues_PrimaryAgency.o_StopLossAdjustmentCatastropheAmount AS StopLossAdjustmentCatastropheAmount,
	EXP_CalValues_PrimaryAgency.o_NetDirectEarnedPremium AS NetDirectEarnedPremium,
	EXP_CalValues_PrimaryAgency.o_NetDirectIncurredLoss AS NetDirectIncurredLoss,
	EXP_CalValues_PrimaryAgency.o_NetLossRatio AS NetLossRatio,
	EXP_CalValues_PrimaryAgency.o_ProfitSharingBonusRate AS ProfitSharingBonusRate,
	EXP_CalValues_PrimaryAgency.o_ProfitSharingCommission AS ProfitSharingCommission,
	EXP_CalValues_PrimaryAgency.o_GuaranteeFee AS GuaranteeFee,
	EXP_CalValues_PrimaryAgency.o_NetProfitSharingAmount AS NetProfitSharingAmount,
	EXP_CalValues_PrimaryAgency.o_ProfitSharingPaymentAmount AS ProfitSharingPaymentAmount,
	EXP_CalValues_PrimaryAgency.o_ProfitSharingGuaranteeFlag AS ProfitSharingGuaranteeFlag,
	EXP_CalValues_PrimaryAgency.GroupExperienceIndicator,
	EXP_CalValues_PrimaryAgency.o_BondsDirectIncurredLossSameYear AS BondsDirectIncurredLossSameYear,
	EXP_CalValues_PrimaryAgency.o_BondsGrowthLossRatio AS BondsGrowthLossRatio
	FROM EXP_CalValues_PrimaryAgency
	LEFT JOIN LKP_AgencyProfitSharingYTDFact_PrimaryAgency
	ON LKP_AgencyProfitSharingYTDFact_PrimaryAgency.SalesDivisionDimID = EXP_CalValues_PrimaryAgency.o_SalesDivisionDimId AND LKP_AgencyProfitSharingYTDFact_PrimaryAgency.AgencyDimId = EXP_CalValues_PrimaryAgency.o_AgencyDimID AND LKP_AgencyProfitSharingYTDFact_PrimaryAgency.RunDateId = EXP_CalValues_PrimaryAgency.o_RunDateID
),
RTR_Insert_Update_PrimaryAgency_INSERT AS (SELECT * FROM RTR_Insert_Update_PrimaryAgency WHERE ISNULL(AgencyProfitSharingYTDFactId)),
RTR_Insert_Update_PrimaryAgency_DEFAULT1 AS (SELECT * FROM RTR_Insert_Update_PrimaryAgency WHERE NOT ( (ISNULL(AgencyProfitSharingYTDFactId)) )),
TGT_AgencyProfitSharingYTDFact_PrimaryAgency_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyProfitSharingYTDFact
	(AuditId, SalesDivisionDimID, AgencyDimId, RunDateId, BondsDirectWrittenPremium, NSIDirectWrittenPremium, ProfitSharingEligibleDirectWrittenPremium, BondsDirectEarnedPremium, NSIEarnedPremium, ProfitSharingEligibleDirectEarnedPremium, BondsDirectIncurredLoss, ProfitSharingEligibleDirectIncurredLoss, BondsGrowthAmount, BondsGrowthBonusRate, BondsGrowthBonusAmount, BondsLossRatio, BondsLossRatioBonusRate, BondsLossRatioBonusAmount, RegularCommission, DividendAmount, NSIExpense, StopLossAdjustmentClaimOccurrenceAmount, StopLossAdjustmentCatastropheAmount, NetDirectEarnedPremium, NetDirectIncurredLoss, NetLossRatio, ProfitSharingBonusRate, ProfitSharingCommission, GuaranteeFee, NetProfitSharingAmount, ProfitSharingPaymentAmount, ProfitSharingGuaranteeFlag, GroupExperienceIndicator, BondsDirectIncurredLossSameYear, BondsGrowthLossRatio)
	SELECT 
	AUDITID, 
	SALESDIVISIONDIMID, 
	AGENCYDIMID, 
	RUNDATEID, 
	BONDSDIRECTWRITTENPREMIUM, 
	NSIDIRECTWRITTENPREMIUM, 
	PROFITSHARINGELIGIBLEDIRECTWRITTENPREMIUM, 
	BONDSDIRECTEARNEDPREMIUM, 
	NSIEARNEDPREMIUM, 
	PROFITSHARINGELIGIBLEDIRECTEARNEDPREMIUM, 
	BONDSDIRECTINCURREDLOSS, 
	PROFITSHARINGELIGIBLEDIRECTINCURREDLOSS, 
	BONDSGROWTHAMOUNT, 
	BONDSGROWTHBONUSRATE, 
	BONDSGROWTHBONUSAMOUNT, 
	BONDSLOSSRATIO, 
	BONDSLOSSRATIOBONUSRATE, 
	BONDSLOSSRATIOBONUSAMOUNT, 
	REGULARCOMMISSION, 
	DIVIDENDAMOUNT, 
	NSIEXPENSE, 
	STOPLOSSADJUSTMENTCLAIMOCCURRENCEAMOUNT, 
	STOPLOSSADJUSTMENTCATASTROPHEAMOUNT, 
	NETDIRECTEARNEDPREMIUM, 
	NETDIRECTINCURREDLOSS, 
	NETLOSSRATIO, 
	PROFITSHARINGBONUSRATE, 
	PROFITSHARINGCOMMISSION, 
	GUARANTEEFEE, 
	NETPROFITSHARINGAMOUNT, 
	PROFITSHARINGPAYMENTAMOUNT, 
	PROFITSHARINGGUARANTEEFLAG, 
	GROUPEXPERIENCEINDICATOR, 
	BONDSDIRECTINCURREDLOSSSAMEYEAR, 
	BONDSGROWTHLOSSRATIO
	FROM RTR_Insert_Update_PrimaryAgency_INSERT
),
LKP_AgencyDim_LegalPrimary_AppointedDate AS (
	SELECT
	AgencyAppointedDate,
	in_LegalPrimaryAgencyCode,
	AgencyCode
	FROM (
		select 
		agy.Agencycode as Agencycode,
		agy.AgencyAppointedDate as AgencyAppointedDate
		
		FROM  @{pipeline().parameters.TARGET_TABLE_OWNER_V3}.AgencyDim agy
		where agy.CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyCode ORDER BY AgencyAppointedDate) = 1
),
EXP_CalValues AS (
	SELECT
	EXP_DataCollect_Individual_Group.RunDateID AS i_RunDateID,
	EXP_DataCollect_Individual_Group.LastYearRunDateID AS i_LastYearRunDateID,
	EXP_DataCollect_Individual_Group.AgencyDimID AS i_AgencyDimID,
	EXP_DataCollect_Individual_Group.SalesDivisionDimId AS i_SalesDivisionDimId,
	EXP_DataCollect_Individual_Group.BondsDirectWrittenPremium AS i_BondsDirectWrittenPremium,
	EXP_DataCollect_Individual_Group.NSIDirectWrittenPremium AS i_NSIDirectWrittenPremium,
	EXP_DataCollect_Individual_Group.ProfitSharingEligibleDirectWrittenPremium AS i_ProfitSharingEligibleDirectWrittenPremium,
	EXP_DataCollect_Individual_Group.RegularCommission AS i_RegularCommission,
	EXP_DataCollect_Individual_Group.BondsDirectEarnedPremium AS i_BondsDirectEarnedPremium,
	EXP_DataCollect_Individual_Group.NSIEarnedPremium AS i_NSIEarnedPremium,
	EXP_DataCollect_Individual_Group.ProfitSharingEligibleDirectEarnedPremium AS i_ProfitSharingEligibleDirectEarnedPremium,
	EXP_DataCollect_Individual_Group.BondsDirectIncurredLoss AS i_BondsDirectIncurredLoss,
	EXP_DataCollect_Individual_Group.BondsDirectIncurredLossSameYear AS i_BondsDirectIncurredLossSameYear,
	EXP_DataCollect_Individual_Group.ProfitSharingEligibleDirectIncurredLoss AS i_ProfitSharingEligibleDirectIncurredLoss,
	EXP_DataCollect_Individual_Group.DividendAmount AS i_DividendAmount,
	EXP_DataCollect_Individual_Group.lkp_LastYearYTDBondsDirectWrittenPremium,
	EXP_DataCollect_Individual_Group.lkp_StopLossAdjustmentAmountOccurrence AS StopLossAdjustmentAmountOccurrence,
	EXP_DataCollect_Individual_Group.AgencyDimId_cat,
	EXP_DataCollect_Individual_Group.lkp_StopLossAdjustmentAmountCatastrophe AS StopLossAdjustmentAmountCatastrophe,
	-- *INF*: IIF(i_AgencyDimID=AgencyDimId_cat,StopLossAdjustmentAmountCatastrophe,0)
	IFF(i_AgencyDimID = AgencyDimId_cat, StopLossAdjustmentAmountCatastrophe, 0) AS lkp_StopLossAdjustmentAmountCatastrophe,
	EXP_DataCollect_Individual_Group.lkp_ProfitSharingGuaranteeFlag,
	LKP_AgencyDim_LegalPrimary_AppointedDate.AgencyAppointedDate,
	LKP_AgencyDim_LegalPrimary_AppointedDate.in_LegalPrimaryAgencyCode,
	-- *INF*: SUBSTR(in_LegalPrimaryAgencyCode,1,2)
	SUBSTR(in_LegalPrimaryAgencyCode, 1, 2) AS v_NSI_Agencies,
	-- *INF*: DATE_DIFF(RunDate,AgencyAppointedDate,'YY')
	DATE_DIFF(RunDate, AgencyAppointedDate, 'YY') AS v_WB_AgencyYears,
	EXP_DataCollect_Individual_Group.RunDate,
	-- *INF*: GET_DATE_PART(RunDate,'MM')
	GET_DATE_PART(RunDate, 'MM') AS v_RunMonth,
	0 AS GuaranteedCommission,
	-- *INF*: ROUND(i_BondsDirectWrittenPremium-lkp_LastYearYTDBondsDirectWrittenPremium,4)
	ROUND(i_BondsDirectWrittenPremium - lkp_LastYearYTDBondsDirectWrittenPremium, 4) AS v_BondsGrowthAmount,
	-- *INF*: DECODE(TRUE,
	-- v_BondsGrowthAmount<25000.00,
	-- 0.00,
	-- v_BondsGrowthAmount>=25000.00 AND v_BondsGrowthAmount<50001.00,
	-- 1.0,
	-- v_BondsGrowthAmount>=50001.00 AND v_BondsGrowthAmount<100001.00,
	-- 2.0,
	-- v_BondsGrowthAmount>=100001.00 AND v_BondsGrowthAmount<150001.00,
	-- 3.0,
	-- v_BondsGrowthAmount>=150001.00,
	-- 4.0)
	-- 
	DECODE(TRUE,
	v_BondsGrowthAmount < 25000.00, 0.00,
	v_BondsGrowthAmount >= 25000.00 AND v_BondsGrowthAmount < 50001.00, 1.0,
	v_BondsGrowthAmount >= 50001.00 AND v_BondsGrowthAmount < 100001.00, 2.0,
	v_BondsGrowthAmount >= 100001.00 AND v_BondsGrowthAmount < 150001.00, 3.0,
	v_BondsGrowthAmount >= 150001.00, 4.0) AS v_BondsGrowthBonusRate,
	-- *INF*: lkp_ProfitSharingGuaranteeFlag
	-- 
	-- --DECODE(lkp_ProfitSharingGuaranteeFlag,'T','1','F','0','')
	lkp_ProfitSharingGuaranteeFlag AS v_ProfitSharingGuaranteeFlag,
	-- *INF*: DECODE(TRUE,i_BondsDirectEarnedPremium<=0,100,i_BondsDirectIncurredLoss <=0,0,ROUND(i_BondsDirectIncurredLoss/ i_BondsDirectEarnedPremium  * 100,4))
	DECODE(TRUE,
	i_BondsDirectEarnedPremium <= 0, 100,
	i_BondsDirectIncurredLoss <= 0, 0,
	ROUND(i_BondsDirectIncurredLoss / i_BondsDirectEarnedPremium * 100, 4)) AS v_BondsLossRatio,
	-- *INF*: DECODE(TRUE,i_BondsDirectEarnedPremium<=0,100,i_BondsDirectIncurredLossSameYear <=0,0,ROUND(i_BondsDirectIncurredLossSameYear/ i_BondsDirectEarnedPremium  * 100,4))
	DECODE(TRUE,
	i_BondsDirectEarnedPremium <= 0, 100,
	i_BondsDirectIncurredLossSameYear <= 0, 0,
	ROUND(i_BondsDirectIncurredLossSameYear / i_BondsDirectEarnedPremium * 100, 4)) AS v_BondsGrowthLossRatio,
	-- *INF*: DECODE(TRUE,
	-- v_BondsLossRatio<5.1,
	-- 4.0,
	-- v_BondsLossRatio>=5.1 AND v_BondsLossRatio<11.1,
	-- 3.0,
	-- v_BondsLossRatio>=11.1 AND v_BondsLossRatio<15.1,
	-- 2.0,
	-- v_BondsLossRatio>=15.1 AND v_BondsLossRatio<20.1,
	-- 1.0,
	-- 0)
	DECODE(TRUE,
	v_BondsLossRatio < 5.1, 4.0,
	v_BondsLossRatio >= 5.1 AND v_BondsLossRatio < 11.1, 3.0,
	v_BondsLossRatio >= 11.1 AND v_BondsLossRatio < 15.1, 2.0,
	v_BondsLossRatio >= 15.1 AND v_BondsLossRatio < 20.1, 1.0,
	0) AS v_BondsLossRatioBonusRate,
	-- *INF*: IIF(i_NSIDirectWrittenPremium<100000,ROUND(i_NSIDirectWrittenPremium*.1,4),0)
	IFF(i_NSIDirectWrittenPremium < 100000, ROUND(i_NSIDirectWrittenPremium * .1, 4), 0) AS v_NSIExpense,
	-- *INF*: ROUND(i_ProfitSharingEligibleDirectEarnedPremium-i_DividendAmount,4)
	ROUND(i_ProfitSharingEligibleDirectEarnedPremium - i_DividendAmount, 4) AS v_NetDirectEarnedPremium,
	-- *INF*: ROUND(i_ProfitSharingEligibleDirectIncurredLoss-StopLossAdjustmentAmountOccurrence-lkp_StopLossAdjustmentAmountCatastrophe+0.00,4)
	-- 
	-- --had v_NSIExpense value were 0.00 was present as it was removed for PROD-8913 issue 4
	ROUND(i_ProfitSharingEligibleDirectIncurredLoss - StopLossAdjustmentAmountOccurrence - lkp_StopLossAdjustmentAmountCatastrophe + 0.00, 4) AS v_NetDirectIncurredLoss,
	-- *INF*: DECODE(TRUE,v_NetDirectEarnedPremium<=0,100,v_NetDirectIncurredLoss<=0,0,ROUND(v_NetDirectIncurredLoss/v_NetDirectEarnedPremium*100,4))
	DECODE(TRUE,
	v_NetDirectEarnedPremium <= 0, 100,
	v_NetDirectIncurredLoss <= 0, 0,
	ROUND(v_NetDirectIncurredLoss / v_NetDirectEarnedPremium * 100, 4)) AS v_NetLossRatio,
	-- *INF*: :LKP.LKP_SUPPROFTSHARINGBONUS(TO_INTEGER(ROUND(i_ProfitSharingEligibleDirectWrittenPremium)),ROUND(v_NetLossRatio,1))
	LKP_SUPPROFTSHARINGBONUS_TO_INTEGER_ROUND_i_ProfitSharingEligibleDirectWrittenPremium_ROUND_v_NetLossRatio_1.ProfitSharingBonusRate AS v_lkpProfitSharingBonusRate,
	-- *INF*: :LKP.LKP_AGENCYBONUSRATES(in_LegalPrimaryAgencyCode)
	LKP_AGENCYBONUSRATES_in_LegalPrimaryAgencyCode.BonusRate AS v_lkpProfitSharingBonusRateOverride,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(v_lkpProfitSharingBonusRate),0,
	-- v_WB_AgencyYears<=2 and i_ProfitSharingEligibleDirectWrittenPremium>=100000,v_lkpProfitSharingBonusRate,
	-- v_WB_AgencyYears > 2 and i_ProfitSharingEligibleDirectWrittenPremium>=250000,v_lkpProfitSharingBonusRate,0)
	DECODE(TRUE,
	v_lkpProfitSharingBonusRate IS NULL, 0,
	v_WB_AgencyYears <= 2 AND i_ProfitSharingEligibleDirectWrittenPremium >= 100000, v_lkpProfitSharingBonusRate,
	v_WB_AgencyYears > 2 AND i_ProfitSharingEligibleDirectWrittenPremium >= 250000, v_lkpProfitSharingBonusRate,
	0) AS v_ProfitSharingBonusRate,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(v_lkpProfitSharingBonusRateOverride),
	-- v_ProfitSharingBonusRate,
	-- v_lkpProfitSharingBonusRateOverride)
	-- -- use Agency Bonus Rate override if it exists else default to calculated Bonus Rate
	-- 
	-- 
	-- 
	-- 
	-- 
	DECODE(TRUE,
	v_lkpProfitSharingBonusRateOverride IS NULL, v_ProfitSharingBonusRate,
	v_lkpProfitSharingBonusRateOverride) AS v_ProfitSharingBonusRateFinal,
	-- *INF*: ROUND((v_NetDirectEarnedPremium * 
	-- v_ProfitSharingBonusRateFinal)/100,4)
	-- --v_ProfitSharingBonusRate)/100,4)
	-- -- Modified to use override rate if it exists else use computed rate
	-- -- Made change as part of fix for EDWP-1812, as BonusRate is %, we need divide by 100.
	ROUND(( v_NetDirectEarnedPremium * v_ProfitSharingBonusRateFinal ) / 100, 4) AS v_ProfitSharingCommission,
	-- *INF*: DECODE(TRUE,
	--  NOT ISNULL(v_lkpProfitSharingBonusRateOverride),'0',
	-- v_ProfitSharingGuaranteeFlag)
	-- --No Guarantee Fee applies for Agency Bonus Rate overrides
	DECODE(TRUE,
	NOT v_lkpProfitSharingBonusRateOverride IS NULL, '0',
	v_ProfitSharingGuaranteeFlag) AS v_ProfitSharingGuaranteeFlagFinal,
	-- *INF*: IIF(IN(v_RunMonth,'10','11','12'),i_GuaranteeFee_Sep,IIF(v_ProfitSharingGuaranteeFlagFinal='1',ROUND(v_ProfitSharingCommission * .15,4),0))
	-- --modified to check for guarantee flag final to allow for bonus rate overrides 
	IFF(IN(v_RunMonth, '10', '11', '12'), i_GuaranteeFee_Sep, IFF(v_ProfitSharingGuaranteeFlagFinal = '1', ROUND(v_ProfitSharingCommission * .15, 4), 0)) AS v_GuaranteeFee,
	-- *INF*: ROUND(v_ProfitSharingCommission-v_GuaranteeFee,4)
	ROUND(v_ProfitSharingCommission - v_GuaranteeFee, 4) AS v_NetProfitSharingAmount,
	-- *INF*: IIF(IN(v_RunMonth,'10','11','12'),IIF(i_GuaranteedCommission>v_NetProfitSharingAmount,i_GuaranteedCommission,v_NetProfitSharingAmount),v_NetProfitSharingAmount)
	IFF(IN(v_RunMonth, '10', '11', '12'), IFF(i_GuaranteedCommission > v_NetProfitSharingAmount, i_GuaranteedCommission, v_NetProfitSharingAmount), v_NetProfitSharingAmount) AS v_ProfitSharingPaymentAmount,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	i_SalesDivisionDimId AS o_SalesDivisionDimId,
	i_AgencyDimID AS o_AgencyDimID,
	i_RunDateID AS o_RunDateID,
	i_BondsDirectWrittenPremium AS o_BondsDirectWrittenPremium,
	i_NSIDirectWrittenPremium AS o_NSIDirectWrittenPremium,
	i_ProfitSharingEligibleDirectWrittenPremium AS o_ProfitSharingEligibleDirectWrittenPremium,
	i_BondsDirectEarnedPremium AS o_BondsDirectEarnedPremium,
	i_NSIEarnedPremium AS o_NSIEarnedPremium,
	i_ProfitSharingEligibleDirectEarnedPremium AS o_ProfitSharingEligibleDirectEarnedPremium,
	i_BondsDirectIncurredLoss AS o_BondsDirectIncurredLoss,
	i_ProfitSharingEligibleDirectIncurredLoss AS o_ProfitSharingEligibleDirectIncurredLoss,
	v_BondsGrowthAmount AS o_BondsGrowthAmount,
	v_BondsGrowthBonusRate AS o_BondsGrowthBonusRate,
	-- *INF*: ROUND(i_BondsDirectWrittenPremium*v_BondsGrowthBonusRate/100,4)
	ROUND(i_BondsDirectWrittenPremium * v_BondsGrowthBonusRate / 100, 4) AS o_BondsGrowthBonusAmount,
	v_BondsLossRatio AS o_BondsLossRatio,
	v_BondsLossRatioBonusRate AS o_BondsLossRatioBonusRate,
	-- *INF*: ROUND(v_BondsLossRatioBonusRate*i_BondsDirectEarnedPremium/100,4)
	ROUND(v_BondsLossRatioBonusRate * i_BondsDirectEarnedPremium / 100, 4) AS o_BondsLossRatioBonusAmount,
	i_RegularCommission AS o_RegularCommission,
	i_DividendAmount AS o_DividendAmount,
	-- *INF*: 0
	-- 
	-- --v_NSIExpense removed this variable condition for PROD-8913 issue #4 and defaulting the value to 0
	0 AS o_NSIExpense,
	StopLossAdjustmentAmountOccurrence AS o_StopLossAdjustmentClaimOccurrenceAmount,
	lkp_StopLossAdjustmentAmountCatastrophe AS o_StopLossAdjustmentCatastropheAmount,
	v_NetDirectEarnedPremium AS o_NetDirectEarnedPremium,
	v_NetDirectIncurredLoss AS o_NetDirectIncurredLoss,
	v_NetLossRatio AS o_NetLossRatio,
	v_ProfitSharingBonusRateFinal AS o_ProfitSharingBonusRate,
	v_ProfitSharingCommission AS o_ProfitSharingCommission,
	-- *INF*: IIF(isnull(v_GuaranteeFee),0,v_GuaranteeFee)
	IFF(v_GuaranteeFee IS NULL, 0, v_GuaranteeFee) AS o_GuaranteeFee,
	-- *INF*: IIF(isnull(v_NetProfitSharingAmount),0,v_NetProfitSharingAmount)
	IFF(v_NetProfitSharingAmount IS NULL, 0, v_NetProfitSharingAmount) AS o_NetProfitSharingAmount,
	-- *INF*: IIF(isnull(v_ProfitSharingPaymentAmount),0,v_ProfitSharingPaymentAmount)
	IFF(v_ProfitSharingPaymentAmount IS NULL, 0, v_ProfitSharingPaymentAmount) AS o_ProfitSharingPaymentAmount,
	v_ProfitSharingGuaranteeFlagFinal AS o_ProfitSharingGuaranteeFlag,
	'INDIVIDUAL' AS GroupExperienceIndicator,
	i_BondsDirectIncurredLoss AS o_BondsDirectIncurredLossSameYear,
	v_BondsGrowthLossRatio AS o_BondsGrowthLossRatio
	FROM EXP_DataCollect_Individual_Group
	LEFT JOIN LKP_AgencyDim_LegalPrimary_AppointedDate
	ON LKP_AgencyDim_LegalPrimary_AppointedDate.AgencyCode = EXP_DataCollect_Individual_Group.O_LegalPrimaryAgencyCode
	LEFT JOIN LKP_SUPPROFTSHARINGBONUS LKP_SUPPROFTSHARINGBONUS_TO_INTEGER_ROUND_i_ProfitSharingEligibleDirectWrittenPremium_ROUND_v_NetLossRatio_1
	ON LKP_SUPPROFTSHARINGBONUS_TO_INTEGER_ROUND_i_ProfitSharingEligibleDirectWrittenPremium_ROUND_v_NetLossRatio_1.DirectWrittenPremiumLow = TO_INTEGER(ROUND(i_ProfitSharingEligibleDirectWrittenPremium))
	AND LKP_SUPPROFTSHARINGBONUS_TO_INTEGER_ROUND_i_ProfitSharingEligibleDirectWrittenPremium_ROUND_v_NetLossRatio_1.DirectWrittenPremiumHigh = ROUND(v_NetLossRatio, 1)

	LEFT JOIN LKP_AGENCYBONUSRATES LKP_AGENCYBONUSRATES_in_LegalPrimaryAgencyCode
	ON LKP_AGENCYBONUSRATES_in_LegalPrimaryAgencyCode.Agency = in_LegalPrimaryAgencyCode

),
LKP_AgencyProfitSharingYTDFact AS (
	SELECT
	AgencyProfitSharingYTDFactId,
	SalesDivisionDimID,
	AgencyDimId,
	RunDateId
	FROM (
		select AgencyDimId AS AgencyDimId,
		SalesDivisionDimID AS SalesDivisionDimID,
		RunDateId AS RunDateId,
		AgencyProfitSharingYTDFactId  AS AgencyProfitSharingYTDFactId
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyProfitSharingYTDFact af
		join @{pipeline().parameters.TARGET_TABLE_OWNER}.calendar_dim cd 
		on af.RunDateId=cd.clndr_id 
		and cd.clndr_yr=YEAR(dateadd(dd,-1,dateadd(mm,DATEDIFF(MM,0,GETDATE())-@{pipeline().parameters.NO_OF_MONTHS},0)))
		and af.GroupExperienceIndicator='INDIVIDUAL'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SalesDivisionDimID,AgencyDimId,RunDateId ORDER BY AgencyProfitSharingYTDFactId DESC) = 1
),
RTR_Insert_Update AS (
	SELECT
	LKP_AgencyProfitSharingYTDFact.AgencyProfitSharingYTDFactId,
	EXP_CalValues.o_AuditId AS AuditId,
	EXP_CalValues.o_SalesDivisionDimId AS SalesDivisionDimID,
	EXP_CalValues.o_AgencyDimID AS AgencyDimId,
	EXP_CalValues.o_RunDateID AS RunDateId,
	EXP_CalValues.o_BondsDirectWrittenPremium AS BondsDirectWrittenPremium,
	EXP_CalValues.o_NSIDirectWrittenPremium AS NSIDirectWrittenPremium,
	EXP_CalValues.o_ProfitSharingEligibleDirectWrittenPremium AS ProfitSharingEligibleDirectWrittenPremium,
	EXP_CalValues.o_BondsDirectEarnedPremium AS BondsDirectEarnedPremium,
	EXP_CalValues.o_NSIEarnedPremium AS NSIEarnedPremium,
	EXP_CalValues.o_ProfitSharingEligibleDirectEarnedPremium AS ProfitSharingEligibleDirectEarnedPremium,
	EXP_CalValues.o_BondsDirectIncurredLoss AS BondsDirectIncurredLoss,
	EXP_CalValues.o_ProfitSharingEligibleDirectIncurredLoss AS ProfitSharingEligibleDirectIncurredLoss,
	EXP_CalValues.o_BondsGrowthAmount AS BondsGrowthAmount,
	EXP_CalValues.o_BondsGrowthBonusRate AS BondsGrowthBonusRate,
	EXP_CalValues.o_BondsGrowthBonusAmount AS BondsGrowthBonusAmount,
	EXP_CalValues.o_BondsLossRatio AS BondsLossRatio,
	EXP_CalValues.o_BondsLossRatioBonusRate AS BondsLossRatioBonusRate,
	EXP_CalValues.o_BondsLossRatioBonusAmount AS BondsLossRatioBonusAmount,
	EXP_CalValues.o_RegularCommission AS RegularCommission,
	EXP_CalValues.o_DividendAmount AS DividendAmount,
	EXP_CalValues.o_NSIExpense AS NSIExpense,
	EXP_CalValues.o_StopLossAdjustmentClaimOccurrenceAmount AS StopLossAdjustmentClaimOccurrenceAmount,
	EXP_CalValues.o_StopLossAdjustmentCatastropheAmount AS StopLossAdjustmentCatastropheAmount,
	EXP_CalValues.o_NetDirectEarnedPremium AS NetDirectEarnedPremium,
	EXP_CalValues.o_NetDirectIncurredLoss AS NetDirectIncurredLoss,
	EXP_CalValues.o_NetLossRatio AS NetLossRatio,
	EXP_CalValues.o_ProfitSharingBonusRate AS ProfitSharingBonusRate,
	EXP_CalValues.o_ProfitSharingCommission AS ProfitSharingCommission,
	EXP_CalValues.o_GuaranteeFee AS GuaranteeFee,
	EXP_CalValues.o_NetProfitSharingAmount AS NetProfitSharingAmount,
	EXP_CalValues.o_ProfitSharingPaymentAmount AS ProfitSharingPaymentAmount,
	EXP_CalValues.o_ProfitSharingGuaranteeFlag AS ProfitSharingGuaranteeFlag,
	EXP_CalValues.GroupExperienceIndicator,
	EXP_CalValues.o_BondsDirectIncurredLossSameYear AS BondsDirectIncurredLossSameYear,
	EXP_CalValues.o_BondsGrowthLossRatio AS BondsGrowthLossRatio
	FROM EXP_CalValues
	LEFT JOIN LKP_AgencyProfitSharingYTDFact
	ON LKP_AgencyProfitSharingYTDFact.SalesDivisionDimID = EXP_CalValues.o_SalesDivisionDimId AND LKP_AgencyProfitSharingYTDFact.AgencyDimId = EXP_CalValues.o_AgencyDimID AND LKP_AgencyProfitSharingYTDFact.RunDateId = EXP_CalValues.o_RunDateID
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE ISNULL(AgencyProfitSharingYTDFactId)),
RTR_Insert_Update_DEFAULT1 AS (SELECT * FROM RTR_Insert_Update WHERE NOT ( (ISNULL(AgencyProfitSharingYTDFactId)) )),
UPD_Target AS (
	SELECT
	AgencyProfitSharingYTDFactId, 
	AuditId, 
	SalesDivisionDimID, 
	AgencyDimId, 
	RunDateId, 
	BondsDirectWrittenPremium, 
	NSIDirectWrittenPremium, 
	ProfitSharingEligibleDirectWrittenPremium, 
	BondsDirectEarnedPremium, 
	NSIEarnedPremium, 
	ProfitSharingEligibleDirectEarnedPremium, 
	BondsDirectIncurredLoss, 
	ProfitSharingEligibleDirectIncurredLoss, 
	BondsGrowthAmount, 
	BondsGrowthBonusRate, 
	BondsGrowthBonusAmount, 
	BondsLossRatio, 
	BondsLossRatioBonusRate, 
	BondsLossRatioBonusAmount, 
	RegularCommission, 
	DividendAmount, 
	NSIExpense, 
	StopLossAdjustmentClaimOccurrenceAmount, 
	StopLossAdjustmentCatastropheAmount, 
	NetDirectEarnedPremium, 
	NetDirectIncurredLoss, 
	NetLossRatio, 
	ProfitSharingBonusRate, 
	ProfitSharingCommission, 
	GuaranteeFee, 
	NetProfitSharingAmount, 
	ProfitSharingPaymentAmount, 
	ProfitSharingGuaranteeFlag, 
	GroupExperienceIndicator AS GroupExperienceIndicator2, 
	BondsDirectIncurredLossSameYear AS BondsDirectIncurredLossSameYear2, 
	BondsGrowthLossRatio AS BondsGrowthLossRatio2
	FROM RTR_Insert_Update_DEFAULT1
),
TGT_AgencyProfitSharingYTDFact_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyProfitSharingYTDFact AS T
	USING UPD_Target AS S
	ON T.AgencyProfitSharingYTDFactId = S.AgencyProfitSharingYTDFactId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AuditId, T.SalesDivisionDimID = S.SalesDivisionDimID, T.AgencyDimId = S.AgencyDimId, T.RunDateId = S.RunDateId, T.BondsDirectWrittenPremium = S.BondsDirectWrittenPremium, T.NSIDirectWrittenPremium = S.NSIDirectWrittenPremium, T.ProfitSharingEligibleDirectWrittenPremium = S.ProfitSharingEligibleDirectWrittenPremium, T.BondsDirectEarnedPremium = S.BondsDirectEarnedPremium, T.NSIEarnedPremium = S.NSIEarnedPremium, T.ProfitSharingEligibleDirectEarnedPremium = S.ProfitSharingEligibleDirectEarnedPremium, T.BondsDirectIncurredLoss = S.BondsDirectIncurredLoss, T.ProfitSharingEligibleDirectIncurredLoss = S.ProfitSharingEligibleDirectIncurredLoss, T.BondsGrowthAmount = S.BondsGrowthAmount, T.BondsGrowthBonusRate = S.BondsGrowthBonusRate, T.BondsGrowthBonusAmount = S.BondsGrowthBonusAmount, T.BondsLossRatio = S.BondsLossRatio, T.BondsLossRatioBonusRate = S.BondsLossRatioBonusRate, T.BondsLossRatioBonusAmount = S.BondsLossRatioBonusAmount, T.RegularCommission = S.RegularCommission, T.DividendAmount = S.DividendAmount, T.NSIExpense = S.NSIExpense, T.StopLossAdjustmentClaimOccurrenceAmount = S.StopLossAdjustmentClaimOccurrenceAmount, T.StopLossAdjustmentCatastropheAmount = S.StopLossAdjustmentCatastropheAmount, T.NetDirectEarnedPremium = S.NetDirectEarnedPremium, T.NetDirectIncurredLoss = S.NetDirectIncurredLoss, T.NetLossRatio = S.NetLossRatio, T.ProfitSharingBonusRate = S.ProfitSharingBonusRate, T.ProfitSharingCommission = S.ProfitSharingCommission, T.GuaranteeFee = S.GuaranteeFee, T.NetProfitSharingAmount = S.NetProfitSharingAmount, T.ProfitSharingPaymentAmount = S.ProfitSharingPaymentAmount, T.ProfitSharingGuaranteeFlag = S.ProfitSharingGuaranteeFlag, T.GroupExperienceIndicator = S.GroupExperienceIndicator2, T.BondsDirectIncurredLossSameYear = S.BondsDirectIncurredLossSameYear2, T.BondsGrowthLossRatio = S.BondsGrowthLossRatio2
),
TGT_AgencyProfitSharingYTDFact_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyProfitSharingYTDFact
	(AuditId, SalesDivisionDimID, AgencyDimId, RunDateId, BondsDirectWrittenPremium, NSIDirectWrittenPremium, ProfitSharingEligibleDirectWrittenPremium, BondsDirectEarnedPremium, NSIEarnedPremium, ProfitSharingEligibleDirectEarnedPremium, BondsDirectIncurredLoss, ProfitSharingEligibleDirectIncurredLoss, BondsGrowthAmount, BondsGrowthBonusRate, BondsGrowthBonusAmount, BondsLossRatio, BondsLossRatioBonusRate, BondsLossRatioBonusAmount, RegularCommission, DividendAmount, NSIExpense, StopLossAdjustmentClaimOccurrenceAmount, StopLossAdjustmentCatastropheAmount, NetDirectEarnedPremium, NetDirectIncurredLoss, NetLossRatio, ProfitSharingBonusRate, ProfitSharingCommission, GuaranteeFee, NetProfitSharingAmount, ProfitSharingPaymentAmount, ProfitSharingGuaranteeFlag, GroupExperienceIndicator, BondsDirectIncurredLossSameYear, BondsGrowthLossRatio)
	SELECT 
	AUDITID, 
	SALESDIVISIONDIMID, 
	AGENCYDIMID, 
	RUNDATEID, 
	BONDSDIRECTWRITTENPREMIUM, 
	NSIDIRECTWRITTENPREMIUM, 
	PROFITSHARINGELIGIBLEDIRECTWRITTENPREMIUM, 
	BONDSDIRECTEARNEDPREMIUM, 
	NSIEARNEDPREMIUM, 
	PROFITSHARINGELIGIBLEDIRECTEARNEDPREMIUM, 
	BONDSDIRECTINCURREDLOSS, 
	PROFITSHARINGELIGIBLEDIRECTINCURREDLOSS, 
	BONDSGROWTHAMOUNT, 
	BONDSGROWTHBONUSRATE, 
	BONDSGROWTHBONUSAMOUNT, 
	BONDSLOSSRATIO, 
	BONDSLOSSRATIOBONUSRATE, 
	BONDSLOSSRATIOBONUSAMOUNT, 
	REGULARCOMMISSION, 
	DIVIDENDAMOUNT, 
	NSIEXPENSE, 
	STOPLOSSADJUSTMENTCLAIMOCCURRENCEAMOUNT, 
	STOPLOSSADJUSTMENTCATASTROPHEAMOUNT, 
	NETDIRECTEARNEDPREMIUM, 
	NETDIRECTINCURREDLOSS, 
	NETLOSSRATIO, 
	PROFITSHARINGBONUSRATE, 
	PROFITSHARINGCOMMISSION, 
	GUARANTEEFEE, 
	NETPROFITSHARINGAMOUNT, 
	PROFITSHARINGPAYMENTAMOUNT, 
	PROFITSHARINGGUARANTEEFLAG, 
	GROUPEXPERIENCEINDICATOR, 
	BONDSDIRECTINCURREDLOSSSAMEYEAR, 
	BONDSGROWTHLOSSRATIO
	FROM RTR_Insert_Update_INSERT
),
UPD_Target_PrimaryAgency AS (
	SELECT
	AgencyProfitSharingYTDFactId, 
	AuditId, 
	SalesDivisionDimID, 
	AgencyDimId, 
	RunDateId, 
	BondsDirectWrittenPremium, 
	NSIDirectWrittenPremium, 
	ProfitSharingEligibleDirectWrittenPremium, 
	BondsDirectEarnedPremium, 
	NSIEarnedPremium, 
	ProfitSharingEligibleDirectEarnedPremium, 
	BondsDirectIncurredLoss, 
	ProfitSharingEligibleDirectIncurredLoss, 
	BondsGrowthAmount, 
	BondsGrowthBonusRate, 
	BondsGrowthBonusAmount, 
	BondsLossRatio, 
	BondsLossRatioBonusRate, 
	BondsLossRatioBonusAmount, 
	RegularCommission, 
	DividendAmount, 
	NSIExpense, 
	StopLossAdjustmentClaimOccurrenceAmount, 
	StopLossAdjustmentCatastropheAmount, 
	NetDirectEarnedPremium, 
	NetDirectIncurredLoss, 
	NetLossRatio, 
	ProfitSharingBonusRate, 
	ProfitSharingCommission, 
	GuaranteeFee, 
	NetProfitSharingAmount, 
	ProfitSharingPaymentAmount, 
	ProfitSharingGuaranteeFlag, 
	GroupExperienceIndicator AS GroupExperienceIndicator2, 
	BondsDirectIncurredLossSameYear AS BondsDirectIncurredLossSameYear2, 
	BondsGrowthLossRatio AS BondsGrowthLossRatio2
	FROM RTR_Insert_Update_PrimaryAgency_DEFAULT1
),
TGT_AgencyProfitSharingYTDFact_PrimaryAgency_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyProfitSharingYTDFact AS T
	USING UPD_Target_PrimaryAgency AS S
	ON T.AgencyProfitSharingYTDFactId = S.AgencyProfitSharingYTDFactId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AuditId, T.SalesDivisionDimID = S.SalesDivisionDimID, T.AgencyDimId = S.AgencyDimId, T.RunDateId = S.RunDateId, T.BondsDirectWrittenPremium = S.BondsDirectWrittenPremium, T.NSIDirectWrittenPremium = S.NSIDirectWrittenPremium, T.ProfitSharingEligibleDirectWrittenPremium = S.ProfitSharingEligibleDirectWrittenPremium, T.BondsDirectEarnedPremium = S.BondsDirectEarnedPremium, T.NSIEarnedPremium = S.NSIEarnedPremium, T.ProfitSharingEligibleDirectEarnedPremium = S.ProfitSharingEligibleDirectEarnedPremium, T.BondsDirectIncurredLoss = S.BondsDirectIncurredLoss, T.ProfitSharingEligibleDirectIncurredLoss = S.ProfitSharingEligibleDirectIncurredLoss, T.BondsGrowthAmount = S.BondsGrowthAmount, T.BondsGrowthBonusRate = S.BondsGrowthBonusRate, T.BondsGrowthBonusAmount = S.BondsGrowthBonusAmount, T.BondsLossRatio = S.BondsLossRatio, T.BondsLossRatioBonusRate = S.BondsLossRatioBonusRate, T.BondsLossRatioBonusAmount = S.BondsLossRatioBonusAmount, T.RegularCommission = S.RegularCommission, T.DividendAmount = S.DividendAmount, T.NSIExpense = S.NSIExpense, T.StopLossAdjustmentClaimOccurrenceAmount = S.StopLossAdjustmentClaimOccurrenceAmount, T.StopLossAdjustmentCatastropheAmount = S.StopLossAdjustmentCatastropheAmount, T.NetDirectEarnedPremium = S.NetDirectEarnedPremium, T.NetDirectIncurredLoss = S.NetDirectIncurredLoss, T.NetLossRatio = S.NetLossRatio, T.ProfitSharingBonusRate = S.ProfitSharingBonusRate, T.ProfitSharingCommission = S.ProfitSharingCommission, T.GuaranteeFee = S.GuaranteeFee, T.NetProfitSharingAmount = S.NetProfitSharingAmount, T.ProfitSharingPaymentAmount = S.ProfitSharingPaymentAmount, T.ProfitSharingGuaranteeFlag = S.ProfitSharingGuaranteeFlag, T.GroupExperienceIndicator = S.GroupExperienceIndicator2, T.BondsDirectIncurredLossSameYear = S.BondsDirectIncurredLossSameYear2, T.BondsGrowthLossRatio = S.BondsGrowthLossRatio2
),
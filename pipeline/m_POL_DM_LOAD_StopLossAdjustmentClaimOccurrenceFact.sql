WITH
LKP_StopLossAdjustmentClaimOccurrenceFact_PreviousInstance AS (
	SELECT
	StopLossAdjustmentAmount,
	EDWAgencyAKID,
	edw_pol_ak_id,
	edw_claim_occurrence_ak_id
	FROM (
		SELECT A.EDWAgencyAKID as EDWAgencyAKID,
		pol.edw_pol_ak_id as edw_pol_ak_id,
		CO.edw_claim_occurrence_ak_id as edw_claim_occurrence_ak_id,
		SLA.StopLossAdjustmentAmount AS StopLossAdjustmentAmount
		FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.StopLossAdjustmentclaimoccurrenceFact SLA,
		       @{pipeline().parameters.TARGET_TABLE_OWNER}.calendar_dim cd,
		       @{pipeline().parameters.TARGET_TABLE_OWNER_V3}.AgencyDim A,
		       @{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim POL,
		       @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence_dim CO
		WHERE  SLA.RunDateId = cd.clndr_id
		and SLA.AgencyDimId=A.AgencyDimID
		and SLA.PolicyDimid=POL.pol_dim_id
		AND SLA.ClaimOccurrenceDimId=CO.claim_occurrence_dim_id
		and cd.CalendarMonthOfYear = 12
		order by A.EDWAgencyAKID,pol.edw_pol_ak_id,CO.edw_claim_occurrence_ak_id,SLA.RunDateid --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWAgencyAKID,edw_pol_ak_id,edw_claim_occurrence_ak_id ORDER BY StopLossAdjustmentAmount DESC) = 1
),
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
SQ_claim_loss_transaction_fact_Occurrence AS (
	DECLARE @DATE2 as datetime
	
	SET @DATE2 = dateadd(SS,-1,dateadd(mm,DATEDIFF(MM,0,getdate())-@{pipeline().parameters.NO_OF_MONTHS},0))
	
	select EDWAgencyAKID,
	AgencyCode,
	
	edw_pol_ak_id,
	A.edw_claim_occurrence_ak_id,
	sum(case when claim_rpted_date<='2013-12-31 00:00:00' then (DirectLossPaidIR+DirectLossOutstandingER+DirectALAEPaidIR) 
	else (DirectLossPaidIR+DirectLossOutstandingER+DirectALAEPaidIR+DirectALAEOutstandingIR) end) total_direct_loss_recovery_incurred,
	--max(claim_loss_date) as Loss_Date,
	--convert(varchar(6),max(claim_loss_date),112) Loss_Year,
	convert(varchar(6),B.Claim_loss_date,112) Loss_Year,
	@DATE2 Rundate
	from (
	SELECT 
	A.EDWAgencyAKID,
	A.AgencyCode,
	POL.edw_pol_ak_id,
	OCC.edw_claim_occurrence_ak_id,
	CLTF.direct_loss_paid_including_recoveries DirectLossPaidIR,
	CLTF.direct_alae_paid_including_recoveries DirectALAEPaidIR,
	0.0 AS DirectLossOutstandingER,
	0.0 AS DirectALAEOutstandingIR,
	OCC.claim_rpted_date,
	CD.clndr_date as RunDate,
	Case when @DATE2 between occ.eff_from_date and occ.eff_to_date then OCC.claim_loss_date else '1800-01-01 01:00:00' end as claim_loss_date
	FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_loss_transaction_fact CLTF
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim CD
	ON CLTF.claim_trans_date_id = CD.clndr_id
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_dim OCC
	ON CLTF.claim_occurrence_dim_id = OCC.claim_occurrence_dim_id
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim A
	ON CLTF.AgencyDimId=A.AgencyDimID
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim POL
	ON CLTF.pol_dim_id=POL.pol_dim_id
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim ird
	ON CLTF.InsuranceReferenceDimId = ird.InsuranceReferenceDimId
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim ircd
	ON CLTF.InsuranceReferenceCoverageDimId = ircd.InsuranceReferenceCoverageDimId
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailDim cdd
	ON CLTF.CoverageDetailDimId = cdd.CoverageDetailDimId
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_product_code_dim apcd
	ON CLTF.asl_prdct_code_dim_id = apcd.asl_prdct_code_dim_id
	WHERE  clndr_date < '01/01/2001 00:00:00'
	and clndr_date<>'01/01/1800 00:00:00'
	and OCC.source_claim_occurrence_status_code in ('O','OPE','C')
	and case when (PolicyOfferingCode in ('600') 
	OR ird.InsuranceSegmentCode = '3'
	OR (ircd.CoverageGroupCode in ('BOILER','CYBERSEC','DATACOMP','CYBERSUITE') 
	OR ircd.CoverageGroupCode like '%TRIA' 
	OR ircd.CoverageGroupDescription in ('MCCA Surcharge'))
	OR ird.InsuranceReferenceLineofBusinessCode in ('350','311','590','812','890','900')
	OR (ird.RatingPlanAbbreviation in ('LRARO','Retro'))
	OR apcd.asl_prdct_code in ('200', '220')
	OR (ird.StrategicProfitCentercode <> '5' and cdd.ISOClassCode='0174')) then 'N' else 'Y' end='Y'
	
	UNION ALL
	
	SELECT 
	A.EDWAgencyAKID,
	A.AgencyCode,
	POL.edw_pol_ak_id,
	OCC.edw_claim_occurrence_ak_id,
	VLMF.DirectLossPaidIR,
	VLMF.DirectALAEPaidIR,
	VLMF.DirectLossOutstandingER,
	VLMF.DirectALAEOutstandingIR,
	OCC.claim_rpted_date,
	CD.clndr_date as RunDate,
	Case when @DATE2 between occ.eff_from_date and occ.eff_to_date then OCC.claim_loss_date else '1800-01-01 01:00:00' end as claim_loss_date
	FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.vwLossMasterFact VLMF
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim CD
	ON VLMF.loss_master_run_date_id = CD.clndr_id
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_dim OCC
	ON VLMF.claim_occurrence_dim_id = OCC.claim_occurrence_dim_id
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim A
	ON VLMF.AgencyDimId=A.AgencyDimID
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim POL
	ON VLMF.pol_dim_id=POL.pol_dim_id
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim ird
	ON VLMF.InsuranceReferenceDimId = ird.InsuranceReferenceDimId
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim ircd
	ON VLMF.InsuranceReferenceCoverageDimId = ircd.InsuranceReferenceCoverageDimId
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailDim cdd
	ON VLMF.CoverageDetailDimId = cdd.CoverageDetailDimId
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_product_code_dim apcd
	ON VLMF.asl_prdct_code_dim_id = apcd.asl_prdct_code_dim_id
	WHERE  clndr_date >= '01/01/2001 00:00:00'
	and OCC.source_claim_occurrence_status_code in ('O','OPE','C')
	and case when (PolicyOfferingCode in ('600') 
	OR ird.InsuranceSegmentCode = '3'
	OR (ircd.CoverageGroupCode in ('BOILER','CYBERSEC','DATACOMP','CYBERSUITE') 
	OR ircd.CoverageGroupCode like '%TRIA' 
	OR ircd.CoverageGroupDescription in ('MCCA Surcharge'))
	OR ird.InsuranceReferenceLineofBusinessCode in ('350','311','590','812','890','900')
	OR (ird.RatingPlanAbbreviation in ('LRARO','Retro'))
	OR apcd.asl_prdct_code in ('200', '220')
	OR (ird.StrategicProfitCentercode <> '5' and cdd.ISOClassCode='0174')) then 'N' else 'Y' end='Y'
	) A
	,(SELECT claim_cat_code,edw_claim_occurrence_ak_id ,claim_loss_date from claim_occurrence_dim
	WHERE  @DATE2 BETWEEN eff_from_date AND eff_to_date) B
	WHERE A.edw_claim_occurrence_ak_id = B.edw_claim_occurrence_ak_id
	and RunDate <=@DATE2
	and B.claim_cat_code='N/A'
	@{pipeline().parameters.WHERECLAUSEOCCURRENCE}
	group by EDWAgencyAKID,
	AgencyCode,
	edw_pol_ak_id,
	A.edw_claim_occurrence_ak_id,
	B.claim_loss_date
	--convert(varchar(6),claim_loss_date,112)
),
Exp_PassThrough AS (
	SELECT
	EDWAgencyAKID,
	AgencyCode,
	edw_pol_ak_id,
	edw_claim_occurrence_ak_id,
	total_direct_loss_recovery_incurred,
	Loss_Year,
	-- *INF*: SUBSTR(Loss_Year,1,4)
	SUBSTR(Loss_Year, 1, 4) AS o_Loss_Year,
	-- *INF*: SUBSTR(Loss_Year,5,2)
	SUBSTR(Loss_Year, 5, 2) AS o_Loss_Month,
	Rundate,
	-- *INF*: GET_DATE_PART(Rundate,'YYYY')
	GET_DATE_PART(Rundate, 'YYYY') AS o_Run_Year
	FROM SQ_claim_loss_transaction_fact_Occurrence
),
LKP_CLOSED_CLAIMS AS (
	SELECT
	loss_master_fact_id,
	edw_claim_occurrence_ak_id,
	Run_Year,
	in_edw_claim_occurrence_ak_id,
	i_Run_Year
	FROM (
		select  vlmf.loss_master_fact_id as loss_master_fact_id,
		cod.edw_claim_occurrence_ak_id  as edw_claim_occurrence_ak_id,
		convert(varchar(4),cd.CalendarDate,112) as Run_Year
		from 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.vwLossMasterFact vlmf
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim cd
		on vlmf.loss_master_run_date_id=cd.clndr_id
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_dim cod
		on vlmf.claim_occurrence_dim_id=cod.claim_occurrence_dim_id
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_occurrence_ak_id,Run_Year ORDER BY loss_master_fact_id) = 1
),
FIL_closed_claims AS (
	SELECT
	LKP_CLOSED_CLAIMS.loss_master_fact_id, 
	LKP_CLOSED_CLAIMS.edw_claim_occurrence_ak_id AS LKP_edw_claim_occurrence_ak_id, 
	Exp_PassThrough.EDWAgencyAKID, 
	Exp_PassThrough.AgencyCode, 
	Exp_PassThrough.edw_pol_ak_id, 
	Exp_PassThrough.edw_claim_occurrence_ak_id, 
	Exp_PassThrough.total_direct_loss_recovery_incurred, 
	Exp_PassThrough.o_Loss_Year, 
	Exp_PassThrough.o_Loss_Month, 
	Exp_PassThrough.Rundate
	FROM Exp_PassThrough
	LEFT JOIN LKP_CLOSED_CLAIMS
	ON LKP_CLOSED_CLAIMS.edw_claim_occurrence_ak_id = Exp_PassThrough.edw_claim_occurrence_ak_id AND LKP_CLOSED_CLAIMS.Run_Year = Exp_PassThrough.o_Run_Year
	WHERE NOT ISNULL(LKP_edw_claim_occurrence_ak_id)
),
EXP_GetValues_Occurrence AS (
	SELECT
	EDWAgencyAKID,
	AgencyCode,
	edw_pol_ak_id,
	edw_claim_occurrence_ak_id,
	total_direct_loss_recovery_incurred,
	o_Loss_Year,
	o_Loss_Month,
	Rundate
	FROM FIL_closed_claims
),
LKP_V3_PrimaryAgencyDimID_CO AS (
	SELECT
	LegalPrimaryAgencyCode,
	IN_Trans_Date,
	Agencycode,
	eff_from_date,
	eff_to_date
	FROM (
		SELECT 
		ard.LegalPrimaryAgencyCode as LegalPrimaryAgencyCode ,
		 
		AgencyDim.AgencyCode as AgencyCode, 
		Ard.AgencyrelationshipEffectiveDate as eff_from_date, 
		Ard.AgencyrelationshipExpirationDate as eff_to_date
		 FROM @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim as AgencyDim
		 
		 left join 
		 (select * from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Agencyrelationshipdim where currentsnapshotflag = 1) ard
		 on (AgencyDim.edwagencyakid=ard.edwagencyakid) 
		
		
		
		--Left join with "agencyrelationshipdim" in order to get legalprimaryagencycode values from --"AgencyRelationshipDim" table
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Agencycode,eff_from_date,eff_to_date ORDER BY LegalPrimaryAgencyCode DESC) = 1
),
EXP_LegalPrimaryagency AS (
	SELECT
	EXP_GetValues_Occurrence.AgencyCode AS IN_AgencyCode,
	LKP_V3_PrimaryAgencyDimID_CO.LegalPrimaryAgencyCode,
	-- *INF*: iif(isnull(LegalPrimaryAgencyCode),IN_AgencyCode,LegalPrimaryAgencyCode)
	IFF(LegalPrimaryAgencyCode IS NULL, IN_AgencyCode, LegalPrimaryAgencyCode) AS o_LegalPrimaryAgencyCode
	FROM EXP_GetValues_Occurrence
	LEFT JOIN LKP_V3_PrimaryAgencyDimID_CO
	ON LKP_V3_PrimaryAgencyDimID_CO.Agencycode = EXP_GetValues_Occurrence.AgencyCode AND LKP_V3_PrimaryAgencyDimID_CO.eff_from_date <= EXP_GetValues_Occurrence.Rundate AND LKP_V3_PrimaryAgencyDimID_CO.eff_to_date >= EXP_GetValues_Occurrence.Rundate
),
LKP_V3_PrimaryAgency_Primary AS (
	SELECT
	LegalPrimaryAgencyCode,
	IN_Trans_Date,
	Agencycode,
	eff_from_date,
	eff_to_date
	FROM (
		SELECT 
		
		ard.LegalPrimaryAgencyCode as LegalPrimaryAgencyCode, 
		 
		AgencyDim.AgencyCode as AgencyCode, 
		Ard.AgencyrelationshipEffectiveDate as eff_from_date, 
		Ard.AgencyrelationshipExpirationDate as eff_to_date
		 FROM @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim as AgencyDim
		 
		 left join 
		 (select * from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Agencyrelationshipdim where currentsnapshotflag = 1) ard
		 on (AgencyDim.edwagencyakid=ard.edwagencyakid) 
		
		
		--Left join with "agencyrelationshipdim" in order to get legalprimaryagencycode values from --"AgencyRelationshipDim" table
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Agencycode,eff_from_date,eff_to_date ORDER BY LegalPrimaryAgencyCode DESC) = 1
),
EXP_LegalPrimaryagency1 AS (
	SELECT
	EXP_GetValues_Occurrence.AgencyCode AS IN_AgencyCode,
	LKP_V3_PrimaryAgency_Primary.LegalPrimaryAgencyCode,
	-- *INF*: iif(isnull(LegalPrimaryAgencyCode),IN_AgencyCode,LegalPrimaryAgencyCode)
	IFF(LegalPrimaryAgencyCode IS NULL, IN_AgencyCode, LegalPrimaryAgencyCode) AS o_LegalPrimaryAgencyCode
	FROM EXP_GetValues_Occurrence
	LEFT JOIN LKP_V3_PrimaryAgency_Primary
	ON LKP_V3_PrimaryAgency_Primary.Agencycode = EXP_LegalPrimaryagency.o_LegalPrimaryAgencyCode AND LKP_V3_PrimaryAgency_Primary.eff_from_date <= EXP_GetValues_Occurrence.Rundate AND LKP_V3_PrimaryAgency_Primary.eff_to_date >= EXP_GetValues_Occurrence.Rundate
),
LKP_V3_PrimaryAgencyDimID_Primary AS (
	SELECT
	agency_dim_id,
	SalesDivisionDimId,
	Agencycode,
	eff_from_date,
	eff_to_date
	FROM (
		SELECT AgencyDim.AgencyDimID as agency_dim_id, 
		AgencyDim.AgencyCode as AgencyCode, 
		AgencyDim.EffectiveDate as eff_from_date, 
		AgencyDim.ExpirationDate as eff_to_date,
		AgencyDim.SalesDivisionDimId as SalesDivisionDimId
		 FROM @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim as AgencyDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Agencycode,eff_from_date,eff_to_date ORDER BY agency_dim_id DESC) = 1
),
LKP_WorkStopLossLimit_Occurrence AS (
	SELECT
	StopLossLimit,
	AgencyPreviousYearDirectWrittenPremium,
	AgencyCode,
	Loss_Year
	FROM (
		SELECT WSL.AgencyCode AS AgencyCode,
		WSL.StopLossLimit AS StopLossLimit,
		WSL.AgencyPreviousYearDirectWrittenPremium AS AgencyPreviousYearDirectWrittenPremium,
		convert(varchar(4),rundate,112) AS Loss_Year
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkStopLossLimit WSL
		WHERE WSL.CurrentSnapshotFlag=1
		--AND YEAR(WSL.RunDate)=YEAR(@{pipeline().parameters.RUNDATEEND})
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyCode,Loss_Year ORDER BY StopLossLimit DESC) = 1
),
LKP_WorkStopLossLimit_Occurrence_Primary AS (
	SELECT
	StopLossLimit,
	AgencyPreviousYearDirectWrittenPremium,
	PrimaryAgencyCode,
	Loss_Year
	FROM (
		SELECT WSL.PrimaryAgencyCode AS PrimaryAgencyCode,
		WSL.StopLossLimit AS StopLossLimit,
		WSL.AgencyPreviousYearDirectWrittenPremium AS AgencyPreviousYearDirectWrittenPremium,
		convert(varchar(4),rundate,112) AS Loss_Year
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkStopLossLimit WSL
		WHERE WSL.CurrentSnapshotFlag=1
		--AND YEAR(WSL.RunDate)=YEAR(@{pipeline().parameters.RUNDATEEND})
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PrimaryAgencyCode,Loss_Year ORDER BY StopLossLimit DESC) = 1
),
LKP_claim_occurrence_dim AS (
	SELECT
	claim_occurrence_dim_id,
	edw_claim_occurrence_ak_id,
	eff_from_date,
	eff_to_date
	FROM (
		SELECT 
			claim_occurrence_dim_id,
			edw_claim_occurrence_ak_id,
			eff_from_date,
			eff_to_date
		FROM claim_occurrence_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_occurrence_dim_id DESC) = 1
),
mplt_PolicyDimID_StopLossCOFact AS (WITH
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
EXP_CalValues_Occurrence AS (
	SELECT
	EXP_GetValues_Occurrence.EDWAgencyAKID,
	EXP_GetValues_Occurrence.AgencyCode,
	EXP_GetValues_Occurrence.Rundate AS i_RunDate,
	LKP_WorkStopLossLimit_Occurrence.StopLossLimit AS lkp_StopLossLimit,
	LKP_WorkStopLossLimit_Occurrence_Primary.StopLossLimit AS lkp_StopLossLimit_2,
	EXP_LegalPrimaryagency1.o_LegalPrimaryAgencyCode AS LKP_LegalPrimaryAgencyCode,
	LKP_WorkStopLossLimit_Occurrence.AgencyPreviousYearDirectWrittenPremium AS lkp_AgencyPreviousYearDirectWrittenPremium,
	LKP_WorkStopLossLimit_Occurrence_Primary.AgencyPreviousYearDirectWrittenPremium AS lkp_AgencyPreviousYearDirectWrittenPremium2,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	LKP_V3_PrimaryAgencyDimID_Primary.SalesDivisionDimId AS IN_SalesDivisionDimID,
	-- *INF*: IIF(ISNULL(IN_SalesDivisionDimID),-1,IN_SalesDivisionDimID)
	IFF(IN_SalesDivisionDimID IS NULL, - 1, IN_SalesDivisionDimID) AS SalesDivisionDimID,
	mplt_PolicyDimID_StopLossCOFact.agency_dim_id AS i_agency_dim_id,
	-- *INF*: IIF(ISNULL(i_agency_dim_id), -1, i_agency_dim_id)
	IFF(i_agency_dim_id IS NULL, - 1, i_agency_dim_id) AS o_agency_dim_id,
	mplt_PolicyDimID_StopLossCOFact.contract_cust_dim_id AS i_contract_cust_dim_id,
	-- *INF*: IIF(ISNULL(i_contract_cust_dim_id), -1, i_contract_cust_dim_id)
	IFF(i_contract_cust_dim_id IS NULL, - 1, i_contract_cust_dim_id) AS o_contract_cust_dim_id,
	mplt_PolicyDimID_StopLossCOFact.pol_dim_id,
	LKP_claim_occurrence_dim.claim_occurrence_dim_id,
	-- *INF*: :LKP.LKP_CALENDER_DIM(SET_DATE_PART(SET_DATE_PART(SET_DATE_PART(i_RunDate,'HH',0),'MI',0),'SS',0))
	LKP_CALENDER_DIM_SET_DATE_PART_SET_DATE_PART_SET_DATE_PART_i_RunDate_HH_0_MI_0_SS_0.clndr_id AS o_RunDateId,
	EXP_GetValues_Occurrence.total_direct_loss_recovery_incurred AS TotalDirectIncurredLoss,
	EXP_GetValues_Occurrence.o_Loss_Year AS IN_LOSS_YEAR,
	EXP_GetValues_Occurrence.o_Loss_Month AS IN_LOSS_MONTH,
	-- *INF*: Decode(TRUE,IN(AgencyCode,'12176','13176'),
	-- DECODE(TRUE,IN_LOSS_YEAR<'1995',100000,
	-- IN_LOSS_YEAR='1995',125000,
	-- IN(IN_LOSS_YEAR,'1996','1997'),200000,
	-- IN(IN_LOSS_YEAR,'1998','1999','2000','2001'),400000,
	-- IN_LOSS_YEAR>'2001',1000000),
	-- IN(AgencyCode,'12651','13651'),
	-- DECODE(TRUE,IN_LOSS_YEAR<'1995',100000,
	-- IN_LOSS_YEAR='1995',125000,
	-- IN(IN_LOSS_YEAR,'1996','1997','1998','1999','2000','2001','2002','2003'),200000,
	-- IN_LOSS_YEAR='2005' and IN(IN_LOSS_MONTH,'04','05','06','07','08','09','10','11','12'),500000,lkp_StopLossLimit),
	-- Decode(TRUE,IN_LOSS_YEAR<'1995',100000,
	-- IN_LOSS_YEAR='1995',125000,
	-- IN(IN_LOSS_YEAR,'1996','1997','1998','1999','2000','2001','2002','2003'),200000,
	-- IN_LOSS_YEAR>='2015' AND lkp_AgencyPreviousYearDirectWrittenPremium<=20000000,500000,
	-- IN_LOSS_YEAR>='2015' AND IN_LOSS_YEAR < '2022' AND lkp_AgencyPreviousYearDirectWrittenPremium>20000000,500000,
	-- IN_LOSS_YEAR>='2022' AND lkp_AgencyPreviousYearDirectWrittenPremium>20000000,1000000,
	-- lkp_StopLossLimit))
	-- --The above decode statement has been implemented to catch the stoplosslimit as per the mainframe code WB11002B
	Decode(TRUE,
	IN(AgencyCode, '12176', '13176'), DECODE(TRUE,
	IN_LOSS_YEAR < '1995', 100000,
	IN_LOSS_YEAR = '1995', 125000,
	IN(IN_LOSS_YEAR, '1996', '1997'), 200000,
	IN(IN_LOSS_YEAR, '1998', '1999', '2000', '2001'), 400000,
	IN_LOSS_YEAR > '2001', 1000000),
	IN(AgencyCode, '12651', '13651'), DECODE(TRUE,
	IN_LOSS_YEAR < '1995', 100000,
	IN_LOSS_YEAR = '1995', 125000,
	IN(IN_LOSS_YEAR, '1996', '1997', '1998', '1999', '2000', '2001', '2002', '2003'), 200000,
	IN_LOSS_YEAR = '2005' AND IN(IN_LOSS_MONTH, '04', '05', '06', '07', '08', '09', '10', '11', '12'), 500000,
	lkp_StopLossLimit),
	Decode(TRUE,
	IN_LOSS_YEAR < '1995', 100000,
	IN_LOSS_YEAR = '1995', 125000,
	IN(IN_LOSS_YEAR, '1996', '1997', '1998', '1999', '2000', '2001', '2002', '2003'), 200000,
	IN_LOSS_YEAR >= '2015' AND lkp_AgencyPreviousYearDirectWrittenPremium <= 20000000, 500000,
	IN_LOSS_YEAR >= '2015' AND IN_LOSS_YEAR < '2022' AND lkp_AgencyPreviousYearDirectWrittenPremium > 20000000, 500000,
	IN_LOSS_YEAR >= '2022' AND lkp_AgencyPreviousYearDirectWrittenPremium > 20000000, 1000000,
	lkp_StopLossLimit)) AS V_Stop_LOSS_LIMIT,
	-- *INF*: Decode(TRUE,IN(AgencyCode,'12176','13176'),
	-- DECODE(TRUE,IN_LOSS_YEAR<'1995',100000,
	-- IN_LOSS_YEAR='1995',125000,
	-- IN(IN_LOSS_YEAR,'1996','1997'),200000,
	-- IN(IN_LOSS_YEAR,'1998','1999','2000','2001'),400000,
	-- IN_LOSS_YEAR>'2001',1000000),
	-- IN(AgencyCode,'12651','13651'),
	-- DECODE(TRUE,IN_LOSS_YEAR<'1995',100000,
	-- IN_LOSS_YEAR='1995',125000,
	-- IN(IN_LOSS_YEAR,'1996','1997','1998','1999','2000','2001','2002','2003'),200000,
	-- IN_LOSS_YEAR='2005' and IN(IN_LOSS_MONTH,'04','05','06','07','08','09','10','11','12'),500000,lkp_StopLossLimit),
	-- Decode(TRUE,IN_LOSS_YEAR<'1995',100000,
	-- IN_LOSS_YEAR='1995',125000,
	-- IN(IN_LOSS_YEAR,'1996','1997','1998','1999','2000','2001','2002','2003'),200000,
	-- IN_LOSS_YEAR>='2015' AND lkp_AgencyPreviousYearDirectWrittenPremium<=20000000,500000,
	-- IN_LOSS_YEAR>='2015' AND IN_LOSS_YEAR < '2022' AND lkp_AgencyPreviousYearDirectWrittenPremium>20000000,500000,
	-- IN_LOSS_YEAR>='2022' AND lkp_AgencyPreviousYearDirectWrittenPremium>20000000,1000000,
	-- lkp_StopLossLimit))
	Decode(TRUE,
	IN(AgencyCode, '12176', '13176'), DECODE(TRUE,
	IN_LOSS_YEAR < '1995', 100000,
	IN_LOSS_YEAR = '1995', 125000,
	IN(IN_LOSS_YEAR, '1996', '1997'), 200000,
	IN(IN_LOSS_YEAR, '1998', '1999', '2000', '2001'), 400000,
	IN_LOSS_YEAR > '2001', 1000000),
	IN(AgencyCode, '12651', '13651'), DECODE(TRUE,
	IN_LOSS_YEAR < '1995', 100000,
	IN_LOSS_YEAR = '1995', 125000,
	IN(IN_LOSS_YEAR, '1996', '1997', '1998', '1999', '2000', '2001', '2002', '2003'), 200000,
	IN_LOSS_YEAR = '2005' AND IN(IN_LOSS_MONTH, '04', '05', '06', '07', '08', '09', '10', '11', '12'), 500000,
	lkp_StopLossLimit),
	Decode(TRUE,
	IN_LOSS_YEAR < '1995', 100000,
	IN_LOSS_YEAR = '1995', 125000,
	IN(IN_LOSS_YEAR, '1996', '1997', '1998', '1999', '2000', '2001', '2002', '2003'), 200000,
	IN_LOSS_YEAR >= '2015' AND lkp_AgencyPreviousYearDirectWrittenPremium <= 20000000, 500000,
	IN_LOSS_YEAR >= '2015' AND IN_LOSS_YEAR < '2022' AND lkp_AgencyPreviousYearDirectWrittenPremium > 20000000, 500000,
	IN_LOSS_YEAR >= '2022' AND lkp_AgencyPreviousYearDirectWrittenPremium > 20000000, 1000000,
	lkp_StopLossLimit)) AS v_Stop_Loss_Limit2,
	-- *INF*: IIF(ISNULL(V_Stop_LOSS_LIMIT) or V_Stop_LOSS_LIMIT=0.0,200000,V_Stop_LOSS_LIMIT)
	-- 
	-- --As per the Cobol code, if a given Agency does not have stop loss limit then we assign 200000 as the default stop loss limit.
	IFF(V_Stop_LOSS_LIMIT IS NULL OR V_Stop_LOSS_LIMIT = 0.0, 200000, V_Stop_LOSS_LIMIT) AS V_Final_Stop_Loss_Limit,
	-- *INF*: IIF(ISNULL(v_Stop_Loss_Limit2) or v_Stop_Loss_Limit2=0.0,200000,v_Stop_Loss_Limit2)
	IFF(v_Stop_Loss_Limit2 IS NULL OR v_Stop_Loss_Limit2 = 0.0, 200000, v_Stop_Loss_Limit2) AS V_Final_Stop_Loss_Limit2,
	V_Final_Stop_Loss_Limit AS o_StopLossLimit,
	-- *INF*: IIF(TotalDirectIncurredLoss<=V_Final_Stop_Loss_Limit , 0.0 , TotalDirectIncurredLoss-V_Final_Stop_Loss_Limit)
	IFF(TotalDirectIncurredLoss <= V_Final_Stop_Loss_Limit, 0.0, TotalDirectIncurredLoss - V_Final_Stop_Loss_Limit) AS V_StopLossAdjustmentAmount,
	-- *INF*: IIF(V_StopLossAdjustmentAmount>0.0,V_StopLossAdjustmentAmount,0.0)
	IFF(V_StopLossAdjustmentAmount > 0.0, V_StopLossAdjustmentAmount, 0.0) AS o_StopLossAdjustmentAmount,
	-- *INF*: IIF(ISNULL(:LKP.LKP_STOPLOSSADJUSTMENTCLAIMOCCURRENCEFACT_PREVIOUSINSTANCE(EDWAgencyAKID,edw_pol_ak_id,edw_claim_occurrence_ak_id)),0.0,:LKP.LKP_STOPLOSSADJUSTMENTCLAIMOCCURRENCEFACT_PREVIOUSINSTANCE(EDWAgencyAKID,edw_pol_ak_id,edw_claim_occurrence_ak_id))
	IFF(LKP_STOPLOSSADJUSTMENTCLAIMOCCURRENCEFACT_PREVIOUSINSTANCE_EDWAgencyAKID_edw_pol_ak_id_edw_claim_occurrence_ak_id.StopLossAdjustmentAmount IS NULL, 0.0, LKP_STOPLOSSADJUSTMENTCLAIMOCCURRENCEFACT_PREVIOUSINSTANCE_EDWAgencyAKID_edw_pol_ak_id_edw_claim_occurrence_ak_id.StopLossAdjustmentAmount) AS Lkp_Previous_StopLossAdjustmentAmount,
	V_StopLossAdjustmentAmount-Lkp_Previous_StopLossAdjustmentAmount AS O_ChangeInStopLossAdjustmentAmount,
	EXP_GetValues_Occurrence.edw_pol_ak_id,
	EXP_GetValues_Occurrence.edw_claim_occurrence_ak_id,
	LKP_V3_PrimaryAgencyDimID_Primary.agency_dim_id AS IN_PrimaryAgencyDimId,
	-- *INF*: IIF(ISNULL(IN_PrimaryAgencyDimId),-1,IN_PrimaryAgencyDimId)
	IFF(IN_PrimaryAgencyDimId IS NULL, - 1, IN_PrimaryAgencyDimId) AS PrimaryAgencyDimId
	FROM EXP_GetValues_Occurrence
	 -- Manually join with EXP_LegalPrimaryagency1
	 -- Manually join with mplt_PolicyDimID_StopLossCOFact
	LEFT JOIN LKP_V3_PrimaryAgencyDimID_Primary
	ON LKP_V3_PrimaryAgencyDimID_Primary.Agencycode = EXP_LegalPrimaryagency.o_LegalPrimaryAgencyCode AND LKP_V3_PrimaryAgencyDimID_Primary.eff_from_date <= LKP_V3_PrimaryAgencyDimID_CO.IN_Trans_Date AND LKP_V3_PrimaryAgencyDimID_Primary.eff_to_date >= LKP_V3_PrimaryAgencyDimID_CO.IN_Trans_Date
	LEFT JOIN LKP_WorkStopLossLimit_Occurrence
	ON LKP_WorkStopLossLimit_Occurrence.AgencyCode = EXP_GetValues_Occurrence.AgencyCode AND LKP_WorkStopLossLimit_Occurrence.Loss_Year = EXP_GetValues_Occurrence.o_Loss_Year
	LEFT JOIN LKP_WorkStopLossLimit_Occurrence_Primary
	ON LKP_WorkStopLossLimit_Occurrence_Primary.PrimaryAgencyCode = EXP_LegalPrimaryagency1.o_LegalPrimaryAgencyCode AND LKP_WorkStopLossLimit_Occurrence_Primary.Loss_Year = EXP_GetValues_Occurrence.o_Loss_Year
	LEFT JOIN LKP_claim_occurrence_dim
	ON LKP_claim_occurrence_dim.edw_claim_occurrence_ak_id = EXP_GetValues_Occurrence.edw_claim_occurrence_ak_id AND LKP_claim_occurrence_dim.eff_from_date <= EXP_GetValues_Occurrence.Rundate AND LKP_claim_occurrence_dim.eff_to_date >= EXP_GetValues_Occurrence.Rundate
	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_SET_DATE_PART_SET_DATE_PART_SET_DATE_PART_i_RunDate_HH_0_MI_0_SS_0
	ON LKP_CALENDER_DIM_SET_DATE_PART_SET_DATE_PART_SET_DATE_PART_i_RunDate_HH_0_MI_0_SS_0.clndr_date = SET_DATE_PART(SET_DATE_PART(SET_DATE_PART(i_RunDate, 'HH', 0), 'MI', 0), 'SS', 0)

	LEFT JOIN LKP_STOPLOSSADJUSTMENTCLAIMOCCURRENCEFACT_PREVIOUSINSTANCE LKP_STOPLOSSADJUSTMENTCLAIMOCCURRENCEFACT_PREVIOUSINSTANCE_EDWAgencyAKID_edw_pol_ak_id_edw_claim_occurrence_ak_id
	ON LKP_STOPLOSSADJUSTMENTCLAIMOCCURRENCEFACT_PREVIOUSINSTANCE_EDWAgencyAKID_edw_pol_ak_id_edw_claim_occurrence_ak_id.EDWAgencyAKID = EDWAgencyAKID
	AND LKP_STOPLOSSADJUSTMENTCLAIMOCCURRENCEFACT_PREVIOUSINSTANCE_EDWAgencyAKID_edw_pol_ak_id_edw_claim_occurrence_ak_id.edw_pol_ak_id = edw_pol_ak_id
	AND LKP_STOPLOSSADJUSTMENTCLAIMOCCURRENCEFACT_PREVIOUSINSTANCE_EDWAgencyAKID_edw_pol_ak_id_edw_claim_occurrence_ak_id.edw_claim_occurrence_ak_id = edw_claim_occurrence_ak_id

),
FIL_StopLossAdjustmentAmount_Occurence AS (
	SELECT
	o_AuditId AS AuditId, 
	SalesDivisionDimID AS SalesDivisionDimId, 
	o_agency_dim_id AS AgencyDimId, 
	o_contract_cust_dim_id AS contract_cust_dim_id, 
	pol_dim_id, 
	claim_occurrence_dim_id, 
	o_RunDateId AS RunDateId, 
	TotalDirectIncurredLoss, 
	o_StopLossLimit AS StopLossLimit, 
	o_StopLossAdjustmentAmount AS StopLossAdjustmentAmount, 
	O_ChangeInStopLossAdjustmentAmount, 
	PrimaryAgencyDimId
	FROM EXP_CalValues_Occurrence
	WHERE IIF(O_ChangeInStopLossAdjustmentAmount=0.0 and StopLossAdjustmentAmount<>0.0,'Y',
IIF(O_ChangeInStopLossAdjustmentAmount<>0.0,'Y','N'))='Y' --AND StopLossLimit>0

--O_ChangeInStopLossAdjustmentAmount<>0 OR StopLossAdjustmentAmount<>0.0

--O_ChangeInStopLossAdjustmentAmount<>0.0 AND StopLossLimit>0
),
LKP_StopLossAdjustmentClaimOccurrenceFact AS (
	SELECT
	StopLossAdjustmentClaimOccurrenceFactId,
	SalesDivisionDimID,
	AgencyDimId,
	ContractCustomerDimId,
	PolicyDimid,
	ClaimOccurrenceDimId,
	RunDateId
	FROM (
		SELECT a.SalesDivisionDimID                      AS SalesDivisionDimID,
		       a.AgencyDimId                             AS AgencyDimId,
		       a.ContractCustomerDimId                   AS ContractCustomerDimId,
		       a.PolicyDimid                             AS PolicyDimid,
		       a.ClaimOccurrenceDimId                    AS ClaimOccurrenceDimId,
		       a.RunDateId                               AS RunDateId,
		       a.StopLossAdjustmentClaimOccurrenceFactId AS StopLossAdjustmentClaimOccurrenceFactId
		FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.StopLossAdjustmentclaimoccurrenceFact a,
		       @{pipeline().parameters.TARGET_TABLE_OWNER}.calendar_dim cd
		WHERE  a.RunDateId = cd.clndr_id
		       AND cd.clndr_yr = Year(dateadd(dd,-1,dateadd(mm,DATEDIFF(MM,0,getdate())-@{pipeline().parameters.NO_OF_MONTHS},0)))
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SalesDivisionDimID,AgencyDimId,ContractCustomerDimId,PolicyDimid,ClaimOccurrenceDimId,RunDateId ORDER BY StopLossAdjustmentClaimOccurrenceFactId DESC) = 1
),
RTR_Insert_Update_Occurrence AS (
	SELECT
	LKP_StopLossAdjustmentClaimOccurrenceFact.StopLossAdjustmentClaimOccurrenceFactId,
	FIL_StopLossAdjustmentAmount_Occurence.AuditId,
	FIL_StopLossAdjustmentAmount_Occurence.SalesDivisionDimId AS SalesDivisionDimID,
	FIL_StopLossAdjustmentAmount_Occurence.AgencyDimId,
	FIL_StopLossAdjustmentAmount_Occurence.PrimaryAgencyDimId,
	FIL_StopLossAdjustmentAmount_Occurence.contract_cust_dim_id AS ContractCustomerDimId,
	FIL_StopLossAdjustmentAmount_Occurence.pol_dim_id AS PolicyDimid,
	FIL_StopLossAdjustmentAmount_Occurence.claim_occurrence_dim_id AS ClaimOccurrenceDimId,
	FIL_StopLossAdjustmentAmount_Occurence.RunDateId,
	FIL_StopLossAdjustmentAmount_Occurence.TotalDirectIncurredLoss AS DirectIncurredLossInceptionToDate,
	FIL_StopLossAdjustmentAmount_Occurence.StopLossLimit,
	FIL_StopLossAdjustmentAmount_Occurence.StopLossAdjustmentAmount,
	FIL_StopLossAdjustmentAmount_Occurence.O_ChangeInStopLossAdjustmentAmount
	FROM FIL_StopLossAdjustmentAmount_Occurence
	LEFT JOIN LKP_StopLossAdjustmentClaimOccurrenceFact
	ON LKP_StopLossAdjustmentClaimOccurrenceFact.SalesDivisionDimID = FIL_StopLossAdjustmentAmount_Occurence.SalesDivisionDimId AND LKP_StopLossAdjustmentClaimOccurrenceFact.AgencyDimId = FIL_StopLossAdjustmentAmount_Occurence.AgencyDimId AND LKP_StopLossAdjustmentClaimOccurrenceFact.ContractCustomerDimId = FIL_StopLossAdjustmentAmount_Occurence.contract_cust_dim_id AND LKP_StopLossAdjustmentClaimOccurrenceFact.PolicyDimid = FIL_StopLossAdjustmentAmount_Occurence.pol_dim_id AND LKP_StopLossAdjustmentClaimOccurrenceFact.ClaimOccurrenceDimId = FIL_StopLossAdjustmentAmount_Occurence.claim_occurrence_dim_id AND LKP_StopLossAdjustmentClaimOccurrenceFact.RunDateId = FIL_StopLossAdjustmentAmount_Occurence.RunDateId
),
RTR_Insert_Update_Occurrence_INSERT AS (SELECT * FROM RTR_Insert_Update_Occurrence WHERE ISNULL(StopLossAdjustmentClaimOccurrenceFactId)),
RTR_Insert_Update_Occurrence_DEFAULT1 AS (SELECT * FROM RTR_Insert_Update_Occurrence WHERE NOT ( (ISNULL(StopLossAdjustmentClaimOccurrenceFactId)) )),
UPD_Occurrence AS (
	SELECT
	StopLossAdjustmentClaimOccurrenceFactId, 
	AuditId, 
	SalesDivisionDimID, 
	AgencyDimId, 
	PrimaryAgencyDimId AS PrimaryAgencyDimId2, 
	ContractCustomerDimId, 
	PolicyDimid, 
	ClaimOccurrenceDimId, 
	RunDateId, 
	DirectIncurredLossInceptionToDate, 
	StopLossLimit, 
	StopLossAdjustmentAmount, 
	O_ChangeInStopLossAdjustmentAmount AS O_ChangeInStopLossAdjustmentAmount2
	FROM RTR_Insert_Update_Occurrence_DEFAULT1
),
TGT_StopLossAdjustmentClaimOccurrenceFact_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.StopLossAdjustmentClaimOccurrenceFact AS T
	USING UPD_Occurrence AS S
	ON T.StopLossAdjustmentClaimOccurrenceFactId = S.StopLossAdjustmentClaimOccurrenceFactId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AuditId, T.SalesDivisionDimId = S.SalesDivisionDimID, T.AgencyDimId = S.AgencyDimId, T.PrimaryAgencyDimId = S.PrimaryAgencyDimId2, T.ContractCustomerDimId = S.ContractCustomerDimId, T.PolicyDimid = S.PolicyDimid, T.ClaimOccurrenceDimId = S.ClaimOccurrenceDimId, T.RunDateId = S.RunDateId, T.DirectIncurredLossInceptionToDate = S.DirectIncurredLossInceptionToDate, T.StopLossLimit = S.StopLossLimit, T.StopLossAdjustmentAmount = S.StopLossAdjustmentAmount, T.ChangeInStopLossAdjustmentAmount = S.O_ChangeInStopLossAdjustmentAmount2
),
TGT_StopLossAdjustmentClaimOccurrenceFact_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.StopLossAdjustmentClaimOccurrenceFact
	(AuditId, SalesDivisionDimId, AgencyDimId, PrimaryAgencyDimId, ContractCustomerDimId, PolicyDimid, ClaimOccurrenceDimId, RunDateId, DirectIncurredLossInceptionToDate, StopLossLimit, StopLossAdjustmentAmount, ChangeInStopLossAdjustmentAmount)
	SELECT 
	AUDITID, 
	SalesDivisionDimID AS SALESDIVISIONDIMID, 
	AGENCYDIMID, 
	PRIMARYAGENCYDIMID, 
	CONTRACTCUSTOMERDIMID, 
	POLICYDIMID, 
	CLAIMOCCURRENCEDIMID, 
	RUNDATEID, 
	DIRECTINCURREDLOSSINCEPTIONTODATE, 
	STOPLOSSLIMIT, 
	STOPLOSSADJUSTMENTAMOUNT, 
	O_ChangeInStopLossAdjustmentAmount AS CHANGEINSTOPLOSSADJUSTMENTAMOUNT
	FROM RTR_Insert_Update_Occurrence_INSERT
),
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
SQ_claim_loss_transaction_fact_Occurrence_CR AS (
	DECLARE @DATE1 as datetime,
	@DATE2 as datetime
	
	SET @DATE1 = dateadd(dd,-1,dateadd(mm,DATEDIFF(MM,0,getdate())-@{pipeline().parameters.NO_OF_MONTHS_CR},0))
	SET @DATE2 = dateadd(SS,-1,dateadd(mm,DATEDIFF(MM,0,getdate())-@{pipeline().parameters.NO_OF_MONTHS_CR},0))
	
	select EDWAgencyAKID,
	A.AgencyCode,
	edw_pol_ak_id,
	A.edw_claim_occurrence_ak_id,
	sum(case when claim_rpted_date<='2013-12-31 00:00:00' then (DirectLossPaidIR+DirectLossOutstandingER+DirectALAEPaidIR) 
	else (DirectLossPaidIR+DirectLossOutstandingER+DirectALAEPaidIR+DirectALAEOutstandingIR) end) total_direct_loss_recovery_incurred,
	convert(varchar(6),claim_loss_date,112) Loss_Year,A.claim_cat_code,CatastropheDimId,
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
	OCC.claim_loss_date,occ.claim_cat_code
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
	OCC.claim_loss_date,occ.claim_cat_code
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
	inner join (SELECT claim_cat_code,edw_claim_occurrence_ak_id  from claim_occurrence_dim
	WHERE  @DATE2 BETWEEN eff_from_date AND eff_to_date) B
	on A.edw_claim_occurrence_ak_id = B.edw_claim_occurrence_ak_id
	and RunDate <=@DATE2
	and B.claim_cat_code<>'N/A'
	and A.claim_cat_code<>'N/A'
	inner join (select C.AgencyCode,D.CatastropheCode,A.CatastropheDimId
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.StopLossAdjustmentCatastropheFact A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim B
	on A.RunDateId=B.clndr_id
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.Agencydim C
	on A.PrimaryAgencyDimId=C.AgencyDimID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CatastropheDim D
	on A.CatastropheDimId=D.CatastropheDimId
	where B.clndr_date=@DATE1) C
	on A.AgencyCode=C.AgencyCode
	and A.claim_cat_code=C.CatastropheCode
	@{pipeline().parameters.WHERECLAUSE_CR}
	group by EDWAgencyAKID,
	A.AgencyCode,
	edw_pol_ak_id,
	A.edw_claim_occurrence_ak_id,
	convert(varchar(6),claim_loss_date,112),A.claim_cat_code,CatastropheDimId
),
SQ_claim_loss_transaction_fact_Occurrence_PR AS (
	DECLARE @DATE1 as datetime,
	@DATE2 as datetime
	
	SET @DATE1 = dateadd(dd,-1,dateadd(mm,DATEDIFF(MM,0,getdate())-@{pipeline().parameters.NO_OF_MONTHS_CR},0))
	SET @DATE2 = dateadd(SS,-1,dateadd(yy,DATEDIFF(MM,0,dateadd(SS,-1,dateadd(mm,DATEDIFF(MM,0,GETDATE())-@{pipeline().parameters.NO_OF_MONTHS_CR},0)))/12-0,0))
	
	select EDWAgencyAKID,
	A.AgencyCode,
	edw_pol_ak_id,
	A.edw_claim_occurrence_ak_id,
	sum(case when claim_rpted_date<='2013-12-31 00:00:00' then (DirectLossPaidIR+DirectLossOutstandingER+DirectALAEPaidIR) 
	else (DirectLossPaidIR+DirectLossOutstandingER+DirectALAEPaidIR+DirectALAEOutstandingIR) end) total_direct_loss_recovery_incurred,
	convert(varchar(6),claim_loss_date,112) Loss_Year,A.claim_cat_code,CatastropheDimId,
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
	OCC.claim_loss_date,occ.claim_cat_code
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
	AND CASE when (PolicyOfferingCode in ('600') 
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
	OCC.claim_loss_date,occ.claim_cat_code
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
	inner join (SELECT claim_cat_code,edw_claim_occurrence_ak_id  from claim_occurrence_dim
	WHERE  @DATE2 BETWEEN eff_from_date AND eff_to_date) B
	on A.edw_claim_occurrence_ak_id = B.edw_claim_occurrence_ak_id
	and RunDate <=@DATE2
	and B.claim_cat_code<>'N/A'
	and A.claim_cat_code<>'N/A'
	inner join (select C.AgencyCode,D.CatastropheCode,A.CatastropheDimId
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.StopLossAdjustmentCatastropheFact A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim B
	on A.RunDateId=B.clndr_id
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.Agencydim C
	on A.PrimaryAgencyDimId=C.AgencyDimID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CatastropheDim D
	on A.CatastropheDimId=D.CatastropheDimId
	where B.clndr_date=@DATE1) C
	on A.AgencyCode=C.AgencyCode
	and A.claim_cat_code=C.CatastropheCode
	@{pipeline().parameters.WHERECLAUSE_PR}
	group by EDWAgencyAKID,
	A.AgencyCode,
	edw_pol_ak_id,
	A.edw_claim_occurrence_ak_id,
	convert(varchar(6),claim_loss_date,112),A.claim_cat_code,CatastropheDimId
),
JNR_CurrentRunMonth_PreviousYear AS (SELECT
	SQ_claim_loss_transaction_fact_Occurrence_CR.EDWAgencyAKID AS EDWAgencyAKID_CR, 
	SQ_claim_loss_transaction_fact_Occurrence_CR.AgencyCode AS AgencyCode_CR, 
	SQ_claim_loss_transaction_fact_Occurrence_CR.edw_pol_ak_id AS edw_pol_ak_id_CR, 
	SQ_claim_loss_transaction_fact_Occurrence_CR.edw_claim_occurrence_ak_id AS edw_claim_occurrence_ak_id_CR, 
	SQ_claim_loss_transaction_fact_Occurrence_CR.total_direct_loss_recovery_incurred AS total_direct_loss_recovery_incurred_CR, 
	SQ_claim_loss_transaction_fact_Occurrence_CR.Loss_Year AS Loss_Year_CR, 
	SQ_claim_loss_transaction_fact_Occurrence_CR.claim_cat_code AS claim_cat_code_CR, 
	SQ_claim_loss_transaction_fact_Occurrence_CR.CatastropheDimId AS CatastropheDimId_CR, 
	SQ_claim_loss_transaction_fact_Occurrence_CR.Rundate AS Rundate_CR, 
	SQ_claim_loss_transaction_fact_Occurrence_PR.EDWAgencyAKID AS EDWAgencyAKID_PR, 
	SQ_claim_loss_transaction_fact_Occurrence_PR.AgencyCode AS AgencyCode_PR, 
	SQ_claim_loss_transaction_fact_Occurrence_PR.edw_pol_ak_id AS edw_pol_ak_id_PR, 
	SQ_claim_loss_transaction_fact_Occurrence_PR.edw_claim_occurrence_ak_id AS edw_claim_occurrence_ak_id_PR, 
	SQ_claim_loss_transaction_fact_Occurrence_PR.total_direct_loss_recovery_incurred AS total_direct_loss_recovery_incurred_PR, 
	SQ_claim_loss_transaction_fact_Occurrence_PR.Loss_Year AS Loss_Year_PR, 
	SQ_claim_loss_transaction_fact_Occurrence_PR.claim_cat_code AS claim_cat_code_PR, 
	SQ_claim_loss_transaction_fact_Occurrence_PR.CatastropheDimId AS CatastropheDimId_PR, 
	SQ_claim_loss_transaction_fact_Occurrence_PR.Rundate AS Rundate_PR
	FROM SQ_claim_loss_transaction_fact_Occurrence_PR
	RIGHT OUTER JOIN SQ_claim_loss_transaction_fact_Occurrence_CR
	ON SQ_claim_loss_transaction_fact_Occurrence_CR.EDWAgencyAKID = SQ_claim_loss_transaction_fact_Occurrence_PR.EDWAgencyAKID AND SQ_claim_loss_transaction_fact_Occurrence_CR.edw_pol_ak_id = SQ_claim_loss_transaction_fact_Occurrence_PR.edw_pol_ak_id AND SQ_claim_loss_transaction_fact_Occurrence_CR.edw_claim_occurrence_ak_id = SQ_claim_loss_transaction_fact_Occurrence_PR.edw_claim_occurrence_ak_id AND SQ_claim_loss_transaction_fact_Occurrence_CR.claim_cat_code = SQ_claim_loss_transaction_fact_Occurrence_PR.claim_cat_code
),
EXP_GetValues_Occurrence AS (
	SELECT
	EDWAgencyAKID_CR AS EDWAgencyAKID,
	AgencyCode_CR AS AgencyCode,
	edw_pol_ak_id_CR AS edw_pol_ak_id,
	edw_claim_occurrence_ak_id_CR AS edw_claim_occurrence_ak_id,
	total_direct_loss_recovery_incurred_CR AS total_direct_loss_recovery_incurred,
	Loss_Year_CR AS Loss_Year,
	-- *INF*: SUBSTR(Loss_Year,1,4)
	SUBSTR(Loss_Year, 1, 4
	) AS O_Loss_Year,
	claim_cat_code_CR,
	CatastropheDimId_CR,
	Rundate_CR AS Rundate,
	total_direct_loss_recovery_incurred_PR,
	-- *INF*: IIF(ISNULL(total_direct_loss_recovery_incurred_PR),0.0,total_direct_loss_recovery_incurred_PR)
	IFF(total_direct_loss_recovery_incurred_PR IS NULL,
		0.0,
		total_direct_loss_recovery_incurred_PR
	) AS V_total_direct_loss_recovery_incurred_PR
	FROM JNR_CurrentRunMonth_PreviousYear
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
EXP_LegalPrimaryAgency AS (
	SELECT
	EXP_GetValues_Occurrence.AgencyCode AS IN_AgencyCode,
	LKP_V3_PrimaryAgencyDimID_CO.LegalPrimaryAgencyCode,
	-- *INF*: iif(isnull(LegalPrimaryAgencyCode),IN_AgencyCode,LegalPrimaryAgencyCode)
	IFF(LegalPrimaryAgencyCode IS NULL,
		IN_AgencyCode,
		LegalPrimaryAgencyCode
	) AS o_LegalPrimaryAgencyCode
	FROM EXP_GetValues_Occurrence
	LEFT JOIN LKP_V3_PrimaryAgencyDimID_CO
	ON LKP_V3_PrimaryAgencyDimID_CO.Agencycode = EXP_GetValues_Occurrence.AgencyCode AND LKP_V3_PrimaryAgencyDimID_CO.eff_from_date <= EXP_GetValues_Occurrence.Rundate AND LKP_V3_PrimaryAgencyDimID_CO.eff_to_date >= EXP_GetValues_Occurrence.Rundate
),
LKP_V3_PrimaryAgencyDimID_CO_Primary AS (
	SELECT
	agency_dim_id,
	Agencycode,
	eff_from_date,
	eff_to_date
	FROM (
		SELECT AgencyDim.AgencyDimID as agency_dim_id, 
		AgencyDim.AgencyCode as AgencyCode, 
		AgencyDim.EffectiveDate as eff_from_date, 
		AgencyDim.ExpirationDate as eff_to_date
		 FROM @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim as AgencyDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Agencycode,eff_from_date,eff_to_date ORDER BY agency_dim_id DESC) = 1
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
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	mplt_PolicyDimID_StopLossCOFact.agency_dim_id AS i_agency_dim_id,
	-- *INF*: IIF(ISNULL(i_agency_dim_id), -1, i_agency_dim_id)
	IFF(i_agency_dim_id IS NULL,
		- 1,
		i_agency_dim_id
	) AS o_agency_dim_id,
	mplt_PolicyDimID_StopLossCOFact.contract_cust_dim_id AS i_contract_cust_dim_id,
	-- *INF*: IIF(ISNULL(i_contract_cust_dim_id), -1, i_contract_cust_dim_id)
	IFF(i_contract_cust_dim_id IS NULL,
		- 1,
		i_contract_cust_dim_id
	) AS o_contract_cust_dim_id,
	mplt_PolicyDimID_StopLossCOFact.pol_dim_id,
	LKP_claim_occurrence_dim.claim_occurrence_dim_id,
	-- *INF*: :LKP.LKP_CALENDER_DIM(SET_DATE_PART(SET_DATE_PART(SET_DATE_PART(i_RunDate,'HH',0),'MI',0),'SS',0))
	LKP_CALENDER_DIM_SET_DATE_PART_SET_DATE_PART_SET_DATE_PART_i_RunDate_HH_0_MI_0_SS_0.clndr_id AS o_RunDateId,
	EXP_GetValues_Occurrence.total_direct_loss_recovery_incurred AS TotalDirectIncurredLoss,
	EXP_GetValues_Occurrence.V_total_direct_loss_recovery_incurred_PR AS total_direct_loss_recovery_incurred_PR,
	-- *INF*: IIF(isnull(total_direct_loss_recovery_incurred_PR),0.0,total_direct_loss_recovery_incurred_PR)
	IFF(total_direct_loss_recovery_incurred_PR IS NULL,
		0.0,
		total_direct_loss_recovery_incurred_PR
	) AS v_total_direct_loss_recovery_incurred_PR,
	EXP_GetValues_Occurrence.Loss_Year AS IN_LOSS_YEAR,
	0.0 AS o_StopLossLimit,
	0.0 AS o_StopLossAdjustmentAmount,
	TotalDirectIncurredLoss-v_total_direct_loss_recovery_incurred_PR AS ChangeInIncurredLoss,
	EXP_GetValues_Occurrence.edw_pol_ak_id,
	EXP_GetValues_Occurrence.edw_claim_occurrence_ak_id,
	LKP_V3_PrimaryAgencyDimID_CO_Primary.agency_dim_id AS IN_PrimaryAgencyDimId,
	-- *INF*: IIF(ISNULL(IN_PrimaryAgencyDimId),-1,IN_PrimaryAgencyDimId)
	IFF(IN_PrimaryAgencyDimId IS NULL,
		- 1,
		IN_PrimaryAgencyDimId
	) AS PrimaryAgencyDimId,
	EXP_GetValues_Occurrence.CatastropheDimId_CR
	FROM EXP_GetValues_Occurrence
	 -- Manually join with mplt_PolicyDimID_StopLossCOFact
	LEFT JOIN LKP_V3_PrimaryAgencyDimID_CO_Primary
	ON LKP_V3_PrimaryAgencyDimID_CO_Primary.Agencycode = EXP_LegalPrimaryAgency.o_LegalPrimaryAgencyCode AND LKP_V3_PrimaryAgencyDimID_CO_Primary.eff_from_date <= LKP_V3_PrimaryAgencyDimID_CO.IN_Trans_Date AND LKP_V3_PrimaryAgencyDimID_CO_Primary.eff_to_date >= LKP_V3_PrimaryAgencyDimID_CO.IN_Trans_Date
	LEFT JOIN LKP_claim_occurrence_dim
	ON LKP_claim_occurrence_dim.edw_claim_occurrence_ak_id = EXP_GetValues_Occurrence.edw_claim_occurrence_ak_id AND LKP_claim_occurrence_dim.eff_from_date <= EXP_GetValues_Occurrence.Rundate AND LKP_claim_occurrence_dim.eff_to_date >= EXP_GetValues_Occurrence.Rundate
	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_SET_DATE_PART_SET_DATE_PART_SET_DATE_PART_i_RunDate_HH_0_MI_0_SS_0
	ON LKP_CALENDER_DIM_SET_DATE_PART_SET_DATE_PART_SET_DATE_PART_i_RunDate_HH_0_MI_0_SS_0.clndr_date = DATEADD(SECOND,0-DATE_PART(SECOND,DATEADD(MINUTE,0-DATE_PART(MINUTE,DATEADD(HOUR,0-DATE_PART(HOUR,i_RunDate),i_RunDate)),DATEADD(HOUR,0-DATE_PART(HOUR,i_RunDate),i_RunDate))),DATEADD(MINUTE,0-DATE_PART(MINUTE,DATEADD(HOUR,0-DATE_PART(HOUR,i_RunDate),i_RunDate)),DATEADD(HOUR,0-DATE_PART(HOUR,i_RunDate),i_RunDate)))

),
FIL_StopLossAdjustmentAmount_Occurence AS (
	SELECT
	o_AuditId AS AuditId, 
	o_agency_dim_id AS AgencyDimId, 
	CatastropheDimId_CR, 
	o_contract_cust_dim_id AS contract_cust_dim_id, 
	pol_dim_id, 
	claim_occurrence_dim_id, 
	o_RunDateId AS RunDateId, 
	TotalDirectIncurredLoss, 
	o_StopLossLimit AS StopLossLimit, 
	o_StopLossAdjustmentAmount AS StopLossAdjustmentAmount, 
	ChangeInIncurredLoss AS O_ChangeInStopLossAdjustmentAmount, 
	PrimaryAgencyDimId
	FROM EXP_CalValues_Occurrence
	WHERE O_ChangeInStopLossAdjustmentAmount<>0.0

--IIF(O_ChangeInStopLossAdjustmentAmount=0.0 and StopLossAdjustmentAmount<>0.0,'Y',
--IIF(O_ChangeInStopLossAdjustmentAmount<>0.0,'Y','N'))='Y' --AND StopLossLimit>0

--O_ChangeInStopLossAdjustmentAmount<>0 OR StopLossAdjustmentAmount<>0.0

--O_ChangeInStopLossAdjustmentAmount<>0.0 AND StopLossLimit>0
),
LKP_StopLossAdjustmentCatastropheClaimOccurrenceFact AS (
	SELECT
	StopLossAdjustmentCatastropheClaimOccurrenceFactId,
	AgencyDimId,
	PrimaryAgencyDimId,
	ContractCustomerDimId,
	PolicyDimId,
	CatastropheDimId,
	ClaimOccurrenceDimId,
	RunDateId
	FROM (
		SELECT 
			StopLossAdjustmentCatastropheClaimOccurrenceFactId,
			AgencyDimId,
			PrimaryAgencyDimId,
			ContractCustomerDimId,
			PolicyDimId,
			CatastropheDimId,
			ClaimOccurrenceDimId,
			RunDateId
		FROM StopLossAdjustmentCatastropheClaimOccurrenceFact
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyDimId,PrimaryAgencyDimId,ContractCustomerDimId,PolicyDimId,CatastropheDimId,ClaimOccurrenceDimId,RunDateId ORDER BY StopLossAdjustmentCatastropheClaimOccurrenceFactId DESC) = 1
),
RTR_Insert_Update_Occurrence AS (
	SELECT
	LKP_StopLossAdjustmentCatastropheClaimOccurrenceFact.StopLossAdjustmentCatastropheClaimOccurrenceFactId AS StopLossAdjustmentClaimOccurrenceFactId,
	FIL_StopLossAdjustmentAmount_Occurence.AuditId,
	FIL_StopLossAdjustmentAmount_Occurence.AgencyDimId,
	FIL_StopLossAdjustmentAmount_Occurence.PrimaryAgencyDimId,
	FIL_StopLossAdjustmentAmount_Occurence.CatastropheDimId_CR AS CatastropheDimId,
	FIL_StopLossAdjustmentAmount_Occurence.contract_cust_dim_id AS ContractCustomerDimId,
	FIL_StopLossAdjustmentAmount_Occurence.pol_dim_id AS PolicyDimid,
	FIL_StopLossAdjustmentAmount_Occurence.claim_occurrence_dim_id AS ClaimOccurrenceDimId,
	FIL_StopLossAdjustmentAmount_Occurence.RunDateId,
	FIL_StopLossAdjustmentAmount_Occurence.TotalDirectIncurredLoss AS DirectIncurredLossInceptionToDate,
	FIL_StopLossAdjustmentAmount_Occurence.StopLossLimit,
	FIL_StopLossAdjustmentAmount_Occurence.StopLossAdjustmentAmount,
	FIL_StopLossAdjustmentAmount_Occurence.O_ChangeInStopLossAdjustmentAmount AS ChangeInStopLossAdjustmentAmount
	FROM FIL_StopLossAdjustmentAmount_Occurence
	LEFT JOIN LKP_StopLossAdjustmentCatastropheClaimOccurrenceFact
	ON LKP_StopLossAdjustmentCatastropheClaimOccurrenceFact.AgencyDimId = FIL_StopLossAdjustmentAmount_Occurence.AgencyDimId AND LKP_StopLossAdjustmentCatastropheClaimOccurrenceFact.PrimaryAgencyDimId = FIL_StopLossAdjustmentAmount_Occurence.PrimaryAgencyDimId AND LKP_StopLossAdjustmentCatastropheClaimOccurrenceFact.ContractCustomerDimId = FIL_StopLossAdjustmentAmount_Occurence.contract_cust_dim_id AND LKP_StopLossAdjustmentCatastropheClaimOccurrenceFact.PolicyDimId = FIL_StopLossAdjustmentAmount_Occurence.pol_dim_id AND LKP_StopLossAdjustmentCatastropheClaimOccurrenceFact.CatastropheDimId = FIL_StopLossAdjustmentAmount_Occurence.CatastropheDimId_CR AND LKP_StopLossAdjustmentCatastropheClaimOccurrenceFact.ClaimOccurrenceDimId = FIL_StopLossAdjustmentAmount_Occurence.claim_occurrence_dim_id AND LKP_StopLossAdjustmentCatastropheClaimOccurrenceFact.RunDateId = FIL_StopLossAdjustmentAmount_Occurence.RunDateId
),
RTR_Insert_Update_Occurrence_INSERT AS (SELECT * FROM RTR_Insert_Update_Occurrence WHERE ISNULL(StopLossAdjustmentClaimOccurrenceFactId)),
RTR_Insert_Update_Occurrence_DEFAULT1 AS (SELECT * FROM RTR_Insert_Update_Occurrence WHERE NOT ( (ISNULL(StopLossAdjustmentClaimOccurrenceFactId)) )),
UPD_Occurrence AS (
	SELECT
	StopLossAdjustmentClaimOccurrenceFactId, 
	AuditId, 
	AgencyDimId, 
	PrimaryAgencyDimId, 
	CatastropheDimId, 
	ContractCustomerDimId, 
	PolicyDimid, 
	ClaimOccurrenceDimId, 
	RunDateId, 
	DirectIncurredLossInceptionToDate, 
	StopLossLimit, 
	StopLossAdjustmentAmount, 
	ChangeInStopLossAdjustmentAmount
	FROM RTR_Insert_Update_Occurrence_DEFAULT1
),
StopLossAdjustmentCatastropheClaimOccurrenceFact_Update AS (
	MERGE INTO StopLossAdjustmentCatastropheClaimOccurrenceFact AS T
	USING UPD_Occurrence AS S
	ON T.StopLossAdjustmentCatastropheClaimOccurrenceFactId = S.StopLossAdjustmentClaimOccurrenceFactId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AuditId, T.AgencyDimId = S.AgencyDimId, T.PrimaryAgencyDimId = S.PrimaryAgencyDimId, T.ContractCustomerDimId = S.ContractCustomerDimId, T.PolicyDimId = S.PolicyDimid, T.CatastropheDimId = S.CatastropheDimId, T.ClaimOccurrenceDimId = S.ClaimOccurrenceDimId, T.RunDateId = S.RunDateId, T.DirectIncurredLossInceptionToDate = S.DirectIncurredLossInceptionToDate, T.ChangeInDirectIncurredLossInceptionToDate = S.ChangeInStopLossAdjustmentAmount
),
StopLossAdjustmentCatastropheClaimOccurrenceFact_Insert AS (
	INSERT INTO StopLossAdjustmentCatastropheClaimOccurrenceFact
	(AuditId, AgencyDimId, PrimaryAgencyDimId, ContractCustomerDimId, PolicyDimId, CatastropheDimId, ClaimOccurrenceDimId, RunDateId, DirectIncurredLossInceptionToDate, ChangeInDirectIncurredLossInceptionToDate)
	SELECT 
	AUDITID, 
	AGENCYDIMID, 
	PRIMARYAGENCYDIMID, 
	CONTRACTCUSTOMERDIMID, 
	PolicyDimid AS POLICYDIMID, 
	CATASTROPHEDIMID, 
	CLAIMOCCURRENCEDIMID, 
	RUNDATEID, 
	DIRECTINCURREDLOSSINCEPTIONTODATE, 
	ChangeInStopLossAdjustmentAmount AS CHANGEINDIRECTINCURREDLOSSINCEPTIONTODATE
	FROM RTR_Insert_Update_Occurrence_INSERT
),
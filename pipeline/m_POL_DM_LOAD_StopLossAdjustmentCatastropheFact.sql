WITH
LKP_StopLossAdjustmentCatastropheFact_PreviousInstance AS (
	SELECT
	StopLossAdjustmentAmount,
	AgencyCode,
	CatastropheCode
	FROM (
		SELECT A.AgencyCode AS AgencyCode,
		C.CatastropheCode AS CatastropheCode,
		SLA.StopLossAdjustmentAmount AS StopLossAdjustmentAmount
		FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.StopLossAdjustmentCatastropheFact SLA,
		      @{pipeline().parameters.TARGET_TABLE_OWNER}.calendar_dim cd,
		       @{pipeline().parameters.TARGET_TABLE_OWNER_V3}.AgencyDim A,
		       @{pipeline().parameters.TARGET_TABLE_OWNER}.CatastropheDim C
		WHERE  SLA.RunDateId = cd.clndr_id
		AND SLA.CatastropheDimId=C.CatastropheDimId
		AND SLA.PrimaryAgencyDimId=A.AgencyDimID
		and cd.CalendarMonthOfYear = 12
		order by 
		A.AgencyCode,
		C.CatastropheCode,SLA.RunDateid --
		--AND cd.clndr_date=DATEADD(DD,-1,DATEADD(YY,DATEDIFF(YY,0,dateadd(SS,-1,dateadd(mm,DATEDIFF(MM,0,getdate())-@{pipeline().parameters.NO_OF_MONTHS},0))),0))
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyCode,CatastropheCode ORDER BY StopLossAdjustmentAmount DESC) = 1
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
SQ_claim_loss_transaction_fact_Catastrophe_Agency14508_CAT_H4_G7 AS (
	DECLARE @DATE1 as datetime
	
	SET @DATE1 = dateadd(SS,-1,dateadd(mm,DATEDIFF(MM,0,getdate())-@{pipeline().parameters.NO_OF_MONTHS},0))
	
	SELECT
	A.LegalPrimaryAgencyCode,
	A.claim_cat_code,
	A.total_direct_loss_recovery_incurred,
	A.Loss_Year,
	A.Rundate,
	cast(convert(varchar(4),A.Rundate,112) as int) Run_Year,
	B.SalesDivisionDimId,
	B.AgencyDimId,
	B.PrimaryAgencyDimId,
	B.CatastropheDimId,
	B.DirectIncurredLossInceptionToDate,
	B.StopLossLimit,
	B.StopLossAdjustmentAmount,
	B.Rundate StopLossRundate
	FROM
	(select LegalPrimaryAgencyCode,
	B.claim_cat_code,
	sum(case when claim_rpted_date<='2013-12-31 00:00:00' then (DirectLossPaidIR+DirectLossOutstandingER+DirectALAEPaidIR) 
	else (DirectLossPaidIR+DirectLossOutstandingER+DirectALAEPaidIR+DirectALAEOutstandingIR) end) total_direct_loss_recovery_incurred,
	Min(convert(varchar(8),claim_loss_date,112)) Loss_Year,
	Min(claim_loss_date) Loss_Date,
	@DATE1 Rundate
	FROM
	(SELECT
	A.EDWAgencyAKID,
	A.AgencyCode,
	(CASE  
	     WHEN ard.legalprimaryagencycode 
	  is null THEN a.agencycode  
	      ELSE ard.legalprimaryagencycode
	END) as LegalPrimaryAgencyCode,
	VLMF.DirectLossPaidIR,
	VLMF.DirectALAEPaidIR,
	VLMF.DirectLossOutstandingER,
	VLMF.DirectALAEOutstandingIR,
	OCC.claim_rpted_date,
	CD.clndr_date AS RunDate,
	OCC.claim_loss_date,
	OCC.edw_claim_occurrence_ak_id
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.vwLossMasterFact VLMF
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim CD on VLMF.loss_master_run_date_id = CD.clndr_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_dim OCC  on VLMF.claim_occurrence_dim_id = OCC.claim_occurrence_dim_id
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim A ON VLMF.AgencyDimId=A.AgencyDimID
	
	left join 
	 (select * from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Agencyrelationshipdim where currentsnapshotflag = 1) ard
	 on (A.edwagencyakid=ard.edwagencyakid 
	 and cd.clndr_date between ard.agencyrelationshipeffectivedate and ard.agencyrelationshipexpirationdate)
	
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim POL ON VLMF.pol_dim_id=POL.pol_dim_id
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim ird ON VLMF.InsuranceReferenceDimId = ird.InsuranceReferenceDimId
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim ircd ON VLMF.InsuranceReferenceCoverageDimId = ircd.InsuranceReferenceCoverageDimId
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailDim cdd ON VLMF.CoverageDetailDimId = cdd.CoverageDetailDimId
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_product_code_dim apcd ON VLMF.asl_prdct_code_dim_id = apcd.asl_prdct_code_dim_id
	where 
	CD.clndr_date>='01/01/2001 00:00:00'
	and CASE WHEN (PolicyOfferingCode in ('600') 
	OR ird.InsuranceSegmentCode = '3'
	OR (ircd.CoverageGroupCode in ('BOILER','CYBERSEC','DATACOMP','CYBERSUITE') 
	OR ircd.CoverageGroupCode like '%TRIA' 
	OR ircd.CoverageGroupDescription in ('MCCA Surcharge'))
	OR ird.InsuranceReferenceLineofBusinessCode in ('350','311','590','812','890','900')
	OR (ird.RatingPlanAbbreviation in ('LRARO','Retro'))
	OR apcd.asl_prdct_code in ('200', '220')
	OR (ird.StrategicProfitCentercode <> '5' and cdd.ISOClassCode='0174')) then 'N' else 'Y' end='Y'
	AND CD.clndr_date Between '01/01/2022' and @DATE1
	) A,
	(SELECT claim_cat_code,edw_claim_occurrence_ak_id  from @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_dim
	WHERE  @DATE1 BETWEEN eff_from_date AND eff_to_date) B
	WHERE A.edw_claim_occurrence_ak_id = B.edw_claim_occurrence_ak_id
	and B.claim_cat_code<>'N/A'
	AND RunDate <= @DATE1
	group by LegalPrimaryAgencyCode,B.claim_cat_code) A
	full outer join
	(
	Select AB.EDWAgencyAKID,AB.AgencyCode,
	(CASE  WHEN ard.legalprimaryagencycode is null THEN ab.agencycode  ELSE ard.legalprimaryagencycode END) as legalprimaryagencycode, 
	AB.CatastropheCode,AB.DirectIncurredLossInceptionToDate,AB.StopLossAdjustmentAmount,
	AB.clndr_date,AB.AgencyDimId,AB.PrimaryAgencyDimId,AB.SalesDivisionDimId,AB.CatastropheDimId,AB.StopLossLimit,AB.Rundate from 
	(select B.EDWAgencyAKID,B.AgencyCode,C.CatastropheCode,A.DirectIncurredLossInceptionToDate,A.StopLossAdjustmentAmount,
	D.clndr_date,A.AgencyDimId,A.PrimaryAgencyDimId,A.SalesDivisionDimId,A.CatastropheDimId,A.StopLossLimit,@DATE1 Rundate
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.StopLossAdjustmentCatastropheFact A,@{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim B,@{pipeline().parameters.SOURCE_TABLE_OWNER}.CatastropheDim C,
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim D
	where A.PrimaryAgencyDimId=B.AgencyDimID
	and A.CatastropheDimId=C.CatastropheDimId
	and A.RunDateId=D.clndr_id
	and D.clndr_date=dateadd(dd,-1,DATEADD(yy,DATEDIFF(yy,0,@DATE1),0))) AB 
	left join   (select * from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Agencyrelationshipdim where currentsnapshotflag = 1) ard
	 on (AB.edwagencyakid=ard.edwagencyakid 
	 and AB.clndr_date between ard.agencyrelationshipeffectivedate and ard.agencyrelationshipexpirationdate) 
	) B
	on A.LegalPrimaryAgencyCode=B.AgencyCode
	and A.claim_cat_code=B.CatastropheCode
	@{pipeline().parameters.WHERECLAUSECATASTROPHE_1}
	
	
	
	
	/*Left join with "agencyrelationshipdim" in order to get legalprimaryagencycode values from "AgencyRelationshipDim" table*/
),
EXP_Default AS (
	SELECT
	LegalPrimaryAgencyCode,
	CatastropheCode,
	total_direct_loss_recovery_incurred,
	Loss_Year,
	Rundate,
	Run_Year,
	SalesDivisionDimId,
	AgencyDimId,
	PrimaryAgencyDimId,
	CatastropheDimId,
	DirectIncurredLossInceptionToDate,
	StopLossLimit,
	StopLossAdjustmentAmount,
	StopLossRundate
	FROM SQ_claim_loss_transaction_fact_Catastrophe_Agency14508_CAT_H4_G7
),
SQ_claim_loss_transaction_fact_Catastrophe AS (
	DECLARE @DATE1 as datetime
	
	SET @DATE1 = dateadd(SS,-1,dateadd(mm,DATEDIFF(MM,0,getdate())-@{pipeline().parameters.NO_OF_MONTHS},0))
	
	SELECT
	A.LegalPrimaryAgencyCode,
	A.claim_cat_code,
	A.total_direct_loss_recovery_incurred,
	A.Loss_Year,
	A.Rundate,
	cast(convert(varchar(4),A.Rundate,112) as int) Run_Year,
	B.SalesDivisionDimId,
	B.AgencyDimId,
	B.PrimaryAgencyDimId,
	B.CatastropheDimId,
	B.DirectIncurredLossInceptionToDate,
	B.StopLossLimit,
	B.StopLossAdjustmentAmount,
	B.Rundate StopLossRundate
	FROM 
	(SELECT LegalPrimaryAgencyCode,
	B.claim_cat_code,
	SUM(case WHEN claim_rpted_date<='2013-12-31 00:00:00' THEN (DirectLossPaidIR+DirectLossOutstandingER+DirectALAEPaidIR) 
	ELSE (DirectLossPaidIR+DirectLossOutstandingER+DirectALAEPaidIR+DirectALAEOutstandingIR) end) total_direct_loss_recovery_incurred,
	Min(convert(varchar(8),claim_loss_date,112)) Loss_Year,
	Min(claim_loss_date) Loss_Date,
	@DATE1 Rundate
	FROM 
	(SELECT
	A.EDWAgencyAKID,
	A.AgencyCode,
	(CASE  WHEN ard.legalprimaryagencycode is null THEN a.agencycode ELSE ard.legalprimaryagencycode END) as LegalPrimaryAgencyCode,
	VLMF.DirectLossPaidIR,
	VLMF.DirectALAEPaidIR,
	VLMF.DirectLossOutstandingER,
	VLMF.DirectALAEOutstandingIR,
	OCC.claim_rpted_date,
	CD.clndr_date AS RunDate,
	OCC.claim_loss_date,
	OCC.edw_claim_occurrence_ak_id
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.vwLossMasterFact VLMF
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim CD on VLMF.loss_master_run_date_id = CD.clndr_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_dim OCC  on VLMF.claim_occurrence_dim_id = OCC.claim_occurrence_dim_id
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim A ON VLMF.AgencyDimId=A.AgencyDimID
	LEFT JOIN
	 (select * from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Agencyrelationshipdim where currentsnapshotflag = 1) ard
	 on (A.edwagencyakid=ard.edwagencyakid 
	 and cd.clndr_date between ard.agencyrelationshipeffectivedate and ard.agencyrelationshipexpirationdate)
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim POL ON VLMF.pol_dim_id=POL.pol_dim_id
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim ird ON VLMF.InsuranceReferenceDimId = ird.InsuranceReferenceDimId
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim ircd ON VLMF.InsuranceReferenceCoverageDimId = ircd.InsuranceReferenceCoverageDimId
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailDim cdd ON VLMF.CoverageDetailDimId = cdd.CoverageDetailDimId
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_product_code_dim apcd ON VLMF.asl_prdct_code_dim_id = apcd.asl_prdct_code_dim_id
	WHERE 
	CD.clndr_date>='01/01/2001 00:00:00'
	and CASE WHEN (PolicyOfferingCode in ('600') 
	OR ird.InsuranceSegmentCode = '3'
	OR (ircd.CoverageGroupCode in ('BOILER','CYBERSEC','DATACOMP','CYBERSUITE') 
	OR ircd.CoverageGroupCode like '%TRIA' 
	OR ircd.CoverageGroupDescription in ('MCCA Surcharge'))
	OR ird.InsuranceReferenceLineofBusinessCode in ('350','311','590','812','890','900')
	OR (ird.RatingPlanAbbreviation in ('LRARO','Retro'))
	OR apcd.asl_prdct_code in ('200', '220')
	OR (ird.StrategicProfitCentercode <> '5' and cdd.ISOClassCode='0174')) then 'N' else 'Y' end='Y'
	) A,
	(SELECT claim_cat_code,edw_claim_occurrence_ak_id  from @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_dim
	WHERE  @DATE1 BETWEEN eff_from_date AND eff_to_date) B
	WHERE A.edw_claim_occurrence_ak_id = B.edw_claim_occurrence_ak_id
	and B.claim_cat_code<>'N/A'
	AND RunDate <= @DATE1
	group by LegalPrimaryAgencyCode,B.claim_cat_code) A
	FULL OUTER JOIN 
	(
	SELECT AB.EDWAgencyAKID,AB.AgencyCode,
	(CASE WHEN ard.legalprimaryagencycode is null THEN ab.agencycode  ELSE ard.legalprimaryagencycode END) as legalprimaryagencycode, 
	AB.CatastropheCode,AB.DirectIncurredLossInceptionToDate,AB.StopLossAdjustmentAmount,
	AB.clndr_date,AB.AgencyDimId,AB.PrimaryAgencyDimId,AB.SalesDivisionDimId,AB.CatastropheDimId,AB.StopLossLimit,AB.Rundate from 
	(select B.EDWAgencyAKID,B.AgencyCode,C.CatastropheCode,A.DirectIncurredLossInceptionToDate,A.StopLossAdjustmentAmount,
	D.clndr_date,A.AgencyDimId,A.PrimaryAgencyDimId,A.SalesDivisionDimId,A.CatastropheDimId,A.StopLossLimit,@DATE1 Rundate
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.StopLossAdjustmentCatastropheFact A,@{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim B,@{pipeline().parameters.SOURCE_TABLE_OWNER}.CatastropheDim C,
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim D
	where A.PrimaryAgencyDimId=B.AgencyDimID
	and A.CatastropheDimId=C.CatastropheDimId
	and A.RunDateId=D.clndr_id
	and D.clndr_date=dateadd(dd,-1,DATEADD(yy,DATEDIFF(yy,0,@DATE1),0))) AB 
	left join   (select * from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Agencyrelationshipdim where currentsnapshotflag = 1) ard
	 on (AB.edwagencyakid=ard.edwagencyakid 
	 and AB.clndr_date between ard.agencyrelationshipeffectivedate and ard.agencyrelationshipexpirationdate) 
	) B
	on A.LegalPrimaryAgencyCode=B.AgencyCode
	and A.claim_cat_code=B.CatastropheCode
	@{pipeline().parameters.WHERECLAUSECATASTROPHE}
	
	/*Left join with "agencyrelationshipdim" in order to get legalprimaryagencycode values from "AgencyRelationshipDim" table*/
),
EXP_SRC_DataCollect AS (
	SELECT
	LegalPrimaryAgencyCode,
	CatastropheCode,
	total_direct_loss_recovery_incurred,
	Loss_Year,
	Rundate,
	Run_Year,
	SalesDivisionDimId,
	AgencyDimId,
	PrimaryAgencyDimId,
	CatastropheDimId,
	DirectIncurredLossInceptionToDate,
	StopLossLimit,
	StopLossAdjustmentAmount,
	StopLossRundate
	FROM SQ_claim_loss_transaction_fact_Catastrophe
),
Union AS (
	SELECT LegalPrimaryAgencyCode, CatastropheCode, total_direct_loss_recovery_incurred, Loss_Year, Rundate, Run_Year, SalesDivisionDimId, AgencyDimId, PrimaryAgencyDimId, CatastropheDimId, DirectIncurredLossInceptionToDate, StopLossLimit, StopLossAdjustmentAmount, StopLossRundate
	FROM EXP_Default
	UNION
	SELECT LegalPrimaryAgencyCode, CatastropheCode, total_direct_loss_recovery_incurred, Loss_Year, Rundate, Run_Year, SalesDivisionDimId, AgencyDimId, PrimaryAgencyDimId, CatastropheDimId, DirectIncurredLossInceptionToDate, StopLossLimit, StopLossAdjustmentAmount, StopLossRundate
	FROM EXP_SRC_DataCollect
),
RTR_Split_New_Balance AS (
	SELECT
	LegalPrimaryAgencyCode,
	CatastropheCode,
	total_direct_loss_recovery_incurred,
	Loss_Year,
	Rundate,
	Run_Year,
	SalesDivisionDimId,
	AgencyDimId,
	PrimaryAgencyDimId,
	CatastropheDimId,
	DirectIncurredLossInceptionToDate,
	StopLossLimit,
	StopLossAdjustmentAmount,
	StopLossRundate
	FROM Union
),
RTR_Split_New_Balance_New AS (SELECT * FROM RTR_Split_New_Balance WHERE NOT ISNULL(LegalPrimaryAgencyCode) and Run_Year-to_integer(substr(Loss_Year,1,4))<=2),
RTR_Split_New_Balance_Balance AS (SELECT * FROM RTR_Split_New_Balance WHERE ISNULL(LegalPrimaryAgencyCode) AND Run_Year-to_integer(substr(Loss_Year,1,4))<=2

-----  This Group is used to write the balancing transaction if CAT Loss becomes Non CAT Loss and Non CAT Loss becomes a CAT Loss. We are writing a balancing transcation to balance the previously written Stop Loss AdjustmentAmount.

---- Added the condition to filter out the rows if CAT loss is more than 3 years.),
EXP_StopLossCat_Balance AS (
	SELECT
	-1000 AS AuditId,
	SalesDivisionDimId,
	AgencyDimId,
	PrimaryAgencyDimId,
	CatastropheDimId,
	DirectIncurredLossInceptionToDate,
	StopLossLimit,
	StopLossAdjustmentAmount,
	0.00 AS O_StopLossAdjustmentAmount,
	0-StopLossAdjustmentAmount AS ChangeINStopLossAdjustmentAmount,
	StopLossRundate,
	-- *INF*: :LKP.LKP_CALENDER_DIM(SET_DATE_PART(SET_DATE_PART(SET_DATE_PART(StopLossRundate,'HH',0),'MI',0),'SS',0))
	LKP_CALENDER_DIM_SET_DATE_PART_SET_DATE_PART_SET_DATE_PART_StopLossRundate_HH_0_MI_0_SS_0.clndr_id AS RunDateID
	FROM RTR_Split_New_Balance_Balance
	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_SET_DATE_PART_SET_DATE_PART_SET_DATE_PART_StopLossRundate_HH_0_MI_0_SS_0
	ON LKP_CALENDER_DIM_SET_DATE_PART_SET_DATE_PART_SET_DATE_PART_StopLossRundate_HH_0_MI_0_SS_0.clndr_date = DATEADD(SECOND,0-DATE_PART(SECOND,DATEADD(MINUTE,0-DATE_PART(MINUTE,DATEADD(HOUR,0-DATE_PART(HOUR,StopLossRundate),StopLossRundate)),DATEADD(HOUR,0-DATE_PART(HOUR,StopLossRundate),StopLossRundate))),DATEADD(MINUTE,0-DATE_PART(MINUTE,DATEADD(HOUR,0-DATE_PART(HOUR,StopLossRundate),StopLossRundate)),DATEADD(HOUR,0-DATE_PART(HOUR,StopLossRundate),StopLossRundate)))

),
TGT_StopLossAdjustmentCatastropheFact_Balance AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.StopLossAdjustmentCatastropheFact
	(AuditId, SalesDivisionDimId, AgencyDimId, PrimaryAgencyDimId, CatastropheDimId, RunDateId, DirectIncurredLossInceptionToDate, StopLossLimit, StopLossAdjustmentAmount, ChangeInStopLossAdjustmentAmount)
	SELECT 
	AUDITID, 
	SALESDIVISIONDIMID, 
	AGENCYDIMID, 
	PRIMARYAGENCYDIMID, 
	CATASTROPHEDIMID, 
	RunDateID AS RUNDATEID, 
	DIRECTINCURREDLOSSINCEPTIONTODATE, 
	STOPLOSSLIMIT, 
	O_StopLossAdjustmentAmount AS STOPLOSSADJUSTMENTAMOUNT, 
	ChangeINStopLossAdjustmentAmount AS CHANGEINSTOPLOSSADJUSTMENTAMOUNT
	FROM EXP_StopLossCat_Balance
),
EXP_GetValues_Catastrophe AS (
	SELECT
	'99999' AS AgencyCode,
	CatastropheCode,
	total_direct_loss_recovery_incurred,
	Loss_Year,
	-- *INF*: substr(Loss_Year,1,4)
	substr(Loss_Year, 1, 4) AS O_Loss_Year,
	Rundate AS RunDate,
	LegalPrimaryAgencyCode
	FROM RTR_Split_New_Balance_New
),
LKP_CatastropheDim AS (
	SELECT
	CatastropheDimId,
	CatastropheCode
	FROM (
		SELECT 
			CatastropheDimId,
			CatastropheCode
		FROM CatastropheDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CatastropheCode ORDER BY CatastropheDimId DESC) = 1
),
LKP_V3_AgencyDimID AS (
	SELECT
	agency_dim_id,
	AgencyCode,
	eff_from_date,
	eff_to_date
	FROM (
		SELECT AgencyDim.AgencyDimID as agency_dim_id,
		 AgencyDim.Agencycode as Agencycode,
		  AgencyDim.EffectiveDate as eff_from_date,
		   AgencyDim.ExpirationDate as eff_to_date
		 FROM @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim as AgencyDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyCode,eff_from_date,eff_to_date ORDER BY agency_dim_id DESC) = 1
),
LKP_V3_PrimaryAgencyDimID_Primary AS (
	SELECT
	LegalPrimaryAgencyCode,
	IN_Trans_Date,
	Agencycode,
	eff_from_date,
	eff_to_date
	FROM (
		SELECT 
		
		ard. LegalPrimaryAgencyCode as LegalPrimaryAgencyCode , 
		 
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
EXP_LegalPrimaryAgencycode AS (
	SELECT
	EXP_GetValues_Catastrophe.LegalPrimaryAgencyCode,
	LKP_V3_PrimaryAgencyDimID_Primary.LegalPrimaryAgencyCode AS LKP_LegalPrimaryAgencyCode,
	-- *INF*: iif(isnull(LKP_LegalPrimaryAgencyCode),LegalPrimaryAgencyCode,LKP_LegalPrimaryAgencyCode)
	IFF(LKP_LegalPrimaryAgencyCode IS NULL, LegalPrimaryAgencyCode, LKP_LegalPrimaryAgencyCode) AS o_LegalPrimaryAgencyCode
	FROM EXP_GetValues_Catastrophe
	LEFT JOIN LKP_V3_PrimaryAgencyDimID_Primary
	ON LKP_V3_PrimaryAgencyDimID_Primary.Agencycode = EXP_GetValues_Catastrophe.LegalPrimaryAgencyCode AND LKP_V3_PrimaryAgencyDimID_Primary.eff_from_date <= EXP_GetValues_Catastrophe.RunDate AND LKP_V3_PrimaryAgencyDimID_Primary.eff_to_date >= EXP_GetValues_Catastrophe.RunDate
),
LKP_V3_PrimaryAgencyDimID_CAT AS (
	SELECT
	agency_dim_id,
	SalesDivisionDimId,
	IN_LegalPrimaryAgencyCode,
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
LKP_WorkStopLossLimit_Catastrophe AS (
	SELECT
	PrimaryAgencyCode,
	StopLossLimit,
	AgencyPreviousYearDirectWrittenPremium,
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
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PrimaryAgencyCode,Loss_Year ORDER BY PrimaryAgencyCode DESC) = 1
),
LKP_WorkStopLossLimit_Catastrophe_Primary AS (
	SELECT
	PrimaryAgencyCode,
	StopLossLimit,
	AgencyPreviousYearDirectWrittenPremium,
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
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PrimaryAgencyCode,Loss_Year ORDER BY PrimaryAgencyCode DESC) = 1
),
EXP_CalValues_Catastrophe AS (
	SELECT
	EXP_GetValues_Catastrophe.total_direct_loss_recovery_incurred AS i_TotalDirectIncurredLoss,
	EXP_GetValues_Catastrophe.AgencyCode,
	EXP_GetValues_Catastrophe.CatastropheCode,
	EXP_GetValues_Catastrophe.RunDate AS i_RunDate,
	LKP_WorkStopLossLimit_Catastrophe.StopLossLimit AS lkp_StopLossLimit,
	LKP_WorkStopLossLimit_Catastrophe_Primary.StopLossLimit AS lkp_StopLossLimit2,
	LKP_V3_PrimaryAgencyDimID_CAT.IN_LegalPrimaryAgencyCode AS lkp_LegalPrimaryAgencyCode2,
	LKP_WorkStopLossLimit_Catastrophe.AgencyPreviousYearDirectWrittenPremium AS lkp_AgencyPreviousYearDirectWrittenPremium,
	LKP_WorkStopLossLimit_Catastrophe_Primary.AgencyPreviousYearDirectWrittenPremium AS lkp_AgencyPreviousYearDirectWrittenPremium2,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	LKP_V3_PrimaryAgencyDimID_CAT.SalesDivisionDimId AS IN_SalesDivisionDimId,
	-- *INF*: IIF(ISNULL(IN_SalesDivisionDimId),-1,IN_SalesDivisionDimId)
	IFF(IN_SalesDivisionDimId IS NULL, - 1, IN_SalesDivisionDimId) AS SalesDivisionDimId,
	LKP_V3_AgencyDimID.agency_dim_id AS AgencyDimId,
	LKP_CatastropheDim.CatastropheDimId,
	-- *INF*: :LKP.LKP_CALENDER_DIM(SET_DATE_PART(SET_DATE_PART(SET_DATE_PART(i_RunDate,'HH',0),'MI',0),'SS',0))
	LKP_CALENDER_DIM_SET_DATE_PART_SET_DATE_PART_SET_DATE_PART_i_RunDate_HH_0_MI_0_SS_0.clndr_id AS o_RunDateId,
	EXP_GetValues_Catastrophe.Loss_Year,
	-- *INF*: substr(Loss_Year,1,4)
	substr(Loss_Year, 1, 4) AS V_LOSS_YEAR,
	-- *INF*: substr(Loss_Year,5,2)
	substr(Loss_Year, 5, 2) AS V_LOSS_MONTH,
	i_TotalDirectIncurredLoss AS o_TotalDirectIncurredLoss,
	-- *INF*: Decode(TRUE,IN(LegalPrimaryAgencyCode,'12176','13176'),
	-- DECODE(TRUE,V_LOSS_YEAR<'1995',100000,
	-- V_LOSS_YEAR='1995',125000,
	-- IN(V_LOSS_YEAR,'1996','1997'),200000,
	-- IN(V_LOSS_YEAR,'1998','1999','2000','2001'),400000,
	-- V_LOSS_YEAR>'2001',1000000),
	-- IN(LegalPrimaryAgencyCode,'12651','13651'),
	-- DECODE(TRUE,V_LOSS_YEAR<'1995',100000,
	-- V_LOSS_YEAR='1995',125000,
	-- IN(V_LOSS_YEAR,'1996','1997','1998','1999','2000','2001','2002','2003'),200000,
	-- V_LOSS_YEAR='2005' and IN(V_LOSS_MONTH,'04','05','06','07','08','09','10','11','12'),500000,lkp_StopLossLimit),
	-- Decode(TRUE,V_LOSS_YEAR<'1995',100000,
	-- V_LOSS_YEAR='1995',125000,
	-- IN(V_LOSS_YEAR,'1996','1997','1998','1999','2000','2001','2002','2003'),200000,
	-- V_LOSS_YEAR>='2015' AND lkp_AgencyPreviousYearDirectWrittenPremium<=20000000,500000,
	-- V_LOSS_YEAR>='2015' AND V_LOSS_YEAR < '2022' AND lkp_AgencyPreviousYearDirectWrittenPremium2 > 20000000,500000,
	-- V_LOSS_YEAR>='2022' AND lkp_AgencyPreviousYearDirectWrittenPremium>20000000,1000000,
	-- lkp_StopLossLimit))
	-- 
	-- --The above decode statement has been implemented to catch the stoplosslimit as per the mainframe code WB11002B
	Decode(
	    TRUE,
	    LegalPrimaryAgencyCode IN ('12176','13176'), DECODE(
	        TRUE,
	        V_LOSS_YEAR < '1995', 100000,
	        V_LOSS_YEAR = '1995', 125000,
	        V_LOSS_YEAR IN ('1996','1997'), 200000,
	        V_LOSS_YEAR IN ('1998','1999','2000','2001'), 400000,
	        V_LOSS_YEAR > '2001', 1000000
	    ),
	    LegalPrimaryAgencyCode IN ('12651','13651'), DECODE(
	        TRUE,
	        V_LOSS_YEAR < '1995', 100000,
	        V_LOSS_YEAR = '1995', 125000,
	        V_LOSS_YEAR IN ('1996','1997','1998','1999','2000','2001','2002','2003'), 200000,
	        V_LOSS_YEAR = '2005' and V_LOSS_MONTH IN ('04','05','06','07','08','09','10','11','12'), 500000,
	        lkp_StopLossLimit
	    ),
	    Decode(
	        TRUE,
	        V_LOSS_YEAR < '1995', 100000,
	        V_LOSS_YEAR = '1995', 125000,
	        V_LOSS_YEAR IN ('1996','1997','1998','1999','2000','2001','2002','2003'), 200000,
	        V_LOSS_YEAR >= '2015' AND lkp_AgencyPreviousYearDirectWrittenPremium <= 20000000, 500000,
	        V_LOSS_YEAR >= '2015' AND V_LOSS_YEAR < '2022' AND lkp_AgencyPreviousYearDirectWrittenPremium2 > 20000000, 500000,
	        V_LOSS_YEAR >= '2022' AND lkp_AgencyPreviousYearDirectWrittenPremium > 20000000, 1000000,
	        lkp_StopLossLimit
	    )
	) AS V_STOP_LOSS_LIMIT,
	-- *INF*: Decode(TRUE,IN(lkp_LegalPrimaryAgencyCode2,'12176','13176'),
	-- DECODE(TRUE,V_LOSS_YEAR<'1995',100000,
	-- V_LOSS_YEAR='1995',125000,
	-- IN(V_LOSS_YEAR,'1996','1997'),200000,
	-- IN(V_LOSS_YEAR,'1998','1999','2000','2001'),400000,
	-- V_LOSS_YEAR>'2001',1000000),
	-- IN(lkp_LegalPrimaryAgencyCode2,'12651','13651'),
	-- DECODE(TRUE,V_LOSS_YEAR<'1995',100000,
	-- V_LOSS_YEAR='1995',125000,
	-- IN(V_LOSS_YEAR,'1996','1997','1998','1999','2000','2001','2002','2003'),200000,
	-- V_LOSS_YEAR='2005' and IN(V_LOSS_MONTH,'04','05','06','07','08','09','10','11','12'),500000,lkp_StopLossLimit2),
	-- Decode(TRUE,V_LOSS_YEAR<'1995',100000,
	-- V_LOSS_YEAR='1995',125000,
	-- IN(V_LOSS_YEAR,'1996','1997','1998','1999','2000','2001','2002','2003'),200000,
	-- V_LOSS_YEAR>='2015' AND lkp_AgencyPreviousYearDirectWrittenPremium2<=20000000,500000,
	-- V_LOSS_YEAR>='2015' AND V_LOSS_YEAR < '2022' AND lkp_AgencyPreviousYearDirectWrittenPremium2 > 20000000,500000,
	-- V_LOSS_YEAR>='2022' AND lkp_AgencyPreviousYearDirectWrittenPremium2>20000000,1000000,
	-- lkp_StopLossLimit2))
	Decode(
	    TRUE,
	    lkp_LegalPrimaryAgencyCode2 IN ('12176','13176'), DECODE(
	        TRUE,
	        V_LOSS_YEAR < '1995', 100000,
	        V_LOSS_YEAR = '1995', 125000,
	        V_LOSS_YEAR IN ('1996','1997'), 200000,
	        V_LOSS_YEAR IN ('1998','1999','2000','2001'), 400000,
	        V_LOSS_YEAR > '2001', 1000000
	    ),
	    lkp_LegalPrimaryAgencyCode2 IN ('12651','13651'), DECODE(
	        TRUE,
	        V_LOSS_YEAR < '1995', 100000,
	        V_LOSS_YEAR = '1995', 125000,
	        V_LOSS_YEAR IN ('1996','1997','1998','1999','2000','2001','2002','2003'), 200000,
	        V_LOSS_YEAR = '2005' and V_LOSS_MONTH IN ('04','05','06','07','08','09','10','11','12'), 500000,
	        lkp_StopLossLimit2
	    ),
	    Decode(
	        TRUE,
	        V_LOSS_YEAR < '1995', 100000,
	        V_LOSS_YEAR = '1995', 125000,
	        V_LOSS_YEAR IN ('1996','1997','1998','1999','2000','2001','2002','2003'), 200000,
	        V_LOSS_YEAR >= '2015' AND lkp_AgencyPreviousYearDirectWrittenPremium2 <= 20000000, 500000,
	        V_LOSS_YEAR >= '2015' AND V_LOSS_YEAR < '2022' AND lkp_AgencyPreviousYearDirectWrittenPremium2 > 20000000, 500000,
	        V_LOSS_YEAR >= '2022' AND lkp_AgencyPreviousYearDirectWrittenPremium2 > 20000000, 1000000,
	        lkp_StopLossLimit2
	    )
	) AS V_STOP_LOSS_LIMIT2,
	-- *INF*: IIF(ISNULL(V_STOP_LOSS_LIMIT) or V_STOP_LOSS_LIMIT=0.0,200000,V_STOP_LOSS_LIMIT)
	-- 
	-- --As per the Cobol code, if a given Agency does not have stop loss limit then we assign 200000 as the default stop loss limit.
	IFF(V_STOP_LOSS_LIMIT IS NULL or V_STOP_LOSS_LIMIT = 0.0, 200000, V_STOP_LOSS_LIMIT) AS V_Final_Stop_Loss_Limit,
	-- *INF*: IIF(ISNULL(V_STOP_LOSS_LIMIT2) or V_STOP_LOSS_LIMIT2=0.0,200000,V_STOP_LOSS_LIMIT2)
	-- 
	-- --As per the Cobol code, if a given Agency does not have stop loss limit then we assign 200000 as the default stop loss limit.
	IFF(V_STOP_LOSS_LIMIT2 IS NULL or V_STOP_LOSS_LIMIT2 = 0.0, 200000, V_STOP_LOSS_LIMIT2) AS V_Final_Stop_Loss_Limit2,
	V_Final_Stop_Loss_Limit AS o_StopLossLimit,
	V_Final_Stop_Loss_Limit2 AS o_StopLossLimit2,
	-- *INF*: IIF(i_TotalDirectIncurredLoss<=V_Final_Stop_Loss_Limit, 0.0 , i_TotalDirectIncurredLoss-V_Final_Stop_Loss_Limit)
	IFF(
	    i_TotalDirectIncurredLoss <= V_Final_Stop_Loss_Limit, 0.0,
	    i_TotalDirectIncurredLoss - V_Final_Stop_Loss_Limit
	) AS V_StopLossAdjustmentAmount,
	-- *INF*: DECODE(TRUE,
	-- LegalPrimaryAgencyCode = '14508' AND IN(CatastropheCode,'0G7','0H4'), i_TotalDirectIncurredLoss,
	-- V_StopLossAdjustmentAmount>0.0,V_StopLossAdjustmentAmount,0.0)
	DECODE(
	    TRUE,
	    LegalPrimaryAgencyCode = '14508' AND CatastropheCode IN ('0G7','0H4'), i_TotalDirectIncurredLoss,
	    V_StopLossAdjustmentAmount > 0.0, V_StopLossAdjustmentAmount,
	    0.0
	) AS o_StopLossAdjustmentAmount,
	-- *INF*: IIF(ISNULL(:LKP.LKP_STOPLOSSADJUSTMENTCATASTROPHEFACT_PREVIOUSINSTANCE(LegalPrimaryAgencyCode,CatastropheCode)),0.0,:LKP.LKP_STOPLOSSADJUSTMENTCATASTROPHEFACT_PREVIOUSINSTANCE(LegalPrimaryAgencyCode,CatastropheCode))
	IFF(
	    LKP_STOPLOSSADJUSTMENTCATASTROPHEFACT_PREVIOUSINSTANCE_LegalPrimaryAgencyCode_CatastropheCode.StopLossAdjustmentAmount IS NULL,
	    0.0,
	    LKP_STOPLOSSADJUSTMENTCATASTROPHEFACT_PREVIOUSINSTANCE_LegalPrimaryAgencyCode_CatastropheCode.StopLossAdjustmentAmount
	) AS Lkp_Previous_StopLossAdjustmentAmount,
	V_StopLossAdjustmentAmount-Lkp_Previous_StopLossAdjustmentAmount AS v_ChangeInStopLossAdjustmentAmount,
	-- *INF*: DECODE(TRUE,
	-- LegalPrimaryAgencyCode = '14508' AND IN(CatastropheCode,'0G7','0H4'), i_TotalDirectIncurredLoss,
	-- LegalPrimaryAgencyCode = '48755' AND CatastropheCode ='0H5', 0.00,
	-- v_ChangeInStopLossAdjustmentAmount)
	-- 
	-- ----- Above is work around fix for ChangeInStopLossAdjusmentAmount field called out in INC0271757/US-413136, this is temporary fix as CAT loss more than 3 years won't get written to target fact table. Above CAT's have originated in 2020 so no data gets written in 2023.
	-- 
	DECODE(
	    TRUE,
	    LegalPrimaryAgencyCode = '14508' AND CatastropheCode IN ('0G7','0H4'), i_TotalDirectIncurredLoss,
	    LegalPrimaryAgencyCode = '48755' AND CatastropheCode = '0H5', 0.00,
	    v_ChangeInStopLossAdjustmentAmount
	) AS O_ChangeInStopLossAdjustmentAmount,
	EXP_GetValues_Catastrophe.LegalPrimaryAgencyCode,
	LKP_WorkStopLossLimit_Catastrophe.PrimaryAgencyCode,
	-- *INF*: IIF(ISNULL(PrimaryAgencyCode),LegalPrimaryAgencyCode,PrimaryAgencyCode)
	IFF(PrimaryAgencyCode IS NULL, LegalPrimaryAgencyCode, PrimaryAgencyCode) AS v_LegalPrimaryAgencyCode,
	LKP_V3_PrimaryAgencyDimID_CAT.agency_dim_id AS IN_PrimaryAgencyDimId,
	-- *INF*: IIF(ISNULL(IN_PrimaryAgencyDimId),-1,IN_PrimaryAgencyDimId)
	IFF(IN_PrimaryAgencyDimId IS NULL, - 1, IN_PrimaryAgencyDimId) AS PrimaryAgencyDimId
	FROM EXP_GetValues_Catastrophe
	LEFT JOIN LKP_CatastropheDim
	ON LKP_CatastropheDim.CatastropheCode = RTR_Split_New_Balance.CatastropheCode1
	LEFT JOIN LKP_V3_AgencyDimID
	ON LKP_V3_AgencyDimID.AgencyCode = EXP_GetValues_Catastrophe.AgencyCode AND LKP_V3_AgencyDimID.eff_from_date <= EXP_GetValues_Catastrophe.RunDate AND LKP_V3_AgencyDimID.eff_to_date >= EXP_GetValues_Catastrophe.RunDate
	LEFT JOIN LKP_V3_PrimaryAgencyDimID_CAT
	ON LKP_V3_PrimaryAgencyDimID_CAT.Agencycode = EXP_LegalPrimaryAgencycode.o_LegalPrimaryAgencyCode AND LKP_V3_PrimaryAgencyDimID_CAT.eff_from_date <= LKP_V3_PrimaryAgencyDimID_Primary.IN_Trans_Date AND LKP_V3_PrimaryAgencyDimID_CAT.eff_to_date >= LKP_V3_PrimaryAgencyDimID_Primary.IN_Trans_Date
	LEFT JOIN LKP_WorkStopLossLimit_Catastrophe
	ON LKP_WorkStopLossLimit_Catastrophe.PrimaryAgencyCode = EXP_GetValues_Catastrophe.LegalPrimaryAgencyCode AND LKP_WorkStopLossLimit_Catastrophe.Loss_Year = EXP_GetValues_Catastrophe.O_Loss_Year
	LEFT JOIN LKP_WorkStopLossLimit_Catastrophe_Primary
	ON LKP_WorkStopLossLimit_Catastrophe_Primary.PrimaryAgencyCode = EXP_LegalPrimaryAgencycode.o_LegalPrimaryAgencyCode AND LKP_WorkStopLossLimit_Catastrophe_Primary.Loss_Year = EXP_GetValues_Catastrophe.O_Loss_Year
	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_SET_DATE_PART_SET_DATE_PART_SET_DATE_PART_i_RunDate_HH_0_MI_0_SS_0
	ON LKP_CALENDER_DIM_SET_DATE_PART_SET_DATE_PART_SET_DATE_PART_i_RunDate_HH_0_MI_0_SS_0.clndr_date = DATEADD(SECOND,0-DATE_PART(SECOND,DATEADD(MINUTE,0-DATE_PART(MINUTE,DATEADD(HOUR,0-DATE_PART(HOUR,i_RunDate),i_RunDate)),DATEADD(HOUR,0-DATE_PART(HOUR,i_RunDate),i_RunDate))),DATEADD(MINUTE,0-DATE_PART(MINUTE,DATEADD(HOUR,0-DATE_PART(HOUR,i_RunDate),i_RunDate)),DATEADD(HOUR,0-DATE_PART(HOUR,i_RunDate),i_RunDate)))

	LEFT JOIN LKP_STOPLOSSADJUSTMENTCATASTROPHEFACT_PREVIOUSINSTANCE LKP_STOPLOSSADJUSTMENTCATASTROPHEFACT_PREVIOUSINSTANCE_LegalPrimaryAgencyCode_CatastropheCode
	ON LKP_STOPLOSSADJUSTMENTCATASTROPHEFACT_PREVIOUSINSTANCE_LegalPrimaryAgencyCode_CatastropheCode.AgencyCode = LegalPrimaryAgencyCode
	AND LKP_STOPLOSSADJUSTMENTCATASTROPHEFACT_PREVIOUSINSTANCE_LegalPrimaryAgencyCode_CatastropheCode.CatastropheCode = CatastropheCode

),
AGG_ByPrimaryAgencyCode AS (
	SELECT
	o_TotalDirectIncurredLoss AS i_TotalDirectIncurredLoss,
	o_StopLossAdjustmentAmount AS i_StopLossAdjustmentAmount,
	O_ChangeInStopLossAdjustmentAmount AS i_ChangeInStopLossAdjustmentAmount,
	o_AuditId AS AuditId,
	SalesDivisionDimId,
	AgencyDimId,
	CatastropheDimId,
	o_RunDateId AS RunDateId,
	o_StopLossLimit2 AS StopLossLimit,
	PrimaryAgencyDimId,
	-- *INF*: SUM(i_TotalDirectIncurredLoss)
	SUM(i_TotalDirectIncurredLoss) AS o_TotalDirectIncurredLoss,
	-- *INF*: SUM(i_StopLossAdjustmentAmount)
	SUM(i_StopLossAdjustmentAmount) AS o_StopLossAdjustmentAmount,
	-- *INF*: SUM(i_ChangeInStopLossAdjustmentAmount)
	SUM(i_ChangeInStopLossAdjustmentAmount) AS o_ChangeInStopLossAdjustmentAmount
	FROM EXP_CalValues_Catastrophe
	GROUP BY CatastropheDimId, RunDateId, PrimaryAgencyDimId
),
FIL_StopLossAdjustmentAmount_Catastrophe AS (
	SELECT
	AuditId, 
	SalesDivisionDimId, 
	AgencyDimId, 
	CatastropheDimId, 
	RunDateId, 
	o_TotalDirectIncurredLoss AS TotalDirectIncurredLoss, 
	StopLossLimit, 
	o_StopLossAdjustmentAmount AS StopLossAdjustmentAmount, 
	o_ChangeInStopLossAdjustmentAmount AS ChangeInStopLossAdjustmentAmount, 
	PrimaryAgencyDimId
	FROM AGG_ByPrimaryAgencyCode
	WHERE IIF(ChangeInStopLossAdjustmentAmount=0.0 and StopLossAdjustmentAmount<>0.0,'Y',
IIF(ChangeInStopLossAdjustmentAmount<>0.0,'Y','N'))='Y' --AND StopLossLimit>0

--O_ChangeInStopLossAdjustmentAmount<>0 OR StopLossAdjustmentAmount<>0.0

--O_ChangeInStopLossAdjustmentAmount<>0 AND StopLossLimit>0
),
LKP_StopLossAdjustmentCatastropheFact AS (
	SELECT
	StopLossAdjustmentCatastropheFactId,
	SalesDivisionDimID,
	AgencyDimId,
	CatastropheDimId,
	RunDateId
	FROM (
		SELECT a.AgencyDimId                         AS AgencyDimId,
		       a.CatastropheDimId                    AS CatastropheDimId,
		       a.SalesDivisionDimID                  AS SalesDivisionDimID,
		       a.RunDateId                           AS RunDateId,
		       a.StopLossAdjustmentCatastropheFactId AS StopLossAdjustmentCatastropheFactId
		FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.StopLossAdjustmentCatastropheFact a,
		       @{pipeline().parameters.TARGET_TABLE_OWNER}.calendar_dim cd
		WHERE  a.RunDateId = cd.clndr_id
		       AND cd.clndr_yr = Year(dateadd(dd,-1,dateadd(mm,DATEDIFF(MM,0,getdate())-@{pipeline().parameters.NO_OF_MONTHS},0)))
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SalesDivisionDimID,AgencyDimId,CatastropheDimId,RunDateId ORDER BY StopLossAdjustmentCatastropheFactId DESC) = 1
),
RTR_Insert_Update_Catastrophe AS (
	SELECT
	LKP_StopLossAdjustmentCatastropheFact.StopLossAdjustmentCatastropheFactId,
	FIL_StopLossAdjustmentAmount_Catastrophe.AuditId,
	FIL_StopLossAdjustmentAmount_Catastrophe.SalesDivisionDimId AS SalesDivisionDimID,
	FIL_StopLossAdjustmentAmount_Catastrophe.AgencyDimId,
	FIL_StopLossAdjustmentAmount_Catastrophe.PrimaryAgencyDimId,
	FIL_StopLossAdjustmentAmount_Catastrophe.CatastropheDimId,
	FIL_StopLossAdjustmentAmount_Catastrophe.RunDateId,
	FIL_StopLossAdjustmentAmount_Catastrophe.TotalDirectIncurredLoss AS DirectIncurredLossInceptionToDate,
	FIL_StopLossAdjustmentAmount_Catastrophe.StopLossLimit,
	FIL_StopLossAdjustmentAmount_Catastrophe.StopLossAdjustmentAmount,
	FIL_StopLossAdjustmentAmount_Catastrophe.ChangeInStopLossAdjustmentAmount AS O_ChangeInStopLossAdjustmentAmount
	FROM FIL_StopLossAdjustmentAmount_Catastrophe
	LEFT JOIN LKP_StopLossAdjustmentCatastropheFact
	ON LKP_StopLossAdjustmentCatastropheFact.SalesDivisionDimID = FIL_StopLossAdjustmentAmount_Catastrophe.SalesDivisionDimId AND LKP_StopLossAdjustmentCatastropheFact.AgencyDimId = FIL_StopLossAdjustmentAmount_Catastrophe.AgencyDimId AND LKP_StopLossAdjustmentCatastropheFact.CatastropheDimId = FIL_StopLossAdjustmentAmount_Catastrophe.CatastropheDimId AND LKP_StopLossAdjustmentCatastropheFact.RunDateId = FIL_StopLossAdjustmentAmount_Catastrophe.RunDateId
),
RTR_Insert_Update_Catastrophe_INSERT AS (SELECT * FROM RTR_Insert_Update_Catastrophe WHERE ISNULL(StopLossAdjustmentCatastropheFactId)),
RTR_Insert_Update_Catastrophe_DEFAULT1 AS (SELECT * FROM RTR_Insert_Update_Catastrophe WHERE NOT ( (ISNULL(StopLossAdjustmentCatastropheFactId)) )),
TGT_StopLossAdjustmentCatastropheFact_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.StopLossAdjustmentCatastropheFact
	(AuditId, SalesDivisionDimId, AgencyDimId, PrimaryAgencyDimId, CatastropheDimId, RunDateId, DirectIncurredLossInceptionToDate, StopLossLimit, StopLossAdjustmentAmount, ChangeInStopLossAdjustmentAmount)
	SELECT 
	AUDITID, 
	SalesDivisionDimID AS SALESDIVISIONDIMID, 
	PrimaryAgencyDimId AS AGENCYDIMID, 
	PRIMARYAGENCYDIMID, 
	CATASTROPHEDIMID, 
	RUNDATEID, 
	DIRECTINCURREDLOSSINCEPTIONTODATE, 
	STOPLOSSLIMIT, 
	STOPLOSSADJUSTMENTAMOUNT, 
	O_ChangeInStopLossAdjustmentAmount AS CHANGEINSTOPLOSSADJUSTMENTAMOUNT
	FROM RTR_Insert_Update_Catastrophe_INSERT
),
UPD_CatastropheFact AS (
	SELECT
	StopLossAdjustmentCatastropheFactId, 
	AuditId, 
	SalesDivisionDimID, 
	PrimaryAgencyDimId AS AgencyDimId, 
	PrimaryAgencyDimId AS PrimaryAgencyDimId2, 
	CatastropheDimId, 
	RunDateId, 
	DirectIncurredLossInceptionToDate, 
	StopLossLimit, 
	StopLossAdjustmentAmount, 
	O_ChangeInStopLossAdjustmentAmount AS O_ChangeInStopLossAdjustmentAmount2
	FROM RTR_Insert_Update_Catastrophe_DEFAULT1
),
TGT_StopLossAdjustmentCatastropheFact_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.StopLossAdjustmentCatastropheFact AS T
	USING UPD_CatastropheFact AS S
	ON T.StopLossAdjustmentCatastropheFactId = S.StopLossAdjustmentCatastropheFactId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AuditId, T.SalesDivisionDimId = S.SalesDivisionDimID, T.AgencyDimId = S.AgencyDimId, T.PrimaryAgencyDimId = S.PrimaryAgencyDimId2, T.CatastropheDimId = S.CatastropheDimId, T.RunDateId = S.RunDateId, T.DirectIncurredLossInceptionToDate = S.DirectIncurredLossInceptionToDate, T.StopLossLimit = S.StopLossLimit, T.StopLossAdjustmentAmount = S.StopLossAdjustmentAmount, T.ChangeInStopLossAdjustmentAmount = S.O_ChangeInStopLossAdjustmentAmount2
),
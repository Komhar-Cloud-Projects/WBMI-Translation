WITH
LKP_AuditSchedule AS (
	SELECT
	AuditStatus,
	PolicyKey,
	InsuranceLine,
	AuditEffectiveDate,
	AuditExpirationDate
	FROM (
		SELECT 
			AuditStatus,
			PolicyKey,
			InsuranceLine,
			AuditEffectiveDate,
			AuditExpirationDate
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.AuditSchedule
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,InsuranceLine,AuditEffectiveDate,AuditExpirationDate ORDER BY AuditStatus) = 1
),
SQ_Loss AS (
	DECLARE @rundate as datetime        
	SET @rundate = DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + (@{pipeline().parameters.NUM_OF_MONTHS}) +1, 0)
	
	--Claims of PMS Policies
	SELECT DISTINCT
	PolicyCoverage.TypeBureauCode
	,policy.pol_num
	,policy.pol_mod
	,policy.pol_sym
	,RiskLocation.StateProvinceCode
	,policy.pol_eff_date
	,policy.pol_exp_date
	,policy.pol_cancellation_date
	,policy.pol_cancellation_ind
	,PolicyCoverage.InterstateRiskId
	,contract_customer.fed_tax_id
	,policy.pol_term
	,InsuranceSegment.InsuranceSegmentAbbreviation
	,contract_customer.cust_role
	,contract_customer.name
	,contract_customer_address.addr_line_1
	,contract_customer_address.city_name
	,contract_customer_address.state_prov_code
	,contract_customer_address.zip_postal_code
	,0 as Exposure
	,FIRST_VALUE(StatisticalCoverage.classcode) over(partition by claim_party_occurrence.claim_party_occurrence_ak_id order by claim_party_occurrence.eff_from_date desc) Class_Code
	,claim_occurrence.claim_loss_date
	,claim_occurrence.claim_occurrence_num
	,claim_occurrence.s3p_claim_num
	,CASE
	      WHEN (claimant_calculation.claimant_date_type = '2OPEN') 
	      THEN 
	         CASE
	            WHEN (StateDim.StateAbbreviation = 'MI' AND lmf.paid_loss_amt = 0) 
	            THEN '4'
	            ELSE '0'
	         END
	      WHEN (claimant_calculation.claimant_date_type = '4REOPEN') 
	      THEN 
	         CASE
	            WHEN (StateDim.StateAbbreviation = 'MI' OR StateDim.StateAbbreviation = 'WI') 
	            THEN '0'
	            ELSE '2'
	         END
	      ELSE '1'
	   END claim_occurrence_status_code
	,claim_occurrence.claim_cat_code
	,claim_occurrence.loss_loc_state
	,workers_comp_claimant_detail.body_part_code
	,workers_comp_claimant_detail.nature_inj_code
	,workers_comp_claimant_detail.cause_inj_code
	,policy.pol_ak_id
	,claimant_coverage_detail.StatisticalCoverageAKID as CoverageAKID
	,SUBSTRING(workers_comp_claimant_detail.loss_condition,3,2)
	,workers_comp_claimant_detail.managed_care_org_type
	,PolicyCoverage.InsuranceLine
	,case when ltrim(rtrim(substring(workers_comp_claimant_detail.wc_claimant_num,8,3)))='' then '001' 
	else substring(workers_comp_claimant_detail.wc_claimant_num,8,3) end claimant_num
	--prod-12901 change, replace claim_party_occurrence.claimant_num with workers_comp_claimant_detail.wc_claimant_num
	,workers_comp_claimant_detail.jurisdiction_state_code
	,claim_occurrence.ClaimRelationshipKey
	,claim_party_occurrence.claim_party_occurrence_ak_id
	,claim_case.suit_how_claim_closed
	,pa.NoncomplianceofWCPoolAudit
	,claim_occurrence.wc_cat_code
	,case when sup_workers_comp_occupation.occuptn_descript is null
	then 'N/A'
	else  sup_workers_comp_occupation.occuptn_descript
	end  as Occupation_Description
	,
	case when workers_comp_claimant_detail.Avg_Wkly_Wage is null
	then 0
	else workers_comp_claimant_detail.Avg_Wkly_Wage
	end as Weekly_Wage_Amount
	,case when EAF.EmployerLegalAmountPaid is null 
	then 0 
	else EAF.EmployerLegalAmountPaid
	end as EmployerAttorneyFees
	
	FROM    @{pipeline().parameters.SOURCE_DATABASE_NAME_DM}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_fact LMF
	-- Note: Loss_Master_Fact is used due missing historic PMS data from IL table
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME_DM}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_dim LMD
	ON LMF.loss_master_dim_id=LMD.loss_master_dim_id
	
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME_DM}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_dim CCDM
	ON LMF.claimant_cov_dim_id=CCDM.claimant_cov_dim_id
	
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME_DM}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StateDim
	on LMD.risk_state_prov_code = StateDim.StateCode
	
	INNER JOIN   @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail
	ON CCDM.edw_claimant_cov_det_ak_id= claimant_coverage_detail.claimant_cov_det_ak_id AND claimant_coverage_detail.crrnt_snpsht_flag = 1
	AND claimant_coverage_detail.PolicySourceID <> 'DUC'
	
	INNER JOIN   @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence 
	ON claimant_coverage_detail.claim_party_occurrence_ak_id=claim_party_occurrence.claim_party_occurrence_ak_id 
	AND claim_party_occurrence.crrnt_snpsht_flag=1 
	
	INNER JOIN   @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence
	ON claim_party_occurrence.claim_occurrence_ak_id=claim_occurrence.claim_occurrence_ak_id 
	AND claim_occurrence.crrnt_snpsht_flag=1 
	
	INNER JOIN   @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_calculation 
	ON claim_occurrence_calculation.claim_occurrence_ak_id=claim_occurrence.claim_occurrence_ak_id 
	AND claim_occurrence_calculation.crrnt_snpsht_flag=1 
	
	INNER JOIN   @{pipeline().parameters.SOURCE_TABLE_OWNER}.workers_comp_claimant_detail 
	ON workers_comp_claimant_detail.claim_party_occurrence_ak_id=claim_party_occurrence.claim_party_occurrence_ak_id 
	AND workers_comp_claimant_detail.crrnt_snpsht_flag=1 
	
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy 
	ON policy.pol_ak_id=claim_occurrence.pol_key_ak_id 
	and policy.crrnt_snpsht_flag=1
	
	INNER JOIN   @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer
	ON contract_customer.contract_cust_ak_id = policy.contract_cust_ak_id and contract_customer.crrnt_snpsht_flag = 1
	
	INNER JOIN   @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_address
	ON contract_customer_address.contract_cust_ak_id = contract_customer.contract_cust_ak_id and contract_customer_address.crrnt_snpsht_flag = 1
	
	INNER JOIN   @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage 
	ON claimant_coverage_detail.StatisticalCoverageAKID=StatisticalCoverage.StatisticalCoverageAKID 
	and StatisticalCoverage.CurrentSnapshotFlag=1 and StatisticalCoverage.SourceSystemID='PMS'
	
	INNER JOIN   @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage
	ON StatisticalCoverage.PolicyCoverageAKId=PolicyCoverage.PolicyCoverageAKId 
	and PolicyCoverage.CurrentSnapshotFlag=1 
	
	INNER JOIN   @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation 
	ON RiskLocation.RiskLocationAKId=PolicyCoverage.RiskLocationAKId 
	and RiskLocation.CurrentSnapshotFlag=1 
	
	INNER JOIN   @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_calculation 
	ON claim_party_occurrence.claim_party_occurrence_ak_id = claimant_calculation.claim_party_occurrence_ak_id
	and claimant_calculation.eff_from_date <= @rundate
	and claimant_calculation.eff_to_date >= @rundate
	
	LEFT JOIN    @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment
	ON InsuranceSegment.InsuranceSegmentAKId = policy.InsuranceSegmentAKId and InsuranceSegment.CurrentSnapshotFlag = 1
	
	LEFT JOIN   @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_case on 
	claim_party_occurrence.claim_case_ak_id=claim_case.claim_case_ak_id and claim_case.crrnt_snpsht_flag=1
	
	LEFT JOIN   @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyAudit PA on 
	policy.pol_ak_id = PA.PolicyAKId
	and PA.CurrentSnapshotFlag = 1
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_workers_comp_occupation on
	workers_comp_claimant_detail.occuptn_code = sup_workers_comp_occupation.occuptn_code
	and sup_workers_comp_occupation.crrnt_snpsht_flag=1
	LEFT JOIN
	(
	SELECT  SUM(
	CASE  
	             WHEN CPC.claim_pay_ctgry_type = 'SA'
	             THEN CPC.claim_pay_ctgry_amt
	             ELSE 0
	        END) AS EmployerLegalAmountPaid,
	       CO.claim_occurrence_ak_id, 
	       WCCD.claim_party_occurrence_ak_id
	FROM   claim_transaction CT 
	       INNER JOIN claimant_coverage_detail CCD
	               ON CT.claimant_cov_det_ak_id = CCD.claimant_cov_det_ak_id
	                  AND CCD.crrnt_snpsht_flag = 1
	       INNER JOIN claim_payment CP
	               ON CP.claim_pay_ak_id = CT.claim_pay_ak_id
	       INNER JOIN claim_payment_category CPC
	               ON CPC.claim_pay_ak_id = CP.claim_pay_ak_id
	      INNER JOIN claim_party_occurrence CPO
	               ON CPO.claim_party_occurrence_ak_id =
	                  CCD.claim_party_occurrence_ak_id
	                  AND CPO.crrnt_snpsht_flag = 1
	       INNER JOIN workers_comp_claimant_detail WCCD
	               ON WCCD.claim_party_occurrence_ak_id =
	                  CCD.claim_party_occurrence_ak_id
	                  AND WCCD.crrnt_snpsht_flag = 1
	       INNER JOIN claim_occurrence CO
	               ON CO.claim_occurrence_ak_id = CPO.claim_occurrence_ak_id
	                  AND CO.crrnt_snpsht_flag = 1
	WHERE  CT.source_sys_id = 'EXCEED'
	group by        CO.claim_occurrence_ak_id, 
	       WCCD.claim_party_occurrence_ak_id
		   ) EAF
		   on 
		   EAF.claim_party_occurrence_ak_id = claim_party_occurrence.claim_party_occurrence_ak_id
		   AND EAF.claim_occurrence_ak_id = claim_occurrence.claim_occurrence_ak_id 
		   
	WHERE PolicyCoverage.TypeBureauCode in ('WC','WP','WorkersCompensation') 
	and ((DATEDIFF(MM,policy.pol_eff_date, DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}, 0) ))) in (18,30,42,54,66,78,90,102,114,126) 
	
	@{pipeline().parameters.LOSS_WHERE_CLAUSE}
	
	--Claims of DCT Policies
	UNION ALL
	
	SELECT DISTINCT
	PC.TypeBureauCode
	,P.pol_num
	,P.pol_mod
	,P.pol_sym as pol_sym
	,null as StateProvinceCode
	,P.pol_eff_date
	,P.pol_exp_date
	,P.pol_cancellation_date
	,P.pol_cancellation_ind
	,null as InterstateRiskId
	,CCust.fed_tax_id
	,P.pol_term
	,ISeg.InsuranceSegmentAbbreviation
	,CCust.cust_role
	,CCust.name
	,contract_customer_address.addr_line_1
	,contract_customer_address.city_name
	,contract_customer_address.state_prov_code
	,contract_customer_address.zip_postal_code
	,null as Exposure
	,null as Class_Code
	,CO.claim_loss_date
	,CO.claim_occurrence_num
	,ltrim(rtrim(CO.s3p_claim_num)) as s3p_claim_num
	,CASE
	      WHEN (CC.claimant_date_type = '2OPEN') 
	      THEN 
	         CASE
	            WHEN (SS.state_code = 'MI' AND LMC.paid_loss_amt = 0) 
	            THEN '4'
	            ELSE '0'
	         END
	      WHEN (CC.claimant_date_type = '4REOPEN') 
	      THEN 
	         CASE
	            WHEN (SS.state_code = 'MI' OR SS.state_code = 'WI') 
	            THEN '0'
	            ELSE '2'
	         END
	      ELSE '1'
	   END as claim_occurrence_status_code
	,CO.claim_cat_code
	,CO.loss_loc_state
	,WCCD.body_part_code
	,WCCD.nature_inj_code
	,WCCD.cause_inj_code
	,P.pol_ak_id
	,CCD.RatingCoverageAKID as CoverageAKID
	,SUBSTRING(WCCD.loss_condition,3,2) as loss_condition
	,WCCD.managed_care_org_type
	,PC.InsuranceLine
	,case when ltrim(rtrim(substring(WCCD.wc_claimant_num,8,3)))='' then '001' else substring(WCCD.wc_claimant_num,8,3) end as claimant_num
	,WCCD.jurisdiction_state_code
	,CO.ClaimRelationshipKey
	,CPO.claim_party_occurrence_ak_id
	,ISNULL(CCase.suit_how_claim_closed,'N/A') as suit_how_claim_closed
	,pa.NoncomplianceofWCPoolAudit
	,CO.wc_cat_code
	,case when sup_workers_comp_occupation.occuptn_descript is null
	then 'N/A'
	else  sup_workers_comp_occupation.occuptn_descript
	end  as Occupation_Description
	,case when WCCD.Avg_Wkly_Wage is null
	then 0
	else WCCD.Avg_Wkly_Wage
	end as Weekly_Wage_Amount
	,case when EAF.EmployerLegalAmountPaid is null 
	then 0 
	else EAF.EmployerLegalAmountPaid
	end as EmployerAttorneyFees
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_calculation LMC INNER JOIN dbo.claim_transaction CT ON LMC.claim_trans_ak_id = CT.claim_trans_ak_id
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD ON CCD.claimant_cov_det_ak_id =  CT.claimant_cov_det_ak_id and CCD.crrnt_snpsht_flag = 1 AND CCD.PolicySourceID = 'DUC'
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence CPO on CCD.claim_party_occurrence_ak_id = CPO.claim_party_occurrence_ak_id and CPO.crrnt_snpsht_flag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence CO ON CPO.claim_occurrence_ak_id=CO.claim_occurrence_ak_id AND CO.crrnt_snpsht_flag=1 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.workers_comp_claimant_detail  WCCD ON WCCD.claim_party_occurrence_ak_id=CPO.claim_party_occurrence_ak_id AND WCCD.crrnt_snpsht_flag=1 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P ON P.pol_ak_id=CO.pol_key_ak_id and P.crrnt_snpsht_flag=1
	INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer CCust ON CCust.contract_cust_ak_id = P.contract_cust_ak_id and CCust.crrnt_snpsht_flag = 1
	INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_address ON contract_customer_address.contract_cust_ak_id = CCust.contract_cust_ak_id and contract_customer_address.crrnt_snpsht_flag = 1
	INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_calculation CC ON CPO.claim_party_occurrence_ak_id = CC.claim_party_occurrence_ak_id
	and CC.eff_from_date <= @rundate and CC.eff_to_date >= @rundate
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_state SS ON LMC.risk_state_prov_code = SS.state_abbrev
	LEFT JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISeg ON ISeg.InsuranceSegmentAKId = P.InsuranceSegmentAKId and ISeg.CurrentSnapshotFlag = 1
	LEFT JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_case CCase ON CPO.claim_case_ak_id=CCase.claim_case_ak_id and CCase.crrnt_snpsht_flag=1
	INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC ON PC.PolicyAKID=P.pol_ak_id and PC.CurrentSnapshotFlag=1 
	
	LEFT JOIN   @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyAudit PA on 
	p.pol_ak_id = PA.PolicyAKId
	and PA.CurrentSnapshotFlag = 1
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_workers_comp_occupation on
	wccd.occuptn_code = sup_workers_comp_occupation.occuptn_code
	and sup_workers_comp_occupation.crrnt_snpsht_flag=1
	LEFT JOIN
	(
	SELECT  SUM(
	CASE  
	             WHEN CPC.claim_pay_ctgry_type = 'SA'
	             THEN CPC.claim_pay_ctgry_amt
	             ELSE 0
	        END) AS EmployerLegalAmountPaid,
	       CO.claim_occurrence_ak_id, 
	       WCCD.claim_party_occurrence_ak_id
	FROM   claim_transaction CT 
	       INNER JOIN claimant_coverage_detail CCD
	               ON CT.claimant_cov_det_ak_id = CCD.claimant_cov_det_ak_id
	                  AND CCD.crrnt_snpsht_flag = 1
	       INNER JOIN claim_payment CP
	               ON CP.claim_pay_ak_id = CT.claim_pay_ak_id
	       INNER JOIN claim_payment_category CPC
	               ON CPC.claim_pay_ak_id = CP.claim_pay_ak_id
	      INNER JOIN claim_party_occurrence CPO
	               ON CPO.claim_party_occurrence_ak_id =
	                  CCD.claim_party_occurrence_ak_id
	                  AND CPO.crrnt_snpsht_flag = 1
	       INNER JOIN workers_comp_claimant_detail WCCD
	               ON WCCD.claim_party_occurrence_ak_id =
	                  CCD.claim_party_occurrence_ak_id
	                  AND WCCD.crrnt_snpsht_flag = 1
	       INNER JOIN claim_occurrence CO
	               ON CO.claim_occurrence_ak_id = CPO.claim_occurrence_ak_id
	                  AND CO.crrnt_snpsht_flag = 1
	WHERE  CT.source_sys_id = 'EXCEED'
	group by        CO.claim_occurrence_ak_id, 
	       WCCD.claim_party_occurrence_ak_id
		   ) EAF
		   on 
		   EAF.claim_party_occurrence_ak_id = CPO.claim_party_occurrence_ak_id
		   AND EAF.claim_occurrence_ak_id = CO.claim_occurrence_ak_id  
	
	WHERE ((DATEDIFF(MM,P.pol_eff_date, DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}, 0) ))) in (18,30,42,54,66,78,90,102,114,126)
	AND PC.TypeBureauCode in ('WC','WP','WorkersCompensation')  
	
	@{pipeline().parameters.LOSS_WHERE_CLAUSE_DCT}
),
EXP_Loss_Metadata AS (
	SELECT
	TypeBureauCode,
	pol_num,
	pol_mod,
	pol_sym,
	StateProvinceCode,
	pol_eff_date,
	pol_exp_date,
	pol_cancellation_date,
	pol_cancellation_ind,
	InterstateRiskId,
	fed_tax_id,
	pol_term,
	InsuranceSegmentAbbreviation,
	cust_role,
	name,
	addr_line_1,
	city_name,
	state_prov_code,
	zip_postal_code,
	Exposure,
	class_code,
	claim_loss_date,
	claim_occurrence_num,
	claim_num,
	-- *INF*: RTRIM(LTRIM(claim_num))
	RTRIM(LTRIM(claim_num)) AS v_s3p_claim_num,
	v_s3p_claim_num AS o_s3p_claim_num,
	claimant_status_type AS in_claimant_status_type,
	-- *INF*: LTRIM(RTRIM(in_claimant_status_type))
	LTRIM(RTRIM(in_claimant_status_type)) AS o_claimant_status_type,
	claim_cat_code,
	loss_loc_state,
	body_part_code,
	nature_inj_code,
	cause_inj_code,
	pol_ak_id,
	CoverageAKID,
	loss_condition,
	managed_care_org_type,
	InsuranceLine AS i_InsuranceLine,
	claimant_num AS in_claimant_num,
	-- *INF*: LTRIM(RTRIM(in_claimant_num))
	LTRIM(RTRIM(in_claimant_num)) AS o_claimant_num,
	pol_sym || pol_num || pol_mod AS v_PolicyKey,
	-- *INF*: :LKP.LKP_AUDITSCHEDULE(v_PolicyKey, i_InsuranceLine, pol_eff_date)
	LKP_AUDITSCHEDULE_v_PolicyKey_i_InsuranceLine_pol_eff_date.AuditStatus AS v_AuditStatus,
	jurisdiction_state_code,
	v_AuditStatus AS o_AuditStatus,
	NoncomplianceofWCPoolAudit,
	-- *INF*: IIF(NoncomplianceofWCPoolAudit=1,'U',DECODE(TRUE,
	--  IN(UPPER(v_AuditStatus), 'BYPASSED', 'REVERSED', 'OVERDUE'),
	-- 'Y',
	-- IN(StateProvinceCode, '21', '48') AND UPPER(v_AuditStatus)='ESTIMATED',
	-- 'U',
	-- 'N'
	--  )
	-- )
	IFF(
	    NoncomplianceofWCPoolAudit = 1, 'U',
	    DECODE(
	        TRUE,
	        UPPER(v_AuditStatus) IN ('BYPASSED','REVERSED','OVERDUE'), 'Y',
	        StateProvinceCode IN ('21','48')
	    and UPPER(v_AuditStatus) = 'ESTIMATED', 'U',
	        'N'
	    )
	) AS o_EstimatedAuditCode,
	ClaimRelationshipKey,
	claim_party_occurrence_ak_id,
	suit_how_claim_closed,
	wc_cat_code,
	OccupationDescription,
	WeeklyWageAmount,
	EmployerAttorneyFees
	FROM SQ_Loss
	LEFT JOIN LKP_AUDITSCHEDULE LKP_AUDITSCHEDULE_v_PolicyKey_i_InsuranceLine_pol_eff_date
	ON LKP_AUDITSCHEDULE_v_PolicyKey_i_InsuranceLine_pol_eff_date.PolicyKey = v_PolicyKey
	AND LKP_AUDITSCHEDULE_v_PolicyKey_i_InsuranceLine_pol_eff_date.InsuranceLine = i_InsuranceLine
	AND LKP_AUDITSCHEDULE_v_PolicyKey_i_InsuranceLine_pol_eff_date.AuditEffectiveDate = pol_eff_date

),
LKP_ClaimPayCtgryType AS (
	SELECT
	o_claim_pay_ctgry_type,
	claim_num,
	claim_party_occurrence_ak_id
	FROM (
		select 
		o_claim_pay_ctgry_type as o_claim_pay_ctgry_type,
		s_ordr as s_ordr,
		clndr_date as clndr_date,
		claim_num as claim_num,
		claim_party_occurrence_ak_id as claim_party_occurrence_ak_id
		from
		(SELECT a.o_claim_pay_ctgry_type as o_claim_pay_ctgry_type ,
				a.s_ordr as s_ordr,
				a.clndr_date as clndr_date,
				a.claim_num as claim_num,
				a.edw_claim_party_occurrence_ak_id as claim_party_occurrence_ak_id,
				row_number() over (partition by a.edw_claim_party_occurrence_ak_id ORDER BY a.claim_num,a.s_ordr,a.clndr_date desc) row_num
		FROM 
		(SELECT  distinct 
			 case  when cd.inj_result_death_ind = 'Y' then 'DT'
		         when cpctd.claim_pay_ctgry_type in ('DT','PT','PP','PB','DF','PD','TD','VR','SI','1B') then cpctd.claim_pay_ctgry_type  
				  else 'ZZ'  
			 end AS o_claim_pay_ctgry_type,  
			 case when cd.inj_result_death_ind = 'Y' then 0
			            when cpctd.claim_pay_ctgry_type in ('DT','PT','PP','PB','DF','PD','TD','VR','SI','1B') then 1  
			 else 2 
			 end AS s_ordr,
			 cod.claim_num as claim_num,
			 c.clndr_date
			 ,cd.edw_claim_party_occurrence_ak_id
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_loss_transaction_fact cltf
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_payment_category_type_dim cpctd on cpctd.claim_pay_ctgry_type_dim_id = cltf.claim_pay_ctgry_type_dim_id
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction_type_dim ctyd on cltf.claim_trans_type_dim_id = ctyd.claim_trans_type_dim_id 
		and trans_kind_code = 'D' 
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim c on cltf.claim_trans_date_id=c.clndr_id
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_occurrence_dim cod on cltf.claim_occurrence_dim_id = cod.claim_occurrence_dim_id
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_dim cd on cd.claimant_dim_id = cltf.claimant_dim_id
		WHERE c.clndr_date < DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0)
		) a
		)x
		where row_num = 1
		--ORDER BY claim_num,s_ordr,clndr_date desc, claim_party_occurrence_ak_id, o_claim_pay_ctgry_type --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_num,claim_party_occurrence_ak_id ORDER BY o_claim_pay_ctgry_type) = 1
),
LKP_RatingCoverage_DCT AS (
	SELECT
	StateProvinceCode,
	InterstateRiskId,
	Exposure,
	ClassCode,
	RatingCoverageAKID,
	PolicyAKID
	FROM (
		SELECT DISTINCT RL.stateprovincecode  AS StateProvinceCode, 
		                PC.interstateriskid   AS InterstateRiskId, 
		                RC.exposure           AS Exposure, 
		                RC.classcode          AS ClassCode, 
		                P.pol_ak_id           AS PolicyAKID, 
		                RC.ratingcoverageakid AS RatingCoverageAKID 
		FROM   v2.policy P INNER JOIN dbo.risklocation RL ON P.pol_ak_id = RL.policyakid AND P.crrnt_snpsht_flag = 1 
		       INNER JOIN dbo.policycoverage PC ON RL.risklocationakid = PC.risklocationakid AND RL.currentsnapshotflag = 1 
		       INNER JOIN dbo.ratingcoverage RC ON PC.policycoverageakid = RC.policycoverageakid 
		WHERE  EXISTS (SELECT 1 FROM   loss_master_calculation LMC WHERE  P.pol_ak_id = LMC.pol_ak_id)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID,RatingCoverageAKID ORDER BY StateProvinceCode DESC) = 1
),
lkp_claim_loss_transactions_fact_calculate_ITD AS (
	SELECT
	claim_party_occurrence_ak_id,
	claim_pay_ctgry_lump_sum_ind,
	PaidIndemnityAmount,
	PaidMedicalAmount,
	DeductibleReimbursementAmount,
	PaidAllocatedLossAdjustmentExpenseAmount,
	IncurredAllocatedLossAdjustmentExpenseAmount,
	IncurredIndemnityAmount,
	IncurredMedicalAmount,
	TypeOfRecoveryCode,
	type_of_loss_code,
	body_part_code,
	nature_of_inj_code,
	cause_inj_code
	FROM (
		SELECT
			MAX(CASE claim_payment_category_type_dim.claim_pay_ctgry_lump_sum_ind
				WHEN 'N/A' THEN 'N'
				ELSE claim_payment_category_type_dim.claim_pay_ctgry_lump_sum_ind
			END) AS claim_pay_ctgry_lump_sum_ind,
			SUM((CASE claimant_coverage_dim.cause_of_loss
				WHEN '05' THEN claim_loss_transaction_fact.direct_loss_paid_including_recoveries
				ELSE 0
			END)) AS PaidIndemnityAmount,
			SUM((CASE claimant_coverage_dim.cause_of_loss
				WHEN '06' THEN (CASE
						WHEN trans_ctgry_code NOT IN ('DX') THEN claim_loss_transaction_fact.direct_loss_paid_including_recoveries
						ELSE 0
					END)
					+
					(CASE
						WHEN trans_ctgry_code = 'WD' THEN ABS(claim_loss_transaction_fact.direct_other_recovery_paid)
						WHEN trans_ctgry_code = 'DR' THEN (CASE
								WHEN s3p_trans_code IN ('28', '29') THEN -(ABS(claim_loss_transaction_fact.direct_other_recovery_paid))
								ELSE ABS(claim_loss_transaction_fact.direct_other_recovery_paid)
							END)
						ELSE 0
					END)
				ELSE 0
			END)) AS PaidMedicalAmount,
			ABS(SUM(CASE
				WHEN claim_transaction_type_dim.trans_ctgry_code IN ('DX', 'WD', 'DR') THEN claim_loss_transaction_fact.direct_loss_paid_including_recoveries
				ELSE 0
			END)) AS DeductibleReimbursementAmount,
			SUM(claim_loss_transaction_fact.direct_alae_paid_including_recoveries) AS PaidAllocatedLossAdjustmentExpenseAmount,
			SUM(claim_loss_transaction_fact.direct_alae_incurred_including_recoveries) AS IncurredAllocatedLossAdjustmentExpenseAmount,
			SUM((CASE claimant_coverage_dim.cause_of_loss
				WHEN '05' THEN (claim_loss_transaction_fact.direct_loss_incurred_including_recoveries)
				ELSE 0
			END)) AS IncurredIndemnityAmount,
			SUM((CASE claimant_coverage_dim.cause_of_loss
				WHEN '06' THEN (CASE
						WHEN trans_ctgry_code NOT IN ('DX') THEN claim_loss_transaction_fact.direct_loss_incurred_including_recoveries
						ELSE 0
					END)
					+
					(CASE
						WHEN trans_ctgry_code = 'WD' THEN ABS(claim_loss_transaction_fact.direct_other_recovery_paid)
						WHEN trans_ctgry_code = 'DR' THEN (CASE
								WHEN s3p_trans_code IN ('28', '29') THEN -(ABS(claim_loss_transaction_fact.direct_other_recovery_paid))
								ELSE ABS(claim_loss_transaction_fact.direct_other_recovery_paid)
							END)
						ELSE 0
					END)
				ELSE 0
			END)) AS IncurredMedicalAmount,
			MAX(CASE
				WHEN (claim_transaction_type_dim.pms_trans_code IN ('81', '82', '83', '84', '85', '86', '87', '88', '89')) AND
					claim_loss_transaction_fact.direct_subrogation_paid <> 0 THEN '03'
				ELSE '01'
			END) AS TypeOfRecoveryCode,
			MAX(claimant_dim.type_of_loss_code) AS type_of_loss_code,
			MIN(CASE
				WHEN body_part_code = 'N/A' THEN 'ZZ'
				ELSE body_part_code
			END) AS body_part_code,
			MIN(claimant_dim.nature_inj_code) AS nature_of_inj_code,
			MIN(cause_inj_code) AS cause_inj_code,
			claimant_dim.edw_claim_party_occurrence_ak_id AS claim_party_occurrence_ak_id
		FROM claim_loss_transaction_fact
		INNER JOIN claim_payment_category_type_dim
			ON claim_payment_category_type_dim.claim_pay_ctgry_type_dim_id = claim_loss_transaction_fact.claim_pay_ctgry_type_dim_id
		INNER JOIN claimant_coverage_dim
			ON claimant_coverage_dim.claimant_cov_dim_id = claim_loss_transaction_fact.claimant_cov_dim_id
		INNER JOIN claim_transaction_type_dim
			ON claim_loss_transaction_fact.claim_trans_type_dim_id = claim_transaction_type_dim.claim_trans_type_dim_id
			AND trans_kind_code = 'D'
		INNER JOIN claimant_dim claimant_dim
			ON claimant_dim.claimant_dim_id = claim_loss_transaction_fact.claimant_dim_id
		INNER JOIN calendar_dim c
			ON claim_loss_transaction_fact.claim_trans_date_id = c.clndr_id
		WHERE c.clndr_date < DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS} + 1, 0)
		GROUP BY claimant_dim.edw_claim_party_occurrence_ak_id
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id ORDER BY claim_party_occurrence_ak_id) = 1
),
EXP_Extract_All AS (
	SELECT
	LKP_RatingCoverage_DCT.StateProvinceCode AS StateProvinceCode_DCT,
	-- *INF*: IIF(ISNULL(StateProvinceCode_DCT),'N/A',StateProvinceCode_DCT)
	IFF(StateProvinceCode_DCT IS NULL, 'N/A', StateProvinceCode_DCT) AS v_StateProvinceCode_DCT,
	LKP_RatingCoverage_DCT.InterstateRiskId AS InterstateRiskId_DCT,
	-- *INF*: IIF(ISNULL(InterstateRiskId_DCT),'N/A',InterstateRiskId_DCT)
	IFF(InterstateRiskId_DCT IS NULL, 'N/A', InterstateRiskId_DCT) AS v_InterstateRiskId_DCT,
	LKP_RatingCoverage_DCT.Exposure AS Exposure_DCT,
	-- *INF*: IIF(ISNULL(Exposure_DCT),0.00,Exposure_DCT)
	IFF(Exposure_DCT IS NULL, 0.00, Exposure_DCT) AS v_Exposure_DCT,
	LKP_RatingCoverage_DCT.ClassCode AS ClassCode_DCT,
	-- *INF*: IIF(ISNULL(ClassCode_DCT),'N/A',ClassCode_DCT)
	IFF(ClassCode_DCT IS NULL, 'N/A', ClassCode_DCT) AS v_ClassCode_DCT,
	LKP_RatingCoverage_DCT.RatingCoverageAKID,
	LKP_RatingCoverage_DCT.PolicyAKID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Auditid,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	EXP_Loss_Metadata.TypeBureauCode,
	EXP_Loss_Metadata.pol_num,
	EXP_Loss_Metadata.pol_mod,
	EXP_Loss_Metadata.pol_sym,
	-- *INF*: IIF(pol_sym='000','',pol_sym)
	IFF(pol_sym = '000', '', pol_sym) AS o_pol_sym,
	EXP_Loss_Metadata.StateProvinceCode,
	-- *INF*: IIF(pol_sym = '000', v_StateProvinceCode_DCT, StateProvinceCode)
	IFF(pol_sym = '000', v_StateProvinceCode_DCT, StateProvinceCode) AS o_StateProvinceCode,
	EXP_Loss_Metadata.pol_eff_date,
	EXP_Loss_Metadata.pol_exp_date,
	EXP_Loss_Metadata.pol_cancellation_date,
	EXP_Loss_Metadata.pol_cancellation_ind,
	EXP_Loss_Metadata.InterstateRiskId,
	-- *INF*: IIF(pol_sym = '000',v_InterstateRiskId_DCT, InterstateRiskId) 
	IFF(pol_sym = '000', v_InterstateRiskId_DCT, InterstateRiskId) AS o_InterstateRiskId,
	EXP_Loss_Metadata.fed_tax_id,
	EXP_Loss_Metadata.pol_term,
	EXP_Loss_Metadata.InsuranceSegmentAbbreviation,
	EXP_Loss_Metadata.cust_role,
	EXP_Loss_Metadata.name,
	EXP_Loss_Metadata.addr_line_1,
	EXP_Loss_Metadata.city_name,
	EXP_Loss_Metadata.state_prov_code,
	EXP_Loss_Metadata.zip_postal_code,
	EXP_Loss_Metadata.Exposure,
	-- *INF*: IIF(pol_sym = '000', v_Exposure_DCT, Exposure) 
	IFF(pol_sym = '000', v_Exposure_DCT, Exposure) AS O_Exposure,
	EXP_Loss_Metadata.class_code,
	-- *INF*: IIF(pol_sym = '000', v_ClassCode_DCT, class_code) 
	IFF(pol_sym = '000', v_ClassCode_DCT, class_code) AS O_class_code,
	EXP_Loss_Metadata.claim_loss_date,
	EXP_Loss_Metadata.claim_occurrence_num,
	EXP_Loss_Metadata.o_s3p_claim_num,
	EXP_Loss_Metadata.o_claimant_status_type,
	EXP_Loss_Metadata.claim_cat_code,
	EXP_Loss_Metadata.loss_loc_state,
	EXP_Loss_Metadata.body_part_code,
	EXP_Loss_Metadata.nature_inj_code,
	EXP_Loss_Metadata.cause_inj_code,
	EXP_Loss_Metadata.pol_ak_id,
	EXP_Loss_Metadata.CoverageAKID,
	EXP_Loss_Metadata.loss_condition,
	EXP_Loss_Metadata.managed_care_org_type,
	EXP_Loss_Metadata.o_claimant_num,
	EXP_Loss_Metadata.jurisdiction_state_code,
	EXP_Loss_Metadata.o_AuditStatus,
	lkp_claim_loss_transactions_fact_calculate_ITD.claim_pay_ctgry_lump_sum_ind AS o_claim_pay_ctgry_lump_sum_ind,
	EXP_Loss_Metadata.o_EstimatedAuditCode,
	EXP_Loss_Metadata.ClaimRelationshipKey,
	LKP_ClaimPayCtgryType.o_claim_pay_ctgry_type,
	lkp_claim_loss_transactions_fact_calculate_ITD.PaidIndemnityAmount,
	lkp_claim_loss_transactions_fact_calculate_ITD.PaidMedicalAmount,
	lkp_claim_loss_transactions_fact_calculate_ITD.DeductibleReimbursementAmount,
	lkp_claim_loss_transactions_fact_calculate_ITD.PaidAllocatedLossAdjustmentExpenseAmount,
	lkp_claim_loss_transactions_fact_calculate_ITD.IncurredAllocatedLossAdjustmentExpenseAmount,
	lkp_claim_loss_transactions_fact_calculate_ITD.IncurredIndemnityAmount,
	lkp_claim_loss_transactions_fact_calculate_ITD.IncurredMedicalAmount,
	lkp_claim_loss_transactions_fact_calculate_ITD.TypeOfRecoveryCode,
	lkp_claim_loss_transactions_fact_calculate_ITD.type_of_loss_code,
	lkp_claim_loss_transactions_fact_calculate_ITD.body_part_code AS body_part_code1,
	lkp_claim_loss_transactions_fact_calculate_ITD.nature_of_inj_code,
	lkp_claim_loss_transactions_fact_calculate_ITD.cause_inj_code AS cause_inj_code1,
	EXP_Loss_Metadata.claim_party_occurrence_ak_id,
	EXP_Loss_Metadata.suit_how_claim_closed,
	EXP_Loss_Metadata.wc_cat_code,
	-- *INF*: wc_cat_code
	-- 
	-- -- Passing COVID cat codes to the claim_cat_code in target definition table - US 189306
	wc_cat_code AS o_claim_cat_code,
	EXP_Loss_Metadata.OccupationDescription,
	EXP_Loss_Metadata.WeeklyWageAmount,
	EXP_Loss_Metadata.EmployerAttorneyFees
	FROM EXP_Loss_Metadata
	LEFT JOIN LKP_ClaimPayCtgryType
	ON LKP_ClaimPayCtgryType.claim_num = EXP_Loss_Metadata.o_s3p_claim_num AND LKP_ClaimPayCtgryType.claim_party_occurrence_ak_id = EXP_Loss_Metadata.claim_party_occurrence_ak_id
	LEFT JOIN LKP_RatingCoverage_DCT
	ON LKP_RatingCoverage_DCT.PolicyAKID = EXP_Loss_Metadata.pol_ak_id AND LKP_RatingCoverage_DCT.RatingCoverageAKID = EXP_Loss_Metadata.CoverageAKID
	LEFT JOIN lkp_claim_loss_transactions_fact_calculate_ITD
	ON lkp_claim_loss_transactions_fact_calculate_ITD.claim_party_occurrence_ak_id = EXP_Loss_Metadata.claim_party_occurrence_ak_id
),
AGG_RemoveDuplicateData AS (
	SELECT
	Auditid,
	CreatedDate,
	ModifiedDate,
	TypeBureauCode,
	pol_num,
	pol_mod,
	o_pol_sym,
	o_StateProvinceCode,
	pol_eff_date,
	pol_exp_date,
	pol_cancellation_date,
	pol_cancellation_ind,
	o_InterstateRiskId,
	fed_tax_id,
	pol_term,
	InsuranceSegmentAbbreviation,
	cust_role,
	name,
	addr_line_1,
	city_name,
	state_prov_code,
	zip_postal_code,
	O_Exposure,
	O_class_code,
	claim_loss_date,
	claim_occurrence_num,
	o_s3p_claim_num,
	o_claimant_status_type,
	o_claim_cat_code AS claim_cat_code,
	loss_loc_state,
	pol_ak_id,
	loss_condition,
	managed_care_org_type,
	o_claim_pay_ctgry_lump_sum_ind,
	o_EstimatedAuditCode,
	o_AuditStatus,
	o_claim_pay_ctgry_type,
	PaidIndemnityAmount,
	PaidMedicalAmount,
	DeductibleReimbursementAmount,
	PaidAllocatedLossAdjustmentExpenseAmount,
	IncurredAllocatedLossAdjustmentExpenseAmount,
	IncurredIndemnityAmount,
	IncurredMedicalAmount,
	o_claimant_num,
	jurisdiction_state_code,
	claim_party_occurrence_ak_id,
	TypeOfRecoveryCode,
	type_of_loss_code,
	body_part_code1,
	nature_of_inj_code,
	cause_inj_code1,
	ClaimRelationshipKey,
	suit_how_claim_closed,
	OccupationDescription,
	WeeklyWageAmount,
	EmployerAttorneyFees
	FROM EXP_Extract_All
	QUALIFY ROW_NUMBER() OVER (PARTITION BY o_s3p_claim_num, pol_ak_id, claim_party_occurrence_ak_id ORDER BY NULL) = 1
),
exp_Before_Target AS (
	SELECT
	Auditid,
	CreatedDate,
	ModifiedDate,
	TypeBureauCode,
	pol_num,
	pol_mod,
	o_pol_sym,
	o_StateProvinceCode,
	pol_eff_date,
	pol_exp_date,
	pol_cancellation_date,
	pol_cancellation_ind,
	o_InterstateRiskId,
	fed_tax_id,
	pol_term,
	InsuranceSegmentAbbreviation,
	cust_role,
	name,
	addr_line_1,
	city_name,
	state_prov_code,
	zip_postal_code,
	O_Exposure,
	O_class_code,
	claim_loss_date,
	claim_occurrence_num,
	o_s3p_claim_num,
	o_claimant_status_type,
	claim_cat_code,
	loss_loc_state,
	pol_ak_id,
	loss_condition,
	managed_care_org_type,
	o_claim_pay_ctgry_lump_sum_ind,
	o_EstimatedAuditCode,
	o_AuditStatus,
	o_claim_pay_ctgry_type,
	PaidIndemnityAmount,
	PaidMedicalAmount,
	DeductibleReimbursementAmount,
	PaidAllocatedLossAdjustmentExpenseAmount,
	IncurredAllocatedLossAdjustmentExpenseAmount,
	IncurredIndemnityAmount,
	IncurredMedicalAmount,
	o_claimant_num,
	jurisdiction_state_code,
	claim_party_occurrence_ak_id,
	TypeOfRecoveryCode AS in_TypeOfRecoveryCode,
	-- *INF*: iif(isnull(in_TypeOfRecoveryCode), 'N/A',in_TypeOfRecoveryCode)
	-- 
	IFF(in_TypeOfRecoveryCode IS NULL, 'N/A', in_TypeOfRecoveryCode) AS TypeOfRecoveryCode1,
	type_of_loss_code AS in_type_of_loss_code,
	-- *INF*: iif(isnull(in_type_of_loss_code), 'N/A',in_type_of_loss_code)
	IFF(in_type_of_loss_code IS NULL, 'N/A', in_type_of_loss_code) AS type_of_loss_code,
	body_part_code1 AS in_body_part_code,
	-- *INF*: iif(isnull(in_body_part_code), 'N/A',in_body_part_code)
	IFF(in_body_part_code IS NULL, 'N/A', in_body_part_code) AS body_part_code,
	nature_of_inj_code AS in_nature_of_inj_code,
	-- *INF*: iif(isnull(in_nature_of_inj_code), 'N/A',in_nature_of_inj_code)
	-- 
	IFF(in_nature_of_inj_code IS NULL, 'N/A', in_nature_of_inj_code) AS nature_of_inj_code,
	cause_inj_code1 AS in_cause_inj_code,
	-- *INF*: iif(isnull(in_cause_inj_code), 'N/A',in_cause_inj_code)
	-- 
	IFF(in_cause_inj_code IS NULL, 'N/A', in_cause_inj_code) AS cause_inj_code,
	ClaimRelationshipKey,
	suit_how_claim_closed,
	OccupationDescription,
	WeeklyWageAmount,
	EmployerAttorneyFees
	FROM AGG_RemoveDuplicateData
),
WorkWCSTATLoss AS (

	------------ PRE SQL ----------
	@{pipeline().parameters.DELETE_PRESQL}
	-------------------------------


	INSERT INTO Shortcut_to_WorkWCSTATLoss
	(AuditId, CreatedDate, ModifiedDate, TypeBureauCode, PolicyNumber, PolicyModulus, PolicySymbol, StateProvinceCode, PolicyEffectiveDate, PolicyExpiryDate, PolicyCancellationDate, PolicyCancellationIndicator, WCInterStateRiskId, FederalTaxId, PolicyTerm, InsuranceSegmentAbbreviation, CustomerRole, Name, AddressLine1, CityName, StateProvCodeContractCustomerAddress, ZipPostalCode, Exposure, ClassCode, ClaimLossDate, ClaimOccurrenceNumber, S3pClaimNumber, ClaimantStatusType, ClaimCatCode, LossLocationState, PolicyAKId, LossCondition, ManagedCareOrganisationType, ClaimPayCategoryLumpSumInd, EstimatedAuditCode, AuditStatus, ClaimPayCategoryType, PaidIndemnityAmount, PaidMedicalAmount, DeductibleReimbursementAmount, PaidAllocatedLossAdjustmentExpenseAmount, IncurredAllocatedLossAdjustmentExpenseAmount, IncurredIndemnityAmount, IncurredMedicalAmount, ClaimantNum, JurisdictionStateCode, ClaimPartyOccurrenceAKId, TypeOfRecoveryCode, TypeOfLossCode, BodyPartCode, NatureOfInjCode, CauseInjCode, ClaimRelationshipKey, SuitHowClaimClosed, OccupationDescription, WeeklyWageAmount, EmployerAttorneyFees)
	SELECT 
	Auditid AS AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	TYPEBUREAUCODE, 
	pol_num AS POLICYNUMBER, 
	pol_mod AS POLICYMODULUS, 
	o_pol_sym AS POLICYSYMBOL, 
	o_StateProvinceCode AS STATEPROVINCECODE, 
	pol_eff_date AS POLICYEFFECTIVEDATE, 
	pol_exp_date AS POLICYEXPIRYDATE, 
	pol_cancellation_date AS POLICYCANCELLATIONDATE, 
	pol_cancellation_ind AS POLICYCANCELLATIONINDICATOR, 
	o_InterstateRiskId AS WCINTERSTATERISKID, 
	fed_tax_id AS FEDERALTAXID, 
	pol_term AS POLICYTERM, 
	INSURANCESEGMENTABBREVIATION, 
	cust_role AS CUSTOMERROLE, 
	name AS NAME, 
	addr_line_1 AS ADDRESSLINE1, 
	city_name AS CITYNAME, 
	state_prov_code AS STATEPROVCODECONTRACTCUSTOMERADDRESS, 
	zip_postal_code AS ZIPPOSTALCODE, 
	O_Exposure AS EXPOSURE, 
	O_class_code AS CLASSCODE, 
	claim_loss_date AS CLAIMLOSSDATE, 
	claim_occurrence_num AS CLAIMOCCURRENCENUMBER, 
	o_s3p_claim_num AS S3PCLAIMNUMBER, 
	o_claimant_status_type AS CLAIMANTSTATUSTYPE, 
	claim_cat_code AS CLAIMCATCODE, 
	loss_loc_state AS LOSSLOCATIONSTATE, 
	pol_ak_id AS POLICYAKID, 
	loss_condition AS LOSSCONDITION, 
	managed_care_org_type AS MANAGEDCAREORGANISATIONTYPE, 
	o_claim_pay_ctgry_lump_sum_ind AS CLAIMPAYCATEGORYLUMPSUMIND, 
	o_EstimatedAuditCode AS ESTIMATEDAUDITCODE, 
	o_AuditStatus AS AUDITSTATUS, 
	o_claim_pay_ctgry_type AS CLAIMPAYCATEGORYTYPE, 
	PAIDINDEMNITYAMOUNT, 
	PAIDMEDICALAMOUNT, 
	DEDUCTIBLEREIMBURSEMENTAMOUNT, 
	PAIDALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	INCURREDALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	INCURREDINDEMNITYAMOUNT, 
	INCURREDMEDICALAMOUNT, 
	o_claimant_num AS CLAIMANTNUM, 
	jurisdiction_state_code AS JURISDICTIONSTATECODE, 
	claim_party_occurrence_ak_id AS CLAIMPARTYOCCURRENCEAKID, 
	TypeOfRecoveryCode1 AS TYPEOFRECOVERYCODE, 
	type_of_loss_code AS TYPEOFLOSSCODE, 
	body_part_code AS BODYPARTCODE, 
	nature_of_inj_code AS NATUREOFINJCODE, 
	cause_inj_code AS CAUSEINJCODE, 
	CLAIMRELATIONSHIPKEY, 
	suit_how_claim_closed AS SUITHOWCLAIMCLOSED, 
	OCCUPATIONDESCRIPTION, 
	WEEKLYWAGEAMOUNT, 
	EMPLOYERATTORNEYFEES
	FROM exp_Before_Target
),
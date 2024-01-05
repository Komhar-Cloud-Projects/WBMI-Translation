WITH
LKP_StatisticalCoverage_StatisticalCoverageAKID AS (
	SELECT
	StatisticalCoverageAKID,
	StatisticalCoverageHashKey
	FROM (
		SELECT StatisticalCoverageAKID AS StatisticalCoverageAKID,
		       StatisticalCoverageHashKey AS StatisticalCoverageHashKey
		FROM	@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation LOC
		INNER JOIN  @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Policy POL
		ON LOC.PolicyAKID = POL.pol_ak_id
		INNER JOIN 	@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage POLCOV
		ON LOC.RiskLocationAKID = POLCOV.RiskLocationAKID
		INNER JOIN 	@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage STATCOV
		ON POLCOV.PolicyCoverageAKID = STATCOV.PolicyCoverageAKID
		WHERE	POL.crrnt_snpsht_flag = 1 AND LOC.CurrentSnapshotFlag =1
				AND STATCOV.CurrentSnapshotFlag =1
				AND POLCOV.CurrentSnapshotFlag =1
		        	AND POL.SOURCE_SYS_ID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		        	AND  EXISTS  (SELECT DISTINCT PolicyKey FROM  
									@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPolicyKeyList
									WHERE POL.pol_key = PolicyKey AND @{pipeline().parameters.WHERE_CLAUSE_EDW})
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StatisticalCoverageHashKey ORDER BY StatisticalCoverageAKID DESC) = 1
),
LKP_Reinsurance_Coverage_CommercialLinesPolicies_Default AS (
	SELECT
	reins_cov_ak_id,
	pol_ak_id,
	reins_ins_line,
	reins_co_num,
	reins_section_code,
	reins_prcnt_facultative_commssn,
	reins_eff_date,
	reins_exp_date
	FROM (
		SELECT reins_cov_ak_id    AS reins_cov_ak_id,
		       pol_ak_id          AS pol_ak_id,
		       reins_ins_line     AS reins_ins_line,
		       reins_co_num       AS reins_co_num,
		       reins_section_code AS reins_section_code,
		       reins_prcnt_facultative_commssn as reins_prcnt_facultative_commssn,
		       reins_eff_date         AS reins_eff_date,
		       reins_exp_date  AS reins_exp_date
		FROM   reinsurance_coverage 
		WHERE  crrnt_snpsht_flag = 1 and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id,reins_ins_line,reins_co_num,reins_section_code,reins_prcnt_facultative_commssn,reins_eff_date,reins_exp_date ORDER BY reins_cov_ak_id DESC) = 1
),
LKP_ReinsuranceCoverage__PersonalLinesPolicies_Default AS (
	SELECT
	reins_cov_ak_id,
	pol_ak_id,
	reins_section_code,
	reins_co_num,
	reins_prcnt_facultative_commssn,
	reins_eff_date,
	reins_exp_date
	FROM (
		SELECT 
		reins_cov_ak_id as reins_cov_ak_id, 
		pol_ak_id as pol_ak_id, 
		reins_section_code as reins_section_code, 
		reins_co_num as reins_co_num, 
		reins_prcnt_facultative_commssn as reins_prcnt_facultative_commssn,
		reins_eff_date as reins_eff_date, 
		reins_exp_date as reins_exp_date
		FROM reinsurance_coverage
		WHERE  crrnt_snpsht_flag = 1 and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id,reins_section_code,reins_co_num,reins_prcnt_facultative_commssn,reins_eff_date,reins_exp_date ORDER BY reins_cov_ak_id DESC) = 1
),
LKP_Pif11Stage AS (
	SELECT
	DocumentText,
	PolicyKey
	FROM (
		select PifSymbol+PifPolicyNumber+PifModule as PolicyKey,
		DocumentText as DocumentText 
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif11Stage
		where DocumentName in ('140601','140603')
		and DocumentText is not null
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY DocumentText) = 1
),
LKP_SupDeductibleBasis AS (
	SELECT
	DeductibleBasis,
	MajorPerilCode,
	CoverageCode
	FROM (
		select distinct
		MajorPerilCode as MajorPerilCode,
		right('000'+CoverageCode,3) as CoverageCode,
		DeductibleBasis as DeductibleBasis
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.SupDeductibleBasis
		where LocationCode='9999' AND TypeBureauCode='AP'
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY MajorPerilCode,CoverageCode ORDER BY DeductibleBasis) = 1
),
LKP_SupDeductibleBasis_byState AS (
	SELECT
	DeductibleBasis,
	i_sar_state,
	MajorPerilCode,
	CoverageCode,
	MasterCompanyNumber
	FROM (
		select distinct
		MajorPerilCode as MajorPerilCode,
		right('000'+CoverageCode,3) as CoverageCode,
		MasterCompanyNumber as MasterCompanyNumber,
		DeductibleBasis as DeductibleBasis
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.SupDeductibleBasis
		where LocationCode='9999' AND TypeBureauCode='AP'
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY MajorPerilCode,CoverageCode,MasterCompanyNumber ORDER BY DeductibleBasis) = 1
),
LKP_SupConstructionCode AS (
	SELECT
	StandardConstructionCodeDescription,
	ConstructionCode
	FROM (
		SELECT 
			StandardConstructionCodeDescription,
			ConstructionCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupConstructionCode
		WHERE CurrentSnapshotFlag=1 AND SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ConstructionCode ORDER BY StandardConstructionCodeDescription) = 1
),
LKP_Policy_strtgc_bus_div_code AS (
	SELECT
	strtgc_bus_dvsn_code,
	pol_ak_id
	FROM (
		SELECT SBD.strtgc_bus_dvsn_code as strtgc_bus_dvsn_code, P.pol_ak_id as pol_ak_id 
		FROM v2.policy P, dbo.strategic_business_division SBD
		WHERE P.strtgc_bus_dvsn_ak_id = SBD.strtgc_bus_dvsn_ak_id AND 
		P.crrnt_snpsht_flag =1 AND SBD.crrnt_snpsht_flag =1 and P.source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}' and SBD.source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id ORDER BY strtgc_bus_dvsn_code DESC) = 1
),
LKP_SupClassificationWorkersCompensation AS (
	SELECT
	SupClassificationWorkersCompensationId,
	ClassCode,
	RatingStateCode
	FROM (
		SELECT 
			SupClassificationWorkersCompensationId,
			ClassCode,
			RatingStateCode
		FROM SupClassificationWorkersCompensation
		WHERE (SubjectToExperienceModificationClassIndicator = 'Y' or ExperienceModificationClassIndicator = 'Y') and CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClassCode,RatingStateCode ORDER BY SupClassificationWorkersCompensationId) = 1
),
LKP_RiskLocation_RiskLocationAKID AS (
	SELECT
	RiskLocationAKID,
	RiskLocationID,
	CurrentSnapshotFlag,
	PolicyAKID,
	LocationUnitNumber,
	RiskTerritory,
	StateProvinceCode,
	ZipPostalCode,
	TaxLocation
	FROM (
		SELECT RiskLocationAKID   AS RiskLocationAKID,
		       PolicyAKID         AS PolicyAKID,
		LOC.RiskLocationID as RiskLocationID,
		LOC.CurrentSnapshotFlag AS CurrentSnapshotFlag,
		       LTRIM(RTRIM(LocationUnitNumber)) AS LocationUnitNumber,
		       LTRIM(RTRIM(RiskTerritory))      AS RiskTerritory,
		       LTRIM(RTRIM(StateProvinceCode))  AS StateProvinceCode,
		       LTRIM(RTRIM(ZipPostalCode))      AS ZipPostalCode,
		       LTRIM(RTRIM(TaxLocation))        AS TaxLocation
		FROM	@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation LOC
		INNER JOIN  @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Policy POL
		ON	LOC.PolicyAKID = POL.pol_ak_id
		WHERE POL.crrnt_snpsht_flag = 1 AND LOC.CurrentSnapshotFlag =1
		       AND POL.SOURCE_SYS_ID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		       AND  EXISTS  (SELECT DISTINCT PolicyKey FROM  
									@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPolicyKeyList
									WHERE POL.pol_key = PolicyKey AND @{pipeline().parameters.WHERE_CLAUSE_EDW})
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID,LocationUnitNumber,RiskTerritory,StateProvinceCode,ZipPostalCode,TaxLocation ORDER BY RiskLocationAKID DESC) = 1
),
LKP_PolicyCoverage_PolicyCoverageAKID AS (
	SELECT
	PolicyCoverageAKID,
	PolicyCoverageHashKey
	FROM (
		SELECT PolicyCoverageAKID    AS PolicyCoverageAKID,
		       	      PolicyCoverageHashKey AS PolicyCoverageHashKey
		FROM	@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation LOC 
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Policy POL
		         ON LOC.PolicyAKID = POL.pol_ak_id 
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage POLCOV
		ON LOC.RiskLocationAKID = POLCOV.RiskLocationAKID
		WHERE  POL.crrnt_snpsht_flag = 1
		       AND LOC.CurrentSnapshotFlag = 1
		       AND POLCOV.CurrentSnapshotFlag = 1 
		       AND POL.SOURCE_SYS_ID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		       AND  EXISTS  (SELECT DISTINCT PolicyKey FROM  
									@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPolicyKeyList
									WHERE POL.pol_key = PolicyKey AND @{pipeline().parameters.WHERE_CLAUSE_EDW})
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyCoverageHashKey ORDER BY PolicyCoverageAKID DESC) = 1
),
LKP_ReinsuranceCoverage_PersonalLinesPolicies AS (
	SELECT
	reins_cov_ak_id,
	pol_ak_id,
	reins_risk_unit,
	reins_section_code,
	reins_co_num,
	reins_prcnt_facultative_commssn,
	reins_eff_date,
	reins_exp_date
	FROM (
		SELECT reins_cov_ak_id as reins_cov_ak_id, pol_ak_id as pol_ak_id, reins_risk_unit as reins_risk_unit, reins_section_code as reins_section_code, reins_co_num as reins_co_num, 
		reins_prcnt_facultative_commssn as reins_prcnt_facultative_commssn, reins_eff_date as reins_eff_date, reins_exp_date as reins_exp_date
		FROM reinsurance_coverage
		WHERE  crrnt_snpsht_flag = 1 and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id,reins_risk_unit,reins_section_code,reins_co_num,reins_prcnt_facultative_commssn,reins_eff_date,reins_exp_date ORDER BY reins_cov_ak_id DESC) = 1
),
LKP_ReinsuranceCoverage_CommercialLinesPolicies AS (
	SELECT
	reins_cov_ak_id,
	pol_ak_id,
	reins_ins_line,
	reins_loc_unit_num,
	reins_sub_loc_unit_num,
	reins_section_code,
	reins_co_num,
	reins_prcnt_facultative_commssn,
	reins_eff_date,
	reins_exp_date
	FROM (
		SELECT reins_cov_ak_id        AS reins_cov_ak_id,
		       pol_ak_id              AS pol_ak_id,
		       reins_ins_line         AS reins_ins_line,
		       CASE  
		           WHEN reins_loc_unit_num <> 'N/A' 
		           THEN 
		              CASE WHEN LEN(reins_loc_unit_num)=1
		                   THEN 
		                     '000' + RTRIM(reins_loc_unit_num) 
		                   ELSE  
		                     CASE WHEN LEN(reins_loc_unit_num)=2
		                     THEN
		                     '00' + RTRIM(reins_loc_unit_num) 
		
		                     ELSE
		                        CASE WHEN LEN(reins_loc_unit_num)=3
		                        THEN
		                         '000' + RTRIM(reins_loc_unit_num)  
		                        ELSE
		                         RTRIM(reins_loc_unit_num) 
		                        END 
		                     END
		              END  
		           ELSE 
		               RTRIM(reins_loc_unit_num)
		       END     
		        as reins_loc_unit_num,
		       CASE  
		           WHEN reins_sub_loc_unit_num <> 'N/A' 
		           THEN 
		              CASE WHEN LEN(reins_sub_loc_unit_num)=1
		                   THEN 
		                     '00' + RTRIM(reins_sub_loc_unit_num) 
		                   ELSE  
		                     CASE WHEN LEN(reins_sub_loc_unit_num)=2
		                     THEN
		                     '0' + RTRIM(reins_sub_loc_unit_num) 
		
		                     ELSE
		                     RTRIM(reins_sub_loc_unit_num) 
		                     END
		              END  
		           ELSE 
		               RTRIM(reins_sub_loc_unit_num)
		       END     
		                              as reins_sub_loc_unit_num,
		       reins_section_code     AS reins_section_code,
		       reins_co_num           AS reins_co_num,
		       reins_prcnt_facultative_commssn as reins_prcnt_facultative_commssn,
		       reins_eff_date         AS reins_eff_date,
		       reins_exp_date  AS reins_exp_date
		FROM   dbo.reinsurance_coverage
		WHERE  crrnt_snpsht_flag = 1 and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id,reins_ins_line,reins_loc_unit_num,reins_sub_loc_unit_num,reins_section_code,reins_co_num,reins_prcnt_facultative_commssn,reins_eff_date,reins_exp_date ORDER BY reins_cov_ak_id DESC) = 1
),
LKP_ReinsuranceCoverage_CommercialLinesPolicies_WithoutSublocation AS (
	SELECT
	reins_cov_ak_id,
	pol_ak_id,
	reins_ins_line,
	reins_loc_unit_num,
	reins_section_code,
	reins_co_num,
	reins_prcnt_facultative_commssn,
	reins_eff_date,
	reins_exp_date
	FROM (
		SELECT reins_cov_ak_id        AS reins_cov_ak_id,
		       pol_ak_id              AS pol_ak_id,
		       reins_ins_line         AS reins_ins_line,
		       CASE  
		           WHEN reins_loc_unit_num <> 'N/A' 
		           THEN 
		              CASE WHEN LEN(reins_loc_unit_num)=1
		                   THEN 
		                     '000' + RTRIM(reins_loc_unit_num) 
		                   ELSE  
		                     CASE WHEN LEN(reins_loc_unit_num)=2
		                     THEN
		                     '00' + RTRIM(reins_loc_unit_num) 
		
		                     ELSE
		                        CASE WHEN LEN(reins_loc_unit_num)=3
		                        THEN
		                         '000' + RTRIM(reins_loc_unit_num)  
		                        ELSE
		                         RTRIM(reins_loc_unit_num) 
		                        END 
		                     END
		              END  
		           ELSE 
		               RTRIM(reins_loc_unit_num)
		       END     
		        as reins_loc_unit_num,
		       reins_section_code     AS reins_section_code,
		       reins_co_num           AS reins_co_num,
		        reins_prcnt_facultative_commssn as reins_prcnt_facultative_commssn,
		       reins_eff_date         AS reins_eff_date,
		       reins_exp_date  AS reins_exp_date
		FROM   dbo.reinsurance_coverage
		WHERE  crrnt_snpsht_flag = 1 and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id,reins_ins_line,reins_loc_unit_num,reins_section_code,reins_co_num,reins_prcnt_facultative_commssn,reins_eff_date,reins_exp_date ORDER BY reins_cov_ak_id DESC) = 1
),
SQ_pif_4514_stage AS (
	SELECT pif_4514_stage_id,
		RTRIM(A.pif_symbol),
	       A.pif_policy_number,
	       A.pif_module,
	       sar_id,
	       LTRIM(RTRIM(sar_insurance_line)) as sar_insurance_line,
	       CASE LEN(sar_location_x) WHEN '0' THEN LTRIM(RTRIM(sar_unit)) ELSE LTRIM(RTRIM(sar_location_x)) END as sar_location_x,
	       LTRIM(RTRIM(sar_sub_location_x)) as sar_sub_location_x,
	       LTRIM(RTRIM(sar_risk_unit_group)) as sar_risk_unit_group,
	       LTRIM(RTRIM(sar_class_code_grp_x + sar_class_code_mem_x)) as sar_class_code_grp_x,
	       sar_class_code_mem_x,
	       LTRIM(RTRIM(sar_unit + sar_risk_unit_continued))      AS sar_unit,
	       sar_risk_unit_continued,
	       CASE LEN(LTRIM(RTRIM(COALESCE(sar_seq_rsk_unt_a, ''))))
	            WHEN '0' THEN 'N/A'
	                  ELSE LTRIM(RTRIM(sar_seq_rsk_unt_a))
	             END                                         AS sar_seq_rsk_unt_a,
		  LTRIM(RTRIM(sar_type_exposure)) as sar_type_exposure,
	        LTRIM(RTRIM(sar_major_peril)) as sar_major_peril,
	        LTRIM(RTRIM(sar_seq_no)) as sar_seq_no,
	       sar_cov_eff_year,
	       sar_cov_eff_month,
	       sar_cov_eff_day,
	       sar_part_code,
	       sar_trans_eff_year,
	       sar_trans_eff_month,
	       sar_trans_eff_day,
	       LTRIM(RTRIM(sar_reinsurance_company_no)),
	       sar_entrd_date,
	       sar_exp_year,
	       sar_exp_month,
	       sar_exp_day,
	       sar_transaction,
	       sar_premium,
	       sar_original_prem,
	       sar_agents_comm_rate,
	       sar_acct_entrd_date,
	       sar_annual_state_line,
	       sar_faculta_comm_rate,
	       LTRIM(RTRIM(sar_state)) as sar_state,
	       LTRIM(RTRIM(sar_loc_prov_territory)) as sar_loc_prov_territory,
		sar_company_number,
	       sar_county_first_two,
	       sar_county_last_one,
	       CASE WHEN LEN(LTRIM(RTRIM(sar_county_first_two)) + LTRIM(RTRIM(sar_county_last_one)) + LTRIM(RTRIM(sar_city))) < 6 THEN '000000' ELSE 
	       LTRIM(RTRIM(sar_county_first_two)) + LTRIM(RTRIM(sar_county_last_one)) + LTRIM(RTRIM(sar_city)) END as sar_city,
	sar_city as lkp_sar_city,
	       sar_rsn_amend_one,
	       sar_rsn_amend_two,
	       sar_rsn_amend_three,
	       sar_special_use,
		sar_stat_breakdown_line,
		 sar_user_line,
	       LTRIM(RTRIM(sar_section)) as sar_section,
	       sar_rating_date_ind,
	       LTRIM(RTRIM(sar_type_bureau)) as sar_type_bureau,
	       LTRIM(RTRIM(sar_class_1_4 + sar_class_5_6))  AS sar_class_1_4,
	       sar_class_5_6,
	
	      Case sar_type_bureau when 'GL' THEN
		    case when sar_exposure in (10000000,-9999999999, 9999999999) then 0 
	          else sar_exposure	
	          End
		Else 
	          sar_exposure
		End as sar_exposure,
	
	       LTRIM(RTRIM(sar_sub_line)) as sar_sub_line,
	      sar_code_1,
	      sar_code_2,
	      sar_code_3,
	      sar_code_4,
	      sar_code_5,
	      sar_code_6,
	      sar_code_7,
	      sar_code_8,
	      sar_code_9,
	      sar_code_10,
	      sar_code_11,
	      sar_code_12,
	      sar_code_13,
	      sar_code_14,
	      sar_code_15,
	      LTRIM(RTRIM(sar_zip_postal_code)) as sar_zip_postal_code,
	      sar_audit_reinst_ind,
		A.logical_flag
	FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME_4514} A
	@{pipeline().parameters.JOIN_CONDITION}
	(SELECT DISTINCT Policykey FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.work_policytransactionstatus 
	WHERE AuditID = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AND PolicyStatus <> 'NOCHANGE')  B
	ON  A.policykey = B.policykey
	
	WHERE A.sar_major_peril NOT IN ('078', '088', '089', '183', '255', '499', '256', '257', '258', '259', '898', '899') 
	AND A.logical_flag IN ('0','1','2','3') 
	@{pipeline().parameters.WHERE_CLAUSE}
	ORDER BY pif_4514_stage_id
	
	----- Added the Order By clause on source qualifier, so that we are reading all the Transactions of a given policy as they are in the source(PIFMSTR, DAILYPIF Files).
	---- We have Premiumloadsequence in the target which preserves the order of the transactions in EDW as they in the source. This order of transactions is very 
	--- important for the generation of Premium Master in EDW.
	---The join conditions on the pif43 segment tables were profiled to avoid duplicates, but in rare case that mutiple matches are still returned, we will be taking the latest record in the 43 segment table.
),
FIL_PassThroughCharges_Records AS (
	SELECT
	pif_4514_stage_id, 
	pif_symbol, 
	pif_policy_number, 
	pif_module, 
	sar_id, 
	sar_insurance_line, 
	sar_location_x, 
	sar_sub_location_x, 
	sar_risk_unit_group, 
	sar_class_code_grp_x, 
	sar_class_code_mem_x, 
	sar_unit, 
	sar_risk_unit_continued, 
	sar_seq_rsk_unt_a, 
	sar_type_exposure, 
	sar_major_peril, 
	sar_seq_no, 
	sar_cov_eff_year, 
	sar_cov_eff_month, 
	sar_cov_eff_day, 
	sar_part_code, 
	sar_trans_eff_year, 
	sar_trans_eff_month, 
	sar_trans_eff_day, 
	sar_reinsurance_company_no, 
	sar_entrd_date, 
	sar_exp_year, 
	sar_exp_month, 
	sar_exp_day, 
	sar_transaction, 
	sar_premium, 
	sar_original_prem, 
	sar_agents_comm_rate, 
	sar_acct_entrd_date, 
	sar_annual_state_line, 
	sar_faculta_comm_rate, 
	sar_state, 
	sar_loc_prov_territory, 
	sar_company_number, 
	sar_county_first_two, 
	sar_county_last_one, 
	sar_city, 
	lkp_sar_city, 
	sar_rsn_amend_one, 
	sar_rsn_amend_two, 
	sar_rsn_amend_three, 
	sar_special_use, 
	sar_stat_breakdown_line, 
	sar_user_line, 
	sar_section, 
	sar_rating_date_ind, 
	sar_type_bureau, 
	sar_class_1_4, 
	sar_class_5_6, 
	sar_exposure, 
	sar_sub_line, 
	sar_code_1, 
	sar_code_2, 
	sar_code_3, 
	sar_code_4, 
	sar_code_5, 
	sar_code_6, 
	sar_code_7, 
	sar_code_8, 
	sar_code_9, 
	sar_code_10, 
	sar_code_11, 
	sar_code_12, 
	sar_code_13, 
	sar_code_14, 
	sar_code_15, 
	sar_zip_postal_code, 
	sar_audit_reinst_ind, 
	logical_flag
	FROM SQ_pif_4514_stage
	WHERE IIF(IN(sar_major_peril,'078', '088', '089', '183', '255', '499', '256', '257', '258', '259', '898', '899'),FALSE,TRUE)
),
EXP_Default AS (
	SELECT
	pif_4514_stage_id,
	pif_symbol,
	pif_policy_number,
	pif_module,
	pif_symbol  ||  pif_policy_number  || pif_module AS Policy_Key,
	sar_id,
	sar_insurance_line,
	-- *INF*: --edwp-1344
	-- iif(sar_insurance_line='IM','GL',sar_insurance_line)
	IFF(sar_insurance_line = 'IM', 'GL', sar_insurance_line) AS sar_insurance_line_GL,
	-- *INF*: --edwp - 1344
	-- iif(sar_insurance_line='IM','CF',sar_insurance_line)
	IFF(sar_insurance_line = 'IM', 'CF', sar_insurance_line) AS sar_insurance_line_CF,
	sar_location_x,
	-- *INF*: DECODE(TRUE,
	-- LENGTH(LTRIM(RTRIM(sar_location_x)))=0, 0, 
	-- REG_MATCH(sar_location_x, '[0-9]+'), TO_DECIMAL(sar_location_x,0) 
	-- , -1)
	DECODE(TRUE,
	LENGTH(LTRIM(RTRIM(sar_location_x))) = 0, 0,
	REG_MATCH(sar_location_x, '[0-9]+'), TO_DECIMAL(sar_location_x, 0),
	- 1) AS lkp_sar_location_x,
	sar_sub_location_x,
	-- *INF*: DECODE(TRUE,
	-- LENGTH(LTRIM(RTRIM(sar_sub_location_x)))=0, 0, 
	-- REG_MATCH(sar_sub_location_x, '[0-9]+'), TO_DECIMAL(sar_sub_location_x,0) 
	-- , -1)
	DECODE(TRUE,
	LENGTH(LTRIM(RTRIM(sar_sub_location_x))) = 0, 0,
	REG_MATCH(sar_sub_location_x, '[0-9]+'), TO_DECIMAL(sar_sub_location_x, 0),
	- 1) AS lkp_sar_sub_location_x,
	sar_risk_unit_group,
	-- *INF*: DECODE(TRUE,
	-- LENGTH(LTRIM(RTRIM(sar_risk_unit_group)))=0, 0, 
	-- REG_MATCH(sar_risk_unit_group, '[0-9]+'), TO_DECIMAL(sar_risk_unit_group,0) 
	-- , -1)
	DECODE(TRUE,
	LENGTH(LTRIM(RTRIM(sar_risk_unit_group))) = 0, 0,
	REG_MATCH(sar_risk_unit_group, '[0-9]+'), TO_DECIMAL(sar_risk_unit_group, 0),
	- 1) AS lkp_sar_risk_unit_group,
	sar_class_code_grp_x,
	-- *INF*: SUBSTR(sar_class_code_grp_x,1,2)
	SUBSTR(sar_class_code_grp_x, 1, 2) AS v_sar_class_code_grp_x,
	-- *INF*: DECODE(TRUE,
	-- LENGTH(LTRIM(RTRIM(v_sar_class_code_grp_x)))=0, 0, 
	-- REG_MATCH(v_sar_class_code_grp_x, '[0-9]+'), TO_DECIMAL(v_sar_class_code_grp_x,0) 
	-- , -1)
	DECODE(TRUE,
	LENGTH(LTRIM(RTRIM(v_sar_class_code_grp_x))) = 0, 0,
	REG_MATCH(v_sar_class_code_grp_x, '[0-9]+'), TO_DECIMAL(v_sar_class_code_grp_x, 0),
	- 1) AS lkp_sar_class_code_grp_x,
	-- *INF*: LENGTH(LTRIM(RTRIM(sar_class_code_grp_x)))
	LENGTH(LTRIM(RTRIM(sar_class_code_grp_x))) AS TEST1,
	-- *INF*: REG_MATCH(sar_class_code_grp_x, '[0-9]+')
	REG_MATCH(sar_class_code_grp_x, '[0-9]+') AS TEST2,
	sar_class_code_mem_x,
	-- *INF*: DECODE(TRUE,
	-- LENGTH(LTRIM(RTRIM(sar_class_code_mem_x)))=0, 0, 
	-- REG_MATCH(sar_class_code_mem_x, '[0-9]+'), TO_DECIMAL(sar_class_code_mem_x,0) 
	-- , -1)
	DECODE(TRUE,
	LENGTH(LTRIM(RTRIM(sar_class_code_mem_x))) = 0, 0,
	REG_MATCH(sar_class_code_mem_x, '[0-9]+'), TO_DECIMAL(sar_class_code_mem_x, 0),
	- 1) AS lkp_sar_class_code_mem_x,
	-- *INF*: SUBSTR(sar_class_code_grp_x,2,1) || sar_class_code_mem_x
	SUBSTR(sar_class_code_grp_x, 2, 1) || sar_class_code_mem_x AS lkp_ReportingClassSeq,
	-- *INF*: DECODE(TRUE,
	-- LENGTH(LTRIM(RTRIM(sar_class_code_grp_x)))=0, 0, 
	-- REG_MATCH(sar_class_code_grp_x, '[0-9]+'), TO_DECIMAL(sar_class_code_grp_x,0) 
	-- , -1)
	DECODE(TRUE,
	LENGTH(LTRIM(RTRIM(sar_class_code_grp_x))) = 0, 0,
	REG_MATCH(sar_class_code_grp_x, '[0-9]+'), TO_DECIMAL(sar_class_code_grp_x, 0),
	- 1) AS lkp_sequencenumber,
	sar_unit,
	-- *INF*: LTRIM(:UDF.DEFAULT_VALUE_FOR_STRINGS(SUBSTR(sar_unit,1,3)),'0')
	LTRIM(:UDF.DEFAULT_VALUE_FOR_STRINGS(SUBSTR(sar_unit, 1, 3)), '0') AS o_sar_unit_1_3_trimmed,
	-- *INF*: length(ltrim(rtrim(sar_unit )  )  ) 
	length(ltrim(rtrim(sar_unit))) AS out_sar_unit,
	-- *INF*: SUBSTR(sar_unit,3,1)
	SUBSTR(sar_unit, 3, 1) AS lkp_sar_unit_3_1,
	sar_risk_unit_continued,
	-- *INF*: SUBSTR(sar_risk_unit_continued ,2,2)
	SUBSTR(sar_risk_unit_continued, 2, 2) AS lkp_ClassCodeSeq,
	-- *INF*: SUBSTR(sar_unit,1,5)
	SUBSTR(sar_unit, 1, 5) AS lkp_Pmduxg1ClassCode,
	sar_seq_rsk_unt_a,
	-- *INF*: DECODE(TRUE,
	-- LENGTH(LTRIM(RTRIM(sar_seq_rsk_unt_a)))=0, 0, 
	-- REG_MATCH(SUBSTR(sar_seq_rsk_unt_a,1,1), '[0-9]+'), TO_DECIMAL(SUBSTR(sar_seq_rsk_unt_a,1,1),0) 
	-- , -1)
	DECODE(TRUE,
	LENGTH(LTRIM(RTRIM(sar_seq_rsk_unt_a))) = 0, 0,
	REG_MATCH(SUBSTR(sar_seq_rsk_unt_a, 1, 1), '[0-9]+'), TO_DECIMAL(SUBSTR(sar_seq_rsk_unt_a, 1, 1), 0),
	- 1) AS lkp_sar_seq_rsk_unt_a,
	-- *INF*: DECODE(TRUE, LENGTH(LTRIM(RTRIM(SUBSTR(sar_seq_rsk_unt_a,2,1))))=0, 'B', SUBSTR(sar_seq_rsk_unt_a,2,1))
	DECODE(TRUE,
	LENGTH(LTRIM(RTRIM(SUBSTR(sar_seq_rsk_unt_a, 2, 1)))) = 0, 'B',
	SUBSTR(sar_seq_rsk_unt_a, 2, 1)) AS lkp_sar_seq_rsk_unt_a_2_1,
	sar_type_exposure,
	sar_major_peril,
	sar_seq_no,
	sar_cov_eff_year,
	sar_cov_eff_month,
	sar_cov_eff_day,
	sar_part_code,
	sar_trans_eff_year,
	sar_trans_eff_month,
	sar_trans_eff_day,
	sar_reinsurance_company_no,
	sar_entrd_date,
	sar_exp_year,
	sar_exp_month,
	sar_exp_day,
	sar_transaction,
	sar_premium,
	sar_original_prem,
	sar_agents_comm_rate,
	sar_acct_entrd_date,
	sar_faculta_comm_rate,
	sar_state,
	sar_loc_prov_territory,
	sar_company_number,
	sar_county_first_two,
	sar_county_last_one,
	sar_county_first_two || sar_county_last_one AS lkp_Location,
	sar_city,
	lkp_sar_city,
	sar_rsn_amend_one,
	sar_rsn_amend_two,
	sar_rsn_amend_three,
	sar_section,
	sar_type_bureau,
	sar_class_1_4,
	-- *INF*: SUBSTR(sar_class_1_4,1,4)
	SUBSTR(sar_class_1_4, 1, 4) AS lkp_sar_class_1_4,
	-- *INF*: SUBSTR(sar_class_1_4,1,4)
	SUBSTR(sar_class_1_4, 1, 4) AS v_lkp_sar_class_1_4,
	-- *INF*: DECODE(TRUE,
	-- LENGTH(LTRIM(RTRIM(v_lkp_sar_class_1_4)))=0, 0, 
	-- REG_MATCH(v_lkp_sar_class_1_4, '[0-9]+'), TO_DECIMAL(v_lkp_sar_class_1_4,0) 
	-- , -1)
	DECODE(TRUE,
	LENGTH(LTRIM(RTRIM(v_lkp_sar_class_1_4))) = 0, 0,
	REG_MATCH(v_lkp_sar_class_1_4, '[0-9]+'), TO_DECIMAL(v_lkp_sar_class_1_4, 0),
	- 1) AS out_lkp_sar_class_1_4,
	sar_class_5_6,
	-- *INF*: DECODE(TRUE,
	-- LENGTH(LTRIM(RTRIM(SUBSTR(sar_class_1_4,1,4) || SUBSTR(sar_class_5_6,1,1))))=0, 0, 
	-- REG_MATCH(SUBSTR(sar_class_1_4,1,4), '[0-9]+') AND REG_MATCH(SUBSTR(sar_class_5_6,1,1), '[0-9]+'), TO_DECIMAL(SUBSTR(sar_class_1_4,1,4) || SUBSTR(sar_class_5_6,1,1),0) 
	-- , -1)
	DECODE(TRUE,
	LENGTH(LTRIM(RTRIM(SUBSTR(sar_class_1_4, 1, 4) || SUBSTR(sar_class_5_6, 1, 1)))) = 0, 0,
	REG_MATCH(SUBSTR(sar_class_1_4, 1, 4), '[0-9]+') AND REG_MATCH(SUBSTR(sar_class_5_6, 1, 1), '[0-9]+'), TO_DECIMAL(SUBSTR(sar_class_1_4, 1, 4) || SUBSTR(sar_class_5_6, 1, 1), 0),
	- 1) AS lkp_ClassCode,
	sar_exposure,
	sar_sub_line,
	sar_zip_postal_code,
	sar_annual_state_line,
	sar_special_use,
	sar_stat_breakdown_line,
	sar_user_line,
	sar_rating_date_ind,
	sar_code_1,
	sar_code_2,
	sar_code_3,
	sar_code_4,
	sar_code_5,
	sar_code_6,
	sar_code_7,
	sar_code_8,
	sar_code_9,
	sar_code_10,
	sar_code_11,
	sar_code_12,
	sar_code_13,
	sar_code_14,
	sar_code_15,
	sar_audit_reinst_ind,
	logical_flag,
	-- *INF*: (sar_code_1 || sar_code_2)
	( sar_code_1 || sar_code_2 ) AS Out_PMACode
	FROM FIL_PassThroughCharges_Records
),
LKP_PIF43NXCRStage AS (
	SELECT
	PMDNXC1PmaCode,
	wb_class_of_business,
	PifSymbol,
	PifPolicyNumber,
	PifPolicyModule,
	PMDNXC1InsuranceLine,
	PMDNXC1LocationNumber,
	PMDNXC1SubLocationNumber,
	PMDNXC1CspClassCode,
	PMDUYC1Coverage
	FROM (
		SELECT distinct crnx.PMDNXC1PmaCode as PMDNXC1PmaCode
		,pif.wb_class_of_business as wb_class_of_business
		, crnx.PifSymbol as PifSymbol
		,crnx.PifPolicyNumber as PifPolicyNumber 
		,crnx.PifPolicyModule as PifPolicyModule 
		,crnx.PMDNXC1InsuranceLine as PMDNXC1InsuranceLine
		,crnx.PMDNXC1LocationNumber as PMDNXC1LocationNumber
		,crnx.PMDNXC1SubLocationNumber as PMDNXC1SubLocationNumber
		,crnx.PMDNXC1CspClassCode as PMDNXC1CspClassCode
		,uy.PMDUYC1Coverage as PMDUYC1Coverage
		from PIF43NXCRStage crnx
		inner join PIF43UYCRStage uy
		on crnx.PifSymbol = uy.PifSymbol
		and crnx.PifPolicyNumber = uy.PifPolicyNumber
		and crnx.PifPolicyModule = uy.PifPolicyModule
		and crnx.PMDNXC1InsuranceLine = uy.PMDUYC1InsuranceLine
		inner join pif_02_stage pif
		on crnx.PifSymbol = pif.pif_symbol
		and crnx.PifPolicyNumber = pif.pif_policy_number
		and crnx.PifPolicyModule = pif.pif_module
		where crnx.PMDNXC1PmaCode is not null
		and crnx.PMDNXC1InsuranceLine = 'cr'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifPolicyModule,PMDNXC1InsuranceLine,PMDNXC1LocationNumber,PMDNXC1SubLocationNumber,PMDNXC1CspClassCode,PMDUYC1Coverage ORDER BY PMDNXC1PmaCode) = 1
),
LKP_Pif350Stage AS (
	SELECT
	PackageModificationAssignment,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	UnitNum
	FROM (
		SELECT PackageModificationAssignment AS PackageModificationAssignment
			,PifSymbol AS PifSymbol
			,PifPolicyNumber AS PifPolicyNumber
			,PifModule AS PifModule
			,(CASE WHEN UnitNum=0 THEN 'N/A' ELSE CONVERT(VARCHAR(3),UnitNum) END) AS UnitNum
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif350Stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule,UnitNum ORDER BY PackageModificationAssignment) = 1
),
LKP_Pif43IXUnmodStage AS (
	SELECT
	Pif43IXUnmodWCRatingState,
	Pif43IXUnmodReportingClassCode,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	Pif43IXUnmodInsuranceLine,
	Pif43IXUnmodSplitRateSeq,
	Pif43IXUnmodYearItemEffective,
	Pif43IXUnmodMonthItemEffective,
	Pif43IXUnmodDayItemEffective
	FROM (
		SELECT E.PifSymbol as PifSymbol, 
		E.PifPolicyNumber as PifPolicyNumber, 
		E.PifModule as PifModule, 
		E.Pif43IXUnmodInsuranceLine as Pif43IXUnmodInsuranceLine, 
		E.Pif43IXUnmodWCRatingState as Pif43IXUnmodWCRatingState, 
		LTRIM(RTRIM(E.Pif43IXUnmodReportingClassCode)) as Pif43IXUnmodReportingClassCode, 
		E.Pif43IXUnmodSplitRateSeq as Pif43IXUnmodSplitRateSeq, 
		E.Pif43IXUnmodYearItemEffective as Pif43IXUnmodYearItemEffective, 
		E.Pif43IXUnmodMonthItemEffective as Pif43IXUnmodMonthItemEffective, 
		E.Pif43IXUnmodDayItemEffective as Pif43IXUnmodDayItemEffective 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43IXUnmodStage E
		WHERE E.Pif43IXUnmodStageID in (
		select max(Pif43IXUnmodStageID) from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43IXUnmodStage B
		where E.PifSymbol=B.PifSymbol
		and E.PifPolicyNumber=B.PifPolicyNumber
		and E.PifModule=B.PifModule
		and B.Pif43IXUnmodWCRatingState=E.Pif43IXUnmodWCRatingState
		and B.Pif43IXUnmodReportingClassCode=E.Pif43IXUnmodReportingClassCode
		and B.Pif43IXUnmodSplitRateSeq=E.Pif43IXUnmodSplitRateSeq
		and B.Pif43IXUnmodYearItemEffective=E.Pif43IXUnmodYearItemEffective
		and B.Pif43IXUnmodMonthItemEffective=E.Pif43IXUnmodMonthItemEffective
		and B.Pif43IXUnmodDayItemEffective=E.Pif43IXUnmodDayItemEffective
		and B.Pif43IXUnmodSegmentPartCode='X'
		and B.Pif43IXUnmodSegmentStatus<>'D')
		and E.Pif43IXUnmodSegmentPartCode='X'
		and E.Pif43IXUnmodSegmentStatus<>'D'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule,Pif43IXUnmodInsuranceLine,Pif43IXUnmodWCRatingState,Pif43IXUnmodReportingClassCode,Pif43IXUnmodSplitRateSeq,Pif43IXUnmodYearItemEffective,Pif43IXUnmodMonthItemEffective,Pif43IXUnmodDayItemEffective ORDER BY Pif43IXUnmodWCRatingState) = 1
),
LKP_Pif43IXZWCModStage AS (
	SELECT
	Pmdi4w1YearItemEffective,
	Pmdi4w1MonthItemEffective,
	Pmdi4w1DayItemEffective,
	Pmdi4w1ModifierDesc,
	Pmdi4w1ModifierRate,
	Pmdi4w1ModifierPremBasis,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	Pmdi4w1InsuranceLine,
	Pmdi4w1WcRatingState,
	Pmdi4w1ReportingClassCode,
	Pmdi4w1SplitRateSeq
	FROM (
		SELECT G.Pmdi4w1ModifierDesc as Pmdi4w1ModifierDesc, 
		G.Pmdi4w1ModifierRate as Pmdi4w1ModifierRate, 
		G.Pmdi4w1ModifierPremBasis as Pmdi4w1ModifierPremBasis, 
		G.PifSymbol as PifSymbol, 
		G.PifPolicyNumber as PifPolicyNumber, 
		G.PifModule as PifModule, 
		G.Pmdi4w1InsuranceLine as Pmdi4w1InsuranceLine, 
		G.Pmdi4w1WcRatingState as Pmdi4w1WcRatingState, 
		LTRIM(RTRIM(G.Pmdi4w1ReportingClassCode)) as Pmdi4w1ReportingClassCode, 
		G.Pmdi4w1ReportingClassSeq as Pmdi4w1ReportingClassSeq, 
		G.Pmdi4w1SplitRateSeq as Pmdi4w1SplitRateSeq, 
		G.Pmdi4w1YearItemEffective as Pmdi4w1YearItemEffective, 
		G.Pmdi4w1MonthItemEffective as Pmdi4w1MonthItemEffective, 
		G.Pmdi4w1DayItemEffective as Pmdi4w1DayItemEffective 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43IXZWCModStage G
		WHERE G.Pif43IXZWCModStageId in (
		select max(Pif43IXZWCModStageID) from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43IXZWCModStage B
		where G.PifSymbol=B.PifSymbol
		and G.PifPolicyNumber=B.PifPolicyNumber
		and G.PifModule=B.PifModule
		and B.Pmdi4w1WcRatingState=G.Pmdi4w1WcRatingState
		and B.Pmdi4w1ReportingClassCode=G.Pmdi4w1ReportingClassCode
		and B.Pmdi4w1ReportingClassSeq=G.Pmdi4w1ReportingClassSeq
		and B.Pmdi4w1SplitRateSeq=G.Pmdi4w1SplitRateSeq
		and B.Pmdi4w1YearItemEffective=G.Pmdi4w1YearItemEffective
		and B.Pmdi4w1MonthItemEffective=G.Pmdi4w1MonthItemEffective
		and B.Pmdi4w1DayItemEffective=G.Pmdi4w1DayItemEffective)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule,Pmdi4w1InsuranceLine,Pmdi4w1WcRatingState,Pmdi4w1ReportingClassCode,Pmdi4w1SplitRateSeq,Pmdi4w1YearItemEffective,Pmdi4w1MonthItemEffective,Pmdi4w1DayItemEffective ORDER BY Pmdi4w1YearItemEffective) = 1
),
LKP_Pif43LXGAStage AS (
	SELECT
	PMDLXA1PmaCode,
	PifSymbol,
	PifPolicyNumber,
	PifModule
	FROM (
		SELECT 
			PMDLXA1PmaCode,
			PifSymbol,
			PifPolicyNumber,
			PifModule
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43LXGAStage
		WHERE PMDLXA1PmaCode is not null and PMDLXA1InsuranceLine = 'GA'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule ORDER BY PMDLXA1PmaCode) = 1
),
LKP_Pif43LXGLStage AS (
	SELECT
	Pmdlxg1YearRetro,
	Pmdlxg1MonthRetro,
	Pmdlxg1DayRetro,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	Pmdlxg1InsuranceLine
	FROM (
		SELECT K.Pmdlxg1YearRetro as Pmdlxg1YearRetro, 
		K.Pmdlxg1MonthRetro as Pmdlxg1MonthRetro, 
		K.Pmdlxg1DayRetro as Pmdlxg1DayRetro, 
		K.PifSymbol as PifSymbol, 
		K.PifPolicyNumber as PifPolicyNumber, 
		K.PifModule as PifModule, 
		K.Pmdlxg1InsuranceLine as Pmdlxg1InsuranceLine 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43LXGLStage K
		WHERE K.Pif43LXGLStageId in (
		select max(Pif43LXGLStageId) from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43LXGLStage B
		where K.PifSymbol=B.PifSymbol
		and K.PifPolicyNumber=B.PifPolicyNumber
		and K.PifModule=B.PifModule)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule,Pmdlxg1InsuranceLine ORDER BY Pmdlxg1YearRetro) = 1
),
LKP_Pif43LXZWCStage AS (
	SELECT
	Pmdl4w1RatingProgramType,
	Pmdl4w1PolicyType,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	Pmdl4w1InsuranceLine
	FROM (
		SELECT D.Pmdl4w1RatingProgramType as Pmdl4w1RatingProgramType, 
		D.Pmdl4w1PolicyType as Pmdl4w1PolicyType, 
		D.PifSymbol as PifSymbol, 
		D.PifPolicyNumber as PifPolicyNumber, 
		D.PifModule as PifModule, 
		D.Pmdl4w1InsuranceLine as Pmdl4w1InsuranceLine 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43LXZWCStage D
		WHERE D.Pif43LXZWCStageId in (
		select max(Pif43LXZWCStageId) from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43LXZWCStage B
		where D.PifSymbol=B.PifSymbol
		and D.PifPolicyNumber=B.PifPolicyNumber
		and D.PifModule=B.PifModule)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule,Pmdl4w1InsuranceLine ORDER BY Pmdl4w1RatingProgramType) = 1
),
LKP_Pif43NXCPStage AS (
	SELECT
	Pmdnxp1OtherMod,
	Pmdnxp1CspConstrCod,
	Pmdnxp1ProtectionClassPart1,
	Pmdnxp1YearBuilt,
	Pmdnxp1Irpm,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	Pmdnxp1InsuranceLine,
	Pmdnxp1LocationNumber,
	Pmdnxp1SubLocationNumber
	FROM (
		SELECT C.Pmdnxp1OtherMod as Pmdnxp1OtherMod, 
		C.Pmdnxp1CspConstrCod as Pmdnxp1CspConstrCod, 
		C.Pmdnxp1ProtectionClassPart1 as Pmdnxp1ProtectionClassPart1, 
		C.Pmdnxp1YearBuilt as Pmdnxp1YearBuilt, 
		C.PifSymbol as PifSymbol, 
		C.PifPolicyNumber as PifPolicyNumber, 
		C.PifModule as PifModule, 
		C.Pmdnxp1InsuranceLine as Pmdnxp1InsuranceLine,
		C.Pmdnxp1LocationNumber as Pmdnxp1LocationNumber, 
		C.Pmdnxp1SubLocationNumber as Pmdnxp1SubLocationNumber,
		C.Pmdnxp1Irpm AS Pmdnxp1Irpm
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43NXCPStage C
		WHERE C.Pif43NXCPStageId in (
		select max(Pif43NXCPStageId) from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43NXCPStage B
		where C.PifSymbol=B.PifSymbol
		and C.PifPolicyNumber=B.PifPolicyNumber
		and C.PifModule=B.PifModule
		and C.Pmdnxp1LocationNumber=B.Pmdnxp1LocationNumber
		and C.Pmdnxp1SubLocationNumber=B.Pmdnxp1SubLocationNumber)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule,Pmdnxp1InsuranceLine,Pmdnxp1LocationNumber,Pmdnxp1SubLocationNumber ORDER BY Pmdnxp1OtherMod) = 1
),
LKP_Pif43RXCPStage AS (
	SELECT
	Pmdrxp1PmaCode,
	Pmdrxp1WindAndHail,
	wb_class_of_business,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	Pmdrxp1InsuranceLine,
	Pmdrxp1LocationNumber,
	Pmdrxp1SubLocationNumber,
	Pmdrxp1CspClsCode,
	Pmdrxp1PmsDefSubjOfIns
	FROM (
		SELECT J.Pmdrxp1PmaCode as Pmdrxp1PmaCode, 
		J.PifSymbol as PifSymbol, 
		J.PifPolicyNumber as PifPolicyNumber, 
		J.PifModule as PifModule, 
		J.Pmdrxp1InsuranceLine as Pmdrxp1InsuranceLine, 
		J.Pmdrxp1LocationNumber as Pmdrxp1LocationNumber, 
		J.Pmdrxp1SubLocationNumber as Pmdrxp1SubLocationNumber, 
		LTRIM(RTRIM(J.Pmdrxp1CspClsCode)) as Pmdrxp1CspClsCode, 
		J.Pmdrxp1PmsDefSubjOfIns as Pmdrxp1PmsDefSubjOfIns, 
		J.Pmdrxp1WindAndHail as Pmdrxp1WindAndHail,
		P.wb_class_of_business as wb_class_of_business
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43RXCPStage J
		inner hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_02_stage P
		on J.PifSymbol=P.pif_symbol and J.PifPolicyNumber=P.pif_policy_number and J.PifModule=P.pif_module
		and J.Pif43RXCPStageId in (
		select max(Pif43RXCPStageId) from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43RXCPStage B
		where J.PifSymbol=B.PifSymbol
		and J.PifPolicyNumber=B.PifPolicyNumber
		and J.PifModule=B.PifModule
		and B.Pmdrxp1LocationNumber=J.Pmdrxp1LocationNumber
		and B.Pmdrxp1SubLocationNumber=J.Pmdrxp1SubLocationNumber
		and B.Pmdrxp1CspClsCode=J.Pmdrxp1CspClsCode
		and B.Pmdrxp1PmsDefSubjOfIns=J.Pmdrxp1PmsDefSubjOfIns)
		and P.pif_02_stage_id in (
		select max(pif_02_stage_id) from @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_02_stage P2
		where P2.pif_symbol=P.pif_symbol and P2.pif_policy_number=P.pif_policy_number and P2.pif_module = P.pif_module)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule,Pmdrxp1InsuranceLine,Pmdrxp1LocationNumber,Pmdrxp1SubLocationNumber,Pmdrxp1CspClsCode,Pmdrxp1PmsDefSubjOfIns ORDER BY Pmdrxp1PmaCode) = 1
),
LKP_Pif43RXGLStage AS (
	SELECT
	Pmdrxg1ScheduleMod,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	Pmdrxg1InsuranceLine,
	Pmdrxg1PmsDefGlSubline,
	Pmdrxg1RiskTypeInd
	FROM (
		SELECT 
			Pmdrxg1ScheduleMod,
			PifSymbol,
			PifPolicyNumber,
			PifModule,
			Pmdrxg1InsuranceLine,
			Pmdrxg1PmsDefGlSubline,
			Pmdrxg1RiskTypeInd
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43RXGLStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule,Pmdrxg1InsuranceLine,Pmdrxg1PmsDefGlSubline,Pmdrxg1RiskTypeInd ORDER BY Pmdrxg1ScheduleMod) = 1
),
LKP_Pif43RXIMStage AS (
	SELECT
	PMDRXI1Irpm,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	PMDRXI1InsuranceLine,
	PMDRXI1LocationNumber,
	PMDRXI1SubLocationNumber,
	PMDRXI1RiskUnitGroup,
	PMDRXI1SequenceNumber
	FROM (
		SELECT 
			PMDRXI1Irpm,
			PifSymbol,
			PifPolicyNumber,
			PifModule,
			PMDRXI1InsuranceLine,
			PMDRXI1LocationNumber,
			PMDRXI1SubLocationNumber,
			PMDRXI1RiskUnitGroup,
			PMDRXI1SequenceNumber
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43RXIMStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule,PMDRXI1InsuranceLine,PMDRXI1LocationNumber,PMDRXI1SubLocationNumber,PMDRXI1RiskUnitGroup,PMDRXI1SequenceNumber ORDER BY PMDRXI1Irpm) = 1
),
LKP_Pif43UXGLStage AS (
	SELECT
	Pmduxg1IncLimitTableInd,
	Pmduxg1PmaCode,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	Pmduxg1InsuranceLine,
	Pmduxg1LocationNumber,
	Pmduxg1PmsDefGlSubline,
	Pmduxg1ClassCodeGroup,
	Pmduxg1ClassCodeMember,
	Pmduxg1ClassCode,
	Pmduxg1RiskSequence,
	Pmduxg1RiskTypeInd
	FROM (
		SELECT I.Pmduxg1IncLimitTableInd as Pmduxg1IncLimitTableInd, 
		I.Pmduxg1PmaCode as Pmduxg1PmaCode, 
		I.PifSymbol as PifSymbol, 
		I.PifPolicyNumber as PifPolicyNumber, 
		I.PifModule as PifModule, 
		I.Pmduxg1InsuranceLine as Pmduxg1InsuranceLine, 
		I.Pmduxg1LocationNumber as Pmduxg1LocationNumber, 
		I.Pmduxg1PmsDefGlSubline as Pmduxg1PmsDefGlSubline, 
		I.Pmduxg1ClassCodeGroup as Pmduxg1ClassCodeGroup, 
		I.Pmduxg1ClassCodeMember as Pmduxg1ClassCodeMember, 
		I.Pmduxg1ClassCode as Pmduxg1ClassCode, 
		I.Pmduxg1RiskSequence as Pmduxg1RiskSequence, 
		I.Pmduxg1RiskTypeInd as Pmduxg1RiskTypeInd
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43UXGLStage I
		WHERE I.Pif43UXGLStageId in (
		select max(Pif43UXGLStageId) from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43UXGLStage B
		where I.PifSymbol=B.PifSymbol
		and I.PifPolicyNumber=B.PifPolicyNumber
		and I.PifModule=B.PifModule
		and B.Pmduxg1LocationNumber=I.Pmduxg1LocationNumber
		and B.Pmduxg1PmsDefGlSubline=I.Pmduxg1PmsDefGlSubline
		and B.Pmduxg1ClassCodeGroup=I.Pmduxg1ClassCodeGroup
		and B.Pmduxg1ClassCodeMember=I.Pmduxg1ClassCodeMember
		and B.Pmduxg1ClassCode=I.Pmduxg1ClassCode
		and B.Pmduxg1RiskSequence=I.Pmduxg1RiskSequence
		and B.Pmduxg1RiskTypeInd=I.Pmduxg1RiskTypeInd
		and B.Pmduxg1RiskTypeInd in ('O','P'))
		and I.Pmduxg1RiskTypeInd in ('O','P')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule,Pmduxg1InsuranceLine,Pmduxg1LocationNumber,Pmduxg1PmsDefGlSubline,Pmduxg1ClassCodeGroup,Pmduxg1ClassCodeMember,Pmduxg1ClassCode,Pmduxg1RiskSequence,Pmduxg1RiskTypeInd ORDER BY Pmduxg1IncLimitTableInd) = 1
),
LKP_Pif43UXWCStage AS (
	SELECT
	Pmdu4w1Rate,
	in_pif_symbol,
	in_pif_policy_number,
	in_pif_module,
	in_sar_insurance_line,
	in_sar_location_x,
	in_sar_state,
	lkp_sar_class_1_4,
	in_sar_class_5_6,
	in_sar_seq_no,
	in_sar_cov_eff_year,
	in_sar_cov_eff_month,
	in_sar_cov_eff_day,
	in_ClassCodeSeq,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	Pmdu4w1InsuranceLine,
	Pmdu4w1WcRatingState,
	Pmdu4w1LocationNumber,
	Pmdu4w1ClassCode,
	Pmdu4w1SplitRateSeq,
	Pmdu4w1YearItemEffective,
	Pmdu4w1MonthItemEffective,
	Pmdu4w1DayItemEffective,
	Pmdu4w1ClassCodeSeq
	FROM (
		SELECT L.Pmdu4w1Rate as Pmdu4w1Rate, 
		L.PifSymbol as PifSymbol, 
		L.PifPolicyNumber as PifPolicyNumber, 
		L.PifModule as PifModule, 
		L.Pmdu4w1InsuranceLine as Pmdu4w1InsuranceLine, 
		L.Pmdu4w1WcRatingState as Pmdu4w1WcRatingState, 
		L.Pmdu4w1LocationNumber as Pmdu4w1LocationNumber, 
		LTRIM(RTRIM(L.Pmdu4w1ClassCode)) as Pmdu4w1ClassCode, 
		L.Pmdu4w1ClassDescInd as Pmdu4w1ClassDescInd, 
		L.Pmdu4w1SplitRateSeq as Pmdu4w1SplitRateSeq, 
		L.Pmdu4w1YearItemEffective as Pmdu4w1YearItemEffective, 
		L.Pmdu4w1MonthItemEffective as Pmdu4w1MonthItemEffective, 
		L.Pmdu4w1DayItemEffective as Pmdu4w1DayItemEffective, 
		L.Pmdu4w1ClassCodeSeq as Pmdu4w1ClassCodeSeq 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43UXWCStage L
		WHERE L.Pif43UXWCStageId in (
		select max(Pif43UXWCStageId) from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43UXWCStage B
		where L.PifSymbol=B.PifSymbol
		and L.PifPolicyNumber=B.PifPolicyNumber
		and L.PifModule=B.PifModule
		and B.Pmdu4w1WcRatingState=L.Pmdu4w1WcRatingState
		and B.Pmdu4w1LocationNumber=L.Pmdu4w1LocationNumber
		and B.Pmdu4w1ClassCode=L.Pmdu4w1ClassCode
		and B.Pmdu4w1ClassDescInd=L.Pmdu4w1ClassDescInd
		and B.Pmdu4w1SplitRateSeq=L.Pmdu4w1SplitRateSeq
		--and B.Pmdu4w1PremiumBasisAmt=L.Pmdu4w1PremiumBasisAmt
		and B.Pmdu4w1YearItemEffective=L.Pmdu4w1YearItemEffective
		and B.Pmdu4w1MonthItemEffective=L.Pmdu4w1MonthItemEffective
		and B.Pmdu4w1DayItemEffective=L.Pmdu4w1DayItemEffective
		and B.Pmdu4w1ClassCodeSeq=L.Pmdu4w1ClassCodeSeq)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule,Pmdu4w1InsuranceLine,Pmdu4w1WcRatingState,Pmdu4w1LocationNumber,Pmdu4w1ClassCode,Pmdu4w1SplitRateSeq,Pmdu4w1YearItemEffective,Pmdu4w1MonthItemEffective,Pmdu4w1DayItemEffective,Pmdu4w1ClassCodeSeq ORDER BY Pmdu4w1Rate) = 1
),
LKP_Pif43UYGLStage AS (
	SELECT
	Pmduyg1PkgModFactor,
	Pmduyg1IncreaseLimitsFactor,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	Pmduyg1InsuranceLine,
	Pmduyg1LocationNumber,
	Pmduyg1PmsDefGlSubline,
	Pmduyg1RiskSequence,
	Pmduyg1RiskTypeInd,
	Pmduyg1ClassCode,
	Pmduyg1YearItemEffective,
	Pmduyg1MonthItemEffective,
	Pmduyg1DayItemEffective
	FROM (
		SELECT F.Pmduyg1PkgModFactor as Pmduyg1PkgModFactor, 
		F.Pmduyg1IncreaseLimitsFactor as Pmduyg1IncreaseLimitsFactor, 
		F.PifSymbol as PifSymbol, 
		F.PifPolicyNumber as PifPolicyNumber, 
		F.PifModule as PifModule, 
		F.Pmduyg1InsuranceLine as Pmduyg1InsuranceLine, 
		F.Pmduyg1LocationNumber as Pmduyg1LocationNumber, 
		F.Pmduyg1PmsDefGlSubline as Pmduyg1PmsDefGlSubline, 
		F.Pmduyg1RiskSequence as Pmduyg1RiskSequence, 
		Case When LEN(LTRIM(RTRIM(F.Pmduyg1RiskTypeInd)))=0 Then 'B' Else F.Pmduyg1RiskTypeInd end as Pmduyg1RiskTypeInd, 
		F.Pmduyg1ClassCode as Pmduyg1ClassCode, 
		F.Pmduyg1YearItemEffective as Pmduyg1YearItemEffective, 
		F.Pmduyg1MonthItemEffective as Pmduyg1MonthItemEffective, 
		F.Pmduyg1DayItemEffective as Pmduyg1DayItemEffective
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43UYGLStage F
		WHERE F.Pif43UYGLStageId in (
		select max(Pif43UYGLStageID) from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43UYGLStage B
		where F.PifSymbol=B.PifSymbol
		and F.PifPolicyNumber=B.PifPolicyNumber
		and F.PifModule=B.PifModule
		and B.Pmduyg1InsuranceLine=F.Pmduyg1InsuranceLine
		and B.Pmduyg1LocationNumber=F.Pmduyg1LocationNumber
		and B.Pmduyg1PmsDefGlSubline=F.Pmduyg1PmsDefGlSubline
		and B.Pmduyg1RiskSequence=F.Pmduyg1RiskSequence
		and B.Pmduyg1RiskTypeInd=F.Pmduyg1RiskTypeInd
		and B.Pmduyg1ClassCode=F.Pmduyg1ClassCode
		and B.Pmduyg1YearItemEffective=F.Pmduyg1YearItemEffective
		and B.Pmduyg1MonthItemEffective=F.Pmduyg1MonthItemEffective
		and B.Pmduyg1DayItemEffective=F.Pmduyg1DayItemEffective)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule,Pmduyg1InsuranceLine,Pmduyg1LocationNumber,Pmduyg1PmsDefGlSubline,Pmduyg1RiskSequence,Pmduyg1RiskTypeInd,Pmduyg1ClassCode,Pmduyg1YearItemEffective,Pmduyg1MonthItemEffective,Pmduyg1DayItemEffective ORDER BY Pmduyg1PkgModFactor) = 1
),
LKP_Pif43UZWCStage AS (
	SELECT
	Pmdu4w1Rate,
	PifSymbol,
	PifPolicyNumber,
	PifPolicyModule,
	PMDU4W1InsuranceLine,
	PMDU4W1WCRatingState,
	PMDU4W1LocationNumber,
	PMDU4W1ClassCode,
	PMDU4W1SplitRateSeq,
	PMDU4W1YearItemEffective,
	PMDU4W1MonthItemEffective,
	PMDU4W1DayItemEffective,
	PMDU4W1ClassCodeSeq
	FROM (
		SELECT L.Pmdu4w1Rate as Pmdu4w1Rate, 
		L.PifSymbol as PifSymbol, 
		L.PifPolicyNumber as PifPolicyNumber, 
		L.PifPolicyModule as PifPolicyModule, 
		L.Pmdu4w1InsuranceLine as Pmdu4w1InsuranceLine, 
		L.Pmdu4w1WcRatingState as Pmdu4w1WcRatingState, 
		L.Pmdu4w1LocationNumber as Pmdu4w1LocationNumber, 
		LTRIM(RTRIM(L.Pmdu4w1ClassCode)) as Pmdu4w1ClassCode, 
		L.Pmdu4w1ClassDescInd as Pmdu4w1ClassDescInd, 
		L.Pmdu4w1SplitRateSeq as Pmdu4w1SplitRateSeq, 
		L.Pmdu4w1YearItemEffective as Pmdu4w1YearItemEffective, 
		L.Pmdu4w1MonthItemEffective as Pmdu4w1MonthItemEffective, 
		L.Pmdu4w1DayItemEffective as Pmdu4w1DayItemEffective, 
		L.Pmdu4w1ClassCodeSeq as Pmdu4w1ClassCodeSeq 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43UZWCStage L
		WHERE L.Pif43UZWCStageId in (
		select max(Pif43UZWCStageId) from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43UZWCStage B
		where L.PifSymbol=B.PifSymbol
		and L.PifPolicyNumber=B.PifPolicyNumber
		and L.PifPolicyModule=B.PifPolicyModule
		and B.Pmdu4w1WcRatingState=L.Pmdu4w1WcRatingState
		and B.Pmdu4w1LocationNumber=L.Pmdu4w1LocationNumber
		and B.Pmdu4w1ClassCode=L.Pmdu4w1ClassCode
		and B.Pmdu4w1ClassDescInd=L.Pmdu4w1ClassDescInd
		and B.Pmdu4w1SplitRateSeq=L.Pmdu4w1SplitRateSeq
		and B.Pmdu4w1YearItemEffective=L.Pmdu4w1YearItemEffective
		and B.Pmdu4w1MonthItemEffective=L.Pmdu4w1MonthItemEffective
		and B.Pmdu4w1DayItemEffective=L.Pmdu4w1DayItemEffective
		and B.Pmdu4w1ClassCodeSeq =L.Pmdu4w1ClassCodeSeq )
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifPolicyModule,PMDU4W1InsuranceLine,PMDU4W1WCRatingState,PMDU4W1LocationNumber,PMDU4W1ClassCode,PMDU4W1SplitRateSeq,PMDU4W1YearItemEffective,PMDU4W1MonthItemEffective,PMDU4W1DayItemEffective,PMDU4W1ClassCodeSeq ORDER BY Pmdu4w1Rate) = 1
),
LKP_PifPUHM17Stage AS (
	SELECT
	HRRConstruction,
	PifSymbol,
	PifPolicyNumber,
	PifPolicyModule,
	HRRUnitAlph,
	HRRZipCode,
	HRRCountyLocation,
	HRRCityLocation
	FROM (
		SELECT M.HRRConstruction as HRRConstruction, 
		M.PifSymbol as PifSymbol, 
		M.PifPolicyNumber as PifPolicyNumber, 
		M.PifPolicyModule as PifPolicyModule, 
		(case M.HRRUnitAlph 
		      when '0' then '1'
		      when '1' then '2' 
		      when '2' then '3' 
		 else 
		      '-1' 
		 end) as HRRUnitAlph, 
		LTRIM(RTRIM(M.HRRZipCode)) as HRRZipCode, 
		LTRIM(RTRIM(M.HRRCountyLocation)) as HRRCountyLocation, 
		LTRIM(RTRIM(M.HRRCityLocation)) as HRRCityLocation 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PifPUHM17Stage M
		WHERE M.PifPUHM17StageID in (
		select max(PifPUHM17StageID) from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PifPUHM17Stage B
		where M.PifSymbol=B.PifSymbol
		and M.PifPolicyNumber=B.PifPolicyNumber
		and M.PifPolicyModule=B.PifPolicyModule
		and M.HRRUnitAlph=B.HRRUnitAlph
		and M.HRRTerritory=B.HRRTerritory
		and M.HRRZipCode=B.HRRZipCode
		and M.HRRCountyLocation=B.HRRCountyLocation
		and M.HRRCityLocation=B.HRRCityLocation)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifPolicyModule,HRRUnitAlph,HRRZipCode,HRRCountyLocation,HRRCityLocation ORDER BY HRRConstruction) = 1
),
LKP_Policy_PolicyAKID AS (
	SELECT
	pol_ak_id,
	pol_key
	FROM (
		SELECT policy.pol_ak_id as pol_ak_id, ltrim(rtrim(policy.pol_key)) as pol_key FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy
		WHERE crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_ak_id DESC) = 1
),
LKP_pif_02_stage AS (
	SELECT
	pif_eff_yr_a,
	pif_eff_mo_a,
	pif_eff_da_a,
	CLBCode,
	pif_symbol,
	pif_policy_number,
	pif_module
	FROM (
		SELECT N.pif_eff_yr_a as pif_eff_yr_a, 
		N.pif_eff_mo_a as pif_eff_mo_a, 
		N.pif_eff_da_a as pif_eff_da_a, 
		N.pif_symbol as pif_symbol, 
		N.pif_policy_number as pif_policy_number, 
		N.pif_module as pif_module ,
		N.wb_class_of_business as CLBCode 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_02_stage N
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module ORDER BY pif_eff_yr_a) = 1
),
EXP_Values AS (
	SELECT
	LKP_Policy_PolicyAKID.pol_ak_id,
	EXP_Default.Policy_Key,
	EXP_Default.sar_location_x,
	-- *INF*: LTRIM(RTRIM(sar_location_x))
	LTRIM(RTRIM(sar_location_x)) AS v_RiskLocation_Unit,
	EXP_Default.sar_state,
	-- *INF*: LTRIM(RTRIM(sar_state))
	LTRIM(RTRIM(sar_state)) AS v_sar_state,
	v_sar_state AS o_sar_state,
	EXP_Default.sar_loc_prov_territory,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_loc_prov_territory)
	-- 
	-- --IIF(ISNULL(sar_loc_prov_territory) OR IS_SPACES(sar_loc_prov_territory) OR LENGTH(sar_loc_prov_territory) = 0, 'N/A',
	-- -- LTRIM(RTRIM(sar_loc_prov_territory)))
	-- 
	-- 
	-- 
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_loc_prov_territory) AS v_sar_loc_prov_territory,
	EXP_Default.sar_city,
	-- *INF*: LTRIM(RTRIM(sar_city))
	-- 
	-- --IIF(IS_SPACES(LTRIM(RTRIM(sar_city)))  OR ISNULL(LTRIM(RTRIM(sar_city))) OR LENGTH(LTRIM(RTRIM(sar_city))) < 3, '000', LTRIM(RTRIM(sar_city)))
	-- 
	-- 
	LTRIM(RTRIM(sar_city)) AS v_sar_city,
	-- *INF*: IIF(REG_MATCH(:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_city) ,'(\d{6})')
	-- ,:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_city)
	-- ,'000000')
	-- 
	-- --v_sar_county_first_two  ||  v_sar_county_last_one  ||  v_sar_city
	-- 
	-- --IIF(ISNULL(Tax_Location)  OR IS_SPACES(Tax_Location)  OR LENGTH(Tax_Location) = 0 , '000000', Tax_Location)
	IFF(REG_MATCH(:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_city), '(\d{6})'), :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_city), '000000') AS v_Tax_Location,
	EXP_Default.sar_zip_postal_code,
	-- *INF*: IIF(ISNULL(sar_zip_postal_code)  OR IS_SPACES(sar_zip_postal_code)  OR LENGTH(sar_zip_postal_code) = 0 , 'N/A', LTRIM(RTRIM(sar_zip_postal_code)))
	IFF(sar_zip_postal_code IS NULL OR IS_SPACES(sar_zip_postal_code) OR LENGTH(sar_zip_postal_code) = 0, 'N/A', LTRIM(RTRIM(sar_zip_postal_code))) AS v_sar_zip_postal_code,
	-- *INF*: :LKP.LKP_RISKLOCATION_RISKLOCATIONAKID(pol_ak_id, v_RiskLocation_Unit, v_sar_loc_prov_territory, v_sar_state, v_sar_zip_postal_code, v_Tax_Location)
	LKP_RISKLOCATION_RISKLOCATIONAKID_pol_ak_id_v_RiskLocation_Unit_v_sar_loc_prov_territory_v_sar_state_v_sar_zip_postal_code_v_Tax_Location.RiskLocationAKID AS v_RiskLocationAKID,
	EXP_Default.sar_insurance_line,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_insurance_line)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_insurance_line) AS v_sar_insurance_line,
	EXP_Default.sar_type_bureau,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_type_bureau)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_type_bureau) AS v_sar_type_bureau,
	EXP_Default.sar_sub_location_x,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_sub_location_x)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_sub_location_x) AS v_sar_sub_location_x,
	EXP_Default.sar_risk_unit_group,
	-- *INF*: IIF(REG_MATCH(:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_risk_unit_group) ,'(\d{3})')
	-- ,:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_risk_unit_group)
	-- ,'N/A')
	-- 
	-- ---- Checking the length of the field to 3 and all the positions of the field are any one of 0-9, if it is not then we are defaulting it to 'N/A', by this way we are cleansing junk values from the source.
	-- 
	-- 
	-- ---:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_risk_unit_group)
	IFF(REG_MATCH(:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_risk_unit_group), '(\d{3})'), :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_risk_unit_group), 'N/A') AS v_sar_risk_unit_group,
	EXP_Default.sar_class_code_grp_x,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_class_code_grp_x)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_class_code_grp_x) AS v_sar_class_code_grp_x,
	EXP_Default.sar_unit,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_unit)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_unit) AS v_sar_unit,
	-- *INF*: SUBSTR(:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_unit),1,3)
	SUBSTR(:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_unit), 1, 3) AS v_sar_unit_3pos,
	EXP_Default.sar_seq_rsk_unt_a,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_seq_rsk_unt_a)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_seq_rsk_unt_a) AS v_sar_seq_rsk_unt_a,
	EXP_Default.sar_type_exposure,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_type_exposure)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_type_exposure) AS v_sar_type_exposure,
	EXP_Default.sar_major_peril,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_major_peril)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_major_peril) AS v_sar_major_peril,
	EXP_Default.sar_seq_no,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_seq_no)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_seq_no) AS v_sar_seq_no,
	-- *INF*: LTRIM(RTRIM(v_sar_seq_no))
	LTRIM(RTRIM(v_sar_seq_no)) AS o_sar_seq_no,
	EXP_Default.sar_cov_eff_year,
	-- *INF*: TO_CHAR(sar_cov_eff_year)
	TO_CHAR(sar_cov_eff_year) AS v_sar_cov_eff_year,
	EXP_Default.sar_cov_eff_month,
	-- *INF*: TO_CHAR(sar_cov_eff_month)
	TO_CHAR(sar_cov_eff_month) AS v_sar_cov_eff_month,
	EXP_Default.sar_cov_eff_day,
	-- *INF*: TO_CHAR(sar_cov_eff_day)
	TO_CHAR(sar_cov_eff_day) AS v_sar_cov_eff_day,
	-- *INF*: TO_DATE(v_sar_cov_eff_month || '/' || v_sar_cov_eff_day || '/'|| v_sar_cov_eff_year ,'MM/DD/YYYY')
	TO_DATE(v_sar_cov_eff_month || '/' || v_sar_cov_eff_day || '/' || v_sar_cov_eff_year, 'MM/DD/YYYY') AS v_sar_cov_eff_date,
	EXP_Default.sar_agents_comm_rate,
	-- *INF*: IIF(ISNULL(sar_agents_comm_rate) , 0.00000 , sar_agents_comm_rate)
	IFF(sar_agents_comm_rate IS NULL, 0.00000, sar_agents_comm_rate) AS v_sar_agents_comm_rate,
	-- *INF*: MD5(TO_CHAR(pol_ak_id)  || 
	--  TO_CHAR(v_RiskLocationAKID)  || 
	--  TO_CHAR(v_sar_insurance_line)  || 
	--  TO_CHAR(v_sar_type_bureau) )
	MD5(TO_CHAR(pol_ak_id) || TO_CHAR(v_RiskLocationAKID) || TO_CHAR(v_sar_insurance_line) || TO_CHAR(v_sar_type_bureau)) AS v_PolicyCoverageHashKey,
	-- *INF*: :LKP.LKP_POLICYCOVERAGE_POLICYCOVERAGEAKID(v_PolicyCoverageHashKey)
	LKP_POLICYCOVERAGE_POLICYCOVERAGEAKID_v_PolicyCoverageHashKey.PolicyCoverageAKID AS v_PolicyCoverageAKID,
	EXP_Default.sar_section,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_section)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_section) AS v_sar_section,
	EXP_Default.sar_class_1_4 AS sar_class_code,
	EXP_Default.lkp_sar_class_1_4 AS in_sar_class_1_4,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_sar_class_1_4)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_sar_class_1_4) AS v_sar_class_1_4,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_class_code)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_class_code) AS v_sar_class_code,
	EXP_Default.out_lkp_sar_class_1_4,
	EXP_Default.sar_exposure,
	-- *INF*: IIF(ISNULL(sar_exposure),0,IIF(IN(ltrim(rtrim(v_sar_class_code)),'770901','090801','0913','091301','091342', '910801','9108A','942801','942802') AND IN( ltrim(rtrim( sar_code_13)),'U','UA','UD','UP') AND ltrim(rtrim(v_sar_insurance_line))='WC', sar_exposure/10,sar_exposure))
	-- 
	-- 
	-- 
	IFF(sar_exposure IS NULL, 0, IFF(IN(ltrim(rtrim(v_sar_class_code)), '770901', '090801', '0913', '091301', '091342', '910801', '9108A', '942801', '942802') AND IN(ltrim(rtrim(sar_code_13)), 'U', 'UA', 'UD', 'UP') AND ltrim(rtrim(v_sar_insurance_line)) = 'WC', sar_exposure / 10, sar_exposure)) AS v_sar_exposure,
	-- *INF*: IIF(ISNULL(sar_exposure),0,IIF(IN(ltrim(rtrim(v_sar_class_code)),'770901','090801','0913','091301','091342', '910801','9108A','942801','942802') AND IN( ltrim(rtrim( sar_code_13)),'U','UA','UD','UP') AND ltrim(rtrim(v_sar_insurance_line))='WC', sar_exposure/10, IIF(ltrim(rtrim(v_sar_insurance_line))='WC',sar_exposure, 0)))
	IFF(sar_exposure IS NULL, 0, IFF(IN(ltrim(rtrim(v_sar_class_code)), '770901', '090801', '0913', '091301', '091342', '910801', '9108A', '942801', '942802') AND IN(ltrim(rtrim(sar_code_13)), 'U', 'UA', 'UD', 'UP') AND ltrim(rtrim(v_sar_insurance_line)) = 'WC', sar_exposure / 10, IFF(ltrim(rtrim(v_sar_insurance_line)) = 'WC', sar_exposure, 0))) AS v_writtenexposure,
	-- *INF*: TO_CHAR(sar_cov_eff_year) || LPAD(TO_CHAR(LTRIM(RTRIM(sar_cov_eff_month))), 2, '0') || LPAD(TO_CHAR(LTRIM(RTRIM(sar_cov_eff_day))), 2, '0')
	TO_CHAR(sar_cov_eff_year) || LPAD(TO_CHAR(LTRIM(RTRIM(sar_cov_eff_month))), 2, '0') || LPAD(TO_CHAR(LTRIM(RTRIM(sar_cov_eff_day))), 2, '0') AS v_cov_eff_date,
	-- *INF*: TO_DATE(v_cov_eff_date, 'YYYYMMDD')
	TO_DATE(v_cov_eff_date, 'YYYYMMDD') AS cov_eff_date,
	EXP_Default.sar_sub_line,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_sub_line)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_sub_line) AS v_sar_sub_line,
	-- *INF*: MD5(
	-- IIF(ISNULL(v_PolicyCoverageAKID),'-1',TO_CHAR(v_PolicyCoverageAKID))   || 
	-- v_sar_sub_location_x   || 
	-- v_sar_risk_unit_group   || 
	-- v_sar_class_code_grp_x   || 
	-- v_sar_unit   || 
	-- v_sar_seq_rsk_unt_a   || 
	-- v_sar_major_peril   || 
	-- v_sar_seq_no   || 
	-- v_sar_sub_line   || 
	-- v_sar_type_exposure   || 
	-- v_sar_class_code   || 
	-- v_sar_section
	-- )
	MD5(IFF(v_PolicyCoverageAKID IS NULL, '-1', TO_CHAR(v_PolicyCoverageAKID)) || v_sar_sub_location_x || v_sar_risk_unit_group || v_sar_class_code_grp_x || v_sar_unit || v_sar_seq_rsk_unt_a || v_sar_major_peril || v_sar_seq_no || v_sar_sub_line || v_sar_type_exposure || v_sar_class_code || v_sar_section) AS v_CoverageDetailHashKey,
	-- *INF*: :LKP.LKP_STATISTICALCOVERAGE_STATISTICALCOVERAGEAKID(v_CoverageDetailHashKey)
	LKP_STATISTICALCOVERAGE_STATISTICALCOVERAGEAKID_v_CoverageDetailHashKey.StatisticalCoverageAKID AS v_CoverageDetailAKID,
	-- *INF*: IIF(NOT ISNULL(v_CoverageDetailAKID), v_CoverageDetailAKID, -1)
	IFF(NOT v_CoverageDetailAKID IS NULL, v_CoverageDetailAKID, - 1) AS CoverageDetailAKID,
	EXP_Default.sar_id,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_id)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_id) AS v_PMSFunctionCode,
	v_PMSFunctionCode AS PMSFunctionCode,
	EXP_Default.sar_part_code,
	-- *INF*: IIF(sar_part_code = '1', 'D','C')
	IFF(sar_part_code = '1', 'D', 'C') AS v_PremiumType,
	v_PremiumType AS PremiumType,
	EXP_Default.sar_trans_eff_year,
	-- *INF*: TO_CHAR(sar_trans_eff_year)
	TO_CHAR(sar_trans_eff_year) AS v_sar_trans_eff_year,
	EXP_Default.sar_trans_eff_month,
	-- *INF*: IIF(TO_CHAR(sar_trans_eff_month) = '0','1',TO_CHAR(sar_trans_eff_month)
	-- )
	IFF(TO_CHAR(sar_trans_eff_month) = '0', '1', TO_CHAR(sar_trans_eff_month)) AS v_sar_trans_eff_month,
	EXP_Default.sar_trans_eff_day,
	-- *INF*: IIF(TO_CHAR(sar_trans_eff_day) ='0','1',TO_CHAR(sar_trans_eff_day))
	IFF(TO_CHAR(sar_trans_eff_day) = '0', '1', TO_CHAR(sar_trans_eff_day)) AS v_sar_trans_eff_day,
	-- *INF*: TO_DATE(v_sar_trans_eff_month || '/' || v_sar_trans_eff_day || '/'|| v_sar_trans_eff_year ,'MM/DD/YYYY')
	TO_DATE(v_sar_trans_eff_month || '/' || v_sar_trans_eff_day || '/' || v_sar_trans_eff_year, 'MM/DD/YYYY') AS v_sar_trans_eff_date,
	v_sar_trans_eff_date AS Trans_eff_date,
	EXP_Default.sar_reinsurance_company_no,
	EXP_Default.sar_entrd_date,
	-- *INF*: TO_DATE(sar_entrd_date,'YYYYMMDD')
	TO_DATE(sar_entrd_date, 'YYYYMMDD') AS v_sar_entrd_date,
	v_sar_entrd_date AS Trans_entered_date,
	EXP_Default.sar_exp_year,
	-- *INF*: TO_CHAR(sar_exp_year)
	TO_CHAR(sar_exp_year) AS v_sar_exp_year,
	EXP_Default.sar_exp_month,
	-- *INF*: TO_CHAR(sar_exp_month)
	TO_CHAR(sar_exp_month) AS v_sar_exp_month,
	EXP_Default.sar_exp_day,
	-- *INF*: TO_CHAR(sar_exp_day)
	TO_CHAR(sar_exp_day) AS v_sar_exp_day,
	-- *INF*: TO_DATE(v_sar_exp_month || '/' || v_sar_exp_day || '/'|| v_sar_exp_year ,'MM/DD/YYYY')
	TO_DATE(v_sar_exp_month || '/' || v_sar_exp_day || '/' || v_sar_exp_year, 'MM/DD/YYYY') AS v_sar_exp_date,
	v_sar_exp_date AS Trans_expiration_date,
	EXP_Default.sar_transaction,
	EXP_Default.sar_premium,
	EXP_Default.sar_original_prem,
	EXP_Default.sar_acct_entrd_date,
	-- *INF*: TO_DATE('01'  || sar_acct_entrd_date, 'DDYYYYMM')
	TO_DATE('01' || sar_acct_entrd_date, 'DDYYYYMM') AS v_sar_acct_entrd_date,
	-- *INF*: TO_DATE('12312100', 'MMDDYYYY')
	TO_DATE('12312100', 'MMDDYYYY') AS v_dummy_start_date,
	-- *INF*: TO_DATE('01011800', 'MMDDYYYY')
	TO_DATE('01011800', 'MMDDYYYY') AS v_dummy_end_date,
	v_sar_acct_entrd_date AS Trans_Booked_date,
	-- *INF*: :LKP.LKP_POLICY_STRTGC_BUS_DIV_CODE(pol_ak_id)
	LKP_POLICY_STRTGC_BUS_DIV_CODE_pol_ak_id.strtgc_bus_dvsn_code AS v_PolicyStrategicBusinessDivisionCode,
	EXP_Default.sar_faculta_comm_rate,
	-- *INF*: IIF(ISNULL(sar_faculta_comm_rate),0.0,sar_faculta_comm_rate)
	IFF(sar_faculta_comm_rate IS NULL, 0.0, sar_faculta_comm_rate) AS v_sar_faculta_comm_rate,
	-- *INF*: IIF (v_PolicyStrategicBusinessDivisionCode = '1', 
	--        NULL,
	--         IIF (v_sar_insurance_line = 'N/A',
	--              NULL, 
	--             :LKP.LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES(pol_ak_id, v_sar_insurance_line, v_RiskLocation_Unit, v_sar_sub_location_x, v_sar_section, sar_reinsurance_company_no,v_sar_faculta_comm_rate,v_sar_trans_eff_date, v_sar_exp_date)
	--           )
	--          )
	-- 
	-- ---- This provides a full key match with location, sublocation and date bounds along with insurance line for CL with InsuranceLine <> 'N/A'
	IFF(v_PolicyStrategicBusinessDivisionCode = '1', NULL, IFF(v_sar_insurance_line = 'N/A', NULL, LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_sub_location_x_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_cov_ak_id)) AS v_ReinsuranceCoverageAKID_CommercialLineslocsubwd,
	-- *INF*: IIF (v_PolicyStrategicBusinessDivisionCode = '1', 
	--        NULL,
	--         IIF (v_sar_insurance_line = 'N/A',
	--              NULL, 
	--              IIF (ISNULL(v_ReinsuranceCoverageAKID_CommercialLineslocsubwd),
	--                   :LKP.LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES(pol_ak_id, v_sar_insurance_line,   v_RiskLocation_Unit, v_sar_sub_location_x, v_sar_section, sar_reinsurance_company_no,v_sar_faculta_comm_rate, v_dummy_start_date, v_dummy_end_date),
	--     v_ReinsuranceCoverageAKID_CommercialLineslocsubwd)
	--                 )
	--              )
	--                    
	-- --- This provides a full key match with location, sublocation and no date bounds along with insurance line for CL with InsuranceLine <> 'N/A' , we try this lookup if a full key match fails. The dummy date ports allow us to reuse the existing lookups with datebounds that satisfy the condition
	IFF(v_PolicyStrategicBusinessDivisionCode = '1', NULL, IFF(v_sar_insurance_line = 'N/A', NULL, IFF(v_ReinsuranceCoverageAKID_CommercialLineslocsubwd IS NULL, LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_sub_location_x_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_cov_ak_id, v_ReinsuranceCoverageAKID_CommercialLineslocsubwd))) AS v_ReinsuranceCoverageAKID_CommercialLineslocsub,
	-- *INF*: IIF (v_PolicyStrategicBusinessDivisionCode = '1', 
	--        NULL,
	--         IIF (v_sar_insurance_line = 'N/A',
	--            NULL, 
	--               IIF(ISNULL(v_ReinsuranceCoverageAKID_CommercialLineslocsub),
	--            :LKP.LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_WITHOUTSUBLOCATION(pol_ak_id, v_sar_insurance_line, v_RiskLocation_Unit, v_sar_section, sar_reinsurance_company_no,v_sar_faculta_comm_rate, v_sar_trans_eff_date, v_sar_exp_date),
	--              v_ReinsuranceCoverageAKID_CommercialLineslocsub)
	--                 )
	--             )
	-- 
	-- --- This provides a full key match with location, no sublocation and date bounds along with insurance line for CL with InsuranceLine <> 'N/A', we try this only if a full key match failed
	IFF(v_PolicyStrategicBusinessDivisionCode = '1', NULL, IFF(v_sar_insurance_line = 'N/A', NULL, IFF(v_ReinsuranceCoverageAKID_CommercialLineslocsub IS NULL, LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_WITHOUTSUBLOCATION_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_cov_ak_id, v_ReinsuranceCoverageAKID_CommercialLineslocsub))) AS v_ReinsuranceCoverageAKID_CommercialLines_locwd,
	-- *INF*: IIF (v_PolicyStrategicBusinessDivisionCode = '1', 
	--        NULL,
	--         IIF (v_sar_insurance_line = 'N/A',
	--            NULL,
	--               IIF(ISNULL (v_ReinsuranceCoverageAKID_CommercialLines_locwd),
	--                 :LKP.LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_WITHOUTSUBLOCATION(pol_ak_id, v_sar_insurance_line, v_RiskLocation_Unit, v_sar_section, sar_reinsurance_company_no,v_sar_faculta_comm_rate, v_dummy_start_date,v_dummy_end_date),
	--                  v_ReinsuranceCoverageAKID_CommercialLines_locwd)
	--                  )
	--            )
	-- 
	-- ---- This provides a full key match with location, no sublocation and no date bounds along with insurance line for CL with InsuranceLine <> 'N/A', we try this also if there was no full key match or match with date range and location alone. The dummy date ports allow us to reuse the existing lookups with datebounds that satisfy the condition
	IFF(v_PolicyStrategicBusinessDivisionCode = '1', NULL, IFF(v_sar_insurance_line = 'N/A', NULL, IFF(v_ReinsuranceCoverageAKID_CommercialLines_locwd IS NULL, LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_WITHOUTSUBLOCATION_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_cov_ak_id, v_ReinsuranceCoverageAKID_CommercialLines_locwd))) AS v_ReinsuranceCoverageAKID_CommercialLines_loc,
	-- *INF*: IIF (v_PolicyStrategicBusinessDivisionCode = '1', 
	--        NULL,
	--         IIF (v_sar_insurance_line = 'N/A',
	--            NULL,
	--             IIF(ISNULL(v_ReinsuranceCoverageAKID_CommercialLines_loc),
	-- :LKP.LKP_REINSURANCE_COVERAGE_COMMERCIALLINESPOLICIES_DEFAULT(pol_ak_id,v_sar_insurance_line,sar_reinsurance_company_no,v_sar_section, v_sar_faculta_comm_rate,v_sar_trans_eff_date, v_sar_exp_date),
	--             v_ReinsuranceCoverageAKID_CommercialLines_loc)
	--                  )
	--          )
	-- 
	-- --- When we are not getting a hit on commercial line policies using location and sublocation or only location with and without dates,  we just default to a lookup on polakid, insline, reinscompanyno, sectioncode with dates
	IFF(v_PolicyStrategicBusinessDivisionCode = '1', NULL, IFF(v_sar_insurance_line = 'N/A', NULL, IFF(v_ReinsuranceCoverageAKID_CommercialLines_loc IS NULL, LKP_REINSURANCE_COVERAGE_COMMERCIALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_insurance_line_sar_reinsurance_company_no_v_sar_section_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_cov_ak_id, v_ReinsuranceCoverageAKID_CommercialLines_loc))) AS v_ReinsuranceCoverageAKID_CommercialLines_defwd,
	-- *INF*: IIF (v_PolicyStrategicBusinessDivisionCode = '1', 
	--        NULL,
	--         IIF (v_sar_insurance_line = 'N/A',
	--            NULL,
	--                 IIF(ISNULL(v_ReinsuranceCoverageAKID_CommercialLines_defwd),
	-- :LKP.LKP_REINSURANCE_COVERAGE_COMMERCIALLINESPOLICIES_DEFAULT(pol_ak_id,v_sar_insurance_line,sar_reinsurance_company_no,v_sar_section,v_sar_faculta_comm_rate, v_dummy_start_date,v_dummy_end_date),
	--                  v_ReinsuranceCoverageAKID_CommercialLines_defwd)
	--                 )
	--           )
	-- 
	-- ---- When we are not getting a hit on commercial line policies using location and sublocation or only location with and without dates,  or even default key with dates we finally default to a lookup on polakid, insline, reinscompanyno, sectioncode without dates, The dummy date ports allow us to reuse the existing lookups with datebounds that satisfy the condition
	IFF(v_PolicyStrategicBusinessDivisionCode = '1', NULL, IFF(v_sar_insurance_line = 'N/A', NULL, IFF(v_ReinsuranceCoverageAKID_CommercialLines_defwd IS NULL, LKP_REINSURANCE_COVERAGE_COMMERCIALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_insurance_line_sar_reinsurance_company_no_v_sar_section_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_cov_ak_id, v_ReinsuranceCoverageAKID_CommercialLines_defwd))) AS v_ReinsuranceCoverageAKID_CommercialLines_def,
	-- *INF*: IIF(v_sar_insurance_line = 'N/A' or v_PolicyStrategicBusinessDivisionCode = '1', :LKP.LKP_REINSURANCECOVERAGE_PERSONALLINESPOLICIES(pol_ak_id, v_sar_unit_3pos,
	-- v_sar_section,sar_reinsurance_company_no,v_sar_faculta_comm_rate,v_sar_trans_eff_date, v_sar_exp_date),NULL)
	-- 
	-- ------- This provides a full key match with reinsurance risk unit, section code, reinsurance company number and date bounds along for PL and CL with InsuranceLine = 'N/A'
	-- 
	IFF(v_sar_insurance_line = 'N/A' OR v_PolicyStrategicBusinessDivisionCode = '1', LKP_REINSURANCECOVERAGE_PERSONALLINESPOLICIES_pol_ak_id_v_sar_unit_3pos_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_cov_ak_id, NULL) AS v_ReinsuranceCoverageAKID_PersonalLineswd,
	-- *INF*: IIF(v_sar_insurance_line = 'N/A' or v_PolicyStrategicBusinessDivisionCode = '1', 
	--  IIF(ISNULL(v_ReinsuranceCoverageAKID_PersonalLineswd),      
	--        :LKP.LKP_REINSURANCECOVERAGE_PERSONALLINESPOLICIES(pol_ak_id, v_sar_unit_3pos,
	-- v_sar_section,sar_reinsurance_company_no,v_sar_faculta_comm_rate,v_dummy_start_date, v_dummy_end_date),
	--        v_ReinsuranceCoverageAKID_PersonalLineswd), NULL
	--          )
	-- 
	-- --- ------- This provides a full key match with reinsurance risk unit, section code, reinsurance company number and no date date bounds along for PL and CL with InsuranceLine = 'N/A' if we don't get a hit with dates. The dummy date ports allow us to reuse the existing lookups with datebounds that satisfy the condition
	IFF(v_sar_insurance_line = 'N/A' OR v_PolicyStrategicBusinessDivisionCode = '1', IFF(v_ReinsuranceCoverageAKID_PersonalLineswd IS NULL, LKP_REINSURANCECOVERAGE_PERSONALLINESPOLICIES_pol_ak_id_v_sar_unit_3pos_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_cov_ak_id, v_ReinsuranceCoverageAKID_PersonalLineswd), NULL) AS v_ReinsuranceCoverageAKID_PersonalLines,
	-- *INF*: IIF(v_sar_insurance_line = 'N/A' or v_PolicyStrategicBusinessDivisionCode = '1', 
	--    IIF(ISNULL(v_ReinsuranceCoverageAKID_PersonalLines),        
	--      :LKP.LKP_REINSURANCECOVERAGE__PERSONALLINESPOLICIES_DEFAULT(pol_ak_id, 
	-- v_sar_section,sar_reinsurance_company_no,v_sar_faculta_comm_rate,v_sar_trans_eff_date, v_sar_exp_date),
	--       v_ReinsuranceCoverageAKID_PersonalLines), NULL
	--        )
	-- 
	-- ------------- This provides a default key match with section code, reinsurance company number and date bounds for PL and CL with InsuranceLine = 'N/A' if we don't get a hit with full key including risk unit with and without dates. The dummy date ports allow us to reuse the existing lookups with datebounds that satisfy the condition
	IFF(v_sar_insurance_line = 'N/A' OR v_PolicyStrategicBusinessDivisionCode = '1', IFF(v_ReinsuranceCoverageAKID_PersonalLines IS NULL, LKP_REINSURANCECOVERAGE__PERSONALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_cov_ak_id, v_ReinsuranceCoverageAKID_PersonalLines), NULL) AS v_ReinsuranceCoverageAKID_PersonalLines_defwd,
	-- *INF*: IIF(v_sar_insurance_line = 'N/A' or v_PolicyStrategicBusinessDivisionCode = '1', 
	--  IIF(ISNULL(v_ReinsuranceCoverageAKID_PersonalLines_defwd),      
	--        :LKP.LKP_REINSURANCECOVERAGE__PERSONALLINESPOLICIES_DEFAULT(pol_ak_id, 
	-- v_sar_section,sar_reinsurance_company_no,v_sar_faculta_comm_rate,v_dummy_start_date, v_dummy_end_date),
	--        v_ReinsuranceCoverageAKID_PersonalLines_defwd), NULL
	--          )
	-- 
	-- ------------- This provides a default key match with section code, reinsurance company number and no date bounds for PL and CL with InsuranceLine = 'N/A' if we don't get a hit with full key including risk unit with and without dates or default key with dates. The dummy date ports allow us to reuse the existing lookups with datebounds that satisfy the condition
	IFF(v_sar_insurance_line = 'N/A' OR v_PolicyStrategicBusinessDivisionCode = '1', IFF(v_ReinsuranceCoverageAKID_PersonalLines_defwd IS NULL, LKP_REINSURANCECOVERAGE__PERSONALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_cov_ak_id, v_ReinsuranceCoverageAKID_PersonalLines_defwd), NULL) AS v_ReinsuranceCoverageAKID_PersonalLines_def,
	-- *INF*: IIF(v_sar_insurance_line = 'N/A' or v_PolicyStrategicBusinessDivisionCode = '1' , v_ReinsuranceCoverageAKID_PersonalLines_def, v_ReinsuranceCoverageAKID_CommercialLines_def)
	-- 
	-- 
	-- ---- If the Strategic Business DivisionCode value is 1 then get the personalline lookup value otherwise commerciallines lookup value.
	IFF(v_sar_insurance_line = 'N/A' OR v_PolicyStrategicBusinessDivisionCode = '1', v_ReinsuranceCoverageAKID_PersonalLines_def, v_ReinsuranceCoverageAKID_CommercialLines_def) AS v_ReinsuranceCoverageAKID_PL_CL,
	-- *INF*: IIF(sar_part_code = '4',IIF(ISNULL(v_ReinsuranceCoverageAKID_PL_CL), -1, v_ReinsuranceCoverageAKID_PL_CL),-1)
	IFF(sar_part_code = '4', IFF(v_ReinsuranceCoverageAKID_PL_CL IS NULL, - 1, v_ReinsuranceCoverageAKID_PL_CL), - 1) AS v_ReinsuranceCoverageAKID,
	v_ReinsuranceCoverageAKID AS ReinsuranceCoverageAKID,
	EXP_Default.sar_rsn_amend_one,
	EXP_Default.sar_rsn_amend_two,
	EXP_Default.sar_rsn_amend_three,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_rsn_amend_one  ||  sar_rsn_amend_two || sar_rsn_amend_three)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_rsn_amend_one || sar_rsn_amend_two || sar_rsn_amend_three) AS v_sar_rsn_amend_code,
	v_sar_rsn_amend_code AS Reason_amend_code,
	-- *INF*: Policy_Key  ||  TO_CHAR(v_PolicyCoverageAKID)
	Policy_Key || TO_CHAR(v_PolicyCoverageAKID) AS v_CoverageKey,
	-- *INF*: Policy_Key  ||  TO_CHAR(v_PolicyCoverageAKID)
	Policy_Key || TO_CHAR(v_PolicyCoverageAKID) AS CoverageKey,
	-- *INF*: MD5(
	-- TO_CHAR(v_ReinsuranceCoverageAKID)  ||  
	-- TO_CHAR(v_CoverageDetailAKID)  ||  
	-- TO_CHAR(v_PMSFunctionCode)  ||  
	-- TO_CHAR(sar_transaction)  ||  
	-- TO_CHAR(v_sar_entrd_date)  ||  
	-- TO_CHAR(v_sar_trans_eff_date)  ||  
	-- TO_CHAR(v_sar_exp_date)  ||  
	-- TO_CHAR(v_sar_acct_entrd_date)  ||  
	-- TO_CHAR(sar_premium)  ||  
	-- TO_CHAR(sar_original_prem)  ||  
	-- TO_CHAR(v_PremiumType)  ||  
	-- TO_CHAR(v_sar_rsn_amend_code) 
	-- )
	MD5(TO_CHAR(v_ReinsuranceCoverageAKID) || TO_CHAR(v_CoverageDetailAKID) || TO_CHAR(v_PMSFunctionCode) || TO_CHAR(sar_transaction) || TO_CHAR(v_sar_entrd_date) || TO_CHAR(v_sar_trans_eff_date) || TO_CHAR(v_sar_exp_date) || TO_CHAR(v_sar_acct_entrd_date) || TO_CHAR(sar_premium) || TO_CHAR(sar_original_prem) || TO_CHAR(v_PremiumType) || TO_CHAR(v_sar_rsn_amend_code)) AS v_PremiumTransactionHashKey,
	v_PremiumTransactionHashKey AS PremiumTransactionHashKey,
	-- *INF*: IIF(Policy_Key = v_prev_row_Pol_Key, v_prev_row_Premium_Sequence + 1,1)
	IFF(Policy_Key = v_prev_row_Pol_Key, v_prev_row_Premium_Sequence + 1, 1) AS v_premium_sequence,
	v_premium_sequence AS PremiumLoadSequence,
	v_premium_sequence AS v_prev_row_Premium_Sequence,
	-- *INF*: IIF(v_prev_row_CoverageDetailAKID = v_CoverageDetailAKID AND 
	-- v_prev_row_ReinsuranceCoverageAKID = v_ReinsuranceCoverageAKID AND
	-- v_prev_row_PMSFunctionCode = v_PMSFunctionCode AND
	-- v_prev_row_sar_entrd_date = v_sar_entrd_date AND
	-- v_prev_row_sar_trans_eff_date = v_sar_trans_eff_date AND
	-- v_prev_row_sar_premium = sar_premium AND
	-- v_prev_row_sar_acct_entrd_date = v_sar_acct_entrd_date AND
	-- v_prev_row_sar_transaction = sar_transaction AND
	-- v_prev_row_sar_exp_date = v_sar_exp_date AND
	-- v_prev_row_sar_original_prem = sar_original_prem AND
	-- v_prev_row_PremiumType = v_PremiumType AND
	-- v_prev_row_sar_rsn_amend_code = v_sar_rsn_amend_code
	-- , v_DuplicateSequenceNum + 1,1)
	IFF(v_prev_row_CoverageDetailAKID = v_CoverageDetailAKID AND v_prev_row_ReinsuranceCoverageAKID = v_ReinsuranceCoverageAKID AND v_prev_row_PMSFunctionCode = v_PMSFunctionCode AND v_prev_row_sar_entrd_date = v_sar_entrd_date AND v_prev_row_sar_trans_eff_date = v_sar_trans_eff_date AND v_prev_row_sar_premium = sar_premium AND v_prev_row_sar_acct_entrd_date = v_sar_acct_entrd_date AND v_prev_row_sar_transaction = sar_transaction AND v_prev_row_sar_exp_date = v_sar_exp_date AND v_prev_row_sar_original_prem = sar_original_prem AND v_prev_row_PremiumType = v_PremiumType AND v_prev_row_sar_rsn_amend_code = v_sar_rsn_amend_code, v_DuplicateSequenceNum + 1, 1) AS v_DuplicateSequenceNum,
	v_DuplicateSequenceNum AS DuplicateSequenceNum,
	Policy_Key AS v_prev_row_Pol_Key,
	v_CoverageDetailAKID AS v_prev_row_CoverageDetailAKID,
	v_ReinsuranceCoverageAKID AS v_prev_row_ReinsuranceCoverageAKID,
	v_PMSFunctionCode AS v_prev_row_PMSFunctionCode,
	v_sar_entrd_date AS v_prev_row_sar_entrd_date,
	v_sar_trans_eff_date AS v_prev_row_sar_trans_eff_date,
	sar_premium AS v_prev_row_sar_premium,
	v_sar_acct_entrd_date AS v_prev_row_sar_acct_entrd_date,
	sar_transaction AS v_prev_row_sar_transaction,
	v_sar_exp_date AS v_prev_row_sar_exp_date,
	sar_original_prem AS v_prev_row_sar_original_prem,
	v_PremiumType AS v_prev_row_PremiumType,
	v_sar_rsn_amend_code AS v_prev_row_sar_rsn_amend_code,
	v_DuplicateSequenceNum AS v_prev_row_DuplicateSequenceNum,
	sar_location_x AS v_prev_row_sar_location_x,
	'' AS sar_zip_postal_code1,
	'' AS sar_insurance_line1,
	'' AS sar_type_bureau1,
	'' AS sar_sub_location_x1,
	'' AS sar_risk_unit_group1,
	'' AS sar_class_code_grp_x1,
	'' AS sar_unit1,
	'' AS sar_seq_rsk_unt_a1,
	'' AS sar_type_exposure1,
	'' AS sar_major_peril1,
	'' AS sar_seq_no1,
	sar_state AS v_prev_row_sar_state,
	sar_loc_prov_territory AS v_prev_row_sar_loc_prov_territory,
	sar_city AS v_prev_row_sar_city,
	EXP_Default.sar_annual_state_line,
	EXP_Default.sar_special_use,
	EXP_Default.sar_stat_breakdown_line,
	EXP_Default.sar_user_line,
	EXP_Default.sar_rating_date_ind,
	EXP_Default.sar_code_1,
	EXP_Default.sar_code_2,
	EXP_Default.sar_code_3,
	EXP_Default.sar_code_4,
	EXP_Default.sar_code_5,
	EXP_Default.sar_code_6,
	EXP_Default.sar_code_7,
	EXP_Default.sar_code_8,
	EXP_Default.sar_code_9,
	EXP_Default.sar_code_10,
	EXP_Default.sar_code_11,
	EXP_Default.sar_code_12,
	EXP_Default.sar_code_13,
	EXP_Default.sar_code_14,
	EXP_Default.sar_code_15,
	EXP_Default.sar_audit_reinst_ind,
	EXP_Default.logical_flag,
	EXP_Default.sar_company_number AS in_sar_company_number,
	LKP_Pif43NXCPStage.Pmdnxp1OtherMod AS in_Pmdnxp1OtherMod,
	LKP_Pif43NXCPStage.Pmdnxp1CspConstrCod AS in_Pmdnxp1CspConstrCod,
	LKP_Pif43NXCPStage.Pmdnxp1ProtectionClassPart1 AS in_Pmdnxp1ProtectionClassPart1,
	LKP_Pif43NXCPStage.Pmdnxp1YearBuilt AS in_Pmdnxp1YearBuilt,
	LKP_Pif43NXCPStage.Pmdnxp1Irpm AS in_Pmdnxp1Irpm,
	LKP_Pif43LXZWCStage.Pmdl4w1RatingProgramType AS in_Pmdl4w1RatingProgramType,
	LKP_Pif43LXZWCStage.Pmdl4w1PolicyType AS in_Pmdl4w1PolicyType,
	LKP_Pif43IXUnmodStage.Pif43IXUnmodWCRatingState AS in_Pif43IXUnmodWCRatingState,
	LKP_Pif43IXUnmodStage.Pif43IXUnmodReportingClassCode AS in_Pif43IXUnmodReportingClassCode,
	LKP_Pif43UYGLStage.Pmduyg1PkgModFactor AS in_Pmduyg1PkgModFactor,
	LKP_Pif43UYGLStage.Pmduyg1IncreaseLimitsFactor AS in_Pmduyg1IncreaseLimitsFactor,
	LKP_Pif43IXZWCModStage.Pmdi4w1YearItemEffective AS in_Pmdi4w1YearItemEffective,
	LKP_Pif43IXZWCModStage.Pmdi4w1MonthItemEffective AS in_Pmdi4w1MonthItemEffective,
	LKP_Pif43IXZWCModStage.Pmdi4w1DayItemEffective AS in_Pmdi4w1DayItemEffective,
	LKP_Pif43IXZWCModStage.Pmdi4w1ModifierDesc AS in_Pmdi4w1ModifierDesc,
	LKP_Pif43IXZWCModStage.Pmdi4w1ModifierRate AS in_Pmdi4w1ModifierRate,
	LKP_Pif43IXZWCModStage.Pmdi4w1ModifierPremBasis AS in_Pmdi4w1ModifierPremBasis,
	LKP_Pif43UXGLStage.Pmduxg1IncLimitTableInd AS in_Pmduxg1IncLimitTableInd,
	LKP_Pif43UXGLStage.Pmduxg1PmaCode AS in_Pmduxg1PmaCode,
	LKP_Pif43RXCPStage.Pmdrxp1PmaCode AS in_Pmdrxp1PmaCode,
	LKP_Pif43RXCPStage.wb_class_of_business AS in_wb_class_of_business,
	LKP_PIF43NXCRStage.PMDNXC1PmaCode AS in_PMDNXC1PmaCode,
	LKP_PIF43NXCRStage.wb_class_of_business AS in_wb_class_of_business_cr,
	EXP_Default.Out_PMACode AS in_PMACode,
	LKP_pif_02_stage.CLBCode AS Lkp_CLBCode,
	LKP_Pif43LXGLStage.Pmdlxg1YearRetro AS in_Pmdlxg1YearRetro,
	LKP_Pif43LXGLStage.Pmdlxg1MonthRetro AS in_Pmdlxg1MonthRetro,
	LKP_Pif43LXGLStage.Pmdlxg1DayRetro AS in_Pmdlxg1DayRetro,
	LKP_Pif43UXWCStage.Pmdu4w1Rate AS in_Pmdu4w1Rate,
	LKP_Pif43UZWCStage.Pmdu4w1Rate AS in_Pmdu4w1Rate_UZWC,
	LKP_PifPUHM17Stage.HRRConstruction AS in_HRRConstruction,
	LKP_pif_02_stage.pif_eff_yr_a AS in_pif_eff_yr_a,
	LKP_pif_02_stage.pif_eff_mo_a AS in_pif_eff_mo_a,
	LKP_pif_02_stage.pif_eff_da_a AS in_pif_eff_da_a,
	LKP_Pif350Stage.PackageModificationAssignment AS in_PackageModificationAssignment,
	LKP_Pif43RXIMStage.PMDRXI1Irpm AS in_PMDRXI1Irpm,
	LKP_Pif43RXGLStage.Pmdrxg1ScheduleMod AS in_Pmdrxg1ScheduleMod,
	LKP_Pif43LXGAStage.PMDLXA1PmaCode AS in_PMDLXA1PmaCode,
	-- *INF*: LTRIM(RTRIM(in_wb_class_of_business_cr))
	LTRIM(RTRIM(in_wb_class_of_business_cr)) AS v_wb_class_of_business_cr,
	-- *INF*: DECODE(TRUE,
	-- v_sar_type_bureau='FT',LTRIM(RTRIM(sar_code_4)),
	-- v_sar_type_bureau='BT',LTRIM(RTRIM(sar_code_7)),
	-- v_sar_type_bureau='CR',LTRIM(RTRIM(sar_code_11)),
	-- '00'
	-- )
	DECODE(TRUE,
	v_sar_type_bureau = 'FT', LTRIM(RTRIM(sar_code_4)),
	v_sar_type_bureau = 'BT', LTRIM(RTRIM(sar_code_7)),
	v_sar_type_bureau = 'CR', LTRIM(RTRIM(sar_code_11)),
	'00') AS v_OriginalPMACode_CR,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_PMACode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_PMACode) AS v_PMACode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_PackageModificationAssignment)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_PackageModificationAssignment) AS v_PackageModificationAssignment,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_Pmduxg1PmaCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_Pmduxg1PmaCode) AS v_Pmduxg1PmaCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_Pmdrxp1PmaCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_Pmdrxp1PmaCode) AS v_Pmdrxp1PmaCode,
	-- *INF*: IIF(ISNULL(in_wb_class_of_business) OR IS_SPACES(in_wb_class_of_business) OR LENGTH(in_wb_class_of_business)=0, '00', LTRIM(RTRIM(in_wb_class_of_business)))
	IFF(in_wb_class_of_business IS NULL OR IS_SPACES(in_wb_class_of_business) OR LENGTH(in_wb_class_of_business) = 0, '00', LTRIM(RTRIM(in_wb_class_of_business))) AS v_wb_class_of_business,
	-- *INF*: IIF(NOT ISNULL(in_Pmdnxp1OtherMod), in_Pmdnxp1OtherMod, 0)
	IFF(NOT in_Pmdnxp1OtherMod IS NULL, in_Pmdnxp1OtherMod, 0) AS v_Pmdnxp1OtherMod,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_PMDNXC1PmaCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_PMDNXC1PmaCode) AS v_PMDNXC1PmaCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_PMDLXA1PmaCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_PMDLXA1PmaCode) AS v_PMDLXA1PmaCode,
	-- *INF*: DECODE(v_PMACode,
	-- 'N/A', 'N/A' ,
	-- '10','N/A',
	-- '31','MH',
	-- '32','A',
	-- '33','O',
	-- '34', 'M',
	-- '35', IIF(Lkp_CLBCode ='AL' ,'AL', 'I'),
	-- '36','N/A', 
	-- '37', IIF(Lkp_CLBCode = 'I'  , 'MW',IIF(Lkp_CLBCode ='O' , 'WW' ,'IP')) ,
	-- '38', IIF(Lkp_CLBCode = 'AG' , 'AG' , IIF(Lkp_CLBCode = 'AM', 'AM', 'C')),
	-- v_PMACode)
	-- 
	DECODE(v_PMACode,
	'N/A', 'N/A',
	'10', 'N/A',
	'31', 'MH',
	'32', 'A',
	'33', 'O',
	'34', 'M',
	'35', IFF(Lkp_CLBCode = 'AL', 'AL', 'I'),
	'36', 'N/A',
	'37', IFF(Lkp_CLBCode = 'I', 'MW', IFF(Lkp_CLBCode = 'O', 'WW', 'IP')),
	'38', IFF(Lkp_CLBCode = 'AG', 'AG', IFF(Lkp_CLBCode = 'AM', 'AM', 'C')),
	v_PMACode) AS v_PMACODE_IM,
	-- *INF*: DECODE(TRUE, 
	-- v_OriginalPMACode_CR = '','N/A',
	-- v_OriginalPMACode_CR='10','N/A',
	-- v_OriginalPMACode_CR='31','MH',
	-- v_OriginalPMACode_CR='32','A',
	-- v_OriginalPMACode_CR='33','O',
	-- v_OriginalPMACode_CR='34','M',
	-- v_OriginalPMACode_CR='35' and v_wb_class_of_business_cr = 'AL','AL',
	-- v_OriginalPMACode_CR='35','I',
	-- v_OriginalPMACode_CR='36', v_PMDNXC1PmaCode,
	-- v_OriginalPMACode_CR='37' and v_wb_class_of_business_cr = 'I','MW',
	-- v_OriginalPMACode_CR='37' and v_wb_class_of_business_cr = 'O','WW',
	-- v_OriginalPMACode_CR='37','IP',
	-- v_OriginalPMACode_CR='38' and v_wb_class_of_business_cr = 'AG','AG',
	-- v_OriginalPMACode_CR='38' and v_wb_class_of_business_cr = 'AM','AM',
	-- v_OriginalPMACode_CR='38','C',
	-- 'N/A'
	-- )
	DECODE(TRUE,
	v_OriginalPMACode_CR = '', 'N/A',
	v_OriginalPMACode_CR = '10', 'N/A',
	v_OriginalPMACode_CR = '31', 'MH',
	v_OriginalPMACode_CR = '32', 'A',
	v_OriginalPMACode_CR = '33', 'O',
	v_OriginalPMACode_CR = '34', 'M',
	v_OriginalPMACode_CR = '35' AND v_wb_class_of_business_cr = 'AL', 'AL',
	v_OriginalPMACode_CR = '35', 'I',
	v_OriginalPMACode_CR = '36', v_PMDNXC1PmaCode,
	v_OriginalPMACode_CR = '37' AND v_wb_class_of_business_cr = 'I', 'MW',
	v_OriginalPMACode_CR = '37' AND v_wb_class_of_business_cr = 'O', 'WW',
	v_OriginalPMACode_CR = '37', 'IP',
	v_OriginalPMACode_CR = '38' AND v_wb_class_of_business_cr = 'AG', 'AG',
	v_OriginalPMACode_CR = '38' AND v_wb_class_of_business_cr = 'AM', 'AM',
	v_OriginalPMACode_CR = '38', 'C',
	'N/A') AS v_PMACODE_CR,
	-- *INF*: IIF(v_sar_insurance_line='CF', v_Pmdnxp1OtherMod, 0)
	IFF(v_sar_insurance_line = 'CF', v_Pmdnxp1OtherMod, 0) AS v_PreferredPropertyFactor,
	-- *INF*: IIF(ISNULL(sar_code_1) OR IS_SPACES(sar_code_1) OR LENGTH(sar_code_1)=0, '0', sar_code_1)
	IFF(sar_code_1 IS NULL OR IS_SPACES(sar_code_1) OR LENGTH(sar_code_1) = 0, '0', sar_code_1) AS v_sar_code_1,
	-- *INF*: IIF(ISNULL(sar_code_2) OR IS_SPACES(sar_code_2) OR LENGTH(sar_code_2)=0, '0', sar_code_2)
	IFF(sar_code_2 IS NULL OR IS_SPACES(sar_code_2) OR LENGTH(sar_code_2) = 0, '0', sar_code_2) AS v_sar_code_2,
	-- *INF*: IIF(ISNULL(sar_code_3) OR IS_SPACES(sar_code_3) OR LENGTH(sar_code_3)=0, '0', sar_code_3)
	IFF(sar_code_3 IS NULL OR IS_SPACES(sar_code_3) OR LENGTH(sar_code_3) = 0, '0', sar_code_3) AS v_sar_code_3,
	-- *INF*: IIF(ISNULL(sar_code_4) OR IS_SPACES(sar_code_4) OR LENGTH(sar_code_4)=0, '00', LTRIM(RTRIM(sar_code_4)))
	IFF(sar_code_4 IS NULL OR IS_SPACES(sar_code_4) OR LENGTH(sar_code_4) = 0, '00', LTRIM(RTRIM(sar_code_4))) AS v_sar_code_4,
	-- *INF*: IIF(ISNULL(sar_code_5) OR IS_SPACES(sar_code_5) OR LENGTH(sar_code_5)=0, '00', LTRIM(RTRIM(sar_code_5)))
	IFF(sar_code_5 IS NULL OR IS_SPACES(sar_code_5) OR LENGTH(sar_code_5) = 0, '00', LTRIM(RTRIM(sar_code_5))) AS v_sar_code_5,
	-- *INF*: IIF(ISNULL(sar_code_6) OR IS_SPACES(sar_code_6) OR LENGTH(sar_code_6)=0, '000', LTRIM(RTRIM(sar_code_6)))
	IFF(sar_code_6 IS NULL OR IS_SPACES(sar_code_6) OR LENGTH(sar_code_6) = 0, '000', LTRIM(RTRIM(sar_code_6))) AS v_sar_code_6,
	-- *INF*: IIF(ISNULL(sar_code_7) OR IS_SPACES(sar_code_7) OR LENGTH(sar_code_7)=0, '000', LTRIM(RTRIM(sar_code_7)))
	IFF(sar_code_7 IS NULL OR IS_SPACES(sar_code_7) OR LENGTH(sar_code_7) = 0, '000', LTRIM(RTRIM(sar_code_7))) AS v_sar_code_7,
	-- *INF*: DECODE(TRUE,
	-- v_sar_code_6 = '32' , 'A',
	-- v_sar_code_6 = '38' and  IN (v_wb_class_of_business, 'AG', 'AM')=0, 'C',
	-- v_sar_code_6 = '35' and IN(v_wb_class_of_business, 'AL')=0, 'I',
	-- v_sar_code_6 = '37' and IN(v_wb_class_of_business,  'I', 'O')=0, 'IP',
	-- v_sar_code_6 = '34' , 'M',
	-- v_sar_code_6 = '31' , 'MH',
	-- v_sar_code_6 = '33',  'O',
	-- v_sar_code_6 = '36' and v_Pmdrxp1PmaCode= 'S' , 'S',
	-- v_sar_code_6 = '36' and v_Pmdrxp1PmaCode= 'TA', 'TA',
	-- v_sar_code_6 = '36' and v_Pmdrxp1PmaCode= 'BA', 'BA',
	-- v_sar_code_6 = '36' and v_Pmdrxp1PmaCode= 'AD', 'AD',
	-- v_sar_code_6 = '36' and v_Pmdrxp1PmaCode= 'TD','TD',
	-- v_sar_code_6 = '36' and v_Pmdrxp1PmaCode= 'BD', 'BD',
	-- v_sar_code_6 = '36' and v_Pmdrxp1PmaCode= 'N' , 'N',
	-- v_sar_code_6 = '36' and v_Pmdrxp1PmaCode= 'TN', 'TN',
	-- v_sar_code_6 = '36' and v_Pmdrxp1PmaCode= 'BN' , 'BN',
	-- v_sar_code_6 = '38' and v_wb_class_of_business = 'AG', 'AG',
	-- v_sar_code_6 = '38' and v_wb_class_of_business = 'AM','AM',
	-- v_sar_code_6 = '37' and v_wb_class_of_business = 'I' , 'MW',
	-- v_sar_code_6 = '37' and v_wb_class_of_business = 'O','WW',
	-- (v_sar_code_6 = '35' and v_wb_class_of_business = 'AL') OR (v_sar_code_6 = '45'), 'AL',
	-- (v_sar_code_6 = '36' and v_Pmdrxp1PmaCode= 'HC') OR (v_sar_code_6 = '46'), 'HC',
	-- v_sar_code_6 = '10' or v_sar_code_6='000',  'N/A',
	-- v_Pmdrxp1PmaCode)
	DECODE(TRUE,
	v_sar_code_6 = '32', 'A',
	v_sar_code_6 = '38' AND IN(v_wb_class_of_business, 'AG', 'AM') = 0, 'C',
	v_sar_code_6 = '35' AND IN(v_wb_class_of_business, 'AL') = 0, 'I',
	v_sar_code_6 = '37' AND IN(v_wb_class_of_business, 'I', 'O') = 0, 'IP',
	v_sar_code_6 = '34', 'M',
	v_sar_code_6 = '31', 'MH',
	v_sar_code_6 = '33', 'O',
	v_sar_code_6 = '36' AND v_Pmdrxp1PmaCode = 'S', 'S',
	v_sar_code_6 = '36' AND v_Pmdrxp1PmaCode = 'TA', 'TA',
	v_sar_code_6 = '36' AND v_Pmdrxp1PmaCode = 'BA', 'BA',
	v_sar_code_6 = '36' AND v_Pmdrxp1PmaCode = 'AD', 'AD',
	v_sar_code_6 = '36' AND v_Pmdrxp1PmaCode = 'TD', 'TD',
	v_sar_code_6 = '36' AND v_Pmdrxp1PmaCode = 'BD', 'BD',
	v_sar_code_6 = '36' AND v_Pmdrxp1PmaCode = 'N', 'N',
	v_sar_code_6 = '36' AND v_Pmdrxp1PmaCode = 'TN', 'TN',
	v_sar_code_6 = '36' AND v_Pmdrxp1PmaCode = 'BN', 'BN',
	v_sar_code_6 = '38' AND v_wb_class_of_business = 'AG', 'AG',
	v_sar_code_6 = '38' AND v_wb_class_of_business = 'AM', 'AM',
	v_sar_code_6 = '37' AND v_wb_class_of_business = 'I', 'MW',
	v_sar_code_6 = '37' AND v_wb_class_of_business = 'O', 'WW',
	( v_sar_code_6 = '35' AND v_wb_class_of_business = 'AL' ) OR ( v_sar_code_6 = '45' ), 'AL',
	( v_sar_code_6 = '36' AND v_Pmdrxp1PmaCode = 'HC' ) OR ( v_sar_code_6 = '46' ), 'HC',
	v_sar_code_6 = '10' OR v_sar_code_6 = '000', 'N/A',
	v_Pmdrxp1PmaCode) AS v_Pmdrxp1PmaCode_Final,
	-- *INF*: IIF(ISNULL(in_sar_company_number), 0, in_sar_company_number)
	IFF(in_sar_company_number IS NULL, 0, in_sar_company_number) AS v_sar_company_number,
	-- *INF*: to_char(in_Pmdlxg1YearRetro) || LPAD(to_char(in_Pmdlxg1MonthRetro), 2, '0')  || LPAD(to_char(in_Pmdlxg1DayRetro), 2, '0')
	to_char(in_Pmdlxg1YearRetro) || LPAD(to_char(in_Pmdlxg1MonthRetro), 2, '0') || LPAD(to_char(in_Pmdlxg1DayRetro), 2, '0') AS v_RetroactiveDate,
	-- *INF*: IIF(ISNULL(in_Pif43IXUnmodWCRatingState) OR IS_SPACES(in_Pif43IXUnmodWCRatingState) OR LENGTH(in_Pif43IXUnmodWCRatingState)=0, '00', LTRIM(RTRIM(in_Pif43IXUnmodWCRatingState)))
	IFF(in_Pif43IXUnmodWCRatingState IS NULL OR IS_SPACES(in_Pif43IXUnmodWCRatingState) OR LENGTH(in_Pif43IXUnmodWCRatingState) = 0, '00', LTRIM(RTRIM(in_Pif43IXUnmodWCRatingState))) AS v_Pif43IXUnmodWCRatingState,
	-- *INF*: IIF(ISNULL(in_Pif43IXUnmodReportingClassCode) OR IS_SPACES(in_Pif43IXUnmodReportingClassCode) OR LENGTH(in_Pif43IXUnmodReportingClassCode)=0, '0000', LTRIM(RTRIM(in_Pif43IXUnmodReportingClassCode)))
	IFF(in_Pif43IXUnmodReportingClassCode IS NULL OR IS_SPACES(in_Pif43IXUnmodReportingClassCode) OR LENGTH(in_Pif43IXUnmodReportingClassCode) = 0, '0000', LTRIM(RTRIM(in_Pif43IXUnmodReportingClassCode))) AS v_Pif43IXUnmodReportingClassCode,
	-- *INF*: IIF(ISNULL(in_Pmdi4w1ModifierRate), 0, in_Pmdi4w1ModifierRate)
	IFF(in_Pmdi4w1ModifierRate IS NULL, 0, in_Pmdi4w1ModifierRate) AS v_Pmdi4w1ModifierRate,
	-- *INF*: IIF(ISNULL(in_Pmdnxp1YearBuilt), '0000', TO_CHAR(in_Pmdnxp1YearBuilt))
	IFF(in_Pmdnxp1YearBuilt IS NULL, '0000', TO_CHAR(in_Pmdnxp1YearBuilt)) AS v_Pmdnxp1YearBuilt,
	-- *INF*: TO_CHAR(in_Pmdi4w1YearItemEffective) || LPAD(TO_CHAR(LTRIM(RTRIM(in_Pmdi4w1MonthItemEffective))), 2, '0') || LPAD(TO_CHAR(LTRIM(RTRIM(in_Pmdi4w1DayItemEffective))), 2, '0')
	TO_CHAR(in_Pmdi4w1YearItemEffective) || LPAD(TO_CHAR(LTRIM(RTRIM(in_Pmdi4w1MonthItemEffective))), 2, '0') || LPAD(TO_CHAR(LTRIM(RTRIM(in_Pmdi4w1DayItemEffective))), 2, '0') AS v_ExperienceModificationEffectiveDate,
	-- *INF*: IIF(ISNULL(in_Pmdnxp1CspConstrCod), 'N/A', TO_CHAR(in_Pmdnxp1CspConstrCod))
	IFF(in_Pmdnxp1CspConstrCod IS NULL, 'N/A', TO_CHAR(in_Pmdnxp1CspConstrCod)) AS v_Pmdnxp1CspConstrCod,
	-- *INF*: IIF(ISNULL(in_HRRConstruction) OR IS_SPACES(in_HRRConstruction) OR LENGTH(in_HRRConstruction)=0, 'N/A', LTRIM(RTRIM(in_HRRConstruction)))
	IFF(in_HRRConstruction IS NULL OR IS_SPACES(in_HRRConstruction) OR LENGTH(in_HRRConstruction) = 0, 'N/A', LTRIM(RTRIM(in_HRRConstruction))) AS v_HRRConstruction,
	in_pif_eff_yr_a||in_pif_eff_mo_a||in_pif_eff_da_a AS v_StateRatingEffectiveDate,
	-- *INF*: IIF(ISNULL(in_Pmdi4w1ModifierPremBasis),0,in_Pmdi4w1ModifierPremBasis)
	IFF(in_Pmdi4w1ModifierPremBasis IS NULL, 0, in_Pmdi4w1ModifierPremBasis) AS v_Pmdi4w1ModifierPremBasis,
	-- *INF*: LTRIM(RTRIM(:LKP.LKP_PIF11STAGE(Policy_Key)))
	LTRIM(RTRIM(LKP_PIF11STAGE_Policy_Key.DocumentText)) AS v_DocumentText,
	-- *INF*: LTRIM(RTRIM(REPLACESTR(TRUE,DECODE(TRUE,
	-- LENGTH(v_DocumentText)>14,SUBSTR(v_DocumentText,8,LENGTH(v_DocumentText)-14),
	-- LENGTH(v_DocumentText)<=14,SUBSTR(v_DocumentText,8),
	-- '0'),',','')))
	LTRIM(RTRIM(REPLACESTR(TRUE, DECODE(TRUE,
	LENGTH(v_DocumentText) > 14, SUBSTR(v_DocumentText, 8, LENGTH(v_DocumentText) - 14),
	LENGTH(v_DocumentText) <= 14, SUBSTR(v_DocumentText, 8),
	'0'), ',', ''))) AS v_DocumentText_Trim,
	EXP_Default.pif_4514_stage_id,
	LKP_Pif43RXCPStage.Pmdrxp1WindAndHail,
	-- *INF*: DECODE(TRUE,
	-- v_sar_insurance_line='IM',
	-- DECODE(v_sar_code_7, '02', '50', '03', '100', '04', '250', '05', '500', '06', '750', '07', '1000', '08', '2500', '09', '5000', '10', '10000', '11', '25000', '90', 'over 25000', '0'),
	-- v_sar_insurance_line='CF',
	-- DECODE(v_sar_code_5, '01', 'FullCoverage', '03', '100', '04', '250', '05', '500', '07', '1000', '08', '2500', '09', '5000', '10', '10000', '11', '25000', '12', '50000', '13', '75000', '0'),
	-- v_sar_insurance_line='GL',
	-- DECODE(v_sar_code_4 || v_sar_code_6, '0101', 'FullCoverage', '0'),
	-- v_sar_insurance_line= 'CR' and v_sar_company_number=20,
	-- DECODE(v_sar_code_7, '0', 'Blank', '100', '100', '0'),
	-- v_sar_insurance_line= 'CR' and v_sar_company_number != 20,
	-- DECODE(v_sar_code_7, '04', '250', '05', '500', '07', '1000', '08', '2500', '09', '5000', '10', '10000', '11', '25000', '0'),
	-- v_sar_insurance_line = 'WC' and v_Pif43IXUnmodWCRatingState = '12' and v_Pif43IXUnmodReportingClassCode = '9931','1000',
	-- v_sar_insurance_line = 'WC' and v_Pif43IXUnmodWCRatingState = '13',DECODE(v_Pif43IXUnmodReportingClassCode,'9940','500','9941','1000','9942','1500','9943','2000','9944','2500','9900','3000','9904','3500','9905','4000','9906','4500','9945','5000','9915','500','9916','1000','9917','1500','9918','2000','9919','2500','0'),
	-- v_sar_insurance_line = 'WC' and v_Pif43IXUnmodWCRatingState = '15',DECODE(v_Pif43IXUnmodReportingClassCode,'9940','500','9941','1000','9942','1500','9943','2000','9944','2500','9945','5000','0'),
	-- v_sar_insurance_line = 'WC' and v_Pif43IXUnmodWCRatingState = '21' and v_Pif43IXUnmodReportingClassCode = '9664',DECODE(v_Pmdi4w1ModifierPremBasis,500,'500',1000,'1000',1500,'1500',2000,'2000',2500,'2500','0'),
	-- v_sar_insurance_line = 'WC' and v_Pif43IXUnmodWCRatingState = '22',DECODE(v_Pif43IXUnmodReportingClassCode,'9940','500','9941','1000','0'),
	-- v_sar_insurance_line = 'WC' and v_Pif43IXUnmodWCRatingState = '24',DECODE(v_Pif43IXUnmodReportingClassCode,'9940','500','9941','1000','9942','1500','9943','2000','9944','2500','9945','5000','0'),
	-- v_sar_insurance_line = 'WC' and v_Pif43IXUnmodWCRatingState = '26' and v_Pif43IXUnmodReportingClassCode = '9664',DECODE(v_Pmdi4w1ModifierPremBasis,500,'500',1000,'1000',1500,'1500',2000,'2000',2500,'2500','0'),
	-- v_sar_insurance_line = 'WC' and v_Pif43IXUnmodWCRatingState = '14' and IN(v_DocumentText_Trim,'100','150','200','250','300','350','400','450','500','550','600','650','700','750','800','850','900','950','1000','1050','2000','2050','3000','3050','4000','4050','5000','5050','6000','6050','7000','7050','8000','8050','9000','9050','10000'),v_DocumentText_Trim,
	-- --EDWP 4517
	-- v_sar_type_bureau= 'PI',
	-- DECODE(v_sar_code_5,'01','0','03','50','10','100','25','250','50','500','82','1000','99','10000','0'),
	-- '0')
	DECODE(TRUE,
	v_sar_insurance_line = 'IM', DECODE(v_sar_code_7,
	'02', '50',
	'03', '100',
	'04', '250',
	'05', '500',
	'06', '750',
	'07', '1000',
	'08', '2500',
	'09', '5000',
	'10', '10000',
	'11', '25000',
	'90', 'over 25000',
	'0'),
	v_sar_insurance_line = 'CF', DECODE(v_sar_code_5,
	'01', 'FullCoverage',
	'03', '100',
	'04', '250',
	'05', '500',
	'07', '1000',
	'08', '2500',
	'09', '5000',
	'10', '10000',
	'11', '25000',
	'12', '50000',
	'13', '75000',
	'0'),
	v_sar_insurance_line = 'GL', DECODE(v_sar_code_4 || v_sar_code_6,
	'0101', 'FullCoverage',
	'0'),
	v_sar_insurance_line = 'CR' AND v_sar_company_number = 20, DECODE(v_sar_code_7,
	'0', 'Blank',
	'100', '100',
	'0'),
	v_sar_insurance_line = 'CR' AND v_sar_company_number != 20, DECODE(v_sar_code_7,
	'04', '250',
	'05', '500',
	'07', '1000',
	'08', '2500',
	'09', '5000',
	'10', '10000',
	'11', '25000',
	'0'),
	v_sar_insurance_line = 'WC' AND v_Pif43IXUnmodWCRatingState = '12' AND v_Pif43IXUnmodReportingClassCode = '9931', '1000',
	v_sar_insurance_line = 'WC' AND v_Pif43IXUnmodWCRatingState = '13', DECODE(v_Pif43IXUnmodReportingClassCode,
	'9940', '500',
	'9941', '1000',
	'9942', '1500',
	'9943', '2000',
	'9944', '2500',
	'9900', '3000',
	'9904', '3500',
	'9905', '4000',
	'9906', '4500',
	'9945', '5000',
	'9915', '500',
	'9916', '1000',
	'9917', '1500',
	'9918', '2000',
	'9919', '2500',
	'0'),
	v_sar_insurance_line = 'WC' AND v_Pif43IXUnmodWCRatingState = '15', DECODE(v_Pif43IXUnmodReportingClassCode,
	'9940', '500',
	'9941', '1000',
	'9942', '1500',
	'9943', '2000',
	'9944', '2500',
	'9945', '5000',
	'0'),
	v_sar_insurance_line = 'WC' AND v_Pif43IXUnmodWCRatingState = '21' AND v_Pif43IXUnmodReportingClassCode = '9664', DECODE(v_Pmdi4w1ModifierPremBasis,
	500, '500',
	1000, '1000',
	1500, '1500',
	2000, '2000',
	2500, '2500',
	'0'),
	v_sar_insurance_line = 'WC' AND v_Pif43IXUnmodWCRatingState = '22', DECODE(v_Pif43IXUnmodReportingClassCode,
	'9940', '500',
	'9941', '1000',
	'0'),
	v_sar_insurance_line = 'WC' AND v_Pif43IXUnmodWCRatingState = '24', DECODE(v_Pif43IXUnmodReportingClassCode,
	'9940', '500',
	'9941', '1000',
	'9942', '1500',
	'9943', '2000',
	'9944', '2500',
	'9945', '5000',
	'0'),
	v_sar_insurance_line = 'WC' AND v_Pif43IXUnmodWCRatingState = '26' AND v_Pif43IXUnmodReportingClassCode = '9664', DECODE(v_Pmdi4w1ModifierPremBasis,
	500, '500',
	1000, '1000',
	1500, '1500',
	2000, '2000',
	2500, '2500',
	'0'),
	v_sar_insurance_line = 'WC' AND v_Pif43IXUnmodWCRatingState = '14' AND IN(v_DocumentText_Trim, '100', '150', '200', '250', '300', '350', '400', '450', '500', '550', '600', '650', '700', '750', '800', '850', '900', '950', '1000', '1050', '2000', '2050', '3000', '3050', '4000', '4050', '5000', '5050', '6000', '6050', '7000', '7050', '8000', '8050', '9000', '9050', '10000'), v_DocumentText_Trim,
	v_sar_type_bureau = 'PI', DECODE(v_sar_code_5,
	'01', '0',
	'03', '50',
	'10', '100',
	'25', '250',
	'50', '500',
	'82', '1000',
	'99', '10000',
	'0'),
	'0') AS out_DeductibleAmount,
	-- *INF*: IIF(IS_DATE(v_RetroactiveDate, 'YYYYMMDD'), TO_DATE(v_RetroactiveDate, 'YYYYMMDD'), TO_DATE('2100-12-31', 'YYYY-MM-DD'))
	IFF(IS_DATE(v_RetroactiveDate, 'YYYYMMDD'), TO_DATE(v_RetroactiveDate, 'YYYYMMDD'), TO_DATE('2100-12-31', 'YYYY-MM-DD')) AS out_RetroactiveDate,
	-- *INF*: IIF(LTRIM(RTRIM(in_Pmdi4w1ModifierDesc))='EXPER MOD', v_Pmdi4w1ModifierRate,0)
	IFF(LTRIM(RTRIM(in_Pmdi4w1ModifierDesc)) = 'EXPER MOD', v_Pmdi4w1ModifierRate, 0) AS out_ExperienceModificationFactor,
	-- *INF*: IIF(IS_DATE(v_ExperienceModificationEffectiveDate, 'YYYYMMDD') AND LTRIM(RTRIM(in_Pmdi4w1ModifierDesc))='EXPER MOD', TO_DATE(v_ExperienceModificationEffectiveDate, 'YYYYMMDD'), TO_DATE('2100-12-31', 'YYYY-MM-DD'))
	IFF(IS_DATE(v_ExperienceModificationEffectiveDate, 'YYYYMMDD') AND LTRIM(RTRIM(in_Pmdi4w1ModifierDesc)) = 'EXPER MOD', TO_DATE(v_ExperienceModificationEffectiveDate, 'YYYYMMDD'), TO_DATE('2100-12-31', 'YYYY-MM-DD')) AS out_ExperienceModificationEffectiveDate,
	-- *INF*: IIF(ISNULL(in_Pmduyg1PkgModFactor), 0, in_Pmduyg1PkgModFactor)
	IFF(in_Pmduyg1PkgModFactor IS NULL, 0, in_Pmduyg1PkgModFactor) AS out_PackageModificationAdjustmentFactor,
	-- *INF*: DECODE(TRUE,
	-- v_sar_insurance_line= 'GL', v_Pmduxg1PmaCode, 
	-- v_sar_insurance_line='CF', v_Pmdrxp1PmaCode_Final, 
	-- v_sar_insurance_line='CR',v_PMACODE_CR, 
	-- v_sar_insurance_line='IM',v_PMACODE_IM,
	-- v_sar_insurance_line='GA',v_PMDLXA1PmaCode,
	-- IN(v_sar_type_bureau,'AL','AP','AN'),v_PackageModificationAssignment,
	-- 'N/A')
	DECODE(TRUE,
	v_sar_insurance_line = 'GL', v_Pmduxg1PmaCode,
	v_sar_insurance_line = 'CF', v_Pmdrxp1PmaCode_Final,
	v_sar_insurance_line = 'CR', v_PMACODE_CR,
	v_sar_insurance_line = 'IM', v_PMACODE_IM,
	v_sar_insurance_line = 'GA', v_PMDLXA1PmaCode,
	IN(v_sar_type_bureau, 'AL', 'AP', 'AN'), v_PackageModificationAssignment,
	'N/A') AS out_PackageModificationAdjustmentGroupCode,
	-- *INF*: IIF(ISNULL(in_Pmduyg1IncreaseLimitsFactor), 0, in_Pmduyg1IncreaseLimitsFactor)
	IFF(in_Pmduyg1IncreaseLimitsFactor IS NULL, 0, in_Pmduyg1IncreaseLimitsFactor) AS out_IncreasedLimitFactor,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_Pmduxg1IncLimitTableInd)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_Pmduxg1IncLimitTableInd) AS out_IncreasedLimitGroupCode,
	-- *INF*: IIF(v_sar_insurance_line='CF', v_Pmdnxp1YearBuilt, '0000')
	IFF(v_sar_insurance_line = 'CF', v_Pmdnxp1YearBuilt, '0000') AS out_YearBuilt,
	v_sar_agents_comm_rate AS out_AgencyActualCommissionRate,
	-- *INF*: DECODE(TRUE,NOT ISNULL(in_Pmdu4w1Rate), in_Pmdu4w1Rate,
	--  NOT ISNULL(in_Pmdu4w1Rate_UZWC),in_Pmdu4w1Rate_UZWC,0)
	DECODE(TRUE,
	NOT in_Pmdu4w1Rate IS NULL, in_Pmdu4w1Rate,
	NOT in_Pmdu4w1Rate_UZWC IS NULL, in_Pmdu4w1Rate_UZWC,
	0) AS out_BaseRate,
	-- *INF*: DECODE(TRUE,v_sar_insurance_line='CF',v_Pmdnxp1CspConstrCod,SUBSTR(Policy_Key,1,2)='HH', v_HRRConstruction,'N/A')
	DECODE(TRUE,
	v_sar_insurance_line = 'CF', v_Pmdnxp1CspConstrCod,
	SUBSTR(Policy_Key, 1, 2) = 'HH', v_HRRConstruction,
	'N/A') AS out_ConstructionCode,
	-- *INF*: IIF(IS_DATE(v_StateRatingEffectiveDate, 'YYYYMMDD'), TO_DATE(v_StateRatingEffectiveDate, 'YYYYMMDD'),
	-- TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'))
	-- 
	-- 
	-- 
	-- --TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
	IFF(IS_DATE(v_StateRatingEffectiveDate, 'YYYYMMDD'), TO_DATE(v_StateRatingEffectiveDate, 'YYYYMMDD'), TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) AS out_StateRatingEffectiveDate,
	-- *INF*: IIF(LTRIM(RTRIM(in_Pmdl4w1RatingProgramType))='R', 1, 0)
	IFF(LTRIM(RTRIM(in_Pmdl4w1RatingProgramType)) = 'R', 1, 0) AS out_WCRetrospectiveRatingIndicator,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_Pmdl4w1PolicyType)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_Pmdl4w1PolicyType) AS out_WCPolicyRatingType,
	-- *INF*: DECODE(LTRIM(RTRIM(in_Pmdl4w1PolicyType)), 'S', '01', 'A', '02', '01')
	DECODE(LTRIM(RTRIM(in_Pmdl4w1PolicyType)),
	'S', '01',
	'A', '02',
	'01') AS out_WCPolicyPlanCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_Pmdnxp1ProtectionClassPart1)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_Pmdnxp1ProtectionClassPart1) AS out_IsoFireProtectionCode,
	v_PreferredPropertyFactor AS out_MultiLocationCreditFactor,
	v_PreferredPropertyFactor AS out_PreferredPropertyFactor,
	-- *INF*: DECODE(sar_insurance_line,'CP',in_Pmdnxp1Irpm,'IM',in_PMDRXI1Irpm,'GL',in_Pmdrxg1ScheduleMod,0)
	DECODE(sar_insurance_line,
	'CP', in_Pmdnxp1Irpm,
	'IM', in_PMDRXI1Irpm,
	'GL', in_Pmdrxg1ScheduleMod,
	0) AS out_IndividualRiskPremiumModification,
	-- *INF*: DECODE(Pmdrxp1WindAndHail, 'Y', 0, 1)
	DECODE(Pmdrxp1WindAndHail,
	'Y', 0,
	1) AS out_WindCoverageIndicator,
	-- *INF*: :LKP.LKP_SupClassificationWorkersCompensation(v_sar_class_1_4,sar_state)
	LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_v_sar_class_1_4_sar_state.SupClassificationWorkersCompensationId AS SupClassificationWorkersCompensationId,
	-- *INF*: :LKP.LKP_SupClassificationWorkersCompensation(v_sar_class_1_4,'99')
	LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_v_sar_class_1_4_99.SupClassificationWorkersCompensationId AS SupClassificationWorkersCompensationId99,
	-- *INF*: DECODE(v_sar_type_bureau,
	-- 'AL','0',
	-- 'AN','D',
	-- 'AP',
	-- IIF(ISNULL(:LKP.LKP_SUPDEDUCTIBLEBASIS_BYSTATE(v_sar_major_peril,v_sar_code_1||sar_code_2||sar_code_3,v_sar_state)),
	-- :LKP.LKP_SUPDEDUCTIBLEBASIS(v_sar_major_peril,v_sar_code_1||sar_code_2||sar_code_3))
	-- )
	DECODE(v_sar_type_bureau,
	'AL', '0',
	'AN', 'D',
	'AP', IFF(LKP_SUPDEDUCTIBLEBASIS_BYSTATE_v_sar_major_peril_v_sar_code_1_sar_code_2_sar_code_3_v_sar_state.DeductibleBasis IS NULL, LKP_SUPDEDUCTIBLEBASIS_v_sar_major_peril_v_sar_code_1_sar_code_2_sar_code_3.DeductibleBasis)) AS v_DeductibleBasis,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_DeductibleBasis)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_DeductibleBasis) AS o_DeductibleBasis,
	v_sar_exposure AS out_Exposure,
	-- *INF*: DECODE (TRUE,
	-- v_sar_rsn_amend_code='CWO', 0, 
	-- v_writtenexposure)
	-- --We actively zero out written exposure for CWO type transactions else we use the incoming exposure value
	DECODE(TRUE,
	v_sar_rsn_amend_code = 'CWO', 0,
	v_writtenexposure) AS out_writtenexposure,
	-- *INF*: LTRIM(RTRIM(v_sar_class_1_4))
	LTRIM(RTRIM(v_sar_class_1_4)) AS o_sar_class_1_4
	FROM EXP_Default
	LEFT JOIN LKP_PIF43NXCRStage
	ON LKP_PIF43NXCRStage.PifSymbol = EXP_Default.pif_symbol AND LKP_PIF43NXCRStage.PifPolicyNumber = EXP_Default.pif_policy_number AND LKP_PIF43NXCRStage.PifPolicyModule = EXP_Default.pif_module AND LKP_PIF43NXCRStage.PMDNXC1InsuranceLine = EXP_Default.sar_insurance_line AND LKP_PIF43NXCRStage.PMDNXC1LocationNumber = EXP_Default.lkp_sar_location_x AND LKP_PIF43NXCRStage.PMDNXC1SubLocationNumber = EXP_Default.lkp_sar_sub_location_x AND LKP_PIF43NXCRStage.PMDNXC1CspClassCode = EXP_Default.out_lkp_sar_class_1_4 AND LKP_PIF43NXCRStage.PMDUYC1Coverage = EXP_Default.out_sar_unit
	LEFT JOIN LKP_Pif350Stage
	ON LKP_Pif350Stage.PifSymbol = EXP_Default.pif_symbol AND LKP_Pif350Stage.PifPolicyNumber = EXP_Default.pif_policy_number AND LKP_Pif350Stage.PifModule = EXP_Default.pif_module AND LKP_Pif350Stage.UnitNum = EXP_Default.o_sar_unit_1_3_trimmed
	LEFT JOIN LKP_Pif43IXUnmodStage
	ON LKP_Pif43IXUnmodStage.PifSymbol = EXP_Default.pif_symbol AND LKP_Pif43IXUnmodStage.PifPolicyNumber = EXP_Default.pif_policy_number AND LKP_Pif43IXUnmodStage.PifModule = EXP_Default.pif_module AND LKP_Pif43IXUnmodStage.Pif43IXUnmodInsuranceLine = EXP_Default.sar_insurance_line AND LKP_Pif43IXUnmodStage.Pif43IXUnmodWCRatingState = EXP_Default.sar_state AND LKP_Pif43IXUnmodStage.Pif43IXUnmodReportingClassCode = EXP_Default.lkp_sar_class_1_4 AND LKP_Pif43IXUnmodStage.Pif43IXUnmodSplitRateSeq = EXP_Default.sar_seq_no AND LKP_Pif43IXUnmodStage.Pif43IXUnmodYearItemEffective = EXP_Default.sar_cov_eff_year AND LKP_Pif43IXUnmodStage.Pif43IXUnmodMonthItemEffective = EXP_Default.sar_cov_eff_month AND LKP_Pif43IXUnmodStage.Pif43IXUnmodDayItemEffective = EXP_Default.sar_cov_eff_day
	LEFT JOIN LKP_Pif43IXZWCModStage
	ON LKP_Pif43IXZWCModStage.PifSymbol = EXP_Default.pif_symbol AND LKP_Pif43IXZWCModStage.PifPolicyNumber = EXP_Default.pif_policy_number AND LKP_Pif43IXZWCModStage.PifModule = EXP_Default.pif_module AND LKP_Pif43IXZWCModStage.Pmdi4w1InsuranceLine = EXP_Default.sar_insurance_line AND LKP_Pif43IXZWCModStage.Pmdi4w1WcRatingState = EXP_Default.sar_state AND LKP_Pif43IXZWCModStage.Pmdi4w1ReportingClassCode = EXP_Default.lkp_sar_class_1_4 AND LKP_Pif43IXZWCModStage.Pmdi4w1SplitRateSeq = EXP_Default.sar_seq_no AND LKP_Pif43IXZWCModStage.Pmdi4w1YearItemEffective = EXP_Default.sar_cov_eff_year AND LKP_Pif43IXZWCModStage.Pmdi4w1MonthItemEffective = EXP_Default.sar_cov_eff_month AND LKP_Pif43IXZWCModStage.Pmdi4w1DayItemEffective = EXP_Default.sar_cov_eff_day
	LEFT JOIN LKP_Pif43LXGAStage
	ON LKP_Pif43LXGAStage.PifSymbol = EXP_Default.pif_symbol AND LKP_Pif43LXGAStage.PifPolicyNumber = EXP_Default.pif_policy_number AND LKP_Pif43LXGAStage.PifModule = EXP_Default.pif_module
	LEFT JOIN LKP_Pif43LXGLStage
	ON LKP_Pif43LXGLStage.PifSymbol = EXP_Default.pif_symbol AND LKP_Pif43LXGLStage.PifPolicyNumber = EXP_Default.pif_policy_number AND LKP_Pif43LXGLStage.PifModule = EXP_Default.pif_module AND LKP_Pif43LXGLStage.Pmdlxg1InsuranceLine = EXP_Default.sar_insurance_line
	LEFT JOIN LKP_Pif43LXZWCStage
	ON LKP_Pif43LXZWCStage.PifSymbol = EXP_Default.pif_symbol AND LKP_Pif43LXZWCStage.PifPolicyNumber = EXP_Default.pif_policy_number AND LKP_Pif43LXZWCStage.PifModule = EXP_Default.pif_module AND LKP_Pif43LXZWCStage.Pmdl4w1InsuranceLine = EXP_Default.sar_insurance_line
	LEFT JOIN LKP_Pif43NXCPStage
	ON LKP_Pif43NXCPStage.PifSymbol = EXP_Default.pif_symbol AND LKP_Pif43NXCPStage.PifPolicyNumber = EXP_Default.pif_policy_number AND LKP_Pif43NXCPStage.PifModule = EXP_Default.pif_module AND LKP_Pif43NXCPStage.Pmdnxp1InsuranceLine = EXP_Default.sar_insurance_line AND LKP_Pif43NXCPStage.Pmdnxp1LocationNumber = EXP_Default.lkp_sar_location_x AND LKP_Pif43NXCPStage.Pmdnxp1SubLocationNumber = EXP_Default.lkp_sar_sub_location_x
	LEFT JOIN LKP_Pif43RXCPStage
	ON LKP_Pif43RXCPStage.PifSymbol = EXP_Default.pif_symbol AND LKP_Pif43RXCPStage.PifPolicyNumber = EXP_Default.pif_policy_number AND LKP_Pif43RXCPStage.PifModule = EXP_Default.pif_module AND LKP_Pif43RXCPStage.Pmdrxp1InsuranceLine = EXP_Default.sar_insurance_line_CF AND LKP_Pif43RXCPStage.Pmdrxp1LocationNumber = EXP_Default.lkp_sar_location_x AND LKP_Pif43RXCPStage.Pmdrxp1SubLocationNumber = EXP_Default.lkp_sar_sub_location_x AND LKP_Pif43RXCPStage.Pmdrxp1CspClsCode = EXP_Default.lkp_sar_class_1_4 AND LKP_Pif43RXCPStage.Pmdrxp1PmsDefSubjOfIns = EXP_Default.lkp_sar_risk_unit_group
	LEFT JOIN LKP_Pif43RXGLStage
	ON LKP_Pif43RXGLStage.PifSymbol = EXP_Default.pif_symbol AND LKP_Pif43RXGLStage.PifPolicyNumber = EXP_Default.pif_policy_number AND LKP_Pif43RXGLStage.PifModule = EXP_Default.pif_module AND LKP_Pif43RXGLStage.Pmdrxg1InsuranceLine = EXP_Default.sar_insurance_line AND LKP_Pif43RXGLStage.Pmdrxg1PmsDefGlSubline = EXP_Default.sar_risk_unit_group AND LKP_Pif43RXGLStage.Pmdrxg1RiskTypeInd = EXP_Default.lkp_sar_seq_rsk_unt_a_2_1
	LEFT JOIN LKP_Pif43RXIMStage
	ON LKP_Pif43RXIMStage.PifSymbol = EXP_Default.pif_symbol AND LKP_Pif43RXIMStage.PifPolicyNumber = EXP_Default.pif_policy_number AND LKP_Pif43RXIMStage.PifModule = EXP_Default.pif_module AND LKP_Pif43RXIMStage.PMDRXI1InsuranceLine = EXP_Default.sar_insurance_line AND LKP_Pif43RXIMStage.PMDRXI1LocationNumber = EXP_Default.lkp_sar_location_x AND LKP_Pif43RXIMStage.PMDRXI1SubLocationNumber = EXP_Default.lkp_sar_sub_location_x AND LKP_Pif43RXIMStage.PMDRXI1RiskUnitGroup = EXP_Default.lkp_sar_risk_unit_group AND LKP_Pif43RXIMStage.PMDRXI1SequenceNumber = EXP_Default.lkp_sequencenumber
	LEFT JOIN LKP_Pif43UXGLStage
	ON LKP_Pif43UXGLStage.PifSymbol = EXP_Default.pif_symbol AND LKP_Pif43UXGLStage.PifPolicyNumber = EXP_Default.pif_policy_number AND LKP_Pif43UXGLStage.PifModule = EXP_Default.pif_module AND LKP_Pif43UXGLStage.Pmduxg1InsuranceLine = EXP_Default.sar_insurance_line_GL AND LKP_Pif43UXGLStage.Pmduxg1LocationNumber = EXP_Default.lkp_sar_location_x AND LKP_Pif43UXGLStage.Pmduxg1PmsDefGlSubline = EXP_Default.lkp_sar_risk_unit_group AND LKP_Pif43UXGLStage.Pmduxg1ClassCodeGroup = EXP_Default.lkp_sar_class_code_grp_x AND LKP_Pif43UXGLStage.Pmduxg1ClassCodeMember = EXP_Default.lkp_sar_class_code_mem_x AND LKP_Pif43UXGLStage.Pmduxg1ClassCode = EXP_Default.lkp_Pmduxg1ClassCode AND LKP_Pif43UXGLStage.Pmduxg1RiskSequence = EXP_Default.lkp_sar_seq_rsk_unt_a AND LKP_Pif43UXGLStage.Pmduxg1RiskTypeInd = EXP_Default.lkp_sar_seq_rsk_unt_a_2_1
	LEFT JOIN LKP_Pif43UXWCStage
	ON LKP_Pif43UXWCStage.PifSymbol = EXP_Default.pif_symbol AND LKP_Pif43UXWCStage.PifPolicyNumber = EXP_Default.pif_policy_number AND LKP_Pif43UXWCStage.PifModule = EXP_Default.pif_module AND LKP_Pif43UXWCStage.Pmdu4w1InsuranceLine = EXP_Default.sar_insurance_line AND LKP_Pif43UXWCStage.Pmdu4w1WcRatingState = EXP_Default.sar_state AND LKP_Pif43UXWCStage.Pmdu4w1LocationNumber = EXP_Default.sar_location_x AND LKP_Pif43UXWCStage.Pmdu4w1ClassCode = EXP_Default.lkp_sar_class_1_4 AND LKP_Pif43UXWCStage.Pmdu4w1SplitRateSeq = EXP_Default.sar_seq_no AND LKP_Pif43UXWCStage.Pmdu4w1YearItemEffective = EXP_Default.sar_cov_eff_year AND LKP_Pif43UXWCStage.Pmdu4w1MonthItemEffective = EXP_Default.sar_cov_eff_month AND LKP_Pif43UXWCStage.Pmdu4w1DayItemEffective = EXP_Default.sar_cov_eff_day AND LKP_Pif43UXWCStage.Pmdu4w1ClassCodeSeq = EXP_Default.lkp_ClassCodeSeq
	LEFT JOIN LKP_Pif43UYGLStage
	ON LKP_Pif43UYGLStage.PifSymbol = EXP_Default.pif_symbol AND LKP_Pif43UYGLStage.PifPolicyNumber = EXP_Default.pif_policy_number AND LKP_Pif43UYGLStage.PifModule = EXP_Default.pif_module AND LKP_Pif43UYGLStage.Pmduyg1InsuranceLine = EXP_Default.sar_insurance_line AND LKP_Pif43UYGLStage.Pmduyg1LocationNumber = EXP_Default.lkp_sar_location_x AND LKP_Pif43UYGLStage.Pmduyg1PmsDefGlSubline = EXP_Default.lkp_sar_risk_unit_group AND LKP_Pif43UYGLStage.Pmduyg1RiskSequence = EXP_Default.lkp_sar_seq_rsk_unt_a AND LKP_Pif43UYGLStage.Pmduyg1RiskTypeInd = EXP_Default.lkp_sar_seq_rsk_unt_a_2_1 AND LKP_Pif43UYGLStage.Pmduyg1ClassCode = EXP_Default.lkp_ClassCode AND LKP_Pif43UYGLStage.Pmduyg1YearItemEffective = EXP_Default.sar_cov_eff_year AND LKP_Pif43UYGLStage.Pmduyg1MonthItemEffective = EXP_Default.sar_cov_eff_month AND LKP_Pif43UYGLStage.Pmduyg1DayItemEffective = EXP_Default.sar_cov_eff_day
	LEFT JOIN LKP_Pif43UZWCStage
	ON LKP_Pif43UZWCStage.PifSymbol = LKP_Pif43UXWCStage.in_pif_symbol AND LKP_Pif43UZWCStage.PifPolicyNumber = LKP_Pif43UXWCStage.in_pif_policy_number AND LKP_Pif43UZWCStage.PifPolicyModule = LKP_Pif43UXWCStage.in_pif_module AND LKP_Pif43UZWCStage.PMDU4W1InsuranceLine = LKP_Pif43UXWCStage.in_sar_insurance_line AND LKP_Pif43UZWCStage.PMDU4W1WCRatingState = LKP_Pif43UXWCStage.in_sar_state AND LKP_Pif43UZWCStage.PMDU4W1LocationNumber = LKP_Pif43UXWCStage.in_sar_location_x AND LKP_Pif43UZWCStage.PMDU4W1ClassCode = LKP_Pif43UXWCStage.lkp_sar_class_1_4 AND LKP_Pif43UZWCStage.PMDU4W1SplitRateSeq = LKP_Pif43UXWCStage.in_sar_seq_no AND LKP_Pif43UZWCStage.PMDU4W1YearItemEffective = LKP_Pif43UXWCStage.in_sar_cov_eff_year AND LKP_Pif43UZWCStage.PMDU4W1MonthItemEffective = LKP_Pif43UXWCStage.in_sar_cov_eff_month AND LKP_Pif43UZWCStage.PMDU4W1DayItemEffective = LKP_Pif43UXWCStage.in_sar_cov_eff_day AND LKP_Pif43UZWCStage.PMDU4W1ClassCodeSeq = LKP_Pif43UXWCStage.in_ClassCodeSeq
	LEFT JOIN LKP_PifPUHM17Stage
	ON LKP_PifPUHM17Stage.PifSymbol = EXP_Default.pif_symbol AND LKP_PifPUHM17Stage.PifPolicyNumber = EXP_Default.pif_policy_number AND LKP_PifPUHM17Stage.PifPolicyModule = EXP_Default.pif_module AND LKP_PifPUHM17Stage.HRRUnitAlph = EXP_Default.lkp_sar_unit_3_1 AND LKP_PifPUHM17Stage.HRRZipCode = EXP_Default.sar_zip_postal_code AND LKP_PifPUHM17Stage.HRRCountyLocation = EXP_Default.lkp_Location AND LKP_PifPUHM17Stage.HRRCityLocation = EXP_Default.lkp_sar_city
	LEFT JOIN LKP_Policy_PolicyAKID
	ON LKP_Policy_PolicyAKID.pol_key = EXP_Default.Policy_Key
	LEFT JOIN LKP_pif_02_stage
	ON LKP_pif_02_stage.pif_symbol = EXP_Default.pif_symbol AND LKP_pif_02_stage.pif_policy_number = EXP_Default.pif_policy_number AND LKP_pif_02_stage.pif_module = EXP_Default.pif_module
	LEFT JOIN LKP_RISKLOCATION_RISKLOCATIONAKID LKP_RISKLOCATION_RISKLOCATIONAKID_pol_ak_id_v_RiskLocation_Unit_v_sar_loc_prov_territory_v_sar_state_v_sar_zip_postal_code_v_Tax_Location
	ON LKP_RISKLOCATION_RISKLOCATIONAKID_pol_ak_id_v_RiskLocation_Unit_v_sar_loc_prov_territory_v_sar_state_v_sar_zip_postal_code_v_Tax_Location.PolicyAKID = pol_ak_id
	AND LKP_RISKLOCATION_RISKLOCATIONAKID_pol_ak_id_v_RiskLocation_Unit_v_sar_loc_prov_territory_v_sar_state_v_sar_zip_postal_code_v_Tax_Location.LocationUnitNumber = v_RiskLocation_Unit
	AND LKP_RISKLOCATION_RISKLOCATIONAKID_pol_ak_id_v_RiskLocation_Unit_v_sar_loc_prov_territory_v_sar_state_v_sar_zip_postal_code_v_Tax_Location.RiskTerritory = v_sar_loc_prov_territory
	AND LKP_RISKLOCATION_RISKLOCATIONAKID_pol_ak_id_v_RiskLocation_Unit_v_sar_loc_prov_territory_v_sar_state_v_sar_zip_postal_code_v_Tax_Location.StateProvinceCode = v_sar_state
	AND LKP_RISKLOCATION_RISKLOCATIONAKID_pol_ak_id_v_RiskLocation_Unit_v_sar_loc_prov_territory_v_sar_state_v_sar_zip_postal_code_v_Tax_Location.ZipPostalCode = v_sar_zip_postal_code
	AND LKP_RISKLOCATION_RISKLOCATIONAKID_pol_ak_id_v_RiskLocation_Unit_v_sar_loc_prov_territory_v_sar_state_v_sar_zip_postal_code_v_Tax_Location.TaxLocation = v_Tax_Location

	LEFT JOIN LKP_POLICYCOVERAGE_POLICYCOVERAGEAKID LKP_POLICYCOVERAGE_POLICYCOVERAGEAKID_v_PolicyCoverageHashKey
	ON LKP_POLICYCOVERAGE_POLICYCOVERAGEAKID_v_PolicyCoverageHashKey.PolicyCoverageHashKey = v_PolicyCoverageHashKey

	LEFT JOIN LKP_STATISTICALCOVERAGE_STATISTICALCOVERAGEAKID LKP_STATISTICALCOVERAGE_STATISTICALCOVERAGEAKID_v_CoverageDetailHashKey
	ON LKP_STATISTICALCOVERAGE_STATISTICALCOVERAGEAKID_v_CoverageDetailHashKey.StatisticalCoverageHashKey = v_CoverageDetailHashKey

	LEFT JOIN LKP_POLICY_STRTGC_BUS_DIV_CODE LKP_POLICY_STRTGC_BUS_DIV_CODE_pol_ak_id
	ON LKP_POLICY_STRTGC_BUS_DIV_CODE_pol_ak_id.pol_ak_id = pol_ak_id

	LEFT JOIN LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_sub_location_x_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date
	ON LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_sub_location_x_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.pol_ak_id = pol_ak_id
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_sub_location_x_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_ins_line = v_sar_insurance_line
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_sub_location_x_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_loc_unit_num = v_RiskLocation_Unit
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_sub_location_x_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_sub_loc_unit_num = v_sar_sub_location_x
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_sub_location_x_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_section_code = v_sar_section
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_sub_location_x_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_co_num = sar_reinsurance_company_no
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_sub_location_x_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_prcnt_facultative_commssn = v_sar_faculta_comm_rate
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_sub_location_x_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_eff_date = v_sar_trans_eff_date
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_sub_location_x_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_exp_date = v_sar_exp_date

	LEFT JOIN LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_sub_location_x_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date
	ON LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_sub_location_x_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.pol_ak_id = pol_ak_id
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_sub_location_x_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_ins_line = v_sar_insurance_line
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_sub_location_x_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_loc_unit_num = v_RiskLocation_Unit
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_sub_location_x_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_sub_loc_unit_num = v_sar_sub_location_x
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_sub_location_x_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_section_code = v_sar_section
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_sub_location_x_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_co_num = sar_reinsurance_company_no
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_sub_location_x_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_prcnt_facultative_commssn = v_sar_faculta_comm_rate
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_sub_location_x_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_eff_date = v_dummy_start_date
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_sub_location_x_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_exp_date = v_dummy_end_date

	LEFT JOIN LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_WITHOUTSUBLOCATION LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_WITHOUTSUBLOCATION_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date
	ON LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_WITHOUTSUBLOCATION_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.pol_ak_id = pol_ak_id
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_WITHOUTSUBLOCATION_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_ins_line = v_sar_insurance_line
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_WITHOUTSUBLOCATION_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_loc_unit_num = v_RiskLocation_Unit
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_WITHOUTSUBLOCATION_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_section_code = v_sar_section
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_WITHOUTSUBLOCATION_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_co_num = sar_reinsurance_company_no
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_WITHOUTSUBLOCATION_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_prcnt_facultative_commssn = v_sar_faculta_comm_rate
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_WITHOUTSUBLOCATION_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_eff_date = v_sar_trans_eff_date
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_WITHOUTSUBLOCATION_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_exp_date = v_sar_exp_date

	LEFT JOIN LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_WITHOUTSUBLOCATION LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_WITHOUTSUBLOCATION_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date
	ON LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_WITHOUTSUBLOCATION_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.pol_ak_id = pol_ak_id
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_WITHOUTSUBLOCATION_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_ins_line = v_sar_insurance_line
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_WITHOUTSUBLOCATION_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_loc_unit_num = v_RiskLocation_Unit
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_WITHOUTSUBLOCATION_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_section_code = v_sar_section
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_WITHOUTSUBLOCATION_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_co_num = sar_reinsurance_company_no
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_WITHOUTSUBLOCATION_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_prcnt_facultative_commssn = v_sar_faculta_comm_rate
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_WITHOUTSUBLOCATION_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_eff_date = v_dummy_start_date
	AND LKP_REINSURANCECOVERAGE_COMMERCIALLINESPOLICIES_WITHOUTSUBLOCATION_pol_ak_id_v_sar_insurance_line_v_RiskLocation_Unit_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_exp_date = v_dummy_end_date

	LEFT JOIN LKP_REINSURANCE_COVERAGE_COMMERCIALLINESPOLICIES_DEFAULT LKP_REINSURANCE_COVERAGE_COMMERCIALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_insurance_line_sar_reinsurance_company_no_v_sar_section_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date
	ON LKP_REINSURANCE_COVERAGE_COMMERCIALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_insurance_line_sar_reinsurance_company_no_v_sar_section_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.pol_ak_id = pol_ak_id
	AND LKP_REINSURANCE_COVERAGE_COMMERCIALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_insurance_line_sar_reinsurance_company_no_v_sar_section_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_ins_line = v_sar_insurance_line
	AND LKP_REINSURANCE_COVERAGE_COMMERCIALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_insurance_line_sar_reinsurance_company_no_v_sar_section_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_co_num = sar_reinsurance_company_no
	AND LKP_REINSURANCE_COVERAGE_COMMERCIALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_insurance_line_sar_reinsurance_company_no_v_sar_section_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_section_code = v_sar_section
	AND LKP_REINSURANCE_COVERAGE_COMMERCIALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_insurance_line_sar_reinsurance_company_no_v_sar_section_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_prcnt_facultative_commssn = v_sar_faculta_comm_rate
	AND LKP_REINSURANCE_COVERAGE_COMMERCIALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_insurance_line_sar_reinsurance_company_no_v_sar_section_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_eff_date = v_sar_trans_eff_date
	AND LKP_REINSURANCE_COVERAGE_COMMERCIALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_insurance_line_sar_reinsurance_company_no_v_sar_section_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_exp_date = v_sar_exp_date

	LEFT JOIN LKP_REINSURANCE_COVERAGE_COMMERCIALLINESPOLICIES_DEFAULT LKP_REINSURANCE_COVERAGE_COMMERCIALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_insurance_line_sar_reinsurance_company_no_v_sar_section_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date
	ON LKP_REINSURANCE_COVERAGE_COMMERCIALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_insurance_line_sar_reinsurance_company_no_v_sar_section_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.pol_ak_id = pol_ak_id
	AND LKP_REINSURANCE_COVERAGE_COMMERCIALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_insurance_line_sar_reinsurance_company_no_v_sar_section_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_ins_line = v_sar_insurance_line
	AND LKP_REINSURANCE_COVERAGE_COMMERCIALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_insurance_line_sar_reinsurance_company_no_v_sar_section_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_co_num = sar_reinsurance_company_no
	AND LKP_REINSURANCE_COVERAGE_COMMERCIALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_insurance_line_sar_reinsurance_company_no_v_sar_section_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_section_code = v_sar_section
	AND LKP_REINSURANCE_COVERAGE_COMMERCIALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_insurance_line_sar_reinsurance_company_no_v_sar_section_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_prcnt_facultative_commssn = v_sar_faculta_comm_rate
	AND LKP_REINSURANCE_COVERAGE_COMMERCIALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_insurance_line_sar_reinsurance_company_no_v_sar_section_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_eff_date = v_dummy_start_date
	AND LKP_REINSURANCE_COVERAGE_COMMERCIALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_insurance_line_sar_reinsurance_company_no_v_sar_section_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_exp_date = v_dummy_end_date

	LEFT JOIN LKP_REINSURANCECOVERAGE_PERSONALLINESPOLICIES LKP_REINSURANCECOVERAGE_PERSONALLINESPOLICIES_pol_ak_id_v_sar_unit_3pos_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date
	ON LKP_REINSURANCECOVERAGE_PERSONALLINESPOLICIES_pol_ak_id_v_sar_unit_3pos_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.pol_ak_id = pol_ak_id
	AND LKP_REINSURANCECOVERAGE_PERSONALLINESPOLICIES_pol_ak_id_v_sar_unit_3pos_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_risk_unit = v_sar_unit_3pos
	AND LKP_REINSURANCECOVERAGE_PERSONALLINESPOLICIES_pol_ak_id_v_sar_unit_3pos_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_section_code = v_sar_section
	AND LKP_REINSURANCECOVERAGE_PERSONALLINESPOLICIES_pol_ak_id_v_sar_unit_3pos_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_co_num = sar_reinsurance_company_no
	AND LKP_REINSURANCECOVERAGE_PERSONALLINESPOLICIES_pol_ak_id_v_sar_unit_3pos_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_prcnt_facultative_commssn = v_sar_faculta_comm_rate
	AND LKP_REINSURANCECOVERAGE_PERSONALLINESPOLICIES_pol_ak_id_v_sar_unit_3pos_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_eff_date = v_sar_trans_eff_date
	AND LKP_REINSURANCECOVERAGE_PERSONALLINESPOLICIES_pol_ak_id_v_sar_unit_3pos_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_exp_date = v_sar_exp_date

	LEFT JOIN LKP_REINSURANCECOVERAGE_PERSONALLINESPOLICIES LKP_REINSURANCECOVERAGE_PERSONALLINESPOLICIES_pol_ak_id_v_sar_unit_3pos_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date
	ON LKP_REINSURANCECOVERAGE_PERSONALLINESPOLICIES_pol_ak_id_v_sar_unit_3pos_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.pol_ak_id = pol_ak_id
	AND LKP_REINSURANCECOVERAGE_PERSONALLINESPOLICIES_pol_ak_id_v_sar_unit_3pos_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_risk_unit = v_sar_unit_3pos
	AND LKP_REINSURANCECOVERAGE_PERSONALLINESPOLICIES_pol_ak_id_v_sar_unit_3pos_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_section_code = v_sar_section
	AND LKP_REINSURANCECOVERAGE_PERSONALLINESPOLICIES_pol_ak_id_v_sar_unit_3pos_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_co_num = sar_reinsurance_company_no
	AND LKP_REINSURANCECOVERAGE_PERSONALLINESPOLICIES_pol_ak_id_v_sar_unit_3pos_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_prcnt_facultative_commssn = v_sar_faculta_comm_rate
	AND LKP_REINSURANCECOVERAGE_PERSONALLINESPOLICIES_pol_ak_id_v_sar_unit_3pos_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_eff_date = v_dummy_start_date
	AND LKP_REINSURANCECOVERAGE_PERSONALLINESPOLICIES_pol_ak_id_v_sar_unit_3pos_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_exp_date = v_dummy_end_date

	LEFT JOIN LKP_REINSURANCECOVERAGE__PERSONALLINESPOLICIES_DEFAULT LKP_REINSURANCECOVERAGE__PERSONALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date
	ON LKP_REINSURANCECOVERAGE__PERSONALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.pol_ak_id = pol_ak_id
	AND LKP_REINSURANCECOVERAGE__PERSONALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_section_code = v_sar_section
	AND LKP_REINSURANCECOVERAGE__PERSONALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_co_num = sar_reinsurance_company_no
	AND LKP_REINSURANCECOVERAGE__PERSONALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_prcnt_facultative_commssn = v_sar_faculta_comm_rate
	AND LKP_REINSURANCECOVERAGE__PERSONALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_eff_date = v_sar_trans_eff_date
	AND LKP_REINSURANCECOVERAGE__PERSONALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_sar_trans_eff_date_v_sar_exp_date.reins_exp_date = v_sar_exp_date

	LEFT JOIN LKP_REINSURANCECOVERAGE__PERSONALLINESPOLICIES_DEFAULT LKP_REINSURANCECOVERAGE__PERSONALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date
	ON LKP_REINSURANCECOVERAGE__PERSONALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.pol_ak_id = pol_ak_id
	AND LKP_REINSURANCECOVERAGE__PERSONALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_section_code = v_sar_section
	AND LKP_REINSURANCECOVERAGE__PERSONALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_co_num = sar_reinsurance_company_no
	AND LKP_REINSURANCECOVERAGE__PERSONALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_prcnt_facultative_commssn = v_sar_faculta_comm_rate
	AND LKP_REINSURANCECOVERAGE__PERSONALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_eff_date = v_dummy_start_date
	AND LKP_REINSURANCECOVERAGE__PERSONALLINESPOLICIES_DEFAULT_pol_ak_id_v_sar_section_sar_reinsurance_company_no_v_sar_faculta_comm_rate_v_dummy_start_date_v_dummy_end_date.reins_exp_date = v_dummy_end_date

	LEFT JOIN LKP_PIF11STAGE LKP_PIF11STAGE_Policy_Key
	ON LKP_PIF11STAGE_Policy_Key.PolicyKey = Policy_Key

	LEFT JOIN LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_v_sar_class_1_4_sar_state
	ON LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_v_sar_class_1_4_sar_state.ClassCode = v_sar_class_1_4
	AND LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_v_sar_class_1_4_sar_state.RatingStateCode = sar_state

	LEFT JOIN LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_v_sar_class_1_4_99
	ON LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_v_sar_class_1_4_99.ClassCode = v_sar_class_1_4
	AND LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_v_sar_class_1_4_99.RatingStateCode = '99'

	LEFT JOIN LKP_SUPDEDUCTIBLEBASIS_BYSTATE LKP_SUPDEDUCTIBLEBASIS_BYSTATE_v_sar_major_peril_v_sar_code_1_sar_code_2_sar_code_3_v_sar_state
	ON LKP_SUPDEDUCTIBLEBASIS_BYSTATE_v_sar_major_peril_v_sar_code_1_sar_code_2_sar_code_3_v_sar_state.MajorPerilCode = v_sar_major_peril
	AND LKP_SUPDEDUCTIBLEBASIS_BYSTATE_v_sar_major_peril_v_sar_code_1_sar_code_2_sar_code_3_v_sar_state.CoverageCode = v_sar_code_1 || sar_code_2 || sar_code_3
	AND LKP_SUPDEDUCTIBLEBASIS_BYSTATE_v_sar_major_peril_v_sar_code_1_sar_code_2_sar_code_3_v_sar_state.MasterCompanyNumber = v_sar_state

	LEFT JOIN LKP_SUPDEDUCTIBLEBASIS LKP_SUPDEDUCTIBLEBASIS_v_sar_major_peril_v_sar_code_1_sar_code_2_sar_code_3
	ON LKP_SUPDEDUCTIBLEBASIS_v_sar_major_peril_v_sar_code_1_sar_code_2_sar_code_3.MajorPerilCode = v_sar_major_peril
	AND LKP_SUPDEDUCTIBLEBASIS_v_sar_major_peril_v_sar_code_1_sar_code_2_sar_code_3.CoverageCode = v_sar_code_1 || sar_code_2 || sar_code_3

),
LKP_PremiumTrans_Exp AS (
	SELECT
	PolKey,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	in_ExperienceModificationEffectiveDate,
	MajorPerilSequenceNumber,
	StateProvinceCode
	FROM (
		select  distinct Substring(PremiumTransaction.PremiumTransactionKey,1,12) as PolKey,
		PremiumTransaction.ExperienceModificationFactor as ExperienceModificationFactor,
		PremiumTransaction.ExperienceModificationEffectiveDate as ExperienceModificationEffectiveDate,
		StatisticalCoverage.MajorPerilSequenceNumber as MajorPerilSequenceNumber,
		SUBSTRING(RiskLocation.StateProvinceCode,1,2) as StateProvinceCode
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation 
		inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage 
		on RiskLocation.RiskLocationAKID=PolicyCoverage.RiskLocationAKID
		inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage
		on PolicyCoverage.PolicyCoverageAKID=StatisticalCoverage.PolicyCoverageAKID
		and StatisticalCoverage.CurrentSnapshotFlag=1  and PolicyCoverage.CurrentSnapshotFlag=1
		inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction
		on StatisticalCoverage.StatisticalCoverageAKID=PremiumTransaction.StatisticalCoverageAKID
		and  PremiumTransaction.CurrentSnapshotFlag=1
		where PolicyCoverage.TypeBureauCode in ('WC','WP','WorkersCompensation') 
		and  PremiumTransaction.PremiumType='D'   
		and PremiumTransaction.ReasonAmendedCode != 'CWO'
		and StatisticalCoverage.ClassCode = '9898' 
		and PremiumTransaction.SourceSystemID = 'PMS'
		order by PolKey--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolKey,ExperienceModificationEffectiveDate,MajorPerilSequenceNumber,StateProvinceCode ORDER BY PolKey) = 1
),
LKP_PremiumTransaction AS (
	SELECT
	PremiumTransactionID,
	PremiumTransactionAKID,
	PremiumTransactionHashKey,
	DuplicateSequence
	FROM (
		SELECT PremiumTransactionID as PremiumTransactionID,
			            PremiumtransactionAKID as PremiumtransactionAKID,
		                  PremiumtransactionHashKey as PremiumtransactionHashKey,
			            DuplicateSequence as DuplicateSequence
		FROM	
					@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Policy POL,
					@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage POLCOV,
					@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage STATCOV,
		                    @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT
		WHERE	POLCOV.PolicyAKID = POL.pol_ak_id
				AND POLCOV.PolicyCoverageAKID = STATCOV.PolicyCoverageAKID
		             AND STATCOV.StatisticalCoverageAKID = PT.StatisticalCoverageAKID
				AND POL.crrnt_snpsht_flag = 1 
				AND STATCOV.CurrentSnapshotFlag =1 
				AND POLCOV.CurrentSnapshotFlag =1 
				AND PT.CurrentSnapshotFlag =1 
				AND  EXISTS  (SELECT DISTINCT PolicyKey FROM  
									@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPolicyKeyList
									WHERE POL.pol_key = PolicyKey AND @{pipeline().parameters.WHERE_CLAUSE_EDW} )
		             AND PT.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionHashKey,DuplicateSequence ORDER BY PremiumTransactionID DESC) = 1
),
LKP_sup_premium_transaction_code AS (
	SELECT
	sup_prem_trans_code_id,
	prem_trans_code
	FROM (
		SELECT 
			sup_prem_trans_code_id,
			prem_trans_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_premium_transaction_code
		WHERE crrnt_snpsht_flag=1 AND source_sys_id= '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY prem_trans_code ORDER BY sup_prem_trans_code_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_PremiumTransaction.PremiumTransactionID,
	LKP_PremiumTransaction.PremiumTransactionAKID AS PremiumTransactionAKID_lkp,
	EXP_Values.CoverageDetailAKID AS StatisticalCoverageAKID,
	0 AS logicalIndicator,
	EXP_Values.PremiumTransactionHashKey,
	EXP_Values.CoverageKey,
	'1' AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	SYSDATE AS CreateDate,
	SYSDATE AS ModifiedDate,
	EXP_Values.PMSFunctionCode,
	EXP_Values.PremiumType,
	EXP_Values.Trans_eff_date,
	EXP_Values.Trans_entered_date,
	EXP_Values.Trans_expiration_date,
	EXP_Values.sar_transaction,
	EXP_Values.sar_premium,
	EXP_Values.sar_original_prem,
	EXP_Values.Trans_Booked_date,
	EXP_Values.ReinsuranceCoverageAKID,
	EXP_Values.Reason_amend_code,
	LKP_sup_premium_transaction_code.sup_prem_trans_code_id AS in_sup_prem_trans_code_id,
	'N/A' AS o_OffsetOnsetIndicator,
	-- *INF*: IIF(ISNULL(in_sup_prem_trans_code_id),-1,in_sup_prem_trans_code_id)
	IFF(in_sup_prem_trans_code_id IS NULL, - 1, in_sup_prem_trans_code_id) AS o_sup_prem_trans_code_id,
	EXP_Values.logical_flag,
	-- *INF*: TO_INTEGER(logical_flag)
	TO_INTEGER(logical_flag) AS logical_flag_out,
	EXP_Values.PremiumLoadSequence,
	EXP_Values.DuplicateSequenceNum,
	EXP_Values.sar_annual_state_line,
	EXP_Values.sar_special_use,
	EXP_Values.sar_stat_breakdown_line,
	EXP_Values.sar_user_line,
	EXP_Values.sar_rating_date_ind,
	EXP_Values.sar_code_1,
	EXP_Values.sar_code_2,
	EXP_Values.sar_code_3,
	EXP_Values.sar_code_4,
	EXP_Values.sar_code_5,
	EXP_Values.sar_code_6,
	EXP_Values.sar_code_7,
	EXP_Values.sar_code_8,
	EXP_Values.sar_code_9,
	EXP_Values.sar_code_10,
	EXP_Values.sar_code_11,
	EXP_Values.sar_code_12,
	EXP_Values.sar_code_13,
	EXP_Values.sar_code_14,
	EXP_Values.sar_code_15,
	EXP_Values.sar_audit_reinst_ind,
	EXP_Values.pif_4514_stage_id,
	EXP_Values.out_DeductibleAmount AS DeductibleAmount,
	EXP_Values.out_RetroactiveDate AS RetroactiveDate,
	EXP_Values.o_sar_class_1_4 AS sar_class_1_4,
	LKP_PremiumTrans_Exp.PolKey AS LKP_PremiumTransactionKey,
	LKP_PremiumTrans_Exp.ExperienceModificationFactor AS LKP_ExperienceModificationFactor,
	LKP_PremiumTrans_Exp.ExperienceModificationEffectiveDate AS LKP_ExperienceModificationEffectiveDate,
	EXP_Values.SupClassificationWorkersCompensationId,
	EXP_Values.SupClassificationWorkersCompensationId99,
	EXP_Values.out_ExperienceModificationFactor AS ExperienceModificationFactor,
	EXP_Values.out_ExperienceModificationEffectiveDate AS ExperienceModificationEffectiveDate,
	-- *INF*: IIF(LTRIM(RTRIM(sar_class_1_4))='9898', ExperienceModificationFactor,IIF((NOT ISNULL(LKP_PremiumTransactionKey)) AND ((NOT ISNULL(SupClassificationWorkersCompensationId)) OR (NOT ISNULL(SupClassificationWorkersCompensationId99))), LKP_ExperienceModificationFactor, ExperienceModificationFactor))
	IFF(LTRIM(RTRIM(sar_class_1_4)) = '9898', ExperienceModificationFactor, IFF(( NOT LKP_PremiumTransactionKey IS NULL ) AND ( ( NOT SupClassificationWorkersCompensationId IS NULL ) OR ( NOT SupClassificationWorkersCompensationId99 IS NULL ) ), LKP_ExperienceModificationFactor, ExperienceModificationFactor)) AS out_ExperienceModificationFactor,
	-- *INF*: IIF(LTRIM(RTRIM(sar_class_1_4)) = '9898', ExperienceModificationEffectiveDate,IIF((NOT ISNULL(LKP_PremiumTransactionKey)) AND ((NOT ISNULL(SupClassificationWorkersCompensationId)) OR (NOT ISNULL(SupClassificationWorkersCompensationId99))), LKP_ExperienceModificationEffectiveDate, ExperienceModificationEffectiveDate))
	IFF(LTRIM(RTRIM(sar_class_1_4)) = '9898', ExperienceModificationEffectiveDate, IFF(( NOT LKP_PremiumTransactionKey IS NULL ) AND ( ( NOT SupClassificationWorkersCompensationId IS NULL ) OR ( NOT SupClassificationWorkersCompensationId99 IS NULL ) ), LKP_ExperienceModificationEffectiveDate, ExperienceModificationEffectiveDate)) AS out_ExperienceModificationEffectiveDate,
	EXP_Values.out_PackageModificationAdjustmentFactor AS PackageModificationAdjustmentFactor,
	EXP_Values.out_PackageModificationAdjustmentGroupCode AS PackageModificationAdjustmentGroupCode,
	EXP_Values.out_IncreasedLimitFactor AS IncreasedLimitFactor,
	EXP_Values.out_IncreasedLimitGroupCode AS IncreasedLimitGroupCode,
	EXP_Values.out_YearBuilt AS YearBuilt,
	EXP_Values.out_AgencyActualCommissionRate AS AgencyActualCommissionRate,
	EXP_Values.out_BaseRate AS BaseRate,
	EXP_Values.out_ConstructionCode AS i_ConstructionCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(:LKP.LKP_SUPCONSTRUCTIONCODE(i_ConstructionCode))
	:UDF.DEFAULT_VALUE_FOR_STRINGS(LKP_SUPCONSTRUCTIONCODE_i_ConstructionCode.StandardConstructionCodeDescription) AS o_ConstructionCode,
	EXP_Values.out_StateRatingEffectiveDate AS StateRatingEffectiveDate,
	EXP_Values.out_WCRetrospectiveRatingIndicator AS WCRetrospectiveRatingIndicator,
	EXP_Values.out_WCPolicyRatingType AS WCPolicyRatingType,
	EXP_Values.out_WCPolicyPlanCode AS WCPolicyPlanCode,
	EXP_Values.out_IsoFireProtectionCode AS IsoFireProtectionCode,
	EXP_Values.out_MultiLocationCreditFactor AS MultiLocationCreditFactor,
	EXP_Values.out_PreferredPropertyFactor AS PreferredPropertyFactor,
	EXP_Values.out_IndividualRiskPremiumModification AS IndividualRiskPremiumModification,
	EXP_Values.out_WindCoverageIndicator AS WindCoverageIndicator,
	EXP_Values.o_DeductibleBasis AS DeductibleBasis,
	EXP_Values.out_Exposure AS Exposure,
	EXP_Values.out_writtenexposure AS WriitenExposure,
	0 AS NumberOfEmployee,
	0 AS DeclaredEventFlag
	FROM EXP_Values
	LEFT JOIN LKP_PremiumTrans_Exp
	ON LKP_PremiumTrans_Exp.PolKey = EXP_Values.Policy_Key AND LKP_PremiumTrans_Exp.ExperienceModificationEffectiveDate = EXP_Values.cov_eff_date AND LKP_PremiumTrans_Exp.MajorPerilSequenceNumber = EXP_Values.o_sar_seq_no AND LKP_PremiumTrans_Exp.StateProvinceCode = EXP_Values.o_sar_state
	LEFT JOIN LKP_PremiumTransaction
	ON LKP_PremiumTransaction.PremiumTransactionHashKey = EXP_Values.PremiumTransactionHashKey AND LKP_PremiumTransaction.DuplicateSequence = EXP_Values.DuplicateSequenceNum
	LEFT JOIN LKP_sup_premium_transaction_code
	ON LKP_sup_premium_transaction_code.prem_trans_code = EXP_Values.sar_transaction
	LEFT JOIN LKP_SUPCONSTRUCTIONCODE LKP_SUPCONSTRUCTIONCODE_i_ConstructionCode
	ON LKP_SUPCONSTRUCTIONCODE_i_ConstructionCode.ConstructionCode = i_ConstructionCode

),
RTR_Insert_Update_PremTran AS (
	SELECT
	PremiumTransactionID,
	PremiumTransactionAKID_lkp AS PremiumTransactionAKID_lookup,
	StatisticalCoverageAKID,
	PremiumTransactionHashKey,
	CoverageKey,
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreateDate,
	ModifiedDate,
	PMSFunctionCode,
	PremiumType,
	Trans_eff_date,
	Trans_entered_date,
	Trans_expiration_date,
	sar_transaction,
	sar_premium,
	sar_original_prem,
	Trans_Booked_date,
	ReinsuranceCoverageAKID,
	Reason_amend_code,
	o_OffsetOnsetIndicator AS OffsetOnsetIndicator,
	o_sup_prem_trans_code_id AS sup_prem_trans_code_id,
	logical_flag_out,
	PremiumLoadSequence,
	DuplicateSequenceNum,
	sar_annual_state_line,
	sar_special_use,
	sar_stat_breakdown_line,
	sar_user_line,
	sar_rating_date_ind,
	sar_code_1,
	sar_code_2,
	sar_code_3,
	sar_code_4,
	sar_code_5,
	sar_code_6,
	sar_code_7,
	sar_code_8,
	sar_code_9,
	sar_code_10,
	sar_code_11,
	sar_code_12,
	sar_code_13,
	sar_code_14,
	sar_code_15,
	sar_audit_reinst_ind,
	pif_4514_stage_id,
	DeductibleAmount,
	RetroactiveDate,
	out_ExperienceModificationFactor AS ExperienceModificationFactor,
	out_ExperienceModificationEffectiveDate AS ExperienceModificationEffectiveDate,
	PackageModificationAdjustmentFactor,
	PackageModificationAdjustmentGroupCode,
	IncreasedLimitFactor,
	IncreasedLimitGroupCode,
	YearBuilt,
	AgencyActualCommissionRate,
	BaseRate,
	o_ConstructionCode AS ConstructionCode,
	StateRatingEffectiveDate,
	WCRetrospectiveRatingIndicator,
	WCPolicyRatingType,
	WCPolicyPlanCode,
	IsoFireProtectionCode,
	MultiLocationCreditFactor,
	PreferredPropertyFactor,
	IndividualRiskPremiumModification,
	WindCoverageIndicator,
	DeductibleBasis,
	Exposure,
	WriitenExposure AS WrittenExposure,
	NumberOfEmployee,
	DeclaredEventFlag
	FROM EXP_Detect_Changes
),
RTR_Insert_Update_PremTran_UPDATE AS (SELECT * FROM RTR_Insert_Update_PremTran WHERE NOT ISNULL(PremiumTransactionAKID_lookup)),
RTR_Insert_Update_PremTran_INSERT AS (SELECT * FROM RTR_Insert_Update_PremTran WHERE ISNULL(PremiumTransactionAKID_lookup) OR NOT ISNULL(PremiumTransactionAKID_lookup)),
UPD_PremiumTransaction AS (
	SELECT
	PremiumTransactionID AS PremiumTransactionID1, 
	PremiumTransactionAKID_lookup, 
	StatisticalCoverageAKID AS StatisticalCoverageAKID1, 
	PremiumTransactionHashKey AS PremiumTransactionHashKey1, 
	CoverageKey AS CoverageKey1, 
	CurrentSnapshotFlag AS CurrentSnapshotFlag1, 
	AuditID AS AuditID1, 
	EffectiveDate AS EffectiveDate1, 
	ExpirationDate AS ExpirationDate1, 
	SourceSystemID AS SourceSystemID1, 
	CreateDate AS CreateDate1, 
	ModifiedDate AS ModifiedDate1, 
	PMSFunctionCode AS PMSFunctionCode1, 
	PremiumType AS PremiumType1, 
	Trans_eff_date AS Trans_eff_date1, 
	Trans_entered_date AS Trans_entered_date1, 
	Trans_expiration_date AS Trans_expiration_date1, 
	sar_transaction AS sar_transaction1, 
	sar_premium AS sar_premium1, 
	sar_original_prem AS sar_original_prem1, 
	Trans_Booked_date AS Trans_Booked_date1, 
	ReinsuranceCoverageAKID AS ReinsuranceCoverageAKID1, 
	Reason_amend_code AS Reason_amend_code1, 
	logical_flag_out AS logical_flag_out1, 
	WindCoverageIndicator AS WindCoverageIndicator1, 
	PremiumLoadSequence AS PremiumLoadSequence1, 
	ExperienceModificationFactor AS ExperienceModificationFactor1, 
	ExperienceModificationEffectiveDate AS ExperienceModificationEffectiveDate1, 
	NumberOfEmployee AS NumberOfEmployee1, 
	DeclaredEventFlag AS DeclaredEventFlag1
	FROM RTR_Insert_Update_PremTran_UPDATE
),
PremiumTransactionUpdate AS (
	MERGE INTO PremiumTransaction AS T
	USING UPD_PremiumTransaction AS S
	ON T.PremiumTransactionID = S.PremiumTransactionID1
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate1, T.PremiumLoadSequence = S.PremiumLoadSequence1, T.ExperienceModificationFactor = S.ExperienceModificationFactor1, T.ExperienceModificationEffectiveDate = S.ExperienceModificationEffectiveDate1, T.NumberOfEmployee = S.NumberOfEmployee1, T.DeclaredEventFlag = S.DeclaredEventFlag1
),
SEQ_PremiumTransactionAKID AS (
	CREATE SEQUENCE SEQ_PremiumTransactionAKID
	START = 0
	INCREMENT = 1;
),
EXP_Detemine_AK_ID AS (
	SELECT
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreateDate,
	ModifiedDate,
	logical_flag_out AS logical_flag_out3,
	1 AS LogicalDeleteFlag,
	PremiumTransactionAKID_lookup,
	SEQ_PremiumTransactionAKID.NEXTVAL,
	PremiumTransactionHashKey,
	PremiumLoadSequence,
	DuplicateSequenceNum AS DuplicateSequenceNum3,
	-- *INF*: IIF(ISNULL(PremiumTransactionAKID_lookup), NEXTVAL, PremiumTransactionAKID_lookup)
	IFF(PremiumTransactionAKID_lookup IS NULL, NEXTVAL, PremiumTransactionAKID_lookup) AS PremiumTransactionAKID_Out,
	ReinsuranceCoverageAKID,
	StatisticalCoverageAKID,
	CoverageKey,
	PMSFunctionCode,
	sar_transaction,
	Trans_entered_date,
	Trans_eff_date,
	Trans_expiration_date,
	Trans_Booked_date,
	sar_premium,
	sar_original_prem,
	PremiumType,
	Reason_amend_code,
	OffsetOnsetIndicator,
	sup_prem_trans_code_id,
	-1 AS RatingCoverageAKID,
	sar_annual_state_line,
	sar_special_use,
	sar_stat_breakdown_line,
	sar_user_line,
	sar_rating_date_ind,
	sar_code_18 AS sar_code_1,
	sar_code_2,
	sar_code_ AS sar_code_3,
	sar_code_4,
	sar_code_5,
	sar_code_6,
	sar_code_7,
	sar_code_8,
	sar_code_9,
	sar_code_10,
	sar_code_11,
	sar_code_12,
	sar_code_1 AS sar_code_13,
	sar_code_14,
	sar_code_15,
	sar_audit_reinst_ind,
	pif_4514_stage_id,
	DeductibleAmount,
	RetroactiveDate,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	PackageModificationAdjustmentFactor,
	PackageModificationAdjustmentGroupCode,
	IncreasedLimitFactor,
	IncreasedLimitGroupCode,
	YearBuilt,
	AgencyActualCommissionRate,
	BaseRate,
	ConstructionCode,
	StateRatingEffectiveDate,
	WCRetrospectiveRatingIndicator,
	WCPolicyRatingType,
	WCPolicyPlanCode,
	IsoFireProtectionCode,
	MultiLocationCreditFactor,
	PreferredPropertyFactor,
	IndividualRiskPremiumModification,
	WindCoverageIndicator AS WindCoverageIndicator3,
	DeductibleBasis,
	Exposure,
	WrittenExposure,
	NumberOfEmployee AS NumberOfEmployee3,
	'N/A' AS NegateRestateCode,
	DeclaredEventFlag AS DeclaredEventFlag3
	FROM RTR_Insert_Update_PremTran_INSERT
),
FIL_Insert_PremiumTransaction AS (
	SELECT
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemID, 
	CreateDate, 
	ModifiedDate, 
	logical_flag_out3, 
	LogicalDeleteFlag, 
	PremiumTransactionAKID_lookup, 
	PremiumTransactionHashKey, 
	PremiumLoadSequence, 
	DuplicateSequenceNum3, 
	PremiumTransactionAKID_Out, 
	ReinsuranceCoverageAKID, 
	StatisticalCoverageAKID, 
	CoverageKey, 
	PMSFunctionCode, 
	sar_transaction, 
	Trans_entered_date, 
	Trans_eff_date, 
	Trans_expiration_date, 
	Trans_Booked_date, 
	sar_premium, 
	sar_original_prem, 
	PremiumType, 
	Reason_amend_code, 
	OffsetOnsetIndicator, 
	sup_prem_trans_code_id, 
	RatingCoverageAKID, 
	pif_4514_stage_id, 
	DeductibleAmount, 
	RetroactiveDate, 
	ExperienceModificationFactor, 
	ExperienceModificationEffectiveDate, 
	PackageModificationAdjustmentFactor, 
	PackageModificationAdjustmentGroupCode, 
	IncreasedLimitFactor, 
	IncreasedLimitGroupCode, 
	YearBuilt, 
	AgencyActualCommissionRate, 
	BaseRate, 
	ConstructionCode, 
	StateRatingEffectiveDate, 
	WCRetrospectiveRatingIndicator, 
	WCPolicyRatingType, 
	WCPolicyPlanCode, 
	IsoFireProtectionCode, 
	MultiLocationCreditFactor, 
	PreferredPropertyFactor, 
	IndividualRiskPremiumModification, 
	WindCoverageIndicator3, 
	DeductibleBasis, 
	Exposure, 
	WrittenExposure, 
	NumberOfEmployee3, 
	NegateRestateCode, 
	DeclaredEventFlag3
	FROM EXP_Detemine_AK_ID
	WHERE IIF(ISNULL(PremiumTransactionAKID_lookup), TRUE, FALSE)
),
EXPTRANS AS (
	SELECT
	AuditID,
	SourceSystemID,
	PremiumTransactionAKID_Out AS PremiumTransactionAKID,
	SYSDATE AS o_CreatedDate,
	pif_4514_stage_id,
	WindCoverageIndicator3,
	DeductibleBasis,
	'N/A' AS o_ExposureBasis,
	'N/A' AS o_TransactionCreatedUserId,
	'N/A' AS o_ServiceCentreName
	FROM FIL_Insert_PremiumTransaction
),
PremiumTransactionInsert AS (

	------------ PRE SQL ----------
	exec [spSetIndexStatus] @Enable = 0, @Schema = 'dbo', @TableName = 'PremiumTransaction', @IndexWildcard = 'AK1PremiumTransaction'
	-------------------------------


	INSERT INTO PremiumTransaction
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, LogicalIndicator, LogicalDeleteFlag, PremiumTransactionHashKey, PremiumLoadSequence, DuplicateSequence, PremiumTransactionAKID, ReinsuranceCoverageAKID, StatisticalCoverageAKID, PremiumTransactionKey, PMSFunctionCode, PremiumTransactionCode, PremiumTransactionEnteredDate, PremiumTransactionEffectiveDate, PremiumTransactionExpirationDate, PremiumTransactionBookedDate, PremiumTransactionAmount, FullTermPremium, PremiumType, ReasonAmendedCode, OffsetOnsetCode, SupPremiumTransactionCodeId, RatingCoverageAKId, DeductibleAmount, ExperienceModificationFactor, ExperienceModificationEffectiveDate, PackageModificationAdjustmentFactor, PackageModificationAdjustmentGroupCode, IncreasedLimitFactor, IncreasedLimitGroupCode, YearBuilt, AgencyActualCommissionRate, BaseRate, ConstructionCode, StateRatingEffectiveDate, IndividualRiskPremiumModification, WindCoverageFlag, DeductibleBasis, ExposureBasis, TransactionCreatedUserId, ServiceCentreName, Exposure, NumberOfEmployee, NegateRestateCode, WrittenExposure, DeclaredEventFlag)
	SELECT 
	FIL_Insert_PremiumTransaction.CURRENTSNAPSHOTFLAG, 
	FIL_Insert_PremiumTransaction.AUDITID, 
	FIL_Insert_PremiumTransaction.EFFECTIVEDATE, 
	FIL_Insert_PremiumTransaction.EXPIRATIONDATE, 
	FIL_Insert_PremiumTransaction.SOURCESYSTEMID, 
	FIL_Insert_PremiumTransaction.CreateDate AS CREATEDDATE, 
	FIL_Insert_PremiumTransaction.MODIFIEDDATE, 
	FIL_Insert_PremiumTransaction.logical_flag_out3 AS LOGICALINDICATOR, 
	FIL_Insert_PremiumTransaction.LOGICALDELETEFLAG, 
	FIL_Insert_PremiumTransaction.PREMIUMTRANSACTIONHASHKEY, 
	FIL_Insert_PremiumTransaction.PREMIUMLOADSEQUENCE, 
	FIL_Insert_PremiumTransaction.DuplicateSequenceNum3 AS DUPLICATESEQUENCE, 
	FIL_Insert_PremiumTransaction.PremiumTransactionAKID_Out AS PREMIUMTRANSACTIONAKID, 
	FIL_Insert_PremiumTransaction.REINSURANCECOVERAGEAKID, 
	FIL_Insert_PremiumTransaction.STATISTICALCOVERAGEAKID, 
	FIL_Insert_PremiumTransaction.CoverageKey AS PREMIUMTRANSACTIONKEY, 
	FIL_Insert_PremiumTransaction.PMSFUNCTIONCODE, 
	FIL_Insert_PremiumTransaction.sar_transaction AS PREMIUMTRANSACTIONCODE, 
	FIL_Insert_PremiumTransaction.Trans_entered_date AS PREMIUMTRANSACTIONENTEREDDATE, 
	FIL_Insert_PremiumTransaction.Trans_eff_date AS PREMIUMTRANSACTIONEFFECTIVEDATE, 
	FIL_Insert_PremiumTransaction.Trans_expiration_date AS PREMIUMTRANSACTIONEXPIRATIONDATE, 
	FIL_Insert_PremiumTransaction.Trans_Booked_date AS PREMIUMTRANSACTIONBOOKEDDATE, 
	FIL_Insert_PremiumTransaction.sar_premium AS PREMIUMTRANSACTIONAMOUNT, 
	FIL_Insert_PremiumTransaction.sar_original_prem AS FULLTERMPREMIUM, 
	FIL_Insert_PremiumTransaction.PREMIUMTYPE, 
	FIL_Insert_PremiumTransaction.Reason_amend_code AS REASONAMENDEDCODE, 
	FIL_Insert_PremiumTransaction.OffsetOnsetIndicator AS OFFSETONSETCODE, 
	FIL_Insert_PremiumTransaction.sup_prem_trans_code_id AS SUPPREMIUMTRANSACTIONCODEID, 
	FIL_Insert_PremiumTransaction.RatingCoverageAKID AS RATINGCOVERAGEAKID, 
	FIL_Insert_PremiumTransaction.DEDUCTIBLEAMOUNT, 
	FIL_Insert_PremiumTransaction.EXPERIENCEMODIFICATIONFACTOR, 
	FIL_Insert_PremiumTransaction.EXPERIENCEMODIFICATIONEFFECTIVEDATE, 
	FIL_Insert_PremiumTransaction.PACKAGEMODIFICATIONADJUSTMENTFACTOR, 
	FIL_Insert_PremiumTransaction.PACKAGEMODIFICATIONADJUSTMENTGROUPCODE, 
	FIL_Insert_PremiumTransaction.INCREASEDLIMITFACTOR, 
	FIL_Insert_PremiumTransaction.INCREASEDLIMITGROUPCODE, 
	FIL_Insert_PremiumTransaction.YEARBUILT, 
	FIL_Insert_PremiumTransaction.AGENCYACTUALCOMMISSIONRATE, 
	FIL_Insert_PremiumTransaction.BASERATE, 
	FIL_Insert_PremiumTransaction.CONSTRUCTIONCODE, 
	FIL_Insert_PremiumTransaction.STATERATINGEFFECTIVEDATE, 
	FIL_Insert_PremiumTransaction.INDIVIDUALRISKPREMIUMMODIFICATION, 
	EXPTRANS.WindCoverageIndicator3 AS WINDCOVERAGEFLAG, 
	EXPTRANS.DEDUCTIBLEBASIS, 
	EXPTRANS.o_ExposureBasis AS EXPOSUREBASIS, 
	EXPTRANS.o_TransactionCreatedUserId AS TRANSACTIONCREATEDUSERID, 
	EXPTRANS.o_ServiceCentreName AS SERVICECENTRENAME, 
	FIL_Insert_PremiumTransaction.EXPOSURE, 
	FIL_Insert_PremiumTransaction.NumberOfEmployee3 AS NUMBEROFEMPLOYEE, 
	FIL_Insert_PremiumTransaction.NEGATERESTATECODE, 
	FIL_Insert_PremiumTransaction.WRITTENEXPOSURE, 
	FIL_Insert_PremiumTransaction.DeclaredEventFlag3 AS DECLAREDEVENTFLAG
	FROM FIL_Insert_PremiumTransaction

	------------ POST SQL ----------
	exec [spSetIndexStatus] @Enable = 1, @Schema = 'dbo', @TableName = 'PremiumTransaction', @IndexWildcard = 'AK1PremiumTransaction'
	-------------------------------


),
WorkPremiumTransaction AS (

	------------ PRE SQL ----------
	if not exists (
	select 1 from @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction
	where AuditID=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}')
	truncate table @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransaction
	-------------------------------


	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransaction
	(AuditID, SourceSystemID, CreatedDate, PremiumTransactionAKId, PremiumTransactionStageId)
	SELECT 
	AUDITID, 
	SOURCESYSTEMID, 
	o_CreatedDate AS CREATEDDATE, 
	PremiumTransactionAKID AS PREMIUMTRANSACTIONAKID, 
	pif_4514_stage_id AS PREMIUMTRANSACTIONSTAGEID
	FROM EXPTRANS
),
EXP_Pre_BureauCodeLkp AS (
	SELECT
	PremiumTransactionAKID_Out AS PremiumTransactionAKID,
	CoverageKey,
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreateDate,
	ModifiedDate,
	LogicalDeleteFlag AS logical_flag,
	sar_stat_breakdown_line,
	sar_user_line,
	sar_code_1 AS BureauCode1,
	sar_code_2 AS BureauCode2,
	sar_code_3 AS BureauCode3,
	sar_code_4 AS BureauCode4,
	sar_code_5 AS BureauCode5,
	sar_code_6 AS BureauCode6,
	sar_code_7 AS BureauCode7,
	sar_code_8 AS BureauCode8,
	sar_code_9 AS BureauCode9,
	sar_code_10 AS BureauCode10,
	sar_code_11 AS BureauCode11,
	sar_code_12 AS BureauCode12,
	sar_code_13 AS BureauCode13,
	sar_code_14 AS BureauCode14,
	sar_code_15 AS BureauCode15,
	sar_special_use AS BureauSpecialUseCode,
	sar_annual_state_line AS PMSAnnualStatementLine,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(PMSAnnualStatementLine)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(PMSAnnualStatementLine) AS v_PMSAnnualStatementLine,
	v_PMSAnnualStatementLine AS PMSAnnualStatementLine_out,
	sar_rating_date_ind AS RatingDateIndicator,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(RatingDateIndicator)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(RatingDateIndicator) AS v_RatingDateIndicator,
	v_RatingDateIndicator AS RatingDateIndicator_out,
	sar_stat_breakdown_line || sar_user_line AS v_BureauStatisticalUserLine,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_BureauStatisticalUserLine)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_BureauStatisticalUserLine) AS BureauStatisticalUserLine,
	sar_audit_reinst_ind AS AuditReinstatementIndicator,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(AuditReinstatementIndicator)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(AuditReinstatementIndicator) AS v_AuditReinstatementIndicator,
	v_AuditReinstatementIndicator AS AuditReinstatementIndicator_out,
	-- *INF*: MD5(
	-- TO_CHAR(PremiumTransactionAKID)  ||  
	-- BureauCode1  ||  
	-- BureauCode2  ||  
	-- BureauCode3  ||  
	-- BureauCode4  ||  
	-- BureauCode5  ||  
	-- BureauCode6  ||  
	-- BureauCode7  ||  
	-- BureauCode8  ||  
	-- BureauCode9  ||  
	-- BureauCode10  ||  
	-- BureauCode11  ||  
	-- BureauCode12  ||  
	-- BureauCode13  ||  
	-- BureauCode14  ||  
	-- BureauCode15  ||  
	-- BureauSpecialUseCode  ||  
	-- PMSAnnualStatementLine  ||  
	-- RatingDateIndicator  ||  
	-- v_BureauStatisticalUserLine  ||
	-- AuditReinstatementIndicator )
	-- 
	MD5(TO_CHAR(PremiumTransactionAKID) || BureauCode1 || BureauCode2 || BureauCode3 || BureauCode4 || BureauCode5 || BureauCode6 || BureauCode7 || BureauCode8 || BureauCode9 || BureauCode10 || BureauCode11 || BureauCode12 || BureauCode13 || BureauCode14 || BureauCode15 || BureauSpecialUseCode || PMSAnnualStatementLine || RatingDateIndicator || v_BureauStatisticalUserLine || AuditReinstatementIndicator) AS v_BureauStatisticalCodeHashKey,
	v_BureauStatisticalCodeHashKey AS BureauStatisticalCodeHashKey
	FROM EXP_Detemine_AK_ID
),
LKP_BureauStatisticalCode AS (
	SELECT
	BureauStatisticalCodeAKID,
	BureauStatisticalCodeHashKey
	FROM (
		SELECT BureauStatisticalCodeAKID as BureauStatisticalCodeAKID,
		                  BureauStatisticalCodeHashKey as BureauStatisticalCodeHashKey
		FROM	@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation LOC, 
					@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Policy POL,
					@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage POLCOV,
					@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage STATCOV,
		                   @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT,
		                   @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.BureauStatisticalCode STATCD
		WHERE	LOC.PolicyAKID = POL.pol_ak_id
				AND LOC.RiskLocationAKID = POLCOV.RiskLocationAKID
				AND POLCOV.PolicyCoverageAKID = STATCOV.PolicyCoverageAKID
		             AND STATCOV.StatisticalCoverageAKID = PT.StatisticalCoverageAKID
		             AND PT.PremiumTransactionAKID = STATCD.PremiumTransactionAKID
				AND POL.crrnt_snpsht_flag = 1 AND LOC.CurrentSnapshotFlag =1
				AND STATCOV.CurrentSnapshotFlag =1 
				AND POLCOV.CurrentSnapshotFlag =1 
				AND PT.CurrentSnapshotFlag =1 
		             AND STATCD.CurrentSnapshotFlag =1 
				AND  EXISTS  (SELECT DISTINCT PolicyKey FROM  
									@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPolicyKeyList
									WHERE POL.pol_key = PolicyKey AND @{pipeline().parameters.WHERE_CLAUSE_EDW})
		             AND STATCD.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY BureauStatisticalCodeHashKey ORDER BY BureauStatisticalCodeAKID DESC) = 1
),
FIL_Insert_BureauStatisticalCode_Rows AS (
	SELECT
	LKP_BureauStatisticalCode.BureauStatisticalCodeAKID, 
	EXP_Pre_BureauCodeLkp.CurrentSnapshotFlag, 
	EXP_Pre_BureauCodeLkp.AuditID, 
	EXP_Pre_BureauCodeLkp.EffectiveDate, 
	EXP_Pre_BureauCodeLkp.ExpirationDate, 
	EXP_Pre_BureauCodeLkp.SourceSystemID, 
	EXP_Pre_BureauCodeLkp.CreateDate, 
	EXP_Pre_BureauCodeLkp.ModifiedDate, 
	EXP_Pre_BureauCodeLkp.logical_flag, 
	EXP_Pre_BureauCodeLkp.BureauStatisticalCodeHashKey, 
	EXP_Pre_BureauCodeLkp.PremiumTransactionAKID, 
	EXP_Pre_BureauCodeLkp.BureauCode1, 
	EXP_Pre_BureauCodeLkp.BureauCode2, 
	EXP_Pre_BureauCodeLkp.BureauCode3, 
	EXP_Pre_BureauCodeLkp.BureauCode4, 
	EXP_Pre_BureauCodeLkp.BureauCode5, 
	EXP_Pre_BureauCodeLkp.BureauCode6, 
	EXP_Pre_BureauCodeLkp.BureauCode7, 
	EXP_Pre_BureauCodeLkp.BureauCode8, 
	EXP_Pre_BureauCodeLkp.BureauCode9, 
	EXP_Pre_BureauCodeLkp.BureauCode10, 
	EXP_Pre_BureauCodeLkp.BureauCode11, 
	EXP_Pre_BureauCodeLkp.BureauCode12, 
	EXP_Pre_BureauCodeLkp.BureauCode13, 
	EXP_Pre_BureauCodeLkp.BureauCode14, 
	EXP_Pre_BureauCodeLkp.BureauCode15, 
	EXP_Pre_BureauCodeLkp.BureauSpecialUseCode, 
	EXP_Pre_BureauCodeLkp.PMSAnnualStatementLine_out AS PMSAnnualStatementLine, 
	EXP_Pre_BureauCodeLkp.RatingDateIndicator_out AS RatingDateIndicator, 
	EXP_Pre_BureauCodeLkp.BureauStatisticalUserLine, 
	EXP_Pre_BureauCodeLkp.AuditReinstatementIndicator_out AS AuditReinstatementIndicator
	FROM EXP_Pre_BureauCodeLkp
	LEFT JOIN LKP_BureauStatisticalCode
	ON LKP_BureauStatisticalCode.BureauStatisticalCodeHashKey = EXP_Pre_BureauCodeLkp.BureauStatisticalCodeHashKey
	WHERE IIF(ISNULL(BureauStatisticalCodeAKID), TRUE, FALSE)
),
SEQ_BureauStatisticalCode_AKID AS (
	CREATE SEQUENCE SEQ_BureauStatisticalCode_AKID
	START = 0
	INCREMENT = 1;
),
EXP_Determine_BureauCode_AKID AS (
	SELECT
	BureauStatisticalCodeAKID,
	SEQ_BureauStatisticalCode_AKID.NEXTVAL,
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreateDate,
	ModifiedDate,
	logical_flag,
	1 AS LogicalDeleteFlag,
	BureauStatisticalCodeHashKey,
	-- *INF*: IIF(ISNULL(BureauStatisticalCodeAKID), NEXTVAL, BureauStatisticalCodeAKID)
	IFF(BureauStatisticalCodeAKID IS NULL, NEXTVAL, BureauStatisticalCodeAKID) AS BureauStatisticalCodeAKID_Out,
	PremiumTransactionAKID,
	-1 AS PassThroughChargeTransactionAKID,
	BureauCode1,
	BureauCode2,
	BureauCode3,
	BureauCode4,
	BureauCode5,
	BureauCode6,
	BureauCode7,
	BureauCode8,
	BureauCode9,
	BureauCode10,
	BureauCode11,
	BureauCode12,
	BureauCode13,
	BureauCode14,
	BureauCode15,
	BureauSpecialUseCode,
	PMSAnnualStatementLine,
	RatingDateIndicator,
	BureauStatisticalUserLine,
	AuditReinstatementIndicator
	FROM FIL_Insert_BureauStatisticalCode_Rows
),
BureauStatisticalCode AS (
	INSERT INTO BureauStatisticalCode
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, LogicalIndicator, LogicalDeleteFlag, BureauStatisticalCodeHashKey, BureauStatisticalCodeAKID, PremiumTransactionAKID, PassThroughChargeTransactionAKID, BureauCode1, BureauCode2, BureauCode3, BureauCode4, BureauCode5, BureauCode6, BureauCode7, BureauCode8, BureauCode9, BureauCode10, BureauCode11, BureauCode12, BureauCode13, BureauCode14, BureauCode15, BureauSpecialUseCode, PMSAnnualStatementLine, RatingDateIndicator, BureauStatisticalUserLine, AuditReinstatementIndicator)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CreateDate AS CREATEDDATE, 
	MODIFIEDDATE, 
	logical_flag AS LOGICALINDICATOR, 
	LOGICALDELETEFLAG, 
	BUREAUSTATISTICALCODEHASHKEY, 
	BureauStatisticalCodeAKID_Out AS BUREAUSTATISTICALCODEAKID, 
	PREMIUMTRANSACTIONAKID, 
	PASSTHROUGHCHARGETRANSACTIONAKID, 
	BUREAUCODE1, 
	BUREAUCODE2, 
	BUREAUCODE3, 
	BUREAUCODE4, 
	BUREAUCODE5, 
	BUREAUCODE6, 
	BUREAUCODE7, 
	BUREAUCODE8, 
	BUREAUCODE9, 
	BUREAUCODE10, 
	BUREAUCODE11, 
	BUREAUCODE12, 
	BUREAUCODE13, 
	BUREAUCODE14, 
	BUREAUCODE15, 
	BUREAUSPECIALUSECODE, 
	PMSANNUALSTATEMENTLINE, 
	RATINGDATEINDICATOR, 
	BUREAUSTATISTICALUSERLINE, 
	AUDITREINSTATEMENTINDICATOR
	FROM EXP_Determine_BureauCode_AKID
),
SQ_PremiumTransaction_Exp AS (
	with lkp as
	(
	select *
	from
	(select *,
	ROW_NUMBER() over (partition by PolicyAKID,MajorPerilSequenceNumber,StateProvinceCode,StatisticalCoverageEffectiveDate order by CreatedDate desc,ExperienceModificationEffectiveDate desc) as rownum
	from
	(select distinct pc.PolicyAKID as PolicyAKID,
	pt.ExperienceModificationFactor as ExperienceModificationFactor,
	pt.ExperienceModificationEffectiveDate as ExperienceModificationEffectiveDate,
	sc.MajorPerilSequenceNumber as MajorPerilSequenceNumber,
	SUBSTRING(rl.StateProvinceCode,1,2) as StateProvinceCode,
	sc.StatisticalCoverageEffectiveDate as StatisticalCoverageEffectiveDate,
	cast(pt.CreatedDate as date) as CreatedDate
	from RiskLocation rl
	inner join PolicyCoverage pc
	on rl.RiskLocationAKID=pc.RiskLocationAKID
	inner join StatisticalCoverage sc
	on pc.PolicyCoverageAKID=sc.PolicyCoverageAKID
	inner join PremiumTransaction pt
	on sc.StatisticalCoverageAKID=pt.StatisticalCoverageAKID
	join v2.policy pol
	on rl.PolicyAKID=pol.pol_ak_id
	and pol.crrnt_snpsht_flag=1
	where pc.TypeBureauCode in ('WC','WP','WorkersCompensation') 
	and pt.PremiumType='D'   
	and pt.ReasonAmendedCode != 'CWO'
	and sc.ClassCode = '9898' 
	and pt.SourceSystemID = 'PMS'
	)t)t
	where rownum=1
	)
	select PremiumTransactionID,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate
	from
	(
	select pt.PremiumTransactionID, 
	lkp.ExperienceModificationFactor,
	lkp.ExperienceModificationEffectiveDate,
	wpmc1.ClassCode as ClassCode1,
	wpmc2.ClassCode as ClassCode2,
	sc.StatisticalCoverageEffectiveDate as StatisticalCoverageEffectiveDate,
	ROW_NUMBER() over (partition by pt.PremiumTransactionID order by lkp.ExperienceModificationEffectiveDate desc) as rownum,
	pt.ExperienceModificationFactor as current_ExperienceModificationFactor,
	pt.ExperienceModificationEffectiveDate as current_ExperienceModificationEffectiveDate
	from RiskLocation rl
	inner join PolicyCoverage pc
	on rl.RiskLocationAKID=pc.RiskLocationAKID
	inner join StatisticalCoverage sc
	on pc.PolicyCoverageAKID=sc.PolicyCoverageAKID
	inner join PremiumTransaction pt
	on sc.StatisticalCoverageAKID=pt.StatisticalCoverageAKID
	inner join lkp
	on lkp.PolicyAKID=pc.PolicyAKID
	and lkp.StateProvinceCode=rl.StateProvinceCode
	and lkp.MajorPerilSequenceNumber=sc.MajorPerilSequenceNumber
	and lkp.ExperienceModificationEffectiveDate<=sc.StatisticalCoverageEffectiveDate
	left join SupClassificationWorkersCompensation wpmc1
	on wpmc1.SubjectToExperienceModificationClassIndicator = 'Y'
	and wpmc1.RatingStateCode = rl.StateProvinceCode
	and wpmc1.ClassCode = sc.ClassCode
	left join SupClassificationWorkersCompensation wpmc2
	on wpmc2.SubjectToExperienceModificationClassIndicator = 'Y'
	and wpmc2.RatingStateCode = '99'
	and wpmc2.ClassCode = sc.ClassCode
	where pc.TypeBureauCode in ('WC','WP','WorkersCompensation') 
	and pt.PremiumType='D' 
	and pt.ReasonAmendedCode!='CWO'
	--and sc.ClassCode<>'9898' 
	and pt.SourceSystemID='PMS'
	) t
	where rownum = 1 and (ClassCode1 is not null or ClassCode2 is not null) 
	and (ExperienceModificationFactor<>current_ExperienceModificationFactor
	or ExperienceModificationEffectiveDate<>current_ExperienceModificationEffectiveDate)
	order by 1
),
UPD_Exp_Mod_Exp_Mod_Date AS (
	SELECT
	PremiumTransactionID, 
	ExperienceModificationFactor AS Exp_Mod_Factor, 
	ExperienceModificationEffectiveDate AS Exp_Mod_Eff_date
	FROM SQ_PremiumTransaction_Exp
),
PremiumTransaction_Update_Exp_Mod AS (
	MERGE INTO PremiumTransaction AS T
	USING UPD_Exp_Mod_Exp_Mod_Date AS S
	ON T.PremiumTransactionID = S.PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ExperienceModificationFactor = S.Exp_Mod_Factor, T.ExperienceModificationEffectiveDate = S.Exp_Mod_Eff_date
),
SQ_PMA AS (
	select c.PolicyAKID,
	c.InsuranceLine,
	a.PremiumTransactionID,
	a.PackageModificationAdjustmentGroupCode
	from PremiumTransaction a
	join BureauStatisticalCode t
	on t.PremiumTransactionAKID=a.PremiumTransactionAKID
	and t.CurrentSnapshotFlag=1
	join StatisticalCoverage b
	on a.StatisticalCoverageAKID=b.StatisticalCoverageAKID
	join PolicyCoverage c
	on c.PolicyCoverageAKID=b.PolicyCoverageAKID
	and (((c.InsuranceLine in ('GL','CF','GA')
	or c.TypeBureauCode in ('AL','AP','AN'))
	and ISNULL(a.PackageModificationAdjustmentGroupCode,'N/A')<>'N/A')
	or (c.InsuranceLine='IM' and ISNULL(a.PackageModificationAdjustmentGroupCode,'N/A')='N/A'))
	where c.PolicyAKID in (
	select D.PolicyAKID from PremiumTransaction a
	join BureauStatisticalCode b
	on a.PremiumTransactionAKID=b.PremiumTransactionAKID
	and b.CurrentSnapshotFlag=1
	and b.BureauCode1='3'
	and b.BureauCode2='6'
	and ISNULL(a.PackageModificationAdjustmentGroupCode,'N/A')='N/A'
	join StatisticalCoverage c
	on c.StatisticalCoverageAKID=a.StatisticalCoverageAKID
	join PolicyCoverage d
	on d.PolicyCoverageAKID=c.PolicyCoverageAKId
	and d.InsuranceLine='IM')
	@{pipeline().parameters.WHERE_CLAUSE_PMA}
	ORDER BY c.PolicyAKID,
	case when c.InsuranceLine='GL' then 1
	when c.InsuranceLine='CF' then 2
	when c.TypeBureauCode in ('AL','AP','AN') then 3
	when c.InsuranceLine='GA' then 4
	else 5 end,a.PremiumTransactionID
),
EXP_CalculatePMA AS (
	SELECT
	PolicyAKID AS i_PolicyAKID,
	InsuranceLine,
	PremiumTransactionID,
	PackageModificationAdjustmentGroupCode AS i_PackageModificationAdjustmentGroupCode,
	-- *INF*: IIF(i_PolicyAKID=v_prev_PolicyAKID,v_PackageModificationAdjustmentGroupCode,i_PackageModificationAdjustmentGroupCode)
	IFF(i_PolicyAKID = v_prev_PolicyAKID, v_PackageModificationAdjustmentGroupCode, i_PackageModificationAdjustmentGroupCode) AS v_PackageModificationAdjustmentGroupCode,
	i_PolicyAKID AS v_prev_PolicyAKID,
	v_PackageModificationAdjustmentGroupCode AS o_PackageModificationAdjustmentGroupCode
	FROM SQ_PMA
),
FILTRANS AS (
	SELECT
	InsuranceLine, 
	PremiumTransactionID, 
	o_PackageModificationAdjustmentGroupCode AS PackageModificationAdjustmentGroupCode
	FROM EXP_CalculatePMA
	WHERE InsuranceLine='IM'
),
UPDTRANS AS (
	SELECT
	PremiumTransactionID, 
	PackageModificationAdjustmentGroupCode
	FROM FILTRANS
),
PremiumTransaction_Update_PMA AS (
	MERGE INTO PremiumTransaction AS T
	USING UPDTRANS AS S
	ON T.PremiumTransactionID = S.PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.PackageModificationAdjustmentGroupCode = S.PackageModificationAdjustmentGroupCode
),
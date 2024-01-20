WITH
SQ_ActuarialAnalysis39Monthly_Loss AS (
	--Loss
	SELECT ird.EnterpriseGroupAbbreviation AS ent_grp_abbr,
		ird.StrategicProfitCenterAbbreviation AS spc_abbr,
		ird.InsuranceReferenceLegalEntityAbbreviation AS legal_ent_abbr,
		ird.PolicyOfferingCode AS pol_offering_code,
		ird.PolicyOfferingAbbreviation AS pol_offering_abbr,
		ird.ProductCode AS product_code,
		ird.ProductAbbreviation AS product_abbr,
		ird.InsuranceReferenceLineOfBusinessCode AS lob_code,
		ird.InsuranceReferenceLineOfBusinessAbbreviation AS lob_abbr,
		ird.InsuranceReferenceLineOfBusinessDescription AS lob_desc,
		ird.InsuranceSegmentCode AS ins_seg_code,
		ird.InsuranceSegmentDescription AS ins_seg_desc,
		pol_current.ProgramCode AS prog_code,
		pol_current.ProgramDescription AS prog_desc,
		pol_current.AssociationCode AS assn_code,
		pol_current.AssociationDescription AS assn_desc,
		ircd.CoverageSummaryCode AS cov_summ_code,
		ircd.CoverageSummaryDescription AS cov_summ_desc,
		ircd.CoverageGroupCode AS cov_grp_code,
		ircd.CoverageGroupDescription AS cov_grp_desc,
		ircd.CoverageCode AS cov_code,
		ircd.CoverageDescription AS cov_desc,
		ircd.RatedCoverageCode AS rated_cov_code,
		ircd.RatedCoverageDescription AS rated_cov_desc,
		CASE WHEN ircd.InsuranceLineCode = 'GL'
				AND PmsMajorPerilDescription = 'GL CLM-MDE' THEN 'CLAIMSMADE' WHEN ircd.InsuranceLineCode = 'GL'
				AND PmsMajorPerilDescription = 'GL OCCUR' THEN 'OCCURRENCE' WHEN ircd.InsuranceLineCode = 'GL'
				AND DctCoverageVersion = 'CLAIMSMADE' THEN 'CLAIMSMADE' WHEN ircd.InsuranceLineCode = 'GL'
				AND DctCoverageVersion = 'OCCURRENCE' THEN 'OCCURRENCE' ELSE 'N/A' END AS cov_trigger_type,
		ird.RatingPlanDescription AS rating_plan,
		cvd.TypeOfLoss AS type_of_loss,
		pol_current.pol_key AS pol_key,
		pol_current.pol_sym AS pol_sym,
		pol_current.pol_num AS pol_no,
		pol_current.pol_mod AS pol_ver,
		cd.RatingStateProvinceCode AS rating_state_code,
		lossdate.clndr_yr AS loss_year,
		lossdate.clndr_qtr AS loss_qtr,
		lossdate.clndr_month AS loss_mo,
		lossdate.clndr_yr AS Year,
		lossdate.clndr_month AS Month,
		lossmaster.clndr_qtr AS acctg_qtr,
		lossmaster.clndr_yr AS acctg_year,
		lossmaster.clndr_month AS acctg_mo,
		aed_current.ProducerCode AS producer_code,
		(CASE WHEN aed_current.AgencyEmployeeRole = 'Producer' THEN aed_current.AgencyEmployeeFirstName + ' ' + aed_current.agencyemployeelastname ELSE 'N/A' END) AS producer_name,
		cc_current.cust_num AS cust_num,
		cc_current.NAME AS insured_name,
		asl.asl_code AS asl_code,
		asl.asl_code_descript AS asl_code_desc,
		asl.sched_p_num AS sched_p_code,
		asl.sched_p_name AS sched_p_desc,
		pol_current.prim_bus_class_code AS prim_bus_class_code,
		pol_current.prim_bus_class_code_descript AS prim_bus_class_desc,
		ad_current.AssignedStateCode AS agency_state_code,
		ad_current.AgencyCode AS agency_code,
		ad_current.AgencyDoingBusinessAsName AS agency_name,
		s.RegionalSalesManagerDisplayName AS rsm_name,
		(CASE WHEN U.AssociateRole <> 'UNDERWRITER ASSISTANT' THEN U.UnderwriterDisplayName ELSE 'N/A' END) AS uw_name,
		(CASE WHEN U.AssociateRole = 'UNDERWRITER ASSISTANT' THEN U.UnderwriterDisplayName ELSE 'N/A' END) AS asst_uw_name,
		U.UnderwriterManagerDisplayName AS uw_mgr_name,
	--	LEFT(SD.LegalPrimaryAgencyCode, 2) AS prim_agency_state_code,
	--	SD.LegalPrimaryAgencyCode AS prim_agency_code,
	--	SD.AgencyDoingBusinessAsName AS prim_agency_name,
		CASE WHEN pol_current.pol_issue_code = 'N' THEN 'Y' ELSE 'N' END AS new_bus_indic,
		pol_current.state_of_domicile_code AS prim_rating_state_code,
		cc_current.sic_code AS sic_code,
		cc_current.sic_code_descript AS sic_code_desc,
		asl.sub_asl_code AS sub_asl_code,
		asl.sub_asl_code_descript AS sub_asl_code_desc,
		asl.sub_non_asl_code AS sub_non_asl_code,
		asl.sub_non_asl_code_descript AS sub_non_asl_code_desc,
		s.SalesTerritoryCodeDescription AS sal_terri_desc,
		u.UnderwritingRegionCodeDescription AS uw_region_name,
		pol_current.orig_incptn_date AS pol_orig_incptn_date,
		pol_current.pol_eff_date AS pol_eff_date,
		pol_current.pol_exp_date AS pol_exp_date,
		ird.AccountingProductAbbreviation AS acctg_prod_abbr,
		cd.PackageModificationAdjustmentGroupCode AS pma_code,
		cd.PackageModificationAdjustmentGroupDescription AS pma_desc,
		cd.IncreasedLimitGroupCode AS ilf_tbl,
		cod.claim_num AS claim_num,
		sum(VLMF.DirectLossIncurredIR) AS loss_inc,
		sum(VLMF.DirectLossPaidIR) AS loss_pd,
		sum(VLMF.DirectALAEPaidIR) AS alae_pd,
		sum(VLMF.DirectALAEIncurredIR) AS alae_inc,
	(
	CASE WHEN lossmaster.clndr_yr < 2022 THEN
	CASE WHEN
	 (ird.PolicyOfferingCode = '600'
	OR ird.InsuranceReferenceLineOfBusinessCode = '330'
	OR ird.StrategicProfitCenterCode = '5'
	OR ird.InsuranceSegmentCode = '3'
	OR (ircd.CoverageGroupCode IN ('BOILER') OR ircd.CoverageGroupCode LIKE '%TRIA'
	OR ircd.CoverageGroupDescription IN ('Earthquake', 'MCCA Surcharge')
	)
	OR ird.InsuranceReferenceLineOfBusinessCode IN ('811', '310', '312', '590', '812', '890', '900', '506' , '505' , '507')
	OR (ird.InsuranceReferenceLineOfBusinessCode = '100' AND cd.ClassCode IN ('9741', '9740'))
	OR (ird.RatingPlanAbbreviation IN ('LRARO', 'Retro'))
	OR apc.asl_prdct_code in ('200', '220')
	OR cd.ClassCode = '0174' ) THEN 'N' ELSE 'Y'
	END
	WHEN   lossmaster.clndr_yr > = 2022 AND ird.StrategicProfitCenterCode = '5' AND ird.RatingPlanAbbreviation NOT IN ('LRARO', 'Retro') THEN 'Y'
	ELSE
	CASE WHEN ird.PolicyOfferingCode = '600'
	OR ird.InsuranceSegmentCode = '3'
	OR (ircd.CoverageGroupCode IN ('BOILER')
	OR ircd.CoverageGroupCode LIKE '%TRIA'
	OR ircd.CoverageGroupDescription IN ('MCCA Surcharge')
	)
	OR ird.InsuranceReferenceLineOfBusinessCode IN ('590', '812', '890', '900', '506' , '505' , '507')
	OR (ird.RatingPlanAbbreviation IN ('LRARO', 'Retro'))
	OR apc.asl_prdct_code in ('200', '220')
	OR cd.ClassCode = '0174' THEN 'N' ELSE 'Y' END
	END
	) AS agency_contigent_ind,
		cdgl.ISOGeneralLiabilityClassGroupCode AS gl_class_grp,
		cdcp.ISOCommercialPropertyRatingGroupCode AS prop_rating_grp,
		cdcp.RateType AS prop_rate_type,
		cdcp.ISOCommercialPropertyCauseofLossGroup AS prop_col_grp,
		cdca.VehicleTypeSize AS auto_veh_type,
		cdca.BusinessUseClass AS auto_use_class,
		cdca.RadiusOfOperation AS auto_radius,
		cdca.FleetType AS auto_fleet_type,
		cdca.SecondaryClassGroup AS auto_sec_class_grp,
		cdc.IndustryGroup AS crime_ind_grp,
		cdcp.ISOSpecialCauseOfLossCategoryCode AS prop_spec_col_cat,
		isnull(bcd.BusinessSegmentCode, 'N/A') AS BusinessSegmentCode,
		isnull(bcd.StrategicBusinessGroupCode, 'N/A') AS StrategicBusinessGroupCode,
		pol_Current.edw_pol_ak_id,
		isnull(ircd.ISOMajorCrimeGroup,'N/A') AS ISOMajorCrimeGroup,
		ad_Current.AgencyStatusCode,
	--	SD.AgencyStatusCode as PrimaryAgencyStatusCode,
		ad_Current.EDWAgencyAKID,
		cd.CensusBlockGroupCountyCode,
		cd.CensusBlockGroupTractCode,
		cd.CensusBlockGroupBlockGroupCode,
		cd.Latitude,
		cd.Longitude,
		cd.RatingTerritory,
	      cod.Catalyst,
	      cod.CauseOfDamage,
	      cod.DamageCaused,
	      cod.ItemDamaged,
		cod.claim_cat_code,
	       pol_current.serv_center_support_code,
	      pol_current.QuoteChannel,
	     pol_current.QuoteChannelOrigin,
		cd.RatingStateProvinceAbbreviation AS RatingStateAbbreviation,
		 cc.mailing_zip_postal_code
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.vwLossMasterFact vlmf WITH (NOLOCK)
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_dim cod WITH (NOLOCK) ON vlmf.claim_occurrence_dim_id = cod.claim_occurrence_dim_id
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim lossdate WITH (NOLOCK) ON lossdate.clndr_date = dateadd(dd, datediff(dd, 0, cod.claim_loss_date), 0)
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim lossmaster WITH (NOLOCK) ON vlmf.loss_master_run_date_id = lossmaster.clndr_id
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailDim cd WITH (NOLOCK) ON cd.CoverageDetailDimId = vlmf.CoverageDetailDimId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim ird WITH (NOLOCK) ON ird.InsuranceReferenceDimId = VLMF.InsuranceReferenceDimId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim ircd WITH (NOLOCK) ON ircd.InsuranceReferenceCoverageDimId = VLMF.InsuranceReferenceCoverageDimId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol WITH (NOLOCK) ON pol.pol_dim_id = VLMF.pol_dim_id
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol_Current WITH (NOLOCK) ON pol.edw_pol_ak_id = pol_Current.edw_pol_ak_id
		AND pol_Current.crrnt_snpsht_flag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.AgencyEmployeeDim AED WITH (NOLOCK) ON aed.AgencyEmployeeDimID = pol_current.AgencyEmployeeDimID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.AgencyEmployeeDim aed_current ON aed.EDWAgencyEmployeeAKID = aed_current.EDWAgencyEmployeeAKID
		AND aed_current.CurrentSnapshotFlag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_dim cvd WITH (NOLOCK) ON vlmf.claimant_cov_dim_id = cvd.claimant_cov_dim_id
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwritingDivisionDim U WITH (NOLOCK) ON u.UnderwritingDivisionDimID = pol_current.UnderwritingDivisionDimId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim ad WITH (NOLOCK) ON ad.AgencyDimID = VLMF.AgencyDimID
	INNER JOIN V3.AgencyDim ad_Current WITH (NOLOCK) ON ad.EDWAgencyAKID = ad_Current.EDWAgencyAKID
		AND ad_Current.CurrentSnapshotFlag = 1
	--INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim SD WITH (NOLOCK) ON SD.AgencyCode = ad_Current.LegalPrimaryAgencyCode
	--	AND SD.CurrentSnapshotFlag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.SalesDivisionDim s WITH (NOLOCK) ON s.SalesDivisionDimID = ad_current.SalesDivisionDimId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_dim cc WITH (NOLOCK) ON cc.contract_cust_dim_id = VLMF.contract_cust_dim_id
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_dim cc_Current WITH (NOLOCK) ON cc.edw_contract_cust_ak_id = cc_Current.edw_contract_cust_ak_id
		AND cc_Current.crrnt_snpsht_flag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_dim asl WITH (NOLOCK) ON asl.asl_dim_id = VLMF.asl_dim_id
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailGeneralLiabilityDim cdgl WITH (NOLOCK) ON vlmf.CoverageDetailDimId = cdgl.CoverageDetailDimId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialPropertyDim cdcp WITH (NOLOCK) ON vlmf.CoverageDetailDimId = cdcp.CoverageDetailDimId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAutoDim cdca WITH (NOLOCK) ON vlmf.CoverageDetailDimId = cdca.CoverageDetailDimId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCrimeDim cdc WITH (NOLOCK) ON vlmf.CoverageDetailDimId = cdc.CoverageDetailDimId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.BusinessClassDim bcd WITH (NOLOCK) ON pol_current.BusinessClassDimId = bcd.BusinessClassDimId
		AND bcd.CurrentSnapshotFlag = 1
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_product_code_dim apc on apc.asl_prdct_code_dim_id=vlmf.asl_prdct_code_dim_id
	WHERE 1=1 
	@{pipeline().parameters.WHERE_CLAUSE}
	GROUP BY ird.EnterpriseGroupAbbreviation,
		ird.StrategicProfitCenterAbbreviation,
		ird.InsuranceReferenceLegalEntityAbbreviation,
		ird.PolicyOfferingCode,
		ird.PolicyOfferingAbbreviation,
		ird.ProductCode,
		ird.ProductAbbreviation,
		ird.InsuranceReferenceLineOfBusinessCode,
		ird.InsuranceReferenceLineOfBusinessAbbreviation,
		ird.InsuranceReferenceLineOfBusinessDescription,
		ird.InsuranceSegmentCode,
		ird.InsuranceSegmentDescription,
		pol_current.ProgramCode,
		pol_current.ProgramDescription,
		pol_current.AssociationCode,
		pol_current.AssociationDescription,
		ircd.CoverageSummaryCode,
		ircd.CoverageSummaryDescription,
		ircd.CoverageGroupCode,
		ircd.CoverageGroupDescription,
		ircd.CoverageCode,
		ircd.CoverageDescription,
		ircd.RatedCoverageCode,
		ircd.RatedCoverageDescription,
		cvd.TypeOfLoss,
		CASE WHEN ircd.InsuranceLineCode = 'GL'
				AND PmsMajorPerilDescription = 'GL CLM-MDE' THEN 'CLAIMSMADE' WHEN ircd.InsuranceLineCode = 'GL'
				AND PmsMajorPerilDescription = 'GL OCCUR' THEN 'OCCURRENCE' WHEN ircd.InsuranceLineCode = 'GL'
				AND DctCoverageVersion = 'CLAIMSMADE' THEN 'CLAIMSMADE' WHEN ircd.InsuranceLineCode = 'GL'
				AND DctCoverageVersion = 'OCCURRENCE' THEN 'OCCURRENCE' ELSE 'N/A' END,
		ird.RatingPlanDescription,
		pol_current.pol_key,
		pol_current.pol_sym,
		pol_current.pol_num,
		pol_current.pol_mod,
		cod.claim_num,
		pol_current.state_of_domicile_code,
		cc_current.sic_code,
		cc_current.sic_code_descript,
		asl.sub_asl_code,
		asl.sub_asl_code_descript,
		asl.sub_non_asl_code,
		asl.sub_non_asl_code_descript,
		s.SalesTerritoryCodeDescription,
		u.UnderwritingRegionCodeDescription,
		pol_current.orig_incptn_date,
		pol_current.pol_eff_date,
		pol_current.pol_exp_date,
		ird.AccountingProductAbbreviation,
		cd.PackageModificationAdjustmentGroupCode,
		cd.PackageModificationAdjustmentGroupDescription,
		cd.IncreasedLimitGroupCode,
		cd.RatingStateProvinceCode,
		lossdate.clndr_yr,
		lossdate.clndr_qtr,
		lossdate.clndr_month,
		lossdate.clndr_yr,
		lossdate.clndr_month,
		ird.RatingPlanAbbreviation,
		ird.StrategicProfitCenterCode,
		cd.ISOClassCode,
		lossmaster.clndr_qtr,
		lossmaster.clndr_yr,
		lossmaster.clndr_month,
		aed_current.ProducerCode,
		(CASE WHEN aed_current.AgencyEmployeeRole = 'Producer' THEN aed_current.AgencyEmployeeFirstName + ' ' + aed_current.agencyemployeelastname ELSE 'N/A' END),
		cc_current.cust_num,
		cc_current.NAME,
		asl.asl_code,
		asl.asl_code_descript,
		asl.sched_p_num,
		asl.sched_p_name,
		pol_current.prim_bus_class_code,
		pol_current.prim_bus_class_code_descript,
		ad_current.AssignedStateCode,
		ad_current.AgencyCode,
		ad_current.AgencyDoingBusinessAsName,
		s.RegionalSalesManagerDisplayName,
		(CASE WHEN U.AssociateRole <> 'UNDERWRITER ASSISTANT' THEN U.UnderwriterDisplayName ELSE 'N/A' END),
		(CASE WHEN U.AssociateRole = 'UNDERWRITER ASSISTANT' THEN U.UnderwriterDisplayName ELSE 'N/A' END),
		U.UnderwriterDisplayName,
		U.UnderwriterManagerDisplayName,
	--	LEFT(SD.LegalPrimaryAgencyCode, 2),
	--	SD.LegalPrimaryAgencyCode,
	--	SD.AgencyDoingBusinessAsName,
		CASE WHEN pol_current.pol_issue_code = 'N' THEN 'Y' ELSE 'N' END,
		cod.claim_num,
	(
	CASE WHEN lossmaster.clndr_yr < 2022 THEN
	CASE WHEN
	 (ird.PolicyOfferingCode = '600'
	OR ird.InsuranceReferenceLineOfBusinessCode = '330'
	OR ird.StrategicProfitCenterCode = '5'
	OR ird.InsuranceSegmentCode = '3'
	OR (ircd.CoverageGroupCode IN ('BOILER') OR ircd.CoverageGroupCode LIKE '%TRIA'
	OR ircd.CoverageGroupDescription IN ('Earthquake', 'MCCA Surcharge')
	)
	OR ird.InsuranceReferenceLineOfBusinessCode IN ('811', '310', '312', '590', '812', '890', '900', '506' , '505' , '507')
	OR (ird.InsuranceReferenceLineOfBusinessCode = '100' AND cd.ClassCode IN ('9741', '9740'))
	OR (ird.RatingPlanAbbreviation IN ('LRARO', 'Retro'))
	OR apc.asl_prdct_code in ('200', '220')
	OR cd.ClassCode = '0174' ) THEN 'N' ELSE 'Y'
	END
	WHEN   lossmaster.clndr_yr > = 2022 AND ird.StrategicProfitCenterCode = '5' AND ird.RatingPlanAbbreviation NOT IN ('LRARO', 'Retro') THEN 'Y'
	ELSE
	CASE WHEN ird.PolicyOfferingCode = '600'
	OR ird.InsuranceSegmentCode = '3'
	OR (ircd.CoverageGroupCode IN ('BOILER')
	OR ircd.CoverageGroupCode LIKE '%TRIA'
	OR ircd.CoverageGroupDescription IN ('MCCA Surcharge')
	)
	OR ird.InsuranceReferenceLineOfBusinessCode IN ('590', '812', '890', '900', '506' , '505' , '507')
	OR (ird.RatingPlanAbbreviation IN ('LRARO', 'Retro'))
	OR apc.asl_prdct_code in ('200', '220')
	OR cd.ClassCode = '0174' THEN 'N' ELSE 'Y' END
	END
	),
		cdgl.ISOGeneralLiabilityClassGroupCode,
		cdcp.ISOCommercialPropertyRatingGroupCode,
		cdcp.RateType,
		cdcp.ISOCommercialPropertyCauseofLossGroup,
		cdca.VehicleTypeSize,
		cdca.BusinessUseClass,
		cdca.RadiusOfOperation,
		cdca.FleetType,
		cdca.SecondaryClassGroup,
		cdc.IndustryGroup,
		cdcp.ISOSpecialCauseOfLossCategoryCode,
		bcd.BusinessSegmentCode,
		bcd.StrategicBusinessGroupCode,
		pol_Current.edw_pol_ak_id,
		isnull(ircd.ISOMajorCrimeGroup,'N/A'),
		ad_Current.AgencyStatusCode,
	--	SD.AgencyStatusCode,
		ad_Current.EDWAgencyAKID,
		cd.CensusBlockGroupCountyCode,
		cd.CensusBlockGroupTractCode,
		cd.CensusBlockGroupBlockGroupCode,
		cd.Latitude,
		cd.Longitude,
		cd.RatingTerritory,
	      cod.Catalyst,
	      cod.CauseOfDamage,
	      cod.DamageCaused,
	      cod.ItemDamaged,
	      cod.claim_cat_code,
	     pol_current.serv_center_support_code,
	      pol_current.QuoteChannel,
	     pol_current.QuoteChannelOrigin,
		cd.RatingStateProvinceAbbreviation,
		 cc.mailing_zip_postal_code
),
LKP_AgencyRelationship AS (
	SELECT
	Prim_Agency_Code,
	Prim_Agency_State_Code,
	prim_agency_name,
	prim_agency_status_code,
	prim_agency_state_abbr,
	prim_agency_city,
	prim_agency_zip_code,
	EDWAgencyAKID
	FROM (
		select Latest_LPAC.EDWAgencyAKId as EDWAgencyAKId ,
		  SD.agencycode as prim_agency_code,
		  Left((SD.agencycode),2) as prim_agency_state_code,
		  SD.AgencyDoingBusinessAsName as prim_agency_name,
		SD.AgencyStatusCode as prim_agency_status_code,
		SD.PhysicalStateAbbreviation as prim_agency_state_abbr,
		SD.PhysicalCity as prim_agency_city,
		SD.PhysicalZipCode as prim_agency_zip_code
		from @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim SD 
		Inner Join  (
		 Select EDWAgencyAKId,EDWLegalPrimaryAgencyAKId from 
		  (
			select EDWAgencyAKId, 
		  EDWLegalPrimaryAgencyAKId, row_number() over (partition by edwAgencyAKID order by AgencyRelationshipexpirationdate desc ) As rowNum 
		  from @{pipeline().parameters.SOURCE_TABLE_OWNER}.AgencyRelationshipCurrent
		  where AgencyRelationshipexpirationdate != '1800-01-01'
		  ) a
		  where a.rowNum = 1 
		  ) Latest_LPAC
		  on Latest_LPAC.EDWLegalPrimaryAgencyAKId=SD.EDWAgencyAKId
		  where SD.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWAgencyAKID ORDER BY Prim_Agency_Code) = 1
),
LKP_BusinessClassDim_SBG AS (
	SELECT
	BusinessSegmentDescription,
	StrategicBusinessGroupDescription,
	BusinessSegmentCode,
	StrategicBusinessGroupCode
	FROM (
		SELECT distinct 
		BusinessSegmentDescription as BusinessSegmentDescription, 
		StrategicBusinessGroupDescription as StrategicBusinessGroupDescription, 
		BusinessSegmentCode as BusinessSegmentCode, 
		StrategicBusinessGroupCode as StrategicBusinessGroupCode 
		FROM BusinessClassDim
		where
		CurrentSnapshotFlag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY BusinessSegmentCode,StrategicBusinessGroupCode ORDER BY BusinessSegmentDescription) = 1
),
LKP_PolicyCurrentStatusDim AS (
	SELECT
	PolicyCancellationDate,
	EDWPolicyAKId
	FROM (
		SELECT P.EDWPolicyAKId as EDWPolicyAKId, 
		P.PolicyCancellationDate as PolicyCancellationDate
		FROM PolicyCurrentStatusDim P
		WHERE P.RunDate = (SELECT max(Pl.RunDate) FROM PolicyCurrentStatusDim Pl WHERE P.EDWPolicyAKId = Pl.EDWPolicyAKId) 
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWPolicyAKId ORDER BY PolicyCancellationDate) = 1
),
EXP_Default AS (
	SELECT
	SQ_ActuarialAnalysis39Monthly_Loss.ent_grp_abbr,
	SQ_ActuarialAnalysis39Monthly_Loss.spc_abbr,
	v_pc_abbr AS pc_abbr,
	-- *INF*: DECODE(TRUE,
	-- ins_seg_code = '3','Pool',
	-- pol_offering_code = '600','Bonds',
	-- spc_abbr = 'NSI','Specialty',
	-- spc_abbr = 'WB - PL','Personal',
	-- spc_abbr = 'WB - CL','Commercial',
	-- spc_abbr = 'NSI','Specialty',
	-- spc_abbr)
	DECODE(
	    TRUE,
	    ins_seg_code = '3', 'Pool',
	    pol_offering_code = '600', 'Bonds',
	    spc_abbr = 'NSI', 'Specialty',
	    spc_abbr = 'WB - PL', 'Personal',
	    spc_abbr = 'WB - CL', 'Commercial',
	    spc_abbr = 'NSI', 'Specialty',
	    spc_abbr
	) AS v_pc_abbr,
	SQ_ActuarialAnalysis39Monthly_Loss.legal_ent_abbr,
	SQ_ActuarialAnalysis39Monthly_Loss.pol_offering_code,
	SQ_ActuarialAnalysis39Monthly_Loss.pol_offering_abbr,
	SQ_ActuarialAnalysis39Monthly_Loss.product_code,
	SQ_ActuarialAnalysis39Monthly_Loss.product_abbr,
	SQ_ActuarialAnalysis39Monthly_Loss.lob_code,
	SQ_ActuarialAnalysis39Monthly_Loss.lob_abbr,
	SQ_ActuarialAnalysis39Monthly_Loss.lob_desc,
	SQ_ActuarialAnalysis39Monthly_Loss.ins_seg_code,
	SQ_ActuarialAnalysis39Monthly_Loss.ins_seg_desc,
	SQ_ActuarialAnalysis39Monthly_Loss.prog_code,
	SQ_ActuarialAnalysis39Monthly_Loss.prog_desc,
	SQ_ActuarialAnalysis39Monthly_Loss.assn_code,
	SQ_ActuarialAnalysis39Monthly_Loss.assn_desc,
	SQ_ActuarialAnalysis39Monthly_Loss.cov_summ_code,
	SQ_ActuarialAnalysis39Monthly_Loss.cov_summ_desc,
	SQ_ActuarialAnalysis39Monthly_Loss.cov_grp_code,
	SQ_ActuarialAnalysis39Monthly_Loss.cov_grp_desc,
	SQ_ActuarialAnalysis39Monthly_Loss.cov_code,
	SQ_ActuarialAnalysis39Monthly_Loss.cov_desc,
	SQ_ActuarialAnalysis39Monthly_Loss.cov_trigger_type,
	SQ_ActuarialAnalysis39Monthly_Loss.rating_plan,
	SQ_ActuarialAnalysis39Monthly_Loss.type_of_loss AS i_type_of_loss,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_type_of_loss))
	UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_type_of_loss)) AS type_of_loss,
	SQ_ActuarialAnalysis39Monthly_Loss.pol_key,
	SQ_ActuarialAnalysis39Monthly_Loss.pol_sym,
	SQ_ActuarialAnalysis39Monthly_Loss.pol_no,
	SQ_ActuarialAnalysis39Monthly_Loss.pol_ver,
	SQ_ActuarialAnalysis39Monthly_Loss.rating_state_code,
	SQ_ActuarialAnalysis39Monthly_Loss.loss_year,
	SQ_ActuarialAnalysis39Monthly_Loss.loss_qtr,
	SQ_ActuarialAnalysis39Monthly_Loss.loss_mo,
	SQ_ActuarialAnalysis39Monthly_Loss.year,
	SQ_ActuarialAnalysis39Monthly_Loss.month,
	SQ_ActuarialAnalysis39Monthly_Loss.acctg_qtr,
	SQ_ActuarialAnalysis39Monthly_Loss.acctg_year,
	SQ_ActuarialAnalysis39Monthly_Loss.acctg_mo,
	SQ_ActuarialAnalysis39Monthly_Loss.producer_code,
	SQ_ActuarialAnalysis39Monthly_Loss.producer_name,
	SQ_ActuarialAnalysis39Monthly_Loss.cust_num,
	SQ_ActuarialAnalysis39Monthly_Loss.insured_name,
	SQ_ActuarialAnalysis39Monthly_Loss.asl_code,
	SQ_ActuarialAnalysis39Monthly_Loss.asl_code_desc,
	SQ_ActuarialAnalysis39Monthly_Loss.sched_p_code,
	SQ_ActuarialAnalysis39Monthly_Loss.sched_p_desc,
	SQ_ActuarialAnalysis39Monthly_Loss.BusinessSegmentCode AS bus_seg_code,
	LKP_BusinessClassDim_SBG.BusinessSegmentDescription AS bus_seg_desc,
	SQ_ActuarialAnalysis39Monthly_Loss.prim_bus_class_code,
	SQ_ActuarialAnalysis39Monthly_Loss.prim_bus_class_desc,
	SQ_ActuarialAnalysis39Monthly_Loss.agency_state_code,
	SQ_ActuarialAnalysis39Monthly_Loss.agency_code,
	SQ_ActuarialAnalysis39Monthly_Loss.agency_name,
	SQ_ActuarialAnalysis39Monthly_Loss.rsm_name,
	SQ_ActuarialAnalysis39Monthly_Loss.uw_name,
	SQ_ActuarialAnalysis39Monthly_Loss.asst_uw_name,
	SQ_ActuarialAnalysis39Monthly_Loss.uw_mgr_name,
	LKP_AgencyRelationship.Prim_Agency_State_Code AS i_prim_agency_state_code,
	LKP_AgencyRelationship.Prim_Agency_Code AS i_prim_agency_code,
	LKP_AgencyRelationship.prim_agency_name AS i_prim_agency_name,
	-- *INF*: IIF(ISNULL(i_prim_agency_state_code),'NA',i_prim_agency_state_code)
	IFF(i_prim_agency_state_code IS NULL, 'NA', i_prim_agency_state_code) AS v_prim_agency_state_code,
	-- *INF*: IIF(ISNULL(i_prim_agency_code),'N/A',i_prim_agency_code)
	IFF(i_prim_agency_code IS NULL, 'N/A', i_prim_agency_code) AS v_prim_agency_code,
	-- *INF*: IIF(ISNULL(i_prim_agency_name),'N/A',i_prim_agency_name)
	IFF(i_prim_agency_name IS NULL, 'N/A', i_prim_agency_name) AS v_prim_agency_name,
	v_prim_agency_state_code AS o_prim_agency_state_code,
	v_prim_agency_code AS o_prim_agency_code,
	v_prim_agency_name AS o_prim_agency_name,
	SQ_ActuarialAnalysis39Monthly_Loss.new_bus_indic,
	SQ_ActuarialAnalysis39Monthly_Loss.prim_rating_state_code,
	SQ_ActuarialAnalysis39Monthly_Loss.sic_code,
	SQ_ActuarialAnalysis39Monthly_Loss.sic_code_desc,
	SQ_ActuarialAnalysis39Monthly_Loss.sub_asl_code,
	SQ_ActuarialAnalysis39Monthly_Loss.sub_asl_code_desc,
	SQ_ActuarialAnalysis39Monthly_Loss.sub_non_asl_code,
	SQ_ActuarialAnalysis39Monthly_Loss.sub_non_asl_code_desc,
	SQ_ActuarialAnalysis39Monthly_Loss.sal_terri_desc,
	SQ_ActuarialAnalysis39Monthly_Loss.uw_region_name,
	SQ_ActuarialAnalysis39Monthly_Loss.pol_orig_incptn_date,
	SQ_ActuarialAnalysis39Monthly_Loss.pol_eff_date,
	SQ_ActuarialAnalysis39Monthly_Loss.pol_exp_date,
	LKP_PolicyCurrentStatusDim.PolicyCancellationDate AS pol_canc_date,
	SQ_ActuarialAnalysis39Monthly_Loss.acctg_prod_abbr,
	SQ_ActuarialAnalysis39Monthly_Loss.pma_code,
	SQ_ActuarialAnalysis39Monthly_Loss.pma_desc,
	SQ_ActuarialAnalysis39Monthly_Loss.ilf_tbl,
	SQ_ActuarialAnalysis39Monthly_Loss.claim_num,
	SQ_ActuarialAnalysis39Monthly_Loss.loss_inc,
	SQ_ActuarialAnalysis39Monthly_Loss.loss_pd,
	SQ_ActuarialAnalysis39Monthly_Loss.alae_pd,
	SQ_ActuarialAnalysis39Monthly_Loss.alae_inc,
	SQ_ActuarialAnalysis39Monthly_Loss.agency_contigent_ind,
	-- *INF*: LAST_DAY(ADD_TO_DATE(ROUND(SYSDATE,'DD'), 'MM', -1))
	LAST_DAY(DATEADD(MONTH,- 1,ROUND(CURRENT_TIMESTAMP, 'DD'))) AS o_last_booked_date,
	0 AS o_pass_thru_amt,
	0 AS o_div_pay_amt,
	0 AS o_dep,
	0 AS o_dwp,
	0 AS o_subj_dep,
	0 AS o_subj_dwp,
	0 AS o_other_dep,
	0 AS o_other_dwp,
	0 AS o_expmod_dep,
	0 AS o_expmod_dwp,
	0 AS o_schmod_dep,
	0 AS o_schmod_dwp,
	0 AS o_ph_div_payable,
	SQ_ActuarialAnalysis39Monthly_Loss.gl_class_grp AS i_gl_class_grp,
	SQ_ActuarialAnalysis39Monthly_Loss.prop_rating_group AS i_prop_rating_group,
	SQ_ActuarialAnalysis39Monthly_Loss.prop_rate_type AS i_prop_rate_type,
	SQ_ActuarialAnalysis39Monthly_Loss.prop_col_grp AS i_prop_col_grp,
	SQ_ActuarialAnalysis39Monthly_Loss.auto_veh_type AS i_auto_veh_type,
	SQ_ActuarialAnalysis39Monthly_Loss.auto_use_class AS i_auto_use_class,
	SQ_ActuarialAnalysis39Monthly_Loss.auto_radius AS i_auto_radius,
	SQ_ActuarialAnalysis39Monthly_Loss.auto_fleet_type AS i_auto_fleet_type,
	SQ_ActuarialAnalysis39Monthly_Loss.auto_sec_class_grp AS i_auto_sec_class_grp,
	SQ_ActuarialAnalysis39Monthly_Loss.crime_ind_grp AS i_crime_ind_grp,
	SQ_ActuarialAnalysis39Monthly_Loss.prop_spec_col_cat AS i_prop_spec_col_cat,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_gl_class_grp))
	UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_gl_class_grp)) AS gl_class_grp,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_prop_rating_group))
	UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_prop_rating_group)) AS prop_rating_group,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_prop_rate_type))
	UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_prop_rate_type)) AS prop_rate_type,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_prop_col_grp))
	UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_prop_col_grp)) AS prop_col_grp,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_auto_veh_type))
	UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_auto_veh_type)) AS auto_veh_type,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_auto_use_class))
	UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_auto_use_class)) AS auto_use_class,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_auto_radius))
	UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_auto_radius)) AS auto_radius,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_auto_fleet_type))
	UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_auto_fleet_type)) AS auto_fleet_type,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_auto_sec_class_grp))
	UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_auto_sec_class_grp)) AS auto_sec_class_grp,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_crime_ind_grp))
	UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_crime_ind_grp)) AS crime_ind_grp,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_prop_spec_col_cat))
	UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_prop_spec_col_cat)) AS prop_spec_col_cat,
	SQ_ActuarialAnalysis39Monthly_Loss.StrategicBusinessGroupCode AS sbg_code,
	LKP_BusinessClassDim_SBG.StrategicBusinessGroupDescription AS sbg_desc,
	SQ_ActuarialAnalysis39Monthly_Loss.ISOMajorCrimeGroup,
	SQ_ActuarialAnalysis39Monthly_Loss.AgencyStatusCode AS i_AgencyStatusCode,
	-- *INF*: IIF(ISNULL(i_AgencyStatusCode),'0',i_AgencyStatusCode)
	IFF(i_AgencyStatusCode IS NULL, '0', i_AgencyStatusCode) AS v_AgencyStatusCode,
	v_AgencyStatusCode AS o_AgencyStatusCode,
	LKP_AgencyRelationship.prim_agency_status_code AS i_PrimaryAgencyStatusCode,
	-- *INF*: IIF(ISNULL(i_PrimaryAgencyStatusCode),'0',i_PrimaryAgencyStatusCode)
	IFF(i_PrimaryAgencyStatusCode IS NULL, '0', i_PrimaryAgencyStatusCode) AS v_PrimaryAgencyStatusCode,
	i_PrimaryAgencyStatusCode AS o_PrimaryAgencyStatusCode,
	'Loss' AS o_Rec_Type,
	acctg_year*10+5 AS o_year_rt,
	SQ_ActuarialAnalysis39Monthly_Loss.rated_cov_code,
	SQ_ActuarialAnalysis39Monthly_Loss.rated_cov_desc,
	SQ_ActuarialAnalysis39Monthly_Loss.CensusBlockGroupCountyCode AS i_CensusBlockGroupCountyCode,
	-- *INF*: IIF(ISNULL(i_CensusBlockGroupCountyCode),'N/A',i_CensusBlockGroupCountyCode)
	IFF(i_CensusBlockGroupCountyCode IS NULL, 'N/A', i_CensusBlockGroupCountyCode) AS v_CensusBlockGroupCountyCode,
	v_CensusBlockGroupCountyCode AS o_CensusBlockGroupCountyCode,
	SQ_ActuarialAnalysis39Monthly_Loss.CensusBlockGroupTractCode AS i_CensusBlockGroupTractCode,
	-- *INF*: IIF(ISNULL(i_CensusBlockGroupTractCode),'N/A',i_CensusBlockGroupTractCode)
	IFF(i_CensusBlockGroupTractCode IS NULL, 'N/A', i_CensusBlockGroupTractCode) AS v_CensusBlockGroupTractCode,
	v_CensusBlockGroupTractCode AS o_CensusBlockGroupTractCode,
	SQ_ActuarialAnalysis39Monthly_Loss.CensusBlockGroupBlockGroupCode AS i_CensusBlockGroupBlockGroupCode,
	-- *INF*: IIF(ISNULL(i_CensusBlockGroupBlockGroupCode),'N/A',i_CensusBlockGroupBlockGroupCode)
	IFF(i_CensusBlockGroupBlockGroupCode IS NULL, 'N/A', i_CensusBlockGroupBlockGroupCode) AS v_CensusBlockGroupBlockGroupCode,
	v_CensusBlockGroupBlockGroupCode AS o_CensusBlockGroupBlockGroupCode,
	SQ_ActuarialAnalysis39Monthly_Loss.Latitude AS i_Latitude,
	-- *INF*: IIF(ISNULL(i_Latitude),0.00,i_Latitude)
	IFF(i_Latitude IS NULL, 0.00, i_Latitude) AS v_Latitude,
	v_Latitude AS o_Latitude,
	SQ_ActuarialAnalysis39Monthly_Loss.Longitude AS i_Longitude,
	-- *INF*: IIF(ISNULL(i_Longitude),0.00,i_Longitude)
	IFF(i_Longitude IS NULL, 0.00, i_Longitude) AS v_Longitude,
	v_Longitude AS o_Longitude,
	SQ_ActuarialAnalysis39Monthly_Loss.RatingTerritory AS i_RatingTerritory,
	-- *INF*: SUBSTR(IIF(ISNULL(i_RatingTerritory),'N/A',i_RatingTerritory),0 ,3)
	SUBSTR(
	    IFF(
	        i_RatingTerritory IS NULL, 'N/A', i_RatingTerritory
	    ), 0, 3) AS v_RatingTerritory,
	v_RatingTerritory AS o_RatingTerritory,
	SQ_ActuarialAnalysis39Monthly_Loss.Catalyst AS i_Catalyst,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_Catalyst)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_Catalyst) AS o_Catalyst,
	SQ_ActuarialAnalysis39Monthly_Loss.CauseOfDamage AS i_CauseOfDamage,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_CauseOfDamage)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_CauseOfDamage) AS o_CauseOfDamage,
	SQ_ActuarialAnalysis39Monthly_Loss.DamageCaused AS i_DamageCaused,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_DamageCaused)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_DamageCaused) AS o_DamageCaused,
	SQ_ActuarialAnalysis39Monthly_Loss.ItemDamaged AS i_ItemDamaged,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ItemDamaged)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_ItemDamaged) AS o_ItemDamaged,
	SQ_ActuarialAnalysis39Monthly_Loss.claim_cat_code,
	SQ_ActuarialAnalysis39Monthly_Loss.serv_center_support_code,
	SQ_ActuarialAnalysis39Monthly_Loss.QuoteChannel AS i_QuoteChannel,
	-- *INF*: iif(isnull(i_QuoteChannel) , 'N/A' , i_QuoteChannel)
	IFF(i_QuoteChannel IS NULL, 'N/A', i_QuoteChannel) AS v_QuoteChannel,
	v_QuoteChannel AS o_QuoteChannel,
	SQ_ActuarialAnalysis39Monthly_Loss.QuoteChannelOrigin,
	-- *INF*: iif(isnull(QuoteChannelOrigin) , 'N/A' , QuoteChannelOrigin)
	IFF(QuoteChannelOrigin IS NULL, 'N/A', QuoteChannelOrigin) AS v_QuoteChannelOrigin,
	v_QuoteChannelOrigin AS o_QuoteChannelOrigin,
	'0' AS o_default_value,
	SQ_ActuarialAnalysis39Monthly_Loss.RatingStateAbbreviation AS i_RatingStateAbbreviation,
	-- *INF*: IIF(ISNULL(i_RatingStateAbbreviation),'N/A',i_RatingStateAbbreviation)
	IFF(i_RatingStateAbbreviation IS NULL, 'N/A', i_RatingStateAbbreviation) AS v_RatingStateAbbreviation,
	v_RatingStateAbbreviation AS o_RatingStateAbbreviation,
	SQ_ActuarialAnalysis39Monthly_Loss.mailing_zip_postal_code,
	-- *INF*: MD5(
	-- :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(ent_grp_abbr))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(spc_abbr))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_pc_abbr))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(legal_ent_abbr))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(pol_offering_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(pol_offering_abbr))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(product_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(product_abbr))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(lob_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(lob_abbr))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(lob_desc))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(ins_seg_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(ins_seg_desc))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(prog_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(prog_desc))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(assn_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(assn_desc))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(cov_summ_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(cov_summ_desc))
	-- 
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(cov_grp_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(cov_grp_desc))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(cov_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(cov_desc))
	-- 
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(cov_trigger_type))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(rating_plan))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_type_of_loss))
	-- 
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(pol_key))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(pol_sym))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(pol_no))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(pol_ver))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(rating_state_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(loss_year))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(loss_qtr))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(loss_mo))
	-- 
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(year))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(month))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(acctg_qtr))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(acctg_year))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(acctg_mo))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(producer_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(producer_name))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(cust_num))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(insured_name))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(asl_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(asl_code_desc))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(sched_p_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(sched_p_desc))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(bus_seg_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(bus_seg_desc))
	-- 
	-- ---wreq-10769 getting seg code,desc,sbg from lkp businessclassdim
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(sbg_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(sbg_desc))
	-- 
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(prim_bus_class_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(prim_bus_class_desc))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(agency_state_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(agency_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(agency_name))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(rsm_name))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(uw_name))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(asst_uw_name))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(uw_mgr_name))
	-- 
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_prim_agency_state_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_prim_agency_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_prim_agency_name))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(new_bus_indic))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(prim_rating_state_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(sic_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(sic_code_desc))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(sub_asl_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(sub_asl_code_desc))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(sub_non_asl_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(sub_non_asl_code_desc))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(sal_terri_desc))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(uw_region_name))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(pol_orig_incptn_date ))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(pol_eff_date))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(pol_exp_date))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(pol_canc_date))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(acctg_prod_abbr))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(pma_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(pma_desc))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(ilf_tbl))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(claim_num))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(TO_CHAR(LAST_DAY(ADD_TO_DATE(ROUND(SYSDATE,'DD'), 'MM', -1)),'YYYY-MM-DD'))
	-- || '^' || '0.00'         --pass_thru_amt
	-- || '^' || '0.00'
	-- || '^' || '0.00'
	-- || '^' || '0.00'
	-- || '^' || '0.00'
	-- || '^' || '0.00'
	-- || '^' || '0.00'
	-- || '^' || '0.00'
	-- || '^' || '0.00'
	-- || '^' || '0.00'
	-- || '^' || '0.00'
	-- || '^' || '0.00'
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(ROUND(loss_inc,2)))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(ROUND(loss_pd,2)))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(ROUND(alae_pd,2)))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(ROUND(alae_inc,2)))
	-- || '^' || '0.00'
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(agency_contigent_ind))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(agency_contigent_ind))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_gl_class_grp))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_prop_rating_group))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_prop_rate_type))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_prop_col_grp))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_auto_veh_type))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_auto_use_class))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_auto_radius))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_auto_fleet_type))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_auto_sec_class_grp))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_crime_ind_grp))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_prop_spec_col_cat))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(ISOMajorCrimeGroup))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_AgencyStatusCode))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_PrimaryAgencyStatusCode))
	-- || '^' || 'Loss'
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(year*10+5))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(rated_cov_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(rated_cov_desc))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(v_CensusBlockGroupCountyCode)
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(v_CensusBlockGroupTractCode)
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(v_CensusBlockGroupBlockGroupCode)
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_Longitude))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_Latitude))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(v_RatingTerritory)
	-- || '^' ||:UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_Catalyst))
	-- || '^' ||:UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_CauseOfDamage))
	-- || '^' ||:UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_DamageCaused))
	-- || '^' ||:UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(i_ItemDamaged))
	-- || '^' ||:UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(claim_cat_code))
	-- || '^' ||:UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(serv_center_support_code))
	-- || '^' ||:UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_QuoteChannel))
	-- || '^' ||:UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_QuoteChannelOrigin))
	-- || '^' ||:UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_RatingStateAbbreviation))
	-- || '^' ||:UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(mailing_zip_postal_code))
	-- )
	MD5(UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(ent_grp_abbr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(spc_abbr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_pc_abbr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(legal_ent_abbr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_offering_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_offering_abbr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(product_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(product_abbr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(lob_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(lob_abbr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(lob_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(ins_seg_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(ins_seg_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(prog_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(prog_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(assn_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(assn_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(cov_summ_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(cov_summ_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(cov_grp_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(cov_grp_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(cov_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(cov_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(cov_trigger_type)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(rating_plan)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_type_of_loss)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_key)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_sym)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_no)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_ver)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(rating_state_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(loss_year)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(loss_qtr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(loss_mo)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(year)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(month)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(acctg_qtr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(acctg_year)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(acctg_mo)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(producer_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(producer_name)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(cust_num)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(insured_name)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(asl_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(asl_code_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sched_p_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sched_p_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(bus_seg_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(bus_seg_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sbg_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sbg_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(prim_bus_class_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(prim_bus_class_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(agency_state_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(agency_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(agency_name)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(rsm_name)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(uw_name)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(asst_uw_name)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(uw_mgr_name)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_prim_agency_state_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_prim_agency_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_prim_agency_name)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(new_bus_indic)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(prim_rating_state_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sic_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sic_code_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sub_asl_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sub_asl_code_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sub_non_asl_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sub_non_asl_code_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sal_terri_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(uw_region_name)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_orig_incptn_date)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_eff_date)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_exp_date)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_canc_date)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(acctg_prod_abbr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pma_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pma_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(ilf_tbl)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(claim_num)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(TO_CHAR(LAST_DAY(DATEADD(MONTH,- 1,ROUND(CURRENT_TIMESTAMP, 'DD'))), 'YYYY-MM-DD')) || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(ROUND(loss_inc, 2))) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(ROUND(loss_pd, 2))) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(ROUND(alae_pd, 2))) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(ROUND(alae_inc, 2))) || '^' || '0.00' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(agency_contigent_ind)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(agency_contigent_ind)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_gl_class_grp)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_prop_rating_group)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_prop_rate_type)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_prop_col_grp)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_auto_veh_type)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_auto_use_class)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_auto_radius)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_auto_fleet_type)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_auto_sec_class_grp)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_crime_ind_grp)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_prop_spec_col_cat)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(ISOMajorCrimeGroup)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_AgencyStatusCode)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_PrimaryAgencyStatusCode)) || '^' || 'Loss' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(year * 10 + 5)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(rated_cov_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(rated_cov_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(v_CensusBlockGroupCountyCode) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(v_CensusBlockGroupTractCode) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(v_CensusBlockGroupBlockGroupCode) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_Longitude)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_Latitude)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(v_RatingTerritory) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_Catalyst)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_CauseOfDamage)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_DamageCaused)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(i_ItemDamaged)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(claim_cat_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(serv_center_support_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_QuoteChannel)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_QuoteChannelOrigin)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_RatingStateAbbreviation)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(mailing_zip_postal_code))) AS v_HashKey,
	v_HashKey AS o_HashKey
	FROM SQ_ActuarialAnalysis39Monthly_Loss
	LEFT JOIN LKP_AgencyRelationship
	ON LKP_AgencyRelationship.EDWAgencyAKID = SQ_ActuarialAnalysis39Monthly_Loss.EDWAgencyAKID
	LEFT JOIN LKP_BusinessClassDim_SBG
	ON LKP_BusinessClassDim_SBG.BusinessSegmentCode = SQ_ActuarialAnalysis39Monthly_Loss.BusinessSegmentCode AND LKP_BusinessClassDim_SBG.StrategicBusinessGroupCode = SQ_ActuarialAnalysis39Monthly_Loss.StrategicBusinessGroupCode
	LEFT JOIN LKP_PolicyCurrentStatusDim
	ON LKP_PolicyCurrentStatusDim.EDWPolicyAKId = SQ_ActuarialAnalysis39Monthly_Loss.edw_pol_ak_id
),
LKP_AA39Monthly AS (
	SELECT
	HashKey,
	in_HashKey
	FROM (
		select HashKey  as HashKey
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.@{pipeline().parameters.TARGET_TABLE_NAME} 
		where Rec_Type ='Loss'
		@{pipeline().parameters.WHERE_CLAUSE_LKP}--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY HashKey ORDER BY HashKey) = 1
),
EXP_Detect AS (
	SELECT
	LKP_AA39Monthly.HashKey AS lkp_HashKey,
	EXP_Default.ent_grp_abbr,
	EXP_Default.spc_abbr,
	EXP_Default.pc_abbr,
	EXP_Default.legal_ent_abbr,
	EXP_Default.pol_offering_code,
	EXP_Default.pol_offering_abbr,
	EXP_Default.product_code,
	EXP_Default.product_abbr,
	EXP_Default.lob_code,
	EXP_Default.lob_abbr,
	EXP_Default.lob_desc,
	EXP_Default.ins_seg_code,
	EXP_Default.ins_seg_desc,
	EXP_Default.prog_code,
	EXP_Default.prog_desc,
	EXP_Default.assn_code,
	EXP_Default.assn_desc,
	EXP_Default.cov_summ_code,
	EXP_Default.cov_summ_desc,
	EXP_Default.cov_grp_code,
	EXP_Default.cov_grp_desc,
	EXP_Default.cov_code,
	EXP_Default.cov_desc,
	EXP_Default.cov_trigger_type,
	EXP_Default.rating_plan,
	EXP_Default.type_of_loss,
	EXP_Default.pol_key,
	EXP_Default.pol_sym,
	EXP_Default.pol_no,
	EXP_Default.pol_ver,
	EXP_Default.rating_state_code,
	EXP_Default.loss_year,
	EXP_Default.loss_qtr,
	EXP_Default.loss_mo,
	EXP_Default.year,
	EXP_Default.month,
	EXP_Default.acctg_qtr,
	EXP_Default.acctg_year,
	EXP_Default.acctg_mo,
	EXP_Default.producer_code,
	EXP_Default.producer_name,
	EXP_Default.cust_num,
	EXP_Default.insured_name,
	EXP_Default.asl_code,
	EXP_Default.asl_code_desc,
	EXP_Default.sched_p_code,
	EXP_Default.sched_p_desc,
	EXP_Default.bus_seg_code,
	EXP_Default.bus_seg_desc,
	EXP_Default.prim_bus_class_code,
	EXP_Default.prim_bus_class_desc,
	EXP_Default.agency_state_code,
	EXP_Default.agency_code,
	EXP_Default.agency_name,
	EXP_Default.rsm_name,
	EXP_Default.uw_name,
	EXP_Default.asst_uw_name,
	EXP_Default.uw_mgr_name,
	EXP_Default.o_prim_agency_state_code AS prim_agency_state_code,
	EXP_Default.o_prim_agency_code AS prim_agency_code,
	EXP_Default.o_prim_agency_name AS prim_agency_name,
	EXP_Default.new_bus_indic,
	EXP_Default.prim_rating_state_code,
	EXP_Default.sic_code,
	EXP_Default.sic_code_desc,
	EXP_Default.sub_asl_code,
	EXP_Default.sub_asl_code_desc,
	EXP_Default.sub_non_asl_code,
	EXP_Default.sub_non_asl_code_desc,
	EXP_Default.sal_terri_desc,
	EXP_Default.uw_region_name,
	EXP_Default.pol_orig_incptn_date,
	EXP_Default.pol_eff_date,
	EXP_Default.pol_exp_date,
	EXP_Default.pol_canc_date,
	EXP_Default.acctg_prod_abbr,
	EXP_Default.pma_code,
	EXP_Default.pma_desc,
	EXP_Default.ilf_tbl,
	EXP_Default.claim_num,
	EXP_Default.o_last_booked_date AS last_booked_date,
	EXP_Default.o_pass_thru_amt AS pass_thru_amt,
	EXP_Default.o_div_pay_amt AS div_pay_amt,
	EXP_Default.o_dep AS dep,
	EXP_Default.o_dwp AS dwp,
	EXP_Default.o_subj_dep AS subj_dep,
	EXP_Default.o_subj_dwp AS subj_dwp,
	EXP_Default.o_other_dep AS other_dep,
	EXP_Default.o_other_dwp AS other_dwp,
	EXP_Default.o_expmod_dep AS expmod_dep,
	EXP_Default.o_expmod_dwp AS expmod_dwp,
	EXP_Default.o_schmod_dep AS schmod_dep,
	EXP_Default.o_schmod_dwp AS schmod_dwp,
	EXP_Default.loss_inc,
	EXP_Default.loss_pd,
	EXP_Default.alae_pd,
	EXP_Default.alae_inc,
	EXP_Default.o_ph_div_payable AS ph_div_payable,
	EXP_Default.agency_contigent_ind,
	EXP_Default.gl_class_grp,
	EXP_Default.prop_rating_group,
	EXP_Default.prop_rate_type,
	EXP_Default.prop_col_grp,
	EXP_Default.auto_veh_type,
	EXP_Default.auto_use_class,
	EXP_Default.auto_radius,
	EXP_Default.auto_fleet_type,
	EXP_Default.auto_sec_class_grp,
	EXP_Default.crime_ind_grp,
	EXP_Default.prop_spec_col_cat,
	EXP_Default.o_Rec_Type AS Rec_Type,
	EXP_Default.o_year_rt AS year_rt,
	-- *INF*: DECODE(true,ISNULL(lkp_HashKey),'INSERT','NOCHANGE')
	-- 
	-- 
	DECODE(
	    true,
	    lkp_HashKey IS NULL, 'INSERT',
	    'NOCHANGE'
	) AS v_ChangeFlag,
	v_ChangeFlag AS o_ChangeFlag,
	EXP_Default.sbg_code,
	EXP_Default.sbg_desc,
	EXP_Default.ISOMajorCrimeGroup,
	EXP_Default.o_AgencyStatusCode AS AgencyStatusCode,
	EXP_Default.o_PrimaryAgencyStatusCode AS PrimaryAgencyStatusCode,
	EXP_Default.rated_cov_code,
	EXP_Default.rated_cov_desc,
	EXP_Default.o_CensusBlockGroupCountyCode AS CensusBlockGroupCountyCode,
	EXP_Default.o_CensusBlockGroupTractCode AS CensusBlockGroupTractCode,
	EXP_Default.o_CensusBlockGroupBlockGroupCode AS CensusBlockGroupBlockGroupCode,
	EXP_Default.o_Latitude AS Latitude,
	EXP_Default.o_Longitude AS Longitude,
	EXP_Default.o_RatingTerritory AS RatingTerritory,
	EXP_Default.o_Catalyst AS Catalyst,
	EXP_Default.o_CauseOfDamage AS CauseOfDamage,
	EXP_Default.o_DamageCaused AS DamageCaused,
	EXP_Default.o_ItemDamaged AS ItemDamaged,
	EXP_Default.claim_cat_code,
	EXP_Default.serv_center_support_code,
	EXP_Default.o_default_value AS CustomerInforceInd,
	EXP_Default.o_default_value AS CustomerProductInforceInd,
	EXP_Default.o_QuoteChannel AS QuoteChannel,
	EXP_Default.o_QuoteChannelOrigin AS QuoteChannelOrigin,
	EXP_Default.o_RatingStateAbbreviation AS RatingStateAbbreviation,
	EXP_Default.mailing_zip_postal_code,
	EXP_Default.o_HashKey AS HashKey
	FROM EXP_Default
	LEFT JOIN LKP_AA39Monthly
	ON LKP_AA39Monthly.HashKey = EXP_Default.o_HashKey
),
RTR_Insert AS (
	SELECT
	ent_grp_abbr,
	spc_abbr,
	pc_abbr,
	legal_ent_abbr,
	pol_offering_code,
	pol_offering_abbr,
	product_code,
	product_abbr,
	lob_code,
	lob_abbr,
	lob_desc,
	ins_seg_code,
	ins_seg_desc,
	prog_code,
	prog_desc,
	assn_code,
	assn_desc,
	cov_summ_code,
	cov_summ_desc,
	cov_grp_code,
	cov_grp_desc,
	cov_code,
	cov_desc,
	cov_trigger_type,
	rating_plan,
	type_of_loss,
	pol_key,
	pol_sym,
	pol_no,
	pol_ver,
	rating_state_code,
	loss_year,
	loss_qtr,
	loss_mo,
	year,
	month,
	acctg_qtr,
	acctg_year,
	acctg_mo,
	producer_code,
	producer_name,
	cust_num,
	insured_name,
	asl_code,
	asl_code_desc,
	sched_p_code,
	sched_p_desc,
	bus_seg_code,
	bus_seg_desc,
	prim_bus_class_code,
	prim_bus_class_desc,
	agency_state_code,
	agency_code,
	agency_name,
	rsm_name,
	uw_name,
	asst_uw_name,
	uw_mgr_name,
	prim_agency_state_code,
	prim_agency_code,
	prim_agency_name,
	new_bus_indic,
	prim_rating_state_code,
	sic_code,
	sic_code_desc,
	sub_asl_code,
	sub_asl_code_desc,
	sub_non_asl_code,
	sub_non_asl_code_desc,
	sal_terri_desc,
	uw_region_name,
	pol_orig_incptn_date,
	pol_eff_date,
	pol_exp_date,
	pol_canc_date,
	acctg_prod_abbr,
	pma_code,
	pma_desc,
	ilf_tbl,
	claim_num,
	last_booked_date,
	pass_thru_amt,
	div_pay_amt,
	dep,
	dwp,
	subj_dep,
	subj_dwp,
	other_dep,
	other_dwp,
	expmod_dep,
	expmod_dwp,
	schmod_dep,
	schmod_dwp,
	loss_inc,
	loss_pd,
	alae_pd,
	alae_inc,
	ph_div_payable,
	agency_contigent_ind,
	gl_class_grp,
	prop_rating_group,
	prop_rate_type,
	prop_col_grp,
	auto_veh_type,
	auto_use_class,
	auto_radius,
	auto_fleet_type,
	auto_sec_class_grp,
	crime_ind_grp,
	prop_spec_col_cat,
	Rec_Type,
	year_rt,
	HashKey,
	o_ChangeFlag,
	sbg_code,
	sbg_desc,
	ISOMajorCrimeGroup,
	AgencyStatusCode,
	PrimaryAgencyStatusCode,
	rated_cov_code,
	rated_cov_desc,
	CensusBlockGroupCountyCode,
	CensusBlockGroupTractCode,
	CensusBlockGroupBlockGroupCode,
	Latitude,
	Longitude,
	RatingTerritory,
	Catalyst,
	CauseOfDamage,
	DamageCaused,
	ItemDamaged,
	claim_cat_code,
	serv_center_support_code,
	CustomerInforceInd,
	CustomerProductInforceInd,
	QuoteChannel,
	QuoteChannelOrigin,
	RatingStateAbbreviation,
	mailing_zip_postal_code
	FROM EXP_Detect
),
RTR_Insert_INSERT AS (SELECT * FROM RTR_Insert WHERE o_ChangeFlag='INSERT'),
RTR_Insert_NOCHANGE AS (SELECT * FROM RTR_Insert WHERE o_ChangeFlag='NOCHANGE'),
AA39Monthly_Loss AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.@{pipeline().parameters.TARGET_TABLE_NAME}
	(ent_grp_abbr, spc_abbr, legal_ent_abbr, pol_offering_code, pol_offering_abbr, product_code, product_abbr, lob_code, lob_abbr, lob_desc, ins_seg_code, ins_seg_desc, prog_code, prog_desc, assn_code, assn_desc, cov_summ_code, cov_summ_desc, cov_grp_code, cov_grp_desc, cov_code, cov_desc, cov_trigger_type, rating_plan, type_of_loss, pol_key, pol_sym, pol_no, pol_ver, rating_state_code, loss_year, loss_qtr, loss_mo, year, month, acctg_qtr, acctg_year, acctg_mo, producer_code, producer_name, cust_num, insured_name, asl_code, asl_code_desc, sched_p_code, sched_p_desc, bus_seg_code, bus_seg_desc, prim_bus_class_code, prim_bus_class_desc, agency_state_code, agency_code, agency_name, rsm_name, uw_name, asst_uw_name, uw_mgr_name, prim_agency_state_code, prim_agency_code, prim_agency_name, new_bus_indic, prim_rating_state_code, sic_code, sic_code_desc, sub_asl_code, sub_asl_code_desc, sub_non_asl_code, sub_non_asl_code_desc, sal_terri_desc, uw_region_name, pol_orig_incptn_date, pol_eff_date, pol_exp_date, pol_canc_date, acctg_prod_abbr, pma_code, pma_desc, ilf_tbl, claim_num, last_booked_date, pass_thru_amt, div_pay_amt, dep, dwp, subj_dep, subj_dwp, other_dep, other_dwp, expmod_dep, expmod_dwp, schmod_dep, schmod_dwp, loss_inc, loss_pd, alae_pd, alae_inc, ph_div_payable, agency_contigent_ind, gl_class_grp, prop_rating_group, prop_rate_type, prop_col_grp, auto_veh_type, auto_use_class, auto_radius, auto_fleet_type, auto_sec_class_grp, crime_ind_grp, prop_spec_col_cat, Rec_Type, Year_rt, HashKey, sbg_code, sbg_desc, maj_crime_grp, agency_status_code, prim_agency_status_code, rated_cov_code, rated_cov_desc, CBG_County, CBG_Tract, CBG_Block, Territory_code, Latitude, Longitude, cs_catalyst, cs_cause_of_damage, cs_damage_caused, cs_item_damaged, serv_center_support_code, claim_cat_code, CustomerInforceInd, CustomerProductInforceInd, QuoteChannel, QuoteChannelOrigin, RatingStateAbbreviation, pc_abbr, mailing_zip_postal_code)
	SELECT 
	ENT_GRP_ABBR, 
	SPC_ABBR, 
	LEGAL_ENT_ABBR, 
	POL_OFFERING_CODE, 
	POL_OFFERING_ABBR, 
	PRODUCT_CODE, 
	PRODUCT_ABBR, 
	LOB_CODE, 
	LOB_ABBR, 
	LOB_DESC, 
	INS_SEG_CODE, 
	INS_SEG_DESC, 
	PROG_CODE, 
	PROG_DESC, 
	ASSN_CODE, 
	ASSN_DESC, 
	COV_SUMM_CODE, 
	COV_SUMM_DESC, 
	COV_GRP_CODE, 
	COV_GRP_DESC, 
	COV_CODE, 
	COV_DESC, 
	COV_TRIGGER_TYPE, 
	RATING_PLAN, 
	TYPE_OF_LOSS, 
	POL_KEY, 
	POL_SYM, 
	POL_NO, 
	POL_VER, 
	RATING_STATE_CODE, 
	LOSS_YEAR, 
	LOSS_QTR, 
	LOSS_MO, 
	YEAR, 
	MONTH, 
	ACCTG_QTR, 
	ACCTG_YEAR, 
	ACCTG_MO, 
	PRODUCER_CODE, 
	PRODUCER_NAME, 
	CUST_NUM, 
	INSURED_NAME, 
	ASL_CODE, 
	ASL_CODE_DESC, 
	SCHED_P_CODE, 
	SCHED_P_DESC, 
	BUS_SEG_CODE, 
	BUS_SEG_DESC, 
	PRIM_BUS_CLASS_CODE, 
	PRIM_BUS_CLASS_DESC, 
	AGENCY_STATE_CODE, 
	AGENCY_CODE, 
	AGENCY_NAME, 
	RSM_NAME, 
	UW_NAME, 
	ASST_UW_NAME, 
	UW_MGR_NAME, 
	PRIM_AGENCY_STATE_CODE, 
	PRIM_AGENCY_CODE, 
	PRIM_AGENCY_NAME, 
	NEW_BUS_INDIC, 
	PRIM_RATING_STATE_CODE, 
	SIC_CODE, 
	SIC_CODE_DESC, 
	SUB_ASL_CODE, 
	SUB_ASL_CODE_DESC, 
	SUB_NON_ASL_CODE, 
	SUB_NON_ASL_CODE_DESC, 
	SAL_TERRI_DESC, 
	UW_REGION_NAME, 
	POL_ORIG_INCPTN_DATE, 
	POL_EFF_DATE, 
	POL_EXP_DATE, 
	POL_CANC_DATE, 
	ACCTG_PROD_ABBR, 
	PMA_CODE, 
	PMA_DESC, 
	ILF_TBL, 
	CLAIM_NUM, 
	LAST_BOOKED_DATE, 
	PASS_THRU_AMT, 
	DIV_PAY_AMT, 
	DEP, 
	DWP, 
	SUBJ_DEP, 
	SUBJ_DWP, 
	OTHER_DEP, 
	OTHER_DWP, 
	EXPMOD_DEP, 
	EXPMOD_DWP, 
	SCHMOD_DEP, 
	SCHMOD_DWP, 
	LOSS_INC, 
	LOSS_PD, 
	ALAE_PD, 
	ALAE_INC, 
	PH_DIV_PAYABLE, 
	AGENCY_CONTIGENT_IND, 
	GL_CLASS_GRP, 
	PROP_RATING_GROUP, 
	PROP_RATE_TYPE, 
	PROP_COL_GRP, 
	AUTO_VEH_TYPE, 
	AUTO_USE_CLASS, 
	AUTO_RADIUS, 
	AUTO_FLEET_TYPE, 
	AUTO_SEC_CLASS_GRP, 
	CRIME_IND_GRP, 
	PROP_SPEC_COL_CAT, 
	REC_TYPE, 
	year_rt AS YEAR_RT, 
	HASHKEY, 
	SBG_CODE, 
	SBG_DESC, 
	ISOMajorCrimeGroup AS MAJ_CRIME_GRP, 
	AgencyStatusCode AS AGENCY_STATUS_CODE, 
	PrimaryAgencyStatusCode AS PRIM_AGENCY_STATUS_CODE, 
	RATED_COV_CODE, 
	RATED_COV_DESC, 
	CensusBlockGroupCountyCode AS CBG_COUNTY, 
	CensusBlockGroupTractCode AS CBG_TRACT, 
	CensusBlockGroupBlockGroupCode AS CBG_BLOCK, 
	RatingTerritory AS TERRITORY_CODE, 
	LATITUDE, 
	LONGITUDE, 
	Catalyst AS CS_CATALYST, 
	CauseOfDamage AS CS_CAUSE_OF_DAMAGE, 
	DamageCaused AS CS_DAMAGE_CAUSED, 
	ItemDamaged AS CS_ITEM_DAMAGED, 
	SERV_CENTER_SUPPORT_CODE, 
	CLAIM_CAT_CODE, 
	CUSTOMERINFORCEIND, 
	CUSTOMERPRODUCTINFORCEIND, 
	QUOTECHANNEL, 
	QUOTECHANNELORIGIN, 
	RATINGSTATEABBREVIATION, 
	PC_ABBR, 
	MAILING_ZIP_POSTAL_CODE
	FROM RTR_Insert_INSERT
),
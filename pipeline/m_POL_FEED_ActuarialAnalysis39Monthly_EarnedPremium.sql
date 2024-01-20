WITH
LKP_CDD AS (
	SELECT
	pma_desc,
	pma_code
	FROM (
		SELECT DISTINCT 
		cd.PackageModificationAdjustmentGroupCode as pma_code,
		cd.PackageModificationAdjustmentGroupDescription as pma_desc
		FROM CoverageDetailDim cd
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pma_code ORDER BY pma_desc) = 1
),
SQ_ActuarialAnalysis39Monthly_EarnedPremium AS (
	SELECT ird.InsuranceReferenceDimId,
		pol_current.ProgramCode AS prog_code,
		pol_current.AssociationCode AS assn_code,
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
		pol_current.pol_key AS pol_key,
		pol_current.pol_sym AS pol_sym,
		pol_current.pol_num AS pol_no,
		pol_current.pol_mod AS pol_ver,
		cd.RatingStateProvinceCode AS rating_state_code,
		c.clndr_yr AS acctg_year,
		c.clndr_month AS acctg_mo,
		aed_current.ProducerCode AS producer_code,
		(CASE WHEN aed_current.AgencyEmployeeRole = 'Producer' THEN aed_current.AgencyEmployeeFirstName + ' ' + aed_current.agencyemployeelastname ELSE 'N/A' END) AS producer_name,
		ccd_current.cust_num AS cust_num,
		ccd_current.NAME AS insured_name,
		asl.asl_dim_id,
		pol_current.prim_bus_class_code AS prim_bus_class_code,
		pol_current.prim_bus_class_code_descript AS prim_bus_class_desc,
		ad_current.AssignedStateCode AS agency_state_code,
		ad_current.AgencyCode AS agency_code,
		ad_current.AgencyDoingBusinessAsName AS agency_name,
		s.SalesDivisionDimID,
		(CASE WHEN U.AssociateRole <> 'UNDERWRITER ASSISTANT' THEN U.UnderwriterDisplayName ELSE 'N/A' END) AS uw_name,
		(CASE WHEN U.AssociateRole = 'UNDERWRITER ASSISTANT' THEN U.UnderwriterDisplayName ELSE 'N/A' END) AS asst_uw_name,
		U.UnderwriterManagerDisplayName AS uw_mgr_name,
	--	LEFT(SD.LegalPrimaryAgencyCode, 2) AS prim_agency_state_code,
	--	SD.LegalPrimaryAgencyCode AS prim_agency_code,
	--	SD.AgencyDoingBusinessAsName AS prim_agency_name,
		CASE WHEN pol_current.pol_issue_code = 'N' THEN 'Y' ELSE 'N' END AS new_bus_indic,
		pol_current.state_of_domicile_code AS prim_rating_state_code,
		ccd_current.sic_code AS sic_code,
		ccd_current.sic_code_descript AS sic_code_desc,
		u.UnderwritingRegionCodeDescription AS uw_region_name,
		pol_current.orig_incptn_date AS pol_orig_incptn_date,
		pol_current.pol_eff_date AS pol_eff_date,
		pol_current.pol_exp_date AS pol_exp_date,
		cd.PackageModificationAdjustmentGroupCode AS pma_code,
		cd.IncreasedLimitGroupCode AS ilf_tbl,
		sum(eptm.EarnedDirectWrittenPremium) AS dep,
		sum(eptm.EarnedSubjectWrittenPremium) AS subj_dep,
		sum(eptm.EarnedOtherModifiedPremium) AS other_dep,
		sum(eptm.EarnedExperienceModifiedPremium) AS expmod_dep,
		sum(eptm.EarnedScheduleModifiedPremium) AS schmod_dep,
	(
	CASE WHEN C.clndr_yr < 2022 THEN
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
	WHEN   C.clndr_yr > = 2022 AND ird.StrategicProfitCenterCode = '5' AND ird.RatingPlanAbbreviation NOT IN ('LRARO', 'Retro') THEN 'Y'
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
		cdcp.ISOCommercialPropertyRatingGroupCode AS prop_rating_group,
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
		pol_current.serv_center_support_code,
		cd.RatingStateProvinceAbbreviation AS RatingStateAbbreviation,
		 ccd.mailing_zip_postal_code
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.ModifiedEarnedPremiumTransactionMonthlyFact eptm
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim ird ON ird.InsuranceReferenceDimId = eptm.InsuranceReferenceDimId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailDim cd ON cd.CoverageDetailDimId = eptm.CoverageDetailDimId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim c ON c.clndr_id = eptm.RunDateId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim ircd ON ircd.InsuranceReferenceCoverageDimId = eptm.InsuranceReferenceCoverageDimId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol ON pol.pol_dim_id = eptm.PolicyDimID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol_Current ON pol.edw_pol_ak_id = pol_Current.edw_pol_ak_id
		AND pol_Current.crrnt_snpsht_flag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.AgencyEmployeeDim aed ON aed.AgencyEmployeeDimID = pol_current.AgencyEmployeeDimID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.AgencyEmployeeDim aed_current ON aed.EDWAgencyEmployeeAKID = aed_current.EDWAgencyEmployeeAKID
		AND aed_current.CurrentSnapshotFlag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwritingDivisionDim U ON u.UnderwritingDivisionDimID = pol_current.UnderwritingDivisionDimId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocationDim rld ON rld.RiskLocationDimID = eptm.RiskLocationDimID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim ad ON ad.AgencyDimID = eptm.AgencyDimID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim ad_Current ON ad.EDWAgencyAKID = ad_Current.EDWAgencyAKID
		AND ad_Current.CurrentSnapshotFlag = 1
	--JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim SD ON SD.AgencyCode = ad_Current.LegalPrimaryAgencyCode
	--	AND SD.CurrentSnapshotFlag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.SalesDivisionDim s ON s.SalesDivisionDimID = ad_Current.SalesDivisionDimId
		AND S.CurrentSnapshotFlag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_dim ccd ON ccd.contract_cust_dim_id = eptm.ContractCustomerDimID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_dim ccd_Current ON ccd.edw_contract_cust_ak_id = ccd_Current.edw_contract_cust_ak_id
		AND ccd_Current.crrnt_snpsht_flag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_dim asl ON asl.asl_dim_id = eptm.AnnualStatementLineDimID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransactionTypeDim ptt ON ptt.PremiumTransactionTypeDimID = eptm.PremiumTransactionTypeDimId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailGeneralLiabilityDim cdgl ON eptm.CoverageDetailDimId = cdgl.CoverageDetailDimId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialPropertyDim cdcp ON eptm.CoverageDetailDimId = cdcp.CoverageDetailDimId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAutoDim cdca ON eptm.CoverageDetailDimId = cdca.CoverageDetailDimId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCrimeDim cdc ON eptm.CoverageDetailDimId = cdc.CoverageDetailDimId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.BusinessClassDim bcd ON pol_current.BusinessClassDimId = bcd.BusinessClassDimId
		AND bcd.CurrentSnapshotFlag = 1
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_product_code_dim apc on apc.asl_prdct_code_dim_id=eptm.AnnualStatementLineProductCodeDimId
	WHERE 1=1 
	@{pipeline().parameters.WHERE_CLAUSE}
	GROUP BY ird.InsuranceReferenceDimId,
		pol_current.ProgramCode,
		pol_current.AssociationCode,
		ircd.CoverageSummaryCode,
		ircd.CoverageSummaryDescription,
		ircd.CoverageGroupCode,
		ircd.CoverageGroupDescription,
		ircd.CoverageCode,
		ircd.CoverageDescription,
		ircd.RatedCoverageCode,
		ircd.RatedCoverageDescription,
		(
			CASE WHEN ircd.InsuranceLineCode = 'GL'
					AND PmsMajorPerilDescription = 'GL CLM-MDE' THEN 'CLAIMSMADE' WHEN ircd.InsuranceLineCode = 'GL'
					AND PmsMajorPerilDescription = 'GL OCCUR' THEN 'OCCURRENCE' WHEN ircd.InsuranceLineCode = 'GL'
					AND DctCoverageVersion = 'CLAIMSMADE' THEN 'CLAIMSMADE' WHEN ircd.InsuranceLineCode = 'GL'
					AND DctCoverageVersion = 'OCCURRENCE' THEN 'OCCURRENCE' ELSE 'N/A' END
			),
		pol_current.pol_key,
		pol_current.pol_sym,
		pol_current.pol_num,
		pol_current.pol_mod,
		cd.RatingStateProvinceCode,
		c.clndr_yr,
		c.clndr_month,
		aed_current.ProducerCode,
		(CASE WHEN aed_current.AgencyEmployeeRole = 'Producer' THEN aed_current.AgencyEmployeeFirstName + ' ' + aed_current.agencyemployeelastname ELSE 'N/A' END),
		ccd_current.cust_num,
		ccd_current.NAME,
		asl.asl_dim_id,
		pol_current.prim_bus_class_code,
		pol_current.prim_bus_class_code_descript,
		ad_current.AssignedStateCode,
		ad_current.AgencyCode,
		ad_current.AgencyDoingBusinessAsName,
		s.SalesDivisionDimID,
		(CASE WHEN U.AssociateRole <> 'UNDERWRITER ASSISTANT' THEN U.UnderwriterDisplayName ELSE 'N/A' END),
		(CASE WHEN U.AssociateRole = 'UNDERWRITER ASSISTANT' THEN U.UnderwriterDisplayName ELSE 'N/A' END),
		U.UnderwriterDisplayName,
		U.UnderwriterManagerDisplayName,
	--	LEFT(SD.LegalPrimaryAgencyCode, 2),
	--	SD.LegalPrimaryAgencyCode,
	--	SD.AgencyDoingBusinessAsName,
		CASE WHEN pol_current.pol_issue_code = 'N' THEN 'Y' ELSE 'N' END,
		pol_current.state_of_domicile_code,
		ccd_current.sic_code,
		ccd_current.sic_code_descript,
		u.UnderwritingRegionCodeDescription,
		pol_current.orig_incptn_date,
		pol_current.pol_eff_date,
		pol_current.pol_exp_date,
		cd.PackageModificationAdjustmentGroupCode,
		cd.IncreasedLimitGroupCode,
	(
	CASE WHEN C.clndr_yr < 2022 THEN
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
	WHEN   C.clndr_yr > = 2022 AND ird.StrategicProfitCenterCode = '5' AND ird.RatingPlanAbbreviation NOT IN ('LRARO', 'Retro') THEN 'Y'
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
	) ,
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
		isnull(bcd.BusinessSegmentCode, 'N/A'),
		isnull(bcd.StrategicBusinessGroupCode, 'N/A'),
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
		pol_current.serv_center_support_code,
		cd.RatingStateProvinceAbbreviation,
		 ccd.mailing_zip_postal_code
),
LKP_ASL_DIM AS (
	SELECT
	asl_dim_id,
	asl_code,
	asl_code_desc,
	sched_p_code,
	sched_p_desc,
	sub_asl_code,
	sub_asl_code_desc,
	sub_non_asl_code,
	sub_non_asl_code_desc
	FROM (
		SELECT
		asl.asl_dim_id AS asl_dim_id , 
		asl.asl_code AS asl_code, 
		asl.asl_code_descript AS asl_code_desc,     
		asl.sched_p_num as sched_p_code,  
		asl.sched_p_name as sched_p_desc,
		asl.sub_asl_code as sub_asl_code,
		asl.sub_asl_code_descript as sub_asl_code_desc,  
		asl.sub_non_asl_code as sub_non_asl_code,
		asl.sub_non_asl_code_descript as sub_non_asl_code_desc
		FROM  asl_dim asl
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY asl_dim_id ORDER BY asl_dim_id) = 1
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
LKP_CalendarDim AS (
	SELECT
	Year,
	Month,
	acctg_qtr,
	acctg_year,
	acctg_mo
	FROM (
		SELECT DISTINCT 
		c.clndr_yr as Year, 
		c.clndr_month as Month, 
		c.clndr_qtr as acctg_qtr, 
		c.clndr_yr as acctg_year,
		c.clndr_month as acctg_mo
		FROM calendar_dim c
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY acctg_year,acctg_mo ORDER BY Year) = 1
),
LKP_InsuranceReferenceDim AS (
	SELECT
	InsuranceReferenceDimId,
	ent_grp_abbr,
	spc_abbr,
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
	rating_plan,
	acctg_prod_abbr
	FROM (
		SELECT 
		ird.InsuranceReferenceDimId as InsuranceReferenceDimId  ,
		ird.EnterpriseGroupAbbreviation as ent_grp_abbr,
		ird.StrategicProfitCenterAbbreviation as spc_abbr,
		ird.InsuranceReferenceLegalEntityAbbreviation as legal_ent_abbr,
		ird.PolicyOfferingCode as pol_offering_code,
		ird.PolicyOfferingAbbreviation as pol_offering_abbr,
		ird.ProductCode as product_code,
		ird.ProductAbbreviation as product_abbr,
		ird.InsuranceReferenceLineOfBusinessCode as lob_code,
		ird.InsuranceReferenceLineOfBusinessAbbreviation as lob_abbr,
		ird.InsuranceReferenceLineOfBusinessDescription as lob_desc,
		ird.InsuranceSegmentCode as ins_seg_code,
		ird.InsuranceSegmentDescription as ins_seg_desc,
		ird.RatingPlanDescription as rating_plan,
		ird.AccountingProductAbbreviation as acctg_prod_abbr
		FROM InsuranceReferenceDim ird
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceReferenceDimId ORDER BY InsuranceReferenceDimId) = 1
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
LKP_SalesDivisionDim AS (
	SELECT
	SalesDivisionDimID,
	rsm_name,
	sal_terri_desc
	FROM (
		SELECT 
		s.SalesDivisionDimID AS   SalesDivisionDimID ,
		s.RegionalSalesManagerDisplayName as rsm_name,
		s.SalesTerritoryCodeDescription as sal_terri_desc
		FROM   SalesDivisionDim s
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SalesDivisionDimID ORDER BY SalesDivisionDimID) = 1
),
LKP_policy_dim_descriptions AS (
	SELECT
	ProgramDescription,
	AssociationDescription,
	QuoteChannel,
	QuoteChannelOrigin,
	edw_pol_ak_id
	FROM (
		select edw_pol_ak_id as edw_pol_ak_id,
		AssociationDescription as AssociationDescription,
		ProgramDescription as ProgramDescription,
		quotechannel as quotechannel,
		quotechannelorigin as quotechannelorigin 
		from policy_dim 
		where crrnt_snpsht_flag = 1 
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_pol_ak_id ORDER BY ProgramDescription) = 1
),
EXP_Default AS (
	SELECT
	LKP_InsuranceReferenceDim.ent_grp_abbr,
	LKP_InsuranceReferenceDim.spc_abbr,
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
	LKP_InsuranceReferenceDim.legal_ent_abbr,
	LKP_InsuranceReferenceDim.pol_offering_code,
	LKP_InsuranceReferenceDim.pol_offering_abbr,
	LKP_InsuranceReferenceDim.product_code,
	LKP_InsuranceReferenceDim.product_abbr,
	LKP_InsuranceReferenceDim.lob_code,
	LKP_InsuranceReferenceDim.lob_abbr,
	LKP_InsuranceReferenceDim.lob_desc,
	LKP_InsuranceReferenceDim.ins_seg_code,
	LKP_InsuranceReferenceDim.ins_seg_desc,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.prog_code,
	LKP_policy_dim_descriptions.ProgramDescription,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.assn_code,
	LKP_policy_dim_descriptions.AssociationDescription,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.cov_summ_code,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.cov_summ_desc,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.cov_grp_code,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.cov_grp_desc,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.cov_code,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.cov_desc,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.cov_trigger_type,
	LKP_InsuranceReferenceDim.rating_plan,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.pol_key,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.pol_sym,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.pol_no,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.pol_ver,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.rating_state_code,
	LKP_CalendarDim.Year AS year,
	LKP_CalendarDim.Month AS month,
	LKP_CalendarDim.acctg_qtr,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.acctg_year,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.acctg_mo,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.producer_code,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.producer_name,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.cust_num,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.insured_name,
	LKP_ASL_DIM.asl_code,
	LKP_ASL_DIM.asl_code_desc,
	LKP_ASL_DIM.sched_p_code,
	LKP_ASL_DIM.sched_p_desc,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.BusinessSegmentCode AS bus_seg_code,
	LKP_BusinessClassDim_SBG.BusinessSegmentDescription AS bus_seg_desc,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.StrategicBusinessGroupCode AS sbg_code,
	LKP_BusinessClassDim_SBG.StrategicBusinessGroupDescription AS sbg_desc,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.prim_bus_class_code,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.prim_bus_class_desc,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.agency_state_code,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.agency_code,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.agency_name,
	LKP_SalesDivisionDim.rsm_name,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.uw_name,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.asst_uw_name,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.uw_mgr_name,
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
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.new_bus_indic,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.prim_rating_state_code,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.sic_code,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.sic_code_desc,
	LKP_ASL_DIM.sub_asl_code,
	LKP_ASL_DIM.sub_asl_code_desc,
	LKP_ASL_DIM.sub_non_asl_code,
	LKP_ASL_DIM.sub_non_asl_code_desc,
	LKP_SalesDivisionDim.sal_terri_desc,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.uw_region_name,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.pol_orig_incptn_date,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.pol_eff_date,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.pol_exp_date,
	LKP_PolicyCurrentStatusDim.PolicyCancellationDate AS pol_canc_date,
	LKP_InsuranceReferenceDim.acctg_prod_abbr,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.pma_code,
	-- *INF*: :LKP.LKP_CDD(pma_code)
	LKP_CDD_pma_code.pma_desc AS v_pma_desc,
	v_pma_desc AS pma_desc,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.ilf_tbl,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.dep,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.subj_dep,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.other_dep,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.expmod_dep,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.schmod_dep,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.agency_contigent_ind,
	'N/A' AS o_type_of_loss,
	NULL AS o_loss_year,
	NULL AS o_loss_qtr,
	NULL AS o_loss_mo,
	NULL AS o_claim_cat_code,
	'0' AS o_claim_num,
	-- *INF*: LAST_DAY(ADD_TO_DATE(ROUND(SYSDATE,'DD'), 'MM', -1))
	LAST_DAY(DATEADD(MONTH,- 1,ROUND(CURRENT_TIMESTAMP, 'DD'))) AS o_last_booked_date,
	0 AS o_pass_thru_amt,
	0 AS o_div_pay_amt,
	0 AS o_dwp,
	0 AS o_subj_dwp,
	0 AS o_other_dwp,
	0 AS o_expmod_dwp,
	0 AS o_schmod_dwp,
	0 AS o_loss_inc,
	0 AS o_loss_pd,
	0 AS o_alae_pd,
	0 AS o_alae_inc,
	0 AS o_ph_div_payable,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.gl_class_grp,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.prop_rating_group,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.prop_rate_type,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.prop_col_grp,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.auto_veh_type,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.auto_use_class,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.auto_radius,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.auto_fleet_type,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.auto_sec_class_grp,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.crime_ind_grp,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.prop_spec_col_cat,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(gl_class_grp))
	UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(gl_class_grp)) AS o_gl_class_grp,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(prop_rating_group))
	UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(prop_rating_group)) AS o_prop_rating_group,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(prop_rate_type))
	UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(prop_rate_type)) AS o_prop_rate_type,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(prop_col_grp))
	UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(prop_col_grp)) AS o_prop_col_grp,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(auto_veh_type))
	UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(auto_veh_type)) AS o_auto_veh_type,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(auto_use_class))
	UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(auto_use_class)) AS o_auto_use_class,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(auto_radius))
	UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(auto_radius)) AS o_auto_radius,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(auto_fleet_type))
	UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(auto_fleet_type)) AS o_auto_fleet_type,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(auto_sec_class_grp))
	UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(auto_sec_class_grp)) AS o_auto_sec_class_grp,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(crime_ind_grp))
	UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(crime_ind_grp)) AS o_crime_ind_grp,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(prop_spec_col_cat))
	UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(prop_spec_col_cat)) AS o_prop_spec_col_cat,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.ISOMajorCrimeGroup,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.AgencyStatusCode AS i_AgencyStatusCode,
	-- *INF*: IIF(ISNULL(i_AgencyStatusCode),'0',i_AgencyStatusCode)
	IFF(i_AgencyStatusCode IS NULL, '0', i_AgencyStatusCode) AS v_AgencyStatusCode,
	v_AgencyStatusCode AS o_AgencyStatusCode,
	LKP_AgencyRelationship.prim_agency_status_code AS i_PrimaryAgencyStatusCode,
	-- *INF*: IIF(ISNULL(i_PrimaryAgencyStatusCode),'0',i_PrimaryAgencyStatusCode)
	IFF(i_PrimaryAgencyStatusCode IS NULL, '0', i_PrimaryAgencyStatusCode) AS v_PrimaryAgencyStatusCode,
	v_PrimaryAgencyStatusCode AS o_PrimaryAgencyStatusCode,
	'EarnedPremium' AS o_rec_Type,
	year*10+2 AS o_year_rt,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.rated_cov_code,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.rated_cov_desc,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.CensusBlockGroupCountyCode AS i_CensusBlockGroupCountyCode,
	-- *INF*: IIF(ISNULL(i_CensusBlockGroupCountyCode),'N/A',i_CensusBlockGroupCountyCode)
	IFF(i_CensusBlockGroupCountyCode IS NULL, 'N/A', i_CensusBlockGroupCountyCode) AS v_CensusBlockGroupCountyCode,
	v_CensusBlockGroupCountyCode AS o_CensusBlockGroupCountyCode,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.CensusBlockGroupTractCode AS i_CensusBlockGroupTractCode,
	-- *INF*: IIF(ISNULL(i_CensusBlockGroupTractCode),'N/A',i_CensusBlockGroupTractCode)
	IFF(i_CensusBlockGroupTractCode IS NULL, 'N/A', i_CensusBlockGroupTractCode) AS v_CensusBlockGroupTractCode,
	v_CensusBlockGroupTractCode AS o_CensusBlockGroupTractCode,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.CensusBlockGroupBlockGroupCode AS i_CensusBlockGroupBlockGroupCode,
	-- *INF*: IIF(ISNULL(i_CensusBlockGroupBlockGroupCode),'N/A',i_CensusBlockGroupBlockGroupCode)
	IFF(i_CensusBlockGroupBlockGroupCode IS NULL, 'N/A', i_CensusBlockGroupBlockGroupCode) AS v_CensusBlockGroupBlockGroupCode,
	v_CensusBlockGroupBlockGroupCode AS o_CensusBlockGroupBlockGroupCode,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.Latitude AS i_Latitude,
	-- *INF*: IIF(ISNULL(i_Latitude),0.00,i_Latitude)
	IFF(i_Latitude IS NULL, 0.00, i_Latitude) AS v_Latitude,
	v_Latitude AS o_Latitude,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.Longitude AS i_Longitude,
	-- *INF*: IIF(ISNULL(i_Longitude),0.00,i_Longitude)
	IFF(i_Longitude IS NULL, 0.00, i_Longitude) AS v_Longitude,
	v_Longitude AS o_Longitude,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.RatingTerritory AS i_RatingTerritory,
	-- *INF*: SUBSTR(IIF(ISNULL(i_RatingTerritory),'N/A',i_RatingTerritory),0 ,3)
	SUBSTR(
	    IFF(
	        i_RatingTerritory IS NULL, 'N/A', i_RatingTerritory
	    ), 0, 3) AS v_RatingTerritory,
	v_RatingTerritory AS o_RatingTerritory,
	'N/A' AS Catalyst,
	'N/A' AS CauseOfDamage,
	'N/A' AS DamageCaused,
	'N/A' AS ItemDamaged,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.serv_center_support_code,
	'0' AS o_defaultvalue,
	LKP_policy_dim_descriptions.QuoteChannel AS i_QuoteChannel,
	-- *INF*: iif(isnull(i_QuoteChannel) , 'N/A' , i_QuoteChannel)
	IFF(i_QuoteChannel IS NULL, 'N/A', i_QuoteChannel) AS v_QuoteChannel,
	v_QuoteChannel AS o_QuoteChannel,
	LKP_policy_dim_descriptions.QuoteChannelOrigin AS i_QuoteChannelOrigin,
	-- *INF*: iif(isnull(i_QuoteChannelOrigin) , 'N/A' , i_QuoteChannelOrigin)
	IFF(i_QuoteChannelOrigin IS NULL, 'N/A', i_QuoteChannelOrigin) AS v_QuoteChannelOrigin,
	v_QuoteChannelOrigin AS o_QuoteChannelOrigin,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.RatingStateAbbreviation AS i_RatingStateAbbreviation,
	-- *INF*: IIF(ISNULL(i_RatingStateAbbreviation),'N/A',i_RatingStateAbbreviation)
	IFF(i_RatingStateAbbreviation IS NULL, 'N/A', i_RatingStateAbbreviation) AS v_RatingStateAbbreviation,
	v_RatingStateAbbreviation AS o_RatingStateAbbreviation,
	SQ_ActuarialAnalysis39Monthly_EarnedPremium.mailing_zip_postal_code,
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
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(ProgramDescription))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(assn_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(AssociationDescription))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(cov_summ_code))      --cov_summ_code
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(cov_summ_desc))     --cov_summ_desc
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(cov_grp_code))      --cov_grp_code
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(cov_grp_desc))     --cov_grp_desc
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(cov_code))    --cov_code
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(cov_desc))      --cov_desc
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(cov_trigger_type))     --cov_trigger_type
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(rating_plan))
	-- || '^' || 'N/A'      --type_of_loss
	-- 
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(pol_key))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(pol_sym))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(pol_no))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(pol_ver))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(rating_state_code))
	-- || '^' || 'NULL'    --loss_year
	-- || '^' || 'NULL'    --loss_qtr
	-- || '^' || 'NULL'    --loss_mo
	-- || '^' || 'NULL'    --claim_cat_code
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
	-- --WREQ-10769 added SBG code&desc
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
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(pma_code))     --pma_code
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_pma_desc))      --pma_desc
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(ilf_tbl))      --ilf_tbl
	-- || '^' || '0'
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(TO_CHAR(LAST_DAY(ADD_TO_DATE(ROUND(SYSDATE,'DD'), 'MM', -1)),'YYYY-MM-DD'))
	-- || '^' || '0.00'      --pass_thru_amt
	-- || '^' || '0.00'   -- div_pay_amt
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(ROUND(dep,2)))   --dep
	-- || '^' || '0.00'  --dwp
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(ROUND(subj_dep,2)))   --subj_dep
	-- || '^' || '0.00'   --subj_dwp
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(ROUND(other_dep,2)))    --other_dep
	-- || '^' || '0.00'   --other_dwp
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(ROUND(expmod_dep,2)))   --expmod_dep
	-- || '^' || '0.00'  --expmod_dwp
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(ROUND(schmod_dep,2)))  --schmod_dep
	-- || '^' || '0.00'   --schmod_dwp
	-- || '^' || '0.00'  --loss_inc
	-- || '^' || '0.00'   --loss_pd
	-- || '^' || '0.00'  --alae_pd
	-- || '^' || '0.00'  --alae_inc
	-- || '^' || '0.00'  --ph_div_payable
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(agency_contigent_ind))     --agency_contigent_ind
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(gl_class_grp))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(prop_rating_group))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(prop_rate_type))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(prop_col_grp))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(auto_veh_type))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(auto_use_class))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(auto_radius))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(auto_fleet_type))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(auto_sec_class_grp))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(crime_ind_grp))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(prop_spec_col_cat))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(ISOMajorCrimeGroup))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_AgencyStatusCode))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_PrimaryAgencyStatusCode))
	-- || '^' || 'EarnedPremium'
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(year*10+2))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(rated_cov_code))    
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(rated_cov_desc))   
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(v_CensusBlockGroupCountyCode)
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(v_CensusBlockGroupTractCode)
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(v_CensusBlockGroupBlockGroupCode)
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_Longitude))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_Latitude))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(v_RatingTerritory)
	-- || '^' || 'N/A'      --Catalyst
	-- || '^' || 'N/A'      --CauseOfDamage
	-- || '^' || 'N/A'      --DamageCaused
	-- || '^' || 'N/A'      --ItemDamaged
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(serv_center_support_code)
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_QuoteChannel))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_QuoteChannelOrigin))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_RatingStateAbbreviation))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(mailing_zip_postal_code))
	-- )
	MD5(UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(ent_grp_abbr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(spc_abbr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_pc_abbr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(legal_ent_abbr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_offering_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_offering_abbr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(product_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(product_abbr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(lob_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(lob_abbr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(lob_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(ins_seg_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(ins_seg_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(prog_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(ProgramDescription)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(assn_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(AssociationDescription)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(cov_summ_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(cov_summ_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(cov_grp_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(cov_grp_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(cov_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(cov_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(cov_trigger_type)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(rating_plan)) || '^' || 'N/A' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_key)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_sym)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_no)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_ver)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(rating_state_code)) || '^' || 'NULL' || '^' || 'NULL' || '^' || 'NULL' || '^' || 'NULL' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(year)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(month)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(acctg_qtr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(acctg_year)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(acctg_mo)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(producer_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(producer_name)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(cust_num)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(insured_name)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(asl_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(asl_code_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sched_p_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sched_p_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(bus_seg_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(bus_seg_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sbg_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sbg_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(prim_bus_class_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(prim_bus_class_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(agency_state_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(agency_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(agency_name)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(rsm_name)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(uw_name)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(asst_uw_name)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(uw_mgr_name)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_prim_agency_state_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_prim_agency_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_prim_agency_name)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(new_bus_indic)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(prim_rating_state_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sic_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sic_code_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sub_asl_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sub_asl_code_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sub_non_asl_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sub_non_asl_code_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sal_terri_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(uw_region_name)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_orig_incptn_date)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_eff_date)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_exp_date)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_canc_date)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(acctg_prod_abbr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pma_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_pma_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(ilf_tbl)) || '^' || '0' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(TO_CHAR(LAST_DAY(DATEADD(MONTH,- 1,ROUND(CURRENT_TIMESTAMP, 'DD'))), 'YYYY-MM-DD')) || '^' || '0.00' || '^' || '0.00' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(ROUND(dep, 2))) || '^' || '0.00' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(ROUND(subj_dep, 2))) || '^' || '0.00' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(ROUND(other_dep, 2))) || '^' || '0.00' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(ROUND(expmod_dep, 2))) || '^' || '0.00' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(ROUND(schmod_dep, 2))) || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(agency_contigent_ind)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(gl_class_grp)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(prop_rating_group)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(prop_rate_type)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(prop_col_grp)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(auto_veh_type)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(auto_use_class)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(auto_radius)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(auto_fleet_type)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(auto_sec_class_grp)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(crime_ind_grp)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(prop_spec_col_cat)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(ISOMajorCrimeGroup)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_AgencyStatusCode)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_PrimaryAgencyStatusCode)) || '^' || 'EarnedPremium' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(year * 10 + 2)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(rated_cov_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(rated_cov_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(v_CensusBlockGroupCountyCode) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(v_CensusBlockGroupTractCode) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(v_CensusBlockGroupBlockGroupCode) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_Longitude)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_Latitude)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(v_RatingTerritory) || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(serv_center_support_code) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_QuoteChannel)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_QuoteChannelOrigin)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_RatingStateAbbreviation)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(mailing_zip_postal_code))) AS v_HashKey,
	v_HashKey AS o_HashKey
	FROM SQ_ActuarialAnalysis39Monthly_EarnedPremium
	LEFT JOIN LKP_ASL_DIM
	ON LKP_ASL_DIM.asl_dim_id = SQ_ActuarialAnalysis39Monthly_EarnedPremium.asl_dim_id
	LEFT JOIN LKP_AgencyRelationship
	ON LKP_AgencyRelationship.EDWAgencyAKID = SQ_ActuarialAnalysis39Monthly_EarnedPremium.EDWAgencyAKID
	LEFT JOIN LKP_BusinessClassDim_SBG
	ON LKP_BusinessClassDim_SBG.BusinessSegmentCode = SQ_ActuarialAnalysis39Monthly_EarnedPremium.BusinessSegmentCode AND LKP_BusinessClassDim_SBG.StrategicBusinessGroupCode = SQ_ActuarialAnalysis39Monthly_EarnedPremium.StrategicBusinessGroupCode
	LEFT JOIN LKP_CalendarDim
	ON LKP_CalendarDim.acctg_year = SQ_ActuarialAnalysis39Monthly_EarnedPremium.acctg_year AND LKP_CalendarDim.acctg_mo = SQ_ActuarialAnalysis39Monthly_EarnedPremium.acctg_mo
	LEFT JOIN LKP_InsuranceReferenceDim
	ON LKP_InsuranceReferenceDim.InsuranceReferenceDimId = SQ_ActuarialAnalysis39Monthly_EarnedPremium.InsuranceReferenceDimId
	LEFT JOIN LKP_PolicyCurrentStatusDim
	ON LKP_PolicyCurrentStatusDim.EDWPolicyAKId = SQ_ActuarialAnalysis39Monthly_EarnedPremium.edw_pol_ak_id
	LEFT JOIN LKP_SalesDivisionDim
	ON LKP_SalesDivisionDim.SalesDivisionDimID = SQ_ActuarialAnalysis39Monthly_EarnedPremium.SalesDivisionDimID
	LEFT JOIN LKP_policy_dim_descriptions
	ON LKP_policy_dim_descriptions.edw_pol_ak_id = SQ_ActuarialAnalysis39Monthly_EarnedPremium.edw_pol_ak_id
	LEFT JOIN LKP_CDD LKP_CDD_pma_code
	ON LKP_CDD_pma_code.pma_code = pma_code

),
LKP_AA39Monthly AS (
	SELECT
	HashKey,
	i_HashKey
	FROM (
		select HashKey  as HashKey
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.AA39Monthly 
		where Rec_Type ='EarnedPremium' 
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
	EXP_Default.ProgramDescription AS prog_desc,
	EXP_Default.assn_code,
	EXP_Default.AssociationDescription AS assn_desc,
	EXP_Default.cov_summ_code,
	EXP_Default.cov_summ_desc,
	EXP_Default.cov_grp_code,
	EXP_Default.cov_grp_desc,
	EXP_Default.cov_code,
	EXP_Default.cov_desc,
	EXP_Default.cov_trigger_type,
	EXP_Default.rating_plan,
	EXP_Default.o_type_of_loss AS type_of_loss,
	EXP_Default.pol_key,
	EXP_Default.pol_sym,
	EXP_Default.pol_no,
	EXP_Default.pol_ver,
	EXP_Default.rating_state_code,
	EXP_Default.o_loss_year AS loss_year,
	EXP_Default.o_loss_qtr AS loss_qtr,
	EXP_Default.o_loss_mo AS loss_mo,
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
	EXP_Default.o_claim_num AS claim_num,
	EXP_Default.o_last_booked_date AS last_booked_date,
	EXP_Default.o_pass_thru_amt AS pass_thru_amt,
	EXP_Default.o_div_pay_amt AS div_pay_amt,
	EXP_Default.dep,
	EXP_Default.o_dwp AS dwp,
	EXP_Default.subj_dep,
	EXP_Default.o_subj_dwp AS subj_dwp,
	EXP_Default.other_dep,
	EXP_Default.o_other_dwp AS other_dwp,
	EXP_Default.expmod_dep,
	EXP_Default.o_expmod_dwp AS expmod_dwp,
	EXP_Default.schmod_dep,
	EXP_Default.o_schmod_dwp AS schmod_dwp,
	EXP_Default.o_loss_inc AS loss_inc,
	EXP_Default.o_loss_pd AS loss_pd,
	EXP_Default.o_alae_pd AS alae_pd,
	EXP_Default.o_alae_inc AS alae_inc,
	EXP_Default.o_ph_div_payable AS ph_div_payable,
	EXP_Default.agency_contigent_ind,
	EXP_Default.o_gl_class_grp AS gl_class_grp,
	EXP_Default.o_prop_rating_group AS prop_rating_group,
	EXP_Default.o_prop_rate_type AS prop_rate_type,
	EXP_Default.o_prop_col_grp AS prop_col_grp,
	EXP_Default.o_auto_veh_type AS auto_veh_type,
	EXP_Default.o_auto_use_class AS auto_use_class,
	EXP_Default.o_auto_radius AS auto_radius,
	EXP_Default.o_auto_fleet_type AS auto_fleet_type,
	EXP_Default.o_auto_sec_class_grp AS auto_sec_class_grp,
	EXP_Default.o_crime_ind_grp AS crime_ind_grp,
	EXP_Default.o_prop_spec_col_cat AS prop_spec_col_cat,
	EXP_Default.o_rec_Type AS Rec_Type,
	EXP_Default.o_year_rt AS year_rt,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_HashKey),'INSERT'
	-- ,'UNCHANGE')
	-- 
	-- 
	DECODE(
	    TRUE,
	    lkp_HashKey IS NULL, 'INSERT',
	    'UNCHANGE'
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
	EXP_Default.Catalyst,
	EXP_Default.CauseOfDamage,
	EXP_Default.DamageCaused,
	EXP_Default.ItemDamaged,
	EXP_Default.o_claim_cat_code,
	EXP_Default.serv_center_support_code,
	EXP_Default.o_defaultvalue AS CustomerInforceInd,
	EXP_Default.o_defaultvalue AS CustomerProductInforceInd,
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
	o_ChangeFlag AS ChangeFlag,
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
	o_claim_cat_code,
	serv_center_support_code,
	CustomerInforceInd,
	CustomerProductInforceInd,
	QuoteChannel,
	QuoteChannelOrigin,
	RatingStateAbbreviation,
	mailing_zip_postal_code
	FROM EXP_Detect
),
RTR_Insert_INSERT AS (SELECT * FROM RTR_Insert WHERE ChangeFlag='INSERT'),
RTR_Insert_UNCHANGE AS (SELECT * FROM RTR_Insert WHERE ChangeFlag='UNCHANGE'),
AA39Monthly_EarnedPremium AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AA39Monthly
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
	o_claim_cat_code AS CLAIM_CAT_CODE, 
	CUSTOMERINFORCEIND, 
	CUSTOMERPRODUCTINFORCEIND, 
	QUOTECHANNEL, 
	QUOTECHANNELORIGIN, 
	RATINGSTATEABBREVIATION, 
	PC_ABBR, 
	MAILING_ZIP_POSTAL_CODE
	FROM RTR_Insert_INSERT
),
WITH
SQ_MonthlyActuarialAnalysis39_Dividend AS (
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
		pol_current.pol_key AS pol_key,
		pol_current.pol_sym AS pol_sym,
		pol_current.pol_num AS pol_no,
		pol_current.pol_mod AS pol_ver,
		std.StateCode AS rating_state_code,
		c.clndr_yr AS Year,
		c.clndr_month AS Month,
		c.clndr_qtr AS acctg_qtr,
		c.clndr_yr AS acctg_year,
		c.clndr_month AS acctg_mo,
		ccd_current.cust_num AS cust_num,
		ccd_current.NAME AS insured_name,
		pol_current.prim_bus_class_code AS prim_bus_class,
		pol_current.prim_bus_class_code_descript AS prim_bus_class_desc,
		ad_current.AssignedStateCode AS agency_state_code,
		ad_current.AgencyCode AS agency_code,
		ad_current.AgencyDoingBusinessAsName AS agency_name,
		s.RegionalSalesManagerDisplayName AS rsm_name,
	--	LEFT(SD.LegalPrimaryAgencyCode, 2) AS prim_agency_state_code,
	--	SD.LegalPrimaryAgencyCode AS prim_agency_code,
	--	SD.AgencyDoingBusinessAsName AS prim_agency_name,
		CASE WHEN pol_current.pol_issue_code = 'N' THEN 'Y' ELSE 'N' END AS new_bus_indicator,
		pol_current.state_of_domicile_code AS pri_rating_state_code,
		ccd_current.sic_code AS sic_code,
		ccd_current.sic_code_descript AS sic_code_desc,
		s.SalesTerritoryCodeDescription AS sal_terri_desc,
		pol_current.orig_incptn_date AS pol_orig_incptn_date,
		pol_current.pol_eff_date AS pol_eff_date,
		pol_current.pol_exp_date AS pol_exp_date,
		isnull(bcd.BusinessSegmentCode, 'N/A') AS BusinessSegmentCode,
		isnull(bcd.StrategicBusinessGroupCode, 'N/A') AS StrategicBusinessGroupCode,
		SUM(dvdf.DividendPaidAmount) AS div_pay_amt,
		SUM(dvdf.DividendPayableAmount) AS ph_div_payable,
		pol_Current.edw_pol_ak_id,
		ad_Current.AgencyStatusCode,
	--	SD.AgencyStatusCode as PrimaryAgencyStatusCode,
		ad_Current.EDWAgencyAKID,
		pol_current.serv_center_support_code,
	      pol_current.QuoteChannel,
	     pol_current.QuoteChannelOrigin,
		 std.StateAbbreviation AS RatingStateAbbreviation,
		 ccd.mailing_zip_postal_code	
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DividendFact dvdf
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim c ON c.clndr_id = dvdf.DividendRunDateId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol ON pol.pol_dim_id = dvdf.PolicyDimID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol_Current ON pol.edw_pol_ak_id = pol_Current.edw_pol_ak_id
		AND pol_Current.crrnt_snpsht_flag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim ad ON ad.AgencyDimID = dvdf.AgencyDimID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim ad_Current ON ad.EDWAgencyAKID = ad_Current.EDWAgencyAKID
		AND ad_Current.CurrentSnapshotFlag = 1
	--JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim SD ON SD.AgencyCode = AD_current.LegalPrimaryAgencyCode
	--	AND SD.CurrentSnapshotFlag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.SalesDivisionDim s ON s.SalesDivisionDimID = dvdf.SalesDivisionDimId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_dim ccd ON ccd.contract_cust_dim_id = dvdf.ContractCustomerDimID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_dim ccd_Current ON ccd.edw_contract_cust_ak_id = ccd_Current.edw_contract_cust_ak_id
		AND ccd_Current.crrnt_snpsht_flag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StateDim std ON std.StateDimId = dvdf.StateDimId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim IRD ON IRD.InsuranceReferenceDimId = dvdf.StrategicProfitCenterDimId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.BusinessClassDim bcd ON pol_current.BusinessClassDimId = bcd.BusinessClassDimId
		AND bcd.CurrentSnapshotFlag = 1
	WHERE 1=1 AND pol.pol_sym <> '000'
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
		pol_current.pol_key,
		pol_current.pol_sym,
		pol_current.pol_num,
		pol_current.pol_mod,
		std.StateCode,
		c.clndr_yr,
		c.clndr_month,
		c.clndr_month,
		c.clndr_qtr,
		c.clndr_yr,
		c.clndr_month,
		ccd_current.cust_num,
		ccd_current.NAME,
		pol_current.prim_bus_class_code,
		pol_current.prim_bus_class_code_descript,
		ad_current.AssignedStateCode,
		ad_current.AgencyCode,
		ad_current.AgencyDoingBusinessAsName,
		s.RegionalSalesManagerDisplayName,
	--	LEFT(SD.LegalPrimaryAgencyCode, 2),
	--	SD.LegalPrimaryAgencyCode,
	--	SD.AgencyDoingBusinessAsName,
		pol_current.state_of_domicile_code,
		ccd_current.sic_code,
		ccd_current.sic_code_descript,
		s.SalesTerritoryCodeDescription,
		pol_current.orig_incptn_date,
		pol_current.pol_eff_date,
		pol_current.pol_exp_date,
		CASE WHEN pol_current.pol_issue_code = 'N' THEN 'Y' ELSE 'N' END,
		ird.StrategicProfitCenterCode,
		isnull(bcd.BusinessSegmentCode, 'N/A'),
		isnull(bcd.StrategicBusinessGroupCode, 'N/A'),
		pol_Current.edw_pol_ak_id,
		ad_Current.AgencyStatusCode,
	--	SD.AgencyStatusCode,
		ad_Current.EDWAgencyAKID,
	       pol_current.serv_center_support_code,
	      pol_current.QuoteChannel,
	     pol_current.QuoteChannelOrigin,
		 std.StateAbbreviation,
		 ccd.mailing_zip_postal_code
		 
	Union all
	
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
		pol_current.pol_key AS pol_key,
		pol_current.pol_sym AS pol_sym,
		pol_current.pol_num AS pol_no,
		pol_current.pol_mod AS pol_ver,
		std.StateCode AS rating_state_code,
		c.clndr_yr AS Year,
		c.clndr_month AS Month,
		c.clndr_qtr AS acctg_qtr,
		c.clndr_yr AS acctg_year,
		c.clndr_month AS acctg_mo,
		ccd_current.cust_num AS cust_num,
		ccd_current.NAME AS insured_name,
		pol_current.prim_bus_class_code AS prim_bus_class,
		pol_current.prim_bus_class_code_descript AS prim_bus_class_desc,
		ad_current.AssignedStateCode AS agency_state_code,
		ad_current.AgencyCode AS agency_code,
		ad_current.AgencyDoingBusinessAsName AS agency_name,
		s.RegionalSalesManagerDisplayName AS rsm_name,
	--	LEFT(SD.LegalPrimaryAgencyCode, 2) AS prim_agency_state_code,
	--	SD.LegalPrimaryAgencyCode AS prim_agency_code,
	--	SD.AgencyDoingBusinessAsName AS prim_agency_name,
		CASE WHEN pol_current.pol_issue_code = 'N' THEN 'Y' ELSE 'N' END AS new_bus_indicator,
		pol_current.state_of_domicile_code AS pri_rating_state_code,
		ccd_current.sic_code AS sic_code,
		ccd_current.sic_code_descript AS sic_code_desc,
		s.SalesTerritoryCodeDescription AS sal_terri_desc,
		pol_current.orig_incptn_date AS pol_orig_incptn_date,
		pol_current.pol_eff_date AS pol_eff_date,
		pol_current.pol_exp_date AS pol_exp_date,
		isnull(bcd.BusinessSegmentCode, 'N/A') AS BusinessSegmentCode,
		isnull(bcd.StrategicBusinessGroupCode, 'N/A') AS StrategicBusinessGroupCode,
		SUM(dvdf.DividendPaidAmount) AS div_pay_amt,
		0.00 as ph_div_payable,
	-- 	SUM(dvdf.DividendPayableAmount) AS ph_div_payable,
		pol_Current.edw_pol_ak_id,
		ad_Current.AgencyStatusCode,
	--	SD.AgencyStatusCode as PrimaryAgencyStatusCode,
		ad_Current.EDWAgencyAKID,
		pol_current.serv_center_support_code,
	      pol_current.QuoteChannel,
	     pol_current.QuoteChannelOrigin,
		 std.StateAbbreviation AS RatingStateAbbreviation,
		 ccd.mailing_zip_postal_code	
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCTDividendFact dvdf
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim c ON c.clndr_id = dvdf.DividendRunDateId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol ON pol.pol_dim_id = dvdf.PolicyDimID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol_Current ON pol.edw_pol_ak_id = pol_Current.edw_pol_ak_id
		AND pol_Current.crrnt_snpsht_flag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim ad ON ad.AgencyDimID = dvdf.AgencyDimID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim ad_Current ON ad.EDWAgencyAKID = ad_Current.EDWAgencyAKID
		AND ad_Current.CurrentSnapshotFlag = 1
	--JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim SD ON SD.AgencyCode = AD_current.LegalPrimaryAgencyCode
	--	AND SD.CurrentSnapshotFlag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.SalesDivisionDim s ON s.SalesDivisionDimID = dvdf.SalesDivisionDimId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_dim ccd ON ccd.contract_cust_dim_id = dvdf.ContractCustomerDimID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_dim ccd_Current ON ccd.edw_contract_cust_ak_id = ccd_Current.edw_contract_cust_ak_id
		AND ccd_Current.crrnt_snpsht_flag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StateDim std ON std.StateDimId = dvdf.StateDimId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim IRD ON IRD.InsuranceReferenceDimId = dvdf.InsuranceReferenceDimId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.BusinessClassDim bcd ON pol_current.BusinessClassDimId = bcd.BusinessClassDimId
		AND bcd.CurrentSnapshotFlag = 1
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
		pol_current.pol_key,
		pol_current.pol_sym,
		pol_current.pol_num,
		pol_current.pol_mod,
		std.StateCode,
		c.clndr_yr,
		c.clndr_month,
		c.clndr_month,
		c.clndr_qtr,
		c.clndr_yr,
		c.clndr_month,
		ccd_current.cust_num,
		ccd_current.NAME,
		pol_current.prim_bus_class_code,
		pol_current.prim_bus_class_code_descript,
		ad_current.AssignedStateCode,
		ad_current.AgencyCode,
		ad_current.AgencyDoingBusinessAsName,
		s.RegionalSalesManagerDisplayName,
	--	LEFT(SD.LegalPrimaryAgencyCode, 2),
	--	SD.LegalPrimaryAgencyCode,
	--	SD.AgencyDoingBusinessAsName,
		pol_current.state_of_domicile_code,
		ccd_current.sic_code,
		ccd_current.sic_code_descript,
		s.SalesTerritoryCodeDescription,
		pol_current.orig_incptn_date,
		pol_current.pol_eff_date,
		pol_current.pol_exp_date,
		CASE WHEN pol_current.pol_issue_code = 'N' THEN 'Y' ELSE 'N' END,
		ird.StrategicProfitCenterCode,
		isnull(bcd.BusinessSegmentCode, 'N/A'),
		isnull(bcd.StrategicBusinessGroupCode, 'N/A'),
		pol_Current.edw_pol_ak_id,
		ad_Current.AgencyStatusCode,
	--	SD.AgencyStatusCode,
		ad_Current.EDWAgencyAKID,
	       pol_current.serv_center_support_code,
	      pol_current.QuoteChannel,
	     pol_current.QuoteChannelOrigin,
		 std.StateAbbreviation,
		 ccd.mailing_zip_postal_code
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
EXP_Values AS (
	SELECT
	SQ_MonthlyActuarialAnalysis39_Dividend.ent_grp_abbr,
	SQ_MonthlyActuarialAnalysis39_Dividend.spc_abbr,
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
	SQ_MonthlyActuarialAnalysis39_Dividend.legal_ent_abbr,
	SQ_MonthlyActuarialAnalysis39_Dividend.pol_offering_code,
	SQ_MonthlyActuarialAnalysis39_Dividend.pol_offering_abbr,
	SQ_MonthlyActuarialAnalysis39_Dividend.product_code,
	SQ_MonthlyActuarialAnalysis39_Dividend.product_abbr,
	SQ_MonthlyActuarialAnalysis39_Dividend.lob_code,
	SQ_MonthlyActuarialAnalysis39_Dividend.lob_abbr,
	SQ_MonthlyActuarialAnalysis39_Dividend.lob_desc,
	SQ_MonthlyActuarialAnalysis39_Dividend.ins_seg_code,
	SQ_MonthlyActuarialAnalysis39_Dividend.ins_seg_desc,
	SQ_MonthlyActuarialAnalysis39_Dividend.prog_code,
	SQ_MonthlyActuarialAnalysis39_Dividend.prog_desc,
	SQ_MonthlyActuarialAnalysis39_Dividend.assn_code,
	SQ_MonthlyActuarialAnalysis39_Dividend.assn_desc,
	'N/A' AS cov_summ_code,
	'N/A' AS cov_summ_desc,
	'N/A' AS cov_grp_code,
	'N/A' AS cov_grp_desc,
	'N/A' AS cov_code,
	'N/A' AS cov_desc,
	'N/A' AS cov_trigger_type,
	'N/A' AS rating_plan,
	'N/A' AS type_of_loss,
	SQ_MonthlyActuarialAnalysis39_Dividend.pol_key,
	SQ_MonthlyActuarialAnalysis39_Dividend.pol_sym,
	SQ_MonthlyActuarialAnalysis39_Dividend.pol_no,
	SQ_MonthlyActuarialAnalysis39_Dividend.pol_ver,
	SQ_MonthlyActuarialAnalysis39_Dividend.rating_state_code,
	null AS loss_year,
	null AS loss_qtr,
	null AS loss_mo,
	null AS claim_cat_code,
	SQ_MonthlyActuarialAnalysis39_Dividend.year,
	SQ_MonthlyActuarialAnalysis39_Dividend.month,
	SQ_MonthlyActuarialAnalysis39_Dividend.acctg_qtr,
	SQ_MonthlyActuarialAnalysis39_Dividend.acctg_year,
	SQ_MonthlyActuarialAnalysis39_Dividend.acctg_mo,
	'N/A' AS producer_code,
	'N/A' AS producer_name,
	SQ_MonthlyActuarialAnalysis39_Dividend.cust_num,
	SQ_MonthlyActuarialAnalysis39_Dividend.insured_name,
	'N/A' AS asl_code,
	'N/A' AS asl_code_desc,
	'N/A' AS sched_p_code,
	'N/A' AS sched_p_desc,
	SQ_MonthlyActuarialAnalysis39_Dividend.BusinessSegmentCode AS bus_seg_code,
	LKP_BusinessClassDim_SBG.BusinessSegmentDescription AS bus_seg_desc,
	SQ_MonthlyActuarialAnalysis39_Dividend.prim_bus_class_code,
	SQ_MonthlyActuarialAnalysis39_Dividend.prim_bus_class_desc,
	SQ_MonthlyActuarialAnalysis39_Dividend.agency_state_code,
	SQ_MonthlyActuarialAnalysis39_Dividend.agency_code,
	SQ_MonthlyActuarialAnalysis39_Dividend.agency_name,
	SQ_MonthlyActuarialAnalysis39_Dividend.rsm_name,
	'N/A' AS uw_name,
	'N/A' AS asst_uw_name,
	'N/A' AS uw_mgr_name,
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
	SQ_MonthlyActuarialAnalysis39_Dividend.new_bus_indic,
	SQ_MonthlyActuarialAnalysis39_Dividend.prim_rating_state_code,
	SQ_MonthlyActuarialAnalysis39_Dividend.sic_code,
	SQ_MonthlyActuarialAnalysis39_Dividend.sic_code_desc,
	'N/A' AS sub_asl_code,
	'N/A' AS sub_asl_code_desc,
	'N/A' AS sub_non_asl_code,
	'N/A' AS sub_non_asl_code_desc,
	SQ_MonthlyActuarialAnalysis39_Dividend.sal_terri_desc,
	'N/A' AS uw_region_name,
	SQ_MonthlyActuarialAnalysis39_Dividend.pol_orig_incptn_date,
	SQ_MonthlyActuarialAnalysis39_Dividend.pol_eff_date,
	SQ_MonthlyActuarialAnalysis39_Dividend.pol_exp_date,
	LKP_PolicyCurrentStatusDim.PolicyCancellationDate AS pol_canc_date,
	'N/A' AS acctg_prod_abbr,
	'N/A' AS pma_code,
	'N/A' AS pma_desc,
	'N/A' AS ilf_tbl,
	'0' AS claim_num,
	-- *INF*: LAST_DAY(ADD_TO_DATE(ROUND(SYSDATE,'DD'), 'MM', -1))
	LAST_DAY(DATEADD(MONTH,- 1,ROUND(CURRENT_TIMESTAMP, 'DD'))) AS last_booked_date,
	0 AS pass_thru_amt,
	SQ_MonthlyActuarialAnalysis39_Dividend.div_pay_amt,
	0 AS dep,
	0 AS dwp,
	0 AS subj_dep,
	0 AS subj_dwp,
	0 AS other_dep,
	0 AS other_dwp,
	0 AS expmod_dep,
	0 AS expmod_dwp,
	0 AS schmod_dep,
	0 AS schmod_dwp,
	0 AS loss_inc,
	0 AS loss_pd,
	0 AS alae_pd,
	0 AS alae_inc,
	SQ_MonthlyActuarialAnalysis39_Dividend.ph_div_payable,
	'N/A' AS agency_contigent_ind,
	'N/A' AS gl_class_grp,
	'N/A' AS prop_rating_group,
	'N/A' AS prop_rate_type,
	'N/A' AS prop_col_grp,
	'N/A' AS auto_veh_type,
	'N/A' AS auto_use_class,
	'N/A' AS auto_radius,
	'N/A' AS auto_fleet_type,
	'N/A' AS auto_sec_class_grp,
	'N/A' AS crime_ind_grp,
	'N/A' AS prop_spec_col_cat,
	'Dividend' AS Rec_Type,
	year*10+3 AS year_rt,
	SQ_MonthlyActuarialAnalysis39_Dividend.StrategicBusinessGroupCode AS sbg_code,
	LKP_BusinessClassDim_SBG.StrategicBusinessGroupDescription AS sbg_desc,
	'N/A' AS ISOMajorCrimeGroup,
	SQ_MonthlyActuarialAnalysis39_Dividend.AgencyStatusCode AS i_AgencyStatusCode,
	-- *INF*: IIF(ISNULL(i_AgencyStatusCode),'0',i_AgencyStatusCode)
	IFF(i_AgencyStatusCode IS NULL, '0', i_AgencyStatusCode) AS v_AgencyStatusCode,
	v_AgencyStatusCode AS o_AgencyStatusCode,
	LKP_AgencyRelationship.prim_agency_status_code AS i_PrimaryAgencyStatusCode,
	-- *INF*: IIF(ISNULL(i_PrimaryAgencyStatusCode),'0',i_PrimaryAgencyStatusCode)
	IFF(i_PrimaryAgencyStatusCode IS NULL, '0', i_PrimaryAgencyStatusCode) AS v_PrimaryAgencyStatusCode,
	v_PrimaryAgencyStatusCode AS o_PrimaryAgencyStatusCode,
	'N/A' AS rated_cov_desc,
	'N/A' AS rated_cov_code,
	'N/A' AS CBG_County,
	'N/A' AS CBG_Tract,
	'N/A' AS CBG_Block,
	'N/A' AS Territory_code,
	000.000000 AS Latitude,
	000.000000 AS Longitude,
	'N/A' AS Catalyst,
	'N/A' AS CauseOfDamage,
	'N/A' AS DamageCaused,
	'N/A' AS ItemDamaged,
	SQ_MonthlyActuarialAnalysis39_Dividend.serv_center_support_code,
	'0' AS o_default_value,
	SQ_MonthlyActuarialAnalysis39_Dividend.QuoteChannel AS i_QuoteChannel,
	-- *INF*: iif(isnull(i_QuoteChannel),'N/A',i_QuoteChannel)
	IFF(i_QuoteChannel IS NULL, 'N/A', i_QuoteChannel) AS v_QuoteChannel,
	v_QuoteChannel AS o_QuoteChannel,
	SQ_MonthlyActuarialAnalysis39_Dividend.QuoteChannelOrigin AS i_QuoteChannelOrigin,
	-- *INF*: iif(isnull(i_QuoteChannelOrigin),'N/A',i_QuoteChannelOrigin)
	IFF(i_QuoteChannelOrigin IS NULL, 'N/A', i_QuoteChannelOrigin) AS v_QuoteChannelOrigin,
	v_QuoteChannelOrigin AS o_QuoteChannelOrigin,
	SQ_MonthlyActuarialAnalysis39_Dividend.RatingStateAbbreviation AS i_RatingStateAbbreviation,
	-- *INF*: IIF(ISNULL(i_RatingStateAbbreviation),'N/A',i_RatingStateAbbreviation)
	IFF(i_RatingStateAbbreviation IS NULL, 'N/A', i_RatingStateAbbreviation) AS v_RatingStateAbbreviation,
	v_RatingStateAbbreviation AS o_RatingStateAbbreviation,
	SQ_MonthlyActuarialAnalysis39_Dividend.mailing_zip_postal_code,
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
	-- || '^' || 'N/A'      --cov_summ_code
	-- || '^' || 'N/A'      --cov_summ_desc
	-- || '^' || 'N/A'      --cov_grp_code
	-- || '^' || 'N/A'      --cov_grp_desc
	-- || '^' || 'N/A'      --cov_code
	-- || '^' || 'N/A'      --cov_desc
	-- || '^' || 'N/A'      --cov_trigger_type
	-- || '^' || 'N/A'      --rating_plan
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
	-- || '^' || 'N/A'      --producer_code
	-- || '^' || 'N/A'      --producer_name
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(cust_num))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(insured_name))
	-- || '^' || 'N/A'      --asl_code
	-- || '^' || 'N/A'      --asl_code_desc
	-- || '^' || 'N/A'      --sched_p_code
	-- || '^' || 'N/A'      --sched_p_desc
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
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_AgencyStatusCode))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(rsm_name))
	-- || '^' || 'N/A'      --uw_name
	-- || '^' || 'N/A'      --asst_uw_name
	-- || '^' || 'N/A'      --uw_mgr_name
	-- 
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_prim_agency_state_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_prim_agency_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_prim_agency_name))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_PrimaryAgencyStatusCode))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(new_bus_indic))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(prim_rating_state_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(sic_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(sic_code_desc))
	-- || '^' || 'N/A'      --sub_asl_code
	-- || '^' || 'N/A'      --sub_asl_code_desc
	-- || '^' || 'N/A'      --sub_non_asl_code
	-- || '^' || 'N/A'      --sub_non_asl_code_desc
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(sal_terri_desc))
	-- || '^' || 'N/A'      --uw_region_name
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(pol_orig_incptn_date) )
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(pol_eff_date))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(pol_exp_date))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(pol_canc_date))
	-- 
	-- || '^' || 'N/A'      --acctg_prod_abbr)
	-- || '^' || 'N/A'      --pma_code
	-- || '^' || 'N/A'      --pma_desc
	-- || '^' || 'N/A'      --ilf_tbl
	-- || '^' || '0'  --claim_num
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(TO_CHAR(LAST_DAY(ADD_TO_DATE(ROUND(SYSDATE,'DD'), 'MM', -1)),'YYYY-MM-DD'))
	-- || '^' || '0.00'         --pass_thru_amt
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(ROUND(div_pay_amt,2)))
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
	-- || '^' || '0.00'
	-- || '^' || '0.00'
	-- || '^' || '0.00'
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(ROUND(ph_div_payable,2)))
	-- || '^' || 'N/A'      --agency_contigent_ind
	-- || '^' || 'N/A'
	-- || '^' || 'N/A'
	-- || '^' || 'N/A'
	-- || '^' || 'N/A'
	-- || '^' || 'N/A'
	-- || '^' || 'N/A'
	-- || '^' || 'N/A'
	-- || '^' || 'N/A'
	-- || '^' || 'N/A'
	-- || '^' || 'N/A'
	-- || '^' || 'N/A'
	-- || '^' || 'N/A'    --ISOMajorCrimeGroup
	-- || '^' || 'Dividend'
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(year*10+3))
	-- || '^' || 'N/A'      --rated_cov_code
	-- || '^' || 'N/A'      --rated_cov_desc
	-- || '^' || 'N/A'      --CBG_County
	-- || '^' || 'N/A'      --CBG_Tract
	-- || '^' || 'N/A'      --CBG_Block
	-- || '^' || 'N/A'      --Territory_code
	-- || '^' || '0.00'      --Latitude
	-- || '^' || '0.00'      --Longitude
	-- || '^' || 'N/A'      --Catalyst
	-- || '^' || 'N/A'      --CauseOfDamage
	-- || '^' || 'N/A'      --DamageCaused
	-- || '^' || 'N/A'      --ItemDamaged
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(serv_center_support_code))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_QuoteChannel))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_QuoteChannelOrigin))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(v_RatingStateAbbreviation))
	-- || '^' || :UDF.DEFAULT_VALUE_FOR_STRINGS(to_char(mailing_zip_postal_code))
	-- )
	MD5(UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(ent_grp_abbr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(spc_abbr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_pc_abbr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(legal_ent_abbr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_offering_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_offering_abbr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(product_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(product_abbr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(lob_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(lob_abbr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(lob_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(ins_seg_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(ins_seg_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(prog_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(prog_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(assn_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(assn_desc)) || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_key)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_sym)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_no)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_ver)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(rating_state_code)) || '^' || 'NULL' || '^' || 'NULL' || '^' || 'NULL' || '^' || 'NULL' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(year)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(month)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(acctg_qtr)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(acctg_year)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(acctg_mo)) || '^' || 'N/A' || '^' || 'N/A' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(cust_num)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(insured_name)) || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(bus_seg_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(bus_seg_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sbg_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sbg_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(prim_bus_class_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(prim_bus_class_desc)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(agency_state_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(agency_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(agency_name)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_AgencyStatusCode)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(rsm_name)) || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_prim_agency_state_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_prim_agency_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_prim_agency_name)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_PrimaryAgencyStatusCode)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(new_bus_indic)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(prim_rating_state_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sic_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sic_code_desc)) || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(sal_terri_desc)) || '^' || 'N/A' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_orig_incptn_date)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_eff_date)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_exp_date)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(pol_canc_date)) || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || '0' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(TO_CHAR(LAST_DAY(DATEADD(MONTH,- 1,ROUND(CURRENT_TIMESTAMP, 'DD'))), 'YYYY-MM-DD')) || '^' || '0.00' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(ROUND(div_pay_amt, 2))) || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || '0.00' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(ROUND(ph_div_payable, 2))) || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'Dividend' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(year * 10 + 3)) || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || '0.00' || '^' || '0.00' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || 'N/A' || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(serv_center_support_code)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_QuoteChannel)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_QuoteChannelOrigin)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(v_RatingStateAbbreviation)) || '^' || UDF_DEFAULT_VALUE_FOR_STRINGS(to_char(mailing_zip_postal_code))) AS v_HashKey,
	v_HashKey AS HashKey
	FROM SQ_MonthlyActuarialAnalysis39_Dividend
	LEFT JOIN LKP_AgencyRelationship
	ON LKP_AgencyRelationship.EDWAgencyAKID = SQ_MonthlyActuarialAnalysis39_Dividend.EDWAgencyAKID
	LEFT JOIN LKP_BusinessClassDim_SBG
	ON LKP_BusinessClassDim_SBG.BusinessSegmentCode = SQ_MonthlyActuarialAnalysis39_Dividend.BusinessSegmentCode AND LKP_BusinessClassDim_SBG.StrategicBusinessGroupCode = SQ_MonthlyActuarialAnalysis39_Dividend.StrategicBusinessGroupCode
	LEFT JOIN LKP_PolicyCurrentStatusDim
	ON LKP_PolicyCurrentStatusDim.EDWPolicyAKId = SQ_MonthlyActuarialAnalysis39_Dividend.edw_pol_ak_id
),
LKP_AA39Monthly AS (
	SELECT
	HashKey,
	in_HashKey
	FROM (
		SELECT HashKey AS HashKey
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.@{pipeline().parameters.TARGET_TABLE_NAME}
		WHERE Rec_Type = 'Dividend' 
		@{pipeline().parameters.WHERE_CLAUSE_LKP} --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY HashKey ORDER BY HashKey) = 1
),
EXP_Detect AS (
	SELECT
	LKP_AA39Monthly.HashKey AS lkp_HashKey,
	EXP_Values.ent_grp_abbr,
	EXP_Values.spc_abbr,
	EXP_Values.pc_abbr,
	EXP_Values.legal_ent_abbr,
	EXP_Values.pol_offering_code,
	EXP_Values.pol_offering_abbr,
	EXP_Values.product_code,
	EXP_Values.product_abbr,
	EXP_Values.lob_code,
	EXP_Values.lob_abbr,
	EXP_Values.lob_desc,
	EXP_Values.ins_seg_code,
	EXP_Values.ins_seg_desc,
	EXP_Values.prog_code,
	EXP_Values.prog_desc,
	EXP_Values.assn_code,
	EXP_Values.assn_desc,
	EXP_Values.cov_summ_code,
	EXP_Values.cov_summ_desc,
	EXP_Values.cov_grp_code,
	EXP_Values.cov_grp_desc,
	EXP_Values.cov_code,
	EXP_Values.cov_desc,
	EXP_Values.cov_trigger_type,
	EXP_Values.rating_plan,
	EXP_Values.type_of_loss,
	EXP_Values.pol_key,
	EXP_Values.pol_sym,
	EXP_Values.pol_no,
	EXP_Values.pol_ver,
	EXP_Values.rating_state_code,
	EXP_Values.loss_year,
	EXP_Values.loss_qtr,
	EXP_Values.loss_mo,
	EXP_Values.year,
	EXP_Values.month,
	EXP_Values.acctg_qtr,
	EXP_Values.acctg_year,
	EXP_Values.acctg_mo,
	EXP_Values.producer_code,
	EXP_Values.producer_name,
	EXP_Values.cust_num,
	EXP_Values.insured_name,
	EXP_Values.asl_code,
	EXP_Values.asl_code_desc,
	EXP_Values.sched_p_code,
	EXP_Values.sched_p_desc,
	EXP_Values.bus_seg_code,
	EXP_Values.bus_seg_desc,
	EXP_Values.prim_bus_class_code,
	EXP_Values.prim_bus_class_desc,
	EXP_Values.agency_state_code,
	EXP_Values.agency_code,
	EXP_Values.agency_name,
	EXP_Values.rsm_name,
	EXP_Values.uw_name,
	EXP_Values.asst_uw_name,
	EXP_Values.uw_mgr_name,
	EXP_Values.o_prim_agency_state_code AS prim_agency_state_code,
	EXP_Values.o_prim_agency_code AS prim_agency_code,
	EXP_Values.o_prim_agency_name AS prim_agency_name,
	EXP_Values.new_bus_indic,
	EXP_Values.prim_rating_state_code,
	EXP_Values.sic_code,
	EXP_Values.sic_code_desc,
	EXP_Values.sub_asl_code,
	EXP_Values.sub_asl_code_desc,
	EXP_Values.sub_non_asl_code,
	EXP_Values.sub_non_asl_code_desc,
	EXP_Values.sal_terri_desc,
	EXP_Values.uw_region_name,
	EXP_Values.pol_orig_incptn_date,
	EXP_Values.pol_eff_date,
	EXP_Values.pol_exp_date,
	EXP_Values.pol_canc_date,
	EXP_Values.acctg_prod_abbr,
	EXP_Values.pma_code,
	EXP_Values.pma_desc,
	EXP_Values.ilf_tbl,
	EXP_Values.claim_num,
	EXP_Values.last_booked_date,
	EXP_Values.pass_thru_amt,
	EXP_Values.div_pay_amt,
	EXP_Values.dep,
	EXP_Values.dwp,
	EXP_Values.subj_dep,
	EXP_Values.subj_dwp,
	EXP_Values.other_dep,
	EXP_Values.other_dwp,
	EXP_Values.expmod_dep,
	EXP_Values.expmod_dwp,
	EXP_Values.schmod_dep,
	EXP_Values.schmod_dwp,
	EXP_Values.loss_inc,
	EXP_Values.loss_pd,
	EXP_Values.alae_pd,
	EXP_Values.alae_inc,
	EXP_Values.ph_div_payable,
	EXP_Values.agency_contigent_ind,
	EXP_Values.gl_class_grp,
	EXP_Values.prop_rating_group,
	EXP_Values.prop_rate_type,
	EXP_Values.prop_col_grp,
	EXP_Values.auto_veh_type,
	EXP_Values.auto_use_class,
	EXP_Values.auto_radius,
	EXP_Values.auto_fleet_type,
	EXP_Values.auto_sec_class_grp,
	EXP_Values.crime_ind_grp,
	EXP_Values.prop_spec_col_cat,
	EXP_Values.Rec_Type,
	EXP_Values.year_rt,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_HashKey),'INSERT',
	-- 'UNCHANGE')
	DECODE(
	    TRUE,
	    lkp_HashKey IS NULL, 'INSERT',
	    'UNCHANGE'
	) AS v_ChangeFlag,
	v_ChangeFlag AS o_ChangeFlag,
	EXP_Values.sbg_code,
	EXP_Values.sbg_desc,
	EXP_Values.ISOMajorCrimeGroup,
	EXP_Values.o_AgencyStatusCode AS AgencyStatusCode,
	EXP_Values.o_PrimaryAgencyStatusCode AS PrimaryAgencyStatusCode,
	EXP_Values.rated_cov_desc,
	EXP_Values.rated_cov_code,
	EXP_Values.CBG_County,
	EXP_Values.CBG_Tract,
	EXP_Values.CBG_Block,
	EXP_Values.Territory_code,
	EXP_Values.Latitude,
	EXP_Values.Longitude,
	EXP_Values.Catalyst,
	EXP_Values.CauseOfDamage,
	EXP_Values.DamageCaused,
	EXP_Values.ItemDamaged,
	EXP_Values.serv_center_support_code,
	EXP_Values.claim_cat_code,
	EXP_Values.o_default_value AS CustomerInforceInd,
	EXP_Values.o_default_value AS CustomerProductInforceInd,
	EXP_Values.o_QuoteChannel AS QuoteChannel,
	EXP_Values.o_QuoteChannelOrigin AS QuoteChannelOrigin,
	EXP_Values.o_RatingStateAbbreviation AS RatingStateAbbreviation,
	EXP_Values.mailing_zip_postal_code,
	EXP_Values.HashKey
	FROM EXP_Values
	LEFT JOIN LKP_AA39Monthly
	ON LKP_AA39Monthly.HashKey = EXP_Values.HashKey
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
	CBG_County,
	CBG_Tract,
	CBG_Block,
	Territory_code,
	Latitude,
	Longitude,
	Catalyst,
	CauseOfDamage,
	DamageCaused,
	ItemDamaged,
	serv_center_support_code,
	claim_cat_code,
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
AA39Monthly_Dividend AS (
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
	CBG_COUNTY, 
	CBG_TRACT, 
	CBG_BLOCK, 
	TERRITORY_CODE, 
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
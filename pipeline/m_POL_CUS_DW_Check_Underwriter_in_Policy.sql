WITH
LKP_BondProducts AS (
	SELECT
	ProductCode,
	pol_id
	FROM (
		select distinct p.pol_id as pol_id,
		prod.ProductCode as ProductCode
		from v2.policy p
			join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage pc on p.pol_ak_id = pc.PolicyAKID
				and pc.CurrentSnapshotFlag = 1
			join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage sc on pc.PolicyCoverageAKID = sc.PolicyCoverageAKID
				and sc.CurrentSnapshotFlag = 1
			join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product prod on sc.ProductAKId = prod.ProductAKId
				and prod.CurrentSnapshotFlag = 1
		where p.source_sys_id = 'PMS'
		 and p.crrnt_snpsht_flag = 1
		 and prod.ProductCode in('610','620','630','640','650','660')
		union
		select distinct p.pol_id, prod.ProductCode
		from v2.policy p
			join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage pc on p.pol_ak_id = pc.PolicyAKID
				and pc.CurrentSnapshotFlag = 1
			join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage rc on pc.PolicyCoverageAKID = rc.PolicyCoverageAKID
				and rc.CurrentSnapshotFlag = 1
			join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product prod on rc.ProductAKId = prod.ProductAKId
				and prod.CurrentSnapshotFlag = 1
		where p.source_sys_id = 'DCT'
		 and p.crrnt_snpsht_flag = 1
		 and prod.ProductCode in('610','620','630','640','650','660')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_id ORDER BY ProductCode DESC) = 1
),
LKP_SBAReinsurance AS (
	SELECT
	ReturnIndicator,
	pol_id,
	reins_co_num
	FROM (
		select b.pol_id as pol_id,
		a.reins_co_num as reins_co_num,
		'Y' as ReturnIndicator
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.reinsurance_coverage a
		join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy b 
		on a.pol_ak_id = b.pol_ak_id
		where a.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_id,reins_co_num ORDER BY ReturnIndicator DESC) = 1
),
SQ_policy AS (
	SELECT
		pol_id,
		crrnt_snpsht_flag,
		audit_id,
		eff_from_date,
		eff_to_date,
		source_sys_id,
		created_date,
		modified_date,
		pol_ak_id,
		contract_cust_ak_id,
		agency_ak_id,
		pol_sym,
		pol_num,
		pol_mod,
		pol_key,
		mco,
		pol_co_num,
		pol_eff_date,
		pol_exp_date,
		orig_incptn_date,
		prim_bus_class_code,
		reins_code,
		pms_pol_lob_code,
		pol_co_line_code,
		pol_cancellation_ind,
		pol_cancellation_date,
		pol_cancellation_rsn_code,
		state_of_domicile_code,
		wbconnect_upload_code,
		serv_center_support_code,
		pol_term,
		terrorism_risk_ind,
		prior_pol_key,
		pol_status_code,
		pol_issue_code,
		pol_age,
		industry_risk_grade_code,
		uw_review_yr,
		mvr_request_code,
		renl_code,
		amend_num,
		anniversary_rerate_code,
		pol_audit_frqncy,
		final_audit_code,
		zip_ind,
		guarantee_ind,
		variation_code,
		county,
		non_smoker_disc_code,
		renl_disc,
		renl_safe_driver_disc_count,
		nonrenewal_flag_date,
		audit_complt_date,
		orig_acct_date,
		pol_enter_date,
		excess_claim_code,
		pol_status_on_pif,
		target_mrkt_code,
		pkg_code,
		pol_kind_code,
		bus_seg_code,
		pif_upload_audit_ind,
		err_flag_bal_txn,
		err_flag_bal_reins,
		producer_code_ak_id,
		prdcr_code,
		ClassOfBusiness,
		strtgc_bus_dvsn_ak_id,
		ErrorFlagBalancePremiumTransaction,
		RenewalPolicyNumber,
		RenewalPolicySymbol,
		RenewalPolicyMod,
		BillingType,
		producer_code_id,
		sup_bus_class_code_id,
		sup_pol_term_id,
		sup_pol_status_code_id,
		sup_pol_issue_code_id,
		sup_pol_audit_frqncy_id,
		sup_industry_risk_grade_code_id,
		sup_state_id,
		SurchargeExemptCode,
		SupSurchargeExemptID,
		StrategicProfitCenterAKId,
		InsuranceSegmentAKId,
		PolicyOfferingAKId,
		ProgramAKId,
		AgencyAKId,
		UnderwritingAssociateAKId,
		ObligeeName,
		AutomatedUnderwritingServicesIndicator,
		AutomaticRenewalIndicator
	FROM policy
	WHERE policy.crrnt_snpsht_flag=1 and policy.UnderwritingAssociateAKId=-1
	and policy.pol_eff_date>='2001-01-01'
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Default AS (
	SELECT
	pol_id,
	pol_key,
	StrategicProfitCenterAKId,
	AgencyAKId,
	PolicyOfferingAKId,
	ProgramAKId,
	wbconnect_upload_code
	FROM SQ_policy
),
LKP_Agency_V2 AS (
	SELECT
	AgencyCode,
	LegalName,
	AgencyAKID
	FROM (
		SELECT 
			AgencyCode,
			LegalName,
			AgencyAKID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Agency
		WHERE CurrentSnapshotFlag=1 and TerminatedDate='2100-12-31 00:00:00.000'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyAKID ORDER BY AgencyCode) = 1
),
LKP_PolicyOffering AS (
	SELECT
	PolicyOfferingCode,
	PolicyOfferingDescription,
	i_PolicyOfferingAKId,
	PolicyOfferingAKId
	FROM (
		SELECT 
			PolicyOfferingCode,
			PolicyOfferingDescription,
			i_PolicyOfferingAKId,
			PolicyOfferingAKId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyOfferingAKId ORDER BY PolicyOfferingCode) = 1
),
LKP_Program AS (
	SELECT
	ProgramCode,
	ProgramDescription,
	ProgramAKId
	FROM (
		SELECT 
			ProgramCode,
			ProgramDescription,
			ProgramAKId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Program
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProgramAKId ORDER BY ProgramCode) = 1
),
LKP_StrategicProfitCenter AS (
	SELECT
	StrategicProfitCenterCode,
	StrategicProfitCenterDescription,
	StrategicProfitCenterAKId
	FROM (
		SELECT 
			StrategicProfitCenterCode,
			StrategicProfitCenterDescription,
			StrategicProfitCenterAKId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StrategicProfitCenterAKId ORDER BY StrategicProfitCenterCode) = 1
),
EXP_Data AS (
	SELECT
	EXP_Default.pol_id AS i_pol_id,
	EXP_Default.wbconnect_upload_code AS i_wbconnect_upload_code,
	EXP_Default.pol_key,
	LKP_StrategicProfitCenter.StrategicProfitCenterCode,
	LKP_Agency_V2.AgencyCode,
	LKP_PolicyOffering.PolicyOfferingCode,
	LKP_Program.ProgramCode,
	LKP_StrategicProfitCenter.StrategicProfitCenterDescription,
	LKP_Agency_V2.LegalName,
	LKP_PolicyOffering.PolicyOfferingDescription,
	LKP_Program.ProgramDescription,
	-- *INF*: DECODE(TRUE,
	-- i_wbconnect_upload_code = 'B','Rapid',
	-- IN(:LKP.LKP_BONDPRODUCTS(i_pol_id),'610','620','630','640','650','660')
	-- and :LKP.LKP_SBAREINSURANCE(i_pol_id,'0125') = 'Y','SBA',
	-- :LKP.LKP_BONDPRODUCTS(i_pol_id) = '610','Contract',
	-- IN(:LKP.LKP_BONDPRODUCTS(i_pol_id),'620','630','640','650','660'),'NonContract',
	-- 'N/A'
	-- )
	DECODE(TRUE,
	i_wbconnect_upload_code = 'B', 'Rapid',
	IN(LKP_BONDPRODUCTS_i_pol_id.ProductCode, '610', '620', '630', '640', '650', '660') AND LKP_SBAREINSURANCE_i_pol_id_0125.ReturnIndicator = 'Y', 'SBA',
	LKP_BONDPRODUCTS_i_pol_id.ProductCode = '610', 'Contract',
	IN(LKP_BONDPRODUCTS_i_pol_id.ProductCode, '620', '630', '640', '650', '660'), 'NonContract',
	'N/A') AS o_BondCategory
	FROM EXP_Default
	LEFT JOIN LKP_Agency_V2
	ON LKP_Agency_V2.AgencyAKID = EXP_Default.AgencyAKId
	LEFT JOIN LKP_PolicyOffering
	ON LKP_PolicyOffering.PolicyOfferingAKId = EXP_Default.PolicyOfferingAKId
	LEFT JOIN LKP_Program
	ON LKP_Program.ProgramAKId = EXP_Default.ProgramAKId
	LEFT JOIN LKP_StrategicProfitCenter
	ON LKP_StrategicProfitCenter.StrategicProfitCenterAKId = EXP_Default.StrategicProfitCenterAKId
	LEFT JOIN LKP_BONDPRODUCTS LKP_BONDPRODUCTS_i_pol_id
	ON LKP_BONDPRODUCTS_i_pol_id.pol_id = i_pol_id

	LEFT JOIN LKP_SBAREINSURANCE LKP_SBAREINSURANCE_i_pol_id_0125
	ON LKP_SBAREINSURANCE_i_pol_id_0125.pol_id = i_pol_id
	AND LKP_SBAREINSURANCE_i_pol_id_0125.reins_co_num = '0125'

),
FIL_Validate AS (
	SELECT
	pol_key, 
	StrategicProfitCenterCode, 
	AgencyCode, 
	PolicyOfferingCode, 
	ProgramCode, 
	StrategicProfitCenterDescription, 
	LegalName, 
	PolicyOfferingDescription, 
	ProgramDescription, 
	o_BondCategory AS BondCategory
	FROM EXP_Data
	WHERE AgencyCode<>'N/A' AND NOT IN  (AgencyCode,'16998','14998','21999','34999','16999','34998','12999','22999','98999','26999','55555','13999','24999','15999','48001','48966','14967') 
AND StrategicProfitCenterCode <> '3'
--and PolicyOfferingCode<>'600'
),
RTR_Hierachy AS (
	SELECT
	pol_key,
	StrategicProfitCenterCode,
	AgencyCode,
	PolicyOfferingCode,
	ProgramCode,
	StrategicProfitCenterDescription,
	LegalName,
	PolicyOfferingDescription,
	ProgramDescription,
	BondCategory
	FROM FIL_Validate
),
RTR_Hierachy_PolicyOffering AS (SELECT * FROM RTR_Hierachy WHERE ProgramCode='N/A'),
RTR_Hierachy_Program AS (SELECT * FROM RTR_Hierachy WHERE ProgramCode<>'N/A'),
RTR_Hierachy_Agency AS (SELECT * FROM RTR_Hierachy WHERE TRUE),
RTR_Hierachy_Bond AS (SELECT * FROM RTR_Hierachy WHERE BondCategory<>'N/A'),
LKP_UnderWriterAgencyRelationShip AS (
	SELECT
	UnderwriterAgencyRelationshipId,
	AgencyCode,
	StrategicProfitCenterCode
	FROM (
		select  distinct UAR.UnderwriterAgencyRelationshipId AS UnderwriterAgencyRelationshipId,
		Agency.AgencyCode AS AgencyCode,
		UAR.StrategicProfitCenterCode AS StrategicProfitCenterCode
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwriterAgencyRelationship UAR
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.VWAgency Agency
		on UAR.AgencyId=Agency.AgencyID
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyCode,StrategicProfitCenterCode ORDER BY UnderwriterAgencyRelationshipId) = 1
),
FIL_Agency AS (
	SELECT
	LKP_UnderWriterAgencyRelationShip.UnderwriterAgencyRelationshipId, 
	RTR_Hierachy_Agency.pol_key, 
	RTR_Hierachy_Agency.StrategicProfitCenterCode, 
	RTR_Hierachy_Agency.AgencyCode, 
	RTR_Hierachy_Agency.PolicyOfferingCode, 
	RTR_Hierachy_Agency.ProgramCode, 
	RTR_Hierachy_Agency.StrategicProfitCenterDescription, 
	RTR_Hierachy_Agency.LegalName, 
	RTR_Hierachy_Agency.PolicyOfferingDescription, 
	RTR_Hierachy_Agency.ProgramDescription
	FROM RTR_Hierachy_Agency
	LEFT JOIN LKP_UnderWriterAgencyRelationShip
	ON LKP_UnderWriterAgencyRelationShip.AgencyCode = RTR_Hierachy.AgencyCode4 AND LKP_UnderWriterAgencyRelationShip.StrategicProfitCenterCode = RTR_Hierachy.StrategicProfitCenterCode4
	WHERE ISNULL(UnderwriterAgencyRelationshipId)
),
EXP_Agency AS (
	SELECT
	pol_key,
	StrategicProfitCenterCode,
	AgencyCode,
	PolicyOfferingCode,
	ProgramCode,
	StrategicProfitCenterDescription,
	LegalName,
	PolicyOfferingDescription,
	ProgramDescription,
	1 AS Level
	FROM FIL_Agency
),
SQ_Agency_Bond AS (
	select distinct Associate.DisplayName,
	Agency.AgencyCode,
	UPR.StrategicProfitCenterCode,
	UPR.PolicyOfferingCode,
	UPR.ProgramCode
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.VWAgency Agency
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwriterAgencyRelationship UAR
	on Agency.AgencyID=UAR.AgencyId
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwriterProductRelationship UPR
	on UAR.AssociateId=UPR.AssociateId
	and UAR.StrategicProfitCenterCode=UPR.StrategicProfitCenterCode
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Associate Associate
	on Associate.AssociateID=UAR.AssociateId
	and Associate.AssociateRole='UNDERWRITER'
	and Associate.UserId<>'N/A'
	and UPR.StrategicProfitCenterCode <>'3'
),
JNR_Bond AS (SELECT
	RTR_Hierachy_Bond.pol_key, 
	RTR_Hierachy_Bond.StrategicProfitCenterCode, 
	RTR_Hierachy_Bond.AgencyCode, 
	RTR_Hierachy_Bond.PolicyOfferingCode, 
	RTR_Hierachy_Bond.ProgramCode, 
	RTR_Hierachy_Bond.StrategicProfitCenterDescription, 
	RTR_Hierachy_Bond.LegalName, 
	RTR_Hierachy_Bond.PolicyOfferingDescription, 
	RTR_Hierachy_Bond.ProgramDescription, 
	RTR_Hierachy_Bond.BondCategory, 
	SQ_Agency_Bond.DisplayName, 
	SQ_Agency_Bond.AgencyCode AS i_AgencyCode, 
	SQ_Agency_Bond.StrategicProfitCenterCode AS i_StrategicProfitCenterCode, 
	SQ_Agency_Bond.PolicyOfferingCode AS i_PolicyOfferingCode, 
	SQ_Agency_Bond.ProgramCode AS i_ProgramCode, 
	SQ_Agency_Bond.BondCategory AS i_BondCategory
	FROM RTR_Hierachy_Bond
	LEFT OUTER JOIN SQ_Agency_Bond
	ON SQ_Agency_Bond.StrategicProfitCenterCode = RTR_Hierachy.StrategicProfitCenterCode5 AND SQ_Agency_Bond.AgencyCode = RTR_Hierachy.AgencyCode5 AND SQ_Agency_Bond.PolicyOfferingCode = RTR_Hierachy.PolicyOfferingCode5 AND SQ_Agency_Bond.ProgramCode = RTR_Hierachy.ProgramCode5 AND SQ_Agency_Bond.BondCategory = RTR_Hierachy.BondCategory5
),
EXP_Bond AS (
	SELECT
	pol_key,
	StrategicProfitCenterCode,
	AgencyCode,
	PolicyOfferingCode,
	ProgramCode,
	StrategicProfitCenterDescription,
	LegalName,
	PolicyOfferingDescription,
	ProgramDescription,
	BondCategory,
	DisplayName,
	4 AS Level
	FROM JNR_Bond
),
SQ_Agency_PolicyOffering AS (
	select distinct Associate.DisplayName,
	Agency.AgencyCode,
	UPR.StrategicProfitCenterCode,
	UPR.PolicyOfferingCode
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.VWAgency Agency
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwriterAgencyRelationship UAR
	on Agency.AgencyID=UAR.AgencyId
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwriterProductRelationship UPR
	on UAR.AssociateId=UPR.AssociateId
	and UAR.StrategicProfitCenterCode=UPR.StrategicProfitCenterCode
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Associate Associate
	on Associate.AssociateID=UAR.AssociateId
	and Associate.AssociateRole='UNDERWRITER'
	and Associate.UserId<>'N/A'
	and UPR.StrategicProfitCenterCode <> '3'
),
JNR_PolicyOffering AS (SELECT
	RTR_Hierachy_PolicyOffering.pol_key, 
	RTR_Hierachy_PolicyOffering.StrategicProfitCenterCode, 
	RTR_Hierachy_PolicyOffering.AgencyCode, 
	RTR_Hierachy_PolicyOffering.PolicyOfferingCode, 
	RTR_Hierachy_PolicyOffering.StrategicProfitCenterDescription, 
	RTR_Hierachy_PolicyOffering.LegalName, 
	RTR_Hierachy_PolicyOffering.PolicyOfferingDescription, 
	SQ_Agency_PolicyOffering.DisplayName, 
	SQ_Agency_PolicyOffering.AgencyCode AS AgencyCode_ODS, 
	SQ_Agency_PolicyOffering.StrategicProfitCenterCode AS StrategicProfitCenterCode_ODS, 
	SQ_Agency_PolicyOffering.PolicyOfferingCode AS PolicyOfferingCode_ODS
	FROM RTR_Hierachy_PolicyOffering
	LEFT OUTER JOIN SQ_Agency_PolicyOffering
	ON SQ_Agency_PolicyOffering.AgencyCode = RTR_Hierachy.AgencyCode2 AND SQ_Agency_PolicyOffering.StrategicProfitCenterCode = RTR_Hierachy.StrategicProfitCenterCode2 AND SQ_Agency_PolicyOffering.PolicyOfferingCode = RTR_Hierachy.PolicyOfferingCode2
),
EXP_PolicyOffering AS (
	SELECT
	pol_key,
	StrategicProfitCenterCode,
	AgencyCode,
	PolicyOfferingCode,
	'N/A' AS ProgramCode,
	StrategicProfitCenterDescription,
	LegalName,
	PolicyOfferingDescription,
	'Not Applicable' AS ProgramDescription,
	DisplayName,
	2 AS Level
	FROM JNR_PolicyOffering
),
SQ_Agency_Program AS (
	select distinct Associate.DisplayName,
	Agency.AgencyCode,
	UPR.StrategicProfitCenterCode,
	UPR.PolicyOfferingCode,
	UPR.ProgramCode
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.VWAgency Agency
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwriterAgencyRelationship UAR
	on Agency.AgencyID=UAR.AgencyId
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwriterProductRelationship UPR
	on UAR.AssociateId=UPR.AssociateId
	and UAR.StrategicProfitCenterCode=UPR.StrategicProfitCenterCode
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Associate Associate
	on Associate.AssociateID=UAR.AssociateId
	and Associate.AssociateRole='UNDERWRITER'
	and Associate.UserId<>'N/A'
	and UPR.StrategicProfitCenterCode <> '3'
),
JNR_Program AS (SELECT
	RTR_Hierachy_Program.pol_key, 
	RTR_Hierachy_Program.StrategicProfitCenterCode, 
	RTR_Hierachy_Program.AgencyCode, 
	RTR_Hierachy_Program.PolicyOfferingCode, 
	RTR_Hierachy_Program.ProgramCode, 
	RTR_Hierachy_Program.StrategicProfitCenterDescription, 
	RTR_Hierachy_Program.LegalName, 
	RTR_Hierachy_Program.PolicyOfferingDescription, 
	RTR_Hierachy_Program.ProgramDescription, 
	SQ_Agency_Program.DisplayName, 
	SQ_Agency_Program.AgencyCode AS AgencyCode_ODS, 
	SQ_Agency_Program.StrategicProfitCenterCode AS StrategicProfitCenterCode_ODS, 
	SQ_Agency_Program.PolicyOfferingCode AS PolicyOfferingCode_ODS, 
	SQ_Agency_Program.ProgramCode AS ProgramCode_ODS
	FROM RTR_Hierachy_Program
	LEFT OUTER JOIN SQ_Agency_Program
	ON SQ_Agency_Program.AgencyCode = RTR_Hierachy.AgencyCode3 AND SQ_Agency_Program.StrategicProfitCenterCode = RTR_Hierachy.StrategicProfitCenterCode3 AND SQ_Agency_Program.PolicyOfferingCode = RTR_Hierachy.PolicyOfferingCode3 AND SQ_Agency_Program.ProgramCode = RTR_Hierachy.ProgramCode3
),
EXP_Program AS (
	SELECT
	pol_key,
	StrategicProfitCenterCode,
	AgencyCode,
	PolicyOfferingCode,
	ProgramCode,
	StrategicProfitCenterDescription,
	LegalName,
	PolicyOfferingDescription,
	ProgramDescription,
	DisplayName,
	3 AS Level
	FROM JNR_Program
),
Union AS (
	SELECT pol_key, StrategicProfitCenterCode, AgencyCode, PolicyOfferingCode, ProgramCode, StrategicProfitCenterDescription, LegalName, PolicyOfferingDescription, ProgramDescription, Level
	FROM EXP_Agency
	UNION
	SELECT pol_key, StrategicProfitCenterCode, AgencyCode, PolicyOfferingCode, ProgramCode, StrategicProfitCenterDescription, LegalName, PolicyOfferingDescription, ProgramDescription, DisplayName, Level
	FROM EXP_PolicyOffering
	UNION
	SELECT pol_key, StrategicProfitCenterCode, AgencyCode, PolicyOfferingCode, ProgramCode, StrategicProfitCenterDescription, LegalName, PolicyOfferingDescription, ProgramDescription, DisplayName, Level
	FROM EXP_Program
	UNION
	SELECT pol_key, StrategicProfitCenterCode, AgencyCode, PolicyOfferingCode, ProgramCode, StrategicProfitCenterDescription, LegalName, PolicyOfferingDescription, ProgramDescription, DisplayName, Level, BondCategory
	FROM EXP_Bond
),
AGG_Level AS (
	SELECT
	pol_key, 
	StrategicProfitCenterCode, 
	AgencyCode, 
	PolicyOfferingCode, 
	ProgramCode, 
	StrategicProfitCenterDescription, 
	LegalName, 
	PolicyOfferingDescription, 
	ProgramDescription, 
	DisplayName, 
	BondCategory, 
	Level AS i_Level, 
	MIN(i_Level) AS Level
	FROM Union
	GROUP BY pol_key, StrategicProfitCenterCode, AgencyCode, PolicyOfferingCode, ProgramCode, DisplayName
),
SRT_Policy AS (
	SELECT
	pol_key, 
	StrategicProfitCenterCode, 
	AgencyCode, 
	PolicyOfferingCode, 
	ProgramCode, 
	StrategicProfitCenterDescription, 
	LegalName, 
	PolicyOfferingDescription, 
	ProgramDescription, 
	DisplayName, 
	BondCategory, 
	Level
	FROM AGG_Level
	ORDER BY pol_key ASC, StrategicProfitCenterCode ASC, AgencyCode ASC, PolicyOfferingCode ASC, ProgramCode ASC, DisplayName ASC
),
EXP_MetaData AS (
	SELECT
	pol_key,
	StrategicProfitCenterCode,
	AgencyCode,
	PolicyOfferingCode,
	ProgramCode,
	StrategicProfitCenterDescription,
	LegalName,
	PolicyOfferingDescription,
	ProgramDescription,
	DisplayName AS i_DisplayName,
	BondCategory,
	Level,
	-- *INF*: IIF(pol_key=v_pol_key,i_DisplayName || ', ' || v_AssociateName,i_DisplayName)
	IFF(pol_key = v_pol_key, i_DisplayName || ', ' || v_AssociateName, i_DisplayName) AS v_AssociateName,
	pol_key AS v_pol_key,
	-- *INF*: LTRIM(RTRIM(v_AssociateName))
	LTRIM(RTRIM(v_AssociateName)) AS o_AssociateId_List
	FROM SRT_Policy
),
AGG_Underwriter AS (
	SELECT
	pol_key, 
	StrategicProfitCenterCode, 
	AgencyCode, 
	PolicyOfferingCode, 
	ProgramCode, 
	StrategicProfitCenterDescription, 
	LegalName, 
	PolicyOfferingDescription, 
	ProgramDescription, 
	BondCategory, 
	Level, 
	o_AssociateId_List AS AssociateId_List, 
	LAST(AssociateId_List) AS o_AssociateId_List, 
	COUNT(1) AS o_Count
	FROM EXP_MetaData
	GROUP BY pol_key, StrategicProfitCenterCode, AgencyCode, PolicyOfferingCode, ProgramCode, Level
),
EXP_Result AS (
	SELECT
	pol_key,
	StrategicProfitCenterCode,
	AgencyCode,
	PolicyOfferingCode,
	ProgramCode,
	StrategicProfitCenterDescription,
	LegalName,
	PolicyOfferingDescription,
	ProgramDescription,
	BondCategory,
	Level,
	o_AssociateId_List AS AssociateId_List,
	@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID} AS o_Session_Id,
	'E' AS o_type_code,
	-- *INF*: DECODE(TRUE,
	-- Level=1,'Policy: ' || pol_key
	--  || ';  StrategicProfitCenter: ' || StrategicProfitCenterCode
	--  || ',  ' || StrategicProfitCenterDescription
	--  || ';  AgencyCode: ' || AgencyCode
	--  || ',  ' || LegalName
	--  || ' does not exist in AgencyODS',
	-- Level=2,IIF(NOT ISNULL(AssociateId_List),'Policy: ' || pol_key
	--  || ';  StrategicProfitCenter: ' || StrategicProfitCenterCode
	--  || ',  ' || StrategicProfitCenterDescription
	--  || ';  AgencyCode: ' || AgencyCode
	--  || ',  ' || LegalName
	--  || ';  PolicyOffering:  ' || PolicyOfferingCode
	--  || ',  ' || PolicyOfferingDescription
	--  || ' has underwriter: '  || AssociateId_List, 
	-- 'Policy: ' || pol_key
	--  || ';  StrategicProfitCenter: ' || StrategicProfitCenterCode
	--  || ',  ' || StrategicProfitCenterDescription
	--  || ';  AgencyCode: ' || AgencyCode
	--  || ',  ' || LegalName
	--  || ';  PolicyOffering: ' || PolicyOfferingCode
	--  || ',  ' || PolicyOfferingDescription
	--  || ' does not exist in AgencyODS'),
	-- Level=3,IIF(NOT ISNULL(AssociateId_List),'Policy: ' || pol_key
	--  || ';  StrategicProfitCenter: ' || StrategicProfitCenterCode
	--  || ',  ' || StrategicProfitCenterDescription
	--  || ';  AgencyCode: ' || AgencyCode
	--  || ',  ' || LegalName
	--  || ';  PolicyOffering: ' || PolicyOfferingCode
	--  || ',  ' || PolicyOfferingDescription
	--  || ';  Program: ' || ProgramCode
	--  || ',  ' || ProgramDescription
	--  || ' has underwriter: '  || AssociateId_List, 
	-- 'Policy: ' || pol_key
	--  || ';  StrategicProfitCenter: ' || StrategicProfitCenterCode
	--  || ',  ' || StrategicProfitCenterDescription
	--  || ';  AgencyCode: ' || AgencyCode
	--  || ',  ' || LegalName
	--  || ';  PolicyOffering: ' || PolicyOfferingCode
	--  || ',  ' || PolicyOfferingDescription
	--  || ';  Program: ' || ProgramCode
	--  || ',  ' || ProgramDescription
	--  || ' does not exist in AgencyODS'),
	-- Level=4,IIF(NOT ISNULL(AssociateId_List),'Policy: ' || pol_key
	--  || ';  StrategicProfitCenter: ' || StrategicProfitCenterCode
	--  || ',  ' || StrategicProfitCenterDescription
	--  || ';  AgencyCode: ' || AgencyCode
	--  || ',  ' || LegalName
	--  || ';  PolicyOffering: ' || PolicyOfferingCode
	--  || ',  ' || PolicyOfferingDescription
	--  || ';  Program: ' || ProgramCode
	--  || ',  ' || ProgramDescription
	--  || ',  Bond Category: ' || BondCategory
	--  || ' has underwriter: '  || AssociateId_List, 
	-- 'Policy: ' || pol_key
	--  || ';  StrategicProfitCenter: ' || StrategicProfitCenterCode
	--  || ',  ' || StrategicProfitCenterDescription
	--  || ';  AgencyCode: ' || AgencyCode
	--  || ',  ' || LegalName
	--  || ';  PolicyOffering: ' || PolicyOfferingCode
	--  || ',  ' || PolicyOfferingDescription
	--  || ';  Program: ' || ProgramCode
	--  || ',  ' || ProgramDescription
	--  || ',  Bond Category: ' || BondCategory
	--  || ' does not exist in AgencyODS'))
	DECODE(TRUE,
	Level = 1, 'Policy: ' || pol_key || ';  StrategicProfitCenter: ' || StrategicProfitCenterCode || ',  ' || StrategicProfitCenterDescription || ';  AgencyCode: ' || AgencyCode || ',  ' || LegalName || ' does not exist in AgencyODS',
	Level = 2, IFF(NOT AssociateId_List IS NULL, 'Policy: ' || pol_key || ';  StrategicProfitCenter: ' || StrategicProfitCenterCode || ',  ' || StrategicProfitCenterDescription || ';  AgencyCode: ' || AgencyCode || ',  ' || LegalName || ';  PolicyOffering:  ' || PolicyOfferingCode || ',  ' || PolicyOfferingDescription || ' has underwriter: ' || AssociateId_List, 'Policy: ' || pol_key || ';  StrategicProfitCenter: ' || StrategicProfitCenterCode || ',  ' || StrategicProfitCenterDescription || ';  AgencyCode: ' || AgencyCode || ',  ' || LegalName || ';  PolicyOffering: ' || PolicyOfferingCode || ',  ' || PolicyOfferingDescription || ' does not exist in AgencyODS'),
	Level = 3, IFF(NOT AssociateId_List IS NULL, 'Policy: ' || pol_key || ';  StrategicProfitCenter: ' || StrategicProfitCenterCode || ',  ' || StrategicProfitCenterDescription || ';  AgencyCode: ' || AgencyCode || ',  ' || LegalName || ';  PolicyOffering: ' || PolicyOfferingCode || ',  ' || PolicyOfferingDescription || ';  Program: ' || ProgramCode || ',  ' || ProgramDescription || ' has underwriter: ' || AssociateId_List, 'Policy: ' || pol_key || ';  StrategicProfitCenter: ' || StrategicProfitCenterCode || ',  ' || StrategicProfitCenterDescription || ';  AgencyCode: ' || AgencyCode || ',  ' || LegalName || ';  PolicyOffering: ' || PolicyOfferingCode || ',  ' || PolicyOfferingDescription || ';  Program: ' || ProgramCode || ',  ' || ProgramDescription || ' does not exist in AgencyODS'),
	Level = 4, IFF(NOT AssociateId_List IS NULL, 'Policy: ' || pol_key || ';  StrategicProfitCenter: ' || StrategicProfitCenterCode || ',  ' || StrategicProfitCenterDescription || ';  AgencyCode: ' || AgencyCode || ',  ' || LegalName || ';  PolicyOffering: ' || PolicyOfferingCode || ',  ' || PolicyOfferingDescription || ';  Program: ' || ProgramCode || ',  ' || ProgramDescription || ',  Bond Category: ' || BondCategory || ' has underwriter: ' || AssociateId_List, 'Policy: ' || pol_key || ';  StrategicProfitCenter: ' || StrategicProfitCenterCode || ',  ' || StrategicProfitCenterDescription || ';  AgencyCode: ' || AgencyCode || ',  ' || LegalName || ';  PolicyOffering: ' || PolicyOfferingCode || ',  ' || PolicyOfferingDescription || ';  Program: ' || ProgramCode || ',  ' || ProgramDescription || ',  Bond Category: ' || BondCategory || ' does not exist in AgencyODS')) AS o_Msg,
	'V2.policy' AS o_name,
	o_Count AS Count,
	SYSDATE AS o_Sysdate,
	'InformS' AS o_user,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM AGG_Underwriter
),
wbmi_checkout_Underwriter AS (
	INSERT INTO wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, source_name, target_count, created_user_id, created_date, modified_user_id, modified_date, AuditID)
	SELECT 
	o_Session_Id AS WBMI_SESSION_CONTROL_RUN_ID, 
	o_type_code AS CHECKOUT_TYPE_CODE, 
	o_Msg AS CHECKOUT_MESSAGE, 
	o_name AS SOURCE_NAME, 
	Count AS TARGET_COUNT, 
	o_user AS CREATED_USER_ID, 
	o_Sysdate AS CREATED_DATE, 
	o_user AS MODIFIED_USER_ID, 
	o_Sysdate AS MODIFIED_DATE, 
	o_AuditId AS AUDITID
	FROM EXP_Result
),
SQ_UnderwriterAgencyRelationship AS (
	SELECT
		AgencyID,
		AssociateID
	FROM UnderwriterAgencyRelationship
	WHERE UnderwriterAgencyRelationship.StrategicProfitCenterCode<>'X' and UnderwriterAgencyRelationship.StrategicProfitCenterCode <>'3'
),
LKP_Associate_ODS AS (
	SELECT
	DisplayName,
	AssociateID
	FROM (
		SELECT 
			DisplayName,
			AssociateID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Associate
		WHERE AssociateRole='UNDERWRITER ASSISTANT'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AssociateID ORDER BY DisplayName) = 1
),
LKP_VWAgency_ODS AS (
	SELECT
	AgencyCode,
	LegalName,
	AgencyID
	FROM (
		SELECT 
			AgencyCode,
			LegalName,
			AgencyID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.VWAgency
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyID ORDER BY AgencyCode) = 1
),
FIL_Valid AS (
	SELECT
	LKP_VWAgency_ODS.AgencyCode, 
	LKP_VWAgency_ODS.LegalName, 
	LKP_Associate_ODS.DisplayName
	FROM 
	LEFT JOIN LKP_Associate_ODS
	ON LKP_Associate_ODS.AssociateID = SQ_UnderwriterAgencyRelationship.AssociateID
	LEFT JOIN LKP_VWAgency_ODS
	ON LKP_VWAgency_ODS.AgencyID = SQ_UnderwriterAgencyRelationship.AgencyID
	WHERE NOT ISNULL(AgencyCode) AND NOT ISNULL(DisplayName)
),
AGG_Duplicate AS (
	SELECT
	AgencyCode, 
	LegalName, 
	DisplayName
	FROM FIL_Valid
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyCode, LegalName, DisplayName ORDER BY NULL) = 1
),
EXP_UWA_Check AS (
	SELECT
	AgencyCode,
	LegalName,
	DisplayName,
	@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID} AS o_Session_Id,
	'E' AS o_type_code,
	'For Agency: ' || AgencyCode || '   ' || LegalName || ', UnderwriterAssistant ' || DisplayName
 || ' is assigned which is incorrect.' AS o_Msg,
	'AgencyManagement' AS o_name,
	SYSDATE AS o_Sysdate,
	'InformS' AS o_user,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM AGG_Duplicate
),
wbmi_checkout_UnderwriterAssistant AS (
	INSERT INTO wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, source_name, created_user_id, created_date, modified_user_id, modified_date, AuditID)
	SELECT 
	o_Session_Id AS WBMI_SESSION_CONTROL_RUN_ID, 
	o_type_code AS CHECKOUT_TYPE_CODE, 
	o_Msg AS CHECKOUT_MESSAGE, 
	o_name AS SOURCE_NAME, 
	o_user AS CREATED_USER_ID, 
	o_Sysdate AS CREATED_DATE, 
	o_user AS MODIFIED_USER_ID, 
	o_Sysdate AS MODIFIED_DATE, 
	o_AuditId AS AUDITID
	FROM EXP_UWA_Check
),
SQ_UnderwriterProductRelationship AS (
	select distinct AssociateId
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.underwriterProductRelationship
	where AssociateId not in (select AssociateId 
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwriterAgencyRelationship)
	and StrategicProfitCenterCode<>'X'
	and StrategicProfitCenterCode<>'3'
),
LKP_Associate_ODS_Product AS (
	SELECT
	DisplayName,
	AssociateID
	FROM (
		SELECT 
			DisplayName,
			AssociateID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Associate
		WHERE AssociateRole='UNDERWRITER ASSISTANT'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AssociateID ORDER BY DisplayName) = 1
),
FIL_Valid_Product AS (
	SELECT
	DisplayName
	FROM LKP_Associate_ODS_Product
	WHERE NOT ISNULL(DisplayName)
),
EXP_UWA_Check_Product AS (
	SELECT
	DisplayName,
	@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID} AS o_Session_Id,
	'E' AS o_type_code,
	'UnderwriterAssistant ' || DisplayName
 || ' is assigned in UnderwriterProductRelationShip table which is incorrect.' AS o_Msg,
	'AgencyManagement' AS o_name,
	SYSDATE AS o_Sysdate,
	'InformS' AS o_user,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM FIL_Valid_Product
),
wbmi_checkout_UnderwriterAssistant_Product AS (
	INSERT INTO wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, source_name, created_user_id, created_date, modified_user_id, modified_date, AuditID)
	SELECT 
	o_Session_Id AS WBMI_SESSION_CONTROL_RUN_ID, 
	o_type_code AS CHECKOUT_TYPE_CODE, 
	o_Msg AS CHECKOUT_MESSAGE, 
	o_name AS SOURCE_NAME, 
	o_user AS CREATED_USER_ID, 
	o_Sysdate AS CREATED_DATE, 
	o_user AS MODIFIED_USER_ID, 
	o_Sysdate AS MODIFIED_DATE, 
	o_AuditId AS AUDITID
	FROM EXP_UWA_Check_Product
),
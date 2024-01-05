WITH
LKP_DCStatCodeStaging AS (
	SELECT
	Value,
	SessionId,
	Type
	FROM (
		SELECT 
			Value,
			SessionId,
			Type
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCStatCodeStaging
		WHERE ObjectName='DC_Coverage' and Type in ('Class', 'Subline')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SessionId,Type ORDER BY Value) = 1
),
SQ_reinsurance_coverage_SRC AS (
	WITH Common AS
	( 
	SELECT DCPOL.sessionid,
	       DCT.historyid,
	       DCPOL.Id,
	       DCPOL.lineofbusiness,
	       DCPOL.policynumber,
	       WBPOL.policyversion,
	       DCRI.type,
	       DCRI.aggregatelimit,
	       DCRI.occurrencelimit,
	       DCRI.percentloss,
	       DCRI.company,
	       DCRI.companynumber,
	       DCRI.effectivedate,
	       DCRI.expirationdate,
	       DCT.transactiondate,
	       WBL.locationnumber,
	       WBP.customernum
	FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.dclocationassociationstaging DCLA
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.wblocationstaging WBL
	ON DCLA.SessionId=WBL.SessionId AND DCLA.LocationId=WBL.LocationId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.dcpolicystaging DCPOL
	ON DCPOL.SessionId=WBL.SessionId AND LEN(DCPOL.PolicyNumber)=7
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.wbpolicystaging WBPOL
	ON DCPOL.SessionId=WBPOL.SessionId AND DCPOL.PolicyId=WBPOL.PolicyId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.dcreinsurancestaging DCRI
	ON DCPOL.SessionId=DCRI.SessionId AND DCPOL.PolicyId=DCRI.PolicyId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.dctransactionstaging DCT
	ON DCPOL.SessionId=DCT.SessionId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.wbpartystaging WBP
	ON DCPOL.SessionId=WBP.SessionId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.dcpartyassociationstaging DCPA
	ON WBP.SessionId=DCPA.SessionId AND WBP.PartyId=DCPA.PartyId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.dclocationstaging DCL
	ON DCPOL.SessionId=DCL.SessionId AND WBL.LocationId=DCL.LocationId
	WHERE  DCLA.locationassociationtype NOT IN ('Location', 'Account', 'Agency' )
	AND DCPA.partyassociationtype = 'Account'
	AND DCPOL.status <> 'Quote'
	AND DCL.description = 'Primary Location'
	AND DCT.State='committed' 
	AND DCT.HistoryID =(SELECT MAX(HistoryID) FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCTransactionStaging DCTR WHERE DCTR.SessionId=DCPOL.SessionId)
	)
	
	SELECT Common.sessionid,
	       Common.historyid,
	       Common.Id,
	       Common.lineofbusiness,
	       Common.policynumber,
	       Common.policyversion,
	       Common.type,
	       Common.aggregatelimit,
	       Common.occurrencelimit,
	       Common.percentloss,
	       Common.company,
	       Common.companynumber,
	       Common.effectivedate,
	       Common.expirationdate,
	       Common.transactiondate,
	       Common.locationnumber,
	       Common.customernum,
	       DCGLR.Type AS GLRisk_Type,
	       DCWCR.Description AS WCRisk_Desc
	FROM Common LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCGLRiskStaging DCGLR
	ON Common.SessionId=DCGLR.SessionId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCWCRiskStaging DCWCR
	ON Common.SessionId=DCWCR.SessionId
),
AGG_RemoveDuplicates AS (
	SELECT
	SessionId AS i_SessionId, 
	HistoryID AS i_HistoryID, 
	Id AS i_Id, 
	LineOfBusiness AS i_LineOfBusiness, 
	PolicyNumber AS i_PolicyNumber, 
	PolicyVersion AS i_PolicyVersion, 
	Type AS i_Type, 
	AggregateLimit AS i_AggregateLimit, 
	OccurrenceLimit AS i_OccurrenceLimit, 
	PercentLoss AS i_PercentLoss, 
	Company AS i_Company, 
	CompanyNumber AS i_CompanyNumber, 
	EffectiveDate AS i_EffectiveDate, 
	ExpirationDate AS i_ExpirationDate, 
	TransactionDate AS i_TransactionDate, 
	LocationNumber AS i_LocationNumber, 
	CustomerNum, 
	GLRisk_Type AS i_GLRisk_Type, 
	WCRisk_Desc AS i_WCRisk_Desc, 
	DECODE(TRUE,
	i_LineOfBusiness = 'GeneralLiability', i_GLRisk_Type,
	i_LineOfBusiness = 'WorkersCompensation', i_WCRisk_Desc,
	'TBD') AS v_reins_risk_unit_grp, 
	IFF(i_LineOfBusiness = 'GeneralLiability' OR i_LineOfBusiness = 'WorkersCompensation', , 'TBD') AS v_reins_risk_unit, 
	IFF(CustomerNum IS NULL OR IS_SPACES(CustomerNum) OR LENGTH(CustomerNum) = 0, 'N/A', LTRIM(RTRIM(CustomerNum))) AS o_CustomerNumber, 
	IFF(i_Id IS NULL OR IS_SPACES(i_Id) OR LENGTH(i_Id) = 0, 'N/A', LTRIM(RTRIM(i_Id))) AS o_Id, 
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_PolicyNumber) AS o_PolicyNumber, 
	IFF(i_PolicyVersion IS NULL, '00', LPAD(TO_CHAR(i_PolicyVersion), 2, '0')) AS o_PolicyVersion, 
	IFF(i_LineOfBusiness IS NULL OR IS_SPACES(i_LineOfBusiness) OR LENGTH(i_LineOfBusiness) = 0, 'N/A', LTRIM(RTRIM(i_LineOfBusiness))) AS o_LineOfBusiness, 
	IFF(i_CompanyNumber IS NULL OR IS_SPACES(i_CompanyNumber) OR LENGTH(i_CompanyNumber) = 0, 'N/A', LTRIM(RTRIM(i_CompanyNumber))) AS o_CompanyNumber, 
	IFF(i_EffectiveDate IS NULL, TO_DATE('18000101', 'YYYYMMDD'), i_EffectiveDate) AS o_EffectiveDate, 
	IFF(i_TransactionDate IS NULL, TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'), i_TransactionDate) AS o_TransactionDate, 
	IFF(i_LocationNumber IS NULL OR IS_SPACES(i_LocationNumber) OR LENGTH(i_LocationNumber) = 0, '0000', LPAD(LTRIM(RTRIM(i_LocationNumber)), 4, '0')) AS o_LocationNumber, 
	'N/A' AS o_reins_sub_loc_unit_num, 
	IFF(v_reins_risk_unit_grp IS NULL, 'N/A', v_reins_risk_unit_grp) AS o_reins_risk_unit_grp, 
	'N/A' AS o_reins_risk_unit_grp_seq_num, 
	IFF(v_reins_risk_unit IS NULL, 'N/A', v_reins_risk_unit) AS o_reins_risk_unit, 
	'N/A' AS o_reins_risk_unit_seq_num, 
	'N/A' AS o_reins_section_code, 
	i_Type AS o_Type, 
	i_AggregateLimit AS o_AggregateLimit, 
	i_OccurrenceLimit AS o_OccurrenceLimit, 
	i_PercentLoss AS o_PercentLoss, 
	i_ExpirationDate AS o_ExpirationDate
	FROM SQ_reinsurance_coverage_SRC
	GROUP BY o_Id, o_PolicyNumber, o_PolicyVersion, o_LineOfBusiness, o_CompanyNumber, o_EffectiveDate, o_LocationNumber, o_reins_sub_loc_unit_num, o_reins_risk_unit_grp, o_reins_risk_unit_grp_seq_num, o_reins_risk_unit, o_reins_risk_unit_seq_num, o_reins_section_code
),
EXP_Values AS (
	SELECT
	o_CustomerNumber AS i_CustomerNumber,
	o_Id AS i_Id,
	o_PolicyNumber AS i_PolicyNumber,
	o_PolicyVersion AS i_PolicyVersion,
	o_LineOfBusiness AS i_LineOfBusiness,
	o_CompanyNumber AS i_CompanyNumber,
	o_EffectiveDate AS i_EffectiveDate,
	o_TransactionDate AS i_TransactionDate,
	o_LocationNumber AS i_LocationNumber,
	o_reins_sub_loc_unit_num AS i_reins_sub_loc_unit_num,
	o_reins_risk_unit_grp AS i_reins_risk_unit_grp,
	o_reins_risk_unit_grp_seq_num AS i_reins_risk_unit_grp_seq_num,
	o_reins_risk_unit AS i_reins_risk_unit,
	o_reins_risk_unit_seq_num AS i_reins_risk_unit_seq_num,
	o_reins_section_code AS i_reins_section_code,
	o_Type AS i_Type,
	o_AggregateLimit AS i_AggregateLimit,
	o_OccurrenceLimit AS i_OccurrenceLimit,
	o_PercentLoss AS i_PercentLoss,
	o_ExpirationDate AS i_ExpirationDate,
	-- *INF*: i_PolicyNumber||i_PolicyVersion
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- --i_CustomerNumber||i_PolicyNumber||i_PolicyVersion
	i_PolicyNumber || i_PolicyVersion AS o_pol_key,
	i_LineOfBusiness AS o_reins_ins_line,
	i_LocationNumber AS o_reins_loc_unit_num,
	i_reins_sub_loc_unit_num AS o_reins_sub_loc_unit_num,
	i_reins_risk_unit_grp AS o_reins_risk_unit_grp,
	i_reins_risk_unit_grp_seq_num AS o_reins_risk_unit_grp_seq_num,
	i_reins_risk_unit AS o_reins_risk_unit,
	i_reins_risk_unit_seq_num AS o_reins_risk_unit_seq_num,
	i_reins_section_code AS o_reins_section_code,
	i_CompanyNumber AS o_reins_co_num,
	i_EffectiveDate AS o_reins_eff_date,
	-- *INF*: IIF(ISNULL(i_ExpirationDate),TO_DATE('21000101235959','YYYYMMDDHH24MISS'),i_ExpirationDate)
	IFF(i_ExpirationDate IS NULL, TO_DATE('21000101235959', 'YYYYMMDDHH24MISS'), i_ExpirationDate) AS o_reins_exp_date,
	-- *INF*: TO_DATE('01011800', 'MMDDYYYY')
	TO_DATE('01011800', 'MMDDYYYY') AS o_reins_enter_date,
	-- *INF*: IIF(ISNULL(i_Type) or IS_SPACES(i_Type) or LENGTH(i_Type)=0,'N/A',LTRIM(RTRIM(i_Type)))
	IFF(i_Type IS NULL OR IS_SPACES(i_Type) OR LENGTH(i_Type) = 0, 'N/A', LTRIM(RTRIM(i_Type))) AS o_reins_type,
	-1 AS o_reins_prcnt_prem_ceded,
	-- *INF*: IIF(ISNULL(i_PercentLoss),0,i_PercentLoss)
	IFF(i_PercentLoss IS NULL, 0, i_PercentLoss) AS o_reins_prcnt_loss_ceded,
	0 AS o_reins_prcnt_facultative_commssn,
	-1 AS o_eins_excess_amt,
	-- *INF*: IIF(ISNULL(i_OccurrenceLimit),0,i_OccurrenceLimit)
	IFF(i_OccurrenceLimit IS NULL, 0, i_OccurrenceLimit) AS o_reins_occurrence_lmt,
	-- *INF*: IIF(ISNULL(i_AggregateLimit),0,i_AggregateLimit)
	IFF(i_AggregateLimit IS NULL, 0, i_AggregateLimit) AS o_reins_agg_lmt
	FROM AGG_RemoveDuplicates
),
LKP_SupReinsuranceMaster AS (
	SELECT
	SupReinsuranceMasterId,
	ReinsuranceMasterReinsuranceCompanyName,
	ReinsuranceMasterReinsuranceType,
	ReinsuranceMasterReinsuranceCompanyNumber
	FROM (
		SELECT 
			SupReinsuranceMasterId,
			ReinsuranceMasterReinsuranceCompanyName,
			ReinsuranceMasterReinsuranceType,
			ReinsuranceMasterReinsuranceCompanyNumber
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupReinsuranceMaster
		WHERE CurrentSnapshotFlag='1' and SourceSystemId='PMS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ReinsuranceMasterReinsuranceCompanyNumber ORDER BY SupReinsuranceMasterId) = 1
),
LKP_policy AS (
	SELECT
	pol_ak_id,
	pol_key
	FROM (
		SELECT 
			pol_ak_id,
			pol_key
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy
		WHERE crrnt_snpsht_flag = 1 and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_ak_id) = 1
),
LKP_sup_insurance_line AS (
	SELECT
	sup_ins_line_id,
	ins_line_descript
	FROM (
		SELECT 
			sup_ins_line_id,
			ins_line_descript
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_insurance_line
		WHERE crrnt_snpsht_flag='1' and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ins_line_descript ORDER BY sup_ins_line_id) = 1
),
LKP_sup_risk_unit AS (
	SELECT
	sup_risk_unit_id,
	i_reins_unit,
	risk_unit_code
	FROM (
		SELECT 
			sup_risk_unit_id,
			i_reins_unit,
			risk_unit_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_risk_unit
		WHERE crrnt_snpsht_flag='1' and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY risk_unit_code ORDER BY sup_risk_unit_id) = 1
),
LKP_sup_risk_unit_group AS (
	SELECT
	sup_risk_unit_grp_id,
	i_reins_risk_unit_grp,
	risk_unit_grp_code
	FROM (
		SELECT 
			sup_risk_unit_grp_id,
			i_reins_risk_unit_grp,
			risk_unit_grp_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_risk_unit_group
		WHERE crrnt_snpsht_flag='1' and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY risk_unit_grp_code ORDER BY sup_risk_unit_grp_id) = 1
),
EXP_LKP_Values AS (
	SELECT
	LKP_policy.pol_ak_id AS i_pol_key_ak_id,
	LKP_sup_insurance_line.sup_ins_line_id AS i_sup_ins_line_id,
	LKP_sup_risk_unit_group.sup_risk_unit_grp_id AS i_sup_risk_unit_grp_id,
	LKP_sup_risk_unit.sup_risk_unit_id AS i_sup_risk_unit_id,
	LKP_SupReinsuranceMaster.SupReinsuranceMasterId AS i_SupReinsuranceMasterId,
	LKP_SupReinsuranceMaster.ReinsuranceMasterReinsuranceCompanyName AS i_reins_co_name,
	LKP_SupReinsuranceMaster.ReinsuranceMasterReinsuranceType,
	EXP_Values.o_reins_ins_line AS i_reins_ins_line,
	EXP_Values.o_reins_loc_unit_num AS i_reins_loc_unit_num,
	EXP_Values.o_reins_sub_loc_unit_num AS i_reins_sub_loc_unit_num,
	EXP_Values.o_reins_risk_unit_grp AS i_reins_risk_unit_grp,
	EXP_Values.o_reins_risk_unit_grp_seq_num AS i_reins_risk_unit_grp_seq_num,
	EXP_Values.o_reins_risk_unit AS i_reins_risk_unit,
	EXP_Values.o_reins_risk_unit_seq_num AS i_reins_risk_unit_seq_num,
	EXP_Values.o_reins_section_code AS i_reins_section_code,
	EXP_Values.o_reins_co_num AS i_reins_co_num,
	EXP_Values.o_reins_eff_date AS i_reins_eff_date,
	EXP_Values.o_reins_exp_date AS i_reins_exp_date,
	EXP_Values.o_reins_enter_date AS i_reins_enter_date,
	EXP_Values.o_reins_type AS i_reins_type,
	EXP_Values.o_reins_prcnt_prem_ceded AS i_reins_prcnt_prem_ceded,
	EXP_Values.o_reins_prcnt_loss_ceded AS i_reins_prcnt_loss_ceded,
	EXP_Values.o_reins_prcnt_facultative_commssn AS i_reins_prcnt_facultative_commssn,
	EXP_Values.o_eins_excess_amt AS i_eins_excess_amt,
	EXP_Values.o_reins_occurrence_lmt AS i_reins_occurrence_lmt,
	EXP_Values.o_reins_agg_lmt AS i_reins_agg_lmt,
	i_pol_key_ak_id AS o_pol_key_ak_id,
	i_reins_ins_line AS o_reins_ins_line,
	i_reins_loc_unit_num AS o_reins_loc_unit_num,
	i_reins_sub_loc_unit_num AS o_reins_sub_loc_unit_num,
	i_reins_risk_unit_grp AS o_reins_risk_unit_grp,
	i_reins_risk_unit_grp_seq_num AS o_reins_risk_unit_grp_seq_num,
	i_reins_risk_unit AS o_reins_risk_unit,
	i_reins_risk_unit_seq_num AS o_reins_risk_unit_seq_num,
	i_reins_section_code AS o_reins_section_code,
	i_reins_co_num AS o_reins_co_num,
	-- *INF*: IIF(ISNULL(i_reins_co_name),'N/A',i_reins_co_name)
	-- 
	-- 
	-- 
	-- 
	-- --IIF(ISNULL(i_reins_co_name),'N/A',i_reins_co_name)
	IFF(i_reins_co_name IS NULL, 'N/A', i_reins_co_name) AS o_reins_co_name,
	i_reins_eff_date AS o_reins_eff_date,
	i_reins_exp_date AS o_reins_exp_date,
	i_reins_enter_date AS o_reins_enter_date,
	i_reins_type AS o_reins_type,
	i_reins_prcnt_prem_ceded AS o_reins_prcnt_prem_ceded,
	i_reins_prcnt_loss_ceded AS o_reins_prcnt_loss_ceded,
	i_reins_prcnt_facultative_commssn AS o_reins_prcnt_facultative_commssn,
	i_eins_excess_amt AS o_eins_excess_amt,
	i_reins_occurrence_lmt AS o_reins_occurrence_lmt,
	i_reins_agg_lmt AS o_reins_agg_lmt,
	-- *INF*: IIF(ISNULL(i_sup_ins_line_id),-1,i_sup_ins_line_id)
	IFF(i_sup_ins_line_id IS NULL, - 1, i_sup_ins_line_id) AS o_sup_ins_line_id,
	-- *INF*: IIF(ISNULL(i_sup_risk_unit_grp_id),-1,i_sup_risk_unit_grp_id)
	IFF(i_sup_risk_unit_grp_id IS NULL, - 1, i_sup_risk_unit_grp_id) AS o_sup_risk_unit_grp_id,
	-- *INF*: IIF(ISNULL(i_sup_risk_unit_id),-1,i_sup_risk_unit_id)
	IFF(i_sup_risk_unit_id IS NULL, - 1, i_sup_risk_unit_id) AS o_sup_risk_unit_id,
	-- *INF*: IIF(ISNULL(i_SupReinsuranceMasterId),-1,i_SupReinsuranceMasterId)
	IFF(i_SupReinsuranceMasterId IS NULL, - 1, i_SupReinsuranceMasterId) AS o_SupReinsuranceMasterID,
	-- *INF*: DECODE(ReinsuranceMasterReinsuranceType,
	-- '1', 'In House',
	-- '2', 'Facultative',
	-- '3', 'Treaty','N/A')
	DECODE(ReinsuranceMasterReinsuranceType,
	'1', 'In House',
	'2', 'Facultative',
	'3', 'Treaty',
	'N/A') AS o_ReinsuranceMethod
	FROM EXP_Values
	LEFT JOIN LKP_SupReinsuranceMaster
	ON LKP_SupReinsuranceMaster.ReinsuranceMasterReinsuranceCompanyNumber = EXP_Values.o_reins_co_num
	LEFT JOIN LKP_policy
	ON LKP_policy.pol_key = EXP_Values.o_pol_key
	LEFT JOIN LKP_sup_insurance_line
	ON LKP_sup_insurance_line.ins_line_descript = EXP_Values.o_reins_ins_line
	LEFT JOIN LKP_sup_risk_unit
	ON LKP_sup_risk_unit.risk_unit_code = EXP_Values.o_reins_risk_unit
	LEFT JOIN LKP_sup_risk_unit_group
	ON LKP_sup_risk_unit_group.risk_unit_grp_code = EXP_Values.o_reins_risk_unit_grp
),
LKP_reinsurance_coverage AS (
	SELECT
	reins_cov_id,
	reins_co_name,
	reins_exp_date,
	reins_type,
	reins_prcnt_prem_ceded,
	reins_prcnt_loss_ceded,
	reins_prcnt_facultative_commssn,
	reins_excess_amt,
	reins_occurrence_lmt,
	reins_agg_lmt,
	reins_cov_ak_id,
	pol_ak_id,
	reins_ins_line,
	reins_loc_unit_num,
	reins_sub_loc_unit_num,
	reins_risk_unit_grp,
	reins_risk_unit_grp_seq_num,
	reins_risk_unit,
	reins_risk_unit_seq_num,
	reins_section_code,
	reins_co_num,
	reins_eff_date,
	reins_enter_date
	FROM (
		SELECT 
			reins_cov_id,
			reins_co_name,
			reins_exp_date,
			reins_type,
			reins_prcnt_prem_ceded,
			reins_prcnt_loss_ceded,
			reins_prcnt_facultative_commssn,
			reins_excess_amt,
			reins_occurrence_lmt,
			reins_agg_lmt,
			reins_cov_ak_id,
			pol_ak_id,
			reins_ins_line,
			reins_loc_unit_num,
			reins_sub_loc_unit_num,
			reins_risk_unit_grp,
			reins_risk_unit_grp_seq_num,
			reins_risk_unit,
			reins_risk_unit_seq_num,
			reins_section_code,
			reins_co_num,
			reins_eff_date,
			reins_enter_date
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.reinsurance_coverage
		WHERE crrnt_snpsht_flag='1' and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id,reins_ins_line,reins_loc_unit_num,reins_sub_loc_unit_num,reins_risk_unit_grp,reins_risk_unit_grp_seq_num,reins_risk_unit,reins_risk_unit_seq_num,reins_section_code,reins_co_num,reins_eff_date,reins_enter_date ORDER BY reins_cov_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_reinsurance_coverage.reins_cov_id AS i_reins_cov_id,
	LKP_reinsurance_coverage.reins_co_name AS i_reins_co_name,
	LKP_reinsurance_coverage.reins_exp_date AS i_reins_exp_date,
	LKP_reinsurance_coverage.reins_type AS i_reins_type,
	LKP_reinsurance_coverage.reins_prcnt_prem_ceded AS i_reins_prcnt_prem_ceded,
	LKP_reinsurance_coverage.reins_prcnt_loss_ceded AS i_reins_prcnt_loss_ceded,
	LKP_reinsurance_coverage.reins_prcnt_facultative_commssn AS i_reins_prcnt_facultative_commssn,
	LKP_reinsurance_coverage.reins_excess_amt AS i_reins_excess_amt,
	LKP_reinsurance_coverage.reins_occurrence_lmt AS i_reins_occurrence_lmt,
	LKP_reinsurance_coverage.reins_agg_lmt AS i_reins_agg_lmt,
	LKP_reinsurance_coverage.reins_cov_ak_id,
	EXP_LKP_Values.o_pol_key_ak_id AS pol_ak_id,
	EXP_LKP_Values.o_reins_ins_line AS reins_ins_line,
	EXP_LKP_Values.o_reins_loc_unit_num AS reins_loc_unit_num,
	EXP_LKP_Values.o_reins_sub_loc_unit_num AS reins_sub_loc_unit_num,
	EXP_LKP_Values.o_reins_risk_unit_grp AS reins_risk_unit_grp,
	EXP_LKP_Values.o_reins_risk_unit_grp_seq_num AS reins_risk_unit_grp_seq_num,
	EXP_LKP_Values.o_reins_risk_unit AS reins_risk_unit,
	EXP_LKP_Values.o_reins_risk_unit_seq_num AS reins_risk_unit_seq_num,
	EXP_LKP_Values.o_reins_section_code AS reins_section_code,
	EXP_LKP_Values.o_reins_co_num AS reins_co_num,
	EXP_LKP_Values.o_reins_co_name AS reins_co_name,
	EXP_LKP_Values.o_reins_eff_date AS reins_eff_date,
	EXP_LKP_Values.o_reins_exp_date AS reins_exp_date,
	EXP_LKP_Values.o_reins_enter_date AS reins_enter_date,
	EXP_LKP_Values.o_reins_type AS reins_type,
	EXP_LKP_Values.o_reins_prcnt_prem_ceded AS reins_prcnt_prem_ceded,
	EXP_LKP_Values.o_reins_prcnt_loss_ceded AS reins_prcnt_loss_ceded,
	EXP_LKP_Values.o_reins_prcnt_facultative_commssn AS reins_prcnt_facultative_commssn,
	EXP_LKP_Values.o_eins_excess_amt AS reins_excess_amt,
	EXP_LKP_Values.o_reins_occurrence_lmt AS reins_occurrence_lmt,
	EXP_LKP_Values.o_reins_agg_lmt AS reins_agg_lmt,
	EXP_LKP_Values.o_sup_ins_line_id AS sup_ins_line_id,
	EXP_LKP_Values.o_sup_risk_unit_grp_id AS sup_risk_unit_grp_id,
	EXP_LKP_Values.o_sup_risk_unit_id AS sup_risk_unit_id,
	-- *INF*: IIF(ISNULL(i_reins_cov_id),'NEW',
	-- 	IIF (
	-- 	LTRIM(RTRIM(i_reins_co_name)) <> LTRIM(RTRIM(reins_co_name)) or
	-- 	(i_reins_exp_date <> reins_exp_date) or
	-- 	(LTRIM(RTRIM(i_reins_type))  <> LTRIM(RTRIM(reins_type))) or
	-- 	i_reins_prcnt_prem_ceded <> reins_prcnt_prem_ceded or
	-- 	i_reins_prcnt_loss_ceded <> reins_prcnt_loss_ceded or
	-- 	i_reins_prcnt_facultative_commssn <> reins_prcnt_facultative_commssn or
	-- 	i_reins_excess_amt <> reins_excess_amt or
	-- 	i_reins_occurrence_lmt <> reins_occurrence_lmt or
	-- 	i_reins_agg_lmt <> reins_agg_lmt
	--   	,'UPDATE'
	-- 	,'NOCHANGE'))
	IFF(i_reins_cov_id IS NULL, 'NEW', IFF(LTRIM(RTRIM(i_reins_co_name)) <> LTRIM(RTRIM(reins_co_name)) OR ( i_reins_exp_date <> reins_exp_date ) OR ( LTRIM(RTRIM(i_reins_type)) <> LTRIM(RTRIM(reins_type)) ) OR i_reins_prcnt_prem_ceded <> reins_prcnt_prem_ceded OR i_reins_prcnt_loss_ceded <> reins_prcnt_loss_ceded OR i_reins_prcnt_facultative_commssn <> reins_prcnt_facultative_commssn OR i_reins_excess_amt <> reins_excess_amt OR i_reins_occurrence_lmt <> reins_occurrence_lmt OR i_reins_agg_lmt <> reins_agg_lmt, 'UPDATE', 'NOCHANGE')) AS Changed_Flag,
	EXP_LKP_Values.o_ReinsuranceMethod
	FROM EXP_LKP_Values
	LEFT JOIN LKP_reinsurance_coverage
	ON LKP_reinsurance_coverage.pol_ak_id = EXP_LKP_Values.o_pol_key_ak_id AND LKP_reinsurance_coverage.reins_ins_line = EXP_LKP_Values.o_reins_ins_line AND LKP_reinsurance_coverage.reins_loc_unit_num = EXP_LKP_Values.o_reins_loc_unit_num AND LKP_reinsurance_coverage.reins_sub_loc_unit_num = EXP_LKP_Values.o_reins_sub_loc_unit_num AND LKP_reinsurance_coverage.reins_risk_unit_grp = EXP_LKP_Values.o_reins_risk_unit_grp AND LKP_reinsurance_coverage.reins_risk_unit_grp_seq_num = EXP_LKP_Values.o_reins_risk_unit_grp_seq_num AND LKP_reinsurance_coverage.reins_risk_unit = EXP_LKP_Values.o_reins_risk_unit AND LKP_reinsurance_coverage.reins_risk_unit_seq_num = EXP_LKP_Values.o_reins_risk_unit_seq_num AND LKP_reinsurance_coverage.reins_section_code = EXP_LKP_Values.o_reins_section_code AND LKP_reinsurance_coverage.reins_co_num = EXP_LKP_Values.o_reins_co_num AND LKP_reinsurance_coverage.reins_eff_date = EXP_LKP_Values.o_reins_eff_date AND LKP_reinsurance_coverage.reins_enter_date = EXP_LKP_Values.o_reins_enter_date
),
FIL_New_or_Changed AS (
	SELECT
	reins_cov_ak_id, 
	pol_ak_id, 
	reins_ins_line, 
	reins_loc_unit_num, 
	reins_sub_loc_unit_num, 
	reins_risk_unit_grp, 
	reins_risk_unit_grp_seq_num, 
	reins_risk_unit, 
	reins_risk_unit_seq_num, 
	reins_section_code, 
	reins_co_num, 
	reins_co_name, 
	reins_eff_date, 
	reins_exp_date, 
	reins_enter_date, 
	reins_type, 
	reins_prcnt_prem_ceded, 
	reins_prcnt_loss_ceded, 
	reins_prcnt_facultative_commssn, 
	reins_excess_amt, 
	reins_occurrence_lmt, 
	reins_agg_lmt, 
	sup_ins_line_id, 
	sup_risk_unit_grp_id, 
	sup_risk_unit_id, 
	Changed_Flag, 
	o_ReinsuranceMethod
	FROM EXP_Detect_Changes
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
SEQ_Reins_Cov_AK_ID AS (
	CREATE SEQUENCE SEQ_Reins_Cov_AK_ID
	START = 0
	INCREMENT = 1;
),
EXP_Determine_AK AS (
	SELECT
	SEQ_Reins_Cov_AK_ID.NEXTVAL AS i_NEXTVAL,
	reins_cov_ak_id AS i_reins_cov_ak_id,
	pol_ak_id,
	reins_ins_line,
	reins_loc_unit_num,
	reins_sub_loc_unit_num,
	reins_risk_unit_grp,
	reins_risk_unit_grp_seq_num,
	reins_risk_unit,
	reins_risk_unit_seq_num,
	reins_section_code,
	reins_co_num,
	reins_co_name,
	reins_eff_date,
	reins_exp_date,
	reins_enter_date,
	reins_type,
	reins_prcnt_prem_ceded,
	reins_prcnt_loss_ceded,
	reins_prcnt_facultative_commssn,
	reins_excess_amt,
	reins_occurrence_lmt,
	reins_agg_lmt,
	sup_ins_line_id,
	sup_risk_unit_grp_id,
	sup_risk_unit_id,
	Changed_Flag,
	1 AS Crrnt_Snpsht_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_Id,
	-- *INF*: IIF(Changed_Flag='NEW', TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'), SYSDATE)
	IFF(Changed_Flag = 'NEW', TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS Eff_From_Date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS Eff_To_Date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID,
	SYSDATE AS Created_Date,
	SYSDATE AS Modified_Date,
	'0' AS logical_flag,
	-- *INF*: IIF(Changed_Flag='NEW',
	-- i_NEXTVAL,
	-- i_reins_cov_ak_id)
	IFF(Changed_Flag = 'NEW', i_NEXTVAL, i_reins_cov_ak_id) AS o_reins_cov_ak_id,
	o_ReinsuranceMethod
	FROM FIL_New_or_Changed
),
TGT_reinsurance_coverage_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.reinsurance_coverage
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, logical_flag, reins_cov_ak_id, pol_ak_id, reins_ins_line, reins_loc_unit_num, reins_sub_loc_unit_num, reins_risk_unit_grp, reins_risk_unit_grp_seq_num, reins_risk_unit, reins_risk_unit_seq_num, reins_section_code, reins_co_num, reins_co_name, reins_eff_date, reins_exp_date, reins_enter_date, reins_type, reins_prcnt_prem_ceded, reins_prcnt_loss_ceded, reins_prcnt_facultative_commssn, reins_excess_amt, reins_occurrence_lmt, reins_agg_lmt, SupInsuranceLineId, SupRiskUnitId, SupRiskUnitGroupId, ReinsuranceMethod)
	SELECT 
	Crrnt_Snpsht_Flag AS CRRNT_SNPSHT_FLAG, 
	Audit_Id AS AUDIT_ID, 
	Eff_From_Date AS EFF_FROM_DATE, 
	Eff_To_Date AS EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	Created_Date AS CREATED_DATE, 
	Modified_Date AS MODIFIED_DATE, 
	LOGICAL_FLAG, 
	o_reins_cov_ak_id AS REINS_COV_AK_ID, 
	POL_AK_ID, 
	REINS_INS_LINE, 
	REINS_LOC_UNIT_NUM, 
	REINS_SUB_LOC_UNIT_NUM, 
	REINS_RISK_UNIT_GRP, 
	REINS_RISK_UNIT_GRP_SEQ_NUM, 
	REINS_RISK_UNIT, 
	REINS_RISK_UNIT_SEQ_NUM, 
	REINS_SECTION_CODE, 
	REINS_CO_NUM, 
	REINS_CO_NAME, 
	REINS_EFF_DATE, 
	REINS_EXP_DATE, 
	REINS_ENTER_DATE, 
	REINS_TYPE, 
	REINS_PRCNT_PREM_CEDED, 
	REINS_PRCNT_LOSS_CEDED, 
	REINS_PRCNT_FACULTATIVE_COMMSSN, 
	REINS_EXCESS_AMT, 
	REINS_OCCURRENCE_LMT, 
	REINS_AGG_LMT, 
	sup_ins_line_id AS SUPINSURANCELINEID, 
	sup_risk_unit_id AS SUPRISKUNITID, 
	sup_risk_unit_grp_id AS SUPRISKUNITGROUPID, 
	o_ReinsuranceMethod AS REINSURANCEMETHOD
	FROM EXP_Determine_AK
),
SQ_reinsurance_coverage AS (
	SELECT 
		 A.reins_cov_id,
	       A.eff_from_date,
	       A.eff_to_date,
		A.reins_cov_ak_id
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.reinsurance_coverage a
	where a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	and EXISTS (SELECT 1			
		FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.reinsurance_coverage b
		WHERE b.crrnt_snpsht_flag = 1
	      AND a.reins_cov_ak_id = b.reins_cov_ak_id
	      AND b.source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		GROUP BY b.reins_cov_ak_id
		HAVING COUNT(*) > 1)
	order by a.reins_cov_ak_id, a.eff_from_date desc
	
	--EXISTS Subquery exists to pick AK Groups that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag and all columns of the AK
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
	
	--ORDER BY of main query orders all rows first by the AK and then by the eff_from_date in a DESC format
	--the descending order is important because this allows us to avoid another lookup and properly apply the eff_to_date by utilizing a local variable to keep track
),
EXP_Expire_Rows AS (
	SELECT
	reins_cov_id AS i_reins_cov_id,
	eff_from_date AS i_eff_from_date,
	eff_to_date AS i_orig_eff_to_date,
	reins_cov_ak_id AS i_reins_cov_ak_id,
	-- *INF*: DECODE (TRUE, 
	-- i_reins_cov_ak_id=v_PREV_ROW_reins_cov_ak_id, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1)
	-- ,i_orig_eff_to_date)
	DECODE(TRUE,
	i_reins_cov_ak_id = v_PREV_ROW_reins_cov_ak_id, ADD_TO_DATE(v_PREV_ROW_eff_from_date, 'SS', - 1),
	i_orig_eff_to_date) AS v_eff_to_date,
	i_reins_cov_ak_id AS v_PREV_ROW_reins_cov_ak_id,
	i_eff_from_date AS v_PREV_ROW_eff_from_date,
	i_orig_eff_to_date AS o_orig_eff_to_date,
	i_reins_cov_id AS o_reins_cov_id,
	0 AS o_crrnt_snapshot_flag,
	v_eff_to_date AS o_eff_to_date,
	sysdate AS o_modified_date
	FROM SQ_reinsurance_coverage
),
FIL_Expire_Rows AS (
	SELECT
	o_orig_eff_to_date AS i_orig_eff_to_date, 
	o_reins_cov_id AS reins_cov_id, 
	o_crrnt_snapshot_flag AS crrnt_snapshot_flag, 
	o_eff_to_date AS eff_to_date, 
	o_modified_date AS modified_date
	FROM EXP_Expire_Rows
	WHERE i_orig_eff_to_date != eff_to_date
),
UPD_reinsurance_coverage AS (
	SELECT
	reins_cov_id, 
	crrnt_snapshot_flag, 
	eff_to_date, 
	modified_date
	FROM FIL_Expire_Rows
),
TGT_reinsurance_coverage_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.reinsurance_coverage AS T
	USING UPD_reinsurance_coverage AS S
	ON T.reins_cov_id = S.reins_cov_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snapshot_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),
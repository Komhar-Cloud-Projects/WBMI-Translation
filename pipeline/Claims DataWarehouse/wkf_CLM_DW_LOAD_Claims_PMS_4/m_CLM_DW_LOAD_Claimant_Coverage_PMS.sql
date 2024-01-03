WITH
SQ_PIF_42X6_stage AS (
	SELECT A.pif_symbol, A.pif_policy_number, A.pif_module, A.ipfcx6_insurance_line, A.ipfcx6_location_number, A.ipfcx6_sub_location_number, A.ipfcx6_risk_unit_group, A.ipfcx6_class_code_group, A.ipfcx6_class_code_member, A.ipfcx6_loss_unit, A.ipfcx6_risk_sequence, A.ipfcx6_risk_type_ind, A.ipfcx6_type_exposure, A.ipfcx6_major_peril, A.ipfcx6_sequence_type_exposure, A.ipfcx6_year_item_effective, A.ipfcx6_month_item_effective, A.ipfcx6_day_item_effective, A.ipfcx6_year_of_loss, A.ipfcx6_month_of_loss, A.ipfcx6_day_of_loss, A.ipfcx6_loss_occ_fdigit, A.ipfcx6_usr_loss_occurence, A.ipfcx6_loss_claimant, A.ipfcx6_member, A.ipfcx6_loss_disability, A.ipfcx6_reserve_category, A.ipfcx6_loss_cause, A.ipfcx6_offset_onset_ind, A.logical_flag 
	FROM
	  @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_42x6_stage A
	WHERE A.logical_flag IN ('0','1')
),
EXP_Source AS (
	SELECT
	PIF_SYMBOL,
	PIF_POLICY_NUMBER,
	PIF_MODULE,
	IPFCX6_INSURANCE_LINE,
	-- *INF*: IIF(IS_SPACES(IPFCX6_INSURANCE_LINE) OR ISNULL(IPFCX6_INSURANCE_LINE)
	-- ,'N/A'
	-- ,LTRIM(RTRIM(IPFCX6_INSURANCE_LINE))
	-- )
	IFF(IS_SPACES(IPFCX6_INSURANCE_LINE) OR IPFCX6_INSURANCE_LINE IS NULL, 'N/A', LTRIM(RTRIM(IPFCX6_INSURANCE_LINE))) AS out_INSURANCE_LINE,
	IPFCX6_LOCATION_NUMBER,
	-- *INF*: IIF(ISNULL(IPFCX6_LOCATION_NUMBER),'0000',LPAD(TO_CHAR(IPFCX6_LOCATION_NUMBER),4,'0'))
	-- 
	-- 
	-- ---IIF(ISNULL(IPFCX6_LOCATION_NUMBER) ,0 ,IPFCX6_LOCATION_NUMBER)
	-- 
	-- 
	-- 
	IFF(IPFCX6_LOCATION_NUMBER IS NULL, '0000', LPAD(TO_CHAR(IPFCX6_LOCATION_NUMBER), 4, '0')) AS out_LOCATION_NUMBER,
	IPFCX6_SUB_LOCATION_NUMBER,
	-- *INF*: IIF(ISNULL(IPFCX6_SUB_LOCATION_NUMBER),'000',TO_CHAR(IPFCX6_SUB_LOCATION_NUMBER))
	-- 
	-- ---,LENGTH(TO_CHAR(IPFCX6_SUB_LOCATION_NUMBER))=2,'0' ||  TO_CHAR(IPFCX6_LOCATION_NUMBER)
	-- 
	-- 
	-- 
	-- ---DECODE(TRUE,ISNULL(IPFCX6_SUB_LOCATION_NUMBER),'000',
	-- ---            LENGTH(TO_CHAR(IPFCX6_SUB_LOCATION_NUMBER))=1,'00' || TO_CHAR(IPFCX6_LOCATION_NUMBER)                 )
	IFF(IPFCX6_SUB_LOCATION_NUMBER IS NULL, '000', TO_CHAR(IPFCX6_SUB_LOCATION_NUMBER)) AS V_SUB_LOCATION_NUMBER,
	-- *INF*: IIF(V_SUB_LOCATION_NUMBER ='000',V_SUB_LOCATION_NUMBER,
	--          LPAD(V_SUB_LOCATION_NUMBER,3,'0'))
	-- 
	-- --DECODE(TRUE,
	--    --    LENGTH(V_SUB_LOCATION_NUMBER)=1,'00' || V_SUB_LOCATION_NUMBER,
	--       ---LENGTH(V_SUB_LOCATION_NUMBER)=2,'0' || V_SUB_LOCATION_NUMBER,
	-- ---      LENGTH(V_SUB_LOCATION_NUMBER)=3, V_SUB_LOCATION_NUMBER)
	IFF(V_SUB_LOCATION_NUMBER = '000', V_SUB_LOCATION_NUMBER, LPAD(V_SUB_LOCATION_NUMBER, 3, '0')) AS out_SUB_LOCATION_NUMBER,
	IPFCX6_RISK_UNIT_GROUP,
	IPFCX6_CLASS_CODE_GROUP,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(IPFCX6_CLASS_CODE_GROUP)
	-- ,'00'
	-- ,IPFCX6_CLASS_CODE_GROUP<10,
	-- '0' || TO_CHAR(IPFCX6_CLASS_CODE_GROUP)
	-- ,TO_CHAR(IPFCX6_CLASS_CODE_GROUP)
	-- )
	-- 
	-- --IIF(ISNULL(IPFCX6_CLASS_CODE_GROUP)
	-- --,'00'
	-- --,TO_CHAR(IPFCX6_CLASS_CODE_GROUP))
	DECODE(TRUE,
	IPFCX6_CLASS_CODE_GROUP IS NULL, '00',
	IPFCX6_CLASS_CODE_GROUP < 10, '0' || TO_CHAR(IPFCX6_CLASS_CODE_GROUP),
	TO_CHAR(IPFCX6_CLASS_CODE_GROUP)) AS out_CLASS_CODE_GROUP,
	IPFCX6_CLASS_CODE_MEMBER,
	-- *INF*: IIF(ISNULL(IPFCX6_CLASS_CODE_MEMBER)
	-- ,'0'
	-- ,TO_CHAR(IPFCX6_CLASS_CODE_MEMBER))
	IFF(IPFCX6_CLASS_CODE_MEMBER IS NULL, '0', TO_CHAR(IPFCX6_CLASS_CODE_MEMBER)) AS out_CLASS_CODE_MEMBER,
	-- *INF*: IIF(IS_SPACES(IPFCX6_RISK_UNIT_GROUP) OR ISNULL(IPFCX6_RISK_UNIT_GROUP)
	-- ,'N/A'
	-- ,LTRIM(RTRIM(IPFCX6_RISK_UNIT_GROUP))
	-- )
	IFF(IS_SPACES(IPFCX6_RISK_UNIT_GROUP) OR IPFCX6_RISK_UNIT_GROUP IS NULL, 'N/A', LTRIM(RTRIM(IPFCX6_RISK_UNIT_GROUP))) AS out_RISK_UNIT_GROUP,
	-- *INF*: IIF(IS_SPACES(TO_CHAR(IPFCX6_CLASS_CODE_GROUP )|| TO_CHAR(IPFCX6_CLASS_CODE_MEMBER)) OR ISNULL(TO_CHAR(IPFCX6_CLASS_CODE_GROUP )|| TO_CHAR(IPFCX6_CLASS_CODE_MEMBER))
	-- ,'N/A'
	-- ,TO_CHAR(IPFCX6_CLASS_CODE_GROUP )|| TO_CHAR(IPFCX6_CLASS_CODE_MEMBER))
	-- 
	-- 
	IFF(IS_SPACES(TO_CHAR(IPFCX6_CLASS_CODE_GROUP) || TO_CHAR(IPFCX6_CLASS_CODE_MEMBER)) OR TO_CHAR(IPFCX6_CLASS_CODE_GROUP) || TO_CHAR(IPFCX6_CLASS_CODE_MEMBER) IS NULL, 'N/A', TO_CHAR(IPFCX6_CLASS_CODE_GROUP) || TO_CHAR(IPFCX6_CLASS_CODE_MEMBER)) AS V_RISK_UNIT_GRP_SEQ,
	-- *INF*: LPAD(RTRIM(V_RISK_UNIT_GRP_SEQ),3,'0')
	LPAD(RTRIM(V_RISK_UNIT_GRP_SEQ), 3, '0') AS RISK_UNIT_GRP_SEQ,
	IPFCX6_LOSS_UNIT,
	-- *INF*: IIF(IS_SPACES(IPFCX6_LOSS_UNIT) OR ISNULL(IPFCX6_LOSS_UNIT)
	-- ,'N/A'
	-- ,LTRIM(RTRIM(IPFCX6_LOSS_UNIT))
	-- )
	IFF(IS_SPACES(IPFCX6_LOSS_UNIT) OR IPFCX6_LOSS_UNIT IS NULL, 'N/A', LTRIM(RTRIM(IPFCX6_LOSS_UNIT))) AS out_LOSS_UNIT,
	IPFCX6_RISK_SEQUENCE,
	-- *INF*: IIF(ISNULL(IPFCX6_RISK_SEQUENCE)
	-- ,'0'
	-- ,TO_CHAR(IPFCX6_RISK_SEQUENCE))
	IFF(IPFCX6_RISK_SEQUENCE IS NULL, '0', TO_CHAR(IPFCX6_RISK_SEQUENCE)) AS out_RISK_SEQUENCE,
	IPFCX6_RISK_TYPE_IND,
	-- *INF*: IIF(IS_SPACES(IPFCX6_RISK_TYPE_IND) OR ISNULL(IPFCX6_RISK_TYPE_IND) OR LENGTH(IPFCX6_RISK_TYPE_IND)=0
	-- ,'N/A'
	-- ,LTRIM(RTRIM(IPFCX6_RISK_TYPE_IND))
	-- )
	IFF(IS_SPACES(IPFCX6_RISK_TYPE_IND) OR IPFCX6_RISK_TYPE_IND IS NULL OR LENGTH(IPFCX6_RISK_TYPE_IND) = 0, 'N/A', LTRIM(RTRIM(IPFCX6_RISK_TYPE_IND))) AS out_RISK_TYPE_IND,
	IPFCX6_TYPE_EXPOSURE,
	-- *INF*: IIF(IS_SPACES(IPFCX6_TYPE_EXPOSURE) OR ISNULL(IPFCX6_TYPE_EXPOSURE)
	-- ,'N/A'
	-- ,LTRIM(RTRIM(IPFCX6_TYPE_EXPOSURE))
	-- )
	IFF(IS_SPACES(IPFCX6_TYPE_EXPOSURE) OR IPFCX6_TYPE_EXPOSURE IS NULL, 'N/A', LTRIM(RTRIM(IPFCX6_TYPE_EXPOSURE))) AS out_TYPE_EXPOSURE,
	IPFCX6_MAJOR_PERIL,
	-- *INF*: IIF(IS_SPACES(IPFCX6_MAJOR_PERIL) OR ISNULL(IPFCX6_MAJOR_PERIL)
	-- ,'N/A'
	-- ,LTRIM(RTRIM(IPFCX6_MAJOR_PERIL))
	-- )
	IFF(IS_SPACES(IPFCX6_MAJOR_PERIL) OR IPFCX6_MAJOR_PERIL IS NULL, 'N/A', LTRIM(RTRIM(IPFCX6_MAJOR_PERIL))) AS out_MAJOR_PERIL,
	IPFCX6_SEQUENCE_TYPE_EXPOSURE,
	-- *INF*: IIF(IS_SPACES(IPFCX6_SEQUENCE_TYPE_EXPOSURE) OR ISNULL(IPFCX6_SEQUENCE_TYPE_EXPOSURE)
	-- ,'N/A'
	-- ,LTRIM(RTRIM(IPFCX6_SEQUENCE_TYPE_EXPOSURE))
	-- )
	IFF(IS_SPACES(IPFCX6_SEQUENCE_TYPE_EXPOSURE) OR IPFCX6_SEQUENCE_TYPE_EXPOSURE IS NULL, 'N/A', LTRIM(RTRIM(IPFCX6_SEQUENCE_TYPE_EXPOSURE))) AS out_SEQUENCE_TYPE_EXPOSURE,
	IPFCX6_YEAR_ITEM_EFFECTIVE,
	IPFCX6_YEAR_ITEM_EFFECTIVE AS out_IPFCX6_YEAR_ITEM_EFFECTIVE,
	IPFCX6_MONTH_ITEM_EFFECTIVE,
	IPFCX6_MONTH_ITEM_EFFECTIVE AS out_IPFCX6_MONTH_ITEM_EFFECTIVE,
	IPFCX6_DAY_ITEM_EFFECTIVE,
	IPFCX6_DAY_ITEM_EFFECTIVE AS out_IPFCX6_DAY_ITEM_EFFECTIVE,
	IPFCX6_YEAR_OF_LOSS,
	-- *INF*: TO_CHAR(IPFCX6_YEAR_OF_LOSS)
	TO_CHAR(IPFCX6_YEAR_OF_LOSS) AS v_IPFCX6_YEAR_OF_LOSS,
	IPFCX6_MONTH_OF_LOSS,
	-- *INF*: TO_CHAR(IPFCX6_MONTH_OF_LOSS)
	TO_CHAR(IPFCX6_MONTH_OF_LOSS) AS v_IPFCX6_MONTH_OF_LOSS,
	IPFCX6_DAY_OF_LOSS,
	-- *INF*: TO_CHAR(IPFCX6_DAY_OF_LOSS)
	TO_CHAR(IPFCX6_DAY_OF_LOSS) AS v_IPFCX6_DAY_OF_LOSS,
	IPFCX6_LOSS_OCC_FDIGIT,
	IPFCX6_USR_LOSS_OCCURENCE,
	-- *INF*: IIF(LENGTH(IPFCX6_USR_LOSS_OCCURENCE) = 1, '0'||LTRIM(RTRIM(IPFCX6_USR_LOSS_OCCURENCE)), LTRIM(RTRIM(TO_CHAR(IPFCX6_USR_LOSS_OCCURENCE))))
	IFF(LENGTH(IPFCX6_USR_LOSS_OCCURENCE) = 1, '0' || LTRIM(RTRIM(IPFCX6_USR_LOSS_OCCURENCE)), LTRIM(RTRIM(TO_CHAR(IPFCX6_USR_LOSS_OCCURENCE)))) AS LOSS_OCCURENCE_VAR_LEN,
	-- *INF*: LTRIM(RTRIM(IPFCX6_LOSS_OCC_FDIGIT)) || LTRIM(RTRIM(LOSS_OCCURENCE_VAR_LEN))
	LTRIM(RTRIM(IPFCX6_LOSS_OCC_FDIGIT)) || LTRIM(RTRIM(LOSS_OCCURENCE_VAR_LEN)) AS LOSS_OCCURENCE,
	IPFCX6_LOSS_CLAIMANT,
	IPFCX6_MEMBER,
	-- *INF*: IIF(IS_SPACES(IPFCX6_MEMBER) OR ISNULL(IPFCX6_MEMBER)
	-- ,'N/A'
	-- ,LTRIM(RTRIM(IPFCX6_MEMBER))
	-- )
	IFF(IS_SPACES(IPFCX6_MEMBER) OR IPFCX6_MEMBER IS NULL, 'N/A', LTRIM(RTRIM(IPFCX6_MEMBER))) AS out_MEMBER,
	IPFCX6_LOSS_DISABILITY,
	-- *INF*: IIF(IS_SPACES(IPFCX6_LOSS_DISABILITY) OR ISNULL(IPFCX6_LOSS_DISABILITY)
	-- ,'N/A'
	-- ,LTRIM(RTRIM(IPFCX6_LOSS_DISABILITY))
	-- )
	IFF(IS_SPACES(IPFCX6_LOSS_DISABILITY) OR IPFCX6_LOSS_DISABILITY IS NULL, 'N/A', LTRIM(RTRIM(IPFCX6_LOSS_DISABILITY))) AS out_LOSS_DISABILITY,
	IPFCX6_RESERVE_CATEGORY,
	-- *INF*: IIF(IS_SPACES(IPFCX6_RESERVE_CATEGORY) OR ISNULL(IPFCX6_RESERVE_CATEGORY)
	-- ,'N/A'
	-- ,LTRIM(RTRIM(IPFCX6_RESERVE_CATEGORY))
	-- )
	IFF(IS_SPACES(IPFCX6_RESERVE_CATEGORY) OR IPFCX6_RESERVE_CATEGORY IS NULL, 'N/A', LTRIM(RTRIM(IPFCX6_RESERVE_CATEGORY))) AS out_RESERVE_CATEGORY,
	IPFCX6_LOSS_CAUSE,
	-- *INF*: DECODE(TRUE,IS_SPACES(IPFCX6_LOSS_CAUSE)  OR ISNULL(IPFCX6_LOSS_CAUSE),'N/A',IPFCX6_MAJOR_PERIL = '032' and IPFCX6_LOSS_CAUSE = '07', '06',LTRIM(RTRIM(IPFCX6_LOSS_CAUSE)))
	-- 
	-- 
	DECODE(TRUE,
	IS_SPACES(IPFCX6_LOSS_CAUSE) OR IPFCX6_LOSS_CAUSE IS NULL, 'N/A',
	IPFCX6_MAJOR_PERIL = '032' AND IPFCX6_LOSS_CAUSE = '07', '06',
	LTRIM(RTRIM(IPFCX6_LOSS_CAUSE))) AS out_LOSS_CAUSE,
	IPFCX6_OFFSET_ONSET_IND,
	-- *INF*: IIF(IS_SPACES(IPFCX6_OFFSET_ONSET_IND) OR ISNULL(IPFCX6_OFFSET_ONSET_IND)
	-- ,'N/A'
	-- ,LTRIM(RTRIM(IPFCX6_OFFSET_ONSET_IND))
	-- )
	IFF(IS_SPACES(IPFCX6_OFFSET_ONSET_IND) OR IPFCX6_OFFSET_ONSET_IND IS NULL, 'N/A', LTRIM(RTRIM(IPFCX6_OFFSET_ONSET_IND))) AS out_OFFSET_ONSET_IND,
	-- *INF*: IIF ( LENGTH(v_IPFCX6_MONTH_OF_LOSS) = 1, '0' || LTRIM(RTRIM(v_IPFCX6_MONTH_OF_LOSS)), LTRIM(RTRIM(v_IPFCX6_MONTH_OF_LOSS)))
	-- ||  
	-- IIF ( LENGTH(v_IPFCX6_DAY_OF_LOSS) = 1, '0' || LTRIM(RTRIM(v_IPFCX6_DAY_OF_LOSS)), LTRIM(RTRIM(v_IPFCX6_DAY_OF_LOSS)) )
	-- ||  
	-- LTRIM(RTRIM(v_IPFCX6_YEAR_OF_LOSS))
	IFF(LENGTH(v_IPFCX6_MONTH_OF_LOSS) = 1, '0' || LTRIM(RTRIM(v_IPFCX6_MONTH_OF_LOSS)), LTRIM(RTRIM(v_IPFCX6_MONTH_OF_LOSS))) || IFF(LENGTH(v_IPFCX6_DAY_OF_LOSS) = 1, '0' || LTRIM(RTRIM(v_IPFCX6_DAY_OF_LOSS)), LTRIM(RTRIM(v_IPFCX6_DAY_OF_LOSS))) || LTRIM(RTRIM(v_IPFCX6_YEAR_OF_LOSS)) AS v_loss_date,
	-- *INF*: LTRIM(RTRIM(PIF_SYMBOL)) || LTRIM(RTRIM(PIF_POLICY_NUMBER)) || LTRIM(RTRIM(PIF_MODULE)) || v_loss_date || LOSS_OCCURENCE
	-- 
	LTRIM(RTRIM(PIF_SYMBOL)) || LTRIM(RTRIM(PIF_POLICY_NUMBER)) || LTRIM(RTRIM(PIF_MODULE)) || v_loss_date || LOSS_OCCURENCE AS Claim_Occurrence_Key,
	Claim_Occurrence_Key AS out_Claim_Occurrence_key,
	-- *INF*: Claim_Occurrence_Key || LTRIM(RTRIM(IPFCX6_LOSS_CLAIMANT)) || 'CMT'
	Claim_Occurrence_Key || LTRIM(RTRIM(IPFCX6_LOSS_CLAIMANT)) || 'CMT' AS Claimant_Key,
	-- *INF*: to_date(to_char(IPFCX6_YEAR_ITEM_EFFECTIVE) || '-' || to_char(IPFCX6_MONTH_ITEM_EFFECTIVE) || '-' || to_char(IPFCX6_DAY_ITEM_EFFECTIVE),'YYYY-MM-DD')
	to_date(to_char(IPFCX6_YEAR_ITEM_EFFECTIVE) || '-' || to_char(IPFCX6_MONTH_ITEM_EFFECTIVE) || '-' || to_char(IPFCX6_DAY_ITEM_EFFECTIVE), 'YYYY-MM-DD') AS Claimant_Coverage_Eff_Date,
	-- *INF*: to_date('2100-12-31', 'YYYY-MM-DD')
	to_date('2100-12-31', 'YYYY-MM-DD') AS Claimant_Coverage_Expiration_Date,
	logical_flag
	FROM SQ_PIF_42X6_stage
),
LKP_Claim_Party_Occurrence_AK_ID AS (
	SELECT
	IN_Claim_Occurrence_key,
	IN_Claim_Party_Key,
	claim_party_occurrence_ak_id,
	claimant_num,
	claim_party_role_code
	FROM (
		SELECT CPO.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, 
		CO.claim_occurrence_type_code as offset_onset_ind,
		LTRIM(RTRIM(CO.claim_occurrence_key)) as claimant_num, 
		LTRIM(RTRIM(CP.claim_party_key)) as claim_party_role_code
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence CPO,
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party CP, 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence CO 
		WHERE CO.claim_occurrence_ak_id = CPO.claim_occurrence_ak_id  AND CP.claim_party_ak_id = CPO.claim_party_ak_id 
		AND CO.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND CP.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		--AND CPO.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND CPO.claim_party_role_code = 'CMT'
		AND CO.crrnt_snpsht_flag = 1
		AND CP.crrnt_snpsht_flag = 1
		AND CPO.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_num,claim_party_role_code ORDER BY IN_Claim_Occurrence_key) = 1
),
LKP_Claimant_Coverage_EDW AS (
	SELECT
	claimant_cov_det_ak_id,
	claimant_cov_eff_date,
	risk_type_ind,
	claim_party_occurrence_ak_id1,
	SOURCE_INS_LINE,
	SOURCE_LOCATION_NUMBER,
	SOURCE_SUB_LOCATION_NUMBER,
	SOURCE_RISK_UNIT_GROUP,
	SOURCE_RISK_UNIT_GRP_SEQ,
	SOURCE_RISK_UNIT,
	SOURCE_RISK_UNIT_SEQ_NUM,
	SOURCE_MAJOR_PERIL_CODE,
	SOURCE_MAJOR_PERIL_SEQ,
	SOURCE_LOSS_DISABILITY,
	SOURCE_RESERVE_CATEGORY,
	SOURCE_LOSS_CAUSE,
	SOURCE_MEMBER,
	SOURCE_TYPE_EXPOSURE,
	SOURCE_OFFSET_ONSET_IND,
	claim_party_occurrence_ak_id,
	ins_line,
	loc_unit_num,
	sub_loc_unit_num,
	risk_unit_grp,
	risk_unit_grp_seq_num,
	risk_unit,
	risk_unit_seq_num,
	major_peril_code,
	major_peril_seq,
	pms_loss_disability,
	reserve_ctgry,
	cause_of_loss,
	pms_mbr,
	pms_type_exposure
	FROM (
		SELECT 
		 a.claimant_cov_det_ak_id as claimant_cov_det_ak_id
		, a.claimant_cov_eff_date as claimant_cov_eff_date
		, ltrim(rtrim(a.risk_type_ind)) as risk_type_ind
		, a.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id
		, ltrim(rtrim(a.ins_line)) as ins_line
		, ltrim(rtrim(a.loc_unit_num)) as loc_unit_num
		, ltrim(rtrim(a.sub_loc_unit_num)) as sub_loc_unit_num
		, ltrim(rtrim(a.risk_unit_grp)) as risk_unit_grp
		, ltrim(rtrim(a.risk_unit_grp_seq_num)) as risk_unit_grp_seq_num
		, ltrim(rtrim(a.risk_unit)) as risk_unit
		, ltrim(rtrim(a.risk_unit_seq_num)) as risk_unit_seq_num
		, ltrim(rtrim(a.major_peril_code)) as major_peril_code
		, ltrim(rtrim(a.major_peril_seq)) as major_peril_seq
		, ltrim(rtrim(a.pms_loss_disability)) as pms_loss_disability
		, ltrim(rtrim(a.reserve_ctgry)) as reserve_ctgry
		, ltrim(rtrim(a.cause_of_loss)) as cause_of_loss
		, ltrim(rtrim(a.pms_mbr)) as pms_mbr
		, ltrim(rtrim(a.pms_type_exposure)) as pms_type_exposure
		, ltrim(rtrim(a.offset_onset_ind)) as offset_onset_ind
		from 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_detail a
		where a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and a.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id,ins_line,loc_unit_num,sub_loc_unit_num,risk_unit_grp,risk_unit_grp_seq_num,risk_unit,risk_unit_seq_num,major_peril_code,major_peril_seq,pms_loss_disability,reserve_ctgry,cause_of_loss,pms_mbr,pms_type_exposure ORDER BY claimant_cov_det_ak_id) = 1
),
LKP_claim_party_occurrence AS (
	SELECT
	claim_occurrence_ak_id,
	claim_party_occurrence_ak_id
	FROM (
		SELECT 
			claim_occurrence_ak_id,
			claim_party_occurrence_ak_id
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id ORDER BY claim_occurrence_ak_id) = 1
),
LKP_claim_occurrence AS (
	SELECT
	pol_key_ak_id,
	claim_occurrence_ak_id
	FROM (
		SELECT 
			pol_key_ak_id,
			claim_occurrence_ak_id
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence
		WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id ORDER BY pol_key_ak_id) = 1
),
LKP_Coverage AS (
	SELECT
	type_bureau_code,
	pol_ak_id,
	ins_line,
	loc_unit_num,
	sub_loc_unit_num,
	risk_unit_grp,
	class_code_group,
	class_code_member,
	risk_unit,
	risk_sequence,
	risk_type_ind,
	major_peril_code,
	sequence_type_exposure,
	year_item_effective,
	month_item_effective,
	day_item_effective
	FROM (
		SELECT a.pol_ak_id as pol_ak_id,
		a.ins_line as ins_line,
		LTRIM(RTRIM(a.loc_unit_num)) as loc_unit_num,
		LTRIM(RTRIM(a.sub_loc_unit_num)) as sub_loc_unit_num,
		LTRIM(RTRIM(a.risk_unit_grp)) as risk_unit_grp,
		(CASE WHEN LTRIM(RTRIM(a.risk_unit_grp_seq_num)) not in('N/A','01','---')
		 THEN SUBSTRING(LTRIM(RTRIM(a.risk_unit_grp_seq_num)),1,2)
		 WHEN LTRIM(RTRIM(a.risk_unit_grp_seq_num)) ='N/A'
		 THEN 'N/A'
		 ELSE NULL 
		 END) as class_code_group,
		(CASE WHEN LTRIM(RTRIM(a.risk_unit_grp_seq_num)) not in('N/A','01','---')
		 THEN SUBSTRING(LTRIM(RTRIM(a.risk_unit_grp_seq_num)),3,1)
		 WHEN LTRIM(RTRIM(a.risk_unit_grp_seq_num))='N/A'
		 THEN 'N/A'
		 ELSE NULL
		 END) as class_code_member,
		LTRIM(RTRIM(a.risk_unit)) as risk_unit,
		(CASE WHEN a.risk_unit_seq_num not in('N/A','-0')
		 THEN SUBSTRING(a.risk_unit_seq_num,1,1)
		 WHEN a.risk_unit_seq_num='N/A'
		 THEN 'N/A'
		 ELSE NULL
		 END) as risk_sequence,
		(CASE WHEN a.risk_unit_seq_num not in('N/A','-0')
		 THEN SUBSTRING(a.risk_unit_seq_num,2,1)
		 WHEN a.risk_unit_seq_num='N/A'
		 THEN 'N/A'
		 ELSE NULL
		 END) as risk_type_ind,
		LTRIM(RTRIM(a.major_peril_code)) as major_peril_code,
		(CASE WHEN a.major_peril_seq_num not in('N/A','--','>0','>1','>2','>6','>7','0','1','2','3','NA')
		 THEN SUBSTRING(a.major_peril_seq_num,1,2)
		 WHEN a.major_peril_seq_num='N/A'
		 THEN 'N/A'
		 ELSE NULL
		 END) as sequence_type_exposure,
		YEAR(a.cov_eff_date) as year_item_effective,
		MONTH(a.cov_eff_date) as month_item_effective,
		DAY(a.cov_eff_date) as day_item_effective,
		a.type_bureau_code as type_bureau_code
		FROM   V2.COVERAGE A  INNER JOIN V2.policy P ON A.pol_ak_id = P.pol_ak_id
		WHERE  A.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		       AND A.crrnt_snpsht_flag = 1 AND P.crrnt_snpsht_flag = 1
		       AND P.pol_key IN (SELECT DISTINCT pif_symbol+pif_policy_number+pif_module from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_42x6_stage)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id,ins_line,loc_unit_num,sub_loc_unit_num,risk_unit_grp,class_code_group,class_code_member,risk_unit,risk_sequence,risk_type_ind,major_peril_code,sequence_type_exposure,year_item_effective,month_item_effective,day_item_effective ORDER BY type_bureau_code) = 1
),
LKP_Policy AS (
	SELECT
	pms_pol_lob_code,
	InsuranceSegmentAKId,
	pol_ak_id
	FROM (
		SELECT 
			pms_pol_lob_code,
			InsuranceSegmentAKId,
			pol_ak_id
		FROM V2.policy
		WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id ORDER BY pms_pol_lob_code) = 1
),
LKP_InusuranceSegment AS (
	SELECT
	InsuranceSegmentCode,
	InsuranceSegmentId
	FROM (
		SELECT 
			InsuranceSegmentCode,
			InsuranceSegmentId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceSegment
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceSegmentId ORDER BY InsuranceSegmentCode) = 1
),
LKP_sup_CauseOfLoss AS (
	SELECT
	CauseOfLossId,
	LineOfBusiness,
	MajorPeril,
	CauseOfLoss
	FROM (
		SELECT LTRIM(RTRIM(a.LineOfBusiness)) as LineOfBusiness,
		LTRIM(RTRIM(a.MajorPeril)) as MajorPeril,
		LTRIM(RTRIM(a.CauseOfLoss)) as CauseOfLoss,
		a.CauseOfLossId as CauseOfLossId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_CauseOfLoss a
		where a.CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY LineOfBusiness,MajorPeril,CauseOfLoss ORDER BY CauseOfLossId) = 1
),
LKP_sup_insurance_line AS (
	SELECT
	sup_ins_line_id,
	ins_line_code
	FROM (
		SELECT 
			sup_ins_line_id,
			ins_line_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_insurance_line
		WHERE source_sys_id = '@{pipeline().parameters.MERGED_SOURCE_SYSTEM_ID}' and crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ins_line_code ORDER BY sup_ins_line_id) = 1
),
LKP_sup_major_peril AS (
	SELECT
	sup_major_peril_id,
	major_peril_code
	FROM (
		SELECT 
			sup_major_peril_id,
			major_peril_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_major_peril
		WHERE source_sys_id = '@{pipeline().parameters.MERGED_SOURCE_SYSTEM_ID}' and crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY major_peril_code ORDER BY sup_major_peril_id) = 1
),
LKP_sup_risk_unit AS (
	SELECT
	in_INSURANCE_LINE,
	sup_risk_unit_id,
	risk_unit_code,
	ins_line
	FROM (
		SELECT 
			in_INSURANCE_LINE,
			sup_risk_unit_id,
			risk_unit_code,
			ins_line
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_risk_unit
		WHERE source_sys_id = '@{pipeline().parameters.MERGED_SOURCE_SYSTEM_ID}' and crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY risk_unit_code,ins_line ORDER BY in_INSURANCE_LINE) = 1
),
LKP_sup_risk_unit_group AS (
	SELECT
	in_INSURANCE_LINE,
	in_RISK_TYPE_IND,
	sup_risk_unit_grp_id,
	risk_unit_grp_code,
	ins_line,
	prdct_type_code
	FROM (
		SELECT 
		sup_risk_unit_group.sup_risk_unit_grp_id as sup_risk_unit_grp_id, sup_risk_unit_group.risk_unit_grp_code as risk_unit_grp_code, 
		sup_risk_unit_group.ins_line as ins_line, 
		LTRIM(RTRIM(sup_risk_unit_group.prdct_type_code)) as prdct_type_code 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_risk_unit_group
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY risk_unit_grp_code,ins_line,prdct_type_code ORDER BY in_INSURANCE_LINE) = 1
),
LKP_sup_type_bureau_code AS (
	SELECT
	sup_type_bureau_code_id,
	type_bureau_code
	FROM (
		SELECT 
			sup_type_bureau_code_id,
			type_bureau_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_type_bureau_code
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY type_bureau_code ORDER BY sup_type_bureau_code_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_Claimant_Coverage_EDW.claimant_cov_det_ak_id,
	LKP_Claimant_Coverage_EDW.claim_party_occurrence_ak_id1,
	LKP_Claimant_Coverage_EDW.claimant_cov_eff_date AS old_claimant_cov_eff_date,
	LKP_Claimant_Coverage_EDW.risk_type_ind AS old_risk_type_ind,
	LKP_Claimant_Coverage_EDW.SOURCE_INS_LINE,
	LKP_Claimant_Coverage_EDW.SOURCE_LOCATION_NUMBER,
	LKP_Claimant_Coverage_EDW.SOURCE_SUB_LOCATION_NUMBER,
	LKP_Claimant_Coverage_EDW.SOURCE_RISK_UNIT_GROUP,
	LKP_Claimant_Coverage_EDW.SOURCE_RISK_UNIT_GRP_SEQ,
	LKP_Claimant_Coverage_EDW.SOURCE_RISK_UNIT,
	LKP_Claimant_Coverage_EDW.SOURCE_RISK_UNIT_SEQ_NUM,
	LKP_Claimant_Coverage_EDW.SOURCE_MAJOR_PERIL_CODE,
	LKP_Claimant_Coverage_EDW.SOURCE_MAJOR_PERIL_SEQ,
	LKP_Claimant_Coverage_EDW.SOURCE_LOSS_DISABILITY,
	LKP_Claimant_Coverage_EDW.SOURCE_RESERVE_CATEGORY,
	LKP_Claimant_Coverage_EDW.SOURCE_LOSS_CAUSE,
	LKP_Claimant_Coverage_EDW.SOURCE_MEMBER,
	LKP_Claimant_Coverage_EDW.SOURCE_TYPE_EXPOSURE,
	LKP_Claimant_Coverage_EDW.SOURCE_OFFSET_ONSET_IND,
	EXP_Source.out_RISK_TYPE_IND AS SOURCE_RISK_TYPE_IND,
	EXP_Source.Claimant_Coverage_Eff_Date,
	EXP_Source.Claimant_Coverage_Expiration_Date,
	-- *INF*: iif(isnull(claimant_cov_det_ak_id)
	-- , 'NEW'
	-- ,iif(old_claimant_cov_eff_date != Claimant_Coverage_Eff_Date or
	-- old_risk_type_ind != SOURCE_RISK_TYPE_IND 
	-- ,'UPDATE'
	-- ,'NOCHANGE')
	-- )
	IFF(claimant_cov_det_ak_id IS NULL, 'NEW', IFF(old_claimant_cov_eff_date != Claimant_Coverage_Eff_Date OR old_risk_type_ind != SOURCE_RISK_TYPE_IND, 'UPDATE', 'NOCHANGE')) AS v_Changed_Flag,
	EXP_Source.logical_flag,
	1 AS crrnt_snpsht_flag,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_ID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS Source_System_Id,
	-- *INF*: iif(v_Changed_Flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_Changed_Flag = 'NEW', to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), sysdate) AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	to_date('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	sysdate AS created_date,
	LKP_sup_insurance_line.sup_ins_line_id,
	-- *INF*: IIF(ISNULL(sup_ins_line_id), -1, sup_ins_line_id)
	IFF(sup_ins_line_id IS NULL, - 1, sup_ins_line_id) AS out_sup_ins_line_id,
	LKP_sup_risk_unit_group.sup_risk_unit_grp_id,
	-- *INF*: IIF(ISNULL(sup_risk_unit_grp_id), -1, sup_risk_unit_grp_id)
	IFF(sup_risk_unit_grp_id IS NULL, - 1, sup_risk_unit_grp_id) AS out_sup_risk_unit_grp_id,
	LKP_sup_risk_unit.sup_risk_unit_id,
	-- *INF*: IIF(ISNULL(sup_risk_unit_id), -1, sup_risk_unit_id)
	IFF(sup_risk_unit_id IS NULL, - 1, sup_risk_unit_id) AS out_sup_risk_unit_id,
	LKP_sup_major_peril.sup_major_peril_id,
	-- *INF*: IIF(ISNULL(sup_major_peril_id), -1, sup_major_peril_id)
	IFF(sup_major_peril_id IS NULL, - 1, sup_major_peril_id) AS out_sup_major_peril_id,
	LKP_sup_CauseOfLoss.CauseOfLossId,
	-- *INF*: IIF(ISNULL(CauseOfLossId), -1, CauseOfLossId)
	IFF(CauseOfLossId IS NULL, - 1, CauseOfLossId) AS out_CauseOfLossId,
	LKP_Coverage.type_bureau_code AS TypeBureauCode,
	LKP_sup_type_bureau_code.sup_type_bureau_code_id,
	-- *INF*: IIF(ISNULL(sup_type_bureau_code_id), -1, sup_type_bureau_code_id)
	IFF(sup_type_bureau_code_id IS NULL, - 1, sup_type_bureau_code_id) AS out_sup_type_bureau_code_id,
	-1 AS SupVehicleRegistrationStateID,
	'PMS' AS policy_src_id,
	'N/A' AS CoverageForm,
	'N/A' AS RiskType,
	'N/A' AS CoverageType,
	'N/A' AS CoverageVersion,
	'N/A' AS AnnualStatementLineNumber,
	'N/A' AS ClassCode,
	'N/A' AS SublineCode,
	-1 AS RatingCoverageAKID,
	LKP_claim_occurrence.pol_key_ak_id,
	-- *INF*: DECODE(TRUE,
	-- SOURCE_RISK_UNIT_SEQ_NUM='0' AND SOURCE_INS_LINE='WC',
	-- '00',
	-- IN(SOURCE_RISK_UNIT_SEQ_NUM, '0','1') AND SOURCE_INS_LINE<>'WC' AND SOURCE_RISK_TYPE_IND='N/A',
	-- 'N/A',
	-- IN(SOURCE_RISK_UNIT_SEQ_NUM, '0','1','2','3','4','8') AND SOURCE_INS_LINE='GL',
	-- SOURCE_RISK_UNIT_SEQ_NUM || SOURCE_RISK_TYPE_IND,
	-- SOURCE_RISK_UNIT_SEQ_NUM
	-- )
	DECODE(TRUE,
	SOURCE_RISK_UNIT_SEQ_NUM = '0' AND SOURCE_INS_LINE = 'WC', '00',
	IN(SOURCE_RISK_UNIT_SEQ_NUM, '0', '1') AND SOURCE_INS_LINE <> 'WC' AND SOURCE_RISK_TYPE_IND = 'N/A', 'N/A',
	IN(SOURCE_RISK_UNIT_SEQ_NUM, '0', '1', '2', '3', '4', '8') AND SOURCE_INS_LINE = 'GL', SOURCE_RISK_UNIT_SEQ_NUM || SOURCE_RISK_TYPE_IND,
	SOURCE_RISK_UNIT_SEQ_NUM) AS o_RiskUnitSequenceNumber_AKId,
	'N/A' AS o_pms_type_bureau_code,
	LKP_InusuranceSegment.InsuranceSegmentCode AS i_InsuranceSegmentCode,
	-- *INF*: IIF(ISNULL(i_InsuranceSegmentCode),'N/A',i_InsuranceSegmentCode)
	IFF(i_InsuranceSegmentCode IS NULL, 'N/A', i_InsuranceSegmentCode) AS o_InsuranceSegmentCode
	FROM EXP_Source
	LEFT JOIN LKP_Claimant_Coverage_EDW
	ON LKP_Claimant_Coverage_EDW.claim_party_occurrence_ak_id = LKP_Claim_Party_Occurrence_AK_ID.claim_party_occurrence_ak_id AND LKP_Claimant_Coverage_EDW.ins_line = EXP_Source.out_INSURANCE_LINE AND LKP_Claimant_Coverage_EDW.loc_unit_num = EXP_Source.out_LOCATION_NUMBER AND LKP_Claimant_Coverage_EDW.sub_loc_unit_num = EXP_Source.out_SUB_LOCATION_NUMBER AND LKP_Claimant_Coverage_EDW.risk_unit_grp = EXP_Source.out_RISK_UNIT_GROUP AND LKP_Claimant_Coverage_EDW.risk_unit_grp_seq_num = EXP_Source.RISK_UNIT_GRP_SEQ AND LKP_Claimant_Coverage_EDW.risk_unit = EXP_Source.out_LOSS_UNIT AND LKP_Claimant_Coverage_EDW.risk_unit_seq_num = EXP_Source.out_RISK_SEQUENCE AND LKP_Claimant_Coverage_EDW.major_peril_code = EXP_Source.out_MAJOR_PERIL AND LKP_Claimant_Coverage_EDW.major_peril_seq = EXP_Source.out_SEQUENCE_TYPE_EXPOSURE AND LKP_Claimant_Coverage_EDW.pms_loss_disability = EXP_Source.out_LOSS_DISABILITY AND LKP_Claimant_Coverage_EDW.reserve_ctgry = EXP_Source.out_RESERVE_CATEGORY AND LKP_Claimant_Coverage_EDW.cause_of_loss = EXP_Source.out_LOSS_CAUSE AND LKP_Claimant_Coverage_EDW.pms_mbr = EXP_Source.out_MEMBER AND LKP_Claimant_Coverage_EDW.pms_type_exposure = EXP_Source.out_TYPE_EXPOSURE
	LEFT JOIN LKP_Coverage
	ON LKP_Coverage.pol_ak_id = LKP_claim_occurrence.pol_key_ak_id AND LKP_Coverage.ins_line = LKP_Claimant_Coverage_EDW.SOURCE_INS_LINE AND LKP_Coverage.loc_unit_num = LKP_Claimant_Coverage_EDW.SOURCE_LOCATION_NUMBER AND LKP_Coverage.sub_loc_unit_num = LKP_Claimant_Coverage_EDW.SOURCE_SUB_LOCATION_NUMBER AND LKP_Coverage.risk_unit_grp = LKP_Claimant_Coverage_EDW.SOURCE_RISK_UNIT_GROUP AND LKP_Coverage.class_code_group = EXP_Source.out_CLASS_CODE_GROUP AND LKP_Coverage.class_code_member = EXP_Source.out_CLASS_CODE_MEMBER AND LKP_Coverage.risk_unit = LKP_Claimant_Coverage_EDW.SOURCE_RISK_UNIT AND LKP_Coverage.risk_sequence = LKP_Claimant_Coverage_EDW.SOURCE_RISK_UNIT_SEQ_NUM AND LKP_Coverage.risk_type_ind = EXP_Source.out_RISK_TYPE_IND AND LKP_Coverage.major_peril_code = LKP_Claimant_Coverage_EDW.SOURCE_MAJOR_PERIL_CODE AND LKP_Coverage.sequence_type_exposure = LKP_Claimant_Coverage_EDW.SOURCE_MAJOR_PERIL_SEQ AND LKP_Coverage.year_item_effective = EXP_Source.out_IPFCX6_YEAR_ITEM_EFFECTIVE AND LKP_Coverage.month_item_effective = EXP_Source.out_IPFCX6_MONTH_ITEM_EFFECTIVE AND LKP_Coverage.day_item_effective = EXP_Source.out_IPFCX6_DAY_ITEM_EFFECTIVE
	LEFT JOIN LKP_InusuranceSegment
	ON LKP_InusuranceSegment.InsuranceSegmentId = LKP_Policy.InsuranceSegmentAKId
	LEFT JOIN LKP_claim_occurrence
	ON LKP_claim_occurrence.claim_occurrence_ak_id = LKP_claim_party_occurrence.claim_occurrence_ak_id
	LEFT JOIN LKP_sup_CauseOfLoss
	ON LKP_sup_CauseOfLoss.LineOfBusiness = LKP_Policy.pms_pol_lob_code AND LKP_sup_CauseOfLoss.MajorPeril = LKP_Claimant_Coverage_EDW.SOURCE_MAJOR_PERIL_CODE AND LKP_sup_CauseOfLoss.CauseOfLoss = LKP_Claimant_Coverage_EDW.SOURCE_LOSS_CAUSE
	LEFT JOIN LKP_sup_insurance_line
	ON LKP_sup_insurance_line.ins_line_code = LKP_Claimant_Coverage_EDW.SOURCE_INS_LINE
	LEFT JOIN LKP_sup_major_peril
	ON LKP_sup_major_peril.major_peril_code = LKP_Claimant_Coverage_EDW.SOURCE_MAJOR_PERIL_CODE
	LEFT JOIN LKP_sup_risk_unit
	ON LKP_sup_risk_unit.risk_unit_code = LKP_Claimant_Coverage_EDW.SOURCE_RISK_UNIT AND LKP_sup_risk_unit.ins_line = EXP_Source.out_INSURANCE_LINE
	LEFT JOIN LKP_sup_risk_unit_group
	ON LKP_sup_risk_unit_group.risk_unit_grp_code = LKP_Claimant_Coverage_EDW.SOURCE_RISK_UNIT_GROUP AND LKP_sup_risk_unit_group.ins_line = EXP_Source.out_INSURANCE_LINE AND LKP_sup_risk_unit_group.prdct_type_code = EXP_Source.out_RISK_TYPE_IND
	LEFT JOIN LKP_sup_type_bureau_code
	ON LKP_sup_type_bureau_code.type_bureau_code = LKP_Coverage.type_bureau_code
),
FIL_Insert AS (
	SELECT
	claimant_cov_det_ak_id, 
	claim_party_occurrence_ak_id1, 
	SOURCE_INS_LINE AS out_INS_LINE, 
	SOURCE_LOCATION_NUMBER AS out_LOCATION_NUMBER, 
	SOURCE_SUB_LOCATION_NUMBER AS out_SUB_LOCATION_NUMBER, 
	SOURCE_RISK_UNIT_GROUP AS out_RISK_UNIT_GROUP, 
	SOURCE_RISK_UNIT_GRP_SEQ AS out_RISK_UNIT_GRP_SEQ, 
	SOURCE_RISK_UNIT AS out_RISK_UNIT, 
	SOURCE_RISK_UNIT_SEQ_NUM AS out_SOURCE_RISK_UNIT_SEQ_NUM, 
	SOURCE_MAJOR_PERIL_CODE AS out_MAJOR_PERIL_CODE, 
	SOURCE_MAJOR_PERIL_SEQ AS out_MAJOR_PERIL_SEQ, 
	SOURCE_LOSS_DISABILITY AS out_LOSS_DISABILITY, 
	SOURCE_RESERVE_CATEGORY AS out_RESERVE_CATEGORY, 
	SOURCE_LOSS_CAUSE AS out_LOSS_CAUSE, 
	SOURCE_MEMBER AS out_MEMBER, 
	SOURCE_TYPE_EXPOSURE AS out_TYPE_EXPOSURE, 
	SOURCE_OFFSET_ONSET_IND AS out_OFFSET_ONSET_IND, 
	SOURCE_RISK_TYPE_IND AS out_RISK_TYPE_IND, 
	Claimant_Coverage_Eff_Date, 
	Claimant_Coverage_Expiration_Date, 
	logical_flag, 
	crrnt_snpsht_flag, 
	Changed_Flag, 
	Audit_ID, 
	Source_System_Id, 
	eff_from_date, 
	eff_to_date, 
	created_date, 
	out_sup_ins_line_id AS sup_ins_line_id, 
	out_sup_risk_unit_grp_id AS sup_risk_unit_grp_id, 
	out_sup_risk_unit_id AS sup_risk_unit_id, 
	out_sup_major_peril_id AS sup_major_peril_id, 
	out_CauseOfLossId AS CauseOfLossId, 
	TypeBureauCode, 
	out_sup_type_bureau_code_id, 
	SupVehicleRegistrationStateID, 
	policy_src_id, 
	CoverageForm, 
	RiskType, 
	CoverageType, 
	CoverageVersion, 
	AnnualStatementLineNumber, 
	ClassCode, 
	SublineCode, 
	RatingCoverageAKID, 
	pol_key_ak_id, 
	o_RiskUnitSequenceNumber_AKId AS RiskUnitSequenceNumber_AKId, 
	o_pms_type_bureau_code AS pms_type_bureau_code, 
	o_InsuranceSegmentCode
	FROM EXP_Detect_Changes
	WHERE Changed_Flag='NEW' or Changed_Flag='UPDATE'
),
LKP_StatisticalCoverageForPMSExceed AS (
	SELECT
	InsuranceReferenceLineOfBusinessAKId,
	ProductAKId,
	StatisticalCoverageAKID,
	CoverageGuid,
	PolicyAKID,
	InsuranceLine,
	LocationNumber,
	MajorPerilCode,
	MajorPerilSequenceNumber,
	RiskUnit,
	RiskUnitSequenceNumber,
	RiskUnitGroup,
	RiskUnitGroupSequenceNumber,
	SubLocationUnitNumber,
	TypeBureauCode,
	MaxPolicyCovEffDate
	FROM (
		Select 
		DISTINCT SC.InsuranceReferenceLineOfBusinessAKID as InsuranceReferenceLineOfBusinessAKID,
		SC.ProductAKID as ProductAKID,
		SC.StatisticalCoverageAKID as StatisticalCoverageAKID,
		SC.CoverageGuid as CoverageGuid,
		PC.PolicyAKID as PolicyAKID, 
		PC.InsuranceLine as InsuranceLine, 
		(CASE WHEN RL.LocationIndicator = 'N' THEN '0000' ELSE RL.LocationUnitNumber END) as LocationNumber,
		SC.MajorPerilCode as MajorPerilCode,
		SC.MajorPerilSequenceNumber as MajorPerilSequenceNumber,
		SC.RiskUnit as RiskUnit,
		(CASE WHEN SC.RiskUnitSequenceNumber = '0' then 'N/A' ELSE SC.RiskUnitSequenceNumber END) as RiskUnitSequenceNumber,
		SC.RiskUnitGroup as RiskUnitGroup,
		SC.RiskUnitGroupSequenceNumber as RiskUnitGroupSequenceNumber,
		(CASE WHEN SC.SubLocationUnitNumber = 'N/A' THEN '000' ELSE SC.SubLocationUnitNumber END) as SubLocationUnitNumber,
		PC.TypeBureauCode as TypeBureauCode,
		MAX(PC.PolicyCoverageEffectiveDate) as MaxPolicyCovEffDate
		
		FROM
		@{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage SC ,
		@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC,
		@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation RL,
		V2.policy p
		WHERE SC.PolicyCoverageAKID = PC.PolicyCoverageAKID 
		AND PC.RiskLocationAKID = RL.RiskLocationAKID  
		AND  PC.PolicyAKID = p.pol_ak_id 
		AND P.crrnt_snpsht_flag=1 
		AND P.source_sys_id='PMS'
		AND  EXISTS (SELECT DISTINCT pol_key_ak_id 
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence,
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PIF_42X6_STAGE
		where claim_occurrence_key=(pif_symbol+pif_policy_number+pif_module+right('0'+convert(varchar,ipfcx6_month_of_loss),2) +right('0'+convert(varchar,ipfcx6_day_of_loss),2)+convert(varchar,ipfcx6_year_of_loss)+ipfcx6_loss_occ_fdigit+right('0'+convert(varchar,ipfcx6_usr_loss_occurence),2) ) and 
		crrnt_snpsht_flag = 1 AND PC.PolicyAKID= pol_key_ak_id )
		GROUP BY SC.InsuranceReferenceLineOfBusinessAKID,
		SC.ProductAKID,
		SC.StatisticalCoverageAKID,
		SC.CoverageGuid,
		PC.PolicyAKID, 
		PC.InsuranceLine,
		CASE WHEN RL.LocationIndicator = 'N' THEN '0000' ELSE RL.LocationUnitNumber END,
		SC.MajorPerilCode,
		SC.MajorPerilSequenceNumber,
		SC.RiskUnit,
		CASE WHEN SC.RiskUnitSequenceNumber = '0' then 'N/A' ELSE SC.RiskUnitSequenceNumber END ,
		SC.RiskUnitGroup, 
		SC.RiskUnitGroupSequenceNumber ,
		CASE WHEN SC.SubLocationUnitNumber = 'N/A' THEN '000' ELSE SC.SubLocationUnitNumber END ,
		PC.TypeBureauCode
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID,InsuranceLine,LocationNumber,MajorPerilCode,MajorPerilSequenceNumber,RiskUnit,RiskUnitSequenceNumber,RiskUnitGroup,RiskUnitGroupSequenceNumber,SubLocationUnitNumber,TypeBureauCode,MaxPolicyCovEffDate ORDER BY InsuranceReferenceLineOfBusinessAKId) = 1
),
Lkp_SupTypeOfLossRules AS (
	SELECT
	TypeOfLoss,
	ClaimTypeCategory,
	ClaimTypeGroup,
	SubrogationEligibleIndicator,
	MajorPerilCode,
	CauseOfLoss,
	InsuranceSegmentCode
	FROM (
		SELECT 
			TypeOfLoss,
			ClaimTypeCategory,
			ClaimTypeGroup,
			SubrogationEligibleIndicator,
			MajorPerilCode,
			CauseOfLoss,
			InsuranceSegmentCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupTypeOfLossRules
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY MajorPerilCode,CauseOfLoss,InsuranceSegmentCode ORDER BY TypeOfLoss DESC) = 1
),
SEQ_Claimant_Coverage_Detail AS (
	CREATE SEQUENCE SEQ_Claimant_Coverage_Detail
	START = 0
	INCREMENT = 1;
),
Exp_Determine_AK AS (
	SELECT
	FIL_Insert.claimant_cov_det_ak_id,
	SEQ_Claimant_Coverage_Detail.NEXTVAL,
	-- *INF*: iif(isnull(claimant_cov_det_ak_id)
	-- ,NEXTVAL
	-- ,claimant_cov_det_ak_id)
	IFF(claimant_cov_det_ak_id IS NULL, NEXTVAL, claimant_cov_det_ak_id) AS out_Claimant_cov_Det_AK_id,
	FIL_Insert.claim_party_occurrence_ak_id1,
	FIL_Insert.out_INS_LINE,
	FIL_Insert.out_LOCATION_NUMBER,
	FIL_Insert.out_SUB_LOCATION_NUMBER,
	FIL_Insert.out_RISK_UNIT_GROUP,
	FIL_Insert.out_RISK_UNIT_GRP_SEQ,
	FIL_Insert.out_RISK_UNIT,
	FIL_Insert.out_SOURCE_RISK_UNIT_SEQ_NUM,
	FIL_Insert.out_MAJOR_PERIL_CODE,
	FIL_Insert.out_MAJOR_PERIL_SEQ,
	FIL_Insert.out_LOSS_DISABILITY,
	FIL_Insert.out_RESERVE_CATEGORY,
	FIL_Insert.out_LOSS_CAUSE,
	FIL_Insert.out_MEMBER,
	FIL_Insert.out_TYPE_EXPOSURE,
	FIL_Insert.out_OFFSET_ONSET_IND,
	FIL_Insert.out_RISK_TYPE_IND,
	FIL_Insert.Claimant_Coverage_Eff_Date,
	FIL_Insert.Claimant_Coverage_Expiration_Date,
	FIL_Insert.logical_flag,
	FIL_Insert.crrnt_snpsht_flag,
	FIL_Insert.Changed_Flag,
	FIL_Insert.Audit_ID,
	FIL_Insert.Source_System_Id,
	FIL_Insert.eff_from_date,
	FIL_Insert.eff_to_date,
	FIL_Insert.created_date,
	'N/A' AS Dummy_String,
	0 AS Dummy_Integer,
	FIL_Insert.sup_ins_line_id,
	FIL_Insert.sup_risk_unit_grp_id,
	FIL_Insert.sup_risk_unit_id,
	FIL_Insert.sup_major_peril_id,
	FIL_Insert.CauseOfLossId,
	FIL_Insert.TypeBureauCode,
	-- *INF*: IIF(ISNULL(TypeBureauCode), 'N/A', TypeBureauCode)
	IFF(TypeBureauCode IS NULL, 'N/A', TypeBureauCode) AS out_TypeBureauCode,
	FIL_Insert.out_sup_type_bureau_code_id,
	FIL_Insert.SupVehicleRegistrationStateID,
	FIL_Insert.policy_src_id,
	FIL_Insert.CoverageForm,
	FIL_Insert.RiskType,
	FIL_Insert.CoverageType,
	FIL_Insert.CoverageVersion,
	FIL_Insert.AnnualStatementLineNumber,
	FIL_Insert.ClassCode,
	FIL_Insert.SublineCode,
	FIL_Insert.RatingCoverageAKID,
	LKP_StatisticalCoverageForPMSExceed.InsuranceReferenceLineOfBusinessAKId AS i_InsuranceReferenceLineOfBusinessAKId,
	LKP_StatisticalCoverageForPMSExceed.ProductAKId AS i_ProductAKId,
	LKP_StatisticalCoverageForPMSExceed.StatisticalCoverageAKID AS i_StatisticalCoverageAKID,
	LKP_StatisticalCoverageForPMSExceed.CoverageGuid AS i_CoverageGuid,
	-- *INF*: IIF(ISNULL(i_CoverageGuid),'N/A',i_CoverageGuid)
	IFF(i_CoverageGuid IS NULL, 'N/A', i_CoverageGuid) AS o_CoverageGUID,
	-- *INF*: IIF(ISNULL(i_InsuranceReferenceLineOfBusinessAKId), -1, i_InsuranceReferenceLineOfBusinessAKId)
	IFF(i_InsuranceReferenceLineOfBusinessAKId IS NULL, - 1, i_InsuranceReferenceLineOfBusinessAKId) AS o_InsuranceReferenceLineOfBusinessAKId,
	-- *INF*: IIF(ISNULL(i_ProductAKId), -1,i_ProductAKId)
	IFF(i_ProductAKId IS NULL, - 1, i_ProductAKId) AS o_ProductAKId,
	-- *INF*: IIF(ISNULL(i_StatisticalCoverageAKID), -1, i_StatisticalCoverageAKID)
	IFF(i_StatisticalCoverageAKID IS NULL, - 1, i_StatisticalCoverageAKID) AS o_StatisticalCoverageAKID,
	Lkp_SupTypeOfLossRules.TypeOfLoss AS i_TypeOfLoss,
	Lkp_SupTypeOfLossRules.ClaimTypeCategory AS i_ClaimTypeCategory,
	Lkp_SupTypeOfLossRules.ClaimTypeGroup AS i_ClaimTypeGroup,
	Lkp_SupTypeOfLossRules.SubrogationEligibleIndicator AS i_SubrogationEligibleIndicator,
	-- *INF*: IIF(ISNULL(i_TypeOfLoss) ,'N/A',i_TypeOfLoss)
	-- --IIF(ISNULL(i_TypeOfLoss)    OR   i_TypeOfLoss = 'Unassigned'       ,'N/A',i_TypeOfLoss)
	IFF(i_TypeOfLoss IS NULL, 'N/A', i_TypeOfLoss) AS o_TypeOfLoss,
	-- *INF*: IIF(ISNULL(i_ClaimTypeCategory),'N/A',i_ClaimTypeCategory)
	IFF(i_ClaimTypeCategory IS NULL, 'N/A', i_ClaimTypeCategory) AS o_ClaimTypeCategory,
	-- *INF*: IIF(ISNULL(i_ClaimTypeGroup),'N/A',i_ClaimTypeGroup)
	IFF(i_ClaimTypeGroup IS NULL, 'N/A', i_ClaimTypeGroup) AS o_ClaimTypeGroup,
	-- *INF*: IIF(ISNULL(i_SubrogationEligibleIndicator),'N/A',i_SubrogationEligibleIndicator)
	IFF(i_SubrogationEligibleIndicator IS NULL, 'N/A', i_SubrogationEligibleIndicator) AS o_SubrogationEligibleIndicator
	FROM FIL_Insert
	LEFT JOIN LKP_StatisticalCoverageForPMSExceed
	ON LKP_StatisticalCoverageForPMSExceed.PolicyAKID = FIL_Insert.pol_key_ak_id AND LKP_StatisticalCoverageForPMSExceed.InsuranceLine = FIL_Insert.out_INS_LINE AND LKP_StatisticalCoverageForPMSExceed.LocationNumber = FIL_Insert.out_LOCATION_NUMBER AND LKP_StatisticalCoverageForPMSExceed.MajorPerilCode = FIL_Insert.out_MAJOR_PERIL_CODE AND LKP_StatisticalCoverageForPMSExceed.MajorPerilSequenceNumber = FIL_Insert.out_MAJOR_PERIL_SEQ AND LKP_StatisticalCoverageForPMSExceed.RiskUnit = FIL_Insert.out_RISK_UNIT AND LKP_StatisticalCoverageForPMSExceed.RiskUnitSequenceNumber = FIL_Insert.RiskUnitSequenceNumber_AKId AND LKP_StatisticalCoverageForPMSExceed.RiskUnitGroup = FIL_Insert.out_RISK_UNIT_GROUP AND LKP_StatisticalCoverageForPMSExceed.RiskUnitGroupSequenceNumber = FIL_Insert.out_RISK_UNIT_GRP_SEQ AND LKP_StatisticalCoverageForPMSExceed.SubLocationUnitNumber = FIL_Insert.out_SUB_LOCATION_NUMBER AND LKP_StatisticalCoverageForPMSExceed.TypeBureauCode = FIL_Insert.TypeBureauCode AND LKP_StatisticalCoverageForPMSExceed.MaxPolicyCovEffDate = FIL_Insert.Claimant_Coverage_Eff_Date
	LEFT JOIN Lkp_SupTypeOfLossRules
	ON Lkp_SupTypeOfLossRules.MajorPerilCode = FIL_Insert.out_MAJOR_PERIL_CODE AND Lkp_SupTypeOfLossRules.CauseOfLoss = FIL_Insert.out_LOSS_CAUSE AND Lkp_SupTypeOfLossRules.InsuranceSegmentCode = FIL_Insert.o_InsuranceSegmentCode
),
claimant_coverage_detail_insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_detail
	(claimant_cov_det_ak_id, claim_party_occurrence_ak_id, s3p_object_type_code, s3p_object_seq_num, s3p_pkg_seq_num, s3p_ins_line_code, s3p_unit_type_code, s3p_wc_class_descript, loc_unit_num, sub_loc_unit_num, ins_line, risk_unit_grp, risk_unit_grp_seq_num, risk_unit, risk_unit_seq_num, major_peril_code, major_peril_seq, pms_loss_disability, reserve_ctgry, cause_of_loss, pms_mbr, pms_type_exposure, pms_type_bureau_code, offset_onset_ind, claimant_cov_eff_date, claimant_cov_exp_date, risk_type_ind, s3p_unit_descript, spec_pers_prop_use_code, pkg_ded_amt, pkg_lmt_amt, manual_entry_ind, unit_veh_registration_state_code, unit_veh_stated_amt, unit_dam_descript, unit_veh_yr, unit_veh_make, unit_vin_num, logical_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, SupInsuranceLineID, sup_risk_unit_grp_id, sup_risk_unit_id, SupMajorPerilID, CauseOfLossID, SupTypeBureauCodeID, SupVehicleRegistrationStateID, PolicySourceID, CoverageForm, RiskType, CoverageType, CoverageVersion, AnnualStatementLineNumber, ClassCode, SublineCode, RatingCoverageAKId, CoverageGUID, StatisticalCoverageAKID, InsuranceReferenceLineOfBusinessAKId, ProductAKId, TypeOfLoss, ClaimTypeCategory, ClaimTypeGroup, SubrogationEligibleIndicator)
	SELECT 
	out_Claimant_cov_Det_AK_id AS CLAIMANT_COV_DET_AK_ID, 
	claim_party_occurrence_ak_id1 AS CLAIM_PARTY_OCCURRENCE_AK_ID, 
	Dummy_String AS S3P_OBJECT_TYPE_CODE, 
	Dummy_String AS S3P_OBJECT_SEQ_NUM, 
	Dummy_String AS S3P_PKG_SEQ_NUM, 
	Dummy_String AS S3P_INS_LINE_CODE, 
	Dummy_String AS S3P_UNIT_TYPE_CODE, 
	Dummy_String AS S3P_WC_CLASS_DESCRIPT, 
	out_LOCATION_NUMBER AS LOC_UNIT_NUM, 
	out_SUB_LOCATION_NUMBER AS SUB_LOC_UNIT_NUM, 
	out_INS_LINE AS INS_LINE, 
	out_RISK_UNIT_GROUP AS RISK_UNIT_GRP, 
	out_RISK_UNIT_GRP_SEQ AS RISK_UNIT_GRP_SEQ_NUM, 
	out_RISK_UNIT AS RISK_UNIT, 
	out_SOURCE_RISK_UNIT_SEQ_NUM AS RISK_UNIT_SEQ_NUM, 
	out_MAJOR_PERIL_CODE AS MAJOR_PERIL_CODE, 
	out_MAJOR_PERIL_SEQ AS MAJOR_PERIL_SEQ, 
	out_LOSS_DISABILITY AS PMS_LOSS_DISABILITY, 
	out_RESERVE_CATEGORY AS RESERVE_CTGRY, 
	out_LOSS_CAUSE AS CAUSE_OF_LOSS, 
	out_MEMBER AS PMS_MBR, 
	out_TYPE_EXPOSURE AS PMS_TYPE_EXPOSURE, 
	out_TypeBureauCode AS PMS_TYPE_BUREAU_CODE, 
	out_OFFSET_ONSET_IND AS OFFSET_ONSET_IND, 
	Claimant_Coverage_Eff_Date AS CLAIMANT_COV_EFF_DATE, 
	Claimant_Coverage_Expiration_Date AS CLAIMANT_COV_EXP_DATE, 
	out_RISK_TYPE_IND AS RISK_TYPE_IND, 
	Dummy_String AS S3P_UNIT_DESCRIPT, 
	Dummy_String AS SPEC_PERS_PROP_USE_CODE, 
	Dummy_Integer AS PKG_DED_AMT, 
	Dummy_Integer AS PKG_LMT_AMT, 
	Dummy_String AS MANUAL_ENTRY_IND, 
	Dummy_String AS UNIT_VEH_REGISTRATION_STATE_CODE, 
	Dummy_Integer AS UNIT_VEH_STATED_AMT, 
	Dummy_String AS UNIT_DAM_DESCRIPT, 
	Dummy_Integer AS UNIT_VEH_YR, 
	Dummy_String AS UNIT_VEH_MAKE, 
	Dummy_String AS UNIT_VIN_NUM, 
	LOGICAL_FLAG, 
	CRRNT_SNPSHT_FLAG, 
	Audit_ID AS AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	Source_System_Id AS SOURCE_SYS_ID, 
	CREATED_DATE, 
	created_date AS MODIFIED_DATE, 
	sup_ins_line_id AS SUPINSURANCELINEID, 
	SUP_RISK_UNIT_GRP_ID, 
	SUP_RISK_UNIT_ID, 
	sup_major_peril_id AS SUPMAJORPERILID, 
	CauseOfLossId AS CAUSEOFLOSSID, 
	out_sup_type_bureau_code_id AS SUPTYPEBUREAUCODEID, 
	SUPVEHICLEREGISTRATIONSTATEID, 
	policy_src_id AS POLICYSOURCEID, 
	COVERAGEFORM, 
	RISKTYPE, 
	COVERAGETYPE, 
	COVERAGEVERSION, 
	ANNUALSTATEMENTLINENUMBER, 
	CLASSCODE, 
	SUBLINECODE, 
	RatingCoverageAKID AS RATINGCOVERAGEAKID, 
	o_CoverageGUID AS COVERAGEGUID, 
	o_StatisticalCoverageAKID AS STATISTICALCOVERAGEAKID, 
	o_InsuranceReferenceLineOfBusinessAKId AS INSURANCEREFERENCELINEOFBUSINESSAKID, 
	o_ProductAKId AS PRODUCTAKID, 
	o_TypeOfLoss AS TYPEOFLOSS, 
	o_ClaimTypeCategory AS CLAIMTYPECATEGORY, 
	o_ClaimTypeGroup AS CLAIMTYPEGROUP, 
	o_SubrogationEligibleIndicator AS SUBROGATIONELIGIBLEINDICATOR
	FROM Exp_Determine_AK
),
SQ_claimant_coverage_detail AS (
	SELECT 
	a.claimant_cov_det_id
	, a.claim_party_occurrence_ak_id
	, a.loc_unit_num
	, a.sub_loc_unit_num
	, a.ins_line
	, a.risk_unit_grp
	, a.risk_unit_grp_seq_num
	, a.risk_unit
	, a.risk_unit_seq_num
	, a.major_peril_code
	, a.major_peril_seq
	, a.pms_loss_disability
	, a.reserve_ctgry
	, a.cause_of_loss
	, a.pms_mbr
	, a.pms_type_exposure
	, a.offset_onset_ind
	, a.eff_from_date
	, a.eff_to_date
	, a.source_sys_id 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_detail a
	where a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	and EXISTS (SELECT 1			
		FROM  DBO.claimant_coverage_detail b
		WHERE b.crrnt_snpsht_flag = 1
		    AND a.claimant_cov_det_ak_id = b.claimant_cov_det_ak_id
	      and a.source_sys_id = b.source_sys_id
		GROUP BY b.claimant_cov_det_ak_id
		HAVING COUNT(*) > 1)
	order by a.claimant_cov_det_ak_id, a.eff_from_date desc
	
	--EXISTS Subquery exists to pick AK Groups that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag and all columns of the AK
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
),
EXP_Expire_Rows AS (
	SELECT
	claimant_cov_det_id,
	claim_party_occurrence_ak_id,
	loc_unit_num,
	sub_loc_unit_num,
	ins_line,
	risk_unit_grp,
	risk_unit_grp_seq_num,
	risk_unit,
	risk_unit_seq_num,
	major_peril_code,
	major_peril_seq,
	pms_loss_disability,
	reserve_ctgry AS pms_reserve_ctgry,
	cause_of_loss AS pms_loss_cause,
	pms_mbr,
	pms_type_exposure,
	offset_onset_ind,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	source_sys_id,
	-- *INF*: DECODE (TRUE, 
	-- claim_party_occurrence_ak_id = v_PREV_ROW_claim_party_occurrence_ak_id and
	-- loc_unit_num  = v_PREV_ROW_loc_unit_num and
	-- sub_loc_unit_num = v_PREV_ROW_sub_loc_unit_num and
	-- ins_line = v_PREV_ROW_ins_line and
	-- risk_unit_grp = v_PREV_ROW_risk_unit_grp and
	-- risk_unit_grp_seq_num = v_PREV_ROW_risk_unit_grp_seq_num and
	-- risk_unit = v_PREV_ROW_risk_unit and
	-- risk_unit_seq_num = v_PREV_ROW_risk_unit_seq_num and
	-- major_peril_code= v_PREV_ROW_major_peril_code and
	-- major_peril_seq = v_PREV_ROW_major_peril_seq and
	-- pms_loss_disability = v_PREV_ROW_pms_loss_disability and
	-- pms_reserve_ctgry = v_PREV_ROW_pms_reserve_ctgry and
	-- pms_loss_cause = v_PREV_ROW_pms_loss_cause and
	-- pms_mbr = v_PREV_ROW_pms_mbr and
	-- pms_type_exposure = v_PREV_ROW_type_exposure and
	-- source_sys_id = v_PREV_ROW_source_sys_id
	-- , ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1)
	-- ,orig_eff_to_date)
	DECODE(TRUE,
	claim_party_occurrence_ak_id = v_PREV_ROW_claim_party_occurrence_ak_id AND loc_unit_num = v_PREV_ROW_loc_unit_num AND sub_loc_unit_num = v_PREV_ROW_sub_loc_unit_num AND ins_line = v_PREV_ROW_ins_line AND risk_unit_grp = v_PREV_ROW_risk_unit_grp AND risk_unit_grp_seq_num = v_PREV_ROW_risk_unit_grp_seq_num AND risk_unit = v_PREV_ROW_risk_unit AND risk_unit_seq_num = v_PREV_ROW_risk_unit_seq_num AND major_peril_code = v_PREV_ROW_major_peril_code AND major_peril_seq = v_PREV_ROW_major_peril_seq AND pms_loss_disability = v_PREV_ROW_pms_loss_disability AND pms_reserve_ctgry = v_PREV_ROW_pms_reserve_ctgry AND pms_loss_cause = v_PREV_ROW_pms_loss_cause AND pms_mbr = v_PREV_ROW_pms_mbr AND pms_type_exposure = v_PREV_ROW_type_exposure AND source_sys_id = v_PREV_ROW_source_sys_id, ADD_TO_DATE(v_PREV_ROW_eff_from_date, 'SS', - 1),
	orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	claim_party_occurrence_ak_id AS v_PREV_ROW_claim_party_occurrence_ak_id,
	loc_unit_num AS v_PREV_ROW_loc_unit_num,
	sub_loc_unit_num AS v_PREV_ROW_sub_loc_unit_num,
	ins_line AS v_PREV_ROW_ins_line,
	risk_unit_grp AS v_PREV_ROW_risk_unit_grp,
	risk_unit_grp_seq_num AS v_PREV_ROW_risk_unit_grp_seq_num,
	risk_unit AS v_PREV_ROW_risk_unit,
	risk_unit_seq_num AS v_PREV_ROW_risk_unit_seq_num,
	major_peril_code AS v_PREV_ROW_major_peril_code,
	major_peril_seq AS v_PREV_ROW_major_peril_seq,
	pms_loss_disability AS v_PREV_ROW_pms_loss_disability,
	pms_reserve_ctgry AS v_PREV_ROW_pms_reserve_ctgry,
	pms_loss_cause AS v_PREV_ROW_pms_loss_cause,
	pms_mbr AS v_PREV_ROW_pms_mbr,
	pms_type_exposure AS v_PREV_ROW_type_exposure,
	offset_onset_ind AS v_PREV_ROW_offset_onset_ind,
	source_sys_id AS v_PREV_ROW_source_sys_id,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	0 AS crrnt_snapshot_flag,
	sysdate AS modified_date
	FROM SQ_claimant_coverage_detail
),
FIL_Claimant_Coverage_Detail AS (
	SELECT
	claimant_cov_det_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snapshot_flag, 
	modified_date
	FROM EXP_Expire_Rows
	WHERE orig_eff_to_date != eff_to_date
),
UPD_Update_Target AS (
	SELECT
	claimant_cov_det_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snapshot_flag, 
	modified_date
	FROM FIL_Claimant_Coverage_Detail
),
claimant_coverage_detail_update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_detail AS T
	USING UPD_Update_Target AS S
	ON T.claimant_cov_det_id = S.claimant_cov_det_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snapshot_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),
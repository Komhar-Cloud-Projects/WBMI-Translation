WITH
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
LKP_PolicyCoverage_InsuranceLine AS (
	SELECT
	InsuranceLine,
	PolicyCoverageAKID
	FROM (
		SELECT 
			InsuranceLine,
			PolicyCoverageAKID
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage
		WHERE CurrentSnapshotFlag=1 AND SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyCoverageAKID ORDER BY InsuranceLine) = 1
),
LKP_SupInsuranceReferenceLineOfBusinessRules AS (
	SELECT
	InsuranceReferenceLineOfBusinessCode,
	SequenceNumber,
	Identifiers
	FROM (
		SELECT InsuranceReferenceLineOfBusinessCode as InsuranceReferenceLineOfBusinessCode, 
		SequenceNumber as SequenceNumber,
		(case	when SequenceNumber=1 then PolicySymbol
				when SequenceNumber=2 then PolicySymbol +'&'+ ClassOfBusiness
				when SequenceNumber=28 then PolicySymbol +'&'+ InsuranceLine+'&'+ TypeBureauCode+'&'+ RiskUnitGroup
				when SequenceNumber=3 then PolicySymbol +'&'+ InsuranceLine+'&'+ TypeBureauCode
				when SequenceNumber=4 then PolicySymbol +'&'+ InsuranceLine+'&'+ RiskUnitGroup
				when SequenceNumber=5 then TypeBureauCode+'&'+ MajorPerilCode
				when SequenceNumber=6 then TypeBureauCode
				when SequenceNumber=7 then PolicySymbol +'&'+ TypeBureauCode+'&'+ MajorPerilCode
				when SequenceNumber=8 then PolicySymbol +'&'+ MajorPerilCode
				when SequenceNumber=9 then PolicySymbol +'&'+ TypeBureauCode
				when SequenceNumber=10 then PolicySymbol +'&'+ TypeBureauCode+'&'+ RiskUnitGroup
				when SequenceNumber=11 then PolicySymbol +'&'+ TypeBureauCode+'&'+ ClassCode
				when SequenceNumber=12 then PolicySymbol +'&'+ TypeBureauCode+'&'+ MajorPerilCode+'&'+ RiskUnitGroup
				when SequenceNumber=13 then PolicySymbol +'&'+ InsuranceLine
				when SequenceNumber=14 then PolicySymbol +'&'+ TypeBureauCode
				when SequenceNumber=15 then MajorPerilCode
				when SequenceNumber=16 then PolicySymbol +'&'+ LocationUnitNumber+'&'+ BureauCode2
				when SequenceNumber=17 then PolicySymbol +'&'+ BureauSpecialUseCode
				when SequenceNumber=18 then PolicySymbol +'&'+ TypeBureauCode
				when SequenceNumber=19 then PolicySymbol +'&'+ TypeBureauCode+'&'+ ClassCode
				when SequenceNumber=20 then PolicySymbol +'&'+ ClassCode
				when SequenceNumber=21 then PolicySymbol +'&'+ MajorPerilCode
				when SequenceNumber=22 then PolicySymbol +'&'+ TypeBureauCode+'&'+ MajorPerilCode
				when SequenceNumber=23 then PolicySymbol +'&'+ TypeBureauCode+'&'+ MajorPerilCode
				when SequenceNumber=24 then PolicySymbol +'&'+ TypeBureauCode+'&'+ MajorPerilCode + '&' + BureauCode2
				when SequenceNumber=25 then TypeBureauCode+'&'+ MajorPerilCode + '&' + BureauCode2
				when SequenceNumber=26 then PolicySymbol + '&'+ ClassCode
				when SequenceNumber=27 then PolicySymbol +'&'+ TypeBureauCode+'&'+ MajorPerilCode
		end
		) as Identifiers
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupInsuranceReferenceLineOfBusinessRules
		where CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SequenceNumber,Identifiers ORDER BY InsuranceReferenceLineOfBusinessCode) = 1
),
SQ_pif_4514_stage AS (
	SELECT DISTINCT RTRIM(A.pif_symbol) as pif_symbol,
	       	   A.pif_policy_number,
	                A.pif_module, 
	                LTRIM(RTRIM(sar_insurance_line)) as sar_insurance_line,
	                CASE LEN(sar_location_x) WHEN '0' THEN LTRIM(RTRIM(sar_unit)) ELSE LTRIM(RTRIM(sar_location_x)) END as sar_location_x,
	                LTRIM(RTRIM(sar_sub_location_x)) as sar_sub_location_x,
	                LTRIM(RTRIM(sar_risk_unit_group)) as sar_risk_unit_group,
	                LTRIM(RTRIM(sar_class_code_grp_x + sar_class_code_mem_x)) as sar_class_code_grp_x,
	                LTRIM(RTRIM(sar_unit + sar_risk_unit_continued))      AS sar_unit,
	                CASE LEN(LTRIM(RTRIM(COALESCE(SAR_SEQ_RSK_UNT_A, ''))))
	                  WHEN '0' THEN 'N/A'
	                          ELSE LTRIM(RTRIM(SAR_SEQ_RSK_UNT_A))
	                END                                         AS sar_seq_rsk_unt_a,
	                LTRIM(RTRIM(sar_type_exposure)) as sar_type_exposure,
	                LTRIM(RTRIM(sar_major_peril)) as sar_major_peril,
	                LTRIM(RTRIM(sar_seq_no)) as sar_seq_no,
	                sar_cov_eff_year,
	                sar_cov_eff_month,
	                sar_cov_eff_day,
	                LTRIM(RTRIM(sar_state)) as sar_state,
	                LTRIM(RTRIM(sar_loc_prov_territory)) as sar_loc_prov_territory,
	                CASE WHEN LEN(LTRIM(RTRIM(sar_county_first_two)) + LTRIM(RTRIM(sar_county_last_one)) + LTRIM(RTRIM(sar_city))) < 6 THEN '000000' ELSE 
	       	    LTRIM(RTRIM(sar_county_first_two)) + LTRIM(RTRIM(sar_county_last_one)) + LTRIM(RTRIM(sar_city)) END as sar_city,
	               LTRIM(RTRIM(sar_special_use)) as sar_special_use,
	                LTRIM(RTRIM(sar_section)) as sar_section,
	                LTRIM(RTRIM(sar_type_bureau)) as sar_type_bureau,
	                LTRIM(RTRIM(sar_class_1_4)) + LTRIM(RTRIM(sar_class_5_6))  AS sar_class_code,
	                LTRIM(RTRIM(sar_class_1_4)) as sar_class_1_4,
	                LTRIM(RTRIM(sar_sub_line)) as sar_sub_line,
	                LTRIM(RTRIM(sar_code_2)) as sar_code_2,
	                LTRIM(RTRIM(sar_zip_postal_code)) as sar_zip_postal_code,
	                LTRIM(RTRIM(pif_line_business)) as pif_line_business,
	                LTRIM(RTRIM(wb_class_of_business)) as wb_class_of_business,
	                D.PMDUYC1ClassOfInsured
	FROM  @{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME_4514} A
	Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_02_stage C
	on C.pif_symbol=A.pif_symbol and C.pif_policy_number=A.pif_policy_number and C.pif_module=A.pif_module
	@{pipeline().parameters.JOIN_CONDITION}
	(SELECT DISTINCT Policykey FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.work_policytransactionstatus 
	WHERE AuditID = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AND PolicyStatus <> 'NOCHANGE')  B
	ON  A.policykey = B.policykey
	
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43UYCRStage D
	on PIF43UYCRstageId in (
	select max(PIF43UYCRstageId) from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43UYCRStage D1
	where D1.PifSymbol=D.PifSymbol
	and D1.PifPolicyNumber=D.PifPolicyNumber
	and D1.PifPolicyModule=D.PifPolicyModule
	and D1.PMDUYC1InsuranceLine=D.PMDUYC1InsuranceLine
	and D1.PMDUYC1LocationNumber=D.PMDUYC1LocationNumber
	and D1.PMDUYC1SubLocationNumber=D.PMDUYC1SubLocationNumber
	and D1.PMDUYC1YearItemEffective=D.PMDUYC1YearItemEffective
	and D1.PMDUYC1MonthItemEffective=D.PMDUYC1MonthItemEffective
	and D1.PMDUYC1DayItemEffective=D.PMDUYC1DayItemEffective
	and D1.PMDUYC1Coverage=D.PMDUYC1Coverage
	)
	and D.PifSymbol=A.pif_symbol
	and D.PifPolicyNumber=A.pif_policy_number
	and D.PifPolicyModule=A.pif_module
	and D.PMDUYC1InsuranceLine=A.sar_insurance_line
	and D.PMDUYC1LocationNumber=(case when len(ltrim(rtrim(sar_location_x)))=0 then 0 
	                                  when isnumeric(sar_location_x)=1 then convert(decimal(4,0),sar_location_x) else -1 end)
	and D.PMDUYC1SubLocationNumber=(case when LEN(ltrim(rtrim(sar_sub_location_x)))=0 then 0 
	                                     when isnumeric(sar_sub_location_x)=1 then convert(numeric(3,0),sar_sub_location_x) else -1 end)
	and D.PMDUYC1YearItemEffective=A.sar_cov_eff_year
	and D.PMDUYC1MonthItemEffective=A.sar_cov_eff_month
	and D.PMDUYC1DayItemEffective=A.sar_cov_eff_day
	and D.PMDUYC1Coverage=A.sar_unit
	
	WHERE A.logical_flag IN ('0','1','2','3')  
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Default AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module,
	pif_symbol  ||  pif_policy_number  || pif_module AS Policy_Key,
	sar_insurance_line,
	sar_location_x,
	sar_sub_location_x,
	sar_risk_unit_group,
	sar_class_code_grp_x,
	sar_unit,
	sar_seq_rsk_unt_a,
	sar_type_exposure,
	sar_major_peril,
	sar_seq_no,
	sar_cov_eff_year,
	sar_cov_eff_month,
	sar_cov_eff_day,
	sar_state,
	sar_loc_prov_territory,
	sar_city,
	sar_section,
	sar_type_bureau,
	sar_class_code,
	sar_sub_line,
	sar_zip_postal_code,
	sar_class_1_4,
	sar_code_2,
	sar_special_use,
	pif_line_business,
	wb_class_of_business,
	PMDUYC1ClassOfInsured
	FROM SQ_pif_4514_stage
),
EXP_Values AS (
	SELECT
	pif_symbol,
	Policy_Key,
	-- *INF*: :LKP.LKP_POLICY_POLICYAKID(Policy_Key)
	LKP_POLICY_POLICYAKID_Policy_Key.pol_ak_id AS v_policyAKID,
	sar_location_x,
	-- *INF*: LTRIM(RTRIM(sar_location_x))
	LTRIM(RTRIM(sar_location_x)) AS v_RiskLocation_Unit,
	sar_state,
	-- *INF*: IIF(LTRIM(RTRIM(sar_state))='00', '0', LTRIM(RTRIM(sar_state)))
	IFF(LTRIM(RTRIM(sar_state)) = '00', '0', LTRIM(RTRIM(sar_state))) AS v_sar_state,
	sar_loc_prov_territory,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_loc_prov_territory)
	-- 
	-- --IIF(ISNULL(sar_loc_prov_territory) OR IS_SPACES(sar_loc_prov_territory) OR LENGTH(sar_loc_prov_territory) = 0, 'N/A',
	-- -- LTRIM(RTRIM(sar_loc_prov_territory)))
	-- 
	-- 
	-- 
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_loc_prov_territory) AS v_sar_loc_prov_territory,
	sar_city,
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
	-- ---:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_county_first_two  ||  sar_county_last_one  ||  sar_city)
	-- 
	-- --v_sar_county_first_two  ||  v_sar_county_last_one  ||  v_sar_city
	-- 
	-- --IIF(ISNULL(Tax_Location)  OR IS_SPACES(Tax_Location)  OR LENGTH(Tax_Location) = 0 , '000000', Tax_Location)
	IFF(REG_MATCH(:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_city), '(\d{6})'), :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_city), '000000') AS v_Tax_Location,
	sar_zip_postal_code,
	-- *INF*: IIF(ISNULL(sar_zip_postal_code)  OR IS_SPACES(sar_zip_postal_code)  OR LENGTH(sar_zip_postal_code) = 0 , 'N/A', LTRIM(RTRIM(sar_zip_postal_code)))
	IFF(sar_zip_postal_code IS NULL OR IS_SPACES(sar_zip_postal_code) OR LENGTH(sar_zip_postal_code) = 0, 'N/A', LTRIM(RTRIM(sar_zip_postal_code))) AS v_sar_zip_postal_code,
	-- *INF*: :LKP.LKP_RISKLOCATION_RISKLOCATIONAKID(v_policyAKID, v_RiskLocation_Unit, v_sar_loc_prov_territory, v_sar_state, v_sar_zip_postal_code, v_Tax_Location)
	-- 
	LKP_RISKLOCATION_RISKLOCATIONAKID_v_policyAKID_v_RiskLocation_Unit_v_sar_loc_prov_territory_v_sar_state_v_sar_zip_postal_code_v_Tax_Location.RiskLocationAKID AS v_RiskLocationAKID,
	sar_insurance_line,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_insurance_line)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_insurance_line) AS v_sar_insurance_line,
	sar_type_bureau,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_type_bureau)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_type_bureau) AS v_sar_type_bureau,
	sar_sub_location_x,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_sub_location_x)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_sub_location_x) AS v_sar_sub_location_x,
	v_sar_sub_location_x AS SubLocationUnitNumber,
	sar_risk_unit_group,
	-- *INF*: IIF(REG_MATCH(:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_risk_unit_group) ,'(\d{3})')
	-- ,:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_risk_unit_group)
	-- ,'N/A')
	-- 
	-- ---- Checking the length of the field to 3 and all the positions of the field are any one of 0-9, if it is not then we are defaulting it to 'N/A', by this way we are cleansing junk values from the source.
	-- 
	-- ---:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_risk_unit_group)
	IFF(REG_MATCH(:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_risk_unit_group), '(\d{3})'), :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_risk_unit_group), 'N/A') AS v_sar_risk_unit_group,
	v_sar_risk_unit_group AS RiskUnitGroup,
	sar_class_code_grp_x,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_class_code_grp_x)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_class_code_grp_x) AS v_sar_class_code_grp_x,
	v_sar_class_code_grp_x AS RiskUnitGroupSequenceNumber,
	sar_unit,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_unit)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_unit) AS v_sar_unit,
	v_sar_unit AS RiskUnit,
	sar_seq_rsk_unt_a,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_seq_rsk_unt_a)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_seq_rsk_unt_a) AS v_sar_seq_rsk_unt_a,
	v_sar_seq_rsk_unt_a AS RiskUnitSequenceNumber,
	-- *INF*: DECODE(v_sar_risk_unit_group, '340', SUBSTR(v_sar_seq_rsk_unt_a,2,1), NULL)
	DECODE(v_sar_risk_unit_group,
	'340', SUBSTR(v_sar_seq_rsk_unt_a, 2, 1),
	NULL) AS prdct_type_code_OUT,
	sar_type_exposure,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_type_exposure)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_type_exposure) AS v_sar_type_exposure,
	v_sar_type_exposure AS PMSTypeExposure,
	sar_major_peril,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_major_peril)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_major_peril) AS v_sar_major_peril,
	v_sar_major_peril AS MajorPerilCode,
	sar_seq_no,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_seq_no)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_seq_no) AS v_sar_seq_no,
	v_sar_seq_no AS MajorPerilSequenceNumber,
	sar_cov_eff_year,
	-- *INF*: TO_CHAR(sar_cov_eff_year)
	TO_CHAR(sar_cov_eff_year) AS v_sar_cov_eff_year,
	sar_cov_eff_month,
	-- *INF*: TO_CHAR(sar_cov_eff_month)
	TO_CHAR(sar_cov_eff_month) AS v_sar_cov_eff_month,
	sar_cov_eff_day,
	-- *INF*: TO_CHAR(sar_cov_eff_day)
	TO_CHAR(sar_cov_eff_day) AS v_sar_cov_eff_day,
	-- *INF*: TO_DATE(v_sar_cov_eff_month || '/' || v_sar_cov_eff_day || '/'|| v_sar_cov_eff_year ,'MM/DD/YYYY')
	TO_DATE(v_sar_cov_eff_month || '/' || v_sar_cov_eff_day || '/' || v_sar_cov_eff_year, 'MM/DD/YYYY') AS v_sar_cov_eff_date,
	v_sar_cov_eff_date AS StatisticalCoverageEffectiveDate,
	-- *INF*: MD5(TO_CHAR(v_policyAKID)  || 
	-- TO_CHAR(v_RiskLocationAKID)  || 
	--  v_sar_insurance_line  || 
	--  v_sar_type_bureau
	-- )
	MD5(TO_CHAR(v_policyAKID) || TO_CHAR(v_RiskLocationAKID) || v_sar_insurance_line || v_sar_type_bureau) AS v_PolicyCoverageHashKey,
	-- *INF*: :LKP.LKP_POLICYCOVERAGE_POLICYCOVERAGEAKID(v_PolicyCoverageHashKey)
	-- 
	-- 
	LKP_POLICYCOVERAGE_POLICYCOVERAGEAKID_v_PolicyCoverageHashKey.PolicyCoverageAKID AS v_PolicyCoverageAKID,
	-- *INF*: DECODE(v_PolicyCoverageAKID, NULL, -1, v_PolicyCoverageAKID)
	DECODE(v_PolicyCoverageAKID,
	NULL, - 1,
	v_PolicyCoverageAKID) AS PolicyCoverageAKID_OUT,
	-- *INF*: :LKP.LKP_POLICYCOVERAGE_INSURANCELINE(v_PolicyCoverageAKID)
	LKP_POLICYCOVERAGE_INSURANCELINE_v_PolicyCoverageAKID.InsuranceLine AS ins_line_OUT,
	sar_section,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_section)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_section) AS v_sar_section,
	v_sar_section AS ReinsuranceSectionCode,
	sar_class_code,
	PMDUYC1ClassOfInsured,
	sar_sub_line,
	sar_class_1_4,
	sar_code_2,
	sar_special_use,
	pif_line_business,
	wb_class_of_business,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(wb_class_of_business)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(wb_class_of_business) AS v_wb_class_of_business,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(PMDUYC1ClassOfInsured)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(PMDUYC1ClassOfInsured) AS v_PMDUYC1ClassOfInsured,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_class_code)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_class_code) AS v_sar_class_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_sub_line)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_sub_line) AS v_sar_sub_line,
	-- *INF*: MD5(
	-- TO_CHAR(v_PolicyCoverageAKID)   || 
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
	MD5(TO_CHAR(v_PolicyCoverageAKID) || v_sar_sub_location_x || v_sar_risk_unit_group || v_sar_class_code_grp_x || v_sar_unit || v_sar_seq_rsk_unt_a || v_sar_major_peril || v_sar_seq_no || v_sar_sub_line || v_sar_type_exposure || v_sar_class_code || v_sar_section) AS v_StatisticalCoverageHashKey,
	-- *INF*: SUBSTR(pif_symbol,1,2)
	SUBSTR(pif_symbol, 1, 2) AS v_pif_symbol_first2,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_class_1_4)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_class_1_4) AS v_sar_class_1_4,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_code_2)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_code_2) AS v_sar_code_2,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_special_use)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_special_use) AS v_sar_special_use,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(pif_line_business)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(pif_line_business) AS v_pif_line_business,
	-- *INF*: ---IIF(UPPER(v_pif_symbol_first2)='NF' AND IN(v_wb_class_of_business, 'XN', 'XO', 'XP', 'XQ'), v_PMDUYC1ClassOfInsured, v_sar_class_code)
	-- 
	-- v_sar_class_code
	v_sar_class_code AS o_ClassCode,
	v_sar_sub_line AS o_SublineCode,
	v_StatisticalCoverageHashKey AS o_StatisticalCoverageHashKey,
	-- *INF*: Policy_Key  ||  TO_CHAR(v_PolicyCoverageAKID)
	Policy_Key || TO_CHAR(v_PolicyCoverageAKID) AS o_StatisticalCoverageKey,
	-- *INF*: ---:LKP.LKP_COVERAGEDETAIL_COVERAGEDETAILAKID(v_CoverageDetailHashKey)
	'' AS o_CoverageDetailAKID,
	-- *INF*: DECODE(TRUE,
	-- v_sar_major_peril='032',
	-- '100',
	-- 
	-- (IN(v_pif_symbol_first2,'BD', 'NA', 'NS', 'BC', 'CP', 'BG', 'BH', 'NB', 'CA','XX') AND IN(v_sar_insurance_line,'N/A','CA') AND IN(v_sar_type_bureau, 'AL', 'AP', 'AN'))  
	-- OR (IN(v_pif_symbol_first2, 'BA', 'BB') AND v_sar_insurance_line='GL' AND IN(v_sar_risk_unit_group, '110', '111')),
	-- '200',
	-- 
	-- (IN(v_pif_symbol_first2, 'CP', 'NS') AND v_sar_insurance_line='GL' AND IN(v_sar_major_peril, '599', '919') AND IN(v_sar_risk_unit_group, '345', '367')) 
	-- OR (IN(v_pif_symbol_first2, 'CP', 'NS') AND v_sar_insurance_line='GL' AND IN(v_sar_major_peril, '530', '540', '919', '599') AND v_sar_class_code != '99999' AND  NOT IN(v_sar_risk_unit_group, '345', '346', '355', '900', '901',  '286', '365', '367','000'))
	-- OR (IN(v_pif_symbol_first2, 'GL', 'XX') AND v_pif_line_business != 'SMP' AND IN(v_sar_major_peril, '084', '085'))
	--  OR (pif_symbol = 'DUM'),
	-- '300',
	-- 
	-- v_pif_symbol_first2='NN',
	-- --'310',
	-- '312',
	-- 
	-- v_pif_symbol_first2='NK',
	-- '311',
	-- 
	-- (IN(v_pif_symbol_first2, 'CP', 'NS') AND v_sar_insurance_line='GL' AND IN(v_sar_major_peril, '530') AND v_sar_class_code='99999' AND IN(v_sar_sub_line, '334', '336')),
	-- '320',
	-- 
	-- (IN(v_pif_symbol_first2, 'CP', 'NS') AND v_sar_insurance_line='GL' AND IN(v_sar_major_peril, '599') AND IN(v_sar_sub_line, '334', '336','345','346','347')),
	-- '320',
	-- 
	-- 
	-- 
	-- (v_pif_symbol_first2='CP' AND v_sar_insurance_line='GL' AND v_sar_risk_unit_group='346'),
	-- '321',
	-- 
	-- v_pif_symbol_first2='NE',
	-- '330',
	-- 
	-- (IN(v_pif_symbol_first2, 'CP', 'NS', 'GA') AND v_sar_insurance_line='GA'),
	-- '340',
	-- 
	-- (IN(v_pif_symbol_first2, 'CD', 'CM') AND IN(v_sar_risk_unit_group, '367', '900')),
	-- '310',
	-- 
	-- (v_pif_symbol_first2='CM' AND v_sar_insurance_line='GL' AND IN(v_sar_risk_unit_group, '901', '902', '903')),
	-- '360',
	-- 
	-- (IN(v_pif_symbol_first2, 'CP', 'NS') AND v_sar_insurance_line='GL' AND v_sar_risk_unit_group='345'),
	-- '365',
	-- 
	-- (IN(v_pif_symbol_first2, 'CP', 'NS') AND v_sar_insurance_line='GL' AND (v_sar_risk_unit_group='355' or v_sar_sub_line='332' )),
	-- '370',
	-- 
	-- (v_pif_symbol_first2='CP' AND v_sar_insurance_line='GL' AND v_sar_sub_line='365'),
	-- '380',
	-- 
	-- (IN(v_pif_symbol_first2, 'BA', 'BB', 'XX', 'XA') AND IN(v_pif_line_business, 'BOP', 'BO') AND v_sar_insurance_line != 'CA'),
	-- '400',
	-- 
	-- (IN(v_pif_symbol_first2, 'BC', 'BD') AND IN(v_sar_insurance_line, 'CF', 'GL', 'CR', 'IM', 'CG', 'N/A')),
	-- '410',
	-- 
	-- (IN(v_pif_symbol_first2, 'BG', 'BH', 'GG') AND IN(v_sar_insurance_line, 'CF', 'GL', 'CR', 'IM', 'GA', 'CG', 'N/A')),
	-- '420',
	-- 
	-- (IN(v_pif_symbol_first2, 'NA', 'NB') AND IN(v_sar_insurance_line, 'CF', 'GL', 'CR', 'IM', 'CG')),
	-- '430',
	-- 
	-- (IN(v_pif_symbol_first2, 'SM','XX') AND (v_pif_line_business = 'SMP') AND IN(v_sar_type_bureau, 'AP','BM','BT','CF','FT','GL','GS','IM')),
	-- '440',
	-- 
	-- v_pif_symbol_first2='BO',
	-- '450',
	-- 
	-- IN(v_pif_symbol_first2,'CP','NS') AND v_pif_line_business = 'CPP' AND  v_sar_type_bureau='CR',
	-- '520',
	-- 
	-- (IN(v_pif_symbol_first2, 'CP', 'NS', 'CF', 'PX') AND IN(v_sar_insurance_line, 'CF', 'CR', 'GS', 'BM', 'N/A', 'CG') AND  NOT IN(v_sar_type_bureau, 'AL', 'AP', 'AN', 'GL', 'IM')),
	-- '500',
	-- 
	-- v_pif_symbol_first2='FF',
	-- '510',
	-- 
	-- (IN(v_pif_symbol_first2, 'CP', 'NS') AND v_sar_type_bureau='IM'),
	-- '550',
	-- 
	-- v_pif_symbol_first2='NC',
	-- '610',
	-- 
	-- (v_pif_symbol_first2='NF' AND IN(v_wb_class_of_business, 'XN', 'XO', 'XP', 'XQ','9')),
	-- '620',
	-- 
	-- v_pif_symbol_first2='NJ',
	-- '630',
	-- 
	-- v_pif_symbol_first2='NL',
	-- '640',
	-- 
	-- v_pif_symbol_first2='NM',
	-- '650',
	-- 
	-- v_pif_symbol_first2='NO',
	-- '660',
	-- 
	-- (IN(v_pif_symbol_first2, 'HA', 'HB', 'HH', 'HX', 'IB', 'IP', 'PA', 'PX', 'XX') AND IN(v_sar_type_bureau, 'PH', 'PI', 'PL', 'PQ', 'MS')),
	-- '800',
	-- 
	-- (IN(v_pif_symbol_first2, 'FL', 'FP') AND IN(v_sar_type_bureau, 'PF', 'PQ', 'MS')) 
	--  OR (v_pif_symbol_first2='HH' AND v_sar_type_bureau='PF'),
	-- '820',
	-- 
	-- (IN(v_pif_symbol_first2, 'HH', 'PA', 'PM', 'PP', 'PS', 'PT', 'HA', 'XX', 'XA') AND IN(v_sar_type_bureau, 'RL', 'RP', 'RN')),
	-- '850',
	-- 
	-- (IN(v_pif_symbol_first2, 'HH', 'UP', 'HX', 'XX') AND v_sar_type_bureau='GL' AND v_sar_major_peril='017'),
	-- '890',
	-- 
	-- (IN(v_pif_symbol_first2, 'NU', 'CU', 'CP', 'UC') AND v_sar_type_bureau='GL' AND v_sar_major_peril='517'),
	-- '900',
	-- 
	-- '000'
	-- )
	DECODE(TRUE,
	v_sar_major_peril = '032', '100',
	( IN(v_pif_symbol_first2, 'BD', 'NA', 'NS', 'BC', 'CP', 'BG', 'BH', 'NB', 'CA', 'XX') AND IN(v_sar_insurance_line, 'N/A', 'CA') AND IN(v_sar_type_bureau, 'AL', 'AP', 'AN') ) OR ( IN(v_pif_symbol_first2, 'BA', 'BB') AND v_sar_insurance_line = 'GL' AND IN(v_sar_risk_unit_group, '110', '111') ), '200',
	( IN(v_pif_symbol_first2, 'CP', 'NS') AND v_sar_insurance_line = 'GL' AND IN(v_sar_major_peril, '599', '919') AND IN(v_sar_risk_unit_group, '345', '367') ) OR ( IN(v_pif_symbol_first2, 'CP', 'NS') AND v_sar_insurance_line = 'GL' AND IN(v_sar_major_peril, '530', '540', '919', '599') AND v_sar_class_code != '99999' AND NOT IN(v_sar_risk_unit_group, '345', '346', '355', '900', '901', '286', '365', '367', '000') ) OR ( IN(v_pif_symbol_first2, 'GL', 'XX') AND v_pif_line_business != 'SMP' AND IN(v_sar_major_peril, '084', '085') ) OR ( pif_symbol = 'DUM' ), '300',
	v_pif_symbol_first2 = 'NN', '312',
	v_pif_symbol_first2 = 'NK', '311',
	( IN(v_pif_symbol_first2, 'CP', 'NS') AND v_sar_insurance_line = 'GL' AND IN(v_sar_major_peril, '530') AND v_sar_class_code = '99999' AND IN(v_sar_sub_line, '334', '336') ), '320',
	( IN(v_pif_symbol_first2, 'CP', 'NS') AND v_sar_insurance_line = 'GL' AND IN(v_sar_major_peril, '599') AND IN(v_sar_sub_line, '334', '336', '345', '346', '347') ), '320',
	( v_pif_symbol_first2 = 'CP' AND v_sar_insurance_line = 'GL' AND v_sar_risk_unit_group = '346' ), '321',
	v_pif_symbol_first2 = 'NE', '330',
	( IN(v_pif_symbol_first2, 'CP', 'NS', 'GA') AND v_sar_insurance_line = 'GA' ), '340',
	( IN(v_pif_symbol_first2, 'CD', 'CM') AND IN(v_sar_risk_unit_group, '367', '900') ), '310',
	( v_pif_symbol_first2 = 'CM' AND v_sar_insurance_line = 'GL' AND IN(v_sar_risk_unit_group, '901', '902', '903') ), '360',
	( IN(v_pif_symbol_first2, 'CP', 'NS') AND v_sar_insurance_line = 'GL' AND v_sar_risk_unit_group = '345' ), '365',
	( IN(v_pif_symbol_first2, 'CP', 'NS') AND v_sar_insurance_line = 'GL' AND ( v_sar_risk_unit_group = '355' OR v_sar_sub_line = '332' ) ), '370',
	( v_pif_symbol_first2 = 'CP' AND v_sar_insurance_line = 'GL' AND v_sar_sub_line = '365' ), '380',
	( IN(v_pif_symbol_first2, 'BA', 'BB', 'XX', 'XA') AND IN(v_pif_line_business, 'BOP', 'BO') AND v_sar_insurance_line != 'CA' ), '400',
	( IN(v_pif_symbol_first2, 'BC', 'BD') AND IN(v_sar_insurance_line, 'CF', 'GL', 'CR', 'IM', 'CG', 'N/A') ), '410',
	( IN(v_pif_symbol_first2, 'BG', 'BH', 'GG') AND IN(v_sar_insurance_line, 'CF', 'GL', 'CR', 'IM', 'GA', 'CG', 'N/A') ), '420',
	( IN(v_pif_symbol_first2, 'NA', 'NB') AND IN(v_sar_insurance_line, 'CF', 'GL', 'CR', 'IM', 'CG') ), '430',
	( IN(v_pif_symbol_first2, 'SM', 'XX') AND ( v_pif_line_business = 'SMP' ) AND IN(v_sar_type_bureau, 'AP', 'BM', 'BT', 'CF', 'FT', 'GL', 'GS', 'IM') ), '440',
	v_pif_symbol_first2 = 'BO', '450',
	IN(v_pif_symbol_first2, 'CP', 'NS') AND v_pif_line_business = 'CPP' AND v_sar_type_bureau = 'CR', '520',
	( IN(v_pif_symbol_first2, 'CP', 'NS', 'CF', 'PX') AND IN(v_sar_insurance_line, 'CF', 'CR', 'GS', 'BM', 'N/A', 'CG') AND NOT IN(v_sar_type_bureau, 'AL', 'AP', 'AN', 'GL', 'IM') ), '500',
	v_pif_symbol_first2 = 'FF', '510',
	( IN(v_pif_symbol_first2, 'CP', 'NS') AND v_sar_type_bureau = 'IM' ), '550',
	v_pif_symbol_first2 = 'NC', '610',
	( v_pif_symbol_first2 = 'NF' AND IN(v_wb_class_of_business, 'XN', 'XO', 'XP', 'XQ', '9') ), '620',
	v_pif_symbol_first2 = 'NJ', '630',
	v_pif_symbol_first2 = 'NL', '640',
	v_pif_symbol_first2 = 'NM', '650',
	v_pif_symbol_first2 = 'NO', '660',
	( IN(v_pif_symbol_first2, 'HA', 'HB', 'HH', 'HX', 'IB', 'IP', 'PA', 'PX', 'XX') AND IN(v_sar_type_bureau, 'PH', 'PI', 'PL', 'PQ', 'MS') ), '800',
	( IN(v_pif_symbol_first2, 'FL', 'FP') AND IN(v_sar_type_bureau, 'PF', 'PQ', 'MS') ) OR ( v_pif_symbol_first2 = 'HH' AND v_sar_type_bureau = 'PF' ), '820',
	( IN(v_pif_symbol_first2, 'HH', 'PA', 'PM', 'PP', 'PS', 'PT', 'HA', 'XX', 'XA') AND IN(v_sar_type_bureau, 'RL', 'RP', 'RN') ), '850',
	( IN(v_pif_symbol_first2, 'HH', 'UP', 'HX', 'XX') AND v_sar_type_bureau = 'GL' AND v_sar_major_peril = '017' ), '890',
	( IN(v_pif_symbol_first2, 'NU', 'CU', 'CP', 'UC') AND v_sar_type_bureau = 'GL' AND v_sar_major_peril = '517' ), '900',
	'000') AS o_SourceProductCode,
	SYSDATE AS o_CurrentDate,
	-- *INF*: :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(1,v_pif_symbol_first2)
	LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_1_v_pif_symbol_first2.InsuranceReferenceLineOfBusinessCode AS v_Rule1,
	-- *INF*: IIF( NOT ISNULL(v_Rule1), v_Rule1, :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(2,v_pif_symbol_first2 || '&' || v_wb_class_of_business))
	IFF(NOT v_Rule1 IS NULL, v_Rule1, LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_2_v_pif_symbol_first2_v_wb_class_of_business.InsuranceReferenceLineOfBusinessCode) AS v_Rule2,
	-- *INF*: IIF( NOT ISNULL(v_Rule2), v_Rule2, :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(28,v_pif_symbol_first2 || '&' || v_sar_insurance_line || '&' || v_sar_type_bureau || '&' || v_sar_risk_unit_group))
	IFF(NOT v_Rule2 IS NULL, v_Rule2, LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_28_v_pif_symbol_first2_v_sar_insurance_line_v_sar_type_bureau_v_sar_risk_unit_group.InsuranceReferenceLineOfBusinessCode) AS v_Rule28,
	-- *INF*: IIF( NOT ISNULL(v_Rule28), v_Rule28, :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(3,v_pif_symbol_first2 || '&' || v_sar_insurance_line || '&' || v_sar_type_bureau))
	IFF(NOT v_Rule28 IS NULL, v_Rule28, LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_3_v_pif_symbol_first2_v_sar_insurance_line_v_sar_type_bureau.InsuranceReferenceLineOfBusinessCode) AS v_Rule3,
	-- *INF*: IIF( NOT ISNULL(v_Rule3), v_Rule3, :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(4,v_pif_symbol_first2 || '&' || v_sar_insurance_line || '&' || v_sar_risk_unit_group))
	IFF(NOT v_Rule3 IS NULL, v_Rule3, LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_4_v_pif_symbol_first2_v_sar_insurance_line_v_sar_risk_unit_group.InsuranceReferenceLineOfBusinessCode) AS v_Rule4,
	-- *INF*: IIF( NOT ISNULL(v_Rule4), v_Rule4, :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(5,v_sar_type_bureau || '&' || v_sar_major_peril))
	IFF(NOT v_Rule4 IS NULL, v_Rule4, LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_5_v_sar_type_bureau_v_sar_major_peril.InsuranceReferenceLineOfBusinessCode) AS v_Rule5,
	-- *INF*: IIF( NOT ISNULL(v_Rule5), v_Rule5, :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(6,v_sar_type_bureau))
	IFF(NOT v_Rule5 IS NULL, v_Rule5, LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_6_v_sar_type_bureau.InsuranceReferenceLineOfBusinessCode) AS v_Rule6,
	-- *INF*: IIF( NOT ISNULL(v_Rule6), v_Rule6, :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(7,v_pif_symbol_first2 || '&' || v_sar_type_bureau || '&' || v_sar_major_peril))
	IFF(NOT v_Rule6 IS NULL, v_Rule6, LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_7_v_pif_symbol_first2_v_sar_type_bureau_v_sar_major_peril.InsuranceReferenceLineOfBusinessCode) AS v_Rule7,
	-- *INF*: IIF( NOT ISNULL(v_Rule7), v_Rule7, :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(8,v_pif_symbol_first2 || '&' || v_sar_major_peril))
	IFF(NOT v_Rule7 IS NULL, v_Rule7, LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_8_v_pif_symbol_first2_v_sar_major_peril.InsuranceReferenceLineOfBusinessCode) AS v_Rule8,
	-- *INF*: IIF( NOT ISNULL(v_Rule8), v_Rule8, :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(9,v_pif_symbol_first2 || '&' || v_sar_type_bureau))
	IFF(NOT v_Rule8 IS NULL, v_Rule8, LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_9_v_pif_symbol_first2_v_sar_type_bureau.InsuranceReferenceLineOfBusinessCode) AS v_Rule9,
	-- *INF*: IIF( NOT ISNULL(v_Rule9), v_Rule9, :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(10,v_pif_symbol_first2 || '&' || v_sar_type_bureau || '&' || v_sar_risk_unit_group))
	IFF(NOT v_Rule9 IS NULL, v_Rule9, LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_10_v_pif_symbol_first2_v_sar_type_bureau_v_sar_risk_unit_group.InsuranceReferenceLineOfBusinessCode) AS v_Rule10,
	-- *INF*: IIF( NOT ISNULL(v_Rule10), v_Rule10, :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(11,v_pif_symbol_first2 || '&' || v_sar_type_bureau || '&'  || v_sar_class_code))
	IFF(NOT v_Rule10 IS NULL, v_Rule10, LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_11_v_pif_symbol_first2_v_sar_type_bureau_v_sar_class_code.InsuranceReferenceLineOfBusinessCode) AS v_Rule11,
	-- *INF*: IIF( NOT ISNULL(v_Rule11), v_Rule11, :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(12,v_pif_symbol_first2 || '&' || v_sar_type_bureau || '&' || v_sar_major_peril || '&' || v_sar_risk_unit_group))
	IFF(NOT v_Rule11 IS NULL, v_Rule11, LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_12_v_pif_symbol_first2_v_sar_type_bureau_v_sar_major_peril_v_sar_risk_unit_group.InsuranceReferenceLineOfBusinessCode) AS v_Rule12,
	-- *INF*: IIF( NOT ISNULL(v_Rule12), v_Rule12, :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(13,v_pif_symbol_first2 || '&' || v_sar_insurance_line))
	IFF(NOT v_Rule12 IS NULL, v_Rule12, LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_13_v_pif_symbol_first2_v_sar_insurance_line.InsuranceReferenceLineOfBusinessCode) AS v_Rule13,
	-- *INF*: DECODE(TRUE,
	--  NOT ISNULL(v_Rule13), 
	-- v_Rule13, 
	-- v_sar_major_peril != '517' AND  NOT IN(SUBSTR(v_sar_class_code,1,5),'22222','22250'), :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(14,v_pif_symbol_first2 || '&' || v_sar_type_bureau), 
	-- NULL)
	DECODE(TRUE,
	NOT v_Rule13 IS NULL, v_Rule13,
	v_sar_major_peril != '517' AND NOT IN(SUBSTR(v_sar_class_code, 1, 5), '22222', '22250'), LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_14_v_pif_symbol_first2_v_sar_type_bureau.InsuranceReferenceLineOfBusinessCode,
	NULL) AS v_Rule14,
	-- *INF*: IIF( NOT ISNULL(v_Rule14), v_Rule14, :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(15,v_sar_major_peril))
	IFF(NOT v_Rule14 IS NULL, v_Rule14, LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_15_v_sar_major_peril.InsuranceReferenceLineOfBusinessCode) AS v_Rule15,
	-- *INF*: IIF( NOT ISNULL(v_Rule15), v_Rule15, :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(16,v_pif_symbol_first2 || '&' || SUBSTR(v_sar_unit,1,1) || '&' || v_sar_code_2))
	IFF(NOT v_Rule15 IS NULL, v_Rule15, LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_16_v_pif_symbol_first2_SUBSTR_v_sar_unit_1_1_v_sar_code_2.InsuranceReferenceLineOfBusinessCode) AS v_Rule16,
	-- *INF*: IIF( NOT ISNULL(v_Rule16), v_Rule16, :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(17,v_pif_symbol_first2 || '&' || SUBSTR(v_sar_special_use,1,4)))
	IFF(NOT v_Rule16 IS NULL, v_Rule16, LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_17_v_pif_symbol_first2_SUBSTR_v_sar_special_use_1_4.InsuranceReferenceLineOfBusinessCode) AS v_Rule17,
	-- *INF*: IIF( NOT ISNULL(v_Rule17), v_Rule17, :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(18,v_pif_symbol_first2 || '&' || v_sar_type_bureau))
	IFF(NOT v_Rule17 IS NULL, v_Rule17, LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_18_v_pif_symbol_first2_v_sar_type_bureau.InsuranceReferenceLineOfBusinessCode) AS v_Rule18,
	-- *INF*: IIF( NOT ISNULL(v_Rule18), v_Rule18, :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(19,v_pif_symbol_first2 || '&' || v_sar_type_bureau || '&' || SUBSTR(v_sar_class_code,1,1)))
	IFF(NOT v_Rule18 IS NULL, v_Rule18, LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_19_v_pif_symbol_first2_v_sar_type_bureau_SUBSTR_v_sar_class_code_1_1.InsuranceReferenceLineOfBusinessCode) AS v_Rule19,
	-- *INF*: IIF( NOT ISNULL(v_Rule19), v_Rule19, :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(20,v_pif_symbol_first2 || '&' || v_sar_class_1_4))
	IFF(NOT v_Rule19 IS NULL, v_Rule19, LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_20_v_pif_symbol_first2_v_sar_class_1_4.InsuranceReferenceLineOfBusinessCode) AS v_Rule20,
	-- *INF*: IIF( NOT ISNULL(v_Rule20), v_Rule20, :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(22, v_pif_symbol_first2 || '&' || v_sar_type_bureau || '&' || v_sar_major_peril ))
	IFF(NOT v_Rule20 IS NULL, v_Rule20, LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_22_v_pif_symbol_first2_v_sar_type_bureau_v_sar_major_peril.InsuranceReferenceLineOfBusinessCode) AS v_Rule22,
	-- *INF*: IIF( NOT ISNULL(v_Rule22), v_Rule22,  :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(23,v_pif_symbol_first2 || '&' ||  v_sar_type_bureau || '&' || v_sar_major_peril))
	IFF(NOT v_Rule22 IS NULL, v_Rule22, LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_23_v_pif_symbol_first2_v_sar_type_bureau_v_sar_major_peril.InsuranceReferenceLineOfBusinessCode) AS v_Rule23,
	-- *INF*: IIF( NOT ISNULL(v_Rule23), v_Rule23,  :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(24,v_pif_symbol_first2 || '&' ||  v_sar_type_bureau || '&' || v_sar_major_peril || '&' || v_sar_code_2))
	IFF(NOT v_Rule23 IS NULL, v_Rule23, LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_24_v_pif_symbol_first2_v_sar_type_bureau_v_sar_major_peril_v_sar_code_2.InsuranceReferenceLineOfBusinessCode) AS v_Rule24,
	-- *INF*: IIF( NOT ISNULL(v_Rule24), v_Rule24,  :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(25,v_sar_type_bureau || '&' ||  v_sar_major_peril || '&' || v_sar_code_2))
	IFF(NOT v_Rule24 IS NULL, v_Rule24, LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_25_v_sar_type_bureau_v_sar_major_peril_v_sar_code_2.InsuranceReferenceLineOfBusinessCode) AS v_Rule25,
	-- *INF*: IIF( NOT ISNULL(v_Rule25), v_Rule25,  :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(26,v_pif_symbol_first2  ||  '&'  ||  v_sar_class_code))
	IFF(NOT v_Rule25 IS NULL, v_Rule25, LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_26_v_pif_symbol_first2_v_sar_class_code.InsuranceReferenceLineOfBusinessCode) AS v_Rule26,
	-- *INF*: DECODE(TRUE, NOT ISNULL(v_Rule26), v_Rule26, 
	-- NOT IN(v_sar_class_code,'923500' , '962000','990000', '943700','944200'),
	--  :LKP.LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES(27,v_pif_symbol_first2 || '&' ||  v_sar_type_bureau || '&' || v_sar_major_peril), 
	-- NULL)
	DECODE(TRUE,
	NOT v_Rule26 IS NULL, v_Rule26,
	NOT IN(v_sar_class_code, '923500', '962000', '990000', '943700', '944200'), LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_27_v_pif_symbol_first2_v_sar_type_bureau_v_sar_major_peril.InsuranceReferenceLineOfBusinessCode,
	NULL) AS v_Rule27,
	-- *INF*: DECODE(TRUE,
	--  NOT ISNULL(v_Rule27),v_Rule27,
	-- '000'
	-- )
	DECODE(TRUE,
	NOT v_Rule27 IS NULL, v_Rule27,
	'000') AS o_InsuranceReferenceLineOfBusinessCode,
	-- *INF*: IIF(v_sar_insurance_line='CF' AND v_sar_class_1_4 != 'N/A' AND SUBSTR(v_sar_class_1_4,1,2)<'35' AND (REG_MATCH(SUBSTR(v_sar_class_1_4,3,1),'[a-zA-Z]')=1 OR LENGTH(v_sar_class_1_4)=2), SUBSTR(v_sar_class_1_4,1,2), 'N/A')
	IFF(v_sar_insurance_line = 'CF' AND v_sar_class_1_4 != 'N/A' AND SUBSTR(v_sar_class_1_4, 1, 2) < '35' AND ( REG_MATCH(SUBSTR(v_sar_class_1_4, 3, 1), '[a-zA-Z]') = 1 OR LENGTH(v_sar_class_1_4) = 2 ), SUBSTR(v_sar_class_1_4, 1, 2), 'N/A') AS o_SpecialClassLevel1,
	v_StatisticalCoverageHashKey AS o_CoverageGuid,
	-- *INF*: DECODE(TRUE,
	-- UPPER(v_sar_insurance_line)='WC', 
	-- 'NCCI', 
	-- IN(UPPER(v_pif_symbol_first2), 'NC', 'NJ', 'NL', 'NM', 'NO'),
	-- 'SFAA',
	-- UPPER( v_pif_symbol_first2)= 'NF' AND IN (UPPER(v_wb_class_of_business), 'XN', 'XO', 'XP', 'XQ'),
	-- 'SFAA',
	-- 'ISO')
	DECODE(TRUE,
	UPPER(v_sar_insurance_line) = 'WC', 'NCCI',
	IN(UPPER(v_pif_symbol_first2), 'NC', 'NJ', 'NL', 'NM', 'NO'), 'SFAA',
	UPPER(v_pif_symbol_first2) = 'NF' AND IN(UPPER(v_wb_class_of_business), 'XN', 'XO', 'XP', 'XQ'), 'SFAA',
	'ISO') AS o_ClassCodeOrganizationCode
	FROM EXP_Default
	LEFT JOIN LKP_POLICY_POLICYAKID LKP_POLICY_POLICYAKID_Policy_Key
	ON LKP_POLICY_POLICYAKID_Policy_Key.pol_key = Policy_Key

	LEFT JOIN LKP_RISKLOCATION_RISKLOCATIONAKID LKP_RISKLOCATION_RISKLOCATIONAKID_v_policyAKID_v_RiskLocation_Unit_v_sar_loc_prov_territory_v_sar_state_v_sar_zip_postal_code_v_Tax_Location
	ON LKP_RISKLOCATION_RISKLOCATIONAKID_v_policyAKID_v_RiskLocation_Unit_v_sar_loc_prov_territory_v_sar_state_v_sar_zip_postal_code_v_Tax_Location.PolicyAKID = v_policyAKID
	AND LKP_RISKLOCATION_RISKLOCATIONAKID_v_policyAKID_v_RiskLocation_Unit_v_sar_loc_prov_territory_v_sar_state_v_sar_zip_postal_code_v_Tax_Location.LocationUnitNumber = v_RiskLocation_Unit
	AND LKP_RISKLOCATION_RISKLOCATIONAKID_v_policyAKID_v_RiskLocation_Unit_v_sar_loc_prov_territory_v_sar_state_v_sar_zip_postal_code_v_Tax_Location.RiskTerritory = v_sar_loc_prov_territory
	AND LKP_RISKLOCATION_RISKLOCATIONAKID_v_policyAKID_v_RiskLocation_Unit_v_sar_loc_prov_territory_v_sar_state_v_sar_zip_postal_code_v_Tax_Location.StateProvinceCode = v_sar_state
	AND LKP_RISKLOCATION_RISKLOCATIONAKID_v_policyAKID_v_RiskLocation_Unit_v_sar_loc_prov_territory_v_sar_state_v_sar_zip_postal_code_v_Tax_Location.ZipPostalCode = v_sar_zip_postal_code
	AND LKP_RISKLOCATION_RISKLOCATIONAKID_v_policyAKID_v_RiskLocation_Unit_v_sar_loc_prov_territory_v_sar_state_v_sar_zip_postal_code_v_Tax_Location.TaxLocation = v_Tax_Location

	LEFT JOIN LKP_POLICYCOVERAGE_POLICYCOVERAGEAKID LKP_POLICYCOVERAGE_POLICYCOVERAGEAKID_v_PolicyCoverageHashKey
	ON LKP_POLICYCOVERAGE_POLICYCOVERAGEAKID_v_PolicyCoverageHashKey.PolicyCoverageHashKey = v_PolicyCoverageHashKey

	LEFT JOIN LKP_POLICYCOVERAGE_INSURANCELINE LKP_POLICYCOVERAGE_INSURANCELINE_v_PolicyCoverageAKID
	ON LKP_POLICYCOVERAGE_INSURANCELINE_v_PolicyCoverageAKID.PolicyCoverageAKID = v_PolicyCoverageAKID

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_1_v_pif_symbol_first2
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_1_v_pif_symbol_first2.SequenceNumber = 1
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_1_v_pif_symbol_first2.Identifiers = v_pif_symbol_first2

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_2_v_pif_symbol_first2_v_wb_class_of_business
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_2_v_pif_symbol_first2_v_wb_class_of_business.SequenceNumber = 2
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_2_v_pif_symbol_first2_v_wb_class_of_business.Identifiers = v_pif_symbol_first2 || '&' || v_wb_class_of_business

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_28_v_pif_symbol_first2_v_sar_insurance_line_v_sar_type_bureau_v_sar_risk_unit_group
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_28_v_pif_symbol_first2_v_sar_insurance_line_v_sar_type_bureau_v_sar_risk_unit_group.SequenceNumber = 28
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_28_v_pif_symbol_first2_v_sar_insurance_line_v_sar_type_bureau_v_sar_risk_unit_group.Identifiers = v_pif_symbol_first2 || '&' || v_sar_insurance_line || '&' || v_sar_type_bureau || '&' || v_sar_risk_unit_group

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_3_v_pif_symbol_first2_v_sar_insurance_line_v_sar_type_bureau
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_3_v_pif_symbol_first2_v_sar_insurance_line_v_sar_type_bureau.SequenceNumber = 3
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_3_v_pif_symbol_first2_v_sar_insurance_line_v_sar_type_bureau.Identifiers = v_pif_symbol_first2 || '&' || v_sar_insurance_line || '&' || v_sar_type_bureau

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_4_v_pif_symbol_first2_v_sar_insurance_line_v_sar_risk_unit_group
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_4_v_pif_symbol_first2_v_sar_insurance_line_v_sar_risk_unit_group.SequenceNumber = 4
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_4_v_pif_symbol_first2_v_sar_insurance_line_v_sar_risk_unit_group.Identifiers = v_pif_symbol_first2 || '&' || v_sar_insurance_line || '&' || v_sar_risk_unit_group

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_5_v_sar_type_bureau_v_sar_major_peril
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_5_v_sar_type_bureau_v_sar_major_peril.SequenceNumber = 5
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_5_v_sar_type_bureau_v_sar_major_peril.Identifiers = v_sar_type_bureau || '&' || v_sar_major_peril

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_6_v_sar_type_bureau
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_6_v_sar_type_bureau.SequenceNumber = 6
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_6_v_sar_type_bureau.Identifiers = v_sar_type_bureau

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_7_v_pif_symbol_first2_v_sar_type_bureau_v_sar_major_peril
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_7_v_pif_symbol_first2_v_sar_type_bureau_v_sar_major_peril.SequenceNumber = 7
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_7_v_pif_symbol_first2_v_sar_type_bureau_v_sar_major_peril.Identifiers = v_pif_symbol_first2 || '&' || v_sar_type_bureau || '&' || v_sar_major_peril

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_8_v_pif_symbol_first2_v_sar_major_peril
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_8_v_pif_symbol_first2_v_sar_major_peril.SequenceNumber = 8
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_8_v_pif_symbol_first2_v_sar_major_peril.Identifiers = v_pif_symbol_first2 || '&' || v_sar_major_peril

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_9_v_pif_symbol_first2_v_sar_type_bureau
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_9_v_pif_symbol_first2_v_sar_type_bureau.SequenceNumber = 9
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_9_v_pif_symbol_first2_v_sar_type_bureau.Identifiers = v_pif_symbol_first2 || '&' || v_sar_type_bureau

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_10_v_pif_symbol_first2_v_sar_type_bureau_v_sar_risk_unit_group
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_10_v_pif_symbol_first2_v_sar_type_bureau_v_sar_risk_unit_group.SequenceNumber = 10
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_10_v_pif_symbol_first2_v_sar_type_bureau_v_sar_risk_unit_group.Identifiers = v_pif_symbol_first2 || '&' || v_sar_type_bureau || '&' || v_sar_risk_unit_group

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_11_v_pif_symbol_first2_v_sar_type_bureau_v_sar_class_code
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_11_v_pif_symbol_first2_v_sar_type_bureau_v_sar_class_code.SequenceNumber = 11
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_11_v_pif_symbol_first2_v_sar_type_bureau_v_sar_class_code.Identifiers = v_pif_symbol_first2 || '&' || v_sar_type_bureau || '&' || v_sar_class_code

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_12_v_pif_symbol_first2_v_sar_type_bureau_v_sar_major_peril_v_sar_risk_unit_group
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_12_v_pif_symbol_first2_v_sar_type_bureau_v_sar_major_peril_v_sar_risk_unit_group.SequenceNumber = 12
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_12_v_pif_symbol_first2_v_sar_type_bureau_v_sar_major_peril_v_sar_risk_unit_group.Identifiers = v_pif_symbol_first2 || '&' || v_sar_type_bureau || '&' || v_sar_major_peril || '&' || v_sar_risk_unit_group

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_13_v_pif_symbol_first2_v_sar_insurance_line
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_13_v_pif_symbol_first2_v_sar_insurance_line.SequenceNumber = 13
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_13_v_pif_symbol_first2_v_sar_insurance_line.Identifiers = v_pif_symbol_first2 || '&' || v_sar_insurance_line

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_14_v_pif_symbol_first2_v_sar_type_bureau
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_14_v_pif_symbol_first2_v_sar_type_bureau.SequenceNumber = 14
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_14_v_pif_symbol_first2_v_sar_type_bureau.Identifiers = v_pif_symbol_first2 || '&' || v_sar_type_bureau

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_15_v_sar_major_peril
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_15_v_sar_major_peril.SequenceNumber = 15
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_15_v_sar_major_peril.Identifiers = v_sar_major_peril

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_16_v_pif_symbol_first2_SUBSTR_v_sar_unit_1_1_v_sar_code_2
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_16_v_pif_symbol_first2_SUBSTR_v_sar_unit_1_1_v_sar_code_2.SequenceNumber = 16
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_16_v_pif_symbol_first2_SUBSTR_v_sar_unit_1_1_v_sar_code_2.Identifiers = v_pif_symbol_first2 || '&' || SUBSTR(v_sar_unit, 1, 1) || '&' || v_sar_code_2

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_17_v_pif_symbol_first2_SUBSTR_v_sar_special_use_1_4
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_17_v_pif_symbol_first2_SUBSTR_v_sar_special_use_1_4.SequenceNumber = 17
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_17_v_pif_symbol_first2_SUBSTR_v_sar_special_use_1_4.Identifiers = v_pif_symbol_first2 || '&' || SUBSTR(v_sar_special_use, 1, 4)

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_18_v_pif_symbol_first2_v_sar_type_bureau
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_18_v_pif_symbol_first2_v_sar_type_bureau.SequenceNumber = 18
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_18_v_pif_symbol_first2_v_sar_type_bureau.Identifiers = v_pif_symbol_first2 || '&' || v_sar_type_bureau

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_19_v_pif_symbol_first2_v_sar_type_bureau_SUBSTR_v_sar_class_code_1_1
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_19_v_pif_symbol_first2_v_sar_type_bureau_SUBSTR_v_sar_class_code_1_1.SequenceNumber = 19
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_19_v_pif_symbol_first2_v_sar_type_bureau_SUBSTR_v_sar_class_code_1_1.Identifiers = v_pif_symbol_first2 || '&' || v_sar_type_bureau || '&' || SUBSTR(v_sar_class_code, 1, 1)

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_20_v_pif_symbol_first2_v_sar_class_1_4
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_20_v_pif_symbol_first2_v_sar_class_1_4.SequenceNumber = 20
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_20_v_pif_symbol_first2_v_sar_class_1_4.Identifiers = v_pif_symbol_first2 || '&' || v_sar_class_1_4

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_22_v_pif_symbol_first2_v_sar_type_bureau_v_sar_major_peril
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_22_v_pif_symbol_first2_v_sar_type_bureau_v_sar_major_peril.SequenceNumber = 22
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_22_v_pif_symbol_first2_v_sar_type_bureau_v_sar_major_peril.Identifiers = v_pif_symbol_first2 || '&' || v_sar_type_bureau || '&' || v_sar_major_peril

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_23_v_pif_symbol_first2_v_sar_type_bureau_v_sar_major_peril
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_23_v_pif_symbol_first2_v_sar_type_bureau_v_sar_major_peril.SequenceNumber = 23
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_23_v_pif_symbol_first2_v_sar_type_bureau_v_sar_major_peril.Identifiers = v_pif_symbol_first2 || '&' || v_sar_type_bureau || '&' || v_sar_major_peril

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_24_v_pif_symbol_first2_v_sar_type_bureau_v_sar_major_peril_v_sar_code_2
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_24_v_pif_symbol_first2_v_sar_type_bureau_v_sar_major_peril_v_sar_code_2.SequenceNumber = 24
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_24_v_pif_symbol_first2_v_sar_type_bureau_v_sar_major_peril_v_sar_code_2.Identifiers = v_pif_symbol_first2 || '&' || v_sar_type_bureau || '&' || v_sar_major_peril || '&' || v_sar_code_2

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_25_v_sar_type_bureau_v_sar_major_peril_v_sar_code_2
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_25_v_sar_type_bureau_v_sar_major_peril_v_sar_code_2.SequenceNumber = 25
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_25_v_sar_type_bureau_v_sar_major_peril_v_sar_code_2.Identifiers = v_sar_type_bureau || '&' || v_sar_major_peril || '&' || v_sar_code_2

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_26_v_pif_symbol_first2_v_sar_class_code
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_26_v_pif_symbol_first2_v_sar_class_code.SequenceNumber = 26
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_26_v_pif_symbol_first2_v_sar_class_code.Identifiers = v_pif_symbol_first2 || '&' || v_sar_class_code

	LEFT JOIN LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_27_v_pif_symbol_first2_v_sar_type_bureau_v_sar_major_peril
	ON LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_27_v_pif_symbol_first2_v_sar_type_bureau_v_sar_major_peril.SequenceNumber = 27
	AND LKP_SUPINSURANCEREFERENCELINEOFBUSINESSRULES_27_v_pif_symbol_first2_v_sar_type_bureau_v_sar_major_peril.Identifiers = v_pif_symbol_first2 || '&' || v_sar_type_bureau || '&' || v_sar_major_peril

),
LKP_ClassificationReference AS (
	SELECT
	OriginatingOrganizationCode,
	InsuranceLineCode,
	ClassCode
	FROM (
		SELECT DISTINCT LTRIM(RTRIM(OriginatingOrganizationCode)) as OriginatingOrganizationCode, 
		LTRIM(RTRIM(InsuranceLineCode)) as InsuranceLineCode, 
		LTRIM(RTRIM(ClassCode)) as ClassCode 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.ClassificationReference
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceLineCode,ClassCode ORDER BY OriginatingOrganizationCode) = 1
),
LKP_InsuranceReferenceLineOfBusiness AS (
	SELECT
	InsuranceReferenceLineOfBusinessAKId,
	InsuranceReferenceLineOfBusinessCode
	FROM (
		SELECT 
			InsuranceReferenceLineOfBusinessAKId,
			InsuranceReferenceLineOfBusinessCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceLineOfBusiness
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceReferenceLineOfBusinessCode ORDER BY InsuranceReferenceLineOfBusinessAKId) = 1
),
LKP_SupProduct AS (
	SELECT
	ProductCode,
	SourceProductCode,
	EffectiveDate,
	ExpirationDate
	FROM (
		SELECT 
			ProductCode,
			SourceProductCode,
			EffectiveDate,
			ExpirationDate
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupProduct
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SourceProductCode,EffectiveDate,ExpirationDate ORDER BY ProductCode) = 1
),
LKP_Product AS (
	SELECT
	ProductAKId,
	ProductCode
	FROM (
		SELECT 
			ProductAKId,
			ProductCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.Product
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProductCode ORDER BY ProductAKId) = 1
),
LKP_StatisticalCoverage AS (
	SELECT
	CurrentSnapshotFlag,
	StatisticalCoverageAKID,
	StatisticalCoverageID,
	SpecialClassGroupCode,
	AnnualStatementLineId,
	ClassCodeOrganizationCode,
	ClassCode,
	StatisticalCoverageHashKey
	FROM (
		SELECT StatisticalCoverageID AS StatisticalCoverageID,
		STATCOV.CurrentSnapshotFlag AS CurrentSnapshotFlag,
		STATCOV.StatisticalCoverageAKID AS StatisticalCoverageAKID,
		StatisticalCoverageHashKey AS StatisticalCoverageHashKey,
		SpecialClassGroupCode as SpecialClassGroupCode,
		AnnualStatementLineId as AnnualStatementLineId,
		ClassCodeOrganizationCode as ClassCodeOrganizationCode,
		ClassCode as ClassCode
		FROM	@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation LOC
		INNER JOIN  @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Policy POL
		ON LOC.PolicyAKID = POL.pol_ak_id
		INNER JOIN 	@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage POLCOV
		ON LOC.RiskLocationAKID = POLCOV.RiskLocationAKID
		INNER JOIN 	@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage STATCOV
		ON POLCOV.PolicyCoverageAKID = STATCOV.PolicyCoverageAKID
		WHERE	POL.crrnt_snpsht_flag = 1 AND LOC.CurrentSnapshotFlag =1
				AND POLCOV.CurrentSnapshotFlag =1
		        	AND POL.SOURCE_SYS_ID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		        	AND  EXISTS  (SELECT DISTINCT PolicyKey FROM  
									@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPolicyKeyList
									WHERE POL.pol_key = PolicyKey AND @{pipeline().parameters.WHERE_CLAUSE_EDW})
		ORDER BY StatisticalCoverageHashKey,STATCOV.CurrentSnapshotFlag
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StatisticalCoverageHashKey ORDER BY CurrentSnapshotFlag DESC) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_StatisticalCoverage.CurrentSnapshotFlag AS LKP_CurrentSnapshotFlag,
	-- *INF*: Decode(LKP_CurrentSnapshotFlag,'T','1','F','0',LKP_CurrentSnapshotFlag)
	Decode(LKP_CurrentSnapshotFlag,
	'T', '1',
	'F', '0',
	LKP_CurrentSnapshotFlag) AS v_LKP_CurrentSnapshotFlag,
	LKP_StatisticalCoverage.StatisticalCoverageAKID AS LKP_StatisticalCoverageAKID,
	LKP_StatisticalCoverage.StatisticalCoverageID AS i_StatisticalCoverageID,
	LKP_StatisticalCoverage.SpecialClassGroupCode AS i_SpecialClassGroupCode,
	LKP_StatisticalCoverage.AnnualStatementLineId AS i_AnnualStatementLineId,
	LKP_StatisticalCoverage.ClassCodeOrganizationCode AS i_ClassCodeOrganizationCode,
	LKP_StatisticalCoverage.ClassCode AS i_ClassCode,
	LKP_InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessAKId AS i_InsuranceReferenceLineOfBusinessAKId,
	LKP_Product.ProductAKId AS i_ProductAKId,
	LKP_ClassificationReference.OriginatingOrganizationCode AS i_OriginatingOrganizationCode,
	EXP_Values.ins_line_OUT AS i_InsuranceLine,
	EXP_Values.PolicyCoverageAKID_OUT,
	EXP_Values.SubLocationUnitNumber,
	EXP_Values.RiskUnitGroup,
	EXP_Values.RiskUnitGroupSequenceNumber,
	EXP_Values.RiskUnit,
	EXP_Values.RiskUnitSequenceNumber,
	EXP_Values.MajorPerilCode,
	EXP_Values.MajorPerilSequenceNumber,
	EXP_Values.o_SublineCode AS SublineCode,
	EXP_Values.PMSTypeExposure,
	EXP_Values.o_ClassCode AS ClassCode,
	EXP_Values.StatisticalCoverageEffectiveDate,
	EXP_Values.ReinsuranceSectionCode,
	EXP_Values.o_StatisticalCoverageHashKey AS StatisticalCoverageHashKey,
	EXP_Values.o_StatisticalCoverageKey AS StatisticalCoverageKey,
	EXP_Values.o_CoverageGuid AS CoverageGuid,
	EXP_Values.o_SpecialClassLevel1 AS SpecialClassGroupCode,
	EXP_Values.o_ClassCodeOrganizationCode AS ClassCodeOrganizationCode,
	-1 AS v_AnnualStatementLineId,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	0 AS o_logicalIndicator,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS o_StatisticalCoverageExpirationDate,
	-- *INF*: IIF(ISNULL(i_ProductAKId),-1,i_ProductAKId)
	IFF(i_ProductAKId IS NULL, - 1, i_ProductAKId) AS o_ProductAKId,
	-- *INF*: IIF(ISNULL(i_InsuranceReferenceLineOfBusinessAKId),-1,i_InsuranceReferenceLineOfBusinessAKId)
	IFF(i_InsuranceReferenceLineOfBusinessAKId IS NULL, - 1, i_InsuranceReferenceLineOfBusinessAKId) AS o_InsuranceReferenceLineOfBusinessAKId,
	v_AnnualStatementLineId AS o_AnnualStatementLineId,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_StatisticalCoverageID), 'NEW', v_LKP_CurrentSnapshotFlag='0' or
	-- i_SpecialClassGroupCode != SpecialClassGroupCode OR 
	-- i_AnnualStatementLineId != v_AnnualStatementLineId OR 
	-- i_ClassCodeOrganizationCode != ClassCodeOrganizationCode OR i_ClassCode != ClassCode,
	-- 'UPDATE',
	-- 'NOCHANGE')
	DECODE(TRUE,
	i_StatisticalCoverageID IS NULL, 'NEW',
	v_LKP_CurrentSnapshotFlag = '0' OR i_SpecialClassGroupCode != SpecialClassGroupCode OR i_AnnualStatementLineId != v_AnnualStatementLineId OR i_ClassCodeOrganizationCode != ClassCodeOrganizationCode OR i_ClassCode != ClassCode, 'UPDATE',
	'NOCHANGE') AS o_ChangeFlag
	FROM EXP_Values
	LEFT JOIN LKP_ClassificationReference
	ON LKP_ClassificationReference.InsuranceLineCode = EXP_Values.ins_line_OUT AND LKP_ClassificationReference.ClassCode = EXP_Values.o_ClassCode
	LEFT JOIN LKP_InsuranceReferenceLineOfBusiness
	ON LKP_InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessCode = EXP_Values.o_InsuranceReferenceLineOfBusinessCode
	LEFT JOIN LKP_Product
	ON LKP_Product.ProductCode = LKP_SupProduct.ProductCode
	LEFT JOIN LKP_StatisticalCoverage
	ON LKP_StatisticalCoverage.StatisticalCoverageHashKey = EXP_Values.o_StatisticalCoverageHashKey
),
AGG_Eliminate_Dups AS (
	SELECT
	LKP_StatisticalCoverageAKID, 
	i_StatisticalCoverageID AS LKP_StatisticalCoverageID, 
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag, 
	o_AuditID AS AuditID, 
	o_EffectiveDate AS EffectiveDate, 
	o_ExpirationDate AS ExpirationDate, 
	o_SourceSystemID AS SourceSystemID, 
	o_CreatedDate AS CreatedDate, 
	o_ModifiedDate AS ModifiedDate, 
	o_logicalIndicator AS logicalIndicator, 
	StatisticalCoverageHashKey, 
	PolicyCoverageAKID_OUT AS PolicyCoverageAKID, 
	StatisticalCoverageKey, 
	SubLocationUnitNumber, 
	RiskUnitGroup, 
	RiskUnitGroupSequenceNumber, 
	RiskUnit, 
	RiskUnitSequenceNumber, 
	MajorPerilCode, 
	MajorPerilSequenceNumber, 
	SublineCode, 
	PMSTypeExposure, 
	ClassCode, 
	StatisticalCoverageEffectiveDate, 
	o_StatisticalCoverageExpirationDate AS StatisticalCoverageExpirationDate, 
	ReinsuranceSectionCode, 
	o_ProductAKId AS ProductAKId, 
	o_InsuranceReferenceLineOfBusinessAKId AS InsuranceReferenceLineOfBusinessAKId, 
	o_AnnualStatementLineId AS AnnualStatementLineId, 
	CoverageGuid, 
	SpecialClassGroupCode, 
	ClassCodeOrganizationCode, 
	o_ChangeFlag AS ChangeFlag
	FROM EXP_Detect_Changes
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StatisticalCoverageHashKey, StatisticalCoverageKey ORDER BY NULL) = 1
),
RTR_StatisticalCoverage AS (
	SELECT
	LKP_StatisticalCoverageAKID,
	LKP_StatisticalCoverageID,
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	logicalIndicator,
	StatisticalCoverageHashKey,
	PolicyCoverageAKID,
	StatisticalCoverageKey,
	SubLocationUnitNumber,
	RiskUnitGroup,
	RiskUnitGroupSequenceNumber,
	RiskUnit,
	RiskUnitSequenceNumber,
	MajorPerilCode,
	MajorPerilSequenceNumber,
	SublineCode,
	PMSTypeExposure,
	ClassCode,
	StatisticalCoverageEffectiveDate,
	StatisticalCoverageExpirationDate,
	ReinsuranceSectionCode,
	ProductAKId,
	InsuranceReferenceLineOfBusinessAKId,
	AnnualStatementLineId,
	CoverageGuid,
	SpecialClassGroupCode,
	ClassCodeOrganizationCode,
	ChangeFlag
	FROM AGG_Eliminate_Dups
),
RTR_StatisticalCoverage_Insert AS (SELECT * FROM RTR_StatisticalCoverage WHERE ChangeFlag='NEW'),
RTR_StatisticalCoverage_Update AS (SELECT * FROM RTR_StatisticalCoverage WHERE ChangeFlag='UPDATE'),
EXP_Update_DataCollect AS (
	SELECT
	LKP_StatisticalCoverageID AS StatisticalCoverageID,
	ModifiedDate,
	ClassCode,
	AnnualStatementLineId,
	SpecialClassGroupCode,
	ClassCodeOrganizationCode,
	'1' AS CurrentSnapshotFlag,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate
	FROM RTR_StatisticalCoverage_Update
),
UPD_StatisticalCoverage AS (
	SELECT
	StatisticalCoverageID, 
	ModifiedDate, 
	ClassCode, 
	AnnualStatementLineId, 
	SpecialClassGroupCode, 
	ClassCodeOrganizationCode, 
	CurrentSnapshotFlag, 
	ExpirationDate
	FROM EXP_Update_DataCollect
),
TGT_StatisticalCoverage_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage AS T
	USING UPD_StatisticalCoverage AS S
	ON T.StatisticalCoverageID = S.StatisticalCoverageID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate, T.ClassCode = S.ClassCode, T.AnnualStatementLineId = S.AnnualStatementLineId, T.SpecialClassGroupCode = S.SpecialClassGroupCode, T.ClassCodeOrganizationCode = S.ClassCodeOrganizationCode
),
SEQ_StatisticalCoverageAKID AS (
	CREATE SEQUENCE SEQ_StatisticalCoverageAKID
	START = 0
	INCREMENT = 1;
),
EXP_TGT_DataCollect AS (
	SELECT
	LKP_StatisticalCoverageAKID,
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	logicalIndicator,
	StatisticalCoverageHashKey,
	-- *INF*: IIF(ISNULL(LKP_StatisticalCoverageAKID),NEXTVAL,LKP_StatisticalCoverageAKID)
	IFF(LKP_StatisticalCoverageAKID IS NULL, NEXTVAL, LKP_StatisticalCoverageAKID) AS StatisticalCoverageAKID,
	PolicyCoverageAKID,
	StatisticalCoverageKey,
	SubLocationUnitNumber,
	RiskUnitGroup,
	RiskUnitGroupSequenceNumber,
	RiskUnit,
	RiskUnitSequenceNumber,
	MajorPerilCode,
	MajorPerilSequenceNumber,
	SublineCode,
	PMSTypeExposure,
	ClassCode,
	StatisticalCoverageEffectiveDate,
	StatisticalCoverageExpirationDate,
	ReinsuranceSectionCode,
	ProductAKId,
	InsuranceReferenceLineOfBusinessAKId,
	AnnualStatementLineId,
	CoverageGuid,
	SpecialClassGroupCode,
	ClassCodeOrganizationCode,
	SEQ_StatisticalCoverageAKID.NEXTVAL
	FROM RTR_StatisticalCoverage_Insert
),
TGT_StatisticalCoverage_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, LogicalIndicator, StatisticalCoverageHashKey, StatisticalCoverageAKID, PolicyCoverageAKID, StatisticalCoverageKey, SubLocationUnitNumber, RiskUnitGroup, RiskUnitGroupSequenceNumber, RiskUnit, RiskUnitSequenceNumber, MajorPerilCode, MajorPerilSequenceNumber, SublineCode, PMSTypeExposure, ClassCode, StatisticalCoverageEffectiveDate, StatisticalCoverageExpirationDate, ReinsuranceSectionCode, ProductAKId, InsuranceReferenceLineOfBusinessAKId, AnnualStatementLineId, CoverageGuid, SpecialClassGroupCode, ClassCodeOrganizationCode)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	logicalIndicator AS LOGICALINDICATOR, 
	STATISTICALCOVERAGEHASHKEY, 
	STATISTICALCOVERAGEAKID, 
	POLICYCOVERAGEAKID, 
	STATISTICALCOVERAGEKEY, 
	SUBLOCATIONUNITNUMBER, 
	RISKUNITGROUP, 
	RISKUNITGROUPSEQUENCENUMBER, 
	RISKUNIT, 
	RISKUNITSEQUENCENUMBER, 
	MAJORPERILCODE, 
	MAJORPERILSEQUENCENUMBER, 
	SUBLINECODE, 
	PMSTYPEEXPOSURE, 
	CLASSCODE, 
	STATISTICALCOVERAGEEFFECTIVEDATE, 
	STATISTICALCOVERAGEEXPIRATIONDATE, 
	REINSURANCESECTIONCODE, 
	PRODUCTAKID, 
	INSURANCEREFERENCELINEOFBUSINESSAKID, 
	ANNUALSTATEMENTLINEID, 
	COVERAGEGUID, 
	SPECIALCLASSGROUPCODE, 
	CLASSCODEORGANIZATIONCODE
	FROM EXP_TGT_DataCollect
),
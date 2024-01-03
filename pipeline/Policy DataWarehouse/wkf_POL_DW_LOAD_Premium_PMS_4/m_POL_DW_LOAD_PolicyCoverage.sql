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
SQ_pif_4514_stage AS (
	SELECT DISTINCT RTRIM(A.pif_symbol) as pif_symbol,
	       A.pif_policy_number,
	       A.pif_module,
	       ltrim(rtrim(sar_insurance_line)) as sar_insurance_line,
	(CASE LEN(ltrim(rtrim(sar_location_x))) 
	WHEN '0' THEN ltrim(rtrim(sar_unit))
	ELSE ltrim(rtrim(sar_location_x)) 
	END) as sar_location_x,
	/*
	       ltrim(rtrim(sar_cov_eff_year)) as sar_cov_eff_year,
	       ltrim(rtrim(sar_cov_eff_month)) as sar_cov_eff_month,
	       ltrim(rtrim(sar_cov_eff_day)) as sar_cov_eff_day,
	*/
	       ltrim(rtrim(sar_state)) as sar_state,
	       ltrim(rtrim(sar_loc_prov_territory)) as sar_loc_prov_territory,
	CASE WHEN LEN(LTRIM(RTRIM(sar_county_first_two)) + LTRIM(RTRIM(sar_county_last_one)) + LTRIM(RTRIM(sar_city))) < 6 THEN '000000' ELSE 
	LTRIM(RTRIM(sar_county_first_two)) + LTRIM(RTRIM(sar_county_last_one)) + LTRIM(RTRIM(sar_city)) END as sar_city,
	       ltrim(rtrim(sar_type_bureau)) as sar_type_bureau,
	       ltrim(rtrim(sar_zip_postal_code)) as sar_zip_postal_code,
	       0 as logical_flag,
	       C.pif_line_business as pif_line_business,
	      C.pif_audit_code as pif_audit_code,
	      C.pif_risk_grade_guide as pif_risk_grade_guide,
	      D.comments_area as PriorCarrierName
	FROM  @{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME_4514}  A
	@{pipeline().parameters.JOIN_CONDITION}
	(SELECT DISTINCT Policykey FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.work_policytransactionstatus 
	WHERE AuditID = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AND
	PolicyStatus <> 'NOCHANGE')  B
	ON  A.policykey = B.policykey
	left join ( select pif_symbol,
	                  pif_policy_number,
	                  pif_module,
	                  pif_audit_code,
	                  pif_risk_grade_guide,
		            pif_line_business,
	                  row_number() over (partition by 
	                  pif_symbol,
	                  pif_policy_number,
	                  pif_module order by pif_02_stage_id desc) rn
	                 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME_02} ) C
	on A.pif_symbol=C.pif_symbol and A.pif_policy_number=C.pif_policy_number and A.pif_module=C.pif_module and C.rn=1
	left join (Select pif_symbol,
	                  pif_policy_number,
	                  pif_module,  
	                  comments_area,
	                  row_number() over (partition by 
	                  pif_symbol,
	                  pif_policy_number,
	                  pif_module order by pif_03_stage_id desc) rn
	 
	 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_03_stage where comments_reason_suspended='ZP') D
	 on A.pif_symbol=D.pif_symbol and A.pif_policy_number=D.pif_policy_number and A.pif_module=D.pif_module and D.rn=1
	WHERE A.logical_flag IN ('0','1','2','3') 
	@{pipeline().parameters.WHERE_CLAUSE}
),
LKP_Pif43LXZWCStage_RatingPlan AS (
	SELECT
	Pmdl4w1RatingProgramType,
	PifSymbol,
	PifPolicyNumber,
	PifModule
	FROM (
		SELECT DISTINCT A.Pmdl4w1RatingProgramType as Pmdl4w1RatingProgramType, 
		A.PifSymbol as PifSymbol, 
		A.PifPolicyNumber as PifPolicyNumber, 
		A.PifModule as PifModule 
		FROM @{pipeline().parameters.SOURCE_TABLE_NAME}.Pif43LXZWCStage A
		where A.Pmdl4w1SplitRateSeq in ( 
		select MAX(Pmdl4w1SplitRateSeq) from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43LXZWCStage B
		where A.PifSymbol = B.PifSymbol
		and A.PifPolicyNumber = B.PifPolicyNumber
		and A.PifModule = B.PifModule
		)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule ORDER BY Pmdl4w1RatingProgramType) = 1
),
LKP_pif_4514_ClassCode_WC AS (
	SELECT
	sar_class_1_4,
	pif_symbol,
	pif_policy_number,
	pif_module,
	sar_insurance_line,
	sar_location_x,
	sar_state,
	sar_loc_prov_territory,
	sar_city,
	sar_type_bureau,
	sar_zip_postal_code
	FROM (
		select distinct A.sar_class_1_4 as sar_class_1_4,
		rtrim(A.pif_symbol) as pif_symbol,
		A.pif_policy_number as pif_policy_number,
		A.pif_module as pif_module,
		ltrim(rtrim(sar_insurance_line)) as sar_insurance_line,
		(case len(ltrim(rtrim(sar_location_x))) when '0' then ltrim(rtrim(sar_unit)) else ltrim(rtrim(sar_location_x)) end) as sar_location_x,
		ltrim(rtrim(sar_state)) as sar_state,
		ltrim(rtrim(sar_loc_prov_territory)) as sar_loc_prov_territory,
		case when len(ltrim(rtrim(sar_county_first_two)) + ltrim(rtrim(sar_county_last_one)) + ltrim(rtrim(sar_city))) < 6 then '000000' 
		     else ltrim(rtrim(sar_county_first_two)) + ltrim(rtrim(sar_county_last_one)) + ltrim(rtrim(sar_city)) end as sar_city,
		ltrim(rtrim(sar_type_bureau)) as sar_type_bureau,
		ltrim(rtrim(sar_zip_postal_code)) as sar_zip_postal_code
		from  @{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME_4514}  A
		@{pipeline().parameters.JOIN_CONDITION}
		(select distinct Policykey from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.work_policytransactionstatus 
		where AuditID = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} and PolicyStatus <> 'NOCHANGE')  B
		ON  A.policykey = B.policykey
		where A.logical_flag in ('0','1','2','3') 
		and A.sar_class_1_4 in ('9657','9663','9664','9667','9668','9669','9670','9671','9672','9673','9674','9675','9676','9677',
		'9678','9679','9758','9759','9760','9761','9762','9763','9764','9770','9771','9772','9773','9780','9781','9784','9785',
		'9786','9787','9788','9789','9790','9791','9792','9793','9794','9795','9796','9797','9798','9799','9801','9870','9871',
		'9872','9878','9881','9882','9888','9895','9900','9901','9902','9903','9904','9905','9906','9907','9908','9909','9910',
		'9911','9912','9913','9914','9915','9916','9917','9918','9919','9920','9924','9925','9926','9927','9928','9929','9930',
		'9931','9932','9933','9934','9935','9936','9937','9938','9939','9940','9941','9942','9943','9944','9945','9946','9947',
		'9948','9949','9950','9951','9952','9953','9954','9955','9970','9971','9972','9973','9974','9975','9981','9982','9983','9986','9987','9991','9992')
		and A.sar_major_peril='032'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module,sar_insurance_line,sar_location_x,sar_state,sar_loc_prov_territory,sar_city,sar_type_bureau,sar_zip_postal_code ORDER BY sar_class_1_4) = 1
),
EXP_Default AS (
	SELECT
	SQ_pif_4514_stage.pif_symbol,
	SQ_pif_4514_stage.pif_policy_number,
	SQ_pif_4514_stage.pif_module,
	-- *INF*: (pif_symbol  || pif_policy_number || pif_module)
	( pif_symbol || pif_policy_number || pif_module ) AS v_Pol_key,
	-- *INF*: :LKP.LKP_POLICY_POLICYAKID(v_Pol_key)
	LKP_POLICY_POLICYAKID_v_Pol_key.pol_ak_id AS v_LKP_PolicyAKID,
	v_LKP_PolicyAKID AS PolicyAKID,
	v_Pol_key AS Pol_Key,
	SQ_pif_4514_stage.sar_insurance_line,
	SQ_pif_4514_stage.sar_location_x AS i_LocationUnitNumber,
	-- *INF*: LTRIM(RTRIM(i_LocationUnitNumber))
	LTRIM(RTRIM(i_LocationUnitNumber)) AS v_LocationUnitNumber,
	SQ_pif_4514_stage.sar_state,
	-- *INF*: IIF(LTRIM(RTRIM(sar_state))='00', '0',LTRIM(RTRIM(sar_state)))
	IFF(LTRIM(RTRIM(sar_state)) = '00', '0', LTRIM(RTRIM(sar_state))) AS v_StateProvinceCode,
	SQ_pif_4514_stage.sar_loc_prov_territory,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_loc_prov_territory)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_loc_prov_territory) AS v_RiskTerritory,
	SQ_pif_4514_stage.sar_city,
	-- *INF*: iif(reg_match(:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_city) ,'(\d{6})')
	-- ,:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_city)
	-- ,'000000')
	IFF(reg_match(:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_city), '(\d{6})'), :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_city), '000000') AS v_TaxLocation,
	SQ_pif_4514_stage.sar_type_bureau,
	SQ_pif_4514_stage.sar_zip_postal_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_zip_postal_code)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_zip_postal_code) AS v_ZipPostalCode,
	SQ_pif_4514_stage.logical_flag AS LogicalIndicator,
	SQ_pif_4514_stage.pif_line_business,
	SQ_pif_4514_stage.pif_audit_code,
	SQ_pif_4514_stage.pif_risk_grade_guide,
	-- *INF*: :LKP.LKP_RISKLOCATION_RISKLOCATIONAKID(v_LKP_PolicyAKID,v_LocationUnitNumber,v_RiskTerritory,v_StateProvinceCode,v_ZipPostalCode,v_TaxLocation)
	-- 
	LKP_RISKLOCATION_RISKLOCATIONAKID_v_LKP_PolicyAKID_v_LocationUnitNumber_v_RiskTerritory_v_StateProvinceCode_v_ZipPostalCode_v_TaxLocation.RiskLocationAKID AS LKP_RiskLocationAKID,
	SQ_pif_4514_stage.PriorCarrierName AS i_PriorCarrierName,
	-- *INF*: LTRIM(RTRIM(i_PriorCarrierName))
	LTRIM(RTRIM(i_PriorCarrierName)) AS o_PriorCarrierName,
	LKP_pif_4514_ClassCode_WC.sar_class_1_4 AS i_sar_class_1_4,
	LKP_Pif43LXZWCStage_RatingPlan.Pmdl4w1RatingProgramType AS i_Pmdl4w1RatingProgramType,
	-- *INF*: UPPER(i_Pmdl4w1RatingProgramType)
	UPPER(i_Pmdl4w1RatingProgramType) AS v_Pmdl4w1RatingProgramType,
	-- *INF*: DECODE(TRUE,
	-- v_Pmdl4w1RatingProgramType='G' AND  NOT ISNULL(i_sar_class_1_4),
	-- 'Small Deductible',
	-- v_Pmdl4w1RatingProgramType='R',
	-- 'Large Risk Alternative Rating Option(LRARO)',
	-- 'Guaranteed Cost')
	DECODE(TRUE,
	v_Pmdl4w1RatingProgramType = 'G' AND NOT i_sar_class_1_4 IS NULL, 'Small Deductible',
	v_Pmdl4w1RatingProgramType = 'R', 'Large Risk Alternative Rating Option(LRARO)',
	'Guaranteed Cost') AS o_RatingPlanDescription
	FROM SQ_pif_4514_stage
	LEFT JOIN LKP_Pif43LXZWCStage_RatingPlan
	ON LKP_Pif43LXZWCStage_RatingPlan.PifSymbol = SQ_pif_4514_stage.pif_symbol AND LKP_Pif43LXZWCStage_RatingPlan.PifPolicyNumber = SQ_pif_4514_stage.pif_policy_number AND LKP_Pif43LXZWCStage_RatingPlan.PifModule = SQ_pif_4514_stage.pif_module
	LEFT JOIN LKP_pif_4514_ClassCode_WC
	ON LKP_pif_4514_ClassCode_WC.pif_symbol = SQ_pif_4514_stage.pif_symbol AND LKP_pif_4514_ClassCode_WC.pif_policy_number = SQ_pif_4514_stage.pif_policy_number AND LKP_pif_4514_ClassCode_WC.pif_module = SQ_pif_4514_stage.pif_module AND LKP_pif_4514_ClassCode_WC.sar_insurance_line = SQ_pif_4514_stage.sar_insurance_line AND LKP_pif_4514_ClassCode_WC.sar_location_x = SQ_pif_4514_stage.sar_location_x AND LKP_pif_4514_ClassCode_WC.sar_state = SQ_pif_4514_stage.sar_state AND LKP_pif_4514_ClassCode_WC.sar_loc_prov_territory = SQ_pif_4514_stage.sar_loc_prov_territory AND LKP_pif_4514_ClassCode_WC.sar_city = SQ_pif_4514_stage.sar_city AND LKP_pif_4514_ClassCode_WC.sar_type_bureau = SQ_pif_4514_stage.sar_type_bureau AND LKP_pif_4514_ClassCode_WC.sar_zip_postal_code = SQ_pif_4514_stage.sar_zip_postal_code
	LEFT JOIN LKP_POLICY_POLICYAKID LKP_POLICY_POLICYAKID_v_Pol_key
	ON LKP_POLICY_POLICYAKID_v_Pol_key.pol_key = v_Pol_key

	LEFT JOIN LKP_RISKLOCATION_RISKLOCATIONAKID LKP_RISKLOCATION_RISKLOCATIONAKID_v_LKP_PolicyAKID_v_LocationUnitNumber_v_RiskTerritory_v_StateProvinceCode_v_ZipPostalCode_v_TaxLocation
	ON LKP_RISKLOCATION_RISKLOCATIONAKID_v_LKP_PolicyAKID_v_LocationUnitNumber_v_RiskTerritory_v_StateProvinceCode_v_ZipPostalCode_v_TaxLocation.PolicyAKID = v_LKP_PolicyAKID
	AND LKP_RISKLOCATION_RISKLOCATIONAKID_v_LKP_PolicyAKID_v_LocationUnitNumber_v_RiskTerritory_v_StateProvinceCode_v_ZipPostalCode_v_TaxLocation.LocationUnitNumber = v_LocationUnitNumber
	AND LKP_RISKLOCATION_RISKLOCATIONAKID_v_LKP_PolicyAKID_v_LocationUnitNumber_v_RiskTerritory_v_StateProvinceCode_v_ZipPostalCode_v_TaxLocation.RiskTerritory = v_RiskTerritory
	AND LKP_RISKLOCATION_RISKLOCATIONAKID_v_LKP_PolicyAKID_v_LocationUnitNumber_v_RiskTerritory_v_StateProvinceCode_v_ZipPostalCode_v_TaxLocation.StateProvinceCode = v_StateProvinceCode
	AND LKP_RISKLOCATION_RISKLOCATIONAKID_v_LKP_PolicyAKID_v_LocationUnitNumber_v_RiskTerritory_v_StateProvinceCode_v_ZipPostalCode_v_TaxLocation.ZipPostalCode = v_ZipPostalCode
	AND LKP_RISKLOCATION_RISKLOCATIONAKID_v_LKP_PolicyAKID_v_LocationUnitNumber_v_RiskTerritory_v_StateProvinceCode_v_ZipPostalCode_v_TaxLocation.TaxLocation = v_TaxLocation

),
EXP_GetAKIDs AS (
	SELECT
	PolicyAKID,
	Pol_Key AS PolKey,
	sar_insurance_line AS in_sar_insurance_line,
	sar_type_bureau AS in_sar_type_bureau,
	pif_audit_code AS in_pif_audit_code,
	pif_risk_grade_guide AS in_pif_risk_grade_guide,
	LKP_RiskLocationAKID AS RiskLocationAKID,
	LogicalIndicator,
	pif_line_business,
	-- *INF*: iif(isnull(PolicyAKID)
	-- ,error('Policy_AK_ID can not be blank'))
	IFF(PolicyAKID IS NULL, error('Policy_AK_ID can not be blank')) AS v_ErrorPolicy,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_sar_insurance_line)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_sar_insurance_line) AS v_InsuranceLine,
	-- *INF*: --TO_CHAR(in_sar_cov_eff_year)
	'' AS v_sar_cov_eff_year,
	-- *INF*: --LPAD(TO_CHAR(in_sar_cov_eff_month),2,'0')
	'' AS v_sar_cov_eff_month,
	-- *INF*: --LPAD(TO_CHAR(in_sar_cov_eff_day),2,'0')
	'' AS v_sar_cov_eff_day,
	-- *INF*: --LPAD(TO_CHAR(in_sar_cov_eff_month),2,'0') || '/' || LPAD(TO_CHAR(in_sar_cov_eff_day),2,'0')	||	'/'	||
	-- --TO_CHAR(in_sar_cov_eff_year)
	'' AS v_sar_cov_eff_date,
	-- *INF*: --TO_DATE(v_sar_cov_eff_month  || '/'  || v_sar_cov_eff_day  || '/'  || v_sar_cov_eff_year, 'MM/DD/YYYY')
	'' AS v_PolicyCoverageEffectiveDate,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_sar_type_bureau)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_sar_type_bureau) AS v_TypeBureauCode,
	-- *INF*: iif(isnull(RiskLocationAKID)
	-- ,error('RiskLocationAKID can not be blank'))
	IFF(RiskLocationAKID IS NULL, error('RiskLocationAKID can not be blank')) AS v_ErrorRiskLocation,
	PolKey || RiskLocationAKID AS v_PolicyCoverageKey,
	-- *INF*: MD5(to_char(PolicyAKID) || to_char(RiskLocationAKID) || v_InsuranceLine || v_TypeBureauCode)
	MD5(to_char(PolicyAKID) || to_char(RiskLocationAKID) || v_InsuranceLine || v_TypeBureauCode) AS v_PolicyCoverageHashKey,
	v_PolicyCoverageHashKey AS out_PolicyCoverageHashKey,
	v_PolicyCoverageKey AS out_PolicyCoverageKey,
	v_InsuranceLine AS out_InsuranceLine,
	v_TypeBureauCode AS out_TypeBureauCode,
	-- *INF*: TO_DATE('01/01/1800 23:59:59' , 'MM/DD/YYYY HH24:MI:SS')
	-- --v_PolicyCoverageEffectiveDate
	TO_DATE('01/01/1800 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS out_PolicyCoverageEffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59' , 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS out_PolicyCoverageExpirationDate,
	-- *INF*: IIF(IN(LTRIM(RTRIM(in_pif_audit_code)),'Y','A','M'),'1','0')
	IFF(IN(LTRIM(RTRIM(in_pif_audit_code)), 'Y', 'A', 'M'), '1', '0') AS out_AuditableIndicator,
	-- *INF*: IIF(ISNULL(in_pif_risk_grade_guide) OR IS_SPACES(in_pif_risk_grade_guide) OR LENGTH(in_pif_risk_grade_guide)=0, 'N/A', LTRIM(RTRIM(in_pif_risk_grade_guide)))
	IFF(in_pif_risk_grade_guide IS NULL OR IS_SPACES(in_pif_risk_grade_guide) OR LENGTH(in_pif_risk_grade_guide) = 0, 'N/A', LTRIM(RTRIM(in_pif_risk_grade_guide))) AS out_RiskGradeCode,
	o_PriorCarrierName AS PriorCarrierName,
	'N/A' AS PriorPolicyKey,
	'N/A' AS PriorInsuranceLine,
	o_RatingPlanDescription AS RatingPlanDescription
	FROM EXP_Default
),
LKP_Pif43LXZWCStage AS (
	SELECT
	Pmdl4w1InterstRiskIdNo2,
	Pmdl4w1InterstRiskFiller,
	PolKey
	FROM (
		select
		lx.PifSymbol+lx.PifPolicyNumber+lx.PifModule as PolKey,
		lx.Pmdl4w1InterstRiskIdNo2 as Pmdl4w1InterstRiskIdNo2,
		lx.Pmdl4w1InterstRiskFiller as Pmdl4w1InterstRiskFiller
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43LXZWCStage lx
		where LTRIM(lx.Pmdl4w1InterstRiskIdNo2)<>''
		and lx.Pmdl4w1SegmentPartCode = 'x'
		and lx.Pmdl4w1SplitRateSeq in ( 
		select MAX(Pmdl4w1SplitRateSeq) from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43LXZWCStage a
		where lx.PifSymbol = a.PifSymbol
		and lx.PifPolicyNumber = a.PifPolicyNumber
		and lx.PifModule = a.PifModule
		and LTRIM(a.Pmdl4w1InterstRiskIdNo2) <> ''
		and lx.Pmdl4w1SegmentPartCode = 'x')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolKey ORDER BY Pmdl4w1InterstRiskIdNo2) = 1
),
LKP_PolicyCoverage AS (
	SELECT
	PolicyCoverageAKID,
	CurrentSnapshotFlag,
	PolicyCoverageID,
	RatingPlanAKId,
	AuditableIndicator,
	RiskGradeCode,
	InterstateRiskId,
	PolicyLimitAKId,
	PriorCoverageId,
	PolicyCoverageHashKey
	FROM (
		SELECT PolicyCoverageID    AS PolicyCoverageID,
		POLCOV.PolicyCoverageAKID AS PolicyCoverageAKID,
		POLCOV.CurrentSnapshotFlag AS CurrentSnapshotFlag,
		       	      PolicyCoverageHashKey AS PolicyCoverageHashKey,
		                   RatingPlanAKId as RatingPlanAKId,
		       	      AuditableIndicator as AuditableIndicator, 
		       	      RiskGradeCode as RiskGradeCode, 
			POLCOV.InterstateRiskId as InterstateRiskId,
					PolicyLimitAKId AS PolicyLimitAKId,
					PriorCoverageId AS PriorCoverageId
		FROM	@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation LOC 
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Policy POL
		         ON LOC.PolicyAKID = POL.pol_ak_id 
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage POLCOV
		ON LOC.RiskLocationAKID = POLCOV.RiskLocationAKID
		WHERE  POL.crrnt_snpsht_flag = 1
		       AND LOC.CurrentSnapshotFlag = 1
		       AND POL.SOURCE_SYS_ID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		       AND  EXISTS  (SELECT DISTINCT PolicyKey FROM  							@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPolicyKeyList
									WHERE POL.pol_key = PolicyKey AND @{pipeline().parameters.WHERE_CLAUSE_EDW})
		ORDER BY PolicyCoverageHashKey,POLCOV.CurrentSnapshotFlag
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyCoverageHashKey ORDER BY PolicyCoverageAKID DESC) = 1
),
LKP_PolicyLimit AS (
	SELECT
	PolicyLimitAKId,
	PolicyAKId,
	InsuranceLine
	FROM (
		SELECT 
			PolicyLimitAKId,
			PolicyAKId,
			InsuranceLine
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyLimit
		WHERE CurrentSnapshotFlag=1 AND SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKId,InsuranceLine ORDER BY PolicyLimitAKId) = 1
),
LKP_PriorCoverage AS (
	SELECT
	PriorCoverageId,
	PriorCarrierName,
	PriorPolicyKey,
	PriorInsuranceLine
	FROM (
		select priorCoverageID as priorCoverageID ,
		PriorCarrierName as PriorCarrierName,
		PriorPolicyKey as PriorPolicyKey,
		PriorInsuranceLine as PriorInsuranceLine
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.PriorCoverage where sourceSystemId='PMS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PriorCarrierName,PriorPolicyKey,PriorInsuranceLine ORDER BY PriorCoverageId) = 1
),
LKP_RatingPlan AS (
	SELECT
	RatingPlanAKId,
	RatingPlanDescription
	FROM (
		SELECT 
			RatingPlanAKId,
			RatingPlanDescription
		FROM RatingPlan
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingPlanDescription ORDER BY RatingPlanAKId) = 1
),
LKP_SupInsuranceLine_SupInsLineId AS (
	SELECT
	sup_ins_line_id,
	ins_line_code
	FROM (
		SELECT 
			sup_ins_line_id,
			ins_line_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_insurance_line
		WHERE source_sys_id = '@{pipeline().parameters.MERGED_SOURCE_SYSTEM_ID}' AND crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ins_line_code ORDER BY sup_ins_line_id) = 1
),
LKP_SupTypeBureauCode_SupTypeBureauCodeId AS (
	SELECT
	sup_type_bureau_code_id,
	type_bureau_code
	FROM (
		SELECT 
			sup_type_bureau_code_id,
			type_bureau_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_type_bureau_code
		WHERE crrnt_snpsht_flag=1 AND source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY type_bureau_code ORDER BY sup_type_bureau_code_id) = 1
),
EXP_GetDefaultValue AS (
	SELECT
	LKP_PolicyCoverage.CurrentSnapshotFlag AS LKP_CurrentSnapshotFlag,
	-- *INF*: Decode(LKP_CurrentSnapshotFlag,'T','1','F','0',LKP_CurrentSnapshotFlag)
	Decode(LKP_CurrentSnapshotFlag,
	'T', '1',
	'F', '0',
	LKP_CurrentSnapshotFlag) AS v_LKP_CurrentSnapshotFlag,
	LKP_PolicyCoverage.PolicyCoverageAKID AS LKP_PolicyCoverageAKID,
	LKP_PolicyCoverage.PolicyCoverageID AS in_PolicyCoverageID,
	LKP_PolicyCoverage.RatingPlanAKId AS in_RatingPlanAKId,
	LKP_PolicyCoverage.AuditableIndicator AS in_AuditableIndicator,
	LKP_PolicyCoverage.RiskGradeCode AS in_RiskGradeCode,
	LKP_PolicyCoverage.InterstateRiskId AS in_InterstateRiskId,
	LKP_PolicyCoverage.PolicyLimitAKId AS in_PolicyLimitAKId,
	LKP_PolicyCoverage.PriorCoverageId AS in_PriorCoverageId,
	LKP_Pif43LXZWCStage.Pmdl4w1InterstRiskIdNo2 AS in_Pmdl4w1InterstRiskIdNo2,
	LKP_Pif43LXZWCStage.Pmdl4w1InterstRiskFiller AS in_Pmdl4w1InterstRiskFiller,
	EXP_GetAKIDs.LogicalIndicator,
	EXP_GetAKIDs.pif_line_business AS in_pif_line_business,
	EXP_GetAKIDs.out_PolicyCoverageHashKey AS PolicyCoverageHashKey,
	EXP_GetAKIDs.PolicyAKID,
	EXP_GetAKIDs.RiskLocationAKID,
	EXP_GetAKIDs.out_PolicyCoverageKey AS PolicyCoverageKey,
	EXP_GetAKIDs.out_InsuranceLine AS InsuranceLine,
	EXP_GetAKIDs.out_TypeBureauCode AS TypeBureauCode,
	EXP_GetAKIDs.out_PolicyCoverageEffectiveDate AS PolicyCoverageEffectiveDate,
	EXP_GetAKIDs.out_PolicyCoverageExpirationDate AS PolicyCoverageExpirationDate,
	EXP_GetAKIDs.out_AuditableIndicator AS AuditableIndicator,
	EXP_GetAKIDs.out_RiskGradeCode AS RiskGradeCode,
	LKP_SupInsuranceLine_SupInsLineId.sup_ins_line_id AS in_sup_ins_line_id,
	LKP_SupTypeBureauCode_SupTypeBureauCodeId.sup_type_bureau_code_id AS in_sup_type_bureau_code_id,
	LKP_PolicyLimit.PolicyLimitAKId AS lkp_PolicyLimitAKId,
	LKP_PriorCoverage.PriorCoverageId AS lkp_PriorCoverageId,
	LKP_RatingPlan.RatingPlanAKId AS lkp_RatingPlanAKId,
	-- *INF*: IIF(ISNULL(lkp_RatingPlanAKId),-1, lkp_RatingPlanAKId)
	IFF(lkp_RatingPlanAKId IS NULL, - 1, lkp_RatingPlanAKId) AS v_RatingPlanAKId,
	-- *INF*: DECODE(in_AuditableIndicator,
	-- 'T',
	-- '1',
	-- 'F',
	-- '0',
	-- NULL)
	DECODE(in_AuditableIndicator,
	'T', '1',
	'F', '0',
	NULL) AS v_LKP_AuditableIndicator,
	-- *INF*: IIF(
	-- ISNULL(in_Pmdl4w1InterstRiskIdNo2) OR ISNULL(in_Pmdl4w1InterstRiskFiller) OR NOT IN(in_pif_line_business,'WC','WCP'),'N/A',
	-- in_Pmdl4w1InterstRiskIdNo2 || in_Pmdl4w1InterstRiskFiller
	-- )
	IFF(in_Pmdl4w1InterstRiskIdNo2 IS NULL OR in_Pmdl4w1InterstRiskFiller IS NULL OR NOT IN(in_pif_line_business, 'WC', 'WCP'), 'N/A', in_Pmdl4w1InterstRiskIdNo2 || in_Pmdl4w1InterstRiskFiller) AS v_InterstateRiskId,
	-- *INF*: IIF(ISNULL(in_sup_ins_line_id), -1, in_sup_ins_line_id)
	IFF(in_sup_ins_line_id IS NULL, - 1, in_sup_ins_line_id) AS out_sup_ins_line_id,
	-- *INF*: IIF(ISNULL(in_sup_type_bureau_code_id), -1, in_sup_type_bureau_code_id)
	IFF(in_sup_type_bureau_code_id IS NULL, - 1, in_sup_type_bureau_code_id) AS out_sup_type_bureau_code_id,
	in_PolicyCoverageID AS LKP_PolicyCoverageID,
	v_RatingPlanAKId AS o_RatingPlanAKId,
	v_InterstateRiskId AS o_InterstateRiskId,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(in_PolicyCoverageID),
	-- 'NEW',v_LKP_CurrentSnapshotFlag='0' or in_RatingPlanAKId != v_RatingPlanAKId OR v_LKP_AuditableIndicator != AuditableIndicator OR in_RiskGradeCode != RiskGradeCode OR in_InterstateRiskId != v_InterstateRiskId OR in_PolicyLimitAKId != lkp_PolicyLimitAKId OR in_PriorCoverageId != lkp_PriorCoverageId,
	-- 'UPDATE',
	-- 'NOCHANGE')
	DECODE(TRUE,
	in_PolicyCoverageID IS NULL, 'NEW',
	v_LKP_CurrentSnapshotFlag = '0' OR in_RatingPlanAKId != v_RatingPlanAKId OR v_LKP_AuditableIndicator != AuditableIndicator OR in_RiskGradeCode != RiskGradeCode OR in_InterstateRiskId != v_InterstateRiskId OR in_PolicyLimitAKId != lkp_PolicyLimitAKId OR in_PriorCoverageId != lkp_PriorCoverageId, 'UPDATE',
	'NOCHANGE') AS o_ChangeFlag,
	lkp_PolicyLimitAKId AS o_PolicyLimitAKId,
	lkp_PriorCoverageId AS o_PriorCoverageId
	FROM EXP_GetAKIDs
	LEFT JOIN LKP_Pif43LXZWCStage
	ON LKP_Pif43LXZWCStage.PolKey = EXP_GetAKIDs.PolKey
	LEFT JOIN LKP_PolicyCoverage
	ON LKP_PolicyCoverage.PolicyCoverageHashKey = EXP_GetAKIDs.out_PolicyCoverageHashKey
	LEFT JOIN LKP_PolicyLimit
	ON LKP_PolicyLimit.PolicyAKId = EXP_GetAKIDs.PolicyAKID AND LKP_PolicyLimit.InsuranceLine = EXP_GetAKIDs.out_InsuranceLine
	LEFT JOIN LKP_PriorCoverage
	ON LKP_PriorCoverage.PriorCarrierName = EXP_GetAKIDs.PriorCarrierName AND LKP_PriorCoverage.PriorPolicyKey = EXP_GetAKIDs.PriorPolicyKey AND LKP_PriorCoverage.PriorInsuranceLine = EXP_GetAKIDs.PriorInsuranceLine
	LEFT JOIN LKP_RatingPlan
	ON LKP_RatingPlan.RatingPlanDescription = EXP_GetAKIDs.RatingPlanDescription
	LEFT JOIN LKP_SupInsuranceLine_SupInsLineId
	ON LKP_SupInsuranceLine_SupInsLineId.ins_line_code = EXP_GetAKIDs.out_InsuranceLine
	LEFT JOIN LKP_SupTypeBureauCode_SupTypeBureauCodeId
	ON LKP_SupTypeBureauCode_SupTypeBureauCodeId.type_bureau_code = EXP_GetAKIDs.out_TypeBureauCode
),
EXP_GetMetaValues AS (
	SELECT
	LKP_PolicyCoverageAKID,
	LogicalIndicator,
	PolicyCoverageHashKey,
	PolicyAKID,
	RiskLocationAKID,
	PolicyCoverageKey,
	InsuranceLine,
	TypeBureauCode,
	PolicyCoverageEffectiveDate,
	PolicyCoverageExpirationDate,
	out_sup_ins_line_id AS sup_ins_line_id,
	out_sup_type_bureau_code_id AS sup_type_bureau_code_id,
	o_RatingPlanAKId AS RatingPlanAKId,
	AuditableIndicator,
	RiskGradeCode,
	LKP_PolicyCoverageID,
	o_InterstateRiskId AS InterstateRiskId,
	o_ChangeFlag AS ChangeFlag,
	1 AS out_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS out_AuditID,
	-- *INF*: TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') AS out_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59' , 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS out_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS out_SourceSystemID,
	SYSDATE AS out_CreateDate,
	SYSDATE AS out_ModifiedDate,
	o_PolicyLimitAKId,
	o_PriorCoverageId,
	0 AS o_CustomerCareCommissionRate
	FROM EXP_GetDefaultValue
),
RTR_PolicyCoverage AS (
	SELECT
	LKP_PolicyCoverageAKID,
	LKP_PolicyCoverageID,
	ChangeFlag,
	out_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	out_AuditID AS AuditID,
	out_EffectiveDate AS EffectiveDate,
	out_ExpirationDate AS ExpirationDate,
	out_SourceSystemID AS SourceSystemID,
	out_CreateDate AS CreateDate,
	out_ModifiedDate AS ModifiedDate,
	LogicalIndicator,
	PolicyCoverageHashKey,
	PolicyAKID,
	RiskLocationAKID,
	PolicyCoverageKey,
	InsuranceLine,
	TypeBureauCode,
	PolicyCoverageEffectiveDate,
	PolicyCoverageExpirationDate,
	sup_ins_line_id,
	sup_type_bureau_code_id,
	RatingPlanAKId,
	AuditableIndicator,
	RiskGradeCode,
	InterstateRiskId,
	o_PolicyLimitAKId AS PolicyLimitAKId,
	o_PriorCoverageId AS PriorCoverageId,
	o_CustomerCareCommissionRate AS CustomerCareCommissionRate
	FROM EXP_GetMetaValues
),
RTR_PolicyCoverage_NEW AS (SELECT * FROM RTR_PolicyCoverage WHERE ChangeFlag='NEW'),
RTR_PolicyCoverage_UPDATE AS (SELECT * FROM RTR_PolicyCoverage WHERE ChangeFlag='UPDATE'),
SEQ_PolicyCoverageAKID AS (
	CREATE SEQUENCE SEQ_PolicyCoverageAKID
	START = 0
	INCREMENT = 1;
),
EXP_TGT_DataCollect AS (
	SELECT
	LKP_PolicyCoverageAKID,
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreateDate,
	ModifiedDate,
	LogicalIndicator,
	PolicyCoverageHashKey,
	-- *INF*: IIF(ISNULL(LKP_PolicyCoverageAKID),NEXTVAL,LKP_PolicyCoverageAKID)
	IFF(LKP_PolicyCoverageAKID IS NULL, NEXTVAL, LKP_PolicyCoverageAKID) AS PolicyCoverageAKID,
	PolicyAKID,
	RiskLocationAKID,
	PolicyCoverageKey,
	InsuranceLine,
	TypeBureauCode,
	PolicyCoverageEffectiveDate,
	PolicyCoverageExpirationDate,
	sup_ins_line_id,
	sup_type_bureau_code_id,
	RatingPlanAKId,
	AuditableIndicator,
	RiskGradeCode,
	InterstateRiskId,
	PolicyLimitAKId,
	PriorCoverageId,
	CustomerCareCommissionRate,
	SEQ_PolicyCoverageAKID.NEXTVAL
	FROM RTR_PolicyCoverage_NEW
),
TGT_PolicyCoverage_New_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, LogicalIndicator, PolicyCoverageHashKey, PolicyCoverageAKID, PolicyAKID, RiskLocationAKID, PolicyCoverageKey, InsuranceLine, TypeBureauCode, PolicyCoverageEffectiveDate, PolicyCoverageExpirationDate, SupInsuranceLineId, SupTypeBureauCodeId, RatingPlanAKId, AuditableIndicator, RiskGradeCode, InterstateRiskId, PolicyLimitAKId, PriorCoverageId, CustomerCareCommissionRate)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CreateDate AS CREATEDDATE, 
	MODIFIEDDATE, 
	LOGICALINDICATOR, 
	POLICYCOVERAGEHASHKEY, 
	POLICYCOVERAGEAKID, 
	POLICYAKID, 
	RISKLOCATIONAKID, 
	POLICYCOVERAGEKEY, 
	INSURANCELINE, 
	TYPEBUREAUCODE, 
	POLICYCOVERAGEEFFECTIVEDATE, 
	POLICYCOVERAGEEXPIRATIONDATE, 
	sup_ins_line_id AS SUPINSURANCELINEID, 
	sup_type_bureau_code_id AS SUPTYPEBUREAUCODEID, 
	RATINGPLANAKID, 
	AUDITABLEINDICATOR, 
	RISKGRADECODE, 
	INTERSTATERISKID, 
	POLICYLIMITAKID, 
	PRIORCOVERAGEID, 
	CUSTOMERCARECOMMISSIONRATE
	FROM EXP_TGT_DataCollect
),
EXP_Update_DataCollect AS (
	SELECT
	LKP_PolicyCoverageID,
	ModifiedDate,
	RatingPlanAKId,
	AuditableIndicator,
	RiskGradeCode,
	InterstateRiskId,
	PolicyLimitAKId,
	PriorCoverageId,
	'1' AS CurrentSnapshotFlag,
	-- *INF*: TO_DATE('12/31/2100 23:59:59' , 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate
	FROM RTR_PolicyCoverage_UPDATE
),
UPD_Existing AS (
	SELECT
	LKP_PolicyCoverageID, 
	ModifiedDate, 
	RatingPlanAKId, 
	AuditableIndicator, 
	RiskGradeCode, 
	InterstateRiskId, 
	PolicyLimitAKId, 
	PriorCoverageId, 
	CurrentSnapshotFlag, 
	ExpirationDate
	FROM EXP_Update_DataCollect
),
TGT_PolicyCoverage_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage AS T
	USING UPD_Existing AS S
	ON T.PolicyCoverageID = S.LKP_PolicyCoverageID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate, T.RatingPlanAKId = S.RatingPlanAKId, T.AuditableIndicator = S.AuditableIndicator, T.RiskGradeCode = S.RiskGradeCode, T.InterstateRiskId = S.InterstateRiskId, T.PolicyLimitAKId = S.PolicyLimitAKId, T.PriorCoverageId = S.PriorCoverageId
),
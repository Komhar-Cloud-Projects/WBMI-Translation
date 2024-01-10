WITH
SQ_claimant_coverage_detail AS (
	SELECT 
	DISTINCT 
	CASE ccd.loc_unit_num WHEN 'N/A' THEN '0000' ELSE ccd.loc_unit_num END as loc_unit_num , 
	CASE ccd.sub_loc_unit_num WHEN 'N/A' THEN '000' ELSE ccd.sub_loc_unit_num END as sub_loc_nuit_num, 
	ccd.ins_line, 
	CASE ccd.risk_unit_grp WHEN 'N/A' THEN '000' ELSE ccd.risk_unit_grp END as risk_unit_grp, 
	ccd.risk_unit_grp_seq_num, 
	rtrim(ccd.risk_unit) as risk_unit, 
	CASE ccd.risk_unit_seq_num WHEN 'N/A' THEN '00' ELSE ccd.risk_unit_seq_num END as risk_unit_seq_num, 
	ccd.major_peril_code, 
	ccd.major_peril_seq, 
	co.pol_key_ak_id
	FROM
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail ccd,
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence co,
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence cp
	WHERE
		 ccd.claim_party_occurrence_ak_id = cp.claim_party_occurrence_ak_id
	 	AND cp.claim_occurrence_ak_id = co.claim_occurrence_ak_id
	 	AND co.crrnt_snpsht_flag = '1' 
	 	AND cp.crrnt_snpsht_flag = '1'
	 	AND ccd.crrnt_snpsht_flag='1'
),
EXP_claimant_coverage_detail_source AS (
	SELECT
	loc_unit_num,
	sub_loc_unit_num,
	ins_line,
	risk_unit_grp,
	risk_unit_grp_seq_num,
	risk_unit,
	risk_unit_seq_num,
	risk_unit_seq_num AS risk_unit_seq_num_out,
	major_peril_code,
	major_peril_seq,
	pol_key_ak_id
	FROM SQ_claimant_coverage_detail
),
SQ_temp_policy_transaction AS (
	SELECT DISTINCT P.pol_ak_id                 AS pol_ak_id,
	                TPT.risk_state_prov_code    AS risk_state_prov_code,
	                TPT.risk_zip_code           AS risk_zip_code,
	                TPT.terr_code               AS terr_code,
	                TPT.tax_loc                 AS tax_loc,
	                TPT.class_code              AS class_code,
	                TPT.exposure                AS exposure,
	                TPT.sub_line_code           AS sub_line_code,
	                TPT.source_sar_asl          AS source_sar_asl,
	                TPT.source_sar_prdct_line   AS source_sar_prdct_line,
	                TPT.source_sar_sp_use_code  AS source_sar_sp_use_code,
	                TPT.source_statistical_code AS source_statistical_code,
	                C.type_bureau_code          AS type_bureau_code,
	                C.major_peril_code          AS major_peril_code,
	                C.major_peril_seq_num       AS major_peril_seq_num,
	                C.ins_line                  AS ins_line,
	                CASE C.loc_unit_num
	                  WHEN 'N/A' THEN '0000'
	                  ELSE C.loc_unit_num
	                END                         AS loc_unit_num,
	                CASE C.sub_loc_unit_num
	                  WHEN 'N/A' THEN '000'
	                  ELSE C.sub_loc_unit_num
	                END                         AS sub_loc_unit_num,
	                CASE C.risk_unit_grp
	                  WHEN 'N/A' THEN '000'
	                  ELSE C.risk_unit_grp
	                END                         AS risk_unit_grp,
	                C.risk_unit_grp_seq_num     AS risk_unit_grp_seq_num,
	                Rtrim(C.risk_unit)          AS risk_unit,
	                CASE C.risk_unit_seq_num
	                  WHEN 'N/A' THEN '00'
	                  ELSE C.risk_unit_seq_num
	                END                         AS risk_unit_seq_num,
	                P.pms_pol_lob_code          AS pms_pol_lob_code,
	                P.variation_code            AS variation_code
	FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.coverage C,
	       @{pipeline().parameters.TARGET_TABLE_OWNER}.temp_policy_transaction TPT,
	       @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P
	WHERE  C.cov_ak_id = TPT.cov_ak_id
	       AND C.pol_ak_id = P.pol_ak_id
	       AND P.crrnt_snpsht_flag = 1
	       AND C.crrnt_snpsht_flag = 1
	       AND TPT.crrnt_snpsht_flag = 1
	       AND P.pol_ak_id 
	      IN
	 (SELECT DISTINCT pol_key_ak_id
	                           FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence CO,
	                                  @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD,
	                                  @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence CPO
	                           WHERE  CCD.claim_party_occurrence_ak_id = CPO.claim_party_occurrence_ak_id
	                                  AND CPO.claim_occurrence_ak_id = CO.claim_occurrence_ak_id
	                                  AND CO.crrnt_snpsht_flag = 1
	                                  AND CPO.crrnt_snpsht_flag = 1
	                                  AND CCD.crrnt_snpsht_flag = 1
	                                  AND CCD.created_date > '@{pipeline().parameters.SELECTION_START_TS}'
	
	
	                           UNION
	
	                           SELECT DISTINCT C.pol_ak_id
	                           FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.coverage C,
	                                  @{pipeline().parameters.TARGET_TABLE_OWNER}.temp_policy_transaction TPT
	                           WHERE  C.cov_ak_id = TPT.cov_ak_id
	                                  AND C.crrnt_snpsht_flag = 1
	                                  AND TPT.crrnt_snpsht_flag = 1
	                                  AND TPT.created_date > '@{pipeline().parameters.SELECTION_START_TS}'
	
	                           UNION
	
	                           SELECT DISTINCT C.pol_ak_id
	                           FROM   V2.coverage C
	                           WHERE  C.created_date > '@{pipeline().parameters.SELECTION_START_TS}'
	)
),
EXP_source_temp_policy_transaction AS (
	SELECT
	pol_ak_id,
	risk_state_prov_code,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(risk_state_prov_code),'N/A',
	-- IS_SPACES(risk_state_prov_code),'N/A',
	-- LENGTH(risk_state_prov_code)=0,'N/A',
	-- LTRIM(RTRIM(risk_state_prov_code)))
	DECODE(TRUE,
		risk_state_prov_code IS NULL, 'N/A',
		LENGTH(risk_state_prov_code)>0 AND TRIM(risk_state_prov_code)='', 'N/A',
		LENGTH(risk_state_prov_code
		) = 0, 'N/A',
		LTRIM(RTRIM(risk_state_prov_code
			)
		)
	) AS risk_state_prov_code_out,
	risk_zip_code,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(risk_zip_code),'N/A',
	-- IS_SPACES(risk_zip_code),'N/A',
	-- LENGTH(risk_zip_code)=0,'N/A',
	-- LTRIM(RTRIM(risk_zip_code)))
	DECODE(TRUE,
		risk_zip_code IS NULL, 'N/A',
		LENGTH(risk_zip_code)>0 AND TRIM(risk_zip_code)='', 'N/A',
		LENGTH(risk_zip_code
		) = 0, 'N/A',
		LTRIM(RTRIM(risk_zip_code
			)
		)
	) AS risk_zip_code_out,
	terr_code,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(terr_code),'N/A',
	-- IS_SPACES(terr_code),'N/A',
	-- LENGTH(terr_code)=0,'N/A',
	-- LTRIM(RTRIM(terr_code)))
	DECODE(TRUE,
		terr_code IS NULL, 'N/A',
		LENGTH(terr_code)>0 AND TRIM(terr_code)='', 'N/A',
		LENGTH(terr_code
		) = 0, 'N/A',
		LTRIM(RTRIM(terr_code
			)
		)
	) AS terr_code_out,
	tax_loc,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRING_NUMERIC(tax_loc)
	:UDF.DEFAULT_VALUE_FOR_STRING_NUMERIC(tax_loc
	) AS tax_loc_out,
	class_code,
	exposure,
	sub_line_code,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(sub_line_code),'N/A',
	-- IS_SPACES(sub_line_code),'N/A',
	-- LENGTH(sub_line_code)=0,'N/A',
	-- LTRIM(RTRIM(sub_line_code)))
	DECODE(TRUE,
		sub_line_code IS NULL, 'N/A',
		LENGTH(sub_line_code)>0 AND TRIM(sub_line_code)='', 'N/A',
		LENGTH(sub_line_code
		) = 0, 'N/A',
		LTRIM(RTRIM(sub_line_code
			)
		)
	) AS sub_line_code_out,
	source_sar_asl,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(source_sar_asl),'N/A',
	-- IS_SPACES(source_sar_asl),'N/A',
	-- LENGTH(source_sar_asl)=0,'N/A',
	-- LTRIM(RTRIM(REPLACECHR(TRUE, source_sar_asl, '.' , ''))))
	-- 
	-- 
	DECODE(TRUE,
		source_sar_asl IS NULL, 'N/A',
		LENGTH(source_sar_asl)>0 AND TRIM(source_sar_asl)='', 'N/A',
		LENGTH(source_sar_asl
		) = 0, 'N/A',
		LTRIM(RTRIM(REGEXP_REPLACE(source_sar_asl,'.','')
			)
		)
	) AS source_sar_asl_out,
	source_sar_prdct_line,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(source_sar_prdct_line),'N/A',
	-- IS_SPACES(source_sar_prdct_line),'N/A',
	-- LENGTH(source_sar_prdct_line)=0,'N/A',
	-- LTRIM(RTRIM(source_sar_prdct_line)))
	DECODE(TRUE,
		source_sar_prdct_line IS NULL, 'N/A',
		LENGTH(source_sar_prdct_line)>0 AND TRIM(source_sar_prdct_line)='', 'N/A',
		LENGTH(source_sar_prdct_line
		) = 0, 'N/A',
		LTRIM(RTRIM(source_sar_prdct_line
			)
		)
	) AS source_sar_prdct_line_out,
	source_sar_sp_use_code,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(source_sar_sp_use_code),'N/A',
	-- IS_SPACES(source_sar_sp_use_code),'N/A',
	-- LENGTH(source_sar_sp_use_code)=0,'N/A',
	-- source_sar_sp_use_code)
	-- 
	-- -- we are not using LTRIM ,RTRIM  functions because we need to spaces as it is as they are used for IBS Bureau Reporting.
	DECODE(TRUE,
		source_sar_sp_use_code IS NULL, 'N/A',
		LENGTH(source_sar_sp_use_code)>0 AND TRIM(source_sar_sp_use_code)='', 'N/A',
		LENGTH(source_sar_sp_use_code
		) = 0, 'N/A',
		source_sar_sp_use_code
	) AS source_sar_sp_use_code_out,
	'N/A' AS auto_reins_facility_out,
	-- *INF*: substr(source_sar_prdct_line,1,2)
	substr(source_sar_prdct_line, 1, 2
	) AS v_statistical_brkdwn_line,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_statistical_brkdwn_line)
	-- 
	-- 
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_statistical_brkdwn_line
	) AS statistical_brkdwn_line_out,
	source_statistical_code,
	type_bureau_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(type_bureau_code)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(type_bureau_code
	) AS v_type_bureau_code,
	v_type_bureau_code AS type_bureau_out,
	major_peril_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(major_peril_code)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(major_peril_code
	) AS major_peril_code_out,
	major_peril_seq_num,
	-- *INF*: DECODE(TRUE,
	-- IN( v_type_bureau_code,'AL','LP','AI','LI','RL'), '100',
	-- IN( v_type_bureau_code,'GS','GM','RG'),'400',
	-- IN( v_type_bureau_code,'WC','WP'),'500',
	-- IN( v_type_bureau_code,'GL','GI','GN','RQ'),'600',
	-- IN( v_type_bureau_code,'FF','FM','BF','BP','FT','FP'),'711',
	-- IN( v_type_bureau_code,'BD'),'722',
	-- IN( v_type_bureau_code,'BI','BT','RB'),'800'
	-- ,'N/A')
	DECODE(TRUE,
		v_type_bureau_code IN ('AL','LP','AI','LI','RL'), '100',
		v_type_bureau_code IN ('GS','GM','RG'), '400',
		v_type_bureau_code IN ('WC','WP'), '500',
		v_type_bureau_code IN ('GL','GI','GN','RQ'), '600',
		v_type_bureau_code IN ('FF','FM','BF','BP','FT','FP'), '711',
		v_type_bureau_code IN ('BD'), '722',
		v_type_bureau_code IN ('BI','BT','RB'), '800',
		'N/A'
	) AS v_statistical_line,
	v_statistical_line AS statistical_line,
	ins_line,
	loc_unit_num,
	sub_loc_unit_num,
	risk_unit_grp,
	risk_unit_grp_seq_num,
	risk_unit,
	risk_unit_seq_num,
	pms_pol_lob_code,
	variation_code,
	-- *INF*: IIF(IN(pms_pol_lob_code,'ACA','AFA','APA','ATA','ACJ','AFJ','APJ'),'6',variation_code)
	IFF(pms_pol_lob_code IN ('ACA','AFA','APA','ATA','ACJ','AFJ','APJ'),
		'6',
		variation_code
	) AS variation_code_Out
	FROM SQ_temp_policy_transaction
),
EXP_Transform_Statistical_Codes AS (
	SELECT
	source_statistical_code AS statistical_code,
	major_peril_code_out AS major_peril,
	type_bureau_out AS Type_Bureau,
	class_code AS sar_class_code,
	-- *INF*: statistical_code
	-- 
	-- --DECODE(TRUE, Type_Bureau = 'BE', ' '  || statistical_code,
	-- --Type_Bureau = 'BF', ' '  || statistical_code,
	-- --Type_Bureau = 'RP' AND major_peril = '145', ' '  || statistical_code,
	-- --Type_Bureau = 'RL' AND major_peril = '114', '  '  || statistical_code,
	-- --Type_Bureau = 'RL' AND major_peril = '119', '     '  || statistical_code,
	-- --statistical_code)
	-- 
	-- ---- Had to introduce space at the begining of the field because of LTRIM(RTRIM)) to statistical codes in Temp_Policy_transaction Table.
	statistical_code AS v_statistical_code,
	'D' AS v_stat_plan_id,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,1,1))=0,' ',SUBSTR(v_statistical_code,1,1))
	-- 
	-- ----8/18/2011 Uma Bollu - Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 1, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 1, 1
		)
	) AS v_pos_1,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,2,1))=0,' ',SUBSTR(v_statistical_code,2,1))
	-- 
	-- ----Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 2, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 2, 1
		)
	) AS v_pos_2,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,3,1))=0,' ',SUBSTR(v_statistical_code,3,1))
	-- 
	-- ----Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 3, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 3, 1
		)
	) AS v_pos_3,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,4,1))=0,' ',SUBSTR(v_statistical_code,4,1))
	-- 
	-- ----Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 4, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 4, 1
		)
	) AS v_pos_4,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,5,1))=0,' ',SUBSTR(v_statistical_code,5,1))
	-- 
	-- ----8/18/2011 Uma Bollu - Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 5, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 5, 1
		)
	) AS v_pos_5,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,6,1))=0,' ',SUBSTR(v_statistical_code,6,1))
	-- 
	-- 
	-- ----8/18/2011 Uma Bollu - Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 6, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 6, 1
		)
	) AS v_pos_6,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,7,1))=0,' ',SUBSTR(v_statistical_code,7,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 7, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 7, 1
		)
	) AS v_pos_7,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,8,1))=0,' ',SUBSTR(v_statistical_code,8,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 8, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 8, 1
		)
	) AS v_pos_8,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,9,1))=0,' ',SUBSTR(v_statistical_code,9,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 9, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 9, 1
		)
	) AS v_pos_9,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,10,1))=0,' ',SUBSTR(v_statistical_code,10,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 10, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 10, 1
		)
	) AS v_pos_10,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,11,1))=0,' ',SUBSTR(v_statistical_code,11,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 11, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 11, 1
		)
	) AS v_pos_11,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,12,1))=0,' ',SUBSTR(v_statistical_code,12,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 12, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 12, 1
		)
	) AS v_pos_12,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,13,1))=0,' ',SUBSTR(v_statistical_code,13,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 13, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 13, 1
		)
	) AS v_pos_13,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,14,1))=0,' ',SUBSTR(v_statistical_code,14,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 14, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 14, 1
		)
	) AS v_pos_14,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,15,1))=0,' ',SUBSTR(v_statistical_code,15,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 15, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 15, 1
		)
	) AS v_pos_15,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,16,1))=0,' ',SUBSTR(v_statistical_code,16,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 16, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 16, 1
		)
	) AS v_pos_16,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,17,1))=0,' ',SUBSTR(v_statistical_code,17,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 17, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 17, 1
		)
	) AS v_pos_17,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,18,1))=0,' ',SUBSTR(v_statistical_code,18,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 18, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 18, 1
		)
	) AS v_pos_18,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,19,1))=0,' ',SUBSTR(v_statistical_code,19,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 19, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 19, 1
		)
	) AS v_pos_19,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,20,1))=0,' ',SUBSTR(v_statistical_code,20,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 20, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 20, 1
		)
	) AS v_pos_20,
	-- *INF*: DECODE(TRUE,Type_Bureau='RP','0',
	-- LENGTH(SUBSTR(v_statistical_code,21,1))=0,' ',SUBSTR(v_statistical_code,21,1))
	-- --IIF(LENGTH(SUBSTR(v_statistical_code,21,1))=0,' ',SUBSTR(v_statistical_code,21,1))
	-- 
	-- 
	-- --- Statistical Code field is initialised at the begining of the WMM01A0 module to all spaces but since it is a sign field for Type Bureau of RP, these are defaulted to '0'
	DECODE(TRUE,
		Type_Bureau = 'RP', '0',
		LENGTH(SUBSTR(v_statistical_code, 21, 1
			)
		) = 0, ' ',
		SUBSTR(v_statistical_code, 21, 1
		)
	) AS v_pos_21,
	-- *INF*: DECODE(TRUE,Type_Bureau='RP','0',
	-- LENGTH(SUBSTR(v_statistical_code,22,1))=0,' ',SUBSTR(v_statistical_code,22,1))
	-- 
	-- --IIF(LENGTH(SUBSTR(v_statistical_code,22,1))=0,' ',SUBSTR(v_statistical_code,22,1))
	-- 
	-- --- Statistical Code field is initialised at the begining of the WMM01A0 module to all spaces but since it is a sign field for Type Bureau of RP, these are defaulted to '0'
	DECODE(TRUE,
		Type_Bureau = 'RP', '0',
		LENGTH(SUBSTR(v_statistical_code, 22, 1
			)
		) = 0, ' ',
		SUBSTR(v_statistical_code, 22, 1
		)
	) AS v_pos_22,
	-- *INF*: DECODE(TRUE,Type_Bureau='RP','0',
	-- LENGTH(SUBSTR(v_statistical_code,23,1))=0,' ',SUBSTR(v_statistical_code,23,1))
	-- 
	-- --IIF(LENGTH(SUBSTR(v_statistical_code,23,1))=0,' ',SUBSTR(v_statistical_code,23,1))
	-- 
	-- --- Statistical Code field is initialised at the begining of the WMM01A0 module to all spaces but since it is a sign field for Type Bureau of RP, these are defaulted to '0'
	DECODE(TRUE,
		Type_Bureau = 'RP', '0',
		LENGTH(SUBSTR(v_statistical_code, 23, 1
			)
		) = 0, ' ',
		SUBSTR(v_statistical_code, 23, 1
		)
	) AS v_pos_23,
	-- *INF*: DECODE(TRUE,Type_Bureau='RP','0',
	-- LENGTH(SUBSTR(v_statistical_code,24,1))=0,' ',SUBSTR(v_statistical_code,24,1))
	-- 
	-- --IIF(LENGTH(SUBSTR(v_statistical_code,24,1))=0,' ',SUBSTR(v_statistical_code,24,1))
	-- 
	-- --- Statistical Code field is initialised at the begining of the WMM01A0 module to all spaces but since it is a sign field for Type Bureau of RP, these are defaulted to '0'
	DECODE(TRUE,
		Type_Bureau = 'RP', '0',
		LENGTH(SUBSTR(v_statistical_code, 24, 1
			)
		) = 0, ' ',
		SUBSTR(v_statistical_code, 24, 1
		)
	) AS v_pos_24,
	-- *INF*: DECODE(TRUE,Type_Bureau='RP','{',
	-- LENGTH(SUBSTR(v_statistical_code,25,1))=0,' ',SUBSTR(v_statistical_code,25,1))
	-- 
	-- --- IN COBOL "{" represents a  +ve sign and "}" is -ve sign, since this is base rate for Type_Bureau RP is a sign field so COBOL creates "{". Replicating the COBOL logic.
	-- 
	-- --- Statistical Code field is initialised at the begining of the WMM01A0 module to all spaces but since it is a sign field for Type Bureau of RP, these are defaulted to '{'
	-- 
	-- --IIF(LENGTH(SUBSTR(v_statistical_code,25,1))=0,' ',SUBSTR(v_statistical_code,25,1))
	DECODE(TRUE,
		Type_Bureau = 'RP', '{',
		LENGTH(SUBSTR(v_statistical_code, 25, 1
			)
		) = 0, ' ',
		SUBSTR(v_statistical_code, 25, 1
		)
	) AS v_pos_25,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,26,1))=0,' ',SUBSTR(v_statistical_code,26,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 26, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 26, 1
		)
	) AS v_pos_26,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,27,1))=0,' ',SUBSTR(v_statistical_code,27,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 27, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 27, 1
		)
	) AS v_pos_27,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,28,1))=0,' ',SUBSTR(v_statistical_code,28,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 28, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 28, 1
		)
	) AS v_pos_28,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,29,1))=0,' ',SUBSTR(v_statistical_code,29,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 29, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 29, 1
		)
	) AS v_pos_29,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,30,1))=0,' ',SUBSTR(v_statistical_code,30,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 30, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 30, 1
		)
	) AS v_pos_30,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,31,1))=0,' ',SUBSTR(v_statistical_code,31,1))
	-- 
	-- ----8/18/2011 Uma Bollu - Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 31, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 31, 1
		)
	) AS v_pos_31,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,32,1))=0,' ',SUBSTR(v_statistical_code,32,1))
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 32, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 32, 1
		)
	) AS v_pos_32,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,33,1))=0,' ',SUBSTR(v_statistical_code,33,1))
	-- 
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 33, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 33, 1
		)
	) AS v_pos_33,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,34,1))=0,' ',SUBSTR(v_statistical_code,34,1))
	-- 
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 34, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 34, 1
		)
	) AS v_pos_34,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,35,1))=0,' ',SUBSTR(v_statistical_code,35,1))
	-- 
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 35, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 35, 1
		)
	) AS v_pos_35,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,36,1))=0,' ',SUBSTR(v_statistical_code,36,1))
	-- 
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 36, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 36, 1
		)
	) AS v_pos_36,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,37,1))=0,' ',SUBSTR(v_statistical_code,37,1))
	-- 
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 37, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 37, 1
		)
	) AS v_pos_37,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,38,1))=0,' ',SUBSTR(v_statistical_code,38,1))
	-- 
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 38, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 38, 1
		)
	) AS v_pos_38,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38 
	) AS Generic,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 )
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 
	) AS v_Stat_Code_AC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 )
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 
	) AS v_Stat_Codes_AI,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_26 || '       ' || v_pos_25 || v_pos_23  || v_pos_24  || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22)
	-- 
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_26 || '       ' || v_pos_25 || v_pos_23 || v_pos_24 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 
	) AS v_Stat_Codes_AL,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_8 || v_pos_9 || v_pos_10  || v_pos_11|| v_pos_20 || v_pos_21  || 
	-- '             ' ||  v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19  )
	-- 
	--  -----It has a Filler of 13 spaces
	-- --- I have checked this code this is fine
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_20 || v_pos_21 || '             ' || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 
	) AS v_Stat_Codes_AN,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 ||
	-- '      ' || v_pos_14 || v_pos_23  || v_pos_24  || '  '  ||  v_pos_26  || v_pos_27  || v_pos_28  || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || '      ' || v_pos_14 || v_pos_23 || v_pos_24 || '  ' || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 
	) AS v_Stat_Codes_AP,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_11 || v_pos_10 || 
	--   v_pos_12 || v_pos_13 )
	-- 
	-- --- Verified the logic
	-- 
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_11 || v_pos_10 || v_pos_12 || v_pos_13 
	) AS v_Stat_Codes_A2,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_11 || v_pos_12 )
	-- 
	-- --- Verified logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_11 || v_pos_12 
	) AS v_Stat_Codes_A3,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 ||
	-- '           '  ||  v_pos_22 || v_pos_29 || '  ' || v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28)
	-- 
	-- --- Verified logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || '           ' || v_pos_22 || v_pos_29 || '  ' || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 
	) AS v_Stat_Codes_BB,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17  || v_pos_20  || v_pos_27  || v_pos_28  || v_pos_29 || '    ' ||v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26 )
	-- 
	-- 
	-- -- Verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_20 || v_pos_27 || v_pos_28 || v_pos_29 || '    ' || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 
	) AS v_Stat_Codes_BC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_5  || v_pos_6 || v_pos_7)
	-- 
	-- --- Verified logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_5 || v_pos_6 || v_pos_7 
	) AS v_Stat_Codes_BD,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 ||  v_pos_6 || v_pos_7 || '                    ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13)
	-- 
	-- 
	--  ---  Verified Logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_6 || v_pos_7 || '                    ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 
	) AS v_Stat_Codes_BE,
	-- *INF*: ('  '  || v_pos_4  || v_pos_5 || ' ' || v_pos_14 || '  ' || v_pos_15 || v_pos_16 || '   ' ||  v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || '     ' || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22 || '   ')
	-- 
	-- 
	-- --8/22/2011 - Added 2 spaces in the beginning. In COBOL, statitistical code field is initialised to spaces at the start of reformatting. If there is no code to move certain fields then the spaces stay as it is except other fileds are layed out over spaces.
	-- --- Verified the logic
	-- 
	( '  ' || v_pos_4 || v_pos_5 || ' ' || v_pos_14 || '  ' || v_pos_15 || v_pos_16 || '   ' || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || '     ' || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || '   ' 
	) AS v_Stat_Codes_BF,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_4  || v_pos_5)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_4 || v_pos_5 
	) AS v_Stat_Codes_BP,
	-- *INF*: (v_pos_1 || v_pos_2 )
	-- 
	-- --- Verified the logic
	( v_pos_1 || v_pos_2 
	) AS v_Stat_Codes_BI,
	-- *INF*: v_pos_1
	-- 
	-- -- verified the logic
	v_pos_1 AS v_Stat_Codes_BL,
	-- *INF*: (SUBSTR(sar_class_code,1,3) || '  ' || v_pos_18  ||  v_pos_19 || v_pos_1 ||  ' ' ||  v_pos_2 || v_pos_3 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 
	-- || '    ' ||  v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34 || v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28 || '   ' )
	-- 
	-- --- Verfied the logic
	( SUBSTR(sar_class_code, 1, 3
		) || '  ' || v_pos_18 || v_pos_19 || v_pos_1 || ' ' || v_pos_2 || v_pos_3 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || '    ' || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || '   ' 
	) AS v_Stat_Codes_BM,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || '      '  ||  v_pos_8 || v_pos_9 || '           ' || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19)
	-- 
	--  ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || '      ' || v_pos_8 || v_pos_9 || '           ' || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 
	) AS v_Stat_Codes_BT,
	-- *INF*: (v_pos_1 || v_pos_2 || '      '  || v_pos_9 || v_pos_10 || v_pos_17 || v_pos_18 ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31)
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || '      ' || v_pos_9 || v_pos_10 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 
	) AS v_Stat_Codes_B2,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17)
	-- 
	-- ----- verified the logic
	-- 
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 
	) AS v_Stat_Codes_CC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || 
	--  v_pos_17 || v_pos_18  || ' ' ||  v_pos_20 || '              ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 )
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_17 || v_pos_18 || ' ' || v_pos_20 || '              ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 
	) AS v_Stat_Codes_CF,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
	-- 
	-- ---- Generic 
	-- -- No Change from Input copybook to Output
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38 
	) AS v_Stat_Code_CR,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_6 || v_pos_7 || ' '  || v_pos_9 || '  ' || v_pos_12 || ' ' || v_pos_14 || v_pos_15 )
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_6 || v_pos_7 || ' ' || v_pos_9 || '  ' || v_pos_12 || ' ' || v_pos_14 || v_pos_15 
	) AS v_Stat_Codes_CI,
	-- *INF*: (v_pos_1 || v_pos_4  || v_pos_6 || v_pos_7 )
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_4 || v_pos_6 || v_pos_7 
	) AS v_Stat_Codes_CL,
	-- *INF*: ('  ' || v_pos_1 || v_pos_2 || v_pos_5  || v_pos_6 || v_pos_7)
	-- 
	-- ---- verified the logic
	( '  ' || v_pos_1 || v_pos_2 || v_pos_5 || v_pos_6 || v_pos_7 
	) AS v_Stat_Codes_CP,
	-- *INF*: (v_pos_3 || v_pos_4  || v_pos_5 )
	-- 
	-- ---- verified the logic
	( v_pos_3 || v_pos_4 || v_pos_5 
	) AS v_Stat_Codes_CN,
	-- *INF*: v_pos_1
	-- 
	-- -----
	v_pos_1 AS v_Stat_Codes_EI,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_6 || v_pos_7 || '                   ' ||v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16)
	-- 
	-- ---- verified the logic
	-- --- 19 spaces
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_7 || '                   ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 
	) AS v_Stat_Codes_EQ,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 )
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 
	) AS v_Stat_Codes_FC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 
	-- || '                  ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 )
	-- 
	-- ---- verified the logic
	-- ---- 18 Spaces
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || '                  ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 
	) AS v_Stat_Codes_FF,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5)
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 
	) AS v_Stat_Codes_FM,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || '                   ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16)
	-- 
	-- ---- verified the logic
	-- --- 19 spaces
	-- 
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || '                   ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 
	) AS v_Stat_Codes_FO,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3)
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 
	) AS v_Stat_Codes_FP,
	-- *INF*: (v_pos_1 || v_pos_2 || ' ' || v_pos_3 || '  ' || v_pos_6 || v_pos_7 || '   ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 ||
	-- '       ' || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22 || '   ')
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || ' ' || v_pos_3 || '  ' || v_pos_6 || v_pos_7 || '   ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || '       ' || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || '   ' 
	) AS v_Stat_Codes_FT,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_12 || v_pos_13 || '                '  || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9)
	-- 
	-- ---- verified the logic
	-- -- 17 Spaces
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_12 || v_pos_13 || '                ' || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 
	) AS v_Stat_Codes_GI,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_6 || v_pos_7 || v_pos_4  || v_pos_5  || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || '      ' || v_pos_13 || v_pos_29  || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19 ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28)
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_6 || v_pos_7 || v_pos_4 || v_pos_5 || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || '      ' || v_pos_13 || v_pos_29 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 
	) AS v_Stat_Codes_GL,
	-- *INF*: (v_pos_1 || '           '  ||   v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7)
	-- 
	-- ---- verified the logic
	-- 
	( v_pos_1 || '           ' || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 
	) AS v_Stat_Codes_GP,
	-- *INF*: (v_pos_1 || '                       ' || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_11 || v_pos_12 || v_pos_13)
	-- 
	-- ---- verified the logic
	-- --- 23 spaces
	-- 
	-- 
	-- 
	( v_pos_1 || '                       ' || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_11 || v_pos_12 || v_pos_13 
	) AS v_Stat_Codes_GS,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_10 || ' ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_18  ||  v_pos_19  
	-- || ' ' || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || '                ')
	-- 
	-- 
	-- ---- verified the logic
	-- --- 16 Spaces at the end
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_10 || ' ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_18 || v_pos_19 || ' ' || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || '                ' 
	) AS v_Stat_Codes_HO,
	-- *INF*: ('        ' || v_pos_11 || v_pos_12 || '               '  || v_pos_4  || v_pos_5  || v_pos_6  || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_14 || v_pos_15 || v_pos_17)
	-- 
	-- ---- verified the logic
	( '        ' || v_pos_11 || v_pos_12 || '               ' || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_14 || v_pos_15 || v_pos_17 
	) AS v_Stat_Codes_IM,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  || v_pos_24  || v_pos_25  || v_pos_26 || v_pos_28  || v_pos_29  || v_pos_30 || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35)
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 
	) AS v_Stat_Codes_JR,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  )
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 
	) AS v_Stat_Codes_ME,
	-- *INF*: (v_pos_1 || ' '  || v_pos_3 || v_pos_4  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_10 || ' ' ||  v_pos_11 || v_pos_12 || v_pos_13 || '  ' || v_pos_18  || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || ' ' || '                ' ) 
	-- 
	-- --- need logic for stat-plan -id
	-- ---- 16 Spaces at the end
	( v_pos_1 || ' ' || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_10 || ' ' || v_pos_11 || v_pos_12 || v_pos_13 || '  ' || v_pos_18 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || ' ' || '                ' 
	) AS v_Stat_Codes_MH,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || '                  '  || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7)
	-- 
	--  --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || '                  ' || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 
	) AS v_Stat_Codes_MI,
	-- *INF*: (v_pos_6 || v_pos_7 || v_pos_3 || v_pos_4  || v_pos_2 || '      ' || v_pos_1 || '        ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || ' ' ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24 )
	-- 
	--  --- verified the logic
	( v_pos_6 || v_pos_7 || v_pos_3 || v_pos_4 || v_pos_2 || '      ' || v_pos_1 || '        ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || ' ' || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 
	) AS v_Stat_Codes_ML,
	-- *INF*: -- No Stats code in the Output Copybook just the policy_type logic
	'' AS v_Stat_Codes_MP,
	-- *INF*: (SUBSTR(sar_class_code,1,3) || v_pos_17 || v_pos_18 ||  v_pos_19  || v_pos_1 || v_pos_2 || v_pos_3 || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || '   ' || '       ' || '      ' )
	-- 
	-- --- Need to look at complete logic
	-- 
	( SUBSTR(sar_class_code, 1, 3
		) || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_1 || v_pos_2 || v_pos_3 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || '   ' || '       ' || '      ' 
	) AS v_Stat_Codes_M2,
	-- *INF*: ( '                 ' || v_stat_plan_id)
	-- 
	-- ----verified the logic
	( '                 ' || v_stat_plan_id 
	) AS v_Stat_Codes_NE,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  ||  v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_19)
	-- 
	-- --- Verified the Logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_19 
	) AS v_Stat_Codes_PC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || ' ' || v_pos_19  || v_pos_20  ||  v_pos_21)
	-- 
	-- --- verified the logic
	--  
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || ' ' || v_pos_19 || v_pos_20 || v_pos_21 
	) AS v_Stat_Codes_PH,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38 
	) AS v_Stat_Code_PF,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38 
	) AS v_Stat_Code_PI,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38 
	) AS v_Stat_Code_PL,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 ||  v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 
	) AS v_Stat_Codes_PM,
	-- *INF*: (v_pos_1 || v_pos_2)
	-- 
	-- --- verified the logic
	-- 
	( v_pos_1 || v_pos_2 
	) AS v_Stat_Codes_RB,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 
	) AS v_Stat_Codes_RG,
	-- *INF*: (v_pos_1 || v_pos_2)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 
	) AS v_Stat_Codes_RI,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 
	) AS v_Stat_Codes_RL,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_8 || v_pos_9 || v_pos_10)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_8 || v_pos_9 || v_pos_10 
	) AS v_Stat_Codes_RM,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || ' ' || 
	-- v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21 || v_pos_22 ||  v_pos_23  || v_pos_24)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || ' ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 
	) AS v_Stat_Codes_RN,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29 || v_pos_30 || v_pos_31|| v_pos_33 || v_pos_34  ||  v_pos_35  || v_pos_32)
	-- 
	-- ----
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_32 
	) AS v_Stat_Codes_RP,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5 )
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 
	) AS v_Stat_Codes_RQ,
	-- *INF*: (v_pos_1 || ' ' || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 )
	-- 
	-- --- verified the logic
	( v_pos_1 || ' ' || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 
	) AS v_Stat_Codes_SM,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_6 || v_pos_8 || v_pos_11 || v_pos_9)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_8 || v_pos_11 || v_pos_9 
	) AS v_Stat_Codes_TH,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 
	-- || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19
	-- ||  v_pos_22  ||  v_pos_23  || v_pos_24 || '       ' || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_22 || v_pos_23 || v_pos_24 || '       ' || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 
	) AS v_Stat_Codes_VL,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19 
	--  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30 || ' ' || v_pos_32  ||  v_pos_33
	-- || v_pos_34  ||  v_pos_35  || v_pos_36 )
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || ' ' || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 
	) AS v_Stat_Codes_VP,
	-- *INF*: ('   ' || v_pos_4  || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12  || ' ' || v_pos_14 || v_pos_15 || '              ' 
	-- || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34 || v_pos_35)
	-- 
	-- --- verified the logic
	( '   ' || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || ' ' || v_pos_14 || v_pos_15 || '              ' || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 
	) AS v_Stat_Codes_VN,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  
	-- || ' ' || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || '    ' || v_pos_36 || v_pos_37  || v_pos_38)
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || ' ' || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || '    ' || v_pos_36 || v_pos_37 || v_pos_38 
	) AS v_Stat_Codes_VC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31)
	-- 
	--  --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 
	) AS v_Stat_Codes_WC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38 
	) AS v_Stat_Code_WP,
	-- *INF*: ('   ' || v_pos_1 || v_pos_2 || '            ' || v_stat_plan_id)
	-- 
	-- --8/19/2011 Added v_stat_plan_id
	-- --- need to bring stat plan_id
	--  --- verified the logic but need stat plan id
	-- 
	( '   ' || v_pos_1 || v_pos_2 || '            ' || v_stat_plan_id 
	) AS v_Stat_Codes_WL,
	-- *INF*: DECODE(Type_Bureau, 'AC', v_Stat_Code_AC, 'AI', v_Stat_Codes_AI, 'AL', v_Stat_Codes_AL, 'AN', v_Stat_Codes_AN, 'AP', v_Stat_Codes_AP, 'A2', v_Stat_Codes_A2, 'A3', v_Stat_Codes_A3, 'BB', v_Stat_Codes_BB, 'BC', v_Stat_Codes_BC, 'BD', v_Stat_Codes_BD, 'BE', v_Stat_Codes_BE, 'BF', v_Stat_Codes_BF, 'BP', v_Stat_Codes_BP, 'BI', v_Stat_Codes_BI, 'BL', v_Stat_Codes_BL, 'BM', v_Stat_Codes_BM, 'BT', v_Stat_Codes_BT, 'B2', v_Stat_Codes_B2, 'CC', v_Stat_Codes_CC, 'CF', v_Stat_Codes_CF, 'CI', v_Stat_Codes_CI, 'CL', v_Stat_Codes_CL, 'CN', v_Stat_Codes_CN, 'CP', v_Stat_Codes_CP, 'EI', v_Stat_Codes_EI, 'EQ', v_Stat_Codes_EQ, 'FC', v_Stat_Codes_FC, 'FF', v_Stat_Codes_FF, 'FM', v_Stat_Codes_FM, 'FO', v_Stat_Codes_FO, 'FP', v_Stat_Codes_FP, 'FT', v_Stat_Codes_FT, 'GI', v_Stat_Codes_GI, 'GL', v_Stat_Codes_GL, 'GP', v_Stat_Codes_GP, 'GS', v_Stat_Codes_GS, 'HO', v_Stat_Codes_HO, 'IM', v_Stat_Codes_IM, 'JR', v_Stat_Codes_JR, 'ME', v_Stat_Codes_ME, 'MH', v_Stat_Codes_MH, 'MI', v_Stat_Codes_MI, 'ML',
	-- v_Stat_Codes_ML, 'MP', v_Stat_Codes_MP, 'M2', v_Stat_Codes_M2, 'NE', v_Stat_Codes_NE, 'PC', v_Stat_Codes_PC, 'PH', v_Stat_Codes_PH, 'PM', v_Stat_Codes_PM, 'RB', v_Stat_Codes_RB, 'RG', v_Stat_Codes_RG, 'RI', v_Stat_Codes_RI, 'RL', v_Stat_Codes_RL, 'RM', v_Stat_Codes_RM, 'RN', v_Stat_Codes_RN, 'RP', v_Stat_Codes_RP, 'RQ', v_Stat_Codes_RQ, 'SM', v_Stat_Codes_SM, 'TH', v_Stat_Codes_TH, 'VL', v_Stat_Codes_VL, 'VP', v_Stat_Codes_VP, 'VN', v_Stat_Codes_VN, 'VC', v_Stat_Codes_VC, 'WC', v_Stat_Codes_WC, 'WL', v_Stat_Codes_WL,
	-- 'CR', v_Stat_Code_CR, 'PF', v_Stat_Code_PF,'PI', v_Stat_Code_PI, 'PL', v_Stat_Code_PL,
	-- 'WP', v_Stat_Code_WP,v_statistical_code) 
	DECODE(Type_Bureau,
		'AC', v_Stat_Code_AC,
		'AI', v_Stat_Codes_AI,
		'AL', v_Stat_Codes_AL,
		'AN', v_Stat_Codes_AN,
		'AP', v_Stat_Codes_AP,
		'A2', v_Stat_Codes_A2,
		'A3', v_Stat_Codes_A3,
		'BB', v_Stat_Codes_BB,
		'BC', v_Stat_Codes_BC,
		'BD', v_Stat_Codes_BD,
		'BE', v_Stat_Codes_BE,
		'BF', v_Stat_Codes_BF,
		'BP', v_Stat_Codes_BP,
		'BI', v_Stat_Codes_BI,
		'BL', v_Stat_Codes_BL,
		'BM', v_Stat_Codes_BM,
		'BT', v_Stat_Codes_BT,
		'B2', v_Stat_Codes_B2,
		'CC', v_Stat_Codes_CC,
		'CF', v_Stat_Codes_CF,
		'CI', v_Stat_Codes_CI,
		'CL', v_Stat_Codes_CL,
		'CN', v_Stat_Codes_CN,
		'CP', v_Stat_Codes_CP,
		'EI', v_Stat_Codes_EI,
		'EQ', v_Stat_Codes_EQ,
		'FC', v_Stat_Codes_FC,
		'FF', v_Stat_Codes_FF,
		'FM', v_Stat_Codes_FM,
		'FO', v_Stat_Codes_FO,
		'FP', v_Stat_Codes_FP,
		'FT', v_Stat_Codes_FT,
		'GI', v_Stat_Codes_GI,
		'GL', v_Stat_Codes_GL,
		'GP', v_Stat_Codes_GP,
		'GS', v_Stat_Codes_GS,
		'HO', v_Stat_Codes_HO,
		'IM', v_Stat_Codes_IM,
		'JR', v_Stat_Codes_JR,
		'ME', v_Stat_Codes_ME,
		'MH', v_Stat_Codes_MH,
		'MI', v_Stat_Codes_MI,
		'ML', v_Stat_Codes_ML,
		'MP', v_Stat_Codes_MP,
		'M2', v_Stat_Codes_M2,
		'NE', v_Stat_Codes_NE,
		'PC', v_Stat_Codes_PC,
		'PH', v_Stat_Codes_PH,
		'PM', v_Stat_Codes_PM,
		'RB', v_Stat_Codes_RB,
		'RG', v_Stat_Codes_RG,
		'RI', v_Stat_Codes_RI,
		'RL', v_Stat_Codes_RL,
		'RM', v_Stat_Codes_RM,
		'RN', v_Stat_Codes_RN,
		'RP', v_Stat_Codes_RP,
		'RQ', v_Stat_Codes_RQ,
		'SM', v_Stat_Codes_SM,
		'TH', v_Stat_Codes_TH,
		'VL', v_Stat_Codes_VL,
		'VP', v_Stat_Codes_VP,
		'VN', v_Stat_Codes_VN,
		'VC', v_Stat_Codes_VC,
		'WC', v_Stat_Codes_WC,
		'WL', v_Stat_Codes_WL,
		'CR', v_Stat_Code_CR,
		'PF', v_Stat_Code_PF,
		'PI', v_Stat_Code_PI,
		'PL', v_Stat_Code_PL,
		'WP', v_Stat_Code_WP,
		v_statistical_code
	) AS V_Formatted_Stat_Codes,
	-- *INF*: SUBSTR(V_Formatted_Stat_Codes,1,25)
	SUBSTR(V_Formatted_Stat_Codes, 1, 25
	) AS Formatted_Stat_Codes,
	-- *INF*: SUBSTR(V_Formatted_Stat_Codes,26,9)
	SUBSTR(V_Formatted_Stat_Codes, 26, 9
	) AS Formatted_Stat_Codes_26_34,
	-- *INF*: SUBSTR(V_Formatted_Stat_Codes,35,4)
	SUBSTR(V_Formatted_Stat_Codes, 35, 4
	) AS Formatted_Stat_Codes_34_38,
	-- *INF*: DECODE(Type_Bureau,'AI', (v_pos_11 || v_pos_12),
	-- 'AL', (v_pos_15  ||  v_pos_16),
	-- 'AN',(v_pos_12 || v_pos_13),
	-- 'AP',(v_pos_12 || v_pos_13),
	-- 'A2',(v_pos_8 || v_pos_9),
	-- 'A3',(v_pos_8 || v_pos_9),
	-- 'BB',(v_pos_20 || v_pos_21),
	-- 'BC',(v_pos_18 || v_pos_19),
	-- 'BE', ( v_pos_4  || v_pos_5),
	-- 'BF', (v_pos_1  ||  v_pos_2),
	-- 'BP', (' '  ||  v_pos_2),
	-- 'BI', (v_pos_3 ||  v_pos_4),
	-- 'BL', (v_pos_3  ||  v_pos_4),
	-- 'BM',(v_pos_20 || v_pos_21),
	-- 'BT', (v_pos_11  ||  v_pos_12),
	-- 'B2',(v_pos_14  ||  v_pos_15),
	-- 'CF', (v_pos_8  || v_pos_9),
	-- 'CI',(v_pos_3  ||  v_pos_4),
	-- 'CN', (v_pos_1  ||  v_pos_2),
	-- 'CP', (v_pos_3  ||  v_pos_4),
	-- 'EI', (v_pos_2  ||  v_pos_3),
	-- 'EQ', (v_pos_8  || v_pos_9),
	-- 'FF', (v_pos_8  || v_pos_9),
	-- 'FI', (v_pos_1  ||  v_pos_2),
	-- 'FM', (v_pos_6  ||  v_pos_7),
	-- 'FO', (v_pos_8  || v_pos_9),
	-- 'FP', (v_pos_2  ||  v_pos_3),
	-- 'FT', (v_pos_4  ||  v_pos_5),
	-- 'GI', (v_pos_10  ||  v_pos_11),
	-- 'GL',(v_pos_20 || v_pos_21),
	-- 'GM', (v_pos_1  ||  v_pos_2),
	-- 'GP', (v_pos_8  || v_pos_9),
	-- 'GS',(v_pos_3  ||  v_pos_4),
	-- 'II', (v_pos_1  ||  v_pos_2),
	-- 'IM', (v_pos_1  ||  v_pos_2),
	-- 'MI',(v_pos_10  ||  v_pos_11),
	-- 'ML', (v_pos_16  ||  v_pos_17),
	-- 'MP', (v_pos_1  ||  v_pos_2),
	-- 'M2', (v_pos_15  ||  v_pos_16),'  ')
	-- 
	-- 
	-- 
	-- 
	DECODE(Type_Bureau,
		'AI', ( v_pos_11 || v_pos_12 
		),
		'AL', ( v_pos_15 || v_pos_16 
		),
		'AN', ( v_pos_12 || v_pos_13 
		),
		'AP', ( v_pos_12 || v_pos_13 
		),
		'A2', ( v_pos_8 || v_pos_9 
		),
		'A3', ( v_pos_8 || v_pos_9 
		),
		'BB', ( v_pos_20 || v_pos_21 
		),
		'BC', ( v_pos_18 || v_pos_19 
		),
		'BE', ( v_pos_4 || v_pos_5 
		),
		'BF', ( v_pos_1 || v_pos_2 
		),
		'BP', ( ' ' || v_pos_2 
		),
		'BI', ( v_pos_3 || v_pos_4 
		),
		'BL', ( v_pos_3 || v_pos_4 
		),
		'BM', ( v_pos_20 || v_pos_21 
		),
		'BT', ( v_pos_11 || v_pos_12 
		),
		'B2', ( v_pos_14 || v_pos_15 
		),
		'CF', ( v_pos_8 || v_pos_9 
		),
		'CI', ( v_pos_3 || v_pos_4 
		),
		'CN', ( v_pos_1 || v_pos_2 
		),
		'CP', ( v_pos_3 || v_pos_4 
		),
		'EI', ( v_pos_2 || v_pos_3 
		),
		'EQ', ( v_pos_8 || v_pos_9 
		),
		'FF', ( v_pos_8 || v_pos_9 
		),
		'FI', ( v_pos_1 || v_pos_2 
		),
		'FM', ( v_pos_6 || v_pos_7 
		),
		'FO', ( v_pos_8 || v_pos_9 
		),
		'FP', ( v_pos_2 || v_pos_3 
		),
		'FT', ( v_pos_4 || v_pos_5 
		),
		'GI', ( v_pos_10 || v_pos_11 
		),
		'GL', ( v_pos_20 || v_pos_21 
		),
		'GM', ( v_pos_1 || v_pos_2 
		),
		'GP', ( v_pos_8 || v_pos_9 
		),
		'GS', ( v_pos_3 || v_pos_4 
		),
		'II', ( v_pos_1 || v_pos_2 
		),
		'IM', ( v_pos_1 || v_pos_2 
		),
		'MI', ( v_pos_10 || v_pos_11 
		),
		'ML', ( v_pos_16 || v_pos_17 
		),
		'MP', ( v_pos_1 || v_pos_2 
		),
		'M2', ( v_pos_15 || v_pos_16 
		),
		'  '
	) AS V_Policy_Type,
	V_Policy_Type AS Policy_Type,
	-- *INF*: SUBSTR(sar_class_code,1,3)
	SUBSTR(sar_class_code, 1, 3
	) AS v_sar_class_3,
	-- *INF*: DECODE(TRUE,
	-- IN (Type_Bureau,'BP','FP','BF','FT'),V_Policy_Type)
	DECODE(TRUE,
		Type_Bureau IN ('BP','FP','BF','FT'), V_Policy_Type
	) AS v_type_policy_45,
	-- *INF*: DECODE(TRUE,
	-- Type_Bureau='BP',v_pos_2,
	-- Type_Bureau='BF',v_pos_2,
	-- Type_Bureau='FP',' ',
	-- Type_Bureau='FT',' '  )
	DECODE(TRUE,
		Type_Bureau = 'BP', v_pos_2,
		Type_Bureau = 'BF', v_pos_2,
		Type_Bureau = 'FP', ' ',
		Type_Bureau = 'FT', ' '
	) AS v_type_of_bond_6,
	-- *INF*: DECODE(TRUE,
	--  IN(Type_Bureau,'BP','BF','FP','FT'),v_sar_class_3  || v_type_policy_45 || v_type_of_bond_6,
	-- sar_class_code)
	DECODE(TRUE,
		Type_Bureau IN ('BP','BF','FP','FT'), v_sar_class_3 || v_type_policy_45 || v_type_of_bond_6,
		sar_class_code
	) AS v_hold_sar_class_code,
	v_hold_sar_class_code AS sar_class_code_out
	FROM EXP_source_temp_policy_transaction
),
EXP_pre_join AS (
	SELECT
	EXP_source_temp_policy_transaction.pol_ak_id,
	EXP_source_temp_policy_transaction.loc_unit_num,
	EXP_source_temp_policy_transaction.sub_loc_unit_num,
	EXP_source_temp_policy_transaction.ins_line,
	EXP_source_temp_policy_transaction.risk_unit_grp,
	EXP_source_temp_policy_transaction.risk_unit_grp_seq_num,
	EXP_source_temp_policy_transaction.risk_unit,
	EXP_source_temp_policy_transaction.risk_unit_seq_num,
	-- *INF*: IIF(risk_unit_seq_num='N/A',risk_unit_seq_num,substr(risk_unit_seq_num,1,1))
	-- -- when comparing to claimant coverage detail we can only match on a single length value.
	IFF(risk_unit_seq_num = 'N/A',
		risk_unit_seq_num,
		substr(risk_unit_seq_num, 1, 1
		)
	) AS risk_unit_seq_num_out,
	EXP_source_temp_policy_transaction.major_peril_code_out AS major_peril_code,
	EXP_source_temp_policy_transaction.major_peril_seq_num,
	EXP_source_temp_policy_transaction.risk_state_prov_code_out AS risk_state_prov_code,
	EXP_source_temp_policy_transaction.risk_zip_code_out AS risk_zip_code,
	EXP_source_temp_policy_transaction.terr_code_out AS terr_code,
	EXP_source_temp_policy_transaction.tax_loc_out AS tax_loc,
	EXP_Transform_Statistical_Codes.sar_class_code_out AS class_code,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(class_code),'N/A',
	-- IS_SPACES(class_code),'N/A',
	-- LENGTH(class_code)=0,'N/A',
	-- LTRIM(RTRIM(class_code)))
	DECODE(TRUE,
		class_code IS NULL, 'N/A',
		LENGTH(class_code)>0 AND TRIM(class_code)='', 'N/A',
		LENGTH(class_code
		) = 0, 'N/A',
		LTRIM(RTRIM(class_code
			)
		)
	) AS class_code_out,
	EXP_source_temp_policy_transaction.exposure,
	EXP_source_temp_policy_transaction.sub_line_code_out AS sub_line_code,
	EXP_source_temp_policy_transaction.source_sar_asl_out AS source_sar_asl,
	EXP_source_temp_policy_transaction.source_sar_prdct_line_out AS source_sar_prdct_line,
	EXP_source_temp_policy_transaction.source_sar_sp_use_code_out AS source_sar_sp_use_code,
	EXP_source_temp_policy_transaction.auto_reins_facility_out,
	EXP_source_temp_policy_transaction.statistical_brkdwn_line_out AS statistical_brkdwn_line,
	EXP_source_temp_policy_transaction.source_statistical_code,
	EXP_Transform_Statistical_Codes.Formatted_Stat_Codes,
	-- *INF*: IIF(ISNULL(Formatted_Stat_Codes) OR IS_SPACES(Formatted_Stat_Codes) OR LENGTH(Formatted_Stat_Codes) = 0 , 'N/A', Formatted_Stat_Codes)
	-- 
	-- --- Previously we were using User Defined function but in the user defined function we have a LTRIM, RTRIM which we dont want to do as we want to preserve the spaces since these fields are used for IBS Bureau Reporting.
	-- 
	IFF(Formatted_Stat_Codes IS NULL 
		OR LENGTH(Formatted_Stat_Codes)>0 AND TRIM(Formatted_Stat_Codes)='' 
		OR LENGTH(Formatted_Stat_Codes
		) = 0,
		'N/A',
		Formatted_Stat_Codes
	) AS statistical_code_1,
	EXP_Transform_Statistical_Codes.Formatted_Stat_Codes_26_34,
	-- *INF*: IIF(ISNULL(Formatted_Stat_Codes_26_34) OR IS_SPACES(Formatted_Stat_Codes_26_34) OR LENGTH(Formatted_Stat_Codes_26_34) = 0 , 'N/A', Formatted_Stat_Codes_26_34)
	-- 
	-- 
	-- --:UDF.DEFAULT_VALUE_FOR_STRINGS(Formatted_Stat_Codes_26_34)
	-- 
	-- --- Previously we were using User Defined function but in the user defined function we have a LTRIM, RTRIM which we dont want to do as we want to preserve the spaces since these fields are used for IBS Bureau Reporting.
	IFF(Formatted_Stat_Codes_26_34 IS NULL 
		OR LENGTH(Formatted_Stat_Codes_26_34)>0 AND TRIM(Formatted_Stat_Codes_26_34)='' 
		OR LENGTH(Formatted_Stat_Codes_26_34
		) = 0,
		'N/A',
		Formatted_Stat_Codes_26_34
	) AS statistical_code_2,
	EXP_Transform_Statistical_Codes.Formatted_Stat_Codes_34_38,
	-- *INF*: IIF(ISNULL(Formatted_Stat_Codes_34_38) OR IS_SPACES(Formatted_Stat_Codes_34_38) OR LENGTH(Formatted_Stat_Codes_34_38) = 0 , 'N/A', Formatted_Stat_Codes_34_38)
	-- 
	-- ---:UDF.DEFAULT_VALUE_FOR_STRINGS(Formatted_Stat_Codes_34_38)
	-- 
	-- --- Previously we were using User Defined function but in the user defined function we have a LTRIM, RTRIM which we dont want to do as we want to preserve the spaces since these fields are used for IBS Bureau Reporting.
	IFF(Formatted_Stat_Codes_34_38 IS NULL 
		OR LENGTH(Formatted_Stat_Codes_34_38)>0 AND TRIM(Formatted_Stat_Codes_34_38)='' 
		OR LENGTH(Formatted_Stat_Codes_34_38
		) = 0,
		'N/A',
		Formatted_Stat_Codes_34_38
	) AS statistical_code_3,
	EXP_source_temp_policy_transaction.type_bureau_out AS type_bureau,
	EXP_source_temp_policy_transaction.variation_code_Out AS variation_code,
	EXP_source_temp_policy_transaction.statistical_line,
	EXP_Transform_Statistical_Codes.Policy_Type
	FROM EXP_Transform_Statistical_Codes
	 -- Manually join with EXP_source_temp_policy_transaction
),
JNR_temp_policy_transaction_and_claim_coverage_detail AS (SELECT
	EXP_claimant_coverage_detail_source.pol_key_ak_id, 
	EXP_claimant_coverage_detail_source.loc_unit_num, 
	EXP_claimant_coverage_detail_source.sub_loc_unit_num, 
	EXP_claimant_coverage_detail_source.ins_line, 
	EXP_claimant_coverage_detail_source.risk_unit_grp, 
	EXP_claimant_coverage_detail_source.risk_unit_grp_seq_num, 
	EXP_claimant_coverage_detail_source.risk_unit, 
	EXP_claimant_coverage_detail_source.risk_unit_seq_num_out AS risk_unit_seq_num, 
	EXP_claimant_coverage_detail_source.major_peril_code, 
	EXP_claimant_coverage_detail_source.major_peril_seq, 
	EXP_pre_join.pol_ak_id AS IN_pol_ak_id, 
	EXP_pre_join.loc_unit_num AS IN_loc_unit_num, 
	EXP_pre_join.sub_loc_unit_num AS IN_sub_loc_unit_num, 
	EXP_pre_join.ins_line AS IN_ins_line, 
	EXP_pre_join.risk_unit_grp AS IN_risk_unit_grp, 
	EXP_pre_join.risk_unit_grp_seq_num AS IN_risk_unit_grp_seq_num, 
	EXP_pre_join.risk_unit AS IN_risk_unit, 
	EXP_pre_join.risk_unit_seq_num_out AS IN_risk_unit_seq_num, 
	EXP_pre_join.major_peril_code AS IN_major_peril_code, 
	EXP_pre_join.major_peril_seq_num AS IN_major_peril_seq_num, 
	EXP_pre_join.risk_state_prov_code AS IN_risk_state_prov_code, 
	EXP_pre_join.risk_zip_code AS IN_risk_zip_code, 
	EXP_pre_join.terr_code AS IN_terr_code, 
	EXP_pre_join.tax_loc AS IN_tax_loc, 
	EXP_pre_join.class_code_out AS IN_class_code, 
	EXP_pre_join.exposure AS IN_exposure, 
	EXP_pre_join.sub_line_code AS IN_sub_line_code, 
	EXP_pre_join.source_sar_asl AS IN_source_sar_asl, 
	EXP_pre_join.source_sar_prdct_line AS IN_source_sar_prdct_line, 
	EXP_pre_join.source_sar_sp_use_code AS IN_source_sar_sp_use_code, 
	EXP_pre_join.auto_reins_facility_out AS IN_auto_reins_facility_out, 
	EXP_pre_join.statistical_brkdwn_line AS IN_statistical_brkdwn_line, 
	EXP_pre_join.statistical_code_1 AS IN_statistical_code_1, 
	EXP_pre_join.statistical_code_2 AS IN_statistical_code_2, 
	EXP_pre_join.statistical_code_3 AS IN_statistical_code_3, 
	EXP_pre_join.variation_code AS IN_variation_code_out, 
	EXP_pre_join.statistical_line AS IN_statistical_line, 
	EXP_pre_join.Policy_Type AS IN_Policy_Type
	FROM EXP_pre_join
	INNER JOIN EXP_claimant_coverage_detail_source
	ON EXP_claimant_coverage_detail_source.pol_key_ak_id = EXP_pre_join.pol_ak_id AND EXP_claimant_coverage_detail_source.loc_unit_num = EXP_pre_join.loc_unit_num AND EXP_claimant_coverage_detail_source.sub_loc_unit_num = EXP_pre_join.sub_loc_unit_num AND EXP_claimant_coverage_detail_source.ins_line = EXP_pre_join.ins_line AND EXP_claimant_coverage_detail_source.risk_unit_grp = EXP_pre_join.risk_unit_grp AND EXP_claimant_coverage_detail_source.risk_unit_grp_seq_num = EXP_pre_join.risk_unit_grp_seq_num AND EXP_claimant_coverage_detail_source.risk_unit = EXP_pre_join.risk_unit AND EXP_claimant_coverage_detail_source.risk_unit_seq_num_out = EXP_pre_join.risk_unit_seq_num_out AND EXP_claimant_coverage_detail_source.major_peril_code = EXP_pre_join.major_peril_code AND EXP_claimant_coverage_detail_source.major_peril_seq = EXP_pre_join.major_peril_seq_num
),
EXP_post_join AS (
	SELECT
	IN_major_peril_code AS major_peril_code,
	IN_risk_state_prov_code AS risk_state_prov_code,
	IN_risk_zip_code AS risk_zip_code,
	IN_terr_code AS terr_code,
	IN_tax_loc AS tax_loc,
	IN_class_code AS class_code,
	IN_exposure AS exposure,
	IN_sub_line_code AS sub_line_code,
	IN_source_sar_asl AS source_sar_asl,
	IN_source_sar_prdct_line AS source_sar_prdct_line,
	IN_source_sar_sp_use_code AS source_sar_sp_use_code,
	IN_auto_reins_facility_out AS auto_reins_facility_out,
	IN_statistical_brkdwn_line AS statistical_brkdwn_line,
	IN_statistical_code_1 AS statistical_code_1,
	IN_statistical_code_2 AS statistical_code_2,
	IN_statistical_code_3 AS statistical_code_3,
	IN_variation_code_out AS variation_code_out,
	IN_statistical_line AS statistical_line,
	IN_Policy_Type AS Policy_Type
	FROM JNR_temp_policy_transaction_and_claim_coverage_detail
),
LKP_gtam_08_CoverageLookup AS (
	SELECT
	coverage_code,
	major_peril
	FROM (
		SELECT gtam_tm08_stage.coverage_code as coverage_code, 
		RTRIM(gtam_tm08_stage.major_peril) as major_peril 
		FROM gtam_tm08_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY major_peril ORDER BY coverage_code) = 1
),
AGG_post_join AS (
	SELECT
	EXP_post_join.risk_state_prov_code AS IN_risk_state_prov_code,
	EXP_post_join.risk_zip_code AS IN_risk_zip_code,
	EXP_post_join.terr_code AS IN_terr_code,
	EXP_post_join.tax_loc AS IN_tax_loc,
	EXP_post_join.class_code AS IN_class_code,
	EXP_post_join.exposure AS IN_exposure,
	EXP_post_join.sub_line_code AS IN_sub_line_code,
	EXP_post_join.source_sar_asl AS IN_source_sar_asl,
	EXP_post_join.source_sar_prdct_line AS IN_source_sar_prdct_line,
	EXP_post_join.source_sar_sp_use_code AS IN_source_sar_sp_use_code,
	EXP_post_join.auto_reins_facility_out AS IN_auto_reins_facility_out,
	EXP_post_join.statistical_brkdwn_line AS IN_statistical_brkdwn_line,
	EXP_post_join.statistical_code_1 AS IN_statistical_code_1,
	EXP_post_join.statistical_code_2 AS IN_statistical_code_2,
	EXP_post_join.statistical_code_3 AS IN_statistical_code_3,
	EXP_post_join.variation_code_out AS IN_variation_code,
	EXP_post_join.statistical_line AS IN_statistical_line,
	EXP_post_join.Policy_Type AS IN_Policy_Type,
	LKP_gtam_08_CoverageLookup.coverage_code AS IN_coverage_code
	FROM EXP_post_join
	LEFT JOIN LKP_gtam_08_CoverageLookup
	ON LKP_gtam_08_CoverageLookup.major_peril = EXP_post_join.major_peril_code
	QUALIFY ROW_NUMBER() OVER (PARTITION BY IN_risk_state_prov_code, IN_risk_zip_code, IN_terr_code, IN_tax_loc, IN_class_code, IN_exposure, IN_sub_line_code, IN_source_sar_asl, IN_source_sar_prdct_line, IN_source_sar_sp_use_code, IN_auto_reins_facility_out, IN_statistical_brkdwn_line, IN_statistical_code_1, IN_statistical_code_2, IN_statistical_code_3, IN_variation_code, IN_statistical_line, IN_Policy_Type, IN_coverage_code ORDER BY NULL) = 1
),
EXP_converge AS (
	SELECT
	IN_risk_state_prov_code AS risk_state_prov_code,
	-- *INF*: ltrim(rtrim(risk_state_prov_code))
	ltrim(rtrim(risk_state_prov_code
		)
	) AS risk_state_prov_code_out,
	IN_risk_zip_code AS risk_zip_code,
	-- *INF*: rtrim(ltrim(risk_zip_code))
	rtrim(ltrim(risk_zip_code
		)
	) AS risk_zip_code_out,
	IN_terr_code AS terr_code,
	-- *INF*: ltrim(rtrim(terr_code))
	ltrim(rtrim(terr_code
		)
	) AS terr_code_out,
	IN_tax_loc AS tax_loc,
	-- *INF*: IIF(NOT IS_NUMBER(tax_loc),'N/A',tax_loc)
	-- 
	-- 
	-- ------ Using IS_NUMBER Function so that we can default (0   ) value to N/A. Do not use LTRIM, RTRIM function as we dont want spaces to be removed.
	IFF(NOT REGEXP_LIKE(tax_loc, '^[0-9]+$'),
		'N/A',
		tax_loc
	) AS tax_loc_out,
	-- *INF*: rtrim(ltrim(tax_loc))
	rtrim(ltrim(tax_loc
		)
	) AS tax_loc_lkp,
	IN_class_code AS class_code,
	-- *INF*: rtrim(ltrim(class_code))
	rtrim(ltrim(class_code
		)
	) AS class_code_out,
	IN_exposure AS exposure,
	IN_sub_line_code AS sub_line_code,
	-- *INF*: rtrim(ltrim(sub_line_code))
	rtrim(ltrim(sub_line_code
		)
	) AS sub_line_code_out,
	IN_source_sar_asl AS source_sar_asl,
	-- *INF*: rtrim(ltrim(source_sar_asl))
	rtrim(ltrim(source_sar_asl
		)
	) AS source_sar_asl_out,
	IN_source_sar_prdct_line AS source_sar_prdct_line,
	-- *INF*: rtrim(ltrim(source_sar_prdct_line))
	rtrim(ltrim(source_sar_prdct_line
		)
	) AS source_sar_prdct_line_out,
	IN_source_sar_sp_use_code AS source_sar_sp_use_code,
	-- *INF*: rtrim(ltrim(source_sar_sp_use_code))
	rtrim(ltrim(source_sar_sp_use_code
		)
	) AS source_sar_sp_use_code_lkp,
	IN_auto_reins_facility_out AS auto_reins_facility,
	-- *INF*: ltrim(rtrim(auto_reins_facility))
	ltrim(rtrim(auto_reins_facility
		)
	) AS auto_reins_facility_out,
	IN_statistical_brkdwn_line AS statistical_brkdwn_line,
	-- *INF*: rtrim(ltrim(statistical_brkdwn_line))
	rtrim(ltrim(statistical_brkdwn_line
		)
	) AS statistical_brkdwn_line_out,
	IN_statistical_code_1 AS statistical_code_1,
	-- *INF*: ltrim(rtrim(statistical_code_1))
	-- 
	-- -- We are trimming the spaces so that we can find a match on the Target Lookup Values  
	ltrim(rtrim(statistical_code_1
		)
	) AS statistical_code_1_lkp,
	IN_statistical_code_2 AS statistical_code_2,
	-- *INF*: ltrim(rtrim(statistical_code_2))
	-- 
	-- -- We are trimming the spaces so that we can find a match on the Target Lookup Values  
	ltrim(rtrim(statistical_code_2
		)
	) AS statistical_code_2_lkp,
	IN_statistical_code_3 AS statistical_code_3,
	-- *INF*: ltrim(rtrim(statistical_code_3))
	-- 
	-- -- We are trimming the spaces so that we can find a match on the Target Lookup Values  
	ltrim(rtrim(statistical_code_3
		)
	) AS statistical_code_3_lkp,
	IN_variation_code AS variation_code,
	-- *INF*: ltrim(rtrim(variation_code))
	ltrim(rtrim(variation_code
		)
	) AS variation_code_out,
	IN_statistical_line AS source_statistical_line,
	-- *INF*: ltrim(rtrim(source_statistical_line))
	ltrim(rtrim(source_statistical_line
		)
	) AS source_statistical_line_out,
	IN_Policy_Type AS Policy_Type,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(Policy_Type)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(Policy_Type
	) AS policy_type_lkp,
	IN_coverage_code AS coverage_code_in,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(coverage_code_in)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(coverage_code_in
	) AS coverage_code_out
	FROM AGG_post_join
),
LKP_loss_master_dim AS (
	SELECT
	loss_master_dim_id,
	risk_state_prov_code,
	risk_zip_code,
	terr_code,
	tax_loc,
	class_code,
	exposure,
	sub_line_code,
	source_sar_asl,
	source_sar_prdct_line,
	source_sar_sp_use_code,
	source_statistical_line,
	variation_code,
	pol_type,
	auto_reins_facility,
	statistical_brkdwn_line,
	statistical_code1,
	statistical_code2,
	statistical_code3,
	loss_master_cov_code
	FROM (
		SELECT loss_master_dim.loss_master_dim_id      AS loss_master_dim_id,
		       ltrim(rtrim(loss_master_dim.risk_state_prov_code))    AS risk_state_prov_code,
		       ltrim(rtrim(loss_master_dim.risk_zip_code))           AS risk_zip_code,
		       ltrim(rtrim(loss_master_dim.terr_code))               AS terr_code,
		       ltrim(rtrim(loss_master_dim.tax_loc))                 AS tax_loc,
		       ltrim(rtrim(loss_master_dim.class_code))              AS class_code,
		       loss_master_dim.exposure                AS exposure,
		       ltrim(rtrim(loss_master_dim.sub_line_code))           AS sub_line_code,
		       ltrim(rtrim(loss_master_dim.source_sar_asl))          AS source_sar_asl,
		       ltrim(rtrim(loss_master_dim.source_sar_prdct_line))   AS source_sar_prdct_line,
		       ltrim(rtrim(loss_master_dim.source_sar_sp_use_code))  AS source_sar_sp_use_code,
		       ltrim(rtrim(loss_master_dim.source_statistical_line)) AS source_statistical_line,
		       ltrim(rtrim(loss_master_dim.variation_code))          AS variation_code,
		       ltrim(rtrim(loss_master_dim.pol_type))                AS pol_type,
		       ltrim(rtrim(loss_master_dim.auto_reins_facility))     AS auto_reins_facility,
		       ltrim(rtrim(loss_master_dim.statistical_brkdwn_line)) AS statistical_brkdwn_line,
		       ltrim(rtrim(loss_master_dim.statistical_code1))       AS statistical_code1,
		       ltrim(rtrim(loss_master_dim.statistical_code2))       AS statistical_code2,
		       ltrim(rtrim(loss_master_dim.statistical_code3))       AS statistical_code3,
		       ltrim(rtrim(loss_master_dim.loss_master_cov_code))    AS loss_master_cov_code
		FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY risk_state_prov_code,risk_zip_code,terr_code,tax_loc,class_code,exposure,sub_line_code,source_sar_asl,source_sar_prdct_line,source_sar_sp_use_code,source_statistical_line,variation_code,pol_type,auto_reins_facility,statistical_brkdwn_line,statistical_code1,statistical_code2,statistical_code3,loss_master_cov_code ORDER BY loss_master_dim_id DESC) = 1
),
EXP_Values AS (
	SELECT
	LKP_loss_master_dim.loss_master_dim_id AS lkp_loss_master_dim_id,
	EXP_converge.risk_state_prov_code_out AS risk_state_prov_code,
	EXP_converge.risk_zip_code_out AS risk_zip_code,
	EXP_converge.terr_code_out AS terr_code,
	EXP_converge.tax_loc_out AS tax_loc,
	EXP_converge.class_code_out AS class_code,
	EXP_converge.exposure,
	EXP_converge.sub_line_code_out AS sub_line_code,
	EXP_converge.source_sar_asl_out AS source_sar_asl,
	EXP_converge.source_sar_prdct_line_out AS source_sar_prdct_line,
	EXP_converge.source_sar_sp_use_code,
	EXP_converge.source_statistical_line_out AS source_statistical_line,
	EXP_converge.variation_code_out AS variation_code,
	EXP_converge.policy_type_lkp AS pol_type,
	EXP_converge.auto_reins_facility_out AS auto_reins_facility,
	EXP_converge.statistical_brkdwn_line_out AS statistical_brkdwn_line,
	EXP_converge.statistical_code_1,
	EXP_converge.statistical_code_2,
	EXP_converge.statistical_code_3,
	EXP_converge.coverage_code_out AS loss_master_cov_code
	FROM EXP_converge
	LEFT JOIN LKP_loss_master_dim
	ON LKP_loss_master_dim.risk_state_prov_code = EXP_converge.risk_state_prov_code_out AND LKP_loss_master_dim.risk_zip_code = EXP_converge.risk_zip_code_out AND LKP_loss_master_dim.terr_code = EXP_converge.terr_code_out AND LKP_loss_master_dim.tax_loc = EXP_converge.tax_loc_lkp AND LKP_loss_master_dim.class_code = EXP_converge.class_code_out AND LKP_loss_master_dim.exposure = EXP_converge.exposure AND LKP_loss_master_dim.sub_line_code = EXP_converge.sub_line_code_out AND LKP_loss_master_dim.source_sar_asl = EXP_converge.source_sar_asl_out AND LKP_loss_master_dim.source_sar_prdct_line = EXP_converge.source_sar_prdct_line_out AND LKP_loss_master_dim.source_sar_sp_use_code = EXP_converge.source_sar_sp_use_code_lkp AND LKP_loss_master_dim.source_statistical_line = EXP_converge.source_statistical_line_out AND LKP_loss_master_dim.variation_code = EXP_converge.variation_code_out AND LKP_loss_master_dim.pol_type = EXP_converge.policy_type_lkp AND LKP_loss_master_dim.auto_reins_facility = EXP_converge.auto_reins_facility_out AND LKP_loss_master_dim.statistical_brkdwn_line = EXP_converge.statistical_brkdwn_line_out AND LKP_loss_master_dim.statistical_code1 = EXP_converge.statistical_code_1_lkp AND LKP_loss_master_dim.statistical_code2 = EXP_converge.statistical_code_2_lkp AND LKP_loss_master_dim.statistical_code3 = EXP_converge.statistical_code_3_lkp AND LKP_loss_master_dim.loss_master_cov_code = EXP_converge.coverage_code_out
),
RTR_Insert_Update AS (
	SELECT
	lkp_loss_master_dim_id,
	risk_state_prov_code,
	risk_zip_code,
	terr_code,
	tax_loc,
	class_code,
	exposure,
	sub_line_code,
	source_sar_asl,
	source_sar_prdct_line,
	source_sar_sp_use_code,
	source_statistical_line,
	variation_code,
	pol_type,
	auto_reins_facility,
	statistical_brkdwn_line,
	statistical_code_1 AS statistical_code1,
	statistical_code_2 AS statistical_code2,
	statistical_code_3 AS statistical_code3,
	loss_master_cov_code
	FROM EXP_Values
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE IIF(ISNULL(lkp_loss_master_dim_id),TRUE,FALSE)),
RTR_Insert_Update_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE IIF(NOT ISNULL(lkp_loss_master_dim_id),TRUE,FALSE)),
EXP_Target1 AS (
	SELECT
	risk_state_prov_code,
	risk_zip_code,
	terr_code,
	tax_loc,
	class_code,
	exposure,
	sub_line_code,
	source_sar_asl,
	source_sar_prdct_line,
	source_sar_sp_use_code,
	source_statistical_line,
	variation_code,
	pol_type,
	auto_reins_facility,
	statistical_brkdwn_line,
	statistical_code AS statistical_code1,
	statistical_code2,
	statistical_code3,
	loss_master_cov_code,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM RTR_Insert_Update_INSERT
),
loss_master_dim_Insert AS (
	INSERT INTO loss_master_dim
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date, risk_state_prov_code, risk_zip_code, terr_code, tax_loc, class_code, exposure, sub_line_code, source_sar_asl, source_sar_prdct_line, source_sar_sp_use_code, source_statistical_line, variation_code, pol_type, auto_reins_facility, statistical_brkdwn_line, statistical_code1, statistical_code2, statistical_code3, loss_master_cov_code)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	RISK_STATE_PROV_CODE, 
	RISK_ZIP_CODE, 
	TERR_CODE, 
	TAX_LOC, 
	CLASS_CODE, 
	EXPOSURE, 
	SUB_LINE_CODE, 
	SOURCE_SAR_ASL, 
	SOURCE_SAR_PRDCT_LINE, 
	SOURCE_SAR_SP_USE_CODE, 
	SOURCE_STATISTICAL_LINE, 
	VARIATION_CODE, 
	POL_TYPE, 
	AUTO_REINS_FACILITY, 
	STATISTICAL_BRKDWN_LINE, 
	STATISTICAL_CODE1, 
	STATISTICAL_CODE2, 
	STATISTICAL_CODE3, 
	LOSS_MASTER_COV_CODE
	FROM EXP_Target1
),
UPD_Update AS (
	SELECT
	lkp_loss_master_dim_id AS lkp_loss_master_dim_id3, 
	risk_state_prov_code AS risk_state_prov_code3, 
	risk_zip_code AS risk_zip_code3, 
	terr_code AS terr_code3, 
	tax_loc AS tax_loc3, 
	class_code AS class_code3, 
	exposure AS exposure3, 
	sub_line_code AS sub_line_code3, 
	source_sar_asl AS source_sar_asl3, 
	source_sar_prdct_line AS source_sar_prdct_line3, 
	source_sar_sp_use_code AS source_sar_sp_use_code3, 
	source_statistical_line AS source_statistical_line3, 
	variation_code AS variation_code3, 
	pol_type AS pol_type3, 
	auto_reins_facility AS auto_reins_facility3, 
	statistical_brkdwn_line AS statistical_brkdwn_line3, 
	statistical_code1 AS statistical_code13, 
	statistical_code2 AS statistical_code23, 
	statistical_code AS statistical_code33, 
	loss_master_cov_code AS loss_master_cov_code3
	FROM RTR_Insert_Update_UPDATE
),
loss_master_dim_Update AS (
	MERGE INTO loss_master_dim AS T
	USING UPD_Update AS S
	ON T.loss_master_dim_id = S.lkp_loss_master_dim_id3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.risk_state_prov_code = S.risk_state_prov_code3, T.risk_zip_code = S.risk_zip_code3, T.terr_code = S.terr_code3, T.tax_loc = S.tax_loc3, T.class_code = S.class_code3, T.exposure = S.exposure3, T.sub_line_code = S.sub_line_code3, T.source_sar_asl = S.source_sar_asl3, T.source_sar_prdct_line = S.source_sar_prdct_line3, T.source_sar_sp_use_code = S.source_sar_sp_use_code3, T.source_statistical_line = S.source_statistical_line3, T.variation_code = S.variation_code3, T.pol_type = S.pol_type3, T.auto_reins_facility = S.auto_reins_facility3, T.statistical_brkdwn_line = S.statistical_brkdwn_line3, T.statistical_code1 = S.statistical_code13, T.statistical_code2 = S.statistical_code23, T.statistical_code3 = S.statistical_code33, T.loss_master_cov_code = S.loss_master_cov_code3
),
SQ_EDWSource_DCTClaims AS (
	SELECT DISTINCT AB.claimant_cov_det_ak_id       AS Claimant_cov_det_ak_id,
		   AB.claim_party_occurrence_ak_id AS claim_party_occurrence_ak_id,
	       AB.PolicySourceID               AS Policysourceid,
		   RC.Classcode                 AS Classcode,
	       AB.SublineCode                  AS Sublinecode,
	       RC.Exposure                     AS Exposure,
	       RC.AnnualStatementLineNumber    AS AnnualStatementLineNumber,
	       RC.SchedulePNumber              AS SchedulePNumber,
	       RC.AnnualStatementLineCode      AS AnnualStatementLineCode,
	       RC.SubAnnualStatementLineNumber AS SubAnnualStatementLineNumber,
	       RC.SubAnnualStatementLineCode   AS SubAnnualStatementLineCode,
	       RC.SubNonAnnualStatementLineCode AS SubNonAnnualStatementLineCode,
	       RL.RiskTerritory                AS Riskterritory,
	       RL.StateProvinceCode            AS Stateprovincecode,
	       RL.ZipPostalCode                AS Zippostalcode,
	       RL.TaxLocation                  AS Taxlocation,
	       RC.Exposure                     AS Exposure
	FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.CLAIMANT_COVERAGE_DETAIL AB,
	       @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC,
	       @{pipeline().parameters.TARGET_TABLE_OWNER}.POLICYCOVERAGE Pc,
	       @{pipeline().parameters.TARGET_TABLE_OWNER}.RISKLOCATION RL
	WHERE  AB.RatingCoverageAKId = RC.RatingCoverageAKID
	       AND RC.PolicyCoverageAKID = PC.PolicyCoverageAKID
	       AND PC.RiskLocationAKID = RL.RiskLocationAKID
	       AND AB.PolicySourceID IN ( 'PDC', 'DUC' )
		  AND AB.crrnt_snpsht_flag = 1 AND RL.CurrentSnapshotFlag =1 AND RC.CurrentSnapshotFlag =1 AND PC.CurrentSnapshotFlag =1
	       AND (AB.modified_date >= '@{pipeline().parameters.SELECTION_START_TS}' OR RL.createddate > ='@{pipeline().parameters.SELECTION_START_TS}' 
		   OR PC.createddate > ='@{pipeline().parameters.SELECTION_START_TS}' OR RC.createddate > ='@{pipeline().parameters.SELECTION_START_TS}')
),
EXP_Default AS (
	SELECT
	claimant_cov_det_ak_id,
	claim_party_occurrence_ak_id,
	PolicySourceID,
	ClassCode,
	SublineCode,
	RiskTerritory,
	StateProvinceCode,
	ZipPostalCode,
	TaxLocation,
	Exposure,
	'N/A' AS Default_NA,
	AnnualStatementLineNumber1 AS AnnualStatementLineNumber,
	SchedulePNumber,
	AnnualStatementLineCode,
	SubAnnualStatementLineNumber,
	SubAnnualStatementLineCode,
	SubNonAnnualStatementLineCode
	FROM SQ_EDWSource_DCTClaims
),
LKP_loss_master_dim_DuckCreekClaims AS (
	SELECT
	loss_master_dim_id,
	risk_state_prov_code,
	risk_zip_code,
	terr_code,
	tax_loc,
	class_code,
	exposure,
	sub_line_code,
	source_sar_asl,
	source_sar_prdct_line,
	source_sar_sp_use_code,
	source_statistical_line,
	variation_code,
	pol_type,
	auto_reins_facility,
	statistical_brkdwn_line,
	statistical_code1,
	statistical_code2,
	statistical_code3,
	loss_master_cov_code
	FROM (
		SELECT loss_master_dim.loss_master_dim_id      AS loss_master_dim_id,
		       ltrim(rtrim(loss_master_dim.risk_state_prov_code))    AS risk_state_prov_code,
		       ltrim(rtrim(loss_master_dim.risk_zip_code))           AS risk_zip_code,
		       ltrim(rtrim(loss_master_dim.terr_code))               AS terr_code,
		       ltrim(rtrim(loss_master_dim.tax_loc))                 AS tax_loc,
		       ltrim(rtrim(loss_master_dim.class_code))              AS class_code,
		       loss_master_dim.exposure                AS exposure,
		       ltrim(rtrim(loss_master_dim.sub_line_code))           AS sub_line_code,
		       ltrim(rtrim(loss_master_dim.source_sar_asl))          AS source_sar_asl,
		       ltrim(rtrim(loss_master_dim.source_sar_prdct_line))   AS source_sar_prdct_line,
		       ltrim(rtrim(loss_master_dim.source_sar_sp_use_code))  AS source_sar_sp_use_code,
		       ltrim(rtrim(loss_master_dim.source_statistical_line)) AS source_statistical_line,
		       ltrim(rtrim(loss_master_dim.variation_code))          AS variation_code,
		       ltrim(rtrim(loss_master_dim.pol_type))                AS pol_type,
		       ltrim(rtrim(loss_master_dim.auto_reins_facility))     AS auto_reins_facility,
		       ltrim(rtrim(loss_master_dim.statistical_brkdwn_line)) AS statistical_brkdwn_line,
		       ltrim(rtrim(loss_master_dim.statistical_code1))       AS statistical_code1,
		       ltrim(rtrim(loss_master_dim.statistical_code2))       AS statistical_code2,
		       ltrim(rtrim(loss_master_dim.statistical_code3))       AS statistical_code3,
		       ltrim(rtrim(loss_master_dim.loss_master_cov_code))    AS loss_master_cov_code
		FROM   loss_master_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY risk_state_prov_code,risk_zip_code,terr_code,tax_loc,class_code,exposure,sub_line_code,source_sar_asl,source_sar_prdct_line,source_sar_sp_use_code,source_statistical_line,variation_code,pol_type,auto_reins_facility,statistical_brkdwn_line,statistical_code1,statistical_code2,statistical_code3,loss_master_cov_code ORDER BY loss_master_dim_id DESC) = 1
),
EXP_Values2 AS (
	SELECT
	LKP_loss_master_dim_DuckCreekClaims.loss_master_dim_id AS lkp_loss_master_dim_id,
	EXP_Default.StateProvinceCode AS risk_state_prov_code,
	EXP_Default.ZipPostalCode AS risk_zip_code,
	EXP_Default.RiskTerritory AS terr_code,
	EXP_Default.TaxLocation AS tax_loc,
	EXP_Default.ClassCode AS class_code,
	EXP_Default.Exposure AS exposure,
	EXP_Default.SublineCode AS sub_line_code,
	EXP_Default.Default_NA AS source_sar_asl,
	EXP_Default.Default_NA AS source_sar_prdct_line,
	EXP_Default.Default_NA AS source_sar_sp_use_code,
	EXP_Default.Default_NA AS source_statistical_line,
	EXP_Default.Default_NA AS variation_code,
	EXP_Default.Default_NA AS pol_type,
	EXP_Default.Default_NA AS auto_reins_facility,
	EXP_Default.Default_NA AS statistical_brkdwn_line,
	EXP_Default.Default_NA AS statistical_code_1,
	EXP_Default.Default_NA AS statistical_code_2,
	EXP_Default.Default_NA AS statistical_code_3,
	EXP_Default.Default_NA AS loss_master_cov_code
	FROM EXP_Default
	LEFT JOIN LKP_loss_master_dim_DuckCreekClaims
	ON LKP_loss_master_dim_DuckCreekClaims.risk_state_prov_code = EXP_Default.StateProvinceCode AND LKP_loss_master_dim_DuckCreekClaims.risk_zip_code = EXP_Default.ZipPostalCode AND LKP_loss_master_dim_DuckCreekClaims.terr_code = EXP_Default.RiskTerritory AND LKP_loss_master_dim_DuckCreekClaims.tax_loc = EXP_Default.TaxLocation AND LKP_loss_master_dim_DuckCreekClaims.class_code = EXP_Default.ClassCode AND LKP_loss_master_dim_DuckCreekClaims.exposure = EXP_Default.Exposure AND LKP_loss_master_dim_DuckCreekClaims.sub_line_code = EXP_Default.SublineCode AND LKP_loss_master_dim_DuckCreekClaims.source_sar_asl = EXP_Default.Default_NA AND LKP_loss_master_dim_DuckCreekClaims.source_sar_prdct_line = EXP_Default.Default_NA AND LKP_loss_master_dim_DuckCreekClaims.source_sar_sp_use_code = EXP_Default.Default_NA AND LKP_loss_master_dim_DuckCreekClaims.source_statistical_line = EXP_Default.Default_NA AND LKP_loss_master_dim_DuckCreekClaims.variation_code = EXP_Default.Default_NA AND LKP_loss_master_dim_DuckCreekClaims.pol_type = EXP_Default.Default_NA AND LKP_loss_master_dim_DuckCreekClaims.auto_reins_facility = EXP_Default.Default_NA AND LKP_loss_master_dim_DuckCreekClaims.statistical_brkdwn_line = EXP_Default.Default_NA AND LKP_loss_master_dim_DuckCreekClaims.statistical_code1 = EXP_Default.Default_NA AND LKP_loss_master_dim_DuckCreekClaims.statistical_code2 = EXP_Default.Default_NA AND LKP_loss_master_dim_DuckCreekClaims.statistical_code3 = EXP_Default.Default_NA AND LKP_loss_master_dim_DuckCreekClaims.loss_master_cov_code = EXP_Default.Default_NA
),
RTR_Insert_Update1 AS (
	SELECT
	lkp_loss_master_dim_id,
	risk_state_prov_code,
	risk_zip_code,
	terr_code,
	tax_loc,
	class_code,
	exposure,
	sub_line_code,
	source_sar_asl,
	source_sar_prdct_line,
	source_sar_sp_use_code,
	source_statistical_line,
	variation_code,
	pol_type,
	auto_reins_facility,
	statistical_brkdwn_line,
	statistical_code_1 AS statistical_code1,
	statistical_code_2 AS statistical_code2,
	statistical_code_3 AS statistical_code3,
	loss_master_cov_code
	FROM EXP_Values2
),
RTR_Insert_Update1_INSERT AS (SELECT * FROM RTR_Insert_Update1 WHERE IIF(ISNULL(lkp_loss_master_dim_id),TRUE,FALSE)),
RTR_Insert_Update1_UPDATE AS (SELECT * FROM RTR_Insert_Update1 WHERE IIF(NOT ISNULL(lkp_loss_master_dim_id),TRUE,FALSE)),
UPD_Update1 AS (
	SELECT
	lkp_loss_master_dim_id AS lkp_loss_master_dim_id3, 
	risk_state_prov_code AS risk_state_prov_code3, 
	risk_zip_code AS risk_zip_code3, 
	terr_code AS terr_code3, 
	tax_loc AS tax_loc3, 
	class_code AS class_code3, 
	exposure AS exposure3, 
	sub_line_code AS sub_line_code3, 
	source_sar_asl AS source_sar_asl3, 
	source_sar_prdct_line AS source_sar_prdct_line3, 
	source_sar_sp_use_code AS source_sar_sp_use_code3, 
	source_statistical_line AS source_statistical_line3, 
	variation_code AS variation_code3, 
	pol_type AS pol_type3, 
	auto_reins_facility AS auto_reins_facility3, 
	statistical_brkdwn_line AS statistical_brkdwn_line3, 
	statistical_code1 AS statistical_code13, 
	statistical_code2 AS statistical_code23, 
	statistical_code AS statistical_code33, 
	loss_master_cov_code AS loss_master_cov_code3
	FROM RTR_Insert_Update1_UPDATE
),
loss_master_dim_Update1 AS (
	MERGE INTO loss_master_dim AS T
	USING UPD_Update1 AS S
	ON T.loss_master_dim_id = S.lkp_loss_master_dim_id3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.risk_state_prov_code = S.risk_state_prov_code3, T.risk_zip_code = S.risk_zip_code3, T.terr_code = S.terr_code3, T.tax_loc = S.tax_loc3, T.class_code = S.class_code3, T.exposure = S.exposure3, T.sub_line_code = S.sub_line_code3, T.source_sar_asl = S.source_sar_asl3, T.source_sar_prdct_line = S.source_sar_prdct_line3, T.source_sar_sp_use_code = S.source_sar_sp_use_code3, T.source_statistical_line = S.source_statistical_line3, T.variation_code = S.variation_code3, T.pol_type = S.pol_type3, T.auto_reins_facility = S.auto_reins_facility3, T.statistical_brkdwn_line = S.statistical_brkdwn_line3, T.statistical_code1 = S.statistical_code13, T.statistical_code2 = S.statistical_code23, T.statistical_code3 = S.statistical_code33, T.loss_master_cov_code = S.loss_master_cov_code3
),
EXP_Target11 AS (
	SELECT
	risk_state_prov_code,
	risk_zip_code,
	terr_code,
	tax_loc,
	class_code,
	exposure,
	sub_line_code,
	source_sar_asl,
	source_sar_prdct_line,
	source_sar_sp_use_code,
	source_statistical_line,
	variation_code,
	pol_type,
	auto_reins_facility,
	statistical_brkdwn_line,
	statistical_code AS statistical_code1,
	statistical_code2,
	statistical_code3,
	loss_master_cov_code,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM RTR_Insert_Update1_INSERT
),
loss_master_dim_Insert1 AS (
	INSERT INTO loss_master_dim
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date, risk_state_prov_code, risk_zip_code, terr_code, tax_loc, class_code, exposure, sub_line_code, source_sar_asl, source_sar_prdct_line, source_sar_sp_use_code, source_statistical_line, variation_code, pol_type, auto_reins_facility, statistical_brkdwn_line, statistical_code1, statistical_code2, statistical_code3, loss_master_cov_code)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	RISK_STATE_PROV_CODE, 
	RISK_ZIP_CODE, 
	TERR_CODE, 
	TAX_LOC, 
	CLASS_CODE, 
	EXPOSURE, 
	SUB_LINE_CODE, 
	SOURCE_SAR_ASL, 
	SOURCE_SAR_PRDCT_LINE, 
	SOURCE_SAR_SP_USE_CODE, 
	SOURCE_STATISTICAL_LINE, 
	VARIATION_CODE, 
	POL_TYPE, 
	AUTO_REINS_FACILITY, 
	STATISTICAL_BRKDWN_LINE, 
	STATISTICAL_CODE1, 
	STATISTICAL_CODE2, 
	STATISTICAL_CODE3, 
	LOSS_MASTER_COV_CODE
	FROM EXP_Target11
),
WITH
SQ_Claim_File_Header AS (
	SELECT
		sup_cms_tin_office_id,
		crrnt_snpsht_flag,
		audit_id,
		eff_from_date,
		eff_to_date,
		source_sys_id,
		created_date,
		modified_date,
		cms_rre_id,
		office_tin_num,
		office_code,
		office_name,
		office_mail_address1,
		office_mail_address2,
		office_mail_city,
		office_mail_state,
		office_mail_zip,
		office_mail_zip4
	FROM sup_cms_tin_office
	WHERE crrnt_snpsht_flag=1
),
EXP_Header AS (
	SELECT
	'H0' AS Header_Identifier,
	cms_rre_id,
	-- *INF*: LPAD(LTRIM(RTRIM(cms_rre_id)),9,'0')
	LPAD(LTRIM(RTRIM(cms_rre_id)), 9, '0') AS v_cms_rre_id,
	v_cms_rre_id AS cms_rre_id_out,
	'IACT' AS File_Type,
	-- *INF*: TO_CHAR(SYSDATE,'YYYYMMDD')
	TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDD') AS Cycle_date,
	'' AS Default_BLANKS
	FROM SQ_Claim_File_Header
),
CMS_Claims_Query_File_Header AS (
	INSERT INTO CMS_Claims_Query_File_Header
	(Header_Indicator, RRE_ID, File_Type, File_Date, Filler)
	SELECT 
	Header_Identifier AS HEADER_INDICATOR, 
	cms_rre_id_out AS RRE_ID, 
	FILE_TYPE, 
	Cycle_date AS FILE_DATE, 
	Default_BLANKS AS FILLER
	FROM EXP_Header
),
SQ_claim_medical AS (
	SELECT 
	claim_party.claim_party_id,
	claim_medical.claim_med_ak_id, 
	claim_medical.claim_party_occurrence_ak_id, 
	claim_party.claim_party_ak_id,
	claim_party.claim_party_key,
	claim_medical.medicare_hicn,
	claim_party.claim_party_last_name,
	claim_party.claim_party_first_name,
	REPLACE(CONVERT(VARCHAR(8), claim_party.claim_party_birthdate, 112),'/','') AS claim_party_birthdate,
	CASE claim_party.claim_party_gndr 
		WHEN 'M' THEN '1' 
		WHEN 'F' THEN '2'
		WHEN '1' THEN '1'
		WHEN '2' THEN '2'
	    ELSE '0'
	END AS claim_party_gndr,
	-- why are we doing this?
	--REPLACE(claim_party.tax_ssn_id,'-','')  as tax_ssn_id,
	claim_party.tax_ssn_id,
	claim_medical.injured_party_id ,
	claim_party.source_sys_id
	FROM
	claim_party,claim_party_occurrence,claim_medical
	WHERE
	claim_medical.claim_party_occurrence_ak_id = claim_party_occurrence.claim_party_occurrence_ak_ID
	AND claim_party_occurrence.claim_party_ak_id = claim_party.claim_party_ak_id
	AND claim_party.crrnt_snpsht_flag = 1 AND claim_party_occurrence.crrnt_snpsht_flag = 1 
	AND claim_medical.crrnt_snpsht_flag = 1
	AND claim_medical.query_requested_ind = 'T'
	
	ORDER BY claim_medical.injured_party_id
	
	
	-------------- need to change the query_requested_ind to 'T' after sometime
),
EXP_Input AS (
	SELECT
	claim_party_id,
	claim_med_ak_id,
	claim_party_occurrence_ak_id,
	claim_party_ak_id,
	claim_party_key,
	medicare_hicn,
	claim_party_last_name,
	claim_party_first_name,
	claim_party_birthdate,
	claim_party_gndr,
	tax_ssn_id,
	-- *INF*: rtrim(ltrim(tax_ssn_id))
	rtrim(ltrim(tax_ssn_id)) AS o_tax_ssn_id,
	injured_party_id,
	source_sys_id
	FROM SQ_claim_medical
),
SQ_claim_medical_SSN AS (
	SELECT
	distinct claim_medical.injured_party_id,  claim_party.tax_ssn_id 
	FROM
	claim_medical
	
	inner join claim_party_occurrence
	ON claim_medical.claim_party_occurrence_ak_id = claim_party_occurrence.claim_party_occurrence_ak_ID
	AND claim_party_occurrence.crrnt_snpsht_flag = 1 
	
	inner join claim_party
	ON claim_party_occurrence.claim_party_ak_id = claim_party.claim_party_ak_id
	AND claim_party.crrnt_snpsht_flag = 1 
	
	WHERE
	claim_medical.crrnt_snpsht_flag = 1  AND
	claim_medical.query_requested_ind = 'T'
	
	ORDER BY 1
),
mplt_Detokinize_WebService_call AS (WITH
	INPUT AS (
		
	),
	EXP_SSN_FEIN_TAXID AS (
		SELECT
		IN_id AS id,
		IN_ssn_fein_taxid AS ssn_fein_taxid,
		-- *INF*: LTRIM(RTRIM(ssn_fein_taxid))
		LTRIM(RTRIM(ssn_fein_taxid)) AS V_ssn_fein_taxid,
		-- *INF*: IIF(LENGTH(V_ssn_fein_taxid)=0  or IS_NUMBER(V_ssn_fein_taxid),'FEIN','NONFEIN')
		-- --IIF(LENGTH(V_ssn_fein_taxid)=11 AND REG_MATCH(V_ssn_fein_taxid,'[\da-zA-Z]+'),'NONFEIN','FEIN')
		-- 
		-- 
		IFF(
		    LENGTH(V_ssn_fein_taxid) = 0 or REGEXP_LIKE(V_ssn_fein_taxid, '^[0-9]+$'), 'FEIN', 'NONFEIN'
		) AS V_flag,
		V_flag AS flag,
		V_ssn_fein_taxid AS o_ssn_fein_taxid
		FROM INPUT
	),
	RTR_SSN_FEIN_TAXID AS (
		SELECT
		id,
		flag,
		o_ssn_fein_taxid AS ssn_fein_taxid,
		o_ssn_fein_taxid AS ssn_fein
		FROM EXP_SSN_FEIN_TAXID
	),
	RTR_SSN_FEIN_TAXID_FEIN_VALUES AS (SELECT * FROM RTR_SSN_FEIN_TAXID WHERE flag = 'FEIN'),
	RTR_SSN_FEIN_TAXID_NONFEIN_VALUES AS (SELECT * FROM RTR_SSN_FEIN_TAXID WHERE flag='NONFEIN'),
	EXP_Values AS (
		SELECT
		id AS ID,
		'Detokenize' AS Function,
		'SSN' AS Scheme,
		ssn_fein_taxid AS SSN_FEIN_TAXID,
		'DATAFEED_JOBS' AS Requestedby,
		'Claims' AS Application,
		'DataFeeds' AS Caller,
		-- *INF*: 'https://intsvc.wbconnect.com/services/TokenService/Token.svc'
		-- 
		-- 
		-- 
		'https://intsvc.wbconnect.com/services/TokenService/Token.svc' AS URL,
		ssn_fein AS SSN_FEIN3
		FROM RTR_SSN_FEIN_TAXID_NONFEIN_VALUES
	),
	Token AS (-- Token
	
		##############################################
	
		# TODO: Place holder for Custom transformation
	
		##############################################
	),
	EXP_Response AS (
		SELECT
		REF_Id AS ID,
		tns_Data0 AS TokenResponse,
		REF_SSN_FEIN_TAXID,
		-- *INF*: ltrim(rtrim(REPLACECHR(0,TokenResponse,'-',NULL)))
		-- 
		ltrim(rtrim(REGEXP_REPLACE(TokenResponse,'-','','i'))) AS o_TokenResponse
		FROM Token
	),
	Union_SSN_FEIN_TAXID AS (
		SELECT id AS ID, ssn_fein_taxid AS TokenReponse, ssn_fein_taxid AS SSN_FEIN
		FROM RTR_SSN_FEIN_TAXID_FEIN_VALUES
		UNION
		SELECT ID, o_TokenResponse AS TokenReponse, REF_SSN_FEIN_TAXID AS SSN_FEIN
		FROM EXP_Response
	),
	OUTPUT AS (
		SELECT
		ID AS OUT_id, 
		TokenReponse AS OUT_TokenResponse, 
		SSN_FEIN AS OUT_IN_ssn_fein_taxid
		FROM Union_SSN_FEIN_TAXID
	),
),
EXP__Standardize_Fields AS (
	SELECT
	OUT_id AS id,
	id AS o_id,
	OUT_TokenResponse AS in_TokenResponse,
	-- *INF*: rtrim(ltrim(in_TokenResponse))
	rtrim(ltrim(in_TokenResponse)) AS o_TokenResponse,
	OUT_IN_ssn_fein_taxid AS in_IN_ssn_fein_taxid,
	-- *INF*: rtrim(ltrim(in_IN_ssn_fein_taxid))
	rtrim(ltrim(in_IN_ssn_fein_taxid)) AS o_input_ssn_fein_taxid
	FROM mplt_Detokinize_WebService_call
),
JNR_claim_medical AS (SELECT
	EXP_Input.claim_party_id, 
	EXP_Input.claim_med_ak_id, 
	EXP_Input.claim_party_occurrence_ak_id, 
	EXP_Input.claim_party_ak_id, 
	EXP_Input.claim_party_key, 
	EXP_Input.medicare_hicn, 
	EXP_Input.claim_party_last_name, 
	EXP_Input.claim_party_first_name, 
	EXP_Input.claim_party_birthdate, 
	EXP_Input.claim_party_gndr, 
	EXP_Input.o_tax_ssn_id AS tax_ssn_id, 
	EXP_Input.injured_party_id, 
	EXP_Input.source_sys_id, 
	EXP__Standardize_Fields.o_TokenResponse AS TokenResponse, 
	EXP__Standardize_Fields.o_input_ssn_fein_taxid AS OUT_IN_ssn_fein_taxid, 
	EXP__Standardize_Fields.o_id AS mplt_id
	FROM EXP_Input
	INNER JOIN EXP__Standardize_Fields
	ON EXP__Standardize_Fields.o_id = EXP_Input.injured_party_id
),
EXP_Source AS (
	SELECT
	claim_med_ak_id,
	claim_party_occurrence_ak_id,
	claim_party_ak_id,
	medicare_hicn,
	claim_party_last_name,
	claim_party_first_name,
	claim_party_birthdate,
	claim_party_gndr,
	TokenResponse AS tax_ssn_id,
	injured_party_id,
	claim_party_key,
	source_sys_id,
	tax_ssn_id AS tax_ssn_id_Work_Table,
	OUT_IN_ssn_fein_taxid AS Token_ssn_fein_taxid
	FROM JNR_claim_medical
),
LKP_cms_pms_relation_stage AS (
	SELECT
	last_name,
	first_name,
	client_key
	FROM (
		SELECT cms_pms_relation_stage.last_name as last_name, cms_pms_relation_stage.first_name as first_name, 
		(pms_policy_sym + pms_policy_num + pms_policy_mod + 
		replace(CONVERT(VARCHAR(10), pms_date_of_loss, 101),'/','') + 
		pms_loss_occurence + pms_loss_claimant + 'CMT') as client_key
		FROM
		 wc_stage.dbo.cms_pms_relation_stage cms_pms_relation_stage, wc_stage.dbo.claim_medical_stage claim_medical_stage
		WHERE
		claim_medical_stage.injured_party_id=cms_pms_relation_stage.injured_party_id
		AND cms_pms_relation_stage.cms_party_type = 'MINJ'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY client_key ORDER BY last_name DESC) = 1
),
EXP_Values AS (
	SELECT
	EXP_Source.claim_med_ak_id,
	EXP_Source.claim_party_occurrence_ak_id,
	EXP_Source.claim_party_ak_id,
	EXP_Source.medicare_hicn,
	LKP_cms_pms_relation_stage.last_name AS last_name_pms,
	LKP_cms_pms_relation_stage.first_name AS first_name_pms,
	EXP_Source.claim_party_last_name,
	-- *INF*: IIF(source_sys_id = 'PMS',last_name_pms,claim_party_last_name)
	IFF(source_sys_id = 'PMS', last_name_pms, claim_party_last_name) AS v_claim_party_last_name,
	-- *INF*: SUBSTR(v_claim_party_last_name,1,6)
	SUBSTR(v_claim_party_last_name, 1, 6) AS claim_party_last_name_Out,
	EXP_Source.claim_party_first_name,
	-- *INF*: IIF(source_sys_id = 'PMS',first_name_pms,claim_party_first_name)
	IFF(source_sys_id = 'PMS', first_name_pms, claim_party_first_name) AS v_claim_party_first_name,
	-- *INF*: SUBSTR(v_claim_party_first_name,1,1)
	SUBSTR(v_claim_party_first_name, 1, 1) AS claim_party_first_name_Out,
	EXP_Source.claim_party_birthdate,
	EXP_Source.claim_party_gndr,
	EXP_Source.tax_ssn_id,
	EXP_Source.injured_party_id,
	EXP_Source.source_sys_id,
	EXP_Source.tax_ssn_id_Work_Table
	FROM EXP_Source
	LEFT JOIN LKP_cms_pms_relation_stage
	ON LKP_cms_pms_relation_stage.client_key = EXP_Source.claim_party_key
),
EXP_Target AS (
	SELECT
	claim_med_ak_id,
	claim_party_occurrence_ak_id,
	claim_party_ak_id,
	medicare_hicn,
	claim_party_last_name_Out AS claim_party_last_name,
	claim_party_first_name_Out AS claim_party_first_name,
	claim_party_birthdate,
	claim_party_gndr,
	tax_ssn_id,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(medicare_hicn)
	UDF_DEFAULT_VALUE_TO_BLANKS(medicare_hicn) AS medicare_hicn_out,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_last_name)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_last_name) AS claim_party_last_name_out,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_first_name)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_first_name) AS claim_party_first_name_out,
	-- *INF*: IIF(claim_party_birthdate='99991231' OR claim_party_birthdate='21001231' ,'00000000',claim_party_birthdate)
	-- 
	IFF(
	    claim_party_birthdate = '99991231' OR claim_party_birthdate = '21001231', '00000000',
	    claim_party_birthdate
	) AS claim_party_birthdate_out,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_party_gndr)
	UDF_DEFAULT_VALUE_TO_BLANKS(claim_party_gndr) AS claim_party_gndr_out,
	-- *INF*: IIF(tax_ssn_id='N/A','000000000',tax_ssn_id)
	IFF(tax_ssn_id = 'N/A', '000000000', tax_ssn_id) AS tax_ssn_id_out,
	injured_party_id,
	'' AS DEFAULT_BLANKS,
	SYSDATE AS System_Date,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_Id,
	tax_ssn_id_Work_Table,
	-- *INF*: IIF(tax_ssn_id_Work_Table='N/A','000000000',tax_ssn_id_Work_Table)
	IFF(tax_ssn_id_Work_Table = 'N/A', '000000000', tax_ssn_id_Work_Table) AS tax_ssn_id_Work_Table_out
	FROM EXP_Values
),
EXP_Target_File AS (
	SELECT
	medicare_hicn_out,
	claim_party_last_name_out,
	claim_party_first_name_out,
	claim_party_birthdate_out,
	claim_party_gndr_out,
	tax_ssn_id_out AS in_tax_ssn_id_out,
	-- *INF*: REPLACECHR(0,in_tax_ssn_id_out,'-','')
	REGEXP_REPLACE(in_tax_ssn_id_out,'-','','i') AS v_tax_ssn_id_out,
	v_tax_ssn_id_out AS tax_ssn_id_out,
	injured_party_id,
	DEFAULT_BLANKS
	FROM EXP_Target
),
CMS_Claims_Query_Input_File AS (
	INSERT INTO CMS_Claims_Query_Input_File
	(HIC_Number, LastName, First_Initial, DOB, Sex_Code, SSN, RRE_DCN_1, RRE_DCN_2, Filler)
	SELECT 
	medicare_hicn_out AS HIC_NUMBER, 
	claim_party_last_name_out AS LASTNAME, 
	claim_party_first_name_out AS FIRST_INITIAL, 
	claim_party_birthdate_out AS DOB, 
	claim_party_gndr_out AS SEX_CODE, 
	tax_ssn_id_out AS SSN, 
	injured_party_id AS RRE_DCN_1, 
	DEFAULT_BLANKS AS RRE_DCN_2, 
	DEFAULT_BLANKS AS FILLER
	FROM EXP_Target_File
),
work_claim_cms_query_extract AS (
	INSERT INTO work_claim_cms_query_extract
	(claim_med_ak_id, claim_party_occurrence_ak_id, claim_party_ak_id, injured_party_hicn, injured_party_last_name, injured_party_first_initial, injured_party_dob, injured_party_gender, injured_party_ssn, filler, created_date, modified_date, audit_id, injured_party_id)
	SELECT 
	CLAIM_MED_AK_ID, 
	CLAIM_PARTY_OCCURRENCE_AK_ID, 
	CLAIM_PARTY_AK_ID, 
	medicare_hicn_out AS INJURED_PARTY_HICN, 
	claim_party_last_name_out AS INJURED_PARTY_LAST_NAME, 
	claim_party_first_name_out AS INJURED_PARTY_FIRST_INITIAL, 
	claim_party_birthdate_out AS INJURED_PARTY_DOB, 
	claim_party_gndr_out AS INJURED_PARTY_GENDER, 
	tax_ssn_id_Work_Table_out AS INJURED_PARTY_SSN, 
	DEFAULT_BLANKS AS FILLER, 
	System_Date AS CREATED_DATE, 
	System_Date AS MODIFIED_DATE, 
	Audit_Id AS AUDIT_ID, 
	INJURED_PARTY_ID
	FROM EXP_Target
),
SQ_Claim_File_Header1 AS (
	SELECT
		sup_cms_tin_office_id,
		crrnt_snpsht_flag,
		audit_id,
		eff_from_date,
		eff_to_date,
		source_sys_id,
		created_date,
		modified_date,
		cms_rre_id,
		office_tin_num,
		office_code,
		office_name,
		office_mail_address1,
		office_mail_address2,
		office_mail_city,
		office_mail_state,
		office_mail_zip,
		office_mail_zip4
	FROM sup_cms_tin_office1
	WHERE crrnt_snpsht_flag=1
),
EXPTRANS AS (
	SELECT
	audit_id,
	cms_rre_id,
	-- *INF*: LPAD(LTRIM(RTRIM(cms_rre_id)),9,'0')
	LPAD(LTRIM(RTRIM(cms_rre_id)), 9, '0') AS v_cms_rre_id,
	v_cms_rre_id AS cms_rre_id_Out,
	1 AS Dummy_Integer
	FROM SQ_Claim_File_Header1
),
LKP_work_claim_cms_query_extract AS (
	SELECT
	work_claim_cms_query_extract_id,
	IN_Dummy_Integer
	FROM (
		SELECT COUNT(*) as work_claim_cms_query_extract_id, 1 as IN_Dummy_Integer 
		FROM work_claim_cms_query_extract
		WHERE audit_id = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY IN_Dummy_Integer ORDER BY work_claim_cms_query_extract_id) = 1
),
EXP_Trailer AS (
	SELECT
	'T0' AS Trailer_Indicator,
	EXPTRANS.cms_rre_id_Out AS RRE_ID,
	'IACT' AS File_Type,
	-- *INF*: TO_CHAR(SYSDATE,'YYYYMMDD')
	TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDD') AS Cycle_Date,
	LKP_work_claim_cms_query_extract.work_claim_cms_query_extract_id,
	EXPTRANS.audit_id,
	-- *INF*: LPAD(TO_CHAR(work_claim_cms_query_extract_id),9,'0')
	LPAD(TO_CHAR(work_claim_cms_query_extract_id), 9, '0') AS o_Total_Number_Of_Records,
	o_Total_Number_Of_Records AS Record_Count,
	'' AS Filler
	FROM EXPTRANS
	LEFT JOIN LKP_work_claim_cms_query_extract
	ON LKP_work_claim_cms_query_extract.IN_Dummy_Integer = EXPTRANS.Dummy_Integer
),
CMS_Claims_Query_File_Trailer AS (
	INSERT INTO CMS_Claims_Query_File_Trailer
	(Trailer_Indicator, RRE_ID, File_Type, File_Date, Record_Count, Filler)
	SELECT 
	TRAILER_INDICATOR, 
	RRE_ID, 
	FILE_TYPE, 
	Cycle_Date AS FILE_DATE, 
	RECORD_COUNT, 
	FILLER
	FROM EXP_Trailer
),
WITH
LKP_sup_cms_tin_office AS (
	SELECT
	cms_rre_id,
	dummy_integer
	FROM (
		SELECT 
		count(*) as cms_rre_id, 
		1 as dummy_integer 
		FROM sup_cms_tin_office
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY dummy_integer ORDER BY cms_rre_id) = 1
),
SQ_TIN_Reference_File_Header AS (
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
	FROM sup_cms_tin_office2
	WHERE crrnt_snpsht_flag=1
),
EXP_Header AS (
	SELECT
	'NGTH' AS Record_Identifier,
	cms_rre_id,
	-- *INF*: lpad(rtrim(cms_rre_id),9,'0')
	lpad(rtrim(cms_rre_id), 9, '0') AS cms_rre_id1,
	'NGHPTIN' AS Reporting_File_Type,
	-- *INF*: TO_CHAR(SYSDATE,'YYYYMMDD')
	TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDD') AS File_Submission_date,
	'' AS Default_String
	FROM SQ_TIN_Reference_File_Header
),
CMS_TIN_Reference_File_Header AS (
	INSERT INTO CMS_TIN_Reference_File_HeaderTrailer
	(Record_Identifier, Reporter_Id, Reporting_File_Type, File_Submission_Date)
	SELECT 
	RECORD_IDENTIFIER, 
	cms_rre_id1 AS REPORTER_ID, 
	REPORTING_FILE_TYPE, 
	File_Submission_date AS FILE_SUBMISSION_DATE
	FROM EXP_Header
),
SQ_sup_cms_tin_office AS (
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
EXP_Source AS (
	SELECT
	'NGTD' AS Record_identifier,
	cms_rre_id,
	-- *INF*: lpad(rtrim(cms_rre_id),9,'0')
	lpad(rtrim(cms_rre_id), 9, '0') AS cms_rre_id1,
	office_tin_num,
	office_code,
	office_name,
	office_mail_address1,
	office_mail_address2,
	office_mail_city,
	office_mail_state,
	office_mail_zip,
	office_mail_zip4
	FROM SQ_sup_cms_tin_office
),
EXP_Target AS (
	SELECT
	Record_identifier,
	cms_rre_id1 AS cms_rre_id,
	office_tin_num,
	office_code,
	office_name,
	office_mail_address1,
	office_mail_address2,
	office_mail_city,
	office_mail_state,
	office_mail_zip,
	office_mail_zip4,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(cms_rre_id)
	UDF_DEFAULT_VALUE_TO_BLANKS(cms_rre_id) AS cms_rre_id1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(office_tin_num)
	UDF_DEFAULT_VALUE_TO_BLANKS(office_tin_num) AS office_tin_num1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(office_code)
	UDF_DEFAULT_VALUE_TO_BLANKS(office_code) AS office_code1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(office_name)
	UDF_DEFAULT_VALUE_TO_BLANKS(office_name) AS office_name1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(office_mail_address1)
	UDF_DEFAULT_VALUE_TO_BLANKS(office_mail_address1) AS office_mail_address11,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(office_mail_address2)
	UDF_DEFAULT_VALUE_TO_BLANKS(office_mail_address2) AS office_mail_address21,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(office_mail_city)
	UDF_DEFAULT_VALUE_TO_BLANKS(office_mail_city) AS office_mail_city1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(office_mail_state)
	UDF_DEFAULT_VALUE_TO_BLANKS(office_mail_state) AS office_mail_state1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(office_mail_zip)
	UDF_DEFAULT_VALUE_TO_BLANKS(office_mail_zip) AS office_mail_zip1,
	-- *INF*: IIF(ISNULL(office_mail_zip4) OR office_mail_zip4='N/A','0000',office_mail_zip4)
	-- 
	IFF(office_mail_zip4 IS NULL OR office_mail_zip4 = 'N/A', '0000', office_mail_zip4) AS office_mail_zip41,
	'' AS Default_String
	FROM EXP_Source
),
CMS_TIN_Reference_File_Detail AS (
	INSERT INTO CMS_TIN_Reference_File_Detail
	(Record_Identifier, Reporter_Id, TIN, Office_Code, Mailing_Name, Mailing_Address_Line1, Mailing_Address_Line112, City, State, Zip, Zip4, Reserved)
	SELECT 
	Record_identifier AS RECORD_IDENTIFIER, 
	cms_rre_id1 AS REPORTER_ID, 
	office_tin_num1 AS TIN, 
	office_code1 AS OFFICE_CODE, 
	office_name1 AS MAILING_NAME, 
	office_mail_address11 AS MAILING_ADDRESS_LINE1, 
	office_mail_address21 AS MAILING_ADDRESS_LINE112, 
	office_mail_city1 AS CITY, 
	office_mail_state1 AS STATE, 
	office_mail_zip1 AS ZIP, 
	office_mail_zip41 AS ZIP4, 
	Default_String AS RESERVED
	FROM EXP_Target
),
SQ_TIN_Reference_File_Trailer AS (
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
EXP_Trailer AS (
	SELECT
	'NGTT' AS Record_Identifier,
	cms_rre_id,
	-- *INF*: lpad(rtrim(cms_rre_id),9,'0')
	lpad(rtrim(cms_rre_id), 9, '0') AS cms_rre_id1,
	'NGHPTIN' AS Reporting_File_Type,
	-- *INF*: TO_CHAR(SYSDATE,'YYYYMMDD')
	TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDD') AS File_Submission_date,
	-- *INF*: :LKP.LKP_sup_cms_tin_office(1)
	LKP_SUP_CMS_TIN_OFFICE_1.cms_rre_id AS v_File_Record_Count,
	-- *INF*: LPAD(v_File_Record_Count,7,'0')
	LPAD(v_File_Record_Count, 7, '0') AS File_Record_Count,
	'' AS Default_String
	FROM SQ_TIN_Reference_File_Trailer
	LEFT JOIN LKP_SUP_CMS_TIN_OFFICE LKP_SUP_CMS_TIN_OFFICE_1
	ON LKP_SUP_CMS_TIN_OFFICE_1.dummy_integer = 1

),
CMS_TIN_Reference_File_Trailer AS (
	INSERT INTO CMS_TIN_Reference_File_HeaderTrailer
	(Record_Identifier, Reporter_Id, Reporting_File_Type, File_Submission_Date, File_Record_Count)
	SELECT 
	RECORD_IDENTIFIER, 
	cms_rre_id1 AS REPORTER_ID, 
	REPORTING_FILE_TYPE, 
	File_Submission_date AS FILE_SUBMISSION_DATE, 
	FILE_RECORD_COUNT
	FROM EXP_Trailer
),
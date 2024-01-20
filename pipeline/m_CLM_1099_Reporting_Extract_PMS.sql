WITH
SQ_1099_Reporting_PMS AS (
	SELECT
	DISTINCT
	C.IRS_NAME,
	C.ADDRESS_LINE_1,
	C.ADDRESS_LINE_2,
	C.CITY,
	C.STATE_CODE,
	C.ZIP_CODE,
	C.SEARCH_TAX_ID,
	C.TAX_ID_TYPE,
	C.TAX_ID,
	C.COUNTRY_CODE,
	C.VENDOR_TYPE_CD,
	SUM(A.DraftAmt) AS PAID_AMT
	FROM
	Pif4578RecStage A with (nolock) 
	
	INNER JOIN 
	pms_adjuster_master_stage B with (nolock) 
	ON A.AdjustorNo = B.adnm_adjustor_nbr 
	
	INNER JOIN 
	Master1099ListMonthlyStage C with (nolock) 
	ON B.adnm_taxid_ssn = C.TAX_ID
	
	WHERE
	A.TransDate >= (SELECT      DATEADD(YEAR, DATEDIFF(YEAR, 0,DATEADD(m,-1,getdate())),0)
	            'First Day of Year using previous month date') 
	AND
	A.TransDate <= (SELECT DATEADD(s,-1,DATEADD(mm, DATEDIFF(m,0,GETDATE())-@{pipeline().parameters.NO_OF_MONTHS},0))
	LastDay_PreviousMonth) AND
	C.REPORTABLE_IND = 'Y' AND
	A.PAIDRESERVEAMT >0 AND
	C.Is_Valid='Y' AND 
	B.adnm_taxid_ssn <> ''
	
	GROUP BY
	C.IRS_NAME,
	C.ADDRESS_LINE_1,
	C.ADDRESS_LINE_2,
	C.CITY,
	C.STATE_CODE,
	C.ZIP_CODE,
	C.SEARCH_TAX_ID,
	C.TAX_ID_TYPE,
	C.TAX_ID,
	C.COUNTRY_CODE,
	C.VENDOR_TYPE_CD
),
EXP_cleanse_input AS (
	SELECT
	irs_name,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(irs_name)
	UDF_DEFAULT_VALUE_FOR_STRINGS(irs_name) AS irs_name_out,
	address_line_1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(address_line_1)
	UDF_DEFAULT_VALUE_FOR_STRINGS(address_line_1) AS address_line_1_out,
	address_line_2,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(address_line_2)
	UDF_DEFAULT_VALUE_FOR_STRINGS(address_line_2) AS address_line_2_out,
	city,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(city)
	UDF_DEFAULT_VALUE_FOR_STRINGS(city) AS city_out,
	state_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(state_code)
	UDF_DEFAULT_VALUE_FOR_STRINGS(state_code) AS state_code_out,
	zip_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(zip_code)
	UDF_DEFAULT_VALUE_FOR_STRINGS(zip_code) AS zip_code_out,
	search_tax_id,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(search_tax_id)
	UDF_DEFAULT_VALUE_FOR_STRINGS(search_tax_id) AS search_tax_id_out,
	tax_id_type,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(tax_id_type)
	UDF_DEFAULT_VALUE_FOR_STRINGS(tax_id_type) AS tax_id_type_out,
	tax_id,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(tax_id)
	UDF_DEFAULT_VALUE_FOR_STRINGS(tax_id) AS tax_id_out,
	country_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(country_code)
	UDF_DEFAULT_VALUE_FOR_STRINGS(country_code) AS country_code_out,
	vendor_type_cd,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(vendor_type_cd)
	UDF_DEFAULT_VALUE_FOR_STRINGS(vendor_type_cd) AS vendor_type_cd_out,
	DraftAmt AS ctx_trs_amt,
	-- *INF*: IIF(ISNULL(ctx_trs_amt),0,ctx_trs_amt)
	IFF(ctx_trs_amt IS NULL, 0, ctx_trs_amt) AS ctx_trs_amt_out
	FROM SQ_1099_Reporting_PMS
),
EXP_output AS (
	SELECT
	1 AS default_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	SYSDATE AS default_date,
	'PMS' AS source_sys_id,
	irs_name_out AS irs_name,
	address_line_1_out AS address_line_1,
	address_line_2_out AS address_line_2,
	city_out AS city,
	state_code_out AS state_code,
	zip_code_out AS zip_code,
	search_tax_id_out AS search_tax_id,
	tax_id_type_out AS tax_id_type,
	tax_id_out AS tax_id,
	country_code_out AS country_code,
	vendor_type_cd_out AS vendor_type_cd,
	ctx_trs_amt_out AS ctx_trs_amt
	FROM EXP_cleanse_input
),
Work1099Reporting AS (
	TRUNCATE TABLE Work1099Reporting;
	INSERT INTO Work1099Reporting
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, IRSName, AddressLine1, AddressLine2, City, StateCode, ZipCode, CountryCode, SearchTaxId, TaxIdType, TaxId, VendorTypeCode, PaidAmount)
	SELECT 
	default_id AS CURRENTSNAPSHOTFLAG, 
	audit_id AS AUDITID, 
	default_date AS EFFECTIVEDATE, 
	default_date AS EXPIRATIONDATE, 
	source_sys_id AS SOURCESYSTEMID, 
	default_date AS CREATEDDATE, 
	default_date AS MODIFIEDDATE, 
	irs_name AS IRSNAME, 
	address_line_1 AS ADDRESSLINE1, 
	address_line_2 AS ADDRESSLINE2, 
	city AS CITY, 
	state_code AS STATECODE, 
	zip_code AS ZIPCODE, 
	country_code AS COUNTRYCODE, 
	search_tax_id AS SEARCHTAXID, 
	tax_id_type AS TAXIDTYPE, 
	tax_id AS TAXID, 
	vendor_type_cd AS VENDORTYPECODE, 
	ctx_trs_amt AS PAIDAMOUNT
	FROM EXP_output
),
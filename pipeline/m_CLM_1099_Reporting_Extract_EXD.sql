WITH
SQ_1099_Reporting_EXD AS (
	SELECT DISTINCT CML.irs_name,
		CML.address_line_1,
		CML.address_line_2,
		CML.city,
		CML.state_code,
		CML.zip_code,
		CML.search_tax_id,
		CML.tax_id_type,
		CML.tax_id,
		CML.country_code,
		CML.vendor_type_cd,
		SUM(CT.ctx_trs_amt) AS Paid_Amt
	FROM claim_transaction_full_extract_stage CT WITH (NOLOCK)
	INNER JOIN ClaimDraftMonthlyStage CDM WITH (NOLOCK) ON CT.ctx_draft_nbr = CDM.dft_draft_nbr
		AND CT.ctx_claim_nbr = CDM.dft_claim_nbr
	INNER JOIN Master1099ListMonthlyStage CML WITH (NOLOCK) ON CDM.dft_tax_id_nbr = CML.search_tax_id
		AND CDM.dft_tax_id_type_cd = CML.tax_id_type
	WHERE
		-- CT.ctx_trs_dt between '2013-01-01' and '2013-05-31' -- make year begin, year end (current or prior?)
	     --  use (-1) for @{pipeline().parameters.NO_OF_MONTHS} to include current month, otherwise 0 will provide previous month
		(CT.ctx_trs_dt >= ( SELECT DATEADD(YEAR, DATEDIFF(YEAR, 0, DATEADD(m, - 1, getdate())), 0) 'First Day of Year using previous month date' )
		   AND CT.ctx_trs_dt <= ( SELECT DATEADD(s, -1, DATEADD(mm, DATEDIFF(m, 0, GETDATE()) - @{pipeline().parameters.NO_OF_MONTHS}, 0)) LastDay_PreviousMonth ))
		AND CT.source_system_id = 'EXCEED'
		AND CML.reportable_ind = 'Y'
		AND CML.is_valid = 'Y'
		AND CDM.dft_dbs_status_cd IN ('P', 'D', 'U')
		AND NOT EXISTS (SELECT 1
			FROM @{pipeline().parameters.DATABASE_NAME_IL}.dbo.claim_payment cpa
			JOIN @{pipeline().parameters.DATABASE_NAME_IL}.dbo.sup_payment_method pm on pm.sup_payment_method_id = cpa.sup_payment_method_id
				and pm.payment_method IN ('Virtual Payment','Debit Card','Digital Prepaid','CAT Card','PayPal','Venmo','Electronic to Lienholders')
			WHERE cpa.claim_pay_num = CDM.dft_draft_nbr) 
	 @{pipeline().parameters.WHERE_CLAUSE} 
	GROUP BY CML.irs_name,
		CML.address_line_1,
		CML.address_line_2,
		CML.city,
		CML.state_code,
		CML.zip_code,
		CML.search_tax_id,
		CML.tax_id_type,
		CML.tax_id,
		CML.country_code,
		CML.vendor_type_cd
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
	ctx_trs_amt,
	-- *INF*: IIF(ISNULL(ctx_trs_amt),0,ctx_trs_amt)
	IFF(ctx_trs_amt IS NULL, 0, ctx_trs_amt) AS ctx_trs_amt_out
	FROM SQ_1099_Reporting_EXD
),
EXP_output AS (
	SELECT
	1 AS default_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	SYSDATE AS default_date,
	'EXCEED' AS source_sys_id,
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
SQ_vendor_dba_1099_stage AS (
	select  
	rtrim(A.tax_id) as tax_id, 
	rtrim(A.vendor_type_cd) as vendor_type_cd 
	from  (
	select 
	vendor_type_cd, tax_id, ROW_NUMBER() over (partition by tax_id order by modified_ts desc  ) as rn
	from dbo.vendor_dba_1099_stage 
		where 
		delete_flag='N' and 
		tax_id is not null and
		vendor_type_cd is not null
		and tax_id != '00-0000000'
	) A
	where rn=1
	order by 1
),
EXP_VendorDBA1099_Input AS (
	SELECT
	tax_id,
	vendor_type_cd
	FROM SQ_vendor_dba_1099_stage
),
SQ_Work1099Reporting AS (
	SELECT 
	Work1099Reporting.Work1099ReportingId, 
	rtrim(Work1099Reporting.TaxId) as TaxId
	FROM
	Work1099Reporting
	Where
	VendorTypeCode='N/A'
	@{pipeline().parameters.WHERE}
	order by 2
),
EXP_work1099Reporting_Input AS (
	SELECT
	Work1099ReportingId,
	TaxId
	FROM SQ_Work1099Reporting
),
JNR_work1099Reporting_VendorDBA AS (SELECT
	EXP_VendorDBA1099_Input.tax_id, 
	EXP_VendorDBA1099_Input.vendor_type_cd, 
	EXP_work1099Reporting_Input.Work1099ReportingId, 
	EXP_work1099Reporting_Input.TaxId
	FROM EXP_VendorDBA1099_Input
	INNER JOIN EXP_work1099Reporting_Input
	ON EXP_work1099Reporting_Input.TaxId = EXP_VendorDBA1099_Input.tax_id
),
EXP_Join_output AS (
	SELECT
	vendor_type_cd,
	Work1099ReportingId
	FROM JNR_work1099Reporting_VendorDBA
),
SRT_Work1099ReportingId AS (
	SELECT
	Work1099ReportingId, 
	vendor_type_cd
	FROM EXP_Join_output
	ORDER BY Work1099ReportingId ASC
),
UPD_Update_work1099Reporting AS (
	SELECT
	Work1099ReportingId, 
	vendor_type_cd
	FROM SRT_Work1099ReportingId
),
EXP_Output_Update AS (
	SELECT
	Work1099ReportingId,
	SYSDATE AS modifieddate,
	vendor_type_cd
	FROM UPD_Update_work1099Reporting
),
Work1099Reporting_Update AS (
	INSERT INTO Work1099Reporting
	(Work1099ReportingId, ModifiedDate, VendorTypeCode)
	SELECT 
	WORK1099REPORTINGID, 
	modifieddate AS MODIFIEDDATE, 
	vendor_type_cd AS VENDORTYPECODE
	FROM EXP_Output_Update
),
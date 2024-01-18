WITH
SQ_customer AS (
	SELECT
		cust_id,
		prdcr_id,
		agency_code,
		assoc_chc_id,
		lgl_ent_chc_id,
		name,
		lgl_name,
		main_ph_num,
		main_bus_email_addr,
		main_bus_web_site_url,
		addr_line_1,
		addr_line_2,
		contact_info,
		contact_ph_num,
		other_lgl_ent_descript,
		city,
		state_or_prov_chc_id,
		county,
		postal_code,
		agency_cust_num,
		pms_cust_num,
		created_user_id,
		created_date,
		modified_user_id,
		modified_date,
		yrs_in_bus,
		non_wbmi_prior_cov_flag,
		wbmi_prior_cov_flag,
		pol_cncld_dclnd_flag,
		prior_losses_flag,
		paper_loss_smr_faxed_or_mailed_flag,
		add_supporting_documentation_flag,
		hidden_flag,
		serv_center_flag,
		named_insd,
		doing_bus_as,
		fein,
		producer_code,
		rollover_bus_flag,
		review_cust_flag,
		review_cust_user_id,
		review_cust_date
	FROM customer
),
EXP_Values AS (
	SELECT
	cust_id,
	prdcr_id,
	agency_code,
	assoc_chc_id,
	lgl_ent_chc_id,
	name,
	lgl_name,
	main_ph_num,
	main_bus_email_addr,
	main_bus_web_site_url,
	addr_line_1,
	addr_line_2,
	contact_info,
	contact_ph_num,
	other_lgl_ent_descript,
	city,
	state_or_prov_chc_id,
	county,
	postal_code,
	agency_cust_num,
	pms_cust_num,
	created_user_id,
	created_date,
	modified_user_id,
	modified_date,
	yrs_in_bus,
	non_wbmi_prior_cov_flag,
	wbmi_prior_cov_flag,
	pol_cncld_dclnd_flag,
	prior_losses_flag,
	paper_loss_smr_faxed_or_mailed_flag,
	add_supporting_documentation_flag,
	hidden_flag,
	serv_center_flag,
	named_insd,
	doing_bus_as,
	fein,
	producer_code,
	rollover_bus_flag,
	review_cust_flag,
	review_cust_user_id,
	review_cust_date,
	SYSDATE AS extract_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_system_id
	FROM SQ_customer
),
customer_cl_stage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.customer_cl_stage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.customer_cl_stage
	(cust_id, prdcr_id, agency_code, assoc_chc_id, lgl_ent_chc_id, name, lgl_name, main_ph_num, main_bus_email_addr, main_bus_web_site_url, addr_line_1, addr_line_2, contact_info, contact_ph_num, other_lgl_ent_descript, city, state_or_prov_chc_id, county, postal_code, agency_cust_num, pms_cust_num, created_user_id, created_date, modified_user_id, modified_date, yrs_in_bus, non_wbmi_prior_cov_flag, wbmi_prior_cov_flag, pol_cncld_dclnd_flag, prior_losses_flag, paper_loss_smr_faxed_or_mailed_flag, add_supporting_documentation_flag, hidden_flag, serv_center_flag, named_insd, doing_bus_as, fein, producer_code, rollover_bus_flag, review_cust_flag, review_cust_user_id, review_cust_date, extract_date, source_system_id)
	SELECT 
	CUST_ID, 
	PRDCR_ID, 
	AGENCY_CODE, 
	ASSOC_CHC_ID, 
	LGL_ENT_CHC_ID, 
	NAME, 
	LGL_NAME, 
	MAIN_PH_NUM, 
	MAIN_BUS_EMAIL_ADDR, 
	MAIN_BUS_WEB_SITE_URL, 
	ADDR_LINE_1, 
	ADDR_LINE_2, 
	CONTACT_INFO, 
	CONTACT_PH_NUM, 
	OTHER_LGL_ENT_DESCRIPT, 
	CITY, 
	STATE_OR_PROV_CHC_ID, 
	COUNTY, 
	POSTAL_CODE, 
	AGENCY_CUST_NUM, 
	PMS_CUST_NUM, 
	CREATED_USER_ID, 
	CREATED_DATE, 
	MODIFIED_USER_ID, 
	MODIFIED_DATE, 
	YRS_IN_BUS, 
	NON_WBMI_PRIOR_COV_FLAG, 
	WBMI_PRIOR_COV_FLAG, 
	POL_CNCLD_DCLND_FLAG, 
	PRIOR_LOSSES_FLAG, 
	PAPER_LOSS_SMR_FAXED_OR_MAILED_FLAG, 
	ADD_SUPPORTING_DOCUMENTATION_FLAG, 
	HIDDEN_FLAG, 
	SERV_CENTER_FLAG, 
	NAMED_INSD, 
	DOING_BUS_AS, 
	FEIN, 
	PRODUCER_CODE, 
	ROLLOVER_BUS_FLAG, 
	REVIEW_CUST_FLAG, 
	REVIEW_CUST_USER_ID, 
	REVIEW_CUST_DATE, 
	EXTRACT_DATE, 
	SOURCE_SYSTEM_ID
	FROM EXP_Values
),
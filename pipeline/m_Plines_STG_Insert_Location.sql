WITH
SQ_location AS (
	SELECT
		loc_seq,
		policy_num,
		policy_sym,
		policy_mod,
		policy_mco,
		cov_part_type_code,
		ho_type_code,
		hm_usage_type_code,
		loc_num,
		street,
		city,
		state_type_code,
		zip_type_code,
		county_type_code,
		section,
		town,
		range,
		town_of,
		credit_exist_ins_exp_date,
		market_desire_value_flag,
		bill_to_type_code,
		bill_time_type_code,
		const_type_code,
		built_year,
		market_value,
		replacement_cost,
		struct_type_code,
		occupancy_type_code,
		num_of_family,
		num_of_apt,
		territory,
		protect_class_type_code,
		hydrant_dist_type_code,
		fire_station_dis,
		resp_fire_dept,
		hydrant_excl_flag,
		protect_device_type_code,
		fire_place_type_code,
		protect_class_rate_type_code,
		suburban_flag,
		roof_type_code,
		standard_amps_flag,
		elec_serv_type_code,
		row_house_fire_wall_flag,
		woodbrn_stove_flag,
		swimming_pool_flag,
		pool_type_code,
		slide_diving_type_code,
		num_of_hm_in_sub_dev,
		inside_city_limit_flag,
		bus_on_premises_flag,
		bus_on_premises_descript,
		num_of_emp,
		flood_plain_flag,
		renovation_flag,
		renovation_descript,
		trampoline_flag,
		trampoline_descript,
		built_other_than_res_flag,
		built_other_than_res_descript,
		log_intricate_corner_flag,
		log_const_system_flag,
		log_frame_flag,
		roomer_boarder_flag,
		roomer_boarder_num,
		full_time_residence_flag,
		add_int_na_flag,
		loss_history_na_flag,
		modified_date,
		modified_user_id,
		visited_status_type_code,
		mine_subsidence_rejection_flag,
		deposit_amt_applied_flag,
		deposit_amt_applied,
		const_eifs_flag,
		pool_depth_by_diving_board,
		tax_location,
		protected_suburban_class_flag,
		num_of_occupied_dwellings_flag,
		water_source_and_pumper_truck_flag,
		pumper_truck_gallons_flag,
		miles_from_populated_municipality_flag,
		daycare_flag,
		ten_or_more_homes_flag,
		accessible_year_round_flag,
		within_five_miles_of_fd_flag,
		dwelling_fire_app,
		manually_rated_code,
		loc_status_type_code,
		const_type_code_change_flag,
		struct_type_code_change_flag,
		endorsement_view_code,
		pif_legal_addr,
		pif_legal_addr_change_flag,
		pif_home_disc,
		woodbrn_stove_flag_change_flag,
		pif_woodbrn_stove_prem,
		protect_class_type_code_change_flag,
		signed_other_struct_excl_flag,
		pif_hm_usage_type_code,
		pif_market_desire_value_flag,
		pif_woodbrn_stove_flag,
		pif_built_year,
		pif_num_of_family,
		pif_protect_class_type_code,
		builders_risk_flag
	FROM location
),
EXP_LOCATION AS (
	SELECT
	loc_seq,
	policy_num,
	policy_sym,
	policy_mod,
	policy_mco,
	cov_part_type_code,
	ho_type_code,
	hm_usage_type_code,
	loc_num,
	street,
	city,
	state_type_code,
	zip_type_code,
	county_type_code,
	section,
	town,
	range,
	town_of,
	credit_exist_ins_exp_date,
	market_desire_value_flag,
	bill_to_type_code,
	bill_time_type_code,
	const_type_code,
	built_year,
	market_value,
	replacement_cost,
	struct_type_code,
	occupancy_type_code,
	num_of_family,
	num_of_apt,
	territory,
	protect_class_type_code,
	hydrant_dist_type_code,
	fire_station_dis,
	resp_fire_dept,
	hydrant_excl_flag,
	protect_device_type_code,
	fire_place_type_code,
	protect_class_rate_type_code,
	suburban_flag,
	roof_type_code,
	standard_amps_flag,
	elec_serv_type_code,
	row_house_fire_wall_flag,
	woodbrn_stove_flag,
	swimming_pool_flag,
	pool_type_code,
	slide_diving_type_code,
	num_of_hm_in_sub_dev,
	inside_city_limit_flag,
	bus_on_premises_flag,
	bus_on_premises_descript,
	num_of_emp,
	flood_plain_flag,
	renovation_flag,
	renovation_descript,
	trampoline_flag,
	trampoline_descript,
	built_other_than_res_flag,
	built_other_than_res_descript,
	log_intricate_corner_flag,
	log_const_system_flag,
	log_frame_flag,
	roomer_boarder_flag,
	roomer_boarder_num,
	full_time_residence_flag,
	add_int_na_flag,
	loss_history_na_flag,
	modified_date,
	modified_user_id,
	visited_status_type_code,
	mine_subsidence_rejection_flag,
	deposit_amt_applied_flag,
	deposit_amt_applied,
	const_eifs_flag,
	pool_depth_by_diving_board,
	tax_location,
	protected_suburban_class_flag,
	num_of_occupied_dwellings_flag,
	water_source_and_pumper_truck_flag,
	pumper_truck_gallons_flag,
	miles_from_populated_municipality_flag,
	daycare_flag,
	ten_or_more_homes_flag,
	accessible_year_round_flag,
	within_five_miles_of_fd_flag,
	dwelling_fire_app,
	manually_rated_code,
	loc_status_type_code,
	const_type_code_change_flag,
	struct_type_code_change_flag,
	endorsement_view_code,
	pif_legal_addr,
	pif_legal_addr_change_flag,
	pif_home_disc,
	woodbrn_stove_flag_change_flag,
	pif_woodbrn_stove_prem,
	protect_class_type_code_change_flag,
	signed_other_struct_excl_flag,
	pif_hm_usage_type_code,
	pif_market_desire_value_flag,
	pif_woodbrn_stove_flag,
	pif_built_year,
	pif_num_of_family,
	pif_protect_class_type_code,
	builders_risk_flag,
	SYSDATE AS EXTRACT_DATE,
	SYSDATE AS AS_OF_DATE,
	'' AS RECORD_COUNT,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID
	FROM SQ_location
),
location_plines_stage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.location_plines_stage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.location_plines_stage
	(loc_seq, policy_num, policy_sym, policy_mod, policy_mco, cov_part_type_code, ho_type_code, hm_usage_type_code, loc_num, street, city, state_type_code, zip_type_code, county_type_code, section, town, range, town_of, credit_exist_ins_exp_date, market_desire_value_flag, bill_to_type_code, bill_time_type_code, const_type_code, built_year, market_value, replacement_cost, struct_type_code, occupancy_type_code, num_of_family, num_of_apt, territory, protect_class_type_code, hydrant_dist_type_code, fire_station_dis, resp_fire_dept, hydrant_excl_flag, protect_device_type_code, fire_place_type_code, protect_class_rate_type_code, suburban_flag, roof_type_code, standard_amps_flag, elec_serv_type_code, row_house_fire_wall_flag, woodbrn_stove_flag, swimming_pool_flag, pool_type_code, slide_diving_type_code, num_of_hm_in_sub_dev, inside_city_limit_flag, bus_on_premises_flag, bus_on_premises_descript, num_of_emp, flood_plain_flag, renovation_flag, renovation_descript, trampoline_flag, trampoline_descript, built_other_than_res_flag, built_other_than_res_descript, log_intricate_corner_flag, log_const_system_flag, log_frame_flag, roomer_boarder_flag, roomer_boarder_num, full_time_residence_flag, add_int_na_flag, loss_history_na_flag, modified_date, modified_user_id, visited_status_type_code, mine_subsidence_rejection_flag, deposit_amt_applied_flag, deposit_amt_applied, const_eifs_flag, pool_depth_by_diving_board, tax_location, protected_suburban_class_flag, num_of_occupied_dwellings_flag, water_source_and_pumper_truck_flag, pumper_truck_gallons_flag, miles_from_populated_municipality_flag, daycare_flag, ten_or_more_homes_flag, accessible_year_round_flag, within_five_miles_of_fd_flag, dwelling_fire_app, manually_rated_code, loc_status_type_code, const_type_code_change_flag, struct_type_code_change_flag, endorsement_view_code, pif_legal_addr, pif_legal_addr_change_flag, pif_home_disc, woodbrn_stove_flag_change_flag, pif_woodbrn_stove_prem, protect_class_type_code_change_flag, signed_other_struct_excl_flag, pif_hm_usage_type_code, pif_market_desire_value_flag, pif_woodbrn_stove_flag, pif_built_year, pif_num_of_family, pif_protect_class_type_code, builders_risk_flag, extract_date, as_of_date, record_count, source_system_id)
	SELECT 
	LOC_SEQ, 
	POLICY_NUM, 
	POLICY_SYM, 
	POLICY_MOD, 
	POLICY_MCO, 
	COV_PART_TYPE_CODE, 
	HO_TYPE_CODE, 
	HM_USAGE_TYPE_CODE, 
	LOC_NUM, 
	STREET, 
	CITY, 
	STATE_TYPE_CODE, 
	ZIP_TYPE_CODE, 
	COUNTY_TYPE_CODE, 
	SECTION, 
	TOWN, 
	RANGE, 
	TOWN_OF, 
	CREDIT_EXIST_INS_EXP_DATE, 
	MARKET_DESIRE_VALUE_FLAG, 
	BILL_TO_TYPE_CODE, 
	BILL_TIME_TYPE_CODE, 
	CONST_TYPE_CODE, 
	BUILT_YEAR, 
	MARKET_VALUE, 
	REPLACEMENT_COST, 
	STRUCT_TYPE_CODE, 
	OCCUPANCY_TYPE_CODE, 
	NUM_OF_FAMILY, 
	NUM_OF_APT, 
	TERRITORY, 
	PROTECT_CLASS_TYPE_CODE, 
	HYDRANT_DIST_TYPE_CODE, 
	FIRE_STATION_DIS, 
	RESP_FIRE_DEPT, 
	HYDRANT_EXCL_FLAG, 
	PROTECT_DEVICE_TYPE_CODE, 
	FIRE_PLACE_TYPE_CODE, 
	PROTECT_CLASS_RATE_TYPE_CODE, 
	SUBURBAN_FLAG, 
	ROOF_TYPE_CODE, 
	STANDARD_AMPS_FLAG, 
	ELEC_SERV_TYPE_CODE, 
	ROW_HOUSE_FIRE_WALL_FLAG, 
	WOODBRN_STOVE_FLAG, 
	SWIMMING_POOL_FLAG, 
	POOL_TYPE_CODE, 
	SLIDE_DIVING_TYPE_CODE, 
	NUM_OF_HM_IN_SUB_DEV, 
	INSIDE_CITY_LIMIT_FLAG, 
	BUS_ON_PREMISES_FLAG, 
	BUS_ON_PREMISES_DESCRIPT, 
	NUM_OF_EMP, 
	FLOOD_PLAIN_FLAG, 
	RENOVATION_FLAG, 
	RENOVATION_DESCRIPT, 
	TRAMPOLINE_FLAG, 
	TRAMPOLINE_DESCRIPT, 
	BUILT_OTHER_THAN_RES_FLAG, 
	BUILT_OTHER_THAN_RES_DESCRIPT, 
	LOG_INTRICATE_CORNER_FLAG, 
	LOG_CONST_SYSTEM_FLAG, 
	LOG_FRAME_FLAG, 
	ROOMER_BOARDER_FLAG, 
	ROOMER_BOARDER_NUM, 
	FULL_TIME_RESIDENCE_FLAG, 
	ADD_INT_NA_FLAG, 
	LOSS_HISTORY_NA_FLAG, 
	MODIFIED_DATE, 
	MODIFIED_USER_ID, 
	VISITED_STATUS_TYPE_CODE, 
	MINE_SUBSIDENCE_REJECTION_FLAG, 
	DEPOSIT_AMT_APPLIED_FLAG, 
	DEPOSIT_AMT_APPLIED, 
	CONST_EIFS_FLAG, 
	POOL_DEPTH_BY_DIVING_BOARD, 
	TAX_LOCATION, 
	PROTECTED_SUBURBAN_CLASS_FLAG, 
	NUM_OF_OCCUPIED_DWELLINGS_FLAG, 
	WATER_SOURCE_AND_PUMPER_TRUCK_FLAG, 
	PUMPER_TRUCK_GALLONS_FLAG, 
	MILES_FROM_POPULATED_MUNICIPALITY_FLAG, 
	DAYCARE_FLAG, 
	TEN_OR_MORE_HOMES_FLAG, 
	ACCESSIBLE_YEAR_ROUND_FLAG, 
	WITHIN_FIVE_MILES_OF_FD_FLAG, 
	DWELLING_FIRE_APP, 
	MANUALLY_RATED_CODE, 
	LOC_STATUS_TYPE_CODE, 
	CONST_TYPE_CODE_CHANGE_FLAG, 
	STRUCT_TYPE_CODE_CHANGE_FLAG, 
	ENDORSEMENT_VIEW_CODE, 
	PIF_LEGAL_ADDR, 
	PIF_LEGAL_ADDR_CHANGE_FLAG, 
	PIF_HOME_DISC, 
	WOODBRN_STOVE_FLAG_CHANGE_FLAG, 
	PIF_WOODBRN_STOVE_PREM, 
	PROTECT_CLASS_TYPE_CODE_CHANGE_FLAG, 
	SIGNED_OTHER_STRUCT_EXCL_FLAG, 
	PIF_HM_USAGE_TYPE_CODE, 
	PIF_MARKET_DESIRE_VALUE_FLAG, 
	PIF_WOODBRN_STOVE_FLAG, 
	PIF_BUILT_YEAR, 
	PIF_NUM_OF_FAMILY, 
	PIF_PROTECT_CLASS_TYPE_CODE, 
	BUILDERS_RISK_FLAG, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID
	FROM EXP_LOCATION
),
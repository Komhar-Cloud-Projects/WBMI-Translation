WITH
ARCH_CLAIM_TAB_STAGE1 AS (

	-- TODO - To be translated
),
SQ_CLAIM_TAB_STAGE AS (
	SELECT
		claim_tab_id AS CLAIM_TAB_ID,
		clm_claim_nbr AS CLM_CLAIM_NBR,
		clm_cause_loss_cd AS CLM_CAUSE_LOSS_CD,
		clm_claim_comments AS CLM_CLAIM_COMMENTS,
		clm_csr_claim_nbr AS CLM_CSR_CLAIM_NBR,
		clm_community_cd AS CLM_COMMUNITY_CD,
		clm_create_ts AS CLM_CREATE_TS,
		clm_entry_opr_id AS CLM_ENTRY_OPR_ID,
		clm_pmsd_ts AS CLM_PMSD_TS,
		clm_occurrence_id AS CLM_OCCURRENCE_ID,
		clm_reported_dt AS CLM_REPORTED_DT,
		clm_status_cd AS CLM_STATUS_CD,
		clm_update_opr_id AS CLM_UPDATE_OPR_ID,
		clm_upd_ts AS CLM_UPD_TS,
		clm_loss_occ_cd AS CLM_LOSS_OCC_CD,
		clm_ato_chg_by AS CLM_ATO_CHG_BY,
		clm_ato_chg_dt AS CLM_ATO_CHG_DT,
		clm_co_asn_dt AS CLM_CO_ASN_DT,
		clm_co_asn_rsn_cd AS CLM_CO_ASN_RSN_CD,
		clm_dir_ato_lmt AS CLM_DIR_ATO_LMT,
		clm_postal_cd AS CLM_POSTAL_CD,
		clm_fin_ind AS CLM_FIN_IND,
		clm_discovery_dt AS CLM_DISCOVERY_DT,
		clm_xpn_ato_lmt AS CLM_XPN_ATO_LMT,
		clm_ho_report_ind AS CLM_HO_REPORT_IND,
		clm_litigation_ind AS CLM_LITIGATION_IND,
		clm_recovery_ind AS CLM_RECOVERY_IND,
		clm_loss_dt AS CLM_LOSS_DT,
		clm_loss_tm AS CLM_LOSS_TM,
		clm_loss_city AS CLM_LOSS_CITY,
		clm_loss_county AS CLM_LOSS_COUNTY,
		clm_loss_des_id AS CLM_LOSS_DES_ID,
		clm_loss_state_cd AS CLM_LOSS_STATE_CD,
		clm_major_case_dt AS CLM_MAJOR_CASE_DT,
		clm_maj_case_rea AS CLM_MAJ_CASE_REA,
		clm_med_ato_lmt AS CLM_MED_ATO_LMT,
		clm_not_claim_ind AS CLM_NOT_CLAIM_IND,
		not_of_occ_ind AS NOT_OF_OCC_IND,
		clm_pre_rpt_ind AS CLM_PRE_RPT_IND,
		clm_reported_tm AS CLM_REPORTED_TM,
		clm_demand_amt AS CLM_DEMAND_AMT,
		clm_archive_dt AS CLM_ARCHIVE_DT,
		clm_archive_plc_cd AS CLM_ARCHIVE_PLC_CD,
		clm_stampled_dt AS CLM_STAMPLED_DT,
		clm_type_cd AS CLM_TYPE_CD,
		clm_closed_dt AS CLM_CLOSED_DT,
		clm_severity_cd AS CLM_SEVERITY_CD,
		clm_indemnity_ind AS CLM_INDEMNITY_IND,
		clm_controvert_ind AS CLM_CONTROVERT_IND,
		clm_mediation_ind AS CLM_MEDIATION_IND,
		clm_doc_type_cd AS CLM_DOC_TYPE_CD,
		clm_loss_county_cd AS CLM_LOSS_COUNTY_CD,
		clm_res_lmt_ind AS CLM_RES_LMT_IND,
		clm_pmt_lmt_ind AS CLM_PMT_LMT_IND,
		clm_loss_place_id AS CLM_LOSS_PLACE_ID,
		clm_report_nbr AS CLM_REPORT_NBR,
		clm_violation_ind AS CLM_VIOLATION_IND,
		clm_road_type_cd AS CLM_ROAD_TYPE_CD,
		clm_road_srf_d AS CLM_ROAD_SRF_D,
		clm_speed_lim_nbr AS CLM_SPEED_LIM_NBR,
		clm_light_con_cd AS CLM_LIGHT_CON_CD,
		clm_densely_ind AS CLM_DENSELY_IND,
		clm_traf_light_ind AS CLM_TRAF_LIGHT_IND,
		clm_prc_sta_ind AS CLM_PRC_STA_IND,
		clm_claim_cat_cd AS CLM_CLAIM_CAT_CD,
		clm_subro_ind AS CLM_SUBRO_IND,
		clm_invest_ind AS CLM_INVEST_IND,
		clm_out_deduct_ind AS CLM_OUT_DEDUCT_IND,
		clm_subcont_ind AS CLM_SUBCONT_IND,
		clm_activity_sta AS CLM_ACTIVITY_STA,
		clm_rei_notify_dt AS CLM_REI_NOTIFY_DT,
		clm_emp_std_ind_cd AS CLM_EMP_STD_IND_CD,
		clm_at_fault_cd AS CLM_AT_FAULT_CD,
		clm_driver_nbr AS CLM_DRIVER_NBR,
		clm_drv_same_ind AS CLM_DRV_SAME_IND,
		clm_rep_to_car_dt AS CLM_REP_TO_CAR_DT,
		clm_viol_cit_desc AS CLM_VIOL_CIT_DESC,
		clm_how_clm_rptd AS CLM_HOW_CLM_RPTD,
		clm_method_rptd AS CLM_METHOD_RPTD,
		clm_agency_code AS CLM_AGENCY_CODE,
		extract_date AS EXTRACT_DATE,
		as_of_date AS AS_OF_DATE,
		record_count AS RECORD_COUNT,
		source_system_id AS SOURCE_SYSTEM_ID,
		clm_wc_cat_code,
		clm_primary_loc_code,
		clm_secondary_dept_code,
		clm_xact_transaction_id,
		clm_survey,
		clm_survey_recipient,
		clm_survey_contact_method,
		clm_survey_primary_handler
	FROM CLAIM_TAB_STAGE
),
EXP_CLAIM_TAB_STAGE AS (
	SELECT
	CLAIM_TAB_ID,
	CLM_CLAIM_NBR,
	CLM_CAUSE_LOSS_CD,
	CLM_CLAIM_COMMENTS,
	CLM_CSR_CLAIM_NBR,
	CLM_COMMUNITY_CD,
	CLM_CREATE_TS,
	CLM_ENTRY_OPR_ID,
	CLM_PMSD_TS,
	CLM_OCCURRENCE_ID,
	CLM_REPORTED_DT,
	CLM_STATUS_CD,
	CLM_UPDATE_OPR_ID,
	CLM_UPD_TS,
	CLM_LOSS_OCC_CD,
	CLM_ATO_CHG_BY,
	CLM_ATO_CHG_DT,
	CLM_CO_ASN_DT,
	CLM_CO_ASN_RSN_CD,
	CLM_DIR_ATO_LMT,
	CLM_POSTAL_CD,
	CLM_FIN_IND,
	CLM_DISCOVERY_DT,
	CLM_XPN_ATO_LMT,
	CLM_HO_REPORT_IND,
	CLM_LITIGATION_IND,
	CLM_RECOVERY_IND,
	CLM_LOSS_DT,
	CLM_LOSS_TM,
	CLM_LOSS_CITY,
	CLM_LOSS_COUNTY,
	CLM_LOSS_DES_ID,
	CLM_LOSS_STATE_CD,
	CLM_MAJOR_CASE_DT,
	CLM_MAJ_CASE_REA,
	CLM_MED_ATO_LMT,
	CLM_NOT_CLAIM_IND,
	NOT_OF_OCC_IND,
	CLM_PRE_RPT_IND,
	CLM_REPORTED_TM,
	CLM_DEMAND_AMT,
	CLM_ARCHIVE_DT,
	CLM_ARCHIVE_PLC_CD,
	CLM_STAMPLED_DT,
	CLM_TYPE_CD,
	CLM_CLOSED_DT,
	CLM_SEVERITY_CD,
	CLM_INDEMNITY_IND,
	CLM_CONTROVERT_IND,
	CLM_MEDIATION_IND,
	CLM_DOC_TYPE_CD,
	CLM_LOSS_COUNTY_CD,
	CLM_RES_LMT_IND,
	CLM_PMT_LMT_IND,
	CLM_LOSS_PLACE_ID,
	CLM_REPORT_NBR,
	CLM_VIOLATION_IND,
	CLM_ROAD_TYPE_CD,
	CLM_ROAD_SRF_D,
	CLM_SPEED_LIM_NBR,
	CLM_LIGHT_CON_CD,
	CLM_DENSELY_IND,
	CLM_TRAF_LIGHT_IND,
	CLM_PRC_STA_IND,
	CLM_CLAIM_CAT_CD,
	CLM_SUBRO_IND,
	CLM_INVEST_IND,
	CLM_OUT_DEDUCT_IND,
	CLM_SUBCONT_IND,
	CLM_ACTIVITY_STA,
	CLM_REI_NOTIFY_DT,
	CLM_EMP_STD_IND_CD,
	CLM_AT_FAULT_CD,
	CLM_DRIVER_NBR,
	CLM_DRV_SAME_IND,
	CLM_REP_TO_CAR_DT,
	CLM_VIOL_CIT_DESC,
	CLM_HOW_CLM_RPTD,
	CLM_METHOD_RPTD,
	CLM_AGENCY_CODE,
	EXTRACT_DATE,
	AS_OF_DATE,
	RECORD_COUNT,
	SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP,
	clm_wc_cat_code,
	clm_primary_loc_code,
	clm_secondary_dept_code,
	clm_xact_transaction_id,
	clm_survey,
	clm_survey_recipient,
	clm_survey_contact_method,
	clm_survey_primary_handler
	FROM SQ_CLAIM_TAB_STAGE
),
ARCH_CLAIM_TAB_STAGE AS (
	INSERT INTO ARCH_CLAIM_TAB_STAGE
	(claim_tab_id, clm_claim_nbr, clm_cause_loss_cd, clm_claim_comments, clm_csr_claim_nbr, clm_community_cd, clm_create_ts, clm_entry_opr_id, clm_pmsd_ts, clm_occurrence_id, clm_reported_dt, clm_status_cd, clm_update_opr_id, clm_upd_ts, clm_loss_occ_cd, clm_ato_chg_by, clm_ato_chg_dt, clm_co_asn_dt, clm_co_asn_rsn_cd, clm_dir_ato_lmt, clm_postal_cd, clm_fin_ind, clm_discovery_dt, clm_xpn_ato_lmt, clm_ho_report_ind, clm_litigation_ind, clm_recovery_ind, clm_loss_dt, clm_loss_tm, clm_loss_city, clm_loss_county, clm_loss_des_id, clm_loss_state_cd, clm_major_case_dt, clm_maj_case_rea, clm_med_ato_lmt, clm_not_claim_ind, not_of_occ_ind, clm_pre_rpt_ind, clm_reported_tm, clm_demand_amt, clm_archive_dt, clm_archive_plc_cd, clm_stampled_dt, clm_type_cd, clm_closed_dt, clm_severity_cd, clm_indemnity_ind, clm_controvert_ind, clm_mediation_ind, clm_doc_type_cd, clm_loss_county_cd, clm_res_lmt_ind, clm_pmt_lmt_ind, clm_loss_place_id, clm_report_nbr, clm_violation_ind, clm_road_type_cd, clm_road_srf_d, clm_speed_lim_nbr, clm_light_con_cd, clm_densely_ind, clm_traf_light_ind, clm_prc_sta_ind, clm_claim_cat_cd, clm_subro_ind, clm_invest_ind, clm_out_deduct_ind, clm_subcont_ind, clm_activity_sta, clm_rei_notify_dt, clm_emp_std_ind_cd, clm_at_fault_cd, clm_driver_nbr, clm_drv_same_ind, clm_rep_to_car_dt, clm_viol_cit_desc, clm_how_clm_rptd, clm_method_rptd, clm_agency_code, extract_date, as_of_date, record_count, source_system_id, audit_id, clm_wc_cat_code, clm_primary_loc_code, clm_secondary_dept_code, clm_xact_transaction_id, clm_survey, clm_survey_recipient, clm_survey_contact_method, clm_survey_primary_handler)
	SELECT 
	CLAIM_TAB_ID AS CLAIM_TAB_ID, 
	CLM_CLAIM_NBR AS CLM_CLAIM_NBR, 
	CLM_CAUSE_LOSS_CD AS CLM_CAUSE_LOSS_CD, 
	CLM_CLAIM_COMMENTS AS CLM_CLAIM_COMMENTS, 
	CLM_CSR_CLAIM_NBR AS CLM_CSR_CLAIM_NBR, 
	CLM_COMMUNITY_CD AS CLM_COMMUNITY_CD, 
	CLM_CREATE_TS AS CLM_CREATE_TS, 
	CLM_ENTRY_OPR_ID AS CLM_ENTRY_OPR_ID, 
	CLM_PMSD_TS AS CLM_PMSD_TS, 
	CLM_OCCURRENCE_ID AS CLM_OCCURRENCE_ID, 
	CLM_REPORTED_DT AS CLM_REPORTED_DT, 
	CLM_STATUS_CD AS CLM_STATUS_CD, 
	CLM_UPDATE_OPR_ID AS CLM_UPDATE_OPR_ID, 
	CLM_UPD_TS AS CLM_UPD_TS, 
	CLM_LOSS_OCC_CD AS CLM_LOSS_OCC_CD, 
	CLM_ATO_CHG_BY AS CLM_ATO_CHG_BY, 
	CLM_ATO_CHG_DT AS CLM_ATO_CHG_DT, 
	CLM_CO_ASN_DT AS CLM_CO_ASN_DT, 
	CLM_CO_ASN_RSN_CD AS CLM_CO_ASN_RSN_CD, 
	CLM_DIR_ATO_LMT AS CLM_DIR_ATO_LMT, 
	CLM_POSTAL_CD AS CLM_POSTAL_CD, 
	CLM_FIN_IND AS CLM_FIN_IND, 
	CLM_DISCOVERY_DT AS CLM_DISCOVERY_DT, 
	CLM_XPN_ATO_LMT AS CLM_XPN_ATO_LMT, 
	CLM_HO_REPORT_IND AS CLM_HO_REPORT_IND, 
	CLM_LITIGATION_IND AS CLM_LITIGATION_IND, 
	CLM_RECOVERY_IND AS CLM_RECOVERY_IND, 
	CLM_LOSS_DT AS CLM_LOSS_DT, 
	CLM_LOSS_TM AS CLM_LOSS_TM, 
	CLM_LOSS_CITY AS CLM_LOSS_CITY, 
	CLM_LOSS_COUNTY AS CLM_LOSS_COUNTY, 
	CLM_LOSS_DES_ID AS CLM_LOSS_DES_ID, 
	CLM_LOSS_STATE_CD AS CLM_LOSS_STATE_CD, 
	CLM_MAJOR_CASE_DT AS CLM_MAJOR_CASE_DT, 
	CLM_MAJ_CASE_REA AS CLM_MAJ_CASE_REA, 
	CLM_MED_ATO_LMT AS CLM_MED_ATO_LMT, 
	CLM_NOT_CLAIM_IND AS CLM_NOT_CLAIM_IND, 
	NOT_OF_OCC_IND AS NOT_OF_OCC_IND, 
	CLM_PRE_RPT_IND AS CLM_PRE_RPT_IND, 
	CLM_REPORTED_TM AS CLM_REPORTED_TM, 
	CLM_DEMAND_AMT AS CLM_DEMAND_AMT, 
	CLM_ARCHIVE_DT AS CLM_ARCHIVE_DT, 
	CLM_ARCHIVE_PLC_CD AS CLM_ARCHIVE_PLC_CD, 
	CLM_STAMPLED_DT AS CLM_STAMPLED_DT, 
	CLM_TYPE_CD AS CLM_TYPE_CD, 
	CLM_CLOSED_DT AS CLM_CLOSED_DT, 
	CLM_SEVERITY_CD AS CLM_SEVERITY_CD, 
	CLM_INDEMNITY_IND AS CLM_INDEMNITY_IND, 
	CLM_CONTROVERT_IND AS CLM_CONTROVERT_IND, 
	CLM_MEDIATION_IND AS CLM_MEDIATION_IND, 
	CLM_DOC_TYPE_CD AS CLM_DOC_TYPE_CD, 
	CLM_LOSS_COUNTY_CD AS CLM_LOSS_COUNTY_CD, 
	CLM_RES_LMT_IND AS CLM_RES_LMT_IND, 
	CLM_PMT_LMT_IND AS CLM_PMT_LMT_IND, 
	CLM_LOSS_PLACE_ID AS CLM_LOSS_PLACE_ID, 
	CLM_REPORT_NBR AS CLM_REPORT_NBR, 
	CLM_VIOLATION_IND AS CLM_VIOLATION_IND, 
	CLM_ROAD_TYPE_CD AS CLM_ROAD_TYPE_CD, 
	CLM_ROAD_SRF_D AS CLM_ROAD_SRF_D, 
	CLM_SPEED_LIM_NBR AS CLM_SPEED_LIM_NBR, 
	CLM_LIGHT_CON_CD AS CLM_LIGHT_CON_CD, 
	CLM_DENSELY_IND AS CLM_DENSELY_IND, 
	CLM_TRAF_LIGHT_IND AS CLM_TRAF_LIGHT_IND, 
	CLM_PRC_STA_IND AS CLM_PRC_STA_IND, 
	CLM_CLAIM_CAT_CD AS CLM_CLAIM_CAT_CD, 
	CLM_SUBRO_IND AS CLM_SUBRO_IND, 
	CLM_INVEST_IND AS CLM_INVEST_IND, 
	CLM_OUT_DEDUCT_IND AS CLM_OUT_DEDUCT_IND, 
	CLM_SUBCONT_IND AS CLM_SUBCONT_IND, 
	CLM_ACTIVITY_STA AS CLM_ACTIVITY_STA, 
	CLM_REI_NOTIFY_DT AS CLM_REI_NOTIFY_DT, 
	CLM_EMP_STD_IND_CD AS CLM_EMP_STD_IND_CD, 
	CLM_AT_FAULT_CD AS CLM_AT_FAULT_CD, 
	CLM_DRIVER_NBR AS CLM_DRIVER_NBR, 
	CLM_DRV_SAME_IND AS CLM_DRV_SAME_IND, 
	CLM_REP_TO_CAR_DT AS CLM_REP_TO_CAR_DT, 
	CLM_VIOL_CIT_DESC AS CLM_VIOL_CIT_DESC, 
	CLM_HOW_CLM_RPTD AS CLM_HOW_CLM_RPTD, 
	CLM_METHOD_RPTD AS CLM_METHOD_RPTD, 
	CLM_AGENCY_CODE AS CLM_AGENCY_CODE, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID, 
	CLM_WC_CAT_CODE, 
	CLM_PRIMARY_LOC_CODE, 
	CLM_SECONDARY_DEPT_CODE, 
	CLM_XACT_TRANSACTION_ID, 
	CLM_SURVEY, 
	CLM_SURVEY_RECIPIENT, 
	CLM_SURVEY_CONTACT_METHOD, 
	CLM_SURVEY_PRIMARY_HANDLER
	FROM EXP_CLAIM_TAB_STAGE
),
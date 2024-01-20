WITH
LKP_calender_dim AS (
	SELECT
	clndr_id,
	clndr_date
	FROM (
		SELECT 
			clndr_id,
			clndr_date
		FROM calendar_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY clndr_date ORDER BY clndr_id) = 1
),
SQ_medical_bill_service AS (
	SELECT medical_bill_service.med_bill_serv_id, medical_bill_service.med_bill_ak_id, medical_bill_service.serv_from_date, medical_bill_service.serv_charge, medical_bill_service.serv_bill_review_red, medical_bill_service.serv_network_red 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.medical_bill_service medical_bill_service
),
EXP_Source AS (
	SELECT
	med_bill_serv_id,
	med_bill_ak_id,
	serv_from_date,
	serv_charge,
	serv_bill_review_red,
	serv_network_red,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS Default_Date
	FROM SQ_medical_bill_service
),
LKP_Claim_Occurrence_Dim_ID AS (
	SELECT
	claim_occurrence_dim_id,
	edw_claim_occurrence_ak_id,
	edw_claim_party_occurrence_ak_id,
	serv_from_date,
	med_bill_ak_id
	FROM (
		SELECT  claim_occurrence_dim.claim_occurrence_dim_id AS claim_occurrence_dim_id, 
		medical_bill.claim_occurrence_ak_id AS edw_claim_occurrence_ak_id,  
		medical_bill.claim_party_occurrence_ak_id as edw_claim_party_occurrence_ak_id,
		medical_bill.med_bill_ak_id as med_bill_ak_id 
		FROM 
		@{pipeline().parameters.DB_NAME_EDW}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.medical_bill medical_bill,
		@{pipeline().parameters.DB_NAME_DATAMART}.@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence_dim claim_occurrence_dim
		WHERE
		medical_bill.claim_occurrence_ak_id = claim_occurrence_dim.edw_claim_occurrence_ak_id
		AND medical_bill.crrnt_snpsht_flag = 1
		AND claim_occurrence_dim.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY med_bill_ak_id ORDER BY claim_occurrence_dim_id DESC) = 1
),
mplt_Claim_occurrence AS (WITH
	LKP_claim_rep_dim AS (
		SELECT
		claim_rep_dim_id,
		claim_rep_wbconnect_user_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_rep_dim_id,
				claim_rep_wbconnect_user_id,
				eff_from_date,
				eff_to_date
			FROM claim_representative_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_wbconnect_user_id,eff_from_date,eff_to_date ORDER BY claim_rep_dim_id DESC) = 1
	),
	LKP_claim_representative AS (
		SELECT
		claim_rep_wbconnect_user_id,
		claim_rep_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
			CASE claim_representative.claim_rep_wbconnect_user_id  WHEN 'N/A' THEN claim_representative.claim_rep_key 
			ELSE claim_representative.claim_rep_wbconnect_user_id END AS claim_rep_wbconnect_user_id, claim_representative.claim_rep_ak_id as claim_rep_ak_id, claim_representative.eff_from_date as eff_from_date, claim_representative.eff_to_date as eff_to_date FROM claim_representative
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_ak_id,eff_from_date,eff_to_date ORDER BY claim_rep_wbconnect_user_id DESC) = 1
	),
	LKP_claim_occurence_reserve_calc AS (
		SELECT
		claim_occurrence_reserve_calculation_id,
		claim_occurrence_ak_id,
		financial_type_code,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_occurrence_reserve_calculation_id,
				claim_occurrence_ak_id,
				financial_type_code,
				eff_from_date,
				eff_to_date
			FROM claim_occurrence_reserve_calculation
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,financial_type_code,eff_from_date,eff_to_date ORDER BY claim_occurrence_reserve_calculation_id DESC) = 1
	),
	INPUT AS (
		
	),
	EXP_get_values AS (
		SELECT
		IN_claim_occurrence_ak_id,
		IN_trans_date
		FROM INPUT
	),
	LKP_claim_occurrence_calc AS (
		SELECT
		claim_occurrence_calculation_id,
		claim_occurrence_reported_date,
		claim_occurrence_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_occurrence_calculation_id,
				claim_occurrence_reported_date,
				claim_occurrence_ak_id,
				eff_from_date,
				eff_to_date
			FROM claim_occurrence_calculation
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_occurrence_calculation_id DESC) = 1
	),
	LKP_Claim_Rep_Occurrence_Examiner AS (
		SELECT
		claim_rep_ak_id,
		eff_from_date,
		eff_to_date,
		claim_occurrence_ak_id
		FROM (
			SELECT claim_representative_occurrence.claim_rep_ak_id as claim_rep_ak_id, claim_representative_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id, claim_representative_occurrence.eff_from_date as eff_from_date, claim_representative_occurrence.eff_to_date as eff_to_date FROM claim_representative_occurrence
			WHERE LTRIM(RTRIM(claim_representative_occurrence.claim_rep_role_code))  = 'E'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_rep_ak_id DESC) = 1
	),
	LKP_Claim_Rep_Occurrence_PLH AS (
		SELECT
		claim_rep_ak_id,
		eff_from_date,
		eff_to_date,
		claim_occurrence_ak_id
		FROM (
			SELECT claim_representative_occurrence.claim_rep_ak_id as claim_rep_ak_id, claim_representative_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id, claim_representative_occurrence.eff_from_date as eff_from_date, claim_representative_occurrence.eff_to_date as eff_to_date FROM claim_representative_occurrence
			WHERE LTRIM(RTRIM(claim_representative_occurrence.claim_rep_role_code))  = 'PLH'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_rep_ak_id DESC) = 1
	),
	LKP_Claim_Rep_Occurrence_Handler AS (
		SELECT
		claim_rep_ak_id,
		claim_assigned_date,
		eff_from_date,
		eff_to_date,
		claim_occurrence_ak_id
		FROM (
			SELECT claim_representative_occurrence.claim_rep_ak_id as claim_rep_ak_id, claim_representative_occurrence.claim_assigned_date as claim_assigned_date, claim_representative_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id, claim_representative_occurrence.eff_from_date as eff_from_date, claim_representative_occurrence.eff_to_date as eff_to_date FROM claim_representative_occurrence
			WHERE LTRIM(RTRIM(claim_representative_occurrence.claim_rep_role_code))  = 'H'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_rep_ak_id DESC) = 1
	),
	LKP_claim_occurrence AS (
		SELECT
		claim_occurrence_id,
		pol_key,
		claim_loss_date,
		claim_discovery_date,
		claim_cat_start_date,
		claim_cat_end_date,
		claim_created_by_key,
		claim_occurrence_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_occurrence_id,
				pol_key,
				claim_loss_date,
				claim_discovery_date,
				claim_cat_start_date,
				claim_cat_end_date,
				claim_created_by_key,
				claim_occurrence_ak_id,
				eff_from_date,
				eff_to_date
			FROM claim_occurrence
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_occurrence_id DESC) = 1
	),
	LKP_Claim_Created_by_rep_ak_id AS (
		SELECT
		claim_rep_ak_id,
		claim_rep_wbconnect_user_id,
		claim_rep_key,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_rep_ak_id,
				claim_rep_wbconnect_user_id,
				claim_rep_key,
				eff_from_date,
				eff_to_date
			FROM claim_representative
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_key,eff_from_date,eff_to_date ORDER BY claim_rep_ak_id) = 1
	),
	LKP_V2_policy AS (
		SELECT
		pol_id,
		contract_cust_ak_id,
		agency_ak_id,
		pol_sym,
		pol_num,
		pol_eff_date,
		pol_exp_date,
		strtgc_bus_dvsn_ak_id,
		AgencyAKID,
		StrategicProfitCenterCode,
		InsuranceSegmentCode,
		PolicyOfferingCode,
		pol_key,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
			policy.pol_id as pol_id, 
			policy.contract_cust_ak_id as contract_cust_ak_id, 
			policy.agency_ak_id as agency_ak_id, 
			policy.pol_sym as pol_sym, 
			policy.pol_num as pol_num, 
			policy.pol_eff_date as pol_eff_date, 
			policy.pol_exp_date as pol_exp_date, 
			policy.strtgc_bus_dvsn_ak_id as strtgc_bus_dvsn_ak_id,
			policy.AgencyAKID as AgencyAKID,
			SPC.StrategicProfitCenterCode as StrategicProfitCenterCode, INSG.InsuranceSegmentCode as InsuranceSegmentCode, 
			PO.PolicyOfferingCode as PolicyOfferingCode, 
			policy.pol_key as pol_key, 
			policy.eff_from_date as eff_from_date, 
			policy.eff_to_date as eff_to_date 
			FROM 
			v2.policy policy,
			StrategicProfitCenter SPC,
			InsuranceSegment INSG,
			PolicyOffering PO
			WHERE 
			policy.StrategicProfitCenterAKId = SPC.StrategicProfitCenterAKId and SPC.CurrentSnapshotFlag =  1
			and policy.InsuranceSegmentAKId = INSG.InsuranceSegmentAKId and INSG.CurrentSnapshotFlag = 1
			and policy.PolicyOfferingAKId = PO.PolicyOfferingAKId and PO.CurrentSnapshotFlag = 1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key,eff_from_date,eff_to_date ORDER BY pol_id DESC) = 1
	),
	EXP_get_reserve_calc_ids AS (
		SELECT
		LKP_claim_occurrence.claim_occurrence_id,
		-- *INF*: IIF(ISNULL(claim_occurrence_id), -1, claim_occurrence_id)
		IFF(claim_occurrence_id IS NULL, - 1, claim_occurrence_id) AS claim_occurrence_id_out,
		-- *INF*: IIF(ISNULL(claim_rep_occurrence_id), -1, claim_rep_occurrence_id)
		IFF(claim_rep_occurrence_id IS NULL, - 1, claim_rep_occurrence_id) AS claim_rep_occurrence_id_out,
		LKP_claim_occurrence_calc.claim_occurrence_calculation_id,
		-- *INF*: IIF(ISNULL(claim_occurrence_calculation_id), -1, claim_occurrence_calculation_id)
		IFF(claim_occurrence_calculation_id IS NULL, - 1, claim_occurrence_calculation_id) AS claim_occurrence_calculation_id_out,
		EXP_get_values.IN_claim_occurrence_ak_id AS claim_occurrence_ak_id,
		-- *INF*: IIF(ISNULL(claim_occurrence_ak_id), -1, claim_occurrence_ak_id)
		IFF(claim_occurrence_ak_id IS NULL, - 1, claim_occurrence_ak_id) AS claim_occurrence_ak_id_out,
		EXP_get_values.IN_trans_date,
		-- *INF*: :LKP.LKP_CLAIM_OCCURENCE_RESERVE_CALC(claim_occurrence_ak_id, 'D', IN_trans_date)
		LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_D_IN_trans_date.claim_occurrence_reserve_calculation_id AS claim_occurrence_reserve_calc_direct_loss_id,
		-- *INF*: iif(isnull(claim_occurrence_reserve_calc_direct_loss_id), -1, claim_occurrence_reserve_calc_direct_loss_id)
		IFF(
		    claim_occurrence_reserve_calc_direct_loss_id IS NULL, - 1,
		    claim_occurrence_reserve_calc_direct_loss_id
		) AS out_claim_occurrence_reserve_calc_direct_loss_id,
		-- *INF*: :LKP.LKP_CLAIM_OCCURENCE_RESERVE_CALC(claim_occurrence_ak_id, 'E', IN_trans_date)
		LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_E_IN_trans_date.claim_occurrence_reserve_calculation_id AS claim_occurrence_reserve_calc_exp_id,
		-- *INF*: IIF(ISNULL(claim_occurrence_reserve_calc_exp_id), -1, claim_occurrence_reserve_calc_exp_id)
		IFF(claim_occurrence_reserve_calc_exp_id IS NULL, - 1, claim_occurrence_reserve_calc_exp_id) AS out_claim_occurrence_reserve_calc_exp_id,
		-- *INF*: :LKP.LKP_CLAIM_OCCURENCE_RESERVE_CALC(claim_occurrence_ak_id, 'B', IN_trans_date)
		LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_B_IN_trans_date.claim_occurrence_reserve_calculation_id AS claim_occurrence_reserve_calc_subrogation_id,
		-- *INF*: IIF(ISNULL(claim_occurrence_reserve_calc_subrogation_id), -1, claim_occurrence_reserve_calc_subrogation_id)
		IFF(
		    claim_occurrence_reserve_calc_subrogation_id IS NULL, - 1,
		    claim_occurrence_reserve_calc_subrogation_id
		) AS out_claim_occurrence_reserve_calc_subrogation_id,
		-- *INF*: :LKP.LKP_CLAIM_OCCURENCE_RESERVE_CALC(claim_occurrence_ak_id, 'S', IN_trans_date)
		LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_S_IN_trans_date.claim_occurrence_reserve_calculation_id AS claim_occurrence_reserve_calc_salvage_id,
		-- *INF*: IIF(ISNULL(claim_occurrence_reserve_calc_salvage_id), -1, claim_occurrence_reserve_calc_salvage_id)
		IFF(
		    claim_occurrence_reserve_calc_salvage_id IS NULL, - 1,
		    claim_occurrence_reserve_calc_salvage_id
		) AS out_claim_occurrence_reserve_calc_salvage_id,
		-- *INF*: :LKP.LKP_CLAIM_OCCURENCE_RESERVE_CALC(claim_occurrence_ak_id, 'R', IN_trans_date)
		LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_R_IN_trans_date.claim_occurrence_reserve_calculation_id AS claim_occurrence_reserve_calc_recovery_id,
		-- *INF*: IIF(ISNULL(claim_occurrence_reserve_calc_recovery_id), -1, claim_occurrence_reserve_calc_recovery_id)
		IFF(
		    claim_occurrence_reserve_calc_recovery_id IS NULL, - 1,
		    claim_occurrence_reserve_calc_recovery_id
		) AS out_claim_occurrence_reserve_calc_recovery_id,
		LKP_claim_occurrence_calc.claim_occurrence_reported_date AS claim_occurrence_rpted_date
		FROM EXP_get_values
		LEFT JOIN LKP_claim_occurrence
		ON LKP_claim_occurrence.claim_occurrence_ak_id = EXP_get_values.IN_claim_occurrence_ak_id AND LKP_claim_occurrence.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_claim_occurrence.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_claim_occurrence_calc
		ON LKP_claim_occurrence_calc.claim_occurrence_ak_id = EXP_get_values.IN_claim_occurrence_ak_id AND LKP_claim_occurrence_calc.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_claim_occurrence_calc.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_CLAIM_OCCURENCE_RESERVE_CALC LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_D_IN_trans_date
		ON LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_D_IN_trans_date.claim_occurrence_ak_id = claim_occurrence_ak_id
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_D_IN_trans_date.financial_type_code = 'D'
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_D_IN_trans_date.eff_from_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_OCCURENCE_RESERVE_CALC LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_E_IN_trans_date
		ON LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_E_IN_trans_date.claim_occurrence_ak_id = claim_occurrence_ak_id
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_E_IN_trans_date.financial_type_code = 'E'
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_E_IN_trans_date.eff_from_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_OCCURENCE_RESERVE_CALC LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_B_IN_trans_date
		ON LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_B_IN_trans_date.claim_occurrence_ak_id = claim_occurrence_ak_id
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_B_IN_trans_date.financial_type_code = 'B'
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_B_IN_trans_date.eff_from_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_OCCURENCE_RESERVE_CALC LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_S_IN_trans_date
		ON LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_S_IN_trans_date.claim_occurrence_ak_id = claim_occurrence_ak_id
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_S_IN_trans_date.financial_type_code = 'S'
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_S_IN_trans_date.eff_from_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_OCCURENCE_RESERVE_CALC LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_R_IN_trans_date
		ON LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_R_IN_trans_date.claim_occurrence_ak_id = claim_occurrence_ak_id
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_R_IN_trans_date.financial_type_code = 'R'
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_R_IN_trans_date.eff_from_date = IN_trans_date
	
	),
	LKP_claim_occurrence_dim AS (
		SELECT
		claim_occurrence_dim_id,
		source_claim_rpted_date,
		claim_rpted_date,
		claim_scripted_date,
		claim_open_date,
		claim_close_date,
		claim_reopen_date,
		claim_closed_after_reopen_date,
		claim_notice_only_date,
		edw_claim_occurrence_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_occurrence_dim_id,
				source_claim_rpted_date,
				claim_rpted_date,
				claim_scripted_date,
				claim_open_date,
				claim_close_date,
				claim_reopen_date,
				claim_closed_after_reopen_date,
				claim_notice_only_date,
				edw_claim_occurrence_ak_id,
				eff_from_date,
				eff_to_date
			FROM claim_occurrence_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_occurrence_dim_id DESC) = 1
	),
	LKP_Agency_Key AS (
		SELECT
		agency_key,
		agency_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				agency_key,
				agency_ak_id,
				eff_from_date,
				eff_to_date
			FROM agency
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY agency_ak_id,eff_from_date,eff_to_date ORDER BY agency_key DESC) = 1
	),
	LKP_contract_customer AS (
		SELECT
		contract_cust_id,
		contract_cust_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				contract_cust_id,
				contract_cust_ak_id,
				eff_from_date,
				eff_to_date
			FROM contract_customer
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY contract_cust_ak_id,eff_from_date,eff_to_date ORDER BY contract_cust_id DESC) = 1
	),
	LKP_Policy_Dim AS (
		SELECT
		pol_dim_id,
		edw_pol_pk_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				pol_dim_id,
				edw_pol_pk_id,
				eff_from_date,
				eff_to_date
			FROM policy_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_pol_pk_id,eff_from_date,eff_to_date ORDER BY pol_dim_id DESC) = 1
	),
	LKP_AgencyDim AS (
		SELECT
		AgencyDimID,
		EDWAgencyAKID,
		EffectiveDate,
		ExpirationDate
		FROM (
			SELECT 
				AgencyDimID,
				EDWAgencyAKID,
				EffectiveDate,
				ExpirationDate
			FROM V3.AgencyDim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWAgencyAKID,EffectiveDate,ExpirationDate ORDER BY AgencyDimID DESC) = 1
	),
	EXP_Claim_Rep_Lkp_Values AS (
		SELECT
		EXP_get_reserve_calc_ids.IN_trans_date,
		LKP_Claim_Rep_Occurrence_Handler.claim_rep_ak_id AS claim_rep_primary_rep_ak_id,
		-- *INF*: :LKP.LKP_CLAIM_REPRESENTATIVE(claim_rep_primary_rep_ak_id, IN_trans_date)
		LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_rep_ak_id_IN_trans_date.claim_rep_wbconnect_user_id AS v_claim_rep_primary_rep_wbconnect_user_id,
		LKP_Claim_Rep_Occurrence_Examiner.claim_rep_ak_id AS claim_rep_examiner_ak_id,
		-- *INF*: :LKP.LKP_CLAIM_REPRESENTATIVE(claim_rep_examiner_ak_id, IN_trans_date)
		LKP_CLAIM_REPRESENTATIVE_claim_rep_examiner_ak_id_IN_trans_date.claim_rep_wbconnect_user_id AS v_claim_rep_examiner_wbconnect_user_id,
		LKP_Claim_Rep_Occurrence_PLH.claim_rep_ak_id AS claim_rep_primary_lit_handler_ak_id,
		-- *INF*: :LKP.LKP_CLAIM_REPRESENTATIVE(claim_rep_primary_lit_handler_ak_id, IN_trans_date)
		LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_lit_handler_ak_id_IN_trans_date.claim_rep_wbconnect_user_id AS v_claim_rep_primary_lit_handler_wbconnect_user_id,
		-- *INF*: :LKP.LKP_CLAIM_REP_DIM(claim_rep_primary_rep_ak_id, v_claim_rep_primary_rep_wbconnect_user_id, IN_trans_date)
		LKP_CLAIM_REP_DIM_claim_rep_primary_rep_ak_id_v_claim_rep_primary_rep_wbconnect_user_id_IN_trans_date.claim_rep_dim_id AS claim_rep_dim_prim_claim_rep_id,
		-- *INF*: :LKP.LKP_CLAIM_REP_DIM(claim_rep_examiner_ak_id, v_claim_rep_examiner_wbconnect_user_id, IN_trans_date)
		LKP_CLAIM_REP_DIM_claim_rep_examiner_ak_id_v_claim_rep_examiner_wbconnect_user_id_IN_trans_date.claim_rep_dim_id AS claim_rep_dim_examiner_id,
		-- *INF*: :LKP.LKP_CLAIM_REP_DIM(claim_rep_primary_lit_handler_ak_id, v_claim_rep_primary_lit_handler_wbconnect_user_id, IN_trans_date)
		LKP_CLAIM_REP_DIM_claim_rep_primary_lit_handler_ak_id_v_claim_rep_primary_lit_handler_wbconnect_user_id_IN_trans_date.claim_rep_dim_id AS claim_rep_dim_prim_litigation_handler_id,
		LKP_Claim_Created_by_rep_ak_id.claim_rep_ak_id,
		LKP_Claim_Created_by_rep_ak_id.claim_rep_wbconnect_user_id,
		-- *INF*: :LKP.LKP_CLAIM_REP_DIM(claim_rep_ak_id,claim_rep_wbconnect_user_id,IN_trans_date)
		LKP_CLAIM_REP_DIM_claim_rep_ak_id_claim_rep_wbconnect_user_id_IN_trans_date.claim_rep_dim_id AS claim_created_by_id
		FROM EXP_get_reserve_calc_ids
		LEFT JOIN LKP_Claim_Created_by_rep_ak_id
		ON LKP_Claim_Created_by_rep_ak_id.claim_rep_key = LKP_claim_occurrence.claim_created_by_key AND LKP_Claim_Created_by_rep_ak_id.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Created_by_rep_ak_id.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Rep_Occurrence_Examiner
		ON LKP_Claim_Rep_Occurrence_Examiner.claim_occurrence_ak_id = EXP_get_values.IN_claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_Examiner.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Rep_Occurrence_Examiner.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Rep_Occurrence_Handler
		ON LKP_Claim_Rep_Occurrence_Handler.claim_occurrence_ak_id = EXP_get_values.IN_claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_Handler.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Rep_Occurrence_Handler.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Rep_Occurrence_PLH
		ON LKP_Claim_Rep_Occurrence_PLH.claim_occurrence_ak_id = EXP_get_values.IN_claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_PLH.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Rep_Occurrence_PLH.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_CLAIM_REPRESENTATIVE LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_rep_ak_id_IN_trans_date
		ON LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_rep_ak_id_IN_trans_date.claim_rep_ak_id = claim_rep_primary_rep_ak_id
		AND LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_rep_ak_id_IN_trans_date.eff_from_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_REPRESENTATIVE LKP_CLAIM_REPRESENTATIVE_claim_rep_examiner_ak_id_IN_trans_date
		ON LKP_CLAIM_REPRESENTATIVE_claim_rep_examiner_ak_id_IN_trans_date.claim_rep_ak_id = claim_rep_examiner_ak_id
		AND LKP_CLAIM_REPRESENTATIVE_claim_rep_examiner_ak_id_IN_trans_date.eff_from_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_REPRESENTATIVE LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_lit_handler_ak_id_IN_trans_date
		ON LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_lit_handler_ak_id_IN_trans_date.claim_rep_ak_id = claim_rep_primary_lit_handler_ak_id
		AND LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_lit_handler_ak_id_IN_trans_date.eff_from_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_REP_DIM LKP_CLAIM_REP_DIM_claim_rep_primary_rep_ak_id_v_claim_rep_primary_rep_wbconnect_user_id_IN_trans_date
		ON LKP_CLAIM_REP_DIM_claim_rep_primary_rep_ak_id_v_claim_rep_primary_rep_wbconnect_user_id_IN_trans_date.claim_rep_wbconnect_user_id = claim_rep_primary_rep_ak_id
		AND LKP_CLAIM_REP_DIM_claim_rep_primary_rep_ak_id_v_claim_rep_primary_rep_wbconnect_user_id_IN_trans_date.eff_from_date = v_claim_rep_primary_rep_wbconnect_user_id
		AND LKP_CLAIM_REP_DIM_claim_rep_primary_rep_ak_id_v_claim_rep_primary_rep_wbconnect_user_id_IN_trans_date.eff_to_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_REP_DIM LKP_CLAIM_REP_DIM_claim_rep_examiner_ak_id_v_claim_rep_examiner_wbconnect_user_id_IN_trans_date
		ON LKP_CLAIM_REP_DIM_claim_rep_examiner_ak_id_v_claim_rep_examiner_wbconnect_user_id_IN_trans_date.claim_rep_wbconnect_user_id = claim_rep_examiner_ak_id
		AND LKP_CLAIM_REP_DIM_claim_rep_examiner_ak_id_v_claim_rep_examiner_wbconnect_user_id_IN_trans_date.eff_from_date = v_claim_rep_examiner_wbconnect_user_id
		AND LKP_CLAIM_REP_DIM_claim_rep_examiner_ak_id_v_claim_rep_examiner_wbconnect_user_id_IN_trans_date.eff_to_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_REP_DIM LKP_CLAIM_REP_DIM_claim_rep_primary_lit_handler_ak_id_v_claim_rep_primary_lit_handler_wbconnect_user_id_IN_trans_date
		ON LKP_CLAIM_REP_DIM_claim_rep_primary_lit_handler_ak_id_v_claim_rep_primary_lit_handler_wbconnect_user_id_IN_trans_date.claim_rep_wbconnect_user_id = claim_rep_primary_lit_handler_ak_id
		AND LKP_CLAIM_REP_DIM_claim_rep_primary_lit_handler_ak_id_v_claim_rep_primary_lit_handler_wbconnect_user_id_IN_trans_date.eff_from_date = v_claim_rep_primary_lit_handler_wbconnect_user_id
		AND LKP_CLAIM_REP_DIM_claim_rep_primary_lit_handler_ak_id_v_claim_rep_primary_lit_handler_wbconnect_user_id_IN_trans_date.eff_to_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_REP_DIM LKP_CLAIM_REP_DIM_claim_rep_ak_id_claim_rep_wbconnect_user_id_IN_trans_date
		ON LKP_CLAIM_REP_DIM_claim_rep_ak_id_claim_rep_wbconnect_user_id_IN_trans_date.claim_rep_wbconnect_user_id = claim_rep_ak_id
		AND LKP_CLAIM_REP_DIM_claim_rep_ak_id_claim_rep_wbconnect_user_id_IN_trans_date.eff_from_date = claim_rep_wbconnect_user_id
		AND LKP_CLAIM_REP_DIM_claim_rep_ak_id_claim_rep_wbconnect_user_id_IN_trans_date.eff_to_date = IN_trans_date
	
	),
	LKP_agency_Dim AS (
		SELECT
		agency_dim_id,
		agency_key,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				agency_dim_id,
				agency_key,
				eff_from_date,
				eff_to_date
			FROM V2.agency_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY agency_key,eff_from_date,eff_to_date ORDER BY agency_dim_id DESC) = 1
	),
	LKP_contract_customer_dim AS (
		SELECT
		contract_cust_dim_id,
		edw_contract_cust_pk_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				contract_cust_dim_id,
				edw_contract_cust_pk_id,
				eff_from_date,
				eff_to_date
			FROM contract_customer_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_contract_cust_pk_id,eff_from_date,eff_to_date ORDER BY contract_cust_dim_id DESC) = 1
	),
	OUTPUT AS (
		SELECT
		LKP_claim_occurrence_dim.claim_occurrence_dim_id, 
		LKP_claim_occurrence.claim_loss_date, 
		LKP_claim_occurrence.claim_discovery_date, 
		LKP_claim_occurrence_dim.claim_scripted_date, 
		LKP_claim_occurrence_dim.source_claim_rpted_date, 
		LKP_claim_occurrence_dim.claim_rpted_date AS claim_occurrence_rpted_date, 
		LKP_claim_occurrence_dim.claim_open_date, 
		LKP_claim_occurrence_dim.claim_close_date, 
		LKP_claim_occurrence_dim.claim_reopen_date, 
		LKP_claim_occurrence_dim.claim_closed_after_reopen_date, 
		LKP_claim_occurrence_dim.claim_notice_only_date, 
		LKP_claim_occurrence.claim_cat_start_date, 
		LKP_claim_occurrence.claim_cat_end_date, 
		LKP_Claim_Rep_Occurrence_Handler.claim_assigned_date AS claim_rep_assigned_date, 
		LKP_Claim_Rep_Occurrence_Handler.eff_to_date AS claim_rep_unassigned_date, 
		EXP_Claim_Rep_Lkp_Values.claim_rep_dim_prim_claim_rep_id, 
		EXP_Claim_Rep_Lkp_Values.claim_rep_dim_examiner_id, 
		EXP_Claim_Rep_Lkp_Values.claim_rep_dim_prim_litigation_handler_id, 
		LKP_Policy_Dim.pol_dim_id AS pol_key_dim_id, 
		LKP_V2_policy.pol_eff_date, 
		LKP_V2_policy.pol_exp_date, 
		LKP_agency_Dim.agency_dim_id, 
		EXP_Claim_Rep_Lkp_Values.claim_created_by_id, 
		LKP_contract_customer_dim.contract_cust_dim_id, 
		LKP_V2_policy.pol_sym, 
		LKP_V2_policy.pol_num, 
		LKP_V2_policy.strtgc_bus_dvsn_ak_id, 
		LKP_AgencyDim.AgencyDimID, 
		LKP_V2_policy.StrategicProfitCenterCode, 
		LKP_V2_policy.InsuranceSegmentCode, 
		LKP_V2_policy.PolicyOfferingCode
		FROM EXP_Claim_Rep_Lkp_Values
		LEFT JOIN LKP_AgencyDim
		ON LKP_AgencyDim.EDWAgencyAKID = LKP_V2_policy.AgencyAKID AND LKP_AgencyDim.EffectiveDate <= EXP_get_values.IN_trans_date AND LKP_AgencyDim.ExpirationDate >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Rep_Occurrence_Handler
		ON LKP_Claim_Rep_Occurrence_Handler.claim_occurrence_ak_id = EXP_get_values.IN_claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_Handler.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Rep_Occurrence_Handler.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Policy_Dim
		ON LKP_Policy_Dim.edw_pol_pk_id = LKP_V2_policy.pol_id AND LKP_Policy_Dim.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Policy_Dim.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_V2_policy
		ON LKP_V2_policy.pol_key = LKP_claim_occurrence.pol_key AND LKP_V2_policy.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_V2_policy.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_agency_Dim
		ON LKP_agency_Dim.agency_key = LKP_Agency_Key.agency_key AND LKP_agency_Dim.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_agency_Dim.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_claim_occurrence
		ON LKP_claim_occurrence.claim_occurrence_ak_id = EXP_get_values.IN_claim_occurrence_ak_id AND LKP_claim_occurrence.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_claim_occurrence.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_claim_occurrence_dim
		ON LKP_claim_occurrence_dim.edw_claim_occurrence_ak_id = EXP_get_reserve_calc_ids.claim_occurrence_ak_id_out AND LKP_claim_occurrence_dim.eff_from_date <= EXP_get_reserve_calc_ids.IN_trans_date AND LKP_claim_occurrence_dim.eff_to_date >= EXP_get_reserve_calc_ids.IN_trans_date
		LEFT JOIN LKP_contract_customer_dim
		ON LKP_contract_customer_dim.edw_contract_cust_pk_id = LKP_contract_customer.contract_cust_id AND LKP_contract_customer_dim.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_contract_customer_dim.eff_to_date >= EXP_get_values.IN_trans_date
	),
),
EXP_Capture_Date_Ids AS (
	SELECT
	claim_loss_date AS IN_claim_loss_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(IN_claim_loss_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_IN_claim_loss_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_loss_date,
	-- *INF*: IIF(NOT ISNULL(v_claim_loss_date), v_claim_loss_date, -1)
	IFF(v_claim_loss_date IS NOT NULL, v_claim_loss_date, - 1) AS claim_loss_date_id,
	claim_discovery_date AS IN_claim_discovery_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(IN_claim_discovery_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_IN_claim_discovery_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS V_claim_discovery_date,
	-- *INF*: IIF(NOT ISNULL(V_claim_discovery_date), V_claim_discovery_date, -1)
	IFF(V_claim_discovery_date IS NOT NULL, V_claim_discovery_date, - 1) AS claim_discovery_date_id,
	claim_scripted_date AS IN_claim_scripted_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(IN_claim_scripted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_IN_claim_scripted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_scripted_date,
	-- *INF*: IIF(ISNULL(v_claim_scripted_date),-1,v_claim_scripted_date)
	IFF(v_claim_scripted_date IS NULL, - 1, v_claim_scripted_date) AS claim_scripted_date_id,
	source_claim_rpted_date AS IN_source_claim_scripted_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(IN_source_claim_scripted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_IN_source_claim_scripted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_source_claim_scripted_date,
	-- *INF*: IIF(ISNULL(v_source_claim_scripted_date),-1,v_source_claim_scripted_date)
	IFF(v_source_claim_scripted_date IS NULL, - 1, v_source_claim_scripted_date) AS source_claim_scripted_date_id,
	claim_occurrence_rpted_date AS IN_claim_rpted_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(IN_claim_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_IN_claim_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_rpted_date,
	-- *INF*: IIF(NOT ISNULL(v_claim_rpted_date), v_claim_rpted_date, -1)
	IFF(v_claim_rpted_date IS NOT NULL, v_claim_rpted_date, - 1) AS claim_rpted_date_id,
	claim_open_date AS IN_claim_open_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(IN_claim_open_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_IN_claim_open_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_open_date,
	-- *INF*: IIF(NOT ISNULL(v_claim_open_date), v_claim_open_date, -1)
	IFF(v_claim_open_date IS NOT NULL, v_claim_open_date, - 1) AS claim_open_date_id,
	claim_close_date AS IN_claim_close_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(IN_claim_close_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_IN_claim_close_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_close_date,
	-- *INF*: IIF(NOT ISNULL(v_claim_close_date), v_claim_close_date, -1)
	IFF(v_claim_close_date IS NOT NULL, v_claim_close_date, - 1) AS claim_close_date_id,
	claim_reopen_date AS IN_claim_reopen_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(IN_claim_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_IN_claim_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_reopen_date,
	-- *INF*: IIF(NOT ISNULL(v_claim_reopen_date), v_claim_reopen_date, -1)
	IFF(v_claim_reopen_date IS NOT NULL, v_claim_reopen_date, - 1) AS claim_reopen_date_id,
	claim_closed_after_reopen_date AS IN_claim_closed_after_reopen_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(IN_claim_closed_after_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_IN_claim_closed_after_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_closed_after_reopen_date,
	-- *INF*: IIF(NOT ISNULL(v_claim_closed_after_reopen_date), v_claim_closed_after_reopen_date, -1)
	IFF(v_claim_closed_after_reopen_date IS NOT NULL, v_claim_closed_after_reopen_date, - 1) AS claim_closed_after_reopen_date_id,
	claim_notice_only_date AS IN_claim_notice_only_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(IN_claim_notice_only_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_IN_claim_notice_only_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_notice_only_date,
	-- *INF*: IIF(NOT ISNULL(v_claim_notice_only_date), v_claim_notice_only_date, -1)
	IFF(v_claim_notice_only_date IS NOT NULL, v_claim_notice_only_date, - 1) AS claim_notice_only_date_id,
	claim_cat_start_date AS IN_claim_cat_start_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(IN_claim_cat_start_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_IN_claim_cat_start_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_cat_start_date,
	-- *INF*: IIF(NOT ISNULL(v_claim_cat_start_date), v_claim_cat_start_date, -1)
	IFF(v_claim_cat_start_date IS NOT NULL, v_claim_cat_start_date, - 1) AS claim_cat_start_date_id,
	claim_cat_end_date AS IN_claim_cat_end_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(IN_claim_cat_end_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_IN_claim_cat_end_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_cat_end_date,
	-- *INF*: IIF(NOT ISNULL(v_claim_cat_end_date), v_claim_cat_end_date, -1)
	IFF(v_claim_cat_end_date IS NOT NULL, v_claim_cat_end_date, - 1) AS claim_cat_end_date_id,
	claim_rep_assigned_date AS IN_claim_rep_assigned_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(IN_claim_rep_assigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_IN_claim_rep_assigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_rep_assigned_date,
	-- *INF*: IIF(NOT ISNULL(v_claim_rep_assigned_date), v_claim_rep_assigned_date, -1)
	IFF(v_claim_rep_assigned_date IS NOT NULL, v_claim_rep_assigned_date, - 1) AS claim_rep_assigned_date_id,
	claim_rep_unassigned_date AS IN_claim_rep_unassigned_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(IN_claim_rep_unassigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_IN_claim_rep_unassigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_rep_unassigned_date,
	-- *INF*: IIF(NOT ISNULL(v_claim_rep_unassigned_date), v_claim_rep_unassigned_date, -1)
	IFF(v_claim_rep_unassigned_date IS NOT NULL, v_claim_rep_unassigned_date, - 1) AS claim_rep_unassigned_date_id,
	pol_eff_date AS IN_pol_eff_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(IN_pol_eff_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_IN_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pol_eff_date,
	-- *INF*: IIF(NOT ISNULL(v_pol_eff_date), v_pol_eff_date, -1)
	IFF(v_pol_eff_date IS NOT NULL, v_pol_eff_date, - 1) AS pol_eff_date_id,
	pol_exp_date AS IN_pol_exp_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(IN_pol_exp_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_IN_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pol_exp_date,
	-- *INF*: IIF(NOT ISNULL(v_pol_exp_date), v_pol_exp_date, -1)
	IFF(v_pol_exp_date IS NOT NULL, v_pol_exp_date, - 1) AS pol_exp_date_id,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(IN_pay_issued_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_IN_pay_issued_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pay_issued_date,
	-- *INF*: IIF(NOT ISNULL(v_pay_issued_date), v_pay_issued_date, -1)
	IFF(v_pay_issued_date IS NOT NULL, v_pay_issued_date, - 1) AS pay_issued_date_id,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(IN_pay_cashed_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_IN_pay_cashed_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pay_cashed_date,
	-- *INF*: IIF(NOT ISNULL(v_pay_cashed_date), v_pay_cashed_date, -1)
	IFF(v_pay_cashed_date IS NOT NULL, v_pay_cashed_date, - 1) AS pay_cashed_date_id,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(IN_pay_voided_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_IN_pay_voided_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pay_voided_date,
	-- *INF*: IIF(NOT ISNULL(v_pay_voided_date), v_pay_voided_date, -1)
	IFF(v_pay_voided_date IS NOT NULL, v_pay_voided_date, - 1) AS pay_voided_date_id,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(IN_pay_reposted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_IN_pay_reposted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pay_reposted_date,
	-- *INF*: IIF(NOT ISNULL(v_pay_reposted_date), v_pay_reposted_date, -1)
	IFF(v_pay_reposted_date IS NOT NULL, v_pay_reposted_date, - 1) AS pay_reposted_date_id
	FROM mplt_Claim_occurrence
	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_IN_claim_loss_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_IN_claim_loss_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(IN_claim_loss_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_IN_claim_discovery_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_IN_claim_discovery_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(IN_claim_discovery_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_IN_claim_scripted_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_IN_claim_scripted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(IN_claim_scripted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_IN_source_claim_scripted_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_IN_source_claim_scripted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(IN_source_claim_scripted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_IN_claim_rpted_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_IN_claim_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(IN_claim_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_IN_claim_open_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_IN_claim_open_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(IN_claim_open_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_IN_claim_close_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_IN_claim_close_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(IN_claim_close_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_IN_claim_reopen_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_IN_claim_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(IN_claim_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_IN_claim_closed_after_reopen_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_IN_claim_closed_after_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(IN_claim_closed_after_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_IN_claim_notice_only_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_IN_claim_notice_only_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(IN_claim_notice_only_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_IN_claim_cat_start_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_IN_claim_cat_start_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(IN_claim_cat_start_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_IN_claim_cat_end_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_IN_claim_cat_end_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(IN_claim_cat_end_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_IN_claim_rep_assigned_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_IN_claim_rep_assigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(IN_claim_rep_assigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_IN_claim_rep_unassigned_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_IN_claim_rep_unassigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(IN_claim_rep_unassigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_IN_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_IN_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(IN_pol_eff_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_IN_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_IN_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(IN_pol_exp_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_IN_pay_issued_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_IN_pay_issued_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(IN_pay_issued_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_IN_pay_cashed_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_IN_pay_cashed_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(IN_pay_cashed_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_IN_pay_voided_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_IN_pay_voided_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(IN_pay_voided_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_IN_pay_reposted_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_IN_pay_reposted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(IN_pay_reposted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

),
LKP_Claimant_Dim AS (
	SELECT
	claimant_dim_id,
	edw_claim_party_occurrence_ak_id
	FROM (
		SELECT 
			claimant_dim_id,
			edw_claim_party_occurrence_ak_id
		FROM claimant_dim
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_party_occurrence_ak_id ORDER BY claimant_dim_id DESC) = 1
),
LKP_Med_Bill_Dim AS (
	SELECT
	med_bill_dim_id,
	edw_med_bill_ak_id
	FROM (
		SELECT 
			med_bill_dim_id,
			edw_med_bill_ak_id
		FROM medical_bill_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_med_bill_ak_id ORDER BY med_bill_dim_id DESC) = 1
),
LKP_Med_Bill_Serv_Dim_ID AS (
	SELECT
	med_bill_serv_dim_id,
	edw_med_bill_serv_pk_id
	FROM (
		SELECT 
			med_bill_serv_dim_id,
			edw_med_bill_serv_pk_id
		FROM medical_bill_service_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_med_bill_serv_pk_id ORDER BY med_bill_serv_dim_id DESC) = 1
),
mplt_Strategic_Business_Division_Dim AS (WITH
	INPUT_Strategic_Business_Division AS (
		
	),
	EXP_inputs AS (
		SELECT
		policy_symbol,
		policy_number,
		policy_eff_date AS policy_eff_date_in,
		-- *INF*: IIF(:UDF.DEFAULT_VALUE_FOR_STRINGS(policy_symbol)='N/A','N/A',substr(policy_symbol,1,1))
		IFF(
		    UDF_DEFAULT_VALUE_FOR_STRINGS(policy_symbol) = 'N/A', 'N/A', substr(policy_symbol, 1, 1)
		) AS policy_symbol_position_1,
		-- *INF*: IIF(:UDF.DEFAULT_VALUE_FOR_STRINGS(policy_number)='N/A','N/A',substr(policy_number,1,1))
		IFF(
		    UDF_DEFAULT_VALUE_FOR_STRINGS(policy_number) = 'N/A', 'N/A', substr(policy_number, 1, 1)
		) AS policy_number_position_1,
		-- *INF*: IIF(isnull(policy_eff_date_in),SYSDATE,policy_eff_date_in)
		IFF(policy_eff_date_in IS NULL, CURRENT_TIMESTAMP, policy_eff_date_in) AS policy_eff_date
		FROM INPUT_Strategic_Business_Division
	),
	LKP_strategic_business_division_dim AS (
		SELECT
		strtgc_bus_dvsn_dim_id,
		crrnt_snpsht_flag,
		audit_id,
		eff_from_date,
		eff_to_date,
		created_date,
		modified_date,
		edw_strtgc_bus_dvsn_ak_id,
		pol_sym_1,
		pol_num_1,
		pol_eff_date,
		pol_exp_date,
		strtgc_bus_dvsn_code,
		strtgc_bus_dvsn_code_descript,
		policy_symbol_position_IN,
		policy_number_position_IN,
		policy_eff_date_IN
		FROM (
			SELECT 
				strtgc_bus_dvsn_dim_id,
				crrnt_snpsht_flag,
				audit_id,
				eff_from_date,
				eff_to_date,
				created_date,
				modified_date,
				edw_strtgc_bus_dvsn_ak_id,
				pol_sym_1,
				pol_num_1,
				pol_eff_date,
				pol_exp_date,
				strtgc_bus_dvsn_code,
				strtgc_bus_dvsn_code_descript,
				policy_symbol_position_IN,
				policy_number_position_IN,
				policy_eff_date_IN
			FROM strategic_business_division_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_sym_1,pol_num_1,pol_eff_date,pol_exp_date ORDER BY strtgc_bus_dvsn_dim_id) = 1
	),
	EXP_check_outputs AS (
		SELECT
		strtgc_bus_dvsn_dim_id,
		edw_strtgc_bus_dvsn_ak_id,
		strtgc_bus_dvsn_code,
		strtgc_bus_dvsn_code_descript,
		-- *INF*: IIF(isnull(strtgc_bus_dvsn_dim_id),-1,strtgc_bus_dvsn_dim_id)
		IFF(strtgc_bus_dvsn_dim_id IS NULL, - 1, strtgc_bus_dvsn_dim_id) AS strtgc_bus_dvsn_id_out,
		-- *INF*: IIF(isnull(edw_strtgc_bus_dvsn_ak_id),-1,edw_strtgc_bus_dvsn_ak_id)
		IFF(edw_strtgc_bus_dvsn_ak_id IS NULL, - 1, edw_strtgc_bus_dvsn_ak_id) AS edw_strtgc_bus_dvsn_ak_id_out,
		-- *INF*: IIF(isnull(strtgc_bus_dvsn_code),'N/A',strtgc_bus_dvsn_code)
		IFF(strtgc_bus_dvsn_code IS NULL, 'N/A', strtgc_bus_dvsn_code) AS strtgc_bus_dvsn_code_out,
		-- *INF*: IIF(isnull(strtgc_bus_dvsn_code_descript),'N/A',strtgc_bus_dvsn_code_descript)
		IFF(strtgc_bus_dvsn_code_descript IS NULL, 'N/A', strtgc_bus_dvsn_code_descript) AS strtgc_bus_dvsn_code_descript_out
		FROM LKP_strategic_business_division_dim
	),
	OUTPUT_return_Strategic_Business_Division AS (
		SELECT
		strtgc_bus_dvsn_id_out AS strtgc_bus_dvsn_dim_id, 
		edw_strtgc_bus_dvsn_ak_id_out AS edw_strtgc_bus_dvsn_ak_id, 
		strtgc_bus_dvsn_code_out AS strtgc_bus_dvsn_code, 
		strtgc_bus_dvsn_code_descript_out AS strtgc_bus_dvsn_code_descript
		FROM EXP_check_outputs
	),
),
EXP_Service_Amounts AS (
	SELECT
	EXP_Source.serv_charge,
	EXP_Source.serv_bill_review_red,
	EXP_Source.serv_network_red,
	serv_bill_review_red + serv_network_red AS v_gross_serv_saving,
	v_gross_serv_saving AS gross_serv_saving,
	-- *INF*: decode(true,
	-- serv_charge <> 0,
	-- (v_gross_serv_saving / serv_charge) * 100,
	-- 0)
	decode(
	    true,
	    serv_charge <> 0, (v_gross_serv_saving / serv_charge) * 100,
	    0
	) AS gross_serv_saving_perc,
	-- *INF*: decode(true,
	-- serv_charge <> 0,
	-- (serv_bill_review_red / serv_charge) * 100,
	-- 0
	-- )
	decode(
	    true,
	    serv_charge <> 0, (serv_bill_review_red / serv_charge) * 100,
	    0
	) AS service_review_saving_perc,
	-- *INF*: decode(true,
	-- serv_charge <> 0,
	-- (serv_network_red / serv_charge) * 100,
	-- 0
	-- )
	decode(
	    true,
	    serv_charge <> 0, (serv_network_red / serv_charge) * 100,
	    0
	) AS total_serv_network_red_perc,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	LKP_Med_Bill_Serv_Dim_ID.med_bill_serv_dim_id AS in_med_bill_serv_dim_id,
	-- *INF*: iif(isnull(in_med_bill_serv_dim_id),-1,in_med_bill_serv_dim_id)
	IFF(in_med_bill_serv_dim_id IS NULL, - 1, in_med_bill_serv_dim_id) AS med_bill_serv_dim_id,
	LKP_Claim_Occurrence_Dim_ID.claim_occurrence_dim_id,
	LKP_Med_Bill_Dim.med_bill_dim_id AS in_med_bill_dim_id,
	-- *INF*: iif(isnull(in_med_bill_dim_id),-1,in_med_bill_dim_id)
	IFF(in_med_bill_dim_id IS NULL, - 1, in_med_bill_dim_id) AS med_bill_dim_id,
	EXP_Capture_Date_Ids.claim_loss_date_id,
	EXP_Capture_Date_Ids.claim_discovery_date_id,
	EXP_Capture_Date_Ids.claim_rpted_date_id,
	EXP_Capture_Date_Ids.claim_open_date_id,
	EXP_Capture_Date_Ids.claim_close_date_id,
	EXP_Capture_Date_Ids.claim_reopen_date_id,
	EXP_Capture_Date_Ids.claim_closed_after_reopen_date_id,
	EXP_Capture_Date_Ids.claim_cat_start_date_id,
	EXP_Capture_Date_Ids.claim_cat_end_date_id,
	EXP_Capture_Date_Ids.claim_rep_assigned_date_id,
	EXP_Capture_Date_Ids.claim_rep_unassigned_date_id,
	EXP_Capture_Date_Ids.claim_scripted_date_id AS Claim_scripted_date_id,
	EXP_Capture_Date_Ids.source_claim_scripted_date_id AS Source_Claim_scripted_date_id,
	EXP_Capture_Date_Ids.claim_notice_only_date_id AS Claim_Notice_only_Date_ID,
	mplt_Claim_occurrence.agency_dim_id AS in_agency_dim_id,
	-- *INF*: iif(isnull(in_agency_dim_id),-1,in_agency_dim_id)
	IFF(in_agency_dim_id IS NULL, - 1, in_agency_dim_id) AS agency_dim_id,
	mplt_Claim_occurrence.contract_cust_dim_id AS in_contract_cust_dim_id,
	-- *INF*: iif(isnull(in_contract_cust_dim_id),-1,in_contract_cust_dim_id)
	IFF(in_contract_cust_dim_id IS NULL, - 1, in_contract_cust_dim_id) AS contract_cust_dim_id,
	mplt_Claim_occurrence.pol_key_dim_id AS in_pol_dim_id,
	-- *INF*: iif(isnull(in_pol_dim_id),-1,in_pol_dim_id)
	IFF(in_pol_dim_id IS NULL, - 1, in_pol_dim_id) AS pol_dim_id,
	EXP_Capture_Date_Ids.pol_eff_date_id,
	EXP_Capture_Date_Ids.pol_exp_date_id,
	LKP_Claimant_Dim.claimant_dim_id AS in_claimant_dim_id,
	-- *INF*: iif(isnull(in_claimant_dim_id),-1,in_claimant_dim_id)
	IFF(in_claimant_dim_id IS NULL, - 1, in_claimant_dim_id) AS claimant_dim_id,
	mplt_Claim_occurrence.claim_rep_dim_prim_claim_rep_id AS in_claim_rep_dim_prim_claim_rep_id,
	-- *INF*: iif(isnull(in_claim_rep_dim_prim_claim_rep_id),-1,in_claim_rep_dim_prim_claim_rep_id)
	IFF(in_claim_rep_dim_prim_claim_rep_id IS NULL, - 1, in_claim_rep_dim_prim_claim_rep_id) AS claim_rep_dim_prim_claim_rep_id,
	mplt_Claim_occurrence.claim_occurrence_dim_id AS claim_occurrence_dim_id1,
	mplt_Strategic_Business_Division_Dim.strtgc_bus_dvsn_dim_id,
	mplt_Claim_occurrence.AgencyDimID
	FROM EXP_Capture_Date_Ids
	 -- Manually join with EXP_Source
	 -- Manually join with mplt_Claim_occurrence
	 -- Manually join with mplt_Strategic_Business_Division_Dim
	LEFT JOIN LKP_Claim_Occurrence_Dim_ID
	ON LKP_Claim_Occurrence_Dim_ID.med_bill_ak_id = EXP_Source.med_bill_ak_id
	LEFT JOIN LKP_Claimant_Dim
	ON LKP_Claimant_Dim.edw_claim_party_occurrence_ak_id = LKP_Claim_Occurrence_Dim_ID.edw_claim_party_occurrence_ak_id
	LEFT JOIN LKP_Med_Bill_Dim
	ON LKP_Med_Bill_Dim.edw_med_bill_ak_id = EXP_Source.med_bill_ak_id
	LEFT JOIN LKP_Med_Bill_Serv_Dim_ID
	ON LKP_Med_Bill_Serv_Dim_ID.edw_med_bill_serv_pk_id = EXP_Source.med_bill_serv_id
),
LKP_Medical_Bill_Service_Fact AS (
	SELECT
	med_bill_serv_fact_id,
	med_bill_dim_id,
	med_bill_serv_dim_id,
	claim_occurrence_dim_id,
	claim_loss_date_id,
	claim_discovery_date_id,
	claim_scripted_date_id,
	source_claim_rpted_date_id,
	claim_rpted_date_id,
	claim_open_date_id,
	claim_close_date_id,
	claim_reopen_date_id,
	claim_closed_after_reopen_date_id,
	claim_notice_only_date_id,
	claim_cat_start_date_id,
	claim_cat_end_date_id,
	claim_rep_assigned_date_id,
	claim_rep_unassigned_date_id,
	gross_serv_saving,
	gross_serv_saving_percentage,
	total_serv_review_saving,
	serv_review_saving_percentage,
	total_serv_network_red,
	total_serv_network_red_percentage,
	total_serv_charge,
	agency_dim_id,
	contract_cust_dim_id,
	pol_dim_id,
	pol_eff_date_id,
	pol_exp_date_id,
	claimant_dim_id,
	claim_rep_dim_prim_claim_rep_id,
	strtgc_bus_dvsn_dim_id
	FROM (
		SELECT 
			med_bill_serv_fact_id,
			med_bill_dim_id,
			med_bill_serv_dim_id,
			claim_occurrence_dim_id,
			claim_loss_date_id,
			claim_discovery_date_id,
			claim_scripted_date_id,
			source_claim_rpted_date_id,
			claim_rpted_date_id,
			claim_open_date_id,
			claim_close_date_id,
			claim_reopen_date_id,
			claim_closed_after_reopen_date_id,
			claim_notice_only_date_id,
			claim_cat_start_date_id,
			claim_cat_end_date_id,
			claim_rep_assigned_date_id,
			claim_rep_unassigned_date_id,
			gross_serv_saving,
			gross_serv_saving_percentage,
			total_serv_review_saving,
			serv_review_saving_percentage,
			total_serv_network_red,
			total_serv_network_red_percentage,
			total_serv_charge,
			agency_dim_id,
			contract_cust_dim_id,
			pol_dim_id,
			pol_eff_date_id,
			pol_exp_date_id,
			claimant_dim_id,
			claim_rep_dim_prim_claim_rep_id,
			strtgc_bus_dvsn_dim_id
		FROM medical_bill_service_fact
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY med_bill_serv_dim_id ORDER BY med_bill_serv_fact_id DESC) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_Medical_Bill_Service_Fact.med_bill_serv_fact_id AS LKP_med_bill_serv_fact_id,
	LKP_Medical_Bill_Service_Fact.med_bill_dim_id AS LKP_med_bill_dim_id,
	LKP_Medical_Bill_Service_Fact.med_bill_serv_dim_id AS LKP_med_bill_serv_dim_id,
	LKP_Medical_Bill_Service_Fact.claim_occurrence_dim_id AS LKP_claim_occurrence_dim_id,
	LKP_Medical_Bill_Service_Fact.claim_loss_date_id AS LKP_claim_loss_date_id,
	LKP_Medical_Bill_Service_Fact.claim_discovery_date_id AS LKP_claim_discovery_date_id,
	LKP_Medical_Bill_Service_Fact.claim_scripted_date_id AS LKP_claim_scripted_date_id,
	LKP_Medical_Bill_Service_Fact.source_claim_rpted_date_id AS LKP_source_claim_rpted_date_id,
	LKP_Medical_Bill_Service_Fact.claim_rpted_date_id AS LKP_claim_rpted_date_id,
	LKP_Medical_Bill_Service_Fact.claim_open_date_id AS LKP_claim_open_date_id,
	LKP_Medical_Bill_Service_Fact.claim_close_date_id AS LKP_claim_close_date_id,
	LKP_Medical_Bill_Service_Fact.claim_reopen_date_id AS LKP_claim_reopen_date_id,
	LKP_Medical_Bill_Service_Fact.claim_closed_after_reopen_date_id AS LKP_claim_closed_after_reopen_date_id,
	LKP_Medical_Bill_Service_Fact.claim_notice_only_date_id AS LKP_claim_notice_only_date_id,
	LKP_Medical_Bill_Service_Fact.claim_cat_start_date_id AS LKP_claim_cat_start_date_id,
	LKP_Medical_Bill_Service_Fact.claim_cat_end_date_id AS LKP_claim_cat_end_date_id,
	LKP_Medical_Bill_Service_Fact.claim_rep_assigned_date_id AS LKP_claim_rep_assigned_date_id,
	LKP_Medical_Bill_Service_Fact.claim_rep_unassigned_date_id AS LKP_claim_rep_unassigned_date_id,
	LKP_Medical_Bill_Service_Fact.gross_serv_saving AS LKP_gross_serv_saving,
	LKP_Medical_Bill_Service_Fact.gross_serv_saving_percentage AS LKP_gross_serv_saving_percentage,
	LKP_Medical_Bill_Service_Fact.total_serv_review_saving AS LKP_total_serv_review_saving,
	LKP_Medical_Bill_Service_Fact.serv_review_saving_percentage AS LKP_serv_review_saving_percentage,
	LKP_Medical_Bill_Service_Fact.total_serv_network_red AS LKP_total_serv_network_red,
	LKP_Medical_Bill_Service_Fact.total_serv_network_red_percentage AS LKP_total_serv_network_red_percentage,
	LKP_Medical_Bill_Service_Fact.total_serv_charge AS LKP_total_serv_charge,
	LKP_Medical_Bill_Service_Fact.agency_dim_id AS LKP_agency_dim_id,
	LKP_Medical_Bill_Service_Fact.contract_cust_dim_id AS LKP_contract_cust_dim_id,
	LKP_Medical_Bill_Service_Fact.pol_dim_id AS LKP_pol_dim_id,
	LKP_Medical_Bill_Service_Fact.pol_eff_date_id AS LKP_pol_eff_date_id,
	LKP_Medical_Bill_Service_Fact.pol_exp_date_id AS LKP_pol_exp_date_id,
	LKP_Medical_Bill_Service_Fact.claimant_dim_id AS LKP_claimant_dim_id,
	LKP_Medical_Bill_Service_Fact.claim_rep_dim_prim_claim_rep_id AS LKP_claim_rep_dim_prim_claim_rep_id,
	LKP_Medical_Bill_Service_Fact.strtgc_bus_dvsn_dim_id AS LKP_strtgc_bus_dvsn_dim_id,
	EXP_Service_Amounts.serv_charge,
	EXP_Service_Amounts.serv_bill_review_red,
	EXP_Service_Amounts.serv_network_red,
	EXP_Service_Amounts.gross_serv_saving,
	EXP_Service_Amounts.gross_serv_saving_perc,
	EXP_Service_Amounts.service_review_saving_perc,
	EXP_Service_Amounts.total_serv_network_red_perc,
	EXP_Service_Amounts.audit_id,
	EXP_Service_Amounts.med_bill_serv_dim_id,
	EXP_Service_Amounts.claim_occurrence_dim_id,
	EXP_Service_Amounts.med_bill_dim_id,
	EXP_Service_Amounts.claim_loss_date_id,
	EXP_Service_Amounts.claim_discovery_date_id,
	EXP_Service_Amounts.claim_rpted_date_id,
	EXP_Service_Amounts.claim_open_date_id,
	EXP_Service_Amounts.claim_close_date_id,
	EXP_Service_Amounts.claim_reopen_date_id,
	EXP_Service_Amounts.claim_closed_after_reopen_date_id,
	EXP_Service_Amounts.claim_cat_start_date_id,
	EXP_Service_Amounts.claim_cat_end_date_id,
	EXP_Service_Amounts.claim_rep_assigned_date_id,
	EXP_Service_Amounts.claim_rep_unassigned_date_id,
	EXP_Service_Amounts.Claim_scripted_date_id,
	EXP_Service_Amounts.Source_Claim_scripted_date_id,
	EXP_Service_Amounts.Claim_Notice_only_Date_ID,
	EXP_Service_Amounts.agency_dim_id,
	EXP_Service_Amounts.contract_cust_dim_id,
	EXP_Service_Amounts.pol_dim_id,
	EXP_Service_Amounts.pol_eff_date_id,
	EXP_Service_Amounts.pol_exp_date_id,
	EXP_Service_Amounts.claimant_dim_id,
	EXP_Service_Amounts.claim_rep_dim_prim_claim_rep_id,
	EXP_Service_Amounts.strtgc_bus_dvsn_dim_id,
	-- *INF*: IIF(NOT ISNULL(LKP_med_bill_serv_fact_id),
	-- IIF(
	-- LKP_med_bill_dim_id  <> med_bill_dim_id OR
	-- LKP_claim_occurrence_dim_id  <> claim_occurrence_dim_id OR
	-- LKP_claim_loss_date_id  <>claim_loss_date_id OR
	-- LKP_claim_discovery_date_id  <>claim_discovery_date_id OR
	-- LKP_claim_scripted_date_id  <> Claim_scripted_date_id OR
	-- LKP_source_claim_rpted_date_id  <> Source_Claim_scripted_date_id OR
	-- LKP_claim_rpted_date_id  <> claim_rpted_date_id  OR
	-- LKP_claim_open_date_id  <> claim_open_date_id OR
	-- LKP_claim_close_date_id  <> claim_close_date_id OR
	-- LKP_claim_reopen_date_id  <> claim_reopen_date_id  OR
	-- LKP_claim_closed_after_reopen_date_id  <> claim_closed_after_reopen_date_id OR
	-- LKP_claim_notice_only_date_id  <> Claim_Notice_only_Date_ID OR
	-- LKP_claim_cat_start_date_id  <> claim_cat_start_date_id OR
	-- LKP_claim_cat_end_date_id  <> claim_cat_end_date_id OR
	-- LKP_claim_rep_assigned_date_id  <> claim_rep_assigned_date_id OR
	-- LKP_claim_rep_unassigned_date_id  <> claim_rep_unassigned_date_id OR
	-- LKP_gross_serv_saving  <> TRUNC(gross_serv_saving,2) OR
	-- LKP_gross_serv_saving_percentage  <> TRUNC(gross_serv_saving_perc,2) OR
	-- LKP_total_serv_review_saving  <>  TRUNC(serv_bill_review_red,2) OR
	-- LKP_serv_review_saving_percentage  <> TRUNC(service_review_saving_perc,2) OR
	-- LKP_total_serv_network_red  <> TRUNC(serv_network_red,2) OR
	-- LKP_total_serv_network_red_percentage  <> TRUNC(total_serv_network_red_perc,2) OR
	-- LKP_total_serv_charge  <> TRUNC(serv_charge,2) OR
	-- LKP_agency_dim_id  <> agency_dim_id OR
	-- LKP_contract_cust_dim_id  <> contract_cust_dim_id  OR
	-- LKP_pol_dim_id  <> pol_dim_id OR
	-- LKP_pol_eff_date_id  <> pol_eff_date_id OR
	-- LKP_pol_exp_date_id  <> pol_exp_date_id OR
	-- LKP_claimant_dim_id  <> claimant_dim_id OR
	-- LKP_claim_rep_dim_prim_claim_rep_id  <> claim_rep_dim_prim_claim_rep_id OR
	-- LKP_strtgc_bus_dvsn_dim_id  <> strtgc_bus_dvsn_dim_id
	-- ,'UPDATE','NOCHANGE'),
	-- 'INSERT')
	-- 
	-- ---- Had to use Trunc function to just limit the percentage to 2 places of precision.
	IFF(
	    LKP_med_bill_serv_fact_id IS NOT NULL,
	    IFF(
	        LKP_med_bill_dim_id <> med_bill_dim_id
	        or LKP_claim_occurrence_dim_id <> claim_occurrence_dim_id
	        or LKP_claim_loss_date_id <> claim_loss_date_id
	        or LKP_claim_discovery_date_id <> claim_discovery_date_id
	        or LKP_claim_scripted_date_id <> Claim_scripted_date_id
	        or LKP_source_claim_rpted_date_id <> Source_Claim_scripted_date_id
	        or LKP_claim_rpted_date_id <> claim_rpted_date_id
	        or LKP_claim_open_date_id <> claim_open_date_id
	        or LKP_claim_close_date_id <> claim_close_date_id
	        or LKP_claim_reopen_date_id <> claim_reopen_date_id
	        or LKP_claim_closed_after_reopen_date_id <> claim_closed_after_reopen_date_id
	        or LKP_claim_notice_only_date_id <> Claim_Notice_only_Date_ID
	        or LKP_claim_cat_start_date_id <> claim_cat_start_date_id
	        or LKP_claim_cat_end_date_id <> claim_cat_end_date_id
	        or LKP_claim_rep_assigned_date_id <> claim_rep_assigned_date_id
	        or LKP_claim_rep_unassigned_date_id <> claim_rep_unassigned_date_id
	        or LKP_gross_serv_saving <> TRUNC(gross_serv_saving,2)
	        or LKP_gross_serv_saving_percentage <> TRUNC(gross_serv_saving_perc,2)
	        or LKP_total_serv_review_saving <> TRUNC(serv_bill_review_red,2)
	        or LKP_serv_review_saving_percentage <> TRUNC(service_review_saving_perc,2)
	        or LKP_total_serv_network_red <> TRUNC(serv_network_red,2)
	        or LKP_total_serv_network_red_percentage <> TRUNC(total_serv_network_red_perc,2)
	        or LKP_total_serv_charge <> TRUNC(serv_charge,2)
	        or LKP_agency_dim_id <> agency_dim_id
	        or LKP_contract_cust_dim_id <> contract_cust_dim_id
	        or LKP_pol_dim_id <> pol_dim_id
	        or LKP_pol_eff_date_id <> pol_eff_date_id
	        or LKP_pol_exp_date_id <> pol_exp_date_id
	        or LKP_claimant_dim_id <> claimant_dim_id
	        or LKP_claim_rep_dim_prim_claim_rep_id <> claim_rep_dim_prim_claim_rep_id
	        or LKP_strtgc_bus_dvsn_dim_id <> strtgc_bus_dvsn_dim_id,
	        'UPDATE',
	        'NOCHANGE'
	    ),
	    'INSERT'
	) AS V_Changed_Flag,
	V_Changed_Flag AS Changed_Flag
	FROM EXP_Service_Amounts
	LEFT JOIN LKP_Medical_Bill_Service_Fact
	ON LKP_Medical_Bill_Service_Fact.med_bill_serv_dim_id = EXP_Service_Amounts.med_bill_serv_dim_id
),
RTR_Insert_Update AS (
	SELECT
	LKP_med_bill_serv_fact_id AS med_bill_serv_fact_id_exists,
	serv_charge,
	serv_bill_review_red,
	serv_network_red,
	gross_serv_saving,
	gross_serv_saving_perc,
	service_review_saving_perc,
	total_serv_network_red_perc,
	audit_id,
	med_bill_serv_dim_id,
	claim_occurrence_dim_id,
	med_bill_dim_id,
	Claim_scripted_date_id,
	Source_Claim_scripted_date_id,
	claim_loss_date_id,
	claim_discovery_date_id,
	claim_rpted_date_id,
	claim_open_date_id,
	claim_close_date_id,
	claim_reopen_date_id,
	claim_closed_after_reopen_date_id,
	Claim_Notice_only_Date_ID,
	claim_cat_start_date_id,
	claim_cat_end_date_id,
	claim_rep_assigned_date_id,
	claim_rep_unassigned_date_id,
	agency_dim_id,
	contract_cust_dim_id,
	pol_dim_id,
	pol_eff_date_id,
	pol_exp_date_id,
	claimant_dim_id,
	claim_rep_dim_prim_claim_rep_id,
	strtgc_bus_dvsn_dim_id,
	Changed_Flag
	FROM EXP_Detect_Changes
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE Changed_Flag ='INSERT'),
RTR_Insert_Update_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE Changed_Flag='UPDATE'),
UPD_Med_Bill_Serv_Fact_Update AS (
	SELECT
	med_bill_serv_fact_id_exists AS med_bill_serv_fact_id_exists2, 
	serv_charge AS serv_charge2, 
	serv_bill_review_red AS serv_bill_review_red2, 
	serv_network_red AS serv_network_red2, 
	gross_serv_saving AS gross_serv_saving2, 
	gross_serv_saving_perc AS gross_serv_saving_perc2, 
	service_review_saving_perc AS service_review_saving_perc2, 
	total_serv_network_red_perc AS total_serv_network_red_perc2, 
	audit_id AS audit_id2, 
	med_bill_serv_dim_id AS med_bill_serv_dim_id2, 
	claim_occurrence_dim_id AS claim_occurrence_dim_id2, 
	med_bill_dim_id AS med_bill_dim_id2, 
	claim_loss_date_id, 
	claim_discovery_date_id, 
	claim_rpted_date_id, 
	claim_open_date_id, 
	claim_close_date_id, 
	claim_reopen_date_id, 
	claim_closed_after_reopen_date_id, 
	claim_cat_start_date_id, 
	claim_cat_end_date_id, 
	claim_rep_assigned_date_id, 
	claim_rep_unassigned_date_id, 
	Claim_scripted_date_id AS Claim_scripted_date_id2, 
	Source_Claim_scripted_date_id AS Source_Claim_scripted_date_id2, 
	Claim_Notice_only_Date_ID AS Claim_Notice_only_Date_ID2, 
	agency_dim_id AS agency_dim_id2, 
	contract_cust_dim_id AS contract_cust_dim_id2, 
	pol_dim_id AS pol_dim_id2, 
	pol_eff_date_id AS pol_eff_date_id2, 
	pol_exp_date_id AS pol_exp_date_id2, 
	claimant_dim_id AS claimant_dim_id2, 
	claim_rep_dim_prim_claim_rep_id AS claim_rep_dim_prim_claim_rep_id2, 
	strtgc_bus_dvsn_dim_id AS strtgc_bus_dvsn_dim_id2
	FROM RTR_Insert_Update_UPDATE
),
medical_bill_service_fact_upd AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill_service_fact AS T
	USING UPD_Med_Bill_Serv_Fact_Update AS S
	ON T.med_bill_serv_fact_id = S.med_bill_serv_fact_id_exists2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.med_bill_dim_id = S.med_bill_dim_id2, T.med_bill_serv_dim_id = S.med_bill_serv_dim_id2, T.claim_occurrence_dim_id = S.claim_occurrence_dim_id2, T.claim_loss_date_id = S.claim_loss_date_id, T.claim_discovery_date_id = S.claim_discovery_date_id, T.claim_scripted_date_id = S.Claim_scripted_date_id2, T.source_claim_rpted_date_id = S.Source_Claim_scripted_date_id2, T.claim_rpted_date_id = S.claim_rpted_date_id, T.claim_open_date_id = S.claim_open_date_id, T.claim_close_date_id = S.claim_close_date_id, T.claim_reopen_date_id = S.claim_reopen_date_id, T.claim_closed_after_reopen_date_id = S.claim_closed_after_reopen_date_id, T.claim_notice_only_date_id = S.Claim_Notice_only_Date_ID2, T.claim_cat_start_date_id = S.claim_cat_start_date_id, T.claim_cat_end_date_id = S.claim_cat_end_date_id, T.claim_rep_assigned_date_id = S.claim_rep_assigned_date_id, T.claim_rep_unassigned_date_id = S.claim_rep_unassigned_date_id, T.gross_serv_saving = S.gross_serv_saving2, T.gross_serv_saving_percentage = S.gross_serv_saving_perc2, T.total_serv_review_saving = S.serv_bill_review_red2, T.serv_review_saving_percentage = S.service_review_saving_perc2, T.total_serv_network_red = S.serv_network_red2, T.total_serv_network_red_percentage = S.total_serv_network_red_perc2, T.total_serv_charge = S.serv_charge2, T.audit_id = S.audit_id2, T.agency_dim_id = S.agency_dim_id2, T.contract_cust_dim_id = S.contract_cust_dim_id2, T.pol_dim_id = S.pol_dim_id2, T.pol_eff_date_id = S.pol_eff_date_id2, T.pol_exp_date_id = S.pol_exp_date_id2, T.claimant_dim_id = S.claimant_dim_id2, T.claim_rep_dim_prim_claim_rep_id = S.claim_rep_dim_prim_claim_rep_id2, T.strtgc_bus_dvsn_dim_id = S.strtgc_bus_dvsn_dim_id2
),
medical_bill_service_fact_ins AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill_service_fact
	(med_bill_dim_id, med_bill_serv_dim_id, claim_occurrence_dim_id, claim_loss_date_id, claim_discovery_date_id, claim_scripted_date_id, source_claim_rpted_date_id, claim_rpted_date_id, claim_open_date_id, claim_close_date_id, claim_reopen_date_id, claim_closed_after_reopen_date_id, claim_notice_only_date_id, claim_cat_start_date_id, claim_cat_end_date_id, claim_rep_assigned_date_id, claim_rep_unassigned_date_id, gross_serv_saving, gross_serv_saving_percentage, total_serv_review_saving, serv_review_saving_percentage, total_serv_network_red, total_serv_network_red_percentage, total_serv_charge, audit_id, agency_dim_id, contract_cust_dim_id, pol_dim_id, pol_eff_date_id, pol_exp_date_id, claimant_dim_id, claim_rep_dim_prim_claim_rep_id, strtgc_bus_dvsn_dim_id)
	SELECT 
	MED_BILL_DIM_ID, 
	MED_BILL_SERV_DIM_ID, 
	CLAIM_OCCURRENCE_DIM_ID, 
	CLAIM_LOSS_DATE_ID, 
	CLAIM_DISCOVERY_DATE_ID, 
	Claim_scripted_date_id AS CLAIM_SCRIPTED_DATE_ID, 
	Source_Claim_scripted_date_id AS SOURCE_CLAIM_RPTED_DATE_ID, 
	CLAIM_RPTED_DATE_ID, 
	CLAIM_OPEN_DATE_ID, 
	CLAIM_CLOSE_DATE_ID, 
	CLAIM_REOPEN_DATE_ID, 
	CLAIM_CLOSED_AFTER_REOPEN_DATE_ID, 
	Claim_Notice_only_Date_ID AS CLAIM_NOTICE_ONLY_DATE_ID, 
	CLAIM_CAT_START_DATE_ID, 
	CLAIM_CAT_END_DATE_ID, 
	CLAIM_REP_ASSIGNED_DATE_ID, 
	CLAIM_REP_UNASSIGNED_DATE_ID, 
	GROSS_SERV_SAVING, 
	gross_serv_saving_perc AS GROSS_SERV_SAVING_PERCENTAGE, 
	serv_bill_review_red AS TOTAL_SERV_REVIEW_SAVING, 
	service_review_saving_perc AS SERV_REVIEW_SAVING_PERCENTAGE, 
	serv_network_red AS TOTAL_SERV_NETWORK_RED, 
	total_serv_network_red_perc AS TOTAL_SERV_NETWORK_RED_PERCENTAGE, 
	serv_charge AS TOTAL_SERV_CHARGE, 
	AUDIT_ID, 
	AGENCY_DIM_ID, 
	CONTRACT_CUST_DIM_ID, 
	POL_DIM_ID, 
	POL_EFF_DATE_ID, 
	POL_EXP_DATE_ID, 
	CLAIMANT_DIM_ID, 
	CLAIM_REP_DIM_PRIM_CLAIM_REP_ID, 
	STRTGC_BUS_DVSN_DIM_ID
	FROM RTR_Insert_Update_INSERT
),
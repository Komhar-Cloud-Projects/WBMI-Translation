WITH
LKP_MAX_CREATE_DATE AS (
	SELECT
	WORK_HIST_CREATE_DATE,
	WC_CLAIMANT_DET_AK_ID
	FROM (
		SELECT  
		p.wc_claimant_det_ak_id                    as  WC_CLAIMANT_DET_AK_ID ,
		p.work_hist_create_date                   as WORK_HIST_CREATE_DATE 
		FROM (
		SELECT a.wc_claimant_det_ak_id                                as  WC_CLAIMANT_DET_AK_ID ,
		MAX(a.work_hist_created_date )                   as WORK_HIST_CREATE_DATE 
		FROM workers_comp_claimant_work_history a
		where a.crrnt_snpsht_flag = 1
		GROUP BY a.wc_claimant_det_ak_id ) p
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WC_CLAIMANT_DET_AK_ID ORDER BY WORK_HIST_CREATE_DATE DESC) = 1
),
SQ_workers_comp_claimant_detail AS (
	select
		a.wc_claimant_det_ak_id
		,a.maint_type_code
		,CONVERT(CHAR(10),a.send_to_state_time,112)
		,a.claim_party_occurrence_ak_id
		,a.jurisdiction_state_code
		,(case a.state_claim_num when 'N/A'  then ' ' else a.state_claim_num end) state_claim_num
		,a.wc_claimant_num
		,(case a.sic_code when 'N/A'  then '' else a.sic_code end) sic_code
		,a.emp_dept_num
		,(case a.nature_inj_code when 'N/A' then '' else a.nature_inj_code end) nature_inj_code
		,(case a.body_part_code when 'N/A' then '' else a.body_part_code end) body_part_code
		,(case a.cause_inj_code when 'N/A' then '' else a.cause_inj_code end) cause_inj_code
		,(case a.inital_treatment_code when 'N/A'  then ''  else a.inital_treatment_code end) inital_treatment_code
		,CONVERT(CHAR(10),a.emplyr_notified_date,112)
		,CONVERT(CHAR(10),a.reported_to_carrier_date,112)
		,(case a.marital_status when 'N/A' then '' else a.marital_status end) marital_status
		,a.num_of_dependents
		,CONVERT(CHAR(10),a.disability_date,112)
		,b.claim_party_death_date
		,(case a.emplymnt_status_code when 'N/A' then '' else  a.emplymnt_status_code end) emplymnt_status_code
		,a.hired_date
		,a.avg_wkly_wage
		,a.wage_period_code
		,Cast(Round(a.emp_day_week,0)as int) as emp_day_week
		,a.full_pay_inj_day_ind
		,a.pre_exst_disability_ind
		,a.max_med_improvement_date
		,a.obtained_lgl_representation_date
		,a.inj_result_death_ind
		,a.fringe_bnft_discontinued_amt
		,a.med_auth_ind
		,a.education_lvl
		,a.auth_to_release_ssn_ind
		,a.occuptn_code
		,a.inj_loc_comment
		,(case a.premises_code when 'N/A' then '' else a.premises_code end) premises_code
		,a.claim_ctgry_code
		,a.wage_rate_amt
		,(case a.emp_id_num when 'N/A' then '' else a.emp_id_num end) emp_id_num
		,(case a.emp_id_type when 'N/A' then '' else a.emp_id_type end) emp_id_type
		,(case a.work_week_days when 'N/A' then '' else a.work_week_days end) work_week_days 
		,(case a.work_week_type when 'N/A' then '' else a.work_week_type end) work_week_type
		,CONVERT(CHAR(10),a.emplyr_lost_time_notified_date,112)
		,(case a.NaicsCode when 'N/A' then '' else a.NaicsCode end) NaicsCode
		,(case a.salary_paid_ind when 'N/A' then '' else a.salary_paid_ind end) salary_paid_ind 
		,(case a.phys_restriction_ind when 'N/A' then '' else a.phys_restriction_ind end) phys_restriction_ind
		,(case a.emp_security_id  when 'N/A' then '' else a.emp_security_id  end) emp_security_id 
		,(case a.act_status_code  when 'N/A' then '' else a.act_status_code  end) act_status_code 
		,(case a.FullDenialReasonCode  when 'N/A' then '' else a.FullDenialReasonCode end) FullDenialReasonCode
		,(case a.IAIABCLossTypeCode  when 'N/A' then '' else a.IAIABCLossTypeCode end) IAIABCLossTypeCode
		,(case a.FROILateReasonCode  when 'N/A' then '' else a.FROILateReasonCode end) FROILateReasonCode
		,(case a.ManualClassificationCode  when 'N/A' then '' else a.ManualClassificationCode  end) ManualClassificationCode
		,a.num_of_exemptions
		,b.claim_party_key
		,b.tax_fed_id
		,(case b.claim_party_full_name when 'N/A' then '' else b.claim_party_full_name end) claim_party_full_name
		,(case b.claim_party_addr when 'N/A' then '' else b.claim_party_addr end) claim_party_addr 
		,(case b.tax_ssn_id when 'N/A' then '' else b.tax_ssn_id end) tax_ssn_id
		,(case b.claim_party_first_name when 'N/A' then '' else b.claim_party_first_name end) claim_party_first_name                  
		,(case b.claim_party_mid_name when 'N/A' then '' else b.claim_party_mid_name end) claim_party_mid_name
		,(case b.claim_party_last_name when 'N/A' then '' else b.claim_party_last_name end) claim_party_last_name
		,(case b.claim_party_city when 'N/A' then '' else b.claim_party_city end) claim_party_city
		,(case b.claim_party_state when 'N/A' then '' else b.claim_party_state end) claim_party_state
		,(case b.claim_party_zip when 'N/A' then '' else b.claim_party_zip end) claim_party_zip
		,b.ph_num
		,CONVERT(CHAR(10),b.claim_party_birthdate,112)
		,b.claim_party_gndr
		,(case b.claim_party_name_sfx when 'N/A' then '' else b.claim_party_name_sfx end) claim_party_name_sfx
		,f.occuptn_descript
		,d.claim_party_role_code
		,d.claim_party_ak_id
		,SPC.StrategicProfitCenterDescription
		,c.claim_occurrence_ak_id
		,c.pol_key_ak_id
		,c.claim_loss_date
		,CONVERT(CHAR(10),c.claim_loss_date,112)
		,c.claim_occurrence_key
		,c.loss_loc_zip
		,c.claim_loss_descript
		,(case c.source_claim_occurrence_status_code when 'N/A' then '' else c.source_claim_occurrence_status_code end) source_claim_occurrence_status_code
		,c.loss_loc_county
		,c.loss_loc_city
		,c.loss_loc_addr
		,c.loss_loc_state
		,h.claim_rep_email
		,h.claim_rep_full_name
		,rep.ph_num
		,rep.ph_extension
		,a.FROIClaimType
		, a.FROIFullDenialReasonNarrative 
	From workers_comp_claimant_detail a
	INNER JOIN  claim_party_occurrence d ON
		a.claim_party_occurrence_ak_id = d.claim_party_occurrence_ak_id
		and d.crrnt_snpsht_flag = 1 and a.crrnt_snpsht_flag = 1
	INNER JOIN claim_occurrence c ON
		d.claim_occurrence_ak_id = c.claim_occurrence_ak_id
		and claim_occurrence_type_code='WCC' and c.crrnt_snpsht_flag = 1 and d.crrnt_snpsht_flag = 1
	INNER JOIN claim_party b ON
		d.claim_party_ak_id = b.claim_party_ak_id
		and b.crrnt_snpsht_flag = 1
	LEFT OUTER JOIN sup_workers_comp_occupation f ON
		a.occuptn_code =  f.occuptn_code
		and f.crrnt_snpsht_flag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Policy pol ON
		pol.pol_ak_id = c.pol_key_ak_id
		and pol.crrnt_snpsht_flag=1
	INNER JOIN StrategicProfitCenter SPC ON
		SPC.StrategicProfitCenterAkID = pol.StrategicProfitCenterAkID and SPC.CurrentSnapshotFlag=1
	INNER JOIN claim_representative_occurrence g ON
		g.claim_occurrence_ak_id = c.claim_occurrence_ak_id
		and g.crrnt_snpsht_flag=1 and g.claim_rep_role_code='H'
	INNER JOIN claim_representative h
		ON h.claim_rep_ak_id = g.claim_rep_ak_id
		and h.crrnt_snpsht_flag=1
	inner join claim_party rep
		ON rep.claim_party_key = h.claim_rep_key
		and rep.crrnt_snpsht_flag=1 
	left join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkNcciMitchell dfm 
		on a.wc_claimant_num=dfm.ClaimAdminClaimNumber 
		and a.crrnt_snpsht_flag=1
		and CONVERT(CHAR(10),a.send_to_state_time,112)=dfm.MaitenanceTypeCodeDate
	WHERE a.crrnt_snpsht_flag = 1  
		and  a.jurisdiction_state_code  IN (@{pipeline().parameters.STATE_CODE})
		and a.send_to_state_ind = 'Y'
		and a.created_date >= '@{pipeline().parameters.SELECTION_START_TS}'
		and dfm.ClaimAdminClaimNumber is null
		and a.source_sys_id <> 'PMS'
		@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_SOURCE AS (
	SELECT
	'148' AS TransactionSetID,
	wc_claimant_det_ak_id,
	maint_type_code,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(maint_type_code))='N/A','',
	-- LTRIM(RTRIM(maint_type_code)) )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(maint_type_code)) = 'N/A', '',
	    LTRIM(RTRIM(maint_type_code))
	) AS o_maint_type_code,
	send_to_state_time,
	-- *INF*: send_to_state_time
	-- 
	-- 
	-- --IIF(send_to_state_time=TO_CHAR(TO_DATE('1800/01/01','YYYY/MM/DD'),'YYYYMMDD'),' ',IIF(send_to_state_time <= TO_CHAR(SYSDATE,'YYYYMMDD'),IIF(send_to_state_time >= convert_claim_loss_date, send_to_state_time,' ')))
	send_to_state_time AS v_send_to_state_time,
	v_send_to_state_time AS o_send_to_state_time,
	claim_party_occurrence_ak_id,
	jurisdiction_state_code AS in_jurisdiction_state_code,
	state_claim_num AS in_state_claim_num,
	-- *INF*: :LKP.LKP_MAX_CREATE_DATE(wc_claimant_det_ak_id)
	LKP_MAX_CREATE_DATE_wc_claimant_det_ak_id.WORK_HIST_CREATE_DATE AS v_max_created_date,
	v_max_created_date AS o_max_created_date,
	wc_claimant_num AS in_wc_claimant_num,
	in_wc_claimant_num AS v_wc_claimant_num,
	v_wc_claimant_num AS o_wc_claimant_num,
	sic_code AS in_sic_code,
	-- *INF*: IIF(LTRIM(RTRIM(in_sic_code))='N/A','',
	-- LTRIM(RTRIM(in_sic_code)) )
	-- 
	IFF(LTRIM(RTRIM(in_sic_code)) = 'N/A', '', LTRIM(RTRIM(in_sic_code))) AS v_sic_code,
	v_sic_code AS o_sic_code,
	emp_dept_num AS in_emp_dept_num,
	-- *INF*: IIF(LTRIM(RTRIM(in_emp_dept_num))='N/A','WC0001',
	-- SUBSTR(ltrim(rtrim(in_emp_dept_num)),1,15)  )
	IFF(
	    LTRIM(RTRIM(in_emp_dept_num)) = 'N/A', 'WC0001',
	    SUBSTR(ltrim(rtrim(in_emp_dept_num)), 1, 15)
	) AS v_emp_dept_num,
	v_emp_dept_num AS o_emp_dept_num,
	nature_inj_code AS in_nature_inj_code,
	-- *INF*: LTRIM(RTRIM(in_nature_inj_code))
	-- 
	-- --IIF(LTRIM(RTRIM(in_nature_inj_code))='N/A','',LTRIM(RTRIM(in_nature_inj_code)) )
	-- 
	-- --IIF (in_nature_inj_code='N/A','',RPAD(in_nature_inj_code,2,' '))
	-- 
	-- --Straight move.  Truncate to 2 posiions. If N/A send 2 blank spaces
	LTRIM(RTRIM(in_nature_inj_code)) AS v_nature_inj_code,
	-- *INF*: v_nature_inj_code
	-- 
	-- --Straight move.  Truncate to 2 posiions. If N/A send 2 blank spaces
	v_nature_inj_code AS o_nature_inj_code,
	body_part_code AS in_body_part_code,
	-- *INF*: LTRIM(RTRIM(in_body_part_code))
	-- 
	-- --IIF(LTRIM(RTRIM(in_body_part_code))='N/A','',LTRIM(RTRIM(in_body_part_code)) )
	-- 
	-- 
	-- --IIF(in_body_part_code='N/A','',RPAD(in_body_part_code,2,' '))
	LTRIM(RTRIM(in_body_part_code)) AS v_body_part_code,
	v_body_part_code AS o_body_part_code,
	cause_inj_code AS in_cause_inj_code,
	-- *INF*: LTRIM(RTRIM(in_cause_inj_code))
	-- 
	-- --IIF(LTRIM(RTRIM(in_cause_inj_code))='N/A','',LTRIM(RTRIM(in_cause_inj_code)) )
	-- 
	-- 
	-- --IIF(in_cause_inj_code='N/A','',RPAD(in_cause_inj_code,2,' '))
	LTRIM(RTRIM(in_cause_inj_code)) AS v_cause_inj_code,
	v_cause_inj_code AS o_cause_inj_code,
	inital_treatment_code AS in_inital_treatment_code,
	-- *INF*: IIF(ISNULL(in_inital_treatment_code),'',ltrim(rtrim(in_inital_treatment_code)) )
	-- 
	-- 
	-- 
	-- --IIF(ISNULL(in_inital_treatment_code),'',IIF(in_inital_treatment_code='N/A','',RPAD(in_inital_treatment_code,2,' ')))
	-- 
	-- --Straight Move. Truncate to 2 positions. Left justify pad with space. If no value or = 'N/A' then send 2 blank spaces
	IFF(in_inital_treatment_code IS NULL, '', ltrim(rtrim(in_inital_treatment_code))) AS v_inital_treatment_code,
	v_inital_treatment_code AS o_inital_treatment_code,
	reported_to_carrier_date AS in_reported_to_carrier_date,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(in_reported_to_carrier_date)) = '18000101',TO_CHAR(claim_loss_date,'YYYYMMDD'),
	-- LTRIM(RTRIM(in_reported_to_carrier_date))
	-- )
	-- 
	-- 
	-- --DECODE(TRUE,
	-- --LTRIM(RTRIM(in_reported_to_carrier_date)) != '18000101', LTRIM(RTRIM(in_reported_to_carrier_date)),
	-- --LTRIM(RTRIM(in_reported_to_carrier_date)) = '18000101', 
	-- --TO_CHAR(claim_loss_date,'YYYYMMDD'),
	-- --'')
	-- 
	-- --IIF(LTRIM(RTRIM(in_reported_to_carrier_date))  = '18000101',TO_CHAR(claim_loss_date,'YYYYMMDD'),LTRIM(RTRIM(in_reported_to_carrier_date)))
	-- 
	-- 
	-- --IIF(LTRIM(RTRIM(in_reported_to_carrier_date))--='18000101',RPAD('',8,' '),LTRIM(RTRIM(in_reported_to_carrier_date)))
	-- 
	-- --IIF(in_reported_to_carrier_date=TO_CHAR(TO_DATE('18000101','YYYYMMDD'),'YYYYMMDD'),RPAD('',8,' '),in_reported_to_carrier_date)
	-- 
	-- --convert(char(10),reported_to_carrier_date,112), if = '1800-01-01' send 8 blank spaces
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(in_reported_to_carrier_date)) = '18000101', TO_CHAR(claim_loss_date, 'YYYYMMDD'),
	    LTRIM(RTRIM(in_reported_to_carrier_date))
	) AS v_reported_to_carrier_date,
	v_reported_to_carrier_date AS o_reported_to_carrier_date,
	emplyr_notified_date AS in_emplyr_notified_date,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(in_emplyr_notified_date)) = '18000101',TO_CHAR(claim_loss_date,'YYYYMMDD'),
	-- LTRIM(RTRIM(in_emplyr_notified_date)) 
	-- )
	-- 
	-- 
	-- --DECODE(TRUE,
	-- --LTRIM(RTRIM(in_emplyr_notified_date)) != '18000101' ,LTRIM(RTRIM(in_emplyr_notified_date)),
	-- --LTRIM(RTRIM(in_emplyr_notified_date)) = '18000101'  AND LTRIM(RTRIM(in_reported_to_carrier_date))  != --'18000101',LTRIM(RTRIM(in_reported_to_carrier_date)),
	-- --LTRIM(RTRIM(in_emplyr_notified_date)) = '18000101'  AND LTRIM(RTRIM(in_reported_to_carrier_date)) = '18000101',TO_CHAR(claim_loss_date,'YYYYMMDD'),
	-- --'')
	-- 
	-- --IIF(LTRIM(RTRIM(in_emplyr_notified_date))  = '18000101',TO_CHAR(claim_loss_date,'YYYYMMDD'),LTRIM(RTRIM(in_emplyr_notified_date)))
	-- 
	-- --Must be valid date
	-- --Must < =today's date
	-- --Must < =MTC dtate
	-- --Must be  > date of injury
	-- 
	-- --convert(char(10),emplyr_notified_date,112), if = '1800-01-01' then claim_occurrence.claim_loss_date.
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(in_emplyr_notified_date)) = '18000101', TO_CHAR(claim_loss_date, 'YYYYMMDD'),
	    LTRIM(RTRIM(in_emplyr_notified_date))
	) AS v_emplyr_notified_date,
	-- *INF*: v_emplyr_notified_date
	-- 
	-- 
	-- 
	-- --convert(char(10),emplyr_notified_date,112), if = '1800-01-01' then claim_occurrence.claim_loss_date.
	v_emplyr_notified_date AS o_emplyr_notified_date,
	marital_status,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(marital_status)) = 'N/A', '' ,
	-- LTRIM(RTRIM(marital_status)) )
	-- 
	-- 
	-- --LTRIM(RTRIM(marital_status))
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(marital_status)) = 'N/A', '',
	    LTRIM(RTRIM(marital_status))
	) AS v_marital_status,
	v_marital_status AS o_marital_status,
	claim_party_death_date AS in_death_date,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(in_inj_result_death_ind)) = 'Y'  AND TO_CHAR(in_death_date,'YYYYMMDD')  != '18000101' ,TO_CHAR(in_death_date,'YYYYMMDD'),
	-- --LTRIM(RTRIM(in_inj_result_death_ind)) = 'N'  AND TO_CHAR(in_death_date,'YYYYMMDD')   =  '18000101','',
	-- '')
	-- 
	-- --Case when workers_comp_claimant_detail.inj_result_death_ind = 'Y' and death_date <> '1800-01-01'  then convert(char(10),death_date,112). If = 'N' or 'Y' and death_date = '1800-01-01'  then send 8 blank spaces
	-- 
	-- 
	-- 
	-- 
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(in_inj_result_death_ind)) = 'Y' AND TO_CHAR(in_death_date, 'YYYYMMDD') != '18000101', TO_CHAR(in_death_date, 'YYYYMMDD'),
	    ''
	) AS v_death_date,
	v_death_date AS o_death_date,
	inj_result_death_ind AS in_inj_result_death_ind,
	-- *INF*: DECODE(TRUE,
	-- 	IN(LTRIM(RTRIM(in_jurisdiction_state_code)),'MN','KS','KY'),
	-- 		DECODE(TRUE,
	-- 			TO_CHAR(in_death_date,'YYYYMMDD') <> '18000101' AND LTRIM(RTRIM(in_inj_result_death_ind)) <> 'N/A', 
	-- 				LTRIM(RTRIM(in_inj_result_death_ind)),
	-- 			''),
	-- 	IN(LTRIM(RTRIM(in_jurisdiction_state_code)),'IL'),
	-- 		DECODE(TRUE,
	-- 		LTRIM(RTRIM(in_inj_result_death_ind)) <> 'N/A', 
	-- 			LTRIM(RTRIM(in_inj_result_death_ind)),
	-- 			''),
	-- 	IN(LTRIM(RTRIM(in_jurisdiction_state_code)),'IA'),
	-- 		DECODE(TRUE,
	-- 		LTRIM(RTRIM(in_inj_result_death_ind)) = 'Y', 
	-- 			LTRIM(RTRIM(in_inj_result_death_ind)),
	-- 			''),
	-- 	'')
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(in_jurisdiction_state_code)) IN ('MN','KS','KY'), DECODE(
	        TRUE,
	        TO_CHAR(in_death_date, 'YYYYMMDD') <> '18000101' AND LTRIM(RTRIM(in_inj_result_death_ind)) <> 'N/A', LTRIM(RTRIM(in_inj_result_death_ind)),
	        ''
	    ),
	    LTRIM(RTRIM(in_jurisdiction_state_code)) IN ('IL'), DECODE(
	        TRUE,
	        LTRIM(RTRIM(in_inj_result_death_ind)) <> 'N/A', LTRIM(RTRIM(in_inj_result_death_ind)),
	        ''
	    ),
	    LTRIM(RTRIM(in_jurisdiction_state_code)) IN ('IA'), DECODE(
	        TRUE,
	        LTRIM(RTRIM(in_inj_result_death_ind)) = 'Y', LTRIM(RTRIM(in_inj_result_death_ind)),
	        ''
	    ),
	    ''
	) AS v_inj_result_death_ind,
	v_inj_result_death_ind AS o_inj_result_death_ind,
	num_of_dependents,
	disability_date AS in_disability_date,
	-- *INF*: IIF(ltrim(rtrim(in_disability_date))='18000101','',LTRIM(RTRIM(in_disability_date)) )
	-- 
	-- 
	-- --convert(char(10),disability_date,112). If disability_date = '1800-01-01' then send 8 blank spaces.
	IFF(ltrim(rtrim(in_disability_date)) = '18000101', '', LTRIM(RTRIM(in_disability_date))) AS v_disability_date,
	v_disability_date AS o_disability_date,
	emplymnt_status_code AS in_emplymnt_status_code,
	-- *INF*: LTRIM(RTRIM(in_emplymnt_status_code)) 
	-- 
	LTRIM(RTRIM(in_emplymnt_status_code)) AS v_emplymnt_status_code,
	v_emplymnt_status_code AS o_emplymnt_status_code,
	hired_date,
	-- *INF*: DECODE(TRUE,
	-- TO_CHAR(hired_date,'YYYYMMDD') = '18000101', '' ,
	-- TO_CHAR(hired_date,'YYYYMMDD') 
	-- )
	-- 
	-- 
	-- 
	-- --IIF(TO_CHAR(hired_date,'YYYYMMDD') != '18000101'
	-- --,TO_CHAR(hired_date,'YYYYMMDD'),'')
	-- 
	-- 
	-- 
	DECODE(
	    TRUE,
	    TO_CHAR(hired_date, 'YYYYMMDD') = '18000101', '',
	    TO_CHAR(hired_date, 'YYYYMMDD')
	) AS v_hired_date,
	v_hired_date AS o_hired_date,
	avg_wkly_wage AS in_avg_wkly_wage,
	-- *INF*: TO_CHAR((in_avg_wkly_wage* 100))
	-- 
	-- --IIF(in_avg_wkly_wage = ROUND(in_avg_wkly_wage) , lpad(TO_CHAR(ROUND(in_avg_wkly_wage)),8,'0') || '.00' ,  
	-- --lpad(TO_CHAR(in_avg_wkly_wage),11,'0') )     
	-- 
	-- 
	TO_CHAR((in_avg_wkly_wage * 100)) AS v_avg_wkly_wage,
	-- *INF*: v_avg_wkly_wage
	-- 
	-- --v_avg_wkly_wage_reformatted
	v_avg_wkly_wage AS o_avg_wkly_wage,
	wage_period_code AS in_wage_period_code,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(v_avg_wkly_wage))  != '' AND LTRIM(RTRIM(v_avg_wkly_wage)) != '0','01',
	-- '' )
	-- 
	-- 
	-- --DECODE(LTRIM(RTRIM(in_wage_period_code)),
	-- --'W','01',
	-- --'B','02',
	-- --'M','04',
	-- --'D','06',
	-- --'H','07',
	-- --'')
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(v_avg_wkly_wage)) != '' AND LTRIM(RTRIM(v_avg_wkly_wage)) != '0', '01',
	    ''
	) AS v_wage_period_code,
	v_wage_period_code AS wage_period_code,
	emp_day_week,
	full_pay_inj_day_ind AS in_full_pay_inj_day_ind,
	-- *INF*: DECODE(LTRIM(RTRIM(in_full_pay_inj_day_ind)),
	-- 'N','N',
	-- 'Y','Y',
	-- 'N/A','',
	-- '')
	-- 
	-- --IIF(in_full_pay_inj_day_ind='N',in_full_pay_inj_day_ind,
	-- --IIF(in_full_pay_inj_day_ind='Y',in_full_pay_inj_day_ind,
	-- --IIF(in_full_pay_inj_day_ind='N/A','  ')))
	-- 
	-- --Straight Move 'Y'; else 'N'; 
	-- --If 'N/A' move 2 blank spaces
	DECODE(
	    LTRIM(RTRIM(in_full_pay_inj_day_ind)),
	    'N', 'N',
	    'Y', 'Y',
	    'N/A', '',
	    ''
	) AS v_full_pay_inj_day_ind,
	v_full_pay_inj_day_ind AS o_full_pay_inj_day_ind,
	pre_exst_disability_ind AS in_pre_exst_disability_ind,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(in_pre_exst_disability_ind)) = 'N/A','',
	-- LTRIM(RTRIM(in_pre_exst_disability_ind)) )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(in_pre_exst_disability_ind)) = 'N/A', '',
	    LTRIM(RTRIM(in_pre_exst_disability_ind))
	) AS v_pre_exst_disability_ind,
	v_pre_exst_disability_ind AS o_pre_exst_disability_ind,
	max_med_improvement_date,
	-- *INF*: DECODE(TRUE,
	-- TO_CHAR(max_med_improvement_date,'YYYYMMDD') != '18000101',TO_CHAR(max_med_improvement_date,'YYYYMMDD'),
	-- '' )
	-- 
	-- --IIF(TO_CHAR(max_med_improvement_date,'YYYYMMDD')='18000101','',TO_CHAR(max_med_improvement_date,'YYYYMMDD'))
	DECODE(
	    TRUE,
	    TO_CHAR(max_med_improvement_date, 'YYYYMMDD') != '18000101', TO_CHAR(max_med_improvement_date, 'YYYYMMDD'),
	    ''
	) AS v_max_med_improvement_date,
	v_max_med_improvement_date AS o_max_med_improvement_date,
	obtained_lgl_representation_date,
	-- *INF*: DECODE(TRUE,
	-- TO_CHAR(obtained_lgl_representation_date,'YYYYMMDD') != '18000101',TO_CHAR(obtained_lgl_representation_date,'YYYYMMDD') ,
	-- '' )
	-- 
	-- --IIF(TO_CHAR(obtained_lgl_representation_date,'YYYYMMDD')--='18000101','',TO_CHAR(obtained_lgl_representation_date,'YYYYMMDD'))
	DECODE(
	    TRUE,
	    TO_CHAR(obtained_lgl_representation_date, 'YYYYMMDD') != '18000101', TO_CHAR(obtained_lgl_representation_date, 'YYYYMMDD'),
	    ''
	) AS v_obtained_lgl_representation_date,
	v_obtained_lgl_representation_date AS o_obtained_lgl_representation_date,
	fringe_bnft_discontinued_amt,
	med_auth_ind,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(med_auth_ind))='N/A','',
	-- LTRIM(RTRIM(med_auth_ind)) )
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(med_auth_ind)) = 'N/A', '',
	    LTRIM(RTRIM(med_auth_ind))
	) AS v_med_auth_ind,
	v_med_auth_ind AS o_med_auth_ind,
	education_lvl,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(education_lvl))='N/A','',
	-- LTRIM(RTRIM(education_lvl)) )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(education_lvl)) = 'N/A', '',
	    LTRIM(RTRIM(education_lvl))
	) AS v_education_lvl,
	v_education_lvl AS o_education_lvl,
	auth_to_release_ssn_ind,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(auth_to_release_ssn_ind))='N/A','',
	-- LTRIM(RTRIM(auth_to_release_ssn_ind)) )
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(auth_to_release_ssn_ind)) = 'N/A', '',
	    LTRIM(RTRIM(auth_to_release_ssn_ind))
	) AS v_auth_to_release_ssn_ind,
	v_auth_to_release_ssn_ind AS o_auth_to_release_ssn_ind,
	occuptn_code,
	inj_loc_comment,
	premises_code,
	claim_ctgry_code,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(claim_ctgry_code)) = 'N/A','',
	-- SUBSTR(LTRIM(RTRIM(claim_ctgry_code)),1,1) )
	-- 
	-- --DECODE(LTRIM(RTRIM(claim_ctgry_code)),
	-- --'MO','M',
	-- --'IN','I',
	-- --'NO','N',
	-- --'')
	-- 
	-- --When 'MO' then 'M' when 'I' then 'I' when 'NO' then 'N' else ' '
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(claim_ctgry_code)) = 'N/A', '',
	    SUBSTR(LTRIM(RTRIM(claim_ctgry_code)), 1, 1)
	) AS v_claim_ctgry_code,
	v_claim_ctgry_code AS o_claim_ctgry_code,
	wage_rate_amt,
	-- *INF*: IIF(LTRIM(RTRIM(in_emplymnt_status_code))<>'9'  AND in_avg_wkly_wage=0,
	-- to_char(wage_rate_amt),to_char(in_avg_wkly_wage) )
	-- 
	-- 
	-- --Case when workers_comp_claimant_detail.emplymnt_status_code <> 9 And CASE When workers_comp_claimant_detial.avg_wkly_wage = 0 then
	-- --workers_comp_claimant_detail.wage_rate_amt  Else
	-- -- workers_comp_claimant_detail.avg_wkly_wage
	-- 
	IFF(
	    LTRIM(RTRIM(in_emplymnt_status_code)) <> '9' AND in_avg_wkly_wage = 0,
	    to_char(wage_rate_amt),
	    to_char(in_avg_wkly_wage)
	) AS v_average_wage,
	-- *INF*: v_average_wage
	-- 
	-- --Case when workers_comp_claimant_detail.emplymnt_status_code <> 9 And CASE When workers_comp_claimant_detial.avg_wkly_wage = 0 then
	-- --workers_comp_claimant_detail.wage_rate_amt  Else
	-- -- workers_comp_claimant_detail.avg_wkly_wage
	-- 
	v_average_wage AS o_average_wage,
	emp_id_num,
	-- *INF*: DECODE(LTRIM(RTRIM(emp_id_num)),
	-- 'N/A','',
	-- LTRIM(RTRIM(emp_id_num)) )
	-- 
	DECODE(
	    LTRIM(RTRIM(emp_id_num)),
	    'N/A', '',
	    LTRIM(RTRIM(emp_id_num))
	) AS v_emp_id_num,
	v_emp_id_num AS o_emp_id_num,
	emp_id_type,
	-- *INF*: LTRIM(RTRIM(emp_id_type))
	LTRIM(RTRIM(emp_id_type)) AS v_emp_id_type,
	v_emp_id_type AS o_emp_id_type,
	work_week_days,
	work_week_type,
	emplyr_lost_time_notified_date,
	-- *INF*: IIF(ltrim(rtrim(emplyr_lost_time_notified_date))='18000101','',LTRIM(RTRIM(emplyr_lost_time_notified_date)) )
	IFF(
	    ltrim(rtrim(emplyr_lost_time_notified_date)) = '18000101', '',
	    LTRIM(RTRIM(emplyr_lost_time_notified_date))
	) AS v_emplyr_lost_time_notified_date,
	v_emplyr_lost_time_notified_date AS o_emplyr_lost_time_notified_date,
	NaicsCode,
	-- *INF*: LTRIM(RTRIM(NaicsCode))
	LTRIM(RTRIM(NaicsCode)) AS o_NaicsCode,
	salary_paid_ind,
	-- *INF*: LTRIM(RTRIM(salary_paid_ind))
	LTRIM(RTRIM(salary_paid_ind)) AS v_salary_paid_ind,
	v_salary_paid_ind AS o_salary_paid_ind,
	phys_restriction_ind,
	emp_security_id,
	act_status_code,
	FullDenialReasonCode,
	-- *INF*: DECODE(TRUE,
	-- FullDenialReasonCode = 'N/A','',
	-- LTRIM(RTRIM(FullDenialReasonCode)) )
	-- 
	DECODE(
	    TRUE,
	    FullDenialReasonCode = 'N/A', '',
	    LTRIM(RTRIM(FullDenialReasonCode))
	) AS o_FullDenialReasonCode,
	IAIABCLossTypeCode,
	-- *INF*: DECODE(TRUE,
	-- IAIABCLossTypeCode = 'N/A','',
	-- LTRIM(RTRIM(IAIABCLossTypeCode)) )
	DECODE(
	    TRUE,
	    IAIABCLossTypeCode = 'N/A', '',
	    LTRIM(RTRIM(IAIABCLossTypeCode))
	) AS o_IAIABCLossTypeCode,
	FROILateReasonCode,
	ManualClassificationCode,
	num_of_exemptions,
	-- *INF*: TO_CHAR(num_of_exemptions)
	TO_CHAR(num_of_exemptions) AS o_num_of_exemptions,
	claim_party_key,
	tax_fed_id,
	claim_party_full_name,
	claim_party_addr AS in_claim_party_addr,
	-- *INF*: REPLACESTR(0,LTRIM(RTRIM(in_claim_party_addr)),
	-- 	'`',
	-- 	'~',
	-- 	'!',
	-- 	'@'
	-- 	,'#',
	-- 	'$',
	-- 	'%',
	-- 	'^',
	-- 	'&',
	-- 	'*',
	-- 	'(',
	-- 	')',
	-- 	'-',
	-- 	'_',
	-- 	'=',
	-- 	'+',
	-- 	'[',
	-- 	'{',
	-- 	']',
	-- 	'}',
	-- 	'\',
	-- 	'|',
	-- 	';',
	-- 	':',
	-- 	'"', 
	-- 	CHR(39),
	-- 	',',
	-- 	'<',
	-- 	'.',
	-- 	'>',
	-- 	'?',
	--        '/',
	-- 	'')
	REGEXP_REPLACE(LTRIM(RTRIM(in_claim_party_addr)),'`','~','!','@','#','$','%','^','&','*','(',')','-','_','=','+','[','{',']','}','\','|',';',':','"',CHR(39),',','<','.','>','?','/','','i') AS v_claim_party_addr,
	-- *INF*: IIF(claim_party_role_code='CLMT',v_claim_party_addr,'' )
	IFF(claim_party_role_code = 'CLMT', v_claim_party_addr, '') AS o_claim_party_addr,
	tax_ssn_id AS in_tax_ssn_id,
	-- *INF*: IIF(in_tax_ssn_id =  'N/A' ,'', LTRIM(RTRIM(in_tax_ssn_id)))
	-- 
	-- --REPLACECHR(0,in_tax_ssn_id,'-','')
	-- --IF claim_party.tax_ssn_id =  'N/A'  THEN ' ' ELSE  Remove '-' with '' in claim_party.tax_ssn_id 
	IFF(in_tax_ssn_id = 'N/A', '', LTRIM(RTRIM(in_tax_ssn_id))) AS v_tax_ssn_id,
	v_tax_ssn_id AS o_tax_ssn_id,
	claim_party_first_name AS in_claim_party_first_name,
	-- *INF*: IIF(claim_party_role_code='CLMT',SUBSTR(LTRIM(RTRIM(in_claim_party_first_name)),1,15),'')
	-- 
	-- --Case when claim_party_occurrence.claim_party_role_code = 'CLMT' then Substring(claim_party.claim_party_first_name,1,15).  Truncate to 15 positions. Left justify and pad if length is less than 15
	IFF(
	    claim_party_role_code = 'CLMT', SUBSTR(LTRIM(RTRIM(in_claim_party_first_name)), 1, 15), ''
	) AS v_claim_party_first_name,
	v_claim_party_first_name AS o_claim_party_first_name,
	claim_party_mid_name AS in_claim_party_mid_name,
	-- *INF*: REPLACESTR(0,LTRIM(RTRIM(in_claim_party_mid_name)),
	-- 	'`',
	-- 	'~',
	-- 	'!',
	-- 	'@'
	-- 	,'#',
	-- 	'$',
	-- 	'%',
	-- 	'^',
	-- 	'&',
	-- 	'*',
	-- 	'(',
	-- 	')',
	-- 	'-',
	-- 	'_',
	-- 	'=',
	-- 	'+',
	-- 	'[',
	-- 	'{',
	-- 	']',
	-- 	'}',
	-- 	'\',
	-- 	'|',
	-- 	';',
	-- 	':',
	-- 	'"', 
	-- 	CHR(39),
	-- 	',',
	-- 	'<',
	-- 	'.',
	-- 	'>',
	-- 	'?',
	--        '/',
	-- 	'')
	REGEXP_REPLACE(LTRIM(RTRIM(in_claim_party_mid_name)),'`','~','!','@','#','$','%','^','&','*','(',')','-','_','=','+','[','{',']','}','\','|',';',':','"',CHR(39),',','<','.','>','?','/','','i') AS v_mid_name_reformatted,
	-- *INF*: DECODE(TRUE,
	-- claim_party_role_code='CLMT', DECODE(TRUE,
	-- LTRIM(RTRIM(v_mid_name_reformatted))='N/A','',
	-- LTRIM(RTRIM(v_mid_name_reformatted)) ),
	-- '' )
	-- 
	-- --IIF(LTRIM(RTRIM(v_mid_name_reformatted))='N/A','',LTRIM(RTRIM(v_mid_name_reformatted)))
	DECODE(
	    TRUE,
	    claim_party_role_code = 'CLMT', DECODE(
	        TRUE,
	        LTRIM(RTRIM(v_mid_name_reformatted)) = 'N/A', '',
	        LTRIM(RTRIM(v_mid_name_reformatted))
	    ),
	    ''
	) AS o_claim_party_mid_name,
	claim_party_last_name AS in_claim_party_last_name,
	-- *INF*: REPLACESTR(0,LTRIM(RTRIM(in_claim_party_last_name)),
	-- 	'`',
	-- 	'~',
	-- 	'!',
	-- 	'@'
	-- 	,'#',
	-- 	'$',
	-- 	'%',
	-- 	'^',
	-- 	'&',
	-- 	'*',
	-- 	'(',
	-- 	')',
	-- 	'_',
	-- 	'=',
	-- 	'+',
	-- 	'[',
	-- 	'{',
	-- 	']',
	-- 	'}',
	-- 	'\',
	-- 	'|',
	-- 	';',
	-- 	':',
	-- 	'"', 
	-- 	CHR(39),
	-- 	',',
	-- 	'<',
	-- 	'.',
	-- 	'>',
	-- 	'?',
	--        '/',
	-- 	'')
	REGEXP_REPLACE(LTRIM(RTRIM(in_claim_party_last_name)),'`','~','!','@','#','$','%','^','&','*','(',')','_','=','+','[','{',']','}','\','|',';',':','"',CHR(39),',','<','.','>','?','/','','i') AS v_last_name_reformatted,
	-- *INF*: IIF(claim_party_role_code='CLMT',v_last_name_reformatted,'' )
	IFF(claim_party_role_code = 'CLMT', v_last_name_reformatted, '') AS o_claim_party_last_name,
	claim_party_city AS in_claim_party_city,
	-- *INF*: IIF(claim_party_role_code='CLMT',SUBSTR(LTRIM(RTRIM(in_claim_party_city)),1,15),'')
	-- 
	-- --Case when claim_party_occurrence.claim_party_role_code = 'CLMT' then Substring(claim_party.claim_party_city,1,15)  if length is less than 15, then left justify and pad with spaces.
	-- 
	-- --v_claim_party_city
	IFF(claim_party_role_code = 'CLMT', SUBSTR(LTRIM(RTRIM(in_claim_party_city)), 1, 15), '') AS v_claim_party_city,
	v_claim_party_city AS o_claim_party_city,
	claim_party_state AS in_claim_party_state,
	-- *INF*: IIF(claim_party_role_code='CLMT',SUBSTR(LTRIM(RTRIM(in_claim_party_state)),1,2),'')
	-- 
	-- --Case when claim_party_occurrence.claim_party_role_code = 'CLMT' then Substring(claim_party.claim_party_state,1,2)
	IFF(claim_party_role_code = 'CLMT', SUBSTR(LTRIM(RTRIM(in_claim_party_state)), 1, 2), '') AS v_claim_party_state,
	v_claim_party_state AS o_claim_party_state,
	claim_party_zip AS in_claim_party_zip,
	-- *INF*: IIF(claim_party_role_code='CLMT',REPLACECHR(0,LTRIM(RTRIM(in_claim_party_zip)),'-',''),'') 
	-- 
	-- --claim_party_occurrence.claim_party_role_code = 'CLMT' then Replace(Substring(claim_party.claim_party_zip,1,9),'-','')Left justified; pad with spaces.
	IFF(
	    claim_party_role_code = 'CLMT', REGEXP_REPLACE(LTRIM(RTRIM(in_claim_party_zip)),'-','','i'),
	    ''
	) AS v_claim_party_zip,
	v_claim_party_zip AS o_claim_party_zip,
	ph_num AS in_ph_num,
	-- *INF*: REPLACECHR(0,ltrim(rtrim(in_ph_num)),'-','')
	-- 
	-- --Replace (-) and truncate to 9 positions.
	-- --Left justified; pad with spaces.
	REGEXP_REPLACE(ltrim(rtrim(in_ph_num)),'-','','i') AS v_ph_num,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(v_ph_num))='N/A','',
	-- LTRIM(RTRIM(v_ph_num)) )
	-- 
	-- 
	-- --IIF(claim_party_role_code='CLMT',v_ph_num,'' )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(v_ph_num)) = 'N/A', '',
	    LTRIM(RTRIM(v_ph_num))
	) AS o_ph_num,
	claim_party_birthdate AS in_claim_party_birthdate,
	-- *INF*: DECODE(TRUE,
	-- claim_party_role_code='CLMT',DECODE(TRUE,
	-- LTRIM(RTRIM(in_claim_party_birthdate))  = '18000101','',
	-- LTRIM(RTRIM(in_claim_party_birthdate)) ),
	-- '' )
	-- 
	-- 
	DECODE(
	    TRUE,
	    claim_party_role_code = 'CLMT', DECODE(
	        TRUE,
	        LTRIM(RTRIM(in_claim_party_birthdate)) = '18000101', '',
	        LTRIM(RTRIM(in_claim_party_birthdate))
	    ),
	    ''
	) AS v_claim_party_birthdate,
	v_claim_party_birthdate AS o_claim_party_birthdate,
	claim_party_gndr AS in_claim_party_gndr,
	-- *INF*: DECODE(TRUE,
	-- claim_party_role_code = 'CLMT', DECODE(TRUE,
	-- LTRIM(RTRIM(in_claim_party_gndr))='N/A','',
	-- LTRIM(RTRIM(in_claim_party_gndr)) ),
	-- '' )
	-- 
	-- 
	-- --IIF(claim_party_role_code = 'CLMT',IIF(LTRIM(RTRIM(in_claim_party_gndr))='N/A','',LTRIM(RTRIM(in_claim_party_gndr)) ))
	-- 
	-- --Case when claim_party_occurrence.claim_party_role_code = 'CLMT' then left justify and truncate to 1 position claim_party.claim_party_gndr. If N/A then send blank space.
	DECODE(
	    TRUE,
	    claim_party_role_code = 'CLMT', DECODE(
	        TRUE,
	        LTRIM(RTRIM(in_claim_party_gndr)) = 'N/A', '',
	        LTRIM(RTRIM(in_claim_party_gndr))
	    ),
	    ''
	) AS v_claim_party_gndr,
	v_claim_party_gndr AS o_claim_party_gndr,
	claim_party_name_sfx,
	occuptn_descript AS in_occuptn_descript,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(in_occuptn_descript),'',
	-- LTRIM(RTRIM(in_occuptn_descript))='N/A','',
	-- LTRIM(RTRIM(in_occuptn_descript)) )
	-- 
	-- 
	-- --IIF(ISNULL(in_occuptn_descript),'',IIF( @{pipeline().parameters.STATE_CODE} = 'MN' , ' ', in_occuptn_descript))
	-- 
	-- --IF (@{pipeline().parameters.STATE_CODE} = 'MN' ) THEN  ' ' ELSE  sup_workers_comp_occupation.occuptn_descript
	DECODE(
	    TRUE,
	    in_occuptn_descript IS NULL, '',
	    LTRIM(RTRIM(in_occuptn_descript)) = 'N/A', '',
	    LTRIM(RTRIM(in_occuptn_descript))
	) AS v_occuptn_descript,
	v_occuptn_descript AS o_occuptn_descript,
	claim_party_role_code,
	claim_party_ak_id,
	StrategicProfitCenterDescription,
	-- *INF*: '390698170'
	-- 
	-- 
	-- --DECODE(LTRIM(RTRIM(StrategicProfitCenterDescription)),
	-- --'West Bend Commercial Lines', '390698170',
	-- --'Argent','390698170',
	-- --'Other','390698170',
	-- --'NSI','095680231',
	-- --'')
	-- 
	'390698170' AS InsurerFEIN,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(StrategicProfitCenterDescription)),'NSI','West Bend Commercial Lines','Argent','Other'),'West Bend',
	-- '')
	-- 
	-- --Case when SPC.StrategicProfitCenterDescription = 'West Bend Commercial Lines' or 'Argent'
	-- --hard-coded:  'West Bend'
	-- --Case when SPC.StrategicProfitCenterDescription = 'NSI'
	-- --hard-coded: NSI
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(StrategicProfitCenterDescription)) IN ('NSI','West Bend Commercial Lines','Argent','Other'), 'West Bend',
	    ''
	) AS v_InsurerName,
	v_InsurerName AS o_InsurerName,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(StrategicProfitCenterDescription)),'NSI','West Bend Commercial Lines','Argent','Other'),'1900 S. 18TH AVE',
	-- '')
	-- 
	-- 
	-- 
	-- --Case when SPC.StrategicProfitCenterDescription = 'NSI' then '8401 GREENWAY BLVD'
	-- --When SPC.StrategicProfitCenterDescription = 'West Bend Commercial Lines' or 'Argent' 
	-- --then '1900 S. 18TH AVE' 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(StrategicProfitCenterDescription)) IN ('NSI','West Bend Commercial Lines','Argent','Other'), '1900 S. 18TH AVE',
	    ''
	) AS v_ClaimAdminAddress1,
	v_ClaimAdminAddress1 AS o_ClaimAdminAddress1,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(StrategicProfitCenterDescription)),'NSI','West Bend Commercial Lines','Argent','Other'),'WEST BEND',
	-- '')
	-- 
	-- 
	-- --Case when SPC.StrategicProfitCenterDescription = 'NSI' then 'MIDDLETON'
	-- --When SPC.StrategicProfitCenterDescription = 'West Bend Commercial Lines' or 'Argent'
	-- --then 'WEST BEND'
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(StrategicProfitCenterDescription)) IN ('NSI','West Bend Commercial Lines','Argent','Other'), 'WEST BEND',
	    ''
	) AS v_ClaimAdministratorCity,
	v_ClaimAdministratorCity AS o_ClaimAdministratorCity,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(StrategicProfitCenterDescription)),'NSI','West Bend Commercial Lines','Argent','Other'),'WI',
	-- '')
	-- 
	-- 
	-- --Case when SPC.StrategicProfitCenterDescription = 'NSI' then 'WI'
	-- --When SPC.StrategicProfitCenterDescription = 'West Bend Commercial Lines' or 'Argent'
	-- --then 'WI'
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(StrategicProfitCenterDescription)) IN ('NSI','West Bend Commercial Lines','Argent','Other'), 'WI',
	    ''
	) AS v_ClaimAdministratorStateCode,
	v_ClaimAdministratorStateCode AS o_ClaimAdministratorStateCode,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(StrategicProfitCenterDescription)),'NSI','West Bend Commercial Lines','Argent','Other'),'530959791',
	-- '')
	-- 
	-- 
	-- --Case when SPC.StrategicProfitCenterDescription = 'NSI' then '535620976'
	-- --When SPC.StrategicProfitCenterDescription = 'West Bend Commercial Lines' or 'Argent'
	-- --then '530959791'
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(StrategicProfitCenterDescription)) IN ('NSI','West Bend Commercial Lines','Argent','Other'), '530959791',
	    ''
	) AS v_ClaimAdministratorPostalCode,
	v_ClaimAdministratorPostalCode AS o_ClaimAdministratorPostalCode,
	'390698170' AS ClaimAdministratorFEIN,
	claim_occurrence_ak_id,
	pol_key_ak_id,
	claim_loss_date,
	-- *INF*: LPAD(TO_CHAR(GET_DATE_PART(claim_loss_date,'HH24')),2,'0')||LPAD(TO_CHAR(GET_DATE_PART(claim_loss_date,'MI')),2,'0')
	LPAD(TO_CHAR(DATE_PART(claim_loss_date, 'HH24')), 2, '0') || LPAD(TO_CHAR(DATE_PART(claim_loss_date, 'MI')), 2, '0') AS v_claim_loss_time,
	v_claim_loss_time AS o_claim_loss_time,
	claim_loss_date_convert AS convert_claim_loss_date,
	-- *INF*: --DECODE(TRUE,
	-- --LTRIM(RTRIM(in_jurisdiction_state_code))='IA',TO_CHAR(claim_loss_date,'MM/DD/YYYY'),
	-- convert_claim_loss_date -- )
	convert_claim_loss_date AS v_convert_claim_loss_date,
	-- *INF*: IIF(LTRIM(RTRIM(v_convert_claim_loss_date))='18000101','',v_convert_claim_loss_date )
	IFF(LTRIM(RTRIM(v_convert_claim_loss_date)) = '18000101', '', v_convert_claim_loss_date) AS o_convert_claim_loss_date,
	claim_occurrence_key,
	loss_loc_zip AS in_loss_loc_zip,
	-- *INF*: IIF(ISNULL(in_loss_loc_zip)  OR LTRIM(RTRIM(in_loss_loc_zip))='N/A','',REPLACECHR(0,in_loss_loc_zip,'-','') )
	-- 
	-- --Replace '-' and truncate to 9 positions. Left justified; pad with spaces.
	IFF(
	    in_loss_loc_zip IS NULL OR LTRIM(RTRIM(in_loss_loc_zip)) = 'N/A', '',
	    REGEXP_REPLACE(in_loss_loc_zip,'-','','i')
	) AS v_loss_loc_zip,
	v_loss_loc_zip AS o_loss_loc_zip,
	claim_loss_descript AS in_claim_loss_descript,
	-- *INF*: REPLACESTR(0,LTRIM(RTRIM(in_claim_loss_descript)),
	-- 	'`',
	-- 	'~',
	-- 	'!',
	-- 	'@'
	-- 	,'#',
	-- 	'$',
	-- 	'%',
	-- 	'^',
	-- 	'&',
	-- 	'*',
	-- 	'(',
	-- 	')',
	-- 	'-',
	-- 	'_',
	-- 	'=',
	-- 	'+',
	-- 	'[',
	-- 	'{',
	-- 	']',
	-- 	'}',
	-- 	'\',
	-- 	'|',
	-- 	';',
	-- 	':',
	-- 	'"', 
	-- 	CHR(39),
	-- 	',',
	-- 	'<',
	-- 	'.',
	-- 	'>',
	-- 	'?',
	-- 	'/',
	-- 	'')
	-- 
	-- --Eliminate the following characters in claim_occurrence.claim_loss_descript ,( '`', '~', '!', '@' ,'#', '$', '%', '^', '&', '*', '(', ')', '-', '_', '=', '+', '[', '{', ']', '}', '\', '|', ';', ':', '"',  CHR(39), ',', '<', '.', '>', '?', '/'')
	REGEXP_REPLACE(LTRIM(RTRIM(in_claim_loss_descript)),'`','~','!','@','#','$','%','^','&','*','(',')','-','_','=','+','[','{',']','}','\','|',';',':','"',CHR(39),',','<','.','>','?','/','','i') AS v_claim_loss_descript,
	v_claim_loss_descript AS o_claim_loss_descript,
	source_claim_occurrence_status_code,
	loss_loc_county,
	-- *INF*: IIF(LTRIM(RTRIM(loss_loc_county))='N/A','',LTRIM(RTRIM(loss_loc_county)) )
	IFF(LTRIM(RTRIM(loss_loc_county)) = 'N/A', '', LTRIM(RTRIM(loss_loc_county))) AS v_loss_loc_county,
	v_loss_loc_county AS o_loss_loc_county,
	loss_loc_city,
	-- *INF*: IIF(LTRIM(RTRIM(loss_loc_city))='N/A','',LTRIM(RTRIM(loss_loc_city)) )
	-- 
	IFF(LTRIM(RTRIM(loss_loc_city)) = 'N/A', '', LTRIM(RTRIM(loss_loc_city))) AS v_loss_loc_city,
	v_loss_loc_city AS o_loss_loc_city,
	loss_loc_addr,
	-- *INF*: IIF(LTRIM(RTRIM(loss_loc_addr))='N/A','',LTRIM(RTRIM(loss_loc_addr)) )
	-- 
	-- 
	-- 
	IFF(LTRIM(RTRIM(loss_loc_addr)) = 'N/A', '', LTRIM(RTRIM(loss_loc_addr))) AS v_loss_loc_addr,
	v_loss_loc_addr AS o_loss_loc_addr,
	loss_loc_state,
	-- *INF*: IIF(LTRIM(RTRIM(loss_loc_state))='N/A','',LTRIM(RTRIM(loss_loc_state)))
	-- 
	-- 
	-- --IIF(ISNULL(loss_loc_state)  OR LTRIM(RTRIM(loss_loc_state))='N/A','',LTRIM(RTRIM(loss_loc_state)))
	-- 
	-- 
	-- 
	IFF(LTRIM(RTRIM(loss_loc_state)) = 'N/A', '', LTRIM(RTRIM(loss_loc_state))) AS v_loss_loc_state,
	v_loss_loc_state AS o_loss_loc_state,
	claim_rep_email,
	-- *INF*: LTRIM(RTRIM(claim_rep_email))
	LTRIM(RTRIM(claim_rep_email)) AS v_claim_rep_email,
	v_claim_rep_email AS o_claim_rep_email,
	claim_rep_full_name,
	-- *INF*: LTRIM(RTRIM(claim_rep_full_name))
	LTRIM(RTRIM(claim_rep_full_name)) AS v_claim_rep_full_name,
	v_claim_rep_full_name AS o_claim_rep_full_name,
	SYSDATE AS CurrentDate,
	Rep_Ph_Num,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(Rep_Ph_Num) OR IS_SPACES(Rep_Ph_Num) OR LENGTH(Rep_Ph_Num) = 0,'',
	-- LTRIM(RTRIM(Rep_Ph_Num))='N/A','',
	-- LTRIM(RTRIM(Rep_Ph_Num))
	--  )
	DECODE(
	    TRUE,
	    Rep_Ph_Num IS NULL OR LENGTH(Rep_Ph_Num)>0 AND TRIM(Rep_Ph_Num)='' OR LENGTH(Rep_Ph_Num) = 0, '',
	    LTRIM(RTRIM(Rep_Ph_Num)) = 'N/A', '',
	    LTRIM(RTRIM(Rep_Ph_Num))
	) AS v_Rep_Ph_Num,
	Rep_Ph_Extension,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(Rep_Ph_Extension)OR IS_SPACES(Rep_Ph_Extension) OR LENGTH(Rep_Ph_Extension) = 0,'',
	-- LTRIM(RTRIM(Rep_Ph_Extension))='N/A','',
	-- LTRIM(RTRIM(Rep_Ph_Extension)) )
	DECODE(
	    TRUE,
	    Rep_Ph_Extension IS NULL OR LENGTH(Rep_Ph_Extension)>0 AND TRIM(Rep_Ph_Extension)='' OR LENGTH(Rep_Ph_Extension) = 0, '',
	    LTRIM(RTRIM(Rep_Ph_Extension)) = 'N/A', '',
	    LTRIM(RTRIM(Rep_Ph_Extension))
	) AS v_Rep_Ph_Extension,
	-- *INF*: DECODE(TRUE,
	-- v_Rep_Ph_Num='','',
	-- SUBSTR(v_Rep_Ph_Num ,1,10)|| SUBSTR(v_Rep_Ph_Extension,1,5)
	-- )
	-- 
	-- 
	DECODE(
	    TRUE,
	    v_Rep_Ph_Num = '', '',
	    SUBSTR(v_Rep_Ph_Num, 1, 10) || SUBSTR(v_Rep_Ph_Extension, 1, 5)
	) AS claim_rep_phone,
	FROIClaimType,
	FROIFullDenialReasonNarrative
	FROM SQ_workers_comp_claimant_detail
	LEFT JOIN LKP_MAX_CREATE_DATE LKP_MAX_CREATE_DATE_wc_claimant_det_ak_id
	ON LKP_MAX_CREATE_DATE_wc_claimant_det_ak_id.WC_CLAIMANT_DET_AK_ID = wc_claimant_det_ak_id

),
LKP_CLASS_CODE_CCD AS (
	SELECT
	risk_unit,
	ClassCode,
	claim_party_occurrence_ak_id
	FROM (
		select
		a.risk_unit as risk_unit
		,a.ClassCode as ClassCode
		,a.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id FROM claimant_coverage_detail a
		where a.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id ORDER BY risk_unit) = 1
),
LKP_Claim_Occurrence AS (
	SELECT
	claim_party_key,
	IN_claim_occurrence_ak_id,
	claim_occurrence_ak_id
	FROM (
		select CP.claim_party_key as claim_party_key,
		CO.claim_occurrence_ak_id as claim_occurrence_ak_id
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence CO
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence CPO
		on CPO.claim_occurrence_ak_id=CO.claim_occurrence_ak_id 
		and CPO.claim_party_role_code='EMPR'
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party CP
		on CP.claim_party_ak_id = CPO.claim_party_ak_id
		where CP.crrnt_snpsht_flag=1 and CPO.crrnt_snpsht_flag=1 and CO.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id ORDER BY claim_party_key) = 1
),
LKP_CLT_ref_relation_stage AS (
	SELECT
	cirf_ref_id,
	client_id,
	cirf_eff_dt,
	cirf_exp_dt
	FROM (
		select client_id as client_id 
		,cirf_ref_id as cirf_ref_id
		,cirf_eff_dt as cirf_eff_dt
		,cirf_exp_dt as cirf_exp_dt
		from @{pipeline().parameters.SOURCE_STAGE_CONNECTION}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.clt_ref_relation_stage
		where history_vld_nbr=0
		and ref_typ_cd = 'UIAI'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY client_id,cirf_eff_dt,cirf_exp_dt ORDER BY cirf_ref_id) = 1
),
LKP_Get_Policy_Details AS (
	SELECT
	pol_key,
	pol_eff_date,
	pol_exp_date,
	source_sys_id,
	pol_key_ak_id
	FROM (
		SELECT 
		policy.pol_key as pol_key,policy.pol_sym as pol_sym, policy.pol_num as pol_num, policy.pol_mod as pol_mod, policy.pol_eff_date as pol_eff_date, policy.pol_exp_date as pol_exp_date, policy.pol_ak_id as pol_key_ak_id,
		policy.source_sys_id as source_sys_id 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy policy
		where policy.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key_ak_id ORDER BY pol_key) = 1
),
LKP_RTW_TYPE_AND_DATE AS (
	SELECT
	min_emp_last_day_worked,
	wc_claimant_det_ak_id
	FROM (
		SELECT  
		p.wc_claimant_det_ak_id                    as  WC_CLAIMANT_DET_AK_ID 
		,p.min_emp_last_day_worked                   as MIN_EMP_LAST_DAY_WORKED
		FROM (
		SELECT a.wc_claimant_det_ak_id                                as  WC_CLAIMANT_DET_AK_ID ,
		MIN(a.emp_last_day_worked)                     as min_emp_last_day_worked
		FROM workers_comp_claimant_work_history a
		where a.crrnt_snpsht_flag = 1
		GROUP BY a.wc_claimant_det_ak_id ) p
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY wc_claimant_det_ak_id ORDER BY min_emp_last_day_worked) = 1
),
LKP_TO_GET_EMPLOYER_DETAILS AS (
	SELECT
	claim_party_full_name,
	claim_party_addr,
	claim_party_city,
	claim_party_state,
	claim_party_zip,
	addr_type,
	tax_ssn_id,
	tax_fed_id,
	in_claim_occurrence_ak_id,
	claim_party_id
	FROM (
		select 
		a.claim_occurrence_ak_id as claim_party_id
		,b.claim_party_full_name as claim_party_full_name
		,b.claim_party_addr as claim_party_addr 
		,b.claim_party_city as claim_party_city
		,b.claim_party_state as claim_party_state
		,b.claim_party_zip as claim_party_zip
		,b.tax_ssn_id as tax_ssn_id
		,b.tax_fed_id as  tax_fed_id
		,b.addr_type as addr_type
		from 
		claim_party_occurrence a
		, claim_party b
		where a.claim_party_ak_id = b.claim_party_ak_id
		and a.claim_party_role_code ='EMPR'
		and a.crrnt_snpsht_flag = 1
		and b.crrnt_snpsht_flag = 1
		--and b.addr_type='BSM'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_id ORDER BY claim_party_full_name) = 1
),
LKP_TO_GET_EMPR_CONTACT_DETAILS AS (
	SELECT
	claim_party_full_name,
	ph_num,
	ph_extension,
	in_claim_occurrence_ak_id,
	claim_party_id
	FROM (
		select 
		a.claim_occurrence_ak_id as claim_party_id
		,b.claim_party_full_name as claim_party_full_name
		,b.ph_num as ph_num 
		,b.ph_extension as ph_extension
		from 
		claim_party_occurrence a
		, claim_party b
		where a.claim_party_ak_id = b.claim_party_ak_id
		and a.claim_party_role_code ='CNCT'
		and a.crrnt_snpsht_flag = 1
		and b.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_id ORDER BY claim_party_full_name) = 1
),
LKP_TO_GET_INSURED_DETAILS AS (
	SELECT
	claim_party_full_name,
	claim_party_addr,
	claim_party_city,
	claim_party_state,
	claim_party_zip,
	tax_ssn_id,
	tax_fed_id,
	in_claim_occurrence_ak_id,
	claim_party_id
	FROM (
		select 
		a.claim_occurrence_ak_id as claim_party_id
		,b.claim_party_full_name as claim_party_full_name
		,b.claim_party_addr as claim_party_addr 
		,b.claim_party_city as claim_party_city
		,b.claim_party_state as claim_party_state
		,b.claim_party_zip as claim_party_zip
		,b.tax_ssn_id as tax_ssn_id
		,b.tax_fed_id as  tax_fed_id 
		from 
		claim_party_occurrence a
		, claim_party b
		where a.claim_party_ak_id = b.claim_party_ak_id
		and a.claim_party_role_code ='PLHR'
		and a.crrnt_snpsht_flag = 1
		and b.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_id ORDER BY claim_party_full_name) = 1
),
LKP_WRK_HISTORY AS (
	SELECT
	min_return_to_work_date,
	max_return_to_work_date,
	return_to_work_type,
	wc_claimant_det_ak_id
	FROM (
		select A.min_return_to_work_date as min_return_to_work_date ,A.max_return_to_work_date as max_return_to_work_date,Hist.return_to_work_type as return_to_work_type,
		Hist.wc_claimant_det_ak_id as wc_claimant_det_ak_id from 
		workers_comp_claimant_work_history Hist inner join 
		(select min(return_to_work_date) as min_return_to_work_date,Max(return_to_work_date) as max_return_to_work_date,w.wc_claimant_det_ak_id as wc_claimant_det_ak_id
		from workers_comp_claimant_work_history w
		inner join vw_workers_comp_claimant_detail wccd
		on wccd.wc_claimant_det_ak_id = w.wc_claimant_det_ak_id
		and ((wccd.jurisdiction_state_code='MN'and w.return_to_work_type='A' and w.return_to_work_date <> '01/01/1800') 
			or (wccd.jurisdiction_state_code='IN'and w.return_to_work_type='A' and w.return_to_work_date <> '01/01/1800') 
			or (wccd.jurisdiction_state_code in ('KS','MO','IA','WI') and w.return_to_work_type in ('A','R') and w.return_to_work_date <> '01/01/1800') )
		WHERE w.crrnt_snpsht_flag = 1
		 and wccd.crrnt_snpsht_flag=1
		 group by w.wc_claimant_det_ak_id) A
		 on A.wc_claimant_det_ak_id = Hist.wc_claimant_det_ak_id
		 and A.min_return_to_work_date=Hist.return_to_work_date
		  and Hist.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY wc_claimant_det_ak_id ORDER BY min_return_to_work_date) = 1
),
LKP_arch_claim_object_stage AS (
	SELECT
	cob_risk_unit,
	cob_client_id,
	cob_claim_nbr
	FROM (
		SELECT arch_claim_object_stage.cob_risk_unit as cob_risk_unit, arch_claim_object_stage.cob_client_id as cob_client_id, arch_claim_object_stage.cob_claim_nbr as cob_claim_nbr FROM arch_claim_object_stage
		order by as_of_date desc --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY cob_client_id,cob_claim_nbr ORDER BY cob_risk_unit) = 1
),
WorkNcciMitchell_Get_Max_Date AS (
	SELECT
	CreatedDate,
	ModifiedDate,
	in_wc_claimant_det_ak_id,
	WCClaimantDetailAKId
	FROM (
		SELECT WCClaimantDetailAKId AS WCClaimantDetailAKId,MAX(CreatedDate) AS CreatedDate
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkNcciMitchell
		WHERE JurisdictionStateCode = 'KS' AND
		MaintenanceTypeCode <> 'CO'
		GROUP BY WCClaimantDetailAKId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WCClaimantDetailAKId ORDER BY CreatedDate) = 1
),
WorkNcciMitchell_Get_MaintCode AS (
	SELECT
	MaintenanceTypeCode,
	MaitenanceTypeCodeDate,
	in_wc_claimant_det_ak_id,
	in_CreatedDate,
	WCClaimantDetailAKId,
	CreatedDate
	FROM (
		SELECT MaintenanceTypeCode AS MaintenanceTypeCode, MaitenanceTypeCodeDate AS MaitenanceTypeCodeDate,
		WCClaimantDetailAKId AS WCClaimantDetailAKId,
		CreatedDate AS CreatedDate
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkNcciMitchell
		WHERE JurisdictionStateCode = 'KS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WCClaimantDetailAKId,CreatedDate ORDER BY MaintenanceTypeCode) = 1
),
EXP_STATE_SPECIFIC AS (
	SELECT
	SYSDATE AS Createddate,
	SYSDATE AS Modifiedate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_ID,
	EXP_SOURCE.wc_claimant_det_ak_id,
	EXP_SOURCE.TransactionSetID,
	EXP_SOURCE.o_maint_type_code AS maint_type_code,
	-- *INF*: LTRIM(RTRIM(maint_type_code))
	-- 
	-- 
	LTRIM(RTRIM(maint_type_code)) AS v_maint_type_code,
	v_maint_type_code AS o_maint_type_code,
	EXP_SOURCE.in_jurisdiction_state_code AS jurisdiction_state_code,
	-- *INF*: LTRIM(RTRIM(jurisdiction_state_code)) 
	LTRIM(RTRIM(jurisdiction_state_code)) AS v_jurisdiction_state_code,
	-- *INF*: LTRIM(RTRIM(v_jurisdiction_state_code))
	-- 
	-- 
	-- 
	LTRIM(RTRIM(v_jurisdiction_state_code)) AS o_jurisdiction_state_code,
	EXP_SOURCE.o_send_to_state_time AS send_to_state_time,
	send_to_state_time AS o_send_to_state_time,
	EXP_SOURCE.in_state_claim_num AS state_claim_num,
	-- *INF*: LTRIM(RTRIM(state_claim_num)) 
	-- 
	-- 
	LTRIM(RTRIM(state_claim_num)) AS v_state_claim_num,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'IL'),NULL,
	-- v_state_claim_num )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('IL'), NULL,
	    v_state_claim_num
	) AS o_state_claim_num,
	EXP_SOURCE.InsurerFEIN,
	EXP_SOURCE.o_InsurerName AS InsurerName,
	-- *INF*: LTRIM(RTRIM(InsurerName)) 
	-- 
	-- 
	LTRIM(RTRIM(InsurerName)) AS v_InsurerName,
	-- *INF*: LTRIM(RTRIM(v_InsurerName))
	-- 
	-- --DECODE(TRUE,
	-- --IN(LTRIM(RTRIM(jurisdiction_state_code)),'KS'),NULL,
	-- --v_InsurerName )
	LTRIM(RTRIM(v_InsurerName)) AS o_InsurerName,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','IN','NE','TN','KY'),'N',
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','IN','NE','TN','KY'), 'N',
	    NULL
	) AS IsTPA,
	EXP_SOURCE.o_ClaimAdminAddress1 AS ClaimAdminAddress1,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','MO','NE','IA','WI','TN','KY'),LTRIM(RTRIM(ClaimAdminAddress1)),
	-- NULL )
	-- 
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','MO','NE','IA','WI','TN','KY'), LTRIM(RTRIM(ClaimAdminAddress1)),
	    NULL
	) AS v_ClaimAdminAddress1,
	v_ClaimAdminAddress1 AS o_ClaimAdminAddress1,
	EXP_SOURCE.o_ClaimAdministratorCity AS ClaimAdministratorCity,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','MO','NE','IA','WI','TN'),LTRIM(RTRIM(ClaimAdministratorCity)),
	-- NULL )
	-- 
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','MO','NE','IA','WI','TN'), LTRIM(RTRIM(ClaimAdministratorCity)),
	    NULL
	) AS v_ClaimAdministratorCity,
	v_ClaimAdministratorCity AS o_ClaimAdministratorCity,
	EXP_SOURCE.o_ClaimAdministratorStateCode AS ClaimAdministratorStateCode,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','MO','NE','IA','WI','TN'),LTRIM(RTRIM(ClaimAdministratorStateCode)),
	-- NULL )
	-- 
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','MO','NE','IA','WI','TN'), LTRIM(RTRIM(ClaimAdministratorStateCode)),
	    NULL
	) AS v_ClaimAdministratorStateCode,
	v_ClaimAdministratorStateCode AS o_ClaimAdministratorStateCode,
	EXP_SOURCE.o_ClaimAdministratorPostalCode AS ClaimAdministratorPostalCode,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','MO','KS','IA','WI','TN','KY'),LTRIM(RTRIM(ClaimAdministratorPostalCode)),
	-- NULL )
	-- 
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','MO','KS','IA','WI','TN','KY'), LTRIM(RTRIM(ClaimAdministratorPostalCode)),
	    NULL
	) AS v_ClaimAdministratorPostalCode,
	v_ClaimAdministratorPostalCode AS o_ClaimAdministratorPostalCode,
	EXP_SOURCE.o_wc_claimant_num AS wc_claimant_num,
	-- *INF*: ltrim(rtrim(wc_claimant_num))
	ltrim(rtrim(wc_claimant_num)) AS v_wc_claimant_num,
	v_wc_claimant_num AS o_wc_claimant_num,
	v_wc_claimant_num AS o_InsuredReportNumber,
	LKP_TO_GET_INSURED_DETAILS.tax_ssn_id AS tax_ssn_id_insured,
	-- *INF*: iif(isnull(tax_ssn_id_insured),'',replacechr(0,ltrim(rtrim(tax_ssn_id_insured)),'-',''))
	IFF(
	    tax_ssn_id_insured IS NULL, '', REGEXP_REPLACE(ltrim(rtrim(tax_ssn_id_insured)),'-','','i')
	) AS tax_ssn_id_insured_reformatted,
	-- *INF*: IIF(LTRIM(RTRIM(tax_ssn_id_insured_reformatted))='N/A'  OR ISNULL(LTRIM(RTRIM(tax_ssn_id_insured_reformatted))),'',
	-- LTRIM(RTRIM(tax_ssn_id_insured_reformatted)))
	-- 
	IFF(
	    LTRIM(RTRIM(tax_ssn_id_insured_reformatted)) = 'N/A'
	    or LTRIM(RTRIM(tax_ssn_id_insured_reformatted)) IS NULL,
	    '',
	    LTRIM(RTRIM(tax_ssn_id_insured_reformatted))
	) AS v_tax_ssn_id_insd,
	LKP_TO_GET_INSURED_DETAILS.tax_fed_id AS tax_fed_id_insured,
	-- *INF*: IIF(ISNULL(tax_fed_id_insured),'',REPLACECHR(0,LTRIM(RTRIM(tax_fed_id_insured)),'-',''))
	-- 
	-- 
	-- --DECODE(TRUE,
	-- --tax_fed_id_insured = 'N/A'   AND  emp_tax_ssn_id != 'N/A', LTRIM(RTRIM(REPLACECHR(0,emp_tax_ssn_id,'-',''))),
	-- --tax_fed_id_insured  !=  'N/A'   AND emp_tax_ssn_id  = 'N/A', LTRIM(RTRIM(REPLACECHR(0,tax_fed_id_insured,'-',''))),
	-- --emp_tax_ssn_id = 'N/A' AND tax_fed_id_insured = 'N/A' ,RPAD('',9,' '))
	IFF(
	    tax_fed_id_insured IS NULL, '', REGEXP_REPLACE(LTRIM(RTRIM(tax_fed_id_insured)),'-','','i')
	) AS tax_fed_id_insured_reformatted,
	-- *INF*: IIF(LTRIM(RTRIM(tax_fed_id_insured_reformatted))='N/A'  OR ISNULL(LTRIM(RTRIM(tax_fed_id_insured_reformatted))),'',
	-- LTRIM(RTRIM(tax_fed_id_insured_reformatted)))
	-- 
	-- --DECODE(TRUE,
	-- --tax_fed_id_insured = 'N/A'   AND  emp_tax_ssn_id != 'N/A', LTRIM(RTRIM(REPLACECHR(0,emp_tax_ssn_id,'-',''))),
	-- --tax_fed_id_insured  !=  'N/A'   AND emp_tax_ssn_id  = 'N/A', LTRIM(RTRIM(REPLACECHR(0,tax_fed_id_insured,'-',''))),
	-- --emp_tax_ssn_id = 'N/A' AND tax_fed_id_insured = 'N/A' ,RPAD('',9,' '))
	IFF(
	    LTRIM(RTRIM(tax_fed_id_insured_reformatted)) = 'N/A'
	    or LTRIM(RTRIM(tax_fed_id_insured_reformatted)) IS NULL,
	    '',
	    LTRIM(RTRIM(tax_fed_id_insured_reformatted))
	) AS v_tax_fed_id_insured,
	v_tax_fed_id_insured AS o_tax_fed_id_insured,
	LKP_TO_GET_INSURED_DETAILS.claim_party_full_name AS full_name_insured,
	-- *INF*: IIF(ISNULL(full_name_insured) OR ltrim(rtrim(full_name_insured))='N/A' ,'',full_name_insured)
	IFF(
	    full_name_insured IS NULL OR ltrim(rtrim(full_name_insured)) = 'N/A', '', full_name_insured
	) AS v_full_name_insured,
	-- *INF*: REPLACESTR(0,ltrim(rtrim(v_full_name_insured)),
	-- 	'`',
	-- 	'~',
	-- 	'!',
	-- 	'@'
	-- 	,'#',
	-- 	'$',
	-- 	'%',
	-- 	'^',
	-- 	'&',
	-- 	'*',
	-- 	'(',
	-- 	')',
	-- 	'-',
	-- 	'_',
	-- 	'=',
	-- 	'+',
	-- 	'[',
	-- 	'{',
	-- 	']',
	-- 	'}',
	-- 	'|',
	-- 	';',
	-- 	':',
	-- 	'"', 
	-- 	CHR(39),
	-- 	',',
	-- 	'<',
	-- 	'.',
	-- 	'>',
	-- 	'?',
	--        '/',
	-- 	'')
	REGEXP_REPLACE(ltrim(rtrim(v_full_name_insured)),'`','~','!','@','#','$','%','^','&','*','(',')','-','_','=','+','[','{',']','}','|',';',':','"',CHR(39),',','<','.','>','?','/','','i') AS v_full_name_reformatted_insured,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','MO','WI','NE','KS','IA','IN','TN','KY'),v_full_name_reformatted_insured,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','MO','WI','NE','KS','IA','IN','TN','KY'), v_full_name_reformatted_insured,
	    NULL
	) AS o_full_name_insured,
	LKP_TO_GET_INSURED_DETAILS.claim_party_addr AS addr_insured,
	-- *INF*: REPLACESTR(0,ltrim(rtrim(addr_insured)),
	-- 	'`',
	-- 	'~',
	-- 	'!',
	-- 	'@'
	-- 	,'#',
	-- 	'$',
	-- 	'%',
	-- 	'^',
	-- 	'&',
	-- 	'*',
	-- 	'(',
	-- 	')',
	-- 	'-',
	-- 	'_',
	-- 	'=',
	-- 	'+',
	-- 	'[',
	-- 	'{',
	-- 	']',
	-- 	'}',
	-- 	'|',
	-- 	';',
	-- 	':',
	-- 	'"', 
	-- 	CHR(39),
	-- 	',',
	-- 	'<',
	-- 	'.',
	-- 	'>',
	-- 	'?',
	-- 	'')
	REGEXP_REPLACE(ltrim(rtrim(addr_insured)),'`','~','!','@','#','$','%','^','&','*','(',')','-','_','=','+','[','{',']','}','|',';',':','"',CHR(39),',','<','.','>','?','','i') AS addr_reformatted_insured,
	-- *INF*: IIF(ISNULL(addr_reformatted_insured),'',addr_reformatted_insured)
	IFF(addr_reformatted_insured IS NULL, '', addr_reformatted_insured) AS v_addr_insured,
	LKP_TO_GET_INSURED_DETAILS.claim_party_city AS city_insured,
	-- *INF*: IIF(ISNULL(city_insured),'',LTRIM(RTRIM(city_insured)))
	-- 
	-- 
	IFF(city_insured IS NULL, '', LTRIM(RTRIM(city_insured))) AS v_city_insured,
	LKP_TO_GET_INSURED_DETAILS.claim_party_state AS state_insured,
	-- *INF*: IIF(ISNULL(state_insured),'',LTRIM(RTRIM(state_insured)))
	-- 
	-- 
	IFF(state_insured IS NULL, '', LTRIM(RTRIM(state_insured))) AS v_state_insured,
	LKP_TO_GET_INSURED_DETAILS.claim_party_zip AS zip_insured,
	-- *INF*: IIF(ISNULL(zip_insured),
	-- 		'',
	-- 	REPLACECHR(0, REPLACECHR(0,ltrim(rtrim(zip_insured)),'-',''), ' ', '')
	-- 	)
	IFF(
	    zip_insured IS NULL, '',
	    REGEXP_REPLACE(REGEXP_REPLACE(ltrim(rtrim(zip_insured)),'-','','i'),' ','','i')
	) AS v_zip_insured,
	LKP_TO_GET_EMPLOYER_DETAILS.claim_party_full_name AS full_name_emplr,
	-- *INF*: REPLACESTR(0,ltrim(rtrim(full_name_emplr)),
	-- 	'`',
	-- 	'~',
	-- 	'!',
	-- 	'@'
	-- 	,'#',
	-- 	'$',
	-- 	'%',
	-- 	'^',
	-- 	'&',
	-- 	'*',
	-- 	'(',
	-- 	')',
	-- 	'-',
	-- 	'_',
	-- 	'=',
	-- 	'+',
	-- 	'[',
	-- 	'{',
	-- 	']',
	-- 	'}',
	-- 	'|',
	-- 	';',
	-- 	':',
	-- 	'"', 
	-- 	CHR(39),
	-- 	',',
	-- 	'<',
	-- 	'.',
	-- 	'>',
	-- 	'?',
	-- 	'')
	REGEXP_REPLACE(ltrim(rtrim(full_name_emplr)),'`','~','!','@','#','$','%','^','&','*','(',')','-','_','=','+','[','{',']','}','|',';',':','"',CHR(39),',','<','.','>','?','','i') AS full_name_emplr_reformatted,
	-- *INF*: IIF(iif(isnull(full_name_emplr_reformatted),v_full_name_reformatted_insured,ltrim(rtrim(full_name_emplr_reformatted)))  = 'N/A'
	-- ,''
	-- ,iif(isnull(full_name_emplr_reformatted),v_full_name_reformatted_insured,ltrim(rtrim(full_name_emplr_reformatted))) )
	-- 
	-- 
	IFF(
	    IFF(
	        full_name_emplr_reformatted IS NULL, v_full_name_reformatted_insured,
	        ltrim(rtrim(full_name_emplr_reformatted))
	    ) = 'N/A',
	    '',
	    IFF(
	        full_name_emplr_reformatted IS NULL, v_full_name_reformatted_insured,
	        ltrim(rtrim(full_name_emplr_reformatted))
	    )
	) AS v_full_name_emplr,
	v_full_name_emplr AS o_full_name_emplr,
	LKP_TO_GET_EMPLOYER_DETAILS.claim_party_addr AS addr_emplr,
	-- *INF*: REPLACESTR(0,ltrim(rtrim(addr_emplr)),
	-- 	'`',
	-- 	'~',
	-- 	'!',
	-- 	'@'
	-- 	,'#',
	-- 	'$',
	-- 	'%',
	-- 	'^',
	-- 	'&',
	-- 	'*',
	-- 	'(',
	-- 	')',
	-- 	'-',
	-- 	'_',
	-- 	'=',
	-- 	'+',
	-- 	'[',
	-- 	'{',
	-- 	']',
	-- 	'}',
	-- 	'|',
	-- 	';',
	-- 	':',
	-- 	'"', 
	-- 	CHR(39),
	-- 	',',
	-- 	'<',
	-- 	'.',
	-- 	'>',
	-- 	'?',
	-- 	'')
	REGEXP_REPLACE(ltrim(rtrim(addr_emplr)),'`','~','!','@','#','$','%','^','&','*','(',')','-','_','=','+','[','{',']','}','|',';',':','"',CHR(39),',','<','.','>','?','','i') AS addr_emplr_reformatted,
	-- *INF*: IIF(iif(isnull(addr_emplr_reformatted),v_addr_insured,ltrim(rtrim(addr_emplr_reformatted)))  = 'N/A'
	-- ,''
	-- ,iif(isnull(addr_emplr_reformatted),v_addr_insured,ltrim(rtrim(addr_emplr_reformatted))) )
	-- 
	-- 
	-- --IIF(ISNULL(addr_emplr_reformatted),v_addr_insured,addr_emplr_reformatted)
	IFF(
	    IFF(
	        addr_emplr_reformatted IS NULL, v_addr_insured, ltrim(rtrim(addr_emplr_reformatted))
	    ) = 'N/A',
	    '',
	    IFF(
	        addr_emplr_reformatted IS NULL, v_addr_insured, ltrim(rtrim(addr_emplr_reformatted))
	    )
	) AS v_addr_emplr,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','MO','WI','IN','KS','TN','KY'),v_addr_emplr,NULL )
	-- 
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','MO','WI','IN','KS','TN','KY'), v_addr_emplr,
	    NULL
	) AS o_addr_emplr,
	LKP_TO_GET_EMPLOYER_DETAILS.claim_party_city AS city_emplr,
	-- *INF*: IIF(iif(isnull(city_emplr),v_city_insured,ltrim(rtrim(city_emplr)))  = 'N/A'
	-- ,''
	-- ,iif(isnull(city_emplr),v_city_insured,ltrim(rtrim(city_emplr))) )
	-- 
	-- 
	IFF(
	    IFF(
	        city_emplr IS NULL, v_city_insured, ltrim(rtrim(city_emplr))
	    ) = 'N/A', '',
	    IFF(
	        city_emplr IS NULL, v_city_insured, ltrim(rtrim(city_emplr))
	    )
	) AS v_city_emplr,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','MO','WI','IN','KS','NE','TN','KY'),v_city_emplr,
	-- NULL )
	-- 
	-- 
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','MO','WI','IN','KS','NE','TN','KY'), v_city_emplr,
	    NULL
	) AS o_city_emplr,
	LKP_TO_GET_EMPLOYER_DETAILS.claim_party_state AS state_emplr,
	-- *INF*: IIF(iif(isnull(state_emplr),v_state_insured,ltrim(rtrim(state_emplr)))  = 'N/A'
	-- ,''
	-- ,iif(isnull(state_emplr),v_state_insured,ltrim(rtrim(state_emplr))) )
	IFF(
	    IFF(
	        state_emplr IS NULL, v_state_insured, ltrim(rtrim(state_emplr))
	    ) = 'N/A',
	    '',
	    IFF(
	        state_emplr IS NULL, v_state_insured, ltrim(rtrim(state_emplr))
	    )
	) AS v_state_emplr,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','MO','WI','IN','KS','NE','IL','TN','KY'),v_state_emplr,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','MO','WI','IN','KS','NE','IL','TN','KY'), v_state_emplr,
	    NULL
	) AS v_state_emplr_by_jurisdiction,
	v_state_emplr_by_jurisdiction AS o_state_emplr,
	LKP_TO_GET_EMPLOYER_DETAILS.claim_party_zip AS zip_emplr,
	-- *INF*: REPLACECHR(0, REPLACECHR(0,LTRIM(RTRIM(zip_emplr)),'-',''), ' ', '')
	REGEXP_REPLACE(REGEXP_REPLACE(LTRIM(RTRIM(zip_emplr)),'-','','i'),' ','','i') AS v_zip_emplr_reformatted,
	-- *INF*: IIF(iif(isnull(v_zip_emplr_reformatted),v_zip_insured,ltrim(rtrim(v_zip_emplr_reformatted)))  = 'N/A'
	-- ,''
	-- ,iif(isnull(v_zip_emplr_reformatted),v_zip_insured,v_zip_emplr_reformatted) )
	IFF(
	    IFF(
	        v_zip_emplr_reformatted IS NULL, v_zip_insured,
	        ltrim(rtrim(v_zip_emplr_reformatted))
	    ) = 'N/A',
	    '',
	    IFF(
	        v_zip_emplr_reformatted IS NULL, v_zip_insured, v_zip_emplr_reformatted
	    )
	) AS v_zip_emplr,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','MO','WI','IN','KS','TN','KY'),v_zip_emplr,
	-- NULL )
	-- 
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','MO','WI','IN','KS','TN','KY'), v_zip_emplr,
	    NULL
	) AS o_zip_emplr,
	-- *INF*: IIF(IN(v_state_emplr_by_jurisdiction,'AB','BC','MB','NB','NL','NT','NS','NU','ON','PE','QC','SK','YT'),
	-- 'CA',
	-- NULL)
	IFF(
	    v_state_emplr_by_jurisdiction IN ('AB','BC','MB','NB','NL','NT','NS','NU','ON','PE','QC','SK','YT'),
	    'CA',
	    NULL
	) AS o_EmployerPhysicalCountry,
	LKP_TO_GET_EMPLOYER_DETAILS.addr_type AS addr_type_empr,
	LKP_TO_GET_EMPLOYER_DETAILS.tax_ssn_id AS tax_ssn_id_emplr,
	-- *INF*: IIF(ISNULL(tax_ssn_id_emplr),'',REPLACECHR(0,LTRIM(RTRIM(tax_ssn_id_emplr)),'-',''))
	IFF(tax_ssn_id_emplr IS NULL, '', REGEXP_REPLACE(LTRIM(RTRIM(tax_ssn_id_emplr)),'-','','i')) AS tax_ssn_id_emplr_reformatted,
	LKP_TO_GET_EMPLOYER_DETAILS.tax_fed_id AS tax_fed_id_emplr,
	-- *INF*: IIF(ISNULL(tax_fed_id_emplr),'',replacechr(0,ltrim(rtrim(tax_fed_id_emplr)),'-',''))
	IFF(tax_fed_id_emplr IS NULL, '', REGEXP_REPLACE(ltrim(rtrim(tax_fed_id_emplr)),'-','','i')) AS tax_fed_id_emplr_reformatted,
	-- *INF*: DECODE(TRUE,
	-- NOT IN(tax_fed_id_emplr_reformatted,'','N/A'),tax_fed_id_emplr_reformatted,
	-- DECODE(TRUE,
	-- NOT IN(tax_ssn_id_emplr_reformatted,'','N/A'),tax_ssn_id_emplr_reformatted,
	-- DECODE(TRUE,
	-- NOT IN(tax_fed_id_insured_reformatted,'','N/A'),tax_fed_id_insured_reformatted,
	-- DECODE(TRUE,
	-- NOT IN(tax_ssn_id_insured_reformatted,'','N/A'),tax_ssn_id_insured_reformatted,
	-- '' ) ) ) )
	-- 
	-- 
	-- 
	-- 
	-- 
	DECODE(
	    TRUE,
	    NOT tax_fed_id_emplr_reformatted IN ('','N/A'), tax_fed_id_emplr_reformatted,
	    DECODE(
	        TRUE,
	        NOT tax_ssn_id_emplr_reformatted IN ('','N/A'), tax_ssn_id_emplr_reformatted,
	        DECODE(
	                TRUE,
	                NOT tax_fed_id_insured_reformatted IN ('','N/A'), tax_fed_id_insured_reformatted,
	                DECODE(
	                    TRUE,
	                    NOT tax_ssn_id_insured_reformatted IN ('','N/A'), tax_ssn_id_insured_reformatted,
	                    ''
	                )
	            )
	    )
	) AS v_tax_fed_id_emplr,
	v_tax_fed_id_emplr AS o_tax_fed_id_emplr,
	EXP_SOURCE.o_emp_dept_num AS emp_dept_num,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'IN'),emp_dept_num,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('IN'), emp_dept_num,
	    NULL
	) AS o_emp_dept_num,
	LKP_Get_Policy_Details.source_sys_id,
	LKP_Get_Policy_Details.pol_key,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(pol_key),'',
	-- LTRIM(RTRIM(pol_key))='N/A','',
	-- LTRIM(RTRIM(pol_key)) )
	-- 
	-- 
	DECODE(
	    TRUE,
	    pol_key IS NULL, '',
	    LTRIM(RTRIM(pol_key)) = 'N/A', '',
	    LTRIM(RTRIM(pol_key))
	) AS v_pol_key,
	v_pol_key AS o_pol_key,
	LKP_Get_Policy_Details.pol_eff_date,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(pol_eff_date),'',
	-- TO_CHAR(pol_eff_date,'YYYYMMDD')='18000101','',
	-- TO_CHAR(pol_eff_date,'YYYYMMDD') )
	-- 
	-- 
	-- 
	-- --IIF(ISNULL(pol_eff_date)
	-- --,''
	-- --,TO_CHAR(pol_eff_date,'YYYYMMDD'))
	DECODE(
	    TRUE,
	    pol_eff_date IS NULL, '',
	    TO_CHAR(pol_eff_date, 'YYYYMMDD') = '18000101', '',
	    TO_CHAR(pol_eff_date, 'YYYYMMDD')
	) AS v_pol_eff_date,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MO','KS','TN'),v_pol_eff_date,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MO','KS','TN'), v_pol_eff_date,
	    NULL
	) AS o_pol_eff_date,
	LKP_Get_Policy_Details.pol_exp_date,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(pol_exp_date),'',
	-- TO_CHAR(pol_exp_date,'YYYYMMDD')='18000101','',
	-- TO_CHAR(pol_exp_date,'YYYYMMDD') )
	-- 
	-- 
	-- 
	-- --IIF(ISNULL(pol_exp_date)
	-- --,''
	-- --,TO_CHAR(pol_exp_date,'YYYYMMDD'))
	DECODE(
	    TRUE,
	    pol_exp_date IS NULL, '',
	    TO_CHAR(pol_exp_date, 'YYYYMMDD') = '18000101', '',
	    TO_CHAR(pol_exp_date, 'YYYYMMDD')
	) AS v_pol_exp_date,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MO','TN'),v_pol_exp_date,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MO','TN'), v_pol_exp_date,
	    NULL
	) AS o_pol_exp_date,
	EXP_SOURCE.o_convert_claim_loss_date AS claim_loss_date,
	EXP_SOURCE.o_claim_loss_time AS claim_loss_time,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(claim_loss_date))='18000101', '',
	--  claim_loss_time )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(claim_loss_date)) = '18000101', '',
	    claim_loss_time
	) AS v_claim_loss_time,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','IL','WI','IA','KS','KY'),v_claim_loss_time,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','IL','WI','IA','KS','KY'), v_claim_loss_time,
	    NULL
	) AS o_claim_loss_time,
	EXP_SOURCE.o_loss_loc_zip AS loss_loc_zip,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','MO','IN','NE','WI','TN','KY'),loss_loc_zip,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'KS') AND LTRIM(RTRIM(premises_code))='E', loss_loc_zip ,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'KS') AND LTRIM(RTRIM(premises_code)) != 'E', '' ,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'IA'),'',
	-- NULL  )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','MO','IN','NE','WI','TN','KY'), loss_loc_zip,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('KS') AND LTRIM(RTRIM(premises_code)) = 'E', loss_loc_zip,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('KS') AND LTRIM(RTRIM(premises_code)) != 'E', '',
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('IA'), '',
	    NULL
	) AS o_loss_loc_zip,
	EXP_SOURCE.o_nature_inj_code AS nature_inj_code,
	EXP_SOURCE.o_body_part_code AS body_part_code,
	EXP_SOURCE.o_cause_inj_code AS cause_inj_code,
	EXP_SOURCE.o_claim_loss_descript AS claim_loss_descript,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'IL'),SUBSTR(
	-- LTRIM(RTRIM(claim_loss_descript )),1,150),
	-- LTRIM(RTRIM(claim_loss_descript )) )
	-- 
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('IL'), SUBSTR(LTRIM(RTRIM(claim_loss_descript)), 1, 150),
	    LTRIM(RTRIM(claim_loss_descript))
	) AS o_claim_loss_descript,
	EXP_SOURCE.o_inital_treatment_code AS inital_treatment_code,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','IN','MO','KS'),inital_treatment_code,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','IN','MO','KS'), inital_treatment_code,
	    NULL
	) AS o_inital_treatment_code,
	EXP_SOURCE.o_emplyr_notified_date AS emplyr_notified_date,
	-- *INF*: DECODE(TRUE,
	-- 	IN(LTRIM(RTRIM(jurisdiction_state_code)),'WI','IL','NE','TN'),
	-- 		NULL ,
	-- 	emplyr_notified_date)
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('WI','IL','NE','TN'), NULL,
	    emplyr_notified_date
	) AS o_emplyr_notified_date,
	EXP_SOURCE.o_reported_to_carrier_date AS reported_to_carrier_date,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','MO','KS','IA','KY'),reported_to_carrier_date,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','MO','KS','IA','KY'), reported_to_carrier_date,
	    NULL
	) AS o_reported_to_carrier_date,
	EXP_SOURCE.o_tax_ssn_id AS emp_tax_ssn_id,
	-- *INF*: LTRIM(RTRIM(emp_tax_ssn_id ))
	-- 
	-- 
	LTRIM(RTRIM(emp_tax_ssn_id)) AS o_emp_tax_ssn_id,
	EXP_SOURCE.o_claim_party_last_name AS emp_claim_party_last_name,
	EXP_SOURCE.o_claim_party_first_name AS emp_claim_party_first_name,
	EXP_SOURCE.o_claim_party_mid_name AS emp_claim_party_mid_name,
	NULL AS o_emp_claim_party_mid_name,
	EXP_SOURCE.o_claim_party_addr AS emp_claim_party_addr,
	-- *INF*: LTRIM(RTRIM(emp_claim_party_addr))
	-- 
	LTRIM(RTRIM(emp_claim_party_addr)) AS o_emp_claim_party_addr,
	EXP_SOURCE.o_claim_party_city AS emp_claim_party_city,
	EXP_SOURCE.o_claim_party_state AS emp_claim_party_state,
	EXP_SOURCE.o_claim_party_zip AS emp_claim_party_zip,
	-- *INF*: LTRIM(RTRIM(emp_claim_party_zip))
	-- 
	-- 
	LTRIM(RTRIM(emp_claim_party_zip)) AS o_emp_claim_party_zip,
	EXP_SOURCE.o_ph_num AS emp_ph_num,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','WI'),emp_ph_num,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','WI'), emp_ph_num,
	    NULL
	) AS o_emp_ph_num,
	EXP_SOURCE.o_claim_party_birthdate AS emp_claim_party_birthdate,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','WI','MO','NE','KS','IA','IN','TN','KY'),emp_claim_party_birthdate,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','WI','MO','NE','KS','IA','IN','TN','KY'), emp_claim_party_birthdate,
	    NULL
	) AS o_emp_claim_party_birthdate,
	EXP_SOURCE.o_claim_party_gndr AS emp_claim_party_gndr,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'IN','NE'),NULL ,
	-- emp_claim_party_gndr )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('IN','NE'), NULL,
	    emp_claim_party_gndr
	) AS o_emp_claim_party_gndr,
	EXP_SOURCE.claim_party_name_sfx AS emp_claim_party_name_sfx,
	-- *INF*: --DECODE(TRUE,
	-- --IN(LTRIM(RTRIM(jurisdiction_state_code)),'IA'),
	-- --LTRIM(RTRIM(emp_claim_party_name_sfx)),
	-- NULL
	NULL AS o_emp_claim_party_name_sfx,
	EXP_SOURCE.o_marital_status AS marital_status,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(jurisdiction_state_code))='MN',
	-- DECODE(TRUE,
	-- IN(LTRIM(RTRIM(marital_status)),'E','L','P'),'S',
	-- IN(LTRIM(RTRIM(marital_status)),'C','D','S','W'),'U',
	-- LTRIM(RTRIM(marital_status))='M','M',
	-- LTRIM(RTRIM(marital_status))='U','K',
	-- NULL ),
	-- LTRIM(RTRIM(jurisdiction_state_code))='IL',
	-- DECODE(TRUE,
	-- IN(LTRIM(RTRIM(marital_status)),'C','D','E','L','P','S','W','U'),'S',
	-- LTRIM(RTRIM(marital_status))='M','M',
	-- NULL ) ,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'IA'),
	-- DECODE(TRUE,
	-- IN(LTRIM(RTRIM(marital_status)),'C','D','S','W'),'U',
	-- IN(LTRIM(RTRIM(marital_status)),'E','L','P'),'S',
	-- LTRIM(RTRIM(marital_status))='U','K',
	-- LTRIM(RTRIM(marital_status))='M','M',
	-- NULL ) ,
	-- LTRIM(RTRIM(marital_status)))
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) = 'MN', DECODE(
	        TRUE,
	        LTRIM(RTRIM(marital_status)) IN ('E','L','P'), 'S',
	        LTRIM(RTRIM(marital_status)) IN ('C','D','S','W'), 'U',
	        LTRIM(RTRIM(marital_status)) = 'M', 'M',
	        LTRIM(RTRIM(marital_status)) = 'U', 'K',
	        NULL
	    ),
	    LTRIM(RTRIM(jurisdiction_state_code)) = 'IL', DECODE(
	        TRUE,
	        LTRIM(RTRIM(marital_status)) IN ('C','D','E','L','P','S','W','U'), 'S',
	        LTRIM(RTRIM(marital_status)) = 'M', 'M',
	        NULL
	    ),
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('IA'), DECODE(
	        TRUE,
	        LTRIM(RTRIM(marital_status)) IN ('C','D','S','W'), 'U',
	        LTRIM(RTRIM(marital_status)) IN ('E','L','P'), 'S',
	        LTRIM(RTRIM(marital_status)) = 'U', 'K',
	        LTRIM(RTRIM(marital_status)) = 'M', 'M',
	        NULL
	    ),
	    LTRIM(RTRIM(marital_status))
	) AS v_marital_status,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','IL','IA'),v_marital_status,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','IL','IA'), v_marital_status,
	    NULL
	) AS o_marital_status,
	EXP_SOURCE.num_of_dependents,
	-- *INF*: DECODE(TRUE,
	-- 	IN(LTRIM(RTRIM(jurisdiction_state_code)),'WI','IL','NE','KS','IA','IN','TN'),
	-- 	NULL,
	-- 	num_of_dependents)
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('WI','IL','NE','KS','IA','IN','TN'), NULL,
	    num_of_dependents
	) AS o_num_of_dependents,
	EXP_SOURCE.o_disability_date AS disability_date,
	'' AS v_disability_date,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'IL','NE','TN'),NULL ,
	-- disability_date )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('IL','NE','TN'), NULL,
	    disability_date
	) AS o_disability_date,
	EXP_SOURCE.o_death_date AS death_date,
	-- *INF*: DECODE(TRUE,
	-- 	IN(LTRIM(RTRIM(jurisdiction_state_code)),'TN'),
	-- 		NULL,
	-- 	death_date)
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('TN'), NULL,
	    death_date
	) AS o_death_date,
	EXP_SOURCE.o_emplymnt_status_code AS emplymnt_status_code,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','KS','IA','KY'),LTRIM(RTRIM(emplymnt_status_code)),
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','KS','IA','KY'), LTRIM(RTRIM(emplymnt_status_code)),
	    NULL
	) AS o_emplymnt_status_code,
	LKP_CLASS_CODE_CCD.risk_unit,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(risk_unit))='N/A' OR ISNULL(risk_unit),'',
	-- SUBSTR(LTRIM(RTRIM(risk_unit)),1,4) )
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(risk_unit)) = 'N/A' OR risk_unit IS NULL, '',
	    SUBSTR(LTRIM(RTRIM(risk_unit)), 1, 4)
	) AS v_risk_unit,
	LKP_CLASS_CODE_CCD.ClassCode AS ClassCode_DCT,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(ClassCode_DCT))='N/A' OR ISNULL(ClassCode_DCT),'', LTRIM(RTRIM(ClassCode_DCT)) )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(ClassCode_DCT)) = 'N/A' OR ClassCode_DCT IS NULL, '',
	    LTRIM(RTRIM(ClassCode_DCT))
	) AS v_ClassCode_DCT,
	EXP_SOURCE.ManualClassificationCode,
	LKP_arch_claim_object_stage.cob_risk_unit,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(cob_risk_unit))='N/A' OR ISNULL(cob_risk_unit),'',
	-- SUBSTR(LTRIM(RTRIM(cob_risk_unit)),1,4) )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(cob_risk_unit)) = 'N/A' OR cob_risk_unit IS NULL, '',
	    SUBSTR(LTRIM(RTRIM(cob_risk_unit)), 1, 4)
	) AS v_cob_risk_unit,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(source_sys_id)) = 'PMS',
	-- DECODE(TRUE,
	-- LTRIM(RTRIM(v_risk_unit)) != '',LTRIM(RTRIM(v_risk_unit)),
	-- LTRIM(RTRIM(v_cob_risk_unit)) ),
	-- LTRIM(RTRIM(v_ClassCode_DCT)) 
	-- )
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(source_sys_id)) = 'PMS', DECODE(
	        TRUE,
	        LTRIM(RTRIM(v_risk_unit)) != '', LTRIM(RTRIM(v_risk_unit)),
	        LTRIM(RTRIM(v_cob_risk_unit))
	    ),
	    LTRIM(RTRIM(v_ClassCode_DCT))
	) AS v_class_code,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'IN','WI','NE','MN','TN'),NULL,
	-- LTRIM(RTRIM(jurisdiction_state_code)) = 'IA',
	-- DECODE(TRUE,
	-- LTRIM(RTRIM(emplymnt_status_code)) = '7',LTRIM(RTRIM(ManualClassificationCode)),
	-- '' ),
	-- v_class_code )
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('IN','WI','NE','MN','TN'), NULL,
	    LTRIM(RTRIM(jurisdiction_state_code)) = 'IA', DECODE(
	        TRUE,
	        LTRIM(RTRIM(emplymnt_status_code)) = '7', LTRIM(RTRIM(ManualClassificationCode)),
	        ''
	    ),
	    v_class_code
	) AS v_class_code_state,
	v_class_code_state AS o_class_code,
	EXP_SOURCE.o_occuptn_descript AS occuptn_descript,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','WI','IA'),LTRIM(RTRIM(occuptn_descript)),
	-- NULL )
	-- 
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','WI','IA'), LTRIM(RTRIM(occuptn_descript)),
	    NULL
	) AS v_occuptn_descript,
	v_occuptn_descript AS o_occuptn_descript,
	EXP_SOURCE.o_hired_date AS hired_date,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','WI','KS','IA','KY'),hired_date,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','WI','KS','IA','KY'), hired_date,
	    NULL
	) AS o_hired_date,
	EXP_SOURCE.o_avg_wkly_wage AS avg_wkly_wage,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'NE','IA'),NULL,
	-- avg_wkly_wage )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('NE','IA'), NULL,
	    avg_wkly_wage
	) AS o_avg_wkly_wage,
	EXP_SOURCE.wage_period_code,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'TN','KY'),
	-- 	IIF(TO_DECIMAL(RTRIM(LTRIM(avg_wkly_wage)), 2) = 0.0, NULL, '01'),
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'IL','NE','IA') ,
	-- 	NULL,
	-- wage_period_code)
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('TN','KY'), IFF(
	        CAST(RTRIM(LTRIM(avg_wkly_wage)) AS FLOAT) = 0.0, NULL, '01'
	    ),
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('IL','NE','IA'), NULL,
	    wage_period_code
	) AS o_wage_period_code,
	EXP_SOURCE.emp_day_week,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','TN'),emp_day_week,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','TN'), emp_day_week,
	    NULL
	) AS o_emp_day_week,
	EXP_SOURCE.o_max_created_date AS max_,
	LKP_RTW_TYPE_AND_DATE.min_emp_last_day_worked AS emp_last_day_worked,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(emp_last_day_worked) OR TO_CHAR(emp_last_day_worked,'YYYYMMDD')  = '18000101','',
	-- TO_CHAR(emp_last_day_worked,'YYYYMMDD') )
	-- 
	-- 
	-- 
	DECODE(
	    TRUE,
	    emp_last_day_worked IS NULL OR TO_CHAR(emp_last_day_worked, 'YYYYMMDD') = '18000101', '',
	    TO_CHAR(emp_last_day_worked, 'YYYYMMDD')
	) AS v_emp_last_day_worked,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'IN','WI','KS','IA','KY'),v_emp_last_day_worked,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('IN','WI','KS','IA','KY'), v_emp_last_day_worked,
	    NULL
	) AS o_emp_last_day_worked,
	EXP_SOURCE.o_full_pay_inj_day_ind AS full_pay_inj_day_ind,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','IN','IA'),full_pay_inj_day_ind,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','IN','IA'), full_pay_inj_day_ind,
	    NULL
	) AS o_full_pay_inj_day_ind,
	LKP_WRK_HISTORY.return_to_work_type,
	-- *INF*: decode(true,
	-- isnull(return_to_work_type),'',
	-- ltrim(rtrim(return_to_work_type))='N/A','',
	-- ltrim(rtrim(return_to_work_type)) )
	decode(
	    true,
	    return_to_work_type IS NULL, '',
	    ltrim(rtrim(return_to_work_type)) = 'N/A', '',
	    ltrim(rtrim(return_to_work_type))
	) AS v_return_to_work_type,
	LKP_WRK_HISTORY.min_return_to_work_date,
	-- *INF*: DECODE(true,
	-- isnull(min_return_to_work_date),'',
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','IN'),
	-- DECODE(TRUE,
	-- ltrim(rtrim(v_return_to_work_type))='A' AND to_char(min_return_to_work_date,'YYYYMMDD') != '18000101', to_char(min_return_to_work_date,'YYYYMMDD'),
	-- '' ),
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'KS','MO','IA','WI'),
	-- DECODE(TRUE,
	-- IN(ltrim(rtrim(v_return_to_work_type)),'A','R') AND to_char(min_return_to_work_date,'YYYYMMDD') != '18000101', to_char(min_return_to_work_date,'YYYYMMDD'),
	-- '' ),
	-- '' )
	-- 
	-- 
	DECODE(
	    true,
	    min_return_to_work_date IS NULL, '',
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','IN'), DECODE(
	        TRUE,
	        ltrim(rtrim(v_return_to_work_type)) = 'A' AND to_char(min_return_to_work_date, 'YYYYMMDD') != '18000101', to_char(min_return_to_work_date, 'YYYYMMDD'),
	        ''
	    ),
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('KS','MO','IA','WI'), DECODE(
	        TRUE,
	        ltrim(rtrim(v_return_to_work_type)) IN ('A','R') AND to_char(min_return_to_work_date, 'YYYYMMDD') != '18000101', to_char(min_return_to_work_date, 'YYYYMMDD'),
	        ''
	    ),
	    ''
	) AS v_return_to_work_date,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'IL','NE','TN','KY'),NULL,
	-- v_return_to_work_date )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('IL','NE','TN','KY'), NULL,
	    v_return_to_work_date
	) AS o_return_to_work_date,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN'),DECODE(TRUE,
	-- ISNULL(min_return_to_work_date),'',
	-- TO_CHAR(min_return_to_work_date,'YYYYMMDD') != '18000101' AND  LTRIM(RTRIM(return_to_work_type)) = 'A','A',
	-- '' ),
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'KS','IA'),
	-- DECODE(TRUE,
	-- ISNULL(min_return_to_work_date),'',
	-- TO_CHAR(min_return_to_work_date,'YYYYMMDD') != '18000101' AND  IN(LTRIM(RTRIM(return_to_work_type)),'A','R'),
	-- LTRIM(RTRIM(return_to_work_type)),
	-- '' ), 
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN'), DECODE(
	        TRUE,
	        min_return_to_work_date IS NULL, '',
	        TO_CHAR(min_return_to_work_date, 'YYYYMMDD') != '18000101' AND LTRIM(RTRIM(return_to_work_type)) = 'A', 'A',
	        ''
	    ),
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('KS','IA'), DECODE(
	        TRUE,
	        min_return_to_work_date IS NULL, '',
	        TO_CHAR(min_return_to_work_date, 'YYYYMMDD') != '18000101' AND LTRIM(RTRIM(return_to_work_type)) IN ('A','R'), LTRIM(RTRIM(return_to_work_type)),
	        ''
	    ),
	    NULL
	) AS o_return_to_work_type,
	EXP_SOURCE.o_pre_exst_disability_ind AS pre_exst_disability_ind,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','IN','IL','WI','MO'),'',
	-- pre_exst_disability_ind )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','IN','IL','WI','MO'), '',
	    pre_exst_disability_ind
	) AS o_pre_exst_disability_ind,
	EXP_SOURCE.o_max_med_improvement_date AS max_med_improvement_date,
	-- *INF*: max_med_improvement_date
	-- 
	-- --DECODE(TRUE,
	-- --@{pipeline().parameters.STATE_CODE}='MN','',
	-- --max_med_improvement_date)
	max_med_improvement_date AS v_max_med_improvement_date,
	v_max_med_improvement_date AS o_max_med_improvement_date,
	LKP_WRK_HISTORY.max_return_to_work_date AS CurrentRTWDate,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(jurisdiction_state_code)) = 'IA',
	-- Decode(TRUE,
	-- isnull(CurrentRTWDate),'',
	-- IN(ltrim(rtrim(v_return_to_work_type)),'A','R') AND to_char(CurrentRTWDate,'YYYYMMDD') != '18000101',
	-- to_char(CurrentRTWDate,'YYYYMMDD'),
	-- '' ),
	-- NULL )
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) = 'IA', Decode(
	        TRUE,
	        CurrentRTWDate IS NULL, '',
	        ltrim(rtrim(v_return_to_work_type)) IN ('A','R') AND to_char(CurrentRTWDate, 'YYYYMMDD') != '18000101', to_char(CurrentRTWDate, 'YYYYMMDD'),
	        ''
	    ),
	    NULL
	) AS v_CurrentRTWDate,
	v_CurrentRTWDate AS o_CurrentRTWDate,
	EXP_SOURCE.source_claim_occurrence_status_code,
	-- *INF*: DECODE(TRUE,
	-- 	IN(LTRIM(RTRIM(jurisdiction_state_code)),'IA','KY'),
	-- 		DECODE(TRUE,
	-- 			LTRIM(RTRIM(source_claim_occurrence_status_code))='',
	-- 				'',
	-- 			LTRIM(RTRIM(source_claim_occurrence_status_code))='O',
	-- 				'O',
	-- 		'C'),
	-- 	NULL)
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('IA','KY'), DECODE(
	        TRUE,
	        LTRIM(RTRIM(source_claim_occurrence_status_code)) = '', '',
	        LTRIM(RTRIM(source_claim_occurrence_status_code)) = 'O', 'O',
	        'C'
	    ),
	    NULL
	) AS o_source_claim_occurrence_status_code,
	EXP_SOURCE.o_claim_ctgry_code AS claim_ctgry_code,
	EXP_SOURCE.o_obtained_lgl_representation_date AS obtained_lgl_representation_date,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code )),'MN','IA','TN','KY'),
	-- @{pipeline().parameters.TEST_PRODUCTION_INDICATOR},
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','IA','TN','KY'), @{pipeline().parameters.TEST_PRODUCTION_INDICATOR},
	    NULL
	) AS Test_Prod_Ind,
	EXP_SOURCE.inj_loc_comment,
	-- *INF*: IIF(LTRIM(RTRIM(jurisdiction_state_code))='MN',LTRIM(RTRIM(inj_loc_comment)),'')
	-- 
	-- 
	-- --IIF(ISNULL(inj_loc_comment)  OR LTRIM(RTRIM(inj_loc_comment))='N/A',v_loss_loc_city||' '||v_loss_loc_state||' '|| loss_loc_zip , LTRIM(RTRIM(inj_loc_comment)))
	-- 
	IFF(LTRIM(RTRIM(jurisdiction_state_code)) = 'MN', LTRIM(RTRIM(inj_loc_comment)), '') AS v_inj_loc_comment,
	v_inj_loc_comment AS o_inj_loc_comment,
	EXP_SOURCE.premises_code,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MO'),premises_code,
	-- '' )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MO'), premises_code,
	    ''
	) AS o_premises_code,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(premises_code)),'E','L') AND LTRIM(RTRIM(v_full_name_reformatted_insured)) = LTRIM(RTRIM(v_full_name_emplr)),LTRIM(RTRIM(v_full_name_reformatted_insured)),
	-- LTRIM(RTRIM(premises_code)) ='E' AND LTRIM(RTRIM(v_full_name_reformatted_insured)) != LTRIM(RTRIM(v_full_name_emplr)),LTRIM(RTRIM(v_full_name_emplr)),
	-- LTRIM(RTRIM(premises_code)) ='L' AND LTRIM(RTRIM(v_full_name_reformatted_insured)) != LTRIM(RTRIM(v_full_name_emplr)),LTRIM(RTRIM(v_full_name_reformatted_insured)),
	-- '' )
	-- 
	-- 
	-- 
	-- --DECODE(TRUE,
	-- --IN(LTRIM(RTRIM(premises_code)),'E','L') AND v_tax_fed_id_emplr=v_tax_fed_id_insured,full_name_reformatted_insured,
	-- --LTRIM(RTRIM(premises_code)) ='E',full_name_emplr_reformatted,
	-- --LTRIM(RTRIM(premises_code))--='L',full_name_reformatted_insured,
	-- --'')
	-- 
	-- 
	-- --IIF(IN(LTRIM(RTRIM(premises_code)),'E','L')  AND v_tax_fed_id_emplr=tax_fed_id_insured_reformatted,v_full_name_reformatted_insured,IIF(LTRIM(RTRIM(premises_code))--='E',full_name_emplr_reformatted,IIF(LTRIM(RTRIM(premises_code))='L',v_full_name_reformatted_insured,' ')))
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(premises_code)) IN ('E','L') AND LTRIM(RTRIM(v_full_name_reformatted_insured)) = LTRIM(RTRIM(v_full_name_emplr)), LTRIM(RTRIM(v_full_name_reformatted_insured)),
	    LTRIM(RTRIM(premises_code)) = 'E' AND LTRIM(RTRIM(v_full_name_reformatted_insured)) != LTRIM(RTRIM(v_full_name_emplr)), LTRIM(RTRIM(v_full_name_emplr)),
	    LTRIM(RTRIM(premises_code)) = 'L' AND LTRIM(RTRIM(v_full_name_reformatted_insured)) != LTRIM(RTRIM(v_full_name_emplr)), LTRIM(RTRIM(v_full_name_reformatted_insured)),
	    ''
	) AS v_AccSite_OrgName,
	-- *INF*: DECODE(TRUE,
	-- 	IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','KY'),
	-- 		v_AccSite_OrgName,
	-- 	IN(LTRIM(RTRIM(jurisdiction_state_code)),'IA'),
	-- 		'',
	-- 	NULL)
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','KY'), v_AccSite_OrgName,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('IA'), '',
	    NULL
	) AS o_AccSite_OrgName,
	EXP_SOURCE.o_loss_loc_county AS loss_loc_county,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(jurisdiction_state_code)) = 'IA',loss_loc_county,
	-- LTRIM(RTRIM(jurisdiction_state_code)) = 'KS',
	-- DECODE(TRUE,
	-- LTRIM(RTRIM(premises_code)) != 'E',
	-- LTRIM(RTRIM(loss_loc_county)),
	-- ''),
	-- NULL )
	-- 
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) = 'IA', loss_loc_county,
	    LTRIM(RTRIM(jurisdiction_state_code)) = 'KS', DECODE(
	        TRUE,
	        LTRIM(RTRIM(premises_code)) != 'E', LTRIM(RTRIM(loss_loc_county)),
	        ''
	    ),
	    NULL
	) AS v_loss_loc_county,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'KS','IA'), v_loss_loc_county,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('KS','IA'), v_loss_loc_county,
	    NULL
	) AS o_loss_loc_county,
	EXP_SOURCE.o_loss_loc_city AS loss_loc_city,
	-- *INF*: DECODE(TRUE,
	-- 	IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','IA'),
	-- 		'',
	-- 	IN(LTRIM(RTRIM(jurisdiction_state_code)),'KY'),
	-- 		loss_loc_city,
	-- 	NULL)
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','IA'), '',
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('KY'), loss_loc_city,
	    NULL
	) AS o_loss_loc_city,
	EXP_SOURCE.o_loss_loc_addr AS loss_loc_addr,
	-- *INF*: DECODE(TRUE,
	-- 	IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','IA'),
	-- 		'',
	-- 	IN(LTRIM(RTRIM(jurisdiction_state_code)),'KY'),
	-- 		loss_loc_addr,
	-- 	NULL)
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','IA'), '',
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('KY'), loss_loc_addr,
	    NULL
	) AS o_loss_loc_addr,
	EXP_SOURCE.o_loss_loc_state AS loss_loc_state,
	-- *INF*: DECODE(TRUE,
	-- 	IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','IA'),
	-- 		'',
	-- 	IN(ltrim(rtrim(jurisdiction_state_code)),'KS'),
	-- 		DECODE(TRUE,
	-- 			LTRIM(RTRIM(premises_code)) != 'E',
	-- 				loss_loc_state,
	-- 			''),
	-- 	IN(LTRIM(RTRIM(jurisdiction_state_code)),'KY'),
	-- 		loss_loc_state,
	-- 	NULL)
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','IA'), '',
	    ltrim(rtrim(jurisdiction_state_code)) IN ('KS'), DECODE(
	        TRUE,
	        LTRIM(RTRIM(premises_code)) != 'E', loss_loc_state,
	        ''
	    ),
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('KY'), loss_loc_state,
	    NULL
	) AS o_loss_loc_state,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(loss_loc_addr))='',NULL,
	-- LTRIM(RTRIM(loss_loc_addr)) )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(loss_loc_addr)) = '', NULL,
	    LTRIM(RTRIM(loss_loc_addr))
	) AS AccSiteLocNarrative_addr,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(loss_loc_city))='',NULL,
	-- LTRIM(RTRIM(loss_loc_city)) )
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(loss_loc_city)) = '', NULL,
	    LTRIM(RTRIM(loss_loc_city))
	) AS AccSiteLocNarrative_city,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(loss_loc_county))='',NULL,
	-- LTRIM(RTRIM(loss_loc_county)) )
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(loss_loc_county)) = '', NULL,
	    LTRIM(RTRIM(loss_loc_county))
	) AS AccSiteLocNarrative_county,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(loss_loc_state))='',NULL,
	-- LTRIM(RTRIM(loss_loc_state)) )
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(loss_loc_state)) = '', NULL,
	    LTRIM(RTRIM(loss_loc_state))
	) AS AccSiteLocNarrative_state,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(loss_loc_zip))='',NULL,
	-- LTRIM(RTRIM(loss_loc_zip)) )
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(loss_loc_zip)) = '', NULL,
	    LTRIM(RTRIM(loss_loc_zip))
	) AS AccSiteLocNarrative_zip,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(AccSiteLocNarrative_addr) AND ISNULL(AccSiteLocNarrative_city),NULL,
	-- NOT ISNULL(AccSiteLocNarrative_addr) AND NOT ISNULL(AccSiteLocNarrative_city), LTRIM(RTRIM(AccSiteLocNarrative_addr)) || ', ' || LTRIM(RTRIM(AccSiteLocNarrative_city)),
	-- ISNULL(AccSiteLocNarrative_addr) AND NOT ISNULL(AccSiteLocNarrative_city), LTRIM(RTRIM(AccSiteLocNarrative_city)),
	-- LTRIM(RTRIM(AccSiteLocNarrative_addr)) )
	DECODE(
	    TRUE,
	    AccSiteLocNarrative_addr IS NULL AND AccSiteLocNarrative_city IS NULL, NULL,
	    AccSiteLocNarrative_addr IS NULL AND AccSiteLocNarrative_city IS NOT NOT NULL, LTRIM(RTRIM(AccSiteLocNarrative_addr)) || ', ' || LTRIM(RTRIM(AccSiteLocNarrative_city)),
	    AccSiteLocNarrative_addr IS NULL AND AccSiteLocNarrative_city IS NOT NULL, LTRIM(RTRIM(AccSiteLocNarrative_city)),
	    LTRIM(RTRIM(AccSiteLocNarrative_addr))
	) AS AccNarrative_CONCAT_12,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(AccSiteLocNarrative_city) AND ISNULL(AccSiteLocNarrative_county),NULL,
	-- NOT ISNULL(AccSiteLocNarrative_city) AND NOT ISNULL(AccSiteLocNarrative_county), LTRIM(RTRIM(AccSiteLocNarrative_city)) || ', ' || LTRIM(RTRIM(AccSiteLocNarrative_county)),
	-- ISNULL(AccSiteLocNarrative_city) AND NOT ISNULL(AccSiteLocNarrative_county), LTRIM(RTRIM(AccSiteLocNarrative_county)),
	-- LTRIM(RTRIM(AccSiteLocNarrative_city)) )
	DECODE(
	    TRUE,
	    AccSiteLocNarrative_city IS NULL AND AccSiteLocNarrative_county IS NULL, NULL,
	    AccSiteLocNarrative_city IS NULL AND AccSiteLocNarrative_county IS NOT NOT NULL, LTRIM(RTRIM(AccSiteLocNarrative_city)) || ', ' || LTRIM(RTRIM(AccSiteLocNarrative_county)),
	    AccSiteLocNarrative_city IS NULL AND AccSiteLocNarrative_county IS NOT NULL, LTRIM(RTRIM(AccSiteLocNarrative_county)),
	    LTRIM(RTRIM(AccSiteLocNarrative_city))
	) AS CONCAT_City_County,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(CONCAT_City_County) AND ISNULL(AccSiteLocNarrative_state),NULL,
	-- NOT ISNULL(CONCAT_City_County) AND NOT ISNULL(AccSiteLocNarrative_state), LTRIM(RTRIM(CONCAT_City_County)) || ', ' || LTRIM(RTRIM(AccSiteLocNarrative_state)),
	-- ISNULL(CONCAT_City_County) AND NOT ISNULL(AccSiteLocNarrative_state), LTRIM(RTRIM(AccSiteLocNarrative_state)),
	-- LTRIM(RTRIM(CONCAT_City_County)) )
	DECODE(
	    TRUE,
	    CONCAT_City_County IS NULL AND AccSiteLocNarrative_state IS NULL, NULL,
	    CONCAT_City_County IS NULL AND AccSiteLocNarrative_state IS NOT NOT NULL, LTRIM(RTRIM(CONCAT_City_County)) || ', ' || LTRIM(RTRIM(AccSiteLocNarrative_state)),
	    CONCAT_City_County IS NULL AND AccSiteLocNarrative_state IS NOT NULL, LTRIM(RTRIM(AccSiteLocNarrative_state)),
	    LTRIM(RTRIM(CONCAT_City_County))
	) AS CONCAT_City_County_State,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(CONCAT_City_County_State) AND ISNULL(AccSiteLocNarrative_zip),NULL,
	-- NOT ISNULL(CONCAT_City_County_State) AND NOT ISNULL(AccSiteLocNarrative_zip), LTRIM(RTRIM(CONCAT_City_County_State)) || ' ' || LTRIM(RTRIM(AccSiteLocNarrative_zip)),
	-- ISNULL(CONCAT_City_County_State) AND NOT ISNULL(AccSiteLocNarrative_zip), LTRIM(RTRIM(AccSiteLocNarrative_zip)),
	-- LTRIM(RTRIM(CONCAT_City_County_State)) )
	DECODE(
	    TRUE,
	    CONCAT_City_County_State IS NULL AND AccSiteLocNarrative_zip IS NULL, NULL,
	    CONCAT_City_County_State IS NULL AND AccSiteLocNarrative_zip IS NOT NOT NULL, LTRIM(RTRIM(CONCAT_City_County_State)) || ' ' || LTRIM(RTRIM(AccSiteLocNarrative_zip)),
	    CONCAT_City_County_State IS NULL AND AccSiteLocNarrative_zip IS NOT NULL, LTRIM(RTRIM(AccSiteLocNarrative_zip)),
	    LTRIM(RTRIM(CONCAT_City_County_State))
	) AS CONCAT_City_County_State_Zip,
	-- *INF*: --LTRIM(RTRIM(loss_loc_addr)) || ', ' || LTRIM(RTRIM(loss_loc_city)) || ', ' || LTRIM(RTRIM(v_loss_loc_county)) || ', ' || 
	-- --LTRIM(RTRIM(loss_loc_state)) || ' ' || LTRIM(RTRIM(loss_loc_zip))
	-- 
	-- 
	'' AS v_AccSiteLocationNarrative,
	-- *INF*: --DECODE(TRUE,
	-- --SUBSTR(LTRIM(RTRIM(v_AccSiteLocationNarrative)),1,1)=',',
	-- --SUBSTR(LTRIM(RTRIM(v_AccSiteLocationNarrative)),2),
	-- --LTRIM(RTRIM(v_AccSiteLocationNarrative)) )
	-- 
	'' AS v_AccSiteLocationNarrative_Reformatted,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','IA'),
	-- DECODE(TRUE,
	-- LTRIM(RTRIM(inj_loc_comment))='N/A',CONCAT_City_County_State_Zip,
	-- LTRIM(RTRIM(inj_loc_comment)) ),
	-- NULL )
	-- 
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','IA'), DECODE(
	        TRUE,
	        LTRIM(RTRIM(inj_loc_comment)) = 'N/A', CONCAT_City_County_State_Zip,
	        LTRIM(RTRIM(inj_loc_comment))
	    ),
	    NULL
	) AS o_AccSiteLocationNarrative,
	EXP_SOURCE.o_claim_rep_email AS Claim_rep_email,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(Claim_rep_email))='N/A','',
	-- LTRIM(RTRIM(Claim_rep_email)) )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(Claim_rep_email)) = 'N/A', '',
	    LTRIM(RTRIM(Claim_rep_email))
	) AS v_Claim_rep_email,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(jurisdiction_state_code)) = 'IA',LTRIM(RTRIM(v_Claim_rep_email)),
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) = 'IA', LTRIM(RTRIM(v_Claim_rep_email)),
	    NULL
	) AS o_Claim_rep_email,
	EXP_SOURCE.o_claim_rep_full_name AS claim_rep_full_name,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(claim_rep_full_name)) = 'N/A','',
	-- LTRIM(RTRIM(claim_rep_full_name)) )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(claim_rep_full_name)) = 'N/A', '',
	    LTRIM(RTRIM(claim_rep_full_name))
	) AS v_claim_rep_full_name,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(jurisdiction_state_code)) = 'IA',
	-- LTRIM(RTRIM(v_claim_rep_full_name)),
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) = 'IA', LTRIM(RTRIM(v_claim_rep_full_name)),
	    NULL
	) AS o_claim_rep_full_name,
	EXP_SOURCE.o_inj_result_death_ind AS inj_result_death_ind,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','KS','IA','IL','KY'),LTRIM(RTRIM(inj_result_death_ind)),
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','KS','IA','IL','KY'), LTRIM(RTRIM(inj_result_death_ind)),
	    NULL
	) AS o_inj_result_death_ind,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(maint_type_code)),'02','CO'),LTRIM(RTRIM(jurisdiction_state_code)) || LTRIM(RTRIM(emp_id_num)),
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(maint_type_code)) IN ('02','CO'), LTRIM(RTRIM(jurisdiction_state_code)) || LTRIM(RTRIM(emp_id_num)),
	    NULL
	) AS v_EmployeeSecurityID,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN'),v_EmployeeSecurityID,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN'), v_EmployeeSecurityID,
	    NULL
	) AS o_EmployeeSecurityID,
	EXP_SOURCE.o_emp_id_type AS emp_id_type,
	-- *INF*: LTRIM(RTRIM(emp_id_type))
	LTRIM(RTRIM(emp_id_type)) AS v_emp_id_type,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','KS'),v_emp_id_type,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','KS'), v_emp_id_type,
	    NULL
	) AS o_emp_id_type,
	EXP_SOURCE.o_emp_id_num AS emp_id_num,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(emp_id_type))='E', LTRIM(RTRIM(emp_id_num)),
	-- '' )
	-- 
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(emp_id_type)) = 'E', LTRIM(RTRIM(emp_id_num)),
	    ''
	) AS v_emp_id_num,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','KS','IA','KY'),LTRIM(RTRIM(v_emp_id_num)),
	-- NULL )
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','KS','IA','KY'), LTRIM(RTRIM(v_emp_id_num)),
	    NULL
	) AS o_EmployeeJurisdicationID,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(jurisdiction_state_code)) = 'IA',
	-- DECODE(TRUE,
	-- LTRIM(RTRIM(v_emp_id_type)) = 'V', LTRIM(RTRIM(emp_id_num)),
	-- '' ),
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) = 'IA', DECODE(
	        TRUE,
	        LTRIM(RTRIM(v_emp_id_type)) = 'V', LTRIM(RTRIM(emp_id_num)),
	        ''
	    ),
	    NULL
	) AS EmployeeEmploymentVisa,
	-- *INF*: DECODE(TRUE,
	-- 	IN(LTRIM(RTRIM(jurisdiction_state_code)),'IA','KY'),
	-- 		DECODE(TRUE,
	-- 			LTRIM(RTRIM(v_emp_id_type)) = 'G', 
	-- 				LTRIM(RTRIM(emp_id_num)),
	-- 		''),
	-- 	NULL)
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('IA','KY'), DECODE(
	        TRUE,
	        LTRIM(RTRIM(v_emp_id_type)) = 'G', LTRIM(RTRIM(emp_id_num)),
	        ''
	    ),
	    NULL
	) AS EmployeeGreenCard,
	-- *INF*: DECODE(TRUE,
	-- 	IN(LTRIM(RTRIM(jurisdiction_state_code)),'IA'),
	-- 		DECODE(TRUE,
	-- 			LTRIM(RTRIM(v_emp_id_type)) = 'P', 
	-- 				LTRIM(RTRIM(emp_id_num)),
	-- 			''),
	-- 	NULL)
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('IA'), DECODE(
	        TRUE,
	        LTRIM(RTRIM(v_emp_id_type)) = 'P', LTRIM(RTRIM(emp_id_num)),
	        ''
	    ),
	    NULL
	) AS EmployeePassportNumber,
	EXP_SOURCE.o_emplyr_lost_time_notified_date AS emplyr_lost_time_notified_date,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','KS'),emplyr_lost_time_notified_date,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','KS'), emplyr_lost_time_notified_date,
	    NULL
	) AS o_emplyr_lost_time_notified_date,
	EXP_SOURCE.o_salary_paid_ind AS salary_paid_ind,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)), 'IN','WI','IA'),LTRIM(RTRIM(salary_paid_ind)),
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('IN','WI','IA'), LTRIM(RTRIM(salary_paid_ind)),
	    NULL
	) AS o_salary_paid_ind,
	EXP_SOURCE.fringe_bnft_discontinued_amt,
	EXP_SOURCE.o_med_auth_ind AS med_auth_ind,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(jurisdiction_state_code))='MN','', med_auth_ind )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) = 'MN', '',
	    med_auth_ind
	) AS v_med_auth_ind,
	v_med_auth_ind AS o_med_auth_ind,
	EXP_SOURCE.o_education_lvl AS education_lvl,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(jurisdiction_state_code))='MN','',
	-- LTRIM(RTRIM(education_lvl)) )
	-- 
	-- --IIF(LTRIM(RTRIM(jurisdiction_state_code))='MN','',IIF(LTRIM(RTRIM(education_lvl))='N/A','  ',LTRIM(RTRIM(education_lvl))))
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) = 'MN', '',
	    LTRIM(RTRIM(education_lvl))
	) AS v_education_lvl,
	v_education_lvl AS o_education_lvl,
	EXP_SOURCE.o_auth_to_release_ssn_ind AS auth_to_release_ssn_ind,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(jurisdiction_state_code))='MN','',
	-- auth_to_release_ssn_ind )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) = 'MN', '',
	    auth_to_release_ssn_ind
	) AS v_auth_to_release_ssn_ind,
	v_auth_to_release_ssn_ind AS o_auth_to_release_ssn_ind,
	LKP_TO_GET_EMPR_CONTACT_DETAILS.ph_num,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(ph_num),'',
	-- LTRIM(RTRIM(ph_num))='N/A','',
	-- REPLACECHR(0,LTRIM(RTRIM(ph_num)),'-','') )
	DECODE(
	    TRUE,
	    ph_num IS NULL, '',
	    LTRIM(RTRIM(ph_num)) = 'N/A', '',
	    REGEXP_REPLACE(LTRIM(RTRIM(ph_num)),'-','','i')
	) AS v_ph_num,
	LKP_TO_GET_EMPR_CONTACT_DETAILS.ph_extension,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(ph_extension),'',
	-- LTRIM(RTRIM(ph_extension))='N/A','',
	-- LTRIM(RTRIM(ph_extension)) )
	DECODE(
	    TRUE,
	    ph_extension IS NULL, '',
	    LTRIM(RTRIM(ph_extension)) = 'N/A', '',
	    LTRIM(RTRIM(ph_extension))
	) AS v_ph_extension,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(jurisdiction_state_code))='KS',LTRIM(RTRIM(v_ph_num)),
	-- NULL )
	-- 
	-- 
	-- 
	-- --DECODE(TRUE,
	-- --LTRIM(RTRIM(jurisdiction_state_code))='MN','',
	-- --v_ph_num='','',
	-- --LENGTH(v_ph_num) + LENGTH(v_ph_extension) <= 15, (v_ph_num || v_ph_extension),
	-- --v_ph_num )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) = 'KS', LTRIM(RTRIM(v_ph_num)),
	    NULL
	) AS v_EmployerContactBusPhone,
	-- *INF*: LTRIM(RTRIM(v_EmployerContactBusPhone))
	LTRIM(RTRIM(v_EmployerContactBusPhone)) AS o_EmployerContactBusPhone,
	LKP_TO_GET_EMPR_CONTACT_DETAILS.claim_party_full_name AS contact_full_name,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(contact_full_name),'',
	-- LTRIM(RTRIM(contact_full_name))='N/A','',
	-- LTRIM(RTRIM(contact_full_name)) )
	-- 
	-- --LTRIM(RTRIM(jurisdiction_state_code))='MN','',
	DECODE(
	    TRUE,
	    contact_full_name IS NULL, '',
	    LTRIM(RTRIM(contact_full_name)) = 'N/A', '',
	    LTRIM(RTRIM(contact_full_name))
	) AS v_contact_full_name,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(jurisdiction_state_code))='KS',LTRIM(RTRIM(v_contact_full_name)),
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) = 'KS', LTRIM(RTRIM(v_contact_full_name)),
	    NULL
	) AS o_contact_full_name,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','IA','KS','KY'),v_city_emplr ,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','IA','KS','KY'), v_city_emplr,
	    NULL
	) AS o_EmployerMailingCity,
	-- *INF*: IIF(IN(v_EmployerMailingState_by_jurisdiction,'AB','BC','MB','NB','NL','NT','NS','NU','ON','PE','QC','SK','YT'),
	-- 		'CA',
	-- 	NULL)
	IFF(
	    v_EmployerMailingState_by_jurisdiction IN ('AB','BC','MB','NB','NL','NT','NS','NU','ON','PE','QC','SK','YT'),
	    'CA',
	    NULL
	) AS o_EmployerMailingCountry,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','IA','KS','KY'),v_zip_emplr,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','IA','KS','KY'), v_zip_emplr,
	    NULL
	) AS o_EmployerMailingPostal,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','IA','KS','KY'),v_addr_emplr,
	-- NULL  )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','IA','KS','KY'), v_addr_emplr,
	    NULL
	) AS o_EmployerMailingAddress1,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','IA','KS','KY'),v_state_emplr,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','IA','KS','KY'), v_state_emplr,
	    NULL
	) AS v_EmployerMailingState_by_jurisdiction,
	v_EmployerMailingState_by_jurisdiction AS o_EmployerMailingState,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'IA'),v_zip_insured,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('IA'), v_zip_insured,
	    NULL
	) AS o_InsuredPostalCode,
	'I' AS InsuredTypeCode,
	'I' AS InsurerTypeCode,
	EXP_SOURCE.claim_party_role_code,
	EXP_SOURCE.ClaimAdministratorFEIN,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','IL','KS','IA','KY'),ClaimAdministratorFEIN,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','IL','KS','IA','KY'), ClaimAdministratorFEIN,
	    NULL
	) AS o_ClaimAdministratorFEIN,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','KS','IA','KY'),'WEST BEND MUTUAL INSURANCE CO',
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','KS','IA','KY'), 'WEST BEND MUTUAL INSURANCE CO',
	    NULL
	) AS ClaimAdministratorName,
	EXP_SOURCE.o_average_wage AS AverageWage,
	EXP_SOURCE.work_week_days,
	EXP_SOURCE.work_week_type,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(claim_loss_date)) >= '20140101',
	-- DECODE(TRUE,
	-- LTRIM(RTRIM(work_week_type)) = 'F' AND LTRIM(RTRIM(work_week_days))='MTWHF','S',
	-- LTRIM(RTRIM(work_week_type)) = 'F','F',
	-- LTRIM(RTRIM(work_week_type)) = 'V','V',
	-- '' ),
	-- '' )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(claim_loss_date)) >= '20140101', DECODE(
	        TRUE,
	        LTRIM(RTRIM(work_week_type)) = 'F' AND LTRIM(RTRIM(work_week_days)) = 'MTWHF', 'S',
	        LTRIM(RTRIM(work_week_type)) = 'F', 'F',
	        LTRIM(RTRIM(work_week_type)) = 'V', 'V',
	        ''
	    ),
	    ''
	) AS v_work_week_type,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN'),v_work_week_type,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN'), v_work_week_type,
	    NULL
	) AS o_work_week_type,
	-- *INF*: DECODE(TRUE,
	-- INSTR(LTRIM(RTRIM(work_week_days)),'S') != 0, 'S',
	-- 'N' )
	-- 
	-- --IIF(INSTR(inputValue, valueToSearch)=0, false, true) 
	DECODE(
	    TRUE,
	    REGEXP_INSTR(LTRIM(RTRIM(work_week_days)), 'S') != 0, 'S',
	    'N'
	) AS Sunday,
	-- *INF*: DECODE(TRUE,
	-- INSTR(LTRIM(RTRIM(work_week_days)),'M') != 0, 'S',
	-- 'N' )
	DECODE(
	    TRUE,
	    REGEXP_INSTR(LTRIM(RTRIM(work_week_days)), 'M') != 0, 'S',
	    'N'
	) AS Monday,
	-- *INF*: DECODE(TRUE,
	-- INSTR(LTRIM(RTRIM(work_week_days)),'T') != 0, 'S',
	-- 'N' )
	DECODE(
	    TRUE,
	    REGEXP_INSTR(LTRIM(RTRIM(work_week_days)), 'T') != 0, 'S',
	    'N'
	) AS Tuesday,
	-- *INF*: DECODE(TRUE,
	-- INSTR(LTRIM(RTRIM(work_week_days)),'W') != 0, 'S',
	-- 'N' )
	DECODE(
	    TRUE,
	    REGEXP_INSTR(LTRIM(RTRIM(work_week_days)), 'W') != 0, 'S',
	    'N'
	) AS Wednesday,
	-- *INF*: DECODE(TRUE,
	-- INSTR(LTRIM(RTRIM(work_week_days)),'H') != 0, 'S',
	-- 'N' )
	DECODE(
	    TRUE,
	    REGEXP_INSTR(LTRIM(RTRIM(work_week_days)), 'H') != 0, 'S',
	    'N'
	) AS Thursday,
	-- *INF*: DECODE(TRUE,
	-- INSTR(LTRIM(RTRIM(work_week_days)),'F') != 0, 'S',
	-- 'N' )
	DECODE(
	    TRUE,
	    REGEXP_INSTR(LTRIM(RTRIM(work_week_days)), 'F') != 0, 'S',
	    'N'
	) AS Friday,
	-- *INF*: DECODE(TRUE,
	-- INSTR(LTRIM(RTRIM(work_week_days)),'A') != 0, 'S',
	-- 'N' )
	DECODE(
	    TRUE,
	    REGEXP_INSTR(LTRIM(RTRIM(work_week_days)), 'A') != 0, 'S',
	    'N'
	) AS Saturday,
	-- *INF*: 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- DECODE(TRUE,
	-- LTRIM(RTRIM(claim_loss_date)) >= '20140101',
	-- DECODE(TRUE,
	-- LTRIM(RTRIM(work_week_type)) = 'F',
	-- Sunday || Monday || Tuesday || Wednesday || Thursday || Friday || Saturday ,
	-- '' ),
	-- '' )
	-- 
	-- 
	-- --DECODE(TRUE,
	-- --LTRIM(RTRIM(v_work_week_type)) = 'F','SSSSSSS',
	-- --IN(LTRIM(RTRIM(v_work_week_type)),'S','V','NNNNNNN' ),
	-- --'' )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(claim_loss_date)) >= '20140101', DECODE(
	        TRUE,
	        LTRIM(RTRIM(work_week_type)) = 'F', Sunday || Monday || Tuesday || Wednesday || Thursday || Friday || Saturday,
	        ''
	    ),
	    ''
	) AS v_WorkDaysScheduledCode,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN'),LTRIM(RTRIM(v_WorkDaysScheduledCode)),
	-- NULL )
	-- 
	-- 
	-- 
	-- 
	-- --Sunday || Monday || Tuesday || Wednesday || Thursday || Friday || Saturday,
	-- --NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN'), LTRIM(RTRIM(v_WorkDaysScheduledCode)),
	    NULL
	) AS o_WorkDaysScheduledCode,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','MO','KY'),
	-- DECODE(LTRIM(RTRIM(premises_code)),
	-- 'E','E',
	-- 'L','L',
	-- 'O','X',
	-- '' ),
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'KS','IA','TN'),
	-- DECODE(LTRIM(RTRIM(premises_code)),
	-- 'E','E',
	-- 'L','L',
	-- 'X' ),
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','MO','KY'), DECODE(
	        LTRIM(RTRIM(premises_code)),
	        'E', 'E',
	        'L', 'L',
	        'O', 'X',
	        ''
	    ),
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('KS','IA','TN'), DECODE(
	        LTRIM(RTRIM(premises_code)),
	        'E', 'E',
	        'L', 'L',
	        'X'
	    ),
	    NULL
	) AS v_AccidentPremisesCode,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','KS','IA','MO','TN','KY'),v_AccidentPremisesCode,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','KS','IA','MO','TN','KY'), v_AccidentPremisesCode,
	    NULL
	) AS o_AccidentPremisesCode,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(jurisdiction_state_code))='MN','',
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) = 'MN', '',
	    NULL
	) AS o_InsolventInsurerFEIN,
	EXP_SOURCE.o_sic_code AS sic_code,
	-- *INF*: DECODE(TRUE,
	-- 	LTRIM(RTRIM(sic_code)) = '',
	-- 		'',
	-- 	IN(LTRIM(RTRIM(jurisdiction_state_code)),'IA'),
	-- 		SUBSTR(LTRIM(RTRIM(sic_code)),1,4),
	-- 	CONCAT(SUBSTR(LTRIM(RTRIM(sic_code)),1,4),'SC')
	-- 	)
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(sic_code)) = '', '',
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('IA'), SUBSTR(LTRIM(RTRIM(sic_code)), 1, 4),
	    CONCAT(SUBSTR(LTRIM(RTRIM(sic_code)), 1, 4), 'SC')
	) AS v_sic_code,
	EXP_SOURCE.o_NaicsCode AS NaicsCode,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'IL','WI','IA'),LTRIM(RTRIM(v_sic_code)),
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','KS','KY'),NaicsCode,
	-- LTRIM(RTRIM(jurisdiction_state_code))='MO',
	-- DECODE(TRUE,
	-- LTRIM(RTRIM(NaicsCode)) != '',LTRIM(RTRIM(NaicsCode)),
	-- LTRIM(RTRIM(v_sic_code)) != '',LTRIM(RTRIM(v_sic_code)),
	-- '' ),
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('IL','WI','IA'), LTRIM(RTRIM(v_sic_code)),
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','KS','KY'), NaicsCode,
	    LTRIM(RTRIM(jurisdiction_state_code)) = 'MO', DECODE(
	        TRUE,
	        LTRIM(RTRIM(NaicsCode)) != '', LTRIM(RTRIM(NaicsCode)),
	        LTRIM(RTRIM(v_sic_code)) != '', LTRIM(RTRIM(v_sic_code)),
	        ''
	    ),
	    NULL
	) AS o_NaicsCode,
	EXP_SOURCE.phys_restriction_ind,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'KS'),
	-- DECODE(TRUE,
	-- ISNULL(min_return_to_work_date),'',
	-- TO_CHAR(min_return_to_work_date,'YYYYMMDD') != '18000101' AND  LTRIM(RTRIM(return_to_work_type))  != '',
	-- LTRIM(RTRIM(phys_restriction_ind)), 
	-- '' ), 
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('KS'), DECODE(
	        TRUE,
	        min_return_to_work_date IS NULL, '',
	        TO_CHAR(min_return_to_work_date, 'YYYYMMDD') != '18000101' AND LTRIM(RTRIM(return_to_work_type)) != '', LTRIM(RTRIM(phys_restriction_ind)),
	        ''
	    ),
	    NULL
	) AS o_phys_restriction_ind,
	EXP_SOURCE.emp_security_id,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(jurisdiction_state_code))='MN',LTRIM(RTRIM(emp_security_id)),
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) = 'MN', LTRIM(RTRIM(emp_security_id)),
	    NULL
	) AS o_emp_security_id,
	EXP_SOURCE.act_status_code,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(maint_type_code)) = '04' AND IN(LTRIM(RTRIM(act_status_code)),'DM','DN'),claim_loss_date,
	--  '' )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(maint_type_code)) = '04' AND LTRIM(RTRIM(act_status_code)) IN ('DM','DN'), claim_loss_date,
	    ''
	) AS v_FullDenialEffectiveDate,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'KS','IA','KY'),
	-- v_FullDenialEffectiveDate,
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('KS','IA','KY'), v_FullDenialEffectiveDate,
	    NULL
	) AS o_FullDenialEffectiveDate,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'IA') ,'',
	-- NULL)
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('IA'), '',
	    NULL
	) AS o_SelfInsurerOrgTypeCode,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'IA') ,'',
	-- NULL)
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('IA'), '',
	    NULL
	) AS o_SelfInsurerAuthTypeCode,
	EXP_SOURCE.o_FullDenialReasonCode AS FullDenialReasonCode,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'KS','IA','KY'),
	-- DECODE(TRUE,
	-- LTRIM(RTRIM(maint_type_code)) = '04' AND IN(LTRIM(RTRIM(act_status_code)),'DM','DN'),LTRIM(RTRIM(FullDenialReasonCode)),
	-- '' ),
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('KS','IA','KY'), DECODE(
	        TRUE,
	        LTRIM(RTRIM(maint_type_code)) = '04' AND LTRIM(RTRIM(act_status_code)) IN ('DM','DN'), LTRIM(RTRIM(FullDenialReasonCode)),
	        ''
	    ),
	    NULL
	) AS o_FullDenialReasonCode,
	EXP_SOURCE.o_IAIABCLossTypeCode AS IAIABCLossTypeCode,
	-- *INF*: DECODE(TRUE,
	-- 	IN(LTRIM(RTRIM(jurisdiction_state_code)),'KS','KY'),
	-- 		LTRIM(RTRIM(IAIABCLossTypeCode)),
	-- 	NULL)
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('KS','KY'), LTRIM(RTRIM(IAIABCLossTypeCode)),
	    NULL
	) AS o_IAIABCLossTypeCode,
	EXP_SOURCE.FROILateReasonCode,
	-- *INF*: DECODE(TRUE,
	-- 	IN(LTRIM(RTRIM(jurisdiction_state_code)),'IA','KY'),
	-- 		LTRIM(RTRIM(FROILateReasonCode)),
	-- 	NULL)
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('IA','KY'), LTRIM(RTRIM(FROILateReasonCode)),
	    NULL
	) AS o_FROILateReasonCode,
	WorkNcciMitchell_Get_MaintCode.MaintenanceTypeCode AS lkp_MaintenanceTypeCode,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_MaintenanceTypeCode),'',
	-- lkp_MaintenanceTypeCode )
	DECODE(
	    TRUE,
	    lkp_MaintenanceTypeCode IS NULL, '',
	    lkp_MaintenanceTypeCode
	) AS v_MaintenanceTypeCode,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(jurisdiction_state_code))='KS',
	-- DECODE(TRUE,
	-- LTRIM(RTRIM(maint_type_code)) = 'CO', v_MaintenanceTypeCode,
	-- '' ),
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) = 'KS', DECODE(
	        TRUE,
	        LTRIM(RTRIM(maint_type_code)) = 'CO', v_MaintenanceTypeCode,
	        ''
	    ),
	    NULL
	) AS o_MaintenanceTypeCode,
	WorkNcciMitchell_Get_MaintCode.MaitenanceTypeCodeDate AS lkp_MaitenanceTypeCodeDate,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_MaitenanceTypeCodeDate),'',
	-- lkp_MaitenanceTypeCodeDate )
	DECODE(
	    TRUE,
	    lkp_MaitenanceTypeCodeDate IS NULL, '',
	    lkp_MaitenanceTypeCodeDate
	) AS v_MaitenanceTypeCodeDate,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(jurisdiction_state_code)) = 'KS' ,
	-- DECODE(TRUE,
	-- LTRIM(RTRIM(maint_type_code)) = 'CO', v_MaitenanceTypeCodeDate,
	-- '' ),
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) = 'KS', DECODE(
	        TRUE,
	        LTRIM(RTRIM(maint_type_code)) = 'CO', v_MaitenanceTypeCodeDate,
	        ''
	    ),
	    NULL
	) AS o_MaitenanceTypeCodeDate,
	-- *INF*: DECODE(TRUE,
	-- 	IN(LTRIM(RTRIM(jurisdiction_state_code)),'KS','KY'),
	-- 		DECODE(TRUE,
	-- 			IN(LTRIM(RTRIM(maint_type_code)), '00','AU','04'),
	-- 				v_tax_fed_id_insured,
	-- 			''),
	-- 	IN(LTRIM(RTRIM(jurisdiction_state_code)),'IA'),
	-- 		v_tax_fed_id_insured,
	-- 	NULL)
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('KS','KY'), DECODE(
	        TRUE,
	        LTRIM(RTRIM(maint_type_code)) IN ('00','AU','04'), v_tax_fed_id_insured,
	        ''
	    ),
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('IA'), v_tax_fed_id_insured,
	    NULL
	) AS o_InsuredFEIN,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(maint_type_code)),'00','04','AU','UR')  AND NOT ISNULL(emp_tax_ssn_id) AND ltrim(rtrim(emp_tax_ssn_id))  != 'N/A','S',
	-- IN(LTRIM(RTRIM(maint_type_code)),'02','CO')  AND NOT ISNULL(EmployeeJurisdicationID)  AND ISNULL(v_EmployeeSecurityID),'A',
	-- '')
	-- 
	-- 
	-- 
	-- 
	-- --Must = 'S' when MTC codes IN('00', '04', AU', 'UR') and DN0042 exists
	-- --Must = 'A' when MTC codes IN('02', 'CO') and DN0154 exists and DN0206 is blank
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(maint_type_code)) IN ('00','04','AU','UR') AND emp_tax_ssn_id IS NULL AND ltrim(rtrim(emp_tax_ssn_id)) != 'NOT N/A', 'S',
	    LTRIM(RTRIM(maint_type_code)) IN ('02','CO') AND EmployeeJurisdicationID IS NULL AND v_EmployeeSecurityID IS NOT NULL, 'A',
	    ''
	) AS v_EmployeeIDTypeQualifier,
	-- *INF*: v_EmployeeIDTypeQualifier
	-- 
	-- 
	-- 
	-- 
	-- --Must = 'S' when MTC codes IN('00', '04', AU', 'UR') and DN0042 exists
	-- --Must = 'A' when MTC codes IN('02', 'CO') and DN0154 exists and DN0206 is blank
	v_EmployeeIDTypeQualifier AS o_EmployeeIDTypeQualifier,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'WI'),'',
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('WI'), '',
	    NULL
	) AS Filler,
	LKP_CLT_ref_relation_stage.cirf_ref_id AS in_cirf_ref_id,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(in_cirf_ref_id),v_tax_fed_id_emplr,
	-- LTRIM(RTRIM(in_cirf_ref_id)) = 'N/A',v_tax_fed_id_emplr,
	-- IS_SPACES(LTRIM(RTRIM(in_cirf_ref_id)))  OR LENGTH(LTRIM(RTRIM(in_cirf_ref_id)))=0,v_tax_fed_id_emplr,
	-- LTRIM(RTRIM(in_cirf_ref_id))
	-- )
	DECODE(
	    TRUE,
	    in_cirf_ref_id IS NULL, v_tax_fed_id_emplr,
	    LTRIM(RTRIM(in_cirf_ref_id)) = 'N/A', v_tax_fed_id_emplr,
	    LENGTH(LTRIM(RTRIM(in_cirf_ref_id)))>0 AND TRIM(LTRIM(RTRIM(in_cirf_ref_id)))='' OR LENGTH(LTRIM(RTRIM(in_cirf_ref_id))) = 0, v_tax_fed_id_emplr,
	    LTRIM(RTRIM(in_cirf_ref_id))
	) AS v_cirf_ref_id,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(jurisdiction_state_code)),'IA'),LTRIM(RTRIM(v_cirf_ref_id)),
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('IA'), LTRIM(RTRIM(v_cirf_ref_id)),
	    NULL
	) AS o_cirf_ref_id,
	EXP_SOURCE.o_num_of_exemptions AS num_of_exemptions,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(jurisdiction_state_code)) = 'IA',
	-- DECODE(TRUE,
	-- IN(LTRIM(RTRIM(maint_type_code)),'02','04','CO','UI'),
	-- LTRIM(RTRIM(num_of_exemptions)),
	-- '' ),
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) = 'IA', DECODE(
	        TRUE,
	        LTRIM(RTRIM(maint_type_code)) IN ('02','04','CO','UI'), LTRIM(RTRIM(num_of_exemptions)),
	        ''
	    ),
	    NULL
	) AS o_EmpNumEntitledExempt,
	EXP_SOURCE.claim_rep_phone,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(jurisdiction_state_code)) = 'IA',
	-- LTRIM(RTRIM(claim_rep_phone)),
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) = 'IA', LTRIM(RTRIM(claim_rep_phone)),
	    NULL
	) AS v_claim_rep_phone,
	EXP_SOURCE.FROIClaimType,
	-- *INF*: DECODE(TRUE,
	-- 	IN(LTRIM(RTRIM(jurisdiction_state_code)),'MN','KY') AND FROIClaimType<>'N/A',
	-- 		FROIClaimType,
	-- 	'')
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('MN','KY') AND FROIClaimType <> 'N/A', FROIClaimType,
	    ''
	) AS v_FROIClaimType,
	v_FROIClaimType AS O_FROIClaimType,
	-- *INF*: DECODE(TRUE,
	-- IN(v_jurisdiction_state_code,'MN','KS','KY'),'30',
	-- IN(v_jurisdiction_state_code,'IN','WI','MO','IL','NE','TN'),'10',
	-- v_jurisdiction_state_code='IA','20',
	-- NULL)
	DECODE(
	    TRUE,
	    v_jurisdiction_state_code IN ('MN','KS','KY'), '30',
	    v_jurisdiction_state_code IN ('IN','WI','MO','IL','NE','TN'), '10',
	    v_jurisdiction_state_code = 'IA', '20',
	    NULL
	) AS o_VerIDReleaseNumber,
	EXP_SOURCE.FROIFullDenialReasonNarrative,
	-- *INF*: DECODE(TRUE,
	-- 	IN(LTRIM(RTRIM(jurisdiction_state_code)),'KY'),
	-- 		IIF(LTRIM(RTRIM(maint_type_code)) = '04',
	-- 			:UDF.DEFAULT_VALUE_TO_BLANKS(FROIFullDenialReasonNarrative),
	-- 		''),
	-- 	NULL)
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(jurisdiction_state_code)) IN ('KY'), IFF(
	        LTRIM(RTRIM(maint_type_code)) = '04',
	        UDF_DEFAULT_VALUE_TO_BLANKS(FROIFullDenialReasonNarrative),
	        ''
	    ),
	    NULL
	) AS v_FROIFullDenialReasonNarrative,
	v_FROIFullDenialReasonNarrative AS o_FROIFullDenialReasonNarrative
	FROM EXP_SOURCE
	LEFT JOIN LKP_CLASS_CODE_CCD
	ON LKP_CLASS_CODE_CCD.claim_party_occurrence_ak_id = EXP_SOURCE.claim_party_occurrence_ak_id
	LEFT JOIN LKP_CLT_ref_relation_stage
	ON LKP_CLT_ref_relation_stage.client_id = LKP_Claim_Occurrence.claim_party_key AND LKP_CLT_ref_relation_stage.cirf_eff_dt <= EXP_SOURCE.CurrentDate AND LKP_CLT_ref_relation_stage.cirf_exp_dt > EXP_SOURCE.CurrentDate
	LEFT JOIN LKP_Get_Policy_Details
	ON LKP_Get_Policy_Details.pol_key_ak_id = EXP_SOURCE.pol_key_ak_id
	LEFT JOIN LKP_RTW_TYPE_AND_DATE
	ON LKP_RTW_TYPE_AND_DATE.wc_claimant_det_ak_id = EXP_SOURCE.wc_claimant_det_ak_id
	LEFT JOIN LKP_TO_GET_EMPLOYER_DETAILS
	ON LKP_TO_GET_EMPLOYER_DETAILS.claim_party_id = EXP_SOURCE.claim_occurrence_ak_id
	LEFT JOIN LKP_TO_GET_EMPR_CONTACT_DETAILS
	ON LKP_TO_GET_EMPR_CONTACT_DETAILS.claim_party_id = EXP_SOURCE.claim_occurrence_ak_id
	LEFT JOIN LKP_TO_GET_INSURED_DETAILS
	ON LKP_TO_GET_INSURED_DETAILS.claim_party_id = EXP_SOURCE.claim_occurrence_ak_id
	LEFT JOIN LKP_WRK_HISTORY
	ON LKP_WRK_HISTORY.wc_claimant_det_ak_id = EXP_SOURCE.wc_claimant_det_ak_id
	LEFT JOIN LKP_arch_claim_object_stage
	ON LKP_arch_claim_object_stage.cob_client_id = EXP_SOURCE.claim_party_key AND LKP_arch_claim_object_stage.cob_claim_nbr = EXP_SOURCE.claim_occurrence_key
	LEFT JOIN WorkNcciMitchell_Get_MaintCode
	ON WorkNcciMitchell_Get_MaintCode.WCClaimantDetailAKId = EXP_SOURCE.wc_claimant_det_ak_id AND WorkNcciMitchell_Get_MaintCode.CreatedDate = WorkNcciMitchell_Get_Max_Date.CreatedDate
),
EXPTRANS_TGT_PASS AS (
	SELECT
	Createddate AS CreatedDate,
	Modifiedate AS ModifiedDate,
	Audit_ID AS AuditID,
	wc_claimant_det_ak_id AS WCClaimantDetailAKId,
	TransactionSetID,
	o_maint_type_code AS MaintenanceTypeCode,
	o_send_to_state_time AS MaitenanceTypeCodeDate,
	o_jurisdiction_state_code AS JurisdictionStateCode,
	o_state_claim_num AS JurisdictionClaimNumber,
	InsurerFEIN,
	o_InsurerName AS InsurerName,
	IsTPA,
	o_ClaimAdminAddress1 AS ClaimAdminAddress1,
	o_ClaimAdministratorCity AS ClaimAdminCity,
	o_ClaimAdministratorStateCode AS ClaimAdminStateCode,
	o_ClaimAdministratorPostalCode AS ClaimAdminPostalCode,
	o_wc_claimant_num AS ClaimAdminClaimNumber,
	o_tax_fed_id_emplr AS EmployerFEIN,
	o_full_name_insured AS InsuredName,
	o_full_name_emplr AS EmployerName,
	o_addr_emplr AS EmployerPhysicalAddress1,
	o_city_emplr AS EmployerPhysicalCity,
	o_state_emplr AS EmployerPhysicalStateCode,
	o_zip_emplr AS EmployerPhysicalPostalCode,
	o_NaicsCode AS IndustryCode,
	o_emp_dept_num AS InsuredLocationIdentifier,
	o_pol_key AS PolicyNumberIdentifier,
	o_pol_eff_date AS PolicyEffectiveDate,
	o_pol_exp_date AS PolicyExpirationDate,
	claim_loss_date AS DateOfInjury,
	o_claim_loss_time AS TimeOfInjury,
	o_loss_loc_zip AS PostalCodeOfInjuryState,
	nature_inj_code AS NatureOfInjuryCode,
	body_part_code AS PartOfBodyInjuryCode,
	cause_inj_code AS CauseOfInjuryCode,
	o_claim_loss_descript AS AccidentDescriptionCause,
	o_inital_treatment_code AS InitialTreatmentCode,
	o_emplyr_notified_date AS DateReportedToEmployer,
	o_reported_to_carrier_date AS DateReportedToClaimAdmin,
	o_emp_tax_ssn_id AS SocialSecurityNumber,
	emp_claim_party_last_name AS EmployeeLastName,
	emp_claim_party_first_name AS EmployeeFirstName,
	o_emp_claim_party_mid_name AS EmployeeMiddleInitial,
	o_emp_claim_party_addr AS EmployeeAddress1,
	emp_claim_party_city AS EmployeeCity,
	emp_claim_party_state AS EmployeeState,
	o_emp_claim_party_zip AS EmployeePostalCode,
	o_emp_ph_num AS EmployeePhone,
	o_emp_claim_party_birthdate AS EmployeeDateOfBirth,
	o_emp_claim_party_gndr AS GenderCode,
	o_marital_status AS MaritalStatusCode,
	o_num_of_dependents AS NumberOfDependents,
	o_disability_date AS InitialDateDisabilityBegan,
	o_death_date AS EmployeeDateOfDeath,
	o_emplymnt_status_code AS EmploymentStatusCode,
	o_class_code AS ClassCode,
	o_occuptn_descript AS OccupationDescription,
	o_hired_date AS DateOfHire,
	o_wage_period_code AS WagePeriodCode,
	o_emp_day_week AS NumberOfDaysWorked,
	o_emp_last_day_worked AS DateLastDayWorked,
	o_full_pay_inj_day_ind AS FullWagesPaidInd,
	o_return_to_work_date AS DateOfReturnToWork,
	o_CurrentRTWDate AS CurrentReturnToWorkDate,
	o_source_claim_occurrence_status_code AS ClaimStatus,
	o_FROILateReasonCode AS LateReasonCode,
	Test_Prod_Ind AS TestProductionInd,
	o_loss_loc_county AS AccSiteCountyParish,
	o_AccSiteLocationNarrative AS AccSiteLocationNarrative,
	o_AccSite_OrgName AS AccSiteOrganizationName,
	o_loss_loc_city AS AccSiteCity,
	o_loss_loc_addr AS AccSiteStreet,
	o_loss_loc_state AS AccSiteState,
	v_claim_rep_phone AS ClaimAdminClaimRepPhone,
	o_Claim_rep_email AS ClaimAdminClaimRepEmail,
	o_claim_rep_full_name AS ClaimAdminClaimRepName,
	o_inj_result_death_ind AS DeathResultofInjuryCode,
	EmployeeEmploymentVisa,
	EmployeeGreenCard,
	o_EmployeeJurisdicationID AS EmployeeJurisdicationID,
	EmployeePassportNumber,
	o_EmployerContactBusPhone AS EmployerContactBusPhone,
	o_contact_full_name AS EmployerContactName,
	o_EmployerPhysicalCountry AS EmployerPhysicalCountry,
	o_EmployerMailingCity AS EmployerMailingCity,
	o_EmployerMailingCountry AS EmployerMailingCountry,
	o_EmployerMailingPostal AS EmployerMailingPostal,
	o_EmployerMailingAddress1 AS EmployerMailingAddress1,
	o_EmployerMailingState AS EmployerMailingState,
	o_InsuredPostalCode AS InsuredPostalCode,
	InsuredTypeCode,
	InsurerTypeCode,
	o_ClaimAdministratorFEIN AS ClaimAdministratorFEIN,
	ClaimAdministratorName,
	o_return_to_work_type AS ReturnToWorkTypeCode,
	o_FullDenialEffectiveDate AS FullDenialEffectiveDate,
	o_work_week_type AS WorkWeekTypeCode,
	o_WorkDaysScheduledCode AS WorkDaysScheduledCode,
	o_emp_security_id AS EmployeeSecurityID,
	o_EmpNumEntitledExempt AS EmpNumberEntitledExempt,
	o_phys_restriction_ind AS PhysicalRestrictionsInd,
	o_SelfInsurerOrgTypeCode AS SelfInsurerOrgTypeCode,
	o_AccidentPremisesCode AS AccidentPremisesCode,
	o_SelfInsurerAuthTypeCode AS SelfInsurerAuthTypeCode,
	o_emp_claim_party_name_sfx AS EmployeeLastNameSuffix,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(SocialSecurityNumber)) != '' AND LTRIM(RTRIM(EmployeeJurisdicationID)) != '','S',
	-- LTRIM(RTRIM(SocialSecurityNumber)) != '','S',
	-- LTRIM(RTRIM(EmployeeJurisdicationID)) != '','A',
	-- '' )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(SocialSecurityNumber)) != '' AND LTRIM(RTRIM(EmployeeJurisdicationID)) != '', 'S',
	    LTRIM(RTRIM(SocialSecurityNumber)) != '', 'S',
	    LTRIM(RTRIM(EmployeeJurisdicationID)) != '', 'A',
	    ''
	) AS v_EmployeeIDTypeQualifier,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(JurisdictionStateCode)),'MN','IA','KS','KY'),LTRIM(RTRIM(v_EmployeeIDTypeQualifier)),
	-- NULL )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(JurisdictionStateCode)) IN ('MN','IA','KS','KY'), LTRIM(RTRIM(v_EmployeeIDTypeQualifier)),
	    NULL
	) AS EmployeeIDTypeQualifier,
	o_salary_paid_ind AS EmployerPaidSalaryComp,
	o_emplyr_lost_time_notified_date AS DateEmployerHadKnowledgeOfDisability,
	o_avg_wkly_wage AS AverageWage,
	o_IAIABCLossTypeCode AS TypeOfLossCode,
	o_InsolventInsurerFEIN AS InsolventInsurerFEIN,
	o_MaintenanceTypeCode AS MaintenanceTypeCorrectionCode,
	o_MaitenanceTypeCodeDate AS MaintenanceTypeCorrectionCodeDate,
	o_InsuredFEIN AS InsuredFEIN,
	o_FullDenialReasonCode AS FullDenialReasonCode,
	o_cirf_ref_id AS EmployerUINumber,
	Filler,
	O_FROIClaimType AS FROIClaimType,
	o_VerIDReleaseNumber AS VerIDReleaseNumber,
	o_FROIFullDenialReasonNarrative AS FROIFullDenialReasonNarrative
	FROM EXP_STATE_SPECIFIC
),
WorkNcciMitchell AS (
	INSERT INTO WorkNcciMitchell
	(CreatedDate, ModifiedDate, AuditID, WCClaimantDetailAKId, TransactionSetID, MaintenanceTypeCode, MaitenanceTypeCodeDate, JurisdictionStateCode, JurisdictionClaimNumber, InsurerFEIN, InsurerName, IsTPA, ClaimAdminAddress1, ClaimAdminCity, ClaimAdminStateCode, ClaimAdminPostalCode, ClaimAdminClaimNumber, EmployerFEIN, InsuredName, EmployerName, EmployerPhysicalAddress1, EmployerPhysicalAddress2, EmployerPhysicalCity, EmployerPhysicalStateCode, EmployerPhysicalPostalCode, IndustryCode, InsuredLocationIdentifier, PolicyNumberIdentifier, PolicyEffectiveDate, PolicyExpirationDate, DateOfInjury, TimeOfInjury, PostalCodeOfInjuryState, NatureOfInjuryCode, PartOfBodyInjuryCode, CauseOfInjuryCode, AccidentDescriptionCause, InitialTreatmentCode, DateReportedToEmployer, DateReportedToClaimAdmin, SocialSecurityNumber, EmployeeLastName, EmployeeFirstName, EmployeeMiddleInitial, EmployeeAddress1, EmployeeAddress2, EmployeeCity, EmployeeState, EmployeePostalCode, EmployeePhone, EmployeeDateOfBirth, GenderCode, MaritalStatusCode, NumberOfDependents, InitialDateDisabilityBegan, EmployeeDateOfDeath, EmploymentStatusCode, ClassCode, OccupationDescription, DateOfHire, WagePeriodCode, NumberOfDaysWorked, DateLastDayWorked, FullWagesPaidInd, DateOfReturnToWork, CurrentReturnToWorkDate, ClaimStatus, ClaimType, LateReasonCode, TestProductionInd, AccSiteCountyParish, AccSiteLocationNarrative, AccSiteOrganizationName, AccSiteCity, AccSiteStreet, AccSiteState, ClaimAdminClaimRepPhone, ClaimAdminClaimRepEmail, ClaimAdminClaimRepName, DeathResultofInjuryCode, EmployeeEmploymentVisa, EmployeeGreenCard, EmployeeJurisdicationID, EmployeePassportNumber, EmployerContactBusPhone, EmployerContactName, EmployerPhysicalCountry, EmployerMailingCity, EmployerMailingCountry, EmployerMailingPostal, EmployerMailingAddress1, EmployerMailingState, InsuredPostalCode, InsuredTypeCode, InsurerTypeCode, ClaimAdministratorFEIN, ClaimAdministratorName, ReturnToWorkTypeCode, FullDenialReasonNarrative, FullDenialEffectiveDate, WorkWeekTypeCode, WorkDaysScheduledCode, EmployeeSecurityID, EmpNumberEntitledExempt, PhysicalRestrictionsInd, SelfInsurerOrgTypeCode, AccidentPremisesCode, SelfInsurerAuthTypeCode, EmployeeLastNameSuffix, EmployeeIDTypeQualifier, EmployerPaidSalaryComp, DateEmployerHadKnowledgeOfDisability, AverageWage, TypeOfLossCode, InsolventInsurerFEIN, MaintenanceTypeCorrectionCode, MaintenanceTypeCorrectionCodeDate, InsuredFEIN, EmployerUINumber, FullDenialReasonCode, VerIDReleaseNumber, PartOfBodyInjuredLine)
	SELECT 
	CREATEDDATE, 
	MODIFIEDDATE, 
	AUDITID, 
	WCCLAIMANTDETAILAKID, 
	TRANSACTIONSETID, 
	MAINTENANCETYPECODE, 
	MAITENANCETYPECODEDATE, 
	JURISDICTIONSTATECODE, 
	JURISDICTIONCLAIMNUMBER, 
	INSURERFEIN, 
	INSURERNAME, 
	ISTPA, 
	CLAIMADMINADDRESS1, 
	CLAIMADMINCITY, 
	CLAIMADMINSTATECODE, 
	CLAIMADMINPOSTALCODE, 
	CLAIMADMINCLAIMNUMBER, 
	EMPLOYERFEIN, 
	INSUREDNAME, 
	EMPLOYERNAME, 
	EMPLOYERPHYSICALADDRESS1, 
	Filler AS EMPLOYERPHYSICALADDRESS2, 
	EMPLOYERPHYSICALCITY, 
	EMPLOYERPHYSICALSTATECODE, 
	EMPLOYERPHYSICALPOSTALCODE, 
	INDUSTRYCODE, 
	INSUREDLOCATIONIDENTIFIER, 
	POLICYNUMBERIDENTIFIER, 
	POLICYEFFECTIVEDATE, 
	POLICYEXPIRATIONDATE, 
	DATEOFINJURY, 
	TIMEOFINJURY, 
	POSTALCODEOFINJURYSTATE, 
	NATUREOFINJURYCODE, 
	PARTOFBODYINJURYCODE, 
	CAUSEOFINJURYCODE, 
	ACCIDENTDESCRIPTIONCAUSE, 
	INITIALTREATMENTCODE, 
	DATEREPORTEDTOEMPLOYER, 
	DATEREPORTEDTOCLAIMADMIN, 
	SOCIALSECURITYNUMBER, 
	EMPLOYEELASTNAME, 
	EMPLOYEEFIRSTNAME, 
	EMPLOYEEMIDDLEINITIAL, 
	EMPLOYEEADDRESS1, 
	Filler AS EMPLOYEEADDRESS2, 
	EMPLOYEECITY, 
	EMPLOYEESTATE, 
	EMPLOYEEPOSTALCODE, 
	EMPLOYEEPHONE, 
	EMPLOYEEDATEOFBIRTH, 
	GENDERCODE, 
	MARITALSTATUSCODE, 
	NUMBEROFDEPENDENTS, 
	INITIALDATEDISABILITYBEGAN, 
	EMPLOYEEDATEOFDEATH, 
	EMPLOYMENTSTATUSCODE, 
	CLASSCODE, 
	OCCUPATIONDESCRIPTION, 
	DATEOFHIRE, 
	WAGEPERIODCODE, 
	NUMBEROFDAYSWORKED, 
	DATELASTDAYWORKED, 
	FULLWAGESPAIDIND, 
	DATEOFRETURNTOWORK, 
	CURRENTRETURNTOWORKDATE, 
	CLAIMSTATUS, 
	FROIClaimType AS CLAIMTYPE, 
	LATEREASONCODE, 
	TESTPRODUCTIONIND, 
	ACCSITECOUNTYPARISH, 
	ACCSITELOCATIONNARRATIVE, 
	ACCSITEORGANIZATIONNAME, 
	ACCSITECITY, 
	ACCSITESTREET, 
	ACCSITESTATE, 
	CLAIMADMINCLAIMREPPHONE, 
	CLAIMADMINCLAIMREPEMAIL, 
	CLAIMADMINCLAIMREPNAME, 
	DEATHRESULTOFINJURYCODE, 
	EMPLOYEEEMPLOYMENTVISA, 
	EMPLOYEEGREENCARD, 
	EMPLOYEEJURISDICATIONID, 
	EMPLOYEEPASSPORTNUMBER, 
	EMPLOYERCONTACTBUSPHONE, 
	EMPLOYERCONTACTNAME, 
	EMPLOYERPHYSICALCOUNTRY, 
	EMPLOYERMAILINGCITY, 
	EMPLOYERMAILINGCOUNTRY, 
	EMPLOYERMAILINGPOSTAL, 
	EMPLOYERMAILINGADDRESS1, 
	EMPLOYERMAILINGSTATE, 
	INSUREDPOSTALCODE, 
	INSUREDTYPECODE, 
	INSURERTYPECODE, 
	CLAIMADMINISTRATORFEIN, 
	CLAIMADMINISTRATORNAME, 
	RETURNTOWORKTYPECODE, 
	FROIFullDenialReasonNarrative AS FULLDENIALREASONNARRATIVE, 
	FULLDENIALEFFECTIVEDATE, 
	WORKWEEKTYPECODE, 
	WORKDAYSSCHEDULEDCODE, 
	EMPLOYEESECURITYID, 
	EMPNUMBERENTITLEDEXEMPT, 
	PHYSICALRESTRICTIONSIND, 
	SELFINSURERORGTYPECODE, 
	ACCIDENTPREMISESCODE, 
	SELFINSURERAUTHTYPECODE, 
	EMPLOYEELASTNAMESUFFIX, 
	EMPLOYEEIDTYPEQUALIFIER, 
	EMPLOYERPAIDSALARYCOMP, 
	DATEEMPLOYERHADKNOWLEDGEOFDISABILITY, 
	AVERAGEWAGE, 
	TYPEOFLOSSCODE, 
	INSOLVENTINSURERFEIN, 
	MAINTENANCETYPECORRECTIONCODE, 
	MAINTENANCETYPECORRECTIONCODEDATE, 
	INSUREDFEIN, 
	EMPLOYERUINUMBER, 
	FULLDENIALREASONCODE, 
	VERIDRELEASENUMBER, 
	PartOfBodyInjuryCode AS PARTOFBODYINJUREDLINE
	FROM EXPTRANS_TGT_PASS
),
SQ_claim_party AS (
	select
	  b.claim_party_full_name
	  ,b.ph_num
	  ,b.ph_extension
	  ,b.ph_type
	  ,a.source_sys_id
	  ,a.wc_claimant_det_ak_id
	from vw_workers_comp_claimant_detail a with (nolock)
	INNER JOIN claim_party_occurrence clmt with (nolock) 
	   ON a.claim_party_occurrence_ak_id = clmt.claim_party_occurrence_ak_id 
	   and clmt.crrnt_snpsht_flag = 1 and a.crrnt_snpsht_flag = 1
	INNER JOIN claim_party_occurrence witn with (nolock) 
	   ON clmt.claim_occurrence_ak_id = witn.claim_occurrence_ak_id 
	   and witn.crrnt_snpsht_flag = 1 and witn.claim_party_role_code IN ('WITN')
	INNER JOIN vw_claim_party1 b with (nolock) 
	   ON witn.claim_party_ak_id = b.claim_party_ak_id and b.crrnt_snpsht_flag = 1
	left join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkNcciMitchell dfm
	   on a.wc_claimant_num=dfm.ClaimAdminClaimNumber 
	   and a.crrnt_snpsht_flag=1
	   and CONVERT(CHAR(10),a.send_to_state_time,112)=dfm.MaitenanceTypeCodeDate
	WHERE a.crrnt_snpsht_flag = 1 
	and a.jurisdiction_state_code  IN (@{pipeline().parameters.STATE_CODE})
	and a.send_to_state_ind = 'Y'
	and a.created_date >= '@{pipeline().parameters.SELECTION_START_TS}'
	and dfm.ClaimAdminClaimNumber is null
	and a.source_sys_id <> 'PMS'
	and 1=2
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXPTRANS AS (
	SELECT
	wc_claimant_det_ak_id,
	claim_party_full_name,
	-- *INF*: LTRIM(RTRIM(claim_party_full_name)) 
	LTRIM(RTRIM(claim_party_full_name)) AS v_claim_party_full_name,
	v_claim_party_full_name AS o_claim_party_full_name,
	source_sys_id,
	ph_type,
	ph_num,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(ph_num),'',
	-- LTRIM(RTRIM(ph_num))='N/A','',
	-- ph_num )
	DECODE(
	    TRUE,
	    ph_num IS NULL, '',
	    LTRIM(RTRIM(ph_num)) = 'N/A', '',
	    ph_num
	) AS v_ph_num,
	ph_extension,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(ph_extension),'',
	-- LTRIM(RTRIM(ph_extension))='N/A','',
	-- LTRIM(RTRIM(ph_extension)) )
	DECODE(
	    TRUE,
	    ph_extension IS NULL, '',
	    LTRIM(RTRIM(ph_extension)) = 'N/A', '',
	    LTRIM(RTRIM(ph_extension))
	) AS v_ph_extension,
	-- *INF*: DECODE(TRUE,
	-- v_ph_num='','',
	-- LTRIM(RTRIM(source_sys_id))='EXCEED' AND IN(LTRIM(RTRIM(ph_type)),'BS','IW'),DECODE(TRUE,
	-- LENGTH(v_ph_num) + LENGTH(v_ph_extension) <= 15, v_ph_num || v_ph_extension,
	-- v_ph_num ),
	-- '' )
	-- 
	-- 
	-- --DECODE(TRUE,
	-- --LTRIM(RTRIM(source_sys_id))='EXCEED' AND IN(LTRIM(RTRIM(ph_type)),'BS','IW'),v_ph_num||v_ph_extension,
	-- --'' )
	DECODE(
	    TRUE,
	    v_ph_num = '', '',
	    LTRIM(RTRIM(source_sys_id)) = 'EXCEED' AND LTRIM(RTRIM(ph_type)) IN ('BS','IW'), DECODE(
	        TRUE,
	        LENGTH(v_ph_num) + LENGTH(v_ph_extension) <= 15, v_ph_num || v_ph_extension,
	        v_ph_num
	    ),
	    ''
	) AS v_BusinessPhone,
	v_BusinessPhone AS o_BusinessPhone
	FROM SQ_claim_party
),
LKP_WorkNcciMitchell AS (
	SELECT
	WorkNcciMitchellID,
	WCClaimantDetailAKId,
	in_wc_claimant_det_ak_id
	FROM (
		select 
		a.WorkNcciMitchellID as WorkNcciMItchellID
		,a.WCClaimantDetailAKId as WCClaimantDetailAKId
		from WorkNcciMitchell a
		where a.AuditID=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WCClaimantDetailAKId ORDER BY WorkNcciMitchellID) = 1
),
FILTRANS AS (
	SELECT
	LKP_WorkNcciMitchell.WorkNcciMitchellID, 
	LKP_WorkNcciMitchell.WCClaimantDetailAKId, 
	EXPTRANS.wc_claimant_det_ak_id, 
	EXPTRANS.o_claim_party_full_name AS claim_party_full_name, 
	EXPTRANS.o_BusinessPhone AS BusinessPhone
	FROM EXPTRANS
	LEFT JOIN LKP_WorkNcciMitchell
	ON LKP_WorkNcciMitchell.WCClaimantDetailAKId = EXPTRANS.wc_claimant_det_ak_id
	WHERE NOT ISNULL(WorkNcciMitchellID)
),
EXPTRANS1 AS (
	SELECT
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	WCClaimantDetailAKId,
	WorkNcciMitchellID,
	wc_claimant_det_ak_id,
	claim_party_full_name,
	BusinessPhone
	FROM FILTRANS
),
EXPTRANS_TGT_PASSTHROUGH AS (
	SELECT
	CreatedDate,
	ModifiedDate,
	AuditID,
	WCClaimantDetailAKId,
	WorkNcciMitchellID,
	claim_party_full_name,
	BusinessPhone
	FROM EXPTRANS1
),
WorkNcciMitchellWitness AS (
	INSERT INTO WorkNcciMitchellWitness
	(CreatedDate, ModifiedDate, AuditID, WCClaimantDetailAKId, WorkNcciMitchellID, WitnessFullName, WitnessBusinessPhone1)
	SELECT 
	CREATEDDATE, 
	MODIFIEDDATE, 
	AUDITID, 
	WCCLAIMANTDETAILAKID, 
	WORKNCCIMITCHELLID, 
	claim_party_full_name AS WITNESSFULLNAME, 
	BusinessPhone AS WITNESSBUSINESSPHONE1
	FROM EXPTRANS_TGT_PASSTHROUGH
),
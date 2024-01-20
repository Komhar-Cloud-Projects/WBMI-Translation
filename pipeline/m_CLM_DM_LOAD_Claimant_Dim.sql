WITH
LKP_sup_state AS (
	SELECT
	state_descript,
	state_abbrev,
	state_code
	FROM (
		SELECT sup_state.state_descript as state_descript, ltrim(rtrim(sup_state.state_code)) as state_code 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_state
		WHERE crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code ORDER BY state_descript) = 1
),
LKP_CLAIM_ANSWER_ANS AS (
	SELECT
	optn_set_item_val,
	IN_app_context_entity_name,
	claim_party_occurrence_ak_id,
	logical_name,
	app_context_entity_name
	FROM (
		select   a.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id
		              ,a.logical_name                           as logical_name      
		              ,a.app_context_entity_name   as app_context_entity_name 
		               ,a.optn_set_item_val                 as optn_set_item_val             
		FROM (
		select  LTRIM(RTRIM( an.optn_set_item_val ) )                as optn_set_item_val  
		               ,LTRIM(RTRIM( a.display_name  )      )                   as display_name
		              ,LTRIM(RTRIM( ac.app_context_entity_name ))  as app_context_entity_name
		             ,LTRIM(RTRIM( q.logical_name )    )                          as logical_name
		      ,an.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id
		from 
		 claim_answer an
		 ,application a
		 ,question q
		 ,application_context ac
		 where an.question_ak_id = q.question_ak_id
		 AND ac.app_ak_id = a.app_ak_id
		 AND ac.app_context_ak_id = q.app_context_ak_id
		 AND a.display_name  = 'Claims Workers Compensation'
		 ----AND ac.app_context_entity_name  = 'Claim.GeneralCase.Questions'
		 ----AND q.logical_name  = 'NcciDciLossType'
		 AND an.optn_set_item_val <> 'N/A'
		 AND an.crrnt_snpsht_flag = 1
		 AND  a.crrnt_snpsht_flag = 1
		 AND  q.crrnt_snpsht_flag = 1
		 AND  ac.crrnt_snpsht_flag = 1) a
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id,logical_name,app_context_entity_name ORDER BY optn_set_item_val DESC) = 1
),
LKP_CLAIM_ANSWER_QUES AS (
	SELECT
	optn_text,
	claim_party_occurrence_ak_id,
	logical_name,
	app_context_entity_name
	FROM (
		select   a.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id
		               ,a.logical_name                          as logical_name      
		               ,a.app_context_entity_name  as app_context_entity_name 
		                ,a.optn_text                                  as optn_text                          
		
		FROM (
		
		select  LTRIM(RTRIM( an.optn_text ) )                                      as optn_text
		               ,LTRIM(RTRIM( a.display_name  )   )                         as display_name
		              ,LTRIM(RTRIM( ac.app_context_entity_name)    )  as app_context_entity_name
		             ,LTRIM(RTRIM( q.logical_name )   )                              as logical_name
		      ,an.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id
		from 
		 claim_answer an
		 ,application a
		 ,question q
		 ,application_context ac
		 where an.question_ak_id = q.question_ak_id
		 AND ac.app_ak_id = a.app_ak_id
		 AND ac.app_context_ak_id = q.app_context_ak_id
		 AND a.display_name  = 'Claims Workers Compensation'
		 ----AND ac.app_context_entity_name  = 'Claim.GeneralCase.Questions'
		 ----AND q.logical_name  = 'NcciDciLossType'
		 AND an.optn_text <> 'N/A'
		 AND an.crrnt_snpsht_flag = 1
		 AND  a.crrnt_snpsht_flag = 1
		 AND  q.crrnt_snpsht_flag = 1
		 AND  ac.crrnt_snpsht_flag = 1) a
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id,logical_name,app_context_entity_name ORDER BY optn_text DESC) = 1
),
LKP_WORK_HIST_MAX_CREATE_DATE AS (
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
LKP_Work_Jurisdictional_Working_day_five AS (
	SELECT
	five_day_week_count,
	clndr_date
	FROM (
		SELECT a.clndr_date AS clndr_date,
		       a.five_day_week_count as five_day_week_count
		FROM work_jurisdictional_working_day_count a
		where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY clndr_date ORDER BY five_day_week_count DESC) = 1
),
LKP_Work_Jurisdictional_Working_day_six AS (
	SELECT
	six_day_week_count,
	clndr_date
	FROM (
		SELECT a.clndr_date AS clndr_date,
		       a.six_day_week_count as six_day_week_count
		FROM work_jurisdictional_working_day_count a
		where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY clndr_date ORDER BY six_day_week_count DESC) = 1
),
SQ_claimant_dim_sources AS (
	SELECT   --DISTINCT
	CP.CLAIM_PARTY_ID AS CLAIM_PARTY_PK_ID,
	CP.CLAIM_PARTY_FULL_NAME,
	CP.CLAIM_PARTY_FIRST_NAME,
	CP.CLAIM_PARTY_LAST_NAME,
	CP.CLAIM_PARTY_MID_NAME,
	CP.CLAIM_PARTY_ADDR,
	CP.CLAIM_PARTY_CITY,
	CP.CLAIM_PARTY_COUNTY,
	CP.CLAIM_PARTY_STATE,
	CP.CLAIM_PARTY_ZIP,
	CP.ADDR_TYPE,
	CP.TAX_SSN_ID,
	CP.TAX_FED_ID,
	CP.CLAIM_PARTY_BIRTHDATE,
	CP.CLAIM_PARTY_GNDR,
	DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE,
	CPO.CLAIM_PARTY_OCCURRENCE_ID AS CLAIM_PARTY_OCCURRENCE_PK_ID,
	CPO.CLAIM_PARTY_OCCURRENCE_AK_ID,
	CPO.CLAIM_CASE_AK_ID,
	RTRIM(CPO.CLAIM_PARTY_ROLE_CODE) as role_code,
	CPO.CLAIMANT_NUM,
	CPO.DENIAL_DATE,
	CC.CLAIMANT_CALCULATION_ID AS CLAIMANT_CALCULATION_PK_ID,
	CC.CLAIMANT_DATE_TYPE,
	CC.CLAIMANT_DATE,
	CC.CLAIMANT_SUPPLEMENTAL_IND,
	CC.CLAIMANT_FINANCIAL_IND,
	CC.CLAIMANT_RECOVERY_IND,
	CC.CLAIMANT_NOTICE_ONLY_IND,
	CRC_D.CLAIMANT_RESERVE_CALCULATION_ID AS  CLAIMANT_RESERVE_CALC_DIRECT_LOSS_PK_ID,
	CRC_D.RESERVE_DATE_TYPE AS CLAIMANT_DIRECT_LOSS_STATUS_CODE,
	CRC_E.CLAIMANT_RESERVE_CALCULATION_ID AS CLAIMANT_RESERVE_CALC_EXP_PK_ID,
	CRC_E.RESERVE_DATE_TYPE AS CLAIMANT_EXP_STATUS_CODE,
	CRC_B.CLAIMANT_RESERVE_CALCULATION_ID AS CLAIMANT_RESERVE_CALC_SUBROGATION_PK_ID,
	CRC_B.RESERVE_DATE_TYPE AS CLAIMANT_SUBROGATION_STATUS_CODE,
	CRC_S.CLAIMANT_RESERVE_CALCULATION_ID AS CLAIMANT_RESERVE_CALC_SALVAGE_PK_ID,
	CRC_S.RESERVE_DATE_TYPE AS CLAMANT_SALVAGE_STATUS_CODE ,
	CRC_R.CLAIMANT_RESERVE_CALCULATION_ID AS CLAIMANT_RESERVE_CALC_OTHER_RECOVERY_PK_ID,
	CRC_R.RESERVE_DATE_TYPE AS CLAIMANT_OTHER_RECOVERY_STATUS_CODE,
	WCCD.WC_CLAIMANT_DET_ID 
	FROM
	(
	SELECT A.CLAIM_PARTY_AK_ID,B.CLAIM_PARTY_OCCURRENCE_AK_ID,A.EFF_FROM_DATE FROM dbo.CLAIM_PARTY A,dbo.CLAIM_PARTY_OCCURRENCE B WHERE
	A.CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}' AND A.CLAIM_PARTY_AK_ID = B.CLAIM_PARTY_AK_ID
	UNION
	SELECT CLAIM_PARTY_AK_ID,CLAIM_PARTY_OCCURRENCE_AK_ID,EFF_FROM_DATE FROM dbo.CLAIM_PARTY_OCCURRENCE WHERE CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
	UNION
	SELECT B.CLAIM_PARTY_AK_ID,A.CLAIM_PARTY_OCCURRENCE_AK_ID,A.EFF_FROM_DATE FROM dbo.CLAIMANT_CALCULATION A,dbo.CLAIM_PARTY_OCCURRENCE B
	WHERE A.CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}' AND A.CLAIM_PARTY_OCCURRENCE_AK_ID = B.CLAIM_PARTY_OCCURRENCE_AK_ID
	UNION
	SELECT B.CLAIM_PARTY_AK_ID,A.CLAIM_PARTY_OCCURRENCE_AK_ID,A.EFF_FROM_DATE FROM dbo.CLAIMANT_RESERVE_CALCULATION A,dbo.CLAIM_PARTY_OCCURRENCE B
	WHERE A.CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}' AND A.CLAIM_PARTY_OCCURRENCE_AK_ID = B.CLAIM_PARTY_OCCURRENCE_AK_ID
	UNION
	SELECT B.CLAIM_PARTY_AK_ID,A.CLAIM_PARTY_OCCURRENCE_AK_ID,A.EFF_FROM_DATE 
	FROM dbo.WORKERS_COMP_CLAIMANT_DETAIL A,dbo.CLAIM_PARTY_OCCURRENCE B
	WHERE A.CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}' AND A.CLAIM_PARTY_OCCURRENCE_AK_ID = B.CLAIM_PARTY_OCCURRENCE_AK_ID
	--- start new 3/5/2011
	UNION
	SELECT Y.CLAIM_PARTY_AK_ID
	    ,  Y.CLAIM_PARTY_OCCURRENCE_AK_ID
	    , MAX(Y.EFF_FROM_DATE)
	FROM (
	SELECT C.CLAIM_PARTY_AK_ID              CLAIM_PARTY_AK_ID
	      ,B.CLAIM_PARTY_OCCURRENCE_AK_ID   CLAIM_PARTY_OCCURRENCE_AK_ID, 
	      MAX(CP.EFF_FROM_DATE )            EFF_FROM_DATE 
	FROM  WORKERS_COMP_CLAIMANT_DETAIL B,
	      WORKERS_COMP_CLAIMANT_WORK_HISTORY A,
	      CLAIM_PARTY_OCCURRENCE C,
	       claim_party CP 
	WHERE A.CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
	     AND A.WC_CLAIMANT_DET_AK_ID =  B.WC_CLAIMANT_DET_AK_ID 
	      AND C.CLAIM_PARTY_OCCURRENCE_AK_ID =   B.CLAIM_PARTY_OCCURRENCE_AK_ID
	      AND CP.CLAIM_PARTY_AK_ID = C.CLAIM_PARTY_AK_ID  ----- CHANGE
	    --  AND C.CLAIM_PARTY_OCCURRENCE_AK_ID =739442503
	GROUP BY  C.CLAIM_PARTY_AK_ID,B.CLAIM_PARTY_OCCURRENCE_AK_ID
	UNION  --2
	SELECT C.CLAIM_PARTY_AK_ID            CLAIM_PARTY_AK_ID   
	      ,B.CLAIM_PARTY_OCCURRENCE_AK_ID CLAIM_PARTY_OCCURRENCE_AK_ID
	      , MAX(B.EFF_FROM_DATE )          EFF_FROM_DATE
	FROM  WORKERS_COMP_CLAIMANT_DETAIL B,
	    WORKERS_COMP_CLAIMANT_WORK_HISTORY A,
	    CLAIM_PARTY_OCCURRENCE C
	    
	WHERE A.CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
	      AND A.WC_CLAIMANT_DET_AK_ID =  B.WC_CLAIMANT_DET_AK_ID 
	      AND C.CLAIM_PARTY_OCCURRENCE_AK_ID =   B.CLAIM_PARTY_OCCURRENCE_AK_ID      
	     -- AND C.CLAIM_PARTY_OCCURRENCE_AK_ID =739442503
	GROUP BY  C.CLAIM_PARTY_AK_ID,B.CLAIM_PARTY_OCCURRENCE_AK_ID
	UNION  --- 3
	SELECT C.CLAIM_PARTY_AK_ID                 CLAIM_PARTY_AK_ID
	       ,B.CLAIM_PARTY_OCCURRENCE_AK_ID     CLAIM_PARTY_OCCURRENCE_AK_ID
	       ,MAX(C.EFF_FROM_DATE )              EFF_FROM_DATE
	FROM  WORKERS_COMP_CLAIMANT_DETAIL B,
	    WORKERS_COMP_CLAIMANT_WORK_HISTORY A,
	    CLAIM_PARTY_OCCURRENCE C    
	WHERE A.CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
	     AND A.WC_CLAIMANT_DET_AK_ID =  B.WC_CLAIMANT_DET_AK_ID 
	      AND C.CLAIM_PARTY_OCCURRENCE_AK_ID =   B.CLAIM_PARTY_OCCURRENCE_AK_ID      
	   
	GROUP BY  C.CLAIM_PARTY_AK_ID,B.CLAIM_PARTY_OCCURRENCE_AK_ID
	) Y
	GROUP BY Y.CLAIM_PARTY_AK_ID
	      ,  Y.CLAIM_PARTY_OCCURRENCE_AK_ID
	--- start
	UNION
	SELECT Y.CLAIM_PARTY_AK_ID
	    ,  Y.CLAIM_PARTY_OCCURRENCE_AK_ID
	    , MAX(Y.EFF_FROM_DATE)
	FROM 
	(
	SELECT C.CLAIM_PARTY_AK_ID,
	       C.CLAIM_PARTY_OCCURRENCE_AK_ID , 
	       MAX(C.EFF_FROM_DATE)  EFF_FROM_DATE
	FROM         CLAIM_ANSWER A,
	             CLAIM_PARTY_OCCURRENCE C 
	WHERE A.CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
	AND A.CLAIM_PARTY_OCCURRENCE_AK_ID =   C.CLAIM_PARTY_OCCURRENCE_AK_ID  
	GROUP BY C.CLAIM_PARTY_AK_ID,C.CLAIM_PARTY_OCCURRENCE_AK_ID
	UNION
	SELECT C.CLAIM_PARTY_AK_ID,
	       C.CLAIM_PARTY_OCCURRENCE_AK_ID , 
	       MAX(CP.EFF_FROM_DATE)  EFF_FROM_DATE
	FROM         CLAIM_ANSWER A,
	             CLAIM_PARTY_OCCURRENCE C,
	             claim_party CP 
	WHERE A.CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
	AND A.CLAIM_PARTY_OCCURRENCE_AK_ID =   C.CLAIM_PARTY_OCCURRENCE_AK_ID  
	AND CP.CLAIM_PARTY_AK_ID           =   C.CLAIM_PARTY_AK_ID  
	GROUP BY C.CLAIM_PARTY_AK_ID,C.CLAIM_PARTY_OCCURRENCE_AK_ID
	UNION --- 3
	SELECT C.CLAIM_PARTY_AK_ID,
	       B.CLAIM_PARTY_OCCURRENCE_AK_ID ,
	       MAX(B.EFF_FROM_DATE )  EFF_FROM_DATE   
	FROM  WORKERS_COMP_CLAIMANT_DETAIL B,
	      CLAIM_PARTY_OCCURRENCE C,
	      CLAIM_ANSWER AA
	WHERE AA.CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
	      AND C.CLAIM_PARTY_OCCURRENCE_AK_ID  =   B.CLAIM_PARTY_OCCURRENCE_AK_ID
	      AND AA.CLAIM_PARTY_OCCURRENCE_AK_ID =   C.CLAIM_PARTY_OCCURRENCE_AK_ID 
	    GROUP BY  C.CLAIM_PARTY_AK_ID,B.CLAIM_PARTY_OCCURRENCE_AK_ID
	    ) Y
	   GROUP BY Y.CLAIM_PARTY_AK_ID
	      ,  Y.CLAIM_PARTY_OCCURRENCE_AK_ID
	
	 
	--UNION
	--SELECT 
	--C.CLAIM_PARTY_AK_ID,C.CLAIM_PARTY_OCCURRENCE_AK_ID , 
	--MAX(A.CREATED_DATE)  EFF_DATE
	--FROM         CLAIM_ANSWER A,
	 --                   CLAIM_PARTY_OCCURRENCE C 
	--WHERE A.CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
	--AND A.CLAIM_PARTY_OCCURRENCE_AK_ID =   C.CLAIM_PARTY_OCCURRENCE_AK_ID 
	--GROUP BY C.CLAIM_PARTY_AK_ID,C.CLAIM_PARTY_OCCURRENCE_AK_ID
	--- end new 3/5/2011
	) AS DISTINCT_EFF_FROM_DATES
	
	LEFT OUTER JOIN dbo.CLAIM_PARTY CP ON DISTINCT_EFF_FROM_DATES.CLAIM_PARTY_AK_ID = CP.CLAIM_PARTY_AK_ID AND
	DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN CP.EFF_FROM_DATE AND CP.EFF_TO_DATE
	
	LEFT OUTER JOIN dbo.CLAIM_PARTY_OCCURRENCE CPO ON DISTINCT_EFF_FROM_DATES.CLAIM_PARTY_AK_ID = CPO.CLAIM_PARTY_AK_ID AND
	DISTINCT_EFF_FROM_DATES.CLAIM_PARTY_OCCURRENCE_AK_ID = CPO.CLAIM_PARTY_OCCURRENCE_AK_ID AND
	DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN CPO.EFF_FROM_DATE AND CPO.EFF_TO_DATE
	
	LEFT OUTER JOIN dbo.CLAIMANT_RESERVE_CALCULATION CRC_D ON DISTINCT_EFF_FROM_DATES.CLAIM_PARTY_OCCURRENCE_AK_ID=CRC_D.CLAIM_PARTY_OCCURRENCE_AK_ID AND DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN CRC_D.EFF_FROM_DATE AND CRC_D.EFF_TO_DATE
	AND CRC_D.FINANCIAL_TYPE_CODE = 'D'
	
	LEFT OUTER JOIN dbo.CLAIMANT_RESERVE_CALCULATION CRC_E ON DISTINCT_EFF_FROM_DATES.CLAIM_PARTY_OCCURRENCE_AK_ID=CRC_E.CLAIM_PARTY_OCCURRENCE_AK_ID AND DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN CRC_E.EFF_FROM_DATE AND CRC_E.EFF_TO_DATE
	AND CRC_E.FINANCIAL_TYPE_CODE = 'E'
	
	LEFT OUTER JOIN dbo.CLAIMANT_RESERVE_CALCULATION CRC_B ON DISTINCT_EFF_FROM_DATES.CLAIM_PARTY_OCCURRENCE_AK_ID=CRC_B.CLAIM_PARTY_OCCURRENCE_AK_ID AND DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN CRC_B.EFF_FROM_DATE AND CRC_B.EFF_TO_DATE
	AND CRC_B.FINANCIAL_TYPE_CODE = 'B'
	
	LEFT OUTER JOIN dbo.CLAIMANT_RESERVE_CALCULATION CRC_S ON DISTINCT_EFF_FROM_DATES.CLAIM_PARTY_OCCURRENCE_AK_ID=CRC_S.CLAIM_PARTY_OCCURRENCE_AK_ID AND DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN CRC_S.EFF_FROM_DATE AND CRC_S.EFF_TO_DATE
	AND CRC_S.FINANCIAL_TYPE_CODE = 'S'
	
	LEFT OUTER JOIN dbo.CLAIMANT_RESERVE_CALCULATION CRC_R ON DISTINCT_EFF_FROM_DATES.CLAIM_PARTY_OCCURRENCE_AK_ID=CRC_R.CLAIM_PARTY_OCCURRENCE_AK_ID AND DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN CRC_R.EFF_FROM_DATE AND CRC_R.EFF_TO_DATE
	AND CRC_R.FINANCIAL_TYPE_CODE = 'R'
	
	LEFT OUTER JOIN dbo.CLAIMANT_CALCULATION CC ON DISTINCT_EFF_FROM_DATES.CLAIM_PARTY_OCCURRENCE_AK_ID=CC.CLAIM_PARTY_OCCURRENCE_AK_ID AND
	DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN CC.EFF_FROM_DATE AND CC.EFF_TO_DATE
	
	LEFT OUTER JOIN dbo.WORKERS_COMP_CLAIMANT_DETAIL WCCD ON 
	DISTINCT_EFF_FROM_DATES.CLAIM_PARTY_OCCURRENCE_AK_ID=WCCD.CLAIM_PARTY_OCCURRENCE_AK_ID 
	AND DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN WCCD.EFF_FROM_DATE AND WCCD.EFF_TO_DATE
	
	WHERE RTRIM(CPO.CLAIM_PARTY_ROLE_CODE) IN ('CLMT','CMT')
),
SQ_workers_comp_claimant_detail AS (
	SELECT DISTINCT
	    
	     CD.WC_CLAIMANT_DET_ID,
		 CD.WC_CLAIMANT_DET_AK_ID,
		 CD.CLAIM_PARTY_OCCURRENCE_AK_ID,
		 RTRIM(CD.JURISDICTION_STATE_CODE),
		 CD.EMPLYR_NOTIFIED_DATE,
		 CD.REPORTED_TO_CARRIER_DATE,
		 CD.JURISDICTION_CLAIM_NUM,
		 CD.CARE_DIRECTED_IND,
		 RTRIM(CD.CARE_DIRECTED_BY),
		 CD.HIRED_STATE_CODE,
		 CD.HIRED_DATE,
		 RTRIM(CD.TAX_FILING_STATUS),
		 RTRIM(CD.OCCUPTN_CODE),
		 RTRIM(CD.EMPLYMNT_STATUS_CODE),
		 CD.LEN_OF_TIME_IN_CRRNT_JOB,
		 CD.EMP_DEPT_NAME,
		 CD.EMP_SHIFT_NUM,
		 RTRIM(CD.MARITAL_STATUS),
		 CD.NUM_OF_DEPENDENTS,
		 CD.NUM_OF_DEPENDENT_CHILDREN,
		 CD.NUM_OF_OTHER_DEPENDENTS,
		 CD.NUM_OF_EXEMPTIONS,
		 RTRIM(CD.EXEMPTION_TYPE),
		 CD.EMP_BLIND_IND,
		 CD.EMP_OVER_65_IND,
		 CD.SPOUSE_BLIND_IND,
		 CD.SPOUSE_OVER_65_IND,
		 CD.EDUCATION_LVL,
		 CD.MED_AUTH_IND,
		 CD.AUTH_TO_RELEASE_SSN_IND,
		 CD.EMP_ID_NUM,
		 RTRIM(CD.EMP_ID_TYPE),
		 CD.EMP_PART_TIME_HOUR_WEEK,
		 CD.EMP_DEPT_NUM,
		 CD.EMP_PART_TIME_HOURLY_WEEK_RATE_AMT,
		 CD.WAGE_RATE_AMT,
		 RTRIM(CD.WAGE_PERIOD_CODE),
		 CD.WAGE_EFF_DATE,
		 CD.WEEKS_WORKED,
		 RTRIM(CD.GROSS_AMT_TYPE),
		 CD.GROSS_WAGE_AMT_EXCLUDING_TIPS,
		 CD.PIECE_WORK_NUM_OF_WEEKS_EXCLUDING_OVERTIME,
		 CD.EMP_REC_MEALS,
		 CD.EMP_REC_ROOM,
		 CD.EMP_REC_TIPS,
		 CD.OVERTIME_AMT,
		 CD.OVERTIME_AFTER_HOUR_IN_A_WEEK,
		 CD.OVERTIME_AFTER_HOUR_IN_A_DAY,
		 CD.FULL_PAY_INJ_DAY_IND,
		 CD.SALARY_PAID_IND,
		 CD.AVG_FULL_TIME_DAYS_WEEK,
		 CD.AVG_FULL_TIME_HOURS_DAY,
		 CD.AVG_FULL_TIME_HOURS_WEEK,
		 CD.AVG_WKLY_WAGE,
		 CD.NUM_OF_FULL_TIME_EMPLYMNT_SAME_JOB,
		 CD.NUM_OF_PART_TIME_EMPLYMNT_SAME_JOB,
		 CD.TTD_RATE,
		 CD.PPD_RATE,
		 CD.PTD_RATE,
		 CD.DTD_RATE,
		 CD.WKLY_ATTORNEY_FEE,
		 CD.FIRST_RPT_INJ_DATE,
		 CD.SUPPLEMENTARY_RPT_INJ_DATE,
		 CD.FRINGE_BNFT_DISCONTINUED_AMT,
		 CD.EMP_START_TIME,
		 CD.EMP_HOUR_DAY,
		 CD.EMP_HOUR_WEEK,
		 CD.EMP_DAY_WEEK,
		 CD.INJ_WORK_DAY_BEGIN_TIME,
		 CD.DISABILITY_DATE,
		 CD.PHYS_RESTRICTION_IND,
		 CD.PRE_EXST_DISABILITY_IND,
		 RTRIM(CD.PREMISES_CODE),
		 CD.WORK_PROCESS_DESCRIPT,
		 CD.TASK_DESCRIPT,
		 RTRIM(CD.BODY_PART_CODE),
		 RTRIM(CD.NATURE_INJ_CODE),
		 RTRIM(CD.CAUSE_INJ_CODE),
		 CD.SAFEGUARD_NOT_USED_IND,
		 CD.INJ_SUBSTANCE_ABUSE_IND,
		 CD.SFTY_DEVICE_NOT_USED_IND,
		 CD.INJ_RULES_NOT_OBEYED_IND,
		 CD.INJ_RESULT_OCCUPATIONAL_INJ_IND,
		 CD.INJ_RESULT_OCCUPATIONAL_DISEASE_IND,
		 CD.INJ_RESULT_DEATH_IND,
		 CD.UNSAFE_ACT_DESCRIPT,
		 CD.RESPONSIBLE_FOR_INJ_DESCRIPT,
		 CD.HAZARD_CONDITION_DESCRIPT,
		 --new start
		 --WCCWH.EMP_LAST_DAY_WORKED,
		 CD.DEATH_DATE,
		 ---WCCWH.RETURN_TO_WORK_DATE,
		---RTRIM(WCCWH.RETURN_TO_WORK_TYPE),
		--- WCCWH.RETURN_TO_WORK_WITH_SAME_EMPLYR_IND,
		 ---new end
		 CD.EMPLYR_NATURE_BUS_DESCRIPT,
		 RTRIM(CD.EMPLYR_TYPE_CODE),
		 RTRIM(CD.INSD_TYPE_CODE),
		 CD.SUBROGATION_STATUTE_EXP_DATE,
		 RTRIM(CD.MANAGED_CARE_ORG_TYPE),
		 CD.SUBROGATION_CODE,
		 RTRIM(CD.LOSS_CONDITION),
		 CD.ATTORNEY_OR_AU_REP_IND,
		 CD.HOSPITAL_COST,
		 CD.DOCTOR_COST,
		 CD.OTHER_MED_COST,
		 CD.CONTROVERTED_CASE_CODE,
		 CD.SURGERY_IND,
		 CD.EMPLYR_LOC_DESCRIPT,
		 CD.INJ_LOC_COMMENT,
		 RTRIM(CD.CLAIM_CTGRY_CODE),
		 RTRIM(CD.ACT_STATUS_CODE),
		 CD.INVESTIGATE_IND,
		 RTRIM(CD.SIC_CODE),
		 CD.HOSPITALIZED_IND,
		 RTRIM(CD.WAGE_METHOD_CODE),
		 CD.PMS_OCCUPTN_DESCRIPT,
		 CD.PMS_TYPE_DISABILITY,
		 CD.NCCI_TYPE_COV,
		--- WCCWH.WC_CLAIMANT_WORK_HIST_ID,
	--- START NEW --> NCCI
	        CD.WC_CLAIMANT_NUM,
	         CD.MAX_MED_IMPROVEMENT_DATE,
	--- END NEW --> NCCI
		CD.AutomaticAdjudicationClaimIndicator, 
		CD.SupCompensableClaimCode 
	FROM
	(
	SELECT A.CLAIM_PARTY_AK_ID,B.CLAIM_PARTY_OCCURRENCE_AK_ID,A.EFF_FROM_DATE FROM  CLAIM_PARTY A, CLAIM_PARTY_OCCURRENCE B WHERE
	A.CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}' AND A.CLAIM_PARTY_AK_ID = B.CLAIM_PARTY_AK_ID
	UNION
	SELECT CLAIM_PARTY_AK_ID,CLAIM_PARTY_OCCURRENCE_AK_ID,EFF_FROM_DATE FROM  CLAIM_PARTY_OCCURRENCE WHERE CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
	UNION
	SELECT B.CLAIM_PARTY_AK_ID,A.CLAIM_PARTY_OCCURRENCE_AK_ID ,A.EFF_FROM_DATE FROM  CLAIMANT_CALCULATION A, CLAIM_PARTY_OCCURRENCE B
	WHERE A.CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}' AND A.CLAIM_PARTY_OCCURRENCE_AK_ID = B.CLAIM_PARTY_OCCURRENCE_AK_ID
	UNION
	SELECT B.CLAIM_PARTY_AK_ID,A.CLAIM_PARTY_OCCURRENCE_AK_ID ,A.EFF_FROM_DATE FROM  CLAIMANT_RESERVE_CALCULATION A, CLAIM_PARTY_OCCURRENCE B
	WHERE A.CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}' AND A.CLAIM_PARTY_OCCURRENCE_AK_ID = B.CLAIM_PARTY_OCCURRENCE_AK_ID
	UNION
	SELECT B.CLAIM_PARTY_AK_ID,A.CLAIM_PARTY_OCCURRENCE_AK_ID ,A.EFF_FROM_DATE FROM  WORKERS_COMP_CLAIMANT_DETAIL A, CLAIM_PARTY_OCCURRENCE B
	WHERE A.CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}' AND A.CLAIM_PARTY_OCCURRENCE_AK_ID = 
	B.CLAIM_PARTY_OCCURRENCE_AK_ID
	---start new
	UNION
	  SELECT Y.CLAIM_PARTY_AK_ID
	    ,  Y.CLAIM_PARTY_OCCURRENCE_AK_ID
	    , MAX(Y.EFF_FROM_DATE)
	FROM (
	SELECT C.CLAIM_PARTY_AK_ID              CLAIM_PARTY_AK_ID
	      ,B.CLAIM_PARTY_OCCURRENCE_AK_ID   CLAIM_PARTY_OCCURRENCE_AK_ID, 
	      MAX(CP.EFF_FROM_DATE )            EFF_FROM_DATE 
	FROM  WORKERS_COMP_CLAIMANT_DETAIL B,
	      WORKERS_COMP_CLAIMANT_WORK_HISTORY A,
	      CLAIM_PARTY_OCCURRENCE C,
	       claim_party CP 
	WHERE A.CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
	     AND A.WC_CLAIMANT_DET_AK_ID =  B.WC_CLAIMANT_DET_AK_ID 
	      AND C.CLAIM_PARTY_OCCURRENCE_AK_ID =   B.CLAIM_PARTY_OCCURRENCE_AK_ID
	      AND CP.CLAIM_PARTY_AK_ID = C.CLAIM_PARTY_AK_ID  ----- CHANGE
	    --  AND C.CLAIM_PARTY_OCCURRENCE_AK_ID =739442503
	GROUP BY  C.CLAIM_PARTY_AK_ID,B.CLAIM_PARTY_OCCURRENCE_AK_ID
	UNION  --2
	SELECT C.CLAIM_PARTY_AK_ID            CLAIM_PARTY_AK_ID   
	      ,B.CLAIM_PARTY_OCCURRENCE_AK_ID CLAIM_PARTY_OCCURRENCE_AK_ID
	      , MAX(B.EFF_FROM_DATE )          EFF_FROM_DATE
	FROM  WORKERS_COMP_CLAIMANT_DETAIL B,
	    WORKERS_COMP_CLAIMANT_WORK_HISTORY A,
	    CLAIM_PARTY_OCCURRENCE C
	    
	WHERE A.CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
	      AND A.WC_CLAIMANT_DET_AK_ID =  B.WC_CLAIMANT_DET_AK_ID 
	      AND C.CLAIM_PARTY_OCCURRENCE_AK_ID =   B.CLAIM_PARTY_OCCURRENCE_AK_ID      
	     -- AND C.CLAIM_PARTY_OCCURRENCE_AK_ID =739442503
	GROUP BY  C.CLAIM_PARTY_AK_ID,B.CLAIM_PARTY_OCCURRENCE_AK_ID
	UNION  --- 3
	SELECT C.CLAIM_PARTY_AK_ID                 CLAIM_PARTY_AK_ID
	       ,B.CLAIM_PARTY_OCCURRENCE_AK_ID     CLAIM_PARTY_OCCURRENCE_AK_ID
	       ,MAX(C.EFF_FROM_DATE )              EFF_FROM_DATE
	FROM  WORKERS_COMP_CLAIMANT_DETAIL B,
	    WORKERS_COMP_CLAIMANT_WORK_HISTORY A,
	    CLAIM_PARTY_OCCURRENCE C    
	WHERE A.CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
	     AND A.WC_CLAIMANT_DET_AK_ID =  B.WC_CLAIMANT_DET_AK_ID 
	      AND C.CLAIM_PARTY_OCCURRENCE_AK_ID =   B.CLAIM_PARTY_OCCURRENCE_AK_ID      
	   
	GROUP BY  C.CLAIM_PARTY_AK_ID,B.CLAIM_PARTY_OCCURRENCE_AK_ID
	) Y
	GROUP BY Y.CLAIM_PARTY_AK_ID
	      ,  Y.CLAIM_PARTY_OCCURRENCE_AK_ID
	UNION
	SELECT Y.CLAIM_PARTY_AK_ID
	    ,  Y.CLAIM_PARTY_OCCURRENCE_AK_ID
	    , MAX(Y.EFF_FROM_DATE)
	FROM 
	(
	SELECT C.CLAIM_PARTY_AK_ID,
	       C.CLAIM_PARTY_OCCURRENCE_AK_ID , 
	       MAX(C.EFF_FROM_DATE)  EFF_FROM_DATE
	FROM         CLAIM_ANSWER A,
	             CLAIM_PARTY_OCCURRENCE C 
	WHERE A.CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
	AND A.CLAIM_PARTY_OCCURRENCE_AK_ID =   C.CLAIM_PARTY_OCCURRENCE_AK_ID  
	GROUP BY C.CLAIM_PARTY_AK_ID,C.CLAIM_PARTY_OCCURRENCE_AK_ID
	UNION
	SELECT C.CLAIM_PARTY_AK_ID,
	       C.CLAIM_PARTY_OCCURRENCE_AK_ID , 
	       MAX(CP.EFF_FROM_DATE)  EFF_FROM_DATE
	FROM         CLAIM_ANSWER A,
	             CLAIM_PARTY_OCCURRENCE C,
	             claim_party CP 
	WHERE A.CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
	AND A.CLAIM_PARTY_OCCURRENCE_AK_ID =   C.CLAIM_PARTY_OCCURRENCE_AK_ID  
	AND CP.CLAIM_PARTY_AK_ID           =   C.CLAIM_PARTY_AK_ID  
	GROUP BY C.CLAIM_PARTY_AK_ID,C.CLAIM_PARTY_OCCURRENCE_AK_ID
	UNION --- 3
	SELECT C.CLAIM_PARTY_AK_ID,
	       B.CLAIM_PARTY_OCCURRENCE_AK_ID ,
	       MAX(B.EFF_FROM_DATE )  EFF_FROM_DATE   
	FROM  WORKERS_COMP_CLAIMANT_DETAIL B,
	      CLAIM_PARTY_OCCURRENCE C,
	      CLAIM_ANSWER AA
	WHERE AA.CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
	      AND C.CLAIM_PARTY_OCCURRENCE_AK_ID  =   B.CLAIM_PARTY_OCCURRENCE_AK_ID
	      AND AA.CLAIM_PARTY_OCCURRENCE_AK_ID =   C.CLAIM_PARTY_OCCURRENCE_AK_ID 
	    GROUP BY  C.CLAIM_PARTY_AK_ID,B.CLAIM_PARTY_OCCURRENCE_AK_ID
	    ) Y
	   GROUP BY Y.CLAIM_PARTY_AK_ID
	      ,  Y.CLAIM_PARTY_OCCURRENCE_AK_ID
	--UNION
	--SELECT 
	----C.CLAIM_PARTY_AK_ID,C.CLAIM_PARTY_OCCURRENCE_AK_ID , 
	----MAX(A.CREATED_DATE)  EFF_DATE
	--FROM         CLAIM_ANSWER A,
	      --              CLAIM_PARTY_OCCURRENCE C 
	--WHERE A.CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
	--AND A.CLAIM_PARTY_OCCURRENCE_AK_ID =   C.CLAIM_PARTY_OCCURRENCE_AK_ID 
	--AND C.CLAIM_PARTY_OCCURRENCE_AK_ID =   A.CLAIM_PARTY_OCCURRENCE_AK_ID 
	--GROUP BY C.CLAIM_PARTY_AK_ID,C.CLAIM_PARTY_OCCURRENCE_AK_ID
	--SELECT 
	--C.CLAIM_PARTY_AK_ID,B.CLAIM_PARTY_OCCURRENCE_AK_ID ,A.EFF_FROM_DATE 
	--FROM  WORKERS_COMP_CLAIMANT_DETAIL B,
	    --  WORKERS_COMP_CLAIMANT_WORK_HISTORY A,
	   --   CLAIM_PARTY_OCCURRENCE C 
	--WHERE A.CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}' AND A.WC_CLAIMANT_DET_AK_ID = 
	     --             B.WC_CLAIMANT_DET_AK_ID AND C.CLAIM_PARTY_OCCURRENCE_AK_ID =   -----B.CLAIM_PARTY_OCCURRENCE_AK_ID
	 
	) AS DISTINCT_EFF_FROM_DATES
	
	LEFT OUTER JOIN  WORKERS_COMP_CLAIMANT_DETAIL CD
	ON DISTINCT_EFF_FROM_DATES.CLAIM_PARTY_OCCURRENCE_AK_ID = CD.CLAIM_PARTY_OCCURRENCE_AK_ID 
	AND DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN 
	CD.EFF_FROM_DATE AND CD.EFF_TO_DATE
),
JNR_claimant_dim_sources AS (SELECT
	SQ_claimant_dim_sources.claim_party_id, 
	SQ_claimant_dim_sources.claim_party_full_name, 
	SQ_claimant_dim_sources.claim_party_first_name, 
	SQ_claimant_dim_sources.claim_party_last_name, 
	SQ_claimant_dim_sources.claim_party_mid_name, 
	SQ_claimant_dim_sources.claim_party_addr, 
	SQ_claimant_dim_sources.claim_party_city, 
	SQ_claimant_dim_sources.claim_party_county, 
	SQ_claimant_dim_sources.claim_party_state, 
	SQ_claimant_dim_sources.claim_party_zip, 
	SQ_claimant_dim_sources.addr_type, 
	SQ_claimant_dim_sources.tax_ssn_id, 
	SQ_claimant_dim_sources.tax_fed_id, 
	SQ_claimant_dim_sources.claim_party_birthdate, 
	SQ_claimant_dim_sources.claim_party_gndr, 
	SQ_claimant_dim_sources.claim_party_occurrence_id, 
	SQ_claimant_dim_sources.claim_party_occurrence_ak_id, 
	SQ_claimant_dim_sources.claim_case_ak_id, 
	SQ_claimant_dim_sources.claim_party_role_code, 
	SQ_claimant_dim_sources.claimant_num, 
	SQ_claimant_dim_sources.denial_date, 
	SQ_claimant_dim_sources.claimant_calculation_id, 
	SQ_claimant_dim_sources.claimant_date_type, 
	SQ_claimant_dim_sources.claimant_date, 
	SQ_claimant_dim_sources.claimant_supplemental_ind, 
	SQ_claimant_dim_sources.claimant_financial_ind, 
	SQ_claimant_dim_sources.claimant_recovery_ind, 
	SQ_claimant_dim_sources.claimant_notice_only_ind, 
	SQ_claimant_dim_sources.claimant_reserve_calculation_id_D, 
	SQ_claimant_dim_sources.claimant_direct_loss_status_code, 
	SQ_claimant_dim_sources.claimant_reserve_calculation_id_E, 
	SQ_claimant_dim_sources.claim_occurrence_exp_status_code AS claimant_exp_status_code, 
	SQ_claimant_dim_sources.claimant_reserve_calculation_id_B, 
	SQ_claimant_dim_sources.claimant_subrogation_status_code, 
	SQ_claimant_dim_sources.claimant_reserve_calculation_id_S, 
	SQ_claimant_dim_sources.claimant_salvage_status_code, 
	SQ_claimant_dim_sources.claimant_reserve_calculation_id_R, 
	SQ_claimant_dim_sources.claimant_other_recovery_status_code, 
	SQ_claimant_dim_sources.eff_from_date, 
	SQ_workers_comp_claimant_detail.wc_claimant_det_id, 
	SQ_workers_comp_claimant_detail.wc_claimant_det_ak_id, 
	SQ_workers_comp_claimant_detail.claim_party_occurrence_ak_id AS claim_party_occurrence_ak_id_W, 
	SQ_workers_comp_claimant_detail.jurisdiction_state_code, 
	SQ_workers_comp_claimant_detail.emplyr_notified_date, 
	SQ_workers_comp_claimant_detail.rpted_to_carrier_date, 
	SQ_workers_comp_claimant_detail.jurisdiction_claim_num, 
	SQ_workers_comp_claimant_detail.care_directed_ind, 
	SQ_workers_comp_claimant_detail.care_directed_by, 
	SQ_workers_comp_claimant_detail.hired_state_code, 
	SQ_workers_comp_claimant_detail.hired_date, 
	SQ_workers_comp_claimant_detail.tax_filing_status, 
	SQ_workers_comp_claimant_detail.occuptn_code, 
	SQ_workers_comp_claimant_detail.emplymnt_status_code, 
	SQ_workers_comp_claimant_detail.len_of_time_in_crrnt_job, 
	SQ_workers_comp_claimant_detail.emp_dept_name, 
	SQ_workers_comp_claimant_detail.emp_shift_num, 
	SQ_workers_comp_claimant_detail.marital_status, 
	SQ_workers_comp_claimant_detail.num_of_dependents, 
	SQ_workers_comp_claimant_detail.num_of_dependent_children, 
	SQ_workers_comp_claimant_detail.num_of_other_dependents, 
	SQ_workers_comp_claimant_detail.num_of_exemptions, 
	SQ_workers_comp_claimant_detail.exemption_type, 
	SQ_workers_comp_claimant_detail.emp_blind_ind, 
	SQ_workers_comp_claimant_detail.emp_over_65_ind, 
	SQ_workers_comp_claimant_detail.spouse_blind_ind, 
	SQ_workers_comp_claimant_detail.spouse_over_65_ind, 
	SQ_workers_comp_claimant_detail.education_lvl, 
	SQ_workers_comp_claimant_detail.med_auth_ind, 
	SQ_workers_comp_claimant_detail.auth_to_release_ssn_ind, 
	SQ_workers_comp_claimant_detail.emp_id_num, 
	SQ_workers_comp_claimant_detail.emp_id_type, 
	SQ_workers_comp_claimant_detail.emp_part_time_hour_week, 
	SQ_workers_comp_claimant_detail.emp_dept_num, 
	SQ_workers_comp_claimant_detail.emp_part_time_hourly_week_rate_amt, 
	SQ_workers_comp_claimant_detail.wage_rate_amt, 
	SQ_workers_comp_claimant_detail.wage_period_code, 
	SQ_workers_comp_claimant_detail.wage_eff_date, 
	SQ_workers_comp_claimant_detail.weeks_worked, 
	SQ_workers_comp_claimant_detail.gross_amt_type, 
	SQ_workers_comp_claimant_detail.gross_wage_amt_excluding_tips, 
	SQ_workers_comp_claimant_detail.piece_work_num_of_weeks_excluding_overtime, 
	SQ_workers_comp_claimant_detail.emp_rec_meals, 
	SQ_workers_comp_claimant_detail.emp_rec_room, 
	SQ_workers_comp_claimant_detail.emp_rec_tips, 
	SQ_workers_comp_claimant_detail.overtime_amt, 
	SQ_workers_comp_claimant_detail.overtime_after_hour_in_a_week, 
	SQ_workers_comp_claimant_detail.overtime_after_hour_in_a_day, 
	SQ_workers_comp_claimant_detail.full_pay_inj_day_ind, 
	SQ_workers_comp_claimant_detail.salary_paid_ind, 
	SQ_workers_comp_claimant_detail.avg_full_time_days_week, 
	SQ_workers_comp_claimant_detail.avg_full_time_hours_day, 
	SQ_workers_comp_claimant_detail.avg_full_time_hours_week, 
	SQ_workers_comp_claimant_detail.avg_wkly_wage, 
	SQ_workers_comp_claimant_detail.num_of_full_time_emplymnt_same_job, 
	SQ_workers_comp_claimant_detail.num_of_part_time_emplymnt_same_job, 
	SQ_workers_comp_claimant_detail.ttd_rate, 
	SQ_workers_comp_claimant_detail.ppd_rate, 
	SQ_workers_comp_claimant_detail.ptd_rate, 
	SQ_workers_comp_claimant_detail.dtd_rate, 
	SQ_workers_comp_claimant_detail.wkly_attorney_fee, 
	SQ_workers_comp_claimant_detail.first_rpt_inj_date, 
	SQ_workers_comp_claimant_detail.supplementary_rpt_inj_date, 
	SQ_workers_comp_claimant_detail.fringe_bnft_discontinued_amt, 
	SQ_workers_comp_claimant_detail.emp_start_time, 
	SQ_workers_comp_claimant_detail.emp_hour_day, 
	SQ_workers_comp_claimant_detail.emp_hour_week, 
	SQ_workers_comp_claimant_detail.emp_day_week, 
	SQ_workers_comp_claimant_detail.inj_work_day_begin_time, 
	SQ_workers_comp_claimant_detail.disability_date, 
	SQ_workers_comp_claimant_detail.phys_restriction_ind, 
	SQ_workers_comp_claimant_detail.pre_exst_disability_ind, 
	SQ_workers_comp_claimant_detail.premises_code, 
	SQ_workers_comp_claimant_detail.work_process_descript, 
	SQ_workers_comp_claimant_detail.task_descript, 
	SQ_workers_comp_claimant_detail.body_part_code, 
	SQ_workers_comp_claimant_detail.nature_inj_code, 
	SQ_workers_comp_claimant_detail.cause_inj_code, 
	SQ_workers_comp_claimant_detail.safeguard_not_used_ind, 
	SQ_workers_comp_claimant_detail.inj_substance_abuse_ind, 
	SQ_workers_comp_claimant_detail.sfty_device_not_used_ind, 
	SQ_workers_comp_claimant_detail.inj_rules_not_obeyed_ind, 
	SQ_workers_comp_claimant_detail.inj_result_occuptnal_inj_ind, 
	SQ_workers_comp_claimant_detail.inj_result_occuptnal_disease_ndicator, 
	SQ_workers_comp_claimant_detail.inj_result_death_ind, 
	SQ_workers_comp_claimant_detail.unsafe_act_descript, 
	SQ_workers_comp_claimant_detail.responsible_for_inj_descript, 
	SQ_workers_comp_claimant_detail.hazard_condition_descript, 
	SQ_workers_comp_claimant_detail.death_date, 
	SQ_workers_comp_claimant_detail.emplyr_nature_bus_descript, 
	SQ_workers_comp_claimant_detail.emplyr_type_code, 
	SQ_workers_comp_claimant_detail.insd_type_code, 
	SQ_workers_comp_claimant_detail.subrogation_statute_exp_date, 
	SQ_workers_comp_claimant_detail.managed_care_org_type, 
	SQ_workers_comp_claimant_detail.subrogation_code, 
	SQ_workers_comp_claimant_detail.loss_condition, 
	SQ_workers_comp_claimant_detail.attorney_or_au_rep_ind, 
	SQ_workers_comp_claimant_detail.hospital_cost, 
	SQ_workers_comp_claimant_detail.doctor_cost, 
	SQ_workers_comp_claimant_detail.other_med_cost, 
	SQ_workers_comp_claimant_detail.controverted_case_code, 
	SQ_workers_comp_claimant_detail.surgery_ind, 
	SQ_workers_comp_claimant_detail.emplyr_loc_descript, 
	SQ_workers_comp_claimant_detail.inj_loc_comment, 
	SQ_workers_comp_claimant_detail.claim_ctgry_code, 
	SQ_workers_comp_claimant_detail.act_status_code, 
	SQ_workers_comp_claimant_detail.investigate_ind, 
	SQ_workers_comp_claimant_detail.sic_code, 
	SQ_workers_comp_claimant_detail.hospitalized_ind, 
	SQ_workers_comp_claimant_detail.wage_method_code, 
	SQ_workers_comp_claimant_detail.pms_occuptn_descript, 
	SQ_workers_comp_claimant_detail.pms_type_disability, 
	SQ_workers_comp_claimant_detail.ncci_type_cov, 
	SQ_claimant_dim_sources.wc_claimant_det_id AS wc_claimant_det_id1, 
	SQ_workers_comp_claimant_detail.wc_claimant_num, 
	SQ_workers_comp_claimant_detail.max_med_improvement_date, 
	SQ_workers_comp_claimant_detail.AutomaticAdjudicationClaimIndicator, 
	SQ_workers_comp_claimant_detail.SupCompensableClaimCode
	FROM SQ_claimant_dim_sources
	LEFT OUTER JOIN SQ_workers_comp_claimant_detail
	ON SQ_workers_comp_claimant_detail.wc_claimant_det_id = SQ_claimant_dim_sources.wc_claimant_det_id
),
LKP_SUP_WC_ACTIVITY_STATUS AS (
	SELECT
	act_status_code1,
	act_status_code,
	act_status_code_descript
	FROM (
		SELECT 
		sup_workers_comp_activity_status.act_status_code_descript as act_status_code_descript, rtrim(sup_workers_comp_activity_status.act_status_code) as act_status_code 
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_workers_comp_activity_status sup_workers_comp_activity_status
		WHERE
		CRRNT_SNPSHT_FLAG = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY act_status_code ORDER BY act_status_code1) = 1
),
LKP_SUP_WC_CLAIM_CATG AS (
	SELECT
	claim_ctgry_code1,
	claim_ctgry_code,
	claim_ctgry_code_descript
	FROM (
		SELECT 
		sup_workers_comp_claim_category.claim_ctgry_code_descript as claim_ctgry_code_descript, rtrim(sup_workers_comp_claim_category.claim_ctgry_code) as claim_ctgry_code 
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_workers_comp_claim_category sup_workers_comp_claim_category
		WHERE
		CRRNT_SNPSHT_FLAG = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_ctgry_code ORDER BY claim_ctgry_code1) = 1
),
LKP_SUP_WC_SIC_CODE AS (
	SELECT
	sic_code1,
	sic_code,
	sic_code_descript
	FROM (
		SELECT 
		sup_workers_comp_sic_code.sic_code_descript as sic_code_descript, 
		sup_workers_comp_sic_code.sic_code as sic_code 
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_workers_comp_sic_code sup_workers_comp_sic_code
		WHERE
		CRRNT_SNPSHT_FLAG = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sic_code ORDER BY sic_code1) = 1
),
LKP_SUP_WC_WAGE_METHOD AS (
	SELECT
	wage_method_code1,
	wage_method_code,
	wage_method_code_descript
	FROM (
		SELECT 
		sup_workers_comp_wage_method.wage_method_code_descript as wage_method_code_descript, rtrim(sup_workers_comp_wage_method.wage_method_code) as wage_method_code 
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_workers_comp_wage_method sup_workers_comp_wage_method
		WHERE
		CRRNT_SNPSHT_FLAG = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY wage_method_code ORDER BY wage_method_code1) = 1
),
LKP_SupCompensableClaimCode AS (
	SELECT
	CompensableClaimDescription,
	SupCompensableClaimCode
	FROM (
		SELECT 
			CompensableClaimDescription,
			SupCompensableClaimCode
		FROM SupCompensableClaimCode
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SupCompensableClaimCode ORDER BY CompensableClaimDescription) = 1
),
EXP_WORK_HISTORY AS (
	SELECT
	wc_claimant_det_ak_id,
	-- *INF*: :LKP.LKP_WORK_HIST_MAX_CREATE_DATE(wc_claimant_det_ak_id)
	--  
	LKP_WORK_HIST_MAX_CREATE_DATE_wc_claimant_det_ak_id.WORK_HIST_CREATE_DATE AS v_max_created_date,
	v_max_created_date AS o_max_created_date
	FROM JNR_claimant_dim_sources
	LEFT JOIN LKP_WORK_HIST_MAX_CREATE_DATE LKP_WORK_HIST_MAX_CREATE_DATE_wc_claimant_det_ak_id
	ON LKP_WORK_HIST_MAX_CREATE_DATE_wc_claimant_det_ak_id.WC_CLAIMANT_DET_AK_ID = wc_claimant_det_ak_id

),
LKP_Workers_Comp_Claimant_Work_History AS (
	SELECT
	emp_last_day_worked,
	return_to_work_date,
	return_to_work_type,
	return_to_work_with_same_emplyr_ind,
	wc_claimant_work_hist_id,
	wc_claimant_det_ak_id,
	work_hist_created_date
	FROM (
		SELECT  
		a.wc_claimant_det_ak_id                               as wc_claimant_det_ak_id ,
		a.work_hist_created_date                              as work_hist_created_date,
		a.emp_last_day_worked                                 as emp_last_day_worked ,
		a.return_to_work_date                                       as return_to_work_date,
		a.return_to_work_type                                       as return_to_work_type,
		a.return_to_work_with_same_emplyr_ind                        as return_to_work_with_same_emplyr_ind,
		a.wc_claimant_work_hist_id as  wc_claimant_work_hist_id
		 FROM workers_comp_claimant_work_history a
		where a.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY wc_claimant_det_ak_id,work_hist_created_date ORDER BY emp_last_day_worked) = 1
),
EXP_CMT_CLMT AS (
	SELECT
	claim_party_role_code,
	-- *INF*: IIF(ltrim(rtrim(claim_party_role_code))='CMT','CLMT',ltrim(rtrim(claim_party_role_code)))
	IFF(
	    ltrim(rtrim(claim_party_role_code)) = 'CMT', 'CLMT', ltrim(rtrim(claim_party_role_code))
	) AS claim_party_role_code_out
	FROM JNR_claimant_dim_sources
),
LKP_sup_claim_party_role_code AS (
	SELECT
	claim_party_role_descript,
	claim_party_role_code
	FROM (
		SELECT sup_claim_party_role_code.claim_party_role_descript as claim_party_role_descript, rtrim(sup_claim_party_role_code.claim_party_role_code) as claim_party_role_code FROM sup_claim_party_role_code where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_role_code ORDER BY claim_party_role_descript) = 1
),
LKP_sup_insured_type AS (
	SELECT
	insd_type_code,
	insd_type_descript,
	IN_insd_type_code
	FROM (
		SELECT sup_insured_type.sup_insd_type_id as sup_insd_type_id, sup_insured_type.insd_type_descript as insd_type_descript, sup_insured_type.crrnt_snpsht_flag as crrnt_snpsht_flag, rtrim(sup_insured_type.insd_type_code) as insd_type_code FROM sup_insured_type where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY insd_type_code ORDER BY insd_type_code) = 1
),
LKP_sup_marital_status AS (
	SELECT
	marital_status_code,
	marital_status_descript,
	IN_marital_status
	FROM (
		SELECT sup_marital_status.marital_status_descript as marital_status_descript,  rtrim(sup_marital_status.marital_status_code) as marital_status_code FROM sup_marital_status where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY marital_status_code ORDER BY marital_status_code) = 1
),
LKP_sup_tax_filing_status AS (
	SELECT
	tax_filing_status_code,
	tax_filing_status_descript,
	IN_tax_filing_status
	FROM (
		SELECT  sup_tax_filing_status.tax_filing_status_descript as tax_filing_status_descript, rtrim(sup_tax_filing_status.tax_filing_status_code) as tax_filing_status_code FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_tax_filing_status
		where sup_tax_filing_status.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY tax_filing_status_code ORDER BY tax_filing_status_code) = 1
),
LKP_sup_workers_comp_body_part AS (
	SELECT
	body_part_code,
	body_part_descript,
	IN_body_part_code
	FROM (
		SELECT sup_workers_comp_body_part.body_part_descript as body_part_descript, rtrim(sup_workers_comp_body_part.body_part_code) as body_part_code FROM sup_workers_comp_body_part where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY body_part_code ORDER BY body_part_code) = 1
),
LKP_sup_workers_comp_care_directed_by AS (
	SELECT
	wc_care_directed_by_code,
	wc_care_directed_by_descript,
	IN_care_directed_by
	FROM (
		SELECT sup_workers_comp_care_directed_by.wc_care_directed_by_descript as wc_care_directed_by_descript, rtrim(sup_workers_comp_care_directed_by.wc_care_directed_by_code) as wc_care_directed_by_code 
		FROM sup_workers_comp_care_directed_by 
		where sup_workers_comp_care_directed_by.crrnt_snpsht_flag  = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY wc_care_directed_by_code ORDER BY wc_care_directed_by_code) = 1
),
LKP_sup_workers_comp_cause_of_injury AS (
	SELECT
	cause_of_inj_code,
	cause_of_inj_descript,
	IN_cause_inj_code
	FROM (
		SELECT  sup_workers_comp_cause_of_injury.cause_of_inj_descript as cause_of_inj_descript,  rtrim(sup_workers_comp_cause_of_injury.cause_of_inj_code) as cause_of_inj_code FROM sup_workers_comp_cause_of_injury where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY cause_of_inj_code ORDER BY cause_of_inj_code) = 1
),
LKP_sup_workers_comp_employee_identification_type AS (
	SELECT
	emp_id_type,
	emp_id_type_descript,
	IN_emp_id_type
	FROM (
		SELECT sup_workers_comp_employee_identification_type.emp_id_type_descript as emp_id_type_descript, sup_workers_comp_employee_identification_type.emp_id_type as emp_id_type FROM sup_workers_comp_employee_identification_type where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY emp_id_type ORDER BY emp_id_type) = 1
),
LKP_sup_workers_comp_employer_type AS (
	SELECT
	emplyr_type_code,
	emplyr_type_descript,
	IN_emplyr_type_code
	FROM (
		SELECT  sup_workers_comp_employer_type.emplyr_type_descript as emplyr_type_descript,  sup_workers_comp_employer_type.emplyr_type_code as emplyr_type_code FROM sup_workers_comp_employer_type where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY emplyr_type_code ORDER BY emplyr_type_code) = 1
),
LKP_sup_workers_comp_employment_status AS (
	SELECT
	wc_emplymnt_code,
	wc_emplymnt_descript,
	emplymnt_status_code
	FROM (
		SELECT  sup_workers_comp_employment_status.wc_emplymnt_descript as wc_emplymnt_descript,  rtrim(sup_workers_comp_employment_status.wc_emplymnt_code) as wc_emplymnt_code FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_workers_comp_employment_status where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY wc_emplymnt_code ORDER BY wc_emplymnt_code) = 1
),
LKP_sup_workers_comp_exemption_type AS (
	SELECT
	wc_exemption_type_code,
	wc_exemption_type_descript,
	IN_exemption_type
	FROM (
		SELECT sup_workers_comp_exemption_type.wc_exemption_type_descript as wc_exemption_type_descript,  rtrim(sup_workers_comp_exemption_type.wc_exemption_type_code) as wc_exemption_type_code FROM sup_workers_comp_exemption_type
		where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY wc_exemption_type_code ORDER BY wc_exemption_type_code) = 1
),
LKP_sup_workers_comp_loss_condition AS (
	SELECT
	loss_condition_code,
	loss_condition_descript,
	IN_loss_condition
	FROM (
		SELECT sup_workers_comp_loss_condition.loss_condition_type as loss_condition_type, sup_workers_comp_loss_condition.loss_condition_descript as loss_condition_descript, rtrim(sup_workers_comp_loss_condition.loss_condition_code) as loss_condition_code FROM sup_workers_comp_loss_condition where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY loss_condition_code ORDER BY loss_condition_code) = 1
),
LKP_sup_workers_comp_managed_care_organization_type AS (
	SELECT
	managed_care_org_type,
	managed_care_org_type_descript,
	IN_managed_care_org_type
	FROM (
		SELECT sup_workers_comp_managed_care_organization_type.managed_care_org_type_descript as managed_care_org_type_descript,  rtrim(sup_workers_comp_managed_care_organization_type.managed_care_org_type) as managed_care_org_type FROM sup_workers_comp_managed_care_organization_type where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY managed_care_org_type ORDER BY managed_care_org_type) = 1
),
LKP_sup_workers_comp_nature_of_injury AS (
	SELECT
	nature_of_inj_code,
	nature_of_inj_descript,
	IN_nature_inj_code
	FROM (
		SELECT sup_workers_comp_nature_of_injury.nature_of_inj_descript as nature_of_inj_descript, rtrim(sup_workers_comp_nature_of_injury.nature_of_inj_code) as nature_of_inj_code FROM sup_workers_comp_nature_of_injury where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY nature_of_inj_code ORDER BY nature_of_inj_code) = 1
),
LKP_sup_workers_comp_occupation AS (
	SELECT
	occuptn_code,
	occuptn_descript,
	IN_occuptn_code
	FROM (
		SELECT sup_workers_comp_occupation.occuptn_descript as occuptn_descript,  rtrim(sup_workers_comp_occupation.occuptn_code) as occuptn_code FROM sup_workers_comp_occupation where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY occuptn_code ORDER BY occuptn_code) = 1
),
LKP_sup_workers_comp_premises_type AS (
	SELECT
	premises_code,
	premises_descript,
	IN_premises_code
	FROM (
		SELECT  sup_workers_comp_premises_type.premises_descript as premises_descript,  rtrim(sup_workers_comp_premises_type.premises_code) as premises_code FROM sup_workers_comp_premises_type where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY premises_code ORDER BY premises_code) = 1
),
LKP_sup_workers_comp_return_to_work_type AS (
	SELECT
	return_to_work_code,
	return_to_work_descript,
	IN_return_to_work_type
	FROM (
		SELECT sup_workers_comp_return_to_work_type.sup_wc_return_to_work_type_id as sup_wc_return_to_work_type_id, sup_workers_comp_return_to_work_type.return_to_work_descript as return_to_work_descript, rtrim(sup_workers_comp_return_to_work_type.return_to_work_code) as return_to_work_code FROM sup_workers_comp_return_to_work_type where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY return_to_work_code ORDER BY return_to_work_code) = 1
),
LKP_sup_workers_comp_wage_gross_amount_type AS (
	SELECT
	wage_gross_amt_type,
	wage_gross_amt_type_descript,
	IN_gross_amt_type
	FROM (
		SELECT sup_workers_comp_wage_gross_amount_type.wage_gross_amt_type_descript as wage_gross_amt_type_descript,  rtrim(sup_workers_comp_wage_gross_amount_type.wage_gross_amt_type) as wage_gross_amt_type FROM sup_workers_comp_wage_gross_amount_type where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY wage_gross_amt_type ORDER BY wage_gross_amt_type) = 1
),
LKP_sup_workers_comp_wage_period AS (
	SELECT
	wage_period_code,
	wage_period_descript,
	IN_wage_period_code
	FROM (
		SELECT  sup_workers_comp_wage_period.wage_period_descript as wage_period_descript, rtrim(sup_workers_comp_wage_period.wage_period_code) as wage_period_code FROM sup_workers_comp_wage_period where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY wage_period_code ORDER BY wage_period_code) = 1
),
EXP_claimant_dim AS (
	SELECT
	JNR_claimant_dim_sources.claim_party_occurrence_id AS in_edw_claim_party_occurrence_pk_id,
	-- *INF*: iif(isnull(in_edw_claim_party_occurrence_pk_id),
	-- -1,in_edw_claim_party_occurrence_pk_id)
	IFF(in_edw_claim_party_occurrence_pk_id IS NULL, - 1, in_edw_claim_party_occurrence_pk_id) AS edw_claim_party_occurrence_pk_id_out,
	JNR_claimant_dim_sources.claim_party_id AS in_edw_claim_party_pk_id,
	-- *INF*: iif(isnull(in_edw_claim_party_pk_id),-1,in_edw_claim_party_pk_id)
	IFF(in_edw_claim_party_pk_id IS NULL, - 1, in_edw_claim_party_pk_id) AS edw_claim_party_pk_id_out,
	JNR_claimant_dim_sources.claimant_calculation_id AS in_edw_claimant_calculation_pk_id,
	-- *INF*: iif(isnull(in_edw_claimant_calculation_pk_id),-1,in_edw_claimant_calculation_pk_id)
	IFF(in_edw_claimant_calculation_pk_id IS NULL, - 1, in_edw_claimant_calculation_pk_id) AS edw_claimant_calculation_pk_id_out,
	JNR_claimant_dim_sources.claimant_reserve_calculation_id_D AS in_edw_claimant_reserve_calculation_direct_loss_pk_id,
	-- *INF*: iif(isnull(in_edw_claimant_reserve_calculation_direct_loss_pk_id),-1,in_edw_claimant_reserve_calculation_direct_loss_pk_id)
	IFF(
	    in_edw_claimant_reserve_calculation_direct_loss_pk_id IS NULL, - 1,
	    in_edw_claimant_reserve_calculation_direct_loss_pk_id
	) AS edw_claimant_reserve_calculation_direct_loss_pk_id_out,
	JNR_claimant_dim_sources.claimant_reserve_calculation_id_E AS in_edw_claimant_reserve_calculation_exp_pk_id,
	-- *INF*: iif(isnull(in_edw_claimant_reserve_calculation_exp_pk_id),-1,in_edw_claimant_reserve_calculation_exp_pk_id)
	IFF(
	    in_edw_claimant_reserve_calculation_exp_pk_id IS NULL, - 1,
	    in_edw_claimant_reserve_calculation_exp_pk_id
	) AS edw_claimant_reserve_calculation_exp_pk_id_out,
	JNR_claimant_dim_sources.claimant_reserve_calculation_id_B AS in_edw_claimant_reserve_calculation_subrogation_pk_id,
	-- *INF*: iif(isnull(in_edw_claimant_reserve_calculation_subrogation_pk_id),-1,in_edw_claimant_reserve_calculation_subrogation_pk_id)
	IFF(
	    in_edw_claimant_reserve_calculation_subrogation_pk_id IS NULL, - 1,
	    in_edw_claimant_reserve_calculation_subrogation_pk_id
	) AS edw_claimant_reserve_calculation_subrogation_pk_id_out,
	JNR_claimant_dim_sources.claimant_reserve_calculation_id_S AS in_edw_claimant_reserve_calculation_salvage_pk_id,
	-- *INF*: iif(isnull(in_edw_claimant_reserve_calculation_salvage_pk_id),-1,in_edw_claimant_reserve_calculation_salvage_pk_id)
	IFF(
	    in_edw_claimant_reserve_calculation_salvage_pk_id IS NULL, - 1,
	    in_edw_claimant_reserve_calculation_salvage_pk_id
	) AS edw_claimant_reserve_calculation_salvage_pk_id_out,
	JNR_claimant_dim_sources.claimant_reserve_calculation_id_R AS in_edw_claimant_reserve_calculation_other_recovery_pk_id,
	-- *INF*: iif(isnull(in_edw_claimant_reserve_calculation_other_recovery_pk_id)
	-- ,-1,in_edw_claimant_reserve_calculation_other_recovery_pk_id)
	IFF(
	    in_edw_claimant_reserve_calculation_other_recovery_pk_id IS NULL, - 1,
	    in_edw_claimant_reserve_calculation_other_recovery_pk_id
	) AS edw_claimant_reserve_calculation_other_recovery_pk_id_out,
	JNR_claimant_dim_sources.wc_claimant_det_id1 AS in_wc_claimant_det_pk_id,
	-- *INF*: iif(isnull(in_wc_claimant_det_pk_id),-1,in_wc_claimant_det_pk_id)
	IFF(in_wc_claimant_det_pk_id IS NULL, - 1, in_wc_claimant_det_pk_id) AS wc_claimant_det_pk_id_out,
	JNR_claimant_dim_sources.claim_party_occurrence_ak_id AS edw_claim_party_occurrence_ak_id,
	JNR_claimant_dim_sources.claim_case_ak_id AS in_edw_claim_case_ak_id,
	JNR_claimant_dim_sources.claimant_date_type,
	-- *INF*: iif(isnull(claimant_date_type) OR claimant_date_type ='N/A'
	-- ,'N/A',
	-- rtrim(substr(claimant_date_type,2)))
	IFF(
	    claimant_date_type IS NULL OR claimant_date_type = 'N/A', 'N/A',
	    rtrim(substr(claimant_date_type, 2))
	) AS v_claimant_date_type_out,
	v_claimant_date_type_out AS claimant_date_type_out,
	JNR_claimant_dim_sources.claimant_date,
	JNR_claimant_dim_sources.claimant_direct_loss_status_code AS in_claimant_direct_loss_status_code,
	-- *INF*: iif(isnull(in_claimant_direct_loss_status_code) OR in_claimant_direct_loss_status_code ='N/A'
	-- ,'N/A',
	-- rtrim(substr(in_claimant_direct_loss_status_code,2)))
	IFF(
	    in_claimant_direct_loss_status_code IS NULL OR in_claimant_direct_loss_status_code = 'N/A',
	    'N/A',
	    rtrim(substr(in_claimant_direct_loss_status_code, 2))
	) AS claimant_direct_loss_status_code_out,
	JNR_claimant_dim_sources.claimant_exp_status_code AS in_claimant_exp_status_code,
	-- *INF*: iif(isnull(in_claimant_exp_status_code) OR in_claimant_exp_status_code='N/A'
	-- ,'N/A',
	-- rtrim(substr(in_claimant_exp_status_code,2)))
	IFF(
	    in_claimant_exp_status_code IS NULL OR in_claimant_exp_status_code = 'N/A', 'N/A',
	    rtrim(substr(in_claimant_exp_status_code, 2))
	) AS claimant_exp_status_code_out,
	JNR_claimant_dim_sources.claimant_subrogation_status_code AS in_claimant_subrogation_status_code,
	-- *INF*: iif(isnull(in_claimant_subrogation_status_code) OR in_claimant_subrogation_status_code = 'N/A'
	-- ,'N/A',
	-- rtrim(substr(in_claimant_subrogation_status_code,2)))
	IFF(
	    in_claimant_subrogation_status_code IS NULL OR in_claimant_subrogation_status_code = 'N/A',
	    'N/A',
	    rtrim(substr(in_claimant_subrogation_status_code, 2))
	) AS claimant_subrogation_status_code_out,
	JNR_claimant_dim_sources.claimant_salvage_status_code AS in_claimant_salvage_status_code,
	-- *INF*: iif(isnull(in_claimant_salvage_status_code) OR in_claimant_salvage_status_code = 'N/A'
	-- ,'N/A'
	-- ,rtrim(substr(in_claimant_salvage_status_code,2)))
	IFF(
	    in_claimant_salvage_status_code IS NULL OR in_claimant_salvage_status_code = 'N/A', 'N/A',
	    rtrim(substr(in_claimant_salvage_status_code, 2))
	) AS claimant_salvage_status_code_out,
	JNR_claimant_dim_sources.claimant_other_recovery_status_code AS in_claimant_other_recovery_status_code,
	-- *INF*: iif(isnull(in_claimant_other_recovery_status_code) OR in_claimant_other_recovery_status_code='N/A'
	-- ,'N/A',
	-- rtrim(substr(in_claimant_other_recovery_status_code,2)))
	IFF(
	    in_claimant_other_recovery_status_code IS NULL
	    or in_claimant_other_recovery_status_code = 'N/A',
	    'N/A',
	    rtrim(substr(in_claimant_other_recovery_status_code, 2))
	) AS claimant_other_recovery_status_code_out,
	JNR_claimant_dim_sources.claimant_reopen_ind AS in_claimant_reopen_ind,
	-- *INF*: iif(isnull(in_claimant_reopen_ind),'N/A',in_claimant_reopen_ind)
	IFF(in_claimant_reopen_ind IS NULL, 'N/A', in_claimant_reopen_ind) AS claimant_reopen_ind_out,
	JNR_claimant_dim_sources.claimant_financial_ind AS in_claimant_financial_ind,
	-- *INF*: iif(isnull(in_claimant_financial_ind),'N/A',in_claimant_financial_ind)
	IFF(in_claimant_financial_ind IS NULL, 'N/A', in_claimant_financial_ind) AS claimant_financial_ind_out,
	JNR_claimant_dim_sources.claimant_supplemental_ind AS in_claimant_supplemental_ind,
	-- *INF*: iif(isnull(in_claimant_supplemental_ind),'N/A',in_claimant_supplemental_ind)
	IFF(in_claimant_supplemental_ind IS NULL, 'N/A', in_claimant_supplemental_ind) AS claimant_supplemental_ind_out,
	JNR_claimant_dim_sources.claimant_recovery_ind AS in_claimant_recovery_ind,
	-- *INF*: iif(isnull(in_claimant_recovery_ind),'N/A',in_claimant_recovery_ind)
	IFF(in_claimant_recovery_ind IS NULL, 'N/A', in_claimant_recovery_ind) AS claimant_recovery_ind_out,
	JNR_claimant_dim_sources.claimant_notice_only_ind AS in_claimant_notice_only_claim_ind,
	-- *INF*: iif(isnull(in_claimant_notice_only_claim_ind),'N/A',in_claimant_notice_only_claim_ind)
	IFF(in_claimant_notice_only_claim_ind IS NULL, 'N/A', in_claimant_notice_only_claim_ind) AS claimant_notice_only_claim_ind_out,
	-- *INF*: IIF(ISNULL(v_claimant_date_type_out),TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),IIF(v_claimant_date_type_out = 'OPEN',claimant_date,TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')))
	-- 
	-- 
	-- 
	-- --DECODE(TRUE, 
	-- --v_claimant_date_type_out ='N/A',TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	-- --v_claimant_date_type_out = 'OPEN',claimant_date,
	-- --(v_claimant_date_type_out != 'N/A' OR v_claimant_date_type_out != 'OPEN' ) AND (edw_claim_party_occurrence_ak_id = v_prev_row_claim_party_occurrence_ak_id), v_prev_row_claimant_open_date)
	-- 
	-- 
	-- 
	-- 
	IFF(
	    v_claimant_date_type_out IS NULL,
	    TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    IFF(
	        v_claimant_date_type_out = 'OPEN', claimant_date,
	        TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
	    )
	) AS v_claimant_open_date,
	v_claimant_open_date AS claimant_open_date,
	-- *INF*: IIF(ISNULL(v_claimant_date_type_out),TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),IIF(v_claimant_date_type_out = 'CLOSED',claimant_date,TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')))
	-- 
	-- 
	-- 
	-- --DECODE(TRUE, 
	-- --v_claimant_date_type_out ='N/A',TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	-- --v_claimant_date_type_out = 'CLOSED',claimant_date,
	-- --(v_claimant_date_type_out != 'N/A' OR v_claimant_date_type_out != 'CLOSED' ) AND (edw_claim_party_occurrence_ak_id = v_prev_row_claim_party_occurrence_ak_id), v_prev_row_claimant_close_date)
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	IFF(
	    v_claimant_date_type_out IS NULL,
	    TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    IFF(
	        v_claimant_date_type_out = 'CLOSED', claimant_date,
	        TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
	    )
	) AS v_claimant_close_date,
	v_claimant_close_date AS claimant_close_date,
	-- *INF*: IIF(ISNULL(v_claimant_date_type_out),TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),IIF(v_claimant_date_type_out = 'REOPEN',claimant_date,TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')))
	-- 
	-- 
	-- --DECODE(TRUE, 
	-- --v_claimant_date_type_out ='N/A',TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	-- --v_claimant_date_type_out = 'REOPEN',claimant_date,
	-- --(v_claimant_date_type_out != 'N/A' OR v_claimant_date_type_out != 'REOPEN' ) AND (edw_claim_party_occurrence_ak_id = --v_prev_row_claim_party_occurrence_ak_id), v_prev_row_claimant_reopen_date)
	-- 
	-- 
	IFF(
	    v_claimant_date_type_out IS NULL,
	    TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    IFF(
	        v_claimant_date_type_out = 'REOPEN', claimant_date,
	        TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
	    )
	) AS v_claimant_reopen_date,
	v_claimant_reopen_date AS claimant_reopen_date,
	-- *INF*: IIF(ISNULL(v_claimant_date_type_out),TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),IIF(v_claimant_date_type_out = 'CLOSEDAFTERREOPEN',claimant_date,TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')))
	-- 
	-- 
	-- --DECODE(TRUE, 
	-- --v_claimant_date_type_out ='N/A',TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	-- --v_claimant_date_type_out = 'CLOSEDAFTERREOPEN',claimant_date,
	-- --(v_claimant_date_type_out != 'N/A' OR v_claimant_date_type_out != 'CLOSEDAFTERREOPEN' ) AND (edw_claim_party_occurrence_ak_id = --v_prev_row_claim_party_occurrence_ak_id), v_prev_row_claimant_reopen_date)
	-- 
	-- 
	-- 
	IFF(
	    v_claimant_date_type_out IS NULL,
	    TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    IFF(
	        v_claimant_date_type_out = 'CLOSEDAFTERREOPEN', claimant_date,
	        TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
	    )
	) AS v_claimant_closed_after_reopen_date,
	v_claimant_closed_after_reopen_date AS claimant_closed_after_reopen_date,
	-- *INF*: IIF(ISNULL(v_claimant_date_type_out),TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),IIF(v_claimant_date_type_out = 'NOTICEONLY',claimant_date,TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')))
	-- 
	-- 
	-- 
	-- --DECODE(TRUE, 
	-- --v_claimant_date_type_out ='N/A',TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	-- --v_claimant_date_type_out = 'NOTICEONLY',claimant_date,
	-- --(v_claimant_date_type_out != 'N/A' OR v_claimant_date_type_out != 'NOTICEONLY' ) AND (edw_claim_party_occurrence_ak_id = v_prev_row_claim_party_occurrence_ak_id), v_prev_row_claimant_noticeonly_date)
	-- 
	-- 
	-- 
	IFF(
	    v_claimant_date_type_out IS NULL,
	    TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    IFF(
	        v_claimant_date_type_out = 'NOTICEONLY', claimant_date,
	        TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
	    )
	) AS v_claimant_noticeonly_date,
	v_claimant_noticeonly_date AS claimant_noticeonly_date,
	edw_claim_party_occurrence_ak_id AS v_prev_row_claim_party_occurrence_ak_id,
	v_claimant_open_date AS v_prev_row_claimant_open_date,
	v_claimant_close_date AS v_prev_row_claimant_close_date,
	v_claimant_reopen_date AS v_prev_row_claimant_reopen_date,
	v_claimant_closed_after_reopen_date AS v_prev_row_claimant_closed_after_reopen_date,
	v_claimant_noticeonly_date AS v_prev_row_claimant_noticeonly_date,
	JNR_claimant_dim_sources.addr_type AS claim_party_addr_type,
	JNR_claimant_dim_sources.claim_party_zip,
	JNR_claimant_dim_sources.claim_party_state,
	JNR_claimant_dim_sources.claim_party_county,
	JNR_claimant_dim_sources.claim_party_city,
	JNR_claimant_dim_sources.claim_party_addr,
	JNR_claimant_dim_sources.claim_party_full_name,
	JNR_claimant_dim_sources.claim_party_first_name,
	JNR_claimant_dim_sources.claim_party_last_name,
	JNR_claimant_dim_sources.claim_party_mid_name,
	JNR_claimant_dim_sources.tax_ssn_id AS claim_party_tax_ssn_id,
	JNR_claimant_dim_sources.tax_fed_id AS claim_party_tax_fed_id,
	JNR_claimant_dim_sources.claim_party_birthdate,
	JNR_claimant_dim_sources.claim_party_gndr,
	JNR_claimant_dim_sources.claim_party_role_code AS in_claim_party_role_code,
	-- *INF*: IIF(ltrim(rtrim(in_claim_party_role_code))='CMT','CLMT',ltrim(rtrim(in_claim_party_role_code)))
	-- 
	-- 
	IFF(
	    ltrim(rtrim(in_claim_party_role_code)) = 'CMT', 'CLMT',
	    ltrim(rtrim(in_claim_party_role_code))
	) AS claim_party_role_code_out,
	LKP_sup_claim_party_role_code.claim_party_role_descript AS in_claim_party_role_code_descript,
	-- *INF*: IIF(ISNULL(in_claim_party_role_code_descript), 'N/A', in_claim_party_role_code_descript)
	IFF(in_claim_party_role_code_descript IS NULL, 'N/A', in_claim_party_role_code_descript) AS out_claim_party_role_code_descript,
	JNR_claimant_dim_sources.claimant_num AS in_claimant_num,
	-- *INF*: IIF(ISNULL(in_claimant_num), 'N/A', in_claimant_num)
	IFF(in_claimant_num IS NULL, 'N/A', in_claimant_num) AS out_claimant_num,
	JNR_claimant_dim_sources.denial_date AS in_denial_date,
	-- *INF*: IIF(ISNULL(in_denial_date),TO_DATE('1/1/1800','MM/DD/YYYY'),in_denial_date)
	IFF(in_denial_date IS NULL, TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'), in_denial_date) AS denial_date_out,
	JNR_claimant_dim_sources.jurisdiction_state_code AS in_jurisdiction_state_code,
	-- *INF*: iif(isnull(in_jurisdiction_state_code),'N/A',in_jurisdiction_state_code)
	IFF(in_jurisdiction_state_code IS NULL, 'N/A', in_jurisdiction_state_code) AS jurisdiction_state_code_out,
	-- *INF*: :LKP.LKP_SUP_STATE(ltrim(rtrim(in_jurisdiction_state_code)))
	LKP_SUP_STATE_ltrim_rtrim_in_jurisdiction_state_code.state_descript AS IN_jurisdiction_state_descript,
	-- *INF*: iif(isnull(IN_jurisdiction_state_descript),'N/A',IN_jurisdiction_state_descript)
	IFF(IN_jurisdiction_state_descript IS NULL, 'N/A', IN_jurisdiction_state_descript) AS jurisdiction_state_descript_OUT,
	JNR_claimant_dim_sources.emplyr_notified_date AS in_emplyr_notified_date,
	-- *INF*: IIF(ISNULL(in_emplyr_notified_date),TO_DATE('1/1/1800','MM/DD/YYYY'),in_emplyr_notified_date)
	IFF(
	    in_emplyr_notified_date IS NULL, TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'),
	    in_emplyr_notified_date
	) AS emplyr_notified_date_out,
	JNR_claimant_dim_sources.rpted_to_carrier_date AS in_rpted_to_carrier_date,
	-- *INF*: IIF(ISNULL(in_rpted_to_carrier_date),TO_DATE('1/1/1800','MM/DD/YYYY'),in_rpted_to_carrier_date)
	IFF(
	    in_rpted_to_carrier_date IS NULL, TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'),
	    in_rpted_to_carrier_date
	) AS rpted_to_carrier_date_out,
	JNR_claimant_dim_sources.jurisdiction_claim_num AS in_jurisdiction_claim_num,
	-- *INF*: iif(isnull(in_jurisdiction_claim_num),'N/A',in_jurisdiction_claim_num)
	IFF(in_jurisdiction_claim_num IS NULL, 'N/A', in_jurisdiction_claim_num) AS jurisdiction_claim_num_out,
	JNR_claimant_dim_sources.care_directed_ind AS in_care_directed_ind,
	-- *INF*: iif(isnull(in_care_directed_ind),'N/A',in_care_directed_ind)
	IFF(in_care_directed_ind IS NULL, 'N/A', in_care_directed_ind) AS care_directed_ind_out,
	JNR_claimant_dim_sources.care_directed_by AS in_care_directed_by,
	-- *INF*: iif(isnull(in_care_directed_by),'N/A',in_care_directed_by)
	IFF(in_care_directed_by IS NULL, 'N/A', in_care_directed_by) AS care_directed_by_out,
	LKP_sup_workers_comp_care_directed_by.wc_care_directed_by_descript AS IN_wc_care_directed_by_descript,
	-- *INF*: iif(isnull(IN_wc_care_directed_by_descript),'N/A',IN_wc_care_directed_by_descript)
	IFF(IN_wc_care_directed_by_descript IS NULL, 'N/A', IN_wc_care_directed_by_descript) AS wc_care_directed_by_descript_OUT,
	JNR_claimant_dim_sources.hired_state_code AS in_hired_state_code,
	-- *INF*: iif(isnull(in_hired_state_code),'N/A',in_hired_state_code)
	IFF(in_hired_state_code IS NULL, 'N/A', in_hired_state_code) AS hired_state_code_out,
	-- *INF*: :LKP.LKP_SUP_STATE(ltrim(rtrim(in_hired_state_code)))
	LKP_SUP_STATE_ltrim_rtrim_in_hired_state_code.state_descript AS IN_hired_state_descript,
	-- *INF*: iif(isnull(IN_hired_state_descript),'N/A',IN_hired_state_descript)
	IFF(IN_hired_state_descript IS NULL, 'N/A', IN_hired_state_descript) AS hired_state_descript_OUT,
	JNR_claimant_dim_sources.hired_date AS in_hired_date,
	-- *INF*: IIF(ISNULL(in_hired_date),TO_DATE('1/1/1800','MM/DD/YYYY'),in_hired_date)
	IFF(in_hired_date IS NULL, TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'), in_hired_date) AS hired_date_out,
	JNR_claimant_dim_sources.tax_filing_status AS in_tax_filing_status,
	-- *INF*: iif(isnull(in_tax_filing_status),'N/A',in_tax_filing_status)
	IFF(in_tax_filing_status IS NULL, 'N/A', in_tax_filing_status) AS tax_filing_status_out,
	LKP_sup_tax_filing_status.tax_filing_status_descript AS IN_tax_filing_status_descript,
	-- *INF*: iif(isnull(IN_tax_filing_status_descript),'N/A',IN_tax_filing_status_descript)
	IFF(IN_tax_filing_status_descript IS NULL, 'N/A', IN_tax_filing_status_descript) AS tax_filing_status_descript_OUT,
	JNR_claimant_dim_sources.occuptn_code AS in_occuptn_code,
	-- *INF*: iif(isnull(in_occuptn_code),'N/A',in_occuptn_code)
	IFF(in_occuptn_code IS NULL, 'N/A', in_occuptn_code) AS occuptn_code_out,
	LKP_sup_workers_comp_occupation.occuptn_descript AS IN_occuptn_descript,
	-- *INF*: iif(isnull(IN_occuptn_descript),'N/A',IN_occuptn_descript)
	IFF(IN_occuptn_descript IS NULL, 'N/A', IN_occuptn_descript) AS occuptn_descript_OUT,
	JNR_claimant_dim_sources.emplymnt_status_code,
	-- *INF*: iif(isnull(emplymnt_status_code),'N/A',emplymnt_status_code)
	IFF(emplymnt_status_code IS NULL, 'N/A', emplymnt_status_code) AS employement_status_code_out,
	LKP_sup_workers_comp_employment_status.wc_emplymnt_descript AS IN_wc_emplymnt_descript,
	-- *INF*: iif(isnull(IN_wc_emplymnt_descript),'N/A',IN_wc_emplymnt_descript)
	IFF(IN_wc_emplymnt_descript IS NULL, 'N/A', IN_wc_emplymnt_descript) AS wc_emplymnt_descript_OUT,
	JNR_claimant_dim_sources.len_of_time_in_crrnt_job,
	-- *INF*: iif(isnull(len_of_time_in_crrnt_job),'N/A',len_of_time_in_crrnt_job)
	IFF(len_of_time_in_crrnt_job IS NULL, 'N/A', len_of_time_in_crrnt_job) AS len_of_time_in_crrnt_job_out,
	JNR_claimant_dim_sources.emp_dept_name AS in_emp_dept_name,
	-- *INF*: iif(isnull(in_emp_dept_name),'N/A',in_emp_dept_name)
	IFF(in_emp_dept_name IS NULL, 'N/A', in_emp_dept_name) AS emp_dept_name_out,
	JNR_claimant_dim_sources.emp_shift_num AS in_emp_shift_num,
	-- *INF*: iif(isnull(in_emp_shift_num),'N/A',in_emp_shift_num)
	IFF(in_emp_shift_num IS NULL, 'N/A', in_emp_shift_num) AS emp_shift_num_out,
	JNR_claimant_dim_sources.marital_status AS in_marital_status,
	-- *INF*: iif(isnull(in_marital_status),'N/A',in_marital_status)
	IFF(in_marital_status IS NULL, 'N/A', in_marital_status) AS marital_status_out,
	LKP_sup_marital_status.marital_status_descript AS IN_marital_status_descript,
	-- *INF*: iif(isnull(IN_marital_status_descript),'N/A',IN_marital_status_descript)
	IFF(IN_marital_status_descript IS NULL, 'N/A', IN_marital_status_descript) AS marital_status_descript_OUT,
	JNR_claimant_dim_sources.num_of_dependents AS in_num_of_dependents,
	-- *INF*: iif(isnull(in_num_of_dependents),0,in_num_of_dependents)
	IFF(in_num_of_dependents IS NULL, 0, in_num_of_dependents) AS num_of_dependents_out,
	JNR_claimant_dim_sources.num_of_dependent_children AS in_num_of_dependent_children,
	-- *INF*: iif(isnull(in_num_of_dependent_children),0,in_num_of_dependent_children)
	IFF(in_num_of_dependent_children IS NULL, 0, in_num_of_dependent_children) AS num_of_dependent_children_out,
	JNR_claimant_dim_sources.num_of_other_dependents AS in_num_of_other_dependents,
	-- *INF*: iif(isnull(in_num_of_other_dependents),0,in_num_of_other_dependents)
	IFF(in_num_of_other_dependents IS NULL, 0, in_num_of_other_dependents) AS num_of_other_dependents_out,
	JNR_claimant_dim_sources.num_of_exemptions AS in_num_of_exemptions,
	-- *INF*: iif(isnull(in_num_of_exemptions),0,in_num_of_exemptions)
	IFF(in_num_of_exemptions IS NULL, 0, in_num_of_exemptions) AS num_of_exemptions_out,
	JNR_claimant_dim_sources.exemption_type AS in_exemption_type,
	-- *INF*: iif(isnull(in_exemption_type),'N/A',in_exemption_type)
	IFF(in_exemption_type IS NULL, 'N/A', in_exemption_type) AS exemption_type_out,
	LKP_sup_workers_comp_exemption_type.wc_exemption_type_descript AS IN_wc_exemption_type_descript,
	-- *INF*: iif(isnull(IN_wc_exemption_type_descript),'N/A',IN_wc_exemption_type_descript)
	IFF(IN_wc_exemption_type_descript IS NULL, 'N/A', IN_wc_exemption_type_descript) AS wc_exemption_type_descript_OUT,
	JNR_claimant_dim_sources.emp_blind_ind AS in_emp_blind_ind,
	-- *INF*: iif(isnull(in_emp_blind_ind),'N/A',in_emp_blind_ind)
	IFF(in_emp_blind_ind IS NULL, 'N/A', in_emp_blind_ind) AS emp_blind_ind_out,
	JNR_claimant_dim_sources.emp_over_65_ind AS in_emp_over_65_ind,
	-- *INF*: iif(isnull(in_emp_over_65_ind),'N/A',in_emp_over_65_ind)
	IFF(in_emp_over_65_ind IS NULL, 'N/A', in_emp_over_65_ind) AS emp_over_65_ind_out,
	JNR_claimant_dim_sources.spouse_blind_ind AS in_spouse_blind_ind,
	-- *INF*: iif(isnull(in_spouse_blind_ind),'N/A',in_spouse_blind_ind)
	IFF(in_spouse_blind_ind IS NULL, 'N/A', in_spouse_blind_ind) AS spouse_blind_ind_out,
	JNR_claimant_dim_sources.spouse_over_65_ind AS in_spouse_over_65_ind,
	-- *INF*: iif(isnull(in_spouse_over_65_ind),'N/A',in_spouse_over_65_ind)
	IFF(in_spouse_over_65_ind IS NULL, 'N/A', in_spouse_over_65_ind) AS spouse_over_65_ind_out,
	JNR_claimant_dim_sources.education_lvl AS in_education_lvl,
	-- *INF*: iif(isnull(in_education_lvl),'N/A',in_education_lvl)
	IFF(in_education_lvl IS NULL, 'N/A', in_education_lvl) AS education_lvl_out,
	JNR_claimant_dim_sources.med_auth_ind AS in_med_auth_ind,
	-- *INF*: iif(isnull(in_med_auth_ind),'N/A',in_med_auth_ind)
	IFF(in_med_auth_ind IS NULL, 'N/A', in_med_auth_ind) AS med_auth_ind_out,
	JNR_claimant_dim_sources.auth_to_release_ssn_ind AS in_auth_to_release_ssn_ind,
	-- *INF*: iif(isnull(in_auth_to_release_ssn_ind),'N/A',in_auth_to_release_ssn_ind)
	IFF(in_auth_to_release_ssn_ind IS NULL, 'N/A', in_auth_to_release_ssn_ind) AS auth_to_release_ssn_ind_out,
	JNR_claimant_dim_sources.emp_id_num AS in_emp_id_num,
	-- *INF*: iif(isnull(in_emp_id_num),'N/A',in_emp_id_num)
	IFF(in_emp_id_num IS NULL, 'N/A', in_emp_id_num) AS emp_id_num_out,
	JNR_claimant_dim_sources.emp_id_type AS in_emp_id_type,
	-- *INF*: iif(isnull(in_emp_id_type),'N/A',in_emp_id_type)
	IFF(in_emp_id_type IS NULL, 'N/A', in_emp_id_type) AS emp_id_type_out,
	LKP_sup_workers_comp_employee_identification_type.emp_id_type_descript AS IN_emp_id_type_descript,
	-- *INF*: iif(isnull(IN_emp_id_type_descript),'N/A',IN_emp_id_type_descript)
	IFF(IN_emp_id_type_descript IS NULL, 'N/A', IN_emp_id_type_descript) AS emp_id_type_descript_OUT,
	JNR_claimant_dim_sources.emp_part_time_hour_week,
	-- *INF*: iif(isnull(emp_part_time_hour_week),0,emp_part_time_hour_week)
	IFF(emp_part_time_hour_week IS NULL, 0, emp_part_time_hour_week) AS emp_part_time_hour_week_out,
	JNR_claimant_dim_sources.emp_dept_num AS in_emp_dept_num,
	-- *INF*: iif(isnull(in_emp_dept_num),'N/A',in_emp_dept_num)
	IFF(in_emp_dept_num IS NULL, 'N/A', in_emp_dept_num) AS emp_dept_num_out,
	JNR_claimant_dim_sources.emp_part_time_hourly_week_rate_amt AS in_emp_part_time_hourly_week_rate_amt,
	-- *INF*: iif(isnull(in_emp_part_time_hourly_week_rate_amt),0,in_emp_part_time_hourly_week_rate_amt)
	IFF(in_emp_part_time_hourly_week_rate_amt IS NULL, 0, in_emp_part_time_hourly_week_rate_amt) AS emp_part_time_hourly_week_rate_amt_out,
	JNR_claimant_dim_sources.wage_rate_amt AS in_wage_rate_amt,
	-- *INF*: iif(isnull(in_wage_rate_amt),0,in_wage_rate_amt)
	IFF(in_wage_rate_amt IS NULL, 0, in_wage_rate_amt) AS wage_rate_amt_out,
	JNR_claimant_dim_sources.wage_period_code AS in_wage_period_code,
	-- *INF*: iif(isnull(in_wage_period_code),'N/A',in_wage_period_code)
	IFF(in_wage_period_code IS NULL, 'N/A', in_wage_period_code) AS wage_period_code_out,
	LKP_sup_workers_comp_wage_period.wage_period_descript AS IN_wage_period_descript,
	-- *INF*: iif(isnull(IN_wage_period_descript),'N/A',IN_wage_period_descript)
	IFF(IN_wage_period_descript IS NULL, 'N/A', IN_wage_period_descript) AS wage_period_descript_OUT,
	JNR_claimant_dim_sources.wage_eff_date AS in_wage_eff_date,
	-- *INF*: IIF(ISNULL(in_wage_eff_date),TO_DATE('1/1/1800','MM/DD/YYYY'),in_wage_eff_date)
	IFF(in_wage_eff_date IS NULL, TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'), in_wage_eff_date) AS wage_eff_date_out,
	JNR_claimant_dim_sources.weeks_worked AS in_weeks_worked,
	-- *INF*: iif(isnull(in_weeks_worked),0,in_weeks_worked)
	IFF(in_weeks_worked IS NULL, 0, in_weeks_worked) AS weeks_worked_out,
	JNR_claimant_dim_sources.gross_amt_type AS in_gross_amt_type,
	-- *INF*: iif(isnull(in_gross_amt_type),'N/A',in_gross_amt_type)
	IFF(in_gross_amt_type IS NULL, 'N/A', in_gross_amt_type) AS gross_amt_type_out,
	LKP_sup_workers_comp_wage_gross_amount_type.wage_gross_amt_type_descript AS IN_wage_gross_amt_type_descript,
	-- *INF*: iif(isnull(IN_wage_gross_amt_type_descript),'N/A',IN_wage_gross_amt_type_descript)
	IFF(IN_wage_gross_amt_type_descript IS NULL, 'N/A', IN_wage_gross_amt_type_descript) AS wage_gross_amt_type_descript_OUT,
	JNR_claimant_dim_sources.gross_wage_amt_excluding_tips AS in_gross_wage_amt_excluding_tips,
	-- *INF*: iif(isnull(in_gross_wage_amt_excluding_tips),0,in_gross_wage_amt_excluding_tips)
	IFF(in_gross_wage_amt_excluding_tips IS NULL, 0, in_gross_wage_amt_excluding_tips) AS gross_wage_amt_excluding_tips_out,
	JNR_claimant_dim_sources.piece_work_num_of_weeks_excluding_overtime AS in_piece_work_num_of_weeks_excluding_overtime,
	-- *INF*: iif(isnull(in_piece_work_num_of_weeks_excluding_overtime),0,in_piece_work_num_of_weeks_excluding_overtime)
	IFF(
	    in_piece_work_num_of_weeks_excluding_overtime IS NULL, 0,
	    in_piece_work_num_of_weeks_excluding_overtime
	) AS piece_work_num_of_weeks_excluding_overtime_out,
	JNR_claimant_dim_sources.emp_rec_meals AS in_emp_rec_meals,
	-- *INF*: iif(isnull(in_emp_rec_meals),0,in_emp_rec_meals)
	IFF(in_emp_rec_meals IS NULL, 0, in_emp_rec_meals) AS emp_rec_meals_out,
	JNR_claimant_dim_sources.emp_rec_room AS in_emp_rec_room,
	-- *INF*: iif(isnull(in_emp_rec_room),0,in_emp_rec_room)
	IFF(in_emp_rec_room IS NULL, 0, in_emp_rec_room) AS emp_rec_room_out,
	JNR_claimant_dim_sources.emp_rec_tips AS in_emp_rec_tips,
	-- *INF*: iif(isnull(in_emp_rec_tips),0,in_emp_rec_tips)
	IFF(in_emp_rec_tips IS NULL, 0, in_emp_rec_tips) AS emp_rec_tips_out,
	JNR_claimant_dim_sources.overtime_amt AS in_overtime_amt,
	-- *INF*: iif(isnull(in_overtime_amt),0,in_overtime_amt)
	IFF(in_overtime_amt IS NULL, 0, in_overtime_amt) AS overtime_amt_out,
	JNR_claimant_dim_sources.overtime_after_hour_in_a_week AS in_overtime_after_hour_in_a_week,
	-- *INF*: iif(isnull(in_overtime_after_hour_in_a_week),0,in_overtime_after_hour_in_a_week)
	IFF(in_overtime_after_hour_in_a_week IS NULL, 0, in_overtime_after_hour_in_a_week) AS overtime_after_hour_in_a_week_out,
	JNR_claimant_dim_sources.overtime_after_hour_in_a_day AS in_overtime_after_hour_in_a_day,
	-- *INF*: iif(isnull(in_overtime_after_hour_in_a_day),0,in_overtime_after_hour_in_a_day)
	IFF(in_overtime_after_hour_in_a_day IS NULL, 0, in_overtime_after_hour_in_a_day) AS overtime_after_hour_in_a_day_out,
	JNR_claimant_dim_sources.full_pay_inj_day_ind AS in_full_pay_inj_day_ind,
	-- *INF*: iif(isnull(in_full_pay_inj_day_ind),'N/A',in_full_pay_inj_day_ind)
	IFF(in_full_pay_inj_day_ind IS NULL, 'N/A', in_full_pay_inj_day_ind) AS full_pay_inj_day_ind_out,
	JNR_claimant_dim_sources.salary_paid_ind AS in_salary_paid_ind,
	-- *INF*: iif(isnull(in_salary_paid_ind),'N/A',in_salary_paid_ind)
	IFF(in_salary_paid_ind IS NULL, 'N/A', in_salary_paid_ind) AS salary_paid_ind_out,
	JNR_claimant_dim_sources.avg_full_time_days_week AS in_avg_full_time_days_week,
	-- *INF*: iif(isnull(in_avg_full_time_days_week),0,in_avg_full_time_days_week)
	IFF(in_avg_full_time_days_week IS NULL, 0, in_avg_full_time_days_week) AS avg_full_time_days_week_out,
	JNR_claimant_dim_sources.avg_full_time_hours_day AS in_avg_full_time_hours_day,
	-- *INF*: iif(isnull(in_avg_full_time_hours_day),0,in_avg_full_time_hours_day)
	IFF(in_avg_full_time_hours_day IS NULL, 0, in_avg_full_time_hours_day) AS avg_full_time_hours_day_out,
	JNR_claimant_dim_sources.avg_full_time_hours_week AS in_avg_full_time_hours_week,
	-- *INF*: iif(isnull(in_avg_full_time_hours_week),0,in_avg_full_time_hours_week)
	IFF(in_avg_full_time_hours_week IS NULL, 0, in_avg_full_time_hours_week) AS avg_full_time_hours_week_out,
	JNR_claimant_dim_sources.avg_wkly_wage AS in_avg_wkly_wage,
	-- *INF*: iif(isnull(in_avg_wkly_wage),0,in_avg_wkly_wage)
	IFF(in_avg_wkly_wage IS NULL, 0, in_avg_wkly_wage) AS avg_wkly_wage_out,
	JNR_claimant_dim_sources.num_of_full_time_emplymnt_same_job AS in_num_of_full_time_emplymnt_same_job,
	-- *INF*: iif(isnull(in_num_of_full_time_emplymnt_same_job),0,in_num_of_full_time_emplymnt_same_job)
	IFF(in_num_of_full_time_emplymnt_same_job IS NULL, 0, in_num_of_full_time_emplymnt_same_job) AS num_of_full_time_emplymnt_same_job_out,
	JNR_claimant_dim_sources.num_of_part_time_emplymnt_same_job AS in_num_of_part_time_emplymnt_same_job,
	-- *INF*: iif(isnull(in_num_of_part_time_emplymnt_same_job),0,in_num_of_part_time_emplymnt_same_job)
	IFF(in_num_of_part_time_emplymnt_same_job IS NULL, 0, in_num_of_part_time_emplymnt_same_job) AS num_of_part_time_emplymnt_same_job_out,
	JNR_claimant_dim_sources.ttd_rate AS in_ttd_rate,
	-- *INF*: iif(isnull(in_ttd_rate),0,in_ttd_rate)
	IFF(in_ttd_rate IS NULL, 0, in_ttd_rate) AS ttd_rate_out,
	JNR_claimant_dim_sources.ppd_rate AS in_ppd_rate,
	-- *INF*: iif(isnull(in_ppd_rate),0,in_ppd_rate)
	IFF(in_ppd_rate IS NULL, 0, in_ppd_rate) AS ppd_rate_out,
	JNR_claimant_dim_sources.ptd_rate AS in_ptd_rate,
	-- *INF*: iif(isnull(in_ptd_rate),0,in_ptd_rate)
	IFF(in_ptd_rate IS NULL, 0, in_ptd_rate) AS ptd_rate_out,
	JNR_claimant_dim_sources.dtd_rate AS in_dtd_rate,
	-- *INF*: iif(isnull(in_dtd_rate),0,in_dtd_rate)
	IFF(in_dtd_rate IS NULL, 0, in_dtd_rate) AS dtd_rate_out,
	JNR_claimant_dim_sources.wkly_attorney_fee AS in_wkly_attorney_fee,
	-- *INF*: iif(isnull(in_wkly_attorney_fee),0,in_wkly_attorney_fee)
	IFF(in_wkly_attorney_fee IS NULL, 0, in_wkly_attorney_fee) AS wkly_attorney_fee_out,
	JNR_claimant_dim_sources.first_rpt_inj_date AS in_first_rpt_inj_date,
	-- *INF*: IIF(ISNULL(in_first_rpt_inj_date),TO_DATE('1/1/1800','MM/DD/YYYY'),in_first_rpt_inj_date)
	IFF(
	    in_first_rpt_inj_date IS NULL, TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'), in_first_rpt_inj_date
	) AS first_rpt_inj_date_out,
	JNR_claimant_dim_sources.supplementary_rpt_inj_date AS in_supplementary_rpt_inj_date,
	-- *INF*: IIF(ISNULL(in_supplementary_rpt_inj_date),TO_DATE('1/1/1800','MM/DD/YYYY'),in_supplementary_rpt_inj_date)
	IFF(
	    in_supplementary_rpt_inj_date IS NULL, TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'),
	    in_supplementary_rpt_inj_date
	) AS supplementary_rpt_inj_date_out,
	JNR_claimant_dim_sources.fringe_bnft_discontinued_amt AS in_fringe_bnft_discontinued_amt,
	-- *INF*: iif(isnull(in_fringe_bnft_discontinued_amt),0,in_fringe_bnft_discontinued_amt)
	IFF(in_fringe_bnft_discontinued_amt IS NULL, 0, in_fringe_bnft_discontinued_amt) AS fringe_bnft_discontinued_amt_out,
	JNR_claimant_dim_sources.emp_start_time AS in_emp_start_time,
	-- *INF*: IIF(ISNULL(in_emp_start_time),'00:00:00',in_emp_start_time)
	IFF(in_emp_start_time IS NULL, '00:00:00', in_emp_start_time) AS emp_start_time_out,
	JNR_claimant_dim_sources.emp_hour_day AS in_emp_hour_day,
	-- *INF*: iif(isnull(in_emp_hour_day),0,in_emp_hour_day)
	IFF(in_emp_hour_day IS NULL, 0, in_emp_hour_day) AS emp_hour_day_out,
	JNR_claimant_dim_sources.emp_hour_week AS in_emp_hour_week,
	-- *INF*: iif(isnull(in_emp_hour_week),0,in_emp_hour_week)
	IFF(in_emp_hour_week IS NULL, 0, in_emp_hour_week) AS emp_hour_week_out,
	JNR_claimant_dim_sources.emp_day_week AS in_emp_day_week,
	-- *INF*: iif(isnull(in_emp_day_week),0,in_emp_day_week)
	IFF(in_emp_day_week IS NULL, 0, in_emp_day_week) AS emp_day_week_out,
	JNR_claimant_dim_sources.inj_work_day_begin_time AS in_inj_work_day_begin_time,
	-- *INF*: IIF(ISNULL(in_inj_work_day_begin_time),TO_DATE('1/1/1800','MM/DD/YYYY'),in_inj_work_day_begin_time)
	IFF(
	    in_inj_work_day_begin_time IS NULL, TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'),
	    in_inj_work_day_begin_time
	) AS inj_work_day_begin_time_out,
	JNR_claimant_dim_sources.disability_date AS in_disability_date,
	-- *INF*: IIF(ISNULL(in_disability_date),TO_DATE('1/1/1800','MM/DD/YYYY'),in_disability_date)
	IFF(in_disability_date IS NULL, TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'), in_disability_date) AS disability_date_out,
	JNR_claimant_dim_sources.phys_restriction_ind AS in_phys_restriction_ind,
	-- *INF*: iif(isnull(in_phys_restriction_ind),'N/A',in_phys_restriction_ind)
	IFF(in_phys_restriction_ind IS NULL, 'N/A', in_phys_restriction_ind) AS phys_restriction_ind_out,
	JNR_claimant_dim_sources.pre_exst_disability_ind AS in_pre_exst_disability_ind,
	-- *INF*: iif(isnull(in_pre_exst_disability_ind),'N/A',in_pre_exst_disability_ind)
	IFF(in_pre_exst_disability_ind IS NULL, 'N/A', in_pre_exst_disability_ind) AS pre_exst_disability_ind_out,
	JNR_claimant_dim_sources.premises_code AS in_premises_code,
	-- *INF*: iif(isnull(in_premises_code),'N/A',in_premises_code)
	IFF(in_premises_code IS NULL, 'N/A', in_premises_code) AS premises_code_out,
	LKP_sup_workers_comp_premises_type.premises_descript AS IN_premises_descript,
	-- *INF*: iif(isnull(IN_premises_descript),'N/A',IN_premises_descript)
	IFF(IN_premises_descript IS NULL, 'N/A', IN_premises_descript) AS premises_descript_OUT,
	JNR_claimant_dim_sources.work_process_descript AS in_work_process_descript,
	-- *INF*: iif(isnull(in_work_process_descript),'N/A',in_work_process_descript)
	IFF(in_work_process_descript IS NULL, 'N/A', in_work_process_descript) AS work_process_descript_out,
	JNR_claimant_dim_sources.task_descript AS in_task_descript,
	-- *INF*: iif(isnull(in_task_descript),'N/A',in_task_descript)
	IFF(in_task_descript IS NULL, 'N/A', in_task_descript) AS task_descript_out,
	JNR_claimant_dim_sources.body_part_code AS in_body_part_code,
	-- *INF*: iif(isnull(in_body_part_code),'N/A',in_body_part_code)
	IFF(in_body_part_code IS NULL, 'N/A', in_body_part_code) AS body_part_code_out,
	LKP_sup_workers_comp_body_part.body_part_descript AS IN_body_part_descript,
	-- *INF*: iif(isnull(IN_body_part_descript),'N/A',IN_body_part_descript)
	IFF(IN_body_part_descript IS NULL, 'N/A', IN_body_part_descript) AS body_part_descript_OUT,
	JNR_claimant_dim_sources.nature_inj_code AS in_nature_inj_code,
	-- *INF*: iif(isnull(in_nature_inj_code),'N/A',in_nature_inj_code)
	IFF(in_nature_inj_code IS NULL, 'N/A', in_nature_inj_code) AS nature_inj_code_out,
	LKP_sup_workers_comp_nature_of_injury.nature_of_inj_descript AS IN_nature_of_inj_descript,
	-- *INF*: iif(isnull(IN_nature_of_inj_descript),'N/A',IN_nature_of_inj_descript)
	IFF(IN_nature_of_inj_descript IS NULL, 'N/A', IN_nature_of_inj_descript) AS nature_of_inj_descript_OUT,
	JNR_claimant_dim_sources.cause_inj_code AS in_cause_inj_code,
	-- *INF*: iif(isnull(in_cause_inj_code),'N/A',in_cause_inj_code)
	IFF(in_cause_inj_code IS NULL, 'N/A', in_cause_inj_code) AS cause_inj_code_out,
	LKP_sup_workers_comp_cause_of_injury.cause_of_inj_descript AS IN_cause_of_inj_descript,
	-- *INF*: iif(isnull(IN_cause_of_inj_descript),'N/A',IN_cause_of_inj_descript)
	IFF(IN_cause_of_inj_descript IS NULL, 'N/A', IN_cause_of_inj_descript) AS cause_of_inj_descript_OUT,
	JNR_claimant_dim_sources.safeguard_not_used_ind AS in_safeguard_not_used_ind,
	-- *INF*: iif(isnull(in_safeguard_not_used_ind),'N/A',in_safeguard_not_used_ind)
	IFF(in_safeguard_not_used_ind IS NULL, 'N/A', in_safeguard_not_used_ind) AS safeguard_not_used_ind_out,
	JNR_claimant_dim_sources.inj_substance_abuse_ind AS in_inj_substance_abuse_ind,
	-- *INF*: iif(isnull(in_inj_substance_abuse_ind),'N/A',in_inj_substance_abuse_ind)
	IFF(in_inj_substance_abuse_ind IS NULL, 'N/A', in_inj_substance_abuse_ind) AS inj_substance_abuse_ind_out,
	JNR_claimant_dim_sources.sfty_device_not_used_ind AS in_sfty_device_not_used_ind,
	-- *INF*: iif(isnull(in_sfty_device_not_used_ind),'N/A',in_sfty_device_not_used_ind)
	IFF(in_sfty_device_not_used_ind IS NULL, 'N/A', in_sfty_device_not_used_ind) AS sfty_device_not_used_ind_out,
	JNR_claimant_dim_sources.inj_rules_not_obeyed_ind AS in_inj_rules_not_obeyed_ind,
	-- *INF*: iif(isnull(in_inj_rules_not_obeyed_ind),'N/A',in_inj_rules_not_obeyed_ind)
	IFF(in_inj_rules_not_obeyed_ind IS NULL, 'N/A', in_inj_rules_not_obeyed_ind) AS inj_rules_not_obeyed_ind_out,
	JNR_claimant_dim_sources.inj_result_occuptnal_inj_ind AS in_inj_result_occuptnal_inj_ind,
	-- *INF*: iif(isnull(in_inj_result_occuptnal_inj_ind),'N/A',in_inj_result_occuptnal_inj_ind)
	IFF(in_inj_result_occuptnal_inj_ind IS NULL, 'N/A', in_inj_result_occuptnal_inj_ind) AS inj_result_occuptnal_inj_ind_out,
	JNR_claimant_dim_sources.inj_result_occuptnal_disease_ndicator AS in_inj_result_occuptnal_disease_ndicator,
	-- *INF*: iif(isnull(in_inj_result_occuptnal_disease_ndicator),'N/A',in_inj_result_occuptnal_disease_ndicator)
	IFF(
	    in_inj_result_occuptnal_disease_ndicator IS NULL, 'N/A',
	    in_inj_result_occuptnal_disease_ndicator
	) AS inj_result_occuptnal_disease_ndicator_out,
	JNR_claimant_dim_sources.inj_result_death_ind AS in_inj_result_death_ind,
	-- *INF*: iif(isnull(in_inj_result_death_ind),'N/A',in_inj_result_death_ind)
	IFF(in_inj_result_death_ind IS NULL, 'N/A', in_inj_result_death_ind) AS inj_result_death_ind_out,
	JNR_claimant_dim_sources.unsafe_act_descript AS in_unsafe_act_descript,
	-- *INF*: iif(isnull(in_unsafe_act_descript),'N/A',in_unsafe_act_descript)
	IFF(in_unsafe_act_descript IS NULL, 'N/A', in_unsafe_act_descript) AS unsafe_act_descript_out,
	JNR_claimant_dim_sources.responsible_for_inj_descript AS in_responsible_for_inj_descript,
	-- *INF*: iif(isnull(in_responsible_for_inj_descript),'N/A',in_responsible_for_inj_descript)
	IFF(in_responsible_for_inj_descript IS NULL, 'N/A', in_responsible_for_inj_descript) AS responsible_for_inj_descript_out,
	JNR_claimant_dim_sources.hazard_condition_descript AS in_hazard_condition_descript,
	-- *INF*: iif(isnull(in_hazard_condition_descript),'N/A',in_hazard_condition_descript)
	IFF(in_hazard_condition_descript IS NULL, 'N/A', in_hazard_condition_descript) AS hazard_condition_descript_out,
	LKP_Workers_Comp_Claimant_Work_History.emp_last_day_worked AS in_emp_last_day_worked,
	-- *INF*: IIF(ISNULL(in_emp_last_day_worked),TO_DATE('1/1/1800','MM/DD/YYYY'),in_emp_last_day_worked)
	IFF(
	    in_emp_last_day_worked IS NULL, TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'),
	    in_emp_last_day_worked
	) AS emp_last_day_worked_out,
	JNR_claimant_dim_sources.death_date AS in_death_date,
	-- *INF*: IIF(ISNULL(in_death_date),TO_DATE('1/1/1800','MM/DD/YYYY'),in_death_date)
	IFF(in_death_date IS NULL, TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'), in_death_date) AS death_date_out,
	LKP_Workers_Comp_Claimant_Work_History.return_to_work_date AS in_return_to_work_date,
	-- *INF*: IIF(ISNULL(in_return_to_work_date),TO_DATE('1/1/1800','MM/DD/YYYY'),in_return_to_work_date)
	IFF(
	    in_return_to_work_date IS NULL, TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'),
	    in_return_to_work_date
	) AS return_to_work_date_out,
	LKP_Workers_Comp_Claimant_Work_History.return_to_work_type AS in_return_to_work_type,
	-- *INF*: iif(isnull(RTRIM(in_return_to_work_type)),'N/A',RTRIM(in_return_to_work_type))
	IFF(RTRIM(in_return_to_work_type) IS NULL, 'N/A', RTRIM(in_return_to_work_type)) AS return_to_work_type_out,
	LKP_sup_workers_comp_return_to_work_type.return_to_work_descript AS IN_return_to_work_descript,
	-- *INF*: iif(isnull(IN_return_to_work_descript),'N/A',IN_return_to_work_descript)
	IFF(IN_return_to_work_descript IS NULL, 'N/A', IN_return_to_work_descript) AS return_to_work_descript_OUT,
	LKP_Workers_Comp_Claimant_Work_History.return_to_work_with_same_emplyr_ind AS in_return_to_work_with_same_emplyr_ind,
	-- *INF*: iif(isnull(in_return_to_work_with_same_emplyr_ind),'N/A',in_return_to_work_with_same_emplyr_ind)
	IFF(
	    in_return_to_work_with_same_emplyr_ind IS NULL, 'N/A',
	    in_return_to_work_with_same_emplyr_ind
	) AS return_to_work_with_same_emplyr_ind_out,
	JNR_claimant_dim_sources.emplyr_nature_bus_descript AS in_emplyr_nature_bus_descript,
	-- *INF*: iif(isnull(in_emplyr_nature_bus_descript),'N/A',in_emplyr_nature_bus_descript)
	IFF(in_emplyr_nature_bus_descript IS NULL, 'N/A', in_emplyr_nature_bus_descript) AS emplyr_nature_bus_descript_out,
	JNR_claimant_dim_sources.emplyr_type_code AS in_emplyr_type_code,
	-- *INF*: iif(isnull(in_emplyr_type_code),'N/A',in_emplyr_type_code)
	IFF(in_emplyr_type_code IS NULL, 'N/A', in_emplyr_type_code) AS emplyr_type_code_out,
	LKP_sup_workers_comp_employer_type.emplyr_type_descript AS IN_emplyr_type_descript,
	-- *INF*: iif(isnull(IN_emplyr_type_descript),'N/A',IN_emplyr_type_descript)
	IFF(IN_emplyr_type_descript IS NULL, 'N/A', IN_emplyr_type_descript) AS emplyr_type_descript_OUT,
	JNR_claimant_dim_sources.insd_type_code AS in_insd_type_code,
	-- *INF*: iif(isnull(in_insd_type_code),'N/A',in_insd_type_code)
	IFF(in_insd_type_code IS NULL, 'N/A', in_insd_type_code) AS insd_type_code_out,
	LKP_sup_insured_type.insd_type_descript AS IN_insd_type_descript,
	-- *INF*: iif(isnull(IN_insd_type_descript),'N/A',IN_insd_type_descript)
	IFF(IN_insd_type_descript IS NULL, 'N/A', IN_insd_type_descript) AS insd_type_descript_OUT,
	JNR_claimant_dim_sources.subrogation_statute_exp_date AS in_subrogation_statute_exp_date,
	-- *INF*: IIF(ISNULL(in_subrogation_statute_exp_date),TO_DATE('1/1/1800','MM/DD/YYYY'),in_subrogation_statute_exp_date)
	IFF(
	    in_subrogation_statute_exp_date IS NULL, TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'),
	    in_subrogation_statute_exp_date
	) AS subrogation_statute_exp_date_out,
	JNR_claimant_dim_sources.managed_care_org_type AS in_managed_care_org_type,
	-- *INF*: iif(isnull(in_managed_care_org_type),'N/A',in_managed_care_org_type)
	IFF(in_managed_care_org_type IS NULL, 'N/A', in_managed_care_org_type) AS managed_care_org_type_out,
	LKP_sup_workers_comp_managed_care_organization_type.managed_care_org_type_descript AS IN_managed_care_org_type_descript,
	-- *INF*: iif(isnull(IN_managed_care_org_type_descript),'N/A',IN_managed_care_org_type_descript)
	IFF(IN_managed_care_org_type_descript IS NULL, 'N/A', IN_managed_care_org_type_descript) AS managed_care_org_type_descript_OUT,
	JNR_claimant_dim_sources.subrogation_code AS in_subrogation_code,
	-- *INF*: iif(isnull(in_subrogation_code),'N/A',in_subrogation_code)
	IFF(in_subrogation_code IS NULL, 'N/A', in_subrogation_code) AS subrogation_code_out,
	JNR_claimant_dim_sources.loss_condition AS in_loss_condition,
	-- *INF*: iif(isnull(in_loss_condition),'N/A',in_loss_condition)
	IFF(in_loss_condition IS NULL, 'N/A', in_loss_condition) AS loss_condition_out,
	LKP_sup_workers_comp_loss_condition.loss_condition_descript AS IN_loss_condition_descript,
	-- *INF*: iif(isnull(IN_loss_condition_descript),'N/A',IN_loss_condition_descript)
	IFF(IN_loss_condition_descript IS NULL, 'N/A', IN_loss_condition_descript) AS loss_condition_descript_OUT,
	JNR_claimant_dim_sources.attorney_or_au_rep_ind AS in_attorney_or_au_rep_ind,
	-- *INF*: iif(isnull(in_attorney_or_au_rep_ind),'N/A',in_attorney_or_au_rep_ind)
	IFF(in_attorney_or_au_rep_ind IS NULL, 'N/A', in_attorney_or_au_rep_ind) AS attorney_or_au_rep_ind_out,
	JNR_claimant_dim_sources.hospital_cost AS in_hospital_cost,
	-- *INF*: iif(isnull(in_hospital_cost),0,in_hospital_cost)
	IFF(in_hospital_cost IS NULL, 0, in_hospital_cost) AS hospital_cost_out,
	JNR_claimant_dim_sources.doctor_cost AS in_doctor_cost,
	-- *INF*: iif(isnull(in_doctor_cost),0,in_doctor_cost)
	IFF(in_doctor_cost IS NULL, 0, in_doctor_cost) AS doctor_cost_out,
	JNR_claimant_dim_sources.other_med_cost AS in_other_med_cost,
	-- *INF*: iif(isnull(in_other_med_cost),0,in_other_med_cost)
	IFF(in_other_med_cost IS NULL, 0, in_other_med_cost) AS other_med_cost_out,
	JNR_claimant_dim_sources.controverted_case_code AS in_controverted_case_code,
	-- *INF*: iif(isnull(in_controverted_case_code),'N/A',in_controverted_case_code)
	IFF(in_controverted_case_code IS NULL, 'N/A', in_controverted_case_code) AS v_controverted_case_code1,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_QUES(edw_claim_party_occurrence_ak_id,'NcciDciWasClaimCompensabilityDisputed', 'Claim.GeneralCase.Questions')
	-- 
	-- 
	-- 
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciWasClaimCompensabilityDisputed_Claim_GeneralCase_Questions.optn_text AS v_controverted_case_code2,
	-- *INF*: iif(isnull(v_controverted_case_code2),'N/A',v_controverted_case_code2)
	IFF(v_controverted_case_code2 IS NULL, 'N/A', v_controverted_case_code2) AS v_controverted_case_code3,
	v_controverted_case_code3 AS controverted_case_code_out,
	JNR_claimant_dim_sources.surgery_ind AS in_surgery_ind,
	-- *INF*: iif(isnull(in_surgery_ind),'N/A',in_surgery_ind)
	IFF(in_surgery_ind IS NULL, 'N/A', in_surgery_ind) AS surgery_ind_out,
	JNR_claimant_dim_sources.emplyr_loc_descript AS in_emplyr_loc_descript,
	-- *INF*: iif(isnull(in_emplyr_loc_descript),'N/A',in_emplyr_loc_descript)
	IFF(in_emplyr_loc_descript IS NULL, 'N/A', in_emplyr_loc_descript) AS emplyr_loc_descript_out,
	JNR_claimant_dim_sources.inj_loc_comment AS in_inj_loc_comment,
	-- *INF*: iif(isnull(in_inj_loc_comment),'N/A',in_inj_loc_comment)
	IFF(in_inj_loc_comment IS NULL, 'N/A', in_inj_loc_comment) AS inj_loc_comment_out,
	JNR_claimant_dim_sources.claim_ctgry_code AS in_claim_ctgry_code,
	-- *INF*: iif(isnull(in_claim_ctgry_code),'N/A',in_claim_ctgry_code)
	IFF(in_claim_ctgry_code IS NULL, 'N/A', in_claim_ctgry_code) AS claim_ctgry_code_out,
	LKP_SUP_WC_CLAIM_CATG.claim_ctgry_code_descript,
	JNR_claimant_dim_sources.act_status_code AS in_act_status_code,
	-- *INF*: iif(isnull(in_act_status_code),'N/A',in_act_status_code)
	IFF(in_act_status_code IS NULL, 'N/A', in_act_status_code) AS act_status_code_out,
	LKP_SUP_WC_ACTIVITY_STATUS.act_status_code_descript,
	JNR_claimant_dim_sources.investigate_ind AS in_investigate_ind,
	-- *INF*: iif(isnull(in_investigate_ind),'N/A',in_investigate_ind)
	IFF(in_investigate_ind IS NULL, 'N/A', in_investigate_ind) AS investigate_ind_out,
	JNR_claimant_dim_sources.sic_code,
	-- *INF*: iif(isnull(sic_code),'N/A',sic_code)
	IFF(sic_code IS NULL, 'N/A', sic_code) AS sic_code_out,
	LKP_SUP_WC_SIC_CODE.sic_code_descript,
	JNR_claimant_dim_sources.hospitalized_ind AS in_hospitalized_ind,
	-- *INF*: iif(isnull(in_hospitalized_ind),'N/A',in_hospitalized_ind)
	IFF(in_hospitalized_ind IS NULL, 'N/A', in_hospitalized_ind) AS hospitalized_ind_out,
	JNR_claimant_dim_sources.wage_method_code AS in_wage_method_code,
	-- *INF*: iif(isnull(in_wage_method_code),'N/A',in_wage_method_code)
	IFF(in_wage_method_code IS NULL, 'N/A', in_wage_method_code) AS wage_method_code_out,
	LKP_SUP_WC_WAGE_METHOD.wage_method_code_descript,
	JNR_claimant_dim_sources.pms_occuptn_descript AS in_pms_occuptn_descript,
	-- *INF*: iif(isnull(in_pms_occuptn_descript),'N/A',in_pms_occuptn_descript)
	IFF(in_pms_occuptn_descript IS NULL, 'N/A', in_pms_occuptn_descript) AS pms_occuptn_descript_out,
	JNR_claimant_dim_sources.pms_type_disability AS in_pms_type_disability,
	-- *INF*: iif(isnull(in_pms_type_disability),'N/A',in_pms_type_disability)
	IFF(in_pms_type_disability IS NULL, 'N/A', in_pms_type_disability) AS pms_type_disability_out,
	JNR_claimant_dim_sources.ncci_type_cov AS in_ncci_type_cov,
	-- *INF*: iif(isnull(in_ncci_type_cov),'N/A',in_ncci_type_cov)
	IFF(in_ncci_type_cov IS NULL, 'N/A', in_ncci_type_cov) AS ncci_type_cov_out,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	JNR_claimant_dim_sources.eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	sysdate AS created_date,
	sysdate AS modified_date,
	'N/A' AS Default,
	LKP_Workers_Comp_Claimant_Work_History.wc_claimant_work_hist_id AS in_wc_claimant_work_hist_pk_id,
	-- *INF*: iif(isnull(in_wc_claimant_work_hist_pk_id),
	-- -1,in_wc_claimant_work_hist_pk_id)
	IFF(in_wc_claimant_work_hist_pk_id IS NULL, - 1, in_wc_claimant_work_hist_pk_id) AS wc_claimant_work_hist_pk_id_out,
	JNR_claimant_dim_sources.wc_claimant_num AS in_wc_claimant_num,
	-- *INF*: iif(isnull(in_wc_claimant_num),'N/A',in_wc_claimant_num)
	-- 
	--  
	IFF(in_wc_claimant_num IS NULL, 'N/A', in_wc_claimant_num) AS wc_claimant_num_out,
	JNR_claimant_dim_sources.max_med_improvement_date AS in_max_med_improvement_date,
	-- *INF*: IIF(ISNULL(in_max_med_improvement_date),TO_DATE('1/1/1800','MM/DD/YYYY'),in_max_med_improvement_date)
	IFF(
	    in_max_med_improvement_date IS NULL, TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'),
	    in_max_med_improvement_date
	) AS max_med_improvement_date,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_ANS(edw_claim_party_occurrence_ak_id,'NcciDciLossType', 'Claim.GeneralCase.Questions')
	-- 
	--  
	-- 
	-- 
	--  
	LKP_CLAIM_ANSWER_ANS_edw_claim_party_occurrence_ak_id_NcciDciLossType_Claim_GeneralCase_Questions.optn_set_item_val AS v_type_of_loss_code,
	-- *INF*: --v_type_of_loss_code
	-- 
	-- IIF(ISNULL(v_type_of_loss_code),'N/A',v_type_of_loss_code)
	-- 
	-- ---IIF(ISNULL( :LKP.LKP_CLAIM_ANSWER_ANS(edw_claim_party_occurrence_ak_id,'Claims Workers Compensation','Claim.GeneralCase.Questions' , 'NcciDciLossType') ),'N/A')
	IFF(v_type_of_loss_code IS NULL, 'N/A', v_type_of_loss_code) AS type_of_loss_code,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_QUES(edw_claim_party_occurrence_ak_id,'NcciDciLossType', 'Claim.GeneralCase.Questions')
	-- 
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciLossType_Claim_GeneralCase_Questions.optn_text AS v_type_of_loss_code_descript,
	-- *INF*: IIF(ISNULL(v_type_of_loss_code_descript ),'N/A',v_type_of_loss_code_descript )
	IFF(v_type_of_loss_code_descript IS NULL, 'N/A', v_type_of_loss_code_descript) AS type_of_loss_code_descript,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_ANS(edw_claim_party_occurrence_ak_id,'NcciDciMethodofDeterminingAww','Claimant.GeneralCase.Questions')
	-- 
	-- 
	LKP_CLAIM_ANSWER_ANS_edw_claim_party_occurrence_ak_id_NcciDciMethodofDeterminingAww_Claimant_GeneralCase_Questions.optn_set_item_val AS v_pre_injury_avg_wkly_wage_code,
	-- *INF*: IIF(ISNULL( v_pre_injury_avg_wkly_wage_code ),'N/A', v_pre_injury_avg_wkly_wage_code)
	IFF(v_pre_injury_avg_wkly_wage_code IS NULL, 'N/A', v_pre_injury_avg_wkly_wage_code) AS pre_injury_avg_wkly_wage_code,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_QUES(edw_claim_party_occurrence_ak_id,'NcciDciMethodofDeterminingAww','Claimant.GeneralCase.Questions')
	--  
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciMethodofDeterminingAww_Claimant_GeneralCase_Questions.optn_text AS v_pre_injury_avg_wkly_wage_code_descript,
	-- *INF*: IIF(ISNULL( v_pre_injury_avg_wkly_wage_code_descript),'N/A',v_pre_injury_avg_wkly_wage_code_descript)
	IFF(
	    v_pre_injury_avg_wkly_wage_code_descript IS NULL, 'N/A',
	    v_pre_injury_avg_wkly_wage_code_descript
	) AS pre_injury_avg_wkly_wage_code_descript,
	-- *INF*:  :LKP.LKP_CLAIM_ANSWER_QUES(edw_claim_party_occurrence_ak_id,'NcciDciPostInjuryWeeklyWageAmount','Claimant.GeneralCase.Questions')
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciPostInjuryWeeklyWageAmount_Claimant_GeneralCase_Questions.optn_text AS v_post_inj_wkly_wage_amt,
	-- *INF*: IIF(ISNULL( TO_DECIMAL(v_post_inj_wkly_wage_amt,2)),0, TO_DECIMAL(v_post_inj_wkly_wage_amt,2))
	--  
	IFF(
	    CAST(v_post_inj_wkly_wage_amt AS FLOAT) IS NULL, 0, CAST(v_post_inj_wkly_wage_amt AS FLOAT)
	) AS post_inj_wkly_wage_amt,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_QUES(edw_claim_party_occurrence_ak_id,'NcciDciBAWDisabilityPercentage' ,'Claim.GeneralCase.Questions')
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciBAWDisabilityPercentage_Claim_GeneralCase_Questions.optn_text AS v_impairment_disability_percentage1,
	-- *INF*: IIF(ISNULL( v_impairment_disability_percentage1),:LKP.LKP_CLAIM_ANSWER_QUES(edw_claim_party_occurrence_ak_id,'NcciDciBodyPartDisabilityPercentage' ,'Claim.GeneralCase.Questions') , v_impairment_disability_percentage1)
	IFF(
	    v_impairment_disability_percentage1 IS NULL,
	    LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciBodyPartDisabilityPercentage_Claim_GeneralCase_Questions.optn_text,
	    v_impairment_disability_percentage1
	) AS v_impairment_disability_percentage2,
	-- *INF*: IIF(ISNULL(TO_DECIMAL(v_impairment_disability_percentage2,2)),0,TO_DECIMAL(v_impairment_disability_percentage2,2))
	IFF(
	    CAST(v_impairment_disability_percentage2 AS FLOAT) IS NULL, 0,
	    CAST(v_impairment_disability_percentage2 AS FLOAT)
	) AS v_impairment_disability_percentage3,
	-- *INF*: IIF((v_impairment_disability_percentage3 > 100),100,v_impairment_disability_percentage3 )
	IFF((v_impairment_disability_percentage3 > 100), 100, v_impairment_disability_percentage3) AS v_impairment_disability_percentage4,
	v_impairment_disability_percentage4 AS impairment_disability_percentage,
	-- *INF*:   :LKP.LKP_CLAIM_ANSWER_ANS(edw_claim_party_occurrence_ak_id,'NcciDciImpairmentPercentageBasis','Claim.GeneralCase.Questions')
	--  
	LKP_CLAIM_ANSWER_ANS_edw_claim_party_occurrence_ak_id_NcciDciImpairmentPercentageBasis_Claim_GeneralCase_Questions.optn_set_item_val AS v_impairment_disability_percentage_basis_code,
	-- *INF*: IIF(ISNULL( v_impairment_disability_percentage_basis_code),'N/A', v_impairment_disability_percentage_basis_code)
	IFF(
	    v_impairment_disability_percentage_basis_code IS NULL, 'N/A',
	    v_impairment_disability_percentage_basis_code
	) AS impairment_disability_percentage_basis_code,
	-- *INF*:   :LKP.LKP_CLAIM_ANSWER_QUES(edw_claim_party_occurrence_ak_id,'NcciDciImpairmentPercentageBasis','Claim.GeneralCase.Questions')
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciImpairmentPercentageBasis_Claim_GeneralCase_Questions.optn_text AS v_impairment_disability_percentage_basis_code_descript,
	-- *INF*: IIF(ISNULL( v_impairment_disability_percentage_basis_code_descript ),'N/A',v_impairment_disability_percentage_basis_code_descript )
	IFF(
	    v_impairment_disability_percentage_basis_code_descript IS NULL, 'N/A',
	    v_impairment_disability_percentage_basis_code_descript
	) AS impairment_disability_percentage_basis_code_descript,
	-- *INF*:   :LKP.LKP_CLAIM_ANSWER_QUES(edw_claim_party_occurrence_ak_id,'NcciDciWereMedicalPaymentsExtinguished','Claim.GeneralCase.Questions')
	--  
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciWereMedicalPaymentsExtinguished_Claim_GeneralCase_Questions.optn_text AS v_med_extinguishment_ind,
	-- *INF*: IIF(ISNULL( v_med_extinguishment_ind ),'N/A',v_med_extinguishment_ind)
	IFF(v_med_extinguishment_ind IS NULL, 'N/A', v_med_extinguishment_ind) AS med_extinguishment_ind,
	-- *INF*:   :LKP.LKP_CLAIM_ANSWER_QUES(edw_claim_party_occurrence_ak_id,'CurrentWorkStatus','Claimant.Disability.Questions')
	-- 
	-- 
	--  
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_CurrentWorkStatus_Claimant_Disability_Questions.optn_text AS v_current_work_status,
	-- *INF*: IIF(ISNULL( v_current_work_status ),'N/A',v_current_work_status)
	IFF(v_current_work_status IS NULL, 'N/A', v_current_work_status) AS current_work_status,
	-- *INF*: iif(isnull(in_jurisdiction_state_code),'N/A',in_jurisdiction_state_code)
	IFF(in_jurisdiction_state_code IS NULL, 'N/A', in_jurisdiction_state_code) AS v_state,
	-- *INF*: TO_DATE(TO_CHAR(sysdate,'YYYY-MM-DD'), 'YYYY-MM-DD')
	TO_TIMESTAMP(TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD'), 'YYYY-MM-DD') AS v_sysdate,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_QUES(edw_claim_party_occurrence_ak_id,'RestrictionDateBegin1', 'Claimant.Disability.Questions')
	-- 
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateBegin1_Claimant_Disability_Questions.optn_text AS v_restriction_begin_date1_str,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_QUES
	-- (edw_claim_party_occurrence_ak_id,'RestrictionDateBegin2', 'Claimant.Disability.Questions')
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateBegin2_Claimant_Disability_Questions.optn_text AS v_restriction_begin_date2_str,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_QUES
	-- (edw_claim_party_occurrence_ak_id,'RestrictionDateBegin3',  'Claimant.Disability.Questions')
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateBegin3_Claimant_Disability_Questions.optn_text AS v_restriction_begin_date3_str,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_QUES
	-- (edw_claim_party_occurrence_ak_id,'RestrictionDateBegin4',  'Claimant.Disability.Questions')
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateBegin4_Claimant_Disability_Questions.optn_text AS v_restriction_begin_date4_str,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_QUES
	-- (edw_claim_party_occurrence_ak_id,'RestrictionDateBegin5',  'Claimant.Disability.Questions')
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateBegin5_Claimant_Disability_Questions.optn_text AS v_restriction_begin_date5_str,
	-- *INF*: IIF(ISNULL(v_restriction_begin_date1_str), v_sysdate, TO_DATE ( v_restriction_begin_date1_str,'YYYY-MM-DD'))
	-- 
	--  
	IFF(
	    v_restriction_begin_date1_str IS NULL, v_sysdate,
	    TO_TIMESTAMP(v_restriction_begin_date1_str, 'YYYY-MM-DD')
	) AS v_restriction_begin_date1,
	-- *INF*: IIF(ISNULL(v_restriction_begin_date2_str), v_sysdate, TO_DATE ( v_restriction_begin_date2_str,'YYYY-MM-DD'))
	IFF(
	    v_restriction_begin_date2_str IS NULL, v_sysdate,
	    TO_TIMESTAMP(v_restriction_begin_date2_str, 'YYYY-MM-DD')
	) AS v_restriction_begin_date2,
	-- *INF*: IIF(ISNULL(v_restriction_begin_date3_str), v_sysdate, TO_DATE ( v_restriction_begin_date3_str,'YYYY-MM-DD'))
	IFF(
	    v_restriction_begin_date3_str IS NULL, v_sysdate,
	    TO_TIMESTAMP(v_restriction_begin_date3_str, 'YYYY-MM-DD')
	) AS v_restriction_begin_date3,
	-- *INF*: IIF(ISNULL(v_restriction_begin_date4_str), v_sysdate, TO_DATE ( v_restriction_begin_date4_str,'YYYY-MM-DD'))
	IFF(
	    v_restriction_begin_date4_str IS NULL, v_sysdate,
	    TO_TIMESTAMP(v_restriction_begin_date4_str, 'YYYY-MM-DD')
	) AS v_restriction_begin_date4,
	-- *INF*: IIF(ISNULL(v_restriction_begin_date5_str), v_sysdate, TO_DATE ( v_restriction_begin_date5_str,'YYYY-MM-DD'))
	IFF(
	    v_restriction_begin_date5_str IS NULL, v_sysdate,
	    TO_TIMESTAMP(v_restriction_begin_date5_str, 'YYYY-MM-DD')
	) AS v_restriction_begin_date5,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_QUES
	-- (edw_claim_party_occurrence_ak_id,'RestrictionDateEnd1',  'Claimant.Disability.Questions')
	-- 
	-- 
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateEnd1_Claimant_Disability_Questions.optn_text AS v_restriction_end_date1_str,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_QUES
	-- (edw_claim_party_occurrence_ak_id,'RestrictionDateEnd2',  'Claimant.Disability.Questions')
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateEnd2_Claimant_Disability_Questions.optn_text AS v_restriction_end_date2_str,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_QUES
	-- (edw_claim_party_occurrence_ak_id,'RestrictionDateEnd3',  'Claimant.Disability.Questions')
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateEnd3_Claimant_Disability_Questions.optn_text AS v_restriction_end_date3_str,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_QUES
	-- (edw_claim_party_occurrence_ak_id,'RestrictionDateEnd4',  'Claimant.Disability.Questions')
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateEnd4_Claimant_Disability_Questions.optn_text AS v_restriction_end_date4_str,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_QUES
	-- (edw_claim_party_occurrence_ak_id,'RestrictionDateEnd5', 'Claimant.Disability.Questions')
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateEnd5_Claimant_Disability_Questions.optn_text AS v_restriction_end_date5_str,
	-- *INF*: IIF(ISNULL(v_restriction_end_date1_str), v_sysdate, TO_DATE ( v_restriction_end_date1_str,'YYYY-MM-DD'))
	IFF(
	    v_restriction_end_date1_str IS NULL, v_sysdate,
	    TO_TIMESTAMP(v_restriction_end_date1_str, 'YYYY-MM-DD')
	) AS v_restriction_end_date1,
	-- *INF*: IIF(ISNULL(v_restriction_end_date2_str), v_sysdate, TO_DATE ( v_restriction_end_date2_str,'YYYY-MM-DD'))
	IFF(
	    v_restriction_end_date2_str IS NULL, v_sysdate,
	    TO_TIMESTAMP(v_restriction_end_date2_str, 'YYYY-MM-DD')
	) AS v_restriction_end_date2,
	-- *INF*: IIF(ISNULL(v_restriction_end_date3_str), v_sysdate, TO_DATE ( v_restriction_end_date3_str,'YYYY-MM-DD'))
	IFF(
	    v_restriction_end_date3_str IS NULL, v_sysdate,
	    TO_TIMESTAMP(v_restriction_end_date3_str, 'YYYY-MM-DD')
	) AS v_restriction_end_date3,
	-- *INF*: IIF(ISNULL(v_restriction_end_date4_str), v_sysdate, TO_DATE ( v_restriction_end_date4_str,'YYYY-MM-DD'))
	IFF(
	    v_restriction_end_date4_str IS NULL, v_sysdate,
	    TO_TIMESTAMP(v_restriction_end_date4_str, 'YYYY-MM-DD')
	) AS v_restriction_end_date4,
	-- *INF*: IIF(ISNULL(v_restriction_end_date5_str), v_sysdate, TO_DATE ( v_restriction_end_date5_str,'YYYY-MM-DD'))
	IFF(
	    v_restriction_end_date5_str IS NULL, v_sysdate,
	    TO_TIMESTAMP(v_restriction_end_date5_str, 'YYYY-MM-DD')
	) AS v_restriction_end_date5,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_QUES
	-- (edw_claim_party_occurrence_ak_id,'RestrictionAccommodated1',  'Claimant.Disability.Questions')
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionAccommodated1_Claimant_Disability_Questions.optn_text AS v_restriction_accommodated1,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_QUES
	-- (edw_claim_party_occurrence_ak_id,'RestrictionAccommodated2',  'Claimant.Disability.Questions')
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionAccommodated2_Claimant_Disability_Questions.optn_text AS v_restriction_accommodated2,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_QUES
	-- (edw_claim_party_occurrence_ak_id,'RestrictionAccommodated3',  'Claimant.Disability.Questions')
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionAccommodated3_Claimant_Disability_Questions.optn_text AS v_restriction_accommodated3,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_QUES
	-- (edw_claim_party_occurrence_ak_id,'RestrictionAccommodated4',  'Claimant.Disability.Questions')
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionAccommodated4_Claimant_Disability_Questions.optn_text AS v_restriction_accommodated4,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_QUES
	-- (edw_claim_party_occurrence_ak_id,'RestrictionAccommodated5',  'Claimant.Disability.Questions')
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionAccommodated5_Claimant_Disability_Questions.optn_text AS v_restriction_accommodated5,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_QUES
	-- (edw_claim_party_occurrence_ak_id,'DailyBenefitRate1',  'Claimant.Disability.Questions')
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_DailyBenefitRate1_Claimant_Disability_Questions.optn_text AS v_daily_benefit_rate1_str,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_QUES
	-- (edw_claim_party_occurrence_ak_id,'DailyBenefitRate2',  'Claimant.Disability.Questions')
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_DailyBenefitRate2_Claimant_Disability_Questions.optn_text AS v_daily_benefit_rate2_str,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_QUES
	-- (edw_claim_party_occurrence_ak_id,'DailyBenefitRate3',  'Claimant.Disability.Questions')
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_DailyBenefitRate3_Claimant_Disability_Questions.optn_text AS v_daily_benefit_rate3_str,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_QUES
	-- (edw_claim_party_occurrence_ak_id,'DailyBenefitRate4', 'Claimant.Disability.Questions')
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_DailyBenefitRate4_Claimant_Disability_Questions.optn_text AS v_daily_benefit_rate4_str,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_QUES
	-- (edw_claim_party_occurrence_ak_id,'DailyBenefitRate5', 'Claimant.Disability.Questions')
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_DailyBenefitRate5_Claimant_Disability_Questions.optn_text AS v_daily_benefit_rate5_str,
	-- *INF*: IIF(ISNULL(v_daily_benefit_rate1_str),0,TO_DECIMAL( v_daily_benefit_rate1_str))
	-- 
	--  
	IFF(v_daily_benefit_rate1_str IS NULL, 0, CAST(v_daily_benefit_rate1_str AS FLOAT)) AS v_daily_benefit_rate1,
	-- *INF*: IIF(ISNULL(v_daily_benefit_rate2_str),0,  
	-- TO_DECIMAL( v_daily_benefit_rate2_str))
	IFF(v_daily_benefit_rate2_str IS NULL, 0, CAST(v_daily_benefit_rate2_str AS FLOAT)) AS v_daily_benefit_rate2,
	-- *INF*: IIF(ISNULL(v_daily_benefit_rate3_str),0,  
	-- TO_DECIMAL( v_daily_benefit_rate3_str))
	IFF(v_daily_benefit_rate3_str IS NULL, 0, CAST(v_daily_benefit_rate3_str AS FLOAT)) AS v_daily_benefit_rate3,
	-- *INF*: IIF(ISNULL(v_daily_benefit_rate4_str),0,  
	-- TO_DECIMAL( v_daily_benefit_rate4_str))
	IFF(v_daily_benefit_rate4_str IS NULL, 0, CAST(v_daily_benefit_rate4_str AS FLOAT)) AS v_daily_benefit_rate4,
	-- *INF*: IIF(ISNULL(v_daily_benefit_rate5_str),0,  
	-- TO_DECIMAL( v_daily_benefit_rate5_str))
	IFF(v_daily_benefit_rate5_str IS NULL, 0, CAST(v_daily_benefit_rate5_str AS FLOAT)) AS v_daily_benefit_rate5,
	-- *INF*: DECODE(v_state, 'WI',6,'MN',5,7)
	-- 
	-- --- This determines the number of days in the week when determining work days by state.
	DECODE(
	    v_state,
	    'WI', 6,
	    'MN', 5,
	    7
	) AS v_work_day_flag,
	-- *INF*: DECODE(v_work_day_flag,
	-- 6, :LKP.LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX(v_restriction_begin_date1  )   ,
	-- 5,      :LKP.LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE(v_restriction_begin_date1  )   
	--  ,0)
	-- 
	-- 
	DECODE(
	    v_work_day_flag,
	    6, LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_begin_date1.six_day_week_count,
	    5, LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_begin_date1.five_day_week_count,
	    0
	) AS v_start_day1_5_6_day_wk,
	-- *INF*: DECODE(v_work_day_flag,
	-- 6, :LKP.LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX(v_restriction_begin_date2  )   ,
	-- 5,      :LKP.LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE(v_restriction_begin_date2  )   
	--  ,0)
	DECODE(
	    v_work_day_flag,
	    6, LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_begin_date2.six_day_week_count,
	    5, LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_begin_date2.five_day_week_count,
	    0
	) AS v_start_day2_5_6_day_wk,
	-- *INF*: DECODE(v_work_day_flag,
	-- 6, :LKP.LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX(v_restriction_begin_date3  )   ,
	-- 5,      :LKP.LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE(v_restriction_begin_date3  )   
	--  ,0)
	DECODE(
	    v_work_day_flag,
	    6, LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_begin_date3.six_day_week_count,
	    5, LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_begin_date3.five_day_week_count,
	    0
	) AS v_start_day3_5_6_day_wk,
	-- *INF*: DECODE(v_work_day_flag,
	-- 6, :LKP.LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX(v_restriction_begin_date4  )   ,
	-- 5,      :LKP.LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE(v_restriction_begin_date4  )   
	--  ,0)
	DECODE(
	    v_work_day_flag,
	    6, LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_begin_date4.six_day_week_count,
	    5, LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_begin_date4.five_day_week_count,
	    0
	) AS v_start_day4_5_6_day_wk,
	-- *INF*: DECODE(v_work_day_flag,
	-- 6, :LKP.LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX(v_restriction_begin_date5  )   ,
	-- 5,      :LKP.LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE(v_restriction_begin_date5  )   
	--  ,0)
	DECODE(
	    v_work_day_flag,
	    6, LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_begin_date5.six_day_week_count,
	    5, LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_begin_date5.five_day_week_count,
	    0
	) AS v_start_day5_5_6_day_wk,
	-- *INF*: DECODE(v_work_day_flag,
	-- 6, :LKP.LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX(v_restriction_end_date1  )   ,
	-- 5,      :LKP.LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE(v_restriction_end_date1  )   
	--  ,0)
	DECODE(
	    v_work_day_flag,
	    6, LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_end_date1.six_day_week_count,
	    5, LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_end_date1.five_day_week_count,
	    0
	) AS v_end_day1_5_6_day_wk,
	-- *INF*: DECODE(v_work_day_flag,
	-- 6, :LKP.LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX(v_restriction_end_date2  )   ,
	-- 5,      :LKP.LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE(v_restriction_end_date2  )   
	--  ,0)
	DECODE(
	    v_work_day_flag,
	    6, LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_end_date2.six_day_week_count,
	    5, LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_end_date2.five_day_week_count,
	    0
	) AS v_end_day2_5_6_day_wk,
	-- *INF*: DECODE(v_work_day_flag,
	-- 6, :LKP.LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX(v_restriction_end_date3  )   ,
	-- 5,      :LKP.LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE(v_restriction_end_date3  )   
	--  ,0)
	DECODE(
	    v_work_day_flag,
	    6, LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_end_date3.six_day_week_count,
	    5, LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_end_date3.five_day_week_count,
	    0
	) AS v_end_day3_5_6_day_wk,
	-- *INF*: DECODE(v_work_day_flag,
	-- 6, :LKP.LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX(v_restriction_end_date4  )   ,
	-- 5,      :LKP.LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE(v_restriction_end_date4  )   
	--  ,0)
	DECODE(
	    v_work_day_flag,
	    6, LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_end_date4.six_day_week_count,
	    5, LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_end_date4.five_day_week_count,
	    0
	) AS v_end_day4_5_6_day_wk,
	-- *INF*: DECODE(v_work_day_flag,
	-- 6, :LKP.LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX(v_restriction_end_date5  )   ,
	-- 5,      :LKP.LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE(v_restriction_end_date5  )   
	--  ,0)
	DECODE(
	    v_work_day_flag,
	    6, LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_end_date5.six_day_week_count,
	    5, LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_end_date5.five_day_week_count,
	    0
	) AS v_end_day5_5_6_day_wk,
	-- *INF*: IIF(v_restriction_accommodated1 = 'No' AND v_restriction_begin_date1 <> v_sysdate, 
	-- IIF( v_work_day_flag = 7,DATE_DIFF(v_restriction_end_date1 ,v_restriction_begin_date1,'DD'), v_end_day1_5_6_day_wk - v_start_day1_5_6_day_wk) , 0)
	-- 
	-- ---  IF Accommodate1 is equal to 'No' and this is a valid record from the CLAIM_ANSWER table 
	-- ---  THEN
	-- ---         IF this is a 7 day work week
	-- ---         THEN  get the date difference for record 1 of 5 USING the date_diff function
	-- ---         ELSE (this is a 5 or 6 day week)  Get the difference between the two lookup values to get the difference in dates
	-- ---                       for a 5 or 6 day week.
	-- 
	IFF(
	    v_restriction_accommodated1 = 'No' AND v_restriction_begin_date1 <> v_sysdate,
	    IFF(
	        v_work_day_flag = 7, DATEDIFF(DAY,v_restriction_end_date1,v_restriction_begin_date1),
	        v_end_day1_5_6_day_wk - v_start_day1_5_6_day_wk
	    ),
	    0
	) AS v_neg_accom_period1,
	-- *INF*: IIF(v_restriction_accommodated2 = 'No' AND v_restriction_begin_date2 <> v_sysdate, 
	-- IIF( v_work_day_flag = 7,DATE_DIFF(v_restriction_end_date2 ,v_restriction_begin_date2,'DD'), v_end_day2_5_6_day_wk - v_start_day2_5_6_day_wk) , 0)
	-- 
	-- 
	-- ---  IF Accommodate2 is equal to 'No' and this is a valid record from the CLAIM_ANSWER table 
	-- ---  THEN
	-- ---         IF this is a 7 day work week
	-- ---         THEN  get the date difference for record 2 of 5 USING the date_diff function
	-- ---         ELSE (this is a 5 or 6 day week)  Get the difference between the two lookup values to get the difference in dates
	-- ---                       for a 5 or 6 day week.
	IFF(
	    v_restriction_accommodated2 = 'No' AND v_restriction_begin_date2 <> v_sysdate,
	    IFF(
	        v_work_day_flag = 7, DATEDIFF(DAY,v_restriction_end_date2,v_restriction_begin_date2),
	        v_end_day2_5_6_day_wk - v_start_day2_5_6_day_wk
	    ),
	    0
	) AS v_neg_accom_period2,
	-- *INF*: IIF(v_restriction_accommodated3 = 'No' AND v_restriction_begin_date3 <> v_sysdate, 
	-- IIF( v_work_day_flag = 7,DATE_DIFF(v_restriction_end_date3 ,v_restriction_begin_date3,'DD'), v_end_day3_5_6_day_wk - v_start_day3_5_6_day_wk) , 0)
	-- 
	-- ---  IF Accommodate3 is equal to 'No' and this is a valid record from the CLAIM_ANSWER table 
	-- ---  THEN
	-- ---         IF this is a 7 day work week
	-- ---         THEN  get the date difference for record 3 of 5 USING the date_diff function
	-- ---         ELSE (this is a 5 or 6 day week)  Get the difference between the two lookup values to get the difference in dates
	-- ---                       for a 5 or 6 day week.
	IFF(
	    v_restriction_accommodated3 = 'No' AND v_restriction_begin_date3 <> v_sysdate,
	    IFF(
	        v_work_day_flag = 7, DATEDIFF(DAY,v_restriction_end_date3,v_restriction_begin_date3),
	        v_end_day3_5_6_day_wk - v_start_day3_5_6_day_wk
	    ),
	    0
	) AS v_neg_accom_period3,
	-- *INF*: IIF(v_restriction_accommodated4 = 'No' AND v_restriction_begin_date4 <> v_sysdate, 
	-- IIF( v_work_day_flag = 7,DATE_DIFF(v_restriction_end_date4 ,v_restriction_begin_date4,'DD'), v_end_day4_5_6_day_wk - v_start_day4_5_6_day_wk) , 0)
	-- 
	-- ---  IF Accommodate4 is equal to 'No' and this is a valid record from the CLAIM_ANSWER table 
	-- ---  THEN
	-- ---         IF this is a 7 day work week
	-- ---         THEN  get the date difference for record 4 of 5 USING the date_diff function
	-- ---         ELSE (this is a 5 or 6 day week)  Get the difference between the two lookup values to get the difference in dates
	-- ---                       for a 5 or 6 day week.
	IFF(
	    v_restriction_accommodated4 = 'No' AND v_restriction_begin_date4 <> v_sysdate,
	    IFF(
	        v_work_day_flag = 7, DATEDIFF(DAY,v_restriction_end_date4,v_restriction_begin_date4),
	        v_end_day4_5_6_day_wk - v_start_day4_5_6_day_wk
	    ),
	    0
	) AS v_neg_accom_period4,
	-- *INF*: IIF(v_restriction_accommodated5 = 'No' AND v_restriction_begin_date5 <> v_sysdate, 
	-- IIF( v_work_day_flag = 7,DATE_DIFF(v_restriction_end_date5 ,v_restriction_begin_date5,'DD'), v_end_day5_5_6_day_wk - v_start_day5_5_6_day_wk) , 0)
	-- 
	-- ---  IF Accommodate5 is equal to 'No' and this is a valid record from the CLAIM_ANSWER table 
	-- ---  THEN
	-- ---         IF this is a 7 day work week
	-- ---         THEN  get the date difference for record 5 of 5 USING the date_diff function
	-- ---         ELSE (this is a 5 or 6 day week)  Get the difference between the two lookup values to get the difference in dates
	-- ---                       for a 5 or 6 day week.
	IFF(
	    v_restriction_accommodated5 = 'No' AND v_restriction_begin_date5 <> v_sysdate,
	    IFF(
	        v_work_day_flag = 7, DATEDIFF(DAY,v_restriction_end_date5,v_restriction_begin_date5),
	        v_end_day5_5_6_day_wk - v_start_day5_5_6_day_wk
	    ),
	    0
	) AS v_neg_accom_period5,
	-- *INF*: IIF(v_restriction_accommodated1 = 'Yes' AND v_restriction_begin_date1 <> v_sysdate, 
	-- IIF( v_work_day_flag = 7,DATE_DIFF(v_restriction_end_date1 ,v_restriction_begin_date1,'DD'), v_end_day1_5_6_day_wk - v_start_day1_5_6_day_wk) , 0)
	-- 
	-- ---  IF Accommodate1 is equal to 'Yes' and this is a valid record from the CLAIM_ANSWER table 
	-- ---  THEN
	-- ---         IF this is a 7 day work week
	-- ---         THEN  get the date difference for record 1 of 5 USING the date_diff function
	-- ---         ELSE (this is a 5 or 6 day week)  Get the difference between the two lookup values to get the difference in dates
	-- ---                       for a 5 or 6 day week.
	IFF(
	    v_restriction_accommodated1 = 'Yes' AND v_restriction_begin_date1 <> v_sysdate,
	    IFF(
	        v_work_day_flag = 7, DATEDIFF(DAY,v_restriction_end_date1,v_restriction_begin_date1),
	        v_end_day1_5_6_day_wk - v_start_day1_5_6_day_wk
	    ),
	    0
	) AS v_pos_accom_period1,
	-- *INF*: IIF(v_restriction_accommodated2 = 'Yes' AND v_restriction_begin_date2 <> v_sysdate, 
	-- IIF( v_work_day_flag = 7,DATE_DIFF(v_restriction_end_date2 ,v_restriction_begin_date2,'DD'), v_end_day2_5_6_day_wk - v_start_day2_5_6_day_wk) , 0)
	-- 
	-- ---  IF Accommodate2 is equal to 'Yes' and this is a valid record from the CLAIM_ANSWER table 
	-- ---  THEN
	-- ---         IF this is a 7 day work week
	-- ---         THEN  get the date difference for record 2 of 5 USING the date_diff function
	-- ---         ELSE (this is a 5 or 6 day week)  Get the difference between the two lookup values to get the difference in dates
	-- ---                       for a 5 or 6 day week.
	IFF(
	    v_restriction_accommodated2 = 'Yes' AND v_restriction_begin_date2 <> v_sysdate,
	    IFF(
	        v_work_day_flag = 7, DATEDIFF(DAY,v_restriction_end_date2,v_restriction_begin_date2),
	        v_end_day2_5_6_day_wk - v_start_day2_5_6_day_wk
	    ),
	    0
	) AS v_pos_accom_period2,
	-- *INF*: IIF(v_restriction_accommodated3 = 'Yes' AND v_restriction_begin_date3 <> v_sysdate, 
	-- IIF( v_work_day_flag = 7,DATE_DIFF(v_restriction_end_date3 ,v_restriction_begin_date3,'DD'), v_end_day3_5_6_day_wk - v_start_day3_5_6_day_wk) , 0)
	-- 
	-- 
	-- ---  IF Accommodate3 is equal to 'Yes' and this is a valid record from the CLAIM_ANSWER table 
	-- ---  THEN
	-- ---         IF this is a 7 day work week
	-- ---         THEN  get the date difference for record 3 of 5 USING the date_diff function
	-- ---         ELSE (this is a 5 or 6 day week)  Get the difference between the two lookup values to get the difference in dates
	-- ---                       for a 5 or 6 day week.
	IFF(
	    v_restriction_accommodated3 = 'Yes' AND v_restriction_begin_date3 <> v_sysdate,
	    IFF(
	        v_work_day_flag = 7, DATEDIFF(DAY,v_restriction_end_date3,v_restriction_begin_date3),
	        v_end_day3_5_6_day_wk - v_start_day3_5_6_day_wk
	    ),
	    0
	) AS v_pos_accom_period3,
	-- *INF*: IIF(v_restriction_accommodated4 = 'Yes' AND v_restriction_begin_date4 <> v_sysdate, 
	-- IIF( v_work_day_flag = 7,DATE_DIFF(v_restriction_end_date4 ,v_restriction_begin_date4,'DD'), v_end_day4_5_6_day_wk - v_start_day4_5_6_day_wk) , 0)
	-- 
	-- 
	-- ---  IF Accommodate4 is equal to 'Yes' and this is a valid record from the CLAIM_ANSWER table 
	-- ---  THEN
	-- ---         IF this is a 7 day work week
	-- ---         THEN  get the date difference for record 4 of 5 USING the date_diff function
	-- ---         ELSE (this is a 5 or 6 day week)  Get the difference between the two lookup values to get the difference in dates
	-- ---                       for a 5 or 6 day week.
	IFF(
	    v_restriction_accommodated4 = 'Yes' AND v_restriction_begin_date4 <> v_sysdate,
	    IFF(
	        v_work_day_flag = 7, DATEDIFF(DAY,v_restriction_end_date4,v_restriction_begin_date4),
	        v_end_day4_5_6_day_wk - v_start_day4_5_6_day_wk
	    ),
	    0
	) AS v_pos_accom_period4,
	-- *INF*: IIF(v_restriction_accommodated5 = 'Yes' AND v_restriction_begin_date5 <> v_sysdate, 
	-- IIF( v_work_day_flag = 7,DATE_DIFF(v_restriction_end_date5 ,v_restriction_begin_date5,'DD'), v_end_day5_5_6_day_wk - v_start_day5_5_6_day_wk) , 0)
	-- 
	-- 
	-- ---  IF Accommodate5 is equal to 'Yes' and this is a valid record from the CLAIM_ANSWER table 
	-- ---  THEN
	-- ---         IF this is a 7 day work week
	-- ---         THEN  get the date difference for record 5 of 5 USING the date_diff function
	-- ---         ELSE (this is a 5 or 6 day week)  Get the difference between the two lookup values to get the difference in dates
	-- ---                       for a 5 or 6 day week.
	IFF(
	    v_restriction_accommodated5 = 'Yes' AND v_restriction_begin_date5 <> v_sysdate,
	    IFF(
	        v_work_day_flag = 7, DATEDIFF(DAY,v_restriction_end_date5,v_restriction_begin_date5),
	        v_end_day5_5_6_day_wk - v_start_day5_5_6_day_wk
	    ),
	    0
	) AS v_pos_accom_period5,
	-- *INF*: ( v_pos_accom_period1 +
	-- v_pos_accom_period2 + 
	-- v_pos_accom_period3 +
	-- v_pos_accom_period4 +
	-- v_pos_accom_period5 ) 
	(v_pos_accom_period1 + v_pos_accom_period2 + v_pos_accom_period3 + v_pos_accom_period4 + v_pos_accom_period5) AS o_pos_accom_period,
	-- *INF*: ( v_neg_accom_period1 +
	-- v_neg_accom_period2 + 
	-- v_neg_accom_period3 +
	-- v_neg_accom_period4 +
	-- v_neg_accom_period5 ) * -1
	(v_neg_accom_period1 + v_neg_accom_period2 + v_neg_accom_period3 + v_neg_accom_period4 + v_neg_accom_period5) * - 1 AS o_neg_accom_period,
	-- *INF*: IIF(v_restriction_accommodated1 = 'No' AND v_restriction_begin_date1 <> v_sysdate, 
	--  v_daily_benefit_rate1 * v_neg_accom_period1 , 0)
	-- 
	-- 
	-- ---- IF Accommodate1 is equal to 'No' and this is a valid record from the CLAIM_ANSWER table then calculate
	-- ---- the negative impact in dollars for this entry(1 of up to 5).
	IFF(
	    v_restriction_accommodated1 = 'No' AND v_restriction_begin_date1 <> v_sysdate,
	    v_daily_benefit_rate1 * v_neg_accom_period1,
	    0
	) AS v_neg_impact1,
	-- *INF*: IIF(v_restriction_accommodated2 = 'No' AND v_restriction_begin_date2 <> v_sysdate, 
	--  v_daily_benefit_rate2 * v_neg_accom_period2 , 0)
	-- 
	-- ---- IF Accommodate2 is equal to 'No' and this is a valid record from the CLAIM_ANSWER table then calculate
	-- ---- the negative impact in dollars for this entry(2 of up to 5).
	IFF(
	    v_restriction_accommodated2 = 'No' AND v_restriction_begin_date2 <> v_sysdate,
	    v_daily_benefit_rate2 * v_neg_accom_period2,
	    0
	) AS v_neg_impact2,
	-- *INF*: IIF(v_restriction_accommodated3 = 'No' AND v_restriction_begin_date3 <> v_sysdate, 
	--  v_daily_benefit_rate3 * v_neg_accom_period3 , 0)
	-- 
	-- ---- IF Accommodate3 is equal to 'No' and this is a valid record from the CLAIM_ANSWER table then calculate
	-- ---- the negative impact in dollars for this entry(3 of up to 5).
	IFF(
	    v_restriction_accommodated3 = 'No' AND v_restriction_begin_date3 <> v_sysdate,
	    v_daily_benefit_rate3 * v_neg_accom_period3,
	    0
	) AS v_neg_impact3,
	-- *INF*: IIF(v_restriction_accommodated4 = 'No' AND v_restriction_begin_date4 <> v_sysdate, 
	--  v_daily_benefit_rate4 * v_neg_accom_period4 , 0)
	-- 
	-- 
	-- ---- IF Accommodate3 is equal to 'No' and this is a valid record from the CLAIM_ANSWER table then calculate
	-- ---- the negative impact in dollars for this entry(3 of up to 5).
	IFF(
	    v_restriction_accommodated4 = 'No' AND v_restriction_begin_date4 <> v_sysdate,
	    v_daily_benefit_rate4 * v_neg_accom_period4,
	    0
	) AS v_neg_impact4,
	-- *INF*: IIF(v_restriction_accommodated5 = 'No' AND v_restriction_begin_date5 <> v_sysdate, 
	--  v_daily_benefit_rate5 * v_neg_accom_period5 , 0)
	-- 
	-- 
	-- ---- IF Accommodate5 is equal to 'No' and this is a valid record from the CLAIM_ANSWER table then calculate
	-- ---- the negative impact in dollars for this entry(5 of up to 5).
	IFF(
	    v_restriction_accommodated5 = 'No' AND v_restriction_begin_date5 <> v_sysdate,
	    v_daily_benefit_rate5 * v_neg_accom_period5,
	    0
	) AS v_neg_impact5,
	-- *INF*: IIF(v_restriction_accommodated1 = 'Yes' AND v_restriction_begin_date1 <> v_sysdate, 
	--  v_daily_benefit_rate1 * v_pos_accom_period1 , 0)
	-- 
	-- 
	-- ---- IF Accommodate1 is equal to 'Yes' and this is a valid record from the CLAIM_ANSWER table then calculate
	-- ---- the negative impact in dollars for this entry(1 of up to 5).
	IFF(
	    v_restriction_accommodated1 = 'Yes' AND v_restriction_begin_date1 <> v_sysdate,
	    v_daily_benefit_rate1 * v_pos_accom_period1,
	    0
	) AS v_pos_impact1,
	-- *INF*: IIF(v_restriction_accommodated2 = 'Yes' AND v_restriction_begin_date2 <> v_sysdate, 
	--  v_daily_benefit_rate2 * v_pos_accom_period2 , 0)
	-- 
	-- 
	-- 
	-- ---- IF Accommodate1 is equal to 'Yes' and this is a valid record from the CLAIM_ANSWER table then calculate
	-- ---- the negative impact in dollars for this entry(2 of up to 5).
	IFF(
	    v_restriction_accommodated2 = 'Yes' AND v_restriction_begin_date2 <> v_sysdate,
	    v_daily_benefit_rate2 * v_pos_accom_period2,
	    0
	) AS v_pos_impact2,
	-- *INF*: IIF(v_restriction_accommodated3 = 'Yes' AND v_restriction_begin_date3 <> v_sysdate, 
	--  v_daily_benefit_rate3 * v_pos_accom_period3 , 0)
	-- 
	-- 
	-- 
	-- ---- IF Accommodate3 is equal to 'Yes' and this is a valid record from the CLAIM_ANSWER table then calculate
	-- ---- the negative impact in dollars for this entry(1 of up to 5).
	IFF(
	    v_restriction_accommodated3 = 'Yes' AND v_restriction_begin_date3 <> v_sysdate,
	    v_daily_benefit_rate3 * v_pos_accom_period3,
	    0
	) AS v_pos_impact3,
	-- *INF*: IIF(v_restriction_accommodated4 = 'Yes' AND v_restriction_begin_date4 <> v_sysdate, 
	--  v_daily_benefit_rate4 * v_pos_accom_period4 , 0)
	-- 
	-- 
	-- 
	-- ---- IF Accommodate4 is equal to 'Yes' and this is a valid record from the CLAIM_ANSWER table then calculate
	-- ---- the negative impact in dollars for this entry(4 of up to 5).
	IFF(
	    v_restriction_accommodated4 = 'Yes' AND v_restriction_begin_date4 <> v_sysdate,
	    v_daily_benefit_rate4 * v_pos_accom_period4,
	    0
	) AS v_pos_impact4,
	-- *INF*: IIF(v_restriction_accommodated5 = 'Yes' AND v_restriction_begin_date5 <> v_sysdate, 
	--  v_daily_benefit_rate5 * v_pos_accom_period5 , 0)
	-- 
	-- ---- IF Accommodate5 is equal to 'Yes' and this is a valid record from the CLAIM_ANSWER table then calculate
	-- ---- the negative impact in dollars for this entry(5 of up to 5).
	IFF(
	    v_restriction_accommodated5 = 'Yes' AND v_restriction_begin_date5 <> v_sysdate,
	    v_daily_benefit_rate5 * v_pos_accom_period5,
	    0
	) AS v_pos_impact5,
	v_pos_impact1 +
v_pos_impact2 +
v_pos_impact3 +
v_pos_impact4 +
v_pos_impact5 AS o_pos_impact,
	-- *INF*: (v_neg_impact1 +
	-- v_neg_impact2 +
	-- v_neg_impact3 +
	-- v_neg_impact4 +
	-- v_neg_impact5) *  -1
	(v_neg_impact1 + v_neg_impact2 + v_neg_impact3 + v_neg_impact4 + v_neg_impact5) * - 1 AS o_neg_impact,
	-1 AS claim_med_id_out,
	'N/A' AS medicare_eligibility_out,
	-- *INF*: :LKP.LKP_CLAIM_ANSWER_QUES
	-- (edw_claim_party_occurrence_ak_id,'InjuredWorkerLostTime',  'Claimant.Disability.Questions')
	LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_InjuredWorkerLostTime_Claimant_Disability_Questions.optn_text AS v_InjuredWorkerLostTime,
	-- *INF*: IIF(isnull(v_InjuredWorkerLostTime),-1,TO_INTEGER(v_InjuredWorkerLostTime))
	IFF(v_InjuredWorkerLostTime IS NULL, - 1, CAST(v_InjuredWorkerLostTime AS INTEGER)) AS InjuredWorkerLostTime,
	JNR_claimant_dim_sources.AutomaticAdjudicationClaimIndicator AS in_AutomaticAdjudicationClaimIndicator,
	-- *INF*: iif(isnull(in_AutomaticAdjudicationClaimIndicator),'N/A',in_AutomaticAdjudicationClaimIndicator)
	IFF(
	    in_AutomaticAdjudicationClaimIndicator IS NULL, 'N/A',
	    in_AutomaticAdjudicationClaimIndicator
	) AS AutomaticAdjudicationClaimIndicator,
	LKP_SupCompensableClaimCode.CompensableClaimDescription AS in_CompensableClaimDescription,
	-- *INF*: iif(isnull(in_CompensableClaimDescription),'N/A',in_CompensableClaimDescription)
	IFF(in_CompensableClaimDescription IS NULL, 'N/A', in_CompensableClaimDescription) AS CompensableClaimDescription
	FROM JNR_claimant_dim_sources
	LEFT JOIN LKP_SUP_WC_ACTIVITY_STATUS
	ON LKP_SUP_WC_ACTIVITY_STATUS.act_status_code = JNR_claimant_dim_sources.act_status_code
	LEFT JOIN LKP_SUP_WC_CLAIM_CATG
	ON LKP_SUP_WC_CLAIM_CATG.claim_ctgry_code = JNR_claimant_dim_sources.claim_ctgry_code
	LEFT JOIN LKP_SUP_WC_SIC_CODE
	ON LKP_SUP_WC_SIC_CODE.sic_code = JNR_claimant_dim_sources.sic_code
	LEFT JOIN LKP_SUP_WC_WAGE_METHOD
	ON LKP_SUP_WC_WAGE_METHOD.wage_method_code = JNR_claimant_dim_sources.wage_method_code
	LEFT JOIN LKP_SupCompensableClaimCode
	ON LKP_SupCompensableClaimCode.SupCompensableClaimCode = JNR_claimant_dim_sources.SupCompensableClaimCode
	LEFT JOIN LKP_Workers_Comp_Claimant_Work_History
	ON LKP_Workers_Comp_Claimant_Work_History.wc_claimant_det_ak_id = EXP_WORK_HISTORY.wc_claimant_det_ak_id AND LKP_Workers_Comp_Claimant_Work_History.work_hist_created_date = EXP_WORK_HISTORY.o_max_created_date
	LEFT JOIN LKP_sup_claim_party_role_code
	ON LKP_sup_claim_party_role_code.claim_party_role_code = EXP_CMT_CLMT.claim_party_role_code_out
	LEFT JOIN LKP_sup_insured_type
	ON LKP_sup_insured_type.insd_type_code = JNR_claimant_dim_sources.insd_type_code
	LEFT JOIN LKP_sup_marital_status
	ON LKP_sup_marital_status.marital_status_code = JNR_claimant_dim_sources.marital_status
	LEFT JOIN LKP_sup_tax_filing_status
	ON LKP_sup_tax_filing_status.tax_filing_status_code = JNR_claimant_dim_sources.tax_filing_status
	LEFT JOIN LKP_sup_workers_comp_body_part
	ON LKP_sup_workers_comp_body_part.body_part_code = JNR_claimant_dim_sources.body_part_code
	LEFT JOIN LKP_sup_workers_comp_care_directed_by
	ON LKP_sup_workers_comp_care_directed_by.wc_care_directed_by_code = JNR_claimant_dim_sources.care_directed_by
	LEFT JOIN LKP_sup_workers_comp_cause_of_injury
	ON LKP_sup_workers_comp_cause_of_injury.cause_of_inj_code = JNR_claimant_dim_sources.cause_inj_code
	LEFT JOIN LKP_sup_workers_comp_employee_identification_type
	ON LKP_sup_workers_comp_employee_identification_type.emp_id_type = JNR_claimant_dim_sources.emp_id_type
	LEFT JOIN LKP_sup_workers_comp_employer_type
	ON LKP_sup_workers_comp_employer_type.emplyr_type_code = JNR_claimant_dim_sources.emplyr_type_code
	LEFT JOIN LKP_sup_workers_comp_employment_status
	ON LKP_sup_workers_comp_employment_status.wc_emplymnt_code = JNR_claimant_dim_sources.emplymnt_status_code
	LEFT JOIN LKP_sup_workers_comp_exemption_type
	ON LKP_sup_workers_comp_exemption_type.wc_exemption_type_code = JNR_claimant_dim_sources.exemption_type
	LEFT JOIN LKP_sup_workers_comp_loss_condition
	ON LKP_sup_workers_comp_loss_condition.loss_condition_code = JNR_claimant_dim_sources.loss_condition
	LEFT JOIN LKP_sup_workers_comp_managed_care_organization_type
	ON LKP_sup_workers_comp_managed_care_organization_type.managed_care_org_type = JNR_claimant_dim_sources.managed_care_org_type
	LEFT JOIN LKP_sup_workers_comp_nature_of_injury
	ON LKP_sup_workers_comp_nature_of_injury.nature_of_inj_code = JNR_claimant_dim_sources.nature_inj_code
	LEFT JOIN LKP_sup_workers_comp_occupation
	ON LKP_sup_workers_comp_occupation.occuptn_code = JNR_claimant_dim_sources.occuptn_code
	LEFT JOIN LKP_sup_workers_comp_premises_type
	ON LKP_sup_workers_comp_premises_type.premises_code = JNR_claimant_dim_sources.premises_code
	LEFT JOIN LKP_sup_workers_comp_return_to_work_type
	ON LKP_sup_workers_comp_return_to_work_type.return_to_work_code = JNR_claimant_dim_sources.return_to_work_type
	LEFT JOIN LKP_sup_workers_comp_wage_gross_amount_type
	ON LKP_sup_workers_comp_wage_gross_amount_type.wage_gross_amt_type = JNR_claimant_dim_sources.gross_amt_type
	LEFT JOIN LKP_sup_workers_comp_wage_period
	ON LKP_sup_workers_comp_wage_period.wage_period_code = JNR_claimant_dim_sources.wage_period_code
	LEFT JOIN LKP_SUP_STATE LKP_SUP_STATE_ltrim_rtrim_in_jurisdiction_state_code
	ON LKP_SUP_STATE_ltrim_rtrim_in_jurisdiction_state_code.state_code = ltrim(rtrim(in_jurisdiction_state_code))

	LEFT JOIN LKP_SUP_STATE LKP_SUP_STATE_ltrim_rtrim_in_hired_state_code
	ON LKP_SUP_STATE_ltrim_rtrim_in_hired_state_code.state_code = ltrim(rtrim(in_hired_state_code))

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciWasClaimCompensabilityDisputed_Claim_GeneralCase_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciWasClaimCompensabilityDisputed_Claim_GeneralCase_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciWasClaimCompensabilityDisputed_Claim_GeneralCase_Questions.logical_name = 'NcciDciWasClaimCompensabilityDisputed'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciWasClaimCompensabilityDisputed_Claim_GeneralCase_Questions.app_context_entity_name = 'Claim.GeneralCase.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_ANS LKP_CLAIM_ANSWER_ANS_edw_claim_party_occurrence_ak_id_NcciDciLossType_Claim_GeneralCase_Questions
	ON LKP_CLAIM_ANSWER_ANS_edw_claim_party_occurrence_ak_id_NcciDciLossType_Claim_GeneralCase_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_ANS_edw_claim_party_occurrence_ak_id_NcciDciLossType_Claim_GeneralCase_Questions.logical_name = 'NcciDciLossType'
	AND LKP_CLAIM_ANSWER_ANS_edw_claim_party_occurrence_ak_id_NcciDciLossType_Claim_GeneralCase_Questions.app_context_entity_name = 'Claim.GeneralCase.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciLossType_Claim_GeneralCase_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciLossType_Claim_GeneralCase_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciLossType_Claim_GeneralCase_Questions.logical_name = 'NcciDciLossType'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciLossType_Claim_GeneralCase_Questions.app_context_entity_name = 'Claim.GeneralCase.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_ANS LKP_CLAIM_ANSWER_ANS_edw_claim_party_occurrence_ak_id_NcciDciMethodofDeterminingAww_Claimant_GeneralCase_Questions
	ON LKP_CLAIM_ANSWER_ANS_edw_claim_party_occurrence_ak_id_NcciDciMethodofDeterminingAww_Claimant_GeneralCase_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_ANS_edw_claim_party_occurrence_ak_id_NcciDciMethodofDeterminingAww_Claimant_GeneralCase_Questions.logical_name = 'NcciDciMethodofDeterminingAww'
	AND LKP_CLAIM_ANSWER_ANS_edw_claim_party_occurrence_ak_id_NcciDciMethodofDeterminingAww_Claimant_GeneralCase_Questions.app_context_entity_name = 'Claimant.GeneralCase.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciMethodofDeterminingAww_Claimant_GeneralCase_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciMethodofDeterminingAww_Claimant_GeneralCase_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciMethodofDeterminingAww_Claimant_GeneralCase_Questions.logical_name = 'NcciDciMethodofDeterminingAww'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciMethodofDeterminingAww_Claimant_GeneralCase_Questions.app_context_entity_name = 'Claimant.GeneralCase.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciPostInjuryWeeklyWageAmount_Claimant_GeneralCase_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciPostInjuryWeeklyWageAmount_Claimant_GeneralCase_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciPostInjuryWeeklyWageAmount_Claimant_GeneralCase_Questions.logical_name = 'NcciDciPostInjuryWeeklyWageAmount'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciPostInjuryWeeklyWageAmount_Claimant_GeneralCase_Questions.app_context_entity_name = 'Claimant.GeneralCase.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciBAWDisabilityPercentage_Claim_GeneralCase_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciBAWDisabilityPercentage_Claim_GeneralCase_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciBAWDisabilityPercentage_Claim_GeneralCase_Questions.logical_name = 'NcciDciBAWDisabilityPercentage'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciBAWDisabilityPercentage_Claim_GeneralCase_Questions.app_context_entity_name = 'Claim.GeneralCase.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciBodyPartDisabilityPercentage_Claim_GeneralCase_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciBodyPartDisabilityPercentage_Claim_GeneralCase_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciBodyPartDisabilityPercentage_Claim_GeneralCase_Questions.logical_name = 'NcciDciBodyPartDisabilityPercentage'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciBodyPartDisabilityPercentage_Claim_GeneralCase_Questions.app_context_entity_name = 'Claim.GeneralCase.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_ANS LKP_CLAIM_ANSWER_ANS_edw_claim_party_occurrence_ak_id_NcciDciImpairmentPercentageBasis_Claim_GeneralCase_Questions
	ON LKP_CLAIM_ANSWER_ANS_edw_claim_party_occurrence_ak_id_NcciDciImpairmentPercentageBasis_Claim_GeneralCase_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_ANS_edw_claim_party_occurrence_ak_id_NcciDciImpairmentPercentageBasis_Claim_GeneralCase_Questions.logical_name = 'NcciDciImpairmentPercentageBasis'
	AND LKP_CLAIM_ANSWER_ANS_edw_claim_party_occurrence_ak_id_NcciDciImpairmentPercentageBasis_Claim_GeneralCase_Questions.app_context_entity_name = 'Claim.GeneralCase.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciImpairmentPercentageBasis_Claim_GeneralCase_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciImpairmentPercentageBasis_Claim_GeneralCase_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciImpairmentPercentageBasis_Claim_GeneralCase_Questions.logical_name = 'NcciDciImpairmentPercentageBasis'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciImpairmentPercentageBasis_Claim_GeneralCase_Questions.app_context_entity_name = 'Claim.GeneralCase.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciWereMedicalPaymentsExtinguished_Claim_GeneralCase_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciWereMedicalPaymentsExtinguished_Claim_GeneralCase_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciWereMedicalPaymentsExtinguished_Claim_GeneralCase_Questions.logical_name = 'NcciDciWereMedicalPaymentsExtinguished'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_NcciDciWereMedicalPaymentsExtinguished_Claim_GeneralCase_Questions.app_context_entity_name = 'Claim.GeneralCase.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_CurrentWorkStatus_Claimant_Disability_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_CurrentWorkStatus_Claimant_Disability_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_CurrentWorkStatus_Claimant_Disability_Questions.logical_name = 'CurrentWorkStatus'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_CurrentWorkStatus_Claimant_Disability_Questions.app_context_entity_name = 'Claimant.Disability.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateBegin1_Claimant_Disability_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateBegin1_Claimant_Disability_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateBegin1_Claimant_Disability_Questions.logical_name = 'RestrictionDateBegin1'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateBegin1_Claimant_Disability_Questions.app_context_entity_name = 'Claimant.Disability.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateBegin2_Claimant_Disability_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateBegin2_Claimant_Disability_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateBegin2_Claimant_Disability_Questions.logical_name = 'RestrictionDateBegin2'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateBegin2_Claimant_Disability_Questions.app_context_entity_name = 'Claimant.Disability.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateBegin3_Claimant_Disability_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateBegin3_Claimant_Disability_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateBegin3_Claimant_Disability_Questions.logical_name = 'RestrictionDateBegin3'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateBegin3_Claimant_Disability_Questions.app_context_entity_name = 'Claimant.Disability.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateBegin4_Claimant_Disability_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateBegin4_Claimant_Disability_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateBegin4_Claimant_Disability_Questions.logical_name = 'RestrictionDateBegin4'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateBegin4_Claimant_Disability_Questions.app_context_entity_name = 'Claimant.Disability.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateBegin5_Claimant_Disability_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateBegin5_Claimant_Disability_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateBegin5_Claimant_Disability_Questions.logical_name = 'RestrictionDateBegin5'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateBegin5_Claimant_Disability_Questions.app_context_entity_name = 'Claimant.Disability.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateEnd1_Claimant_Disability_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateEnd1_Claimant_Disability_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateEnd1_Claimant_Disability_Questions.logical_name = 'RestrictionDateEnd1'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateEnd1_Claimant_Disability_Questions.app_context_entity_name = 'Claimant.Disability.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateEnd2_Claimant_Disability_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateEnd2_Claimant_Disability_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateEnd2_Claimant_Disability_Questions.logical_name = 'RestrictionDateEnd2'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateEnd2_Claimant_Disability_Questions.app_context_entity_name = 'Claimant.Disability.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateEnd3_Claimant_Disability_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateEnd3_Claimant_Disability_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateEnd3_Claimant_Disability_Questions.logical_name = 'RestrictionDateEnd3'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateEnd3_Claimant_Disability_Questions.app_context_entity_name = 'Claimant.Disability.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateEnd4_Claimant_Disability_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateEnd4_Claimant_Disability_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateEnd4_Claimant_Disability_Questions.logical_name = 'RestrictionDateEnd4'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateEnd4_Claimant_Disability_Questions.app_context_entity_name = 'Claimant.Disability.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateEnd5_Claimant_Disability_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateEnd5_Claimant_Disability_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateEnd5_Claimant_Disability_Questions.logical_name = 'RestrictionDateEnd5'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionDateEnd5_Claimant_Disability_Questions.app_context_entity_name = 'Claimant.Disability.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionAccommodated1_Claimant_Disability_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionAccommodated1_Claimant_Disability_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionAccommodated1_Claimant_Disability_Questions.logical_name = 'RestrictionAccommodated1'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionAccommodated1_Claimant_Disability_Questions.app_context_entity_name = 'Claimant.Disability.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionAccommodated2_Claimant_Disability_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionAccommodated2_Claimant_Disability_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionAccommodated2_Claimant_Disability_Questions.logical_name = 'RestrictionAccommodated2'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionAccommodated2_Claimant_Disability_Questions.app_context_entity_name = 'Claimant.Disability.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionAccommodated3_Claimant_Disability_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionAccommodated3_Claimant_Disability_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionAccommodated3_Claimant_Disability_Questions.logical_name = 'RestrictionAccommodated3'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionAccommodated3_Claimant_Disability_Questions.app_context_entity_name = 'Claimant.Disability.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionAccommodated4_Claimant_Disability_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionAccommodated4_Claimant_Disability_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionAccommodated4_Claimant_Disability_Questions.logical_name = 'RestrictionAccommodated4'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionAccommodated4_Claimant_Disability_Questions.app_context_entity_name = 'Claimant.Disability.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionAccommodated5_Claimant_Disability_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionAccommodated5_Claimant_Disability_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionAccommodated5_Claimant_Disability_Questions.logical_name = 'RestrictionAccommodated5'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_RestrictionAccommodated5_Claimant_Disability_Questions.app_context_entity_name = 'Claimant.Disability.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_DailyBenefitRate1_Claimant_Disability_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_DailyBenefitRate1_Claimant_Disability_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_DailyBenefitRate1_Claimant_Disability_Questions.logical_name = 'DailyBenefitRate1'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_DailyBenefitRate1_Claimant_Disability_Questions.app_context_entity_name = 'Claimant.Disability.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_DailyBenefitRate2_Claimant_Disability_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_DailyBenefitRate2_Claimant_Disability_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_DailyBenefitRate2_Claimant_Disability_Questions.logical_name = 'DailyBenefitRate2'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_DailyBenefitRate2_Claimant_Disability_Questions.app_context_entity_name = 'Claimant.Disability.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_DailyBenefitRate3_Claimant_Disability_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_DailyBenefitRate3_Claimant_Disability_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_DailyBenefitRate3_Claimant_Disability_Questions.logical_name = 'DailyBenefitRate3'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_DailyBenefitRate3_Claimant_Disability_Questions.app_context_entity_name = 'Claimant.Disability.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_DailyBenefitRate4_Claimant_Disability_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_DailyBenefitRate4_Claimant_Disability_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_DailyBenefitRate4_Claimant_Disability_Questions.logical_name = 'DailyBenefitRate4'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_DailyBenefitRate4_Claimant_Disability_Questions.app_context_entity_name = 'Claimant.Disability.Questions'

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_DailyBenefitRate5_Claimant_Disability_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_DailyBenefitRate5_Claimant_Disability_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_DailyBenefitRate5_Claimant_Disability_Questions.logical_name = 'DailyBenefitRate5'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_DailyBenefitRate5_Claimant_Disability_Questions.app_context_entity_name = 'Claimant.Disability.Questions'

	LEFT JOIN LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_begin_date1
	ON LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_begin_date1.clndr_date = v_restriction_begin_date1

	LEFT JOIN LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_begin_date1
	ON LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_begin_date1.clndr_date = v_restriction_begin_date1

	LEFT JOIN LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_begin_date2
	ON LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_begin_date2.clndr_date = v_restriction_begin_date2

	LEFT JOIN LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_begin_date2
	ON LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_begin_date2.clndr_date = v_restriction_begin_date2

	LEFT JOIN LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_begin_date3
	ON LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_begin_date3.clndr_date = v_restriction_begin_date3

	LEFT JOIN LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_begin_date3
	ON LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_begin_date3.clndr_date = v_restriction_begin_date3

	LEFT JOIN LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_begin_date4
	ON LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_begin_date4.clndr_date = v_restriction_begin_date4

	LEFT JOIN LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_begin_date4
	ON LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_begin_date4.clndr_date = v_restriction_begin_date4

	LEFT JOIN LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_begin_date5
	ON LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_begin_date5.clndr_date = v_restriction_begin_date5

	LEFT JOIN LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_begin_date5
	ON LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_begin_date5.clndr_date = v_restriction_begin_date5

	LEFT JOIN LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_end_date1
	ON LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_end_date1.clndr_date = v_restriction_end_date1

	LEFT JOIN LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_end_date1
	ON LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_end_date1.clndr_date = v_restriction_end_date1

	LEFT JOIN LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_end_date2
	ON LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_end_date2.clndr_date = v_restriction_end_date2

	LEFT JOIN LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_end_date2
	ON LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_end_date2.clndr_date = v_restriction_end_date2

	LEFT JOIN LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_end_date3
	ON LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_end_date3.clndr_date = v_restriction_end_date3

	LEFT JOIN LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_end_date3
	ON LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_end_date3.clndr_date = v_restriction_end_date3

	LEFT JOIN LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_end_date4
	ON LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_end_date4.clndr_date = v_restriction_end_date4

	LEFT JOIN LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_end_date4
	ON LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_end_date4.clndr_date = v_restriction_end_date4

	LEFT JOIN LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_end_date5
	ON LKP_WORK_JURISDICTIONAL_WORKING_DAY_SIX_v_restriction_end_date5.clndr_date = v_restriction_end_date5

	LEFT JOIN LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_end_date5
	ON LKP_WORK_JURISDICTIONAL_WORKING_DAY_FIVE_v_restriction_end_date5.clndr_date = v_restriction_end_date5

	LEFT JOIN LKP_CLAIM_ANSWER_QUES LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_InjuredWorkerLostTime_Claimant_Disability_Questions
	ON LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_InjuredWorkerLostTime_Claimant_Disability_Questions.claim_party_occurrence_ak_id = edw_claim_party_occurrence_ak_id
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_InjuredWorkerLostTime_Claimant_Disability_Questions.logical_name = 'InjuredWorkerLostTime'
	AND LKP_CLAIM_ANSWER_QUES_edw_claim_party_occurrence_ak_id_InjuredWorkerLostTime_Claimant_Disability_Questions.app_context_entity_name = 'Claimant.Disability.Questions'

),
EXP_CHECK_VALIDITY AS (
	SELECT
	claimant_date_type_out,
	claimant_direct_loss_status_code_out,
	claimant_exp_status_code_out,
	claimant_subrogation_status_code_out,
	claimant_salvage_status_code_out,
	claimant_other_recovery_status_code_out,
	-- *INF*: DECODE(TRUE,
	-- SUBSTR(claimant_date_type_out,1,6) = 'CLOSED' AND 
	-- (claimant_direct_loss_status_code_out = 'OPEN' OR
	-- claimant_exp_status_code_out = 'OPEN' OR
	-- claimant_subrogation_status_code_out = 'OPEN' OR
	-- claimant_salvage_status_code_out = 'OPEN' OR
	-- claimant_other_recovery_status_code_out = 'OPEN' OR 
	-- claimant_direct_loss_status_code_out = 'REOPEN' OR
	-- claimant_exp_status_code_out = 'REOPEN' OR
	-- claimant_subrogation_status_code_out = 'REOPEN' OR
	-- claimant_salvage_status_code_out = 'REOPEN' OR
	-- claimant_other_recovery_status_code_out = 'REOPEN'), 'INVALID',
	-- 
	-- 
	-- (SUBSTR(claimant_date_type_out,1,4) = 'OPEN' OR SUBSTR(claimant_date_type_out,1,6) = 'REOPEN' OR SUBSTR(claimant_date_type_out,1,6) = 'CLOSED')
	-- AND
	-- (
	-- SUBSTR(claimant_direct_loss_status_code_out,1,6) = 'N/A' AND
	-- SUBSTR(claimant_exp_status_code_out,1,6) = 'N/A' AND
	-- SUBSTR(claimant_subrogation_status_code_out,1,6) = 'N/A' AND
	-- SUBSTR(claimant_salvage_status_code_out,1,6) = 'N/A' AND
	-- SUBSTR(claimant_other_recovery_status_code_out,1,6) = 'N/A' 
	-- ), 'VALID',
	-- 
	-- 
	-- (SUBSTR(claimant_date_type_out,1,4) = 'OPEN' OR SUBSTR(claimant_date_type_out,1,6) = 'REOPEN')
	-- AND
	-- (
	-- (SUBSTR(claimant_direct_loss_status_code_out,1,6) = 'CLOSED' OR claimant_direct_loss_status_code_out = 'N/A' OR claimant_direct_loss_status_code_out = 'NOTICEONLY')
	-- AND
	-- (SUBSTR(claimant_exp_status_code_out,1,6) = 'CLOSED' OR claimant_exp_status_code_out = 'N/A' OR claimant_exp_status_code_out = 'NOTICEONLY')
	-- AND
	-- (SUBSTR(claimant_subrogation_status_code_out,1,6) = 'CLOSED' OR claimant_subrogation_status_code_out = 'N/A' OR claimant_subrogation_status_code_out = 'NOTICEONLY')
	-- AND
	-- (SUBSTR(claimant_salvage_status_code_out,1,6) = 'CLOSED' OR claimant_salvage_status_code_out = 'N/A'  OR claimant_salvage_status_code_out = 'NOTICEONLY')
	-- AND
	-- (SUBSTR(claimant_other_recovery_status_code_out,1,6) = 'CLOSED' OR claimant_other_recovery_status_code_out = 'N/A'  OR claimant_other_recovery_status_code_out = 'NOTICEONLY')
	-- ), 'INVALID',
	-- 
	-- 
	-- claimant_date_type_out = 'NOTICEONLY' AND 
	-- (
	-- claimant_direct_loss_status_code_out = 'OPEN' OR 
	-- claimant_direct_loss_status_code_out = 'REOPEN' OR 
	-- SUBSTR(claimant_direct_loss_status_code_out,1,6) = 'CLOSED' OR 
	-- claimant_exp_status_code_out = 'OPEN' OR 
	-- claimant_exp_status_code_out = 'REOPEN' OR 
	-- SUBSTR(claimant_exp_status_code_out,1,6) = 'CLOSED' OR 
	-- claimant_subrogation_status_code_out = 'OPEN' OR 
	-- claimant_subrogation_status_code_out = 'REOPEN' OR 
	-- SUBSTR(claimant_subrogation_status_code_out,1,6) = 'CLOSED' OR 
	-- claimant_salvage_status_code_out = 'OPEN' OR 
	-- claimant_salvage_status_code_out = 'REOPEN' OR 
	-- SUBSTR(claimant_salvage_status_code_out,1,6) = 'CLOSED' OR 
	-- claimant_other_recovery_status_code_out = 'OPEN' OR 
	-- claimant_other_recovery_status_code_out = 'REOPEN' OR 
	-- SUBSTR(claimant_other_recovery_status_code_out,1,6) = 'CLOSED'  
	-- )
	-- ,'INVALID',
	-- 
	-- SUBSTR(claimant_date_type_out,1,6) = 'CLOSED' AND
	-- SUBSTR(claimant_direct_loss_status_code_out,1,6) <> 'CLOSED' AND 
	-- SUBSTR(claimant_exp_status_code_out,1,6) <> 'CLOSED' AND 
	-- SUBSTR(claimant_salvage_status_code_out,1,6) <> 'CLOSED' AND 
	-- SUBSTR(claimant_subrogation_status_code_out,1,6) <> 'CLOSED' AND 
	-- SUBSTR(claimant_other_recovery_status_code_out,1,6) <> 'CLOSED' , 'INVALID', 
	-- 
	-- 'VALID')
	-- 
	-- //----------------------------------------------------------------------------------------------------
	-- //This expression eliminates any invalid combination of reserve and non-reserve status.
	-- //If Claimant Status is CLOSED AND if any of the other fin typ code status is either open or reopen then 
	-- //filter the row out.
	-- // If Claimant Status is Open or Reopen AND if any of the other fin typ code status is either open or reopen 
	-- //then DON'T filter the row out.
	-- // If Claimant Status is NOTICEONLY AND all the other fin typ code status also NOTICEONLY or N/A then 
	-- //DON'T filter the row out
	-- //If NR Status is Open/Openedinerror/Openwithnofinancial and all fin types are N/A then DONT filter
	-- //If NR Status is Closed and if none of the other fin type status is closed, then filter the row
	-- //----------------------------------------------------------------------------------------------------
	DECODE(
	    TRUE,
	    SUBSTR(claimant_date_type_out, 1, 6) = 'CLOSED' AND (claimant_direct_loss_status_code_out = 'OPEN' OR claimant_exp_status_code_out = 'OPEN' OR claimant_subrogation_status_code_out = 'OPEN' OR claimant_salvage_status_code_out = 'OPEN' OR claimant_other_recovery_status_code_out = 'OPEN' OR claimant_direct_loss_status_code_out = 'REOPEN' OR claimant_exp_status_code_out = 'REOPEN' OR claimant_subrogation_status_code_out = 'REOPEN' OR claimant_salvage_status_code_out = 'REOPEN' OR claimant_other_recovery_status_code_out = 'REOPEN'), 'INVALID',
	    (SUBSTR(claimant_date_type_out, 1, 4) = 'OPEN' OR SUBSTR(claimant_date_type_out, 1, 6) = 'REOPEN' OR SUBSTR(claimant_date_type_out, 1, 6) = 'CLOSED') AND (SUBSTR(claimant_direct_loss_status_code_out, 1, 6) = 'N/A' AND SUBSTR(claimant_exp_status_code_out, 1, 6) = 'N/A' AND SUBSTR(claimant_subrogation_status_code_out, 1, 6) = 'N/A' AND SUBSTR(claimant_salvage_status_code_out, 1, 6) = 'N/A' AND SUBSTR(claimant_other_recovery_status_code_out, 1, 6) = 'N/A'), 'VALID',
	    (SUBSTR(claimant_date_type_out, 1, 4) = 'OPEN' OR SUBSTR(claimant_date_type_out, 1, 6) = 'REOPEN') AND ((SUBSTR(claimant_direct_loss_status_code_out, 1, 6) = 'CLOSED' OR claimant_direct_loss_status_code_out = 'N/A' OR claimant_direct_loss_status_code_out = 'NOTICEONLY') AND (SUBSTR(claimant_exp_status_code_out, 1, 6) = 'CLOSED' OR claimant_exp_status_code_out = 'N/A' OR claimant_exp_status_code_out = 'NOTICEONLY') AND (SUBSTR(claimant_subrogation_status_code_out, 1, 6) = 'CLOSED' OR claimant_subrogation_status_code_out = 'N/A' OR claimant_subrogation_status_code_out = 'NOTICEONLY') AND (SUBSTR(claimant_salvage_status_code_out, 1, 6) = 'CLOSED' OR claimant_salvage_status_code_out = 'N/A' OR claimant_salvage_status_code_out = 'NOTICEONLY') AND (SUBSTR(claimant_other_recovery_status_code_out, 1, 6) = 'CLOSED' OR claimant_other_recovery_status_code_out = 'N/A' OR claimant_other_recovery_status_code_out = 'NOTICEONLY')), 'INVALID',
	    claimant_date_type_out = 'NOTICEONLY' AND (claimant_direct_loss_status_code_out = 'OPEN' OR claimant_direct_loss_status_code_out = 'REOPEN' OR SUBSTR(claimant_direct_loss_status_code_out, 1, 6) = 'CLOSED' OR claimant_exp_status_code_out = 'OPEN' OR claimant_exp_status_code_out = 'REOPEN' OR SUBSTR(claimant_exp_status_code_out, 1, 6) = 'CLOSED' OR claimant_subrogation_status_code_out = 'OPEN' OR claimant_subrogation_status_code_out = 'REOPEN' OR SUBSTR(claimant_subrogation_status_code_out, 1, 6) = 'CLOSED' OR claimant_salvage_status_code_out = 'OPEN' OR claimant_salvage_status_code_out = 'REOPEN' OR SUBSTR(claimant_salvage_status_code_out, 1, 6) = 'CLOSED' OR claimant_other_recovery_status_code_out = 'OPEN' OR claimant_other_recovery_status_code_out = 'REOPEN' OR SUBSTR(claimant_other_recovery_status_code_out, 1, 6) = 'CLOSED'), 'INVALID',
	    SUBSTR(claimant_date_type_out, 1, 6) = 'CLOSED' AND SUBSTR(claimant_direct_loss_status_code_out, 1, 6) <> 'CLOSED' AND SUBSTR(claimant_exp_status_code_out, 1, 6) <> 'CLOSED' AND SUBSTR(claimant_salvage_status_code_out, 1, 6) <> 'CLOSED' AND SUBSTR(claimant_subrogation_status_code_out, 1, 6) <> 'CLOSED' AND SUBSTR(claimant_other_recovery_status_code_out, 1, 6) <> 'CLOSED', 'INVALID',
	    'VALID'
	) AS ROW_VALID
	FROM EXP_claimant_dim
),
LKP_claimant_dim AS (
	SELECT
	claimant_dim_id,
	edw_claim_party_occurrence_pk_id,
	edw_claim_party_pk_id,
	edw_claimant_calculation_pk_id,
	edw_claimant_reserve_calculation_direct_loss_pk_id,
	edw_claimant_reserve_calculation_exp_pk_id,
	edw_claimant_reserve_calculation_subrogation_pk_id,
	edw_claimant_reserve_calculation_salvage_pk_id,
	edw_claimant_reserve_calculation_other_recovery_pk_id,
	edw_wc_claimant_det_pk_id,
	edw_wc_claimant_work_hist_pk_id
	FROM (
		SELECT 
			claimant_dim_id,
			edw_claim_party_occurrence_pk_id,
			edw_claim_party_pk_id,
			edw_claimant_calculation_pk_id,
			edw_claimant_reserve_calculation_direct_loss_pk_id,
			edw_claimant_reserve_calculation_exp_pk_id,
			edw_claimant_reserve_calculation_subrogation_pk_id,
			edw_claimant_reserve_calculation_salvage_pk_id,
			edw_claimant_reserve_calculation_other_recovery_pk_id,
			edw_wc_claimant_det_pk_id,
			edw_wc_claimant_work_hist_pk_id
		FROM claimant_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_party_occurrence_pk_id,edw_claim_party_pk_id,edw_claimant_calculation_pk_id,edw_claimant_reserve_calculation_direct_loss_pk_id,edw_claimant_reserve_calculation_exp_pk_id,edw_claimant_reserve_calculation_subrogation_pk_id,edw_claimant_reserve_calculation_salvage_pk_id,edw_claimant_reserve_calculation_other_recovery_pk_id,edw_wc_claimant_det_pk_id,edw_wc_claimant_work_hist_pk_id ORDER BY claimant_dim_id DESC) = 1
),
RTR_claimant_dim AS (
	SELECT
	LKP_claimant_dim.claimant_dim_id,
	EXP_claimant_dim.edw_claim_party_pk_id_out AS claim_party_id,
	EXP_claimant_dim.edw_claim_party_occurrence_pk_id_out AS claim_party_occurrence_id,
	EXP_claimant_dim.edw_claimant_calculation_pk_id_out AS claimant_calculation_id,
	EXP_claimant_dim.edw_claimant_reserve_calculation_direct_loss_pk_id_out AS claimant_reserve_calculation_id_D,
	EXP_claimant_dim.edw_claimant_reserve_calculation_exp_pk_id_out AS claimant_reserve_calculation_id_E,
	EXP_claimant_dim.edw_claimant_reserve_calculation_subrogation_pk_id_out AS claimant_reserve_calculation_id_B,
	EXP_claimant_dim.edw_claimant_reserve_calculation_salvage_pk_id_out AS claimant_reserve_calculation_id_S,
	EXP_claimant_dim.edw_claimant_reserve_calculation_other_recovery_pk_id_out AS claimant_reserve_calculation_id_R,
	EXP_claimant_dim.wc_claimant_det_pk_id_out AS wc_claimant_det_id,
	EXP_claimant_dim.wc_claimant_work_hist_pk_id_out AS wc_claimant_work_hist_pk_id,
	EXP_claimant_dim.wc_claimant_num_out AS wc_claimant_num,
	EXP_claimant_dim.max_med_improvement_date,
	EXP_claimant_dim.edw_claim_party_occurrence_ak_id AS claim_party_occurrence_ak_id,
	EXP_claimant_dim.in_edw_claim_case_ak_id,
	EXP_claimant_dim.claimant_date_type_out AS claimant_status_code,
	EXP_claimant_dim.claimant_direct_loss_status_code_out AS claimant_direct_loss_status_code,
	EXP_claimant_dim.claimant_exp_status_code_out,
	EXP_claimant_dim.claimant_subrogation_status_code_out AS claimant_subrogation_status_code,
	EXP_claimant_dim.claimant_salvage_status_code_out AS claimant_salvage_status_code,
	EXP_claimant_dim.claimant_other_recovery_status_code_out AS claimant_other_recovery_status_code,
	EXP_claimant_dim.claimant_reopen_ind_out AS claimant_reopen_ind,
	EXP_claimant_dim.claimant_supplemental_ind_out AS claimant_supplemental_ind,
	EXP_claimant_dim.claimant_financial_ind_out AS claimant_financial_ind,
	EXP_claimant_dim.claimant_recovery_ind_out AS claimant_recovery_ind,
	EXP_claimant_dim.claimant_notice_only_claim_ind_out AS claimant_notice_only_ind,
	EXP_claimant_dim.claimant_open_date,
	EXP_claimant_dim.claimant_close_date,
	EXP_claimant_dim.claimant_reopen_date,
	EXP_claimant_dim.claimant_closed_after_reopen_date,
	EXP_claimant_dim.claimant_noticeonly_date,
	EXP_claimant_dim.claim_party_addr_type AS addr_type,
	EXP_claimant_dim.claim_party_zip,
	EXP_claimant_dim.claim_party_state,
	EXP_claimant_dim.claim_party_county,
	EXP_claimant_dim.claim_party_city,
	EXP_claimant_dim.claim_party_addr,
	EXP_claimant_dim.claim_party_full_name,
	EXP_claimant_dim.claim_party_first_name,
	EXP_claimant_dim.claim_party_last_name,
	EXP_claimant_dim.claim_party_mid_name,
	EXP_claimant_dim.claim_party_tax_ssn_id AS tax_ssn_id,
	EXP_claimant_dim.claim_party_tax_fed_id AS tax_fed_id,
	EXP_claimant_dim.claim_party_birthdate,
	EXP_claimant_dim.claim_party_gndr,
	EXP_claimant_dim.claim_party_role_code_out,
	EXP_claimant_dim.out_claim_party_role_code_descript,
	EXP_claimant_dim.out_claimant_num AS claimant_num,
	EXP_claimant_dim.denial_date_out,
	EXP_claimant_dim.jurisdiction_state_code_out,
	EXP_claimant_dim.jurisdiction_state_descript_OUT,
	EXP_claimant_dim.emplyr_notified_date_out,
	EXP_claimant_dim.rpted_to_carrier_date_out,
	EXP_claimant_dim.jurisdiction_claim_num_out,
	EXP_claimant_dim.care_directed_ind_out,
	EXP_claimant_dim.care_directed_by_out,
	EXP_claimant_dim.wc_care_directed_by_descript_OUT,
	EXP_claimant_dim.hired_state_code_out,
	EXP_claimant_dim.hired_state_descript_OUT,
	EXP_claimant_dim.hired_date_out,
	EXP_claimant_dim.tax_filing_status_out,
	EXP_claimant_dim.tax_filing_status_descript_OUT,
	EXP_claimant_dim.occuptn_code_out,
	EXP_claimant_dim.occuptn_descript_OUT,
	EXP_claimant_dim.employement_status_code_out,
	EXP_claimant_dim.wc_emplymnt_descript_OUT,
	EXP_claimant_dim.len_of_time_in_crrnt_job_out,
	EXP_claimant_dim.emp_dept_name_out,
	EXP_claimant_dim.emp_shift_num_out,
	EXP_claimant_dim.marital_status_out,
	EXP_claimant_dim.marital_status_descript_OUT,
	EXP_claimant_dim.num_of_dependents_out,
	EXP_claimant_dim.num_of_dependent_children_out,
	EXP_claimant_dim.num_of_other_dependents_out,
	EXP_claimant_dim.num_of_exemptions_out,
	EXP_claimant_dim.exemption_type_out,
	EXP_claimant_dim.wc_exemption_type_descript_OUT,
	EXP_claimant_dim.emp_blind_ind_out,
	EXP_claimant_dim.emp_over_65_ind_out,
	EXP_claimant_dim.spouse_blind_ind_out,
	EXP_claimant_dim.spouse_over_65_ind_out,
	EXP_claimant_dim.education_lvl_out,
	EXP_claimant_dim.med_auth_ind_out,
	EXP_claimant_dim.auth_to_release_ssn_ind_out,
	EXP_claimant_dim.emp_id_num_out,
	EXP_claimant_dim.emp_id_type_out,
	EXP_claimant_dim.emp_id_type_descript_OUT,
	EXP_claimant_dim.emp_part_time_hour_week_out,
	EXP_claimant_dim.emp_dept_num_out,
	EXP_claimant_dim.emp_part_time_hourly_week_rate_amt_out,
	EXP_claimant_dim.wage_rate_amt_out,
	EXP_claimant_dim.wage_period_code_out,
	EXP_claimant_dim.wage_period_descript_OUT,
	EXP_claimant_dim.wage_eff_date_out,
	EXP_claimant_dim.weeks_worked_out,
	EXP_claimant_dim.gross_amt_type_out,
	EXP_claimant_dim.wage_gross_amt_type_descript_OUT,
	EXP_claimant_dim.gross_wage_amt_excluding_tips_out,
	EXP_claimant_dim.piece_work_num_of_weeks_excluding_overtime_out,
	EXP_claimant_dim.emp_rec_meals_out,
	EXP_claimant_dim.emp_rec_room_out,
	EXP_claimant_dim.emp_rec_tips_out,
	EXP_claimant_dim.overtime_amt_out,
	EXP_claimant_dim.overtime_after_hour_in_a_week_out,
	EXP_claimant_dim.overtime_after_hour_in_a_day_out,
	EXP_claimant_dim.full_pay_inj_day_ind_out,
	EXP_claimant_dim.salary_paid_ind_out,
	EXP_claimant_dim.avg_full_time_days_week_out,
	EXP_claimant_dim.avg_full_time_hours_day_out,
	EXP_claimant_dim.avg_full_time_hours_week_out,
	EXP_claimant_dim.avg_wkly_wage_out,
	EXP_claimant_dim.num_of_full_time_emplymnt_same_job_out,
	EXP_claimant_dim.num_of_part_time_emplymnt_same_job_out,
	EXP_claimant_dim.ttd_rate_out,
	EXP_claimant_dim.ppd_rate_out,
	EXP_claimant_dim.ptd_rate_out,
	EXP_claimant_dim.dtd_rate_out,
	EXP_claimant_dim.wkly_attorney_fee_out,
	EXP_claimant_dim.first_rpt_inj_date_out,
	EXP_claimant_dim.supplementary_rpt_inj_date_out,
	EXP_claimant_dim.fringe_bnft_discontinued_amt_out,
	EXP_claimant_dim.emp_start_time_out,
	EXP_claimant_dim.emp_hour_day_out,
	EXP_claimant_dim.emp_hour_week_out,
	EXP_claimant_dim.emp_day_week_out,
	EXP_claimant_dim.inj_work_day_begin_time_out,
	EXP_claimant_dim.disability_date_out,
	EXP_claimant_dim.phys_restriction_ind_out,
	EXP_claimant_dim.pre_exst_disability_ind_out,
	EXP_claimant_dim.premises_code_out,
	EXP_claimant_dim.premises_descript_OUT,
	EXP_claimant_dim.work_process_descript_out,
	EXP_claimant_dim.task_descript_out,
	EXP_claimant_dim.body_part_code_out,
	EXP_claimant_dim.body_part_descript_OUT,
	EXP_claimant_dim.nature_inj_code_out,
	EXP_claimant_dim.nature_of_inj_descript_OUT,
	EXP_claimant_dim.cause_inj_code_out,
	EXP_claimant_dim.cause_of_inj_descript_OUT,
	EXP_claimant_dim.safeguard_not_used_ind_out,
	EXP_claimant_dim.inj_substance_abuse_ind_out,
	EXP_claimant_dim.sfty_device_not_used_ind_out,
	EXP_claimant_dim.inj_rules_not_obeyed_ind_out,
	EXP_claimant_dim.inj_result_occuptnal_inj_ind_out,
	EXP_claimant_dim.inj_result_occuptnal_disease_ndicator_out,
	EXP_claimant_dim.inj_result_death_ind_out,
	EXP_claimant_dim.unsafe_act_descript_out,
	EXP_claimant_dim.responsible_for_inj_descript_out,
	EXP_claimant_dim.hazard_condition_descript_out,
	EXP_claimant_dim.emp_last_day_worked_out,
	EXP_claimant_dim.death_date_out,
	EXP_claimant_dim.return_to_work_date_out,
	EXP_claimant_dim.return_to_work_type_out,
	EXP_claimant_dim.return_to_work_descript_OUT,
	EXP_claimant_dim.return_to_work_with_same_emplyr_ind_out,
	EXP_claimant_dim.emplyr_nature_bus_descript_out,
	EXP_claimant_dim.emplyr_type_code_out,
	EXP_claimant_dim.emplyr_type_descript_OUT,
	EXP_claimant_dim.insd_type_code_out AS nsd_type_code_out,
	EXP_claimant_dim.insd_type_descript_OUT,
	EXP_claimant_dim.subrogation_statute_exp_date_out,
	EXP_claimant_dim.managed_care_org_type_out,
	EXP_claimant_dim.managed_care_org_type_descript_OUT,
	EXP_claimant_dim.subrogation_code_out,
	EXP_claimant_dim.loss_condition_out,
	EXP_claimant_dim.loss_condition_descript_OUT,
	EXP_claimant_dim.attorney_or_au_rep_ind_out,
	EXP_claimant_dim.hospital_cost_out,
	EXP_claimant_dim.doctor_cost_out,
	EXP_claimant_dim.other_med_cost_out,
	EXP_claimant_dim.controverted_case_code_out,
	EXP_claimant_dim.surgery_ind_out,
	EXP_claimant_dim.emplyr_loc_descript_out,
	EXP_claimant_dim.inj_loc_comment_out,
	EXP_claimant_dim.claim_ctgry_code_out,
	EXP_claimant_dim.claim_ctgry_code_descript,
	EXP_claimant_dim.act_status_code_out,
	EXP_claimant_dim.act_status_code_descript,
	EXP_claimant_dim.investigate_ind_out,
	EXP_claimant_dim.sic_code_out,
	EXP_claimant_dim.sic_code_descript,
	EXP_claimant_dim.hospitalized_ind_out,
	EXP_claimant_dim.wage_method_code_out,
	EXP_claimant_dim.wage_method_code_descript,
	EXP_claimant_dim.pms_occuptn_descript_out,
	EXP_claimant_dim.pms_type_disability_out,
	EXP_claimant_dim.ncci_type_cov_out,
	EXP_claimant_dim.crrnt_snpsht_flag,
	EXP_claimant_dim.audit_id,
	EXP_claimant_dim.eff_from_date,
	EXP_claimant_dim.eff_to_date,
	EXP_claimant_dim.created_date,
	EXP_claimant_dim.modified_date,
	EXP_CHECK_VALIDITY.ROW_VALID,
	EXP_claimant_dim.Default,
	EXP_claimant_dim.type_of_loss_code,
	EXP_claimant_dim.type_of_loss_code_descript,
	EXP_claimant_dim.pre_injury_avg_wkly_wage_code,
	EXP_claimant_dim.pre_injury_avg_wkly_wage_code_descript,
	EXP_claimant_dim.post_inj_wkly_wage_amt,
	EXP_claimant_dim.impairment_disability_percentage,
	EXP_claimant_dim.impairment_disability_percentage_basis_code,
	EXP_claimant_dim.impairment_disability_percentage_basis_code_descript,
	EXP_claimant_dim.med_extinguishment_ind,
	EXP_claimant_dim.current_work_status,
	EXP_claimant_dim.o_pos_accom_period AS positive_accommodation_period,
	EXP_claimant_dim.o_neg_accom_period AS negative_accommodation_period,
	EXP_claimant_dim.o_pos_impact AS positive_rtw_impact,
	EXP_claimant_dim.o_neg_impact AS negative_rtw_impact,
	EXP_claimant_dim.claim_med_id_out,
	EXP_claimant_dim.medicare_eligibility_out,
	EXP_claimant_dim.InjuredWorkerLostTime,
	EXP_claimant_dim.AutomaticAdjudicationClaimIndicator,
	EXP_claimant_dim.CompensableClaimDescription
	FROM EXP_CHECK_VALIDITY
	 -- Manually join with EXP_claimant_dim
	LEFT JOIN LKP_claimant_dim
	ON LKP_claimant_dim.edw_claim_party_occurrence_pk_id = EXP_claimant_dim.edw_claim_party_occurrence_pk_id_out AND LKP_claimant_dim.edw_claim_party_pk_id = EXP_claimant_dim.edw_claim_party_pk_id_out AND LKP_claimant_dim.edw_claimant_calculation_pk_id = EXP_claimant_dim.edw_claimant_calculation_pk_id_out AND LKP_claimant_dim.edw_claimant_reserve_calculation_direct_loss_pk_id = EXP_claimant_dim.edw_claimant_reserve_calculation_direct_loss_pk_id_out AND LKP_claimant_dim.edw_claimant_reserve_calculation_exp_pk_id = EXP_claimant_dim.edw_claimant_reserve_calculation_exp_pk_id_out AND LKP_claimant_dim.edw_claimant_reserve_calculation_subrogation_pk_id = EXP_claimant_dim.edw_claimant_reserve_calculation_subrogation_pk_id_out AND LKP_claimant_dim.edw_claimant_reserve_calculation_salvage_pk_id = EXP_claimant_dim.edw_claimant_reserve_calculation_salvage_pk_id_out AND LKP_claimant_dim.edw_claimant_reserve_calculation_other_recovery_pk_id = EXP_claimant_dim.edw_claimant_reserve_calculation_other_recovery_pk_id_out AND LKP_claimant_dim.edw_wc_claimant_det_pk_id = EXP_claimant_dim.wc_claimant_det_pk_id_out AND LKP_claimant_dim.edw_wc_claimant_work_hist_pk_id = EXP_claimant_dim.wc_claimant_work_hist_pk_id_out
),
RTR_claimant_dim_INSERT AS (SELECT * FROM RTR_claimant_dim WHERE isnull(claimant_dim_id) AND RTRIM(ROW_VALID) = 'VALID'),
RTR_claimant_dim_DEFAULT1 AS (SELECT * FROM RTR_claimant_dim WHERE NOT ( (isnull(claimant_dim_id) AND RTRIM(ROW_VALID) = 'VALID') )),
UPD_claimant_dim_Update AS (
	SELECT
	claimant_dim_id AS claimant_dim_id2, 
	claim_party_id AS claim_party_id2, 
	claim_party_occurrence_id AS claim_party_occurrence_id2, 
	claimant_calculation_id AS claimant_calculation_id2, 
	claimant_reserve_calculation_id_D AS claimant_reserve_calculation_id_D2, 
	claimant_reserve_calculation_id_E AS claimant_reserve_calculation_id_E2, 
	claimant_reserve_calculation_id_B AS claimant_reserve_calculation_id_B2, 
	claimant_reserve_calculation_id_S AS claimant_reserve_calculation_id_S2, 
	claimant_reserve_calculation_id_R AS claimant_reserve_calculation_id_R2, 
	wc_claimant_det_id AS wc_claimant_det_id2, 
	claim_party_occurrence_ak_id AS claim_party_occurrence_ak_id2, 
	edw_claim_case_pk_id_out AS edw_claim_case_pk_id_out2, 
	in_edw_claim_case_ak_id AS in_edw_claim_case_ak_id2, 
	wc_claimant_work_hist_pk_id AS wc_work_hist_pk_id, 
	wc_claimant_num AS wc_claimant_num2, 
	max_med_improvement_date AS max_med_improvement_date2, 
	claimant_status_code AS claimant_status_code2, 
	claimant_direct_loss_status_code AS claimant_direct_loss_status_code2, 
	claimant_exp_status_code_out AS claimant_exp_status_code_out2, 
	claimant_subrogation_status_code AS claimant_subrogation_status_code2, 
	claimant_salvage_status_code AS claimant_salvage_status_code2, 
	claimant_other_recovery_status_code AS claimant_other_recovery_status_code2, 
	claimant_reopen_ind AS claimant_reopen_ind2, 
	claimant_supplemental_ind AS claimant_supplemental_ind2, 
	claimant_financial_ind AS claimant_financial_ind2, 
	claimant_recovery_ind AS claimant_recovery_ind2, 
	claimant_notice_only_ind AS claimant_notice_only_ind2, 
	claimant_open_date AS claimant_open_date2, 
	claimant_close_date AS claimant_close_date2, 
	claimant_reopen_date AS claimant_reopen_date2, 
	claimant_closed_after_reopen_date AS claimant_closed_after_reopen_date2, 
	claimant_noticeonly_date, 
	addr_type AS addr_type2, 
	claim_party_zip AS claim_party_zip2, 
	claim_party_state AS claim_party_state2, 
	claim_party_county AS claim_party_county2, 
	claim_party_city AS claim_party_city2, 
	claim_party_addr AS claim_party_addr2, 
	claim_party_full_name AS claim_party_full_name2, 
	claim_party_first_name AS claim_party_first_name2, 
	claim_party_last_name AS claim_party_last_name2, 
	claim_party_mid_name AS claim_party_mid_name2, 
	tax_ssn_id AS tax_ssn_id2, 
	tax_fed_id AS tax_fed_id2, 
	claim_party_birthdate AS claim_party_birthdate2, 
	claim_party_gndr AS claim_party_gndr2, 
	claim_party_role_code_out AS claim_party_role_code_out2, 
	out_claim_party_role_code_descript AS out_claim_party_role_code_descript2, 
	claimant_num AS claimant_num_out2, 
	denial_date_out, 
	jurisdiction_state_code_out AS jurisdiction_state_code_out2, 
	jurisdiction_state_descript_OUT AS jurisdiction_state_descript_OUT2, 
	emplyr_notified_date_out AS emplyr_notified_date_out2, 
	rpted_to_carrier_date_out AS rpted_to_carrier_date_out2, 
	jurisdiction_claim_num_out AS jurisdiction_claim_num_out2, 
	care_directed_ind_out AS care_directed_ind_out2, 
	care_directed_by_out AS care_directed_by_out2, 
	wc_care_directed_by_descript_OUT AS wc_care_directed_by_descript_OUT2, 
	hired_state_code_out AS hired_state_code_out2, 
	hired_state_descript_OUT AS hired_state_descript_OUT2, 
	hired_date_out AS hired_date_out2, 
	tax_filing_status_out AS tax_filing_status_out2, 
	tax_filing_status_descript_OUT AS tax_filing_status_descript_OUT2, 
	occuptn_code_out AS occuptn_code_out2, 
	occuptn_descript_OUT AS occuptn_descript_OUT2, 
	employement_status_code_out AS employement_status_code_out2, 
	wc_emplymnt_descript_OUT AS wc_emplymnt_descript_OUT2, 
	len_of_time_in_crrnt_job_out, 
	emp_dept_name_out AS emp_dept_name_out2, 
	emp_shift_num_out AS emp_shift_num_out2, 
	marital_status_out AS marital_status_out2, 
	marital_status_descript_OUT AS marital_status_descript_OUT2, 
	num_of_dependents_out AS num_of_dependents_out2, 
	num_of_dependent_children_out AS num_of_dependent_children_out2, 
	num_of_other_dependents_out AS num_of_other_dependents_out2, 
	num_of_exemptions_out AS num_of_exemptions_out2, 
	exemption_type_out AS exemption_type_out2, 
	wc_exemption_type_descript_OUT AS wc_exemption_type_descript_OUT2, 
	emp_blind_ind_out AS emp_blind_ind_out2, 
	emp_over_65_ind_out AS emp_over_65_ind_out2, 
	spouse_blind_ind_out AS spouse_blind_ind_out2, 
	spouse_over_65_ind_out AS spouse_over_65_ind_out2, 
	education_lvl_out AS education_lvl_out2, 
	med_auth_ind_out AS med_auth_ind_out2, 
	auth_to_release_ssn_ind_out AS auth_to_release_ssn_ind_out2, 
	emp_id_num_out AS emp_id_num_out2, 
	emp_id_type_out, 
	emp_id_type_descript_OUT AS emp_id_type_descript_OUT2, 
	emp_part_time_hour_week_out AS emp_part_time_hour_week_out2, 
	emp_dept_num_out AS emp_dept_num_out2, 
	emp_part_time_hourly_week_rate_amt_out AS emp_part_time_hourly_week_rate_amt_out2, 
	wage_rate_amt_out AS wage_rate_amt_out2, 
	wage_period_code_out AS wage_period_code_out2, 
	wage_period_descript_OUT AS wage_period_descript_OUT2, 
	wage_eff_date_out AS wage_eff_date_out2, 
	weeks_worked_out AS weeks_worked_out2, 
	gross_amt_type_out AS gross_amt_type_out2, 
	wage_gross_amt_type_descript_OUT AS wage_gross_amt_type_descript_OUT2, 
	gross_wage_amt_excluding_tips_out AS gross_wage_amt_excluding_tips_out2, 
	piece_work_num_of_weeks_excluding_overtime_out AS piece_work_num_of_weeks_excluding_overtime_out2, 
	emp_rec_meals_out AS emp_rec_meals_out2, 
	emp_rec_room_out AS emp_rec_room_out2, 
	emp_rec_tips_out AS emp_rec_tips_out2, 
	overtime_amt_out AS overtime_amt_out2, 
	overtime_after_hour_in_a_week_out AS overtime_after_hour_in_a_week_out2, 
	overtime_after_hour_in_a_day_out AS overtime_after_hour_in_a_day_out2, 
	full_pay_inj_day_ind_out AS full_pay_inj_day_ind_out2, 
	salary_paid_ind_out AS salary_paid_ind_out2, 
	avg_full_time_days_week_out AS avg_full_time_days_week_out2, 
	avg_full_time_hours_day_out AS avg_full_time_hours_day_out2, 
	avg_full_time_hours_week_out AS avg_full_time_hours_week_out2, 
	avg_wkly_wage_out AS avg_wkly_wage_out2, 
	num_of_full_time_emplymnt_same_job_out AS num_of_full_time_emplymnt_same_job_out2, 
	num_of_part_time_emplymnt_same_job_out AS num_of_part_time_emplymnt_same_job_out2, 
	ttd_rate_out AS ttd_rate_out2, 
	ppd_rate_out AS ppd_rate_out2, 
	ptd_rate_out AS ptd_rate_out2, 
	dtd_rate_out AS dtd_rate_out2, 
	wkly_attorney_fee_out AS wkly_attorney_fee_out2, 
	first_rpt_inj_date_out AS first_rpt_inj_date_out2, 
	supplementary_rpt_inj_date_out AS supplementary_rpt_inj_date_out2, 
	fringe_bnft_discontinued_amt_out AS fringe_bnft_discontinued_amt_out2, 
	emp_start_time_out AS emp_start_time_out2, 
	emp_hour_day_out AS emp_hour_day_out2, 
	emp_hour_week_out AS emp_hour_week_out2, 
	emp_day_week_out AS emp_day_week_out2, 
	inj_work_day_begin_time_out AS inj_work_day_begin_time_out2, 
	disability_date_out AS disability_date_out2, 
	phys_restriction_ind_out AS phys_restriction_ind_out2, 
	pre_exst_disability_ind_out AS pre_exst_disability_ind_out2, 
	premises_code_out AS premises_code_out2, 
	premises_descript_OUT AS premises_descript_OUT2, 
	work_process_descript_out AS work_process_descript_out2, 
	task_descript_out AS task_descript_out2, 
	body_part_code_out AS body_part_code_out2, 
	body_part_descript_OUT, 
	nature_inj_code_out AS nature_inj_code_out2, 
	nature_of_inj_descript_OUT AS nature_of_inj_descript_OUT2, 
	cause_inj_code_out AS cause_inj_code_out2, 
	cause_of_inj_descript_OUT AS cause_of_inj_descript_OUT2, 
	safeguard_not_used_ind_out AS safeguard_not_used_ind_out2, 
	inj_substance_abuse_ind_out AS inj_substance_abuse_ind_out2, 
	sfty_device_not_used_ind_out AS sfty_device_not_used_ind_out2, 
	inj_rules_not_obeyed_ind_out AS inj_rules_not_obeyed_ind_out2, 
	inj_result_occuptnal_inj_ind_out AS inj_result_occuptnal_inj_ind_out2, 
	inj_result_occuptnal_disease_ndicator_out AS inj_result_occuptnal_disease_ndicator_out2, 
	inj_result_death_ind_out AS inj_result_death_ind_out2, 
	unsafe_act_descript_out AS unsafe_act_descript_out2, 
	responsible_for_inj_descript_out AS responsible_for_inj_descript_out2, 
	hazard_condition_descript_out AS hazard_condition_descript_out2, 
	emp_last_day_worked_out AS emp_last_day_worked_out2, 
	death_date_out AS death_date_out2, 
	return_to_work_date_out AS return_to_work_date_out2, 
	return_to_work_type_out AS return_to_work_type_out2, 
	return_to_work_descript_OUT AS return_to_work_descript_OUT2, 
	return_to_work_with_same_emplyr_ind_out AS return_to_work_with_same_emplyr_ind_out2, 
	emplyr_nature_bus_descript_out AS emplyr_nature_bus_descript_out2, 
	emplyr_type_code_out AS emplyr_type_code_out2, 
	nsd_type_code_out AS nsd_type_code_out2, 
	insd_type_descript_OUT AS insd_type_descript_OUT2, 
	emplyr_type_descript_OUT AS emplyr_type_descript_OUT2, 
	subrogation_statute_exp_date_out AS subrogation_statute_exp_date_out2, 
	managed_care_org_type_out AS managed_care_org_type_out2, 
	managed_care_org_type_descript_OUT AS managed_care_org_type_descript_OUT2, 
	subrogation_code_out AS subrogation_code_out2, 
	loss_condition_out AS loss_condition_out2, 
	loss_condition_descript_OUT AS loss_condition_descript_OUT2, 
	attorney_or_au_rep_ind_out AS attorney_or_au_rep_ind_out2, 
	hospital_cost_out AS hospital_cost_out2, 
	doctor_cost_out AS doctor_cost_out2, 
	other_med_cost_out AS other_med_cost_out2, 
	controverted_case_code_out AS controverted_case_code_out2, 
	surgery_ind_out AS surgery_ind_out2, 
	emplyr_loc_descript_out AS emplyr_loc_descript_out2, 
	inj_loc_comment_out AS inj_loc_comment_out2, 
	claim_ctgry_code_out AS claim_ctgry_code_out2, 
	claim_ctgry_code_descript AS claim_ctgry_code_descript2, 
	act_status_code_out AS act_status_code_out2, 
	act_status_code_descript, 
	investigate_ind_out AS investigate_ind_out2, 
	sic_code_out, 
	sic_code_descript, 
	hospitalized_ind_out AS hospitalized_ind_out2, 
	wage_method_code_out AS wage_method_code_out2, 
	wage_method_code_descript AS wage_method_code_descript2, 
	pms_occuptn_descript_out AS pms_occuptn_descript_out2, 
	pms_type_disability_out AS pms_type_disability_out2, 
	ncci_type_cov_out AS ncci_type_cov_out2, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag2, 
	audit_id AS audit_id2, 
	eff_from_date AS eff_from_date2, 
	eff_to_date AS eff_to_date2, 
	created_date AS created_date2, 
	modified_date AS modified_date2, 
	Default AS Default2, 
	type_of_loss_code AS type_of_loss_code2, 
	type_of_loss_code_descript AS type_of_loss_code_descript2, 
	pre_injury_avg_wkly_wage_code AS pre_injury_avg_wkly_wage_code2, 
	pre_injury_avg_wkly_wage_code_descript AS pre_injury_avg_wkly_wage_code_descript2, 
	post_inj_wkly_wage_amt AS post_inj_wkly_wage_amt2, 
	impairment_disability_percentage AS impairment_disability_percentage2, 
	impairment_disability_percentage_basis_code AS impairment_disability_percentage_basis_code2, 
	impairment_disability_percentage_basis_code_descript AS impairment_disability_percentage_basis_code_descript2, 
	med_extinguishment_ind AS med_extinguishment_ind2, 
	current_work_status AS current_work_status2, 
	positive_accommodation_period, 
	negative_accommodation_period, 
	positive_rtw_impact, 
	negative_rtw_impact, 
	claim_med_id_out AS claim_med_id_out2, 
	medicare_eligibility_out AS medicare_eligibility_out2, 
	InjuredWorkerLostTime AS InjuredWorkerLostTime2, 
	AutomaticAdjudicationClaimIndicator AS AutomaticAdjudicationClaimIndicator2, 
	CompensableClaimDescription AS CompensableClaimDescription2
	FROM RTR_claimant_dim_DEFAULT1
),
claimant_dim_Update AS (
	MERGE INTO claimant_dim AS T
	USING UPD_claimant_dim_Update AS S
	ON T.claimant_dim_id = S.claimant_dim_id2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.edw_claim_party_occurrence_pk_id = S.claim_party_occurrence_id2, T.edw_claim_party_pk_id = S.claim_party_id2, T.edw_claimant_calculation_pk_id = S.claimant_calculation_id2, T.edw_claimant_reserve_calculation_direct_loss_pk_id = S.claimant_reserve_calculation_id_D2, T.edw_claimant_reserve_calculation_exp_pk_id = S.claimant_reserve_calculation_id_E2, T.edw_claimant_reserve_calculation_subrogation_pk_id = S.claimant_reserve_calculation_id_B2, T.edw_claimant_reserve_calculation_salvage_pk_id = S.claimant_reserve_calculation_id_S2, T.edw_claimant_reserve_calculation_other_recovery_pk_id = S.claimant_reserve_calculation_id_R2, T.edw_wc_claimant_det_pk_id = S.wc_claimant_det_id2, T.edw_claim_party_occurrence_ak_id = S.claim_party_occurrence_ak_id2, T.claimant_status_type = S.claimant_status_code2, T.claimant_direct_loss_status_type = S.claimant_direct_loss_status_code2, T.claimant_exp_status_type = S.claimant_exp_status_code_out2, T.claimant_salvage_status_type = S.claimant_salvage_status_code2, T.claimant_subrogation_status_type = S.claimant_subrogation_status_code2, T.claimant_other_recovery_status_type = S.claimant_other_recovery_status_code2, T.claimant_financial_ind = S.claimant_financial_ind2, T.claimant_supplemental_ind = S.claimant_supplemental_ind2, T.claimant_recovery_ind = S.claimant_recovery_ind2, T.claimant_notice_only_claim_ind = S.claimant_notice_only_ind2, T.claimant_open_date = S.claimant_open_date2, T.claimant_close_date = S.claimant_close_date2, T.claimant_reopen_date = S.claimant_reopen_date2, T.claimant_closed_after_reopen_date = S.claimant_closed_after_reopen_date2, T.claimant_notice_only_date = S.claimant_noticeonly_date, T.claimant_addr_type = S.addr_type2, T.claimant_zip = S.claim_party_zip2, T.claimant_state = S.claim_party_state2, T.claimant_county = S.claim_party_county2, T.claimant_city = S.claim_party_city2, T.claimant_addr = S.claim_party_addr2, T.claimant_full_name = S.claim_party_full_name2, T.claimant_first_name = S.claim_party_first_name2, T.claimant_last_name = S.claim_party_last_name2, T.claimant_mid_name = S.claim_party_mid_name2, T.claimant_tax_ssn_id = S.tax_ssn_id2, T.claimant_tax_fed_id = S.tax_fed_id2, T.claimant_birthdate = S.claim_party_birthdate2, T.claimant_gndr = S.claim_party_gndr2, T.claimant_num = S.claimant_num_out2, T.denial_date = S.denial_date_out, T.jurisdiction_state_code = S.jurisdiction_state_code_out2, T.jurisdiction_state_descript = S.jurisdiction_state_descript_OUT2, T.emplyr_notified_date = S.emplyr_notified_date_out2, T.rpted_to_carrier_date = S.rpted_to_carrier_date_out2, T.jurisdiction_claim_num = S.jurisdiction_claim_num_out2, T.care_directed_ind = S.care_directed_ind_out2, T.care_directed_by = S.care_directed_by_out2, T.care_directed_by_descript = S.wc_care_directed_by_descript_OUT2, T.hired_state_code = S.hired_state_code_out2, T.hired_state_descript = S.hired_state_descript_OUT2, T.hired_date = S.hired_date_out2, T.tax_filing_status = S.tax_filing_status_out2, T.tax_filing_status_descript = S.tax_filing_status_descript_OUT2, T.occuptn_code = S.occuptn_code_out2, T.occuptn_code_descript = S.occuptn_descript_OUT2, T.emplymnt_status_code = S.employement_status_code_out2, T.emplymnt_status_code_descript = S.wc_emplymnt_descript_OUT2, T.len_of_time_in_crrnt_job = S.len_of_time_in_crrnt_job_out, T.emp_dept_name = S.emp_dept_name_out2, T.emp_shift_num = S.emp_shift_num_out2, T.marital_status = S.marital_status_out2, T.marital_status_descript = S.marital_status_descript_OUT2, T.num_of_dependents = S.num_of_dependents_out2, T.num_of_dependent_children = S.num_of_dependent_children_out2, T.num_of_other_dependents = S.num_of_other_dependents_out2, T.num_of_exemptions = S.num_of_exemptions_out2, T.exemption_type = S.exemption_type_out2, T.exemption_type_descript = S.wc_exemption_type_descript_OUT2, T.emp_blind_ind = S.emp_blind_ind_out2, T.emp_over_65_ind = S.emp_over_65_ind_out2, T.spouse_blind_ind = S.spouse_blind_ind_out2, T.spouse_over_65_ind = S.spouse_over_65_ind_out2, T.education_lvl = S.education_lvl_out2, T.med_auth_ind = S.med_auth_ind_out2, T.auth_to_release_ssn_ind = S.auth_to_release_ssn_ind_out2, T.emp_id_num = S.emp_id_num_out2, T.emp_id_type = S.emp_id_type_out, T.emp_id_type_descript = S.emp_id_type_descript_OUT2, T.emp_part_time_hour_week = S.emp_part_time_hour_week_out2, T.emp_dept_num = S.emp_dept_num_out2, T.emp_part_time_hourly_week_rate_amt = S.emp_part_time_hourly_week_rate_amt_out2, T.wage_rate_amt = S.wage_rate_amt_out2, T.wage_period_code = S.wage_period_code_out2, T.wage_period_code_descript = S.wage_period_descript_OUT2, T.wage_eff_date = S.wage_eff_date_out2, T.weeks_worked = S.weeks_worked_out2, T.gross_amt_type = S.gross_amt_type_out2, T.gross_amt_type_descript = S.wage_gross_amt_type_descript_OUT2, T.gross_wage_amt_excluding_tips = S.gross_wage_amt_excluding_tips_out2, T.piece_work_num_of_weeks_excluding_overtime = S.piece_work_num_of_weeks_excluding_overtime_out2, T.emp_rec_meals = S.emp_rec_meals_out2, T.emp_rec_room = S.emp_rec_room_out2, T.emp_rec_tips = S.emp_rec_tips_out2, T.overtime_amt = S.overtime_amt_out2, T.overtime_after_hour_in_a_week = S.overtime_after_hour_in_a_week_out2, T.overtime_after_hour_in_a_day = S.overtime_after_hour_in_a_day_out2, T.full_pay_inj_day_ind = S.full_pay_inj_day_ind_out2, T.salary_paid_ind = S.salary_paid_ind_out2, T.avg_full_time_days_week = S.avg_full_time_days_week_out2, T.avg_full_time_hours_day = S.avg_full_time_hours_day_out2, T.avg_full_time_hours_week = S.avg_full_time_hours_week_out2, T.avg_wkly_wage = S.avg_wkly_wage_out2, T.num_of_full_time_emplymnt_same_job = S.num_of_full_time_emplymnt_same_job_out2, T.num_of_part_time_emplymnt_same_job = S.num_of_part_time_emplymnt_same_job_out2, T.ttd_rate = S.ttd_rate_out2, T.ppd_rate = S.ppd_rate_out2, T.ptd_rate = S.ptd_rate_out2, T.dtd_rate = S.dtd_rate_out2, T.wkly_attorney_fee = S.wkly_attorney_fee_out2, T.first_rpt_inj_date = S.first_rpt_inj_date_out2, T.supplementary_rpt_inj_date = S.supplementary_rpt_inj_date_out2, T.fringe_bnft_discontinued_amt = S.fringe_bnft_discontinued_amt_out2, T.emp_start_time = S.emp_start_time_out2, T.emp_hour_day = S.emp_hour_day_out2, T.emp_hour_week = S.emp_hour_week_out2, T.emp_day_week = S.emp_day_week_out2, T.inj_work_day_begin_time = S.inj_work_day_begin_time_out2, T.disability_date = S.disability_date_out2, T.phys_restriction_ind = S.phys_restriction_ind_out2, T.pre_exst_disability_ind = S.pre_exst_disability_ind_out2, T.premises_code = S.premises_code_out2, T.premises_code_descript = S.premises_descript_OUT2, T.work_process_descript = S.work_process_descript_out2, T.task_descript = S.task_descript_out2, T.body_part_code = S.body_part_code_out2, T.body_part_code_descript = S.body_part_descript_OUT, T.nature_inj_code = S.nature_inj_code_out2, T.nature_inj_code_descript = S.nature_of_inj_descript_OUT2, T.cause_inj_code = S.cause_inj_code_out2, T.cause_inj_code_descript = S.cause_of_inj_descript_OUT2, T.safeguard_not_used_ind = S.safeguard_not_used_ind_out2, T.inj_substance_abuse_ind = S.inj_substance_abuse_ind_out2, T.sfty_device_not_used_ind = S.sfty_device_not_used_ind_out2, T.inj_rules_not_obeyed_ind = S.inj_rules_not_obeyed_ind_out2, T.inj_result_occuptnal_inj_ind = S.inj_result_occuptnal_inj_ind_out2, T.inj_result_occuptnal_disease_ind = S.inj_result_occuptnal_disease_ndicator_out2, T.inj_result_death_ind = S.inj_result_death_ind_out2, T.unsafe_act_descript = S.unsafe_act_descript_out2, T.responsible_for_inj_descript = S.responsible_for_inj_descript_out2, T.hazard_condition_descript = S.hazard_condition_descript_out2, T.emp_last_day_worked = S.emp_last_day_worked_out2, T.death_date = S.death_date_out2, T.return_to_work_date = S.return_to_work_date_out2, T.return_to_work_type = S.return_to_work_type_out2, T.return_to_work_type_descript = S.return_to_work_descript_OUT2, T.return_to_work_with_same_emplyr_ind = S.return_to_work_with_same_emplyr_ind_out2, T.emplyr_nature_bus_descript = S.emplyr_nature_bus_descript_out2, T.emplyr_type_code = S.emplyr_type_code_out2, T.emplyr_type_code_descript = S.emplyr_type_descript_OUT2, T.insd_type_code = S.nsd_type_code_out2, T.insd_type_code_descript = S.insd_type_descript_OUT2, T.subrogation_statute_exp_date = S.subrogation_statute_exp_date_out2, T.managed_care_org_type = S.managed_care_org_type_out2, T.managed_care_org_type_descript = S.managed_care_org_type_descript_OUT2, T.subrogation_code = S.subrogation_code_out2, T.loss_condition = S.loss_condition_out2, T.attorney_or_au_rep_ind = S.attorney_or_au_rep_ind_out2, T.hospital_cost = S.hospital_cost_out2, T.doctor_cost = S.doctor_cost_out2, T.other_med_cost = S.other_med_cost_out2, T.controverted_case_code = S.controverted_case_code_out2, T.surgery_ind = S.surgery_ind_out2, T.emplyr_loc_descript = S.emplyr_loc_descript_out2, T.inj_loc_comment = S.inj_loc_comment_out2, T.claim_ctgry_code = S.claim_ctgry_code_out2, T.claim_ctgry_code_descript = S.claim_ctgry_code_descript2, T.act_status_code = S.act_status_code_out2, T.act_status_code_descript = S.act_status_code_descript, T.investigate_ind = S.investigate_ind_out2, T.sic_code = S.sic_code_out, T.sic_code_descript = S.sic_code_descript, T.hospitalized_ind = S.hospitalized_ind_out2, T.wage_method_code = S.wage_method_code_out2, T.wage_method_code_descript = S.wage_method_code_descript2, T.pms_occuptn_descript = S.pms_occuptn_descript_out2, T.pms_type_disability = S.pms_type_disability_out2, T.ncci_type_cov = S.ncci_type_cov_out2, T.crrnt_snpsht_flag = S.crrnt_snpsht_flag2, T.audit_id = S.audit_id2, T.eff_from_date = S.eff_from_date2, T.eff_to_date = S.eff_to_date2, T.created_date = S.created_date2, T.modified_date = S.modified_date2, T.edw_wc_claimant_work_hist_pk_id = S.wc_work_hist_pk_id, T.wc_claimant_num = S.wc_claimant_num2, T.type_of_loss_code = S.type_of_loss_code2, T.type_of_loss_descript = S.type_of_loss_code_descript2, T.pre_inj_avg_wkly_wage_code = S.pre_injury_avg_wkly_wage_code2, T.pre_inj_avg_wkly_wage_descript = S.pre_injury_avg_wkly_wage_code_descript2, T.post_inj_wkly_wage_amt = S.post_inj_wkly_wage_amt2, T.impairment_disability_percentage = S.impairment_disability_percentage2, T.impairment_disability_percentage_basis_code = S.impairment_disability_percentage_basis_code2, T.impairment_disability_percentage_basis_code_descript = S.impairment_disability_percentage_basis_code_descript2, T.max_med_improvement_date = S.max_med_improvement_date2, T.med_extinguishment_ind = S.med_extinguishment_ind2, T.crrnt_work_status = S.current_work_status2, T.edw_claim_med_pk_id = S.claim_med_id_out2, T.medicare_eligibility = S.medicare_eligibility_out2, T.positive_accommodation_period = S.positive_accommodation_period, T.negative_accommodation_period = S.negative_accommodation_period, T.positive_rtw_impact = S.positive_rtw_impact, T.negative_rtw_impact = S.negative_rtw_impact, T.InjuredWorkerLostTime = S.InjuredWorkerLostTime2, T.AutomaticAdjudicationClaimIndicator = S.AutomaticAdjudicationClaimIndicator2, T.CompensableClaimDescription = S.CompensableClaimDescription2
),
UPD_claimant_dim_Insert AS (
	SELECT
	claimant_dim_id AS claimant_dim_id1, 
	claim_party_id AS claim_party_id1, 
	claim_party_occurrence_id AS claim_party_occurrence_id1, 
	claimant_calculation_id AS claimant_calculation_id1, 
	claimant_reserve_calculation_id_D AS claimant_reserve_calculation_id_D1, 
	claimant_reserve_calculation_id_E AS claimant_reserve_calculation_id_E1, 
	claimant_reserve_calculation_id_B AS claimant_reserve_calculation_id_B1, 
	claimant_reserve_calculation_id_S AS claimant_reserve_calculation_id_S1, 
	claimant_reserve_calculation_id_R AS claimant_reserve_calculation_id_R1, 
	wc_claimant_det_id AS wc_claimant_det_id1, 
	wc_claimant_work_hist_pk_id AS wc_work_hist_pk_id1, 
	wc_claimant_num AS wc_claimant_num1, 
	max_med_improvement_date AS max_med_improvement_date1, 
	claim_party_occurrence_ak_id AS claim_party_occurrence_ak_id1, 
	edw_claim_case_pk_id_out AS edw_claim_case_pk_id_out1, 
	in_edw_claim_case_ak_id AS in_edw_claim_case_ak_id1, 
	claimant_status_code AS claimant_status_code1, 
	claimant_direct_loss_status_code AS claimant_direct_loss_status_code1, 
	claimant_exp_status_code_out AS claimant_exp_status_code_out1, 
	claimant_subrogation_status_code AS claimant_subrogation_status_code1, 
	claimant_salvage_status_code AS claimant_salvage_status_code1, 
	claimant_other_recovery_status_code AS claimant_other_recovery_status_code1, 
	claimant_reopen_ind AS claimant_reopen_ind1, 
	claimant_supplemental_ind AS claimant_supplemental_ind1, 
	claimant_financial_ind AS claimant_financial_ind1, 
	claimant_recovery_ind AS claimant_recovery_ind1, 
	claimant_notice_only_ind AS claimant_notice_only_ind1, 
	claimant_open_date AS claimant_open_date1, 
	claimant_close_date AS claimant_close_date1, 
	claimant_reopen_date AS claimant_reopen_date1, 
	claimant_closed_after_reopen_date AS claimant_closed_after_reopen_date1, 
	claimant_noticeonly_date, 
	addr_type AS addr_type1, 
	claim_party_zip AS claim_party_zip1, 
	claim_party_state AS claim_party_state1, 
	claim_party_county AS claim_party_county1, 
	claim_party_city AS claim_party_city1, 
	claim_party_addr AS claim_party_addr1, 
	claim_party_full_name AS claim_party_full_name1, 
	claim_party_first_name AS claim_party_first_name1, 
	claim_party_last_name AS claim_party_last_name1, 
	claim_party_mid_name AS claim_party_mid_name1, 
	tax_ssn_id AS tax_ssn_id1, 
	tax_fed_id AS tax_fed_id1, 
	claim_party_birthdate AS claim_party_birthdate1, 
	claim_party_gndr AS claim_party_gndr1, 
	claim_party_role_code_out AS claim_party_role_code_out1, 
	out_claim_party_role_code_descript AS out_claim_party_role_code_descript1, 
	claimant_num AS claimant_num_out1, 
	denial_date_out, 
	jurisdiction_state_code_out AS jurisdiction_state_code_out1, 
	jurisdiction_state_descript_OUT AS jurisdiction_state_descript_OUT1, 
	emplyr_notified_date_out AS emplyr_notified_date_out1, 
	rpted_to_carrier_date_out AS rpted_to_carrier_date_out1, 
	jurisdiction_claim_num_out AS jurisdiction_claim_num_out1, 
	care_directed_ind_out AS care_directed_ind_out1, 
	care_directed_by_out AS care_directed_by_out1, 
	wc_care_directed_by_descript_OUT AS wc_care_directed_by_descript_OUT1, 
	hired_state_code_out AS hired_state_code_out1, 
	hired_state_descript_OUT AS hired_state_descript_OUT1, 
	hired_date_out AS hired_date_out1, 
	tax_filing_status_out AS tax_filing_status_out1, 
	tax_filing_status_descript_OUT AS tax_filing_status_descript_OUT1, 
	occuptn_code_out AS occuptn_code_out1, 
	occuptn_descript_OUT AS occuptn_descript_OUT1, 
	employement_status_code_out AS employement_status_code_out1, 
	wc_emplymnt_descript_OUT AS wc_emplymnt_descript_OUT1, 
	len_of_time_in_crrnt_job_out, 
	emp_dept_name_out AS emp_dept_name_out1, 
	emp_shift_num_out AS emp_shift_num_out1, 
	marital_status_out AS marital_status_out1, 
	marital_status_descript_OUT AS marital_status_descript_OUT1, 
	num_of_dependents_out AS num_of_dependents_out1, 
	num_of_dependent_children_out AS num_of_dependent_children_out1, 
	num_of_other_dependents_out AS num_of_other_dependents_out1, 
	num_of_exemptions_out AS num_of_exemptions_out1, 
	exemption_type_out AS exemption_type_out1, 
	wc_exemption_type_descript_OUT AS wc_exemption_type_descript_OUT1, 
	emp_blind_ind_out AS emp_blind_ind_out1, 
	emp_over_65_ind_out AS emp_over_65_ind_out1, 
	spouse_blind_ind_out AS spouse_blind_ind_out1, 
	spouse_over_65_ind_out AS spouse_over_65_ind_out1, 
	education_lvl_out AS education_lvl_out1, 
	med_auth_ind_out AS med_auth_ind_out1, 
	auth_to_release_ssn_ind_out AS auth_to_release_ssn_ind_out1, 
	emp_id_num_out AS emp_id_num_out1, 
	emp_id_type_out, 
	emp_id_type_descript_OUT AS emp_id_type_descript_OUT1, 
	emp_part_time_hour_week_out AS emp_part_time_hour_week_out1, 
	emp_dept_num_out AS emp_dept_num_out1, 
	emp_part_time_hourly_week_rate_amt_out AS emp_part_time_hourly_week_rate_amt_out1, 
	wage_rate_amt_out AS wage_rate_amt_out1, 
	wage_period_code_out AS wage_period_code_out1, 
	wage_period_descript_OUT AS wage_period_descript_OUT1, 
	wage_eff_date_out AS wage_eff_date_out1, 
	weeks_worked_out AS weeks_worked_out1, 
	gross_amt_type_out AS gross_amt_type_out1, 
	wage_gross_amt_type_descript_OUT AS wage_gross_amt_type_descript_OUT1, 
	gross_wage_amt_excluding_tips_out AS gross_wage_amt_excluding_tips_out1, 
	piece_work_num_of_weeks_excluding_overtime_out AS piece_work_num_of_weeks_excluding_overtime_out1, 
	emp_rec_meals_out AS emp_rec_meals_out1, 
	emp_rec_room_out AS emp_rec_room_out1, 
	emp_rec_tips_out AS emp_rec_tips_out1, 
	overtime_amt_out AS overtime_amt_out1, 
	overtime_after_hour_in_a_week_out AS overtime_after_hour_in_a_week_out1, 
	overtime_after_hour_in_a_day_out AS overtime_after_hour_in_a_day_out1, 
	full_pay_inj_day_ind_out AS full_pay_inj_day_ind_out1, 
	salary_paid_ind_out AS salary_paid_ind_out1, 
	avg_full_time_days_week_out AS avg_full_time_days_week_out1, 
	avg_full_time_hours_day_out AS avg_full_time_hours_day_out1, 
	avg_full_time_hours_week_out AS avg_full_time_hours_week_out1, 
	avg_wkly_wage_out AS avg_wkly_wage_out1, 
	num_of_full_time_emplymnt_same_job_out AS num_of_full_time_emplymnt_same_job_out1, 
	num_of_part_time_emplymnt_same_job_out AS num_of_part_time_emplymnt_same_job_out1, 
	ttd_rate_out AS ttd_rate_out1, 
	ppd_rate_out AS ppd_rate_out1, 
	ptd_rate_out AS ptd_rate_out1, 
	dtd_rate_out AS dtd_rate_out1, 
	wkly_attorney_fee_out AS wkly_attorney_fee_out1, 
	first_rpt_inj_date_out AS first_rpt_inj_date_out1, 
	supplementary_rpt_inj_date_out AS supplementary_rpt_inj_date_out1, 
	fringe_bnft_discontinued_amt_out AS fringe_bnft_discontinued_amt_out1, 
	emp_start_time_out AS emp_start_time_out1, 
	emp_hour_day_out AS emp_hour_day_out1, 
	emp_hour_week_out AS emp_hour_week_out1, 
	emp_day_week_out AS emp_day_week_out1, 
	inj_work_day_begin_time_out AS inj_work_day_begin_time_out1, 
	disability_date_out AS disability_date_out1, 
	phys_restriction_ind_out AS phys_restriction_ind_out1, 
	pre_exst_disability_ind_out AS pre_exst_disability_ind_out1, 
	premises_code_out AS premises_code_out1, 
	premises_descript_OUT AS premises_descript_OUT1, 
	work_process_descript_out AS work_process_descript_out1, 
	task_descript_out AS task_descript_out1, 
	body_part_code_out AS body_part_code_out1, 
	body_part_descript_OUT AS body_part_descript_OUT1, 
	nature_inj_code_out AS nature_inj_code_out1, 
	nature_of_inj_descript_OUT AS nature_of_inj_descript_OUT1, 
	cause_inj_code_out AS cause_inj_code_out1, 
	cause_of_inj_descript_OUT AS cause_of_inj_descript_OUT1, 
	safeguard_not_used_ind_out AS safeguard_not_used_ind_out1, 
	inj_substance_abuse_ind_out AS inj_substance_abuse_ind_out1, 
	sfty_device_not_used_ind_out AS sfty_device_not_used_ind_out1, 
	inj_rules_not_obeyed_ind_out AS inj_rules_not_obeyed_ind_out1, 
	inj_result_occuptnal_inj_ind_out AS inj_result_occuptnal_inj_ind_out1, 
	inj_result_occuptnal_disease_ndicator_out AS inj_result_occuptnal_disease_ndicator_out1, 
	inj_result_death_ind_out AS inj_result_death_ind_out1, 
	unsafe_act_descript_out AS unsafe_act_descript_out1, 
	responsible_for_inj_descript_out AS responsible_for_inj_descript_out1, 
	hazard_condition_descript_out AS hazard_condition_descript_out1, 
	emp_last_day_worked_out AS emp_last_day_worked_out1, 
	death_date_out AS death_date_out1, 
	return_to_work_date_out AS return_to_work_date_out1, 
	return_to_work_type_out AS return_to_work_type_out1, 
	return_to_work_descript_OUT AS return_to_work_descript_OUT1, 
	return_to_work_with_same_emplyr_ind_out AS return_to_work_with_same_emplyr_ind_out1, 
	emplyr_nature_bus_descript_out AS emplyr_nature_bus_descript_out1, 
	emplyr_type_code_out AS emplyr_type_code_out1, 
	emplyr_type_descript_OUT AS emplyr_type_descript_OUT1, 
	nsd_type_code_out AS nsd_type_code_out1, 
	insd_type_descript_OUT AS insd_type_descript_OUT1, 
	subrogation_statute_exp_date_out AS subrogation_statute_exp_date_out1, 
	managed_care_org_type_out AS managed_care_org_type_out1, 
	managed_care_org_type_descript_OUT AS managed_care_org_type_descript_OUT1, 
	subrogation_code_out AS subrogation_code_out1, 
	loss_condition_out AS loss_condition_out1, 
	loss_condition_descript_OUT AS loss_condition_descript_OUT1, 
	attorney_or_au_rep_ind_out AS attorney_or_au_rep_ind_out1, 
	hospital_cost_out AS hospital_cost_out1, 
	doctor_cost_out AS doctor_cost_out1, 
	other_med_cost_out AS other_med_cost_out1, 
	controverted_case_code_out AS controverted_case_code_out1, 
	surgery_ind_out AS surgery_ind_out1, 
	emplyr_loc_descript_out AS emplyr_loc_descript_out1, 
	inj_loc_comment_out AS inj_loc_comment_out1, 
	claim_ctgry_code_out AS claim_ctgry_code_out1, 
	claim_ctgry_code_descript AS claim_ctgry_code_descript1, 
	act_status_code_out AS act_status_code_out1, 
	act_status_code_descript, 
	investigate_ind_out AS investigate_ind_out1, 
	sic_code_out, 
	sic_code_descript, 
	hospitalized_ind_out AS hospitalized_ind_out1, 
	wage_method_code_out AS wage_method_code_out1, 
	wage_method_code_descript AS wage_method_code_descript1, 
	pms_occuptn_descript_out AS pms_occuptn_descript_out1, 
	pms_type_disability_out AS pms_type_disability_out1, 
	ncci_type_cov_out AS ncci_type_cov_out1, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag1, 
	audit_id AS audit_id1, 
	eff_from_date AS eff_from_date1, 
	eff_to_date AS eff_to_date1, 
	created_date AS created_date1, 
	modified_date AS modified_date1, 
	Default AS Default1, 
	type_of_loss_code AS type_of_loss_code1, 
	type_of_loss_code_descript AS type_of_loss_code_descript1, 
	pre_injury_avg_wkly_wage_code AS pre_injury_avg_wkly_wage_code1, 
	pre_injury_avg_wkly_wage_code_descript AS pre_injury_avg_wkly_wage_code_descript1, 
	post_inj_wkly_wage_amt AS post_inj_wkly_wage_amt1, 
	impairment_disability_percentage AS impairment_disability_percentage1, 
	impairment_disability_percentage_basis_code AS impairment_disability_percentage_basis_code1, 
	impairment_disability_percentage_basis_code_descript AS impairment_disability_percentage_basis_code_descript1, 
	med_extinguishment_ind AS med_extinguishment_ind1, 
	current_work_status AS current_work_status1, 
	positive_accommodation_period, 
	negative_accommodation_period, 
	positive_rtw_impact, 
	negative_rtw_impact, 
	claim_med_id_out AS claim_med_id_out1, 
	medicare_eligibility_out AS medicare_eligibility_out1, 
	InjuredWorkerLostTime AS InjuredWorkerLostTime1, 
	AutomaticAdjudicationClaimIndicator AS AutomaticAdjudicationClaimIndicator1, 
	CompensableClaimDescription AS CompensableClaimDescription1
	FROM RTR_claimant_dim_INSERT
),
claimant_dim_Insert AS (
	INSERT INTO claimant_dim
	(edw_claim_party_occurrence_pk_id, edw_claim_party_pk_id, edw_claimant_calculation_pk_id, edw_claimant_reserve_calculation_direct_loss_pk_id, edw_claimant_reserve_calculation_exp_pk_id, edw_claimant_reserve_calculation_subrogation_pk_id, edw_claimant_reserve_calculation_salvage_pk_id, edw_claimant_reserve_calculation_other_recovery_pk_id, edw_wc_claimant_det_pk_id, edw_claim_party_occurrence_ak_id, claimant_status_type, claimant_direct_loss_status_type, claimant_exp_status_type, claimant_salvage_status_type, claimant_subrogation_status_type, claimant_other_recovery_status_type, claimant_financial_ind, claimant_supplemental_ind, claimant_recovery_ind, claimant_notice_only_claim_ind, claimant_open_date, claimant_close_date, claimant_reopen_date, claimant_closed_after_reopen_date, claimant_notice_only_date, claimant_addr_type, claimant_zip, claimant_state, claimant_county, claimant_city, claimant_addr, claimant_full_name, claimant_first_name, claimant_last_name, claimant_mid_name, claimant_tax_ssn_id, claimant_tax_fed_id, claimant_birthdate, claimant_gndr, claimant_num, denial_date, jurisdiction_state_code, jurisdiction_state_descript, emplyr_notified_date, rpted_to_carrier_date, jurisdiction_claim_num, care_directed_ind, care_directed_by, care_directed_by_descript, hired_state_code, hired_state_descript, hired_date, tax_filing_status, tax_filing_status_descript, occuptn_code, occuptn_code_descript, emplymnt_status_code, emplymnt_status_code_descript, len_of_time_in_crrnt_job, emp_dept_name, emp_shift_num, marital_status, marital_status_descript, num_of_dependents, num_of_dependent_children, num_of_other_dependents, num_of_exemptions, exemption_type, exemption_type_descript, emp_blind_ind, emp_over_65_ind, spouse_blind_ind, spouse_over_65_ind, education_lvl, med_auth_ind, auth_to_release_ssn_ind, emp_id_num, emp_id_type, emp_id_type_descript, emp_part_time_hour_week, emp_dept_num, emp_part_time_hourly_week_rate_amt, wage_rate_amt, wage_period_code, wage_period_code_descript, wage_eff_date, weeks_worked, gross_amt_type, gross_amt_type_descript, gross_wage_amt_excluding_tips, piece_work_num_of_weeks_excluding_overtime, emp_rec_meals, emp_rec_room, emp_rec_tips, overtime_amt, overtime_after_hour_in_a_week, overtime_after_hour_in_a_day, full_pay_inj_day_ind, salary_paid_ind, avg_full_time_days_week, avg_full_time_hours_day, avg_full_time_hours_week, avg_wkly_wage, num_of_full_time_emplymnt_same_job, num_of_part_time_emplymnt_same_job, ttd_rate, ppd_rate, ptd_rate, dtd_rate, wkly_attorney_fee, first_rpt_inj_date, supplementary_rpt_inj_date, fringe_bnft_discontinued_amt, emp_start_time, emp_hour_day, emp_hour_week, emp_day_week, inj_work_day_begin_time, disability_date, phys_restriction_ind, pre_exst_disability_ind, premises_code, premises_code_descript, work_process_descript, task_descript, body_part_code, body_part_code_descript, nature_inj_code, nature_inj_code_descript, cause_inj_code, cause_inj_code_descript, safeguard_not_used_ind, inj_substance_abuse_ind, sfty_device_not_used_ind, inj_rules_not_obeyed_ind, inj_result_occuptnal_inj_ind, inj_result_occuptnal_disease_ind, inj_result_death_ind, unsafe_act_descript, responsible_for_inj_descript, hazard_condition_descript, emp_last_day_worked, death_date, return_to_work_date, return_to_work_type, return_to_work_type_descript, return_to_work_with_same_emplyr_ind, emplyr_nature_bus_descript, emplyr_type_code, emplyr_type_code_descript, insd_type_code, insd_type_code_descript, subrogation_statute_exp_date, managed_care_org_type, managed_care_org_type_descript, subrogation_code, loss_condition, attorney_or_au_rep_ind, hospital_cost, doctor_cost, other_med_cost, controverted_case_code, surgery_ind, emplyr_loc_descript, inj_loc_comment, claim_ctgry_code, claim_ctgry_code_descript, act_status_code, act_status_code_descript, investigate_ind, sic_code, sic_code_descript, hospitalized_ind, wage_method_code, wage_method_code_descript, pms_occuptn_descript, pms_type_disability, ncci_type_cov, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date, edw_wc_claimant_work_hist_pk_id, wc_claimant_num, type_of_loss_code, type_of_loss_descript, pre_inj_avg_wkly_wage_code, pre_inj_avg_wkly_wage_descript, post_inj_wkly_wage_amt, impairment_disability_percentage, impairment_disability_percentage_basis_code, impairment_disability_percentage_basis_code_descript, max_med_improvement_date, med_extinguishment_ind, crrnt_work_status, edw_claim_med_pk_id, medicare_eligibility, positive_accommodation_period, negative_accommodation_period, positive_rtw_impact, negative_rtw_impact, InjuredWorkerLostTime, AutomaticAdjudicationClaimIndicator, CompensableClaimDescription)
	SELECT 
	claim_party_occurrence_id1 AS EDW_CLAIM_PARTY_OCCURRENCE_PK_ID, 
	claim_party_id1 AS EDW_CLAIM_PARTY_PK_ID, 
	claimant_calculation_id1 AS EDW_CLAIMANT_CALCULATION_PK_ID, 
	claimant_reserve_calculation_id_D1 AS EDW_CLAIMANT_RESERVE_CALCULATION_DIRECT_LOSS_PK_ID, 
	claimant_reserve_calculation_id_E1 AS EDW_CLAIMANT_RESERVE_CALCULATION_EXP_PK_ID, 
	claimant_reserve_calculation_id_B1 AS EDW_CLAIMANT_RESERVE_CALCULATION_SUBROGATION_PK_ID, 
	claimant_reserve_calculation_id_S1 AS EDW_CLAIMANT_RESERVE_CALCULATION_SALVAGE_PK_ID, 
	claimant_reserve_calculation_id_R1 AS EDW_CLAIMANT_RESERVE_CALCULATION_OTHER_RECOVERY_PK_ID, 
	wc_claimant_det_id1 AS EDW_WC_CLAIMANT_DET_PK_ID, 
	claim_party_occurrence_ak_id1 AS EDW_CLAIM_PARTY_OCCURRENCE_AK_ID, 
	claimant_status_code1 AS CLAIMANT_STATUS_TYPE, 
	claimant_direct_loss_status_code1 AS CLAIMANT_DIRECT_LOSS_STATUS_TYPE, 
	claimant_exp_status_code_out1 AS CLAIMANT_EXP_STATUS_TYPE, 
	claimant_salvage_status_code1 AS CLAIMANT_SALVAGE_STATUS_TYPE, 
	claimant_subrogation_status_code1 AS CLAIMANT_SUBROGATION_STATUS_TYPE, 
	claimant_other_recovery_status_code1 AS CLAIMANT_OTHER_RECOVERY_STATUS_TYPE, 
	claimant_financial_ind1 AS CLAIMANT_FINANCIAL_IND, 
	claimant_supplemental_ind1 AS CLAIMANT_SUPPLEMENTAL_IND, 
	claimant_recovery_ind1 AS CLAIMANT_RECOVERY_IND, 
	claimant_notice_only_ind1 AS CLAIMANT_NOTICE_ONLY_CLAIM_IND, 
	claimant_open_date1 AS CLAIMANT_OPEN_DATE, 
	claimant_close_date1 AS CLAIMANT_CLOSE_DATE, 
	claimant_reopen_date1 AS CLAIMANT_REOPEN_DATE, 
	claimant_closed_after_reopen_date1 AS CLAIMANT_CLOSED_AFTER_REOPEN_DATE, 
	claimant_noticeonly_date AS CLAIMANT_NOTICE_ONLY_DATE, 
	addr_type1 AS CLAIMANT_ADDR_TYPE, 
	claim_party_zip1 AS CLAIMANT_ZIP, 
	claim_party_state1 AS CLAIMANT_STATE, 
	claim_party_county1 AS CLAIMANT_COUNTY, 
	claim_party_city1 AS CLAIMANT_CITY, 
	claim_party_addr1 AS CLAIMANT_ADDR, 
	claim_party_full_name1 AS CLAIMANT_FULL_NAME, 
	claim_party_first_name1 AS CLAIMANT_FIRST_NAME, 
	claim_party_last_name1 AS CLAIMANT_LAST_NAME, 
	claim_party_mid_name1 AS CLAIMANT_MID_NAME, 
	tax_ssn_id1 AS CLAIMANT_TAX_SSN_ID, 
	tax_fed_id1 AS CLAIMANT_TAX_FED_ID, 
	claim_party_birthdate1 AS CLAIMANT_BIRTHDATE, 
	claim_party_gndr1 AS CLAIMANT_GNDR, 
	claimant_num_out1 AS CLAIMANT_NUM, 
	denial_date_out AS DENIAL_DATE, 
	jurisdiction_state_code_out1 AS JURISDICTION_STATE_CODE, 
	jurisdiction_state_descript_OUT1 AS JURISDICTION_STATE_DESCRIPT, 
	emplyr_notified_date_out1 AS EMPLYR_NOTIFIED_DATE, 
	rpted_to_carrier_date_out1 AS RPTED_TO_CARRIER_DATE, 
	jurisdiction_claim_num_out1 AS JURISDICTION_CLAIM_NUM, 
	care_directed_ind_out1 AS CARE_DIRECTED_IND, 
	care_directed_by_out1 AS CARE_DIRECTED_BY, 
	wc_care_directed_by_descript_OUT1 AS CARE_DIRECTED_BY_DESCRIPT, 
	hired_state_code_out1 AS HIRED_STATE_CODE, 
	hired_state_descript_OUT1 AS HIRED_STATE_DESCRIPT, 
	hired_date_out1 AS HIRED_DATE, 
	tax_filing_status_out1 AS TAX_FILING_STATUS, 
	tax_filing_status_descript_OUT1 AS TAX_FILING_STATUS_DESCRIPT, 
	occuptn_code_out1 AS OCCUPTN_CODE, 
	occuptn_descript_OUT1 AS OCCUPTN_CODE_DESCRIPT, 
	employement_status_code_out1 AS EMPLYMNT_STATUS_CODE, 
	wc_emplymnt_descript_OUT1 AS EMPLYMNT_STATUS_CODE_DESCRIPT, 
	len_of_time_in_crrnt_job_out AS LEN_OF_TIME_IN_CRRNT_JOB, 
	emp_dept_name_out1 AS EMP_DEPT_NAME, 
	emp_shift_num_out1 AS EMP_SHIFT_NUM, 
	marital_status_out1 AS MARITAL_STATUS, 
	marital_status_descript_OUT1 AS MARITAL_STATUS_DESCRIPT, 
	num_of_dependents_out1 AS NUM_OF_DEPENDENTS, 
	num_of_dependent_children_out1 AS NUM_OF_DEPENDENT_CHILDREN, 
	num_of_other_dependents_out1 AS NUM_OF_OTHER_DEPENDENTS, 
	num_of_exemptions_out1 AS NUM_OF_EXEMPTIONS, 
	exemption_type_out1 AS EXEMPTION_TYPE, 
	wc_exemption_type_descript_OUT1 AS EXEMPTION_TYPE_DESCRIPT, 
	emp_blind_ind_out1 AS EMP_BLIND_IND, 
	emp_over_65_ind_out1 AS EMP_OVER_65_IND, 
	spouse_blind_ind_out1 AS SPOUSE_BLIND_IND, 
	spouse_over_65_ind_out1 AS SPOUSE_OVER_65_IND, 
	education_lvl_out1 AS EDUCATION_LVL, 
	med_auth_ind_out1 AS MED_AUTH_IND, 
	auth_to_release_ssn_ind_out1 AS AUTH_TO_RELEASE_SSN_IND, 
	emp_id_num_out1 AS EMP_ID_NUM, 
	emp_id_type_out AS EMP_ID_TYPE, 
	emp_id_type_descript_OUT1 AS EMP_ID_TYPE_DESCRIPT, 
	emp_part_time_hour_week_out1 AS EMP_PART_TIME_HOUR_WEEK, 
	emp_dept_num_out1 AS EMP_DEPT_NUM, 
	emp_part_time_hourly_week_rate_amt_out1 AS EMP_PART_TIME_HOURLY_WEEK_RATE_AMT, 
	wage_rate_amt_out1 AS WAGE_RATE_AMT, 
	wage_period_code_out1 AS WAGE_PERIOD_CODE, 
	wage_period_descript_OUT1 AS WAGE_PERIOD_CODE_DESCRIPT, 
	wage_eff_date_out1 AS WAGE_EFF_DATE, 
	weeks_worked_out1 AS WEEKS_WORKED, 
	gross_amt_type_out1 AS GROSS_AMT_TYPE, 
	wage_gross_amt_type_descript_OUT1 AS GROSS_AMT_TYPE_DESCRIPT, 
	gross_wage_amt_excluding_tips_out1 AS GROSS_WAGE_AMT_EXCLUDING_TIPS, 
	piece_work_num_of_weeks_excluding_overtime_out1 AS PIECE_WORK_NUM_OF_WEEKS_EXCLUDING_OVERTIME, 
	emp_rec_meals_out1 AS EMP_REC_MEALS, 
	emp_rec_room_out1 AS EMP_REC_ROOM, 
	emp_rec_tips_out1 AS EMP_REC_TIPS, 
	overtime_amt_out1 AS OVERTIME_AMT, 
	overtime_after_hour_in_a_week_out1 AS OVERTIME_AFTER_HOUR_IN_A_WEEK, 
	overtime_after_hour_in_a_day_out1 AS OVERTIME_AFTER_HOUR_IN_A_DAY, 
	full_pay_inj_day_ind_out1 AS FULL_PAY_INJ_DAY_IND, 
	salary_paid_ind_out1 AS SALARY_PAID_IND, 
	avg_full_time_days_week_out1 AS AVG_FULL_TIME_DAYS_WEEK, 
	avg_full_time_hours_day_out1 AS AVG_FULL_TIME_HOURS_DAY, 
	avg_full_time_hours_week_out1 AS AVG_FULL_TIME_HOURS_WEEK, 
	avg_wkly_wage_out1 AS AVG_WKLY_WAGE, 
	num_of_full_time_emplymnt_same_job_out1 AS NUM_OF_FULL_TIME_EMPLYMNT_SAME_JOB, 
	num_of_part_time_emplymnt_same_job_out1 AS NUM_OF_PART_TIME_EMPLYMNT_SAME_JOB, 
	ttd_rate_out1 AS TTD_RATE, 
	ppd_rate_out1 AS PPD_RATE, 
	ptd_rate_out1 AS PTD_RATE, 
	dtd_rate_out1 AS DTD_RATE, 
	wkly_attorney_fee_out1 AS WKLY_ATTORNEY_FEE, 
	first_rpt_inj_date_out1 AS FIRST_RPT_INJ_DATE, 
	supplementary_rpt_inj_date_out1 AS SUPPLEMENTARY_RPT_INJ_DATE, 
	fringe_bnft_discontinued_amt_out1 AS FRINGE_BNFT_DISCONTINUED_AMT, 
	emp_start_time_out1 AS EMP_START_TIME, 
	emp_hour_day_out1 AS EMP_HOUR_DAY, 
	emp_hour_week_out1 AS EMP_HOUR_WEEK, 
	emp_day_week_out1 AS EMP_DAY_WEEK, 
	inj_work_day_begin_time_out1 AS INJ_WORK_DAY_BEGIN_TIME, 
	disability_date_out1 AS DISABILITY_DATE, 
	phys_restriction_ind_out1 AS PHYS_RESTRICTION_IND, 
	pre_exst_disability_ind_out1 AS PRE_EXST_DISABILITY_IND, 
	premises_code_out1 AS PREMISES_CODE, 
	premises_descript_OUT1 AS PREMISES_CODE_DESCRIPT, 
	work_process_descript_out1 AS WORK_PROCESS_DESCRIPT, 
	task_descript_out1 AS TASK_DESCRIPT, 
	body_part_code_out1 AS BODY_PART_CODE, 
	body_part_descript_OUT1 AS BODY_PART_CODE_DESCRIPT, 
	nature_inj_code_out1 AS NATURE_INJ_CODE, 
	nature_of_inj_descript_OUT1 AS NATURE_INJ_CODE_DESCRIPT, 
	cause_inj_code_out1 AS CAUSE_INJ_CODE, 
	cause_of_inj_descript_OUT1 AS CAUSE_INJ_CODE_DESCRIPT, 
	safeguard_not_used_ind_out1 AS SAFEGUARD_NOT_USED_IND, 
	inj_substance_abuse_ind_out1 AS INJ_SUBSTANCE_ABUSE_IND, 
	sfty_device_not_used_ind_out1 AS SFTY_DEVICE_NOT_USED_IND, 
	inj_rules_not_obeyed_ind_out1 AS INJ_RULES_NOT_OBEYED_IND, 
	inj_result_occuptnal_inj_ind_out1 AS INJ_RESULT_OCCUPTNAL_INJ_IND, 
	inj_result_occuptnal_disease_ndicator_out1 AS INJ_RESULT_OCCUPTNAL_DISEASE_IND, 
	inj_result_death_ind_out1 AS INJ_RESULT_DEATH_IND, 
	unsafe_act_descript_out1 AS UNSAFE_ACT_DESCRIPT, 
	responsible_for_inj_descript_out1 AS RESPONSIBLE_FOR_INJ_DESCRIPT, 
	hazard_condition_descript_out1 AS HAZARD_CONDITION_DESCRIPT, 
	emp_last_day_worked_out1 AS EMP_LAST_DAY_WORKED, 
	death_date_out1 AS DEATH_DATE, 
	return_to_work_date_out1 AS RETURN_TO_WORK_DATE, 
	return_to_work_type_out1 AS RETURN_TO_WORK_TYPE, 
	return_to_work_descript_OUT1 AS RETURN_TO_WORK_TYPE_DESCRIPT, 
	return_to_work_with_same_emplyr_ind_out1 AS RETURN_TO_WORK_WITH_SAME_EMPLYR_IND, 
	emplyr_nature_bus_descript_out1 AS EMPLYR_NATURE_BUS_DESCRIPT, 
	emplyr_type_code_out1 AS EMPLYR_TYPE_CODE, 
	emplyr_type_descript_OUT1 AS EMPLYR_TYPE_CODE_DESCRIPT, 
	nsd_type_code_out1 AS INSD_TYPE_CODE, 
	insd_type_descript_OUT1 AS INSD_TYPE_CODE_DESCRIPT, 
	subrogation_statute_exp_date_out1 AS SUBROGATION_STATUTE_EXP_DATE, 
	managed_care_org_type_out1 AS MANAGED_CARE_ORG_TYPE, 
	managed_care_org_type_descript_OUT1 AS MANAGED_CARE_ORG_TYPE_DESCRIPT, 
	subrogation_code_out1 AS SUBROGATION_CODE, 
	loss_condition_out1 AS LOSS_CONDITION, 
	attorney_or_au_rep_ind_out1 AS ATTORNEY_OR_AU_REP_IND, 
	hospital_cost_out1 AS HOSPITAL_COST, 
	doctor_cost_out1 AS DOCTOR_COST, 
	other_med_cost_out1 AS OTHER_MED_COST, 
	controverted_case_code_out1 AS CONTROVERTED_CASE_CODE, 
	surgery_ind_out1 AS SURGERY_IND, 
	emplyr_loc_descript_out1 AS EMPLYR_LOC_DESCRIPT, 
	inj_loc_comment_out1 AS INJ_LOC_COMMENT, 
	claim_ctgry_code_out1 AS CLAIM_CTGRY_CODE, 
	claim_ctgry_code_descript1 AS CLAIM_CTGRY_CODE_DESCRIPT, 
	act_status_code_out1 AS ACT_STATUS_CODE, 
	ACT_STATUS_CODE_DESCRIPT, 
	investigate_ind_out1 AS INVESTIGATE_IND, 
	sic_code_out AS SIC_CODE, 
	SIC_CODE_DESCRIPT, 
	hospitalized_ind_out1 AS HOSPITALIZED_IND, 
	wage_method_code_out1 AS WAGE_METHOD_CODE, 
	wage_method_code_descript1 AS WAGE_METHOD_CODE_DESCRIPT, 
	pms_occuptn_descript_out1 AS PMS_OCCUPTN_DESCRIPT, 
	pms_type_disability_out1 AS PMS_TYPE_DISABILITY, 
	ncci_type_cov_out1 AS NCCI_TYPE_COV, 
	crrnt_snpsht_flag1 AS CRRNT_SNPSHT_FLAG, 
	audit_id1 AS AUDIT_ID, 
	eff_from_date1 AS EFF_FROM_DATE, 
	eff_to_date1 AS EFF_TO_DATE, 
	created_date1 AS CREATED_DATE, 
	modified_date1 AS MODIFIED_DATE, 
	wc_work_hist_pk_id1 AS EDW_WC_CLAIMANT_WORK_HIST_PK_ID, 
	wc_claimant_num1 AS WC_CLAIMANT_NUM, 
	type_of_loss_code1 AS TYPE_OF_LOSS_CODE, 
	type_of_loss_code_descript1 AS TYPE_OF_LOSS_DESCRIPT, 
	pre_injury_avg_wkly_wage_code1 AS PRE_INJ_AVG_WKLY_WAGE_CODE, 
	pre_injury_avg_wkly_wage_code_descript1 AS PRE_INJ_AVG_WKLY_WAGE_DESCRIPT, 
	post_inj_wkly_wage_amt1 AS POST_INJ_WKLY_WAGE_AMT, 
	impairment_disability_percentage1 AS IMPAIRMENT_DISABILITY_PERCENTAGE, 
	impairment_disability_percentage_basis_code1 AS IMPAIRMENT_DISABILITY_PERCENTAGE_BASIS_CODE, 
	impairment_disability_percentage_basis_code_descript1 AS IMPAIRMENT_DISABILITY_PERCENTAGE_BASIS_CODE_DESCRIPT, 
	max_med_improvement_date1 AS MAX_MED_IMPROVEMENT_DATE, 
	med_extinguishment_ind1 AS MED_EXTINGUISHMENT_IND, 
	current_work_status1 AS CRRNT_WORK_STATUS, 
	claim_med_id_out1 AS EDW_CLAIM_MED_PK_ID, 
	medicare_eligibility_out1 AS MEDICARE_ELIGIBILITY, 
	POSITIVE_ACCOMMODATION_PERIOD, 
	NEGATIVE_ACCOMMODATION_PERIOD, 
	POSITIVE_RTW_IMPACT, 
	NEGATIVE_RTW_IMPACT, 
	InjuredWorkerLostTime1 AS INJUREDWORKERLOSTTIME, 
	AutomaticAdjudicationClaimIndicator1 AS AUTOMATICADJUDICATIONCLAIMINDICATOR, 
	CompensableClaimDescription1 AS COMPENSABLECLAIMDESCRIPTION
	FROM UPD_claimant_dim_Insert
),
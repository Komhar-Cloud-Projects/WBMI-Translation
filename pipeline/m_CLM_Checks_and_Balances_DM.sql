WITH
SQ_wbmi_checkout_Dashboard AS (
	SELECT 'Count of reported claims among different grains that dont match in Dashboard = ' + convert(varchar,cnt),
	'CLAIM_DASHBOARD_FACT',cnt FROM 
	(
	SELECT COUNT(*) cnt
	FROM
	(Select sum(rpted_claims) as SUM_REPORTED_REP
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_dashboard_fact 
	where grain_lvl_ind = 'REP') A ,
	(Select sum(rpted_claims) as SUM_REPORTED_TEAM
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_dashboard_fact 
	where grain_lvl_ind = 'TEAM') B,
	(Select sum(rpted_claims) as SUM_REPORTED_DEPT
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_dashboard_fact 
	where grain_lvl_ind = 'DEPT') C,
	(Select sum(rpted_claims) as SUM_REPORTED_DVSN
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_dashboard_fact 
	where grain_lvl_ind = 'DVSN') D
	WHERE 
	 A.SUM_REPORTED_REP <>  B.SUM_REPORTED_TEAM 
	OR  A.SUM_REPORTED_REP <> C.SUM_REPORTED_DEPT
	OR  A.SUM_REPORTED_REP <>  D.SUM_REPORTED_DVSN
	) AS SUM_OF_REPORTED_CLAIMS
	
	UNION
	
	SELECT 'Count of closed claims among different grains that dont match in Dashboard = ' + convert(varchar,cnt),
	'CLAIM_DASHBOARD_FACT',cnt FROM (
	SELECT COUNT(*) cnt
	FROM
	(Select sum(closed_claims) as SUM_CLOSED_REP
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_dashboard_fact 
	where grain_lvl_ind = 'REP') A ,
	(Select sum(closed_claims) as SUM_CLOSED_TEAM
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_dashboard_fact 
	where grain_lvl_ind = 'TEAM') B,
	(Select sum(closed_claims) as SUM_CLOSED_DEPT
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_dashboard_fact 
	where grain_lvl_ind = 'DEPT') C,
	(Select sum(closed_claims) as SUM_CLOSED_DVSN
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_dashboard_fact 
	where grain_lvl_ind = 'DVSN') D
	WHERE 
	 A.SUM_CLOSED_REP <>  B.SUM_CLOSED_TEAM 
	OR  A.SUM_CLOSED_REP <> C.SUM_CLOSED_DEPT
	OR  A.SUM_CLOSED_REP <>  D.SUM_CLOSED_DVSN
	)  AS SUM_OF_CLOSED_CLAIMS
	
	UNION
	
	SELECT 'Count of pending claims among different grains that dont match in Dashboard = ' + convert(varchar,cnt),
	'CLAIM_DASHBOARD_FACT',cnt FROM (
	SELECT COUNT(*) cnt
	FROM
	(Select sum(pend_claims) as SUM_PENDING_REP
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_dashboard_fact 
	where grain_lvl_ind = 'REP') A ,
	(Select sum(pend_claims) as SUM_PENDING_TEAM
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_dashboard_fact 
	where grain_lvl_ind = 'TEAM') B,
	(Select sum(pend_claims) as SUM_PENDING_DEPT
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_dashboard_fact 
	where grain_lvl_ind = 'DEPT') C,
	(Select sum(pend_claims) as SUM_PENDING_DVSN
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_dashboard_fact 
	where grain_lvl_ind = 'DVSN') D
	WHERE 
	 A.SUM_PENDING_REP <>  B.SUM_PENDING_TEAM 
	OR  A.SUM_PENDING_REP <> C.SUM_PENDING_DEPT
	OR  A.SUM_PENDING_REP <>  D.SUM_PENDING_DVSN
	) AS SUM_OF_PENDING_CLAIMS
	
	UNION
	
	SELECT 'Count of reopen claims among different grains that dont match in Dashboard = ' + convert(varchar,cnt),
	'CLAIM_DASHBOARD_FACT',cnt FROM (
	SELECT COUNT(*) cnt
	FROM
	(Select sum(reopened_claims) as SUM_REOPENED_REP
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_dashboard_fact 
	where grain_lvl_ind = 'REP') A ,
	(Select sum(reopened_claims) as SUM_REOPENED_TEAM
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_dashboard_fact 
	where grain_lvl_ind = 'TEAM') B,
	(Select sum(reopened_claims) as SUM_REOPENED_DEPT
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_dashboard_fact 
	where grain_lvl_ind = 'DEPT') C,
	(Select sum(reopened_claims) as SUM_REOPENED_DVSN
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_dashboard_fact 
	where grain_lvl_ind = 'DVSN') D
	WHERE 
	 A.SUM_REOPENED_REP <>  B.SUM_REOPENED_TEAM 
	OR  A.SUM_REOPENED_REP <> C.SUM_REOPENED_DEPT
	OR  A.SUM_REOPENED_REP <>  D.SUM_REOPENED_DVSN
	)  AS SUM_OF_REOPEN_CLAIMS
	
	UNION
	
	SELECT 'Count of TI and TO that dont match at a rep level = ' + convert(varchar,cnt),
	'CLAIM_DASHBOARD_FACT',cnt FROM (
	Select count(*) cnt 
	FROM 
	(Select sum(transferred_in_claims) as SUM_TI_REP
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_dashboard_fact 
	where grain_lvl_ind = 'REP') A, 
	(Select sum(transferred_out_claims) as SUM_TO_REP
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_dashboard_fact 
	where grain_lvl_ind = 'REP') B
	WHERE a.SUM_TI_REP <> B.SUM_TO_REP
	)  AS SUM_OF_TI_T0_REP
	
	
	UNION
	
	SELECT 'Count of TI and TO that dont match at a team level = ' + convert(varchar,cnt),
	'CLAIM_DASHBOARD_FACT',cnt FROM (
	Select count(*) cnt
	FROM 
	(Select sum(transferred_in_claims) as SUM_TI_TEAM
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_dashboard_fact 
	where grain_lvl_ind = 'TEAM') A, 
	(Select sum(transferred_out_claims) as SUM_TO_TEAM
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_dashboard_fact 
	where grain_lvl_ind = 'TEAM') B
	WHERE a.SUM_TI_TEAM <> B.SUM_TO_TEAM
	)  AS SUM_OF_TI_T0_TEAM
	
	
	UNION
	
	SELECT 'Count of TI and TO that dont match at a dept level = ' + convert(varchar,cnt),
	'CLAIM_DASHBOARD_FACT',cnt FROM (
	Select COUNT(*) cnt
	FROM 
	(Select sum(transferred_in_claims) as SUM_TI_DEPT
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_dashboard_fact 
	where grain_lvl_ind = 'DEPT') A, 
	(Select sum(transferred_out_claims) as SUM_TO_DEPT
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_dashboard_fact 
	where grain_lvl_ind = 'DEPT') B
	WHERE a.SUM_TI_DEPT <> B.SUM_TO_DEPT
	)  AS SUM_OF_TI_T0_DEPT
	
	UNION
	
	SELECT 'Count of TI and TO that dont match at a divison level = ' + convert(varchar,cnt),
	'CLAIM_DASHBOARD_FACT',cnt FROM (
	Select count(*) cnt 
	FROM 
	(Select sum(transferred_in_claims) as SUM_TI_DVSN
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_dashboard_fact 
	where grain_lvl_ind = 'DVSN') A, 
	(Select sum(transferred_out_claims) as SUM_TO_DVSN
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_dashboard_fact 
	where grain_lvl_ind = 'DVSN') B
	WHERE a.SUM_TI_DVSN <> B.SUM_TO_DVSN
	)  AS SUM_OF_TI_T0_DVSN
),
EXP_DASHBOARD AS (
	SELECT
	checkout_message AS check_out_message,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	target_name,
	target_count,
	@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID} AS WBMI_SESSION_CONTROL_RUN_ID,
	'InformS' AS User_ID,
	0 AS Default_Amt,
	'E' AS checkout_type
	FROM SQ_wbmi_checkout_Dashboard
),
FIL_ZERO_COUNTS7 AS (
	SELECT
	check_out_message, 
	created_date, 
	modified_date, 
	target_name, 
	target_count, 
	WBMI_SESSION_CONTROL_RUN_ID, 
	User_ID, 
	Default_Amt, 
	checkout_type
	FROM EXP_DASHBOARD
	WHERE target_count > 0
),
UPD_CHECKOUT_TABLE_INS7 AS (
	SELECT
	check_out_message, 
	created_date, 
	modified_date, 
	target_name, 
	target_count, 
	WBMI_SESSION_CONTROL_RUN_ID, 
	User_ID, 
	Default_Amt, 
	checkout_type
	FROM FIL_ZERO_COUNTS7
),
wbmi_checkout_dashboard_fact AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, target_name, target_count, created_user_id, created_date, modified_user_id, modified_date)
	SELECT 
	WBMI_SESSION_CONTROL_RUN_ID AS WBMI_SESSION_CONTROL_RUN_ID, 
	checkout_type AS CHECKOUT_TYPE_CODE, 
	check_out_message AS CHECKOUT_MESSAGE, 
	TARGET_NAME, 
	Default_Amt AS TARGET_COUNT, 
	User_ID AS CREATED_USER_ID, 
	CREATED_DATE, 
	User_ID AS MODIFIED_USER_ID, 
	MODIFIED_DATE
	FROM UPD_CHECKOUT_TABLE_INS7
),
SQ_wbmi_checkout_Claim_Occ_Dim AS (
	SELECT  'Count of claims in claim occurrence that are not in the claim occurrence dim table = ' + convert(varchar,cnt),
	'CLAIM_OCCURRENCE_DIM',
	cnt
	 from (
	SELECT COUNT(DISTINCT CLAIM_OCCURRENCE_AK_ID)  cnt  FROM RPT_EDM.dbo.claim_occurrence a
	where 
	not exists (
	SELECT 1 from dbo.claim_occurrence_dim b
	where a.claim_occurrence_ak_id = b.edw_claim_occurrence_ak_id)
	)as cnt
	
	UNION
	
	SELECT  'Count of claims in claim occ calc that are not in the claim occurrence dim table = ' + convert(varchar,cnt) ,
	'CLAIM_OCCURRENCE_DIM',
	cnt
	from (
	SELECT count(DISTINCT CLAIM_OCCURRENCE_AK_Id) cnt  from rpt_edm.dbo.claim_occurrence_calculation a
	where 
	not exists (
	SELECT 1 from dbo.claim_occurrence_dim b
	where a.claim_occurrence_ak_Id = b.edw_claim_occurrence_ak_id)
	)as cnt
	
	UNION
	
	SELECT  'Count of claims in claim occ reserve calc that are not in the claim occurrence dim table = ' + convert(varchar,cnt) ,
	'CLAIM_OCCURRENCE_DIM',
	cnt
	from (
	SELECT count(DISTINCT CLAIM_OCCURRENCE_AK_Id) cnt  from rpt_edm.dbo.claim_occurrence_reserve_calculation a
	where 
	not exists (
	SELECT 1 from dbo.claim_occurrence_dim b
	where a.claim_occurrence_ak_Id = b.edw_claim_occurrence_ak_id)
	)as cnt
	
	UNION
	
	SELECT  'Count of claims in claim occurrence that are not in the claim occurrence dim table for a specific date/time = ' + convert(varchar,cnt) ,
	'CLAIM_OCCURRENCE_DIM',
	cnt
	from (
	SELECT COUNT(DISTINCT CLAIM_OCCURRENCE_AK_ID) cnt FROM rpt_edm.dbo.CLAIM_OCCURRENCE A WHERE NOT EXISTS 
	(
	SELECT 1 FROM CLAIM_OCCURRENCE_DIM B 
	WHERE A.CLAIM_OCCURRENCE_AK_ID = B.EDW_CLAIM_OCCURRENCE_AK_iD AND 
	A.EFF_FROM_DATE BETWEEN B.EFF_fROM_DATE AND B.EFF_TO_DATE
	)
	)as cnt
	
	UNION
	
	SELECT  'Count of claims in claim occurrence calc that are not in the claim occurrence dim table for a specific date/time = ' + convert(varchar,cnt) ,
	'CLAIM_OCCURRENCE_DIM',
	cnt
	from (
	SELECT COUNT(DISTINCT CLAIM_OCCURRENCE_AK_ID) cnt FROM rpt_edm.dbo.CLAIM_OCCURRENCE_calculation A WHERE NOT EXISTS 
	(
	SELECT 1 FROM CLAIM_OCCURRENCE_DIM B 
	WHERE A.CLAIM_OCCURRENCE_AK_ID = B.EDW_CLAIM_OCCURRENCE_AK_iD AND 
	A.EFF_FROM_DATE BETWEEN B.EFF_fROM_DATE AND B.EFF_TO_DATE
	)
	)as cnt
	
	UNION
	
	SELECT  'Count of claims in claim occurrence reserve calc that are not in the claim occurrence dim table for a specific date/time = ' + convert(varchar,cnt) ,
	'CLAIM_OCCURRENCE_DIM',
	cnt
	from (
	SELECT COUNT(DISTINCT CLAIM_OCCURRENCE_AK_ID) cnt FROM rpt_edm.dbo.CLAIM_OCCURRENCE_reserve_calculation A WHERE NOT EXISTS 
	(
	SELECT 1 FROM CLAIM_OCCURRENCE_DIM B 
	WHERE A.CLAIM_OCCURRENCE_AK_ID = B.EDW_CLAIM_OCCURRENCE_AK_iD AND 
	A.EFF_FROM_DATE BETWEEN B.EFF_fROM_DATE AND B.EFF_TO_DATE
	)
	)as cnt
	
	UNION
	
	SELECT  'Count of claims in claim occurrence dim where eff_to_date < eff_from_date = ' + convert(varchar,cnt) ,
	'CLAIM_OCCURRENCE_DIM',
	cnt
	from (
	SELECT COUNT(DISTINCT EDW_CLAIM_OCCURRENCE_ak_ID) CNT FROM CLAIM_OCCURRENCE_DIM WHERE  EFF_TO_DATE < EFF_FROM_DATE
	)as cnt
	
	UNION
	
	SELECT  'Count of claims in claim occurrence dim with an incorrect current row = ' + convert(varchar,cnt) ,
	'CLAIM_OCCURRENCE_DIM',
	cnt
	from (
	SELECT COUNT(DISTINCT EDW_CLAIM_OCCURRENCE_AK_ID) CNT FROM CLAIM_OCCURRENCE_DIM WHERE CRRNT_SNPSHT_FLAG = 1 AND EFF_TO_DATE <> '2100-12-31 23:59:59.000'
	)as cnt
),
EXP_CLAIM_OCC_DIM AS (
	SELECT
	checkout_message AS check_out_message,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	target_name,
	target_count,
	@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID} AS WBMI_SESSION_CONTROL_RUN_ID,
	'InformS' AS User_ID,
	0 AS Default_Amt,
	'E' AS checkout_type
	FROM SQ_wbmi_checkout_Claim_Occ_Dim
),
FIL_ZERO_COUNTS4 AS (
	SELECT
	check_out_message, 
	created_date, 
	modified_date, 
	target_name, 
	target_count, 
	WBMI_SESSION_CONTROL_RUN_ID, 
	User_ID, 
	Default_Amt, 
	checkout_type
	FROM EXP_CLAIM_OCC_DIM
	WHERE target_count > 0
),
UPD_CHECKOUT_TABLE_INS4 AS (
	SELECT
	check_out_message, 
	created_date, 
	modified_date, 
	target_name, 
	target_count, 
	WBMI_SESSION_CONTROL_RUN_ID, 
	User_ID, 
	Default_Amt, 
	checkout_type
	FROM FIL_ZERO_COUNTS4
),
wbmi_checkout_clm_occ_dim AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, target_name, target_count, created_user_id, created_date, modified_user_id, modified_date)
	SELECT 
	WBMI_SESSION_CONTROL_RUN_ID AS WBMI_SESSION_CONTROL_RUN_ID, 
	checkout_type AS CHECKOUT_TYPE_CODE, 
	check_out_message AS CHECKOUT_MESSAGE, 
	TARGET_NAME, 
	Default_Amt AS TARGET_COUNT, 
	User_ID AS CREATED_USER_ID, 
	CREATED_DATE, 
	User_ID AS MODIFIED_USER_ID, 
	MODIFIED_DATE
	FROM UPD_CHECKOUT_TABLE_INS4
),
SQ_wbmi_checkout_Clmt_Cov_Dim AS (
	select  'Count of coverage in claimant coverage that are not in the claimant coverage dim table = ' + convert(varchar,cnt) ,'CLAIMANT_COVERAGE_DIM'
	,cnt
	from (
	
	SELECT COUNT(DISTINCT claimant_cov_det_ak_id)  cnt  FROM RPT_EDM.dbo.claimant_coverage_detail a
	where 
	not exists (
	SELECT 1 from dbo.claimant_coverage_dim b
	where a.claimant_cov_det_ak_id = b.edw_claimant_cov_det_ak_id)
	
	)as cnt
	
	
	UNION
	
	
	select  'Count of coverages in claimant cov det calc that are not in the claimant coverage dim table = ' + convert(varchar,cnt) ,'CLAIMANT_COVERAGE_DIM' ,cnt
	from (
	
	
	SELECT count(distinct claimant_cov_det_ak_id) cnt FROM rpt_edm.dbo.claimant_coverage_detail_calculation a
	where  
	not exists (
	SELECT 1 from dbo.claimant_coverage_dim b
	where a.claimant_cov_det_ak_id = b.edw_claimant_cov_det_ak_id)
	
	)as cnt
	
	
	
	UNION
	
	select  'Count of coverages in claimant cov det reserve calc that are not in the claimant coverage dim table = ' + convert(varchar,cnt) ,'CLAIMANT_COVERAGE_DIM',cnt
	from (
	
	
	SELECT count(distinct claimant_cov_det_ak_id) cnt FROM rpt_edm.dbo.claimant_coverage_detail_reserve_calculation a
	where  
	not exists (
	SELECT 1 from dbo.claimant_coverage_dim b
	where a.claimant_cov_det_ak_id = b.edw_claimant_cov_det_ak_id)
	
	)as cnt
	
	UNION
	
	
	select  'Count of coverages in claimant coverage detail that are not in the claimant coverage dim table for a specific date/time = ' + convert(varchar,cnt) ,'CLAIMANT_COVERAGE_DIM',cnt
	from (
	
	SELECT count(distinct claimant_cov_det_ak_Id) cnt FROM RPT_EDM.DBO.CLAIMANT_COVERAGE_DETAIL A
	WHERE  
	NOT EXISTS
	(
	        SELECT 1 FROM DBO.CLAIMANT_COVERAGE_DIM Z
	        WHERE
	        A.CLAIMANT_COV_DET_AK_ID = Z.EDW_CLAIMANT_COV_DET_AK_ID AND
	        A.EFF_FROM_DATE between Z.EFF_FROM_DATE and Z.EFF_TO_DATE
	)
	
	)as cnt
	
	
	UNION
	
	
	select  'Count of coverages in claimant coverage detail calc that are not in the claimant coverage dim table for a specific date/time = ' + convert(varchar,cnt) ,'CLAIMANT_COVERAGE_DIM',cnt
	from (
	
	SELECT count(distinct claimant_cov_det_ak_Id) cnt FROM RPT_EDM.DBO.CLAIMANT_COVERAGE_DETAIL_calculation A
	WHERE  
	NOT EXISTS
	(
	        SELECT 1 FROM DBO.CLAIMANT_COVERAGE_DIM Z
	        WHERE
	        A.CLAIMANT_COV_DET_AK_ID = Z.EDW_CLAIMANT_COV_DET_AK_ID AND
	        A.EFF_FROM_DATE between Z.EFF_FROM_DATE and Z.EFF_TO_DATE
	)
	
	)as cnt
	
	UNION
	
	select  'Count of coverages in claimant coverage detail reserve calc that are not in the claimant coverage dim table for a specific date/time = ' + convert(varchar,cnt) ,'CLAIMANT_COVERAGE_DIM' ,cnt
	from (
	
	SELECT count(distinct claimant_cov_det_ak_Id) cnt FROM RPT_EDM.DBO.CLAIMANT_COVERAGE_DETAIL_reserve_calculation A
	WHERE  
	NOT EXISTS
	(
	        SELECT 1 FROM DBO.CLAIMANT_COVERAGE_DIM Z
	        WHERE
	        A.CLAIMANT_COV_DET_AK_ID = Z.EDW_CLAIMANT_COV_DET_AK_ID AND
	        A.EFF_FROM_DATE between Z.EFF_FROM_DATE and Z.EFF_TO_DATE
	)
	
	)as cnt
	
	UNION
	
	select  'Count of coverages in claimant coverage dim where eff_to_date < eff_from_date = ' + convert(varchar,cnt) ,'CLAIMANT_COVERAGE_DIM' ,cnt
	from (
	
	SELECT COUNT(DISTINCT edw_claimant_cov_det_ak_id) CNT FROM claimant_coverage_dim WHERE  EFF_TO_DATE < EFF_FROM_DATE
	
	)as cnt
	
	UNION
	
	select  'Count of coverages in claimant coverage dim with more than one current row = ' + convert(varchar,cnt) ,'CLAIMANT_COVERAGE_DIM',cnt
	from (
	
	
	SELECT count(distinct edw_claimant_cov_det_ak_id) cnt  FROM claimant_coverage_dim
	WHERE  CRRNT_SNPSHT_FLAG = 1 
	GROUP BY edw_claimant_cov_det_ak_id HAVING COUNT(*) > 1 
	
	)as cnt
	
	
	UNION
	
	select  'Count of coverages in claimant coverage dim without a current row  = ' + convert(varchar,cnt) ,'CLAIMANT_COVERAGE_DIM' ,cnt
	from (
	
	
	SELECT count(distinct edw_claimant_cov_det_ak_id) cnt  FROM claimant_coverage_dim 
	GROUP BY edw_claimant_cov_det_ak_id HAVING max(CRRNT_SNPSHT_FLAG) <> 1 
	
	)as cnt
	
	UNION
	
	
	select  'Count of coverages in claimant coverage dim with an incorrect current row = ' + convert(varchar,cnt) ,'CLAIMANT_COVERAGE_DIM',cnt
	from (
	
	SELECT COUNT(DISTINCT edw_claimant_cov_det_ak_id) CNT FROM claimant_coverage_dim WHERE CRRNT_SNPSHT_FLAG = 1 AND EFF_TO_DATE <> '2100-12-31 23:59:59.000'
	
	)as cnt
),
EXP_CLAIMNT_COV_DIM AS (
	SELECT
	checkout_message AS check_out_message,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	target_name,
	target_count,
	@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID} AS WBMI_SESSION_CONTROL_RUN_ID,
	'InformS' AS User_ID,
	0 AS Default_Amt,
	'E' AS checkout_type
	FROM SQ_wbmi_checkout_Clmt_Cov_Dim
),
FIL_ZERO_COUNTS3 AS (
	SELECT
	check_out_message, 
	created_date, 
	modified_date, 
	target_name, 
	target_count, 
	WBMI_SESSION_CONTROL_RUN_ID, 
	User_ID, 
	Default_Amt, 
	checkout_type
	FROM EXP_CLAIMNT_COV_DIM
	WHERE target_count > 0
),
UPD_CHECKOUT_TABLE_INS3 AS (
	SELECT
	check_out_message, 
	created_date, 
	modified_date, 
	target_name, 
	target_count, 
	WBMI_SESSION_CONTROL_RUN_ID, 
	User_ID, 
	Default_Amt, 
	checkout_type
	FROM FIL_ZERO_COUNTS3
),
wbmi_checkout_clmt_cov_dim AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, target_name, target_count, created_user_id, created_date, modified_user_id, modified_date)
	SELECT 
	WBMI_SESSION_CONTROL_RUN_ID AS WBMI_SESSION_CONTROL_RUN_ID, 
	checkout_type AS CHECKOUT_TYPE_CODE, 
	check_out_message AS CHECKOUT_MESSAGE, 
	TARGET_NAME, 
	Default_Amt AS TARGET_COUNT, 
	User_ID AS CREATED_USER_ID, 
	CREATED_DATE, 
	User_ID AS MODIFIED_USER_ID, 
	MODIFIED_DATE
	FROM UPD_CHECKOUT_TABLE_INS3
),
SQ_wbmi_checkout_Clmt_Dim AS (
	select  'Count of party occurrence in claim party occurrence that are not in the claimant dim table = ' + convert(varchar,cnt) ,'CLAIMANT_DIM',cnt from (
	
	SELECT COUNT(DISTINCT claim_party_occurrence_ak_id)  cnt  FROM RPT_EDM.dbo.claim_party_occurrence a
	where 
	not exists (
	SELECT 1 from dbo.claimant_dim b
	where a.claim_party_occurrence_ak_id = b.edw_claim_party_occurrence_ak_id)
	
	)as cnt
	
	
	union
	
	
	select  'Count of party occurrence in claimant calc that are not in the claimant dim table = ' + convert(varchar,cnt) ,'CLAIMANT_DIM',cnt 
	from (
	
	
	SELECT count(distinct claim_party_occurrence_ak_id) cnt FROM rpt_edm.dbo.claimant_calculation a
	where  
	not exists (
	SELECT 1 from dbo.claimant_dim b
	where a.claim_party_occurrence_ak_id = b.edw_claim_party_occurrence_ak_id)
	
	)as cnt
	
	
	union
	
	
	select  'Count of party occurrence in claimant reserve calc that are not in the claimant dim table = ' + convert(varchar,cnt) ,'CLAIMANT_DIM',cnt 
	from (
	
	
	SELECT count(distinct claim_party_occurrence_ak_id) cnt FROM rpt_edm.dbo.claimant_reserve_calculation a
	where  
	not exists (
	SELECT 1 from dbo.claimant_dim b
	where a.claim_party_occurrence_ak_id = b.edw_claim_party_occurrence_ak_id)
	
	)as cnt
	
	
	union
	
	select  'Count of party occurrence in claim party occurrence that are not in the claimant dim table for a specific date/time = ' + convert(varchar,cnt) ,'CLAIMANT_DIM',cnt 
	from (
	
	SELECT count(distinct claim_party_occurrence_ak_id) cnt FROM RPT_EDM.DBO.claim_party_occurrence A
	WHERE  
	NOT EXISTS
	(
	        SELECT 1 FROM DBO.CLAIMANT_DIM Z
	        WHERE
	        A.claim_party_occurrence_ak_id = Z.edw_claim_party_occurrence_ak_id AND
	        A.EFF_FROM_DATE between Z.EFF_FROM_DATE and Z.EFF_TO_DATE
	)
	
	)as cnt
	
	
	union
	
	
	select  'Count of party occurrence in claimant calc that are not in the claimant dim table for a specific date/time = ' + convert(varchar,cnt) ,'CLAIMANT_DIM',cnt 
	from (
	
	SELECT count(distinct claim_party_occurrence_ak_id) cnt FROM RPT_EDM.DBO.CLAIMANT_calculation A
	WHERE  
	NOT EXISTS
	(
	        SELECT 1 FROM DBO.CLAIMANT_DIM Z
	        WHERE
	        A.claim_party_occurrence_ak_id = Z.edw_claim_party_occurrence_ak_id AND
	        A.EFF_FROM_DATE between Z.EFF_FROM_DATE and Z.EFF_TO_DATE
	)
	
	)as cnt
	
	
	union
	
	select  'Count of party occurrence in claimant reserve calc that are not in the claimant dim table for a specific date/time = ' + convert(varchar,cnt) ,'CLAIMANT_DIM',cnt 
	from (
	
	SELECT count(distinct claim_party_occurrence_ak_id) cnt FROM RPT_EDM.DBO.CLAIMANT_reserve_calculation A
	WHERE  
	NOT EXISTS
	(
	        SELECT 1 FROM DBO.CLAIMANT_DIM Z
	        WHERE
	        A.claim_party_occurrence_ak_id = Z.edw_claim_party_occurrence_ak_id AND
	        A.EFF_FROM_DATE between Z.EFF_FROM_DATE and Z.EFF_TO_DATE
	)
	
	)as cnt
	
	union
	
	select  'Count of party occurrence in claimant dim where eff_to_date < eff_from_date = ' + convert(varchar,cnt) ,'CLAIMANT_DIM',cnt 
	from (
	
	SELECT COUNT(DISTINCT edw_claim_party_occurrence_ak_id) CNT FROM claimant_dim WHERE  EFF_TO_DATE < EFF_FROM_DATE
	
	)as cnt
	
	union
	
	select  'Count of party occurrence in claimant dim with more than one current row = ' + convert(varchar,count(*)) ,'CLAIMANT_DIM',count(*)
	from (
	
	
	SELECT edw_claim_party_occurrence_ak_id FROM claimant_dim
	WHERE  CRRNT_SNPSHT_FLAG = 1 
	GROUP BY edw_claim_party_occurrence_ak_id HAVING COUNT(*) > 1 
	
	)as cnt
	
	union
	
	
	select  'Count of party occurrence in claimant dim without a current row  = ' + convert(varchar,count(*)) ,'CLAIMANT_DIM',count(*)
	from (
	
	
	SELECT edw_claim_party_occurrence_ak_id FROM claimant_dim 
	GROUP BY edw_claim_party_occurrence_ak_id HAVING max(CRRNT_SNPSHT_FLAG) <> 1 
	
	)as cnt
	
	union
	
	
	select  'Count of party occurrence in claimant dim with an incorrect current row = ' + convert(varchar,cnt) ,'CLAIMANT_DIM',cnt
	from (
	
	SELECT COUNT(DISTINCT edw_claim_party_occurrence_ak_id) CNT FROM claimant_dim WHERE CRRNT_SNPSHT_FLAG = 1 AND EFF_TO_DATE <> '2100-12-31 23:59:59.000'
	
	)as cnt
),
EXP_CLMNT_DIM AS (
	SELECT
	checkout_message AS check_out_message,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	target_name,
	target_count,
	@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID} AS WBMI_SESSION_CONTROL_RUN_ID,
	'InformS' AS User_ID,
	0 AS Default_Amt,
	'E' AS checkout_type
	FROM SQ_wbmi_checkout_Clmt_Dim
),
FIL_ZERO_COUNTS6 AS (
	SELECT
	check_out_message, 
	created_date, 
	modified_date, 
	target_name, 
	target_count, 
	WBMI_SESSION_CONTROL_RUN_ID, 
	User_ID, 
	Default_Amt, 
	checkout_type
	FROM EXP_CLMNT_DIM
	WHERE target_count > 0
),
UPD_CHECKOUT_TABLE_INS6 AS (
	SELECT
	check_out_message, 
	created_date, 
	modified_date, 
	target_name, 
	target_count, 
	WBMI_SESSION_CONTROL_RUN_ID, 
	User_ID, 
	Default_Amt, 
	checkout_type
	FROM FIL_ZERO_COUNTS6
),
wbmi_checkout_clmt_dim AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, target_name, target_count, created_user_id, created_date, modified_user_id, modified_date)
	SELECT 
	WBMI_SESSION_CONTROL_RUN_ID AS WBMI_SESSION_CONTROL_RUN_ID, 
	checkout_type AS CHECKOUT_TYPE_CODE, 
	check_out_message AS CHECKOUT_MESSAGE, 
	TARGET_NAME, 
	Default_Amt AS TARGET_COUNT, 
	User_ID AS CREATED_USER_ID, 
	CREATED_DATE, 
	User_ID AS MODIFIED_USER_ID, 
	MODIFIED_DATE
	FROM UPD_CHECKOUT_TABLE_INS6
),
SQ_wbmi_checkout_policy AS (
	SELECT  'Count of policies in policy dim where eff_to_date < eff_from_date = ' + convert(varchar,cnt) , 'POLICY_KEY',cnt
	from (
	SELECT COUNT(DISTINCT edw_pol_ak_id) CNT FROM policy_dim WHERE  EFF_TO_DATE < EFF_FROM_DATE
	)as cnt
	
	UNION
	
	SELECT  'Count of policies in policy dim with more than one current row = ' + convert(varchar,count(*)) , 'POLICY_KEY',count(*)
	from (
	SELECT edw_pol_ak_id  FROM policy_dim
	WHERE  CRRNT_SNPSHT_FLAG = 1 
	GROUP BY edw_pol_ak_id HAVING COUNT(*) > 1 
	)as cnt
	
	UNION
	
	SELECT  'Count of policies in pol key dim without a current row  = ' + convert(varchar,count(*)), 'POLICY_KEY',count(*)
	from (
	--SELECT edw_pol_ak_Id FROM policy_dim
	--GROUP BY edw_pol_ak_Id HAVING max(CRRNT_SNPSHT_FLAG) <> 1 
	select edw_pol_ak_Id FROM policy_dim where edw_pol_ak_Id NOT IN
	(SELECT edw_pol_ak_Id FROM policy_dim where CRRNT_SNPSHT_FLAG = 1)
	
	)as cnt
	
	UNION
	
	SELECT  'Count of policies in pol key dim with an incorrect current row = ' + convert(varchar,cnt) ,'POLICY_KEY',cnt
	from (
	SELECT COUNT(DISTINCT edw_pol_ak_id) CNT FROM policy_dim WHERE CRRNT_SNPSHT_FLAG = 1 AND EFF_TO_DATE <> '2100-12-31 23:59:59.000'
	)as cnt
	
	UNION
	
	SELECT  'Count of policies in pol key EDW that are not in pol key dim table = ' + convert(varchar,cnt), 'POLICY_KEY',cnt
	from (
	SELECT COUNT(DISTINCT pol_ak_Id)  cnt  FROM RPT_EDM.V2.policy a
	where 
	not exists (
	SELECT 1 from policy_dim b
	where a.pol_ak_Id = b.edw_pol_ak_Id)
	)as cnt
	
	UNION
	
	SELECT  'Count of agencies in agency key dim with more than one current row for an eff_from_date = ' + convert(varchar,count(*)) , 'AGENCY_DIM', count(*) 
	from (
	SELECT edw_agency_ak_id  FROM v2.agency_dim
	WHERE  CRRNT_SNPSHT_FLAG = 1 
	GROUP BY edw_agency_ak_id HAVING COUNT(distinct eff_From_date) > 1 
	)as cnt
),
EXP_POL_kEY_DIM AS (
	SELECT
	checkout_message AS check_out_message,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	target_name,
	target_count,
	@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID} AS WBMI_SESSION_CONTROL_RUN_ID,
	'InformS' AS User_ID,
	0 AS Default_Amt,
	'E' AS checkout_type
	FROM SQ_wbmi_checkout_policy
),
FIL_ZERO_COUNTS2 AS (
	SELECT
	check_out_message, 
	created_date, 
	modified_date, 
	target_name, 
	target_count, 
	WBMI_SESSION_CONTROL_RUN_ID, 
	User_ID, 
	Default_Amt, 
	checkout_type
	FROM EXP_POL_kEY_DIM
	WHERE target_count > 0
),
UPD_CHECKOUT_TABLE_INS2 AS (
	SELECT
	check_out_message, 
	created_date, 
	modified_date, 
	target_name, 
	target_count, 
	WBMI_SESSION_CONTROL_RUN_ID, 
	User_ID, 
	Default_Amt, 
	checkout_type
	FROM FIL_ZERO_COUNTS2
),
wbmi_checkout_pol_key_dim AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, target_name, target_count, created_user_id, created_date, modified_user_id, modified_date)
	SELECT 
	WBMI_SESSION_CONTROL_RUN_ID AS WBMI_SESSION_CONTROL_RUN_ID, 
	checkout_type AS CHECKOUT_TYPE_CODE, 
	check_out_message AS CHECKOUT_MESSAGE, 
	TARGET_NAME, 
	Default_Amt AS TARGET_COUNT, 
	User_ID AS CREATED_USER_ID, 
	CREATED_DATE, 
	User_ID AS MODIFIED_USER_ID, 
	MODIFIED_DATE
	FROM UPD_CHECKOUT_TABLE_INS2
),
SQ_wbmi_checkout_agency_key_dim AS (
	select  'Count of policies in policy dim where eff_to_date < eff_from_date = ' + convert(varchar,cnt) , 'POLICY',cnt
	from (
	
	SELECT COUNT(DISTINCT edw_pol_ak_id) CNT FROM policy_dim WHERE  EFF_TO_DATE < EFF_FROM_DATE
	
	)as cnt
	
	union
	
	select  'Count of agencies in agency key dim with more than one current row for an eff_from_date = ' + convert(varchar,count(*)) , 'AGENCY_DIM', count(*) 
	from (
	
	
	SELECT edw_agency_ak_id  FROM v2.agency_dim
	WHERE  CRRNT_SNPSHT_FLAG = 1 
	GROUP BY edw_agency_ak_id HAVING COUNT(distinct eff_From_date) > 1 
	
	)as cnt
	
	union
	
	
	select  'Count of policies in pol dim without a current row  = ' + convert(varchar,count(*)), 'POLICY',count(*)
	from (
	
	--SELECT edw_pol_ak_Id FROM policy_dim
	--GROUP BY edw_pol_ak_Id HAVING max(CRRNT_SNPSHT_FLAG) <> 1 
	select edw_pol_ak_Id FROM policy_dim where edw_pol_ak_Id NOT IN
	(SELECT edw_pol_ak_Id FROM policy_dim where CRRNT_SNPSHT_FLAG = 1)
	
	
	)as cnt
	
	union
	
	
	select  'Count of policies in pol dim with an incorrect current row = ' + convert(varchar,cnt) ,'POLICY',cnt
	from (
	
	SELECT COUNT(DISTINCT edw_pol_ak_id) CNT FROM policy_dim WHERE CRRNT_SNPSHT_FLAG = 1 AND EFF_TO_DATE <> '2100-12-31 23:59:59.000'
	
	)as cnt
	
	union
	
	select  'Count of policies in pol EDW that are not in pol dim table = ' + convert(varchar,cnt), 'POLICY',cnt
	from (
	
	SELECT COUNT(DISTINCT pol_ak_Id)  cnt  FROM RPT_EDM.V2.policy a
	where 
	not exists (
	SELECT 1 from policy_dim b
	where a.pol_ak_Id = b.edw_pol_ak_Id)
	
	)as cnt
),
EXP_AGENCY_KEY_DIM AS (
	SELECT
	checkout_message AS check_out_message,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	target_name,
	target_count,
	@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID} AS WBMI_SESSION_CONTROL_RUN_ID,
	'InformS' AS User_ID,
	0 AS Default_Amt,
	'E' AS checkout_type
	FROM SQ_wbmi_checkout_agency_key_dim
),
FIL_ZERO_COUNTS1 AS (
	SELECT
	check_out_message, 
	created_date, 
	modified_date, 
	target_name, 
	target_count, 
	WBMI_SESSION_CONTROL_RUN_ID, 
	User_ID, 
	Default_Amt, 
	checkout_type
	FROM EXP_AGENCY_KEY_DIM
	WHERE target_count > 0
),
UPD_CHECKOUT_TABLE_INS1 AS (
	SELECT
	check_out_message, 
	created_date, 
	modified_date, 
	target_name, 
	target_count, 
	WBMI_SESSION_CONTROL_RUN_ID, 
	User_ID, 
	Default_Amt, 
	checkout_type
	FROM FIL_ZERO_COUNTS1
),
wbmi_checkout_agency_key_dim AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, target_name, target_count, created_user_id, created_date, modified_user_id, modified_date)
	SELECT 
	WBMI_SESSION_CONTROL_RUN_ID AS WBMI_SESSION_CONTROL_RUN_ID, 
	checkout_type AS CHECKOUT_TYPE_CODE, 
	check_out_message AS CHECKOUT_MESSAGE, 
	TARGET_NAME, 
	Default_Amt AS TARGET_COUNT, 
	User_ID AS CREATED_USER_ID, 
	CREATED_DATE, 
	User_ID AS MODIFIED_USER_ID, 
	MODIFIED_DATE
	FROM UPD_CHECKOUT_TABLE_INS1
),
SQ_wbmi_checkout_claim_rep_dim AS (
	SELECT  'Count of claims in claim rep occurrence that are not in the claim occurrence dim table for prim claim rep = ' + convert(varchar,cnt) ,
	'Claim_Occurrence_Dim',
	cnt
	from (
	SELECT COUNT(DISTINCT CLAIM_OCCURRENCE_AK_ID) cnt FROM rpt_edm.dbo.claim_representative_occurrence a
	where a.claim_rep_role_code = 'H' and 
	not exists (
	SELECT 1 from dbo.claim_occurrence_dim b
	where a.claim_rep_occurrence_id = b.edw_claim_rep_occurrence_pk_id_prim_claim_rep)
	)as cnt
	
	UNION
	
	SELECT  'Count of claims in claim rep occurrence that are not in the claim occurrence dim table for examiner = ' + convert(varchar,cnt) ,
	'Claim_Occurrence_Dim',
	cnt
	from (
	SELECT COUNT(DISTINCT CLAIM_OCCURRENCE_AK_ID) cnt FROM rpt_edm.dbo.claim_representative_occurrence a
	where a.claim_rep_role_code = 'E' and 
	not exists (
	SELECT 1 from dbo.claim_occurrence_dim b
	where a.claim_rep_occurrence_id = b.edw_claim_rep_occurrence_pk_id_examiner)
	)as cnt
),
EXP_CLAIM_REP_DIM AS (
	SELECT
	checkout_message AS check_out_message,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	target_name,
	target_count,
	@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID} AS WBMI_SESSION_CONTROL_RUN_ID,
	'InformS' AS User_ID,
	0 AS Default_Amt,
	'W' AS checkout_type
	FROM SQ_wbmi_checkout_claim_rep_dim
),
FIL_ZERO_COUNTS AS (
	SELECT
	check_out_message, 
	created_date, 
	modified_date, 
	target_name, 
	target_count, 
	WBMI_SESSION_CONTROL_RUN_ID, 
	User_ID, 
	Default_Amt, 
	checkout_type
	FROM EXP_CLAIM_REP_DIM
	WHERE target_count > 0
),
UPD_CHECKOUT_TABLE_INS AS (
	SELECT
	check_out_message, 
	created_date, 
	modified_date, 
	target_name, 
	target_count, 
	WBMI_SESSION_CONTROL_RUN_ID, 
	User_ID, 
	Default_Amt, 
	checkout_type
	FROM FIL_ZERO_COUNTS
),
wbmi_checkout_claim_rep_dim AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, target_name, target_count, created_user_id, created_date, modified_user_id, modified_date)
	SELECT 
	WBMI_SESSION_CONTROL_RUN_ID AS WBMI_SESSION_CONTROL_RUN_ID, 
	checkout_type AS CHECKOUT_TYPE_CODE, 
	check_out_message AS CHECKOUT_MESSAGE, 
	TARGET_NAME, 
	TARGET_COUNT, 
	User_ID AS CREATED_USER_ID, 
	CREATED_DATE, 
	User_ID AS MODIFIED_USER_ID, 
	MODIFIED_DATE
	FROM UPD_CHECKOUT_TABLE_INS
),
SQ_wbmi_checkout_fact_tables AS (
	SELECT 'Count of Claim_Loss_Trans_fact_ids from Claim_Loss_Transaction_Fact where err_flag is not 000000000000000 = ' + convert(varchar,count(*)) as check_out_message, 'Claim_Loss_Transaction_Fact' as target_name, count(*) as target_count
	FROM dbo.claim_loss_transaction_fact
	             WHERE  (err_flag <> '000000000000000') and audit_id >0
	
	UNION
	
	SELECT 'Count of EDW_Claim_trans_pk_id from Claim_Loss_Transaction_Fact with claimant_cov_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Loss_Transaction_Fact' as target_name, count(*) as target_count
	FROM (select edw_claim_trans_pk_id from claim_loss_transaction_fact where claimant_cov_dim_id = -1 and audit_id > 0)CLTF
	
	UNION
	
	SELECT 'Count of EDW_Claim_trans_pk_id from Claim_Loss_Transaction_Fact with claimant_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Loss_Transaction_Fact' as target_name, count(*) as target_count
	FROM (select edw_claim_trans_pk_id from claim_loss_transaction_fact where claimant_dim_id = -1 and audit_id > 0)CLTF
	
	UNION
	
	SELECT 'Count of EDW_Claim_trans_pk_id from Claim_Loss_Transaction_Fact with claim_occurrence_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Loss_Transaction_Fact' as target_name, count(*) as target_count
	FROM (select edw_claim_trans_pk_id from claim_loss_transaction_fact where claim_occurrence_dim_id = -1 and audit_id > 0)CLTF
	
	UNION
	
	SELECT 'Count of EDW_Claim_trans_pk_id from Claim_Loss_Transaction_Fact with pol_key_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Loss_Transaction_Fact' as target_name, count(*) as target_count
	FROM (select edw_claim_trans_pk_id from claim_loss_transaction_fact where pol_dim_id = -1 and audit_id > 0)CLTF
	
	UNION
	
	SELECT 'Count of EDW_Claim_trans_pk_id from Claim_Loss_Transaction_Fact with agency_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Loss_Transaction_Fact' as target_name, count(*) as target_count
	FROM (select edw_claim_trans_pk_id from claim_loss_transaction_fact where agency_dim_id = -1 and audit_id > 0)CLTF
	
	UNION
	
	SELECT 'Count of EDW_Claim_trans_pk_id from Claim_Loss_Transaction_Fact with claim_rep_dim_prim_claim_rep_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Loss_Transaction_Fact' as target_name, count(*) as target_count
	FROM (select edw_claim_trans_pk_id from claim_loss_transaction_fact where claim_rep_dim_prim_claim_rep_id = -1 and audit_id > 0)CLTF
	
	UNION
	
	SELECT 'Count of edw_claim_pay_ctgry_pk_id from Claim_Payment_Category_Fact with claim_rep_dim_prim_claim_rep_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Payment_Category_Fact' as target_name,count(*) as target_count
	FROM (select edw_claim_pay_ctgry_pk_id from Claim_Payment_Category_Fact where claim_rep_dim_prim_claim_rep_id = -1)CPCF
	
	UNION
	
	-- check that manually created ceded mp 50 records match the direct mp 50s 
	SELECT  'Difference between manually created ceded MP 50 records and direct MP 50 records in Claim Loss Transaction Fact ' + convert(varchar,count(*)) as check_out_message, 'Claim_Loss_Transaction_Fact' as target_name, count(*) as target_count
	From
	claim_loss_transaction_fact CLTF where CLTF.audit_id = '-50'
	-
	(select COUNT(*)
	FROM   dbo.claim_loss_transaction_fact CLTF , 
	RPT_EDM.dbo.VW_claim_transaction CT , 
	RPT_EDM.dbo.claimant_coverage_detail CCD
	WHERE 
	CLTF.edw_claim_trans_pk_id = CT.claim_trans_id
	AND 
	CT.claimant_cov_det_ak_id = CCD.claimant_cov_det_ak_id
	AND 
	CCD.major_peril_code = '050' AND CLTF.audit_id >0)
	
	UNION
	
	SELECT 'Count of EDW_Claim_trans_pk_id from Claim_Loss_Transaction_Fact with claim_trans_type_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Loss_Transaction_Fact' as target_name, count(*) as target_count
	FROM (select edw_claim_trans_pk_id from claim_loss_transaction_fact where claim_trans_type_dim_id = -1 and audit_id > 0)CLTF
	
	UNION
	
	SELECT 'Count of EDW_Claim_trans_pk_id from Claim_Loss_Transaction_Fact with claim_financial_type_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Loss_Transaction_Fact' as target_name, count(*) as target_count
	FROM (select edw_claim_trans_pk_id from claim_loss_transaction_fact where claim_financial_type_dim_id = -1 and audit_id > 0)CLTF
	
	UNION 
	
	SELECT 'Count of EDW_Claim_trans_pk_id from Claim_Loss_Transaction_Fact with contract_cust_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Loss_Transaction_Fact' as target_name, count(*) as target_count
	FROM (select edw_claim_trans_pk_id from claim_loss_transaction_fact where contract_cust_dim_id = -1 and audit_id > 0)CLTF
	
	UNION
	
	SELECT 'Count of EDW_Claim_trans_pk_id from Claim_Loss_Transaction_Fact with claim_loss_date_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Loss_Transaction_Fact' as target_name, count(*) as target_count
	FROM (select edw_claim_trans_pk_id from claim_loss_transaction_fact where claim_loss_date_id = -1 and audit_id > 0)CLTF
	
	UNION
	
	SELECT 'Count of EDW_Claim_trans_pk_id from Claim_Loss_Transaction_Fact with claim_trans_date_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Loss_Transaction_Fact' as target_name, count(*) as target_count
	FROM (select edw_claim_trans_pk_id from claim_loss_transaction_fact where claim_trans_date_id = -1 and audit_id > 0)CLTF
	
	UNION
	
	SELECT 'Count of EDW_Claim_trans_pk_id from Claim_Loss_Transaction_Fact with pol_eff_date_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Loss_Transaction_Fact' as target_name, count(*) as target_count
	FROM (select edw_claim_trans_pk_id from claim_loss_transaction_fact where pol_eff_date_id = -1 and audit_id > 0)CLTF
	
	UNION
	
	SELECT 'Count of EDW_Claim_trans_pk_id from Claim_Loss_Transaction_Fact with pol_exp_date_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Loss_Transaction_Fact' as target_name, count(*) as target_count
	FROM (select edw_claim_trans_pk_id from claim_loss_transaction_fact where pol_exp_date_id = -1 and audit_id > 0)CLTF
	
	UNION
	
	SELECT 'Count of EDW_Claim_trans_pk_id from Claim_Loss_Transaction_Fact with asl_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Loss_Transaction_Fact' as target_name, count(*) as target_count
	FROM (select edw_claim_trans_pk_id from claim_loss_transaction_fact where asl_dim_id = -1 and audit_id > 0)CLTF
	
	UNION
	
	SELECT 'Count of EDW_Claim_trans_pk_id from Claim_Loss_Transaction_Fact with asl_prdct_code_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Loss_Transaction_Fact' as target_name, count(*) as target_count
	FROM (select edw_claim_trans_pk_id from claim_loss_transaction_fact where asl_prdct_code_dim_id = -1 and audit_id > 0)CLTF
	
	UNION
	
	SELECT 'Count of EDW_Claim_trans_pk_id from Claim_Loss_Transaction_Fact with loss_master_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Loss_Transaction_Fact' as target_name, count(*) as target_count
	FROM (select edw_claim_trans_pk_id from claim_loss_transaction_fact where loss_master_dim_id = -1 and audit_id > 0)CLTF
	
	UNION
	
	SELECT 'Count of EDW_Claim_trans_pk_id from Claim_Loss_Transaction_Fact with strtgc_bus_dvsn_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Loss_Transaction_Fact' as target_name, count(*) as target_count
	FROM (select edw_claim_trans_pk_id from claim_loss_transaction_fact where strtgc_bus_dvsn_dim_id = -1 and audit_id > 0)CLTF
	
	UNION
	
	SELECT 'Count of EDW_Claim_trans_pk_id from Claim_Loss_Transaction_Fact with prdct_code_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Loss_Transaction_Fact' as target_name, count(*) as target_count
	FROM (select edw_claim_trans_pk_id from claim_loss_transaction_fact where prdct_code_dim_id = -1 and audit_id > 0)CLTF
	
	UNION
	
	SELECT 'Count of edw_claim_pay_ctgry_pk_id from Claim_Payment_Category_Fact with claim_financial_type_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Payment_Category_Fact' as target_name, count(*) as target_count
	FROM (select edw_claim_pay_ctgry_pk_id from Claim_Payment_Category_Fact where claim_financial_type_dim_id = -1 )CPCF
	
	UNION 
	
	SELECT 'Count of edw_claim_pay_ctgry_pk_id from Claim_Payment_Category_Fact with contract_cust_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Payment_Category_Fact' as target_name, count(*) as target_count
	FROM (select edw_claim_pay_ctgry_pk_id from Claim_Payment_Category_Fact where contract_cust_dim_id = -1 )CPCF
	
	UNION
	
	SELECT 'Count of edw_claim_pay_ctgry_pk_id from Claim_Payment_Category_Fact with claim_loss_date_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Payment_Category_Fact' as target_name, count(*) as target_count
	FROM (select edw_claim_pay_ctgry_pk_id from Claim_Payment_Category_Fact where claim_loss_date_id = -1 )CPCF
	
	UNION
	
	SELECT 'Count of edw_claim_pay_ctgry_pk_id from Claim_Payment_Category_Fact with pol_eff_date_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Payment_Category_Fact' as target_name, count(*) as target_count
	FROM (select edw_claim_pay_ctgry_pk_id from Claim_Payment_Category_Fact where pol_eff_date_id = -1 )CPCF
	
	UNION
	
	SELECT 'Count of edw_claim_pay_ctgry_pk_id from Claim_Payment_Category_Fact with pol_exp_date_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Payment_Category_Fact' as target_name, count(*) as target_count
	FROM (select edw_claim_pay_ctgry_pk_id from Claim_Payment_Category_Fact where pol_exp_date_id = -1 )CPCF
	
	UNION
	
	SELECT 'Count of edw_claim_pay_ctgry_pk_id from Claim_Payment_Category_Fact with strtgc_bus_dvsn_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Payment_Category_Fact' as target_name, count(*) as target_count
	FROM (select edw_claim_pay_ctgry_pk_id from Claim_Payment_Category_Fact where strtgc_bus_dvsn_dim_id = -1 )CPCF
	
	UNION
	
	SELECT 'Count of edw_claim_pay_ctgry_pk_id from Claim_Payment_Category_Fact with claimant_cov_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Payment_Category_Fact' as target_name,count(*) as target_count
	FROM (select edw_claim_pay_ctgry_pk_id from Claim_Payment_Category_Fact where claimant_cov_dim_id = -1)CPCF
	
	UNION
	
	SELECT 'Count of edw_claim_pay_ctgry_pk_id from Claim_Payment_Category_Fact with claimant_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Payment_Category_Fact' as target_name,count(*) as target_count
	FROM (select edw_claim_pay_ctgry_pk_id from Claim_Payment_Category_Fact where claimant_dim_id = -1)CPCF
	
	UNION
	
	SELECT 'Count of edw_claim_pay_ctgry_pk_id from Claim_Payment_Category_Fact with claim_occurrence_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Payment_Category_Fact' as target_name,count(*) as target_count
	FROM (select edw_claim_pay_ctgry_pk_id from Claim_Payment_Category_Fact where claim_occurrence_dim_id = -1)CPCF
	
	UNION
	
	SELECT 'Count of edw_claim_pay_ctgry_pk_id from Claim_Payment_Category_Fact with pol_key_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Payment_Category_Fact' as target_name,count(*) as target_count
	FROM (select edw_claim_pay_ctgry_pk_id from Claim_Payment_Category_Fact where pol_dim_id = -1)CPCF
	
	UNION
	
	SELECT 'Count of edw_claim_pay_ctgry_pk_id from Claim_Payment_Category_Fact with agency_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Payment_Category_Fact' as target_name,count(*) as target_count
	FROM (select edw_claim_pay_ctgry_pk_id from Claim_Payment_Category_Fact where agency_dim_id = -1)CPCF
	
	UNION
	
	SELECT 'Count of med_bill_dim_id from Medical_Bill_Fact with claimant_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Medical_Bill_Fact' as target_name, count(*) as target_count
	FROM (select med_bill_dim_id from Medical_Bill_Fact where claimant_dim_id = -1 )MBF
	
	UNION
	
	SELECT 'Count of med_bill_dim_id from Medical_Bill_Fact with claim_occurrence_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Medical_Bill_Fact' as target_name, count(*) as target_count
	FROM (select med_bill_dim_id from Medical_Bill_Fact where claim_occurrence_dim_id = -1 )MBF
	
	UNION
	
	SELECT 'Count of med_bill_dim_id from Medical_Bill_Fact with pol_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Medical_Bill_Fact' as target_name, count(*) as target_count
	FROM (select med_bill_dim_id from Medical_Bill_Fact where pol_dim_id = -1 )MBF
	
	UNION
	
	SELECT 'Count of med_bill_dim_id from Medical_Bill_Fact with agency_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Medical_Bill_Fact' as target_name, count(*) as target_count
	FROM (select med_bill_dim_id from Medical_Bill_Fact where agency_dim_id = -1 )MBF
	
	UNION
	
	SELECT 'Count of med_bill_dim_id from Medical_Bill_Fact with claim_rep_dim_prim_claim_rep_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Medical_Bill_Fact' as target_name, count(*) as target_count
	FROM (select med_bill_dim_id from Medical_Bill_Fact where claim_rep_dim_prim_claim_rep_id = -1 )MBF
	
	UNION 
	
	SELECT 'Count of med_bill_dim_id from Medical_Bill_Fact with contract_cust_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Medical_Bill_Fact' as target_name, count(*) as target_count
	FROM (select med_bill_dim_id from Medical_Bill_Fact where contract_cust_dim_id = -1 )MBF
	
	UNION
	
	SELECT 'Count of med_bill_dim_id from Medical_Bill_Fact with claim_loss_date_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Medical_Bill_Fact' as target_name, count(*) as target_count
	FROM (select med_bill_dim_id from Medical_Bill_Fact where claim_loss_date_id = -1 )MBF
	
	UNION
	
	SELECT 'Count of med_bill_dim_id from Medical_Bill_Fact with pol_eff_date_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Medical_Bill_Fact' as target_name, count(*) as target_count
	FROM (select med_bill_dim_id from Medical_Bill_Fact where pol_eff_date_id = -1 )MBF
	
	UNION
	
	SELECT 'Count of med_bill_dim_id from Medical_Bill_Fact with pol_exp_date_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Medical_Bill_Fact' as target_name, count(*) as target_count
	FROM (select med_bill_dim_id from Medical_Bill_Fact where pol_exp_date_id = -1 )MBF
	
	UNION
	
	SELECT 'Count of med_bill_dim_id from Medical_Bill_Fact with strtgc_bus_dvsn_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Medical_Bill_Fact' as target_name, count(*) as target_count
	FROM (select med_bill_dim_id from Medical_Bill_Fact where strtgc_bus_dvsn_dim_id = -1 )MBF
	
	UNION
	
	SELECT 'Count of med_bill_serv_fact_id from Medical_Bill_Service_Fact with claimant_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Medical_Bill_Service_Fact' as target_name, count(*) as target_count
	FROM (select med_bill_serv_fact_id from Medical_Bill_Service_Fact where claimant_dim_id = -1 )MBSF
	
	UNION
	
	SELECT 'Count of med_bill_serv_fact_id from Medical_Bill_Service_Fact with claim_occurrence_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Medical_Bill_Service_Fact' as target_name, count(*) as target_count
	FROM (select med_bill_serv_fact_id from Medical_Bill_Service_Fact where claim_occurrence_dim_id = -1 )MBSF
	
	UNION
	
	SELECT 'Count of med_bill_serv_fact_id from Medical_Bill_Service_Fact with pol_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Medical_Bill_Service_Fact' as target_name, count(*) as target_count
	FROM (select med_bill_serv_fact_id from Medical_Bill_Service_Fact where pol_dim_id = -1 )MBSF
	
	UNION
	
	SELECT 'Count of med_bill_serv_fact_id from Medical_Bill_Service_Fact with agency_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Medical_Bill_Service_Fact' as target_name, count(*) as target_count
	FROM (select med_bill_serv_fact_id from Medical_Bill_Service_Fact where agency_dim_id = -1 )MBSF
	
	UNION
	
	SELECT 'Count of med_bill_serv_fact_id from Medical_Bill_Service_Fact with claim_rep_dim_prim_claim_rep_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Medical_Bill_Service_Fact' as target_name, count(*) as target_count
	FROM (select med_bill_serv_fact_id from Medical_Bill_Service_Fact where claim_rep_dim_prim_claim_rep_id = -1 )MBSF
	
	UNION 
	
	SELECT 'Count of med_bill_serv_fact_id from Medical_Bill_Service_Fact with contract_cust_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Medical_Bill_Service_Fact' as target_name, count(*) as target_count
	FROM (select med_bill_serv_fact_id from Medical_Bill_Service_Fact where contract_cust_dim_id = -1 )MBSF
	
	UNION
	
	SELECT 'Count of med_bill_serv_fact_id from Medical_Bill_Service_Fact with claim_loss_date_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Medical_Bill_Service_Fact' as target_name, count(*) as target_count
	FROM (select med_bill_serv_fact_id from Medical_Bill_Service_Fact where claim_loss_date_id = -1 )MBSF
	
	UNION
	
	SELECT 'Count of med_bill_serv_fact_id from Medical_Bill_Service_Fact with pol_eff_date_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Medical_Bill_Service_Fact' as target_name, count(*) as target_count
	FROM (select med_bill_serv_fact_id from Medical_Bill_Service_Fact where pol_eff_date_id = -1 )MBSF
	
	UNION
	
	SELECT 'Count of med_bill_serv_fact_id from Medical_Bill_Service_Fact with pol_exp_date_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Medical_Bill_Service_Fact' as target_name, count(*) as target_count
	FROM (select med_bill_serv_fact_id from Medical_Bill_Service_Fact where pol_exp_date_id = -1 )MBSF
	
	UNION
	
	SELECT 'Count of med_bill_serv_fact_id from Medical_Bill_Service_Fact with strtgc_bus_dvsn_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Medical_Bill_Service_Fact' as target_name, count(*) as target_count
	FROM (select med_bill_serv_fact_id from Medical_Bill_Service_Fact where strtgc_bus_dvsn_dim_id = -1 )MBSF
),
EXP_CLAIM_FACT_TABLES AS (
	SELECT
	checkout_message AS check_out_message,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	target_name,
	target_count,
	@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID} AS WBMI_SESSION_CONTROL_RUN_ID,
	'InformS' AS User_ID,
	0 AS Default_Amt,
	'E' AS checkout_type
	FROM SQ_wbmi_checkout_fact_tables
),
FIL_ZERO_COUNTS5 AS (
	SELECT
	check_out_message, 
	created_date, 
	modified_date, 
	target_name, 
	target_count, 
	WBMI_SESSION_CONTROL_RUN_ID, 
	User_ID, 
	Default_Amt, 
	checkout_type
	FROM EXP_CLAIM_FACT_TABLES
	WHERE target_count > 0
),
UPD_CHECKOUT_TABLE_INS5 AS (
	SELECT
	check_out_message, 
	created_date, 
	modified_date, 
	target_name, 
	target_count, 
	WBMI_SESSION_CONTROL_RUN_ID, 
	User_ID, 
	Default_Amt, 
	checkout_type
	FROM FIL_ZERO_COUNTS5
),
wbmi_checkout_claim_fact_tables AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, target_name, target_count, created_user_id, created_date, modified_user_id, modified_date)
	SELECT 
	WBMI_SESSION_CONTROL_RUN_ID AS WBMI_SESSION_CONTROL_RUN_ID, 
	checkout_type AS CHECKOUT_TYPE_CODE, 
	check_out_message AS CHECKOUT_MESSAGE, 
	TARGET_NAME, 
	Default_Amt AS TARGET_COUNT, 
	User_ID AS CREATED_USER_ID, 
	CREATED_DATE, 
	User_ID AS MODIFIED_USER_ID, 
	MODIFIED_DATE
	FROM UPD_CHECKOUT_TABLE_INS5
),
SQ_wbmi_checkout_daily_counts AS (
	SELECT 'Count of asl_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'asl_dim' as target_name, count(*) as target_count
	FROM 
	(select asl_dim_id
	FROM  asl_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') AD
	
	UNION
	
	SELECT 'Count of asl_product_code_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'asl_product_code_dim' as target_name, count(*) as target_count
	FROM  
	(select asl_prdct_code_dim_id From
	asl_product_code_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') APC
	
	UNION
	
	SELECT 'Count of claim_case_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'claim_case_dim' as target_name, count(*) as target_count
	FROM  
	(select claim_case_dim_id FROM
	claim_case_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') CCD
	
	UNION
	
	SELECT 'Count of claim_financial_type_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'claim_financial_type_dim' as target_name, count(*) as target_count
	FROM  
	(select claim_financial_type_dim_id from
	claim_financial_type_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') CFTD
	
	UNION
	
	SELECT 'Count of claim_master_1099_list_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'claim_master_1099_list_dim' as target_name, count(*) as target_count
	FROM  
	(select claim_master_1099_list_dim_id from 
	claim_master_1099_list_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') CM1L
	
	UNION
	
	SELECT 'Count of claim_occurrence_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'claim_occurrence_dim' as target_name, count(*) as target_count
	FROM  
	(select claim_occurrence_dim_id from 
	claim_occurrence_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') COD
	
	UNION
	
	SELECT 'Count of claim_party_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'claim_party_dim' as target_name, count(*) as target_count
	FROM  
	(select claim_party_dim_id from 
	claim_party_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') CPD
	
	UNION
	
	SELECT 'Count of claim_party_role_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'claim_party_role_dim' as target_name, count(*) as target_count
	FROM  
	(select claim_party_role_dim_id from claim_party_role_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') CPRD
	
	UNION
	
	SELECT 'Count of claim_payment_category_type_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'claim_payment_category_type_dim' as target_name, count(*) as target_count
	FROM  
	(select claim_pay_ctgry_type_dim_id from 
	claim_payment_category_type_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') CPCT
	
	UNION
	
	SELECT 'Count of claim_payment_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'claim_payment_dim' as target_name, count(*) as target_count
	FROM  
	(select claim_pay_dim_id from 
	claim_payment_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') CPD
	
	UNION
	
	SELECT 'Count of claim_representative_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'claim_representative_dim' as target_name, count(*) as target_count
	FROM  
	(select claim_rep_dim_id from 
	claim_representative_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') CRD
	
	UNION
	
	SELECT 'Count of claim_subrogation_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'claim_subrogation_dim' as target_name, count(*) as target_count
	FROM  
	(select claim_subrogation_dim_id from 
	claim_subrogation_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') CSD
	
	UNION
	
	SELECT 'Count of claim_total_loss_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'claim_total_loss_dim' as target_name, count(*) as target_count
	FROM  
	(select claim_total_loss_dim_id from 
	claim_total_loss_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') CTLD
	
	UNION
	
	SELECT 'Count of claim_transaction_type_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'claim_transaction_type_dim' as target_name, count(*) as target_count
	FROM  
	(select claim_trans_type_dim_id from 
	claim_transaction_type_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') CTT
	
	UNION
	
	SELECT 'Count of claimant_coverage_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'claimant_coverage_dim' as target_name, count(*) as target_count
	FROM 
	(select claimant_cov_dim_id 
	from  claimant_coverage_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') CCD
	
	UNION
	
	SELECT 'Count of claimant_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'claimant_dim' as target_name, count(*) as target_count
	FROM  
	(select claimant_dim_id from 
	claimant_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') CD
	
	UNION
	
	SELECT 'Count of claims_survey_form_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'claims_survey_form_dim' as target_name, count(*) as target_count
	FROM  
	(select claims_survey_form_dim_id from
	claims_survey_form_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') CSF
	
	UNION
	
	SELECT 'Count of contract_customer_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'contract_customer_dim' as target_name, count(*) as target_count
	FROM 
	(select contract_cust_dim_id from 
	 contract_customer_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') CCD
	
	UNION
	
	SELECT 'Count of coverage_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'coverage_dim' as target_name, count(*) as target_count
	FROM  
	(select cov_dim_id from 
	coverage_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') CD
	
	UNION
	
	SELECT 'Count of loss_master_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'loss_master_dim' as target_name, count(*) as target_count
	FROM  
	(select loss_master_dim_id from 
	loss_master_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') LMD
	
	UNION
	
	SELECT 'Count of medical_bill_code_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'medical_bill_code_dim' as target_name, count(*) as target_count
	FROM  
	(select med_bill_code_dim_id 
	from medical_bill_code_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') MBC
	
	UNION
	
	SELECT 'Count of medical_bill_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'medical_bill_dim' as target_name, count(*) as target_count
	FROM  
	(select med_bill_dim_id 
	from medical_bill_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') MBD
	
	UNION
	
	SELECT 'Count of medical_bill_service_code_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'medical_bill_service_code_dim' as target_name, count(*) as target_count
	FROM  
	(select med_bill_serv_code_dim_id 
	from medical_bill_service_code_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') MBSC
	
	UNION
	
	SELECT 'Count of medical_bill_service_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'medical_bill_service_dim' as target_name, count(*) as target_count
	FROM  
	(select med_bill_serv_dim_id 
	from medical_bill_service_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') MBS
	
	UNION
	
	SELECT 'Count of policy_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'policy_dim' as target_name, count(*) as target_count
	FROM  
	(select pol_dim_id from 
	policy_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') PD
	
	UNION
	
	SELECT 'Count of product_code_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'product_code_dim' as target_name, count(*) as target_count
	FROM  
	(select prdct_code_dim_id from 
	product_code_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') PCD
	
	UNION
	
	SELECT 'Count of reinsurance_coverage_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'reinsurance_coverage_dim' as target_name, count(*) as target_count
	FROM  
	(select reins_cov_dim_id 
	from reinsurance_coverage_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') RCD
	
	UNION
	
	SELECT 'Count of strategic_business_division_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'strategic_business_division_dim' as target_name, count(*) as target_count
	FROM  
	(select strtgc_bus_dvsn_dim_id from 
	strategic_business_division_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') SBDD
	
	UNION
	
	SELECT 'Count of v2.agency_dim inserts = ' + convert(varchar,count(*)) as check_out_message,'v2.agency_dim' as target_name, count(*) as target_count
	FROM  
	(select  agency_dim_id from 
	v2.agency_dim where created_date >= '@{pipeline().parameters.SELECTION_START_TS}') AD
),
EXP_daily_counts AS (
	SELECT
	checkout_message AS check_out_message,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	target_name,
	target_count,
	@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID} AS WBMI_SESSION_CONTROL_RUN_ID,
	'InformS' AS User_ID,
	0 AS Default_Amt,
	'C' AS checkout_type
	FROM SQ_wbmi_checkout_daily_counts
),
FIL_1000_daily_counts AS (
	SELECT
	check_out_message, 
	created_date, 
	modified_date, 
	target_name, 
	target_count, 
	WBMI_SESSION_CONTROL_RUN_ID, 
	User_ID, 
	Default_Amt, 
	checkout_type
	FROM EXP_daily_counts
	WHERE target_count > 1000
),
UPD_CHECKOUT_TABLE_INS_daily_counts AS (
	SELECT
	check_out_message, 
	created_date, 
	modified_date, 
	target_name, 
	target_count, 
	WBMI_SESSION_CONTROL_RUN_ID, 
	User_ID, 
	Default_Amt, 
	checkout_type
	FROM FIL_1000_daily_counts
),
wbmi_checkout_daily_counts AS (
	INSERT INTO wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, target_name, target_count, created_user_id, created_date, modified_user_id, modified_date)
	SELECT 
	WBMI_SESSION_CONTROL_RUN_ID AS WBMI_SESSION_CONTROL_RUN_ID, 
	checkout_type AS CHECKOUT_TYPE_CODE, 
	check_out_message AS CHECKOUT_MESSAGE, 
	TARGET_NAME, 
	TARGET_COUNT, 
	User_ID AS CREATED_USER_ID, 
	CREATED_DATE, 
	User_ID AS MODIFIED_USER_ID, 
	MODIFIED_DATE
	FROM UPD_CHECKOUT_TABLE_INS_daily_counts
),
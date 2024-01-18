WITH
SQ_gtam_wb_region_stage AS (
	SELECT 
	                a.agency_code    as agency_code             
	               ,a.bus_unit_ind as bus_unit_ind          
	               ,a.uw_mgr_name_routing_station          
	                 ,a.uw_mgr_region         
	  FROM  gtam_wb_region_stage a
),
EXP_Default_Values AS (
	SELECT
	agency_code,
	-- *INF*: SUBSTR(agency_code,1,2)
	SUBSTR(agency_code, 1, 2) AS v_agency_state,
	-- *INF*: iif(isnull(v_agency_state),' ',v_agency_state)
	-- 
	-- 
	-- 
	IFF(v_agency_state IS NULL, ' ', v_agency_state) AS agency_state_out,
	-- *INF*: SUBSTR(agency_code,3,3)
	SUBSTR(agency_code, 3, 3) AS v_agency_num,
	-- *INF*: iif(isnull(v_agency_num),'N/A ',v_agency_num)
	-- 
	--  
	IFF(v_agency_num IS NULL, 'N/A ', v_agency_num) AS agency_num_out,
	bus_unit_ind,
	-- *INF*: iif(isnull(bus_unit_ind),' ',bus_unit_ind)
	-- 
	IFF(bus_unit_ind IS NULL, ' ', bus_unit_ind) AS bus_unit_ind_out,
	uw_mgr_name_routing_station,
	-- *INF*: SUBSTR(uw_mgr_name_routing_station,28,3)
	SUBSTR(uw_mgr_name_routing_station, 28, 3) AS v_routing_station,
	-- *INF*: iif(isnull(v_routing_station),'N/A ',v_routing_station)
	IFF(v_routing_station IS NULL, 'N/A ', v_routing_station) AS routing_station_out,
	-- *INF*: SUBSTR(uw_mgr_name_routing_station,1,27)
	SUBSTR(uw_mgr_name_routing_station, 1, 27) AS v_uw_mgr,
	-- *INF*: iif(isnull(v_uw_mgr),'N/A ',v_uw_mgr)
	IFF(v_uw_mgr IS NULL, 'N/A ', v_uw_mgr) AS uw_mgr_out,
	uw_mgr_region,
	-- *INF*: iif(isnull(uw_mgr_region),'N/A',uw_mgr_region)
	-- 
	-- 
	IFF(uw_mgr_region IS NULL, 'N/A', uw_mgr_region) AS uw_mgr_region_out
	FROM SQ_gtam_wb_region_stage
),
LKP_underwriter_manager_region AS (
	SELECT
	sup_uw_mgr_region_id,
	agency_state,
	agency_num,
	bus_unit_ind,
	routing_station,
	uw_mgr,
	uw_mgr_region
	FROM (
		SELECT 
		       a.sup_uw_mgr_region_id as sup_uw_mgr_region_id
		      ,a.agency_state as agency_state
		      ,a.agency_num as agency_num 
		      ,a.bus_unit_ind as bus_unit_ind
		      ,a.routing_station as routing_station
		      ,a.uw_mgr as uw_mgr 
		      ,a.uw_mgr_region as uw_mgr_region 
		  FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_underwriter_manager_region a
		  WHERE a.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY agency_state,agency_num,bus_unit_ind ORDER BY sup_uw_mgr_region_id DESC) = 1
),
EXP_Detect_Changes AS (
	SELECT
	EXP_Default_Values.agency_state_out,
	EXP_Default_Values.agency_num_out,
	EXP_Default_Values.bus_unit_ind_out,
	LKP_underwriter_manager_region.sup_uw_mgr_region_id AS LKP_sup_uw_mgr_region_id,
	LKP_underwriter_manager_region.routing_station AS LKP_rounting_station,
	LKP_underwriter_manager_region.uw_mgr AS LKP_uw_mgr,
	LKP_underwriter_manager_region.uw_mgr_region AS LKP_uw_mgr_region,
	EXP_Default_Values.routing_station_out,
	EXP_Default_Values.uw_mgr_out,
	EXP_Default_Values.uw_mgr_region_out,
	-- *INF*: IIF(ISNULL(LKP_sup_uw_mgr_region_id), 'NEW', IIF(
	-- LTRIM(RTRIM(LKP_rounting_station)) != (LTRIM(RTRIM(routing_station_out)  )  ) 
	-- OR
	-- LTRIM(RTRIM(LKP_uw_mgr_region)) != (LTRIM(RTRIM(uw_mgr_region_out)  )  ) 
	-- OR
	-- LTRIM(RTRIM(LKP_uw_mgr)) != (LTRIM(RTRIM(uw_mgr_out)  )  ) 
	-- 
	--  , 'UPDATE', 'NOCHANGE'))
	IFF(
	    LKP_sup_uw_mgr_region_id IS NULL, 'NEW',
	    IFF(
	        LTRIM(RTRIM(LKP_rounting_station)) != (LTRIM(RTRIM(routing_station_out)))
	        or LTRIM(RTRIM(LKP_uw_mgr_region)) != (LTRIM(RTRIM(uw_mgr_region_out)))
	        or LTRIM(RTRIM(LKP_uw_mgr)) != (LTRIM(RTRIM(uw_mgr_out))),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS V_changed_flag,
	V_changed_flag AS CHANGED_FLAG,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(V_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(
	    V_changed_flag = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id
	FROM EXP_Default_Values
	LEFT JOIN LKP_underwriter_manager_region
	ON LKP_underwriter_manager_region.agency_state = EXP_Default_Values.agency_state_out AND LKP_underwriter_manager_region.agency_num = EXP_Default_Values.agency_num_out AND LKP_underwriter_manager_region.bus_unit_ind = EXP_Default_Values.bus_unit_ind_out
),
FIL_sup_workers_comp_employer_type AS (
	SELECT
	agency_state_out, 
	agency_num_out, 
	bus_unit_ind_out, 
	routing_station_out, 
	uw_mgr_out, 
	uw_mgr_region_out, 
	CHANGED_FLAG, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	created_date, 
	modified_date, 
	source_sys_id
	FROM EXP_Detect_Changes
	WHERE CHANGED_FLAG = 'NEW' or CHANGED_FLAG = 'UPDATE'
),
sup_underwriter_manager_region_INS AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_underwriter_manager_region
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, agency_state, agency_num, bus_unit_ind, routing_station, uw_mgr, uw_mgr_region)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	agency_state_out AS AGENCY_STATE, 
	agency_num_out AS AGENCY_NUM, 
	bus_unit_ind_out AS BUS_UNIT_IND, 
	routing_station_out AS ROUTING_STATION, 
	uw_mgr_out AS UW_MGR, 
	uw_mgr_region_out AS UW_MGR_REGION
	FROM FIL_sup_workers_comp_employer_type
),
SQ_sup_underwriter_manager_region AS (
	SELECT
	     a.sup_uw_mgr_region_id
	  ,a.eff_from_date
	     ,a.eff_to_date 
	     ,a.agency_state
	      ,a.agency_num
	      ,a.bus_unit_ind   
	  FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_underwriter_manager_region a
	 WHERE EXISTS ( SELECT 1
	        FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_underwriter_manager_region b
			WHERE source_sys_id= '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.agency_state = b.agency_state
			AND a.agency_num = b.agency_num
			AND a.bus_unit_ind= b.bus_unit_ind          
	 GROUP BY b.agency_state,  	b.agency_num, b.bus_unit_ind
	             HAVING COUNT(*) > 1)
	ORDER BY a.agency_state,  	a.agency_num, a.bus_unit_ind  , a.eff_from_date  DESC
),
EXP_Lag_Eff_From_Date1 AS (
	SELECT
	sup_uw_mgr_region_id,
	agency_state,
	agency_num,
	bus_unit_ind,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	agency_state= v_prev_row_agency_state
	-- AND agency_num = v_prev_row_agency_num
	-- AND bus_unit_ind = v_prev_row_bus_unit_ind
	-- , ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	DECODE(
	    TRUE,
	    agency_state = v_prev_row_agency_state AND agency_num = v_prev_row_agency_num AND bus_unit_ind = v_prev_row_bus_unit_ind, DATEADD(SECOND,- 1,v_prev_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	agency_state AS v_prev_row_agency_state,
	agency_num AS v_prev_row_agency_num,
	bus_unit_ind AS v_prev_row_bus_unit_ind,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_sup_underwriter_manager_region
),
FIL_First_Row_In_AK_Group AS (
	SELECT
	sup_uw_mgr_region_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_Eff_From_Date1
	WHERE orig_eff_to_date !=eff_to_date
),
UPD_sup_underwriter_manager_region AS (
	SELECT
	sup_uw_mgr_region_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_First_Row_In_AK_Group
),
sup_underwriter_manager_region_UPD AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_underwriter_manager_region AS T
	USING UPD_sup_underwriter_manager_region AS S
	ON T.sup_uw_mgr_region_id = S.sup_uw_mgr_region_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),
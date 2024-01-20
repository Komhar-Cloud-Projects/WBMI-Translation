WITH
SQ_gtam_tl07rx_stage1 AS (
	SELECT
		gtam_tl07rx_stage_id,
		table_fld,
		key_len,
		class_code,
		class_description_indicator,
		state,
		class_description_sequence,
		data_len,
		class_description,
		class_desc_cont_ind,
		class_code_suffix,
		premium_type,
		a_rated_class_indicator,
		hazard_group,
		manual_rating_indicator,
		companion_class_indicator,
		ex_medical_rate_indicator,
		non_rate_comp_class_code,
		west_bend_connect_rated,
		customer_future_use,
		discount_date,
		repl_class_code,
		repl_class_code_desc_ind,
		sic_code,
		start_date,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_tl07rx_stage1
),
LKP_arch_tl07rx_stage AS (
	SELECT
	arch_gtam_tl07rx_stage_id,
	class_description,
	class_desc_cont_ind,
	class_code_suffix,
	premium_type,
	a_rated_class_indicator,
	hazard_group,
	manual_rating_indicator,
	companion_class_indicator,
	ex_medical_rate_indicator,
	non_rate_comp_class_code,
	west_bend_connect_rated,
	customer_future_use,
	discount_date,
	repl_class_code,
	repl_class_code_desc_ind,
	sic_code,
	start_date,
	class_code,
	class_description_indicator,
	state,
	class_description_sequence
	FROM (
		SELECT tl.arch_gtam_tl07rx_stage_id as arch_gtam_tl07rx_stage_id, 
		tl.class_code                                                as class_code,
		tl.class_description_indicator                as class_description_indicator,
		tl.state                                                            as state,
		tl.class_description_sequence            as class_description_sequence,      
		      tl.class_description                            as class_description,
		     tl.class_desc_cont_ind as class_desc_cont_ind ,
		     tl.class_code_suffix as class_code_suffix,
		      tl.premium_type as premium_type ,
		      tl.a_rated_class_indicator as a_rated_class_indicator,
		      tl.hazard_group as hazard_group ,
		      tl.manual_rating_indicator as manual_rating_indicator,
		     tl.companion_class_indicator as companion_class_indicator,
		      tl.ex_medical_rate_indicator as ex_medical_rate_indicator ,
		      tl.non_rate_comp_class_code as non_rate_comp_class_code,
		      tl.west_bend_connect_rated as west_bend_connect_rated ,
		     tl.customer_future_use as customer_future_use,
		      tl.discount_date as discount_date ,
		      tl.repl_class_code as repl_class_code,
		      tl.repl_class_code_desc_ind as repl_class_code_desc_ind,
		      tl.sic_code as sic_code,
		      tl.start_date as start_date
		FROM arch_gtam_tl07rx_stage tl
		where 	tl.arch_gtam_tl07rx_stage_id In
			(Select max(arch_gtam_tl07rx_stage_id) from arch_gtam_tl07rx_stage b
			group by b.class_code,  b.class_description_indicator,     b.state,
		b.class_description_sequence)
		order by 
		tl.class_code, 
		tl.class_description_indicator ,    
		tl.state,
		tl.class_description_sequence--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY class_code,class_description_indicator,state,class_description_sequence ORDER BY arch_gtam_tl07rx_stage_id) = 1
),
EXP_arch_tl07rx_stage AS (
	SELECT
	SQ_gtam_tl07rx_stage1.gtam_tl07rx_stage_id,
	SQ_gtam_tl07rx_stage1.table_fld AS Table_fld,
	SQ_gtam_tl07rx_stage1.key_len AS Key_len,
	SQ_gtam_tl07rx_stage1.class_code,
	SQ_gtam_tl07rx_stage1.class_description_indicator AS class_desc_ind,
	SQ_gtam_tl07rx_stage1.state,
	SQ_gtam_tl07rx_stage1.class_description_sequence AS class_desc_seq,
	SQ_gtam_tl07rx_stage1.data_len,
	SQ_gtam_tl07rx_stage1.class_description,
	SQ_gtam_tl07rx_stage1.class_desc_cont_ind,
	SQ_gtam_tl07rx_stage1.class_code_suffix,
	SQ_gtam_tl07rx_stage1.premium_type,
	SQ_gtam_tl07rx_stage1.a_rated_class_indicator,
	SQ_gtam_tl07rx_stage1.hazard_group,
	SQ_gtam_tl07rx_stage1.manual_rating_indicator,
	SQ_gtam_tl07rx_stage1.companion_class_indicator,
	SQ_gtam_tl07rx_stage1.ex_medical_rate_indicator,
	SQ_gtam_tl07rx_stage1.non_rate_comp_class_code,
	SQ_gtam_tl07rx_stage1.west_bend_connect_rated,
	SQ_gtam_tl07rx_stage1.customer_future_use,
	SQ_gtam_tl07rx_stage1.discount_date,
	SQ_gtam_tl07rx_stage1.repl_class_code,
	SQ_gtam_tl07rx_stage1.repl_class_code_desc_ind,
	SQ_gtam_tl07rx_stage1.sic_code,
	SQ_gtam_tl07rx_stage1.start_date,
	SQ_gtam_tl07rx_stage1.extract_date AS EXTRACT_DATE,
	SQ_gtam_tl07rx_stage1.as_of_date AS AS_OF_DATE,
	SQ_gtam_tl07rx_stage1.record_count AS RECORD_COUNT,
	SQ_gtam_tl07rx_stage1.source_system_id AS SOURCE_SYSTEM_ID,
	LKP_arch_tl07rx_stage.arch_gtam_tl07rx_stage_id AS LKP_arch_gtam_tl07rx_stage_id,
	LKP_arch_tl07rx_stage.class_description AS LKP_class_description,
	LKP_arch_tl07rx_stage.class_desc_cont_ind AS LKP_class_desc_cont_ind,
	LKP_arch_tl07rx_stage.class_code_suffix AS LKP_class_code_suffix,
	LKP_arch_tl07rx_stage.premium_type AS LKP_premium_type,
	LKP_arch_tl07rx_stage.a_rated_class_indicator AS LKP_a_rated_class_indicator,
	LKP_arch_tl07rx_stage.hazard_group AS LKP_hazard_group,
	LKP_arch_tl07rx_stage.manual_rating_indicator AS LKP_manual_rating_indicator,
	LKP_arch_tl07rx_stage.companion_class_indicator AS LKP_companion_class_indicator,
	LKP_arch_tl07rx_stage.ex_medical_rate_indicator AS LKP_ex_medical_rate_indicator,
	LKP_arch_tl07rx_stage.non_rate_comp_class_code AS LKP_non_rate_comp_class_code,
	LKP_arch_tl07rx_stage.west_bend_connect_rated AS LKP_west_bend_connect_rated,
	LKP_arch_tl07rx_stage.customer_future_use AS LKP_customer_future_use,
	LKP_arch_tl07rx_stage.discount_date AS LKP_discount_date,
	LKP_arch_tl07rx_stage.repl_class_code AS LKP_repl_class_code,
	LKP_arch_tl07rx_stage.repl_class_code_desc_ind AS LKP_repl_class_code_desc_ind,
	LKP_arch_tl07rx_stage.sic_code AS LKP_sic_code,
	LKP_arch_tl07rx_stage.start_date AS LKP_start_date,
	-- *INF*: iif(isnull(LKP_arch_gtam_tl07rx_stage_id),'NEW',
	--     iif((
	-- ltrim(rtrim(LKP_class_description)) <> ltrim(rtrim(class_description ))
	-- OR ltrim(rtrim(LKP_class_desc_cont_ind)) <> ltrim(rtrim(class_desc_cont_ind))
	-- OR
	--  ltrim(rtrim(LKP_class_code_suffix ))<> ltrim(rtrim(class_code_suffix ))
	-- OR ltrim(rtrim(LKP_premium_type)) <> ltrim(rtrim(premium_type ))
	-- OR ltrim(rtrim(LKP_a_rated_class_indicator ))<>  ltrim(rtrim(a_rated_class_indicator))
	-- OR
	--  ltrim(rtrim(LKP_hazard_group ))<> ltrim(rtrim(hazard_group))  
	-- OR ltrim(rtrim(LKP_manual_rating_indicator ))<> ltrim(rtrim(manual_rating_indicator))
	-- OR ltrim(rtrim(LKP_companion_class_indicator ))<> ltrim(rtrim(companion_class_indicator))
	-- OR ltrim(rtrim(LKP_ex_medical_rate_indicator)) <> ltrim(rtrim(ex_medical_rate_indicator))
	-- OR 
	-- ltrim(rtrim(LKP_non_rate_comp_class_code)) <> ltrim(rtrim(non_rate_comp_class_code ))   
	-- OR ltrim(rtrim(LKP_west_bend_connect_rated)) <> ltrim(rtrim(west_bend_connect_rated ))
	-- OR ltrim(rtrim(LKP_customer_future_use)) <> ltrim(rtrim(customer_future_use))
	-- OR ltrim(rtrim(LKP_discount_date ))<> ltrim(rtrim(discount_date))
	-- OR ltrim(rtrim(LKP_repl_class_code)) <> ltrim(rtrim(repl_class_code))
	-- OR ltrim(rtrim( LKP_repl_class_code_desc_ind)) <> ltrim(rtrim(repl_class_code_desc_ind))
	-- OR ltrim(rtrim(LKP_sic_code)) <> ltrim(rtrim(sic_code))
	-- OR ltrim(rtrim(LKP_start_date)) <> ltrim(rtrim(start_date))
	-- ), 'UPDATE', 'NOCHANGE'))
	IFF(
	    LKP_arch_gtam_tl07rx_stage_id IS NULL, 'NEW',
	    IFF(
	        (ltrim(rtrim(LKP_class_description)) <> ltrim(rtrim(class_description))
	        or ltrim(rtrim(LKP_class_desc_cont_ind)) <> ltrim(rtrim(class_desc_cont_ind))
	        or ltrim(rtrim(LKP_class_code_suffix)) <> ltrim(rtrim(class_code_suffix))
	        or ltrim(rtrim(LKP_premium_type)) <> ltrim(rtrim(premium_type))
	        or ltrim(rtrim(LKP_a_rated_class_indicator)) <> ltrim(rtrim(a_rated_class_indicator))
	        or ltrim(rtrim(LKP_hazard_group)) <> ltrim(rtrim(hazard_group))
	        or ltrim(rtrim(LKP_manual_rating_indicator)) <> ltrim(rtrim(manual_rating_indicator))
	        or ltrim(rtrim(LKP_companion_class_indicator)) <> ltrim(rtrim(companion_class_indicator))
	        or ltrim(rtrim(LKP_ex_medical_rate_indicator)) <> ltrim(rtrim(ex_medical_rate_indicator))
	        or ltrim(rtrim(LKP_non_rate_comp_class_code)) <> ltrim(rtrim(non_rate_comp_class_code))
	        or ltrim(rtrim(LKP_west_bend_connect_rated)) <> ltrim(rtrim(west_bend_connect_rated))
	        or ltrim(rtrim(LKP_customer_future_use)) <> ltrim(rtrim(customer_future_use))
	        or ltrim(rtrim(LKP_discount_date)) <> ltrim(rtrim(discount_date))
	        or ltrim(rtrim(LKP_repl_class_code)) <> ltrim(rtrim(repl_class_code))
	        or ltrim(rtrim(LKP_repl_class_code_desc_ind)) <> ltrim(rtrim(repl_class_code_desc_ind))
	        or ltrim(rtrim(LKP_sic_code)) <> ltrim(rtrim(sic_code))
	        or ltrim(rtrim(LKP_start_date)) <> ltrim(rtrim(start_date))),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_Changed_Flag,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID
	FROM SQ_gtam_tl07rx_stage1
	LEFT JOIN LKP_arch_tl07rx_stage
	ON LKP_arch_tl07rx_stage.class_code = SQ_gtam_tl07rx_stage1.class_code AND LKP_arch_tl07rx_stage.class_description_indicator = SQ_gtam_tl07rx_stage1.class_description_indicator AND LKP_arch_tl07rx_stage.state = SQ_gtam_tl07rx_stage1.state AND LKP_arch_tl07rx_stage.class_description_sequence = SQ_gtam_tl07rx_stage1.class_description_sequence
),
FIL_Inserts AS (
	SELECT
	gtam_tl07rx_stage_id, 
	Table_fld, 
	Key_len, 
	class_code, 
	class_desc_ind, 
	state, 
	class_desc_seq, 
	data_len, 
	class_description, 
	class_desc_cont_ind, 
	class_code_suffix, 
	premium_type, 
	a_rated_class_indicator, 
	hazard_group, 
	manual_rating_indicator, 
	companion_class_indicator, 
	ex_medical_rate_indicator, 
	non_rate_comp_class_code, 
	west_bend_connect_rated, 
	customer_future_use, 
	discount_date, 
	repl_class_code, 
	repl_class_code_desc_ind, 
	sic_code, 
	start_date, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	Changed_Flag, 
	AUDIT_ID
	FROM EXP_arch_tl07rx_stage
	WHERE Changed_Flag = 'NEW' or Changed_Flag = 'UPDATE'
),
arch_gtam_tl07rx_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_gtam_tl07rx_stage
	(gtam_tl07rx_stage_id, table_fld, key_len, class_code, class_description_indicator, state, class_description_sequence, data_len, class_description, class_desc_cont_ind, class_code_suffix, premium_type, a_rated_class_indicator, hazard_group, manual_rating_indicator, companion_class_indicator, ex_medical_rate_indicator, non_rate_comp_class_code, west_bend_connect_rated, customer_future_use, discount_date, repl_class_code, repl_class_code_desc_ind, sic_code, start_date, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	GTAM_TL07RX_STAGE_ID, 
	Table_fld AS TABLE_FLD, 
	Key_len AS KEY_LEN, 
	CLASS_CODE, 
	class_desc_ind AS CLASS_DESCRIPTION_INDICATOR, 
	STATE, 
	class_desc_seq AS CLASS_DESCRIPTION_SEQUENCE, 
	DATA_LEN, 
	CLASS_DESCRIPTION, 
	CLASS_DESC_CONT_IND, 
	CLASS_CODE_SUFFIX, 
	PREMIUM_TYPE, 
	A_RATED_CLASS_INDICATOR, 
	HAZARD_GROUP, 
	MANUAL_RATING_INDICATOR, 
	COMPANION_CLASS_INDICATOR, 
	EX_MEDICAL_RATE_INDICATOR, 
	NON_RATE_COMP_CLASS_CODE, 
	WEST_BEND_CONNECT_RATED, 
	CUSTOMER_FUTURE_USE, 
	DISCOUNT_DATE, 
	REPL_CLASS_CODE, 
	REPL_CLASS_CODE_DESC_IND, 
	SIC_CODE, 
	START_DATE, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	AUDIT_ID AS AUDIT_ID
	FROM FIL_Inserts
),
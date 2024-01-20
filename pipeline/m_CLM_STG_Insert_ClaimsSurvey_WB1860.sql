WITH
SQ_Shortcut_to_ClaimsSurvey_WB1860 AS (

-- TODO Manual --

),
EXP_AUDIT_FIELDS AS (
	SELECT
	FORM_ID,
	CLAIM_REP_CODE,
	Q_ID_1,
	-- *INF*: IIF(IS_NUMBER(Q_ID_1),TO_INTEGER(Q_ID_1),NULL)
	IFF(REGEXP_LIKE(Q_ID_1, '^[0-9]+$'), CAST(Q_ID_1 AS INTEGER), NULL) AS ques1_resp_val,
	Q_ID_2,
	-- *INF*: IIF(IS_NUMBER(Q_ID_2),TO_INTEGER(Q_ID_2),NULL)
	IFF(REGEXP_LIKE(Q_ID_2, '^[0-9]+$'), CAST(Q_ID_2 AS INTEGER), NULL) AS ques2_resp_val,
	Q_ID_3,
	-- *INF*: IIF(IS_NUMBER(Q_ID_3),TO_INTEGER(Q_ID_3),NULL)
	IFF(REGEXP_LIKE(Q_ID_3, '^[0-9]+$'), CAST(Q_ID_3 AS INTEGER), NULL) AS ques3_resp_val,
	Q_ID_4,
	-- *INF*: IIF(IS_NUMBER(Q_ID_4),TO_INTEGER(Q_ID_4),NULL)
	IFF(REGEXP_LIKE(Q_ID_4, '^[0-9]+$'), CAST(Q_ID_4 AS INTEGER), NULL) AS ques4_resp_val,
	Q_ID_5,
	-- *INF*: IIF(IS_NUMBER(Q_ID_5),TO_INTEGER(Q_ID_5),NULL)
	IFF(REGEXP_LIKE(Q_ID_5, '^[0-9]+$'), CAST(Q_ID_5 AS INTEGER), NULL) AS ques5_resp_val,
	Q_ID_6,
	-- *INF*: IIF(IS_NUMBER(Q_ID_6),TO_INTEGER(Q_ID_6),NULL)
	IFF(REGEXP_LIKE(Q_ID_6, '^[0-9]+$'), CAST(Q_ID_6 AS INTEGER), NULL) AS ques6_resp_val,
	Q_ID_9,
	-- *INF*: IIF(IS_NUMBER(Q_ID_9),TO_INTEGER(Q_ID_9),NULL)
	IFF(REGEXP_LIKE(Q_ID_9, '^[0-9]+$'), CAST(Q_ID_9 AS INTEGER), NULL) AS ques9_resp_val,
	Q_ID_10,
	-- *INF*: IIF(IS_NUMBER(Q_ID_10),TO_INTEGER(Q_ID_10),NULL)
	IFF(REGEXP_LIKE(Q_ID_10, '^[0-9]+$'), CAST(Q_ID_10 AS INTEGER), NULL) AS ques10_resp_val,
	CLAIM_NUM,
	SYSDATE AS EXTRACT_DATE,
	'dummy' AS Dummy_String,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID
	FROM SQ_Shortcut_to_ClaimsSurvey_WB1860
),
SQ_ClaimsSurvey_ExtractDate AS (

-- TODO Manual --

),
Agg_Output_OneRow_ClaimsSurvey AS (
	SELECT
	Extract_Date,
	-- *INF*: max(Extract_Date)
	max(Extract_Date) AS o_Extract_Date
	FROM SQ_ClaimsSurvey_ExtractDate
	GROUP BY 
),
Expression_Add_Dummy_Port AS (
	SELECT
	o_Extract_Date AS Entry_Date,
	-- *INF*: IIF(IS_DATE(Entry_Date, 'MMDDYYYY'), TO_DATE(Entry_Date, 'MMDDYYYY'),
	-- IIF(IS_DATE(Entry_Date, 'MM/DD/YYYY'), TO_DATE(Entry_Date, 'MM/DD/YYYY'),
	-- IIF(IS_DATE(Entry_Date, 'MM-DD-YYYY'), TO_DATE(Entry_Date, 'MM-DD-YYYY'),
	-- IIF(IS_DATE(Entry_Date, 'MMDDYY'), TO_DATE(Entry_Date, 'MMDDYY'),
	-- IIF(IS_DATE(Entry_Date, 'MM/DD/YY'), TO_DATE(Entry_Date, 'MM/DD/YY'),
	-- IIF(IS_DATE(Entry_Date, 'MM-DD-YY'), TO_DATE(Entry_Date, 'MM-DD-YY'),
	-- IIF(IS_DATE(Entry_Date, 'DDMMYYYY'), TO_DATE(Entry_Date, 'DDMMYYYY'),
	-- IIF(IS_DATE(Entry_Date, 'DD/MM/YYYY'), TO_DATE(Entry_Date, 'DD/MM/YYYY'),
	-- IIF(IS_DATE(Entry_Date, 'DD-MM-YYYY'), TO_DATE(Entry_Date, 'DD-MM-YYYY'),
	-- IIF(IS_DATE(Entry_Date, 'DDMMYY'), TO_DATE(Entry_Date, 'DDMMYY'),
	-- IIF(IS_DATE(Entry_Date, 'DD/MM/YY'), TO_DATE(Entry_Date, 'DD/MM/YY'),
	-- IIF(IS_DATE(Entry_Date, 'DD-MM-YY'), TO_DATE(Entry_Date, 'DD-MM-YY'),
	-- IIF(IS_DATE(Entry_Date, 'YYYYMMDD'), TO_DATE(Entry_Date, 'YYYYMMDD'),
	-- IIF(IS_DATE(Entry_Date, 'YYYY/MM/DD'), TO_DATE(Entry_Date, 'YYYY/MM/DD'),
	-- IIF(IS_DATE(Entry_Date, 'YYYY-MM-DD'), TO_DATE(Entry_Date, 'YYYY-MM-DD'),
	-- IIF(IS_DATE(Entry_Date, 'MON DD YYYY'), TO_DATE(Entry_Date, 'MON DD YYYY'),
	-- IIF(IS_DATE(Entry_Date, 'MONTH DD YYYY'),TO_DATE(Entry_Date, 'MONTH DD YYYY')
	-- )))))))))))))))))
	IFF(
	    IS_DATE(Entry_Date, 'MMDDYYYY'), TO_TIMESTAMP(Entry_Date, 'MMDDYYYY'),
	    IFF(
	        IS_DATE(Entry_Date, 'MM/DD/YYYY'), TO_TIMESTAMP(Entry_Date, 'MM/DD/YYYY'),
	        IFF(
	            IS_DATE(Entry_Date, 'MM-DD-YYYY'), TO_TIMESTAMP(Entry_Date, 'MM-DD-YYYY'),
	            IFF(
	                IS_DATE(Entry_Date, 'MMDDYY'), TO_TIMESTAMP(Entry_Date, 'MMDDYY'),
	                IFF(
	                    IS_DATE(Entry_Date, 'MM/DD/YY'),
	                    TO_TIMESTAMP(Entry_Date, 'MM/DD/YY'),
	                    IFF(
	                        IS_DATE(Entry_Date, 'MM-DD-YY'),
	                        TO_TIMESTAMP(Entry_Date, 'MM-DD-YY'),
	                        IFF(
	                            IS_DATE(Entry_Date, 'DDMMYYYY'),
	                            TO_TIMESTAMP(Entry_Date, 'DDMMYYYY'),
	                            IFF(
	                                IS_DATE(Entry_Date, 'DD/MM/YYYY'),
	                                TO_TIMESTAMP(Entry_Date, 'DD/MM/YYYY'),
	                                IFF(
	                                    IS_DATE(Entry_Date, 'DD-MM-YYYY'),
	                                    TO_TIMESTAMP(Entry_Date, 'DD-MM-YYYY'),
	                                    IFF(
	                                        IS_DATE(Entry_Date, 'DDMMYY'),
	                                        TO_TIMESTAMP(Entry_Date, 'DDMMYY'),
	                                        IFF(
	                                            IS_DATE(Entry_Date, 'DD/MM/YY'),
	                                            TO_TIMESTAMP(Entry_Date, 'DD/MM/YY'),
	                                            IFF(
	                                                IS_DATE(Entry_Date, 'DD-MM-YY'),
	                                                TO_TIMESTAMP(Entry_Date, 'DD-MM-YY'),
	                                                IFF(
	                                                    IS_DATE(Entry_Date, 'YYYYMMDD'),
	                                                    TO_TIMESTAMP(Entry_Date, 'YYYYMMDD'),
	                                                    IFF(
	                                                        IS_DATE(Entry_Date, 'YYYY/MM/DD'),
	                                                        TO_TIMESTAMP(Entry_Date, 'YYYY/MM/DD'),
	                                                        IFF(
	                                                            IS_DATE(Entry_Date, 'YYYY-MM-DD'),
	                                                            TO_TIMESTAMP(Entry_Date, 'YYYY-MM-DD'),
	                                                            IFF(
	                                                                IS_DATE(Entry_Date, 'MON DD YYYY'),
	                                                                TO_TIMESTAMP(Entry_Date, 'MON DD YYYY'),
	                                                                IFF(
	                                                                    IS_DATE(Entry_Date, 'MONTH DD YYYY'),
	                                                                    TO_TIMESTAMP(Entry_Date, 'MONTH DD YYYY')
	                                                                )
	                                                            )
	                                                        )
	                                                    )
	                                                )
	                                            )
	                                        )
	                                    )
	                                )
	                            )
	                        )
	                    )
	                )
	            )
	        )
	    )
	) AS o_Entry_Date,
	'dummy' AS Dummy_String
	FROM Agg_Output_OneRow_ClaimsSurvey
),
JNR_Get_Date AS (SELECT
	EXP_AUDIT_FIELDS.FORM_ID, 
	EXP_AUDIT_FIELDS.CLAIM_REP_CODE, 
	EXP_AUDIT_FIELDS.ques1_resp_val, 
	EXP_AUDIT_FIELDS.ques2_resp_val, 
	EXP_AUDIT_FIELDS.ques3_resp_val, 
	EXP_AUDIT_FIELDS.ques4_resp_val, 
	EXP_AUDIT_FIELDS.ques5_resp_val, 
	EXP_AUDIT_FIELDS.ques6_resp_val, 
	EXP_AUDIT_FIELDS.ques9_resp_val, 
	EXP_AUDIT_FIELDS.ques10_resp_val, 
	EXP_AUDIT_FIELDS.CLAIM_NUM, 
	EXP_AUDIT_FIELDS.EXTRACT_DATE, 
	EXP_AUDIT_FIELDS.SOURCE_SYSTEM_ID, 
	EXP_AUDIT_FIELDS.Dummy_String, 
	Expression_Add_Dummy_Port.o_Entry_Date, 
	Expression_Add_Dummy_Port.Dummy_String AS Dummy_String1
	FROM EXP_AUDIT_FIELDS
	LEFT OUTER JOIN Expression_Add_Dummy_Port
	ON Expression_Add_Dummy_Port.Dummy_String = EXP_AUDIT_FIELDS.Dummy_String
),
Exp_Check_SurveyData_For_BadExtractDate AS (
	SELECT
	FORM_ID,
	CLAIM_REP_CODE,
	ques1_resp_val,
	ques2_resp_val,
	ques3_resp_val,
	ques4_resp_val,
	ques5_resp_val,
	ques6_resp_val,
	ques9_resp_val,
	ques10_resp_val,
	CLAIM_NUM,
	EXTRACT_DATE AS Extract_Date,
	SOURCE_SYSTEM_ID,
	o_Entry_Date,
	-- *INF*: iif(isnull(o_Entry_Date),abort('Not a valid Date. Please check with source system. Until it is fixed, this step can be skipped.'))
	IFF(
	    o_Entry_Date IS NULL,
	    abort('Not a valid Date. Please check with source system. Until it is fixed, this step can be skipped.')
	) AS Check_Error
	FROM JNR_Get_Date
),
claims_survey_result_stage AS (
	TRUNCATE TABLE claims_survey_result_stage;
	INSERT INTO claims_survey_result_stage
	(form_num, claim_rep_code, ques1_resp_val, ques2_resp_val, ques3_resp_val, ques4_resp_val, ques5_resp_val, ques6_resp_val, ques7_resp_val, ques8_resp_val, ques9_resp_val, ques10_resp_val, ques11_resp_val, ques12_resp_val, ques13_resp_val, ques14_resp_val, ques15_resp_val, ques16_resp_val, ques17_resp_val, ques18_resp_val, entry_date, claim_num, extract_date, source_system_id)
	SELECT 
	FORM_ID AS FORM_NUM, 
	CLAIM_REP_CODE AS CLAIM_REP_CODE, 
	QUES1_RESP_VAL, 
	QUES2_RESP_VAL, 
	QUES3_RESP_VAL, 
	QUES4_RESP_VAL, 
	QUES5_RESP_VAL, 
	QUES6_RESP_VAL, 
	QUES7_RESP_VAL, 
	QUES8_RESP_VAL, 
	QUES9_RESP_VAL, 
	QUES10_RESP_VAL, 
	QUES11_RESP_VAL, 
	QUES12_RESP_VAL, 
	QUES13_RESP_VAL, 
	QUES14_RESP_VAL, 
	QUES15_RESP_VAL, 
	QUES16_RESP_VAL, 
	QUES17_RESP_VAL, 
	QUES18_RESP_VAL, 
	o_Entry_Date AS ENTRY_DATE, 
	CLAIM_NUM AS CLAIM_NUM, 
	Extract_Date AS EXTRACT_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID
	FROM Exp_Check_SurveyData_For_BadExtractDate
),
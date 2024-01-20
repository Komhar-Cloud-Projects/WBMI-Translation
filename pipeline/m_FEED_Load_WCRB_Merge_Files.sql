WITH
LKP_Get_OutStandingLossReserve_PreviousQuarter AS (
	SELECT
	OutstandingLossReserveAmount,
	ReportingQuarter
	FROM (
		SELECT 
			OutstandingLossReserveAmount,
			ReportingQuarter
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCRBOutstandingLossReserveQuarterly
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ReportingQuarter ORDER BY OutstandingLossReserveAmount) = 1
),
SQ_WCRB_Type3_DCT AS (

-- TODO Manual --

),
SQ_WCRB_Type3_PMS AS (

-- TODO Manual --

),
UN_Type3 AS (
	SELECT Records
	FROM SQ_WCRB_Type3_PMS
	UNION
	SELECT Records
	FROM SQ_WCRB_Type3_DCT
),
AGG_OutstandingLossReserves_Current AS (
	SELECT
	Records AS in_Records,
	1 AS out_Dummy,
	-- *INF*: sum(to_decimal(ltrim(rtrim(substr(REPLACESTR(0,REPLACESTR(0,in_Records,'~',''),'*',''),192,12))))) + sum(to_decimal(ltrim(rtrim(substr(REPLACESTR(0,REPLACESTR(0,in_Records,'~',''),'*',''),204,12)))))
	sum(CAST(ltrim(rtrim(substr(REGEXP_REPLACE(REGEXP_REPLACE(in_Records,'~','','i'),'*','','i'), 192, 12))) AS FLOAT)) + sum(CAST(ltrim(rtrim(substr(REGEXP_REPLACE(REGEXP_REPLACE(in_Records,'~','','i'),'*','','i'), 204, 12))) AS FLOAT)) AS out_OutstandingLossReserves_Current
	FROM UN_Type3
	GROUP BY out_Dummy
),
SQ_Dummy AS (
	select distinct CONCAT(Year(max(PremiumMasterRunDate)),
	DATEPART(QUARTER,max(PremiumMasterRunDate))) as ReportingQuarter,
	CONCAT(Year(dateadd(month,-3,max(PremiumMasterRunDate))),
	DATEPART(QUARTER,dateadd(month,-3,max(PremiumMasterRunDate)))) as PreviousReportingQuarter
	from WcrbWorkTable
	where @{pipeline().parameters.EXTRACTDATE}
),
EXP_Type1_GetOutstandingLossReservesPreviousQuarter AS (
	SELECT
	ReportingQuarter,
	PreviousReportingQuarter AS i_PreviousReportingQuarter,
	-- *INF*: IIF( ISNULL(:LKP.LKP_GET_OUTSTANDINGLOSSRESERVE_PREVIOUSQUARTER(i_PreviousReportingQuarter)),'000000000000',
	-- :LKP.LKP_GET_OUTSTANDINGLOSSRESERVE_PREVIOUSQUARTER(i_PreviousReportingQuarter))
	IFF(
	    LKP_GET_OUTSTANDINGLOSSRESERVE_PREVIOUSQUARTER_i_PreviousReportingQuarter.OutstandingLossReserveAmount IS NULL,
	    '000000000000',
	    LKP_GET_OUTSTANDINGLOSSRESERVE_PREVIOUSQUARTER_i_PreviousReportingQuarter.OutstandingLossReserveAmount
	) AS lkp_OutstandingAmount_Previous,
	1 AS o_Dummy
	FROM SQ_Dummy
	LEFT JOIN LKP_GET_OUTSTANDINGLOSSRESERVE_PREVIOUSQUARTER LKP_GET_OUTSTANDINGLOSSRESERVE_PREVIOUSQUARTER_i_PreviousReportingQuarter
	ON LKP_GET_OUTSTANDINGLOSSRESERVE_PREVIOUSQUARTER_i_PreviousReportingQuarter.ReportingQuarter = i_PreviousReportingQuarter

),
JNR_OutstandingLossReserves_Current AS (SELECT
	AGG_OutstandingLossReserves_Current.out_Dummy AS i_Dummy, 
	AGG_OutstandingLossReserves_Current.out_OutstandingLossReserves_Current AS OutstandingLossReserves_Current, 
	EXP_Type1_GetOutstandingLossReservesPreviousQuarter.o_Dummy AS Dummy, 
	EXP_Type1_GetOutstandingLossReservesPreviousQuarter.ReportingQuarter, 
	EXP_Type1_GetOutstandingLossReservesPreviousQuarter.lkp_OutstandingAmount_Previous AS OutstandingAmount_Previous
	FROM AGG_OutstandingLossReserves_Current
	INNER JOIN EXP_Type1_GetOutstandingLossReservesPreviousQuarter
	ON EXP_Type1_GetOutstandingLossReservesPreviousQuarter.o_Dummy = AGG_OutstandingLossReserves_Current.out_Dummy
),
EXP_Passthrough AS (
	SELECT
	sessstarttime AS out_CreatedDate,
	sessstarttime AS out_ModifiedDate,
	ReportingQuarter,
	OutstandingLossReserves_Current AS in_OutstandingLossReserves_Current,
	-- *INF*: LPAD(TO_CHAR(ROUND(in_OutstandingLossReserves_Current)),12,'0')
	LPAD(TO_CHAR(ROUND(in_OutstandingLossReserves_Current)), 12, '0') AS out_OutstandingLossReserves_Current
	FROM JNR_OutstandingLossReserves_Current
),
WorkWCRBOutstandingLossReserveQuarterly AS (
	INSERT INTO WorkWCRBOutstandingLossReserveQuarterly
	(CreatedDate, ModifiedDate, ReportingQuarter, OutstandingLossReserveAmount)
	SELECT 
	out_CreatedDate AS CREATEDDATE, 
	out_ModifiedDate AS MODIFIEDDATE, 
	REPORTINGQUARTER, 
	out_OutstandingLossReserves_Current AS OUTSTANDINGLOSSRESERVEAMOUNT
	FROM EXP_Passthrough
),
SQ_WCRB_Type4 AS (

-- TODO Manual --

),
SQ_WCRB_Type5 AS (

-- TODO Manual --

),
SQ_WCRB_Type2_DCT AS (

-- TODO Manual --

),
SQ_WCRB_Type2_PMS AS (

-- TODO Manual --

),
UN_Type2 AS (
	SELECT Records
	FROM SQ_WCRB_Type2_PMS
	UNION
	SELECT Records
	FROM SQ_WCRB_Type2_DCT
),
SQ_WCRB_Type6_DCT AS (

-- TODO Manual --

),
SQ_WCRB_Type6_PMS AS (

-- TODO Manual --

),
UN_Type6 AS (
	SELECT Records
	FROM SQ_WCRB_Type6_PMS
	UNION
	SELECT Records
	FROM SQ_WCRB_Type6_DCT
),
UN_Type2_to_6 AS (
	SELEC
	FROM 
	UNION
	SELECT Records
	FROM UN_Type2
	UNION
	SELECT Records
	FROM UN_Type3
	UNION
	SELECT Records
	FROM SQ_WCRB_Type4
	UNION
	SELECT Records
	FROM SQ_WCRB_Type5
	UNION
	SELECT Records
	FROM UN_Type6
),
AGG_CountOfAllRows AS (
	SELECT
	Records,
	-- *INF*: COUNT(Records)
	COUNT(Records) AS o_Count,
	1 AS o_Dummy
	FROM UN_Type2_to_6
	GROUP BY 
),
JNR_CombineType1WithCountAndLossReserve AS (SELECT
	JNR_OutstandingLossReserves_Current.ReportingQuarter, 
	JNR_OutstandingLossReserves_Current.OutstandingLossReserves_Current AS OutstandingAmount_Current, 
	JNR_OutstandingLossReserves_Current.Dummy AS i_Dummy_Loss, 
	AGG_CountOfAllRows.o_Count AS Count, 
	AGG_CountOfAllRows.o_Dummy AS i_Dummy_Count, 
	JNR_OutstandingLossReserves_Current.OutstandingAmount_Previous
	FROM JNR_OutstandingLossReserves_Current
	INNER JOIN AGG_CountOfAllRows
	ON AGG_CountOfAllRows.o_Dummy = JNR_OutstandingLossReserves_Current.Dummy
),
EXP_WCRB_Type1 AS (
	SELECT
	ReportingQuarter AS i_ReportingQuarter,
	OutstandingAmount_Current AS i_OutstandingAmount_Current,
	OutstandingAmount_Previous AS i_OutstandingAmount_Previous,
	Count AS i_Count,
	'01' AS v_RecordType_01_02,
	'17124' AS v_CarrierCode_03_07,
	-- *INF*: RPAD('West Bend Mutual Insurance Co',40,' ')
	RPAD('West Bend Mutual Insurance Co', 40, ' ') AS v_NameOfCarrier_08_47,
	i_ReportingQuarter AS v_ReportingQuarter_48_52,
	-- *INF*: RPAD('Chuck Hosp',40,' ')
	RPAD('Chuck Hosp', 40, ' ') AS v_ReportPreparer_53_92,
	'2623346597' AS v_Tel_93_102,
	-- *INF*: RPAD('1900 South 18th Ave',60,' ')
	RPAD('1900 South 18th Ave', 60, ' ') AS v_Address_103_162,
	-- *INF*: RPAD('West Bend',40,' ')
	RPAD('West Bend', 40, ' ') AS v_City_163_202,
	'WI' AS v_State_203_204,
	-- *INF*: RPAD('53095',9,' ')
	RPAD('53095', 9, ' ') AS v_ZipCode_205_213,
	-- *INF*: RPAD('chosp@wbmi.com',50,' ')
	RPAD('chosp@wbmi.com', 50, ' ') AS v_Email_214_263,
	-- *INF*: LPAD(i_OutstandingAmount_Previous,12,0)
	-- --LPAD(TO_CHAR(ROUND(i_OutstandingAmount_Previous_old)),12,'0')
	LPAD(i_OutstandingAmount_Previous, 12, 0) AS v_OutstandingLoss_264_275,
	-- *INF*: LPAD(TO_CHAR(ROUND(i_OutstandingAmount_Current)),12,'0')
	LPAD(TO_CHAR(ROUND(i_OutstandingAmount_Current)), 12, '0') AS v_OutstandingLoss_276_287,
	-- *INF*: LPAD(TO_CHAR(i_Count),9,'0')
	LPAD(TO_CHAR(i_Count), 9, '0') AS v_TotalCount_288_296,
	v_RecordType_01_02
 || v_CarrierCode_03_07
 || v_NameOfCarrier_08_47
 || v_ReportingQuarter_48_52
 || v_ReportPreparer_53_92
 || v_Tel_93_102
 || v_Address_103_162
 || v_City_163_202
 || v_State_203_204
 || v_ZipCode_205_213
 || v_Email_214_263
 || v_OutstandingLoss_264_275
 || v_OutstandingLoss_276_287
 || v_TotalCount_288_296 AS o_Record_Type1
	FROM JNR_CombineType1WithCountAndLossReserve
),
EXP_WCRB_Type2_to_6 AS (
	SELECT
	Records AS i_Records,
	-- *INF*: REPLACESTR(0,REPLACESTR(0,i_Records,'~',''),'*','')
	REGEXP_REPLACE(REGEXP_REPLACE(i_Records,'~','','i'),'*','','i') AS o_Records
	FROM UN_Type2_to_6
),
UN_MergeType1WithAllOtherTypes AS (
	SELECT o_Record_Type1 AS Records
	FROM EXP_WCRB_Type1
	UNION
	SELECT o_Records AS Records
	FROM EXP_WCRB_Type2_to_6
),
SRT_SorttheMergedFile AS (
	SELECT
	Records
	FROM UN_MergeType1WithAllOtherTypes
	ORDER BY Records ASC
),
EXP_PadFileLength AS (
	SELECT
	Records AS in_Records,
	-- *INF*: RPAD(in_Records,296,' ')
	-- -- right padding the record to hit the required 296 length
	RPAD(in_Records, 296, ' ') AS out_Records
	FROM SRT_SorttheMergedFile
),
WCRB_FlatFile_Target AS (
	INSERT INTO DataFeed_FlatFile
	(Record)
	SELECT 
	out_Records AS RECORD
	FROM EXP_PadFileLength
),
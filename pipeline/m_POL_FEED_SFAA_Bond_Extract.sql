WITH
SQ_SFAA_TL_SC AS (

-- TODO Manual --

),
EXP_GetLastRecord AS (
	SELECT
	Record,
	-- *INF*: SUBSTR(LTRIM(RTRIM(Record)),1,2)
	SUBSTR(LTRIM(RTRIM(Record)), 1, 2) AS FilterFlag
	FROM SQ_SFAA_TL_SC
),
FIL_LastRecord AS (
	SELECT
	Record, 
	FilterFlag
	FROM EXP_GetLastRecord
	WHERE FilterFlag='SC'
),
RTRTRANS AS (
	SELECT
	Record
	FROM FIL_LastRecord
),
RTRTRANS_Transmittal AS (SELECT * FROM RTRTRANS WHERE TRUE),
RTRTRANS_SubmissionControl AS (SELECT * FROM RTRTRANS WHERE TRUE),
EXP_SC AS (
	SELECT
	Record AS i_Record,
	'SC' AS v_SC_1_2,
	-- *INF*: SUBSTR(i_Record,3,2)
	SUBSTR(i_Record, 3, 2) AS v_ModuleIdentification_3_4,
	-- *INF*: SUBSTR(i_Record,5,4)
	SUBSTR(i_Record, 5, 4) AS v_TRGroup_5_8,
	-- *INF*: SUBSTR(i_Record,9,1)
	SUBSTR(i_Record, 9, 1) AS v_AccountingMonth_9,
	-- *INF*: SUBSTR(i_Record,10,1)
	SUBSTR(i_Record, 10, 1) AS v_AccountingYear_10,
	-- *INF*: SUBSTR(i_Record,11,1)
	SUBSTR(i_Record, 11, 1) AS v_TypeofStatistic_11,
	-- *INF*: SUBSTR(i_Record,12,1)
	SUBSTR(i_Record, 12, 1) AS v_TypeofSubmission_12,
	-- *INF*: SUBSTR(i_Record,13,2)
	SUBSTR(i_Record, 13, 2) AS v_CountofSubmission_13_14,
	-- *INF*: SUBSTR(i_Record,15,10)
	SUBSTR(i_Record, 15, 10) AS v_DollarAmount_15_24,
	-- *INF*: SUBSTR(i_Record,25,9)
	SUBSTR(i_Record, 25, 9) AS v_RecordCount_25_33,
	-- *INF*: RPAD(' ', 47, ' ')
	RPAD(' ', 47, ' ') AS v_Filler_34_80,
	v_SC_1_2
 || v_ModuleIdentification_3_4
 || v_TRGroup_5_8
 || v_AccountingMonth_9
 || v_AccountingYear_10
 || v_TypeofStatistic_11
 || v_TypeofSubmission_12
 || v_CountofSubmission_13_14
 || v_DollarAmount_15_24
 || v_RecordCount_25_33
 || v_Filler_34_80 AS o_TLRecord,
	2 AS o_OrdInd
	FROM RTRTRANS_SubmissionControl
),
EXP_TL AS (
	SELECT
	Record AS i_Record,
	'TL' AS v_TL_1_2,
	'6115' AS v_TRGroup_3_6,
	'04' AS v_StatisticalPlan_7_8,
	-- *INF*: DECODE(SUBSTR(LTRIM(RTRIM(i_Record)),9,1),
	-- '1','01',
	-- '2','02',
	-- '3','03',
	-- '4','04',
	-- '5','05',
	-- '6','06',
	-- '7','07',
	-- '8','08',
	-- '9','09',
	-- 'O','10',
	-- '_','11',
	-- '&','12'
	-- )
	DECODE(
	    SUBSTR(LTRIM(RTRIM(i_Record)), 9, 1),
	    '1', '01',
	    '2', '02',
	    '3', '03',
	    '4', '04',
	    '5', '05',
	    '6', '06',
	    '7', '07',
	    '8', '08',
	    '9', '09',
	    'O', '10',
	    '_', '11',
	    '&', '12'
	) AS v_AccountingMonth_9_10,
	-- *INF*: TO_CHAR(TO_DATE(SUBSTR(i_Record,10,1),'Y'),'RR')
	TO_CHAR(TO_TIMESTAMP(SUBSTR(i_Record, 10, 1), 'Y'), 'RR') AS v_AccountingYear_11_12,
	-- *INF*: DECODE(TRUE,
	-- 
	-- TO_INTEGER(TO_CHAR(SYSDATE,'YYYY')) - 1 = TO_INTEGER(TO_CHAR(TO_DATE(SUBSTR(i_Record,10,1),'Y'),'YYYY')),v_AccountingYear_11_12,
	-- 
	-- TO_INTEGER(TO_CHAR(TO_DATE(SUBSTR(i_Record,10,1),'Y'),'YYYY')) - (TO_INTEGER(TO_CHAR(SYSDATE,'YYYY'))-1) =10, TO_CHAR(ADD_TO_DATE(TO_DATE(SUBSTR(i_Record,10,1),'Y'),'YYYY',-10),'RR'),
	-- 
	-- v_AccountingYear_11_12
	-- )
	-- 
	-- 
	DECODE(
	    TRUE,
	    CAST(TO_CHAR(CURRENT_TIMESTAMP, 'YYYY') AS INTEGER) - 1 = CAST(TO_CHAR(TO_TIMESTAMP(SUBSTR(i_Record, 10, 1), 'Y'), 'YYYY') AS INTEGER), v_AccountingYear_11_12,
	    CAST(TO_CHAR(TO_TIMESTAMP(SUBSTR(i_Record, 10, 1), 'Y'), 'YYYY') AS INTEGER) - (CAST(TO_CHAR(CURRENT_TIMESTAMP, 'YYYY') AS INTEGER) - 1) = 10, TO_CHAR(DATEADD(YEAR,- 10,TO_TIMESTAMP(SUBSTR(i_Record, 10, 1), 'Y')), 'RR'),
	    v_AccountingYear_11_12
	) AS v_AccountingYear_11_12_Mod,
	-- *INF*: SUBSTR(i_Record,11,1)
	SUBSTR(i_Record, 11, 1) AS v_TypeofStatistic_13,
	-- *INF*: SUBSTR(i_Record,12,1)
	SUBSTR(i_Record, 12, 1) AS v_TypeofSubmission_14,
	-- *INF*: SUBSTR(i_Record,13,2)
	SUBSTR(i_Record, 13, 2) AS v_CountofSubmission_15_16,
	' ' AS v_FinalPartialIndicator_17,
	' ' AS v_TypeofSubmissiontobeCorrected_18,
	'  ' AS v_CountofSubmissiontobeCorrected_19_20,
	-- *INF*: RPAD(' ', 60, ' ')
	RPAD(' ', 60, ' ') AS v_Filler_21_80,
	v_TL_1_2
 || v_TRGroup_3_6
 || v_StatisticalPlan_7_8
 || v_AccountingMonth_9_10
 || v_AccountingYear_11_12_Mod
 || v_TypeofStatistic_13
 || v_TypeofSubmission_14
 || v_CountofSubmission_15_16
 || v_FinalPartialIndicator_17
 || v_TypeofSubmissiontobeCorrected_18
 || v_CountofSubmissiontobeCorrected_19_20
 || v_Filler_21_80 AS o_TLRecord,
	1 AS o_OrderInd,
	-- *INF*: @{pipeline().parameters.FILENAME_PREFIX} || '_' || TO_CHAR(TO_DATE(v_AccountingYear_11_12_Mod,'YY'),'YYYY') || '.txt'
	@{pipeline().parameters.FILENAME_PREFIX} || '_' || TO_CHAR(TO_TIMESTAMP(v_AccountingYear_11_12_Mod, 'YY'), 'YYYY') || '.txt' AS FileName,
	-- *INF*: IIF(
	-- TO_INTEGER(TO_CHAR(SYSDATE,'YYYY')) - 1 <> TO_INTEGER(TO_CHAR(TO_DATE(v_AccountingYear_11_12_Mod,'YY'),'YYYY')),ABORT('the mainframe source files are not available for the most recent accounting year'),'pass')
	IFF(
	    CAST(TO_CHAR(CURRENT_TIMESTAMP, 'YYYY') AS INTEGER) - 1 <> CAST(TO_CHAR(TO_TIMESTAMP(v_AccountingYear_11_12_Mod, 'YY'), 'YYYY') AS INTEGER),
	    ABORT('the mainframe source files are not available for the most recent accounting year'),
	    'pass'
	) AS v_DataCheck
	FROM RTRTRANS_Transmittal
),
SQ_SFAA_Extract AS (

-- TODO Manual --

),
EXP_Content AS (
	SELECT
	Record,
	3 AS o_OrderInd,
	-- *INF*: SUBSTR(LTRIM(RTRIM(Record)),1,2)
	SUBSTR(LTRIM(RTRIM(Record)), 1, 2) AS FilterFlag
	FROM SQ_SFAA_Extract
),
FIL_DataRecords AS (
	SELECT
	Record, 
	o_OrderInd, 
	FilterFlag
	FROM EXP_Content
	WHERE FilterFlag<>'SC'
),
Union AS (
	SELECT o_TLRecord AS Record, o_OrderInd AS OrderInd, FileName
	FROM EXP_TL
	UNION
	SELECT o_TLRecord AS Record, o_OrdInd AS OrderInd
	FROM EXP_SC
	UNION
	SELECT Record, o_OrderInd AS OrderInd
	FROM FIL_DataRecords
),
EXPTRANS AS (
	SELECT
	Record,
	OrderInd,
	FileName
	FROM Union
),
SRTTRANS AS (
	SELECT
	Record, 
	OrderInd, 
	FileName
	FROM EXPTRANS
	ORDER BY OrderInd ASC
),
TGT_SFAA_Extract AS (
	INSERT INTO SFAA_Extract
	(Record, FileName)
	SELECT 
	RECORD, 
	FILENAME
	FROM SRTTRANS
),
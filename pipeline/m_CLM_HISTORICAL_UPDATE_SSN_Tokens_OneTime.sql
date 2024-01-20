WITH
SQ_WorkTokenExResponse_RPT_EDM AS (
	SELECT 1 as SSNId, 1 as SSNToken
),
EXP_RPT_EDM AS (
	SELECT
	SSNId,
	SSNToken,
	@{pipeline().parameters.SQL_CONNECTION_RPT_EDM} AS SQL_DataBaseConnectionName_RPTEDM
	FROM SQ_WorkTokenExResponse_RPT_EDM
),
SQL_RPT_EDM AS (-- SQL_RPT_EDM

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
sql_error_RPT_EDM AS (
	INSERT INTO sql_error
	(sql_error)
	SELECT 
	SQLError AS SQL_ERROR
	FROM SQL_RPT_EDM
),
SQ_WorkTokenExResponse_WC_Data_Mart AS (
	SELECT 1 as SSNId, 1 as SSNToken
),
EXP_WC_Data_Mart AS (
	SELECT
	SSNId,
	SSNToken,
	@{pipeline().parameters.SQL_CONNECTION_WC_DATAMART} AS SQL_DataBaseConnectionName,
	@{pipeline().parameters.SQL_CONNECTION_RPT_EDM} AS SQL_DataBaseConnectionName_RPTEDM
	FROM SQ_WorkTokenExResponse_WC_Data_Mart
),
SQL_WC_Data_Mart AS (-- SQL_WC_Data_Mart

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
sql_error_WC_Data_Mart AS (
	INSERT INTO sql_error
	(sql_error)
	SELECT 
	SQLError AS SQL_ERROR
	FROM SQL_WC_Data_Mart
),
SQ_WorkTokenExResponse_WC_Stage AS (
	SELECT 1 as SSNId, 1 as SSNToken
),
EXP_WC_Stage AS (
	SELECT
	SSNId,
	SSNToken,
	@{pipeline().parameters.SQL_CONNECTION_WC_STAGE} AS SQL_DataBaseConnectionName,
	@{pipeline().parameters.SQL_CONNECTION_RPT_EDM} AS SQL_DataBaseConnectionName_RPTEDM
	FROM SQ_WorkTokenExResponse_WC_Stage
),
SQL_WC_Stage AS (-- SQL_WC_Stage

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
sql_error_WC_Stage AS (
	INSERT INTO sql_error
	(sql_error)
	SELECT 
	SQLError AS SQL_ERROR
	FROM SQL_WC_Stage
),
SQ_WorkTokenExResponse_DataFeedMart AS (
	SELECT 1 as SSNId, 1 as SSNToken
),
EXP_DataFeedMart AS (
	SELECT
	SSNId,
	SSNToken,
	@{pipeline().parameters.SQL_CONNECTION_DATAFEEDMART} AS SQL_DataBaseConnectionName,
	@{pipeline().parameters.SQL_CONNECTION_RPT_EDM} AS SQL_DataBaseConnectionName_RPTEDM
	FROM SQ_WorkTokenExResponse_DataFeedMart
),
SQL_DataFeedMart AS (-- SQL_DataFeedMart

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
sql_error_DataFeedMart AS (
	INSERT INTO sql_error
	(sql_error)
	SELECT 
	SQLError AS SQL_ERROR
	FROM SQL_DataFeedMart
),
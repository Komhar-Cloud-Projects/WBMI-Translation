WITH
SQ_package AS (
	SELECT
		pkg_id,
		cust_id,
		created_user_id,
		created_date,
		modified_user_id,
		modified_date,
		accting_status_chc_id,
		accting_submitted_by_userid,
		accting_submitted_date,
		uw_status_chc_id,
		uw_submitted_by_userid,
		uw_submitted_date,
		agent_correspondence,
		staffware_pkg_id
	FROM package
),
EXP_Values AS (
	SELECT
	pkg_id,
	cust_id,
	created_user_id,
	created_date,
	modified_user_id,
	modified_date,
	accting_status_chc_id,
	accting_submitted_by_userid,
	accting_submitted_date,
	uw_status_chc_id,
	uw_submitted_by_userid,
	uw_submitted_date,
	agent_correspondence,
	staffware_pkg_id,
	SYSDATE AS extract_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_system_id
	FROM SQ_package
),
package_cl_stage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.package_cl_stage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.package_cl_stage
	(pkg_id, cust_id, created_user_id, created_date, modified_user_id, modified_date, accting_status_chc_id, accting_submitted_by_userid, accting_submitted_date, uw_status_chc_id, uw_submitted_by_userid, uw_submitted_date, agent_correspondence, staffware_pkg_id, extract_date, source_system_id)
	SELECT 
	PKG_ID, 
	CUST_ID, 
	CREATED_USER_ID, 
	CREATED_DATE, 
	MODIFIED_USER_ID, 
	MODIFIED_DATE, 
	ACCTING_STATUS_CHC_ID, 
	ACCTING_SUBMITTED_BY_USERID, 
	ACCTING_SUBMITTED_DATE, 
	UW_STATUS_CHC_ID, 
	UW_SUBMITTED_BY_USERID, 
	UW_SUBMITTED_DATE, 
	AGENT_CORRESPONDENCE, 
	STAFFWARE_PKG_ID, 
	EXTRACT_DATE, 
	SOURCE_SYSTEM_ID
	FROM EXP_Values
),
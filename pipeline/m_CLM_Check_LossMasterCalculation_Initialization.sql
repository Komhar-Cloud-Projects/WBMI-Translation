WITH
SQ_wbmi_checkout AS (
	select checkout_type_code,
	t1.checkout_message
	from wbmi_checkout t1
	join
	(
	select a.WBMIChecksAndBalancingRuleId,
	max(a.Created_Date) max_date from wbmi_checkout a
	join WBMIChecksAndBalancingRule b
	on a.WBMIChecksAndBalancingRuleId=b.WBMIChecksAndBalancingRuleId
	where b.RuleLabel=@{pipeline().parameters.RULE_LABEL}
	group by a.WBMIChecksAndBalancingRuleId
	) t2
	on t1.WBMIChecksAndBalancingRuleId=t2.WBMIChecksAndBalancingRuleId
	and t1.created_date=t2.max_date
),
FIL_IssueCheck AS (
	SELECT
	checkout_type_code, 
	checkout_message
	FROM SQ_wbmi_checkout
	WHERE IN(checkout_type_code,'E')
),
EXP_Abort AS (
	SELECT
	checkout_type_code,
	-- *INF*: Abort('There are issues with the EDW data')
	Abort('There are issues with the EDW data'
	) AS error
	FROM FIL_IssueCheck
),
FIL_STOP_PROCESSING AS (
	SELECT
	error
	FROM EXP_Abort
	WHERE TRUE
),
wbmi_checkout_dummy_target AS (
	INSERT INTO wbmi_checkout
	(checkout_message)
	SELECT 
	error AS CHECKOUT_MESSAGE
	FROM FIL_STOP_PROCESSING
),
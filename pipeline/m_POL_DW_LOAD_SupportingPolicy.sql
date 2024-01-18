WITH
SQ_arch_pif_03_stage AS (
	with t(rownum) as
	(select 1 rownum
	union all
	select rownum+1 from t
	where rownum<100)
	select a.pif_symbol,a.pif_policy_number,a.pif_module,
	','+rtrim(a.comments_area)
	+case when right(rtrim(a.comments_area),1)<>',' and len(a.comments_area_2)>0 then ',' else '' end
	+rtrim(a.comments_area_2)+',' as comments_area,t.rownum
	from arch_pif_03_stage a
	join t
	on t.rownum between 1 and len(a.comments_area+isnull(a.comments_area_2,''))
	-len(replace(a.comments_area+isnull(a.comments_area_2,''),',',''))+1
	+case when right(rtrim(a.comments_area),1)<>',' and len(a.comments_area_2)>0 then 1 else 0 end 
	where a.comments_reason_suspended = 'ZS'
	and a.comments_area is not null
	and not exists (
	select 1 from arch_pif_03_stage b
	where b.arch_pif_03_stage_id>a.arch_pif_03_stage_id
	and a.pif_symbol=b.pif_symbol
	and a.pif_policy_number=b.pif_policy_number
	and a.pif_module=b.pif_module
	and b.comments_reason_suspended = 'ZS'
	and b.comments_area is not null)
	and not exists (
	select 1 from arch_pif_03_stage b
	where b.audit_id>a.audit_id
	and a.pif_symbol=b.pif_symbol
	and a.pif_policy_number=b.pif_policy_number
	and a.pif_module=b.pif_module)
	order by a.audit_id,a.arch_pif_03_stage_id,a.pif_symbol,a.pif_policy_number,a.pif_module,rownum
),
EXPTRANS AS (
	SELECT
	pif_symbol AS i_pif_symbol,
	pif_policy_number AS i_pif_policy_number,
	pif_module AS i_pif_module,
	comments_area AS i_comments_area,
	rownum AS i_rownum,
	-- *INF*: REG_REPLACE(i_comments_area, '[^A-Za-z0-9,]', '')
	REGEXP_REPLACE(i_comments_area, '[^A-Za-z0-9,]', '') AS v_comments_area,
	-- *INF*: INSTR(v_comments_area,',',1,i_rownum)+1
	REGEXP_INSTR(v_comments_area, ',', 1, i_rownum) + 1 AS v_start_pos,
	-- *INF*: INSTR(v_comments_area,',',1,i_rownum+1)-v_start_pos
	REGEXP_INSTR(v_comments_area, ',', 1, i_rownum + 1) - v_start_pos AS v_end_pos,
	-- *INF*: SUBSTR(v_comments_area,v_start_pos,v_end_pos)
	SUBSTR(v_comments_area, v_start_pos, v_end_pos) AS v_Parsed,
	-- *INF*: RTRIM(i_pif_symbol)||i_pif_policy_number||i_pif_module
	RTRIM(i_pif_symbol) || i_pif_policy_number || i_pif_module AS o_PolicyKey,
	-- *INF*: Substr(v_Parsed,1,1) ||  REG_REPLACE(Substr(v_Parsed,2), '[^0-9,]', '')
	Substr(v_Parsed, 1, 1) || REGEXP_REPLACE(Substr(v_Parsed, 2), '[^0-9,]', '') AS o_Parsed
	FROM SQ_arch_pif_03_stage
),
LKP_policy AS (
	SELECT
	pol_ak_id,
	pol_key
	FROM (
		SELECT a.pol_ak_id as pol_ak_id, a.pol_key as pol_key
		FROM V2.policy a
		join dbo.StrategicProfitCenter b
		on a.StrategicProfitCenterAKId=b.StrategicProfitCenterAKId
		and b.CurrentSnapshotFlag=1
		and b.StrategicProfitCenterAbbreviation='WB - PL'
		where a.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_ak_id) = 1
),
FILTRANS AS (
	SELECT
	LKP_policy.pol_ak_id AS PolicyAKId, 
	EXPTRANS.o_Parsed AS Parsed
	FROM EXPTRANS
	LEFT JOIN LKP_policy
	ON LKP_policy.pol_key = EXPTRANS.o_PolicyKey
	WHERE NOT ISNULL(PolicyAKId) AND LENGTH(Parsed)=7
),
AGGTRANS AS (
	SELECT
	PolicyAKId,
	Parsed
	FROM FILTRANS
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKId, Parsed ORDER BY NULL) = 1
),
EXPTRANS1 AS (
	SELECT
	PolicyAKId,
	Parsed
	FROM AGGTRANS
),
SQ_policy AS (
	DECLARE @FirstDayOfCurrMonth datetime
	
	set @FirstDayOfCurrMonth=DATEADD(month, DATEDIFF(month, 0, getdate()), 0)
	
	SELECT a.pol_ak_id as pol_ak_id,
	a.pol_num as pol_num,
	a.pol_key as pol_key
	FROM V2.policy a
	where not exists (
	select 1 from V2.policy b
	where a.pol_num=b.pol_num
	and b.crrnt_snpsht_flag=1
	and b.pol_mod>a.pol_mod and b.pol_eff_date < @FirstDayOfCurrMonth)
	and a.crrnt_snpsht_flag=1 and a.pol_eff_date < @FirstDayOfCurrMonth
),
JNRTRANS AS (SELECT
	SQ_policy.pol_ak_id, 
	SQ_policy.pol_num, 
	SQ_policy.pol_key, 
	EXPTRANS1.PolicyAKId, 
	EXPTRANS1.Parsed
	FROM SQ_policy
	INNER JOIN EXPTRANS1
	ON EXPTRANS1.Parsed = SQ_policy.pol_num
),
EXPTRANS2 AS (
	SELECT
	PolicyAKId,
	pol_ak_id AS SupportingPolicyAKId,
	pol_key AS SupportingPolicyKey,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	'PMS' AS SourceSystemId,
	SYSDATE AS CreatedDate,
	-- *INF*: TRUNC(SYSDATE,'MM')
	CAST(TRUNC(CURRENT_TIMESTAMP, 'MONTH') AS TIMESTAMP_NTZ(0)) AS RunDate,
	'loo' AS SupportingPolicyType
	FROM JNRTRANS
),
SupportingPolicy AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.SupportingPolicy;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SupportingPolicy
	(AuditId, SourceSystemId, CreatedDate, PolicyAKId, RunDate, SupportingPolicyAKId, SupportingPolicyKey, SupportingPolicyType)
	SELECT 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	POLICYAKID, 
	RUNDATE, 
	SUPPORTINGPOLICYAKID, 
	SUPPORTINGPOLICYKEY, 
	SUPPORTINGPOLICYTYPE
	FROM EXPTRANS2
),
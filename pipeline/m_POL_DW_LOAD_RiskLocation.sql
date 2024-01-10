WITH
LKP_SupCounty_ValidCheck AS (
	SELECT
	SupCountyId,
	StateAbbreviation,
	CountyName
	FROM (
		select a.SupCountyId as SupCountyId, 
		RIGHT('00'+b.state_abbrev,2) as StateAbbreviation,
		Upper(replace(a.CountyName,' ','')) as CountyName
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.SupCounty a
		join @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_state b
		on a.StateAbbreviation=b.state_code
		where a.CurrentSnapshotFlag=1
		and b.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StateAbbreviation,CountyName ORDER BY SupCountyId) = 1
),
SQ_pif_4514_stage AS (
	SELECT  DISTINCT RTRIM(pif_symbol) as pif_symbol,
	       pif_policy_number,
	       pif_module,
	       CASE LEN(sar_location_x) WHEN '0' THEN LTRIM(RTRIM(sar_unit)) ELSE LTRIM(RTRIM(sar_location_x)) END as sar_location_x,
		CASE LEN(sar_location_x) WHEN '0' THEN 'N'  ELSE 'Y' END as LocationIndicator,
	       LTRIM(RTRIM(sar_state)) as sar_state,
	       LTRIM(RTRIM(sar_loc_prov_territory)) as sar_loc_prov_territory,
	       CASE WHEN LEN(LTRIM(RTRIM(sar_county_first_two)) + LTRIM(RTRIM(sar_county_last_one)) + LTRIM(RTRIM(sar_city))) < 6 THEN '000000' ELSE 
	       LTRIM(RTRIM(sar_county_first_two)) + LTRIM(RTRIM(sar_county_last_one)) + LTRIM(RTRIM(sar_city)) END as Tax_Location,LTRIM(RTRIM(sar_code_14)) as sar_code_14,
	       LTRIM(RTRIM(sar_zip_postal_code)) as sar_zip_postal_code
	FROM  
	@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME_4514} A 
	@{pipeline().parameters.JOIN_CONDITION}
	(SELECT DISTINCT Policykey FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.work_policytransactionstatus 
	WHERE  AuditID = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AND  PolicyStatus <> 'NOCHANGE')  B
	ON  A.policykey = B.policykey
	WHERE A.logical_flag IN ('0','1','2','3')  
	@{pipeline().parameters.WHERE_CLAUSE}
	/*and exists (
	select 1 from rpt_edm_co.V2.policy p
	where p.pol_key=a.PolicyKey
	and p.crrnt_snpsht_flag='1')*/
),
EXP_Default AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module,
	pif_symbol  ||  pif_policy_number  || pif_module AS Policy_Key,
	sar_location_x,
	sar_unit AS locationIndicator,
	sar_state AS i_sar_state,
	-- *INF*: IIF(ISNULL(i_sar_state) OR IS_SPACES(i_sar_state) OR LENGTH(i_sar_state)=0, 'N/A', LPAD(LTRIM(RTRIM(i_sar_state)), 2, '0'))
	IFF(i_sar_state IS NULL OR IS_SPACES(i_sar_state) OR LENGTH(i_sar_state) = 0, 'N/A', LPAD(LTRIM(RTRIM(i_sar_state)), 2, '0')) AS o_sar_state,
	sar_loc_prov_territory,
	sar_city AS Tax_Location,
	-- *INF*: IIF(REG_MATCH(:UDF.DEFAULT_VALUE_FOR_STRINGS(Tax_Location) ,'(\d{6})')
	-- ,:UDF.DEFAULT_VALUE_FOR_STRINGS(Tax_Location)
	-- ,'000000')
	IFF(REG_MATCH(:UDF.DEFAULT_VALUE_FOR_STRINGS(Tax_Location), '(\d{6})'), :UDF.DEFAULT_VALUE_FOR_STRINGS(Tax_Location), '000000') AS v_Tax_Location,
	v_Tax_Location AS o_Tax_Location,
	-- *INF*: SUBSTR(v_Tax_Location,1,2)
	SUBSTR(v_Tax_Location, 1, 2) AS o_Tax_Location_1_2,
	sar_code_14,
	sar_zip_postal_code
	FROM SQ_pif_4514_stage
),
LKP_ISOFireProtectStage AS (
	SELECT
	City,
	County,
	TaxLoc
	FROM (
		select City as City,
		County as County,
		TaxLoc as TaxLoc
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ISOFireProtectStage where ISOExpDate='2999-12-31 00:00:00.000'
		order by ISOEffDate,ISOFireProtectStageID --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY TaxLoc ORDER BY City DESC) = 1
),
LKP_Pif43IXZWCUnmodStage AS (
	SELECT
	Pmdi4w1IntrastateIdNum,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	pif_risk_state_prov
	FROM (
		select distinct ltrim(rtrim(ix.PifSymbol)) as PifSymbol,
		ltrim(rtrim(ix.PifPolicyNumber)) as PifPolicyNumber,
		ltrim(rtrim(ix.PifModule)) as PifModule,
		ix.Pmdi4w1IntrastateIdNum AS Pmdi4w1IntrastateIdNum,
		RIGHT('00'+bp.pif_risk_state_prov,2) as pif_risk_state_prov
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43IXZWCUnmodStage ix, 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_02_stage bp
		where ltrim(rtrim(ix.PifSymbol)) = ltrim(rtrim(bp.pif_symbol))
		    and ltrim(rtrim(ix.PifPolicyNumber)) = ltrim(rtrim(bp.pif_policy_number))
		    and ltrim(rtrim(ix.PifModule)) = ltrim(rtrim(bp.pif_module))
		    and ix.Pmdi4w1WcRatingState = bp.pif_risk_state_prov
		and LTRIM(ix.Pmdi4w1IntrastateIdNum)<>''
		 and ix.Pmdi4w1SegmentPartCode = 'x'
		 and bp.pif_line_business in ('WC','WCP')
		 and cast(ix.Pmdi4w1YearProcess AS varchar)+
		RIGHT('0'+cast(ix.Pmdi4w1MonthProcess AS varchar),2)+RIGHT('0'+cast(ix.Pmdi4w1DayProcess as varchar),2)
		 +ix.Pmdi4w1SplitRateSeq = 
		 (
			select MAX(cast(Pmdi4w1YearProcess AS varchar)+
		RIGHT('0'+cast(Pmdi4w1MonthProcess AS varchar),2)+RIGHT('0'+cast(Pmdi4w1DayProcess as varchar),2)
		 +Pmdi4w1SplitRateSeq) from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43IXZWCUnmodStage a
			where ix.PifSymbol = a.PifSymbol
			and ix.PifPolicyNumber = a.PifPolicyNumber
			and ix.PifModule = a.PifModule
			and ltrim(a.Pmdi4w1IntrastateIdNum) <> ''
			and a.Pmdi4w1SegmentPartCode = 'x'
		      and ix.Pmdi4w1WcRatingState=a.Pmdi4w1WcRatingState
		 )
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule,pif_risk_state_prov ORDER BY Pmdi4w1IntrastateIdNum) = 1
),
LKP_Policy_PolicyAKID AS (
	SELECT
	pol_ak_id,
	pol_key
	FROM (
		SELECT policy.pol_ak_id as pol_ak_id, ltrim(rtrim(policy.pol_key)) as pol_key FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy
		WHERE crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_ak_id DESC) = 1
),
LKP_SupCounty_IL_IN_KY AS (
	SELECT
	CountyName,
	TaxLocationCountyCode,
	StateAbbreviation
	FROM (
		select Upper(a.CountyName) as CountyName,
		a.TaxLocationCountyCode as TaxLocationCountyCode,
		RIGHT('00'+b.state_abbrev,2) as StateAbbreviation
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.SupCounty a
		join @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_state b
		on a.StateAbbreviation=b.state_code
		where a.CurrentSnapshotFlag=1
		and b.crrnt_snpsht_flag=1
		and a.StateAbbreviation in ('IL','IN','KY')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY TaxLocationCountyCode,StateAbbreviation ORDER BY CountyName) = 1
),
LKP_pif_43jj_stage AS (
	SELECT
	pmd4j_address_line_1,
	pmd4j_addr_lin_2_pos_1,
	pmd4j_addr_lin_2_pos_2_30,
	pmd4j_address_line_3,
	pmd4j_address_line_4,
	pif_symbol,
	pif_policy_number,
	pif_module,
	pmd4j_location_number
	FROM (
		select SQ_LKP.pif_symbol  as pif_symbol ,
		SQ_LKP.pif_policy_number as pif_policy_number,
		SQ_LKP.pif_module as pif_module,
		SQ_LKP.pmd4j_location_number as pmd4j_location_number,
		SQ_LKP.pmd4j_address_line_1 as pmd4j_address_line_1,
		SQ_LKP.pmd4j_addr_lin_2_pos_1 as pmd4j_addr_lin_2_pos_1,
		SQ_LKP.pmd4j_addr_lin_2_pos_2_30 as pmd4j_addr_lin_2_pos_2_30,
		SQ_LKP.pmd4j_address_line_3 as pmd4j_address_line_3,
		SQ_LKP.pmd4j_address_line_4 as pmd4j_address_line_4
		from
		(select pif_symbol as pif_symbol,
		pif_policy_number as pif_policy_number,
		pif_module as pif_module,
		pmd4j_location_number as pmd4j_location_number,
		pmd4j_address_line_1 as pmd4j_address_line_1,
		pmd4j_addr_lin_2_pos_1 as pmd4j_addr_lin_2_pos_1,
		pmd4j_addr_lin_2_pos_2_30 as pmd4j_addr_lin_2_pos_2_30,
		pmd4j_address_line_3 as pmd4j_address_line_3,
		pmd4j_address_line_4 as pmd4j_address_line_4
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_43jj_stage
		where pmd4j_use_code='LOC' and source_system_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		
		union all
		
		select pif_symbol as pif_symbol,
		pif_policy_number as pif_policy_number,
		pif_module as pif_module,
		'0000' as pmd4j_location_number,
		pmd4j_address_line_1 as pmd4j_address_line_1,
		pmd4j_addr_lin_2_pos_1 as pmd4j_addr_lin_2_pos_1,
		pmd4j_addr_lin_2_pos_2_30 as pmd4j_addr_lin_2_pos_2_30,
		pmd4j_address_line_3 as pmd4j_address_line_3,
		pmd4j_address_line_4 as pmd4j_address_line_4
		from 
		(select pif_symbol,
		pif_policy_number,
		pif_module,
		pmd4j_address_line_1,
		pmd4j_addr_lin_2_pos_1,
		pmd4j_addr_lin_2_pos_2_30,
		pmd4j_address_line_3,
		pmd4j_address_line_4,
		ROW_NUMBER() over(partition by pif_symbol,pif_policy_number,pif_module order by pmd4j_location_number) as rn
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_43jj_stage
		where pmd4j_use_code='LOC' and source_system_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}' and CHARINDEX(',',pmd4j_addr_lin_2_pos_1+pmd4j_addr_lin_2_pos_2_30+pmd4j_address_line_3+pmd4j_address_line_4)>0
		) a
		where a.rn=1) SQ_LKP
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module,pmd4j_location_number ORDER BY pmd4j_address_line_1) = 1
),
EXP_Values AS (
	SELECT
	LKP_Policy_PolicyAKID.pol_ak_id,
	EXP_Default.Policy_Key,
	EXP_Default.sar_location_x,
	-- *INF*: LTRIM(RTRIM(sar_location_x))
	LTRIM(RTRIM(sar_location_x)) AS v_RiskLocation_Unit,
	v_RiskLocation_Unit AS RiskLocation_Unit,
	EXP_Default.o_sar_state AS sar_state,
	sar_state AS sar_state_out,
	EXP_Default.sar_loc_prov_territory,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sar_loc_prov_territory)
	-- 
	-- --IIF(ISNULL(sar_loc_prov_territory) OR IS_SPACES(sar_loc_prov_territory) OR LENGTH(sar_loc_prov_territory) = 0, 'N/A',
	-- -- LTRIM(RTRIM(sar_loc_prov_territory)))
	-- 
	-- 
	-- 
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sar_loc_prov_territory) AS sar_loc_prov_territory_Out,
	-- *INF*: LTRIM(RTRIM(sar_county_first_two))
	-- 
	-- --IIF(LTRIM(RTRIM(sar_county_first_two)) = '0' OR LENGTH(LTRIM(RTRIM(sar_county_first_two)))<2 OR IS_SPACES(sar_county_first_two), '00',LTRIM(RTRIM(sar_county_first_two)))
	LTRIM(RTRIM(sar_county_first_two)) AS v_sar_county_first_two,
	-- *INF*: LTRIM(RTRIM(sar_county_last_one))
	-- 
	-- --IIF(IS_SPACES(LTRIM(RTRIM(sar_county_last_one))) OR LENGTH(LTRIM(RTRIM(sar_county_last_one))) < 3 ,'000',LTRIM(RTRIM(sar_county_last_one)))
	LTRIM(RTRIM(sar_county_last_one)) AS v_sar_county_last_one,
	EXP_Default.o_Tax_Location AS Tax_Location,
	-- *INF*: LTRIM(RTRIM(Tax_Location))
	-- 
	-- --IIF(IS_SPACES(LTRIM(RTRIM(sar_city)))  OR ISNULL(LTRIM(RTRIM(sar_city))) OR LENGTH(LTRIM(RTRIM(sar_city))) < 3, '000', LTRIM(RTRIM(sar_city)))
	-- 
	-- 
	LTRIM(RTRIM(Tax_Location)) AS v_sar_city,
	Tax_Location AS Tax_Location_out,
	EXP_Default.sar_zip_postal_code,
	-- *INF*: IIF(ISNULL(sar_zip_postal_code)  OR IS_SPACES(sar_zip_postal_code)  OR LENGTH(sar_zip_postal_code) = 0 , 'N/A', LTRIM(RTRIM(sar_zip_postal_code)))
	IFF(sar_zip_postal_code IS NULL OR IS_SPACES(sar_zip_postal_code) OR LENGTH(sar_zip_postal_code) = 0, 'N/A', LTRIM(RTRIM(sar_zip_postal_code))) AS v_sar_zip_postal_code,
	v_sar_zip_postal_code AS sar_zip_postal_code_Out,
	0 AS logicalIndicator,
	Policy_Key  || v_RiskLocation_Unit AS RiskLocationKey,
	EXP_Default.locationIndicator,
	EXP_Default.sar_code_14,
	LKP_pif_43jj_stage.pmd4j_address_line_1 AS lkp_pmd4j_address_line_1,
	LKP_pif_43jj_stage.pmd4j_addr_lin_2_pos_1 AS lkp_pmd4j_addr_lin_2_pos_1,
	LKP_pif_43jj_stage.pmd4j_addr_lin_2_pos_2_30 AS lkp_pmd4j_addr_lin_2_pos_2_30,
	LKP_pif_43jj_stage.pmd4j_address_line_3 AS lkp_pmd4j_address_line_3,
	LKP_pif_43jj_stage.pmd4j_address_line_4 AS lkp_pmd4j_address_line_4,
	-- *INF*: IIF(ISNULL(lkp_pmd4j_address_line_1) OR IS_SPACES(lkp_pmd4j_address_line_1) OR LENGTH(lkp_pmd4j_address_line_1)=0,'',LTRIM(lkp_pmd4j_address_line_1))
	IFF(lkp_pmd4j_address_line_1 IS NULL OR IS_SPACES(lkp_pmd4j_address_line_1) OR LENGTH(lkp_pmd4j_address_line_1) = 0, '', LTRIM(lkp_pmd4j_address_line_1)) AS v_pmd4j_address_line_1,
	-- *INF*: IIF(ISNULL(lkp_pmd4j_addr_lin_2_pos_1) OR IS_SPACES(lkp_pmd4j_addr_lin_2_pos_1) OR LENGTH(lkp_pmd4j_addr_lin_2_pos_1)=0,'',LTRIM(lkp_pmd4j_addr_lin_2_pos_1))
	IFF(lkp_pmd4j_addr_lin_2_pos_1 IS NULL OR IS_SPACES(lkp_pmd4j_addr_lin_2_pos_1) OR LENGTH(lkp_pmd4j_addr_lin_2_pos_1) = 0, '', LTRIM(lkp_pmd4j_addr_lin_2_pos_1)) AS v_pmd4j_addr_lin_2_pos_1,
	-- *INF*: IIF(ISNULL(lkp_pmd4j_addr_lin_2_pos_2_30) OR IS_SPACES(lkp_pmd4j_addr_lin_2_pos_2_30) OR LENGTH(lkp_pmd4j_addr_lin_2_pos_2_30)=0,'',LTRIM(lkp_pmd4j_addr_lin_2_pos_2_30))
	IFF(lkp_pmd4j_addr_lin_2_pos_2_30 IS NULL OR IS_SPACES(lkp_pmd4j_addr_lin_2_pos_2_30) OR LENGTH(lkp_pmd4j_addr_lin_2_pos_2_30) = 0, '', LTRIM(lkp_pmd4j_addr_lin_2_pos_2_30)) AS v_pmd4j_addr_lin_2_pos_2_30,
	-- *INF*: IIF(ISNULL(lkp_pmd4j_address_line_3) OR IS_SPACES(lkp_pmd4j_address_line_3) OR LENGTH(lkp_pmd4j_address_line_3)=0,'',LTRIM(lkp_pmd4j_address_line_3))
	IFF(lkp_pmd4j_address_line_3 IS NULL OR IS_SPACES(lkp_pmd4j_address_line_3) OR LENGTH(lkp_pmd4j_address_line_3) = 0, '', LTRIM(lkp_pmd4j_address_line_3)) AS v_pmd4j_address_line_3,
	-- *INF*: IIF(ISNULL(lkp_pmd4j_address_line_4) OR IS_SPACES(lkp_pmd4j_address_line_4) OR LENGTH(lkp_pmd4j_address_line_4)=0,'',LTRIM(lkp_pmd4j_address_line_4))
	IFF(lkp_pmd4j_address_line_4 IS NULL OR IS_SPACES(lkp_pmd4j_address_line_4) OR LENGTH(lkp_pmd4j_address_line_4) = 0, '', LTRIM(lkp_pmd4j_address_line_4)) AS v_pmd4j_address_line_4,
	v_pmd4j_address_line_1||v_pmd4j_addr_lin_2_pos_1||v_pmd4j_addr_lin_2_pos_2_30||v_pmd4j_address_line_3||v_pmd4j_address_line_4 AS v_FullAddress,
	v_pmd4j_addr_lin_2_pos_1||v_pmd4j_addr_lin_2_pos_2_30||v_pmd4j_address_line_3||v_pmd4j_address_line_4 AS v_Address_RatingCounty,
	-- *INF*: LTRIM(RTRIM(IIF(INSTR(v_Address_RatingCounty,',',-1,2)=0,'N/A',
	-- SUBSTR(v_Address_RatingCounty,INSTR(v_Address_RatingCounty,',',-1,2)+1, INSTR(v_Address_RatingCounty,',',-1,1)-INSTR(v_Address_RatingCounty,',',-1,2)-1))))
	LTRIM(RTRIM(IFF(INSTR(v_Address_RatingCounty, ',', - 1, 2) = 0, 'N/A', SUBSTR(v_Address_RatingCounty, INSTR(v_Address_RatingCounty, ',', - 1, 2) + 1, INSTR(v_Address_RatingCounty, ',', - 1, 1) - INSTR(v_Address_RatingCounty, ',', - 1, 2) - 1)))) AS v_RatingCounty_pif43jj,
	-- *INF*: DECODE(TRUE,
	-- v_RatingCounty_pif43jj='ROCK ISLAND','ROCK',
	-- v_RatingCounty_pif43jj='ST LOUIS CITY','ST LOUIS',
	-- v_RatingCounty_pif43jj='SAINT LOUIS','ST LOUIS',
	-- v_RatingCounty_pif43jj='LACROSSE','LA CROSSE',
	-- v_RatingCounty_pif43jj='BLACKHAWK','BLACK HAWK',
	-- v_RatingCounty_pif43jj='OLMSTEAD','OLMSTED',
	-- v_RatingCounty_pif43jj='HENNIPEN','HENNEPIN',
	-- v_RatingCounty_pif43jj='OUTGAMIE','OUTAGAMIE',
	-- v_RatingCounty_pif43jj='MIWAUKEE','MILWAUKEE',
	-- v_RatingCounty_pif43jj='OTTERTAIL','OTTER TAIL',
	-- v_RatingCounty_pif43jj='TREMPELEAU','TREMPEALEAU',
	-- v_RatingCounty_pif43jj='WAUKEHSA','WAUKESHA',
	-- v_RatingCounty_pif43jj='SAINT CROIX','ST CROIX',
	-- IN(v_RatingCounty_pif43jj,'ADAM','ADMAS'),'ADAMS',
	-- IN(v_RatingCounty_pif43jj,'BARREN','BARRO'),'BARRON',
	-- v_RatingCounty_pif43jj
	-- )
	-- 
	-- --only add several data cleansing logic here, it should have more
	DECODE(TRUE,
		v_RatingCounty_pif43jj = 'ROCK ISLAND', 'ROCK',
		v_RatingCounty_pif43jj = 'ST LOUIS CITY', 'ST LOUIS',
		v_RatingCounty_pif43jj = 'SAINT LOUIS', 'ST LOUIS',
		v_RatingCounty_pif43jj = 'LACROSSE', 'LA CROSSE',
		v_RatingCounty_pif43jj = 'BLACKHAWK', 'BLACK HAWK',
		v_RatingCounty_pif43jj = 'OLMSTEAD', 'OLMSTED',
		v_RatingCounty_pif43jj = 'HENNIPEN', 'HENNEPIN',
		v_RatingCounty_pif43jj = 'OUTGAMIE', 'OUTAGAMIE',
		v_RatingCounty_pif43jj = 'MIWAUKEE', 'MILWAUKEE',
		v_RatingCounty_pif43jj = 'OTTERTAIL', 'OTTER TAIL',
		v_RatingCounty_pif43jj = 'TREMPELEAU', 'TREMPEALEAU',
		v_RatingCounty_pif43jj = 'WAUKEHSA', 'WAUKESHA',
		v_RatingCounty_pif43jj = 'SAINT CROIX', 'ST CROIX',
		IN(v_RatingCounty_pif43jj, 'ADAM', 'ADMAS'), 'ADAMS',
		IN(v_RatingCounty_pif43jj, 'BARREN', 'BARRO'), 'BARRON',
		v_RatingCounty_pif43jj) AS v_RatingCounty_pif43jj_special,
	-- *INF*: LTRIM(RTRIM(REPLACESTR(0,v_RatingCounty_pif43jj_special,'COUNTY','.','')))
	LTRIM(RTRIM(REPLACESTR(0, v_RatingCounty_pif43jj_special, 'COUNTY', '.', ''))) AS v_RatingCounty_pif43jj_format,
	LKP_SupCounty_IL_IN_KY.CountyName AS i_CountyName_IL_IN_KY,
	-- *INF*: UPPER(DECODE(TRUE,
	-- IN(sar_state,'12','13','16'),
	-- IIF(i_CountyName_IL_IN_KY='N/A',v_RatingCounty_pif43jj_format,i_CountyName_IL_IN_KY),
	-- v_RatingCounty_pif43jj_format
	-- ))
	-- 
	-- --IL,IN,KY
	UPPER(DECODE(TRUE,
		IN(sar_state, '12', '13', '16'), IFF(i_CountyName_IL_IN_KY = 'N/A', v_RatingCounty_pif43jj_format, i_CountyName_IL_IN_KY),
		v_RatingCounty_pif43jj_format)) AS v_RatingCounty,
	-- *INF*: IIF(ISNULL(:LKP.LKP_SUPCOUNTY_VALIDCHECK(sar_state,REPLACESTR(0,v_RatingCounty,' ',''))),'N/A',v_RatingCounty)
	IFF(LKP_SUPCOUNTY_VALIDCHECK_sar_state_REPLACESTR_0_v_RatingCounty.SupCountyId IS NULL, 'N/A', v_RatingCounty) AS v_RatingCounty_ValidCheck,
	-- *INF*: DECODE(TRUE,REG_MATCH(v_FullAddress,'.*\s\s[^,]*,[^,]*,[^,]*'),REG_EXTRACT(v_FullAddress,'(.*)\s\s([^,]*),([^,]*),([^,]*)',1),REG_MATCH(v_FullAddress,'.*\s\s[^,]*,[^,]*'),REG_EXTRACT(v_FullAddress,'(.*)\s\s([^,]*),([^,]*)',1),v_FullAddress)
	DECODE(TRUE,
		REG_MATCH(v_FullAddress, '.*\s\s[^,]*,[^,]*,[^,]*'), REG_EXTRACT(v_FullAddress, '(.*)\s\s([^,]*),([^,]*),([^,]*)', 1),
		REG_MATCH(v_FullAddress, '.*\s\s[^,]*,[^,]*'), REG_EXTRACT(v_FullAddress, '(.*)\s\s([^,]*),([^,]*)', 1),
		v_FullAddress) AS v_AlphaStreetAddress,
	-- *INF*: RTRIM(REG_REPLACE( v_AlphaStreetAddress, '\s+', ' '))
	RTRIM(REG_REPLACE(v_AlphaStreetAddress, '\s+', ' ')) AS v_StreetAddress,
	-- *INF*: LTRIM(RTRIM(DECODE(TRUE,REG_MATCH(v_FullAddress,'.*\s\s[^,]*,[^,]*,[^,]*'),REG_EXTRACT(v_FullAddress,'(.*)\s\s([^,]*),([^,]*),([^,]*)',2),REG_MATCH(v_FullAddress,'.*\s\s[^,]*,[^,]*'),REG_EXTRACT(v_FullAddress,'(.*)\s\s([^,]*),([^,]*)',2),'N/A')))
	LTRIM(RTRIM(DECODE(TRUE,
		REG_MATCH(v_FullAddress, '.*\s\s[^,]*,[^,]*,[^,]*'), REG_EXTRACT(v_FullAddress, '(.*)\s\s([^,]*),([^,]*),([^,]*)', 2),
		REG_MATCH(v_FullAddress, '.*\s\s[^,]*,[^,]*'), REG_EXTRACT(v_FullAddress, '(.*)\s\s([^,]*),([^,]*)', 2),
		'N/A'))) AS v_RatingCity,
	LKP_ISOFireProtectStage.City AS lkp_ISOFireProtectCity,
	LKP_ISOFireProtectStage.County AS lkp_ISOFireProtectCounty,
	-- *INF*: IIF(v_RatingCity='','N/A',v_RatingCity)
	IFF(v_RatingCity = '', 'N/A', v_RatingCity) AS o_RatingCity,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_RatingCounty_ValidCheck)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_RatingCounty_ValidCheck) AS o_RatingCounty,
	-- *INF*: IIF(ISNULL(sar_code_14) OR IS_SPACES(sar_code_14) OR LENGTH(sar_code_14)=0 OR sar_state<>'16','N/A',sar_code_14)
	IFF(sar_code_14 IS NULL OR IS_SPACES(sar_code_14) OR LENGTH(sar_code_14) = 0 OR sar_state <> '16', 'N/A', sar_code_14) AS o_KYTaxCode,
	-- *INF*: IIF(v_StreetAddress='','N/A',v_StreetAddress)
	IFF(v_StreetAddress = '', 'N/A', v_StreetAddress) AS o_StreetAddress,
	-- *INF*: IIF(ISNULL(lkp_ISOFireProtectCity),'N/A',ltrim(rtrim(lkp_ISOFireProtectCity)))
	IFF(lkp_ISOFireProtectCity IS NULL, 'N/A', ltrim(rtrim(lkp_ISOFireProtectCity))) AS o_ISOFireProtectCity,
	-- *INF*: IIF(ISNULL(lkp_ISOFireProtectCounty),'N/A',ltrim(rtrim(lkp_ISOFireProtectCounty)))
	IFF(lkp_ISOFireProtectCounty IS NULL, 'N/A', ltrim(rtrim(lkp_ISOFireProtectCounty))) AS o_ISOFireProtectCounty,
	LKP_Pif43IXZWCUnmodStage.Pmdi4w1IntrastateIdNum AS i_Pmdi4w1IntrastateIdNum,
	-- *INF*: IIF(ISNULL(i_Pmdi4w1IntrastateIdNum),'N/A',i_Pmdi4w1IntrastateIdNum)
	IFF(i_Pmdi4w1IntrastateIdNum IS NULL, 'N/A', i_Pmdi4w1IntrastateIdNum) AS o_IntrastateRiskId
	FROM EXP_Default
	LEFT JOIN LKP_ISOFireProtectStage
	ON LKP_ISOFireProtectStage.TaxLoc = EXP_Default.o_Tax_Location
	LEFT JOIN LKP_Pif43IXZWCUnmodStage
	ON LKP_Pif43IXZWCUnmodStage.PifSymbol = EXP_Default.pif_symbol AND LKP_Pif43IXZWCUnmodStage.PifPolicyNumber = EXP_Default.pif_policy_number AND LKP_Pif43IXZWCUnmodStage.PifModule = EXP_Default.pif_module AND LKP_Pif43IXZWCUnmodStage.pif_risk_state_prov = EXP_Default.o_sar_state
	LEFT JOIN LKP_Policy_PolicyAKID
	ON LKP_Policy_PolicyAKID.pol_key = EXP_Default.Policy_Key
	LEFT JOIN LKP_SupCounty_IL_IN_KY
	ON LKP_SupCounty_IL_IN_KY.TaxLocationCountyCode = EXP_Default.o_Tax_Location_1_2 AND LKP_SupCounty_IL_IN_KY.StateAbbreviation = EXP_Default.o_sar_state
	LEFT JOIN LKP_pif_43jj_stage
	ON LKP_pif_43jj_stage.pif_symbol = EXP_Default.pif_symbol AND LKP_pif_43jj_stage.pif_policy_number = EXP_Default.pif_policy_number AND LKP_pif_43jj_stage.pif_module = EXP_Default.pif_module AND LKP_pif_43jj_stage.pmd4j_location_number = EXP_Default.sar_location_x
	LEFT JOIN LKP_SUPCOUNTY_VALIDCHECK LKP_SUPCOUNTY_VALIDCHECK_sar_state_REPLACESTR_0_v_RatingCounty
	ON LKP_SUPCOUNTY_VALIDCHECK_sar_state_REPLACESTR_0_v_RatingCounty.StateAbbreviation = sar_state
	AND LKP_SUPCOUNTY_VALIDCHECK_sar_state_REPLACESTR_0_v_RatingCounty.CountyName = REPLACESTR(0, v_RatingCounty, ' ', '')

),
LKP_RiskLocation_RiskLocationAKID AS (
	SELECT
	RiskLocationAKID,
	RiskLocationID,
	CurrentSnapshotFlag,
	PolicyAKID,
	LocationUnitNumber,
	RiskTerritory,
	StateProvinceCode,
	ZipPostalCode,
	TaxLocation
	FROM (
		SELECT RiskLocationAKID   AS RiskLocationAKID,
		       PolicyAKID         AS PolicyAKID,
		LOC.RiskLocationID as RiskLocationID,
		LOC.CurrentSnapshotFlag AS CurrentSnapshotFlag,
		       LTRIM(RTRIM(LocationUnitNumber)) AS LocationUnitNumber,
		       LTRIM(RTRIM(RiskTerritory))      AS RiskTerritory,
		       LTRIM(RTRIM(StateProvinceCode))  AS StateProvinceCode,
		       LTRIM(RTRIM(ZipPostalCode))      AS ZipPostalCode,
		       LTRIM(RTRIM(TaxLocation))        AS TaxLocation
		FROM	@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation LOC
		INNER JOIN  @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Policy POL
		ON	LOC.PolicyAKID = POL.pol_ak_id
		WHERE POL.crrnt_snpsht_flag = 1 AND LOC.CurrentSnapshotFlag =1
		       AND POL.SOURCE_SYS_ID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		       AND  EXISTS  (SELECT DISTINCT PolicyKey FROM  
									@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPolicyKeyList
									WHERE POL.pol_key = PolicyKey AND @{pipeline().parameters.WHERE_CLAUSE_EDW})
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID,LocationUnitNumber,RiskTerritory,StateProvinceCode,ZipPostalCode,TaxLocation ORDER BY RiskLocationAKID DESC) = 1
),
LKP_Sup_State AS (
	SELECT
	sup_state_id,
	state_abbrev
	FROM (
		SELECT 
			sup_state_id,
			state_abbrev
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_state
		WHERE source_sys_id = 'EXCEED' AND crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_abbrev ORDER BY sup_state_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_RiskLocation_RiskLocationAKID.RiskLocationID,
	LKP_RiskLocation_RiskLocationAKID.CurrentSnapshotFlag AS LKP_CurrentSnapshotFlag,
	-- *INF*: Decode(LKP_CurrentSnapshotFlag,'T','1','F','0',LKP_CurrentSnapshotFlag)
	Decode(LKP_CurrentSnapshotFlag,
		'T', '1',
		'F', '0',
		LKP_CurrentSnapshotFlag) AS v_LKP_CurrentSnapshotFlag,
	LKP_RiskLocation_RiskLocationAKID.RiskLocationAKID,
	EXP_Values.pol_ak_id,
	EXP_Values.RiskLocation_Unit,
	EXP_Values.sar_state_out AS sar_state,
	EXP_Values.sar_loc_prov_territory_Out AS sar_loc_prov_territory,
	EXP_Values.Tax_Location_out AS Tax_Location,
	EXP_Values.sar_zip_postal_code_Out AS sar_zip_postal_code,
	EXP_Values.logicalIndicator,
	EXP_Values.RiskLocationKey,
	'1' AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	-- *INF*: IIF(ISNULL(RiskLocationAKID) , 'NEW',IIF(v_LKP_CurrentSnapshotFlag='0','REACTIVATE','EXISTS'))
	-- 
	-- 
	-- --IIF(ISNULL(RiskLocationAKID) OR (NOT ISNULL(RiskLocationAKID) and v_LKP_CurrentSnapshotFlag='0'), 'NEW','EXISTS')
	IFF(RiskLocationAKID IS NULL, 'NEW', IFF(v_LKP_CurrentSnapshotFlag = '0', 'REACTIVATE', 'EXISTS')) AS Changed_Flag,
	EXP_Values.locationIndicator,
	LKP_Sup_State.sup_state_id,
	EXP_Values.o_RatingCity AS RatingCity,
	EXP_Values.o_RatingCounty AS RatingCounty,
	EXP_Values.o_KYTaxCode AS KYTaxCode,
	EXP_Values.o_StreetAddress AS StreetAddress,
	-- *INF*: MD5(StreetAddress||RatingCity||sar_state||sar_zip_postal_code||Tax_Location)
	MD5(StreetAddress || RatingCity || sar_state || sar_zip_postal_code || Tax_Location) AS o_RiskLocationHashKey,
	EXP_Values.o_ISOFireProtectCity AS ISOFireProtectCity,
	EXP_Values.o_ISOFireProtectCounty AS ISOFireProtectCounty,
	EXP_Values.o_IntrastateRiskId AS IntrastateRiskId
	FROM EXP_Values
	LEFT JOIN LKP_RiskLocation_RiskLocationAKID
	ON LKP_RiskLocation_RiskLocationAKID.PolicyAKID = EXP_Values.pol_ak_id AND LKP_RiskLocation_RiskLocationAKID.LocationUnitNumber = EXP_Values.RiskLocation_Unit AND LKP_RiskLocation_RiskLocationAKID.RiskTerritory = EXP_Values.sar_loc_prov_territory_Out AND LKP_RiskLocation_RiskLocationAKID.StateProvinceCode = EXP_Values.sar_state_out AND LKP_RiskLocation_RiskLocationAKID.ZipPostalCode = EXP_Values.sar_zip_postal_code_Out AND LKP_RiskLocation_RiskLocationAKID.TaxLocation = EXP_Values.Tax_Location_out
	LEFT JOIN LKP_Sup_State
	ON LKP_Sup_State.state_abbrev = EXP_Values.sar_state_out
),
RTR_New_ReActivate_Exists AS (
	SELECT
	RiskLocationID,
	RiskLocationAKID,
	pol_ak_id,
	RiskLocation_Unit,
	sar_state,
	sar_loc_prov_territory,
	Tax_Location,
	sar_zip_postal_code,
	logicalIndicator,
	RiskLocationKey,
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	Changed_Flag,
	locationIndicator,
	sup_state_id,
	RatingCity,
	RatingCounty,
	KYTaxCode,
	StreetAddress,
	o_RiskLocationHashKey,
	ISOFireProtectCity,
	ISOFireProtectCounty,
	IntrastateRiskId
	FROM EXP_Detect_Changes
),
RTR_New_ReActivate_Exists_New AS (SELECT * FROM RTR_New_ReActivate_Exists WHERE Changed_Flag='NEW'),
RTR_New_ReActivate_Exists_ReActivate AS (SELECT * FROM RTR_New_ReActivate_Exists WHERE Changed_Flag='REACTIVATE'),
RTR_New_ReActivate_Exists_Exists AS (SELECT * FROM RTR_New_ReActivate_Exists WHERE Changed_Flag='EXISTS'),
EXP_Update_DataCollect AS (
	SELECT
	RiskLocationID,
	'1' AS CurrentSnapshotFlag,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate
	FROM RTR_New_ReActivate_Exists_ReActivate
),
UPD_RiskLocation AS (
	SELECT
	RiskLocationID, 
	CurrentSnapshotFlag, 
	ExpirationDate
	FROM EXP_Update_DataCollect
),
TGT_RiskLocation_Update AS (

	------------ PRE SQL ----------
	exec [spSetIndexStatus] @Enable = 0, @Schema = 'dbo', @TableName = 'RiskLocation', @IndexWildcard = 'Ak1RiskLocation'
	-------------------------------


	MERGE INTO RiskLocation AS T
	USING UPD_RiskLocation AS S
	ON T.RiskLocationID = S.RiskLocationID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate

	------------ POST SQL ----------
	exec [spSetIndexStatus] @Enable = 1, @Schema = 'dbo', @TableName = 'RiskLocation', @IndexWildcard = 'Ak1RiskLocation'
	-------------------------------


),
AGG_RemoveDuplicates AS (
	SELECT
	RiskLocationAKID,
	pol_ak_id,
	RiskLocation_Unit,
	sar_state,
	sar_loc_prov_territory AS sar_loc_prov_territory_Out,
	Tax_Location AS Tax_Location_Out,
	sar_zip_postal_code AS sar_zip_postal_code_Out,
	logicalIndicator,
	RiskLocationKey,
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	locationIndicator,
	sup_state_id,
	RatingCity,
	RatingCounty,
	KYTaxCode,
	StreetAddress,
	o_RiskLocationHashKey AS RiskLocationHashKey,
	ISOFireProtectCity,
	ISOFireProtectCounty,
	IntrastateRiskId
	FROM RTR_New_ReActivate_Exists_New
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id, RiskLocation_Unit, sar_state, sar_loc_prov_territory_Out, Tax_Location_Out, sar_zip_postal_code_Out, KYTaxCode ORDER BY NULL) = 1
),
SEQ_RiskLocationAKID AS (
	CREATE SEQUENCE SEQ_RiskLocationAKID
	START = 0
	INCREMENT = 1;
),
EXP_Detemine_AK_ID AS (
	SELECT
	SEQ_RiskLocationAKID.NEXTVAL,
	NEXTVAL AS RiskLocationAKID_Out,
	pol_ak_id,
	RiskLocation_Unit,
	sar_state,
	sar_loc_prov_territory_Out,
	Tax_Location_Out,
	sar_zip_postal_code_Out,
	logicalIndicator,
	RiskLocationKey,
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	locationIndicator,
	sup_state_id,
	RatingCity,
	RatingCounty,
	KYTaxCode,
	StreetAddress,
	RiskLocationHashKey,
	ISOFireProtectCity,
	ISOFireProtectCounty,
	IntrastateRiskId
	FROM AGG_RemoveDuplicates
),
TGT_RiskLocation_Insert AS (

	------------ PRE SQL ----------
	exec [spSetIndexStatus] @Enable = 0, @Schema = 'dbo', @TableName = 'RiskLocation', @IndexWildcard = 'Ak1RiskLocation'
	-------------------------------


	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, LogicalIndicator, RiskLocationAKID, PolicyAKID, RiskLocationKey, LocationUnitNumber, LocationIndicator, RiskTerritory, StateProvinceCode, ZipPostalCode, TaxLocation, sup_state_id, RatingCity, RatingCounty, TaxCode, RiskLocationHashKey, StreetAddress, ISOFireProtectCity, ISOFireProtectCounty, IntrastateRiskId)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	logicalIndicator AS LOGICALINDICATOR, 
	RiskLocationAKID_Out AS RISKLOCATIONAKID, 
	pol_ak_id AS POLICYAKID, 
	RISKLOCATIONKEY, 
	RiskLocation_Unit AS LOCATIONUNITNUMBER, 
	locationIndicator AS LOCATIONINDICATOR, 
	sar_loc_prov_territory_Out AS RISKTERRITORY, 
	sar_state AS STATEPROVINCECODE, 
	sar_zip_postal_code_Out AS ZIPPOSTALCODE, 
	Tax_Location_Out AS TAXLOCATION, 
	SUP_STATE_ID, 
	RATINGCITY, 
	RATINGCOUNTY, 
	KYTaxCode AS TAXCODE, 
	RISKLOCATIONHASHKEY, 
	STREETADDRESS, 
	ISOFIREPROTECTCITY, 
	ISOFIREPROTECTCOUNTY, 
	INTRASTATERISKID
	FROM EXP_Detemine_AK_ID

	------------ POST SQL ----------
	exec [spSetIndexStatus] @Enable = 1, @Schema = 'dbo', @TableName = 'RiskLocation', @IndexWildcard = 'Ak1RiskLocation'
	-------------------------------


),
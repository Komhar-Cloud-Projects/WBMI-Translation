WITH
SQ_WorkPLLocation AS (
	select distinct P.PolicySymbol,
	P.PolicyNumber,
	P.PolicyVersion,
	substring(REPLACE(Addresskey,P.Policykey+'||',''),1,charindex('|',REPLACE(Addresskey,P.Policykey+'||','') ,1)-1) Locationid,
	ISNULL(L.StreetAddressLine1,'N/A') StreetAddressLine1,
	ISNULL(L.CityName,'N/A') CityName,
	ISNULL(L.CountyName,'N/A') CountyName,
	ISNULL(L.StateUspsCode,'N/A') StateUspsCode,
	ISNULL(L.TerritoryCode,'N/A') TerritoryCode,
	ISNULL(L.PostalCode,'N/A') PostalCode
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLPolicy P
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLLocation L
	on P.PolicyKey=L.PolicyKey
	and P.StartDate=L.StartDate
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLCoverage C
	on P.PolicyKey=C.PolicyKey
	and P.StartDate=C.StartDate
	and L.AddressKey=C.RiskAddressKey
	and not exists(select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLPolicy P2 where P2.LineageId=P.LineageId and P2.PolicyStatusKey='ClaimFreeAward')
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION
	
	select distinct P.PolicySymbol,
	P.PolicyNumber,
	P.PolicyVersion,
	'' Locationid,
	ISNULL(PT.StreetAddressLine1,'N/A') StreetAddressLine1,
	ISNULL(PT.CityName,'N/A') CityName,
	ISNULL(PT.CountyName,'N/A') CountyName,
	ISNULL(PT.StateName,'N/A') StateUspsCode,
	'N/A' TerritoryCode,
	ISNULL(PT.PostalCode,'N/A') PostalCode
	from 
	(select *,case when AddressType='Insured MailingAddress' then 1 else 2 end Customer_Record
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLParty 
	where AddressType in ('Insured MailingAddress','Insured InsuredsAddress')) PT
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLPolicy P
	on PT.PolicyKey=P.PolicyKey
	and PT.StartDate=P.StartDate
	and not exists(select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLPolicy P2 where P2.LineageId=P.LineageId and P2.PolicyStatusKey='ClaimFreeAward')
	inner join (select PolicyKey,StartDate,max(case when AddressType='Insured MailingAddress' then 1 else 2 end) Customer_Record
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLParty P
	where AddressType in ('Insured MailingAddress','Insured InsuredsAddress')
	group by PolicyKey,StartDate) B
	on PT.Policykey=B.POlicykey
	and PT.StartDate=B.STartDate
	and PT.Customer_Record=B.Customer_Record
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Src_DataCollect AS (
	SELECT
	PolicySymbol,
	PolicyNumber,
	PolicyVersion,
	-- *INF*: PolicyNumber || IIF(ISNULL(ltrim(rtrim(PolicyVersion))) or Length(ltrim(rtrim(PolicyVersion)))=0 or IS_SPACES(PolicyVersion),'00',PolicyVersion)
	PolicyNumber || IFF(ltrim(rtrim(PolicyVersion)) IS NULL OR Length(ltrim(rtrim(PolicyVersion))) = 0 OR IS_SPACES(PolicyVersion), '00', PolicyVersion) AS v_PolicyKey,
	v_PolicyKey AS o_Policykey,
	Locationid,
	'0000' AS v_LocationNumber,
	v_LocationNumber AS o_LocationNumber,
	v_PolicyKey || '|' || Locationid || '|' ||v_LocationNumber AS o_RiskLocationKey,
	StreetAddressLine1,
	CityName,
	CountyName,
	StateUspsCode,
	TerritoryCode,
	PostalCode,
	'N/A' AS o_TaxLocation,
	'N/A' AS o_TaxCode,
	'N/A' AS o_ISOFireProtectCity,
	'N/A' AS o_IntrastateRiskID
	FROM SQ_WorkPLLocation
),
LKP_RiskLocation AS (
	SELECT
	RiskLocationAKID,
	RiskLocationKey,
	LocationIndicator,
	TaxLocation,
	sup_state_id,
	TaxCode,
	RiskLocationHashKey,
	IntrastateRiskId,
	ISOFireProtectCity,
	LocationUnitNumber,
	RiskTerritory
	FROM (
		SELECT 
			RiskLocationAKID,
			RiskLocationKey,
			LocationIndicator,
			TaxLocation,
			sup_state_id,
			TaxCode,
			RiskLocationHashKey,
			IntrastateRiskId,
			ISOFireProtectCity,
			LocationUnitNumber,
			RiskTerritory
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation
		WHERE CurrentSnapshotFlag = 1 and SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and
		PolicyAKId in (
		select pol_ak_id from @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol
		where exists (
		select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLPolicy WCT
		where WCT.PolicyNumber=pol.pol_num
		and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol.pol_mod)
		and pol.crrnt_snpsht_flag=1)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationKey ORDER BY RiskLocationAKID) = 1
),
LKP_SupState AS (
	SELECT
	sup_state_id,
	state_abbrev,
	state_code
	FROM (
		SELECT 
			sup_state_id,
			state_abbrev,
			state_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_state
		WHERE crrnt_snpsht_flag = 1 and source_sys_id = 'EXCEED'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code ORDER BY sup_state_id) = 1
),
LKP_policy AS (
	SELECT
	pol_ak_id,
	pol_key
	FROM (
		SELECT 
			pol_ak_id,
			pol_key
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy
		WHERE crrnt_snpsht_flag='1' and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and exists (
		select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLPolicy WCT
		where WCT.PolicyNumber=pol_num
		and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol_mod)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_ak_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_RiskLocation.RiskLocationAKID AS lkp_RiskLocationAKID,
	LKP_RiskLocation.LocationIndicator AS lkp_LocationIndicator,
	LKP_RiskLocation.TaxLocation AS lkp_TaxLocation,
	LKP_RiskLocation.sup_state_id AS lkp_sup_state_id,
	LKP_RiskLocation.TaxCode AS lkp_KYTaxCode,
	LKP_RiskLocation.RiskLocationHashKey AS lkp_RiskLocationHashKey,
	LKP_RiskLocation.IntrastateRiskId AS lkp_IntrastateRiskId,
	LKP_RiskLocation.ISOFireProtectCity AS lkp_ISOFireProtectCity,
	LKP_RiskLocation.LocationUnitNumber AS lkp_LocationUnitNumber,
	LKP_RiskLocation.RiskTerritory AS lkp_RiskTerritory,
	LKP_RiskLocation.RiskLocationKey AS lkp_RiskLocationKey,
	LKP_SupState.sup_state_id AS i_sup_state_id,
	LKP_SupState.state_abbrev AS i_state_abbrev,
	0 AS v_LogicalIndicator,
	EXP_Src_DataCollect.o_RiskLocationKey AS i_RiskLocationKey,
	EXP_Src_DataCollect.o_LocationNumber AS i_LocationUnitNumber,
	'N' AS v_LocationIndicator,
	EXP_Src_DataCollect.TerritoryCode AS i_RiskTerritory,
	EXP_Src_DataCollect.PostalCode AS i_ZipPostalCode,
	EXP_Src_DataCollect.o_TaxLocation AS i_TaxLocation,
	EXP_Src_DataCollect.CityName AS i_RatingCity,
	EXP_Src_DataCollect.CountyName AS i_RatingCounty,
	EXP_Src_DataCollect.o_TaxCode AS i_TaxCode,
	EXP_Src_DataCollect.StreetAddressLine1 AS i_Address1,
	EXP_Src_DataCollect.o_ISOFireProtectCity AS i_ISOFireProtectCity,
	-- *INF*: IIF(i_state_abbrev='12' and NOT ISNULL(i_ISOFireProtectCity),i_ISOFireProtectCity,'N/A')
	IFF(i_state_abbrev = '12' AND NOT i_ISOFireProtectCity IS NULL, i_ISOFireProtectCity, 'N/A') AS v_ISOFireProtectCity,
	-- *INF*: IIF(ISNULL(i_state_abbrev), 'N/A', i_state_abbrev)
	IFF(i_state_abbrev IS NULL, 'N/A', i_state_abbrev) AS v_StateProvinceCode,
	-- *INF*: MD5(i_Address1||i_RatingCity||v_StateProvinceCode||i_ZipPostalCode)
	MD5(i_Address1 || i_RatingCity || v_StateProvinceCode || i_ZipPostalCode) AS v_RiskLocationHashKey,
	-- *INF*:   IIF(ISNULL(lkp_RiskLocationAKID), 'NEW', 
	-- IIF(
	-- LTRIM(RTRIM(lkp_LocationIndicator)) != LTRIM(RTRIM(v_LocationIndicator)) OR LTRIM(RTRIM(lkp_LocationUnitNumber)) != LTRIM(RTRIM(i_LocationUnitNumber)) OR LTRIM(RTRIM(lkp_RiskTerritory)) != LTRIM(RTRIM(i_RiskTerritory)) OR
	-- LTRIM(RTRIM(lkp_TaxLocation)) != LTRIM(RTRIM(i_TaxLocation)) OR lkp_sup_state_id != i_sup_state_id OR LTRIM(RTRIM(lkp_KYTaxCode)) != LTRIM(RTRIM(i_TaxCode)) OR LTRIM(RTRIM(lkp_RiskLocationHashKey)) != LTRIM(RTRIM(v_RiskLocationHashKey))  OR LTRIM(RTRIM(lkp_IntrastateRiskId)) != LTRIM(RTRIM(IntrastateRiskID)) OR LTRIM(RTRIM(lkp_ISOFireProtectCity)) != LTRIM(RTRIM(v_ISOFireProtectCity)) ,
	-- 'UPDATE', 'NOCHANGE'))
	IFF(lkp_RiskLocationAKID IS NULL, 'NEW', IFF(LTRIM(RTRIM(lkp_LocationIndicator)) != LTRIM(RTRIM(v_LocationIndicator)) OR LTRIM(RTRIM(lkp_LocationUnitNumber)) != LTRIM(RTRIM(i_LocationUnitNumber)) OR LTRIM(RTRIM(lkp_RiskTerritory)) != LTRIM(RTRIM(i_RiskTerritory)) OR LTRIM(RTRIM(lkp_TaxLocation)) != LTRIM(RTRIM(i_TaxLocation)) OR lkp_sup_state_id != i_sup_state_id OR LTRIM(RTRIM(lkp_KYTaxCode)) != LTRIM(RTRIM(i_TaxCode)) OR LTRIM(RTRIM(lkp_RiskLocationHashKey)) != LTRIM(RTRIM(v_RiskLocationHashKey)) OR LTRIM(RTRIM(lkp_IntrastateRiskId)) != LTRIM(RTRIM(IntrastateRiskID)) OR LTRIM(RTRIM(lkp_ISOFireProtectCity)) != LTRIM(RTRIM(v_ISOFireProtectCity)), 'UPDATE', 'NOCHANGE')) AS v_Changed_Flag,
	v_Changed_Flag AS o_Changed_Flag,
	lkp_RiskLocationAKID AS o_RiskLocationAKID,
	v_LogicalIndicator AS o_LogicalIndicator,
	LKP_policy.pol_ak_id AS lkp_PolicyAKID,
	-- *INF*: IIF(ISNULL(lkp_PolicyAKID),-1,lkp_PolicyAKID)
	IFF(lkp_PolicyAKID IS NULL, - 1, lkp_PolicyAKID) AS o_PolicyAkid,
	-- *INF*: IIF(ISNULL(lkp_RiskLocationAKID),i_RiskLocationKey,lkp_RiskLocationKey)
	IFF(lkp_RiskLocationAKID IS NULL, i_RiskLocationKey, lkp_RiskLocationKey) AS o_RiskLocationKey,
	i_LocationUnitNumber AS o_LocationUnitNumber,
	v_LocationIndicator AS o_LocationIndicator,
	i_RiskTerritory AS o_RiskTerritory,
	v_StateProvinceCode AS o_StateProvinceCode,
	i_ZipPostalCode AS o_ZipPostalCode,
	i_TaxLocation AS o_TaxLocation,
	i_sup_state_id AS o_sup_state_id,
	i_RatingCity AS o_RatingCity,
	i_RatingCounty AS o_RatingCounty,
	i_TaxCode AS o_TaxCode,
	v_RiskLocationHashKey AS o_RiskLocationHashKey,
	i_Address1 AS o_Address1,
	v_ISOFireProtectCity AS o_ISOFireProtectCity,
	EXP_Src_DataCollect.o_IntrastateRiskID AS IntrastateRiskID
	FROM EXP_Src_DataCollect
	LEFT JOIN LKP_RiskLocation
	ON LKP_RiskLocation.RiskLocationKey = EXP_Src_DataCollect.o_RiskLocationKey
	LEFT JOIN LKP_SupState
	ON LKP_SupState.state_code = EXP_Src_DataCollect.StateUspsCode
	LEFT JOIN LKP_policy
	ON LKP_policy.pol_key = EXP_Src_DataCollect.o_Policykey
),
FIL_Insert AS (
	SELECT
	o_Changed_Flag AS Changed_Flag, 
	o_RiskLocationAKID AS RiskLocationAKID, 
	o_LogicalIndicator AS LogicalIndicator, 
	o_PolicyAkid AS PolicyAKID, 
	o_RiskLocationKey AS RiskLocationKey, 
	o_LocationUnitNumber AS LocationUnitNumber, 
	o_LocationIndicator AS LocationIndicator, 
	o_RiskTerritory AS RiskTerritory, 
	o_StateProvinceCode AS StateProvinceCode, 
	o_ZipPostalCode AS ZipPostalCode, 
	o_TaxLocation AS TaxLocation, 
	o_sup_state_id AS sup_state_id, 
	o_RatingCity AS RatingCity, 
	o_RatingCounty AS RatingCounty, 
	o_TaxCode AS TaxCode, 
	o_RiskLocationHashKey AS RiskLocationHashKey, 
	o_Address1 AS Address1, 
	IntrastateRiskID, 
	o_ISOFireProtectCity AS ISOFireProtectCity
	FROM EXP_Detect_Changes
	WHERE (Changed_Flag='NEW' OR Changed_Flag='UPDATE')
),
SEQ_RiskLocationAKID AS (
	CREATE SEQUENCE SEQ_RiskLocationAKID
	START = 0
	INCREMENT = 1;
),
EXP_Detemine_AK_ID AS (
	SELECT
	SEQ_RiskLocationAKID.NEXTVAL,
	Changed_Flag AS i_Changed_Flag,
	RiskLocationAKID AS i_RiskLocationAKID,
	LogicalIndicator,
	PolicyAKID,
	RiskLocationKey,
	LocationUnitNumber,
	LocationIndicator,
	RiskTerritory,
	StateProvinceCode,
	ZipPostalCode,
	TaxLocation,
	sup_state_id,
	RatingCity,
	RatingCounty,
	TaxCode,
	-- *INF*: IIF(i_Changed_Flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),SYSDATE)
	IFF(i_Changed_Flag = 'NEW', TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS v_EffectiveDate,
	RiskLocationHashKey,
	Address1,
	IntrastateRiskID,
	ISOFireProtectCity,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	v_EffectiveDate AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	-- *INF*: IIF(ISNULL(i_RiskLocationAKID),NEXTVAL,i_RiskLocationAKID)
	IFF(i_RiskLocationAKID IS NULL, NEXTVAL, i_RiskLocationAKID) AS RiskLocationAKID,
	'N/A' AS o_ISOFireProtectCounty
	FROM FIL_Insert
),
Tgt_RiskLocation_Insert AS (

	------------ PRE SQL ----------
	exec [spSetIndexStatus] @Enable = 0, @Schema = 'dbo', @TableName = 'RiskLocation', @IndexWildcard = 'Ak1RiskLocation'
	-------------------------------


	INSERT INTO RiskLocation
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, LogicalIndicator, RiskLocationAKID, PolicyAKID, RiskLocationKey, LocationUnitNumber, LocationIndicator, RiskTerritory, StateProvinceCode, ZipPostalCode, TaxLocation, sup_state_id, RatingCity, RatingCounty, TaxCode, RiskLocationHashKey, StreetAddress, ISOFireProtectCity, ISOFireProtectCounty, IntrastateRiskId)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	LOGICALINDICATOR, 
	RISKLOCATIONAKID, 
	POLICYAKID, 
	RISKLOCATIONKEY, 
	LOCATIONUNITNUMBER, 
	LOCATIONINDICATOR, 
	RISKTERRITORY, 
	STATEPROVINCECODE, 
	ZIPPOSTALCODE, 
	TAXLOCATION, 
	SUP_STATE_ID, 
	RATINGCITY, 
	RATINGCOUNTY, 
	TAXCODE, 
	RISKLOCATIONHASHKEY, 
	Address1 AS STREETADDRESS, 
	ISOFIREPROTECTCITY, 
	o_ISOFireProtectCounty AS ISOFIREPROTECTCOUNTY, 
	IntrastateRiskID AS INTRASTATERISKID
	FROM EXP_Detemine_AK_ID
),
SQ_RiskLocation AS (
	SELECT 
		RiskLocationID,
		EffectiveDate, 
		ExpirationDate,
		RiskLocationAKID
	FROM
		@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation a
	WHERE   EXISTS
		 (SELECT 1
		 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation b 
		   WHERE CurrentSnapshotFlag = 1 and SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		    and a.RiskLocationAKID = b.RiskLocationAKID
	GROUP BY  RiskLocationAKID  HAVING count(*) > 1)
	AND SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
	ORDER BY  RiskLocationAKID ,EffectiveDate  DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	RiskLocationID,
	EffectiveDate AS i_eff_from_date,
	ExpirationDate AS orig_eff_to_date,
	RiskLocationAKID AS i_RiskLocationAKID,
	-- *INF*: DECODE(TRUE,
	-- i_RiskLocationAKID = v_PrevRiskLocationAKID ,
	-- ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),orig_eff_to_date)
	DECODE(TRUE,
		i_RiskLocationAKID = v_PrevRiskLocationAKID, ADD_TO_DATE(v_prev_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	i_RiskLocationAKID AS v_PrevRiskLocationAKID,
	i_eff_from_date AS v_prev_eff_from_date,
	0 AS o_crrnt_snpsht_flag,
	v_eff_to_date AS o_eff_to_date,
	SYSDATE AS o_modified_date
	FROM SQ_RiskLocation
),
FIL_FirstRowInAKGroup AS (
	SELECT
	RiskLocationID, 
	orig_eff_to_date, 
	o_crrnt_snpsht_flag AS crrnt_snpsht_flag, 
	o_eff_to_date AS eff_to_date, 
	o_modified_date AS modified_date
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_RiskLocation AS (
	SELECT
	RiskLocationID, 
	crrnt_snpsht_flag, 
	eff_to_date, 
	modified_date
	FROM FIL_FirstRowInAKGroup
),
Tgt_RiskLocation_Update AS (
	MERGE INTO RiskLocation AS T
	USING UPD_RiskLocation AS S
	ON T.RiskLocationID = S.RiskLocationID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.crrnt_snpsht_flag, T.ExpirationDate = S.eff_to_date, T.ModifiedDate = S.modified_date

	------------ POST SQL ----------
	exec [spSetIndexStatus] @Enable = 1, @Schema = 'dbo', @TableName = 'RiskLocation', @IndexWildcard = 'Ak1RiskLocation'
	-------------------------------


),
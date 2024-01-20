WITH
SQ_RiskLocation AS (
	SELECT RiskLocationHashKey, 
	LocationUnitNumber, 
	RiskLocationID, 
	LocationIndicator, 
	RiskTerritory, 
	ZipPostalCode, 
	TaxLocation, 
	sup_state_id, 
	RatingCity, 
	RatingCounty, 
	TaxCode, 
	StreetAddress, 
	ISOFireProtectCity, 
	ISOFireProtectCounty 
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation 
	WHERE CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}' 
	AND CurrentSnapshotFlag=1
	
	ORDER BY RiskLocationHashKey, LocationUnitNumber, 
	case when TaxLocation='N/A' then 0 else 1 end,RiskLocationID
),
AGG_RemoveDuplicates AS (
	SELECT
	RiskLocationHashKey,
	LocationUnitNumber,
	RiskLocationID,
	LocationIndicator,
	RiskTerritory,
	ZipPostalCode,
	TaxLocation,
	sup_state_id,
	RatingCity,
	RatingCounty,
	TaxCode,
	StreetAddress,
	ISOFireProtectCity,
	ISOFireProtectCounty
	FROM SQ_RiskLocation
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationHashKey ORDER BY NULL) = 1
),
EXP_Def_Values AS (
	SELECT
	RiskLocationHashKey,
	LocationUnitNumber,
	LocationIndicator,
	-- *INF*: IIF(LocationIndicator =  'Y',LocationUnitNumber,'N/A')
	IFF(LocationIndicator = 'Y', LocationUnitNumber, 'N/A') AS LocationNumber,
	RiskTerritory,
	ZipPostalCode,
	TaxLocation,
	sup_state_id,
	RatingCity,
	RatingCounty,
	TaxCode,
	StreetAddress,
	ISOFireProtectCity,
	ISOFireProtectCounty
	FROM AGG_RemoveDuplicates
),
LKP_SupState AS (
	SELECT
	state_code,
	state_abbrev,
	sup_state_id
	FROM (
		SELECT 
			state_code,
			state_abbrev,
			sup_state_id
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_state
		WHERE crrnt_snpsht_flag = 1 and source_sys_id='EXCEED'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_state_id ORDER BY state_code) = 1
),
lkp_RiskLocationDim AS (
	SELECT
	LocationNumber,
	RiskTerritory,
	TaxLocation,
	RatingCounty,
	TaxCode,
	ISOFireProtectCity,
	ISOFireProtectCounty,
	RiskLocationHashKey,
	RiskLocationDimID
	FROM (
		SELECT 
			LocationNumber,
			RiskTerritory,
			TaxLocation,
			RatingCounty,
			TaxCode,
			ISOFireProtectCity,
			ISOFireProtectCounty,
			RiskLocationHashKey,
			RiskLocationDimID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocationDim
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationHashKey ORDER BY LocationNumber) = 1
),
Exp_RiskLocationDimINS AS (
	SELECT
	lkp_RiskLocationDim.LocationNumber AS lkp_LocationNumber,
	LKP_SupState.state_code AS lkp_state_code,
	LKP_SupState.state_abbrev AS lkp_state_abbrev,
	lkp_RiskLocationDim.RiskTerritory AS lkp_RiskTerritory,
	lkp_RiskLocationDim.TaxLocation AS lkp_TaxLocation,
	lkp_RiskLocationDim.RatingCounty AS lkp_RatingCounty,
	lkp_RiskLocationDim.TaxCode AS lkp_TaxCode,
	lkp_RiskLocationDim.ISOFireProtectCity AS lkp_ISOFireProtectCity,
	lkp_RiskLocationDim.ISOFireProtectCounty AS lkp_ISOFireProtectCounty,
	lkp_RiskLocationDim.RiskLocationHashKey AS lkp_RiskLocationHashKey,
	lkp_RiskLocationDim.RiskLocationDimID AS lkp_RiskLocationDimID,
	EXP_Def_Values.RiskLocationHashKey,
	EXP_Def_Values.LocationNumber,
	EXP_Def_Values.RiskTerritory,
	EXP_Def_Values.ZipPostalCode,
	EXP_Def_Values.TaxLocation,
	EXP_Def_Values.RatingCity,
	EXP_Def_Values.RatingCounty,
	EXP_Def_Values.TaxCode,
	EXP_Def_Values.StreetAddress,
	EXP_Def_Values.ISOFireProtectCity,
	EXP_Def_Values.ISOFireProtectCounty,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_RiskLocationHashKey),1,
	-- lkp_LocationNumber != LocationNumber 
	-- OR lkp_RiskTerritory!=RiskTerritory 
	-- OR lkp_TaxLocation!=TaxLocation 
	-- OR lkp_RatingCounty!=RatingCounty 
	-- OR lkp_TaxCode!=TaxCode 
	-- OR lkp_ISOFireProtectCity != ISOFireProtectCity 
	-- OR lkp_ISOFireProtectCounty != ISOFireProtectCounty,2,
	-- 0)
	DECODE(
	    TRUE,
	    lkp_RiskLocationHashKey IS NULL, 1,
	    lkp_LocationNumber != LocationNumber OR lkp_RiskTerritory != RiskTerritory OR lkp_TaxLocation != TaxLocation OR lkp_RatingCounty != RatingCounty OR lkp_TaxCode != TaxCode OR lkp_ISOFireProtectCity != ISOFireProtectCity OR lkp_ISOFireProtectCounty != ISOFireProtectCounty, 2,
	    0
	) AS o_ChangeFlag,
	1 AS o_crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_audit_id,
	-- *INF*: TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS o_ExpirationDate,
	SYSDATE AS o_createddate,
	SYSDATE AS o_modifieddate,
	-- *INF*: IIF(ISNULL(lkp_state_code) OR IS_SPACES(lkp_state_code) OR LENGTH(lkp_state_code)=0,'N/A',LTRIM(RTRIM(lkp_state_code)))
	IFF(
	    lkp_state_code IS NULL
	    or LENGTH(lkp_state_code)>0
	    and TRIM(lkp_state_code)=''
	    or LENGTH(lkp_state_code) = 0,
	    'N/A',
	    LTRIM(RTRIM(lkp_state_code))
	) AS o_StateProvinceCode,
	-- *INF*: IIF(ISNULL(lkp_state_abbrev) OR IS_SPACES(lkp_state_abbrev) OR LENGTH(lkp_state_abbrev)=0, 'N/A',LTRIM(RTRIM(lkp_state_abbrev)))
	IFF(
	    lkp_state_abbrev IS NULL
	    or LENGTH(lkp_state_abbrev)>0
	    and TRIM(lkp_state_abbrev)=''
	    or LENGTH(lkp_state_abbrev) = 0,
	    'N/A',
	    LTRIM(RTRIM(lkp_state_abbrev))
	) AS o_StateProvinceCodeAbbreviation
	FROM EXP_Def_Values
	LEFT JOIN LKP_SupState
	ON LKP_SupState.sup_state_id = EXP_Def_Values.sup_state_id
	LEFT JOIN lkp_RiskLocationDim
	ON lkp_RiskLocationDim.RiskLocationHashKey = EXP_Def_Values.RiskLocationHashKey
),
RTR_InsertOrUpdate AS (
	SELECT
	o_ChangeFlag AS ChangeFlag,
	lkp_RiskLocationDimID AS RiskLocationDimID,
	o_crrnt_snpsht_flag AS crrnt_snpsht_flag,
	o_audit_id AS audit_id,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_createddate AS createddate,
	o_modifieddate AS modifieddate,
	LocationNumber,
	RiskTerritory,
	o_StateProvinceCodeAbbreviation AS StateProvinceCode,
	o_StateProvinceCode AS StateProvinceCodeAbbreviation,
	ZipPostalCode,
	TaxLocation,
	RatingCity,
	RatingCounty,
	TaxCode,
	RiskLocationHashKey,
	StreetAddress,
	ISOFireProtectCity,
	ISOFireProtectCounty
	FROM Exp_RiskLocationDimINS
),
RTR_InsertOrUpdate_INSERT AS (SELECT * FROM RTR_InsertOrUpdate WHERE ChangeFlag=1),
RTR_InsertOrUpdate_UPDATE AS (SELECT * FROM RTR_InsertOrUpdate WHERE ChangeFlag=2),
UPD_RiskLocationDim AS (
	SELECT
	RiskLocationDimID, 
	modifieddate AS ModifiedDate, 
	LocationNumber, 
	RiskTerritory, 
	TaxLocation, 
	RatingCounty, 
	TaxCode, 
	ISOFireProtectCity, 
	ISOFireProtectCounty
	FROM RTR_InsertOrUpdate_UPDATE
),
TGT_RiskLocationDim_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocationDim AS T
	USING UPD_RiskLocationDim AS S
	ON T.RiskLocationDimID = S.RiskLocationDimID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.LocationNumber = S.LocationNumber, T.RiskTerritory = S.RiskTerritory, T.TaxLocation = S.TaxLocation, T.RatingCounty = S.RatingCounty, T.TaxCode = S.TaxCode, T.ISOFireProtectCity = S.ISOFireProtectCity, T.ISOFireProtectCounty = S.ISOFireProtectCounty
),
TGT_RiskLocationDim_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocationDim
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, LocationNumber, RiskTerritory, StateProvinceCode, StateProvinceCodeAbbreviation, ZipPostalCode, TaxLocation, RatingCity, RatingCounty, TaxCode, RiskLocationHashKey, StreetAddress, ISOFireProtectCity, ISOFireProtectCounty)
	SELECT 
	crrnt_snpsht_flag AS CURRENTSNAPSHOTFLAG, 
	audit_id AS AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	createddate AS CREATEDDATE, 
	modifieddate AS MODIFIEDDATE, 
	LOCATIONNUMBER, 
	RISKTERRITORY, 
	STATEPROVINCECODE, 
	STATEPROVINCECODEABBREVIATION, 
	ZIPPOSTALCODE, 
	TAXLOCATION, 
	RATINGCITY, 
	RATINGCOUNTY, 
	TAXCODE, 
	RISKLOCATIONHASHKEY, 
	STREETADDRESS, 
	ISOFIREPROTECTCITY, 
	ISOFIREPROTECTCOUNTY
	FROM RTR_InsertOrUpdate_INSERT
),
SQ_RiskLocationDim_DCT_IL_TaxLoc_Dataplug AS (
	SELECT 
	RD.RiskLocationDimid,
	IL.TaxLocation
	FROM
	@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocationDim RD
	inner join
	(select distinct 
	RL.risklocationhashkey,
	RL.taxlocation
	from 
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.risklocation RL
	inner join
	sup_state S
	on S.state_code = 'IL' and S.sup_state_id = RL.sup_state_id
	where CurrentSnapshotFlag = 1 and SourceSystemID = 'DCT' 
	) IL
	on RD.RiskLocationHashKey = IL.RiskLocationHashKey
	and RD.TaxLocation <> IL.TaxLocation
	ORDER BY 
	RD.RiskLocationDimid,
	case when IL.TaxLocation = 'N/A' then 0
	else 1 end,
	IL.TaxLocation
),
AGG_Remove_Dups_with_TaxLoc_NA AS (
	SELECT
	RiskLocationDimID,
	TaxLocation
	FROM SQ_RiskLocationDim_DCT_IL_TaxLoc_Dataplug
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationDimID ORDER BY NULL) = 1
),
FIL_TaxLoc AS (
	SELECT
	RiskLocationDimID, 
	TaxLocation
	FROM AGG_Remove_Dups_with_TaxLoc_NA
	WHERE TaxLocation != 'N/A'
),
EXP_TaxLoc AS (
	SELECT
	RiskLocationDimID,
	TaxLocation
	FROM FIL_TaxLoc
),
UPD_RiskLocationDim_DCT_TaxLoc AS (
	SELECT
	RiskLocationDimID, 
	TaxLocation
	FROM EXP_TaxLoc
),
TGT_RiskLocationDim_Update_DCT_TaxLoc AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocationDim AS T
	USING UPD_RiskLocationDim_DCT_TaxLoc AS S
	ON T.RiskLocationDimID = S.RiskLocationDimID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.TaxLocation = S.TaxLocation
),
SQ_RiskLocationDim_DCT_IL_ISOCity_Dataplug AS (
	SELECT 
	RD.RiskLocationDimid,
	IL.ISOFireProtectCity
	FROM
	@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocationDim RD
	inner join
	(select distinct 
	RL.risklocationhashkey,
	RL.ISOFireProtectCity
	from 
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.risklocation RL
	inner join
	sup_state S
	on S.state_code = 'IL' and S.sup_state_id = RL.sup_state_id
	where CurrentSnapshotFlag = 1 and SourceSystemID = 'DCT' 
	) IL
	on RD.RiskLocationHashKey = IL.RiskLocationHashKey
	and RD.ISOFireProtectCity <> IL.ISOFireProtectCity
	ORDER BY 
	RD.RiskLocationDimid,
	case when IL.ISOFireProtectCity = 'N/A' then 0
	else 1 end,
	IL.ISOFireProtectCity
),
AGG_Remove_Dups_with_TaxLoc_NA1 AS (
	SELECT
	RiskLocationDimID,
	ISOFireProtectCity
	FROM SQ_RiskLocationDim_DCT_IL_ISOCity_Dataplug
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationDimID ORDER BY NULL) = 1
),
FIL_ISOCity AS (
	SELECT
	RiskLocationDimID, 
	ISOFireProtectCity
	FROM AGG_Remove_Dups_with_TaxLoc_NA1
	WHERE ISOFireProtectCity != 'N/A'
),
EXP_ISOCity AS (
	SELECT
	RiskLocationDimID,
	ISOFireProtectCity
	FROM FIL_ISOCity
),
UPD_RiskLocationDim_DCT_ISOCity AS (
	SELECT
	RiskLocationDimID, 
	ISOFireProtectCity
	FROM EXP_ISOCity
),
TGT_RiskLocationDim_Update_DCT_ISOCity AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocationDim AS T
	USING UPD_RiskLocationDim_DCT_ISOCity AS S
	ON T.RiskLocationDimID = S.RiskLocationDimID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ISOFireProtectCity = S.ISOFireProtectCity
),
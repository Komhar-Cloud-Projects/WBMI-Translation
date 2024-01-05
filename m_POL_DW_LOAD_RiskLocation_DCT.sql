WITH
SQ_DCLocationStaging AS (
	SELECT WorkDCTLocation.SessionId, WorkDCTPolicy.PartyId, WorkDCTLocation.LocationId, WorkDCTLocation.StateProvince,
	 WorkDCTLocation.PostalCode, WorkDCTPolicy.PolicyGUId, WorkDCTPolicy.PolicyNumber, WorkDCTPolicy.PolicyVersion, 
	 WorkDCTLocation.LocationNumber, WorkDCTLocation.LocationXmlId, WorkDCTPolicy.CustomerNum, WorkDCTLocation.City,
	  WorkDCTLocation.County, WorkDCTLocation.Address1, WorkDCTLocation.Territory 
	  ,CTE.ParentCoverageObjectName,CTE.LineType, CTE.CoverageGUID 
	  FROM
	  DBO.WorkDCTLocation INNER JOIN DBO.WorkDCTPolicy ON WorkDCTLocation.SessionId = WorkDCTPolicy.SessionId
	  Left Outer JOIN 
	  (
	 SELECT
	 WorkDCTCoverageTransaction.ParentCoverageObjectName, 
	  WorkDCTInsuranceLine.LineType, WorkDCTCoverageTransaction.CoverageGUID,WorkDCTTransactionInsuranceLineLocationBridge.LocationAssociationId,
	  WorkDCTInsuranceLine.PolicyId
	  FROM WorkDCTCoverageTransaction, WorkDCTInsuranceLine,WorkDCTTransactionInsuranceLineLocationBridge
	   WHERE WorkDCTCoverageTransaction.CoverageId=WorkDCTTransactionInsuranceLineLocationBridge.CoverageId
	and
	WorkDCTInsuranceLine.LineId=WorkDCTTransactionInsuranceLineLocationBridge.LineId
	) CTE
	ON CTE.LocationAssociationId=WorkDCTLocation.LocationAssociationId
	AND CTE.PolicyId=WorkDCTPolicy.PolicyId
	WHERE
	 WorkDCTPolicy.PolicyStatus<>'Quote'and WorkDCTPolicy.TransactionState='committed'
	And 
	WorkDCTPolicy.TransactionType NOT IN ('RescindNonRenew','Reporting','VoidReporting','Information','Dividend','RevisedDividend',
	'VoidDividend','NonRenew','RescindCancelPending','CancelPending')
),
EXP_Extract_Stage AS (
	SELECT
	SessionId,
	PartyId,
	LocationId,
	StateProvince,
	PostalCode,
	PolicyGUId,
	PolicyNumber,
	PolicyVersion,
	LocationNumber,
	LocationXmlId,
	CustomerNum,
	City,
	County,
	Address1,
	Territory,
	ParentCoverageObjectName,
	LineType,
	CoverageGUID
	FROM SQ_DCLocationStaging
),
Mplt_RiskLocation_Key AS (WITH
	LKP_RiskLocation_RiskLocationKey_LocNum AS (
		SELECT
		RiskLocationAKID,
		RiskLocationKey,
		LocationUnitNumber
		FROM (
			SELECT 
				RiskLocationAKID,
				RiskLocationKey,
				LocationUnitNumber
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation
			WHERE CurrentSnapshotFlag = 1 and SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and 
			exists
			 ( select 1 from @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol inner join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT 
			 on WCT.PolicyNumber=pol.pol_num and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol.pol_mod and pol.crrnt_snpsht_flag=1 and PolicyAKID=pol.Pol_AK_ID ) Order By EffectiveDate DESC,CreatedDate DESC,RiskLocationAKID desc--
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationKey,LocationUnitNumber ORDER BY RiskLocationAKID) = 1
	),
	LKP_RiskLocation_RiskLocationKey AS (
		SELECT
		RiskLocationAKID,
		RiskLocationKey
		FROM (
			SELECT 
				RiskLocationAKID,
				RiskLocationKey
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation
			WHERE CurrentSnapshotFlag = 1 and SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and 
			exists
			 ( select 1 from @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol inner join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT 
			 on WCT.PolicyNumber=pol.pol_num and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol.pol_mod and pol.crrnt_snpsht_flag=1 and PolicyAKID=pol.Pol_AK_ID ) Order By EffectiveDate DESC,CreatedDate DESC,RiskLocationAKID desc--
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationKey ORDER BY RiskLocationAKID) = 1
	),
	LKP_RiskLocation_RiskLocationKey_LocNum_Territory AS (
		SELECT
		RiskLocationAKID,
		RiskLocationKey,
		LocationUnitNumber,
		RiskTerritory
		FROM (
			SELECT 
				RiskLocationAKID,
				RiskLocationKey,
				LocationUnitNumber,
				RiskTerritory
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation
			WHERE CurrentSnapshotFlag = 1 and SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and 
			exists
			 ( select 1 from @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol inner join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT 
			 on WCT.PolicyNumber=pol.pol_num and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol.pol_mod and pol.crrnt_snpsht_flag=1 and PolicyAKID=pol.Pol_AK_ID ) Order By EffectiveDate DESC,CreatedDate DESC,RiskLocationAKID desc--
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationKey,LocationUnitNumber,RiskTerritory ORDER BY RiskLocationAKID) = 1
	),
	Source_Input AS (
		
	),
	EXP_Source_Data AS (
		SELECT
		SessionId,
		PartyId,
		LocationId,
		StateProvince,
		PostalCode,
		PolicyGUId,
		PolicyNumber,
		PolicyVersion,
		LocationNumber,
		LocationXmlId,
		CustomerNum,
		City,
		County,
		Address1,
		Territory,
		ParentCoverageObjectName,
		LineType,
		CoverageGUID,
		-- *INF*: rtrim(ltrim(IIF(ISNULL(PolicyNumber) or IS_SPACES(PolicyNumber) or LENGTH(PolicyNumber)=0, 'N/A', LTRIM(RTRIM(PolicyNumber)))))
		rtrim(ltrim(IFF(PolicyNumber IS NULL OR IS_SPACES(PolicyNumber) OR LENGTH(PolicyNumber) = 0, 'N/A', LTRIM(RTRIM(PolicyNumber))))) AS v_PolicyNumber,
		-- *INF*: rtrim(ltrim(IIF(ISNULL(PolicyVersion), '00', LPAD(TO_CHAR(PolicyVersion),2,'0'))))
		rtrim(ltrim(IFF(PolicyVersion IS NULL, '00', LPAD(TO_CHAR(PolicyVersion), 2, '0')))) AS v_PolicyVersion,
		v_PolicyNumber AS o_PolicyNumber,
		v_PolicyVersion AS o_PolicyVersion,
		v_PolicyNumber||v_PolicyVersion AS o_PolicyKey
		FROM Source_Input
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
			select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT
			where WCT.PolicyNumber=pol_num
			and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol_mod)
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_ak_id) = 1
	),
	EXP_PolicyAKID_Deduction AS (
		SELECT
		EXP_Source_Data.SessionId,
		EXP_Source_Data.PartyId,
		EXP_Source_Data.LocationId,
		EXP_Source_Data.StateProvince,
		EXP_Source_Data.PostalCode,
		EXP_Source_Data.PolicyGUId,
		EXP_Source_Data.o_PolicyNumber AS PolicyNumber,
		EXP_Source_Data.o_PolicyVersion AS PolicyVersion,
		EXP_Source_Data.LocationNumber,
		EXP_Source_Data.LocationXmlId,
		EXP_Source_Data.CustomerNum,
		EXP_Source_Data.City,
		EXP_Source_Data.County,
		EXP_Source_Data.Address1,
		EXP_Source_Data.Territory,
		EXP_Source_Data.ParentCoverageObjectName,
		EXP_Source_Data.LineType,
		EXP_Source_Data.CoverageGUID,
		EXP_Source_Data.o_PolicyKey,
		LKP_policy.pol_ak_id AS i_pol_ak_id,
		-- *INF*: iif(isnull(i_pol_ak_id),-1,i_pol_ak_id)
		IFF(i_pol_ak_id IS NULL, - 1, i_pol_ak_id) AS o_Pol_AK_ID
		FROM EXP_Source_Data
		LEFT JOIN LKP_policy
		ON LKP_policy.pol_key = EXP_Source_Data.o_PolicyKey
	),
	RTE_Parent_Object_Group AS (
		SELECT
		SessionId,
		PartyId,
		LocationId,
		StateProvince,
		PostalCode,
		PolicyGUId,
		PolicyNumber,
		PolicyVersion,
		LocationNumber,
		LocationXmlId,
		CustomerNum,
		City,
		County,
		Address1,
		Territory,
		ParentCoverageObjectName,
		LineType,
		CoverageGUID,
		o_PolicyKey AS PolicyKey,
		o_Pol_AK_ID AS Pol_AK_ID
		FROM EXP_PolicyAKID_Deduction
	),
	RTE_Parent_Object_Group_RiskLevel AS (SELECT * FROM RTE_Parent_Object_Group WHERE 1=1),
	RTE_Parent_Object_Group_StateLevel AS (SELECT * FROM RTE_Parent_Object_Group WHERE IN(ParentCoverageObjectName,'DC_CA_State','DC_WC_StateTerm','WB_GOC_State','WB_HIO_State','WB_IM_State', 'WB_EC_State','DC_WC_State')=1),
	RTE_Parent_Object_Group_LineLevel AS (SELECT * FROM RTE_Parent_Object_Group WHERE IN(ParentCoverageObjectName,'DC_CR_Endorsement','DC_CR_Risk','DC_CR_RiskCrime','DC_Line','DC_CU_UmbrellaEmployersLiability','DC_IM_CoverageForm','WB_CU_PremiumDetail')=1),
	Exp_RiskLevel AS (
		SELECT
		SessionId,
		PartyId,
		LocationId,
		StateProvince,
		PostalCode,
		PolicyGUId,
		PolicyNumber,
		PolicyVersion,
		LocationNumber,
		LocationXmlId,
		CustomerNum,
		City,
		County,
		Address AS Address1,
		Territory,
		ParentCoverageObjectName,
		LineType,
		CoverageGUID,
		PolicyKey,
		Pol_AK_ID,
		-- *INF*: IIF(ISNULL(StateProvince) or IS_SPACES(StateProvince)  or LENGTH(StateProvince)=0,'N/A',LTRIM(RTRIM(StateProvince)))
		IFF(StateProvince IS NULL OR IS_SPACES(StateProvince) OR LENGTH(StateProvince) = 0, 'N/A', LTRIM(RTRIM(StateProvince))) AS v_StateProvince,
		v_StateProvince AS o_StateProvince,
		-- *INF*: IIF(ISNULL(LineType) or IS_SPACES(LineType) or LENGTH(LineType)=0,'N/A',LTRIM(RTRIM(LineType)))
		IFF(LineType IS NULL OR IS_SPACES(LineType) OR LENGTH(LineType) = 0, 'N/A', LTRIM(RTRIM(LineType))) AS v_LineType,
		v_LineType AS o_LineType,
		-- *INF*: IIF(ISNULL(LocationXmlId) OR IS_SPACES(LocationXmlId) OR LENGTH(LocationXmlId)=0,'N/A',LTRIM(RTRIM(LocationXmlId)))
		IFF(LocationXmlId IS NULL OR IS_SPACES(LocationXmlId) OR LENGTH(LocationXmlId) = 0, 'N/A', LTRIM(RTRIM(LocationXmlId))) AS v_LocationXmlId,
		v_LocationXmlId AS o_LocationXmlId,
		Pol_AK_ID||'~'||v_LocationXmlId AS o_RiskLocationKey
		FROM RTE_Parent_Object_Group_RiskLevel
	),
	Exp_StateLevel AS (
		SELECT
		SessionId,
		PartyId,
		LocationId,
		StateProvince,
		PostalCode,
		PolicyGUId,
		PolicyNumber,
		PolicyVersion,
		LocationNumber,
		LocationXmlId,
		CustomerNum,
		City,
		County,
		Address1,
		Territory,
		ParentCoverageObjectName,
		LineType,
		CoverageGUID,
		PolicyKey,
		Pol_AK_ID,
		-- *INF*: IIF(ISNULL(LocationXmlId) OR IS_SPACES(LocationXmlId) OR LENGTH(LocationXmlId)=0,'N/A',LTRIM(RTRIM(LocationXmlId)))
		IFF(LocationXmlId IS NULL OR IS_SPACES(LocationXmlId) OR LENGTH(LocationXmlId) = 0, 'N/A', LTRIM(RTRIM(LocationXmlId))) AS v_LocationXmlId,
		v_LocationXmlId AS o_LocationXmlId,
		-- *INF*: IIF(ISNULL(StateProvince) or IS_SPACES(StateProvince)  or LENGTH(StateProvince)=0,'N/A',LTRIM(RTRIM(StateProvince)))
		IFF(StateProvince IS NULL OR IS_SPACES(StateProvince) OR LENGTH(StateProvince) = 0, 'N/A', LTRIM(RTRIM(StateProvince))) AS v_StateProvince,
		v_StateProvince AS o_StateProvince,
		-- *INF*: IIF(ISNULL(LineType) or IS_SPACES(LineType) or LENGTH(LineType)=0,'N/A',LTRIM(RTRIM(LineType)))
		IFF(LineType IS NULL OR IS_SPACES(LineType) OR LENGTH(LineType) = 0, 'N/A', LTRIM(RTRIM(LineType))) AS v_LineType,
		v_LineType AS o_LineType,
		Pol_AK_ID||'~'||'PrimaryLocation'||'~'||v_StateProvince||'~'||v_LineType AS RiskLocationKey
		FROM RTE_Parent_Object_Group_StateLevel
	),
	Exp_LineLevel AS (
		SELECT
		SessionId,
		PartyId,
		LocationId,
		StateProvince,
		PostalCode,
		PolicyGUId,
		PolicyNumber,
		PolicyVersion,
		LocationNumber,
		LocationXmlId,
		CustomerNum,
		City,
		County,
		Address1,
		Territory,
		ParentCoverageObjectName,
		LineType,
		CoverageGUID,
		PolicyKey,
		Pol_AK_ID,
		-- *INF*: IIF(ISNULL(LocationXmlId) OR IS_SPACES(LocationXmlId) OR LENGTH(LocationXmlId)=0,'N/A',LTRIM(RTRIM(LocationXmlId)))
		IFF(LocationXmlId IS NULL OR IS_SPACES(LocationXmlId) OR LENGTH(LocationXmlId) = 0, 'N/A', LTRIM(RTRIM(LocationXmlId))) AS v_LocationXmlId,
		v_LocationXmlId AS o_LocationXmlId,
		-- *INF*: IIF(ISNULL(StateProvince) or IS_SPACES(StateProvince)  or LENGTH(StateProvince)=0,'N/A',LTRIM(RTRIM(StateProvince)))
		IFF(StateProvince IS NULL OR IS_SPACES(StateProvince) OR LENGTH(StateProvince) = 0, 'N/A', LTRIM(RTRIM(StateProvince))) AS v_StateProvince,
		v_StateProvince AS o_StateProvince,
		-- *INF*: IIF(ISNULL(LineType) or IS_SPACES(LineType) or LENGTH(LineType)=0,'N/A',LTRIM(RTRIM(LineType)))
		IFF(LineType IS NULL OR IS_SPACES(LineType) OR LENGTH(LineType) = 0, 'N/A', LTRIM(RTRIM(LineType))) AS v_LineType,
		v_LineType AS o_LineType,
		Pol_AK_ID||'~'||'PrimaryLocation'||'~'||v_LineType AS RiskLocationKey
		FROM RTE_Parent_Object_Group_LineLevel
	),
	Un_Risk_State_Line_Object AS (
		SELECT SessionId, PartyId, LocationId, PostalCode, PolicyGUId, PolicyNumber, PolicyVersion, LocationNumber, CustomerNum, City, County, Address1, Territory, ParentCoverageObjectName, CoverageGUID, PolicyKey, Pol_AK_ID, o_StateProvince AS StateProvince, o_LocationXmlId AS LocationXmlId, o_RiskLocationKey AS RiskLocationKey, o_LineType AS LineType
		FROM Exp_RiskLevel
		UNION
		SELECT SessionId, PartyId, LocationId, PostalCode, PolicyGUId, PolicyNumber, PolicyVersion, LocationNumber, CustomerNum, City, County, Address1, Territory, ParentCoverageObjectName, CoverageGUID, PolicyKey, Pol_AK_ID, o_StateProvince AS StateProvince, o_LocationXmlId AS LocationXmlId, RiskLocationKey, o_LineType AS LineType
		FROM Exp_StateLevel
		UNION
		SELECT SessionId, PartyId, LocationId, PostalCode, PolicyGUId, PolicyNumber, PolicyVersion, LocationNumber, CustomerNum, City, County, Address1, Territory, ParentCoverageObjectName, CoverageGUID, PolicyKey, Pol_AK_ID, o_StateProvince AS StateProvince, o_LocationXmlId AS LocationXmlId, RiskLocationKey, o_LineType AS LineType
		FROM Exp_LineLevel
	),
	LKP_RatingCoverage_RiskLocation AS (
		SELECT
		RiskLocationAKID,
		RiskLocationKey,
		PolicyAKID,
		CoverageGUID
		FROM (
			SELECT R.RiskLocationAKID as RiskLocationAKID, R.RiskLocationKey as RiskLocationKey, R.PolicyAKID as PolicyAKID, RC.CoverageGUID as CoverageGUID 
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC
			INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC ON RC.PolicyCoverageAKID = PC.PolicyCoverageAKID
			AND RC.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
			INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation R ON R.RiskLocationAKID = PC.RiskLocationAKID
				AND PC.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
				AND PC.CurrentSnapshotFlag = 1
			       AND R.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
				AND R.CurrentSnapshotFlag = 1
				AND EXISTS ( SELECT 1 from @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol 
			                 INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT 
			                ON WCT.PolicyNumber=pol.pol_num and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol.pol_mod 
			                 AND pol.crrnt_snpsht_flag=1 and R.PolicyAKId=pol.pol_ak_id) 
			ORDER BY RC.RatingCoverageAKID	,RC.EffectiveDate --
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID,CoverageGUID ORDER BY RiskLocationAKID DESC) = 1
	),
	EXP_ParentObject_Level AS (
		SELECT
		Un_Risk_State_Line_Object.SessionId,
		Un_Risk_State_Line_Object.PartyId,
		Un_Risk_State_Line_Object.LocationId,
		Un_Risk_State_Line_Object.PostalCode,
		Un_Risk_State_Line_Object.PolicyGUId,
		Un_Risk_State_Line_Object.PolicyNumber,
		Un_Risk_State_Line_Object.PolicyVersion,
		Un_Risk_State_Line_Object.LocationNumber,
		Un_Risk_State_Line_Object.CustomerNum,
		Un_Risk_State_Line_Object.City,
		Un_Risk_State_Line_Object.County,
		Un_Risk_State_Line_Object.Address1,
		Un_Risk_State_Line_Object.Territory,
		LKP_RatingCoverage_RiskLocation.RiskLocationAKID AS lkp_RiskLocationAKID,
		LKP_RatingCoverage_RiskLocation.RiskLocationKey AS lkp_RiskLocationKey,
		Un_Risk_State_Line_Object.ParentCoverageObjectName,
		Un_Risk_State_Line_Object.CoverageGUID,
		Un_Risk_State_Line_Object.PolicyKey,
		Un_Risk_State_Line_Object.Pol_AK_ID,
		Un_Risk_State_Line_Object.StateProvince,
		Un_Risk_State_Line_Object.LocationXmlId,
		Un_Risk_State_Line_Object.RiskLocationKey,
		Un_Risk_State_Line_Object.LineType,
		-- *INF*: IIF(ISNULL(LocationNumber) or IS_SPACES(LocationNumber) or LENGTH(LocationNumber)=0,'0000', LPAD(LTRIM(RTRIM (LocationNumber)), 4, '0'))
		IFF(LocationNumber IS NULL OR IS_SPACES(LocationNumber) OR LENGTH(LocationNumber) = 0, '0000', LPAD(LTRIM(RTRIM(LocationNumber)), 4, '0')) AS v_LocationNumber,
		-- *INF*: IIF(ISNULL(Territory) or IS_SPACES(Territory)  or LENGTH(Territory)=0,'N/A',LTRIM(RTRIM(Territory)))
		IFF(Territory IS NULL OR IS_SPACES(Territory) OR LENGTH(Territory) = 0, 'N/A', LTRIM(RTRIM(Territory))) AS v_Territory,
		-- *INF*: :LKP.LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY(RiskLocationKey,v_LocationNumber,v_Territory)
		LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY_RiskLocationKey_v_LocationNumber_v_Territory.RiskLocationAKID AS v_RiskLocationAKID_RiskKey_Location_Territory,
		-- *INF*: IIF(ISNULL(v_RiskLocationAKID_RiskKey_Location_Territory),:LKP.LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM(RiskLocationKey,v_LocationNumber),v_RiskLocationAKID_RiskKey_Location_Territory)
		IFF(v_RiskLocationAKID_RiskKey_Location_Territory IS NULL, LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_RiskLocationKey_v_LocationNumber.RiskLocationAKID, v_RiskLocationAKID_RiskKey_Location_Territory) AS v_RiskLocationAKID_RiskKey_Location,
		-- *INF*: IIF(ISNULL(v_RiskLocationAKID_RiskKey_Location),:LKP.LKP_RISKLOCATION_RISKLOCATIONKEY(RiskLocationKey),v_RiskLocationAKID_RiskKey_Location)
		IFF(v_RiskLocationAKID_RiskKey_Location IS NULL, LKP_RISKLOCATION_RISKLOCATIONKEY_RiskLocationKey.RiskLocationAKID, v_RiskLocationAKID_RiskKey_Location) AS v_RiskLocationAKID_RiskKey,
		-- *INF*: DECODE ( TRUE, 
		-- ---Condition to check for Locations which are not associated with any of the coverageguid
		-- ISNULL(CoverageGUID),v_RiskLocationAKID_RiskKey,
		-- ---Condition to check the RisklocationAKID for RiskLevel made up records(Records which are not part of risk level parenet object but created for State and Line level)
		-- NOT IN (ParentCoverageObjectName,'DC_BP_Location','DC_BP_Risk','DC_CA_Risk','DC_CF_Risk','DC_GL_Risk','DC_IM_Risk','DC_WC_Risk','DCBPLocation','WB_GOC_Risk','WB_HIO_Risk') AND RiskLocationKey = Pol_AK_ID||'~'||LocationXmlId AND (lkp_RiskLocationKey  != RiskLocationKey),v_RiskLocationAKID_RiskKey,
		-- --Condition to check risklocationakid for new location which is coming with new coverage.
		-- ISNULL(lkp_RiskLocationAKID),v_RiskLocationAKID_RiskKey,
		-- lkp_RiskLocationAKID)
		DECODE(TRUE,
		CoverageGUID IS NULL, v_RiskLocationAKID_RiskKey,
		NOT IN(ParentCoverageObjectName, 'DC_BP_Location', 'DC_BP_Risk', 'DC_CA_Risk', 'DC_CF_Risk', 'DC_GL_Risk', 'DC_IM_Risk', 'DC_WC_Risk', 'DCBPLocation', 'WB_GOC_Risk', 'WB_HIO_Risk') AND RiskLocationKey = Pol_AK_ID || '~' || LocationXmlId AND ( lkp_RiskLocationKey != RiskLocationKey ), v_RiskLocationAKID_RiskKey,
		lkp_RiskLocationAKID IS NULL, v_RiskLocationAKID_RiskKey,
		lkp_RiskLocationAKID) AS v_RiskLocationAKID,
		v_RiskLocationAKID AS o_RiskLocationAKID,
		-- *INF*: iif(isnull(to_char(v_RiskLocationAKID)),RiskLocationKey,to_char(v_RiskLocationAKID))
		-- -- This column is created to group by in mapping based on Exsiting RisklocationAKID or New RiskLocationKey
		IFF(to_char(v_RiskLocationAKID) IS NULL, RiskLocationKey, to_char(v_RiskLocationAKID)) AS RiskLocation_Group
		FROM Un_Risk_State_Line_Object
		LEFT JOIN LKP_RatingCoverage_RiskLocation
		ON LKP_RatingCoverage_RiskLocation.PolicyAKID = Un_Risk_State_Line_Object.Pol_AK_ID AND LKP_RatingCoverage_RiskLocation.CoverageGUID = Un_Risk_State_Line_Object.CoverageGUID
		LEFT JOIN LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY_RiskLocationKey_v_LocationNumber_v_Territory
		ON LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY_RiskLocationKey_v_LocationNumber_v_Territory.RiskLocationKey = RiskLocationKey
		AND LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY_RiskLocationKey_v_LocationNumber_v_Territory.LocationUnitNumber = v_LocationNumber
		AND LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY_RiskLocationKey_v_LocationNumber_v_Territory.RiskTerritory = v_Territory
	
		LEFT JOIN LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_RiskLocationKey_v_LocationNumber
		ON LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_RiskLocationKey_v_LocationNumber.RiskLocationKey = RiskLocationKey
		AND LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_RiskLocationKey_v_LocationNumber.LocationUnitNumber = v_LocationNumber
	
		LEFT JOIN LKP_RISKLOCATION_RISKLOCATIONKEY LKP_RISKLOCATION_RISKLOCATIONKEY_RiskLocationKey
		ON LKP_RISKLOCATION_RISKLOCATIONKEY_RiskLocationKey.RiskLocationKey = RiskLocationKey
	
	),
	RiskTarget AS (
		SELECT
		SessionId, 
		PartyId, 
		LocationId, 
		PostalCode, 
		PolicyGUId, 
		PolicyNumber, 
		PolicyVersion, 
		LocationNumber, 
		CustomerNum, 
		City, 
		County, 
		Address1, 
		Territory, 
		ParentCoverageObjectName, 
		CoverageGUID, 
		PolicyKey, 
		Pol_AK_ID, 
		StateProvince, 
		LocationXmlId, 
		RiskLocationKey, 
		LineType, 
		o_RiskLocationAKID AS o_RiskLocationnAKID, 
		RiskLocation_Group
		FROM EXP_ParentObject_Level
	),
),
LKP_WBLocationAccountStage AS (
	SELECT
	CityTaxCode,
	GeoTaxCountyDistrictCode,
	GeoTaxCityDistrictCode,
	KYTaxFactorAppliedIndicator,
	TerritoryIllinoisFireTaxLocationCode,
	SessionId,
	LocationId
	FROM (
		SELECT a.CityTaxCode as CityTaxCode,
		a.GeoTaxCountyDistrictCode as GeoTaxCountyDistrictCode,
		a.GeoTaxCityDistrictCode as GeoTaxCityDistrictCode,
		CASE WHEN b.KYTaxFactorAppliedIndicator='Difference' 
		THEN (CASE WHEN b.TaxFactorApplied=a.CityTaxPercent THEN 'City' WHEN b.TaxFactorApplied=a.CountyTaxPercent THEN 'County' END)
		ELSE b.KYTaxFactorAppliedIndicator END as KYTaxFactorAppliedIndicator,
		a.TerritoryIllinoisFireTaxLocationCode as TerritoryIllinoisFireTaxLocationCode,
		a.SessionId as SessionId,
		a.LocationId as LocationId
		FROM dbo.WBLocationAccountStage a
		left join dbo.WBCLLocationAccountStage b
		on a.SessionId=b.SessionId
		and a.WBLocationAccountId=b.WBLocationAccountId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SessionId,LocationId ORDER BY CityTaxCode) = 1
),
EXP_Extract_RiskLocationKey AS (
	SELECT
	Mplt_RiskLocation_Key.SessionId1 AS SessionId,
	Mplt_RiskLocation_Key.PartyId1 AS PartyId,
	Mplt_RiskLocation_Key.LocationId1 AS LocationId,
	Mplt_RiskLocation_Key.PostalCode1 AS PostalCode,
	Mplt_RiskLocation_Key.PolicyGUId1 AS PolicyGUId,
	Mplt_RiskLocation_Key.PolicyNumber1 AS PolicyNumber,
	Mplt_RiskLocation_Key.PolicyVersion1 AS PolicyVersion,
	Mplt_RiskLocation_Key.LocationNumber1 AS LocationNumber,
	Mplt_RiskLocation_Key.CustomerNum1 AS CustomerNum,
	Mplt_RiskLocation_Key.City1 AS City,
	Mplt_RiskLocation_Key.County1 AS County,
	Mplt_RiskLocation_Key.Address11 AS Address1,
	Mplt_RiskLocation_Key.Territory1 AS Territory,
	Mplt_RiskLocation_Key.ParentCoverageObjectName1 AS ParentCoverageObjectName,
	Mplt_RiskLocation_Key.CoverageGUID1 AS CoverageGUID,
	Mplt_RiskLocation_Key.PolicyKey,
	Mplt_RiskLocation_Key.Pol_AK_ID,
	Mplt_RiskLocation_Key.StateProvince1 AS StateProvince,
	Mplt_RiskLocation_Key.LocationXmlId1 AS LocationXmlId,
	Mplt_RiskLocation_Key.RiskLocationKey,
	Mplt_RiskLocation_Key.LineType1 AS LineType,
	Mplt_RiskLocation_Key.o_RiskLocationnAKID,
	Mplt_RiskLocation_Key.RiskLocation_Group,
	LKP_WBLocationAccountStage.CityTaxCode,
	LKP_WBLocationAccountStage.GeoTaxCountyDistrictCode,
	LKP_WBLocationAccountStage.GeoTaxCityDistrictCode,
	LKP_WBLocationAccountStage.KYTaxFactorAppliedIndicator,
	LKP_WBLocationAccountStage.TerritoryIllinoisFireTaxLocationCode
	FROM Mplt_RiskLocation_Key
	LEFT JOIN LKP_WBLocationAccountStage
	ON LKP_WBLocationAccountStage.SessionId = Mplt_RiskLocation_Key.SessionId1 AND LKP_WBLocationAccountStage.LocationId = Mplt_RiskLocation_Key.LocationId1
),
AGG_Remove_Duplicate AS (
	SELECT
	SessionId AS i_SessionId, 
	PartyId AS i_PartyId, 
	StateProvince AS i_StateProv, 
	PostalCode AS i_PostalCode, 
	PolicyGUId AS i_Id, 
	PolicyVersion AS i_PolicyVersion, 
	PolicyNumber AS i_PolicyNumber, 
	LocationNumber AS i_LocationNumber, 
	LocationXmlId AS i_LocationXmlId, 
	CustomerNum AS i_CustomerNumber, 
	CityTaxCode AS i_CityTaxCode, 
	GeoTaxCountyDistrictCode AS i_GeoTaxCountyDistrictCode, 
	GeoTaxCityDistrictCode AS i_GeoTaxCityDistrictCode, 
	KYTaxFactorAppliedIndicator AS i_KYTaxFactorAppliedIndicator, 
	TerritoryIllinoisFireTaxLocationCode AS i_TerritoryIllinoisFireTaxLocationCode, 
	Territory AS i_Territory, 
	City, 
	County, 
	Address1, 
	Pol_AK_ID AS pol_ak_id, 
	i_SessionId AS o_SessionID, 
	i_PartyId AS o_PartyId, 
	IFF(i_CustomerNumber IS NULL OR IS_SPACES(i_CustomerNumber) OR LENGTH(i_CustomerNumber) = 0, 'N/A', LTRIM(RTRIM(i_CustomerNumber))) AS o_CustomerNumber, 
	IFF(i_Id IS NULL OR IS_SPACES(i_Id) OR LENGTH(i_Id) = 0, 'N/A', LTRIM(RTRIM(i_Id))) AS o_Id, 
	IFF(i_PolicyNumber IS NULL OR IS_SPACES(i_PolicyNumber) OR LENGTH(i_PolicyNumber) = 0, 'N/A', LTRIM(RTRIM(i_PolicyNumber))) AS o_PolicyNumber, 
	IFF(i_PolicyVersion IS NULL, '00', LPAD(TO_CHAR(i_PolicyVersion), 2, '0')) AS o_PolicyVersion, 
	IFF(i_LocationNumber IS NULL OR IS_SPACES(i_LocationNumber) OR LENGTH(i_LocationNumber) = 0, '0000', LPAD(LTRIM(RTRIM(i_LocationNumber)), 4, '0')) AS o_LocationNumber, 
	IFF(i_Territory IS NULL OR IS_SPACES(i_Territory) OR LENGTH(i_Territory) = 0, 'N/A', LTRIM(RTRIM(i_Territory))) AS o_Territory, 
	i_LocationXmlId AS o_LocationXmlId, 
	i_StateProv AS o_StateProv, 
	i_PostalCode AS o_PostalCode, 
	IFF(i_CityTaxCode IS NULL OR IS_SPACES(i_CityTaxCode) OR LENGTH(i_CityTaxCode) = 0, 'N/A', LTRIM(RTRIM(i_CityTaxCode))) AS o_CityTaxCode, 
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_KYTaxFactorAppliedIndicator) AS o_KYTaxFactorAppliedIndicator, 
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_GeoTaxCountyDistrictCode) AS o_GeoTaxCountyDistrictCode, 
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_GeoTaxCityDistrictCode) AS o_GeoTaxCityDistrictCode, 
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_TerritoryIllinoisFireTaxLocationCode) AS o_TerritoryIllinoisFireTaxLocationCode, 
	RiskLocationKey AS o_RiskLocationKey, 
	o_RiskLocationnAKID AS o_RiskLocationAKID, 
	RiskLocation_Group AS RiskLocation_GroupKey
	FROM EXP_Extract_RiskLocationKey
	GROUP BY RiskLocation_GroupKey
),
LKP_DCWCStateDefaultStaging AS (
	SELECT
	ExperienceModRiskIDDefault,
	SessionId,
	PartyId,
	State
	FROM (
		select ptya.SessionId as SessionId,
		ptya.PartyId as PartyId,
		LTRIM(RTRIM(st.State)) as State,
		LTRIM(RTRIM(sd.ExperienceModRiskIDDefault)) as ExperienceModRiskIDDefault
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCWCStateDefaultStaging sd,
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.DCWCStateStaging st, 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLineStaging lin,
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.DCPartyAssociationStaging ptya
		where sd.SessionId = st.SessionId
		and sd.WC_StateId = st.WC_StateId
		and st.SessionId = lin.SessionId
		and st.LineId = lin.LineId
		and ptya.SessionId = lin.SessionId
		and ptya.PartyAssociationType = 'Account'
		and lin.Type = 'WorkersCompensation'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SessionId,PartyId,State ORDER BY ExperienceModRiskIDDefault) = 1
),
EXP_Values AS (
	SELECT
	AGG_Remove_Duplicate.City AS i_City,
	AGG_Remove_Duplicate.County AS i_County,
	AGG_Remove_Duplicate.Address1 AS i_Address1,
	AGG_Remove_Duplicate.o_CustomerNumber AS i_CustomerNumber,
	AGG_Remove_Duplicate.o_Id AS i_Id,
	AGG_Remove_Duplicate.o_PolicyNumber AS i_PolicyNumber,
	AGG_Remove_Duplicate.o_PolicyVersion AS i_PolicyVersion,
	AGG_Remove_Duplicate.o_LocationNumber AS i_LocationNumber,
	AGG_Remove_Duplicate.o_Territory AS i_Territory,
	AGG_Remove_Duplicate.o_LocationXmlId AS i_LocationXmlId,
	AGG_Remove_Duplicate.o_StateProv AS i_StateProv,
	AGG_Remove_Duplicate.o_PostalCode AS i_PostalCode,
	AGG_Remove_Duplicate.o_CityTaxCode AS i_CityTaxCode,
	LKP_DCWCStateDefaultStaging.ExperienceModRiskIDDefault AS i_ExperienceModRiskIDDefault,
	AGG_Remove_Duplicate.o_KYTaxFactorAppliedIndicator AS i_KYTaxFactorAppliedIndicator,
	AGG_Remove_Duplicate.o_GeoTaxCountyDistrictCode AS i_GeoTaxCountyDistrictCode,
	AGG_Remove_Duplicate.o_GeoTaxCityDistrictCode AS i_GeoTaxCityDistrictCode,
	AGG_Remove_Duplicate.o_TerritoryIllinoisFireTaxLocationCode AS i_TerritoryIllinoisFireTaxLocationCode,
	AGG_Remove_Duplicate.pol_ak_id AS i_pol_ak_id,
	-- *INF*: IIF(ISNULL(i_StateProv) or IS_SPACES(i_StateProv)  or LENGTH(i_StateProv)=0,'N/A',LTRIM(RTRIM(i_StateProv)))
	IFF(i_StateProv IS NULL OR IS_SPACES(i_StateProv) OR LENGTH(i_StateProv) = 0, 'N/A', LTRIM(RTRIM(i_StateProv))) AS v_StateProv,
	0 AS o_logicalIndicator,
	-- *INF*: IIF(ISNULL(i_pol_ak_id), -1, i_pol_ak_id)
	IFF(i_pol_ak_id IS NULL, - 1, i_pol_ak_id) AS v_pol_ak_id,
	-- *INF*: v_pol_ak_id
	-- 
	-- -- change policykey with PolAKID as per UID Change
	-- --v_Pol_Key
	-- 
	v_pol_ak_id AS o_Pol_Key,
	i_LocationNumber AS o_LocationNumber,
	-- *INF*: IIF(i_LocationNumber = '0000','N','Y')
	-- 
	-- 
	-- 
	IFF(i_LocationNumber = '0000', 'N', 'Y') AS o_locationIndicator,
	-- *INF*: IIF(ISNULL(i_Territory) or IS_SPACES(i_Territory)  or LENGTH(i_Territory)=0,'N/A',LTRIM(RTRIM(i_Territory)))
	IFF(i_Territory IS NULL OR IS_SPACES(i_Territory) OR LENGTH(i_Territory) = 0, 'N/A', LTRIM(RTRIM(i_Territory))) AS o_Territory,
	v_StateProv AS o_StateProv,
	-- *INF*: IIF(ISNULL(i_PostalCode) or IS_SPACES(i_PostalCode)  or LENGTH(i_PostalCode)=0,'N/A',LTRIM(RTRIM(i_PostalCode)))
	IFF(i_PostalCode IS NULL OR IS_SPACES(i_PostalCode) OR LENGTH(i_PostalCode) = 0, 'N/A', LTRIM(RTRIM(i_PostalCode))) AS o_PostalCode,
	-- *INF*: DECODE(TRUE,
	-- --IL
	-- i_StateProv = 'IL' and i_TerritoryIllinoisFireTaxLocationCode <> 'N/A', i_TerritoryIllinoisFireTaxLocationCode,
	-- --Other states
	-- i_KYTaxFactorAppliedIndicator='City',i_GeoTaxCityDistrictCode,
	-- i_KYTaxFactorAppliedIndicator='County',i_GeoTaxCountyDistrictCode,
	-- 'N/A'
	-- )
	DECODE(TRUE,
	i_StateProv = 'IL' AND i_TerritoryIllinoisFireTaxLocationCode <> 'N/A', i_TerritoryIllinoisFireTaxLocationCode,
	i_KYTaxFactorAppliedIndicator = 'City', i_GeoTaxCityDistrictCode,
	i_KYTaxFactorAppliedIndicator = 'County', i_GeoTaxCountyDistrictCode,
	'N/A') AS o_TaxLocation,
	-- *INF*: IIF(ISNULL(i_City) OR IS_SPACES(i_City) OR LENGTH(i_City)=0,'N/A',LTRIM(RTRIM(i_City)))
	IFF(i_City IS NULL OR IS_SPACES(i_City) OR LENGTH(i_City) = 0, 'N/A', LTRIM(RTRIM(i_City))) AS o_RatingCity,
	-- *INF*: IIF(ISNULL(i_County) OR IS_SPACES(i_County) OR LENGTH(i_County)=0,'N/A',LTRIM(RTRIM(i_County)))
	IFF(i_County IS NULL OR IS_SPACES(i_County) OR LENGTH(i_County) = 0, 'N/A', LTRIM(RTRIM(i_County))) AS v_RatingCounty,
	-- *INF*: LTRIM(RTRIM(REPLACESTR(0,v_RatingCounty,'county','')))
	LTRIM(RTRIM(REPLACESTR(0, v_RatingCounty, 'county', ''))) AS o_RatingCounty,
	i_CityTaxCode AS o_TaxCode,
	-- *INF*: IIF(ISNULL(i_Address1) OR IS_SPACES(i_Address1) OR LENGTH(i_Address1)=0,'N/A',LTRIM(RTRIM(i_Address1)))
	IFF(i_Address1 IS NULL OR IS_SPACES(i_Address1) OR LENGTH(i_Address1) = 0, 'N/A', LTRIM(RTRIM(i_Address1))) AS o_Address1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(SUBSTR(i_ExperienceModRiskIDDefault,1,9))
	:UDF.DEFAULT_VALUE_FOR_STRINGS(SUBSTR(i_ExperienceModRiskIDDefault, 1, 9)) AS o_IntrastateRiskID,
	AGG_Remove_Duplicate.o_RiskLocationKey,
	AGG_Remove_Duplicate.o_RiskLocationAKID
	FROM AGG_Remove_Duplicate
	LEFT JOIN LKP_DCWCStateDefaultStaging
	ON LKP_DCWCStateDefaultStaging.SessionId = AGG_Remove_Duplicate.o_SessionID AND LKP_DCWCStateDefaultStaging.PartyId = AGG_Remove_Duplicate.o_PartyId AND LKP_DCWCStateDefaultStaging.State = AGG_Remove_Duplicate.o_StateProv
),
LKP_SupCounty_IL AS (
	SELECT
	TaxLocationCountyCode,
	CountyName,
	StateCode
	FROM (
		select ltrim(rtrim(a.CountyName)) as CountyName,
		ltrim(rtrim(b.state_code)) as StateCode,
		a.TaxLocationCountyCode as TaxLocationCountyCode
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.SupCounty a
		join @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_state b
		on a.StateAbbreviation=b.state_code
		where a.CurrentSnapshotFlag=1
		and b.crrnt_snpsht_flag=1
		and a.StateAbbreviation in ('IL')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CountyName,StateCode ORDER BY TaxLocationCountyCode) = 1
),
LKP_ISOFireProtectStage AS (
	SELECT
	City,
	TaxLoc_County,
	TaxLoc_City
	FROM (
		SELECT  City AS City,  case when substring(TaxLoc,1,2)='00' then 'N/A' 
		else  substring(TaxLoc,1,2) end AS TaxLoc_County,  substring(TaxLoc,3,4) AS TaxLoc_City from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.ISOFireProtectStage
		where ISOExpDate='2999-12-31 00:00:00.000'
		and taxloc is not null
		and ltrim(rtrim(taxloc)) <> '' ORDER BY TaxLoc_County,TaxLoc_City,City
		---
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY TaxLoc_County,TaxLoc_City ORDER BY City) = 1
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
		select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT
		where WCT.PolicyNumber=pol.pol_num
		and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol.pol_mod)
		and pol.crrnt_snpsht_flag=1)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationAKID ORDER BY RiskLocationAKID) = 1
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
	EXP_Values.o_logicalIndicator AS i_LogicalIndicator,
	EXP_Values.o_RiskLocationKey AS i_RiskLocationKey,
	EXP_Values.o_LocationNumber AS i_LocationUnitNumber,
	EXP_Values.o_locationIndicator AS i_LocationIndicator,
	EXP_Values.o_Territory AS i_RiskTerritory,
	EXP_Values.o_PostalCode AS i_ZipPostalCode,
	EXP_Values.o_TaxLocation AS i_TaxLocation,
	EXP_Values.o_RatingCity AS i_RatingCity,
	EXP_Values.o_RatingCounty AS i_RatingCounty,
	EXP_Values.o_TaxCode AS i_TaxCode,
	EXP_Values.o_Address1 AS i_Address1,
	LKP_ISOFireProtectStage.City AS i_ISOFireProtectCity,
	-- *INF*: IIF(i_state_abbrev='12' and NOT ISNULL(i_ISOFireProtectCity),i_ISOFireProtectCity,'N/A')
	IFF(i_state_abbrev = '12' AND NOT i_ISOFireProtectCity IS NULL, i_ISOFireProtectCity, 'N/A') AS v_ISOFireProtectCity,
	-- *INF*: IIF(ISNULL(i_state_abbrev), 'N/A', i_state_abbrev)
	IFF(i_state_abbrev IS NULL, 'N/A', i_state_abbrev) AS v_StateProvinceCode,
	-- *INF*: MD5(i_Address1||i_RatingCity||v_StateProvinceCode||i_ZipPostalCode)
	MD5(i_Address1 || i_RatingCity || v_StateProvinceCode || i_ZipPostalCode) AS v_RiskLocationHashKey,
	-- *INF*:   IIF(ISNULL(lkp_RiskLocationAKID), 'NEW', 
	-- IIF(
	-- LTRIM(RTRIM(lkp_LocationIndicator)) != LTRIM(RTRIM(i_LocationIndicator)) OR LTRIM(RTRIM(lkp_LocationUnitNumber)) != LTRIM(RTRIM(i_LocationUnitNumber)) OR LTRIM(RTRIM(lkp_RiskTerritory)) != LTRIM(RTRIM(i_RiskTerritory)) OR
	-- LTRIM(RTRIM(lkp_TaxLocation)) != LTRIM(RTRIM(i_TaxLocation)) OR lkp_sup_state_id != i_sup_state_id OR LTRIM(RTRIM(lkp_KYTaxCode)) != LTRIM(RTRIM(i_TaxCode)) OR LTRIM(RTRIM(lkp_RiskLocationHashKey)) != LTRIM(RTRIM(v_RiskLocationHashKey))  OR LTRIM(RTRIM(lkp_IntrastateRiskId)) != LTRIM(RTRIM(IntrastateRiskID)) OR LTRIM(RTRIM(lkp_ISOFireProtectCity)) != LTRIM(RTRIM(v_ISOFireProtectCity)) ,
	-- 'UPDATE', 'NOCHANGE'))
	IFF(lkp_RiskLocationAKID IS NULL, 'NEW', IFF(LTRIM(RTRIM(lkp_LocationIndicator)) != LTRIM(RTRIM(i_LocationIndicator)) OR LTRIM(RTRIM(lkp_LocationUnitNumber)) != LTRIM(RTRIM(i_LocationUnitNumber)) OR LTRIM(RTRIM(lkp_RiskTerritory)) != LTRIM(RTRIM(i_RiskTerritory)) OR LTRIM(RTRIM(lkp_TaxLocation)) != LTRIM(RTRIM(i_TaxLocation)) OR lkp_sup_state_id != i_sup_state_id OR LTRIM(RTRIM(lkp_KYTaxCode)) != LTRIM(RTRIM(i_TaxCode)) OR LTRIM(RTRIM(lkp_RiskLocationHashKey)) != LTRIM(RTRIM(v_RiskLocationHashKey)) OR LTRIM(RTRIM(lkp_IntrastateRiskId)) != LTRIM(RTRIM(IntrastateRiskID)) OR LTRIM(RTRIM(lkp_ISOFireProtectCity)) != LTRIM(RTRIM(v_ISOFireProtectCity)), 'UPDATE', 'NOCHANGE')) AS v_Changed_Flag,
	v_Changed_Flag AS o_Changed_Flag,
	lkp_RiskLocationAKID AS o_RiskLocationAKID,
	i_LogicalIndicator AS o_LogicalIndicator,
	EXP_Values.o_Pol_Key AS o_PolicyAKID,
	-- *INF*: IIF(ISNULL(lkp_RiskLocationAKID),i_RiskLocationKey,lkp_RiskLocationKey)
	IFF(lkp_RiskLocationAKID IS NULL, i_RiskLocationKey, lkp_RiskLocationKey) AS o_RiskLocationKey,
	i_LocationUnitNumber AS o_LocationUnitNumber,
	i_LocationIndicator AS o_LocationIndicator,
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
	EXP_Values.o_IntrastateRiskID AS IntrastateRiskID
	FROM EXP_Values
	LEFT JOIN LKP_ISOFireProtectStage
	ON LKP_ISOFireProtectStage.TaxLoc_County = LKP_SupCounty_IL.TaxLocationCountyCode AND LKP_ISOFireProtectStage.TaxLoc_City = EXP_Values.o_TaxLocation
	LEFT JOIN LKP_RiskLocation
	ON LKP_RiskLocation.RiskLocationAKID = EXP_Values.o_RiskLocationAKID
	LEFT JOIN LKP_SupState
	ON LKP_SupState.state_code = EXP_Values.o_StateProv
),
FIL_Insert AS (
	SELECT
	o_Changed_Flag AS Changed_Flag, 
	o_RiskLocationAKID AS RiskLocationAKID, 
	o_LogicalIndicator AS LogicalIndicator, 
	o_PolicyAKID AS PolicyAKID, 
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
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation AS T
	USING UPD_RiskLocation AS S
	ON T.RiskLocationID = S.RiskLocationID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.crrnt_snpsht_flag, T.ExpirationDate = S.eff_to_date, T.ModifiedDate = S.modified_date

	------------ POST SQL ----------
	exec [spSetIndexStatus] @Enable = 1, @Schema = 'dbo', @TableName = 'RiskLocation', @IndexWildcard = 'Ak1RiskLocation'
	-------------------------------


),
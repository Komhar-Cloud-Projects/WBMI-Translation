WITH
LKP_ProcessStatus AS (
	SELECT
	SupProcessStatusId,
	ProcessStatus
	FROM (
		SELECT 
			SupProcessStatusId,
			ProcessStatus
		FROM SupProcessStatus
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProcessStatus ORDER BY SupProcessStatusId) = 1
),
SQ_ChangedPolicyParties AS (
	Declare @Date as Datetime
	
	Set @Date= DATEADD(MI, -@{pipeline().parameters.TIME_OFFSET},CAST('@{pipeline().parameters.SELECTION_START_TS}' AS DATETIME))
	
	SELECT DC_Contact.PartyId, DC_Contact.ContactId, DC_Contact.SessionId As SessionId1, DC_Contact.Type, DC_Contact.PhoneNumber, DC_Contact.PhoneExtension, DC_Contact.Email, DC_Location.LocationId, DC_Location.SessionId As SessionId2, DC_Location.LocationXmlId, DC_Location.Deleted, DC_Location.Description, DC_Location.Address1, DC_Location.Address2, DC_Location.City, DC_Location.County, DC_Location.StateProv, DC_Location.PostalCode, DC_Location.Country, DC_LocationAssociation.ObjectId, DC_LocationAssociation.ObjectName, DC_LocationAssociation.LocationId, DC_LocationAssociation.LocationAssociationId, DC_LocationAssociation.SessionId As SessionId3, DC_LocationAssociation.LocationXmlId, DC_LocationAssociation.LocationAssociationType, DC_Party.PartyId, DC_Party.SessionId As SessionId4, DC_Party.PartyXmlId, DC_Party.Type, DC_Party.OtherType, DC_Party.Name, DC_Party.DateOfBirth, DC_Party.Gender, DC_Party.FirstName, DC_Party.LastName, DC_Party.MiddleName, DC_Party.MaritalStatus, DC_Party.Title, DC_Party.Reference, DC_Party.ContactName, DC_PartyAssociation.ObjectId, DC_PartyAssociation.ObjectName, DC_PartyAssociation.PartyId, DC_PartyAssociation.PartyAssociationId, DC_PartyAssociation.SessionId As SessionId5, DC_PartyAssociation.PartyXmlId As PartyXmlId2, DC_PartyAssociation.PartyAssociationType, DC_PartyAssociation.PartyAssociationStatus, DC_PartyAssociation.PartyReference, DC_PartyAssociation.Description, DC_PartyAssociation.EntityType, DC_PartyAssociation.EntityOtherType, DC_PartyAssociation.FederalEmployeeIDNumber, DC_PartyAssociation.CompanyNumber, DC_PartyAssociation.LicensePlateNumber, DC_Policy.PolicyId, DC_Policy.SessionId As SessionId6, DC_Policy.Id, DC_Policy.EffectiveDate, DC_Policy.ExpirationDate, DC_Policy.LineOfBusiness, DC_Policy.Term, DC_Policy.PrimaryRatingState, DC_Policy.Product, DC_Policy.HonorRates, DC_Policy.AuditPeriod, DC_Policy.SICCode, DC_Policy.SICCodeDesc, DC_Policy.NAICSCode, DC_Policy.NAICSCodeDesc, DC_Policy.QuoteNumber, DC_Policy.TermFactor, DC_Policy.CancellationDate, DC_Policy.Description, DC_Policy.PolicyNumber, DC_Policy.Status, DC_Policy.TransactionDate, DC_Policy.TransactionDateTime, DC_Policy.PreviousPolicyNumber, DC_Policy.InceptionDate, DC_Policy.PolicyTermID, DC_Policy.AccountID, DC_Policy.TaxesSurcharges, DC_Policy.Auditable, DC_Session.SessionId, DC_Session.ExampleQuoteId, DC_Session.UserName, DC_Session.CreateDateTime, DC_Session.Purpose, (select COUNT(*) from DC_Transaction where DC_Transaction.SessionId = DC_Session.SessionId and DC_Transaction.Type <> 'New') as NonNewBusinessTransactionCount
	,0,0,'',null,null,null,null,null,null,null,null,null,null,0,0,0,null,null,null,null,null,null,null,null,null,null,null,null,null,null
	FROM
	DC_Session
	  join DC_Policy on DC_Session.SessionId = DC_Policy.SessionId and 
		DC_Policy.Status in ('InForce', 'NonRenewed', 'Cancelled', 'Cancel-Pending')
	  join DC_PartyAssociation 
		on DC_Session.SessionId = DC_PartyAssociation.SessionId
			and ((DC_PartyAssociation.PartyAssociationType in ('Account', 'Registrant', 'AdditionalInsured', 'LossPayee', 'Mortgagee')) or 
			 (SUBSTRING(DC_PartyAssociation.PartyAssociationType, 1, 2) in ('WB', 'CA', 'CG', 'CR', 'BP', 'CU', 'IM')))
	  join DC_Party on DC_PartyAssociation.PartyId = DC_Party.PartyId and DC_PartyAssociation.SessionId = DC_Party.SessionId and DC_Party.PartyXmlId is not null
	 join WB_Transaction on DC_Policy.SessionId = WB_Transaction.SessionId and (WB_Transaction.DataFix != 'Y' OR WB_Transaction.DataFix is NULL)
	  
	  left outer join DC_LocationAssociation
	  on 
		DC_LocationAssociation.SessionId = DC_PartyAssociation.SessionId and
		((DC_PartyAssociation.SessionId = DC_LocationAssociation.ObjectId 
		and DC_LocationAssociation.ObjectName = 'DC_Session' 
		and DC_LocationAssociation.LocationAssociationType = DC_PartyAssociation.PartyAssociationType) or 
		(DC_LocationAssociation.ObjectName <> 'DC_Session' and
		DC_PartyAssociation.ObjectId = DC_LocationAssociation.ObjectId 
		and DC_LocationAssociation.ObjectName = DC_PartyAssociation.ObjectName))
	  
	  left outer join DC_Location on DC_LocationAssociation.LocationId = DC_Location.LocationId
	  left outer join DC_Contact on DC_Party.PartyId = DC_Contact.PartyId and DC_Contact.Type = 'Primary'
	  where DC_Session.CreateDateTime >  @Date
	   
	union all
	
	SELECT null, 0, null, null, null, null, null, DC_Location.LocationId, DC_Location.SessionId As SessionId2, DC_Location.LocationXmlId, DC_Location.Deleted, DC_Location.Description, DC_Location.Address1, DC_Location.Address2, DC_Location.City, DC_Location.County, DC_Location.StateProv, DC_Location.PostalCode, DC_Location.Country, DC_LocationAssociation.ObjectId, DC_LocationAssociation.ObjectName, DC_LocationAssociation.LocationId, DC_LocationAssociation.LocationAssociationId, DC_LocationAssociation.SessionId As SessionId3, DC_LocationAssociation.LocationXmlId, DC_LocationAssociation.LocationAssociationType, 0, 0, DC_CA_Driver.Id As PartyXmlId, '', '', '', WB_CA_Driver.DateOfBirth As DateOfBirth, '', WB_CA_Driver.Name As FirstName, WB_CA_Driver.LastName As LastName, '', '', '', '', '', 0, '', null, 0, null, null, null, null, null, null, null, null, null, null, null, DC_Policy.PolicyId, DC_Policy.SessionId As SessionId6, DC_Policy.Id, DC_Policy.EffectiveDate, DC_Policy.ExpirationDate, DC_Policy.LineOfBusiness, DC_Policy.Term, DC_Policy.PrimaryRatingState, DC_Policy.Product, DC_Policy.HonorRates, DC_Policy.AuditPeriod, DC_Policy.SICCode, DC_Policy.SICCodeDesc, DC_Policy.NAICSCode, DC_Policy.NAICSCodeDesc, DC_Policy.QuoteNumber, DC_Policy.TermFactor, DC_Policy.CancellationDate, DC_Policy.Description, DC_Policy.PolicyNumber, DC_Policy.Status, DC_Policy.TransactionDate, DC_Policy.TransactionDateTime, DC_Policy.PreviousPolicyNumber, DC_Policy.InceptionDate, DC_Policy.PolicyTermID, DC_Policy.AccountID, DC_Policy.TaxesSurcharges, DC_Policy.Auditable, DC_Session.SessionId, DC_Session.ExampleQuoteId, DC_Session.UserName, DC_Session.CreateDateTime, DC_Session.Purpose, (select COUNT(*) from DC_Transaction where DC_Transaction.SessionId = DC_Session.SessionId and DC_Transaction.Type <> 'New') as NonNewBusinessTransactionCount,
	      DC_CA_Driver.CA_DriverId
	      ,DC_CA_Driver.SessionId
	      ,DC_CA_Driver.Id
	      ,DC_CA_Driver.BroadenNoFault
	      ,DC_CA_Driver.DateOfHire
	      ,DC_CA_Driver.DriversLicenseNumber
	      ,DC_CA_Driver.JobTitle
	      ,DC_CA_Driver.PercentageOfUse
	      ,DC_CA_Driver.StateLicensed
	      ,DC_CA_Driver.UseVehicleNumber
	      ,DC_CA_Driver.YearsExperience
	      ,DC_CA_Driver.YearLicensed
	      ,DC_CA_Driver.LineId
		  ,WB_CA_Driver.CA_DriverId
	      ,WB_CA_Driver.WB_CA_DriverId
	      ,WB_CA_Driver.SessionId
	      ,WB_CA_Driver.ExcludeDriver
	      ,WB_CA_Driver.WatchDriver
	      ,WB_CA_Driver.PermanentDriver
	      ,WB_CA_Driver.MVRDate
	      ,WB_CA_Driver.MVRStatus
	      ,WB_CA_Driver.SelectForMVR
	      ,WB_CA_Driver.TaskFlagCAMVRViolationCategoryNotFound
	      ,WB_CA_Driver.TaskFlagCAMVRViolationCategoryNotFoundEARS
	      ,WB_CA_Driver.DateOfBirth
	      ,WB_CA_Driver.Name
	      ,WB_CA_Driver.MiddleInitial
	      ,WB_CA_Driver.LastName
	      ,WB_CA_Driver.Gender
	      ,WB_CA_Driver.MaritalStatus
	FROM
	DC_Session
	  join DC_Policy on DC_Session.SessionId = DC_Policy.SessionId and 
		DC_Policy.Status in ('InForce', 'NonRenewed', 'Cancelled', 'Cancel-Pending')
	  join DC_CA_Driver on DC_Session.SessionId = DC_CA_Driver.SessionId
	  join WB_CA_Driver on DC_Session.SessionId = WB_CA_Driver.SessionId and WB_CA_Driver.CA_DriverId = DC_CA_Driver.CA_DriverId
	 join WB_Transaction on DC_Policy.SessionId = WB_Transaction.SessionId and (WB_Transaction.DataFix != 'Y' OR WB_Transaction.DataFix is NULL)
	  left outer join DC_LocationAssociation
	  on 
		DC_LocationAssociation.SessionId = DC_Session.SessionId and
		DC_CA_Driver.Id = DC_LocationAssociation.LocationXmlId 
		and DC_LocationAssociation.ObjectName = 'DC_CA_Driver' 
	  
	  left outer join DC_Location on DC_LocationAssociation.LocationId = DC_Location.LocationId
	   where DC_Session.CreateDateTime > @Date
	 	and DC_CA_Driver.Id is not NULL 
	   
	  -- order by PolicyNumber, PartyXmlId, SessionId
),
SRTTRANS AS (
	SELECT
	PolicyNumber, 
	PartyXmlId, 
	PartyId, 
	ContactId, 
	SessionId, 
	Type, 
	PhoneNumber, 
	PhoneExtension, 
	Email, 
	LocationId, 
	SessionId1, 
	LocationXmlId, 
	Deleted, 
	Description, 
	Address1, 
	Address2, 
	City, 
	County, 
	StateProv, 
	PostalCode, 
	Country, 
	ObjectId, 
	ObjectName, 
	LocationId1, 
	LocationAssociationId, 
	SessionId2, 
	LocationXmlId1, 
	LocationAssociationType, 
	PartyId1, 
	SessionId3, 
	Type1, 
	OtherType, 
	Name, 
	DateOfBirth, 
	Gender, 
	FirstName, 
	LastName, 
	MiddleName, 
	MaritalStatus, 
	Title, 
	Reference, 
	ContactName, 
	ObjectId1, 
	ObjectName1, 
	PartyId2, 
	PartyAssociationId, 
	SessionId4, 
	PartyXmlId1, 
	PartyAssociationType, 
	PartyAssociationStatus, 
	PartyReference, 
	Description1, 
	EntityType, 
	EntityOtherType, 
	FederalEmployeeIDNumber, 
	CompanyNumber, 
	LicensePlateNumber, 
	PolicyId, 
	SessionId5, 
	Id, 
	EffectiveDate, 
	ExpirationDate, 
	LineOfBusiness, 
	Term, 
	PrimaryRatingState, 
	Product, 
	HonorRates, 
	AuditPeriod, 
	SICCode, 
	SICCodeDesc, 
	NAICSCode, 
	NAICSCodeDesc, 
	QuoteNumber, 
	TermFactor, 
	CancellationDate, 
	Description2, 
	Status, 
	TransactionDate, 
	TransactionDateTime, 
	PreviousPolicyNumber, 
	InceptionDate, 
	PolicyTermID, 
	AccountID, 
	TaxesSurcharges, 
	Auditable, 
	SessionId6, 
	ExampleQuoteId, 
	UserName, 
	CreateDateTime, 
	Purpose, 
	NonNewBusinessTransactionCount, 
	LineId, 
	CA_DriverId, 
	SessionId7, 
	Id1, 
	BroadenNoFault, 
	DateOfHire, 
	DriversLicenseNumber, 
	JobTitle, 
	PercentageOfUse, 
	StateLicensed, 
	UseVehicleNumber, 
	YearsExperience, 
	YearLicensed, 
	CA_DriverId1, 
	WB_CA_DriverId, 
	SessionId8, 
	DateOfBirth1, 
	Name1, 
	MiddleInitial, 
	LastName1, 
	Gender1, 
	MaritalStatus1, 
	ExcludeDriver, 
	WatchDriver, 
	PermanentDriver, 
	SelectForMVR, 
	TaskFlagCAMVRViolationCategoryNotFound, 
	TaskFlagCAMVRViolationCategoryNotFoundEARS, 
	MVRDate, 
	MVRStatus, 
	TransactionId, 
	WB_TransactionId, 
	SessionId9, 
	ProRataFactor, 
	CreatedUserContext, 
	QuoteActionUserClassification, 
	QuoteActionTimeStamp, 
	QuoteActionUserName, 
	QuoteActionStatus, 
	VerifiedDate, 
	DataFix, 
	DataFixDate, 
	DataFixType, 
	HistoryIDOriginal, 
	OriginalID, 
	OnsetBy
	FROM SQ_ChangedPolicyParties
	ORDER BY PolicyNumber ASC, PartyXmlId ASC, SessionId ASC
),
EXP_ChangedPolicyParties AS (
	SELECT
	PartyId,
	ContactId,
	SessionId,
	Type,
	PhoneNumber,
	PhoneExtension,
	Email,
	LocationId,
	SessionId1,
	LocationXmlId,
	Deleted,
	Description,
	Address1,
	Address2,
	City,
	County,
	StateProv,
	PostalCode,
	Country,
	ObjectId,
	ObjectName,
	LocationId1,
	LocationAssociationId,
	SessionId2,
	LocationXmlId1,
	LocationAssociationType,
	PartyId1,
	SessionId3,
	PartyXmlId,
	Type1,
	OtherType,
	Name,
	DateOfBirth,
	Gender,
	FirstName,
	LastName,
	MiddleName,
	MaritalStatus,
	Title,
	Reference,
	ContactName,
	ObjectId1,
	ObjectName1,
	PartyId2,
	PartyAssociationId,
	SessionId4,
	PartyXmlId1,
	PartyAssociationType,
	PartyAssociationStatus,
	PartyReference,
	Description1,
	EntityType,
	EntityOtherType,
	FederalEmployeeIDNumber,
	CompanyNumber,
	LicensePlateNumber,
	PolicyId,
	SessionId5,
	Id,
	EffectiveDate,
	ExpirationDate,
	LineOfBusiness,
	Term,
	PrimaryRatingState,
	Product,
	HonorRates,
	AuditPeriod,
	SICCode,
	SICCodeDesc,
	NAICSCode,
	NAICSCodeDesc,
	QuoteNumber,
	TermFactor,
	CancellationDate,
	Description2,
	PolicyNumber,
	Status,
	TransactionDate,
	TransactionDateTime,
	PreviousPolicyNumber,
	InceptionDate,
	PolicyTermID,
	AccountID,
	TaxesSurcharges,
	Auditable,
	SessionId6,
	ExampleQuoteId,
	UserName,
	CreateDateTime,
	Purpose,
	NonNewBusinessTransactionCount,
	-- *INF*: IIF(ISNULL(LastName), Name, LastName)
	IFF(LastName IS NULL, Name, LastName) AS out_LastOrCompanyName,
	0 AS out_PolicyVersion,
	-- *INF*: SYSTIMESTAMP()
	CURRENT_TIMESTAMP() AS out_ChangedDate,
	-- *INF*: :LKP.LKP_PROCESSSTATUS('Request')
	LKP_PROCESSSTATUS__Request.SupProcessStatusId AS out_RequestStatusId,
	-- *INF*: :LKP.LKP_PROCESSSTATUS('Complete')
	LKP_PROCESSSTATUS__Complete.SupProcessStatusId AS out_CompleteStatusId,
	-- *INF*: IIF(NonNewBusinessTransactionCount = 0, 'Y', 'N')
	IFF(NonNewBusinessTransactionCount = 0, 'Y', 'N') AS out_PolicySessionIsNewBusiness,
	LineId,
	CA_DriverId,
	SessionId7,
	Id1,
	BroadenNoFault,
	DateOfHire,
	DriversLicenseNumber,
	JobTitle,
	PercentageOfUse,
	StateLicensed,
	UseVehicleNumber,
	YearsExperience,
	YearLicensed,
	CA_DriverId1,
	WB_CA_DriverId,
	SessionId8,
	DateOfBirth1,
	Name1,
	MiddleInitial,
	LastName1,
	Gender1,
	MaritalStatus1,
	ExcludeDriver,
	WatchDriver,
	PermanentDriver,
	SelectForMVR,
	TaskFlagCAMVRViolationCategoryNotFound,
	TaskFlagCAMVRViolationCategoryNotFoundEARS,
	MVRDate,
	MVRStatus
	FROM SRTTRANS
	LEFT JOIN LKP_PROCESSSTATUS LKP_PROCESSSTATUS__Request
	ON LKP_PROCESSSTATUS__Request.ProcessStatus = 'Request'

	LEFT JOIN LKP_PROCESSSTATUS LKP_PROCESSSTATUS__Complete
	ON LKP_PROCESSSTATUS__Complete.ProcessStatus = 'Complete'

),
AGG_GroupByPolicyNumber AS (
	SELECT
	PolicyNumber,
	PartyId,
	ContactId,
	SessionId,
	Type,
	PhoneNumber,
	PhoneExtension,
	Email,
	LocationId,
	SessionId1,
	LocationXmlId,
	Deleted,
	Description,
	Address1,
	Address2,
	City,
	County,
	StateProv,
	PostalCode,
	Country,
	ObjectId,
	ObjectName,
	LocationId1,
	LocationAssociationId,
	SessionId2,
	LocationXmlId1,
	LocationAssociationType,
	PartyId1,
	SessionId3,
	PartyXmlId,
	Type1,
	OtherType,
	Name,
	DateOfBirth,
	Gender,
	FirstName,
	LastName,
	MiddleName,
	MaritalStatus,
	Title,
	Reference,
	ContactName,
	ObjectId1,
	ObjectName1,
	PartyId2,
	PartyAssociationId,
	SessionId4,
	PartyXmlId1,
	PartyAssociationType,
	PartyAssociationStatus,
	PartyReference,
	Description1,
	EntityType,
	EntityOtherType,
	FederalEmployeeIDNumber,
	CompanyNumber,
	LicensePlateNumber,
	PolicyId,
	SessionId5,
	Id,
	EffectiveDate,
	ExpirationDate,
	LineOfBusiness,
	Term,
	PrimaryRatingState,
	Product,
	HonorRates,
	AuditPeriod,
	SICCode,
	SICCodeDesc,
	NAICSCode,
	NAICSCodeDesc,
	QuoteNumber,
	TermFactor,
	CancellationDate,
	Description2,
	Status,
	TransactionDate,
	TransactionDateTime,
	PreviousPolicyNumber,
	InceptionDate,
	PolicyTermID,
	AccountID,
	TaxesSurcharges,
	Auditable,
	SessionId6,
	ExampleQuoteId,
	UserName,
	CreateDateTime,
	Purpose,
	out_LastOrCompanyName,
	out_PolicyVersion,
	out_ChangedDate,
	out_RequestStatusId,
	out_CompleteStatusId,
	out_PolicySessionIsNewBusiness,
	LineId,
	CA_DriverId,
	SessionId7,
	Id1,
	BroadenNoFault,
	DateOfHire,
	DriversLicenseNumber,
	JobTitle,
	PercentageOfUse,
	StateLicensed,
	UseVehicleNumber,
	YearsExperience,
	YearLicensed,
	CA_DriverId1,
	WB_CA_DriverId,
	SessionId8,
	DateOfBirth1,
	Name1,
	MiddleInitial,
	LastName1,
	Gender1,
	MaritalStatus1,
	ExcludeDriver,
	WatchDriver,
	PermanentDriver,
	SelectForMVR,
	TaskFlagCAMVRViolationCategoryNotFound,
	TaskFlagCAMVRViolationCategoryNotFoundEARS,
	MVRDate,
	MVRStatus
	FROM EXP_ChangedPolicyParties
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber, PartyXmlId ORDER BY NULL) = 1
),
LKP_Target_DCTClaimClientStage AS (
	SELECT
	DCTClaimClientStageId,
	FirstName,
	LastName,
	Street1,
	Street2,
	City,
	StateCode,
	ZipCode,
	WorkPhoneNumber,
	BirthDate,
	PolicyNumber,
	AgreementPartyId
	FROM (
		SELECT 
			DCTClaimClientStageId,
			FirstName,
			LastName,
			Street1,
			Street2,
			City,
			StateCode,
			ZipCode,
			WorkPhoneNumber,
			BirthDate,
			PolicyNumber,
			AgreementPartyId
		FROM DCTClaimClientStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber,AgreementPartyId ORDER BY DCTClaimClientStageId) = 1
),
RTR_InsertOrUpdateToTarget AS (
	SELECT
	LKP_Target_DCTClaimClientStage.DCTClaimClientStageId,
	AGG_GroupByPolicyNumber.PolicyNumber AS in_PolicyNumber,
	AGG_GroupByPolicyNumber.PartyXmlId AS in_PartyXmlId,
	AGG_GroupByPolicyNumber.FirstName AS NewFirstName,
	AGG_GroupByPolicyNumber.out_LastOrCompanyName AS NewLastOrCompanyName,
	AGG_GroupByPolicyNumber.Address1 AS NewAddress1,
	AGG_GroupByPolicyNumber.Address2 AS NewAddress2,
	AGG_GroupByPolicyNumber.City AS NewCity,
	AGG_GroupByPolicyNumber.StateProv AS NewStateProv,
	AGG_GroupByPolicyNumber.PostalCode AS NewPostalCode,
	AGG_GroupByPolicyNumber.PhoneNumber AS NewPhoneNumber,
	AGG_GroupByPolicyNumber.DateOfBirth AS NewDateOfBirth,
	AGG_GroupByPolicyNumber.out_PolicyVersion,
	AGG_GroupByPolicyNumber.out_ChangedDate,
	AGG_GroupByPolicyNumber.out_RequestStatusId,
	AGG_GroupByPolicyNumber.out_CompleteStatusId,
	AGG_GroupByPolicyNumber.out_PolicySessionIsNewBusiness,
	LKP_Target_DCTClaimClientStage.FirstName AS OldFirstName,
	LKP_Target_DCTClaimClientStage.LastName AS OldLastOrCompanyName,
	LKP_Target_DCTClaimClientStage.Street1 AS OldAddress1,
	LKP_Target_DCTClaimClientStage.Street2 AS OldAddress2,
	LKP_Target_DCTClaimClientStage.City AS OldCity,
	LKP_Target_DCTClaimClientStage.StateCode AS OldStateProv,
	LKP_Target_DCTClaimClientStage.ZipCode AS OldPostalCode,
	LKP_Target_DCTClaimClientStage.WorkPhoneNumber AS OldPhoneNumber,
	LKP_Target_DCTClaimClientStage.BirthDate AS OldDateOfBirth
	FROM AGG_GroupByPolicyNumber
	LEFT JOIN LKP_Target_DCTClaimClientStage
	ON LKP_Target_DCTClaimClientStage.PolicyNumber = AGG_GroupByPolicyNumber.PolicyNumber AND LKP_Target_DCTClaimClientStage.AgreementPartyId = AGG_GroupByPolicyNumber.PartyXmlId
),
RTR_InsertOrUpdateToTarget_Insert_NewBusiness AS (SELECT * FROM RTR_InsertOrUpdateToTarget WHERE ISNULL(DCTClaimClientStageId) AND out_PolicySessionIsNewBusiness = 'Y'),
RTR_InsertOrUpdateToTarget_Update_PartyChanged AS (SELECT * FROM RTR_InsertOrUpdateToTarget WHERE NOT ISNULL(DCTClaimClientStageId) AND  (OldFirstName  != NewFirstName OR OldLastOrCompanyName != NewLastOrCompanyName OR OldAddress1 != NewAddress1 OR OldAddress2 != NewAddress2 OR OldCity != NewCity OR OldStateProv != NewStateProv OR OldPostalCode != NewPostalCode OR OldPhoneNumber != NewPhoneNumber OR OldDateOfBirth != NewDateOfBirth)),
RTR_InsertOrUpdateToTarget_Insert_Endorsement AS (SELECT * FROM RTR_InsertOrUpdateToTarget WHERE ISNULL(DCTClaimClientStageId) AND out_PolicySessionIsNewBusiness  !=  'Y'),
TGT_DCTClaimClientStage_InsertRequestStatus AS (
	INSERT INTO DCTClaimClientStage
	(CreatedDate, PolicyNumber, PolicyVersion, AgreementPartyId, FirstName, LastName, Street1, Street2, City, StateCode, ZipCode, WorkPhoneNumber, SupProcessStatusId, BirthDate)
	SELECT 
	out_ChangedDate AS CREATEDDATE, 
	in_PolicyNumber AS POLICYNUMBER, 
	out_PolicyVersion AS POLICYVERSION, 
	in_PartyXmlId AS AGREEMENTPARTYID, 
	NewFirstName AS FIRSTNAME, 
	NewLastOrCompanyName AS LASTNAME, 
	NewAddress AS STREET1, 
	NewAddress2 AS STREET2, 
	NewCity AS CITY, 
	NewStateProv AS STATECODE, 
	NewPostalCode AS ZIPCODE, 
	NewPhoneNumber AS WORKPHONENUMBER, 
	out_CompleteStatusId AS SUPPROCESSSTATUSID, 
	NewDateOfBirth AS BIRTHDATE
	FROM RTR_InsertOrUpdateToTarget_Insert_NewBusiness
),
UPD_Target AS (
	SELECT
	NewPhoneNumber AS PhoneNumber3, 
	NewAddress1 AS Address13, 
	NewAddress2 AS Address23, 
	NewCity AS City3, 
	NewStateProv AS StateProv3, 
	NewPostalCode AS PostalCode3, 
	NewDateOfBirth AS DateOfBirth3, 
	NewFirstName AS FirstName3, 
	NewLastOrCompanyName AS out_LastOrCompanyName3, 
	out_PolicyVersion AS out_PolicyVersion3, 
	out_ChangedDate AS out_ChangedDate3, 
	out_RequestStatusId AS out_RequestStatusId3, 
	DCTClaimClientStageId AS DCTClaimClientStageId3, 
	in_PolicyNumber AS in_PolicyNumber3, 
	in_PartyXmlId AS in_PartyXmlId3
	FROM RTR_InsertOrUpdateToTarget_Update_PartyChanged
),
TGT_DCTClaimClientStage_Update AS (
	MERGE INTO DCTClaimClientStage AS T
	USING UPD_Target AS S
	ON T.DCTClaimClientStageId = S.DCTClaimClientStageId3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.out_ChangedDate3, T.PolicyNumber = S.in_PolicyNumber3, T.PolicyVersion = S.out_PolicyVersion3, T.AgreementPartyId = S.in_PartyXmlId3, T.FirstName = S.FirstName3, T.LastName = S.out_LastOrCompanyName3, T.Street1 = S.Address13, T.Street2 = S.Address23, T.City = S.City3, T.StateCode = S.StateProv3, T.ZipCode = S.PostalCode3, T.WorkPhoneNumber = S.PhoneNumber3, T.SupProcessStatusId = S.out_RequestStatusId3, T.BirthDate = S.DateOfBirth3
),
TGT_DCTClaimClientStage_InsertCompleteStatus AS (
	INSERT INTO DCTClaimClientStage
	(CreatedDate, PolicyNumber, PolicyVersion, AgreementPartyId, FirstName, LastName, Street1, Street2, City, StateCode, ZipCode, WorkPhoneNumber, SupProcessStatusId, BirthDate)
	SELECT 
	out_ChangedDate AS CREATEDDATE, 
	in_PolicyNumber AS POLICYNUMBER, 
	out_PolicyVersion AS POLICYVERSION, 
	in_PartyXmlId AS AGREEMENTPARTYID, 
	NewFirstName AS FIRSTNAME, 
	NewLastOrCompanyName AS LASTNAME, 
	NewAddress1 AS STREET1, 
	NewAddress2 AS STREET2, 
	NewCity AS CITY, 
	NewStateProv AS STATECODE, 
	NewPostalCode AS ZIPCODE, 
	NewPhoneNumber AS WORKPHONENUMBER, 
	out_RequestStatusId AS SUPPROCESSSTATUSID, 
	NewDateOfBirth AS BIRTHDATE
	FROM RTR_InsertOrUpdateToTarget_Insert_Endorsement
),
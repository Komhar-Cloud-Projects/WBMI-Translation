WITH
SQ_Pif046Stage AS (
	SELECT
		Pif046StageId,
		ExtractDate,
		SourceSystemId,
		AuditId,
		UndhnPifSymbol,
		UndhnPifPolicyNumber,
		UndhnPifModule,
		UndhnRecordId,
		UndhnSegmentnumber,
		UndhnUnitnumber,
		UndhnChangedate,
		UndhnAwareMedInfo,
		UndhnMedsDistributed,
		UndhnWrittenConsenmed,
		UndhnVehAvailable,
		UndhnDiscipline,
		UndhnAdultSupervision,
		UndhnYrsinchildcare,
		UndhnNumchildren,
		UndhnLicensingaware,
		UndhnFormaltraining,
		UndhnArrestconvict,
		UndhnLawsuits,
		UndhnFireExtingrs,
		UndhnSmokeDetectrs,
		UndhnChildSafetyLock,
		UndhnOutletSafePlugs,
		UndhnStairsGated,
		UndhnPlaygrFenced,
		UndhnPlaygrequipmt,
		UndhnMaterlinplayarea,
		UndhnSwimmingArea,
		UndhnPetsAnimals,
		UndhnChildrenOffprems,
		UndhnWrittenAuthoriztn,
		UndhnSeatRestrainsts,
		UndhnPickupAuthoriztn,
		UndhnLocation,
		UndhnHmdycareDeldate,
		UndhnWatercraftDeldate,
		UndhnBoatingExperience,
		UndhnWatercrftClaims,
		UndhnGt1MovingViolatn,
		UndhnCertificateRecvd,
		UndhnModified1,
		UndhnModified2,
		UndhnModified3,
		UndhnModified4,
		UndhnModified5,
		UndhnModified6,
		UndhnCooking1,
		UndhnCooking2,
		UndhnCooking3,
		UndhnCooking4,
		UndhnCooking5,
		UndhnCooking6,
		UndhnCharter1,
		UndhnCharter2,
		UndhnCharter3,
		UndhnCharter4,
		UndhnCharter5,
		UndhnCharter6,
		UndhnBusiness1,
		UndhnBusiness2,
		UndhnBusiness3,
		UndhnBusiness4,
		UndhnBusiness5,
		UndhnBusiness6,
		UndhnRacing1,
		UndhnRacing2,
		UndhnRacing3,
		UndhnRacing4,
		UndhnRacing5,
		UndhnRacing6,
		UndhnStorage1,
		UndhnStorage2,
		UndhnStorage3,
		UndhnStorage4,
		UndhnStorage5,
		UndhnStorage6,
		UndhnPmsFutureuse,
		UndhnCustFutureuse,
		UndhnYr2000custuse
	FROM Pif046Stage
),
EXPTRANS AS (
	SELECT
	Pif046StageId,
	ExtractDate,
	SourceSystemId,
	AuditId,
	UndhnPifSymbol,
	UndhnPifPolicyNumber,
	UndhnPifModule,
	UndhnRecordId,
	UndhnSegmentnumber,
	UndhnUnitnumber,
	UndhnChangedate,
	UndhnAwareMedInfo,
	UndhnMedsDistributed,
	UndhnWrittenConsenmed,
	UndhnVehAvailable,
	UndhnDiscipline,
	UndhnAdultSupervision,
	UndhnYrsinchildcare,
	UndhnNumchildren,
	UndhnLicensingaware,
	UndhnFormaltraining,
	UndhnArrestconvict,
	UndhnLawsuits,
	UndhnFireExtingrs,
	UndhnSmokeDetectrs,
	UndhnChildSafetyLock,
	UndhnOutletSafePlugs,
	UndhnStairsGated,
	UndhnPlaygrFenced,
	UndhnPlaygrequipmt,
	UndhnMaterlinplayarea,
	UndhnSwimmingArea,
	UndhnPetsAnimals,
	UndhnChildrenOffprems,
	UndhnWrittenAuthoriztn,
	UndhnSeatRestrainsts,
	UndhnPickupAuthoriztn,
	UndhnLocation,
	UndhnHmdycareDeldate,
	UndhnWatercraftDeldate,
	UndhnBoatingExperience,
	UndhnWatercrftClaims,
	UndhnGt1MovingViolatn,
	UndhnCertificateRecvd,
	UndhnModified1,
	UndhnModified2,
	UndhnModified3,
	UndhnModified4,
	UndhnModified5,
	UndhnModified6,
	UndhnCooking1,
	UndhnCooking2,
	UndhnCooking3,
	UndhnCooking4,
	UndhnCooking5,
	UndhnCooking6,
	UndhnCharter1,
	UndhnCharter2,
	UndhnCharter3,
	UndhnCharter4,
	UndhnCharter5,
	UndhnCharter6,
	UndhnBusiness1,
	UndhnBusiness2,
	UndhnBusiness3,
	UndhnBusiness4,
	UndhnBusiness5,
	UndhnBusiness6,
	UndhnRacing1,
	UndhnRacing2,
	UndhnRacing3,
	UndhnRacing4,
	UndhnRacing5,
	UndhnRacing6,
	UndhnStorage1,
	UndhnStorage2,
	UndhnStorage3,
	UndhnStorage4,
	UndhnStorage5,
	UndhnStorage6,
	UndhnPmsFutureuse,
	UndhnCustFutureuse,
	UndhnYr2000custuse
	FROM SQ_Pif046Stage
),
ArchPif046Stage AS (
	INSERT INTO ArchPif046Stage
	(Pif046StageId, ExtractDate, SourceSystemId, AuditId, UndhnPifSymbol, UndhnPifPolicyNumber, UndhnPifModule, UndhnRecordId, UndhnSegmentnumber, UndhnUnitnumber, UndhnChangedate, UndhnAwareMedInfo, UndhnMedsDistributed, UndhnWrittenConsenmed, UndhnVehAvailable, UndhnDiscipline, UndhnAdultSupervision, UndhnYrsinchildcare, UndhnNumchildren, UndhnLicensingaware, UndhnFormaltraining, UndhnArrestconvict, UndhnLawsuits, UndhnFireExtingrs, UndhnSmokeDetectrs, UndhnChildSafetyLock, UndhnOutletSafePlugs, UndhnStairsGated, UndhnPlaygrFenced, UndhnPlaygrequipmt, UndhnMaterlinplayarea, UndhnSwimmingArea, UndhnPetsAnimals, UndhnChildrenOffprems, UndhnWrittenAuthoriztn, UndhnSeatRestrainsts, UndhnPickupAuthoriztn, UndhnLocation, UndhnHmdycareDeldate, UndhnWatercraftDeldate, UndhnBoatingExperience, UndhnWatercrftClaims, UndhnGt1MovingViolatn, UndhnCertificateRecvd, UndhnModified1, UndhnModified2, UndhnModified3, UndhnModified4, UndhnModified5, UndhnModified6, UndhnCooking1, UndhnCooking2, UndhnCooking3, UndhnCooking4, UndhnCooking5, UndhnCooking6, UndhnCharter1, UndhnCharter2, UndhnCharter3, UndhnCharter4, UndhnCharter5, UndhnCharter6, UndhnBusiness1, UndhnBusiness2, UndhnBusiness3, UndhnBusiness4, UndhnBusiness5, UndhnBusiness6, UndhnRacing1, UndhnRacing2, UndhnRacing3, UndhnRacing4, UndhnRacing5, UndhnRacing6, UndhnStorage1, UndhnStorage2, UndhnStorage3, UndhnStorage4, UndhnStorage5, UndhnStorage6, UndhnPmsFutureuse, UndhnCustFutureuse, UndhnYr2000custuse)
	SELECT 
	PIF046STAGEID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID, 
	UNDHNPIFSYMBOL, 
	UNDHNPIFPOLICYNUMBER, 
	UNDHNPIFMODULE, 
	UNDHNRECORDID, 
	UNDHNSEGMENTNUMBER, 
	UNDHNUNITNUMBER, 
	UNDHNCHANGEDATE, 
	UNDHNAWAREMEDINFO, 
	UNDHNMEDSDISTRIBUTED, 
	UNDHNWRITTENCONSENMED, 
	UNDHNVEHAVAILABLE, 
	UNDHNDISCIPLINE, 
	UNDHNADULTSUPERVISION, 
	UNDHNYRSINCHILDCARE, 
	UNDHNNUMCHILDREN, 
	UNDHNLICENSINGAWARE, 
	UNDHNFORMALTRAINING, 
	UNDHNARRESTCONVICT, 
	UNDHNLAWSUITS, 
	UNDHNFIREEXTINGRS, 
	UNDHNSMOKEDETECTRS, 
	UNDHNCHILDSAFETYLOCK, 
	UNDHNOUTLETSAFEPLUGS, 
	UNDHNSTAIRSGATED, 
	UNDHNPLAYGRFENCED, 
	UNDHNPLAYGREQUIPMT, 
	UNDHNMATERLINPLAYAREA, 
	UNDHNSWIMMINGAREA, 
	UNDHNPETSANIMALS, 
	UNDHNCHILDRENOFFPREMS, 
	UNDHNWRITTENAUTHORIZTN, 
	UNDHNSEATRESTRAINSTS, 
	UNDHNPICKUPAUTHORIZTN, 
	UNDHNLOCATION, 
	UNDHNHMDYCAREDELDATE, 
	UNDHNWATERCRAFTDELDATE, 
	UNDHNBOATINGEXPERIENCE, 
	UNDHNWATERCRFTCLAIMS, 
	UNDHNGT1MOVINGVIOLATN, 
	UNDHNCERTIFICATERECVD, 
	UNDHNMODIFIED1, 
	UNDHNMODIFIED2, 
	UNDHNMODIFIED3, 
	UNDHNMODIFIED4, 
	UNDHNMODIFIED5, 
	UNDHNMODIFIED6, 
	UNDHNCOOKING1, 
	UNDHNCOOKING2, 
	UNDHNCOOKING3, 
	UNDHNCOOKING4, 
	UNDHNCOOKING5, 
	UNDHNCOOKING6, 
	UNDHNCHARTER1, 
	UNDHNCHARTER2, 
	UNDHNCHARTER3, 
	UNDHNCHARTER4, 
	UNDHNCHARTER5, 
	UNDHNCHARTER6, 
	UNDHNBUSINESS1, 
	UNDHNBUSINESS2, 
	UNDHNBUSINESS3, 
	UNDHNBUSINESS4, 
	UNDHNBUSINESS5, 
	UNDHNBUSINESS6, 
	UNDHNRACING1, 
	UNDHNRACING2, 
	UNDHNRACING3, 
	UNDHNRACING4, 
	UNDHNRACING5, 
	UNDHNRACING6, 
	UNDHNSTORAGE1, 
	UNDHNSTORAGE2, 
	UNDHNSTORAGE3, 
	UNDHNSTORAGE4, 
	UNDHNSTORAGE5, 
	UNDHNSTORAGE6, 
	UNDHNPMSFUTUREUSE, 
	UNDHNCUSTFUTUREUSE, 
	UNDHNYR2000CUSTUSE
	FROM EXPTRANS
),
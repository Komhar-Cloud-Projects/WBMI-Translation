WITH
SQ_WCPOLS_Record01 AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ExperienceRatingCode)+
	max(InterstateRiskIDNumber)+
	max(PolicyExpirationDate)+
	max(ThirdPartyEntityFederalEmployerIdentificationNumber)+
	max(TypeOfCoverageIDCode)+
	max(EmployeeLeasingPolicyTypeCode)+
	max(PolicyTermCode)+
	max(PriorPolicyNumberIdentifier)+
	max(ReservedForFutureUse2)+
	max(LegalNatureOfInsuredCode)+
	max(TypeOfPlanIDCode)+
	max(WrapUpOwnerControlledInsuranceProgramCode)+
	max(BusinessSegmentIdentifier)+
	max(PolicyMinimumPremiumAmount)+
	max(PolicyMinimumPremiumStateCode)+
	max(PolicyEstimatedStandardPremiumTotal)+
	max(PolicyDepositPremiumAmount)+
	max(AuditFrequencyCode)+
	max(BillingFrequencyCode)+
	max(RetrospectiveRatingCode)+
	max(EmployerLiabilityLimitAmountBodilyInjuryByAccidentEachAccidentAmount)+
	max(EmployerLiabilityLimitAmountBodilyInjuryByDiseasePolicyLimitAmount)+
	max(EmployerLiabilityLimitAmountBodilyInjuryByDiseaseEachEmployeeAmount)+
	max(NameOfProducer)+
	max(AssignedRiskBinderNumberFirstSevenPositions)+
	max(GroupCoverageStatusCode)+
	max(ReservedForFutureUse3)+
	max(OriginalCarrierCode)+
	max(OriginalPolicyNumberIdentifier)+
	max(OriginalPolicyEffectiveDate)+
	max(TextForOtherLegalNatureOfInsured)+
	max(AssignmentDate)+
	max(AssignedRiskBinderNumberLastElevenPositions)+
	max(ReservedForFutureUse4)+
	max(PolicyChangeEffectiveDate)+
	max(PolicyChangeExpirationDate),
	max(WCTrackHistoryID) WCTrackHistoryID From (
	Select WCPols01RecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	replicate(' ',2) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	case 
	when B.COLUMN_NAME='ExperienceRatingCode' and C.FieldName='ExperienceRatingCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ExperienceRatingCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ExperienceRatingCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.ExperienceRatingCode,''),1) end
	else 
	case when B.COLUMN_NAME='ExperienceRatingCode' and C.FieldName='ExperienceRatingCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end ExperienceRatingCode,
	case 
	when B.COLUMN_NAME='InterstateRiskIDNumber' and C.FieldName='InterstateRiskIDNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.InterstateRiskIDNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.InterstateRiskIDNumber,'')+replicate(' ',9),9) else Right(replicate('0',9)+ISNULL(A.InterstateRiskIDNumber,''),9) end
	else 
	case when B.COLUMN_NAME='InterstateRiskIDNumber' and C.FieldName='InterstateRiskIDNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',9) else replicate('0',9) end
	end end InterstateRiskIDNumber,
	case 
	when B.COLUMN_NAME='PolicyExpirationDate' and C.FieldName='PolicyExpirationDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.PolicyExpirationDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PolicyExpirationDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.PolicyExpirationDate,''),6) end
	else 
	case when B.COLUMN_NAME='PolicyExpirationDate' and C.FieldName='PolicyExpirationDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end PolicyExpirationDate,
	case 
	when B.COLUMN_NAME='ThirdPartyEntityFederalEmployerIdentificationNumber' and C.FieldName='ThirdPartyEntityFederalEmployerIdentificationNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ThirdPartyEntityFederalEmployerIdentificationNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ThirdPartyEntityFederalEmployerIdentificationNumber,'')+replicate(' ',9),9) else Right(replicate('0',9)+ISNULL(A.ThirdPartyEntityFederalEmployerIdentificationNumber,''),9) end
	else 
	case when B.COLUMN_NAME='ThirdPartyEntityFederalEmployerIdentificationNumber' and C.FieldName='ThirdPartyEntityFederalEmployerIdentificationNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',9) else replicate('0',9) end
	end end ThirdPartyEntityFederalEmployerIdentificationNumber,
	case 
	when B.COLUMN_NAME='TypeOfCoverageIDCode' and C.FieldName='TypeOfCoverageIDCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.TypeOfCoverageIDCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.TypeOfCoverageIDCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.TypeOfCoverageIDCode,''),2) end
	else 
	case when B.COLUMN_NAME='TypeOfCoverageIDCode' and C.FieldName='TypeOfCoverageIDCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end TypeOfCoverageIDCode,
	case 
	when B.COLUMN_NAME='EmployeeLeasingPolicyTypeCode' and C.FieldName='EmployeeLeasingPolicyTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EmployeeLeasingPolicyTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EmployeeLeasingPolicyTypeCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.EmployeeLeasingPolicyTypeCode,''),1) end
	else 
	case when B.COLUMN_NAME='EmployeeLeasingPolicyTypeCode' and C.FieldName='EmployeeLeasingPolicyTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end EmployeeLeasingPolicyTypeCode,
	case 
	when B.COLUMN_NAME='PolicyTermCode' and C.FieldName='PolicyTermCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.PolicyTermCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PolicyTermCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.PolicyTermCode,''),1) end
	else 
	case when B.COLUMN_NAME='PolicyTermCode' and C.FieldName='PolicyTermCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end PolicyTermCode,
	case 
	when B.COLUMN_NAME='PriorPolicyNumberIdentifier' and C.FieldName='PriorPolicyNumberIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.PriorPolicyNumberIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PriorPolicyNumberIdentifier,'')+replicate(' ',18),18) else Right(replicate('0',18)+ISNULL(A.PriorPolicyNumberIdentifier,''),18) end
	else 
	case when B.COLUMN_NAME='PriorPolicyNumberIdentifier' and C.FieldName='PriorPolicyNumberIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',18) else replicate('0',18) end
	end end PriorPolicyNumberIdentifier,
	replicate(' ',11) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='LegalNatureOfInsuredCode' and C.FieldName='LegalNatureOfInsuredCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LegalNatureOfInsuredCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LegalNatureOfInsuredCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.LegalNatureOfInsuredCode,''),2) end
	else 
	case when B.COLUMN_NAME='LegalNatureOfInsuredCode' and C.FieldName='LegalNatureOfInsuredCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end LegalNatureOfInsuredCode,
	case 
	when B.COLUMN_NAME='TypeOfPlanIDCode' and C.FieldName='TypeOfPlanIDCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.TypeOfPlanIDCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.TypeOfPlanIDCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.TypeOfPlanIDCode,''),1) end
	else 
	case when B.COLUMN_NAME='TypeOfPlanIDCode' and C.FieldName='TypeOfPlanIDCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end TypeOfPlanIDCode,
	case 
	when B.COLUMN_NAME='WrapUpOwnerControlledInsuranceProgramCode' and C.FieldName='WrapUpOwnerControlledInsuranceProgramCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.WrapUpOwnerControlledInsuranceProgramCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.WrapUpOwnerControlledInsuranceProgramCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.WrapUpOwnerControlledInsuranceProgramCode,''),1) end
	else 
	case when B.COLUMN_NAME='WrapUpOwnerControlledInsuranceProgramCode' and C.FieldName='WrapUpOwnerControlledInsuranceProgramCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end WrapUpOwnerControlledInsuranceProgramCode,
	case 
	when B.COLUMN_NAME='BusinessSegmentIdentifier' and C.FieldName='BusinessSegmentIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BusinessSegmentIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BusinessSegmentIdentifier,'')+replicate(' ',7),7) else Right(replicate('0',7)+ISNULL(A.BusinessSegmentIdentifier,''),7) end
	else 
	case when B.COLUMN_NAME='BusinessSegmentIdentifier' and C.FieldName='BusinessSegmentIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',7) else replicate('0',7) end
	end end BusinessSegmentIdentifier,
	case 
	when B.COLUMN_NAME='PolicyMinimumPremiumAmount' and C.FieldName='PolicyMinimumPremiumAmount' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.PolicyMinimumPremiumAmount or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PolicyMinimumPremiumAmount,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.PolicyMinimumPremiumAmount,''),10) end
	else 
	case when B.COLUMN_NAME='PolicyMinimumPremiumAmount' and C.FieldName='PolicyMinimumPremiumAmount' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end PolicyMinimumPremiumAmount,
	case 
	when B.COLUMN_NAME='PolicyMinimumPremiumStateCode' and C.FieldName='PolicyMinimumPremiumStateCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.PolicyMinimumPremiumStateCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PolicyMinimumPremiumStateCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.PolicyMinimumPremiumStateCode,''),2) end
	else 
	case when B.COLUMN_NAME='PolicyMinimumPremiumStateCode' and C.FieldName='PolicyMinimumPremiumStateCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end PolicyMinimumPremiumStateCode,
	case 
	when B.COLUMN_NAME='PolicyEstimatedStandardPremiumTotal' and C.FieldName='PolicyEstimatedStandardPremiumTotal' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.PolicyEstimatedStandardPremiumTotal or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PolicyEstimatedStandardPremiumTotal,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.PolicyEstimatedStandardPremiumTotal,''),10) end
	else 
	case when B.COLUMN_NAME='PolicyEstimatedStandardPremiumTotal' and C.FieldName='PolicyEstimatedStandardPremiumTotal' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end PolicyEstimatedStandardPremiumTotal,
	case 
	when B.COLUMN_NAME='PolicyDepositPremiumAmount' and C.FieldName='PolicyDepositPremiumAmount' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.PolicyDepositPremiumAmount or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PolicyDepositPremiumAmount,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.PolicyDepositPremiumAmount,''),10) end
	else 
	case when B.COLUMN_NAME='PolicyDepositPremiumAmount' and C.FieldName='PolicyDepositPremiumAmount' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end PolicyDepositPremiumAmount,
	case 
	when B.COLUMN_NAME='AuditFrequencyCode' and C.FieldName='AuditFrequencyCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AuditFrequencyCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AuditFrequencyCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.AuditFrequencyCode,''),1) end
	else 
	case when B.COLUMN_NAME='AuditFrequencyCode' and C.FieldName='AuditFrequencyCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end AuditFrequencyCode,
	case 
	when B.COLUMN_NAME='BillingFrequencyCode' and C.FieldName='BillingFrequencyCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BillingFrequencyCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BillingFrequencyCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BillingFrequencyCode,''),1) end
	else 
	case when B.COLUMN_NAME='BillingFrequencyCode' and C.FieldName='BillingFrequencyCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BillingFrequencyCode,
	case 
	when B.COLUMN_NAME='RetrospectiveRatingCode' and C.FieldName='RetrospectiveRatingCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RetrospectiveRatingCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RetrospectiveRatingCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.RetrospectiveRatingCode,''),1) end
	else 
	case when B.COLUMN_NAME='RetrospectiveRatingCode' and C.FieldName='RetrospectiveRatingCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end RetrospectiveRatingCode,
	case 
	when B.COLUMN_NAME='EmployerLiabilityLimitAmountBodilyInjuryByAccidentEachAccidentAmount' and C.FieldName='EmployerLiabilityLimitAmountBodilyInjuryByAccidentEachAccidentAmount' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EmployerLiabilityLimitAmountBodilyInjuryByAccidentEachAccidentAmount or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EmployerLiabilityLimitAmountBodilyInjuryByAccidentEachAccidentAmount,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.EmployerLiabilityLimitAmountBodilyInjuryByAccidentEachAccidentAmount,''),10) end
	else 
	case when B.COLUMN_NAME='EmployerLiabilityLimitAmountBodilyInjuryByAccidentEachAccidentAmount' and C.FieldName='EmployerLiabilityLimitAmountBodilyInjuryByAccidentEachAccidentAmount' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end EmployerLiabilityLimitAmountBodilyInjuryByAccidentEachAccidentAmount,
	case 
	when B.COLUMN_NAME='EmployerLiabilityLimitAmountBodilyInjuryByDiseasePolicyLimitAmount' and C.FieldName='EmployerLiabilityLimitAmountBodilyInjuryByDiseasePolicyLimitAmount' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EmployerLiabilityLimitAmountBodilyInjuryByDiseasePolicyLimitAmount or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EmployerLiabilityLimitAmountBodilyInjuryByDiseasePolicyLimitAmount,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.EmployerLiabilityLimitAmountBodilyInjuryByDiseasePolicyLimitAmount,''),10) end
	else 
	case when B.COLUMN_NAME='EmployerLiabilityLimitAmountBodilyInjuryByDiseasePolicyLimitAmount' and C.FieldName='EmployerLiabilityLimitAmountBodilyInjuryByDiseasePolicyLimitAmount' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end EmployerLiabilityLimitAmountBodilyInjuryByDiseasePolicyLimitAmount,
	case 
	when B.COLUMN_NAME='EmployerLiabilityLimitAmountBodilyInjuryByDiseaseEachEmployeeAmount' and C.FieldName='EmployerLiabilityLimitAmountBodilyInjuryByDiseaseEachEmployeeAmount' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EmployerLiabilityLimitAmountBodilyInjuryByDiseaseEachEmployeeAmount or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EmployerLiabilityLimitAmountBodilyInjuryByDiseaseEachEmployeeAmount,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.EmployerLiabilityLimitAmountBodilyInjuryByDiseaseEachEmployeeAmount,''),10) end
	else 
	case when B.COLUMN_NAME='EmployerLiabilityLimitAmountBodilyInjuryByDiseaseEachEmployeeAmount' and C.FieldName='EmployerLiabilityLimitAmountBodilyInjuryByDiseaseEachEmployeeAmount' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end EmployerLiabilityLimitAmountBodilyInjuryByDiseaseEachEmployeeAmount,
	case 
	when B.COLUMN_NAME='NameOfProducer' and C.FieldName='NameOfProducer' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfProducer or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfProducer,'')+replicate(' ',30),30) else Right(replicate('0',30)+ISNULL(A.NameOfProducer,''),30) end
	else 
	case when B.COLUMN_NAME='NameOfProducer' and C.FieldName='NameOfProducer' then case when C.FieldDataType in ('A','AN') then replicate(' ',30) else replicate('0',30) end
	end end NameOfProducer,
	case 
	when B.COLUMN_NAME='AssignedRiskBinderNumberFirstSevenPositions' and C.FieldName='AssignedRiskBinderNumberFirstSevenPositions' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AssignedRiskBinderNumberFirstSevenPositions or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AssignedRiskBinderNumberFirstSevenPositions,'')+replicate(' ',7),7) else Right(replicate('0',7)+ISNULL(A.AssignedRiskBinderNumberFirstSevenPositions,''),7) end
	else 
	case when B.COLUMN_NAME='AssignedRiskBinderNumberFirstSevenPositions' and C.FieldName='AssignedRiskBinderNumberFirstSevenPositions' then case when C.FieldDataType in ('A','AN') then replicate(' ',7) else replicate('0',7) end
	end end AssignedRiskBinderNumberFirstSevenPositions,
	case 
	when B.COLUMN_NAME='GroupCoverageStatusCode' and C.FieldName='GroupCoverageStatusCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.GroupCoverageStatusCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.GroupCoverageStatusCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.GroupCoverageStatusCode,''),1) end
	else 
	case when B.COLUMN_NAME='GroupCoverageStatusCode' and C.FieldName='GroupCoverageStatusCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end GroupCoverageStatusCode,
	replicate(' ',1) ReservedForFutureUse3,
	case 
	when B.COLUMN_NAME='OriginalCarrierCode' and C.FieldName='OriginalCarrierCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.OriginalCarrierCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.OriginalCarrierCode,'')+replicate(' ',5),5) else Right(replicate('0',5)+ISNULL(A.OriginalCarrierCode,''),5) end
	else 
	case when B.COLUMN_NAME='OriginalCarrierCode' and C.FieldName='OriginalCarrierCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',5) else replicate('0',5) end
	end end OriginalCarrierCode,
	case 
	when B.COLUMN_NAME='OriginalPolicyNumberIdentifier' and C.FieldName='OriginalPolicyNumberIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.OriginalPolicyNumberIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.OriginalPolicyNumberIdentifier,'')+replicate(' ',18),18) else Right(replicate('0',18)+ISNULL(A.OriginalPolicyNumberIdentifier,''),18) end
	else 
	case when B.COLUMN_NAME='OriginalPolicyNumberIdentifier' and C.FieldName='OriginalPolicyNumberIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',18) else replicate('0',18) end
	end end OriginalPolicyNumberIdentifier,
	case 
	when B.COLUMN_NAME='OriginalPolicyEffectiveDate' and C.FieldName='OriginalPolicyEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.OriginalPolicyEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.OriginalPolicyEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.OriginalPolicyEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='OriginalPolicyEffectiveDate' and C.FieldName='OriginalPolicyEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end OriginalPolicyEffectiveDate,
	case 
	when B.COLUMN_NAME='TextForOtherLegalNatureOfInsured' and C.FieldName='TextForOtherLegalNatureOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.TextForOtherLegalNatureOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.TextForOtherLegalNatureOfInsured,'')+replicate(' ',20),20) else Right(replicate('0',20)+ISNULL(A.TextForOtherLegalNatureOfInsured,''),20) end
	else 
	case when B.COLUMN_NAME='TextForOtherLegalNatureOfInsured' and C.FieldName='TextForOtherLegalNatureOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',20) else replicate('0',20) end
	end end TextForOtherLegalNatureOfInsured,
	case 
	when B.COLUMN_NAME='AssignmentDate' and C.FieldName='AssignmentDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AssignmentDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AssignmentDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.AssignmentDate,''),6) end
	else 
	case when B.COLUMN_NAME='AssignmentDate' and C.FieldName='AssignmentDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end AssignmentDate,
	case 
	when B.COLUMN_NAME='AssignedRiskBinderNumberLastElevenPositions' and C.FieldName='AssignedRiskBinderNumberLastElevenPositions' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AssignedRiskBinderNumberLastElevenPositions or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AssignedRiskBinderNumberLastElevenPositions,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.AssignedRiskBinderNumberLastElevenPositions,''),11) end
	else 
	case when B.COLUMN_NAME='AssignedRiskBinderNumberLastElevenPositions' and C.FieldName='AssignedRiskBinderNumberLastElevenPositions' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end AssignedRiskBinderNumberLastElevenPositions,
	replicate(' ',2) ReservedForFutureUse4,
	case 
	when B.COLUMN_NAME='PolicyChangeEffectiveDate' and C.FieldName='PolicyChangeEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.PolicyChangeEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PolicyChangeEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.PolicyChangeEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='PolicyChangeEffectiveDate' and C.FieldName='PolicyChangeEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end PolicyChangeEffectiveDate,
	case 
	when B.COLUMN_NAME='PolicyChangeExpirationDate' and C.FieldName='PolicyChangeExpirationDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType and (D.WCPOLSCode=A.PolicyChangeExpirationDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PolicyChangeExpirationDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.PolicyChangeExpirationDate,''),6) end
	else 
	case when B.COLUMN_NAME='PolicyChangeExpirationDate' and C.FieldName='PolicyChangeExpirationDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end PolicyChangeExpirationDate
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols01Record A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols01Record'
	and c.TableName='WCPols01Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols01RecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ExperienceRatingCode)+
	max(InterstateRiskIDNumber)+
	max(PolicyExpirationDate)+
	max(ThirdPartyEntityFederalEmployerIdentificationNumber)+
	max(TypeOfCoverageIDCode)+
	max(EmployeeLeasingPolicyTypeCode)+
	max(PolicyTermCode)+
	max(PriorPolicyNumberIdentifier)+
	max(ReservedForFutureUse2)+
	max(LegalNatureOfInsuredCode)+
	max(TypeOfPlanIDCode)+
	max(WrapUpOwnerControlledInsuranceProgramCode)+
	max(BusinessSegmentIdentifier)+
	max(PolicyMinimumPremiumAmount)+
	max(PolicyMinimumPremiumStateCode)+
	max(PolicyEstimatedStandardPremiumTotal)+
	max(PolicyDepositPremiumAmount)+
	max(AuditFrequencyCode)+
	max(BillingFrequencyCode)+
	max(RetrospectiveRatingCode)+
	max(EmployerLiabilityLimitAmountBodilyInjuryByAccidentEachAccidentAmount)+
	max(EmployerLiabilityLimitAmountBodilyInjuryByDiseasePolicyLimitAmount)+
	max(EmployerLiabilityLimitAmountBodilyInjuryByDiseaseEachEmployeeAmount)+
	max(NameOfProducer)+
	max(AssignedRiskBinderNumberFirstSevenPositions)+
	max(GroupCoverageStatusCode)+
	max(ReservedForFutureUse3)+
	max(OriginalCarrierCode)+
	max(OriginalPolicyNumberIdentifier)+
	max(OriginalPolicyEffectiveDate)+
	max(TextForOtherLegalNatureOfInsured)+
	max(AssignmentDate)+
	max(AssignedRiskBinderNumberLastElevenPositions)+
	max(ReservedForFutureUse4)+
	max(PolicyChangeEffectiveDate)+
	max(PolicyChangeExpirationDate)
	,'0','')))<>''
),
EXP_DataCollect_Record01 AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data,
	WCTrackHistoryID
	FROM SQ_WCPOLS_Record01
),
SQ_WCPOLS_Record02 AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(NameTypeCode)+
	max(NameLinkIdentifier)+
	max(ProfessionalEmployerOrganizationOrClientCompanyCode)+
	max(NameOfInsured)+
	max(ReservedForFutureUse2)+
	max(FederalEmployerIdentificationNumber)+
	max(ContinuationSequenceNumber)+
	max(LegalNatureOfEntityCode)+
	max(StateCode01)+
	max(StateUnemploymentNumber01)+
	max(StateCode02)+
	max(StateUnemploymentNumber02)+
	max(StateCode03)+
	max(StateUnemploymentNumber03)+
	max(ReservedForFutureUse3)+
	max(StateUnemploymentNumberRecordSequenceNumber)+
	max(ReservedForFutureUse4)+
	max(NameLinkCounterIdentifier)+
	max(ReservedForFutureUse5)+
	max(PolicyChangeEffectiveDate)+
	max(PolicyChangeExpirationDate) From (
	Select WCPols02RecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	replicate(' ',2) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	case 
	when B.COLUMN_NAME='NameTypeCode' and C.FieldName='NameTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameTypeCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.NameTypeCode,''),1) end
	else 
	case when B.COLUMN_NAME='NameTypeCode' and C.FieldName='NameTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end NameTypeCode,
	case 
	when B.COLUMN_NAME='NameLinkIdentifier' and C.FieldName='NameLinkIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameLinkIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameLinkIdentifier,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.NameLinkIdentifier,''),3) end
	else 
	case when B.COLUMN_NAME='NameLinkIdentifier' and C.FieldName='NameLinkIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end NameLinkIdentifier,
	case 
	when B.COLUMN_NAME='ProfessionalEmployerOrganizationOrClientCompanyCode' and C.FieldName='ProfessionalEmployerOrganizationOrClientCompanyCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ProfessionalEmployerOrganizationOrClientCompanyCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ProfessionalEmployerOrganizationOrClientCompanyCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.ProfessionalEmployerOrganizationOrClientCompanyCode,''),1) end
	else 
	case when B.COLUMN_NAME='ProfessionalEmployerOrganizationOrClientCompanyCode' and C.FieldName='ProfessionalEmployerOrganizationOrClientCompanyCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end ProfessionalEmployerOrganizationOrClientCompanyCode,
	case 
	when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfInsured,'')+replicate(' ',90),90) else Right(replicate('0',90)+ISNULL(A.NameOfInsured,''),90) end
	else 
	case when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',90) else replicate('0',90) end
	end end NameOfInsured,
	replicate(' ',6) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='FederalEmployerIdentificationNumber' and C.FieldName='FederalEmployerIdentificationNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.FederalEmployerIdentificationNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.FederalEmployerIdentificationNumber,'')+replicate(' ',9),9) else Right(replicate('0',9)+ISNULL(A.FederalEmployerIdentificationNumber,''),9) end
	else 
	case when B.COLUMN_NAME='FederalEmployerIdentificationNumber' and C.FieldName='FederalEmployerIdentificationNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',9) else replicate('0',9) end
	end end FederalEmployerIdentificationNumber,
	case 
	when B.COLUMN_NAME='ContinuationSequenceNumber' and C.FieldName='ContinuationSequenceNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ContinuationSequenceNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ContinuationSequenceNumber,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.ContinuationSequenceNumber,''),3) end
	else 
	case when B.COLUMN_NAME='ContinuationSequenceNumber' and C.FieldName='ContinuationSequenceNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end ContinuationSequenceNumber,
	case 
	when B.COLUMN_NAME='LegalNatureOfEntityCode' and C.FieldName='LegalNatureOfEntityCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LegalNatureOfEntityCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LegalNatureOfEntityCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.LegalNatureOfEntityCode,''),2) end
	else 
	case when B.COLUMN_NAME='LegalNatureOfEntityCode' and C.FieldName='LegalNatureOfEntityCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end LegalNatureOfEntityCode,
	case 
	when B.COLUMN_NAME='StateCode01' and C.FieldName='StateCode01' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode01 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode01,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode01,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode01' and C.FieldName='StateCode01' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode01,
	case 
	when B.COLUMN_NAME='StateUnemploymentNumber01' and C.FieldName='StateUnemploymentNumber01' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateUnemploymentNumber01 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateUnemploymentNumber01,'')+replicate(' ',15),15) else Right(replicate('0',15)+ISNULL(A.StateUnemploymentNumber01,''),15) end
	else 
	case when B.COLUMN_NAME='StateUnemploymentNumber01' and C.FieldName='StateUnemploymentNumber01' then case when C.FieldDataType in ('A','AN') then replicate(' ',15) else replicate('0',15) end
	end end StateUnemploymentNumber01,
	case 
	when B.COLUMN_NAME='StateCode02' and C.FieldName='StateCode02' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode02 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode02,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode02,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode02' and C.FieldName='StateCode02' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode02,
	case 
	when B.COLUMN_NAME='StateUnemploymentNumber02' and C.FieldName='StateUnemploymentNumber02' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateUnemploymentNumber02 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateUnemploymentNumber02,'')+replicate(' ',15),15) else Right(replicate('0',15)+ISNULL(A.StateUnemploymentNumber02,''),15) end
	else 
	case when B.COLUMN_NAME='StateUnemploymentNumber02' and C.FieldName='StateUnemploymentNumber02' then case when C.FieldDataType in ('A','AN') then replicate(' ',15) else replicate('0',15) end
	end end StateUnemploymentNumber02,
	case 
	when B.COLUMN_NAME='StateCode03' and C.FieldName='StateCode03' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode03 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode03,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode03,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode03' and C.FieldName='StateCode03' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode03,
	case 
	when B.COLUMN_NAME='StateUnemploymentNumber03' and C.FieldName='StateUnemploymentNumber03' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateUnemploymentNumber03 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateUnemploymentNumber03,'')+replicate(' ',15),15) else Right(replicate('0',15)+ISNULL(A.StateUnemploymentNumber03,''),15) end
	else 
	case when B.COLUMN_NAME='StateUnemploymentNumber03' and C.FieldName='StateUnemploymentNumber03' then case when C.FieldDataType in ('A','AN') then replicate(' ',15) else replicate('0',15) end
	end end StateUnemploymentNumber03,
	replicate(' ',34) ReservedForFutureUse3,
	case 
	when B.COLUMN_NAME='StateUnemploymentNumberRecordSequenceNumber' and C.FieldName='StateUnemploymentNumberRecordSequenceNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateUnemploymentNumberRecordSequenceNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateUnemploymentNumberRecordSequenceNumber,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateUnemploymentNumberRecordSequenceNumber,''),2) end
	else 
	case when B.COLUMN_NAME='StateUnemploymentNumberRecordSequenceNumber' and C.FieldName='StateUnemploymentNumberRecordSequenceNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateUnemploymentNumberRecordSequenceNumber,
	replicate(' ',20) ReservedForFutureUse4,
	case 
	when B.COLUMN_NAME='NameLinkCounterIdentifier' and C.FieldName='NameLinkCounterIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameLinkCounterIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameLinkCounterIdentifier,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.NameLinkCounterIdentifier,''),2) end
	else 
	case when B.COLUMN_NAME='NameLinkCounterIdentifier' and C.FieldName='NameLinkCounterIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end NameLinkCounterIdentifier,
	replicate(' ',17) ReservedForFutureUse5,
	case 
	when B.COLUMN_NAME='PolicyChangeEffectiveDate' and C.FieldName='PolicyChangeEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.PolicyChangeEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PolicyChangeEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.PolicyChangeEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='PolicyChangeEffectiveDate' and C.FieldName='PolicyChangeEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end PolicyChangeEffectiveDate,
	case 
	when B.COLUMN_NAME='PolicyChangeExpirationDate' and C.FieldName='PolicyChangeExpirationDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType and (D.WCPOLSCode=A.PolicyChangeExpirationDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PolicyChangeExpirationDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.PolicyChangeExpirationDate,''),6) end
	else 
	case when B.COLUMN_NAME='PolicyChangeExpirationDate' and C.FieldName='PolicyChangeExpirationDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end PolicyChangeExpirationDate
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols02Record A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols02Record'
	and c.TableName='WCPols02Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols02RecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(NameTypeCode)+
	max(NameLinkIdentifier)+
	max(ProfessionalEmployerOrganizationOrClientCompanyCode)+
	max(NameOfInsured)+
	max(ReservedForFutureUse2)+
	max(FederalEmployerIdentificationNumber)+
	max(ContinuationSequenceNumber)+
	max(LegalNatureOfEntityCode)+
	max(StateCode01)+
	max(StateUnemploymentNumber01)+
	max(StateCode02)+
	max(StateUnemploymentNumber02)+
	max(StateCode03)+
	max(StateUnemploymentNumber03)+
	max(ReservedForFutureUse3)+
	max(StateUnemploymentNumberRecordSequenceNumber)+
	max(ReservedForFutureUse4)+
	max(NameLinkCounterIdentifier)+
	max(ReservedForFutureUse5)+
	max(PolicyChangeEffectiveDate)+
	max(PolicyChangeExpirationDate)
	,'0','')))<>''
),
EXP_DataCollect_Record02 AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPOLS_Record02
),
SQ_WCPOLS_Record03 AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(AddressTypeCode)+
	max(ForeignAddressIndicator)+
	max(AddressStructureCode)+
	max(AddressStreet)+
	max(AddressCity)+
	max(AddressState)+
	max(AddressZipCode)+
	max(NameLinkIdentifier)+
	max(StateCodeLink)+
	max(ExposureRecordLinkForLocationCode)+
	max(ReservedForFutureUse2)+
	max(PhoneNumberOfInsured)+
	max(NumberOfEmployees)+
	max(IndustryCode)+
	max(GeographicArea)+
	max(EmailAddress)+
	max(ReservedForFutureUse3)+
	max(CountryCode)+
	max(NameLinkCounterIdentifier)+
	max(ReservedForFutureUse4)+
	max(PolicyChangeEffectiveDate)+
	max(PolicyChangeExpirationDate) From (
	Select WCPols03RecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	replicate(' ',2) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	case 
	when B.COLUMN_NAME='AddressTypeCode' and C.FieldName='AddressTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressTypeCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.AddressTypeCode,''),1) end
	else 
	case when B.COLUMN_NAME='AddressTypeCode' and C.FieldName='AddressTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end AddressTypeCode,
	case 
	when B.COLUMN_NAME='ForeignAddressIndicator' and C.FieldName='ForeignAddressIndicator' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ForeignAddressIndicator or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ForeignAddressIndicator,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.ForeignAddressIndicator,''),1) end
	else 
	case when B.COLUMN_NAME='ForeignAddressIndicator' and C.FieldName='ForeignAddressIndicator' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end ForeignAddressIndicator,
	case 
	when B.COLUMN_NAME='AddressStructureCode' and C.FieldName='AddressStructureCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressStructureCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressStructureCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.AddressStructureCode,''),1) end
	else 
	case when B.COLUMN_NAME='AddressStructureCode' and C.FieldName='AddressStructureCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end AddressStructureCode,
	case 
	when B.COLUMN_NAME='AddressStreet' and C.FieldName='AddressStreet' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressStreet or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressStreet,'')+replicate(' ',60),60) else Right(replicate('0',60)+ISNULL(A.AddressStreet,''),60) end
	else 
	case when B.COLUMN_NAME='AddressStreet' and C.FieldName='AddressStreet' then case when C.FieldDataType in ('A','AN') then replicate(' ',60) else replicate('0',60) end
	end end AddressStreet,
	case 
	when B.COLUMN_NAME='AddressCity' and C.FieldName='AddressCity' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressCity or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressCity,'')+replicate(' ',30),30) else Right(replicate('0',30)+ISNULL(A.AddressCity,''),30) end
	else 
	case when B.COLUMN_NAME='AddressCity' and C.FieldName='AddressCity' then case when C.FieldDataType in ('A','AN') then replicate(' ',30) else replicate('0',30) end
	end end AddressCity,
	case 
	when B.COLUMN_NAME='AddressState' and C.FieldName='AddressState' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressState or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressState,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.AddressState,''),2) end
	else 
	case when B.COLUMN_NAME='AddressState' and C.FieldName='AddressState' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end AddressState,
	case 
	when B.COLUMN_NAME='AddressZipCode' and C.FieldName='AddressZipCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressZipCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressZipCode,'')+replicate(' ',9),9) else Right(replicate('0',9)+ISNULL(A.AddressZipCode,''),9) end
	else 
	case when B.COLUMN_NAME='AddressZipCode' and C.FieldName='AddressZipCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',9) else replicate('0',9) end
	end end AddressZipCode,
	case 
	when B.COLUMN_NAME='NameLinkIdentifier' and C.FieldName='NameLinkIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameLinkIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameLinkIdentifier,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.NameLinkIdentifier,''),3) end
	else 
	case when B.COLUMN_NAME='NameLinkIdentifier' and C.FieldName='NameLinkIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end NameLinkIdentifier,
	case 
	when B.COLUMN_NAME='StateCodeLink' and C.FieldName='StateCodeLink' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCodeLink or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCodeLink,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCodeLink,''),2) end
	else 
	case when B.COLUMN_NAME='StateCodeLink' and C.FieldName='StateCodeLink' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCodeLink,
	case 
	when B.COLUMN_NAME='ExposureRecordLinkForLocationCode' and C.FieldName='ExposureRecordLinkForLocationCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ExposureRecordLinkForLocationCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ExposureRecordLinkForLocationCode,'')+replicate(' ',5),5) else Right(replicate('0',5)+ISNULL(A.ExposureRecordLinkForLocationCode,''),5) end
	else 
	case when B.COLUMN_NAME='ExposureRecordLinkForLocationCode' and C.FieldName='ExposureRecordLinkForLocationCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',5) else replicate('0',5) end
	end end ExposureRecordLinkForLocationCode,
	replicate(' ',25) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='PhoneNumberOfInsured' and C.FieldName='PhoneNumberOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.PhoneNumberOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PhoneNumberOfInsured,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.PhoneNumberOfInsured,''),10) end
	else 
	case when B.COLUMN_NAME='PhoneNumberOfInsured' and C.FieldName='PhoneNumberOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end PhoneNumberOfInsured,
	case 
	when B.COLUMN_NAME='NumberOfEmployees' and C.FieldName='NumberOfEmployees' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NumberOfEmployees or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NumberOfEmployees,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.NumberOfEmployees,''),6) end
	else 
	case when B.COLUMN_NAME='NumberOfEmployees' and C.FieldName='NumberOfEmployees' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end NumberOfEmployees,
	case 
	when B.COLUMN_NAME='IndustryCode' and C.FieldName='IndustryCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.IndustryCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.IndustryCode,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.IndustryCode,''),6) end
	else 
	case when B.COLUMN_NAME='IndustryCode' and C.FieldName='IndustryCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end IndustryCode,
	case 
	when B.COLUMN_NAME='GeographicArea' and C.FieldName='GeographicArea' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.GeographicArea or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.GeographicArea,'')+replicate(' ',16),16) else Right(replicate('0',16)+ISNULL(A.GeographicArea,''),16) end
	else 
	case when B.COLUMN_NAME='GeographicArea' and C.FieldName='GeographicArea' then case when C.FieldDataType in ('A','AN') then replicate(' ',16) else replicate('0',16) end
	end end GeographicArea,
	case 
	when B.COLUMN_NAME='EmailAddress' and C.FieldName='EmailAddress' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EmailAddress or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EmailAddress,'')+replicate(' ',39),39) else Right(replicate('0',39)+ISNULL(A.EmailAddress,''),39) end
	else 
	case when B.COLUMN_NAME='EmailAddress' and C.FieldName='EmailAddress' then case when C.FieldDataType in ('A','AN') then replicate(' ',39) else replicate('0',39) end
	end end EmailAddress,
	replicate(' ',3) ReservedForFutureUse3,
	case 
	when B.COLUMN_NAME='CountryCode' and C.FieldName='CountryCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CountryCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CountryCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.CountryCode,''),2) end
	else 
	case when B.COLUMN_NAME='CountryCode' and C.FieldName='CountryCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end CountryCode,
	case 
	when B.COLUMN_NAME='NameLinkCounterIdentifier' and C.FieldName='NameLinkCounterIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameLinkCounterIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameLinkCounterIdentifier,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.NameLinkCounterIdentifier,''),2) end
	else 
	case when B.COLUMN_NAME='NameLinkCounterIdentifier' and C.FieldName='NameLinkCounterIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end NameLinkCounterIdentifier,
	replicate(' ',18) ReservedForFutureUse4,
	case 
	when B.COLUMN_NAME='PolicyChangeEffectiveDate' and C.FieldName='PolicyChangeEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.PolicyChangeEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PolicyChangeEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.PolicyChangeEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='PolicyChangeEffectiveDate' and C.FieldName='PolicyChangeEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end PolicyChangeEffectiveDate,
	case 
	when B.COLUMN_NAME='PolicyChangeExpirationDate' and C.FieldName='PolicyChangeExpirationDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType and (D.WCPOLSCode=A.PolicyChangeExpirationDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PolicyChangeExpirationDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.PolicyChangeExpirationDate,''),6) end
	else 
	case when B.COLUMN_NAME='PolicyChangeExpirationDate' and C.FieldName='PolicyChangeExpirationDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end PolicyChangeExpirationDate
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols03Record A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols03Record'
	and c.TableName='WCPols03Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols03RecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(AddressTypeCode)+
	max(ForeignAddressIndicator)+
	max(AddressStructureCode)+
	max(AddressStreet)+
	max(AddressCity)+
	max(AddressState)+
	max(AddressZipCode)+
	max(NameLinkIdentifier)+
	max(StateCodeLink)+
	max(ExposureRecordLinkForLocationCode)+
	max(ReservedForFutureUse2)+
	max(PhoneNumberOfInsured)+
	max(NumberOfEmployees)+
	max(IndustryCode)+
	max(GeographicArea)+
	max(EmailAddress)+
	max(ReservedForFutureUse3)+
	max(CountryCode)+
	max(NameLinkCounterIdentifier)+
	max(ReservedForFutureUse4)+
	max(PolicyChangeEffectiveDate)+
	max(PolicyChangeExpirationDate)
	,'0','')))<>''
),
EXP_DataCollect_Record03 AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPOLS_Record03
),
SQ_WCPOLS_Record03_INIAMO AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(AddressTypeCode)+
	max(ForeignAddressIndicator)+
	max(AddressStructureCode)+
	max(AddressStreet)+
	max(AddressCity)+
	max(AddressState)+
	max(AddressZipCode)+
	max(NameLinkIdentifier)+
	max(StateCodeLink)+
	max(ExposureRecordLinkForLocationCode)+
	max(ReservedForFutureUse2)+
	max(PhoneNumberOfInsured)+
	max(NumberOfEmployees)+
	max(IndustryCode)+
	max(GeographicArea)+
	max(EmailAddress)+
	max(ReservedForFutureUse3)+
	max(CountryCode)+
	max(NameLinkCounterIdentifier)+
	max(ReservedForFutureUse4)+
	max(PolicyChangeEffectiveDate)+
	max(PolicyChangeExpirationDate) From (
	Select WCPols03RecordINIAMOID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	replicate(' ',2) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	case 
	when B.COLUMN_NAME='AddressTypeCode' and C.FieldName='AddressTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressTypeCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.AddressTypeCode,''),1) end
	else 
	case when B.COLUMN_NAME='AddressTypeCode' and C.FieldName='AddressTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end AddressTypeCode,
	case 
	when B.COLUMN_NAME='ForeignAddressIndicator' and C.FieldName='ForeignAddressIndicator' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ForeignAddressIndicator or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ForeignAddressIndicator,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.ForeignAddressIndicator,''),1) end
	else 
	case when B.COLUMN_NAME='ForeignAddressIndicator' and C.FieldName='ForeignAddressIndicator' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end ForeignAddressIndicator,
	case 
	when B.COLUMN_NAME='AddressStructureCode' and C.FieldName='AddressStructureCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressStructureCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressStructureCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.AddressStructureCode,''),1) end
	else 
	case when B.COLUMN_NAME='AddressStructureCode' and C.FieldName='AddressStructureCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end AddressStructureCode,
	case 
	when B.COLUMN_NAME='AddressStreet' and C.FieldName='AddressStreet' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressStreet or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressStreet,'')+replicate(' ',60),60) else Right(replicate('0',60)+ISNULL(A.AddressStreet,''),60) end
	else 
	case when B.COLUMN_NAME='AddressStreet' and C.FieldName='AddressStreet' then case when C.FieldDataType in ('A','AN') then replicate(' ',60) else replicate('0',60) end
	end end AddressStreet,
	case 
	when B.COLUMN_NAME='AddressCity' and C.FieldName='AddressCity' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressCity or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressCity,'')+replicate(' ',30),30) else Right(replicate('0',30)+ISNULL(A.AddressCity,''),30) end
	else 
	case when B.COLUMN_NAME='AddressCity' and C.FieldName='AddressCity' then case when C.FieldDataType in ('A','AN') then replicate(' ',30) else replicate('0',30) end
	end end AddressCity,
	case 
	when B.COLUMN_NAME='AddressState' and C.FieldName='AddressState' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressState or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressState,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.AddressState,''),2) end
	else 
	case when B.COLUMN_NAME='AddressState' and C.FieldName='AddressState' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end AddressState,
	case 
	when B.COLUMN_NAME='AddressZipCode' and C.FieldName='AddressZipCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressZipCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressZipCode,'')+replicate(' ',9),9) else Right(replicate('0',9)+ISNULL(A.AddressZipCode,''),9) end
	else 
	case when B.COLUMN_NAME='AddressZipCode' and C.FieldName='AddressZipCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',9) else replicate('0',9) end
	end end AddressZipCode,
	case 
	when B.COLUMN_NAME='NameLinkIdentifier' and C.FieldName='NameLinkIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameLinkIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameLinkIdentifier,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.NameLinkIdentifier,''),3) end
	else 
	case when B.COLUMN_NAME='NameLinkIdentifier' and C.FieldName='NameLinkIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end NameLinkIdentifier,
	case 
	when B.COLUMN_NAME='StateCodeLink' and C.FieldName='StateCodeLink' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCodeLink or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCodeLink,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCodeLink,''),2) end
	else 
	case when B.COLUMN_NAME='StateCodeLink' and C.FieldName='StateCodeLink' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCodeLink,
	case 
	when B.COLUMN_NAME='ExposureRecordLinkForLocationCode' and C.FieldName='ExposureRecordLinkForLocationCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ExposureRecordLinkForLocationCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ExposureRecordLinkForLocationCode,'')+replicate(' ',5),5) else Right(replicate('0',5)+ISNULL(A.ExposureRecordLinkForLocationCode,''),5) end
	else 
	case when B.COLUMN_NAME='ExposureRecordLinkForLocationCode' and C.FieldName='ExposureRecordLinkForLocationCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',5) else replicate('0',5) end
	end end ExposureRecordLinkForLocationCode,
	replicate(' ',25) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='PhoneNumberOfInsured' and C.FieldName='PhoneNumberOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.PhoneNumberOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PhoneNumberOfInsured,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.PhoneNumberOfInsured,''),10) end
	else 
	case when B.COLUMN_NAME='PhoneNumberOfInsured' and C.FieldName='PhoneNumberOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end PhoneNumberOfInsured,
	case 
	when B.COLUMN_NAME='NumberOfEmployees' and C.FieldName='NumberOfEmployees' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NumberOfEmployees or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NumberOfEmployees,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.NumberOfEmployees,''),6) end
	else 
	case when B.COLUMN_NAME='NumberOfEmployees' and C.FieldName='NumberOfEmployees' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end NumberOfEmployees,
	case 
	when B.COLUMN_NAME='IndustryCode' and C.FieldName='IndustryCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.IndustryCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.IndustryCode,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.IndustryCode,''),6) end
	else 
	case when B.COLUMN_NAME='IndustryCode' and C.FieldName='IndustryCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end IndustryCode,
	case 
	when B.COLUMN_NAME='GeographicArea' and C.FieldName='GeographicArea' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.GeographicArea or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.GeographicArea,'')+replicate(' ',16),16) else Right(replicate('0',16)+ISNULL(A.GeographicArea,''),16) end
	else 
	case when B.COLUMN_NAME='GeographicArea' and C.FieldName='GeographicArea' then case when C.FieldDataType in ('A','AN') then replicate(' ',16) else replicate('0',16) end
	end end GeographicArea,
	case 
	when B.COLUMN_NAME='EmailAddress' and C.FieldName='EmailAddress' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EmailAddress or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EmailAddress,'')+replicate(' ',39),39) else Right(replicate('0',39)+ISNULL(A.EmailAddress,''),39) end
	else 
	case when B.COLUMN_NAME='EmailAddress' and C.FieldName='EmailAddress' then case when C.FieldDataType in ('A','AN') then replicate(' ',39) else replicate('0',39) end
	end end EmailAddress,
	replicate(' ',3) ReservedForFutureUse3,
	case 
	when B.COLUMN_NAME='CountryCode' and C.FieldName='CountryCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CountryCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CountryCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.CountryCode,''),2) end
	else 
	case when B.COLUMN_NAME='CountryCode' and C.FieldName='CountryCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end CountryCode,
	case 
	when B.COLUMN_NAME='NameLinkCounterIdentifier' and C.FieldName='NameLinkCounterIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameLinkCounterIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameLinkCounterIdentifier,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.NameLinkCounterIdentifier,''),2) end
	else 
	case when B.COLUMN_NAME='NameLinkCounterIdentifier' and C.FieldName='NameLinkCounterIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end NameLinkCounterIdentifier,
	replicate(' ',18) ReservedForFutureUse4,
	case 
	when B.COLUMN_NAME='PolicyChangeEffectiveDate' and C.FieldName='PolicyChangeEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.PolicyChangeEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PolicyChangeEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.PolicyChangeEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='PolicyChangeEffectiveDate' and C.FieldName='PolicyChangeEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end PolicyChangeEffectiveDate,
	case 
	when B.COLUMN_NAME='PolicyChangeExpirationDate' and C.FieldName='PolicyChangeExpirationDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType and (D.WCPOLSCode=A.PolicyChangeExpirationDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PolicyChangeExpirationDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.PolicyChangeExpirationDate,''),6) end
	else 
	case when B.COLUMN_NAME='PolicyChangeExpirationDate' and C.FieldName='PolicyChangeExpirationDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end PolicyChangeExpirationDate
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols03RecordINIAMO A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols03Record'
	and c.TableName='WCPols03Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols03RecordINIAMOID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(AddressTypeCode)+
	max(ForeignAddressIndicator)+
	max(AddressStructureCode)+
	max(AddressStreet)+
	max(AddressCity)+
	max(AddressState)+
	max(AddressZipCode)+
	max(NameLinkIdentifier)+
	max(StateCodeLink)+
	max(ExposureRecordLinkForLocationCode)+
	max(ReservedForFutureUse2)+
	max(PhoneNumberOfInsured)+
	max(NumberOfEmployees)+
	max(IndustryCode)+
	max(GeographicArea)+
	max(EmailAddress)+
	max(ReservedForFutureUse3)+
	max(CountryCode)+
	max(NameLinkCounterIdentifier)+
	max(ReservedForFutureUse4)+
	max(PolicyChangeEffectiveDate)+
	max(PolicyChangeExpirationDate)
	,'0','')))<>''
),
EXP_DataCollect_Record03_INIAMO AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPOLS_Record03_INIAMO
),
SQ_WCPOLS_Record04 AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(StateAddDeleteCode)+
	max(ClaimAdministratorFEIN)+
	max(IndependentDCORiskIDNumberFileNumberAccountNumber)+
	max(ReservedForFutureUse2)+
	max(CarrierCode)+
	max(ExperienceModificationFactorMeritRatingFactor)+
	max(ExperienceModificationStatusCode)+
	max(ExperienceModificationPlanTypeCode)+
	max(OtherIndividualRiskRatingFactor)+
	max(InsurerPremiumDeviationFactor)+
	max(TypeOfPremiumDeviationCode)+
	max(EstimatedStateStandardPremiumTotal)+
	max(ExpenseConstantAmount)+
	max(LossConstantAmount)+
	max(PremiumDiscountAmount)+
	max(ProRatedExpenseConstantAmountReasonCode)+
	max(ProRatedMinimumPremiumAmountReasonCode)+
	max(ReasonStateWasAddedToThePolicyCode)+
	max(ReservedForFutureUse3)+
	max(ExperienceModificationEffectiveDate)+
	max(AnniversaryRatingDate)+
	max(AssignedRiskAdjustmentProgramFactor)+
	max(ReservedForFutureUse4)+
	max(PremiumAdjustmentPeriodCode)+
	max(TypeOfNonStandardIDCode)+
	max(ReservedForFutureUse5)+
	max(PolicyChangeEffectiveDate)+
	max(PolicyChangeExpirationDate) From (
	Select WCPols04Record@{pipeline().parameters.RECORD_04_TABLENAMEID}, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	case 
	when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	case 
	when B.COLUMN_NAME='StateAddDeleteCode' and C.FieldName='StateAddDeleteCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateAddDeleteCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateAddDeleteCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.StateAddDeleteCode,''),1) end
	else 
	case when B.COLUMN_NAME='StateAddDeleteCode' and C.FieldName='StateAddDeleteCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end StateAddDeleteCode,
	--This case statement was added to replace replicate(' ',9) ReservedForFutureUse1 so that we could utilize this location for the newly added ClaimAdministratorFEIN number
	case 
	when B.COLUMN_NAME='ClaimAdministratorFEIN' and C.FieldName='ClaimAdministratorFEIN' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ClaimAdministratorFEIN or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ClaimAdministratorFEIN,'')+replicate(' ',9),9) else Right(replicate('0',9)+ISNULL(A.ClaimAdministratorFEIN,''),9) end
	else 
	case when B.COLUMN_NAME='ClaimAdministratorFEIN' and C.FieldName='ClaimAdministratorFEIN' then case when C.FieldDataType in ('A','AN') then replicate(' ',9) else replicate('0',9) end
	end end ClaimAdministratorFEIN,
	case 
	when B.COLUMN_NAME='IndependentDCORiskIDNumberFileNumberAccountNumber' and C.FieldName='IndependentDCORiskIDNumberFileNumberAccountNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.IndependentDCORiskIDNumberFileNumberAccountNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.IndependentDCORiskIDNumberFileNumberAccountNumber,'')+replicate(' ',15),15) else Right(replicate('0',15)+ISNULL(A.IndependentDCORiskIDNumberFileNumberAccountNumber,''),15) end
	else 
	case when B.COLUMN_NAME='IndependentDCORiskIDNumberFileNumberAccountNumber' and C.FieldName='IndependentDCORiskIDNumberFileNumberAccountNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',15) else replicate('0',15) end
	end end IndependentDCORiskIDNumberFileNumberAccountNumber,
	replicate(' ',15) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='CarrierCode' and C.FieldName='CarrierCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierCode,'')+replicate(' ',5),5) else Right(replicate('0',5)+ISNULL(A.CarrierCode,''),5) end
	else 
	case when B.COLUMN_NAME='CarrierCode' and C.FieldName='CarrierCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',5) else replicate('0',5) end
	end end CarrierCode,
	case 
	when B.COLUMN_NAME='ExperienceModificationFactorMeritRatingFactor' and C.FieldName='ExperienceModificationFactorMeritRatingFactor' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ExperienceModificationFactorMeritRatingFactor or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ExperienceModificationFactorMeritRatingFactor,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.ExperienceModificationFactorMeritRatingFactor,''),4) end
	else 
	case when B.COLUMN_NAME='ExperienceModificationFactorMeritRatingFactor' and C.FieldName='ExperienceModificationFactorMeritRatingFactor' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end ExperienceModificationFactorMeritRatingFactor,
	case 
	when B.COLUMN_NAME='ExperienceModificationStatusCode' and C.FieldName='ExperienceModificationStatusCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ExperienceModificationStatusCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ExperienceModificationStatusCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.ExperienceModificationStatusCode,''),1) end
	else 
	case when B.COLUMN_NAME='ExperienceModificationStatusCode' and C.FieldName='ExperienceModificationStatusCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end ExperienceModificationStatusCode,
	case 
	when B.COLUMN_NAME='ExperienceModificationPlanTypeCode' and C.FieldName='ExperienceModificationPlanTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ExperienceModificationPlanTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ExperienceModificationPlanTypeCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.ExperienceModificationPlanTypeCode,''),1) end
	else 
	case when B.COLUMN_NAME='ExperienceModificationPlanTypeCode' and C.FieldName='ExperienceModificationPlanTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end ExperienceModificationPlanTypeCode,
	case 
	when B.COLUMN_NAME='OtherIndividualRiskRatingFactor' and C.FieldName='OtherIndividualRiskRatingFactor' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.OtherIndividualRiskRatingFactor or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.OtherIndividualRiskRatingFactor,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.OtherIndividualRiskRatingFactor,''),4) end
	else 
	case when B.COLUMN_NAME='OtherIndividualRiskRatingFactor' and C.FieldName='OtherIndividualRiskRatingFactor' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end OtherIndividualRiskRatingFactor,
	case 
	when B.COLUMN_NAME='InsurerPremiumDeviationFactor' and C.FieldName='InsurerPremiumDeviationFactor' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.InsurerPremiumDeviationFactor or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.InsurerPremiumDeviationFactor,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.InsurerPremiumDeviationFactor,''),4) end
	else 
	case when B.COLUMN_NAME='InsurerPremiumDeviationFactor' and C.FieldName='InsurerPremiumDeviationFactor' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end InsurerPremiumDeviationFactor,
	case 
	when B.COLUMN_NAME='TypeOfPremiumDeviationCode' and C.FieldName='TypeOfPremiumDeviationCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.TypeOfPremiumDeviationCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.TypeOfPremiumDeviationCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.TypeOfPremiumDeviationCode,''),1) end
	else 
	case when B.COLUMN_NAME='TypeOfPremiumDeviationCode' and C.FieldName='TypeOfPremiumDeviationCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end TypeOfPremiumDeviationCode,
	case 
	when B.COLUMN_NAME='EstimatedStateStandardPremiumTotal' and C.FieldName='EstimatedStateStandardPremiumTotal' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EstimatedStateStandardPremiumTotal or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EstimatedStateStandardPremiumTotal,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.EstimatedStateStandardPremiumTotal,''),10) end
	else 
	case when B.COLUMN_NAME='EstimatedStateStandardPremiumTotal' and C.FieldName='EstimatedStateStandardPremiumTotal' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end EstimatedStateStandardPremiumTotal,
	case 
	when B.COLUMN_NAME='ExpenseConstantAmount' and C.FieldName='ExpenseConstantAmount' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ExpenseConstantAmount or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ExpenseConstantAmount,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.ExpenseConstantAmount,''),10) end
	else 
	case when B.COLUMN_NAME='ExpenseConstantAmount' and C.FieldName='ExpenseConstantAmount' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end ExpenseConstantAmount,
	case 
	when B.COLUMN_NAME='LossConstantAmount' and C.FieldName='LossConstantAmount' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LossConstantAmount or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LossConstantAmount,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.LossConstantAmount,''),10) end
	else 
	case when B.COLUMN_NAME='LossConstantAmount' and C.FieldName='LossConstantAmount' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end LossConstantAmount,
	case 
	when B.COLUMN_NAME='PremiumDiscountAmount' and C.FieldName='PremiumDiscountAmount' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.PremiumDiscountAmount or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PremiumDiscountAmount,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.PremiumDiscountAmount,''),10) end
	else 
	case when B.COLUMN_NAME='PremiumDiscountAmount' and C.FieldName='PremiumDiscountAmount' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end PremiumDiscountAmount,
	case 
	when B.COLUMN_NAME='ProRatedExpenseConstantAmountReasonCode' and C.FieldName='ProRatedExpenseConstantAmountReasonCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ProRatedExpenseConstantAmountReasonCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ProRatedExpenseConstantAmountReasonCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.ProRatedExpenseConstantAmountReasonCode,''),1) end
	else 
	case when B.COLUMN_NAME='ProRatedExpenseConstantAmountReasonCode' and C.FieldName='ProRatedExpenseConstantAmountReasonCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end ProRatedExpenseConstantAmountReasonCode,
	case 
	when B.COLUMN_NAME='ProRatedMinimumPremiumAmountReasonCode' and C.FieldName='ProRatedMinimumPremiumAmountReasonCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ProRatedMinimumPremiumAmountReasonCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ProRatedMinimumPremiumAmountReasonCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.ProRatedMinimumPremiumAmountReasonCode,''),1) end
	else 
	case when B.COLUMN_NAME='ProRatedMinimumPremiumAmountReasonCode' and C.FieldName='ProRatedMinimumPremiumAmountReasonCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end ProRatedMinimumPremiumAmountReasonCode,
	case 
	when B.COLUMN_NAME='ReasonStateWasAddedToThePolicyCode' and C.FieldName='ReasonStateWasAddedToThePolicyCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ReasonStateWasAddedToThePolicyCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ReasonStateWasAddedToThePolicyCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.ReasonStateWasAddedToThePolicyCode,''),1) end
	else 
	case when B.COLUMN_NAME='ReasonStateWasAddedToThePolicyCode' and C.FieldName='ReasonStateWasAddedToThePolicyCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end ReasonStateWasAddedToThePolicyCode,
	replicate(' ',3) ReservedForFutureUse3,
	case 
	when B.COLUMN_NAME='ExperienceModificationEffectiveDate' and C.FieldName='ExperienceModificationEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ExperienceModificationEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ExperienceModificationEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.ExperienceModificationEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='ExperienceModificationEffectiveDate' and C.FieldName='ExperienceModificationEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end ExperienceModificationEffectiveDate,
	case 
	when B.COLUMN_NAME='AnniversaryRatingDate' and C.FieldName='AnniversaryRatingDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AnniversaryRatingDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AnniversaryRatingDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.AnniversaryRatingDate,''),6) end
	else 
	case when B.COLUMN_NAME='AnniversaryRatingDate' and C.FieldName='AnniversaryRatingDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end AnniversaryRatingDate,
	case 
	when B.COLUMN_NAME='AssignedRiskAdjustmentProgramFactor' and C.FieldName='AssignedRiskAdjustmentProgramFactor' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AssignedRiskAdjustmentProgramFactor or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AssignedRiskAdjustmentProgramFactor,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.AssignedRiskAdjustmentProgramFactor,''),4) end
	else 
	case when B.COLUMN_NAME='AssignedRiskAdjustmentProgramFactor' and C.FieldName='AssignedRiskAdjustmentProgramFactor' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end AssignedRiskAdjustmentProgramFactor,
	replicate(' ',16) ReservedForFutureUse4,
	case 
	when B.COLUMN_NAME='PremiumAdjustmentPeriodCode' and C.FieldName='PremiumAdjustmentPeriodCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.PremiumAdjustmentPeriodCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PremiumAdjustmentPeriodCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.PremiumAdjustmentPeriodCode,''),1) end
	else 
	case when B.COLUMN_NAME='PremiumAdjustmentPeriodCode' and C.FieldName='PremiumAdjustmentPeriodCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end PremiumAdjustmentPeriodCode,
	case 
	when B.COLUMN_NAME='TypeOfNonStandardIDCode' and C.FieldName='TypeOfNonStandardIDCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.TypeOfNonStandardIDCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.TypeOfNonStandardIDCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.TypeOfNonStandardIDCode,''),2) end
	else 
	case when B.COLUMN_NAME='TypeOfNonStandardIDCode' and C.FieldName='TypeOfNonStandardIDCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end TypeOfNonStandardIDCode,
	replicate(' ',100) ReservedForFutureUse5,
	case 
	when B.COLUMN_NAME='PolicyChangeEffectiveDate' and C.FieldName='PolicyChangeEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.PolicyChangeEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PolicyChangeEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.PolicyChangeEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='PolicyChangeEffectiveDate' and C.FieldName='PolicyChangeEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end PolicyChangeEffectiveDate,
	case 
	when B.COLUMN_NAME='PolicyChangeExpirationDate' and C.FieldName='PolicyChangeExpirationDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType and (D.WCPOLSCode=A.PolicyChangeExpirationDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PolicyChangeExpirationDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.PolicyChangeExpirationDate,''),6) end
	else 
	case when B.COLUMN_NAME='PolicyChangeExpirationDate' and C.FieldName='PolicyChangeExpirationDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end PolicyChangeExpirationDate
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols04Record@{pipeline().parameters.RECORD_04_TABLENAME} A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols04Record'
	and c.TableName='WCPols04Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols04Record@{pipeline().parameters.RECORD_04_TABLENAMEID}, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(StateAddDeleteCode)+
	max(ClaimAdministratorFEIN)+
	max(IndependentDCORiskIDNumberFileNumberAccountNumber)+
	max(ReservedForFutureUse2)+
	max(CarrierCode)+
	max(ExperienceModificationFactorMeritRatingFactor)+
	max(ExperienceModificationStatusCode)+
	max(ExperienceModificationPlanTypeCode)+
	max(OtherIndividualRiskRatingFactor)+
	max(InsurerPremiumDeviationFactor)+
	max(TypeOfPremiumDeviationCode)+
	max(EstimatedStateStandardPremiumTotal)+
	max(ExpenseConstantAmount)+
	max(LossConstantAmount)+
	max(PremiumDiscountAmount)+
	max(ProRatedExpenseConstantAmountReasonCode)+
	max(ProRatedMinimumPremiumAmountReasonCode)+
	max(ReasonStateWasAddedToThePolicyCode)+
	max(ReservedForFutureUse3)+
	max(ExperienceModificationEffectiveDate)+
	max(AnniversaryRatingDate)+
	max(AssignedRiskAdjustmentProgramFactor)+
	max(ReservedForFutureUse4)+
	max(PremiumAdjustmentPeriodCode)+
	max(TypeOfNonStandardIDCode)+
	max(ReservedForFutureUse5)+
	max(PolicyChangeEffectiveDate)+
	max(PolicyChangeExpirationDate)
	,'0','')))<>''
),
EXP_DataCollect_Record04 AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPOLS_Record04
),
SQ_WCPOLS_Record05 AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse1)+
	max(ClassificationCode)+
	max(ClassificationUseCode)+
	max(ReservedForFutureUse2)+
	max(ClassificationWordingSuffix)+
	max(ExposureActExposureCoverageCode)+
	max(ManualChargedRate)+
	max(ExposurePeriodEffectiveDate)+
	max(ReservedForFutureUse3)+
	max(EstimatedExposureAmount)+
	max(EstimatedPremiumAmount)+
	max(ExposurePeriodCode)+
	max(ClassificationWording)+
	max(ReservedForFutureUse4)+
	max(NameLinkIdentifier)+
	max(StateCodeLink)+
	max(ExposureRecordLinkForExposureCode)+
	max(NameLinkCounterIdentifier)+
	max(ReservedForFutureUse5)+
	max(NumberOfPiecesOfApparatus)+
	max(NumberOfVolunteers)+
	max(PolicySurchargeFactor)+
	max(PlanPremiumAdjustmentFactor)+
	max(ReservedForFutureUse6)+
	max(PolicyChangeEffectiveDate)+
	max(PolicyChangeExpirationDate) From (
	Select WCPols05RecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	case 
	when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	replicate(' ',3) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='ClassificationCode' and C.FieldName='ClassificationCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ClassificationCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ClassificationCode,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.ClassificationCode,''),4) end
	else 
	case when B.COLUMN_NAME='ClassificationCode' and C.FieldName='ClassificationCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end ClassificationCode,
	case 
	when B.COLUMN_NAME='ClassificationUseCode' and C.FieldName='ClassificationUseCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ClassificationUseCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ClassificationUseCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.ClassificationUseCode,''),1) end
	else 
	case when B.COLUMN_NAME='ClassificationUseCode' and C.FieldName='ClassificationUseCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end ClassificationUseCode,
	replicate(' ',9) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='ClassificationWordingSuffix' and C.FieldName='ClassificationWordingSuffix' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ClassificationWordingSuffix or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ClassificationWordingSuffix,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.ClassificationWordingSuffix,''),2) end
	else 
	case when B.COLUMN_NAME='ClassificationWordingSuffix' and C.FieldName='ClassificationWordingSuffix' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end ClassificationWordingSuffix,
	case 
	when B.COLUMN_NAME='ExposureActExposureCoverageCode' and C.FieldName='ExposureActExposureCoverageCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ExposureActExposureCoverageCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ExposureActExposureCoverageCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.ExposureActExposureCoverageCode,''),2) end
	else 
	case when B.COLUMN_NAME='ExposureActExposureCoverageCode' and C.FieldName='ExposureActExposureCoverageCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end ExposureActExposureCoverageCode,
	case 
	when B.COLUMN_NAME='ManualChargedRate' and C.FieldName='ManualChargedRate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ManualChargedRate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ManualChargedRate,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.ManualChargedRate,''),10) end
	else 
	case when B.COLUMN_NAME='ManualChargedRate' and C.FieldName='ManualChargedRate' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end ManualChargedRate,
	case 
	when B.COLUMN_NAME='ExposurePeriodEffectiveDate' and C.FieldName='ExposurePeriodEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ExposurePeriodEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ExposurePeriodEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.ExposurePeriodEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='ExposurePeriodEffectiveDate' and C.FieldName='ExposurePeriodEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end ExposurePeriodEffectiveDate,
	replicate(' ',10) ReservedForFutureUse3,
	case 
	when B.COLUMN_NAME='EstimatedExposureAmount' and C.FieldName='EstimatedExposureAmount' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EstimatedExposureAmount or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EstimatedExposureAmount,'')+replicate(' ',12),12) else Right(replicate('0',12)+ISNULL(A.EstimatedExposureAmount,''),12) end
	else 
	case when B.COLUMN_NAME='EstimatedExposureAmount' and C.FieldName='EstimatedExposureAmount' then case when C.FieldDataType in ('A','AN') then replicate(' ',12) else replicate('0',12) end
	end end EstimatedExposureAmount,
	case 
	when B.COLUMN_NAME='EstimatedPremiumAmount' and C.FieldName='EstimatedPremiumAmount' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EstimatedPremiumAmount or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EstimatedPremiumAmount,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.EstimatedPremiumAmount,''),10) end
	else 
	case when B.COLUMN_NAME='EstimatedPremiumAmount' and C.FieldName='EstimatedPremiumAmount' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end EstimatedPremiumAmount,
	case 
	when B.COLUMN_NAME='ExposurePeriodCode' and C.FieldName='ExposurePeriodCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ExposurePeriodCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ExposurePeriodCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.ExposurePeriodCode,''),1) end
	else 
	case when B.COLUMN_NAME='ExposurePeriodCode' and C.FieldName='ExposurePeriodCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end ExposurePeriodCode,
	case 
	when B.COLUMN_NAME='ClassificationWording' and C.FieldName='ClassificationWording' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ClassificationWording or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ClassificationWording,'')+replicate(' ',101),101) else Right(replicate('0',101)+ISNULL(A.ClassificationWording,''),101) end
	else 
	case when B.COLUMN_NAME='ClassificationWording' and C.FieldName='ClassificationWording' then case when C.FieldDataType in ('A','AN') then replicate(' ',101) else replicate('0',101) end
	end end ClassificationWording,
	replicate(' ',2) ReservedForFutureUse4,
	case 
	when B.COLUMN_NAME='NameLinkIdentifier' and C.FieldName='NameLinkIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameLinkIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameLinkIdentifier,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.NameLinkIdentifier,''),3) end
	else 
	case when B.COLUMN_NAME='NameLinkIdentifier' and C.FieldName='NameLinkIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end NameLinkIdentifier,
	case 
	when B.COLUMN_NAME='StateCodeLink' and C.FieldName='StateCodeLink' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCodeLink or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCodeLink,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCodeLink,''),2) end
	else 
	case when B.COLUMN_NAME='StateCodeLink' and C.FieldName='StateCodeLink' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCodeLink,
	case 
	when B.COLUMN_NAME='ExposureRecordLinkForExposureCode' and C.FieldName='ExposureRecordLinkForExposureCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ExposureRecordLinkForExposureCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ExposureRecordLinkForExposureCode,'')+replicate(' ',5),5) else Right(replicate('0',5)+ISNULL(A.ExposureRecordLinkForExposureCode,''),5) end
	else 
	case when B.COLUMN_NAME='ExposureRecordLinkForExposureCode' and C.FieldName='ExposureRecordLinkForExposureCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',5) else replicate('0',5) end
	end end ExposureRecordLinkForExposureCode,
	case 
	when B.COLUMN_NAME='NameLinkCounterIdentifier' and C.FieldName='NameLinkCounterIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameLinkCounterIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameLinkCounterIdentifier,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.NameLinkCounterIdentifier,''),2) end
	else 
	case when B.COLUMN_NAME='NameLinkCounterIdentifier' and C.FieldName='NameLinkCounterIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end NameLinkCounterIdentifier,
	replicate(' ',28) ReservedForFutureUse5,
	case 
	when B.COLUMN_NAME='NumberOfPiecesOfApparatus' and C.FieldName='NumberOfPiecesOfApparatus' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NumberOfPiecesOfApparatus or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NumberOfPiecesOfApparatus,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.NumberOfPiecesOfApparatus,''),3) end
	else 
	case when B.COLUMN_NAME='NumberOfPiecesOfApparatus' and C.FieldName='NumberOfPiecesOfApparatus' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end NumberOfPiecesOfApparatus,
	case 
	when B.COLUMN_NAME='NumberOfVolunteers' and C.FieldName='NumberOfVolunteers' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NumberOfVolunteers or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NumberOfVolunteers,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.NumberOfVolunteers,''),3) end
	else 
	case when B.COLUMN_NAME='NumberOfVolunteers' and C.FieldName='NumberOfVolunteers' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end NumberOfVolunteers,
	case 
	when B.COLUMN_NAME='PolicySurchargeFactor' and C.FieldName='PolicySurchargeFactor' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.PolicySurchargeFactor or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PolicySurchargeFactor,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.PolicySurchargeFactor,''),10) end
	else 
	case when B.COLUMN_NAME='PolicySurchargeFactor' and C.FieldName='PolicySurchargeFactor' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end PolicySurchargeFactor,
	case 
	when B.COLUMN_NAME='PlanPremiumAdjustmentFactor' and C.FieldName='PlanPremiumAdjustmentFactor' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.PlanPremiumAdjustmentFactor or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PlanPremiumAdjustmentFactor,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.PlanPremiumAdjustmentFactor,''),3) end
	else 
	case when B.COLUMN_NAME='PlanPremiumAdjustmentFactor' and C.FieldName='PlanPremiumAdjustmentFactor' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end PlanPremiumAdjustmentFactor,
	replicate(' ',9) ReservedForFutureUse6,
	case 
	when B.COLUMN_NAME='PolicyChangeEffectiveDate' and C.FieldName='PolicyChangeEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.PolicyChangeEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PolicyChangeEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.PolicyChangeEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='PolicyChangeEffectiveDate' and C.FieldName='PolicyChangeEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end PolicyChangeEffectiveDate,
	case 
	when B.COLUMN_NAME='PolicyChangeExpirationDate' and C.FieldName='PolicyChangeExpirationDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType and (D.WCPOLSCode=A.PolicyChangeExpirationDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PolicyChangeExpirationDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.PolicyChangeExpirationDate,''),6) end
	else 
	case when B.COLUMN_NAME='PolicyChangeExpirationDate' and C.FieldName='PolicyChangeExpirationDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end PolicyChangeExpirationDate
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols05Record A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols05Record'
	and c.TableName='WCPols05Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols05RecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse1)+
	max(ClassificationCode)+
	max(ClassificationUseCode)+
	max(ReservedForFutureUse2)+
	max(ClassificationWordingSuffix)+
	max(ExposureActExposureCoverageCode)+
	max(ManualChargedRate)+
	max(ExposurePeriodEffectiveDate)+
	max(ReservedForFutureUse3)+
	max(EstimatedExposureAmount)+
	max(EstimatedPremiumAmount)+
	max(ExposurePeriodCode)+
	max(ClassificationWording)+
	max(ReservedForFutureUse4)+
	max(NameLinkIdentifier)+
	max(StateCodeLink)+
	max(ExposureRecordLinkForExposureCode)+
	max(NameLinkCounterIdentifier)+
	max(ReservedForFutureUse5)+
	max(NumberOfPiecesOfApparatus)+
	max(NumberOfVolunteers)+
	max(PolicySurchargeFactor)+
	max(PlanPremiumAdjustmentFactor)+
	max(ReservedForFutureUse6)+
	max(PolicyChangeEffectiveDate)+
	max(PolicyChangeExpirationDate)
	,'0','')))<>''
),
EXP_DataCollect_Record05 AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPOLS_Record05
),
SQ_WCPOLS_Record06 AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(InclusionExclusionCode)+
	max(StateCode01)+
	max(StateCode02)+
	max(StateCode03)+
	max(StateCode04)+
	max(StateCode05)+
	max(StateCode06)+
	max(StateCode07)+
	max(StateCode08)+
	max(StateCode09)+
	max(StateCode10)+
	max(StateCode11)+
	max(StateCode12)+
	max(StateCode13)+
	max(StateCode14)+
	max(StateCode15)+
	max(StateCode16)+
	max(StateCode17)+
	max(StateCode18)+
	max(StateCode19)+
	max(StateCode20)+
	max(StateCode21)+
	max(StateCode22)+
	max(StateCode23)+
	max(StateCode24)+
	max(StateCode25)+
	max(StateCode26)+
	max(StateCode27)+
	max(StateCode28)+
	max(StateCode29)+
	max(StateCode30)+
	max(StateCode31)+
	max(StateCode32)+
	max(StateCode33)+
	max(StateCode34)+
	max(StateCode35)+
	max(StateCode36)+
	max(StateCode37)+
	max(StateCode38)+
	max(StateCode39)+
	max(StateCode40)+
	max(StateCode41)+
	max(StateCode42)+
	max(StateCode43)+
	max(StateCode44)+
	max(StateCode45)+
	max(StateCode46)+
	max(StateCode47)+
	max(StateCode48)+
	max(StateCode49)+
	max(StateCode50)+
	max(StateCode51)+
	max(StateCode52)+
	max(StateCode53)+
	max(StateCode54)+
	max(StateCode55)+
	max(StateCode56)+
	max(StateCode57)+
	max(StateCode58)+
	max(StateCode59)+
	max(StateCode60)+
	max(ReservedForFutureUse2)+
	max(PolicyChangeEffectiveDate)+
	max(PolicyChangeExpirationDate) From (
	Select WCPols06RecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	replicate(' ',2) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	case 
	when B.COLUMN_NAME='InclusionExclusionCode' and C.FieldName='InclusionExclusionCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.InclusionExclusionCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.InclusionExclusionCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.InclusionExclusionCode,''),1) end
	else 
	case when B.COLUMN_NAME='InclusionExclusionCode' and C.FieldName='InclusionExclusionCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end InclusionExclusionCode,
	case 
	when B.COLUMN_NAME='StateCode01' and C.FieldName='StateCode01' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode01 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode01,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode01,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode01' and C.FieldName='StateCode01' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode01,
	case 
	when B.COLUMN_NAME='StateCode02' and C.FieldName='StateCode02' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode02 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode02,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode02,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode02' and C.FieldName='StateCode02' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode02,
	case 
	when B.COLUMN_NAME='StateCode03' and C.FieldName='StateCode03' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode03 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode03,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode03,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode03' and C.FieldName='StateCode03' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode03,
	case 
	when B.COLUMN_NAME='StateCode04' and C.FieldName='StateCode04' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode04 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode04,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode04,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode04' and C.FieldName='StateCode04' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode04,
	case 
	when B.COLUMN_NAME='StateCode05' and C.FieldName='StateCode05' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode05 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode05,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode05,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode05' and C.FieldName='StateCode05' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode05,
	case 
	when B.COLUMN_NAME='StateCode06' and C.FieldName='StateCode06' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode06 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode06,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode06,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode06' and C.FieldName='StateCode06' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode06,
	case 
	when B.COLUMN_NAME='StateCode07' and C.FieldName='StateCode07' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode07 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode07,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode07,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode07' and C.FieldName='StateCode07' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode07,
	case 
	when B.COLUMN_NAME='StateCode08' and C.FieldName='StateCode08' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode08 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode08,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode08,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode08' and C.FieldName='StateCode08' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode08,
	case 
	when B.COLUMN_NAME='StateCode09' and C.FieldName='StateCode09' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode09 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode09,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode09,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode09' and C.FieldName='StateCode09' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode09,
	case 
	when B.COLUMN_NAME='StateCode10' and C.FieldName='StateCode10' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode10 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode10,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode10,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode10' and C.FieldName='StateCode10' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode10,
	case 
	when B.COLUMN_NAME='StateCode11' and C.FieldName='StateCode11' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode11 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode11,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode11,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode11' and C.FieldName='StateCode11' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode11,
	case 
	when B.COLUMN_NAME='StateCode12' and C.FieldName='StateCode12' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode12 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode12,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode12,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode12' and C.FieldName='StateCode12' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode12,
	case 
	when B.COLUMN_NAME='StateCode13' and C.FieldName='StateCode13' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode13 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode13,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode13,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode13' and C.FieldName='StateCode13' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode13,
	case 
	when B.COLUMN_NAME='StateCode14' and C.FieldName='StateCode14' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode14 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode14,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode14,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode14' and C.FieldName='StateCode14' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode14,
	case 
	when B.COLUMN_NAME='StateCode15' and C.FieldName='StateCode15' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode15 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode15,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode15,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode15' and C.FieldName='StateCode15' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode15,
	case 
	when B.COLUMN_NAME='StateCode16' and C.FieldName='StateCode16' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode16 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode16,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode16,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode16' and C.FieldName='StateCode16' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode16,
	case 
	when B.COLUMN_NAME='StateCode17' and C.FieldName='StateCode17' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode17 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode17,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode17,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode17' and C.FieldName='StateCode17' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode17,
	case 
	when B.COLUMN_NAME='StateCode18' and C.FieldName='StateCode18' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode18 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode18,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode18,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode18' and C.FieldName='StateCode18' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode18,
	case 
	when B.COLUMN_NAME='StateCode19' and C.FieldName='StateCode19' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode19 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode19,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode19,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode19' and C.FieldName='StateCode19' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode19,
	case 
	when B.COLUMN_NAME='StateCode20' and C.FieldName='StateCode20' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode20 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode20,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode20,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode20' and C.FieldName='StateCode20' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode20,
	case 
	when B.COLUMN_NAME='StateCode21' and C.FieldName='StateCode21' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode21 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode21,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode21,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode21' and C.FieldName='StateCode21' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode21,
	case 
	when B.COLUMN_NAME='StateCode22' and C.FieldName='StateCode22' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode22 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode22,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode22,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode22' and C.FieldName='StateCode22' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode22,
	case 
	when B.COLUMN_NAME='StateCode23' and C.FieldName='StateCode23' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode23 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode23,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode23,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode23' and C.FieldName='StateCode23' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode23,
	case 
	when B.COLUMN_NAME='StateCode24' and C.FieldName='StateCode24' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode24 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode24,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode24,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode24' and C.FieldName='StateCode24' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode24,
	case 
	when B.COLUMN_NAME='StateCode25' and C.FieldName='StateCode25' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode25 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode25,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode25,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode25' and C.FieldName='StateCode25' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode25,
	case 
	when B.COLUMN_NAME='StateCode26' and C.FieldName='StateCode26' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode26 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode26,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode26,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode26' and C.FieldName='StateCode26' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode26,
	case 
	when B.COLUMN_NAME='StateCode27' and C.FieldName='StateCode27' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode27 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode27,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode27,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode27' and C.FieldName='StateCode27' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode27,
	case 
	when B.COLUMN_NAME='StateCode28' and C.FieldName='StateCode28' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode28 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode28,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode28,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode28' and C.FieldName='StateCode28' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode28,
	case 
	when B.COLUMN_NAME='StateCode29' and C.FieldName='StateCode29' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode29 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode29,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode29,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode29' and C.FieldName='StateCode29' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode29,
	case 
	when B.COLUMN_NAME='StateCode30' and C.FieldName='StateCode30' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode30 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode30,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode30,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode30' and C.FieldName='StateCode30' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode30,
	case 
	when B.COLUMN_NAME='StateCode31' and C.FieldName='StateCode31' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode31 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode31,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode31,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode31' and C.FieldName='StateCode31' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode31,
	case 
	when B.COLUMN_NAME='StateCode32' and C.FieldName='StateCode32' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode32 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode32,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode32,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode32' and C.FieldName='StateCode32' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode32,
	case 
	when B.COLUMN_NAME='StateCode33' and C.FieldName='StateCode33' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode33 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode33,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode33,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode33' and C.FieldName='StateCode33' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode33,
	case 
	when B.COLUMN_NAME='StateCode34' and C.FieldName='StateCode34' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode34 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode34,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode34,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode34' and C.FieldName='StateCode34' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode34,
	case 
	when B.COLUMN_NAME='StateCode35' and C.FieldName='StateCode35' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode35 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode35,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode35,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode35' and C.FieldName='StateCode35' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode35,
	case 
	when B.COLUMN_NAME='StateCode36' and C.FieldName='StateCode36' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode36 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode36,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode36,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode36' and C.FieldName='StateCode36' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode36,
	case 
	when B.COLUMN_NAME='StateCode37' and C.FieldName='StateCode37' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode37 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode37,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode37,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode37' and C.FieldName='StateCode37' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode37,
	case 
	when B.COLUMN_NAME='StateCode38' and C.FieldName='StateCode38' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode38 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode38,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode38,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode38' and C.FieldName='StateCode38' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode38,
	case 
	when B.COLUMN_NAME='StateCode39' and C.FieldName='StateCode39' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode39 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode39,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode39,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode39' and C.FieldName='StateCode39' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode39,
	case 
	when B.COLUMN_NAME='StateCode40' and C.FieldName='StateCode40' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode40 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode40,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode40,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode40' and C.FieldName='StateCode40' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode40,
	case 
	when B.COLUMN_NAME='StateCode41' and C.FieldName='StateCode41' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode41 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode41,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode41,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode41' and C.FieldName='StateCode41' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode41,
	case 
	when B.COLUMN_NAME='StateCode42' and C.FieldName='StateCode42' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode42 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode42,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode42,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode42' and C.FieldName='StateCode42' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode42,
	case 
	when B.COLUMN_NAME='StateCode43' and C.FieldName='StateCode43' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode43 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode43,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode43,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode43' and C.FieldName='StateCode43' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode43,
	case 
	when B.COLUMN_NAME='StateCode44' and C.FieldName='StateCode44' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode44 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode44,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode44,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode44' and C.FieldName='StateCode44' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode44,
	case 
	when B.COLUMN_NAME='StateCode45' and C.FieldName='StateCode45' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode45 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode45,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode45,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode45' and C.FieldName='StateCode45' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode45,
	case 
	when B.COLUMN_NAME='StateCode46' and C.FieldName='StateCode46' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode46 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode46,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode46,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode46' and C.FieldName='StateCode46' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode46,
	case 
	when B.COLUMN_NAME='StateCode47' and C.FieldName='StateCode47' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode47 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode47,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode47,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode47' and C.FieldName='StateCode47' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode47,
	case 
	when B.COLUMN_NAME='StateCode48' and C.FieldName='StateCode48' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode48 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode48,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode48,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode48' and C.FieldName='StateCode48' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode48,
	case 
	when B.COLUMN_NAME='StateCode49' and C.FieldName='StateCode49' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode49 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode49,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode49,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode49' and C.FieldName='StateCode49' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode49,
	case 
	when B.COLUMN_NAME='StateCode50' and C.FieldName='StateCode50' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode50 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode50,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode50,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode50' and C.FieldName='StateCode50' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode50,
	case 
	when B.COLUMN_NAME='StateCode51' and C.FieldName='StateCode51' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode51 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode51,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode51,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode51' and C.FieldName='StateCode51' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode51,
	case 
	when B.COLUMN_NAME='StateCode52' and C.FieldName='StateCode52' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode52 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode52,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode52,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode52' and C.FieldName='StateCode52' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode52,
	case 
	when B.COLUMN_NAME='StateCode53' and C.FieldName='StateCode53' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode53 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode53,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode53,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode53' and C.FieldName='StateCode53' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode53,
	case 
	when B.COLUMN_NAME='StateCode54' and C.FieldName='StateCode54' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode54 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode54,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode54,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode54' and C.FieldName='StateCode54' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode54,
	case 
	when B.COLUMN_NAME='StateCode55' and C.FieldName='StateCode55' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode55 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode55,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode55,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode55' and C.FieldName='StateCode55' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode55,
	case 
	when B.COLUMN_NAME='StateCode56' and C.FieldName='StateCode56' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode56 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode56,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode56,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode56' and C.FieldName='StateCode56' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode56,
	case 
	when B.COLUMN_NAME='StateCode57' and C.FieldName='StateCode57' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode57 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode57,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode57,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode57' and C.FieldName='StateCode57' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode57,
	case 
	when B.COLUMN_NAME='StateCode58' and C.FieldName='StateCode58' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode58 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode58,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode58,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode58' and C.FieldName='StateCode58' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode58,
	case 
	when B.COLUMN_NAME='StateCode59' and C.FieldName='StateCode59' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode59 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode59,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode59,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode59' and C.FieldName='StateCode59' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode59,
	case 
	when B.COLUMN_NAME='StateCode60' and C.FieldName='StateCode60' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode60 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode60,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode60,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode60' and C.FieldName='StateCode60' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode60,
	replicate(' ',120) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='PolicyChangeEffectiveDate' and C.FieldName='PolicyChangeEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.PolicyChangeEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PolicyChangeEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.PolicyChangeEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='PolicyChangeEffectiveDate' and C.FieldName='PolicyChangeEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end PolicyChangeEffectiveDate,
	case 
	when B.COLUMN_NAME='PolicyChangeExpirationDate' and C.FieldName='PolicyChangeExpirationDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType and (D.WCPOLSCode=A.PolicyChangeExpirationDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PolicyChangeExpirationDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.PolicyChangeExpirationDate,''),6) end
	else 
	case when B.COLUMN_NAME='PolicyChangeExpirationDate' and C.FieldName='PolicyChangeExpirationDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end PolicyChangeExpirationDate
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols06Record A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols06Record'
	and c.TableName='WCPols06Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols06RecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(InclusionExclusionCode)+
	max(StateCode01)+
	max(StateCode02)+
	max(StateCode03)+
	max(StateCode04)+
	max(StateCode05)+
	max(StateCode06)+
	max(StateCode07)+
	max(StateCode08)+
	max(StateCode09)+
	max(StateCode10)+
	max(StateCode11)+
	max(StateCode12)+
	max(StateCode13)+
	max(StateCode14)+
	max(StateCode15)+
	max(StateCode16)+
	max(StateCode17)+
	max(StateCode18)+
	max(StateCode19)+
	max(StateCode20)+
	max(StateCode21)+
	max(StateCode22)+
	max(StateCode23)+
	max(StateCode24)+
	max(StateCode25)+
	max(StateCode26)+
	max(StateCode27)+
	max(StateCode28)+
	max(StateCode29)+
	max(StateCode30)+
	max(StateCode31)+
	max(StateCode32)+
	max(StateCode33)+
	max(StateCode34)+
	max(StateCode35)+
	max(StateCode36)+
	max(StateCode37)+
	max(StateCode38)+
	max(StateCode39)+
	max(StateCode40)+
	max(StateCode41)+
	max(StateCode42)+
	max(StateCode43)+
	max(StateCode44)+
	max(StateCode45)+
	max(StateCode46)+
	max(StateCode47)+
	max(StateCode48)+
	max(StateCode49)+
	max(StateCode50)+
	max(StateCode51)+
	max(StateCode52)+
	max(StateCode53)+
	max(StateCode54)+
	max(StateCode55)+
	max(StateCode56)+
	max(StateCode57)+
	max(StateCode58)+
	max(StateCode59)+
	max(StateCode60)+
	max(ReservedForFutureUse2)+
	max(PolicyChangeEffectiveDate)+
	max(PolicyChangeExpirationDate)
	,'0','')))<>''
),
EXP_DataCollect_Record06 AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPOLS_Record06
),
SQ_WCPOLS_Record07 AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse1)+
	max(EndorsementNumber01)+
	max(BureauVersionIdentifierEditionIdentifier01)+
	max(CarrierVersionIdentifier01)+
	max(EndorsementNumber02)+
	max(BureauVersionIdentifierEditionIdentifier02)+
	max(CarrierVersionIdentifier02)+
	max(EndorsementNumber03)+
	max(BureauVersionIdentifierEditionIdentifier03)+
	max(CarrierVersionIdentifier03)+
	max(EndorsementNumber04)+
	max(BureauVersionIdentifierEditionIdentifier04)+
	max(CarrierVersionIdentifier04)+
	max(EndorsementNumber05)+
	max(BureauVersionIdentifierEditionIdentifier05)+
	max(CarrierVersionIdentifier05)+
	max(EndorsementNumber06)+
	max(BureauVersionIdentifierEditionIdentifier06)+
	max(CarrierVersionIdentifier06)+
	max(EndorsementNumber07)+
	max(BureauVersionIdentifierEditionIdentifier07)+
	max(CarrierVersionIdentifier07)+
	max(EndorsementNumber08)+
	max(BureauVersionIdentifierEditionIdentifier08)+
	max(CarrierVersionIdentifier08)+
	max(EndorsementNumber09)+
	max(BureauVersionIdentifierEditionIdentifier09)+
	max(CarrierVersionIdentifier09)+
	max(EndorsementNumber10)+
	max(BureauVersionIdentifierEditionIdentifier10)+
	max(CarrierVersionIdentifier10)+
	max(EndorsementNumber11)+
	max(BureauVersionIdentifierEditionIdentifier11)+
	max(CarrierVersionIdentifier11)+
	max(ReservedForFutureUse2)+
	max(PolicyChangeEffectiveDate)+
	max(PolicyChangeExpirationDate) From (
	Select WCPols07RecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	case 
	when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	replicate(' ',3) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='EndorsementNumber01' and C.FieldName='EndorsementNumber01' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber01 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber01,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber01,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber01' and C.FieldName='EndorsementNumber01' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber01,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier01' and C.FieldName='BureauVersionIdentifierEditionIdentifier01' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier01 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier01,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier01,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier01' and C.FieldName='BureauVersionIdentifierEditionIdentifier01' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier01,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier01' and C.FieldName='CarrierVersionIdentifier01' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier01 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier01,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier01,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier01' and C.FieldName='CarrierVersionIdentifier01' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier01,
	case 
	when B.COLUMN_NAME='EndorsementNumber02' and C.FieldName='EndorsementNumber02' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber02 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber02,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber02,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber02' and C.FieldName='EndorsementNumber02' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber02,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier02' and C.FieldName='BureauVersionIdentifierEditionIdentifier02' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier02 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier02,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier02,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier02' and C.FieldName='BureauVersionIdentifierEditionIdentifier02' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier02,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier02' and C.FieldName='CarrierVersionIdentifier02' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier02 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier02,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier02,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier02' and C.FieldName='CarrierVersionIdentifier02' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier02,
	case 
	when B.COLUMN_NAME='EndorsementNumber03' and C.FieldName='EndorsementNumber03' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber03 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber03,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber03,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber03' and C.FieldName='EndorsementNumber03' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber03,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier03' and C.FieldName='BureauVersionIdentifierEditionIdentifier03' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier03 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier03,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier03,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier03' and C.FieldName='BureauVersionIdentifierEditionIdentifier03' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier03,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier03' and C.FieldName='CarrierVersionIdentifier03' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier03 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier03,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier03,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier03' and C.FieldName='CarrierVersionIdentifier03' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier03,
	case 
	when B.COLUMN_NAME='EndorsementNumber04' and C.FieldName='EndorsementNumber04' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber04 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber04,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber04,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber04' and C.FieldName='EndorsementNumber04' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber04,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier04' and C.FieldName='BureauVersionIdentifierEditionIdentifier04' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier04 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier04,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier04,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier04' and C.FieldName='BureauVersionIdentifierEditionIdentifier04' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier04,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier04' and C.FieldName='CarrierVersionIdentifier04' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier04 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier04,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier04,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier04' and C.FieldName='CarrierVersionIdentifier04' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier04,
	case 
	when B.COLUMN_NAME='EndorsementNumber05' and C.FieldName='EndorsementNumber05' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber05 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber05,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber05,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber05' and C.FieldName='EndorsementNumber05' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber05,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier05' and C.FieldName='BureauVersionIdentifierEditionIdentifier05' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier05 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier05,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier05,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier05' and C.FieldName='BureauVersionIdentifierEditionIdentifier05' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier05,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier05' and C.FieldName='CarrierVersionIdentifier05' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier05 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier05,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier05,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier05' and C.FieldName='CarrierVersionIdentifier05' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier05,
	case 
	when B.COLUMN_NAME='EndorsementNumber06' and C.FieldName='EndorsementNumber06' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber06 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber06,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber06,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber06' and C.FieldName='EndorsementNumber06' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber06,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier06' and C.FieldName='BureauVersionIdentifierEditionIdentifier06' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier06 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier06,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier06,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier06' and C.FieldName='BureauVersionIdentifierEditionIdentifier06' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier06,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier06' and C.FieldName='CarrierVersionIdentifier06' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier06 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier06,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier06,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier06' and C.FieldName='CarrierVersionIdentifier06' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier06,
	case 
	when B.COLUMN_NAME='EndorsementNumber07' and C.FieldName='EndorsementNumber07' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber07 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber07,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber07,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber07' and C.FieldName='EndorsementNumber07' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber07,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier07' and C.FieldName='BureauVersionIdentifierEditionIdentifier07' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier07 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier07,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier07,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier07' and C.FieldName='BureauVersionIdentifierEditionIdentifier07' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier07,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier07' and C.FieldName='CarrierVersionIdentifier07' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier07 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier07,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier07,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier07' and C.FieldName='CarrierVersionIdentifier07' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier07,
	case 
	when B.COLUMN_NAME='EndorsementNumber08' and C.FieldName='EndorsementNumber08' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber08 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber08,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber08,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber08' and C.FieldName='EndorsementNumber08' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber08,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier08' and C.FieldName='BureauVersionIdentifierEditionIdentifier08' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier08 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier08,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier08,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier08' and C.FieldName='BureauVersionIdentifierEditionIdentifier08' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier08,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier08' and C.FieldName='CarrierVersionIdentifier08' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier08 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier08,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier08,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier08' and C.FieldName='CarrierVersionIdentifier08' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier08,
	case 
	when B.COLUMN_NAME='EndorsementNumber09' and C.FieldName='EndorsementNumber09' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber09 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber09,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber09,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber09' and C.FieldName='EndorsementNumber09' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber09,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier09' and C.FieldName='BureauVersionIdentifierEditionIdentifier09' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier09 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier09,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier09,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier09' and C.FieldName='BureauVersionIdentifierEditionIdentifier09' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier09,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier09' and C.FieldName='CarrierVersionIdentifier09' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier09 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier09,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier09,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier09' and C.FieldName='CarrierVersionIdentifier09' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier09,
	case 
	when B.COLUMN_NAME='EndorsementNumber10' and C.FieldName='EndorsementNumber10' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber10 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber10,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber10,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber10' and C.FieldName='EndorsementNumber10' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber10,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier10' and C.FieldName='BureauVersionIdentifierEditionIdentifier10' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier10 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier10,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier10,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier10' and C.FieldName='BureauVersionIdentifierEditionIdentifier10' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier10,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier10' and C.FieldName='CarrierVersionIdentifier10' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier10 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier10,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier10,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier10' and C.FieldName='CarrierVersionIdentifier10' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier10,
	case 
	when B.COLUMN_NAME='EndorsementNumber11' and C.FieldName='EndorsementNumber11' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber11 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber11,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber11,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber11' and C.FieldName='EndorsementNumber11' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber11,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier11' and C.FieldName='BureauVersionIdentifierEditionIdentifier11' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier11 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier11,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier11,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier11' and C.FieldName='BureauVersionIdentifierEditionIdentifier11' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier11,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier11' and C.FieldName='CarrierVersionIdentifier11' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier11 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier11,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier11,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier11' and C.FieldName='CarrierVersionIdentifier11' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier11,
	replicate(' ',18) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='PolicyChangeEffectiveDate' and C.FieldName='PolicyChangeEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.PolicyChangeEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PolicyChangeEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.PolicyChangeEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='PolicyChangeEffectiveDate' and C.FieldName='PolicyChangeEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end PolicyChangeEffectiveDate,
	case 
	when B.COLUMN_NAME='PolicyChangeExpirationDate' and C.FieldName='PolicyChangeExpirationDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType and (D.WCPOLSCode=A.PolicyChangeExpirationDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PolicyChangeExpirationDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.PolicyChangeExpirationDate,''),6) end
	else 
	case when B.COLUMN_NAME='PolicyChangeExpirationDate' and C.FieldName='PolicyChangeExpirationDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end PolicyChangeExpirationDate
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols07Record A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols07Record'
	and c.TableName='WCPols07Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols07RecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse1)+
	max(EndorsementNumber01)+
	max(BureauVersionIdentifierEditionIdentifier01)+
	max(CarrierVersionIdentifier01)+
	max(EndorsementNumber02)+
	max(BureauVersionIdentifierEditionIdentifier02)+
	max(CarrierVersionIdentifier02)+
	max(EndorsementNumber03)+
	max(BureauVersionIdentifierEditionIdentifier03)+
	max(CarrierVersionIdentifier03)+
	max(EndorsementNumber04)+
	max(BureauVersionIdentifierEditionIdentifier04)+
	max(CarrierVersionIdentifier04)+
	max(EndorsementNumber05)+
	max(BureauVersionIdentifierEditionIdentifier05)+
	max(CarrierVersionIdentifier05)+
	max(EndorsementNumber06)+
	max(BureauVersionIdentifierEditionIdentifier06)+
	max(CarrierVersionIdentifier06)+
	max(EndorsementNumber07)+
	max(BureauVersionIdentifierEditionIdentifier07)+
	max(CarrierVersionIdentifier07)+
	max(EndorsementNumber08)+
	max(BureauVersionIdentifierEditionIdentifier08)+
	max(CarrierVersionIdentifier08)+
	max(EndorsementNumber09)+
	max(BureauVersionIdentifierEditionIdentifier09)+
	max(CarrierVersionIdentifier09)+
	max(EndorsementNumber10)+
	max(BureauVersionIdentifierEditionIdentifier10)+
	max(CarrierVersionIdentifier10)+
	max(EndorsementNumber11)+
	max(BureauVersionIdentifierEditionIdentifier11)+
	max(CarrierVersionIdentifier11)+
	max(ReservedForFutureUse2)+
	max(PolicyChangeEffectiveDate)+
	max(PolicyChangeExpirationDate)
	,'0','')))<>''
),
EXP_DataCollect_Record07 AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPOLS_Record07
),
SQ_WCPOLS_Record08 AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(CancellationReinstatementIDCode)+
	max(CancellationTypeCode)+
	max(ReasonForCancellationCode)+
	max(ReinstatementTypeCode)+
	max(NameOfInsured)+
	max(AddressOfInsured)+
	max(NatureOfInsured)+
	max(CancellationMailedtoInsuredDate)+
	max(CancellationReinstatementTransactionSequenceNumber)+
	max(ReasonForReinstatementCode)+
	max(ReservedForFutureUse1)+
	max(CorrespondingCancellationEffectiveDate)+
	max(CancellationReinstatementEffectiveDate)+
	max(ReservedForFutureUse2),
	max(WCTrackHistoryID) WCTrackHistoryID From (
	Select WCPols08Record@{pipeline().parameters.RECORD_08_TABLENAMEID}, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	case 
	when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	case 
	when B.COLUMN_NAME='CancellationReinstatementIDCode' and C.FieldName='CancellationReinstatementIDCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CancellationReinstatementIDCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CancellationReinstatementIDCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.CancellationReinstatementIDCode,''),1) end
	else 
	case when B.COLUMN_NAME='CancellationReinstatementIDCode' and C.FieldName='CancellationReinstatementIDCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end CancellationReinstatementIDCode,
	case 
	when B.COLUMN_NAME='CancellationTypeCode' and C.FieldName='CancellationTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CancellationTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CancellationTypeCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.CancellationTypeCode,''),1) end
	else 
	case when B.COLUMN_NAME='CancellationTypeCode' and C.FieldName='CancellationTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end CancellationTypeCode,
	case 
	when B.COLUMN_NAME='ReasonForCancellationCode' and C.FieldName='ReasonForCancellationCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ReasonForCancellationCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ReasonForCancellationCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.ReasonForCancellationCode,''),2) end
	else 
	case when B.COLUMN_NAME='ReasonForCancellationCode' and C.FieldName='ReasonForCancellationCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end ReasonForCancellationCode,
	case 
	when B.COLUMN_NAME='ReinstatementTypeCode' and C.FieldName='ReinstatementTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ReinstatementTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ReinstatementTypeCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.ReinstatementTypeCode,''),1) end
	else 
	case when B.COLUMN_NAME='ReinstatementTypeCode' and C.FieldName='ReinstatementTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end ReinstatementTypeCode,
	case 
	when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfInsured,'')+replicate(' ',90),90) else Right(replicate('0',90)+ISNULL(A.NameOfInsured,''),90) end
	else 
	case when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',90) else replicate('0',90) end
	end end NameOfInsured,
	case 
	when B.COLUMN_NAME='AddressOfInsured' and C.FieldName='AddressOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressOfInsured,'')+replicate(' ',90),90) else Right(replicate('0',90)+ISNULL(A.AddressOfInsured,''),90) end
	else 
	case when B.COLUMN_NAME='AddressOfInsured' and C.FieldName='AddressOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',90) else replicate('0',90) end
	end end AddressOfInsured,
	case 
	when B.COLUMN_NAME='NatureOfInsured' and C.FieldName='NatureOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NatureOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NatureOfInsured,'')+replicate(' ',20),20) else Right(replicate('0',20)+ISNULL(A.NatureOfInsured,''),20) end
	else 
	case when B.COLUMN_NAME='NatureOfInsured' and C.FieldName='NatureOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',20) else replicate('0',20) end
	end end NatureOfInsured,
	case 
	when B.COLUMN_NAME='CancellationMailedtoInsuredDate' and C.FieldName='CancellationMailedtoInsuredDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CancellationMailedtoInsuredDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CancellationMailedtoInsuredDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.CancellationMailedtoInsuredDate,''),6) end
	else 
	case when B.COLUMN_NAME='CancellationMailedtoInsuredDate' and C.FieldName='CancellationMailedtoInsuredDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end CancellationMailedtoInsuredDate,
	case 
	when B.COLUMN_NAME='CancellationReinstatementTransactionSequenceNumber' and C.FieldName='CancellationReinstatementTransactionSequenceNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CancellationReinstatementTransactionSequenceNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CancellationReinstatementTransactionSequenceNumber,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.CancellationReinstatementTransactionSequenceNumber,''),2) end
	else 
	case when B.COLUMN_NAME='CancellationReinstatementTransactionSequenceNumber' and C.FieldName='CancellationReinstatementTransactionSequenceNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end CancellationReinstatementTransactionSequenceNumber,
	case 
	when B.COLUMN_NAME='ReasonForReinstatementCode' and C.FieldName='ReasonForReinstatementCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ReasonForReinstatementCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ReasonForReinstatementCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.ReasonForReinstatementCode,''),2) end
	else 
	case when B.COLUMN_NAME='ReasonForReinstatementCode' and C.FieldName='ReasonForReinstatementCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end ReasonForReinstatementCode,
	replicate(' ',20) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='CorrespondingCancellationEffectiveDate' and C.FieldName='CorrespondingCancellationEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CorrespondingCancellationEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CorrespondingCancellationEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.CorrespondingCancellationEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='CorrespondingCancellationEffectiveDate' and C.FieldName='CorrespondingCancellationEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end CorrespondingCancellationEffectiveDate,
	case 
	when B.COLUMN_NAME='CancellationReinstatementEffectiveDate' and C.FieldName='CancellationReinstatementEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CancellationReinstatementEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CancellationReinstatementEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.CancellationReinstatementEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='CancellationReinstatementEffectiveDate' and C.FieldName='CancellationReinstatementEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end CancellationReinstatementEffectiveDate,
	replicate(' ',6)ReservedForFutureUse2
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols08Record@{pipeline().parameters.RECORD_08_TABLENAME} A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols08Record'
	and c.TableName='WCPols08Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols08Record@{pipeline().parameters.RECORD_08_TABLENAMEID}, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(CancellationReinstatementIDCode)+
	max(CancellationTypeCode)+
	max(ReasonForCancellationCode)+
	max(ReinstatementTypeCode)+
	max(NameOfInsured)+
	max(AddressOfInsured)+
	max(NatureOfInsured)+
	max(CancellationMailedtoInsuredDate)+
	max(CancellationReinstatementTransactionSequenceNumber)+
	max(ReasonForReinstatementCode)+
	max(ReservedForFutureUse1)+
	max(CorrespondingCancellationEffectiveDate)+
	max(CancellationReinstatementEffectiveDate)+
	max(ReservedForFutureUse2)
	,'0','')))<>''
),
EXP_DataCollect_Record08 AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data,
	WCTrackHistoryID
	FROM SQ_WCPOLS_Record08
),
SQ_WCPOLS_Record09 AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse1)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(AnniversaryRatingDate)+
	max(ReservedForFutureUse2)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse3) From (
	Select WCPols09RecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	case 
	when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	replicate(' ',3) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier,
	case 
	when B.COLUMN_NAME='AnniversaryRatingDate' and C.FieldName='AnniversaryRatingDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AnniversaryRatingDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AnniversaryRatingDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.AnniversaryRatingDate,''),6) end
	else 
	case when B.COLUMN_NAME='AnniversaryRatingDate' and C.FieldName='AnniversaryRatingDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end AnniversaryRatingDate,
	replicate(' ',178) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfInsured,'')+replicate(' ',34),34) else Right(replicate('0',34)+ISNULL(A.NameOfInsured,''),34) end
	else 
	case when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',34) else replicate('0',34) end
	end end NameOfInsured,
	case 
	when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.EndorsementEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end EndorsementEffectiveDate,
	replicate(' ',6)ReservedForFutureUse3
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols09Record A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols09Record'
	and c.TableName='WCPols09Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols09RecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse1)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(AnniversaryRatingDate)+
	max(ReservedForFutureUse2)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse3)
	,'0','')))<>''
),
EXP_DataCollect_Record09 AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPOLS_Record09
),
SQ_WCPols15Record AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse1)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(RetrospectivePremiumOptionCode)+
	max(LossLimitationAmount)+
	max(LossConversionFactor)+
	max(HazardGroupCode)+
	max(ReservedForFutureUse2)+
	max(TaxMultiplierFactorStateOtherthanFClasses)+
	max(TaxMultiplierFactorFederalFClassesOnly)+
	max(TaxMultiplierFactorWeightedAverageTaxMultiplierFactor)+
	max(RetrospectiveDevelopmentFactorFirstFactor)+
	max(RetrospectiveDevelopmentFactorSecondFactor)+
	max(RetrospectiveDevelopmentFactorThirdFactor)+
	max(ReservedForFutureUse3)+
	max(MinimumRetrospectivePremiumFactor)+
	max(MaximumRetrospectivePremiumFactor)+
	max(BasicPremiumFactor50pct)+
	max(BasicPremiumFactor100pct)+
	max(BasicPremiumFactor150pct)+
	max(EstimatedStandardPremiumAmount50pct)+
	max(EstimatedStandardPremiumAmount100pct)+
	max(EstimatedStandardPremiumAmount150pct)+
	max(ExcessLossFactorStateOtherthanFClasses)+
	max(ExcessLossFactorFederalFClassesOnly)+
	max(ReservedForFutureUse4)+
	max(RetrospectiveRatingPlanEffectiveDate)+
	max(OtherPolicyNumberIdentifier)+
	max(AddendumFormNumber)+
	max(ReservedForFutureUse5)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse6) From (
	Select WCPols15RecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	case 
	when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	replicate(' ',3) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier,
	case 
	when B.COLUMN_NAME='RetrospectivePremiumOptionCode' and C.FieldName='RetrospectivePremiumOptionCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RetrospectivePremiumOptionCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RetrospectivePremiumOptionCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.RetrospectivePremiumOptionCode,''),1) end
	else 
	case when B.COLUMN_NAME='RetrospectivePremiumOptionCode' and C.FieldName='RetrospectivePremiumOptionCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end RetrospectivePremiumOptionCode,
	case 
	when B.COLUMN_NAME='LossLimitationAmount' and C.FieldName='LossLimitationAmount' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LossLimitationAmount or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LossLimitationAmount,'')+replicate(' ',7),7) else Right(replicate('0',7)+ISNULL(A.LossLimitationAmount,''),7) end
	else 
	case when B.COLUMN_NAME='LossLimitationAmount' and C.FieldName='LossLimitationAmount' then case when C.FieldDataType in ('A','AN') then replicate(' ',7) else replicate('0',7) end
	end end LossLimitationAmount,
	case 
	when B.COLUMN_NAME='LossConversionFactor' and C.FieldName='LossConversionFactor' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LossConversionFactor or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LossConversionFactor,'')+replicate(' ',5),5) else Right(replicate('0',5)+ISNULL(A.LossConversionFactor,''),5) end
	else 
	case when B.COLUMN_NAME='LossConversionFactor' and C.FieldName='LossConversionFactor' then case when C.FieldDataType in ('A','AN') then replicate(' ',5) else replicate('0',5) end
	end end LossConversionFactor,
	case 
	when B.COLUMN_NAME='HazardGroupCode' and C.FieldName='HazardGroupCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.HazardGroupCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.HazardGroupCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.HazardGroupCode,''),1) end
	else 
	case when B.COLUMN_NAME='HazardGroupCode' and C.FieldName='HazardGroupCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end HazardGroupCode,
	replicate(' ',24) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='TaxMultiplierFactorStateOtherthanFClasses' and C.FieldName='TaxMultiplierFactorStateOtherthanFClasses' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.TaxMultiplierFactorStateOtherthanFClasses or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.TaxMultiplierFactorStateOtherthanFClasses,'')+replicate(' ',5),5) else Right(replicate('0',5)+ISNULL(A.TaxMultiplierFactorStateOtherthanFClasses,''),5) end
	else 
	case when B.COLUMN_NAME='TaxMultiplierFactorStateOtherthanFClasses' and C.FieldName='TaxMultiplierFactorStateOtherthanFClasses' then case when C.FieldDataType in ('A','AN') then replicate(' ',5) else replicate('0',5) end
	end end TaxMultiplierFactorStateOtherthanFClasses,
	case 
	when B.COLUMN_NAME='TaxMultiplierFactorFederalFClassesOnly' and C.FieldName='TaxMultiplierFactorFederalFClassesOnly' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.TaxMultiplierFactorFederalFClassesOnly or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.TaxMultiplierFactorFederalFClassesOnly,'')+replicate(' ',7),7) else Right(replicate('0',7)+ISNULL(A.TaxMultiplierFactorFederalFClassesOnly,''),7) end
	else 
	case when B.COLUMN_NAME='TaxMultiplierFactorFederalFClassesOnly' and C.FieldName='TaxMultiplierFactorFederalFClassesOnly' then case when C.FieldDataType in ('A','AN') then replicate(' ',7) else replicate('0',7) end
	end end TaxMultiplierFactorFederalFClassesOnly,
	case 
	when B.COLUMN_NAME='TaxMultiplierFactorWeightedAverageTaxMultiplierFactor' and C.FieldName='TaxMultiplierFactorWeightedAverageTaxMultiplierFactor' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.TaxMultiplierFactorWeightedAverageTaxMultiplierFactor or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.TaxMultiplierFactorWeightedAverageTaxMultiplierFactor,'')+replicate(' ',5),5) else Right(replicate('0',5)+ISNULL(A.TaxMultiplierFactorWeightedAverageTaxMultiplierFactor,''),5) end
	else 
	case when B.COLUMN_NAME='TaxMultiplierFactorWeightedAverageTaxMultiplierFactor' and C.FieldName='TaxMultiplierFactorWeightedAverageTaxMultiplierFactor' then case when C.FieldDataType in ('A','AN') then replicate(' ',5) else replicate('0',5) end
	end end TaxMultiplierFactorWeightedAverageTaxMultiplierFactor,
	case 
	when B.COLUMN_NAME='RetrospectiveDevelopmentFactorFirstFactor' and C.FieldName='RetrospectiveDevelopmentFactorFirstFactor' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RetrospectiveDevelopmentFactorFirstFactor or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RetrospectiveDevelopmentFactorFirstFactor,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RetrospectiveDevelopmentFactorFirstFactor,''),2) end
	else 
	case when B.COLUMN_NAME='RetrospectiveDevelopmentFactorFirstFactor' and C.FieldName='RetrospectiveDevelopmentFactorFirstFactor' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RetrospectiveDevelopmentFactorFirstFactor,
	case 
	when B.COLUMN_NAME='RetrospectiveDevelopmentFactorSecondFactor' and C.FieldName='RetrospectiveDevelopmentFactorSecondFactor' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RetrospectiveDevelopmentFactorSecondFactor or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RetrospectiveDevelopmentFactorSecondFactor,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RetrospectiveDevelopmentFactorSecondFactor,''),2) end
	else 
	case when B.COLUMN_NAME='RetrospectiveDevelopmentFactorSecondFactor' and C.FieldName='RetrospectiveDevelopmentFactorSecondFactor' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RetrospectiveDevelopmentFactorSecondFactor,
	case 
	when B.COLUMN_NAME='RetrospectiveDevelopmentFactorThirdFactor' and C.FieldName='RetrospectiveDevelopmentFactorThirdFactor' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RetrospectiveDevelopmentFactorThirdFactor or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RetrospectiveDevelopmentFactorThirdFactor,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RetrospectiveDevelopmentFactorThirdFactor,''),2) end
	else 
	case when B.COLUMN_NAME='RetrospectiveDevelopmentFactorThirdFactor' and C.FieldName='RetrospectiveDevelopmentFactorThirdFactor' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RetrospectiveDevelopmentFactorThirdFactor,
	replicate(' ',3) ReservedForFutureUse3,
	case 
	when B.COLUMN_NAME='MinimumRetrospectivePremiumFactor' and C.FieldName='MinimumRetrospectivePremiumFactor' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.MinimumRetrospectivePremiumFactor or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.MinimumRetrospectivePremiumFactor,'')+replicate(' ',15),15) else Right(replicate('0',15)+ISNULL(A.MinimumRetrospectivePremiumFactor,''),15) end
	else 
	case when B.COLUMN_NAME='MinimumRetrospectivePremiumFactor' and C.FieldName='MinimumRetrospectivePremiumFactor' then case when C.FieldDataType in ('A','AN') then replicate(' ',15) else replicate('0',15) end
	end end MinimumRetrospectivePremiumFactor,
	case 
	when B.COLUMN_NAME='MaximumRetrospectivePremiumFactor' and C.FieldName='MaximumRetrospectivePremiumFactor' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.MaximumRetrospectivePremiumFactor or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.MaximumRetrospectivePremiumFactor,'')+replicate(' ',15),15) else Right(replicate('0',15)+ISNULL(A.MaximumRetrospectivePremiumFactor,''),15) end
	else 
	case when B.COLUMN_NAME='MaximumRetrospectivePremiumFactor' and C.FieldName='MaximumRetrospectivePremiumFactor' then case when C.FieldDataType in ('A','AN') then replicate(' ',15) else replicate('0',15) end
	end end MaximumRetrospectivePremiumFactor,
	case 
	when B.COLUMN_NAME='BasicPremiumFactor50pct' and C.FieldName='BasicPremiumFactor50pct' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BasicPremiumFactor50pct or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BasicPremiumFactor50pct,'')+replicate(' ',5),5) else Right(replicate('0',5)+ISNULL(A.BasicPremiumFactor50pct,''),5) end
	else 
	case when B.COLUMN_NAME='BasicPremiumFactor50pct' and C.FieldName='BasicPremiumFactor50pct' then case when C.FieldDataType in ('A','AN') then replicate(' ',5) else replicate('0',5) end
	end end BasicPremiumFactor50pct,
	case 
	when B.COLUMN_NAME='BasicPremiumFactor100pct' and C.FieldName='BasicPremiumFactor100pct' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BasicPremiumFactor100pct or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BasicPremiumFactor100pct,'')+replicate(' ',5),5) else Right(replicate('0',5)+ISNULL(A.BasicPremiumFactor100pct,''),5) end
	else 
	case when B.COLUMN_NAME='BasicPremiumFactor100pct' and C.FieldName='BasicPremiumFactor100pct' then case when C.FieldDataType in ('A','AN') then replicate(' ',5) else replicate('0',5) end
	end end BasicPremiumFactor100pct,
	case 
	when B.COLUMN_NAME='BasicPremiumFactor150pct' and C.FieldName='BasicPremiumFactor150pct' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BasicPremiumFactor150pct or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BasicPremiumFactor150pct,'')+replicate(' ',5),5) else Right(replicate('0',5)+ISNULL(A.BasicPremiumFactor150pct,''),5) end
	else 
	case when B.COLUMN_NAME='BasicPremiumFactor150pct' and C.FieldName='BasicPremiumFactor150pct' then case when C.FieldDataType in ('A','AN') then replicate(' ',5) else replicate('0',5) end
	end end BasicPremiumFactor150pct,
	case 
	when B.COLUMN_NAME='EstimatedStandardPremiumAmount50pct' and C.FieldName='EstimatedStandardPremiumAmount50pct' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EstimatedStandardPremiumAmount50pct or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EstimatedStandardPremiumAmount50pct,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.EstimatedStandardPremiumAmount50pct,''),10) end
	else 
	case when B.COLUMN_NAME='EstimatedStandardPremiumAmount50pct' and C.FieldName='EstimatedStandardPremiumAmount50pct' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end EstimatedStandardPremiumAmount50pct,
	case 
	when B.COLUMN_NAME='EstimatedStandardPremiumAmount100pct' and C.FieldName='EstimatedStandardPremiumAmount100pct' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EstimatedStandardPremiumAmount100pct or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EstimatedStandardPremiumAmount100pct,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.EstimatedStandardPremiumAmount100pct,''),10) end
	else 
	case when B.COLUMN_NAME='EstimatedStandardPremiumAmount100pct' and C.FieldName='EstimatedStandardPremiumAmount100pct' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end EstimatedStandardPremiumAmount100pct,
	case 
	when B.COLUMN_NAME='EstimatedStandardPremiumAmount150pct' and C.FieldName='EstimatedStandardPremiumAmount150pct' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EstimatedStandardPremiumAmount150pct or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EstimatedStandardPremiumAmount150pct,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.EstimatedStandardPremiumAmount150pct,''),10) end
	else 
	case when B.COLUMN_NAME='EstimatedStandardPremiumAmount150pct' and C.FieldName='EstimatedStandardPremiumAmount150pct' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end EstimatedStandardPremiumAmount150pct,
	case 
	when B.COLUMN_NAME='ExcessLossFactorStateOtherthanFClasses' and C.FieldName='ExcessLossFactorStateOtherthanFClasses' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ExcessLossFactorStateOtherthanFClasses or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ExcessLossFactorStateOtherthanFClasses,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.ExcessLossFactorStateOtherthanFClasses,''),3) end
	else 
	case when B.COLUMN_NAME='ExcessLossFactorStateOtherthanFClasses' and C.FieldName='ExcessLossFactorStateOtherthanFClasses' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end ExcessLossFactorStateOtherthanFClasses,
	case 
	when B.COLUMN_NAME='ExcessLossFactorFederalFClassesOnly' and C.FieldName='ExcessLossFactorFederalFClassesOnly' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ExcessLossFactorFederalFClassesOnly or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ExcessLossFactorFederalFClassesOnly,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.ExcessLossFactorFederalFClassesOnly,''),3) end
	else 
	case when B.COLUMN_NAME='ExcessLossFactorFederalFClassesOnly' and C.FieldName='ExcessLossFactorFederalFClassesOnly' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end ExcessLossFactorFederalFClassesOnly,
	replicate(' ',3) ReservedForFutureUse4,
	case 
	when B.COLUMN_NAME='RetrospectiveRatingPlanEffectiveDate' and C.FieldName='RetrospectiveRatingPlanEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RetrospectiveRatingPlanEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RetrospectiveRatingPlanEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.RetrospectiveRatingPlanEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='RetrospectiveRatingPlanEffectiveDate' and C.FieldName='RetrospectiveRatingPlanEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end RetrospectiveRatingPlanEffectiveDate,
	case 
	when B.COLUMN_NAME='OtherPolicyNumberIdentifier' and C.FieldName='OtherPolicyNumberIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.OtherPolicyNumberIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.OtherPolicyNumberIdentifier,'')+replicate(' ',18),18) else Right(replicate('0',18)+ISNULL(A.OtherPolicyNumberIdentifier,''),18) end
	else 
	case when B.COLUMN_NAME='OtherPolicyNumberIdentifier' and C.FieldName='OtherPolicyNumberIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',18) else replicate('0',18) end
	end end OtherPolicyNumberIdentifier,
	case 
	when B.COLUMN_NAME='AddendumFormNumber' and C.FieldName='AddendumFormNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddendumFormNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddendumFormNumber,'')+replicate(' ',12),12) else Right(replicate('0',12)+ISNULL(A.AddendumFormNumber,''),12) end
	else 
	case when B.COLUMN_NAME='AddendumFormNumber' and C.FieldName='AddendumFormNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',12) else replicate('0',12) end
	end end AddendumFormNumber,
	replicate(' ',2) ReservedForFutureUse5,
	case 
	when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfInsured,'')+replicate(' ',34),34) else Right(replicate('0',34)+ISNULL(A.NameOfInsured,''),34) end
	else 
	case when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',34) else replicate('0',34) end
	end end NameOfInsured,
	case 
	when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.EndorsementEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end EndorsementEffectiveDate,
	replicate(' ',6)ReservedForFutureUse6
	
	from WCPols15Record A
	inner join WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols15Record'
	and c.TableName='WCPols15Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols15RecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse1)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(RetrospectivePremiumOptionCode)+
	max(LossLimitationAmount)+
	max(LossConversionFactor)+
	max(HazardGroupCode)+
	max(ReservedForFutureUse2)+
	max(TaxMultiplierFactorStateOtherthanFClasses)+
	max(TaxMultiplierFactorFederalFClassesOnly)+
	max(TaxMultiplierFactorWeightedAverageTaxMultiplierFactor)+
	max(RetrospectiveDevelopmentFactorFirstFactor)+
	max(RetrospectiveDevelopmentFactorSecondFactor)+
	max(RetrospectiveDevelopmentFactorThirdFactor)+
	max(ReservedForFutureUse3)+
	max(MinimumRetrospectivePremiumFactor)+
	max(MaximumRetrospectivePremiumFactor)+
	max(BasicPremiumFactor50pct)+
	max(BasicPremiumFactor100pct)+
	max(BasicPremiumFactor150pct)+
	max(EstimatedStandardPremiumAmount50pct)+
	max(EstimatedStandardPremiumAmount100pct)+
	max(EstimatedStandardPremiumAmount150pct)+
	max(ExcessLossFactorStateOtherthanFClasses)+
	max(ExcessLossFactorFederalFClassesOnly)+
	max(ReservedForFutureUse4)+
	max(RetrospectiveRatingPlanEffectiveDate)+
	max(OtherPolicyNumberIdentifier)+
	max(AddendumFormNumber)+
	max(ReservedForFutureUse5)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse6)
	,'0','')))<>''
),
EXP_DataCollect_Record15 AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPols15Record
),
SQ_WCPols18Record AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(EmployerLiabilityLimitAmountFederalBodilyInjuryByAccidentAmount)+
	max(EmployerLiabilityLimitAmountFederalBodilyInjuryByDiseaseAmount)+
	max(ScheduleStateCode)+
	max(ReservedForFutureUse3)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4) From (
	Select WCPols18RecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	replicate(' ',2) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	replicate(' ',3) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier,
	case 
	when B.COLUMN_NAME='EmployerLiabilityLimitAmountFederalBodilyInjuryByAccidentAmount' and C.FieldName='EmployerLiabilityLimitAmountFederalBodilyInjuryByAccidentAmount' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EmployerLiabilityLimitAmountFederalBodilyInjuryByAccidentAmount or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EmployerLiabilityLimitAmountFederalBodilyInjuryByAccidentAmount,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.EmployerLiabilityLimitAmountFederalBodilyInjuryByAccidentAmount,''),10) end
	else 
	case when B.COLUMN_NAME='EmployerLiabilityLimitAmountFederalBodilyInjuryByAccidentAmount' and C.FieldName='EmployerLiabilityLimitAmountFederalBodilyInjuryByAccidentAmount' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end EmployerLiabilityLimitAmountFederalBodilyInjuryByAccidentAmount,
	case 
	when B.COLUMN_NAME='EmployerLiabilityLimitAmountFederalBodilyInjuryByDiseaseAmount' and C.FieldName='EmployerLiabilityLimitAmountFederalBodilyInjuryByDiseaseAmount' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EmployerLiabilityLimitAmountFederalBodilyInjuryByDiseaseAmount or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EmployerLiabilityLimitAmountFederalBodilyInjuryByDiseaseAmount,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.EmployerLiabilityLimitAmountFederalBodilyInjuryByDiseaseAmount,''),10) end
	else 
	case when B.COLUMN_NAME='EmployerLiabilityLimitAmountFederalBodilyInjuryByDiseaseAmount' and C.FieldName='EmployerLiabilityLimitAmountFederalBodilyInjuryByDiseaseAmount' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end EmployerLiabilityLimitAmountFederalBodilyInjuryByDiseaseAmount,
	case 
	when B.COLUMN_NAME='ScheduleStateCode' and C.FieldName='ScheduleStateCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ScheduleStateCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ScheduleStateCode,'')+replicate(' ',100),100) else Right(replicate('0',100)+ISNULL(A.ScheduleStateCode,''),100) end
	else 
	case when B.COLUMN_NAME='ScheduleStateCode' and C.FieldName='ScheduleStateCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',100) else replicate('0',100) end
	end end ScheduleStateCode,
	replicate(' ',64) ReservedForFutureUse3,
	case 
	when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfInsured,'')+replicate(' ',34),34) else Right(replicate('0',34)+ISNULL(A.NameOfInsured,''),34) end
	else 
	case when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',34) else replicate('0',34) end
	end end NameOfInsured,
	case 
	when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.EndorsementEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end EndorsementEffectiveDate,
	replicate(' ',6)ReservedForFutureUse4
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols18Record A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols18Record'
	and c.TableName='WCPols18Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols18RecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(EmployerLiabilityLimitAmountFederalBodilyInjuryByAccidentAmount)+
	max(EmployerLiabilityLimitAmountFederalBodilyInjuryByDiseaseAmount)+
	max(ScheduleStateCode)+
	max(ReservedForFutureUse3)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4)
	,'0','')))<>''
),
EXP_DataCollect_Record18 AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPols18Record
),
SQ_WCPols19Record AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(StateCode)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor)+
	max(StateCode2)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor2)+
	max(StateCode3)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor3)+
	max(StateCode4)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor4)+
	max(StateCode5)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor5)+
	max(StateCode6)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor6)+
	max(StateCode7)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor7)+
	max(StateCode8)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor8)+
	max(StateCode9)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor9)+
	max(StateCode10)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor10)+
	max(StateCode11)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor11)+
	max(StateCode12)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor12)+
	max(StateCode13)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor13)+
	max(StateCode14)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor14)+
	max(StateCode15)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor15)+
	max(StateCode16)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor16)+
	max(StateCode17)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor17)+
	max(StateCode18)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor18)+
	max(StateCode19)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor19)+
	max(StateCode20)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor20)+
	max(StateCode21)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor21)+
	max(StateCode22)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor22)+
	max(StateCode23)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor23)+
	max(StateCode24)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor24)+
	max(StateCode25)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor25)+
	max(ReservedForFutureUse3)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4) From (
	Select WCPols19RecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	replicate(' ',2) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	replicate(' ',3) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier,
	case 
	when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode,
	case 
	when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor,''),4) end
	else 
	case when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor,
	case 
	when B.COLUMN_NAME='StateCode2' and C.FieldName='StateCode2' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode2 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode2,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode2,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode2' and C.FieldName='StateCode2' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode2,
	case 
	when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor2' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor2' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor2 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor2,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor2,''),4) end
	else 
	case when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor2' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor2' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor2,
	case 
	when B.COLUMN_NAME='StateCode3' and C.FieldName='StateCode3' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode3 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode3,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode3,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode3' and C.FieldName='StateCode3' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode3,
	case 
	when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor3' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor3' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor3 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor3,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor3,''),4) end
	else 
	case when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor3' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor3' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor3,
	case 
	when B.COLUMN_NAME='StateCode4' and C.FieldName='StateCode4' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode4 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode4,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode4,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode4' and C.FieldName='StateCode4' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode4,
	case 
	when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor4' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor4' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor4 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor4,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor4,''),4) end
	else 
	case when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor4' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor4' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor4,
	case 
	when B.COLUMN_NAME='StateCode5' and C.FieldName='StateCode5' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode5 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode5,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode5,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode5' and C.FieldName='StateCode5' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode5,
	case 
	when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor5' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor5' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor5 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor5,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor5,''),4) end
	else 
	case when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor5' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor5' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor5,
	case 
	when B.COLUMN_NAME='StateCode6' and C.FieldName='StateCode6' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode6 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode6,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode6,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode6' and C.FieldName='StateCode6' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode6,
	case 
	when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor6' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor6' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor6 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor6,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor6,''),4) end
	else 
	case when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor6' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor6' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor6,
	case 
	when B.COLUMN_NAME='StateCode7' and C.FieldName='StateCode7' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode7 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode7,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode7,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode7' and C.FieldName='StateCode7' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode7,
	case 
	when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor7' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor7' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor7 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor7,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor7,''),4) end
	else 
	case when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor7' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor7' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor7,
	case 
	when B.COLUMN_NAME='StateCode8' and C.FieldName='StateCode8' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode8 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode8,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode8,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode8' and C.FieldName='StateCode8' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode8,
	case 
	when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor8' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor8' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor8 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor8,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor8,''),4) end
	else 
	case when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor8' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor8' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor8,
	case 
	when B.COLUMN_NAME='StateCode9' and C.FieldName='StateCode9' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode9 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode9,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode9,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode9' and C.FieldName='StateCode9' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode9,
	case 
	when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor9' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor9' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor9 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor9,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor9,''),4) end
	else 
	case when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor9' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor9' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor9,
	case 
	when B.COLUMN_NAME='StateCode10' and C.FieldName='StateCode10' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode10 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode10,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode10,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode10' and C.FieldName='StateCode10' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode10,
	case 
	when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor10' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor10' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor10 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor10,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor10,''),4) end
	else 
	case when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor10' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor10' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor10,
	case 
	when B.COLUMN_NAME='StateCode11' and C.FieldName='StateCode11' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode11 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode11,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode11,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode11' and C.FieldName='StateCode11' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode11,
	case 
	when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor11' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor11' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor11 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor11,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor11,''),4) end
	else 
	case when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor11' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor11' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor11,
	case 
	when B.COLUMN_NAME='StateCode12' and C.FieldName='StateCode12' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode12 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode12,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode12,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode12' and C.FieldName='StateCode12' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode12,
	case 
	when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor12' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor12' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor12 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor12,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor12,''),4) end
	else 
	case when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor12' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor12' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor12,
	case 
	when B.COLUMN_NAME='StateCode13' and C.FieldName='StateCode13' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode13 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode13,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode13,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode13' and C.FieldName='StateCode13' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode13,
	case 
	when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor13' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor13' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor13 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor13,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor13,''),4) end
	else 
	case when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor13' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor13' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor13,
	case 
	when B.COLUMN_NAME='StateCode14' and C.FieldName='StateCode14' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode14 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode14,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode14,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode14' and C.FieldName='StateCode14' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode14,
	case 
	when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor14' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor14' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor14 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor14,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor14,''),4) end
	else 
	case when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor14' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor14' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor14,
	case 
	when B.COLUMN_NAME='StateCode15' and C.FieldName='StateCode15' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode15 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode15,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode15,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode15' and C.FieldName='StateCode15' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode15,
	case 
	when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor15' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor15' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor15 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor15,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor15,''),4) end
	else 
	case when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor15' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor15' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor15,
	case 
	when B.COLUMN_NAME='StateCode16' and C.FieldName='StateCode16' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode16 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode16,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode16,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode16' and C.FieldName='StateCode16' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode16,
	case 
	when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor16' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor16' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor16 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor16,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor16,''),4) end
	else 
	case when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor16' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor16' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor16,
	case 
	when B.COLUMN_NAME='StateCode17' and C.FieldName='StateCode17' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode17 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode17,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode17,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode17' and C.FieldName='StateCode17' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode17,
	case 
	when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor17' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor17' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor17 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor17,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor17,''),4) end
	else 
	case when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor17' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor17' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor17,
	case 
	when B.COLUMN_NAME='StateCode18' and C.FieldName='StateCode18' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode18 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode18,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode18,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode18' and C.FieldName='StateCode18' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode18,
	case 
	when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor18' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor18' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor18 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor18,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor18,''),4) end
	else 
	case when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor18' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor18' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor18,
	case 
	when B.COLUMN_NAME='StateCode19' and C.FieldName='StateCode19' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode19 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode19,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode19,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode19' and C.FieldName='StateCode19' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode19,
	case 
	when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor19' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor19' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor19 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor19,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor19,''),4) end
	else 
	case when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor19' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor19' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor19,
	case 
	when B.COLUMN_NAME='StateCode20' and C.FieldName='StateCode20' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode20 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode20,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode20,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode20' and C.FieldName='StateCode20' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode20,
	case 
	when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor20' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor20' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor20 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor20,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor20,''),4) end
	else 
	case when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor20' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor20' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor20,
	case 
	when B.COLUMN_NAME='StateCode21' and C.FieldName='StateCode21' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode21 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode21,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode21,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode21' and C.FieldName='StateCode21' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode21,
	case 
	when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor21' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor21' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor21 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor21,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor21,''),4) end
	else 
	case when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor21' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor21' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor21,
	case 
	when B.COLUMN_NAME='StateCode22' and C.FieldName='StateCode22' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode22 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode22,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode22,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode22' and C.FieldName='StateCode22' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode22,
	case 
	when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor22' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor22' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor22 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor22,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor22,''),4) end
	else 
	case when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor22' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor22' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor22,
	case 
	when B.COLUMN_NAME='StateCode23' and C.FieldName='StateCode23' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode23 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode23,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode23,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode23' and C.FieldName='StateCode23' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode23,
	case 
	when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor23' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor23' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor23 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor23,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor23,''),4) end
	else 
	case when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor23' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor23' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor23,
	case 
	when B.COLUMN_NAME='StateCode24' and C.FieldName='StateCode24' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode24 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode24,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode24,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode24' and C.FieldName='StateCode24' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode24,
	case 
	when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor24' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor24' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor24 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor24,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor24,''),4) end
	else 
	case when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor24' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor24' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor24,
	case 
	when B.COLUMN_NAME='StateCode25' and C.FieldName='StateCode25' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode25 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode25,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode25,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode25' and C.FieldName='StateCode25' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode25,
	case 
	when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor25' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor25' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor25 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor25,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor25,''),4) end
	else 
	case when B.COLUMN_NAME='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor25' and C.FieldName='UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor25' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor25,
	replicate(' ',34) ReservedForFutureUse3,
	case 
	when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfInsured,'')+replicate(' ',34),34) else Right(replicate('0',34)+ISNULL(A.NameOfInsured,''),34) end
	else 
	case when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',34) else replicate('0',34) end
	end end NameOfInsured,
	case 
	when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.EndorsementEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end EndorsementEffectiveDate,
	replicate(' ',6)ReservedForFutureUse4
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols19Record A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols19Record'
	and c.TableName='WCPols19Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols19RecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(StateCode)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor)+
	max(StateCode2)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor2)+
	max(StateCode3)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor3)+
	max(StateCode4)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor4)+
	max(StateCode5)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor5)+
	max(StateCode6)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor6)+
	max(StateCode7)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor7)+
	max(StateCode8)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor8)+
	max(StateCode9)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor9)+
	max(StateCode10)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor10)+
	max(StateCode11)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor11)+
	max(StateCode12)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor12)+
	max(StateCode13)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor13)+
	max(StateCode14)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor14)+
	max(StateCode15)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor15)+
	max(StateCode16)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor16)+
	max(StateCode17)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor17)+
	max(StateCode18)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor18)+
	max(StateCode19)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor19)+
	max(StateCode20)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor20)+
	max(StateCode21)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor21)+
	max(StateCode22)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor22)+
	max(StateCode23)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor23)+
	max(StateCode24)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor24)+
	max(StateCode25)+
	max(UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor25)+
	max(ReservedForFutureUse3)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4)
	,'0','')))<>''
),
EXP_DataCollect_Record19 AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPols19Record
),
SQ_WCPOLS_Record21 AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(EmployerLiabilityLimitAmountMaritimeBodilyInjuryByAccidentAmount)+
	max(EmployerLiabilityLimitAmountMaritimeBodilyInjuryByDiseaseAmount)+
	max(TransportationWagesMaintenanceAndCurePremiumAmount)+
	max(WorkDescription)+
	max(ReservedForFutureUse3)+
	max(EndorsementSequenceNumber)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4) From (
	Select WCPols21RecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	replicate(' ',2) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	replicate(' ',3) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier,
	case 
	when B.COLUMN_NAME='EmployerLiabilityLimitAmountMaritimeBodilyInjuryByAccidentAmount' and C.FieldName='EmployerLiabilityLimitAmountMaritimeBodilyInjuryByAccidentAmount' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EmployerLiabilityLimitAmountMaritimeBodilyInjuryByAccidentAmount or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EmployerLiabilityLimitAmountMaritimeBodilyInjuryByAccidentAmount,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.EmployerLiabilityLimitAmountMaritimeBodilyInjuryByAccidentAmount,''),10) end
	else 
	case when B.COLUMN_NAME='EmployerLiabilityLimitAmountMaritimeBodilyInjuryByAccidentAmount' and C.FieldName='EmployerLiabilityLimitAmountMaritimeBodilyInjuryByAccidentAmount' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end EmployerLiabilityLimitAmountMaritimeBodilyInjuryByAccidentAmount,
	case 
	when B.COLUMN_NAME='EmployerLiabilityLimitAmountMaritimeBodilyInjuryByDiseaseAmount' and C.FieldName='EmployerLiabilityLimitAmountMaritimeBodilyInjuryByDiseaseAmount' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EmployerLiabilityLimitAmountMaritimeBodilyInjuryByDiseaseAmount or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EmployerLiabilityLimitAmountMaritimeBodilyInjuryByDiseaseAmount,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.EmployerLiabilityLimitAmountMaritimeBodilyInjuryByDiseaseAmount,''),10) end
	else 
	case when B.COLUMN_NAME='EmployerLiabilityLimitAmountMaritimeBodilyInjuryByDiseaseAmount' and C.FieldName='EmployerLiabilityLimitAmountMaritimeBodilyInjuryByDiseaseAmount' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end EmployerLiabilityLimitAmountMaritimeBodilyInjuryByDiseaseAmount,
	case 
	when B.COLUMN_NAME='TransportationWagesMaintenanceAndCurePremiumAmount' and C.FieldName='TransportationWagesMaintenanceAndCurePremiumAmount' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.TransportationWagesMaintenanceAndCurePremiumAmount or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.TransportationWagesMaintenanceAndCurePremiumAmount,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.TransportationWagesMaintenanceAndCurePremiumAmount,''),10) end
	else 
	case when B.COLUMN_NAME='TransportationWagesMaintenanceAndCurePremiumAmount' and C.FieldName='TransportationWagesMaintenanceAndCurePremiumAmount' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end TransportationWagesMaintenanceAndCurePremiumAmount,
	case 
	when B.COLUMN_NAME='WorkDescription' and C.FieldName='WorkDescription' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.WorkDescription or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.WorkDescription,'')+replicate(' ',120),120) else Right(replicate('0',120)+ISNULL(A.WorkDescription,''),120) end
	else 
	case when B.COLUMN_NAME='WorkDescription' and C.FieldName='WorkDescription' then case when C.FieldDataType in ('A','AN') then replicate(' ',120) else replicate('0',120) end
	end end WorkDescription,
	replicate(' ',32) ReservedForFutureUse3,
	case 
	when B.COLUMN_NAME='EndorsementSequenceNumber' and C.FieldName='EndorsementSequenceNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementSequenceNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementSequenceNumber,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.EndorsementSequenceNumber,''),2) end
	else 
	case when B.COLUMN_NAME='EndorsementSequenceNumber' and C.FieldName='EndorsementSequenceNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end EndorsementSequenceNumber,
	case 
	when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfInsured,'')+replicate(' ',34),34) else Right(replicate('0',34)+ISNULL(A.NameOfInsured,''),34) end
	else 
	case when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',34) else replicate('0',34) end
	end end NameOfInsured,
	case 
	when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.EndorsementEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end EndorsementEffectiveDate,
	replicate(' ',6)ReservedForFutureUse4
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols21Record A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols21Record'
	and c.TableName='WCPols21Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols21RecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(EmployerLiabilityLimitAmountMaritimeBodilyInjuryByAccidentAmount)+
	max(EmployerLiabilityLimitAmountMaritimeBodilyInjuryByDiseaseAmount)+
	max(TransportationWagesMaintenanceAndCurePremiumAmount)+
	max(WorkDescription)+
	max(ReservedForFutureUse3)+
	max(EndorsementSequenceNumber)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4)
	,'0','')))<>''
),
EXP_DataCollect_Record21 AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPOLS_Record21
),
SQ_WCPols23Record AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(NameOfVessel)+
	max(WorkersCompensationLaw)+
	max(DescriptionOfWork)+
	max(ReservedForFutureUse3)+
	max(EndorsementSequenceNumber)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4) From (
	Select WCPols23RecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	replicate(' ',2) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	replicate(' ',3) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier,
	case 
	when B.COLUMN_NAME='NameOfVessel' and C.FieldName='NameOfVessel' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfVessel or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfVessel,'')+replicate(' ',60),60) else Right(replicate('0',60)+ISNULL(A.NameOfVessel,''),60) end
	else 
	case when B.COLUMN_NAME='NameOfVessel' and C.FieldName='NameOfVessel' then case when C.FieldDataType in ('A','AN') then replicate(' ',60) else replicate('0',60) end
	end end NameOfVessel,
	case 
	when B.COLUMN_NAME='WorkersCompensationLaw' and C.FieldName='WorkersCompensationLaw' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.WorkersCompensationLaw or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.WorkersCompensationLaw,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.WorkersCompensationLaw,''),10) end
	else 
	case when B.COLUMN_NAME='WorkersCompensationLaw' and C.FieldName='WorkersCompensationLaw' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end WorkersCompensationLaw,
	case 
	when B.COLUMN_NAME='DescriptionOfWork' and C.FieldName='DescriptionOfWork' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.DescriptionOfWork or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.DescriptionOfWork,'')+replicate(' ',40),40) else Right(replicate('0',40)+ISNULL(A.DescriptionOfWork,''),40) end
	else 
	case when B.COLUMN_NAME='DescriptionOfWork' and C.FieldName='DescriptionOfWork' then case when C.FieldDataType in ('A','AN') then replicate(' ',40) else replicate('0',40) end
	end end DescriptionOfWork,
	replicate(' ',72) ReservedForFutureUse3,
	case 
	when B.COLUMN_NAME='EndorsementSequenceNumber' and C.FieldName='EndorsementSequenceNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementSequenceNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementSequenceNumber,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.EndorsementSequenceNumber,''),2) end
	else 
	case when B.COLUMN_NAME='EndorsementSequenceNumber' and C.FieldName='EndorsementSequenceNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end EndorsementSequenceNumber,
	case 
	when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfInsured,'')+replicate(' ',34),34) else Right(replicate('0',34)+ISNULL(A.NameOfInsured,''),34) end
	else 
	case when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',34) else replicate('0',34) end
	end end NameOfInsured,
	case 
	when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.EndorsementEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end EndorsementEffectiveDate,
	replicate(' ',6)ReservedForFutureUse4
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols23Record A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols23Record'
	and c.TableName='WCPols23Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols23RecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(NameOfVessel)+
	max(WorkersCompensationLaw)+
	max(DescriptionOfWork)+
	max(ReservedForFutureUse3)+
	max(EndorsementSequenceNumber)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4)
	,'0','')))<>''
),
EXP_DataCollect_Record23 AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPols23Record
),
SQ_WCPols24Record AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(NameOfAlternateEmployer)+
	max(AddressOfAlternateEmployer)+
	max(StateOfSpecialTemporaryEmployment)+
	max(NameOfContractOrProject)+
	max(ReservedForFutureUse3)+
	max(EndorsementSequenceNumber)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4) From (
	Select WCPols24RecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	replicate(' ',2) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	replicate(' ',3) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier,
	case 
	when B.COLUMN_NAME='NameOfAlternateEmployer' and C.FieldName='NameOfAlternateEmployer' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfAlternateEmployer or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfAlternateEmployer,'')+replicate(' ',60),60) else Right(replicate('0',60)+ISNULL(A.NameOfAlternateEmployer,''),60) end
	else 
	case when B.COLUMN_NAME='NameOfAlternateEmployer' and C.FieldName='NameOfAlternateEmployer' then case when C.FieldDataType in ('A','AN') then replicate(' ',60) else replicate('0',60) end
	end end NameOfAlternateEmployer,
	case 
	when B.COLUMN_NAME='AddressOfAlternateEmployer' and C.FieldName='AddressOfAlternateEmployer' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressOfAlternateEmployer or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressOfAlternateEmployer,'')+replicate(' ',52),52) else Right(replicate('0',52)+ISNULL(A.AddressOfAlternateEmployer,''),52) end
	else 
	case when B.COLUMN_NAME='AddressOfAlternateEmployer' and C.FieldName='AddressOfAlternateEmployer' then case when C.FieldDataType in ('A','AN') then replicate(' ',52) else replicate('0',52) end
	end end AddressOfAlternateEmployer,
	case 
	when B.COLUMN_NAME='StateOfSpecialTemporaryEmployment' and C.FieldName='StateOfSpecialTemporaryEmployment' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateOfSpecialTemporaryEmployment or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateOfSpecialTemporaryEmployment,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateOfSpecialTemporaryEmployment,''),2) end
	else 
	case when B.COLUMN_NAME='StateOfSpecialTemporaryEmployment' and C.FieldName='StateOfSpecialTemporaryEmployment' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateOfSpecialTemporaryEmployment,
	case 
	when B.COLUMN_NAME='NameOfContractOrProject' and C.FieldName='NameOfContractOrProject' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfContractOrProject or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfContractOrProject,'')+replicate(' ',50),50) else Right(replicate('0',50)+ISNULL(A.NameOfContractOrProject,''),50) end
	else 
	case when B.COLUMN_NAME='NameOfContractOrProject' and C.FieldName='NameOfContractOrProject' then case when C.FieldDataType in ('A','AN') then replicate(' ',50) else replicate('0',50) end
	end end NameOfContractOrProject,
	replicate(' ',18) ReservedForFutureUse3,
	case 
	when B.COLUMN_NAME='EndorsementSequenceNumber' and C.FieldName='EndorsementSequenceNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementSequenceNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementSequenceNumber,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.EndorsementSequenceNumber,''),2) end
	else 
	case when B.COLUMN_NAME='EndorsementSequenceNumber' and C.FieldName='EndorsementSequenceNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end EndorsementSequenceNumber,
	case 
	when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfInsured,'')+replicate(' ',34),34) else Right(replicate('0',34)+ISNULL(A.NameOfInsured,''),34) end
	else 
	case when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',34) else replicate('0',34) end
	end end NameOfInsured,
	case 
	when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.EndorsementEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end EndorsementEffectiveDate,
	replicate(' ',6)ReservedForFutureUse4
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols24Record A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols24Record'
	and c.TableName='WCPols24Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols24RecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(NameOfAlternateEmployer)+
	max(AddressOfAlternateEmployer)+
	max(StateOfSpecialTemporaryEmployment)+
	max(NameOfContractOrProject)+
	max(ReservedForFutureUse3)+
	max(EndorsementSequenceNumber)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4)
	,'0','')))<>''
),
EXP_DataCollect_Record24 AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPols24Record
),
SQ_WCPols25Record AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(AddressNotCovered)+
	max(ReservedForFutureUse3)+
	max(EndorsementSequenceNumber)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4) From (
	Select WCPols25RecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	replicate(' ',2) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	replicate(' ',3) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier,
	case 
	when B.COLUMN_NAME='AddressNotCovered' and C.FieldName='AddressNotCovered' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressNotCovered or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressNotCovered,'')+replicate(' ',120),120) else Right(replicate('0',120)+ISNULL(A.AddressNotCovered,''),120) end
	else 
	case when B.COLUMN_NAME='AddressNotCovered' and C.FieldName='AddressNotCovered' then case when C.FieldDataType in ('A','AN') then replicate(' ',120) else replicate('0',120) end
	end end AddressNotCovered,
	replicate(' ',62) ReservedForFutureUse3,
	case 
	when B.COLUMN_NAME='EndorsementSequenceNumber' and C.FieldName='EndorsementSequenceNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementSequenceNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementSequenceNumber,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.EndorsementSequenceNumber,''),2) end
	else 
	case when B.COLUMN_NAME='EndorsementSequenceNumber' and C.FieldName='EndorsementSequenceNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end EndorsementSequenceNumber,
	case 
	when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfInsured,'')+replicate(' ',34),34) else Right(replicate('0',34)+ISNULL(A.NameOfInsured,''),34) end
	else 
	case when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',34) else replicate('0',34) end
	end end NameOfInsured,
	case 
	when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.EndorsementEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end EndorsementEffectiveDate,
	replicate(' ',6)ReservedForFutureUse4
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols25Record A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols25Record'
	and c.TableName='WCPols25Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols25RecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(AddressNotCovered)+
	max(ReservedForFutureUse3)+
	max(EndorsementSequenceNumber)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4)
	,'0','')))<>''
),
EXP_DataCollect_Record25 AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPols25Record
),
SQ_WCPols29Record AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(IdentifyEmployees)+
	max(StateOfEmployment)+
	max(DesignatedWorkersCompensationLawOrDescription)+
	max(ReservedForFutureUse3)+
	max(EndorsementSequenceNumber)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4) From (
	Select WCPols29RecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	replicate(' ',2) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	replicate(' ',3) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier,
	case 
	when B.COLUMN_NAME='IdentifyEmployees' and C.FieldName='IdentifyEmployees' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.IdentifyEmployees or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.IdentifyEmployees,'')+replicate(' ',100),100) else Right(replicate('0',100)+ISNULL(A.IdentifyEmployees,''),100) end
	else 
	case when B.COLUMN_NAME='IdentifyEmployees' and C.FieldName='IdentifyEmployees' then case when C.FieldDataType in ('A','AN') then replicate(' ',100) else replicate('0',100) end
	end end IdentifyEmployees,
	case 
	when B.COLUMN_NAME='StateOfEmployment' and C.FieldName='StateOfEmployment' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateOfEmployment or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateOfEmployment,'')+replicate(' ',40),40) else Right(replicate('0',40)+ISNULL(A.StateOfEmployment,''),40) end
	else 
	case when B.COLUMN_NAME='StateOfEmployment' and C.FieldName='StateOfEmployment' then case when C.FieldDataType in ('A','AN') then replicate(' ',40) else replicate('0',40) end
	end end StateOfEmployment,
	case 
	when B.COLUMN_NAME='DesignatedWorkersCompensationLawOrDescription' and C.FieldName='DesignatedWorkersCompensationLawOrDescription' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.DesignatedWorkersCompensationLawOrDescription or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.DesignatedWorkersCompensationLawOrDescription,'')+replicate(' ',40),40) else Right(replicate('0',40)+ISNULL(A.DesignatedWorkersCompensationLawOrDescription,''),40) end
	else 
	case when B.COLUMN_NAME='DesignatedWorkersCompensationLawOrDescription' and C.FieldName='DesignatedWorkersCompensationLawOrDescription' then case when C.FieldDataType in ('A','AN') then replicate(' ',40) else replicate('0',40) end
	end end DesignatedWorkersCompensationLawOrDescription,
	replicate(' ',2) ReservedForFutureUse3,
	case 
	when B.COLUMN_NAME='EndorsementSequenceNumber' and C.FieldName='EndorsementSequenceNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementSequenceNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementSequenceNumber,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.EndorsementSequenceNumber,''),2) end
	else 
	case when B.COLUMN_NAME='EndorsementSequenceNumber' and C.FieldName='EndorsementSequenceNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end EndorsementSequenceNumber,
	case 
	when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfInsured,'')+replicate(' ',34),34) else Right(replicate('0',34)+ISNULL(A.NameOfInsured,''),34) end
	else 
	case when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',34) else replicate('0',34) end
	end end NameOfInsured,
	case 
	when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.EndorsementEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end EndorsementEffectiveDate,
	replicate(' ',6)ReservedForFutureUse4
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols29Record A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols29Record'
	and c.TableName='WCPols29Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols29RecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(IdentifyEmployees)+
	max(StateOfEmployment)+
	max(DesignatedWorkersCompensationLawOrDescription)+
	max(ReservedForFutureUse3)+
	max(EndorsementSequenceNumber)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4)
	,'0','')))<>''
),
EXP_DataCollect_Record29 AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPols29Record
),
SQ_WCPols30Record AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(StateCode)+
	max(FirstPremiumDiscountLayer)+
	max(FirstPremiumDiscountPercentage)+
	max(SecondNextPremiumDiscountLayer)+
	max(SecondNextPremiumDiscountPercentage)+
	max(ThirdNextPremiumDiscountLayer)+
	max(ThirdNextPremiumDiscountPercentage)+
	max(BalancePremiumDiscountLayer)+
	max(BalancePremiumDiscountPercentage)+
	max(AveragePercentageDiscount)+
	max(StateCode2)+
	max(FirstPremiumDiscountLayer2)+
	max(FirstPremiumDiscountPercentage2)+
	max(SecondNextPremiumDiscountLayer2)+
	max(SecondNextPremiumDiscountPercentage2)+
	max(ThirdNextPremiumDiscountLayer2)+
	max(ThirdNextPremiumDiscountPercentage2)+
	max(BalancePremiumDiscountLayer2)+
	max(BalancePremiumDiscountPercentage2)+
	max(StateCode3)+
	max(FirstPremiumDiscountLayer3)+
	max(FirstPremiumDiscountPercentage3)+
	max(SecondNextPremiumDiscountLayer3)+
	max(SecondNextPremiumDiscountPercentage3)+
	max(ThirdNextPremiumDiscountLayer3)+
	max(ThirdNextPremiumDiscountPercentage3)+
	max(BalancePremiumDiscountLayer3)+
	max(BalancePremiumDiscountPercentage3)+
	max(StateCode4)+
	max(FirstPremiumDiscountLayer4)+
	max(FirstPremiumDiscountPercentage4)+
	max(SecondNextPremiumDiscountLayer4)+
	max(SecondNextPremiumDiscountPercentage4)+
	max(ThirdNextPremiumDiscountLayer4)+
	max(ThirdNextPremiumDiscountPercentage4)+
	max(BalancePremiumDiscountLayer4)+
	max(BalancePremiumDiscountPercentage4)+
	max(StateCode5)+
	max(FirstPremiumDiscountLayer5)+
	max(FirstPremiumDiscountPercentage5)+
	max(SecondNextPremiumDiscountLayer5)+
	max(SecondNextPremiumDiscountPercentage5)+
	max(ThirdNextPremiumDiscountLayer5)+
	max(ThirdNextPremiumDiscountPercentage5)+
	max(BalancePremiumDiscountLayer5)+
	max(BalancePremiumDiscountPercentage5)+
	max(StateCode6)+
	max(FirstPremiumDiscountLayer6)+
	max(FirstPremiumDiscountPercentage6)+
	max(SecondNextPremiumDiscountLayer6)+
	max(SecondNextPremiumDiscountPercentage6)+
	max(ThirdNextPremiumDiscountLayer6)+
	max(ThirdNextPremiumDiscountPercentage6)+
	max(BalancePremiumDiscountLayer6)+
	max(BalancePremiumDiscountPercentage6)+
	max(ReservedForFutureUse3)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4) From (
	Select WCPols30RecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	replicate(' ',2) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	replicate(' ',3) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier,
	case 
	when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode,
	case 
	when B.COLUMN_NAME='FirstPremiumDiscountLayer' and C.FieldName='FirstPremiumDiscountLayer' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.FirstPremiumDiscountLayer or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.FirstPremiumDiscountLayer,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.FirstPremiumDiscountLayer,''),4) end
	else 
	case when B.COLUMN_NAME='FirstPremiumDiscountLayer' and C.FieldName='FirstPremiumDiscountLayer' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end FirstPremiumDiscountLayer,
	case 
	when B.COLUMN_NAME='FirstPremiumDiscountPercentage' and C.FieldName='FirstPremiumDiscountPercentage' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.FirstPremiumDiscountPercentage or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.FirstPremiumDiscountPercentage,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.FirstPremiumDiscountPercentage,''),3) end
	else 
	case when B.COLUMN_NAME='FirstPremiumDiscountPercentage' and C.FieldName='FirstPremiumDiscountPercentage' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end FirstPremiumDiscountPercentage,
	case 
	when B.COLUMN_NAME='SecondNextPremiumDiscountLayer' and C.FieldName='SecondNextPremiumDiscountLayer' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.SecondNextPremiumDiscountLayer or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.SecondNextPremiumDiscountLayer,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.SecondNextPremiumDiscountLayer,''),4) end
	else 
	case when B.COLUMN_NAME='SecondNextPremiumDiscountLayer' and C.FieldName='SecondNextPremiumDiscountLayer' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end SecondNextPremiumDiscountLayer,
	case 
	when B.COLUMN_NAME='SecondNextPremiumDiscountPercentage' and C.FieldName='SecondNextPremiumDiscountPercentage' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.SecondNextPremiumDiscountPercentage or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.SecondNextPremiumDiscountPercentage,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.SecondNextPremiumDiscountPercentage,''),3) end
	else 
	case when B.COLUMN_NAME='SecondNextPremiumDiscountPercentage' and C.FieldName='SecondNextPremiumDiscountPercentage' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end SecondNextPremiumDiscountPercentage,
	case 
	when B.COLUMN_NAME='ThirdNextPremiumDiscountLayer' and C.FieldName='ThirdNextPremiumDiscountLayer' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ThirdNextPremiumDiscountLayer or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ThirdNextPremiumDiscountLayer,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.ThirdNextPremiumDiscountLayer,''),4) end
	else 
	case when B.COLUMN_NAME='ThirdNextPremiumDiscountLayer' and C.FieldName='ThirdNextPremiumDiscountLayer' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end ThirdNextPremiumDiscountLayer,
	case 
	when B.COLUMN_NAME='ThirdNextPremiumDiscountPercentage' and C.FieldName='ThirdNextPremiumDiscountPercentage' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ThirdNextPremiumDiscountPercentage or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ThirdNextPremiumDiscountPercentage,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.ThirdNextPremiumDiscountPercentage,''),3) end
	else 
	case when B.COLUMN_NAME='ThirdNextPremiumDiscountPercentage' and C.FieldName='ThirdNextPremiumDiscountPercentage' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end ThirdNextPremiumDiscountPercentage,
	case 
	when B.COLUMN_NAME='BalancePremiumDiscountLayer' and C.FieldName='BalancePremiumDiscountLayer' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BalancePremiumDiscountLayer or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BalancePremiumDiscountLayer,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.BalancePremiumDiscountLayer,''),4) end
	else 
	case when B.COLUMN_NAME='BalancePremiumDiscountLayer' and C.FieldName='BalancePremiumDiscountLayer' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end BalancePremiumDiscountLayer,
	case 
	when B.COLUMN_NAME='BalancePremiumDiscountPercentage' and C.FieldName='BalancePremiumDiscountPercentage' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BalancePremiumDiscountPercentage or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BalancePremiumDiscountPercentage,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.BalancePremiumDiscountPercentage,''),3) end
	else 
	case when B.COLUMN_NAME='BalancePremiumDiscountPercentage' and C.FieldName='BalancePremiumDiscountPercentage' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end BalancePremiumDiscountPercentage,
	case 
	when B.COLUMN_NAME='AveragePercentageDiscount' and C.FieldName='AveragePercentageDiscount' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AveragePercentageDiscount or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AveragePercentageDiscount,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.AveragePercentageDiscount,''),3) end
	else 
	case when B.COLUMN_NAME='AveragePercentageDiscount' and C.FieldName='AveragePercentageDiscount' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end AveragePercentageDiscount,
	case 
	when B.COLUMN_NAME='StateCode2' and C.FieldName='StateCode2' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode2 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode2,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode2,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode2' and C.FieldName='StateCode2' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode2,
	case 
	when B.COLUMN_NAME='FirstPremiumDiscountLayer2' and C.FieldName='FirstPremiumDiscountLayer2' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.FirstPremiumDiscountLayer2 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.FirstPremiumDiscountLayer2,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.FirstPremiumDiscountLayer2,''),4) end
	else 
	case when B.COLUMN_NAME='FirstPremiumDiscountLayer2' and C.FieldName='FirstPremiumDiscountLayer2' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end FirstPremiumDiscountLayer2,
	case 
	when B.COLUMN_NAME='FirstPremiumDiscountPercentage2' and C.FieldName='FirstPremiumDiscountPercentage2' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.FirstPremiumDiscountPercentage2 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.FirstPremiumDiscountPercentage2,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.FirstPremiumDiscountPercentage2,''),3) end
	else 
	case when B.COLUMN_NAME='FirstPremiumDiscountPercentage2' and C.FieldName='FirstPremiumDiscountPercentage2' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end FirstPremiumDiscountPercentage2,
	case 
	when B.COLUMN_NAME='SecondNextPremiumDiscountLayer2' and C.FieldName='SecondNextPremiumDiscountLayer2' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.SecondNextPremiumDiscountLayer2 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.SecondNextPremiumDiscountLayer2,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.SecondNextPremiumDiscountLayer2,''),4) end
	else 
	case when B.COLUMN_NAME='SecondNextPremiumDiscountLayer2' and C.FieldName='SecondNextPremiumDiscountLayer2' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end SecondNextPremiumDiscountLayer2,
	case 
	when B.COLUMN_NAME='SecondNextPremiumDiscountPercentage2' and C.FieldName='SecondNextPremiumDiscountPercentage2' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.SecondNextPremiumDiscountPercentage2 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.SecondNextPremiumDiscountPercentage2,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.SecondNextPremiumDiscountPercentage2,''),3) end
	else 
	case when B.COLUMN_NAME='SecondNextPremiumDiscountPercentage2' and C.FieldName='SecondNextPremiumDiscountPercentage2' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end SecondNextPremiumDiscountPercentage2,
	case 
	when B.COLUMN_NAME='ThirdNextPremiumDiscountLayer2' and C.FieldName='ThirdNextPremiumDiscountLayer2' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ThirdNextPremiumDiscountLayer2 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ThirdNextPremiumDiscountLayer2,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.ThirdNextPremiumDiscountLayer2,''),4) end
	else 
	case when B.COLUMN_NAME='ThirdNextPremiumDiscountLayer2' and C.FieldName='ThirdNextPremiumDiscountLayer2' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end ThirdNextPremiumDiscountLayer2,
	case 
	when B.COLUMN_NAME='ThirdNextPremiumDiscountPercentage2' and C.FieldName='ThirdNextPremiumDiscountPercentage2' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ThirdNextPremiumDiscountPercentage2 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ThirdNextPremiumDiscountPercentage2,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.ThirdNextPremiumDiscountPercentage2,''),3) end
	else 
	case when B.COLUMN_NAME='ThirdNextPremiumDiscountPercentage2' and C.FieldName='ThirdNextPremiumDiscountPercentage2' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end ThirdNextPremiumDiscountPercentage2,
	case 
	when B.COLUMN_NAME='BalancePremiumDiscountLayer2' and C.FieldName='BalancePremiumDiscountLayer2' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BalancePremiumDiscountLayer2 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BalancePremiumDiscountLayer2,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.BalancePremiumDiscountLayer2,''),4) end
	else 
	case when B.COLUMN_NAME='BalancePremiumDiscountLayer2' and C.FieldName='BalancePremiumDiscountLayer2' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end BalancePremiumDiscountLayer2,
	case 
	when B.COLUMN_NAME='BalancePremiumDiscountPercentage2' and C.FieldName='BalancePremiumDiscountPercentage2' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BalancePremiumDiscountPercentage2 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BalancePremiumDiscountPercentage2,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.BalancePremiumDiscountPercentage2,''),3) end
	else 
	case when B.COLUMN_NAME='BalancePremiumDiscountPercentage2' and C.FieldName='BalancePremiumDiscountPercentage2' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end BalancePremiumDiscountPercentage2,
	case 
	when B.COLUMN_NAME='StateCode3' and C.FieldName='StateCode3' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode3 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode3,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode3,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode3' and C.FieldName='StateCode3' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode3,
	case 
	when B.COLUMN_NAME='FirstPremiumDiscountLayer3' and C.FieldName='FirstPremiumDiscountLayer3' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.FirstPremiumDiscountLayer3 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.FirstPremiumDiscountLayer3,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.FirstPremiumDiscountLayer3,''),4) end
	else 
	case when B.COLUMN_NAME='FirstPremiumDiscountLayer3' and C.FieldName='FirstPremiumDiscountLayer3' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end FirstPremiumDiscountLayer3,
	case 
	when B.COLUMN_NAME='FirstPremiumDiscountPercentage3' and C.FieldName='FirstPremiumDiscountPercentage3' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.FirstPremiumDiscountPercentage3 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.FirstPremiumDiscountPercentage3,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.FirstPremiumDiscountPercentage3,''),3) end
	else 
	case when B.COLUMN_NAME='FirstPremiumDiscountPercentage3' and C.FieldName='FirstPremiumDiscountPercentage3' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end FirstPremiumDiscountPercentage3,
	case 
	when B.COLUMN_NAME='SecondNextPremiumDiscountLayer3' and C.FieldName='SecondNextPremiumDiscountLayer3' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.SecondNextPremiumDiscountLayer3 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.SecondNextPremiumDiscountLayer3,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.SecondNextPremiumDiscountLayer3,''),4) end
	else 
	case when B.COLUMN_NAME='SecondNextPremiumDiscountLayer3' and C.FieldName='SecondNextPremiumDiscountLayer3' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end SecondNextPremiumDiscountLayer3,
	case 
	when B.COLUMN_NAME='SecondNextPremiumDiscountPercentage3' and C.FieldName='SecondNextPremiumDiscountPercentage3' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.SecondNextPremiumDiscountPercentage3 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.SecondNextPremiumDiscountPercentage3,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.SecondNextPremiumDiscountPercentage3,''),3) end
	else 
	case when B.COLUMN_NAME='SecondNextPremiumDiscountPercentage3' and C.FieldName='SecondNextPremiumDiscountPercentage3' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end SecondNextPremiumDiscountPercentage3,
	case 
	when B.COLUMN_NAME='ThirdNextPremiumDiscountLayer3' and C.FieldName='ThirdNextPremiumDiscountLayer3' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ThirdNextPremiumDiscountLayer3 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ThirdNextPremiumDiscountLayer3,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.ThirdNextPremiumDiscountLayer3,''),4) end
	else 
	case when B.COLUMN_NAME='ThirdNextPremiumDiscountLayer3' and C.FieldName='ThirdNextPremiumDiscountLayer3' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end ThirdNextPremiumDiscountLayer3,
	case 
	when B.COLUMN_NAME='ThirdNextPremiumDiscountPercentage3' and C.FieldName='ThirdNextPremiumDiscountPercentage3' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ThirdNextPremiumDiscountPercentage3 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ThirdNextPremiumDiscountPercentage3,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.ThirdNextPremiumDiscountPercentage3,''),3) end
	else 
	case when B.COLUMN_NAME='ThirdNextPremiumDiscountPercentage3' and C.FieldName='ThirdNextPremiumDiscountPercentage3' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end ThirdNextPremiumDiscountPercentage3,
	case 
	when B.COLUMN_NAME='BalancePremiumDiscountLayer3' and C.FieldName='BalancePremiumDiscountLayer3' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BalancePremiumDiscountLayer3 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BalancePremiumDiscountLayer3,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.BalancePremiumDiscountLayer3,''),4) end
	else 
	case when B.COLUMN_NAME='BalancePremiumDiscountLayer3' and C.FieldName='BalancePremiumDiscountLayer3' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end BalancePremiumDiscountLayer3,
	case 
	when B.COLUMN_NAME='BalancePremiumDiscountPercentage3' and C.FieldName='BalancePremiumDiscountPercentage3' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BalancePremiumDiscountPercentage3 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BalancePremiumDiscountPercentage3,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.BalancePremiumDiscountPercentage3,''),3) end
	else 
	case when B.COLUMN_NAME='BalancePremiumDiscountPercentage3' and C.FieldName='BalancePremiumDiscountPercentage3' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end BalancePremiumDiscountPercentage3,
	case 
	when B.COLUMN_NAME='StateCode4' and C.FieldName='StateCode4' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode4 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode4,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode4,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode4' and C.FieldName='StateCode4' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode4,
	case 
	when B.COLUMN_NAME='FirstPremiumDiscountLayer4' and C.FieldName='FirstPremiumDiscountLayer4' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.FirstPremiumDiscountLayer4 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.FirstPremiumDiscountLayer4,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.FirstPremiumDiscountLayer4,''),4) end
	else 
	case when B.COLUMN_NAME='FirstPremiumDiscountLayer4' and C.FieldName='FirstPremiumDiscountLayer4' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end FirstPremiumDiscountLayer4,
	case 
	when B.COLUMN_NAME='FirstPremiumDiscountPercentage4' and C.FieldName='FirstPremiumDiscountPercentage4' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.FirstPremiumDiscountPercentage4 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.FirstPremiumDiscountPercentage4,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.FirstPremiumDiscountPercentage4,''),3) end
	else 
	case when B.COLUMN_NAME='FirstPremiumDiscountPercentage4' and C.FieldName='FirstPremiumDiscountPercentage4' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end FirstPremiumDiscountPercentage4,
	case 
	when B.COLUMN_NAME='SecondNextPremiumDiscountLayer4' and C.FieldName='SecondNextPremiumDiscountLayer4' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.SecondNextPremiumDiscountLayer4 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.SecondNextPremiumDiscountLayer4,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.SecondNextPremiumDiscountLayer4,''),4) end
	else 
	case when B.COLUMN_NAME='SecondNextPremiumDiscountLayer4' and C.FieldName='SecondNextPremiumDiscountLayer4' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end SecondNextPremiumDiscountLayer4,
	case 
	when B.COLUMN_NAME='SecondNextPremiumDiscountPercentage4' and C.FieldName='SecondNextPremiumDiscountPercentage4' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.SecondNextPremiumDiscountPercentage4 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.SecondNextPremiumDiscountPercentage4,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.SecondNextPremiumDiscountPercentage4,''),3) end
	else 
	case when B.COLUMN_NAME='SecondNextPremiumDiscountPercentage4' and C.FieldName='SecondNextPremiumDiscountPercentage4' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end SecondNextPremiumDiscountPercentage4,
	case 
	when B.COLUMN_NAME='ThirdNextPremiumDiscountLayer4' and C.FieldName='ThirdNextPremiumDiscountLayer4' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ThirdNextPremiumDiscountLayer4 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ThirdNextPremiumDiscountLayer4,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.ThirdNextPremiumDiscountLayer4,''),4) end
	else 
	case when B.COLUMN_NAME='ThirdNextPremiumDiscountLayer4' and C.FieldName='ThirdNextPremiumDiscountLayer4' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end ThirdNextPremiumDiscountLayer4,
	case 
	when B.COLUMN_NAME='ThirdNextPremiumDiscountPercentage4' and C.FieldName='ThirdNextPremiumDiscountPercentage4' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ThirdNextPremiumDiscountPercentage4 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ThirdNextPremiumDiscountPercentage4,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.ThirdNextPremiumDiscountPercentage4,''),3) end
	else 
	case when B.COLUMN_NAME='ThirdNextPremiumDiscountPercentage4' and C.FieldName='ThirdNextPremiumDiscountPercentage4' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end ThirdNextPremiumDiscountPercentage4,
	case 
	when B.COLUMN_NAME='BalancePremiumDiscountLayer4' and C.FieldName='BalancePremiumDiscountLayer4' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BalancePremiumDiscountLayer4 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BalancePremiumDiscountLayer4,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.BalancePremiumDiscountLayer4,''),4) end
	else 
	case when B.COLUMN_NAME='BalancePremiumDiscountLayer4' and C.FieldName='BalancePremiumDiscountLayer4' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end BalancePremiumDiscountLayer4,
	case 
	when B.COLUMN_NAME='BalancePremiumDiscountPercentage4' and C.FieldName='BalancePremiumDiscountPercentage4' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BalancePremiumDiscountPercentage4 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BalancePremiumDiscountPercentage4,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.BalancePremiumDiscountPercentage4,''),3) end
	else 
	case when B.COLUMN_NAME='BalancePremiumDiscountPercentage4' and C.FieldName='BalancePremiumDiscountPercentage4' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end BalancePremiumDiscountPercentage4,
	case 
	when B.COLUMN_NAME='StateCode5' and C.FieldName='StateCode5' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode5 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode5,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode5,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode5' and C.FieldName='StateCode5' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode5,
	case 
	when B.COLUMN_NAME='FirstPremiumDiscountLayer5' and C.FieldName='FirstPremiumDiscountLayer5' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.FirstPremiumDiscountLayer5 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.FirstPremiumDiscountLayer5,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.FirstPremiumDiscountLayer5,''),4) end
	else 
	case when B.COLUMN_NAME='FirstPremiumDiscountLayer5' and C.FieldName='FirstPremiumDiscountLayer5' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end FirstPremiumDiscountLayer5,
	case 
	when B.COLUMN_NAME='FirstPremiumDiscountPercentage5' and C.FieldName='FirstPremiumDiscountPercentage5' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.FirstPremiumDiscountPercentage5 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.FirstPremiumDiscountPercentage5,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.FirstPremiumDiscountPercentage5,''),3) end
	else 
	case when B.COLUMN_NAME='FirstPremiumDiscountPercentage5' and C.FieldName='FirstPremiumDiscountPercentage5' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end FirstPremiumDiscountPercentage5,
	case 
	when B.COLUMN_NAME='SecondNextPremiumDiscountLayer5' and C.FieldName='SecondNextPremiumDiscountLayer5' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.SecondNextPremiumDiscountLayer5 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.SecondNextPremiumDiscountLayer5,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.SecondNextPremiumDiscountLayer5,''),4) end
	else 
	case when B.COLUMN_NAME='SecondNextPremiumDiscountLayer5' and C.FieldName='SecondNextPremiumDiscountLayer5' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end SecondNextPremiumDiscountLayer5,
	case 
	when B.COLUMN_NAME='SecondNextPremiumDiscountPercentage5' and C.FieldName='SecondNextPremiumDiscountPercentage5' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.SecondNextPremiumDiscountPercentage5 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.SecondNextPremiumDiscountPercentage5,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.SecondNextPremiumDiscountPercentage5,''),3) end
	else 
	case when B.COLUMN_NAME='SecondNextPremiumDiscountPercentage5' and C.FieldName='SecondNextPremiumDiscountPercentage5' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end SecondNextPremiumDiscountPercentage5,
	case 
	when B.COLUMN_NAME='ThirdNextPremiumDiscountLayer5' and C.FieldName='ThirdNextPremiumDiscountLayer5' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ThirdNextPremiumDiscountLayer5 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ThirdNextPremiumDiscountLayer5,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.ThirdNextPremiumDiscountLayer5,''),4) end
	else 
	case when B.COLUMN_NAME='ThirdNextPremiumDiscountLayer5' and C.FieldName='ThirdNextPremiumDiscountLayer5' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end ThirdNextPremiumDiscountLayer5,
	case 
	when B.COLUMN_NAME='ThirdNextPremiumDiscountPercentage5' and C.FieldName='ThirdNextPremiumDiscountPercentage5' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ThirdNextPremiumDiscountPercentage5 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ThirdNextPremiumDiscountPercentage5,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.ThirdNextPremiumDiscountPercentage5,''),3) end
	else 
	case when B.COLUMN_NAME='ThirdNextPremiumDiscountPercentage5' and C.FieldName='ThirdNextPremiumDiscountPercentage5' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end ThirdNextPremiumDiscountPercentage5,
	case 
	when B.COLUMN_NAME='BalancePremiumDiscountLayer5' and C.FieldName='BalancePremiumDiscountLayer5' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BalancePremiumDiscountLayer5 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BalancePremiumDiscountLayer5,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.BalancePremiumDiscountLayer5,''),4) end
	else 
	case when B.COLUMN_NAME='BalancePremiumDiscountLayer5' and C.FieldName='BalancePremiumDiscountLayer5' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end BalancePremiumDiscountLayer5,
	case 
	when B.COLUMN_NAME='BalancePremiumDiscountPercentage5' and C.FieldName='BalancePremiumDiscountPercentage5' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BalancePremiumDiscountPercentage5 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BalancePremiumDiscountPercentage5,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.BalancePremiumDiscountPercentage5,''),3) end
	else 
	case when B.COLUMN_NAME='BalancePremiumDiscountPercentage5' and C.FieldName='BalancePremiumDiscountPercentage5' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end BalancePremiumDiscountPercentage5,
	case 
	when B.COLUMN_NAME='StateCode6' and C.FieldName='StateCode6' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode6 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode6,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode6,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode6' and C.FieldName='StateCode6' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode6,
	case 
	when B.COLUMN_NAME='FirstPremiumDiscountLayer6' and C.FieldName='FirstPremiumDiscountLayer6' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.FirstPremiumDiscountLayer6 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.FirstPremiumDiscountLayer6,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.FirstPremiumDiscountLayer6,''),4) end
	else 
	case when B.COLUMN_NAME='FirstPremiumDiscountLayer6' and C.FieldName='FirstPremiumDiscountLayer6' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end FirstPremiumDiscountLayer6,
	case 
	when B.COLUMN_NAME='FirstPremiumDiscountPercentage6' and C.FieldName='FirstPremiumDiscountPercentage6' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.FirstPremiumDiscountPercentage6 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.FirstPremiumDiscountPercentage6,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.FirstPremiumDiscountPercentage6,''),3) end
	else 
	case when B.COLUMN_NAME='FirstPremiumDiscountPercentage6' and C.FieldName='FirstPremiumDiscountPercentage6' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end FirstPremiumDiscountPercentage6,
	case 
	when B.COLUMN_NAME='SecondNextPremiumDiscountLayer6' and C.FieldName='SecondNextPremiumDiscountLayer6' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.SecondNextPremiumDiscountLayer6 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.SecondNextPremiumDiscountLayer6,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.SecondNextPremiumDiscountLayer6,''),4) end
	else 
	case when B.COLUMN_NAME='SecondNextPremiumDiscountLayer6' and C.FieldName='SecondNextPremiumDiscountLayer6' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end SecondNextPremiumDiscountLayer6,
	case 
	when B.COLUMN_NAME='SecondNextPremiumDiscountPercentage6' and C.FieldName='SecondNextPremiumDiscountPercentage6' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.SecondNextPremiumDiscountPercentage6 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.SecondNextPremiumDiscountPercentage6,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.SecondNextPremiumDiscountPercentage6,''),3) end
	else 
	case when B.COLUMN_NAME='SecondNextPremiumDiscountPercentage6' and C.FieldName='SecondNextPremiumDiscountPercentage6' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end SecondNextPremiumDiscountPercentage6,
	case 
	when B.COLUMN_NAME='ThirdNextPremiumDiscountLayer6' and C.FieldName='ThirdNextPremiumDiscountLayer6' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ThirdNextPremiumDiscountLayer6 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ThirdNextPremiumDiscountLayer6,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.ThirdNextPremiumDiscountLayer6,''),4) end
	else 
	case when B.COLUMN_NAME='ThirdNextPremiumDiscountLayer6' and C.FieldName='ThirdNextPremiumDiscountLayer6' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end ThirdNextPremiumDiscountLayer6,
	case 
	when B.COLUMN_NAME='ThirdNextPremiumDiscountPercentage6' and C.FieldName='ThirdNextPremiumDiscountPercentage6' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ThirdNextPremiumDiscountPercentage6 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ThirdNextPremiumDiscountPercentage6,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.ThirdNextPremiumDiscountPercentage6,''),3) end
	else 
	case when B.COLUMN_NAME='ThirdNextPremiumDiscountPercentage6' and C.FieldName='ThirdNextPremiumDiscountPercentage6' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end ThirdNextPremiumDiscountPercentage6,
	case 
	when B.COLUMN_NAME='BalancePremiumDiscountLayer6' and C.FieldName='BalancePremiumDiscountLayer6' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BalancePremiumDiscountLayer6 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BalancePremiumDiscountLayer6,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.BalancePremiumDiscountLayer6,''),4) end
	else 
	case when B.COLUMN_NAME='BalancePremiumDiscountLayer6' and C.FieldName='BalancePremiumDiscountLayer6' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end BalancePremiumDiscountLayer6,
	case 
	when B.COLUMN_NAME='BalancePremiumDiscountPercentage6' and C.FieldName='BalancePremiumDiscountPercentage6' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BalancePremiumDiscountPercentage6 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BalancePremiumDiscountPercentage6,'')+replicate(' ',3),3) else Right(replicate('0',3)+ISNULL(A.BalancePremiumDiscountPercentage6,''),3) end
	else 
	case when B.COLUMN_NAME='BalancePremiumDiscountPercentage6' and C.FieldName='BalancePremiumDiscountPercentage6' then case when C.FieldDataType in ('A','AN') then replicate(' ',3) else replicate('0',3) end
	end end BalancePremiumDiscountPercentage6,
	replicate(' ',1) ReservedForFutureUse3,
	case 
	when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfInsured,'')+replicate(' ',34),34) else Right(replicate('0',34)+ISNULL(A.NameOfInsured,''),34) end
	else 
	case when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',34) else replicate('0',34) end
	end end NameOfInsured,
	case 
	when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.EndorsementEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end EndorsementEffectiveDate,
	replicate(' ',6)ReservedForFutureUse4
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols30Record A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols30Record'
	and c.TableName='WCPols30Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols30RecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(StateCode)+
	max(FirstPremiumDiscountLayer)+
	max(FirstPremiumDiscountPercentage)+
	max(SecondNextPremiumDiscountLayer)+
	max(SecondNextPremiumDiscountPercentage)+
	max(ThirdNextPremiumDiscountLayer)+
	max(ThirdNextPremiumDiscountPercentage)+
	max(BalancePremiumDiscountLayer)+
	max(BalancePremiumDiscountPercentage)+
	max(AveragePercentageDiscount)+
	max(StateCode2)+
	max(FirstPremiumDiscountLayer2)+
	max(FirstPremiumDiscountPercentage2)+
	max(SecondNextPremiumDiscountLayer2)+
	max(SecondNextPremiumDiscountPercentage2)+
	max(ThirdNextPremiumDiscountLayer2)+
	max(ThirdNextPremiumDiscountPercentage2)+
	max(BalancePremiumDiscountLayer2)+
	max(BalancePremiumDiscountPercentage2)+
	max(StateCode3)+
	max(FirstPremiumDiscountLayer3)+
	max(FirstPremiumDiscountPercentage3)+
	max(SecondNextPremiumDiscountLayer3)+
	max(SecondNextPremiumDiscountPercentage3)+
	max(ThirdNextPremiumDiscountLayer3)+
	max(ThirdNextPremiumDiscountPercentage3)+
	max(BalancePremiumDiscountLayer3)+
	max(BalancePremiumDiscountPercentage3)+
	max(StateCode4)+
	max(FirstPremiumDiscountLayer4)+
	max(FirstPremiumDiscountPercentage4)+
	max(SecondNextPremiumDiscountLayer4)+
	max(SecondNextPremiumDiscountPercentage4)+
	max(ThirdNextPremiumDiscountLayer4)+
	max(ThirdNextPremiumDiscountPercentage4)+
	max(BalancePremiumDiscountLayer4)+
	max(BalancePremiumDiscountPercentage4)+
	max(StateCode5)+
	max(FirstPremiumDiscountLayer5)+
	max(FirstPremiumDiscountPercentage5)+
	max(SecondNextPremiumDiscountLayer5)+
	max(SecondNextPremiumDiscountPercentage5)+
	max(ThirdNextPremiumDiscountLayer5)+
	max(ThirdNextPremiumDiscountPercentage5)+
	max(BalancePremiumDiscountLayer5)+
	max(BalancePremiumDiscountPercentage5)+
	max(StateCode6)+
	max(FirstPremiumDiscountLayer6)+
	max(FirstPremiumDiscountPercentage6)+
	max(SecondNextPremiumDiscountLayer6)+
	max(SecondNextPremiumDiscountPercentage6)+
	max(ThirdNextPremiumDiscountLayer6)+
	max(ThirdNextPremiumDiscountPercentage6)+
	max(BalancePremiumDiscountLayer6)+
	max(BalancePremiumDiscountPercentage6)+
	max(ReservedForFutureUse3)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4)
	,'0','')))<>''
),
EXP_DataCollect_Record30 AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPols30Record
),
SQ_WCPols36Record AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(NameOfPerson)+
	max(NameOfOrganization)+
	max(ReservedForFutureUse3)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4) From (
	Select WCPols36RecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	replicate(' ',2) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	replicate(' ',3) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier,
	case 
	when B.COLUMN_NAME='NameOfPerson' and C.FieldName='NameOfPerson' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfPerson or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfPerson,'')+replicate(' ',60),60) else Right(replicate('0',60)+ISNULL(A.NameOfPerson,''),60) end
	else 
	case when B.COLUMN_NAME='NameOfPerson' and C.FieldName='NameOfPerson' then case when C.FieldDataType in ('A','AN') then replicate(' ',60) else replicate('0',60) end
	end end NameOfPerson,
	case 
	when B.COLUMN_NAME='NameOfOrganization' and C.FieldName='NameOfOrganization' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfOrganization or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfOrganization,'')+replicate(' ',60),60) else Right(replicate('0',60)+ISNULL(A.NameOfOrganization,''),60) end
	else 
	case when B.COLUMN_NAME='NameOfOrganization' and C.FieldName='NameOfOrganization' then case when C.FieldDataType in ('A','AN') then replicate(' ',60) else replicate('0',60) end
	end end NameOfOrganization,
	replicate(' ',64) ReservedForFutureUse3,
	case 
	when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfInsured,'')+replicate(' ',34),34) else Right(replicate('0',34)+ISNULL(A.NameOfInsured,''),34) end
	else 
	case when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',34) else replicate('0',34) end
	end end NameOfInsured,
	case 
	when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.EndorsementEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end EndorsementEffectiveDate,
	replicate(' ',6)ReservedForFutureUse4
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols36Record A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols36Record'
	and c.TableName='WCPols36Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols36RecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(NameOfPerson)+
	max(NameOfOrganization)+
	max(ReservedForFutureUse3)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4)
	,'0','')))<>''
),
EXP_DataCollect_Record36 AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPols36Record
),
SQ_WCPols37Record AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(DescriptorCode)+
	max(NameOfPersonToBeIncluded)+
	max(StateCode)+
	max(DescriptorCode2)+
	max(NameOfPersonToBeIncluded2)+
	max(StateCode2)+
	max(DescriptorCode3)+
	max(NameOfPersonToBeIncluded3)+
	max(StateCode3)+
	max(ReservedForFutureUse3)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4) From (
	Select WCPols37RecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	replicate(' ',2) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	replicate(' ',3) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier,
	case 
	when B.COLUMN_NAME='DescriptorCode' and C.FieldName='DescriptorCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.DescriptorCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.DescriptorCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.DescriptorCode,''),1) end
	else 
	case when B.COLUMN_NAME='DescriptorCode' and C.FieldName='DescriptorCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end DescriptorCode,
	case 
	when B.COLUMN_NAME='NameOfPersonToBeIncluded' and C.FieldName='NameOfPersonToBeIncluded' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfPersonToBeIncluded or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfPersonToBeIncluded,'')+replicate(' ',50),50) else Right(replicate('0',50)+ISNULL(A.NameOfPersonToBeIncluded,''),50) end
	else 
	case when B.COLUMN_NAME='NameOfPersonToBeIncluded' and C.FieldName='NameOfPersonToBeIncluded' then case when C.FieldDataType in ('A','AN') then replicate(' ',50) else replicate('0',50) end
	end end NameOfPersonToBeIncluded,
	case 
	when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode,
	case 
	when B.COLUMN_NAME='DescriptorCode2' and C.FieldName='DescriptorCode2' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.DescriptorCode2 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.DescriptorCode2,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.DescriptorCode2,''),1) end
	else 
	case when B.COLUMN_NAME='DescriptorCode2' and C.FieldName='DescriptorCode2' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end DescriptorCode2,
	case 
	when B.COLUMN_NAME='NameOfPersonToBeIncluded2' and C.FieldName='NameOfPersonToBeIncluded2' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfPersonToBeIncluded2 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfPersonToBeIncluded2,'')+replicate(' ',50),50) else Right(replicate('0',50)+ISNULL(A.NameOfPersonToBeIncluded2,''),50) end
	else 
	case when B.COLUMN_NAME='NameOfPersonToBeIncluded2' and C.FieldName='NameOfPersonToBeIncluded2' then case when C.FieldDataType in ('A','AN') then replicate(' ',50) else replicate('0',50) end
	end end NameOfPersonToBeIncluded2,
	case 
	when B.COLUMN_NAME='StateCode2' and C.FieldName='StateCode2' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode2 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode2,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode2,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode2' and C.FieldName='StateCode2' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode2,
	case 
	when B.COLUMN_NAME='DescriptorCode3' and C.FieldName='DescriptorCode3' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.DescriptorCode3 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.DescriptorCode3,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.DescriptorCode3,''),1) end
	else 
	case when B.COLUMN_NAME='DescriptorCode3' and C.FieldName='DescriptorCode3' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end DescriptorCode3,
	case 
	when B.COLUMN_NAME='NameOfPersonToBeIncluded3' and C.FieldName='NameOfPersonToBeIncluded3' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfPersonToBeIncluded3 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfPersonToBeIncluded3,'')+replicate(' ',50),50) else Right(replicate('0',50)+ISNULL(A.NameOfPersonToBeIncluded3,''),50) end
	else 
	case when B.COLUMN_NAME='NameOfPersonToBeIncluded3' and C.FieldName='NameOfPersonToBeIncluded3' then case when C.FieldDataType in ('A','AN') then replicate(' ',50) else replicate('0',50) end
	end end NameOfPersonToBeIncluded3,
	case 
	when B.COLUMN_NAME='StateCode3' and C.FieldName='StateCode3' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode3 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode3,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode3,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode3' and C.FieldName='StateCode3' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode3,
	replicate(' ',25) ReservedForFutureUse3,
	case 
	when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfInsured,'')+replicate(' ',34),34) else Right(replicate('0',34)+ISNULL(A.NameOfInsured,''),34) end
	else 
	case when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',34) else replicate('0',34) end
	end end NameOfInsured,
	case 
	when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.EndorsementEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end EndorsementEffectiveDate,
	replicate(' ',6)ReservedForFutureUse4
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols37Record A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols37Record'
	and c.TableName='WCPols37Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols37RecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(DescriptorCode)+
	max(NameOfPersonToBeIncluded)+
	max(StateCode)+
	max(DescriptorCode2)+
	max(NameOfPersonToBeIncluded2)+
	max(StateCode2)+
	max(DescriptorCode3)+
	max(NameOfPersonToBeIncluded3)+
	max(StateCode3)+
	max(ReservedForFutureUse3)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4)
	,'0','')))<>''
),
EXP_DataCollect_Record37 AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPols37Record
),
SQ_WCPols38Record AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(DescriptorCode)+
	max(NameOfPersonToBeExcluded)+
	max(DescriptorCode2)+
	max(NameOfPersonToBeExcluded2)+
	max(DescriptorCode3)+
	max(NameOfPersonToBeExcluded3)+
	max(ReservedForFutureUse3)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4) From (
	Select WCPols38RecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	replicate(' ',2) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	replicate(' ',3) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier,
	case 
	when B.COLUMN_NAME='DescriptorCode' and C.FieldName='DescriptorCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.DescriptorCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.DescriptorCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.DescriptorCode,''),1) end
	else 
	case when B.COLUMN_NAME='DescriptorCode' and C.FieldName='DescriptorCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end DescriptorCode,
	case 
	when B.COLUMN_NAME='NameOfPersonToBeExcluded' and C.FieldName='NameOfPersonToBeExcluded' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfPersonToBeExcluded or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfPersonToBeExcluded,'')+replicate(' ',60),60) else Right(replicate('0',60)+ISNULL(A.NameOfPersonToBeExcluded,''),60) end
	else 
	case when B.COLUMN_NAME='NameOfPersonToBeExcluded' and C.FieldName='NameOfPersonToBeExcluded' then case when C.FieldDataType in ('A','AN') then replicate(' ',60) else replicate('0',60) end
	end end NameOfPersonToBeExcluded,
	case 
	when B.COLUMN_NAME='DescriptorCode2' and C.FieldName='DescriptorCode2' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.DescriptorCode2 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.DescriptorCode2,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.DescriptorCode2,''),1) end
	else 
	case when B.COLUMN_NAME='DescriptorCode2' and C.FieldName='DescriptorCode2' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end DescriptorCode2,
	case 
	when B.COLUMN_NAME='NameOfPersonToBeExcluded2' and C.FieldName='NameOfPersonToBeExcluded2' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfPersonToBeExcluded2 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfPersonToBeExcluded2,'')+replicate(' ',60),60) else Right(replicate('0',60)+ISNULL(A.NameOfPersonToBeExcluded2,''),60) end
	else 
	case when B.COLUMN_NAME='NameOfPersonToBeExcluded2' and C.FieldName='NameOfPersonToBeExcluded2' then case when C.FieldDataType in ('A','AN') then replicate(' ',60) else replicate('0',60) end
	end end NameOfPersonToBeExcluded2,
	case 
	when B.COLUMN_NAME='DescriptorCode3' and C.FieldName='DescriptorCode3' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.DescriptorCode3 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.DescriptorCode3,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.DescriptorCode3,''),1) end
	else 
	case when B.COLUMN_NAME='DescriptorCode3' and C.FieldName='DescriptorCode3' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end DescriptorCode3,
	case 
	when B.COLUMN_NAME='NameOfPersonToBeExcluded3' and C.FieldName='NameOfPersonToBeExcluded3' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfPersonToBeExcluded3 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfPersonToBeExcluded3,'')+replicate(' ',60),60) else Right(replicate('0',60)+ISNULL(A.NameOfPersonToBeExcluded3,''),60) end
	else 
	case when B.COLUMN_NAME='NameOfPersonToBeExcluded3' and C.FieldName='NameOfPersonToBeExcluded3' then case when C.FieldDataType in ('A','AN') then replicate(' ',60) else replicate('0',60) end
	end end NameOfPersonToBeExcluded3,
	replicate(' ',1) ReservedForFutureUse3,
	case 
	when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfInsured,'')+replicate(' ',34),34) else Right(replicate('0',34)+ISNULL(A.NameOfInsured,''),34) end
	else 
	case when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',34) else replicate('0',34) end
	end end NameOfInsured,
	case 
	when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.EndorsementEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end EndorsementEffectiveDate,
	replicate(' ',6)ReservedForFutureUse4
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols38Record A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols38Record'
	and c.TableName='WCPols38Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols38RecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(DescriptorCode)+
	max(NameOfPersonToBeExcluded)+
	max(DescriptorCode2)+
	max(NameOfPersonToBeExcluded2)+
	max(DescriptorCode3)+
	max(NameOfPersonToBeExcluded3)+
	max(ReservedForFutureUse3)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4)
	,'0','')))<>''
),
EXP_DataCollect_Record38 AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPols38Record
),
SQ_WCPols40Record AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(StateCode)+
	max(StateCode2)+
	max(StateCode3)+
	max(StateCode4)+
	max(StateCode5)+
	max(StateCode6)+
	max(StateCode7)+
	max(StateCode8)+
	max(StateCode9)+
	max(StateCode10)+
	max(StateCode11)+
	max(StateCode12)+
	max(StateCode13)+
	max(StateCode14)+
	max(StateCode15)+
	max(StateCode16)+
	max(StateCode17)+
	max(StateCode18)+
	max(StateCode19)+
	max(StateCode20)+
	max(StateCode21)+
	max(StateCode22)+
	max(StateCode23)+
	max(StateCode24)+
	max(StateCode25)+
	max(StateCode26)+
	max(StateCode27)+
	max(StateCode28)+
	max(StateCode29)+
	max(StateCode30)+
	max(StateCode31)+
	max(StateCode32)+
	max(StateCode33)+
	max(StateCode34)+
	max(StateCode35)+
	max(StateCode36)+
	max(StateCode37)+
	max(StateCode38)+
	max(StateCode39)+
	max(StateCode40)+
	max(StateCode41)+
	max(StateCode42)+
	max(StateCode43)+
	max(StateCode44)+
	max(StateCode45)+
	max(StateCode46)+
	max(StateCode47)+
	max(StateCode48)+
	max(StateCode49)+
	max(StateCode50)+
	max(ReservedForFutureUse3)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4) From (
	Select WCPols40RecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	replicate(' ',2) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	replicate(' ',3) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier,
	case 
	when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode,
	case 
	when B.COLUMN_NAME='StateCode2' and C.FieldName='StateCode2' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode2 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode2,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode2,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode2' and C.FieldName='StateCode2' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode2,
	case 
	when B.COLUMN_NAME='StateCode3' and C.FieldName='StateCode3' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode3 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode3,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode3,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode3' and C.FieldName='StateCode3' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode3,
	case 
	when B.COLUMN_NAME='StateCode4' and C.FieldName='StateCode4' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode4 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode4,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode4,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode4' and C.FieldName='StateCode4' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode4,
	case 
	when B.COLUMN_NAME='StateCode5' and C.FieldName='StateCode5' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode5 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode5,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode5,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode5' and C.FieldName='StateCode5' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode5,
	case 
	when B.COLUMN_NAME='StateCode6' and C.FieldName='StateCode6' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode6 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode6,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode6,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode6' and C.FieldName='StateCode6' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode6,
	case 
	when B.COLUMN_NAME='StateCode7' and C.FieldName='StateCode7' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode7 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode7,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode7,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode7' and C.FieldName='StateCode7' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode7,
	case 
	when B.COLUMN_NAME='StateCode8' and C.FieldName='StateCode8' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode8 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode8,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode8,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode8' and C.FieldName='StateCode8' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode8,
	case 
	when B.COLUMN_NAME='StateCode9' and C.FieldName='StateCode9' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode9 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode9,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode9,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode9' and C.FieldName='StateCode9' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode9,
	case 
	when B.COLUMN_NAME='StateCode10' and C.FieldName='StateCode10' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode10 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode10,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode10,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode10' and C.FieldName='StateCode10' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode10,
	case 
	when B.COLUMN_NAME='StateCode11' and C.FieldName='StateCode11' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode11 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode11,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode11,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode11' and C.FieldName='StateCode11' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode11,
	case 
	when B.COLUMN_NAME='StateCode12' and C.FieldName='StateCode12' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode12 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode12,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode12,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode12' and C.FieldName='StateCode12' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode12,
	case 
	when B.COLUMN_NAME='StateCode13' and C.FieldName='StateCode13' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode13 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode13,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode13,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode13' and C.FieldName='StateCode13' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode13,
	case 
	when B.COLUMN_NAME='StateCode14' and C.FieldName='StateCode14' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode14 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode14,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode14,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode14' and C.FieldName='StateCode14' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode14,
	case 
	when B.COLUMN_NAME='StateCode15' and C.FieldName='StateCode15' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode15 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode15,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode15,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode15' and C.FieldName='StateCode15' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode15,
	case 
	when B.COLUMN_NAME='StateCode16' and C.FieldName='StateCode16' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode16 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode16,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode16,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode16' and C.FieldName='StateCode16' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode16,
	case 
	when B.COLUMN_NAME='StateCode17' and C.FieldName='StateCode17' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode17 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode17,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode17,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode17' and C.FieldName='StateCode17' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode17,
	case 
	when B.COLUMN_NAME='StateCode18' and C.FieldName='StateCode18' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode18 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode18,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode18,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode18' and C.FieldName='StateCode18' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode18,
	case 
	when B.COLUMN_NAME='StateCode19' and C.FieldName='StateCode19' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode19 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode19,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode19,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode19' and C.FieldName='StateCode19' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode19,
	case 
	when B.COLUMN_NAME='StateCode20' and C.FieldName='StateCode20' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode20 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode20,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode20,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode20' and C.FieldName='StateCode20' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode20,
	case 
	when B.COLUMN_NAME='StateCode21' and C.FieldName='StateCode21' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode21 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode21,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode21,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode21' and C.FieldName='StateCode21' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode21,
	case 
	when B.COLUMN_NAME='StateCode22' and C.FieldName='StateCode22' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode22 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode22,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode22,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode22' and C.FieldName='StateCode22' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode22,
	case 
	when B.COLUMN_NAME='StateCode23' and C.FieldName='StateCode23' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode23 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode23,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode23,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode23' and C.FieldName='StateCode23' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode23,
	case 
	when B.COLUMN_NAME='StateCode24' and C.FieldName='StateCode24' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode24 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode24,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode24,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode24' and C.FieldName='StateCode24' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode24,
	case 
	when B.COLUMN_NAME='StateCode25' and C.FieldName='StateCode25' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode25 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode25,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode25,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode25' and C.FieldName='StateCode25' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode25,
	case 
	when B.COLUMN_NAME='StateCode26' and C.FieldName='StateCode26' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode26 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode26,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode26,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode26' and C.FieldName='StateCode26' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode26,
	case 
	when B.COLUMN_NAME='StateCode27' and C.FieldName='StateCode27' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode27 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode27,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode27,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode27' and C.FieldName='StateCode27' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode27,
	case 
	when B.COLUMN_NAME='StateCode28' and C.FieldName='StateCode28' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode28 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode28,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode28,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode28' and C.FieldName='StateCode28' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode28,
	case 
	when B.COLUMN_NAME='StateCode29' and C.FieldName='StateCode29' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode29 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode29,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode29,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode29' and C.FieldName='StateCode29' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode29,
	case 
	when B.COLUMN_NAME='StateCode30' and C.FieldName='StateCode30' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode30 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode30,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode30,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode30' and C.FieldName='StateCode30' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode30,
	case 
	when B.COLUMN_NAME='StateCode31' and C.FieldName='StateCode31' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode31 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode31,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode31,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode31' and C.FieldName='StateCode31' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode31,
	case 
	when B.COLUMN_NAME='StateCode32' and C.FieldName='StateCode32' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode32 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode32,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode32,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode32' and C.FieldName='StateCode32' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode32,
	case 
	when B.COLUMN_NAME='StateCode33' and C.FieldName='StateCode33' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode33 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode33,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode33,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode33' and C.FieldName='StateCode33' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode33,
	case 
	when B.COLUMN_NAME='StateCode34' and C.FieldName='StateCode34' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode34 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode34,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode34,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode34' and C.FieldName='StateCode34' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode34,
	case 
	when B.COLUMN_NAME='StateCode35' and C.FieldName='StateCode35' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode35 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode35,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode35,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode35' and C.FieldName='StateCode35' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode35,
	case 
	when B.COLUMN_NAME='StateCode36' and C.FieldName='StateCode36' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode36 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode36,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode36,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode36' and C.FieldName='StateCode36' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode36,
	case 
	when B.COLUMN_NAME='StateCode37' and C.FieldName='StateCode37' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode37 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode37,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode37,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode37' and C.FieldName='StateCode37' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode37,
	case 
	when B.COLUMN_NAME='StateCode38' and C.FieldName='StateCode38' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode38 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode38,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode38,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode38' and C.FieldName='StateCode38' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode38,
	case 
	when B.COLUMN_NAME='StateCode39' and C.FieldName='StateCode39' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode39 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode39,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode39,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode39' and C.FieldName='StateCode39' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode39,
	case 
	when B.COLUMN_NAME='StateCode40' and C.FieldName='StateCode40' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode40 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode40,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode40,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode40' and C.FieldName='StateCode40' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode40,
	case 
	when B.COLUMN_NAME='StateCode41' and C.FieldName='StateCode41' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode41 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode41,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode41,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode41' and C.FieldName='StateCode41' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode41,
	case 
	when B.COLUMN_NAME='StateCode42' and C.FieldName='StateCode42' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode42 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode42,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode42,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode42' and C.FieldName='StateCode42' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode42,
	case 
	when B.COLUMN_NAME='StateCode43' and C.FieldName='StateCode43' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode43 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode43,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode43,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode43' and C.FieldName='StateCode43' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode43,
	case 
	when B.COLUMN_NAME='StateCode44' and C.FieldName='StateCode44' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode44 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode44,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode44,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode44' and C.FieldName='StateCode44' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode44,
	case 
	when B.COLUMN_NAME='StateCode45' and C.FieldName='StateCode45' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode45 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode45,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode45,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode45' and C.FieldName='StateCode45' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode45,
	case 
	when B.COLUMN_NAME='StateCode46' and C.FieldName='StateCode46' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode46 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode46,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode46,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode46' and C.FieldName='StateCode46' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode46,
	case 
	when B.COLUMN_NAME='StateCode47' and C.FieldName='StateCode47' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode47 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode47,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode47,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode47' and C.FieldName='StateCode47' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode47,
	case 
	when B.COLUMN_NAME='StateCode48' and C.FieldName='StateCode48' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode48 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode48,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode48,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode48' and C.FieldName='StateCode48' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode48,
	case 
	when B.COLUMN_NAME='StateCode49' and C.FieldName='StateCode49' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode49 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode49,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode49,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode49' and C.FieldName='StateCode49' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode49,
	case 
	when B.COLUMN_NAME='StateCode50' and C.FieldName='StateCode50' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode50 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode50,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode50,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode50' and C.FieldName='StateCode50' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode50,
	replicate(' ',84) ReservedForFutureUse3,
	case 
	when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfInsured,'')+replicate(' ',34),34) else Right(replicate('0',34)+ISNULL(A.NameOfInsured,''),34) end
	else 
	case when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',34) else replicate('0',34) end
	end end NameOfInsured,
	case 
	when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.EndorsementEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end EndorsementEffectiveDate,
	replicate(' ',6)ReservedForFutureUse4
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols40Record A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols40Record'
	and c.TableName='WCPols40Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols40RecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(StateCode)+
	max(StateCode2)+
	max(StateCode3)+
	max(StateCode4)+
	max(StateCode5)+
	max(StateCode6)+
	max(StateCode7)+
	max(StateCode8)+
	max(StateCode9)+
	max(StateCode10)+
	max(StateCode11)+
	max(StateCode12)+
	max(StateCode13)+
	max(StateCode14)+
	max(StateCode15)+
	max(StateCode16)+
	max(StateCode17)+
	max(StateCode18)+
	max(StateCode19)+
	max(StateCode20)+
	max(StateCode21)+
	max(StateCode22)+
	max(StateCode23)+
	max(StateCode24)+
	max(StateCode25)+
	max(StateCode26)+
	max(StateCode27)+
	max(StateCode28)+
	max(StateCode29)+
	max(StateCode30)+
	max(StateCode31)+
	max(StateCode32)+
	max(StateCode33)+
	max(StateCode34)+
	max(StateCode35)+
	max(StateCode36)+
	max(StateCode37)+
	max(StateCode38)+
	max(StateCode39)+
	max(StateCode40)+
	max(StateCode41)+
	max(StateCode42)+
	max(StateCode43)+
	max(StateCode44)+
	max(StateCode45)+
	max(StateCode46)+
	max(StateCode47)+
	max(StateCode48)+
	max(StateCode49)+
	max(StateCode50)+
	max(ReservedForFutureUse3)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4)
	,'0','')))<>''
),
EXP_DataCollect_Record40 AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPols40Record
),
SQ_WCPOLS_Record42 AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse1)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(ContingentModificationEffectiveDate)+
	max(ContingentExperienceModificationFactor)+
	max(ReservedForFutureUse2)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse3) From (
	Select WCPols42RecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	case 
	when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	replicate(' ',3) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier,
	case 
	when B.COLUMN_NAME='ContingentModificationEffectiveDate' and C.FieldName='ContingentModificationEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ContingentModificationEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ContingentModificationEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.ContingentModificationEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='ContingentModificationEffectiveDate' and C.FieldName='ContingentModificationEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end ContingentModificationEffectiveDate,
	case 
	when B.COLUMN_NAME='ContingentExperienceModificationFactor' and C.FieldName='ContingentExperienceModificationFactor' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ContingentExperienceModificationFactor or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ContingentExperienceModificationFactor,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.ContingentExperienceModificationFactor,''),4) end
	else 
	case when B.COLUMN_NAME='ContingentExperienceModificationFactor' and C.FieldName='ContingentExperienceModificationFactor' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end ContingentExperienceModificationFactor,
	replicate(' ',174) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfInsured,'')+replicate(' ',34),34) else Right(replicate('0',34)+ISNULL(A.NameOfInsured,''),34) end
	else 
	case when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',34) else replicate('0',34) end
	end end NameOfInsured,
	case 
	when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.EndorsementEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end EndorsementEffectiveDate,
	replicate(' ',6)ReservedForFutureUse3
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols42Record A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols42Record'
	and c.TableName='WCPols42Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols42RecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse1)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(ContingentModificationEffectiveDate)+
	max(ContingentExperienceModificationFactor)+
	max(ReservedForFutureUse2)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse3)
	,'0','')))<>''
),
EXP_DataCollect_Record42 AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPOLS_Record42
),
SQ_WCPOLS_Record43 AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse1)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(LossesSubjectToDeductibleCode)+
	max(BasisOfDeductibleCalculationCode)+
	max(DeductiblePercentage)+
	max(DeductibleAmountPerClaimAccident)+
	max(DeductibleAmountAggregate)+
	max(PremiumReductionPercentage)+
	max(ReservedForFutureUse2)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse3) From (
	Select WCPols43RecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	case 
	when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	replicate(' ',3) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier,
	case 
	when B.COLUMN_NAME='LossesSubjectToDeductibleCode' and C.FieldName='LossesSubjectToDeductibleCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LossesSubjectToDeductibleCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LossesSubjectToDeductibleCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.LossesSubjectToDeductibleCode,''),2) end
	else 
	case when B.COLUMN_NAME='LossesSubjectToDeductibleCode' and C.FieldName='LossesSubjectToDeductibleCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end LossesSubjectToDeductibleCode,
	case 
	when B.COLUMN_NAME='BasisOfDeductibleCalculationCode' and C.FieldName='BasisOfDeductibleCalculationCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BasisOfDeductibleCalculationCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BasisOfDeductibleCalculationCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.BasisOfDeductibleCalculationCode,''),2) end
	else 
	case when B.COLUMN_NAME='BasisOfDeductibleCalculationCode' and C.FieldName='BasisOfDeductibleCalculationCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end BasisOfDeductibleCalculationCode,
	case 
	when B.COLUMN_NAME='DeductiblePercentage' and C.FieldName='DeductiblePercentage' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.DeductiblePercentage or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.DeductiblePercentage,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.DeductiblePercentage,''),2) end
	else 
	case when B.COLUMN_NAME='DeductiblePercentage' and C.FieldName='DeductiblePercentage' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end DeductiblePercentage,
	case 
	when B.COLUMN_NAME='DeductibleAmountPerClaimAccident' and C.FieldName='DeductibleAmountPerClaimAccident' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.DeductibleAmountPerClaimAccident or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.DeductibleAmountPerClaimAccident,'')+replicate(' ',9),9) else Right(replicate('0',9)+ISNULL(A.DeductibleAmountPerClaimAccident,''),9) end
	else 
	case when B.COLUMN_NAME='DeductibleAmountPerClaimAccident' and C.FieldName='DeductibleAmountPerClaimAccident' then case when C.FieldDataType in ('A','AN') then replicate(' ',9) else replicate('0',9) end
	end end DeductibleAmountPerClaimAccident,
	case 
	when B.COLUMN_NAME='DeductibleAmountAggregate' and C.FieldName='DeductibleAmountAggregate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.DeductibleAmountAggregate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.DeductibleAmountAggregate,'')+replicate(' ',9),9) else Right(replicate('0',9)+ISNULL(A.DeductibleAmountAggregate,''),9) end
	else 
	case when B.COLUMN_NAME='DeductibleAmountAggregate' and C.FieldName='DeductibleAmountAggregate' then case when C.FieldDataType in ('A','AN') then replicate(' ',9) else replicate('0',9) end
	end end DeductibleAmountAggregate,
	case 
	when B.COLUMN_NAME='PremiumReductionPercentage' and C.FieldName='PremiumReductionPercentage' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.PremiumReductionPercentage or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PremiumReductionPercentage,'')+replicate(' ',5),5) else Right(replicate('0',5)+ISNULL(A.PremiumReductionPercentage,''),5) end
	else 
	case when B.COLUMN_NAME='PremiumReductionPercentage' and C.FieldName='PremiumReductionPercentage' then case when C.FieldDataType in ('A','AN') then replicate(' ',5) else replicate('0',5) end
	end end PremiumReductionPercentage,
	replicate(' ',155) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfInsured,'')+replicate(' ',34),34) else Right(replicate('0',34)+ISNULL(A.NameOfInsured,''),34) end
	else 
	case when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',34) else replicate('0',34) end
	end end NameOfInsured,
	case 
	when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.EndorsementEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end EndorsementEffectiveDate,
	replicate(' ',6)ReservedForFutureUse3
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols43Record A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols43Record'
	and c.TableName='WCPols43Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols43RecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse1)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(LossesSubjectToDeductibleCode)+
	max(BasisOfDeductibleCalculationCode)+
	max(DeductiblePercentage)+
	max(DeductibleAmountPerClaimAccident)+
	max(DeductibleAmountAggregate)+
	max(PremiumReductionPercentage)+
	max(ReservedForFutureUse2)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse3)
	,'0','')))<>''
),
EXP_DataCollect_Record43 AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPOLS_Record43
),
SQ_WCPols44Record AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(StateAbbreviation)+
	max(BasisOfAuditNoncomplianceCharge)+
	max(MaximumAuditNoncomplianceChargeMultiplier)+
	max(StateAbbreviation2)+
	max(BasisOfAuditNoncomplianceCharge2)+
	max(MaximumAuditNoncomplianceChargeMultiplier2)+
	max(StateAbbreviation3)+
	max(BasisOfAuditNoncomplianceCharge3)+
	max(MaximumAuditNoncomplianceChargeMultiplier3)+
	max(ReservedForFutureUse3)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4) From (
	Select WCPols44RecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	replicate(' ',2) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	replicate(' ',3) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier,
	case 
	when B.COLUMN_NAME='StateAbbreviation' and C.FieldName='StateAbbreviation' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateAbbreviation or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateAbbreviation,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateAbbreviation,''),2) end
	else 
	case when B.COLUMN_NAME='StateAbbreviation' and C.FieldName='StateAbbreviation' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateAbbreviation,
	case 
	when B.COLUMN_NAME='BasisOfAuditNoncomplianceCharge' and C.FieldName='BasisOfAuditNoncomplianceCharge' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BasisOfAuditNoncomplianceCharge or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BasisOfAuditNoncomplianceCharge,'')+replicate(' ',50),50) else Right(replicate('0',50)+ISNULL(A.BasisOfAuditNoncomplianceCharge,''),50) end
	else 
	case when B.COLUMN_NAME='BasisOfAuditNoncomplianceCharge' and C.FieldName='BasisOfAuditNoncomplianceCharge' then case when C.FieldDataType in ('A','AN') then replicate(' ',50) else replicate('0',50) end
	end end BasisOfAuditNoncomplianceCharge,
	case 
	when B.COLUMN_NAME='MaximumAuditNoncomplianceChargeMultiplier' and C.FieldName='MaximumAuditNoncomplianceChargeMultiplier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.MaximumAuditNoncomplianceChargeMultiplier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.MaximumAuditNoncomplianceChargeMultiplier,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.MaximumAuditNoncomplianceChargeMultiplier,''),4) end
	else 
	case when B.COLUMN_NAME='MaximumAuditNoncomplianceChargeMultiplier' and C.FieldName='MaximumAuditNoncomplianceChargeMultiplier' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end MaximumAuditNoncomplianceChargeMultiplier,
	case 
	when B.COLUMN_NAME='StateAbbreviation2' and C.FieldName='StateAbbreviation2' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateAbbreviation2 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateAbbreviation2,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateAbbreviation2,''),2) end
	else 
	case when B.COLUMN_NAME='StateAbbreviation2' and C.FieldName='StateAbbreviation2' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateAbbreviation2,
	case 
	when B.COLUMN_NAME='BasisOfAuditNoncomplianceCharge2' and C.FieldName='BasisOfAuditNoncomplianceCharge2' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BasisOfAuditNoncomplianceCharge2 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BasisOfAuditNoncomplianceCharge2,'')+replicate(' ',50),50) else Right(replicate('0',50)+ISNULL(A.BasisOfAuditNoncomplianceCharge2,''),50) end
	else 
	case when B.COLUMN_NAME='BasisOfAuditNoncomplianceCharge2' and C.FieldName='BasisOfAuditNoncomplianceCharge2' then case when C.FieldDataType in ('A','AN') then replicate(' ',50) else replicate('0',50) end
	end end BasisOfAuditNoncomplianceCharge2,
	case 
	when B.COLUMN_NAME='MaximumAuditNoncomplianceChargeMultiplier2' and C.FieldName='MaximumAuditNoncomplianceChargeMultiplier2' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.MaximumAuditNoncomplianceChargeMultiplier2 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.MaximumAuditNoncomplianceChargeMultiplier2,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.MaximumAuditNoncomplianceChargeMultiplier2,''),4) end
	else 
	case when B.COLUMN_NAME='MaximumAuditNoncomplianceChargeMultiplier2' and C.FieldName='MaximumAuditNoncomplianceChargeMultiplier2' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end MaximumAuditNoncomplianceChargeMultiplier2,
	case 
	when B.COLUMN_NAME='StateAbbreviation3' and C.FieldName='StateAbbreviation3' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateAbbreviation3 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateAbbreviation3,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateAbbreviation3,''),2) end
	else 
	case when B.COLUMN_NAME='StateAbbreviation3' and C.FieldName='StateAbbreviation3' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateAbbreviation3,
	case 
	when B.COLUMN_NAME='BasisOfAuditNoncomplianceCharge3' and C.FieldName='BasisOfAuditNoncomplianceCharge3' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BasisOfAuditNoncomplianceCharge3 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BasisOfAuditNoncomplianceCharge3,'')+replicate(' ',50),50) else Right(replicate('0',50)+ISNULL(A.BasisOfAuditNoncomplianceCharge3,''),50) end
	else 
	case when B.COLUMN_NAME='BasisOfAuditNoncomplianceCharge3' and C.FieldName='BasisOfAuditNoncomplianceCharge3' then case when C.FieldDataType in ('A','AN') then replicate(' ',50) else replicate('0',50) end
	end end BasisOfAuditNoncomplianceCharge3,
	case 
	when B.COLUMN_NAME='MaximumAuditNoncomplianceChargeMultiplier3' and C.FieldName='MaximumAuditNoncomplianceChargeMultiplier3' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.MaximumAuditNoncomplianceChargeMultiplier3 or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.MaximumAuditNoncomplianceChargeMultiplier3,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.MaximumAuditNoncomplianceChargeMultiplier3,''),4) end
	else 
	case when B.COLUMN_NAME='MaximumAuditNoncomplianceChargeMultiplier3' and C.FieldName='MaximumAuditNoncomplianceChargeMultiplier3' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end MaximumAuditNoncomplianceChargeMultiplier3,
	replicate(' ',16) ReservedForFutureUse3,
	case 
	when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfInsured,'')+replicate(' ',34),34) else Right(replicate('0',34)+ISNULL(A.NameOfInsured,''),34) end
	else 
	case when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',34) else replicate('0',34) end
	end end NameOfInsured,
	case 
	when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.EndorsementEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end EndorsementEffectiveDate,
	replicate(' ',6)ReservedForFutureUse4
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols44Record A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPols44Record'
	and c.TableName='WCPols44Record'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPols44RecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(ReservedForFutureUse1)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse2)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(StateAbbreviation)+
	max(BasisOfAuditNoncomplianceCharge)+
	max(MaximumAuditNoncomplianceChargeMultiplier)+
	max(StateAbbreviation2)+
	max(BasisOfAuditNoncomplianceCharge2)+
	max(MaximumAuditNoncomplianceChargeMultiplier2)+
	max(StateAbbreviation3)+
	max(BasisOfAuditNoncomplianceCharge3)+
	max(MaximumAuditNoncomplianceChargeMultiplier3)+
	max(ReservedForFutureUse3)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4)
	,'0','')))<>''
),
EXP_DataCollect_Record44 AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPols44Record
),
SQ_WCPolsEBRecord AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse1)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(NameOfOriginalCarrier)+
	max(NameOfNewCarrier)+
	max(ReservedForFutureUse2)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse3) From (
	Select WCPolsEBRecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	case 
	when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	replicate(' ',3) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier,
	case 
	when B.COLUMN_NAME='NameOfOriginalCarrier' and C.FieldName='NameOfOriginalCarrier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfOriginalCarrier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfOriginalCarrier,'')+replicate(' ',30),30) else Right(replicate('0',30)+ISNULL(A.NameOfOriginalCarrier,''),30) end
	else 
	case when B.COLUMN_NAME='NameOfOriginalCarrier' and C.FieldName='NameOfOriginalCarrier' then case when C.FieldDataType in ('A','AN') then replicate(' ',30) else replicate('0',30) end
	end end NameOfOriginalCarrier,
	case 
	when B.COLUMN_NAME='NameOfNewCarrier' and C.FieldName='NameOfNewCarrier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfNewCarrier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfNewCarrier,'')+replicate(' ',100),100) else Right(replicate('0',100)+ISNULL(A.NameOfNewCarrier,''),100) end
	else 
	case when B.COLUMN_NAME='NameOfNewCarrier' and C.FieldName='NameOfNewCarrier' then case when C.FieldDataType in ('A','AN') then replicate(' ',100) else replicate('0',100) end
	end end NameOfNewCarrier,
	replicate(' ',54) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfInsured,'')+replicate(' ',34),34) else Right(replicate('0',34)+ISNULL(A.NameOfInsured,''),34) end
	else 
	case when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',34) else replicate('0',34) end
	end end NameOfInsured,
	case 
	when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.EndorsementEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end EndorsementEffectiveDate,
	replicate(' ',6)ReservedForFutureUse3
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPolsEBRecord A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPolsEBRecord'
	and c.TableName='WCPolsEBRecord'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPolsEBRecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse1)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(NameOfOriginalCarrier)+
	max(NameOfNewCarrier)+
	max(ReservedForFutureUse2)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse3)
	,'0','')))<>''
),
EXP_DataCollect_RecordEB AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPolsEBRecord
),
SQ_WCPolsECRecord AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse1)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(NameOfClientOrNameOfLaborContractor)+
	max(AddressStreet)+
	max(AddressCity)+
	max(AddressState)+
	max(AddressZipcode)+
	max(FederalEmployerIdentificationNumber)+
	max(ClientPremiumAmount)+
	max(ReservedForFutureUse2)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse3) From (
	Select WCPolsECRecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	case 
	when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	replicate(' ',3) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier,
	case 
	when B.COLUMN_NAME='NameOfClientOrNameOfLaborContractor' and C.FieldName='NameOfClientOrNameOfLaborContractor' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfClientOrNameOfLaborContractor or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfClientOrNameOfLaborContractor,'')+replicate(' ',60),60) else Right(replicate('0',60)+ISNULL(A.NameOfClientOrNameOfLaborContractor,''),60) end
	else 
	case when B.COLUMN_NAME='NameOfClientOrNameOfLaborContractor' and C.FieldName='NameOfClientOrNameOfLaborContractor' then case when C.FieldDataType in ('A','AN') then replicate(' ',60) else replicate('0',60) end
	end end NameOfClientOrNameOfLaborContractor,
	case 
	when B.COLUMN_NAME='AddressStreet' and C.FieldName='AddressStreet' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressStreet or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressStreet,'')+replicate(' ',60),60) else Right(replicate('0',60)+ISNULL(A.AddressStreet,''),60) end
	else 
	case when B.COLUMN_NAME='AddressStreet' and C.FieldName='AddressStreet' then case when C.FieldDataType in ('A','AN') then replicate(' ',60) else replicate('0',60) end
	end end AddressStreet,
	case 
	when B.COLUMN_NAME='AddressCity' and C.FieldName='AddressCity' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressCity or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressCity,'')+replicate(' ',30),30) else Right(replicate('0',30)+ISNULL(A.AddressCity,''),30) end
	else 
	case when B.COLUMN_NAME='AddressCity' and C.FieldName='AddressCity' then case when C.FieldDataType in ('A','AN') then replicate(' ',30) else replicate('0',30) end
	end end AddressCity,
	case 
	when B.COLUMN_NAME='AddressState' and C.FieldName='AddressState' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressState or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressState,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.AddressState,''),2) end
	else 
	case when B.COLUMN_NAME='AddressState' and C.FieldName='AddressState' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end AddressState,
	case 
	when B.COLUMN_NAME='AddressZipcode' and C.FieldName='AddressZipcode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressZipcode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressZipcode,'')+replicate(' ',9),9) else Right(replicate('0',9)+ISNULL(A.AddressZipcode,''),9) end
	else 
	case when B.COLUMN_NAME='AddressZipcode' and C.FieldName='AddressZipcode' then case when C.FieldDataType in ('A','AN') then replicate(' ',9) else replicate('0',9) end
	end end AddressZipcode,
	case 
	when B.COLUMN_NAME='FederalEmployerIdentificationNumber' and C.FieldName='FederalEmployerIdentificationNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.FederalEmployerIdentificationNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.FederalEmployerIdentificationNumber,'')+replicate(' ',9),9) else Right(replicate('0',9)+ISNULL(A.FederalEmployerIdentificationNumber,''),9) end
	else 
	case when B.COLUMN_NAME='FederalEmployerIdentificationNumber' and C.FieldName='FederalEmployerIdentificationNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',9) else replicate('0',9) end
	end end FederalEmployerIdentificationNumber,
	case 
	when B.COLUMN_NAME='ClientPremiumAmount' and C.FieldName='ClientPremiumAmount' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ClientPremiumAmount or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ClientPremiumAmount,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.ClientPremiumAmount,''),10) end
	else 
	case when B.COLUMN_NAME='ClientPremiumAmount' and C.FieldName='ClientPremiumAmount' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end ClientPremiumAmount,
	replicate(' ',4) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfInsured,'')+replicate(' ',34),34) else Right(replicate('0',34)+ISNULL(A.NameOfInsured,''),34) end
	else 
	case when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',34) else replicate('0',34) end
	end end NameOfInsured,
	case 
	when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.EndorsementEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end EndorsementEffectiveDate,
	replicate(' ',6)ReservedForFutureUse3
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPolsECRecord A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPolsECRecord'
	and c.TableName='WCPolsECRecord'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPolsECRecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse1)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(NameOfClientOrNameOfLaborContractor)+
	max(AddressStreet)+
	max(AddressCity)+
	max(AddressState)+
	max(AddressZipcode)+
	max(FederalEmployerIdentificationNumber)+
	max(ClientPremiumAmount)+
	max(ReservedForFutureUse2)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse3)
	,'0','')))<>''
),
EXP_DataCollect_RecordEC AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPolsECRecord
),
SQ_WCPolsEDRecord AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse1)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(NameOfEmployeeLeasingCompany)+
	max(NameOfClient)+
	max(TerminationEffectiveDate)+
	max(EntitiesReceivingThisForm)+
	max(DateSent)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse2) From (
	Select WCPolsEDRecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	case 
	when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	replicate(' ',3) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier,
	case 
	when B.COLUMN_NAME='NameOfEmployeeLeasingCompany' and C.FieldName='NameOfEmployeeLeasingCompany' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfEmployeeLeasingCompany or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfEmployeeLeasingCompany,'')+replicate(' ',60),60) else Right(replicate('0',60)+ISNULL(A.NameOfEmployeeLeasingCompany,''),60) end
	else 
	case when B.COLUMN_NAME='NameOfEmployeeLeasingCompany' and C.FieldName='NameOfEmployeeLeasingCompany' then case when C.FieldDataType in ('A','AN') then replicate(' ',60) else replicate('0',60) end
	end end NameOfEmployeeLeasingCompany,
	case 
	when B.COLUMN_NAME='NameOfClient' and C.FieldName='NameOfClient' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfClient or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfClient,'')+replicate(' ',60),60) else Right(replicate('0',60)+ISNULL(A.NameOfClient,''),60) end
	else 
	case when B.COLUMN_NAME='NameOfClient' and C.FieldName='NameOfClient' then case when C.FieldDataType in ('A','AN') then replicate(' ',60) else replicate('0',60) end
	end end NameOfClient,
	case 
	when B.COLUMN_NAME='TerminationEffectiveDate' and C.FieldName='TerminationEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.TerminationEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.TerminationEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.TerminationEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='TerminationEffectiveDate' and C.FieldName='TerminationEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end TerminationEffectiveDate,
	case 
	when B.COLUMN_NAME='EntitiesReceivingThisForm' and C.FieldName='EntitiesReceivingThisForm' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EntitiesReceivingThisForm or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EntitiesReceivingThisForm,'')+replicate(' ',52),52) else Right(replicate('0',52)+ISNULL(A.EntitiesReceivingThisForm,''),52) end
	else 
	case when B.COLUMN_NAME='EntitiesReceivingThisForm' and C.FieldName='EntitiesReceivingThisForm' then case when C.FieldDataType in ('A','AN') then replicate(' ',52) else replicate('0',52) end
	end end EntitiesReceivingThisForm,
	case 
	when B.COLUMN_NAME='DateSent' and C.FieldName='DateSent' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.DateSent or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.DateSent,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.DateSent,''),6) end
	else 
	case when B.COLUMN_NAME='DateSent' and C.FieldName='DateSent' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end DateSent,
	case 
	when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfInsured,'')+replicate(' ',34),34) else Right(replicate('0',34)+ISNULL(A.NameOfInsured,''),34) end
	else 
	case when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',34) else replicate('0',34) end
	end end NameOfInsured,
	case 
	when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.EndorsementEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end EndorsementEffectiveDate,
	replicate(' ',6)ReservedForFutureUse2
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPolsEDRecord A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPolsEDRecord'
	and c.TableName='WCPolsEDRecord'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPolsEDRecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse1)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(NameOfEmployeeLeasingCompany)+
	max(NameOfClient)+
	max(TerminationEffectiveDate)+
	max(EntitiesReceivingThisForm)+
	max(DateSent)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse2)
	,'0','')))<>''
),
EXP_DataCollect_RecordED AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPolsEDRecord
),
SQ_WCPolsEIRecord AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse1)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(NameOfLaborContractor)+
	max(PolicyNumberOfLaborContractor)+
	max(FEINOfLaborContractor)+
	max(AddressOfLaborContractorStreet)+
	max(AddressOfLaborContractorCity)+
	max(AddressOfLaborContractorState)+
	max(AddressOfLaborContractorZipcode)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse2) From (
	Select WCPolsEIRecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	case 
	when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	replicate(' ',3) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier,
	case 
	when B.COLUMN_NAME='NameOfLaborContractor' and C.FieldName='NameOfLaborContractor' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfLaborContractor or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfLaborContractor,'')+replicate(' ',60),60) else Right(replicate('0',60)+ISNULL(A.NameOfLaborContractor,''),60) end
	else 
	case when B.COLUMN_NAME='NameOfLaborContractor' and C.FieldName='NameOfLaborContractor' then case when C.FieldDataType in ('A','AN') then replicate(' ',60) else replicate('0',60) end
	end end NameOfLaborContractor,
	case 
	when B.COLUMN_NAME='PolicyNumberOfLaborContractor' and C.FieldName='PolicyNumberOfLaborContractor' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.PolicyNumberOfLaborContractor or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.PolicyNumberOfLaborContractor,'')+replicate(' ',18),18) else Right(replicate('0',18)+ISNULL(A.PolicyNumberOfLaborContractor,''),18) end
	else 
	case when B.COLUMN_NAME='PolicyNumberOfLaborContractor' and C.FieldName='PolicyNumberOfLaborContractor' then case when C.FieldDataType in ('A','AN') then replicate(' ',18) else replicate('0',18) end
	end end PolicyNumberOfLaborContractor,
	case 
	when B.COLUMN_NAME='FEINOfLaborContractor' and C.FieldName='FEINOfLaborContractor' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.FEINOfLaborContractor or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.FEINOfLaborContractor,'')+replicate(' ',9),9) else Right(replicate('0',9)+ISNULL(A.FEINOfLaborContractor,''),9) end
	else 
	case when B.COLUMN_NAME='FEINOfLaborContractor' and C.FieldName='FEINOfLaborContractor' then case when C.FieldDataType in ('A','AN') then replicate(' ',9) else replicate('0',9) end
	end end FEINOfLaborContractor,
	case 
	when B.COLUMN_NAME='AddressOfLaborContractorStreet' and C.FieldName='AddressOfLaborContractorStreet' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressOfLaborContractorStreet or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressOfLaborContractorStreet,'')+replicate(' ',56),56) else Right(replicate('0',56)+ISNULL(A.AddressOfLaborContractorStreet,''),56) end
	else 
	case when B.COLUMN_NAME='AddressOfLaborContractorStreet' and C.FieldName='AddressOfLaborContractorStreet' then case when C.FieldDataType in ('A','AN') then replicate(' ',56) else replicate('0',56) end
	end end AddressOfLaborContractorStreet,
	case 
	when B.COLUMN_NAME='AddressOfLaborContractorCity' and C.FieldName='AddressOfLaborContractorCity' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressOfLaborContractorCity or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressOfLaborContractorCity,'')+replicate(' ',30),30) else Right(replicate('0',30)+ISNULL(A.AddressOfLaborContractorCity,''),30) end
	else 
	case when B.COLUMN_NAME='AddressOfLaborContractorCity' and C.FieldName='AddressOfLaborContractorCity' then case when C.FieldDataType in ('A','AN') then replicate(' ',30) else replicate('0',30) end
	end end AddressOfLaborContractorCity,
	case 
	when B.COLUMN_NAME='AddressOfLaborContractorState' and C.FieldName='AddressOfLaborContractorState' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressOfLaborContractorState or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressOfLaborContractorState,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.AddressOfLaborContractorState,''),2) end
	else 
	case when B.COLUMN_NAME='AddressOfLaborContractorState' and C.FieldName='AddressOfLaborContractorState' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end AddressOfLaborContractorState,
	case 
	when B.COLUMN_NAME='AddressOfLaborContractorZipcode' and C.FieldName='AddressOfLaborContractorZipcode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressOfLaborContractorZipcode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressOfLaborContractorZipcode,'')+replicate(' ',9),9) else Right(replicate('0',9)+ISNULL(A.AddressOfLaborContractorZipcode,''),9) end
	else 
	case when B.COLUMN_NAME='AddressOfLaborContractorZipcode' and C.FieldName='AddressOfLaborContractorZipcode' then case when C.FieldDataType in ('A','AN') then replicate(' ',9) else replicate('0',9) end
	end end AddressOfLaborContractorZipcode,
	
	case 
	when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfInsured or D.WCPOLSCode is null) then
	left(ISNULL(A.NameOfInsured,'')+replicate(' ',34),34)
	else 
	case when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',34) else replicate('0',34) end
	end end NameOfInsured,
	case 
	when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.EndorsementEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end EndorsementEffectiveDate,
	replicate(' ',6)ReservedForFutureUse2
	from dbo.WCPolsEIRecord A
	inner join dbo.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join dbo.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPolsEIRecord'
	and c.TableName='WCPolsEIRecord'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPolsEIRecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse1)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(NameOfLaborContractor)+
	max(PolicyNumberOfLaborContractor)+
	max(FEINOfLaborContractor)+
	max(AddressOfLaborContractorStreet)+
	max(AddressOfLaborContractorCity)+
	max(AddressOfLaborContractorState)+
	max(AddressOfLaborContractorZipcode)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse2)
	,'0','')))<>''
),
EXP_DataCollect_RecordEI AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPolsEIRecord
),
SQ_WCPolsHARecord AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse1)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(NameOfIndependentContractor)+
	max(ClassificationCode)+
	max(ClassificationWording)+
	max(EstimatedExposureAmount)+
	max(RateChargedRate)+
	max(MinimumPremiumAmount)+
	max(EstimatedAnnualPremiumAmount)+
	max(ReservedForFutureUse2)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse3) From (
	Select WCPolsHARecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	case 
	when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	replicate(' ',3) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier,
	case 
	when B.COLUMN_NAME='NameOfIndependentContractor' and C.FieldName='NameOfIndependentContractor' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfIndependentContractor or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfIndependentContractor,'')+replicate(' ',90),90) else Right(replicate('0',90)+ISNULL(A.NameOfIndependentContractor,''),90) end
	else 
	case when B.COLUMN_NAME='NameOfIndependentContractor' and C.FieldName='NameOfIndependentContractor' then case when C.FieldDataType in ('A','AN') then replicate(' ',90) else replicate('0',90) end
	end end NameOfIndependentContractor,
	case 
	when B.COLUMN_NAME='ClassificationCode' and C.FieldName='ClassificationCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ClassificationCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ClassificationCode,'')+replicate(' ',4),4) else Right(replicate('0',4)+ISNULL(A.ClassificationCode,''),4) end
	else 
	case when B.COLUMN_NAME='ClassificationCode' and C.FieldName='ClassificationCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',4) else replicate('0',4) end
	end end ClassificationCode,
	case 
	when B.COLUMN_NAME='ClassificationWording' and C.FieldName='ClassificationWording' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ClassificationWording or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ClassificationWording,'')+replicate(' ',47),47) else Right(replicate('0',47)+ISNULL(A.ClassificationWording,''),47) end
	else 
	case when B.COLUMN_NAME='ClassificationWording' and C.FieldName='ClassificationWording' then case when C.FieldDataType in ('A','AN') then replicate(' ',47) else replicate('0',47) end
	end end ClassificationWording,
	case 
	when B.COLUMN_NAME='EstimatedExposureAmount' and C.FieldName='EstimatedExposureAmount' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EstimatedExposureAmount or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EstimatedExposureAmount,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.EstimatedExposureAmount,''),10) end
	else 
	case when B.COLUMN_NAME='EstimatedExposureAmount' and C.FieldName='EstimatedExposureAmount' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end EstimatedExposureAmount,
	case 
	when B.COLUMN_NAME='RateChargedRate' and C.FieldName='RateChargedRate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RateChargedRate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RateChargedRate,'')+replicate(' ',7),7) else Right(replicate('0',7)+ISNULL(A.RateChargedRate,''),7) end
	else 
	case when B.COLUMN_NAME='RateChargedRate' and C.FieldName='RateChargedRate' then case when C.FieldDataType in ('A','AN') then replicate(' ',7) else replicate('0',7) end
	end end RateChargedRate,
	case 
	when B.COLUMN_NAME='MinimumPremiumAmount' and C.FieldName='MinimumPremiumAmount' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.MinimumPremiumAmount or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.MinimumPremiumAmount,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.MinimumPremiumAmount,''),10) end
	else 
	case when B.COLUMN_NAME='MinimumPremiumAmount' and C.FieldName='MinimumPremiumAmount' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end MinimumPremiumAmount,
	case 
	when B.COLUMN_NAME='EstimatedAnnualPremiumAmount' and C.FieldName='EstimatedAnnualPremiumAmount' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EstimatedAnnualPremiumAmount or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EstimatedAnnualPremiumAmount,'')+replicate(' ',10),10) else Right(replicate('0',10)+ISNULL(A.EstimatedAnnualPremiumAmount,''),10) end
	else 
	case when B.COLUMN_NAME='EstimatedAnnualPremiumAmount' and C.FieldName='EstimatedAnnualPremiumAmount' then case when C.FieldDataType in ('A','AN') then replicate(' ',10) else replicate('0',10) end
	end end EstimatedAnnualPremiumAmount,
	replicate(' ',6) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfInsured,'')+replicate(' ',34),34) else Right(replicate('0',34)+ISNULL(A.NameOfInsured,''),34) end
	else 
	case when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',34) else replicate('0',34) end
	end end NameOfInsured,
	case 
	when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.EndorsementEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end EndorsementEffectiveDate,
	replicate(' ',6)ReservedForFutureUse3
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPolsHARecord A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPolsHARecord'
	and c.TableName='WCPolsHARecord'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPolsHARecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse1)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(NameOfIndependentContractor)+
	max(ClassificationCode)+
	max(ClassificationWording)+
	max(EstimatedExposureAmount)+
	max(RateChargedRate)+
	max(MinimumPremiumAmount)+
	max(EstimatedAnnualPremiumAmount)+
	max(ReservedForFutureUse2)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse3)
	,'0','')))<>''
),
EXP_DataCollect_RecordHA AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPolsHARecord
),
SQ_WCPolsHCRecord AS (
	Select max(LinkData) LinkData,
	max(RecordTypeCode) RecordTypeCode,
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse1)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(NameOfClient)+
	max(LeasingAddressTypeCode)+
	max(AddressStreet)+
	max(AddressCity)+
	max(AddressState)+
	max(AddressZipCode)+
	max(ReservedForFutureUse2)+
	max(ClientFederalEmployerIdentificationNumber)+
	max(ClientsUnemploymentInsuranceNumber)+
	max(ReservedForFutureUse3)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4) From (
	Select WCPolsHCRecordID, 
	A.WCTrackHistoryID,
	case 
	when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LinkData or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LinkData,'')+replicate(' ',43),43) else Right(replicate('0',43)+ISNULL(A.LinkData,''),43) end
	else 
	case when B.COLUMN_NAME='LinkData' and C.FieldName='LinkData' then case when C.FieldDataType in ('A','AN') then replicate(' ',43) else replicate('0',43) end
	end end LinkData,
	case 
	when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.StateCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.StateCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.StateCode,''),2) end
	else 
	case when B.COLUMN_NAME='StateCode' and C.FieldName='StateCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end StateCode,
	case 
	when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.RecordTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.RecordTypeCode,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.RecordTypeCode,''),2) end
	else 
	case when B.COLUMN_NAME='RecordTypeCode' and C.FieldName='RecordTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end RecordTypeCode,
	replicate(' ',3) ReservedForFutureUse1,
	case 
	when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementNumber,'')+replicate(' ',8),8) else Right(replicate('0',8)+ISNULL(A.EndorsementNumber,''),8) end
	else 
	case when B.COLUMN_NAME='EndorsementNumber' and C.FieldName='EndorsementNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',8) else replicate('0',8) end
	end end EndorsementNumber,
	case 
	when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.BureauVersionIdentifierEditionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.BureauVersionIdentifierEditionIdentifier,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.BureauVersionIdentifierEditionIdentifier,''),1) end
	else 
	case when B.COLUMN_NAME='BureauVersionIdentifierEditionIdentifier' and C.FieldName='BureauVersionIdentifierEditionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end BureauVersionIdentifierEditionIdentifier,
	case 
	when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.CarrierVersionIdentifier or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.CarrierVersionIdentifier,'')+replicate(' ',11),11) else Right(replicate('0',11)+ISNULL(A.CarrierVersionIdentifier,''),11) end
	else 
	case when B.COLUMN_NAME='CarrierVersionIdentifier' and C.FieldName='CarrierVersionIdentifier' then case when C.FieldDataType in ('A','AN') then replicate(' ',11) else replicate('0',11) end
	end end CarrierVersionIdentifier,
	case 
	when B.COLUMN_NAME='NameOfClient' and C.FieldName='NameOfClient' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfClient or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfClient,'')+replicate(' ',50),50) else Right(replicate('0',50)+ISNULL(A.NameOfClient,''),50) end
	else 
	case when B.COLUMN_NAME='NameOfClient' and C.FieldName='NameOfClient' then case when C.FieldDataType in ('A','AN') then replicate(' ',50) else replicate('0',50) end
	end end NameOfClient,
	case 
	when B.COLUMN_NAME='LeasingAddressTypeCode' and C.FieldName='LeasingAddressTypeCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.LeasingAddressTypeCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.LeasingAddressTypeCode,'')+replicate(' ',1),1) else Right(replicate('0',1)+ISNULL(A.LeasingAddressTypeCode,''),1) end
	else 
	case when B.COLUMN_NAME='LeasingAddressTypeCode' and C.FieldName='LeasingAddressTypeCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',1) else replicate('0',1) end
	end end LeasingAddressTypeCode,
	case 
	when B.COLUMN_NAME='AddressStreet' and C.FieldName='AddressStreet' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressStreet or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressStreet,'')+replicate(' ',60),60) else Right(replicate('0',60)+ISNULL(A.AddressStreet,''),60) end
	else 
	case when B.COLUMN_NAME='AddressStreet' and C.FieldName='AddressStreet' then case when C.FieldDataType in ('A','AN') then replicate(' ',60) else replicate('0',60) end
	end end AddressStreet,
	case 
	when B.COLUMN_NAME='AddressCity' and C.FieldName='AddressCity' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressCity or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressCity,'')+replicate(' ',30),30) else Right(replicate('0',30)+ISNULL(A.AddressCity,''),30) end
	else 
	case when B.COLUMN_NAME='AddressCity' and C.FieldName='AddressCity' then case when C.FieldDataType in ('A','AN') then replicate(' ',30) else replicate('0',30) end
	end end AddressCity,
	case 
	when B.COLUMN_NAME='AddressState' and C.FieldName='AddressState' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressState or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressState,'')+replicate(' ',2),2) else Right(replicate('0',2)+ISNULL(A.AddressState,''),2) end
	else 
	case when B.COLUMN_NAME='AddressState' and C.FieldName='AddressState' then case when C.FieldDataType in ('A','AN') then replicate(' ',2) else replicate('0',2) end
	end end AddressState,
	case 
	when B.COLUMN_NAME='AddressZipCode' and C.FieldName='AddressZipCode' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.AddressZipCode or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.AddressZipCode,'')+replicate(' ',9),9) else Right(replicate('0',9)+ISNULL(A.AddressZipCode,''),9) end
	else 
	case when B.COLUMN_NAME='AddressZipCode' and C.FieldName='AddressZipCode' then case when C.FieldDataType in ('A','AN') then replicate(' ',9) else replicate('0',9) end
	end end AddressZipCode,
	replicate(' ',2) ReservedForFutureUse2,
	case 
	when B.COLUMN_NAME='ClientFederalEmployerIdentificationNumber' and C.FieldName='ClientFederalEmployerIdentificationNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ClientFederalEmployerIdentificationNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ClientFederalEmployerIdentificationNumber,'')+replicate(' ',9),9) else Right(replicate('0',9)+ISNULL(A.ClientFederalEmployerIdentificationNumber,''),9) end
	else 
	case when B.COLUMN_NAME='ClientFederalEmployerIdentificationNumber' and C.FieldName='ClientFederalEmployerIdentificationNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',9) else replicate('0',9) end
	end end ClientFederalEmployerIdentificationNumber,
	case 
	when B.COLUMN_NAME='ClientsUnemploymentInsuranceNumber' and C.FieldName='ClientsUnemploymentInsuranceNumber' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.ClientsUnemploymentInsuranceNumber or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.ClientsUnemploymentInsuranceNumber,'')+replicate(' ',15),15) else Right(replicate('0',15)+ISNULL(A.ClientsUnemploymentInsuranceNumber,''),15) end
	else 
	case when B.COLUMN_NAME='ClientsUnemploymentInsuranceNumber' and C.FieldName='ClientsUnemploymentInsuranceNumber' then case when C.FieldDataType in ('A','AN') then replicate(' ',15) else replicate('0',15) end
	end end ClientsUnemploymentInsuranceNumber,
	replicate(' ',6) ReservedForFutureUse3,
	case 
	when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.NameOfInsured or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.NameOfInsured,'')+replicate(' ',34),34) else Right(replicate('0',34)+ISNULL(A.NameOfInsured,''),34) end
	else 
	case when B.COLUMN_NAME='NameOfInsured' and C.FieldName='NameOfInsured' then case when C.FieldDataType in ('A','AN') then replicate(' ',34) else replicate('0',34) end
	end end NameOfInsured,
	case 
	when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' and F.TransactionCode=D.WCPOLSTransactionType and E.TransactionType=D.SourceTransactionType  and (D.WCPOLSCode=A.EndorsementEffectiveDate or D.WCPOLSCode is null) then
	case when C.FieldDataType in ('A','AN') then left(ISNULL(A.EndorsementEffectiveDate,'')+replicate(' ',6),6) else Right(replicate('0',6)+ISNULL(A.EndorsementEffectiveDate,''),6) end
	else 
	case when B.COLUMN_NAME='EndorsementEffectiveDate' and C.FieldName='EndorsementEffectiveDate' then case when C.FieldDataType in ('A','AN') then replicate(' ',6) else replicate('0',6) end
	end end EndorsementEffectiveDate,
	replicate(' ',6)ReservedForFutureUse4
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPolsHCRecord A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory  E
	on A.WCTrackHistoryID=E.WCTrackHistoryID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols00Record F
	on A.WCTrackHistoryID=F.WCTrackHistoryID
	inner join INFORMATION_SCHEMA.COLUMNS B
	on 1=1
	inner join SupWCPOLSFieldNeeded C
	on B.TABLE_NAME=C.TableName
	and B.COLUMN_NAME=C.FieldName
	Left join SUPWCPOLSAllCombinations D
	on B.TABLE_NAME=D.TableName
	and case when B.COLUMN_NAME like 'StateCode%' and B.COLUMN_NAME not in ('StateCodeLink') then 'StateCode' else B.COLUMN_NAME end=D.FieldName
	and D.FinalFileName='@{pipeline().parameters.FILENAME}'
	where B.TABLE_NAME='WCPolsHCRecord'
	and c.TableName='WCPolsHCRecord'
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and E.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and F.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and (('@{pipeline().parameters.FILENAME}'='NCCI' and E.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and E.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and E.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and E.MNRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='NC' and E.NCRequiredFlag=1))
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	group by A.WCPolsHCRecordID, A.WCTrackHistoryID
	having ltrim(rtrim(replace(
	max(LinkData)+
	max(StateCode)+
	max(RecordTypeCode)+
	max(ReservedForFutureUse1)+
	max(EndorsementNumber)+
	max(BureauVersionIdentifierEditionIdentifier)+
	max(CarrierVersionIdentifier)+
	max(NameOfClient)+
	max(LeasingAddressTypeCode)+
	max(AddressStreet)+
	max(AddressCity)+
	max(AddressState)+
	max(AddressZipCode)+
	max(ReservedForFutureUse2)+
	max(ClientFederalEmployerIdentificationNumber)+
	max(ClientsUnemploymentInsuranceNumber)+
	max(ReservedForFutureUse3)+
	max(NameOfInsured)+
	max(EndorsementEffectiveDate)+
	max(ReservedForFutureUse4)
	,'0','')))<>''
),
EXP_DataCollect_RecordHC AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data
	FROM SQ_WCPolsHCRecord
),
U_Records AS (
	SELECT LinkData, RecordTypeCode, Data, WCTrackHistoryID
	FROM EXP_DataCollect_Record01
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_Record02
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_Record03
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_Record04
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_Record05
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_Record06
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_Record07
	UNION
	SELECT LinkData, RecordTypeCode, Data, WCTrackHistoryID
	FROM EXP_DataCollect_Record08
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_Record09
	UNION
	SELEC
	FROM 
	UNION
	SELEC
	FROM 
	UNION
	SELEC
	FROM 
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_Record21
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_Record42
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_Record43
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_Record03_INIAMO
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_Record15
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_Record18
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_Record19
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_Record23
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_Record24
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_Record25
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_Record29
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_Record30
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_Record36
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_Record37
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_Record38
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_Record40
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_Record44
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_RecordEB
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_RecordEC
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_RecordED
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_RecordHA
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_RecordHC
	UNION
	SELECT LinkData, RecordTypeCode, Data
	FROM EXP_DataCollect_RecordEI
),
EXP_RecordCount AS (
	SELECT
	LinkData,
	RecordTypeCode,
	Data,
	-- *INF*: ReplaceChr( 0, Data, '[]', chr(39))
	-- 
	-- --REPLACESTR(1,Data ,chr(96) ,chr(39))
	REGEXP_REPLACE(Data,'[]',chr(39),'i') AS o_Data,
	WCTrackHistoryID,
	-- *INF*: IIF(RecordTypeCode='01',1+v_HeaderCount,v_HeaderCount)
	IFF(RecordTypeCode = '01', 1 + v_HeaderCount, v_HeaderCount) AS v_HeaderCount,
	v_HeaderCount AS HeaderCount,
	1+v_RecordCount AS v_RecordCount,
	-- *INF*: DECODE(TRUE,
	-- IN(@{pipeline().parameters.FILENAME},'MI','WI','MN','NC'),v_RecordCount+1,
	-- v_RecordCount)
	DECODE(
	    TRUE,
	    @{pipeline().parameters.FILENAME} IN ('MI','WI','MN','NC'), v_RecordCount + 1,
	    v_RecordCount
	) AS RecordCount,
	1 AS Srt
	FROM U_Records
),
LKP_TransactionDate AS (
	SELECT
	TransactionDate,
	LinkData
	FROM (
		select 
		B.TransactionDate as TransactionDate, 
		A.WCTrackHistoryID as WCTrackHistoryID,
		A.LinkData as LinkData
		FROM 
		WCPols00Record A inner join WorkWCPolicy B on 
			A.WCTrackHistoryID=B.WCTrackHistoryID
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY LinkData ORDER BY TransactionDate) = 1
),
AGG_RecordCounts AS (
	SELECT
	EXP_RecordCount.HeaderCount,
	-- *INF*: max(HeaderCount)
	max(HeaderCount) AS o_HeaderCount,
	EXP_RecordCount.RecordCount,
	-- *INF*: max(RecordCount)
	max(RecordCount) AS o_RecordCount,
	LKP_TransactionDate.TransactionDate,
	-- *INF*: MIN(TransactionDate)
	MIN(TransactionDate) AS MinTransactionDate,
	-- *INF*: Max(TransactionDate)
	Max(TransactionDate) AS MaxTransactionDate,
	1 AS JoinField
	FROM EXP_RecordCount
	LEFT JOIN LKP_TransactionDate
	ON LKP_TransactionDate.LinkData = EXP_RecordCount.LinkData
	GROUP BY 
),
SQ_SupWCPOLS AS (
	SELECT TOP 1 SupWCPOLS.SupWCPOLSID 
	FROM
	 SupWCPOLS
),
EXP_Record99_Input AS (
	SELECT
	SupWCPOLSID,
	1 AS JoinField
	FROM SQ_SupWCPOLS
),
JNR_ForceRecord99 AS (SELECT
	AGG_RecordCounts.o_HeaderCount, 
	AGG_RecordCounts.o_RecordCount, 
	AGG_RecordCounts.MinTransactionDate, 
	AGG_RecordCounts.MaxTransactionDate, 
	AGG_RecordCounts.JoinField, 
	EXP_Record99_Input.JoinField AS JoinField1
	FROM AGG_RecordCounts
	RIGHT OUTER JOIN EXP_Record99_Input
	ON EXP_Record99_Input.JoinField = AGG_RecordCounts.JoinField
),
EXP_Record99 AS (
	SELECT
	o_HeaderCount AS i_HeaderCount,
	o_RecordCount AS i_RecordCount,
	-- *INF*: IIF(ISNULL(i_HeaderCount),0,i_HeaderCount)
	IFF(i_HeaderCount IS NULL, 0, i_HeaderCount) AS v_HeaderCount,
	-- *INF*: IIF(ISNULL(i_RecordCount),0,i_RecordCount)
	IFF(i_RecordCount IS NULL, 0, i_RecordCount) AS v_RecordCount,
	MinTransactionDate AS i_MinTransactionDate,
	MaxTransactionDate AS i_MaxTransactionDate,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_MinTransactionDate), TO_CHAR(SESSSTARTTIME,'YYYYMMDD'),
	-- TO_CHAR(i_MinTransactionDate,'YYYYMMDD')
	-- )
	DECODE(
	    TRUE,
	    i_MinTransactionDate IS NULL, TO_CHAR(SESSSTARTTIME, 'YYYYMMDD'),
	    TO_CHAR(i_MinTransactionDate, 'YYYYMMDD')
	) AS v_MinTransactionDate,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_MaxTransactionDate),TO_CHAR(SESSSTARTTIME,'YYYYMMDD'),
	-- TO_CHAR(i_MaxTransactionDate,'YYYYMMDD')
	-- )
	DECODE(
	    TRUE,
	    i_MaxTransactionDate IS NULL, TO_CHAR(SESSSTARTTIME, 'YYYYMMDD'),
	    TO_CHAR(i_MaxTransactionDate, 'YYYYMMDD')
	) AS v_MaxTransactionDate,
	-- *INF*: lpad('',45,' ')
	lpad('', 45, ' ') AS v_LinkData,
	v_LinkData AS LinkData,
	'99' AS v_RecordTypeCode,
	v_RecordTypeCode AS RecordTypeCode,
	-- *INF*: DECODE(TRUE,
	-- @{pipeline().parameters.FILENAME} = 'NCCI',v_LinkData||v_RecordTypeCode||lpad(to_char(v_RecordCount),10,'0')||lpad(to_char(v_HeaderCount),8,'0')||lpad('',235,' '),
	--  v_LinkData||v_RecordTypeCode||lpad(to_char(v_RecordCount),10,'0')||lpad(to_char(v_HeaderCount),8,'0')||v_MinTransactionDate||v_MaxTransactionDate||lpad('',219,' ')
	-- )
	-- 
	-- 
	-- -- if NCCI then no min/max date else add min max date and decrease padding at the end
	DECODE(
	    TRUE,
	    @{pipeline().parameters.FILENAME} = 'NCCI', v_LinkData || v_RecordTypeCode || lpad(to_char(v_RecordCount), 10, '0') || lpad(to_char(v_HeaderCount), 8, '0') || lpad('', 235, ' '),
	    v_LinkData || v_RecordTypeCode || lpad(to_char(v_RecordCount), 10, '0') || lpad(to_char(v_HeaderCount), 8, '0') || v_MinTransactionDate || v_MaxTransactionDate || lpad('', 219, ' ')
	) AS Data,
	2 AS Srt
	FROM JNR_ForceRecord99
),
EXP_ETR AS (
	SELECT
	o_HeaderCount AS IN_RecordCount,
	-- *INF*: lpad('',45,' ')
	lpad('', 45, ' ') AS o_LinkData,
	'00' AS o_RecordTypeCode,
	-- *INF*: LPAD('$!+WORKCOMP+!$',14,' ')
	LPAD('$!+WORKCOMP+!$', 14, ' ') AS v_Label,
	-- *INF*: LPAD('CDXSubmission@WBMI.com',31,' ')
	LPAD('CDXSubmission@WBMI.com', 31, ' ') AS v_DataProviderContactEmailAddress,
	'  ' AS v_RecordTypeCode,
	'WCP' AS v_DataTypeCode,
	-- *INF*: DECODE(TRUE,
	-- @{pipeline().parameters.FILENAME}='WI','00048',
	-- @{pipeline().parameters.FILENAME}='MI','00021',
	-- @{pipeline().parameters.FILENAME}='MN','00022',
	-- @{pipeline().parameters.FILENAME}='NC','00032',
	-- LPAD('',5,' ')
	-- )
	DECODE(
	    TRUE,
	    @{pipeline().parameters.FILENAME} = 'WI', '00048',
	    @{pipeline().parameters.FILENAME} = 'MI', '00021',
	    @{pipeline().parameters.FILENAME} = 'MN', '00022',
	    @{pipeline().parameters.FILENAME} = 'NC', '00032',
	    LPAD('', 5, ' ')
	) AS v_DataReceiverCode,
	-- *INF*: SET_DATE_PART(SYSDATE,'MM',1)
	DATEADD(MONTH,1-DATE_PART(MONTH,CURRENT_TIMESTAMP),CURRENT_TIMESTAMP) AS v_Month,
	-- *INF*: SET_DATE_PART(v_Month,'DD',1)
	DATEADD(DAY,1-DATE_PART(DAY,v_Month),v_Month) AS v_Date,
	-- *INF*: SUBSTR(GET_DATE_PART(SYSDATE,'YY'),3,2)||LPAD(DATE_DIFF(SYSDATE,v_Date,'DDD')+1,3,'0')||'V'||@{pipeline().parameters.TRANSMISSION_NUMBER}
	SUBSTR(DATE_PART(CURRENT_TIMESTAMP, 'YY'), 3, 2) || LPAD(DATEDIFF(DAY,CURRENT_TIMESTAMP,v_Date) + 1, 3, '0') || 'V' || @{pipeline().parameters.TRANSMISSION_NUMBER} AS v_TransmissionVersionIdentifier,
	-- *INF*: LPAD(@{pipeline().parameters.SUBMISSION_TYPE_CODE},1,' ')
	-- 
	-- --typically will be T for test or S for standard; R used for resubmissions after file rejections
	LPAD(@{pipeline().parameters.SUBMISSION_TYPE_CODE}, 1, ' ') AS v_SubmissionTypeCode,
	-- *INF*: LPAD(@{pipeline().parameters.SUBMISSION_REPLACEMENT_IDENTIFIER},8,' ')
	LPAD(@{pipeline().parameters.SUBMISSION_REPLACEMENT_IDENTIFIER}, 8, ' ') AS v_SubmissionReplacementIdentifier,
	'17124' AS v_DataProviderCode,
	-- *INF*: LPAD('Sheila Smiley',25,' ')
	LPAD('Sheila Smiley', 25, ' ') AS v_NameOfDataProviderContact,
	' ' AS v_ReservedForFutureUse,
	'2623385152' AS v_PhoneNumber,
	-- *INF*: LPAD('',6,' ')
	LPAD('', 6, ' ') AS v_PhoneNumberExtension,
	-- *INF*: LPAD('',10,' ')
	LPAD('', 10, ' ') AS v_FaxNumber,
	-- *INF*: TO_CHAR(SYSDATE,'YYYYMMDD')
	TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDD') AS v_ProcessedDate,
	-- *INF*: LPAD('1900 S 18th Avenue',60,' ')
	LPAD('1900 S 18th Avenue', 60, ' ') AS v_AddressOfContactStreet,
	-- *INF*: LPAD('West Bend',30,' ')
	LPAD('West Bend', 30, ' ') AS v_AddressOfContactCity,
	'WI' AS v_AddressOfContactState,
	-- *INF*: LPAD('53095',9,' ')
	LPAD('53095', 9, ' ') AS v_AddressOfContactZipCode,
	'C' AS v_DataProviderTypeCode,
	-- *INF*: LPAD('',9,' ')
	LPAD('', 9, ' ') AS v_ThirdPartyEntityFederalEmployerIdentificationNumber,
	' ' AS v_ReservedForFutureUse2,
	-- *INF*: LPAD('',101,' ')
	LPAD('', 101, ' ') AS v_ReservedForFutureUse3,
	-- *INF*: SUBSTR(v_Label||v_DataProviderContactEmailAddress||v_RecordTypeCode||v_DataTypeCode||v_DataReceiverCode||v_TransmissionVersionIdentifier||v_SubmissionTypeCode||v_SubmissionReplacementIdentifier||v_DataProviderCode||v_NameOfDataProviderContact||v_ReservedForFutureUse||v_PhoneNumber||v_PhoneNumberExtension||v_FaxNumber||v_ProcessedDate||v_AddressOfContactStreet||v_AddressOfContactCity||v_AddressOfContactState||v_AddressOfContactZipCode||v_DataProviderTypeCode||v_ThirdPartyEntityFederalEmployerIdentificationNumber||v_ReservedForFutureUse2||v_ReservedForFutureUse3,1,300)
	-- 
	-- 
	SUBSTR(v_Label || v_DataProviderContactEmailAddress || v_RecordTypeCode || v_DataTypeCode || v_DataReceiverCode || v_TransmissionVersionIdentifier || v_SubmissionTypeCode || v_SubmissionReplacementIdentifier || v_DataProviderCode || v_NameOfDataProviderContact || v_ReservedForFutureUse || v_PhoneNumber || v_PhoneNumberExtension || v_FaxNumber || v_ProcessedDate || v_AddressOfContactStreet || v_AddressOfContactCity || v_AddressOfContactState || v_AddressOfContactZipCode || v_DataProviderTypeCode || v_ThirdPartyEntityFederalEmployerIdentificationNumber || v_ReservedForFutureUse2 || v_ReservedForFutureUse3, 1, 300) AS o_DATA,
	0 AS o_Srt
	FROM JNR_ForceRecord99
),
FIL_ETR_RECORD AS (
	SELECT
	o_LinkData AS LinkData, 
	o_RecordTypeCode AS RecordTypeCode, 
	o_DATA AS DATA, 
	o_Srt AS Srt
	FROM EXP_ETR
	WHERE IN(@{pipeline().parameters.FILENAME},'MI','WI','MN','NC')
),
U_Data AS (
	SELECT LinkData, RecordTypeCode, o_Data AS Data, Srt
	FROM EXP_RecordCount
	UNION
	SELECT LinkData, RecordTypeCode, Data, Srt
	FROM EXP_Record99
	UNION
	SELECT LinkData, RecordTypeCode, DATA AS Data, Srt
	FROM FIL_ETR_RECORD
),
SRT_Data AS (
	SELECT
	Srt, 
	LinkData, 
	RecordTypeCode, 
	Data
	FROM U_Data
	ORDER BY Srt ASC, LinkData ASC, RecordTypeCode ASC
),
EXP_FileName AS (
	SELECT
	Data,
	-- *INF*: @{pipeline().parameters.FILENAME_PREFIX}||lpad('21',5,'0')||'_'||to_char(SESSSTARTTIME,'YYYYMMDDHH24MI')||'.'||@{pipeline().parameters.FILE_EXTENSION}
	@{pipeline().parameters.FILENAME_PREFIX} || lpad('21', 5, '0') || '_' || to_char(SESSSTARTTIME, 'YYYYMMDDHH24MI') || '.' || @{pipeline().parameters.FILE_EXTENSION} AS v_MI_FileName,
	-- *INF*: @{pipeline().parameters.FILENAME_PREFIX}||lpad('48',5,'0')||'_'||to_char(SESSSTARTTIME,'YYYYMMDDHH24MI')||'.'||@{pipeline().parameters.FILE_EXTENSION}
	@{pipeline().parameters.FILENAME_PREFIX} || lpad('48', 5, '0') || '_' || to_char(SESSSTARTTIME, 'YYYYMMDDHH24MI') || '.' || @{pipeline().parameters.FILE_EXTENSION} AS v_WI_FileName,
	-- *INF*: @{pipeline().parameters.FILENAME_PREFIX}||lpad('22',5,'0')||'_'||to_char(SESSSTARTTIME,'YYYYMMDDHH24MI')||'.'||@{pipeline().parameters.FILE_EXTENSION}
	@{pipeline().parameters.FILENAME_PREFIX} || lpad('22', 5, '0') || '_' || to_char(SESSSTARTTIME, 'YYYYMMDDHH24MI') || '.' || @{pipeline().parameters.FILE_EXTENSION} AS v_MN_FileName,
	-- *INF*: @{pipeline().parameters.FILENAME_PREFIX}||lpad('32',5,'0')||'_'||to_char(SESSSTARTTIME,'YYYYMMDDHH24MI')||'.'||@{pipeline().parameters.FILE_EXTENSION}
	@{pipeline().parameters.FILENAME_PREFIX} || lpad('32', 5, '0') || '_' || to_char(SESSSTARTTIME, 'YYYYMMDDHH24MI') || '.' || @{pipeline().parameters.FILE_EXTENSION} AS v_NC_FileName,
	-- *INF*: @{pipeline().parameters.FILENAME_PREFIX}||to_char(SESSSTARTTIME,'MMDDYYYY')||'.'||@{pipeline().parameters.FILE_EXTENSION}
	@{pipeline().parameters.FILENAME_PREFIX} || to_char(SESSSTARTTIME, 'MMDDYYYY') || '.' || @{pipeline().parameters.FILE_EXTENSION} AS v_NCCI_FileName,
	-- *INF*: Decode(TRUE,
	-- @{pipeline().parameters.FILENAME}='MI',v_MI_FileName,
	-- @{pipeline().parameters.FILENAME}='MN',v_MN_FileName,
	-- @{pipeline().parameters.FILENAME}='WI',v_WI_FileName,
	-- @{pipeline().parameters.FILENAME}='NC',v_NC_FileName,
	-- v_NCCI_FileName)
	Decode(
	    TRUE,
	    @{pipeline().parameters.FILENAME} = 'MI', v_MI_FileName,
	    @{pipeline().parameters.FILENAME} = 'MN', v_MN_FileName,
	    @{pipeline().parameters.FILENAME} = 'WI', v_WI_FileName,
	    @{pipeline().parameters.FILENAME} = 'NC', v_NC_FileName,
	    v_NCCI_FileName
	) AS o_FileName
	FROM SRT_Data
),
WCPOLS_DataFile AS (
	INSERT INTO WCPOLS_DataFile
	(Data, FileName)
	SELECT 
	DATA, 
	o_FileName AS FILENAME
	FROM EXP_FileName
),
EXP_Reconciliation AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: RPAD(@{pipeline().parameters.FILENAME},10,' ')
	RPAD(@{pipeline().parameters.FILENAME}, 10, ' ') AS BureauName,
	LinkData,
	-- *INF*: SUBSTR(LinkData,6,9)
	SUBSTR(LinkData, 6, 9) AS PolicyKey,
	-- *INF*: SUBSTR(LinkData,42,2)
	SUBSTR(LinkData, 42, 2) AS TransactionCode,
	RecordTypeCode,
	WCTrackHistoryID
	FROM EXP_RecordCount
),
FIL_ReconciliationData AS (
	SELECT
	AuditID, 
	BureauName, 
	LinkData, 
	PolicyKey, 
	TransactionCode, 
	RecordTypeCode, 
	WCTrackHistoryID
	FROM EXP_Reconciliation
	WHERE IN(RecordTypeCode,'01','08')
),
SQ_WorkWCTrackHistory AS (
	Select distinct B.LinkData as LinkData,
	A.HistoryID as HistoryID,
	A.WCTrackHistoryID as WCTrackHistoryID
	from WorkWCTrackHistory A
	inner join WCPols00Record B
	on A.WCTrackHistoryID=B.WCTrackHistoryID
	and A.Auditid=B.AuditId
	where A.AuditID=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
),
JNR_ReconciliationData AS (SELECT
	SQ_WorkWCTrackHistory.LinkData AS LinkData_TrackHistory, 
	SQ_WorkWCTrackHistory.HistoryID AS HistoryID_TrackHistory, 
	SQ_WorkWCTrackHistory.WCTrackHistoryID AS WCTrackHistoryID_TrackHistory, 
	FIL_ReconciliationData.AuditID, 
	FIL_ReconciliationData.BureauName, 
	FIL_ReconciliationData.LinkData, 
	FIL_ReconciliationData.PolicyKey, 
	FIL_ReconciliationData.TransactionCode, 
	FIL_ReconciliationData.RecordTypeCode, 
	FIL_ReconciliationData.WCTrackHistoryID
	FROM SQ_WorkWCTrackHistory
	INNER JOIN FIL_ReconciliationData
	ON FIL_ReconciliationData.LinkData = SQ_WorkWCTrackHistory.LinkData AND FIL_ReconciliationData.WCTrackHistoryID = SQ_WorkWCTrackHistory.WCTrackHistoryID
),
AGG_RemoveDuplicates AS (
	SELECT
	AuditID,
	BureauName,
	LinkData,
	PolicyKey,
	TransactionCode,
	RecordTypeCode,
	HistoryID_TrackHistory AS HistoryID
	FROM JNR_ReconciliationData
	QUALIFY ROW_NUMBER() OVER (PARTITION BY BureauName, PolicyKey, HistoryID ORDER BY NULL) = 1
),
EXP_ReconciliationID AS (
	SELECT
	v_Counter+1 AS v_Counter,
	v_Counter AS ReconciliationID,
	AuditID,
	BureauName,
	PolicyKey,
	TransactionCode,
	HistoryID
	FROM AGG_RemoveDuplicates
),
WCPOLS_Reconciliation_File AS (
	INSERT INTO WCPOLS_Reconciliation_File
	(ReconciliationID, AuditID, BureauName, PolicyKey, TransactionType, HistoryID)
	SELECT 
	RECONCILIATIONID, 
	AUDITID, 
	BUREAUNAME, 
	POLICYKEY, 
	TransactionCode AS TRANSACTIONTYPE, 
	HISTORYID
	FROM EXP_ReconciliationID
),
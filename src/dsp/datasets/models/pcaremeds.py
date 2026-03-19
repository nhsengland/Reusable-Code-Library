from datetime import date

from decimal import Decimal
from dsp.datasets.common import Fields as CommonFields
from dsp.model.ons_record import ONSRecordPaths
from dsp.common.structured_model import _META, Decimal_14_5, Decimal_19_0, DerivedAttribute, \
    DerivedAttributePlaceholder, \
    DSPStructuredModel, META, SubmittedAttribute, MPSConfidenceScores, AssignableAttribute
from dsp.common.expressions import (DerivePostcode, Literal, NotNull, PostcodeFromODS, Select, ToDate,
                                              ValToDateTime, Cast, AgeBand, If, CCGFromGPPracticeCode)
from dsp.common.pcaremeds_expressions import PaidIndicator, ItemCount
from dsp.shared.constants import DS


class _PrimaryCareMedicineModel(DSPStructuredModel):
    META = SubmittedAttribute(CommonFields.META, _META)

    BSAPrescriptionID = SubmittedAttribute('BSAPrescriptionID', Decimal_19_0)
    ItemID = SubmittedAttribute('ItemID', int)
    EPSPrescriptionID = SubmittedAttribute('EPSPrescriptionID', str)
    PrescriberType = SubmittedAttribute('PrescriberType', int)
    PrescriberID = SubmittedAttribute('PrescriberID', str)
    CostCentreType = SubmittedAttribute('CostCentreType', int)
    CostCentreODSCode = SubmittedAttribute('CostCentreODSCode', str)
    CostCentreSubType = SubmittedAttribute('CostCentreSubType', str)
    DispensedPharmacyType = SubmittedAttribute('DispensedPharmacyType', int)
    DispensedPharmacyODSCode = SubmittedAttribute('DispensedPharmacyODSCode', str)
    PrescribedCountryCode = SubmittedAttribute('PrescribedCountryCode', int)
    DispensedCountryCode = SubmittedAttribute('DispensedCountryCode', int)
    ProcessedPeriod = SubmittedAttribute('ProcessedPeriod', int)
    ChargeStatus = SubmittedAttribute('ChargeStatus', str)
    ExemptionCode = SubmittedAttribute('ExemptionCode', str)
    NHSNumber = SubmittedAttribute('NHSNumber', str)
    PatientDoB = SubmittedAttribute('PatientDoB', str)
    PatientAge = SubmittedAttribute('PatientAge', int)
    PatientGender = SubmittedAttribute('PatientGender', int)
    ItemActualCost = SubmittedAttribute('ItemActualCost', Decimal_14_5)
    ItemNIC = SubmittedAttribute('ItemNIC', Decimal_19_0)
    PrescribeddmdCode = SubmittedAttribute('PrescribeddmdCode', int)
    PrescribedBNFCode = SubmittedAttribute('PrescribedBNFCode', str)
    PrescribedBNFName = SubmittedAttribute('PrescribedBNFName', str)
    PrescribedFormulation = SubmittedAttribute('PrescribedFormulation', str)
    PrescribedSupplierName = SubmittedAttribute('PrescribedSupplierName', str)
    PrescribedMedicineStrength = SubmittedAttribute('PrescribedMedicineStrength', str)
    PrescribedQuantity = SubmittedAttribute('PrescribedQuantity', Decimal_14_5)
    PaiddmdCode = SubmittedAttribute('PaiddmdCode', int)
    PaidBNFCode = SubmittedAttribute('PaidBNFCode', str)
    PaidBNFName = SubmittedAttribute('PaidBNFName', str)
    PaidFormulation = SubmittedAttribute('PaidFormulation', str)
    PaidSupplierName = SubmittedAttribute('PaidSupplierName', str)
    PaidDrugStrength = SubmittedAttribute('PaidDrugStrength', str)
    PaidQuantity = SubmittedAttribute('PaidQuantity', Decimal_14_5)
    PaidPADMIndicator = SubmittedAttribute('PaidPADMIndicator', str)
    PaidCDIndicator = SubmittedAttribute('PaidCDIndicator', str)
    PaidACBSIndicator = SubmittedAttribute('PaidACBSIndicator', str)
    PaidFlavourIndicator = SubmittedAttribute('PaidFlavourIndicator', str)
    PaidSpecContIndicator = SubmittedAttribute('PaidSpecContIndicator', str)
    NotDispensedIndicator = SubmittedAttribute('NotDispensedIndicator', str)
    HighVolVaccineIndicator = SubmittedAttribute('HighVolVaccineIndicator', str)
    PaidDissallowedIndicator = SubmittedAttribute('PaidDissallowedIndicator', str)
    PaidDisallowedReason = SubmittedAttribute('PaidDisallowedReason', int)
    OutOfHoursIndicator = SubmittedAttribute('OutOfHoursIndicator', int)
    PrivatePrescriptionIndicator = SubmittedAttribute('PrivatePrescriptionIndicator', int)
    EPSPrescriptionIndicator = DerivedAttributePlaceholder('EPSPrescriptionIndicator', int)
    ProcessingPeriodDate = DerivedAttributePlaceholder('ProcessingPeriodDate', date)
    MaternityExemptionFlag = DerivedAttributePlaceholder('MaternityExemptionFlag', str)
    CostCentreLSOA = DerivedAttributePlaceholder('CostCentreLSOA', str)
    DispensedPharmacyLSOA = DerivedAttributePlaceholder('DispensedPharmacyLSOA', str)
    AgeBands = DerivedAttributePlaceholder('AgeBands', str)
    ItemCount = DerivedAttributePlaceholder('ItemCount', Decimal_14_5)
    PaidIndicator = DerivedAttributePlaceholder('PaidIndicator', str)

    #MPSConfidence = AssignableAttribute('MPSConfidence', MPSConfidenceScores)  # type: AssignableAttribute
    Person_ID = AssignableAttribute('Person_ID', str)  # type: AssignableAttribute

    PatientGPODS = AssignableAttribute('PatientGPODS', str)  # type: AssignableAttribute
    MPSDateOfBirth = AssignableAttribute("MPSDateOfBirth", date)  # type: AssignableAttribute
    MPSGender = AssignableAttribute("MPSGender", str)  # type: AssignableAttribute
    MPSPostcode = AssignableAttribute("MPSPostcode", str)  # type: AssignableAttribute

    PatientLSOA = DerivedAttributePlaceholder('PatientLSOA', str)
    PatientGPLSOA = DerivedAttributePlaceholder('PatientGPLSOA', str)

    PatientCCG = DerivedAttributePlaceholder('PatientCCG', str)
    PatientLA = DerivedAttributePlaceholder('PatientLA', str)
    PatientGPCCG = DerivedAttributePlaceholder('PatientGPCCG', str)
    PatientGPLA = DerivedAttributePlaceholder('PatientGPLA', str)


class PrimaryCareMedicineModel(_PrimaryCareMedicineModel):
    __concrete__ = True
    __table__ = DS.PCAREMEDS

    META = SubmittedAttribute('META', META)  # type: SubmittedAttribute

    EPSPrescriptionIndicator = DerivedAttribute(
        'EPSPrescriptionID', int,
        Cast(NotNull(Select(_PrimaryCareMedicineModel.EPSPrescriptionID)), int)
    )
    ProcessingPeriodDate = DerivedAttribute(
        'ProcessingPeriodDate', date,
        ToDate(ValToDateTime(Select(_PrimaryCareMedicineModel.ProcessedPeriod), '%Y%m'))
    )
    MaternityExemptionFlag = DerivedAttribute(
        'MaternityExemptionFlag', int,
        Cast(Select(_PrimaryCareMedicineModel.ExemptionCode) == Literal('D'), int)
    )
    CostCentreLSOA = DerivedAttribute(
        'CostCentreLSOA', str,
        DerivePostcode(
            PostcodeFromODS(
                Select(_PrimaryCareMedicineModel.CostCentreODSCode),
                Select(_PrimaryCareMedicineModel.ProcessingPeriodDate)
            ),
            Select(_PrimaryCareMedicineModel.ProcessingPeriodDate),
            ONSRecordPaths.LOWER_LAYER_SOA
        )
    )
    DispensedPharmacyLSOA = DerivedAttribute(
        'DispensedPharmacyLSOA', str,
        DerivePostcode(
            PostcodeFromODS(
                Select(_PrimaryCareMedicineModel.DispensedPharmacyODSCode),
                Select(_PrimaryCareMedicineModel.ProcessingPeriodDate)
            ),
            Select(_PrimaryCareMedicineModel.ProcessingPeriodDate),
            ONSRecordPaths.LOWER_LAYER_SOA
        )
    )
    AgeBands = DerivedAttribute(
        'AgeBands', str,
        AgeBand(age_yr_expr=Select(_PrimaryCareMedicineModel.PatientAge))
    )
    ItemCount = DerivedAttribute(
        'ItemCount', Decimal_14_5,
        ItemCount(highvolvaccineindicator=Select(_PrimaryCareMedicineModel.HighVolVaccineIndicator),
                  dispensedpharmacytype=Select(_PrimaryCareMedicineModel.DispensedPharmacyType),
                  paidformulation=Select(_PrimaryCareMedicineModel.PaidFormulation),
                  paidquantity=Select(_PrimaryCareMedicineModel.PaidQuantity)
                  )
    )
    PaidIndicator = DerivedAttribute(
        'PaidIndicator', str,
        PaidIndicator(
            prescribedbnfcode=Select(_PrimaryCareMedicineModel.PrescribedBNFCode),
            paidquantity=Select(_PrimaryCareMedicineModel.PaidQuantity),
            itemactualcost=Select(_PrimaryCareMedicineModel.ItemActualCost),
            paiddissallowedindicator=Select(_PrimaryCareMedicineModel.PaidDissallowedIndicator),
            notdispensedindicator=Select(_PrimaryCareMedicineModel.NotDispensedIndicator),
            privateprescriptionindicator=Select(_PrimaryCareMedicineModel.PrivatePrescriptionIndicator),
            outofhoursindicator=Select(_PrimaryCareMedicineModel.OutOfHoursIndicator)
        )
    )
    PatientLSOA = DerivedAttribute(
        'PatientLSOA', str,
        DerivePostcode(
            Select(_PrimaryCareMedicineModel.MPSPostcode),
            Select(_PrimaryCareMedicineModel.ProcessingPeriodDate),
            ONSRecordPaths.LOWER_LAYER_SOA
        )
    )
    PatientGPLSOA = DerivedAttribute(
        'PatientGPLSOA', str,
        DerivePostcode(
            PostcodeFromODS(
                Select(_PrimaryCareMedicineModel.PatientGPODS),
                Select(_PrimaryCareMedicineModel.ProcessingPeriodDate)
            ),
            Select(_PrimaryCareMedicineModel.ProcessingPeriodDate),
            ONSRecordPaths.LOWER_LAYER_SOA
        )
    )
    PatientCCG = DerivedAttribute(
        'PatientCCG', str,
        DerivePostcode(
            Select(_PrimaryCareMedicineModel.MPSPostcode),
            Select(_PrimaryCareMedicineModel.ProcessingPeriodDate),
            ONSRecordPaths.CCG
        )
    )
    PatientLA = DerivedAttribute(
        'PatientLA', str,
        DerivePostcode(
            Select(_PrimaryCareMedicineModel.MPSPostcode),
            Select(_PrimaryCareMedicineModel.ProcessingPeriodDate),
            ONSRecordPaths.UNITARY_AUTHORITY
        )
    )
    PatientGPCCG = DerivedAttribute(
        'PatientGPCCG', str,
        CCGFromGPPracticeCode(
            Select(_PrimaryCareMedicineModel.PatientGPODS),
            Select(_PrimaryCareMedicineModel.ProcessingPeriodDate)
        )
    )
    PatientGPLA = DerivedAttribute(
        'PatientGPLA', str,
        DerivePostcode(
            PostcodeFromODS(
                Select(_PrimaryCareMedicineModel.PatientGPODS),
                Select(_PrimaryCareMedicineModel.ProcessingPeriodDate)
            ),
            Select(_PrimaryCareMedicineModel.ProcessingPeriodDate),
            ONSRecordPaths.UNITARY_AUTHORITY
        )
    )


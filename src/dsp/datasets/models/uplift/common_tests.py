from dsp.datasets.models.uplift.common import rename_fields, add_fields, remove_fields


def test_rename_fields():
    test_dict = {
        "pets":
            [
                {"dog": "rex"},
                {"dog": "fido"},
                {"cat": "mittens"}
            ],
        "colour": "yellow",
        "weather": "sunny",
        "dog": "henry"
    }

    mapping = {"dog": "pooch"}
    updated_dict = rename_fields(test_dict, mapping)
    assert updated_dict == {
        "pets":
            [
                {"pooch": "rex"},
                {"pooch": "fido"},
                {"cat": "mittens"}
            ],
        "colour": "yellow",
        "weather": "sunny",
        "pooch": "henry"
    }


def test_add_fields():
    test_dict = {
        "Patient":
            {
                "DisabilityTypes": ["mobility", "TBI"],
            },
        "HospitalVisits":[
            {'Date': '01-01-01'}
        ],
        "LaboursAndDeliveries": [
            {
                "CareActivityLaboursAndDeliveries": [
                    {"OxytocinAdministeredInd": None}
                ]
            }
        ]
    }

    new_fields = ['Patient.PersonBirthDate', "Referral.ServiceTypeReferredTo.TeamType",
                  "NHSNumber", "HospitalVisits.Reason",
                  'LaboursAndDeliveries.CareActivityLaboursAndDeliveries.PostpartumBloodLoss']
    updated_dict = add_fields(test_dict, new_fields)
    assert updated_dict == {
        "Patient":
            {
                "DisabilityTypes": ["mobility", "TBI"],
                "PersonBirthDate": None
            },
        "HospitalVisits": [
            {
                'Date': '01-01-01',
                'Reason': None
            }
        ],
        "Referral": {
            "ServiceTypeReferredTo": {
                "TeamType": None,
            },
        },
        "NHSNumber": None,
        "LaboursAndDeliveries":[
            {
                "CareActivityLaboursAndDeliveries": [
                    {
                        "OxytocinAdministeredInd": None,
                        "PostpartumBloodLoss": None}
                ]
             }
        ]
    }


def test_remove_fields():
    test_dict = {
        "pets":
            [
                {"dog": "rex"},
                {"dog": "fido"},
                {"cat": "mittens"}
            ],
        "colour": "yellow",
        "weather": "sunny",
        "dog": "henry"
    }

    fields_to_remove = frozenset(['dog'])

    updated_dict = remove_fields(test_dict, fields_to_remove)

    assert updated_dict == {
        "pets":
            [
                {},
                {},
                {"cat": "mittens"}
            ],
        "colour": "yellow",
        "weather": "sunny"
    }

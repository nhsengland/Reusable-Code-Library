from dsp.common.structured_model import DSPStructuredModel

def check_derived_values(model_attr: DSPStructuredModel, expected_dict: dict):
    for attr_name, expected_value in expected_dict.items():
        attr_value = getattr(model_attr, attr_name)
        if isinstance(attr_value, list):
            if not isinstance(expected_value, list):
                raise Exception(f"Expected a list for {attr_name}")
            if len(attr_value) != len(expected_value):
                print(f"Length comparison failure: Expected {expected_value}, got {attr_value} for {attr_name}")
            assert len(attr_value) == len(expected_value), \
                f"Length comparison failure: Expected {expected_value}, got {attr_value} for {attr_name}"
            for attr_value_elm, expected_value_elm in zip(attr_value, expected_value):
                if isinstance(attr_value_elm, DSPStructuredModel):
                    check_derived_values(attr_value_elm, expected_value_elm)
                else:
                    if attr_value_elm != expected_value_elm:
                        print(f"Expected value was {expected_value_elm}, got {attr_value_elm} for {attr_name}")
                    assert attr_value_elm == expected_value_elm, \
                        f"Expected value was {expected_value_elm}, got {attr_value_elm} for {attr_name}"
        elif isinstance(attr_value, DSPStructuredModel):
            check_derived_values(attr_value, expected_value)
        else:
            if attr_value != expected_value:
                print(f"Expected {expected_value}, got {attr_value} for {attr_name}")
            assert attr_value == expected_value, \
                f"Expected {expected_value}, got {attr_value} for {attr_name}"

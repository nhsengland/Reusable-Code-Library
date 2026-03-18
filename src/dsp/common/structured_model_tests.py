import copy
import pickle
from datetime import date, datetime
from typing import List

import pytest
import json
from mock import Mock
from pyspark.sql import SparkSession
from pyspark.sql.types import Row

from dsp.common.expressions import Count, Select, AgeAtDate, Multiply, Sum, ModelExpression, If, NotNull, \
    Literal
from dsp.common.model_accessor import ModelAttribute
from dsp.common.structured_model import (
    AssignableAttribute,
    DerivedAttribute,
    DerivedAttributePlaceholder,
    DSPStructuredModel,
    RepeatingSubmittedAttribute,
    SubmittedAttribute)


class _Address(DSPStructuredModel):
    Line1 = SubmittedAttribute('Line1', str)  # type: SubmittedAttribute
    Postcode = SubmittedAttribute('Postcode', str)  # type: SubmittedAttribute
    City = SubmittedAttribute('City', str)  # type: SubmittedAttribute
    PostcodeBlank = DerivedAttributePlaceholder('PostcodeBlank', bool)  # type: DerivedAttributePlaceholder


class _Patient(DSPStructuredModel):
    Name = SubmittedAttribute('Name', str)  # type: SubmittedAttribute
    Dob = SubmittedAttribute('Dob', datetime)  # type: SubmittedAttribute
    Addresses = RepeatingSubmittedAttribute('Addresses', _Address)  # type: RepeatingSubmittedAttribute
    NumberOfAddresses = DerivedAttributePlaceholder('NumberOfAddresses', int)  # type: DerivedAttributePlaceholder


class _Event(DSPStructuredModel):
    Patient = SubmittedAttribute('Patient', _Patient)  # type: SubmittedAttribute
    Date = SubmittedAttribute('Date', datetime)  # type: SubmittedAttribute
    AgeAtEvent = DerivedAttributePlaceholder('AgeAtEvent', int)  # type: DerivedAttributePlaceholder


class Address(_Address):
    __concrete__ = True

    PostcodeBlank = DerivedAttribute('PostcodeBlank',
                                     bool,
                                     If(
                                         NotNull(Select(_Address.Postcode)),
                                         then=Literal(False),
                                         otherwise=Literal(True)
                                     ))  # type: DerivedAttribute

class Patient(_Patient):
    __concrete__ = True

    Addresses = RepeatingSubmittedAttribute('Addresses', Address)  # type: RepeatingSubmittedAttribute
    NumberOfAddresses = DerivedAttribute('NumberOfAddresses', int,
                                         Count(Select(_Patient.Addresses)))  # type: DerivedAttribute


class Event(_Event):
    __concrete__ = True

    # @TODO: Have to restate the attribute to lock in the concrete type. Ideally, we shouldn't need to do this - maybe
    # retain a catalogue of concrete types by spec types?
    Patient = SubmittedAttribute('Patient', Patient)  # type: SubmittedAttribute

    # @TODO: Ideally don't restate the field name or type as an argument. Decorator should be able to pull spec from the
    # base class to validate the implementation.
    AgeAtEvent = DerivedAttribute('AgeAtEvent', int,
                                  AgeAtDate(Select(_Event.Patient.Dob), Select(_Event.Date)))  # type: DerivedAttribute


def test_equal():
    address_london = Address({
        'Line1': '24 Leinster Gardens',
        'Postcode': 'W2 3AN',
        'City': 'London'
    })
    address_leeds = Address({
        'Line1': '2 Whitehall Qua',
        'Postcode': 'LS1 4HR',
        'City': 'Leeds'
    })
    event_london_1 = Event({
        'Date': datetime(2028, 10, 10),
        'Patient': {
            'Name': 'Bob',
            'Dob': datetime(2018, 10, 10),
            'Addresses': [address_london]
        }
    })
    event_london_2 = Event({
        'Date': datetime(2028, 10, 10),
        'Patient': {
            'Name': 'Bob',
            'Dob': datetime(2018, 10, 10),
            'Addresses': [address_london]
        }
    })
    event_leeds = Event({
        'Date': datetime(2028, 10, 10),
        'Patient': {
            'Name': 'Bob',
            'Dob': datetime(2018, 10, 10),
            'Addresses': [address_leeds]
        }
    })

    assert not address_london == event_london_1
    assert event_london_1 == event_london_2
    assert event_london_1 != event_leeds


def test_is_picklable():
    event = Event({
        'Date': datetime(2028, 10, 10),
        'Patient': {
            'Name': 'Bob',
            'Dob': datetime(2018, 10, 10),
            'Addresses': [
                {
                    'Line1': '24 Leinster Gardens',
                    'Postcode': 'W2 3AN',
                    'City': 'London'
                }
            ]
        }
    })

    event_unpickled = pickle.loads(pickle.dumps(event))

    assert event_unpickled == event


def test_derivation_can_be_performed():
    event = Event({
        'Date': datetime(2028, 10, 10),
        'Patient': {
            'Name': 'Bob',
            'Dob': datetime(2018, 10, 10),
            'Addresses': [
                {
                    'Line1': '24 Leinster Gardens',
                    'Postcode': 'W2 3AN',
                    'City': 'London'
                }
            ]
        }
    })

    # submitted attributes, including relationships and collections
    assert event.Date == datetime(2028, 10, 10)
    assert event.Patient.Name == 'Bob'
    assert event.Patient.Dob == datetime(2018, 10, 10)
    assert event.Patient.NumberOfAddresses == 1
    assert len(event.Patient.Addresses) == 1
    assert event.Patient.Addresses[0].Line1 == '24 Leinster Gardens'
    assert event.Patient.Addresses[0].Postcode == 'W2 3AN'
    assert event.Patient.Addresses[0].City == 'London'

    # derivations
    assert event.AgeAtEvent == 10
    assert event.Patient.Addresses[0].PostcodeBlank == False

    json_str = '''{"Date": "2028-10-10T00:00:00", "Patient": {
                "Name": "Bob", "Dob": "2018-10-10T00:00:00", "Addresses": [
                {"Line1": "24 Leinster Gardens", "Postcode": "W2 3AN", "City": "London", "PostcodeBlank": false}],
                 "NumberOfAddresses": 1},"AgeAtEvent": 10}'''
    assert json.loads(event.as_json()) == json.loads(json_str)


def test_derivation_can_be_performed_when_value_is_cached():

    #Test case 1: Cached derivation values retained
    event = Event({
        'Date': datetime(2028, 10, 10),
        'Patient': {
            'Name': 'Bob',
            'Dob': datetime(2018, 10, 10),
            'Addresses': [
                {
                    'Line1': '24 Leinster Gardens',
                    'Postcode': 'W2 3AN',
                    'City': 'London',
                    'PostcodeBlank': True
                }
            ],
            'NumberOfAddresses': 10
        },
        'AgeAtEvent': 5
    })

    assert event.Patient.NumberOfAddresses == 10
    assert event.AgeAtEvent == 5
    assert event.Patient.Addresses[0].PostcodeBlank == True

    json_str = '''{"Date": "2028-10-10T00:00:00", "Patient": {
                "Name": "Bob", "Dob": "2018-10-10T00:00:00", "Addresses": [
                {"Line1": "24 Leinster Gardens", "Postcode": "W2 3AN", "City": "London", "PostcodeBlank": true}],
                 "NumberOfAddresses": 10},"AgeAtEvent": 5}'''
    assert json.loads(event.as_json()) == json.loads(json_str)

    #Test case 2: All three derivations re-derived
    event = Event({
        'Date': datetime(2028, 10, 10),
        'Patient': {
            'Name': 'Bob',
            'Dob': datetime(2018, 10, 10),
            'Addresses': [
                {
                    'Line1': '24 Leinster Gardens',
                    'Postcode': 'W2 3AN',
                    'City': 'London',
                    'PostcodeBlank': True
                }
            ],
            'NumberOfAddresses': 10
        },
        'AgeAtEvent': 5
    }, ignore_cached_values={'AgeAtEvent', 'NumberOfAddresses', 'PostcodeBlank'})

    assert event.AgeAtEvent == 10
    assert event.Patient.NumberOfAddresses == 1
    assert event.Patient.Addresses[0].PostcodeBlank == False

    json_str = '''{"Date": "2028-10-10T00:00:00", "Patient": {
                "Name": "Bob", "Dob": "2018-10-10T00:00:00", "Addresses": [
                {"Line1": "24 Leinster Gardens", "Postcode": "W2 3AN", "City": "London", "PostcodeBlank": false}],
                 "NumberOfAddresses": 1},"AgeAtEvent": 10}'''
    assert json.loads(event.as_json()) == json.loads(json_str)

    #Test case 3: Nested derivations re-derived, root one not
    event = Event({
        'Date': datetime(2028, 10, 10),
        'Patient': {
            'Name': 'Bob',
            'Dob': datetime(2018, 10, 10),
            'Addresses': [
                {
                    'Line1': '24 Leinster Gardens',
                    'Postcode': 'W2 3AN',
                    'City': 'London',
                    'PostcodeBlank': True
                }
            ],
            'NumberOfAddresses': 10
        },
        'AgeAtEvent': 5
    }, ignore_cached_values={'NumberOfAddresses', 'PostcodeBlank'})

    assert event.AgeAtEvent == 5
    assert event.Patient.NumberOfAddresses == 1
    assert event.Patient.Addresses[0].PostcodeBlank == False

    json_str = '''{"Date": "2028-10-10T00:00:00", "Patient": {
                "Name": "Bob", "Dob": "2018-10-10T00:00:00", "Addresses": [
                {"Line1": "24 Leinster Gardens", "Postcode": "W2 3AN", "City": "London", "PostcodeBlank": false}],
                 "NumberOfAddresses": 1},"AgeAtEvent": 5}'''
    assert json.loads(event.as_json()) == json.loads(json_str)

def test_access_parent_attributes():
    class _Product(DSPStructuredModel):
        ProductID = SubmittedAttribute('ProductID', str)  # type: SubmittedAttribute

        OrderID = DerivedAttributePlaceholder('OrderID', str)  # type: DerivedAttributePlaceholder
        Quantity = DerivedAttributePlaceholder('Quantity', int)  # type: DerivedAttributePlaceholder

    class _OrderItem(DSPStructuredModel):
        OrderedProduct = SubmittedAttribute('OrderedProduct', _Product)  # type: SubmittedAttribute
        Quantity = SubmittedAttribute('Quantity', int)  # type: SubmittedAttribute

    class _Order(DSPStructuredModel):
        OrderID = SubmittedAttribute('OrderID', str)  # type: SubmittedAttribute
        OrderedItem = SubmittedAttribute('OrderedItem', _OrderItem)  # type: SubmittedAttribute

    _Product.Root = ModelAttribute('root', _Order)
    _Product.Parent = ModelAttribute('parent', _OrderItem)

    _OrderItem.Root = ModelAttribute('root', _Order)
    _OrderItem.Parent = ModelAttribute('parent', _Order)

    _Order.Root = ModelAttribute('root', _Order)

    class Product(_Product):
        __concrete__ = True

        OrderID = DerivedAttribute('OrderID', str, Select(_Product.Root.OrderID))
        Quantity = DerivedAttribute('Quantity', int, Select(_Product.Parent.Quantity))

    class OrderItem(_OrderItem):
        __concrete__ = True

        OrderedProduct = SubmittedAttribute('OrderedProduct', Product)  # type: SubmittedAttribute

    class Order(_Order):
        __concrete__ = True

        OrderedItem = SubmittedAttribute('OrderedItem', OrderItem)  # type: SubmittedAttribute

    order_dict = {
        'OrderID': '123',
        'OrderedItem': {
            'OrderedProduct': {
                'ProductID': '456'
            },
            'Quantity': 3
        }
    }

    order = Order(order_dict)

    assert order.OrderedItem.OrderedProduct.OrderID == '123'
    assert order.OrderedItem.OrderedProduct.Quantity == 3


def test_repeating_attributes():
    class _Product(DSPStructuredModel):
        Cost = SubmittedAttribute('Cost', int)  # type: SubmittedAttribute

    class _OrderItem(DSPStructuredModel):
        Product = SubmittedAttribute('Product', _Product)  # type: SubmittedAttribute
        Quantity = SubmittedAttribute('Quantity', int)  # type: SubmittedAttribute

        TotalItems = DerivedAttributePlaceholder('TotalItems', int)  # type: DerivedAttributePlaceholder
        TotalCost = DerivedAttributePlaceholder('TotalCost', int)  # type: DerivedAttributePlaceholder

    class _Order(DSPStructuredModel):
        Items = RepeatingSubmittedAttribute('Items', _OrderItem)  # type: RepeatingSubmittedAttribute

        TotalCost = DerivedAttributePlaceholder('TotalCost', int)  # type: DerivedAttributePlaceholder

    class CProduct(_Product):
        __concrete__ = True

    class OrderItem(_OrderItem):
        __concrete__ = True

        Product = SubmittedAttribute('Product', CProduct)  # type: SubmittedAttribute

        TotalCost = DerivedAttribute('TotalCost', int,
                                     Multiply(Select(_OrderItem.Product.Cost),
                                              Select(_OrderItem.Quantity)))  # type: DerivedAttribute

    class Order(_Order):
        __concrete__ = True

        Items = RepeatingSubmittedAttribute('Items', OrderItem)  # type: RepeatingSubmittedAttribute

        TotalItems = DerivedAttribute('TotalItems', int, Sum(Select(_Order.Items.Quantity)))
        TotalUnitCost = DerivedAttribute('TotalUnitCost', int, Sum(Select(_Order.Items.Product.Cost)))
        TotalCost = DerivedAttribute('TotalCost', int, Sum(Select(_Order.Items.TotalCost)))

    order_dict = {
        'Items': [
            {
                'Product': {
                    'Cost': 123
                },
                'Quantity': 1
            },
            {
                'Product': {
                    'Cost': 456
                },
                'Quantity': 3
            }
        ]
    }

    order = Order(order_dict)
    assert order.TotalItems == 4
    assert order.TotalCost == 1491
    assert order.TotalUnitCost == 579

    assert len(order.Items) == 2
    assert order.Items[0].TotalCost == 123
    assert order.Items[1].TotalCost == 1368

    path = _Order.Items[0:2]
    assert str(path) == "Items[0:2]"
    assert path.__get__(order) == order.Items


def test_from_row():
    row = Row(**{
        'Line1': '24 Leinster Gardens',
        'Postcode': 'W2 3AN',
        'City': 'London'
    })

    address = Address.from_row(row)

    assert address.Line1 == '24 Leinster Gardens'
    assert address.Postcode == 'W2 3AN'
    assert address.City == 'London'


def test_from_df(spark: SparkSession):
    df = spark.createDataFrame([
        {
            'Line1': '24 Leinster Gardens',
            'Postcode': 'W2 3AN',
            'City': 'London'
        },
        {
            'Line1': '2 Whitehall Quay',
            'Postcode': 'LS1 4HR',
            'City': 'Leeds'
        }
    ])

    rdd_of_models = Address.from_dataframe(df)
    assert rdd_of_models.count() == 2

    models = rdd_of_models.collect()  # type: List[Address]

    assert models[0].Line1 == '24 Leinster Gardens'
    assert models[0].Postcode == 'W2 3AN'
    assert models[0].City == 'London'

    assert models[1].Line1 == '2 Whitehall Quay'
    assert models[1].Postcode == 'LS1 4HR'
    assert models[1].City == 'Leeds'


def test_reject_model_with_derived_attributes_specified():
    class Model(DSPStructuredModel):
        __concrete__ = True

        value = DerivedAttributePlaceholder('value', str)

    with pytest.raises(ValueError):
        Model({'value': 'Hello world!'}, accept_derived_values=False)


def test_derivation_results_cached():
    mock_expression = Mock(ModelExpression)
    mock_expression.resolve_value.return_value = 123

    class Model(DSPStructuredModel):
        __concrete__ = True

        derivation = DerivedAttribute('derivation', int, mock_expression)

    model = Model({})
    assert model.derivation == 123
    mock_expression.resolve_value.assert_called_once_with(model)

    # Fetch derivation again and ensure expression not invoked a second time
    mock_expression.reset_mock()
    assert model.derivation == 123
    mock_expression.resolve_value.assert_not_called()


class _VetAppointment(DSPStructuredModel):
    VetName=SubmittedAttribute('VetName', str)
    Diagnoses=SubmittedAttribute('Diagnoses',List[str])


class VetAppointment(_VetAppointment):
    __concrete__= True


class _Animal(DSPStructuredModel):
    Name = SubmittedAttribute('Name', str)
    Species = SubmittedAttribute('Species', str)
    Age = SubmittedAttribute('Age', int)
    FavouriteFoods = AssignableAttribute('FavouriteFoods', List[str])
    NumberOfTreats = AssignableAttribute('NumberOfTreats', List[int])
    KeyDates = SubmittedAttribute('KeyDates', List[date])
    Appointments = RepeatingSubmittedAttribute('Appointments', _VetAppointment)

  
class Animal(_Animal):
    __concrete__=True
    Appointments = RepeatingSubmittedAttribute('Appointments', VetAppointment)


def test_list_type_assignable_attr():
    _record_with_casting = {'Name': 'Larry',
               'Age': '13',
               'Species': 'dog',
               'FavouriteFoods': ['biscuits', 'chicken'],
               'NumberOfTreats': ['1', '10', '5'],
               'KeyDates': [date(2021,1,5), date(2021,5,7)],
               'Appointments': [{'VetName': 'Dr Jones',
                                 'Diagnoses': ['Swallowed tennis ball', 'Bead stuck up nose']},
                                {'VetName': 'Dr Adams',
                                 'Diagnoses': ['Ate jug of fat']}]
               }
    
    animal = Animal(_record_with_casting)
    _expected = copy.deepcopy(_record_with_casting)
    _expected['Age'] = 13
    _expected['NumberOfTreats'] = [1, 10, 5]
    
    assert animal.as_dict() == _expected
    
    _record_without_casting = {'Name': 'Larry',
               'Age': 13,
               'Species': 'dog',
               'FavouriteFoods': ['biscuits', 'chicken'],
               'NumberOfTreats': [1, 10, 5],
               'KeyDates': [date(2021,1,5), date(2021,5,7)],
               'Appointments': [{'VetName': 'Dr Jones',
                                 'Diagnoses': ['Swallowed tennis ball', 'Bead stuck up nose']},
                                {'VetName': 'Dr Adams',
                                 'Diagnoses': ['Ate jug of fat']}]
               }
    animal = Animal(_record_without_casting)

    assert animal.as_dict() == _record_without_casting
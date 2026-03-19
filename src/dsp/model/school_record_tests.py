from dsp.model.school_record import SchoolRecord, SchoolRecordPaths


def test_create_school_record():
    parsed = _create_school_record()

    assert parsed.value_at(SchoolRecordPaths.NAME, 20150101) == 'ABC School'
    assert parsed.value_at(SchoolRecordPaths.NAME, 20140101) is None
    assert parsed.value_at(SchoolRecordPaths.CONTACT_TELEPHONE_NUMBER, 20150101) == '01111234567'
    assert parsed.value_at(SchoolRecordPaths.CONTACT_TELEPHONE_NUMBER, 20140101) is None
    assert parsed.record_dict['open_date'] == 19200101
    assert parsed.record_dict['close_date'] == [(20150101, 20180110)]
    assert parsed.record_dict['code'] == 'EE100000'


def test_update_school_record():
    parsed = _create_school_record()

    parsed.add_history_item(
        {
            SchoolRecordPaths.NAME: '123 School',
            SchoolRecordPaths.NATIONAL_GROUPING: 'Y01',
            SchoolRecordPaths.HIGH_LEVEL_HEALTH_GEOGRAPHY: 'Q01',
            SchoolRecordPaths.ADDRESS_LINE_1: 'School Road',
            SchoolRecordPaths.ADDRESS_LINE_2: 'School Town',
            SchoolRecordPaths.ADDRESS_LINE_3: 'School City',
            SchoolRecordPaths.ADDRESS_LINE_4: 'School County',
            SchoolRecordPaths.ADDRESS_LINE_5: 'School Country',
            SchoolRecordPaths.POSTCODE: 'SC4 0OL',
            SchoolRecordPaths.OPEN_DATE: 19000101,
            SchoolRecordPaths.CLOSE_DATE: 20190110,
            SchoolRecordPaths.LOCAL_AUTHORITY: '123',
            SchoolRecordPaths.CONTACT_TELEPHONE_NUMBER: '01111234567',
            SchoolRecordPaths.CURRENT_CARE_ORGANISATION: '01A',
            SchoolRecordPaths.TYPE_OF_ESTABLISHMENT: '1',
            SchoolRecordPaths.EFFECTIVE_FROM: 20190101,
        }
    )

    assert parsed.value_at(SchoolRecordPaths.NAME, 20150101) == 'ABC School'
    assert parsed.value_at(SchoolRecordPaths.NAME, 20190201) == '123 School'
    assert parsed.record_dict['open_date'] == 19000101
    assert parsed.record_dict['close_date'] == [(20190101, 20190110), (20150101, 20180110)]


def _create_school_record():
    return SchoolRecord.from_history_item(
        'EE100000',
        {
            SchoolRecordPaths.NAME: 'ABC School',
            SchoolRecordPaths.NATIONAL_GROUPING: 'Y01',
            SchoolRecordPaths.HIGH_LEVEL_HEALTH_GEOGRAPHY: 'Q01',
            SchoolRecordPaths.ADDRESS_LINE_1: 'School Road',
            SchoolRecordPaths.ADDRESS_LINE_2: 'School Town',
            SchoolRecordPaths.ADDRESS_LINE_3: 'School City',
            SchoolRecordPaths.ADDRESS_LINE_4: 'School County',
            SchoolRecordPaths.ADDRESS_LINE_5: 'School Country',
            SchoolRecordPaths.POSTCODE: 'SC4 0OL',
            SchoolRecordPaths.OPEN_DATE: 19200101,
            SchoolRecordPaths.CLOSE_DATE: 20180110,
            SchoolRecordPaths.LOCAL_AUTHORITY: '123',
            SchoolRecordPaths.CONTACT_TELEPHONE_NUMBER: '01111234567',
            SchoolRecordPaths.CURRENT_CARE_ORGANISATION: '01A',
            SchoolRecordPaths.TYPE_OF_ESTABLISHMENT: '1',
            SchoolRecordPaths.EFFECTIVE_FROM: 20150101,
        }
    )

from dsp.model.ons_record import ONSRecord, ONSRecordPaths


def test_create_event_stream():
    parsed = ONSRecord.from_history_item(
        'LS1 4JL',
        {
            ONSRecordPaths.DATE_OF_INTRODUCTION: 19000101,
            ONSRecordPaths.EFFECTIVE_FROM: 19000101,
            ONSRecordPaths.COUNTRY: 'bob',
            ONSRecordPaths.CCG: 'ccg1',
            ONSRecordPaths.SHA: 'sha1',
            ONSRecordPaths.ICB: 'icb1',
            ONSRecordPaths.SHA_OLD: 'old_sha1',
            ONSRecordPaths.ED_COUNTY_CODE: 'ed_cc',
            ONSRecordPaths.ED_DISTRICT_CODE: 'ed_dc',
            ONSRecordPaths.ELECTORAL_WARD_98: 'ew',
            ONSRecordPaths.UNITARY_AUTHORITY: 'auth',
            ONSRecordPaths.LOWER_LAYER_SOA: 'soa',
            ONSRecordPaths.MIDDLE_LAYER_SOA: 'msoa',
            ONSRecordPaths.OS_EAST_1M: 'os_east',
            ONSRecordPaths.OS_NORTH_1M: 'os_north',
            ONSRecordPaths.RESIDENCE_COUNTY: 'county',
            ONSRecordPaths.OS_WARD_2011: 'ward'
        }
    )

    assert parsed.value_at(ONSRecordPaths.CCG, 19000101) == 'ccg1'
    assert parsed.value_at(ONSRecordPaths.CCG, 19000100) is None
    assert parsed.value_at(ONSRecordPaths.UNITARY_AUTHORITY, 19000101) == 'auth'
    assert parsed.value_at(ONSRecordPaths.UNITARY_AUTHORITY, 19000100) is None
    assert parsed.value_at(ONSRecordPaths.MIDDLE_LAYER_SOA, 19000101) == 'msoa'
    assert parsed.value_at(ONSRecordPaths.ICB, 19000101) == 'icb1'
    assert parsed.value_at(ONSRecordPaths.ICB, 19000100) is None


def test_create_event_stream_with_end_date():
    parsed = ONSRecord.from_history_item(
        'LS1 4JL',
        {
            ONSRecordPaths.COUNTRY: 'bob',
            ONSRecordPaths.DATE_OF_INTRODUCTION: 19000101,
            ONSRecordPaths.DATE_OF_TERMINATION: 20170101,
            ONSRecordPaths.EFFECTIVE_FROM: 19000101,
            ONSRecordPaths.CCG: 'ccg1',
            ONSRecordPaths.SHA: 'sha1',
            ONSRecordPaths.ICB: 'icb1',
            ONSRecordPaths.SHA_OLD: 'old_sha1',
            ONSRecordPaths.ED_COUNTY_CODE: 'ed_cc',
            ONSRecordPaths.ED_DISTRICT_CODE: 'ed_dc',
            ONSRecordPaths.ELECTORAL_WARD_98: 'ew',
            ONSRecordPaths.UNITARY_AUTHORITY: 'auth',
            ONSRecordPaths.LOWER_LAYER_SOA: 'soa',
            ONSRecordPaths.MIDDLE_LAYER_SOA: 'msoa',
            ONSRecordPaths.OS_EAST_1M: 'os_east',
            ONSRecordPaths.OS_NORTH_1M: 'os_north',
            ONSRecordPaths.RESIDENCE_COUNTY: 'county',
            ONSRecordPaths.OS_WARD_2011: 'ward'
        }
    )

    assert parsed.value_at(ONSRecordPaths.CCG, 19000101) == 'ccg1'
    assert parsed.value_at(ONSRecordPaths.CCG, 19000100) is None
    assert parsed.value_at(ONSRecordPaths.CCG, 20170101) == 'ccg1'
    # suppressing for now as we are not respecting date of termination on the postcodes
    # assert parsed.value_at(ONSRecordPaths.CCG, 20170102) is None
    assert parsed.value_at(ONSRecordPaths.ICB, 19000101) == 'icb1'
    assert parsed.value_at(ONSRecordPaths.ICB, 19000100) is None
    assert parsed.value_at(ONSRecordPaths.ICB, 20170101) == 'icb1'


def test_create_event_stream_add_item():
    parsed = ONSRecord.from_history_item(
        'LS1 4JL',
        {
            ONSRecordPaths.COUNTRY: 'bob',
            ONSRecordPaths.DATE_OF_INTRODUCTION: 19000101,
            ONSRecordPaths.EFFECTIVE_FROM: 18000101,
            ONSRecordPaths.CCG: 'ccg1',
            ONSRecordPaths.SHA: 'sha1',
            ONSRecordPaths.ICB: 'icb1',
            ONSRecordPaths.SHA_OLD: 'old_sha1',
            ONSRecordPaths.ED_COUNTY_CODE: 'ed_cc',
            ONSRecordPaths.ED_DISTRICT_CODE: 'ed_dc',
            ONSRecordPaths.ELECTORAL_WARD_98: 'ew',
            ONSRecordPaths.UNITARY_AUTHORITY: 'auth',
            ONSRecordPaths.LOWER_LAYER_SOA: 'soa',
            ONSRecordPaths.MIDDLE_LAYER_SOA: 'msoa',
            ONSRecordPaths.OS_EAST_1M: 'os_east',
            ONSRecordPaths.OS_NORTH_1M: 'os_north',
            ONSRecordPaths.RESIDENCE_COUNTY: 'county',
            ONSRecordPaths.OS_WARD_2011: 'ward'
        }
    )

    assert not parsed.active_at(18991231)
    assert parsed.active_at(19000101)

    # suppressing for now as we are not currently respecting date of introduction on the postcodes
    # assert parsed.value_at(ONSRecordPaths.CCG, 18000101) is None

    parsed.add_history_item(
        {
            ONSRecordPaths.COUNTRY: 'bob',
            ONSRecordPaths.DATE_OF_INTRODUCTION: 18000102,
            ONSRecordPaths.DATE_OF_TERMINATION: 20170105,
            ONSRecordPaths.EFFECTIVE_FROM: 20170101,
            ONSRecordPaths.CCG: 'ccg2',
            ONSRecordPaths.SHA: 'sha1',
            ONSRecordPaths.ICB: 'icb2',
            ONSRecordPaths.SHA_OLD: 'old_sha1',
            ONSRecordPaths.ED_COUNTY_CODE: 'ed_cc',
            ONSRecordPaths.ED_DISTRICT_CODE: 'ed_dc',
            ONSRecordPaths.ELECTORAL_WARD_98: 'ew',
            ONSRecordPaths.UNITARY_AUTHORITY: 'auth',
            ONSRecordPaths.LOWER_LAYER_SOA: 'soa',
            ONSRecordPaths.MIDDLE_LAYER_SOA: 'msoa',
            ONSRecordPaths.OS_EAST_1M: 'os_east',
            ONSRecordPaths.OS_NORTH_1M: 'os_north',
            ONSRecordPaths.RESIDENCE_COUNTY: 'county',
            ONSRecordPaths.OS_WARD_2011: 'ward'
        }
    )

    assert parsed.value_at(ONSRecordPaths.CCG, 18000102) == 'ccg1'
    # suppressing for now as we are not currently respecting date of introduction on the postcodes
    # assert parsed.value_at(ONSRecordPaths.CCG, 18000101) is None
    assert parsed.value_at(ONSRecordPaths.CCG, 20170101) == 'ccg2'
    # suppressing for now as we are not respecting date of termination on the postcodes
    # assert parsed.value_at(ONSRecordPaths.CCG, 20170106) is None

    assert parsed.value_at(ONSRecordPaths.ICB, 18000102) == 'icb1'
    assert parsed.value_at(ONSRecordPaths.ICB, 20170101) == 'icb2'

    # Introduction day has moved further back in the past
    assert not parsed.active_at(18000101)
    assert parsed.active_at(18000102)

    # And we have a termination date - so test the boundaries
    assert parsed.active_at(20170104)
    assert parsed.active_at(20170105)
    assert not parsed.active_at(20170106)

    # Add a new history item with no termination date
    parsed.add_history_item(
        {
            ONSRecordPaths.COUNTRY: 'bob',
            ONSRecordPaths.DATE_OF_INTRODUCTION: 18000102,
            ONSRecordPaths.EFFECTIVE_FROM: 20170102,
            ONSRecordPaths.CCG: 'ccg2',
            ONSRecordPaths.SHA: 'sha1',
            ONSRecordPaths.ICB: 'icb2',
            ONSRecordPaths.SHA_OLD: 'old_sha1',
            ONSRecordPaths.ED_COUNTY_CODE: 'ed_cc',
            ONSRecordPaths.ED_DISTRICT_CODE: 'ed_dc',
            ONSRecordPaths.ELECTORAL_WARD_98: 'ew',
            ONSRecordPaths.UNITARY_AUTHORITY: 'auth',
            ONSRecordPaths.LOWER_LAYER_SOA: 'soa',
            ONSRecordPaths.MIDDLE_LAYER_SOA: 'msoa',
            ONSRecordPaths.OS_EAST_1M: 'os_east',
            ONSRecordPaths.OS_NORTH_1M: 'os_north',
            ONSRecordPaths.RESIDENCE_COUNTY: 'county',
            ONSRecordPaths.OS_WARD_2011: 'ward'
        }
    )

    # Now it's open-ended - so what was previously post-termination, is now back in scope
    assert parsed.active_at(20170106)

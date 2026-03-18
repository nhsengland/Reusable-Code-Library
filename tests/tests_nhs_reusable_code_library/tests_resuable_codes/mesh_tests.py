from pytest import raises

from dsp.shared.store.mesh import mesh_header_encode, mesh_header_decode, HeaderUnparseable, HeaderNotEncodeable


def test_mesh_header_encode_safe_chars():
    kv_dict = {
        'foo': 'bar',
        'baz': 12345,
    }
    assert mesh_header_encode(kv_dict) == 'baz=12345;foo=bar;'


def test_mesh_header_encode_unsafe_chars():
    kv_dict = {
        'foo': 'bar',
        'baz': '&;=',
    }
    with raises(HeaderNotEncodeable):
        mesh_header_encode(kv_dict)


def test_mesh_header_decode():
    assert mesh_header_decode('baz=12345;foo=bar;') == {
        'foo': 'bar',
        'baz': '12345',
    }
    assert mesh_header_decode('') == {}
    with raises(HeaderUnparseable):
        mesh_header_decode('broken')
    with raises(HeaderUnparseable):
        mesh_header_decode('foo=bar;baz')

import os
from threading import Lock

import lmdb

_lmdb_clinical_transaction = None
_lmdb_clinical_lock = Lock()
_lmdb_ref_data_transaction = None
_lmdb_ref_data_lock = Lock()
_lmdb_dss_org_transaction = None
_lmdb_dss_org_lock = Lock()


def lmdb_environment(db_location, readonly=True, max_dbs=1, map_size=1073741824, lock=False, **kwargs):
    """
    Args:
        db_location (str): file path for the database file
        readonly (Optional[bool]): whether to open the database in readonly mode
        max_dbs (Optional[int]): maximum number of databases, default 1 (don't allow sub databases)
        map_size (Optional[int]): memory map size in bytes, file of the memory map file
        lock (Optional[bool]): whether to lock the database default False, generally we only want this in
         readonly / single writer mode
        **kwargs: other args to open the database with

    Returns:
        lmdb.Environment: the lmdb environment
    """
    return lmdb.Environment(db_location, map_size=map_size, max_dbs=max_dbs, lock=lock, readonly=readonly, **kwargs)


def lmdb_location(db_name):
    relative_path = os.path.realpath(os.path.join(os.path.dirname(__file__), '../', db_name))

    if os.path.exists(relative_path):
        return relative_path

    return os.path.join('/var', db_name)


def lmdb_transaction(prefix: str) -> lmdb.Transaction:
    """
    Args:
        prefix: string that will be used to build the filename and identifiers for the readonly transaction object
                as well as the readonly lock object
    """

    def inner(readonly: bool = True):
        lmdb_filename = '{}.lmdb'.format(prefix)
        transaction = globals()['_lmdb_{}_transaction'.format(prefix)]
        lock = globals()['_lmdb_{}_lock'.format(prefix)]

        if not readonly:
            env = lmdb_environment(lmdb_location(lmdb_filename), readonly=False)
            return env.begin(write=True)

        if not transaction:
            with lock:
                if not transaction:
                    env = lmdb_environment(lmdb_location(lmdb_filename))
                    transaction = env.begin()

        return transaction

    return inner


lmdb_ref_data = lmdb_transaction('ref_data')

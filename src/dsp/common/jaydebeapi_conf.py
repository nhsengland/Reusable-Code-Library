import os
import sys
import traceback
from datetime import datetime
from decimal import Decimal

import jaydebeapi
import jpype
from jaydebeapi import Connection

from dsp.common import maven_jar, DataTypeConversionError
from dsp.shared.aws import local_mode
from dsp.shared.logger import add_fields
from dsp.shared import safe_issubclass

_JAYDEBEAPI_INTIALISED = False


def connect_to_access_db(path: str, is_local_mode: bool = None, driver_args = None) -> Connection:
    """
    Connect to an MS Access database via the jaydebapi library using the ucanaccess driver

    Args:
        path (str): Path to the database
        is_local_mode (bool): load jars from local venv or from dbfs

    Returns:
        Connection: jaydebeapi connection to database
    """
    driver_args = driver_args or {}
    is_local_mode = local_mode() if is_local_mode is None else is_local_mode

    _init_jaydebeapi()

    jars = [
        maven_jar('ucanaccess-4.0.4.jar', is_local_mode=is_local_mode),
        maven_jar('hsqldb-2.4.1.jar', is_local_mode=is_local_mode),
        maven_jar('jackcess-2.2.0.jar', is_local_mode=is_local_mode),
        maven_jar('commons-logging-1.1.3.jar', is_local_mode=is_local_mode),
        maven_jar('commons-lang-2.6.jar', is_local_mode=is_local_mode),
        maven_jar('log4j-1.2.17.jar', is_local_mode=is_local_mode),
        maven_jar('poi-4.0.0.jar', is_local_mode=is_local_mode)
    ]

    def _start_jvm(minimum_heap_size, maximum_heap_size):
        jvm_options = [
            '-Xms{minimum_heap_size}'.format(minimum_heap_size=minimum_heap_size),
            '-Xmx{maximum_heap_size}'.format(maximum_heap_size=maximum_heap_size),
            '-Djava.class.path={classpath}'.format(classpath=os.pathsep.join(jars))
        ]
        if not jpype.isJVMStarted():
            jpype.startJVM(
                jpype.getDefaultJVMPath(),
                *jvm_options
            )

    # Smaller files locally, so don't use all the local resources
    start_mem = '2g' if is_local_mode else '4g'
    max_mem = '2g' if is_local_mode else '16g'

    _start_jvm(start_mem, max_mem)

    return jaydebeapi.connect(
        jclassname="net.ucanaccess.jdbc.UcanaccessDriver",
        url="jdbc:ucanaccess://{};memory=true".format(path),
        driver_args=driver_args
    )


def _init_jaydebeapi():
    """
    Initialise the jaydebeapi module, if this has not already been done.

    The default configuration of the jaydebeapi module returns data in datatypes other than those we'd require, for
    instance returning a string instead of a datetime. This configuration is part of global state, and must be adjusted
    prior to the first connection made with the module.
    """
    global _JAYDEBEAPI_INTIALISED

    if _JAYDEBEAPI_INTIALISED:
        return

    def _handle_type_conversion_error(result_set, column, java_val):
        metadata = result_set.getMetaData()
        error_context = "Failed to parse {} from column {}.{}".format(
            java_val,
            metadata.getTableName(column),
            metadata.getColumnName(column),
        )
        raise DataTypeConversionError(error_context)

    def _custom_to_datetime(result_set, column):
        java_val = result_set.getTimestamp(column)
        if not java_val:
            return None

        try:
            python_val = datetime.strptime(str(java_val)[:19], "%Y-%m-%d %H:%M:%S")
            return python_val.replace(microsecond=int(str(java_val.getNanos())[:6]))
        except:
            _handle_type_conversion_error(result_set, column, java_val)

    def _custom_to_decimal(result_set, column):
        java_val = result_set.getString(column)
        if not java_val:
            return None
        try:
            return Decimal(java_val)
        except:
            _handle_type_conversion_error(result_set, column, java_val)

    jaydebeapi._DEFAULT_CONVERTERS.update(  # pylint: disable=W0212
        {'TIMESTAMP': _custom_to_datetime, 'NUMERIC': _custom_to_decimal})

    _JAYDEBEAPI_INTIALISED = True


def catch_sql_exception_jpype():
    SQLException = jpype.java.sql.SQLException
    exc_info = sys.exc_info()
    if safe_issubclass(exc_info[1].__javaclass__, SQLException):
        add_fields(access_db_connection_error=traceback.format_exc())
        return

    raise

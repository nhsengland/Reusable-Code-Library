"""
This file will be overwritten during the process that builds release packages.
"""
BUILD_NUMBER = "0"
BUILD_TIMESTAMP = "0"
COMMIT_SHA = "abcdef1"
BUILD_LABEL = "{}-{}".format(BUILD_NUMBER, COMMIT_SHA)
FULL_VERSION_STRING = "{}+{}.{}".format(BUILD_NUMBER, BUILD_TIMESTAMP, COMMIT_SHA)

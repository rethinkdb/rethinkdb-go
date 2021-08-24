#!/usr/bin/env bash

set -e

if [[ ! -d $REQL_TEST_DIR ]]
then
    echo "REQL_TEST_DIR must be set to the local copy of the https://github.com/rethinkdb/rethinkdb/tree/next/test/rql_test/src/ directory."
    exit 1
fi

SCRIPT_DIR=$(dirname "$0")

$SCRIPT_DIR/gen_tests.py --test-dir=$REQL_TEST_DIR

goimports -w . > /dev/null

exit 0

#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""Integration tests for scan_bdgd() against the broken scan BDGD."""

import pytest

from tests.data.scan_manifest_utils import load_manifest, SCAN_MANIFEST_PATH
from tests.data.validation_helpers import assert_validation_message, run_patched_scan_case


SCAN_CASES = load_manifest(SCAN_MANIFEST_PATH)
GDB_SCAN_CASES = [case for case in SCAN_CASES if not case.get("patch_in_memory")]
PATCHED_SCAN_CASES = [case for case in SCAN_CASES if case.get("patch_in_memory")]


@pytest.mark.parametrize("case", GDB_SCAN_CASES, ids=lambda case: case["id"])
def test_scan_validation_from_broken_gdb(case, scan_errors):
    """Each injectable scan rule from bdgd2dss_error.json must appear in scan output."""
    assert_validation_message(
        scan_errors,
        contains=case["expected_message_contains"],
        table=case.get("expected_table"),
    )


@pytest.mark.parametrize("case", PATCHED_SCAN_CASES, ids=lambda case: case["id"])
def test_scan_validation_patched_in_memory(case, clean_mux_gdb, tmp_path):
    """Float/int scan branches are validated with in-memory patches after GDB load."""
    errors = run_patched_scan_case(clean_mux_gdb, tmp_path, case)
    assert_validation_message(
        errors,
        contains=case["expected_message_contains"],
        table=case.get("expected_table"),
    )

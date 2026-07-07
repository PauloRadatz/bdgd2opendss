#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""Integration tests for run_validation() against the broken Etapa17 BDGD."""

import pytest

from tests.data.scan_manifest_utils import ETAPA17_MANIFEST_PATH, load_manifest
from tests.data.validation_helpers import (
    assert_validation_message,
    run_patched_etapa17_case,
)

ETAPA17_CASES = load_manifest(ETAPA17_MANIFEST_PATH)
GDB_ETAPA17_CASES = [case for case in ETAPA17_CASES if not case.get("patch_in_memory")]
PATCHED_ETAPA17_CASES = [case for case in ETAPA17_CASES if case.get("patch_in_memory")]


@pytest.mark.parametrize("case", GDB_ETAPA17_CASES, ids=lambda case: case["id"])
def test_etapa17_validation_from_broken_bdgd(case, etapa17_errors):
    """Each Etapa17 validation step must report its expected business-rule error."""
    assert_validation_message(
        etapa17_errors,
        contains=case["expected_message_contains"],
        table=case.get("expected_table"),
    )


@pytest.mark.parametrize("case", PATCHED_ETAPA17_CASES, ids=lambda case: case["id"])
def test_etapa17_validation_patched_in_memory(case, clean_mux_gdb, tmp_path):
    """Some Etapa17 rules are validated with in-memory patches on the clean GDB."""
    errors = run_patched_etapa17_case(clean_mux_gdb, tmp_path, case)
    assert_validation_message(
        errors,
        contains=case["expected_message_contains"],
        table=case.get("expected_table"),
    )

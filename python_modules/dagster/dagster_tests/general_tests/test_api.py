import importlib
import re
import subprocess
import sys

import pytest

from dagster._module_alias_map import AliasedModuleFinder, get_meta_path_insertion_index


def test_no_experimental_warnings():
    process = subprocess.run(
        [sys.executable, "-c", "import dagster"], check=False, capture_output=True
    )
    assert not re.search(r"ExperimentalWarning", process.stderr.decode("utf-8"))


def test_deprecated_imports():
    with pytest.warns(
        DeprecationWarning, match=re.escape("dagster_type_materializer is deprecated")
    ):
        from dagster import dagster_type_materializer  # noqa: F401
    with pytest.warns(DeprecationWarning, match=re.escape("DagsterTypeMaterializer is deprecated")):
        from dagster import DagsterTypeMaterializer  # noqa: F401


@pytest.fixture
def patch_sys_meta_path():
    aliased_finder = AliasedModuleFinder({"dagster.foo": "dagster.core"})
    sys.meta_path.insert(get_meta_path_insertion_index(), aliased_finder)
    yield
    sys.meta_path.remove(aliased_finder)


@pytest.mark.usefixtures("patch_sys_meta_path")
def test_aliased_module_finder_import():
    assert importlib.import_module("dagster.foo") == importlib.import_module("dagster.core")


@pytest.mark.usefixtures("patch_sys_meta_path")
def test_aliased_module_finder_nested_import():
    assert importlib.import_module("dagster.foo.definitions") == importlib.import_module(
        "dagster.core.definitions"
    )


def test_deprecated_top_level_submodule_import():
    assert importlib.import_module("dagster.check") == importlib.import_module("dagster._check")

from docs_snippets.concepts.assets.asset_io_manager import my_repository


def test():
    # pylint: disable=protected-access
    assert len(my_repository._assets_defs_by_key) == 2

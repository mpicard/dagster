import pandas as pd

from dagster import build_op_context

from .assets_v2 import items
from .resources.resources_v2 import StubHNClient


# start
# test_assets.py


def test_items():
    context = build_op_context(
        resources={"hn_client": StubHNClient()},
        op_config={"N": StubHNClient().fetch_max_item_id()},
    )
    hn_dataset = items(context)
    assert isinstance(hn_dataset, pd.DataFrame)

    expected_data = pd.DataFrame(StubHNClient().data.values()).rename(
        columns={"by": "user_id"}
    )

    assert (hn_dataset == expected_data).all().all()


# end

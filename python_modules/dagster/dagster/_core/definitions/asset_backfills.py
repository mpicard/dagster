from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Dict,
    Iterable,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    cast,
)

import dagster._check as check
from dagster._core.definitions.asset_reconciliation_sensor import (
    bfs_asset_partition_graph,
    build_run_requests,
    find_parent_materialized_asset_partitions,
)
from dagster._core.definitions.events import AssetKey, AssetKeyPartitionKey
from dagster._utils.caching_instance_queryer import CachingInstanceQueryer

from .asset_selection import AssetGraph, AssetSelection
from .partition import PartitionsSubset
from .run_request import RunRequest

if TYPE_CHECKING:
    from dagster._core.instance import DagsterInstance


class AssetBackfill(NamedTuple):
    target_subsets_by_asset_key: Mapping[AssetKey, PartitionsSubset]

    def is_complete(
        self, materialized_subsets_by_asset_key: Mapping[AssetKey, PartitionsSubset]
    ) -> bool:
        print("in is_complete")
        print("materialized:" + str(materialized_subsets_by_asset_key))
        print("target:" + str(self.target_subsets_by_asset_key))
        return all(
            asset_key in materialized_subsets_by_asset_key
            and len(materialized_subsets_by_asset_key[asset_key]) == len(target_subset)
            for asset_key, target_subset in self.target_subsets_by_asset_key.items()
        )

    def is_target(self, asset_partition: AssetKeyPartitionKey) -> bool:
        return asset_partition.partition_key in self.target_subsets_by_asset_key.get(
            asset_partition.asset_key, []
        )

    @property
    def asset_keys(self) -> Iterable[AssetKey]:
        return self.target_subsets_by_asset_key.keys()

    def get_root_asset_partitions(self, asset_graph: AssetGraph) -> Iterable[AssetKeyPartitionKey]:
        root_asset_keys = AssetSelection.keys(*self.asset_keys).sources().resolve(asset_graph)
        # TODO: this doesn't handle self-deps
        return [
            AssetKeyPartitionKey(asset_key, partition_key)
            for asset_key in root_asset_keys
            for partition_key in self.target_subsets_by_asset_key[asset_key].get_partition_keys()
        ]


class AssetBackfillCursor(NamedTuple):
    roots_were_requested: bool
    latest_storage_id: Optional[int]
    materialized_subsets_by_asset_key: Mapping[AssetKey, PartitionsSubset]

    @classmethod
    def empty(cls) -> "AssetBackfillCursor":
        return cls(
            roots_were_requested=False,
            materialized_subsets_by_asset_key={},
            latest_storage_id=None,
        )


class AssetBackfillIterationResult(NamedTuple):
    run_requests: Sequence[RunRequest]
    cursor: AssetBackfillCursor


def single_backfill_iteration(
    backfill: AssetBackfill,
    cursor: AssetBackfillCursor,
    asset_graph: AssetGraph,
    instance: "DagsterInstance",
) -> AssetBackfillIterationResult:
    print()
    print("STARTING BACKFILL ITERATION")
    instance_queryer = CachingInstanceQueryer(instance=instance)

    initial_candidates: Set[AssetKeyPartitionKey] = set()
    request_roots = not cursor.roots_were_requested
    if request_roots:
        root_asset_partitions = backfill.get_root_asset_partitions(asset_graph)
        initial_candidates.update(root_asset_partitions)

    (
        parent_materialized_asset_partitions,
        next_latest_storage_id,
    ) = find_parent_materialized_asset_partitions(
        asset_graph=asset_graph,
        instance_queryer=instance_queryer,
        target_asset_selection=AssetSelection.keys(*backfill.asset_keys),
        latest_storage_id=cursor.latest_storage_id,
    )
    print(f"parent_materialized_asset_partitions: {parent_materialized_asset_partitions}")
    initial_candidates.update(parent_materialized_asset_partitions)

    recently_materialized_partitions_by_asset_key: Mapping[AssetKey, AbstractSet[str]] = {
        asset_key: {
            cast(str, record.partition_key)
            for record in instance_queryer.get_materialization_records(
                asset_key=asset_key, after_cursor=cursor.latest_storage_id
            )
        }
        for asset_key in backfill.asset_keys
        # TODO: filter by backfill tag?
        # TODO: filter by partition?
    }
    print("recently_materialized_partitions_by_asset_key")
    print(recently_materialized_partitions_by_asset_key)
    updated_materialized_subsets_by_asset_key: Dict[AssetKey, PartitionsSubset] = {}
    for asset_key in (
        cursor.materialized_subsets_by_asset_key | recently_materialized_partitions_by_asset_key
    ):
        partitions_def = asset_graph.get_partitions_def(asset_key)
        subset = cursor.materialized_subsets_by_asset_key.get(
            asset_key, partitions_def.empty_subset()
        )
        updated_materialized_subsets_by_asset_key[asset_key] = subset.with_partition_keys(
            recently_materialized_partitions_by_asset_key.get(asset_key, [])
        )

    for asset_key in backfill.asset_keys:
        for failure in instance_queryer.get_materialization_failures(
            asset_key, after_cursor=cursor.latest_storage_id
        ):
            # find all descendant asset partitions in the backfills and cancel them
            ...

    asset_partitions_to_request: Set[AssetKeyPartitionKey] = set()

    def handle_candidate(candidate: AssetKeyPartitionKey) -> bool:
        print(f"handling candidate: {candidate}")
        if (
            backfill.is_target(candidate)
            and
            # all of its parents materialized first
            all(
                (
                    (
                        parent in asset_partitions_to_request
                        # if they don't have the same partitioning, then we can't launch a run that
                        # targets both, so we need to wait until the parent is reconciled before
                        # launching a run for the child
                        and asset_graph.have_same_partitioning(
                            parent.asset_key, candidate.asset_key
                        )
                    )
                    or parent.partition_key
                    in updated_materialized_subsets_by_asset_key.get(parent.asset_key, [])
                    or parent.partition_key
                    not in backfill.target_subsets_by_asset_key.get(parent.asset_key, [])
                )
                for parent in asset_graph.get_parents_partitions(
                    candidate.asset_key, candidate.partition_key
                )
            )
            and candidate
            not in updated_materialized_subsets_by_asset_key.get(candidate.asset_key, [])
        ):
            asset_partitions_to_request.add(candidate)
            return True

        return False

    bfs_asset_partition_graph(
        handle_candidate, initial_nodes=initial_candidates, asset_graph=asset_graph
    )

    run_requests = build_run_requests(asset_partitions_to_request, asset_graph, {})
    if request_roots:
        check.invariant(
            asset_partitions_to_request >= set(root_asset_partitions), "all roots are included"
        )

    updated_cursor = AssetBackfillCursor(
        latest_storage_id=next_latest_storage_id or cursor.latest_storage_id,
        roots_were_requested=cursor.roots_were_requested or request_roots,
        materialized_subsets_by_asset_key=updated_materialized_subsets_by_asset_key,
    )
    return AssetBackfillIterationResult(run_requests, updated_cursor)

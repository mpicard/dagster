from abc import ABC, abstractmethod
from typing import NamedTuple, Optional, Union, cast

import dagster._check as check
from dagster._annotations import experimental, public
from dagster._core.definitions.multi_dimensional_partitions import (
    MultiPartitionKey,
    MultiPartitionsDefinition,
)
from dagster._core.definitions.partition import PartitionsDefinition, PartitionsSubset
from dagster._core.definitions.partition_key_range import PartitionKeyRange
from dagster._core.errors import DagsterInvalidInvocationError
from dagster._serdes import whitelist_for_serdes


@experimental
class PartitionMapping(ABC):
    """Defines a correspondence between the partitions in an asset and the partitions in an asset
    that it depends on.
    """

    @public
    @abstractmethod
    def get_upstream_partitions_for_partition_range(
        self,
        downstream_partition_key_range: Optional[PartitionKeyRange],
        downstream_partitions_def: Optional[PartitionsDefinition],
        upstream_partitions_def: PartitionsDefinition,
    ) -> PartitionKeyRange:
        """Returns the range of partition keys in the upstream asset that include data necessary
        to compute the contents of the given partition key range in the downstream asset.

        Args:
            downstream_partition_key_range (PartitionKeyRange): The range of partition keys in the
                downstream asset.
            downstream_partitions_def (PartitionsDefinition): The partitions definition for the
                downstream asset.
            upstream_partitions_def (PartitionsDefinition): The partitions definition for the
                upstream asset.
        """

    @public
    @abstractmethod
    def get_downstream_partitions_for_partition_range(
        self,
        upstream_partition_key_range: PartitionKeyRange,
        downstream_partitions_def: Optional[PartitionsDefinition],
        upstream_partitions_def: PartitionsDefinition,
    ) -> PartitionKeyRange:
        """Returns the range of partition keys in the downstream asset that use the data in the given
        partition key range of the upstream asset.

        Args:
            upstream_partition_key_range (PartitionKeyRange): The range of partition keys in the
                upstream asset.
            downstream_partitions_def (PartitionsDefinition): The partitions definition for the
                downstream asset.
            upstream_partitions_def (PartitionsDefinition): The partitions definition for the
                upstream asset.
        """

    @public
    def get_upstream_partitions_for_partition_subset(
        self,
        downstream_partition_key_subset: Optional[Union[PartitionKeyRange, PartitionsSubset]],
        downstream_partitions_def: Optional[PartitionsDefinition],
        upstream_partitions_def: PartitionsDefinition,
    ) -> PartitionsSubset:
        """
        Returns the subset of partition keys in the upstream asset that include data necessary
        to compute the contents of the given partition key subset in the downstream asset.

        Args:
            downstream_partition_key_subset (Optional[Union[PartitionKeyRange, PartitionsSubset]]):
                The subset of partition keys in the downstream asset.
            downstream_partitions_def (PartitionsDefinition): The partitions definition for the
                downstream asset.
            upstream_partitions_def (PartitionsDefinition): The partitions definition for the
                upstream asset.
        """
        if isinstance(downstream_partition_key_subset, PartitionsSubset):
            raise NotImplementedError(
                "Must be implemented by subclass if passing a PartitionsSubset"
            )
        else:
            upstream_key_range = self.get_upstream_partitions_for_partition_range(
                downstream_partition_key_subset,
                downstream_partitions_def,
                upstream_partitions_def,
            )
            return upstream_partitions_def.empty_subset().with_partition_keys(
                upstream_partitions_def.get_partition_keys_in_range(upstream_key_range)
            )

    @public
    def get_downstream_partitions_for_partition_subset(
        self,
        upstream_partition_key_subset: Union[PartitionKeyRange, PartitionsSubset],
        downstream_partitions_def: Optional[PartitionsDefinition],
        upstream_partitions_def: PartitionsDefinition,
    ) -> PartitionsSubset:
        """
        Returns the subset of partition keys in the downstream asset that use the data in the given
        partition key subset of the upstream asset.

        Args:
            upstream_partition_key_subset (Union[PartitionKeyRange, PartitionsSubset]): The
                subset of partition keys in the upstream asset.
            downstream_partitions_def (PartitionsDefinition): The partitions definition for the
                downstream asset.
            upstream_partitions_def (PartitionsDefinition): The partitions definition for the
                upstream asset.
        """
        if isinstance(upstream_partition_key_subset, PartitionsSubset):
            raise NotImplementedError(
                "Must be implemented by subclass if passing a PartitionsSubset"
            )
        else:
            downstream_range = self.get_downstream_partitions_for_partition_range(
                upstream_partition_key_subset,
                downstream_partitions_def,
                upstream_partitions_def,
            )
            if downstream_partitions_def is None:
                raise DagsterInvalidInvocationError(
                    "downstream partitions definition must be defined"
                )
            return downstream_partitions_def.empty_subset().with_partition_keys(
                downstream_partitions_def.get_partition_keys_in_range(downstream_range)
            )


@experimental
@whitelist_for_serdes
class IdentityPartitionMapping(PartitionMapping, NamedTuple("_IdentityPartitionMapping", [])):
    def get_upstream_partitions_for_partition_range(
        self,
        downstream_partition_key_range: Optional[PartitionKeyRange],
        downstream_partitions_def: Optional[PartitionsDefinition],
        upstream_partitions_def: PartitionsDefinition,
    ) -> PartitionKeyRange:
        if downstream_partitions_def is None or downstream_partition_key_range is None:
            check.failed("downstream asset is not partitioned")

        return downstream_partition_key_range

    def get_downstream_partitions_for_partition_range(
        self,
        upstream_partition_key_range: PartitionKeyRange,
        downstream_partitions_def: Optional[PartitionsDefinition],
        upstream_partitions_def: PartitionsDefinition,
    ) -> PartitionKeyRange:
        return upstream_partition_key_range


@experimental
@whitelist_for_serdes
class AllPartitionMapping(PartitionMapping, NamedTuple("_AllPartitionMapping", [])):
    def get_upstream_partitions_for_partition_range(
        self,
        downstream_partition_key_range: Optional[PartitionKeyRange],
        downstream_partitions_def: Optional[PartitionsDefinition],
        upstream_partitions_def: PartitionsDefinition,
    ) -> PartitionKeyRange:
        return PartitionKeyRange(
            upstream_partitions_def.get_first_partition_key(),
            upstream_partitions_def.get_last_partition_key(),
        )

    def get_downstream_partitions_for_partition_range(
        self,
        upstream_partition_key_range: PartitionKeyRange,
        downstream_partitions_def: Optional[PartitionsDefinition],
        upstream_partitions_def: PartitionsDefinition,
    ) -> PartitionKeyRange:
        raise NotImplementedError()


@experimental
@whitelist_for_serdes
class LastPartitionMapping(PartitionMapping, NamedTuple("_LastPartitionMapping", [])):
    def get_upstream_partitions_for_partition_range(
        self,
        downstream_partition_key_range: Optional[PartitionKeyRange],
        downstream_partitions_def: Optional[PartitionsDefinition],
        upstream_partitions_def: PartitionsDefinition,
    ) -> PartitionKeyRange:
        last_partition_key = upstream_partitions_def.get_last_partition_key()
        return PartitionKeyRange(last_partition_key, last_partition_key)

    def get_downstream_partitions_for_partition_range(
        self,
        upstream_partition_key_range: PartitionKeyRange,
        downstream_partitions_def: Optional[PartitionsDefinition],
        upstream_partitions_def: PartitionsDefinition,
    ) -> PartitionKeyRange:
        raise NotImplementedError()


@experimental
class SingleDimensionToMultiPartitionMapping(PartitionMapping):
    """
    Defines a correspondence between an upstream single-dimensional partitions definition
    and a downstream MultiPartitionsDefinition. The upstream partitions definition must be
    a dimension of the downstream MultiPartitionsDefinition.

    Args:
        partition_dimension_name (str): The name of the partition dimension in the downstream
            MultiPartitionsDefinition that matches the upstream partitions definition.
    """

    def __init__(self, partition_dimension_name: str):
        self.partition_dimension_name = partition_dimension_name

    def _check_upstream_partitions_def_equals_selected_dimension(
        self,
        upstream_partitions_def: PartitionsDefinition,
        downstream_partitions_def: MultiPartitionsDefinition,
    ) -> None:
        matching_dimensions = [
            partitions_def
            for partitions_def in downstream_partitions_def.partitions_defs
            if partitions_def.name == self.partition_dimension_name
        ]
        if len(matching_dimensions) != 1:
            check.failed(f"Partition dimension '{self.partition_dimension_name}' not found")
        matching_dimension_def = next(iter(matching_dimensions))

        if upstream_partitions_def != matching_dimension_def.partitions_def:
            check.failed(
                "The upstream partitions definition does not have the same partitions definition "
                f"as dimension {matching_dimension_def.name}"
            )

    def get_upstream_partitions_for_partition_range(
        self,
        downstream_partition_key_range: Optional[PartitionKeyRange],
        downstream_partitions_def: Optional[PartitionsDefinition],
        upstream_partitions_def: PartitionsDefinition,
    ) -> PartitionKeyRange:
        if downstream_partition_key_range is None:
            check.failed("Must provide downstream partition key range")
        if not (
            isinstance(downstream_partition_key_range.start, MultiPartitionKey)
            and isinstance(downstream_partition_key_range.end, MultiPartitionKey)
        ):
            check.failed(
                "Start and end fields of downstream partition key range must be MultiPartitionKeys"
            )

        range_start = cast(MultiPartitionKey, downstream_partition_key_range.start)
        range_end = cast(MultiPartitionKey, downstream_partition_key_range.end)

        return PartitionKeyRange(
            range_start.keys_by_dimension[self.partition_dimension_name],
            range_end.keys_by_dimension[self.partition_dimension_name],
        )

    def get_downstream_partitions_for_partition_range(
        self,
        upstream_partition_key_range: PartitionKeyRange,
        downstream_partitions_def: Optional[PartitionsDefinition],
        upstream_partitions_def: PartitionsDefinition,
    ) -> PartitionKeyRange:
        """
        TODO docstring
        """
        raise NotImplementedError()

    def get_upstream_partitions_for_partition_subset(
        self,
        downstream_partition_key_subset: Optional[Union[PartitionKeyRange, PartitionsSubset]],
        downstream_partitions_def: Optional[PartitionsDefinition],
        upstream_partitions_def: PartitionsDefinition,
    ) -> PartitionsSubset:
        if downstream_partitions_def is None or downstream_partition_key_subset is None:
            check.failed("downstream asset is not partitioned")

        if not isinstance(downstream_partitions_def, MultiPartitionsDefinition):
            check.failed("downstream asset is not multi-partitioned")

        self._check_upstream_partitions_def_equals_selected_dimension(
            upstream_partitions_def, downstream_partitions_def
        )

        if isinstance(downstream_partition_key_subset, PartitionKeyRange):
            return upstream_partitions_def.empty_subset().with_partition_keys(
                upstream_partitions_def.get_partition_keys_in_range(
                    self.get_upstream_partitions_for_partition_range(
                        downstream_partition_key_subset,
                        downstream_partitions_def,
                        upstream_partitions_def,
                    )
                )
            )
        else:
            upstream_partitions = set()
            for partition_key in downstream_partition_key_subset.get_partition_keys():
                if not isinstance(partition_key, MultiPartitionKey):
                    check.failed(
                        "Partition keys in downstream partition key subset must be MultiPartitionKeys"
                    )
                upstream_partitions.add(
                    partition_key.keys_by_dimension[self.partition_dimension_name]
                )
            return upstream_partitions_def.empty_subset().with_partition_keys(upstream_partitions)

    def get_downstream_partitions_for_partition_subset(
        self,
        upstream_partition_key_subset: Union[PartitionKeyRange, PartitionsSubset],
        downstream_partitions_def: Optional[PartitionsDefinition],
        upstream_partitions_def: PartitionsDefinition,
    ) -> PartitionsSubset:
        if downstream_partitions_def is None or not isinstance(
            downstream_partitions_def, MultiPartitionsDefinition
        ):
            check.failed("downstream asset is not multi-partitioned")

        self._check_upstream_partitions_def_equals_selected_dimension(
            upstream_partitions_def, downstream_partitions_def
        )

        if isinstance(upstream_partition_key_subset, PartitionKeyRange):
            upstream_keys = upstream_partitions_def.get_partition_keys_in_range(
                upstream_partition_key_subset
            )
        else:
            upstream_keys = list(upstream_partition_key_subset.get_partition_keys())

        matching_keys = []
        for key in downstream_partitions_def.get_partition_keys():
            key = cast(MultiPartitionKey, key)
            if key.keys_by_dimension[self.partition_dimension_name] in upstream_keys:
                matching_keys.append(key)

        return downstream_partitions_def.empty_subset().with_partition_keys(set(matching_keys))


def infer_partition_mapping(
    partition_mapping: Optional[PartitionMapping], partitions_def: Optional[PartitionsDefinition]
) -> PartitionMapping:
    if partition_mapping is not None:
        return partition_mapping
    elif partitions_def is not None:
        return partitions_def.get_default_partition_mapping()
    else:
        return AllPartitionMapping()


def get_builtin_partition_mapping_types():
    from dagster._core.definitions.time_window_partition_mapping import TimeWindowPartitionMapping

    return (
        AllPartitionMapping,
        IdentityPartitionMapping,
        LastPartitionMapping,
        TimeWindowPartitionMapping,
    )

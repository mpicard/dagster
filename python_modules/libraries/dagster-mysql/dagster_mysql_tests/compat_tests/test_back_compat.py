# pylint: disable=protected-access
import os
import subprocess
import tempfile
from urllib.parse import urlparse

import pytest
from sqlalchemy import create_engine, inspect

from dagster import AssetKey, AssetMaterialization, AssetObservation, Output, job, op
from dagster._core.errors import DagsterInvalidInvocationError
from dagster._core.instance import DagsterInstance
from dagster._core.storage.event_log.migration import ASSET_KEY_INDEX_COLS
from dagster._utils import file_relative_path


def get_columns(instance, table_name: str):
    return set(c["name"] for c in inspect(instance.run_storage._engine).get_columns(table_name))


def get_indexes(instance, table_name: str):
    return set(c["name"] for c in inspect(instance.run_storage._engine).get_indexes(table_name))


def get_tables(instance):
    return instance.run_storage._engine.table_names()


def _reconstruct_from_file(conn_string, path, _username="root", _password="test"):
    parse_result = urlparse(conn_string)
    hostname = parse_result.hostname
    port = parse_result.port
    engine = create_engine(conn_string)
    engine.execute("drop schema test;")
    engine.execute("create schema test;")
    env = os.environ.copy()
    env["MYSQL_PWD"] = "test"
    subprocess.check_call(f"mysql -uroot -h{hostname} -P{port} test < {path}", shell=True, env=env)
    return hostname, port


def test_0_13_17_mysql_convert_float_cols(conn_string):
    hostname, port = _reconstruct_from_file(
        conn_string,
        file_relative_path(__file__, "snapshot_0_13_18_start_end_timestamp.sql"),
    )

    with tempfile.TemporaryDirectory() as tempdir:
        with open(
            file_relative_path(__file__, "dagster.yaml"), "r", encoding="utf8"
        ) as template_fd:
            with open(os.path.join(tempdir, "dagster.yaml"), "w", encoding="utf8") as target_fd:
                template = template_fd.read().format(hostname=hostname, port=port)
                target_fd.write(template)

        instance = DagsterInstance.from_config(tempdir)
        record = instance.get_run_records(limit=1)[0]
        assert int(record.start_time) == 1643760000
        assert int(record.end_time) == 1643760000

        instance.upgrade()

        record = instance.get_run_records(limit=1)[0]
        assert record.start_time is None
        assert record.end_time is None

        instance.reindex()

        record = instance.get_run_records(limit=1)[0]
        assert int(record.start_time) == 1643788829
        assert int(record.end_time) == 1643788834


def test_instigators_table_backcompat(conn_string):
    hostname, port = _reconstruct_from_file(
        conn_string,
        file_relative_path(__file__, "snapshot_0_14_6_instigators_table.sql"),
    )

    with tempfile.TemporaryDirectory() as tempdir:
        with open(
            file_relative_path(__file__, "dagster.yaml"), "r", encoding="utf8"
        ) as template_fd:
            with open(os.path.join(tempdir, "dagster.yaml"), "w", encoding="utf8") as target_fd:
                template = template_fd.read().format(hostname=hostname, port=port)
                target_fd.write(template)

        instance = DagsterInstance.from_config(tempdir)

        assert not instance.schedule_storage.has_instigators_table()

        instance.upgrade()

        assert instance.schedule_storage.has_instigators_table()


def test_asset_observation_backcompat(conn_string):
    hostname, port = _reconstruct_from_file(
        conn_string,
        file_relative_path(__file__, "snapshot_0_11_16_pre_add_asset_key_index_cols.sql"),
    )

    @op
    def asset_op(_):
        yield AssetObservation(asset_key=AssetKey(["a"]))
        yield Output(1)

    @job
    def asset_job():
        asset_op()

    with tempfile.TemporaryDirectory() as tempdir:
        with open(
            file_relative_path(__file__, "dagster.yaml"), "r", encoding="utf8"
        ) as template_fd:
            with open(os.path.join(tempdir, "dagster.yaml"), "w", encoding="utf8") as target_fd:
                template = template_fd.read().format(hostname=hostname, port=port)
                target_fd.write(template)

        with DagsterInstance.from_config(tempdir) as instance:
            storage = instance._event_storage

            assert not instance.event_log_storage.has_secondary_index(ASSET_KEY_INDEX_COLS)

            asset_job.execute_in_process(instance=instance)
            assert storage.has_asset_key(AssetKey(["a"]))


def test_jobs_selector_id_migration(conn_string):
    import sqlalchemy as db

    from dagster._core.storage.schedules.migration import SCHEDULE_JOBS_SELECTOR_ID
    from dagster._core.storage.schedules.schema import InstigatorsTable, JobTable, JobTickTable

    hostname, port = _reconstruct_from_file(
        conn_string,
        file_relative_path(__file__, "snapshot_0_14_6_post_schema_pre_data_migration.sql"),
    )

    with tempfile.TemporaryDirectory() as tempdir:
        with open(
            file_relative_path(__file__, "dagster.yaml"), "r", encoding="utf8"
        ) as template_fd:
            with open(os.path.join(tempdir, "dagster.yaml"), "w", encoding="utf8") as target_fd:
                template = template_fd.read().format(hostname=hostname, port=port)
                target_fd.write(template)

        with DagsterInstance.from_config(tempdir) as instance:
            # runs the required data migrations
            instance.upgrade()

            assert instance.schedule_storage.has_built_index(SCHEDULE_JOBS_SELECTOR_ID)
            legacy_count = len(instance.all_instigator_state())
            migrated_instigator_count = instance.schedule_storage.execute(
                db.select([db.func.count()]).select_from(InstigatorsTable)
            )[0][0]
            assert migrated_instigator_count == legacy_count

            migrated_job_count = instance.schedule_storage.execute(
                db.select([db.func.count()])
                .select_from(JobTable)
                .where(JobTable.c.selector_id.isnot(None))
            )[0][0]
            assert migrated_job_count == legacy_count

            legacy_tick_count = instance.schedule_storage.execute(
                db.select([db.func.count()]).select_from(JobTickTable)
            )[0][0]
            assert legacy_tick_count > 0

            # tick migrations are optional
            migrated_tick_count = instance.schedule_storage.execute(
                db.select([db.func.count()])
                .select_from(JobTickTable)
                .where(JobTickTable.c.selector_id.isnot(None))
            )[0][0]
            assert migrated_tick_count == 0

            # run the optional migrations
            instance.reindex()

            migrated_tick_count = instance.schedule_storage.execute(
                db.select([db.func.count()])
                .select_from(JobTickTable)
                .where(JobTickTable.c.selector_id.isnot(None))
            )[0][0]
            assert migrated_tick_count == legacy_tick_count


def test_add_bulk_actions_columns(conn_string):
    new_columns = {"selector_id", "action_type"}
    new_indexes = {"idx_bulk_actions_action_type", "idx_bulk_actions_selector_id"}

    hostname, port = _reconstruct_from_file(
        conn_string,
        # use an old snapshot, it has the bulk actions table but not the new columns
        file_relative_path(__file__, "snapshot_0_14_6_post_schema_pre_data_migration.sql"),
    )

    with tempfile.TemporaryDirectory() as tempdir:
        with open(
            file_relative_path(__file__, "dagster.yaml"), "r", encoding="utf8"
        ) as template_fd:
            with open(os.path.join(tempdir, "dagster.yaml"), "w", encoding="utf8") as target_fd:
                template = template_fd.read().format(hostname=hostname, port=port)
                target_fd.write(template)

        with DagsterInstance.from_config(tempdir) as instance:
            assert get_columns(instance, "bulk_actions") & new_columns == set()
            assert get_indexes(instance, "bulk_actions") & new_indexes == set()

            instance.upgrade()
            assert new_columns <= get_columns(instance, "bulk_actions")
            assert new_indexes <= get_indexes(instance, "bulk_actions")


def test_add_kvs_table(conn_string):
    hostname, port = _reconstruct_from_file(
        conn_string,
        # use an old snapshot
        file_relative_path(__file__, "snapshot_0_14_6_post_schema_pre_data_migration.sql"),
    )

    with tempfile.TemporaryDirectory() as tempdir:
        with open(
            file_relative_path(__file__, "dagster.yaml"), "r", encoding="utf8"
        ) as template_fd:
            with open(os.path.join(tempdir, "dagster.yaml"), "w", encoding="utf8") as target_fd:
                template = template_fd.read().format(hostname=hostname, port=port)
                target_fd.write(template)

        with DagsterInstance.from_config(tempdir) as instance:
            assert "kvs" not in get_tables(instance)

            instance.upgrade()
            assert "kvs" in get_tables(instance)
            assert "idx_kvs_keys_unique" in get_indexes(instance, "kvs")


def test_add_asset_event_tags_table(conn_string):
    @op
    def yields_materialization_w_tags(_):
        yield AssetMaterialization(asset_key=AssetKey(["a"]), tags={"dagster/foo": "bar"})
        yield Output(1)

    @job
    def asset_job():
        yields_materialization_w_tags()

    hostname, port = _reconstruct_from_file(
        conn_string,
        # use an old snapshot
        file_relative_path(__file__, "snapshot_1_0_12_pre_add_asset_event_tags_table.sql"),
    )

    with tempfile.TemporaryDirectory() as tempdir:
        with open(
            file_relative_path(__file__, "dagster.yaml"), "r", encoding="utf8"
        ) as template_fd:
            with open(os.path.join(tempdir, "dagster.yaml"), "w", encoding="utf8") as target_fd:
                template = template_fd.read().format(hostname=hostname, port=port)
                target_fd.write(template)

        with DagsterInstance.from_config(tempdir) as instance:
            assert "asset_event_tags" not in get_tables(instance)
            asset_job.execute_in_process(instance=instance)
            with pytest.raises(
                DagsterInvalidInvocationError, match="In order to search for asset event tags"
            ):
                instance._event_storage.get_event_tags_for_asset(asset_key=AssetKey(["a"]))

            instance.upgrade()
            assert "asset_event_tags" in get_tables(instance)
            asset_job.execute_in_process(instance=instance)
            assert instance._event_storage.get_event_tags_for_asset(asset_key=AssetKey(["a"])) == [
                {"dagster/foo": "bar"}
            ]

            indexes = get_indexes(instance, "asset_event_tags")
            assert "idx_asset_event_tags" in indexes
            assert "idx_asset_event_tags_event_id" in indexes


def test_add_cached_status_data_column(conn_string):
    new_columns = {"cached_status_data"}

    hostname, port = _reconstruct_from_file(
        conn_string,
        # use an old snapshot, it has the bulk actions table but not the new columns
        file_relative_path(__file__, "snapshot_1_0_17_add_cached_status_data_column.sql"),
    )

    with tempfile.TemporaryDirectory() as tempdir:
        with open(
            file_relative_path(__file__, "dagster.yaml"), "r", encoding="utf8"
        ) as template_fd:
            with open(os.path.join(tempdir, "dagster.yaml"), "w", encoding="utf8") as target_fd:
                template = template_fd.read().format(hostname=hostname, port=port)
                target_fd.write(template)

        with DagsterInstance.from_config(tempdir) as instance:
            assert get_columns(instance, "asset_keys") & new_columns == set()

            instance.upgrade()
            assert new_columns <= get_columns(instance, "asset_keys")

#!/usr/bin/env python3
import argparse
import copy
import os
import sys
import structlog
import traceback
from uuid import uuid4

import sqlalchemy
from sqlalchemy import create_engine, select, event
from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy.engine import Engine

logger = structlog.get_logger(__name__)

TABLE_PREFIX = "dbin_"
DEFAULT_BATCH_SIZE = 1000

DEFAULT_DB_IN = "postgresql+psycopg2://user:password@127.0.0.1:5432/dbin"
DEFAULT_DB_OUT = "mysql+mysqldb://user:password@127.0.0.1:3306/dbout"


def setup_fixtures(in_engine: Engine) -> None:
    """Install fixtures for testing."""
    metadata_in = MetaData()
    users = Table(
        "users",
        metadata_in,
        Column(
            "id",
            sqlalchemy.dialects.postgresql.UUID(as_uuid=True),
            primary_key=True,
            default=uuid4,
        ),
        Column("num", Integer),
        Column("full_name", String),
    )
    metadata_in.drop_all(in_engine)
    metadata_in.create_all(in_engine)
    ins = users.insert().values(num=2, full_name="Louis de Funès")

    conn = in_engine.connect()
    conn.execute(ins)
    conn.close()


def get_generic_type(type):
    if isinstance(type, sqlalchemy.dialects.postgresql.UUID):
        return String(length=36)

    try:
        new = type.as_generic()

        if isinstance(new, String) and not new.length:
            # Length is required for VARCHAR
            # (turns out we use the same length as Airbyte)
            new.length = 512

        return new

    except NotImplementedError:
        traceback.print_exc()
        return type


def copy_table(
    table: Table,
    *,
    in_engine: Engine,
    out_engine: Engine,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> None:
    """Copy a table."""
    log = logger.bind(table_name=table.name)

    out_table = copy.copy(table)
    out_table.name = f"{TABLE_PREFIX}{table.name}"
    # Do not copy constraints
    out_table.constraints = set([])
    # Do not copy indexes
    out_table.indexes = set([])

    out_table.drop(out_engine, checkfirst=True)
    log.info("created_table")
    out_table.create(out_engine)

    with in_engine.connect() as conn_in:
        with out_engine.connect() as conn_out:
            stmt = select(table)
            # stream_results does not work for all db dialects
            for i, r in enumerate(
                conn_in.execution_options(stream_results=True).execute(stmt)
            ):
                # TODO: could use batched queries with bound params, see
                # sqlalchemy's doc

                ins = out_table.insert().values(**r)
                conn_out.execute(ins)

                if i and i % 1000 == 0:
                    log.info("inserted", n=i)


def copy_db(in_db_url: str, out_db_url: str) -> None:
    """Copy the db to its destination"""
    in_engine = create_engine(in_db_url, connect_args={"connect_timeout": 10})
    out_engine = create_engine(out_db_url, connect_args={"connect_timeout": 10})

    metadata = MetaData()

    @event.listens_for(metadata, "column_reflect")
    def genericize_datatypes(inspector, table, column_dict):
        previously = column_dict["type"]
        # No need for default value (such as nextval(...))
        del column_dict["default"]
        column_dict["type"] = get_generic_type(previously)
        logger.info(
            "reflected type",
            table=table.name,
            column=column_dict["name"],
            previous=previously,
            new=column_dict["type"],
        )

    metadata.reflect(bind=in_engine)

    for t in reversed(metadata.sorted_tables):
        copy_table(t, in_engine=in_engine, out_engine=out_engine)


def main(should_install_fixtures: bool = False) -> int:
    in_db_url = os.environ.get("DB_IN", DEFAULT_DB_IN)
    out_db_url = os.environ.get("DB_OUT", DEFAULT_DB_OUT)

    logger.info(f"copy db from={in_db_url} to={out_db_url}")

    if should_install_fixtures:
        setup_fixtures(in_db_url=in_db_url)

    copy_db(in_db_url=in_db_url, out_db_url=out_db_url)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="copy db to destination")
    parser.add_argument("--fixtures", action="store_true", help="install fixtures")
    args = parser.parse_args()

    sys.exit(main(should_install_fixtures=args.fixtures))

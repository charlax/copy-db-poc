#!/usr/bin/env python3
import argparse
import copy
import sys
import traceback
from uuid import uuid4
from typing import cast

import sqlalchemy
from sqlalchemy import create_engine, select, func, event
from sqlalchemy import Table, Column, Integer, String, MetaData

TABLE_PREFIX = "dbin_"
DEFAULT_BATCH_SIZE = 1000

in_engine = create_engine(
    "postgresql+psycopg2://user:password@127.0.0.1:5432/dbin", echo=True
)
out_engine = create_engine(
    "mysql+mysqldb://user:password@127.0.0.1:3306/dbout", echo=True
)


def setup_fixtures():
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
            # For MySQL
            new.length = 500

        return new

    except NotImplementedError:
        traceback.print_exc()
        return type


def copy_table(
    table: Table,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> None:
    """Copy a table."""
    out_table = copy.copy(table)
    out_table.name = f"{TABLE_PREFIX}{table.name}"
    # Do not copy constraints
    out_table.constraints = set([])

    out_table.drop(out_engine, checkfirst=True)
    out_table.create(out_engine)

    with in_engine.connect() as conn_in:
        with out_engine.connect() as conn_out:
            stmt = select(table)
            # stream_results does not work for all db dialects
            for r in conn_in.execution_options(stream_results=True).execute(stmt):
                # TODO: could use batched queries with bound params, see
                # sqlalchemy's doc

                ins = out_table.insert().values(**r)
                conn_out.execute(ins)


def copy_db():
    """Copy the db to its destination"""
    metadata = MetaData()

    @event.listens_for(metadata, "column_reflect")
    def genericize_datatypes(inspector, tablename, column_dict):
        previously = column_dict["type"]
        column_dict["type"] = get_generic_type(previously)

    metadata.reflect(bind=in_engine)

    for t in reversed(metadata.sorted_tables):
        copy_table(t)


def main() -> int:
    setup_fixtures()

    copy_db()
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="copy db over")
    args = parser.parse_args()

    sys.exit(main())

from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    inspect,
)


async def create_connection(db_path):
    engine = create_engine(f"sqlite:///{str(db_path)}", echo=True)
    conn = engine.connect()
    return engine, conn


async def initialize_table_if_not_exist(engine, conn):
    insp = inspect(engine)
    meta = MetaData()
    Table(
        "containers",
        meta,
        Column("container_id", String, primary_key=True),
        Column("service_ip", String),
        Column("service_port", Integer),
        Column("config", Text),
        Column("status", String),
        Column("timestamp", String),
    )
    if "containers" not in insp.get_table_names():
        meta.create_all(engine)


async def get_all_containers(engine, conn):
    insp = inspect(engine)
    meta = MetaData()
    containers = Table(
        "containers",
        meta,
        Column("container_id", String, primary_key=True),
        Column("service_ip", String),
        Column("service_port", Integer),
        Column("config", Text),
        Column("status", String),
        Column("timestamp", String),
    )

    if "containers" not in insp.get_table_names():
        meta.create_all(engine)
    rows = conn.execute(containers.select())
    return rows, containers


async def insert_new_container(
    conn,
    table,
    container_id,
    service_ip,
    service_port,
    config,
    status,
    timestamp,
):
    ins = table.insert().values(
        container_id=container_id,
        service_ip=service_ip,
        service_port=int(service_port),
        config=str(config),
        status=status,
        timestamp=timestamp,
    )
    conn.execute(ins)


async def delete_container_record(conn, container_id):

    meta = MetaData()
    table = Table(
        "containers",
        meta,
        Column("container_id", String, primary_key=True),
        Column("service_ip", String),
        Column("service_port", Integer),
        Column("config", Text),
        Column("status", String),
        Column("timestamp", String),
    )
    del_sql = table.delete().where(
        table.c.container_id == container_id,
    )
    conn.execute(del_sql)

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

meta = MetaData()
containers = Table(
    "containers",
    meta,
    Column("container_id", String, primary_key=True),
    Column("container_name", String),
    Column("service_ip", String),
    Column("service_port", Integer),
    Column("config", Text),
    Column("status", String),
    Column("timestamp", String),
)


async def create_connection(db_path):
    engine = create_engine(f"sqlite:///{str(db_path)}", echo=False)
    conn = engine.connect()
    return engine, conn


async def initialize_table_if_not_exist(engine, conn):
    insp = inspect(engine)
    if "containers" not in insp.get_table_names():
        meta.create_all(engine)


async def get_all_containers(engine, conn):
    insp = inspect(engine)
    if "containers" not in insp.get_table_names():
        meta.create_all(engine)
    rows = conn.execute(containers.select())
    return rows, containers


async def get_filebrowser_by_container_id(engine, conn, container_id):
    insp = inspect(engine)
    if "containers" not in insp.get_table_names():
        meta.create_all(engine)
    rows = conn.execute(
        containers.select().where(
            containers.c.container_id == container_id,
        ),
    )
    return rows, containers


async def insert_new_container(
    conn,
    container_id,
    container_name,
    service_ip,
    service_port,
    config,
    status,
    timestamp,
):
    ins = containers.insert().values(
        container_id=container_id,
        container_name=container_name,
        service_ip=service_ip,
        service_port=int(service_port),
        config=str(config),
        status=status,
        timestamp=timestamp,
    )
    conn.execute(ins)


async def delete_container_record(conn, container_id):
    del_sql = containers.delete().where(
        containers.c.container_id == container_id,
    )
    conn.execute(del_sql)
    conn.close()

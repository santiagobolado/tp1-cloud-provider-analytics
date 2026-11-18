"""Conexión a AstraDB / Cassandra y carga desde Spark con el driver de Python.

Por qué el driver y no el Spark Cassandra Connector (D-017): la consigna admite
"conector o foreachBatch/driver"; el conector es un JAR de la JVM atado a una
versión de Spark y Scala, y su compatibilidad con Spark 4.2 no está asegurada.
El driver no depende de Spark y se instala con pip.

La carga corre en foreachPartition: cada partición abre su propia sesión y
escribe con sentencias preparadas en paralelo. Todo INSERT en Cassandra es un
upsert por clave primaria, así que re-cargar no duplica.

Credenciales SOLO por variables de entorno (nunca en el repo):
    AstraDB:  ASTRA_DB_BUNDLE_PATH, ASTRA_DB_TOKEN   (ASTRA_DB_KEYSPACE opcional)
              ASTRA_DB_TOKEN_READONLY                 (consultas; mínimo privilegio)
    Local:    CASSANDRA_HOST                          (plan alternativo, G-02)

Mínimo privilegio (D-025): la carga crea tablas y escribe, así que usa el token
de administrador (ASTRA_DB_TOKEN). Las consultas solo leen y usan un token con
rol de solo lectura (ASTRA_DB_TOKEN_READONLY). Si ese token no está definido,
las consultas caen al de administrador y lo dejan registrado en la evidencia.
"""

import os
import re

DEFAULT_KEYSPACE = "cloud_analytics"
CQL_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "infra", "cql")


class ServingConfigError(RuntimeError):
    pass


def connection_params(read_only: bool = False) -> dict:
    """Parámetros serializables (viajan a los workers de Spark).

    read_only=True elige el token de solo lectura si existe (consultas, D-025).
    `token_role` queda en los parámetros para que la evidencia diga cuál se usó.
    """
    keyspace = os.environ.get("ASTRA_DB_KEYSPACE", DEFAULT_KEYSPACE)
    bundle, token = os.environ.get("ASTRA_DB_BUNDLE_PATH"), os.environ.get("ASTRA_DB_TOKEN")
    ro_token = os.environ.get("ASTRA_DB_TOKEN_READONLY")
    host = os.environ.get("CASSANDRA_HOST")
    if bundle and (token or (read_only and ro_token)):
        if not os.path.isfile(bundle):
            raise ServingConfigError(f"No existe el Secure Connect Bundle: {bundle}")
        use_ro = read_only and bool(ro_token)
        return {"mode": "astra", "bundle": os.path.abspath(bundle),
                "token": ro_token if use_ro else token,
                "token_role": "solo_lectura" if use_ro else "administrador",
                "keyspace": keyspace}
    if host:
        return {"mode": "local", "host": host, "keyspace": keyspace, "token_role": "sin_auth"}
    raise ServingConfigError(
        "Faltan credenciales. Definí ASTRA_DB_BUNDLE_PATH y ASTRA_DB_TOKEN "
        "(o CASSANDRA_HOST para Cassandra local). Ver config/.env.example.")


def connect(params: dict, keyspace: bool = True):
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster

    if params["mode"] == "astra":
        cluster = Cluster(cloud={"secure_connect_bundle": params["bundle"]},
                          auth_provider=PlainTextAuthProvider("token", params["token"]))
    else:
        cluster = Cluster([params["host"]])
    session = cluster.connect(params["keyspace"] if keyspace else None)
    session.default_timeout = 30
    return cluster, session


def cql_statements(filename: str) -> list:
    """Sentencias de un archivo .cql, sin comentarios."""
    with open(os.path.join(CQL_DIR, filename), encoding="utf-8") as fh:
        text = re.sub(r"--[^\n]*", "", fh.read())
    return [s.strip() for s in text.split(";") if s.strip()]


def apply_schema(params: dict) -> list:
    if params["mode"] == "local":
        cluster, session = connect(params, keyspace=False)
        for stmt in cql_statements("keyspace_local.cql"):
            session.execute(stmt)
        cluster.shutdown()
    cluster, session = connect(params)
    created = []
    for stmt in cql_statements("schema.cql"):
        session.execute(stmt)
        created.append(re.search(r"EXISTS\s+(\w+)", stmt).group(1))
    cluster.shutdown()
    return created


def _partition_writer(params: dict, table: str, cols: list):
    def write(rows):
        from cassandra.concurrent import execute_concurrent_with_args
        rows = [tuple(r[c] for c in cols) for r in rows]
        if not rows:
            return
        cluster, session = connect(params)
        try:
            stmt = session.prepare(f"INSERT INTO {table} ({', '.join(cols)}) "
                                   f"VALUES ({', '.join('?' * len(cols))})")
            results = execute_concurrent_with_args(session, stmt, rows, concurrency=64,
                                                   raise_on_first_error=True)
            failed = [r for ok, r in results if not ok]
            if failed:
                raise RuntimeError(f"{len(failed)} filas fallaron al escribir {table}")
        finally:
            cluster.shutdown()
    return write


def load(df, table: str, params: dict, partitions: int = 4) -> int:
    """Escribe df en la tabla (upsert). Devuelve las filas enviadas."""
    cols = df.columns
    df = df.repartition(partitions).cache()
    n = df.count()
    df.foreachPartition(_partition_writer(params, table, cols))
    df.unpersist()
    return n


def count_rows(params: dict, table: str) -> int:
    cluster, session = connect(params)
    try:
        return session.execute(f"SELECT COUNT(*) FROM {table}", timeout=60).one()[0]
    finally:
        cluster.shutdown()


def can_write(session) -> bool:
    """¿La sesión puede escribir? Prueba de mínimo privilegio para la evidencia.

    Borra una clave que no existe: si el token tuviera permisos, no cambia
    ningún dato. Con un token de solo lectura, AstraDB responde Unauthorized.
    """
    try:
        session.execute("DELETE FROM revenue_by_org_month WHERE org_id = '__readonly_check__'")
        return True
    except Exception as exc:                  # el driver no siempre tipa el error de permisos
        if "unauthorized" in type(exc).__name__.lower() or "permission" in str(exc).lower() \
                or "unauthorized" in str(exc).lower():
            return False
        raise

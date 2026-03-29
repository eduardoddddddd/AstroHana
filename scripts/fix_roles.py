import os

from dotenv import load_dotenv
from hdbcli import dbapi

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

HOST = os.getenv("HANA_HOST")
PORT = int(os.getenv("HANA_PORT", 443))
DBADMIN_USER = os.getenv("HANA_USER", "DBADMIN")
DBADMIN_PASS = os.getenv("HANA_PASS")
GRANTHELPER_USER = os.getenv("HANA_GRANTHELPER_USER", "GRANTHELPER")
GRANTHELPER_PASS = os.getenv("HANA_GRANTHELPER_PASS")


def require_env(name: str, value: str | None) -> str:
    if value:
        return value
    raise RuntimeError(f"Falta la variable de entorno requerida: {name}")


def main():
    host = require_env("HANA_HOST", HOST)
    dbadmin_pass = require_env("HANA_PASS", DBADMIN_PASS)
    granthelper_pass = require_env("HANA_GRANTHELPER_PASS", GRANTHELPER_PASS)

    gh = dbapi.connect(
        address=host,
        port=PORT,
        user=GRANTHELPER_USER,
        password=granthelper_pass,
        encrypt=True,
        sslValidateCertificate=False,
    )
    cur = gh.cursor()

    try:
        cur.execute("DROP PROCEDURE DBADMIN.DO_KMEANS")
        gh.commit()
    except Exception:
        pass

    db = dbapi.connect(
        address=host,
        port=PORT,
        user=DBADMIN_USER,
        password=dbadmin_pass,
        encrypt=True,
        sslValidateCertificate=False,
    )
    dbc = db.cursor()
    for table_name in ["PAL_KMEANS_RESULT", "PAL_KMEANS_CENTROIDS"]:
        try:
            dbc.execute(f"DROP TABLE DBADMIN.{table_name}")
            db.commit()
        except Exception:
            pass
    dbc.execute(
        """CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_RESULT (
        ID INTEGER, CLUSTER_ID INTEGER, DISTANCE DOUBLE, SLIGHT_SILHOUETTE DOUBLE)"""
    )
    dbc.execute(
        """CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_CENTROIDS (
        CLUSTER_ID INTEGER, ATTR_NAME NVARCHAR(256), VALUE DOUBLE)"""
    )
    db.commit()
    print("Tablas resultado recreadas con schema correcto")
    db.close()

    proc_sql = """CREATE PROCEDURE DBADMIN.DO_KMEANS()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER AS
BEGIN
    lt_data   = SELECT * FROM DBADMIN.PAL_KMEANS_INPUT;
    lt_params = SELECT NAME, INTVAL, DBLVAL, STRVAL FROM DBADMIN.PAL_KM_PARAMS;
    CALL _SYS_AFL.PAL_KMEANS(
        :lt_data, :lt_params,
        lt_result, lt_centers, lt_center_stats, lt_stats, lt_model
    );
    INSERT INTO DBADMIN.PAL_KMEANS_RESULT    SELECT * FROM :lt_result;
    INSERT INTO DBADMIN.PAL_KMEANS_CENTROIDS SELECT * FROM :lt_centers;
END"""

    try:
        cur.execute(proc_sql)
        gh.commit()
        print("Procedure DO_KMEANS creado OK")
    except Exception as exc:
        print(f"FAIL procedure: {exc}")
        gh.close()
        raise SystemExit(1)

    try:
        cur.execute(f"GRANT EXECUTE ON DBADMIN.DO_KMEANS TO {DBADMIN_USER}")
        gh.commit()
        print("EXECUTE grant a DBADMIN OK")
    except Exception as exc:
        print(f"FAIL grant: {exc}")

    gh.close()


if __name__ == "__main__":
    main()

from hdbcli import dbapi

HOST = "20178d0a-d4af-4825-bba6-11a2aa151d20.hna1.prod-us10.hanacloud.ondemand.com"
PORT = 443

gh = dbapi.connect(address=HOST, port=PORT, user="GRANTHELPER",
                   password="Temp4Grant!", encrypt=True, sslValidateCertificate=False)
cur = gh.cursor()

try: cur.execute("DROP PROCEDURE DBADMIN.DO_KMEANS"); gh.commit()
except: pass

# Tablas de resultado con todas las columnas que PAL devuelve
db = dbapi.connect(address=HOST, port=PORT, user="DBADMIN",
                   password="Edu01edu.", encrypt=True, sslValidateCertificate=False)
dbc = db.cursor()
for t in ["PAL_KMEANS_RESULT","PAL_KMEANS_CENTROIDS"]:
    try: dbc.execute(f"DROP TABLE DBADMIN.{t}"); db.commit()
    except: pass
dbc.execute("""CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_RESULT (
    ID INTEGER, CLUSTER_ID INTEGER, DISTANCE DOUBLE, SLIGHT_SILHOUETTE DOUBLE)""")
dbc.execute("""CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_CENTROIDS (
    CLUSTER_ID INTEGER, ATTR_NAME NVARCHAR(256), VALUE DOUBLE)""")
db.commit()
print("Tablas resultado recreadas con schema correcto")
db.close()

# Procedure con INSERT usando las 4 columnas reales de result
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
except Exception as e:
    print(f"FAIL procedure: {e}")
    gh.close()
    exit(1)

try:
    cur.execute("GRANT EXECUTE ON DBADMIN.DO_KMEANS TO DBADMIN")
    gh.commit()
    print("EXECUTE grant a DBADMIN OK")
except Exception as e:
    print(f"FAIL grant: {e}")

gh.close()

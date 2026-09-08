"""Isolated SQL transport. Host-owned templates only; credentials never leave here."""

import contextlib
import io
import json
import re
import struct
import sys
from importlib.metadata import version


def query(request):
    """Return one fixed SELECT and parameters, rejecting arbitrary SQL input."""
    if set(request) != {"server", "database", "operation", "schema_name", "table_name"}:
        raise ValueError("Invalid SQL read request")
    if not isinstance(request["server"], str) or not re.fullmatch(r"[a-zA-Z0-9-]+\.datawarehouse\.fabric\.microsoft\.com", request["server"]):
        raise ValueError("Invalid SQL server")
    if not isinstance(request["database"], str) or not re.fullmatch(r"[A-Za-z0-9_ -]{1,128}", request["database"]):
        raise ValueError("Invalid SQL database")
    for key in ("schema_name", "table_name"):
        if not isinstance(request[key], str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", request[key]):
            raise ValueError("Unsupported SQL identifier")
    table = f"[{request['schema_name']}].[{request['table_name']}]"
    if request["operation"] == "lakehouse_count":
        return f"SELECT COUNT_BIG(*) AS row_count FROM {table}", ()
    if request["operation"] == "lakehouse_preview":
        return f"SELECT TOP (101) * FROM {table}", ()
    if request["operation"] == "lakehouse_schema":
        return ("SELECT TOP (101) COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"), (request["schema_name"], request["table_name"])
    raise ValueError("Unsupported SQL operation")


def sql_token(*, interactive=False, browser=False):
    if browser and not interactive:
        raise ValueError("Browser authorization requires explicit interactive login")
    if version("ms-fabric-cli") != "1.7.0":
        raise ModuleNotFoundError("Pinned Fabric auth adapter unavailable")
    from fabric_cli.core.fab_auth import FabAuth
    from fabric_cli.core.fab_exceptions import FabricCLIError
    from fabric_cli.core import fab_constant
    from .fabric_auth import load_service_principal
    try:
        auth = load_service_principal(FabAuth())
        scope = ["https://database.windows.net/.default"]
        if browser:
            import msal
            if auth.get_identity_type() != "user":
                raise PermissionError("Browser sign-in requires an existing Fabric user profile")
            # Version-checked adapter: retain the enrolled tenant, client and encrypted
            # project cache. Only an explicit local login may launch browser UI.
            app = msal.PublicClientApplication(
                client_id=fab_constant.AUTH_DEFAULT_CLIENT_ID,
                authority=auth._get_authority_url(),
                token_cache=auth._get_app().token_cache,
                enable_broker_on_windows=False,
                enable_broker_on_mac=False,
            )
            result = app.acquire_token_interactive(scopes=scope, prompt="select_account", timeout=300)
            token = result.get("access_token") if isinstance(result, dict) else None
        else:
            token = auth.get_access_token(scope, interactive_renew=interactive)
    except FabricCLIError as exc:
        if exc.status_code == fab_constant.ERROR_AUTHENTICATION_FAILED:
            raise PermissionError("SQL sign-in required") from None
        raise
    if not isinstance(token, str) or not token:
        raise PermissionError("SQL sign-in required")
    return token


def execute(request, *, connect=None, token=None):
    sql, params = query(request)
    if connect is None:
        import pyodbc
        if version("pyodbc") != "5.3.0" or "ODBC Driver 18 for SQL Server" not in pyodbc.drivers():
            raise ModuleNotFoundError("SQL transport prerequisites unavailable")
        connect = pyodbc.connect
    if token is None:
        token = sql_token()
    if not isinstance(token, str) or not token:
        raise PermissionError("Sign-in required")
    raw = token.encode("utf-16-le")
    connection = connect(
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{request['server']},1433;Database={{{request['database']}}};"
        "Encrypt=yes;TrustServerCertificate=no;ApplicationIntent=ReadOnly;",
        attrs_before={1256: struct.pack("<I", len(raw)) + raw}, timeout=15, autocommit=True,
    )
    try:
        connection.timeout = 45
        cursor = connection.cursor()
        try:
            cursor.execute(sql, params) if params else cursor.execute(sql)
            columns = [col[0] for col in cursor.description]
            fetched = cursor.fetchmany(101)
            rows = [list(row) for row in fetched[:100]]
            data = {"columns": columns, "rows": rows, "truncated": len(fetched) > 100,
                    "coverage": "table_count" if request["operation"] == "lakehouse_count" else request["operation"],
                    "consistency": "SQL analytics endpoint at query time; Delta synchronization may lag"}
            # Round-trip non-JSON SQL values such as Decimal and datetime as strings.
            encoded = json.dumps(data, default=str, ensure_ascii=False)
            if len(encoded.encode("utf-8")) > 16000:
                raise OverflowError("SQL result exceeded the read limit")
            return json.loads(encoded)
        finally:
            cursor.close()
    finally:
        connection.close()


def main(*, login=False, browser=False):
    try:
        request = None if login else json.loads(sys.stdin.read(4097))
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            if login:
                # Only the explicit local login command enables interactive auth.
                # Never print, return or persist the token outside the auth cache.
                sql_token(interactive=True, browser=browser)
                data = {"sql_signin": "ready", "note": "SQL token acquired; database permissions and connectivity are not yet verified."}
            else:
                data = execute(request)
        print(json.dumps(data))
        return 0
    except ModuleNotFoundError:
        code = "FABRIC_SQL_SETUP"
    except PermissionError:
        code = "FABRIC_SQL_SIGNIN_REQUIRED"
    except OverflowError:
        code = "FABRIC_READ_TOO_LARGE"
    except Exception as exc:
        # Never expose ODBC messages, which may include connection or source data.
        code = "FABRIC_SQL_TABLE_UNAVAILABLE" if exc.args and exc.args[0] == "42S02" else "FABRIC_SQL_UNAVAILABLE"
    print(json.dumps({"error_code": code}))
    return 1


if __name__ == "__main__":
    if sys.argv[1:] not in ([], ["--login"], ["--login", "--browser"]):
        raise SystemExit("Unsupported SQL worker arguments")
    sys.exit(main(login="--login" in sys.argv[1:], browser="--browser" in sys.argv[1:]))

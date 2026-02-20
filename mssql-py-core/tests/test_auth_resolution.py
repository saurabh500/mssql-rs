# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for auth resolution via PyCoreConnection (Rust pipeline).

Test numbering matches the ODBC Auth Resolution Deep Dive document §3.
Each test exercises the full mssql-py-core pipeline:
  Python dict → validate_auth() → transform_auth() → ClientContext → TDS connect

Tests that can't complete a real connection (Entra ID, SSPI, AccessToken without
a live token) validate the pipeline up to the point of failure — confirming that
the error comes from the server or TDS layer, not from our validator/transformer.
"""

import os
import sys
import pytest
import mssql_py_core
from pathlib import Path
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Helpers — follows same pattern as test_client_context.py
# ---------------------------------------------------------------------------

def _load_env():
    env_path = Path(__file__).parent.parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)


def _get_db_credentials():
    _load_env()
    username = os.environ.get("DB_USERNAME", "sa")
    password = os.environ.get("SQL_PASSWORD")
    if not password:
        try:
            with open("/tmp/password", "r") as f:
                password = f.read().strip()
        except FileNotFoundError:
            pytest.skip("SQL_PASSWORD not set and /tmp/password not found")
    return username, password


def _server():
    return os.environ.get("SQL_SERVER", "localhost")


def _database():
    return os.environ.get("SQL_DATABASE", "master")


def _trust_cert():
    return os.environ.get("TRUST_SERVER_CERTIFICATE", "false").lower() == "true"


_skip_no_kerberos = pytest.mark.skipif(
    not os.environ.get("KERBEROS_TEST"),
    reason="Requires Kerberos environment — set KERBEROS_TEST=1 to run",
)


def _server_reachable():
    """Check if the SQL Server is reachable via TCP."""
    import socket
    _load_env()
    try:
        socket.create_connection((_server(), 1433), timeout=2)
        return True
    except OSError:
        return False


_skip_no_server = pytest.mark.skipif(
    not _server_reachable(),
    reason=f"SQL Server ({_server()}) not reachable",
)


def _base_ctx():
    """Context with server + UID + PWD for SQL auth happy paths."""
    username, password = _get_db_credentials()
    return {
        "server": _server(),
        "database": _database(),
        "user_name": username,
        "password": password,
        "trust_server_certificate": _trust_cert(),
        "encryption": os.environ.get("ENCRYPTION", "Optional"),
    }


def _bare_ctx():
    """Context with only server + TrustServerCertificate — no credentials."""
    _load_env()
    return {
        "server": _server(),
        "database": _database(),
        "trust_server_certificate": _trust_cert(),
        "encryption": os.environ.get("ENCRYPTION", "Optional"),
        "connect_timeout": 1,  # Short timeout for expected failures
    }


def _connect_ok(ctx):
    """Assert that the context connects and can SELECT 1."""
    conn = mssql_py_core.PyCoreConnection(ctx)
    cur = conn.cursor()
    cur.execute("SELECT 1 AS ok")
    row = cur.fetchone()
    conn.close()
    assert row[0] == 1


def _expect_validation_error(ctx, match_text):
    """Assert PyCoreConnection raises RuntimeError matching match_text."""
    with pytest.raises(RuntimeError, match=match_text):
        mssql_py_core.PyCoreConnection(ctx)


def _expect_no_validation_error(ctx):
    """Assert pipeline doesn't raise a validator/transformer error.

    Server-side auth failures are expected and acceptable.
    """
    try:
        conn = mssql_py_core.PyCoreConnection(ctx)
        conn.close()
    except RuntimeError as e:
        msg = str(e)
        assert "Access Token" not in msg
        assert "Integrated Security" not in msg
        assert "Unsupported Authentication" not in msg
        assert "Both User and Password" not in msg
        assert "User or Password" not in msg
        assert "Trusted_Connection" not in msg
        assert "Cannot set both" not in msg


# ═══════════════════════════════════════════════════════════════════════════
# §3.1 — Happy-Path Combinations
# ═══════════════════════════════════════════════════════════════════════════

class TestHappyPaths:
    """ODBC conflict matrix §3.1: rows #1–#17."""

    # #1 — UID + PWD, no auth keyword → SQL Server auth
    @_skip_no_server
    def test_01_uid_pwd_only(self):
        _connect_ok(_base_ctx())

    # #2 — nothing set → SQL Server auth with blank UID (will likely fail auth,
    #       but should NOT fail in validator/transformer)
    def test_02_nothing_set(self):
        ctx = _bare_ctx()
        # Expect server-side auth failure, not a client-side validation error
        try:
            _connect_ok(ctx)
        except RuntimeError as e:
            assert "Access Token" not in str(e)
            assert "Integrated Security" not in str(e)
            assert "Unsupported Authentication" not in str(e)

    # #3 — Trusted_Connection=Yes → Kerberos/SSPI
    @_skip_no_kerberos
    def test_03_tc_yes(self):
        ctx = _bare_ctx()
        ctx["trusted_connection"] = "Yes"
        _connect_ok(ctx)

    # #4 — Trusted_Connection=Yes + UID + PWD → Kerberos/SSPI, creds silently ignored
    @_skip_no_kerberos
    def test_04_tc_yes_uid_pwd_silently_ignored(self):
        ctx = _base_ctx()
        ctx["trusted_connection"] = "Yes"
        _connect_ok(ctx)

    # #5 — Trusted_Connection=No + UID + PWD → SQL Server auth
    @_skip_no_server
    def test_05_tc_no_uid_pwd(self):
        ctx = _base_ctx()
        ctx["trusted_connection"] = "No"
        _connect_ok(ctx)

    # #6 — Trusted_Connection=No, no creds → SQL auth with blank UID
    def test_06_tc_no_no_creds(self):
        ctx = _bare_ctx()
        ctx["trusted_connection"] = "No"
        try:
            _connect_ok(ctx)
        except RuntimeError as e:
            # Server-side failure is expected, not validator/transformer
            assert "Access Token" not in str(e)
            assert "Integrated Security" not in str(e)

    # #7 — SqlPassword + UID + PWD → SQL auth (validated)
    @_skip_no_server
    def test_07_sqlpassword_uid_pwd(self):
        ctx = _base_ctx()
        ctx["authentication"] = "SqlPassword"
        _connect_ok(ctx)

    # #8 — ADPassword + UID + PWD → AAD Password
    #     (will fail against local SQL Server — that's fine, validates pipeline)
    def test_08_ad_password_uid_pwd(self):
        ctx = _base_ctx()
        ctx["authentication"] = "ActiveDirectoryPassword"
        try:
            _connect_ok(ctx)
        except RuntimeError as e:
            assert "Unsupported Authentication" not in str(e)
            assert "Both User and Password" not in str(e)

    # #9 — ADIntegrated, no creds
    def test_09_ad_integrated_alone(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryIntegrated"
        try:
            _connect_ok(ctx)
        except RuntimeError as e:
            assert "Unsupported Authentication" not in str(e)
            assert "User or Password" not in str(e)

    # #10 — ADInteractive + UID (as hint)
    def test_10_ad_interactive_with_hint(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryInteractive"
        ctx["user_name"] = "user@domain.com"
        try:
            _connect_ok(ctx)
        except RuntimeError as e:
            assert "Unsupported Authentication" not in str(e)

    # #11 — ADInteractive, no hint
    def test_11_ad_interactive_no_hint(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryInteractive"
        try:
            _connect_ok(ctx)
        except RuntimeError as e:
            assert "Unsupported Authentication" not in str(e)

    # #12 — ADMSI, system-assigned
    def test_12_admsi_system_assigned(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryMSI"
        try:
            _connect_ok(ctx)
        except RuntimeError as e:
            assert "Unsupported Authentication" not in str(e)

    # #13 — ADMSI + UID (client_id for user-assigned)
    def test_13_admsi_user_assigned(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryMSI"
        ctx["user_name"] = "some-client-id"
        try:
            _connect_ok(ctx)
        except RuntimeError as e:
            assert "Unsupported Authentication" not in str(e)

    # #14 — ADServicePrincipal + UID + PWD
    def test_14_adspa_uid_pwd(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryServicePrincipal"
        ctx["user_name"] = "client-id"
        ctx["password"] = "client-secret"
        try:
            _connect_ok(ctx)
        except RuntimeError as e:
            assert "Unsupported Authentication" not in str(e)
            assert "Both User and Password" not in str(e)

    # #15 — TC=No + ADPassword + UID + PWD (TC=No is default, no conflict)
    def test_15_tc_no_ad_password(self):
        ctx = _base_ctx()
        ctx["trusted_connection"] = "No"
        ctx["authentication"] = "ActiveDirectoryPassword"
        try:
            _connect_ok(ctx)
        except RuntimeError as e:
            assert "Integrated Security" not in str(e)

    # #16 — TC=No + SqlPassword + UID + PWD
    @_skip_no_server
    def test_16_tc_no_sqlpassword(self):
        ctx = _base_ctx()
        ctx["trusted_connection"] = "No"
        ctx["authentication"] = "SqlPassword"
        _connect_ok(ctx)

    # #17 — Access Token alone
    @pytest.mark.skipif(
        not os.environ.get("ACCESS_TOKEN"),
        reason="ACCESS_TOKEN env var not set",
    )
    def test_17_access_token_alone(self):
        ctx = _bare_ctx()
        ctx["access_token"] = os.environ["ACCESS_TOKEN"]
        _connect_ok(ctx)


# ═══════════════════════════════════════════════════════════════════════════
# §3.2 — Silent Clears (no error, credentials quietly dropped)
# ═══════════════════════════════════════════════════════════════════════════

class TestSilentClears:
    """ODBC conflict matrix §3.2: rows #18–#23.

    These verify that the transformer silently clears credentials that the
    resolved auth method does not use. Since we can't introspect ClientContext
    from Python, we verify no validation error is raised — the transformer
    must have cleared the conflicting fields before the validator would reject
    them (for ADIntegrated). For ADInteractive/ADMSI, we check the pipeline
    passes without error.

    ADIntegrated + UID/PWD is silently cleared by the transformer (matching
    ODBC dialog path behavior). Tests #21-#23 verify no validation error.
    """

    # #18 — ADInteractive + UID + PWD → PWD silently cleared, UID kept as hint
    def test_18_ad_interactive_clears_pwd(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryInteractive"
        ctx["user_name"] = "user@domain.com"
        ctx["password"] = "should-be-cleared"
        # Validator allows this (no rule against ADInteractive + PWD)
        # Transformer clears password; connection attempt reaches server
        try:
            _connect_ok(ctx)
        except RuntimeError as e:
            assert "Both User and Password" not in str(e)
            assert "Unsupported Authentication" not in str(e)

    # #19 — ADMSI + UID + PWD → PWD silently cleared, UID kept as client_id
    def test_19_admsi_user_assigned_clears_pwd(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryMSI"
        ctx["user_name"] = "client-id-123"
        ctx["password"] = "should-be-cleared"
        try:
            _connect_ok(ctx)
        except RuntimeError as e:
            assert "Both User and Password" not in str(e)

    # #20 — ADMSI + PWD only → PWD silently cleared
    def test_20_admsi_system_clears_pwd(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryMSI"
        ctx["password"] = "should-be-cleared"
        try:
            _connect_ok(ctx)
        except RuntimeError as e:
            assert "Both User and Password" not in str(e)

    # #21 — ADIntegrated + UID → UID silently cleared (ODBC dialog behavior)
    def test_21_ad_integrated_clears_uid(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryIntegrated"
        ctx["user_name"] = "user@domain.com"
        try:
            _connect_ok(ctx)
        except RuntimeError as e:
            assert "ActiveDirectoryIntegrated" not in str(e)
            assert "User or Password" not in str(e)

    # #22 — ADIntegrated + PWD → PWD silently cleared
    def test_22_ad_integrated_clears_pwd(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryIntegrated"
        ctx["password"] = "secret"
        try:
            _connect_ok(ctx)
        except RuntimeError as e:
            assert "ActiveDirectoryIntegrated" not in str(e)
            assert "User or Password" not in str(e)

    # #23 — ADIntegrated + UID + PWD → both silently cleared
    def test_23_ad_integrated_clears_both(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryIntegrated"
        ctx["user_name"] = "user@domain.com"
        ctx["password"] = "secret"
        try:
            _connect_ok(ctx)
        except RuntimeError as e:
            assert "ActiveDirectoryIntegrated" not in str(e)
            assert "User or Password" not in str(e)


# ═══════════════════════════════════════════════════════════════════════════
# §3.3 — Authentication + Trusted_Connection Clashes
# ═══════════════════════════════════════════════════════════════════════════

class TestAuthTcClashes:
    """ODBC conflict matrix §3.3: rows #24–#29."""

    # #24 — SqlPassword + TC=Yes
    def test_24_sqlpassword_tc(self):
        ctx = _base_ctx()
        ctx["authentication"] = "SqlPassword"
        ctx["trusted_connection"] = "Yes"
        _expect_validation_error(ctx, "Trusted_Connection")

    # #25 — ADPassword + TC=Yes
    def test_25_ad_password_tc(self):
        ctx = _base_ctx()
        ctx["authentication"] = "ActiveDirectoryPassword"
        ctx["trusted_connection"] = "Yes"
        _expect_validation_error(ctx, "Trusted_Connection")

    # #26 — ADIntegrated + TC=Yes
    def test_26_ad_integrated_tc(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryIntegrated"
        ctx["trusted_connection"] = "Yes"
        _expect_validation_error(ctx, "Trusted_Connection")

    # #27 — ADInteractive + TC=Yes
    def test_27_ad_interactive_tc(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryInteractive"
        ctx["user_name"] = "hint@domain.com"
        ctx["trusted_connection"] = "Yes"
        _expect_validation_error(ctx, "Trusted_Connection")

    # #28 — ADMSI + TC=Yes
    def test_28_admsi_tc(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryMSI"
        ctx["trusted_connection"] = "Yes"
        _expect_validation_error(ctx, "Trusted_Connection")

    # #29 — ADSPA + TC=Yes
    def test_29_adspa_tc(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryServicePrincipal"
        ctx["user_name"] = "cid"
        ctx["password"] = "sec"
        ctx["trusted_connection"] = "Yes"
        _expect_validation_error(ctx, "Trusted_Connection")


# ═══════════════════════════════════════════════════════════════════════════
# §3.4 — Partial / Forbidden Credentials
# ═══════════════════════════════════════════════════════════════════════════

class TestPartialCredentials:
    """ODBC conflict matrix §3.4: rows #30–#38."""

    # ── SqlPassword ──

    # #30 — SqlPassword, no UID, no PWD
    def test_30_sqlpassword_no_creds(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "SqlPassword"
        _expect_validation_error(ctx, "Both User and Password")

    # #31 — SqlPassword + UID only
    def test_31_sqlpassword_uid_only(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "SqlPassword"
        ctx["user_name"] = "sa"
        _expect_validation_error(ctx, "Both User and Password")

    # #32 — SqlPassword + PWD only
    def test_32_sqlpassword_pwd_only(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "SqlPassword"
        ctx["password"] = "secret"
        _expect_validation_error(ctx, "Both User and Password")

    # ── ADPassword ──

    # #33 — ADPassword, no UID, no PWD
    def test_33_ad_password_no_creds(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryPassword"
        _expect_validation_error(ctx, "Both User and Password")

    # #34 — ADPassword + UID only
    def test_34_ad_password_uid_only(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryPassword"
        ctx["user_name"] = "user@domain.com"
        _expect_validation_error(ctx, "Both User and Password")

    # #35 — ADPassword + PWD only
    def test_35_ad_password_pwd_only(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryPassword"
        ctx["password"] = "secret"
        _expect_validation_error(ctx, "Both User and Password")

    # ── ADServicePrincipal ──

    # #36 — ADSPA, no UID, no PWD
    def test_36_adspa_no_creds(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryServicePrincipal"
        _expect_validation_error(ctx, "Both User and Password")

    # #37 — ADSPA + UID only
    def test_37_adspa_uid_only(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryServicePrincipal"
        ctx["user_name"] = "client-id"
        _expect_validation_error(ctx, "Both User and Password")

    # #38 — ADSPA + PWD only
    def test_38_adspa_pwd_only(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryServicePrincipal"
        ctx["password"] = "client-secret"
        _expect_validation_error(ctx, "Both User and Password")


# ═══════════════════════════════════════════════════════════════════════════
# §3.5 — Access Token Clashes
# ═══════════════════════════════════════════════════════════════════════════

class TestAccessTokenClashes:
    """ODBC conflict matrix §3.5: rows #39–#43."""

    # #39 — Access Token + TC=Yes
    def test_39_token_tc(self):
        ctx = _bare_ctx()
        ctx["access_token"] = "fake-jwt"
        ctx["trusted_connection"] = "Yes"
        _expect_validation_error(ctx, "Access Token")

    # #40 — Access Token + Authentication keyword
    def test_40_token_auth_keyword(self):
        ctx = _bare_ctx()
        ctx["access_token"] = "fake-jwt"
        ctx["authentication"] = "SqlPassword"
        _expect_validation_error(ctx, "Access Token")

    # #41 — Access Token + UID
    def test_41_token_uid(self):
        ctx = _bare_ctx()
        ctx["access_token"] = "fake-jwt"
        ctx["user_name"] = "sa"
        _expect_validation_error(ctx, "Access Token")

    # #42 — Access Token + PWD
    def test_42_token_pwd(self):
        ctx = _bare_ctx()
        ctx["access_token"] = "fake-jwt"
        ctx["password"] = "secret"
        _expect_validation_error(ctx, "Access Token")

    # #43 — Access Token + TC + Auth + UID + PWD (first error is Access Token)
    def test_43_token_everything(self):
        ctx = _base_ctx()
        ctx["access_token"] = "fake-jwt"
        ctx["trusted_connection"] = "Yes"
        ctx["authentication"] = "SqlPassword"
        _expect_validation_error(ctx, "Access Token")


# ═══════════════════════════════════════════════════════════════════════════
# §2.5 — SqlPassword → Password Collapse
# ═══════════════════════════════════════════════════════════════════════════

class TestSqlPasswordCollapse:
    """§2.5: SqlPassword is identical to bare UID/PWD on the wire."""

    @_skip_no_server
    def test_sqlpassword_same_as_bare_uid_pwd(self):
        """Both should connect successfully with identical behavior."""
        ctx_bare = _base_ctx()
        _connect_ok(ctx_bare)

        ctx_sqlpwd = _base_ctx()
        ctx_sqlpwd["authentication"] = "SqlPassword"
        _connect_ok(ctx_sqlpwd)

    def test_sqlpassword_enforces_both_creds(self):
        """SqlPassword rejects missing PWD; bare UID/PWD does not."""
        # With SqlPassword — rejected
        ctx = _bare_ctx()
        ctx["authentication"] = "SqlPassword"
        ctx["user_name"] = "sa"
        _expect_validation_error(ctx, "Both User and Password")

        # Without SqlPassword — accepted (server decides)
        ctx2 = _bare_ctx()
        ctx2["user_name"] = "sa"
        try:
            _connect_ok(ctx2)
        except RuntimeError as e:
            # Server-side failure is fine — no client-side validation error
            assert "Both User and Password" not in str(e)


# ═══════════════════════════════════════════════════════════════════════════
# §2.6 — Default Behavior (no auth keyword, no TC)
# ═══════════════════════════════════════════════════════════════════════════

class TestDefaultBehavior:
    """§2.6: No auto-promote to SSPI when nothing is specified."""

    @_skip_no_server
    def test_no_auth_keyword_defaults_to_password(self):
        """No authentication keyword → Password auth with uid/pwd."""
        _connect_ok(_base_ctx())

    def test_blank_uid_does_not_promote_to_sspi(self):
        """Blank UID should NOT auto-promote to integrated auth."""
        ctx = _bare_ctx()
        try:
            _connect_ok(ctx)
        except RuntimeError as e:
            # Must fail with server-side auth error, not SSPI
            assert "SSPI" not in str(e)
            assert "Integrated Security" not in str(e)

    @_skip_no_server
    def test_empty_auth_keyword_falls_through(self):
        """authentication='' is an intentional reset → default Password."""
        ctx = _base_ctx()
        ctx["authentication"] = ""
        _connect_ok(ctx)


# ═══════════════════════════════════════════════════════════════════════════
# §5 — Edge Cases & Gotchas
# ═══════════════════════════════════════════════════════════════════════════

class TestEdgeCases:
    """§5: Footguns and subtle behaviors documented in the ODBC deep dive."""

    # §5.1 — TC=Yes + UID + PWD → creds silently ignored
    @_skip_no_kerberos
    def test_tc_yes_uid_pwd_silently_ignored(self):
        ctx = _base_ctx()
        ctx["trusted_connection"] = "Yes"
        # UID and PWD should be silently cleared by transformer
        _connect_ok(ctx)

    # §5.2 — SqlPassword + UID only → ERROR (unlike bare UID which is accepted)
    def test_sqlpassword_uid_only_rejected(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "SqlPassword"
        ctx["user_name"] = "sa"
        _expect_validation_error(ctx, "Both User and Password")

    # §5.3 — Empty auth string clears any previously set auth
    @_skip_no_server
    def test_empty_auth_string_resets(self):
        ctx = _base_ctx()
        ctx["authentication"] = ""
        _connect_ok(ctx)

    # §5.5 — SqlPassword collapses to Password on the wire
    @_skip_no_server
    def test_sqlpassword_wire_identical_to_password(self):
        ctx = _base_ctx()
        ctx["authentication"] = "SqlPassword"
        _connect_ok(ctx)

    # Case insensitivity for all auth keywords
    @_skip_no_server
    def test_case_insensitive_sqlpassword(self):
        ctx = _base_ctx()
        ctx["authentication"] = "SQLPASSWORD"
        _connect_ok(ctx)

    def test_case_insensitive_ad_password(self):
        ctx = _base_ctx()
        ctx["authentication"] = "activedirectorypassword"
        try:
            _connect_ok(ctx)
        except RuntimeError as e:
            assert "Unsupported Authentication" not in str(e)

    # Bogus auth keyword
    def test_bogus_auth_keyword_rejected(self):
        ctx = _base_ctx()
        ctx["authentication"] = "NotARealAuthMethod"
        _expect_validation_error(ctx, "Unsupported Authentication value")

    # ActiveDirectoryDefault
    def test_ad_default_alone(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryDefault"
        try:
            _connect_ok(ctx)
        except RuntimeError as e:
            assert "Unsupported Authentication" not in str(e)

    # ActiveDirectoryDeviceCodeFlow
    def test_ad_device_code_flow_alone(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryDeviceCodeFlow"
        try:
            _connect_ok(ctx)
        except RuntimeError as e:
            assert "Unsupported Authentication" not in str(e)

    # ActiveDirectoryWorkloadIdentity
    def test_ad_workload_identity_alone(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryWorkloadIdentity"
        try:
            _connect_ok(ctx)
        except RuntimeError as e:
            assert "Unsupported Authentication" not in str(e)

    # ActiveDirectoryManagedIdentity is NOT an ODBC keyword — only ActiveDirectoryMSI is
    def test_managed_identity_rejected(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryManagedIdentity"
        _expect_validation_error(ctx, "Unsupported Authentication value")


# ═══════════════════════════════════════════════════════════════════════════
# Additional auth keyword combinations (§3.3 extended)
# ═══════════════════════════════════════════════════════════════════════════

class TestAuthTcClashesExtended:
    """Additional Auth + TC combinations not in the core matrix."""

    def test_ad_default_tc(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryDefault"
        ctx["trusted_connection"] = "Yes"
        _expect_validation_error(ctx, "Trusted_Connection")

    def test_ad_device_code_tc(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryDeviceCodeFlow"
        ctx["trusted_connection"] = "Yes"
        _expect_validation_error(ctx, "Trusted_Connection")

    def test_ad_workload_identity_tc(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryWorkloadIdentity"
        ctx["trusted_connection"] = "Yes"
        _expect_validation_error(ctx, "Trusted_Connection")

    def test_ad_managed_identity_tc(self):
        ctx = _bare_ctx()
        ctx["authentication"] = "ActiveDirectoryManagedIdentity"
        ctx["trusted_connection"] = "Yes"
        # TC clash fires first (auth is non-empty), but keyword is also invalid
        _expect_validation_error(ctx, "Trusted_Connection")


# ═══════════════════════════════════════════════════════════════════════════
# Access Token with AD keyword variants (§3.5 extended)
# ═══════════════════════════════════════════════════════════════════════════

class TestAccessTokenClashesExtended:
    """Access Token conflicts with each AD auth keyword."""

    def test_token_ad_password(self):
        ctx = _bare_ctx()
        ctx["access_token"] = "fake-jwt"
        ctx["authentication"] = "ActiveDirectoryPassword"
        _expect_validation_error(ctx, "Access Token")

    def test_token_ad_integrated(self):
        ctx = _bare_ctx()
        ctx["access_token"] = "fake-jwt"
        ctx["authentication"] = "ActiveDirectoryIntegrated"
        _expect_validation_error(ctx, "Access Token")

    def test_token_ad_interactive(self):
        ctx = _bare_ctx()
        ctx["access_token"] = "fake-jwt"
        ctx["authentication"] = "ActiveDirectoryInteractive"
        _expect_validation_error(ctx, "Access Token")

    def test_token_admsi(self):
        ctx = _bare_ctx()
        ctx["access_token"] = "fake-jwt"
        ctx["authentication"] = "ActiveDirectoryMSI"
        _expect_validation_error(ctx, "Access Token")

    def test_token_adspa(self):
        ctx = _bare_ctx()
        ctx["access_token"] = "fake-jwt"
        ctx["authentication"] = "ActiveDirectoryServicePrincipal"
        _expect_validation_error(ctx, "Access Token")

    def test_token_uid_pwd_combined(self):
        ctx = _base_ctx()
        ctx["access_token"] = "fake-jwt"
        _expect_validation_error(ctx, "Access Token")

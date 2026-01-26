# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for bulk copy options.

Tests the following kwargs options for cursor.bulkcopy():

Options that use INSERT BULK WITH hints:
- fire_triggers: Fire INSERT triggers on destination table (WITH FIRE_TRIGGERS)
- check_constraints: Enforce CHECK constraints during bulk copy (WITH CHECK_CONSTRAINTS)
- keep_nulls: Preserve NULL values instead of using column defaults (WITH KEEP_NULLS)
- table_lock: Acquire table-level lock during bulk copy (WITH TABLOCK)

Options that use TDS protocol mechanisms (NOT WITH hints):
- keep_identity: Use explicit IDENTITY values from source data.
                 Unlike BULK INSERT's KEEPIDENTITY hint, INSERT BULK controls
                 identity preservation by including/excluding the identity column
                 in the column list and TDS metadata flags.
"""
import time
import pytest
import mssql_py_core


def unique_table_name(prefix: str) -> str:
    """Generate a unique table name with timestamp suffix."""
    return f"{prefix}_{int(time.time() * 1000)}"


# =============================================================================
# FIRE_TRIGGERS TESTS
# =============================================================================


@pytest.mark.integration
def test_bulkcopy_fire_triggers_true(client_context):
    """Test that fire_triggers=True causes INSERT triggers to fire.

    Creates a table with an AFTER INSERT trigger that writes to a marker table.
    Verifies trigger execution by checking the marker table after bulk copy.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Generate unique table names
    main_table = unique_table_name("BulkCopyFireTriggersMain")
    marker_table = unique_table_name("BulkCopyFireTriggersMarker")
    trigger_name = unique_table_name("trg_FireTriggersTest")

    try:
        # Create marker table to track trigger execution
        cursor.execute(
            f"IF OBJECT_ID('{marker_table}', 'U') IS NOT NULL DROP TABLE {marker_table}"
        )
        cursor.execute(
            f"CREATE TABLE {marker_table} (id INT IDENTITY(1,1), triggered_at DATETIME DEFAULT GETDATE())"
        )

        # Create main table
        cursor.execute(
            f"IF OBJECT_ID('{main_table}', 'U') IS NOT NULL DROP TABLE {main_table}"
        )
        cursor.execute(f"CREATE TABLE {main_table} (id INT, value NVARCHAR(50))")

        # Create AFTER INSERT trigger
        cursor.execute(f"""
            CREATE TRIGGER {trigger_name} ON {main_table}
            AFTER INSERT
            AS
            BEGIN
                INSERT INTO {marker_table} (triggered_at) VALUES (GETDATE())
            END
        """)

        # Prepare test data
        data = [
            (1, "Row 1"),
            (2, "Row 2"),
            (3, "Row 3"),
        ]

        # Execute bulk copy with fire_triggers=True
        result = cursor.bulkcopy(
            main_table,
            iter(data),
            kwargs={
                "fire_triggers": True,
                "batch_size": 1000,
            },
        )

        # Verify bulk copy succeeded
        assert result is not None
        assert result["rows_copied"] == 3

        # Verify trigger fired by checking marker table
        # With batch_size=1000 and 3 rows, trigger fires once per batch
        cursor.execute(f"SELECT COUNT(*) FROM {marker_table}")
        rows = cursor.fetchall()
        assert rows[0][0] == 1, "Trigger should have fired exactly once for the batch"

    finally:
        # Cleanup
        cursor.execute(f"IF OBJECT_ID('{trigger_name}', 'TR') IS NOT NULL DROP TRIGGER {trigger_name}")
        cursor.execute(f"IF OBJECT_ID('{main_table}', 'U') IS NOT NULL DROP TABLE {main_table}")
        cursor.execute(f"IF OBJECT_ID('{marker_table}', 'U') IS NOT NULL DROP TABLE {marker_table}")
        conn.close()


@pytest.mark.integration
def test_bulkcopy_fire_triggers_default_skips_triggers(client_context):
    """Test that not specifying fire_triggers defaults to False (triggers skipped).

    Creates a table with an AFTER INSERT trigger that writes to a marker table.
    Verifies trigger is NOT executed when fire_triggers is not specified.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Generate unique table names
    main_table = unique_table_name("BulkCopyDefaultTriggersMain")
    marker_table = unique_table_name("BulkCopyDefaultTriggersMarker")
    trigger_name = unique_table_name("trg_DefaultTriggersTest")

    try:
        # Create marker table
        cursor.execute(
            f"IF OBJECT_ID('{marker_table}', 'U') IS NOT NULL DROP TABLE {marker_table}"
        )
        cursor.execute(
            f"CREATE TABLE {marker_table} (id INT IDENTITY(1,1), triggered_at DATETIME DEFAULT GETDATE())"
        )

        # Create main table
        cursor.execute(
            f"IF OBJECT_ID('{main_table}', 'U') IS NOT NULL DROP TABLE {main_table}"
        )
        cursor.execute(f"CREATE TABLE {main_table} (id INT, value NVARCHAR(50))")

        # Create AFTER INSERT trigger
        cursor.execute(f"""
            CREATE TRIGGER {trigger_name} ON {main_table}
            AFTER INSERT
            AS
            BEGIN
                INSERT INTO {marker_table} (triggered_at) VALUES (GETDATE())
            END
        """)

        # Prepare test data
        data = [
            (1, "Row 1"),
            (2, "Row 2"),
        ]

        # Execute bulk copy WITHOUT specifying fire_triggers - should default to False
        result = cursor.bulkcopy(
            main_table,
            iter(data),
            kwargs={
                "batch_size": 1000,
            },
        )

        # Verify bulk copy succeeded
        assert result is not None
        assert result["rows_copied"] == 2

        # Verify trigger did NOT fire (default behavior)
        cursor.execute(f"SELECT COUNT(*) FROM {marker_table}")
        rows = cursor.fetchall()
        assert rows[0][0] == 0, "Trigger should NOT have fired when fire_triggers is not specified (default=False)"

    finally:
        # Cleanup
        cursor.execute(f"IF OBJECT_ID('{trigger_name}', 'TR') IS NOT NULL DROP TRIGGER {trigger_name}")
        cursor.execute(f"IF OBJECT_ID('{main_table}', 'U') IS NOT NULL DROP TABLE {main_table}")
        cursor.execute(f"IF OBJECT_ID('{marker_table}', 'U') IS NOT NULL DROP TABLE {marker_table}")
        conn.close()


@pytest.mark.integration
def test_bulkcopy_fire_triggers_false_skips_triggers(client_context):
    """Test that fire_triggers=False explicitly skips INSERT triggers.

    Creates a table with an AFTER INSERT trigger that writes to a marker table.
    Verifies trigger is NOT executed when fire_triggers is False.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Generate unique table names
    main_table = unique_table_name("BulkCopyNoTriggersMain")
    marker_table = unique_table_name("BulkCopyNoTriggersMarker")
    trigger_name = unique_table_name("trg_NoTriggersTest")

    try:
        # Create marker table
        cursor.execute(
            f"IF OBJECT_ID('{marker_table}', 'U') IS NOT NULL DROP TABLE {marker_table}"
        )
        cursor.execute(
            f"CREATE TABLE {marker_table} (id INT IDENTITY(1,1), triggered_at DATETIME DEFAULT GETDATE())"
        )

        # Create main table
        cursor.execute(
            f"IF OBJECT_ID('{main_table}', 'U') IS NOT NULL DROP TABLE {main_table}"
        )
        cursor.execute(f"CREATE TABLE {main_table} (id INT, value NVARCHAR(50))")

        # Create AFTER INSERT trigger
        cursor.execute(f"""
            CREATE TRIGGER {trigger_name} ON {main_table}
            AFTER INSERT
            AS
            BEGIN
                INSERT INTO {marker_table} (triggered_at) VALUES (GETDATE())
            END
        """)

        # Prepare test data
        data = [
            (1, "Row 1"),
            (2, "Row 2"),
        ]

        # Execute bulk copy with fire_triggers=False (explicit)
        result = cursor.bulkcopy(
            main_table,
            iter(data),
            kwargs={
                "fire_triggers": False,
                "batch_size": 1000,
            },
        )

        # Verify bulk copy succeeded
        assert result is not None
        assert result["rows_copied"] == 2

        # Verify trigger did NOT fire
        cursor.execute(f"SELECT COUNT(*) FROM {marker_table}")
        rows = cursor.fetchall()
        assert rows[0][0] == 0, "Trigger should NOT have fired when fire_triggers=False"

    finally:
        # Cleanup
        cursor.execute(f"IF OBJECT_ID('{trigger_name}', 'TR') IS NOT NULL DROP TRIGGER {trigger_name}")
        cursor.execute(f"IF OBJECT_ID('{main_table}', 'U') IS NOT NULL DROP TABLE {main_table}")
        cursor.execute(f"IF OBJECT_ID('{marker_table}', 'U') IS NOT NULL DROP TABLE {marker_table}")
        conn.close()


# =============================================================================
# CHECK_CONSTRAINTS TESTS
# =============================================================================


@pytest.mark.integration
def test_bulkcopy_check_constraints_true_enforces_constraints(client_context):
    """Test that check_constraints=True enforces CHECK constraints.

    Creates a table with a CHECK constraint and attempts to insert
    violating data. Should raise an error when check_constraints=True.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = unique_table_name("BulkCopyCheckConstraints")

    try:
        # Create table with CHECK constraint (value must be > 0)
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT,
                value INT CHECK (value > 0)
            )
        """)

        # Prepare data with constraint violation (negative value)
        data = [
            (1, 100),
            (2, -50),  # Violates CHECK constraint
            (3, 200),
        ]

        # Execute bulk copy with check_constraints=True - should fail
        with pytest.raises(Exception) as exc_info:
            cursor.bulkcopy(
                table_name,
                iter(data),
                kwargs={
                    "check_constraints": True,
                    "batch_size": 1000,
                },
            )

        # Verify we got a constraint violation error
        # SQL Server error 547: CHECK constraint violation
        error_msg = str(exc_info.value).lower()
        assert "conflicted with the check constraint" in error_msg, \
            f"Expected constraint violation error, got: {exc_info.value}"

    finally:
        # Cleanup
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
        conn.close()


@pytest.mark.integration
def test_bulkcopy_check_constraints_false_allows_violations(client_context):
    """Test that check_constraints=False (default) allows constraint violations.

    Creates a table with a CHECK constraint and inserts violating data.
    Should succeed when check_constraints=False.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = unique_table_name("BulkCopyNoCheckConstraints")

    try:
        # Create table with CHECK constraint (value must be > 0)
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT,
                value INT CHECK (value > 0)
            )
        """)

        # Prepare data with constraint violation
        data = [
            (1, 100),
            (2, -50),  # Violates CHECK constraint
            (3, 200),
        ]

        # Execute bulk copy with check_constraints=False - should succeed
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            kwargs={
                "check_constraints": False,
                "batch_size": 1000,
            },
        )

        # Verify bulk copy succeeded
        assert result is not None
        assert result["rows_copied"] == 3

        # Verify all data was inserted (including constraint-violating row)
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        rows = cursor.fetchall()
        assert rows[0][0] == 3

        # Verify the negative value was actually inserted
        cursor.execute(f"SELECT value FROM {table_name} WHERE id = 2")
        rows = cursor.fetchall()
        assert rows[0][0] == -50

    finally:
        # Cleanup
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
        conn.close()


# =============================================================================
# KEEP_NULLS TESTS
# =============================================================================


@pytest.mark.integration
def test_bulkcopy_keep_nulls_true_preserves_nulls(client_context):
    """Test that keep_nulls=True preserves NULL values over column defaults.

    Creates a table with a DEFAULT constraint and inserts NULL values.
    With keep_nulls=True, NULLs should remain NULL, not be replaced by defaults.

    Note: In SQL Server, KEEP_NULLS affects behavior when a NULL value is
    explicitly provided in the bulk data. When True, the NULL is preserved.
    When False, the default value is used instead of the NULL.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = unique_table_name("BulkCopyKeepNulls")

    try:
        # Create table with DEFAULT constraint
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT,
                value INT DEFAULT 999
            )
        """)

        # Prepare data with NULL values
        data = [
            (1, 100),
            (2, None),  # NULL value - should remain NULL with keep_nulls=True
            (3, 200),
        ]

        # Execute bulk copy with keep_nulls=True
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            kwargs={
                "keep_nulls": True,
                "batch_size": 1000,
            },
        )

        # Verify bulk copy succeeded
        assert result is not None
        assert result["rows_copied"] == 3

        # Verify NULL was preserved (not replaced by default 999)
        cursor.execute(f"SELECT value FROM {table_name} WHERE id = 2")
        rows = cursor.fetchall()
        assert rows[0][0] is None, "NULL should be preserved when keep_nulls=True"

        # Verify other values are correct
        cursor.execute(f"SELECT value FROM {table_name} WHERE id = 1")
        rows = cursor.fetchall()
        assert rows[0][0] == 100

    finally:
        # Cleanup
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
        conn.close()


@pytest.mark.integration
def test_bulkcopy_keep_nulls_false_uses_defaults(client_context):
    """Test that keep_nulls=False uses column defaults for NULL values.

    Creates a table with a DEFAULT constraint and inserts NULL values.
    With keep_nulls=False, NULLs should be replaced by the default value.

    Note: This behavior depends on how the bulk copy implementation handles
    the KEEP_NULLS hint. The hint instructs SQL Server to either preserve
    NULLs (True) or use defaults for NULL values (False).
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = unique_table_name("BulkCopyNoKeepNulls")

    try:
        # Create table with DEFAULT constraint
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT,
                value INT DEFAULT 999
            )
        """)

        # Prepare data with NULL values
        data = [
            (1, 100),
            (2, None),  # NULL value - should become 999 with keep_nulls=False
            (3, 200),
        ]

        # Execute bulk copy with keep_nulls=False (explicit)
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            kwargs={
                "keep_nulls": False,
                "batch_size": 1000,
            },
        )

        # Verify bulk copy succeeded
        assert result is not None
        assert result["rows_copied"] == 3

        # Verify NULL was replaced by default value 999
        # Note: If this fails, it may indicate the KEEP_NULLS hint is not being
        # properly applied in the INSERT BULK statement. This is expected behavior
        # per SQL Server documentation for KEEP_NULLS = OFF.
        cursor.execute(f"SELECT value FROM {table_name} WHERE id = 2")
        rows = cursor.fetchall()
        # When keep_nulls=False, SQL Server should use the default value (999) for NULLs
        assert rows[0][0] == 999, f"NULL should be replaced by default (999) when keep_nulls=False, got {rows[0][0]}"

    finally:
        # Cleanup
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
        conn.close()


# =============================================================================
# KEEP_IDENTITY TESTS
# =============================================================================


@pytest.mark.integration
def test_bulkcopy_keep_identity_true_uses_explicit_values(client_context):
    """Test that keep_identity=True allows explicit IDENTITY column values.

    Creates a table with an IDENTITY column and inserts explicit values.
    With keep_identity=True, the provided values should be used instead of
    auto-generated values.

    Note: Unlike other bulk copy options, keep_identity does NOT use an
    INSERT BULK WITH hint. Instead, it controls whether the identity column
    is included in the TDS column metadata, allowing explicit values to be sent.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = unique_table_name("BulkCopyKeepIdentity")

    try:
        # Create table with IDENTITY column
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT IDENTITY(1,1),
                value NVARCHAR(50)
            )
        """)

        # Prepare data with explicit identity values (non-sequential to prove they're used)
        data = [
            (100, "Row at 100"),
            (200, "Row at 200"),
            (300, "Row at 300"),
        ]

        # Execute bulk copy with keep_identity=True
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            kwargs={
                "keep_identity": True,
                "batch_size": 1000,
            },
        )

        # Verify bulk copy succeeded
        assert result is not None
        assert result["rows_copied"] == 3

        # Verify explicit identity values were used (not 1, 2, 3)
        cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
        rows = cursor.fetchall()
        assert len(rows) == 3
        assert rows[0][0] == 100, f"Expected id=100, got {rows[0][0]}"
        assert rows[1][0] == 200, f"Expected id=200, got {rows[1][0]}"
        assert rows[2][0] == 300, f"Expected id=300, got {rows[2][0]}"

        # Verify the values are correct too
        assert rows[0][1] == "Row at 100"
        assert rows[1][1] == "Row at 200"
        assert rows[2][1] == "Row at 300"

    finally:
        # Cleanup
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
        conn.close()


@pytest.mark.integration
def test_bulkcopy_keep_identity_false_auto_generates(client_context):
    """Test that keep_identity=False uses auto-generated IDENTITY values.

    Creates a table with an IDENTITY column. With keep_identity=False,
    the IDENTITY column is excluded from the TDS column metadata, so
    SQL Server auto-generates identity values.

    Note: The source data should only contain non-identity columns when
    keep_identity=False, and column_mappings should map to non-identity columns.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = unique_table_name("BulkCopyNoKeepIdentity")

    try:
        # Create table with IDENTITY column
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT IDENTITY(1,1),
                value NVARCHAR(50)
            )
        """)

        # Prepare data WITHOUT identity column (only the value column)
        data = [
            ("Row 1",),
            ("Row 2",),
            ("Row 3",),
        ]

        # Execute bulk copy with keep_identity=False
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            kwargs={
                "keep_identity": False,
                "batch_size": 1000,
                "column_mappings": [(0, "value")],  # Map only to non-identity column
            },
        )

        # Verify bulk copy succeeded
        assert result is not None
        assert result["rows_copied"] == 3

        # Verify auto-generated identity values (1, 2, 3)
        cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
        rows = cursor.fetchall()
        assert len(rows) == 3
        assert rows[0][0] == 1, f"Expected auto-generated id=1, got {rows[0][0]}"
        assert rows[1][0] == 2, f"Expected auto-generated id=2, got {rows[1][0]}"
        assert rows[2][0] == 3, f"Expected auto-generated id=3, got {rows[2][0]}"

        # Verify values
        assert rows[0][1] == "Row 1"
        assert rows[1][1] == "Row 2"
        assert rows[2][1] == "Row 3"

    finally:
        # Cleanup
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
        conn.close()


# =============================================================================
# TABLE_LOCK TESTS
# =============================================================================


@pytest.mark.integration
def test_bulkcopy_table_lock_true(client_context):
    """Test that table_lock=True acquires a table-level lock.

    This test verifies the option is accepted and bulk copy completes successfully.
    The actual lock behavior is difficult to verify in a single-connection test.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = unique_table_name("BulkCopyTableLock")

    try:
        # Create table
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"CREATE TABLE {table_name} (id INT, value NVARCHAR(50))")

        # Prepare test data
        data = [
            (1, "Row 1"),
            (2, "Row 2"),
            (3, "Row 3"),
        ]

        # Execute bulk copy with table_lock=True
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            kwargs={
                "table_lock": True,
                "batch_size": 1000,
            },
        )

        # Verify bulk copy succeeded
        assert result is not None
        assert result["rows_copied"] == 3

        # Verify data was inserted
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        rows = cursor.fetchall()
        assert rows[0][0] == 3

    finally:
        # Cleanup
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
        conn.close()


# =============================================================================
# COMBINED OPTIONS TESTS
# =============================================================================


@pytest.mark.integration
def test_bulkcopy_multiple_options_combined(client_context):
    """Test using multiple bulk copy options together.

    Combines fire_triggers, check_constraints, keep_nulls, and table_lock options.
    Verifies each option's effect is correctly applied when used together.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    main_table = unique_table_name("BulkCopyCombinedMain")
    marker_table = unique_table_name("BulkCopyCombinedMarker")
    trigger_name = unique_table_name("trg_CombinedTest")

    try:
        # Create marker table
        cursor.execute(
            f"IF OBJECT_ID('{marker_table}', 'U') IS NOT NULL DROP TABLE {marker_table}"
        )
        cursor.execute(
            f"CREATE TABLE {marker_table} (id INT IDENTITY(1,1), triggered_at DATETIME DEFAULT GETDATE())"
        )

        # Create main table with CHECK constraint and DEFAULT
        cursor.execute(
            f"IF OBJECT_ID('{main_table}', 'U') IS NOT NULL DROP TABLE {main_table}"
        )
        cursor.execute(f"""
            CREATE TABLE {main_table} (
                id INT,
                value INT CHECK (value >= 0) DEFAULT 999
            )
        """)

        # Create trigger
        cursor.execute(f"""
            CREATE TRIGGER {trigger_name} ON {main_table}
            AFTER INSERT
            AS
            BEGIN
                INSERT INTO {marker_table} (triggered_at) VALUES (GETDATE())
            END
        """)

        # Prepare data with NULL (to test keep_nulls) and valid values
        # Note: value >= 0 constraint, so all non-NULL values must be >= 0
        data = [
            (1, 100),
            (2, None),  # Test keep_nulls - NULL should be preserved
            (3, 200),
            (4, 0),     # Edge case: exactly at constraint boundary
        ]

        # Execute bulk copy with multiple options
        result = cursor.bulkcopy(
            main_table,
            iter(data),
            kwargs={
                "fire_triggers": True,
                "check_constraints": True,
                "keep_nulls": True,
                "table_lock": True,
                "batch_size": 1000,
            },
        )

        # Verify bulk copy succeeded
        assert result is not None
        assert result["rows_copied"] == 4

        # Verify trigger fired (fire_triggers=True)
        cursor.execute(f"SELECT COUNT(*) FROM {marker_table}")
        rows = cursor.fetchall()
        assert rows[0][0] == 1, f"Trigger should have fired exactly once, fired {rows[0][0]} times"

        # Verify NULL was preserved (keep_nulls=True)
        cursor.execute(f"SELECT value FROM {main_table} WHERE id = 2")
        rows = cursor.fetchall()
        assert rows[0][0] is None, f"NULL should be preserved, got {rows[0][0]}"

        # Verify all 4 rows were inserted
        cursor.execute(f"SELECT COUNT(*) FROM {main_table}")
        rows = cursor.fetchall()
        assert rows[0][0] == 4, f"Expected 4 rows, got {rows[0][0]}"

        # Verify boundary value was inserted (check_constraints allowed it)
        cursor.execute(f"SELECT value FROM {main_table} WHERE id = 4")
        rows = cursor.fetchall()
        assert rows[0][0] == 0, f"Boundary value 0 should be inserted, got {rows[0][0]}"

    finally:
        # Cleanup
        cursor.execute(f"IF OBJECT_ID('{trigger_name}', 'TR') IS NOT NULL DROP TRIGGER {trigger_name}")
        cursor.execute(f"IF OBJECT_ID('{main_table}', 'U') IS NOT NULL DROP TABLE {main_table}")
        cursor.execute(f"IF OBJECT_ID('{marker_table}', 'U') IS NOT NULL DROP TABLE {marker_table}")
        conn.close()


@pytest.mark.integration
def test_bulkcopy_check_constraints_true_rejects_boundary_violation(client_context):
    """Test that check_constraints=True rejects values at the constraint boundary.

    Creates a table with CHECK (value > 0) and attempts to insert value=0.
    This should fail because 0 is not > 0.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = unique_table_name("BulkCopyCheckBoundary")

    try:
        # Create table with CHECK constraint (value must be > 0, not >= 0)
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT,
                value INT CHECK (value > 0)
            )
        """)

        # Prepare data with boundary violation (0 is not > 0)
        data = [
            (1, 100),
            (2, 0),  # Violates CHECK constraint (0 is not > 0)
            (3, 200),
        ]

        # Execute bulk copy with check_constraints=True - should fail
        with pytest.raises(Exception) as exc_info:
            cursor.bulkcopy(
                table_name,
                iter(data),
                kwargs={
                    "check_constraints": True,
                    "batch_size": 1000,
                },
            )

        # Verify we got a constraint violation error
        error_msg = str(exc_info.value).lower()
        assert any(keyword in error_msg for keyword in ["check", "constraint", "conflict", "547"]), \
            f"Expected constraint violation error, got: {exc_info.value}"

    finally:
        # Cleanup
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
        conn.close()

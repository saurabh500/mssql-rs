
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for XML data type using Python end-to-end path.

These tests validate PyCoreCursor.bulkcopy() for XML columns, ensuring correct mapping to SQL Server XML columns and on-wire serialization via the Rust core.
"""

import pytest
import mssql_py_core

@pytest.mark.integration
def test_cursor_bulkcopy_xml_basic(client_context):
    """Bulk copy into a table with XML column, including a NULL, and verify roundtrip."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyXmlTest"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, xml_col XML NULL)")

    data = [
        # Basic XML
        (1, '<root><child>Test1</child></root>'),
        # XML with attributes, namespaces, special characters, and CDATA
        (2, '<root xmlns="http://example.com" attr="value"><child id="42">Test2 &amp; special &lt;chars&gt;</child><nested><deep><element>🎉 Unicode!</element></deep></nested><![CDATA[Raw <data> here]]></root>'),
        # NULL XML
        (3, None),
        # XML with processing instruction and comments
        (4, '<?xml version="1.0"?><!-- This is a comment --><root><data>Test with PI</data></root>'),
        # XML with entities
        (5, '<root><text>&lt;tag&gt; &amp; &quot;quoted&quot; &apos;apostrophe&apos;</text></root>'),
        # Empty XML
        (6, ''),
    ]

    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={
            "timeout": 30,
            "column_mappings": [(0, "id"), (1, "xml_col")],
        },
    )

    assert result is not None
    assert result["rows_copied"] == 6

    cursor.execute(f"SELECT id, xml_col FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 6
    assert rows[0][0] == 1 and rows[0][1] == '<root><child>Test1</child></root>'
    assert rows[1][0] == 2
    # SQL Server normalizes XML, so just verify it contains key elements
    assert '<child id="42">' in rows[1][1] and 'Test2 &amp; special &lt;chars&gt;' in rows[1][1]
    # CDATA content is normalized by SQL Server - check for the escaped form
    assert '<![CDATA[Raw <data> here]]>' in rows[1][1] or 'Raw &lt;data&gt; here' in rows[1][1]
    assert rows[2][0] == 3 and rows[2][1] is None
    # SQL Server may preserve or normalize the XML with PI - just verify the data element exists
    assert rows[3][0] == 4 and '<data>Test with PI</data>' in rows[3][1]
    # Verify entities are preserved (SQL Server may normalize some)
    assert rows[4][0] == 5
    assert '&lt;' in rows[4][1] and '&gt;' in rows[4][1] and '&amp;' in rows[4][1]
    # Empty string is preserved as empty string
    assert rows[5][0] == 6 and rows[5][1] == ''

    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_xml_malformed(client_context):
    """Test that malformed XML is rejected by SQL Server."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyXmlMalformedTest"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, xml_col XML)")

    # Malformed XML: unclosed tag
    malformed_data = [
        (1, '<root><child>Test</root>'),  # Missing </child>
    ]

    # SQL Server should reject this during bulk copy
    error_raised = False
    try:
        result = cursor.bulkcopy(
            table_name,
            iter(malformed_data),
            kwargs={
                "timeout": 30,
                "column_mappings": [(0, "id"), (1, "xml_col")],
            },
        )
        print(f"No error raised. Result: {result}")
    except (ValueError, RuntimeError) as e:
        error_raised = True
        print(f"Expected error caught: {e}")
    assert error_raised

    # Cleanup can surface a deferred server error; ignore for DROP
    try:
        cursor.execute(f"DROP TABLE {table_name}")
    except Exception:
        pass
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_xml_large_document(client_context):
    """Test bulk copy with a large XML document."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyXmlLargeTest"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, xml_col XML)")

    # Create a moderately large XML document (not extreme to keep tests fast)
    # Generate 1000 child elements
    children = ''.join(f'<item id="{i}">Value_{i}</item>' for i in range(1000))
    large_xml = f'<root>{children}</root>'

    data = [
        (1, large_xml),
    ]

    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={
            "timeout": 30,
            "column_mappings": [(0, "id"), (1, "xml_col")],
        },
    )

    assert result is not None
    assert result["rows_copied"] == 1

    cursor.execute(f"SELECT id, xml_col FROM {table_name}")
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 1
    # Verify some elements exist in the returned XML
    assert '<item id="0">Value_0</item>' in rows[0][1]
    assert '<item id="999">Value_999</item>' in rows[0][1]

    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_xml_multiple_columns(client_context):
    """Test bulk copy with multiple XML columns in the same table."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyMultiXmlTest"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, xml_col1 XML, xml_col2 XML, xml_col3 XML NULL)")

    data = [
        (1, '<doc1>First</doc1>', '<doc2>Second</doc2>', '<doc3>Third</doc3>'),
        (2, '<doc1>Alpha</doc1>', '<doc2>Beta</doc2>', None),
    ]

    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={
            "timeout": 30,
            "column_mappings": [(0, "id"), (1, "xml_col1"), (2, "xml_col2"), (3, "xml_col3")],
        },
    )

    assert result is not None
    assert result["rows_copied"] == 2

    cursor.execute(f"SELECT id, xml_col1, xml_col2, xml_col3 FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 2
    assert rows[0][0] == 1
    assert rows[0][1] == '<doc1>First</doc1>'
    assert rows[0][2] == '<doc2>Second</doc2>'
    assert rows[0][3] == '<doc3>Third</doc3>'
    assert rows[1][0] == 2
    assert rows[1][1] == '<doc1>Alpha</doc1>'
    assert rows[1][2] == '<doc2>Beta</doc2>'
    assert rows[1][3] is None

    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_xml_mixed_with_other_types(client_context):
    """Test bulk copy with XML column mixed with various other data types."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyXmlMixedTest"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"""
        CREATE TABLE {table_name} (
            id INT,
            name NVARCHAR(100),
            xml_col XML,
            amount DECIMAL(10, 2),
            created DATETIME
        )
    """)

    data = [
        (1, 'First', '<data>XML1</data>', 123.45, '2024-01-15 10:30:00'),
        (2, 'Second', '<data>XML2</data>', 678.90, '2024-01-16 14:20:00'),
    ]

    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={
            "timeout": 30,
            "column_mappings": [
                (0, "id"),
                (1, "name"),
                (2, "xml_col"),
                (3, "amount"),
                (4, "created")
            ],
        },
    )

    assert result is not None
    assert result["rows_copied"] == 2

    cursor.execute(f"SELECT id, name, xml_col, amount FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 2
    assert rows[0][0] == 1 and rows[0][1] == 'First'
    assert rows[0][2] == '<data>XML1</data>'
    assert float(rows[0][3]) == 123.45
    assert rows[1][0] == 2 and rows[1][1] == 'Second'
    assert rows[1][2] == '<data>XML2</data>'
    assert float(rows[1][3]) == 678.90

    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()

package ru.fizteh.fivt.students.msandrikova.storeable;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ru.fizteh.fivt.storage.structured.Storeable;
import ru.fizteh.fivt.storage.structured.TableProvider;
import ru.fizteh.fivt.students.msandrikova.shell.Utils;

public class StoreableTableTest {
	private StoreableTable table;
	private File path;
	private TableProvider tableProvider;

	@After
	public void clear() {
		if(path.exists()) {
			try {
				Utils.remover(path, "test", false);
			} catch (Exception e) {
				System.err.println("Can not remove something");
			}
		}
	}

	@Before
	public void setUp() throws Exception {
		path = new File(System.getProperty("user.home"), "sandbox");
		clear();
		new File(path, "tableName").mkdirs();
		List<Class<?>> columnTypes = new ArrayList<Class<?>>();
		columnTypes.add(Integer.class);
		columnTypes.add(Boolean.class);
		columnTypes.add(String.class);
		tableProvider = new StoreableTableProvider(path);
		table = new StoreableTable(path, "tableName", columnTypes, tableProvider);
	}
	
	@Test
	public void testGetName() {
		assertEquals(table.getName(), "tableName");
	}

	@Test
	public void testPutGetOverwrite() {
		Storeable value = null;
		Storeable newvalue = null;
		try {
			value = tableProvider.deserialize(table, "[1 , true, \" value 1 \"]");
			newvalue = tableProvider.deserialize(table, "[1 , null, \" value 1 \"]");
		} catch (ParseException e) {}
		assertNull(table.put("key", value));
		assertEquals(table.get("key"), value);
		assertEquals(table.put("key", newvalue), value);
		assertEquals(table.get("key"), "newvalue");
	}

	@Test
	public void testRemove() {
		Storeable value = null;
		try {
			value = tableProvider.deserialize(table, "[1 , true, \" value 1 \"]");
		} catch (ParseException e) {}
		assertNull(table.put("key", value));
		assertEquals(table.remove("key"), value);
		assertNull(table.get("key"));
	}

	@Test
	public void testSize() {
		Storeable value = null;
		Storeable newvalue = null;
		try {
			value = tableProvider.deserialize(table, "[1 , true, \" value 1 \"]");
			newvalue = tableProvider.deserialize(table, "[1 , null, \" value 1 \"]");
		} catch (ParseException e) {}
		assertNull(table.put("key", value));
		assertEquals(table.size(), 1);
		assertEquals(table.put("key", newvalue), value);
		assertEquals(table.size(), 1);
		assertEquals(table.remove("key"), newvalue);
		assertEquals(table.size(), 0);
	}

	@Test
	public void testCommitRollback() {
		Storeable value = null;
		Storeable stuff = null;
		try {
			value = tableProvider.deserialize(table, "[1 , true, \" value 1 \"]");
			stuff = tableProvider.deserialize(table, "[13, false, null]");
		} catch (ParseException e) {}
		assertNull(table.put("key", value));
		try {
			assertEquals(table.commit(), 1);
		} catch (IllegalStateException e) {} catch (IOException e) {}
		assertEquals(table.remove("key"), value);
		assertEquals(table.put("key", value), null);
		assertEquals(table.rollback(), 0);
		assertNull(table.put("stuff", stuff));
		assertEquals(table.rollback(), 1);
		assertNull(table.get("stuff"));
	}

	@Test
	public void testGetColumnsCount() {
		assertEquals(table.getColumnsCount(), 3);
	}

	@Test
	public void testGetColumnType() {
		assertEquals(table.getColumnType(0), Integer.class);
		assertEquals(table.getColumnType(1), Boolean.class);
		assertEquals(table.getColumnType(2), String.class);
	}

}

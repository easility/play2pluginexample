package controllers;

import java.util.ArrayList;
import java.util.Collection;

import java.util.List;



import play.Play;

import play.mvc.Controller;
import play.mvc.Result;
import views.html.index;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.z3api.NoSqlTypedSession;
import com.alvazan.orm.api.z5api.IndexPoint;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboDatabaseMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.TypedRow;
import com.alvazan.orm.models.test.Account;
import com.alvazan.play.NoSql;

public class Application extends Controller {

	static Cursor<List<TypedRow>> rowsIter;
	static int batchsize = 50;

	public static Result index() {
		return ok(index.render("Test String"));
	}

	public static Result allTables() {
		//String keyspace = Play.application().configuration().getString("nosql.cassandra.keyspace");
		NoSqlEntityManager mgr = NoSql.em();
		DboDatabaseMeta database = mgr.find(DboDatabaseMeta.class,
				DboDatabaseMeta.META_DB_ROWKEY);
		Collection<DboTableMeta> allColumnFamilies = database.getAllTables();
		ArrayList<String> tableList = new ArrayList<String>();
		for (DboTableMeta tableMeta : allColumnFamilies) {
			System.out.println("Adding table :" + tableMeta.getColumnFamily() + " in list");
			tableList.add(tableMeta.getColumnFamily());
		}
		//return ok(views.html.allTables.render(tableList));
		System.out.println("SUCESSFUL TILL HERE");
		return TODO;
	}

	public static Result detailResult(String columnFamilyName) {
		NoSqlEntityManager mgr = NoSql.em();
		DboDatabaseMeta database = mgr.find(DboDatabaseMeta.class,
				DboDatabaseMeta.META_DB_ROWKEY);
		Collection<DboTableMeta> allTables = database.getAllTables();
		ArrayList<String> columnList = new ArrayList<String>();
		ArrayList<String> indexList = new ArrayList<String>();
		ArrayList<String> showList = new ArrayList<String>();
		for (DboTableMeta tableMeta : allTables) {
			if (tableMeta.getColumnFamily().equals(columnFamilyName)) {
				Collection<DboColumnMeta> allColumns = tableMeta
						.getAllColumns();
				Collection<DboColumnMeta> allIndexColumn = tableMeta
						.getIndexedColumns();
				for (DboColumnMeta column : allColumns)
					columnList.add(column.getColumnName());
				for (DboColumnMeta index : allIndexColumn)
					indexList.add(index.getColumnName());
				for (DboColumnMeta column : allColumns) {
					if (!indexList.contains(column.getColumnName()))
						showList.add(column.getColumnName());
				}
				break;
			}
		}
		return TODO;
		//render(indexList, columnFamilyName, showList);
	}

	public static void processIndex(String columnFamilyName, String index) {
		NoSqlEntityManager mgr = NoSql.em();
		NoSqlTypedSession s = mgr.getTypedSession();
		ArrayList<String> columnList = new ArrayList<String>();
		Cursor<IndexPoint> indexView = s.indexView(columnFamilyName, index,
				null, null);
		int count = 0;
		int first = 50;
		while (indexView.next() && count < first) {
			IndexPoint current = indexView.getCurrent();
			String indVal = current.getIndexedValueAsString();
			columnList.add(indVal);
			count++;
		}
		//render(columnList, columnFamilyName, index, first, count);
	}
	
	public static Result get() {
		NoSqlEntityManager mgr = NoSql.em();
		TypedRow row = mgr.getTypedSession().find("Account", "acc1");
		if (row !=null ) {
			System.out.println("row.getRowKeyString()" + row.getRowKeyString());
			System.out.println("row.getRowKeyString()" + row.getColumnsAsColl().toString());
		}		
        System.out.println("GET SUCESSFUL TILL HERE");
		return TODO;
		
	}
	
	public static Result save() {
		NoSqlEntityManager mgr = NoSql.em();
        Account acc1e = new Account();
        acc1e.setId("acc1");
        acc1e.setIsActive(false);
        acc1e.setSomeField(34);
		mgr.put(acc1e);
		mgr.flush();
        System.out.println("SUCESSFUL SAVE TILL HERE");
		return TODO;
	}


}
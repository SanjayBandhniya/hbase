package com.mikemunhall.hbasedaotest.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.mikemunhall.hbasedaotest.service.HbaseService;

@RestController
public class TestController {

	@Autowired
	private HbaseService hbaseService;

	@GetMapping(value = "/create")
	public void create() throws IOException {
		String[] column = { "id", "name", "city" };
		String[] values = { "1", "ABC", "Japan" };
		hbaseService.addData("row1", "emp1", "personal", column, values);
	}

	@GetMapping(value = "/get")
	public List<Result> get(@RequestParam String name) {
		return hbaseService.scaner(name);
	}

	@GetMapping(value = "/find/{paramId}")
	public List<Employee> findById(@PathVariable String paramId) throws IOException {
		List<Result> resultList = hbaseService.findById("emp1", "personal", "id", paramId);
		List<Employee> empList = new ArrayList<>();
		resultList.forEach(data -> {

			NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> aa = data.getMap();
			for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : aa.entrySet()) {
				//System.out.println(Bytes.toString(entry.getKey()) + ":" + entry.getValue());

				NavigableMap<byte[], byte[]> map = data.getFamilyMap(Bytes.toBytes(Bytes.toString(entry.getKey())));
				for (Entry<byte[], byte[]> bb : map.entrySet()) {
					System.out.println(Bytes.toString(entry.getKey()) + ":" + Bytes.toString(bb.getValue()));
				}

			}

			// System.out.println(data.getColumnCells(Bytes.toBytes("personal"),
			// Bytes.toBytes("name")));

			String id = Bytes.toString(data.getValue(Bytes.toBytes("personal"), Bytes.toBytes("id")));
			String name = Bytes.toString(data.getValue(Bytes.toBytes("personal"), Bytes.toBytes("name")));
			String city = Bytes.toString(data.getValue(Bytes.toBytes("personal"), Bytes.toBytes("city")));
			empList.add(new Employee(id, name, city));
		});
		return empList;

	}

	@GetMapping(value = "/data")
	public List<Employee> getData() throws IOException {
		List<String> rowKeys = Arrays.asList("row1", "row2");
		List<Result> resultList = hbaseService.getListRowKey("emp1", rowKeys, "personal", "");
		List<Employee> empList = new ArrayList<>();
		resultList.forEach(data -> {

			System.out.println(data);

			String id = Bytes.toString(data.getValue(Bytes.toBytes("personal"), Bytes.toBytes("id")));
			String name = Bytes.toString(data.getValue(Bytes.toBytes("personal"), Bytes.toBytes("name")));
			String city = Bytes.toString(data.getValue(Bytes.toBytes("personal"), Bytes.toBytes("city")));
			empList.add(new Employee(id, name, city));
		});
		return empList;
	}

	private void readData() throws IOException {
		// Instantiating Configuration class
		Configuration config = HBaseConfiguration.create();

		// Instantiating HTable class
		HTable table = new HTable(config, "emp");

		// Instantiating Get class
		Get g = new Get(Bytes.toBytes("row1"));

		// Reading the data
		Result result = table.get(g);

		// Reading values from Result class object
		byte[] value = result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("name"));

		byte[] value1 = result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("city"));

		// Printing the values
		String name = Bytes.toString(value);
		String city = Bytes.toString(value1);

		System.out.println("name: " + name + " city: " + city);

	}

	@GetMapping(value = "/insertData")
	private void insertData() throws IOException {
		// Instantiating Configuration class
		Configuration config = HBaseConfiguration.create();

		// Instantiating HTable class
		HTable hTable = new HTable(config, "emp1");

		// Instantiating Put class
		// accepts a row name.
		Put p = new Put(Bytes.toBytes("row1"));

		// adding values using add() method
		// accepts column family name, qualifier/row name ,value
		p.add(Bytes.toBytes("personal"), Bytes.toBytes("id"), Bytes.toBytes(1));
		p.add(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes("raju"));
		p.add(Bytes.toBytes("personal"), Bytes.toBytes("city"), Bytes.toBytes("hyderabad"));

		// Saving the put Instance to the HTable.
		hTable.put(p);
		System.out.println("data inserted");

		// closing HTable
		hTable.close();

	}

	@GetMapping(value = "/createTable")
	private void createTable() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		System.err.println("Here.....");
		// Instantiating configuration class
		Configuration con = HBaseConfiguration.create();
		// Instantiating HbaseAdmin class
		HBaseAdmin admin = new HBaseAdmin(con);
		// Instantiating table descriptor class
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("emp1"));

		// Adding column families to table descriptor
		tableDescriptor.addFamily(new HColumnDescriptor("personal"));
		tableDescriptor.addFamily(new HColumnDescriptor("professional"));

		// Execute the table through admin
		admin.createTable(tableDescriptor);
		System.out.println(" Table created ");
	}

	private void listTable() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {

		Configuration con = HBaseConfiguration.create();
		// Instantiating HbaseAdmin class
		HBaseAdmin admin = new HBaseAdmin(con);

		HTableDescriptor[] tableDescriptor = admin.listTables();

		// printing all the table names.
		for (int i = 0; i < tableDescriptor.length; i++) {
			System.out.println(tableDescriptor[i].getNameAsString());
		}
	}

}

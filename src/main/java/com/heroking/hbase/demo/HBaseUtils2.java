package com.heroking.hbase.demo;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.*;


/**
 * 2.x，3.x可用
 * HTableDescriptor,HColumnDescriptor等2.x废弃，3.x移除
 * @author k
 * @version 1.0
 */
public class HBaseUtils2 {

    // 建表
    public static void createTable(String tableName, String[] cols) throws IOException {
        Admin admin = null;
        Connection connection = null;
        try {
            connection = HBaseFactory.getConnection();
            admin = connection.getAdmin();
            TableName tName = TableName.valueOf(tableName);
            if (admin.tableExists(tName)) {
                System.out.println("talbe is exists!");
            } else {
                System.out.println("创建表!");
                TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
                for (String col : cols) {
                    ColumnFamilyDescriptor family = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(col)).build();//构建列族对象
                    tableDescriptor.setColumnFamily(family);//设置列族
                }
                admin.createTable(tableDescriptor.build());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                admin.close();
            }
            if (connection != null) {
                //如果频繁使用，不用关闭连接
                connection.close();
            }
        }


    }

    /**
     * 同步写
     *
     * @param tableName       表名
     * @param rowKey          行键(此处测试使用了string类型，实际可以任意类型拼接的byte[])
     * @param columnFamily    列族
     * @param columnQualifier 列名
     * @param value           值
     * @return boolean 是否插入成功
     * @throws Exception
     */
    public Boolean put(String tableName, String rowKey, String columnFamily, String columnQualifier, String value) throws Exception {
        Table table = null;
        Connection connection = null;
        boolean flag = false;
        try {
            connection = HBaseFactory.getConnection();
            table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier), Bytes.toBytes(value));
            table.put(put);
            flag = true;
        } finally {
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                //如果频繁使用，不用关闭连接
                connection.close();
            }
        }
        return flag;
    }

    // 异步写
    @Test
    public void testNonSyncPut() throws Exception {
        BufferedMutator bufferedMutator = null;
        Connection connection = null;
        try {
            connection = HBaseFactory.getConnection();
            bufferedMutator = connection.getBufferedMutator(TableName.valueOf("user表"));
            Put put = new Put(Bytes.toBytes("rowKeyUser11"));
            put.addColumn(Bytes.toBytes("列族名"), Bytes.toBytes("user_name"), Bytes.toBytes("张三"));
            bufferedMutator.mutate(put);
            List<Put> putList = new LinkedList<Put>();
            Put put1 = new Put(Bytes.toBytes("rowKeyUser12"));
            put1.addColumn(Bytes.toBytes("列族名"), Bytes.toBytes("user_name"), Bytes.toBytes("李四"));
            Put put2 = new Put(Bytes.toBytes("rowKeyUser13"));
            put2.addColumn(Bytes.toBytes("列族名"), Bytes.toBytes("user_name"), Bytes.toBytes("王五"));
            putList.add(put1);
            putList.add(put2);
            bufferedMutator.mutate(putList);//批量异步写

        } finally {
            if (bufferedMutator != null) {
                bufferedMutator.close();
            }
            if (connection != null) {
                //如果频繁使用，不用关闭连接
                connection.close();
            }
        }

    }

    /**
     * @param tableName 表名
     * @param rowKey    行键(此处测试使用了string类型，实际可以任意类型拼接的byte[])
     */
    public static void get(String tableName, String rowKey) throws Exception {
        Table table = null;
        Connection connection = null;
        try {
            connection = HBaseFactory.getConnection();
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            //如果列数较多，可以指定拿特定列的值
            //get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("user_name"));
            Result result = table.get(get);
            for (Cell cell : result.listCells()) {
                System.out.println("qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("value:" + Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("-------------------------------");
            }
        } finally {
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                //如果频繁使用，不用关闭连接
                connection.close();
            }
        }
    }

    /**
     * 通过getList获取多条记录
     *
     * @param tableName 表名
     * @param getList   多个自定义的Get
     * @return 结果集
     * @throws Exception
     */
    public List<Map<String, Object>> getList(String tableName, List<Get> getList) throws Exception {
        Table table = null;
        Connection connection = null;
        List<Map<String, Object>> getResults;
        try {
            connection = HBaseFactory.getConnection();
            table = connection.getTable(TableName.valueOf(tableName));
            Result[] results = table.get(getList);
            getResults = new LinkedList<Map<String, Object>>();
            for (Result result : results) {
                List<Cell> ceList = result.listCells();
                Map<String, Object> map = new HashMap<String, Object>();
                if (ceList != null && ceList.size() > 0) {
                    for (Cell cell : ceList) {
                        /*map.put(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) +
                                        "_" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()),
                                Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));*/
                        map.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
                    }
                }
                getResults.add(map);
            }
        } finally {
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                //如果频繁使用，不用关闭连接
                connection.close();
            }
        }
        return getResults;
    }

    // 批量查找数据
    public static void scanPageData(String tableName, String colFamily, String startRow, String stopRow, String lastRowKey, int pageSize) throws IOException {
        Table table = null;
        Connection connection = null;
        try {
            connection = HBaseFactory.getConnection();
            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.addFamily(colFamily.getBytes());
            if (StringUtils.isNotBlank(lastRowKey)) {
                scan.withStartRow(Bytes.toBytes(lastRowKey),true);
            } else {
                scan.withStartRow(Bytes.toBytes(startRow),true);
            }
            scan.withStopRow(Bytes.toBytes(stopRow),true);
            //MUST_PASS_ALL过滤器必须全符合
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            //分页过滤器(与其它过滤器一起使用是分页过滤器放到后面)
            PageFilter pageFilter = new PageFilter(pageSize);
            filterList.addFilter(pageFilter);
            scan.setFilter(filterList);
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner.next(pageSize)) {
                String rowKey = new String(CellUtil.cloneRow(result.rawCells()[0]));
                if (lastRowKey.equals(rowKey)) {
                    continue;
                }
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    String resRowKey = new String(CellUtil.cloneRow(cell));
                    String name = new String(CellUtil.cloneQualifier(cell));
                    String value = new String(CellUtil.cloneValue(cell));
                    System.out.println("rowKey=" + resRowKey + ",key=" + name + ",value=" + value);
                }
                System.out.println("----------");
            }
            table.close();
            //Scan类常用方法说明
            //指定需要的family或column ，如果没有调用任何addFamily或Column，会返回所有的columns；
            // scan.addFamily();
            // scan.addColumn();
            // scan.setMaxVersions(); //指定最大的版本个数。如果不带任何参数调用setMaxVersions，表示取所有的版本。如果不掉用setMaxVersions，只会取到最新的版本.
            // scan.setTimeRange(); //指定最大的时间戳和最小的时间戳，只有在此范围内的cell才能被获取.
            // scan.setTimeStamp(); //指定时间戳
            // scan.setFilter(); //指定Filter来过滤掉不需要的信息
            // scan.setStartRow(); //指定开始的行。如果不调用，则从表头开始；
            // scan.setStopRow(); //指定结束的行（不含此行）；
            // scan.setBatch(); //指定最多返回的Cell数目。用于防止一行中有过多的数据，导致OutofMemory错误。

            //过滤器
            //1、FilterList代表一个过滤器列表
            //FilterList.Operator.MUST_PASS_ALL -->and
            //FilterList.Operator.MUST_PASS_ONE -->or
            //eg、FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
            //2、SingleColumnValueFilter
            //3、ColumnPrefixFilter用于指定列名前缀值相等
            //4、MultipleColumnPrefixFilter和ColumnPrefixFilter行为差不多，但可以指定多个前缀。
            //5、QualifierFilter是基于列名的过滤器。
            //6、RowFilter
            //7、RegexStringComparator是支持正则表达式的比较器。
            //8、SubstringComparator用于检测一个子串是否存在于值中，大小写不敏感。

            //设置想取的数据的版本号，不设置则取最新值
            //scan.setMaxVersions();
            //指定最多返回的Cell数目。用于防止一行中有过多的数据，导致OutofMemory错误。
            //scan.setBatch(1000);

            //scan.setTimeStamp(NumberUtils.toLong("1370336286283"));
            //scan.setTimeRange(NumberUtils.toLong("1370336286283"), NumberUtils.toLong("1370336337163"));
            //scan.setStartRow(Bytes.toBytes("quanzhou"));
            //scan.setStopRow(Bytes.toBytes("xiamen"));
            //scan.addFamily(Bytes.toBytes("info"));
            //scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"));

            //查询列镞为info，列id值为1的记录
            //方法一(单个查询)
            // Filter filter = new SingleColumnValueFilter(
            //         Bytes.toBytes("info"), Bytes.toBytes("id"), CompareOp.EQUAL, Bytes.toBytes("1"));
            // scan.setFilter(filter);

            //方法二(组合查询)
            //FilterList filterList=new FilterList();
            //Filter filter = new SingleColumnValueFilter(
            //    Bytes.toBytes("info"), Bytes.toBytes("id"), CompareOp.EQUAL, Bytes.toBytes("1"));
            //filterList.addFilter(filter);
            //scan.setFilter(filterList);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                //如果频繁使用，不用关闭连接
                connection.close();
            }
        }


    }

    @Test
    public void testPut() throws Exception {
        put("tname", "10003", "col", "name", "小明");
    }


    @Test
    public void testGet() throws Exception {
        get("tname", "10003");
    }

    @Test
    public void testGetList() {
        List<Get> getList = new ArrayList<Get>();
        Get get1 = new Get(Bytes.toBytes("10003"));
        Get get2 = new Get(Bytes.toBytes("10004"));
        getList.add(get1);
        getList.add(get2);
        try {
            List<Map<String, Object>> users = getList("tname", getList);
            for (Map<String, Object> user : users) {
                Iterator<Map.Entry<String, Object>> entries = user.entrySet().iterator();
                while (entries.hasNext()) {
                    Map.Entry<String, Object> entry = entries.next();
                    System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
                }
                System.out.println("-------------------------------");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreateTable() throws Exception {
        String[] arr = new String[]{"sku"};
        createTable("tname", arr);
    }

    @Test
    public void testScanPageList() throws Exception {
        //起止行符合包左不包右原则
        scanPageData("tname","col","10001","10009","",5);
        System.out.println("=============");
        scanPageData("tname","col","10001","10009","10005",5);
        System.out.println("=============");

        scanPageData("tname","col","10001","10009","10010",5);
        System.out.println("=============");

    }

    @Test
    public void testScan() throws Exception {
        Connection connection = null;
        Table table = null;
        ResultScanner scanner = null;
        try {
            connection = HBaseFactory.getConnection();
            table = connection.getTable(TableName.valueOf("tname"));
            Scan scan = new Scan();
            //请根据业务场景合理设置StartKey，StopKey
            scan.withStartRow(Bytes.toBytes("10001"),true);
            scan.withStopRow(Bytes.toBytes("10004"),true);
            scanner = table.getScanner(scan);
            for (Result result : scanner) {
                System.out.println(Bytes.toString(result.getRow()));
                for (Cell cell : result.listCells()) {
                    System.out.println("qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                    System.out.println("value:" + Bytes.toString(CellUtil.cloneValue(cell)));
                    System.out.println("**************");
                }
                System.out.println("-------------------------------");
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                //如果频繁使用，不用关闭连接
                connection.close();
            }
        }
    }

}


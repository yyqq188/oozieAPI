package com.pactera.oozieAPI.json2sql.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class HBaseUtils {
    private static Configuration conf = null;
    private static Admin admin = null;
    private static ExecutorService pool = Executors.newScheduledThreadPool(20);    //设置hbase连接池
    private static Connection connection = null;
    private static HBaseUtils instance = null;
    private static HBaseConfig hbaseConfig = new HBaseConfig();

    private HBaseUtils() {
        if (connection == null) {
            try {
                //将hbase配置类中定义的配置加载到连接池中每个连接里
                conf = hbaseConfig.getConfiguration();
                connection = ConnectionFactory.createConnection(conf, pool);
                admin = connection.getAdmin();
            } catch (IOException e) {
                System.out.println("HbaseUtils实例初始化失败！错误信息为：" + e.getMessage());
            }
        }
    }


    //简单单例方法，如果autowired自动注入就不需要此方法
    public static synchronized HBaseUtils getInstance() {
        if (instance == null) {
            instance = new HBaseUtils();
        }
        return instance;
    }

    public static Connection getConnection() {
        if (connection == null) {
            try {
                //将hbase配置类中定义的配置加载到连接池中每个连接里
                conf = hbaseConfig.getConfiguration();
                connection = ConnectionFactory.createConnection(conf, pool);
                admin = connection.getAdmin();
            } catch (IOException e) {
                System.out.println("HbaseUtils实例初始化失败！错误信息为：" + e.getMessage());
            }
        }
        return connection;
    }

    static Map<String, String> map = new HashMap<String, String>();

    public static void PutHbase(Connection connection, String key, String value, String tablename, boolean flg) throws Exception {
        if (flg) {
            map.put(key, value);
        }
        if (!flg) {
            HTable indexTable = (HTable) connection.getTable(TableName.valueOf(tablename));
            Put indexPut = new Put(Bytes.toBytes(map.get("rowkey")));
            Set<String> set = map.keySet();
            for (String keys : set) {
                indexPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(keys), Bytes.toBytes(map.get(keys)));
            }
            indexTable.put(indexPut);
            indexTable.close();
            map.clear();
        }
    }


    /*
     * 创建表
     *
     * @param tableName         表名
     * @param columnFamily      列族（数组）
     */

    public static void createTable(String tableName, String[] columnFamily) throws IOException {
        TableName name = TableName.valueOf(tableName);
        //如果存在则删除
        if (admin.tableExists(name)) {
//            admin.disableTable(name);
//            admin.deleteTable(name);
            System.out.println("create htable error! this table {} already exists!"+ name);
        } else {
            HTableDescriptor desc = new HTableDescriptor(name);
            for (String cf : columnFamily) {
                desc.addFamily(new HColumnDescriptor(cf));
            }
            admin.createTable(desc);
        }
    }

    /*
     * 插入记录（单行单列族-多列多值）
     *
     * @param tableName         表名
     * @param row               行名
     * @param columnFamilys     列族名
     * @param columns           列名（数组）
     * @param values            值（数组）（且需要和列一一对应）
     */

    public static void insertRecords(Connection connection, String tableName, String row, String columnFamilys, String[] columns, String[] values) throws IOException {
        TableName name = TableName.valueOf(tableName);
        Table table = connection.getTable(name);
        Put put = new Put(Bytes.toBytes(row));
        for (int i = 0; i < columns.length; i++) {
            put.addColumn(Bytes.toBytes(columnFamilys), Bytes.toBytes(columns[i]), Bytes.toBytes(values[i]));
            table.put(put);
        }
    }

    /*
     * 插入记录（单行单列族-单列单值）
     *
     * @param tableName         表名
     * @param row               行名
     * @param columnFamily      列族名
     * @param column            列名
     * @param value             值
     */

    public static void insertOneRecord(Connection connection, String tableName, String row, String columnFamily, String column, String value) throws IOException {
        TableName name = TableName.valueOf(tableName);
        Table table = connection.getTable(name);
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
    }

    /**
     * 删除一行记录
     *
     * @param tablename 表名
     * @param rowkey    行名
     */

    public static void deleteRow(Connection connection, String tablename, String rowkey) throws IOException {
        TableName name = TableName.valueOf(tablename);
        Table table = connection.getTable(name);
        Delete d = new Delete(rowkey.getBytes());
        table.delete(d);
    }

    /*
     * 删除单行单列族记录
     * @param tablename         表名
     * @param rowkey            行名
     * @param columnFamily      列族名
     */

    public static void deleteColumnFamily(Connection connection, String tablename, String rowkey, String columnFamily) throws IOException {
        TableName name = TableName.valueOf(tablename);
        Table table = connection.getTable(name);
        Delete d = new Delete(rowkey.getBytes()).addFamily(Bytes.toBytes(columnFamily));
        table.delete(d);
    }

    /*
     * 删除单行单列族单列记录
     *
     * @param tablename         表名
     * @param rowkey            行名
     * @param columnFamily      列族名
     * @param column            列名
     */

    public static void deleteColumn(Connection connection, String tablename, String rowkey, String columnFamily, String column) throws IOException {
        TableName name = TableName.valueOf(tablename);
        Table table = connection.getTable(name);
        Delete d = new Delete(rowkey.getBytes()).addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        table.delete(d);
    }


    /**
     * 查找一行记录
     *
     * @param tablename 表名
     * @param rowKey    行名
     */

    public static String selectRow(Connection connection, String tablename, String rowKey) throws IOException {
        String record = "";
        TableName name = TableName.valueOf(tablename);
        Table table = connection.getTable(name);
        Get g = new Get(rowKey.getBytes());
        Result rs = table.get(g);
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = rs.getMap();
        for (Cell cell : rs.rawCells()) {
            StringBuffer stringBuffer = new StringBuffer()
                    .append(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())).append("\t")
                    .append(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())).append("\t")
                    .append(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())).append("\t")
                    .append(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())).append("\n");
            String str = stringBuffer.toString();
            record += str;
        }
        return record;
    }
    /*
     * @Description 根据tablename,rowkey(主键)获取一行数据
     * @Params [tablename, rowKey]
     * @Return java.util.Map<java.lang.String,java.lang.Object>
     * @Author yangjie
     */

    public static Map<String, Object> selectRowMap(Connection connection, String tablename, String rowKey) throws IOException {
        TableName name = TableName.valueOf(tablename);
        Table table = connection.getTable(name);
        Get g = new Get(rowKey.getBytes());
        Result rs = table.get(g);
        Map<String, Object> resultMap = new HashMap<>();
        if (!rs.isEmpty()) {
            resultMap = revResutToMap(rs);
        }
        return resultMap;
    }



    /*
     * 查找单行单列族单列记录
     *
     * @param tablename         表名
     * @param rowKey            行名
     * @param columnFamily      列族名
     * @param column            列名
     * @return
     */

    public static String selectValue(Connection connection, String tablename, String rowKey, String columnFamily, String column) throws IOException {
        TableName name = TableName.valueOf(tablename);
        Table table = connection.getTable(name);
        Get g = new Get(rowKey.getBytes());
        g.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        Result rs = table.get(g);
        return Bytes.toString(rs.value());
    }

    /**
     * 查询表多行多列数据
     * @param connection hbase连接
     * @param tablename 表名
     * @param columnFamilys 列族名
     * @param columns 列名
     * @return list集合
     * @throws IOException 异常
     */
    public static List<Map<String, Object>> selectScanList(Connection connection, String tablename, List<String> columnFamilys,List<String> columns) throws IOException {
        List<Map<String, Object>> record = new ArrayList<>();
        TableName name = TableName.valueOf(tablename);
        Table table = connection.getTable(name);
        Scan scan = new Scan();
        if(columnFamilys!=null) {
            for (int i = 0; i < columnFamilys.size(); i++) {
                scan.addColumn(Bytes.toBytes(columnFamilys.get(i)), Bytes.toBytes(columns.get(i)));
            }
        }
        ResultScanner scanner = table.getScanner(scan);
        try {
            for (Result result : scanner) {
                Map<String, Object> resultMap = new HashMap<>();
                resultMap = revResutToMap(result);
                record.add(resultMap);
                /*for (Cell cell : result.rawCells()) {
                    StringBuffer stringBuffer = new StringBuffer()
                            .append(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())).append("\t")
                            .append(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())).append("\t")
                            .append(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())).append("\t")
                            .append(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())).append("\n");
                    String str = stringBuffer.toString();
                    record += str;
                }*/
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
        return record;
    }

    /*
     * 查询表中所有行（Scan方式）
     *
     * @param tablename
     * @return
     */

    public static String scanAllRecord(Connection connection, String tablename) throws IOException {
        String record = "";
        TableName name = TableName.valueOf(tablename);
        Table table = connection.getTable(name);
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        try {
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    StringBuffer stringBuffer = new StringBuffer()
                            .append(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())).append("\t")
                            .append(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())).append("\t")
                            .append(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())).append("\t")
                            .append(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())).append("\n");
                    String str = stringBuffer.toString();
                    record += str;
                }
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }

        return record;
    }

    /*
     * 根据rowkey关键字查询报告记录
     * @param tablename
     * @param rowKeyword
     * @return
     */

    public static List scanReportDataByRowKeyword(Connection connection, String tablename, String rowKeyword) throws IOException {
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        Table table = connection.getTable(TableName.valueOf(tablename));
        Scan scan = new Scan();
        //添加行键过滤器，根据关键字匹配,包含子串匹配,不区分大小写。
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(rowKeyword));
        scan.setFilter(rowFilter);
        ResultScanner scanner = table.getScanner(scan);
        try {
            if (scanner != null) {
                list = revScannerToList(scanner);
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }

        return list;
    }

    /*
     *  将result转换成map集合
     * @param rs
     * @return
     */

    public static Map<String, Object> revResutToMap(Result rs) {
        Map<String, Object> resultMap = new HashMap<>();
        List<Cell> cells = rs.listCells();
        for (Cell cell : cells) {
            String key = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            resultMap.put(key, value);
        }
        return resultMap;
    }
    /*
     * @Description 将scanner转换为List
     * @Params [scanner]
     * @Return java.util.ArrayList<java.lang.Object>
     * @Author yangjie
     */

    public static ArrayList<Map<String, Object>> revScannerToList(ResultScanner scanner) {
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        for (Result result : scanner) {
            list.add(revResutToMap(result));
        }
        return list;
    }

    /*
     * 根据rowkey关键字和时间戳范围查询报告记录
     *
     * @param tablename
     * @param rowKeyword
     * @return
     */

    public static List scanReportDataByRowKeywordTimestamp(Connection connection, String tablename, String rowKeyword, Long minStamp, Long maxStamp) throws IOException {
        ArrayList<Object> list = new ArrayList<Object>();

        Table table = connection.getTable(TableName.valueOf(tablename));
        Scan scan = new Scan();
        //添加scan的时间范围
        scan.setTimeRange(minStamp, maxStamp);

        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(rowKeyword));
        scan.setFilter(rowFilter);

        ResultScanner scanner = table.getScanner(scan);
        try {
            for (Result result : scanner) {
                //TODO 此处根据业务来自定义实现
                list.add(null);
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }

        return list;
    }


    /*
     * 删除表操作
     *
     * @param tablename
     */

    public static void deleteTable(String tablename) throws IOException {
        TableName name = TableName.valueOf(tablename);
        if (admin.tableExists(name)) {
            admin.disableTable(name);
            admin.deleteTable(name);
        }
    }

    public static void main(String[] args) {
        new HBaseUtils();

    }

}

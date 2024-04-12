package com.cnso.flinkcdc.service;

import com.cnso.flinkcdc.util.MysqlTypeDbUtil;
import com.google.inject.internal.cglib.core.$LocalVariablesSorter;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.xssf.usermodel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DataCheckSys {

    public final static String FILENAME = "dbs.properties";

    public final static String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public final static String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";

    //Mysql
    public final static String DB_URL_201 = "201.db.r.mysql.url";
    public final static String DB_USER_201 = "201.db.r.mysql.user";
    public final static String DB_PWD_201 = "201.db.r.mysql.pwd";

    //Tidb
    public final static String DB_URL_TIDB = "tidb.db.r.mysql.url";
    public final static String DB_USER_TIDB = "tidb.db.r.mysql.user";
    public final static String DB_PWD_TIDB = "tidb.db.r.mysql.pwd";

    //Hive
    public final static String DB_URL_hive = "hive.url";

    //DS
    public static BasicDataSource DsTidb = null;
    public static BasicDataSource Ds201 = null;
    public static BasicDataSource DsHive = null;

    // TIDB <表 L>
    public static Map<String, Long> tidbCountMap = new ConcurrentHashMap<>();
    // DS201
    public static Map<String, Long> Ds201CountMap = new ConcurrentHashMap<>();
    // DSHive <表 L>
    public static Map<String, Long> hiveCountMap = new ConcurrentHashMap<>();

    // 201[<db, tab>,,,]
    public static List<Map<String, String>> dbAndTableOf201 = new ArrayList<>();

    public static String dt = "2023-11-01";

    public static final Logger log = LoggerFactory.getLogger(DataCheckSys.class);

    public static void main(String[] args) throws IOException{
        // 初始化数据源
        initDs();

        // 计算myslq的cnt
        System.out.println("mysql count start:====================");
        Thread mysqlThread = new Thread( () -> {
            try {
                getMysqlCountMap();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
        mysqlThread.start();

        // 计算tidb的count数
        System.out.println("tidb count start:=====================");
        Thread tidbThread = new Thread(() -> {
            try {
                getTidbCountMap();
            } catch(Exception e) {
                e.printStackTrace();
            }
        });
        tidbThread.start();
        try {
            tidbThread.join();
            mysqlThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        int i = reRun201TimeoutTable();
        if (i != 0) {
            System.out.println("超时表 " + timeOutTable201);
        }

        System.out.println(Ds201CountMap.toString());
        System.out.println("mysql cnt done.");
        System.out.println(tidbCountMap.toString());
        System.out.println("tidb cnt done");

        myPrint2Excel(Ds201CountMap, tidbCountMap, hiveCountMap);

    }

    // 将map写入excel
    private static void myPrint2Excel(Map<String, Long> ds201CountMap, Map<String, Long> tidbCountMap, Map<String, Long> hiveCountMap) {
        XSSFWorkbook workbook = new XSSFWorkbook();
        XSSFCellStyle errCellStyle = workbook.createCellStyle();
        XSSFFont font = workbook.createFont();
        font.setColor(IndexedColors.RED.getIndex());
        errCellStyle.setFillBackgroundColor(IndexedColors.YELLOW.getIndex());
        errCellStyle.setFont(font);

        XSSFSheet sheet = workbook.createSheet("MYSQL_TiDB_Hive");

        // 表头
        XSSFRow row = sheet.createRow(0);
        row.createCell(0).setCellValue("tab");
        row.createCell(1).setCellValue("mysql");
        row.createCell(2).setCellValue("tidb");
        row.createCell(3).setCellValue("hive");

        // 表体
        Set<String> tabSet = ds201CountMap.keySet();
        ArrayList<String> tabList = new ArrayList<>(tabSet);
        Collections.sort(tabList);
        for (int i = 0; i < tabList.size(); i++) {
            XSSFRow row1 = sheet.createRow(i + 1);
            row1.createCell(0).setCellValue(tabList.get(i));
            Long mysqlCount = ds201CountMap.getOrDefault(tabList.get(i), 0L);
            row1.createCell(1).setCellValue(mysqlCount);

            Long tidbCount = tidbCountMap.getOrDefault(tabList.get(i), 0L);
            XSSFCell tidbCell = row1.createCell(2);
            tidbCell.setCellValue(tidbCount);
            if (!tidbCount.equals(mysqlCount)) {
                tidbCell.setCellStyle(errCellStyle);
            }
            Long hiveCount = hiveCountMap.getOrDefault(tabList.get(i), 0L);
            XSSFCell hiveCell = row1.createCell(3);
            hiveCell.setCellValue(hiveCount);
            if(!hiveCount.equals(mysqlCount))
                hiveCell.setCellStyle(errCellStyle);
        }

        //TIDB
        XSSFSheet tidbSheet = workbook.createSheet("TIDB");
        //表头
        XSSFRow tidbRow = tidbSheet.createRow(0);
        tidbRow.createCell(0).setCellValue("tab");
        tidbRow.createCell(1).setCellValue("tidb");
        //表体
        Set<String> tidbTabSet = tidbCountMap.keySet();
        ArrayList<String> tidbTabList = new ArrayList<>(tidbTabSet);
        Collections.sort(tidbTabList);
        for (int i = 0; i < tidbTabList.size(); i++) {
            XSSFRow row1 = tidbSheet.createRow(i + 1);
            row1.createCell(0).setCellValue(tidbTabList.get(i));
            row1.createCell(1).setCellValue(tidbCountMap.get(tidbTabList.get(i)));
        }

        //HIVE
        XSSFSheet hiveSheet = workbook.createSheet("HIVE");
        //表头
        XSSFRow hiveRow = tidbSheet.createRow(0);
        tidbRow.createCell(0).setCellValue("tab");
        tidbRow.createCell(1).setCellValue("hive");
        //表体
        Set<String> hiveTabSet = tidbCountMap.keySet();
        ArrayList<String> hiveTabList = new ArrayList<>(tidbTabSet);
        Collections.sort(hiveTabList);
        for (int i = 0; i < hiveTabList.size(); i++) {
            XSSFRow row1 = hiveSheet.createRow(i + 1);
            row1.createCell(0).setCellValue(hiveTabList.get(i));
            row1.createCell(1).setCellValue(hiveCountMap.get(tidbTabList.get(i)));
        }



        // 写入文件
        String filePath="D:\\dbDiffCnt.xlsx";
        File file = null;
        try {
            file = new File(filePath);
            workbook.write(new FileOutputStream(file));
            System.out.println("写入完毕");
            workbook.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static int reRun201TimeoutTable() {
        for (int i = 0; i < 3; i++) {
            if (timeOutTable201.size() != 0) {
                ArrayList<String> finishList = new ArrayList<>();
                timeOutTable201.forEach(x -> {
                    Connection conn201 = null;
                    Statement stat201 = null;
                    ResultSet resultSet = null;
                    try {
                        conn201 = Ds201.getConnection();
                        System.out.println(Thread.currentThread().getName() + "201: " + x);
                        stat201 = conn201.createStatement();
                        resultSet = stat201.executeQuery("select count(1) from " + x);
                        while (resultSet.next()) {
                            long aLong = resultSet.getLong(1);
                            String tab = x.split("\\.")[1];

                            Long lastIsNum = null;
                            String cleanTab = null;
                            if(tab.contains("_")) {
                                try{
                                    lastIsNum = Long.parseLong(tab.substring(tab.lastIndexOf("_") + 1));
                                    if (lastIsNum.toString().length() == 4) {
                                        lastIsNum = null;
                                    }
                                } catch (Exception e) {

                                }
                            }
                            if(lastIsNum != null) {
                                cleanTab = tab.substring(0, tab.lastIndexOf("_"));
                            } else {
                                cleanTab = tab;
                            }
                            System.out.println(Thread.currentThread().getName() + "cleanTab :" + cleanTab);
                            Long along1 = Ds201CountMap.get(cleanTab);
                            System.out.println(Thread.currentThread().getName() + "count now: " + aLong);
                            if (along1!= null && along1 != 0) {
                                Ds201CountMap.put(cleanTab, aLong + along1);
                            } else {
                                Ds201CountMap.put(cleanTab, aLong);
                            }

                            /* 源表不合
                            Long aLong2 = Ds202CountMap.get(tab);
                            if (aLong2!=null && aLong2 != 0){
                                Ds201CountMap.put(tab, aLong + aLong2);
                            } else {
                                Ds201CountMap.put(tab, aLong);
                            }*/
                        }
                        finishList.add(x);
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    } finally {
                        try {
                            resultSet.close();
                        } catch (SQLException throwables) {
                            throwables.printStackTrace();
                        }
                        try {
                            stat201.close();
                        } catch (SQLException throwables) {
                            throwables.printStackTrace();
                        }
                        try {
                            conn201.close();
                        } catch (SQLException throwables) {
                            throwables.printStackTrace();
                        }
                    }
                });
                timeOutTable201.removeAll(finishList);
            }
        }
        return timeOutTable201.size()==0 ? 0 : 1;
    }

    // 获取所有tidb表的cnt数据
    private static void getTidbCountMap() throws SQLException, InterruptedException {
        Connection conn = DsTidb.getConnection();
        Statement statement = conn.createStatement();
        ArrayList<String> tidbProdEnterpriseList = new ArrayList<>();
        ResultSet showTables = statement.executeQuery("show tables");
        while (showTables.next()){
            String table = showTables.getString(1);
            tidbProdEnterpriseList.add(table);
        }
        showTables.close();
        statement.close();
        conn.close();

        ExecutorService tidbExecutorService = Executors.newFixedThreadPool(30);
        int subListSize = 50;
        ArrayList<List<String>> tidbSubLists = new ArrayList<>();
        for (int i = 0; i < tidbProdEnterpriseList.size(); i += subListSize) {
            tidbSubLists.add(tidbProdEnterpriseList.subList(i, Math.min(i+subListSize, tidbProdEnterpriseList.size())));
        }
        for (List<String> tidbSubList :
                tidbSubLists) {
            tidbExecutorService.submit(() -> tidbSubList.forEach(x -> {
                Connection conn1 = null;
                Statement stat = null;
                ResultSet resultSet = null;
                try {
                    conn1 = DsTidb.getConnection();
                    stat = conn1.createStatement();
                    resultSet = stat.executeQuery("select count(1) c from " + x);
                    if (resultSet.next()) {
                        long aLong = resultSet.getLong(1);
                        System.out.println(Thread.currentThread().getName() + "---> tidb table " + x + " count: " + aLong);
                        tidbCountMap.put(x, aLong);
                    }
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                } finally {
                    try {
                        resultSet.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                    try {
                        stat.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                    try {
                        conn1.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
            }));
        }
        tidbExecutorService.shutdown();
        tidbExecutorService.awaitTermination(24, TimeUnit.HOURS);
    }

    public static List<String> timeOutTable201 = new ArrayList<>();
    // 获取mysql的所有表cnt
    private static void getMysqlCountMap() throws SQLException {
        // 生成库表结构
        genMysql201Schema();

        // 获取cnt数
        List<String> dt201 = resetSchema(dbAndTableOf201);
        // 多线程
        ArrayList<List<String>> dt201SubLists = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(40);
        //200个表均分
        int subListSize = 30;
        for (int i = 0; i < dt201.size(); i+= subListSize)
            dt201SubLists.add(dt201.subList(i, Math.min(i + subListSize, dt201.size())));
        for (List<String> dt201SubList:
             dt201SubLists) {
            executorService.submit(() -> dt201SubList.forEach(x -> {
                Connection conn201 = null;
                Statement stat201 = null;
                ResultSet res = null;
                try{
                    conn201 = Ds201.getConnection();
                    System.out.println(Thread.currentThread().getName() + "201: " + x);
                    stat201 = conn201.createStatement();
                    res = stat201.executeQuery("select count(1) from " + x);
                    while (res.next()){
                        long aLong = res.getLong(1);
                        String tab = x.split("\\.")[1];
                        // 分表 后缀_1
                        Long lastIsNum = null;
                        String cleanTab;
                        if (tab.contains("_")) {
                            try {
                                lastIsNum = Long.parseLong(tab.substring(tab.lastIndexOf("_") + 1));
                                if (lastIsNum.toString().length() == 4)
                                    lastIsNum = null;
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        if (lastIsNum != null) {
                            cleanTab = tab.substring(0, tab.lastIndexOf("_"));
                        } else {
                            cleanTab = tab;
                        }
                        System.out.println(Thread.currentThread().getName() + "cleanTab : " + cleanTab);
                        Long aLong1 = Ds201CountMap.get(cleanTab);
                        System.out.println(Thread.currentThread().getName() + "count now : " + aLong);
                        if (aLong1 != null && aLong1 != 0) {
                            Ds201CountMap.put(cleanTab, aLong + aLong1);
                        } else {
                            Ds201CountMap.put(cleanTab, aLong);
                        }
                    }

                } catch (SQLException throwables) {
                    // 计算超时
                    timeOutTable201.add(x);
                    throwables.printStackTrace();
                } finally {
                    if (res != null) {
                        try {
                            res.close();
                        } catch (SQLException throwables) {
                            throwables.printStackTrace();
                        }
                    }
                    try {
                        stat201.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                    try {
                        conn201.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
            }));
        }

        executorService.shutdown();
        try {
            executorService.awaitTermination(24, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    // 清洗成flinkSchema
    private static List<String> resetSchema(List<Map<String, String>> list) {
        ArrayList<String> flinkSchemaList = new ArrayList<>();
        list.forEach(x -> {
            Set<Map.Entry<String, String>> entries = x.entrySet();
            for (Map.Entry<String, String> entry :
                    entries) {
                flinkSchemaList.add(entry.getKey() + "." + entry.getValue());
            }
        });
        return flinkSchemaList;
    }

    // 生成201库表结构
    private static void genMysql201Schema() throws SQLException {
        Connection conn201 = Ds201.getConnection();
        Statement showDatabse = conn201.createStatement();
        ResultSet databaseRes = showDatabse.executeQuery("show database");
        ArrayList<String> dbList201 = new ArrayList<>();
        while (databaseRes.next()) {
            String db = databaseRes.getString(1);
            if (db.startsWith("db_"))
                dbList201.add(db);
        }
        databaseRes.close();
        showDatabse.close();

        // 获取表名
        dbList201.forEach( x -> {
            Statement stat = null;
            ResultSet res = null;
            try {
                stat = conn201.createStatement();
                res = stat.executeQuery("SELECT table_name FROM information_schema.tables WHERE table_name = '" + x + "'");
                while (res.next()) {
                    String tabName = res.getString(1);
                    HashMap<String, String> dbAndTableMap = new HashMap<>(1);
                    dbAndTableMap.put(x, tabName);
                    dbAndTableOf201.add(dbAndTableMap);
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            } finally {
                if(null != res) {
                    try {
                        res.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
                if(null != stat) {
                    try {
                        stat.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
            }
        });
        conn201.close();
    }


    // 初始化数据源
    public static void initDs() throws IOException {
        Properties prop = loadProp();
        DsTidb = MysqlTypeDbUtil.init(MYSQL_DRIVER, prop.getProperty(DB_URL_TIDB), prop.getProperty(DB_USER_TIDB), prop.getProperty(DB_PWD_TIDB));
        Ds201 = MysqlTypeDbUtil.init(MYSQL_DRIVER, prop.getProperty(DB_URL_201), prop.getProperty(DB_USER_201), prop.getProperty(DB_PWD_201));
        DsHive = MysqlTypeDbUtil.init(HIVE_DRIVER, prop.getProperty(DB_URL_hive), "hive", "hive@2022");

    }

    public static Properties loadProp() throws IOException {
        Properties prop = new Properties();
        InputStream in = DataCheckSys.class.getClassLoader().getResourceAsStream(FILENAME);
        prop.load(in);
        return prop;
    }
}

package com.cnso.flinkcdc.service;

import com.cnso.flinkcdc.util.MysqlTypeDbUtil;
import com.google.inject.internal.util.$AsynchronousComputationException;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.xssf.usermodel.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class TidbAndHiveSchemaAndPk {

    public final static String tidbDBName = "prod_enterprise";
    public final static String hiveDBName = "ods_enterprise";

    public static void main(String[] args) throws IOException, SQLException {
        DataCheckSys.initDs();
        Map<String, String> tidbTablesAndPkMap = getTidbTablesAndPkMap();
        List<String> hiveMetaDataTableNameAndPK = getHiveMetaDataTableNameAndPK();

        ArrayList<String> hiveCleanTabs = new ArrayList<>();
        hiveMetaDataTableNameAndPK.forEach(x -> {
            String cleanTab = x.replace("ods_prod_enterprise_", "").replace("_df", "");
            hiveCleanTabs.add(cleanTab);
        });
        myPrint2Excel(tidbTablesAndPkMap, hiveCleanTabs);
    }

    private static void myPrint2Excel(Map<String, String> tidbTablesAndPkMap, ArrayList<String> hiveCleanTabs) {
        XSSFWorkbook workbook = new XSSFWorkbook();
        XSSFCellStyle errCellStyle = workbook.createCellStyle();
        XSSFFont font = workbook.createFont();
        font.setColor(IndexedColors.RED.getIndex());
        errCellStyle.setFillBackgroundColor(IndexedColors.YELLOW.getIndex());
        errCellStyle.setFont(font);

        XSSFSheet sheet = workbook.createSheet("TIDB-HIVE");
        XSSFRow row = sheet.createRow(0);
        row.createCell(0).setCellValue("tidb-db");
        row.createCell(1).setCellValue("tidb-tab");
        row.createCell(2).setCellValue("hive-db");
        row.createCell(3).setCellValue("hive-tab");
        row.createCell(4).setCellValue("pk");

        // 表体
        Set<String> tabSet = tidbTablesAndPkMap.keySet();
        ArrayList<String> tabList = new ArrayList<>(tabSet);
        Collections.sort(tabList);
        for (int i = 0; i < tabList.size(); i++) {
            XSSFRow row1 = sheet.createRow(i + 1);
            String tidbTab = tabList.get(i);
            row1.createCell(0).setCellValue(tidbDBName);
            row1.createCell(1).setCellValue(tidbTab);
            row1.createCell(2).setCellValue(hiveDBName);
            hiveCleanTabs.forEach(x->{
                if(x.equals(tidbTab)){
                    row1.createCell(3).setCellValue("ods_prod_enterprise_"+x+"_df");
                }
            });
            row1.createCell(4).setCellValue(tidbTablesAndPkMap.get(tidbTab).toLowerCase());
        }
        // 写入excel
        String path = "D:\\TidbAndHiveSchema.xlsx";
        File file = new File(path);
        try{
            workbook.write(new FileOutputStream(file));
            System.out.println("写入完毕");
            workbook.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static List<String> getHiveMetaDataTableNameAndPK() throws SQLException {
        BasicDataSource hiveMetaDs = MysqlTypeDbUtil.init(DataCheckSys.MYSQL_DRIVER, "jdbc:mysql://node128:3306/metastore?characterEncoding=utf8&useSSL=false&rewriteBatchedStatements=true&useServerPrepStmts=true&allowMultiQueries=true&connectTimeout=30000&socketTimeout=0", "root", "pwd");
        Connection conn = hiveMetaDs.getConnection();
        Statement statement = conn.createStatement();
        ArrayList<String> hiveTableList = new ArrayList<>();
        ResultSet show_tables = statement.executeQuery("SELECT t.TBL_ID, t.TBL_NAME, MAX(p.PART_NAME) max_dt " +
                "FROM dbs d, tbls t, partitions p " +
                "WHERE d.`name`='ods_enterprise' AND d.DB_ID = t.DB_ID AND t.TBL_ID = p.TBL_ID" +
                "GROUP BY t.TBL_ID, t.TABLE_NAME " +
                "ORDER BY max_dt");
        while (show_tables.next()) {
            String table = show_tables.getString("TBL_NAME");
            hiveTableList.add(table);
        }
        show_tables.close();
        statement.close();
        conn.close();
        return hiveTableList;
    }

    private static Map<String, String> getTidbTablesAndPkMap() throws SQLException {
        Connection conn = DataCheckSys.DsTidb.getConnection();
        Statement statement = conn.createStatement();
        ArrayList<String> tidbProdEnterpriseTablesList = new ArrayList<>();
        ResultSet show_tables = statement.executeQuery("show tables");
        while (show_tables.next()) {
            String table = show_tables.getString(1);
            if (!table.endsWith("_tmp") && !table.endsWith("_old") && !table.endsWith("_tmp")){
                tidbProdEnterpriseTablesList.add(table);
            }
        }
        show_tables.close();
        statement.close();

        HashMap<String, String> tableNameAndPKMap = new HashMap<>();
        tidbProdEnterpriseTablesList.forEach(x -> {
            try{
                Statement statement1 = conn.createStatement();
                ResultSet res = statement1.executeQuery("show keys from " + x + " where key_name= 'PRIMARY'");
                while (res.next()){
                    String column_name = res.getString("column_name");
                    tableNameAndPKMap.put(x, column_name);
                }
                res.close();
                statement1.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        });
        conn.close();
        return tableNameAndPKMap;
    }
}

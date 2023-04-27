package com.cnso.flinkcdc.sink;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.compress.Gzip;
import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson.JSON;
import com.cnso.flinkcdc.model.BinlogData;
import com.cnso.flinkcdc.util.TiDBUtil;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.cnso.flinkcdc.common.constant.ParameterConstant.IS_SEND_OFFSET;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SEND_OFFSET_URL;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SINK_TIDB_DATABASE;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SINK_TIDB_DRIVER;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SINK_TIDB_PASSWORD;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SINK_TIDB_URL;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SINK_TIDB_USERNAME;

@Deprecated
public class TiDBSink extends RichSinkFunction<Iterable<String>> {

    private final static Logger logger = LoggerFactory.getLogger(TiDBSink.class);

    private final static String DateParsePattern = "yyyy-MM-dd HH:mm:ss";

    // private final static ArrayList<String> dbPrefix = Lists.newArrayList("db_df_enterprise_", "db_business_reproduce_", "db_code_", "db_df_bidding_", "db_df_credit_", "db_df_danger_", "db_df_enterprise_reports_", "db_df_gspublic_", "db_df_institution_", "db_df_invest_", "db_df_ip_", "db_df_justice_", "db_df_management_", "db_df_miningdata_", "db_df_tax_", "db_df_trademark_", "db_df_trademark_other_", "db_df_yq_article_", "db_enterprise_index_", "db_qualification_certificate_", "db_df_lawsuits_", "db_szse_");

    private ParameterTool params;
    private String db;
    private Boolean isSend;
    private String sendOffsetUrl;

    public TiDBSink(ParameterTool params) {
        this.params = params;
        this.db = params.get(SINK_TIDB_DATABASE);
        this.isSend = params.getBoolean(IS_SEND_OFFSET, false);
        this.sendOffsetUrl = params.get(SEND_OFFSET_URL, null);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        String driver = params.get(SINK_TIDB_DRIVER, "com.mysql.cj.jdbc.Driver");
        String url = params.get(SINK_TIDB_URL);
        String user = params.get(SINK_TIDB_USERNAME);
        String pwd = params.get(SINK_TIDB_PASSWORD);

        TiDBUtil.init(driver, url, user, pwd);
        logger.info("Connection pool init succeed!!!");
    }

    @Override
    public void close() throws Exception {
        TiDBUtil.close();
        super.close();
    }

    @Override
    public void invoke(Iterable<String> value, Context context) throws Exception {
        Connection conn = null;
        Statement stmt = null;
        Map<String, Long> offsetMap = null;
        try {
            conn = TiDBUtil.getConn();
            conn.setAutoCommit(false);
            stmt = conn.createStatement();

            int flag = 0;
            offsetMap = new HashMap<>(171);
            for (String line : value) {
                BinlogData binlogData = JSON.parseObject(line, BinlogData.class);
                List<Map<String, Object>> dataList = binlogData.getData();
                String topic = binlogData.getTopic();
                String tableName = getTableName(topic);
                if (isSend) {
                    Long offset = binlogData.getOffset();
                    offsetMap.compute(tableName, (k, v) -> v == null ? offset : Math.max(v, offset));
                }

                for (Map<String, Object> temp : dataList) {
                    BinlogData tempD = new BinlogData();
                    BeanUtil.copyProperties(binlogData, tempD);
                    List<Map<String, Object>> dt = new ArrayList<>();
                    dt.add(temp);
                    tempD.setData(dt);
                    String pk = tempD.getPkNames()
                            .stream()
                            .map(col -> String.valueOf(temp.get(col)))
                            .collect(Collectors.joining("|"));
                    String database = tempD.getDatabase();
                    String table = tempD.getTable();
                    String updateTime = temp.get("local_row_update_time").toString();
                    Long upTimeStamp = parsNanoTime(updateTime);
                    Long ts = tempD.getTs();
                    String op = tempD.getType();
                    Map<String, Object> params = new HashMap<>(7);
                    String message = encodeMessage(JSON.toJSONString(tempD));
                    params.put("msg_content", message);
                    params.put("exec_time", upTimeStamp);
                    params.put("pk_id", pk);
                    params.put("db_name", database);
                    params.put("table_name", table);
                    params.put("ts", ts);
                    params.put("op", op);
                    String now = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    params.put("create_time", now);
                    params.put("update_time", now);

                    String execSql = buildExecSql(this.db, tableName, params);
                    stmt.addBatch(execSql);
                    flag += 1;
                }

                if (flag % 100 == 0) {
                    stmt.executeBatch();
                    conn.commit();
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            stmt.executeBatch();
            conn.commit();

            if (stmt != null) {
                stmt.close();
            }

            if (conn != null) {
                conn.close();
            }

            if (isSend) {
                sendOffset(offsetMap, sendOffsetUrl);
            }
        }
    }

    private String buildExecSql(String db, String table, Map<String, Object> params) {
        Set<String> cols = params.keySet();

        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ");
        sqlBuilder.append(db)
                .append(".")
                .append(table)
                .append(" (");
        cols.forEach(col -> sqlBuilder.append(col).append(", "));
        sqlBuilder.delete(sqlBuilder.length() - 2, sqlBuilder.length());
        sqlBuilder.append(") VALUES(");
        cols.forEach(col -> {
            if (("exec_time").equals(col) || ("ts").equals(col)) {
                sqlBuilder.append(params.get(col)).append(", ");
            } else {
                sqlBuilder.append("'").append(params.get(col)).append("', ");
            }
        });
        sqlBuilder.delete(sqlBuilder.length() -2, sqlBuilder.length());
        sqlBuilder.append(")");


        return sqlBuilder.toString();
    }

    private Long parsNanoTime(String dateStr){
        Long l = 0l;
        if (StringUtils.isNotEmpty(dateStr)){
            String[] split = dateStr.split("\\.");
            if (split.length ==2){
                try {
                    Date date = DateUtils.parseDate(split[0], DateParsePattern);
                    l = date.getTime()/1000;
                    double pow = Math.pow(10, split[1].length());
                    l = l * new Double(pow).longValue();
                    l = l + new Long(split[1]).longValue();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }
        return l;
    }

    private String getTableName(String topic) {

        String tableName = topic;

        String[] tmp = topic.split("_t_");
        if (tmp.length == 2) {
            tableName = "t_" + tmp[1];
        } else {
            tableName = topic.split("db_szse_")[1];
        }

        return tableName;
    }

    private void sendOffset(Map<String, Long> offsetMap, String url) {
        ExecutorService executor = Executors.newSingleThreadExecutor();

        Runnable run = new Runnable() {
            @Override
            public void run() {
                try {
                    Map<String, Object> paramMap = new HashMap<>();
                    paramMap.put("offsetMap", offsetMap);
                    HttpUtil.post(url, paramMap);
                } catch (Exception e) {
                    logger.warn("send offset failed", e);
                }
            }
        };

        executor.execute(run);
    }

    private static byte[] compressByGZip(byte[] sourceFileBytes) throws IOException {
        ByteArrayOutputStream gzipBaos = null;
        GzipCompressorOutputStream gzipGcos = null;

        try {
            gzipBaos = new ByteArrayOutputStream();
            gzipGcos = new GzipCompressorOutputStream(gzipBaos);

            gzipGcos.write(sourceFileBytes);
        } finally {
            if (gzipBaos != null) {
                gzipBaos.close();
            }
            if (gzipGcos != null) {
                gzipGcos.close();
            }
        }

        return gzipBaos.toByteArray();
    }

    private static String encodeMessage(String str) throws IOException {
        byte[] bytes = compressByGZip(str.getBytes(StandardCharsets.UTF_8));
        byte[] encode = Base64.getEncoder().encode(bytes);
        return new String(encode,StandardCharsets.UTF_8);
    }


    public static void main(String[] args) {
        try {
            String str = "select 't_enterprise' as tn, count(0) from t_enterprise union all\n" +
                    "select 't_whole_park' as tn, count(0) from t_whole_park union all\n" +
                    "select 't_whole_park_relation' as tn, count(0) from t_whole_park_relation union all\n" +
                    "select 't_country_code' as tn, count(0) from t_country_code union all\n" +
                    "select 't_currency_code' as tn, count(0) from t_currency_code union all\n" +
                    "select 't_econ_kind_code' as tn, count(0) from t_econ_kind_code union all\n" +
                    "select 't_industry_code' as tn, count(0) from t_industry_code union all\n" +
                    "select 't_job_title_code' as tn, count(0) from t_job_title_code union all\n" +
                    "select 't_procedure_code' as tn, count(0) from t_procedure_code union all\n" +
                    "select 't_bidding_content' as tn, count(0) from t_bidding_content union all\n" +
                    "select 't_bidding_info' as tn, count(0) from t_bidding_info union all\n" +
                    "select 't_bidding_related' as tn, count(0) from t_bidding_related union all\n" +
                    "select 't_administrative_punishment' as tn, count(0) from t_administrative_punishment union all\n" +
                    "select 't_huanbaochufas' as tn, count(0) from t_huanbaochufas union all\n" +
                    "select 't_executions' as tn, count(0) from t_executions union all\n" +
                    "select 't_restricted_consumer' as tn, count(0) from t_restricted_consumer union all\n" +
                    "select 't_terminationcaseitem' as tn, count(0) from t_terminationcaseitem union all\n" +
                    "select 't_address' as tn, count(0) from t_address union all\n" +
                    "select 't_best_brief' as tn, count(0) from t_best_brief union all\n" +
                    "select 't_branches' as tn, count(0) from t_branches union all\n" +
                    "select 't_change_records' as tn, count(0) from t_change_records union all\n" +
                    "select 't_emails' as tn, count(0) from t_emails union all\n" +
                    "select 't_history_names' as tn, count(0) from t_history_names union all\n" +
                    "select 't_history_oper_names' as tn, count(0) from t_history_oper_names union all\n" +
                    "select 't_history_regist_capis' as tn, count(0) from t_history_regist_capis union all\n" +
                    "select 't_investments' as tn, count(0) from t_investments union all\n" +
                    "select 't_last_industry' as tn, count(0) from t_last_industry union all\n" +
                    "select 't_partners_alls' as tn, count(0) from t_partners_alls union all\n" +
                    "select 't_telephones' as tn, count(0) from t_telephones union all\n" +
                    "select 't_websites' as tn, count(0) from t_websites union all\n" +
                    "select 't_report_details' as tn, count(0) from t_report_details union all\n" +
                    "select 't_social_security' as tn, count(0) from t_social_security union all\n" +
                    "select 't_abnormal' as tn, count(0) from t_abnormal union all\n" +
                    "select 't_checkups' as tn, count(0) from t_checkups union all\n" +
                    "select 't_clear_account' as tn, count(0) from t_clear_account union all\n" +
                    "select 't_double_checkups' as tn, count(0) from t_double_checkups union all\n" +
                    "select 't_equityquality' as tn, count(0) from t_equityquality union all\n" +
                    "select 't_judicial_freezes' as tn, count(0) from t_judicial_freezes union all\n" +
                    "select 't_knowledge_properties' as tn, count(0) from t_knowledge_properties union all\n" +
                    "select 't_license_info' as tn, count(0) from t_license_info union all\n" +
                    "select 't_logoutannouncement' as tn, count(0) from t_logoutannouncement union all\n" +
                    "select 't_mortgages' as tn, count(0) from t_mortgages union all\n" +
                    "select 't_serious_illegal' as tn, count(0) from t_serious_illegal union all\n" +
                    "select 't_simple_cancellation_announcement' as tn, count(0) from t_simple_cancellation_announcement union all\n" +
                    "select 't_privatefund_emprecord' as tn, count(0) from t_privatefund_emprecord union all\n" +
                    "select 't_privatefund_manager' as tn, count(0) from t_privatefund_manager union all\n" +
                    "select 't_privatefund_mgrdishonest' as tn, count(0) from t_privatefund_mgrdishonest union all\n" +
                    "select 't_privatefund_partner' as tn, count(0) from t_privatefund_partner union all\n" +
                    "select 't_company_finance' as tn, count(0) from t_company_finance union all\n" +
                    "select 't_company_products' as tn, count(0) from t_company_products union all\n" +
                    "select 't_company_similars' as tn, count(0) from t_company_similars union all\n" +
                    "select 't_invest_info' as tn, count(0) from t_invest_info union all\n" +
                    "select 't_investment_events' as tn, count(0) from t_investment_events union all\n" +
                    "select 't_project_info' as tn, count(0) from t_project_info union all\n" +
                    "select 't_project_member' as tn, count(0) from t_project_member union all\n" +
                    "select 't_project_tag' as tn, count(0) from t_project_tag union all\n" +
                    "select 't_copyrights_relations' as tn, count(0) from t_copyrights_relations union all\n" +
                    "select 't_patents_info' as tn, count(0) from t_patents_info union all\n" +
                    "select 't_auctions_relations' as tn, count(0) from t_auctions_relations union all\n" +
                    "select 't_cases' as tn, count(0) from t_cases union all\n" +
                    "select 't_cases_relations' as tn, count(0) from t_cases_relations union all\n" +
                    "select 't_kaitinggonggaos' as tn, count(0) from t_kaitinggonggaos union all\n" +
                    "select 't_kaitinggonggaos_relations' as tn, count(0) from t_kaitinggonggaos_relations union all\n" +
                    "select 't_notices' as tn, count(0) from t_notices union all\n" +
                    "select 't_notices_relations' as tn, count(0) from t_notices_relations union all\n" +
                    "select 't_creditimportexport_data' as tn, count(0) from t_creditimportexport_data union all\n" +
                    "select 't_goudi_information' as tn, count(0) from t_goudi_information union all\n" +
                    "select 't_new_jobs' as tn, count(0) from t_new_jobs union all\n" +
                    "select 't_migration_enterprise' as tn, count(0) from t_migration_enterprise union all\n" +
                    "select 't_abnormal_enterprises' as tn, count(0) from t_abnormal_enterprises union all\n" +
                    "select 't_general_taxpayer' as tn, count(0) from t_general_taxpayer union all\n" +
                    "select 't_huge_tax_punishment' as tn, count(0) from t_huge_tax_punishment union all\n" +
                    "select 't_overduetaxs' as tn, count(0) from t_overduetaxs union all\n" +
                    "select 't_pay_taxes' as tn, count(0) from t_pay_taxes union all\n" +
                    "select 't_trademark_info' as tn, count(0) from t_trademark_info union all\n" +
                    "select 't_trademark_product' as tn, count(0) from t_trademark_product union all\n" +
                    "select 't_trademark_step' as tn, count(0) from t_trademark_step union all\n" +
                    "select 't_trademark_notice' as tn, count(0) from t_trademark_notice union all\n" +
                    "select 't_trademark_relation' as tn, count(0) from t_trademark_relation union all\n" +
                    "select 't_article_enterprise' as tn, count(0) from t_article_enterprise union all\n" +
                    "select 't_article_enterprise_tags' as tn, count(0) from t_article_enterprise_tags union all\n" +
                    "select 't_credit_no_index_full' as tn, count(0) from t_credit_no_index_full union all\n" +
                    "select 't_name_index_full' as tn, count(0) from t_name_index_full union all\n" +
                    "select 't_org_no_index_full' as tn, count(0) from t_org_no_index_full union all\n" +
                    "select 't_advanced_technology_service' as tn, count(0) from t_advanced_technology_service union all\n" +
                    "select 't_building' as tn, count(0) from t_building union all\n" +
                    "select 't_building_engineering' as tn, count(0) from t_building_engineering union all\n" +
                    "select 't_business_concession' as tn, count(0) from t_business_concession union all\n" +
                    "select 't_business_incubator' as tn, count(0) from t_business_incubator union all\n" +
                    "select 't_ccc' as tn, count(0) from t_ccc union all\n" +
                    "select 't_certificate_main' as tn, count(0) from t_certificate_main union all\n" +
                    "select 't_chinese_medicine_record' as tn, count(0) from t_chinese_medicine_record union all\n" +
                    "select 't_computer_project_manager' as tn, count(0) from t_computer_project_manager union all\n" +
                    "select 't_cosmetic_nonspecial' as tn, count(0) from t_cosmetic_nonspecial union all\n" +
                    "select 't_cosmetic_produce_license' as tn, count(0) from t_cosmetic_produce_license union all\n" +
                    "select 't_cosmetic_special' as tn, count(0) from t_cosmetic_special union all\n" +
                    "select 't_drug_business' as tn, count(0) from t_drug_business union all\n" +
                    "select 't_drug_permit_cn' as tn, count(0) from t_drug_permit_cn union all\n" +
                    "select 't_drug_produce' as tn, count(0) from t_drug_produce union all\n" +
                    "select 't_drug_record' as tn, count(0) from t_drug_record union all\n" +
                    "select 't_electronic_certification_agency' as tn, count(0) from t_electronic_certification_agency union all\n" +
                    "select 't_estate_qualification' as tn, count(0) from t_estate_qualification union all\n" +
                    "select 't_financial' as tn, count(0) from t_financial union all\n" +
                    "select 't_firefighting_system' as tn, count(0) from t_firefighting_system union all\n" +
                    "select 't_food_additives' as tn, count(0) from t_food_additives union all\n" +
                    "select 't_food_license' as tn, count(0) from t_food_license union all\n" +
                    "select 't_food_product_ent' as tn, count(0) from t_food_product_ent union all\n" +
                    "select 't_food_product_license' as tn, count(0) from t_food_product_license union all\n" +
                    "select 't_forestry_permit' as tn, count(0) from t_forestry_permit union all\n" +
                    "select 't_game' as tn, count(0) from t_game union all\n" +
                    "select 't_hazardous_chemicals_licence' as tn, count(0) from t_hazardous_chemicals_licence union all\n" +
                    "select 't_hightech_enterprises' as tn, count(0) from t_hightech_enterprises union all\n" +
                    "select 't_hygiene_product' as tn, count(0) from t_hygiene_product union all\n" +
                    "select 't_industry_produce' as tn, count(0) from t_industry_produce union all\n" +
                    "select 't_industry_volunteer' as tn, count(0) from t_industry_volunteer union all\n" +
                    "select 't_innovative_enterprises' as tn, count(0) from t_innovative_enterprises union all\n" +
                    "select 't_internet_drug' as tn, count(0) from t_internet_drug union all\n" +
                    "select 't_invisible_champion' as tn, count(0) from t_invisible_champion union all\n" +
                    "select 't_it_safe' as tn, count(0) from t_it_safe union all\n" +
                    "select 't_license_drug' as tn, count(0) from t_license_drug union all\n" +
                    "select 't_maker_space' as tn, count(0) from t_maker_space union all\n" +
                    "select 't_management_system' as tn, count(0) from t_management_system union all\n" +
                    "select 't_manufacturing_champion' as tn, count(0) from t_manufacturing_champion union all\n" +
                    "select 't_medical_advertising' as tn, count(0) from t_medical_advertising union all\n" +
                    "select 't_medical_instruments_business' as tn, count(0) from t_medical_instruments_business union all\n" +
                    "select 't_medical_instruments_import' as tn, count(0) from t_medical_instruments_import union all\n" +
                    "select 't_medical_instruments_produce' as tn, count(0) from t_medical_instruments_produce union all\n" +
                    "select 't_medical_instruments_registered' as tn, count(0) from t_medical_instruments_registered union all\n" +
                    "select 't_medicine_extract' as tn, count(0) from t_medicine_extract union all\n" +
                    "select 't_mining_exploration' as tn, count(0) from t_mining_exploration union all\n" +
                    "select 't_mining_license' as tn, count(0) from t_mining_license union all\n" +
                    "select 't_network_license' as tn, count(0) from t_network_license union all\n" +
                    "select 't_non_coal_mine' as tn, count(0) from t_non_coal_mine union all\n" +
                    "select 't_pesticide_produce' as tn, count(0) from t_pesticide_produce union all\n" +
                    "select 't_pesticide_produce_enterprise' as tn, count(0) from t_pesticide_produce_enterprise union all\n" +
                    "select 't_print' as tn, count(0) from t_print union all\n" +
                    "select 't_private_technology' as tn, count(0) from t_private_technology union all\n" +
                    "select 't_pro_person' as tn, count(0) from t_pro_person union all\n" +
                    "select 't_product_safe_license' as tn, count(0) from t_product_safe_license union all\n" +
                    "select 't_radio_customs_approve' as tn, count(0) from t_radio_customs_approve union all\n" +
                    "select 't_radio_model_approve' as tn, count(0) from t_radio_model_approve union all\n" +
                    "select 't_service' as tn, count(0) from t_service union all\n" +
                    "select 't_sewage_permit' as tn, count(0) from t_sewage_permit union all\n" +
                    "select 't_software' as tn, count(0) from t_software union all\n" +
                    "select 't_special_new_little_giant' as tn, count(0) from t_special_new_little_giant union all\n" +
                    "select 't_specialized_and_new_enterprise' as tn, count(0) from t_specialized_and_new_enterprise union all\n" +
                    "select 't_surveying' as tn, count(0) from t_surveying union all\n" +
                    "select 't_technological_alls' as tn, count(0) from t_technological_alls union all\n" +
                    "select 't_technological_innovation_demonstration' as tn, count(0) from t_technological_innovation_demonstration union all\n" +
                    "select 't_technology_center' as tn, count(0) from t_technology_center union all\n" +
                    "select 't_technology_giant' as tn, count(0) from t_technology_giant union all\n" +
                    "select 't_telecom_approval' as tn, count(0) from t_telecom_approval union all\n" +
                    "select 't_telecom_licence' as tn, count(0) from t_telecom_licence union all\n" +
                    "select 't_torch_plan' as tn, count(0) from t_torch_plan union all\n" +
                    "select 't_unicorn_enterprise' as tn, count(0) from t_unicorn_enterprise union all\n" +
                    "select 't_wildebeest_enterprise' as tn, count(0) from t_wildebeest_enterprise union all\n" +
                    "select 't_young_eagle_enterprise' as tn, count(0) from t_young_eagle_enterprise union all\n" +
                    "select 'szse_eid_pname_pid' as tn, count(0) from szse_eid_pname_pid union all\n" +
                    "select 'szse_pid_relation' as tn, count(0) from szse_pid_relation union all\n" +
                    "select 't_executedpersons' as tn, count(0) from t_executedpersons union all\n" +
                    "select 't_admin_division_code' as tn, count(0) from t_admin_division_code union all\n" +
                    "select 't_admin_division_code_change_history' as tn, count(0) from t_admin_division_code_change_history union all\n" +
                    "select 't_best_stock_info' as tn, count(0) from t_best_stock_info union all\n" +
                    "select 't_employees_alls' as tn, count(0) from t_employees_alls union all\n" +
                    "select 't_domains_alls' as tn, count(0) from t_domains_alls union all\n" +
                    "select 't_article_content' as tn, count(0) from t_article_content union all\n" +
                    "select 't_article_mining' as tn, count(0) from t_article_mining union all\n" +
                    "select 't_reg_no_index_full' as tn, count(0) from t_reg_no_index_full union all\n" +
                    "select 't_lawsuits_rolerelations' as tn, count(0) from t_lawsuits_rolerelations union all\n" +
                    "select 't_civilaviation_business_license' as tn, count(0) from t_civilaviation_business_license union all\n" +
                    "select 't_patents_relations_info' as tn, count(0) from t_patents_relations_info;\n" +
                    "\n";
            System.out.println(encodeMessage(str));
            System.out.println(encodeMessage(str).getBytes(StandardCharsets.UTF_8).length);
            System.out.println(str.getBytes(StandardCharsets.UTF_8).length);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

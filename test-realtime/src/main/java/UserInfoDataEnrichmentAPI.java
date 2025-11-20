import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class UserInfoDataEnrichmentAPI {

    private static final Random random = new Random();
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    @SneakyThrows
    public static void main(String[] args) {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().disableCheckpointing();
        env.setParallelism(1);

        Properties debeziumProperties = new Properties();
        debeziumProperties.put("connect.timeout.ms", 10000);
        debeziumProperties.put("request.timeout.ms", 15000);
        debeziumProperties.put("heartbeat.interval.ms", 10000);
        debeziumProperties.put("snapshot.mode", "initial");
        debeziumProperties.put("database.history.store.only.monitored.tables.ddl", "true");
        debeziumProperties.put("snapshot.locking.mode", "none");
        debeziumProperties.put("snapshot.fetch.size", 200);
        debeziumProperties.put("snapshot.isolation.mode", "snapshot");
        debeziumProperties.put("signal.data.collection", "public.user_info_base");

        DebeziumSourceFunction<String> postgresSource = PostgreSQLSource.<String>builder()
                .hostname("192.168.200.32")
                .port(5432)
                .database("spider_db")
                .schemaList("public")
                .tableList("public.user_info_base")
                .username("postgres")
                .password("Tzx123../")
                .decodingPluginName("pgoutput")
                .debeziumProperties(debeziumProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(postgresSource, "_transaction_log_source1");

        // 打印原始数据
        dataStreamSource.print("原始数据").setParallelism(1);

        // 数据增强处理
        SingleOutputStreamOperator<JSONObject> enrichedStream = dataStreamSource
                .map(JSON::parseObject)
                .uid("parseJson")
                .name("parseJson")
                .map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        System.out.println("=== 开始处理数据 ===");
                        String op = value.getString("op");
                        System.out.println("操作类型: " + op);

                        JSONObject after = value.getJSONObject("after");
                        if (after != null) {
                            System.out.println("原始数据: " + after.toJSONString());

                            // 获取现有字段值
                            String uname = after.getString("uname");
                            String phoneNum = after.getString("phone_num");
                            String address = after.getString("address");
                            String userId = after.getString("user_id");

                            // 1. 处理birthday字段
                            String currentBirthday = after.getString("birthday");
                            if (isEmptyField(currentBirthday)) {
                                String generatedBirthday = generateBirthday();
                                after.put("birthday", generatedBirthday);
                                System.out.println("✅ 生成生日: " + generatedBirthday);
                            } else {
                                System.out.println("✅ 已有生日: " + currentBirthday);
                            }

                            // 2. 处理gender字段
                            String currentGender = after.getString("gender");
                            if (isEmptyField(currentGender)) {
                                String generatedGender = generateGender(uname);
                                after.put("gender", generatedGender);
                                System.out.println("✅ 生成性别: " + generatedGender);
                            } else {
                                System.out.println("✅ 已有性别: " + currentGender);
                            }

                            // 3. 处理年龄字段 - 基于生日计算或生成
                            Integer currentAge = after.getInteger("年龄");
                            if (currentAge == null || currentAge == 0) {
                                String birthday = after.getString("birthday");
                                int generatedAge = calculateAgeFromBirthday(birthday);
                                after.put("年龄", generatedAge);
                                System.out.println("✅ 生成年龄: " + generatedAge);
                            } else {
                                System.out.println("✅ 已有年龄: " + currentAge);
                            }

                            // 4. 处理星座字段 - 基于生日计算或生成
                            String currentConstellation = after.getString("星座");
                            if (isEmptyField(currentConstellation) || "未知".equals(currentConstellation)) {
                                String birthday = after.getString("birthday");
                                String generatedConstellation = calculateConstellationFromBirthday(birthday);
                                after.put("星座", generatedConstellation);
                                System.out.println("✅ 生成星座: " + generatedConstellation);
                            } else {
                                System.out.println("✅ 已有星座: " + currentConstellation);
                            }

                            // 5. 处理金额字段 - 修复Base64解码问题
                            Object currentAmountObj = after.get("金额");
                            Double currentAmount = parseAmount(currentAmountObj);
                            if (currentAmount == null || currentAmount == 0.0) {
                                double generatedAmount = generateSmartAmount(address, uname);
                                after.put("金额", generatedAmount);
                                System.out.println("✅ 生成金额: " + generatedAmount);
                            } else {
                                // 如果金额字段有有效值，直接使用
                                after.put("金额", currentAmount);
                                System.out.println("✅ 已有金额: " + currentAmount);
                            }

                            // 添加处理标记和时间戳
                            after.put("data_enriched", "true");
                            after.put("enrich_timestamp", System.currentTimeMillis());
                            after.put("op_type", op);

                            System.out.println("🎯 增强后数据: " + after.toJSONString());
                            System.out.println("=== 数据处理完成 ===\n");
                        }
                        return value;
                    }
                })
                .uid("enrichData")
                .name("enrichData");

        // 打印处理后的数据
        enrichedStream.print("增强后数据").setParallelism(1);

        // 添加HBase Sink同步数据
        enrichedStream.addSink(new HBaseSink())
                .uid("hbaseSink")
                .name("hbaseSink");

        env.execute("UserInfoDataEnrichmentAPI");
    }

    // HBase Sink 类
    public static class HBaseSink extends RichSinkFunction<JSONObject> {
        private Connection connection;
        private Table table;
        private Admin admin;
        private static final int MAX_RETRIES = 3;
        private static final long RETRY_INTERVAL_MS = 3000;
        private boolean hbaseAvailable = false;

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("开始初始化 HBase 连接...");

            org.apache.hadoop.conf.Configuration hbaseConfig = HBaseConfiguration.create();
            hbaseConfig.set("hbase.zookeeper.quorum", "192.168.200.32");
            hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
            hbaseConfig.set("hbase.client.retries.number", "3");
            hbaseConfig.set("hbase.rpc.timeout", "10000");
            hbaseConfig.set("hbase.client.operation.timeout", "20000");
            hbaseConfig.set("hbase.client.scanner.timeout.period", "30000");
            hbaseConfig.set("zookeeper.session.timeout", "20000");

            // 添加重试机制
            int retryCount = 0;
            while (retryCount < MAX_RETRIES) {
                try {
                    System.out.println("尝试连接 HBase (尝试 " + (retryCount + 1) + "/" + MAX_RETRIES + ")...");
                    connection = ConnectionFactory.createConnection(hbaseConfig);
                    admin = connection.getAdmin();

                    // 检查表是否存在，如果不存在则创建
                    createTableIfNotExists();

                    hbaseAvailable = true;
                    System.out.println("✅ HBase 连接成功，表 'user_info_base' 已就绪");
                    break;

                } catch (Exception e) {
                    retryCount++;
                    System.err.println("连接 HBase 失败 (尝试 " + retryCount + "/" + MAX_RETRIES + "): " + e.getMessage());

                    // 关闭资源
                    closeResources();

                    if (retryCount >= MAX_RETRIES) {
                        System.err.println("⚠️ HBase 连接失败，将继续运行但不写入 HBase");
                        hbaseAvailable = false;
                        break;
                    }

                    // 等待后重试
                    System.out.println("等待 " + RETRY_INTERVAL_MS + "ms 后重试...");
                    TimeUnit.MILLISECONDS.sleep(RETRY_INTERVAL_MS);
                }
            }
        }

        // 创建表的辅助方法
        private void createTableIfNotExists() throws Exception {
            TableName tableName = TableName.valueOf("user_info_base");
            if (!admin.tableExists(tableName)) {
                System.out.println("🔄 表 'user_info_base' 不存在，开始创建...");

                // 创建表描述符
                TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName);

                // 创建列族描述符
                ColumnFamilyDescriptorBuilder cfBuilder = ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes("cf"))
                        .setMaxVersions(1);

                tableBuilder.setColumnFamily(cfBuilder.build());

                // 创建表
                admin.createTable(tableBuilder.build());
                System.out.println("✅ 成功创建 HBase 表 'user_info_base'");

                // 等待表创建完成
                Thread.sleep(2000);
            } else {
                System.out.println("✅ HBase 表 'user_info_base' 已存在");
            }

            // 获取表连接
            table = connection.getTable(tableName);
        }

        @Override
        public void invoke(JSONObject value, Context context) throws Exception {
            JSONObject after = value.getJSONObject("after");
            if (after == null) {
                System.out.println("CDC 事件中没有找到 'after' 数据");
                return;
            }

            if (!hbaseAvailable) {
                System.out.println("📋 HBase 不可用，跳过写入: " + after.toJSONString());
                return;
            }

            try {
                // 使用 id 作为 rowkey，如果id为空则使用user_id
                String rowKey = after.getString("id");
                if (rowKey == null || rowKey.isEmpty()) {
                    rowKey = after.getString("user_id");
                    if (rowKey == null || rowKey.isEmpty()) {
                        rowKey = "row_" + System.currentTimeMillis() + "_" + (int)(Math.random() * 1000);
                    }
                }

                Put put = new Put(Bytes.toBytes(rowKey));

                // 添加所有字段到 HBase
                addColumnToPut(put, "id", after.getString("id"));
                addColumnToPut(put, "user_id", after.getString("user_id"));
                addColumnToPut(put, "uname", after.getString("uname"));
                addColumnToPut(put, "phone_num", after.getString("phone_num"));
                addColumnToPut(put, "birthday", after.getString("birthday"));
                addColumnToPut(put, "gender", after.getString("gender"));
                addColumnToPut(put, "address", after.getString("address"));
                addColumnToPut(put, "ts", after.getString("ts"));
                addColumnToPut(put, "年龄", after.getString("年龄"));
                addColumnToPut(put, "星座", after.getString("星座"));
                addColumnToPut(put, "金额", after.getString("金额"));
                addColumnToPut(put, "data_enriched", after.getString("data_enriched"));
                addColumnToPut(put, "enrich_timestamp", after.getString("enrich_timestamp"));
                addColumnToPut(put, "op_type", after.getString("op_type"));

                // 检查 Put 是否包含列
                if (!put.isEmpty()) {
                    table.put(put);
                    System.out.println("✅ 成功写入 HBase, rowKey: " + rowKey + ", 姓名: " + after.getString("uname"));
                } else {
                    System.out.println("⚠️ 没有有效数据可写入 HBase, rowKey: " + rowKey);
                }

            } catch (Exception e) {
                System.err.println("❌ 写入 HBase 时出错: " + e.getMessage());
                // 输出数据到控制台作为备用
                System.out.println("📋 写入 HBase 失败，数据内容: " + after.toJSONString());
            }
        }

        // 辅助方法：添加列到Put对象
        private void addColumnToPut(Put put, String column, String value) {
            if (value != null && !value.isEmpty() && !"null".equals(value)) {
                put.addColumn(
                        Bytes.toBytes("cf"),
                        Bytes.toBytes(column),
                        Bytes.toBytes(value)
                );
            }
        }

        @Override
        public void close() throws Exception {
            closeResources();
            System.out.println("HBase Sink 已关闭");
        }

        private void closeResources() {
            if (table != null) {
                try {
                    table.close();
                    System.out.println("HBase 表连接已关闭");
                } catch (Exception e) {
                    System.err.println("关闭 HBase 表时出错: " + e.getMessage());
                }
                table = null;
            }
            if (admin != null) {
                try {
                    admin.close();
                    System.out.println("HBase Admin 连接已关闭");
                } catch (Exception e) {
                    System.err.println("关闭 HBase Admin 时出错: " + e.getMessage());
                }
                admin = null;
            }
            if (connection != null) {
                try {
                    connection.close();
                    System.out.println("HBase 连接已关闭");
                } catch (Exception e) {
                    System.err.println("关闭 HBase 连接时出错: " + e.getMessage());
                }
                connection = null;
            }
        }
    }

    // 原有的辅助方法保持不变
    // 解析金额字段，处理Base64编码和普通数值
    private static Double parseAmount(Object amountObj) {
        if (amountObj == null) {
            return null;
        }

        try {
            if (amountObj instanceof String) {
                String amountStr = (String) amountObj;

                // 检查是否是Base64编码
                if (isBase64(amountStr)) {
                    try {
                        // 解码Base64
                        byte[] decodedBytes = Base64.getDecoder().decode(amountStr);
                        // 将字节数组转换为字符串
                        String decodedStr = new String(decodedBytes);
                        return Double.parseDouble(decodedStr);
                    } catch (Exception e) {
                        System.out.println("⚠️ Base64解码失败: " + amountStr);
                        return null;
                    }
                } else {
                    // 直接解析为double
                    return Double.parseDouble(amountStr);
                }
            } else if (amountObj instanceof Number) {
                return ((Number) amountObj).doubleValue();
            } else {
                return null;
            }
        } catch (Exception e) {
            System.out.println("⚠️ 解析金额失败: " + amountObj);
            return null;
        }
    }

    // 检查字符串是否是Base64编码
    private static boolean isBase64(String str) {
        if (str == null || str.isEmpty()) {
            return false;
        }
        // Base64编码通常以=结尾，且只包含特定字符
        return str.matches("^[A-Za-z0-9+/]*={0,2}$") && str.length() % 4 == 0;
    }

    // 检查字段是否为空
    private static boolean isEmptyField(String field) {
        return field == null || field.isEmpty() || "null".equals(field) || "未知".equals(field);
    }

    // 生成随机生日 (1980-2005年之间，更合理的年龄分布)
    private static String generateBirthday() {
        try {
            int startYear = 1980;
            int endYear = 2005;
            int year = startYear + random.nextInt(endYear - startYear + 1);
            int month = random.nextInt(12) + 1;
            int day = random.nextInt(28) + 1; // 简单处理，避免2月问题

            return String.format("%d-%02d-%02d", year, month, day);
        } catch (Exception e) {
            return "1990-01-01"; // 默认生日
        }
    }

    // 根据姓名生成性别
    private static String generateGender(String uname) {
        if (uname == null || uname.length() < 2) {
            return random.nextBoolean() ? "男" : "女";
        }

        // 常见女性名字特征
        String[] femaleIndicators = {"丽", "婷", "娜", "芳", "娟", "敏", "静", "琳", "艳", "玲",
                "英", "慧", "秀", "美", "娇", "媛", "婉", "妮", "蕊", "雅",
                "女", "姐", "妹", "娘", "妃", "莹", "雪", "雨", "婷", "娜"};
        // 常见男性名字特征
        String[] maleIndicators = {"伟", "强", "勇", "军", "磊", "涛", "鹏", "杰", "健", "斌",
                "超", "明", "亮", "峰", "龙", "刚", "平", "辉", "建", "波",
                "男", "哥", "兄", "弟", "爷", "豪", "雄", "威", "武", "斌"};

        String namePart = uname.length() > 1 ? uname.substring(1) : uname;

        // 检查女性特征
        for (String indicator : femaleIndicators) {
            if (namePart.contains(indicator)) {
                return "女";
            }
        }

        // 检查男性特征
        for (String indicator : maleIndicators) {
            if (namePart.contains(indicator)) {
                return "男";
            }
        }

        // 如果包含"先生"则认为是男性
        if (uname.contains("先生")) {
            return "男";
        }

        // 如果包含"小姐"、"女士"则认为是女性
        if (uname.contains("小姐") || uname.contains("女士")) {
            return "女";
        }

        // 如果无法判断，基于姓名哈希值随机分配
        int hash = Math.abs(uname.hashCode());
        return (hash % 2 == 0) ? "男" : "女";
    }

    // 从生日计算年龄
    private static int calculateAgeFromBirthday(String birthday) {
        if (birthday == null || birthday.isEmpty()) {
            // 如果没有生日，生成一个合理的随机年龄
            return 20 + random.nextInt(30); // 20-50岁
        }

        try {
            Date birthDate = dateFormat.parse(birthday);
            Calendar now = Calendar.getInstance();
            Calendar birth = Calendar.getInstance();
            birth.setTime(birthDate);

            int age = now.get(Calendar.YEAR) - birth.get(Calendar.YEAR);
            if (now.get(Calendar.MONTH) < birth.get(Calendar.MONTH) ||
                    (now.get(Calendar.MONTH) == birth.get(Calendar.MONTH) &&
                            now.get(Calendar.DAY_OF_MONTH) < birth.get(Calendar.DAY_OF_MONTH))) {
                age--;
            }
            return Math.max(18, Math.min(age, 65)); // 确保年龄在18-65岁之间
        } catch (Exception e) {
            return 25 + random.nextInt(20); // 25-45岁默认范围
        }
    }

    // 从生日计算星座
    private static String calculateConstellationFromBirthday(String birthday) {
        if (birthday == null || birthday.isEmpty()) {
            return generateRandomConstellation();
        }

        try {
            Date birthDate = dateFormat.parse(birthday);
            Calendar cal = Calendar.getInstance();
            cal.setTime(birthDate);

            int month = cal.get(Calendar.MONTH) + 1;
            int day = cal.get(Calendar.DAY_OF_MONTH);

            if ((month == 3 && day >= 21) || (month == 4 && day <= 19)) return "白羊座";
            else if ((month == 4 && day >= 20) || (month == 5 && day <= 20)) return "金牛座";
            else if ((month == 5 && day >= 21) || (month == 6 && day <= 21)) return "双子座";
            else if ((month == 6 && day >= 22) || (month == 7 && day <= 22)) return "巨蟹座";
            else if ((month == 7 && day >= 23) || (month == 8 && day <= 22)) return "狮子座";
            else if ((month == 8 && day >= 23) || (month == 9 && day <= 22)) return "处女座";
            else if ((month == 9 && day >= 23) || (month == 10 && day <= 23)) return "天秤座";
            else if ((month == 10 && day >= 24) || (month == 11 && day <= 22)) return "天蝎座";
            else if ((month == 11 && day >= 23) || (month == 12 && day <= 21)) return "射手座";
            else if ((month == 12 && day >= 22) || (month == 1 && day <= 19)) return "摩羯座";
            else if ((month == 1 && day >= 20) || (month == 2 && day <= 18)) return "水瓶座";
            else return "双鱼座";
        } catch (Exception e) {
            return generateRandomConstellation();
        }
    }

    // 生成随机星座
    private static String generateRandomConstellation() {
        String[] constellations = {
                "白羊座", "金牛座", "双子座", "巨蟹座", "狮子座", "处女座",
                "天秤座", "天蝎座", "射手座", "摩羯座", "水瓶座", "双鱼座"
        };
        return constellations[random.nextInt(constellations.length)];
    }

    // 生成智能金额（基于地址和姓名的哈希值）
    private static double generateSmartAmount(String address, String uname) {
        int addressHash = address != null ? Math.abs(address.hashCode()) : random.nextInt();
        int nameHash = uname != null ? Math.abs(uname.hashCode()) : random.nextInt();
        int combinedHash = (addressHash + nameHash) % 100;

        // 金额分布逻辑 - 更合理的消费金额分布
        if (combinedHash < 50) {
            // 50% 小额: 50-500元
            return 50 + (combinedHash * 9);
        } else if (combinedHash < 85) {
            // 35% 中额: 501-2000元
            return 501 + ((combinedHash - 50) * 42.86);
        } else {
            // 15% 大额: 2001-5000元
            return 2001 + ((combinedHash - 85) * 200);
        }
    }
}
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseTableViewer {

    private Connection connection;
    private Admin admin;

    // 初始化连接
    public void init() throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "192.168.200.32");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        
        connection = ConnectionFactory.createConnection(config);
        admin = connection.getAdmin();
        System.out.println("✅ HBase 连接成功");
    }

    // 关闭连接
    public void close() throws IOException {
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
        System.out.println("🔌 HBase 连接已关闭");
    }

    // 1. 查看所有表
    public void listAllTables() throws IOException {
        System.out.println("\n=== HBase 所有表 ===");
        TableName[] tableNames = admin.listTableNames();
        for (TableName tableName : tableNames) {
            System.out.println("表名: " + tableName.getNameAsString());
        }
        System.out.println("总计: " + tableNames.length + " 个表");
    }

    // 2. 查看表结构
    public void describeTable(String tableName) throws IOException {
        System.out.println("\n=== 表结构: " + tableName + " ===");
        TableName tn = TableName.valueOf(tableName);
        
        if (!admin.tableExists(tn)) {
            System.out.println("❌ 表不存在: " + tableName);
            return;
        }

        TableDescriptor tableDescriptor = admin.getDescriptor(tn);
        System.out.println("表名: " + tableDescriptor.getTableName().getNameAsString());
        
        // 列族信息
        System.out.println("列族信息:");
        for (ColumnFamilyDescriptor family : tableDescriptor.getColumnFamilies()) {
            System.out.println("  - 列族: " + family.getNameAsString());
            System.out.println("    最大版本: " + family.getMaxVersions());
            System.out.println("    压缩: " + family.getCompressionType());
            System.out.println("    TTL: " + family.getTimeToLive());
        }
    }

    // 3. 查看表数据 - 扫描全表
    public void scanTable(String tableName, int limit) throws IOException {
        System.out.println("\n=== 表数据扫描: " + tableName + " (限制 " + limit + " 行) ===");
        
        TableName tn = TableName.valueOf(tableName);
        if (!admin.tableExists(tn)) {
            System.out.println("❌ 表不存在: " + tableName);
            return;
        }

        try (Table table = connection.getTable(tn)) {
            Scan scan = new Scan();
            scan.setLimit(limit); // 限制返回行数
            
            ResultScanner scanner = table.getScanner(scan);
            int count = 0;
            
            for (Result result : scanner) {
                System.out.println("\n--- RowKey: " + Bytes.toString(result.getRow()) + " ---");
                
                // 遍历所有列族和列
                for (Cell cell : result.listCells()) {
                    String family = Bytes.toString(cell.getFamilyArray(), 
                                                 cell.getFamilyOffset(), 
                                                 cell.getFamilyLength());
                    String qualifier = Bytes.toString(cell.getQualifierArray(), 
                                                    cell.getQualifierOffset(), 
                                                    cell.getQualifierLength());
                    String value = Bytes.toString(cell.getValueArray(), 
                                                cell.getValueOffset(), 
                                                cell.getValueLength());
                    long timestamp = cell.getTimestamp();
                    
                    System.out.println("  " + family + ":" + qualifier + " = " + value + 
                                     " (timestamp: " + timestamp + ")");
                }
                
                count++;
                if (count >= limit) break;
            }
            scanner.close();
            System.out.println("总计扫描: " + count + " 行");
        }
    }

    // 4. 根据 RowKey 查询特定行
    public void getByRowKey(String tableName, String rowKey) throws IOException {
        System.out.println("\n=== 查询特定行: " + tableName + " RowKey: " + rowKey + " ===");
        
        TableName tn = TableName.valueOf(tableName);
        if (!admin.tableExists(tn)) {
            System.out.println("❌ 表不存在: " + tableName);
            return;
        }

        try (Table table = connection.getTable(tn)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            
            if (result.isEmpty()) {
                System.out.println("❌ 未找到 RowKey: " + rowKey);
                return;
            }
            
            System.out.println("RowKey: " + Bytes.toString(result.getRow()));
            
            for (Cell cell : result.listCells()) {
                String family = Bytes.toString(cell.getFamilyArray(), 
                                             cell.getFamilyOffset(), 
                                             cell.getFamilyLength());
                String qualifier = Bytes.toString(cell.getQualifierArray(), 
                                                cell.getQualifierOffset(), 
                                                cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), 
                                            cell.getValueOffset(), 
                                            cell.getValueLength());
                long timestamp = cell.getTimestamp();
                
                System.out.println("  " + family + ":" + qualifier + " = " + value + 
                                 " (timestamp: " + timestamp + ")");
            }
        }
    }

    // 5. 查看表区域信息
    public void getTableRegions(String tableName) throws IOException {
        System.out.println("\n=== 表区域信息: " + tableName + " ===");
        
        TableName tn = TableName.valueOf(tableName);
        if (!admin.tableExists(tn)) {
            System.out.println("❌ 表不存在: " + tableName);
            return;
        }

        List<RegionInfo> regions = admin.getRegions(tn);
        System.out.println("区域数量: " + regions.size());
        
        for (RegionInfo region : regions) {
            System.out.println("区域: " + region.getRegionNameAsString());
            System.out.println("  起始Key: " + 
                (region.getStartKey().length == 0 ? "(开始)" : Bytes.toString(region.getStartKey())));
            System.out.println("  结束Key: " + 
                (region.getEndKey().length == 0 ? "(结束)" : Bytes.toString(region.getEndKey())));
        }
    }

    // 6. 检查表是否存在
    public boolean tableExists(String tableName) throws IOException {
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        System.out.println("表 " + tableName + " 存在: " + exists);
        return exists;
    }

    // 7. 获取表的行数估算
    public void getRowCount(String tableName) throws IOException {
        System.out.println("\n=== 表行数估算: " + tableName + " ===");
        
        TableName tn = TableName.valueOf(tableName);
        if (!admin.tableExists(tn)) {
            System.out.println("❌ 表不存在: " + tableName);
            return;
        }

        try (Table table = connection.getTable(tn)) {
            Scan scan = new Scan();
            scan.setCaching(1000); // 提高扫描性能
            
            ResultScanner scanner = table.getScanner(scan);
            int count = 0;
            for (Result result : scanner) {
                count++;
            }
            scanner.close();
            System.out.println("估算行数: " + count);
        }
    }

    // 主方法 - 测试使用
    public static void main(String[] args) {
        HBaseTableViewer viewer = new HBaseTableViewer();
        
        try {
            // 初始化连接
            viewer.init();
            
            // 1. 查看所有表
            viewer.listAllTables();
            
            // 2. 检查特定表是否存在
            String targetTable = "user_info_base";
            if (viewer.tableExists(targetTable)) {
                // 3. 查看表结构
                viewer.describeTable(targetTable);
                
                // 4. 查看表区域信息
                viewer.getTableRegions(targetTable);
                
                // 5. 扫描表数据（前10行）
                viewer.scanTable(targetTable, 10);
                
                // 6. 估算行数
                viewer.getRowCount(targetTable);
                
                // 7. 查询特定行（如果有数据的话）
                // viewer.getByRowKey(targetTable, "1"); // 替换为实际的 rowKey
            } else {
                System.out.println("❌ 目标表 " + targetTable + " 不存在");
                System.out.println("💡 请先创建表: create 'user_info_base', 'cf'");
            }
            
        } catch (Exception e) {
            System.err.println("❌ 执行出错: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                viewer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
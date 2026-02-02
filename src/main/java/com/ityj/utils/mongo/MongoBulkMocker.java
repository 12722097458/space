package com.ityj.utils.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class MongoBulkMocker {

    // 配置参数
//    private static final String CONNECTION_STRING = "mongodb://mongo:mongo@localhost:27017/?authSource=admin";
    private static final String CONNECTION_STRING = "mongodb://panda:panda@192.168.1.5:27017/?authSource=admin";
    private static final String DATABASE_NAME = "tqx";                 // 数据库名
    private static final String COLLECTION_NAME = "workflow_competition_llm_sessions";
    private static final String COMPETITION_NAME = "2026_global_traders_competition";

    public static void main(String[] args) {
        // 1. 获取 Agent 映射关系
        Map<Integer, String> agentMap = getAgentMapping();

        // 2. 创建 MongoDB 客户端
        try (MongoClient mongoClient = MongoClients.create(CONNECTION_STRING)) {
            MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
            MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

            List<WriteModel<Document>> bulkOperations = new ArrayList<>();
            ThreadLocalRandom random = ThreadLocalRandom.current();

            System.out.println("正在构造批量数据...");

            // 3. 构造数据
            for (Map.Entry<Integer, String> entry : agentMap.entrySet()) {
                Integer accountId = entry.getKey();
                String agentId = entry.getValue();

                for (int i = 0; i < 100; i++) {
                    Document doc = createMockDocument(accountId, agentId, random);
                    // 使用 InsertOneModel 封装每一个插入操作
                    bulkOperations.add(new InsertOneModel<>(doc));
                }
            }

            // 4. 执行批量写入
            if (!bulkOperations.isEmpty()) {
                System.out.println("开始执行 Bulk Write (共 " + bulkOperations.size() + " 条)...");
                collection.bulkWrite(bulkOperations);
                System.out.println("数据写入成功！");
            }

        } catch (Exception e) {
            System.err.println("发生错误: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static Document createMockDocument(Integer accountId, String agentId, ThreadLocalRandom random) {
        // 随机生成过去一年内的时间戳
        Instant now = Instant.now();
        Instant randomPast = now.minus(random.nextInt(1, 365), ChronoUnit.DAYS)
                                .minus(random.nextInt(0, 86400), ChronoUnit.SECONDS);

        String[] analysisTemplates = {
            "当前 1810.HK 在 EMA(20) 附近震荡，RSI 指标显示动能不足。",
            "0700.HK 出现超卖信号，MACD 绿柱开始缩短，短期内可能反弹。",
            "市场整体流动性一般，ATR 波动率维持平稳，建议观察支撑位。",
            "技术指标显示下行趋势放缓，但尚未出现明确的反转信号。"
        };

        return new Document()
                .append("_id", new ObjectId()) // 自动生成 MongoDB ObjectId
                .append("user_id", 666)
                .append("competition_name", COMPETITION_NAME)
                .append("workflow_run_id", new ObjectId().toHexString())
                .append("account_id", accountId)
                .append("agent_id", agentId)
                .append("broker_user_id", String.valueOf(2005000 + random.nextInt(1000)))
                .append("model_name", "g4o")
                .append("analysis", analysisTemplates[random.nextInt(analysisTemplates.length)])
                .append("decisions", Collections.emptyList())
                .append("llm_duration", 5.0 + (random.nextDouble() * 10))
                .append("create_at", Date.from(randomPast)); // 驱动会自动处理为 BSON Date 类型
    }

    private static Map<Integer, String> getAgentMapping() {
        Map<Integer, String> map = new LinkedHashMap<>();
        // 图片 1
        map.put(100129, "agent_001"); map.put(100130, "agent_002");
        map.put(100131, "agent_003"); map.put(100132, "agent_004");
        map.put(100133, "agent_005"); map.put(100134, "agent_006");
        map.put(100135, "agent_007"); map.put(100136, "agent_008");
        map.put(100137, "agent_009"); map.put(100138, "agent_010");
        // 图片 2
        map.put(100139, "agent_011"); map.put(100140, "agent_012");
        map.put(100141, "agent_013"); map.put(100142, "agent_014");
        map.put(100143, "agent_015"); map.put(100144, "agent_016");
        map.put(100145, "agent_017"); map.put(100146, "agent_018");
        map.put(100147, "agent_019"); map.put(100148, "agent_020");
        return map;
    }
}
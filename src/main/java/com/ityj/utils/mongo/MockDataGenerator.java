package com.ityj.utils.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MockDataGenerator {

    private static final String CONNECTION_STRING = "mongodb://panda:panda@192.168.1.5:27017/?authSource=admin";
//    private static final String CONNECTION_STRING = "mongodb://mongo:mongo@localhost:27017/?authSource=admin";
    private static final String DATABASE_NAME = "tqx";
    private static final String COLLECTION_NAME = "workflow_competition_llm_sessions";
    private static final String COMPETITION_NAME = "2026_global_traders_competition";

    private static final List<String> AGENTS = new ArrayList<>();
    private static final List<String> ANALYSIS_TEMPLATES = Arrays.asList(
            "市场数据整体平淡，多数股票价格极低且RSI超买，账户资金有限，无交易机会。",
            "所有可交易股票RSI均为100，MACD无趋势，成交量稀少，建议观望。",
            "当前市场流动性极低，价格波动微小，账户余额仅139.69 HKD，不具备开仓条件。",
            "LLM服务暂时不可用，无法获取决策。",
            "监测到2608.HK在4小时级别有微弱放量，但整体波动仍不足以交易。",
            "所有标的RSI持续100，处于极端超买，但价格无变化，可能为数据异常。",
            "账户可用资金39.70 HKD，不足以购买最小交易单位，无操作。",
            "市场状态与前一周期一致，未出现新的交易信号。",
            "部分股票如2048.HK出现微量成交，但不足以形成趋势，继续观望。",
            "系统运行正常，但无交易决策生成。",
            "技术指标显示无明显趋势，建议保持空仓。",
            "由于缺乏波动性，今日不进行任何交易。",
            "资金余额过低，无法执行交易。",
            "监控到价格异常波动，但成交量不足，暂不介入。",
            "当前市场环境不适合交易，建议等待更明确信号。"
    );

    static {
        for (int i = 1; i <= 20; i++) {
            AGENTS.add(String.format("agent_%03d", i));
        }
    }

    public static void main(String[] args) {
        try (MongoClient mongoClient = MongoClients.create(CONNECTION_STRING)) {
            MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
            MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

            Random random = new Random();
            long currentTime = System.currentTimeMillis();
            long oneDayMillis = TimeUnit.DAYS.toMillis(1);
            long sevenDaysAgo = currentTime - 7 * oneDayMillis;

            int totalRecords = 0;
            List<Document> batch = new ArrayList<>();

            for (String agentId : AGENTS) {
                for (int j = 0; j < 200; j++) {
                    // 生成随机时间戳（过去7天内）
                    long randomTime = sevenDaysAgo + (long) (random.nextDouble() * 7 * oneDayMillis);
                    // 随机 drive_mode
                    String driveMode = random.nextBoolean() ? "chat" : "schedule";
                    // 随机 analysis
                    String analysis = ANALYSIS_TEMPLATES.get(random.nextInt(ANALYSIS_TEMPLATES.size()));

                    Document doc = new Document("_id", new ObjectId())
                            .append("competition_name", COMPETITION_NAME)
                            .append("agent_id", agentId)
                            .append("create_at", randomTime)
                            .append("analysis", analysis)
                            .append("drive_mode", driveMode);

                    batch.add(doc);
                    totalRecords++;

                    // 每500条批量插入一次
                    if (batch.size() >= 500) {
                        collection.insertMany(batch);
                        System.out.println("已插入 " + totalRecords + " 条记录...");
                        batch.clear();
                    }
                }
            }

            // 插入剩余记录
            if (!batch.isEmpty()) {
                collection.insertMany(batch);
                System.out.println("最终插入 " + totalRecords + " 条记录。");
            }

            System.out.println("完成！共插入 " + totalRecords + " 条记录。");
        }
    }
}
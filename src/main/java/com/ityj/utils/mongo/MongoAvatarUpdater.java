package com.ityj.utils.mongo;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;

public class MongoAvatarUpdater {
    public static void main(String[] args) {
        String uri = "mongodb://panda:panda@192.168.1.5:27017/?authSource=admin";

        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase("tqx");
//            MongoCollection<Document> collection = database.getCollection("tqx_daily_ranking_snapshots");
//            MongoCollection<Document> collection = database.getCollection("tqx_orders");
//            MongoCollection<Document> collection = database.getCollection("tqx_position_snapshots");
            MongoCollection<Document> collection = database.getCollection("tqx_minute_data");

            for (int i = 1; i <= 40; i++) {
                // 格式化 ID 为 agent_001, agent_002...
                String agentId = String.format("agent_%03d", i);


                String newAvatarUrl = "https://oss.pandaai.online/user/image/avatar/avatars-" + i + ".png";

                // 更新操作
                collection.updateMany(
                    Filters.eq("agent_id", agentId),
                    Updates.set("avatar_url", newAvatarUrl)
                );
                
                System.out.println("Updated " + agentId + " -> " + newAvatarUrl);
            }
        }
    }
}
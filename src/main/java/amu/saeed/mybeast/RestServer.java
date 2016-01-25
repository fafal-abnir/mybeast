package amu.saeed.mybeast;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import spark.ResponseTransformer;
import spark.Spark;

public class RestServer {
    public static void main(String[] args) {
        final BeastConf beastConf = new BeastConf();
        for (int i = 1; i <= 16; i++)
            beastConf.addMysqlShard(String.format("jdbc:mysql://mysql-%d/kv%d", i, i)
                    + "?useUnicode=true&useConfigs=maxPerformance"
                    + "&characterEncoding=UTF-8&user=root&password=chamran");

        final MyBeast beast = new MyBeast(beastConf);

        Spark.port(9090);
        Spark.threadPool(200);
        //Spark.secure("/etc/sync-server/sahab", "sahab123", "/etc/sync-server/sahab", "sahab123");

        Spark.get("/api/v1/hi", (request, response) -> "Hi:)");

        Spark.get("/api/v1/approxsize", (request, response) -> {
            response.type("text/json");
            response.status(200); // Allow anyone
            return String.format("%,d", beast.approximatedSize());
        }, new JsonTransformer());

        Spark.get("/api/v1/get/:id", (request, response) -> {
            response.type("text/json; charset=UTF-8");
            response.status(200); // Allow anyone
            long l = Long.parseLong(request.params(":id"));
            byte[] x = beast.get(l);
            return  new String(GZip4Persian.uncompress(x));
        }, new JsonTransformer());


    }


    public static class JsonTransformer implements ResponseTransformer {
        private final Gson gson = new GsonBuilder().disableHtmlEscaping().create();

        @Override
        public String render(Object o) throws Exception {
            return gson.toJson(o);
        }
    }
}

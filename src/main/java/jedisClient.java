import redis.clients.jedis.Jedis;

public class jedisClient {
    public static void main(String[] args) {
        //创建Jedis
        //host： redis数据库的ip地址
        //port： redis数据库的端口号

        Jedis jedis = new Jedis("10.211.55.101", 6379);

        //通过jedis存储值
        jedis.set("s4","jedis test");

        //通过jedis获取值
        String s4 = jedis.get("s4");

        System.out.println(s4);

        //关闭jedis
        jedis.close();

    }
}

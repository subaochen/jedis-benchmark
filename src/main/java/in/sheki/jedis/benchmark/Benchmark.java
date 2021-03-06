package in.sheki.jedis.benchmark;

import com.beust.jcommander.JCommander;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisDataException;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author abhishekk
 */
public class Benchmark
{
    private final int noOps_;
    private final LinkedBlockingQueue<Long> setRunTimes = new LinkedBlockingQueue<Long>();
    private PausableThreadPoolExecutor executor;
    private final JedisPool pool;
    private static JedisCluster jc;
    private final String data;
    private final CountDownLatch shutDownLatch;
    private long totalNanoRunTime;
    private static boolean clustered = false;
    private static boolean proxyed = false;
    final Logger logger = LoggerFactory.getLogger(Benchmark.class);
    private static long no = 1;


    public Benchmark(final int noOps, final int noThreads, final int noJedisConn, final String host, final int port, int dataSize, int clustered, int proxy)
    {
        int finalPort = port;


        // 如果使用proxy方式访问redis集群的话
        if(proxy != 0) {
            finalPort = proxy;
        }

        this.noOps_ = noOps;
        this.executor = new PausableThreadPoolExecutor(noThreads, noThreads*5, 5, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        final GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        //poolConfig.setMaxIdle(100);
        poolConfig.setMaxTotal(noJedisConn);
        this.pool = new JedisPool(poolConfig, host, finalPort);
        this.data = RandomStringUtils.random(dataSize);
        shutDownLatch = new CountDownLatch(noOps);

        if(clustered != 0) {
            Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
            jedisClusterNodes.add(new HostAndPort(host, port));
            jc = new JedisCluster(jedisClusterNodes,5000,poolConfig);
        }


    }

    class SetTask implements Runnable
    {
        private CountDownLatch latch_;

        SetTask(CountDownLatch latch)
        {
            this.latch_ = latch;
        }

        public void run()
        {
            long startTime = System.nanoTime();
            String key = RandomStringUtils.random(15);
            if(clustered && !proxyed) {
                // jedis-benchmark在测试redis cluster经常无故挂起？
                //logger.info("before send to redis cluster,key=" + key );
                jc.set(key, data);
                //logger.info("after sent to redis cluster,key=" + key );
            } else {
                Jedis jedis = pool.getResource();
                jedis.set(key, data);
                pool.returnResource(jedis);
            }
            setRunTimes.offer(System.nanoTime() - startTime);
            latch_.countDown();
        }
    }

    public void performBenchmark() throws InterruptedException
    {
        executor.pause();
        for (int i = 0; i < noOps_; i++)
        {
            executor.submit(new SetTask(shutDownLatch));
        }
        long startTime = System.nanoTime();
        executor.resume();
        executor.shutdown();
        shutDownLatch.await();
        totalNanoRunTime = System.nanoTime() - startTime;
    }

    public void printStats()
    {
        List<Long> points = new ArrayList<Long>();
        setRunTimes.drainTo(points);
        Collections.sort(points);
        long sum = 0;
        for (Long l : points)
        {
            sum += l;
        }
        System.out.println("no:" + no++ + ",time:" + new Date());
        System.out.println("Data size :" + data.length());
        System.out.println("Threads : " + executor.getMaximumPoolSize());
        System.out.println("Time Test Ran for (ms) : " + TimeUnit.NANOSECONDS.toMillis(totalNanoRunTime));
        System.out.println("Average : " + TimeUnit.NANOSECONDS.toMillis(sum / points.size()));
        System.out.println("50 % <=" + TimeUnit.NANOSECONDS.toMillis(points.get((points.size() / 2) - 1)));
        System.out.println("90 % <=" + TimeUnit.NANOSECONDS.toMillis(points.get((points.size() * 90 / 100) - 1)));
        System.out.println("95 % <=" + TimeUnit.NANOSECONDS.toMillis(points.get((points.size() * 95 / 100) - 1)));
        System.out.println("99 % <=" + TimeUnit.NANOSECONDS.toMillis(points.get((points.size() * 99 / 100) - 1)));
        System.out.println("99.9 % <=" + TimeUnit.NANOSECONDS.toMillis(points.get((points.size() * 999 / 1000) - 1)));
        System.out.println("100 % <=" + TimeUnit.NANOSECONDS.toMillis(points.get(points.size() - 1)));
        System.out.println((noOps_ * 1000 / TimeUnit.NANOSECONDS.toMillis(totalNanoRunTime)) + " Operations per second");
    }

    public static void main(String[] args) throws InterruptedException
    {

        CommandLineArgs cla = new CommandLineArgs();
        new JCommander(cla, args);
        if(cla.loop != 0) {
            while (true) {
                doBenchmark(cla);
            }
        } else {
            doBenchmark(cla);
        }
    }

    private static void doBenchmark(CommandLineArgs cla) throws InterruptedException {
        Benchmark benchmark = new Benchmark(cla.noOps, cla.noThreads, cla.noConnections, cla.host, cla.port, cla.dataSize, cla.clustered, cla.proxy);
        if (cla.proxy != 0) proxyed = true;
        if (cla.clustered != 0) {
            clustered = true;
            flushAllNodes();
        }

        benchmark.performBenchmark();
        benchmark.printStats();

        // 必须关闭已经不用的jc对象，否则
        if(clustered) {
            try {
                jc.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void flushAllNodes() {
        Map<String,JedisPool> nodes = jc.getClusterNodes();
        for(String hnp:nodes.keySet()) {
            Jedis node = createNode(hnp);
            // will report such message on slave node:(error) READONLY You can't write against a read only slave.
            // we can just ignore it safely
            try {
                node.flushAll();
            }catch(JedisDataException e) {}
        }
    }

    private static Jedis createNode(String hnp) {
        // TODO 如何正确获取cluster host？
        String host = hnp.split(":")[0];
        String port = hnp.split(":")[1];
        return new Jedis(host, Integer.parseInt(port));
    }


}

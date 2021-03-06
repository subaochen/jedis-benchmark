package in.sheki.jedis.benchmark;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;

import java.util.List;

/**
 * @author abhishekk
 */
public class CommandLineArgs
{
    @Parameter
    public List<String> parameters = Lists.newArrayList();

    @Parameter(names = "-n", description = "No. of operations")
    public Integer noOps = 100000;

    @Parameter(names = "-t", description = "No. of threads")
    public Integer noThreads = 1;

    @Parameter(names = "-c", description = "No of connections")
    public Integer noConnections = 1;

    @Parameter(names = "-h", description = "Host")
    public String host = "localhost";

    @Parameter(names = "-p", description = "port")
    public Integer port = 6379;
    @Parameter(names = "-s", description = "data size in bytes")
    public Integer dataSize = 100;

    @Parameter(names = "-g", description = "cluster enabled")
    public Integer clustered = 0;

    @Parameter(names="-x", description = "proxy port")
    public Integer proxy = 0;

    @Parameter(names="-l", description = "infinite loop mode if not zero")
    public Integer loop = 0;
}

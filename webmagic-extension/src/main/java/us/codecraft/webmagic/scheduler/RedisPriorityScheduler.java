package us.codecraft.webmagic.scheduler;

import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Task;

import java.util.Set;

/**
 * the redis scheduler with priority
 * @author sai
 * Created by sai on 16-5-27.
 */
public class RedisPriorityScheduler extends RedisScheduler
{

    private static final String ZSET_PREFIX = "zset_";

    private static final String QUEUE_PREFIX = "queue_";

    private static final String NO_PRIORITY_SUFFIX = "_zore";

    private static final String PLUS_PRIORITY_SUFFIX    = "_plus";

    private static final String MINUS_PRIORITY_SUFFIX   = "_minus";

    public RedisPriorityScheduler(String host) {
        super(host);
    }

    public RedisPriorityScheduler(JedisPool pool) {
        super(pool);
    }

    @Override
    protected void pushWhenNoDuplicate(Request request, Task task)
    {
        Jedis jedis = pool.getResource();
        try
        {
            if(request.getPriority() > 0)
                jedis.zadd(getZsetPlusPriorityKey(task), request.getPriority(), request.getUrl());
            else if(request.getPriority() < 0)
                jedis.zadd(getZsetMinusPriorityKey(task), request.getPriority(), request.getUrl());
            else
                jedis.rpush(getQueueNoPriorityKey(task), request.getUrl());

            if (checkForAdditionalInfo(request)) {
                setExtrasInItem(jedis, request, task);
            }
        }
        finally
        {
            jedis.close();
        }
    }

    @Override
    public synchronized Request poll(Task task)
    {
        Jedis jedis = pool.getResource();
        try
        {
            String url = getRequest(jedis, task);
            if(StringUtils.isBlank(url))
                return null;
            return getExtrasInItem(jedis, url, task);
        }
        finally
        {
            jedis.close();
        }
    }

    private String getRequest(Jedis jedis, Task task)
    {
        String url;
        Set<String> urls = jedis.zrevrange(getZsetPlusPriorityKey(task), 0, 0);
        if(urls.isEmpty())
        {
            url = jedis.lpop(getQueueNoPriorityKey(task));
            if(StringUtils.isBlank(url))
            {
                urls = jedis.zrevrange(getZsetMinusPriorityKey(task), 0, 0);
                if(!urls.isEmpty())
                {
                    url = urls.toArray(new String[0])[0];
                    jedis.zrem(getZsetMinusPriorityKey(task), url);
                }
            }
        }
        else
        {
            url = urls.toArray(new String[0])[0];
            jedis.zrem(getZsetPlusPriorityKey(task), url);
        }
        return url;
    }

    @Override
    public void resetDuplicateCheck(Task task)
    {
        Jedis jedis = pool.getResource();
        try
        {
            jedis.del(getSetKey(task));
        }
        finally
        {
            jedis.close();
        }
    }

    private String getZsetPlusPriorityKey(Task task)
    {
        return getPrefix() + ZSET_PREFIX + task.getUUID() + PLUS_PRIORITY_SUFFIX;
    }

    private String getQueueNoPriorityKey(Task task)
    {
        return getPrefix() + QUEUE_PREFIX + task.getUUID() + NO_PRIORITY_SUFFIX;
    }

    private String getZsetMinusPriorityKey(Task task)
    {
        return getPrefix() + ZSET_PREFIX + task.getUUID() + MINUS_PRIORITY_SUFFIX;
    }

    @Override
    public int getLeftRequestsCount(Task task) {
        Jedis jedis = pool.getResource();
        try {
            Long size = jedis.llen(getQueueNoPriorityKey(task));
            size += jedis.zcard(getZsetPlusPriorityKey(task));
            size += jedis.zcard(getZsetMinusPriorityKey(task));
            return size.intValue();
        } finally {
            jedis.close();
        }
    }
}

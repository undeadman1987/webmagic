package us.codecraft.webmagic.scheduler;

import com.alibaba.fastjson.JSON;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.scheduler.component.DuplicateRemover;

/**
 * Use Redis as url scheduler for distributed crawlers.<br>
 *
 * @author code4crafter@gmail.com <br>
 * @since 0.2.0
 */
public class RedisScheduler extends DuplicateRemovedScheduler implements MonitorableScheduler, DuplicateRemover {

    protected JedisPool pool;

    private static final String QUEUE_PREFIX = "queue_";

    private static final String SET_PREFIX = "set_";

    private static final String ITEM_PREFIX = "item_";

    private String prefix = "";

    public RedisScheduler(String host) {
        this(new JedisPool(new JedisPoolConfig(), host));
    }

    public RedisScheduler(JedisPool pool) {
        this.pool = pool;
        setDuplicateRemover(this);
    }

    public String getPrefix() {
        return prefix;
    }

    public RedisScheduler setPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

    @Override
    public void resetDuplicateCheck(Task task) {
        Jedis jedis = pool.getResource();
        try {
            jedis.del(getSetKey(task));
        } finally {
            jedis.close();
        }
    }

    @Override
    public boolean isDuplicate(Request request, Task task) {
        Jedis jedis = pool.getResource();
        try {
            return jedis.sadd(getSetKey(task), request.getUrl()) == 0;
        } finally {
            jedis.close();
        }

    }

    @Override
    protected void pushWhenNoDuplicate(Request request, Task task) {
        Jedis jedis = pool.getResource();
        try {
            jedis.rpush(getQueueKey(task), request.getUrl());
            if (checkForAdditionalInfo(request)) {
                setExtrasInItem(jedis, request, task);
            }
        } finally {
            jedis.close();
        }
    }

    protected boolean checkForAdditionalInfo(Request request) {
        if (request == null) {
            return false;
        }

        if (!request.getHeaders().isEmpty() || !request.getCookies().isEmpty()) {
            return true;
        }

        if (StringUtils.isNotBlank(request.getCharset()) || StringUtils.isNotBlank(request.getMethod())) {
            return true;
        }

        if (request.isBinaryContent() || request.getRequestBody() != null) {
            return true;
        }

        if (request.getExtras() != null && !request.getExtras().isEmpty()) {
            return true;
        }
        if (request.getCurrentDepth() != 0) {
            return true;
        }
        if (request.getPriority() != 0L) {
            return true;
        }

        return false;
    }

    @Override
    public synchronized Request poll(Task task) {
        Jedis jedis = pool.getResource();
        try {
            String url = jedis.lpop(getQueueKey(task));
            if (StringUtils.isBlank(url)) {
                return null;
            }
            return getExtrasInItem(jedis, url, task);
        } finally {
            jedis.close();
        }
    }

    protected String getSetKey(Task task) {
        return prefix + SET_PREFIX + task.getUUID();
    }

    protected String getQueueKey(Task task) {
        return prefix + QUEUE_PREFIX + task.getUUID();
    }

    protected String getItemKey(Task task) {
        return prefix + ITEM_PREFIX + task.getUUID();
    }

    protected void setExtrasInItem(Jedis jedis, Request request, Task task) {
        String field = DigestUtils.shaHex(request.getUrl());
        String value = JSON.toJSONString(request);
        jedis.hset(getItemKey(task), field, value);
    }

    protected Request getExtrasInItem(Jedis jedis, String url, Task task) {
        String key = getItemKey(task);
        String field = DigestUtils.shaHex(url);
        byte[] bytes = jedis.hget(key.getBytes(), field.getBytes());
        if (bytes != null) {
            jedis.hdel(key.getBytes(), field.getBytes());
            return JSON.parseObject(new String(bytes), Request.class);
        }
        return new Request(url);
    }

    @Override
    public int getLeftRequestsCount(Task task) {
        Jedis jedis = pool.getResource();
        try {
            Long size = jedis.llen(getQueueKey(task));
            return size.intValue();
        } finally {
            jedis.close();
        }
    }

    @Override
    public int getTotalRequestsCount(Task task) {
        Jedis jedis = pool.getResource();
        try {
            Long size = jedis.scard(getSetKey(task));
            return size.intValue();
        } finally {
            jedis.close();
        }
    }
}

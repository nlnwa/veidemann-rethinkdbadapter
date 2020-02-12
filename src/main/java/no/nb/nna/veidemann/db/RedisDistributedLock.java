/*
 * Copyright 2019 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package no.nb.nna.veidemann.db;

import com.rethinkdb.RethinkDB;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.DistributedLock;
import no.nb.nna.veidemann.commons.util.ApiTools;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.ByteArrayCodec;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class RedisDistributedLock implements DistributedLock {
    private static final Logger LOG = LoggerFactory.getLogger(RedisDistributedLock.class);
    static final RethinkDB r = RethinkDB.r;

    private final RethinkDbConnection conn;
    private final Key key;
    private String dbKey;
    private final int expireSeconds = 3600;
    private final String instanceId;
    private final static RedissonClient redisson;
    RLock lock;

    static {
        String redisHost = System.getProperty("lock.redis.host", "");
        int redisPort = Integer.parseInt(System.getProperty("lock.redis.port", "0"));
        if (redisPort > 0) {
            Config config = new Config();
            config.useSingleServer().setAddress("redis://" + redisHost + ":" + redisPort);
            config.setCodec(ByteArrayCodec.INSTANCE);
            redisson = Redisson.create(config);
        } else {
            redisson = null;
        }
    }

    public RedisDistributedLock(RethinkDbConnection conn, Key key) {
        Objects.requireNonNull(conn, "Database connection cannot be null");
        Objects.requireNonNull(key, "Lock key cannot be null");
        this.conn = conn;
        this.key = key;
        this.dbKey = key.getDomain();
        if ((key.getDomain().getBytes().length + key.getKey().getBytes().length) > 120) {
            this.dbKey += ApiTools.createSha1Digest(key.getKey());
        } else {
            this.dbKey += key.getKey();
        }
        this.instanceId = UUID.randomUUID().toString();

        lock = redisson.getLock(key.toString());
    }

    public void lock() {
        lock.lock(expireSeconds, TimeUnit.SECONDS);
    }

    public void lockInterruptibly() throws InterruptedException {
        lock.lockInterruptibly(expireSeconds, TimeUnit.SECONDS);
    }

    public boolean tryLock() {
        try {
            return lock.tryLock(0, expireSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean tryLock(long timeout, TimeUnit unit) throws DbQueryException, DbConnectionException, InterruptedException {
        long expire = unit.convert(expireSeconds, TimeUnit.SECONDS);
        return lock.tryLock(timeout, expire, unit);
    }

    public void unlock() throws DbQueryException, DbConnectionException {
        if (lock.isHeldByCurrentThread()) {
            try {
                lock.unlock();
            } catch (Exception e) {
                LOG.warn("Could not release lock", e);
            }
        }
    }

    public String getInstanceId() {
        return instanceId;
    }

    public Key getKey() {
        return key;
    }
}

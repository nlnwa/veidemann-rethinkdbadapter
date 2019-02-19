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
import com.rethinkdb.net.Cursor;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.db.DistributedLock;
import no.nb.nna.veidemann.commons.db.DistributedLock.Key;
import no.nb.nna.veidemann.commons.db.FutureOptional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class RethinkDbCrawlQueueFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(RethinkDbDistributedLock.class);
    private static final long RESCHEDULE_DELAY = 1000;
    private final long CHG_EXPIRATION_SECONDS = 1800;
    private final int LOCK_EXPIRATION_SECONDS = 1800;
    private static final RethinkDB r = RethinkDB.r;

    private final RethinkDbConnection conn;
    private final RethinkDbCrawlQueueAdapter crawlQueueAdapter;
    private final LinkedBlockingQueue<Map<String, Object>> chgQueue;

    public RethinkDbCrawlQueueFetcher(RethinkDbConnection conn, RethinkDbCrawlQueueAdapter crawlQueueAdapter) {
        this.conn = conn;
        this.crawlQueueAdapter = crawlQueueAdapter;
        chgQueue = new LinkedBlockingQueue<>();
    }

    private void fillChgQueue() {
        try (Cursor<Map<String, Object>> response = conn.exec("db-borrowFirstReadyCrawlHostGroup",
                r.table(Tables.CRAWL_HOST_GROUP.name)
                        .orderBy().optArg("index", "nextFetchTime")
                        .between(r.minval(), r.now()).optArg("right_bound", "closed").limit(100)
        )) {
            chgQueue.addAll(response.toList());
        } catch (Exception e) {
            LOG.error("Failed listing CrawlHostGroups", e);
        }
    }

    private synchronized Map<String, Object> getNextCrawlHostGroup() throws InterruptedException {
        Map<String, Object> chg = chgQueue.poll();
        while (chg == null) {
            fillChgQueue();
            chg = chgQueue.poll();
            if (chg == null) {
                Thread.sleep(RESCHEDULE_DELAY);
            }
        }
        return chg;
    }

    public CrawlableUri getNextToFetch() throws InterruptedException {
        try {
            while (true) {
                if (DbService.getInstance().getDbAdapter().getDesiredPausedState()) {
                    Thread.sleep(RESCHEDULE_DELAY);
                    continue;
                }

                Map<String, Object> chgDoc = getNextCrawlHostGroup();
                List chgId = (List) chgDoc.get("id");

                DistributedLock lock = conn.createDistributedLock(createKey(chgId), LOCK_EXPIRATION_SECONDS);
                if (lock.tryLock(3, TimeUnit.SECONDS)) {
                    try {
                        Map<String, Object> borrowResponse = conn.exec("db-borrowFirstReadyCrawlHostGroup",
                                r.table(Tables.CRAWL_HOST_GROUP.name).optArg("read_mode", "majority")
                                        .get(chgId)
                                        .replace(d ->
                                                r.branch(
                                                        // CrawlHostGroup doesn't exist, return null
                                                        d.eq(null),
                                                        null,

                                                        // Busy is false or expired. This is the one we want, ensure busy is false and return it
                                                        d.g("busy").eq(false).or(d.g("expires").lt(r.now())),
                                                        d.merge(r.hashMap("busy", false).with("expires", r.now())),

                                                        // The CrawlHostGroup is busy, return it unchanged
                                                        d
                                                ))
                                        .optArg("return_changes", true)
                                        .optArg("durability", "hard")
                        );

                        if (borrowResponse != null) {
                            long replaced = (long) borrowResponse.get("replaced");
                            if (replaced == 1L) {
                                Map<String, Object> newDoc = ((List<Map<String, Map>>) borrowResponse.get("changes")).get(0).get("new_val");
                                long queueCount = crawlHostGroupQueueCount(chgId);

                                if (queueCount == 0L) {
                                    // No more URIs in queue for this CrawlHostGroup, delete it
                                    conn.exec("db-deleteCrawlHostGroup",
                                            r.table(Tables.CRAWL_HOST_GROUP.name).optArg("read_mode", "majority")
                                                    .get(chgId)
                                                    .delete()
                                                    .optArg("return_changes", false)
                                                    .optArg("durability", "hard")
                                    );
                                } else {
                                    // Found available CrawlHostGroup, set it as busy
                                    newDoc.put("busy", true);
                                    newDoc.put("expires", r.now().add(CHG_EXPIRATION_SECONDS));
                                    CrawlHostGroup chg = buildCrawlHostGroup(newDoc, queueCount);

                                    conn.exec("db-saveCrawlHostGroup",
                                            r.table(Tables.CRAWL_HOST_GROUP.name).optArg("read_mode", "majority")
                                                    .get(chgId)
                                                    .replace(newDoc)
                                                    .optArg("return_changes", false)
                                                    .optArg("durability", "hard")
                                    );

                                    // try to find URI for CrawlHostGroup
                                    FutureOptional<QueuedUri> foqu = getNextQueuedUriToFetch(chg);

                                    long sleep;
                                    if (foqu.isPresent()) {
                                        // A fetchabel URI was found, return it
                                        LOG.debug("Found Queued URI: {}, crawlHostGroup: {}, sequence: {}",
                                                foqu.get().getUri(), foqu.get().getCrawlHostGroupId(), foqu.get().getSequence());
                                        return new CrawlableUri(chg, foqu.get());
                                    } else if (foqu.isMaybeInFuture()) {
                                        // A URI was found, but isn't fetchable yet. Wait for it
                                        LOG.debug("Queued URI might be available at: {}", foqu.getWhen());
                                        sleep = foqu.getDelayMs();
                                    } else {
                                        // No URI found for this CrawlHostGroup. Wait for RESCHEDULE_DELAY and try again.
                                        LOG.trace("No Queued URI found waiting {}ms before retry", RESCHEDULE_DELAY);
                                        sleep = RESCHEDULE_DELAY;
                                    }
                                    crawlQueueAdapter.releaseCrawlHostGroup(chg, sleep);
                                }
                            }
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Failed borrowing CrawlHostGroup", e);
        }

        return null;
    }

    public FutureOptional<QueuedUri> getNextQueuedUriToFetch(CrawlHostGroup crawlHostGroup) throws DbException {
        List fromKey = r.array(
                crawlHostGroup.getId(),
                crawlHostGroup.getPolitenessId(),
                r.minval(),
                r.minval()
        );

        List toKey = r.array(
                crawlHostGroup.getId(),
                crawlHostGroup.getPolitenessId(),
                r.maxval(),
                r.now()
        );

        List<Map<String, Object>> eids = conn.exec("db-getNextQueuedUriToFetch",
                r.table(Tables.URI_QUEUE.name).optArg("read_mode", "majority")
                        .orderBy().optArg("index", "crawlHostGroupKey_sequence_earliestFetch")
                        .between(fromKey, toKey)
                        .pluck("executionId", "priorityWeight")
                        .distinct());

        List<String> randomExecutionIds = getWeightedRandomExecutionIds(eids);
        for (String randomExecutionId : randomExecutionIds) {
            try (Cursor<Map<String, Object>> cursor = conn.exec("db-getNextQueuedUriToFetch",
                    r.table(Tables.URI_QUEUE.name).optArg("read_mode", "majority")
                            .orderBy().optArg("index", "crawlHostGroupKey_sequence_earliestFetch")
                            .between(fromKey, toKey)
                            .filter(r.hashMap("executionId", randomExecutionId))
                            .limit(1))) {

                if (cursor.hasNext()) {
                    QueuedUri qUri = ProtoUtils.rethinkToProto(cursor.next(), QueuedUri.class);
                    // URI is ready to be processed, remove it from queue
                    conn.exec("db-setTimeoutForQueuedUri",
                            r.table(Tables.URI_QUEUE.name)
                                    .get(qUri.getId())
                                    .update(r.hashMap("earliestFetchTimeStamp", r.now().add(1800)))
                    );

                    return FutureOptional.of(qUri);
                }
            }
        }
        return FutureOptional.empty();
    }

    private CrawlHostGroup buildCrawlHostGroup(Map<String, Object> resultDoc, long queueCount) throws DbException {
        CrawlHostGroup.Builder chg = CrawlHostGroup.newBuilder()
                .setId(((List<String>) resultDoc.get("id")).get(0))
                .setPolitenessId(((List<String>) resultDoc.get("id")).get(1))
                .setNextFetchTime(ProtoUtils.odtToTs((OffsetDateTime) resultDoc.get("nextFetchTime")))
                .setBusy((boolean) resultDoc.get("busy"))
                .setQueuedUriCount(queueCount);

        return chg.build();
    }

    private long crawlHostGroupQueueCount(List<String> crawlHostGroupId) throws DbQueryException, DbConnectionException {
        String chgId = crawlHostGroupId.get(0);
        String politenessId = crawlHostGroupId.get(1);

        List fromKey = r.array(
                chgId,
                politenessId,
                r.minval(),
                r.minval()
        );

        List toKey = r.array(
                chgId,
                politenessId,
                r.maxval(),
                r.maxval()
        );

        return conn.exec("", r.table(Tables.URI_QUEUE.name)
                .optArg("read_mode", "majority")
                .between(fromKey, toKey).optArg("index", "crawlHostGroupKey_sequence_earliestFetch")
                .count());
    }

    private Key createKey(String crawlHostGroupId, String politenessId) {
        return new Key("chg", crawlHostGroupId + ":" + politenessId);
    }

    private Key createKey(List<String> crawlHostGroupId) {
        return new Key("chg", crawlHostGroupId.get(0) + ":" + crawlHostGroupId.get(1));
    }

    public static class CrawlableUri {
        final CrawlHostGroup crawlHostGroup;
        final QueuedUri uri;

        public CrawlableUri(CrawlHostGroup crawlHostGroup, QueuedUri uri) {
            this.crawlHostGroup = crawlHostGroup;
            this.uri = uri;
        }

        public CrawlHostGroup getCrawlHostGroup() {
            return crawlHostGroup;
        }

        public QueuedUri getUri() {
            return uri;
        }
    }

    static class IdWeight {
        final String id;
        final double weight;

        public IdWeight(String id, double weight) {
            this.id = id;
            this.weight = weight;
        }
    }

    static List<String> getWeightedRandomExecutionIds(List<Map<String, Object>> executionIds) {
        List<IdWeight> idWeights = new ArrayList<>();
        executionIds.forEach(e -> idWeights.add(new IdWeight(
                (String) e.get("executionId"),
                ((Number) e.get("priorityWeight")).doubleValue())));
        List<String> res = new ArrayList<>();
        for (int i = 0; i < executionIds.size(); i++) {
            IdWeight idWeight = getWeightedRandomExecutionId(idWeights);
            res.add(0, idWeight.id);
            idWeights.remove(idWeight);
        }
        return res;
    }

    static IdWeight getWeightedRandomExecutionId(List<IdWeight> executionIds) {
        // Compute the total weight of all items together
        double totalWeight = 0.0d;
        IdWeight[] items = new IdWeight[executionIds.size()];
        for (int i = 0; i < executionIds.size(); ++i) {
            items[i] = executionIds.get(i);
            totalWeight += items[i].weight;
        }
        // Choose a random item
        int randomIndex = -1;
        double random = Math.random() * totalWeight;
        for (int i = 0; i < executionIds.size(); ++i) {
            random -= items[i].weight;
            if (random <= 0.0d) {
                randomIndex = i;
                break;
            }
        }
        return items[randomIndex];
    }
}

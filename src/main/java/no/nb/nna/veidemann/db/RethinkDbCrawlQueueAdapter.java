package no.nb.nna.veidemann.db;

import com.google.protobuf.Timestamp;
import com.rethinkdb.RethinkDB;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.db.CrawlQueueAdapter;
import no.nb.nna.veidemann.commons.db.CrawlQueueFetcher;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DistributedLock;
import no.nb.nna.veidemann.commons.db.DistributedLock.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class RethinkDbCrawlQueueAdapter implements CrawlQueueAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(RethinkDbCrawlQueueAdapter.class);
    static final RethinkDB r = RethinkDB.r;

    private final RethinkDbConnection conn;

    private final long expirationSeconds = 3600;
    private final int lockExpirationSeconds = 3600;

    public RethinkDbCrawlQueueAdapter(RethinkDbConnection conn) {
        this.conn = conn;
    }

    static Key createKey(String crawlHostGroupId, String politenessId) {
        return new Key("chg", crawlHostGroupId + ":" + politenessId);
    }

    static Key createKey(List<String> crawlHostGroupId) {
        return createKey(crawlHostGroupId.get(0), crawlHostGroupId.get(1));
    }

    @Override
    public QueuedUri addToCrawlHostGroup(QueuedUri qUri) throws DbException {
        String crawlHostGroupId = qUri.getCrawlHostGroupId();
        String politenessId = qUri.getPolitenessRef().getId();
        Objects.requireNonNull(crawlHostGroupId, "CrawlHostGroupId cannot be null");
        Objects.requireNonNull(politenessId, "PolitenessId cannot be null");
        if (qUri.getSequence() <= 0L) {
            throw new IllegalArgumentException("Sequence must be a positive number");
        }

        DistributedLock lock = conn.createDistributedLock(createKey(crawlHostGroupId, politenessId), lockExpirationSeconds);
        lock.lock();
        try {
            if (!qUri.hasEarliestFetchTimeStamp()) {
                qUri = qUri.toBuilder().setEarliestFetchTimeStamp(ProtoUtils.getNowTs()).build();
            }
            OffsetDateTime earliestFetchTimeStamp = ProtoUtils.tsToOdt(qUri.getEarliestFetchTimeStamp());

            List key = r.array(crawlHostGroupId, politenessId);
            conn.exec("db-addToCrawlHostGroup",
                    r.table(Tables.CRAWL_HOST_GROUP.name).optArg("read_mode", "majority")
                            .insert(r.hashMap("id", key)
                                    .with("nextFetchTime", earliestFetchTimeStamp)
                                    .with("busy", false))
                            .optArg("return_changes", false)
                            .optArg("conflict", (id, old_doc, new_doc) -> old_doc)
                            .optArg("durability", "hard")
            );

            Map rMap = ProtoUtils.protoToRethink(qUri);
            return conn.executeInsert("db-saveQueuedUri",
                    r.table(Tables.URI_QUEUE.name)
                            .insert(rMap)
                            .optArg("conflict", "replace"),
                    QueuedUri.class
            );
        } finally {
            lock.unlock();
        }
    }

    @Override
    public CrawlQueueFetcher getCrawlQueueFetcher() {
        return new RethinkDbCrawlQueueFetcher(conn, this);
    }

    @Override
    public void releaseCrawlHostGroup(CrawlHostGroup crawlHostGroup, long nextFetchDelayMs) throws DbException {
        List key = r.array(crawlHostGroup.getId(), crawlHostGroup.getPolitenessId());
        double nextFetchDelayS = nextFetchDelayMs / 1000.0;

        DistributedLock lock = conn.createDistributedLock(createKey(crawlHostGroup.getId(), crawlHostGroup.getPolitenessId()), lockExpirationSeconds);
        lock.lock();
        try {
            Map<String, Object> response = conn.exec("db-releaseCrawlHostGroup",
                    r.table(Tables.CRAWL_HOST_GROUP.name).optArg("read_mode", "majority")
                            .get(key)
                            .replace(d ->
                                    r.branch(
                                            // CrawlHostGroup doesn't exist, return null
                                            d.eq(null),
                                            null,

                                            // CrawlHostGroup is busy, release it
                                            d.g("busy").eq(true),
                                            d.merge(r.hashMap("busy", false)
                                                    .with("expires", null)
                                                    .with("nextFetchTime", r.now().add(nextFetchDelayS))),

                                            // The CrawlHostGroup is already released probably because it was expired.
                                            d
                                    ))
                            .optArg("return_changes", false)
                            .optArg("durability", "hard")
            );

            long replaced = (long) response.get("replaced");
            if (replaced != 1L) {
                LOG.warn("Could not release crawl host group");
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long deleteQueuedUrisForExecution(String executionId) throws DbException {
        long deleted = 0;

        List<Map<String, Object>> chgKeys = conn.exec(
                r.table(Tables.URI_QUEUE.name).optArg("read_mode", "majority")
                        .getAll(executionId).optArg("index", "executionId")
                        .pluck("crawlHostGroupId", "politenessRef")
                        .distinct()
        );


        for (Map<String, Object> group : chgKeys) {
            String crawlHostGroupId = (String) group.get("crawlHostGroupId");
            String politenessId = ((Map<String, String>) group.get("politenessRef")).get("id");
            List<String> startKey = r.array(crawlHostGroupId, politenessId, r.minval(), r.minval());
            List<String> endKey = r.array(crawlHostGroupId, politenessId, r.maxval(), r.maxval());

            DistributedLock lock = conn.createDistributedLock(createKey(crawlHostGroupId, politenessId), lockExpirationSeconds);
            lock.lock();
            try {
                long deleteResponse = conn.exec("db-deleteQueuedUrisForExecution",
                        r.table(Tables.URI_QUEUE.name)
                                .between(startKey, endKey)
                                .optArg("index", "crawlHostGroupKey_sequence_earliestFetch")
                                .filter(row -> row.g("executionId").eq(executionId))
                                .delete()
                                .g("deleted")
                );
                deleted += deleteResponse;
            } finally {
                lock.unlock();
            }
        }

        return deleted;
    }

    @Override
    public long queuedUriCount(String executionId) throws DbException {
        return conn.exec("db-queuedUriCount",
                r.table(Tables.URI_QUEUE.name).optArg("read_mode", "majority")
                        .getAll(executionId).optArg("index", "executionId")
                        .count()
        );
    }

    @Override
    public boolean uriNotIncludedInQueue(QueuedUri qu, Timestamp since) throws DbException {
        return conn.exec("db-uriNotIncludedInQueue",
                r.table(Tables.CRAWL_LOG.name)
                        .between(
                                r.array(qu.getSurt(), ProtoUtils.tsToOdt(since)),
                                r.array(qu.getSurt(), r.maxval()))
                        .optArg("index", "surt_time").filter(row -> row.g("statusCode").lt(500)).limit(1)
                        .union(
                                r.table(Tables.URI_QUEUE.name).getAll(qu.getSurt())
                                        .optArg("index", "surt")
                                        .limit(1)
                        ).isEmpty());
    }

}

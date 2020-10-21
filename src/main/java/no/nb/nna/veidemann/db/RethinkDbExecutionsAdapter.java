/*
 * Copyright 2018 National Library of Norway.
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

import com.google.protobuf.Message;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Cursor;
import no.nb.nna.veidemann.api.commons.v1.ExtractedText;
import no.nb.nna.veidemann.api.contentwriter.v1.CrawledContent;
import no.nb.nna.veidemann.api.contentwriter.v1.StorageRef;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.CrawlLog;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.PageLog;
import no.nb.nna.veidemann.api.report.v1.CrawlExecutionsListRequest;
import no.nb.nna.veidemann.api.report.v1.CrawlLogListRequest;
import no.nb.nna.veidemann.api.report.v1.JobExecutionsListRequest;
import no.nb.nna.veidemann.api.report.v1.ListCountResponse;
import no.nb.nna.veidemann.api.report.v1.PageLogListRequest;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.ExecutionsAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class RethinkDbExecutionsAdapter implements ExecutionsAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(RethinkDbExecutionsAdapter.class);

    static final RethinkDB r = RethinkDB.r;

    private final RethinkDbConnection conn;

    public RethinkDbExecutionsAdapter(RethinkDbConnection conn) {
        this.conn = conn;
    }

    @Override
    public JobExecutionStatus createJobExecutionStatus(String jobId) throws DbException {
        Map rMap = ProtoUtils.protoToRethink(JobExecutionStatus.newBuilder()
                .setJobId(jobId)
                .setStartTime(ProtoUtils.getNowTs())
                .setState(JobExecutionStatus.State.RUNNING));

        return conn.executeInsert("db-saveJobExecutionStatus",
                r.table(Tables.JOB_EXECUTIONS.name)
                        .insert(rMap)
                        .optArg("conflict", (id, oldDoc, newDoc) -> r.branch(
                                oldDoc.hasFields("endTime"),
                                newDoc.merge(
                                        r.hashMap("state", oldDoc.g("state")).with("endTime", oldDoc.g("endTime"))
                                ),
                                newDoc
                        )),
                JobExecutionStatus.class);
    }

    @Override
    public JobExecutionStatus getJobExecutionStatus(String jobExecutionId) throws DbException {
        JobExecutionStatus jes = ProtoUtils.rethinkToProto(conn.exec("db-getJobExecutionStatus",
                r.table(Tables.JOB_EXECUTIONS.name)
                        .get(jobExecutionId)
        ), JobExecutionStatus.class);

        if (!jes.hasEndTime()) {
            LOG.debug("JobExecution '{}' is still running. Aggregating stats snapshot", jobExecutionId);
            Map sums = summarizeJobExecutionStats(jes.getId());

            JobExecutionStatus.Builder jesBuilder = jes.toBuilder()
                    .setDocumentsCrawled((long) sums.get("documentsCrawled"))
                    .setDocumentsDenied((long) sums.get("documentsDenied"))
                    .setDocumentsFailed((long) sums.get("documentsFailed"))
                    .setDocumentsOutOfScope((long) sums.get("documentsOutOfScope"))
                    .setDocumentsRetried((long) sums.get("documentsRetried"))
                    .setUrisCrawled((long) sums.get("urisCrawled"))
                    .setBytesCrawled((long) sums.get("bytesCrawled"));

            for (CrawlExecutionStatus.State s : CrawlExecutionStatus.State.values()) {
                jesBuilder.putExecutionsState(s.name(), ((Long) sums.get(s.name())).intValue());
            }

            jes = jesBuilder.build();
        }

        return jes;
    }

    @Override
    public ChangeFeed<JobExecutionStatus> listJobExecutionStatus(JobExecutionsListRequest jobExecutionsListRequest) throws DbException {
        ListJobExecutionQueryBuilder q = new ListJobExecutionQueryBuilder(jobExecutionsListRequest);

        Cursor<Map<String, Object>> res = conn.exec("db-listJobExecutions", q.getListQuery());

        return new ChangeFeedBase<JobExecutionStatus>(res) {
            @Override
            @SuppressWarnings("unchecked")
            protected Function<Map<String, Object>, JobExecutionStatus> mapper() {
                return co -> {
                    // In case of a change feed, the real object is stored in new_val
                    // If new_val is empty, the object is deleted. We skip those.
                    if (co.containsKey("new_val")) {
                        co = (Map) co.get("new_val");
                        if (co == null) {
                            return null;
                        }
                    }
                    JobExecutionStatus jes = ProtoUtils.rethinkToProto(co, JobExecutionStatus.class);
                    if (!jes.hasEndTime()) {
                        LOG.debug("JobExecution '{}' is still running. Aggregating stats snapshot", jes.getId());
                        try {
                            Map sums = summarizeJobExecutionStats(jes.getId());

                            JobExecutionStatus.Builder jesBuilder = jes.toBuilder()
                                    .setDocumentsCrawled((long) sums.get("documentsCrawled"))
                                    .setDocumentsDenied((long) sums.get("documentsDenied"))
                                    .setDocumentsFailed((long) sums.get("documentsFailed"))
                                    .setDocumentsOutOfScope((long) sums.get("documentsOutOfScope"))
                                    .setDocumentsRetried((long) sums.get("documentsRetried"))
                                    .setUrisCrawled((long) sums.get("urisCrawled"))
                                    .setBytesCrawled((long) sums.get("bytesCrawled"));

                            for (CrawlExecutionStatus.State s : CrawlExecutionStatus.State.values()) {
                                jesBuilder.putExecutionsState(s.name(), ((Long) sums.get(s.name())).intValue());
                            }

                            jes = jesBuilder.build();
                        } catch (DbException ex) {
                            // DB error; skip calculating runtime sum
                        }
                    }
                    return jes;
                };
            }
        };
    }

    @Override
    public JobExecutionStatus setJobExecutionStateAborted(String jobExecutionId) throws DbException {
        JobExecutionStatus result = conn.executeUpdate("db-setJobExecutionStateAborted",
                r.table(Tables.JOB_EXECUTIONS.name)
                        .get(jobExecutionId)
                        .update(
                                doc -> r.branch(
                                        doc.hasFields("endTime"),
                                        r.hashMap(),
                                        r.hashMap("state", JobExecutionStatus.State.ABORTED_MANUAL.name()))
                        ),
                JobExecutionStatus.class);

        // Set all Crawl Executions which are part of this Job Execution to aborted
        Cursor<Map<String, String>> res = conn.exec("db-setJobExecutionStateAborted",
                r.table(Tables.EXECUTIONS.name)
                        .between(
                                r.array(jobExecutionId, r.minval()),
                                r.array(jobExecutionId, r.maxval()))
                        .optArg("index", "jobExecutionId_seedId")
                        .pluck("id")
        );

        res.iterator().forEachRemaining(e -> {
            try {
                setCrawlExecutionStateAborted(e.get("id"), CrawlExecutionStatus.State.ABORTED_MANUAL);
            } catch (DbException ex) {
                LOG.error("Error while aborting Crawl Execution", ex);
            }
        });

        return result;
    }

    @Override
    public void setJobExecutionStateAbortedTimeout(String jobExecutionId) throws DbException {
        // Set all Crawl Executions which are part of this Job Execution to aborted
        Cursor<Map<String, String>> res = conn.exec("db-setJobExecutionStateAborted",
                r.table(Tables.EXECUTIONS.name)
                        .between(
                                r.array(jobExecutionId, r.minval()),
                                r.array(jobExecutionId, r.maxval()))
                        .optArg("index", "jobExecutionId_seedId")
                        .pluck("id")
        );

        res.iterator().forEachRemaining(e -> {
            try {
                setCrawlExecutionStateAborted(e.get("id"), CrawlExecutionStatus.State.ABORTED_TIMEOUT);
            } catch (DbException ex) {
                LOG.error("Error while setting ABORTED_TIMEOUT for Crawl Execution {}", e.get("id"), ex);
            }
        });
    }

    @Override
    public CrawlExecutionStatus getCrawlExecutionStatus(String crawlExecutionId) throws DbException {
        Map<String, Object> response = conn.exec("db-getExecutionStatus",
                r.table(Tables.EXECUTIONS.name)
                        .get(crawlExecutionId)
        );

        return ProtoUtils.rethinkToProto(response, CrawlExecutionStatus.class);
    }

    @Override
    public ChangeFeed<CrawlExecutionStatus> listCrawlExecutionStatus(CrawlExecutionsListRequest crawlExecutionsListRequest) throws DbException {
        ListCrawlExecutionQueryBuilder q = new ListCrawlExecutionQueryBuilder(crawlExecutionsListRequest);

        Cursor<Map<String, Object>> res = conn.exec("db-listCrawlExecutions", q.getListQuery());

        return new ChangeFeedBase<CrawlExecutionStatus>(res) {
            @Override
            @SuppressWarnings("unchecked")
            protected Function<Map<String, Object>, CrawlExecutionStatus> mapper() {
                return co -> {
                    // In case of a change feed, the real object is stored in new_val
                    // If new_val is empty, the object is deleted. We skip those.
                    if (co.containsKey("new_val")) {
                        co = (Map) co.get("new_val");
                        if (co == null) {
                            return null;
                        }
                    }
                    CrawlExecutionStatus res = ProtoUtils.rethinkToProto(co, CrawlExecutionStatus.class);
                    return res;
                };
            }
        };
    }

    @Override
    public CrawlExecutionStatus setCrawlExecutionStateAborted(String crawlExecutionId, CrawlExecutionStatus.State state)
            throws DbException {
        switch (state) {
            case ABORTED_SIZE:
            case ABORTED_TIMEOUT:
            case ABORTED_MANUAL:
                return conn.executeUpdate("db-setExecutionStateAborted",
                        r.table(Tables.EXECUTIONS.name)
                                .get(crawlExecutionId)
                                .update(
                                        doc -> r.branch(
                                                doc.hasFields("endTime"),
                                                r.hashMap(),
                                                r.hashMap("state", state.name()))
                                ),
                        CrawlExecutionStatus.class);
            default:
                throw new IllegalArgumentException(
                        "State must be one of ABORTED_SIZE, ABORTED_TIMEOUT, ABORTED_MANUAL, but was: " + state);
        }
    }

    @Override
    public Optional<CrawledContent> hasCrawledContent(CrawledContent crawledContent) throws DbException {
        ensureContainsValue(crawledContent, "digest");
        ensureContainsValue(crawledContent, "warc_id");
        ensureContainsValue(crawledContent, "target_uri");
        ensureContainsValue(crawledContent, "date");

        Map rMap = ProtoUtils.protoToRethink(crawledContent);
        Map<String, Object> response = conn.exec("db-hasCrawledContent",
                r.table(Tables.CRAWLED_CONTENT.name)
                        .insert(rMap)
                        .optArg("conflict", (id, old_doc, new_doc) -> old_doc)
                        .optArg("return_changes", "always")
                        .g("changes").nth(0).g("old_val"));

        if (response == null) {
            return Optional.empty();
        } else {
            CrawledContent result = ProtoUtils.rethinkToProto(response, CrawledContent.class);

            // Check existence of original in storage ref table.
            // This prevents false positives in the case writing of original record was cancelled after
            // crawled_content table was updated.
            Object check = conn.exec("db-hasCrawledContentCheck",
                    r.table(Tables.STORAGE_REF.name).get(result.getWarcId()));
            if (check == null) {
                return Optional.empty();
            } else {
                return Optional.of(result);
            }
        }
    }

    public void deleteCrawledContent(String digest) throws DbException {
        conn.exec("db-deleteCrawledContent", r.table(Tables.CRAWLED_CONTENT.name).get(digest).delete());
    }

    @Override
    public StorageRef saveStorageRef(StorageRef storageRef) throws DbException {
        ensureContainsValue(storageRef, "warc_id");
        ensureContainsValue(storageRef, "storage_ref");

        Map rMap = ProtoUtils.protoToRethink(storageRef);
        return conn.executeInsert("db-saveStorageRef",
                r.table(Tables.STORAGE_REF.name)
                        .insert(rMap)
                        .optArg("conflict", "replace"),
                StorageRef.class
        );
    }

    @Override
    public StorageRef getStorageRef(String warcId) throws DbException {
        return conn.executeGet("get-getStorageRef",
                r.table(Tables.STORAGE_REF.name).get(warcId),
                StorageRef.class
        );
    }

    @Override
    public CrawlLog saveCrawlLog(CrawlLog crawlLog) throws DbException {
        if (!"text/dns".equals(crawlLog.getContentType())) {
            if (crawlLog.getJobExecutionId().isEmpty()) {
                LOG.error("Missing JobExecutionId in CrawlLog: {}", crawlLog, new IllegalStateException());
            }
            if (crawlLog.getExecutionId().isEmpty()) {
                LOG.error("Missing ExecutionId in CrawlLog: {}", crawlLog, new IllegalStateException());
            }
        }
        if (crawlLog.getCollectionFinalName().isEmpty()) {
            LOG.error("Missing collectionFinalName: {}", crawlLog, new IllegalStateException());
        }
        if (!crawlLog.hasTimeStamp()) {
            crawlLog = crawlLog.toBuilder().setTimeStamp(ProtoUtils.getNowTs()).build();
        }

        Map rMap = ProtoUtils.protoToRethink(crawlLog);
        return conn.executeInsert("db-saveCrawlLog",
                r.table(Tables.CRAWL_LOG.name)
                        .insert(rMap)
                        .optArg("conflict", "replace"),
                CrawlLog.class
        );
    }

    @Override
    public ChangeFeed<CrawlLog> listCrawlLogs(CrawlLogListRequest crawlLogListRequest) throws DbException {
        ListCrawlLogQueryBuilder q = new ListCrawlLogQueryBuilder(crawlLogListRequest);

        Cursor<Map<String, Object>> res = conn.exec("db-listCrawlLogs", q.getListQuery());

        return new ChangeFeedBase<CrawlLog>(res) {
            @Override
            @SuppressWarnings("unchecked")
            protected Function<Map<String, Object>, CrawlLog> mapper() {
                return co -> {
                    // In case of a change feed, the real object is stored in new_val
                    // If new_val is empty, the object is deleted. We skip those.
                    if (co.containsKey("new_val")) {
                        co = (Map) co.get("new_val");
                        if (co == null) {
                            return null;
                        }
                    }
                    CrawlLog res = ProtoUtils.rethinkToProto(co, CrawlLog.class);
                    return res;
                };
            }
        };
    }

    @Override
    public ListCountResponse countCrawlLogs(CrawlLogListRequest crawlLogListRequest) throws DbException {
        ListCrawlLogQueryBuilder q = new ListCrawlLogQueryBuilder(crawlLogListRequest);
        long res = conn.exec("db-countCrawlLogs", q.getCountQuery());
        return ListCountResponse.newBuilder().setCount(res).build();
    }

    @Override
    public PageLog savePageLog(PageLog pageLog) throws DbException {
        if (pageLog.getCollectionFinalName().isEmpty()) {
            LOG.error("Missing collectionFinalName: {}", pageLog, new IllegalStateException());
        }
        Map rMap = ProtoUtils.protoToRethink(pageLog);
        return conn.executeInsert("db-savePageLog",
                r.table(Tables.PAGE_LOG.name)
                        .insert(rMap)
                        .optArg("conflict", "replace"),
                PageLog.class
        );
    }

    @Override
    public ChangeFeed<PageLog> listPageLogs(PageLogListRequest pageLogListRequest) throws DbException {
        ListPageLogQueryBuilder q = new ListPageLogQueryBuilder(pageLogListRequest);

        Cursor<Map<String, Object>> res = conn.exec("db-listPageLogs", q.getListQuery());

        return new ChangeFeedBase<PageLog>(res) {
            @Override
            @SuppressWarnings("unchecked")
            protected Function<Map<String, Object>, PageLog> mapper() {
                return co -> {
                    // In case of a change feed, the real object is stored in new_val
                    // If new_val is empty, the object is deleted. We skip those.
                    if (co.containsKey("new_val")) {
                        co = (Map) co.get("new_val");
                        if (co == null) {
                            return null;
                        }
                    }
                    PageLog res = ProtoUtils.rethinkToProto(co, PageLog.class);
                    return res;
                };
            }
        };
    }

    @Override
    public ListCountResponse countPageLogs(PageLogListRequest pageLogListRequest) throws DbException {
        ListPageLogQueryBuilder q = new ListPageLogQueryBuilder(pageLogListRequest);
        long res = conn.exec("db-countPageLogs", q.getCountQuery());
        return ListCountResponse.newBuilder().setCount(res).build();
    }

    @Override
    public ExtractedText addExtractedText(ExtractedText extractedText) throws DbException {
        ensureContainsValue(extractedText, "warc_id");
        ensureContainsValue(extractedText, "text");

        Map rMap = ProtoUtils.protoToRethink(extractedText);
        Map<String, Object> response = conn.exec("db-addExtractedText",
                r.table(Tables.EXTRACTED_TEXT.name)
                        .insert(rMap)
                        .optArg("conflict", "error"));

        return extractedText;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean setDesiredPausedState(boolean value) throws DbException {
        String id = "state";
        String key = "shouldPause";
        Map<String, List<Map<String, Map>>> state = conn.exec("set-paused",
                r.table(Tables.SYSTEM.name)
                        .insert(r.hashMap("id", id).with(key, value))
                        .optArg("conflict", "update")
                        .optArg("return_changes", "always")
        );
        Map oldValue = state.get("changes").get(0).get("old_val");
        if (oldValue == null || (Boolean) oldValue.computeIfAbsent(key, k -> Boolean.FALSE) == false) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public boolean getDesiredPausedState() throws DbException {
        String id = "state";
        String key = "shouldPause";
        Map<String, Object> state = conn.exec("get-paused",
                r.table(Tables.SYSTEM.name)
                        .get(id)
        );
        if (state == null) {
            return false;
        }
        return (Boolean) state.computeIfAbsent(key, k -> Boolean.FALSE);
    }

    private Map summarizeJobExecutionStats(String jobExecutionId) throws DbException {
        String[] EXECUTIONS_STAT_FIELDS = new String[]{"documentsCrawled", "documentsDenied",
                "documentsFailed", "documentsOutOfScope", "documentsRetried", "urisCrawled", "bytesCrawled"};

        return conn.exec("db-summarizeJobExecutionStats",
                r.table(Tables.EXECUTIONS.name)
                        .between(r.array(jobExecutionId, r.minval()), r.array(jobExecutionId, r.maxval()))
                        .optArg("index", "jobExecutionId_seedId")
                        .map(doc -> {
                                    MapObject m = r.hashMap();
                                    for (String f : EXECUTIONS_STAT_FIELDS) {
                                        m.with(f, doc.getField(f).default_(0));
                                    }
                                    for (CrawlExecutionStatus.State s : CrawlExecutionStatus.State.values()) {
                                        m.with(s.name(), r.branch(doc.getField("state").eq(s.name()), 1, 0));
                                    }
                                    return m;
                                }
                        )
                        .reduce((left, right) -> {
                                    MapObject m = r.hashMap();
                                    for (String f : EXECUTIONS_STAT_FIELDS) {
                                        m.with(f, left.getField(f).add(right.getField(f)));
                                    }
                                    for (CrawlExecutionStatus.State s : CrawlExecutionStatus.State.values()) {
                                        m.with(s.name(), left.getField(s.name()).add(right.getField(s.name())));
                                    }
                                    return m;
                                }
                        ).default_((doc) -> {
                            MapObject m = r.hashMap();
                            for (String f : EXECUTIONS_STAT_FIELDS) {
                                m.with(f, 0);
                            }
                            for (CrawlExecutionStatus.State s : CrawlExecutionStatus.State.values()) {
                                m.with(s.name(), 0);
                            }
                            return m;
                        }
                )
        );
    }

    private void ensureContainsValue(Message msg, String fieldName) {
        if (!msg.getAllFields().keySet().stream().filter(k -> k.getName().equals(fieldName)).findFirst().isPresent()) {
            throw new IllegalArgumentException("The required field '" + fieldName + "' is missing from: '" + msg
                    .getClass().getSimpleName() + "'");
        }
    }

}

package no.nb.nna.veidemann.db;

import com.google.protobuf.Message;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.contentwriter.v1.CrawledContent;
import no.nb.nna.veidemann.api.contentwriter.v1.StorageRef;
import no.nb.nna.veidemann.api.eventhandler.v1.EventObject;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;

public enum Tables {
    SYSTEM("system", null),
    CONFIG("config", ConfigObject.getDefaultInstance()),
    CRAWLED_CONTENT("crawled_content", CrawledContent.getDefaultInstance()),
    URI_QUEUE("uri_queue", QueuedUri.getDefaultInstance()),
    EXECUTIONS("executions", CrawlExecutionStatus.getDefaultInstance()),
    JOB_EXECUTIONS("job_executions", JobExecutionStatus.getDefaultInstance()),
    CRAWL_ENTITIES("config_crawl_entities", ConfigObject.getDefaultInstance()),
    SEEDS("config_seeds", ConfigObject.getDefaultInstance()),
    EVENTS("events", EventObject.getDefaultInstance());

    public final String name;

    public final Message schema;

    Tables(String name, Message schema) {
        this.name = name;
        this.schema = schema;
    }

}

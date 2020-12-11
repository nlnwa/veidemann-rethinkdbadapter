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

package no.nb.nna.veidemann.db.fieldmask;

import no.nb.nna.veidemann.api.commons.v1.FieldMask;
import no.nb.nna.veidemann.db.fieldmask.Indexes.Index;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class IndexesTest {

    @org.junit.jupiter.api.Test
    void getBestIndex() {
        FieldMask mask = FieldMask.getDefaultInstance();
        CrawlExecutionQueryBuilder queryBuilder = new CrawlExecutionQueryBuilder(mask);
        Index idx = queryBuilder.getBestIndex();
        assertThat(idx).isNull();

        mask = FieldMask.newBuilder().addPaths("bytesCrawled").build();
        queryBuilder = new CrawlExecutionQueryBuilder(mask);
        idx = queryBuilder.getBestIndex();
        assertThat(idx).isNull();

        mask = FieldMask.newBuilder().addPaths("id").build();
        queryBuilder = new CrawlExecutionQueryBuilder(mask);
        idx = queryBuilder.getBestIndex();
        assertThat(idx.indexName).isEqualTo("id");

        mask = FieldMask.newBuilder().addPaths("jobExecutionId").build();
        queryBuilder = new CrawlExecutionQueryBuilder(mask);
        idx = queryBuilder.getBestIndex();
        assertThat(idx.indexName).isEqualTo("jobExecutionId_seedId");

        mask = FieldMask.newBuilder().addPaths("jobExecutionId").addPaths("seedId").build();
        queryBuilder = new CrawlExecutionQueryBuilder(mask);
        idx = queryBuilder.getBestIndex();
        assertThat(idx.indexName).isEqualTo("jobExecutionId_seedId");

        mask = FieldMask.newBuilder().addPaths("seedId").build();
        queryBuilder = new CrawlExecutionQueryBuilder(mask);
        idx = queryBuilder.getBestIndex();
        assertThat(idx.indexName).isEqualTo("seedId");

        mask = FieldMask.newBuilder().addPaths("seedId").addPaths("createdTime").build();
        queryBuilder = new CrawlExecutionQueryBuilder(mask);
        idx = queryBuilder.getBestIndex();
        assertThat(idx.indexName).isEqualTo("seedId_createdTime");
    }

    @Test
    void getBestIndexesForMask() {
        FieldMask mask = FieldMask.getDefaultInstance();
        RethinkDbFieldMasksQueryBuilder queryBuilder = new JobExecutionQueryBuilder(mask);
        List<Index> result = queryBuilder.getBestIndexes();
        assertThat(result)
                .as("none")
                .hasSize(0);

        mask = FieldMask.newBuilder().addPaths("startTime").build();
        queryBuilder = new JobExecutionQueryBuilder(mask);
        result = queryBuilder.getBestIndexes();
        assertThat(result)
                .as("startTime")
                .containsExactly(
                        new Index("startTime", false, 0, "startTime")
                );

        mask = FieldMask.newBuilder().addPaths("jobId").build();
        queryBuilder = new JobExecutionQueryBuilder(mask);
        result = queryBuilder.getBestIndexes();
        assertThat(result)
                .as("jobId")
                .containsExactly(
                        new Index("jobId", false, 0, "jobId"),
                        new Index("jobId_startTime", false, 1, "jobId", "startTime")
                );

        mask = FieldMask.newBuilder().addPaths("jobId").addPaths("startTime").build();
        queryBuilder = new JobExecutionQueryBuilder(mask);
        result = queryBuilder.getBestIndexes();
        assertThat(result)
                .as("jobId, startTime")
                .containsExactly(
                        new Index("jobId_startTime", false, 0, "jobId", "startTime"),
                        new Index("jobId", false, 1, "jobId")
                );

        mask = FieldMask.newBuilder().addPaths("id").addPaths("jobId").addPaths("startTime").build();
        queryBuilder = new JobExecutionQueryBuilder(mask);
        result = queryBuilder.getBestIndexes();
        assertThat(result)
                .as("id, jobId, startTime")
                .containsExactly(
                        new Index("id", false, 0, "id")
                );

        mask = FieldMask.newBuilder().addPaths("id").build();
        queryBuilder = new JobExecutionQueryBuilder(mask);
        result = queryBuilder.getBestIndexes();
        assertThat(result)
                .as("id")
                .containsExactly(
                        new Index("id", false, 0, "id")
                );

        mask = FieldMask.newBuilder().addPaths("id").addPaths("startTime").build();
        queryBuilder = new JobExecutionQueryBuilder(mask);
        result = queryBuilder.getBestIndexes();
        assertThat(result)
                .as("id, startTime")
                .containsExactly(
                        new Index("id", false, 0, "id")
                );

        mask = FieldMask.newBuilder().addPaths("meta.name").build();
        queryBuilder = new ConfigObjectQueryBuilder(mask);
        result = queryBuilder.getBestIndexes();
        assertThat(result)
                .as("meta.name")
                .containsExactly(
                        new Index("name", false, 0, "meta.name")
                );

        mask = FieldMask.newBuilder().addPaths("meta.label").addPaths("meta.name").build();
        queryBuilder = new ConfigObjectQueryBuilder(mask);
        result = queryBuilder.getBestIndexes();
        assertThat(result)
                .as("meta.label, meta.name")
                .containsExactly(
                        new Index("label", false, 1, "meta.label")
                );
    }

    @Test
    void getBestIndexes() {
        RethinkDbFieldMasksQueryBuilder noMaskBuilder = new JobExecutionQueryBuilder();

        List<Index> result = noMaskBuilder.getBestIndexes("startTime");
        assertThat(result)
                .as("startTime")
                .containsExactly(
                        new Index("startTime", false, 0, "startTime")
                );

        result = noMaskBuilder.getBestIndexes("jobId");
        assertThat(result)
                .as("jobId")
                .containsExactly(
                        new Index("jobId", false, 0, "jobId"),
                        new Index("jobId_startTime", false, 1, "jobId", "startTime")
                );

        result = noMaskBuilder.getBestIndexes("startTime", "jobId");
        assertThat(result)
                .as("startTime, jobId")
                .containsExactly(
                        new Index("startTime", false, 1, "startTime")
                );

        result = noMaskBuilder.getBestIndexes("jobId", "startTime");
        assertThat(result)
                .as("jobId, startTime")
                .containsExactly(
                        new Index("jobId_startTime", false, 0, "jobId", "startTime"),
                        new Index("jobId", false, 1, "jobId")
                );

        result = noMaskBuilder.getBestIndexes("id", "jobId", "startTime");
        assertThat(result)
                .as("id, jobId, startTime")
                .containsExactly(
                        new Index("id", false, 0, "id")
                );

        result = noMaskBuilder.getBestIndexes("id");
        assertThat(result)
                .as("id")
                .containsExactly(
                        new Index("id", false, 0, "id")
                );

        result = noMaskBuilder.getBestIndexes("id", "startTime");
        assertThat(result)
                .as("id, startTime")
                .containsExactly(
                        new Index("id", false, 0, "id")
                );

        result = noMaskBuilder.getBestIndexes("startTime", "id");
        assertThat(result)
                .as("startTime, id")
                .containsExactly(
                        new Index("id", false, 0, "id")
                );

        noMaskBuilder = new ConfigObjectQueryBuilder();
        result = noMaskBuilder.getBestIndexes("meta.name");
        assertThat(result)
                .as("meta.name")
                .containsExactly(
                        new Index("name", false, 0, "meta.name")
                );

        result = noMaskBuilder.getBestIndexes("meta.name", "meta.label");
        assertThat(result)
                .as("meta.name, meta.label")
                .containsExactly(
                        new Index("name", false, 1, "meta.name")
                );

        result = noMaskBuilder.getBestIndexes("meta.label", "meta.name");
        assertThat(result)
                .as("meta.label, meta.name")
                .containsExactly(
                        new Index("label", false, 1, "meta.label")
                );
    }
}
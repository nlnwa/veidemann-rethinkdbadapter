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

import static org.assertj.core.api.Assertions.assertThat;

class IndexesTest {

    @org.junit.jupiter.api.Test
    void getBestIndex() {
        FieldMask mask = FieldMask.newBuilder().addPaths("bytesCrawled").build();
        CrawlExecutionQueryBuilder queryBuilder = new CrawlExecutionQueryBuilder(mask);
        Index idx = queryBuilder.getBestIndex();
        assertThat(idx).isNull();

        mask = FieldMask.newBuilder().addPaths("id").build();
        queryBuilder = new CrawlExecutionQueryBuilder(mask);
        idx = queryBuilder.getBestIndex();
        assertThat(idx.IndexName).isEqualTo("id");

        mask = FieldMask.newBuilder().addPaths("jobExecutionId").build();
        queryBuilder = new CrawlExecutionQueryBuilder(mask);
        idx = queryBuilder.getBestIndex();
        assertThat(idx.IndexName).isEqualTo("jobExecutionId_seedId");

        mask = FieldMask.newBuilder().addPaths("jobExecutionId").addPaths("seedId").build();
        queryBuilder = new CrawlExecutionQueryBuilder(mask);
        idx = queryBuilder.getBestIndex();
        assertThat(idx.IndexName).isEqualTo("jobExecutionId_seedId");

        mask = FieldMask.newBuilder().addPaths("seedId").build();
        queryBuilder = new CrawlExecutionQueryBuilder(mask);
        idx = queryBuilder.getBestIndex();
        assertThat(idx.IndexName).isEqualTo("seedId");

        mask = FieldMask.newBuilder().addPaths("seedId").addPaths("createdTime").build();
        queryBuilder = new CrawlExecutionQueryBuilder(mask);
        idx = queryBuilder.getBestIndex();
        assertThat(idx.IndexName).isEqualTo("seedId_createdTime");
    }
}
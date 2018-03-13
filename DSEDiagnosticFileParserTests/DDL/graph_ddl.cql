
CREATE KEYSPACE xnetpipeline1 WITH replication = {'class': 'NetworkTopologyStrategy', 'Chicago': '3'}  AND durable_writes = true;

CREATE TABLE xnetpipeline1."JobProperties_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "eventProcessClass" text,
    "eventSourceClass" text,
    "fromDateTime" bigint,
    "toDateTime" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."JobPlan_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "fromDateTime" bigint,
    name text,
    "toDateTime" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."BillerRTQueryRegister_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."FileSource_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."DdpDeviceTypeXref_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."BillerSyncQueryRegister_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "filePath" text,
    "queryType" text,
    "schema" text,
    "siteId" text,
    "sourceEventTime" bigint,
    "sourceId" bigint,
    "sourceProcessTime" bigint,
    "storeName" text,
    system text,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."FileSource_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "filePath" text,
    "fileSizeInBytes" bigint,
    "fromDateTime" bigint,
    "processedDate" text,
    "sourceTypeWithDate" text,
    "toDateTime" bigint,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX FileSource_2i_bySourceId ON xnetpipeline1."FileSource_p" (xid);
CREATE INDEX FileSource_2i_byFsProcessedDate ON xnetpipeline1."FileSource_p" ("processedDate");
CREATE INDEX FileSource_2i_byFsSourceTypeWithDate ON xnetpipeline1."FileSource_p" ("sourceTypeWithDate");

CREATE TABLE xnetpipeline1."BillerRTQueryRegister_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "lastPullDate" text,
    "lastPullSlot" text,
    "lastPullTime" text,
    "queryType" text,
    "schema" text,
    "siteId" text,
    "sourceEventTime" bigint,
    "sourceId" bigint,
    "sourceProcessTime" bigint,
    "storeName" text,
    system text,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."DdpDeviceTypeXref_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "addressType" text,
    "boxEquip" text,
    "deviceType" text,
    "longDesc" text,
    "siteId" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."JobProperties_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."JobBundle_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."ServiceSource_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "fromDateTime" bigint,
    name text,
    "processedDate" text,
    "sourceTypeWithDate" text,
    "toDateTime" bigint,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX ServiceSource_2i_bySersProcessedDate ON xnetpipeline1."ServiceSource_p" ("processedDate");
CREATE INDEX ServiceSource_2i_bySersSourceTypeWithDate ON xnetpipeline1."ServiceSource_p" ("sourceTypeWithDate");
CREATE INDEX ServiceSource_2i_byServiceSourceId ON xnetpipeline1."ServiceSource_p" (xid);

CREATE TABLE xnetpipeline1."JobAttempt_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."ServiceSource_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."BillerSyncQueryRegister_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."IMSSyncRegister_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."FileEventGulla_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "bundleNumber" bigint,
    context bigint,
    "eventCount" bigint,
    "eventGullaVersion" bigint,
    "eventTypeId" bigint,
    events blob,
    "fromDateTime" bigint,
    "jobAttemptId" bigint,
    "processedDate" text,
    "processedTime" bigint,
    "sourceTypeWithDate" text,
    "threadID" bigint,
    "toDateTime" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX FileEventGulla_2i_byBundleNumber ON xnetpipeline1."FileEventGulla_p" ("bundleNumber");
CREATE INDEX FileEventGulla_2i_byFegSourceTypeWithDate ON xnetpipeline1."FileEventGulla_p" ("sourceTypeWithDate");
CREATE INDEX FileEventGulla_2i_byGullaJobAttemptId ON xnetpipeline1."FileEventGulla_p" ("jobAttemptId");
CREATE INDEX FileEventGulla_2i_byFegProcessedDate ON xnetpipeline1."FileEventGulla_p" ("processedDate");
CREATE INDEX FileEventGulla_2i_byProcessedTime ON xnetpipeline1."FileEventGulla_p" ("processedTime");

CREATE TABLE xnetpipeline1."Context_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."CsgDeviceTypeXref_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "deviceType" text,
    "eqpCmpntEqp" text,
    "eqpTypDesc" text,
    "eqpTypEqp" text,
    prin text,
    sys text,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."SourceStatus_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."JobManager_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."FileEventGulla_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."SourceType_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "fromDateTime" bigint,
    name text,
    "toDateTime" bigint,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX SourceType_2i_byname ON xnetpipeline1."SourceType_p" (name);

CREATE TABLE xnetpipeline1."IMSSyncRegister_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "filePath" text,
    "partitionKeyId" text,
    "sourceEventTime" bigint,
    "sourceId" bigint,
    "sourceProcessTime" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1.id_allocation (
    partition_hash int,
    dc_id smallint,
    member_id bigint,
    PRIMARY KEY ((partition_hash, dc_id))
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."JobBundle_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BQE@fail#qc" bigint,
    "BQE@finish#qc" bigint,
    "BQE@query#mwt" bigint,
    "BQE@submit#qc" bigint,
    "EC@queuepresize#ec" bigint,
    "EC@transform#mwt" bigint,
    "EH@2errorQueueAck#ec" bigint,
    "EH@2errorQueueSent#ec" bigint,
    "EH@businessFail#ec" bigint,
    "EH@commit#ec" bigint,
    "EH@commit#mwt" bigint,
    "EH@commitFail#ec" bigint,
    "EH@errorQueueAck#ec" bigint,
    "EH@errorQueueSent#ec" bigint,
    "EH@fail#ec" bigint,
    "EH@handlerFail#ec" bigint,
    "EH@kafkaPersistence#ec" bigint,
    "EH@kafkaPersistence#mwt" bigint,
    "EH@postpone#ec" bigint,
    "EH@postponeFail#ec" bigint,
    "EH@process#ec" bigint,
    "EH@readyFail#ec" bigint,
    "EH@receive#ec" bigint,
    "EH@success#ec" bigint,
    "EH@systemFail#ec" bigint,
    "FW@process#fc" bigint,
    "GEC@persist#mwt" bigint,
    "GEC@persist#nc" bigint,
    "GEC@persistFail#nc" bigint,
    "GEC@prepare#edc" bigint,
    "GEC@prepare#nc" bigint,
    "GEC@prepare#pkt" bigint,
    "GEC@prepareAll#nc" bigint,
    "GEC@prepareDelete#edc" bigint,
    "GEC@prepareDelete#nc" bigint,
    "GEC@prepareUpdate#edc" bigint,
    "GEC@prepareUpdate#nc" bigint,
    "GEC@vabReuse#nc" bigint,
    "GP@create#edc" bigint,
    "GP@create#pc" bigint,
    "GP@create#pvc" bigint,
    "GP@delete#pvc" bigint,
    "GP@dexpiredrop#edc" bigint,
    "GP@dexpiredrop#nc" bigint,
    "GP@expire#edc" bigint,
    "GP@expire#nc" bigint,
    "GP@expiredrop#edc" bigint,
    "GP@expiredrop#nc" bigint,
    "GP@fetch#edc" bigint,
    "GP@fetch#nc" bigint,
    "GP@fetch#pc" bigint,
    "GP@fetch#pvc" bigint,
    "GP@increate#nc" bigint,
    "GP@outcreate#nc" bigint,
    "GP@revive#nc" bigint,
    "GP@update#nc" bigint,
    "GP@update#pc" bigint,
    "GP@update#pvc" bigint,
    "JB@main#note" text,
    "JM@job#no" bigint,
    "JM@jobBundle#ewt" bigint,
    "JM@jobBundle#no" bigint,
    "JM@jobBundleEnd#time" bigint,
    "JM@jobBundleStart#time" bigint,
    "MF@filtered#mc" bigint,
    "MF@unfiltered#mc" bigint,
    "MS@fetch#bytes" bigint,
    "MS@ignored#ec" bigint,
    "MS@rdd#mc" bigint,
    "MS@receive#mc" bigint,
    "MS@workFail#ec" bigint,
    "MSS@all#mc" bigint,
    "MSS@batchCurrent#ewt" bigint,
    "MSS@batchPrev#ewt" bigint,
    "MSS@batchStartCurrent#time" bigint,
    "MSS@batchStartPrev#time" bigint,
    "MSS@common#mc" bigint,
    "MSS@compare#mwt" bigint,
    "MSS@delete#mc" bigint,
    "MSS@deleteEdge#mc" bigint,
    "MSS@devicetype#nulc" bigint,
    "MSS@extractCurrent#mwt" bigint,
    "MSS@extractPrev#mwt" bigint,
    "MSS@insert#mc" bigint,
    "MSS@macCurrent#nulc" bigint,
    "MSS@macPrev#nulc" bigint,
    "MSS@override#mc" bigint,
    "MSS@prepareCompare#mwt" bigint,
    "MSS@process#bac" bigint,
    "MSS@processCurrent#bac" bigint,
    "MSS@processPrev#bac" bigint,
    "MSS@receiveCurrent#mc" bigint,
    "MSS@receivePrev#mc" bigint,
    "MSS@scoutDevice#nfc" bigint,
    "MSS@update#mc" bigint,
    "MSS@webService#rqc" bigint,
    "MT@passthrough#ec" bigint,
    "MT@transform#ec" bigint,
    "NP@createB#mwt" bigint,
    "NP@createB#nc" bigint,
    "NP@createBR#nc" bigint,
    "NP@createS#mwt" bigint,
    "NP@createS#nc" bigint,
    "NP@createSR#nc" bigint,
    "NP@fetchB#mwt" bigint,
    "NP@fetchB#nc" bigint,
    "NP@fetchS#mwt" bigint,
    "NP@fetchS#nc" bigint,
    "NP@nodeCache#hc" bigint,
    "NP@prefetch#mwt" bigint,
    "NP@prefetch#nc" bigint,
    "PITA@all#rsc" bigint,
    "PITA@complete#tc" bigint,
    "PITA@fail#tc" bigint,
    "PITA@fetch#buc" bigint,
    "PITA@start#tc" bigint,
    "PJM@job#no" bigint,
    "PJM@jobBundle#ewt" bigint,
    "PJM@jobBundle#no" bigint,
    "PJM@jobBundleEnd#time" bigint,
    "PJM@jobBundleStart#time" bigint,
    "PL@commit#mwt" bigint,
    "PL@fail#ec" bigint,
    "PL@fail#gc" bigint,
    "PL@success#ec" bigint,
    "PL@success#gc" bigint,
    "QEC@persist@ec" bigint,
    "QEC@persist@mwt" bigint,
    "RP@extract#mwt" bigint,
    "bundleNumber" bigint,
    errorcode bigint,
    errordescription text,
    "filePath" text,
    "fileSizeInBytes" bigint,
    "hostName" text,
    "jobAttemptId" bigint,
    "processedDate" text,
    "sourceTypeName" text,
    "sourceTypeWithDate" text,
    status text,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX JobBundle_2i_byJbSourceTypeName ON xnetpipeline1."JobBundle_p" ("sourceTypeName");
CREATE INDEX JobBundle_2i_byBundleStartTime ON xnetpipeline1."JobBundle_p" ("JM@jobBundleStart#time");
CREATE INDEX JobBundle_2i_byBundleNumber ON xnetpipeline1."JobBundle_p" ("bundleNumber");
CREATE INDEX JobBundle_2i_byJobAtmpId ON xnetpipeline1."JobBundle_p" ("jobAttemptId");
CREATE INDEX JobBundle_2i_byJbProcessedDate ON xnetpipeline1."JobBundle_p" ("processedDate");
CREATE INDEX JobBundle_2i_byStatus ON xnetpipeline1."JobBundle_p" (status);
CREATE INDEX JobBundle_2i_byJbSourceTypeWithDate ON xnetpipeline1."JobBundle_p" ("sourceTypeWithDate");
CREATE INDEX JobBundle_2i_byFilePath ON xnetpipeline1."JobBundle_p" ("filePath");

CREATE TABLE xnetpipeline1."Context_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "fromDateTime" bigint,
    "locationId" bigint,
    "processedDate" text,
    "programTypeId" bigint,
    purpose text,
    "toDateTime" bigint,
    "userName" text,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX Context_2i_byContextId ON xnetpipeline1."Context_p" (xid);

CREATE TABLE xnetpipeline1."SourceStatus_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "fromDateTime" bigint,
    "sourceId" bigint,
    status text,
    "toDateTime" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."CsgDeviceTypeXref_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."JobAttemptStatus_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."OpenwaveSyncRegister_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."Job_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."OpenWaveChangeNumber_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "changeNumber" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."HarmonySyncRegister_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "filePath" text,
    "partitionKeyId" text,
    "sourceEventTime" bigint,
    "sourceId" bigint,
    "sourceProcessTime" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."Job_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "fromDateTime" bigint,
    "processedDate" text,
    "sourceId" bigint,
    "sourceTypeWithDate" text,
    "toDateTime" bigint,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX Job_2i_byJProcessedDate ON xnetpipeline1."Job_p" ("processedDate");
CREATE INDEX Job_2i_byJobId ON xnetpipeline1."Job_p" (xid);
CREATE INDEX Job_2i_byJSourceTypeWithDate ON xnetpipeline1."Job_p" ("sourceTypeWithDate");

CREATE TABLE xnetpipeline1."OpenWaveChangeNumber_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."Source_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."JobAttemptStatus_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "fromDateTime" bigint,
    status text,
    "toDateTime" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."OpenwaveSyncRegister_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "filePath" text,
    "sourceEventTime" bigint,
    "sourceId" bigint,
    "sourceProcessTime" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."HarmonySyncRegister_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."JobManager_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "fromDateTime" bigint,
    "jobManagerClass" text,
    "toDateTime" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."FileReceived_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."JobAttempt_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "fromDateTime" bigint,
    "jobId" bigint,
    "processedDate" text,
    "sourceTypeWithDate" text,
    "toDateTime" bigint,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX JobAttempt_2i_byJaSourceTypeWithDate ON xnetpipeline1."JobAttempt_p" ("sourceTypeWithDate");
CREATE INDEX JobAttempt_2i_byJaProcessedDate ON xnetpipeline1."JobAttempt_p" ("processedDate");
CREATE INDEX JobAttempt_2i_byJobAttemptId ON xnetpipeline1."JobAttempt_p" (xid);

CREATE TABLE xnetpipeline1."FileReceived_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "destinationFilePath" text,
    "fromDateTime" bigint,
    "jobAttemptId" bigint,
    "processedDate" text,
    "sourceFilePath" text,
    "sourceTypeWithDate" text,
    "toDateTime" bigint,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX FileReceived_2i_byFrProcessedDate ON xnetpipeline1."FileReceived_p" ("processedDate");
CREATE INDEX FileReceived_2i_byDestinationFilePath ON xnetpipeline1."FileReceived_p" ("destinationFilePath");
CREATE INDEX FileReceived_2i_byJbAttemptId ON xnetpipeline1."FileReceived_p" ("jobAttemptId");
CREATE INDEX FileReceived_2i_byFrSourceTypeWithDate ON xnetpipeline1."FileReceived_p" ("sourceTypeWithDate");
CREATE INDEX FileReceived_2i_bySourceFilePath ON xnetpipeline1."FileReceived_p" ("sourceFilePath");

CREATE TABLE xnetpipeline1."Source_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "filePath" text,
    "fileSizeInBytes" bigint,
    "fromDateTime" bigint,
    "processedDate" text,
    "sourceTypeName" text,
    "sourceTypeWithDate" text,
    "toDateTime" bigint,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."SourceType_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1."JobPlan_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE KEYSPACE xnetgraph1_system WITH replication = {'class': 'NetworkTopologyStrategy', 'Chicago': '5'}  AND durable_writes = true;

CREATE TABLE xnetgraph1_system.shared_data (
    dataspace text,
    valid_until timeuuid,
    namespace text,
    name text,
    boolean boolean,
    double double,
    duration bigint,
    enum text,
    float float,
    inet inet,
    instant timestamp,
    integer int,
    json blob,
    last_updated timeuuid static,
    long bigint,
    string text,
    type text,
    written_on timeuuid,
    PRIMARY KEY (dataspace, valid_until, namespace, name)
) WITH CLUSTERING ORDER BY (valid_until ASC, namespace ASC, name ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = '11a87e41-c993-11e7-81cc-999680da1b3e'
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1_system.shared_data_versions (
    node inet PRIMARY KEY,
    version uuid
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1_system.dseg_variables (
    key text PRIMARY KEY,
    type text,
    value text
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE KEYSPACE xnetgraph_pvt WITH replication = {'class': 'LocalStrategy'}  AND durable_writes = true;

CREATE KEYSPACE "xnetgraph17G_system" WITH replication = {'class': 'NetworkTopologyStrategy', 'Chicago': '5'}  AND durable_writes = true;

CREATE TABLE "xnetgraph17G_system".shared_data (
    dataspace text,
    valid_until timeuuid,
    namespace text,
    name text,
    boolean boolean,
    double double,
    duration bigint,
    enum text,
    float float,
    inet inet,
    instant timestamp,
    integer int,
    json blob,
    last_updated timeuuid static,
    long bigint,
    string text,
    type text,
    written_on timeuuid,
    PRIMARY KEY (dataspace, valid_until, namespace, name)
) WITH CLUSTERING ORDER BY (valid_until ASC, namespace ASC, name ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = 'e46b67a1-a4a6-11e7-a34a-c7e88f6b959e'
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G_system".shared_data_versions (
    node inet PRIMARY KEY,
    version uuid
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G_system".dseg_variables (
    key text PRIMARY KEY,
    type text,
    value text
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE KEYSPACE dse_perf WITH replication = {'class': 'NetworkTopologyStrategy', 'Chicago': '1', 'Potomac': '1'}  AND durable_writes = true;

CREATE TABLE dse_perf.thread_pool (
    node_ip inet,
    pool_name text,
    active bigint,
    all_time_blocked bigint,
    blocked bigint,
    completed bigint,
    pending bigint,
    PRIMARY KEY (node_ip, pool_name)
) WITH CLUSTERING ORDER BY (pool_name ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'NONE', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 0
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 3600000
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE dse_perf.net_stats (
    node_ip inet PRIMARY KEY,
    commands_completed bigint,
    commands_pending int,
    read_repair_attempted bigint,
    read_repaired_background bigint,
    read_repaired_blocking bigint,
    responses_completed bigint,
    responses_pending int
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'NONE', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 0
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 3600000
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE dse_perf.graph_event_log (
    systemid uuid,
    node_ip inet,
    date timestamp,
    time timeuuid,
    attributes text,
    cause text,
    duration bigint,
    graphid text,
    txgroup text,
    txid uuid,
    type_name text,
    PRIMARY KEY ((systemid, node_ip, date), time)
) WITH CLUSTERING ORDER BY (time DESC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 0
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 3600000
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE dse_perf.schema_migration_log (
    systemid uuid,
    node_ip inet,
    date timestamp,
    time timeuuid,
    attributes text,
    cause text,
    duration bigint,
    graphid text,
    txgroup text,
    txid uuid,
    type_name text,
    PRIMARY KEY ((systemid, node_ip, date), time)
) WITH CLUSTERING ORDER BY (time DESC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 0
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 3600000
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE dse_perf.key_cache (
    node_ip inet PRIMARY KEY,
    cache_capacity bigint,
    cache_hits bigint,
    cache_requests bigint,
    cache_size bigint,
    hit_rate double
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'NONE', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 0
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 3600000
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE dse_perf.thread_pool_messages (
    node_ip inet,
    message_type text,
    dropped_count int,
    PRIMARY KEY (node_ip, message_type)
) WITH CLUSTERING ORDER BY (message_type ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'NONE', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 0
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 3600000
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE dse_perf.slow_transaction_log (
    systemid uuid,
    node_ip inet,
    date timestamp,
    time timeuuid,
    attributes text,
    cause text,
    duration bigint,
    graphid text,
    txgroup text,
    txid uuid,
    type_name text,
    PRIMARY KEY ((systemid, node_ip, date), time)
) WITH CLUSTERING ORDER BY (time DESC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 0
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 3600000
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE dse_perf.node_slow_log (
    node_ip inet,
    date timestamp,
    start_time timeuuid,
    commands list<text>,
    duration bigint,
    parameters map<text, text>,
    source_ip inet,
    table_names set<text>,
    username text,
    PRIMARY KEY ((node_ip, date), start_time)
) WITH CLUSTERING ORDER BY (start_time DESC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 0
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 3600000
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE KEYSPACE xnetpipeline_system WITH replication = {'class': 'NetworkTopologyStrategy', 'Chicago': '5', 'Potomac': '5'}  AND durable_writes = true;

CREATE TABLE xnetpipeline_system.shared_data (
    dataspace text,
    valid_until timeuuid,
    namespace text,
    name text,
    boolean boolean,
    double double,
    duration bigint,
    enum text,
    float float,
    inet inet,
    instant timestamp,
    integer int,
    json blob,
    last_updated timeuuid static,
    long bigint,
    string text,
    type text,
    written_on timeuuid,
    PRIMARY KEY (dataspace, valid_until, namespace, name)
) WITH CLUSTERING ORDER BY (valid_until ASC, namespace ASC, name ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = 'c5abd0d1-181d-11e8-be27-97120eea457d'
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline_system.shared_data_versions (
    node inet PRIMARY KEY,
    version uuid
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline_system.dseg_variables (
    key text PRIMARY KEY,
    type text,
    value text
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE KEYSPACE xnetgraph_system WITH replication = {'class': 'NetworkTopologyStrategy', 'Chicago': '5', 'Potomac': '5'}  AND durable_writes = true;

CREATE TABLE xnetgraph_system.shared_data (
    dataspace text,
    valid_until timeuuid,
    namespace text,
    name text,
    boolean boolean,
    double double,
    duration bigint,
    enum text,
    float float,
    inet inet,
    instant timestamp,
    integer int,
    json blob,
    last_updated timeuuid static,
    long bigint,
    string text,
    type text,
    written_on timeuuid,
    PRIMARY KEY (dataspace, valid_until, namespace, name)
) WITH CLUSTERING ORDER BY (valid_until ASC, namespace ASC, name ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = 'e479ad31-1bff-11e8-8fb8-cd07bf2d3a76'
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph_system.shared_data_versions (
    node inet PRIMARY KEY,
    version uuid
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph_system.dseg_variables (
    key text PRIMARY KEY,
    type text,
    value text
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE KEYSPACE "xnetpipeline17G_system" WITH replication = {'class': 'NetworkTopologyStrategy', 'Chicago': '5'}  AND durable_writes = true;

CREATE TABLE "xnetpipeline17G_system".shared_data (
    dataspace text,
    valid_until timeuuid,
    namespace text,
    name text,
    boolean boolean,
    double double,
    duration bigint,
    enum text,
    float float,
    inet inet,
    instant timestamp,
    integer int,
    json blob,
    last_updated timeuuid static,
    long bigint,
    string text,
    type text,
    written_on timeuuid,
    PRIMARY KEY (dataspace, valid_until, namespace, name)
) WITH CLUSTERING ORDER BY (valid_until ASC, namespace ASC, name ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = 'edce4051-a560-11e7-97a6-b9c7c41477ee'
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G_system".shared_data_versions (
    node inet PRIMARY KEY,
    version uuid
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G_system".dseg_variables (
    key text PRIMARY KEY,
    type text,
    value text
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE KEYSPACE "xnetpipeline17G" WITH replication = {'class': 'NetworkTopologyStrategy', 'Chicago': '3'}  AND durable_writes = true;

CREATE TABLE "xnetpipeline17G"."JobProperties_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "eventProcessClass" text,
    "eventSourceClass" text,
    "fromDateTime" bigint,
    "toDateTime" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G"."JobPlan_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "fromDateTime" bigint,
    name text,
    "toDateTime" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G"."JobAttempt_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G"."ImsSyncSource_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "filePath" text,
    "fileSizeInBytes" bigint,
    "fromDateTime" bigint,
    "partitionKeyId" text,
    "processedDate" text,
    "sourceTypeName" text,
    "sourceTypeWithDate" text,
    "toDateTime" bigint,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G"."SourceType_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "fromDateTime" bigint,
    name text,
    "sourceGroup" bigint,
    "toDateTime" bigint,
    version bigint,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX SourceType_2i_bySourceGroup ON "xnetpipeline17G"."SourceType_p" ("sourceGroup");
CREATE INDEX SourceType_2i_byVersion ON "xnetpipeline17G"."SourceType_p" (version);
CREATE INDEX SourceType_2i_byname ON "xnetpipeline17G"."SourceType_p" (name);

CREATE TABLE "xnetpipeline17G"."JobBundle_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G"."FileSource_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "filePath" text,
    "fileSizeInBytes" bigint,
    "fromDateTime" bigint,
    "processedDate" text,
    "sourceTypeWithDate" text,
    "toDateTime" bigint,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX FileSource_2i_bySourceId ON "xnetpipeline17G"."FileSource_p" (xid);
CREATE INDEX FileSource_2i_byFsProcessedDate ON "xnetpipeline17G"."FileSource_p" ("processedDate");
CREATE INDEX FileSource_2i_byFsSourceTypeWithDate ON "xnetpipeline17G"."FileSource_p" ("sourceTypeWithDate");

CREATE TABLE "xnetpipeline17G"."JobAttempt_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "fromDateTime" bigint,
    "jobId" bigint,
    "processedDate" text,
    "sourceTypeWithDate" text,
    "toDateTime" bigint,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX JobAttempt_2i_byJaSourceTypeWithDate ON "xnetpipeline17G"."JobAttempt_p" ("sourceTypeWithDate");
CREATE INDEX JobAttempt_2i_byJaProcessedDate ON "xnetpipeline17G"."JobAttempt_p" ("processedDate");
CREATE INDEX JobAttempt_2i_byJobAttemptId ON "xnetpipeline17G"."JobAttempt_p" (xid);

CREATE TABLE "xnetpipeline17G"."JobProperties_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G"."ServiceSource_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "fromDateTime" bigint,
    name text,
    "processedDate" text,
    "sourceTypeWithDate" text,
    "toDateTime" bigint,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX ServiceSource_2i_bySersProcessedDate ON "xnetpipeline17G"."ServiceSource_p" ("processedDate");
CREATE INDEX ServiceSource_2i_bySersSourceTypeWithDate ON "xnetpipeline17G"."ServiceSource_p" ("sourceTypeWithDate");
CREATE INDEX ServiceSource_2i_byServiceSourceId ON "xnetpipeline17G"."ServiceSource_p" (xid);

CREATE TABLE "xnetpipeline17G"."JobBundle_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BQE@fail#qc" bigint,
    "BQE@finish#qc" bigint,
    "BQE@query#mwt" bigint,
    "BQE@submit#qc" bigint,
    "EC@transform#mwt" bigint,
    "EH@2errorQueueAck#ec" bigint,
    "EH@2errorQueueSent#ec" bigint,
    "EH@businessFail#ec" bigint,
    "EH@commit#ec" bigint,
    "EH@commit#mwt" bigint,
    "EH@commitFail#ec" bigint,
    "EH@errorQueueAck#ec" bigint,
    "EH@errorQueueSent#ec" bigint,
    "EH@fail#ec" bigint,
    "EH@handlerFail#ec" bigint,
    "EH@kafkaPersistence#ec" bigint,
    "EH@kafkaPersistence#mwt" bigint,
    "EH@postpone#ec" bigint,
    "EH@postponeFail#ec" bigint,
    "EH@process#ec" bigint,
    "EH@readyFail#ec" bigint,
    "EH@receive#ec" bigint,
    "EH@success#ec" bigint,
    "EH@systemFail#ec" bigint,
    "FW@process#fc" bigint,
    "GEC@persist#mwt" bigint,
    "GEC@persist#nc" bigint,
    "GEC@persistFail#nc" bigint,
    "GEC@prepare#edc" bigint,
    "GEC@prepare#nc" bigint,
    "GEC@prepare#pkt" bigint,
    "GEC@prepareAll#nc" bigint,
    "GEC@prepareDelete#edc" bigint,
    "GEC@prepareDelete#nc" bigint,
    "GEC@prepareUpdate#edc" bigint,
    "GEC@prepareUpdate#nc" bigint,
    "GEC@vabReuse#nc" bigint,
    "GP@consult#edc" bigint,
    "GP@consult#pc" bigint,
    "GP@consultdrop#edc" bigint,
    "GP@consultdrop#pc" bigint,
    "GP@create#edc" bigint,
    "GP@create#pc" bigint,
    "GP@createexpire#edc" bigint,
    "GP@delete#edc" bigint,
    "GP@delete#pc" bigint,
    "GP@expire#edc" bigint,
    "GP@expiredrop#edc" bigint,
    "GP@fetch#edc" bigint,
    "GP@fetch#pc" bigint,
    "GP@icreate#nc" bigint,
    "GP@ifetch#nc" bigint,
    "GP@jcheck#nc" bigint,
    "GP@jcreate#nc" bigint,
    "GP@ocorrection#nc" bigint,
    "GP@ocreate#nc" bigint,
    "GP@ocreateexpire#nc" bigint,
    "GP@oexpire#nc" bigint,
    "GP@oexpiredrop#nc" bigint,
    "GP@oexpirelog#nc" bigint,
    "GP@ofetch#nc" bigint,
    "GP@orevive#nc" bigint,
    "GP@oupdate#nc" bigint,
    "GP@oupdatedrop#nc" bigint,
    "GP@unconsult#edc" bigint,
    "JM@job#no" bigint,
    "JM@jobBundle#ewt" bigint,
    "JM@jobBundle#no" bigint,
    "JM@jobBundleEnd#time" bigint,
    "JM@jobBundleStart#time" bigint,
    "MF@filtered#mc" bigint,
    "MF@unfiltered#mc" bigint,
    "MS@fetch#bytes" bigint,
    "MS@ignored#ec" bigint,
    "MS@rdd#mc" bigint,
    "MS@receive#mc" bigint,
    "MS@workFail#ec" bigint,
    "MSS@all#mc" bigint,
    "MSS@batchCurrent#ewt" bigint,
    "MSS@batchPrev#ewt" bigint,
    "MSS@batchStartCurrent#time" bigint,
    "MSS@batchStartPrev#time" bigint,
    "MSS@common#mc" bigint,
    "MSS@compare#mwt" bigint,
    "MSS@delete#mc" bigint,
    "MSS@deleteEdge#mc" bigint,
    "MSS@devicetype#nulc" bigint,
    "MSS@extractCurrent#mwt" bigint,
    "MSS@extractPrev#mwt" bigint,
    "MSS@insert#mc" bigint,
    "MSS@macCurrent#nulc" bigint,
    "MSS@macPrev#nulc" bigint,
    "MSS@override#mc" bigint,
    "MSS@prepareCompare#mwt" bigint,
    "MSS@process#bac" bigint,
    "MSS@processCurrent#bac" bigint,
    "MSS@processPrev#bac" bigint,
    "MSS@receiveCurrent#mc" bigint,
    "MSS@receivePrev#mc" bigint,
    "MSS@scoutDevice#nfc" bigint,
    "MSS@update#mc" bigint,
    "MSS@webService#rqc" bigint,
    "MT@passthrough#ec" bigint,
    "MT@transform#ec" bigint,
    "NP@createB#mwt" bigint,
    "NP@createB#nc" bigint,
    "NP@createBR#nc" bigint,
    "NP@createS#mwt" bigint,
    "NP@createS#nc" bigint,
    "NP@createSR#nc" bigint,
    "NP@fetchB#mwt" bigint,
    "NP@fetchB#nc" bigint,
    "NP@fetchS#mwt" bigint,
    "NP@fetchS#nc" bigint,
    "NP@nodeCache#hc" bigint,
    "NP@prefetch#mwt" bigint,
    "NP@prefetch#nc" bigint,
    "PITA@all#rsc" bigint,
    "PITA@complete#tc" bigint,
    "PITA@fail#tc" bigint,
    "PITA@fetch#buc" bigint,
    "PITA@start#tc" bigint,
    "PJM@job#no" bigint,
    "PJM@jobBundle#ewt" bigint,
    "PJM@jobBundle#no" bigint,
    "PJM@jobBundleEnd#time" bigint,
    "PJM@jobBundleStart#time" bigint,
    "PL@commit#mwt" bigint,
    "PL@fail#ec" bigint,
    "PL@fail#gc" bigint,
    "PL@success#ec" bigint,
    "PL@success#gc" bigint,
    "QEC@persist@ec" bigint,
    "QEC@persist@mwt" bigint,
    "RP@extract#mwt" bigint,
    "bundleNumber" bigint,
    errorcode bigint,
    errordescription text,
    "filePath" text,
    "fileSizeInBytes" bigint,
    "hostName" text,
    "jobAttemptId" bigint,
    "processedDate" text,
    "sourceTypeName" text,
    "sourceTypeWithDate" text,
    status text,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX JobBundle_2i_byJbSourceTypeName ON "xnetpipeline17G"."JobBundle_p" ("sourceTypeName");
CREATE INDEX JobBundle_2i_byBundleStartTime ON "xnetpipeline17G"."JobBundle_p" ("JM@jobBundleStart#time");
CREATE INDEX JobBundle_2i_byBundleNumber ON "xnetpipeline17G"."JobBundle_p" ("bundleNumber");
CREATE INDEX JobBundle_2i_byJobAtmpId ON "xnetpipeline17G"."JobBundle_p" ("jobAttemptId");
CREATE INDEX JobBundle_2i_byJbProcessedDate ON "xnetpipeline17G"."JobBundle_p" ("processedDate");
CREATE INDEX JobBundle_2i_byStatus ON "xnetpipeline17G"."JobBundle_p" (status);
CREATE INDEX JobBundle_2i_byJbSourceTypeWithDate ON "xnetpipeline17G"."JobBundle_p" ("sourceTypeWithDate");
CREATE INDEX JobBundle_2i_byFilePath ON "xnetpipeline17G"."JobBundle_p" ("filePath");

CREATE TABLE "xnetpipeline17G"."FileEventGulla_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "bundleNumber" bigint,
    context bigint,
    "eventCount" bigint,
    "eventGullaVersion" bigint,
    "eventTypeId" bigint,
    events blob,
    "fromDateTime" bigint,
    "jobAttemptId" bigint,
    "processedDate" text,
    "processedTime" bigint,
    "sourceTypeWithDate" text,
    "threadID" bigint,
    "toDateTime" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX FileEventGulla_2i_byBundleNumber ON "xnetpipeline17G"."FileEventGulla_p" ("bundleNumber");
CREATE INDEX FileEventGulla_2i_byFegSourceTypeWithDate ON "xnetpipeline17G"."FileEventGulla_p" ("sourceTypeWithDate");
CREATE INDEX FileEventGulla_2i_byGullaJobAttemptId ON "xnetpipeline17G"."FileEventGulla_p" ("jobAttemptId");
CREATE INDEX FileEventGulla_2i_byFegProcessedDate ON "xnetpipeline17G"."FileEventGulla_p" ("processedDate");
CREATE INDEX FileEventGulla_2i_byProcessedTime ON "xnetpipeline17G"."FileEventGulla_p" ("processedTime");

CREATE TABLE "xnetpipeline17G"."Context_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G"."SourceStatus_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G"."JobManager_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G"."FileEventGulla_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G".id_allocation (
    partition_hash int,
    dc_id smallint,
    member_id bigint,
    PRIMARY KEY ((partition_hash, dc_id))
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G"."JobManager_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "fromDateTime" bigint,
    "jobManagerClass" text,
    "toDateTime" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G"."FileSource_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G"."Context_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "fromDateTime" bigint,
    "locationId" bigint,
    "processedDate" text,
    "programTypeId" bigint,
    purpose text,
    "toDateTime" bigint,
    "userName" text,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX Context_2i_byContextId ON "xnetpipeline17G"."Context_p" (xid);

CREATE TABLE "xnetpipeline17G"."SourceStatus_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "fromDateTime" bigint,
    "sourceId" bigint,
    status text,
    "toDateTime" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G"."JobAttemptStatus_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G"."Job_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G"."SyncSource_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G"."Job_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "fromDateTime" bigint,
    "processedDate" text,
    "sourceId" bigint,
    "sourceTypeWithDate" text,
    "toDateTime" bigint,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX Job_2i_byJProcessedDate ON "xnetpipeline17G"."Job_p" ("processedDate");
CREATE INDEX Job_2i_byJobId ON "xnetpipeline17G"."Job_p" (xid);
CREATE INDEX Job_2i_byJSourceTypeWithDate ON "xnetpipeline17G"."Job_p" ("sourceTypeWithDate");

CREATE TABLE "xnetpipeline17G"."JobAttemptStatus_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "fromDateTime" bigint,
    status text,
    "toDateTime" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G"."SyncSource_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "filePath" text,
    "fileSizeInBytes" bigint,
    "fromDateTime" bigint,
    "processedDate" text,
    "sourceTypeName" text,
    "sourceTypeWithDate" text,
    "toDateTime" bigint,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX SyncSource_2i_bySsProcessedDate ON "xnetpipeline17G"."SyncSource_p" ("processedDate");
CREATE INDEX SyncSource_2i_bySourcetypeName ON "xnetpipeline17G"."SyncSource_p" ("sourceTypeName");
CREATE INDEX SyncSource_2i_bySsSourceTypeWithDate ON "xnetpipeline17G"."SyncSource_p" ("sourceTypeWithDate");

CREATE TABLE "xnetpipeline17G"."ImsSyncSource_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G"."Source_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G"."FileReceived_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G"."ServiceSource_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G"."BaccQueryRegister_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "filePath" text,
    "rduName" text,
    "sourceEventTime" bigint,
    "sourceId" bigint,
    "sourceProcessTime" bigint,
    "sourceSyncTime" bigint,
    system text,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G"."FileReceived_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "destinationFilePath" text,
    "fromDateTime" bigint,
    "jobAttemptId" bigint,
    "processedDate" text,
    "sourceFilePath" text,
    "sourceTypeWithDate" text,
    "toDateTime" bigint,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX FileReceived_2i_byFrProcessedDate ON "xnetpipeline17G"."FileReceived_p" ("processedDate");
CREATE INDEX FileReceived_2i_byDestinationFilePath ON "xnetpipeline17G"."FileReceived_p" ("destinationFilePath");
CREATE INDEX FileReceived_2i_byJbAttemptId ON "xnetpipeline17G"."FileReceived_p" ("jobAttemptId");
CREATE INDEX FileReceived_2i_byFrSourceTypeWithDate ON "xnetpipeline17G"."FileReceived_p" ("sourceTypeWithDate");
CREATE INDEX FileReceived_2i_bySourceFilePath ON "xnetpipeline17G"."FileReceived_p" ("sourceFilePath");

CREATE TABLE "xnetpipeline17G"."Source_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "filePath" text,
    "fileSizeInBytes" bigint,
    "fromDateTime" bigint,
    "processedDate" text,
    "sourceTypeName" text,
    "sourceTypeWithDate" text,
    "toDateTime" bigint,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G"."BaccQueryRegister_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G"."SourceType_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetpipeline17G"."JobPlan_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE KEYSPACE xnetpipeline_pvt WITH replication = {'class': 'LocalStrategy'}  AND durable_writes = true;

CREATE KEYSPACE xnetpipeline1_system WITH replication = {'class': 'NetworkTopologyStrategy', 'Chicago': '5'}  AND durable_writes = true;

CREATE TABLE xnetpipeline1_system.shared_data (
    dataspace text,
    valid_until timeuuid,
    namespace text,
    name text,
    boolean boolean,
    double double,
    duration bigint,
    enum text,
    float float,
    inet inet,
    instant timestamp,
    integer int,
    json blob,
    last_updated timeuuid static,
    long bigint,
    string text,
    type text,
    written_on timeuuid,
    PRIMARY KEY (dataspace, valid_until, namespace, name)
) WITH CLUSTERING ORDER BY (valid_until ASC, namespace ASC, name ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = '65c6eac1-c993-11e7-81c6-e1d044565569'
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1_system.shared_data_versions (
    node inet PRIMARY KEY,
    version uuid
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline1_system.dseg_variables (
    key text PRIMARY KEY,
    type text,
    value text
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE KEYSPACE xnetgraph1_pvt WITH replication = {'class': 'LocalStrategy'}  AND durable_writes = true;

CREATE KEYSPACE "xnetgraph17G" WITH replication = {'class': 'NetworkTopologyStrategy', 'Chicago': '3'}  AND durable_writes = true;

CREATE TABLE "xnetgraph17G"."BDCREF_e" (
    "BDC" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BDC", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BPGREF_e" (
    "BPG" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BPG", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BDYREF_e" (
    "BDY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BDY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BCDREF_p" (
    "BCD" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BCD", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."MAC_e" (
    "PKEY" bigint,
    "macAddressCKEY" bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~CDT" bigint,
    "~CEID" bigint,
    "~CFL" boolean,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~DEID" bigint,
    "~DPDT" bigint,
    "~DSER" int,
    "~DSTP" int,
    "~EDAY" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~UEID" bigint,
    "~UPDT" bigint,
    "~USTP" int,
    "~inHash" bigint,
    "~outHash" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "macAddressCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("macAddressCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BDC_p" (
    "PKEY" bigint,
    "BDCCKEY" bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BDC" text,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BDCCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BDCCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BMA_p" (
    "PKEY" bigint,
    "BMACKEY" bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BMA" text,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BMACKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BMACKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BRL_e" (
    "PKEY" bigint,
    "BRLCKEY" bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~CDT" bigint,
    "~CEID" bigint,
    "~CFL" boolean,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~DEID" bigint,
    "~DPDT" bigint,
    "~DSER" int,
    "~DSTP" int,
    "~EDAY" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~UEID" bigint,
    "~UPDT" bigint,
    "~USTP" int,
    "~inHash" bigint,
    "~outHash" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BRLCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BRLCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."TN_p" (
    "PKEY" bigint,
    "tnCKEY" bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    tenancy text,
    tn text,
    "~CDT" bigint,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~UEID" bigint,
    "~UPDT" bigint,
    "~USTP" int,
    "~propHash" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "tnCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("tnCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BDM_p" (
    "PKEY" bigint,
    "BDMCKEY" bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BDM" text,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BDMCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BDMCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BDC_e" (
    "PKEY" bigint,
    "BDCCKEY" bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~CDT" bigint,
    "~CEID" bigint,
    "~CFL" boolean,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~DEID" bigint,
    "~DPDT" bigint,
    "~DSER" int,
    "~DSTP" int,
    "~EDAY" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~UEID" bigint,
    "~UPDT" bigint,
    "~USTP" int,
    "~inHash" bigint,
    "~outHash" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BDCCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BDCCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."MAC_p" (
    "PKEY" bigint,
    "macAddressCKEY" bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "macAddress" text,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "macAddressCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("macAddressCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BRS_p" (
    "PKEY" bigint,
    "BRSCKEY" bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BRS" text,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BRSCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BRSCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BDCREF_p" (
    "BDC" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BDC", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BCDREF_e" (
    "BCD" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BCD", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BDYREF_p" (
    "BDY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BDY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BRL_p" (
    "PKEY" bigint,
    "BRLCKEY" bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BRL" text,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BRLCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BRLCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BDM_e" (
    "PKEY" bigint,
    "BDMCKEY" bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~CDT" bigint,
    "~CEID" bigint,
    "~CFL" boolean,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~DEID" bigint,
    "~DPDT" bigint,
    "~DSER" int,
    "~DSTP" int,
    "~EDAY" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~UEID" bigint,
    "~UPDT" bigint,
    "~USTP" int,
    "~inHash" bigint,
    "~outHash" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BDMCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BDMCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BRS_e" (
    "PKEY" bigint,
    "BRSCKEY" bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~CDT" bigint,
    "~CEID" bigint,
    "~CFL" boolean,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~DEID" bigint,
    "~DPDT" bigint,
    "~DSER" int,
    "~DSTP" int,
    "~EDAY" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~UEID" bigint,
    "~UPDT" bigint,
    "~USTP" int,
    "~inHash" bigint,
    "~outHash" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BRSCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BRSCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BMA_e" (
    "PKEY" bigint,
    "BMACKEY" bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~CDT" bigint,
    "~CEID" bigint,
    "~CFL" boolean,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~DEID" bigint,
    "~DPDT" bigint,
    "~DSER" int,
    "~DSTP" int,
    "~EDAY" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~UEID" bigint,
    "~UPDT" bigint,
    "~USTP" int,
    "~inHash" bigint,
    "~outHash" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BMACKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BMACKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BRD_e" (
    "PKEY" bigint,
    "BRDCKEY" bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~CDT" bigint,
    "~CEID" bigint,
    "~CFL" boolean,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~DEID" bigint,
    "~DPDT" bigint,
    "~DSER" int,
    "~DSTP" int,
    "~EDAY" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~UEID" bigint,
    "~UPDT" bigint,
    "~USTP" int,
    "~inHash" bigint,
    "~outHash" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BRDCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BRDCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BBF_e" (
    "PKEY" bigint,
    "BBFCKEY" bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~CDT" bigint,
    "~CEID" bigint,
    "~CFL" boolean,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~DEID" bigint,
    "~DPDT" bigint,
    "~DSER" int,
    "~DSTP" int,
    "~EDAY" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~UEID" bigint,
    "~UPDT" bigint,
    "~USTP" int,
    "~inHash" bigint,
    "~outHash" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BBFCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BBFCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BFQ_e" (
    "PKEY" bigint,
    "BFQCKEY" bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~CDT" bigint,
    "~CEID" bigint,
    "~CFL" boolean,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~DEID" bigint,
    "~DPDT" bigint,
    "~DSER" int,
    "~DSTP" int,
    "~EDAY" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~UEID" bigint,
    "~UPDT" bigint,
    "~USTP" int,
    "~inHash" bigint,
    "~outHash" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BFQCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BFQCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BRDREF_p" (
    "BRD" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BRD", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BPSREF_p" (
    "BPS" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BPS", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BBFREF_p" (
    "BBF" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BBF", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BCOREF_e" (
    "BCO" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BCO", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BRSREF_e" (
    "BRS" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BRS", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."ACCT_p" (
    "PKEY" bigint,
    "accountNumberCKEY" bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "accountNumber" text,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "accountNumberCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("accountNumberCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BPS_p" (
    "PKEY" bigint,
    "BPSCKEY" bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BPS" text,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BPSCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BPSCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."J_BACCCOS_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BCO" text,
    "BDY" text,
    "BRA" text,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX J_BACCCOS_2i_byBACCCOS_BDY ON "xnetgraph17G"."J_BACCCOS_p" ("BDY");
CREATE INDEX J_BACCCOS_2i_byBACCCOS_BRA ON "xnetgraph17G"."J_BACCCOS_p" ("BRA");
CREATE INDEX J_BACCCOS_2i_byBACCCOS_BCO ON "xnetgraph17G"."J_BACCCOS_p" ("BCO");

CREATE TABLE "xnetgraph17G"."BFQ_p" (
    "PKEY" bigint,
    "BFQCKEY" bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BFQ" text,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BFQCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BFQCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BSY_p" (
    "PKEY" bigint,
    "BSYCKEY" bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BSD" text,
    "BSY" text,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CDT" bigint,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~UEID" bigint,
    "~UPDT" bigint,
    "~USTP" int,
    "~propHash" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BSYCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BSYCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BRDREF_e" (
    "BRD" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BRD", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BBF_p" (
    "PKEY" bigint,
    "BBFCKEY" bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BBF" text,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BBFCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BBFCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G".id_allocation (
    partition_hash int,
    dc_id smallint,
    member_id bigint,
    PRIMARY KEY ((partition_hash, dc_id))
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BST_e" (
    "PKEY" bigint,
    "BSTCKEY" bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~CDT" bigint,
    "~CEID" bigint,
    "~CFL" boolean,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~DEID" bigint,
    "~DPDT" bigint,
    "~DSER" int,
    "~DSTP" int,
    "~EDAY" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~UEID" bigint,
    "~UPDT" bigint,
    "~USTP" int,
    "~inHash" bigint,
    "~outHash" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BSTCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BSTCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."ACCT_e" (
    "PKEY" bigint,
    "accountNumberCKEY" bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "accountNumberCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("accountNumberCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BCO_e" (
    "PKEY" bigint,
    "BCOCKEY" bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~CDT" bigint,
    "~CEID" bigint,
    "~CFL" boolean,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~DEID" bigint,
    "~DPDT" bigint,
    "~DSER" int,
    "~DSTP" int,
    "~EDAY" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~UEID" bigint,
    "~UPDT" bigint,
    "~USTP" int,
    "~inHash" bigint,
    "~outHash" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BCOCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BCOCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BCOREF_p" (
    "BCO" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BCO", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BDTREF_p" (
    "BDT" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BDT", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BDMREF_p" (
    "BDM" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BDM", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BST_p" (
    "PKEY" bigint,
    "BSTCKEY" bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BST" text,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BSTCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BSTCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BPG_p" (
    "PKEY" bigint,
    "BPGCKEY" bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BPG" text,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BPGCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BPGCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BED_e" (
    "PKEY" bigint,
    "BEDCKEY" bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~CDT" bigint,
    "~CEID" bigint,
    "~CFL" boolean,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~DEID" bigint,
    "~DPDT" bigint,
    "~DSER" int,
    "~DSTP" int,
    "~EDAY" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~UEID" bigint,
    "~UPDT" bigint,
    "~USTP" int,
    "~inHash" bigint,
    "~outHash" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BEDCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BEDCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."JBMR_p" (
    "PKEY" bigint,
    "JBMRCKEY" bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BMR" text,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "processMinute" bigint,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "JBMRCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("JBMRCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BRA_p" (
    "PKEY" bigint,
    "BRACKEY" bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BRA" text,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BRACKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BRACKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BDT_p" (
    "PKEY" bigint,
    "BDTCKEY" bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BDT" text,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BDTCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BDTCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BCD_p" (
    "PKEY" bigint,
    "BCDCKEY" bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BCD" text,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BCDCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BCDCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BPG_e" (
    "PKEY" bigint,
    "BPGCKEY" bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~CDT" bigint,
    "~CEID" bigint,
    "~CFL" boolean,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~DEID" bigint,
    "~DPDT" bigint,
    "~DSER" int,
    "~DSTP" int,
    "~EDAY" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~UEID" bigint,
    "~UPDT" bigint,
    "~USTP" int,
    "~inHash" bigint,
    "~outHash" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BPGCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BPGCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BSY_e" (
    "PKEY" bigint,
    "BSYCKEY" bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~CDT" bigint,
    "~CEID" bigint,
    "~CFL" boolean,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~DEID" bigint,
    "~DPDT" bigint,
    "~DSER" int,
    "~DSTP" int,
    "~EDAY" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~UEID" bigint,
    "~UPDT" bigint,
    "~USTP" int,
    "~inHash" bigint,
    "~outHash" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BSYCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BSYCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BSTREF_p" (
    "BST" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BST", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BDTREF_e" (
    "BDT" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BDT", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BDMREF_e" (
    "BDM" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BDM", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BRA_e" (
    "PKEY" bigint,
    "BRACKEY" bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~CDT" bigint,
    "~CEID" bigint,
    "~CFL" boolean,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~DEID" bigint,
    "~DPDT" bigint,
    "~DSER" int,
    "~DSTP" int,
    "~EDAY" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~UEID" bigint,
    "~UPDT" bigint,
    "~USTP" int,
    "~inHash" bigint,
    "~outHash" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BRACKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BRACKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BPSREF_e" (
    "BPS" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BPS", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BCD_e" (
    "PKEY" bigint,
    "BCDCKEY" bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~CDT" bigint,
    "~CEID" bigint,
    "~CFL" boolean,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~DEID" bigint,
    "~DPDT" bigint,
    "~DSER" int,
    "~DSTP" int,
    "~EDAY" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~UEID" bigint,
    "~UPDT" bigint,
    "~USTP" int,
    "~inHash" bigint,
    "~outHash" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BCDCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BCDCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BDT_e" (
    "PKEY" bigint,
    "BDTCKEY" bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~CDT" bigint,
    "~CEID" bigint,
    "~CFL" boolean,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~DEID" bigint,
    "~DPDT" bigint,
    "~DSER" int,
    "~DSTP" int,
    "~EDAY" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~UEID" bigint,
    "~UPDT" bigint,
    "~USTP" int,
    "~inHash" bigint,
    "~outHash" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BDTCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BDTCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BED_p" (
    "PKEY" bigint,
    "BEDCKEY" bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BED" text,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BEDCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BEDCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."JBMR_e" (
    "PKEY" bigint,
    "JBMRCKEY" bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "JBMRCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("JBMRCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BRD_p" (
    "PKEY" bigint,
    "BRDCKEY" bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BRD" text,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BRDCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BRDCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BRAREF_p" (
    "BRA" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BRA", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BRLREF_e" (
    "BRL" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BRL", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BPS_e" (
    "PKEY" bigint,
    "BPSCKEY" bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~CDT" bigint,
    "~CEID" bigint,
    "~CFL" boolean,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~DEID" bigint,
    "~DPDT" bigint,
    "~DSER" int,
    "~DSTP" int,
    "~EDAY" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~UEID" bigint,
    "~UPDT" bigint,
    "~USTP" int,
    "~inHash" bigint,
    "~outHash" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BPSCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BPSCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BDY_e" (
    "PKEY" bigint,
    "BDYCKEY" bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~CDT" bigint,
    "~CEID" bigint,
    "~CFL" boolean,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~DEID" bigint,
    "~DPDT" bigint,
    "~DSER" int,
    "~DSTP" int,
    "~EDAY" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~UEID" bigint,
    "~UPDT" bigint,
    "~USTP" int,
    "~inHash" bigint,
    "~outHash" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BDYCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BDYCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BMR_p" (
    "PKEY" bigint,
    "BMRCKEY" bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "200" text,
    "201" text,
    "202" text,
    "203" text,
    "204" text,
    "205" text,
    "BCA" text,
    "BCC" text,
    "BCI" text,
    "BCL" text,
    "BDI" text,
    "BDP" text,
    "BDS" text,
    "BEC" text,
    "BHL" text,
    "BHT" text,
    "BHV" text,
    "BI6" text,
    "BLE" bigint,
    "BLO" text,
    "BLS" bigint,
    "BMR" text,
    "BMT" text,
    "BOR" text,
    "BPC" text,
    "BRC" text,
    "BRR" text,
    "BRV" text,
    "BSL" text,
    "BSV" text,
    "BVE" text,
    "BVN" text,
    "BVO" text,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CDT" bigint,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~UEID" bigint,
    "~UPDT" bigint,
    "~USTP" int,
    "~propHash" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BMRCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BMRCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BRLREF_p" (
    "BRL" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BRL", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."J_BACCCOS_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~CDT" bigint,
    "~CEID" bigint,
    "~CFL" boolean,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~DEID" bigint,
    "~DPDT" bigint,
    "~DSER" int,
    "~DSTP" int,
    "~EDAY" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~UEID" bigint,
    "~UPDT" bigint,
    "~USTP" int,
    "~inHash" bigint,
    "~outHash" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE MATERIALIZED VIEW "xnetgraph17G"."J_BACCCOS_e_OUT_byBACCCOS_BDY_TDT_e" AS
    SELECT *
    FROM "xnetgraph17G"."J_BACCCOS_e"
    WHERE community_id IS NOT NULL AND member_id IS NOT NULL AND "~~edge_label_id" = 65784 AND "~TDT" IS NOT NULL AND "~~adjacent_vertex_id" IS NOT NULL AND "~~adjacent_label_id" IS NOT NULL AND "~~edge_id" IS NOT NULL
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~TDT", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
    WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~TDT" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE MATERIALIZED VIEW "xnetgraph17G"."J_BACCCOS_e_OUT_byBACCCOS_BCO_TDT_e" AS
    SELECT *
    FROM "xnetgraph17G"."J_BACCCOS_e"
    WHERE community_id IS NOT NULL AND member_id IS NOT NULL AND "~~edge_label_id" = 65782 AND "~TDT" IS NOT NULL AND "~~adjacent_vertex_id" IS NOT NULL AND "~~adjacent_label_id" IS NOT NULL AND "~~edge_id" IS NOT NULL
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~TDT", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
    WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~TDT" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE MATERIALIZED VIEW "xnetgraph17G"."J_BACCCOS_e_OUT_byBACCCOS_BRA_FDT_e" AS
    SELECT *
    FROM "xnetgraph17G"."J_BACCCOS_e"
    WHERE community_id IS NOT NULL AND member_id IS NOT NULL AND "~~edge_label_id" = 65786 AND "~FDT" IS NOT NULL AND "~~adjacent_vertex_id" IS NOT NULL AND "~~adjacent_label_id" IS NOT NULL AND "~~edge_id" IS NOT NULL
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~FDT", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
    WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~FDT" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE MATERIALIZED VIEW "xnetgraph17G"."J_BACCCOS_e_OUT_byBACCCOS_BRA_TDT_e" AS
    SELECT *
    FROM "xnetgraph17G"."J_BACCCOS_e"
    WHERE community_id IS NOT NULL AND member_id IS NOT NULL AND "~~edge_label_id" = 65786 AND "~TDT" IS NOT NULL AND "~~adjacent_vertex_id" IS NOT NULL AND "~~adjacent_label_id" IS NOT NULL AND "~~edge_id" IS NOT NULL
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~TDT", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
    WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~TDT" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE MATERIALIZED VIEW "xnetgraph17G"."J_BACCCOS_e_OUT_byBACCCOS_BDY_FDT_e" AS
    SELECT *
    FROM "xnetgraph17G"."J_BACCCOS_e"
    WHERE community_id IS NOT NULL AND member_id IS NOT NULL AND "~~edge_label_id" = 65784 AND "~FDT" IS NOT NULL AND "~~adjacent_vertex_id" IS NOT NULL AND "~~adjacent_label_id" IS NOT NULL AND "~~edge_id" IS NOT NULL
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~FDT", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
    WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~FDT" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE MATERIALIZED VIEW "xnetgraph17G"."J_BACCCOS_e_OUT_byBACCCOS_BCO_FDT_e" AS
    SELECT *
    FROM "xnetgraph17G"."J_BACCCOS_e"
    WHERE community_id IS NOT NULL AND member_id IS NOT NULL AND "~~edge_label_id" = 65782 AND "~FDT" IS NOT NULL AND "~~adjacent_vertex_id" IS NOT NULL AND "~~adjacent_label_id" IS NOT NULL AND "~~edge_id" IS NOT NULL
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~FDT", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
    WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~FDT" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BRSREF_p" (
    "BRS" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BRS", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BRAREF_e" (
    "BRA" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BRA", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BEDREF_p" (
    "BED" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BED", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BBFREF_e" (
    "BBF" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BBF", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BCO_p" (
    "PKEY" bigint,
    "BCOCKEY" bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BCO" text,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BCOCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BCOCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BMR_e" (
    "PKEY" bigint,
    "BMRCKEY" bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~CDT" bigint,
    "~CEID" bigint,
    "~CFL" boolean,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~DEID" bigint,
    "~DPDT" bigint,
    "~DSER" int,
    "~DSTP" int,
    "~EDAY" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~UEID" bigint,
    "~UPDT" bigint,
    "~USTP" int,
    "~inHash" bigint,
    "~outHash" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BMRCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BMRCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BDY_p" (
    "PKEY" bigint,
    "BDYCKEY" bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BDY" text,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BDYCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BDYCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BSTREF_e" (
    "BST" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BST", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BEDREF_e" (
    "BED" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BED", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."TN_e" (
    "PKEY" bigint,
    "tnCKEY" bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "tnCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("tnCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE "xnetgraph17G"."BPGREF_p" (
    "BPG" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "CDT" bigint,
    "CEID" bigint,
    "CPDT" bigint,
    "CSER" int,
    "CSTP" int,
    "DEID" bigint,
    "DPDT" bigint,
    "DSTP" int,
    "FDT" bigint,
    "TDT" bigint,
    "UEID" bigint,
    "UPDT" bigint,
    "USTP" int,
    "VER" int,
    "VERS" int,
    "~CEID" bigint,
    "~CPDT" bigint,
    "~CSER" int,
    "~CSTP" int,
    "~FDT" bigint,
    "~TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BPG", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE KEYSPACE xnetgraph WITH replication = {'class': 'NetworkTopologyStrategy', 'Chicago': '3', 'Potomac': '3'}  AND durable_writes = true;

CREATE TABLE xnetgraph."PsProvisionedCosRef_p" (
    "pSCos" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("pSCos", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."ACCT_STATUS_p" (
    "PKEY" text,
    "accountStatusCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "accountStatus" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "accountStatusCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("accountStatusCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BCDREF_p" (
    "BCD" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BCD", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."MAC_e" (
    "PKEY" text,
    "macAddressCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "macAddressCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("macAddressCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BRD_e" (
    "PKEY" text,
    "BRDCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BRDCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BRDCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSEdvaMac_e" (
    "PKEY" text,
    "imsEdvaMacCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "imsEdvaMacCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("imsEdvaMacCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BLSYSTEM_e" (
    "PKEY" text,
    "billingSystemCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "billingSystemCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("billingSystemCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BRD_p" (
    "PKEY" text,
    "BRDCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BRD" text,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BRDCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BRDCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."MAC_p" (
    "PKEY" text,
    "macAddressCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "macAddress" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "macAddressCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("macAddressCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSEdvaMac_p" (
    "PKEY" text,
    "imsEdvaMacCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "imsEdvaMac" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "imsEdvaMacCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("imsEdvaMacCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BCDREF_e" (
    "BCD" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BCD", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."ACCT_STATUS_e" (
    "PKEY" text,
    "accountStatusCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "accountStatusCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("accountStatusCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BLSYSTEM_p" (
    "PKEY" text,
    "billingSystemCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "billingSystem" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "billingSystemCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("billingSystemCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSEdvaPort_e" (
    "PKEY" text,
    "imsEdvaPortCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "imsEdvaPortCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("imsEdvaPortCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSHSID_e" (
    "PKEY" text,
    "imsSIDCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "imsSIDCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("imsSIDCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BBF_e" (
    "PKEY" text,
    "BBFCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BBFCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BBFCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PsDerivedRduRef_p" (
    "pSRdu" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("pSRdu", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BL_DEV_MAC_e" (
    "PKEY" text,
    "deviceMacAddressCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "deviceMacAddressCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("deviceMacAddressCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BACTMAC_p" (
    "PKEY" text,
    "acctNumMacCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "acctNumMac" text,
    "deviceSerialNumber" text,
    "deviceType" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "acctNumMacCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("acctNumMacCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BCOREF_e" (
    "BCO" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BCO", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PsEquipDevicetypeRef_e" (
    "pSDeviceType" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("pSDeviceType", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BRLREF_p" (
    "BRL" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BRL", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BACTMAC_e" (
    "PKEY" text,
    "acctNumMacCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "acctNumMacCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("acctNumMacCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BL_DEV_MAC_p" (
    "PKEY" text,
    "deviceMacAddressCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "deviceMacAddress" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "deviceMacAddressCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("deviceMacAddressCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSHSID_p" (
    "PKEY" text,
    "imsSIDCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "imsAkFlag" text,
    "imsAliasId" text,
    "imsAliasServiceId" text,
    "imsAsId" text,
    "imsAsName" text,
    "imsAuthRegFlag" text,
    "imsBarringIndication" text,
    "imsBaseDataTs" text,
    "imsBaseDataTsExt" text,
    "imsCaveAkaFlag" text,
    "imsChrgTplId" text,
    "imsConsFlg" text,
    "imsCurrentScscFname" text,
    "imsCxDataTs" text,
    "imsCxDataTsExt" text,
    "imsDataReference" text,
    "imsDefaultImpu" text,
    "imsDnAliasId" text,
    "imsDnHsidInfoRdn" text,
    "imsDnImpuInformation" text,
    "imsDnOperation" text,
    "imsDnPi" text,
    "imsDnPublic" text,
    "imsDnPublicId" text,
    "imsDnServiceInd" text,
    "imsDnSifcsetId" text,
    "imsDnSip" text,
    "imsDnSpId" text,
    "imsDnSubscriberId" text,
    "imsEaaFlag" text,
    "imsEiaFlag" text,
    "imsEsrn" text,
    "imsExpiryTime" text,
    "imsGiDataTs" text,
    "imsGiDataTsExt" text,
    "imsHdAsFlag" text,
    "imsHdaInfoRdn" text,
    "imsHsIdInfoRdn" text,
    "imsHssIndex" text,
    "imsImpiAuthInfoRdn" text,
    "imsImpiInfoRdn" text,
    "imsImpuInfoRdn" text,
    "imsImpuType" text,
    "imsImsRoamId" text,
    "imsIrsId" text,
    "imsLooseRouteIndication" text,
    "imsMediaProfileId" text,
    "imsNassRoamId" text,
    "imsNbaFlag" text,
    "imsNnaFlag" text,
    "imsNncFlag" text,
    "imsPassword" text,
    "imsPbxUserFlag" text,
    "imsPendingFlag" text,
    "imsPi" text,
    "imsPiid" text,
    "imsPuid" text,
    "imsRealm" text,
    "imsReassignmentPendingFlag" text,
    "imsRefNumber" text,
    "imsRepositDataTs" text,
    "imsRepositDataTsExt" text,
    "imsSID" text,
    "imsSIDsNameInformation" text,
    "imsScscHost" text,
    "imsScscRealm" text,
    "imsSdaFlag" text,
    "imsServiceInd" text,
    "imsServiceProviderId" text,
    "imsShDataTs" text,
    "imsShDataTsExt" text,
    "imsShared" text,
    "imsSifCseTid" text,
    "imsSpId" text,
    "imsStdPriority" text,
    "imsSubId" text,
    "imsSubIdInformation" text,
    "imsSubInfo" text,
    "imsSubInfoRdn" text,
    "imsTenancy" text,
    "imsTimeStamp" text,
    "imsUserName" text,
    "imsUserPriority" text,
    "imsUserState" text,
    "imsUserType" text,
    "imsVncFlag" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "imsSIDCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("imsSIDCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BBF_p" (
    "PKEY" text,
    "BBFCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BBF" text,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BBFCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BBFCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PsEquipDevicetypeRef_p" (
    "pSDeviceType" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("pSDeviceType", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BRLREF_e" (
    "BRL" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BRL", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."VMMobileNPA_p" (
    "PKEY" text,
    "vmMobileNPACKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "vmMobileNPA" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "vmMobileNPACKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("vmMobileNPACKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BCOREF_p" (
    "BCO" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BCO", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."VMNPA_e" (
    "PKEY" text,
    "vmNPACKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "vmNPACKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("vmNPACKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSACCTTN_p" (
    "PKEY" text,
    "pSACCTTNCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "pSACCTTN" text,
    "pSCmsDqosProfile" text,
    "pSCmsFqdn" text,
    "pSCmsMtaProfile" text,
    "pSCmsRateCenter" text,
    "pSCmsSecurityProfile" text,
    "pSCmsServType" text,
    "pSDeleteFlag" text,
    "pSDisplayName" text,
    "pSFeaturesEnabled" text,
    "pSLineAdminState" text,
    "pSLpicProvider" text,
    "pSLrn" text,
    "pSPicProvider" text,
    "pSPortingStatus" text,
    "pSVmAdminState" text,
    "pSVmCos" text,
    "pSVmEmail" text,
    "pSVmMsgStoreServer" text,
    "pSVmName" text,
    "pSVmServerFqdn" text,
    "pSVmType" text,
    "pSrecChangeDate" text,
    "pSrecCreateDate" text,
    "pSsourceRequestTimestamp" text,
    "psRequestId" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "pSACCTTNCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("pSACCTTNCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BED_e" (
    "PKEY" text,
    "BEDCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BEDCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BEDCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSDeviceStatus_p" (
    "PKEY" text,
    "pSDeviceStatusCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "pSDeviceStatus" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "pSDeviceStatusCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("pSDeviceStatusCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BDT_p" (
    "PKEY" text,
    "BDTCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BDT" text,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BDTCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BDTCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."VMNPA_p" (
    "PKEY" text,
    "vmNPACKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "vmNPA" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "vmNPACKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("vmNPACKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BDT_e" (
    "PKEY" text,
    "BDTCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BDTCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BDTCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BED_p" (
    "PKEY" text,
    "BEDCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BED" text,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BEDCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BEDCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSDeviceStatus_e" (
    "PKEY" text,
    "pSDeviceStatusCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "pSDeviceStatusCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("pSDeviceStatusCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSEdvaPort_p" (
    "PKEY" text,
    "imsEdvaPortCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "imsEdvaPort" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "imsEdvaPortCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("imsEdvaPortCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSNPA_e" (
    "PKEY" text,
    "imsNPACKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "imsNPACKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("imsNPACKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSDomain_e" (
    "PKEY" text,
    "imsDomainCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "imsDomainCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("imsDomainCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSFSbr1_e" (
    "PKEY" text,
    "imsFeatureSbr1SidCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "imsFeatureSbr1SidCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("imsFeatureSbr1SidCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BEDREF_p" (
    "BED" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BED", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BCO_e" (
    "PKEY" text,
    "BCOCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BCOCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BCOCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSNPA_p" (
    "PKEY" text,
    "imsNPACKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "imsNPA" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "imsNPACKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("imsNPACKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSDomain_p" (
    "PKEY" text,
    "imsDomainCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "imsDomain" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "imsDomainCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("imsDomainCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BEDREF_e" (
    "BED" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BED", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSTN_e" (
    "PKEY" text,
    "pSTNCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "pSTNCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("pSTNCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSFSbr1_p" (
    "PKEY" text,
    "imsFeatureSbr1SidCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "imsFeatureSbr1Ctda" text,
    "imsFeatureSbr1Ctintnanp" text,
    "imsFeatureSbr1Ctintworld" text,
    "imsFeatureSbr1Ctlco" text,
    "imsFeatureSbr1Ctlct" text,
    "imsFeatureSbr1Ctld" text,
    "imsFeatureSbr1Ctosm" text,
    "imsFeatureSbr1Ctosp" text,
    "imsFeatureSbr1Iddci" text,
    "imsFeatureSbr1Ltci" text,
    "imsFeatureSbr1Ntci" text,
    "imsFeatureSbr1Operator" text,
    "imsFeatureSbr1Sid" text,
    "imsFeatureSbr1Supysrv" text,
    "imsFeatureSbr1UnknownCodec" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "imsFeatureSbr1SidCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("imsFeatureSbr1SidCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BDCREF_e" (
    "BDC" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BDC", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BDYREF_e" (
    "BDY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BDY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSCmsCliRef_p" (
    "pSCmsCli" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("pSCmsCli", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSHFSbr1_p" (
    "PKEY" text,
    "imsFeatureSbr1SidCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "imsFeatureSbr1Ctda" text,
    "imsFeatureSbr1Ctintnanp" text,
    "imsFeatureSbr1Ctintworld" text,
    "imsFeatureSbr1Ctlco" text,
    "imsFeatureSbr1Ctlct" text,
    "imsFeatureSbr1Ctld" text,
    "imsFeatureSbr1Ctosm" text,
    "imsFeatureSbr1Ctosp" text,
    "imsFeatureSbr1Iddci" text,
    "imsFeatureSbr1Ltci" text,
    "imsFeatureSbr1Ntci" text,
    "imsFeatureSbr1Operator" text,
    "imsFeatureSbr1Sid" text,
    "imsFeatureSbr1Supysrv" text,
    "imsFeatureSbr1UnknownCodec" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "imsFeatureSbr1SidCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("imsFeatureSbr1SidCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."J_BACCCOS_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE MATERIALIZED VIEW xnetgraph."J_BACCCOS_e_OUT_byBACCCOS_BDY_TDT_e" AS
    SELECT *
    FROM xnetgraph."J_BACCCOS_e"
    WHERE community_id IS NOT NULL AND member_id IS NOT NULL AND "~~edge_label_id" = 65752 AND "~TDT" IS NOT NULL AND "~~adjacent_vertex_id" IS NOT NULL AND "~~adjacent_label_id" IS NOT NULL AND "~~edge_id" IS NOT NULL
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~TDT", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
    WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~TDT" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE MATERIALIZED VIEW xnetgraph."J_BACCCOS_e_OUT_byBACCCOS_BCO_TDT_e" AS
    SELECT *
    FROM xnetgraph."J_BACCCOS_e"
    WHERE community_id IS NOT NULL AND member_id IS NOT NULL AND "~~edge_label_id" = 65750 AND "~TDT" IS NOT NULL AND "~~adjacent_vertex_id" IS NOT NULL AND "~~adjacent_label_id" IS NOT NULL AND "~~edge_id" IS NOT NULL
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~TDT", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
    WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~TDT" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE MATERIALIZED VIEW xnetgraph."J_BACCCOS_e_OUT_byBACCCOS_BRA_FDT_e" AS
    SELECT *
    FROM xnetgraph."J_BACCCOS_e"
    WHERE community_id IS NOT NULL AND member_id IS NOT NULL AND "~~edge_label_id" = 65754 AND "~FDT" IS NOT NULL AND "~~adjacent_vertex_id" IS NOT NULL AND "~~adjacent_label_id" IS NOT NULL AND "~~edge_id" IS NOT NULL
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~FDT", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
    WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~FDT" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE MATERIALIZED VIEW xnetgraph."J_BACCCOS_e_OUT_byBACCCOS_BRA_TDT_e" AS
    SELECT *
    FROM xnetgraph."J_BACCCOS_e"
    WHERE community_id IS NOT NULL AND member_id IS NOT NULL AND "~~edge_label_id" = 65754 AND "~TDT" IS NOT NULL AND "~~adjacent_vertex_id" IS NOT NULL AND "~~adjacent_label_id" IS NOT NULL AND "~~edge_id" IS NOT NULL
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~TDT", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
    WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~TDT" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE MATERIALIZED VIEW xnetgraph."J_BACCCOS_e_OUT_byBACCCOS_BDY_FDT_e" AS
    SELECT *
    FROM xnetgraph."J_BACCCOS_e"
    WHERE community_id IS NOT NULL AND member_id IS NOT NULL AND "~~edge_label_id" = 65752 AND "~FDT" IS NOT NULL AND "~~adjacent_vertex_id" IS NOT NULL AND "~~adjacent_label_id" IS NOT NULL AND "~~edge_id" IS NOT NULL
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~FDT", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
    WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~FDT" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE MATERIALIZED VIEW xnetgraph."J_BACCCOS_e_OUT_byBACCCOS_BCO_FDT_e" AS
    SELECT *
    FROM xnetgraph."J_BACCCOS_e"
    WHERE community_id IS NOT NULL AND member_id IS NOT NULL AND "~~edge_label_id" = 65750 AND "~FDT" IS NOT NULL AND "~~adjacent_vertex_id" IS NOT NULL AND "~~adjacent_label_id" IS NOT NULL AND "~~edge_id" IS NOT NULL
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~FDT", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
    WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~FDT" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSMacaddress_p" (
    "PKEY" text,
    "pSMacaddressCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "pSMacaddress" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "pSMacaddressCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("pSMacaddressCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BMA_p" (
    "PKEY" text,
    "BMACKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BMA" text,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BMACKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BMACKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSHEdvaMac_e" (
    "PKEY" text,
    "imsEdvaMacCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "imsEdvaMacCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("imsEdvaMacCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSHNPA_e" (
    "PKEY" text,
    "imsNPACKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "imsNPACKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("imsNPACKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSMacaddress_e" (
    "PKEY" text,
    "pSMacaddressCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "pSMacaddressCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("pSMacaddressCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BDCREF_p" (
    "BDC" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BDC", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSCmsCliRef_e" (
    "pSCmsCli" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("pSCmsCli", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BDYREF_p" (
    "BDY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BDY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSHEdvaMac_p" (
    "PKEY" text,
    "imsEdvaMacCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "imsEdvaMac" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "imsEdvaMacCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("imsEdvaMacCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BMA_e" (
    "PKEY" text,
    "BMACKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BMACKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BMACKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSHNPA_p" (
    "PKEY" text,
    "imsNPACKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "imsNPA" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "imsNPACKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("imsNPACKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSRduMac_p" (
    "PKEY" text,
    "pSRduMacCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "pSChRouterMode" text,
    "pSConfigFile" text,
    "pSDataSource" text,
    "pSDeleteFlag" text,
    "pSDhcpCriteria" text,
    "pSDnsServer" text,
    "pSDocsisVersion" text,
    "pSDomainName" text,
    "pSEquipGuid" text,
    "pSGiAddress" text,
    "pSHostName" text,
    "pSIspcode" text,
    "pSOwnerId" text,
    "pSRduMac" text,
    "pSRegionId" text,
    "pSrecChangeDate" text,
    "pSrecCreateDate" text,
    "pSsourceRequestTimestamp" text,
    "psGateway" text,
    "psRequestId" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "pSRduMacCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("pSRduMacCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."ACTSTATUSREF_p" (
    "accountStatus" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("accountStatus", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSHFSbrEx_p" (
    "PKEY" text,
    "imsFeatureSbrExSidCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "imsFeatureSbrExMcidmode" text,
    "imsFeatureSbrExNs3pty" text,
    "imsFeatureSbrExNsacrm" text,
    "imsFeatureSbrExNsasslst" text,
    "imsFeatureSbrExNscaller" text,
    "imsFeatureSbrExNsccw" text,
    "imsFeatureSbrExNscd" text,
    "imsFeatureSbrExNscdvm" text,
    "imsFeatureSbrExNscfbvm" text,
    "imsFeatureSbrExNscfnlvm" text,
    "imsFeatureSbrExNscfnrcvm" text,
    "imsFeatureSbrExNscfnrvm" text,
    "imsFeatureSbrExNscfs" text,
    "imsFeatureSbrExNscfu" text,
    "imsFeatureSbrExNscidcw" text,
    "imsFeatureSbrExNsclip" text,
    "imsFeatureSbrExNsclir" text,
    "imsFeatureSbrExNscnip" text,
    "imsFeatureSbrExNscnir" text,
    "imsFeatureSbrExNscr" text,
    "imsFeatureSbrExNscun" text,
    "imsFeatureSbrExNscw" text,
    "imsFeatureSbrExNsdstr" text,
    "imsFeatureSbrExNshold" text,
    "imsFeatureSbrExNshrcn" text,
    "imsFeatureSbrExNsmcid" text,
    "imsFeatureSbrExNsmwi" text,
    "imsFeatureSbrExNsnrid" text,
    "imsFeatureSbrExNsqsns" text,
    "imsFeatureSbrExNsrd" text,
    "imsFeatureSbrExNsrid" text,
    "imsFeatureSbrExNssb" text,
    "imsFeatureSbrExNssd1d" text,
    "imsFeatureSbrExNssd2d" text,
    "imsFeatureSbrExNssic" text,
    "imsFeatureSbrExNsss" text,
    "imsFeatureSbrExSid" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "imsFeatureSbrExSidCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("imsFeatureSbrExSidCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSCmsCli_e" (
    "PKEY" text,
    "pSCmsCliCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "pSCmsCliCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("pSCmsCliCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."ACCT_p" (
    "PKEY" text,
    "accountNumberCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "accountNumber" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "accountNumberCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("accountNumberCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."ACTSTATUSREF_e" (
    "accountStatus" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("accountStatus", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSRduMac_e" (
    "PKEY" text,
    "pSRduMacCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "pSRduMacCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("pSRduMacCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."ACCT_e" (
    "PKEY" text,
    "accountNumberCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "accountNumberCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("accountNumberCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSHFSbrEx_e" (
    "PKEY" text,
    "imsFeatureSbrExSidCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "imsFeatureSbrExSidCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("imsFeatureSbrExSidCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSCmsCli_p" (
    "PKEY" text,
    "pSCmsCliCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "pSCmsCli" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "pSCmsCliCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("pSCmsCliCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BDTREF_p" (
    "BDT" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BDT", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSMtaPort_p" (
    "PKEY" text,
    "pSMtaPortCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "pSMtaPort" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "pSMtaPortCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("pSMtaPortCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BDMREF_p" (
    "BDM" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BDM", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSFSbr_p" (
    "PKEY" text,
    "imsFeatureSbrSidCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "imsFeatureSbrAtsdtmbusy" text,
    "imsFeatureSbrCharsc" text,
    "imsFeatureSbrCig" text,
    "imsFeatureSbrCpc" text,
    "imsFeatureSbrCrbtid" text,
    "imsFeatureSbrCsc" text,
    "imsFeatureSbrDpr" text,
    "imsFeatureSbrDprf" text,
    "imsFeatureSbrDprv" text,
    "imsFeatureSbrDspidx" text,
    "imsFeatureSbrImpu" text,
    "imsFeatureSbrLmtgrp" text,
    "imsFeatureSbrLp" text,
    "imsFeatureSbrMaxparacall" text,
    "imsFeatureSbrMcidmode" text,
    "imsFeatureSbrNoat" text,
    "imsFeatureSbrPchg" text,
    "imsFeatureSbrPolidx" text,
    "imsFeatureSbrPrepaididx" text,
    "imsFeatureSbrPron" text,
    "imsFeatureSbrProna" text,
    "imsFeatureSbrPronv" text,
    "imsFeatureSbrRingcount" text,
    "imsFeatureSbrRsc" text,
    "imsFeatureSbrSid" text,
    "imsFeatureSbrSlbgrp" text,
    "imsFeatureSbrSocbptoneidx" text,
    "imsFeatureSbrStype" text,
    "imsFeatureSbrTemplateidx" text,
    "imsFeatureSbrTgrp" text,
    "imsFeatureSbrTidcw" text,
    "imsFeatureSbrTidhld" text,
    "imsFeatureSbrTimezone" text,
    "imsFeatureSbrTreat" text,
    "imsFeatureSbrUname" text,
    "imsFeatureSbrUpflag" text,
    "imsFeatureSbrUtype" text,
    "imsFeatureSbrVccflag" text,
    "imsFeatureSbrVdmaind" text,
    "imsFeatureSbrVmaind" text,
    "imsFeatureSbrVtflag" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "imsFeatureSbrSidCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("imsFeatureSbrSidCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BPG_p" (
    "PKEY" text,
    "BPGCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BPG" text,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BPGCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BPGCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BRAREF_e" (
    "BRA" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BRA", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BLSITEID_e" (
    "PKEY" text,
    "siteIdCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "siteIdCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("siteIdCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BCD_p" (
    "PKEY" text,
    "BCDCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BCD" text,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BCDCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BCDCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BPG_e" (
    "PKEY" text,
    "BPGCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BPGCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BPGCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSFSbr_e" (
    "PKEY" text,
    "imsFeatureSbrSidCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "imsFeatureSbrSidCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("imsFeatureSbrSidCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BDTREF_e" (
    "BDT" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BDT", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BDMREF_e" (
    "BDM" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BDM", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BPSREF_e" (
    "BPS" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BPS", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BCD_e" (
    "PKEY" text,
    "BCDCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BCDCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BCDCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BRAREF_p" (
    "BRA" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BRA", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BLSITEID_p" (
    "PKEY" text,
    "siteIdCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "siteId" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "siteIdCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("siteIdCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BLSYSTEMREF_p" (
    "billingSystem" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("billingSystem", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BDY_e" (
    "PKEY" text,
    "BDYCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BDYCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BDYCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BRSREF_p" (
    "BRS" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BRS", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BSY_e" (
    "PKEY" text,
    "BSYCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BSYCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BSYCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BRSREF_e" (
    "BRS" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BRS", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BDY_p" (
    "PKEY" text,
    "BDYCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BDY" text,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BDYCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BDYCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BLSYSTEMREF_e" (
    "billingSystem" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("billingSystem", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BRS_p" (
    "PKEY" text,
    "BRSCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BRS" text,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BRSCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BRSCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BDC_p" (
    "PKEY" text,
    "BDCCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BDC" text,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BDCCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BDCCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."VMTn_p" (
    "PKEY" text,
    "vmTnCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "vmBucket" text,
    "vmCorpMailAddress" text,
    "vmCos" text,
    "vmEmailAddress" text,
    "vmFirstName" text,
    "vmLastName" text,
    "vmMail" text,
    "vmMailBox" text,
    "vmNotifyService" text,
    "vmPassword" text,
    "vmPin" text,
    "vmPinTimestamp" text,
    "vmPn" text,
    "vmStatus" text,
    "vmSwitchName" text,
    "vmTenancy" text,
    "vmTimeZone" text,
    "vmTn" text,
    "vmZipCode" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "vmTnCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("vmTnCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSTN_p" (
    "PKEY" text,
    "pSTNCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "pSTN" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "pSTNCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("pSTNCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."FIB_NODE_p" (
    "PKEY" text,
    "fiberNodeCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "fiberNode" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "fiberNodeCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("fiberNodeCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSAccountStatus_e" (
    "PKEY" text,
    "pSAccountStatusCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "pSAccountStatusCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("pSAccountStatusCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSDeviceType_p" (
    "PKEY" text,
    "pSDeviceTypeCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "pSDeviceType" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "pSDeviceTypeCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("pSDeviceTypeCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BDC_e" (
    "PKEY" text,
    "BDCCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BDCCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BDCCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BL_TN_e" (
    "PKEY" text,
    "blTnCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "blTnCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("blTnCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PsProvisionedCosRef_e" (
    "pSCos" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("pSCos", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."VMTn_e" (
    "PKEY" text,
    "vmTnCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "vmTnCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("vmTnCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BRS_e" (
    "PKEY" text,
    "BRSCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BRSCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BRSCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."VMMobileNPA_e" (
    "PKEY" text,
    "vmMobileNPACKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "vmMobileNPACKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("vmMobileNPACKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSDeviceType_e" (
    "PKEY" text,
    "pSDeviceTypeCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "pSDeviceTypeCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("pSDeviceTypeCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."FIB_NODE_e" (
    "PKEY" text,
    "fiberNodeCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "fiberNodeCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("fiberNodeCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSAccountStatus_p" (
    "PKEY" text,
    "pSAccountStatusCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "pSAccountStatus" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "pSAccountStatusCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("pSAccountStatusCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSHFSbr_p" (
    "PKEY" text,
    "imsFeatureSbrSidCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "imsFeatureSbrAtsdtmbusy" text,
    "imsFeatureSbrCharsc" text,
    "imsFeatureSbrCig" text,
    "imsFeatureSbrCpc" text,
    "imsFeatureSbrCrbtid" text,
    "imsFeatureSbrCsc" text,
    "imsFeatureSbrDpr" text,
    "imsFeatureSbrDprf" text,
    "imsFeatureSbrDprv" text,
    "imsFeatureSbrDspidx" text,
    "imsFeatureSbrImpu" text,
    "imsFeatureSbrLmtgrp" text,
    "imsFeatureSbrLp" text,
    "imsFeatureSbrMaxparacall" text,
    "imsFeatureSbrMcidmode" text,
    "imsFeatureSbrNoat" text,
    "imsFeatureSbrPchg" text,
    "imsFeatureSbrPolidx" text,
    "imsFeatureSbrPrepaididx" text,
    "imsFeatureSbrPron" text,
    "imsFeatureSbrProna" text,
    "imsFeatureSbrPronv" text,
    "imsFeatureSbrRingcount" text,
    "imsFeatureSbrRsc" text,
    "imsFeatureSbrSid" text,
    "imsFeatureSbrSlbgrp" text,
    "imsFeatureSbrSocbptoneidx" text,
    "imsFeatureSbrStype" text,
    "imsFeatureSbrTemplateidx" text,
    "imsFeatureSbrTgrp" text,
    "imsFeatureSbrTidcw" text,
    "imsFeatureSbrTidhld" text,
    "imsFeatureSbrTimezone" text,
    "imsFeatureSbrTreat" text,
    "imsFeatureSbrUname" text,
    "imsFeatureSbrUpflag" text,
    "imsFeatureSbrUtype" text,
    "imsFeatureSbrVccflag" text,
    "imsFeatureSbrVdmaind" text,
    "imsFeatureSbrVmaind" text,
    "imsFeatureSbrVtflag" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "imsFeatureSbrSidCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("imsFeatureSbrSidCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BFQ_e" (
    "PKEY" text,
    "BFQCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BFQCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BFQCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BPSREF_p" (
    "BPS" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BPS", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PsDeviceStatusRef_p" (
    "pSDeviceStatus" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("pSDeviceStatus", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BFQ_p" (
    "PKEY" text,
    "BFQCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BFQ" text,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BFQCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BFQCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BSY_p" (
    "PKEY" text,
    "BSYCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BSD" text,
    "BSY" text,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BSYCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BSYCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSHFSbr_e" (
    "PKEY" text,
    "imsFeatureSbrSidCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "imsFeatureSbrSidCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("imsFeatureSbrSidCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PsDerivedRduRef_e" (
    "pSRdu" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("pSRdu", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PsDeviceStatusRef_e" (
    "pSDeviceStatus" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("pSDeviceStatus", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BPGREF_e" (
    "BPG" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BPG", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSSID_e" (
    "PKEY" text,
    "imsSIDCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "imsSIDCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("imsSIDCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."ABMR_e" (
    "PKEY" text,
    "ABMRCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "ABMRCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("ABMRCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."GROUPPROFILE_p" (
    "PKEY" text,
    "groupIdCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "callingLineIdName" text,
    "callingLineIdPhone" text,
    "defaultDomain" text,
    "groupId" text,
    "groupName" text,
    "inTrunkGroup" text,
    "timeZone" text,
    "userLimit" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "groupIdCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("groupIdCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BACTTN_p" (
    "PKEY" text,
    "acctNumTnCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "acctNumTn" text,
    "portedFlag" text,
    "primaryTnFlag" text,
    "tnStatus" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "acctNumTnCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("acctNumTnCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSHDomain_p" (
    "PKEY" text,
    "imsDomainCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "imsDomain" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "imsDomainCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("imsDomainCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."VMMobileTn_e" (
    "PKEY" text,
    "vmMobileTnCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "vmMobileTnCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("vmMobileTnCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."GROUPPROFILE_e" (
    "PKEY" text,
    "groupIdCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "groupIdCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("groupIdCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."ABMR_p" (
    "PKEY" text,
    "ABMRCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "ABMRCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("ABMRCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSAccountStatusRef_e" (
    "pSAccountStatus" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("pSAccountStatus", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BPGREF_p" (
    "BPG" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BPG", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."VMMobileTn_p" (
    "PKEY" text,
    "vmMobileTnCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "vmBucket" text,
    "vmCorpMailAddress" text,
    "vmCos" text,
    "vmEmailAddress" text,
    "vmFirstName" text,
    "vmLastName" text,
    "vmMail" text,
    "vmMailBox" text,
    "vmMobileTn" text,
    "vmNotifyService" text,
    "vmPassword" text,
    "vmPin" text,
    "vmPinTimestamp" text,
    "vmPn" text,
    "vmStatus" text,
    "vmSwitchName" text,
    "vmTenancy" text,
    "vmTimeZone" text,
    "vmZipCode" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "vmMobileTnCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("vmMobileTnCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BACTTN_e" (
    "PKEY" text,
    "acctNumTnCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "acctNumTnCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("acctNumTnCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSHDomain_e" (
    "PKEY" text,
    "imsDomainCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "imsDomainCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("imsDomainCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSFSbrEx_e" (
    "PKEY" text,
    "imsFeatureSbrExSidCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "imsFeatureSbrExSidCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("imsFeatureSbrExSidCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BLSITEIDREF_p" (
    "siteId" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("siteId", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BPS_e" (
    "PKEY" text,
    "BPSCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BPSCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BPSCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BMR_p" (
    "PKEY" text,
    "BMRCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "200" text,
    "201" text,
    "202" text,
    "203" text,
    "204" text,
    "205" text,
    "BCA" text,
    "BCC" text,
    "BCI" text,
    "BCL" text,
    "BDI" text,
    "BDP" text,
    "BDS" text,
    "BEC" text,
    "BHL" text,
    "BHT" text,
    "BHV" text,
    "BI6" text,
    "BLE" bigint,
    "BLO" text,
    "BLS" bigint,
    "BMR" text,
    "BMT" text,
    "BOR" text,
    "BPC" text,
    "BRC" text,
    "BRR" text,
    "BRV" text,
    "BSL" text,
    "BSV" text,
    "BVE" text,
    "BVN" text,
    "BVO" text,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BMRCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BMRCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSMtaPortRef_e" (
    "pSMtaPort" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("pSMtaPort", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."AA_e" (
    "PKEY" text,
    "tnCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "tnCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("tnCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BLACCT_e" (
    "PKEY" text,
    "blAccountNumberCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "blAccountNumberCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("blAccountNumberCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."SERPROVID_p" (
    "PKEY" text,
    "serviceProviderIdCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "serviceProviderId" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "serviceProviderIdCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("serviceProviderIdCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BPS_p" (
    "PKEY" text,
    "BPSCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BPS" text,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BPSCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BPSCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BMR_e" (
    "PKEY" text,
    "BMRCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BMRCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BMRCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSFSbrEx_p" (
    "PKEY" text,
    "imsFeatureSbrExSidCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "imsFeatureSbrExMcidmode" text,
    "imsFeatureSbrExNs3pty" text,
    "imsFeatureSbrExNsacrm" text,
    "imsFeatureSbrExNsasslst" text,
    "imsFeatureSbrExNscaller" text,
    "imsFeatureSbrExNsccw" text,
    "imsFeatureSbrExNscd" text,
    "imsFeatureSbrExNscdvm" text,
    "imsFeatureSbrExNscfbvm" text,
    "imsFeatureSbrExNscfnlvm" text,
    "imsFeatureSbrExNscfnrcvm" text,
    "imsFeatureSbrExNscfnrvm" text,
    "imsFeatureSbrExNscfs" text,
    "imsFeatureSbrExNscfu" text,
    "imsFeatureSbrExNscidcw" text,
    "imsFeatureSbrExNsclip" text,
    "imsFeatureSbrExNsclir" text,
    "imsFeatureSbrExNscnip" text,
    "imsFeatureSbrExNscnir" text,
    "imsFeatureSbrExNscr" text,
    "imsFeatureSbrExNscun" text,
    "imsFeatureSbrExNscw" text,
    "imsFeatureSbrExNsdstr" text,
    "imsFeatureSbrExNshold" text,
    "imsFeatureSbrExNshrcn" text,
    "imsFeatureSbrExNsmcid" text,
    "imsFeatureSbrExNsmwi" text,
    "imsFeatureSbrExNsnrid" text,
    "imsFeatureSbrExNsqsns" text,
    "imsFeatureSbrExNsrd" text,
    "imsFeatureSbrExNsrid" text,
    "imsFeatureSbrExNssb" text,
    "imsFeatureSbrExNssd1d" text,
    "imsFeatureSbrExNssd2d" text,
    "imsFeatureSbrExNssic" text,
    "imsFeatureSbrExNsss" text,
    "imsFeatureSbrExSid" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "imsFeatureSbrExSidCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("imsFeatureSbrExSidCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BLSITEIDREF_e" (
    "siteId" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("siteId", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSMtaPort_e" (
    "PKEY" text,
    "pSMtaPortCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "pSMtaPortCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("pSMtaPortCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."SERPROVID_e" (
    "PKEY" text,
    "serviceProviderIdCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "serviceProviderIdCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("serviceProviderIdCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSMtaPortRef_p" (
    "pSMtaPort" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("pSMtaPort", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BLACCT_p" (
    "PKEY" text,
    "blAccountNumberCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "blAccountNumber" text,
    "blAddressComplex" text,
    "blApartment" text,
    "blBuilding" text,
    "blCity" text,
    "blCounty" text,
    "blDwelling" text,
    "blHouseNumber" text,
    "blHouseNumberFraction" text,
    "blState" text,
    "blStreetName" text,
    "blStreetSuffix" text,
    "blZipCode" text,
    "customerFirstName" text,
    "customerLastName" text,
    "customerMiddleInitial" text,
    "customerType" text,
    "houseKey" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "blAccountNumberCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("blAccountNumberCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."AA_p" (
    "PKEY" text,
    "tnCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "aaName" text,
    tn text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "tnCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("tnCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."RduDeviceCOS_e" (
    "PKEY" text,
    "rduDeviceCOSCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "rduDeviceCOSCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("rduDeviceCOSCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSHEdvaPort_p" (
    "PKEY" text,
    "imsEdvaPortCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "imsEdvaPort" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "imsEdvaPortCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("imsEdvaPortCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSHTN_p" (
    "PKEY" text,
    "imsTNCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "imsTN" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "imsTNCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("imsTNCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BRL_e" (
    "PKEY" text,
    "BRLCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BRLCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BRLCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BDM_p" (
    "PKEY" text,
    "BDMCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BDM" text,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BDMCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BDMCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BRL_p" (
    "PKEY" text,
    "BRLCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BRL" text,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BRLCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BRLCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSHTN_e" (
    "PKEY" text,
    "imsTNCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "imsTNCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("imsTNCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BDM_e" (
    "PKEY" text,
    "BDMCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BDMCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BDMCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSHEdvaPort_e" (
    "PKEY" text,
    "imsEdvaPortCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "imsEdvaPortCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("imsEdvaPortCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."RduDeviceCOS_p" (
    "PKEY" text,
    "rduDeviceCOSCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BGAdhocReq" text,
    "BGCO" text,
    "BGDY" text,
    "BGRA" text,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "rduDeviceCOS" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "rduDeviceCOSCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("rduDeviceCOSCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX RduDeviceCOS_2i_byRduDeviceCOS_BGBDY ON xnetgraph."RduDeviceCOS_p" ("BGDY");
CREATE INDEX RduDeviceCOS_2i_byRduDeviceCOS_BGBCO ON xnetgraph."RduDeviceCOS_p" ("BGCO");
CREATE INDEX RduDeviceCOS_2i_byRduDeviceCOS_BGBRA ON xnetgraph."RduDeviceCOS_p" ("BGRA");

CREATE TABLE xnetgraph."PSRdu_e" (
    "PKEY" text,
    "pSRduCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "pSRduCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("pSRduCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BSTREF_p" (
    "BST" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BST", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BRDREF_p" (
    "BRD" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BRD", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."HG_e" (
    "PKEY" text,
    "huntGroupIdCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "huntGroupIdCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("huntGroupIdCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BBFREF_p" (
    "BBF" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("BBF", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."NPAREFH_e" (
    "imsNPA" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("imsNPA", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSAccountStatusRef_p" (
    "pSAccountStatus" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("pSAccountStatus", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BST_p" (
    "PKEY" text,
    "BSTCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BST" text,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BSTCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BSTCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."J_BACCCOS_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BCO" text,
    "BDY" text,
    "BRA" text,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX J_BACCCOS_2i_byBACCCOS_BDY ON xnetgraph."J_BACCCOS_p" ("BDY");
CREATE INDEX J_BACCCOS_2i_byBACCCOS_BRA ON xnetgraph."J_BACCCOS_p" ("BRA");
CREATE INDEX J_BACCCOS_2i_byBACCCOS_BCO ON xnetgraph."J_BACCCOS_p" ("BCO");

CREATE TABLE xnetgraph."HG_p" (
    "PKEY" text,
    "huntGroupIdCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "huntGroupId" text,
    "huntGroupName" text,
    "huntGroupType" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "huntGroupIdCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("huntGroupIdCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BRDREF_e" (
    "BRD" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BRD", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSRdu_p" (
    "PKEY" text,
    "pSRduCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "pSRdu" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "pSRduCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("pSRduCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSSID_p" (
    "PKEY" text,
    "imsSIDCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "imsAkFlag" text,
    "imsAliasId" text,
    "imsAliasServiceId" text,
    "imsAsId" text,
    "imsAsName" text,
    "imsAuthRegFlag" text,
    "imsBarringIndication" text,
    "imsBaseDataTs" text,
    "imsBaseDataTsExt" text,
    "imsCaveAkaFlag" text,
    "imsChrgTplId" text,
    "imsConsFlg" text,
    "imsCurrentScscFname" text,
    "imsCxDataTs" text,
    "imsCxDataTsExt" text,
    "imsDataReference" text,
    "imsDefaultImpu" text,
    "imsDnAliasId" text,
    "imsDnHsidInfoRdn" text,
    "imsDnImpuInformation" text,
    "imsDnOperation" text,
    "imsDnPi" text,
    "imsDnPublic" text,
    "imsDnPublicId" text,
    "imsDnServiceInd" text,
    "imsDnSifcsetId" text,
    "imsDnSip" text,
    "imsDnSpId" text,
    "imsDnSubscriberId" text,
    "imsEaaFlag" text,
    "imsEiaFlag" text,
    "imsEsrn" text,
    "imsExpiryTime" text,
    "imsGiDataTs" text,
    "imsGiDataTsExt" text,
    "imsHdAsFlag" text,
    "imsHdaInfoRdn" text,
    "imsHsIdInfoRdn" text,
    "imsHssIndex" text,
    "imsImpiAuthInfoRdn" text,
    "imsImpiInfoRdn" text,
    "imsImpuInfoRdn" text,
    "imsImpuType" text,
    "imsImsRoamId" text,
    "imsIrsId" text,
    "imsLooseRouteIndication" text,
    "imsMediaProfileId" text,
    "imsNassRoamId" text,
    "imsNbaFlag" text,
    "imsNnaFlag" text,
    "imsNncFlag" text,
    "imsPassword" text,
    "imsPbxUserFlag" text,
    "imsPendingFlag" text,
    "imsPi" text,
    "imsPiid" text,
    "imsPuid" text,
    "imsRealm" text,
    "imsReassignmentPendingFlag" text,
    "imsRefNumber" text,
    "imsRepositDataTs" text,
    "imsRepositDataTsExt" text,
    "imsSID" text,
    "imsSIDsNameInformation" text,
    "imsScscHost" text,
    "imsScscRealm" text,
    "imsSdaFlag" text,
    "imsServiceInd" text,
    "imsServiceProviderId" text,
    "imsShDataTs" text,
    "imsShDataTsExt" text,
    "imsShared" text,
    "imsSifCseTid" text,
    "imsSpId" text,
    "imsStdPriority" text,
    "imsSubId" text,
    "imsSubIdInformation" text,
    "imsSubInfo" text,
    "imsSubInfoRdn" text,
    "imsTenancy" text,
    "imsTimeStamp" text,
    "imsUserName" text,
    "imsUserPriority" text,
    "imsUserState" text,
    "imsUserType" text,
    "imsVncFlag" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "imsSIDCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("imsSIDCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BSTREF_e" (
    "BST" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BST", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph.id_allocation (
    partition_hash int,
    dc_id smallint,
    member_id bigint,
    PRIMARY KEY ((partition_hash, dc_id))
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BST_e" (
    "PKEY" text,
    "BSTCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BSTCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BSTCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BBFREF_e" (
    "BBF" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("BBF", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."USERPROFILE_e" (
    "PKEY" text,
    "tnCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "tnCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("tnCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."USERPROFILE_p" (
    "PKEY" text,
    "tnCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "chargeNumber" text,
    "defaultAlias" text,
    "directoryTn" text,
    "firstName" text,
    "huntGroupSeq" text,
    "lastName" text,
    "networkCos" text,
    "numberActivation" text,
    "servicePackName" text,
    tn text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "tnCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("tnCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSAccountNum_p" (
    "PKEY" text,
    "pSAccountNumCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "pSAccountCorp" text,
    "pSAccountNum" text,
    "pSAddressLine1" text,
    "pSAddressLine2" text,
    "pSAgent" text,
    "pSCity" text,
    "pSDeleteFlag" text,
    "pSEmail" text,
    "pSFirstName" text,
    "pSFta" text,
    "pSFtaId" text,
    "pSHeadend" text,
    "pSLastName" text,
    "pSNode" text,
    "pSNodeId" text,
    "pSPrincipal" text,
    "pSState" text,
    "pSSystem" text,
    "pSTelephoneNum" text,
    "pSZipCode" text,
    "pSbsMacAddress" text,
    "pSrecChangeDate" text,
    "pSrecCreateDate" text,
    "pSsourceRequestTimestamp" text,
    "psBillerSource" text,
    "psRequestId" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "pSAccountNumCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("pSAccountNumCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSACCTTN_e" (
    "PKEY" text,
    "pSACCTTNCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "pSACCTTNCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("pSACCTTNCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BL_TN_p" (
    "PKEY" text,
    "blTnCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "blTn" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "blTnCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("blTnCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSHFSbr1_e" (
    "PKEY" text,
    "imsFeatureSbr1SidCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "imsFeatureSbr1SidCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("imsFeatureSbr1SidCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BRA_e" (
    "PKEY" text,
    "BRACKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "BRACKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("BRACKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BRA_p" (
    "PKEY" text,
    "BRACKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BRA" text,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BRACKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BRACKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSAccountNum_e" (
    "PKEY" text,
    "pSAccountNumCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "pSAccountNumCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("pSAccountNumCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."NPAREF_p" (
    "imsNPA" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("imsNPA", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSTN_e" (
    "PKEY" text,
    "imsTNCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "imsTNCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("imsTNCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSCos_p" (
    "PKEY" text,
    "pSCosCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "pSCos" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "pSCosCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("pSCosCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."NPAREFH_p" (
    "imsNPA" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("imsNPA", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."TN_p" (
    "PKEY" text,
    "tnCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    tenancy text,
    tn text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "tnCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("tnCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."BCO_p" (
    "PKEY" text,
    "BCOCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BCO" text,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "BCOCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("BCOCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."NPAREF_e" (
    "imsNPA" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("imsNPA", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."TN_e" (
    "PKEY" text,
    "tnCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "tnCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("tnCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."IMSTN_p" (
    "PKEY" text,
    "imsTNCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "imsTN" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "imsTNCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("imsTNCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph."PSCos_e" (
    "PKEY" text,
    "pSCosCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "pSCosCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("pSCosCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE KEYSPACE dse_security WITH replication = {'class': 'NetworkTopologyStrategy', 'Chicago': '1', 'Potomac': '1'}  AND durable_writes = true;

CREATE TABLE dse_security.role_options (
    role text PRIMARY KEY,
    options map<text, text>
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = 'role options'
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 7776000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 3600000
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE dse_security.digest_tokens (
    id blob PRIMARY KEY,
    password blob
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = 'digest_tokens'
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 7776000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 3600000
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE dse_security.spark_security (
    dc text PRIMARY KEY,
    shared_secret text
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = 'Spark security settings'
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 7776000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 3600000
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE KEYSPACE "xnetpipeline17G_pvt" WITH replication = {'class': 'LocalStrategy'}  AND durable_writes = true;

CREATE KEYSPACE xnetgraph1 WITH replication = {'class': 'NetworkTopologyStrategy', 'Chicago': '3'}  AND durable_writes = true;

CREATE TABLE xnetgraph1."ACCT_STATUS_p" (
    "PKEY" text,
    "accountStatusCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "accountStatus" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "accountStatusCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("accountStatusCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSCmsCliRef_p" (
    "pSCmsCli" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("pSCmsCli", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."MAC_e" (
    "PKEY" text,
    "macAddressCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "macAddressCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("macAddressCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."ACTSTATUSREF_e" (
    "accountStatus" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("accountStatus", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSMacaddress_p" (
    "PKEY" text,
    "pSMacaddressCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "pSMacaddress" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "pSMacaddressCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("pSMacaddressCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."FIB_NODE_p" (
    "PKEY" text,
    "fiberNodeCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "fiberNode" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "fiberNodeCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("fiberNodeCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSAccountStatus_e" (
    "PKEY" text,
    "pSAccountStatusCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "pSAccountStatusCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("pSAccountStatusCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSTN_p" (
    "PKEY" text,
    "pSTNCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "pSTN" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "pSTNCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("pSTNCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSDeviceType_p" (
    "PKEY" text,
    "pSDeviceTypeCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "pSDeviceType" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "pSDeviceTypeCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("pSDeviceTypeCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."BLSYSTEM_e" (
    "PKEY" text,
    "billingSystemCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "billingSystemCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("billingSystemCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSAccountStatusRef_p" (
    "pSAccountStatus" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("pSAccountStatus", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."MAC_p" (
    "PKEY" text,
    "macAddressCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "macAddress" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "macAddressCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("macAddressCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSMacaddress_e" (
    "PKEY" text,
    "pSMacaddressCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "pSMacaddressCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("pSMacaddressCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSCmsCliRef_e" (
    "pSCmsCli" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("pSCmsCli", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."ACCT_STATUS_e" (
    "PKEY" text,
    "accountStatusCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "accountStatusCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("accountStatusCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."BLSYSTEM_p" (
    "PKEY" text,
    "billingSystemCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "billingSystem" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "billingSystemCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("billingSystemCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSDeviceType_e" (
    "PKEY" text,
    "pSDeviceTypeCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "pSDeviceTypeCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("pSDeviceTypeCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."FIB_NODE_e" (
    "PKEY" text,
    "fiberNodeCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "fiberNodeCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("fiberNodeCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSMtaPort_p" (
    "PKEY" text,
    "pSMtaPortCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "pSMtaPort" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "pSMtaPortCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("pSMtaPortCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSAccountStatus_p" (
    "PKEY" text,
    "pSAccountStatusCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "pSAccountStatus" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "pSAccountStatusCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("pSAccountStatusCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSAccountStatusRef_e" (
    "pSAccountStatus" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("pSAccountStatus", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSRduMac_p" (
    "PKEY" text,
    "pSRduMacCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "pSChRouterMode" text,
    "pSConfigFile" text,
    "pSDataSource" text,
    "pSDhcpCriteria" text,
    "pSDnsServer" text,
    "pSDocsisVersion" text,
    "pSDomainName" text,
    "pSEquipGuid" text,
    "pSGiAddress" text,
    "pSHostName" text,
    "pSIspcode" text,
    "pSOwnerId" text,
    "pSRduMac" text,
    "pSRegionId" text,
    "pSrecChangeDate" text,
    "pSrecCreateDate" text,
    "pSsourceRequestTimestamp" text,
    "psGateway" text,
    "psRequestId" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "pSRduMacCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("pSRduMacCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSRdu_e" (
    "PKEY" text,
    "pSRduCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "pSRduCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("pSRduCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSDeviceStatus_p" (
    "PKEY" text,
    "pSDeviceStatusCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "pSDeviceStatus" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "pSDeviceStatusCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("pSDeviceStatusCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."BL_DEV_MAC_e" (
    "PKEY" text,
    "deviceMacAddressCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "deviceMacAddressCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("deviceMacAddressCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."BACTMAC_p" (
    "PKEY" text,
    "acctNumMacCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "acctNumMac" text,
    "deviceSerialNumber" text,
    "deviceType" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "acctNumMacCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("acctNumMacCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."OWaveNPA_p" (
    "PKEY" text,
    "owaveNPACKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "owaveNPA" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "owaveNPACKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("owaveNPACKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."OWaveMobileNPA_p" (
    "PKEY" text,
    "owaveMobileNPACKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "owaveMobileNPA" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "owaveMobileNPACKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("owaveMobileNPACKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSCmsCli_e" (
    "PKEY" text,
    "pSCmsCliCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "pSCmsCliCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("pSCmsCliCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PsProvisionedCosRef_p" (
    "pSCos" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("pSCos", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PsEquipDevicetypeRef_e" (
    "pSDeviceType" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("pSDeviceType", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."ACCT_p" (
    "PKEY" text,
    "accountNumberCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "accountNumber" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "accountNumberCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("accountNumberCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."BACTMAC_e" (
    "PKEY" text,
    "acctNumMacCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "acctNumMacCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("acctNumMacCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."BL_DEV_MAC_p" (
    "PKEY" text,
    "deviceMacAddressCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "deviceMacAddress" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "deviceMacAddressCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("deviceMacAddressCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSRdu_p" (
    "PKEY" text,
    "pSRduCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "pSRdu" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "pSRduCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("pSRduCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSRduMac_e" (
    "PKEY" text,
    "pSRduMacCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "pSRduMacCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("pSRduMacCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PsDerivedRduRef_e" (
    "pSRdu" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("pSRdu", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."OWaveMobileNPA_e" (
    "PKEY" text,
    "owaveMobileNPACKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "owaveMobileNPACKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("owaveMobileNPACKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PsEquipDevicetypeRef_p" (
    "pSDeviceType" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("pSDeviceType", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."ACCT_e" (
    "PKEY" text,
    "accountNumberCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "accountNumberCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("accountNumberCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."OWaveNPA_e" (
    "PKEY" text,
    "owaveNPACKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "owaveNPACKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("owaveNPACKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSCmsCli_p" (
    "PKEY" text,
    "pSCmsCliCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "pSCmsCli" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "pSCmsCliCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("pSCmsCliCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSACCTTN_p" (
    "PKEY" text,
    "pSACCTTNCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "pSACCTTN" text,
    "pSCmsDqosProfile" text,
    "pSCmsFqdn" text,
    "pSCmsMtaProfile" text,
    "pSCmsRateCenter" text,
    "pSCmsSecurityProfile" text,
    "pSCmsServType" text,
    "pSDisplayName" text,
    "pSFeaturesEnabled" text,
    "pSLineAdminState" text,
    "pSLpicProvider" text,
    "pSLrn" text,
    "pSPicProvider" text,
    "pSPortingStatus" text,
    "pSVmAdminState" text,
    "pSVmCos" text,
    "pSVmEmail" text,
    "pSVmMsgStoreServer" text,
    "pSVmName" text,
    "pSVmServerFqdn" text,
    "pSVmType" text,
    "pSrecChangeDate" text,
    "pSrecCreateDate" text,
    "pSsourceRequestTimestamp" text,
    "psRequestId" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "pSACCTTNCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("pSACCTTNCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSDeviceStatus_e" (
    "PKEY" text,
    "pSDeviceStatusCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "pSDeviceStatusCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("pSDeviceStatusCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."BACTTN_p" (
    "PKEY" text,
    "acctNumTnCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "acctNumTn" text,
    "portedFlag" text,
    "primaryTnFlag" text,
    "tnStatus" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "acctNumTnCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("acctNumTnCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."BLSITEID_e" (
    "PKEY" text,
    "siteIdCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "siteIdCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("siteIdCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSAccountNum_p" (
    "PKEY" text,
    "pSAccountNumCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "pSAccountCorp" text,
    "pSAccountNum" text,
    "pSAddressLine1" text,
    "pSAddressLine2" text,
    "pSAgent" text,
    "pSCity" text,
    "pSEmail" text,
    "pSFirstName" text,
    "pSFta" text,
    "pSFtaId" text,
    "pSHeadend" text,
    "pSLastName" text,
    "pSNode" text,
    "pSNodeId" text,
    "pSPrincipal" text,
    "pSState" text,
    "pSSystem" text,
    "pSTelephoneNum" text,
    "pSZipCode" text,
    "pSbsMacAddress" text,
    "pSrecChangeDate" text,
    "pSrecCreateDate" text,
    "pSsourceRequestTimestamp" text,
    "psBillerSource" text,
    "psRequestId" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "pSAccountNumCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("pSAccountNumCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSACCTTN_e" (
    "PKEY" text,
    "pSACCTTNCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "pSACCTTNCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("pSACCTTNCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."BL_TN_p" (
    "PKEY" text,
    "blTnCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "blTn" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "blTnCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("blTnCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."OwaveMobileTn_e" (
    "PKEY" text,
    "owaveMobileTnCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "owaveMobileTnCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("owaveMobileTnCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1.id_allocation (
    partition_hash int,
    dc_id smallint,
    member_id bigint,
    PRIMARY KEY ((partition_hash, dc_id))
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."BL_TN_e" (
    "PKEY" text,
    "blTnCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "blTnCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("blTnCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."OwaveMobileTn_p" (
    "PKEY" text,
    "owaveMobileTnCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "owaveAdminPolicyDn" text,
    "owaveCorpMailPrimaryAddress" text,
    "owaveCreateTimeStamp" text,
    "owaveGivenName" text,
    "owaveMail" text,
    "owaveMailBoxId" text,
    "owaveMailBoxStatus" text,
    "owaveMailLogin" text,
    "owaveMailMessageStore" text,
    "owaveMailPassword" text,
    "owaveMobileTn" text,
    "owaveModifyTimeStamp" text,
    "owaveMsgTimeZone" text,
    "owaveNotifyServiceOverride" text,
    "owavePostalCode" text,
    "owaveSn" text,
    "owaveTenancy" text,
    "owaveUmAccountLastAccessTime" text,
    "owaveUmDestSearchParam" text,
    "owaveUmPin" text,
    "owaveUmPinLastModifyTime" text,
    "owaveUmPn" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "owaveMobileTnCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("owaveMobileTnCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSMtaPort_e" (
    "PKEY" text,
    "pSMtaPortCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "pSMtaPortCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("pSMtaPortCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."BACTTN_e" (
    "PKEY" text,
    "acctNumTnCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "acctNumTnCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("acctNumTnCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSAccountNum_e" (
    "PKEY" text,
    "pSAccountNumCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "pSAccountNumCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("pSAccountNumCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."BLSITEID_p" (
    "PKEY" text,
    "siteIdCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "siteId" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "siteIdCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("siteIdCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."BLSYSTEMREF_p" (
    "billingSystem" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("billingSystem", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."BLSITEIDREF_p" (
    "siteId" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("siteId", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."ACTSTATUSREF_p" (
    "accountStatus" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("accountStatus", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."TN_p" (
    "PKEY" text,
    "tnCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    tenancy text,
    tn text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "tnCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("tnCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSMtaPortRef_e" (
    "pSMtaPort" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("pSMtaPort", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSCos_p" (
    "PKEY" text,
    "pSCosCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "pSCos" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "pSCosCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("pSCosCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."BLACCT_e" (
    "PKEY" text,
    "blAccountNumberCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "blAccountNumberCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("blAccountNumberCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PsProvisionedCosRef_e" (
    "pSCos" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("pSCos", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."OwaveTn_p" (
    "PKEY" text,
    "owaveTnCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "owaveAdminPolicyDn" text,
    "owaveCorpMailPrimaryAddress" text,
    "owaveCreateTimeStamp" text,
    "owaveGivenName" text,
    "owaveMail" text,
    "owaveMailBoxId" text,
    "owaveMailBoxStatus" text,
    "owaveMailLogin" text,
    "owaveMailMessageStore" text,
    "owaveMailPassword" text,
    "owaveModifyTimeStamp" text,
    "owaveMsgTimeZone" text,
    "owaveNotifyServiceOverride" text,
    "owavePostalCode" text,
    "owaveSn" text,
    "owaveTenancy" text,
    "owaveTn" text,
    "owaveUmAccountLastAccessTime" text,
    "owaveUmDestSearchParam" text,
    "owaveUmPin" text,
    "owaveUmPinLastModifyTime" text,
    "owaveUmPn" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "owaveTnCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("owaveTnCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PsDeviceStatusRef_e" (
    "pSDeviceStatus" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("pSDeviceStatus", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PsDerivedRduRef_p" (
    "pSRdu" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("pSRdu", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PsDeviceStatusRef_p" (
    "pSDeviceStatus" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("pSDeviceStatus", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."BLSYSTEMREF_e" (
    "billingSystem" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("billingSystem", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."BLSITEIDREF_e" (
    "siteId" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("siteId", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."TN_e" (
    "PKEY" text,
    "tnCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "tnCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("tnCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."OwaveTn_e" (
    "PKEY" text,
    "owaveTnCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "owaveTnCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("owaveTnCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSMtaPortRef_p" (
    "pSMtaPort" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("pSMtaPort", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSTN_e" (
    "PKEY" text,
    "pSTNCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "pSTNCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("pSTNCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."BLACCT_p" (
    "PKEY" text,
    "blAccountNumberCKEY" text,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "EID" bigint,
    "FDT" bigint,
    "SCDT" bigint,
    "SEDT" bigint,
    "STP" bigint,
    "TDT" bigint,
    "blAccountNumber" text,
    "blAddressComplex" text,
    "blApartment" text,
    "blBuilding" text,
    "blCity" text,
    "blCounty" text,
    "blDwelling" text,
    "blHouseNumber" text,
    "blHouseNumberFraction" text,
    "blState" text,
    "blStreetName" text,
    "blStreetSuffix" text,
    "blZipCode" text,
    "customerFirstName" text,
    "customerLastName" text,
    "customerMiddleInitial" text,
    "customerType" text,
    "houseKey" text,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~STP" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY ("PKEY", "blAccountNumberCKEY", "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY ("blAccountNumberCKEY" ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetgraph1."PSCos_e" (
    "PKEY" text,
    "pSCosCKEY" text,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~EID" bigint,
    "~FDT" bigint,
    "~SCDT" bigint,
    "~SEDT" bigint,
    "~STP" bigint,
    "~TDT" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY ("PKEY", "pSCosCKEY", "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY ("pSCosCKEY" ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE KEYSPACE dse_leases WITH replication = {'class': 'NetworkTopologyStrategy', 'Chicago': '1', 'Potomac': '1'}  AND durable_writes = true;

CREATE TABLE dse_leases.leases (
    name text,
    dc text,
    duration_ms int,
    epoch bigint,
    holder inet,
    PRIMARY KEY ((name, dc))
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = 'DSE Lease coordination'
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 0
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 3600000
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE dse_leases.logs (
    name text,
    dc text,
    monitor inet,
    at timestamp,
    new_holder inet,
    old_holder inet,
    PRIMARY KEY ((name, dc), monitor, at)
) WITH CLUSTERING ORDER BY (monitor ASC, at ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = 'DSE Lease history'
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 0
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 3600000
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE KEYSPACE "xnetgraph17G_pvt" WITH replication = {'class': 'LocalStrategy'}  AND durable_writes = true;

CREATE KEYSPACE xnetpipeline WITH replication = {'class': 'NetworkTopologyStrategy', 'Chicago': '3', 'Potomac': '3'}  AND durable_writes = true;

CREATE TABLE xnetpipeline."JobProperties_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "eventProcessClass" text,
    "eventSourceClass" text,
    "fromDateTime" bigint,
    "toDateTime" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."JobPlan_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "fromDateTime" bigint,
    name text,
    "toDateTime" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."BillerRTQueryRegister_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."FileSource_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."DdpDeviceTypeXref_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."BillerSyncQueryRegister_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "filePath" text,
    "queryType" text,
    "schema" text,
    "siteId" text,
    "sourceEventTime" bigint,
    "sourceId" bigint,
    "sourceProcessTime" bigint,
    "storeName" text,
    system text,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."FileSource_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "filePath" text,
    "fileSizeInBytes" bigint,
    "fromDateTime" bigint,
    "processedDate" text,
    "sourceTypeWithDate" text,
    "toDateTime" bigint,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX FileSource_2i_bySourceId ON xnetpipeline."FileSource_p" (xid);
CREATE INDEX FileSource_2i_byFsProcessedDate ON xnetpipeline."FileSource_p" ("processedDate");
CREATE INDEX FileSource_2i_byFsSourceTypeWithDate ON xnetpipeline."FileSource_p" ("sourceTypeWithDate");

CREATE TABLE xnetpipeline."BillerRTQueryRegister_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "lastPullDate" text,
    "lastPullSlot" text,
    "lastPullTime" text,
    "queryType" text,
    "schema" text,
    "siteId" text,
    "sourceEventTime" bigint,
    "sourceId" bigint,
    "sourceProcessTime" bigint,
    "storeName" text,
    system text,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."DdpDeviceTypeXref_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "addressType" text,
    "boxEquip" text,
    "deviceType" text,
    "longDesc" text,
    "siteId" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."JobProperties_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."JobBundle_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."ServiceSource_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "fromDateTime" bigint,
    name text,
    "processedDate" text,
    "sourceTypeWithDate" text,
    "toDateTime" bigint,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX ServiceSource_2i_bySersProcessedDate ON xnetpipeline."ServiceSource_p" ("processedDate");
CREATE INDEX ServiceSource_2i_bySersSourceTypeWithDate ON xnetpipeline."ServiceSource_p" ("sourceTypeWithDate");
CREATE INDEX ServiceSource_2i_byServiceSourceId ON xnetpipeline."ServiceSource_p" (xid);

CREATE TABLE xnetpipeline."JobAttempt_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."ServiceSource_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."BillerSyncQueryRegister_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."IMSSyncRegister_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."FileEventGulla_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "bundleNumber" bigint,
    context bigint,
    "eventCount" bigint,
    "eventGullaVersion" bigint,
    "eventTypeId" bigint,
    events blob,
    "fromDateTime" bigint,
    "jobAttemptId" bigint,
    "processedDate" text,
    "processedTime" bigint,
    "sourceTypeWithDate" text,
    "threadID" bigint,
    "toDateTime" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX FileEventGulla_2i_byBundleNumber ON xnetpipeline."FileEventGulla_p" ("bundleNumber");
CREATE INDEX FileEventGulla_2i_byFegSourceTypeWithDate ON xnetpipeline."FileEventGulla_p" ("sourceTypeWithDate");
CREATE INDEX FileEventGulla_2i_byGullaJobAttemptId ON xnetpipeline."FileEventGulla_p" ("jobAttemptId");
CREATE INDEX FileEventGulla_2i_byFegProcessedDate ON xnetpipeline."FileEventGulla_p" ("processedDate");
CREATE INDEX FileEventGulla_2i_byProcessedTime ON xnetpipeline."FileEventGulla_p" ("processedTime");

CREATE TABLE xnetpipeline."Context_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."CsgDeviceTypeXref_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "deviceType" text,
    "eqpCmpntEqp" text,
    "eqpTypDesc" text,
    "eqpTypEqp" text,
    prin text,
    sys text,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."SourceStatus_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."JobManager_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."FileEventGulla_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."SourceType_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "fromDateTime" bigint,
    name text,
    "toDateTime" bigint,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX SourceType_2i_byname ON xnetpipeline."SourceType_p" (name);

CREATE TABLE xnetpipeline."IMSSyncRegister_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "filePath" text,
    "partitionKeyId" text,
    "sourceEventTime" bigint,
    "sourceId" bigint,
    "sourceProcessTime" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline.id_allocation (
    partition_hash int,
    dc_id smallint,
    member_id bigint,
    PRIMARY KEY ((partition_hash, dc_id))
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."JobBundle_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "BQE@fail#qc" bigint,
    "BQE@finish#qc" bigint,
    "BQE@query#mwt" bigint,
    "BQE@submit#qc" bigint,
    "EC@queuepresize#ec" bigint,
    "EC@transform#mwt" bigint,
    "EH@2errorQueueAck#ec" bigint,
    "EH@2errorQueueSent#ec" bigint,
    "EH@businessFail#ec" bigint,
    "EH@commit#ec" bigint,
    "EH@commit#mwt" bigint,
    "EH@commitFail#ec" bigint,
    "EH@errorQueueAck#ec" bigint,
    "EH@errorQueueSent#ec" bigint,
    "EH@fail#ec" bigint,
    "EH@handlerFail#ec" bigint,
    "EH@kafkaPersistence#ec" bigint,
    "EH@kafkaPersistence#mwt" bigint,
    "EH@postpone#ec" bigint,
    "EH@postponeFail#ec" bigint,
    "EH@process#ec" bigint,
    "EH@readyFail#ec" bigint,
    "EH@receive#ec" bigint,
    "EH@success#ec" bigint,
    "EH@systemFail#ec" bigint,
    "FW@process#fc" bigint,
    "GEC@execute#mwt" bigint,
    "GEC@persist#mwt" bigint,
    "GEC@persist#nc" bigint,
    "GEC@persistFail#nc" bigint,
    "GEC@prepare#edc" bigint,
    "GEC@prepare#nc" bigint,
    "GEC@prepare#pkt" bigint,
    "GEC@prepareAll#nc" bigint,
    "GEC@prepareDelete#edc" bigint,
    "GEC@prepareDelete#nc" bigint,
    "GEC@prepareUpdate#edc" bigint,
    "GEC@prepareUpdate#nc" bigint,
    "GEC@vabReuse#nc" bigint,
    "GP@create#edc" bigint,
    "GP@create#pc" bigint,
    "GP@create#pvc" bigint,
    "GP@createedc#mwt" bigint,
    "GP@createpvc#mwt" bigint,
    "GP@delete#pvc" bigint,
    "GP@deleteppvc#mwt" bigint,
    "GP@deletepvc#mwt" bigint,
    "GP@dexpiredrop#edc" bigint,
    "GP@dexpiredrop#nc" bigint,
    "GP@dexpiredropedc#mwt" bigint,
    "GP@dexpireedc#mwt" bigint,
    "GP@dexpirepedc#mwt" bigint,
    "GP@execute#mwt" bigint,
    "GP@expire#edc" bigint,
    "GP@expire#nc" bigint,
    "GP@expiredrop#edc" bigint,
    "GP@expiredrop#nc" bigint,
    "GP@expireedc#mwt" bigint,
    "GP@expirepedc#mwt" bigint,
    "GP@fetch#edc" bigint,
    "GP@fetch#nc" bigint,
    "GP@fetch#pc" bigint,
    "GP@fetch#pvc" bigint,
    "GP@fetchedc#mwt" bigint,
    "GP@fetchnc#mwt" bigint,
    "GP@fetchpc#mwt" bigint,
    "GP@fetchpvc#mwt" bigint,
    "GP@fetchtranc#mwt" bigint,
    "GP@icreatenc#mwt" bigint,
    "GP@increate#nc" bigint,
    "GP@isexistsnc#mwt" bigint,
    "GP@ocreatenc#mwt" bigint,
    "GP@ocreatepnc#mwt" bigint,
    "GP@oupdatenc#mwt" bigint,
    "GP@outcreate#nc" bigint,
    "GP@processpvc#mwt" bigint,
    "GP@revive#nc" bigint,
    "GP@update#nc" bigint,
    "GP@update#pc" bigint,
    "GP@update#pvc" bigint,
    "JB@main#note" text,
    "JM@job#no" bigint,
    "JM@jobBundle#ewt" bigint,
    "JM@jobBundle#no" bigint,
    "JM@jobBundleEnd#time" bigint,
    "JM@jobBundleStart#time" bigint,
    "MF@filtered#mc" bigint,
    "MF@unfiltered#mc" bigint,
    "MS@fetch#bytes" bigint,
    "MS@ignored#ec" bigint,
    "MS@rdd#mc" bigint,
    "MS@receive#mc" bigint,
    "MS@workFail#ec" bigint,
    "MSS@all#mc" bigint,
    "MSS@batchCurrent#ewt" bigint,
    "MSS@batchPrev#ewt" bigint,
    "MSS@batchStartCurrent#time" bigint,
    "MSS@batchStartPrev#time" bigint,
    "MSS@common#mc" bigint,
    "MSS@compare#mwt" bigint,
    "MSS@delete#mc" bigint,
    "MSS@deleteEdge#mc" bigint,
    "MSS@devicetype#nulc" bigint,
    "MSS@extractCurrent#mwt" bigint,
    "MSS@extractPrev#mwt" bigint,
    "MSS@insert#mc" bigint,
    "MSS@macCurrent#nulc" bigint,
    "MSS@macPrev#nulc" bigint,
    "MSS@override#mc" bigint,
    "MSS@prepareCompare#mwt" bigint,
    "MSS@process#bac" bigint,
    "MSS@processCurrent#bac" bigint,
    "MSS@processPrev#bac" bigint,
    "MSS@receiveCurrent#mc" bigint,
    "MSS@receivePrev#mc" bigint,
    "MSS@scoutDevice#nfc" bigint,
    "MSS@update#mc" bigint,
    "MSS@webService#rqc" bigint,
    "MT@passthrough#ec" bigint,
    "MT@transform#ec" bigint,
    "NP@createB#mwt" bigint,
    "NP@createB#nc" bigint,
    "NP@createBR#nc" bigint,
    "NP@createS#mwt" bigint,
    "NP@createS#nc" bigint,
    "NP@createSR#nc" bigint,
    "NP@fetchB#mwt" bigint,
    "NP@fetchB#nc" bigint,
    "NP@fetchS#mwt" bigint,
    "NP@fetchS#nc" bigint,
    "NP@nodeCache#hc" bigint,
    "NP@prefetch#mwt" bigint,
    "NP@prefetch#nc" bigint,
    "PITA@all#rsc" bigint,
    "PITA@complete#tc" bigint,
    "PITA@fail#tc" bigint,
    "PITA@fetch#buc" bigint,
    "PITA@start#tc" bigint,
    "PJM@job#no" bigint,
    "PJM@jobBundle#ewt" bigint,
    "PJM@jobBundle#no" bigint,
    "PJM@jobBundleEnd#time" bigint,
    "PJM@jobBundleStart#time" bigint,
    "PL@commit#mwt" bigint,
    "PL@fail#ec" bigint,
    "PL@fail#gc" bigint,
    "PL@success#ec" bigint,
    "PL@success#gc" bigint,
    "QEC@persist@ec" bigint,
    "QEC@persist@mwt" bigint,
    "RP@extract#mwt" bigint,
    "bundleNumber" bigint,
    errorcode bigint,
    errordescription text,
    "filePath" text,
    "fileSizeInBytes" bigint,
    "hostName" text,
    "jobAttemptId" bigint,
    "processedDate" text,
    "sourceTypeName" text,
    "sourceTypeWithDate" text,
    status text,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX JobBundle_2i_byJbSourceTypeName ON xnetpipeline."JobBundle_p" ("sourceTypeName");
CREATE INDEX JobBundle_2i_byBundleStartTime ON xnetpipeline."JobBundle_p" ("JM@jobBundleStart#time");
CREATE INDEX JobBundle_2i_byBundleNumber ON xnetpipeline."JobBundle_p" ("bundleNumber");
CREATE INDEX JobBundle_2i_byJobAtmpId ON xnetpipeline."JobBundle_p" ("jobAttemptId");
CREATE INDEX JobBundle_2i_byJbProcessedDate ON xnetpipeline."JobBundle_p" ("processedDate");
CREATE INDEX JobBundle_2i_byStatus ON xnetpipeline."JobBundle_p" (status);
CREATE INDEX JobBundle_2i_byJbSourceTypeWithDate ON xnetpipeline."JobBundle_p" ("sourceTypeWithDate");
CREATE INDEX JobBundle_2i_byFilePath ON xnetpipeline."JobBundle_p" ("filePath");

CREATE TABLE xnetpipeline."Context_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "fromDateTime" bigint,
    "locationId" bigint,
    "processedDate" text,
    "programTypeId" bigint,
    purpose text,
    "toDateTime" bigint,
    "userName" text,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX Context_2i_byContextId ON xnetpipeline."Context_p" (xid);

CREATE TABLE xnetpipeline."SourceStatus_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "fromDateTime" bigint,
    "sourceId" bigint,
    status text,
    "toDateTime" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."CsgDeviceTypeXref_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."JobAttemptStatus_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."OpenwaveSyncRegister_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."Job_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."OpenWaveChangeNumber_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "OpenwaveLastEventTime" text,
    "changeNumber" text,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."HarmonySyncRegister_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "filePath" text,
    "partitionKeyId" text,
    "sourceEventTime" bigint,
    "sourceId" bigint,
    "sourceProcessTime" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."SyncSource_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."Job_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "fromDateTime" bigint,
    "processedDate" text,
    "sourceId" bigint,
    "sourceTypeWithDate" text,
    "toDateTime" bigint,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX Job_2i_byJProcessedDate ON xnetpipeline."Job_p" ("processedDate");
CREATE INDEX Job_2i_byJobId ON xnetpipeline."Job_p" (xid);
CREATE INDEX Job_2i_byJSourceTypeWithDate ON xnetpipeline."Job_p" ("sourceTypeWithDate");

CREATE TABLE xnetpipeline."OpenWaveChangeNumber_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."Source_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."JobAttemptStatus_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "fromDateTime" bigint,
    status text,
    "toDateTime" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."OpenwaveSyncRegister_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    "filePath" text,
    "sourceEventTime" bigint,
    "sourceId" bigint,
    "sourceProcessTime" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."HarmonySyncRegister_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."SyncSource_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "filePath" text,
    "fileSizeInBytes" bigint,
    "fromDateTime" bigint,
    "lastEventTime" bigint,
    "lastProcessTime" bigint,
    "processedDate" text,
    "sourceTypeName" text,
    "sourceTypeWithDate" text,
    "toDateTime" bigint,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."JobManager_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "fromDateTime" bigint,
    "jobManagerClass" text,
    "toDateTime" bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."FileReceived_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."JobAttempt_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "fromDateTime" bigint,
    "jobId" bigint,
    "processedDate" text,
    "sourceTypeWithDate" text,
    "toDateTime" bigint,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX JobAttempt_2i_byJaSourceTypeWithDate ON xnetpipeline."JobAttempt_p" ("sourceTypeWithDate");
CREATE INDEX JobAttempt_2i_byJaProcessedDate ON xnetpipeline."JobAttempt_p" ("processedDate");
CREATE INDEX JobAttempt_2i_byJobAttemptId ON xnetpipeline."JobAttempt_p" (xid);

CREATE TABLE xnetpipeline."FileReceived_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "destinationFilePath" text,
    "fromDateTime" bigint,
    "jobAttemptId" bigint,
    "processedDate" text,
    "sourceFilePath" text,
    "sourceTypeWithDate" text,
    "toDateTime" bigint,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX FileReceived_2i_byFrProcessedDate ON xnetpipeline."FileReceived_p" ("processedDate");
CREATE INDEX FileReceived_2i_byDestinationFilePath ON xnetpipeline."FileReceived_p" ("destinationFilePath");
CREATE INDEX FileReceived_2i_byJbAttemptId ON xnetpipeline."FileReceived_p" ("jobAttemptId");
CREATE INDEX FileReceived_2i_byFrSourceTypeWithDate ON xnetpipeline."FileReceived_p" ("sourceTypeWithDate");
CREATE INDEX FileReceived_2i_bySourceFilePath ON xnetpipeline."FileReceived_p" ("sourceFilePath");

CREATE TABLE xnetpipeline."Source_p" (
    community_id int,
    member_id bigint,
    "~~property_key_id" int,
    "~~property_id" uuid,
    context bigint,
    "filePath" text,
    "fileSizeInBytes" bigint,
    "fromDateTime" bigint,
    "processedDate" text,
    "sourceTypeName" text,
    "sourceTypeWithDate" text,
    "toDateTime" bigint,
    xid bigint,
    "~~vertex_exists" boolean,
    PRIMARY KEY (community_id, member_id, "~~property_key_id", "~~property_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~property_key_id" ASC, "~~property_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."SourceType_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE xnetpipeline."JobPlan_e" (
    community_id int,
    member_id bigint,
    "~~edge_label_id" int,
    "~~adjacent_vertex_id" blob,
    "~~adjacent_label_id" smallint,
    "~~edge_id" uuid,
    "~fromDateTime" bigint,
    "~toDateTime" bigint,
    "~~edge_exists" boolean,
    "~~simple_edge_id" uuid,
    PRIMARY KEY (community_id, member_id, "~~edge_label_id", "~~adjacent_vertex_id", "~~adjacent_label_id", "~~edge_id")
) WITH CLUSTERING ORDER BY (member_id ASC, "~~edge_label_id" ASC, "~~adjacent_vertex_id" ASC, "~~adjacent_label_id" ASC, "~~edge_id" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE KEYSPACE xnetpipeline1_pvt WITH replication = {'class': 'LocalStrategy'}  AND durable_writes = true;

CREATE KEYSPACE dse_system WITH replication = {'class': 'EverywhereStrategy'}  AND durable_writes = true;

CREATE TABLE dse_system.encrypted_keys (
    key_file text,
    cipher text,
    strength int,
    key_id timeuuid,
    key text,
    PRIMARY KEY (key_file, cipher, strength, key_id)
) WITH CLUSTERING ORDER BY (cipher ASC, strength ASC, key_id ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 0
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 3600000
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE dse_system.shared_data_versions (
    node inet PRIMARY KEY,
    version uuid
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 1209600
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 3600000
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE dse_system.leases (
    lease_id text PRIMARY KEY,
    duration int,
    generation bigint,
    owner uuid,
    ts timestamp
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 1209600
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 3600000
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE dse_system.shared_data (
    dataspace text,
    valid_until timeuuid,
    namespace text,
    name text,
    boolean boolean,
    double double,
    duration bigint,
    enum text,
    float float,
    inet inet,
    instant timestamp,
    integer int,
    json blob,
    last_updated timeuuid static,
    long bigint,
    string text,
    type text,
    written_on timeuuid,
    PRIMARY KEY (dataspace, valid_until, namespace, name)
) WITH CLUSTERING ORDER BY (valid_until ASC, namespace ASC, name ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = 'f5fe6d80-c668-11e7-81c6-e1d044565569'
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 1209600
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 3600000
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';


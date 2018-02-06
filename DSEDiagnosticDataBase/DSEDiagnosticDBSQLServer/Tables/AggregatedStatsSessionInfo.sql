CREATE TABLE [DSEDiagnostic].[AggregatedStatsSessionInfo] (
    [AggStatId]                BIGINT             IDENTITY (1, 1) NOT NULL,
    [RunSessionId]             BIGINT             NOT NULL,
    [Source]                   VARCHAR (25)       NOT NULL,
    [Attribute]                VARCHAR (100)      NOT NULL,
    [SubAttribute]             VARCHAR (25)       NULL,
    [ReferenceDateTime]        DATETIMEOFFSET (4) NOT NULL,
    [DataCenterId]             BIGINT             NULL,
    [NodeId]                   BIGINT             NULL,
    [KeyspaceId]               BIGINT             NULL,
    [DDLId]                    BIGINT             NULL,
    [AggStatGroup]             UNIQUEIDENTIFIER   NULL,
    [EventClass]               VARCHAR (50)       NULL,
    [AggregatePeriod_MIN]      INT                NULL,
    [UnitOfMeasure]            VARCHAR (25)       NULL,
    [Value]                    DECIMAL (28, 6)    NULL,
    [Maximum]                  DECIMAL (28, 6)    NULL,
    [Minimum]                  DECIMAL (28, 6)    NULL,
    [Mean]                     DECIMAL (28, 6)    NULL,
    [StandardDeviation]        DECIMAL (28, 6)    NULL,
    [NbrOccurrences]           INT                CONSTRAINT [defo_AggregatedStatsSessionInfo_NbrOccurrences] DEFAULT ((1)) NOT NULL,
    [ReferencedEventSessionId] BIGINT             NULL,
    CONSTRAINT [Pk_AggregatedStatsSessionInfo_AggStatId] PRIMARY KEY CLUSTERED ([AggStatId] ASC),
    CONSTRAINT [fk_aggregatedstatssessioninfo_DataCenterId] FOREIGN KEY ([DataCenterId]) REFERENCES [DSEDiagnostic].[DataCenters] ([DataCenterId]) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT [fk_aggregatedstatssessioninfo_DDL] FOREIGN KEY ([DDLId]) REFERENCES [DSEDiagnostic].[ClusterDDLObjects] ([DDLId]),
    CONSTRAINT [fk_aggregatedstatssessioninfo_EventSessionId] FOREIGN KEY ([ReferencedEventSessionId]) REFERENCES [DSEDiagnostic].[EventStatsSessionInfo] ([EventSessionId]),
    CONSTRAINT [fk_aggregatedstatssessioninfo_Keyspace] FOREIGN KEY ([KeyspaceId]) REFERENCES [DSEDiagnostic].[ClusterDDLObjects] ([DDLId]),
    CONSTRAINT [fk_aggregatedstatssessioninfo_NodeId] FOREIGN KEY ([NodeId]) REFERENCES [DSEDiagnostic].[Nodes] ([NodeId]) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT [fk_aggregatedstatssessioninfo_RunSessionId] FOREIGN KEY ([RunSessionId]) REFERENCES [DSEDiagnostic].[RunSessionInfo] ([RunSessionId]) ON DELETE CASCADE ON UPDATE CASCADE
);


GO
CREATE NONCLUSTERED INDEX [Idx_AggregatedStatsSessionInfo_SessionRunId]
    ON [DSEDiagnostic].[AggregatedStatsSessionInfo]([RunSessionId] ASC);


GO
CREATE NONCLUSTERED INDEX [Idx_AggregatedStatsSessionInfo_DataCenterId]
    ON [DSEDiagnostic].[AggregatedStatsSessionInfo]([DataCenterId] ASC);


GO
CREATE NONCLUSTERED INDEX [Idx_AggregatedStatsSessionInfo_NodeId]
    ON [DSEDiagnostic].[AggregatedStatsSessionInfo]([NodeId] ASC);


GO
CREATE NONCLUSTERED INDEX [Idx_AggregatedStatsSessionInfo_DDLId]
    ON [DSEDiagnostic].[AggregatedStatsSessionInfo]([DDLId] ASC);


GO
CREATE NONCLUSTERED INDEX [Idx_AggregatedStatsSessionInfo_KeyspaceId]
    ON [DSEDiagnostic].[AggregatedStatsSessionInfo]([KeyspaceId] ASC);


GO
CREATE NONCLUSTERED INDEX [Idx_AggregatedStatsSessionInfo_ReferencedEventSessionId]
    ON [DSEDiagnostic].[AggregatedStatsSessionInfo]([ReferencedEventSessionId] ASC);


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Contains Aggregated Data from Nodetool and log based information for a run session', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'AggregatedStatsSessionInfo';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'CassandraLog,\nCFStats,\nTPStats,\nHistogram,\nOpsCenterRepairSession,\nCQL,\netc.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'AggregatedStatsSessionInfo', @level2type = N'COLUMN', @level2name = N'Source';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'e.g., Local Read Latency, Space used (total), MutationStage, Compacted partition, Compaction Rate, MemTable OPS, etc.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'AggregatedStatsSessionInfo', @level2type = N'COLUMN', @level2name = N'Attribute';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'e.g., Completed (e.g., Attr: MutationStage, ReadStage), Dropped (e.g., Attr: MUTATION, READ), Eden Space Change (e.g., Attr: GC), etc.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'AggregatedStatsSessionInfo', @level2type = N'COLUMN', @level2name = N'SubAttribute';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'If from a log souece this would be the starting timestamp within the log for node tool data this would be the node''s beginning uptime.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'AggregatedStatsSessionInfo', @level2type = N'COLUMN', @level2name = N'ReferenceDateTime';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Used to group related stats. for instance the Local Read data (count, latency)', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'AggregatedStatsSessionInfo', @level2type = N'COLUMN', @level2name = N'AggStatGroup';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Compaction\nAntiCompaction\nMemtableFlush\nGC\nPause\nRepair\nDrops\nPerformance\netc.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'AggregatedStatsSessionInfo', @level2type = N'COLUMN', @level2name = N'EventClass';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'The period of time (minutes) that makes up this aggregate. For node tool data this would be the node''s uptime', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'AggregatedStatsSessionInfo', @level2type = N'COLUMN', @level2name = N'AggregatePeriod_MIN';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'The UOM of the Value, Maximum, Minimum, Mean, etc. fields. This should be a standardized across different types. Time will be in milliseconds, memory/storage should be MB, rates should be MB/second, etc.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'AggregatedStatsSessionInfo', @level2type = N'COLUMN', @level2name = N'UnitOfMeasure';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'For node tool data the actual value (e.g., local read latency) for aggrated period data (freom logs), this would be the average value.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'AggregatedStatsSessionInfo', @level2type = N'COLUMN', @level2name = N'Value';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Null for Nodetool data, otherwise the maximum value from aggrate period from log stat', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'AggregatedStatsSessionInfo', @level2type = N'COLUMN', @level2name = N'Maximum';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Null for Nodetool data, otherwise the minimum value from aggrate period from log stat', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'AggregatedStatsSessionInfo', @level2type = N'COLUMN', @level2name = N'Minimum';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Null for Nodetool data, otherwise the Mena value from aggrate period from log stat', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'AggregatedStatsSessionInfo', @level2type = N'COLUMN', @level2name = N'Mean';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Null for Nodetool data, otherwise the StdDevp value from aggrate period from log stat', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'AggregatedStatsSessionInfo', @level2type = N'COLUMN', @level2name = N'StandardDeviation';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Number of Instances that make up Aggregate. For events the number of event instances that make up the Maximum, Minimum, Mean values.\n\nFor nodetool data this would be the assocated count. For example, Local Read Latency, this would be the Local Read Count.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'AggregatedStatsSessionInfo', @level2type = N'COLUMN', @level2name = N'NbrOccurrences';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Reference back to orginal event that generated this aggrated stats', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'AggregatedStatsSessionInfo', @level2type = N'COLUMN', @level2name = N'ReferencedEventSessionId';


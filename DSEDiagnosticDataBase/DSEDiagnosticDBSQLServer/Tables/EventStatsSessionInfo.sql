CREATE TABLE [DSEDiagnostic].[EventStatsSessionInfo] (
    [EventSessionId]      BIGINT             IDENTITY (1, 1) NOT NULL,
    [RunSessionId]        BIGINT             NOT NULL,
    [EventDateTimeBegin]  DATETIMEOFFSET (4) NOT NULL,
    [Source]              VARCHAR (25)       NOT NULL,
    [EventGroup]          UNIQUEIDENTIFIER   NULL,
    [DataCenterId]        BIGINT             NOT NULL,
    [Nodeid]              BIGINT             NULL,
    [AssociatedNode]      BIGINT             NULL,
    [KeyspaceId]          BIGINT             NULL,
    [DDLId]               BIGINT             NULL,
    [Event]               VARCHAR (250)      NULL,
    [Exception]           VARCHAR (250)      NULL,
    [ExceptionPath]       VARCHAR (250)      NULL,
    [EventType]           VARCHAR (25)       NOT NULL,
    [EventClass]          VARCHAR (50)       NULL,
    [AggregatePeriod_MIN] SMALLINT           NOT NULL,
    [Occurrences]         INT                CONSTRAINT [defo_EventStatsSessionInfo_Occurrences] DEFAULT ((1)) NOT NULL,
    [LastOccurrence]      DATETIMEOFFSET (7) NULL,
    CONSTRAINT [Pk_LogStatsSessionInfo_EventSessionId] PRIMARY KEY CLUSTERED ([EventSessionId] ASC),
    CONSTRAINT [fk_eventstatssessioninfo_AssocNodeId] FOREIGN KEY ([AssociatedNode]) REFERENCES [DSEDiagnostic].[Nodes] ([NodeId]),
    CONSTRAINT [fk_eventstatssessioninfo_DDLId] FOREIGN KEY ([DDLId]) REFERENCES [DSEDiagnostic].[ClusterDDLObjects] ([DDLId]),
    CONSTRAINT [fk_logstatssessioninfo] FOREIGN KEY ([RunSessionId]) REFERENCES [DSEDiagnostic].[RunSessionInfo] ([RunSessionId]) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT [fk_logstatssessioninfo_DataCenterId] FOREIGN KEY ([DataCenterId]) REFERENCES [DSEDiagnostic].[DataCenters] ([DataCenterId]) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT [fk_logstatssessioninfo_KeyspaceId] FOREIGN KEY ([KeyspaceId]) REFERENCES [DSEDiagnostic].[ClusterDDLObjects] ([DDLId]) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT [fk_logstatssessioninfo_NodeId] FOREIGN KEY ([Nodeid]) REFERENCES [DSEDiagnostic].[Nodes] ([NodeId]) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT [Idx_LogStatsSessionInfo_RunSessionId] UNIQUE NONCLUSTERED ([RunSessionId] ASC)
);


GO
CREATE NONCLUSTERED INDEX [Idx_LogStatsSessionInfo_DataCenterId]
    ON [DSEDiagnostic].[EventStatsSessionInfo]([DataCenterId] ASC);


GO
CREATE NONCLUSTERED INDEX [Idx_LogStatsSessionInfo_DDLId]
    ON [DSEDiagnostic].[EventStatsSessionInfo]([DDLId] ASC);


GO
CREATE NONCLUSTERED INDEX [Idx_LogStatsSessionInfo_KeyspaceId]
    ON [DSEDiagnostic].[EventStatsSessionInfo]([KeyspaceId] ASC);


GO
CREATE NONCLUSTERED INDEX [Idx_LogStatsSessionInfo_Nodeid]
    ON [DSEDiagnostic].[EventStatsSessionInfo]([Nodeid] ASC);


GO
CREATE NONCLUSTERED INDEX [Idx_EventStatsSessionInfo_AssociatedNode]
    ON [DSEDiagnostic].[EventStatsSessionInfo]([AssociatedNode] ASC);


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'This consist of information usually obtained from the log files but can come from generated events from nodetool data. This information is an aggregate of occurrences of an event within the aggregated time period. This information may generate aggregate stat information.\n\nExample:\n\nAn specific exception occured 10 times starting at 2018-01-01 22:45 for10 minutes (up to 22:55)\n15 GC occurred at 2018-01-01 22:45 for 10 minutes (note this would generate aggrate stat data like latency)', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'EventStatsSessionInfo';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Used to group related events together', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'EventStatsSessionInfo', @level2type = N'COLUMN', @level2name = N'EventGroup';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Node that is somehow associated to this event (e.g., downed node)', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'EventStatsSessionInfo', @level2type = N'COLUMN', @level2name = N'AssociatedNode';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Exception\nFatal\nCompaction\nAntiCompaction\nMemtableFlush\nGC\nPause\nRepair\nDrops\nPerformance\nOrphaned\nHintHandOff\nDataCenter\nNode\nKeyspace\nTableViewIndex\nConfig\nDetection\nNotHandled', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'EventStatsSessionInfo', @level2type = N'COLUMN', @level2name = N'EventClass';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'in ninutes', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'EventStatsSessionInfo', @level2type = N'COLUMN', @level2name = N'AggregatePeriod_MIN';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Number of events within the aggregated time period', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'EventStatsSessionInfo', @level2type = N'COLUMN', @level2name = N'Occurrences';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Last time the event occurred within the aggrated period.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'EventStatsSessionInfo', @level2type = N'COLUMN', @level2name = N'LastOccurrence';


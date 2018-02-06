CREATE TABLE [DSEDiagnostic].[NodeSessionInfo] (
    [NodeSessionId]          BIGINT             IDENTITY (1, 1) NOT NULL,
    [RunSessionId]           BIGINT             NOT NULL,
    [NodeId]                 BIGINT             NOT NULL,
    [DataCenterSessionId]    BIGINT             NOT NULL,
    [DSEHostId]              UNIQUEIDENTIFIER   NULL,
    [PhysicalHostId]         VARCHAR (100)      NULL,
    [RackSessionId]          BIGINT             NULL,
    [WorkloadType]           VARCHAR (50)       NULL,
    [NodeStatus]             VARCHAR (8)        NULL,
    [StorageUsed_MB]         DECIMAL (9, 4)     NULL,
    [StorageUtilization_PCT] DECIMAL (5, 2)     NULL,
    [HealthRating]           VARCHAR (25)       NULL,
    [NbrVNodes]              SMALLINT           NULL,
    [IsSeedNode]             BIT                NULL,
    [NbrExceptions]          SMALLINT           NULL,
    [NativeTransportEnabled] BIT                NULL,
    [DSEVersion]             VARCHAR (25)       NULL,
    [CassandraVersion]       VARCHAR (25)       NULL,
    [SolrVersion]            VARCHAR (25)       NULL,
    [SparkVersion]           VARCHAR (25)       NULL,
    [OpsCenterAgentVersion]  VARCHAR (25)       NULL,
    [EndPointSnitch]         VARCHAR (100)      NULL,
    [RepaiServicesEnabled]   BIT                NULL,
    [GossipEnabled]          BIT                NULL,
    [ThriftEnabled]          BIT                NULL,
    [NodeStartDateTime]      DATETIMEOFFSET (2) NOT NULL,
    [NodeUptimeDuration_SEC] BIGINT             NOT NULL,
    [LogDateTimeMin]         DATETIMEOFFSET (4) NOT NULL,
    [LogDateTimeMax]         DATETIMEOFFSET (4) NOT NULL,
    [LogGapDuration_SEC]     INT                NOT NULL,
    CONSTRAINT [Pk_NodeSessionInfo_NodeSessionId] PRIMARY KEY CLUSTERED ([NodeSessionId] ASC),
    CONSTRAINT [fk_nodesessioninfo_DataCenterId] FOREIGN KEY ([DataCenterSessionId]) REFERENCES [DSEDiagnostic].[DataCenterSessionInfo] ([DataCenterSessionId]) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT [fk_nodesessioninfo_nodes] FOREIGN KEY ([NodeId]) REFERENCES [DSEDiagnostic].[Nodes] ([NodeId]) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT [fk_nodesessioninfo_RackSession] FOREIGN KEY ([RackSessionId]) REFERENCES [DSEDiagnostic].[RackSessionInfo] ([RackSessionId]),
    CONSTRAINT [fk_nodesessioninfo_RunSessionId] FOREIGN KEY ([RunSessionId]) REFERENCES [DSEDiagnostic].[RunSessionInfo] ([RunSessionId]),
    CONSTRAINT [CUI_NodeSessionInfo_RunSessionId_NodeId] UNIQUE NONCLUSTERED ([RunSessionId] ASC, [NodeId] ASC)
);


GO
CREATE NONCLUSTERED INDEX [Idx_NodeSessionInfo_DataCenterId]
    ON [DSEDiagnostic].[NodeSessionInfo]([DataCenterSessionId] ASC);


GO
CREATE NONCLUSTERED INDEX [Idx_NodeSessionInfo_RunSessionId]
    ON [DSEDiagnostic].[NodeSessionInfo]([RunSessionId] ASC);


GO
CREATE NONCLUSTERED INDEX [Idx_NodeSessionInfo_NodeId]
    ON [DSEDiagnostic].[NodeSessionInfo]([NodeId] ASC);


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Node information assocated with a node for a run session', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'NodeSessionInfo';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Rack info can be obtained from the DSEConfigSessionInfo Table', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'NodeSessionInfo', @level2type = N'COLUMN', @level2name = N'RackSessionId';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'The datetime of when the noded started', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'NodeSessionInfo', @level2type = N'COLUMN', @level2name = N'NodeStartDateTime';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Amount of time node is up', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'NodeSessionInfo', @level2type = N'COLUMN', @level2name = N'NodeUptimeDuration_SEC';


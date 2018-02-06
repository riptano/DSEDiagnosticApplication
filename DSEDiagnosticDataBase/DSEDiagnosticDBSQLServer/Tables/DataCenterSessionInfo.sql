CREATE TABLE [DSEDiagnostic].[DataCenterSessionInfo] (
    [DataCenterSessionId]       BIGINT             IDENTITY (1, 1) NOT NULL,
    [RunSessionId]              BIGINT             NOT NULL,
    [DataCenterId]              BIGINT             NOT NULL,
    [NbrNodesCassandra]         SMALLINT           NOT NULL,
    [NbrNodesSearch]            SMALLINT           NULL,
    [NbrNodesAnalytics]         SMALLINT           NULL,
    [NbrNodesGraph]             SMALLINT           NULL,
    [NbrRacks]                  SMALLINT           NULL,
    [RacksBalanced]             BIT                NULL,
    [NbrApplicationKeyspaces]   SMALLINT           NOT NULL,
    [NbrTables]                 SMALLINT           NOT NULL,
    [NbrViews]                  SMALLINT           NULL,
    [NbrSecondaryIndexes]       SMALLINT           NULL,
    [NbrSolrIndexes]            SMALLINT           NULL,
    [NbrSASIndexes]             SMALLINT           NULL,
    [NbrCustomIndexes]          SMALLINT           NULL,
    [NbrTriggers]               SMALLINT           NULL,
    [NbrSTCS]                   SMALLINT           NULL,
    [NbrLCS]                    SMALLINT           NULL,
    [NbrDTCS]                   SMALLINT           NULL,
    [NbrTCS]                    SMALLINT           NULL,
    [NbrTWCS]                   SMALLINT           NULL,
    [NbrOtherStrategies]        SMALLINT           NULL,
    [NbrActiveObjects]          SMALLINT           NULL,
    [LogDateTimeMin]            DATETIMEOFFSET (4) NOT NULL,
    [LogDateTimeMax]            DATETIMEOFFSET (4) NOT NULL,
    [LogGapDuration_SEC]        INT                NOT NULL,
    [NbrLogFiles]               SMALLINT           NULL,
    [NodeupTimeMin]             DATETIMEOFFSET (2) NOT NULL,
    [NodeUpTimeDurationMax_SEC] INT                NOT NULL,
    [NodeUpTimeDurationMin_SEC] INT                NOT NULL,
    [NodeUpTimeAverage_SEC]     INT                NOT NULL,
    [NodeUpTimeStdDev_SEC]      INT                NOT NULL,
    [StorageUsed_MB]            DECIMAL (19, 4)    NULL,
    CONSTRAINT [Pk_DataCenterSessionInfo_DataCenterSessionId] PRIMARY KEY CLUSTERED ([DataCenterSessionId] ASC),
    CONSTRAINT [fk_datacentersessioninfo_DataCenterId] FOREIGN KEY ([DataCenterId]) REFERENCES [DSEDiagnostic].[DataCenters] ([DataCenterId]) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT [fk_datacentersessioninfo_RunSessionId] FOREIGN KEY ([RunSessionId]) REFERENCES [DSEDiagnostic].[RunSessionInfo] ([RunSessionId]) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT [Unq_DataCenterSessionInfo_RunSessionId] UNIQUE NONCLUSTERED ([RunSessionId] ASC, [DataCenterId] ASC)
);


GO
CREATE NONCLUSTERED INDEX [Idx_DataCenterSessionInfo_DataCenterId]
    ON [DSEDiagnostic].[DataCenterSessionInfo]([DataCenterId] ASC);


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'This consist of data center information assocated with a run session.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'DataCenterSessionInfo';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Total Nbr of CQL Objects that have been detected as activite', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'DataCenterSessionInfo', @level2type = N'COLUMN', @level2name = N'NbrActiveObjects';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Number of seconds of log gaps that were detected during processing.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'DataCenterSessionInfo', @level2type = N'COLUMN', @level2name = N'LogGapDuration_SEC';


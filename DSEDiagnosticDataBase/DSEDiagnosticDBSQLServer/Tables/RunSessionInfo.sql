CREATE TABLE [DSEDiagnostic].[RunSessionInfo] (
    [RunSessionId]                  BIGINT             IDENTITY (1, 1) NOT NULL,
    [ClusterId]                     BIGINT             NOT NULL,
    [RunDateTimeStart]              DATETIMEOFFSET (2) NOT NULL,
    [RunDateTimeFinsh]              DATETIMEOFFSET (2) NULL,
    [RunCompleted]                  BIT                CONSTRAINT [defo_RunSessionInfo_RunCompleted] DEFAULT ((0)) NOT NULL,
    [LogDateTimeMin]                DATETIMEOFFSET (4) NOT NULL,
    [LogDateTimeMax]                DATETIMEOFFSET (4) NOT NULL,
    [LogGapDuration_SEC]            INT                NULL,
    [NbrLogFiles]                   SMALLINT           NULL,
    [AnalysisBeginDateTime]         DATETIMEOFFSET (7) NOT NULL,
    [AnalysisEndDateTime]           DATETIMEOFFSET (7) NOT NULL,
    [NodeUpTimeMin]                 DATETIMEOFFSET (2) NOT NULL,
    [NodeUpTimeDurationMax_SEC]     INT                NOT NULL,
    [NodeUpTimeDurationMin_SEC]     INT                NOT NULL,
    [NodeUpTimeAverage_SEC]         INT                NOT NULL,
    [NodeUpTimeStdDev_SEC]          INT                NOT NULL,
    [NbrDataCenters]                SMALLINT           NULL,
    [NbrNodesCassandra]             SMALLINT           NULL,
    [NbrNodesSearch]                SMALLINT           NULL,
    [NbrNodesAnalytics]             SMALLINT           NULL,
    [NbrNodesGraph]                 SMALLINT           NULL,
    [NbrKeyspaces]                  SMALLINT           NULL,
    [NbrTables]                     SMALLINT           NULL,
    [StorageUsed_MB]                DECIMAL (19, 4)    NULL,
    [DetectedShardingSchemaChanges] BIT                NULL,
    [HealthAssessmentSurveyURL]     NVARCHAR (2083)    NULL,
    [HealthAssessmentExcelWorkURL]  NVARCHAR (2083)    NULL,
    [ClusterDiagnosicDirectoryURL]  NVARCHAR (2083)    NULL,
    [ApplicationAnalysisParams]     VARCHAR (MAX)      NULL,
    CONSTRAINT [PK_RunSessionId] PRIMARY KEY CLUSTERED ([RunSessionId] ASC),
    CONSTRAINT [fk_runsessioninfo_clusters] FOREIGN KEY ([ClusterId]) REFERENCES [DSEDiagnostic].[Clusters] ([ClusterId])
);


GO
CREATE NONCLUSTERED INDEX [Idx_RunSessionInfo_ClusterId]
    ON [DSEDiagnostic].[RunSessionInfo]([ClusterId] ASC);


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'A Run Session is one analysis review of diagnostic files for a cluster. You can think of this as a "snapshot" of the cluster''s enviroment at the analysis timeframe.\n\nNote: There should be a "system" session that is used for "global" objects like system keyspaces, etc. RunSessionId value should be 0...', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'RunSessionInfo';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Start time of the analysis, this will not be the same as the log timeframes or node up timeframes.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'RunSessionInfo', @level2type = N'COLUMN', @level2name = N'RunDateTimeStart';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'True if analysis completed', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'RunSessionInfo', @level2type = N'COLUMN', @level2name = N'RunCompleted';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'The overall minimual date time across all nodes reviewed for this run session', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'RunSessionInfo', @level2type = N'COLUMN', @level2name = N'LogDateTimeMin';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'The overall maximum date time across all nodes reviewed for this run session', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'RunSessionInfo', @level2type = N'COLUMN', @level2name = N'LogDateTimeMax';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'The total number of seconds for all gaps that where detected in the logs during processing across all nodes', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'RunSessionInfo', @level2type = N'COLUMN', @level2name = N'LogGapDuration_SEC';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'The actual beginning timeframe of the analysis for this run session', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'RunSessionInfo', @level2type = N'COLUMN', @level2name = N'AnalysisBeginDateTime';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'The actual ending timeframe of the analysis for this run session', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'RunSessionInfo', @level2type = N'COLUMN', @level2name = N'AnalysisEndDateTime';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Mimumal node begin timeframe across all nodes.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'RunSessionInfo', @level2type = N'COLUMN', @level2name = N'NodeUpTimeMin';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Maximal node Uptime across all nodes', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'RunSessionInfo', @level2type = N'COLUMN', @level2name = N'NodeUpTimeDurationMax_SEC';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Total storage used by this cluster at this analysis timeframe', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'RunSessionInfo', @level2type = N'COLUMN', @level2name = N'StorageUsed_MB';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'True if there are any detected sharding or schema changed events. Review the event session table for details.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'RunSessionInfo', @level2type = N'COLUMN', @level2name = N'DetectedShardingSchemaChanges';


CREATE TABLE [DSEDiagnostic].[DSEConfigSessionInfo] (
    [DataCenterSessionId] BIGINT             NOT NULL,
    [IsMajorityValue]     BIT                CONSTRAINT [defo_DSEConfigSessionInfo_IsMajorityValue] DEFAULT ((0)) NOT NULL,
    [IsCommon]            BIT                CONSTRAINT [defo_DSEConfigSessionInfo_IsCommon] DEFAULT ((0)) NOT NULL,
    [NodeSessionId]       BIGINT             NULL,
    [Source]              VARCHAR (25)       NOT NULL,
    [LogTimestamp]        DATETIMEOFFSET (4) NULL,
    [Property]            VARCHAR (250)      NOT NULL,
    [Value]               VARCHAR (250)      NULL,
    CONSTRAINT [fk_dseconfigsessioninfo] FOREIGN KEY ([DataCenterSessionId]) REFERENCES [DSEDiagnostic].[DataCenterSessionInfo] ([DataCenterSessionId]) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT [fk_dseconfigsessioninfo_NodeSession] FOREIGN KEY ([NodeSessionId]) REFERENCES [DSEDiagnostic].[NodeSessionInfo] ([NodeSessionId]),
    CONSTRAINT [CUI_DSEConfigSessionInfo_DC_Sess_Prop] UNIQUE NONCLUSTERED ([DataCenterSessionId] ASC, [IsMajorityValue] ASC, [IsCommon] ASC, [NodeSessionId] ASC, [Source] ASC, [LogTimestamp] ASC, [Property] ASC)
);


GO
CREATE NONCLUSTERED INDEX [Idx_DSEConfigSessionInfo_NodeSessionId]
    ON [DSEDiagnostic].[DSEConfigSessionInfo]([NodeSessionId] ASC);


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Contains DSE node configuration information for a run session. This information can come from env, yaml, and event log files (Nodes config that is dumped upon start up), and cassandra topology (racks).', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'DSEConfigSessionInfo';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'If true major of nodes in the data center have the same value. IsCommon may or may not be true.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'DSEConfigSessionInfo', @level2type = N'COLUMN', @level2name = N'IsMajorityValue';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'If true, all nodes in the DC have this value (implies IsMajorityValue is also true). If false, One or more nodes in the DC do NOT have this value.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'DSEConfigSessionInfo', @level2type = N'COLUMN', @level2name = N'IsCommon';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'If not null, this node''s property value is different from the common value (based on the majority of the nodes in the DC) for this data center. In this case IsCommon and IsMajorityValue will be false.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'DSEConfigSessionInfo', @level2type = N'COLUMN', @level2name = N'NodeSessionId';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'The source of the config information. Examples are Cassandra, DSE, Hadoop, Solr, Spark, Snitch, OpsCenter. There is also a sub-source (e.g., yaml, env, log, etc.). Example of a complete source is Cassandra.yaml.\n\nNote this value can come from the log!', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'DSEConfigSessionInfo', @level2type = N'COLUMN', @level2name = N'Source';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'If source is from a log, this is the event''s timestamp occurrence.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'DSEConfigSessionInfo', @level2type = N'COLUMN', @level2name = N'LogTimestamp';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'The config property. Examples are seed_provider[1].class_name, seed_provider[1].parameters[1].seeds, etc.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'DSEConfigSessionInfo', @level2type = N'COLUMN', @level2name = N'Property';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Config Value', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'DSEConfigSessionInfo', @level2type = N'COLUMN', @level2name = N'Value';


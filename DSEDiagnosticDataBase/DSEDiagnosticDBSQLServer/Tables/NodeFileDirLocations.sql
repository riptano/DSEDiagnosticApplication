CREATE TABLE [DSEDiagnostic].[NodeFileDirLocations] (
    [NodeSessionId] BIGINT         NOT NULL,
    [Sequence]      TINYINT        CONSTRAINT [defo_NodeFileDirLocations_Sequence] DEFAULT ((0)) NOT NULL,
    [Property]      VARCHAR (25)   NOT NULL,
    [IsFile]        BIT            NOT NULL,
    [Value]         VARCHAR (2089) NOT NULL,
    CONSTRAINT [fk_nodefiledirlocations] FOREIGN KEY ([NodeSessionId]) REFERENCES [DSEDiagnostic].[NodeSessionInfo] ([NodeSessionId]) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT [UCI_NodeFileDirLocations_NodeSess_Seq_Prop] UNIQUE NONCLUSTERED ([NodeSessionId] ASC, [Sequence] ASC, [Property] ASC)
);


GO
CREATE NONCLUSTERED INDEX [Idx_NodeFileDirLocations_NodeSessionId]
    ON [DSEDiagnostic].[NodeFileDirLocations]([NodeSessionId] ASC);


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Used in cases when more then one file/dir is used for a property. e.g., DataDir', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'NodeFileDirLocations', @level2type = N'COLUMN', @level2name = N'Sequence';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'e.g., Cassandra.yaml, DSE.yaml, CommitDir, DataDir, etc.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'NodeFileDirLocations', @level2type = N'COLUMN', @level2name = N'Property';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'If true, the location is a file, False it is a directory', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'NodeFileDirLocations', @level2type = N'COLUMN', @level2name = N'IsFile';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'The file or directory path', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'NodeFileDirLocations', @level2type = N'COLUMN', @level2name = N'Value';


CREATE TABLE [DSEDiagnostic].[ClusterDDLObjects] (
    [DDLId]          BIGINT        IDENTITY (1, 1) NOT NULL,
    [ClusterId]      BIGINT        NOT NULL,
    [Type]           VARCHAR (25)  NOT NULL,
    [Name]           VARCHAR (100) NOT NULL,
    [RelatedDDLItem] BIGINT        NULL,
    [Active]         BIT           CONSTRAINT [defo_ClusterDDLObjects_Active] DEFAULT ((1)) NOT NULL,
    CONSTRAINT [Pk_ClusterDDLObjects_DDLId] PRIMARY KEY CLUSTERED ([DDLId] ASC),
    CONSTRAINT [fk_clusterddlobjects] FOREIGN KEY ([RelatedDDLItem]) REFERENCES [DSEDiagnostic].[ClusterDDLObjects] ([DDLId]),
    CONSTRAINT [fk_clusterddlobjects_clusters] FOREIGN KEY ([ClusterId]) REFERENCES [DSEDiagnostic].[Clusters] ([ClusterId]) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT [UIdx_ClusterDDLObjects] UNIQUE NONCLUSTERED ([ClusterId] ASC, [Type] ASC, [Name] ASC),
    CONSTRAINT [UIdx_ClusterDDLObjects_Id_Name] UNIQUE NONCLUSTERED ([Name] ASC, [DDLId] ASC)
);


GO
CREATE NONCLUSTERED INDEX [Idx_ClusterDDLObjects_RelatedDDLItem]
    ON [DSEDiagnostic].[ClusterDDLObjects]([RelatedDDLItem] ASC);


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Contents DDL objects associated with this cluster', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'ClusterDDLObjects';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'e.g., Keyspace, Table, Index, SolrIndex, Materialized View, UUID, etc.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'ClusterDDLObjects', @level2type = N'COLUMN', @level2name = N'Type';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'For objects within a keyspace the keyspace name shouuld be included (e.g., keyspac1.tableA).', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'ClusterDDLObjects', @level2type = N'COLUMN', @level2name = N'Name';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'e.g., for a table points to the keyspce item', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'ClusterDDLObjects', @level2type = N'COLUMN', @level2name = N'RelatedDDLItem';


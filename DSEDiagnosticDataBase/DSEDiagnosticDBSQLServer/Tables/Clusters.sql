CREATE TABLE [DSEDiagnostic].[Clusters] (
    [ClusterId]                 BIGINT        IDENTITY (1, 1) NOT NULL,
    [CustId]                    BIGINT        NOT NULL,
    [ClusterName]               VARCHAR (100) NOT NULL,
    [LastCompletedRunSessionId] BIGINT        NULL,
    CONSTRAINT [PK_ClusterId] PRIMARY KEY CLUSTERED ([ClusterId] ASC),
    CONSTRAINT [fk_clusters_customers] FOREIGN KEY ([CustId]) REFERENCES [DSEDiagnostic].[Customers] ([CustId]) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT [ClusterName_CustId_Unique_Key] UNIQUE NONCLUSTERED ([ClusterName] ASC, [CustId] ASC)
);


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'The name of the cluster assocated with the custome.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'Clusters';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'The last completed run session for this cluster', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'Clusters', @level2type = N'COLUMN', @level2name = N'LastCompletedRunSessionId';


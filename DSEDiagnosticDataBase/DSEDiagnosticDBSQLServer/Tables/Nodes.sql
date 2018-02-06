CREATE TABLE [DSEDiagnostic].[Nodes] (
    [NodeId]       BIGINT        IDENTITY (1, 1) NOT NULL,
    [DataCenterId] BIGINT        NOT NULL,
    [ClusterId]    BIGINT        NOT NULL,
    [IP4Address]   VARCHAR (12)  NULL,
    [IP6Address]   VARCHAR (45)  NULL,
    [HostName]     VARCHAR (100) NULL,
    [Active]       BIT           CONSTRAINT [defo_Nodes_Active] DEFAULT ((1)) NULL,
    CONSTRAINT [PK_NodeId] PRIMARY KEY CLUSTERED ([NodeId] ASC),
    CONSTRAINT [Cns_NodeCluster] CHECK ([IP4Address] IS NOT NULL OR [IP6Address] IS NOT NULL OR [HostName] IS NOT NULL),
    CONSTRAINT [fk_nodecluster_clusters] FOREIGN KEY ([ClusterId]) REFERENCES [DSEDiagnostic].[Clusters] ([ClusterId]),
    CONSTRAINT [fk_nodes_datacenters] FOREIGN KEY ([DataCenterId]) REFERENCES [DSEDiagnostic].[DataCenters] ([DataCenterId]),
    CONSTRAINT [Idx_NodeCluster_ClusterId] UNIQUE NONCLUSTERED ([NodeId] ASC, [ClusterId] ASC)
);


GO
CREATE NONCLUSTERED INDEX [Idx_Nodes_DataCenterId]
    ON [DSEDiagnostic].[Nodes]([DataCenterId] ASC);


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'IP address or Host Name information assocated to a node.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'Nodes';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'If false, this node is no longer active (been removed from the cluster)', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'Nodes', @level2type = N'COLUMN', @level2name = N'Active';


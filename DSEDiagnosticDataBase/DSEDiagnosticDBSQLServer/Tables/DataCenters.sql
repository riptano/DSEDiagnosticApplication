CREATE TABLE [DSEDiagnostic].[DataCenters] (
    [DataCenterId] BIGINT        NOT NULL,
    [ClusterId]    BIGINT        NOT NULL,
    [Name]         VARCHAR (100) NOT NULL,
    [Active]       BIT           CONSTRAINT [defo_DataCenters_Active] DEFAULT ((1)) NULL,
    CONSTRAINT [Pk_DataCenterId_DataCenterId] PRIMARY KEY CLUSTERED ([DataCenterId] ASC),
    CONSTRAINT [fk_datacenterid_clusters] FOREIGN KEY ([ClusterId]) REFERENCES [DSEDiagnostic].[Clusters] ([ClusterId])
);


GO
CREATE NONCLUSTERED INDEX [Idx_DataCenterId_ClusterId]
    ON [DSEDiagnostic].[DataCenters]([ClusterId] ASC);


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'The data center associated to a cluster.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'DataCenters';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'If false, this data center is no longer active (removed from the cluster)', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'DataCenters', @level2type = N'COLUMN', @level2name = N'Active';


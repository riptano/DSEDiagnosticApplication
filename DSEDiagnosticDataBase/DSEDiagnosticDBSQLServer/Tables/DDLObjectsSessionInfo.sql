CREATE TABLE [DSEDiagnostic].[DDLObjectsSessionInfo] (
    [DDLSessionId]              BIGINT         IDENTITY (1, 1) NOT NULL,
    [RunSessionId]              BIGINT         NOT NULL,
    [KeyspaceSessionId]         BIGINT         NOT NULL,
    [DDLId]                     BIGINT         NOT NULL,
    [Name]                      VARCHAR (100)  NULL,
    [DDLType]                   VARCHAR (25)   NOT NULL,
    [NbrPartitionKeys]          SMALLINT       NOT NULL,
    [PartitionKeyHasCollection] BIT            NULL,
    [PartitionKeyHasBlob]       BIT            NULL,
    [PartitionKeyHasUDT]        BIT            NULL,
    [PartitionKeyHasTuple]      BIT            NULL,
    [NbrClusterColumns]         TINYINT        NULL,
    [ClusterColHasCollection]   BIT            NULL,
    [ClusterColHasBlob]         BIT            NULL,
    [ClusterColHasUDT]          BIT            NULL,
    [ClusterColHasTuple]        BIT            NULL,
    [CompactionStrategy]        VARCHAR (50)   NULL,
    [Compression]               VARCHAR (25)   NULL,
    [ReadRepairChance]          DECIMAL (5, 2) NULL,
    [ReadRepairDCChance]        DECIMAL (5, 2) NULL,
    [ReadRepairPolicy]          VARCHAR (25)   NULL,
    [GCGracePeriod_SEC]         BIGINT         NULL,
    [TTL_SEC]                   BIGINT         NULL,
    [AssociatedTableId]         BIGINT         NULL,
    [NbrCollections]            TINYINT        NULL,
    [NbrCounters]               TINYINT        NULL,
    [NbrBobs]                   TINYINT        NULL,
    [NbrStatic]                 TINYINT        NULL,
    [NbrFrozen]                 TINYINT        NULL,
    [NbrTuples]                 TINYINT        NULL,
    [NbrUDTs]                   TINYINT        NULL,
    [NbrOfAssociatedObjcts]     TINYINT        NULL,
    [NbrOrderByCols]            TINYINT        NULL,
    [OrderByHasCollection]      BIT            NULL,
    [OrderByHasUDT]             BIT            NULL,
    [OrderByHasTuple]           BIT            NULL,
    [OrderByHasBlob]            BIT            NULL,
    [IsActive]                  BIT            NULL,
    [NbrProperties]             TINYINT        NULL,
    [Storage_Used_MB]           DECIMAL (9, 4) NULL,
    [DDLStrCheckSum]            INT            NULL,
    CONSTRAINT [Pk_DDLObjectsSessionInfo_TableId] PRIMARY KEY CLUSTERED ([DDLSessionId] ASC),
    CONSTRAINT [fk_ddlobjectssessioninfo] FOREIGN KEY ([Name], [DDLId]) REFERENCES [DSEDiagnostic].[ClusterDDLObjects] ([Name], [DDLId]) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT [fk_ddlobjectssessioninfo_KeyspaceId] FOREIGN KEY ([KeyspaceSessionId]) REFERENCES [DSEDiagnostic].[KeyspaceSessionInfo] ([KeyspaceSessionId]) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT [fk_ddlobjectssessioninfo_RunSessionId] FOREIGN KEY ([RunSessionId]) REFERENCES [DSEDiagnostic].[RunSessionInfo] ([RunSessionId]),
    CONSTRAINT [UIdx_DDLObjectsSessionInfo_KeyspaceSession_Name] UNIQUE NONCLUSTERED ([KeyspaceSessionId] ASC, [Name] ASC)
);


GO
CREATE NONCLUSTERED INDEX [Idx_DDLObjectsSessionInfo_RunSessionId]
    ON [DSEDiagnostic].[DDLObjectsSessionInfo]([RunSessionId] ASC);


GO
CREATE NONCLUSTERED INDEX [Idx_DDLObjectsSessionInfo_DDLId]
    ON [DSEDiagnostic].[DDLObjectsSessionInfo]([DDLId] ASC);


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'This is the DDL that defines the data model used for this run session. \n\nNo actual DDL will be stored due to security/proprietary concerns by customers.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'DDLObjectsSessionInfo';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'e.g., Table, solrindex, materizedview, user type, etc.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'DDLObjectsSessionInfo', @level2type = N'COLUMN', @level2name = N'DDLType';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Reference back to this table in cases of indexes, views, etc. Otherwise this is null', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'DDLObjectsSessionInfo', @level2type = N'COLUMN', @level2name = N'AssociatedTableId';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'The number of assocated objects (e.g., indexes)', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'DDLObjectsSessionInfo', @level2type = N'COLUMN', @level2name = N'NbrOfAssociatedObjcts';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'The checksum of the DDL string as a fast check to see if different session has different values.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'DDLObjectsSessionInfo', @level2type = N'COLUMN', @level2name = N'DDLStrCheckSum';


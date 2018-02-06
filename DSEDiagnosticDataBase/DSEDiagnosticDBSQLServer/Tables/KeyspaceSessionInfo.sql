CREATE TABLE [DSEDiagnostic].[KeyspaceSessionInfo] (
    [KeyspaceSessionId]             BIGINT          IDENTITY (1, 1) NOT NULL,
    [RunSessionId]                  BIGINT          NOT NULL,
    [DDLId]                         BIGINT          NOT NULL,
    [Name]                          VARCHAR (100)   NOT NULL,
    [ReplicationStrategy]           VARCHAR (100)   NOT NULL,
    [DurableWrites]                 BIT             NULL,
    [NbrTables]                     SMALLINT        NULL,
    [NbrViews]                      SMALLINT        NULL,
    [NbrSecondaryIndexes]           SMALLINT        NULL,
    [NbrSolrIndexes]                SMALLINT        NULL,
    [NbrCustomIndexes]              SMALLINT        NULL,
    [NbrTriggers]                   SMALLINT        NULL,
    [NbrSTCS]                       SMALLINT        NULL,
    [NbrLCS]                        SMALLINT        NULL,
    [NbrDTCS]                       SMALLINT        NULL,
    [NbrTCS]                        SMALLINT        NULL,
    [NbrTWCS]                       SMALLINT        NULL,
    [NbrSASIndexes]                 SMALLINT        NULL,
    [NbrOtherStrategies]            SMALLINT        NULL,
    [NbrActiveObjects]              SMALLINT        NULL,
    [Storage_Used_MB]               DECIMAL (19, 4) NULL,
    [DetectedShardingSchemaChanges] BIT             NULL,
    [DDLStrCheckSum]                INT             NULL,
    CONSTRAINT [Pk_KeyspaceSessionInfo_KeyspaceId] PRIMARY KEY CLUSTERED ([KeyspaceSessionId] ASC),
    CONSTRAINT [fk_keyspacesessioninfo] FOREIGN KEY ([Name], [DDLId]) REFERENCES [DSEDiagnostic].[ClusterDDLObjects] ([Name], [DDLId]),
    CONSTRAINT [fk_keyspacesessioninfo_RunSessionId] FOREIGN KEY ([RunSessionId]) REFERENCES [DSEDiagnostic].[RunSessionInfo] ([RunSessionId]) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT [UIdx_KeyspaceSessionInfo_DDL_RunSession] UNIQUE NONCLUSTERED ([DDLId] ASC, [RunSessionId] ASC)
);


GO
CREATE NONCLUSTERED INDEX [Idx_KeyspaceSessionInfo_DDLId]
    ON [DSEDiagnostic].[KeyspaceSessionInfo]([DDLId] ASC);


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Consists of run session information about the keyspace (data model at the time of the run)', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'KeyspaceSessionInfo';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'For system and DSE keyspaces this should be 0 and RunSessionId should be 0', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'KeyspaceSessionInfo', @level2type = N'COLUMN', @level2name = N'KeyspaceSessionId';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'For System and DSE keyspaces this should be the default system session (e.g., 0)', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'KeyspaceSessionInfo', @level2type = N'COLUMN', @level2name = N'RunSessionId';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'The checksum of the DDL string as a fast check to see if different session has different values.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'KeyspaceSessionInfo', @level2type = N'COLUMN', @level2name = N'DDLStrCheckSum';


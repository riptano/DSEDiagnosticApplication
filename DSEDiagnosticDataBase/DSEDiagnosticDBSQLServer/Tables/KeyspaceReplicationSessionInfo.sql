CREATE TABLE [DSEDiagnostic].[KeyspaceReplicationSessionInfo] (
    [KeyspaceSessionId] BIGINT   NOT NULL,
    [DataCenterId]      BIGINT   NOT NULL,
    [ReplicationFactor] SMALLINT NOT NULL,
    CONSTRAINT [fk_keyspacereplicationsessioninfo_DataCenterId] FOREIGN KEY ([DataCenterId]) REFERENCES [DSEDiagnostic].[DataCenters] ([DataCenterId]) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT [fk_keyspacereplicationsessioninfo_KeyspaceId] FOREIGN KEY ([KeyspaceSessionId]) REFERENCES [DSEDiagnostic].[KeyspaceSessionInfo] ([KeyspaceSessionId]) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT [UIdx_KeyspaceReplicationSessionInfo_KeyspaceId] UNIQUE NONCLUSTERED ([KeyspaceSessionId] ASC, [DataCenterId] ASC)
);


GO
CREATE NONCLUSTERED INDEX [Idx_KeyspaceReplicationSessionInfo_DataCenterId]
    ON [DSEDiagnostic].[KeyspaceReplicationSessionInfo]([DataCenterId] ASC);


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Contents information for a run session that assocates the keyspace to a data center with replication factor', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'KeyspaceReplicationSessionInfo';


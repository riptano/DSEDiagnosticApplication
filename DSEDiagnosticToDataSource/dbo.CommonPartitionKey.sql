CREATE TABLE [dbo].[CommonPartitionKey] (
	[DiagnosticId]			VARCHAR (115) NOT NULL,
	[RowId]					   BIGINT NOT NULL,
    [Partition Key]         VARCHAR (255) NOT NULL,
    [Data Center]           VARCHAR (100) NULL,
    [Keyspace Name]         VARCHAR (255) NULL,
    [Table Name]            VARCHAR (255) NULL,
    [Across Tables]         INT           NULL,
    [Factor]                DECIMAL (18)  NULL,
    [Reads]                 BIGINT        NULL,
    [Read StdDev]           DECIMAL (18)  NULL,
    [Read Factor]           DECIMAL (18)  NULL,
    [Writes]                BIGINT        NULL,
    [Write StdDev]          DECIMAL (18)  NULL,
    [Write Factor]          DECIMAL (18)  NULL,
    [Partition Keys]        BIGINT        NULL,
    [Partition Keys StdDev] DECIMAL (18)  NULL,
    [Partition Keys Factor] DECIMAL (18)  NULL,
    [Storage]               DECIMAL (18)  NULL, 
    CONSTRAINT [PK_CommonPartitionKey] PRIMARY KEY ([DiagnosticId], [RowId])
);


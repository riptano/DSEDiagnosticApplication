CREATE TABLE [dbo].[Large Partition Wide Rows] (
    [DiagnosticId]                   VARCHAR (115) NOT NULL,
    [RowId]                          BIGINT        NOT NULL,
	[Keyspace/Compaction/Type] VARCHAR (255) NULL,
    [Table Name]               VARCHAR (255) NULL,
    [Partition Size Max]       DECIMAL (18)  NULL,
    [Partition Size Min]       DECIMAL (18)  NULL,
    [Partition Size Avg]       DECIMAL (18)  NULL,
    [Keys Percent]             DECIMAL (18)  NULL,
    [Storage Percent]          DECIMAL (18)  NULL,
    [Read Factor]              DECIMAL (18)  NULL,
    [Read Percent]             DECIMAL (18)  NULL,
    [Write Factor]             DECIMAL (18)  NULL,
    [Write Percent]            DECIMAL (18)  NULL,
    [Tombstone Ratio Factor]   DECIMAL (18)  NULL,
    [SSTables Factor]          DECIMAL (18)  NULL,
    [Secondary Indexes]        INT           NULL,
    [Materialized Views]       INT           NULL,
    [Common Key]               VARCHAR (255) NULL,
    [Common Key Factor]        DECIMAL (18)  NULL,
    [Base Table Factor]        DECIMAL (18)  NULL, 
    CONSTRAINT [PK_Large Partition Wide Rows] PRIMARY KEY ([DiagnosticId], [RowId])
);


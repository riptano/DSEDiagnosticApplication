﻿CREATE TABLE [dbo].[Tombstones] (
    [DiagnosticId]                   VARCHAR (115) NOT NULL,
    [RowId]                          BIGINT        NOT NULL,
	[Keyspace/Compaction/Type] VARCHAR (255) NULL,
    [Table Name]               VARCHAR (255) NULL,
    [Tombstone Ratio Max]      DECIMAL (18)  NULL,
    [Tombstone Ratio Min]      DECIMAL (18)  NULL,
    [Tombstone Ratio Avg]      DECIMAL (18)  NULL,
    [Tombstones Read]          BIGINT        NULL,
    [Live Read]                BIGINT        NULL,
    [Keys Percent]             DECIMAL (18)  NULL,
    [Storage Percent]          DECIMAL (18)  NULL,
    [Read Factor]              DECIMAL (18)  NULL,
    [Read Percent]             DECIMAL (18)  NULL,
    [Write Factor]             DECIMAL (18)  NULL,
    [Write Percent]            DECIMAL (18)  NULL,
    [SSTables Factor]          DECIMAL (18)  NULL,
    [Partition Size Factor]    DECIMAL (18)  NULL,
    [Secondary Indexes]        INT           NULL,
    [Materialized Views]       INT           NULL,
    [Common Key]               VARCHAR (255) NULL,
    [Common Key Factor]        DECIMAL (18)  NULL,
    [Base Table Factor]        DECIMAL (18)  NULL, 
    CONSTRAINT [PK_Tombstones] PRIMARY KEY ([DiagnosticId], [RowId])
);

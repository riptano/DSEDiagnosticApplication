CREATE TABLE [dbo].[PFDataCenter] (
    [DiagnosticId]             VARCHAR (115) NOT NULL,
	[Data Center]			   VARCHAR (100) NOT NULL,
    [Keyspace/Compaction/Type] VARCHAR (255) NOT NULL,
    [Table Name]               VARCHAR (255) NOT NULL,  
	[DC ABBR]				   VARCHAR (20)  NULL, 
    [Read Factor]              DECIMAL (18)  NULL,   
	[Write Factor]             DECIMAL (18)  NULL,
    [SSTables Factor]          DECIMAL (18)  NULL,
    [Tombstone Ratio Factor]   DECIMAL (18)  NULL,   
	[Partition Factor]			DECIMAL (18)  NULL,   
    CONSTRAINT [PK_PFDataCenter] PRIMARY KEY ([DiagnosticId], [Data Center], [Keyspace/Compaction/Type], [Table Name])
);
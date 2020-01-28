CREATE TABLE [dbo].[AggregatedStats] (
	[DiagnosticId]			   VARCHAR (115) NOT NULL,
	[RowId]					   BIGINT NOT NULL,
    [Source]                   VARCHAR (25) NOT NULL,
    [Type]                     VARCHAR (100) NOT NULL,
    [Data Center]              VARCHAR (100) NULL,
    [Node IPAddress]           VARCHAR (55) NOT NULL,
    [Keyspace Name]            VARCHAR (255) NULL,
    [Table Name]               VARCHAR (255)  NULL,
    [CQL Type]                 VARCHAR (25) NULL,
    [Properties]               VARCHAR (50) NULL,
    [Attribute]                VARCHAR (255) NOT NULL,
    [Value]                    VARCHAR (MAX) NULL,
    [NumericValue]             VARCHAR (MAX) NULL,
    [Unit of Measure]          VARCHAR (255) NULL,
    [Raw Value]                VARCHAR (MAX) NULL,
    [Active]                   BIT           NULL,
    [Reconciliation Reference] VARCHAR (MAX) NULL, 
    CONSTRAINT [PK_AggregatedStats] PRIMARY KEY ([DiagnosticId], [RowId])
);


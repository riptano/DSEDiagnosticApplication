CREATE TABLE [dbo].[DSENodeConfigChanges] (
	[DiagnosticId]	     VARCHAR (115) NOT NULL,
	[RowId]				   BIGINT NOT NULL,
    [Data Center]          VARCHAR (100) NOT NULL,
    [Node IPAddress]       VARCHAR (55) NOT NULL,
    [UTC Timestamp]        DATETIME      NOT NULL,
    [Log Local Timestamp]  DATETIME      NULL,
    [Log Time Zone Offset] VARCHAR (255) NULL,
    [Type]                 VARCHAR (50) NULL,
    [Property]             VARCHAR (MAX) NOT NULL,
    [Value]                VARCHAR (MAX) NULL,
    [Current Value]        VARCHAR (MAX) NULL, 
    CONSTRAINT [PK_DSENodeConfigChanges] PRIMARY KEY ([DiagnosticId], [RowId])
);


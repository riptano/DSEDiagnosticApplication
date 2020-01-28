CREATE TABLE [dbo].[DSEDevices] (
    [DiagnosticId]	     VARCHAR (115) NOT NULL,
	[RowId]				 BIGINT NOT NULL,
	[Node IPAddress]     VARCHAR (55) NOT NULL,
    [Data Center]        VARCHAR (100) NOT NULL,
    [Data]               VARCHAR (255) NULL,
    [Data Utilization]   DECIMAL (18)  NULL,
    [Commit Log]         VARCHAR (255) NULL,
    [Commit Utilization] DECIMAL (18)  NULL,
    [Saved Cache]        VARCHAR (255) NULL,
    [Cache Utilization]  DECIMAL (18)  NULL,
    [Other]              VARCHAR (255) NULL,
    [Other Utilization]  DECIMAL (18)  NULL, 
    CONSTRAINT [PK_DSEDevices] PRIMARY KEY ([DiagnosticId], [RowId])
);


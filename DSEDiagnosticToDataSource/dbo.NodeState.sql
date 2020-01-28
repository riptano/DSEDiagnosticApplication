CREATE TABLE [dbo].[NodeState] (
	[DiagnosticId]	      VARCHAR (115) NOT NULL,
	[RowId]				  BIGINT NOT NULL,
    [UTC Timestamp]       DATETIME      NOT NULL,
    [Log Local Timestamp] DATETIME      NULL,
    [Data Center]         VARCHAR (100) NOT NULL,
    [Node IPAddress]      VARCHAR (55) NOT NULL,
    [Sort Order]          INT           NOT NULL,
    [State]               VARCHAR (50) NOT NULL,
    [Duration]            BIGINT        NULL,
    [Source Node]         VARCHAR (45) NULL, 
    CONSTRAINT [PK_NodeState] PRIMARY KEY ([DiagnosticId], [RowId])
);


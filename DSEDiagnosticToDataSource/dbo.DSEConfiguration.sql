CREATE TABLE [dbo].[DSEConfiguration] (
    [DiagnosticId]	 VARCHAR (115) NOT NULL,
	[RowId]			 BIGINT NOT NULL,
	[Data Center]    VARCHAR (100) NOT NULL,
    [Node IPAddress] VARCHAR (MAX) NOT NULL,
    [Yaml Type]      VARCHAR (50) NOT NULL,
    [Property]       VARCHAR (MAX) NOT NULL,
    [Value]          VARCHAR (MAX) NULL, 
    CONSTRAINT [PK_DSEConfiguration] PRIMARY KEY ([DiagnosticId], [RowId])
);


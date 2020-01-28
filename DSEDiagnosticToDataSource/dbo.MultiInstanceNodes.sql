CREATE TABLE [dbo].[MultiInstanceNodes] (
	[DiagnosticId]				  VARCHAR (115) NOT NULL,
    [Multi-Instance Server Id] VARCHAR (38) NOT NULL,
    [Data Center]              VARCHAR (100) NULL,
    [Node IPAddress]           VARCHAR (55) NOT NULL,
    [Rack]                     VARCHAR (150) NULL,
    [Instance Type]            VARCHAR (50) NULL,
    [Host Names]               VARCHAR (255) NULL,
    CONSTRAINT [PK_MultiInstanceNodes] PRIMARY KEY CLUSTERED ([DiagnosticId] ASC, [Multi-Instance Server Id] ASC, [Node IPAddress] ASC)
);


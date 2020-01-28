CREATE TABLE [dbo].[TokenRanges] (
    [DiagnosticId]			  VARCHAR (115) NOT NULL,
	[RowId]					  BIGINT NOT NULL,
	[Data Center]             VARCHAR (100) NOT NULL,
    [Node IPAddress]          VARCHAR (55) NOT NULL,
    [Start Token (exclusive)] VARCHAR (25) NOT NULL,
    [End Token (inclusive)]   VARCHAR (25) NULL,
    [Slots]                   VARCHAR (25) NOT NULL,
    [Load(MB)]                DECIMAL (18)  NULL,
    [Wraps Range]             BIT           NULL, 
    CONSTRAINT [PK_TokenRanges] PRIMARY KEY ([DiagnosticId], [RowId])
);


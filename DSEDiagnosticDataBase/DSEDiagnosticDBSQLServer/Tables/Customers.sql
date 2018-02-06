CREATE TABLE [DSEDiagnostic].[Customers] (
    [CustId]       BIGINT        IDENTITY (1, 1) NOT NULL,
    [Company]      VARCHAR (100) NOT NULL,
    [Organization] VARCHAR (100) NOT NULL,
    CONSTRAINT [PK_CustId] PRIMARY KEY CLUSTERED ([CustId] ASC),
    CONSTRAINT [CustCompanyOrg_Unique_Key] UNIQUE NONCLUSTERED ([Company] ASC, [Organization] ASC)
);


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Customer information.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'Customers';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Customer Name', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'Customers', @level2type = N'COLUMN', @level2name = N'Company';


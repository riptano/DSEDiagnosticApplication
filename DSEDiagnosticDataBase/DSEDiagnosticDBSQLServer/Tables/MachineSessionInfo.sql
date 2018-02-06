CREATE TABLE [DSEDiagnostic].[MachineSessionInfo] (
    [NodeSessionId]             BIGINT         NOT NULL,
    [CPUArchitecture]           VARCHAR (25)   NULL,
    [Cores]                     INT            NULL,
    [PhysicalMemory_MB]         DECIMAL (9, 4) NULL,
    [OS]                        VARCHAR (25)   NULL,
    [OSVersion]                 VARCHAR (25)   NULL,
    [Kernel]                    VARCHAR (25)   NULL,
    [TimeZone]                  VARCHAR (25)   NOT NULL,
    [TimeZoneOffset]            SMALLINT       NOT NULL,
    [CPU_Average]               DECIMAL (5, 2) NULL,
    [CPU_Idle]                  DECIMAL (5, 2) NULL,
    [CPU_System]                DECIMAL (5, 2) NULL,
    [CPU_User]                  DECIMAL (5, 2) NULL,
    [Memory_Available_MB]       DECIMAL (9, 4) NULL,
    [Memory_Cache_MB]           DECIMAL (9, 4) NULL,
    [Memory_Buffer_MB]          DECIMAL (9, 4) NULL,
    [Memory_Shared_MB]          DECIMAL (9, 4) NULL,
    [Memory_Free_MB]            DECIMAL (9, 4) NULL,
    [Memory_Used_MB]            DECIMAL (9, 4) NULL,
    [Java_Vendor]               VARCHAR (50)   NULL,
    [Java_Model]                VARCHAR (10)   NULL,
    [Java_RunTime]              VARCHAR (50)   NULL,
    [Java_RunTime_Version]      VARCHAR (25)   NULL,
    [Java_NonHeap_Init_MB]      DECIMAL (9, 4) NULL,
    [Java_NonHeap_Max_MB]       DECIMAL (9, 4) NULL,
    [Java_NonHeap_Committed_MB] DECIMAL (9, 4) NULL,
    [Java_NonHeap_Used_MB]      DECIMAL (9, 4) NULL,
    [Java_Heap_Committed_MB]    DECIMAL (9, 4) NULL,
    [Java_GC]                   VARCHAR (25)   NULL,
    [Java_Heap_Init_MB]         DECIMAL (9, 4) NULL,
    [Java_Heap_Max_MB]          DECIMAL (9, 4) NULL,
    [Java_Heap_Used_MB]         DECIMAL (9, 4) NULL,
    [NTP_Correction_MS]         BIGINT         NULL,
    [NTP_Polling_SEC]           BIGINT         NULL,
    [NTP_Max_Error_US]          BIGINT         NULL,
    [NTP_Estimated_Error_US]    BIGINT         NULL,
    [NTP_Time_Constant]         BIGINT         NULL,
    [NTP_Precrision_US]         BIGINT         NULL,
    [NTP_Frequency_PPM]         BIGINT         NULL,
    [NTP_Tolerance_PPM]         BIGINT         NULL,
    CONSTRAINT [fk_machinesessioninfo] FOREIGN KEY ([NodeSessionId]) REFERENCES [DSEDiagnostic].[NodeSessionInfo] ([NodeSessionId]) ON DELETE CASCADE ON UPDATE CASCADE
);


GO
CREATE NONCLUSTERED INDEX [Idx_MachineSessionInfo_NodeSessionId]
    ON [DSEDiagnostic].[MachineSessionInfo]([NodeSessionId] ASC);


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Machine, OS, NTP data for a run session.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'MachineSessionInfo';


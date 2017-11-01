namespace DSEDiagnosticApplication
{
    partial class FormLogEventGrid
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.components = new System.ComponentModel.Container();
            Infragistics.Win.UltraWinDataSource.UltraDataBand ultraDataBand1 = new Infragistics.Win.UltraWinDataSource.UltraDataBand("BandParentEvents");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn1 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("EventTime");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn2 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("LocalEventTime");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn3 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("SessionTieOutId");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn4 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("BeginSessionTime");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn5 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("EndSessionTime");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn6 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("Id");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn7 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("LogMessage");
            Infragistics.Win.UltraWinDataSource.UltraDataBand ultraDataBand2 = new Infragistics.Win.UltraWinDataSource.UltraDataBand("BandSSTables");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn8 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("SSTablePath");
            Infragistics.Win.UltraWinDataSource.UltraDataBand ultraDataBand3 = new Infragistics.Win.UltraWinDataSource.UltraDataBand("BandDDLItems");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn9 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("DDLStmt");
            Infragistics.Win.UltraWinDataSource.UltraDataBand ultraDataBand4 = new Infragistics.Win.UltraWinDataSource.UltraDataBand("BandNodes");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn10 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("Node");
            Infragistics.Win.UltraWinDataSource.UltraDataBand ultraDataBand5 = new Infragistics.Win.UltraWinDataSource.UltraDataBand("BandTokenRanges");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn11 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("Start");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn12 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("End");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn13 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("Slots");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn14 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("Load");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn15 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("Wraps");
            Infragistics.Win.UltraWinDataSource.UltraDataBand ultraDataBand6 = new Infragistics.Win.UltraWinDataSource.UltraDataBand("BandExceptionPaths");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn16 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("ExceptionPath");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn17 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("Source");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn18 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("Path");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn19 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("Cluster");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn20 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("DataCenter");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn21 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("Node");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn22 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("Type");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn23 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("Class");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn24 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("SubClass");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn25 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("Id");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn26 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("Timezone");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn27 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("EventTime");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn28 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("EventTimeOffset");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn29 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("LocalEventTime");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn30 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("SessionTieOutId");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn31 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("BeginSessionTime");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn32 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("EndSessionTime");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn33 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("SessionDuration");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn34 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("DSEProduct");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn35 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("Keyspace");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn36 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("TableViewIndex");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn37 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("Items");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn38 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("Exception");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn39 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("LineNbr");
            Infragistics.Win.UltraWinDataSource.UltraDataColumn ultraDataColumn40 = new Infragistics.Win.UltraWinDataSource.UltraDataColumn("LogMessage");
            Infragistics.Win.Appearance appearance13 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance14 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance15 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance16 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance17 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance18 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance1 = new Infragistics.Win.Appearance();
            Infragistics.Win.UltraWinGrid.UltraGridBand ultraGridBand1 = new Infragistics.Win.UltraWinGrid.UltraGridBand("BandLogEvent", -1);
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn1 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("Source");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn2 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("Path");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn3 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("Cluster");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn4 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("DataCenter");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn5 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("Node");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn6 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("Items");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn7 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("LineNbr");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn8 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("Class");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn9 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("SubClass");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn10 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("Id");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn11 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("Timezone");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn12 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("EventTime");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn13 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("EventTimeOffset");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn14 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("LocalEventTime");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn15 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("SessionTieOutId");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn16 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("BeginSessionTime");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn17 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("EndSessionTime");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn18 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("SessionDuration");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn19 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("DSEProduct");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn20 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("Keyspace");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn21 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("TableViewIndex");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn22 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("Exception");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn23 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("LogMessage");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn46 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("Type");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn24 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("BandParentEvents");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn25 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("BandSSTables");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn26 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("BandDDLItems");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn27 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("BandNodes");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn28 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("BandTokenRanges");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn29 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("BandExceptionPaths");
            Infragistics.Win.UltraWinGrid.UltraGridBand ultraGridBand2 = new Infragistics.Win.UltraWinGrid.UltraGridBand("BandParentEvents", 0);
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn30 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("EventTime");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn31 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("LocalEventTime");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn32 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("SessionTieOutId");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn33 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("BeginSessionTime");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn34 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("EndSessionTime");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn35 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("Id");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn36 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("LogMessage");
            Infragistics.Win.UltraWinGrid.UltraGridBand ultraGridBand3 = new Infragistics.Win.UltraWinGrid.UltraGridBand("BandSSTables", 0);
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn37 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("SSTablePath");
            Infragistics.Win.UltraWinGrid.UltraGridBand ultraGridBand4 = new Infragistics.Win.UltraWinGrid.UltraGridBand("BandDDLItems", 0);
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn38 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("DDLStmt");
            Infragistics.Win.UltraWinGrid.UltraGridBand ultraGridBand5 = new Infragistics.Win.UltraWinGrid.UltraGridBand("BandNodes", 0);
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn39 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("Node");
            Infragistics.Win.UltraWinGrid.UltraGridBand ultraGridBand6 = new Infragistics.Win.UltraWinGrid.UltraGridBand("BandTokenRanges", 0);
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn40 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("Start");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn41 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("End");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn45 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("Slots");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn42 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("Load");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn43 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("Wraps");
            Infragistics.Win.UltraWinGrid.UltraGridBand ultraGridBand7 = new Infragistics.Win.UltraWinGrid.UltraGridBand("BandExceptionPaths", 0);
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn44 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("ExceptionPath");
            Infragistics.Win.Appearance appearance2 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance3 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance4 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance5 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance6 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance7 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance8 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance9 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance10 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance11 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance12 = new Infragistics.Win.Appearance();
            this.ultraDataSourceLogEvents = new Infragistics.Win.UltraWinDataSource.UltraDataSource(this.components);
            this.ultraExpandableGroupBox1 = new Infragistics.Win.Misc.UltraExpandableGroupBox();
            this.ultraExpandableGroupBoxPanel1 = new Infragistics.Win.Misc.UltraExpandableGroupBoxPanel();
            this.ultraGanttView1 = new Infragistics.Win.UltraWinGanttView.UltraGanttView();
            this.ultraCalendarInfoLogEvents = new Infragistics.Win.UltraWinSchedule.UltraCalendarInfo(this.components);
            this.ultraTextEditorTZOffset = new Infragistics.Win.UltraWinEditors.UltraTextEditor();
            this.ultraTextEditorTimeZone = new Infragistics.Win.UltraWinEditors.UltraTextEditor();
            this.ultraLabel5 = new Infragistics.Win.Misc.UltraLabel();
            this.ultraTextEditorFilePath = new Infragistics.Win.UltraWinEditors.UltraTextEditor();
            this.ultraLabel4 = new Infragistics.Win.Misc.UltraLabel();
            this.ultraTextEditorNode = new Infragistics.Win.UltraWinEditors.UltraTextEditor();
            this.ultraLabel3 = new Infragistics.Win.Misc.UltraLabel();
            this.ultraTextEditorDCName = new Infragistics.Win.UltraWinEditors.UltraTextEditor();
            this.ultraLabel2 = new Infragistics.Win.Misc.UltraLabel();
            this.ultraTextEditorClusterName = new Infragistics.Win.UltraWinEditors.UltraTextEditor();
            this.ultraLabel1 = new Infragistics.Win.Misc.UltraLabel();
            this.ultraGrid1 = new Infragistics.Win.UltraWinGrid.UltraGrid();
            ((System.ComponentModel.ISupportInitialize)(this.ultraDataSourceLogEvents)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraExpandableGroupBox1)).BeginInit();
            this.ultraExpandableGroupBox1.SuspendLayout();
            this.ultraExpandableGroupBoxPanel1.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.ultraGanttView1)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorTZOffset)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorTimeZone)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorFilePath)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorNode)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorDCName)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorClusterName)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraGrid1)).BeginInit();
            this.SuspendLayout();
            // 
            // ultraDataSourceLogEvents
            // 
            ultraDataColumn1.DataType = typeof(System.DateTime);
            ultraDataColumn2.DataType = typeof(System.DateTime);
            ultraDataColumn4.AllowDBNull = Infragistics.Win.DefaultableBoolean.True;
            ultraDataColumn4.DataType = typeof(System.DateTime);
            ultraDataColumn5.AllowDBNull = Infragistics.Win.DefaultableBoolean.True;
            ultraDataColumn5.DataType = typeof(System.DateTime);
            ultraDataColumn6.DataType = typeof(System.Guid);
            ultraDataBand1.Columns.AddRange(new object[] {
            ultraDataColumn1,
            ultraDataColumn2,
            ultraDataColumn3,
            ultraDataColumn4,
            ultraDataColumn5,
            ultraDataColumn6,
            ultraDataColumn7});
            ultraDataBand2.Columns.AddRange(new object[] {
            ultraDataColumn8});
            ultraDataBand3.Columns.AddRange(new object[] {
            ultraDataColumn9});
            ultraDataBand4.Columns.AddRange(new object[] {
            ultraDataColumn10});
            ultraDataColumn11.DataType = typeof(long);
            ultraDataColumn12.DataType = typeof(long);
            ultraDataColumn13.DataType = typeof(ulong);
            ultraDataColumn14.DataType = typeof(decimal);
            ultraDataColumn15.DataType = typeof(bool);
            ultraDataBand5.Columns.AddRange(new object[] {
            ultraDataColumn11,
            ultraDataColumn12,
            ultraDataColumn13,
            ultraDataColumn14,
            ultraDataColumn15});
            ultraDataBand6.Columns.AddRange(new object[] {
            ultraDataColumn16});
            this.ultraDataSourceLogEvents.Band.ChildBands.AddRange(new object[] {
            ultraDataBand1,
            ultraDataBand2,
            ultraDataBand3,
            ultraDataBand4,
            ultraDataBand5,
            ultraDataBand6});
            ultraDataColumn25.DataType = typeof(System.Guid);
            ultraDataColumn26.DataType = typeof(object);
            ultraDataColumn27.DataType = typeof(System.DateTime);
            ultraDataColumn28.DataType = typeof(decimal);
            ultraDataColumn29.DataType = typeof(System.DateTime);
            ultraDataColumn31.AllowDBNull = Infragistics.Win.DefaultableBoolean.True;
            ultraDataColumn31.DataType = typeof(System.DateTime);
            ultraDataColumn32.AllowDBNull = Infragistics.Win.DefaultableBoolean.True;
            ultraDataColumn32.DataType = typeof(System.DateTime);
            ultraDataColumn33.AllowDBNull = Infragistics.Win.DefaultableBoolean.True;
            ultraDataColumn33.DataType = typeof(decimal);
            ultraDataColumn37.DataType = typeof(int);
            ultraDataColumn39.DataType = typeof(uint);
            this.ultraDataSourceLogEvents.Band.Columns.AddRange(new object[] {
            ultraDataColumn17,
            ultraDataColumn18,
            ultraDataColumn19,
            ultraDataColumn20,
            ultraDataColumn21,
            ultraDataColumn22,
            ultraDataColumn23,
            ultraDataColumn24,
            ultraDataColumn25,
            ultraDataColumn26,
            ultraDataColumn27,
            ultraDataColumn28,
            ultraDataColumn29,
            ultraDataColumn30,
            ultraDataColumn31,
            ultraDataColumn32,
            ultraDataColumn33,
            ultraDataColumn34,
            ultraDataColumn35,
            ultraDataColumn36,
            ultraDataColumn37,
            ultraDataColumn38,
            ultraDataColumn39,
            ultraDataColumn40});
            this.ultraDataSourceLogEvents.Band.Key = "BandLogEvent";
            this.ultraDataSourceLogEvents.CellDataRequested += new Infragistics.Win.UltraWinDataSource.CellDataRequestedEventHandler(this.ultraDataSourceLogEvents_CellDataRequested);
            // 
            // ultraExpandableGroupBox1
            // 
            this.ultraExpandableGroupBox1.Controls.Add(this.ultraExpandableGroupBoxPanel1);
            this.ultraExpandableGroupBox1.Dock = System.Windows.Forms.DockStyle.Top;
            this.ultraExpandableGroupBox1.ExpandedSize = new System.Drawing.Size(899, 187);
            this.ultraExpandableGroupBox1.Location = new System.Drawing.Point(0, 0);
            this.ultraExpandableGroupBox1.Name = "ultraExpandableGroupBox1";
            this.ultraExpandableGroupBox1.Size = new System.Drawing.Size(899, 187);
            this.ultraExpandableGroupBox1.TabIndex = 0;
            this.ultraExpandableGroupBox1.Text = "Log Event Information";
            // 
            // ultraExpandableGroupBoxPanel1
            // 
            this.ultraExpandableGroupBoxPanel1.Controls.Add(this.ultraGanttView1);
            this.ultraExpandableGroupBoxPanel1.Controls.Add(this.ultraTextEditorTZOffset);
            this.ultraExpandableGroupBoxPanel1.Controls.Add(this.ultraTextEditorTimeZone);
            this.ultraExpandableGroupBoxPanel1.Controls.Add(this.ultraLabel5);
            this.ultraExpandableGroupBoxPanel1.Controls.Add(this.ultraTextEditorFilePath);
            this.ultraExpandableGroupBoxPanel1.Controls.Add(this.ultraLabel4);
            this.ultraExpandableGroupBoxPanel1.Controls.Add(this.ultraTextEditorNode);
            this.ultraExpandableGroupBoxPanel1.Controls.Add(this.ultraLabel3);
            this.ultraExpandableGroupBoxPanel1.Controls.Add(this.ultraTextEditorDCName);
            this.ultraExpandableGroupBoxPanel1.Controls.Add(this.ultraLabel2);
            this.ultraExpandableGroupBoxPanel1.Controls.Add(this.ultraTextEditorClusterName);
            this.ultraExpandableGroupBoxPanel1.Controls.Add(this.ultraLabel1);
            this.ultraExpandableGroupBoxPanel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.ultraExpandableGroupBoxPanel1.Location = new System.Drawing.Point(3, 19);
            this.ultraExpandableGroupBoxPanel1.Name = "ultraExpandableGroupBoxPanel1";
            this.ultraExpandableGroupBoxPanel1.Size = new System.Drawing.Size(893, 165);
            this.ultraExpandableGroupBoxPanel1.TabIndex = 0;
            // 
            // ultraGanttView1
            // 
            this.ultraGanttView1.CalendarInfo = this.ultraCalendarInfoLogEvents;
            this.ultraGanttView1.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.ultraGanttView1.GridSettings.ColumnSettings.GetValue("Constraint").VisiblePosition = 6;
            this.ultraGanttView1.GridSettings.ColumnSettings.GetValue("ConstraintDateTime").VisiblePosition = 7;
            this.ultraGanttView1.GridSettings.ColumnSettings.GetValue("Dependencies").VisiblePosition = 4;
            this.ultraGanttView1.GridSettings.ColumnSettings.GetValue("Deadline").VisiblePosition = 8;
            this.ultraGanttView1.GridSettings.ColumnSettings.GetValue("Duration").VisiblePosition = 1;
            this.ultraGanttView1.GridSettings.ColumnSettings.GetValue("EndDateTime").VisiblePosition = 3;
            this.ultraGanttView1.GridSettings.ColumnSettings.GetValue("Milestone").VisiblePosition = 9;
            this.ultraGanttView1.GridSettings.ColumnSettings.GetValue("Name").VisiblePosition = 0;
            this.ultraGanttView1.GridSettings.ColumnSettings.GetValue("Notes").VisiblePosition = 10;
            this.ultraGanttView1.GridSettings.ColumnSettings.GetValue("PercentComplete").VisiblePosition = 11;
            this.ultraGanttView1.GridSettings.ColumnSettings.GetValue("Resources").VisiblePosition = 5;
            this.ultraGanttView1.GridSettings.ColumnSettings.GetValue("StartDateTime").VisiblePosition = 2;
            this.ultraGanttView1.GridSettings.ColumnSettings.GetValue("RowNumber").VisiblePosition = 12;
            this.ultraGanttView1.Location = new System.Drawing.Point(0, 82);
            this.ultraGanttView1.Name = "ultraGanttView1";
            this.ultraGanttView1.Size = new System.Drawing.Size(893, 83);
            this.ultraGanttView1.TabIndex = 11;
            this.ultraGanttView1.Text = "ultraGanttView1";
            this.ultraGanttView1.VerticalSplitterMinimumResizeWidth = 10;
            // 
            // ultraCalendarInfoLogEvents
            // 
            this.ultraCalendarInfoLogEvents.DataBindingsForAppointments.BindingContextControl = this;
            this.ultraCalendarInfoLogEvents.DataBindingsForOwners.BindingContextControl = this;
            // 
            // ultraTextEditorTZOffset
            // 
            this.ultraTextEditorTZOffset.Location = new System.Drawing.Point(455, 27);
            this.ultraTextEditorTZOffset.Name = "ultraTextEditorTZOffset";
            this.ultraTextEditorTZOffset.NullText = "N/A";
            appearance13.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.ultraTextEditorTZOffset.NullTextAppearance = appearance13;
            this.ultraTextEditorTZOffset.ReadOnly = true;
            this.ultraTextEditorTZOffset.Size = new System.Drawing.Size(62, 21);
            this.ultraTextEditorTZOffset.TabIndex = 10;
            // 
            // ultraTextEditorTimeZone
            // 
            this.ultraTextEditorTimeZone.Location = new System.Drawing.Point(287, 27);
            this.ultraTextEditorTimeZone.Name = "ultraTextEditorTimeZone";
            this.ultraTextEditorTimeZone.NullText = "N/A";
            appearance14.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.ultraTextEditorTimeZone.NullTextAppearance = appearance14;
            this.ultraTextEditorTimeZone.ReadOnly = true;
            this.ultraTextEditorTimeZone.ShowOverflowIndicator = true;
            this.ultraTextEditorTimeZone.Size = new System.Drawing.Size(162, 21);
            this.ultraTextEditorTimeZone.TabIndex = 9;
            // 
            // ultraLabel5
            // 
            this.ultraLabel5.AutoSize = true;
            this.ultraLabel5.Location = new System.Drawing.Point(229, 31);
            this.ultraLabel5.Name = "ultraLabel5";
            this.ultraLabel5.Size = new System.Drawing.Size(58, 14);
            this.ultraLabel5.TabIndex = 8;
            this.ultraLabel5.Text = "TimeZone:";
            // 
            // ultraTextEditorFilePath
            // 
            this.ultraTextEditorFilePath.Location = new System.Drawing.Point(287, 0);
            this.ultraTextEditorFilePath.Name = "ultraTextEditorFilePath";
            this.ultraTextEditorFilePath.NullText = "N/A";
            appearance15.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.ultraTextEditorFilePath.NullTextAppearance = appearance15;
            this.ultraTextEditorFilePath.ReadOnly = true;
            this.ultraTextEditorFilePath.ShowOverflowIndicator = true;
            this.ultraTextEditorFilePath.Size = new System.Drawing.Size(281, 21);
            this.ultraTextEditorFilePath.TabIndex = 7;
            // 
            // ultraLabel4
            // 
            this.ultraLabel4.AutoSize = true;
            this.ultraLabel4.Location = new System.Drawing.Point(229, 4);
            this.ultraLabel4.Name = "ultraLabel4";
            this.ultraLabel4.Size = new System.Drawing.Size(52, 14);
            this.ultraLabel4.TabIndex = 6;
            this.ultraLabel4.Text = "File Path:";
            // 
            // ultraTextEditorNode
            // 
            this.ultraTextEditorNode.Location = new System.Drawing.Point(59, 54);
            this.ultraTextEditorNode.Name = "ultraTextEditorNode";
            this.ultraTextEditorNode.NullText = "N/A";
            appearance16.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.ultraTextEditorNode.NullTextAppearance = appearance16;
            this.ultraTextEditorNode.ReadOnly = true;
            this.ultraTextEditorNode.ShowOverflowIndicator = true;
            this.ultraTextEditorNode.Size = new System.Drawing.Size(162, 21);
            this.ultraTextEditorNode.TabIndex = 5;
            // 
            // ultraLabel3
            // 
            this.ultraLabel3.AutoSize = true;
            this.ultraLabel3.Location = new System.Drawing.Point(10, 58);
            this.ultraLabel3.Name = "ultraLabel3";
            this.ultraLabel3.Size = new System.Drawing.Size(34, 14);
            this.ultraLabel3.TabIndex = 4;
            this.ultraLabel3.Text = "Node:";
            // 
            // ultraTextEditorDCName
            // 
            this.ultraTextEditorDCName.Location = new System.Drawing.Point(59, 27);
            this.ultraTextEditorDCName.Name = "ultraTextEditorDCName";
            this.ultraTextEditorDCName.NullText = "N/A";
            appearance17.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.ultraTextEditorDCName.NullTextAppearance = appearance17;
            this.ultraTextEditorDCName.ReadOnly = true;
            this.ultraTextEditorDCName.ShowOverflowIndicator = true;
            this.ultraTextEditorDCName.Size = new System.Drawing.Size(162, 21);
            this.ultraTextEditorDCName.TabIndex = 3;
            // 
            // ultraLabel2
            // 
            this.ultraLabel2.AutoSize = true;
            this.ultraLabel2.Location = new System.Drawing.Point(10, 31);
            this.ultraLabel2.Name = "ultraLabel2";
            this.ultraLabel2.Size = new System.Drawing.Size(24, 14);
            this.ultraLabel2.TabIndex = 2;
            this.ultraLabel2.Text = "DC:";
            // 
            // ultraTextEditorClusterName
            // 
            this.ultraTextEditorClusterName.Location = new System.Drawing.Point(59, 0);
            this.ultraTextEditorClusterName.Name = "ultraTextEditorClusterName";
            this.ultraTextEditorClusterName.NullText = "N/A";
            appearance18.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.ultraTextEditorClusterName.NullTextAppearance = appearance18;
            this.ultraTextEditorClusterName.ReadOnly = true;
            this.ultraTextEditorClusterName.ShowOverflowIndicator = true;
            this.ultraTextEditorClusterName.Size = new System.Drawing.Size(162, 21);
            this.ultraTextEditorClusterName.TabIndex = 1;
            // 
            // ultraLabel1
            // 
            this.ultraLabel1.AutoSize = true;
            this.ultraLabel1.Location = new System.Drawing.Point(10, 4);
            this.ultraLabel1.Name = "ultraLabel1";
            this.ultraLabel1.Size = new System.Drawing.Size(43, 14);
            this.ultraLabel1.TabIndex = 0;
            this.ultraLabel1.Text = "Cluster:";
            // 
            // ultraGrid1
            // 
            this.ultraGrid1.DataSource = this.ultraDataSourceLogEvents;
            appearance1.BackColor = System.Drawing.SystemColors.Window;
            appearance1.BorderColor = System.Drawing.SystemColors.InactiveCaption;
            this.ultraGrid1.DisplayLayout.Appearance = appearance1;
            ultraGridColumn1.Header.VisiblePosition = 0;
            ultraGridColumn1.Hidden = true;
            ultraGridColumn2.Header.VisiblePosition = 1;
            ultraGridColumn2.Hidden = true;
            ultraGridColumn3.Header.VisiblePosition = 2;
            ultraGridColumn3.Hidden = true;
            ultraGridColumn4.Header.VisiblePosition = 3;
            ultraGridColumn4.Hidden = true;
            ultraGridColumn5.Header.VisiblePosition = 4;
            ultraGridColumn5.Hidden = true;
            ultraGridColumn6.Header.VisiblePosition = 20;
            ultraGridColumn6.Width = 77;
            ultraGridColumn7.Header.VisiblePosition = 22;
            ultraGridColumn7.Width = 82;
            ultraGridColumn8.Header.VisiblePosition = 6;
            ultraGridColumn8.Width = 83;
            ultraGridColumn9.Header.VisiblePosition = 7;
            ultraGridColumn9.Width = 86;
            ultraGridColumn10.Header.VisiblePosition = 8;
            ultraGridColumn10.Width = 124;
            ultraGridColumn11.Header.VisiblePosition = 9;
            ultraGridColumn11.Hidden = true;
            ultraGridColumn12.Format = "yyyy-MM-dd HH:mm:ss.fff";
            ultraGridColumn12.Header.VisiblePosition = 10;
            ultraGridColumn12.Width = 124;
            ultraGridColumn13.Header.VisiblePosition = 11;
            ultraGridColumn13.Hidden = true;
            ultraGridColumn14.Format = "yyyy-MM-dd HH:mm:ss.fff";
            ultraGridColumn14.Header.VisiblePosition = 12;
            ultraGridColumn15.Header.VisiblePosition = 13;
            ultraGridColumn16.Format = "yyyy-MM-dd HH:mm:ss.fff";
            ultraGridColumn16.Header.VisiblePosition = 14;
            ultraGridColumn17.Format = "yyyy-MM-dd HH:mm:ss.fff";
            ultraGridColumn17.Header.VisiblePosition = 15;
            ultraGridColumn18.Header.VisiblePosition = 16;
            ultraGridColumn19.Header.VisiblePosition = 17;
            ultraGridColumn20.Header.VisiblePosition = 18;
            ultraGridColumn21.Header.VisiblePosition = 19;
            ultraGridColumn22.Header.VisiblePosition = 21;
            ultraGridColumn23.Header.VisiblePosition = 23;
            ultraGridColumn46.Header.VisiblePosition = 5;
            ultraGridColumn24.Header.VisiblePosition = 24;
            ultraGridColumn25.Header.VisiblePosition = 25;
            ultraGridColumn26.Header.VisiblePosition = 26;
            ultraGridColumn27.Header.VisiblePosition = 27;
            ultraGridColumn28.Header.VisiblePosition = 28;
            ultraGridColumn29.Header.VisiblePosition = 29;
            ultraGridBand1.Columns.AddRange(new object[] {
            ultraGridColumn1,
            ultraGridColumn2,
            ultraGridColumn3,
            ultraGridColumn4,
            ultraGridColumn5,
            ultraGridColumn6,
            ultraGridColumn7,
            ultraGridColumn8,
            ultraGridColumn9,
            ultraGridColumn10,
            ultraGridColumn11,
            ultraGridColumn12,
            ultraGridColumn13,
            ultraGridColumn14,
            ultraGridColumn15,
            ultraGridColumn16,
            ultraGridColumn17,
            ultraGridColumn18,
            ultraGridColumn19,
            ultraGridColumn20,
            ultraGridColumn21,
            ultraGridColumn22,
            ultraGridColumn23,
            ultraGridColumn46,
            ultraGridColumn24,
            ultraGridColumn25,
            ultraGridColumn26,
            ultraGridColumn27,
            ultraGridColumn28,
            ultraGridColumn29});
            ultraGridColumn30.Header.VisiblePosition = 0;
            ultraGridColumn30.Width = 79;
            ultraGridColumn31.Header.VisiblePosition = 1;
            ultraGridColumn31.Width = 83;
            ultraGridColumn32.Header.VisiblePosition = 2;
            ultraGridColumn32.Width = 86;
            ultraGridColumn33.Header.VisiblePosition = 3;
            ultraGridColumn33.Width = 124;
            ultraGridColumn34.Header.VisiblePosition = 4;
            ultraGridColumn34.Width = 124;
            ultraGridColumn35.Header.VisiblePosition = 5;
            ultraGridColumn35.Width = 124;
            ultraGridColumn36.Header.VisiblePosition = 6;
            ultraGridBand2.Columns.AddRange(new object[] {
            ultraGridColumn30,
            ultraGridColumn31,
            ultraGridColumn32,
            ultraGridColumn33,
            ultraGridColumn34,
            ultraGridColumn35,
            ultraGridColumn36});
            ultraGridColumn37.Header.VisiblePosition = 0;
            ultraGridColumn37.Width = 79;
            ultraGridBand3.Columns.AddRange(new object[] {
            ultraGridColumn37});
            ultraGridColumn38.Header.VisiblePosition = 0;
            ultraGridColumn38.Width = 79;
            ultraGridBand4.Columns.AddRange(new object[] {
            ultraGridColumn38});
            ultraGridColumn39.Header.VisiblePosition = 0;
            ultraGridColumn39.Width = 79;
            ultraGridBand5.Columns.AddRange(new object[] {
            ultraGridColumn39});
            ultraGridColumn40.Header.VisiblePosition = 0;
            ultraGridColumn40.Width = 79;
            ultraGridColumn41.Header.VisiblePosition = 1;
            ultraGridColumn41.Width = 83;
            ultraGridColumn45.Header.VisiblePosition = 4;
            ultraGridColumn42.Header.VisiblePosition = 2;
            ultraGridColumn42.Width = 86;
            ultraGridColumn43.Header.VisiblePosition = 3;
            ultraGridColumn43.Width = 124;
            ultraGridBand6.Columns.AddRange(new object[] {
            ultraGridColumn40,
            ultraGridColumn41,
            ultraGridColumn45,
            ultraGridColumn42,
            ultraGridColumn43});
            ultraGridColumn44.Header.VisiblePosition = 0;
            ultraGridColumn44.Width = 79;
            ultraGridBand7.Columns.AddRange(new object[] {
            ultraGridColumn44});
            this.ultraGrid1.DisplayLayout.BandsSerializer.Add(ultraGridBand1);
            this.ultraGrid1.DisplayLayout.BandsSerializer.Add(ultraGridBand2);
            this.ultraGrid1.DisplayLayout.BandsSerializer.Add(ultraGridBand3);
            this.ultraGrid1.DisplayLayout.BandsSerializer.Add(ultraGridBand4);
            this.ultraGrid1.DisplayLayout.BandsSerializer.Add(ultraGridBand5);
            this.ultraGrid1.DisplayLayout.BandsSerializer.Add(ultraGridBand6);
            this.ultraGrid1.DisplayLayout.BandsSerializer.Add(ultraGridBand7);
            this.ultraGrid1.DisplayLayout.BorderStyle = Infragistics.Win.UIElementBorderStyle.Solid;
            this.ultraGrid1.DisplayLayout.CaptionVisible = Infragistics.Win.DefaultableBoolean.False;
            appearance2.BackColor = System.Drawing.SystemColors.ActiveBorder;
            appearance2.BackColor2 = System.Drawing.SystemColors.ControlDark;
            appearance2.BackGradientStyle = Infragistics.Win.GradientStyle.Vertical;
            appearance2.BorderColor = System.Drawing.SystemColors.Window;
            this.ultraGrid1.DisplayLayout.GroupByBox.Appearance = appearance2;
            appearance3.ForeColor = System.Drawing.SystemColors.GrayText;
            this.ultraGrid1.DisplayLayout.GroupByBox.BandLabelAppearance = appearance3;
            this.ultraGrid1.DisplayLayout.GroupByBox.BorderStyle = Infragistics.Win.UIElementBorderStyle.Solid;
            appearance4.BackColor = System.Drawing.SystemColors.ControlLightLight;
            appearance4.BackColor2 = System.Drawing.SystemColors.Control;
            appearance4.BackGradientStyle = Infragistics.Win.GradientStyle.Horizontal;
            appearance4.ForeColor = System.Drawing.SystemColors.GrayText;
            this.ultraGrid1.DisplayLayout.GroupByBox.PromptAppearance = appearance4;
            this.ultraGrid1.DisplayLayout.LoadStyle = Infragistics.Win.UltraWinGrid.LoadStyle.LoadOnDemand;
            this.ultraGrid1.DisplayLayout.MaxColScrollRegions = 1;
            this.ultraGrid1.DisplayLayout.MaxRowScrollRegions = 1;
            appearance5.BackColor = System.Drawing.SystemColors.Window;
            appearance5.ForeColor = System.Drawing.SystemColors.ControlText;
            this.ultraGrid1.DisplayLayout.Override.ActiveCellAppearance = appearance5;
            appearance6.BackColor = System.Drawing.SystemColors.Highlight;
            appearance6.ForeColor = System.Drawing.SystemColors.HighlightText;
            this.ultraGrid1.DisplayLayout.Override.ActiveRowAppearance = appearance6;
            this.ultraGrid1.DisplayLayout.Override.AllowAddNew = Infragistics.Win.UltraWinGrid.AllowAddNew.No;
            this.ultraGrid1.DisplayLayout.Override.AllowDelete = Infragistics.Win.DefaultableBoolean.False;
            this.ultraGrid1.DisplayLayout.Override.AllowRowFiltering = Infragistics.Win.DefaultableBoolean.False;
            this.ultraGrid1.DisplayLayout.Override.AllowUpdate = Infragistics.Win.DefaultableBoolean.False;
            this.ultraGrid1.DisplayLayout.Override.BorderStyleCell = Infragistics.Win.UIElementBorderStyle.Dotted;
            this.ultraGrid1.DisplayLayout.Override.BorderStyleRow = Infragistics.Win.UIElementBorderStyle.Dotted;
            appearance7.BackColor = System.Drawing.SystemColors.Window;
            this.ultraGrid1.DisplayLayout.Override.CardAreaAppearance = appearance7;
            appearance8.BorderColor = System.Drawing.Color.Silver;
            appearance8.TextTrimming = Infragistics.Win.TextTrimming.EllipsisCharacter;
            this.ultraGrid1.DisplayLayout.Override.CellAppearance = appearance8;
            this.ultraGrid1.DisplayLayout.Override.CellClickAction = Infragistics.Win.UltraWinGrid.CellClickAction.EditAndSelectText;
            this.ultraGrid1.DisplayLayout.Override.CellPadding = 0;
            appearance9.BackColor = System.Drawing.SystemColors.Control;
            appearance9.BackColor2 = System.Drawing.SystemColors.ControlDark;
            appearance9.BackGradientAlignment = Infragistics.Win.GradientAlignment.Element;
            appearance9.BackGradientStyle = Infragistics.Win.GradientStyle.Horizontal;
            appearance9.BorderColor = System.Drawing.SystemColors.Window;
            this.ultraGrid1.DisplayLayout.Override.GroupByRowAppearance = appearance9;
            appearance10.TextHAlignAsString = "Left";
            this.ultraGrid1.DisplayLayout.Override.HeaderAppearance = appearance10;
            this.ultraGrid1.DisplayLayout.Override.HeaderStyle = Infragistics.Win.HeaderStyle.WindowsXPCommand;
            appearance11.BackColor = System.Drawing.SystemColors.Window;
            appearance11.BorderColor = System.Drawing.Color.Silver;
            this.ultraGrid1.DisplayLayout.Override.RowAppearance = appearance11;
            this.ultraGrid1.DisplayLayout.Override.RowSelectorHeaderStyle = Infragistics.Win.UltraWinGrid.RowSelectorHeaderStyle.ColumnChooserButton;
            this.ultraGrid1.DisplayLayout.Override.RowSelectorNumberStyle = Infragistics.Win.UltraWinGrid.RowSelectorNumberStyle.RowIndex;
            this.ultraGrid1.DisplayLayout.Override.RowSelectors = Infragistics.Win.DefaultableBoolean.True;
            this.ultraGrid1.DisplayLayout.Override.SupportDataErrorInfo = Infragistics.Win.UltraWinGrid.SupportDataErrorInfo.RowsOnly;
            appearance12.BackColor = System.Drawing.SystemColors.ControlLight;
            this.ultraGrid1.DisplayLayout.Override.TemplateAddRowAppearance = appearance12;
            this.ultraGrid1.DisplayLayout.ScrollBounds = Infragistics.Win.UltraWinGrid.ScrollBounds.ScrollToFill;
            this.ultraGrid1.DisplayLayout.ScrollStyle = Infragistics.Win.UltraWinGrid.ScrollStyle.Immediate;
            this.ultraGrid1.DisplayLayout.UseFixedHeaders = true;
            this.ultraGrid1.DisplayLayout.ViewStyleBand = Infragistics.Win.UltraWinGrid.ViewStyleBand.OutlookGroupBy;
            this.ultraGrid1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.ultraGrid1.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.ultraGrid1.Location = new System.Drawing.Point(0, 187);
            this.ultraGrid1.Name = "ultraGrid1";
            this.ultraGrid1.Size = new System.Drawing.Size(899, 314);
            this.ultraGrid1.TabIndex = 1;
            this.ultraGrid1.Text = "ultraGrid1";
            this.ultraGrid1.InitializeRowsCollection += new Infragistics.Win.UltraWinGrid.InitializeRowsCollectionEventHandler(this.ultraGrid1_InitializeRowsCollection);
            // 
            // FormLogEventGrid
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(899, 501);
            this.Controls.Add(this.ultraGrid1);
            this.Controls.Add(this.ultraExpandableGroupBox1);
            this.Name = "FormLogEventGrid";
            this.Text = "Log Events";
            this.Load += new System.EventHandler(this.FormLogEventGrid_Load);
            ((System.ComponentModel.ISupportInitialize)(this.ultraDataSourceLogEvents)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraExpandableGroupBox1)).EndInit();
            this.ultraExpandableGroupBox1.ResumeLayout(false);
            this.ultraExpandableGroupBoxPanel1.ResumeLayout(false);
            this.ultraExpandableGroupBoxPanel1.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.ultraGanttView1)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorTZOffset)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorTimeZone)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorFilePath)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorNode)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorDCName)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorClusterName)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraGrid1)).EndInit();
            this.ResumeLayout(false);

        }

        #endregion

        private Infragistics.Win.UltraWinDataSource.UltraDataSource ultraDataSourceLogEvents;
        private Infragistics.Win.Misc.UltraExpandableGroupBox ultraExpandableGroupBox1;
        private Infragistics.Win.Misc.UltraExpandableGroupBoxPanel ultraExpandableGroupBoxPanel1;
        private Infragistics.Win.UltraWinEditors.UltraTextEditor ultraTextEditorTZOffset;
        private Infragistics.Win.UltraWinEditors.UltraTextEditor ultraTextEditorTimeZone;
        private Infragistics.Win.Misc.UltraLabel ultraLabel5;
        private Infragistics.Win.UltraWinEditors.UltraTextEditor ultraTextEditorFilePath;
        private Infragistics.Win.Misc.UltraLabel ultraLabel4;
        private Infragistics.Win.UltraWinEditors.UltraTextEditor ultraTextEditorNode;
        private Infragistics.Win.Misc.UltraLabel ultraLabel3;
        private Infragistics.Win.UltraWinEditors.UltraTextEditor ultraTextEditorDCName;
        private Infragistics.Win.Misc.UltraLabel ultraLabel2;
        private Infragistics.Win.UltraWinEditors.UltraTextEditor ultraTextEditorClusterName;
        private Infragistics.Win.Misc.UltraLabel ultraLabel1;
        private Infragistics.Win.UltraWinGrid.UltraGrid ultraGrid1;
        private Infragistics.Win.UltraWinGanttView.UltraGanttView ultraGanttView1;
        private Infragistics.Win.UltraWinSchedule.UltraCalendarInfo ultraCalendarInfoLogEvents;
    }
}
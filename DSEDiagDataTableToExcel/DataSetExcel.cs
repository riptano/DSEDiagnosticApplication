using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Data;
using System.Threading.Tasks;
using OfficeOpenXml;
using Common;
using OfficeOpenXml.ConditionalFormatting;
using System.Runtime.Serialization.Json;
using System.Xml;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace DataTableToExcel
{
    public enum WorkBookProcessingStage
    {
        /// <summary>
        /// Action called before any data table or excel processing. Called only once.
        /// </summary>
        PreProcess,
        /// <summary>
        /// Action called before workbook is open so that the file name can be modified. (one time per workbook)
        /// </summary>
        PrepareFileName,
        /// <summary>
        /// Only called if the data table needs to be split because it exceeds maximum number rows in a workbook. Called before actual table split. Called only once.
        /// </summary>
        PreProcessDataTable,
        /// <summary>
        /// Called after a workbook is open but before the worksheet is processed. Can be called multiple times (one time per workbook)
        /// </summary>
        PreLoad,
        /// <summary>
        /// Called after a worksheet is loaded but before the workbook is closed and saved. Can be called multiple times (one time per workbook)
        /// </summary>
        PreSave,
        /// <summary>
        /// Called after the workbook is saved but still open. Can be called multiple times (one time per workbook)
        /// </summary>
        Saved,
        /// <summary>
        /// Called after workbook processing (workbook is closed). Called only once.
        /// </summary>
        PostProcess
    }

    public sealed class ExcelPkgCache : IDisposable
    {
        public ExcelPkgCache(IFilePath excelFilePath, bool isCached, IFilePath excelTemplateFilePath = null)
        {
            this.ExcelFilePath = excelFilePath;
            this.ExcelPackage = excelTemplateFilePath == null ? new ExcelPackage(excelFilePath.FileInfo()) : new ExcelPackage(excelFilePath.FileInfo(), excelTemplateFilePath.FileInfo());
            this._refCnt = isCached ? 2 : 1;
            this.CachedValue = isCached;
        }

        #region Public Members

        public ExcelPackage ExcelPackage { get; }
        public IFilePath ExcelFilePath { get; }
        public bool CachedValue { get; }

        private int _refCnt;
        public int RefCnt { get { return _refCnt; } }

        public void Close()
        {
            if (this._refCnt > 0 && !this.Disposed) this.ExcelPackage.Dispose();
            this._refCnt = 0;
        }

        public void Save()
        {
            if (this._refCnt > 0 && !this.Disposed)
            {                
                this.ExcelPackage.Save();
            }
        }

        public void Save(IFilePath newFilePath)
        {
            if (this._refCnt > 0 && !this.Disposed) this.ExcelPackage.SaveAs(newFilePath.FileInfo());
        }

        public void SaveAndClose(IFilePath newFilePath = null)
        {
            if (newFilePath == null)
                this.Save();
            else
                this.Save(newFilePath);

            this.Close();
        }

        public int IncrementRefCnt()
        {
            if (this.Disposed) return 0;
            return System.Threading.Interlocked.Increment(ref this._refCnt);
        }

        #endregion

        #region Static Members

        static private System.Collections.Concurrent.ConcurrentDictionary<IFilePath, ExcelPkgCache> _Caches = new System.Collections.Concurrent.ConcurrentDictionary<IFilePath, ExcelPkgCache>();

        static public IReadOnlyList<KeyValuePair<IFilePath, ExcelPkgCache>> Caches { get { return _Caches.ToList(); } }

        static public ExcelPackage GetExcelPackage(IFilePath excelPath)
        {            
            if(_Caches.TryGetValue(excelPath, out ExcelPkgCache cache))
            {
                return cache.ExcelPackage;
            }

            return null;
        }

        static public ExcelPkgCache GetExcelPackageCache(IFilePath excelPath)
        {            
            if (_Caches.TryGetValue(excelPath, out ExcelPkgCache cache))
            {
                return cache;
            }

            return null;
        }

        static public ExcelPkgCache GetAddExcelPackageCache(IFilePath excelPath, bool cacheValue = true, IFilePath excelTemplateFilePath = null)
        {
            ExcelPkgCache newInstance = null;
            var cache = _Caches.GetOrAdd(excelPath, excelFile => newInstance = new ExcelPkgCache(excelPath, cacheValue, excelTemplateFilePath));

            if (newInstance == null) cache.IncrementRefCnt();

            return cache;
        }

        static public ExcelPkgCache RemoveExcelPackageCache(IFilePath excelPath, bool saveFile = false, bool disposePackage = true)
        {           
            if(_Caches.TryRemove(excelPath, out ExcelPkgCache cache))
            {
                if (saveFile) cache.Save();
                if (disposePackage) cache.DisposeForce();
            }

            return cache;
        }

        static public int SaveAllExcelFiles(bool closePackage = false)
        {
            int nSaved = 0;

            foreach(var cache in Caches)
            {                
                if (closePackage)
                {
                    RemoveExcelPackageCache(cache.Key, true, true);
                    Logger.Instance.InfoFormat("Excel WorkBooks saved and closed to \"{0}\"", cache.Key.PathResolved);
                }
                else
                {
                    cache.Value.Save();
                    Logger.Instance.InfoFormat("Excel WorkBooks saved to \"{0}\"", cache.Key.PathResolved);
                }
                nSaved++;
            }

            return nSaved;
        }
        #endregion

        #region Dispose Methods

        public bool Disposed { get; private set; }

        public void DisposeForce()
        {
            Dispose(false);
            GC.SuppressFinalize(this);            
        }

        // Implement IDisposable.
        // Do not make this method virtual.
        // A derived class should not be able to override this method.
        public void Dispose()
        {
            Dispose(true);

            if (this.Disposed)
            {
                // This object will be cleaned up by the Dispose method.
                // Therefore, you should call GC.SupressFinalize to
                // take this object off the finalization queue
                // and prevent finalization code for this object
                // from executing a second time.
                GC.SuppressFinalize(this);
            }
        }

        // Dispose(bool disposing) executes in two distinct scenarios.
        // If disposing equals true, the method has been called directly
        // or indirectly by a user's code. Managed and unmanaged resources
        // can be disposed.
        // If disposing equals false, the method has been called by the
        // runtime from inside the finalizer and you should not reference
        // other objects. Only unmanaged resources can be disposed.
        private void Dispose(bool disposing)
        {
            // Check to see if Dispose has already been called.
            if (!this.Disposed)
            {
                if (disposing)
                {                    
                    if (this._refCnt > 0)
                    {
                        if (System.Threading.Interlocked.Decrement(ref this._refCnt) <= 0 || !this.CachedValue)
                        {
                            this._refCnt = 0;
                            this.ExcelPackage.Dispose();
                            this.Disposed = true;
                        }                                               
                    }
                }
                else
                {
                    this._refCnt = 0;
                    this.ExcelPackage.Dispose();
                    this.Disposed = true;
                }
            }
        }

        #endregion //end of Dispose Methods

    }

    public sealed class ConditionalFormatValue
    {
        public enum Types
        {

            //
            // Summary:
            //     Formula
            Formula = 0,
            //
            // Summary:
            //     Maximum Value
            Max = 1,
            //
            // Summary:
            //     Minimum Value
            Min = 2,
            //
            // Summary:
            //     Number Value
            Num = 3,
            //
            // Summary:
            //     Percent
            Percent = 4,
            //
            // Summary:
            //     Percentile
            Percentile = 5,

            Automatic = 15,
            NoValue = 16,
            Text = Formula
        }

        public enum RuleTypes
        {

            //
            // Summary:
            //     This conditional formatting rule highlights cells that are above the average
            //     for all values in the range.
            //
            // Remarks:
            //     AboveAverage Excel CF Rule Type
            AboveAverage = 0,
            //
            // Summary:
            //     This conditional formatting rule highlights cells that are above or equal the
            //     average for all values in the range.
            //
            // Remarks:
            //     AboveAverage Excel CF Rule Type
            AboveOrEqualAverage = 1,
            //
            // Summary:
            //     This conditional formatting rule highlights cells that are below the average
            //     for all values in the range.
            //
            // Remarks:
            //     AboveAverage Excel CF Rule Type
            BelowAverage = 2,
            //
            // Summary:
            //     This conditional formatting rule highlights cells that are below or equal the
            //     average for all values in the range.
            //
            // Remarks:
            //     AboveAverage Excel CF Rule Type
            BelowOrEqualAverage = 3,
            //
            // Summary:
            //     This conditional formatting rule highlights cells that are above the standard
            //     deviationa for all values in the range. AboveAverage Excel CF Rule Type
            AboveStdDev = 4,
            //
            // Summary:
            //     This conditional formatting rule highlights cells that are below the standard
            //     deviationa for all values in the range.
            //
            // Remarks:
            //     AboveAverage Excel CF Rule Type
            BelowStdDev = 5,
            //
            // Summary:
            //     This conditional formatting rule highlights cells whose values fall in the bottom
            //     N bracket as specified.
            //
            // Remarks:
            //     Top10 Excel CF Rule Type
            Bottom = 6,
            //
            // Summary:
            //     This conditional formatting rule highlights cells whose values fall in the bottom
            //     N percent as specified.
            //
            // Remarks:
            //     Top10 Excel CF Rule Type
            BottomPercent = 7,
            //
            // Summary:
            //     This conditional formatting rule highlights cells whose values fall in the top
            //     N bracket as specified.
            //
            // Remarks:
            //     Top10 Excel CF Rule Type
            Top = 8,
            //
            // Summary:
            //     This conditional formatting rule highlights cells whose values fall in the top
            //     N percent as specified.
            //
            // Remarks:
            //     Top10 Excel CF Rule Type
            TopPercent = 9,
            //
            // Summary:
            //     This conditional formatting rule highlights cells containing dates in the last
            //     7 days.
            //
            // Remarks:
            //     TimePeriod Excel CF Rule Type
            Last7Days = 10,
            //
            // Summary:
            //     This conditional formatting rule highlights cells containing dates in the last
            //     month.
            //
            // Remarks:
            //     TimePeriod Excel CF Rule Type
            LastMonth = 11,
            //
            // Summary:
            //     This conditional formatting rule highlights cells containing dates in the last
            //     week.
            //
            // Remarks:
            //     TimePeriod Excel CF Rule Type
            LastWeek = 12,
            //
            // Summary:
            //     This conditional formatting rule highlights cells containing dates in the next
            //     month.
            //
            // Remarks:
            //     TimePeriod Excel CF Rule Type
            NextMonth = 13,
            //
            // Summary:
            //     This conditional formatting rule highlights cells containing dates in the next
            //     week.
            //
            // Remarks:
            //     TimePeriod Excel CF Rule Type
            NextWeek = 14,
            //
            // Summary:
            //     This conditional formatting rule highlights cells containing dates in this month.
            //
            // Remarks:
            //     TimePeriod Excel CF Rule Type
            ThisMonth = 15,
            //
            // Summary:
            //     This conditional formatting rule highlights cells containing dates in this week.
            //
            // Remarks:
            //     TimePeriod Excel CF Rule Type
            ThisWeek = 16,
            //
            // Summary:
            //     This conditional formatting rule highlights cells containing today dates.
            //
            // Remarks:
            //     TimePeriod Excel CF Rule Type
            Today = 17,
            //
            // Summary:
            //     This conditional formatting rule highlights cells containing tomorrow dates.
            //
            // Remarks:
            //     TimePeriod Excel CF Rule Type
            Tomorrow = 18,
            //
            // Summary:
            //     This conditional formatting rule highlights cells containing yesterday dates.
            //
            // Remarks:
            //     TimePeriod Excel CF Rule Type
            Yesterday = 19,
            //
            // Summary:
            //     This conditional formatting rule highlights cells in the range that begin with
            //     the given text.
            //
            // Remarks:
            //     Equivalent to using the LEFT() sheet function and comparing values.
            BeginsWith = 20,
            //
            // Summary:
            //     This conditional formatting rule highlights cells in the range between the given
            //     two formulas.
            //
            // Remarks:
            //     CellIs Excel CF Rule Type
            Between = 21,
            //
            // Summary:
            //     This conditional formatting rule highlights cells that are completely blank.
            //
            // Remarks:
            //     Equivalent of using LEN(TRIM()). This means that if the cell contains only characters
            //     that TRIM() would remove, then it is considered blank. An empty cell is also
            //     considered blank.
            ContainsBlanks = 22,
            //
            // Summary:
            //     This conditional formatting rule highlights cells with formula errors.
            //
            // Remarks:
            //     Equivalent to using ISERROR() sheet function to determine if there is a formula
            //     error.
            ContainsErrors = 23,
            //
            // Summary:
            //     This conditional formatting rule highlights cells in the range that begin with
            //     the given text.
            //
            // Remarks:
            //     Equivalent to using the LEFT() sheet function and comparing values.
            ContainsText = 24,
            //
            // Summary:
            //     This conditional formatting rule highlights duplicated values.
            //
            // Remarks:
            //     DuplicateValues Excel CF Rule Type
            DuplicateValues = 25,
            //
            // Summary:
            //     This conditional formatting rule highlights cells ending with given text.
            //
            // Remarks:
            //     Equivalent to using the RIGHT() sheet function and comparing values.
            EndsWith = 26,
            //
            // Summary:
            //     This conditional formatting rule highlights cells equals to with given formula.
            //
            // Remarks:
            //     CellIs Excel CF Rule Type
            Equal = 27,
            //
            // Summary:
            //     This conditional formatting rule contains a formula to evaluate. When the formula
            //     result is true, the cell is highlighted.
            //
            // Remarks:
            //     Expression Excel CF Rule Type
            Expression = 28,
            //
            // Summary:
            //     This conditional formatting rule highlights cells greater than the given formula.
            //
            // Remarks:
            //     CellIs Excel CF Rule Type
            GreaterThan = 29,
            //
            // Summary:
            //     This conditional formatting rule highlights cells greater than or equal the given
            //     formula.
            //
            // Remarks:
            //     CellIs Excel CF Rule Type
            GreaterThanOrEqual = 30,
            //
            // Summary:
            //     This conditional formatting rule highlights cells less than the given formula.
            //
            // Remarks:
            //     CellIs Excel CF Rule Type
            LessThan = 31,
            //
            // Summary:
            //     This conditional formatting rule highlights cells less than or equal the given
            //     formula.
            //
            // Remarks:
            //     CellIs Excel CF Rule Type
            LessThanOrEqual = 32,
            //
            // Summary:
            //     This conditional formatting rule highlights cells outside the range in given
            //     two formulas.
            //
            // Remarks:
            //     CellIs Excel CF Rule Type
            NotBetween = 33,
            //
            // Summary:
            //     This conditional formatting rule highlights cells that does not contains the
            //     given formula.
            //
            // Remarks:
            //     CellIs Excel CF Rule Type
            NotContains = 34,
            //
            // Summary:
            //     This conditional formatting rule highlights cells that are not blank.
            //
            // Remarks:
            //     Equivalent of using LEN(TRIM()). This means that if the cell contains only characters
            //     that TRIM() would remove, then it is considered blank. An empty cell is also
            //     considered blank.
            NotContainsBlanks = 35,
            //
            // Summary:
            //     This conditional formatting rule highlights cells without formula errors.
            //
            // Remarks:
            //     Equivalent to using ISERROR() sheet function to determine if there is a formula
            //     error.
            NotContainsErrors = 36,
            //
            // Summary:
            //     This conditional formatting rule highlights cells that do not contain the given
            //     text.
            //
            // Remarks:
            //     Equivalent to using the SEARCH() sheet function.
            NotContainsText = 37,
            //
            // Summary:
            //     This conditional formatting rule highlights cells not equals to with given formula.
            //
            // Remarks:
            //     CellIs Excel CF Rule Type
            NotEqual = 38,
            //
            // Summary:
            //     This conditional formatting rule highlights unique values in the range.
            //
            // Remarks:
            //     UniqueValues Excel CF Rule Type
            UniqueValues = 39,
            //
            // Summary:
            //     Three Color Scale (Low, Middle and High Color Scale)
            //
            // Remarks:
            //     ColorScale Excel CF Rule Type
            ThreeColorScale = 40,
            //
            // Summary:
            //     Two Color Scale (Low and High Color Scale)
            //
            // Remarks:
            //     ColorScale Excel CF Rule Type
            TwoColorScale = 41,
            //
            // Summary:
            //     This conditional formatting rule applies a 3 set icons to cells according to
            //     their values.
            //
            // Remarks:
            //     IconSet Excel CF Rule Type
            ThreeIconSet = 42,
            //
            // Summary:
            //     This conditional formatting rule applies a 4 set icons to cells according to
            //     their values.
            //
            // Remarks:
            //     IconSet Excel CF Rule Type
            FourIconSet = 43,
            //
            // Summary:
            //     This conditional formatting rule applies a 5 set icons to cells according to
            //     their values.
            //
            // Remarks:
            //     IconSet Excel CF Rule Type
            FiveIconSet = 44,
            //
            // Summary:
            //     This conditional formatting rule displays a gradated data bar in the range of
            //     cells.
            //
            // Remarks:
            //     DataBar Excel CF Rule Type
            DataBar = 45
        }

        [JsonConverter(typeof(StringEnumConverter))]
        public Types Type { get; set; } = Types.Num;
        [JsonConverter(typeof(StringEnumConverter))]
        public Types SubType { get; set; } = Types.Num;

        [JsonConverter(typeof(StringEnumConverter))]
        public RuleTypes RuleType { get; set; } = RuleTypes.Equal;
        [JsonConverter(typeof(StringEnumConverter))]
        public RuleTypes SubRuleType { get; set; } = RuleTypes.Equal;

        public double Value { get; set; }
        public string FormulaText { get; set; }
        public string FormulaTextBetween { get; set; }
        
        public System.Drawing.Color Color { get; set; } = System.Drawing.Color.Empty;

        public bool ShowValue { get; set; } = true;
        public bool StopIfTrue { get; set; } = false;

        public int Priority { get; set; } = 1;

        public bool IncludeTotalRow { get; set; }

        public bool Enabled { get; set; } = true;

        public override string ToString()
        {
            return Newtonsoft.Json.JsonConvert.SerializeObject(this);
        }
    }

    public static class Helpers
    {
        private static String HexConverter(System.Drawing.Color c)
        {
            return c.R.ToString("X2") + c.G.ToString("X2") + c.B.ToString("X2");
        }

        static public void CreateDataBarAutomatic(ExcelWorksheet workSheet, ExcelRange formatRange, ConditionalFormatValue condFormatValue)
        {
            var condFmtId = Guid.NewGuid();
            var barColor = condFormatValue.Color;
            var condFmt = workSheet.ConditionalFormatting.AddDatabar(formatRange, barColor);

            condFmt.ShowValue = condFormatValue.ShowValue;
            condFmt.StopIfTrue = condFormatValue.StopIfTrue;
            condFmt.Priority = condFormatValue.Priority;

            {
                var ns = new System.Xml.XmlNamespaceManager(workSheet.WorksheetXml.NameTable);
                ns.AddNamespace("d", workSheet.WorksheetXml.DocumentElement.NamespaceURI);

                {
                    var dataBarNode = workSheet.WorksheetXml.SelectSingleNode(string.Format("//d:conditionalFormatting[@sqref='{0}']//d:dataBar", formatRange), ns);
                    var parentNode = dataBarNode.ParentNode;

                    parentNode.RemoveChild(dataBarNode);

                    var newDataBarNode = parentNode.OwnerDocument.CreateNode(XmlNodeType.Element, "dataBar", workSheet.WorksheetXml.DocumentElement.NamespaceURI);
                    var newDataBarElement = (System.Xml.XmlElement)newDataBarNode;

                    parentNode.AppendChild(newDataBarNode);

                    var newcfvoNode = newDataBarNode.OwnerDocument.CreateNode(XmlNodeType.Element, "cfvo", workSheet.WorksheetXml.DocumentElement.NamespaceURI);
                    var newcfvoElement = (System.Xml.XmlElement)newcfvoNode;

                    newDataBarNode.AppendChild(newcfvoNode);
                    newcfvoElement.SetAttribute("type", "min");

                    newcfvoNode = newDataBarNode.OwnerDocument.CreateNode(XmlNodeType.Element, "cfvo", workSheet.WorksheetXml.DocumentElement.NamespaceURI);
                    newcfvoElement = (System.Xml.XmlElement)newcfvoNode;

                    newDataBarNode.AppendChild(newcfvoNode);
                    newcfvoElement.SetAttribute("type", "max");

                    var newcolorNode = newDataBarNode.OwnerDocument.CreateNode(XmlNodeType.Element, "color", workSheet.WorksheetXml.DocumentElement.NamespaceURI);
                    var newcolorElement = (System.Xml.XmlElement)newcolorNode;

                    newDataBarNode.AppendChild(newcolorNode);
                    newcolorElement.SetAttribute("rgb", HexConverter(barColor));
                }

                {
                    var cfRuleNode = workSheet.WorksheetXml.SelectSingleNode(string.Format("//d:conditionalFormatting[@sqref='{0}']//d:cfRule", formatRange), ns);
                    var extLstWs = cfRuleNode.SelectSingleNode("//d:extList", ns);

                    if (extLstWs == null)
                    {
                        extLstWs = cfRuleNode.OwnerDocument.CreateNode(XmlNodeType.Element, "extLst", workSheet.WorksheetXml.DocumentElement.NamespaceURI);

                        extLstWs.InnerXml = string.Format(@"<ext uri=""{{B025F937-C7B1-47D3-B67F-A62EFF666E3E}}""
														xmlns:x14=""http://schemas.microsoft.com/office/spreadsheetml/2009/9/main"">
														<x14:id>{{{0}}}</x14:id>
													</ext>",
                                                            condFmtId);

                        cfRuleNode.AppendChild(extLstWs);
                    }
                    else
                    {
                        var extWs = extLstWs.OwnerDocument.CreateNode(XmlNodeType.Element, "ext", "http://schemas.microsoft.com/office/spreadsheetml/2009/9/main");

                        extLstWs.AppendChild(extWs);

                        ((System.Xml.XmlElement)extWs).SetAttribute("uri",
                                                                    "http://schemas.microsoft.com/office/spreadsheetml/2009/9/main",
                                                                    "{B025F937-C7B1-47D3-B67F-A62EFF666E3E}");

                        var extId = extWs.OwnerDocument.CreateNode(XmlNodeType.Element, "id", "http://schemas.microsoft.com/office/spreadsheetml/2009/9/main");

                        extId.InnerXml = string.Format("{{{0}}}", condFmtId);

                        extWs.AppendChild(extId);
                    }
                }
            }

            {
                var xdoc = workSheet.WorksheetXml;
                var nsm = new System.Xml.XmlNamespaceManager(workSheet.WorksheetXml.NameTable);
                nsm.AddNamespace("default", workSheet.WorksheetXml.DocumentElement.NamespaceURI);
                nsm.AddNamespace("x14", "http://schemas.microsoft.com/office/spreadsheetml/2009/9/main");
                nsm.AddNamespace("xm", "http://schemas.microsoft.com/office/excel/2006/main");

                var condFormattingsNode = xdoc.SelectSingleNode("//x14:conditionalFormattings", nsm);

                if (condFormattingsNode == null)
                {
                    var extLstWs = xdoc.CreateNode(XmlNodeType.Element, "extLst", workSheet.WorksheetXml.DocumentElement.NamespaceURI);

                    extLstWs.InnerXml = string.Format(@"<ext uri=""{{78C0D931-6437-407d-A8EE-F0AAD7539E65}}"" 
                                        xmlns:x14=""http://schemas.microsoft.com/office/spreadsheetml/2009/9/main"">
                                    <x14:conditionalFormattings>
                                    <x14:conditionalFormatting xmlns:xm=""http://schemas.microsoft.com/office/excel/2006/main"">
                                    <x14:cfRule type=""dataBar"" id=""{{{0}}}"">
	                                        <x14:dataBar minLength=""0"" maxLength=""100"" border=""1"" negativeBarBorderColorSameAsPositive=""0"">
	                                        <x14:cfvo type=""autoMin""/>
											<x14:cfvo type = ""autoMax""/>
											<x14:borderColor rgb = ""{1}""/>
											<x14:negativeFillColor rgb = ""FFFF0000""/>
											<x14:negativeBorderColor rgb = ""FFFF0000""/>
											<x14:axisColor rgb = ""FF000000""/>
											 </x14:dataBar>
                                    </x14:cfRule>
                                    <xm:sqref>{2}</xm:sqref>
                                    </x14:conditionalFormatting>                                    
                                    </x14:conditionalFormattings>
                                </ext>",
                                    condFmtId,
                                    HexConverter(barColor),
                                    formatRange);
                    var wsNode = xdoc.SelectSingleNode("/default:worksheet", nsm);
                    wsNode.AppendChild(extLstWs);
                }
                else
                {
                    var condFormatNode = xdoc.CreateNode(XmlNodeType.Element, "x14:conditionalFormatting", "http://schemas.microsoft.com/office/spreadsheetml/2009/9/main");

                    condFormatNode.InnerXml = string.Format(@"<x14:cfRule type=""dataBar"" id=""{{{0}}}"" xmlns:x14=""http://schemas.microsoft.com/office/spreadsheetml/2009/9/main"">
						                                        <x14:dataBar minLength=""0"" maxLength=""100"" border=""1"" negativeBarBorderColorSameAsPositive=""0"">
						                                        <x14:cfvo type=""autoMin""/>
																<x14:cfvo type = ""autoMax""/>
																<x14:borderColor rgb = ""{1}""/>
																<x14:negativeFillColor rgb = ""FFFF0000""/>
																<x14:negativeBorderColor rgb = ""FFFF0000""/>
																<x14:axisColor rgb = ""FF000000""/>
																 </x14:dataBar>
					                                    </x14:cfRule>
					                                    <xm:sqref xmlns:xm=""http://schemas.microsoft.com/office/excel/2006/main"">{2}</xm:sqref>",
                                                        condFmtId,
                                                        HexConverter(condFmt.Color),
                                                        formatRange);

                    condFormattingsNode.AppendChild(condFormatNode);
                }

            }
        }

        static public void CreateConditionalFormat(this ExcelWorksheet workSheet, ExcelRange formatRange, params ConditionalFormatValue[] condFormatValues)
        {
            if (condFormatValues.Length == 1)
            {
                if (!condFormatValues[0].Enabled) return;

                if (condFormatValues[0].RuleType == ConditionalFormatValue.RuleTypes.DataBar)
                {
                    if(condFormatValues[0].Type == ConditionalFormatValue.Types.Automatic)
                        CreateDataBarAutomatic(workSheet, formatRange, condFormatValues[0]);
                    else
                    {
                        var condFmt = workSheet.ConditionalFormatting.AddDatabar(formatRange, condFormatValues[0].Color);

                        condFmt.ShowValue = condFormatValues[0].ShowValue;
                        condFmt.StopIfTrue = condFormatValues[0].StopIfTrue;
                        condFmt.Priority = condFormatValues[0].Priority;
                    }

                    return;
                }
            }
            else if (condFormatValues.Length == 2)
            {
                if (condFormatValues[0].Enabled && condFormatValues[1].Enabled)
                {
                    if (condFormatValues[0].RuleType == ConditionalFormatValue.RuleTypes.DataBar
                        && condFormatValues[1].RuleType == ConditionalFormatValue.RuleTypes.DataBar
                        && condFormatValues[0].Color == condFormatValues[1].Color)
                    {
                        var condFmt = workSheet.ConditionalFormatting.AddDatabar(formatRange, condFormatValues[0].Color);

                        condFmt.ShowValue = condFormatValues[0].ShowValue || condFormatValues[1].ShowValue;
                        condFmt.StopIfTrue = condFormatValues[0].StopIfTrue || condFormatValues[1].StopIfTrue;
                        condFmt.Priority = Math.Min(condFormatValues[0].Priority, condFormatValues[1].Priority);

                        if (condFormatValues[0].Type == ConditionalFormatValue.Types.Max)
                        {
                            if (condFormatValues[0].SubType == ConditionalFormatValue.Types.NoValue)
                            {
                                condFmt.HighValue.Type = eExcelConditionalFormattingValueObjectType.Max;
                            }
                            else
                            {
                                condFmt.HighValue.Type = (eExcelConditionalFormattingValueObjectType)condFormatValues[0].SubType;
                                condFmt.HighValue.Formula = condFormatValues[0].FormulaText;
                                condFmt.HighValue.Value = condFormatValues[0].Value;
                            }
                            condFmt.HighValue.GreaterThanOrEqualTo = condFormatValues[0].SubRuleType == ConditionalFormatValue.RuleTypes.GreaterThanOrEqual;

                            if (condFormatValues[1].SubType == ConditionalFormatValue.Types.NoValue)
                            {
                                condFmt.LowValue.Type = eExcelConditionalFormattingValueObjectType.Min;
                            }
                            else
                            {
                                condFmt.LowValue.Type = (eExcelConditionalFormattingValueObjectType)condFormatValues[1].SubType;
                                condFmt.LowValue.Formula = condFormatValues[1].FormulaText;
                                condFmt.LowValue.Value = condFormatValues[1].Value;
                            }

                            condFmt.LowValue.GreaterThanOrEqualTo = condFormatValues[1].SubRuleType == ConditionalFormatValue.RuleTypes.GreaterThanOrEqual;
                        }
                        else if (condFormatValues[1].Type == ConditionalFormatValue.Types.Max)
                        {
                            if (condFormatValues[1].SubType == ConditionalFormatValue.Types.NoValue)
                            {
                                condFmt.HighValue.Type = eExcelConditionalFormattingValueObjectType.Max;
                            }
                            else
                            {
                                condFmt.HighValue.Type = (eExcelConditionalFormattingValueObjectType)condFormatValues[1].SubType;
                                condFmt.HighValue.Formula = condFormatValues[1].FormulaText;
                                condFmt.HighValue.Value = condFormatValues[1].Value;
                            }
                            condFmt.HighValue.GreaterThanOrEqualTo = condFormatValues[1].SubRuleType == ConditionalFormatValue.RuleTypes.GreaterThanOrEqual;

                            if (condFormatValues[0].SubType == ConditionalFormatValue.Types.NoValue)
                            {
                                condFmt.LowValue.Type = eExcelConditionalFormattingValueObjectType.Min;
                            }
                            else
                            {
                                condFmt.LowValue.Type = (eExcelConditionalFormattingValueObjectType)condFormatValues[0].SubType;
                                condFmt.LowValue.Formula = condFormatValues[0].FormulaText;
                                condFmt.LowValue.Value = condFormatValues[0].Value;
                            }

                            condFmt.LowValue.GreaterThanOrEqualTo = condFormatValues[0].SubRuleType == ConditionalFormatValue.RuleTypes.GreaterThanOrEqual;
                        }
                        else
                        {
                            throw new ArgumentException("Invalid DataBar Parameters");
                        }
                        return;
                    }
                    else if (condFormatValues[0].RuleType == ConditionalFormatValue.RuleTypes.TwoColorScale
                                && condFormatValues[1].RuleType == ConditionalFormatValue.RuleTypes.TwoColorScale)
                    {
                        var condFmt = workSheet.ConditionalFormatting.AddTwoColorScale(formatRange);

                        condFmt.StopIfTrue = condFormatValues[0].StopIfTrue || condFormatValues[1].StopIfTrue;
                        condFmt.Priority = Math.Min(condFormatValues[0].Priority, condFormatValues[1].Priority);

                        if (condFormatValues[0].Type == ConditionalFormatValue.Types.Max)
                        {
                            if (condFormatValues[0].SubType == ConditionalFormatValue.Types.NoValue)
                            {
                                condFmt.HighValue.Type = eExcelConditionalFormattingValueObjectType.Max;
                            }
                            else
                            {
                                condFmt.HighValue.Type = (eExcelConditionalFormattingValueObjectType)condFormatValues[0].SubType;
                                condFmt.HighValue.Formula = condFormatValues[0].FormulaText;
                                condFmt.HighValue.Value = condFormatValues[0].Value;
                            }
                            condFmt.HighValue.Color = condFormatValues[0].Color;

                            if (condFormatValues[1].SubType == ConditionalFormatValue.Types.NoValue)
                            {
                                condFmt.LowValue.Type = eExcelConditionalFormattingValueObjectType.Min;
                            }
                            else
                            {
                                condFmt.LowValue.Type = (eExcelConditionalFormattingValueObjectType)condFormatValues[1].SubType;
                                condFmt.LowValue.Formula = condFormatValues[1].FormulaText;
                                condFmt.LowValue.Value = condFormatValues[1].Value;
                            }
                            condFmt.LowValue.Color = condFormatValues[1].Color;
                        }
                        else if (condFormatValues[1].Type == ConditionalFormatValue.Types.Max)
                        {
                            if (condFormatValues[1].SubType == ConditionalFormatValue.Types.NoValue)
                            {
                                condFmt.HighValue.Type = eExcelConditionalFormattingValueObjectType.Max;
                            }
                            else
                            {
                                condFmt.HighValue.Type = (eExcelConditionalFormattingValueObjectType)condFormatValues[1].SubType;
                                condFmt.HighValue.Formula = condFormatValues[1].FormulaText;
                                condFmt.HighValue.Value = condFormatValues[1].Value;
                            }
                            condFmt.HighValue.Color = condFormatValues[1].Color;

                            if (condFormatValues[0].SubType == ConditionalFormatValue.Types.NoValue)
                            {
                                condFmt.LowValue.Type = eExcelConditionalFormattingValueObjectType.Min;
                            }
                            else
                            {
                                condFmt.LowValue.Type = (eExcelConditionalFormattingValueObjectType)condFormatValues[0].SubType;
                                condFmt.LowValue.Formula = condFormatValues[0].FormulaText;
                                condFmt.LowValue.Value = condFormatValues[0].Value;
                            }
                            condFmt.LowValue.Color = condFormatValues[0].Color;
                        }
                        else
                        {
                            throw new ArgumentException("Invalid TwoColorScale Parameters");
                        }
                        return;
                    }
                }
                else
                {
                    return;
                }
            }
            else if (condFormatValues.Length == 3)
            {
                if (condFormatValues[0].Enabled && condFormatValues[1].Enabled && condFormatValues[2].Enabled)
                {
                    if (condFormatValues[0].RuleType == ConditionalFormatValue.RuleTypes.ThreeColorScale
                            && condFormatValues[1].RuleType == ConditionalFormatValue.RuleTypes.ThreeColorScale
                            && condFormatValues[2].RuleType == ConditionalFormatValue.RuleTypes.ThreeColorScale)
                    {
                        var condFmt = workSheet.ConditionalFormatting.AddThreeColorScale(formatRange);

                        condFmt.StopIfTrue = condFormatValues[0].StopIfTrue || condFormatValues[1].StopIfTrue || condFormatValues[2].StopIfTrue;
                        condFmt.Priority = Math.Min(condFormatValues[0].Priority, Math.Min(condFormatValues[1].Priority, condFormatValues[2].Priority));

                        foreach (var condFmtValue in condFormatValues)
                        {
                            if (condFmtValue.Type == ConditionalFormatValue.Types.Max)
                            {
                                if (condFmtValue.SubType == ConditionalFormatValue.Types.NoValue)
                                {
                                    condFmt.HighValue.Type = eExcelConditionalFormattingValueObjectType.Max;
                                }
                                else
                                {
                                    condFmt.HighValue.Type = (eExcelConditionalFormattingValueObjectType)condFmtValue.SubType;
                                    condFmt.HighValue.Formula = condFmtValue.FormulaText;
                                    condFmt.HighValue.Value = condFmtValue.Value;
                                }
                                condFmt.HighValue.Color = condFmtValue.Color;
                            }
                            else if (condFmtValue.Type == ConditionalFormatValue.Types.Min)
                            {
                                if (condFmtValue.SubType == ConditionalFormatValue.Types.NoValue)
                                {
                                    condFmt.LowValue.Type = eExcelConditionalFormattingValueObjectType.Min;
                                }
                                else
                                {
                                    condFmt.LowValue.Type = (eExcelConditionalFormattingValueObjectType)condFmtValue.SubType;
                                    condFmt.LowValue.Formula = condFmtValue.FormulaText;
                                    condFmt.LowValue.Value = condFmtValue.Value;
                                }
                                condFmt.LowValue.Color = condFmtValue.Color;
                            }
                            else
                            {
                                condFmt.MiddleValue.Type = (eExcelConditionalFormattingValueObjectType)condFmtValue.SubType;
                                condFmt.MiddleValue.Formula = condFmtValue.FormulaText;
                                condFmt.MiddleValue.Value = condFmtValue.Value;
                                condFmt.MiddleValue.Color = condFmtValue.Color;
                            }
                        }


                        return;
                    }
                }
                else
                {
                    return;
                }
            }

            foreach (var condFmtValue in condFormatValues)
            {
                if(!condFmtValue.Enabled) continue;

                switch (condFmtValue.RuleType)
                {
                    case ConditionalFormatValue.RuleTypes.AboveAverage:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddAboveAverage(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.AboveOrEqualAverage:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddAboveOrEqualAverage(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;                            
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.BelowAverage:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddBelowAverage(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;                            
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.BelowOrEqualAverage:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddBelowOrEqualAverage(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.AboveStdDev:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddAboveStdDev(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;
                            condFmt.StdDev = (ushort)condFmtValue.Value;
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.BelowStdDev:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddBelowStdDev(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;
                            condFmt.StdDev = (ushort)condFmtValue.Value;
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.Bottom:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddBottom(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;
                            condFmt.Rank = (ushort)condFmtValue.Value;
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.BottomPercent:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddBottomPercent(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;
                            condFmt.Rank = (ushort)condFmtValue.Value;
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.Top:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddTop(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;
                            condFmt.Rank = (ushort)condFmtValue.Value;
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.TopPercent:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddTopPercent(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;                           
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.Last7Days:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddLast7Days(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;                            
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.LastMonth:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddLastMonth(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;                            
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.LastWeek:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddLast7Days(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;                            
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.NextMonth:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddNextMonth(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.NextWeek:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddNextWeek(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;                            
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.ThisMonth:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddThisMonth(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;                            
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.ThisWeek:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddThisWeek(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;                            
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.Today:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddToday(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;                            
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.Tomorrow:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddTomorrow(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;                            
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.Yesterday:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddYesterday(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.BeginsWith:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddBeginsWith(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;
                            condFmt.Text = condFmtValue.FormulaText;
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.Between:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddBetween(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;
                            condFmt.Formula = condFmtValue.FormulaText;
                            condFmt.Formula2 = condFmtValue.FormulaTextBetween;                            
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.ContainsBlanks:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddContainsBlanks(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;                            
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.ContainsErrors:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddContainsErrors(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;                            
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.ContainsText:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddContainsText(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;
                            condFmt.Text = condFmtValue.FormulaText;
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.DuplicateValues:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddDuplicateValues(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;                            
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.EndsWith:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddEndsWith(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;
                            condFmt.Text = condFmtValue.FormulaText;
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.Equal:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddEqual(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;
                            condFmt.Formula = condFmtValue.FormulaText;
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.Expression:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddExpression(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;
                            condFmt.Formula = condFmtValue.FormulaText;
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.GreaterThan:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddGreaterThan(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;
                            condFmt.Formula = condFmtValue.FormulaText;
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.GreaterThanOrEqual:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddGreaterThanOrEqual(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;
                            condFmt.Formula = condFmtValue.FormulaText;                            
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.LessThan:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddLessThan(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;
                            condFmt.Formula = condFmtValue.FormulaText;
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.LessThanOrEqual:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddLessThanOrEqual(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;
                            condFmt.Formula = condFmtValue.FormulaText;                            
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.NotBetween:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddNotBetween(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;
                            condFmt.Formula = condFmtValue.FormulaText;
                            condFmt.Formula2 = condFmtValue.FormulaTextBetween;
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.NotContains:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddNotContainsText(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;
                            condFmt.Text = condFmtValue.FormulaTextBetween;
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.NotContainsBlanks:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddNotContainsBlanks(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;                            
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.NotContainsErrors:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddNotContainsErrors(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;                            
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.NotContainsText:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddNotContainsText(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;
                            condFmt.Text = condFmtValue.FormulaText;
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.NotEqual:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddNotEqual(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;
                            condFmt.Formula = condFmtValue.FormulaText;                           
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.UniqueValues:
                        {
                            var condFmt = workSheet.ConditionalFormatting.AddUniqueValues(formatRange);

                            condFmt.Style.Fill.BackgroundColor.Color = condFmtValue.Color;
                            condFmt.StopIfTrue = condFmtValue.StopIfTrue;
                            condFmt.Priority = condFmtValue.Priority;                           
                        }
                        break;
                    case ConditionalFormatValue.RuleTypes.ThreeColorScale:
                        throw new ArgumentException("ThreeColorScale was detected but is not valid");                        
                    case ConditionalFormatValue.RuleTypes.TwoColorScale:
                        throw new ArgumentException("TwoColorScale was detected but is not valid");
                    case ConditionalFormatValue.RuleTypes.ThreeIconSet:
                        throw new NotImplementedException("ThreeIconSet");
                        //break;
                    case ConditionalFormatValue.RuleTypes.FourIconSet:
                        throw new NotImplementedException("FourIconSet");
                    case ConditionalFormatValue.RuleTypes.FiveIconSet:
                        throw new NotImplementedException("FiveIconSet");
                    case ConditionalFormatValue.RuleTypes.DataBar:
                        {
                            CreateDataBarAutomatic(workSheet, formatRange, condFmtValue);
                        }
                        break;
                    default:
                        break;
                }

            }
            

        }

        static public void AutoFitColumn(this ExcelWorksheet workSheet, params ExcelRange[] autoFitRanges)
        {
            AutoFitColumn(workSheet, null, autoFitRanges);
        }

        static public void AutoFitColumn(this ExcelWorksheet workSheet, DataTable dataTable, params ExcelRange[] autoFitRanges)
        {
            try
            {
                if (autoFitRanges == null || autoFitRanges.Length == 0)
                {
                    workSheet.Cells.AutoFitColumns();
                }
                else
                {
                    foreach (var range in autoFitRanges)
                    {
                        try
                        {
                            range.AutoFitColumns();
                        }
                        catch (OverflowException)
                        {}                        
                    }
                }
            }
            catch (System.ArithmeticException)
            { }
            catch (System.ArgumentOutOfRangeException)
            { }
            catch (System.Exception ex)
            {
                Logger.Instance.Error(string.Format("Excel AutoFitColumns Exception Occurred in Worksheet \"{0}\". Worksheet was loaded/updated but NOT auto-formatted...", workSheet.Name), ex);
            }

            if (dataTable != null)
            {
                workSheet.ApplyColumnAttribs(dataTable);
            }
        }

        static public ExcelRange[] ExcelRange(this ExcelWorksheet workSheet, params DataColumn[] dcRanges)
        {
            var columnPairs = new List<Tuple<string, string>>();
            
            for (int nIdx = 0; nIdx < dcRanges.Length; ++nIdx)
            {
                columnPairs.Add(new Tuple<string, string>(dcRanges[nIdx].GetExcelColumnLetter(),
                                                            (dcRanges.ElementAtOrDefault(++nIdx) ?? dcRanges[nIdx-1]).GetExcelColumnLetter()));
            }
                                        

            return columnPairs.Select(l => workSheet.Cells[l.Item1 + ":" + l.Item2]).ToArray();
        }

        static public ExcelRange[] ExcelRange(this ExcelWorksheet workSheet, int excelRow, params DataColumn[] dcRanges)
        {
            var columnPairs = new List<Tuple<string, string>>();

            for (int nIdx = 0; nIdx < dcRanges.Length; ++nIdx)
            {
                columnPairs.Add(new Tuple<string, string>(dcRanges[nIdx].GetExcelColumnLetter(),
                                                            (dcRanges.ElementAtOrDefault(++nIdx) ?? dcRanges[nIdx - 1]).GetExcelColumnLetter()));
            }

            return columnPairs.Select(l => workSheet.Cells[string.Format("{0}{2}:{1}{2}", l.Item1, l.Item2, excelRow)]).ToArray();
        }

        static public void AutoFitColumn(this ExcelWorksheet workSheet, DataTable dataTable = null)
        {
            if(dataTable != null)
            {
                var colsWidths = new List<DataColumn>();
                var dtcols = new List<DataColumn>();

                foreach (DataColumn dataColumn in dataTable.Columns)
                {
                    dtcols.Add(dataColumn);

                    if (dataColumn.ExtendedProperties.Count == 0) continue;

                    foreach (var item in dataColumn.ExtendedProperties.Keys)
                    {
                        if (item != null && item is string)
                        {
                            var key = (string)item;

                            if (key == "ColumnWidth")
                            {
                                var colWidth = (int)dataColumn.ExtendedProperties["ColumnWidth"];

                                if (colWidth >= 0)
                                    colsWidths.Add(dataColumn);
                            }
                        }
                    }
                }

                if(colsWidths.HasAtLeastOneElement())
                {
                    var colRange = new List<DataColumn>();
                    DataColumn lstCol = null;
                    bool begRange = true;

                    foreach (var tblCol in dtcols)
                    {
                        if(colsWidths.Contains(tblCol))
                        {
                            if (lstCol != null)
                            {
                                colRange.Add(lstCol);
                                begRange = true;
                                lstCol = null;
                            }
                            continue;
                        }
                        lstCol = tblCol;

                        if(begRange)
                        {
                            colRange.Add(lstCol);
                            begRange = false;
                        }
                    }

                    if(lstCol != null)
                        colRange.Add(lstCol);

                    if (colRange.HasAtLeastOneElement())
                    {
                        if(colRange.Count % 2 != 0)
                            colRange.Add(lstCol);

                        workSheet.AutoFitColumn(dataTable, workSheet.ExcelRange(colRange.ToArray()));
                    }

                    return;
                }
            }

            try
            {
                workSheet.Cells.AutoFitColumns();
            }
            catch (System.ArithmeticException)
            { }
            catch (System.ArgumentOutOfRangeException)
            { }
            catch (System.Exception ex)
            {
                Logger.Instance.Error(string.Format("Excel AutoFitColumns Exception Occurred in Worksheet \"{0}\". Worksheet was loaded/updated but NOT auto-formatted...", workSheet.Name), ex);
            }

            if(dataTable != null)
            {
                workSheet.ApplyColumnAttribs(dataTable);
            }
        }

        /// <summary>
        /// Loads data table into a workbook&apos;s worksheet. If the worksheet exists it is cleared otherwise it is created.
        /// </summary>
        /// <param name="excelPkg">
        /// Open workbook instance.
        /// </param>
        /// <param name="workSheetName">
        /// Worksheet name. If null the data table name is used.
        /// </param>
        /// <param name="dtExcel">
        /// data table
        /// </param>
        /// <param name="worksheetAction">
        /// Called after the worksheet is loaded.
        /// </param>
        /// <param name="enableMaxRowLimitPerWorkSheet">
        /// if true (default) and the row count exceeds the maximum EPPlus row limit, an additional worksheets are created to accommodate all the rows. worksheetAction is called for all additional worksheets.
        /// </param>
        /// <param name="maxRowInExcelWorkSheet"></param>
        /// The maximum number of rows within a worksheet before another worksheet is created.
        /// <param name="startingWSCell"></param>
        /// <param name="useDefaultView">
        /// if useDefaultView is true the datatable&apos;s default view is used to filter and/or sort the table.
        /// Default is false.
        /// </param>
        /// <param name="renameFirstWorksheetIfDivided">
        /// renames the first worksheet (default) in cases where the maxRowInExcelWorkSheet is exceed. Otherwise the original workSheetName is used.
        /// </param>
        /// <param name="appendToWorkSheet">
        /// if true, worksheet is appended (not overwritten). The default is false.
        /// </param>
        /// <param name="clearWorkSheet">
        /// If true (default), the worksheet is first cleared (ignored if appendToWorksheet is true)
        /// </param>
        /// <param name="printHeaders">
        /// </param>
        /// <returns></returns>
        static public ExcelRangeBase WorkSheet(this ExcelPackage excelPkg,
                                                string workSheetName,
                                                System.Data.DataTable dtExcel,
                                                Action<ExcelWorksheet> worksheetAction = null,
                                                bool enableMaxRowLimitPerWorkSheet = true,
                                                int maxRowInExcelWorkSheet = -1,
                                                string startingWSCell = "A1",
                                                bool useDefaultView = false,
                                                bool renameFirstWorksheetIfDivided = true,
                                                bool appendToWorkSheet = false,
                                                bool printHeaders = true,
                                                bool clearWorkSheet = true)
        {
            if (string.IsNullOrEmpty(workSheetName)) workSheetName = dtExcel.TableName;

            dtExcel.AcceptChanges();

            var dtErrors = dtExcel.GetErrors();
            if (dtErrors.Length > 0)
            {
                Logger.Instance.Dump(dtErrors, Logger.DumpType.Error, "Table \"{0}\" Has Error", dtExcel.TableName);
            }

            if (dtExcel.Rows.Count == 0)
            {
                Logger.Instance.InfoFormat("No row in DataTable \"{0}\" for Excel WorkSheet \"{1}\" in Workbook \"{2}\".{3}",
                                            dtExcel.TableName,
                                            workSheetName,
                                            excelPkg.File?.Name,
                                            clearWorkSheet ? " Worksheet Cleared" : string.Empty);
                if (!appendToWorkSheet && clearWorkSheet)
                {
                    var clrWorkSheet = excelPkg.Workbook.Worksheets[workSheetName];

                    if (clrWorkSheet != null)
                    {
                        clrWorkSheet.Cells.Clear();
                        foreach (ExcelComment comment in clrWorkSheet.Comments.Cast<ExcelComment>().ToArray())
                        {
                            try
                            {
                                clrWorkSheet.Comments.Remove(comment);
                            }
                            catch { }
                        }
                    }
                }
                return null;
            }

            if(useDefaultView)
            {
                if (string.IsNullOrEmpty(dtExcel.DefaultView.Sort)
                        && string.IsNullOrEmpty(dtExcel.DefaultView.RowFilter)
                        && dtExcel.DefaultView.RowStateFilter == DataViewRowState.CurrentRows)
                {
                    useDefaultView = false;
                }
                else
                {
                    dtExcel = dtExcel.DefaultView.ToTable();
                }
            }

            if (dtExcel.Rows.Count == 0)
            {
                if (useDefaultView)
                {
                    Logger.Instance.InfoFormat("Using Default DataView (\"{2}\", \"{3}\", \"{4}\") within Excel Worksheet Loading for DataTable \"{0}\" into WorkSheet \"{1}\" for Workbook \"{5}\"",
                                                dtExcel.TableName,
                                                workSheetName,
                                                dtExcel.DefaultView.Sort,
                                                dtExcel.DefaultView.RowFilter,
                                                dtExcel.DefaultView.RowStateFilter,
                                                excelPkg.File?.Name);
                }
                Logger.Instance.InfoFormat("No row in DataTable \"{0}\" for Excel WorkSheet \"{1}\" in Workbook \"{2}\".{3}",
                                            dtExcel.TableName,
                                            workSheetName,
                                            excelPkg.File?.Name,
                                            clearWorkSheet ? " Worksheet Cleared" : string.Empty);
                if (!appendToWorkSheet && clearWorkSheet)
                {
                    var clrWorkSheet = excelPkg.Workbook.Worksheets[workSheetName];

                    if (clrWorkSheet != null)
                    {
                        clrWorkSheet.Cells.Clear();
                        foreach (ExcelComment comment in clrWorkSheet.Comments.Cast<ExcelComment>().ToArray())
                        {
                            try
                            {
                                clrWorkSheet.Comments.Remove(comment);
                            }
                            catch { }
                        }
                    }
                }
                return null;
            }

            if (maxRowInExcelWorkSheet > 0
                   && dtExcel.Rows.Count > maxRowInExcelWorkSheet)
            {
                var dtSplits = dtExcel.SplitTable(maxRowInExcelWorkSheet, useDefaultView);
                ExcelRangeBase excelRange = null;
                int splitCnt = 1;

                foreach (var dtSplit in dtSplits)
                {
                    excelRange = WorkSheet(excelPkg,
                                            renameFirstWorksheetIfDivided || splitCnt > 1
                                                ? string.Format("{0}-{1:000}", workSheetName, splitCnt)
                                                : workSheetName,
                                            dtSplit,
                                            worksheetAction,
                                            true,
                                            -1,
                                            startingWSCell,
                                            false,
                                            false,
                                            appendToWorkSheet,
                                            printHeaders,
                                            clearWorkSheet);
                    ++splitCnt;
                }

                return excelRange;
            }

            var workSheet = excelPkg.Workbook.Worksheets[workSheetName];
            if (workSheet == null)
            {
                try
                {
                    workSheet = excelPkg.Workbook.Worksheets.Add(workSheetName);
                }
                catch (InvalidOperationException)
                {
                    workSheet = excelPkg.Workbook.Worksheets[workSheetName];

                    if (workSheet == null)
                    {
                        throw;
                    }
                }
                catch
                {
                    throw;
                }
            }
            else
            {
                if (appendToWorkSheet)
                {
                    var row = workSheet.Dimension.End.Row;
                    var startingRange = workSheet.Cells[startingWSCell];
                    var endingColumn = workSheet.Dimension.End.Column;

                    if (row > startingRange.Start.Row)
                    {
                        while (row >= 1)
                        {
                            var range = workSheet.Cells[row, 1, row, endingColumn];

                            if (range.Any(c => !string.IsNullOrEmpty(c.Text)))
                            {
                                break;
                            }
                            row--;
                        }

                        startingWSCell = workSheet.Cells[row + 1, startingRange.End.Column].Address;
                        printHeaders = false;
                        clearWorkSheet = false;
                    }
                }

                if(clearWorkSheet)
                {
                    workSheet.Cells.Clear();
                    foreach (ExcelComment comment in workSheet.Comments.Cast<ExcelComment>().ToArray())
                    {
                        try
                        {
                            workSheet.Comments.Remove(comment);
                        }
                        catch { }
                    }
                }
            }

            Logger.Instance.InfoFormat("Loading DataTable \"{0}\" into Excel WorkSheet \"{1}\" at Cell \"{4}\" for Workbook \"{3}\". Rows: {2:###,###,##0}",
                                        dtExcel.TableName,
                                        workSheet.Name,
                                        dtExcel.Rows.Count,
                                        excelPkg.File?.Name,
                                        startingWSCell);

            if (useDefaultView)
            {
                Logger.Instance.InfoFormat("Using Default DataView (\"{2}\", \"{3}\", \"{4}\") within Excel Worksheet Loading for DataTable \"{0}\" into WorkSheet \"{1}\" for Workbook \"{5}\"",
                                            dtExcel.TableName,
                                            workSheetName,
                                            dtExcel.DefaultView.Sort,
                                            dtExcel.DefaultView.RowFilter,
                                            dtExcel.DefaultView.RowStateFilter,
                                            excelPkg.File?.Name);
            }

            if (dtExcel.Rows.Count > ExcelPackage.MaxRows)
            {
                if (enableMaxRowLimitPerWorkSheet
                        && Properties.Settings.Default.ExcelPackageMaxRowsLimit > 0)
                {
                    Logger.Instance.ErrorFormat("Table \"{0}\" with {1:###,###,##0} rows exceed maximum number of rows allowed by EPPlus (Max. Limit of {2:###,###,##0}) for worksheet \"{3}\" in workbook \"{4}\". The worksheet will be divided, creating multiple worksheets with a {5:###,###,##0} rows each. This may cause some workbook features to not work correctly!",
                                                dtExcel.TableName,
                                                dtExcel.Rows.Count,
                                                ExcelPackage.MaxRows,
                                                workSheetName,
                                                excelPkg.File?.Name,
                                                Properties.Settings.Default.ExcelPackageMaxRowsLimit);

                    return WorkSheet(excelPkg,
                                        workSheetName,
                                        dtExcel,
                                        worksheetAction,
                                        false,
                                        Properties.Settings.Default.ExcelPackageMaxRowsLimit,
                                        startingWSCell,
                                        false,
                                        false,
                                        appendToWorkSheet,
                                        printHeaders,
                                        clearWorkSheet);
                }
                else
                {
                    throw new ArgumentOutOfRangeException("dtExcel.Rows.Count",
                                                            string.Format("Table \"{0}\" with {1:###,###,##0} rows exceed maximum number of rows allowed by EPPlus (Max. Limit of {2:###,###,##0}) for worksheet \"{3}\" in workbook \"{4}\". Maybe enable DivideWorksheetIfExceedMaxRows parameter, or divide worksheet creation into smaller chucks.",
                                                                            dtExcel.TableName,
                                                                            dtExcel.Rows.Count,
                                                                            ExcelPackage.MaxRows,
                                                                            workSheetName,
                                                                            excelPkg.File?.Name));
                }
            }

            var loadRange = workSheet.Cells[startingWSCell].LoadFromDataTable(dtExcel, printHeaders);

            if (loadRange != null && worksheetAction != null)
            {
                try
                {
                    worksheetAction(workSheet);
                }
                catch (System.Exception ex)
                {
                    Logger.Instance.Error(string.Format("Exception Occurred in Worksheet \"{0}\" for Workbook \"{1}\"", workSheetName, excelPkg.File?.Name), ex);
                }
            }

            Logger.Instance.InfoFormat("Loaded DataTable \"{0}\" into Excel WorkSheet \"{1}\" at \"{4}\" for Workbook \"{3}\". Range: {2}",
                                        dtExcel.TableName,
                                        workSheet.Name,
                                        loadRange,
                                        excelPkg.File?.Name,
                                        startingWSCell);

            return loadRange;
        }

        static public void WorkBookWorkSheet(IFilePath orgTargetFile,
                                                IFilePath excelFile,
                                                string workSheetName,
                                                System.Data.DataTable dtExcel,
                                                ref int nResult,
                                                Action<WorkBookProcessingStage, IFilePath, IFilePath, string, ExcelPackage, DataTable, int, string> workBookActions = null,
                                                Action<ExcelWorksheet> worksheetAction = null,
                                                bool enableMaxRowLimitPerWorkSheet = true,
                                                int maxRowInExcelWorkSheet = -1,
                                                string startingWSCell = "A1",
                                                bool useDefaultView = false,
                                                bool renameFirstWorksheetIfDivided = true,
                                                bool appendToWorkSheet = false,
                                                bool printHeaders = true,
                                                bool clearWorkSheet = true,
                                                bool cachePackage = false,
                                                bool saveWorkSheet = true,
                                                IFilePath excelTemplateFile = null)
        {
            using (var excelPkgCache = ExcelPkgCache.GetAddExcelPackageCache(excelFile, cachePackage, excelTemplateFile))
            {
                var excelPkg = excelPkgCache.ExcelPackage;

                workBookActions?.Invoke(WorkBookProcessingStage.PreLoad, orgTargetFile, excelFile, workSheetName, excelPkg, dtExcel, -1, null);

                var loadRange = WorkSheet(excelPkg,
                                            workSheetName,
                                            dtExcel,
                                            worksheetAction,
                                            enableMaxRowLimitPerWorkSheet,
                                            maxRowInExcelWorkSheet,
                                            startingWSCell,
                                            useDefaultView,
                                            renameFirstWorksheetIfDivided,
                                            appendToWorkSheet,
                                            printHeaders,
                                            clearWorkSheet);

                System.Threading.Interlocked.Add(ref nResult, dtExcel.Rows.Count);

                workBookActions?.Invoke(WorkBookProcessingStage.PreSave, orgTargetFile, excelFile, workSheetName, excelPkg, dtExcel, dtExcel.Rows.Count, loadRange?.Address);

                if (saveWorkSheet)
                {
                    excelPkg.Save();
                    workBookActions?.Invoke(WorkBookProcessingStage.Saved, orgTargetFile, excelFile, workSheetName, excelPkg, dtExcel, dtExcel.Rows.Count, loadRange?.Address);
                    Logger.Instance.InfoFormat("Excel WorkBooks saved to \"{0}\"", excelFile.PathResolved);
                }
            }
        }



        /// <summary>
        /// Loads the data table into a new or existing Excel workbook based on the associated worksheet name (if worksheet exists it is cleared).
        /// </summary>
        /// <param name="excelFilePath"></param>
        /// <param name="workSheetName">
        /// if null the data table name is used.
        /// </param>
        /// <param name="dtExcel"></param>
        /// <param name="workBookActions">
        /// Called based on the workbook action
        /// The action has the following parameters:
        ///     Processing Stage <seealso cref="WorkBookProcessingStage"/>
        ///     Original workbook file
        ///     Actual workbook file that will be or is opened (null on PreProcess, PreProcessDataTable, and PostProcess)
        ///         Note on PrepareFileName action, the file path instance can be changed to reflect another actual file.
        ///     Worksheet Name
        ///     Excel Package Instance (workbook instance), null if workbook is not open
        ///         Additional EPPlus operations can be conducted against this instance.
        ///     Data Table
        ///     Total Number of rows loaded (-1 on PreProcess, PrepareFileName, PreLoad, PreProcessDataTable, and PostProcess).
        /// </param>
        /// <param name="worksheetAction">
        /// Called on each worksheet action
        /// </param>
        /// <param name="maxRowInExcelWorkBook">
        /// maximum number of rows that a workbook can contain. If rows exceed this number, multiple workbooks are created (if already exists that workbook is used).
        /// </param>
        /// <param name="maxRowInExcelWorkSheet">
        /// maximum number of rows in an individual worksheet. If rows exceed this number, multiple worksheets are created in this workbook. if maxRowInExcelWorkBook is exceeded a new workbook is created.
        /// </param>
        /// <param name="startingWSCell"></param>
        /// <param name="useDefaultView">
        /// if useDefaultView is true the datatable&apos;s default view is used to filter and/or sort the table.
        /// Default is false.
        /// </param>
        /// <param name="appendToWorkSheet">
        /// if true, worksheet is appended (not overwritten). The default is false.
        /// </param>
        /// <param name="clearWorkSheet">
        /// If true (default), the worksheet is first cleared (ignored if appendToWorksheetis true)
        /// </param>
        /// <param name="generateNewFileName">
        /// If true, the worksheet name is appended to the file name. Default is false.
        /// </param>
        /// <param name="processWorkSheetOnEmptyDataTable">
        /// If true (default), empty data tables are still processed. If false this method just returns (calls PostProcess event).
        /// </param>
        /// <returns></returns>
        static public int WorkBook(string excelFilePath,
                                                    string workSheetName,
                                                    DataTable dtExcel,
                                                    Action<WorkBookProcessingStage, IFilePath, IFilePath, string, ExcelPackage, DataTable, int, string> workBookActions = null,
                                                    Action<ExcelWorksheet> worksheetAction = null,
                                                    int maxRowInExcelWorkBook = -1,
                                                    int maxRowInExcelWorkSheet = -1,
                                                    string startingWSCell = "A1",
                                                    bool useDefaultView = false,
                                                    bool generateNewFileName = false,
                                                    bool appendToWorkSheet = false,
                                                    bool clearWorkSheet = true,
                                                    bool processWorkSheetOnEmptyDataTable = true,
                                                    bool cachePackage = false,
                                                    bool saveWorkSheet = true,
                                                    IFilePath excelTemplateFile = null)
        {
            var excelTargetFile = Common.Path.PathUtils.BuildFilePath(excelFilePath);
            var orgTargetFile = (IFilePath)excelTargetFile.Clone();
            int nResult = 0;

            if (string.IsNullOrEmpty(workSheetName)) workSheetName = dtExcel.TableName;

            workBookActions?.Invoke(WorkBookProcessingStage.PreProcess,
                                        orgTargetFile,
                                        null,
                                        workSheetName,
                                        null,
                                        dtExcel,
                                        -1,
                                        null);

            if (dtExcel.Rows.Count == 0 && !processWorkSheetOnEmptyDataTable)
            {
                workBookActions?.Invoke(WorkBookProcessingStage.PostProcess, orgTargetFile, null, null, null, dtExcel, 0, null);
                return 0;
            }

            if (maxRowInExcelWorkBook <= 0)
            {
                excelTargetFile.FileNameFormat = string.Format("{0}-{{0}}{1}",
                                                                        excelTargetFile.Name,
                                                                        string.IsNullOrEmpty(Properties.Settings.Default.ExcelWorkBookFileExtension)
                                                                            ? excelTargetFile.FileExtension
                                                                            : Properties.Settings.Default.ExcelWorkBookFileExtension);

                var excelFile = generateNewFileName
                                    ? ((IFilePath)excelTargetFile.Clone()).ApplyFileNameFormat(new object[] { workSheetName })
                                    : excelTargetFile;

                workBookActions?.Invoke(WorkBookProcessingStage.PrepareFileName, orgTargetFile, excelFile, workSheetName, null, dtExcel, -1, null);

                WorkBookWorkSheet(orgTargetFile,
                                    excelFile,
                                    workSheetName,                                                
                                    dtExcel,
                                    ref nResult,
                                    workBookActions: workBookActions,
                                    worksheetAction: worksheetAction,
                                    maxRowInExcelWorkSheet: maxRowInExcelWorkSheet,
                                    startingWSCell: startingWSCell,
                                    useDefaultView: useDefaultView,
                                    appendToWorkSheet: appendToWorkSheet,
                                    clearWorkSheet: clearWorkSheet,
                                    cachePackage: cachePackage,
                                    saveWorkSheet: saveWorkSheet,
                                    excelTemplateFile: excelTemplateFile);                

                workBookActions?.Invoke(WorkBookProcessingStage.PostProcess, orgTargetFile, null, workSheetName, null, dtExcel, dtExcel.Rows.Count, null);
                return dtExcel.Rows.Count;
            }

            workBookActions?.Invoke(WorkBookProcessingStage.PreProcessDataTable, orgTargetFile, null, workSheetName, null, dtExcel, -1, null);

            var dtSplits = dtExcel.SplitTable(maxRowInExcelWorkBook, useDefaultView);            
            long totalRows = 0;

            if (dtSplits.Count() == 1)
            {
                excelTargetFile.FileNameFormat = string.Format("{0}-{{0}}{1}",
                                                                        excelTargetFile.Name,
                                                                        string.IsNullOrEmpty(Properties.Settings.Default.ExcelWorkBookFileExtension)
                                                                            ? excelTargetFile.FileExtension
                                                                            : Properties.Settings.Default.ExcelWorkBookFileExtension);
            }
            else
            {
                excelTargetFile.FileNameFormat = string.Format("{0}-{{0}}-{{1:000}}{1}",
                                                                excelTargetFile.Name,
                                                                string.IsNullOrEmpty(Properties.Settings.Default.ExcelWorkBookFileExtension)
                                                                            ? excelTargetFile.FileExtension
                                                                            : Properties.Settings.Default.ExcelWorkBookFileExtension);
                cachePackage = false;
                saveWorkSheet = true;
            }

            Parallel.ForEach(dtSplits, dtSplit =>
            //foreach (var dtSplit in dtSplits)
            {
                var excelFile = generateNewFileName
                                        ? ((IFilePath)excelTargetFile.Clone())
                                                .ApplyFileNameFormat(new object[] { workSheetName, System.Threading.Interlocked.Increment(ref totalRows) })
                                         : excelTargetFile;

                workBookActions?.Invoke(WorkBookProcessingStage.PrepareFileName, orgTargetFile, excelFile, workSheetName, null, dtSplit, -1, null);

                WorkBookWorkSheet(orgTargetFile,
                                    excelFile,
                                    workSheetName,
                                    dtSplit,
                                    ref nResult,
                                    workBookActions: workBookActions,
                                    worksheetAction: worksheetAction,
                                    maxRowInExcelWorkSheet: maxRowInExcelWorkSheet,
                                    startingWSCell: startingWSCell,
                                    useDefaultView: false,
                                    appendToWorkSheet: appendToWorkSheet,
                                    clearWorkSheet: clearWorkSheet,
                                    cachePackage: cachePackage,
                                    saveWorkSheet: saveWorkSheet,
                                    excelTemplateFile: excelTemplateFile);        
            });

            workBookActions?.Invoke(WorkBookProcessingStage.PostProcess, orgTargetFile, null, workSheetName, null, null, nResult, null);

            return nResult;
        }

        static public int SaveCloseAllWorkBooks()
        {
            return ExcelPkgCache.SaveAllExcelFiles(true);
        }

        static public int WorkSheetLoadColumnDefaults(this ExcelWorksheet workSheet,
                                                        string column,
                                                        string[] defaultValues)
        {
            int nbrLoaded = 0;
            for (int emptyRow = workSheet.Dimension.End.Row + 1, posValue = 0; posValue < defaultValues.Length; ++emptyRow, ++posValue)
            {
                workSheet.Cells[string.Format("{0}{1}", column, emptyRow)].Value = defaultValues[posValue];
                ++nbrLoaded;
            }
            return nbrLoaded;
        }

        static public int WorkSheetLoadColumnDefaults(this ExcelWorksheet workSheet,
                                                        string column,
                                                        int startRow,
                                                        string[] defaultValues)
        {
            int nbrLoaded = 0;
            for (int emptyRow = startRow, posValue = 0; posValue < defaultValues.Length; ++emptyRow, ++posValue)
            {
                workSheet.Cells[string.Format("{0}{1}", column, emptyRow)].Value = defaultValues[posValue];
                ++nbrLoaded;
            }
            return nbrLoaded;
        }

        static public int WorkSheetLoadColumnDefaults(this ExcelPackage excelPkg,
                                                        string workSheetName,
                                                        string column,
                                                        int startRow,
                                                        string[] defaultValues)
        {
            var workSheet = excelPkg.Workbook.Worksheets[workSheetName];
            int nbrLoaded = 0;
            if (workSheet == null)
            {
                workSheet = excelPkg.Workbook.Worksheets.Add(workSheetName);
            }
            else
            {
                workSheet.Cells[string.Format("{0}:{1}", startRow, workSheet.Dimension.End.Row)].Clear();
            }

            for (int emptyRow = startRow, posValue = 0; posValue < defaultValues.Length; ++emptyRow, ++posValue)
            {
                workSheet.Cells[string.Format("{0}{1}", column, emptyRow)].Value = defaultValues[posValue];
                ++nbrLoaded;
            }
            return nbrLoaded;
        }

        static public int WorkSheetLoadColumnDefaults(this ExcelWorksheet workSheet, WorkSheetColAttrDefaults defaultAttrs)
        {
            if (string.IsNullOrEmpty(defaultAttrs.Column) || defaultAttrs.Attrs == null || defaultAttrs.Attrs.Length == 0)
            {
                return 0;
            }

            return WorkSheetLoadColumnDefaults(workSheet, defaultAttrs.Column, defaultAttrs.Attrs);           
        }

        static public int WorkSheetLoadColumnDefaults(this ExcelWorksheet workSheet, WorkSheetColAttrDefaults defaultAttrs, int startRow)
        {
            if (string.IsNullOrEmpty(defaultAttrs.Column) || defaultAttrs.Attrs == null || defaultAttrs.Attrs.Length == 0)
            {
                return 0;
            }

            return WorkSheetLoadColumnDefaults(workSheet, defaultAttrs.Column, startRow, defaultAttrs.Attrs);            
        }

        static public ExcelRange TranslaateToExcelRange(this ExcelWorksheet excelWorksheet, DataTable dataTable, string dtColumnName, int excelEndRow = -1, int excelStartRow = 1)
        {
            var dtCol = dataTable.Columns[dtColumnName];

            if(excelEndRow <= 0)
            {
                excelEndRow = dataTable.Rows.Count;
            }

            return excelWorksheet.Cells[excelStartRow, dtCol.Ordinal + 1, excelEndRow, dtCol.Ordinal + 1];
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="excelWorksheet"></param>
        /// <param name="dataTable"></param>
        /// <param name="dtColumnName1"></param>
        /// <param name="dtColumnName2"></param>
        /// <param name="excelRow">
        /// if -1 (default), returns only an Excel Range from dtColumnName1 to dtColumnName2 
        /// otherwise if depends on the value of excelEndRow
        /// </param>
        /// <param name="excelEndRow">
        /// Only if excelRow &gt; 0
        /// if excelEndRow equals -1 (default); returns Excel Range from dtColumnName1 excelRow to dtColumnName2 excelRow
        /// if excelEndRow equals 0; returns Excel Range from dtColumnName1 excelRow to dtColumnName2 &lt;last row in datatable&gt;
        /// if excelEndRow excelRow &gt; 0; returns Excel Range from dtColumnName1 excelRow to dtColumnName2 excelEndRow
        /// </param>
        /// <returns></returns>
        static public ExcelRange TranslaateToColumnRange(this ExcelWorksheet excelWorksheet, DataTable dataTable, string dtColumnName1, string dtColumnName2, int excelRow = -1, int excelEndRow = -1)
        {
            var dtCol1 = dataTable.Columns[dtColumnName1];
            var dtCol2 = dataTable.Columns[dtColumnName2];

            if (excelRow > 0)
            {
                if (excelEndRow == 0)
                {
                    excelEndRow = dataTable.Rows.Count;  
                }

                if(excelEndRow <= 0)
                    return excelWorksheet.Cells[excelRow, dtCol1.Ordinal + 1, excelRow, dtCol2.Ordinal + 1];

                return excelWorksheet.Cells[excelRow, dtCol1.Ordinal + 1, excelEndRow, dtCol2.Ordinal + 1];
            }

            return excelWorksheet.Cells[string.Format("{0}:{1}",
                                                        DataTableHelpers.ExcelTranslateColumnFromColOrdinaltoLetter(dtCol1.Ordinal),
                                                        DataTableHelpers.ExcelTranslateColumnFromColOrdinaltoLetter(dtCol2.Ordinal))];
        }

    }
}

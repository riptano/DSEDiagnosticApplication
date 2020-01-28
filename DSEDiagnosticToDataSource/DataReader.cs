using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using System.Linq;
using Common;

namespace DSEDiagnosticToDataSource
{
    /// <summary>
    /// Class DataReader
    /// </summary>
    public sealed class  DataReader : IEnumerable<DataReader>, System.Data.IDataReader
    {
        private readonly DataTable  _reader;
        private readonly DataTable _schemaDT;
        private readonly IEnumerator _rowEnumerator;
        private DataRow _currentRow;
        private bool _isClosed = true;
        private long _rowReadNbr = -1;
        private int _currentRowColOffset = 0;
        private readonly string _diagnosticId;
        public const string PKHashCode = "HashCode";
        public const string RowID = "RowId";
        public readonly int _colordinalPKHashCode = -1;
        public readonly int _colordinalRowId = -1;

        public DataReader(DataTable reader, string diagnosticId = null)
        {
            bool rowidColAdded = false;

            this._reader = reader;
            this._rowEnumerator = this._reader.Rows.GetEnumerator();
            this._currentRow = (DataRow)this._rowEnumerator.Current;
            this._rowReadNbr = 0;
            this._diagnosticId = diagnosticId;

            if (this._diagnosticId == null)
            {
                this._schemaDT = this._reader;
            }
            else
            {
                this._schemaDT = new DataTable(this._reader.TableName, this._reader.Namespace);
                
                DataColumn copyDC = new DataColumn("DiagnosticId", typeof(string));
                var primaryKeys = new List<DataColumn>() { copyDC };

                copyDC.AllowDBNull = false;
                copyDC.Caption = "DiagnosticId";
                copyDC.DefaultValue = this._diagnosticId;
                copyDC.MaxLength = 115;
                ++this._currentRowColOffset;
                this._schemaDT.Columns.Add(copyDC);

                if ((this._reader.PrimaryKey == null || this._reader.PrimaryKey.Length == 0)
                    && !this._reader.Columns.Contains(RowID))
                {
                    copyDC = new DataColumn(RowID, typeof(long));
                    primaryKeys.Add(copyDC);

                    copyDC.AllowDBNull = false;                    
                    ++this._currentRowColOffset;
                    this._schemaDT.Columns.Add(copyDC);
                    rowidColAdded = true;
                }

                foreach (DataColumn column in this._reader.Columns)
                {
                    copyDC = new DataColumn(column.ColumnName, column.DataType);
                    copyDC.AllowDBNull = column.AllowDBNull;
                    copyDC.Caption = column.Caption;
                    copyDC.DefaultValue = column.DefaultValue;
                    copyDC.MaxLength = column.MaxLength;

                    if (this._reader.PrimaryKey != null && this._reader.PrimaryKey.Contains(column))
                        primaryKeys.Add(copyDC);

                    this._schemaDT.Columns.Add(copyDC);
                }

                if(primaryKeys.Count > 1)
                    this._schemaDT.PrimaryKey = primaryKeys.ToArray();
            }

            this._colordinalPKHashCode = this._schemaDT.Columns.IndexOf(PKHashCode);
            if(!rowidColAdded)
                this._colordinalRowId = this._schemaDT.Columns.IndexOf(RowID);

            this._isClosed = false;
        }

    #region System.Data.IDataReader Members

        /// <summary>
        /// Gets a value that indicates whether this DataReader
        ///     contains one or more rows.
        /// </summary>
        public bool HasRows
        {
            get { return this._reader.Rows.Count > 0; }
        }
       
        /// <summary>
        ///  Gets a value indicating the depth of nesting for the current row.
        /// </summary>
        public int Depth { get { return 0; } }
            
        /// <summary>
        /// Gets a value indicating whether the DataReader is closed.
        /// </summary>
        /// <exception cref="System.InvalidOperationException">The DataReader is closed.</exception>
        public bool IsClosed { get { return this._isClosed; } }
        
        /// <summary>
        /// Gets the number of rows changed, inserted, or deleted by execution of the
        ///     SQL statement.
        ///     
        /// Note -1 for SELECT statements;
        ///     0 if no rows were affected or the statement failed.
        /// </summary>
        public int RecordsAffected { get { return -1; } }
       
        /// <summary>
        /// Gets the number of fields in the DataReader that are not hidden.
        /// </summary>
        public int VisibleFieldCount
        {
            get
            {
                return this._schemaDT.Columns.Count;
            }
        }
                
        /// <summary>
        /// Closes the DataReader Object.
        /// </summary>
        public void Close()
        {
            this._isClosed = true;

            if (this._rowEnumerator != null)
            {
                this._rowEnumerator.Reset();
                this._currentRow = (DataRow) this._rowEnumerator.Current;
                this._rowReadNbr = 0;
            }
        }
        
        /// <summary>
        ///  Returns a System.Data.DataTable that describes the column meta-data of the
        ///     DataReader.
        /// </summary>
        /// <returns>A System.Data.DataTable that describes the column meta-data.</returns>
        /// <exception cref="System.InvalidOperationException">The DataReader is closed.</exception>
        public System.Data.DataTable GetSchemaTable()
        {
            return this._schemaDT;
        }
        
        /// <summary>
        /// Advances the data reader to the next result, when reading the results of
        ///     batch SQL statements.
        /// </summary>
        /// <returns>true if there are more rows; otherwise, false.</returns>
        public bool NextResult()
        {
            return false;
        }
        
        /// <summary>
        /// Advances the DataReader to the next record.
        /// </summary>
        /// <returns>true if there are more rows; otherwise, false.</returns>
        public bool Read()
        {
            if(this._rowEnumerator.MoveNext())
            {
                this._currentRow = (DataRow)this._rowEnumerator.Current;
                ++this._rowReadNbr;
                return true;
            }
            return false;
        }

    #endregion //end of System.Data.IDataReader Members

    #region System.Data.IDataRecord 
        
        /// <summary>
        /// Gets the number of columns in the current row.
        /// </summary>
        /// <exception cref="System.NotSupportedException">There is no current connection to an instance of SQL Server.</exception>
        public int FieldCount
        {
            get
            {               
                return this._currentRow?.ItemArray.Length + this._currentRowColOffset ?? 0;
            }
        }

        /// <summary>
        /// Gets the value of the specified column as an instance of System.Object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        /// <exception cref="System.IndexOutOfRangeException">The index passed was outside the range of 0 through FieldCount</exception>
        public object this[int ordinal] { get { return this.GetValue(ordinal); } }

        /// <summary>
        /// Gets the value of the specified column as an instance of System.Object.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>The value of the specified column.</returns>
        /// <exception cref="System.IndexOutOfRangeException">No column with the specified name was found.</exception>
        public object this[string name] { get { return this.GetValue(this.GetOrdinal(name)); } }
       
        /// <summary>
        /// Gets the value of the specified column as a Boolean.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The value of the column.</returns>
        /// <exception cref="System.IndexOutOfRangeException">The index passed was outside the range of 0 through System.Data.IDataRecord.FieldCount.</exception>
        public bool GetBoolean(int i) { return (bool)this.GetValue(i);  }
        
        /// <summary>
        /// Gets the 8-bit unsigned integer value of the specified column.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The 8-bit unsigned integer value of the specified column.</returns>
        /// <exception cref="System.IndexOutOfRangeException">The index passed was outside the range of 0 through System.Data.IDataRecord.FieldCount.</exception>
        public byte GetByte(int i) { return (byte)this.GetValue(i); }
        
        /// <summary>
        ///  Reads a stream of bytes from the specified column offset into the buffer
        ///     as an array, starting at the given buffer offset.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <param name="fieldOffSet">The index within the field from which to start the read operation.</param>
        /// <param name="buffer">The buffer into which to read the stream of bytes.</param>
        /// <param name="bufferOffSet">The index for buffer to start the read operation.</param>
        /// <param name="length">The number of bytes to read.</param>
        /// <returns>The actual number of bytes read.</returns>
        /// <exception cref="System.IndexOutOfRangeException">The index passed was outside the range of 0 through System.Data.IDataRecord.FieldCount.</exception>
        public long GetBytes(int i, long fieldOffSet, byte[] buffer, int bufferOffSet, int length)
        {           
            var value = this.GetValue(i);

            if(value is string strValue)
            {
                var charArray = strValue.ToCharArray();

                long copiedBytes = 0;
                long valueIndx = fieldOffSet;
                long lngLength = length;

                for (int destIdx = bufferOffSet;
                        copiedBytes <= lngLength && destIdx < buffer.Length && valueIndx < charArray.LongLength;
                        ++destIdx, ++valueIndx)
                {
                    buffer[destIdx] = (byte) charArray[valueIndx];
                    ++copiedBytes;
                }

                return copiedBytes;
            }
            if(value is byte[] byteArray)
            {                
                long copiedBytes = 0;
                long valueIndx = fieldOffSet;
                long lngLength = length;

                for (int destIdx = bufferOffSet;
                        copiedBytes <= lngLength && destIdx < buffer.Length && valueIndx < byteArray.LongLength;
                        ++destIdx, ++valueIndx)
                {
                    buffer[destIdx] = byteArray[valueIndx];
                    ++copiedBytes;
                }

                return copiedBytes;
            }

            throw new ArgumentException(string.Format("Byte Array \"{0}\" is not a proper value for Column \"{1}\"",
                                                        value,
                                                        this.GetName(i)));
        }
        
        /// <summary>
        /// Gets the character value of the specified column.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The character value of the specified column.</returns>
        /// <exception cref="System.IndexOutOfRangeException">The index passed was outside the range of 0 through System.Data.IDataRecord.FieldCount.</exception>
        public char GetChar(int i) { return (char)this.GetValue(i); }
        
        /// <summary>
        /// Reads a stream of characters from the specified column offset into the buffer
        ///     as an array, starting at the given buffer offset.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <param name="fieldOffSet">The index within the row from which to start the read operation.</param>
        /// <param name="buffer">The buffer into which to read the stream of bytes.</param>
        /// <param name="bufferOffSet">The index for buffer to start the read operation.</param>
        /// <param name="length">The number of bytes to read.</param>
        /// <returns>The actual number of characters read.</returns>
        /// <exception cref="System.IndexOutOfRangeException">The index passed was outside the range of 0 through System.Data.IDataRecord.FieldCount.</exception>
        public long GetChars(int i, long fieldOffSet, char[] buffer, int bufferOffSet, int length)
        {
            var value = this.GetValue(i);

            if (value is string strValue)
            {
                var charArray = strValue.ToCharArray();

                long copiedBytes = 0;
                long valueIndx = fieldOffSet;
                long lngLength = length;

                for (int destIdx = bufferOffSet;
                        copiedBytes <= lngLength && destIdx < buffer.Length && valueIndx < charArray.LongLength;
                        ++destIdx, ++valueIndx)
                {
                    buffer[destIdx] = charArray[valueIndx];
                    ++copiedBytes;
                }

                return copiedBytes;
            }
            if (value is byte[] byteArray)
            {
                long copiedBytes = 0;
                long valueIndx = fieldOffSet;
                long lngLength = length;

                for (int destIdx = bufferOffSet;
                        copiedBytes <= lngLength && destIdx < buffer.Length && valueIndx < byteArray.LongLength;
                        ++destIdx, ++valueIndx)
                {
                    buffer[destIdx] = (char) byteArray[valueIndx];
                    ++copiedBytes;
                }

                return copiedBytes;
            }

            throw new ArgumentException(string.Format("Char Array Value \"{0}\" is not a proper value for Column \"{1}\"",
                                                        value,
                                                        this.GetName(i)));
        }
        
        /// <summary>
        /// Returns an System.Data.IDataReader for the specified column ordinal.
        /// </summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>An System.Data.IDataReader.</returns>
        /// <exception cref="System.IndexOutOfRangeException">The index passed was outside the range of 0 through System.Data.IDataRecord.FieldCount.</exception>
        [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
        public System.Data.IDataReader GetData(int i)
        { throw new NotImplementedException(); } 
       
        /// <summary>
        /// Gets the data type information for the specified field.
        /// </summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>The data type information for the specified field.</returns>
        /// <exception cref="System.IndexOutOfRangeException">The index passed was outside the range of 0 through System.Data.IDataRecord.FieldCount.</exception>
        public string GetDataTypeName(int i) { return this.GetFieldType(i).Name; }
        
        /// <summary>
        ///  Gets the date and time data value of the specified field.
        /// </summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>The date and time data value of the specified field.</returns>
        /// <exception cref="System.IndexOutOfRangeException">The index passed was outside the range of 0 through System.Data.IDataRecord.FieldCount.</exception>
        public DateTime GetDateTime(int i) { return (DateTime)this.GetValue(i); }
       
        /// <summary>
        /// Gets the fixed-position numeric value of the specified field.
        /// </summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>The fixed-position numeric value of the specified field.</returns>
        /// <exception cref="System.IndexOutOfRangeException">The index passed was outside the range of 0 through System.Data.IDataRecord.FieldCount.</exception>
        public decimal GetDecimal(int i) { return (decimal)this.GetValue(i); }
      
        /// <summary>
        /// Gets the double-precision floating point number of the specified field.
        /// </summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>The double-precision floating point number of the specified field.</returns>
        /// <exception cref="System.IndexOutOfRangeException">The index passed was outside the range of 0 through System.Data.IDataRecord.FieldCount.</exception>
        public double GetDouble(int i) { return (double)this.GetValue(i); }
        
        /// <summary>
        /// Gets the System.Type information corresponding to the type of System.Object
        ///     that would be returned from System.Data.IDataRecord.GetValue(System.Int32).
        /// </summary>
        /// <param name="i"> The index of the field to find.</param>
        /// <returns>
        /// The System.Type information corresponding to the type of System.Object that
        ///     would be returned from System.Data.IDataRecord.GetValue(System.Int32).
        ///  </returns>
        /// <exception cref="System.IndexOutOfRangeException">The index passed was outside the range of 0 through System.Data.IDataRecord.FieldCount.</exception>
        public Type GetFieldType(int i)
        {
            return this._schemaDT.Columns[i].DataType;
        }
        
        /// <summary>
        /// Gets the single-precision floating point number of the specified field.
        /// </summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>The single-precision floating point number of the specified field.</returns>
        /// <exception cref="System.IndexOutOfRangeException">The index passed was outside the range of 0 through System.Data.IDataRecord.FieldCount.</exception>
        public float GetFloat(int i) { return (float)this.GetValue(i); }
        
        /// <summary>
        /// Returns the GUID value of the specified field.
        /// </summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>The GUID value of the specified field.</returns>
        /// <exception cref="System.IndexOutOfRangeException">The index passed was outside the range of 0 through System.Data.IDataRecord.FieldCount.</exception>
        public Guid GetGuid(int i) { return (Guid)this.GetValue(i); }
        
        /// <summary>
        /// Gets the 16-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>The 16-bit signed integer value of the specified field.</returns>
        /// <exception cref="System.IndexOutOfRangeException">The index passed was outside the range of 0 through System.Data.IDataRecord.FieldCount.</exception>
        public short GetInt16(int i) { return (Int16)this.GetValue(i); }
        
        /// <summary>
        /// Gets the 32-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>The 32-bit signed integer value of the specified field.</returns>
        /// <exception cref="System.IndexOutOfRangeException">The index passed was outside the range of 0 through System.Data.IDataRecord.FieldCount.</exception>
        public int GetInt32(int i) { return (Int32)this.GetValue(i); }
        
        /// <summary>
        /// Gets the 64-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>The 64-bit signed integer value of the specified field.</returns>
        /// <exception cref="System.IndexOutOfRangeException">The index passed was outside the range of 0 through System.Data.IDataRecord.FieldCount.</exception>
        public long GetInt64(int i) { return (Int64)this.GetValue(i); }
        
        /// <summary>
        /// Gets the name for the field to find.
        /// </summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>The name of the field or the empty string (""), if there is no value to return.</returns>
        /// <exception cref="System.IndexOutOfRangeException">The index passed was outside the range of 0 through System.Data.IDataRecord.FieldCount.</exception>
        public string GetName(int i)
        {
            return this._schemaDT.Columns[i].ColumnName;
        }
        
        /// <summary>
        /// Return the index of the named field.
        /// </summary>
        /// <param name="name">The name of the field to find.</param>
        /// <returns>The index of the named field.</returns>
        public int GetOrdinal(string name)
        {
            return this._schemaDT.Columns[name].Ordinal;
        }
        
        /// <summary>
        /// Gets the string value of the specified field.
        /// </summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>The string value of the specified field..</returns>
        /// <exception cref="System.IndexOutOfRangeException">The index passed was outside the range of 0 through System.Data.IDataRecord.FieldCount.</exception>
        public string GetString(int i) { return (string)this.GetValue(i); }
        
        /// <summary>
        /// Return the value of the specified field.
        /// </summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>he System.Object which will contain the field value upon return.</returns>
        /// <exception cref="System.IndexOutOfRangeException">The index passed was outside the range of 0 through System.Data.IDataRecord.FieldCount.</exception>
        public object GetValue(int i)
        {
            object value;

            if (this._diagnosticId == null)
            {
                value = this._currentRow.ItemArray[i];
            }
            else
            {
                if (i == 0 && this._currentRowColOffset > 0)
                    return this._diagnosticId;               
                else if (i == 1 && this._currentRowColOffset > 1)
                    return this._rowReadNbr;
                else
                {
                    value = this._currentRow.ItemArray[i - this._currentRowColOffset];
                }
            }

            if (i == this._colordinalPKHashCode && value is int hashCode && hashCode == 0)
                return this._currentRow.GetHashCode();
            else if (i == this._colordinalRowId && value is long rowId && rowId < 0)
                return this._rowReadNbr;
            else if (value is TimeSpan timeSpan)
                return timeSpan.Ticks;            

            return value;
        }
        

        /// <summary>
        /// Populates an array of objects with the column values of the current record.
        /// </summary>
        /// <param name="values">An array of System.Object to copy the attribute fields into.</param>
        /// <returns>The number of instances of System.Object in the array.</returns>
        public int GetValues(object[] values)
        {
            int numberOfCopiedValues = Math.Min(this._currentRow.ItemArray.Length, values.Length);
            object value;

            for (int i = 0; i < numberOfCopiedValues; i++)
            {
                if (this._diagnosticId == null)
                {
                    value = this._currentRow.ItemArray[i];
                }
                else
                {
                    if (i == 0 && this._currentRowColOffset > 0)
                    {
                        value = this._diagnosticId;
                    }
                    else if (i == 1 && this._currentRowColOffset > 1)
                    {
                        value = this._rowReadNbr;
                    }
                    else
                    {
                        value = this._currentRow.ItemArray[i - this._currentRowColOffset];
                    }
                }

                if (i == this._colordinalPKHashCode && value is int hashCode && hashCode == 0)
                    value = this._currentRow.GetHashCode();
                else if (i == this._colordinalRowId && value is long rowId && rowId < 0)
                    value = this._rowReadNbr;
                else if (value is TimeSpan timeSpan)
                {
                    values[i] = timeSpan.Ticks;
                }
                else
                    values[i] = value;
            }

            return numberOfCopiedValues;
        }
        
        /// <summary>
        /// Return whether the specified field is set to DB Null.
        /// </summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>true if the specified field is set to null; otherwise, false.</returns>
        /// <exception cref="System.IndexOutOfRangeException">The index passed was outside the range of 0 through System.Data.IDataRecord.FieldCount.</exception>
        public bool IsDBNull(int i)
        {
            var value = this.GetValue(i);

            if (value == DBNull.Value) return true;

            return false;
        }

        #endregion //end of System.Data.IDataRecord

        #region IEnumerable Members 

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return (new List<DataReader>() { this }).GetEnumerator();
        }

        public System.Collections.Generic.IEnumerator<DataReader> GetEnumerator()
        {
            return (new List<DataReader>() { this }).GetEnumerator();
        }
       
    #endregion //end of IEnumerator Members

    #region IDisposableProperty Members

        #region Dispose Methods

        public bool IsDisposed { get; private set; }
                
        public void Dispose()
        {
        	Dispose(true);
        	// This object will be cleaned up by the Dispose method.
        	// Therefore, you should call GC.SupressFinalize to
        	// take this object off the finalization queue
        	// and prevent finalization code for this object
        	// from executing a second time.
        	GC.SuppressFinalize(this);
        }
                
        private void Dispose(bool disposing)
        {
        	// Check to see if Dispose has already been called.
        	if(!this.IsDisposed)
        	{
        		
        		if(disposing)
        		{
        			// Dispose all managed resources.
                    if (this._reader != null)
                    {
                        this._isClosed = true;
                        this._currentRow = null;
                        this._rowReadNbr = -1;
                    }
        		}
        
        		//Dispose of all unmanaged resources
        
        		// Note disposing has been done.
        		this.IsDisposed = true;
        
        	}
        }
        #endregion //end of Dispose Methods


    #endregion //end of IDisposableProperty Members
       
        /// <summary>
        /// Returns the provider-specific field type of the specified column.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The System.Type object that describes the data type of the specified column.</returns>
        [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
        public Type GetProviderSpecificFieldType(int ordinal)
        { return this.GetFieldType(ordinal); }
       
        /// <summary>
        /// Gets the value of the specified column as an instance of System.Object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
        public object GetProviderSpecificValue(int ordinal)
        { return this.GetValue(ordinal); }
        
        /// <summary>
        /// Gets all provider-specific attribute columns in the collection for the current
        ///     row.
        /// </summary>
        /// <param name="values">An array of System.Object into which to copy the attribute columns.</param>
        /// <returns>The number of instances of System.Object in the array.</returns>
        [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
        public int GetProviderSpecificValues(object[] values)
        { return this.GetValues(values); }

        /// <summary>
        /// Returns the Column's Ordinal Index.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Ordinal Index</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public int GetColumn(string name)
        {
            return this.GetOrdinal(name);            
        }

        /// <summary>
        /// Returns the Column's Value.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public T GetValue<T>(string name)
        {
            return (T)this.GetValue(this.GetColumn(name));
        }

        /// <summary>
        /// Returns the Column's Value.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public bool GetBoolean(string name)
        {
            var iCol = this.GetColumn(name);
            return this.GetBoolean(iCol);
        }

        /// <summary>
        /// Returns the Column's Value.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
		/// <param name="defaultValue">If true and if the column is DBNull, this value is returned</param>
        /// <returns>The Column's Value</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public bool GetBoolean(string name, bool defaultValue)
        {
            var iCol = this.GetColumn(name);

            if (!this.IsDBNull(iCol))
            {
                return this.GetBoolean(iCol);
            }

            return defaultValue;
        }

        /// <summary>
        /// Returns the Column's Value or null.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value or null to indicate that it is DB Null.</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public bool? GetBooleanNullable(string name)
        {
            var iCol = this.GetColumn(name);
            if (!this.IsDBNull(iCol))
            {
                return this.GetBoolean(iCol);
            }

            return null;
        }

        /// <summary>
        /// Returns the Column's Value.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public string GetString(string name)
        {
            var iCol = this.GetColumn(name);
            return this.GetString(iCol);
        }

        /// <summary>
        /// Returns the Column's Value or String.Empty.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value or String.Empty to indicate that it is DB Null</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public string GetStringNullable(string name)
        {
            var iCol = this.GetColumn(name);
            if (!this.IsDBNull(iCol))
            {
                return this.GetString(iCol);
            }
            return string.Empty;
        }

        /// <summary>
        /// Returns the Column's Value.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public int GetInt(string name)
        {
            var iCol = this.GetColumn(name);
            return this.GetInt32(iCol);
        }

        /// <summary>
        /// Returns the Column's Value or a Default Value if DB Null.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
		/// <param name="defaultValue">If true and if the column is DBNull, this value is returned</param>
        /// <returns>The Column's Value or a Default Value if DB Null</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public int GetInt(string name, int defaultValue)
        {
            var iCol = this.GetColumn(name);

            if (!this.IsDBNull(iCol))
            {
                return this.GetInt32(iCol);
            }

            return defaultValue;
        }

        /// <summary>
        /// Returns the Column's Value or null
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value or null in the case it is DB Null</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public int? GetIntNullable(string name)
        {
            var iCol = this.GetColumn(name);
            if (!this.IsDBNull(iCol))
            {
                return this.GetInt32(iCol);
            }
            return null;
        }

        /// <summary>
        /// Returns the Column's Value.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public decimal GetDecimal(string name)
        {
            var iCol = this.GetColumn(name);
            return this.GetDecimal(iCol);
        }

        /// <summary>
        /// Returns the Column's Value or a Default Value if DB Null.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
		/// <param name="defaultValue">If true and if the column is DBNull, this value is returned</param>
        /// <returns>The Column's Value or a Default Value if DB Null</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public decimal GetDecimal(string name, decimal defaultValue)
        {
            var iCol = this.GetColumn(name);

            if (!this.IsDBNull(iCol))
            {
                return this.GetDecimal(iCol);
            }

            return defaultValue;
        }

        /// <summary>
        /// Returns the Column's Value or null
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value or null in the case it is DB Null</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public decimal? GetDecimalNullable(string name)
        {
            var iCol = this.GetColumn(name);
            if (!this.IsDBNull(iCol))
            {
                return this.GetDecimal(iCol);
            }
            return null;
        }

        /// <summary>
        /// Returns the Column's Value.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public Guid GetGuid(string name)
        {
            var iCol = this.GetColumn(name);
            return this.GetGuid(iCol);
        }

        /// <summary>
        /// Returns the Column's Value.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public DateTime GetDateTime(string name)
        {
            var icol = this.GetColumn(name);
            return this.GetDateTime(icol);
        }

        /// <summary>
        /// Returns the Column's Value or null
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value or null in the case it is DB Null</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public DateTime? GetDateTimeNullable(string name)
        {
            var iCol = this.GetColumn(name);
            if (!this.IsDBNull(iCol))
            {
                return this.GetDateTime(iCol);
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// Returns the Column's Value or a Default Value if DB Null.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
		/// <param name="defaultValue">If true and if the column is DBNull, this value is returned</param>
        /// <returns>The Column's Value or a Default Value if DB Null</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public DateTime GetDateTime(string name, DateTime defaultValue)
        {
            var iCol = this.GetColumn(name);

            if (!this.IsDBNull(iCol))
            {
                return this.GetDateTime(iCol);
            }

            return defaultValue;
        }

        /// <summary>
        /// Returns the Column's Value.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public long GetLong(string name)
        {
            var iCol = this.GetColumn(name);
            return this.GetInt64(iCol);
        }

        /// <summary>
        /// Returns the Column's Value or a Default Value if DB Null.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
		/// <param name="defaultValue">If true and if the column is DBNull, this value is returned</param>
        /// <returns>The Column's Value or a Default Value if DB Null</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public long GetLong(string name, long defaultValue)
        {
            var iCol = this.GetColumn(name);

            if (!this.IsDBNull(iCol))
            {
                return this.GetInt64(iCol);
            }

            return defaultValue;
        }

        /// <summary>
        /// Returns the Column's Value or null
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value or null in the case it is DB Null</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public long? GetLongNullable(string name)
        {
            var iCol = this.GetColumn(name);
            if (!this.IsDBNull(iCol))
            {
                return this.GetInt64(iCol);
            }
            return null;
        }

        /// <summary>
        /// Returns the Column's Value.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public short GetShort(string name)
        {
            var iCol = this.GetColumn(name);
            return this.GetInt16(iCol);
        }

        /// <summary>
        /// Returns the Column's Value or a Default Value if DB Null.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
		/// <param name="defaultValue">If true and if the column is DBNull, this value is returned</param>
        /// <returns>The Column's Value or a Default Value if DB Null</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public short GetShort(string name, short defaultValue)
        {
            var iCol = this.GetColumn(name);

            if (!this.IsDBNull(iCol))
            {
                return this.GetInt16(iCol);
            }

            return defaultValue;
        }

        /// <summary>
        /// Returns the Column's Value or null
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value or null in the case it is DB Null</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public short? GetShortNullable(string name)
        {
            var iCol = this.GetColumn(name);
            if (!this.IsDBNull(iCol))
            {
                return this.GetInt16(iCol);
            }
            return null;
        }

        /// <summary>
        /// Returns the Column's Value.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public float GetFloat(string name)
        {
            var iCol = this.GetColumn(name);
            return this.GetFloat(iCol);
        }

        /// <summary>
        /// Returns the Column's Value or a Default Value if DB Null.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
		/// <param name="defaultValue">If true and if the column is DBNull, this value is returned</param>
        /// <returns>The Column's Value or a Default Value if DB Null</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public float GetFloat(string name, float defaultValue)
        {
            var iCol = this.GetColumn(name);

            if (!this.IsDBNull(iCol))
            {
                return this.GetFloat(iCol);
            }

            return defaultValue;
        }

        /// <summary>
        /// Returns the Column's Value or null
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value or null in the case it is DB Null</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public float? GetFloatNullable(string name)
        {
            var iCol = this.GetColumn(name);
            if (!this.IsDBNull(iCol))
            {
                return this.GetFloat(iCol);
            }
            return null;
        }

        /// <summary>
        /// Returns the Column's Value.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public char GetChar(string name)
        {
            var iCol = this.GetColumn(name);
            return this.GetChar(iCol);
        }

        /// <summary>
        /// Returns the Column's Value or a Default Value if DB Null.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
		/// <param name="defaultValue">If true and if the column is DBNull, this value is returned</param>
        /// <returns>The Column's Value or a Default Value if DB Null</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public char GetChar(string name, char defaultValue)
        {
            var iCol = this.GetColumn(name);

            if (!this.IsDBNull(iCol))
            {
                return this.GetChar(iCol);
            }

            return defaultValue;
        }

        /// <summary>
        /// Returns the Column's Value or null
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value or null in the case it is DB Null</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public char? GetCharNullable(string name)
        {
            var iCol = this.GetColumn(name);
            if (!this.IsDBNull(iCol))
            {
                return this.GetChar(iCol);
            }
            return null;
        }

        /// <summary>
        /// Returns the Column's Value.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public byte GetByte(string name)
        {
            var iCol = this.GetColumn(name);
            return this.GetByte(iCol);
        }

        /// <summary>
        /// Returns the Column's Value or a Default Value if DB Null.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
		/// <param name="defaultValue">If true and if the column is DBNull, this value is returned</param>
        /// <returns>The Column's Value or a Default Value if DB Null</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public byte GetByte(string name, byte defaultValue)
        {
            var iCol = this.GetColumn(name);

            if (!this.IsDBNull(iCol))
            {
                return this.GetByte(iCol);
            }

            return defaultValue;
        }

        /// <summary>
        /// Returns the Column's Value or null
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value or null in the case it is DB Null</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public byte? GetByteNullable(string name)
        {
            var iCol = this.GetColumn(name);
            if (!this.IsDBNull(iCol))
            {
                return this.GetByte(iCol);
            }
            return null;
        }

        /// <summary>
        ///  Reads a stream of bytes from the specified column offset into the buffer
        ///     as an array, starting at the given buffer offset.
        /// </summary>
        /// <param name="fieldName">The name of the Column.</param>
        /// <param name="fieldOffSet">The index within the field from which to start the read operation.</param>
        /// <param name="buffer">The buffer into which to read the stream of bytes.</param>
        /// <param name="bufferOffSet">The index for buffer to start the read operation.</param>
        /// <param name="length">The number of bytes to read.</param>
        /// <returns>The actual number of bytes read or -1 to indicate that the value was DB Null</returns>
        /// <exception cref="System.IndexOutOfRangeException">The Column Name does not exists</exception>
        public long GetBytes(string fieldName, long fieldOffSet, byte[] buffer, int bufferOffSet, int length)
        {
            long nbrBytes = -1;
            var iCol = this.GetColumn(fieldName);

            if (!this.IsDBNull(iCol))
            {
                nbrBytes = this.GetBytes(iCol, fieldOffSet, buffer, bufferOffSet, length);
            }

            return nbrBytes;
        }

        /// <summary>
        /// Reads a stream of characters from the specified column offset into the buffer
        ///     as an array, starting at the given buffer offset.
        /// </summary>
        /// <param name="fieldName">The name of the Column</param>
        /// <param name="fieldOffSet">The index within the row from which to start the read operation.</param>
        /// <param name="buffer">The buffer into which to read the stream of bytes.</param>
        /// <param name="bufferOffSet">The index for buffer to start the read operation.</param>
        /// <param name="length">The number of bytes to read.</param>
        /// <returns>The actual number of characters read or -1 to indicate that the field was DB Null.</returns>
        /// <exception cref="System.IndexOutOfRangeException">Column Name was NOT found..</exception>
        public long GetChars(string fieldName, long fieldOffSet, char[] buffer, int bufferOffSet, int length)
        {
            long nbrBytes = -1;
            var iCol = this.GetColumn(fieldName);

            if (!this.IsDBNull(iCol))
            {
                nbrBytes = this.GetChars(iCol, fieldOffSet, buffer, bufferOffSet, length);
            }

            return nbrBytes;
        }

        /// <summary>
        /// Returns an Enum of Type T based on the Column's Value.
        /// 
        /// If the Column's Value is a string an Enum.Parse is used to create the enum otherwise the value is casted to Type T.
        /// </summary>
        /// <typeparam name="T">The Enum Type</typeparam>
        /// <param name="name">The Name of the Column</param>
        /// <param name="ignoreCase">If true (default) and if the Column's Value is a String, the string's case is ignored.</param>
        /// <returns>Enum value of T</returns>
        /// <exception cref="System.IndexOutOfRangeException">The Column does not exits.</exception>
        /// <exception cref="System.ArgumentException">If the Column's Value is a string, the string could NOT be parsed into Enum of Type T.</exception>
        /// <exception cref="System.InvalidCastException">The Column's value was NOT a string, and the value could NOT be casted to Type T.</exception>
        public T GetEnum<T> (string name, bool ignoreCase = true)
            where T : struct
        {            
            return this.GetEnum<T>(this.GetColumn(name));            
        }

        /// <summary>
        /// Returns an Enum of Type T based on the Column's Value.
        /// 
        /// If the Column's Value is a string an Enum.Parse is used to create the enum otherwise the value is casted to Type T.
        /// </summary>
        /// <typeparam name="T">The Enum Type</typeparam>
        /// <param name="name">The Name of the Column</param>
        /// <param name="defaultValue">If the Column's Value is a DB Null or the Value could NOT be parsed/casted, this value is returned.</param>
        /// <param name="ignoreCase">If true (default) and if the Column's Value is a String, the string's case is ignored.</param>
        /// <returns>Enum value of T or the Default Value</returns>
        /// <exception cref="System.IndexOutOfRangeException">The Column does not exits.</exception>        
        public T GetEnum<T>(string name, T defaultValue, bool ignoreCase = true)
            where T : struct
        {
            return this.GetEnum<T>(this.GetColumn(name));            
        }

        /// <summary>
        /// Returns an Enum of Type T based on the Column's Value.
        /// 
        /// If the Column's Value is a string an Enum.Parse is used to create the enum otherwise the value is casted to Type T.
        /// </summary>
        /// <typeparam name="T">The Enum Type</typeparam>
        /// <param name="name">The Name of the Column</param>        
        /// <param name="ignoreCase">If true (default) and if the Column's Value is a String, the string's case is ignored.</param>
        /// <returns>Enum value of T or null. Null is returned if the Column's Value is a DB Null or if the value could NOT be parsed/casted to Enum type T.</returns>
        /// <exception cref="System.IndexOutOfRangeException">The Column does not exits.</exception>
        public T? GetEnumNullable<T>(string name, bool ignoreCase = true)
            where T : struct
        {
            var iCol = this.GetColumn(name);

            if (this.IsDBNull(iCol))
            {
                return null;
            }

            return this.GetEnum<T>(iCol);
        }

        /// <summary>
        /// Returns the Column's Value.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public TimeSpan GetTime(string name)
        {
            var iCol = this.GetColumn(name);
            return this.GetTime(iCol);
        }

        /// <summary>
        /// Returns the Column's Value or a Default Value if DB Null.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
		/// <param name="defaultValue">If true and if the column is DBNull, this value is returned</param>
        /// <returns>The Column's Value or a Default Value if DB Null</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public TimeSpan GetTime(string name, TimeSpan defaultValue)
        {
            var iCol = this.GetColumn(name);

            if (!this.IsDBNull(iCol))
            {
                return this.GetTime(iCol);
            }

            return defaultValue;
        }

        /// <summary>
        /// Returns the Column's Value or null
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value or null in the case it is DB Null</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public TimeSpan? GetTimeNullable(string name)
        {
            var iCol = this.GetColumn(name);
            if (!this.IsDBNull(iCol))
            {
                return this.GetTime(iCol);
            }
            return null;
        }

        /// <summary>
        /// Returns the Column's Value.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public DateTimeOffset GetDateTimeOffset(string name)
        {
            var iCol = this.GetColumn(name);
            return this.GetDateTimeOffset(iCol);
        }

        /// <summary>
        /// Returns the Column's Value or a Default Value if DB Null.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
		/// <param name="defaultValue">If true and if the column is DBNull, this value is returned</param>
        /// <returns>The Column's Value or a Default Value if DB Null</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public DateTimeOffset GetDateTimeOffset(string name, DateTimeOffset defaultValue)
        {
            var iCol = this.GetColumn(name);

            if (!this.IsDBNull(iCol))
            {
                return this.GetDateTimeOffset(iCol);
            }

            return defaultValue;
        }

        /// <summary>
        /// Returns the Column's Value or null
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value or null in the case it is DB Null</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public DateTimeOffset? GetDateTimeOffsetNullable(string name)
        {
            var iCol = this.GetColumn(name);
            if (!this.IsDBNull(iCol))
            {
                return this.GetDateTimeOffset(iCol);
            }
            return null;
        }

        /// <summary>
        /// Returns the Column's Value.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public decimal GetMoney(string name)
        {
            var iCol = this.GetColumn(name);
            return this.GetMoney(iCol);
        }

        /// <summary>
        /// Returns the Column's Value or a Default Value if DB Null.
        /// </summary>
        /// <param name="name">The Name of the Column</param>
		/// <param name="defaultValue">If true and if the column is DBNull, this value is returned</param>
        /// <returns>The Column's Value or a Default Value if DB Null</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public decimal GetMoney(string name, decimal defaultValue)
        {
            var iCol = this.GetColumn(name);

            if (!this.IsDBNull(iCol))
            {
                return this.GetMoney(iCol);
            }

            return defaultValue;
        }

        /// <summary>
        /// Returns the Column's Value or null
        /// </summary>
        /// <param name="name">The Name of the Column</param>
        /// <returns>The Column's Value or null in the case it is DB Null</returns>
        /// <exception cref="System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        public decimal? GetMoneyNullable(string name)
        {
            var iCol = this.GetColumn(name);
            if (!this.IsDBNull(iCol))
            {
                return this.GetMoney(iCol);
            }
            return null;
        }

    #region Enhanced Getters 

        public TimeSpan GetTime(int i)
        {
            var value = this.GetValue(i);

            if(value is TimeSpan tsValue)
            {
                return tsValue;
            }
            else if(value is string strValue)
            {
                if (TimeSpan.TryParse(strValue, out TimeSpan timeSpan))
                    return timeSpan;
            }
            else if (value is long lngValue)
            {
                return new TimeSpan(lngValue);
            }
            else if (value is DateTime dtValue)
            {
                return new TimeSpan(dtValue.Ticks);
            }

            throw new ArgumentException(string.Format("TimeSpan Value \"{0}\" is not a proper value for Column \"{1}\"",
                                                        value,
                                                        this.GetName(i)));
        }

        public DateTimeOffset GetDateTimeOffset(int i)
        {
            var value = this.GetValue(i);

            if (value is DateTimeOffset dtoValue)
            {
                return dtoValue;
            }
            else if (value is string strValue)
            {
                if (DateTimeOffset.TryParse(strValue, out DateTimeOffset datetimeOffset))
                    return datetimeOffset;
            }
            else if (i + 1 < this.FieldCount) //Check next column to determine is it has the offset   
            {
                if (value is long lngValue)
                {
                    var offset = this.GetValue(i + 1);

                    if (offset is long tsValue)
                        return new DateTimeOffset(lngValue, new TimeSpan(tsValue));
                    else if (offset is int intValue)
                        return new DateTimeOffset(lngValue, new TimeSpan(intValue, 0, 0));                
                }
                else if (value is DateTime dtValue)
                {
                    var offset = this.GetValue(i + 1);

                    if (offset is long tsValue)
                        return new DateTimeOffset(dtValue, new TimeSpan(tsValue));
                    else if (offset is int intValue)
                        return new DateTimeOffset(dtValue, new TimeSpan(intValue, 0, 0));                    
                }
            }

            throw new ArgumentException(string.Format("DateTimeOffset Value \"{0}\" is not a proper value for Column \"{1}\"",
                                                        value,
                                                        this.GetName(i)));
        }
        
        public decimal GetMoney(int i)
        {
            return this.GetDecimal(i);
        }


        public T GetEnum<T>(int iCol, bool ignoreCase = true)
            where T : struct
        {
            var value = this.GetValue(iCol);

            if (value is string strValue)
            {
                T enumValue;

                if (System.Enum.TryParse<T>(strValue, ignoreCase, out enumValue))
                {
                    return enumValue;
                }

                throw new ArgumentException(string.Format("Cannot Convert \"{0}\" ({1}'s Value) to Enum Type of {2}.",
                                                            strValue,
                                                            this.GetName(iCol),
                                                            typeof(T).FullName), "name");
            }

            return (T)value;
        }

        public T GetEnum<T>(int iCol, T defaultValue, bool ignoreCase = true)
            where T : struct
        {
            if (this.IsDBNull(iCol))
            {
                return defaultValue;
            }

            var value = this.GetValue(iCol);

            if (value is string strValue)
            {
                T enumValue;

                if (System.Enum.TryParse<T>(strValue, ignoreCase, out enumValue))
                {
                    return enumValue;
                }

                return defaultValue;
            }

            try
            {
                return (T)value;
            }
            catch (System.InvalidCastException)
            { }

            return defaultValue;
        }

        #endregion

    }
}

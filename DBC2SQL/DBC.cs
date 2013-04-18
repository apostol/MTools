using System;
using System.Collections;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;

namespace dbc2sql
{
    public class DBC : IDisposable
    {
        private readonly string _dbcName;
        private readonly FileStream _dbcStream;
        private readonly Hashtable _rowTable = new Hashtable();
        private readonly XmlElement _xmlLayout;

        private DbcHeader _dbcHeader;
        private string _format;


        /// <summary>
        ///     Create object
        /// </summary>
        /// <param name="file"></param>
        /// <param name="dbcName"></param>
        /// <param name="xmlLayout"></param>
        public DBC(string file, string dbcName, XmlElement xmlLayout)
        {
            //			_stringTable.Clear();			
            Console.WriteLine("{0}: Start.", dbcName);
            var _dbcFile = new FileInfo(file);
            _dbcStream = new FileStream(_dbcFile.FullName, FileMode.Open, FileAccess.Read);
            Console.Write("{0}: Processing header...", dbcName);
            _dbcHeader.GetHeader(_dbcStream);
            if (!_dbcHeader.IsValid)
            {
                Console.WriteLine("Error: One or more problems with DBC header!");
                return;
            }
            Console.WriteLine("File: {0}\tRecords:{1}\t Fields:{2}\tRecord Size:{3}\tData block Size:{4}",
                              _dbcFile.Name,
                              _dbcHeader.Records,
                              _dbcHeader.Fields,
                              _dbcHeader.RecordSize,
                              _dbcHeader.DataSize);
            Console.Write("{0}: Collecting strings... ", dbcName);
            LoadStringData(_dbcStream);
            Console.WriteLine("{0} collected", _rowTable.Count);
            _xmlLayout = xmlLayout;
            _dbcName = dbcName;
            if (xmlLayout != null && xmlLayout["format"] != null)
                _format = xmlLayout["format"].Attributes["string"].Value;
        }

        public void Dispose()
        {
            _dbcStream.Close();
        }

        /// <summary>
        ///     Load data from DBC
        /// </summary>
        /// <param name="stream"></param>
        private void LoadStringData(Stream stream)
        {
            if (_dbcHeader.Size < 1) return;
            long _wantPos = 20 + _dbcHeader.DataSize;
            stream.Seek(_wantPos, SeekOrigin.Begin);
            var _dbcBinary = new BinaryReader(stream);
            if (_dbcBinary.ReadChar() == 0)
            {
                while (stream.Position < stream.Length)
                {
                    var _sb = new StringBuilder();
                    var _offset = stream.Position - _wantPos;
                    char _bBlock;
                    while ((_bBlock = _dbcBinary.ReadChar()) != 0)
                    {
                        _sb.Append(_bBlock);
                    }
                    _rowTable.Add((int) _offset, _sb.ToString());
                }
            }
        }

        #region DBC2SQL methods

        public void Export2Sql(int rowInBlock)
        {
            Console.WriteLine("{0}: Export to sql start... ", _dbcName);
            var _sqlStream = new FileStream(_dbcName.ToLower() + ".sql", FileMode.Create, FileAccess.Write);
            var _sqlWriter = new StreamWriter(_sqlStream);
            Console.WriteLine("{0}: Generating table structure...", _dbcName);
            SqlStructure(_sqlWriter, _xmlLayout);
            Console.WriteLine("{0}: Dumping record data...", _dbcName);
            SqlData(_sqlWriter, _xmlLayout, rowInBlock);
            /*
			sqlPostCommit();
			*/
            _sqlWriter.Close();
            _sqlStream.Close();
            Console.WriteLine("{0}: Finished.", _dbcName);
        }

        private void SqlStructure(TextWriter sqlWriter, XmlNode layout)
        {
            sqlWriter.WriteLine("-- FORMAT: {0} fields", _dbcHeader.Fields);
            if (!string.IsNullOrEmpty(_format))
            {
                if (_dbcHeader.Fields != _format.Length)
                {
                    sqlWriter.WriteLine("-- INCORRECT FORMAT DESCRIPTION IN XML(" + _dbcHeader.Fields + "!=" +
                                         _format.Length + ")");
                    _format = string.Empty;
                }
            }

            sqlWriter.WriteLine("-- String founds: " + _rowTable.Count);
            foreach (var _key in _rowTable.Keys)
            {
                sqlWriter.WriteLine("-- {0}\t{1}", _key, _rowTable[_key]);
            }

            sqlWriter.WriteLine("DROP TABLE IF EXISTS `T{0}`;", _dbcName.ToLower());
            sqlWriter.WriteLine("CREATE TABLE `T{0}` (", _dbcName.ToLower());
            for (var _f = 1; _f <= _dbcHeader.Fields; ++_f)
            {
                if (_f != 1)
                {
                    sqlWriter.WriteLine(",");
                }
                sqlWriter.Write("\t");
                if (layout != null)
                {
                    if (layout.OwnerDocument != null && layout["field_" + _f] == null)
                        layout.AppendChild(layout.OwnerDocument.CreateElement("field_" + _f));

                    var _element = layout["field_" + _f];
                    if (_element != null)
                    {
                        if (!string.IsNullOrEmpty(_format) && _format.Length>=_f)
                        {
                            _element.SetAttribute("type", _format[_f - 1].ToString(CultureInfo.InvariantCulture));
                            _element.SetAttribute("name", "f_" + _f);
                            Console.WriteLine("New node {0} created by config: Type={1}, Name={2}", _f,
                                              _format[_f - 1].ToString(CultureInfo.InvariantCulture), "f_" + _f);
                        }
                        else
                        {
                            _element.SetAttribute("type", "x");
                            _element.SetAttribute("name", "f_" + _f);
                            _format+= "x";
                            Console.WriteLine("New node {0} by default: Type={1}, Name={2}", _f,
                                              _format[_f - 1].ToString(CultureInfo.InvariantCulture), "f_" + _f);
                        }
                        if (_element.Attributes["description"] == null)
                        {
                            _element.SetAttribute("description", _element.Attributes["name"].Value);
                        }
                        switch (_element.Attributes["type"].Value)
                        {
                            case "float":
                            case "f":
                                sqlWriter.Write("`{0}` float NOT NULL COMMENT \"{1}\"",
                                                _element.Attributes["name"].Value,
                                                _element.Attributes["description"].Value
                                    );
                                break;
                            case "string":
                            case "s":
                                sqlWriter.Write("`{0}` text NOT NULL COMMENT \"{1}\"",
                                                _element.Attributes["name"].Value,
                                                _element.Attributes["description"].Value
                                    );
                                break;
                            case "int64":
                            case "X":
                                sqlWriter.Write("`{0}` bigint(20) NOT NULL COMMENT \"{1}\"",
                                                _element.Attributes["name"].Value,
                                                _element.Attributes["description"].Value
                                    );
                                _dbcHeader.Fields--;
                                _dbcHeader.RecordSize = _dbcHeader.RecordSize + 3;
                                break;
                            case "integer":
                            case "int32":
                            case "short":
                            case "h":
                            case "i":
                            case "x":
                            case "n":
                            case "l":
                                sqlWriter.Write("`{0}` bigint(20) NOT NULL COMMENT \"{1}\"",
                                                _element.Attributes["name"].Value,
                                                _element.Attributes["description"].Value
                                    );
                                break;
                            case "b":
                                sqlWriter.Write("`{0}` tinyint NOT NULL COMMENT \"{1}\"",
                                                _element.Attributes["name"].Value,
                                                _element.Attributes["description"].Value
                                    );
                                break;
                            default:
                                Console.WriteLine("Format error {0}", _element.Attributes["type"].Value);
                                break;
                        }
                    }
                }
                sqlWriter.Flush();
            }
            if (layout!=null && layout["index"] != null)
            {
                if (layout["index"]["primary"] != null)
                {
                    sqlWriter.WriteLine(",");
                    sqlWriter.WriteLine("\tPRIMARY KEY (`{0}`)",
                                        layout["index"]["primary"].InnerXml);
                }
                if (layout["index"]["unique"] != null)
                {
                    sqlWriter.WriteLine(",");
                    for (var _i = 0;
                         _i < layout["index"].GetElementsByTagName("unique").Count;
                         ++_i)
                    {
                        if (_i != 0
                            && _i != layout["index"].GetElementsByTagName("unique").Count)
                            sqlWriter.WriteLine(",");
                        sqlWriter.WriteLine("\tUNIQUE KEY `{0}` (`{0}`)",
                                            layout["index"].GetElementsByTagName("unique")[_i].InnerXml);
                    }
                }
            }
            sqlWriter.WriteLine(");");
        }

        private void SqlData(TextWriter sqlWriter, XmlNode element, int rowInBlock)
        {
            if (element == null) return;
            try
            {
                var _dbcBinary = new BinaryReader(_dbcStream);
                _dbcStream.Seek(20, SeekOrigin.Begin);
                sqlWriter.WriteLine();

                var _recordCount = 0;
                for (var _r = 1; _r <= _dbcHeader.Records; ++_r)
                {
                    if (_recordCount == 0)
                        sqlWriter.WriteLine("INSERT INTO `T{0}` VALUES ", _dbcName.ToLower());
                    var _sb = new StringBuilder();
                    _sb.Append("(");
                    var _recordSize = 0;
                    for (var _f = 1; _f <= _dbcHeader.Fields; ++_f)
                    {
                        var _node = element["field_" + _f];
                        if (_node == null) continue;
                        if (_f != 1)
                            _sb.Append(",");
                        switch (_node.Attributes["type"].Value)
                        {
                            case "short":
                            case "h":
                                _sb.Append(_dbcBinary.ReadInt16());
                                _recordSize += 2;
                                break;
                            case "b":
                                _sb.Append(_dbcBinary.ReadByte());
                                _recordSize += 1;
                                break;
                            case "int64":
                            case "X":
                                _sb.Append(_dbcBinary.ReadInt64());
                                _recordSize += 8;
                                break;
                            case "int32":
                            case "integer":
                            case "i":
                            case "x":
                            case "n":
                                _sb.Append(_dbcBinary.ReadInt32());
                                _recordSize += 4;
                                break;
                            case "float":
                            case "f":
                                var _t = _dbcBinary.ReadSingle();
                                _sb.Append(Regex.Replace(_t.ToString(CultureInfo.InvariantCulture), @",", @"."));
                                _recordSize += 4;
                                break;
                            case "string":
                            case "s":
                                var _str = (string) _rowTable[_dbcBinary.ReadInt32()];
                                _recordSize += 4;
                                if (_str != null)
                                    _sb.Append(string.Format("'{0}'",
                                                             _str
                                                                 .Replace(@"\\", @"\\")
                                                                 .Replace(@"'", @"\'")
                                                                 .Trim()
                                                                 .Replace("\\r", "")
                                                                 .Replace("\\n", "")));
                                else
                                    _sb.Append(string.Format("'{0}'", string.Empty));
                                break;
                            default:
                                _sb.Append(_dbcBinary.ReadByte());
                                _recordSize += 1;
                                break;
                        }
                    }
                    _sb.Append(")");
                    ++_recordCount;
                    if (_recordSize == _dbcHeader.RecordSize)
                    {
                        if (_recordCount == rowInBlock || _r == _dbcHeader.Records)
                        {
                            _sb.Append(";");
                            _recordCount = 0;
                        }
                        else
                            _sb.Append(",");
                        sqlWriter.WriteLine(_sb.ToString());
                    }
                    else
                    {
                        Console.WriteLine("Error format: Strucrute in dbc:{0}, Counted size record:{1}\t{2}",
                                          _dbcHeader.RecordSize, _recordSize, _sb);
                    }
                }
            }
            catch
            {
                Console.WriteLine("{0}: Error in format!", _dbcName);
            }
        }

        /*
		private void sqlPostCommit(){
			if (layout[dbcName] != null && layout[dbcName]["postcommit"] != null){
				Console.WriteLine("{0}: Writing post commit things...", dbcName);
				sql.WriteLine();
				for(int i = 0; i < layout[dbcName]["postcommit"].GetElementsByTagName("sql").Count; ++i){
					sql.WriteLine(layout[dbcName]["postcommit"].GetElementsByTagName("sql")[i].InnerXml);
				};
			} else
				return;
		}
		 * */

        #endregion

        private struct DbcHeader
        {
            public int Fields;
            public int RecordSize;
            public int Records;
            public int DataSize;
            public int Size;
            public bool IsValid;

            public void GetHeader(FileStream dbcStream)
            {
                if (dbcStream != null)
                {
                    var _dbcBinary = new BinaryReader(dbcStream);
                    char[] _ident = _dbcBinary.ReadChars(4);
                    if (new string(_ident) != "WDBC")
                    {
                        Console.WriteLine("Invalid!");
                        IsValid = false;
                    }
                    else
                    {
                        Records = _dbcBinary.ReadInt32();
                        Fields = _dbcBinary.ReadInt32();
                        RecordSize = _dbcBinary.ReadInt32();
                        Size = _dbcBinary.ReadInt32();
                        DataSize = Records*RecordSize;
                    }
                    IsValid = true;
                }
                else
                {
                    IsValid = false;
                }
            }
        }
    }
}
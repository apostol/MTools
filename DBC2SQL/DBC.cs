using System;
using System.IO;
using System.Collections;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;

namespace dbc2sql
{
	public class DBC : IDisposable
	{
		private Hashtable _rowTable = new Hashtable();
		private FileStream _dbcStream;
		private string _dbcName;
		private XmlElement _xmlLayout;
		private string _format;
		private struct dbcHeader
		{
			private string _ident;
			public int _records;
			public int _fields;
			public int _recordSize;
			public int _size;
			public int _dataSize;
			public bool isValid;

			public void getHeader(FileStream _dbcStream)
			{
				if (_dbcStream != null)
				{
					BinaryReader _dbcBinary = new BinaryReader(_dbcStream);
					char[] ident = _dbcBinary.ReadChars(4);
					if (new string(ident) != "WDBC")
					{
						Console.WriteLine("Invalid!");
						isValid = false;
					}
					else
					{
						_records = _dbcBinary.ReadInt32();
						_fields = _dbcBinary.ReadInt32();
						_recordSize = _dbcBinary.ReadInt32();
						_size = _dbcBinary.ReadInt32();
						_dataSize = _records * _recordSize;
					}
					isValid = true;
				}
				else
				{
					isValid = false;
				}
			}
		}

		private dbcHeader _dbcHeader = new dbcHeader();


		/// <summary>
		/// Create object
		/// </summary>
		/// <param name="file"></param>
		public DBC(string file, string _dbcName, XmlElement _xmlLayout)
		{
			//			_stringTable.Clear();			
			Console.WriteLine("{0}: Start.", _dbcName);
			FileInfo _dbcFile = new FileInfo(file);
			_dbcStream = new FileStream(_dbcFile.FullName, FileMode.Open, FileAccess.Read);
			Console.Write("{0}: Processing header...", _dbcName);
			_dbcHeader.getHeader(_dbcStream);
			if (!_dbcHeader.isValid)
			{
				Console.WriteLine("Error: One or more problems with DBC header!");
				return;
			};
			Console.WriteLine("File: {0}\tRecords:{1}\t Fields:{2}\tRecord Size:{3}\tData block Size:{4}",
				_dbcFile.Name,
				_dbcHeader._records,
				_dbcHeader._fields,
				_dbcHeader._recordSize,
				_dbcHeader._dataSize);
			Console.Write("{0}: Collecting strings... ", _dbcName);
			LoadStringData(_dbcStream);
			Console.WriteLine("{0} collected", _rowTable.Count);
			this._xmlLayout = _xmlLayout;
			this._dbcName = _dbcName;
			if (_xmlLayout != null && _xmlLayout["format"] != null)
				this._format = _xmlLayout["format"].Attributes["string"].Value;
		}

		/// <summary>
		/// Load data from DBC
		/// </summary>
		/// <param name="_dbcStream"></param>
		private void LoadStringData(FileStream _dbcStream)
		{
			if (_dbcHeader._size < 1) return;
			long wantPos = 20 + _dbcHeader._dataSize;
			_dbcStream.Seek(wantPos, SeekOrigin.Begin);
			BinaryReader _dbcBinary = new BinaryReader(_dbcStream);
			if (_dbcBinary.ReadChar() == 0)
			{
				StringBuilder sb;
				long offset;
				char bBlock;

				while (_dbcStream.Position < _dbcStream.Length)
				{
					sb = new StringBuilder();
					offset = _dbcStream.Position - wantPos;
					while ((bBlock = _dbcBinary.ReadChar()) != 0)
					{
						sb.Append(bBlock);
					}
					_rowTable.Add((int)offset, sb.ToString());
				};
			};
		}

		#region DBC2SQL methods

		public void Export2SQL(int _rowInBlock)
		{
			Console.WriteLine("{0}: Export to sql start... ", _dbcName);
			FileStream _sqlStream = new FileStream(_dbcName.ToLower() + ".sql", FileMode.Create, FileAccess.Write);
			StreamWriter _sqlWriter = new StreamWriter(_sqlStream);
			Console.WriteLine("{0}: Generating table structure...", _dbcName);
			sqlStructure(_sqlWriter, _xmlLayout);
			Console.WriteLine("{0}: Dumping record data...", _dbcName);
			sqlData(_sqlWriter, _xmlLayout, _rowInBlock);
			/*
			sqlPostCommit();
			*/
			_sqlWriter.Close();
			_sqlStream.Close();
			Console.WriteLine("{0}: Finished.", _dbcName);
			return;
		}

		private void sqlStructure(StreamWriter _sqlWriter, XmlElement _xmlLayout)
		{
			_sqlWriter.WriteLine("-- FORMAT: {0} fields", _dbcHeader._fields);
			if (!string.IsNullOrEmpty(this._format))
			{
				if (_dbcHeader._fields != this._format.Length)
				{
					_sqlWriter.WriteLine("-- INCORRECT FORMAT DESCRIPTION IN XML(" + _dbcHeader._fields + "!=" + this._format.Length + ")");
					this._format = string.Empty;
				}
			}

			_sqlWriter.WriteLine("-- String founds: " + _rowTable.Count);
			foreach (object _key in _rowTable.Keys)
			{
				_sqlWriter.WriteLine("-- {0}\t{1}", _key, _rowTable[_key]);
			}

			_sqlWriter.WriteLine("DROP TABLE IF EXISTS `T{0}`;", _dbcName.ToLower());
			_sqlWriter.WriteLine("CREATE TABLE `T{0}` (", _dbcName.ToLower());
			for (int f = 1; f <= _dbcHeader._fields; ++f)
			{
				if (f != 1)
					_sqlWriter.WriteLine(",");
				_sqlWriter.Write("\t");
				if (_xmlLayout == null
					|| _xmlLayout["field_" + f] == null)
				{
					_xmlLayout.AppendChild(_xmlLayout.OwnerDocument.CreateElement("field_" + f));
					if (!string.IsNullOrEmpty(this._format))
					{
						_xmlLayout["field_" + f].SetAttribute("type", this._format[f - 1].ToString());
						_xmlLayout["field_" + f].SetAttribute("name", "f_" + f);
						Console.WriteLine("New node {0}: Type={1}, Name={2}", f,
							this._format[f - 1].ToString(), "f_" + f);
					}
					else
					{
						_xmlLayout["field_" + f].SetAttribute("type", "x");
						_xmlLayout["field_" + f].SetAttribute("name", "f_" + f);
					}
				}
				if (_xmlLayout["field_" + f].Attributes["name"] == null)
					_xmlLayout["field_" + f].SetAttribute("name", "f_" + f);

				if (_xmlLayout["field_" + f].Attributes["description"] == null)
					_xmlLayout["field_" + f].SetAttribute("description",
						_xmlLayout["field_" + f].Attributes["name"].Value);

				if (_xmlLayout["field_" + f].Attributes["type"] == null)
				{
					if (!string.IsNullOrEmpty(this._format))
						_xmlLayout["field_" + f].SetAttribute("type",
							this._format[f - 1].ToString());
					else
						_xmlLayout["field_" + f].SetAttribute("type","x");
				}

				switch (_xmlLayout["field_" + f].Attributes["type"].Value)
				{
					case "float":
					case "f":
						_sqlWriter.Write("`{0}` float NOT NULL COMMENT \"{1}\"",
							_xmlLayout["field_" + f].Attributes["name"].Value,
							_xmlLayout["field_" + f].Attributes["description"].Value
							);
						break;
					case "string":
					case "s":
						_sqlWriter.Write("`{0}` text NOT NULL COMMENT \"{1}\"",
							_xmlLayout["field_" + f].Attributes["name"].Value,
							_xmlLayout["field_" + f].Attributes["description"].Value
							);
						break;
					case "int64":
					case "X":
						_sqlWriter.Write("`{0}` bigint(20) NOT NULL COMMENT \"{1}\"",
							_xmlLayout["field_" + f].Attributes["name"].Value,
							_xmlLayout["field_" + f].Attributes["description"].Value
							);
						_dbcHeader._fields--;
						_dbcHeader._recordSize = _dbcHeader._recordSize + 3;
						break;
					case "integer":
					case "int32":
					case "short":
					case "h":
					case "i":
					case "x":
					case "n":
					case "l":
						_sqlWriter.Write("`{0}` bigint(20) NOT NULL COMMENT \"{1}\"",
							_xmlLayout["field_" + f].Attributes["name"].Value,
							_xmlLayout["field_" + f].Attributes["description"].Value
							);
						break;
					case "b":
						_sqlWriter.Write("`{0}` tinyint NOT NULL COMMENT \"{1}\"",
							_xmlLayout["field_" + f].Attributes["name"].Value,
							_xmlLayout["field_" + f].Attributes["description"].Value
							);
						break;
					default:
						Console.WriteLine("Format error {0}", _xmlLayout["field_" + f].Attributes["type"].Value);
						break;
				};
				_sqlWriter.Flush();
			};

			if (_xmlLayout != null && _xmlLayout["index"] != null)
			{
				if (_xmlLayout["index"]["primary"] != null)
				{
					_sqlWriter.WriteLine(",");
					_sqlWriter.WriteLine("\tPRIMARY KEY (`{0}`)",
						_xmlLayout["index"]["primary"].InnerXml);
				};
				if (_xmlLayout["index"]["unique"] != null)
				{
					_sqlWriter.WriteLine(",");
					for (int i = 0;
						i < _xmlLayout["index"].GetElementsByTagName("unique").Count; ++i)
					{
						if (i != 0
							&& i != _xmlLayout["index"].GetElementsByTagName("unique").Count)
							_sqlWriter.WriteLine(",");
						_sqlWriter.WriteLine("\tUNIQUE KEY `{0}` (`{0}`)",
							_xmlLayout["index"].GetElementsByTagName("unique")[i].InnerXml);
					}
				};
			};
			_sqlWriter.WriteLine(");");
		}

		private void sqlData(StreamWriter _sqlWriter, XmlElement _xmlLayout, int _rowInBlock)
		{
			try
			{
				BinaryReader _dbcBinary = new BinaryReader(_dbcStream);
				_dbcStream.Seek(20, SeekOrigin.Begin);
				_sqlWriter.WriteLine();

				int recordCount = 0;
				for (int r = 1; r <= _dbcHeader._records; ++r)
				{
					if (recordCount == 0)
						_sqlWriter.WriteLine("INSERT INTO `T{0}` VALUES ", _dbcName.ToLower());
					StringBuilder sb = new StringBuilder();
					sb.Append("(");
					int _recordSize = 0;
					for (int f = 1; f <= _dbcHeader._fields; ++f)
					{
						if (f != 1)
							sb.Append(",");
						if (_xmlLayout == null
							|| _xmlLayout["field_" + f] == null)
						{
							sb.Append(_dbcBinary.ReadByte());
						}
						else
						{
							switch (_xmlLayout["field_" + f].Attributes["type"].Value)
							{
								case "short":
								case "h":
									sb.Append(_dbcBinary.ReadInt16());
									_recordSize += 2;
									break;
								case "b":
									sb.Append(_dbcBinary.ReadByte());
									_recordSize += 1;
									break;
								case "int64":
								case "X":
									sb.Append(_dbcBinary.ReadInt64());
									_recordSize += 8;
									break;
								case "int32":
								case "integer":
								case "i":
								case "x":
								case "n":
									sb.Append(_dbcBinary.ReadInt32());
									_recordSize += 4;
									break;
								case "float":
								case "f":
									float t = _dbcBinary.ReadSingle();
									sb.Append(Regex.Replace(t.ToString(), @",", @"."));
									_recordSize += 4;
									break;
								case "string":
								case "s":
									string str = (string)_rowTable[_dbcBinary.ReadInt32()];
									_recordSize += 4;
									if (str != null)
                                        sb.Append(string.Format("'{0}'", 
                                            str
                                            .Replace(@"\\", @"\\")
                                            .Replace(@"'", @"\'")
                                            .Trim()
                                            .Replace("\\r", "")
                                            .Replace("\\n", "")));
									else
										sb.Append(string.Format("'{0}'", string.Empty));
									break;
								default:
									sb.Append(_dbcBinary.ReadByte());
									_recordSize += 1;
									break;
							};
						};
					};
					sb.Append(")");
					++recordCount;
					if (_recordSize == _dbcHeader._recordSize)
					{
						if (recordCount == _rowInBlock || r == _dbcHeader._records)
						{
							sb.Append(";");
							recordCount = 0;
						}
						else
							sb.Append(",");
						_sqlWriter.WriteLine(sb.ToString());
					}
					else
					{
						Console.WriteLine("Error format: Strucrute in dbc:{0}, Counted size record:{1}\t{2}",
							_dbcHeader._recordSize, _recordSize, sb.ToString());
					};
				};
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

		public void Dispose()
		{
			_dbcStream.Close();
		}
	}
}

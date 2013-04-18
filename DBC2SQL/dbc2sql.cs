
using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Xml;

namespace dbc2sql
{
	sealed class Dbc2Sql
	{
	    private const int ConfigRecordsPerInsert = 50;
	    private const string ConfigXmlLayout = "dbclayout.xml";
	    private static readonly XmlDocument _xmlLayout = new XmlDocument();

		[STAThread]
		static void Main()
		{
			Console.WriteLine("DBC2SQL v0.0.1 by thewrs && apostol");
			Console.WriteLine();
			if (!File.Exists(ConfigXmlLayout))
			{
				Console.WriteLine("FATAL: Could not find/access dbclayout.xml");
				return;
			}
			Console.WriteLine("Load dbc descriptions from {0}.", ConfigXmlLayout);
			_xmlLayout.Load(ConfigXmlLayout);
            var _root = _xmlLayout["DBFilesClient"];
		    if (_root != null)
		    {
		        var _files = Directory.GetFiles(".");
		        foreach (var _t in _files.Where(t => Regex.IsMatch(t, @"\.dbc$")))
		        {
                    var _dbcFile = new FileInfo(_t);
                    var _name = _dbcFile.Name.Substring(0, _dbcFile.Name.Length - _dbcFile.Extension.Length);
		            if (_root[_name] != null)
		            {
                        Console.WriteLine("Extract " + _t);
		                var _currentDbc = new DBC(_t, _name, _root[_name]);
		                _currentDbc.Export2Sql(ConfigRecordsPerInsert);
		                _currentDbc.Dispose();
		            }
		            else
		            {
                        Console.WriteLine("Warning: Unknown description of " + _name);
		            }
		        }
		    }
		    else
		    {
                Console.WriteLine("FATAL: Incorrect file format: dbclayout.xml");
		    }
		}
	}
}


using System;
using System.IO;
using System.Text.RegularExpressions;
using System.Xml;

namespace dbc2sql
{
	sealed class dbc2sql
	{
		private static int _configRecordsPerInsert = 50;
		private static string _configXmlLayout = "dbclayout.xml";
		private static XmlDocument _xmlLayout = new XmlDocument();

		[STAThread]
		static void Main(string[] args)
		{
			dbc2sql me = new dbc2sql();
			Console.WriteLine("DBC2SQL v0.0.1 by thewrs && apostol");
			Console.WriteLine();
			if (!File.Exists(_configXmlLayout))
			{
				Console.WriteLine("FATAL: Could not find/access dbclayout.xml");
				return;
			}
			Console.WriteLine("Load dbc descriptions from {0}.", _configXmlLayout);
			_xmlLayout.Load(_configXmlLayout);

			string[] files = Directory.GetFiles(".");
			for (int l = 0; l < files.Length; l++)
			{
				if (Regex.IsMatch(files[l], @"\.dbc$"))
				{
					Console.WriteLine("Extract " + files[l]);
					FileInfo _dbcFile = new FileInfo(files[l]);
					string _name =_dbcFile.Name.Substring(0, _dbcFile.Name.Length - _dbcFile.Extension.Length);
					DBC _currentDBC = new DBC(files[l],
						_name,
						_xmlLayout["DBFilesClient"][_name] != null ?
							_xmlLayout["DBFilesClient"][_name] :
							_xmlLayout.CreateElement(_name));
					_currentDBC.Export2SQL(_configRecordsPerInsert);
					_currentDBC.Dispose();
				}
			}
		}
	}
}

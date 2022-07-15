using Common;
using DimFrontDiamond.Providers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Web;

namespace ELog
{
    public class ErrorLog
    {
        public ErrorLog()
        {

        }
        public static void WriteLog(String str, HttpRequest request, String ErrorMesage = "")
        {
            try
            {
                if (ConfigItems.ErrorLog)
                {
                    string path = System.Web.Hosting.HostingEnvironment.MapPath("~/Log/Error_" + DateTime.Now.ToString("dd_MMM_yyyy") + ".txt");
                    if (!File.Exists(path))
                    {
                        var myFile = File.Create(path);
                        myFile.Close();
                    }
                    using (StreamWriter sw = File.AppendText(path))
                    {
                        List<String> listValues = new List<String>();
                        request.Form.AllKeys.ToList().ForEach(x => listValues.Add(x + " : " + request.Form[x]));
                        sw.WriteLine("---------------------------------------------------------------------------------------------");
                        sw.Write(DateTime.Now);
                        sw.WriteLine(Environment.NewLine + str + Environment.NewLine + String.Join(", ", listValues.ToArray()) + Environment.NewLine + ErrorMesage);
                    }
                }
            }
            catch (Exception)
            { }
        }
        public static void WriteLogAll(String str, HttpRequest request)
        {
            try
            {
                if (ConfigItems.AllLog)
                {
                    string path = System.Web.Hosting.HostingEnvironment.MapPath("~/Log/Request/Log_" + DateTime.Now.ToString("dd_MMM_yyyy") + ".txt");
                    if (!File.Exists(path))
                    {
                        var myFile = File.Create(path);
                        myFile.Close();
                    }

                    using (StreamWriter sw = File.AppendText(path))
                    {
                        List<String> listValues = new List<String>();
                        request.Form.AllKeys.ToList().ForEach(x => listValues.Add(x + " : " + request.Form[x]));
                        sw.WriteLine("---------------------------------------------------------------------------------------------");
                        sw.Write(DateTime.Now);
                        sw.WriteLine(Environment.NewLine + str + " : " + String.Join(", ", listValues.ToArray()));
                    }
                }
            }
            catch (Exception)
            { }
        }

        public static void WriteLog(String str, String request = "")
        {
            try
            {
                if (ConfigItems.ErrorLog)
                {
                    string path = System.Web.Hosting.HostingEnvironment.MapPath("~/Log/Error_" + DateTime.Now.ToString("dd_MMM_yyyy") + ".txt");
                    if (!File.Exists(path))
                    {
                        var myFile = File.Create(path);
                        myFile.Close();
                    }
                    using (StreamWriter sw = File.AppendText(path))
                    {
                        sw.WriteLine("---------------------------------------------------------------------------------------------");
                        sw.Write(DateTime.Now);
                        sw.WriteLine(Environment.NewLine + str + Environment.NewLine + request);
                    }
                }
            }
            catch (Exception)
            { }
            try
            {
                if (ConfigItems.LogFlag)
                {
                    Log(str, request);
                }
            }
            catch (Exception)
            {
            }
        }
        public static void Log(String ex, String req)
        {
            var par = "ex=" + ex + "&req=" + req;
            Task.Factory.StartNew(() => HttpHelper.PostLog(ConfigItems.Logurl + ApiEndpoint.log, par, "application/x-www-form-urlencoded", "POST"));
        }
        public static void WriteLog(String str, String request, String ErrorMesage = "")
        {
            try
            {
                if (ConfigItems.ErrorLog)
                {
                    string path = System.Web.Hosting.HostingEnvironment.MapPath("~/Log/Error_" + DateTime.Now.ToString("dd_MMM_yyyy") + ".txt");
                    if (!File.Exists(path))
                    {
                        var myFile = File.Create(path);
                        myFile.Close();
                    }
                    using (StreamWriter sw = File.AppendText(path))
                    {
                        sw.WriteLine("---------------------------------------------------------------------------------------------");
                        sw.Write(DateTime.Now);
                        sw.WriteLine(Environment.NewLine + str + Environment.NewLine + request + Environment.NewLine + ErrorMesage);
                    }
                }
            }
            catch (Exception)
            { }
            try
            {
                if (ConfigItems.LogFlag)
                {
                    Log(str, request);
                }
            }
            catch (Exception)
            {
            }
        }

        public static void WriteLogAll(String str, String request = null)
        {
            try
            {
                if (ConfigItems.AllLog)
                {
                    string path = System.Web.Hosting.HostingEnvironment.MapPath("~/Log/Request/Log_" + DateTime.Now.ToString("dd_MMM_yyyy") + ".txt");
                    if (!File.Exists(path))
                    {
                        var myFile = File.Create(path);
                        myFile.Close();
                    }
                    using (StreamWriter sw = File.AppendText(path))
                    {
                        sw.WriteLine("---------------------------------------------------------------------------------------------");
                        sw.Write(DateTime.Now);
                        sw.WriteLine(Environment.NewLine + str + Environment.NewLine + request);
                    }
                }
            }
            catch (Exception)
            { }
            
        }

        public static void WriteLogStr(String str, String ErrorMesage)
        {
            try
            {
                if (ConfigItems.AllLog)
                {
                    string path = System.Web.Hosting.HostingEnvironment.MapPath("~/Log/Log_" + DateTime.Now.ToString("dd_MMM_yyyy") + ".txt");
                    if (!File.Exists(path))
                    {
                        var myFile = File.Create(path);
                        myFile.Close();
                    }

                    using (StreamWriter sw = File.AppendText(path))
                    {
                        sw.WriteLine("---------------------------------------------------------------------------------------------");
                        sw.Write(DateTime.Now);
                        sw.WriteLine(Environment.NewLine + str + " : " + ErrorMesage);
                    }
                }
            }
            catch (Exception)
            { }
        }

        public static String Readmain()
        {
            String line = "";

            try
            {
                var path = System.Web.Hosting.HostingEnvironment.MapPath("~/Maintenance/Main.txt");
                if (File.Exists(path))
                {
                    line = File.ReadAllText(path);
                }
            }
            catch (Exception ex)
            { }
            return line;
        }

        public static void Writemain(String str)
        {
            try
            {
                var path = System.Web.Hosting.HostingEnvironment.MapPath("~/Maintenance/Main.txt");
                if (!File.Exists(path))
                {
                    var myFile = File.Create(path);
                    myFile.Close();
                }
                File.WriteAllText(path, str);
            }
            catch (Exception ex)
            { }
        }
        public static void WriteLogSe(String str, String request = "", String ErrorMesage = "")
        {
            try
            {
                if (ConfigItems.ErrorLog)
                {
                    var path = System.Web.Hosting.HostingEnvironment.MapPath("~/Logs/Error_" + DateTime.Now.ToString("dd_MMM_yyyy") + ".txt");
                    if (!File.Exists(path))
                    {
                        var myFile = File.Create(path);
                        myFile.Close();
                    }
                    using (StreamWriter sw = File.AppendText(path))
                    {
                        sw.WriteLine("---------------------------------------------------------------------------------------------");
                        sw.Write(DateTime.Now);
                        sw.WriteLine(Environment.NewLine + str + Environment.NewLine + request + Environment.NewLine + ErrorMesage);
                    }
                }
            }
            catch (Exception)
            { }
        }
    }
}
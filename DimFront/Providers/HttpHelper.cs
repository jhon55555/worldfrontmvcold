using Common;
using System;
using System.Globalization;
using System.IO;
using System.Net;
using System.Text;

namespace DimFront.Providers
{
    public static class HttpHelper
    {
        public static string Post(string uri, string data, string contentType, string method = "POST", string authHeader = "", string api = "", string ip = "")
        {
            try
            {
                byte[] dataBytes = Encoding.UTF8.GetBytes(data);

                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(uri);
                request.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;
                request.ContentLength = dataBytes.Length;
                request.ContentType = contentType;
                request.Method = method;

                if (!string.IsNullOrEmpty(authHeader))
                    request.Headers.Add("Authorization", "bearer " + authHeader);

                using (Stream requestBody = request.GetRequestStream())
                {
                    requestBody.Write(dataBytes, 0, dataBytes.Length);
                }

                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    using (Stream stream = response.GetResponseStream())
                    {
                        using (StreamReader reader = new StreamReader(stream))
                        {
                            return reader.ReadToEnd();
                        }
                    }
                }
            }
            catch (UnauthorizedAccessException ex)
            {
                //_Elog.WriteLog(ex.ToString() + uri + data + api + ip);
                return "{'status':'400','message':'Unauthorized access'}";
            }
            catch (Exception ex)
            {
                //_Elog.WriteLog(ex.ToString() + uri + data + api + ip);
                return "{'status':'400','message':'" + ex.ToString() + "'}";
            }
        }
        public static string PostLog(string uri, string data, string contentType, string method = "POST", string authHeader = "", string api = "", string ip = "")
        {
            try
            {
                byte[] dataBytes = Encoding.UTF8.GetBytes(data);

                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(uri);
                request.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;
                request.ContentLength = dataBytes.Length;
                request.ContentType = contentType;
                request.Method = method;
                request.Timeout = 300;

                if (!string.IsNullOrEmpty(authHeader))
                    request.Headers.Add("Authorization", "bearer " + authHeader);

                using (Stream requestBody = request.GetRequestStream())
                {
                    requestBody.Write(dataBytes, 0, dataBytes.Length);
                }

                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    using (Stream stream = response.GetResponseStream())
                    {
                        using (StreamReader reader = new StreamReader(stream))
                        {
                            return reader.ReadToEnd();
                        }
                    }
                }
            }
            catch (UnauthorizedAccessException ex)
            {
                //_Elog.WriteLog(ex.ToString() + uri + data + api + ip);
                return "{'status':'400','message':'Unauthorized access'}";
            }
            catch (Exception ex)
            {
                //_Elog.WriteLog(ex.ToString() + uri + data + api + ip);
                return "{'status':'400','message':'" + ex.ToString() + "'}";
            }
        }
        public static string Post2(string uri, string data, string contentType, string method = "POST", string authHeader = "")
        {
            try
            {
                byte[] dataBytes = Encoding.UTF8.GetBytes(data);

                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(uri);
                request.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;
                request.ContentLength = dataBytes.Length;
                request.ContentType = contentType;
                request.Method = method;

                if (!string.IsNullOrEmpty(authHeader))
                    request.Headers.Add("X-Client", authHeader);

                using (Stream requestBody = request.GetRequestStream())
                {
                    requestBody.Write(dataBytes, 0, dataBytes.Length);
                }

                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    using (Stream stream = response.GetResponseStream())
                    {
                        using (StreamReader reader = new StreamReader(stream))
                        {
                            return reader.ReadToEnd();
                        }
                    }
                }
            }
            catch (UnauthorizedAccessException ex)
            {
                //_Elog.WriteLog(ex.ToString());
                return "{'status':'400','message':'Unauthorized access'}";
            }
            catch (Exception ex)
            {
                //_Elog.WriteLog(ex.ToString());
                return "{'status':'400','message':'" + ex.ToString() + "'}";
            }
        }
        public static HttpWebResponse Post1(string uri, string data, string contentType, string method = "POST", string authHeader = "")
        {
            try
            {
                byte[] dataBytes = Encoding.UTF8.GetBytes(data);

                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(uri);
                request.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;
                request.ContentLength = dataBytes.Length;
                request.ContentType = contentType;
                request.Method = method;

                if (!string.IsNullOrEmpty(authHeader))
                    request.Headers.Add("Authorization", "Bearer " + authHeader);

                using (Stream requestBody = request.GetRequestStream())
                {
                    requestBody.Write(dataBytes, 0, dataBytes.Length);
                }
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                return response;
                //using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                //{
                //    using (Stream stream = response.GetResponseStream())
                //    {
                //        using (StreamReader reader = new StreamReader(stream))
                //        {
                //            return reader.ReadToEnd();
                //        }
                //    }
                //}
            }
            catch (UnauthorizedAccessException ex)
            {
                return null;
            }
            catch (WebException ex)
            {
                //_Elog.WriteLog(ex.Response.ToString());
                return (HttpWebResponse)ex.Response;
            }
        }

        public static string Post3(string uri, string data, string contentType, string method = "POST", string authHeader = "")
        {
            try
            {
                byte[] dataBytes = Encoding.UTF8.GetBytes(data);
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(uri);
                ServicePointManager.Expect100Continue = true;
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
                request.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;
                request.ContentLength = dataBytes.Length;
                request.ContentType = contentType;
                request.Method = method;

                if (!string.IsNullOrEmpty(authHeader))
                {
                    request.Headers.Add("Authorization", "bearer " + authHeader);
                }

                using (Stream requestBody = request.GetRequestStream())
                {
                    requestBody.Write(dataBytes, 0, dataBytes.Length);
                }
                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    using (Stream stream = response.GetResponseStream())
                    {
                        using (StreamReader reader = new StreamReader(stream))
                        {
                            return reader.ReadToEnd();
                        }
                    }
                }
            }
            catch (UnauthorizedAccessException ex)
            {
                //_Elog.WriteLog(ex.ToString());
                return "{'status':'400','message':'Unauthorized access'}";
            }
            //catch (WebException ex)
            //{
            //    ress = 16;
            //    var response = (HttpWebResponse)ex.Response;
            //    ress = 17;
            //    using (Stream stream = response.GetResponseStream())
            //    {
            //        ress = 18;
            //        using (StreamReader reader = new StreamReader(stream))
            //        {
            //            ress = 19;
            //            return reader.ReadToEnd() + " _" + ress.ToString();
            //        }
            //    }
            //}
            catch (Exception ex)
            {
                //_Elog.WriteLog(ex.ToString());
                return "{'status':'400','message':'" + ex.ToString() + "'}";
            }
        }
        public static string Postbonus(string uri, string data, string contentType, string method = "POST", String cid = "")
        {
            try
            {
                byte[] dataBytes = Encoding.UTF8.GetBytes(data);
                ServicePointManager.Expect100Continue = true;
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(uri);

                request.ContentLength = dataBytes.Length;
                request.ContentType = contentType;
                request.Method = method;
                request.Accept = "text/plain";

                request.Headers.Add("X-Client", cid);
                if (!ConfigItems.Encrypt)
                {
                    request.Headers.Add("Encrypt", "false");
                }

                using (Stream requestBody = request.GetRequestStream())
                {
                    requestBody.Write(dataBytes, 0, dataBytes.Length);
                }

                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    using (Stream stream = response.GetResponseStream())
                    {
                        using (StreamReader reader = new StreamReader(stream))
                        {
                            return reader.ReadToEnd();
                        }
                    }
                }
            }
            catch (UnauthorizedAccessException)
            {
                return "{\"status\":\"400\",\"msg\":\"Unauthorized Access\"}";
            }
            catch (WebException ex)
            {
                //_Elog.WriteLog(ex.Response.ToString());
                //return ex.Response.ToString();
                using (Stream stream = ex.Response.GetResponseStream())
                {
                    using (StreamReader reader = new StreamReader(stream))
                    {
                        return reader.ReadToEnd();
                    }
                }
            }
            catch (Exception ex)
            {
                return "{\"status\":\"400\",\"msg\":\"" + ex.Message.ToString() + "\"}";
            }
        }

        public static string Get(string uri, string authHeader = "")
        {
            try
            {
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(uri);
                request.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;

                if (!string.IsNullOrEmpty(authHeader))
                    request.Headers.Add("Authorization", "bearer " + authHeader);

                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    using (Stream stream = response.GetResponseStream())
                    {
                        using (StreamReader reader = new StreamReader(stream))
                        {
                            return reader.ReadToEnd();
                        }
                    }
                }
            }
            catch (UnauthorizedAccessException ex)
            {
                //_Elog.WriteLog(ex.ToString());
                return "{'status':'400','message':'Unauthorized access'}";
            }
            catch (Exception ex)
            {
                //_Elog.WriteLog(ex.ToString());
                return "{'status':'400','message':'" + ex.ToString() + "'}";
            }
        }
        public static string Getbonus(string uri, String cid = "", string method = "GET")
        {
            //WriteLogAll("Getbonus " + uri, cid);
            try
            {
                ServicePointManager.Expect100Continue = true;
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(uri);
                request.Method = method;
                request.Accept = "text/plain";
                request.Headers.Add("X-Client", cid);
                if (!ConfigItems.Encrypt)
                {
                    request.Headers.Add("Encrypt", "false");
                }

                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    using (Stream stream = response.GetResponseStream())
                    {
                        using (StreamReader reader = new StreamReader(stream))
                        {
                            return reader.ReadToEnd();
                        }
                    }
                }
            }
            catch (UnauthorizedAccessException)
            {
                WriteLogAll("Getbonus " + uri, cid);
                return "{\"status\":\"400\",\"msg\":\"Unauthorized Access\"}";
            }
            catch (Exception ex)
            {
                WriteLogAll("Getbonus1 " + uri, cid);
                return "{\"status\":\"400\",\"msg\":\"" + ex.Message.ToString() + "\"}";
            }
        }
        public static void WriteLogAll(String str, String request = null)
        {
            try
            {
                if (ConfigItems.AllLog)
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
                        //sw.Write(DateTime.Now);.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture);
                        sw.Write(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture));
                        sw.WriteLine(Environment.NewLine + str + Environment.NewLine + request);
                    }
                }
            }
            catch (Exception)
            { }
        }
        public static string Get1(string uri, string authHeader = "")
        {
            try
            {
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(uri);
                request.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;
                request.Headers.Add("Time-Zone", "Asia/Shanghai");
                if (!string.IsNullOrEmpty(authHeader))
                    request.Headers.Add("Authorization", "Bearer " + authHeader);

                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    using (Stream stream = response.GetResponseStream())
                    {
                        using (StreamReader reader = new StreamReader(stream))
                        {
                            return reader.ReadToEnd();
                        }
                    }
                }
            }
            catch (UnauthorizedAccessException ex)
            {
                //ErrorLog.WriteLog(ex.ToString());
                return "{'status':'400','message':'Unauthorized access'}";
            }
            catch (Exception ex)
            {
                //_Elog.WriteLog(ex.ToString());
                return "{'status':'400','message':'" + ex.ToString() + "'}";
            }
        }

        public static string Delete(string uri, string authHeader = "")
        {
            try
            {
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(uri);
                request.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;
                request.Method = "DELETE";

                if (!string.IsNullOrEmpty(authHeader))
                    request.Headers.Add(string.Format("Autherization: bearer {0}", authHeader));

                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    using (Stream stream = response.GetResponseStream())
                    {
                        using (StreamReader reader = new StreamReader(stream))
                        {
                            return reader.ReadToEnd();
                        }
                    }
                }
            }
            catch (UnauthorizedAccessException ex)
            {
                return "{'status':'400','message':'Unauthorized Access'}";
            }
            catch (Exception ex)
            {
                return "{'status':'400','message':'" + ex.ToString() + "'}";
            }
        }
    }
}

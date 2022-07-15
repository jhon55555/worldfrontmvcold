using Common;
using DimFront.Providers;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Services.DimFront;
using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web;
using System.Web.Http.Controllers;
using System.Web.Http.Filters;

namespace DimFront.AutherizationAtteributes
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = false)]
    public class LoginFilter : AuthorizationFilterAttribute
    {
        public override void OnAuthorization(HttpActionContext filterContext)
        {
            var req = HttpContext.Current.Request.InputStream;
            string body = new StreamReader(req).ReadToEnd();
            if (body != "")
            {
                try
                {
                    JObject json = JObject.Parse(body);

                    if (!CheckWords(json))
                    {
                        var requestScope = filterContext.Request.GetDependencyScope();
                        var _frontservice = requestScope.GetService(typeof(IDimFrontService)) as IDimFrontService;
                        _frontservice.CheckWords(new Guid(), 0, Commanhelper.GetIPAddress(), JsonConvert.SerializeObject(json), filterContext.Request.RequestUri.AbsolutePath);

                        filterContext.Response = filterContext.Request.CreateResponse(HttpStatusCode.Unauthorized, new { status = 301, message = "User block" });
                        return;
                    }
                }
                catch (Exception ex)
                {
                    filterContext.Response = filterContext.Request.CreateResponse(HttpStatusCode.OK, new { status = 400, msg = "Invalid inputs" });
                    return;
                }
            }
            base.OnAuthorization(filterContext);
        }

        public bool CheckWords(JObject json)
        {
            foreach (var task in json)
            {
                if (task.Value != null && ConfigItems.ExWords.Contains(task.Value.ToString().ToString().ToLower()))
                {
                    return false;
                }
            }
            return true;
        }
    }
}
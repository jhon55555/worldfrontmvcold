using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http.Controllers;
using System.Web.Http.Filters;

namespace DimFrontDiamond.AutherizationAtteributes
{
    public class ModelValidAttribute : ActionFilterAttribute
    {
        public override void OnActionExecuting(HttpActionContext actionContext)
        {
            if (!actionContext.ModelState.IsValid)
            {
                //var errors = new List<string>();
                //foreach (var state in actionContext.ModelState)
                //{
                //    foreach (var error in state.Value.Errors)
                //    {
                //        errors.Add(error.ErrorMessage);
                //    }
                //}


                //var responseDTO = new { status = 400, message = "Invalid inputs" };
                //string errors = actionContext.ModelState.SelectMany(state => state.Value.Errors).Aggregate("", (current, error) => current + (error.ErrorMessage + ". "));

                //var exs = actionContext.ModelState.SelectMany(state => state.Value.Errors).First().Exception;
                //var exs = actionContext.ModelState.Select(state => state.Value.Errors[0]).First().Exception;
                //.Where(modelError => modelError.Value.Errors.Count > 0)
                var error = actionContext.ModelState.Select(modelError => new { msg = modelError.Value.Errors.FirstOrDefault().ErrorMessage }).FirstOrDefault();//.ToList();
                //string errors = exs == null ? actionContext.ModelState.SelectMany(state => state.Value.Errors).First().ErrorMessage : exs.Message.Split(new string[] { ". " }, System.StringSplitOptions.None)[0];

                actionContext.Response = actionContext.Request.CreateResponse(HttpStatusCode.BadRequest, new { status = 100, message = error.msg.ToString() });
                return;
            }
            else
            {
                base.OnActionExecuting(actionContext);
            }
        }
    }
}
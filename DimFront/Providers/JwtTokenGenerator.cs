using Common;
using DimFront.Providers;
using Microsoft.Owin.Security.DataHandler.Encoder;
using Models.DimFront;
using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens;
using System.Linq;
using System.Security.Claims;
using System.Web;
using Thinktecture.IdentityModel.Tokens;

namespace DimFront.Providers
{
    public static class JwtTokenGenerator
    {
        public static JsonWebToken Create(string sauidint, string saguid, string ppart, string host = "", string vpn = "", string uredis = "", string levno = "")
        {
            string audienceId = ConfigItems.AudienceId;
            var nowUtc = DateTime.UtcNow;
            var expires1 = nowUtc.AddMinutes(ConfigItems.DefaultJwtExpireInMin);
            var centuryBegin = new DateTime(1970, 1, 1).ToUniversalTime();
            var exp = (long)(new TimeSpan(expires1.Ticks - centuryBegin.Ticks).TotalSeconds);
            var iat = (long)(new TimeSpan(nowUtc.Ticks - centuryBegin.Ticks).TotalSeconds);

            var issued = DateTime.UtcNow;
            var expires = DateTime.UtcNow.AddMinutes(ConfigItems.DefaultJwtExpireInMin);

            string symmetricKeyAsBase64 = ConfigItems.AudienceSecret;

            var keyByteArray = TextEncodings.Base64Url.Decode(symmetricKeyAsBase64);
            var signingKey = new HmacSigningCredentials(keyByteArray);

            var identity = new ClaimsIdentity("JWT");
            identity.AddClaim(new Claim("saguid", saguid));
            identity.AddClaim(new Claim("sauidint", sauidint));
            //identity.AddClaim(new Claim("uguid", uguid));
            identity.AddClaim(new Claim("ppart", ppart));
            identity.AddClaim(new Claim("host", host));
            identity.AddClaim(new Claim("vpn", vpn));
            identity.AddClaim(new Claim("uredis", uredis));
            identity.AddClaim(new Claim("levno", levno));
            var jwt = new JwtSecurityToken(ConfigItems.JwtIssuer, audienceId, identity.Claims, issued, expires, signingKey);
            var handler = new JwtSecurityTokenHandler();
            var token = handler.WriteToken(jwt);
            return new JsonWebToken
            {
                AccessToken = token,
                Expires = exp
            };
        }
        public static int GetUid(string authHeader)
        {
            var handler = new JwtSecurityTokenHandler();
            var tokenS = handler.ReadToken(authHeader.Replace("bearer", "").Replace("Bearer", "").Trim()) as JwtSecurityToken;

            var unique_name = tokenS.Claims.First(claim => claim.Type == "sauidint").Value;

            return Convert.ToInt32(unique_name);
        }

        public static string GetSaGuid(string authHeader)
        {
            var handler = new JwtSecurityTokenHandler();
            var tokenS = handler.ReadToken(authHeader.Replace("bearer", "").Replace("Bearer", "").Trim()) as JwtSecurityToken;

            var unique_name = tokenS.Claims.First(claim => claim.Type == "saguid").Value;

            return unique_name;
        }
        public static bool Geturedis(string authHeader)
        {
            var handler = new JwtSecurityTokenHandler();
            var tokenS = handler.ReadToken(authHeader.Replace("bearer", "").Replace("Bearer", "").Trim()) as JwtSecurityToken;

            var unique_name = Convert.ToBoolean(tokenS.Claims.First(claim => claim.Type == "uredis").Value);

            return unique_name;
        }
        public static string GetSauid(string authHeader)
        {
            var handler = new JwtSecurityTokenHandler();
            var tokenS = handler.ReadToken(authHeader.Replace("bearer", "").Replace("Bearer", "").Trim()) as JwtSecurityToken;

            var unique_name = tokenS.Claims.First(claim => claim.Type == "sauidint").Value;

            return unique_name;
        }
        public static string Getppart(string authHeader)
        {
            var handler = new JwtSecurityTokenHandler();
            var tokenS = handler.ReadToken(authHeader.Replace("bearer", "").Replace("Bearer", "").Trim()) as JwtSecurityToken;

            var unique_name = tokenS.Claims.First(claim => claim.Type == "ppart").Value;

            return unique_name;
        }
        public static string getlevno(string authHeader)
        {
            var handler = new JwtSecurityTokenHandler();
            var tokenS = handler.ReadToken(authHeader.Replace("bearer", "").Replace("Bearer", "").Trim()) as JwtSecurityToken;

            var unique_name = tokenS.Claims.First(claim => claim.Type == "levno").Value;

            return unique_name;
        }
        public static string GetHost(string authHeader)
        {
            var handler = new JwtSecurityTokenHandler();
            var tokenS = handler.ReadToken(authHeader.Replace("bearer", "").Replace("Bearer", "").Trim()) as JwtSecurityToken;

            var unique_name = tokenS.Claims.First(claim => claim.Type == "host").Value;

            return unique_name;
        }
        public static string GetVpn(string authHeader)
        {
            var handler = new JwtSecurityTokenHandler();
            var tokenS = handler.ReadToken(authHeader.Replace("bearer", "").Replace("Bearer", "").Trim()) as JwtSecurityToken;

            var unique_name = tokenS.Claims.First(claim => claim.Type == "vpn").Value;

            return unique_name;
        }
        //public static int GetUid(string authHeader)
        //{
        //    var ec = ED.DecryptString(ConfigItems.FrontSecret, authHeader.Replace("Bearer", ""));
        //    var handler = new JwtSecurityTokenHandler();
        //    var tokenS = handler.ReadToken(ec.Replace("bearer", "").Replace("Bearer", "").Trim()) as JwtSecurityToken;

        //    var unique_name = tokenS.Claims.First(claim => claim.Type == "sauidint").Value;

        //    return Convert.ToInt32(unique_name);
        //}

        //public static string GetSaGuid(string authHeader)
        //{
        //    var ec = ED.DecryptString(ConfigItems.FrontSecret, authHeader.Replace("Bearer",""));
        //    var handler = new JwtSecurityTokenHandler();
        //    var tokenS = handler.ReadToken(ec.Replace("bearer", "").Replace("Bearer", "").Trim()) as JwtSecurityToken;

        //    var unique_name = tokenS.Claims.First(claim => claim.Type == "saguid").Value;

        //    return unique_name;
        //}
        //public static bool Geturedis(string authHeader)
        //{
        //    var ec = ED.DecryptString(ConfigItems.FrontSecret, authHeader.Replace("Bearer", ""));
        //    var handler = new JwtSecurityTokenHandler();
        //    var tokenS = handler.ReadToken(ec.Replace("bearer", "").Replace("Bearer", "").Trim()) as JwtSecurityToken;

        //    var unique_name = Convert.ToBoolean(tokenS.Claims.First(claim => claim.Type == "uredis").Value);

        //    return unique_name;
        //}
        //public static string GetSauid(string authHeader)
        //{
        //    var ec = ED.DecryptString(ConfigItems.FrontSecret, authHeader.Replace("Bearer", ""));
        //    var handler = new JwtSecurityTokenHandler();
        //    var tokenS = handler.ReadToken(ec.Replace("bearer", "").Replace("Bearer", "").Trim()) as JwtSecurityToken;

        //    var unique_name = tokenS.Claims.First(claim => claim.Type == "sauidint").Value;

        //    return unique_name;
        //}
        //public static string Getppart(string authHeader)
        //{
        //    var ec = ED.DecryptString(ConfigItems.FrontSecret, authHeader.Replace("Bearer", ""));
        //    var handler = new JwtSecurityTokenHandler();
        //    var tokenS = handler.ReadToken(ec.Replace("bearer", "").Replace("Bearer", "").Trim()) as JwtSecurityToken;

        //    var unique_name = tokenS.Claims.First(claim => claim.Type == "ppart").Value;

        //    return unique_name;
        //}
        //public static string getlevno(string authHeader)
        //{
        //    var ec = ED.DecryptString(ConfigItems.FrontSecret, authHeader.Replace("Bearer", ""));
        //    var handler = new JwtSecurityTokenHandler();
        //    var tokenS = handler.ReadToken(ec.Replace("bearer", "").Replace("Bearer", "").Trim()) as JwtSecurityToken;

        //    var unique_name = tokenS.Claims.First(claim => claim.Type == "levno").Value;

        //    return unique_name;
        //}
        //public static string GetHost(string authHeader)
        //{
        //    var ec = ED.DecryptString(ConfigItems.FrontSecret, authHeader.Replace("Bearer", ""));
        //    var handler = new JwtSecurityTokenHandler();
        //    var tokenS = handler.ReadToken(ec.Replace("bearer", "").Replace("Bearer", "").Trim()) as JwtSecurityToken;

        //    var unique_name = tokenS.Claims.First(claim => claim.Type == "host").Value;

        //    return unique_name;
        //}
        //public static string GetVpn(string authHeader)
        //{
        //    var ec = ED.DecryptString(ConfigItems.FrontSecret, authHeader.Replace("Bearer", ""));
        //    var handler = new JwtSecurityTokenHandler();
        //    var tokenS = handler.ReadToken(ec.Replace("bearer", "").Replace("Bearer", "").Trim()) as JwtSecurityToken;

        //    var unique_name = tokenS.Claims.First(claim => claim.Type == "vpn").Value;

        //    return unique_name;
        //}
        //public static RefreshToken Refresh(RefreshToken refresh)
        //{
        //    try
        //    {
        //        var principal = GetPrincipalFromExpiredToken(refresh.token);
        //        var username = principal.Identity.Name;
        //        //var savedRefreshToken = GetRefreshToken(username); //retrieve the refresh token from a data store
        //        //if (savedRefreshToken != refreshToken)
        //        //    throw new SecurityTokenException("Invalid refresh token");
        //        JwtPar jwtPar = new JwtPar();
        //        jwtPar.uguid = principal.Claims.Any(claim => claim.Type == "uguid") ? principal.Claims.First(claim => claim.Type == "uguid")?.Value : "";
        //        jwtPar.sauidint = principal.Claims.Any(claim => claim.Type == "sauidint") ? principal.Claims.First(claim => claim.Type == "sauidint").Value : "";
        //        jwtPar.saguid = principal.Claims.Any(claim => claim.Type == "saguid") ? principal.Claims.First(claim => claim.Type == "saguid").Value : "";
        //        jwtPar.ppart = principal.Claims.Any(claim => claim.Type == "ppart") ? principal.Claims.First(claim => claim.Type == "ppart").Value : "";
        //        jwtPar.host = principal.Claims.Any(claim => claim.Type == "host") ? principal.Claims.First(claim => claim.Type == "host").Value : "";
        //        jwtPar.vpn = principal.Claims.Any(claim => claim.Type == "vpn") ? principal.Claims.First(claim => claim.Type == "vpn").Value : "";
        //        jwtPar.uredis = principal.Claims.Any(claim => claim.Type == "uredis") ? principal.Claims.First(claim => claim.Type == "uredis").Value : "";
        //        jwtPar.levno = principal.Claims.Any(claim => claim.Type == "levno") ? principal.Claims.First(claim => claim.Type == "levno").Value : "";
        //        jwtPar.boutypeid = principal.Claims.Any(claim => claim.Type == "boutypeid") ? principal.Claims.First(claim => claim.Type == "boutypeid").Value : "";
        //        jwtPar.scode = principal.Claims.Any(claim => claim.Type == "scode") ? principal.Claims.First(claim => claim.Type == "scode").Value : "";
        //        var newJwtToken = Create(jwtPar);
        //        var newRefreshToken = GenerateRefreshToken();
        //        //DeleteRefreshToken(username, refreshToken);
        //        //SaveRefreshToken(username, newRefreshToken);
        //        return new RefreshToken
        //        {
        //            id = 1,
        //            token = newJwtToken.AccessToken,
        //            rt = newRefreshToken
        //        };
        //    }
        //    catch (Exception ex)
        //    {
        //        return new RefreshToken
        //        {
        //            id = 0,
        //            token = "",
        //            rt = ""
        //        };
        //    }
        //}
    }
}
using System;
using System.Configuration;

namespace Common
{
    public class ConfigItems
    {
        public static string Conn_AccD
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["Conn_AccD"].ToString();
            }
        }
        public static string Conn_Casinogroup
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["Conn_Casinogroup"].ToString();
            }
        }
        public static string Conn_AccDgroup
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["Conn_AccDgroup"].ToString();
            }
        }
        public static string Conn_Betgroup
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["Conn_Betgroup"].ToString();
            }
        }
        public static string Conn_Loggameodds
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["Conn_Loggameodds"].ToString();
            }
        }
        public static string Conn_PgAdmin
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["Conn_PgAdmin"].ToString();
            }
        }
        public static string Conn_LogD
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["Conn_LogD"].ToString();
            }
        }
        public static string Conn_Test3db
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["Conn_Test3db"].ToString();
            }
        }
        public static Boolean LogFlag
        {
            get
            {
                return Convert.ToBoolean(ConfigurationManager.AppSettings["LogFlag"].ToString());
            }
        }
        public static string Logurl
        {
            get
            {
                return ConfigurationManager.AppSettings["Logurl"].ToString();
            }
        }
        public static string Logurlwa
        {
            get
            {
                return ConfigurationManager.AppSettings["Logurlwa"].ToString();
            }
        }
        public static string Conn_Casino
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["Conn_Casino"].ToString();
            }
        }
        public static string Conn_Odds
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["Conn_Odds"].ToString();
            }
        }
        public static int Redismain
        {
            get
            {
                return Convert.ToInt32(ConfigurationManager.AppSettings["Redismain"].ToString());
            }
        }
        public static Boolean isfixture
        {
            get
            {
                return Convert.ToBoolean(ConfigurationManager.AppSettings["isfixture"].ToString());
            }
        }
        public static string Conn_Bet
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["Conn_Bet"].ToString();
            }
        }
        public static string Postgres
        {
            get
            {
                return ConfigurationManager.AppSettings["Postgres"].ToString();
            }
        }
        public static string Conn_CasinoFancy
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["Conn_CasinoFancy"].ToString();
            }
        }
        public static string Conn_CasinoFancygroup
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["Conn_CasinoFancygroup"].ToString();
            }
        }
        public static string Conn_Fancy
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["Conn_Fancy"].ToString();
            }
        }
        public static string Conn_Fancygroup
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["Conn_Fancygroup"].ToString();
            }
        }
        public static string Conn_worlic
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["Conn_worlic"].ToString();
            }
        }
        public static string Conn_CasinoMaster
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["Conn_CasinoMaster"].ToString();
            }
        }
        public static string Conn_CasinoM
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["Conn_CasinoM"].ToString();
            }
        }
        public static string Conn_CasinoMgroup
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["Conn_CasinoMgroup"].ToString();
            }
        }
        public static string Conn_Daba
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["Conn_Daba"].ToString();
            }
        }
        public static string Conn_Dream
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["Conn_Dream"].ToString();
            }
        }
        public static string Conn_CasinoTP
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["Conn_CasinoTP"].ToString();
            }
        }
        public static string Conn_LogA
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["Conn_LogA"].ToString();
            }
        }

        public static Boolean ErrorLog
        {
            get
            {
                return Convert.ToBoolean(ConfigurationManager.AppSettings["ErrorLog"].ToString());
            }
        }
        public static Boolean AllLog
        {
            get
            {
                return Convert.ToBoolean(ConfigurationManager.AppSettings["AllLog"].ToString());
            }
        }
        public static string JwtIssuer
        {
            get
            {
                return ConfigurationManager.AppSettings["JwtIssuer"].ToString();
            }
        }
        public static string AudienceId
        {
            get
            {
                return ConfigurationManager.AppSettings["AudienceId"].ToString();
            }
        }
        public static string AudienceSecret
        {
            get
            {
                return ConfigurationManager.AppSettings["AudienceSecret"].ToString();
            }
        }
        public static int DefaultJwtExpireInMin
        {
            get
            {
                return Convert.ToInt32(ConfigurationManager.AppSettings["DefaultJwtExpireInMin"]);
            }
        }
        public static string RedisServer
        {
            get
            {
                return ConfigurationManager.AppSettings["RedisServer"].ToString();
            }
        }
        public static int RedisDataBase
        {
            get
            {
                return Convert.ToInt32(ConfigurationManager.AppSettings["RedisDataBase"].ToString());
            }
        }
        public static string[] ExWords
        {
            get
            {
                return ConfigurationManager.AppSettings["ExWords"].ToString().Split(',');
            }
        }
        public static Boolean SqlIP
        {
            get
            {
                return Convert.ToBoolean(ConfigurationManager.AppSettings["SqlIP"].ToString());
            }
        }
        public static Boolean Domain
        {
            get
            {
                return Convert.ToBoolean(ConfigurationManager.AppSettings["Domain"].ToString());
            }
        }
        public static Boolean IP_API
        {
            get
            {
                return Convert.ToBoolean(ConfigurationManager.AppSettings["IP_API"].ToString());
            }
        }
        public static Boolean Vpn
        {
            get
            {
                return Convert.ToBoolean(ConfigurationManager.AppSettings["Vpn"].ToString());
            }
        }
        public static Boolean Hosting
        {
            get
            {
                return Convert.ToBoolean(ConfigurationManager.AppSettings["Hosting"].ToString());
            }
        }
        public static string IPUrl
        {
            get
            {
                return ConfigurationManager.AppSettings["IPUrl"].ToString();
            }
        }
        public static string FrontSecret
        {
            get
            {
                return ConfigurationManager.AppSettings["FrontSecret"].ToString();
            }
        }
        public static string PaymentSecret
        {
            get
            {
                return ConfigurationManager.AppSettings["PaymentSecret"].ToString();
            }
        }
        public static string RedisUrl
        {
            get
            {
                return ConfigurationManager.AppSettings["RedisUrl"].ToString();
            }
        }



        ////////////////////////////
        public static string RedisLocal
        {
            get
            {
                return ConfigurationManager.AppSettings["RedisLocal"].ToString();
            }
        }
        public static int RedisLocaldb
        {
            get
            {
                return Convert.ToInt32(ConfigurationManager.AppSettings["RedisLocaldb"].ToString());
            }
        }
        public static int RedisLocaldbclist
        {
            get
            {
                return Convert.ToInt32(ConfigurationManager.AppSettings["RedisLocaldbclist"].ToString());
            }
        }
        public static int RedisCasino
        {
            get
            {
                return Convert.ToInt32(ConfigurationManager.AppSettings["RedisCasino"].ToString());
            }
        }
        public static string secretKey
        {
            get
            {
                return ConfigurationManager.AppSettings["secretKey"].ToString();
            }
        }
        public static string issuer
        {
            get
            {
                return ConfigurationManager.AppSettings["issuer"].ToString();
            }
        }
        public static int expiryMinutes
        {
            get
            {
                return Convert.ToInt32(ConfigurationManager.AppSettings["expiryMinutes"].ToString());
            }
        }
        public static string CFUrlApi
        {
            get
            {
                return ConfigurationManager.AppSettings["CFUrlApi"].ToString();
            }
        }
        public static string QTUrlApi
        {
            get
            {
                return ConfigurationManager.AppSettings["QTUrlApi"].ToString();
            }
        }
        public static string SSUrlApi
        {
            get
            {
                return ConfigurationManager.AppSettings["SSUrlApi"].ToString();
            }
        }
        public static string RNUrlApi
        {
            get
            {
                return ConfigurationManager.AppSettings["RNUrlApi"].ToString();
            }
        }
        public static string Popcid
        {
            get
            {
                return ConfigurationManager.AppSettings["Popcid"].ToString();
            }
        }
        public static string Secret
        {
            get
            {
                return ConfigurationManager.AppSettings["Secret"].ToString();
            }
        }
        public static string Ludocid
        {
            get
            {
                return ConfigurationManager.AppSettings["Ludocid"].ToString();
            }
        }
        public static string LudoSecret
        {
            get
            {
                return ConfigurationManager.AppSettings["LudoSecret"].ToString();
            }
        }
        public static string TeleUrl
        {
            get
            {
                return ConfigurationManager.AppSettings["TeleUrl"].ToString();
            }
        }
        public static string Popball
        {
            get
            {
                return ConfigurationManager.AppSettings["Popball"].ToString();
            }
        }
        public static string Ludo
        {
            get
            {
                return ConfigurationManager.AppSettings["Ludo"].ToString();
            }
        }
        public static string RedisUrlC
        {
            get
            {
                return ConfigurationManager.AppSettings["RedisUrlC"].ToString();
            }
        }
        public static string RedisUrlV
        {
            get
            {
                return ConfigurationManager.AppSettings["RedisUrlV"].ToString();
            }
        }
        public static string Urlsc
        {
            get
            {
                return ConfigurationManager.AppSettings["Urlsc"].ToString();
            }
        }
        public static string TVUrl
        {
            get
            {
                return ConfigurationManager.AppSettings["TVUrl"].ToString();
            }
        }
        public static string CCode
        {
            get
            {
                return ConfigurationManager.AppSettings["CCode"].ToString();
            }
        }
        public static string RedisTrader
        {
            get
            {
                return ConfigurationManager.AppSettings["RedisTrader"].ToString();
            }
        }
        public static int RedisCommon
        {
            get
            {
                return Convert.ToInt32(ConfigurationManager.AppSettings["RedisCommon"].ToString());
            }
        }
        public static int Redisudb
        {
            get
            {
                return Convert.ToInt32(ConfigurationManager.AppSettings["Redisudb"].ToString());
            }
        }
        public static int re
        {
            get
            {
                return Convert.ToInt32(ConfigurationManager.AppSettings["re"].ToString());
            }
        }
        public static int Redisfixstr
        {
            get
            {
                return Convert.ToInt32(ConfigurationManager.AppSettings["Redisfixstr"].ToString());
            }
        }
        public static int RedisAuthdb
        {
            get
            {
                return Convert.ToInt32(ConfigurationManager.AppSettings["RedisAuthdb"].ToString());
            }
        }
        public static string Conn_Redis
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["Conn_Redis"].ToString();
            }
        }
        public static string RedisBeepPanel
        {
            get
            {
                return ConfigurationManager.AppSettings["RedisBeepPanel"].ToString();
            }
        }
        public static string RedisServerSta
        {
            get
            {
                return ConfigurationManager.AppSettings["RedisServerSta"].ToString();
            }
        }
        public static string[] RedisServerSta1
        {
            get
            {
                return ConfigurationManager.AppSettings["RedisServerSta1"].ToString().Split(',');
            }
        }
        public static string[] RedisServerCric
        {
            get
            {
                return ConfigurationManager.AppSettings["RedisServerCric"].ToString().Split(',');
            }
        }
        public static string[] RedisServerFoot
        {
            get
            {
                return ConfigurationManager.AppSettings["RedisServerFoot"].ToString().Split(',');
            }
        }
        public static string[] RedisServerTenni
        {
            get
            {
                return ConfigurationManager.AppSettings["RedisServerTenni"].ToString().Split(',');
            }
        }
        public static string[] RedisServerOther
        {
            get
            {
                return ConfigurationManager.AppSettings["RedisServerOther"].ToString().Split(',');
            }
        }
        public static string Redd = "1";
        public static int RedisStaC
        {
            get
            {
                return Convert.ToInt32(ConfigurationManager.AppSettings["RedisStaC"].ToString());
            }
        }
        public static int Rediscdb
        {
            get
            {
                return Convert.ToInt32(ConfigurationManager.AppSettings["Rediscdb"].ToString());
            }
        }
        public static string TGSUrlApi
        {
            get
            {
                return ConfigurationManager.AppSettings["TGSUrlApi"].ToString();
            }
        }
        public static string Urlpop
        {
            get
            {
                return ConfigurationManager.AppSettings["Urlpop"].ToString();
            }
        }
        public static string CopUrl
        {
            get
            {
                return ConfigurationManager.AppSettings["CopUrl"].ToString();
            }
        }
        public static bool Encrypt
        {
            get
            {
                return Convert.ToBoolean(ConfigurationManager.AppSettings["Encrypt"].ToString());
            }
        }
        public static string TPUrl
        {
            get
            {
                return ConfigurationManager.AppSettings["TPUrl"].ToString();
            }
        }
        public static string CommonTPUrl
        {
            get
            {
                return ConfigurationManager.AppSettings["CommonTPUrl"];
            }
        }
    }
}

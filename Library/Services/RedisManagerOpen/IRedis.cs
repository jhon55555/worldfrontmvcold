using System.Collections.Generic;
using System.Threading.Tasks;

namespace Services.RedisManagerOpen
{
    public interface IRedis
    {
        Task<string> GetSetMembersstr(string key, int database, int re);
        bool IsExist(string key, int database = 0);
    }
}

using System.Collections.Generic;
using System.Threading.Tasks;

namespace Services.CacheManager
{
    public interface IRedis1
    {
        //IList<T> GetAllkeysData<T>(string pattern, int database = 1, int serverr = 1);
        Task<IEnumerable<T>> GetSetMembers<T>(string key, int database, int re);
        Task<string> GetSetMembersstr(string key, int database, int re);
        //void Flush(string db);
        bool IsExist(string key, int database = 0);
    }
}

using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Services.RedisManager
{
    public static class Extensions
    {
        public static async Task<T> GetAsync<T>(this IDatabaseAsync db, string key)
        {
            var data = await db.StringGetAsync(key);
            return JsonConvert.DeserializeObject<T>(data);
        }

        public static async Task<IEnumerable<T>> MGetAsync<T>(this IDatabaseAsync db, IEnumerable<RedisKey> keys)
        {
            var data = await db.StringGetAsync(keys.Select(x => (RedisKey)x.ToString()).ToArray());
            return data.Select(x => JsonConvert.DeserializeObject<T>(x)).ToArray();
        }

        public static async Task<IEnumerable<T>> MGetAsync<T>(this IDatabaseAsync db, IEnumerable<string> keys)
        {
            var data = await db.StringGetAsync(keys.Select(x => (RedisKey)x).ToArray());
            return data.Select(x => JsonConvert.DeserializeObject<T>(x)).ToArray();
        }

    }
}

namespace Common
{
    public static class GlobalCacheKey
    {
        public readonly static string GameMatchData = "dim.gamematch.data-{0}";
        public readonly static string UserIdleCacheKey = "dim.user.idle.key-{0}";
        public readonly static string UserBlockSessionKey = "dim.user.block.accesstoken-{0}-{1}";
        public readonly static string UserAccessToken = "dim.user.accesstoken-{0}";
        public readonly static string UserTableToken = "dim.user.tabletoken-{0}";
        public readonly static string UserTempAccessToken = "dim.usertemp.accesstoken-{0}";

        public readonly static string AdminGameMatchData = "dim.admin.gamematch.data-{0}";
        public readonly static string AdminAccessToken = "dim.admin.accesstoken-{0}";
        public readonly static string AdminIdleCacheKey = "dim.admin.idle.key-{0}";
        public readonly static string AdminBlockSessionKey = "dim.admin.block.accesstoken-{0}-{1}";
        public readonly static string AdminSubAccessToken = "dim.adminsub.accesstoken-{0}";


        /// Game Match data keys
        public readonly static string RedisCacheKey = "game.market.key.data-{0}-{1}";
        public readonly static string RedisCacheKeySession = "game.session.market.key.data-{0}-{1}";
        public readonly static string RedisBookMakerGame = "game.bookmaker.key.data-{0}-{1}";
        public readonly static string RedisCacheKeyFancy1 = "game.fancy1.market.key.data-{0}-{1}";
        public readonly static string RedisCacheKeyStock = "dim.data.share.key";

        //void RemoveByPatterns(int database, string pattern)
        public readonly static string RedisCacheKeyUserData = "dim.userdata.key-{0}>{1}";
        public readonly static string Set = "Set_";
    }
}

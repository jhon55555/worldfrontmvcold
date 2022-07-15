namespace Models.DimFront
{
    public class From
    {
        public int id { get; set; }
        public bool is_bot { get; set; }
        public string first_name { get; set; }
        public string username { get; set; }
    }

    public class Chat
    {
        public int id { get; set; }
        public string first_name { get; set; }
        public string type { get; set; }
    }

    public class Result
    {
        public int message_id { get; set; }
        public From from { get; set; }
        public Chat chat { get; set; }
        public int date { get; set; }
        public string text { get; set; }
    }

    public class TeleResp
    {
        public bool ok { get; set; }
        public Result result { get; set; }
        public int error_code { get; set; }
        public string description { get; set; }
    }
}

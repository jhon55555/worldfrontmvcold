using System;
using System.Collections.Generic;
using System.Text;

namespace Models.DimFrontGroup
{
    // Root myDeserializedClass = JsonConvert.DeserializeObject<Root>(myJsonResponse); 
    public class Child2
    {
        public String gmid { get; set; }
        public String name { get; set; }
        public Int64 etid { get; set; }
        public int iscc { get; set; }
    }

    public class Child
    {
        public String cid { get; set; }
        public String name { get; set; }
        public List<Child2> children { get; set; }

    }

    public class T3
    {
        public String etid { get; set; }
        public String name { get; set; }
       
        public List<Child> children { get; set; }

    }

    public class Data2
    {
        public List<T3> t1 { get; set; }

    }

    public class TreeviewDataRes
    {
        public int status { get; set; }
        public String msg { get; set; }
        public Data2 data { get; set; }

    }



}

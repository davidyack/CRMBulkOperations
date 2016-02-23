using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ctccrm.ServerCommon.OrgServiceHelpers
{
    public enum CrmBulkOpStatusType
    {
        OperationStart,
        OperationEnd,
        BatchStart,
        BatchEnd,
        ThreadStart,
        ThreadEnd
    };
    public class CrmBulkOpStatus
    {
        public CrmBulkOpStatusType Type { get; set; }

        public string Message { get; set; }

        public long ElapsedMS { get; set; }

        public int ThreadID { get; set; }

        public int Count { get; set; }

    }
}

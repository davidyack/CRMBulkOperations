using ctccrm.ServerCommon.OrgServiceHelpers;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Tooling.Connector;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CTCImportSpeedTest
{
    class Program
    {
        static void Main(string[] args)
        {
            int TotalRecordsToProcess = 1000;
            int ThreadsToUse = 2;
            int BatchSize = 50;

            var connectionString = ConfigurationManager.ConnectionStrings["CRM"].ConnectionString;
            

            List<IOrganizationService> services = new List<IOrganizationService>();

            for (int i = 0; i < ThreadsToUse; i++)
            {
                CrmServiceClient connection = new CrmServiceClient(connectionString);
                IOrganizationService service = connection.OrganizationServiceProxy as IOrganizationService;
                services.Add(service);
            }

            CrmBulkServiceManager mgr = new CrmBulkServiceManager(services,statusAction:LogProgress);

            List<Entity> entityList = new List<Entity>();
            for (int i = 0; i < TotalRecordsToProcess; i++)
            {
                Entity entity = new Entity("account");
                entity["name"] = "account " + DateTime.Now.ToString();
                entityList.Add(entity);
            }
            Stopwatch sw = new Stopwatch();
            sw.Start();
            var results = mgr.BulkInsert(entityList, batchSize: BatchSize);
            sw.Stop();
            Console.WriteLine("Import took " + sw.ElapsedMilliseconds / 1000 + " seconds ");

            List<EntityReference> refList = new List<EntityReference>();
            foreach(var result in results.ResultItems)
            {
                var refItem = new EntityReference("account", result.ItemID);
                refList.Add(refItem);
            }
            Console.WriteLine("Cleanup Starting");
           // mgr.BulkDelete(refList,batchSize:50);
            Console.WriteLine("Cleanup completed");

        }
        private static void LogProgress(CrmBulkOpStatus status)
        {
            if (status.Type == CrmBulkOpStatusType.ThreadStart)
                Console.WriteLine("   Thread Start({0}) {1} - Count is {2}", status.ThreadID, status.Message, status.Count);
            if (status.Type == CrmBulkOpStatusType.ThreadEnd)
                Console.WriteLine("   Thread End  ({0}) {1} - Elappsed is {2} ", status.ThreadID, status.Message, status.ElapsedMS);
            if (status.Type == CrmBulkOpStatusType.BatchStart)
                Console.WriteLine("Batch  Start({0}) {1} ", status.ThreadID, status.Message);
            if (status.Type == CrmBulkOpStatusType.BatchEnd)
                Console.WriteLine("Batch  End  ({0}) {1} - Elappsed is {2} ", status.ThreadID, status.Message, status.ElapsedMS);
        }
    }
}

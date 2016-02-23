﻿using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Client;
using Microsoft.Xrm.Client.Services;
using ctccrm.ServerCommon.OrgServiceHelpers;
using Microsoft.Xrm.Sdk;
using System.Collections.Generic;
using Microsoft.Crm.Sdk.Messages;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Query;

namespace CTCBulkOpLibTests
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void BasicRunMultipleTest()
        {
            ExecuteBasicRunMultipleTest();
        }
        [TestMethod]
        public void BasicRunMultipleTestTModeSingle()
        {
            ExecuteBasicRunMultipleTest(transactionMode: CTCBulkTransactionMode.Single);
        }
        [TestMethod]
        public void BasicRunMultipleTestTModeBatch()
        {
            ExecuteBasicRunMultipleTest(transactionMode: CTCBulkTransactionMode.Batch);
        }

        private static void ExecuteBasicRunMultipleTest(int batchSize=10,CTCBulkTransactionMode transactionMode= CTCBulkTransactionMode.None)
        {
            CrmConnection c = new CrmConnection("CRM");
            OrganizationService service = new OrganizationService(c);
            CrmBulkServiceManager mgr = new CrmBulkServiceManager(service);

            List<OrganizationRequest> requests = new List<OrganizationRequest>();
            for (int i = 0; i < 100; i++)
            {
                WhoAmIRequest req = new WhoAmIRequest();
                requests.Add(req);
            }

            var results = mgr.RunMultipleRequests(requests, batchSize: batchSize, transactionMode:transactionMode);
        }

       


        [TestMethod]
        public void ExcessiveBatchSizeRunMultipleTest()
        {
            ExecuteExcessiveBatchSize();
        }
        [TestMethod]
        public void ExcessiveBatchSizeRunMultipleTModeSingle()
        {
            bool singleFailed = false;
            try
            {
                ExecuteExcessiveBatchSize(transactionMode: CTCBulkTransactionMode.Single);
            }
            catch(Exception ex)
            {
                if (ex.Message.Contains("batch size exceeds"))
                    singleFailed = true;
            }

            Assert.IsTrue(singleFailed);

        }
        [TestMethod]
        public void ExcessiveBatchSizeRunMultipleTModeBatch()
        {
            ExecuteExcessiveBatchSize(transactionMode: CTCBulkTransactionMode.Batch);
        }

        private static void ExecuteExcessiveBatchSize(CTCBulkTransactionMode transactionMode = CTCBulkTransactionMode.None)
        {
            CrmConnection c = new CrmConnection("CRM");
            OrganizationService service = new OrganizationService(c);
            CrmBulkServiceManager mgr = new CrmBulkServiceManager(service);

            List<OrganizationRequest> requests = new List<OrganizationRequest>();
            for (int i = 0; i < 1100; i++)
            {
                WhoAmIRequest req = new WhoAmIRequest();
                requests.Add(req);
            }

            var results = mgr.RunMultipleRequests(requests, batchSize: 1100,transactionMode:transactionMode);
        }

        [TestMethod]
        public void BasicRunMultipleTestParallelExecute()
        {
            CrmConnection c = new CrmConnection("CRM");
            
            List<IOrganizationService> services = new List<IOrganizationService>();
            
            for (int i = 0; i < 10; i++)
            {
                OrganizationService service = new OrganizationService(c);
                services.Add(service);
            }


            CrmBulkServiceManager mgr = new CrmBulkServiceManager(services);

            List<OrganizationRequest> requests = new List<OrganizationRequest>();
            for (int i = 0; i < 100; i++)
            {
                WhoAmIRequest req = new WhoAmIRequest();
                requests.Add(req);
            }

            var results = mgr.RunMultipleRequests(requests, batchSize: 1);

        }

        [TestMethod]
        public void ConccurentRunMultipleTest()
        {
            CrmConnection c = new CrmConnection("CRM");
            OrganizationService service = new OrganizationService(c);
            RunWhoAmITest(service);
            Parallel.Invoke(() => RunWhoAmITest(service), () => RunWhoAmITest(service), () => RunWhoAmITest(service),
                () => RunWhoAmITest(service), () => RunWhoAmITest(service), () => RunWhoAmITest(service), () => RunWhoAmITest(service), () => RunWhoAmITest(service),
                () => RunWhoAmITest(service), () => RunWhoAmITest(service), () => RunWhoAmITest(service), () => RunWhoAmITest(service), () => RunWhoAmITest(service)
                );

        }

        [TestMethod]
        public void ConccurentRunMultipleTes2t()
        {
            CrmConnection c = new CrmConnection("CRM");
            OrganizationService service = new OrganizationService(c);
            RunWhoAmITest(service);
            Parallel.Invoke(() => RunBulkInsertTest(service), () => RunBulkInsertTest(service), () => RunBulkInsertTest(service),
                () => RunBulkInsertTest(service), () => RunBulkInsertTest(service), () => RunBulkInsertTest(service), () => RunBulkInsertTest(service), () => RunBulkInsertTest(service),
                () => RunBulkInsertTest(service), () => RunBulkInsertTest(service), () => RunBulkInsertTest(service), () => RunBulkInsertTest(service), () => RunBulkInsertTest(service)
                );

        }

        private static void RunWhoAmITest(IOrganizationService service)
        {

            CrmBulkServiceManager mgr = new CrmBulkServiceManager(service);

            List<OrganizationRequest> requests = new List<OrganizationRequest>();
            for (int i = 0; i < 50; i++)
            {
                WhoAmIRequest req = new WhoAmIRequest();
                requests.Add(req);
            }

            var results = mgr.RunMultipleRequests(requests, batchSize: 5);
        }
        private static void RunBulkInsertTest(IOrganizationService service,int recordsToInsert=50)
        {

            CrmBulkServiceManager mgr = new CrmBulkServiceManager(service);

            List<Entity> entityList = new List<Entity>();
            for (int i = 0; i < recordsToInsert; i++)
            {
                Entity entity = new Entity("account");
                entity["name"] = "account " + DateTime.Now.ToString();
                entityList.Add(entity);
            }

            var results = mgr.BulkInsert(entityList, batchSize: 1000);
        }

        [TestMethod]
        public void BulkInsertTest1()
        {
            ExecuteBulkInsertTest();

        }
        [TestMethod]
        public void BulkInsertTest1TModeSingle()
        {
            ExecuteBulkInsertTest(transactionMode: CTCBulkTransactionMode.Single);

        }
        [TestMethod]
        public void BulkInsertTest1TModeBatch()
        {
            ExecuteBulkInsertTest(transactionMode: CTCBulkTransactionMode.Batch);

        }

        private static void ExecuteBulkInsertTest(CTCBulkTransactionMode transactionMode = CTCBulkTransactionMode.None)
        {
            CrmConnection c = new CrmConnection("CRM");
            OrganizationService service = new OrganizationService(c);
            CrmBulkServiceManager mgr = new CrmBulkServiceManager(service);

            List<Entity> entityList = new List<Entity>();
            for (int i = 0; i < 10; i++)
            {
                Entity entity = new Entity("account");
                entity["name"] = "account " + DateTime.Now.ToString();
                entityList.Add(entity);
            }

            var results = mgr.BulkInsert(entityList,transactionMode:transactionMode);
        }



        [TestMethod]
        public void BulkInsertTest2()
        {
            CrmConnection c = new CrmConnection("CRM");
            List<IOrganizationService> services = new List<IOrganizationService>();

            for (int i = 0; i < 4; i++)
            {
                OrganizationService service = new OrganizationService(c);
                services.Add(service);
            }

            CrmBulkServiceManager mgr = new CrmBulkServiceManager(services);

            List<Entity> entityList = new List<Entity>();
            for (int i = 0; i < 480; i++)
            {
                Entity entity = new Entity("account");
                entity["name"] = "account " + DateTime.Now.ToString();
                entityList.Add(entity);
            }

            var results = mgr.BulkInsert(entityList,batchSize:120);

        }

        [TestMethod]
        public void BulkUpdateTest1()
        {
            CrmConnection c = new CrmConnection("CRM");
            OrganizationService service = new OrganizationService(c);
            CrmBulkServiceManager mgr = new CrmBulkServiceManager(service);

            List<Entity> entityList = new List<Entity>();
            for (int i = 0; i < 2; i++)
            {
                Entity entity = new Entity("account");
                entity["name"] = "account " + DateTime.Now.ToString();
                entityList.Add(entity);
            }

            var results = mgr.BulkInsert(entityList);
            entityList.Clear();
            foreach (var item in results.ResultItems)
            {
                Entity entity = new Entity("account");
                entity.Id = item.ItemID;
                entity["name"] = "account " + DateTime.Now.ToString();
                entityList.Add(entity);
            }

            results = mgr.BulkUpdate(entityList);


        }

        [TestMethod]
        public void BulkUpsertTestInsert()
        {
            CrmConnection c = new CrmConnection("CRM");
            OrganizationService service = new OrganizationService(c);
            CrmBulkServiceManager mgr = new CrmBulkServiceManager(service);

            List<Entity> entityList = new List<Entity>();
            for (int i = 0; i < 10; i++)
            {
                Entity entity = new Entity("account");
                entity["name"] = "account " + DateTime.Now.ToString();
                entityList.Add(entity);
            }

            var results = mgr.BulkUpdate(entityList,useUpsert:true);


        }

        [TestMethod]
        public void BulkUpsertTestInsertAndUpdate()
        {
            CrmConnection c = new CrmConnection("CRM");
            OrganizationService service = new OrganizationService(c);
            CrmBulkServiceManager mgr = new CrmBulkServiceManager(service);

            List<Entity> entityList = new List<Entity>();
            for (int i = 0; i < 10; i++)
            {
                Entity entity = new Entity("account");
                entity["name"] = "account " + DateTime.Now.ToString();
                entityList.Add(entity);
            }

            var resultsInsert = mgr.BulkInsert(entityList);
            entityList.Clear();
            foreach (var item in resultsInsert.ResultItems)
            {
                Entity entity = new Entity("account");
                entity.Id = item.ItemID;
                entity["name"] = "account " + DateTime.Now.ToString();
                entityList.Add(entity);
            }
            
            for (int i = 0; i < 10; i++)
            {
                Entity entity = new Entity("account");
                entity["name"] = "account " + DateTime.Now.ToString();
                entityList.Add(entity);
            }

            var results = mgr.BulkUpdate(entityList, useUpsert: true);


        }

        [TestMethod]
        public void BulkDeleteTest1()
        {
            CrmConnection c = new CrmConnection("CRM");
            OrganizationService service = new OrganizationService(c);
            CrmBulkServiceManager mgr = new CrmBulkServiceManager(service);

            List<Entity> entityList = new List<Entity>();

            QueryExpression q = new QueryExpression("account");
            q.ColumnSet = new ColumnSet();
            q.ColumnSet.AddColumn("accountid");
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition("name", ConditionOperator.BeginsWith, "bulk updated");

            var results = mgr.BulkDelete(q, "Bulk Delete Test" + DateTime.Now.ToString());

        }
        [TestMethod]
        public void BulkUpdateQueryTest1()
        {
            CrmConnection c = new CrmConnection("CRM");
            OrganizationService service = new OrganizationService(c);
            CrmBulkServiceManager mgr = new CrmBulkServiceManager(service);

            List<Entity> entityList = new List<Entity>();

            QueryExpression q = new QueryExpression("account");
            q.ColumnSet = new ColumnSet();
            q.ColumnSet.AddColumn("accountid");
            Entity entityData = new Entity("account");
            entityData["name"] = "bulk updated " + DateTime.Now.ToString();

            var results = mgr.BulkUpdate(q, entityData);

        }

        [TestMethod]
        public void BulkSetStateTest1()
        {
            CrmConnection c = new CrmConnection("CRM");
            OrganizationService service = new OrganizationService(c);
            CrmBulkServiceManager mgr = new CrmBulkServiceManager(service);

            List<Entity> entityList = new List<Entity>();
            for (int i = 0; i < 10; i++)
            {
                Entity entity = new Entity("account");
                entity["name"] = "account " + DateTime.Now.ToString();
                entityList.Add(entity);
            }

            var results = mgr.BulkInsert(entityList);
            entityList.Clear();
            foreach (var item in results.ResultItems)
            {
                Entity entity = new Entity("account");
                entity.Id = item.ItemID;
                entity["name"] = "account " + DateTime.Now.ToString();
                entityList.Add(entity);
            }

            results = mgr.BulkSetState(entityList, new OptionSetValue(1), new OptionSetValue(2));


        }

        [TestMethod]
        public void BulkSetStateQueryTest1()
        {
            CrmConnection c = new CrmConnection("CRM");
            OrganizationService service = new OrganizationService(c);
            CrmBulkServiceManager mgr = new CrmBulkServiceManager(service);

            List<Entity> entityList = new List<Entity>();

            QueryExpression q = new QueryExpression("account");
            q.ColumnSet = new ColumnSet();
            q.ColumnSet.AddColumn("accountid");
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition("statecode", ConditionOperator.Equal, 1);

            var results = mgr.BulkSetState(q, new OptionSetValue(0), new OptionSetValue(1));


        }

        [TestMethod]
        public void BulkRunWorkflowTest1()
        {
            CrmConnection c = new CrmConnection("CRM");
            OrganizationService service = new OrganizationService(c);
            CrmBulkServiceManager mgr = new CrmBulkServiceManager(service);

            List<Entity> entityList = new List<Entity>();
            for (int i = 0; i < 10; i++)
            {
                Entity entity = new Entity("account");
                entity["name"] = "account " + DateTime.Now.ToString();
                entityList.Add(entity);
            }

            var results = mgr.BulkInsert(entityList);
            entityList.Clear();
            foreach (var item in results.ResultItems)
            {
                Entity entity = new Entity("account");
                entity.Id = item.ItemID;
                entity["name"] = "account " + DateTime.Now.ToString();
                entityList.Add(entity);
            }

            var workflowID = GetWorkflowID(service, "account", "UnitTest Account");

            results = mgr.BulkRunWorkflow(entityList, workflowID);


        }

        [TestMethod]
        public void BulkRunWorkflowQueryTest1()
        {
            CrmConnection c = new CrmConnection("CRM");
            OrganizationService service = new OrganizationService(c);
            CrmBulkServiceManager mgr = new CrmBulkServiceManager(service);

            List<Entity> entityList = new List<Entity>();

            QueryExpression q = new QueryExpression("account");
            q.ColumnSet = new ColumnSet();
            q.ColumnSet.AddColumn("accountid");
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition("statecode", ConditionOperator.Equal, 0);

            var workflowID = GetWorkflowID(service, "account", "UnitTest Account");

            var results = mgr.BulkRunWorkflow(q, workflowID);


        }

        private Guid GetWorkflowID(IOrganizationService service, string entityName, string workflowName)
        {
            QueryExpression qe = new QueryExpression();
            string[] colList = { "workflowid", "name" };
            qe.ColumnSet = new ColumnSet(colList);
            qe.Distinct = false;
            qe.EntityName = "workflow";
            FilterExpression exp = new FilterExpression();
            exp.FilterOperator = LogicalOperator.And;

            //Entity Name must match
            ConditionExpression condPrimaryEntity = new ConditionExpression();
            condPrimaryEntity.AttributeName = "primaryentity";
            object[] valueList = { entityName };

            condPrimaryEntity.Values.AddRange(valueList);
            condPrimaryEntity.Operator = ConditionOperator.Equal;
            exp.Conditions.Add(condPrimaryEntity);

            //They must be marked as on demand
            ConditionExpression condOnDemand = new ConditionExpression();
            condOnDemand.AttributeName = "ondemand";
            bool isOnDemand = true;
            object[] valueList2 = { isOnDemand };
            //condOnDemand.Values = valueList2;
            condOnDemand.Values.AddRange(valueList2);
            condOnDemand.Operator = ConditionOperator.Equal;
            exp.Conditions.Add(condOnDemand);

            //They must be published
            ConditionExpression condPublished = new ConditionExpression();
            condPublished.AttributeName = "name";
            object[] valueList3 = { workflowName };
            condPublished.Values.AddRange(valueList3);
            condPublished.Operator = ConditionOperator.Equal;
            exp.Conditions.Add(condPublished);

            qe.Criteria = exp;

            var results = service.RetrieveMultiple(qe);

            var firstEntity = results.Entities.FirstOrDefault();

            if (firstEntity == null)
                return Guid.Empty;

            return firstEntity.Id;

        }
    }
}

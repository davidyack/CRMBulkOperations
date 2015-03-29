using ctccrm.ServerCommon.OrgServiceHelpers;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
// =====================================================================
//  File:		CrmChangeTrackingManager
//  Summary:	Tracks changes to CRM entities
//              This module is currently experimental 
// =====================================================================
// 
//
//  Copyright(C) 2015 Colorado Technology Consultants Inc.  All rights reserved.
//
//
//  THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY
//  KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
//  IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
//  PARTICULAR PURPOSE.
// 
//  Any use or other rights related to this source code, resulting object code or 
//  related artifacts are controlled the prevailing EULA in effect. See the EULA
//  for detail rights. In the event no EULA was provided contact copyright holder
//  for a current copy.
//
// =====================================================================
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CTCBulkOpLib
{
    public class CrmChangeTrackingManager
    {
        private IOrganizationService _Service = null;
        private int _DefaultPageSize = 100;

        public CrmChangeTrackingManager(IOrganizationService crmService)
        {
            _Service = crmService;
           
           
        }
        public void ApplyChanges(IOrganizationService targetService, ApplyChangesOptions[] entities, bool reprocessFailed = true)
        {
            foreach(var entity in entities)
            {
                ApplyChanges(targetService, entity.EntityName,entity.Columns, reprocessFailed: false);
            }

            if (reprocessFailed)
            {
                 CrmBulkServiceManager bulkMgr = new CrmBulkServiceManager(targetService);

                foreach (var entity in entities)
                {
                    ReProcessFailedForEntity(targetService, entity.EntityName, entity.Columns, bulkMgr);
                }
            }
        }
        public void ApplyChanges(IOrganizationService targetService, string entityName, ColumnSet columns,bool reprocessFailed=true)
        {
            RetrieveEntityChangesRequest changeRequest = new RetrieveEntityChangesRequest();
            changeRequest.EntityName = entityName;
            changeRequest.Columns = columns;
            changeRequest.PageInfo = new PagingInfo() { Count = _DefaultPageSize, PageNumber = 1, ReturnTotalRecordCount = false };
            var trackEntity = RetrieveTrackingEntity(targetService, entityName);
            if (trackEntity.Contains("ctccrm_datatoken"))
                changeRequest.DataVersion = trackEntity.GetAttributeValue<string>("ctccrm_datatoken");

            CrmBulkServiceManager bulkMgr = new CrmBulkServiceManager(targetService);
            
            ChangesToToAction(changeRequest, (response) =>
            {                
                List<Entity> entitiesToUpSert = new List<Entity>();
                List<EntityReference> entitiesToDelete = new List<EntityReference>();

                PrepareUpsertDeleteLists(response, entitiesToUpSert, entitiesToDelete);
               
                trackEntity["ctccrm_datatoken"] = response.EntityChanges.DataToken;
                entitiesToUpSert.Add(trackEntity);
                try
                {
                   bulkMgr.BulkUpsertAndDelete(entitiesToUpSert, entitiesToDelete,
                        transactionMode: CTCBulkTransactionMode.Single);
                }
                catch(Exception ex)
                {
                    if (changeRequest.PageInfo.Count == 1)
                    {
                        RecordFailedRecord(entityName, trackEntity, bulkMgr, entitiesToUpSert, ex);
                    }
                    else
                        throw ex;
                }

            });

            if (reprocessFailed)
                ReProcessFailedForEntity(targetService, entityName, columns, bulkMgr);


        }

        private void ReProcessFailedForEntity(IOrganizationService targetService, string entityName, ColumnSet columns, CrmBulkServiceManager bulkMgr)
        {
            QueryExpression queryFailed = new QueryExpression("ctccrm_entitychangefailed");
            queryFailed.ColumnSet = new ColumnSet(new string[] { "ctccrm_recordid", "ctccrm_retrycount" });
            queryFailed.Criteria = new FilterExpression();
            queryFailed.Criteria.AddCondition("ctccrm_name", ConditionOperator.Equal, entityName);
            
            ConditionExpression limitRetry = new ConditionExpression("ctccrm_retrycount", ConditionOperator.LessThan, 6);
            ConditionExpression retryNullOK = new ConditionExpression("ctccrm_retrycount", ConditionOperator.Null);
            var filterRetry = new FilterExpression( LogicalOperator.Or);
            filterRetry.AddCondition(limitRetry);
            filterRetry.AddCondition(retryNullOK);
            queryFailed.Criteria.Filters.Add(filterRetry);
           

            QueryToAction(targetService, queryFailed, (entityList) =>
            {
                foreach (var entity in entityList.Entities)
                {
                    try
                    {
                        var sourceEntity = _Service.Retrieve(entityName, Guid.Parse(entity.GetAttributeValue<string>("ctccrm_recordid")), columns);
                        TrimFieldsFromChanges(sourceEntity);
                        UpsertRequest targetReq = new UpsertRequest();
                        targetReq.Target = sourceEntity;
                        DeleteRequest deleteReq = new DeleteRequest();
                        deleteReq.Target = entity.ToEntityReference();
                        bulkMgr.BulkTransaction(new OrganizationRequest[] { targetReq, deleteReq });
                    }
                    catch (Exception ex)
                    {
                        int retryCount = entity.GetAttributeValue<int>("ctccrm_retrycount");
                        retryCount++;
                        entity["ctccrm_retrycount"] = retryCount;
                        targetService.Update(entity);
                    }

                }


            });
        }
        /// <summary>
        /// Helper method to query a page of data and call the action provided
        /// used to facilitate paging of the bulk operations 
        /// </summary>
        /// <param name="q"></param>
        /// <param name="action"></param>
        private void QueryToAction(IOrganizationService service,QueryExpression q, Action<EntityCollection> action)
        {

            int pageCount = 1;

            while (true)
            {

                var results = service.RetrieveMultiple(q);

                action(results);

                if (results.MoreRecords)
                {
                    pageCount++;
                    q.PageInfo = new PagingInfo();
                    q.PageInfo.Count = 5000;
                    q.PageInfo.PageNumber = pageCount;
                    q.PageInfo.PagingCookie = results.PagingCookie;
                }
                else
                    break;
            }


        }

        private  void RecordFailedRecord(string entityName, Entity trackEntity, CrmBulkServiceManager bulkMgr, List<Entity> entitiesToUpSert, Exception ex)
        {
            Entity failedEntity = new Entity("ctccrm_entitychangefailed");
            failedEntity["ctccrm_name"] = entityName;
            failedEntity["ctccrm_recordid"] = entitiesToUpSert[0].Id.ToString();
            failedEntity.KeyAttributes = new KeyAttributeCollection();
            failedEntity.KeyAttributes.Add("ctccrm_name", entityName);
            failedEntity.KeyAttributes.Add("ctccrm_recordid", entitiesToUpSert[0].Id.ToString());
            failedEntity["ctccrm_errormessage"] = ex.Message;
            List<Entity> failedLog = new List<Entity>();
            failedLog.Add(failedEntity);
            trackEntity.KeyAttributes = new KeyAttributeCollection();
            trackEntity.KeyAttributes.Add("ctccrm_name", entityName);
            failedLog.Add(trackEntity);
            bulkMgr.BulkUpdate(failedLog, transactionMode: CTCBulkTransactionMode.Single, useUpsert: true);
        }

        private  Entity RetrieveTrackingEntity(IOrganizationService targetService, string entityName)
        {
            try
            {
                RetrieveRequest trackReq = new RetrieveRequest();
                trackReq.ColumnSet = new ColumnSet(new string[] { "ctccrm_datatoken" });
                trackReq.Target = new EntityReference("ctccrm_entitychangetracking", new KeyAttributeCollection());
                trackReq.Target.KeyAttributes.Add("ctccrm_name", entityName);
                var trackResponse = targetService.Execute(trackReq) as RetrieveResponse;
                var trackEntity = trackResponse.Entity;
                return trackEntity;
            }
            catch(Exception)
            {
                var newEntity = new Entity("ctccrm_entitychangetracking");
                newEntity["ctccrm_name"] = entityName;
                return newEntity;
            }
        }

        private  void PrepareUpsertDeleteLists(RetrieveEntityChangesResponse response, List<Entity> entitiesToUpSert, List<EntityReference> entitiesToDelete)
        {
            foreach (var change in response.EntityChanges.Changes)
            {
                if (change.Type == ChangeType.NewOrUpdated)
                {
                    var changeditem = (NewOrUpdatedItem)change;
                    Entity changedEntity = changeditem.NewOrUpdatedEntity;
                    TrimFieldsFromChanges(changedEntity);
                    entitiesToUpSert.Add(changedEntity);
                }
                else if (change.Type == ChangeType.RemoveOrDeleted)
                {
                    var deleteditem = (RemovedOrDeletedItem)change;
                    EntityReference deletedEntityReference = deleteditem.RemovedItem;
                    entitiesToDelete.Add(deletedEntityReference);
                }
            }
        }

        private  void TrimFieldsFromChanges(Entity changedEntity)
        {
            if (changedEntity.Contains("ownerid"))
                changedEntity.Attributes.Remove("ownerid");
            if (changedEntity.Contains("transactioncurrencyid"))
                changedEntity.Attributes.Remove("transactioncurrencyid");

            //the following is due to a bug, currently the API stores type as contact intstead of account
            if (changedEntity.LogicalName == "contact")
                if (changedEntity.Contains("primarycustomerid"))
                    changedEntity.Attributes.Remove("primarycustomerid");
        }
        private void ChangesToToAction(RetrieveEntityChangesRequest req, Action<RetrieveEntityChangesResponse> action)
        {

            int pageCount = 1;
            
            while (true)
            {

                var results = _Service.Execute(req) as RetrieveEntityChangesResponse;

                try
                {
                    action(results);

                    if (results.EntityChanges.MoreRecords)
                    {
                        req.DataVersion = results.EntityChanges.DataToken;
                        pageCount++;
                        req.PageInfo = new PagingInfo();                        
                        req.PageInfo.Count = _DefaultPageSize;
                        req.PageInfo.PageNumber = pageCount;
                        req.PageInfo.PagingCookie = results.EntityChanges.PagingCookie;
                    }
                    else
                        break;
                }
                catch(Exception ex)
                {
                    //force retry of one record
                    req.PageInfo.Count = 1;
                    ChangesToToAction(req, action);
                }
            }


        }

       
    }
    public class ApplyChangesOptions
    {
        public string EntityName { get; set; }
        public ColumnSet Columns { get; set; }
    }
}

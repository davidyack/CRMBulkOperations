// =====================================================================
//  File:		CrmBulkServiceManager
//  Summary:	Manages the crm service for bulk calls
// =====================================================================
//
//  This file is part of the ctccrm.ServerCommon library.
//
//  Copyright(C) 2013 Colorado Technology Consultants Inc.  All rights reserved.
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

using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Threading;
using System.Threading.Tasks;

namespace ctccrm.ServerCommon.OrgServiceHelpers
{
    public class CrmBulkServiceManager
    {
        private IOrganizationService _Service = null;

        private List<IOrganizationService> _Services = null;

        /// <summary>
        /// Create an instance and set the Organization Service to use
        /// </summary>
        /// <param name="crmService"></param>
        public CrmBulkServiceManager(IOrganizationService crmService)
        {
            _Service = crmService;
            _Services = new List<IOrganizationService>();
            _Services.Add(_Service);
            
        }

        /// <summary>
        /// Create instance and pass a set of organization service - this will 
        /// cause requests to be processed using multiple threads
        /// </summary>
        /// <param name="serviceList"></param>
        public CrmBulkServiceManager(List<IOrganizationService> serviceList)
        {
            _Service = serviceList.FirstOrDefault(); ;
            _Services = new List<IOrganizationService>();
            _Services.AddRange(serviceList);

        }

        /// <summary>
        /// Bulk insert entities
        /// </summary>
        /// <param name="entityList"></param>
        /// <param name="batchSize"></param>
        /// <returns></returns>
        public CTCRunMultipleResponse BulkInsert(List<Entity> entityList, int batchSize = 500)
        {
            var requests = new List<OrganizationRequest>();
            foreach (var entity in entityList)
            {
                requests.Add(new CreateRequest() { Target = entity });
            }

            var results = RunMultipleRequests(requests, batchSize: batchSize);
            foreach (var item in results.ResultItems)
            {
                var createResp = item.Response as CreateResponse;
                if (createResp != null)
                    item.ItemID = createResp.id;
            }
            return results;
        }
        /// <summary>
        /// Bulk update the list of entities provided
        /// </summary>
        /// <param name="entityList"></param>
        /// <param name="batchSize"></param>
        /// <returns></returns>
        public CTCRunMultipleResponse BulkUpdate(List<Entity> entityList, int batchSize = 500)
        {
            var requests = new List<OrganizationRequest>();
            foreach (var entity in entityList)
            {
                requests.Add(new UpdateRequest() { Target = entity });
            }

            var results = RunMultipleRequests(requests, batchSize: batchSize);
            foreach (var item in results.ResultItems)
            {
                var updateReq = item.Request as UpdateRequest;
                if (updateReq != null)
                    item.ItemID = updateReq.Target.Id;
            }
            return results;
        }
        /// <summary>
        /// Helper method to query a page of data and call the action provided
        /// used to facilitate paging of the bulk operations 
        /// </summary>
        /// <param name="q"></param>
        /// <param name="action"></param>
        private void QueryToAction(QueryExpression q,  Action<EntityCollection> action)
        {
            
            int pageCount = 1;

            while (true)
            {
                
                var results = _Service.RetrieveMultiple(q);

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
        /// <summary>
        /// Query records, update with data provided in UpdateEntity
        /// </summary>
        /// <param name="q"></param>
        /// <param name="updateEntity"></param>
        /// <param name="batchSize"></param>
        /// <returns></returns>
        public CTCRunMultipleResponse BulkUpdate(QueryExpression q, Entity updateEntity, int batchSize = 500)
        {
            CTCRunMultipleResponse finalResults = new CTCRunMultipleResponse();
            finalResults.ResultItems = new List<CTCRunMultipleResponseItem>();
            
            QueryToAction(q, (entityList) =>
            {

                List<Entity> entitiesToUpdate = new List<Entity>();

                foreach (var entity in entityList.Entities)
                {
                    Entity updatedEntity = new Entity(entity.LogicalName);
                    updatedEntity.Id = entity.Id;
                    updatedEntity.Attributes.AddRange(updateEntity.Attributes);
                    entitiesToUpdate.Add(updatedEntity);
                }

                var results = BulkUpdate(entitiesToUpdate, batchSize: batchSize);
                finalResults.ResultItems.AddRange(results.ResultItems);
                finalResults.StoppedEarly = results.StoppedEarly;
                
            });

            return finalResults;
        }
        /// <summary>
        /// Query records and set state
        /// </summary>
        /// <param name="q"></param>
        /// <param name="state"></param>
        /// <param name="status"></param>
        /// <param name="batchSize"></param>
        /// <returns></returns>
        public CTCRunMultipleResponse BulkSetState(QueryExpression q, OptionSetValue state, OptionSetValue status, int batchSize = 100)
        {
            CTCRunMultipleResponse finalResults = new CTCRunMultipleResponse();
            finalResults.ResultItems = new List<CTCRunMultipleResponseItem>();

            QueryToAction(q, (entityList) =>
            {

                var requests = new List<OrganizationRequest>();

                foreach (var entity in entityList.Entities)
                {
                    requests.Add(new SetStateRequest() { EntityMoniker = entity.ToEntityReference(), State = state, Status = status });
                }

                var results = RunMultipleRequests(requests, batchSize: batchSize);
                finalResults.ResultItems.AddRange(results.ResultItems);
                finalResults.StoppedEarly = results.StoppedEarly;
            });


            return finalResults;
        }
        /// <summary>
        /// Set state for the list of entity records provided
        /// </summary>
        /// <param name="entityList"></param>
        /// <param name="state"></param>
        /// <param name="status"></param>
        /// <param name="batchSize"></param>
        /// <returns></returns>
        public CTCRunMultipleResponse BulkSetState(List<Entity> entityList, OptionSetValue state, OptionSetValue status, int batchSize = 100)
        {
            var requests = new List<OrganizationRequest>();
            foreach (var entity in entityList)
            {
                requests.Add(new SetStateRequest() { EntityMoniker = entity.ToEntityReference(), State = state, Status = status });
            }

            var results = RunMultipleRequests(requests, batchSize: batchSize);

            return results;
        }

        /// <summary>
        /// Query records and run specified workflow for each record
        /// </summary>
        /// <param name="q"></param>
        /// <param name="workflowID"></param>
        /// <param name="batchSize"></param>
        /// <returns></returns>
        public CTCRunMultipleResponse BulkRunWorkflow(QueryExpression q, Guid workflowID, int batchSize = 100)
        {
            CTCRunMultipleResponse finalResults = new CTCRunMultipleResponse();
            finalResults.ResultItems = new List<CTCRunMultipleResponseItem>();

            QueryToAction(q, (entityList) =>
            {

                var requests = new List<OrganizationRequest>();

                foreach (var entity in entityList.Entities)
                {
                    requests.Add(new ExecuteWorkflowRequest() { WorkflowId = workflowID, EntityId = entity.Id });
                }

                var results = RunMultipleRequests(requests, batchSize: batchSize);
                finalResults.ResultItems.AddRange(results.ResultItems);
                finalResults.StoppedEarly = results.StoppedEarly;
            });


            return finalResults;
        }
        /// <summary>
        /// Run a workflow for each entity in list
        /// </summary>
        /// <param name="entityList"></param>
        /// <param name="workflowID"></param>
        /// <param name="batchSize"></param>
        /// <returns></returns>
        public CTCRunMultipleResponse BulkRunWorkflow(List<Entity> entityList, Guid workflowID, int batchSize = 100)
        {
            var requests = new List<OrganizationRequest>();
            foreach (var entity in entityList)
            {
                requests.Add(new ExecuteWorkflowRequest() { WorkflowId = workflowID, EntityId = entity.Id });
            }

            var results = RunMultipleRequests(requests, batchSize: batchSize);

            return results;
        }
        /// <summary>
        /// Worker method to run multiple requests used by all helper methods but can be used standalone to invoke any series of CRM Organization Requests
        /// </summary>
        /// <param name="requests"></param>
        /// <param name="batchSize"></param>
        /// <param name="returnResponses"></param>
        /// <param name="continueOnError"></param>
        /// <param name="retryCount"></param>
        /// <param name="retrySeconds"></param>
        /// <returns></returns>
        public CTCRunMultipleResponse RunMultipleRequests(List<OrganizationRequest> requests, int batchSize = 100, bool returnResponses = true, bool continueOnError = true, int retryCount = 3, int retrySeconds = 10)
        {

            CTCRunMultipleState state = new CTCRunMultipleState();
            state.RequestIndex = 0;
            state.Results = new CTCRunMultipleResponse();
            state.Results.ResultItems = new List<CTCRunMultipleResponseItem>();
            state.RetryCount = retryCount;
            state.RetryDelaySeconds = retrySeconds;
            state.BatchSize = batchSize;
            state.ContinueOnError = continueOnError;
            state.ReturnResponses = true;
            state.Requests = requests;


            return RunMultipleRequestsInternal(state);

        }
        /// <summary>
        /// Helper method called by public method manages faults - will recurse to handle faults
        /// </summary>
        /// <param name="state"></param>
        /// <returns></returns>
        private CTCRunMultipleResponse RunMultipleRequestsInternal(CTCRunMultipleState state)
        {

            try
            {
                while (state.RequestIndex < state.Requests.Count)
                {
                    RunMultipleRequestsInternalBatch(state);

                    if (state.Results.StoppedEarly)
                    {
                        return state.Results;
                    }

                }

            }
            catch (FaultException<OrganizationServiceFault> fault)
            {
                return RunMultipleRequestsHandleFault(state, fault);
            }

            state.Results.StoppedEarly = false;
            return state.Results;
        }
        /// <summary>
        /// Worker method to handle fualt and track if retry is needed
        /// </summary>
        /// <param name="state"></param>
        /// <param name="fault"></param>
        /// <returns></returns>
        private CTCRunMultipleResponse RunMultipleRequestsHandleFault(CTCRunMultipleState state, FaultException<OrganizationServiceFault> fault)
        {
            if (fault.Detail.ErrorDetails.Contains("MaxBatchSize"))
            {
                //return request index to last position
                state.RequestIndex -= state.BatchSize;
                var allowedBatchSize = Convert.ToInt32(fault.Detail.ErrorDetails["MaxBatchSize"]);
                state.BatchSize = allowedBatchSize;
                return RunMultipleRequestsInternal(state);
            }
            if (fault.Detail.ErrorDetails.Contains("Server Busy"))
            {
                //return request index to last position
                state.RequestIndex -= state.BatchSize;
                if (state.RetryCount == 0)
                {
                    state.Results.StoppedEarly = true;
                    return state.Results;
                }

                Thread.Sleep(state.RetryDelaySeconds * 1000);
                state.RetryDelaySeconds = state.RetryDelaySeconds * 2;
                state.RetryCount--;
                return RunMultipleRequestsInternal(state);
            }

            throw fault;
        }
        /// <summary>
        /// Worker method to process batch of requests
        /// </summary>
        /// <param name="state"></param>
        private void RunMultipleRequestsInternalBatch(CTCRunMultipleState state)
        {
            var taskStateList = new List<CTCRunMultipleTaskState>();

            foreach (var service in _Services)
            {
                CTCRunMultipleTaskState taskState = new CTCRunMultipleTaskState();
                taskState.Service = service;
                taskState.ReturnResponses = state.ReturnResponses;
                taskState.ContinueOnError = state.ContinueOnError;
                taskState.Requests = state.Requests.Skip(state.RequestIndex).Take(state.BatchSize);
                state.RequestIndex += state.BatchSize;
                taskState.Responses = new List<CTCRunMultipleResponseItem>();
                taskStateList.Add(taskState);
            }
            Parallel.ForEach<CTCRunMultipleTaskState>(taskStateList, currentTask =>
                {
                    if (currentTask.Requests.Count() > 1)
                    {
                        ExecuteMultipleRequest req = new ExecuteMultipleRequest() { Settings = new ExecuteMultipleSettings(), Requests = new OrganizationRequestCollection() };
                        req.Settings.ContinueOnError = currentTask.ContinueOnError;
                        req.Settings.ReturnResponses = currentTask.ReturnResponses;
                        req.Requests.AddRange(currentTask.Requests);
                        var response = _Service.Execute(req) as ExecuteMultipleResponse;

                        foreach (var item in response.Responses)
                        {
                            CTCRunMultipleResponseItem resultItem = new CTCRunMultipleResponseItem();

                            resultItem.Fault = item.Fault;
                            resultItem.Request = req.Requests[item.RequestIndex];
                            resultItem.Response = item.Response;
                            currentTask.Responses.Add(resultItem);

                        }
                        if ((!state.ContinueOnError) && (response.IsFaulted))
                        {
                            state.Results.StoppedEarly = true;
                        }
                    }
                    else
                    {

                        var request = currentTask.Requests.FirstOrDefault();
                        if (request != null)
                        {
                            var response = _Service.Execute(request);

                            CTCRunMultipleResponseItem resultItem = new CTCRunMultipleResponseItem();
                            resultItem.Fault = null;
                            resultItem.Request = request;
                            resultItem.Response = response;
                            currentTask.Responses.Add(resultItem);
                        }
                    }

                    
                });

            foreach(var task in taskStateList)
            {
                state.Results.ResultItems.AddRange(task.Responses);
            }
           
        }

    }

    /// <summary>
    /// represents result of bulk and multiple operations
    /// </summary>
    public class CTCRunMultipleResponseItem
    {
        public OrganizationRequest Request { get; set; }
        public OrganizationResponse Response { get; set; }
        public OrganizationServiceFault Fault { get; set; }
        public Guid ItemID { get; set; }
    }
    /// <summary>
    /// return class from any bulk / multiple operation
    /// </summary>
    public class CTCRunMultipleResponse
    {
        public bool StoppedEarly { get; set; }
        public List<CTCRunMultipleResponseItem> ResultItems { get; set; }
        public List<CTCRunMultipleResponseItem> FaultedItems
        {
            get
            {
                return ResultItems.Where(i => i.Fault != null).ToList();
            }
        }
    }
    /// <summary>
    /// private class used to manage state between calls on batches of requests
    /// </summary>
    class CTCRunMultipleState
    {
        public CTCRunMultipleResponse Results { get; set; }
        public int RequestIndex { get; set; }
        public int RetryCount { get; set; }
        public int RetryDelaySeconds { get; set; }
        public bool ContinueOnError { get; set; }
        public bool ReturnResponses { get; set; }
        public int BatchSize { get; set; }
        public List<OrganizationRequest> Requests { get; set; }

    }

    class CTCRunMultipleTaskState
    {
        public IOrganizationService Service { get; set; }

        public IEnumerable<OrganizationRequest> Requests { get; set; }

        public  List<CTCRunMultipleResponseItem> Responses { get; set; }
        public bool ContinueOnError { get; set; }
        public bool ReturnResponses { get; set; }
    }
}

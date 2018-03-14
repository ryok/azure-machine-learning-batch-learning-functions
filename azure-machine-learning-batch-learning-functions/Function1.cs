using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using System.Configuration;

using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;

namespace AzureMLBatchLearningBlobTrigger
{
    public class AzureBlobDataReference
    {
        // Storage connection string used for regular blobs. It has the following format:
        // DefaultEndpointsProtocol=https;AccountName=ACCOUNT_NAME;AccountKey=ACCOUNT_KEY
        // It's not used for shared access signature blobs.
        public string ConnectionString { get; set; }

        // Relative uri for the blob, used for regular blobs as well as shared access
        // signature blobs.
        public string RelativeLocation { get; set; }

        // Base url, only used for shared access signature blobs.
        public string BaseLocation { get; set; }

        // Shared access signature, only used for shared access signature blobs.
        public string SasBlobToken { get; set; }
    }

    public enum BatchScoreStatusCode
    {
        NotStarted,
        Running,
        Failed,
        Cancelled,
        Finished
    }

    public class BatchScoreStatus
    {
        // Status code for the batch scoring job
        public BatchScoreStatusCode StatusCode { get; set; }

        // Locations for the potential multiple batch scoring outputs
        public IDictionary<string, AzureBlobDataReference> Results { get; set; }

        // Error details, if any
        public string Details { get; set; }
    }

    public class BatchExecutionRequest
    {

        public IDictionary<string, AzureBlobDataReference> Inputs { get; set; }

        public IDictionary<string, string> GlobalParameters { get; set; }

        // Locations for the potential multiple batch scoring outputs
        public IDictionary<string, AzureBlobDataReference> Outputs { get; set; }
    }

    public static class BondBatchLearningBlobTrigger
    {
        [FunctionName("BondBatchLearningBlobTrigger")]
        public static void Run(
            [BlobTrigger("supply-demand-predict-dataset/{name}", Connection = "BLOB_STORAGE_CONNECTION_4_BATCH_LEARNING")] Stream myBlob,
            string name,
            TraceWriter log)
        {
            log.Info($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");
            InvokeBatchExecutionService(name, myBlob, log).Wait();
            log.Info($"End.");
        }

        static async Task WriteFailedResponse(HttpResponseMessage response, TraceWriter log)
        {
            log.Info(string.Format("The request failed with status code: {0}", response.StatusCode));

            // Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
            log.Info(response.Headers.ToString());

            string responseContent = await response.Content.ReadAsStringAsync();
            log.Info(responseContent);
        }

        /*
        static void SaveBlobToFile(AzureBlobDataReference blobLocation, string resultsLabel, TraceWriter log)
        {
            const string OutputFileLocation = @"C:Temp\myresultsfile.ilearner"; // Replace this with the location you would like to use for your output file, and valid file extension (usually .csv for scoring results, or .ilearner for trained models)

            var credentials = new StorageCredentials(blobLocation.SasBlobToken);
            var blobUrl = new Uri(new Uri(blobLocation.BaseLocation), blobLocation.RelativeLocation);
            var cloudBlob = new CloudBlockBlob(blobUrl, credentials);

            log.Info(string.Format("Reading the result from {0}", blobUrl.ToString()));
            cloudBlob.DownloadToFile(OutputFileLocation, FileMode.Create);

            log.Info(string.Format("{0} have been written to the file {1}", resultsLabel, OutputFileLocation));
        }*/

        static void ProcessResults(BatchScoreStatus status, TraceWriter log)
        {
            ;
            foreach (var output in status.Results)
            {
                var blobLocation = output.Value;
                log.Info(string.Format("The result '{0}' is available at the following Azure Storage location:", output.Key));
                log.Info(string.Format("BaseLocation: {0}", blobLocation.BaseLocation));
                log.Info(string.Format("RelativeLocation: {0}", blobLocation.RelativeLocation));
                log.Info(string.Format("SasBlobToken: {0}", blobLocation.SasBlobToken));
                log.Info("");
            }
        }

        static async Task InvokeBatchExecutionService(string name, Stream blob, TraceWriter log)
        {
            // Web Service Info
            string BaseUrl = ConfigurationManager.AppSettings.Get("SUPPLY_DEMAND_PREDICT_BATCH_LEARNING_API_URL");
            string apiKey = ConfigurationManager.AppSettings.Get("SUPPLY_DEMAND_PREDICT_BATCH_LEARNING_API_KEY");

            // Storage Account Info
            string StorageAccountName = ConfigurationManager.AppSettings.Get("STORAGE_ACCOUNT_NAME_4_BATCH_LEARNING");
            string StorageAccountKey = ConfigurationManager.AppSettings.Get("STORAGE_ACCOUNT_KEY_4_BATCH_LEARNING");
            string StorageContainerName4Input = ConfigurationManager.AppSettings.Get("STORAGE_CONTAINER_NAME_4_BATCH_LEARNING_INPUT");
            string StorageContainerName4Output = ConfigurationManager.AppSettings.Get("STORAGE_CONTAINER_NAME_4_BATCH_LEARNING_OUTPUT");
            string storageConnectionString = string.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}", StorageAccountName, StorageAccountKey);

            // set a time out for polling status
            const int TimeOutInMilliseconds = 120 * 1000; // Set a timeout of 2 minutes

            using (HttpClient client = new HttpClient())
            {
                var request = new BatchExecutionRequest()
                {
                    Inputs = new Dictionary<string, AzureBlobDataReference>() {
                        {
                            "input1",
                             new AzureBlobDataReference()
                             {
                                 ConnectionString = storageConnectionString,
                                 RelativeLocation = string.Format("{0}/dataset_buy_azureml.csv", StorageContainerName4Input)
                             }
                        },
                    },

                    Outputs = new Dictionary<string, AzureBlobDataReference>() {
                        {
                            "output2",
                            new AzureBlobDataReference()
                            {
                                ConnectionString = storageConnectionString,
                                RelativeLocation = string.Format("{0}/output2results.ilearner", StorageContainerName4Output) /* output file (.csv for scoring results, or .ilearner for trained models) */
                            }
                        },
                        {
                            "output1",
                            new AzureBlobDataReference()
                            {
                                ConnectionString = storageConnectionString,
                                RelativeLocation = string.Format("{0}/output1results.csv", StorageContainerName4Output) /*output file (.csv for scoring results, or .ilearner for trained models) */
                            }
                        },
                    },

                    GlobalParameters = new Dictionary<string, string>()
                    {
                    }
                };

                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);

                // WARNING: The 'await' statement below can result in a deadlock
                // if you are calling this code from the UI thread of an ASP.Net application.
                // One way to address this would be to call ConfigureAwait(false)
                // so that the execution does not attempt to resume on the original context.
                // For instance, replace code such as:
                //      result = await DoSomeTask()
                // with the following:
                //      result = await DoSomeTask().ConfigureAwait(false)

                log.Info("Submitting the job...");

                // submit the job
                var response = await client.PostAsJsonAsync(BaseUrl + "?api-version=2.0", request);

                if (!response.IsSuccessStatusCode)
                {
                    await WriteFailedResponse(response, log);
                    return;
                }

                string jobId = await response.Content.ReadAsAsync<string>();
                log.Info(string.Format("Job ID: {0}", jobId));

                // start the job
                log.Info("Starting the job...");
                response = await client.PostAsync(BaseUrl + "/" + jobId + "/start?api-version=2.0", null);
                if (!response.IsSuccessStatusCode)
                {
                    await WriteFailedResponse(response, log);
                    return;
                }

                string jobLocation = BaseUrl + "/" + jobId + "?api-version=2.0";
                Stopwatch watch = Stopwatch.StartNew();
                bool done = false;
                while (!done)
                {
                    log.Info("Checking the job status...");
                    response = await client.GetAsync(jobLocation);
                    if (!response.IsSuccessStatusCode)
                    {
                        await WriteFailedResponse(response, log);
                        return;
                    }

                    BatchScoreStatus status = await response.Content.ReadAsAsync<BatchScoreStatus>();
                    if (watch.ElapsedMilliseconds > TimeOutInMilliseconds)
                    {
                        done = true;
                        log.Info(string.Format("Timed out. Deleting job {0} ...", jobId));
                        await client.DeleteAsync(jobLocation);
                    }
                    switch (status.StatusCode)
                    {
                        case BatchScoreStatusCode.NotStarted:
                            log.Info(string.Format("Job {0} not yet started...", jobId));
                            break;
                        case BatchScoreStatusCode.Running:
                            log.Info(string.Format("Job {0} running...", jobId));
                            break;
                        case BatchScoreStatusCode.Failed:
                            log.Info(string.Format("Job {0} failed!", jobId));
                            log.Info(string.Format("Error details: {0}", status.Details));
                            done = true;
                            break;
                        case BatchScoreStatusCode.Cancelled:
                            log.Info(string.Format("Job {0} cancelled!", jobId));
                            done = true;
                            break;
                        case BatchScoreStatusCode.Finished:
                            done = true;
                            log.Info(string.Format("Job {0} finished!", jobId));
                            ProcessResults(status, log);
                            break;
                    }

                    if (!done)
                    {
                        Thread.Sleep(1000); // Wait one second
                    }
                }
            }
        }
    }
}
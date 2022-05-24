/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using QuantConnect.Configuration;
using QuantConnect.Data.Auxiliary;
using QuantConnect.DataSource;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Logging;
using QuantConnect.Util;

namespace QuantConnect.DataProcessing
{
    /// <summary>
    /// QuiverCNBCDataDownloader implementation.
    /// </summary>
    public class QuiverCNBCDataDownloader : IDisposable
    {
        public const string VendorName = "Quiver";
        public const string VendorDataName = "CNBC";
        
        private readonly string _destinationFolder;
        private readonly string _universeFolder;
        private readonly string _clientKey;
        private readonly string _dataFolder = Globals.DataFolder;
        private readonly bool _canCreateUniverseFiles;
        private readonly int _maxRetries = 5;
        private static readonly List<char> _defunctDelimiters = new()
        {
            '-',
            '_'
        };
        private ConcurrentDictionary<string, ConcurrentQueue<string>> _tempData = new();
        
        private readonly JsonSerializerSettings _jsonSerializerSettings = new()
        {
            DateTimeZoneHandling = DateTimeZoneHandling.Utc
        };

        /// <summary>
        /// Control the rate of download per unit of time.
        /// </summary>
        private readonly RateGate _indexGate;

        /// <summary>
        /// Creates a new instance of <see cref="QuiverCNBC"/>
        /// </summary>
        /// <param name="destinationFolder">The folder where the data will be saved</param>
        /// <param name="apiKey">The Vendor API key</param>
        public QuiverCNBCDataDownloader(string destinationFolder, string apiKey = null)
        {
            _destinationFolder = Path.Combine(destinationFolder, VendorDataName);
            _universeFolder = Path.Combine(_destinationFolder, "universe");
            _clientKey = apiKey ?? Config.Get("vendor-auth-token");
            _canCreateUniverseFiles = Directory.Exists(Path.Combine(_dataFolder, "equity", "usa", "map_files"));

            // Represents rate limits of 10 requests per 1.1 second
            _indexGate = new RateGate(10, TimeSpan.FromSeconds(1.1));

            Directory.CreateDirectory(_destinationFolder);
            Directory.CreateDirectory(_universeFolder);
        }

        /// <summary>
        /// Runs the instance of the object.
        /// </summary>
        /// <returns>True if process all downloads successfully</returns>
        public bool Run()
        {
            var stopwatch = Stopwatch.StartNew();
            var today = DateTime.UtcNow.Date;

            var mapFileProvider = new LocalZipMapFileProvider();
            mapFileProvider.Initialize(new DefaultDataProvider());

            try
            {
                var companies = GetCompanies().Result.DistinctBy(x => x.Ticker).ToList();
                var count = companies.Count;
                var currentPercent = 0.05;
                var percent = 0.05;
                var i = 0;

                Log.Trace(
                    $"QuiverCNBCDataDownloader.Run(): Start processing {count.ToStringInvariant()} companies");

                var tasks = new List<Task>();
                var minDate = today;

                // This is the dictionary that
                // key: Date
                // value: List<String> csv content of that specific date
                IDictionary<DateTime, List<string>> MastercsvContents = new Dictionary<DateTime, List<string>>();

                foreach (var company in companies[1..20])
                {
                    // Include tickers that are "defunct".
                    // Remove the tag because it cannot be part of the API endpoint.
                    // This is separate from the NormalizeTicker(...) method since
                    // we don't convert tickers with `-`s into the format we can successfully
                    // index mapfiles with.
                    var quiverTicker = company.Ticker;
                    string ticker;


                    if (!TryNormalizeDefunctTicker(quiverTicker, out ticker))
                    {
                        Log.Error(
                            $"QuiverCNBCDataDownloader(): Defunct ticker {quiverTicker} is unable to be parsed. Continuing...");
                        continue;
                    }

                    // Begin processing ticker with a normalized value
                    Log.Trace($"QuiverCNBCDataDownloader.Run(): Processing {ticker}");

                    // Makes sure we don't overrun Quiver rate limits accidentally
                    _indexGate.WaitToProceed();

                    var sid = SecurityIdentifier.GenerateEquity(ticker, Market.USA, true, mapFileProvider, today);

                    tasks.Add(
                        HttpRequester($"live/cnbc?ticker={ticker}")
                            .ContinueWith(
                                y =>
                                {
                                    i++;

                                    if (y.IsFaulted)
                                    {
                                        Log.Error(
                                            $"QuiverCNBCDataDownloader.Run(): Failed to get data for {company}");
                                        return;
                                    }

                                    var result = y.Result;
                                    if (string.IsNullOrEmpty(result))
                                    {
                                        // We've already logged inside HttpRequester
                                        return;
                                    }

                                    var recentCNBC =
                                        JsonConvert.DeserializeObject<List<QuiverCNBC>>(result,
                                            _jsonSerializerSettings);
                                    var csvContents = new List<string>();

                                    foreach (var contract in recentCNBC)
                                    {
                                        if (contract.Traders == null)
                                        {
                                            continue;
                                        }

                                        var curTdate = contract.Date;

                                        if (curTdate == today)
                                        {
                                            Log.Trace($"Encountered data from today for {ticker}: {today:yyyy-MM-dd} - Skipping");
                                            continue;
                                        }

                                        if( DateTime.Compare(curTdate, minDate) < 0){
                                            minDate = curTdate;
                                        }

                                        var date = $"{contract.Date:yyyyMMdd}";

                                        var curRow = $"{date},{contract.Notes},{contract.Direction},{contract.Traders}";

                                        csvContents.Add(curRow);

                                        if (!MastercsvContents.ContainsKey(curTdate))
                                        {
                                            MastercsvContents.Add(curTdate, new List<string>());
                                        }

                                        MastercsvContents[curTdate].Add(curRow);

                                        if (!_canCreateUniverseFiles)
                                            continue;

                                        var queue = _tempData.GetOrAdd(date, new ConcurrentQueue<string>());
                                        //universe creation
                                        queue.Enqueue($"{sid},{ticker},{curRow}");
                                    }

                                    if (csvContents.Count != 0)
                                    {
                                        SaveContentToFile(_destinationFolder, ticker, csvContents);
                                    }

                                    var percentageDone = i / count;
                                    if (percentageDone >= currentPercent)
                                    {
                                        Log.Trace(
                                            $"QuiverCNBCDataDownloader.Run(): {percentageDone.ToStringInvariant("P2")} complete");
                                        currentPercent += percent;
                                    }
                                }
                            )
                    );

                    if (tasks.Count == 10)
                    {
                        Task.WaitAll(tasks.ToArray());
                        tasks.Clear();
                    }
                }

                if (tasks.Count != 0)
                {
                    Task.WaitAll(tasks.ToArray());
                    tasks.Clear();
                    // we will save the files by group dates
                    // tasks are all done and the MastercsvContents is filled need to seperate the 
                    // collected data by dates they occured
                    foreach (DateTime day in EachDay(minDate, today)){
                        var d = new List<string>();
                        //second for loop to collect all prior days up to the current day variable
                        foreach (DateTime daytwo in EachDay(minDate, day)){
                            d.AddRange(MastercsvContents[daytwo]);
                        }
                        SaveContentToFile(Path.Combine(_destinationFolder, "universe"), day.ToString(), d);
                    }

                }
            }
            catch (Exception e)
            {
                Log.Error(e);
                return false;
            }

            Log.Trace($"QuiverCNBCDataDownloader.Run(): Finished in {stopwatch.Elapsed.ToStringInvariant(null)}");
            return true;
        }

        /// <summary>
        /// Sends a GET request for the provided URL
        /// </summary>
        /// <param name="url">URL to send GET request for</param>
        /// <returns>Content as string</returns>
        /// <exception cref="Exception">Failed to get data after exceeding retries</exception>
        private async Task<string> HttpRequester(string url)
        {
            for (var retries = 1; retries <= _maxRetries; retries++)
            {
                try
                {
                    using (var client = new HttpClient())
                    {
                        client.BaseAddress = new Uri("https://api.quiverquant.com/beta/");
                        client.DefaultRequestHeaders.Clear();

                        // You must supply your API key in the HTTP header,
                        // otherwise you will receive a 403 Forbidden response
                        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Token", _clientKey);

                        // Responses are in JSON: you need to specify the HTTP header Accept: application/json
                        client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                        
                        // Makes sure we don't overrun Quiver rate limits accidentally
                        _indexGate.WaitToProceed();

                        var response = await client.GetAsync(Uri.EscapeUriString(url));
                        if (response.StatusCode == HttpStatusCode.NotFound)
                        {
                            Log.Error($"QuiverCNBCDataDownloader.HttpRequester(): Files not found at url: {Uri.EscapeUriString(url)}");
                            response.DisposeSafely();
                            return string.Empty;
                        }

                        if (response.StatusCode == HttpStatusCode.Unauthorized)
                        {
                            var finalRequestUri = response.RequestMessage.RequestUri; // contains the final location after following the redirect.
                            response = client.GetAsync(finalRequestUri).Result; // Reissue the request. The DefaultRequestHeaders configured on the client will be used, so we don't have to set them again.
                        }

                        response.EnsureSuccessStatusCode();

                        var result =  await response.Content.ReadAsStringAsync();
                        response.DisposeSafely();

                        return result;
                    }
                }
                catch (Exception e)
                {
                    Log.Error(e, $"QuiverCNBCDataDownloader.HttpRequester(): Error at HttpRequester. (retry {retries}/{_maxRetries})");
                    Thread.Sleep(1000);
                }
            }

            throw new Exception($"Request failed with no more retries remaining (retry {_maxRetries}/{_maxRetries})");
        }

        /// <summary>
        /// Gets the list of companies
        /// </summary>
        /// <returns>List of companies</returns>
        /// <exception cref="Exception"></exception>
        private async Task<List<Company>> GetCompanies()
        {
            try
            {
                var content = await HttpRequester("companies");
                return JsonConvert.DeserializeObject<List<Company>>(content);
            }
            catch (Exception e)
            {
                throw new Exception("QuiverDownloader.GetSymbols(): Error parsing companies list", e);
            }
        }

        /// <summary>
        /// Saves contents to disk, deleting existing zip files
        /// </summary>
        /// <param name="destinationFolder">Final destination of the data</param>
        /// <param name="name">file name</param>
        /// <param name="contents">Contents to write</param>
        private void SaveContentToFile(string destinationFolder, string name, IEnumerable<string> contents)
        {
            name = name.ToLowerInvariant();
            var finalPath = Path.Combine(destinationFolder, $"{name}.csv");
            var finalFileExists = File.Exists(finalPath);

            var lines = new HashSet<string>(contents);
            if (finalFileExists)
            {
                foreach (var line in File.ReadAllLines(finalPath))
                {
                    lines.Add(line);
                }
            }

            var finalLines = destinationFolder.Contains("universe") ? 
                lines.OrderBy(x => x.Split(',').First()).ToList() :
                lines
                .OrderBy(x => DateTime.ParseExact(x.Split(',').First(), "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal))
                .ToList();

            var tempPath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}.tmp");
            File.WriteAllLines(tempPath, finalLines);
            var tempFilePath = new FileInfo(tempPath);
            tempFilePath.MoveTo(finalPath, true);
        }

        /// <summary>
        /// Tries to normalize a potentially defunct ticker into a normal ticker.
        /// </summary>
        /// <param name="ticker">Ticker as received from Estimize</param>
        /// <param name="nonDefunctTicker">Set as the non-defunct ticker</param>
        /// <returns>true for success, false for failure</returns>
        private static bool TryNormalizeDefunctTicker(string ticker, out string nonDefunctTicker)
        {
            // The "defunct" indicator can be in any capitalization/case
            if (ticker.IndexOf("defunct", StringComparison.OrdinalIgnoreCase) > 0)
            {
                foreach (var delimChar in _defunctDelimiters)
                {
                    var length = ticker.IndexOf(delimChar);

                    // Continue until we exhaust all delimiters
                    if (length == -1)
                    {
                        continue;
                    }

                    nonDefunctTicker = ticker[..length].Trim();
                    return true;
                }

                nonDefunctTicker = string.Empty;
                return false;
            }

            nonDefunctTicker = ticker;
            return true;
        }

        private class Company
        {
            /// <summary>
            /// The name of the company
            /// </summary>
            [JsonProperty(PropertyName = "Name")]
            public string Name { get; set; }

            /// <summary>
            /// The ticker/symbol for the company
            /// </summary>
            [JsonProperty(PropertyName = "Ticker")]
            public string Ticker { get; set; }
        }

        /// <summary>
        /// creates an IEnumerable for a range of dates
        /// </summary>
        /// <returns>IEnumerable for a range of dates</returns>
        public IEnumerable<DateTime> EachDay(DateTime from, DateTime thru)
        {
            for(var day = from.Date; day.Date <= thru.Date; day = day.AddDays(1))
                yield return day;
        }

        /// <summary>
        /// Disposes of unmanaged resources
        /// </summary>
        public void Dispose()
        {
            _indexGate?.Dispose();
        }
    }
}
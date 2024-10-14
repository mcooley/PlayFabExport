using PlayFab;
using PlayFab.AdminModels;
using System.Text;
using XenoAtom.CommandLine;

namespace PlayFabExport
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            string? title = null;
            string? segment = null;
            string? exportId = null;
            string? outputFile = null;

            var commandApp = new CommandApp("playfabexport")
            {
                new HelpOption(),
                new Command("download")
                {
                    new HelpOption(),
                    {"t|title=", "The title ID", t => title = t },
                    {"s|segment=", "The segment to download", s => segment = s },
                    {"e|export=", "The already-started export to download", e => exportId = e },
                    {"o|output=", "The output file", o => outputFile = o },
                    async (ctx, arguments) =>
                    {
                        if (string.IsNullOrEmpty(title)) throw new OptionException("Missing required value for option '--title'", "title");

                        string? playFabSecretKey = Environment.GetEnvironmentVariable("PLAYFAB_SECRET_KEY");
                        if (string.IsNullOrEmpty(playFabSecretKey))
                        {
                            Console.Error.WriteLine("The PlayFab secret key environment variable (PLAYFAB_SECRET_KEY) is not set.");
                            return -1;
                        }

                        PlayFabSettings.staticSettings.TitleId = title;
                        PlayFabSettings.staticSettings.DeveloperSecretKey = playFabSecretKey;

                        if (string.IsNullOrEmpty(outputFile)) throw new OptionException("Missing required value for option '--output'", "output");
                        using FileStream outputFileStream = File.OpenWrite(outputFile);

                        bool useExportId = !string.IsNullOrEmpty(exportId);
                        if (!useExportId && string.IsNullOrEmpty(segment))
                        {
                            throw new OptionException("Missing required value for option '--segment'", "segment");
                        }

                        try
                        {
                            if (useExportId)
                            {
                                await DownloadExportAsync(exportId, outputFileStream);
                            }
                            else
                            {
                                await ExportAsync(segment, outputFileStream);
                            }
                        }
                        catch (Exception e)
                        {
                            Console.Error.WriteLine($"Error while exporting segment: {e.Message}");
                            return -1;
                        }

                        return 0;
                    }
                }
            };

            await commandApp.RunAsync(args);
        }

        static async Task ExportAsync(string segmentId, FileStream outputFileStream)
        {
            Console.WriteLine($"Starting export for segment {segmentId}...");
            var exportId = await StartExportAsync(segmentId);
            Console.WriteLine($"Started export with id {exportId}. Checking for results...");

            await DownloadExportAsync(exportId, outputFileStream);
        }

        static async Task DownloadExportAsync(string exportId, FileStream outputFileStream)
        {
            Console.WriteLine($"Checking for results...");

            string indexUrl = string.Empty;
            while (indexUrl == string.Empty)
            {
                indexUrl = await GetIndexUrlAsync(exportId);

                if (indexUrl == string.Empty)
                {
                    Console.WriteLine("Export is not ready yet. Waiting 10 seconds before trying again...");
                    await Task.Delay(TimeSpan.FromSeconds(10));
                }
            }

            Console.WriteLine($"Export is ready at {indexUrl}. Downloading entries...");
            var indexEntries = await GetIndexEntriesAsync(indexUrl);

            Console.WriteLine($"Found {indexEntries.Length} files to download. Downloading...");

            bool wroteHeaderRow = false;
            for (int j = 0; j < indexEntries.Length; j++)
            {
                var playerEntries = await GetPlayerEntriesAsync(indexEntries[j]);
                var headerRow = playerEntries[0];
                if (!headerRow.Contains("PlayerId"))
                {
                    throw new Exception($"First row of exported file {indexEntries[j]} did not contain a header row");
                }

                if (!wroteHeaderRow)
                {
                    await outputFileStream.WriteAsync(Encoding.UTF8.GetBytes(headerRow));
                    await outputFileStream.WriteAsync(Encoding.UTF8.GetBytes("\n"));
                    wroteHeaderRow = true;
                }

                for (int i = 1; i < playerEntries.Length; i++)
                {
                    var playerEntry = playerEntries[i];
                    if (string.IsNullOrEmpty(playerEntry))
                    {
                        continue;
                    }

                    await outputFileStream.WriteAsync(Encoding.UTF8.GetBytes(playerEntry));
                    await outputFileStream.WriteAsync(Encoding.UTF8.GetBytes("\n"));
                }

                Console.WriteLine($"Downloaded {j + 1} files out of {indexEntries.Length}...");
                await Task.Delay(TimeSpan.FromSeconds(1));
            }

            Console.WriteLine("Segment download complete!");
        }

        static async Task<string> StartExportAsync(string segmentId)
        {
            var request = new ExportPlayersInSegmentRequest
            {
                SegmentId = segmentId,
            };

            var result = await PlayFabAdminAPI.ExportPlayersInSegmentAsync(request);

            if (result.Error != null)
            {
                throw new Exception($"Error exporting segment: {result.Error.ErrorMessage}");
            }

            return result.Result.ExportId;
        }

        static async Task<string> GetIndexUrlAsync(string exportId)
        {
            var request = new GetPlayersInSegmentExportRequest
            {
                ExportId = exportId,
            };

            var result = await PlayFabAdminAPI.GetSegmentExportAsync(request);

            if (result.Error != null)
            {
                throw new Exception($"Error getting segment index URL: {result.Error.ErrorMessage}");
            }

            if (result.Result.State == "Complete")
            {
                return result.Result.IndexUrl;
            }

            return string.Empty;
        }

        static async Task<string[]> GetIndexEntriesAsync(string indexFileUrl)
        {
            var client = new HttpClient();
            var result = await client.GetAsync(indexFileUrl);
            
            if (!result.IsSuccessStatusCode)
            {
                throw new Exception($"Error getting index file: HTTP {result.StatusCode}");
            }

            var contents = await result.Content.ReadAsStringAsync();
            return contents.Split('\n');
        }

        static async Task<string[]> GetPlayerEntriesAsync(string tsvFileUrl)
        {
            var client = new HttpClient();
            var result = await client.GetAsync(tsvFileUrl);

            if (!result.IsSuccessStatusCode)
            {
                throw new Exception($"Error getting tsv file: HTTP {result.StatusCode}");
            }

            var contents = await result.Content.ReadAsStringAsync();
            return contents.Split('\n');
        }
    }
}

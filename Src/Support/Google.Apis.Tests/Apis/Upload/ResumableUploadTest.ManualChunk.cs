/*
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

using Google.Apis.Tests.Mocks;
using Google.Apis.Upload;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Google.Apis.Tests.Apis.Upload
{
    public partial class ResumableUploadTest
    {
        private class ManualChunkServer : TestServer.Handler
        {
            public List<byte> ReceivedBytes { get; } = new List<byte>();
            public List<string> ReceivedContentRanges { get; } = new List<string>();

            public ManualChunkServer(TestServer server) : base(server) { }

            protected override async Task<IEnumerable<byte>> HandleCall(HttpListenerRequest request, HttpListenerResponse response)
            {
                switch (RemovePrefix(request.Url.PathAndQuery))
                {
                    case "ManualChunk?uploadType=resumable":
                        response.Headers[HttpResponseHeader.Location] = $"{HttpPrefix}{UploadPath}";
                        return null;

                    case UploadPath:
                        string contentRange = request.Headers["Content-Range"];
                        ReceivedContentRanges.Add(contentRange);

                        var bytesStream = new MemoryStream();
                        await request.InputStream.CopyToAsync(bytesStream);
                        var data = bytesStream.ToArray();
                        ReceivedBytes.AddRange(data);

                        // If Content-Range contains /* (intermediate chunk) or bytes */* (status query), return 308
                        if (contentRange != null && (contentRange.EndsWith("/*") || contentRange == "bytes */*"))
                        {
                            response.StatusCode = 308;
                            if (ReceivedBytes.Count > 0)
                            {
                                response.AddHeader("Range", $"bytes 0-{ReceivedBytes.Count - 1}");
                            }
                            return null;
                        }

                        // Otherwise it's a final chunk or finalize call
                        response.StatusCode = 200;
                        return Encoding.UTF8.GetBytes("{\"id\":\"test-object\"}");

                    default:
                        throw new InvalidOperationException($"Unexpected request path: {request.Url.PathAndQuery}");
                }
            }
        }

        [Fact]
        public async Task TestUploadChunk_IntermediateChunk256KiB_Success()
        {
            using (var server = new ManualChunkServer(_server))
            using (var service = new MockClientService(server.HttpPrefix))
            {
                var tmpUploader = new TestResumableUpload(service, "ManualChunk", "POST", Stream.Null, "text/plain", 100);
                var sessionUri = await tmpUploader.InitiateSessionAsync();
                Assert.NotNull(sessionUri);

                // 256 KiB chunk
                byte[] chunkData = new byte[ResumableUpload.MinimumChunkMultiple];
                for (int i = 0; i < chunkData.Length; i++)
                {
                    chunkData[i] = (byte)(i % 256);
                }

                using var chunkStream = new MemoryStream(chunkData);
                var uploader = ResumableUpload.CreateFromUploadUri(sessionUri, chunkStream);
                var progress = await uploader.UploadChunkAsync(chunkStream, isFinalChunk: false);

                Assert.Equal(UploadStatus.Uploading, progress.Status);
                Assert.Equal(ResumableUpload.MinimumChunkMultiple, progress.BytesSent);
                Assert.Single(server.ReceivedContentRanges);
                Assert.Equal($"bytes 0-{ResumableUpload.MinimumChunkMultiple - 1}/*", server.ReceivedContentRanges[0]);
                Assert.Equal(chunkData, server.ReceivedBytes.ToArray());
            }
        }

        [Fact]
        public async Task TestUploadChunk_UnalignedIntermediateChunk_ThrowsArgumentException()
        {
            using (var server = new ManualChunkServer(_server))
            using (var service = new MockClientService(server.HttpPrefix))
            {
                var tmpUploader = new TestResumableUpload(service, "ManualChunk", "POST", Stream.Null, "text/plain", 100);
                var sessionUri = await tmpUploader.InitiateSessionAsync();

                // 1000 bytes (not a multiple of 256 KiB)
                byte[] unalignedData = new byte[1000];
                using var chunkStream = new MemoryStream(unalignedData);
                var uploader = ResumableUpload.CreateFromUploadUri(sessionUri, chunkStream);

                var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
                    uploader.UploadChunkAsync(chunkStream, isFinalChunk: false));

                Assert.Contains("256 KiB", ex.Message);
                Assert.Empty(server.ReceivedContentRanges);
            }
        }

        [Fact]
        public async Task TestUploadChunk_FinalChunkArbitrarySize_Success()
        {
            using (var server = new ManualChunkServer(_server))
            using (var service = new MockClientService(server.HttpPrefix))
            {
                var tmpUploader = new TestResumableUpload(service, "ManualChunk", "POST", Stream.Null, "text/plain", 100);
                var sessionUri = await tmpUploader.InitiateSessionAsync();

                // Final chunk of arbitrary size (100 bytes)
                byte[] finalData = new byte[100];
                using var chunkStream = new MemoryStream(finalData);
                var uploader = ResumableUpload.CreateFromUploadUri(sessionUri, chunkStream);

                var progress = await uploader.UploadChunkAsync(chunkStream, isFinalChunk: true);

                Assert.Equal(UploadStatus.Completed, progress.Status);
                Assert.Equal(100, progress.BytesSent);
                Assert.Single(server.ReceivedContentRanges);
                Assert.Equal("bytes 0-99/100", server.ReceivedContentRanges[0]);
            }
        }

        [Fact]
        public async Task TestUploadChunk_MultiChunkSequence_Success()
        {
            using (var server = new ManualChunkServer(_server))
            using (var service = new MockClientService(server.HttpPrefix))
            {
                var tmpUploader = new TestResumableUpload(service, "ManualChunk", "POST", Stream.Null, "text/plain", 100);
                var sessionUri = await tmpUploader.InitiateSessionAsync();
                var uploader = ResumableUpload.CreateFromUploadUri(sessionUri, Stream.Null);

                // Chunk 1: 256 KiB
                byte[] chunk1 = new byte[ResumableUpload.MinimumChunkMultiple];
                using (var stream1 = new MemoryStream(chunk1))
                {
                    var p1 = await uploader.UploadChunkAsync(stream1, isFinalChunk: false);
                    Assert.Equal(UploadStatus.Uploading, p1.Status);
                    Assert.Equal(ResumableUpload.MinimumChunkMultiple, p1.BytesSent);
                }

                // Chunk 2: 512 KiB (2 * 256 KiB)
                byte[] chunk2 = new byte[2 * ResumableUpload.MinimumChunkMultiple];
                using (var stream2 = new MemoryStream(chunk2))
                {
                    var p2 = await uploader.UploadChunkAsync(stream2, isFinalChunk: false);
                    Assert.Equal(UploadStatus.Uploading, p2.Status);
                    Assert.Equal(3 * ResumableUpload.MinimumChunkMultiple, p2.BytesSent);
                }

                // Chunk 3: 50 bytes (Final chunk)
                byte[] chunk3 = new byte[50];
                using (var stream3 = new MemoryStream(chunk3))
                {
                    var p3 = await uploader.UploadChunkAsync(stream3, isFinalChunk: true);
                    Assert.Equal(UploadStatus.Completed, p3.Status);
                    Assert.Equal(3 * ResumableUpload.MinimumChunkMultiple + 50, p3.BytesSent);
                }

                Assert.Equal(3, server.ReceivedContentRanges.Count);
                Assert.Equal($"bytes 0-{ResumableUpload.MinimumChunkMultiple - 1}/*", server.ReceivedContentRanges[0]);
                Assert.Equal($"bytes {ResumableUpload.MinimumChunkMultiple}-{3 * ResumableUpload.MinimumChunkMultiple - 1}/*", server.ReceivedContentRanges[1]);
                Assert.Equal($"bytes {3 * ResumableUpload.MinimumChunkMultiple}-{3 * ResumableUpload.MinimumChunkMultiple + 49}/{3 * ResumableUpload.MinimumChunkMultiple + 50}", server.ReceivedContentRanges[2]);
            }
        }

        [Fact]
        public async Task TestUploadChunk_DoesNotDisposeCallerStream()
        {
            using (var server = new ManualChunkServer(_server))
            using (var service = new MockClientService(server.HttpPrefix))
            {
                var tmpUploader = new TestResumableUpload(service, "ManualChunk", "POST", Stream.Null, "text/plain", 100);
                var sessionUri = await tmpUploader.InitiateSessionAsync();

                byte[] chunkData = new byte[ResumableUpload.MinimumChunkMultiple];
                var callerStream = new MemoryStream(chunkData);
                var uploader = ResumableUpload.CreateFromUploadUri(sessionUri, callerStream);

                var progress = await uploader.UploadChunkAsync(callerStream, isFinalChunk: false);

                Assert.Equal(UploadStatus.Uploading, progress.Status);

                // Ensure the caller stream is still usable and not disposed
                Assert.True(callerStream.CanRead);
                Assert.True(callerStream.CanSeek);
                callerStream.Position = 0;
                Assert.Equal(ResumableUpload.MinimumChunkMultiple, callerStream.Length);
            }
        }

        [Fact]
        public async Task TestUploadChunk_NonSeekableStream()
        {
            using (var server = new ManualChunkServer(_server))
            using (var service = new MockClientService(server.HttpPrefix))
            {
                var tmpUploader = new TestResumableUpload(service, "ManualChunk", "POST", Stream.Null, "text/plain", 100);
                var sessionUri = await tmpUploader.InitiateSessionAsync();

                byte[] chunkData = new byte[ResumableUpload.MinimumChunkMultiple];
                var nonSeekableStream = new UnknownSizeMemoryStream(chunkData);
                var uploader = ResumableUpload.CreateFromUploadUri(sessionUri, nonSeekableStream);

                var progress = await uploader.UploadChunkAsync(nonSeekableStream, isFinalChunk: false);

                Assert.Equal(UploadStatus.Uploading, progress.Status);
                Assert.Equal(ResumableUpload.MinimumChunkMultiple, progress.BytesSent);
            }
        }

        [Fact]
        public async Task TestFinalizeUpload_SendsContentRangeBytesStarTotal()
        {
            using (var server = new ManualChunkServer(_server))
            using (var service = new MockClientService(server.HttpPrefix))
            {
                var tmpUploader = new TestResumableUpload(service, "ManualChunk", "POST", Stream.Null, "text/plain", 100);
                var sessionUri = await tmpUploader.InitiateSessionAsync();
                var uploader = ResumableUpload.CreateFromUploadUri(sessionUri, Stream.Null);

                // Send 256 KiB intermediate chunk
                byte[] chunkData = new byte[ResumableUpload.MinimumChunkMultiple];
                using (var stream = new MemoryStream(chunkData))
                {
                    await uploader.UploadChunkAsync(stream, isFinalChunk: false);
                }

                // Finalize session with zero-byte finalization
                var finalizeProgress = await uploader.FinalizeUploadAsync(ResumableUpload.MinimumChunkMultiple);

                Assert.Equal(UploadStatus.Completed, finalizeProgress.Status);
                Assert.Equal(ResumableUpload.MinimumChunkMultiple, finalizeProgress.BytesSent);
                Assert.Equal(2, server.ReceivedContentRanges.Count);
                Assert.Equal($"bytes */{ResumableUpload.MinimumChunkMultiple}", server.ReceivedContentRanges[1]);
            }
        }

        [Fact]
        public async Task TestQueryUploadStatus_ParsesRangeHeader()
        {
            using (var server = new ManualChunkServer(_server))
            using (var service = new MockClientService(server.HttpPrefix))
            {
                var tmpUploader = new TestResumableUpload(service, "ManualChunk", "POST", Stream.Null, "text/plain", 100);
                var sessionUri = await tmpUploader.InitiateSessionAsync();
                var uploader = ResumableUpload.CreateFromUploadUri(sessionUri, Stream.Null);

                // Send 256 KiB chunk
                byte[] chunkData = new byte[ResumableUpload.MinimumChunkMultiple];
                using (var stream = new MemoryStream(chunkData))
                {
                    await uploader.UploadChunkAsync(stream, isFinalChunk: false);
                }

                // Query status
                long committedBytes = await uploader.QueryUploadStatusAsync();

                Assert.Equal(ResumableUpload.MinimumChunkMultiple, committedBytes);
                Assert.Equal(2, server.ReceivedContentRanges.Count);
                Assert.Equal("bytes */*", server.ReceivedContentRanges[1]);
            }
        }

        [Fact]
        public async Task TestUploadChunk_UninitiatedSession_ThrowsInvalidOperationException()
        {
            using (var service = new MockClientService("http://localhost/"))
            {
                var uploader = new TestResumableUpload(service, "ManualChunk", "POST", Stream.Null, "text/plain", 100);
                byte[] chunkData = new byte[ResumableUpload.MinimumChunkMultiple];
                using var stream = new MemoryStream(chunkData);

                await Assert.ThrowsAsync<InvalidOperationException>(() =>
                    uploader.UploadChunkAsync(stream, isFinalChunk: false));

                await Assert.ThrowsAsync<InvalidOperationException>(() =>
                    uploader.FinalizeUploadAsync(100));

                await Assert.ThrowsAsync<InvalidOperationException>(() =>
                    uploader.QueryUploadStatusAsync());
            }
        }

        [Fact]
        public async Task TestSyncWrappers_WorkAsExpected()
        {
            using (var server = new ManualChunkServer(_server))
            using (var service = new MockClientService(server.HttpPrefix))
            {
                var tmpUploader = new TestResumableUpload(service, "ManualChunk", "POST", Stream.Null, "text/plain", 100);
                var sessionUri = await tmpUploader.InitiateSessionAsync();
                Assert.NotNull(sessionUri);

                var uploader = ResumableUpload.CreateFromUploadUri(sessionUri, Stream.Null);

                byte[] chunkData = new byte[ResumableUpload.MinimumChunkMultiple];
                using var stream = new MemoryStream(chunkData);

                var progress = uploader.UploadChunk(stream, isFinalChunk: false);
                Assert.Equal(UploadStatus.Uploading, progress.Status);

                long offset = uploader.QueryUploadStatus();
                Assert.Equal(ResumableUpload.MinimumChunkMultiple, offset);

                var finalProgress = uploader.FinalizeUpload(ResumableUpload.MinimumChunkMultiple);
                Assert.Equal(UploadStatus.Completed, finalProgress.Status);
            }
        }
    }
}

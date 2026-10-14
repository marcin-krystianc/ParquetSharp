#if !NETFRAMEWORK
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using ParquetSharp.IO;
using ParquetSharp.RowOriented;
using ArrowFileReader = ParquetSharp.Arrow.FileReader;
using ArrowFileWriter = ParquetSharp.Arrow.FileWriter;

namespace ParquetSharp.Test
{
    /// <summary>
    /// Verifies that writing to and reading from .NET streams uses the zero-copy span overloads
    /// and does not allocate managed memory proportional to the data size
    /// (see https://github.com/G-Research/ParquetSharp/issues/716).
    /// The net471 build intentionally uses the array fallback, so this fixture is excluded from
    /// .NET Framework test runs, which resolve the package's net471 assembly.
    /// </summary>
    [TestFixture]
    internal static class TestStreamAllocations
    {
        // Measured baselines on net10.0 linux-x64 are about 20 KB for an 8.5 MB write and well
        // under the budget for reads. Before the #716 fix the same operations allocated one
        // byte[] per native callback, roughly the full file size (~8.6 MB), so the budget
        // separates a regression from the baseline by close to an order of magnitude.
        private const long AllocationBudget = 1024 * 1024;

        private const int SmallRowCount = 250_000;
        private const int LargeRowCount = 2_000_000;

        [Test]
        public static void TestManagedStreamsUseSpanOverloads()
        {
            var values = CreateInts(1024 * 1024);
            using var stream = new OverloadCountingStream();

            using (var output = new ManagedOutputStream(stream, leaveOpen: true))
            using (var writer = new ParquetFileWriter(output, new Column[] { new Column<int>("x") }))
            {
                WriteRowGroup(writer, values);
            }

            Assert.That(stream.SpanWrites, Is.GreaterThan(0));
            Assert.That(stream.ArrayWrites, Is.Zero);

            stream.Position = 0;
            using var input = new ManagedRandomAccessFile(stream, leaveOpen: true);
            using (var reader = new ParquetFileReader(input))
            {
                AssertReadsBack(reader, values);
            }

            Assert.That(stream.SpanReads, Is.GreaterThan(0));
            Assert.That(stream.ArrayReads, Is.Zero);
        }

        [Test]
        public static void TestStreamConstructorsUseSpanOverloads()
        {
            // The Stream-based ParquetFileWriter and ParquetFileReader constructors wrap the
            // managed stream classes internally. Snappy also varies the write callback sizes.
            var values = CreateInts(1024 * 1024);
            using var stream = new OverloadCountingStream();

            using (var writer = new ParquetFileWriter(
                       stream, new Column[] { new Column<int>("x") }, compression: Compression.Snappy, leaveOpen: true))
            {
                WriteRowGroup(writer, values);
            }

            Assert.That(stream.SpanWrites, Is.GreaterThan(0));
            Assert.That(stream.ArrayWrites, Is.Zero);

            stream.Position = 0;
            using (var reader = new ParquetFileReader(stream, leaveOpen: true))
            {
                AssertReadsBack(reader, values);
            }

            Assert.That(stream.SpanReads, Is.GreaterThan(0));
            Assert.That(stream.ArrayReads, Is.Zero);
        }

        [Test]
        public static async Task TestArrowFileApiUsesSpanOverloads()
        {
            const int numRows = 100_000;
            var fields = new[] { new Apache.Arrow.Field("x", new Apache.Arrow.Types.Int32Type(), nullable: false) };
            var schema = new Apache.Arrow.Schema(fields, null);
            using var stream = new OverloadCountingStream();

            using (var writer = new ArrowFileWriter(stream, schema, leaveOpen: true))
            {
                var array = new Apache.Arrow.Int32Array.Builder().AppendRange(Enumerable.Range(0, numRows)).Build();
                using var batch = new Apache.Arrow.RecordBatch(schema, new Apache.Arrow.IArrowArray[] { array }, numRows);
                writer.WriteRecordBatch(batch);
                writer.Close();
            }

            Assert.That(stream.SpanWrites, Is.GreaterThan(0));
            Assert.That(stream.ArrayWrites, Is.Zero);

            stream.Position = 0;
            using (var reader = new ArrowFileReader(stream, leaveOpen: true))
            {
                using var batchReader = reader.GetRecordBatchReader();
                var rowsRead = 0L;
                Apache.Arrow.RecordBatch? batch;
                while ((batch = await batchReader.ReadNextRecordBatchAsync()) != null)
                {
                    rowsRead += batch.Length;
                    batch.Dispose();
                }
                Assert.That(rowsRead, Is.EqualTo(numRows));
            }

            Assert.That(stream.SpanReads, Is.GreaterThan(0));
            Assert.That(stream.ArrayReads, Is.Zero);
        }

        [Test]
        public static void TestRowOrientedApiUsesSpanOverloads()
        {
            const int numRows = 100_000;
            var rows = new (int, float)[numRows];
            for (var i = 0; i < numRows; ++i)
            {
                rows[i] = (i, i / 100f);
            }

            using var stream = new OverloadCountingStream();
            using (var output = new ManagedOutputStream(stream, leaveOpen: true))
            using (var writer = ParquetFile.CreateRowWriter<(int, float)>(output, new[] { "i", "f" }))
            {
                writer.WriteRows(rows);
                writer.Close();
            }

            Assert.That(stream.SpanWrites, Is.GreaterThan(0));
            Assert.That(stream.ArrayWrites, Is.Zero);

            stream.Position = 0;
            using var input = new ManagedRandomAccessFile(stream, leaveOpen: true);
            using (var reader = ParquetFile.CreateRowReader<(int, float)>(input))
            {
                Assert.That(reader.ReadRows(rowGroup: 0), Is.EqualTo(rows));
            }

            Assert.That(stream.SpanReads, Is.GreaterThan(0));
            Assert.That(stream.ArrayReads, Is.Zero);
        }

        [Test]
        public static void TestWriteAllocationsDoNotScaleWithDataSize()
        {
            // JIT and lazy-initialisation warm-up, so first-call allocations are not measured.
            WriteIntsAndFloats(new DiscardStream(), CreateInts(1_000), CreateFloats(1_000));

            var smallInts = CreateInts(SmallRowCount);
            var smallFloats = CreateFloats(SmallRowCount);
            var largeInts = CreateInts(LargeRowCount);
            var largeFloats = CreateFloats(LargeRowCount);
            var smallSink = new DiscardStream();
            var largeSink = new DiscardStream();

            var smallAllocated = MeasureAllocatedBytes(() => WriteIntsAndFloats(smallSink, smallInts, smallFloats));
            var largeAllocated = MeasureAllocatedBytes(() => WriteIntsAndFloats(largeSink, largeInts, largeFloats));
            TestContext.Out.WriteLine(
                $"wrote {smallSink.Length} bytes allocating {smallAllocated}, then {largeSink.Length} bytes allocating {largeAllocated}");

            Assert.That(largeSink.Length, Is.GreaterThan(smallSink.Length * 4), "sanity check: the large file must be much bigger");
            Assert.That(smallAllocated, Is.LessThan(AllocationBudget),
                $"writing {smallSink.Length} bytes allocated {smallAllocated} bytes of managed memory");
            Assert.That(largeAllocated, Is.LessThan(AllocationBudget),
                $"writing {largeSink.Length} bytes allocated {largeAllocated} bytes of managed memory, " +
                "which suggests a per-callback copy on the write path (see #716)");
        }

        [Test]
        public static void TestReadAllocationsDoNotScaleWithDataSize()
        {
            using var smallFile = BuildInMemoryFile(SmallRowCount);
            using var largeFile = BuildInMemoryFile(LargeRowCount);
            var destination = new int[LargeRowCount];

            // JIT and lazy-initialisation warm-up, so first-call allocations are not measured.
            ReadInts(smallFile, destination, SmallRowCount);

            var smallAllocated = MeasureAllocatedBytes(() => ReadInts(smallFile, destination, SmallRowCount));
            var largeAllocated = MeasureAllocatedBytes(() => ReadInts(largeFile, destination, LargeRowCount));
            TestContext.Out.WriteLine(
                $"read {smallFile.Length} bytes allocating {smallAllocated}, then {largeFile.Length} bytes allocating {largeAllocated}");

            Assert.That(smallAllocated, Is.LessThan(AllocationBudget),
                $"reading {smallFile.Length} bytes allocated {smallAllocated} bytes of managed memory");
            Assert.That(largeAllocated, Is.LessThan(AllocationBudget),
                $"reading {largeFile.Length} bytes allocated {largeAllocated} bytes of managed memory, " +
                "which suggests a per-callback copy on the read path (see #716)");
        }

        /// <summary>
        /// GC.GetAllocatedBytesForCurrentThread is a per-thread counter and the native stream
        /// callbacks re-enter on the calling thread, so tests running concurrently on other
        /// threads cannot affect the measurement. The measured action must stay synchronous:
        /// awaiting inside it could resume on a different thread.
        /// </summary>
        private static long MeasureAllocatedBytes(Action action)
        {
            var before = GC.GetAllocatedBytesForCurrentThread();
            action();
            return GC.GetAllocatedBytesForCurrentThread() - before;
        }

        private static void WriteIntsAndFloats(Stream sink, int[] ints, float[] floats)
        {
            using var output = new ManagedOutputStream(sink, leaveOpen: true);
            using var writer = new ParquetFileWriter(
                output, new Column[] { new Column<int>("i"), new Column<float>("f") }, Compression.Uncompressed);
            using var rowGroup = writer.AppendRowGroup();
            using (var intWriter = rowGroup.NextColumn().LogicalWriter<int>())
            {
                intWriter.WriteBatch(ints);
            }
            using (var floatWriter = rowGroup.NextColumn().LogicalWriter<float>())
            {
                floatWriter.WriteBatch(floats);
            }
            writer.Close();
        }

        private static MemoryStream BuildInMemoryFile(int numRows)
        {
            var stream = new MemoryStream();
            using var writer = new ParquetFileWriter(
                stream, new Column[] { new Column<int>("x") }, compression: Compression.Uncompressed, leaveOpen: true);
            WriteRowGroup(writer, CreateInts(numRows));
            return stream;
        }

        private static void ReadInts(MemoryStream stream, int[] destination, int expectedRows)
        {
            stream.Position = 0;
            using var reader = new ParquetFileReader(stream, leaveOpen: true);
            using var rowGroup = reader.RowGroup(0);
            using var column = rowGroup.Column(0).LogicalReader<int>();
            var numRows = checked((int) rowGroup.MetaData.NumRows);
            var totalRead = 0;
            while (totalRead < numRows)
            {
                var read = column.ReadBatch(destination.AsSpan(totalRead, numRows - totalRead));
                if (read == 0) break;
                totalRead += read;
            }
            reader.Close();
            if (totalRead != expectedRows)
            {
                throw new InvalidDataException($"expected {expectedRows} rows, read {totalRead}");
            }
        }

        private static void WriteRowGroup(ParquetFileWriter writer, int[] values)
        {
            using var rowGroup = writer.AppendRowGroup();
            using (var columnWriter = rowGroup.NextColumn().LogicalWriter<int>())
            {
                columnWriter.WriteBatch(values);
            }
            writer.Close();
        }

        private static void AssertReadsBack(ParquetFileReader reader, int[] expected)
        {
            using var rowGroup = reader.RowGroup(0);
            using var column = rowGroup.Column(0).LogicalReader<int>();
            Assert.That(column.ReadAll(checked((int) rowGroup.MetaData.NumRows)), Is.EqualTo(expected));
        }

        private static int[] CreateInts(int numRows)
        {
            var values = new int[numRows];
            for (var i = 0; i < numRows; ++i)
            {
                values[i] = i;
            }
            return values;
        }

        private static float[] CreateFloats(int numRows)
        {
            var values = new float[numRows];
            for (var i = 0; i < numRows; ++i)
            {
                values[i] = i / 100f;
            }
            return values;
        }

        /// <summary>
        /// Counts which Stream overloads are reached. Derives from Stream rather than MemoryStream
        /// because the MemoryStream span overloads fall back to the virtual array overloads for
        /// derived types, which would hide the distinction being tested.
        /// </summary>
        private sealed class OverloadCountingStream : Stream
        {
            public int ArrayWrites { get; private set; }
            public int SpanWrites { get; private set; }
            public int ArrayReads { get; private set; }
            public int SpanReads { get; private set; }

            public override void Write(byte[] buffer, int offset, int count)
            {
                ++ArrayWrites;
                _inner.Write(buffer, offset, count);
            }

            public override void Write(ReadOnlySpan<byte> buffer)
            {
                ++SpanWrites;
                _inner.Write(buffer);
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                ++ArrayReads;
                return _inner.Read(buffer, offset, count);
            }

            public override int Read(Span<byte> buffer)
            {
                ++SpanReads;
                return _inner.Read(buffer);
            }

            public override bool CanRead => true;
            public override bool CanSeek => true;
            public override bool CanWrite => true;
            public override long Length => _inner.Length;

            public override long Position
            {
                get => _inner.Position;
                set => _inner.Position = value;
            }

            public override void Flush() => _inner.Flush();
            public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);
            public override void SetLength(long value) => _inner.SetLength(value);

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    _inner.Dispose();
                }
                base.Dispose(disposing);
            }

            private readonly MemoryStream _inner = new();
        }

        /// <summary>
        /// Discards everything written, so that no managed buffer inside the sink grows with the
        /// data and the only size-dependent allocations are those made by the library itself.
        /// </summary>
        private sealed class DiscardStream : Stream
        {
            public override void Write(byte[] buffer, int offset, int count) => _position += count;
            public override void Write(ReadOnlySpan<byte> buffer) => _position += buffer.Length;
            public override bool CanRead => false;
            public override bool CanSeek => false;
            public override bool CanWrite => true;
            public override long Length => _position;

            public override long Position
            {
                get => _position;
                set => throw new NotSupportedException();
            }

            public override void Flush()
            {
            }

            public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();

            private long _position;
        }
    }
}
#endif

using System;
using System.IO;
using System.Linq;
using ParquetSharp.IO;

namespace ParquetSharp.Benchmark
{
    internal static class Program
    {
        public static int Main()
        {
            var expected = Enumerable.Range(0, 1024).ToArray();
            var filePath = "test.parquet";

            // Write test data.
            using (var writer = new ParquetFileWriter(filePath, new Column[] { new Column<int>("ids") }))
            {
                using var groupWriter = writer.AppendRowGroup();
                using var columnWriter = groupWriter.NextColumn().LogicalWriter<int>();

                columnWriter.WriteBatch(expected);

                writer.Close();
            }

            for (var i = 0; i < 100_000; ++i)
            {
                // Read test data, not disposing of the ManagedRandomAccessFile or ParquetFileReader
                var fileStream = File.OpenRead(filePath);
                using var managedFile = new ManagedRandomAccessFile(fileStream);
                var reader = new ParquetFileReader(managedFile);
                using var groupReader = reader.RowGroup(0);
                using var columnReader = groupReader.Column(0).LogicalReader<int>();
                columnReader.ReadAll(expected.Length);
                
                GC.Collect();
            }

            return 0;
        }
    }
}

// See https://aka.ms/new-console-template for more information
using Microsoft.Data.Sqlite;
using System;
using System.Data;
using System.Linq.Expressions;
using System.Threading.Tasks;

Console.WriteLine("Hello, World!");

var path = new System.IO.DirectoryInfo(System.IO.Path.Combine(Environment.CurrentDirectory, "data"));
if (!path.Exists)
    path.Create();


var file = new FileInfo(Path.Combine(Environment.CurrentDirectory, "data", "database.db"));
if (file.Exists)
    file.Delete();


//Environment.CurrentDirectory = path.FullName;


Console.WriteLine("work dir:" + Environment.CurrentDirectory);

//string symbol = "al9999";
int timeframe = 1;


string[] symbols = { "al9999", "cu9999", "rb9999" };

Task[] tasks = new Task[symbols.Length];

int i = 0;
foreach (string symbol in symbols)
{
    tasks[i] = Task.Factory.StartNew(() =>
    {
        CandlesDatabase the_candles_database = new CandlesDatabase(symbol, timeframe);
        the_candles_database.Open();
        the_candles_database.CreateTable();
        the_candles_database.test_trans();
        the_candles_database.Close();
    });

    i++;

}

try
{
    Task.WaitAll(tasks);
}
catch (AggregateException ae)
{
    Console.WriteLine("One or more exceptions occurred: ");
    foreach (var ex in ae.Flatten().InnerExceptions)
        Console.WriteLine("   {0}", ex.Message);
}


// CandlesDatabase candles_database = new CandlesDatabase(symbol, timeframe);
// candles_database.Open();
// candles_database.PrintTableScheme();
// candles_database.InsertData();
// Int64 pre_ths_datetime = await candles_database.GetPreTimeEnd();

// Console.WriteLine(pre_ths_datetime);


// Task t = Task.Factory.StartNew(() => {
//     candles_database.test_trans();
// });

// t.Wait();


//candles_database.Close();







class Utis
{
    public static string GetTimeFrameStr(int timeframe)
    {
        string retval;
        switch (timeframe)
        {
            case 1:
                retval = "M1";
                break;
            case 5:
                retval = "M5";
                break;
            case 60:
                retval = "H1";
                break;
            case 240:
                retval = "H4";
                break;
            default:
                retval = "M1";
                break;
        }

        return retval;
    }
}



class CandlesDatabase
{
    public CandlesDatabase(string symbol, int timeframe)
    {
        symbol_ = symbol;
        timeframe_ = timeframe;
        string sqlite_connection_string = String.Format(@"Data Source=./data/{0}_{1}.db", symbol, Utis.GetTimeFrameStr(timeframe));
        connection_ = new SqliteConnection(sqlite_connection_string);
    }


    public void Open()
    {
        connection_.Open();
    }

    public void Close()
    {
        connection_.Close();
    }

    public async void CreateTable()
    {
        var command = connection_.CreateCommand();

        command.CommandText = @"CREATE TABLE IF NOT EXISTS OHLC 
            ( id INTEGER PRIMARY KEY AUTOINCREMENT, 
            symbol TEXT NOT NULL,
            datetime INTEGER NOT NULL,
            ths_datetime INTEGER NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            amount REAL NOT NULL
            );
        ";
        command.Prepare();
        await command.ExecuteNonQueryAsync();
    }

    public void PrintTableScheme()
    {
        var command = connection_.CreateCommand();
        command.CommandText = "PRAGMA TABLE_INFO ('OHLC')";
        using (var reader = command.ExecuteReader())
        {
            var dt = new DataTable();
            dt.Load(reader);

            foreach (var row in dt.AsEnumerable())
            {
                foreach (DataColumn col in dt.Columns)
                    Console.Write($"{col.ColumnName}:{row[col.ColumnName]} ");
                Console.WriteLine();
            }
        }

    }

    public async void InsertData()
    {
        var writeCommand = connection_.CreateCommand();

        writeCommand.CommandText = String.Format(@"insert into OHLC values (NULL, @symbol, @datetime, @ths_datetime, @open, @high, @low, @close, @volume, @amount);");
        writeCommand.Parameters.AddWithValue("@symbol", symbol_);
        writeCommand.Parameters.AddWithValue("@datetime", 1674090780);
        writeCommand.Parameters.AddWithValue("@ths_datetime", 1674090780);
        writeCommand.Parameters.AddWithValue("@open", 99999.9);
        writeCommand.Parameters.AddWithValue("@high", 99999.9);
        writeCommand.Parameters.AddWithValue("@low", 99999.9);
        writeCommand.Parameters.AddWithValue("@close", 99999.9);
        writeCommand.Parameters.AddWithValue("@volume", 99999.9);
        writeCommand.Parameters.AddWithValue("@amount", 99999.9);


        writeCommand.Prepare();
        await writeCommand.ExecuteNonQueryAsync();
    }




    public void test_trans()
    {
        var transaction = connection_.BeginTransaction(deferred: true);
        var writeCommand = connection_.CreateCommand();
        writeCommand.Transaction = transaction;

        for (int i = 1; i <= 10000000; i++)
        {
            writeCommand.Parameters.Clear();
            writeCommand.CommandText = String.Format(@"insert into OHLC values (NULL, @symbol, @datetime, @ths_datetime, @open, @high, @low, @close, @volume, @amount);");
            writeCommand.Parameters.AddWithValue("@symbol", symbol_);
            writeCommand.Parameters.AddWithValue("@datetime", 1674090780);
            writeCommand.Parameters.AddWithValue("@ths_datetime", 1674090780);
            writeCommand.Parameters.AddWithValue("@open", 99999.9);
            writeCommand.Parameters.AddWithValue("@high", 99999.9);
            writeCommand.Parameters.AddWithValue("@low", 99999.9);
            writeCommand.Parameters.AddWithValue("@close", 99999.9);
            writeCommand.Parameters.AddWithValue("@volume", 99999.9);
            writeCommand.Parameters.AddWithValue("@amount", 99999.9);

            writeCommand.Prepare();
            writeCommand.ExecuteNonQuery();

            if (i % 10000 == 9999)
            {
                transaction.Commit();
                transaction = connection_.BeginTransaction(deferred: true);
                writeCommand.Transaction = transaction;
            }
        }

        transaction.Commit();

        Console.WriteLine(String.Format("{0} 执行完毕", symbol_));
    }


    void test_memory_database()
    {
        using (var connection = new SqliteConnection("Data Source=:memory:"))
        {
            connection.Open();


            var command = connection.CreateCommand();

            // テーブルを作成する
            command.CommandText = "CREATE TABLE IF NOT EXISTS saitama ( no INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL);";
            command.ExecuteNonQuery();


            var transaction = connection.BeginTransaction(deferred: true);
            var writeCommand = connection.CreateCommand();
            writeCommand.Transaction = transaction;

            for (int i = 1; i <= 10000000; i++)
            {

                writeCommand.CommandText = String.Format(@"insert into saitama values ({0}, 'test123');", i);
                //writeCommand.Parameters.AddWithValue("@ID", i);


                //writeCommand.Parameters["@ID"].Value = i;
                //writeCommand.Parameters.AddWithValue("@index", i);
                writeCommand.Prepare();
                writeCommand.ExecuteNonQuery();

                if (i % 10000 == 9999)
                {
                    transaction.Commit();
                    transaction = connection.BeginTransaction(deferred: true);
                    writeCommand.Transaction = transaction;
                }
            }

            transaction.Commit();

            Console.WriteLine("执行完毕");


            var backupCommand = connection.CreateCommand();
            backupCommand.CommandText = "VACUUM INTO 'out.db3';";
            backupCommand.ExecuteNonQuery();

        }
    }



    public async Task<Int64> GetPreTimeEnd()
    {
        var command = connection_.CreateCommand();
        command.CommandText = @"SELECT ths_datetime FROM OHLC ORDER BY ths_datetime ASC LIMIT 1";
        await command.PrepareAsync();
        object ret = await command.ExecuteScalarAsync();
        if (ret != null && ret.GetType() == typeof(Int64))
            return (Int64)ret;
        else
            return 0;

    }

    private SqliteConnection connection_;
    private string symbol_;
    private int timeframe_;

}

